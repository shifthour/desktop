
# COMMAND ----------
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
COPYRIGHT 2013 BLUE CROSS/BLUE SHIELD OF KANSAS CITY

DESCRIPTION:
Shared container used for Primary Keying of Mbr Vbb Cmpnt Rwrd

MODIFICATIONS:
Developer           Date            Change Description                            Project/Altius #           Development Project           Code Reviewer      Date Reviewed
------------------  --------------  -------------------------------------------   ------------------------   ---------------------------   -----------------  -------------
Raja Gummadi        2013-05-22      Initial Programming                           4963 VBB Phase III         IntegrateNewDevl              Bhoomi Dasari      7/3/2013

ANNOTATIONS:
- join primary key info with table info
- update primary key table (K_VBB_PLN) with new keys created today
- Assign primary surrogate key
- primary key hash file only contains current run keys and is cleared before writing
- Temp table is tuncated before load and runstat done after load
- SQL joins temp table with key table to assign known keys
- Load IDS temp. table
- Hash file (hf_vbb_cmpnt_allcol) cleared in the calling program - IdsMbrVbbCmpntRwrdExtr
- Hashfile cleared in calling program
- This container is used in:
    IdsMbrVbbCmpntRwrdExtr
  These programs need to be re-compiled when logic changes
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def run_MbrVbbCmpntRwrdPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict,
) -> DataFrame:
    """
    Executes the logic of the MbrVbbCmpntRwrdPK shared container.

    Parameters
    ----------
    df_AllCol : DataFrame
        DataFrame corresponding to link 'AllCol' (hf_mbr_vbb_cmpnt_rwrd_allcol).
    df_Transform : DataFrame
        DataFrame corresponding to link 'Transform' (hf_mbr_vbb_cmpnt_rwrd_trnsfrm).
    params : dict
        Runtime parameters and environment settings.

    Returns
    -------
    DataFrame
        DataFrame corresponding to link 'Key' (output to ClmProvPK).
    """

    # --------------------------------------------------
    # Unpack parameters (exactly once)
    # --------------------------------------------------
    CurrRunCycle        = params["CurrRunCycle"]
    SrcSysCd            = params["SrcSysCd"]
    IDSOwner            = params["IDSOwner"]
    ids_secret_name     = params["ids_secret_name"]
    jdbc_url            = params["ids_jdbc_url"]
    jdbc_props          = params["ids_jdbc_props"]
    adls_path           = params["adls_path"]
    adls_path_raw       = params["adls_path_raw"]
    adls_path_publish   = params["adls_path_publish"]

    # --------------------------------------------------
    # Step 1 – Deduplicate hashed-file inputs (scenario a)
    # --------------------------------------------------
    key_cols = [
        "MBR_UNIQ_KEY",
        "VBB_CMPNT_UNIQ_KEY",
        "VBB_CMPNT_RWRD_SEQ_NO",
        "SRC_SYS_CD_SK",
    ]

    df_AllColOut = dedup_sort(df_AllCol, key_cols, [("MBR_UNIQ_KEY", "A")])
    df_Dedupe    = dedup_sort(df_Transform, key_cols, [("MBR_UNIQ_KEY", "A")])

    # --------------------------------------------------
    # Step 2 – Load K_MBR_VBB_CMPNT_RWRD_TEMP (DB2)
    # --------------------------------------------------
    execute_dml(
        f"DROP TABLE IF EXISTS {IDSOwner}.K_MBR_VBB_CMPNT_RWRD_TEMP",
        jdbc_url,
        jdbc_props,
    )

    execute_dml(
        f"""
        CREATE TABLE {IDSOwner}.K_MBR_VBB_CMPNT_RWRD_TEMP (
            MBR_UNIQ_KEY              INT  NOT NULL,
            VBB_CMPNT_UNIQ_KEY        INT  NOT NULL,
            VBB_CMPNT_RWRD_SEQ_NO     INT  NOT NULL,
            SRC_SYS_CD_SK             INT  NOT NULL,
            PRIMARY KEY (
                MBR_UNIQ_KEY,
                VBB_CMPNT_UNIQ_KEY,
                VBB_CMPNT_RWRD_SEQ_NO,
                SRC_SYS_CD_SK
            )
        )
        """,
        jdbc_url,
        jdbc_props,
    )

    (
        df_Dedupe.write.format("jdbc")
        .option("url", jdbc_url)
        .options(**jdbc_props)
        .option("dbtable", f"{IDSOwner}.K_MBR_VBB_CMPNT_RWRD_TEMP")
        .mode("append")
        .save()
    )

    # --------------------------------------------------
    # Step 3 – Extract W_Extract result-set
    # --------------------------------------------------
    extract_query = f"""
    SELECT k.MBR_VBB_CMPNT_RWRD_SK,
           w.MBR_UNIQ_KEY,
           w.VBB_CMPNT_UNIQ_KEY,
           w.VBB_CMPNT_RWRD_SEQ_NO,
           w.SRC_SYS_CD_SK,
           k.CRT_RUN_CYC_EXCTN_SK
    FROM {IDSOwner}.K_MBR_VBB_CMPNT_RWRD_TEMP w
    JOIN {IDSOwner}.K_MBR_VBB_CMPNT_RWRD k
         ON w.SRC_SYS_CD_SK         = k.SRC_SYS_CD_SK
        AND w.VBB_CMPNT_UNIQ_KEY     = k.VBB_CMPNT_UNIQ_KEY
        AND w.VBB_CMPNT_RWRD_SEQ_NO  = k.VBB_CMPNT_RWRD_SEQ_NO
        AND w.MBR_UNIQ_KEY           = k.MBR_UNIQ_KEY
    UNION
    SELECT -1 AS MBR_VBB_CMPNT_RWRD_SK,
           w2.MBR_UNIQ_KEY,
           w2.VBB_CMPNT_UNIQ_KEY,
           w2.VBB_CMPNT_RWRD_SEQ_NO,
           w2.SRC_SYS_CD_SK,
           {CurrRunCycle} AS CRT_RUN_CYC_EXCTN_SK
    FROM {IDSOwner}.K_MBR_VBB_CMPNT_RWRD_TEMP w2
    WHERE NOT EXISTS (
        SELECT 1
        FROM {IDSOwner}.K_MBR_VBB_CMPNT_RWRD k2
        WHERE w2.SRC_SYS_CD_SK        = k2.SRC_SYS_CD_SK
          AND w2.VBB_CMPNT_UNIQ_KEY    = k2.VBB_CMPNT_UNIQ_KEY
          AND w2.VBB_CMPNT_RWRD_SEQ_NO = k2.VBB_CMPNT_RWRD_SEQ_NO
          AND w2.MBR_UNIQ_KEY          = k2.MBR_UNIQ_KEY
    )
    """

    df_W_Extract = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .options(**jdbc_props)
        .option("query", extract_query)
        .load()
    )

    # --------------------------------------------------
    # Step 4 – Primary-Key Transformer logic
    # --------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn(
            "INSRT_UPDT_CD",
            F.when(F.col("MBR_VBB_CMPNT_RWRD_SK") == F.lit(-1), F.lit("I")).otherwise(F.lit("U")),
        )
        .withColumn("SRC_SYS_CD", F.lit(SrcSysCd))
        .withColumn(
            "CRT_RUN_CYC_EXCTN_SK",
            F.when(
                F.col("MBR_VBB_CMPNT_RWRD_SK") == F.lit(-1),
                F.lit(CurrRunCycle),
            ).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK")),
        )
        .withColumn(
            "MBR_VBB_CMPNT_RWRD_SK",
            F.when(
                F.col("MBR_VBB_CMPNT_RWRD_SK") == F.lit(-1),
                F.lit(None),
            ).otherwise(F.col("MBR_VBB_CMPNT_RWRD_SK")),
        )
    )

    # Surrogate-Key Generation (mandatory placeholder call)
    df_enriched = SurrogateKeyGen(
        df_enriched,
        <DB sequence name>,
        "MBR_VBB_CMPNT_RWRD_SK",
        <schema>,
        <secret_name>,
    )

    # --------------------------------------------------
    # Step 5 – Derive Transformer output links
    # --------------------------------------------------
    df_updt = df_enriched.select(
        "MBR_UNIQ_KEY",
        "VBB_CMPNT_UNIQ_KEY",
        "VBB_CMPNT_RWRD_SEQ_NO",
        "SRC_SYS_CD",
        "CRT_RUN_CYC_EXCTN_SK",
        "MBR_VBB_CMPNT_RWRD_SK",
    )

    df_NewKeys = (
        df_enriched.filter(F.col("INSRT_UPDT_CD") == F.lit("I"))
        .select(
            "MBR_UNIQ_KEY",
            "VBB_CMPNT_UNIQ_KEY",
            "VBB_CMPNT_RWRD_SEQ_NO",
            "SRC_SYS_CD_SK",
            "CRT_RUN_CYC_EXCTN_SK",
            "MBR_VBB_CMPNT_RWRD_SK",
        )
    )

    df_Keys = df_enriched.select(
        "MBR_VBB_CMPNT_RWRD_SK",
        "MBR_UNIQ_KEY",
        "VBB_CMPNT_UNIQ_KEY",
        "VBB_CMPNT_RWRD_SEQ_NO",
        "SRC_SYS_CD_SK",
        "SRC_SYS_CD",
        "INSRT_UPDT_CD",
        "CRT_RUN_CYC_EXCTN_SK",
    )

    # --------------------------------------------------
    # Step 6 – Write hashed-file and sequential-file targets
    # --------------------------------------------------
    write_files(
        df_updt,
        f"{adls_path}/MbrVbbCmpntRwrdPK_updt.parquet",
        mode="overwrite",
        is_pqruet=True,
    )

    write_files(
        df_NewKeys,
        f"{adls_path}/load/K_MBR_VBB_CMPNT_RWRD.dat",
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote='"',
    )

    # --------------------------------------------------
    # Step 7 – Merge Transformer
    # --------------------------------------------------
    df_Merge = (
        df_AllColOut.alias("AllColOut")
        .join(
            df_Keys.alias("Keys"),
            [
                F.col("AllColOut.MBR_UNIQ_KEY") == F.col("Keys.MBR_UNIQ_KEY"),
                F.col("AllColOut.VBB_CMPNT_UNIQ_KEY") == F.col("Keys.VBB_CMPNT_UNIQ_KEY"),
                F.col("AllColOut.VBB_CMPNT_RWRD_SEQ_NO") == F.col("Keys.VBB_CMPNT_RWRD_SEQ_NO"),
            ],
            "left",
        )
    )

    df_Key = df_Merge.select(
        F.col("AllColOut.JOB_EXCTN_RCRD_ERR_SK"),
        F.col("Keys.INSRT_UPDT_CD"),
        F.col("AllColOut.DISCARD_IN"),
        F.col("AllColOut.PASS_THRU_IN"),
        F.col("AllColOut.FIRST_RECYC_DT"),
        F.col("AllColOut.ERR_CT"),
        F.col("AllColOut.RECYCLE_CT"),
        F.col("Keys.SRC_SYS_CD"),
        F.col("AllColOut.PRI_KEY_STRING"),
        F.col("Keys.MBR_VBB_CMPNT_RWRD_SK"),
        F.col("AllColOut.MBR_UNIQ_KEY"),
        F.col("AllColOut.VBB_CMPNT_UNIQ_KEY"),
        F.col("AllColOut.VBB_CMPNT_RWRD_SEQ_NO"),
        F.col("AllColOut.SRC_SYS_CD_SK"),
        F.col("Keys.CRT_RUN_CYC_EXCTN_SK"),
        F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("AllColOut.MERP_LIMIT_REASON"),
        F.col("AllColOut.ERN_RWRD_END_DT_SK"),
        F.col("AllColOut.ERN_RWRD_STRT_DT_SK"),
        F.col("AllColOut.SRC_SYS_CRT_DTM"),
        F.col("AllColOut.SRC_SYS_UPDT_DTM"),
        F.col("AllColOut.CMPLD_ACHV_LVL_NO"),
        F.col("AllColOut.ERN_RWRD_AMT"),
        F.col("AllColOut.PD_RWRD_AMT"),
        F.col("AllColOut.TRZ_MBR_UNVRS_ID"),
    )

    # --------------------------------------------------
    # Return the container’s single output link
    # --------------------------------------------------
    return df_Key
