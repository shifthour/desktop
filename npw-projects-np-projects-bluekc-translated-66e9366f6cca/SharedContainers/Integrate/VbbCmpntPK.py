# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ---------- 

"""
VbbCmpntPK  - Shared Container converted from IBM DataStage

COPYRIGHT 2013 BLUE CROSS/BLUE SHIELD OF KANSAS CITY

PROCESSING: Shared container used for Primary Keying of VBB CMPNT

MODIFICATIONS:
Developer           Date            Change Description                      Project/Altius #              Development Project          Code Reviewer          Date Reviewed
------------------- --------------- --------------------------------------- ----------------------------- ---------------------------- ---------------------- ----------------
Raja Gummadi        2013-05-13      Initial Programming                     4963 VBB Phase III Integrate   NewDevl                      Bhoomi Dasari          5/16/2013

Annotations:
- join primary key info with table info
- update primary key table (K_VBB_PLN) with new keys created today
- Assign primary surrogate key
- primary key hash file only contains current run keys and is cleared before writing
- Temp table is tuncated before load and runstat done after load
- SQL joins temp table with key table to assign known keys
- Load IDS temp. table
- Hash file (hf_vbb_cmpnt_allcol) cleared in the calling program - IdsVbbCmpntExtr
- Hashfile cleared in calling program
- This container is used in:
  IdsVbbCmpntExtr
  These programs need to be re-compiled when logic changes
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def run_VbbCmpntPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Executes the VbbCmpntPK shared-container logic.

    Parameters
    ----------
    df_AllCol : DataFrame
        Input link ‘AllCol’.
    df_Transform : DataFrame
        Input link ‘Transform’.
    params : dict
        Runtime parameters.

    Returns
    -------
    DataFrame
        Output link ‘Key’.
    """

    # ----- unpack parameters -----
    CurrRunCycle       = params["CurrRunCycle"]
    SrcSysCd           = params["SrcSysCd"]
    IDSOwner           = params["IDSOwner"]
    ids_secret_name    = params["ids_secret_name"]
    ids_jdbc_url       = params["ids_jdbc_url"]
    ids_jdbc_props     = params["ids_jdbc_props"]
    adls_path          = params["adls_path"]
    adls_path_raw      = params["adls_path_raw"]
    adls_path_publish  = params["adls_path_publish"]
    # --------------------------------

    # ----- remove intermediate hash file: hf_vbb_cmpnt_allcol -----
    df_AllCol_dedup = dedup_sort(
        df_AllCol,
        ["VBB_CMPNT_UNIQ_KEY", "SRC_SYS_CD_SK"],
        []
    )

    # ----- remove intermediate hash file: hf_vbb_cmpnt_trnsfrm -----
    df_Transform_dedup = dedup_sort(
        df_Transform,
        ["VBB_CMPNT_UNIQ_KEY", "SRC_SYS_CD_SK"],
        []
    )

    # ----------------------------------------------------------------
    # K_VBB_CMPNT_TEMP (DB2)  :  create / load / runstats
    # ----------------------------------------------------------------
    table_temp = f"{IDSOwner}.K_VBB_CMPNT_TEMP"

    execute_dml(f"DROP TABLE IF EXISTS {table_temp}", ids_jdbc_url, ids_jdbc_props)

    execute_dml(
        f"""
        CREATE TABLE {table_temp} (
            VBB_CMPNT_UNIQ_KEY INTEGER NOT NULL,
            SRC_SYS_CD_SK      INTEGER NOT NULL,
            PRIMARY KEY (VBB_CMPNT_UNIQ_KEY, SRC_SYS_CD_SK)
        )
        """,
        ids_jdbc_url,
        ids_jdbc_props
    )

    (
        df_Transform_dedup
        .write
        .format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", table_temp)
        .mode("append")
        .save()
    )

    execute_dml(
        f"""CALL SYSPROC.ADMIN_CMD(
            'runstats on table {table_temp} on key columns with distribution
             on key columns and detailed indexes all allow write access'
        )""",
        ids_jdbc_url,
        ids_jdbc_props
    )

    # ----------------------------------------------------------------
    # W_Extract  :  read union query from K_VBB_CMPNT_TEMP / K_VBB_CMPNT
    # ----------------------------------------------------------------
    extract_query = f"""
        SELECT  k.VBB_CMPNT_SK,
                w.VBB_CMPNT_UNIQ_KEY,
                w.SRC_SYS_CD_SK,
                k.CRT_RUN_CYC_EXCTN_SK
        FROM    {IDSOwner}.K_VBB_CMPNT_TEMP w,
                {IDSOwner}.K_VBB_CMPNT       k
        WHERE   w.SRC_SYS_CD_SK       = k.SRC_SYS_CD_SK
          AND   w.VBB_CMPNT_UNIQ_KEY  = k.VBB_CMPNT_UNIQ_KEY

        UNION

        SELECT  -1,
                w2.VBB_CMPNT_UNIQ_KEY,
                w2.SRC_SYS_CD_SK,
                {CurrRunCycle}
        FROM    {IDSOwner}.K_VBB_CMPNT_TEMP w2
        WHERE   NOT EXISTS (
                    SELECT 1
                    FROM   {IDSOwner}.K_VBB_CMPNT k2
                    WHERE  w2.SRC_SYS_CD_SK      = k2.SRC_SYS_CD_SK
                      AND  w2.VBB_CMPNT_UNIQ_KEY = k2.VBB_CMPNT_UNIQ_KEY
                )
    """

    df_W_Extract = (
        spark.read.format("jdbc")
             .option("url", ids_jdbc_url)
             .options(**ids_jdbc_props)
             .option("query", extract_query)
             .load()
    )

    # ----------------------------------------------------------------
    # PrimaryKey transformer
    # ----------------------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn(
            "svInstUpdt",
            F.when(F.col("VBB_CMPNT_SK") == F.lit(-1), F.lit("I")).otherwise(F.lit("U"))
        )
        .withColumn("svSrcSysCd", F.lit(SrcSysCd))
        .withColumn(
            "VBB_CMPNT_SK",
            F.when(F.col("svInstUpdt") == F.lit("I"), F.lit(None).cast("long"))
             .otherwise(F.col("VBB_CMPNT_SK"))
        )
        .withColumn(
            "svCrtRunCycExctnSk",
            F.when(F.col("svInstUpdt") == F.lit("I"), F.lit(CurrRunCycle))
             .otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
        )
    )

    # surrogate-key generation
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"VBB_CMPNT_SK",<schema>,<secret_name>)

    # ----------------------------------------------------------------
    # updt  ->  hf_vbb_cmpnt  (parquet)
    # ----------------------------------------------------------------
    df_updt = df_enriched.select(
        "VBB_CMPNT_UNIQ_KEY",
        F.col("svSrcSysCd").alias("SRC_SYS_CD"),
        F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
        "VBB_CMPNT_SK"
    )

    write_files(
        df_updt,
        f"{adls_path}/VbbCmpntPK_updt.parquet",
        is_pqruet=True
    )

    # ----------------------------------------------------------------
    # NewKeys  ->  K_VBB_CMPNT.dat  (CSV)
    # ----------------------------------------------------------------
    df_NewKeys = (
        df_enriched
        .filter(F.col("svInstUpdt") == F.lit("I"))
        .select(
            "VBB_CMPNT_UNIQ_KEY",
            "SRC_SYS_CD_SK",
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            "VBB_CMPNT_SK"
        )
    )

    write_files(
        df_NewKeys,
        f"{adls_path}/load/K_VBB_CMPNT.dat",
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote='"',
        nullValue=None
    )

    # ----------------------------------------------------------------
    # Keys link for Merge
    # ----------------------------------------------------------------
    df_Keys = df_enriched.select(
        "VBB_CMPNT_SK",
        "VBB_CMPNT_UNIQ_KEY",
        "SRC_SYS_CD_SK",
        F.col("svSrcSysCd").alias("SRC_SYS_CD"),
        F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
        F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
    )

    # ----------------------------------------------------------------
    # Merge transformer
    # ----------------------------------------------------------------
    df_Key = (
        df_AllCol_dedup.alias("all")
        .join(
            df_Keys.alias("k"),
            (F.col("all.VBB_CMPNT_UNIQ_KEY") == F.col("k.VBB_CMPNT_UNIQ_KEY")) &
            (F.col("all.SRC_SYS_CD_SK")    == F.col("k.SRC_SYS_CD_SK")),
            "left"
        )
        .select(
            F.col("all.JOB_EXCTN_RCRD_ERR_SK"),
            F.col("k.INSRT_UPDT_CD"),
            F.col("all.DISCARD_IN"),
            F.col("all.PASS_THRU_IN"),
            F.col("all.FIRST_RECYC_DT"),
            F.col("all.ERR_CT"),
            F.col("all.RECYCLE_CT"),
            F.col("k.SRC_SYS_CD"),
            F.col("all.PRI_KEY_STRING"),
            F.col("k.VBB_CMPNT_SK"),
            F.col("all.VBB_CMPNT_UNIQ_KEY"),
            F.col("all.SRC_SYS_CD_SK"),
            F.col("k.CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("all.HIPL_ID"),
            F.col("all.INPR_FINISH_RULE"),
            F.col("all.CDVL_VALUE_FUNCTION"),
            F.col("all.INPR_COMPLIANCE_METHOD"),
            F.col("all.MBR_ACHV_VALID_ALW_IN"),
            F.col("all.VBB_CMPNT_REENR_IN"),
            F.col("all.SRC_SYS_CRT_DTM"),
            F.col("all.SRC_SYS_UPDT_DTM"),
            F.col("all.VBB_CMPNT_ACHV_LVL_CT"),
            F.col("all.VBB_CMPNT_FNSH_DAYS_NO"),
            F.col("all.VBB_CMPNT_REINST_DAYS_NO"),
            F.col("all.VBB_TMPLT_UNIQ_KEY"),
            F.col("all.VBB_CMPNT_NM"),
            F.col("all.VBB_CMPNT_TYP_NM")
        )
    )

    # return the container’s single output link
    return df_Key