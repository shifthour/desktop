
"""
Job Name       : VbbRwrdPK
Folder Path    : Shared Containers/PrimaryKey
Job Type       : Server Job
Job Category   : DS_Integrate

DESCRIPTION:
COPYRIGHT 2013 BLUE CROSS/BLUE SHIELD OF KANSAS CITY

Shared container used for Primary Keying of VBB Reward

PROCESSING:


MODIFICATIONS:
Developer                       Date            Change Description                               Project/Altius #          Development Project      Code Reviewer        Date Reviewed
-----------------------         ------------    ---------------------------------------------    -----------------------    ----------------------   -----------------   --------------
Karthik Chintalapani            2013-04-29      Initial Programming                              4963 VBB Phase III         IntegrateNewDevl         Bhoomi Dasari        5/16/2013

DataStage Annotations:
- join primary key info with table info
- update primary key table (K_VBB_PLN) with new keys created today
- Assign primary surrogate key
- primary key hash file NOT cleared before routine. Used in Fkey routine GetFkeyVbbPln
- Temp table is tuncated before load and runstat done after load
- SQL joins temp table with key table to assign known keys
- Load IDS temp. table
- Hash file (hf_vbb_rwrd_allcol) cleared in the calling program - IdsVbbRwrdExtr
- Hashfile cleared in calling program
- This container is used in:
  IdsVbbRwrdExtr
  These programs need to be re-compiled when logic changes
"""

# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def run_VbbRwrdPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Executes the logic of the VbbRwrdPK shared container.

    Parameters
    ----------
    df_AllCol : DataFrame
        Input stream corresponding to link 'AllCol'.
    df_Transform : DataFrame
        Input stream corresponding to link 'Transform'.
    params : dict
        Dictionary of runtime parameters.

    Returns
    -------
    DataFrame
        Output stream corresponding to link 'Key'.
    """

    # ------------------------------------------------------------------
    # Unpack parameters (skip DB connection‐style ones per specification)
    # ------------------------------------------------------------------
    CurrRunCycle        = params["CurrRunCycle"]
    SrcSysCd            = params["SrcSysCd"]
    IDSOwner            = params["IDSOwner"]
    ids_secret_name     = params["ids_secret_name"]
    ids_jdbc_url        = params["ids_jdbc_url"]
    ids_jdbc_props      = params["ids_jdbc_props"]
    adls_path           = params["adls_path"]
    adls_path_raw       = params["adls_path_raw"]
    adls_path_publish   = params["adls_path_publish"]

    # ------------------------------------------------------------------
    # Replace intermediate hash file: hf_vbb_rwrd_allcol  (Scenario-a)
    # ------------------------------------------------------------------
    df_AllColOut = dedup_sort(
        df_AllCol,
        ["VBB_VNDR_UNIQ_KEY", "VBB_VNDR_RWRD_SEQ_NO", "SRC_SYS_CD_SK"],
        []
    )

    # ------------------------------------------------------------------
    # Replace intermediate hash file: hf_vbb_rwrd_dedupe  (Scenario-a)
    # ------------------------------------------------------------------
    df_DSLink49 = dedup_sort(
        df_Transform,
        ["VBB_VNDR_UNIQ_KEY", "VBB_VNDR_RWRD_SEQ_NO", "SRC_SYS_CD_SK"],
        []
    )

    # ------------------------------------------------------------------
    # Load temp table  IDSOwner.K_VBB_RWRD_TEMP
    # ------------------------------------------------------------------
    execute_dml(
        f"DROP TABLE {IDSOwner}.K_VBB_RWRD_TEMP",
        ids_jdbc_url,
        ids_jdbc_props
    )

    execute_dml(
        f"""
        CREATE TABLE {IDSOwner}.K_VBB_RWRD_TEMP (
            VBB_VNDR_UNIQ_KEY      INTEGER NOT NULL,
            VBB_VNDR_RWRD_SEQ_NO   INTEGER NOT NULL,
            SRC_SYS_CD_SK          INTEGER NOT NULL,
            PRIMARY KEY (VBB_VNDR_UNIQ_KEY, VBB_VNDR_RWRD_SEQ_NO, SRC_SYS_CD_SK)
        )
        """,
        ids_jdbc_url,
        ids_jdbc_props
    )

    (
        df_DSLink49.write
        .mode("append")
        .jdbc(url=ids_jdbc_url, table=f"{IDSOwner}.K_VBB_RWRD_TEMP", properties=ids_jdbc_props)
    )

    execute_dml(
        f"CALL SYSPROC.ADMIN_CMD('runstats on table {IDSOwner}.K_VBB_RWRD_TEMP on key columns with distribution on key columns and detailed indexes all allow write access')",
        ids_jdbc_url,
        ids_jdbc_props
    )

    # ------------------------------------------------------------------
    # Extract W_Extract stream from database
    # ------------------------------------------------------------------
    extract_query = f"""
        SELECT  k.VBB_RWRD_SK,
                w.VBB_VNDR_UNIQ_KEY,
                w.VBB_VNDR_RWRD_SEQ_NO,
                w.SRC_SYS_CD_SK,
                k.CRT_RUN_CYC_EXCTN_SK
        FROM    {IDSOwner}.K_VBB_RWRD_TEMP w
        JOIN    {IDSOwner}.K_VBB_RWRD      k
          ON    w.SRC_SYS_CD_SK        = k.SRC_SYS_CD_SK
         AND    w.VBB_VNDR_UNIQ_KEY     = k.VBB_VNDR_UNIQ_KEY
         AND    w.VBB_VNDR_RWRD_SEQ_NO  = k.VBB_VNDR_RWRD_SEQ_NO

        UNION

        SELECT  -1,
                w2.VBB_VNDR_UNIQ_KEY,
                w2.VBB_VNDR_RWRD_SEQ_NO,
                w2.SRC_SYS_CD_SK,
                {CurrRunCycle}
        FROM    {IDSOwner}.K_VBB_RWRD_TEMP w2
        WHERE NOT EXISTS (
              SELECT 1
              FROM   {IDSOwner}.K_VBB_RWRD k2
              WHERE  w2.SRC_SYS_CD_SK        = k2.SRC_SYS_CD_SK
                AND  w2.VBB_VNDR_UNIQ_KEY    = k2.VBB_VNDR_UNIQ_KEY
                AND  w2.VBB_VNDR_RWRD_SEQ_NO = k2.VBB_VNDR_RWRD_SEQ_NO
        )
    """

    df_W_Extract = (
        spark.read.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("query", extract_query)
        .load()
    )

    # ------------------------------------------------------------------
    # Transformer: PrimaryKey
    # ------------------------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn(
            "svInstUpdt",
            F.when(F.col("VBB_RWRD_SK") == F.lit(-1), F.lit("I")).otherwise(F.lit("U"))
        )
        .withColumn("svSrcSysCd", F.lit(SrcSysCd))
        .withColumn(
            "svSK",
            F.when(F.col("svInstUpdt") == F.lit("U"), F.col("VBB_RWRD_SK")).otherwise(F.lit(None))
        )
        .withColumn(
            "svCrtRunCycExctnSk",
            F.when(F.col("svInstUpdt") == F.lit("I"), F.lit(CurrRunCycle)).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
        )
    )

    # --------------------------------------------------------------
    # Surrogate Key generation (special placeholder call required)
    # --------------------------------------------------------------
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"svSK",<schema>,<secret_name>)

    # --------------------------------------------------------------
    # updt stream  ->  hf_vbb_rwrd  (Scenario-c : Parquet file)
    # --------------------------------------------------------------
    df_updt = (
        df_enriched
        .select(
            "VBB_VNDR_UNIQ_KEY",
            "VBB_VNDR_RWRD_SEQ_NO",
            F.col("svSrcSysCd").alias("SRC_SYS_CD"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("svSK").alias("VBB_RWRD_SK")
        )
    )

    file_path_updt = f"{adls_path}/VbbRwrdPK_updt.parquet"
    write_files(
        df_updt,
        file_path_updt,
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote="\"",
        nullValue=None
    )

    # --------------------------------------------------------------
    # NewKeys stream  ->  K_VBB_RWRD.dat  (Sequential file)
    # --------------------------------------------------------------
    df_NewKeys = (
        df_enriched
        .filter(F.col("svInstUpdt") == F.lit("I"))
        .select(
            "VBB_VNDR_UNIQ_KEY",
            "VBB_VNDR_RWRD_SEQ_NO",
            "SRC_SYS_CD_SK",
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("svSK").alias("VBB_RWRD_SK")
        )
    )

    file_path_newkeys = f"{adls_path}/load/K_VBB_RWRD.dat"
    write_files(
        df_NewKeys,
        file_path_newkeys,
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote="\"",
        nullValue=None
    )

    # --------------------------------------------------------------
    # Keys stream (for merge)
    # --------------------------------------------------------------
    df_Keys = (
        df_enriched
        .select(
            F.col("svSK").alias("VBB_RWRD_SK"),
            "VBB_VNDR_UNIQ_KEY",
            "VBB_VNDR_RWRD_SEQ_NO",
            "SRC_SYS_CD_SK",
            F.col("svSrcSysCd").alias("SRC_SYS_CD"),
            F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
        )
    )

    # ------------------------------------------------------------------
    # Merge stage: join df_AllColOut with df_Keys  (left join per rule)
    # ------------------------------------------------------------------
    join_expr = [
        df_AllColOut["VBB_VNDR_UNIQ_KEY"] == df_Keys["VBB_VNDR_UNIQ_KEY"],
        df_AllColOut["VBB_VNDR_RWRD_SEQ_NO"] == df_Keys["VBB_VNDR_RWRD_SEQ_NO"],
        df_AllColOut["SRC_SYS_CD_SK"] == df_Keys["SRC_SYS_CD_SK"]
    ]

    df_Key = (
        df_AllColOut.alias("all")
        .join(df_Keys.alias("k"), join_expr, "left")
        .select(
            F.col("all.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
            F.col("k.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
            F.col("all.DISCARD_IN").alias("DISCARD_IN"),
            F.col("all.PASS_THRU_IN").alias("PASS_THRU_IN"),
            F.col("all.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
            F.col("all.ERR_CT").alias("ERR_CT"),
            F.col("all.RECYCLE_CT").alias("RECYCLE_CT"),
            F.col("k.SRC_SYS_CD").alias("SRC_SYS_CD"),
            F.col("all.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
            F.col("k.VBB_RWRD_SK").alias("VBB_RWRD_SK"),
            F.col("all.VBB_VNDR_UNIQ_KEY").alias("VBB_VNDR_UNIQ_KEY"),
            F.col("all.VBB_VNDR_RWRD_SEQ_NO").alias("VBB_VNDR_RWRD_SEQ_NO"),
            F.col("all.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
            F.col("k.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("all.VBB_RWRD_BEG_DT_SK").alias("VBB_RWRD_BEG_DT_SK"),
            F.col("all.VBB_RWRD_END_DT_SK").alias("VBB_RWRD_END_DT_SK"),
            F.col("all.SRC_SYS_CRT_DTM").alias("SRC_SYS_CRT_DTM"),
            F.col("all.SRC_SYS_UPDT_DTM").alias("SRC_SYS_UPDT_DTM"),
            F.col("all.VBB_RWRD_DESC").alias("VBB_RWRD_DESC"),
            F.col("all.VBB_RWRD_TYP_NM").alias("VBB_RWRD_TYP_NM"),
            F.col("all.VBB_VNDR_NM").alias("VBB_VNDR_NM")
        )
    )

    # ------------------------------------------------------------------
    # Return container output (Key)
    # ------------------------------------------------------------------
    return df_Key
