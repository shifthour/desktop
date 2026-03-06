# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
"""
DataStage Shared Container: CaseDefnPK

***************************************************************************************************************************************************************
COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 

DESCRIPTION: Shared container used for Primary Keying of CaseDefn job  
CALLED BY : ImpProCaseDefnExtr

VC LOG HISTORY
^1_1 02/05/09 11:41:41 Batch  15012_42105 PROMOTE bckcetl ids20 dsadm bls for sg
^1_1 02/05/09 10:22:42 Batch  15012_37366 INIT bckcett testIDS dsadm bls for sg
^1_2 01/29/09 10:10:07 Batch  15005_36616 PROMOTE bckcett testIDS u03651 steph - primary key/balancing
^1_2 01/29/09 09:38:37 Batch  15005_34720 INIT bckcett devlIDS u03651 steffy
^1_1 01/26/09 14:40:15 Batch  15002_52819 INIT bckcett devlIDS u03651 steffy

ANNOTATIONS
- Used by ImpProCaseDefnExtr
- IDS Primary Key Container for Case Defn
- primary key hash file only contains current run keys and is cleared before writing
- update primary key table (K_CASE_DEFN) with new keys created today
- Assign primary surrogate key
- Hash file (hf_case_defn_allcol) cleared in calling job
- join primary key info with table info
- Load IDS temp. table
- Temp table is truncated before load and runstat done after load
- SQL joins temp table with key table to assign known keys
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def run_CaseDefnPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    # ------------------------------------------------------------------
    # Parameter Unpacking
    # ------------------------------------------------------------------
    CurrRunCycle       = params["CurrRunCycle"]
    SrcSysCd           = params["SrcSysCd"]
    IDSOwner           = params["IDSOwner"]
    ids_secret_name    = params["ids_secret_name"]
    ids_jdbc_url       = params["ids_jdbc_url"]
    ids_jdbc_props     = params["ids_jdbc_props"]
    adls_path          = params["adls_path"]
    adls_path_raw      = params["adls_path_raw"]
    adls_path_publish  = params["adls_path_publish"]

    # ------------------------------------------------------------------
    # Replace intermediate hash file (hf_case_defn_allcol) with deduped DF
    # ------------------------------------------------------------------
    df_AllCol_dedup = dedup_sort(
        df_AllCol,
        ["SRC_SYS_CD_SK", "CASE_DEFN_ID"],
        [("SRC_SYS_CD_SK", "A"), ("CASE_DEFN_ID", "A")]
    )

    # ------------------------------------------------------------------
    # Load data into IDS temporary table K_CASE_DEFN_TEMP
    # ------------------------------------------------------------------
    (
        df_Transform.write.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.K_CASE_DEFN_TEMP")
        .mode("overwrite")
        .save()
    )

    execute_dml(
        f"CALL SYSPROC.ADMIN_CMD('runstats on table {IDSOwner}.K_CASE_DEFN_TEMP on key columns with distribution on key columns and detailed indexes all allow write access')",
        ids_jdbc_url,
        ids_jdbc_props
    )

    # ------------------------------------------------------------------
    # Extract data (W_Extract)
    # ------------------------------------------------------------------
    extract_query = f"""
    SELECT  k.CASE_DEFN_SK   AS CASE_DEFN_SK,
            w.SRC_SYS_CD_SK  AS SRC_SYS_CD_SK,
            w.CASE_DEFN_ID   AS CASE_DEFN_ID,
            k.CRT_RUN_CYC_EXCTN_SK AS CRT_RUN_CYC_EXCTN_SK
    FROM {IDSOwner}.K_CASE_DEFN_TEMP w
    JOIN {IDSOwner}.K_CASE_DEFN k
          ON w.SRC_SYS_CD_SK = k.SRC_SYS_CD_SK
         AND w.CASE_DEFN_ID  = k.CASE_DEFN_ID
    UNION
    SELECT -1                          AS CASE_DEFN_SK,
           w2.SRC_SYS_CD_SK            AS SRC_SYS_CD_SK,
           w2.CASE_DEFN_ID             AS CASE_DEFN_ID,
           {CurrRunCycle}              AS CRT_RUN_CYC_EXCTN_SK
    FROM {IDSOwner}.K_CASE_DEFN_TEMP w2
    WHERE NOT EXISTS (
        SELECT 1
        FROM {IDSOwner}.K_CASE_DEFN k2
        WHERE w2.SRC_SYS_CD_SK = k2.SRC_SYS_CD_SK
          AND w2.CASE_DEFN_ID  = k2.CASE_DEFN_ID
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
    # PrimaryKey Transformer Logic
    # ------------------------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn("svInstUpdt",
                    F.when(F.col("CASE_DEFN_SK") == F.lit(-1), F.lit("I"))
                     .otherwise(F.lit("U")))
        .withColumn("SrcSysCd", F.lit(SrcSysCd))
        .withColumn("svCaseDefnId", F.trim(F.col("CASE_DEFN_ID")))
        .withColumn("svSK",
                    F.when(F.col("svInstUpdt") == F.lit("I"), F.lit(None))
                     .otherwise(F.col("CASE_DEFN_SK")))
        .withColumn("svCrtRunCycExctnSk",
                    F.when(F.col("svInstUpdt") == F.lit("I"), F.lit(CurrRunCycle))
                     .otherwise(F.col("CRT_RUN_CYC_EXCTN_SK")))
    )

    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,'svSK',<schema>,<secret_name>)

    # ------------------------------------------------------------------
    # Build intermediate DataFrames
    # ------------------------------------------------------------------
    df_updt = df_enriched.select(
        F.col("SrcSysCd").alias("SRC_SYS_CD"),
        F.col("svCaseDefnId").alias("CASE_DEFN_ID"),
        F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("svSK").alias("CASE_DEFN_SK")
    )

    df_NewKeys = (
        df_enriched.filter(F.col("svInstUpdt") == F.lit("I"))
        .select(
            F.col("SRC_SYS_CD_SK"),
            F.col("svCaseDefnId").alias("CASE_DEFN_ID"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("svSK").alias("CASE_DEFN_SK")
        )
    )

    df_Keys = df_enriched.select(
        F.col("svSK").alias("CASE_DEFN_SK"),
        F.col("SRC_SYS_CD_SK"),
        F.col("SrcSysCd").alias("SRC_SYS_CD"),
        F.col("svCaseDefnId").alias("CASE_DEFN_ID"),
        F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
        F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
    )

    # ------------------------------------------------------------------
    # Merge Transformer Logic
    # ------------------------------------------------------------------
    df_Key = (
        df_AllCol_dedup.alias("AllColOut")
        .join(
            df_Keys.alias("Keys"),
            (F.col("AllColOut.SRC_SYS_CD_SK") == F.col("Keys.SRC_SYS_CD_SK")) &
            (F.col("AllColOut.CASE_DEFN_ID") == F.col("Keys.CASE_DEFN_ID")),
            "left"
        )
        .select(
            F.col("AllColOut.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
            F.col("Keys.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
            F.col("AllColOut.DISCARD_IN").alias("DISCARD_IN"),
            F.col("AllColOut.PASS_THRU_IN").alias("PASS_THRU_IN"),
            F.col("AllColOut.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
            F.col("AllColOut.ERR_CT").alias("ERR_CT"),
            F.col("AllColOut.RECYCLE_CT").alias("RECYCLE_CT"),
            F.col("Keys.SRC_SYS_CD").alias("SRC_SYS_CD"),
            F.col("AllColOut.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
            F.col("Keys.CASE_DEFN_SK").alias("CASE_DEFN_SK"),
            F.col("AllColOut.CASE_DEFN_ID").alias("CASE_DEFN_ID"),
            F.col("Keys.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("AllColOut.BCBSKC_CLNCL_PGM_TYP_CD").alias("BCBSKC_CLNCL_PGM_TYP_CD"),
            F.col("AllColOut.CASE_DEFN_CAT_CD_SK").alias("CASE_DEFN_CAT_CD_SK"),
            F.col("AllColOut.SH_DESC").alias("SH_DESC")
        )
    )

    # ------------------------------------------------------------------
    # Write Outputs
    # ------------------------------------------------------------------
    write_files(
        df_NewKeys,
        f"{adls_path}/load/K_CASE_DEFN.dat",
        delimiter=',',
        mode='overwrite',
        is_pqruet=False,
        header=False,
        quote='\"',
        nullValue=None
    )

    write_files(
        df_updt,
        f"{adls_path}/CaseDefnPK_updt.parquet",
        delimiter=',',
        mode='overwrite',
        is_pqruet=True,
        header=True,
        quote='\"',
        nullValue=None
    )

    # ------------------------------------------------------------------
    # Return container output
    # ------------------------------------------------------------------
    return df_Key