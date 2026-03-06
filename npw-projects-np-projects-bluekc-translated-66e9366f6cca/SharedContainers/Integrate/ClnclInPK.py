# COMMAND ----------
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
"""
JobName: ClnclInPK
JobType: Server Job
JobCategory: DS_Integrate
FolderPath: Shared Containers/PrimaryKey

DESCRIPTION:
Shared container used for Primary Keying of Clinical Indicator job
CALLED BY : ImpProClnclInExtr

MODIFICATIONS:
Developer        Date         Change Description
Bhoomi Dasari    2008-08-25   Initial program
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit, trim
from pyspark.sql import Window

# COMMAND ----------
def run_ClnclInPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    # ---- Parameter unpacking ----
    CurrRunCycle      = params["CurrRunCycle"]
    SrcSysCd          = params["SrcSysCd"]
    IDSOwner          = params["IDSOwner"]
    ids_secret_name   = params["ids_secret_name"]
    ids_jdbc_url      = params["ids_jdbc_url"]
    ids_jdbc_props    = params["ids_jdbc_props"]
    adls_path         = params["adls_path"]
    adls_path_raw     = params["adls_path_raw"]
    adls_path_publish = params["adls_path_publish"]
    # ---- Database temp-table handling ----
    temp_table = f"{IDSOwner}.K_CLNCL_IN_TEMP"
    execute_dml(f"TRUNCATE TABLE {temp_table}", ids_jdbc_url, ids_jdbc_props)
    (
        df_Transform.select("SRC_SYS_CD_SK", "CLNCL_IN_ID")
        .write.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", temp_table)
        .mode("append")
        .save()
    )
    execute_dml(
        f"CALL SYSPROC.ADMIN_CMD('runstats on table {temp_table} on key columns with distribution on key columns and detailed indexes all allow write access')",
        ids_jdbc_url,
        ids_jdbc_props
    )
    # ---- Extract W_Extract ----
    extract_query = f"""
    SELECT  k.CLNCL_IN_SK,
            w.SRC_SYS_CD_SK,
            w.CLNCL_IN_ID,
            k.CRT_RUN_CYC_EXCTN_SK
      FROM {IDSOwner}.K_CLNCL_IN_TEMP w
      JOIN {IDSOwner}.K_CLNCL_IN k
        ON w.SRC_SYS_CD_SK = k.SRC_SYS_CD_SK
       AND w.CLNCL_IN_ID   = k.CLNCL_IN_ID
    UNION
    SELECT  -1,
            w2.SRC_SYS_CD_SK,
            w2.CLNCL_IN_ID,
            {CurrRunCycle}
      FROM {IDSOwner}.K_CLNCL_IN_TEMP w2
     WHERE NOT EXISTS (
            SELECT 1
              FROM {IDSOwner}.K_CLNCL_IN k2
             WHERE w2.SRC_SYS_CD_SK = k2.SRC_SYS_CD_SK
               AND w2.CLNCL_IN_ID   = k2.CLNCL_IN_ID
     )
    """
    df_W_Extract = (
        spark.read.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("query", extract_query)
        .load()
    )
    # ---- Transformer: PrimaryKey ----
    df_enriched = (
        df_W_Extract
        .withColumn("svInstUpdt", when(col("CLNCL_IN_SK") == -1, lit("I")).otherwise(lit("U")))
        .withColumn("svSrcSysCd", lit("FACETS"))
        .withColumn("svClnclId", trim(col("CLNCL_IN_ID")))
        .withColumn(
            "svCrtRunCycExctnSk",
            when(col("svInstUpdt") == "I", lit(CurrRunCycle)).otherwise(col("CRT_RUN_CYC_EXCTN_SK"))
        )
        .withColumn(
            "svSK",
            when(col("svInstUpdt") == "I", lit(None)).otherwise(col("CLNCL_IN_SK"))
        )
    )
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"svSK",<schema>,<secret_name>)
    # ---- Build link outputs from transformer ----
    df_updt = df_enriched.select(
        lit(SrcSysCd).alias("SRC_SYS_CD"),
        col("svClnclId").alias("CLNCL_IN_ID"),
        col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("svSK").alias("CLNCL_IN_SK")
    )
    df_newkeys = (
        df_enriched.filter(col("svInstUpdt") == "I")
        .select(
            col("SRC_SYS_CD_SK"),
            col("svClnclId").alias("CLNCL_IN_ID"),
            col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            col("svSK").alias("CLNCL_IN_SK")
        )
    )
    df_keys = df_enriched.select(
        col("svSK").alias("CLNCL_IN_SK"),
        col("SRC_SYS_CD_SK"),
        lit(SrcSysCd).alias("SRC_SYS_CD"),
        col("svClnclId").alias("CLNCL_IN_ID"),
        col("svInstUpdt").alias("INSRT_UPDT_CD"),
        col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
    )
    # ---- Hashed file hf_clncl_in_allcol (scenario a ‑ dedup) ----
    df_allcol_dedup = dedup_sort(
        df_AllCol,
        partition_cols=["SRC_SYS_CD_SK", "CLNCL_IN_ID"],
        sort_cols=[("CRT_RUN_CYC_EXCTN_SK", "D")]
    )
    # ---- Merge transformer ----
    join_expr = [
        col("all.SRC_SYS_CD_SK") == col("k.SRC_SYS_CD_SK"),
        col("all.CLNCL_IN_ID")   == col("k.CLNCL_IN_ID")
    ]
    df_key = (
        df_allcol_dedup.alias("all")
        .join(df_keys.alias("k"), join_expr, "left")
        .select(
            col("all.JOB_EXCTN_RCRD_ERR_SK"),
            col("k.INSRT_UPDT_CD"),
            col("all.DISCARD_IN"),
            col("all.PASS_THRU_IN"),
            col("all.FIRST_RECYC_DT"),
            col("all.ERR_CT"),
            col("all.RECYCLE_CT"),
            col("k.SRC_SYS_CD"),
            col("all.PRI_KEY_STRING"),
            col("k.CLNCL_IN_SK"),
            col("all.CLNCL_IN_ID"),
            col("k.CRT_RUN_CYC_EXCTN_SK"),
            lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            col("all.BCBSKC_CLNCL_PGM_TYP_CD"),
            col("all.CLNCL_IN_GNRL_CAT_CD"),
            col("all.CLNCL_IN_PRI_DSS_CD"),
            col("all.SH_DESC")
        )
    )
    # ---- Parquet and sequential file outputs ----
    parquet_path_updt = f"{adls_path}/ClnclInPK_updt.parquet"
    write_files(
        df_updt,
        parquet_path_updt,
        mode="overwrite",
        is_pqruet=True
    )
    seq_file_path = f"{adls_path}/load/K_CLNCL_IN.dat"
    write_files(
        df_newkeys,
        seq_file_path,
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False
    )
    return df_key
# COMMAND ----------