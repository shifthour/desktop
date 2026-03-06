# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

from pyspark.sql import DataFrame, functions as F

def run_ClmLnLoadPK(df_ClmPK: DataFrame, params: dict) -> DataFrame:
    CurrRunCycle = params["CurrRunCycle"]
    IDSOwner = params["IDSOwner"]
    SrcSysCd = params["SrcSysCd"]
    ids_jdbc_url = params["ids_jdbc_url"]
    ids_jdbc_props = params["ids_jdbc_props"]
    adls_path = params["adls_path"]
    adls_path_raw = params["adls_path_raw"]
    adls_path_publish = params["adls_path_publish"]
    ids_secret_name = params.get("ids_secret_name", "")
    truncate_sql = f"TRUNCATE TABLE {IDSOwner}.K_CLM_LN_TEMP IMMEDIATE"
    execute_dml(truncate_sql, ids_jdbc_url, ids_jdbc_props)
    (
        df_ClmPK.write.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.K_CLM_LN_TEMP")
        .mode("append")
        .save()
    )
    runstat_sql = f"CALL SYSPROC.ADMIN_CMD('runstats on table {IDSOwner}.K_CLM_LN_TEMP on key columns with distribution on key columns and detailed indexes all allow write access')"
    execute_dml(runstat_sql, ids_jdbc_url, ids_jdbc_props)
    extract_query = f"""
    SELECT k.CLM_LN_SK,
           w.SRC_SYS_CD_SK,
           w.CLM_ID,
           w.CLM_LN_SEQ_NO,
           k.CRT_RUN_CYC_EXCTN_SK
    FROM {IDSOwner}.K_CLM_LN_TEMP w
    JOIN {IDSOwner}.K_CLM_LN k
      ON w.SRC_SYS_CD_SK = k.SRC_SYS_CD_SK
     AND w.CLM_ID = k.CLM_ID
     AND w.CLM_LN_SEQ_NO = k.CLM_LN_SEQ_NO
    UNION
    SELECT -1,
           w2.SRC_SYS_CD_SK,
           w2.CLM_ID,
           w2.CLM_LN_SEQ_NO,
           {CurrRunCycle}
    FROM {IDSOwner}.K_CLM_LN_TEMP w2
    WHERE NOT EXISTS (
        SELECT 1
        FROM {IDSOwner}.K_CLM_LN k2
        WHERE w2.SRC_SYS_CD_SK = k2.SRC_SYS_CD_SK
          AND w2.CLM_ID = k2.CLM_ID
          AND w2.CLM_LN_SEQ_NO = k2.CLM_LN_SEQ_NO
    )
    """
    df_W_Extract = (
        spark.read.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("query", extract_query)
        .load()
    )
    df_enriched = df_W_Extract.withColumn(
        "svInstUpdt",
        F.when(F.col("CLM_LN_SK") == -1, F.lit("I")).otherwise(F.lit("U")),
    ).withColumn(
        "svSK",
        F.when(F.col("CLM_LN_SK") == -1, F.lit(None)).otherwise(F.col("CLM_LN_SK")),
    )
    df_enriched = SurrogateKeyGen(df_enriched,"<DB sequence name>",'svSK',"<schema>","<secret_name>")
    df_enriched = df_enriched.withColumn(
        "svCrtRunCycExctnSk",
        F.when(F.col("svInstUpdt") == "I", F.lit(CurrRunCycle)).otherwise(
            F.col("CRT_RUN_CYC_EXCTN_SK")
        ),
    )
    df_updt = df_enriched.select(
        F.lit(SrcSysCd).alias("SRC_SYS_CD"),
        "CLM_ID",
        "CLM_LN_SEQ_NO",
        F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("svSK").alias("CLM_LN_SK"),
    )
    df_NewKeys = (
        df_enriched.filter(F.col("svInstUpdt") == "I")
        .select(
            "SRC_SYS_CD_SK",
            "CLM_ID",
            "CLM_LN_SEQ_NO",
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("svSK").alias("CLM_LN_SK"),
        )
    )
    write_files(
        df_NewKeys,
        f"{adls_path}/load/K_CLM_LN.dat",
        delimiter=",",
        mode="append",
        is_pqruet=False,
        header=False,
        quote='"',
        nullValue=None,
    )
    df_PK_Clms = dedup_sort(
        df_updt,
        ["SRC_SYS_CD", "CLM_ID", "CLM_LN_SEQ_NO"],
        [("CRT_RUN_CYC_EXCTN_SK", "D")],
    )
    return df_PK_Clms