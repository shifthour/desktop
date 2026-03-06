# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
from pyspark.sql import DataFrame, functions as F
from typing import Tuple

def run_MbrExchPK(df_AllCol: DataFrame, df_Transform: DataFrame, params: dict) -> DataFrame:
    CurrRunCycle = params["CurrRunCycle"]
    IDSOwner = params["IDSOwner"]
    ids_secret_name = params["ids_secret_name"]
    ids_jdbc_url = params["ids_jdbc_url"]
    ids_jdbc_props = params["ids_jdbc_props"]
    adls_path = params["adls_path"]
    adls_path_raw = params["adls_path_raw"]
    adls_path_publish = params["adls_path_publish"]
    df_AllColOut = dedup_sort(df_AllCol, ["MBR_UNIQ_KEY", "EFF_DT_SK", "SRC_SYS_CD"], [("MBR_UNIQ_KEY", "A")])
    df_Ktemp = dedup_sort(df_Transform, ["MBR_UNIQ_KEY", "EFF_DT_SK", "SRC_SYS_CD"], [("MBR_UNIQ_KEY", "A")])
    before_sql = f"CALL {IDSOwner}.BCSP_TRUNCATE('{IDSOwner}', 'K_MBR_EXCH_TEMP')"
    execute_dml(before_sql, ids_jdbc_url, ids_jdbc_props)
    df_Ktemp.write.format("jdbc").option("url", ids_jdbc_url).options(**ids_jdbc_props).option("dbtable", f"{IDSOwner}.K_MBR_EXCH_TEMP").mode("append").save()
    after_sql = f"CALL SYSPROC.ADMIN_CMD('runstats on table {IDSOwner}.K_MBR_CARE_OPP_TEMP on key columns with distribution on key columns and detailed indexes all allow write access')"
    execute_dml(after_sql, ids_jdbc_url, ids_jdbc_props)
    extract_query = f"""SELECT k.MBR_EXCH_SK,
       w.SRC_SYS_CD,
       w.MBR_UNIQ_KEY,
       w.EFF_DT_SK,
       k.CRT_RUN_CYC_EXCTN_SK
FROM {IDSOwner}.K_MBR_EXCH_TEMP w,
     {IDSOwner}.K_MBR_EXCH k
WHERE w.SRC_SYS_CD = k.SRC_SYS_CD
  AND w.MBR_UNIQ_KEY = k.MBR_UNIQ_KEY
  AND w.EFF_DT_SK = k.EFF_DT_SK
UNION
SELECT -1,
       w2.SRC_SYS_CD,
       w2.MBR_UNIQ_KEY,
       w2.EFF_DT_SK,
       {CurrRunCycle}
FROM {IDSOwner}.K_MBR_EXCH_TEMP w2
WHERE NOT EXISTS (
    SELECT k2.MBR_EXCH_SK
    FROM {IDSOwner}.K_MBR_EXCH k2
    WHERE w2.SRC_SYS_CD = k2.SRC_SYS_CD
      AND w2.MBR_UNIQ_KEY = k2.MBR_UNIQ_KEY
      AND w2.EFF_DT_SK = k2.EFF_DT_SK
)"""
    df_extract = spark.read.format("jdbc").option("url", ids_jdbc_url).options(**ids_jdbc_props).option("query", extract_query).load()
    df_enriched = df_extract.withColumn("svInstUpdt", F.when(F.col("MBR_EXCH_SK") == F.lit(-1), F.lit("I")).otherwise(F.lit("U"))).withColumn("svSrcSysCd", F.lit("FACETS")).withColumn("svSK", F.when(F.col("svInstUpdt") == "I", F.lit(None)).otherwise(F.col("MBR_EXCH_SK"))).withColumn("svCrtRunCycExctnSk", F.when(F.col("svInstUpdt") == "I", F.lit(CurrRunCycle)).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK")))
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,'svSK',<schema>,<secret_name>)
    df_updt = df_enriched.select("MBR_UNIQ_KEY", "EFF_DT_SK", F.col("svSrcSysCd").alias("SRC_SYS_CD"), F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"), F.col("svSK").alias("MBR_CARE_OPP_SK"))
    df_newKeys = df_enriched.filter(F.col("svInstUpdt") == "I").select("MBR_UNIQ_KEY", "EFF_DT_SK", F.col("svSrcSysCd").alias("SRC_SYS_CD"), F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"), F.col("svSK").alias("MBR_EXCH_SK"))
    df_keys = df_enriched.select(F.col("svSK").alias("MBR_EXCH_SK"), "MBR_UNIQ_KEY", "EFF_DT_SK", F.col("svSrcSysCd").alias("SRC_SYS_CD"), F.col("svInstUpdt").alias("INSRT_UPDT_CD"), F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"))
    parquet_path = f"{adls_path}/MbrExchPK_updt.parquet"
    write_files(df_updt, parquet_path, delimiter=",", mode="overwrite", is_pqruet=True, header=True, quote="\"", nullValue=None)
    seq_path = f"{adls_path}/load/K_MBR_EXCH.dat"
    write_files(df_newKeys, seq_path, delimiter=",", mode="overwrite", is_pqruet=False, header=False, quote="\"", nullValue=None)
    df_key = df_AllColOut.alias("a").join(df_keys.alias("k"), (F.col("a.MBR_UNIQ_KEY") == F.col("k.MBR_UNIQ_KEY")) & (F.col("a.EFF_DT_SK") == F.col("k.EFF_DT_SK")) & (F.col("a.SRC_SYS_CD") == F.col("k.SRC_SYS_CD")), "left").select("a.JOB_EXCTN_RCRD_ERR_SK", "k.INSRT_UPDT_CD", "a.DISCARD_IN", "a.PASS_THRU_IN", "a.FIRST_RECYC_DT", "a.ERR_CT", "a.RECYCLE_CT", "k.SRC_SYS_CD", "a.PRI_KEY_STRING", "k.MBR_EXCH_SK", "k.MBR_UNIQ_KEY", "k.EFF_DT_SK", "a.SRC_SYS_CD_SK", "k.CRT_RUN_CYC_EXCTN_SK", F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"), "a.QHP_ID", "a.SUB_EXCH_CHAN_CD", "a.SUB_EXCH_ENR_METH_CD", "a.SUB_EXCH_TYP_CD", "a.APTC_IN", "a.SRC_SYS_LAST_UPDT_DTM", "a.TERM_DT_SK", "a.EXCH_MBR_ID", "a.EXCH_POL_ID", "a.PAYMT_TRANS_ID")
    return df_key