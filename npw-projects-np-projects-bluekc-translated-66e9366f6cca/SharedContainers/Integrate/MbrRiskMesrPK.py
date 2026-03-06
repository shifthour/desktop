# COMMAND ----------
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def run_MbrRiskMesrPK(df_AllCol: DataFrame, df_Transform: DataFrame, params: dict) -> DataFrame:
    CurrRunCycle = params["CurrRunCycle"]
    SrcSysCd = params["SrcSysCd"]
    IDSOwner = params["IDSOwner"]
    ids_secret_name = params["ids_secret_name"]
    ids_jdbc_url = params["ids_jdbc_url"]
    ids_jdbc_props = params["ids_jdbc_props"]
    adls_path = params["adls_path"]
    adls_path_raw = params["adls_path_raw"]
    adls_path_publish = params["adls_path_publish"]
    df_AllColOut = dedup_sort(df_AllCol, ["SRC_SYS_CD_SK", "MBR_UNIQ_KEY", "RISK_CAT_ID", "PRCS_YR_MO_SK"], [("SRC_SYS_CD_SK", "D")])
    df_DSLink69 = dedup_sort(df_Transform, ["SRC_SYS_CD_SK", "MBR_UNIQ_KEY", "RISK_CAT_ID", "PRCS_YR_MO_SK"], [("SRC_SYS_CD_SK", "D")])
    execute_dml(f"DROP TABLE IF EXISTS {IDSOwner}.K_MBR_RISK_MESR_TEMP", ids_jdbc_url, ids_jdbc_props)
    execute_dml(f"""CREATE TABLE {IDSOwner}.K_MBR_RISK_MESR_TEMP (
SRC_SYS_CD_SK INTEGER NOT NULL,
MBR_UNIQ_KEY INTEGER NOT NULL,
RISK_CAT_ID VARCHAR(20) NOT NULL,
PRCS_YR_MO_SK CHAR(6) NOT NULL,
PRIMARY KEY (SRC_SYS_CD_SK, MBR_UNIQ_KEY, RISK_CAT_ID, PRCS_YR_MO_SK)
)""", ids_jdbc_url, ids_jdbc_props)
    (df_DSLink69.write.format("jdbc").option("url", ids_jdbc_url).options(**ids_jdbc_props).option("dbtable", f"{IDSOwner}.K_MBR_RISK_MESR_TEMP").mode("append").save())
    extract_query = f"""
SELECT k.MBR_RISK_MESR_SK,
       w.SRC_SYS_CD_SK,
       w.MBR_UNIQ_KEY,
       w.RISK_CAT_ID,
       w.PRCS_YR_MO_SK,
       k.CRT_RUN_CYC_EXCTN_SK
FROM {IDSOwner}.K_MBR_RISK_MESR_TEMP w
JOIN {IDSOwner}.K_MBR_RISK_MESR k
ON w.SRC_SYS_CD_SK = k.SRC_SYS_CD_SK
AND w.MBR_UNIQ_KEY = k.MBR_UNIQ_KEY
AND w.PRCS_YR_MO_SK = k.PRCS_YR_MO_SK
AND w.RISK_CAT_ID = k.RISK_CAT_ID
UNION
SELECT -1,
       w2.SRC_SYS_CD_SK,
       w2.MBR_UNIQ_KEY,
       w2.RISK_CAT_ID,
       w2.PRCS_YR_MO_SK,
       {CurrRunCycle}
FROM {IDSOwner}.K_MBR_RISK_MESR_TEMP w2
WHERE NOT EXISTS (
    SELECT 1
    FROM {IDSOwner}.K_MBR_RISK_MESR k2
    WHERE w2.SRC_SYS_CD_SK = k2.SRC_SYS_CD_SK
      AND w2.MBR_UNIQ_KEY = k2.MBR_UNIQ_KEY
      AND w2.PRCS_YR_MO_SK = k2.PRCS_YR_MO_SK
      AND w2.RISK_CAT_ID = k2.RISK_CAT_ID
)
"""
    df_W_Extract = spark.read.format("jdbc").option("url", ids_jdbc_url).options(**ids_jdbc_props).option("query", extract_query).load()
    df_enriched = df_W_Extract.withColumn("svInstUpdt", F.when(F.col("MBR_RISK_MESR_SK") == F.lit(-1), F.lit("I")).otherwise(F.lit("U")))
    df_enriched = df_enriched.withColumn("svSK", F.when(F.col("svInstUpdt") == "I", F.lit(None)).otherwise(F.col("MBR_RISK_MESR_SK")))
    df_enriched = df_enriched.withColumn("svCrtRunCycExctnSk", F.when(F.col("svInstUpdt") == "I", F.lit(CurrRunCycle)).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK")))
    df_enriched = df_enriched.withColumn("svRiskCatId", F.trim(F.col("RISK_CAT_ID")))
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"svSK",<schema>,<secret_name>)
    df_updt = df_enriched.select(F.lit(SrcSysCd).alias("SRC_SYS_CD"), F.col("MBR_UNIQ_KEY"), F.col("svRiskCatId").alias("RISK_CAT_ID"), F.col("PRCS_YR_MO_SK"), F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"), F.col("svSK").alias("MBR_RISK_MESR_SK"))
    df_NewKeys = df_enriched.filter(F.col("svInstUpdt") == "I").select(F.col("SRC_SYS_CD_SK"), F.col("MBR_UNIQ_KEY"), F.col("svRiskCatId").alias("RISK_CAT_ID"), F.col("PRCS_YR_MO_SK"), F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"), F.col("svSK").alias("MBR_RISK_MESR_SK"))
    df_Keys = df_enriched.select(F.col("svSK").alias("MBR_RISK_MESR_SK"), F.col("SRC_SYS_CD_SK"), F.lit(SrcSysCd).alias("SRC_SYS_CD"), F.col("MBR_UNIQ_KEY"), F.col("PRCS_YR_MO_SK"), F.col("svRiskCatId").alias("RISK_CAT_ID"), F.col("svInstUpdt").alias("INSRT_UPDT_CD"), F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"))
    write_files(df_updt, f"{adls_path}/MbrRiskMesrPK_updt.parquet", delimiter=",", mode="overwrite", is_pqruet=True, header=True, quote="\"", nullValue=None)
    write_files(df_NewKeys, f"{adls_path}/load/K_MBR_RISK_MESR.dat", delimiter=",", mode="overwrite", is_pqruet=False, header=False, quote="\"", nullValue=None)
    join_cond = (F.col("all.SRC_SYS_CD_SK") == F.col("k.SRC_SYS_CD_SK")) & (F.col("all.MBR_UNIQ_KEY") == F.col("k.MBR_UNIQ_KEY")) & (F.col("all.RISK_CAT_ID") == F.col("k.RISK_CAT_ID")) & (F.col("all.PRCS_YR_MO_SK") == F.col("k.PRCS_YR_MO_SK"))
    df_Key = df_AllColOut.alias("all").join(df_Keys.alias("k"), join_cond, "left").select(
        F.col("all.JOB_EXCTN_RCRD_ERR_SK"),
        F.col("k.INSRT_UPDT_CD"),
        F.col("all.DISCARD_IN"),
        F.col("all.PASS_THRU_IN"),
        F.col("all.FIRST_RECYC_DT"),
        F.col("all.ERR_CT"),
        F.col("all.RECYCLE_CT"),
        F.col("k.SRC_SYS_CD"),
        F.col("all.PRI_KEY_STRING"),
        F.col("k.MBR_RISK_MESR_SK"),
        F.col("all.MBR_UNIQ_KEY"),
        F.col("all.RISK_CAT_ID"),
        F.col("all.PRCS_YR_MO_SK"),
        F.col("k.CRT_RUN_CYC_EXCTN_SK"),
        F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("all.MBR_CK"),
        F.col("all.MBR_MED_MESRS_CD"),
        F.col("all.RISK_CAT_SK"),
        F.col("all.FTR_RELTV_RISK_NO"),
        F.col("all.RISK_SVRTY_CD"),
        F.col("all.RISK_IDNT_DT"),
        F.col("all.MCSRC_RISK_LVL_NO"),
        F.col("all.MDCSN_RISK_SCORE_NO"),
        F.col("all.MDCSN_HLTH_STTUS_MESR_NO"),
        F.col("all.CRG_ID"),
        F.col("all.CRG_DESC"),
        F.col("all.AGG_CRG_BASE_3_ID"),
        F.col("all.AGG_CRG_BASE_3_DESC"),
        F.col("all.CRG_WT"),
        F.col("all.CRG_MDL_ID"),
        F.col("all.CRG_VRSN_ID"),
        F.col("all.INDV_BE_MARA_RSLT_SK"),
        F.col("all.RISK_MTHDLGY_CD"),
        F.col("all.RISK_MTHDLGY_TYP_CD"),
        F.col("all.RISK_MTHDLGY_TYP_SCORE_NO")
    )
    return df_Key
# COMMAND ----------