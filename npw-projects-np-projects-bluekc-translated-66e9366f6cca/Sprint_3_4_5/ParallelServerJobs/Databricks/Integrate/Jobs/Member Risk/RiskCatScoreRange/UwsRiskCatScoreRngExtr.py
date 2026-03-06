# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2010 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:   Extracts data from UWS RISK_CAT and loads the IDS table RISK_CAT.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                              Date                         Change Description                                                   Project #          Development Project          Code Reviewer          Date Reviewed  
# MAGIC -----------------------                ----------------------------     ----------------------------------------------------------------------------           ------------------        ------------------------------------       ----------------------------      -------------------------
# MAGIC Krishnakanth                     2016-10-14                   Original Programming                                                   300001             IntegrateDev2                   Jag Yelavarthi            2016-11-18
# MAGIC     Manivannan

# MAGIC Writing Sequential File to /pkey
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import *
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
RunDate = get_widget_value('RunDate','')
SourceSk = get_widget_value('SourceSk','')
SrcSysCd = get_widget_value('SrcSysCd','')
IDSOwner = get_widget_value('IDSOwner','')
UWSOwner = get_widget_value('UWSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
uws_secret_name = get_widget_value('uws_secret_name','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
df_CD_MPPNG2 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"SELECT SRC_CD AS RISK_MTHDLGY_TYP_CD, CD_MPPNG_SK FROM {IDSOwner}.CD_MPPNG WHERE SRC_SYS_CD='UWS' AND SRC_CLCTN_CD='UNIFIEDWAREHOUSESPPT' AND TRGT_CLCTN_CD='IDS' AND SRC_DOMAIN_NM='RISK METHODOLOGY TYPE' AND TRGT_DOMAIN_NM=SRC_DOMAIN_NM")
    .load()
)
df_hf_RiskMethTypCd_Lkp = df_CD_MPPNG2.dropDuplicates(["RISK_MTHDLGY_TYP_CD"])

df_CD_MPPNG1 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"SELECT SRC_CD AS RISK_MTHDLGY_CD, CD_MPPNG_SK FROM {IDSOwner}.CD_MPPNG WHERE SRC_SYS_CD='UWS' AND SRC_CLCTN_CD='UNIFIEDWAREHOUSESPPT' AND TRGT_CLCTN_CD='IDS' AND SRC_DOMAIN_NM='RISK METHODOLOGY' AND TRGT_DOMAIN_NM=SRC_DOMAIN_NM")
    .load()
)
df_hf_RiskMethCd_Lkp = df_CD_MPPNG1.dropDuplicates(["RISK_MTHDLGY_CD"])

jdbc_url_uws, jdbc_props_uws = get_db_config(uws_secret_name)
df_RISK_CAT_SCORE_RNG_ex = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_uws)
    .options(**jdbc_props_uws)
    .option("query", f"SELECT SRC_SYS_CD, RISK_CAT_ID, RISK_MTHDLGY_CD, RISK_MTHDLGY_TYP_CD, RISK_CAT_LOW_SCORE_NO, RISK_CAT_HI_SCORE_NO, RISK_CAT_SCORE_RNG_EFF_DT_SK, RISK_CAT_SCORE_RNG_TERM_DT_SK, CRT_USER_ID, CRT_DT_SK, LAST_UPDT_USER_ID, LAST_UPDT_DT_SK FROM {UWSOwner}.RISK_CAT_SCORE_RNG")
    .load()
)

df_Strip = df_RISK_CAT_SCORE_RNG_ex.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("RISK_CAT_ID").alias("RISK_CAT_ID"),
    F.col("RISK_MTHDLGY_CD").alias("RISK_MTHDLGY_CD"),
    F.col("RISK_MTHDLGY_TYP_CD").alias("RISK_MTHDLGY_TYP_CD"),
    F.col("RISK_CAT_LOW_SCORE_NO").alias("RISK_CAT_LOW_SCORE_NO"),
    F.col("RISK_CAT_HI_SCORE_NO").alias("RISK_CAT_HI_SCORE_NO"),
    F.col("RISK_CAT_SCORE_RNG_EFF_DT_SK").alias("RISK_CAT_SCORE_RNG_EFF_DT_SK"),
    F.col("RISK_CAT_SCORE_RNG_TERM_DT_SK").alias("RISK_CAT_SCORE_RNG_TERM_DT_SK"),
    F.col("CRT_USER_ID").alias("CRT_USER_ID"),
    F.col("CRT_DT_SK").alias("CRT_DT_SK"),
    F.col("LAST_UPDT_USER_ID").alias("LAST_UPDT_USER_ID"),
    F.col("LAST_UPDT_DT_SK").alias("LAST_UPDT_USER_DT_SK")
)

df_BusinessRules = (
    df_Strip.alias("Strip")
    .join(
        df_hf_RiskMethTypCd_Lkp.alias("CdMpRiskMethType"),
        F.col("Strip.RISK_MTHDLGY_TYP_CD") == F.col("CdMpRiskMethType.RISK_MTHDLGY_TYP_CD"),
        "left"
    )
    .join(
        df_hf_RiskMethCd_Lkp.alias("CdMpRiskMethCd"),
        F.col("Strip.RISK_MTHDLGY_CD") == F.col("CdMpRiskMethCd.RISK_MTHDLGY_CD"),
        "left"
    )
    .select(
        F.col("Strip.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Strip.RISK_CAT_ID").alias("RISK_CAT_ID"),
        F.col("Strip.RISK_MTHDLGY_CD").alias("RISK_MTHDLGY_CD"),
        F.col("Strip.RISK_MTHDLGY_TYP_CD").alias("RISK_MTHDLGY_TYP_CD"),
        F.col("Strip.RISK_CAT_LOW_SCORE_NO").alias("RISK_CAT_LOW_SCORE_NO"),
        F.col("Strip.RISK_CAT_HI_SCORE_NO").alias("RISK_CAT_HI_SCORE_NO"),
        F.col("Strip.RISK_CAT_SCORE_RNG_EFF_DT_SK").alias("RISK_CAT_SCORE_RNG_EFF_DT_SK"),
        F.col("Strip.RISK_CAT_SCORE_RNG_TERM_DT_SK").alias("RISK_CAT_SCORE_RNG_TERM_DT_SK"),
        F.when(F.col("CdMpRiskMethCd.CD_MPPNG_SK").isNull(), F.lit(0)).otherwise(F.col("CdMpRiskMethCd.CD_MPPNG_SK")).alias("RISK_MTHDLGY_CD_SK"),
        F.when(F.col("CdMpRiskMethType.CD_MPPNG_SK").isNull(), F.lit(0)).otherwise(F.col("CdMpRiskMethType.CD_MPPNG_SK")).alias("RISK_MTHDLGY_TYP_CD_SK"),
        F.col("Strip.CRT_USER_ID").alias("CRT_USER_ID"),
        F.col("Strip.CRT_DT_SK").alias("CRT_DT_SK"),
        F.col("Strip.LAST_UPDT_USER_ID").alias("LAST_UPDT_USER_ID"),
        F.col("Strip.LAST_UPDT_USER_DT_SK").alias("LAST_UPDT_DT_SK")
    )
)

df_final = (
    df_BusinessRules
    .withColumn("RISK_MTHDLGY_CD", F.rpad(F.col("RISK_MTHDLGY_CD"), F.lit("<...>"), F.lit(" ")))
    .withColumn("RISK_MTHDLGY_TYP_CD", F.rpad(F.col("RISK_MTHDLGY_TYP_CD"), F.lit("<...>"), F.lit(" ")))
    .withColumn("RISK_CAT_SCORE_RNG_EFF_DT_SK", F.rpad(F.col("RISK_CAT_SCORE_RNG_EFF_DT_SK"), F.lit(10), F.lit(" ")))
    .withColumn("RISK_CAT_SCORE_RNG_TERM_DT_SK", F.rpad(F.col("RISK_CAT_SCORE_RNG_TERM_DT_SK"), F.lit(10), F.lit(" ")))
    .withColumn("CRT_DT_SK", F.rpad(F.col("CRT_DT_SK"), F.lit(10), F.lit(" ")))
    .withColumn("LAST_UPDT_DT_SK", F.rpad(F.col("LAST_UPDT_DT_SK"), F.lit(10), F.lit(" ")))
    .select(
        "SRC_SYS_CD",
        "RISK_CAT_ID",
        "RISK_MTHDLGY_CD",
        "RISK_MTHDLGY_TYP_CD",
        "RISK_CAT_LOW_SCORE_NO",
        "RISK_CAT_HI_SCORE_NO",
        "RISK_CAT_SCORE_RNG_EFF_DT_SK",
        "RISK_CAT_SCORE_RNG_TERM_DT_SK",
        "RISK_MTHDLGY_CD_SK",
        "RISK_MTHDLGY_TYP_CD_SK",
        "CRT_USER_ID",
        "CRT_DT_SK",
        "LAST_UPDT_USER_ID",
        "LAST_UPDT_DT_SK"
    )
)

write_files(
    df_final,
    f"{adls_path}/key/UwsRiskCatScoreRngExtr.RiskCatScoreRng.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)