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
# MAGIC Kalyan Neelam                 2010-08-11                   Original Programming                                                      4297              RebuildIntNewDevl           Steph Goddard           08/12/2010

# MAGIC Writing Sequential File to /pkey
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, when, concat, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------

# MAGIC %run ../../../../shared_containers/PrimaryKey/RiskCatPK
# COMMAND ----------

CurrRunCycle = get_widget_value("CurrRunCycle","155")
RunID = get_widget_value("RunID","")
RunDate = get_widget_value("RunDate","")
SourceSk = get_widget_value("SourceSk","")
SrcSysCd = get_widget_value("SrcSysCd","")
IDSOwner = get_widget_value("IDSOwner","$PROJDEF")
UWSOwner = get_widget_value("UWSOwner","$PROJDEF")
ids_secret_name = get_widget_value("ids_secret_name","")
uws_secret_name = get_widget_value("uws_secret_name","")

jdbc_url, jdbc_props = get_db_config(uws_secret_name)
extract_query = f"SELECT RISK_CAT.SRC_SYS_CD, RISK_CAT.RISK_CAT_ID, RISK_CAT.MAJ_PRCTC_CAT_CD, RISK_CAT.MAJ_PRCTC_CAT_DESC, RISK_CAT.RISK_CAT_DESC, RISK_CAT.RISK_CAT_LABEL, RISK_CAT.RISK_CAT_LONG_DESC, RISK_CAT.RISK_CAT_NM FROM {UWSOwner}.RISK_CAT"
df_UWS_RISK_CAT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_strip = df_UWS_RISK_CAT.select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("RISK_CAT_ID").alias("RISK_CAT_ID"),
    col("MAJ_PRCTC_CAT_CD").alias("MAJ_PRCTC_CAT_CD"),
    trim(UpCase(col("MAJ_PRCTC_CAT_DESC"))).alias("MAJ_PRCTC_CAT_DESC"),
    trim(UpCase(col("RISK_CAT_DESC"))).alias("RISK_CAT_DESC"),
    col("RISK_CAT_LABEL").alias("RISK_CAT_LABEL"),
    trim(UpCase(col("RISK_CAT_LONG_DESC"))).alias("RISK_CAT_LONG_DESC"),
    trim(UpCase(col("RISK_CAT_NM"))).alias("RISK_CAT_NM")
)

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"SELECT TRGT_CD, CD_MPPNG_SK FROM {IDSOwner}.CD_MPPNG WHERE TRGT_DOMAIN_NM = 'SOURCE SYSTEM'"
df_CD_MPPNG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_CD_MPPNG_renamed = df_CD_MPPNG.select(
    col("TRGT_CD").alias("SRC_SYS_CD"),
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
)

df_hf_uwsriskcat_srcsyscdsk = dedup_sort(df_CD_MPPNG_renamed, ["SRC_SYS_CD"], [])

df_businessrules = (
    df_strip.alias("Strip")
    .join(
        df_hf_uwsriskcat_srcsyscdsk.alias("SrcCdLkup"),
        col("Strip.SRC_SYS_CD") == col("SrcCdLkup.SRC_SYS_CD"),
        "left"
    )
    .select("Strip.*", col("SrcCdLkup.CD_MPPNG_SK").alias("CD_MPPNG_SK"))
    .withColumn("RowPassThru", lit("Y"))
)

df_allcol = df_businessrules.select(
    when(col("CD_MPPNG_SK").isNull(), lit(0)).otherwise(col("CD_MPPNG_SK")).alias("SRC_SYS_CD_SK"),
    col("RISK_CAT_ID").alias("RISK_CAT_ID"),
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    lit("I").alias("INSRT_UPDT_CD"),
    lit("N").alias("DISCARD_IN"),
    col("RowPassThru").alias("PASS_THRU_IN"),
    lit(RunDate).alias("FIRST_RECYC_DT"),
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    concat(col("SRC_SYS_CD"), lit(";"), col("RISK_CAT_ID")).alias("PRI_KEY_STRING"),
    lit(0).alias("RISK_CAT_SK"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("MAJ_PRCTC_CAT_CD").alias("MAJ_PRCTC_CAT_CD"),
    col("RISK_CAT_DESC").alias("RISK_CAT_DESC"),
    col("RISK_CAT_LABEL").alias("RISK_CAT_LABEL"),
    col("RISK_CAT_LONG_DESC").alias("RISK_CAT_LONG_DESC"),
    col("RISK_CAT_NM").alias("RISK_CAT_NM")
)

df_transform = df_businessrules.select(
    when(col("CD_MPPNG_SK").isNull(), lit(0)).otherwise(col("CD_MPPNG_SK")).alias("SRC_SYS_CD_SK"),
    col("RISK_CAT_ID").alias("RISK_CAT_ID")
)

params = {
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd,
    "RunID": RunID,
    "CurrentDate": RunDate,
    "IDSOwner": IDSOwner
}
df_Key = RiskCatPK(df_allcol, df_transform, params)

df_final = df_Key.select(
    col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    col("ERR_CT").alias("ERR_CT"),
    col("RECYCLE_CT").alias("RECYCLE_CT"),
    rpad(col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    col("RISK_CAT_SK").alias("RISK_CAT_SK"),
    col("RISK_CAT_ID").alias("RISK_CAT_ID"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(col("MAJ_PRCTC_CAT_CD"), <...>, " ").alias("MAJ_PRCTC_CAT_CD"),
    rpad(col("RISK_CAT_DESC"), <...>, " ").alias("RISK_CAT_DESC"),
    rpad(col("RISK_CAT_LABEL"), <...>, " ").alias("RISK_CAT_LABEL"),
    rpad(col("RISK_CAT_LONG_DESC"), <...>, " ").alias("RISK_CAT_LONG_DESC"),
    rpad(col("RISK_CAT_NM"), <...>, " ").alias("RISK_CAT_NM")
)

write_files(
    df_final,
    f"{adls_path}/key/UwsRiskCatExtr.RiskCat.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)