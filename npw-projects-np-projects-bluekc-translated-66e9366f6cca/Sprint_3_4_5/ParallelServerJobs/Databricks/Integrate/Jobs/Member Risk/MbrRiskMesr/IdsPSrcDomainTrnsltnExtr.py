# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2010 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY : McSourceMbrRiskExtrSeq
# MAGIC 
# MAGIC PROCESSING: Extract data from the UWS table SRC_DOMAIN_TRNSLTN to load into IDS
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                       Date               Project/Altiris #               Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------   ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Kalyan Neelam                2010-07-19       4297                             Original Programming                           RebuildIntNewDevl      Steph Goddard            07/21/2010

# MAGIC IDS P_SRC_DOMAIN_TRNSLTN Extract
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


UWSOwner = get_widget_value('UWSOwner','')
uws_secret_name = get_widget_value('uws_secret_name','')

jdbc_url, jdbc_props = get_db_config(uws_secret_name)

extract_query = f"SELECT SRC_SYS_CD, DOMAIN_ID, SRC_DOMAIN_TX, EFF_DT_SK, TERM_DT_SK, TRGT_DOMAIN_TX, LAST_UPDT_USER_ID, LAST_UPDT_DT_SK FROM {UWSOwner}.SRC_DOMAIN_TRNSLTN WHERE TERM_DT_SK = '2199-12-31'"

df_UWS_SRC_DOMAIN_TRNSLTN = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_Transformer_0 = df_UWS_SRC_DOMAIN_TRNSLTN.select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("DOMAIN_ID").alias("DOMAIN_ID"),
    col("SRC_DOMAIN_TX").alias("SRC_DOMAIN_TX"),
    col("TRGT_DOMAIN_TX").alias("TRGT_DOMAIN_TX"),
    col("LAST_UPDT_USER_ID").alias("LAST_UPDT_USER_ID"),
    col("LAST_UPDT_DT_SK").alias("LAST_UPDT_DT_SK")
)

df_Transformer_0 = df_Transformer_0.withColumn("LAST_UPDT_DT_SK", rpad("LAST_UPDT_DT_SK", 10, " "))

df_final = df_Transformer_0.select(
    "SRC_SYS_CD",
    "DOMAIN_ID",
    "SRC_DOMAIN_TX",
    "TRGT_DOMAIN_TX",
    "LAST_UPDT_USER_ID",
    "LAST_UPDT_DT_SK"
)

write_files(
    df_final,
    f"{adls_path}/load/P_SRC_DOMAIN_TRNSLTN.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)