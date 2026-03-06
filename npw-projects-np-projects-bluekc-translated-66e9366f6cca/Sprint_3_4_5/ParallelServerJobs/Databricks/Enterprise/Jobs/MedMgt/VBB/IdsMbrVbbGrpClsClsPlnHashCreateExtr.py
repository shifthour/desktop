# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2013 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  EdwVbbExtrLoadSeq
# MAGIC 
# MAGIC PROCESSING: Extracts data from GRP_D, CLS_D and CLS_PLN_D and loads them into hashed files to be used by VBB extract jobs
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date               Project/Altiris #                  Change Description                               Development Project      Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------      ---------------------------------------------------------       ----------------------------------     ---------------------------------    -------------------------   
# MAGIC Kalyan Neelam                2013-05-11    4963 VBB Phase III           Initial Programming                                    EnterpriseNewdevl        Bhoomi Dasari             5/17/2013

# MAGIC These Hashed files are used in IdsMbrVbbCmpntEnrFExtr and IdsMbrVbbPlnEnrFExtr. Cleared in IdsMbrVbbCmpntEnrFExtr
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')

jdbc_url, jdbc_props = get_db_config(edw_secret_name)
df_GRP_D = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"SELECT GRP_D.GRP_SK as GRP_SK,GRP_D.GRP_ID as GRP_ID FROM {EDWOwner}.GRP_D GRP_D")
    .load()
)
df_hf_mbrvbbenr_grpid = df_GRP_D.select("GRP_SK","GRP_ID")
write_files(
    df_hf_mbrvbbenr_grpid,
    f"{adls_path}/hf_mbrvbbenr_grpid.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

jdbc_url, jdbc_props = get_db_config(edw_secret_name)
df_CLS_D = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"SELECT CLS_D.CLS_SK as CLS_SK,CLS_D.CLS_ID as CLS_ID FROM {EDWOwner}.CLS_D CLS_D")
    .load()
)
df_hf_mbrvbbenr_clsid = df_CLS_D.select("CLS_SK","CLS_ID")
write_files(
    df_hf_mbrvbbenr_clsid,
    f"{adls_path}/hf_mbrvbbenr_clsid.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

jdbc_url, jdbc_props = get_db_config(edw_secret_name)
df_CLS_PLN_D = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"SELECT CLS_PLN_D.CLS_PLN_SK as CLS_PLN_SK,CLS_PLN_D.CLS_PLN_ID as CLS_PLN_ID FROM {EDWOwner}.CLS_PLN_D CLS_PLN_D")
    .load()
)
df_hf_mbrvbbenr_clsplnid = df_CLS_PLN_D.select("CLS_PLN_SK","CLS_PLN_ID")
write_files(
    df_hf_mbrvbbenr_clsplnid,
    f"{adls_path}/hf_mbrvbbenr_clsplnid.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)