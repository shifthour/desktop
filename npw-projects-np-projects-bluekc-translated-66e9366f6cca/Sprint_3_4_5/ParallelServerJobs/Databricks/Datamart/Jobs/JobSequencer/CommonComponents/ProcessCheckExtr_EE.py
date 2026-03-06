# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC               B Leland           03/22/2006  -    Initial programming
# MAGIC               B Leland           08/14/2006  -     Changed SQL logic to correctly see jobs running.
# MAGIC               B Leland           10/30/2006  -     Added additional SQL to union statement to look for jobs extracting from the system being updated.
# MAGIC 
# MAGIC Leandrerw Moore              2013-07-01      5114                        rewrite  in parallel                                          EnterpriseWrshDevl      Bhoomi Dasari             8/15/2013

# MAGIC Write out processing statistics to file
# MAGIC 
# MAGIC Job Name:
# MAGIC IdsEdwfProcessCheckExtr
# MAGIC Select data from P_RUN_CYC table based on job parameters
# MAGIC Records are written to peak
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
# MAGIC %run ../../../../Utility_DataMart3
# COMMAND ----------


IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
Subject = get_widget_value("Subject","")
SourceSys = get_widget_value("SourceSys","")
TargetSys = get_widget_value("TargetSys","")

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query_db2_RUN_CYCLE = f"""select SUBJ_CD, TRGT_SYS_CD, SRC_SYS_CD
  from {IDSOwner}.P_RUN_CYC
where CYC_STTUS_CD = 'RUNNING'
   and SUBJ_CD = '{Subject}'
   and TRGT_SYS_CD =  '{TargetSys}'

UNION

select SUBJ_CD, TRGT_SYS_CD, SRC_SYS_CD
  from {IDSOwner}.P_RUN_CYC
where CYC_STTUS_CD = 'RUNNING'
   and SUBJ_CD = '{Subject}'
   and SRC_SYS_CD <> '{SourceSys}'
   and TRGT_SYS_CD = '{TargetSys}'

UNION

select SUBJ_CD, TRGT_SYS_CD, SRC_SYS_CD
  from {IDSOwner}.P_RUN_CYC
where CYC_STTUS_CD = 'RUNNING'
  and SUBJ_CD = '{Subject}'
   and SRC_SYS_CD = '{SourceSys}'"""

df_db2_RUN_CYCLE = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_RUN_CYCLE)
    .load()
)

df_Trans1 = df_db2_RUN_CYCLE.select(
    F.col("SUBJ_CD").alias("SUBJ_CD"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("TRGT_SYS_CD").alias("TRGT_SYS_CD")
)

df_peek_process_run_check = df_Trans1
df_peek_process_run_check.show(10)