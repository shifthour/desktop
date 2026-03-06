# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2012 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  Update the P_RUN_CYC table EDW load indicator fields to show those records have been copied to the EDW.
# MAGIC       
# MAGIC         
# MAGIC MODIFICATIONS:                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              
# MAGIC Date                 Developer                Project  / TaskTracker     Change Description                                                         Environment               Reviewer                   Review
# MAGIC ------------------      ----------------------------     -----------------------------             -------------------------------------------------------------------------------       ----------------------              ----------------------            ----------------------             
# MAGIC 2012-11-13       Kalyan Neelam           4830                                 Initial Programming                                                      EnterpriseNewDevl       Bhoomi Dasari            11/29/2012

# MAGIC Set EDW_LOAD_IN to 'Y'
# MAGIC Update the P_RUN_CYC table to indicate source system data has been loaded to the EDW for each of the run cycles processed.
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


BeginCycle = get_widget_value('BeginCycle','0')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"SELECT SRC_SYS_CD, TRGT_SYS_CD, SUBJ_CD, MIN(RUN_CYC_NO) AS RUN_CYC_NO FROM {IDSOwner}.P_RUN_CYC WHERE TRGT_SYS_CD = 'IDS' AND SUBJ_CD = 'GRP_AHY_PGM' AND EDW_LOAD_IN = 'N' AND RUN_CYC_NO >= {BeginCycle} GROUP BY SRC_SYS_CD, TRGT_SYS_CD, SUBJ_CD"
df_IDS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_enriched = df_IDS.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("SUBJ_CD").alias("SUBJ_CD"),
    F.col("TRGT_SYS_CD").alias("TRGT_SYS_CD"),
    F.col("RUN_CYC_NO").alias("RUN_CYC_NO")
)

execute_dml("DROP TABLE IF EXISTS STAGING.EdwLoadGrpAhyPgmRunCycUpd_P_RUN_CYC_temp", jdbc_url, jdbc_props)

df_enriched.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.EdwLoadGrpAhyPgmRunCycUpd_P_RUN_CYC_temp") \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO {IDSOwner}.P_RUN_CYC AS T
USING STAGING.EdwLoadGrpAhyPgmRunCycUpd_P_RUN_CYC_temp AS S
ON T.SRC_SYS_CD = S.SRC_SYS_CD
   AND T.SUBJ_CD = S.SUBJ_CD
   AND T.TRGT_SYS_CD = S.TRGT_SYS_CD
   AND T.RUN_CYC_NO >= S.RUN_CYC_NO
   AND T.EDW_LOAD_IN = 'N'
WHEN MATCHED THEN
  UPDATE SET T.EDW_LOAD_IN = 'Y';
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)