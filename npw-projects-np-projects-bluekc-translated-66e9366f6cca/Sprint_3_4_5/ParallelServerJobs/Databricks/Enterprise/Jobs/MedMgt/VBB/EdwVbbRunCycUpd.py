# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2013 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  EdwVbbCntl
# MAGIC 
# MAGIC PROCESSING: Run Cycle update job for VBB
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date               Project/Altiris #                  Change Description                               Development Project      Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------      ---------------------------------------------------------       ----------------------------------     ---------------------------------    -------------------------   
# MAGIC Kalyan Neelam                2013-05-11    4963 VBB Phase III           Initial Programming                                    EnterpriseNewDevl       Bhoomi Dasari              5/17/2013

# MAGIC Extract P_RUN_CYC records for IDS Health Coach with a value \"N\" in the  EDW_LOAD_IN where the run cycle is >= to the current run cycle used for extracting.
# MAGIC Set EDW_LOAD_IN to 'Y'
# MAGIC Update the P_RUN_CYC table to indicate source system data has been loaded to the EDW for each of the run cycles processed.
# MAGIC end of  EDW  Clinical Health Coach Run Cyc Update
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


# Parameters
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

# Stage: IDS (DB2Connector)
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"""
SELECT
  SRC_SYS_CD,
  TRGT_SYS_CD,
  SUBJ_CD,
  MIN(RUN_CYC_NO) AS RUN_CYC_NO
FROM {IDSOwner}.P_RUN_CYC
WHERE TRGT_SYS_CD = 'IDS'
  AND SUBJ_CD = 'VBB_PLN_CMPNT_RWRD'
  AND EDW_LOAD_IN = 'N'
GROUP BY
  SRC_SYS_CD,
  TRGT_SYS_CD,
  SUBJ_CD
"""
df_IDS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# Stage: BusinessRules (CTransformerStage)
df_BusinessRules = df_IDS.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("SUBJ_CD").alias("SUBJ_CD"),
    F.col("TRGT_SYS_CD").alias("TRGT_SYS_CD"),
    F.col("RUN_CYC_NO").alias("RUN_CYC_NO")
)

# Stage: P_RUN_CYC (CODBCStage) - replicate UPDATE logic with MERGE
df_P_RUN_CYC = df_BusinessRules

execute_dml(
    "DROP TABLE IF EXISTS STAGING.EdwVbbRunCycUpd_P_RUN_CYC_temp",
    jdbc_url,
    jdbc_props
)

(
    df_P_RUN_CYC.write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", "STAGING.EdwVbbRunCycUpd_P_RUN_CYC_temp")
    .mode("append")
    .save()
)

merge_sql = f"""
MERGE INTO {IDSOwner}.P_RUN_CYC AS T
USING STAGING.EdwVbbRunCycUpd_P_RUN_CYC_temp AS S
ON
    T.SRC_SYS_CD = S.SRC_SYS_CD
    AND T.SUBJ_CD = S.SUBJ_CD
    AND T.TRGT_SYS_CD = S.TRGT_SYS_CD
    AND T.RUN_CYC_NO >= S.RUN_CYC_NO
    AND T.EDW_LOAD_IN = 'N'
WHEN MATCHED THEN
  UPDATE SET
    T.EDW_LOAD_IN = 'Y'
;
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)