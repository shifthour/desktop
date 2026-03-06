# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC ****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:     MpiBcbsIdsBekeyBalCntl
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   
# MAGIC                 The Job Updates the P_RUN_CYC table with the ROW_CT_BAL_IN set to 'Y' after balancing is done for the subject area
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Abhiram Dasarathy          01/17/2013          4426                              Originally Programmed                           IntegrateNewDevl      Sharon Andrew            2013-01-22

# MAGIC Update the P_RUN_CYC table to indicate source system data has been balanced with respect to IDS for each of the run cycles processed.
# MAGIC Set ROW_CT_BAL_IN to 'Y'
# MAGIC Extract P_RUN_CYC records for IDS Medical Management with a value \"N\" in the  ROW_CT_BAL_IN where the run cycle is >= to the current run cycle used for extracting.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


ids_secret_name = get_widget_value('ids_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
BeginCycle = get_widget_value('BeginCycle','')
SrcSys = get_widget_value('SrcSys','')
TrgtSys = get_widget_value('TrgtSys','')
SubjCd = get_widget_value('SubjCd','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = f"""
SELECT 
SRC_SYS_CD,
TRGT_SYS_CD,
SUBJ_CD,
max(RUN_CYC_NO)
FROM {IDSOwner}.P_RUN_CYC
WHERE TRGT_SYS_CD = '{TrgtSys}'
AND SRC_SYS_CD = '{SrcSys}'
AND SUBJ_CD = '{SubjCd}'
AND ROW_CT_BAL_IN = 'N'
AND RUN_CYC_NO >= {BeginCycle}
GROUP BY SRC_SYS_CD,
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

df_BusinessRules = df_IDS.select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("SUBJ_CD").alias("SUBJ_CD"),
    col("TRGT_SYS_CD").alias("TRGT_SYS_CD"),
    col("MAX(RUN_CYC_NO)").alias("RUN_CYC_NO")
)

execute_dml(
    f"DROP TABLE IF EXISTS STAGING.RowCtMPIRunCycUpd_P_RUN_CYC_temp",
    jdbc_url,
    jdbc_props
)

(
    df_BusinessRules
    .write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", "STAGING.RowCtMPIRunCycUpd_P_RUN_CYC_temp")
    .mode("append")
    .save()
)

merge_sql = f"""
MERGE {IDSOwner}.P_RUN_CYC AS target
USING STAGING.RowCtMPIRunCycUpd_P_RUN_CYC_temp AS source
ON (
  target.SRC_SYS_CD = source.SRC_SYS_CD
  AND target.SUBJ_CD = source.SUBJ_CD
  AND target.TRGT_SYS_CD = source.TRGT_SYS_CD
  AND target.RUN_CYC_NO <= source.RUN_CYC_NO
  AND target.ROW_CT_BAL_IN = 'N'
)
WHEN MATCHED THEN UPDATE SET
  target.ROW_CT_BAL_IN = 'Y';
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)