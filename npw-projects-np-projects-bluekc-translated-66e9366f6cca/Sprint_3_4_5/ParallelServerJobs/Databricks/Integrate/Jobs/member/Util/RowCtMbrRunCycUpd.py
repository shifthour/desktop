# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 01/28/08 13:39:53 Batch  14638_49197 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 01/10/08 10:01:22 Batch  14620_36086 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 12/26/07 15:33:58 Batch  14605_56043 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/23/07 15:42:37 Batch  14572_56566 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 09/25/07 16:07:19 Batch  14513_58043 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 09/20/07 14:41:37 Batch  14508_52916 PROMOTE bckcetl ids20 dsadm bls for bl
# MAGIC ^1_1 09/20/07 13:58:24 Batch  14508_50316 INIT bckcett testIDS30 dsadm bls for bl
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:     FctsMbrshpBalCntl 
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   
# MAGIC                 The Job Updates the P_RUN_CYC table with the ROW_CT_BAL_IN set to 'Y' after balancing is done for the subject area
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               06/21/2007          3264                              Originally Programmed                           devlIDS30

# MAGIC Update the P_RUN_CYC table to indicate source system data has been balanced with respect to IDS for each of the run cycles processed.
# MAGIC Set ROW_CT_BAL_IN to 'Y'
# MAGIC Extract P_RUN_CYC records for IDS Membership with a value \"N\" in the  ROW_CT_BAL_IN where the run cycle is >= to the current run cycle used for extracting.
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
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
beginCycle = get_widget_value('BeginCycle','100')
srcSys = get_widget_value('SrcSys','')
trgtSys = get_widget_value('TrgtSys','')
subjCd = get_widget_value('SubjCd','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = f"""
SELECT
  SRC_SYS_CD,
  TRGT_SYS_CD,
  SUBJ_CD,
  MAX(RUN_CYC_NO) AS RUN_CYC_NO
FROM {IDSOwner}.P_RUN_CYC
WHERE TRGT_SYS_CD = '{trgtSys}'
  AND SRC_SYS_CD = '{srcSys}'
  AND SUBJ_CD = '{subjCd}'
  AND ROW_CT_BAL_IN = 'N'
  AND RUN_CYC_NO >= {beginCycle}
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

df_BusinessRules = df_IDS.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("SUBJ_CD").alias("SUBJ_CD"),
    F.col("TRGT_SYS_CD").alias("TRGT_SYS_CD"),
    F.col("RUN_CYC_NO").alias("RUN_CYC_NO")
)

temp_table = "STAGING.RowCtMbrRunCycUpd_P_RUN_CYC_temp"

execute_dml(f"DROP TABLE IF EXISTS {temp_table}", jdbc_url, jdbc_props)

df_BusinessRules.write.format("jdbc")\
    .option("url", jdbc_url)\
    .options(**jdbc_props)\
    .option("dbtable", temp_table)\
    .mode("append")\
    .save()

merge_sql = f"""
MERGE INTO {IDSOwner}.P_RUN_CYC AS T
USING {temp_table} AS S
  ON T.SRC_SYS_CD = S.SRC_SYS_CD
 AND T.SUBJ_CD = S.SUBJ_CD
 AND T.TRGT_SYS_CD = S.TRGT_SYS_CD
 AND T.RUN_CYC_NO <= S.RUN_CYC_NO
 AND T.ROW_CT_BAL_IN = 'N'
WHEN MATCHED THEN
  UPDATE SET ROW_CT_BAL_IN = 'Y';
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)