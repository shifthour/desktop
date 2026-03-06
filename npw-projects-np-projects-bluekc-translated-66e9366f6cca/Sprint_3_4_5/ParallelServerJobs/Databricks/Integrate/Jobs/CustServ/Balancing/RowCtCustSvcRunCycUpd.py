# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 11/01/07 16:40:40 Batch  14550_60068 PROMOTE bckcetl ids20 dsadm rc for brent
# MAGIC ^1_1 11/01/07 16:19:50 Batch  14550_58802 INIT bckcett testIDS30 dsadm rc for brent
# MAGIC ^1_2 10/29/07 15:58:44 Batch  14547_57529 PROMOTE bckcett testIDS30 u08717 Brent
# MAGIC ^1_2 10/29/07 15:35:38 Batch  14547_56143 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_1 10/29/07 14:48:31 Batch  14547_53315 INIT bckcett devlIDS30 u08717 Brent
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:    FctsCustSvcBalCntl
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   
# MAGIC                 The Job Updates the P_RUN_CYC table with the ROW_CT_BAL_IN set to 'Y' after balancing is done for the subject area
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               06/26/2007          3264                              Originally Programmed                           devlIDS30                 Steph Goddard             9/14/07

# MAGIC Update the P_RUN_CYC table to indicate source system data has been balanced with respect to IDS for each of the run cycles processed.
# MAGIC Set ROW_CT_BAL_IN to 'Y'
# MAGIC Extract P_RUN_CYC records for IDS Customer Service with a value \"N\" in the  ROW_CT_BAL_IN where the run cycle is >= to the current run cycle used for extracting.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
FctsBeginCycle = get_widget_value('FctsBeginCycle','100')
NpsBeginCycle = get_widget_value('NpsBeginCycle','100')
FctsSrcSys = get_widget_value('FctsSrcSys','FACETS')
NpsSrcSys = get_widget_value('NpsSrcSys','NPS')
TrgtSys = get_widget_value('TrgtSys','IDS')
SubjCd = get_widget_value('SubjCd','CUSTSVC')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = f"""
SELECT
  SRC_SYS_CD,
  TRGT_SYS_CD,
  SUBJ_CD,
  max(RUN_CYC_NO) as RUN_CYC_NO
FROM {IDSOwner}.P_RUN_CYC
WHERE
  TRGT_SYS_CD = '{TrgtSys}'
  AND (SRC_SYS_CD = '{FctsSrcSys}' OR SRC_SYS_CD = '{NpsSrcSys}')
  AND SUBJ_CD = '{SubjCd}'
  AND ROW_CT_BAL_IN = 'N'
  AND (RUN_CYC_NO >= {FctsBeginCycle} OR RUN_CYC_NO >= {NpsBeginCycle})
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
    "SRC_SYS_CD",
    "SUBJ_CD",
    "TRGT_SYS_CD",
    "RUN_CYC_NO"
)

temp_table_name = "STAGING.RowCtCustSvcRunCycUpd_P_RUN_CYC_temp"

execute_dml(f"DROP TABLE IF EXISTS {temp_table_name}", jdbc_url, jdbc_props)

df_BusinessRules.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", temp_table_name) \
    .option("user", jdbc_props["user"]) \
    .option("password", jdbc_props["password"]) \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE {IDSOwner}.P_RUN_CYC AS T
USING {temp_table_name} AS S
ON
  T.SRC_SYS_CD = S.SRC_SYS_CD
  AND T.SUBJ_CD = S.SUBJ_CD
  AND T.TRGT_SYS_CD = S.TRGT_SYS_CD
  AND T.RUN_CYC_NO <= S.RUN_CYC_NO
  AND T.ROW_CT_BAL_IN = 'N'
WHEN MATCHED THEN
  UPDATE SET T.ROW_CT_BAL_IN = 'Y'
WHEN NOT MATCHED THEN
  INSERT (SRC_SYS_CD, SUBJ_CD, TRGT_SYS_CD, RUN_CYC_NO, ROW_CT_BAL_IN)
  VALUES (S.SRC_SYS_CD, S.SUBJ_CD, S.TRGT_SYS_CD, S.RUN_CYC_NO, 'Y');
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)