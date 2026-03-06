# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_2 11/07/07 14:46:06 Batch  14556_53195 PROMOTE bckcetl ids20 dsadm bls for rt
# MAGIC ^1_2 11/07/07 14:33:10 Batch  14556_52396 INIT bckcett testIDScur dsadm bls for rt
# MAGIC ^1_2 11/06/07 10:51:39 Batch  14555_39105 PROMOTE bckcett testIDScur u06640 Ralph
# MAGIC ^1_2 11/06/07 10:33:20 Batch  14555_38005 PROMOTE bckcett testIDScur u06640 Ralph
# MAGIC ^1_2 11/06/07 10:32:05 Batch  14555_37929 INIT bckcett devlIDS30 u06640 Ralph
# MAGIC ^1_1 11/06/07 09:47:12 Batch  14555_35238 INIT bckcett devlIDS30 u06640 Ralph
# MAGIC ^1_1 10/08/07 16:58:04 Batch  14526_61088 INIT bckcett devlIDS30 u08717 Brent
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:      PSIJrnlEntryBalCntl, PSIFnclLobBalCntl and FctsPaymtSumBalCntl
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   
# MAGIC                 The Job Updates the P_RUN_CYC table with the ROW_CT_BAL_IN set to 'Y' after balancing is done for the subject area
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               06/21/2007          3264                              Originally Programmed                           devlIDS30                 Steph Goddard            09/15/2007

# MAGIC Update the P_RUN_CYC table to indicate source system data has been balanced with respect to IDS for each of the run cycles processed.
# MAGIC Set ROW_CT_BAL_IN to 'Y'
# MAGIC Extract P_RUN_CYC records for IDS Finance with a value \"N\" in the  ROW_CT_BAL_IN where the run cycle is >= to the current run cycle used for extracting.
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


# MAGIC %run ../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
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
  max(RUN_CYC_NO) as RUN_CYC_NO
FROM {IDSOwner}.P_RUN_CYC
WHERE TRGT_SYS_CD = '{TrgtSys}'
  AND SRC_SYS_CD = '{SrcSys}'
  AND SUBJ_CD = '{SubjCd}'
  AND ROW_CT_BAL_IN = 'N'
  AND RUN_CYC_NO >= {BeginCycle}
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
    df_IDS["SRC_SYS_CD"].alias("SRC_SYS_CD"),
    df_IDS["SUBJ_CD"].alias("SUBJ_CD"),
    df_IDS["TRGT_SYS_CD"].alias("TRGT_SYS_CD"),
    df_IDS["RUN_CYC_NO"].alias("RUN_CYC_NO")
)

df_P_RUN_CYC = df_BusinessRules.select(
    "SRC_SYS_CD",
    "SUBJ_CD",
    "TRGT_SYS_CD",
    "RUN_CYC_NO"
)

drop_temp_sql = "DROP TABLE IF EXISTS STAGING.RowCtFinanceRunCycUpd_P_RUN_CYC_temp"
execute_dml(drop_temp_sql, jdbc_url, jdbc_props)

df_P_RUN_CYC.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "STAGING.RowCtFinanceRunCycUpd_P_RUN_CYC_temp") \
    .options(**jdbc_props) \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO {IDSOwner}.P_RUN_CYC AS T
USING STAGING.RowCtFinanceRunCycUpd_P_RUN_CYC_temp AS S
ON T.SRC_SYS_CD = S.SRC_SYS_CD
   AND T.SUBJ_CD = S.SUBJ_CD
   AND T.TRGT_SYS_CD = S.TRGT_SYS_CD
   AND T.RUN_CYC_NO <= S.RUN_CYC_NO
   AND T.ROW_CT_BAL_IN = 'N'
WHEN MATCHED THEN
  UPDATE SET T.ROW_CT_BAL_IN = 'Y';
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)