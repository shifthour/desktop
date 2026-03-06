# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 01/28/08 08:28:47 Batch  14638_30532 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 12/26/07 11:16:35 Batch  14605_40601 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/21/07 14:41:36 Batch  14570_52900 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 10/10/07 13:50:33 Batch  14528_49851 PROMOTE bckcetl ids20 dsadm bls for bl
# MAGIC ^1_1 10/10/07 13:38:24 Batch  14528_49120 INIT bckcett testIDS30 dsadm bls for bl
# MAGIC ^1_1 10/08/07 17:37:28 Batch  14526_63461 PROMOTE bckcett testIDS30 u08717 Brent
# MAGIC ^1_1 10/08/07 17:30:51 Batch  14526_63055 INIT bckcett devlIDS30 u08717 Brent
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:     FdbNdcBalCntl
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   
# MAGIC                 The Job Updates the P_RUN_CYC table with the ROW_CT_BAL_IN set to 'Y' after balancing is done for the subject area
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               06/21/2007          3264                              Originally Programmed                           devlIDS30                  Steph Goddard           09/27/2007

# MAGIC Update the P_RUN_CYC table to indicate source system data has been balanced with respect to IDS for each of the run cycles processed.
# MAGIC Set ROW_CT_BAL_IN to 'Y'
# MAGIC Extract P_RUN_CYC records for IDS NDC Drugs with a value \"N\" in the  ROW_CT_BAL_IN where the run cycle is >= to the current run cycle used for extracting.
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


IDSOwner = get_widget_value('IDSOwner','$PROJDEF')
ids_secret_name = get_widget_value('ids_secret_name','')
BeginCycle = get_widget_value('BeginCycle','100')
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
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("SUBJ_CD").alias("SUBJ_CD"),
    F.col("TRGT_SYS_CD").alias("TRGT_SYS_CD"),
    F.col("RUN_CYC_NO").alias("RUN_CYC_NO")
)

execute_dml("DROP TABLE IF EXISTS STAGING.RowCtNdcRunCycUpd_P_RUN_CYC_temp", jdbc_url, jdbc_props)

(
    df_BusinessRules.write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", "STAGING.RowCtNdcRunCycUpd_P_RUN_CYC_temp")
    .mode("overwrite")
    .save()
)

merge_sql = f"""
MERGE {IDSOwner}.P_RUN_CYC AS T
USING STAGING.RowCtNdcRunCycUpd_P_RUN_CYC_temp AS S
ON (
  T.SRC_SYS_CD = S.SRC_SYS_CD
  AND T.SUBJ_CD = S.SUBJ_CD
  AND T.TRGT_SYS_CD = S.TRGT_SYS_CD
  AND T.RUN_CYC_NO <= S.RUN_CYC_NO
  AND T.ROW_CT_BAL_IN = 'N'
)
WHEN MATCHED THEN
  UPDATE SET T.ROW_CT_BAL_IN = 'Y'
WHEN NOT MATCHED THEN
  INSERT (SRC_SYS_CD, SUBJ_CD, TRGT_SYS_CD, RUN_CYC_NO)
  VALUES (S.SRC_SYS_CD, S.SUBJ_CD, S.TRGT_SYS_CD, S.RUN_CYC_NO);
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)