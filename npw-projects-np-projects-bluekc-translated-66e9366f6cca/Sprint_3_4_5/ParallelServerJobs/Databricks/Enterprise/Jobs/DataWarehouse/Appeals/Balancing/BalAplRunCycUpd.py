# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 11/28/07 12:47:07 Batch  14577_46088 PROMOTE bckcetl edw10 dsadm bls for hs
# MAGIC ^1_1 11/28/07 11:24:20 Batch  14577_41063 INIT bckcett testEDW10 dsadm bls for hs
# MAGIC ^1_1 11/27/07 16:26:55 Batch  14576_59222 PROMOTE bckcett testEDW10 u11141 hs
# MAGIC ^1_1 11/27/07 16:25:10 Batch  14576_59115 INIT bckcett devlEDW10 u11141 hs
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:    IdsAplBalCntl
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   
# MAGIC                 The Job Updates the P_RUN_CYC table with the ROW_TO_ROW_BAL_IN and RI_BAL_IN set to 'Y' after balancing is done for the subject area
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Bhoomi Dasari                 10/07/2007          3264                              Originally Programmed                          devlEDW10                Steph Goddard               10/18/2007

# MAGIC Extract P_RUN_CYC records for IDS Appeals with a value \"N\" in the  ROW_TO_ROW_BAL_IN where the run cycle is >= to the current run cycle used for extracting.
# MAGIC Update the P_RUN_CYC table with Balancing Indicators set to 'Y' to indicate source system data has been balanced with respect to IDS for each of the run cycles processed.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
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
WHERE 
  TRGT_SYS_CD = '{TrgtSys}'
  AND SRC_SYS_CD = '{SrcSys}'
  AND SUBJ_CD = '{SubjCd}'
  AND ROW_TO_ROW_BAL_IN = 'N'
  AND RI_BAL_IN = 'N'
  AND RUN_CYC_NO >= {BeginCycle}
GROUP BY 
  SRC_SYS_CD,
  TRGT_SYS_CD,
  SUBJ_CD
""".strip()

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

drop_tmp_sql = "DROP TABLE IF EXISTS STAGING.BalAplRunCycUpd_P_RUN_CYC_temp"
execute_dml(drop_tmp_sql, jdbc_url, jdbc_props)

(
    df_BusinessRules.write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", "STAGING.BalAplRunCycUpd_P_RUN_CYC_temp")
    .mode("overwrite")
    .save()
)

merge_sql = f"""
MERGE {IDSOwner}.P_RUN_CYC AS T
USING STAGING.BalAplRunCycUpd_P_RUN_CYC_temp AS S
   ON (
       T.SRC_SYS_CD = S.SRC_SYS_CD
       AND T.SUBJ_CD = S.SUBJ_CD
       AND T.TRGT_SYS_CD = S.TRGT_SYS_CD
       AND T.RUN_CYC_NO <= S.RUN_CYC_NO
       AND T.ROW_TO_ROW_BAL_IN = 'N'
       AND T.RI_BAL_IN = 'N'
      )
WHEN MATCHED THEN
   UPDATE SET
       T.ROW_TO_ROW_BAL_IN = 'Y',
       T.RI_BAL_IN = 'Y'
;
""".strip()

execute_dml(merge_sql, jdbc_url, jdbc_props)