# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_3 03/23/09 15:20:40 Batch  15058_55245 INIT bckcetl edw10 dsadm dsadm
# MAGIC ^1_2 03/12/09 19:44:50 Batch  15047_71107 INIT bckcetl edw10 dcg01 sa - bringing everthing down for test
# MAGIC ^1_2 02/25/09 17:10:06 Batch  15032_61828 INIT bckcetl edw10 dcg01 Bringing production code down to edw_test  claim only
# MAGIC ^1_2 02/01/08 12:08:08 Batch  14642_43698 INIT bckcetl edw10 dsadm dsadm
# MAGIC ^1_1 12/27/07 14:18:21 Batch  14606_51505 INIT bckcetl edw10 dsadm dsadm
# MAGIC ^1_1 11/05/07 14:33:17 Batch  14554_52458 PROMOTE bckcetl edw10 dsadm rc for brent
# MAGIC ^1_1 11/05/07 13:30:09 Batch  14554_48642 INIT bckcett testEDW10 dsadm rc for brent
# MAGIC ^1_3 11/02/07 15:11:26 Batch  14551_54690 PROMOTE bckcett testEDW10 u08717 Brent
# MAGIC ^1_3 11/02/07 15:00:18 Batch  14551_54021 INIT bckcett devlEDW10 u08717 Brent
# MAGIC ^1_2 11/02/07 07:43:09 Batch  14551_27794 INIT bckcett devlEDW10 u08717 Brent
# MAGIC ^1_1 11/01/07 11:00:01 Batch  14550_39611 INIT bckcett devlEDW10 u08717 Brent
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:    IdsClmBalCntl
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   
# MAGIC                 The Job Updates the P_RUN_CYC table with the ROW_CT_BAL_IN, ROW_TO_ROW_BAL_IN and CLMN_SUM_BAL_IN set to 'Y' after balancing is done for the subject area
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               09/07/2007          3264                              Originally Programmed                          devlEDW10                Steph Goddard            10/22/2007

# MAGIC Extract P_RUN_CYC records for IDS Claims with a value \"N\" in the ROW_CT_BAL_IN, ROW_TO_ROW_BAL_IN and CLMN_SUM_BAL_IN where the run cycle is >= to the current run cycle used for extracting.
# MAGIC Update the P_RUN_CYC table with Balancing Indicators set to 'Y' to indicate source system data has been balanced with respect to IDS for each of the run cycles processed.
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


IDSOwner = get_widget_value('IDSOwner','')
BeginCycle = get_widget_value('BeginCycle','')
SrcSys = get_widget_value('SrcSys','')
TrgtSys = get_widget_value('TrgtSys','')
SubjCd = get_widget_value('SubjCd','')
ids_secret_name = get_widget_value('ids_secret_name','')

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
  AND ROW_TO_ROW_BAL_IN = 'N'
  AND CLMN_SUM_BAL_IN = 'N'
  AND RI_BAL_IN = 'N'
  AND RELSHP_CLMN_SUM_BAL_IN = 'N'
  AND CRS_FOOT_BAL_IN = 'N'
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

drop_sql = "DROP TABLE IF EXISTS STAGING.BalClmRunCycUpd_P_RUN_CYC_temp"
execute_dml(drop_sql, jdbc_url, jdbc_props)

df_BusinessRules.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.BalClmRunCycUpd_P_RUN_CYC_temp") \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO {IDSOwner}.P_RUN_CYC AS T
USING STAGING.BalClmRunCycUpd_P_RUN_CYC_temp AS S
ON T.SRC_SYS_CD = S.SRC_SYS_CD
   AND T.SUBJ_CD = S.SUBJ_CD
   AND T.TRGT_SYS_CD = S.TRGT_SYS_CD
   AND T.RUN_CYC_NO <= S.RUN_CYC_NO
   AND T.ROW_CT_BAL_IN = 'N'
   AND T.ROW_TO_ROW_BAL_IN = 'N'
   AND T.CLMN_SUM_BAL_IN = 'N'
   AND T.RI_BAL_IN = 'N'
   AND T.RELSHP_CLMN_SUM_BAL_IN = 'N'
   AND T.CRS_FOOT_BAL_IN = 'N'
WHEN MATCHED THEN
  UPDATE SET
    T.ROW_CT_BAL_IN = 'Y',
    T.ROW_TO_ROW_BAL_IN = 'Y',
    T.CLMN_SUM_BAL_IN = 'Y',
    T.RI_BAL_IN = 'Y',
    T.RELSHP_CLMN_SUM_BAL_IN = 'Y',
    T.CRS_FOOT_BAL_IN = 'Y'
WHEN NOT MATCHED THEN
  INSERT (
    SRC_SYS_CD,
    SUBJ_CD,
    TRGT_SYS_CD,
    RUN_CYC_NO,
    ROW_CT_BAL_IN,
    ROW_TO_ROW_BAL_IN,
    CLMN_SUM_BAL_IN,
    RI_BAL_IN,
    RELSHP_CLMN_SUM_BAL_IN,
    CRS_FOOT_BAL_IN
  )
  VALUES (
    S.SRC_SYS_CD,
    S.SUBJ_CD,
    S.TRGT_SYS_CD,
    S.RUN_CYC_NO,
    'Y','Y','Y','Y','Y','Y'
  );
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)