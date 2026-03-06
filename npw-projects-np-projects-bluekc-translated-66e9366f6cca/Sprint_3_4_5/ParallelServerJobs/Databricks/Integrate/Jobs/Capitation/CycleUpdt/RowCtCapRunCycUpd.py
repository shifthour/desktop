# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 10/24/07 09:59:24 Batch  14542_35967 PROMOTE bckcetl ids20 dsadm bls for on
# MAGIC ^1_1 10/24/07 09:54:15 Batch  14542_35658 INIT bckcett testIDS30 dsadm bls for on
# MAGIC ^1_2 10/17/07 13:59:14 Batch  14535_50357 PROMOTE bckcett testIDS30 u03651 steffy
# MAGIC ^1_2 10/17/07 13:54:06 Batch  14535_50077 INIT bckcett devlIDS30 u03651 steffy
# MAGIC ^1_1 10/16/07 10:22:11 Batch  14534_37334 INIT bckcett devlIDS30 u03651 steffy
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:     FctsCapBalCntl
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   
# MAGIC                 The Job Updates the P_RUN_CYC table with the ROW_CT_BAL_IN set to 'Y' after balancing is done for the subject area
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               06/21/2007          3264                              Originally Programmed                           devlIDS30                 Steph Goddard            09/19/2007

# MAGIC Update the P_RUN_CYC table to indicate source system data has been balanced with respect to IDS for each of the run cycles processed.
# MAGIC Set ROW_CT_BAL_IN to 'Y'
# MAGIC Extract P_RUN_CYC records for IDS Capitation with a value \"N\" in the  ROW_CT_BAL_IN where the run cycle is >= to the current run cycle used for extracting.
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
ids_secret_name = get_widget_value('ids_secret_name','')
BeginCycle = get_widget_value('BeginCycle','')
SrcSys = get_widget_value('SrcSys','')
TrgtSys = get_widget_value('TrgtSys','')
SubjCd = get_widget_value('SubjCd','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = (
    f"SELECT SRC_SYS_CD, TRGT_SYS_CD, SUBJ_CD, max(RUN_CYC_NO) as RUN_CYC_NO "
    f"FROM {IDSOwner}.P_RUN_CYC "
    f"WHERE TRGT_SYS_CD = '{TrgtSys}' "
    f"AND SRC_SYS_CD = '{SrcSys}' "
    f"AND SUBJ_CD = '{SubjCd}' "
    f"AND ROW_CT_BAL_IN = 'N' "
    f"AND RUN_CYC_NO >= {BeginCycle} "
    f"GROUP BY SRC_SYS_CD, TRGT_SYS_CD, SUBJ_CD"
)

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

drop_temp_table_sql = "DROP TABLE IF EXISTS STAGING.RowCtCapRunCycUpd_P_RUN_CYC_temp"
execute_dml(drop_temp_table_sql, jdbc_url, jdbc_props)

df_BusinessRules.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.RowCtCapRunCycUpd_P_RUN_CYC_temp") \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO {IDSOwner}.P_RUN_CYC AS T
USING STAGING.RowCtCapRunCycUpd_P_RUN_CYC_temp AS S
ON 
  T.SRC_SYS_CD = S.SRC_SYS_CD
  AND T.SUBJ_CD = S.SUBJ_CD
  AND T.TRGT_SYS_CD = S.TRGT_SYS_CD
  AND T.RUN_CYC_NO <= S.RUN_CYC_NO
  AND T.ROW_CT_BAL_IN = 'N'
WHEN MATCHED THEN
  UPDATE SET
    T.ROW_CT_BAL_IN = 'Y';
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)