# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 02/01/08 12:08:08 Batch  14642_43698 INIT bckcetl edw10 dsadm dsadm
# MAGIC ^1_1 12/20/07 11:01:10 Batch  14599_39692 PROMOTE bckcetl edw10 dsadm bls for sa
# MAGIC ^1_1 12/20/07 10:56:16 Batch  14599_39378 INIT bckcett testEDW10 dsadm bls for sa
# MAGIC ^1_1 12/17/07 20:20:09 Batch  14596_73222 PROMOTE bckcett testEDW10 u03651 Steph for Sharon
# MAGIC ^1_1 12/17/07 20:17:09 Batch  14596_73033 INIT bckcett devlEDW10 u03651 steffy
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  Update the P_RUN_CYC table EDW load indicator fields to show those records have been copied to the EDW.
# MAGIC       
# MAGIC                 
# MAGIC                            
# MAGIC PROCESSING:  Extracts the P_RUN_CYC records for IDS HEALTH SCREEN with the EDW_LOAD_IN = 'N'.  These rows are used to update the P_RUN_CYCLE, load indicator field for the particular source and subject.  All run cycles >= to the current run cycle where the indicator is "N" are updated to "Y".
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC               
# MAGIC MODIFICATIONS:                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  datastage                   Code                         Date
# MAGIC Date                 Developer                Project  / TaskTracker     Change Description                                                                                           Environment               Reviewer                   Review
# MAGIC ------------------      ----------------------------     -----------------------------             -------------------------------------------------------------------------------                                         ----------------------              ----------------------            ----------------------             
# MAGIC 2006-04-13      sharon andrew          IAD Clinicla                         Original Programming.                                                                                      devlEDW10              Steph Goddard          12/17/2007

# MAGIC Extract P_RUN_CYC records for IDS Health Screen with a value \"N\" in the  EDW_LOAD_IN where the run cycle is >= to the current run cycle used for extracting.
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
from pyspark.sql import functions as F, types as T
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


BeginCycle = get_widget_value('BeginCycle','0')
IDSOwner = get_widget_value('$IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = f"SELECT SRC_SYS_CD, TRGT_SYS_CD, SUBJ_CD, MIN(RUN_CYC_NO) AS RUN_CYC_NO FROM {IDSOwner}.P_RUN_CYC WHERE TRGT_SYS_CD = 'IDS' AND SUBJ_CD = 'HEALTH_SCREEN' AND EDW_LOAD_IN = 'N' GROUP BY SRC_SYS_CD, TRGT_SYS_CD, SUBJ_CD"
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

merge_temp_table = "STAGING.EdwLoadClnclHlthScreenRunCycUpd_P_RUN_CYC_temp"
execute_dml(f"DROP TABLE IF EXISTS {merge_temp_table}", jdbc_url, jdbc_props)

df_BusinessRules.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", merge_temp_table) \
    .mode("overwrite") \
    .save()

merge_sql = f"""MERGE INTO {IDSOwner}.P_RUN_CYC AS T
USING {merge_temp_table} AS S
ON (
  T.SRC_SYS_CD = S.SRC_SYS_CD
  AND T.SUBJ_CD = S.SUBJ_CD
  AND T.TRGT_SYS_CD = S.TRGT_SYS_CD
  AND T.RUN_CYC_NO >= S.RUN_CYC_NO
  AND T.EDW_LOAD_IN = 'N'
)
WHEN MATCHED THEN
  UPDATE SET T.EDW_LOAD_IN = 'Y'
WHEN NOT MATCHED THEN
  INSERT (SRC_SYS_CD, SUBJ_CD, TRGT_SYS_CD, RUN_CYC_NO, EDW_LOAD_IN)
  VALUES (S.SRC_SYS_CD, S.SUBJ_CD, S.TRGT_SYS_CD, S.RUN_CYC_NO, 'Y');
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)