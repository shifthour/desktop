# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2010 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:  EdwIndvBePopHlthPgmEnrDiagRunCycUpdt
# MAGIC 
# MAGIC CALLED BY: EdwIndvBePopHlthPgmEnrDiagCntl
# MAGIC 
# MAGIC DESCRIPTION:  Update the P_RUN_CYC table EDW load indicator fields to show those records have been copied to the EDW.
# MAGIC       
# MAGIC                 
# MAGIC                            
# MAGIC PROCESSING:  Extracts the P_RUN_CYC records for IDS ALINEO_POP_HLTH ENR DIAG with the EDW_LOAD_IN = 'N'.  These rows are used to update the P_RUN_CYCLE, load indicator field for the particular source and subject.  All run cycles >= to the current run cycle where the indicator is "N" are updated to "Y".
# MAGIC 
# MAGIC 
# MAGIC               
# MAGIC MODIFICATIONS:                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 Datastage                   Code                         Date                                          
# MAGIC Date                 Developer                Project  / TaskTracker     Change Description                                                                             Environment               Reviewer                   Reviewed
# MAGIC ------------------      ----------------------------     -----------------------------             -------------------------------------------------------------------------------                         ----------------------              ----------------------            ----------------------             
# MAGIC 2010-07-22     Bhoomi Dasari           4297/Alineo-2                    Original Programming.                                                                      EnterpriseNewDevl      Steph Goddard           07/26/2010 
# MAGIC 
# MAGIC 2019-06-11     Jaideep Mankala      US110616                      	 update subject code in extract sql to 'PGM_ENR_TRANS'               EnterpriseDev2	Abhiram Dasarathy	   2019-06-12

# MAGIC Extract P_RUN_CYC records for IDS Mbr Wellness Activity Redemption with a value \"N\" in the  EDW_LOAD_IN where the run cycle is >= to the current run cycle used for extracting.
# MAGIC Set EDW_LOAD_IN to 'Y'
# MAGIC Update the P_RUN_CYC table to indicate source system data has been loaded to the EDW for each of the run cycles processed.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


BeginCycle = get_widget_value("BeginCycle","")
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = f"""
SELECT 
  SRC_SYS_CD,
  TRGT_SYS_CD,
  SUBJ_CD,
  min(RUN_CYC_NO) as RUN_CYC_NO
FROM {IDSOwner}.P_RUN_CYC
WHERE TRGT_SYS_CD = 'IDS'
  AND SUBJ_CD = 'PGM_ENR_TRANS'
  AND EDW_LOAD_IN = 'N'
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

drop_sql = "DROP TABLE IF EXISTS STAGING.EdwIndvBePopHlthPgmEnrDiagRunCycUpdt_P_RUN_CYC_temp"
execute_dml(drop_sql, jdbc_url, jdbc_props)

(
    df_BusinessRules.write.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", "STAGING.EdwIndvBePopHlthPgmEnrDiagRunCycUpdt_P_RUN_CYC_temp")
    .mode("overwrite")
    .save()
)

merge_sql = f"""
MERGE {IDSOwner}.P_RUN_CYC AS T
USING STAGING.EdwIndvBePopHlthPgmEnrDiagRunCycUpdt_P_RUN_CYC_temp AS S
ON 
  T.SRC_SYS_CD = S.SRC_SYS_CD
  AND T.SUBJ_CD = S.SUBJ_CD
  AND T.TRGT_SYS_CD = S.TRGT_SYS_CD
  AND T.RUN_CYC_NO >= S.RUN_CYC_NO
  AND T.EDW_LOAD_IN = 'N'
WHEN MATCHED THEN 
  UPDATE SET T.EDW_LOAD_IN = 'Y';
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)