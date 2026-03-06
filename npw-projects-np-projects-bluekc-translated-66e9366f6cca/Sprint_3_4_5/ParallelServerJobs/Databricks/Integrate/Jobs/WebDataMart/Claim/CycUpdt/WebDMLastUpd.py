# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 01/29/08 14:29:13 Batch  14639_52158 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 01/09/08 13:32:13 Batch  14619_48738 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/24/07 09:35:50 Batch  14573_34558 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_3 03/28/07 06:21:15 Batch  14332_22879 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 03/07/07 16:30:43 Batch  14311_59451 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 05/19/06 09:43:19 Batch  14019_35008 INIT bckcett devlIDS30 u10913 Ollie move KCREE from devl to test
# MAGIC 
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     WebDMLastUpd
# MAGIC 
# MAGIC DESCRIPTION:     Update reference table with subject area last update date and time
# MAGIC       
# MAGIC 
# MAGIC INPUTS:          Subject name
# MAGIC                         Date time
# MAGIC 	
# MAGIC   
# MAGIC HASH FILES: None
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  None
# MAGIC                            
# MAGIC 
# MAGIC PROCESSING:  The sysdummy table is used to initiate the SQL server update.
# MAGIC 
# MAGIC 
# MAGIC OUTPUTS:     direct update of REF_DM_LAST_UPDT
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC            Brent Leland  05/17/2006  -   Originally Programmed

# MAGIC This SQL is only to initiate the routines in the Transform Stage.  The data row is never used.
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
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


ClmMartOwner = get_widget_value('ClmMartOwner','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')
SubjectName = get_widget_value('SubjectName','')
UpdateDate = get_widget_value('UpdateDate','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
df_SYSDUMMY1 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", "SELECT IBMREQD FROM sysibm.sysdummy1")
    .load()
)

df_CreateRow = df_SYSDUMMY1.select(
    F.lit(SubjectName).alias("DM_SUBJ_AREA_NM"),
    F.lit(UpdateDate).alias("DM_LAST_UPDT_DTM")
)

df_final = df_CreateRow.select(
    F.rpad(F.col("DM_SUBJ_AREA_NM"), <...>, " ").alias("DM_SUBJ_AREA_NM"),
    F.rpad(F.col("DM_LAST_UPDT_DTM"), <...>, " ").alias("DM_LAST_UPDT_DTM")
)

jdbc_url2, jdbc_props2 = get_db_config(clmmart_secret_name)
execute_dml(f"DROP TABLE IF EXISTS STAGING.WebDMLastUpd_REF_DM_LAST_UPDT_temp", jdbc_url2, jdbc_props2)

df_final.write.format("jdbc") \
    .option("url", jdbc_url2) \
    .options(**jdbc_props2) \
    .option("dbtable", "STAGING.WebDMLastUpd_REF_DM_LAST_UPDT_temp") \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO {ClmMartOwner}.REF_DM_LAST_UPDT AS T
USING STAGING.WebDMLastUpd_REF_DM_LAST_UPDT_temp AS S
ON T.DM_SUBJ_AREA_NM = S.DM_SUBJ_AREA_NM
WHEN MATCHED THEN
  UPDATE SET T.DM_LAST_UPDT_DTM = S.DM_LAST_UPDT_DTM
WHEN NOT MATCHED THEN
  INSERT (DM_SUBJ_AREA_NM, DM_LAST_UPDT_DTM)
  VALUES (S.DM_SUBJ_AREA_NM, S.DM_LAST_UPDT_DTM);
"""
execute_dml(merge_sql, jdbc_url2, jdbc_props2)