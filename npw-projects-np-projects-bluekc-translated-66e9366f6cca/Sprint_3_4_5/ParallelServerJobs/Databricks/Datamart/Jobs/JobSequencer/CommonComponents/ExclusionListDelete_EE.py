# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:    ExclusionListExtr
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:   Extract the records for a subject from the DB2 - W_TASK_EXCL table.
# MAGIC       
# MAGIC 
# MAGIC INPUTS:	 Subject name
# MAGIC 
# MAGIC   
# MAGIC HASH FILES:  TASK_EXCL
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  None
# MAGIC                         
# MAGIC    
# MAGIC PROCESSING:   Task rows from DB2 table are concatenated together with a semi-colon delimiter.  Each row overwrites the previous row in the TASK_EXCL UV table.  The last entry is the combination of all DB2 entries.
# MAGIC  
# MAGIC 
# MAGIC OUTPUTS:  A semi-colon delimited string of tasks to exclude for a subject.
# MAGIC                     
# MAGIC 
# MAGIC ===============================================
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                       
# MAGIC DEVELOPER                         DATE                 PROJECT                                                  DESCRIPTION                                                       DATASTAGE   ENVIRONMENT                     CODE  REVIEW            REVIEW DATE                    
# MAGIC -----------------------------                 ----------------------      ------------------------------                                  -----------------------------------                                            ------------------------------                                          ------------------------------      --------------------
# MAGIC  B Leland                                 05/25/2005  -                                                                      Initial programming
# MAGIC 
# MAGIC 
# MAGIC Nagesh Bandi                         07/01/2013        5114-Enterprise Efficiencies                 Extract subject records from W_TASK_EXCL              EnterpriseWrhseDevl                                      Jag Yelavarthi              2013-08-16

# MAGIC Delete records from W_TASK_EXCL table for that particular Subject.
# MAGIC 
# MAGIC Delete TASK_EXCL.dat file created by EXCLUSIONLISTExtr job using After-job subroutine.
# MAGIC 
# MAGIC Job Name:
# MAGIC EXCLUSIONLISTDelete
# MAGIC Generate a dummy row.
# MAGIC Delete information related tto that particular Subject from TASK_EXCL table
# MAGIC Pass Parameter
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_DataMart3
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
Subject = get_widget_value('Subject','')

schema_rgen_Task_Subj = StructType([
    StructField("TASK_SUBJ_NM", StringType(), True)
])
df_rgen_Task_Subj = spark.createDataFrame([("",)], schema_rgen_Task_Subj)

df_xfm_Task = df_rgen_Task_Subj.withColumn("TASK_SUBJ_NM", lit(Subject))
df_xfm_Task = df_xfm_Task.filter(col("TASK_SUBJ_NM").isNotNull())

df_db2_TASK_EXCL_delete = df_xfm_Task.select("TASK_SUBJ_NM")
df_db2_TASK_EXCL_delete = df_db2_TASK_EXCL_delete.withColumn("TASK_SUBJ_NM", rpad("TASK_SUBJ_NM", 50, " "))

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

execute_dml("DROP TABLE IF EXISTS STAGING.ExclusionListDelete_EE_db2_TASK_EXCL_delete_temp", jdbc_url, jdbc_props)

df_db2_TASK_EXCL_delete.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .mode("overwrite") \
    .option("dbtable", "STAGING.ExclusionListDelete_EE_db2_TASK_EXCL_delete_temp") \
    .save()

merge_sql = f"""
MERGE INTO {IDSOwner}.W_TASK_EXCL AS T
USING STAGING.ExclusionListDelete_EE_db2_TASK_EXCL_delete_temp AS S
ON T.TASK_SUBJ_NM = S.TASK_SUBJ_NM
WHEN MATCHED THEN DELETE;
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)