# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_2 12/27/07 10:15:44 Batch  14606_36948 INIT bckcetl edw10 dsadm dsadm
# MAGIC ^1_1 08/31/07 15:38:12 Batch  14488_56296 INIT bckcetl edw10 dsadm dsadm
# MAGIC ^1_1 04/02/07 14:36:36 Batch  14337_52603 INIT bckcetl edw10 dsadm dsadm
# MAGIC ^1_1 10/20/05 15:02:18 Batch  13808_54143 PROMOTE bckcetl edw10 dcg01 sa
# MAGIC ^1_1 10/20/05 15:01:39 Batch  13808_54102 INIT bckcett devlEDW10 u10157 sa
# MAGIC ^1_1 09/02/05 16:08:59 Batch  13760_58141 INIT bckcett devlEDW10 u10157 sa
# MAGIC ^1_1 07/19/05 12:40:07 Batch  13715_45613 INIT bckcett devlEDW10 dsadm GIna Parr
# MAGIC 
# MAGIC 
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:    IdsRunCycleExtr
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:   Find the max last update run cycle in the specified IDS table.
# MAGIC       
# MAGIC 
# MAGIC INPUTS:	 Table name
# MAGIC 
# MAGIC   
# MAGIC HASH FILES:  RUN_CYCLE
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  None
# MAGIC                         
# MAGIC    
# MAGIC PROCESSING:   EDW jobs will drive IDS extracts off the last run cycle update field.  This job extracts the max value for a given table, adds 1 to it and save the value in the Universe table.
# MAGIC  
# MAGIC 
# MAGIC OUTPUTS:  Universe table entry.
# MAGIC                     
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC               B Leland           06/23/2005  -  Initial programming

# MAGIC Extract maximum field value from specified table.
# MAGIC The maximum value in the data is stored for the next run.  Programs should use: 
# MAGIC                   "Max Value > This Value" 
# MAGIC in the SQL where clause.
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


TableName = get_widget_value('TableName','')
Subject = get_widget_value('Subject','')
ColumnName = get_widget_value('ColumnName','')
Owner = get_widget_value('Owner','')
DB = get_widget_value('DB','')
Acct = get_widget_value('Acct','')
PW = get_widget_value('PW','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = f"SELECT max({ColumnName}) AS LAST_UPDT_RUN_CYC_EXCTN_SK FROM {Owner}.{TableName}"
df_Input_Table = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_Trans1 = df_Input_Table.select(
    trim(Subject).alias("SUBJECT"),
    F.when(F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").isNull(), F.lit(0))
     .otherwise(F.col("LAST_UPDT_RUN_CYC_EXCTN_SK")).alias("MAXVALUE")
)

execute_dml("DROP TABLE IF EXISTS STAGING.GetMaxFieldValueExtr_MAX_VALUE_temp", jdbc_url, jdbc_props)
df_Trans1.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.GetMaxFieldValueExtr_MAX_VALUE_temp") \
    .mode("overwrite") \
    .save()

merge_sql = """
MERGE MAX_VALUE AS T
USING STAGING.GetMaxFieldValueExtr_MAX_VALUE_temp AS S
ON T.SUBJECT = S.SUBJECT
WHEN MATCHED THEN
  UPDATE SET
    T.MAXVALUE = S.MAXVALUE
WHEN NOT MATCHED THEN
  INSERT (SUBJECT, MAXVALUE)
  VALUES (S.SUBJECT, S.MAXVALUE);
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)