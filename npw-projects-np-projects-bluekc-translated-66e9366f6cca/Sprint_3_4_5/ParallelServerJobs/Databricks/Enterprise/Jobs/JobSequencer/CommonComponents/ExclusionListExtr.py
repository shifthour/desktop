# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_2 12/27/07 10:15:44 Batch  14606_36948 INIT bckcetl edw10 dsadm dsadm
# MAGIC ^1_1 08/31/07 15:38:12 Batch  14488_56296 INIT bckcetl edw10 dsadm dsadm
# MAGIC ^1_1 04/02/07 14:36:36 Batch  14337_52603 INIT bckcetl edw10 dsadm dsadm
# MAGIC ^1_1 07/19/05 12:41:16 Batch  13715_45682 PROMOTE bckcetl edw10 dsadm Gina Parr
# MAGIC ^1_1 07/19/05 12:40:07 Batch  13715_45613 INIT bckcett devlEDW10 dsadm GIna Parr
# MAGIC ^1_4 06/20/05 09:33:39 Batch  13686_34424 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_3 06/15/05 11:06:14 Batch  13681_39980 PROMOTE bckcett VERSION u08717 Brent
# MAGIC ^1_3 06/15/05 11:05:50 Batch  13681_39953 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_2 05/25/05 14:24:33 Batch  13660_51876 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_1 05/25/05 14:13:44 Batch  13660_51232 PROMOTE bckcett VERSION u08717 Brent
# MAGIC ^1_1 05/25/05 14:02:38 Batch  13660_50563 INIT bckccdt ids20 dsadm Brent
# MAGIC 
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
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC               B Leland           05/25/2005  -  Initial programming


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


ids_secret_name = get_widget_value('ids_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
Subject = get_widget_value('Subject','')
uv_secret_name = get_widget_value('uv_secret_name','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"SELECT TASK_SUBJ_NM as TASK_SUBJ_NM, TASK_NM as TASK_NM FROM {IDSOwner}.W_TASK_EXCL WHERE TASK_SUBJ_NM = '{Subject}'"
df_W_TASK_EXCL = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_concat = (
    df_W_TASK_EXCL
    .groupBy(F.trim(F.col("TASK_SUBJ_NM")).alias("SUBJECT"))
    .agg(F.concat_ws(";", F.collect_list(F.trim(F.col("TASK_NM")))).alias("TASKLIST"))
)

df_final = df_concat.select(
    F.rpad(F.col("SUBJECT"), 255, " ").alias("SUBJECT"),
    F.rpad(F.col("TASKLIST"), 4000, " ").alias("TASKLIST")
)

jdbc_url_uv, jdbc_props_uv = get_db_config(uv_secret_name)
drop_temp_sql = "DROP TABLE IF EXISTS STAGING.ExclusionListExtr_TASK_EXCL_temp"
execute_dml(drop_temp_sql, jdbc_url_uv, jdbc_props_uv)

df_final.write.format("jdbc") \
    .option("url", jdbc_url_uv) \
    .options(**jdbc_props_uv) \
    .option("dbtable", "STAGING.ExclusionListExtr_TASK_EXCL_temp") \
    .mode("overwrite") \
    .save()

merge_sql = """
MERGE INTO TASK_EXCL as T
USING STAGING.ExclusionListExtr_TASK_EXCL_temp as S
ON T.SUBJECT = S.SUBJECT
WHEN MATCHED THEN
UPDATE SET
 T.TASKLIST = S.TASKLIST
WHEN NOT MATCHED THEN
INSERT (SUBJECT, TASKLIST)
VALUES (S.SUBJECT, S.TASKLIST);
"""
execute_dml(merge_sql, jdbc_url_uv, jdbc_props_uv)