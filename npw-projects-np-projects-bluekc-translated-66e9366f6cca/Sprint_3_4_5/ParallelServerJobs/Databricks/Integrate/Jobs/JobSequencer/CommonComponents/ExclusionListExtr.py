# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 01/30/08 05:00:10 Batch  14640_18019 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 12/26/07 08:56:40 Batch  14605_32206 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/21/07 15:09:17 Batch  14570_54560 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 08/23/07 08:22:09 Batch  14480_30134 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 03/07/07 16:30:43 Batch  14311_59451 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 01/26/06 07:54:36 Batch  13906_28483 INIT bckcetl ids20 dsadm Gina
# MAGIC ^1_2 07/29/05 14:39:17 Batch  13725_52764 INIT bckcetl ids20 dsadm Brent
# MAGIC ^1_1 07/29/05 13:02:36 Batch  13725_46962 INIT bckcetl ids20 dsadm Brent
# MAGIC ^1_1 07/06/05 08:44:13 Batch  13702_31459 PROMOTE bckcetl ids20 dsadm Gina Parr
# MAGIC ^1_1 07/06/05 08:41:57 Batch  13702_31322 INIT bckcett testIDS30 dsadm Gina Parr
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
import pyspark.sql.functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
Subject = get_widget_value('Subject','')
DSE_UVNLSOFF_SKIP_WARNINGS = get_widget_value('DSE_UVNLSOFF_SKIP_WARNINGS','')
UVNLSOFF = get_widget_value('UVNLSOFF','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"SELECT TASK_SUBJ_NM as TASK_SUBJ_NM, TASK_NM as TASK_NM FROM {IDSOwner}.W_TASK_EXCL WHERE TASK_SUBJ_NM = '{Subject}'"

df_W_TASK_EXCL = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_Concat = (
    df_W_TASK_EXCL
    .groupBy(F.trim(F.col("TASK_SUBJ_NM")).alias("SUBJECT"))
    .agg(F.collect_list(F.trim(F.col("TASK_NM"))).alias("tmp_task_list"))
    .withColumn(
        "TASKLIST",
        F.concat(F.lit(";"), F.concat_ws(";", F.col("tmp_task_list")), F.lit(";"))
    )
    .select("SUBJECT", "TASKLIST")
)

df_TASK_EXCL = df_Concat.select(
    F.col("SUBJECT"),
    F.col("TASKLIST")
)

df_TASK_EXCL = df_TASK_EXCL.withColumn("SUBJECT", F.rpad(F.col("SUBJECT"), <...>, " "))
df_TASK_EXCL = df_TASK_EXCL.withColumn("TASKLIST", F.rpad(F.col("TASKLIST"), <...>, " "))

drop_sql = "DROP TABLE IF EXISTS STAGING.ExclusionListExtr_TASK_EXCL_temp"
execute_dml(drop_sql, jdbc_url, jdbc_props)

(
    df_TASK_EXCL
    .write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", "STAGING.ExclusionListExtr_TASK_EXCL_temp")
    .mode("overwrite")
    .save()
)

merge_sql = """
MERGE TASK_EXCL as T
USING STAGING.ExclusionListExtr_TASK_EXCL_temp as S
ON T.SUBJECT = S.SUBJECT
WHEN MATCHED THEN
  UPDATE SET
    T.TASKLIST = S.TASKLIST
WHEN NOT MATCHED THEN
  INSERT (SUBJECT, TASKLIST)
  VALUES (S.SUBJECT, S.TASKLIST);
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)