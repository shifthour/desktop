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
# MAGIC JOB NAME:    ExclusionListDelete
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:   Remove entries in the DB2 - W_TASK_EXCL and Universe TASK_EXCL tables for a subject.
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
# MAGIC PROCESSING:   Two rows are generated in the transform to initiate the delete in each environment.
# MAGIC  
# MAGIC 
# MAGIC OUTPUTS:  None
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
from pyspark.sql.types import StructField, StructType, StringType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
subject = get_widget_value('Subject','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = f"SELECT DISTINCT(TASK_SUBJ_NM) as TASK_SUBJ_NM FROM {IDSOwner}.W_TASK_EXCL WHERE TASK_SUBJ_NM = '{subject}'"
df_W_TASK_EXCL = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_exclusion = df_W_TASK_EXCL.filter(F.col("TASK_SUBJ_NM").isNotNull())

df_xo = df_exclusion.withColumn("TASK_SUBJ_NM", F.lit(subject))
df_xo = df_xo.withColumn("TASK_SUBJ_NM", rpad("TASK_SUBJ_NM", 50, " "))
df_xo = df_xo.select("TASK_SUBJ_NM")

df_xo_load = df_exclusion.withColumn("SUBJECT", F.lit(subject))
df_xo_load = df_xo_load.withColumn("SUBJECT", rpad("SUBJECT", 50, " "))
df_xo_load = df_xo_load.select("SUBJECT")

temp_table_db2 = "STAGING.ExclusionListDelete_DB2_temp"
spark.sql(f"DROP TABLE IF EXISTS {temp_table_db2}")
(
    df_xo.write.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", temp_table_db2)
    .mode("overwrite")
    .save()
)

merge_sql_db2 = f"""
MERGE INTO {IDSOwner}.W_TASK_EXCL AS T
USING {temp_table_db2} AS S
ON T.TASK_SUBJ_NM = S.TASK_SUBJ_NM
WHEN MATCHED THEN
  DELETE;
"""
execute_dml(merge_sql_db2, jdbc_url, jdbc_props)

uv_secret_name = get_widget_value('uv_secret_name','<...>')
jdbc_url_uv, jdbc_props_uv = get_db_config(uv_secret_name)

temp_table_uv = "STAGING.ExclusionListDelete_task_excld_temp"
spark.sql(f"DROP TABLE IF EXISTS {temp_table_uv}")
(
    df_xo_load.write.format("jdbc")
    .option("url", jdbc_url_uv)
    .options(**jdbc_props_uv)
    .option("dbtable", temp_table_uv)
    .mode("overwrite")
    .save()
)

merge_sql_uv = f"""
MERGE INTO TASK_EXCL AS T
USING {temp_table_uv} AS S
ON T.SUBJECT = S.SUBJECT
WHEN MATCHED THEN
  DELETE;
"""
execute_dml(merge_sql_uv, jdbc_url_uv, jdbc_props_uv)