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
from pyspark.sql.functions import lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


ids_secret_name = get_widget_value('ids_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
Subject = get_widget_value('Subject','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"SELECT DISTINCT(TASK_SUBJ_NM) as TASK_SUBJ_NM FROM {IDSOwner}.W_TASK_EXCL WHERE TASK_SUBJ_NM = '{Subject}'"
df_W_TASK_EXCL = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_x_trigger_xo = (
    df_W_TASK_EXCL
    .filter("TASK_SUBJ_NM IS NOT NULL")
    .withColumn("TASK_SUBJ_NM", rpad(lit(Subject), 50, " "))
    .select("TASK_SUBJ_NM")
)

df_x_trigger_xo_load = (
    df_W_TASK_EXCL
    .filter("TASK_SUBJ_NM IS NOT NULL")
    .withColumn("SUBJECT", rpad(lit(Subject), 50, " "))
    .select("SUBJECT")
)

execute_dml("DROP TABLE IF EXISTS STAGING.ExclusionListDelete_DB2_temp", jdbc_url, jdbc_props)
(
    df_x_trigger_xo.write.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", "STAGING.ExclusionListDelete_DB2_temp")
    .mode("append")
    .save()
)
merge_sql_DB2 = f"""
MERGE {IDSOwner}.W_TASK_EXCL as T
USING STAGING.ExclusionListDelete_DB2_temp as S
ON T.TASK_SUBJ_NM = S.TASK_SUBJ_NM
WHEN MATCHED THEN DELETE;
"""
execute_dml(merge_sql_DB2, jdbc_url, jdbc_props)

unknown_secret_name = <...>
jdbc_url_2, jdbc_props_2 = get_db_config(<...>)
execute_dml("DROP TABLE IF EXISTS STAGING.ExclusionListDelete_task_excld_temp", jdbc_url_2, jdbc_props_2)
(
    df_x_trigger_xo_load.write.format("jdbc")
    .option("url", jdbc_url_2)
    .options(**jdbc_props_2)
    .option("dbtable", "STAGING.ExclusionListDelete_task_excld_temp")
    .mode("append")
    .save()
)
merge_sql_task_excld = """
MERGE TASK_EXCL as T
USING STAGING.ExclusionListDelete_task_excld_temp as S
ON T.SUBJECT = S.SUBJECT
WHEN MATCHED THEN DELETE;
"""
execute_dml(merge_sql_task_excld, jdbc_url_2, jdbc_props_2)