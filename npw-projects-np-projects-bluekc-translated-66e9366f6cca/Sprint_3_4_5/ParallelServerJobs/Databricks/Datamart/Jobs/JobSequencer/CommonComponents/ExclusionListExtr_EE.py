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
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                       
# MAGIC DEVELOPER                         DATE                 PROJECT                                                  DESCRIPTION                                                          DATASTAGE   ENVIRONMENT                     CODE  REVIEW            REVIEW DATE                    
# MAGIC -----------------------------                 ----------------------      ------------------------------                                  -----------------------------------                                                ------------------------------                                          ------------------------------      --------------------
# MAGIC 
# MAGIC     B Leland                              05/25/2005  -                                                                   Initial programming
# MAGIC 
# MAGIC 
# MAGIC Nagesh Bandi                         07/01/2013        5114-Enterprise Efficiencies                 Extract subject records from W_TASK_EXCL                EnterpriseWrhseDevl                                     Jag Yelavarthi           2013-08-16

# MAGIC Extract subject records from W_TASK_EXCL and Loads into TASK_EXCL file
# MAGIC 
# MAGIC Job Name:
# MAGIC EXCLUSIONLISTExtr
# MAGIC Read TASK_SUBJ_NM,TASK_NM fields from W_TASK_EXCL table  from IDS.
# MAGIC This file has Job names which are supposed to skip from run cycle.
# MAGIC 
# MAGIC Job names from file TASK_EXCL are pulled in sequencer going further.
# MAGIC Concat TASKLIST
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
# MAGIC %run ../../../../Utility_DataMart3
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner', '')
subject = get_widget_value('Subject', '')
ids_secret_name = get_widget_value('ids_secret_name', '')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"SELECT TASK_SUBJ_NM, TASK_NM FROM {IDSOwner}.W_TASK_EXCL WHERE TASK_SUBJ_NM = '{subject}'"
df_db2_W_TASK_EXCL_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_xfm_Concat = (
    df_db2_W_TASK_EXCL_Extr
    .groupBy("TASK_SUBJ_NM")
    .agg(
        F.concat(
            F.concat_ws(';', F.collect_list(trim(F.col("TASK_NM")))),
            F.lit(';')
        ).alias("TASKLIST")
    )
    .withColumn("SUBJECT", trim(F.col("TASK_SUBJ_NM")))
)

df_final = (
    df_xfm_Concat
    .select(
        F.rpad(F.col("SUBJECT"), <...>, " ").alias("SUBJECT"),
        F.rpad(F.col("TASKLIST"), <...>, " ").alias("TASKLIST")
    )
)

write_files(
    df_final,
    f"{adls_path}/load/{subject}_TASK_EXCL.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)