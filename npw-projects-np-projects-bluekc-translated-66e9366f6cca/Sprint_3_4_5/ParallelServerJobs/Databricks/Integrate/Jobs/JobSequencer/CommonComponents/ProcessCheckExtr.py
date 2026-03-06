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
# MAGIC ^1_1 11/07/06 10:31:19 Batch  14191_37885 PROMOTE bckcetl ids20 dsadm Keith for Brent
# MAGIC ^1_1 11/07/06 10:29:22 Batch  14191_37768 INIT bckcett testIDS30 dsadm Keith for Brent
# MAGIC 
# MAGIC 
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:    ProcessCheckExtr
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:   Using the P_RUN_CYC table check for other processes running that impact the callers ability to extract and update data.
# MAGIC       
# MAGIC 
# MAGIC INPUTS:	 Inputs are values from the callers perspective.
# MAGIC 
# MAGIC                  Subject name    (CLAIM, MEMBER, etc.)
# MAGIC                  Source System  (IDS, EDW, CDM)
# MAGIC                  Target System   (IDS, EDW, CDM)
# MAGIC 
# MAGIC   
# MAGIC HASH FILES:  none
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  None
# MAGIC                         
# MAGIC    
# MAGIC PROCESSING:   Search P_RUN_CYC table for any rows with a status of "RUNNING" for a subject where the callers source system is being update or the callers target system is being extracted from.  Calling Sequencer will check row count exiting transform to determine whether to continue.
# MAGIC  
# MAGIC 
# MAGIC OUTPUTS:  Output is not written anywhere.  Calling sequencer only cares about row count coming out of the transform stage.
# MAGIC                     
# MAGIC TEST CASES:  
# MAGIC                          Test                                                                                                          Results
# MAGIC                          1. Source being updated by another process                                           Returns one row
# MAGIC                          2. Source being extracted from by another source                                   Returns zero rows
# MAGIC                          3. Target being updated by another process for same subject                 Returns one row
# MAGIC                          4. Target being extracted from for same subject                                       Returns one row
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC               B Leland           03/22/2006  -  Initial programming
# MAGIC               B Leland           08/14/2006  -  Changed SQL logic to correctly see jobs running.
# MAGIC               B Leland           10/30/2006  -  Added additional SQL to union statement to look for jobs extracting from the system being updated.


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
Subject = get_widget_value('Subject','')
TargetSys = get_widget_value('TargetSys','')
SourceSys = get_widget_value('SourceSys','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = f"""select SUBJ_CD, TRGT_SYS_CD, SRC_SYS_CD
from {IDSOwner}.P_RUN_CYC
where CYC_STTUS_CD = 'RUNNING'
   and SUBJ_CD = '{Subject}'
   and TRGT_SYS_CD = '{SourceSys}'

UNION

select SUBJ_CD, TRGT_SYS_CD, SRC_SYS_CD
from {IDSOwner}.P_RUN_CYC
where CYC_STTUS_CD = 'RUNNING'
   and SUBJ_CD = '{Subject}'
   and SRC_SYS_CD <> '{SourceSys}'
   and TRGT_SYS_CD = '{TargetSys}'

UNION

select SUBJ_CD, TRGT_SYS_CD, SRC_SYS_CD
from {IDSOwner}.P_RUN_CYC
where CYC_STTUS_CD = 'RUNNING'
  and SUBJ_CD = '{Subject}'
  and SRC_SYS_CD = '{TargetSys}'

{IDSOwner}.W_TASK_EXCL
"""

df_P_RUN_CYC = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_Trans1 = df_P_RUN_CYC.select(
    col("SUBJ_CD").alias("SUBJ_CD"),
    col("TRGT_SYS_CD").alias("TRGT_SYS_CD"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD")
)

df_Trans1_final = df_Trans1.select(
    rpad("SUBJ_CD", <...>, " ").alias("SUBJ_CD"),
    rpad("TRGT_SYS_CD", <...>, " ").alias("TRGT_SYS_CD"),
    rpad("SRC_SYS_CD", <...>, " ").alias("SRC_SYS_CD")
)

write_files(
    df_Trans1_final,
    f"{adls_path}/dev/null",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)