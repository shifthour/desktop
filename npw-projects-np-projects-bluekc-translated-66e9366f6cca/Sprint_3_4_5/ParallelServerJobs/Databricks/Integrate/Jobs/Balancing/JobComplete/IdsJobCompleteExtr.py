# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 03/07/07 16:30:43 Batch  14311_59451 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 07/27/06 13:58:47 Batch  14088_50337 PROMOTE bckcetl ids20 dsadm Keith for Brent
# MAGIC ^1_1 07/27/06 13:57:03 Batch  14088_50231 INIT bckcett testIDS30 dsadm Keith for Brent
# MAGIC ^1_2 07/27/06 10:58:07 Batch  14088_39491 PROMOTE bckcett testIDS30 u08717 Brent
# MAGIC ^1_2 07/27/06 10:57:33 Batch  14088_39456 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_1 07/27/06 10:52:48 Batch  14088_39172 INIT bckcett devlIDS30 u08717 Brent
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsArugsJobCompleteExtr
# MAGIC 
# MAGIC DESCRIPTION:  Query DataStage for IDS Arugs run time completion information.
# MAGIC       
# MAGIC 
# MAGIC INPUTS: DB2 SYSDUMMY1 table is used to create a row that drives the routine calls.
# MAGIC 
# MAGIC    
# MAGIC HASH FILES:  None 
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  AuditGetJobInfo(
# MAGIC 
# MAGIC                            
# MAGIC PROCESSING: Extract a row from the DB2 SYSDUMMY table to initiate AuditGetJobInfo() routines in the transform stage that extract DataStage dates and information about specified jobs.  If status is not "Finished" or run time is not today, email notification.
# MAGIC 
# MAGIC 
# MAGIC OUTPUTS: .../balancing/IdsArgusJobComplete.txt
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC              Brent Leland  07/20/2006    Originally Programmed
# MAGIC              Brent Leland  08/01/2006    Updated AuditGetJobInfo rountine to pass the correct values.
# MAGIC                                                           Changed search for status to Index routine since multiple Finished statuses are returned.

# MAGIC This SQL is only to initiate the routines in the Transform Stage.  The data row is never used.
# MAGIC Ids Argus Job Run Information Collection
# MAGIC Write out to txt file jobs that are not finished or have not run today into #$FilePath#/balancing
# MAGIC Only keep AuditGetJobInfo results that are for jobs still running or for jobs started before current date.   These types of jobs are those that need on-call notice.
# MAGIC Calls the routine AuditGetJobInfo for jobname provided by stage variable. 
# MAGIC AuditGetJobInfo is kept Rountines/Audit folder and returns job statistics.
# MAGIC StageVarriables for JobNames are the actual datastage job controller names.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import types as T
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


CurrDate = get_widget_value('CurrDate', '2006-08-01')
JobName1 = get_widget_value('JobName1', '')
IDSOwner = get_widget_value('IDSOwner', '')
ids_secret_name = get_widget_value('ids_secret_name', '')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
df_SYSDUMMY1 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", "SELECT IBMREQD FROM sysibm.sysdummy1")
    .load()
)

df_CallAuditGetJobInfo = (
    df_SYSDUMMY1
    .withColumn("JobName", F.lit(JobName1))
    .withColumn("JobStartTime", AuditGetJobInfo(JobName1, "Start"))
    .withColumn("JobStatus", AuditGetJobInfo(JobName1, "Status"))
    .withColumn("JobRunTime", AuditGetJobInfo(JobName1, "RunTime"))
    .withColumn("JobStartDate", AuditGetJobInfo(JobName1, "StartDate"))
)

df_Trans2 = (
    df_CallAuditGetJobInfo
    .filter(
        (F.instr(F.col("JobStatus"), "Finished") == 0)
        | (F.col("JobStartDate") != CurrDate)
    )
    .select(
        F.col("JobName"),
        F.col("JobStartTime"),
        F.col("JobStatus"),
        F.col("JobRunTime"),
        F.col("JobStartDate"),
        F.lit(CurrDate).alias("CurrentDate")
    )
)

write_files(
    df_Trans2.select(
        "JobName",
        "JobStartTime",
        "JobStatus",
        "JobRunTime",
        "JobStartDate",
        "CurrentDate"
    ),
    f"{adls_path}/balancing/{JobName1}Complete.txt",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)