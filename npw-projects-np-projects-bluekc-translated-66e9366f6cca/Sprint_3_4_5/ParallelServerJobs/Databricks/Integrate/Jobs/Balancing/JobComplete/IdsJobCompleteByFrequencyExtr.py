# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2006 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:     IdsJobCompleteByFrequencyExtr
# MAGIC 
# MAGIC DESCRIPTION:  Query DataStage for IDS run time completion information.
# MAGIC 
# MAGIC CALLED BY:  IdsJobCompleteByFrequencyCntl
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Date                 Developer                Change Description                                                                                                             Environment                                Reviewed By                            Reviewed Date
# MAGIC ------------------      ----------------------------     --------------------------------------------------------------------------------------------------------                                    -----------------------------                       ---------------------                            -------------------------
# MAGIC 2014-04-15      Dan Long                 New job.                                                                                                                              IntegrateNewDevl                        Bhoomi Dasari                          6/6/2014

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
from pyspark.sql.functions import col, lit, substring
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


LastProcessedDate = get_widget_value('LastProcessedDate','2014-05-20')
IDSOwner = get_widget_value('IDSOwner','$PROJDEF')
ids_secret_name = get_widget_value('ids_secret_name','')
Frequency = get_widget_value('Frequency','')
JobName1 = get_widget_value('JobName1','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
df_SYSDUMMY1 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", "SELECT IBMREQD FROM sysibm.sysdummy1")
    .load()
)

df_CallAuditGetJobInfo = df_SYSDUMMY1.withColumn("JobName", lit(JobName1)) \
    .withColumn("JobStartTime", AuditGetJobInfo(JobName1, "Start")) \
    .withColumn("JobStatus", AuditGetJobInfo(JobName1, "Status")) \
    .withColumn("JobRunTime", AuditGetJobInfo(JobName1, "RunTime")) \
    .withColumn("JobStartDate", AuditGetJobInfo(JobName1, "StartDate"))

df_Trans2_intermediate = df_CallAuditGetJobInfo.filter(
    substring(col("JobStartDate"), 1, 10) < lit(LastProcessedDate)
)

df_Trans2 = df_Trans2_intermediate.select(
    col("JobName").alias("JobName"),
    col("JobStartTime").alias("JobStartTime"),
    col("JobStatus").alias("JobStatus"),
    col("JobRunTime").alias("JobRunTime"),
    col("JobStartDate").alias("JobStartDate"),
    lit(LastProcessedDate).alias("LastProcessedDate")
)

write_files(
    df_Trans2,
    f"{adls_path}/balancing/{JobName1}Complete.txt",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)