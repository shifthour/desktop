# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2006 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  IdsPullJobsToProcessExtr
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION: Call job to check for IDS job competion and email list of not finished or failed jobs.  
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  IdsJobCompleteByFrequencyCntl
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Date                 Developer                Change Description                                                                                                             Environment                                Reviewed By                            Reviewed Date
# MAGIC ------------------      ----------------------------     --------------------------------------------------------------------------------------------------------                                    -----------------------------                       ---------------------                            -------------------------
# MAGIC 2014-04-15      Dan Long                 New job.                                                                                                                              IntegrateNewDevl                       Bhoomi Dasari                            6/6/2014

# MAGIC Select Job Names to process from the #$FilePath#/load/IdsJobsToMonitor.dat file and format the Job Names into one record
# MAGIC This file (#$FilePath#/load/IdsJobsToMonitorExtracted.dat) is used in the job.
# MAGIC The record layout for the #$FilePath#/load/IdsJobsToMonitor.dat file is as follows: 
# MAGIC 
# MAGIC The first character is the Frequency of the Jobs 
# MAGIC After the comma delimiter the next 30 characters represent the Job name.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType
import pyspark.sql.functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


Frequency = get_widget_value('Frequency','M')

schema_IdsJobsToMonitor = StructType([
    StructField("Freq", StringType(), False),
    StructField("JobNm", StringType(), False)
])

df_IdsJobsToMonitor = (
    spark.read
    .option("delimiter", ",")
    .option("header", "false")
    .option("quote", "\u0000")
    .schema(schema_IdsJobsToMonitor)
    .csv(f"{adls_path}/load/IdsJobsToMonitor.dat")
)

df_Transformer_4 = df_IdsJobsToMonitor.filter(F.col("Freq") == Frequency).select("JobNm")
df_Transformer_4 = df_Transformer_4.withColumn("JobNm", F.rpad(F.col("JobNm"), <...>, " "))
df_Transformer_4 = df_Transformer_4.select("JobNm")

write_files(
    df_Transformer_4,
    f"{adls_path}/load/IdsJobNamesSelected.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\u0000",
    nullValue=None
)