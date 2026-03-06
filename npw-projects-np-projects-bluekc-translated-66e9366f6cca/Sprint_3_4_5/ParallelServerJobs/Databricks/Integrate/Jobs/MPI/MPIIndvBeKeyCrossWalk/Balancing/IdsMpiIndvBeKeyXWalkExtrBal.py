# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2012 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by:
# MAGIC                     IdsMpiIndvBeKeyXWalkExtrBalSeq
# MAGIC 
# MAGIC Processing:
# MAGIC                     This is a daily process. These records are written to the MPI BCBS Extension Database from the MPI tool and provides any existing BE_KEY's that have received a new BE_KEY.
# MAGIC 
# MAGIC Control Job Rerun Information: 
# MAGIC                     Previous Run Successful:    Reload B_MPI_INDV_BE_CRSWALK table
# MAGIC                     Previous Run Aborted:         Restart, no other steps necessary
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                                        Project/                                                                                                                            Code                   Date
# MAGIC Developer                 Date              Altiris #          Change Description                                                                                     Reviewer            Reviewed
# MAGIC -------------------------------  -------------------   ------------------   -----------------------------------------------------------------------------------------------------------------   -------------------------  -------------------
# MAGIC Abhiram Dasarathy   07-19-2012    4426 - MPI    Original Programming                                                                                   Bhoomi Dasari    08/16/2012  
# MAGIC Hugh Sisson             2012-10-18    4426             Fixed TransformLogic stage to include all 8 columns from the DB2 stage    Kalyan Neelam   2012-11-15
# MAGIC                                                                             Fixed contraint to check all 8 columns for nulls

# MAGIC MPI CROSSWALK EXTRACT BALANCE JOB
# MAGIC Rows with Failed Comparison on Specified columns
# MAGIC Detail file for on-call to research errors
# MAGIC File checked later for rows and email to on-call
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


RunID = get_widget_value('RunID','')
ToleranceCd = get_widget_value('ToleranceCd','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"""SELECT 
MPI_INDV_BE_CRSWALK.MPI_MBR_ID AS SRC_MPI_MBR_ID,
MPI_INDV_BE_CRSWALK.PRCS_OWNER_ID AS SRC_PRCS_OWNER_ID,
MPI_INDV_BE_CRSWALK.SRC_SYS_ID AS SRC_SRC_SYS_ID,
MPI_INDV_BE_CRSWALK.PRCS_RUN_DTM AS SRC_PRCS_RUN_DTM,
B_MPI_INDV_BE_CRSWALK.MPI_MBR_ID AS TRGT_MPI_MBR_ID,
B_MPI_INDV_BE_CRSWALK.PRCS_OWNER_ID AS TRGT_PRCS_OWNER_ID,
B_MPI_INDV_BE_CRSWALK.SRC_SYS_ID AS TRGT_SRC_SYS_ID,
B_MPI_INDV_BE_CRSWALK.PRCS_RUN_DTM AS TRGT_PRCS_RUN_DTM
FROM {IDSOwner}.MPI_INDV_BE_CRSWALK MPI_INDV_BE_CRSWALK
FULL OUTER JOIN {IDSOwner}.B_MPI_INDV_BE_CRSWALK B_MPI_INDV_BE_CRSWALK
ON
MPI_INDV_BE_CRSWALK.MPI_MBR_ID = B_MPI_INDV_BE_CRSWALK.MPI_MBR_ID
AND
MPI_INDV_BE_CRSWALK.PRCS_RUN_DTM = B_MPI_INDV_BE_CRSWALK.PRCS_RUN_DTM
AND
MPI_INDV_BE_CRSWALK.PRCS_OWNER_ID = B_MPI_INDV_BE_CRSWALK.PRCS_OWNER_ID
AND
MPI_INDV_BE_CRSWALK.SRC_SYS_ID = B_MPI_INDV_BE_CRSWALK.SRC_SYS_ID
WHERE
MPI_INDV_BE_CRSWALK.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
"""
df_Missing = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_Research = df_Missing.filter(
    col("SRC_MPI_MBR_ID").isNull() |
    col("SRC_PRCS_OWNER_ID").isNull() |
    col("SRC_SRC_SYS_ID").isNull() |
    col("SRC_PRCS_RUN_DTM").isNull() |
    col("TRGT_MPI_MBR_ID").isNull() |
    col("TRGT_PRCS_OWNER_ID").isNull() |
    col("TRGT_SRC_SYS_ID").isNull() |
    col("TRGT_PRCS_RUN_DTM").isNull()
)
df_Research = df_Research.select(
    "TRGT_MPI_MBR_ID",
    "TRGT_PRCS_OWNER_ID",
    "TRGT_SRC_SYS_ID",
    "TRGT_PRCS_RUN_DTM",
    "SRC_MPI_MBR_ID",
    "SRC_PRCS_OWNER_ID",
    "SRC_SRC_SYS_ID",
    "SRC_PRCS_RUN_DTM"
)

if ToleranceCd == 'OUT' and df_Missing.limit(1).count() > 0:
    df_Notify = spark.createDataFrame(
        [("ROW COUNT BALANCING MPI BCBS EXTENSION - IDS MPI INDV BE CRSWALK OUT OF TOLERANCE",)],
        "NOTIFICATION: string"
    )
    df_Notify = df_Notify.select(rpad(col("NOTIFICATION"), 70, " ").alias("NOTIFICATION"))
else:
    df_Notify = spark.createDataFrame([], "NOTIFICATION: string")

write_files(
    df_Notify,
    f"{adls_path}/balancing/notify/MPIIndvBeXWalkNotification.dat",
    ",",
    "append",
    False,
    False,
    "\"",
    None
)

write_files(
    df_Research,
    f"{adls_path}/balancing/research/IdsMpiIndvBeXWalkExtrBalResearch.dat.{RunID}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)