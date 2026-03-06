# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2011 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:    WebUserInfoAHYEvtSchedulingEvtStaffTypBalSeq
# MAGIC 
# MAGIC PROCESSING: Row Count errors balancing job for EVT_STAFF_TYP
# MAGIC                       
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                          Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Kalyan Neelam                  2011-02-04             4529                            Initial Programming                                            IntegrateNewDevl       Steph Goddard            02/08/2011

# MAGIC File checked later for rows and email to on-call
# MAGIC Detail file for on-call to research errors
# MAGIC Rows with Failed Comparison on Specified columns
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


run_id = get_widget_value('RunID','')
tolerance_cd = get_widget_value('ToleranceCd','')
extr_run_cycle = get_widget_value('ExtrRunCycle','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = f"""
SELECT
EVT_STAFF_TYP.EVT_STAFF_TYP_ID AS SRC_EVT_STAFF_TYP_ID,
EVT_STAFF_TYP.SRC_SYS_CD_SK AS SRC_SRC_SYS_CD_SK,
B_EVT_STAFF_TYP.EVT_STAFF_TYP_ID AS TRGT_EVT_STAFF_TYP_ID,
B_EVT_STAFF_TYP.SRC_SYS_CD_SK AS TRGT_SRC_SYS_CD_SK
FROM {IDSOwner}.EVT_STAFF_TYP EVT_STAFF_TYP
     FULL OUTER JOIN {IDSOwner}.B_EVT_STAFF_TYP B_EVT_STAFF_TYP
       ON EVT_STAFF_TYP.EVT_STAFF_TYP_ID = B_EVT_STAFF_TYP.EVT_STAFF_TYP_ID
       AND EVT_STAFF_TYP.SRC_SYS_CD_SK = B_EVT_STAFF_TYP.SRC_SYS_CD_SK
WHERE EVT_STAFF_TYP.LAST_UPDT_RUN_CYC_EXCTN_SK >= {extr_run_cycle}
"""

df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_Research = df_SrcTrgtComp.filter(
    (F.col("SRC_EVT_STAFF_TYP_ID").isNull())
    | (F.col("SRC_SRC_SYS_CD_SK").isNull())
    | (F.col("TRGT_EVT_STAFF_TYP_ID").isNull())
    | (F.col("TRGT_SRC_SYS_CD_SK").isNull())
).select(
    "TRGT_EVT_STAFF_TYP_ID",
    "TRGT_SRC_SYS_CD_SK",
    "SRC_EVT_STAFF_TYP_ID",
    "SRC_SRC_SYS_CD_SK"
)

notify_schema = StructType([StructField("NOTIFICATION", StringType(), True)])
df_temp_notify = df_SrcTrgtComp.limit(1)
if tolerance_cd == 'OUT':
    if df_temp_notify.count() > 0:
        df_Notify = df_temp_notify.select(
            F.lit("ROW COUNT BALANCING WEBUSERINFO - IDS EVT_STAFF_TYP OUT OF TOLERANCE").alias("NOTIFICATION")
        )
    else:
        df_Notify = spark.createDataFrame([], notify_schema)
else:
    df_Notify = spark.createDataFrame([], notify_schema)

df_Research_out = df_Research.select(
    "TRGT_EVT_STAFF_TYP_ID",
    "TRGT_SRC_SYS_CD_SK",
    "SRC_EVT_STAFF_TYP_ID",
    "SRC_SRC_SYS_CD_SK"
)

research_file_path = f"{adls_path}/balancing/research/WebUserInfoAhyEvtStaffTypeResearch.dat.{run_id}"
write_files(
    df_Research_out,
    research_file_path,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_Notify = df_Notify.withColumn("NOTIFICATION", F.rpad(F.col("NOTIFICATION"), 70, " "))
df_Notify_out = df_Notify.select("NOTIFICATION")

notify_file_path = f"{adls_path}/balancing/notify/SchedulingBalancingNotification.dat"
write_files(
    df_Notify_out,
    notify_file_path,
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)