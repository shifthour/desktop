# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2011 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:    WebUserInfoAHYEvtSchedulingEvtLocBalSeq
# MAGIC 
# MAGIC PROCESSING: Row Count errors balancing job for EVT_LOC
# MAGIC                       
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                          Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Kalyan Neelam                  2011-02-04             4529                            Initial Programming                                            IntegrateNewDevl      Steph Goddard             02/08/2011

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
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, lit, row_number, rpad
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


run_id = get_widget_value('RunID','')
toleranceCd = get_widget_value('ToleranceCd','')
extrRunCycle = get_widget_value('ExtrRunCycle','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = f"""
SELECT
EVT_LOC.EVT_LOC_ID AS SRC_EVT_LOC_ID,
EVT_LOC.SRC_SYS_CD_SK AS SRC_SRC_SYS_CD_SK,
B_EVT_LOC.EVT_LOC_ID AS TRGT_EVT_LOC_ID,
B_EVT_LOC.SRC_SYS_CD_SK AS TRGT_SRC_SYS_CD_SK
FROM {IDSOwner}.EVT_LOC EVT_LOC
FULL OUTER JOIN {IDSOwner}.B_EVT_LOC B_EVT_LOC
ON EVT_LOC.EVT_LOC_ID = B_EVT_LOC.EVT_LOC_ID
AND EVT_LOC.SRC_SYS_CD_SK = B_EVT_LOC.SRC_SYS_CD_SK
WHERE
EVT_LOC.LAST_UPDT_RUN_CYC_EXCTN_SK >= {extrRunCycle}
"""

df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_missing = df_SrcTrgtComp

df_research = (
    df_missing.filter(
        "SRC_EVT_LOC_ID IS NULL OR SRC_SRC_SYS_CD_SK IS NULL OR TRGT_EVT_LOC_ID IS NULL OR TRGT_SRC_SYS_CD_SK IS NULL"
    )
    .select(
        "TRGT_EVT_LOC_ID",
        "TRGT_SRC_SYS_CD_SK",
        "SRC_EVT_LOC_ID",
        "SRC_SRC_SYS_CD_SK"
    )
)

window_spec = Window.orderBy(lit(1))
df_missing_withRowNum = df_missing.withColumn("__rownum", row_number().over(window_spec))

if toleranceCd == 'OUT':
    df_notify_temp = df_missing_withRowNum.filter(col("__rownum") == 1).select(
        lit("ROW COUNT BALANCING WEBUSERINFO - IDS EVT_LOC OUT OF TOLERANCE").alias("NOTIFICATION")
    )
else:
    schema_notify = StructType([StructField("NOTIFICATION", StringType(), True)])
    df_notify_temp = spark.createDataFrame([], schema_notify)

df_notify = df_notify_temp.select(
    rpad("NOTIFICATION", 70, " ").alias("NOTIFICATION")
)

write_files(
    df_research,
    f"{adls_path}/balancing/research/WebUserInfoAhyEvtLocResearch.dat.{run_id}",
    ",",
    "overwrite",
    False,
    False,
    '"',
    None
)

write_files(
    df_notify,
    f"{adls_path}/balancing/notify/SchedulingBalancingNotification.dat",
    ",",
    "append",
    False,
    False,
    '"',
    None
)