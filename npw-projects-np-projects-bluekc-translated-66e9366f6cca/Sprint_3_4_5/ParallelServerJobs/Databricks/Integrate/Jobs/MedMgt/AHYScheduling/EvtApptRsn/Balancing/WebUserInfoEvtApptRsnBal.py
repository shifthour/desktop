# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2011 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:    WebUserInfoAHYEvtSchedulingEvtApptRsnBalSeq
# MAGIC 
# MAGIC PROCESSING: Row Count errors balancing job for EVT_APPT_RSN
# MAGIC                       
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                          Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Kalyan Neelam                  2011-02-04             4529                            Initial Programming                                            IntegrateNewDevl      Steph Goddard              02/08/2011

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
from pyspark.sql.functions import col, rpad
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
RunID = get_widget_value('RunID','')
ToleranceCd = get_widget_value('ToleranceCd','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"""
SELECT
  EVT_APPT_RSN.EVT_TYP_ID AS SRC_EVT_TYP_ID,
  EVT_APPT_RSN.EVT_APPT_RSN_NM AS SRC_EVT_APPT_RSN_NM,
  EVT_APPT_RSN.EFF_DT_SK AS SRC_EFF_DT_SK,
  EVT_APPT_RSN.SRC_SYS_CD_SK AS SRC_SRC_SYS_CD_SK,
  B_EVT_APPT_RSN.EVT_TYP_ID AS TRGT_EVT_TYP_ID,
  B_EVT_APPT_RSN.EVT_APPT_RSN_NM AS TRGT_EVT_APPT_RSN_NM,
  B_EVT_APPT_RSN.EFF_DT_SK AS TRGT_EFF_DT_SK,
  B_EVT_APPT_RSN.SRC_SYS_CD_SK AS TRGT_SRC_SYS_CD_SK
FROM {IDSOwner}.EVT_APPT_RSN EVT_APPT_RSN
FULL OUTER JOIN {IDSOwner}.B_EVT_APPT_RSN B_EVT_APPT_RSN
  ON EVT_APPT_RSN.EVT_TYP_ID = B_EVT_APPT_RSN.EVT_TYP_ID
  AND EVT_APPT_RSN.EVT_APPT_RSN_NM = B_EVT_APPT_RSN.EVT_APPT_RSN_NM
  AND EVT_APPT_RSN.EFF_DT_SK = B_EVT_APPT_RSN.EFF_DT_SK
  AND EVT_APPT_RSN.SRC_SYS_CD_SK = B_EVT_APPT_RSN.SRC_SYS_CD_SK
WHERE EVT_APPT_RSN.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
"""

df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_Research = df_SrcTrgtComp.filter(
    col("SRC_EVT_TYP_ID").isNull()
    | col("SRC_EVT_APPT_RSN_NM").isNull()
    | col("SRC_EFF_DT_SK").isNull()
    | col("SRC_SRC_SYS_CD_SK").isNull()
    | col("TRGT_EVT_TYP_ID").isNull()
    | col("TRGT_EVT_APPT_RSN_NM").isNull()
    | col("TRGT_EFF_DT_SK").isNull()
    | col("TRGT_SRC_SYS_CD_SK").isNull()
)

df_Research = df_Research.select(
    col("TRGT_EVT_TYP_ID"),
    col("TRGT_EVT_APPT_RSN_NM"),
    rpad("TRGT_EFF_DT_SK", 10, " ").alias("TRGT_EFF_DT_SK"),
    col("TRGT_SRC_SYS_CD_SK"),
    col("SRC_EVT_TYP_ID"),
    col("SRC_EVT_APPT_RSN_NM"),
    rpad("SRC_EFF_DT_SK", 10, " ").alias("SRC_EFF_DT_SK"),
    col("SRC_SRC_SYS_CD_SK")
)

rowCount = df_SrcTrgtComp.count()
if rowCount > 0 and ToleranceCd == 'OUT':
    df_Notify = spark.createDataFrame(
        [("ROW COUNT BALANCING WEBUSERINFO - IDS EVT_APPT_RSN OUT OF TOLERANCE",)],
        ["NOTIFICATION"]
    )
else:
    empty_schema = StructType([StructField("NOTIFICATION", StringType(), True)])
    df_Notify = spark.createDataFrame([], empty_schema)

df_Notify = df_Notify.select(rpad("NOTIFICATION", 70, " ").alias("NOTIFICATION"))

write_files(
    df_Research,
    f"{adls_path}/balancing/research/WebUserInfoAhyEvtApptRsnResearch.dat.{RunID}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)

write_files(
    df_Notify,
    f"{adls_path}/balancing/notify/SchedulingBalancingNotification.dat",
    ",",
    "append",
    False,
    False,
    "\"",
    None
)