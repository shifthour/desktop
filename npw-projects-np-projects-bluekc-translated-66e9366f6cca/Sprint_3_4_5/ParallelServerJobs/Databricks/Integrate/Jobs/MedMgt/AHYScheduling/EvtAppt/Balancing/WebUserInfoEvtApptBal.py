# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2011 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:    WebUserInfoAHYEvtSchedulingEvtApptBalSeq
# MAGIC 
# MAGIC PROCESSING: Row Count errors balancing job for EVT_APPT
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
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, rpad
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
RunID = get_widget_value('RunID','')
ToleranceCd = get_widget_value('ToleranceCd','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = f"""
SELECT
EVT_APPT.GRP_ID AS SRC_GRP_ID,
EVT_APPT.EVT_DT_SK AS SRC_EVT_DT_SK,
EVT_APPT.EVT_TYP_ID AS SRC_EVT_TYP_ID,
EVT_APPT.SEQ_NO AS SRC_SEQ_NO,
EVT_APPT.EVT_APPT_STRT_TM_TX AS SRC_EVT_APPT_STRT_TM_TX,
EVT_APPT.SRC_SYS_CD_SK AS SRC_SRC_SYS_CD_SK,
B_EVT_APPT.GRP_ID AS TRGT_GRP_ID,
B_EVT_APPT.EVT_DT_SK AS TRGT_EVT_DT_SK,
B_EVT_APPT.EVT_TYP_ID AS TRGT_EVT_TYP_ID,
B_EVT_APPT.SEQ_NO AS TRGT_SEQ_NO,
B_EVT_APPT.EVT_APPT_STRT_TM_TX AS TRGT_EVT_APPT_STRT_TM_TX,
B_EVT_APPT.SRC_SYS_CD_SK AS TRGT_SRC_SYS_CD_SK
FROM {IDSOwner}.EVT_APPT EVT_APPT
FULL OUTER JOIN {IDSOwner}.B_EVT_APPT B_EVT_APPT
ON EVT_APPT.GRP_ID = B_EVT_APPT.GRP_ID
AND EVT_APPT.EVT_DT_SK = B_EVT_APPT.EVT_DT_SK
AND EVT_APPT.EVT_TYP_ID = B_EVT_APPT.EVT_TYP_ID
AND EVT_APPT.SEQ_NO = B_EVT_APPT.SEQ_NO
AND EVT_APPT.EVT_APPT_STRT_TM_TX = B_EVT_APPT.EVT_APPT_STRT_TM_TX
AND EVT_APPT.SRC_SYS_CD_SK = B_EVT_APPT.SRC_SYS_CD_SK
WHERE EVT_APPT.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
"""

df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_Research = df_SrcTrgtComp.filter(
    (col("SRC_GRP_ID").isNull())
    | (col("SRC_EVT_DT_SK").isNull())
    | (col("SRC_EVT_TYP_ID").isNull())
    | (col("SRC_SEQ_NO").isNull())
    | (col("SRC_EVT_APPT_STRT_TM_TX").isNull())
    | (col("SRC_SRC_SYS_CD_SK").isNull())
    | (col("TRGT_GRP_ID").isNull())
    | (col("TRGT_EVT_DT_SK").isNull())
    | (col("TRGT_EVT_TYP_ID").isNull())
    | (col("TRGT_SEQ_NO").isNull())
    | (col("TRGT_EVT_APPT_STRT_TM_TX").isNull())
    | (col("TRGT_SRC_SYS_CD_SK").isNull())
)

df_Research = df_Research.withColumn("TRGT_EVT_DT_SK", rpad(col("TRGT_EVT_DT_SK"), 10, " "))
df_Research = df_Research.withColumn("SRC_EVT_DT_SK", rpad(col("SRC_EVT_DT_SK"), 10, " "))
df_Research = df_Research.select(
    "TRGT_GRP_ID",
    "TRGT_EVT_DT_SK",
    "TRGT_EVT_TYP_ID",
    "TRGT_SEQ_NO",
    "TRGT_EVT_APPT_STRT_TM_TX",
    "TRGT_SRC_SYS_CD_SK",
    "SRC_GRP_ID",
    "SRC_EVT_DT_SK",
    "SRC_EVT_TYP_ID",
    "SRC_SEQ_NO",
    "SRC_EVT_APPT_STRT_TM_TX",
    "SRC_SRC_SYS_CD_SK"
)

if ToleranceCd == 'OUT':
    df_Notify = spark.createDataFrame(
        [("ROW COUNT BALANCING WEBUSERINFO - IDS EVT_APPT OUT OF TOLERANCE",)],
        StructType([StructField("NOTIFICATION", StringType(), True)])
    )
else:
    df_Notify = spark.createDataFrame(
        [],
        StructType([StructField("NOTIFICATION", StringType(), True)])
    )

df_Notify = df_Notify.withColumn("NOTIFICATION", rpad(col("NOTIFICATION"), 70, " "))

write_files(
    df_Research,
    f"{adls_path}/balancing/research/WebUserInfoAhyEvtApptResearch.dat.{RunID}",
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