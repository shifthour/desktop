# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2011 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:    WebUserInfoAHYEvtSchedulingEvtTypBalSeq
# MAGIC 
# MAGIC PROCESSING: Row Count errors balancing job for EVT_TYP
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
from pyspark.sql.functions import col, isnull, rpad
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
RunID = get_widget_value('RunID','')
ToleranceCd = get_widget_value('ToleranceCd','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"""SELECT
EVT_TYP.EVT_TYP_ID as SRC_EVT_TYP_ID,
EVT_TYP.SRC_SYS_CD_SK as SRC_SRC_SYS_CD_SK,
B_EVT_TYP.EVT_TYP_ID as TRGT_EVT_TYP_ID,
B_EVT_TYP.SRC_SYS_CD_SK as TRGT_SRC_SYS_CD_SK
FROM {IDSOwner}.EVT_TYP EVT_TYP
FULL OUTER JOIN {IDSOwner}.B_EVT_TYP B_EVT_TYP
  ON EVT_TYP.EVT_TYP_ID = B_EVT_TYP.EVT_TYP_ID
  AND EVT_TYP.SRC_SYS_CD_SK = B_EVT_TYP.SRC_SYS_CD_SK
WHERE EVT_TYP.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
"""

df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_Research = df_SrcTrgtComp.filter(
    (col("SRC_EVT_TYP_ID").isNull())
    | (col("SRC_SRC_SYS_CD_SK").isNull())
    | (col("TRGT_EVT_TYP_ID").isNull())
    | (col("TRGT_SRC_SYS_CD_SK").isNull())
)

df_Research = df_Research.select(
    "TRGT_EVT_TYP_ID",
    "TRGT_SRC_SYS_CD_SK",
    "SRC_EVT_TYP_ID",
    "SRC_SRC_SYS_CD_SK"
)

df_firstRow = df_SrcTrgtComp.limit(1)
if ToleranceCd == "OUT" and df_firstRow.count() == 1:
    df_Notify_temp = spark.createDataFrame(
        [("ROW COUNT BALANCING WEBUSERINFO - IDS EVT_TYP OUT OF TOLERANCE",)],
        ["NOTIFICATION"]
    )
else:
    df_Notify_temp = spark.createDataFrame([], ["NOTIFICATION"])

df_Notify = df_Notify_temp.select(
    rpad("NOTIFICATION", 70, " ").alias("NOTIFICATION")
)

write_files(
    df_Research,
    f"{adls_path}/balancing/research/WebUserInfoAhyEvtTypResearch.dat.{RunID}",
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