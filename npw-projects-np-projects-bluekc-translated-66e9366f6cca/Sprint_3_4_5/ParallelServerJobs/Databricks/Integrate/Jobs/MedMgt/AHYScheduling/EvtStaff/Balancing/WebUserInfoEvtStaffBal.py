# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2011 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:    WebUserInfoAHYEvtSchedulingEvtStaffBalSeq
# MAGIC 
# MAGIC PROCESSING: Row Count errors balancing job for EVT_STAFF
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
from pyspark.sql.functions import col, rpad
from pyspark.sql.types import StructType, StructField, StringType
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
  EVT_STAFF.EVT_STAFF_ID AS SRC_EVT_STAFF_ID,
  EVT_STAFF.SRC_SYS_CD_SK AS SRC_SRC_SYS_CD_SK,
  B_EVT_STAFF.EVT_STAFF_ID AS TRGT_EVT_STAFF_ID,
  B_EVT_STAFF.SRC_SYS_CD_SK AS TRGT_SRC_SYS_CD_SK
FROM {IDSOwner}.EVT_STAFF AS EVT_STAFF
FULL OUTER JOIN {IDSOwner}.B_EVT_STAFF AS B_EVT_STAFF
  ON EVT_STAFF.EVT_STAFF_ID = B_EVT_STAFF.EVT_STAFF_ID
  AND EVT_STAFF.SRC_SYS_CD_SK = B_EVT_STAFF.SRC_SYS_CD_SK
WHERE EVT_STAFF.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
"""

df_missing = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_research = df_missing.filter(
    col("SRC_EVT_STAFF_ID").isNull()
    | col("SRC_SRC_SYS_CD_SK").isNull()
    | col("TRGT_EVT_STAFF_ID").isNull()
    | col("TRGT_SRC_SYS_CD_SK").isNull()
).select(
    "TRGT_EVT_STAFF_ID",
    "TRGT_SRC_SYS_CD_SK",
    "SRC_EVT_STAFF_ID",
    "SRC_SRC_SYS_CD_SK"
)

if ToleranceCd == "OUT":
    temp_df_notify = spark.createDataFrame(
        [("ROW COUNT BALANCING WEBUSERINFO - IDS EVT_STAFF OUT OF TOLERANCE",)],
        StructType([StructField("NOTIFICATION", StringType(), True)])
    )
else:
    temp_df_notify = spark.createDataFrame(
        [],
        StructType([StructField("NOTIFICATION", StringType(), True)])
    )

df_notify = temp_df_notify.withColumn("NOTIFICATION", rpad(col("NOTIFICATION"), 70, " "))

write_files(
    df_research,
    f"{adls_path}/balancing/research/WebUserInfoAhyEvtStaffResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

write_files(
    df_notify.select("NOTIFICATION"),
    f"{adls_path}/balancing/notify/SchedulingBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)