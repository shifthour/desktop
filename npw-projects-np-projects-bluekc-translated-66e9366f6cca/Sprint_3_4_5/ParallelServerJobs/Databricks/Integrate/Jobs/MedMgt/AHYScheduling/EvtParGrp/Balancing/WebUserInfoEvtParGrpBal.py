# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2011 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:    WebUserInfoAHYEvtSchedulingEvtParGrpBalSeq
# MAGIC 
# MAGIC PROCESSING: Row Count errors balancing job for EVT_PAR_GRP
# MAGIC                       
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                          Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Manasa Andru               2012-04-03           4830 - AHY 3.0                Initial Programming                                            IntegrateCurDevl          SAndrew                       2012-04-24

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
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, expr, rpad

# Retrieve all parameter values
RunID = get_widget_value('RunID','')
ToleranceCd = get_widget_value('ToleranceCd','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

# Configure JDBC connection for IDS
jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# Read from DB2Connector Stage: SrcTrgtComp
extract_query = f"""
SELECT 
EVT_PAR_GRP.GRP_ID AS SRC_GRP_ID,
EVT_PAR_GRP.SRC_SYS_CD_SK AS SRC_SRC_SYS_CD_SK,
B_EVT_PAR_GRP.GRP_ID AS TRGT_GRP_ID,
B_EVT_PAR_GRP.SRC_SYS_CD_SK AS TRGT_SRC_SYS_CD_SK
FROM {IDSOwner}.EVT_PAR_GRP EVT_PAR_GRP
     FULL OUTER JOIN {IDSOwner}.B_EVT_PAR_GRP B_EVT_PAR_GRP
       ON EVT_PAR_GRP.GRP_ID = B_EVT_PAR_GRP.GRP_ID
          AND EVT_PAR_GRP.SRC_SYS_CD_SK = B_EVT_PAR_GRP.SRC_SYS_CD_SK
WHERE EVT_PAR_GRP.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
"""

df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# Transformer Stage: TransformLogic
df_Research = df_SrcTrgtComp.filter(
    col("SRC_GRP_ID").isNull()
    | col("SRC_SRC_SYS_CD_SK").isNull()
    | col("TRGT_GRP_ID").isNull()
    | col("TRGT_SRC_SYS_CD_SK").isNull()
)

df_Research = df_Research.select("TRGT_GRP_ID","TRGT_SRC_SYS_CD_SK","SRC_GRP_ID","SRC_SRC_SYS_CD_SK")

if ToleranceCd == 'OUT':
    df_temp = df_SrcTrgtComp.limit(1)
    df_Notify = df_temp.select(expr("'ROW COUNT BALANCING WEBUSERINFO - IDS EVT_PAR_GRP OUT OF TOLERANCE' AS NOTIFICATION"))
    df_Notify = df_Notify.withColumn("NOTIFICATION", rpad("NOTIFICATION", 70, " "))
else:
    # Create empty DataFrame for NOTIFICATION column
    from pyspark.sql.types import StructType, StructField, StringType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------

    empty_schema = StructType([StructField("NOTIFICATION", StringType(), True)])
    df_Notify = spark.createDataFrame([], empty_schema)
    df_Notify = df_Notify.select("NOTIFICATION")

# Write to ResearchFile
df_Research_final = df_Research.select("TRGT_GRP_ID","TRGT_SRC_SYS_CD_SK","SRC_GRP_ID","SRC_SRC_SYS_CD_SK")
write_files(
    df_Research_final,
    f"{adls_path}/balancing/research/WebUserInfoAhyEvtParGrpResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Write to ErrorNotificationFile
df_Notify_final = df_Notify.select("NOTIFICATION")
write_files(
    df_Notify_final,
    f"{adls_path}/balancing/notify/SchedulingBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)