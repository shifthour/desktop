# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2014 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:    IdsMbrSrvyAnsrRowCntBalSeq
# MAGIC 
# MAGIC PROCESSING: Row Count errors balancing job for MBR_SRVY_ANSWER
# MAGIC                       
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                          Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Kalyan Neelam                2014-09-15           TFS 9558                         Initial Programming                                            IntegrateNewDevl      Bhoomi Dasari             10/20/2014

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
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


# Parameters
RunID = get_widget_value("RunID","")
ToleranceCd = get_widget_value("ToleranceCd","")
ExtrRunCycle = get_widget_value("ExtrRunCycle","")
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
SrcSysCd = get_widget_value("SrcSysCd","")

# DB Config
jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# Read from DB2Connector (SrcTrgtComp)
extract_query = f"""
SELECT
MBR_SRVY_ANSWER.MBR_SRVY_TYP_CD AS SRC_MBR_SRVY_TYP_CD,
MBR_SRVY_ANSWER.MBR_SRVY_QSTN_CD_TX AS SRC_MBR_SRVY_QSTN_CD_TX,
MBR_SRVY_ANSWER.MBR_SRVY_ANSWER_CD_TX AS SRC_MBR_SRVY_ANSWER_CD_TX,
MBR_SRVY_ANSWER.SRC_SYS_CD_SK AS SRC_SRC_SYS_CD_SK,
B_MBR_SRVY_ANSWER.MBR_SRVY_TYP_CD AS TRGT_MBR_SRVY_TYP_CD,
B_MBR_SRVY_ANSWER.MBR_SRVY_QSTN_CD_TX AS TRGT_MBR_SRVY_QSTN_CD_TX,
B_MBR_SRVY_ANSWER.MBR_SRVY_ANSWER_CD_TX AS TRGT_MBR_SRVY_ANSWER_CD_TX,
B_MBR_SRVY_ANSWER.SRC_SYS_CD_SK AS TRGT_SRC_SYS_CD_SK
FROM {IDSOwner}.MBR_SRVY_ANSWER MBR_SRVY_ANSWER
FULL OUTER JOIN {IDSOwner}.B_MBR_SRVY_ANSWER B_MBR_SRVY_ANSWER
  ON MBR_SRVY_ANSWER.MBR_SRVY_TYP_CD = B_MBR_SRVY_ANSWER.MBR_SRVY_TYP_CD
    AND MBR_SRVY_ANSWER.MBR_SRVY_QSTN_CD_TX = B_MBR_SRVY_ANSWER.MBR_SRVY_QSTN_CD_TX
    AND MBR_SRVY_ANSWER.MBR_SRVY_ANSWER_CD_TX = B_MBR_SRVY_ANSWER.MBR_SRVY_ANSWER_CD_TX
    AND MBR_SRVY_ANSWER.SRC_SYS_CD_SK = B_MBR_SRVY_ANSWER.SRC_SYS_CD_SK
WHERE
MBR_SRVY_ANSWER.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
"""
df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# Transformer Stage (TransformLogic) - Research link
df_Research = df_SrcTrgtComp.filter(
    (F.col("SRC_MBR_SRVY_TYP_CD").isNull())
    | (F.col("SRC_MBR_SRVY_QSTN_CD_TX").isNull())
    | (F.col("SRC_MBR_SRVY_ANSWER_CD_TX").isNull())
    | (F.col("SRC_SRC_SYS_CD_SK").isNull())
    | (F.col("TRGT_MBR_SRVY_TYP_CD").isNull())
    | (F.col("TRGT_MBR_SRVY_QSTN_CD_TX").isNull())
    | (F.col("TRGT_MBR_SRVY_ANSWER_CD_TX").isNull())
    | (F.col("TRGT_SRC_SYS_CD_SK").isNull())
)
df_Research = df_Research.select(
    F.col("TRGT_MBR_SRVY_TYP_CD").alias("TRGT_MBR_SRVY_TYP_CD"),
    F.col("TRGT_MBR_SRVY_QSTN_CD_TX").alias("TRGT_QSTN_CD_TX"),
    F.col("TRGT_MBR_SRVY_ANSWER_CD_TX").alias("TRGT_MBR_SRVY_ANSWER_CD_TX"),
    F.col("TRGT_SRC_SYS_CD_SK").alias("TRGT_SRC_SYS_CD_SK"),
    F.col("SRC_MBR_SRVY_TYP_CD").alias("SRC_MBR_SRVY_TYP_CD"),
    F.col("SRC_MBR_SRVY_QSTN_CD_TX").alias("SRC_QSTN_CD_TX"),
    F.col("SRC_MBR_SRVY_ANSWER_CD_TX").alias("SRC_MBR_SRVY_ANSWER_CD_TX"),
    F.col("SRC_SRC_SYS_CD_SK").alias("SRC_SRC_SYS_CD_SK")
)

# Transformer Stage (TransformLogic) - Notify link
df_Notify = df_SrcTrgtComp.filter(F.lit(ToleranceCd) == F.lit("OUT")).limit(1)
df_Notify = df_Notify.select(
    F.lit(f"ROW COUNT BALANCING {SrcSysCd} - IDS MBR_SRVY_ANSWER OUT OF TOLERANCE").alias("NOTIFICATION")
)
df_Notify = df_Notify.withColumn("NOTIFICATION", F.rpad(F.col("NOTIFICATION"), 70, " "))

# ResearchFile output
write_files(
    df_Research,
    f"{adls_path}/balancing/research/IdsMbrSrvyAnswerResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# ErrorNotificationFile output
write_files(
    df_Notify,
    f"{adls_path}/balancing/notify/MemberSurveyBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)