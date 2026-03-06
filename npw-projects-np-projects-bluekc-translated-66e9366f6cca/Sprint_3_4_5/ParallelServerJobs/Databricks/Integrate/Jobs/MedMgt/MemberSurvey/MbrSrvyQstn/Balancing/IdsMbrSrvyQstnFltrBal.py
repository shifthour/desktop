# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2014 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:    IdsMbrSrvyRspnRowCntBalSeq
# MAGIC 
# MAGIC PROCESSING: Row Count errors balancing job for MBR_SRVY_QSTN_FLTR
# MAGIC                       
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                          Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Kalyan Neelam                2014-08-18             TFS 9528                      Initial Programming                                            IntegrateNewDevl      Bhoomi Dasari               10/20/2014

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
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col, lit, rpad, concat
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


RunID = get_widget_value('RunID','')
ToleranceCd = get_widget_value('ToleranceCd','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
SrcSysCd = get_widget_value('SrcSysCd','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = f"""
SELECT
  MBR_SRVY_QSTN_FLTR.MBR_SRVY_TYP_CD AS SRC_MBR_SRVY_TYP_CD,
  MBR_SRVY_QSTN_FLTR.MBR_SRVY_QSTN_CD_TX AS SRC_MBR_SRVY_QSTN_CD_TX,
  MBR_SRVY_QSTN_FLTR.EFF_DT_SK AS SRC_EFF_DT_SK,
  MBR_SRVY_QSTN_FLTR.SRC_SYS_CD_SK AS SRC_SRC_SYS_CD_SK,
  B_MBR_SRVY_QSTN_FLTR.MBR_SRVY_TYP_CD AS TRGT_MBR_SRVY_TYP_CD,
  B_MBR_SRVY_QSTN_FLTR.MBR_SRVY_QSTN_CD_TX AS TRGT_MBR_SRVY_QSTN_CD_TX,
  B_MBR_SRVY_QSTN_FLTR.EFF_DT_SK AS TRGT_EFF_DT_SK,
  B_MBR_SRVY_QSTN_FLTR.SRC_SYS_CD_SK AS TRGT_SRC_SYS_CD_SK
FROM {IDSOwner}.MBR_SRVY_QSTN_FLTR MBR_SRVY_QSTN_FLTR
FULL OUTER JOIN {IDSOwner}.B_MBR_SRVY_QSTN_FLTR B_MBR_SRVY_QSTN_FLTR
  ON MBR_SRVY_QSTN_FLTR.MBR_SRVY_TYP_CD = B_MBR_SRVY_QSTN_FLTR.MBR_SRVY_TYP_CD
  AND MBR_SRVY_QSTN_FLTR.MBR_SRVY_QSTN_CD_TX = B_MBR_SRVY_QSTN_FLTR.MBR_SRVY_QSTN_CD_TX
  AND MBR_SRVY_QSTN_FLTR.EFF_DT_SK = B_MBR_SRVY_QSTN_FLTR.EFF_DT_SK
  AND MBR_SRVY_QSTN_FLTR.SRC_SYS_CD_SK = B_MBR_SRVY_QSTN_FLTR.SRC_SYS_CD_SK
WHERE MBR_SRVY_QSTN_FLTR.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
"""

df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

research_condition = (
    col("SRC_MBR_SRVY_TYP_CD").isNull() |
    col("SRC_MBR_SRVY_QSTN_CD_TX").isNull() |
    col("SRC_EFF_DT_SK").isNull() |
    col("SRC_SRC_SYS_CD_SK").isNull() |
    col("TRGT_MBR_SRVY_TYP_CD").isNull() |
    col("TRGT_MBR_SRVY_QSTN_CD_TX").isNull() |
    col("TRGT_EFF_DT_SK").isNull() |
    col("TRGT_SRC_SYS_CD_SK").isNull()
)

df_research = df_SrcTrgtComp.filter(research_condition)
df_research = df_research.withColumn("TRGT_EFF_DT_SK", rpad(col("TRGT_EFF_DT_SK"), 10, " "))
df_research = df_research.withColumn("SRC_EFF_DT_SK", rpad(col("SRC_EFF_DT_SK"), 10, " "))
df_research = df_research.select(
    "TRGT_MBR_SRVY_TYP_CD",
    "TRGT_MBR_SRVY_QSTN_CD_TX",
    "TRGT_EFF_DT_SK",
    "TRGT_SRC_SYS_CD_SK",
    "SRC_MBR_SRVY_TYP_CD",
    "SRC_MBR_SRVY_QSTN_CD_TX",
    "SRC_EFF_DT_SK",
    "SRC_SRC_SYS_CD_SK"
)

write_files(
    df_research,
    f"{adls_path}/balancing/research/IdsMbrSrvyQstnFltrResearch.dat.{RunID}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)

df_with_rownumNotify = df_SrcTrgtComp.withColumn("rownum", row_number().over(Window.orderBy(lit(1))))
df_notify = df_with_rownumNotify.filter((col("rownum") == 1) & (col("ToleranceCd") == "OUT"))
df_notify = df_notify.withColumn(
    "NOTIFICATION",
    concat(lit("ROW COUNT BALANCING "), col("SrcSysCd"), lit(" - IDS MBR_SRVY_QSTN_FLTR OUT OF TOLERANCE"))
)
df_notify = df_notify.withColumn("NOTIFICATION", rpad(col("NOTIFICATION"), 70, " "))
df_notify = df_notify.select("NOTIFICATION")

write_files(
    df_notify,
    f"{adls_path}/balancing/notify/MemberSurveyBalancingNotification.dat",
    ",",
    "append",
    False,
    False,
    "\"",
    None
)