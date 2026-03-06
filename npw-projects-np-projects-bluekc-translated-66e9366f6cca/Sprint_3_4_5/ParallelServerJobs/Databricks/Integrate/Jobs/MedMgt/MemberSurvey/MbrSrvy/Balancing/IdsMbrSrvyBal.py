# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2014 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:    IdsMbrSrvyRowCntBalSeq
# MAGIC 
# MAGIC PROCESSING: Row Count errors balancing job for MBR_SRVY
# MAGIC                       
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                          Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Kalyan Neelam                2014-09-15           TFS 9558                        Initial Programming                                            IntegrateNewDevl       Bhoomi Dasari             10/20/2014

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


RunID = get_widget_value('RunID','')
ToleranceCd = get_widget_value('ToleranceCd','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
SrcSysCd = get_widget_value('SrcSysCd','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = """SELECT 
MBR_SRVY.MBR_SRVY_TYP_CD,
MBR_SRVY.SRC_SYS_CD_SK,
B_MBR_SRVY.MBR_SRVY_TYP_CD,
B_MBR_SRVY.SRC_SYS_CD_SK
FROM #$IDSOwner#.MBR_SRVY MBR_SRVY
FULL OUTER JOIN #$IDSOwner#.B_MBR_SRVY B_MBR_SRVY
ON MBR_SRVY.MBR_SRVY_TYP_CD = B_MBR_SRVY.MBR_SRVY_TYP_CD
AND MBR_SRVY.SRC_SYS_CD_SK = B_MBR_SRVY.SRC_SYS_CD_SK
WHERE 
MBR_SRVY.LAST_UPDT_RUN_CYC_EXCTN_SK >= #ExtrRunCycle#
"""
df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_SrcTrgtComp = df_SrcTrgtComp.select(
    F.col("MBR_SRVY_TYP_CD").alias("SRC_MBR_SRVY_TYP_CD"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SRC_SYS_CD_SK"),
    F.col("MBR_SRVY_TYP_CD_1").alias("TRGT_MBR_SRVY_TYP_CD"),
    F.col("SRC_SYS_CD_SK_1").alias("TRGT_SRC_SYS_CD_SK")
)

df_Research = df_SrcTrgtComp.filter(
    (F.col("SRC_MBR_SRVY_TYP_CD").isNull())
    | (F.col("SRC_SRC_SYS_CD_SK").isNull())
    | (F.col("TRGT_MBR_SRVY_TYP_CD").isNull())
    | (F.col("TRGT_SRC_SYS_CD_SK").isNull())
).select(
    "TRGT_MBR_SRVY_TYP_CD",
    "TRGT_SRC_SYS_CD_SK",
    "SRC_MBR_SRVY_TYP_CD",
    "SRC_SRC_SYS_CD_SK"
)

if ToleranceCd == 'OUT':
    df_notify_temp = df_SrcTrgtComp.limit(1)
    df_notify = df_notify_temp.select(
        F.rpad(
            F.lit("ROW COUNT BALANCING " + SrcSysCd + " - IDS MBR_SRVY OUT OF TOLERANCE"),
            70,
            " "
        ).alias("NOTIFICATION")
    )
else:
    empty_schema = StructType([StructField("NOTIFICATION", StringType(), True)])
    df_notify = spark.createDataFrame([], schema=empty_schema)

write_files(
    df_Research,
    f"{adls_path}/balancing/research/IdsMbrSrvyResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

write_files(
    df_notify,
    f"{adls_path}/balancing/notify/MemberSurveyBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)