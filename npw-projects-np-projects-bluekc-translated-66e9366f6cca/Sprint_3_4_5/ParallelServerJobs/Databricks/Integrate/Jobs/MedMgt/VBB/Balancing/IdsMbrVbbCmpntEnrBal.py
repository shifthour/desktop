# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2011 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC PROCESSING: Extracts Target and Source Natural Key columns to Research errors
# MAGIC 
# MAGIC Called by : IdsMbrVbbCmpntEnrBalSeq
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                       Date               Project/Altiris #               Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------     -------------------   -----------------------------------    ---------------------------------------------------------   ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Raja Gummadi              2013-09-25       4963 - VBB                       Original Programming                           IntegrateNewDevl        Kalyan Neelam             2013-10-28

# MAGIC File checked later for rows and email to on-call
# MAGIC Detail file for on-call to research errors
# MAGIC Rows with Failed Comparison on Specified columns
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import StructType, StructField, StringType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
RunID = get_widget_value('RunID','')
ToleranceCd = get_widget_value('ToleranceCd','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
SELECT
MBR_VBB_CMPNT_ENR.MBR_UNIQ_KEY AS SRC_MBR_UNIQ_KEY,
MBR_VBB_CMPNT_ENR.VBB_CMPNT_UNIQ_KEY AS SRC_VBB_CMPNT_UNIQ_KEY,
MBR_VBB_CMPNT_ENR.SRC_SYS_CD_SK AS SRC_SRC_SYS_CD_SK,
B_MBR_VBB_CMPNT_ENR.MBR_UNIQ_KEY AS TRGT_MBR_UNIQ_KEY,
B_MBR_VBB_CMPNT_ENR.VBB_CMPNT_UNIQ_KEY AS TRGT_VBB_CMPNT_UNIQ_KEY,
B_MBR_VBB_CMPNT_ENR.SRC_SYS_CD_SK AS TRGT_SRC_SYS_CD_SK
FROM {IDSOwner}.MBR_VBB_CMPNT_ENR MBR_VBB_CMPNT_ENR
FULL OUTER JOIN {IDSOwner}.B_MBR_VBB_CMPNT_ENR B_MBR_VBB_CMPNT_ENR
ON (MBR_VBB_CMPNT_ENR.MBR_UNIQ_KEY = B_MBR_VBB_CMPNT_ENR.MBR_UNIQ_KEY
AND MBR_VBB_CMPNT_ENR.VBB_CMPNT_UNIQ_KEY = B_MBR_VBB_CMPNT_ENR.VBB_CMPNT_UNIQ_KEY
AND MBR_VBB_CMPNT_ENR.SRC_SYS_CD_SK = B_MBR_VBB_CMPNT_ENR.SRC_SYS_CD_SK)
WHERE MBR_VBB_CMPNT_ENR.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
"""
    )
    .load()
)

df_Research = df_SrcTrgtComp.filter(
    (F.col("SRC_SRC_SYS_CD_SK").isNull())
    | (F.col("SRC_MBR_UNIQ_KEY").isNull())
    | (F.col("TRGT_SRC_SYS_CD_SK").isNull())
    | (F.col("SRC_VBB_CMPNT_UNIQ_KEY").isNull())
    | (F.col("TRGT_MBR_UNIQ_KEY").isNull())
    | (F.col("TRGT_VBB_CMPNT_UNIQ_KEY").isNull())
)
df_Research = df_Research.select(
    "TRGT_MBR_UNIQ_KEY",
    "TRGT_VBB_CMPNT_UNIQ_KEY",
    "TRGT_SRC_SYS_CD_SK",
    "SRC_MBR_UNIQ_KEY",
    "SRC_VBB_CMPNT_UNIQ_KEY",
    "SRC_SRC_SYS_CD_SK"
)

if ToleranceCd == 'OUT':
    df_Notify_temp = df_SrcTrgtComp.selectExpr(
        "'ROW COUNT BALANCING VBB - MBR VBB CMPNT ENR OUT OF TOLERANCE' as NOTIFICATION"
    ).limit(1)
    df_Notify_temp = df_Notify_temp.withColumn("NOTIFICATION", F.rpad(F.col("NOTIFICATION"), 70, " "))
    df_Notify = df_Notify_temp
else:
    empty_schema = StructType([StructField("NOTIFICATION", StringType(), True)])
    df_Notify = spark.createDataFrame([], empty_schema)

write_files(
    df_Research,
    f"{adls_path}/balancing/research/IdsMbrVbbCmpntEnrBalResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

df_Notify = df_Notify.select(F.rpad(F.col("NOTIFICATION"), 70, " ").alias("NOTIFICATION"))
write_files(
    df_Notify,
    f"{adls_path}/balancing/notify/MedMgtBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)