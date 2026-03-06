# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2011 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC PROCESSING: Extracts Target and Source Natural Key columns to Research errors
# MAGIC 
# MAGIC Called by : IdsVbbPlnBalSeq
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                       Date               Project/Altiris #               Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------     -------------------   -----------------------------------    ---------------------------------------------------------   ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Raja Gummadi              2013-09-25       4963 - VBB                       Original Programming                           IntegrateNewDevl        Kalyan Neelam            2013-10-28

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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


RunID = get_widget_value('RunID','')
ToleranceCd = get_widget_value('ToleranceCd','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = f"""SELECT
VBB_PLN.VBB_PLN_UNIQ_KEY,
VBB_PLN.SRC_SYS_CD_SK,
B_VBB_PLN.VBB_PLN_UNIQ_KEY,
B_VBB_PLN.SRC_SYS_CD_SK
FROM 
{IDSOwner}.VBB_PLN VBB_PLN FULL OUTER JOIN {IDSOwner}.B_VBB_PLN B_VBB_PLN
ON VBB_PLN.VBB_PLN_UNIQ_KEY = B_VBB_PLN.VBB_PLN_UNIQ_KEY
AND VBB_PLN.SRC_SYS_CD_SK = B_VBB_PLN.SRC_SYS_CD_SK
WHERE 
VBB_PLN.VBB_PLN_SK NOT IN (0,1)
AND VBB_PLN.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}"""

df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# Rename columns to match DataStage link names
df_SrcTrgtComp = (
    df_SrcTrgtComp
    .withColumnRenamed("VBB_PLN_UNIQ_KEY", "SRC_VBB_PLN_UNIQ_KEY")
    .withColumnRenamed("SRC_SYS_CD_SK", "SRC_SRC_SYS_CD_SK")
    .withColumnRenamed("VBB_PLN_UNIQ_KEY_1", "TRGT_VBB_PLN_UNIQ_KEY")
    .withColumnRenamed("SRC_SYS_CD_SK_1", "TRGT_SRC_SYS_CD_SK")
)

df_Research = df_SrcTrgtComp.filter(
    col("SRC_SRC_SYS_CD_SK").isNull()
    | col("SRC_VBB_PLN_UNIQ_KEY").isNull()
    | col("TRGT_SRC_SYS_CD_SK").isNull()
    | col("TRGT_VBB_PLN_UNIQ_KEY").isNull()
).select(
    "TRGT_VBB_PLN_UNIQ_KEY",
    "TRGT_SRC_SYS_CD_SK",
    "SRC_VBB_PLN_UNIQ_KEY",
    "SRC_SRC_SYS_CD_SK"
)

if ToleranceCd == 'OUT':
    df_Notify = df_SrcTrgtComp.limit(1)
else:
    df_Notify = spark.createDataFrame([], df_SrcTrgtComp.schema)

df_Notify = df_Notify.withColumn("NOTIFICATION", lit("ROW COUNT BALANCING VBB - VBB_PLN OUT OF TOLERANCE"))
df_Notify = df_Notify.select(rpad(col("NOTIFICATION"), 70, " ").alias("NOTIFICATION"))

write_files(
    df_Research.select("TRGT_VBB_PLN_UNIQ_KEY", "TRGT_SRC_SYS_CD_SK", "SRC_VBB_PLN_UNIQ_KEY", "SRC_SRC_SYS_CD_SK"),
    f"{adls_path}/balancing/research/IdsVbbPlnBalResearch.dat.{RunID}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)

write_files(
    df_Notify.select("NOTIFICATION"),
    f"{adls_path}/balancing/notify/MedMgtBalancingNotification.dat",
    ",",
    "append",
    False,
    False,
    "\"",
    None
)