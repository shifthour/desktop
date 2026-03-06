# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2011 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC PROCESSING: Extracts Target and Source Natural Key columns to Research errors
# MAGIC 
# MAGIC Called by : IdsVbbCmpntBalSeq
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                       Date               Project/Altiris #               Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------     -------------------   -----------------------------------    ---------------------------------------------------------   ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Raja Gummadi              2013-08-25       4963 - VBB                       Original Programming                           IntegrateNewDevl        Kalyan Neelam            2013-10-28

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
from pyspark.sql import functions as F
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


RunID = get_widget_value('RunID','')
ToleranceCd = get_widget_value('ToleranceCd','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"""
SELECT
VBB_CMPNT.VBB_CMPNT_UNIQ_KEY AS SRC_VBB_CMPNT_UNIQ_KEY,
VBB_CMPNT.SRC_SYS_CD_SK AS SRC_SRC_SYS_CD_SK,
B_VBB_CMPNT.VBB_CMPNT_UNIQ_KEY AS TRGT_VBB_CMPNT_UNIQ_KEY,
B_VBB_CMPNT.SRC_SYS_CD_SK AS TRGT_SRC_SYS_CD_SK
FROM {IDSOwner}.VBB_CMPNT VBB_CMPNT
FULL OUTER JOIN {IDSOwner}.B_VBB_CMPNT B_VBB_CMPNT
ON VBB_CMPNT.VBB_CMPNT_UNIQ_KEY = B_VBB_CMPNT.VBB_CMPNT_UNIQ_KEY
AND VBB_CMPNT.SRC_SYS_CD_SK = B_VBB_CMPNT.SRC_SYS_CD_SK
WHERE
VBB_CMPNT.VBB_CMPNT_SK NOT IN (0,1)
AND VBB_CMPNT.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
"""
df_SrcTrgtComp = (
    spark.read.format("jdbc")
        .option("url", jdbc_url)
        .options(**jdbc_props)
        .option("query", extract_query)
        .load()
)

df_Missing = df_SrcTrgtComp

df_Research = df_Missing.filter(
    F.col("SRC_SRC_SYS_CD_SK").isNull()
    | F.col("SRC_VBB_CMPNT_UNIQ_KEY").isNull()
    | F.col("TRGT_SRC_SYS_CD_SK").isNull()
    | F.col("TRGT_VBB_CMPNT_UNIQ_KEY").isNull()
).select(
    "TRGT_VBB_CMPNT_UNIQ_KEY",
    "TRGT_SRC_SYS_CD_SK",
    "SRC_VBB_CMPNT_UNIQ_KEY",
    "SRC_SRC_SYS_CD_SK"
)

write_files(
    df_Research,
    f"{adls_path}/balancing/research/IdsVbbCmpntBalResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_Missing_with_rn = df_Missing.withColumn(
    "row_number",
    F.row_number().over(Window.orderBy(F.lit(1)))
)

df_Notify_pre = df_Missing_with_rn.filter(
    (F.col("row_number") == 1) & (F.lit(ToleranceCd) == F.lit("OUT"))
)

df_Notify = df_Notify_pre.select(
    F.lit("ROW COUNT BALANCING VBB - VBB_CMPNT OUT OF TOLERANCE").alias("NOTIFICATION")
)

df_Notify = df_Notify.withColumn(
    "NOTIFICATION",
    F.rpad(F.col("NOTIFICATION"), 70, " ")
)

write_files(
    df_Notify,
    f"{adls_path}/balancing/notify/MedMgtBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)