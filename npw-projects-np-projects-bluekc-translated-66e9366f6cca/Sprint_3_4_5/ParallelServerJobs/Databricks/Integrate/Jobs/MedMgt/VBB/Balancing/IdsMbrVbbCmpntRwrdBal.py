# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2011 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC PROCESSING: Extracts Target and Source Natural Key columns to Research errors
# MAGIC 
# MAGIC Called by : IdsMbrVbbCmpntRwrdBalSeq
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                       Date               Project/Altiris #               Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------     -------------------   -----------------------------------    ---------------------------------------------------------   ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Raja Gummadi              2013-08-25       4963 - VBB                       Original Programming                           IntegrateNewDevl        Kalyan Neelam             2013-10-28

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
from pyspark.sql import Window
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
MBR_VBB_CMPNT_RWRD.MBR_UNIQ_KEY as SRC_MBR_UNIQ_KEY,
MBR_VBB_CMPNT_RWRD.VBB_CMPNT_UNIQ_KEY as SRC_VBB_CMPNT_UNIQ_KEY,
MBR_VBB_CMPNT_RWRD.VBB_CMPNT_RWRD_SEQ_NO as SRC_VBB_CMPNT_RWRD_SEQ_NO,
MBR_VBB_CMPNT_RWRD.SRC_SYS_CD_SK as SRC_SRC_SYS_CD_SK,
B_MBR_VBB_CMPNT_RWRD.MBR_UNIQ_KEY as TRGT_MBR_UNIQ_KEY,
B_MBR_VBB_CMPNT_RWRD.VBB_CMPNT_UNIQ_KEY as TRGT_VBB_CMPNT_UNIQ_KEY,
B_MBR_VBB_CMPNT_RWRD.VBB_CMPNT_RWRD_SEQ_NO as TRGT_VBB_CMPNT_RWRD_SEQ_NO,
B_MBR_VBB_CMPNT_RWRD.SRC_SYS_CD_SK as TRGT_SRC_SYS_CD_SK
FROM {IDSOwner}.MBR_VBB_CMPNT_RWRD MBR_VBB_CMPNT_RWRD
FULL OUTER JOIN {IDSOwner}.B_MBR_VBB_CMPNT_RWRD B_MBR_VBB_CMPNT_RWRD
ON MBR_VBB_CMPNT_RWRD.MBR_UNIQ_KEY = B_MBR_VBB_CMPNT_RWRD.MBR_UNIQ_KEY
AND MBR_VBB_CMPNT_RWRD.VBB_CMPNT_UNIQ_KEY = B_MBR_VBB_CMPNT_RWRD.VBB_CMPNT_UNIQ_KEY
AND MBR_VBB_CMPNT_RWRD.VBB_CMPNT_RWRD_SEQ_NO = B_MBR_VBB_CMPNT_RWRD.VBB_CMPNT_RWRD_SEQ_NO
AND MBR_VBB_CMPNT_RWRD.SRC_SYS_CD_SK = B_MBR_VBB_CMPNT_RWRD.SRC_SYS_CD_SK
WHERE MBR_VBB_CMPNT_RWRD.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
"""

df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_Research = df_SrcTrgtComp.filter(
    (F.col("SRC_SRC_SYS_CD_SK").isNull()) |
    (F.col("SRC_MBR_UNIQ_KEY").isNull()) |
    (F.col("SRC_VBB_CMPNT_RWRD_SEQ_NO").isNull()) |
    (F.col("SRC_VBB_CMPNT_UNIQ_KEY").isNull()) |
    (F.col("SRC_SRC_SYS_CD_SK").isNull()) |
    (F.col("TRGT_MBR_UNIQ_KEY").isNull()) |
    (F.col("TRGT_VBB_CMPNT_RWRD_SEQ_NO").isNull()) |
    (F.col("TRGT_VBB_CMPNT_UNIQ_KEY").isNull())
).select(
    "TRGT_MBR_UNIQ_KEY",
    "TRGT_VBB_CMPNT_UNIQ_KEY",
    "TRGT_VBB_CMPNT_RWRD_SEQ_NO",
    "TRGT_SRC_SYS_CD_SK",
    "SRC_MBR_UNIQ_KEY",
    "SRC_VBB_CMPNT_UNIQ_KEY",
    "SRC_VBB_CMPNT_RWRD_SEQ_NO",
    "SRC_SRC_SYS_CD_SK"
)

window = Window.orderBy(F.lit(1))
df_with_rownum = df_SrcTrgtComp.withColumn("row_num", F.row_number().over(window))
df_Notify = df_with_rownum.filter(
    (F.col("row_num") == 1) & (F.lit(ToleranceCd) == 'OUT')
).select(
    F.expr('"ROW COUNT BALANCING VBB - MBR_VBB_CMPNT_RWRD OUT OF TOLERANCE" as NOTIFICATION')
)

df_Notify = df_Notify.withColumn(
    "NOTIFICATION", rpad("NOTIFICATION", 70, " ")
).select("NOTIFICATION")

write_files(
    df_Research,
    f"{adls_path}/balancing/research/IdsMbrVbbCmpntRwrdBalResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"'
)

write_files(
    df_Notify,
    f"{adls_path}/balancing/notify/MedMgtBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote='"'
)