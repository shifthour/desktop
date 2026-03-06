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
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Parameter retrieval
RunID = get_widget_value("RunID", "")
ToleranceCd = get_widget_value("ToleranceCd", "")
IDSOwner = get_widget_value("IDSOwner", "")
ids_secret_name = get_widget_value("ids_secret_name", "")
ExtrRunCycle = get_widget_value("ExtrRunCycle", "")

# Database configuration
jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# DB2Connector: SrcTrgtComp
extract_query = f"""
SELECT
VBB_PLN_CMPNT_DPNDC.VBB_PLN_UNIQ_KEY AS SRC_VBB_PLN_UNIQ_KEY,
VBB_PLN_CMPNT_DPNDC.VBB_CMPNT_UNIQ_KEY AS SRC_VBB_CMPNT_UNIQ_KEY,
VBB_PLN_CMPNT_DPNDC.RQRD_VBB_CMPNT_UNIQ_KEY AS SRC_RQRD_VBB_CMPNT_UNIQ_KEY,
VBB_PLN_CMPNT_DPNDC.SRC_SYS_CD_SK AS SRC_SRC_SYS_CD_SK,
B_VBB_PLN_CMPNT_DPNDC.VBB_PLN_UNIQ_KEY AS TRGT_VBB_PLN_UNIQ_KEY,
B_VBB_PLN_CMPNT_DPNDC.VBB_CMPNT_UNIQ_KEY AS TRGT_VBB_CMPNT_UNIQ_KEY,
B_VBB_PLN_CMPNT_DPNDC.RQRD_VBB_CMPNT_UNIQ_KEY AS TRGT_RQRD_VBB_CMPNT_UNIQ_KEY,
B_VBB_PLN_CMPNT_DPNDC.SRC_SYS_CD_SK AS TRGT_SRC_SYS_CD_SK
FROM {IDSOwner}.VBB_PLN_CMPNT_DPNDC VBB_PLN_CMPNT_DPNDC
FULL OUTER JOIN {IDSOwner}.B_VBB_PLN_CMPNT_DPNDC B_VBB_PLN_CMPNT_DPNDC
ON 
VBB_PLN_CMPNT_DPNDC.VBB_PLN_UNIQ_KEY = B_VBB_PLN_CMPNT_DPNDC.VBB_PLN_UNIQ_KEY
AND VBB_PLN_CMPNT_DPNDC.VBB_CMPNT_UNIQ_KEY = B_VBB_PLN_CMPNT_DPNDC.VBB_CMPNT_UNIQ_KEY
AND VBB_PLN_CMPNT_DPNDC.RQRD_VBB_CMPNT_UNIQ_KEY = B_VBB_PLN_CMPNT_DPNDC.RQRD_VBB_CMPNT_UNIQ_KEY
AND VBB_PLN_CMPNT_DPNDC.SRC_SYS_CD_SK = B_VBB_PLN_CMPNT_DPNDC.SRC_SYS_CD_SK
WHERE
VBB_PLN_CMPNT_DPNDC.VBB_PLN_CMPNT_DPNDC_SK NOT IN (0,1)
AND VBB_PLN_CMPNT_DPNDC.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
"""

df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# Transformer: TransformLogic, output link "Research"
df_research = df_SrcTrgtComp.filter(
    (F.col("SRC_SRC_SYS_CD_SK").isNull())
    | (F.col("SRC_VBB_CMPNT_UNIQ_KEY").isNull())
    | (F.col("TRGT_SRC_SYS_CD_SK").isNull())
    | (F.col("TRGT_VBB_CMPNT_UNIQ_KEY").isNull())
).select(
    F.col("TRGT_VBB_CMPNT_UNIQ_KEY"),
    F.col("TRGT_SRC_SYS_CD_SK"),
    F.col("SRC_VBB_CMPNT_UNIQ_KEY"),
    F.col("SRC_SRC_SYS_CD_SK")
)

# Transformer: TransformLogic, output link "Notify"
windowSpec = Window.orderBy(F.lit(1))
df_with_inrownum = df_SrcTrgtComp.withColumn("INROWNUM", F.row_number().over(windowSpec))
df_notify = df_with_inrownum.filter(
    (F.col("INROWNUM") == 1) & (F.lit(ToleranceCd) == F.lit("OUT"))
).select(
    F.lit("ROW COUNT BALANCING VBB - VBB_CMPNT OUT OF TOLERANCE").alias("NOTIFICATION")
)
df_notify = df_notify.withColumn("NOTIFICATION", F.rpad("NOTIFICATION", 70, " "))

# CSeqFileStage: ResearchFile
write_files(
    df_research.select("TRGT_VBB_CMPNT_UNIQ_KEY", "TRGT_SRC_SYS_CD_SK", "SRC_VBB_CMPNT_UNIQ_KEY", "SRC_SRC_SYS_CD_SK"),
    f"{adls_path}/balancing/research/IdsVbbPlnCmpntDpndcBalResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# CSeqFileStage: ErrorNotificationFile
write_files(
    df_notify.select("NOTIFICATION"),
    f"{adls_path}/balancing/notify/MedMgtBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)