# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2011 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC PROCESSING: Extracts Target and Source Natural Key columns to Research errors
# MAGIC 
# MAGIC Called by : IdsMbrVbbPlnEnrBalSeq
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


RunID = get_widget_value('RunID','')
ToleranceCd = get_widget_value('ToleranceCd','')
IDSOwner = get_widget_value('IDSOwner','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query_SrcTrgtComp = (
    "SELECT "
    "MBR_VBB_PLN_ENR.MBR_UNIQ_KEY AS SRC_MBR_UNIQ_KEY, "
    "MBR_VBB_PLN_ENR.VBB_PLN_UNIQ_KEY AS SRC_VBB_PLN_UNIQ_KEY, "
    "MBR_VBB_PLN_ENR.SRC_SYS_CD_SK AS SRC_SRC_SYS_CD_SK, "
    "B_MBR_VBB_PLN_ENR.MBR_UNIQ_KEY AS TRGT_MBR_UNIQ_KEY, "
    "B_MBR_VBB_PLN_ENR.VBB_PLN_UNIQ_KEY AS TRGT_VBB_PLN_UNIQ_KEY, "
    "B_MBR_VBB_PLN_ENR.SRC_SYS_CD_SK AS TRGT_SRC_SYS_CD_SK "
    f"FROM {IDSOwner}.MBR_VBB_PLN_ENR MBR_VBB_PLN_ENR "
    f"FULL OUTER JOIN {IDSOwner}.B_MBR_VBB_PLN_ENR B_MBR_VBB_PLN_ENR "
    "ON MBR_VBB_PLN_ENR.MBR_UNIQ_KEY = B_MBR_VBB_PLN_ENR.MBR_UNIQ_KEY "
    "AND MBR_VBB_PLN_ENR.VBB_PLN_UNIQ_KEY = B_MBR_VBB_PLN_ENR.VBB_PLN_UNIQ_KEY "
    "AND MBR_VBB_PLN_ENR.SRC_SYS_CD_SK = B_MBR_VBB_PLN_ENR.SRC_SYS_CD_SK "
    f"WHERE MBR_VBB_PLN_ENR.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}"
)

df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_SrcTrgtComp)
    .load()
)

df_ResearchFile = df_SrcTrgtComp.filter(
    F.col("SRC_SRC_SYS_CD_SK").isNull()
    | F.col("SRC_VBB_PLN_UNIQ_KEY").isNull()
    | F.col("SRC_MBR_UNIQ_KEY").isNull()
    | F.col("TRGT_VBB_PLN_UNIQ_KEY").isNull()
    | F.col("TRGT_MBR_UNIQ_KEY").isNull()
    | F.col("TRGT_SRC_SYS_CD_SK").isNull()
).select(
    "TRGT_MBR_UNIQ_KEY",
    "TRGT_VBB_PLN_UNIQ_KEY",
    "TRGT_SRC_SYS_CD_SK",
    "SRC_MBR_UNIQ_KEY",
    "SRC_VBB_PLN_UNIQ_KEY",
    "SRC_SRC_SYS_CD_SK"
)

df_SrcTrgtComp_with_rn = df_SrcTrgtComp.withColumn(
    "rownum",
    F.row_number().over(Window.orderBy(F.monotonically_increasing_id()))
)
df_Notify_temp = df_SrcTrgtComp_with_rn.withColumn(
    "NOTIFICATION",
    F.lit("ROW COUNT BALANCING VBB - MBR_VBB_PLN_ENR OUT OF TOLERANCE")
).filter(
    (F.col("rownum") == 1) & (F.lit(ToleranceCd) == F.lit("OUT"))
)

df_Notify = df_Notify_temp.select(
    rpad("NOTIFICATION", 70, " ").alias("NOTIFICATION")
)

write_files(
    df_ResearchFile,
    f"{adls_path}/balancing/research/IdsMbrVbbPlnEnrBalResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

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