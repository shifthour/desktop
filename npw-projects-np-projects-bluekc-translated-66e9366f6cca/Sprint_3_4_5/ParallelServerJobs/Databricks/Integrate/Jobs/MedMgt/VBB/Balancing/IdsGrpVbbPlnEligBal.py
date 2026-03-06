# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2014 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC PROCESSING: Extracts Target and Source Natural Key columns to Research errors
# MAGIC 
# MAGIC Called by : IdsGrpVbbPlnEligBalSeq
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                       Date               Project/Altiris #               Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------     -------------------   -----------------------------------    ---------------------------------------------------------   ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Kalyan Neelam                2014-09-09       9558                              Initial Programming                               IntegrateNewDevl       Bhoomi Dasari              12/09/2014

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


RunID = get_widget_value("RunID","")
ToleranceCd = get_widget_value("ToleranceCd","")
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
ExtrRunCycle = get_widget_value("ExtrRunCycle","")

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"SELECT GRP_VBB_PLN_ELIG.VBB_PLN_UNIQ_KEY, GRP_VBB_PLN_ELIG.GRP_ID, GRP_VBB_PLN_ELIG.CLS_ID, GRP_VBB_PLN_ELIG.CLS_PLN_ID, GRP_VBB_PLN_ELIG.SRC_SYS_CD_SK, B_GRP_VBB_PLN_ELIG.VBB_PLN_UNIQ_KEY, B_GRP_VBB_PLN_ELIG.GRP_ID, B_GRP_VBB_PLN_ELIG.CLS_ID, B_GRP_VBB_PLN_ELIG.CLS_PLN_ID, B_GRP_VBB_PLN_ELIG.SRC_SYS_CD_SK FROM {IDSOwner}.GRP_VBB_PLN_ELIG GRP_VBB_PLN_ELIG FULL OUTER JOIN {IDSOwner}.B_GRP_VBB_PLN_ELIG B_GRP_VBB_PLN_ELIG ON GRP_VBB_PLN_ELIG.VBB_PLN_UNIQ_KEY = B_GRP_VBB_PLN_ELIG.VBB_PLN_UNIQ_KEY AND GRP_VBB_PLN_ELIG.GRP_ID = B_GRP_VBB_PLN_ELIG.GRP_ID AND GRP_VBB_PLN_ELIG.CLS_ID = B_GRP_VBB_PLN_ELIG.CLS_ID AND GRP_VBB_PLN_ELIG.CLS_PLN_ID = B_GRP_VBB_PLN_ELIG.CLS_PLN_ID AND GRP_VBB_PLN_ELIG.SRC_SYS_CD_SK = B_GRP_VBB_PLN_ELIG.SRC_SYS_CD_SK WHERE GRP_VBB_PLN_ELIG.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}"
    )
    .load()
)

cols = df_SrcTrgtComp.columns
df_SrcTrgtCompRenamed = df_SrcTrgtComp
df_SrcTrgtCompRenamed = df_SrcTrgtCompRenamed.withColumnRenamed(cols[0], "SRC_VBB_PLN_UNIQ_KEY")
df_SrcTrgtCompRenamed = df_SrcTrgtCompRenamed.withColumnRenamed(cols[1], "SRC_GRP_ID")
df_SrcTrgtCompRenamed = df_SrcTrgtCompRenamed.withColumnRenamed(cols[2], "SRC_CLS_ID")
df_SrcTrgtCompRenamed = df_SrcTrgtCompRenamed.withColumnRenamed(cols[3], "SRC_CLS_PLN_ID")
df_SrcTrgtCompRenamed = df_SrcTrgtCompRenamed.withColumnRenamed(cols[4], "SRC_SRC_SYS_CD_SK")
df_SrcTrgtCompRenamed = df_SrcTrgtCompRenamed.withColumnRenamed(cols[5], "TRGT_VBB_PLN_UNIQ_KEY")
df_SrcTrgtCompRenamed = df_SrcTrgtCompRenamed.withColumnRenamed(cols[6], "TRGT_GRP_ID")
df_SrcTrgtCompRenamed = df_SrcTrgtCompRenamed.withColumnRenamed(cols[7], "TRGT_CLS_ID")
df_SrcTrgtCompRenamed = df_SrcTrgtCompRenamed.withColumnRenamed(cols[8], "TRGT_CLS_PLN_ID")
df_SrcTrgtCompRenamed = df_SrcTrgtCompRenamed.withColumnRenamed(cols[9], "TRGT_SRC_SYS_CD_SK")

research_condition = (
    (F.col("SRC_CLS_PLN_ID").isNull())
    | (F.col("SRC_CLS_PLN_ID").isNull())
    | (F.col("SRC_GRP_ID").isNull())
    | (F.col("SRC_SRC_SYS_CD_SK").isNull())
    | (F.col("SRC_VBB_PLN_UNIQ_KEY").isNull())
    | (F.col("TRGT_CLS_ID").isNull())
    | (F.col("TRGT_CLS_PLN_ID").isNull())
    | (F.col("TRGT_GRP_ID").isNull())
    | (F.col("TRGT_SRC_SYS_CD_SK").isNull())
    | (F.col("TRGT_VBB_PLN_UNIQ_KEY").isNull())
)

df_Research = df_SrcTrgtCompRenamed.filter(research_condition).select(
    F.col("TRGT_VBB_PLN_UNIQ_KEY"),
    F.col("TRGT_GRP_ID"),
    F.col("TRGT_CLS_ID"),
    F.col("TRGT_CLS_PLN_ID"),
    F.col("TRGT_SRC_SYS_CD_SK"),
    F.col("SRC_VBB_PLN_UNIQ_KEY"),
    F.col("SRC_GRP_ID"),
    F.col("SRC_CLS_ID"),
    F.col("SRC_CLS_PLN_ID"),
    F.col("SRC_SRC_SYS_CD_SK"),
)

w = Window.orderBy(F.lit(1))
df_temp = df_SrcTrgtCompRenamed.withColumn("rownum", F.row_number().over(w))
df_Notify = df_temp.filter((F.col("rownum") == 1) & (ToleranceCd == "OUT")).select(
    F.lit("ROW COUNT BALANCING VBB - GRP_VBB_PLN_ELIG OUT OF TOLERANCE").alias("NOTIFICATION")
)
df_Notify = df_Notify.withColumn("NOTIFICATION", F.rpad(F.col("NOTIFICATION"), 70, " "))

write_files(
    df_Research,
    f"{adls_path}/balancing/research/IdsGrpVbbPlnEligBalResearch.dat.{RunID}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)

write_files(
    df_Notify,
    f"{adls_path}/balancing/notify/MedMgtBalancingNotification.dat",
    ",",
    "append",
    False,
    False,
    "\"",
    None
)