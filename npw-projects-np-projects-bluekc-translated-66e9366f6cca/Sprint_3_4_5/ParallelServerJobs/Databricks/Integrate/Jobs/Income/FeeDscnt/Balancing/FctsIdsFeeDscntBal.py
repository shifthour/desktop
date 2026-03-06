# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 01/30/08 05:00:10 Batch  14640_18019 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_2 11/12/07 10:00:11 Batch  14561_36015 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_1 11/02/07 13:07:35 Batch  14551_47275 PROMOTE bckcetl ids20 dsadm bls for on
# MAGIC ^1_1 11/02/07 12:52:45 Batch  14551_46368 INIT bckcett testIDS30 dsadm bls for on
# MAGIC ^1_1 10/31/07 13:17:35 Batch  14549_47857 PROMOTE bckcett testIDS30 u03651 steffy
# MAGIC ^1_1 10/31/07 13:09:37 Batch  14549_47387 INIT bckcett devlIDS30 u03651 steffy
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:          FctsIdsFeeDscntBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                        Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                   ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               06/13/2007          3264                              Originally Programmed                                  devlIDS30      
# MAGIC 
# MAGIC Parikshith Chada               8/23/2007         3264                              Modified the balancing process,                      devlIDS30                     Steph Goddard            09/15/2007
# MAGIC                                                                                                           by changing snapshot file to snapshot table

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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


RunID = get_widget_value('RunID','')
ToleranceCd = get_widget_value('ToleranceCd','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"SELECT FEE_DSCNT.SRC_SYS_CD_SK, FEE_DSCNT.FEE_DSCNT_ID, B_FEE_DSCNT.SRC_SYS_CD_SK AS SRC_SYS_CD_SK_1, B_FEE_DSCNT.FEE_DSCNT_ID AS FEE_DSCNT_ID_1 FROM {IDSOwner}.FEE_DSCNT FEE_DSCNT FULL OUTER JOIN {IDSOwner}.B_FEE_DSCNT B_FEE_DSCNT ON FEE_DSCNT.SRC_SYS_CD_SK = B_FEE_DSCNT.SRC_SYS_CD_SK AND FEE_DSCNT.FEE_DSCNT_ID = B_FEE_DSCNT.FEE_DSCNT_ID WHERE FEE_DSCNT.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}"
df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_SrcTrgtComp = df_SrcTrgtComp.withColumnRenamed("SRC_SYS_CD_SK", "SRC_SRC_SYS_CD_SK") \
    .withColumnRenamed("FEE_DSCNT_ID", "SRC_FEE_DSCNT_ID") \
    .withColumnRenamed("SRC_SYS_CD_SK_1", "TRGT_SRC_SYS_CD_SK") \
    .withColumnRenamed("FEE_DSCNT_ID_1", "TRGT_FEE_DSCNT_ID")

df_research = df_SrcTrgtComp.filter(
    (F.col("SRC_FEE_DSCNT_ID").isNull()) |
    (F.col("SRC_SRC_SYS_CD_SK").isNull()) |
    (F.col("TRGT_FEE_DSCNT_ID").isNull()) |
    (F.col("TRGT_SRC_SYS_CD_SK").isNull())
).select(
    "TRGT_SRC_SYS_CD_SK",
    "TRGT_FEE_DSCNT_ID",
    "SRC_SRC_SYS_CD_SK",
    "SRC_FEE_DSCNT_ID"
)

df_notify_temp = df_SrcTrgtComp.limit(1).select(F.lit(1).alias("dummy"))
df_notify_temp = df_notify_temp.withColumn("TOLERANCECD", F.lit(ToleranceCd))
df_notify_temp = df_notify_temp.filter(F.col("TOLERANCECD") == "OUT")
df_notify = df_notify_temp.select(F.lit("ROW COUNT BALANCING FACETS - IDS FEE DSCNT OUT OF TOLERANCE").alias("NOTIFICATION"))
df_notify = df_notify.select(F.rpad(F.col("NOTIFICATION"), 70, " ").alias("NOTIFICATION"))

write_files(
    df_research,
    f"{adls_path}/balancing/research/FctsIdsFeeDscntResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

write_files(
    df_notify,
    f"{adls_path}/balancing/notify/IncomeBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)