# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 01/28/08 08:28:47 Batch  14638_30532 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 12/26/07 11:16:35 Batch  14605_40601 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/21/07 14:41:36 Batch  14570_52900 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 10/10/07 13:50:33 Batch  14528_49851 PROMOTE bckcetl ids20 dsadm bls for bl
# MAGIC ^1_1 10/10/07 13:38:24 Batch  14528_49120 INIT bckcett testIDS30 dsadm bls for bl
# MAGIC ^1_1 10/08/07 17:37:28 Batch  14526_63461 PROMOTE bckcett testIDS30 u08717 Brent
# MAGIC ^1_1 10/08/07 17:30:51 Batch  14526_63055 INIT bckcett devlIDS30 u08717 Brent
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:      FdbIdsTccBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                          Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              04/17/2007          3264                              Originally Programmed                                      devlIDS30      
# MAGIC 
# MAGIC Parikshith Chada               8/28/2007         3264                              Modified the balancing process,                         devlIDS30                   Steph Goddard             09/27/2007
# MAGIC                                                                                                           by changing snapshot file to snapshot table

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
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


RunID = get_widget_value("RunID","")
ToleranceCd = get_widget_value("ToleranceCd","")
IDSOwner = get_widget_value("IDSOwner","")
ExtrRunCycle = get_widget_value("ExtrRunCycle","")
ids_secret_name = get_widget_value("ids_secret_name","")

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"SELECT TCC.TCC as SRC_TCC, B_TCC.TCC as TRGT_TCC FROM {IDSOwner}.TCC TCC FULL OUTER JOIN {IDSOwner}.B_TCC B_TCC ON TCC.TCC = B_TCC.TCC WHERE TCC.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}"
df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_SrcTrgtComp = df_SrcTrgtComp.withColumn("_rownum", F.row_number().over(Window.orderBy(F.lit(1))))

df_Research = df_SrcTrgtComp.filter(
    (F.col("SRC_TCC").isNull()) | (F.col("TRGT_TCC").isNull())
).select("TRGT_TCC", "SRC_TCC")

if ToleranceCd == "OUT":
    df_NotifyRaw = df_SrcTrgtComp.filter(F.col("_rownum") == 1)
else:
    empty_schema = StructType([
        StructField("SRC_TCC", StringType(), True),
        StructField("TRGT_TCC", StringType(), True),
        StructField("_rownum", IntegerType(), True)
    ])
    df_NotifyRaw = spark.createDataFrame([], schema=empty_schema)

df_Notify = df_NotifyRaw.withColumn(
    "NOTIFICATION",
    F.lit("ROW COUNT BALANCING FIRST DATA BANK - IDS TCC OUT OF TOLERANCE")
).select("NOTIFICATION")
df_Notify = df_Notify.withColumn("NOTIFICATION", F.rpad(F.col("NOTIFICATION"), 70, " "))

df_Research_selected = df_Research.select("TRGT_TCC", "SRC_TCC")
write_files(
    df_Research_selected,
    f"{adls_path}/balancing/research/FdbIdsTccResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_Notify_selected = df_Notify.select("NOTIFICATION")
write_files(
    df_Notify_selected,
    f"{adls_path}/balancing/notify/NdcBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)