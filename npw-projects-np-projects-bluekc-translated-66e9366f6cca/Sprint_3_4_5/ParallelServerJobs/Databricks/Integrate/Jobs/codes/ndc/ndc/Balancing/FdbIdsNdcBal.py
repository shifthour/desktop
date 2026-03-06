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
# MAGIC CALLED BY:     FdbIdsNdcDrugBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                          Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                     ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              04/17/2007          3264                              Originally Programmed                                      devlIDS30  
# MAGIC 
# MAGIC Parikshith Chada               8/28/2007         3264                              Modified the balancing process,                        devlIDS30                     Steph Goddard            09/27/2007
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
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


RunID = get_widget_value("RunID", "")
ToleranceCd = get_widget_value("ToleranceCd", "")
IDSOwner = get_widget_value("IDSOwner", "")
ExtrRunCycle = get_widget_value("ExtrRunCycle", "")
ids_secret_name = get_widget_value("ids_secret_name", "")

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query_SrcTrgtComp = f"""
SELECT
  NDC.NDC AS SRC_NDC,
  B_NDC.NDC AS TRGT_NDC
FROM {IDSOwner}.NDC NDC
FULL OUTER JOIN {IDSOwner}.B_NDC B_NDC
ON NDC.NDC = B_NDC.NDC
WHERE NDC.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
"""

df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_SrcTrgtComp)
    .load()
)

df_research = df_SrcTrgtComp.filter(
    (F.col("SRC_NDC").isNull()) | (F.col("TRGT_NDC").isNull())
).select(
    "TRGT_NDC",
    "SRC_NDC"
)

if ToleranceCd == "OUT":
    df_notify_temp = df_SrcTrgtComp.limit(1)
else:
    df_notify_temp = spark.createDataFrame([], df_SrcTrgtComp.schema)

df_notify = df_notify_temp.select(
    F.lit("ROW COUNT BALANCING FIRST DATA BANK - IDS NDC OUT OF TOLERANCE").alias("NOTIFICATION")
)

df_notify = df_notify.withColumn("NOTIFICATION", F.rpad(F.col("NOTIFICATION"), 70, " "))

write_files(
    df_research,
    f"{adls_path}/balancing/research/FdbIdsNdcResearch.dat.{RunID}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)

write_files(
    df_notify,
    f"{adls_path}/balancing/notify/NdcBalancingNotification.dat",
    ",",
    "append",
    False,
    False,
    "\"",
    None
)