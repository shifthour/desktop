# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 10/24/07 09:59:24 Batch  14542_35967 PROMOTE bckcetl ids20 dsadm bls for on
# MAGIC ^1_1 10/24/07 09:54:15 Batch  14542_35658 INIT bckcett testIDS30 dsadm bls for on
# MAGIC ^1_2 10/17/07 13:59:14 Batch  14535_50357 PROMOTE bckcett testIDS30 u03651 steffy
# MAGIC ^1_2 10/17/07 13:54:06 Batch  14535_50077 INIT bckcett devlIDS30 u03651 steffy
# MAGIC ^1_1 10/16/07 09:44:13 Batch  14534_35065 INIT bckcett devlIDS30 u03651 steffy
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:    FctsIdsCapFundBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                           Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                     ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              04/19/2007         3264                              Originally Programmed                                       devlIDS30       
# MAGIC 
# MAGIC Parikshith Chada               8/28/2007         3264                              Modified the balancing process,                        devlIDS30                     Steph Goddard             09/19/2007
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
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


RunID = get_widget_value("RunID","")
ToleranceCd = get_widget_value("ToleranceCd","")
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
ExtrRunCycle = get_widget_value("ExtrRunCycle","")

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"""SELECT
  CAP_FUND.SRC_SYS_CD_SK AS SRC_SRC_SYS_CD_SK,
  CAP_FUND.CAP_FUND_ID AS SRC_CAP_FUND_ID,
  B_CAP_FUND.SRC_SYS_CD_SK AS TRGT_SRC_SYS_CD_SK,
  B_CAP_FUND.CAP_FUND_ID AS TRGT_CAP_FUND_ID
FROM {IDSOwner}.CAP_FUND CAP_FUND
FULL OUTER JOIN {IDSOwner}.B_CAP_FUND B_CAP_FUND
  ON CAP_FUND.SRC_SYS_CD_SK = B_CAP_FUND.SRC_SYS_CD_SK
  AND CAP_FUND.CAP_FUND_ID = B_CAP_FUND.CAP_FUND_ID
WHERE CAP_FUND.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
"""

df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_missing = df_SrcTrgtComp

df_research = df_missing.filter(
    (F.col("SRC_CAP_FUND_ID").isNull())
    | (F.col("SRC_SRC_SYS_CD_SK").isNull())
    | (F.col("TRGT_CAP_FUND_ID").isNull())
    | (F.col("TRGT_SRC_SYS_CD_SK").isNull())
).select(
    "TRGT_SRC_SYS_CD_SK",
    "TRGT_CAP_FUND_ID",
    "SRC_SRC_SYS_CD_SK",
    "SRC_CAP_FUND_ID",
)

write_files(
    df_research,
    f"{adls_path}/balancing/research/FacetsIdsCapFundResearch.dat." + RunID,
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)

w = Window.orderBy(F.lit(1))
df_missing_rn = df_missing.withColumn("inrownum", F.row_number().over(w))
df_notify_temp = df_missing_rn.filter(
    (F.col("inrownum") == 1) & (F.lit(ToleranceCd) == "OUT")
)
df_notify = df_notify_temp.select(
    F.lit("ROW COUNT BALANCING FACETS - IDS CAP FUND OUT OF TOLERANCE").alias("NOTIFICATION")
)
df_notify = df_notify.withColumn("NOTIFICATION", F.rpad(F.col("NOTIFICATION"), 70, " "))

write_files(
    df_notify,
    f"{adls_path}/balancing/notify/CapitationBalancingNotification.dat",
    ",",
    "append",
    False,
    False,
    "\"",
    None
)