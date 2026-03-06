# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 01/28/08 09:39:22 Batch  14638_34767 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 12/27/07 09:46:56 Batch  14606_35219 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/24/07 09:35:50 Batch  14573_34558 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 10/31/07 14:11:20 Batch  14549_51083 PROMOTE bckcetl ids20 dsadm bls for on
# MAGIC ^1_1 10/31/07 12:55:15 Batch  14549_46521 PROMOTE bckcetl ids20 dsadm bls for on
# MAGIC ^1_1 10/31/07 12:32:28 Batch  14549_45152 INIT bckcett testIDS30 dsadm bls for on
# MAGIC ^1_1 10/26/07 08:07:18 Batch  14544_29241 PROMOTE bckcett testIDS30 u03651 steffy
# MAGIC ^1_1 10/26/07 07:46:17 Batch  14544_27982 INIT bckcett devlIDS30 u03651 steffy
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:        FctsIdsBillEntyBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                          Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              06/04/2007          3264                              Originally Programmed                                     devlIDS30         
# MAGIC 
# MAGIC Parikshith Chada               8/22/2007         3264                              Modified the balancing process,                       devlIDS30                     Steph Goddard              09/18/2007
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


IDSOwner = get_widget_value('IDSOwner','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
RunID = get_widget_value('RunID','')
ToleranceCd = get_widget_value('ToleranceCd','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = f"""
SELECT
  BILL_ENTY.SRC_SYS_CD_SK AS SRC_SRC_SYS_CD_SK,
  BILL_ENTY.BILL_ENTY_UNIQ_KEY AS SRC_BILL_ENTY_UNIQ_KEY,
  B_BILL_ENTY.SRC_SYS_CD_SK AS TRGT_SRC_SYS_CD_SK,
  B_BILL_ENTY.BILL_ENTY_UNIQ_KEY AS TRGT_BILL_ENTY_UNIQ_KEY
FROM {IDSOwner}.BILL_ENTY BILL_ENTY
FULL OUTER JOIN {IDSOwner}.B_BILL_ENTY B_BILL_ENTY
  ON BILL_ENTY.SRC_SYS_CD_SK = B_BILL_ENTY.SRC_SYS_CD_SK
  AND BILL_ENTY.BILL_ENTY_UNIQ_KEY = B_BILL_ENTY.BILL_ENTY_UNIQ_KEY
WHERE BILL_ENTY.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
"""

df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_Research = df_SrcTrgtComp.filter(
    F.col("SRC_BILL_ENTY_UNIQ_KEY").isNull()
    | F.col("SRC_SRC_SYS_CD_SK").isNull()
    | F.col("TRGT_BILL_ENTY_UNIQ_KEY").isNull()
    | F.col("TRGT_SRC_SYS_CD_SK").isNull()
).select(
    "TRGT_SRC_SYS_CD_SK",
    "TRGT_BILL_ENTY_UNIQ_KEY",
    "SRC_SRC_SYS_CD_SK",
    "SRC_BILL_ENTY_UNIQ_KEY"
)

if ToleranceCd == "OUT":
    df_Notify = df_SrcTrgtComp.limit(1)
else:
    df_Notify = spark.createDataFrame([], df_SrcTrgtComp.schema)

df_Notify = df_Notify.withColumn(
    "NOTIFICATION",
    F.lit("ROW COUNT BALANCING FACETS - IDS BILL ENTY OUT OF TOLERANCE")
)
df_Notify = df_Notify.withColumn(
    "NOTIFICATION",
    F.rpad(F.col("NOTIFICATION"), 70, " ")
)
df_Notify = df_Notify.select("NOTIFICATION")

write_files(
    df_Research,
    f"{adls_path}/balancing/research/FctsIdsBillEntyResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

write_files(
    df_Notify,
    f"{adls_path}/balancing/notify/CommissionsBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)