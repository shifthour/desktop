# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 01/30/08 05:00:10 Batch  14640_18019 INIT bckcetl ids20 dsadm dsadm
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
# MAGIC CALLED BY:      FctsIdsComsnErnBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                        Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                   ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              2007-05-30          3264                              Originally Programmed                                     devlIDS30  
# MAGIC 
# MAGIC Parikshith Chada               8/22/2007         3264                              Modified the balancing process,                      devlIDS30                     Steph Goddard            09/17/2007
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
import pyspark.sql.functions as F
from pyspark.sql.functions import col, lit, row_number, rpad
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType
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
  COMSN_ERN.SRC_SYS_CD_SK AS SRC_SRC_SYS_CD_SK,
  COMSN_ERN.COMSN_BILL_REL_UNIQ_KEY AS SRC_COMSN_BILL_REL_UNIQ_KEY,
  COMSN_ERN.COMSN_CALC_LOB_CD_SK AS SRC_COMSN_CALC_LOB_CD_SK,
  COMSN_ERN.COMSN_SCHD_TIER_UNIQ_KEY AS SRC_COMSN_SCHD_TIER_UNIQ_KEY,
  COMSN_ERN.SEQ_NO AS SRC_SEQ_NO,
  B_COMSN_ERN.SRC_SYS_CD_SK AS TRGT_SRC_SYS_CD_SK,
  B_COMSN_ERN.COMSN_BILL_REL_UNIQ_KEY AS TRGT_COMSN_BILL_REL_UNIQ_KEY,
  B_COMSN_ERN.COMSN_CALC_LOB_CD_SK AS TRGT_COMSN_CALC_LOB_CD_SK,
  B_COMSN_ERN.COMSN_SCHD_TIER_UNIQ_KEY AS TRGT_COMSN_SCHD_TIER_UNIQ_KEY,
  B_COMSN_ERN.SEQ_NO AS TRGT_SEQ_NO
FROM {IDSOwner}.COMSN_ERN COMSN_ERN
FULL OUTER JOIN {IDSOwner}.B_COMSN_ERN B_COMSN_ERN
  ON COMSN_ERN.SRC_SYS_CD_SK = B_COMSN_ERN.SRC_SYS_CD_SK
  AND COMSN_ERN.COMSN_BILL_REL_UNIQ_KEY = B_COMSN_ERN.COMSN_BILL_REL_UNIQ_KEY
  AND COMSN_ERN.COMSN_CALC_LOB_CD_SK = B_COMSN_ERN.COMSN_CALC_LOB_CD_SK
  AND COMSN_ERN.COMSN_SCHD_TIER_UNIQ_KEY = B_COMSN_ERN.COMSN_SCHD_TIER_UNIQ_KEY
  AND COMSN_ERN.SEQ_NO = B_COMSN_ERN.SEQ_NO
WHERE
  COMSN_ERN.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
"""

df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

windowSpec = Window.orderBy(lit(1))
df_SrcTrgtComp = df_SrcTrgtComp.withColumn("__rownum", row_number().over(windowSpec))

df_research = df_SrcTrgtComp.filter(
    (col("SRC_COMSN_BILL_REL_UNIQ_KEY").isNull())
    | (col("SRC_SRC_SYS_CD_SK").isNull())
    | (col("SRC_SEQ_NO").isNull())
    | (col("SRC_COMSN_CALC_LOB_CD_SK").isNull())
    | (col("SRC_COMSN_SCHD_TIER_UNIQ_KEY").isNull())
    | (col("TRGT_COMSN_BILL_REL_UNIQ_KEY").isNull())
    | (col("TRGT_SRC_SYS_CD_SK").isNull())
    | (col("TRGT_SEQ_NO").isNull())
    | (col("TRGT_COMSN_CALC_LOB_CD_SK").isNull())
    | (col("TRGT_COMSN_SCHD_TIER_UNIQ_KEY").isNull())
).select(
    "TRGT_SRC_SYS_CD_SK",
    "TRGT_COMSN_BILL_REL_UNIQ_KEY",
    "TRGT_COMSN_CALC_LOB_CD_SK",
    "TRGT_COMSN_SCHD_TIER_UNIQ_KEY",
    "TRGT_SEQ_NO",
    "SRC_SRC_SYS_CD_SK",
    "SRC_COMSN_BILL_REL_UNIQ_KEY",
    "SRC_COMSN_CALC_LOB_CD_SK",
    "SRC_COMSN_SCHD_TIER_UNIQ_KEY",
    "SRC_SEQ_NO"
)

df_notify = (
    df_SrcTrgtComp.filter(
        (col("__rownum") == 1)
        & (lit(ToleranceCd) == lit("OUT"))
    )
    .select(
        F.lit("ROW COUNT BALANCING FACETS - IDS COMSN ERN OUT OF TOLERANCE").alias("NOTIFICATION")
    )
)
df_notify = df_notify.withColumn("NOTIFICATION", rpad(col("NOTIFICATION"), 70, " "))

researchFilePath = f"{adls_path}/balancing/research/FctsIdsComsnErnResearch.dat.{RunID}"
write_files(
    df_research,
    researchFilePath,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

notificationFilePath = f"{adls_path}/balancing/notify/CommissionsBalancingNotification.dat"
write_files(
    df_notify,
    notificationFilePath,
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)