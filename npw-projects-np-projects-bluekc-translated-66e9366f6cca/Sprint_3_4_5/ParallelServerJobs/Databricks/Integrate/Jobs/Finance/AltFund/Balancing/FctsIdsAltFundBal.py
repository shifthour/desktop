# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 01/28/08 09:39:22 Batch  14638_34767 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 12/27/07 09:46:56 Batch  14606_35219 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/24/07 09:35:50 Batch  14573_34558 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/07/07 14:46:06 Batch  14556_53195 PROMOTE bckcetl ids20 dsadm bls for rt
# MAGIC ^1_1 11/07/07 14:33:10 Batch  14556_52396 INIT bckcett testIDScur dsadm bls for rt
# MAGIC ^1_4 11/06/07 08:57:18 Batch  14555_32244 PROMOTE bckcett testIDScur u06640 Ralph
# MAGIC ^1_4 11/06/07 08:48:19 Batch  14555_31709 INIT bckcett devlIDS30 u06640 Ralph
# MAGIC ^1_3 11/05/07 14:34:44 Batch  14554_52490 INIT bckcett devlIDS30 u06640 Ralph
# MAGIC ^1_2 10/19/07 11:03:12 Batch  14537_39798 INIT bckcett devlIDS30 u03651 steffy 
# MAGIC ^1_1 10/17/07 13:17:55 Batch  14535_47880 INIT bckcett devlIDS30 u06640 Ralph
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:        FctsIdsAltFundBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                       Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                  ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              10/04/2007          3259                              Originally Programmed                                  devlIDS30                     Steph Goddard            10/19/2007

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
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
RunID = get_widget_value('RunID','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = f"""
SELECT 
  ALT_FUND.SRC_SYS_CD_SK AS SRC_SRC_SYS_CD_SK,
  ALT_FUND.ALT_FUND_UNIQ_KEY AS SRC_ALT_FUND_UNIQ_KEY,
  B_ALT_FUND.SRC_SYS_CD_SK AS TRGT_SRC_SYS_CD_SK,
  B_ALT_FUND.ALT_FUND_UNIQ_KEY AS TRGT_ALT_FUND_UNIQ_KEY
FROM {IDSOwner}.ALT_FUND ALT_FUND
FULL OUTER JOIN {IDSOwner}.B_ALT_FUND B_ALT_FUND
  ON ALT_FUND.SRC_SYS_CD_SK = B_ALT_FUND.SRC_SYS_CD_SK
  AND ALT_FUND.ALT_FUND_UNIQ_KEY = B_ALT_FUND.ALT_FUND_UNIQ_KEY
WHERE ALT_FUND.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
"""

df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_Research = (
    df_SrcTrgtComp
    .filter((col("TRGT_ALT_FUND_UNIQ_KEY").isNull()) | (col("SRC_ALT_FUND_UNIQ_KEY").isNull()))
    .select(
        "TRGT_SRC_SYS_CD_SK",
        "TRGT_ALT_FUND_UNIQ_KEY",
        "SRC_SRC_SYS_CD_SK",
        "SRC_ALT_FUND_UNIQ_KEY"
    )
)

df_Notify = (
    df_SrcTrgtComp
    .limit(1)
    .select(
        lit("ROW COUNT BALANCING FACETS - IDS ALT FUND OUT OF TOLERANCE").alias("NOTIFICATION")
    )
)

write_files(
    df_Research.select(
        "TRGT_SRC_SYS_CD_SK",
        "TRGT_ALT_FUND_UNIQ_KEY",
        "SRC_SRC_SYS_CD_SK",
        "SRC_ALT_FUND_UNIQ_KEY"
    ),
    f"{adls_path}/balancing/research/FctsIdsAltFundResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_Notify_write = df_Notify.select(
    rpad("NOTIFICATION", 70, " ").alias("NOTIFICATION")
)

write_files(
    df_Notify_write.select("NOTIFICATION"),
    f"{adls_path}/balancing/notify/FinanceBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)