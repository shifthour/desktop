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
# MAGIC CALLED BY:        FctsIdsAltFundInvcPaymtBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                       Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                  ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              10/05/2007          3259                            Originally Programmed                                  devlIDS30                       Steph Goddard             10/19/2007

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
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
ExtrRunCycle = get_widget_value("ExtrRunCycle","")
RunID = get_widget_value("RunID","")

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = f"""SELECT 
ALT_FUND_INVC_PAYMT.SRC_SYS_CD_SK  SRC_SRC_SYS_CD_SK,
ALT_FUND_INVC_PAYMT.ALT_FUND_INVC_ID  SRC_ALT_FUND_INVC_ID,
ALT_FUND_INVC_PAYMT.MBR_UNIQ_KEY  SRC_MBR_UNIQ_KEY,
ALT_FUND_INVC_PAYMT.ALT_FUND_CNTR_PERD_NO  SRC_ALT_FUND_CNTR_PERD_NO,
ALT_FUND_INVC_PAYMT.CLS_PLN_ID  SRC_CLS_PLN_ID,
ALT_FUND_INVC_PAYMT.PROD_ID  SRC_PROD_ID,
ALT_FUND_INVC_PAYMT.CLM_ID  SRC_CLM_ID,
ALT_FUND_INVC_PAYMT.SUB_UNIQ_KEY  SRC_SUB_UNIQ_KEY,
ALT_FUND_INVC_PAYMT.SEQ_NO  SRC_SEQ_NO,
B_ALT_FUND_INVC_PAYMT.SRC_SYS_CD_SK  TRGT_SRC_SYS_CD_SK,
B_ALT_FUND_INVC_PAYMT.ALT_FUND_INVC_ID  TRGT_ALT_FUND_INVC_ID,
B_ALT_FUND_INVC_PAYMT.MBR_UNIQ_KEY  TRGT_MBR_UNIQ_KEY,
B_ALT_FUND_INVC_PAYMT.ALT_FUND_CNTR_PERD_NO  TRGT_ALT_FUND_CNTR_PERD_NO,
B_ALT_FUND_INVC_PAYMT.CLS_PLN_ID  TRGT_CLS_PLN_ID,
B_ALT_FUND_INVC_PAYMT.PROD_ID  TRGT_PROD_ID,
B_ALT_FUND_INVC_PAYMT.CLM_ID  TRGT_CLM_ID,
B_ALT_FUND_INVC_PAYMT.SUB_UNIQ_KEY  TRGT_SUB_UNIQ_KEY,
B_ALT_FUND_INVC_PAYMT.SEQ_NO  TRGT_SEQ_NO

FROM {IDSOwner}.ALT_FUND_INVC_PAYMT ALT_FUND_INVC_PAYMT
FULL OUTER JOIN {IDSOwner}.B_ALT_FUND_INVC_PAYMT B_ALT_FUND_INVC_PAYMT
ON ALT_FUND_INVC_PAYMT.SRC_SYS_CD_SK = B_ALT_FUND_INVC_PAYMT.SRC_SYS_CD_SK
AND ALT_FUND_INVC_PAYMT.ALT_FUND_INVC_ID = B_ALT_FUND_INVC_PAYMT.ALT_FUND_INVC_ID
AND ALT_FUND_INVC_PAYMT.MBR_UNIQ_KEY = B_ALT_FUND_INVC_PAYMT.MBR_UNIQ_KEY
AND ALT_FUND_INVC_PAYMT.ALT_FUND_CNTR_PERD_NO = B_ALT_FUND_INVC_PAYMT.ALT_FUND_CNTR_PERD_NO
AND ALT_FUND_INVC_PAYMT.CLS_PLN_ID = B_ALT_FUND_INVC_PAYMT.CLS_PLN_ID
AND ALT_FUND_INVC_PAYMT.PROD_ID = B_ALT_FUND_INVC_PAYMT.PROD_ID
AND ALT_FUND_INVC_PAYMT.CLM_ID = B_ALT_FUND_INVC_PAYMT.CLM_ID
AND ALT_FUND_INVC_PAYMT.SUB_UNIQ_KEY = B_ALT_FUND_INVC_PAYMT.SUB_UNIQ_KEY
AND ALT_FUND_INVC_PAYMT.SEQ_NO = B_ALT_FUND_INVC_PAYMT.SEQ_NO

WHERE ALT_FUND_INVC_PAYMT.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
"""

df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_transformlogic = df_SrcTrgtComp

df_transformlogic_research = df_transformlogic.filter(
    F.col("TRGT_ALT_FUND_INVC_ID").isNull() |
    F.col("TRGT_SEQ_NO").isNull() |
    F.col("TRGT_SUB_UNIQ_KEY").isNull() |
    F.col("TRGT_ALT_FUND_CNTR_PERD_NO").isNull() |
    F.col("TRGT_MBR_UNIQ_KEY").isNull() |
    F.col("TRGT_CLM_ID").isNull() |
    F.col("TRGT_PROD_ID").isNull() |
    F.col("TRGT_CLS_PLN_ID").isNull() |
    F.col("SRC_ALT_FUND_INVC_ID").isNull() |
    F.col("SRC_SEQ_NO").isNull() |
    F.col("SRC_SUB_UNIQ_KEY").isNull() |
    F.col("SRC_ALT_FUND_CNTR_PERD_NO").isNull() |
    F.col("SRC_MBR_UNIQ_KEY").isNull() |
    F.col("SRC_CLM_ID").isNull() |
    F.col("SRC_PROD_ID").isNull() |
    F.col("SRC_CLS_PLN_ID").isNull()
).select(
    "SRC_SRC_SYS_CD_SK",
    "SRC_ALT_FUND_INVC_ID",
    "SRC_MBR_UNIQ_KEY",
    "SRC_ALT_FUND_CNTR_PERD_NO",
    "SRC_CLS_PLN_ID",
    "SRC_PROD_ID",
    "SRC_CLM_ID",
    "SRC_SUB_UNIQ_KEY",
    "SRC_SEQ_NO",
    "TRGT_SRC_SYS_CD_SK",
    "TRGT_ALT_FUND_INVC_ID",
    "TRGT_MBR_UNIQ_KEY",
    "TRGT_ALT_FUND_CNTR_PERD_NO",
    "TRGT_CLS_PLN_ID",
    "TRGT_PROD_ID",
    "TRGT_CLM_ID",
    "TRGT_SUB_UNIQ_KEY",
    "TRGT_SEQ_NO"
)

df_transformlogic_Notify = df_transformlogic.select(
    F.lit("ROW COUNT BALANCING FACETS - IDS ALT FUND INVC PAYMT OUT OF TOLERANCE").alias("NOTIFICATION")
).limit(1)

df_transformlogic_Notify = df_transformlogic_Notify.withColumn("NOTIFICATION", F.rpad(F.col("NOTIFICATION"), 70, " "))

write_files(
    df_transformlogic_research,
    f"{adls_path}/balancing/research/FctsIdsAltFundInvcPaymtResearch.dat.{RunID}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)

write_files(
    df_transformlogic_Notify,
    f"{adls_path}/balancing/notify/FinanceBalancingNotification.dat",
    ",",
    "append",
    False,
    False,
    "\"",
    None
)