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
# MAGIC CALLED BY:        FctsIdsAltFundPlnBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                       Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                  ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              10/05/2007          3259                              Originally Programmed                                  devlIDS30                      Steph Goddard            10/19/2007

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


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
RunID = get_widget_value('RunID','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = f"""SELECT 
BILL_RDUCTN_HIST.SRC_SYS_CD_SK,
BILL_RDUCTN_HIST.MBR_UNIQ_KEY,
BILL_RDUCTN_HIST.BILL_ENTY_UNIQ_KEY,
BILL_RDUCTN_HIST.ALT_FUND_CNTR_PERD_NO,
BILL_RDUCTN_HIST.FUND_FROM_DT_SK,
BILL_RDUCTN_HIST.SEQ_NO,
B_BILL_RDUCTN_HIST.SRC_SYS_CD_SK,
B_BILL_RDUCTN_HIST.MBR_UNIQ_KEY,
B_BILL_RDUCTN_HIST.BILL_ENTY_UNIQ_KEY,
B_BILL_RDUCTN_HIST.ALT_FUND_CNTR_PERD_NO,
B_BILL_RDUCTN_HIST.FUND_FROM_DT_SK,
B_BILL_RDUCTN_HIST.SEQ_NO
FROM {IDSOwner}.BILL_RDUCTN_HIST BILL_RDUCTN_HIST 
FULL OUTER JOIN {IDSOwner}.B_BILL_RDUCTN_HIST B_BILL_RDUCTN_HIST
ON BILL_RDUCTN_HIST.SRC_SYS_CD_SK = B_BILL_RDUCTN_HIST.SRC_SYS_CD_SK
AND BILL_RDUCTN_HIST.MBR_UNIQ_KEY = B_BILL_RDUCTN_HIST.MBR_UNIQ_KEY 
AND BILL_RDUCTN_HIST.BILL_ENTY_UNIQ_KEY = B_BILL_RDUCTN_HIST.BILL_ENTY_UNIQ_KEY 
AND BILL_RDUCTN_HIST.ALT_FUND_CNTR_PERD_NO = B_BILL_RDUCTN_HIST.ALT_FUND_CNTR_PERD_NO 
AND BILL_RDUCTN_HIST.FUND_FROM_DT_SK = B_BILL_RDUCTN_HIST.FUND_FROM_DT_SK 
AND BILL_RDUCTN_HIST.SEQ_NO = B_BILL_RDUCTN_HIST.SEQ_NO
WHERE BILL_RDUCTN_HIST.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
"""

df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_SrcTrgtComp_renamed = (
    df_SrcTrgtComp
    .withColumnRenamed("SRC_SYS_CD_SK", "SRC_SRC_SYS_CD_SK")
    .withColumnRenamed("MBR_UNIQ_KEY", "SRC_MBR_UNIQ_KEY")
    .withColumnRenamed("BILL_ENTY_UNIQ_KEY", "SRC_BILL_ENTY_UNIQ_KEY")
    .withColumnRenamed("ALT_FUND_CNTR_PERD_NO", "SRC_ALT_FUND_CNTR_PERD_NO")
    .withColumnRenamed("FUND_FROM_DT_SK", "SRC_FUND_FROM_DT_SK")
    .withColumnRenamed("SEQ_NO", "SRC_SEQ_NO")
    .withColumnRenamed("SRC_SYS_CD_SK_1", "TRGT_SRC_SYS_CD_SK")
    .withColumnRenamed("MBR_UNIQ_KEY_1", "TRGT_MBR_UNIQ_KEY")
    .withColumnRenamed("BILL_ENTY_UNIQ_KEY_1", "TRGT_BILL_ENTY_UNIQ_KEY")
    .withColumnRenamed("ALT_FUND_CNTR_PERD_NO_1", "TRGT_ALT_FUND_CNTR_PERD_NO")
    .withColumnRenamed("FUND_FROM_DT_SK_1", "TRGT_FUND_FROM_DT_SK")
    .withColumnRenamed("SEQ_NO_1", "TRGT_SEQ_NO")
)

df_Research = df_SrcTrgtComp_renamed.filter(
    (F.col("TRGT_MBR_UNIQ_KEY").isNull())
    | (F.col("TRGT_SEQ_NO").isNull())
    | (F.col("TRGT_FUND_FROM_DT_SK").isNull())
    | (F.col("TRGT_BILL_ENTY_UNIQ_KEY").isNull())
    | (F.col("TRGT_ALT_FUND_CNTR_PERD_NO").isNull())
    | (F.col("SRC_MBR_UNIQ_KEY").isNull())
    | (F.col("SRC_SEQ_NO").isNull())
    | (F.col("SRC_FUND_FROM_DT_SK").isNull())
    | (F.col("SRC_BILL_ENTY_UNIQ_KEY").isNull())
    | (F.col("SRC_ALT_FUND_CNTR_PERD_NO").isNull())
)

df_Research_final = (
    df_Research
    .withColumn("SRC_FUND_FROM_DT_SK", F.rpad(F.col("SRC_FUND_FROM_DT_SK"), 10, " "))
    .withColumn("TRGT_FUND_FROM_DT_SK", F.rpad(F.col("TRGT_FUND_FROM_DT_SK"), 10, " "))
    .select(
        "SRC_SRC_SYS_CD_SK",
        "SRC_MBR_UNIQ_KEY",
        "SRC_BILL_ENTY_UNIQ_KEY",
        "SRC_ALT_FUND_CNTR_PERD_NO",
        "SRC_FUND_FROM_DT_SK",
        "SRC_SEQ_NO",
        "TRGT_SRC_SYS_CD_SK",
        "TRGT_MBR_UNIQ_KEY",
        "TRGT_BILL_ENTY_UNIQ_KEY",
        "TRGT_ALT_FUND_CNTR_PERD_NO",
        "TRGT_FUND_FROM_DT_SK",
        "TRGT_SEQ_NO"
    )
)

write_files(
    df_Research_final,
    f"{adls_path}/balancing/research/FctsIdsBillRdctnHistResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_Notify = df_SrcTrgtComp_renamed.limit(1).select(
    F.lit("ROW COUNT BALANCING FACETS - IDS BILL RDCTN HIST OUT OF TOLERANCE").alias("NOTIFICATION")
)

df_Notify_final = (
    df_Notify
    .withColumn("NOTIFICATION", F.rpad(F.col("NOTIFICATION"), 70, " "))
    .select("NOTIFICATION")
)

write_files(
    df_Notify_final,
    f"{adls_path}/balancing/notify/FinanceBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)