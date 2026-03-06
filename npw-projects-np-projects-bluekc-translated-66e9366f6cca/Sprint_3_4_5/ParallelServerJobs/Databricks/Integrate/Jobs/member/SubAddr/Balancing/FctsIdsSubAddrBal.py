# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 01/28/08 13:39:53 Batch  14638_49197 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 01/10/08 10:01:22 Batch  14620_36086 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 12/26/07 15:33:58 Batch  14605_56043 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/23/07 15:42:37 Batch  14572_56566 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 09/25/07 16:07:19 Batch  14513_58043 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 09/20/07 14:24:47 Batch  14508_51893 PROMOTE bckcetl ids20 dsadm bls for bl
# MAGIC ^1_1 09/20/07 10:51:09 Batch  14508_39073 INIT bckcett testIDS30 dsadm bls for bl
# MAGIC ^1_1 09/19/07 08:11:02 Batch  14507_29467 PROMOTE bckcett testIDS30 u08717 brent
# MAGIC ^1_1 09/19/07 07:44:31 Batch  14507_27876 INIT bckcett devlIDS30 u08717 Brent
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:      FctsIdsSubAddrBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                          Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              04/03/2007          3264                              Originally Programmed                                     devlIDS30                    Brent Leland                 08-30-2007
# MAGIC                                                                                                           Modified the balancing process, 
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


RunID = get_widget_value('RunID','')
ToleranceCd = get_widget_value('ToleranceCd','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
IDSOwner = get_widget_value('$IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = f"""
SELECT
  SUB_ADDR.SRC_SYS_CD_SK AS SRC_SRC_SYS_CD_SK,
  SUB_ADDR.SUB_UNIQ_KEY AS SRC_SUB_UNIQ_KEY,
  SUB_ADDR.SUB_ADDR_CD_SK AS SRC_SUB_ADDR_CD_SK,
  B_SUB_ADDR.SRC_SYS_CD_SK AS TRGT_SRC_SYS_CD_SK,
  B_SUB_ADDR.SUB_UNIQ_KEY AS TRGT_SUB_UNIQ_KEY,
  B_SUB_ADDR.SUB_ADDR_CD_SK AS TRGT_SUB_ADDR_CD_SK
FROM {IDSOwner}.SUB_ADDR SUB_ADDR
FULL OUTER JOIN {IDSOwner}.B_SUB_ADDR B_SUB_ADDR
  ON SUB_ADDR.SRC_SYS_CD_SK = B_SUB_ADDR.SRC_SYS_CD_SK
  AND SUB_ADDR.SUB_UNIQ_KEY = B_SUB_ADDR.SUB_UNIQ_KEY
  AND SUB_ADDR.SUB_ADDR_CD_SK = B_SUB_ADDR.SUB_ADDR_CD_SK
WHERE SUB_ADDR.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
"""

df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_Research = (
    df_SrcTrgtComp.filter(
        (F.col("SRC_SRC_SYS_CD_SK").isNull())
        | (F.col("SRC_SUB_ADDR_CD_SK").isNull())
        | (F.col("SRC_SUB_UNIQ_KEY").isNull())
        | (F.col("TRGT_SRC_SYS_CD_SK").isNull())
        | (F.col("TRGT_SUB_ADDR_CD_SK").isNull())
        | (F.col("TRGT_SUB_UNIQ_KEY").isNull())
    )
    .select(
        F.col("TRGT_SRC_SYS_CD_SK").alias("TRGT_SRC_SYS_CD_SK"),
        F.col("TRGT_SUB_UNIQ_KEY").alias("TRGT_SUB_UNIQ_KEY"),
        F.col("TRGT_SUB_ADDR_CD_SK").alias("TRGT_SUB_ADDR_CD_SK"),
        F.col("SRC_SRC_SYS_CD_SK").alias("SRC_SRC_SYS_CD_SK"),
        F.col("SRC_SUB_UNIQ_KEY").alias("SRC_SUB_UNIQ_KEY"),
        F.col("SRC_SUB_ADDR_CD_SK").alias("SRC_SUB_ADDR_CD_SK"),
    )
)

w1 = Window.orderBy(F.lit(1))
df_SrcTrgtComp_numbered = df_SrcTrgtComp.withColumn("row_num", F.row_number().over(w1))

df_NotifyPre = df_SrcTrgtComp_numbered.filter(
    (F.col("row_num") == 1) & (F.lit(ToleranceCd) == F.lit("OUT"))
)

df_Notify = df_NotifyPre.select(
    F.lit("ROW COUNT BALANCING FACETS - IDS SUB ADDR OUT OF TOLERANCE").alias("NOTIFICATION")
).withColumn("NOTIFICATION", F.rpad(F.col("NOTIFICATION"), 70, " "))

df_Research = df_Research.select(
    "TRGT_SRC_SYS_CD_SK",
    "TRGT_SUB_UNIQ_KEY",
    "TRGT_SUB_ADDR_CD_SK",
    "SRC_SRC_SYS_CD_SK",
    "SRC_SUB_UNIQ_KEY",
    "SRC_SUB_ADDR_CD_SK"
)

write_files(
    df_Research,
    f"{adls_path}/balancing/research/FacetsIdsSubAddrResearch.dat.{RunID}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)

df_Notify = df_Notify.select("NOTIFICATION")

write_files(
    df_Notify,
    f"{adls_path}/balancing/notify/MbrshpBalancingNotification.dat",
    ",",
    "append",
    False,
    False,
    "\"",
    None
)