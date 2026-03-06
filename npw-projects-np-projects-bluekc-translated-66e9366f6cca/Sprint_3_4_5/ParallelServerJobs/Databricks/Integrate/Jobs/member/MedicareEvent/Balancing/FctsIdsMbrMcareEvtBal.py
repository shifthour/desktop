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
# MAGIC CALLED BY:      FctsIdsMbrMcareEvtBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                       Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                 ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              3/27/2007          3264                              Originally Programmed                                     devlIDS30                   Brent Leland                 08-30-2007
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
from pyspark.sql import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


RunID = get_widget_value('RunID','')
ToleranceCd = get_widget_value('ToleranceCd','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = f"""
SELECT
MBR_MCARE_EVT.SRC_SYS_CD_SK AS SRC_SRC_SYS_CD_SK,
MBR_MCARE_EVT.MBR_UNIQ_KEY AS SRC_MBR_UNIQ_KEY,
MBR_MCARE_EVT.MBR_MCARE_EVT_CD_SK AS SRC_MBR_MCARE_EVT_CD_SK,
MBR_MCARE_EVT.EFF_DT_SK AS SRC_EFF_DT_SK,
B_MBR_MCARE_EVT.SRC_SYS_CD_SK AS TRGT_SRC_SYS_CD_SK,
B_MBR_MCARE_EVT.MBR_UNIQ_KEY AS TRGT_MBR_UNIQ_KEY,
B_MBR_MCARE_EVT.MBR_MCARE_EVT_CD_SK AS TRGT_MBR_MCARE_EVT_CD_SK,
B_MBR_MCARE_EVT.EFF_DT_SK AS TRGT_EFF_DT_SK
FROM {IDSOwner}.MBR_MCARE_EVT MBR_MCARE_EVT
FULL OUTER JOIN {IDSOwner}.B_MBR_MCARE_EVT B_MBR_MCARE_EVT
ON MBR_MCARE_EVT.SRC_SYS_CD_SK = B_MBR_MCARE_EVT.SRC_SYS_CD_SK
AND MBR_MCARE_EVT.MBR_UNIQ_KEY = B_MBR_MCARE_EVT.MBR_UNIQ_KEY
AND MBR_MCARE_EVT.MBR_MCARE_EVT_CD_SK = B_MBR_MCARE_EVT.MBR_MCARE_EVT_CD_SK
AND MBR_MCARE_EVT.EFF_DT_SK = B_MBR_MCARE_EVT.EFF_DT_SK
WHERE
MBR_MCARE_EVT.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
"""

df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_research = df_SrcTrgtComp.filter(
    F.col("SRC_EFF_DT_SK").isNull()
    | F.col("SRC_MBR_MCARE_EVT_CD_SK").isNull()
    | F.col("SRC_MBR_UNIQ_KEY").isNull()
    | F.col("SRC_SRC_SYS_CD_SK").isNull()
    | F.col("TRGT_EFF_DT_SK").isNull()
    | F.col("TRGT_MBR_MCARE_EVT_CD_SK").isNull()
    | F.col("TRGT_MBR_UNIQ_KEY").isNull()
    | F.col("TRGT_SRC_SYS_CD_SK").isNull()
)

df_research = df_research.withColumn(
    "TRGT_EFF_DT_SK", rpad(F.col("TRGT_EFF_DT_SK"), 10, " ")
).withColumn(
    "SRC_EFF_DT_SK", rpad(F.col("SRC_EFF_DT_SK"), 10, " ")
).select(
    "TRGT_SRC_SYS_CD_SK",
    "TRGT_MBR_UNIQ_KEY",
    "TRGT_MBR_MCARE_EVT_CD_SK",
    "TRGT_EFF_DT_SK",
    "SRC_SRC_SYS_CD_SK",
    "SRC_MBR_UNIQ_KEY",
    "SRC_MBR_MCARE_EVT_CD_SK",
    "SRC_EFF_DT_SK"
)

write_files(
    df_research,
    f"{adls_path}/balancing/research/FacetsIdsMbrMcareEvtResearch.dat.{RunID}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)

w = Window.orderBy(F.lit(1))
df_notifyCandidate = df_SrcTrgtComp.withColumn("row_num", F.row_number().over(w)).filter(
    (F.col("row_num") == 1) & (F.lit(ToleranceCd) == F.lit("OUT"))
)

df_notify = df_notifyCandidate.select(
    rpad(
        F.lit("ROW COUNT BALANCING FACETS - IDS MBR MCARE EVT OUT OF TOLERANCE"), 70, " "
    ).alias("NOTIFICATION")
)

write_files(
    df_notify,
    f"{adls_path}/balancing/notify/MbrshpBalancingNotification.dat",
    ",",
    "append",
    False,
    False,
    "\"",
    None
)