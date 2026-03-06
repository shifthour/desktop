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
# MAGIC ^1_1 09/20/07 14:41:37 Batch  14508_52916 PROMOTE bckcetl ids20 dsadm bls for bl
# MAGIC ^1_1 09/20/07 13:58:24 Batch  14508_50316 INIT bckcett testIDS30 dsadm bls for bl
# MAGIC ^1_1 09/19/07 08:11:02 Batch  14507_29467 PROMOTE bckcett testIDS30 u08717 brent
# MAGIC ^1_1 09/19/07 07:44:31 Batch  14507_27876 INIT bckcett devlIDS30 u08717 Brent
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:       FctsIdsSubRateBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                       Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                 ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              04/05/2007          3264                              Originally Programmed                                  devlIDS30                    Brent Leland                 08-30-2007
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
from pyspark.sql import types as T
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


RunID = get_widget_value('RunID','')
ToleranceCd = get_widget_value('ToleranceCd','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query_SrcTrgtComp = f"""
SELECT 
    SUB_RATE.SRC_SYS_CD_SK as SRC_SRC_SYS_CD_SK,
    SUB_RATE.SUB_UNIQ_KEY as SRC_SUB_UNIQ_KEY,
    SUB_RATE.EFF_DT_SK as SRC_EFF_DT_SK,
    B_SUB_RATE.SRC_SYS_CD_SK as TRGT_SRC_SYS_CD_SK,
    B_SUB_RATE.SUB_UNIQ_KEY as TRGT_SUB_UNIQ_KEY,
    B_SUB_RATE.EFF_DT_SK as TRGT_EFF_DT_SK
FROM {IDSOwner}.SUB_RATE SUB_RATE
FULL OUTER JOIN {IDSOwner}.B_SUB_RATE B_SUB_RATE
    ON SUB_RATE.SRC_SYS_CD_SK = B_SUB_RATE.SRC_SYS_CD_SK
    AND SUB_RATE.SUB_UNIQ_KEY = B_SUB_RATE.SUB_UNIQ_KEY
    AND SUB_RATE.EFF_DT_SK = B_SUB_RATE.EFF_DT_SK
WHERE SUB_RATE.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
"""

df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_SrcTrgtComp)
    .load()
)

df_Research = df_SrcTrgtComp.filter(
    F.isnull("SRC_EFF_DT_SK")
    | F.isnull("SRC_SRC_SYS_CD_SK")
    | F.isnull("SRC_SUB_UNIQ_KEY")
    | F.isnull("TRGT_EFF_DT_SK")
    | F.isnull("TRGT_SRC_SYS_CD_SK")
    | F.isnull("TRGT_SUB_UNIQ_KEY")
).select(
    F.col("TRGT_SRC_SYS_CD_SK"),
    F.col("TRGT_SUB_UNIQ_KEY"),
    F.expr("rpad(TRGT_EFF_DT_SK, 10, ' ')").alias("TRGT_EFF_DT_SK"),
    F.col("SRC_SRC_SYS_CD_SK"),
    F.col("SRC_SUB_UNIQ_KEY"),
    F.expr("rpad(SRC_EFF_DT_SK, 10, ' ')").alias("SRC_EFF_DT_SK")
)

if ToleranceCd == "OUT":
    df_notify_temp = df_SrcTrgtComp.limit(1)
    df_Notify = df_notify_temp.select(
        F.expr("rpad('ROW COUNT BALANCING FACETS - IDS SUB RATE OUT OF TOLERANCE', 70, ' ')").alias("NOTIFICATION")
    )
else:
    empty_schema = T.StructType([T.StructField("NOTIFICATION", T.StringType(), True)])
    df_Notify = spark.createDataFrame([], schema=empty_schema)

write_files(
    df_Research,
    f"{adls_path}/balancing/research/FacetsIdsSubRateResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

write_files(
    df_Notify,
    f"{adls_path}/balancing/notify/MbrshpBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)