# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_3 03/23/09 15:20:40 Batch  15058_55245 INIT bckcetl edw10 dsadm dsadm
# MAGIC ^1_1 03/12/09 19:44:50 Batch  15047_71107 INIT bckcetl edw10 dcg01 sa - bringing everthing down for test
# MAGIC ^1_2 02/25/09 17:10:06 Batch  15032_61828 INIT bckcetl edw10 dcg01 Bringing production code down to edw_test  claim only
# MAGIC ^1_1 02/01/08 12:08:08 Batch  14642_43698 INIT bckcetl edw10 dsadm dsadm
# MAGIC ^1_1 12/27/07 14:18:21 Batch  14606_51505 INIT bckcetl edw10 dsadm dsadm
# MAGIC ^1_1 11/05/07 14:33:17 Batch  14554_52458 PROMOTE bckcetl edw10 dsadm rc for brent
# MAGIC ^1_1 11/05/07 13:30:09 Batch  14554_48642 INIT bckcett testEDW10 dsadm rc for brent
# MAGIC ^1_3 11/02/07 15:11:26 Batch  14551_54690 PROMOTE bckcett testEDW10 u08717 Brent
# MAGIC ^1_3 11/02/07 15:00:18 Batch  14551_54021 INIT bckcett devlEDW10 u08717 Brent
# MAGIC ^1_2 11/02/07 07:43:09 Batch  14551_27794 INIT bckcett devlEDW10 u08717 Brent
# MAGIC ^1_1 11/01/07 11:00:01 Batch  14550_39611 INIT bckcett devlEDW10 u08717 Brent
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:          IdsEdwRIClmFAdjFromClmIdBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target tables are compared on the specified column to see if there are any discrepancies
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               09/21/2007          3264                              Originally Programmed                           devlEDW10              Steph Goddard             10/22/2007

# MAGIC Rows with Failed Comparison on Specified columns
# MAGIC File checked later for rows and email to on-call
# MAGIC Detail files for on-call to research errors
# MAGIC File checked later for rows and email to on-call
# MAGIC Detail files for on-call to research errors
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------

EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
RunID = get_widget_value('RunID','')
jdbc_url, jdbc_props = get_db_config(edw_secret_name)

extract_query_pkey = f"""
SELECT
ClmF1.CLM_SK,
ClmF2.CLM_ADJ_FROM_CLM_SK
FROM
{EDWOwner}.CLM_F ClmF2 FULL OUTER JOIN {EDWOwner}.CLM_F ClmF1
ON ClmF2.CLM_ADJ_FROM_CLM_SK = ClmF1.CLM_SK
WHERE 
ClmF2.LAST_UPDT_RUN_CYC_EXCTN_SK = {ExtrRunCycle}
"""
df_SrcTrgtRowComp_Pkey = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_pkey)
    .load()
)

extract_query_match = f"""
SELECT
ClmF1.CLM_ID,
ClmF2.CLM_ADJ_FROM_CLM_ID
FROM
{EDWOwner}.CLM_F ClmF2
INNER JOIN {EDWOwner}.CLM_F ClmF1
ON ClmF2.CLM_ADJ_FROM_CLM_ID = ClmF1.CLM_ID 
AND ClmF2.SRC_SYS_CD = ClmF1.SRC_SYS_CD
WHERE 
ClmF2.LAST_UPDT_RUN_CYC_EXCTN_SK = {ExtrRunCycle}
"""
df_SrcTrgtRowComp_Match = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_match)
    .load()
)

extract_query_natkey = f"""
SELECT
ClmF1.CLM_ID,
ClmF2.CLM_ADJ_FROM_CLM_ID
FROM
{EDWOwner}.CLM_F ClmF2
FULL OUTER JOIN {EDWOwner}.CLM_F ClmF1
ON ClmF2.CLM_ADJ_FROM_CLM_ID = ClmF1.CLM_ID 
AND ClmF2.SRC_SYS_CD = ClmF1.SRC_SYS_CD
WHERE 
ClmF2.LAST_UPDT_RUN_CYC_EXCTN_SK = {ExtrRunCycle}
"""
df_SrcTrgtRowComp_NatKey = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_natkey)
    .load()
)

df_ParChldMatch = df_SrcTrgtRowComp_Match.select(
    F.col("CLM_ID"),
    F.col("CLM_ADJ_FROM_CLM_ID")
)
write_files(
    df_ParChldMatch,
    f"{adls_path}/balancing/sync/ClmFAdjFromClmIdBalancingTotalMatch.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_Transform1_Research1 = df_SrcTrgtRowComp_Pkey.filter(
    F.col("CLM_ADJ_FROM_CLM_SK").isNull()
).select(
    F.col("CLM_SK").alias("CLM_SK"),
    F.col("CLM_ADJ_FROM_CLM_SK").alias("CLM_ADJ_FROM_CLM_SK")
)

df_Transformer_23_Research1a = df_Transform1_Research1.select(
    F.col("CLM_SK").alias("CLM_SK"),
    F.col("CLM_ADJ_FROM_CLM_SK").alias("CLM_ADJ_FROM_CLM_SK")
)

df_Transformer_23_Notify_temp = df_Transform1_Research1.limit(1).select(
    F.lit("REFERENTIAL INTEGRITY BALANCING PRIMARY KEY IDS - EDW CLM F ADJ FROM CLM ID CHECK FOR OUT OF TOLERANCE").alias("NOTIFICATION")
)
df_Transformer_23_Notify = df_Transformer_23_Notify_temp.select(
    F.rpad(F.col("NOTIFICATION"), 70, " ").alias("NOTIFICATION")
)
write_files(
    df_Transformer_23_Notify,
    f"{adls_path}/balancing/notify/ClaimsBalancingNotification.txt",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_ResearchFile1 = df_Transformer_23_Research1a.select(
    F.col("CLM_SK"),
    F.col("CLM_ADJ_FROM_CLM_SK")
)
write_files(
    df_ResearchFile1,
    f"{adls_path}/balancing/research/IdsEdwPkeyParChldClmFAdjFromClmIdRIResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_Transform2_Research2 = df_SrcTrgtRowComp_NatKey.filter(
    F.col("CLM_ADJ_FROM_CLM_ID").isNull()
).select(
    F.col("CLM_ID").alias("CLM_F_CLM_ID"),
    F.col("CLM_ADJ_FROM_CLM_ID").alias("CLM_ADJ_FROM_CLM_ID")
)

df_Transformer_21_Notify_temp = df_Transform2_Research2.limit(1).select(
    F.lit("REFERENTIAL INTEGRITY BALANCING NATURAL KEYS IDS - EDW CLM F ADJ FROM CLM ID CHECK FOR OUT OF TOLERANCE").alias("NOTIFICATION")
)
df_Transformer_21_Notify = df_Transformer_21_Notify_temp.select(
    F.rpad(F.col("NOTIFICATION"), 70, " ").alias("NOTIFICATION")
)

df_Transformer_21_Research2a = df_Transform2_Research2.select(
    F.col("CLM_F_CLM_ID").alias("CLM_F_CLM_ID"),
    F.col("CLM_ADJ_FROM_CLM_ID").alias("CLM_ADJ_FROM_CLM_ID")
)

write_files(
    df_Transformer_21_Notify,
    f"{adls_path}/balancing/notify/ClaimsBalancingNotification.txt",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_ResearchFile2 = df_Transformer_21_Research2a.select(
    F.col("CLM_F_CLM_ID"),
    F.col("CLM_ADJ_FROM_CLM_ID")
)
write_files(
    df_ResearchFile2,
    f"{adls_path}/balancing/research/IdsEdwNatkeyParChldClmFAdjFromClmIdRIResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)