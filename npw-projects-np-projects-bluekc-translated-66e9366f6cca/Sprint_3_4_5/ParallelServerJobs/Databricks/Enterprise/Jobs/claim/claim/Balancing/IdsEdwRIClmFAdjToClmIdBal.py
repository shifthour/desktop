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
# MAGIC CALLED BY:          IdsEdwRIClmFAdjToClmIdBalSeq
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
# MAGIC Detail files for on-call to research errors
# MAGIC File checked later for rows and email to on-call
# MAGIC Detail files for on-call to research errors
# MAGIC File checked later for rows and email to on-call
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, isnull, rpad
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


# Parameters
EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
RunID = get_widget_value('RunID','')

# Obtain DB connection properties
jdbc_url, jdbc_props = get_db_config(edw_secret_name)

# Read data from EDW (SrcTrgtRowComp Stage) - Output pin: Pkey
extract_query_Pkey = f"""SELECT
ClmF1.CLM_SK,
ClmF2.CLM_ADJ_TO_CLM_SK
FROM {EDWOwner}.CLM_F ClmF2 
FULL OUTER JOIN {EDWOwner}.CLM_F ClmF1 
ON ClmF2.CLM_ADJ_TO_CLM_SK = ClmF1.CLM_SK
WHERE ClmF2.LAST_UPDT_RUN_CYC_EXCTN_SK = {ExtrRunCycle}
"""
df_SrcTrgtRowComp_Pkey = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_Pkey)
    .load()
)

# Read data from EDW (SrcTrgtRowComp Stage) - Output pin: Match
extract_query_Match = f"""SELECT
ClmF1.CLM_ID,
ClmF2.CLM_ADJ_TO_CLM_ID
FROM {EDWOwner}.CLM_F ClmF2 
INNER JOIN {EDWOwner}.CLM_F ClmF1 
ON ClmF2.CLM_ADJ_TO_CLM_ID = ClmF1.CLM_ID 
AND ClmF2.SRC_SYS_CD = ClmF1.SRC_SYS_CD
WHERE ClmF2.LAST_UPDT_RUN_CYC_EXCTN_SK = {ExtrRunCycle}
"""
df_SrcTrgtRowComp_Match = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_Match)
    .load()
)

# Read data from EDW (SrcTrgtRowComp Stage) - Output pin: NatKey
extract_query_NatKey = f"""SELECT
ClmF1.CLM_ID,
ClmF2.CLM_ADJ_TO_CLM_ID
FROM {EDWOwner}.CLM_F ClmF2 
FULL OUTER JOIN {EDWOwner}.CLM_F ClmF1 
ON ClmF2.CLM_ADJ_TO_CLM_ID = ClmF1.CLM_ID 
AND ClmF2.SRC_SYS_CD = ClmF1.SRC_SYS_CD
WHERE ClmF2.LAST_UPDT_RUN_CYC_EXCTN_SK = {ExtrRunCycle}
"""
df_SrcTrgtRowComp_NatKey = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_NatKey)
    .load()
)

# Read data from EDW (SrcTrgtRowComp Stage) - Output pin: RvrslNatKey
extract_query_RvrslNatKey = f"""SELECT
ClmF1.CLM_ID,
ClmF2.CLM_ADJ_TO_CLM_ID
FROM {EDWOwner}.CLM_F ClmF2 
FULL OUTER JOIN {EDWOwner}.CLM_F ClmF1 
ON ClmF2.CLM_ID || 'R' = ClmF1.CLM_ID 
AND ClmF2.CLM_ADJ_TO_CLM_ID = ClmF1.CLM_ID
AND ClmF2.SRC_SYS_CD = ClmF1.SRC_SYS_CD
WHERE ClmF2.LAST_UPDT_RUN_CYC_EXCTN_SK = {ExtrRunCycle}
"""
df_SrcTrgtRowComp_RvrslNatKey = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_RvrslNatKey)
    .load()
)

# ParChldMatch Stage (CSeqFileStage) writing from df_SrcTrgtRowComp_Match
df_ParChldMatch = df_SrcTrgtRowComp_Match.select("CLM_ID", "CLM_ADJ_TO_CLM_ID")
write_files(
    df_ParChldMatch,
    f"{adls_path}/balancing/sync/ClmFAdjToClmIdBalancingTotalMatch.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)

# Transform1: filter ISNULL(CLM_ADJ_TO_CLM_SK)
df_Transform1_Research1 = (
    df_SrcTrgtRowComp_Pkey
    .filter(col("CLM_ADJ_TO_CLM_SK").isNull())
    .select(
        col("CLM_SK").alias("CLM_SK"),
        col("CLM_ADJ_TO_CLM_SK").alias("CLM_ADJ_TO_CLM_SK")
    )
)

# Transformer_25
# Primary output: Research1a (no constraint other than the input to the stage)
df_Transformer25_Research1a = df_Transform1_Research1.select(
    col("CLM_SK"),
    col("CLM_ADJ_TO_CLM_SK")
)
# Notify (Constraint = @INROWNUM=1)
df_Transformer25_Notify = df_Transform1_Research1.limit(1).withColumn(
    "NOTIFICATION",
    lit("REFERENTIAL INTEGRITY BALANCING PRIMARY KEY IDS - EDW CLM F ADJ TO CLM ID CHECK FOR OUT OF TOLERANCE")
)
df_Transformer25_Notify = df_Transformer25_Notify.withColumn(
    "NOTIFICATION",
    rpad("NOTIFICATION", 70, " ")
)

# NotificationFile1
df_NotificationFile1 = df_Transformer25_Notify.select("NOTIFICATION")
write_files(
    df_NotificationFile1,
    f"{adls_path}/balancing/notify/ClaimsBalancingNotification.txt",
    ",",
    "append",
    False,
    False,
    "\"",
    None
)

# ResearchFile1
df_ResearchFile1 = df_Transformer25_Research1a.select("CLM_SK", "CLM_ADJ_TO_CLM_SK")
write_files(
    df_ResearchFile1,
    f"{adls_path}/balancing/research/IdsEdwPkeyParChldClmFAdjFromClmIdRIResearch.dat.{RunID}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)

# Transform2: filter ISNULL(CLM_ADJ_TO_CLM_ID), rename columns
df_Transform2_Research2 = (
    df_SrcTrgtRowComp_NatKey
    .filter(col("CLM_ADJ_TO_CLM_ID").isNull())
    .select(
        col("CLM_ID").alias("CLM_F_CLM_ID"),
        col("CLM_ADJ_TO_CLM_ID").alias("CLM_ADJ_TO_CLM_ID")
    )
)

# Transformer_27
# Primary output: Reseach2a
df_Transformer27_Research2a = df_Transform2_Research2.select(
    col("CLM_F_CLM_ID"),
    col("CLM_ADJ_TO_CLM_ID")
)
# Notify (@INROWNUM=1)
df_Transformer27_Notify = df_Transform2_Research2.limit(1).withColumn(
    "NOTIFICATION",
    lit("REFERENTIAL INTEGRITY BALANCING NATURAL KEYS IDS - EDW CLM F ADJ TO CLM ID CHECK FOR OUT OF TOLERANCE")
)
df_Transformer27_Notify = df_Transformer27_Notify.withColumn(
    "NOTIFICATION",
    rpad("NOTIFICATION", 70, " ")
)

# ResearchFile2
df_ResearchFile2 = df_Transformer27_Research2a.select("CLM_F_CLM_ID", "CLM_ADJ_TO_CLM_ID")
write_files(
    df_ResearchFile2,
    f"{adls_path}/balancing/research/IdsEdwNatkeyParChldClmFAdjToClmIdRIResearch.dat.{RunID}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)

# NotificationFile2
df_NotificationFile2 = df_Transformer27_Notify.select("NOTIFICATION")
write_files(
    df_NotificationFile2,
    f"{adls_path}/balancing/notify/ClaimsBalancingNotification.txt",
    ",",
    "append",
    False,
    False,
    "\"",
    None
)

# Transform3: rename "CLM_ADJ_TO_CLM_ID" -> "CLM_RVRSL_ADJ_TO_CLM_ID", filter ISNULL
df_Transform3 = df_SrcTrgtRowComp_RvrslNatKey.withColumnRenamed(
    "CLM_ADJ_TO_CLM_ID", 
    "CLM_RVRSL_ADJ_TO_CLM_ID"
)
df_Transform3_Research3 = (
    df_Transform3
    .filter(col("CLM_RVRSL_ADJ_TO_CLM_ID").isNull())
    .select(
        col("CLM_ID").alias("CLM_ID"),
        col("CLM_RVRSL_ADJ_TO_CLM_ID").alias("CLM_RVRSL_ADJ_TO_CLM_ID")
    )
)

# Transformer_29
# Primary output: Reseach3a
df_Transformer29_Research3a = df_Transform3_Research3.select(
    col("CLM_ID"),
    col("CLM_RVRSL_ADJ_TO_CLM_ID")
)
# Notify (@INROWNUM=1)
df_Transformer29_Notify = df_Transform3_Research3.limit(1).withColumn(
    "NOTIFICATION",
    lit("REFERENTIAL INTEGRITY BALANCING REVERSAL NATURAL KEYS IDS - EDW CLM F ADJ TO CLM ID CHECK FOR OUT OF TOLERANCE")
)
df_Transformer29_Notify = df_Transformer29_Notify.withColumn(
    "NOTIFICATION",
    rpad("NOTIFICATION", 70, " ")
)

# ResearchFile3
df_ResearchFile3 = df_Transformer29_Research3a.select("CLM_ID", "CLM_RVRSL_ADJ_TO_CLM_ID")
write_files(
    df_ResearchFile3,
    f"{adls_path}/balancing/research/IdsEdwRvrslNatkeyParChldClmFAdjToClmIdRIResearch.dat.{RunID}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)

# NotificationFile3
df_NotificationFile3 = df_Transformer29_Notify.select("NOTIFICATION")
write_files(
    df_NotificationFile3,
    f"{adls_path}/balancing/notify/ClaimsBalancingNotification.txt",
    ",",
    "append",
    False,
    False,
    "\"",
    None
)