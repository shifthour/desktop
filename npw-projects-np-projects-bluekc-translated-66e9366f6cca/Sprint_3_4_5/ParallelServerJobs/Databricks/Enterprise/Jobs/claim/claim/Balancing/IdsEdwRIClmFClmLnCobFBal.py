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
# MAGIC CALLED BY:          IdsEdwRIClmFClmLnCobFBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target tables are compared on the specified column to see if there are any discrepancies
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               09/20/2007          3264                              Originally Programmed                           devlEDW10              Steph Goddard             10/23/2007

# MAGIC Rows with Failed Comparison on Specified columns
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
from pyspark.sql.functions import col, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


# Parameter retrieval
EDWOwner = get_widget_value('EDWOwner', '')
edw_secret_name = get_widget_value('edw_secret_name', '')
ExtrRunCycle = get_widget_value('ExtrRunCycle', '')
RunID = get_widget_value('RunID', '')

# Get DB config
jdbc_url, jdbc_props = get_db_config(edw_secret_name)

# --------------------------------------------------------------------------------
# Source Stage: SrcTrgtRowComp (DB2Connector, Database=EDW)
# Output Link: prmKey
extract_query_prmKey = f"""
SELECT
CLM_F.CLM_SK,
CLM_LN_COB_F.CLM_LN_SK
FROM
{EDWOwner}.CLM_LN_F CLM_LN_F FULL OUTER JOIN {EDWOwner}.CLM_LN_COB_F CLM_LN_COB_F
ON CLM_LN_F.CLM_LN_SK = CLM_LN_COB_F.CLM_LN_SK,
{EDWOwner}.CLM_F CLM_F
WHERE
CLM_F.LAST_UPDT_RUN_CYC_EXCTN_SK = {ExtrRunCycle}
AND CLM_F.CLM_COB_IN <> 'Y'
AND CLM_F.CLM_SK = CLM_LN_F.CLM_SK
"""
df_SrcTrgtRowComp_prmKey = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_prmKey)
    .load()
)
df_SrcTrgtRowComp_prmKey = df_SrcTrgtRowComp_prmKey.select(
    col("CLM_SK").alias("CLM_F_CLM_SK"),
    col("CLM_LN_SK").alias("CLM_LN_COB_F_CLM_LN_SK")
)

# Output Link: Match
extract_query_Match = f"""
SELECT
CLM_F.SRC_SYS_CD,
CLM_F.CLM_ID,
CLM_LN_COB_F.SRC_SYS_CD,
CLM_LN_COB_F.CLM_ID
FROM
{EDWOwner}.CLM_F CLM_F INNER JOIN {EDWOwner}.CLM_LN_COB_F CLM_LN_COB_F
ON CLM_F.SRC_SYS_CD = CLM_LN_COB_F.SRC_SYS_CD
AND CLM_F.CLM_ID = CLM_LN_COB_F.CLM_ID
WHERE
CLM_F.LAST_UPDT_RUN_CYC_EXCTN_SK = {ExtrRunCycle}
AND CLM_F.CLM_COB_IN <> 'Y'
"""
df_SrcTrgtRowComp_Match = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_Match)
    .load()
)
# Rename columns so they match the downstream usage
df_SrcTrgtRowComp_Match = df_SrcTrgtRowComp_Match.select(
    col("SRC_SYS_CD").alias("CLM_F_SRC_SYS_CD"),
    col("CLM_ID").alias("CLM_F_CLM_ID"),
    col("SRC_SYS_CD_1").alias("CLM_LN_COB_F_SRC_SYS_CD"),
    col("CLM_ID_1").alias("CLM_LN_COB_F_CLM_ID")
)

# Output Link: nKey
extract_query_nKey = f"""
SELECT
CLM_F.SRC_SYS_CD,
CLM_F.CLM_ID,
CLM_LN_COB_F.SRC_SYS_CD,
CLM_LN_COB_F.CLM_ID
FROM
{EDWOwner}.CLM_F CLM_F FULL OUTER JOIN {EDWOwner}.CLM_LN_COB_F CLM_LN_COB_F
ON CLM_F.SRC_SYS_CD = CLM_LN_COB_F.SRC_SYS_CD AND CLM_F.CLM_ID = CLM_LN_COB_F.CLM_ID
WHERE
CLM_F.LAST_UPDT_RUN_CYC_EXCTN_SK = {ExtrRunCycle}
AND CLM_F.CLM_COB_IN <> 'Y'
"""
df_SrcTrgtRowComp_nKey = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_nKey)
    .load()
)
df_SrcTrgtRowComp_nKey = df_SrcTrgtRowComp_nKey.select(
    col("SRC_SYS_CD").alias("CLM_F_SRC_SYS_CD"),
    col("CLM_ID").alias("CLM_F_CLM_ID"),
    col("SRC_SYS_CD_1").alias("CLM_LN_COB_F_SRC_SYS_CD"),
    col("CLM_ID_1").alias("CLM_LN_COB_F_CLM_ID")
)

# --------------------------------------------------------------------------------
# Stage: ParChldMatch (CSeqFileStage)
df_ParChldMatch_out = df_SrcTrgtRowComp_Match.select(
    rpad(col("CLM_F_SRC_SYS_CD"), <...>, " ").alias("CLM_F_SRC_SYS_CD"),
    rpad(col("CLM_F_CLM_ID"), <...>, " ").alias("CLM_F_CLM_ID"),
    rpad(col("CLM_LN_COB_F_SRC_SYS_CD"), <...>, " ").alias("CLM_LN_COB_F_SRC_SYS_CD"),
    rpad(col("CLM_LN_COB_F_CLM_ID"), <...>, " ").alias("CLM_LN_COB_F_CLM_ID")
)
write_files(
    df_ParChldMatch_out,
    f"{adls_path}/balancing/sync/ClmFClmLnCobFBalancingTotalMatch.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)

# --------------------------------------------------------------------------------
# Stage: Transformer_19 (CTransformerStage) => Input: df_SrcTrgtRowComp_nKey
df_Transformer_19_NatKey = df_SrcTrgtRowComp_nKey.filter(
    (col("CLM_F_CLM_ID").isNull())
    | (col("CLM_F_SRC_SYS_CD").isNull())
    | (col("CLM_LN_COB_F_CLM_ID").isNull())
    | (col("CLM_LN_COB_F_SRC_SYS_CD").isNull())
).select(
    col("CLM_F_SRC_SYS_CD"),
    col("CLM_F_CLM_ID"),
    col("CLM_LN_COB_F_SRC_SYS_CD"),
    col("CLM_LN_COB_F_CLM_ID")
)

# --------------------------------------------------------------------------------
# Stage: Transform2 (CTransformerStage) => Input: df_Transformer_19_NatKey
df_Transform2_Research3 = df_Transformer_19_NatKey.filter(
    (col("CLM_F_CLM_ID").isNull()) | (col("CLM_F_SRC_SYS_CD").isNull())
).select(
    col("CLM_F_SRC_SYS_CD"),
    col("CLM_F_CLM_ID"),
    col("CLM_LN_COB_F_SRC_SYS_CD"),
    col("CLM_LN_COB_F_CLM_ID")
)

df_Transform2_Research4 = df_Transformer_19_NatKey.filter(
    (col("CLM_LN_COB_F_CLM_ID").isNull()) | (col("CLM_LN_COB_F_SRC_SYS_CD").isNull())
).select(
    col("CLM_F_SRC_SYS_CD"),
    col("CLM_F_CLM_ID"),
    col("CLM_LN_COB_F_SRC_SYS_CD"),
    col("CLM_LN_COB_F_CLM_ID")
)

df_Transform2_Notify = (
    df_Transformer_19_NatKey.limit(1)
    .select(
        rpad(
            lit("REFERENTIAL INTEGRITY BALANCING NATURAL KEYS IDS - EDW CLM F AND CLM LN COB F CHECK FOR OUT OF TOLERANCE"),
            70,
            " "
        ).alias("NOTIFICATION")
    )
)

# --------------------------------------------------------------------------------
# Stage: ResearchFile3 (CSeqFileStage)
df_ResearchFile3_out = df_Transform2_Research3.select(
    rpad(col("CLM_F_SRC_SYS_CD"), <...>, " ").alias("CLM_F_SRC_SYS_CD"),
    rpad(col("CLM_F_CLM_ID"), <...>, " ").alias("CLM_F_CLM_ID"),
    rpad(col("CLM_LN_COB_F_SRC_SYS_CD"), <...>, " ").alias("CLM_LN_COB_F_SRC_SYS_CD"),
    rpad(col("CLM_LN_COB_F_CLM_ID"), <...>, " ").alias("CLM_LN_COB_F_CLM_ID")
)
write_files(
    df_ResearchFile3_out,
    f"{adls_path}/balancing/research/IdsEdwNatkeyParChldClmFClmLnCobFRIResearch.dat.{RunID}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)

# --------------------------------------------------------------------------------
# Stage: ResearchFile4 (CSeqFileStage)
df_ResearchFile4_out = df_Transform2_Research4.select(
    rpad(col("CLM_F_SRC_SYS_CD"), <...>, " ").alias("CLM_F_SRC_SYS_CD"),
    rpad(col("CLM_F_CLM_ID"), <...>, " ").alias("CLM_F_CLM_ID"),
    rpad(col("CLM_LN_COB_F_SRC_SYS_CD"), <...>, " ").alias("CLM_LN_COB_F_SRC_SYS_CD"),
    rpad(col("CLM_LN_COB_F_CLM_ID"), <...>, " ").alias("CLM_LN_COB_F_CLM_ID")
)
write_files(
    df_ResearchFile4_out,
    f"{adls_path}/balancing/research/IdsEdwNatkeyChldParClmFClmLnCobFRIResearch.dat.{RunID}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)

# --------------------------------------------------------------------------------
# Stage: NotificationFile2 (CSeqFileStage)
df_NotificationFile2_out = df_Transform2_Notify.select(
    rpad(col("NOTIFICATION"), 70, " ").alias("NOTIFICATION")
)
write_files(
    df_NotificationFile2_out,
    f"{adls_path}/balancing/notify/ClaimsBalancingNotification.txt",
    ",",
    "append",
    False,
    False,
    "\"",
    None
)

# --------------------------------------------------------------------------------
# Stage: Transformer_20 (CTransformerStage) => Input: df_SrcTrgtRowComp_prmKey
df_Transformer_20_Pkey = df_SrcTrgtRowComp_prmKey.filter(
    (col("CLM_F_CLM_SK").isNull()) | (col("CLM_LN_COB_F_CLM_LN_SK").isNull())
).select(
    col("CLM_F_CLM_SK"),
    col("CLM_LN_COB_F_CLM_LN_SK")
)

# --------------------------------------------------------------------------------
# Stage: Transform1 (CTransformerStage) => Input: df_Transformer_20_Pkey
df_Transform1_Research1 = df_Transformer_20_Pkey.filter(
    col("CLM_F_CLM_SK").isNull()
).select(
    col("CLM_F_CLM_SK"),
    col("CLM_LN_COB_F_CLM_LN_SK")
)

df_Transform1_Research2 = df_Transformer_20_Pkey.filter(
    col("CLM_LN_COB_F_CLM_LN_SK").isNull()
).select(
    col("CLM_F_CLM_SK"),
    col("CLM_LN_COB_F_CLM_LN_SK")
)

df_Transform1_Notify = (
    df_Transformer_20_Pkey.limit(1)
    .select(
        rpad(
            lit("REFERENTIAL INTEGRITY BALANCING PRIMARY KEY IDS - EDW CLM F AND CLM LN COB F CHECK FOR OUT OF TOLERANCE"),
            70,
            " "
        ).alias("NOTIFICATION")
    )
)

# --------------------------------------------------------------------------------
# Stage: NotificationFile1 (CSeqFileStage)
df_NotificationFile1_out = df_Transform1_Notify.select(
    rpad(col("NOTIFICATION"), 70, " ").alias("NOTIFICATION")
)
write_files(
    df_NotificationFile1_out,
    f"{adls_path}/balancing/notify/ClaimsBalancingNotification.txt",
    ",",
    "append",
    False,
    False,
    "\"",
    None
)

# --------------------------------------------------------------------------------
# Stage: ResearchFile1 (CSeqFileStage)
df_ResearchFile1_out = df_Transform1_Research1.select(
    rpad(col("CLM_F_CLM_SK"), <...>, " ").alias("CLM_F_CLM_SK"),
    rpad(col("CLM_LN_COB_F_CLM_LN_SK"), <...>, " ").alias("CLM_LN_COB_F_CLM_LN_SK")
)
write_files(
    df_ResearchFile1_out,
    f"{adls_path}/balancing/research/IdsEdwPkeyParChldClmFClmLnCobFRIResearch.dat.{RunID}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)

# --------------------------------------------------------------------------------
# Stage: ResearchFile2 (CSeqFileStage)
df_ResearchFile2_out = df_Transform1_Research2.select(
    rpad(col("CLM_F_CLM_SK"), <...>, " ").alias("CLM_F_CLM_SK"),
    rpad(col("CLM_LN_COB_F_CLM_LN_SK"), <...>, " ").alias("CLM_LN_COB_F_CLM_LN_SK")
)
write_files(
    df_ResearchFile2_out,
    f"{adls_path}/balancing/research/IdsEdwPkeyChldParClmFClmLnCobFRIResearch.dat.{RunID}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)