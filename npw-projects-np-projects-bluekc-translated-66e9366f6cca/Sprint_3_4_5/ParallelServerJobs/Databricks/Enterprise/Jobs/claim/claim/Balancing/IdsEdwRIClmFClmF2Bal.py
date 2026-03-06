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
# MAGIC CALLED BY:          IdsEdwRIClmFClmF2BalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target tables are compared on the specified column to see if there are any discrepancies
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               09/19/2007          3264                              Originally Programmed                           devlEDW10              Steph Goddard            10/22/2007

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


EDWOwner = get_widget_value("EDWOwner","")
edw_secret_name = get_widget_value("edw_secret_name","")
ExtrRunCycle = get_widget_value("ExtrRunCycle","")
RunID = get_widget_value("RunID","")

jdbc_url, jdbc_props = get_db_config(edw_secret_name)

# SrcTrgtRowComp - DB2Connector (Pkey)
query_SrcTrgtRowComp_Pkey = f"""
SELECT
CLM_F.CLM_SK as CLM_F_CLM_SK,
CLM_F2.CLM_SK as CLM_F2_CLM_SK
FROM {EDWOwner}.CLM_F CLM_F
FULL OUTER JOIN {EDWOwner}.CLM_F2 CLM_F2
ON CLM_F.CLM_SK = CLM_F2.CLM_SK
WHERE
CLM_F.LAST_UPDT_RUN_CYC_EXCTN_SK = {ExtrRunCycle}
AND CLM_F.CLM_STTUS_CD IN ('A02','A08','A09')
"""
df_SrcTrgtRowComp_Pkey = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_SrcTrgtRowComp_Pkey)
    .load()
)

# SrcTrgtRowComp - DB2Connector (Match)
query_SrcTrgtRowComp_Match = f"""
SELECT
CLM_F.SRC_SYS_CD as CLM_F_SRC_SYS_CD,
CLM_F.CLM_ID as CLM_F_CLM_ID,
CLM_F2.SRC_SYS_CD as CLM_F2_SRC_SYS_CD,
CLM_F2.CLM_ID as CLM_F2_CLM_ID
FROM {EDWOwner}.CLM_F CLM_F
INNER JOIN {EDWOwner}.CLM_F2 CLM_F2
ON CLM_F.SRC_SYS_CD = CLM_F2.SRC_SYS_CD
AND CLM_F.CLM_ID = CLM_F2.CLM_ID
WHERE
CLM_F.LAST_UPDT_RUN_CYC_EXCTN_SK = {ExtrRunCycle}
AND CLM_F.CLM_STTUS_CD IN ('A02','A08','A09')
"""
df_SrcTrgtRowComp_Match = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_SrcTrgtRowComp_Match)
    .load()
)

# SrcTrgtRowComp - DB2Connector (NatKey)
query_SrcTrgtRowComp_NatKey = f"""
SELECT
CLM_F.SRC_SYS_CD as CLM_F_SRC_SYS_CD,
CLM_F.CLM_ID as CLM_F_CLM_ID,
CLM_F2.SRC_SYS_CD as CLM_F2_SRC_SYS_CD,
CLM_F2.CLM_ID as CLM_F2_CLM_ID
FROM {EDWOwner}.CLM_F CLM_F
FULL OUTER JOIN {EDWOwner}.CLM_F2 CLM_F2
ON CLM_F.SRC_SYS_CD = CLM_F2.SRC_SYS_CD
AND CLM_F.CLM_ID = CLM_F2.CLM_ID
WHERE
CLM_F.LAST_UPDT_RUN_CYC_EXCTN_SK = {ExtrRunCycle}
AND CLM_F.CLM_STTUS_CD IN ('A02','A08','A09')
"""
df_SrcTrgtRowComp_NatKey = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_SrcTrgtRowComp_NatKey)
    .load()
)

# ParChldMatch - CSeqFileStage (Write)
write_files(
    df_SrcTrgtRowComp_Match.select(
        "CLM_F_SRC_SYS_CD",
        "CLM_F_CLM_ID",
        "CLM_F2_SRC_SYS_CD",
        "CLM_F2_CLM_ID"
    ),
    f"{adls_path}/balancing/sync/ClmFClmF2BalancingTotalMatch.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Transform1 - CTransformerStage
df_Transform1_Research1 = (
    df_SrcTrgtRowComp_Pkey
    .filter(col("CLM_F_CLM_SK").isNull())
    .select("CLM_F_CLM_SK","CLM_F2_CLM_SK")
)
df_Transform1_Research2 = (
    df_SrcTrgtRowComp_Pkey
    .filter(col("CLM_F2_CLM_SK").isNull())
    .select("CLM_F_CLM_SK","CLM_F2_CLM_SK")
)
df_Transform1_Notify1 = (
    df_SrcTrgtRowComp_Pkey
    .filter(col("CLM_F_CLM_SK").isNull() | col("CLM_F2_CLM_SK").isNull())
    .select("CLM_F2_CLM_SK")
)

# ResearchFile1 - CSeqFileStage (Write)
write_files(
    df_Transform1_Research1.select("CLM_F_CLM_SK","CLM_F2_CLM_SK"),
    f"{adls_path}/balancing/research/IdsEdwPkeyParChldClmFClmF2RIResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# ResearchFile2 - CSeqFileStage (Write)
write_files(
    df_Transform1_Research2.select("CLM_F_CLM_SK","CLM_F2_CLM_SK"),
    f"{adls_path}/balancing/research/IdsEdwPkeyChldParClmFClmF2RIResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Transform2 - CTransformerStage
df_Transform2_Research3 = (
    df_SrcTrgtRowComp_NatKey
    .filter(col("CLM_F_CLM_ID").isNull() | col("CLM_F_SRC_SYS_CD").isNull())
    .select("CLM_F_SRC_SYS_CD","CLM_F_CLM_ID","CLM_F2_SRC_SYS_CD","CLM_F2_CLM_ID")
)
df_Transform2_Research4 = (
    df_SrcTrgtRowComp_NatKey
    .filter(col("CLM_F2_CLM_ID").isNull() | col("CLM_F2_SRC_SYS_CD").isNull())
    .select("CLM_F_SRC_SYS_CD","CLM_F_CLM_ID","CLM_F2_SRC_SYS_CD","CLM_F2_CLM_ID")
)
df_Transform2_Notify2 = (
    df_SrcTrgtRowComp_NatKey
    .filter(
        col("CLM_F_CLM_ID").isNull() |
        col("CLM_F_SRC_SYS_CD").isNull() |
        col("CLM_F2_CLM_ID").isNull() |
        col("CLM_F2_SRC_SYS_CD").isNull()
    )
    .select("CLM_F_CLM_ID")
)

# ResearchFile3 - CSeqFileStage (Write)
write_files(
    df_Transform2_Research3.select(
        "CLM_F_SRC_SYS_CD",
        "CLM_F_CLM_ID",
        "CLM_F2_SRC_SYS_CD",
        "CLM_F2_CLM_ID"
    ),
    f"{adls_path}/balancing/research/IdsEdwNatkeyParChldClmFClmF2RIResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# ResearchFile4 - CSeqFileStage (Write)
write_files(
    df_Transform2_Research4.select(
        "CLM_F_SRC_SYS_CD",
        "CLM_F_CLM_ID",
        "CLM_F2_SRC_SYS_CD",
        "CLM_F2_CLM_ID"
    ),
    f"{adls_path}/balancing/research/IdsEdwNatkeyChldParClmFClmF2RIResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Transformer_19 - CTransformerStage
df_Transformer_19_Notify = df_Transform1_Notify1.limit(1)
df_Transformer_19_Notify = df_Transformer_19_Notify.withColumn(
    "NOTIFICATION", 
    lit("REFERENTIAL INTEGRITY BALANCING PRIMARY KEY IDS - EDW CLM F AND CLM F2 CHECK FOR OUT OF TOLERANCE")
)
df_Transformer_19_Notify = df_Transformer_19_Notify.withColumn(
    "NOTIFICATION",
    rpad(col("NOTIFICATION"), 70, " ")
)
df_Transformer_19_Notify = df_Transformer_19_Notify.select("NOTIFICATION")

# NotificationFile1 - CSeqFileStage (Write)
write_files(
    df_Transformer_19_Notify.select("NOTIFICATION"),
    f"{adls_path}/balancing/notify/ClaimsBalancingNotification.txt",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Transformer_21 - CTransformerStage
df_Transformer_21_Notify3 = df_Transform2_Notify2.limit(1)
df_Transformer_21_Notify3 = df_Transformer_21_Notify3.withColumn(
    "NOTIFICATION",
    lit("REFERENTIAL INTEGRITY BALANCING NATURAL KEYS IDS - EDW CLM F AND CLM F2 CHECK FOR OUT OF TOLERANCE")
)
df_Transformer_21_Notify3 = df_Transformer_21_Notify3.withColumn(
    "NOTIFICATION",
    rpad(col("NOTIFICATION"), 70, " ")
)
df_Transformer_21_Notify3 = df_Transformer_21_Notify3.select("NOTIFICATION")

# NotificationFile2 - CSeqFileStage (Write)
write_files(
    df_Transformer_21_Notify3.select("NOTIFICATION"),
    f"{adls_path}/balancing/notify/ClaimsBalancingNotification.txt",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)