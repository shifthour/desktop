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
# MAGIC ^1_1 10/19/07 14:39:28 Batch  14537_52774 INIT bckcett devlEDW10 u150208 Parik
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:          IdsEdwRIClmFClmLnFBalSeq
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
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
RunID = get_widget_value('RunID','')

jdbc_url, jdbc_props = get_db_config(edw_secret_name)

# Read from DB2Connector stage "SrcTrgtRowComp"
# Output pin "prmKey"
query_SrcTrgtRowComp_prmKey = f"""
SELECT
CLM_F.CLM_SK AS CLM_F_CLM_SK,
CLM_LN_F.CLM_SK AS CLM_LN_F_CLM_SK
FROM {EDWOwner}.CLM_F CLM_F
FULL OUTER JOIN {EDWOwner}.CLM_LN_F CLM_LN_F
ON CLM_F.CLM_SK = CLM_LN_F.CLM_SK
WHERE
CLM_F.LAST_UPDT_RUN_CYC_EXCTN_SK = {ExtrRunCycle}
AND CLM_F.CLM_STTUS_CD IN ('A02','A08','A09')
"""
df_SrcTrgtRowComp_prmKey = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_SrcTrgtRowComp_prmKey.strip())
    .load()
)

# Read from DB2Connector stage "SrcTrgtRowComp"
# Output pin "Match"
query_SrcTrgtRowComp_match = f"""
SELECT
CLM_F.SRC_SYS_CD AS CLM_F_SRC_SYS_CD,
CLM_F.CLM_ID AS CLM_F_CLM_ID,
CLM_LN_F.SRC_SYS_CD AS CLM_LN_F_SRC_SYS_CD,
CLM_LN_F.CLM_ID AS CLM_LN_F_CLM_ID
FROM {EDWOwner}.CLM_F CLM_F
INNER JOIN {EDWOwner}.CLM_LN_F CLM_LN_F
ON CLM_F.SRC_SYS_CD = CLM_LN_F.SRC_SYS_CD 
AND CLM_F.CLM_ID = CLM_LN_F.CLM_ID
WHERE
CLM_F.LAST_UPDT_RUN_CYC_EXCTN_SK = {ExtrRunCycle}
AND CLM_F.CLM_STTUS_CD IN ('A02','A08','A09')
"""
df_SrcTrgtRowComp_match = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_SrcTrgtRowComp_match.strip())
    .load()
)

# Read from DB2Connector stage "SrcTrgtRowComp"
# Output pin "nKey"
query_SrcTrgtRowComp_nKey = f"""
SELECT
CLM_F.SRC_SYS_CD AS CLM_F_SRC_SYS_CD,
CLM_F.CLM_ID AS CLM_F_CLM_ID,
CLM_LN_F.SRC_SYS_CD AS CLM_LN_F_SRC_SYS_CD,
CLM_LN_F.CLM_ID AS CLM_LN_F_CLM_ID
FROM {EDWOwner}.CLM_F CLM_F
FULL OUTER JOIN {EDWOwner}.CLM_LN_F CLM_LN_F
ON CLM_F.SRC_SYS_CD = CLM_LN_F.SRC_SYS_CD
AND CLM_F.CLM_ID = CLM_LN_F.CLM_ID
WHERE
CLM_F.LAST_UPDT_RUN_CYC_EXCTN_SK = {ExtrRunCycle}
AND CLM_F.CLM_STTUS_CD IN ('A02','A08','A09')
"""
df_SrcTrgtRowComp_nKey = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_SrcTrgtRowComp_nKey.strip())
    .load()
)

# Stage "ParChldMatch" writes df_SrcTrgtRowComp_match to file
df_ParChldMatch = df_SrcTrgtRowComp_match.select(
    "CLM_F_SRC_SYS_CD",
    "CLM_F_CLM_ID",
    "CLM_LN_F_SRC_SYS_CD",
    "CLM_LN_F_CLM_ID"
)
write_files(
    df_ParChldMatch,
    f"{adls_path}/balancing/sync/ClmFClmLnFBalancingTotalMatch.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Stage "Transformer_19" => Output pin "Pkey"
df_Transformer_19_Pkey = df_SrcTrgtRowComp_prmKey.filter(
    (F.col("CLM_F_CLM_SK").isNull()) | (F.col("CLM_LN_F_CLM_SK").isNull())
).select(
    F.col("CLM_F_CLM_SK").alias("CLM_F_CLM_SK"),
    F.col("CLM_LN_F_CLM_SK").alias("CLM_LN_F_CLM_SK")
)

# Stage "Transform1" => Input is df_Transformer_19_Pkey
# Output pin "Research1" => constraint: ISNULL(Pkey.CLM_F_CLM_SK) = @TRUE
df_Transform1_Research1 = df_Transformer_19_Pkey.filter(
    F.col("CLM_F_CLM_SK").isNull()
).select(
    "CLM_F_CLM_SK",
    "CLM_LN_F_CLM_SK"
)

# Output pin "Research2" => constraint: ISNULL(Pkey.CLM_LN_F_CLM_SK) = @TRUE
df_Transform1_Research2 = df_Transformer_19_Pkey.filter(
    F.col("CLM_LN_F_CLM_SK").isNull()
).select(
    "CLM_F_CLM_SK",
    "CLM_LN_F_CLM_SK"
)

# Output pin "Notify" => constraint: @INROWNUM = 1
df_Transform1_Notify = df_Transformer_19_Pkey.limit(1).select(
    F.rpad(
        F.lit("REFERENTIAL INTEGRITY BALANCING PRIMARY KEY IDS - EDW CLM F AND CLM LN F CHECK FOR OUT OF TOLERANCE"),
        70,
        " "
    ).alias("NOTIFICATION")
)

# Stage "NotificationFile1"
write_files(
    df_Transform1_Notify,
    f"{adls_path}/balancing/notify/ClaimsBalancingNotification.txt",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Stage "ResearchFile1"
write_files(
    df_Transform1_Research1,
    f"{adls_path}/balancing/research/IdsEdwPkeyParChldClmFClmLnFRIResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Stage "ResearchFile2"
write_files(
    df_Transform1_Research2,
    f"{adls_path}/balancing/research/IdsEdwPkeyChldParClmFClmLnFRIResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Stage "Transformer_20" => Output pin "NatKey"
df_Transformer_20_NatKey = df_SrcTrgtRowComp_nKey.filter(
    F.col("CLM_F_CLM_ID").isNull() |
    F.col("CLM_F_SRC_SYS_CD").isNull() |
    F.col("CLM_LN_F_CLM_ID").isNull() |
    F.col("CLM_LN_F_SRC_SYS_CD").isNull()
).select(
    F.col("CLM_F_SRC_SYS_CD").alias("CLM_F_SRC_SYS_CD"),
    F.col("CLM_F_CLM_ID").alias("CLM_F_CLM_ID"),
    F.col("CLM_LN_F_SRC_SYS_CD").alias("CLM_LN_F_SRC_SYS_CD"),
    F.col("CLM_LN_F_CLM_ID").alias("CLM_LN_F_CLM_ID")
)

# Stage "Transform2" => Input is df_Transformer_20_NatKey
# Output pin "Research3"
df_Transform2_Research3 = df_Transformer_20_NatKey.filter(
    F.col("CLM_F_CLM_ID").isNull() | F.col("CLM_F_SRC_SYS_CD").isNull()
).select(
    "CLM_F_SRC_SYS_CD",
    "CLM_F_CLM_ID",
    "CLM_LN_F_SRC_SYS_CD",
    "CLM_LN_F_CLM_ID"
)

# Output pin "Research4"
df_Transform2_Research4 = df_Transformer_20_NatKey.filter(
    F.col("CLM_LN_F_CLM_ID").isNull() | F.col("CLM_LN_F_SRC_SYS_CD").isNull()
).select(
    "CLM_F_SRC_SYS_CD",
    "CLM_F_CLM_ID",
    "CLM_LN_F_SRC_SYS_CD",
    "CLM_LN_F_CLM_ID"
)

# Output pin "Notify"
df_Transform2_Notify = df_Transformer_20_NatKey.limit(1).select(
    F.rpad(
        F.lit("REFERENTIAL INTEGRITY BALANCING NATURAL KEYS IDS - EDW CLM F AND CLM LN F CHECK FOR OUT OF TOLERANCE"),
        70,
        " "
    ).alias("NOTIFICATION")
)

# Stage "ResearchFile3"
write_files(
    df_Transform2_Research3,
    f"{adls_path}/balancing/research/IdsEdwNatkeyParChldClmFClmLnFRIResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Stage "ResearchFile4"
write_files(
    df_Transform2_Research4,
    f"{adls_path}/balancing/research/IdsEdwNatkeyChldParClmFClmLnFRIResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Stage "NotificationFile2"
write_files(
    df_Transform2_Notify,
    f"{adls_path}/balancing/notify/ClaimsBalancingNotification.txt",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)