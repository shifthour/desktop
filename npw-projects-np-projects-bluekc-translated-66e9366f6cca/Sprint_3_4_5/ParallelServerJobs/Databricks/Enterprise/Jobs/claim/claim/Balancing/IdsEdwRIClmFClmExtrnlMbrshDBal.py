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
# MAGIC CALLED BY:          IdsEdwRIClmFClmExtrnlMbrshDBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target tables are compared on the specified column to see if there are any discrepancies
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               09/21/2007          3264                              Originally Programmed                           devlEDW10               Steph Goddard           10/22/2007

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
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


# Retrieve parameter values
EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
RunID = get_widget_value('RunID','')

# Database connection for EDW
jdbc_url, jdbc_props = get_db_config(edw_secret_name)

# --------------------------------------------------------------------------------
# Stage: SrcTrgtRowComp (DB2Connector) - Three output links
# --------------------------------------------------------------------------------

# 1) PrmKey link
query_Prkey = f"""
SELECT
CLM_F2.CLM_SK AS CLM_F_CLM_SK,
CLM_EXTRNL_MBRSH_D.CLM_EXTRNL_MBRSH_SK AS CLM_EXTRNL_MBRSH_D_CLM_EXTRNL_MBRSH_SK
FROM {EDWOwner}.CLM_F2 CLM_F2
FULL OUTER JOIN {EDWOwner}.CLM_EXTRNL_MBRSH_D CLM_EXTRNL_MBRSH_D
  ON CLM_F2.CLM_EXTRNL_MBRSH_SK = CLM_EXTRNL_MBRSH_D.CLM_EXTRNL_MBRSH_SK,
{EDWOwner}.CLM_F CLM_F
WHERE
CLM_F.LAST_UPDT_RUN_CYC_EXCTN_SK = {ExtrRunCycle}
AND CLM_F.CLM_HOST_IN = 'N'
AND CLM_F.CLM_ITS_IN = 'Y'
AND CLM_F.CLM_STTUS_CD IN ('A02','A08','A09')
AND CLM_F.CLM_SK = CLM_F2.CLM_SK
"""

df_SrcTrgtRowComp_Prkey = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_Prkey)
    .load()
)

# 2) Match link
query_Match = f"""
SELECT
CLM_F.SRC_SYS_CD AS CLM_F_SRC_SYS_CD,
CLM_F.CLM_ID AS CLM_F_CLM_ID,
CLM_EXTRNL_MBRSH_D.SRC_SYS_CD AS CLM_EXTRNL_MBRSH_D_SRC_SYS_CD,
CLM_EXTRNL_MBRSH_D.CLM_ID AS CLM_EXTRNL_MBRSH_D_CLM_ID
FROM {EDWOwner}.CLM_F CLM_F
INNER JOIN {EDWOwner}.CLM_EXTRNL_MBRSH_D CLM_EXTRNL_MBRSH_D
  ON CLM_F.SRC_SYS_CD = CLM_EXTRNL_MBRSH_D.SRC_SYS_CD
  AND CLM_F.CLM_ID = CLM_EXTRNL_MBRSH_D.CLM_ID
WHERE
CLM_F.LAST_UPDT_RUN_CYC_EXCTN_SK = {ExtrRunCycle}
AND CLM_F.CLM_HOST_IN = 'N'
AND CLM_F.CLM_ITS_IN = 'Y'
AND CLM_F.CLM_STTUS_CD IN ('A02','A08','A09')
"""

df_SrcTrgtRowComp_Match = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_Match)
    .load()
)

# 3) nKey link
query_nKey = f"""
SELECT
CLM_F.SRC_SYS_CD AS CLM_F_SRC_SYS_CD,
CLM_F.CLM_ID AS CLM_F_CLM_ID,
CLM_EXTRNL_MBRSH_D.SRC_SYS_CD AS CLM_EXTRNL_MBRSH_D_SRC_SYS_CD,
CLM_EXTRNL_MBRSH_D.CLM_ID AS CLM_EXTRNL_MBRSH_D_CLM_ID
FROM {EDWOwner}.CLM_F CLM_F
FULL OUTER JOIN {EDWOwner}.CLM_EXTRNL_MBRSH_D CLM_EXTRNL_MBRSH_D
  ON CLM_F.SRC_SYS_CD = CLM_EXTRNL_MBRSH_D.SRC_SYS_CD
  AND CLM_F.CLM_ID = CLM_EXTRNL_MBRSH_D.CLM_ID
WHERE
CLM_F.LAST_UPDT_RUN_CYC_EXCTN_SK = {ExtrRunCycle}
AND CLM_F.CLM_HOST_IN = 'N'
AND CLM_F.CLM_ITS_IN = 'Y'
AND CLM_F.CLM_STTUS_CD IN ('A02','A08','A09')
"""

df_SrcTrgtRowComp_nKey = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_nKey)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: ParChldMatch (CSeqFileStage) - Write from "Match" link
# --------------------------------------------------------------------------------

df_ParChldMatch_out = df_SrcTrgtRowComp_Match.select(
    "CLM_F_SRC_SYS_CD",
    "CLM_F_CLM_ID",
    "CLM_EXTRNL_MBRSH_D_SRC_SYS_CD",
    "CLM_EXTRNL_MBRSH_D_CLM_ID"
)

write_files(
    df_ParChldMatch_out,
    f"{adls_path}/balancing/sync/ClmFClmExtrnlMbrshDBalancingTotalMatch.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

# --------------------------------------------------------------------------------
# Stage: Transformer_19 -> Output "Pkey"
# --------------------------------------------------------------------------------

df_Transformer_19_Pkey = df_SrcTrgtRowComp_Prkey.filter(
    (F.col("CLM_F_CLM_SK").isNull()) | (F.col("CLM_EXTRNL_MBRSH_D_CLM_EXTRNL_MBRSH_SK").isNull())
)

# --------------------------------------------------------------------------------
# Stage: Transform1 (CTransformerStage) - Input from "Pkey"
# --------------------------------------------------------------------------------

df_Transform1_base = df_Transformer_19_Pkey

# Research1 link
df_Transform1_Research1 = df_Transform1_base.filter(F.col("CLM_F_CLM_SK").isNull()).select(
    "CLM_F_CLM_SK",
    "CLM_EXTRNL_MBRSH_D_CLM_EXTRNL_MBRSH_SK"
)

# Research2 link
df_Transform1_Research2 = df_Transform1_base.filter(F.col("CLM_EXTRNL_MBRSH_D_CLM_EXTRNL_MBRSH_SK").isNull()).select(
    "CLM_F_CLM_SK",
    "CLM_EXTRNL_MBRSH_D_CLM_EXTRNL_MBRSH_SK"
)

# Notify link (@INROWNUM = 1)
w_transform1 = Window.orderBy(F.lit(1))
df_Transform1_withRow = df_Transform1_base.withColumn("rownum", F.row_number().over(w_transform1))
df_Transform1_Notify = df_Transform1_withRow.filter(F.col("rownum") == 1).drop("rownum")
df_Transform1_Notify = df_Transform1_Notify.withColumn(
    "NOTIFICATION",
    F.lit("REFERENTIAL INTEGRITY BALANCING PRIMARY KEY IDS - EDW CLM F AND CLM EXTRNL MBRSH D CHECK FOR OUT OF TOLERANCE")
)
df_Transform1_Notify = df_Transform1_Notify.withColumn(
    "NOTIFICATION",
    F.rpad(F.col("NOTIFICATION"), 70, " ")
).select("NOTIFICATION")

# --------------------------------------------------------------------------------
# Stage: NotificationFile1 (CSeqFileStage) - Append
# --------------------------------------------------------------------------------

write_files(
    df_Transform1_Notify,
    f"{adls_path}/balancing/notify/ClaimsBalancingNotification.txt",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

# --------------------------------------------------------------------------------
# Stage: ResearchFile1 (CSeqFileStage) - Overwrite
# --------------------------------------------------------------------------------

write_files(
    df_Transform1_Research1,
    f"{adls_path}/balancing/research/IdsEdwPkeyParChldClmFClmExtrnlMbrshDRIResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

# --------------------------------------------------------------------------------
# Stage: ResearchFile2 (CSeqFileStage) - Overwrite
# --------------------------------------------------------------------------------

write_files(
    df_Transform1_Research2,
    f"{adls_path}/balancing/research/IdsEdwPkeyChldParClmFClmExtrnlMbrshDRIResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

# --------------------------------------------------------------------------------
# Stage: Transformer_20 -> Output "NatKey"
# --------------------------------------------------------------------------------

df_Transformer_20_NatKey = df_SrcTrgtRowComp_nKey.filter(
    F.col("CLM_F_CLM_ID").isNull() |
    F.col("CLM_F_SRC_SYS_CD").isNull() |
    F.col("CLM_EXTRNL_MBRSH_D_CLM_ID").isNull() |
    F.col("CLM_EXTRNL_MBRSH_D_SRC_SYS_CD").isNull()
).select(
    "CLM_F_SRC_SYS_CD",
    "CLM_F_CLM_ID",
    "CLM_EXTRNL_MBRSH_D_SRC_SYS_CD",
    "CLM_EXTRNL_MBRSH_D_CLM_ID"
)

# --------------------------------------------------------------------------------
# Stage: Transform2 (CTransformerStage) - Input from "NatKey"
# --------------------------------------------------------------------------------

df_Transform2_base = df_Transformer_20_NatKey

# Research3 link
df_Transform2_Research3 = df_Transform2_base.filter(
    F.col("CLM_F_CLM_ID").isNull() | F.col("CLM_F_SRC_SYS_CD").isNull()
).select(
    "CLM_F_SRC_SYS_CD",
    "CLM_F_CLM_ID",
    "CLM_EXTRNL_MBRSH_D_SRC_SYS_CD",
    "CLM_EXTRNL_MBRSH_D_CLM_ID"
)

# Research4 link
df_Transform2_Research4 = df_Transform2_base.filter(
    F.col("CLM_EXTRNL_MBRSH_D_CLM_ID").isNull() | F.col("CLM_EXTRNL_MBRSH_D_SRC_SYS_CD").isNull()
).select(
    "CLM_F_SRC_SYS_CD",
    "CLM_F_CLM_ID",
    "CLM_EXTRNL_MBRSH_D_SRC_SYS_CD",
    "CLM_EXTRNL_MBRSH_D_CLM_ID"
)

# Notify link (@INROWNUM = 1)
w_transform2 = Window.orderBy(F.lit(1))
df_Transform2_withRow = df_Transform2_base.withColumn("rownum", F.row_number().over(w_transform2))
df_Transform2_Notify = df_Transform2_withRow.filter(F.col("rownum") == 1).drop("rownum")
df_Transform2_Notify = df_Transform2_Notify.withColumn(
    "NOTIFICATION",
    F.lit("REFERENTIAL INTEGRITY BALANCING NATURAL KEYS IDS - EDW CLM F AND CLM EXTRNL MBRSH D CHECK FOR OUT OF TOLERANCE")
)
df_Transform2_Notify = df_Transform2_Notify.withColumn(
    "NOTIFICATION",
    F.rpad(F.col("NOTIFICATION"), 70, " ")
).select("NOTIFICATION")

# --------------------------------------------------------------------------------
# Stage: ResearchFile3 (CSeqFileStage) - Overwrite
# --------------------------------------------------------------------------------

write_files(
    df_Transform2_Research3,
    f"{adls_path}/balancing/research/IdsEdwNatkeyParChldClmFClmExtrnlMbrshDRIResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

# --------------------------------------------------------------------------------
# Stage: ResearchFile4 (CSeqFileStage) - Overwrite
# --------------------------------------------------------------------------------

write_files(
    df_Transform2_Research4,
    f"{adls_path}/balancing/research/IdsEdwNatkeyChldParClmFClmExtrnlMbrshDRIResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

# --------------------------------------------------------------------------------
# Stage: NotificationFile2 (CSeqFileStage) - Append
# --------------------------------------------------------------------------------

write_files(
    df_Transform2_Notify,
    f"{adls_path}/balancing/notify/ClaimsBalancingNotification.txt",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)