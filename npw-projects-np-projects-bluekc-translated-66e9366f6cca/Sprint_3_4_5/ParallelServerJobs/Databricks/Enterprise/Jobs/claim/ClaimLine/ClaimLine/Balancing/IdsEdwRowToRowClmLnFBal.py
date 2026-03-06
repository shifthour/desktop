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
# MAGIC CALLED BY:          IdsEdwRowToRowClmLnFBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target tables are compared on the specified column to see if there are any discrepancies
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               08/31/2007          3264                              Originally Programmed                           devlEDW10              Steph Goddard             10/23/2007

# MAGIC Rows with Failed Comparison on Specified columns
# MAGIC Detail file for on-call to research errors
# MAGIC File checked later for rows and email to on-call
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, rpad
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


# MAGIC %run ../../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

# Parameters
EDWOwner = get_widget_value('$EDWOwner', '')
ExtrRunCycle = get_widget_value('ExtrRunCycle', '')
RunID = get_widget_value('RunID', '')
edw_secret_name = get_widget_value('edw_secret_name', '')

# Get DB config
jdbc_url_EDW, jdbc_props_EDW = get_db_config(edw_secret_name)

# Stage: SrcTrgtRowComp (DB2Connector) - Output Pin "Missing"
extract_query_missing = f"""
SELECT
  CLM_LN_F.SRC_SYS_CD,
  CLM_LN_F.CLM_ID,
  CLM_LN_F.CLM_LN_SEQ_NO,
  CLM_LN_F.CLM_LN_PROC_CD_SK,
  CLM_LN_F.CLM_LN_RVNU_CD_SK
FROM {EDWOwner}.CLM_LN_F CLM_LN_F
FULL OUTER JOIN {EDWOwner}.B_CLM_LN_F B_CLM_LN_F
  ON CLM_LN_F.SRC_SYS_CD = B_CLM_LN_F.SRC_SYS_CD
  AND CLM_LN_F.CLM_ID = B_CLM_LN_F.CLM_ID
  AND CLM_LN_F.CLM_LN_SEQ_NO = B_CLM_LN_F.CLM_LN_SEQ_NO
WHERE CLM_LN_F.LAST_UPDT_RUN_CYC_EXCTN_SK = {ExtrRunCycle}
  AND (
    CLM_LN_F.CLM_LN_PROC_CD_SK <> B_CLM_LN_F.CLM_LN_PROC_CD_SK
    OR CLM_LN_F.CLM_LN_RVNU_CD_SK <> B_CLM_LN_F.CLM_LN_RVNU_CD_SK
  )
"""
df_SrcTrgtRowComp_Missing = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_EDW)
    .options(**jdbc_props_EDW)
    .option("query", extract_query_missing)
    .load()
)

# Stage: SrcTrgtRowComp (DB2Connector) - Output Pin "Match"
extract_query_match = f"""
SELECT
  CLM_LN_F.CLM_LN_PROC_CD_SK,
  CLM_LN_F.CLM_LN_RVNU_CD_SK
FROM {EDWOwner}.CLM_LN_F CLM_LN_F
INNER JOIN {EDWOwner}.B_CLM_LN_F B_CLM_LN_F
  ON CLM_LN_F.SRC_SYS_CD = B_CLM_LN_F.SRC_SYS_CD
  AND CLM_LN_F.CLM_ID = B_CLM_LN_F.CLM_ID
  AND CLM_LN_F.CLM_LN_SEQ_NO = B_CLM_LN_F.CLM_LN_SEQ_NO
WHERE CLM_LN_F.LAST_UPDT_RUN_CYC_EXCTN_SK = {ExtrRunCycle}
  AND CLM_LN_F.CLM_LN_PROC_CD_SK = B_CLM_LN_F.CLM_LN_PROC_CD_SK
  AND CLM_LN_F.CLM_LN_RVNU_CD_SK = B_CLM_LN_F.CLM_LN_RVNU_CD_SK
"""
df_SrcTrgtRowComp_Match = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_EDW)
    .options(**jdbc_props_EDW)
    .option("query", extract_query_match)
    .load()
)

# Stage: SrcTrgtSyncFile (CSeqFileStage) - Input "Match"
df_SrcTrgtSyncFile = df_SrcTrgtRowComp_Match.select("CLM_LN_PROC_CD_SK", "CLM_LN_RVNU_CD_SK")
write_files(
    df_SrcTrgtSyncFile,
    f"{adls_path}/balancing/sync/ClmLnFBalancingTotalMatch.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Stage: TransformLogic (CTransformerStage) - Input "Missing"
# Output pin "Research"
df_TransformLogic_Research = df_SrcTrgtRowComp_Missing.select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("CLM_ID").alias("CLM_ID"),
    col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    col("CLM_LN_PROC_CD_SK").alias("CLM_LN_PROC_CD_SK"),
    col("CLM_LN_RVNU_CD_SK").alias("CLM_LN_RVNU_CD_SK")
)

# Output pin "Notify" (constraint @INROWNUM=1)
df_TransformLogic_Notify = df_SrcTrgtRowComp_Missing.limit(1).select(
    lit("ROW TO ROW BALANCING IDS - EDW CLM LN F OUT OF TOLERANCE").alias("NOTIFICATION")
)
df_TransformLogic_Notify = df_TransformLogic_Notify.withColumn("NOTIFICATION", rpad("NOTIFICATION", 70, " "))

# Stage: ErrorNotificationFile (CSeqFileStage) - Input "Notify"
write_files(
    df_TransformLogic_Notify.select("NOTIFICATION"),
    f"{adls_path}/balancing/notify/ClaimsBalancingNotification.txt",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Stage: ResearchFile (CSeqFileStage) - Input "Research"
df_ResearchFile = df_TransformLogic_Research.select(
    "SRC_SYS_CD",
    "CLM_ID",
    "CLM_LN_SEQ_NO",
    "CLM_LN_PROC_CD_SK",
    "CLM_LN_RVNU_CD_SK"
)
write_files(
    df_ResearchFile,
    f"{adls_path}/balancing/research/IdsEdwClmLnFRowToRowResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)