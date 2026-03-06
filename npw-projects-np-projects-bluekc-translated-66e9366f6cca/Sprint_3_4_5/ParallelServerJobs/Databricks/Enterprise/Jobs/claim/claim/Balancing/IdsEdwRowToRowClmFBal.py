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
# MAGIC CALLED BY:          IdsEdwRowToRowClmFBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target tables are compared on the specified column to see if there are any discrepancies
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               08/31/2007          3264                              Originally Programmed                           devlEDW10              Steph Goddard            10/23/2007

# MAGIC Rows with Failed Comparison on Specified columns
# MAGIC Detail file for on-call to research errors
# MAGIC File checked later for rows and email to on-call
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import rpad, lit
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
RunID = get_widget_value('RunID','')

jdbc_url, jdbc_props = get_db_config(edw_secret_name)

missing_sql = f"""SELECT
CLM_F.SRC_SYS_CD,
CLM_F.CLM_ID,
CLM_F.MBR_SK,
CLM_F.EXPRNC_CAT_CD,
CLM_F.FNCL_LOB_CD,
CLM_F.CLM_SVC_STRT_DT_SK,
CLM_F.CLM_SVC_STRT_YR_MO_SK,
CLM_F.GRP_ID
FROM {EDWOwner}.CLM_F CLM_F
FULL OUTER JOIN {EDWOwner}.B_CLM_F B_CLM_F
ON CLM_F.SRC_SYS_CD = B_CLM_F.SRC_SYS_CD
AND CLM_F.CLM_ID = B_CLM_F.CLM_ID
WHERE CLM_F.LAST_UPDT_RUN_CYC_EXCTN_SK = {ExtrRunCycle}
AND
(
  CLM_F.MBR_SK <> B_CLM_F.MBR_SK
  OR CLM_F.EXPRNC_CAT_CD <> B_CLM_F.EXPRNC_CAT_CD
  OR CLM_F.FNCL_LOB_CD <> B_CLM_F.FNCL_LOB_CD
  OR CLM_F.CLM_SVC_STRT_DT_SK <> B_CLM_F.CLM_SVC_STRT_DT_SK
  OR CLM_F.CLM_SVC_STRT_YR_MO_SK <> B_CLM_F.CLM_SVC_STRT_YR_MO_SK
  OR CLM_F.GRP_ID <> B_CLM_F.GRP_ID
)
"""

df_SrcTrgtRowComp_Missing = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", missing_sql)
    .load()
)

match_sql = f"""SELECT
CLM_F.MBR_SK,
CLM_F.EXPRNC_CAT_CD,
CLM_F.FNCL_LOB_CD,
CLM_F.CLM_SVC_STRT_DT_SK,
CLM_F.CLM_SVC_STRT_YR_MO_SK,
CLM_F.GRP_ID
FROM {EDWOwner}.CLM_F CLM_F
INNER JOIN {EDWOwner}.B_CLM_F B_CLM_F
ON CLM_F.SRC_SYS_CD = B_CLM_F.SRC_SYS_CD
AND CLM_F.CLM_ID = B_CLM_F.CLM_ID
WHERE CLM_F.LAST_UPDT_RUN_CYC_EXCTN_SK = {ExtrRunCycle}
AND CLM_F.MBR_SK = B_CLM_F.MBR_SK
AND CLM_F.EXPRNC_CAT_CD = B_CLM_F.EXPRNC_CAT_CD
AND CLM_F.FNCL_LOB_CD = B_CLM_F.FNCL_LOB_CD
AND CLM_F.CLM_SVC_STRT_DT_SK = B_CLM_F.CLM_SVC_STRT_DT_SK
AND CLM_F.CLM_SVC_STRT_YR_MO_SK = B_CLM_F.CLM_SVC_STRT_YR_MO_SK
AND CLM_F.GRP_ID = B_CLM_F.GRP_ID
"""

df_SrcTrgtRowComp_Match = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", match_sql)
    .load()
)

write_files(
    df_SrcTrgtRowComp_Match.select(
        "MBR_SK",
        "EXPRNC_CAT_CD",
        "FNCL_LOB_CD",
        "CLM_SVC_STRT_DT_SK",
        "CLM_SVC_STRT_YR_MO_SK",
        "GRP_ID"
    ),
    f"{adls_path}/balancing/sync/ClmFBalancingTotalMatch.dat",
    ',',
    'overwrite',
    False,
    False,
    '"',
    None
)

df_Research = df_SrcTrgtRowComp_Missing.select(
    df_SrcTrgtRowComp_Missing["SRC_SYS_CD"].alias("SRC_SYS_CD"),
    df_SrcTrgtRowComp_Missing["CLM_ID"].alias("CLM_ID"),
    df_SrcTrgtRowComp_Missing["MBR_SK"].alias("MBR_SK"),
    df_SrcTrgtRowComp_Missing["EXPRNC_CAT_CD"].alias("EXPRNC_CAT_CD"),
    df_SrcTrgtRowComp_Missing["FNCL_LOB_CD"].alias("FNCL_LOB_CD"),
    rpad(df_SrcTrgtRowComp_Missing["CLM_SVC_STRT_DT_SK"], 10, " ").alias("CLM_SVC_STRT_DT_SK"),
    rpad(df_SrcTrgtRowComp_Missing["CLM_SVC_STRT_YR_MO_SK"], 6, " ").alias("CLM_SVC_STRT_YR_MO_SK"),
    df_SrcTrgtRowComp_Missing["GRP_ID"].alias("GRP_ID")
)

df_Notify = df_SrcTrgtRowComp_Missing.limit(1).select(
    rpad(lit("ROW TO ROW BALANCING IDS - EDW CLM F OUT OF TOLERANCE"), 70, " ").alias("NOTIFICATION")
)

write_files(
    df_Notify.select("NOTIFICATION"),
    f"{adls_path}/balancing/notify/ClaimsBalancingNotification.txt",
    ',',
    'append',
    False,
    False,
    '"',
    None
)

write_files(
    df_Research.select(
        "SRC_SYS_CD",
        "CLM_ID",
        "MBR_SK",
        "EXPRNC_CAT_CD",
        "FNCL_LOB_CD",
        "CLM_SVC_STRT_DT_SK",
        "CLM_SVC_STRT_YR_MO_SK",
        "GRP_ID"
    ),
    f"{adls_path}/balancing/research/IdsEdwClmFRowToRowResearch.dat.{RunID}",
    ',',
    'overwrite',
    False,
    False,
    '"',
    None
)