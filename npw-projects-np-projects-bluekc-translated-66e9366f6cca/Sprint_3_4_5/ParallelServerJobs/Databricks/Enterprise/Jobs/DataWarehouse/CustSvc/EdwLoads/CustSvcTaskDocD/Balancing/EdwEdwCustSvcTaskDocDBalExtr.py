# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               07/02/2007          3264                              Originally Programmed                           devlEDW10              Steph Goddard             10/21/2007          
# MAGIC 
# MAGIC Rama Kamjula                  01/02/2013           5114                          Rewritten to Parallel version                  EnterpriseWrhsDevl      Jag Yelavarthi               2014-02-25

# MAGIC Rows with Failed Comparison on Specified columns
# MAGIC Copy stage used to remove duplicates and create one record for the Notification file.
# MAGIC IDS - EDW Customer Service Task Document Dim Row To Row Comparisons
# MAGIC File checked later for rows and email to on-call
# MAGIC Detail file for on-call to research errors
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../../../../Utility_Enterprise
# COMMAND ----------


ExtrRunCycle = get_widget_value("ExtrRunCycle", "")
EDWOwner = get_widget_value("EDWOwner", "")
edw_secret_name = get_widget_value("edw_secret_name", "")
RunID = get_widget_value("RunID", "")

jdbc_url, jdbc_props = get_db_config(edw_secret_name)

extract_query_db2_SrcTrgtRowComp = f"""
SELECT 
CustSvcTaskDocD.SRC_SYS_CD,
CustSvcTaskDocD.CUST_SVC_ID,
CustSvcTaskDocD.CUST_SVC_TASK_SEQ_NO,
CustSvcTaskDocD.CUST_SVC_TASK_DOC_SEQ_NO,
CustSvcTaskDocD.CUST_SVC_TASK_CSTM_DTL_CD,
CustSvcTaskDocD.CUST_SVC_TASK_CSTM_DTL_UNIQ_ID,
CustSvcTaskDocD.DOC_ID
FROM 
{EDWOwner}.CUST_SVC_TASK_DOC_D CustSvcTaskDocD
FULL OUTER JOIN {EDWOwner}.B_CUST_SVC_TASK_DOC_D BCustSvcTaskDocD
ON CustSvcTaskDocD.SRC_SYS_CD = BCustSvcTaskDocD.SRC_SYS_CD
AND CustSvcTaskDocD.CUST_SVC_ID = BCustSvcTaskDocD.CUST_SVC_ID
AND CustSvcTaskDocD.CUST_SVC_TASK_SEQ_NO = BCustSvcTaskDocD.CUST_SVC_TASK_SEQ_NO
AND CustSvcTaskDocD.CUST_SVC_TASK_DOC_SEQ_NO = BCustSvcTaskDocD.CUST_SVC_TASK_DOC_SEQ_NO
AND CustSvcTaskDocD.CUST_SVC_TASK_CSTM_DTL_CD = BCustSvcTaskDocD.CUST_SVC_TASK_CSTM_DTL_CD
AND CustSvcTaskDocD.CUST_SVC_TASK_CSTM_DTL_UNIQ_ID = BCustSvcTaskDocD.CUST_SVC_TASK_CSTM_DTL_UNIQ_ID
WHERE 
CustSvcTaskDocD.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
AND CustSvcTaskDocD.DOC_ID <> BCustSvcTaskDocD.DOC_ID
"""

df_db2_SrcTrgtRowComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_SrcTrgtRowComp)
    .load()
)

df_xfm_BusinessLogic_lnk_Research = df_db2_SrcTrgtRowComp.select(
    "SRC_SYS_CD",
    "CUST_SVC_ID",
    "CUST_SVC_TASK_SEQ_NO",
    "CUST_SVC_TASK_DOC_SEQ_NO",
    "CUST_SVC_TASK_CSTM_DTL_CD",
    "CUST_SVC_TASK_CSTM_DTL_UNIQ_ID",
    "DOC_ID"
)

df_xfm_BusinessLogic_lnk_Xfm_Out = (
    df_db2_SrcTrgtRowComp.limit(1)
    .select(
        lit("ROW TO ROW BALANCING IDS - EDW CUST SVC TASK DOC D OUT OF TOLERANCE").alias("NOTIFICATION")
    )
)

df_seq_ResearchFile = df_xfm_BusinessLogic_lnk_Research.select(
    "SRC_SYS_CD",
    "CUST_SVC_ID",
    "CUST_SVC_TASK_SEQ_NO",
    "CUST_SVC_TASK_DOC_SEQ_NO",
    "CUST_SVC_TASK_CSTM_DTL_CD",
    "CUST_SVC_TASK_CSTM_DTL_UNIQ_ID",
    "DOC_ID"
)

write_files(
    df_seq_ResearchFile,
    f"{adls_path}/balancing/research/IdsEdwCustSvcTaskDocDRowToRowResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

df_cpy_DDup_out = (
    df_xfm_BusinessLogic_lnk_Xfm_Out
    .sort(col("NOTIFICATION").asc())
    .select(
        rpad(col("NOTIFICATION"), 70, " ").alias("NOTIFICATION")
    )
)

df_seq_ErrorNotificationFile = df_cpy_DDup_out.select("NOTIFICATION")

write_files(
    df_seq_ErrorNotificationFile,
    f"{adls_path}/balancing/notify/CustSvcBalancingNotification.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='^',
    nullValue=None
)

extract_query_db2_SrcTrgtRowComp_Match = f"""
SELECT
CustSvcTaskDocD.DOC_ID
FROM
{EDWOwner}.CUST_SVC_TASK_DOC_D CustSvcTaskDocD
INNER JOIN {EDWOwner}.B_CUST_SVC_TASK_DOC_D BCustSvcTaskDocD
ON CustSvcTaskDocD.SRC_SYS_CD = BCustSvcTaskDocD.SRC_SYS_CD
AND CustSvcTaskDocD.CUST_SVC_ID = BCustSvcTaskDocD.CUST_SVC_ID
AND CustSvcTaskDocD.CUST_SVC_TASK_SEQ_NO = BCustSvcTaskDocD.CUST_SVC_TASK_SEQ_NO
AND CustSvcTaskDocD.CUST_SVC_TASK_DOC_SEQ_NO = BCustSvcTaskDocD.CUST_SVC_TASK_DOC_SEQ_NO
AND CustSvcTaskDocD.CUST_SVC_TASK_CSTM_DTL_CD = BCustSvcTaskDocD.CUST_SVC_TASK_CSTM_DTL_CD
AND CustSvcTaskDocD.CUST_SVC_TASK_CSTM_DTL_UNIQ_ID = BCustSvcTaskDocD.CUST_SVC_TASK_CSTM_DTL_UNIQ_ID
WHERE
CustSvcTaskDocD.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
AND CustSvcTaskDocD.DOC_ID = BCustSvcTaskDocD.DOC_ID
"""

df_db2_SrcTrgtRowComp_Match = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_SrcTrgtRowComp_Match)
    .load()
)

df_seq_SrcTrgtSyncFile = df_db2_SrcTrgtRowComp_Match.select("DOC_ID")

write_files(
    df_seq_SrcTrgtSyncFile,
    f"{adls_path}/balancing/sync/CustSvcTaskDocDBalancingTotalMatch.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)