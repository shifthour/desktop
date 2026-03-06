# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               07/02/2007          3264                              Originally Programmed                           devlEDW10              Steph Goddard            10/21/2007             
# MAGIC 
# MAGIC Rama Kamjula                  01/02/2013           5114                          Rewritten to Parallel version                  EnterpriseWrhsDevl

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
from pyspark.sql.functions import col, lit, rpad, row_number
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../../../Utility_Enterprise
# COMMAND ----------


ExtrRunCycle = get_widget_value('ExtrRunCycle','')
EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
RunID = get_widget_value('RunID','')

jdbc_url, jdbc_props = get_db_config(edw_secret_name)

db2_SrcTrgtRowComp_query = f"""SELECT
CustSvcTaskBckdtD.SRC_SYS_CD,
CustSvcTaskBckdtD.CUST_SVC_ID,
CustSvcTaskBckdtD.CUST_SVC_TASK_SEQ_NO,
CustSvcTaskBckdtD.CUST_SVC_TASK_BCKDT_SEQ_NO,
CustSvcTaskBckdtD.CUST_SVC_TASK_CSTM_DTL_CD,
CustSvcTaskBckdtD.CUST_SVC_TASK_CSTM_DTL_UNIQ_ID,
CAST(CustSvcTaskBckdtD.BCKDT_DT AS CHAR(10)) as BCKDT_DT
FROM {EDWOwner}.CUST_SVC_TASK_BCKDT_D CustSvcTaskBckdtD
FULL OUTER JOIN {EDWOwner}.B_CUST_SVC_TASK_BCKDT_D BCustSvcTaskBckdtD
ON CustSvcTaskBckdtD.SRC_SYS_CD = BCustSvcTaskBckdtD.SRC_SYS_CD
AND CustSvcTaskBckdtD.CUST_SVC_ID = BCustSvcTaskBckdtD.CUST_SVC_ID
AND CustSvcTaskBckdtD.CUST_SVC_TASK_SEQ_NO = BCustSvcTaskBckdtD.CUST_SVC_TASK_SEQ_NO
AND CustSvcTaskBckdtD.CUST_SVC_TASK_BCKDT_SEQ_NO = BCustSvcTaskBckdtD.CUST_SVC_TASK_BCKDT_SEQ_NO
AND CustSvcTaskBckdtD.CUST_SVC_TASK_CSTM_DTL_CD = BCustSvcTaskBckdtD.CUST_SVC_TASK_CSTM_DTL_CD
AND CustSvcTaskBckdtD.CUST_SVC_TASK_CSTM_DTL_UNIQ_ID = BCustSvcTaskBckdtD.CUST_SVC_TASK_CSTM_DTL_UNIQ_ID
WHERE CustSvcTaskBckdtD.LAST_UPDT_RUN_CYC_EXCTN_SK >= 2013
AND CAST(CustSvcTaskBckdtD.BCKDT_DT AS CHAR(10)) <> BCustSvcTaskBckdtD.BCKDT_DT
"""

df_db2_SrcTrgtRowComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", db2_SrcTrgtRowComp_query)
    .load()
)

df_lnk_Missing = df_db2_SrcTrgtRowComp

df_xfm_BusinessLogic_lnk_Research = df_lnk_Missing.select(
    col("SRC_SYS_CD"),
    col("CUST_SVC_ID"),
    col("CUST_SVC_TASK_SEQ_NO"),
    col("CUST_SVC_TASK_BCKDT_SEQ_NO"),
    col("CUST_SVC_TASK_CSTM_DTL_CD"),
    col("CUST_SVC_TASK_CSTM_DTL_UNIQ_ID"),
    col("BCKDT_DT")
)

windowSpec = Window.orderBy(lit(1))
df_xfm_BusinessLogic_lnk_DD_Cp_intermediate = df_lnk_Missing.withColumn("rownum", row_number().over(windowSpec))
df_xfm_BusinessLogic_lnk_DD_Cp_filtered = df_xfm_BusinessLogic_lnk_DD_Cp_intermediate.filter("rownum = 1")
df_xfm_BusinessLogic_lnk_DD_Cp = df_xfm_BusinessLogic_lnk_DD_Cp_filtered.select(
    lit("ROW TO ROW BALANCING IDS - EDW CUST SVC TASK BCKDT D OUT OF TOLERANCE").alias("NOTIFICATION")
)

df_seq_ResearchFile = df_xfm_BusinessLogic_lnk_Research.select(
    col("SRC_SYS_CD"),
    col("CUST_SVC_ID"),
    col("CUST_SVC_TASK_SEQ_NO"),
    col("CUST_SVC_TASK_BCKDT_SEQ_NO"),
    col("CUST_SVC_TASK_CSTM_DTL_CD"),
    col("CUST_SVC_TASK_CSTM_DTL_UNIQ_ID"),
    rpad(col("BCKDT_DT"), 10, " ").alias("BCKDT_DT")
)

write_files(
    df_seq_ResearchFile,
    f"{adls_path}/balancing/research/IdsEdwCustSvcTaskBckdtDRowToRowResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_Cp_Dup_lnk_Notify = df_xfm_BusinessLogic_lnk_DD_Cp

df_seq_ErrorNotificationFile = df_Cp_Dup_lnk_Notify.select(
    rpad(col("NOTIFICATION"), 70, " ").alias("NOTIFICATION")
)

write_files(
    df_seq_ErrorNotificationFile,
    f"{adls_path}/balancing/notify/CustSvcBalancingNotification.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

db2_SrcTrgtRowComp_Match_query = f"""SELECT
CAST(CustSvcTaskBckdtD.BCKDT_DT AS CHAR(10)) as BCKDT_DT
FROM {EDWOwner}.CUST_SVC_TASK_BCKDT_D CustSvcTaskBckdtD
INNER JOIN {EDWOwner}.B_CUST_SVC_TASK_BCKDT_D BCustSvcTaskBckdtD
ON CustSvcTaskBckdtD.SRC_SYS_CD = BCustSvcTaskBckdtD.SRC_SYS_CD
AND CustSvcTaskBckdtD.CUST_SVC_ID = BCustSvcTaskBckdtD.CUST_SVC_ID
AND CustSvcTaskBckdtD.CUST_SVC_TASK_SEQ_NO = BCustSvcTaskBckdtD.CUST_SVC_TASK_SEQ_NO
AND CustSvcTaskBckdtD.CUST_SVC_TASK_BCKDT_SEQ_NO = BCustSvcTaskBckdtD.CUST_SVC_TASK_BCKDT_SEQ_NO
AND CustSvcTaskBckdtD.CUST_SVC_TASK_CSTM_DTL_CD = BCustSvcTaskBckdtD.CUST_SVC_TASK_CSTM_DTL_CD
AND CustSvcTaskBckdtD.CUST_SVC_TASK_CSTM_DTL_UNIQ_ID = BCustSvcTaskBckdtD.CUST_SVC_TASK_CSTM_DTL_UNIQ_ID
WHERE CustSvcTaskBckdtD.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
AND CAST(CustSvcTaskBckdtD.BCKDT_DT AS CHAR(10)) = BCustSvcTaskBckdtD.BCKDT_DT
"""

df_db2_SrcTrgtRowComp_Match = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", db2_SrcTrgtRowComp_Match_query)
    .load()
)

df_seq_SrcTrgtSyncFile = df_db2_SrcTrgtRowComp_Match.select(
    rpad(col("BCKDT_DT"), 10, " ").alias("BCKDT_DT")
)

write_files(
    df_seq_SrcTrgtSyncFile,
    f"{adls_path}/balancing/sync/CustSvcTaskBckdtDBalancingTotalMatch.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)