# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:          IdsEdwCustSvcTaskDBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target tables are compared on the specified column to see if there are any discrepancies
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               07/02/2007          3264                              Originally Programmed                         devlEDW10                Steph Goddard            10/21/2007              
# MAGIC   
# MAGIC Archana Palivela               12/06/2013          5114                               Rewrite in Parallel                        EnterpriseWhseDevl        Peter Marshall               1/8/2014

# MAGIC Rows with Failed Comparison on Specified columns
# MAGIC Detail file for on-call to research errors
# MAGIC Job: EdwEdwCustSvcTaskDBal
# MAGIC File checked later for rows and email to on-call
# MAGIC Copy stage used to remove duplicates and create one record for the Notification file.
# MAGIC Write all the Matching Records from EDWtables:
# MAGIC CUST_SVC_TASK_D and B_CUST_SVC_TASK_D
# MAGIC Pull all the Matching Records from CUST_SVC_TASK_D
# MAGIC B_CUST_SVC_TASK_D
# MAGIC Pull all the Missing Records from
# MAGIC CUST_SVC_TASK_D
# MAGIC B_CUST_SVC_TASK_D
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../../../Utility_Enterprise
# COMMAND ----------


EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
RunID = get_widget_value('RunID','')

jdbc_url, jdbc_props = get_db_config(edw_secret_name)

extract_query_1 = f"""
SELECT
CustSvcTaskD.SRC_SYS_CD,
CustSvcTaskD.CUST_SVC_ID,
CustSvcTaskD.CUST_SVC_TASK_SEQ_NO,
CustSvcTaskD.CUST_SVC_TASK_CUST_ID
FROM
{EDWOwner}.CUST_SVC_TASK_D CustSvcTaskD
FULL OUTER JOIN {EDWOwner}.B_CUST_SVC_TASK_D BCustSvcTaskD
ON CustSvcTaskD.SRC_SYS_CD = BCustSvcTaskD.SRC_SYS_CD
AND CustSvcTaskD.CUST_SVC_ID = BCustSvcTaskD.CUST_SVC_ID
AND CustSvcTaskD.CUST_SVC_TASK_SEQ_NO = BCustSvcTaskD.CUST_SVC_TASK_SEQ_NO
WHERE
CustSvcTaskD.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
AND CustSvcTaskD.CUST_SVC_TASK_CUST_ID <> BCustSvcTaskD.CUST_SVC_TASK_CUST_ID
"""

df_db2_B_CUST_SVC_TASK_D_Missing_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_1)
    .load()
)

df_EdwEdwCustSvcTaskDBal_Error_InABC = df_db2_B_CUST_SVC_TASK_D_Missing_in.select(
    F.col("SRC_SYS_CD"),
    F.col("CUST_SVC_ID"),
    F.col("CUST_SVC_TASK_SEQ_NO"),
    F.col("CUST_SVC_TASK_CUST_ID")
)

df_NotifyDDup_in = df_db2_B_CUST_SVC_TASK_D_Missing_in.limit(1).selectExpr(
    "'ROW TO ROW BALANCING IDS - EDW CUST SVC TASK D OUT OF TOLERANCE' as NOTIFICATION"
)

write_files(
    df_EdwEdwCustSvcTaskDBal_Error_InABC.select(
        "SRC_SYS_CD",
        "CUST_SVC_ID",
        "CUST_SVC_TASK_SEQ_NO",
        "CUST_SVC_TASK_CUST_ID"
    ),
    f"{adls_path}/balancing/research/IdsEdwCustSvcTaskDRowToRowResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

df_cpy_DDup = dedup_sort(
    df_NotifyDDup_in,
    partition_cols=["NOTIFICATION"],
    sort_cols=[("NOTIFICATION", "A")]
)

df_seq_ErrorNotificationFile_csv_load = df_cpy_DDup.select(
    F.rpad(F.col("NOTIFICATION"), 70, " ").alias("NOTIFICATION")
)

write_files(
    df_seq_ErrorNotificationFile_csv_load,
    f"{adls_path}/balancing/notify/CustSvcBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

extract_query_2 = f"""
SELECT
CustSvcTaskD.CUST_SVC_TASK_CUST_ID
FROM
{EDWOwner}.CUST_SVC_TASK_D CustSvcTaskD
FULL OUTER JOIN {EDWOwner}.B_CUST_SVC_TASK_D BCustSvcTaskD
ON CustSvcTaskD.SRC_SYS_CD = BCustSvcTaskD.SRC_SYS_CD
AND CustSvcTaskD.CUST_SVC_ID = BCustSvcTaskD.CUST_SVC_ID
AND CustSvcTaskD.CUST_SVC_TASK_SEQ_NO = BCustSvcTaskD.CUST_SVC_TASK_SEQ_NO
WHERE
CustSvcTaskD.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
AND CustSvcTaskD.CUST_SVC_TASK_CUST_ID <> BCustSvcTaskD.CUST_SVC_TASK_CUST_ID
"""

df_db2_B_CUST_SVC_TASK_D_Matching_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_2)
    .load()
)

write_files(
    df_db2_B_CUST_SVC_TASK_D_Matching_in.select("CUST_SVC_TASK_CUST_ID"),
    f"{adls_path}/balancing/sync/CustSvcTaskDBalancingTotalMatch.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)