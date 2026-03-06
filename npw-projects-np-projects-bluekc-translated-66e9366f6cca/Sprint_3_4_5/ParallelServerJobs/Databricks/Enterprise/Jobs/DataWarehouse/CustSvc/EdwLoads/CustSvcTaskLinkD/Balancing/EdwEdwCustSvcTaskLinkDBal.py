# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:          IdsEdwCustSvcTaskLinkDBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target tables are compared on the specified column to see if there are any discrepancies
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               07/02/2007          3264                              Originally Programmed                         devlEDW10                Steph Goddard             10/21/2007         
# MAGIC 
# MAGIC Archana Palivela               12/06/2013          5114                               Rewrite in Parallel                        EnterpriseWhseDevl          Jag Yelavarthi               2014-02-26

# MAGIC Rows with Failed Comparison on Specified columns
# MAGIC Detail file for on-call to research errors
# MAGIC Job: EdwEdwCustSvcTaskFundFBal
# MAGIC File checked later for rows and email to on-call
# MAGIC Copy stage used to remove duplicates and create one record for the Notification file.
# MAGIC Write all the Matching Records from EDWtables:
# MAGIC CUST_SVC_TASK_LINK_D and B_CUST_SVC_TASK_LINK_D
# MAGIC Pull all the Matching Records from 
# MAGIC CUST_SVC_TASK_LINK_D 
# MAGIC B_CUST_SVC_TASK_LINK_D
# MAGIC Pull all the Missing Records from
# MAGIC CUST_SVC_TASK_LINK_D
# MAGIC B_CUST_SVC_TASK_LINK_D
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../../../../Utility_Enterprise
# COMMAND ----------


edw_secret_name = get_widget_value('edw_secret_name', '')
EDWOwner = get_widget_value('EDWOwner', '')
ExtrRunCycle = get_widget_value('ExtrRunCycle', '')
RunID = get_widget_value('RunID', '')

jdbc_url_db2_B_CUST_SVC_TASK_LINK_D_Missing_in, jdbc_props_db2_B_CUST_SVC_TASK_LINK_D_Missing_in = get_db_config(edw_secret_name)
query_db2_B_CUST_SVC_TASK_LINK_D_Missing_in = (
    "SELECT \n"
    "CustSvcTaskLinkD.SRC_SYS_CD,\n"
    "CustSvcTaskLinkD.CUST_SVC_ID,\n"
    "CustSvcTaskLinkD.CUST_SVC_TASK_SEQ_NO,\n"
    "CustSvcTaskLinkD.CUST_SVC_TASK_LINK_TYP_CD,\n"
    "CustSvcTaskLinkD.CUST_SVC_TASK_LINK_RCRD_ID,\n"
    "CustSvcTaskLinkD.APL_SK\n"
    "FROM \n"
    + EDWOwner
    + ".CUST_SVC_TASK_LINK_D CustSvcTaskLinkD FULL OUTER JOIN "
    + EDWOwner
    + ".B_CUST_SVC_TASK_LINK_D BCustSvcTaskLinkD \n"
    "ON CustSvcTaskLinkD.SRC_SYS_CD = BCustSvcTaskLinkD.SRC_SYS_CD AND CustSvcTaskLinkD.CUST_SVC_ID = BCustSvcTaskLinkD.CUST_SVC_ID \n"
    "AND CustSvcTaskLinkD.CUST_SVC_TASK_SEQ_NO = BCustSvcTaskLinkD.CUST_SVC_TASK_SEQ_NO \n"
    "AND CustSvcTaskLinkD.CUST_SVC_TASK_LINK_TYP_CD = BCustSvcTaskLinkD.CUST_SVC_TASK_LINK_TYP_CD \n"
    "AND CustSvcTaskLinkD.CUST_SVC_TASK_LINK_RCRD_ID = BCustSvcTaskLinkD.CUST_SVC_TASK_LINK_RCRD_ID\n"
    "WHERE \n"
    "CustSvcTaskLinkD.LAST_UPDT_RUN_CYC_EXCTN_SK >= "
    + ExtrRunCycle
    + "\n"
    "AND CustSvcTaskLinkD.APL_SK <> BCustSvcTaskLinkD.APL_SK"
)
df_db2_B_CUST_SVC_TASK_LINK_D_Missing_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_B_CUST_SVC_TASK_LINK_D_Missing_in)
    .options(**jdbc_props_db2_B_CUST_SVC_TASK_LINK_D_Missing_in)
    .option("query", query_db2_B_CUST_SVC_TASK_LINK_D_Missing_in)
    .load()
)

df_xfrm_BusinessLogic_out1 = df_db2_B_CUST_SVC_TASK_LINK_D_Missing_in.select(
    F.col("SRC_SYS_CD"),
    F.col("CUST_SVC_ID"),
    F.col("CUST_SVC_TASK_SEQ_NO"),
    F.col("CUST_SVC_TASK_LINK_TYP_CD"),
    F.col("CUST_SVC_TASK_LINK_RCRD_ID"),
    F.col("APL_SK")
)

df_xfrm_BusinessLogic_out2 = (
    df_db2_B_CUST_SVC_TASK_LINK_D_Missing_in.limit(1)
    .select(
        F.lit("ROW TO ROW BALANCING IDS - EDW CUST SVC TASK LINK D OUT OF TOLERANCE").alias("NOTIFICATION")
    )
)

df_seq_ResearchFile_csv_load = df_xfrm_BusinessLogic_out1.select(
    "SRC_SYS_CD",
    "CUST_SVC_ID",
    "CUST_SVC_TASK_SEQ_NO",
    "CUST_SVC_TASK_LINK_TYP_CD",
    "CUST_SVC_TASK_LINK_RCRD_ID",
    "APL_SK"
)

write_files(
    df_seq_ResearchFile_csv_load,
    f"{adls_path}/balancing/research/IdsEdwCustSvcTaskLinkDRowToRowResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

df_cpy_DDup_dedup = dedup_sort(
    df_xfrm_BusinessLogic_out2,
    partition_cols=["NOTIFICATION"],
    sort_cols=[("NOTIFICATION", "A")]
)

df_cpy_DDup = df_cpy_DDup_dedup.select(
    F.col("NOTIFICATION")
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

jdbc_url_db2_B_CUST_SVC_TASK_LINK_D_Matching_in, jdbc_props_db2_B_CUST_SVC_TASK_LINK_D_Matching_in = get_db_config(edw_secret_name)
query_db2_B_CUST_SVC_TASK_LINK_D_Matching_in = (
    "SELECT \n"
    "CustSvcTaskLinkD.APL_SK\n"
    "FROM \n"
    + EDWOwner
    + ".CUST_SVC_TASK_LINK_D CustSvcTaskLinkD FULL OUTER JOIN "
    + EDWOwner
    + ".B_CUST_SVC_TASK_LINK_D BCustSvcTaskLinkD \n"
    "ON CustSvcTaskLinkD.SRC_SYS_CD = BCustSvcTaskLinkD.SRC_SYS_CD AND CustSvcTaskLinkD.CUST_SVC_ID = BCustSvcTaskLinkD.CUST_SVC_ID \n"
    "AND CustSvcTaskLinkD.CUST_SVC_TASK_SEQ_NO = BCustSvcTaskLinkD.CUST_SVC_TASK_SEQ_NO \n"
    "AND CustSvcTaskLinkD.CUST_SVC_TASK_LINK_TYP_CD = BCustSvcTaskLinkD.CUST_SVC_TASK_LINK_TYP_CD \n"
    "AND CustSvcTaskLinkD.CUST_SVC_TASK_LINK_RCRD_ID = BCustSvcTaskLinkD.CUST_SVC_TASK_LINK_RCRD_ID\n"
    "WHERE \n"
    "CustSvcTaskLinkD.LAST_UPDT_RUN_CYC_EXCTN_SK >= "
    + ExtrRunCycle
    + "\n"
    "AND CustSvcTaskLinkD.APL_SK <> BCustSvcTaskLinkD.APL_SK"
)
df_db2_B_CUST_SVC_TASK_LINK_D_Matching_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_B_CUST_SVC_TASK_LINK_D_Matching_in)
    .options(**jdbc_props_db2_B_CUST_SVC_TASK_LINK_D_Matching_in)
    .option("query", query_db2_B_CUST_SVC_TASK_LINK_D_Matching_in)
    .load()
)

df_seq_B_CUST_SVC_TASK_LINK_D_Matching_csv_out = df_db2_B_CUST_SVC_TASK_LINK_D_Matching_in.select(
    "APL_SK"
)

write_files(
    df_seq_B_CUST_SVC_TASK_LINK_D_Matching_csv_out,
    f"{adls_path}/balancing/sync/CustSvcTaskLinkDBalancingTotalMatch.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)