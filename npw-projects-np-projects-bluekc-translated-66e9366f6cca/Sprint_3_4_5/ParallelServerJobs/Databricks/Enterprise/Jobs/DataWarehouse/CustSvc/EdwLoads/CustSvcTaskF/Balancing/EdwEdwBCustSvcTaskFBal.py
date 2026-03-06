# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC CALLED BY:          IdsEdwCustSvcTaskFBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target tables are compared on the specified column to see if there are any discrepancies
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               06/29/2007          3264                              Originally Programmed                          devlEDW10               Steph Goddard             10/21/2007             
# MAGIC 
# MAGIC 
# MAGIC Bhupinder Kaur                12/09/2013          5114                               Create Load File for                          EnterpriseWhseDevl       Jag Yelavarthi              2014-02-25
# MAGIC                                                                                                              EDW Table  
# MAGIC                                                                                                              B_CUST_SVC_TASK_F

# MAGIC Rows with Failed Comparison on Specified columns
# MAGIC Detail file for on-call to research errors
# MAGIC Job: EdwEdwBCustSvcTaskFBal
# MAGIC     
# MAGIC 
# MAGIC IDS - EDW Customer Service Task Facts Row To Row Comparisons
# MAGIC File checked later for rows and email to on-call
# MAGIC Pull all the Matching Records from 
# MAGIC CUST_SVC_F
# MAGIC B_CUST_SVC_TASK_F
# MAGIC Write all the Matching Records from EDWtables:
# MAGIC CUST_SVC_TASK_F and B_CUST_SVC_TASK_F
# MAGIC Copy stage used to remove duplicates and create one record for the Notification file.
# MAGIC Pull all the Missing Records from
# MAGIC CUST_SVC_TASK_F 
# MAGIC B_CUST_SVC_TASK_F
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

EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
RunID = get_widget_value('RunID','')

jdbc_url, jdbc_props = get_db_config(edw_secret_name)

extract_query_db2_B_CUST_SVC_TASK_F_Missing_in = f"""
SELECT 
CustSvcTaskF.SRC_SYS_CD,
CustSvcTaskF.CUST_SVC_ID,
CustSvcTaskF.CUST_SVC_TASK_SEQ_NO,
CustSvcTaskF.GRP_ID
FROM {EDWOwner}.CUST_SVC_TASK_F CustSvcTaskF
FULL OUTER JOIN {EDWOwner}.B_CUST_SVC_TASK_F BCustSvcTaskF
ON CustSvcTaskF.SRC_SYS_CD = BCustSvcTaskF.SRC_SYS_CD
AND CustSvcTaskF.CUST_SVC_ID = BCustSvcTaskF.CUST_SVC_ID
AND CustSvcTaskF.CUST_SVC_TASK_SEQ_NO = BCustSvcTaskF.CUST_SVC_TASK_SEQ_NO
WHERE 
CustSvcTaskF.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
AND CustSvcTaskF.GRP_ID <> BCustSvcTaskF.GRP_ID
"""
df_db2_B_CUST_SVC_TASK_F_Missing_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_B_CUST_SVC_TASK_F_Missing_in)
    .load()
)

df_xfrm_BusinessLogic_1 = df_db2_B_CUST_SVC_TASK_F_Missing_in.select(
    col("SRC_SYS_CD"),
    col("CUST_SVC_ID"),
    col("CUST_SVC_TASK_SEQ_NO"),
    col("GRP_ID")
)

df_xfrm_BusinessLogic_2_input = df_db2_B_CUST_SVC_TASK_F_Missing_in.limit(1)
df_xfrm_BusinessLogic_2 = df_xfrm_BusinessLogic_2_input.select(
    lit("ROW TO ROW BALANCING IDS - EDW CUST SVC TASK F OUT OF TOLERANCE").alias("NOTIFICATION")
)

write_files(
    df_xfrm_BusinessLogic_1.select("SRC_SYS_CD", "CUST_SVC_ID", "CUST_SVC_TASK_SEQ_NO", "GRP_ID"),
    f"{adls_path}/balancing/research/IdsEdwCustSvcTaskFRowToRowResearch.dat.{RunID}",
    ",",
    "overwrite",
    False,
    False,
    "^",
    None
)

df_cpy_DDup = dedup_sort(
    df_xfrm_BusinessLogic_2,
    ["NOTIFICATION"],
    [("NOTIFICATION", "A")]
)

df_cpy_DDup_final = df_cpy_DDup.withColumn(
    "NOTIFICATION",
    rpad("NOTIFICATION", 70, " ")
)

write_files(
    df_cpy_DDup_final.select("NOTIFICATION"),
    f"{adls_path}/balancing/notify/CustSvcBalancingNotification.dat",
    ",",
    "append",
    False,
    False,
    "^",
    None
)

extract_query_db2_B_CUST_SVC_TASK_F_Matching_in = f"""
SELECT
CustSvcTaskF.GRP_ID
FROM {EDWOwner}.CUST_SVC_TASK_F CustSvcTaskF
INNER JOIN {EDWOwner}.B_CUST_SVC_TASK_F BCustSvcTaskF
ON CustSvcTaskF.SRC_SYS_CD = BCustSvcTaskF.SRC_SYS_CD
AND CustSvcTaskF.CUST_SVC_ID = BCustSvcTaskF.CUST_SVC_ID
AND CustSvcTaskF.CUST_SVC_TASK_SEQ_NO = BCustSvcTaskF.CUST_SVC_TASK_SEQ_NO
WHERE
CustSvcTaskF.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
AND CustSvcTaskF.GRP_ID = BCustSvcTaskF.GRP_ID
"""
df_db2_B_CUST_SVC_TASK_F_Matching_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_B_CUST_SVC_TASK_F_Matching_in)
    .load()
)

write_files(
    df_db2_B_CUST_SVC_TASK_F_Matching_in.select("GRP_ID"),
    f"{adls_path}/balancing/sync/CustSvcTaskFBalancingTotalMatch.dat",
    ",",
    "overwrite",
    False,
    False,
    "^",
    None
)