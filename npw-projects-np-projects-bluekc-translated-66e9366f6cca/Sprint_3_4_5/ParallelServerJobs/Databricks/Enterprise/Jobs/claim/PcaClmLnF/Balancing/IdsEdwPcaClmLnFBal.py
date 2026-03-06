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
# MAGIC CALLED BY:          IdsEdwPcaClmLnFBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target tables are compared on the specified column to see if there are any discrepancies
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               09/06/2007          3264                              Originally Programmed                           devlEDW10              Steph Goddad             10/23/2007

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
from pyspark.sql.functions import col, lit, row_number, rpad
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


edw_secret_name = get_widget_value('edw_secret_name','')
EDWOwner = get_widget_value('EDWOwner','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
RunID = get_widget_value('RunID','')

jdbc_url, jdbc_props = get_db_config(edw_secret_name)

extract_query_1 = f"""
SELECT
  PCA_CLM_LN_F.SRC_SYS_CD,
  PCA_CLM_LN_F.CLM_ID,
  PCA_CLM_LN_F.CLM_LN_SEQ_NO,
  PCA_CLM_LN_F.CLM_LN_PCA_PD_AMT
FROM {EDWOwner}.PCA_CLM_LN_F PCA_CLM_LN_F
FULL OUTER JOIN {EDWOwner}.B_PCA_CLM_LN_F B_PCA_CLM_LN_F
  ON PCA_CLM_LN_F.SRC_SYS_CD = B_PCA_CLM_LN_F.SRC_SYS_CD
  AND PCA_CLM_LN_F.CLM_ID = B_PCA_CLM_LN_F.CLM_ID
  AND PCA_CLM_LN_F.CLM_LN_SEQ_NO = B_PCA_CLM_LN_F.CLM_LN_SEQ_NO
WHERE PCA_CLM_LN_F.LAST_UPDT_RUN_CYC_EXCTN_SK = {ExtrRunCycle}
  AND PCA_CLM_LN_F.CLM_LN_PCA_PD_AMT <> B_PCA_CLM_LN_F.CLM_LN_PCA_PD_AMT
"""

extract_query_2 = f"""
SELECT
  *
FROM {EDWOwner}.CUST_SVC_D CustSvcD
FULL OUTER JOIN {EDWOwner}.B_CUST_SVC_D BCustSvcD
  ON CustSvcD.SRC_SYS_CD = BCustSvcD.SRC_SYS_CD
  AND CustSvcD.CUST_SVC_ID = BCustSvcD.CUST_SVC_ID
"""

df_SrcTrgtRowComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_1)
    .load()
)

df_unused = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_2)
    .load()
)

df_SrcTrgtRowComp_numbered = df_SrcTrgtRowComp.withColumn(
    "rownum",
    row_number().over(Window.orderBy(lit(1)))
)

df_Notify_temp = df_SrcTrgtRowComp_numbered.filter(col("rownum") == 1).select(
    lit("COLUMN SUM BALANCING IDS - EDW PCA CLM LN F OUT OF TOLERANCE").alias("NOTIFICATION")
)

df_Notify = df_Notify_temp.withColumn(
    "NOTIFICATION", rpad("NOTIFICATION", 70, " ")
)

df_Research = df_SrcTrgtRowComp_numbered.select(
    "SRC_SYS_CD",
    "CLM_ID",
    "CLM_LN_SEQ_NO",
    "CLM_LN_PCA_PD_AMT"
)

write_files(
    df_Notify,
    f"{adls_path}/balancing/notify/ClaimsBalancingNotification.txt",
    ",",
    "append",
    False,
    False,
    "\"",
    None
)

write_files(
    df_Research,
    f"{adls_path}/balancing/research/IdsEdwPcaClmLnFColumnSumResearch.dat.{RunID}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)