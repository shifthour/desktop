# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 02/01/08 12:08:08 Batch  14642_43698 INIT bckcetl edw10 dsadm dsadm
# MAGIC ^1_1 11/02/07 14:51:45 Batch  14551_53511 PROMOTE bckcetl edw10 dsadm bls for on
# MAGIC ^1_1 11/02/07 14:44:15 Batch  14551_53058 INIT bckcett testEDW10 dsadm bls for on
# MAGIC ^1_1 11/01/07 12:56:04 Batch  14550_46572 PROMOTE bckcett testEDW10 u03651 steffy
# MAGIC ^1_1 11/01/07 12:51:30 Batch  14550_46292 INIT bckcett devlEDW10 u03651 steffy
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:          IdsEdwClmnSumUmSvcFBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target tables are compared on the specified column to see if there are any discrepancies
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               07/30/2007          3264                              Originally Programmed                           devlEDW10              Steph Goddard             10/26/2007

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
from pyspark.sql.functions import lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


EDWOwner = get_widget_value('EDWOwner','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
RunID = get_widget_value('RunID','')
edw_secret_name = get_widget_value('edw_secret_name','')

jdbc_url, jdbc_props = get_db_config(edw_secret_name)
extract_query = """SELECT
UM_SVC_F.SRC_SYS_CD,
UM_SVC_F.UM_REF_ID,
UM_SVC_F.UM_SVC_SEQ_NO,
UM_SVC_F.UM_SVC_PD_AMT,
UM_SVC_F.UM_SVC_ALW_UNIT_CT

FROM 
#$EDWOwner#.UM_SVC_F UM_SVC_F FULL OUTER JOIN #$EDWOwner#.B_UM_SVC_F B_UM_SVC_F
ON UM_SVC_F.SRC_SYS_CD = B_UM_SVC_F.SRC_SYS_CD 
AND UM_SVC_F.UM_REF_ID = B_UM_SVC_F.UM_REF_ID 
AND UM_SVC_F.UM_SVC_SEQ_NO = B_UM_SVC_F.UM_SVC_SEQ_NO

WHERE 
UM_SVC_F.LAST_UPDT_RUN_CYC_EXCTN_SK >= #ExtrRunCycle#
AND  
(UM_SVC_F.UM_SVC_PD_AMT <> B_UM_SVC_F.UM_SVC_PD_AMT 
OR UM_SVC_F.UM_SVC_ALW_UNIT_CT <> B_UM_SVC_F.UM_SVC_ALW_UNIT_CT)
;
#$EDWOwner#.CUST_SVC_D CustSvcD FULL OUTER JOIN #$EDWOwner#.B_CUST_SVC_D BCustSvcD ON CustSvcD.SRC_SYS_CD = BCustSvcD.SRC_SYS_CD AND CustSvcD.CUST_SVC_ID = BCustSvcD.CUST_SVC_ID
"""

df_SrcTrgtRowComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_research = df_SrcTrgtRowComp.select(
    "SRC_SYS_CD",
    "UM_REF_ID",
    "UM_SVC_SEQ_NO",
    "UM_SVC_PD_AMT",
    "UM_SVC_ALW_UNIT_CT"
)

df_notify_temp = df_SrcTrgtRowComp.limit(1).select(
    lit("COLUMN SUM BALANCING IDS - EDW UM SVC F OUT OF TOLERANCE").alias("NOTIFICATION")
)

df_notify = df_notify_temp.select(
    rpad("NOTIFICATION", 70, " ").alias("NOTIFICATION")
)

write_files(
    df_notify,
    f"{adls_path}/balancing/notify/MedMgtBalancingNotification.txt",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

df_research_out = df_research.select(
    "SRC_SYS_CD",
    "UM_REF_ID",
    "UM_SVC_SEQ_NO",
    "UM_SVC_PD_AMT",
    "UM_SVC_ALW_UNIT_CT"
)

write_files(
    df_research_out,
    f"{adls_path}/balancing/research/IdsEdwUmSvcFColumnSumResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)