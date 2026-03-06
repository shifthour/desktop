# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 01/29/08 14:29:13 Batch  14639_52158 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 01/09/08 13:32:13 Batch  14619_48738 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/24/07 09:35:50 Batch  14573_34558 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 10/11/07 15:00:54 Batch  14529_54061 PROMOTE bckcetl ids20 dsadm bls for bl
# MAGIC ^1_1 10/11/07 14:39:56 Batch  14529_52800 INIT bckcett testIDS30 dsadm bls for bl
# MAGIC ^1_1 10/10/07 07:56:07 Batch  14528_28571 PROMOTE bckcett testIDS30 u08717 Brent
# MAGIC ^1_1 10/10/07 07:39:54 Batch  14528_27598 INIT bckcett devlIDS30 u08717 Brent
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:          IdsWebdmClmnSumUmSvcBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target tables are compared on the specified column to see if there are any discrepancies
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               07/31/2007          3264                              Originally Programmed                           devlIDS30                              
# MAGIC 
# MAGIC Manasa Andru                  11/02/2011        TTR- 1234               Changed the Output file extension from       IntegrateNewDevl        SAndrew                        2011-11-17
# MAGIC                                                                                                                     .TXT to .DAT

# MAGIC File checked later for rows and email to on-call
# MAGIC Detail file for on-call to research errors
# MAGIC Rows with Failed Comparison on Specified columns
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import rpad
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


ClmMartOwner = get_widget_value('ClmMartOwner','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
RunID = get_widget_value('RunID','')

jdbc_url, jdbc_props = get_db_config(clmmart_secret_name)

extract_query = f"""
SELECT
UM_SVC.SRC_SYS_CD,
UM_SVC.UM_REF_ID,
UM_SVC.UM_SVC_SEQ_NO,
UM_SVC.UM_SVC_PD_AMT,
UM_SVC.UM_SVC_ALW_UNIT_CT
FROM {ClmMartOwner}.MED_MGT_DM_UM_SVC UM_SVC
FULL OUTER JOIN {ClmMartOwner}.B_MED_MGT_DM_UM_SVC BUmSvc
ON UM_SVC.SRC_SYS_CD = BUmSvc.SRC_SYS_CD
AND UM_SVC.UM_REF_ID = BUmSvc.UM_REF_ID
AND UM_SVC.UM_SVC_SEQ_NO = BUmSvc.UM_SVC_SEQ_NO
WHERE UM_SVC.LAST_UPDT_RUN_CYC_NO >= {ExtrRunCycle}
AND (
  UM_SVC.UM_SVC_PD_AMT <> BUmSvc.UM_SVC_PD_AMT
  OR UM_SVC.UM_SVC_ALW_UNIT_CT <> BUmSvc.UM_SVC_ALW_UNIT_CT
)
"""

df_SrcTrgtRowComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_TransformLogic_Research = df_SrcTrgtRowComp.select(
    "SRC_SYS_CD",
    "UM_REF_ID",
    "UM_SVC_SEQ_NO",
    "UM_SVC_PD_AMT",
    "UM_SVC_ALW_UNIT_CT"
)

df_TransformLogic_Notify_pre = df_SrcTrgtRowComp.limit(1).selectExpr(
    "'COLUMN SUM BALANCING IDS - DATAMART MED MGT DM UM SVC OUT OF TOLERANCE' as NOTIFICATION"
)
df_TransformLogic_Notify = df_TransformLogic_Notify_pre.withColumn(
    "NOTIFICATION",
    rpad("NOTIFICATION", 70, " ")
)

df_ResearchFile = df_TransformLogic_Research.select(
    "SRC_SYS_CD",
    "UM_REF_ID",
    "UM_SVC_SEQ_NO",
    "UM_SVC_PD_AMT",
    "UM_SVC_ALW_UNIT_CT"
)

write_files(
    df_ResearchFile,
    f"{adls_path}/balancing/research/IdsWebdmUmSvcColumnSumResearch.dat.{RunID}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)

df_ErrorNotificationFile = df_TransformLogic_Notify.select("NOTIFICATION")

write_files(
    df_ErrorNotificationFile,
    f"{adls_path}/balancing/notify/MedMgtBalancingNotification.dat",
    ",",
    "append",
    False,
    False,
    "\"",
    None
)