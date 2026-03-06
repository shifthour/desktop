# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 01/29/08 14:29:13 Batch  14639_52158 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 01/09/08 13:32:13 Batch  14619_48738 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/24/07 09:35:50 Batch  14573_34558 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_2 11/01/07 15:16:03 Batch  14550_54982 PROMOTE bckcetl ids20 dsadm rc for brent
# MAGIC ^1_2 11/01/07 15:05:36 Batch  14550_54348 INIT bckcett testIDS30 dsadm rc for brent 
# MAGIC ^1_2 10/31/07 10:30:33 Batch  14549_37838 PROMOTE bckcett testIDS30 u08717 Brent
# MAGIC ^1_2 10/31/07 10:24:16 Batch  14549_37464 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_1 10/24/07 15:30:15 Batch  14542_55818 INIT bckcett devlIDS30 u10157 sa - DRG project - moving to ids_current devlopment for coding changes
# MAGIC ^1_1 10/10/07 07:31:23 Batch  14528_27086 INIT bckcett devlIDS30 u08717 Brent
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:          IdsWebdmRowToRowClmBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target tables are compared on the specified column to see if there are any discrepancies
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Bhoomi Dasari                 09/04/2007          3264                              Originally Programmed                           devlIDS30                   Steph Goddard             09/28/2007

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
from pyspark.sql.functions import lit, rpad
from pyspark.sql import DataFrame
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


ClmMartOwner = get_widget_value('ClmMartOwner','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
runID = get_widget_value('RunID','')

jdbc_url, jdbc_props = get_db_config(clmmart_secret_name)

query_missing = f"SELECT CLM.SRC_SYS_CD, CLM.CLM_ID, CLM.CLM_SVC_STRT_DT, CLM.MBR_UNIQ_KEY, CLM.GRP_ID FROM {ClmMartOwner}.CLM_DM_CLM CLM FULL OUTER JOIN {ClmMartOwner}.B_CLM_DM_CLM B_CLM ON CLM.SRC_SYS_CD = B_CLM.SRC_SYS_CD AND CLM.CLM_ID = B_CLM.CLM_ID WHERE CLM.LAST_UPDT_RUN_CYC_NO = {ExtrRunCycle} AND (CLM.CLM_SVC_STRT_DT <> B_CLM.CLM_SVC_STRT_DT OR CLM.MBR_UNIQ_KEY <> B_CLM.MBR_UNIQ_KEY OR CLM.GRP_ID <> B_CLM.GRP_ID)"
df_SrcTrgtRowComp_Missing = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_missing)
    .load()
)

query_match = f"SELECT CLM.CLM_SVC_STRT_DT, CLM.MBR_UNIQ_KEY, CLM.GRP_ID FROM {ClmMartOwner}.CLM_DM_CLM CLM INNER JOIN {ClmMartOwner}.B_CLM_DM_CLM B_CLM ON CLM.SRC_SYS_CD = B_CLM.SRC_SYS_CD AND CLM.CLM_ID = B_CLM.CLM_ID WHERE CLM.LAST_UPDT_RUN_CYC_NO = {ExtrRunCycle} AND CLM.CLM_SVC_STRT_DT = B_CLM.CLM_SVC_STRT_DT AND CLM.MBR_UNIQ_KEY = B_CLM.MBR_UNIQ_KEY AND CLM.GRP_ID = B_CLM.GRP_ID"
df_SrcTrgtRowComp_Match = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_match)
    .load()
)

df_SrcTrgtRowComp_Match_out = df_SrcTrgtRowComp_Match.select(
    "CLM_SVC_STRT_DT",
    "MBR_UNIQ_KEY",
    "GRP_ID"
)
write_files(
    df_SrcTrgtRowComp_Match_out,
    f"{adls_path}/balancing/sync/ClmDmClmRowToRowBalancingTotalMatch.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)

df_TransformLogic_Research = df_SrcTrgtRowComp_Missing.select(
    "SRC_SYS_CD",
    "CLM_ID",
    "CLM_SVC_STRT_DT",
    "MBR_UNIQ_KEY",
    "GRP_ID"
)

df_TransformLogic_Notify_pre = df_SrcTrgtRowComp_Missing.limit(1).select(
    lit("ROW TO ROW BALANCING IDS - DATAMART CLM DM CLM OUT OF TOLERANCE").alias("NOTIFICATION")
)
df_TransformLogic_Notify = df_TransformLogic_Notify_pre.withColumn(
    "NOTIFICATION", rpad("NOTIFICATION", 70, " ")
)

df_ResearchFile_out = df_TransformLogic_Research.select(
    "SRC_SYS_CD",
    "CLM_ID",
    "CLM_SVC_STRT_DT",
    "MBR_UNIQ_KEY",
    "GRP_ID"
)
write_files(
    df_ResearchFile_out,
    f"{adls_path}/balancing/research/IdsWebdmClmRowToRowResearch.dat.{runID}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)

df_ErrorNotificationFile_out = df_TransformLogic_Notify.select("NOTIFICATION")
write_files(
    df_ErrorNotificationFile_out,
    f"{adls_path}/balancing/notify/ClmBalancingNotification.dat",
    ",",
    "append",
    False,
    False,
    "\"",
    None
)