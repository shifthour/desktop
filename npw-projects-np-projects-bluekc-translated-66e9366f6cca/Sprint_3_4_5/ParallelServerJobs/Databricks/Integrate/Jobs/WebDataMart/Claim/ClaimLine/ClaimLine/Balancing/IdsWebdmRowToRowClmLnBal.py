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
# MAGIC CALLED BY:          IdsWebdmRowToRowClmLnBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target tables are compared on the specified column to see if there are any discrepancies
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Bhoomi Dasari               09/05/2007          3264                              Originally Programmed                           devlIDS30                      Steph Goddard            09/28/2007

# MAGIC File checked later for rows and email to on-call
# MAGIC Detail file for on-call to research errors
# MAGIC Rows with Failed Comparison on Specified columns
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
# COMMAND ----------
# MAGIC %run ../../../../../../../Utility_Integrate
# COMMAND ----------


ClmMartOwner = get_widget_value('ClmMartOwner','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')

jdbc_url, jdbc_props = get_db_config(clmmart_secret_name)

df_SrcTrgtRowComp_Missing = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""SELECT 
Clmln.SRC_SYS_CD, 
Clmln.CLM_ID, 
Clmln.CLM_LN_SEQ_NO, 
Clmln.CLM_LN_RVNU_CD, 
Clmln.PROC_CD 
FROM {ClmMartOwner}.CLM_DM_CLM_LN Clmln 
FULL OUTER JOIN {ClmMartOwner}.B_CLM_DM_CLM_LN BClmln  
ON Clmln.SRC_SYS_CD = BClmln.SRC_SYS_CD 
AND Clmln.CLM_ID = BClmln.CLM_ID
AND Clmln.CLM_LN_SEQ_NO = BClmln.CLM_LN_SEQ_NO
WHERE 
Clmln.LAST_UPDT_RUN_CYC_NO = {ExtrRunCycle}
AND (Clmln.CLM_LN_RVNU_CD <> BClmln.CLM_LN_RVNU_CD 
OR Clmln.PROC_CD <> BClmln.CLM_LN_PROC_CD)"""
    )
    .load()
)

df_SrcTrgtRowComp_Match = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""SELECT
Clmln.CLM_LN_RVNU_CD, 
Clmln.PROC_CD 
FROM {ClmMartOwner}.CLM_DM_CLM_LN Clmln 
INNER JOIN {ClmMartOwner}.B_CLM_DM_CLM_LN BClmln  
ON Clmln.SRC_SYS_CD = BClmln.SRC_SYS_CD 
AND Clmln.CLM_ID = BClmln.CLM_ID
AND Clmln.CLM_LN_SEQ_NO = BClmln.CLM_LN_SEQ_NO
WHERE 
Clmln.LAST_UPDT_RUN_CYC_NO = {ExtrRunCycle}
AND Clmln.CLM_LN_RVNU_CD = BClmln.CLM_LN_RVNU_CD 
AND Clmln.PROC_CD = BClmln.CLM_LN_PROC_CD"""
    )
    .load()
)

df_SrcTrgtSyncFile = df_SrcTrgtRowComp_Match.select("CLM_LN_RVNU_CD", "PROC_CD")
write_files(
    df_SrcTrgtSyncFile,
    f"{adls_path}/balancing/sync/ClmDmClmLnRowToRowBalancingTotalMatch.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)

w_1 = Window.orderBy(F.lit(1))
df_TransformLogic_in = df_SrcTrgtRowComp_Missing.withColumn("_row_id", F.row_number().over(w_1))

df_TransformLogic_Research = df_TransformLogic_in.select(
    "SRC_SYS_CD",
    "CLM_ID",
    "CLM_LN_SEQ_NO",
    "CLM_LN_RVNU_CD",
    "PROC_CD",
    "_row_id"
).drop("_row_id")

df_TransformLogic_Notify = df_TransformLogic_in.filter(F.col("_row_id") == 1).select(
    F.lit("ROW TO ROW BALANCING IDS - DATAMART CLM DM CLM LN OUT OF TOLERANCE").alias("NOTIFICATION")
).withColumn("NOTIFICATION", F.rpad(F.col("NOTIFICATION"), 70, " "))

df_ResearchFile = df_TransformLogic_Research.select(
    "SRC_SYS_CD",
    "CLM_ID",
    "CLM_LN_SEQ_NO",
    "CLM_LN_RVNU_CD",
    "PROC_CD"
)
write_files(
    df_ResearchFile,
    f"{adls_path}/balancing/research/IdsWebdmClmLnRowToRowResearch.dat.#RunID#",
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
    f"{adls_path}/balancing/notify/ClmLnBalancingNotification.dat",
    ",",
    "append",
    False,
    False,
    "\"",
    None
)