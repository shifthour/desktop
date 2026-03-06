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
# MAGIC CALLED BY:          IdsWebdmClmnSumInitClmBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target tables are compared on the specified column to see if there are any discrepancies
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Bhoomi Dasari               08/31/2007          3264                              Originally Programmed                           devlIDS30                     Steph Goddard             09/28/2007

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
from pyspark.sql.types import StructType, StructField, StringType, DecimalType
from pyspark.sql.functions import lit, rpad, col
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


ClmMartOwner = get_widget_value('ClmMartOwner','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')

jdbc_url, jdbc_props = get_db_config(clmmart_secret_name)

extract_query = f"""SELECT 
INIT_CLM.SRC_SYS_CD, 
INIT_CLM.CLM_ID,
INIT_CLM.CLM_CHRG_AMT, 
INIT_CLM.CLM_PAYBL_AMT
FROM {ClmMartOwner}.CLM_DM_INIT_CLM INIT_CLM
FULL OUTER JOIN {ClmMartOwner}.B_CLM_DM_CLM B_CLM
ON INIT_CLM.SRC_SYS_CD = B_CLM.SRC_SYS_CD
AND INIT_CLM.CLM_ID = B_CLM.CLM_ID
WHERE
INIT_CLM.CLM_CHRG_AMT <> B_CLM.CLM_LN_TOT_CHRG_AMT
AND INIT_CLM.CLM_PAYBL_AMT <> B_CLM.CLM_PAYBL_AMT
"""

df_SrcTrgtRowComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_TransformLogic_in = df_SrcTrgtRowComp

# Research link
df_Research = df_TransformLogic_in.select(
    col("SRC_SYS_CD"),
    col("CLM_ID"),
    col("CLM_CHRG_AMT"),
    col("CLM_PAYBL_AMT")
)

# Notify link (Constraint: @INROWNUM = 1)
df_notify_temp = df_TransformLogic_in.limit(1).select(
    lit("COLUMN SUM BALANCING IDS - DATAMART CLM DM INIT CLM OUT OF TOLERANCE").alias("NOTIFICATION")
)
df_Notify = df_notify_temp

# Stage: ResearchFile
df_ResearchFileOut = df_Research.select(
    col("SRC_SYS_CD"),
    col("CLM_ID"),
    col("CLM_CHRG_AMT"),
    col("CLM_PAYBL_AMT")
)
write_files(
    df_ResearchFileOut,
    f"{adls_path}/balancing/research/IdsWebdmInitClmColumnSumResearch.dat.{ExtrRunCycle}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Stage: ErrorNotificationFile
df_Notify_out = df_Notify.withColumn("NOTIFICATION", rpad(col("NOTIFICATION"), 70, " "))
df_Notify_out = df_Notify_out.select("NOTIFICATION")
write_files(
    df_Notify_out,
    f"{adls_path}/balancing/notify/InitClmBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)