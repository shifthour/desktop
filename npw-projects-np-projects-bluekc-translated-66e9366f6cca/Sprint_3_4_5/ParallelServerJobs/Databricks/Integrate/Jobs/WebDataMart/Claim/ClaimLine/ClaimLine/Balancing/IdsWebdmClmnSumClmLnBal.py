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
# MAGIC CALLED BY:          IdsWebdmClmnSumClmLnBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target tables are compared on the specified column to see if there are any discrepancies
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Bhoomi Dasari                 09/05/2007          3264                              Originally Programmed                           devlIDS30                   Steph Goddard             09/28/2007

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
from pyspark.sql.types import *
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../../../Utility_Integrate
# COMMAND ----------


ClmMartOwner = get_widget_value('ClmMartOwner','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')
extrruncycle = get_widget_value('ExtrRunCycle','')
runid = get_widget_value('RunID','')

jdbc_url, jdbc_props = get_db_config(clmmart_secret_name)

extract_query = f"""
SELECT 
Clmln.SRC_SYS_CD, 
Clmln.CLM_ID, 
Clmln.CLM_LN_SEQ_NO,
Clmln.CLM_LN_CHRG_AMT,
Clmln.CLM_LN_ALW_AMT,
Clmln.CLM_LN_PAYBL_AMT

FROM 
{ClmMartOwner}.CLM_DM_CLM_LN Clmln FULL OUTER JOIN {ClmMartOwner}.B_CLM_DM_CLM_LN BClmln
ON Clmln.SRC_SYS_CD = BClmln.SRC_SYS_CD 
AND Clmln.CLM_ID = BClmln.CLM_ID
AND Clmln.CLM_LN_SEQ_NO = BClmln.CLM_LN_SEQ_NO

WHERE 
Clmln.LAST_UPDT_RUN_CYC_NO = {extrruncycle}
AND Clmln.CLM_LN_CHRG_AMT <> BClmln.CLM_LN_CHRG_AMT
AND Clmln.CLM_LN_ALW_AMT <> BClmln.CLM_LN_ALW_AMT
AND Clmln.CLM_LN_PAYBL_AMT <> BClmln.CLM_LN_PAYBL_AMT
""".strip()

df_SrcTrgtRowComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_SrcTrgtRowComp_rn = df_SrcTrgtRowComp.withColumn("INROWNUM", F.row_number().over(Window.orderBy(F.lit(1))))

df_research = df_SrcTrgtRowComp_rn.select(
    "SRC_SYS_CD",
    "CLM_ID",
    "CLM_LN_SEQ_NO",
    "CLM_LN_CHRG_AMT",
    "CLM_LN_ALW_AMT",
    "CLM_LN_PAYBL_AMT"
)

df_notify = df_SrcTrgtRowComp_rn.filter(F.col("INROWNUM") == 1).select(
    F.lit("COLUMN SUM BALANCING IDS - DATAMART CLM DM CLM LN OUT OF TOLERANCE").alias("NOTIFICATION")
)
df_notify = df_notify.withColumn("NOTIFICATION", F.rpad(F.col("NOTIFICATION"), 70, " ")).select("NOTIFICATION")

write_files(
    df_research,
    f"{adls_path}/balancing/research/IdsWebdmClmLnColumnSumResearch.dat.{runid}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

write_files(
    df_notify,
    f"{adls_path}/balancing/notify/ClmLnBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)