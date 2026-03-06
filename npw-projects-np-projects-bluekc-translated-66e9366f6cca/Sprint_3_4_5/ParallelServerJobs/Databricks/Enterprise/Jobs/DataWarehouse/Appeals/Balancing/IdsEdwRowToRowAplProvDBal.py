# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 11/28/07 12:47:07 Batch  14577_46088 PROMOTE bckcetl edw10 dsadm bls for hs
# MAGIC ^1_1 11/28/07 11:24:20 Batch  14577_41063 INIT bckcett testEDW10 dsadm bls for hs
# MAGIC ^1_1 11/27/07 15:06:26 Batch  14576_54393 PROMOTE bckcett testEDW10 u03651 steph for Hugh
# MAGIC ^1_1 11/27/07 14:52:16 Batch  14576_53540 INIT bckcett devlEDW10 u03651 steffy
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:          IdsEdwRowToRowUmSvcFBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target tables are compared on the specified column to see if there are any discrepancies
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               07/12/2007          3264                              Originally Programmed                           devlEDW10               Steph Goddard            10/18/2007

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
from pyspark.sql.functions import col, rpad
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
RunID = get_widget_value('RunID','')

jdbc_url, jdbc_props = get_db_config(edw_secret_name)

query_missing = f"""
SELECT
APL_PROV_D.SRC_SYS_CD,
APL_PROV_D.APL_ID,
APL_PROV_D.APL_PROV_SEQ_NO,
APL_PROV_D.PROV_ID
FROM {EDWOwner}.APL_PROV_D APL_PROV_D
FULL OUTER JOIN {EDWOwner}.B_APL_PROV_D B_APL_PROV_D
ON APL_PROV_D.SRC_SYS_CD = B_APL_PROV_D.SRC_SYS_CD
AND APL_PROV_D.APL_ID = B_APL_PROV_D.APL_ID
AND APL_PROV_D.APL_PROV_SEQ_NO = B_APL_PROV_D.APL_PROV_SEQ_NO
WHERE 
APL_PROV_D.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
AND (APL_PROV_D.PROV_ID <> B_APL_PROV_D.PROV_ID)
"""

df_SrcTrgtRowComp_Missing = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_missing)
    .load()
)

query_match = f"""
SELECT
APL_PROV_D.PROV_ID
FROM {EDWOwner}.APL_PROV_D APL_PROV_D
INNER JOIN {EDWOwner}.B_APL_PROV_D B_APL_PROV_D
ON APL_PROV_D.SRC_SYS_CD = B_APL_PROV_D.SRC_SYS_CD
AND APL_PROV_D.APL_ID = B_APL_PROV_D.APL_ID
AND APL_PROV_D.APL_PROV_SEQ_NO = B_APL_PROV_D.APL_PROV_SEQ_NO
WHERE 
APL_PROV_D.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
AND APL_PROV_D.PROV_ID = B_APL_PROV_D.PROV_ID
"""

df_SrcTrgtRowComp_Match = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_match)
    .load()
)

df_SrcTrgtRowComp_Match_final = df_SrcTrgtRowComp_Match.select("PROV_ID")
write_files(
    df_SrcTrgtRowComp_Match_final,
    f"{adls_path}/balancing/sync/AplProvDBalancingTotalMatch.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_TransformLogic_Research = df_SrcTrgtRowComp_Missing.select(
    "SRC_SYS_CD",
    "APL_ID",
    "APL_PROV_SEQ_NO",
    "PROV_ID"
)

df_TransformLogic_Notify = df_SrcTrgtRowComp_Missing.limit(1).selectExpr(
    "'ROW TO ROW BALANCING IDS - EDW APL PROV D OUT OF TOLERANCE' as NOTIFICATION"
)
df_TransformLogic_Notify = df_TransformLogic_Notify.withColumn("NOTIFICATION", rpad(col("NOTIFICATION"), 70, " "))

write_files(
    df_TransformLogic_Notify.select("NOTIFICATION"),
    f"{adls_path}/balancing/notify/AplBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_ResearchFile_final = df_TransformLogic_Research.select(
    "SRC_SYS_CD",
    "APL_ID",
    "APL_PROV_SEQ_NO",
    "PROV_ID"
)
write_files(
    df_ResearchFile_final,
    f"{adls_path}/balancing/research/IdsEdwAplProvDRowToRowResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)