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
# MAGIC CALLED BY:    IdsEdwAplBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This pulls data from the Concatenated Source Sequential file coming from IDS and loads into the B table in EDW
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                  Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------            ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Bhoomi Dasari                 10/02/2007         3028                              Originally Programmed                            devlEDW10                      Steph Goddard            10/18/2007

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

extract_query_missing = f"""
SELECT
APL_LINK_D.SRC_SYS_CD,
APL_LINK_D.APL_ID,
APL_LINK_D.APL_LINK_TYP_CD,
APL_LINK_D.APL_LINK_ID,
APL_LINK_D.APL_LINK_DESC,
APL_LINK_D.LAST_UPDT_DT_SK
FROM {EDWOwner}.APL_LINK_D APL_LINK_D
FULL OUTER JOIN {EDWOwner}.B_APL_LINK_D B_APL_LINK_D
ON APL_LINK_D.SRC_SYS_CD = B_APL_LINK_D.SRC_SYS_CD
AND APL_LINK_D.APL_ID = B_APL_LINK_D.APL_ID
AND APL_LINK_D.APL_LINK_TYP_CD = B_APL_LINK_D.APL_LINK_TYP_CD
AND APL_LINK_D.APL_LINK_ID = B_APL_LINK_D.APL_LINK_ID
WHERE APL_LINK_D.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
AND (
  APL_LINK_D.APL_LINK_DESC <> B_APL_LINK_D.APL_LINK_DESC
  OR APL_LINK_D.LAST_UPDT_DT_SK <> B_APL_LINK_D.LAST_UPDT_DT_SK
)
"""

df_SrcTrgtRowComp_Missing = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_missing)
    .load()
)

extract_query_match = f"""
SELECT
APL_LINK_D.APL_LINK_DESC,
APL_LINK_D.LAST_UPDT_DT_SK
FROM {EDWOwner}.APL_LINK_D APL_LINK_D
INNER JOIN {EDWOwner}.B_APL_LINK_D B_APL_LINK_D
ON APL_LINK_D.SRC_SYS_CD = B_APL_LINK_D.SRC_SYS_CD
AND APL_LINK_D.APL_ID = B_APL_LINK_D.APL_ID
AND APL_LINK_D.APL_LINK_TYP_CD = B_APL_LINK_D.APL_LINK_TYP_CD
AND APL_LINK_D.APL_LINK_ID = B_APL_LINK_D.APL_LINK_ID
WHERE APL_LINK_D.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
AND APL_LINK_D.APL_LINK_DESC = B_APL_LINK_D.APL_LINK_DESC
AND APL_LINK_D.LAST_UPDT_DT_SK = B_APL_LINK_D.LAST_UPDT_DT_SK
"""

df_SrcTrgtRowComp_Match = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_match)
    .load()
)

df_SrcTrgtRowComp_Match_final = df_SrcTrgtRowComp_Match.select(
    rpad("APL_LINK_DESC", <...>, " ").alias("APL_LINK_DESC"),
    rpad("LAST_UPDT_DT_SK", 10, " ").alias("LAST_UPDT_DT_SK")
)

write_files(
    df_SrcTrgtRowComp_Match_final,
    f"{adls_path}/balancing/sync/AplLinkDBalancingTotalMatch.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_Research = df_SrcTrgtRowComp_Missing.select(
    "SRC_SYS_CD",
    "APL_ID",
    "APL_LINK_TYP_CD",
    "APL_LINK_ID",
    "APL_LINK_DESC",
    "LAST_UPDT_DT_SK"
)

df_Research_final = df_Research.select(
    rpad("SRC_SYS_CD", <...>, " ").alias("SRC_SYS_CD"),
    rpad("APL_ID", <...>, " ").alias("APL_ID"),
    rpad("APL_LINK_TYP_CD", <...>, " ").alias("APL_LINK_TYP_CD"),
    rpad("APL_LINK_ID", <...>, " ").alias("APL_LINK_ID"),
    rpad("APL_LINK_DESC", <...>, " ").alias("APL_LINK_DESC"),
    rpad("LAST_UPDT_DT_SK", 10, " ").alias("LAST_UPDT_DT_SK")
)

df_Notify = df_SrcTrgtRowComp_Missing.limit(1).withColumn(
    "NOTIFICATION",
    lit("ROW TO ROW BALANCING IDS - EDW APL LINK D OUT OF TOLERANCE")
)

df_Notify_final = df_Notify.select(
    rpad("NOTIFICATION", 70, " ").alias("NOTIFICATION")
)

write_files(
    df_Notify_final,
    f"{adls_path}/balancing/notify/AplBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

write_files(
    df_Research_final,
    f"{adls_path}/balancing/research/IdsEdwAplLinkDRowToRowResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)