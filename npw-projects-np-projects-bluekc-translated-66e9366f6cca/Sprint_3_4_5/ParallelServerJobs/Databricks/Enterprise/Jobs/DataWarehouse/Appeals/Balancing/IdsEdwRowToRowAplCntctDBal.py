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
# MAGIC Bhoomi Dasari                 10/02/2007         3028                              Originally Programmed                            devlEDW10                     Steph Goddard             10/18/2007

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
from pyspark.sql.functions import col, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


EDWOwner = get_widget_value("EDWOwner","")
edw_secret_name = get_widget_value("edw_secret_name","")
ExtrRunCycle = get_widget_value("ExtrRunCycle","")
RunID = get_widget_value("RunID","")

jdbc_url, jdbc_props = get_db_config(edw_secret_name)

missing_query = f"""
SELECT
APL_CNTCT_D.SRC_SYS_CD,
APL_CNTCT_D.APL_ID,
APL_CNTCT_D.APL_CNTCT_SEQ_NO,
APL_CNTCT_D.APPEAL_CONTACT_IDENTIFIER
FROM {EDWOwner}.APL_CNTCT_D APL_CNTCT_D FULL OUTER JOIN {EDWOwner}.B_APL_CNTCT_D B_APL_CNTCT_D
ON APL_CNTCT_D.SRC_SYS_CD = B_APL_CNTCT_D.SRC_SYS_CD
AND APL_CNTCT_D.APL_ID = B_APL_CNTCT_D.APL_ID
AND APL_CNTCT_D.APL_CNTCT_SEQ_NO = B_APL_CNTCT_D.APL_CNTCT_SEQ_NO
WHERE
APL_CNTCT_D.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
AND
(APL_CNTCT_D.APPEAL_CONTACT_IDENTIFIER <> B_APL_CNTCT_D.APL_CNTCT_ID)
"""

df_SrcTrgtRowComp_Missing = (
    spark.read.format("jdbc")
        .option("url", jdbc_url)
        .options(**jdbc_props)
        .option("query", missing_query)
        .load()
)

match_query = f"""
SELECT
APL_CNTCT_D.APPEAL_CONTACT_IDENTIFIER
FROM {EDWOwner}.APL_CNTCT_D APL_CNTCT_D INNER JOIN {EDWOwner}.B_APL_CNTCT_D B_APL_CNTCT_D
ON APL_CNTCT_D.SRC_SYS_CD = B_APL_CNTCT_D.SRC_SYS_CD
AND APL_CNTCT_D.APL_ID = B_APL_CNTCT_D.APL_ID
AND APL_CNTCT_D.APL_CNTCT_SEQ_NO = B_APL_CNTCT_D.APL_CNTCT_SEQ_NO
WHERE
APL_CNTCT_D.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
AND APL_CNTCT_D.APPEAL_CONTACT_IDENTIFIER = B_APL_CNTCT_D.APL_CNTCT_ID
"""

df_SrcTrgtRowComp_Match = (
    spark.read.format("jdbc")
        .option("url", jdbc_url)
        .options(**jdbc_props)
        .option("query", match_query)
        .load()
)

write_files(
    df_SrcTrgtRowComp_Match.select("APPEAL_CONTACT_IDENTIFIER"),
    f"{adls_path}/balancing/sync/AplCntctDBalancingTotalMatch.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)

df_Research = df_SrcTrgtRowComp_Missing.select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("APL_ID").alias("APL_ID"),
    col("APL_CNTCT_SEQ_NO").alias("APL_CNTCT_SEQ_NO"),
    col("APPEAL_CONTACT_IDENTIFIER").alias("APPEAL_CONTACT_IDENTIFIER")
)

df_Notify = df_SrcTrgtRowComp_Missing.limit(1).select(
    lit("ROW TO ROW BALANCING IDS - EDW APL CNTCT D OUT OF TOLERANCE").alias("NOTIFICATION")
)
df_Notify = df_Notify.withColumn("NOTIFICATION", rpad(col("NOTIFICATION"), 70, " "))

write_files(
    df_Notify.select("NOTIFICATION"),
    f"{adls_path}/balancing/notify/AplBalancingNotification.dat",
    ",",
    "append",
    False,
    False,
    "\"",
    None
)

file_path_Research = f"{adls_path}/balancing/research/IdsEdwAplCntctDRowToRowResearch.dat.{RunID}"
write_files(
    df_Research.select("SRC_SYS_CD","APL_ID","APL_CNTCT_SEQ_NO","APPEAL_CONTACT_IDENTIFIER"),
    file_path_Research,
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)