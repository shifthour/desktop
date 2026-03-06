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
# MAGIC Bhoomi Dasari                 10/02/2007         3028                              Originally Programmed                            devlEDW10                      Steph Goddard           10/18/2007

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

extract_query_missing = f"""
SELECT
APL_LVL_LTR_PRT_D.SRC_SYS_CD,
APL_LVL_LTR_PRT_D.APL_ID,
APL_LVL_LTR_PRT_D.APL_LVL_SEQ_NO,
APL_LVL_LTR_PRT_D.APL_LVL_LTR_STYLE_CD,
APL_LVL_LTR_PRT_D.APL_LVL_LTR_SEQ_NO,
APL_LVL_LTR_PRT_D.APL_LVL_LTR_DEST_ID,
APL_LVL_LTR_PRT_D.APL_LVL_LTR_PRT_SEQ_NO,
APL_LVL_LTR_PRT_D.APL_LVL_LTR_PRT_DT_SK,
APL_LVL_LTR_PRT_D.LAST_UPDT_DT_SK
FROM {EDWOwner}.APL_LVL_LTR_PRT_D APL_LVL_LTR_PRT_D
FULL OUTER JOIN {EDWOwner}.B_APL_LTR_PRT_D B_APL_LTR_PRT_D
ON APL_LVL_LTR_PRT_D.SRC_SYS_CD = B_APL_LTR_PRT_D.SRC_SYS_CD
AND APL_LVL_LTR_PRT_D.APL_ID = B_APL_LTR_PRT_D.APL_ID
AND APL_LVL_LTR_PRT_D.APL_LVL_SEQ_NO = B_APL_LTR_PRT_D.APL_LVL_SEQ_NO
AND APL_LVL_LTR_PRT_D.APL_LVL_LTR_STYLE_CD = B_APL_LTR_PRT_D.APL_LVL_LTR_STYLE_CD
AND APL_LVL_LTR_PRT_D.APL_LVL_LTR_SEQ_NO = B_APL_LTR_PRT_D.APL_LVL_LTR_SEQ_NO
AND APL_LVL_LTR_PRT_D.APL_LVL_LTR_DEST_ID = B_APL_LTR_PRT_D.APL_LVL_LTR_DEST_ID
AND APL_LVL_LTR_PRT_D.APL_LVL_LTR_PRT_SEQ_NO = B_APL_LTR_PRT_D.APL_LVL_LTR_PRT_SEQ_NO
WHERE
APL_LVL_LTR_PRT_D.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
AND
(
  APL_LVL_LTR_PRT_D.APL_LVL_LTR_PRT_DT_SK <> B_APL_LTR_PRT_D.APL_LVL_LTR_PRT_DT_SK
  OR APL_LVL_LTR_PRT_D.LAST_UPDT_DT_SK <> B_APL_LTR_PRT_D.LAST_UPDT_DT_SK
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
APL_LVL_LTR_PRT_D.APL_LVL_LTR_PRT_DT_SK,
APL_LVL_LTR_PRT_D.LAST_UPDT_DT_SK
FROM {EDWOwner}.APL_LVL_LTR_PRT_D APL_LVL_LTR_PRT_D
INNER JOIN {EDWOwner}.B_APL_LTR_PRT_D B_APL_LTR_PRT_D
ON APL_LVL_LTR_PRT_D.SRC_SYS_CD = B_APL_LTR_PRT_D.SRC_SYS_CD
AND APL_LVL_LTR_PRT_D.APL_ID = B_APL_LTR_PRT_D.APL_ID
AND APL_LVL_LTR_PRT_D.APL_LVL_SEQ_NO = B_APL_LTR_PRT_D.APL_LVL_SEQ_NO
AND APL_LVL_LTR_PRT_D.APL_LVL_LTR_STYLE_CD = B_APL_LTR_PRT_D.APL_LVL_LTR_STYLE_CD
AND APL_LVL_LTR_PRT_D.APL_LVL_LTR_SEQ_NO = B_APL_LTR_PRT_D.APL_LVL_LTR_SEQ_NO
AND APL_LVL_LTR_PRT_D.APL_LVL_LTR_DEST_ID = B_APL_LTR_PRT_D.APL_LVL_LTR_DEST_ID
AND APL_LVL_LTR_PRT_D.APL_LVL_LTR_PRT_SEQ_NO = B_APL_LTR_PRT_D.APL_LVL_LTR_PRT_SEQ_NO
WHERE
APL_LVL_LTR_PRT_D.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
AND APL_LVL_LTR_PRT_D.APL_LVL_LTR_PRT_DT_SK = B_APL_LTR_PRT_D.APL_LVL_LTR_PRT_DT_SK
AND APL_LVL_LTR_PRT_D.LAST_UPDT_DT_SK = B_APL_LTR_PRT_D.LAST_UPDT_DT_SK
"""

df_SrcTrgtRowComp_Match = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_match)
    .load()
)

df_SrcTrgtRowComp_Match_write = df_SrcTrgtRowComp_Match.select("APL_LVL_LTR_PRT_DT_SK","LAST_UPDT_DT_SK")

write_files(
    df_SrcTrgtRowComp_Match_write,
    f"{adls_path}/balancing/sync/AplLvlLtrPrtDBalancingTotalMatch.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_TransformLogic_Notify_temp = df_SrcTrgtRowComp_Missing.limit(1).select().withColumn(
    "NOTIFICATION",
    lit("ROW TO ROW BALANCING IDS - EDW APL LVL LTR PRT D OUT OF TOLERANCE")
)
df_TransformLogic_Notify = df_TransformLogic_Notify_temp.withColumn(
    "NOTIFICATION", rpad(col("NOTIFICATION"), 70, " ")
)

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

df_TransformLogic_Research_temp = df_SrcTrgtRowComp_Missing.select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("APL_ID").alias("APL_ID"),
    col("APL_LVL_SEQ_NO").alias("APL_LVL_SEQ_NO"),
    col("APL_LVL_LTR_STYLE_CD").alias("APL_LVL_LTR_STYLE_CD"),
    col("APL_LVL_LTR_SEQ_NO").alias("APL_LVL_LTR_SEQ_NO"),
    col("APL_LVL_LTR_DEST_ID").alias("APL_LVL_LTR_DEST_ID"),
    col("APL_LVL_LTR_PRT_SEQ_NO").alias("APL_LVL_LTR_PRT_SEQ_NO"),
    col("APL_LVL_LTR_PRT_DT_SK").alias("APL_LVL_LTR_PRT_DT_SK"),
    col("LAST_UPDT_DT_SK").alias("LAST_UPDT_DT_SK")
)

df_TransformLogic_Research = (
    df_TransformLogic_Research_temp
    .withColumn("APL_LVL_LTR_PRT_DT_SK", rpad(col("APL_LVL_LTR_PRT_DT_SK"), 10, " "))
    .withColumn("LAST_UPDT_DT_SK", rpad(col("LAST_UPDT_DT_SK"), 10, " "))
)

write_files(
    df_TransformLogic_Research.select(
        "SRC_SYS_CD",
        "APL_ID",
        "APL_LVL_SEQ_NO",
        "APL_LVL_LTR_STYLE_CD",
        "APL_LVL_LTR_SEQ_NO",
        "APL_LVL_LTR_DEST_ID",
        "APL_LVL_LTR_PRT_SEQ_NO",
        "APL_LVL_LTR_PRT_DT_SK",
        "LAST_UPDT_DT_SK"
    ),
    f"{adls_path}/balancing/research/IdsEdwAplLvlLtrPrtDRowToRowResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)