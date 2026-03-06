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
from pyspark.sql import functions as F
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
RunID = get_widget_value('RunID','')

jdbc_url, jdbc_props = get_db_config(edw_secret_name)

missing_query = f"""SELECT
APL_LVL_D.SRC_SYS_CD,
APL_LVL_D.APL_ID,
APL_LVL_D.APL_LVL_SEQ_NO,
APL_LVL_D.APL_LVL_CUR_STTUS_DT_SK,
APL_LVL_D.APL_LVL_CUR_STTUS_SEQ_NO,
APL_LVL_D.APL_LVL_DCSN_DT_SK


FROM 
{EDWOwner}.APL_LVL_D APL_LVL_D FULL OUTER JOIN {EDWOwner}.B_APL_LVL_D B_APL_LVL_D
ON APL_LVL_D.SRC_SYS_CD = B_APL_LVL_D.SRC_SYS_CD 
AND APL_LVL_D.APL_ID = B_APL_LVL_D.APL_ID 
AND APL_LVL_D.APL_LVL_SEQ_NO = B_APL_LVL_D.APL_LVL_SEQ_NO


WHERE 
APL_LVL_D.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
AND  
(APL_LVL_D.APL_LVL_CUR_STTUS_DT_SK <> B_APL_LVL_D.APL_LVL_CUR_STTUS_DT_SK
OR APL_LVL_D.APL_LVL_CUR_STTUS_SEQ_NO <> B_APL_LVL_D.APL_LVL_CUR_STTUS_SEQ_NO
OR APL_LVL_D.APL_LVL_DCSN_DT_SK <> B_APL_LVL_D.APL_LVL_DCSN_DT_SK)
"""

df_SrcTrgtRowComp_Missing = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", missing_query)
    .load()
)

match_query = f"""SELECT
APL_LVL_D.APL_LVL_CUR_STTUS_DT_SK,
APL_LVL_D.APL_LVL_CUR_STTUS_SEQ_NO,
APL_LVL_D.APL_LVL_DCSN_DT_SK


FROM 
{EDWOwner}.APL_LVL_D APL_LVL_D INNER JOIN {EDWOwner}.B_APL_LVL_D B_APL_LVL_D
ON APL_LVL_D.SRC_SYS_CD = B_APL_LVL_D.SRC_SYS_CD 
AND APL_LVL_D.APL_ID = B_APL_LVL_D.APL_ID 
AND APL_LVL_D.APL_LVL_SEQ_NO = B_APL_LVL_D.APL_LVL_SEQ_NO


WHERE 
APL_LVL_D.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
AND APL_LVL_D.APL_LVL_CUR_STTUS_DT_SK = B_APL_LVL_D.APL_LVL_CUR_STTUS_DT_SK
AND APL_LVL_D.APL_LVL_CUR_STTUS_SEQ_NO = B_APL_LVL_D.APL_LVL_CUR_STTUS_SEQ_NO
AND APL_LVL_D.APL_LVL_DCSN_DT_SK = B_APL_LVL_D.APL_LVL_DCSN_DT_SK
"""

df_SrcTrgtRowComp_Match = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", match_query)
    .load()
)

df_SrcTrgtRowComp_Match_final = df_SrcTrgtRowComp_Match.select(
    "APL_LVL_CUR_STTUS_DT_SK",
    "APL_LVL_CUR_STTUS_SEQ_NO",
    "APL_LVL_DCSN_DT_SK"
)

write_files(
    df_SrcTrgtRowComp_Match_final,
    f"{adls_path}/balancing/sync/AplLvlDBalancingTotalMatch.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)

windowSpec = Window.orderBy(F.monotonically_increasing_id())
df_TransformLogic_base = df_SrcTrgtRowComp_Missing.withColumn("row_number", F.row_number().over(windowSpec))

df_Notify = df_TransformLogic_base.filter(F.col("row_number") == 1).select(
    F.rpad(F.lit("ROW TO ROW BALANCING IDS - EDW APL LEVEL D OUT OF TOLERANCE"), 70, " ").alias("NOTIFICATION")
)

df_Research = df_TransformLogic_base.select(
    F.col("SRC_SYS_CD"),
    F.col("APL_ID"),
    F.col("APL_LVL_SEQ_NO"),
    F.rpad(F.col("APL_LVL_CUR_STTUS_DT_SK"), 10, " ").alias("APL_LVL_CUR_STTUS_DT_SK"),
    F.col("APL_LVL_CUR_STTUS_SEQ_NO"),
    F.rpad(F.col("APL_LVL_DCSN_DT_SK"), 10, " ").alias("APL_LVL_DCSN_DT_SK")
)

write_files(
    df_Notify,
    f"{adls_path}/balancing/notify/AplBalancingNotification.dat",
    ",",
    "append",
    False,
    False,
    "\"",
    None
)

write_files(
    df_Research,
    f"{adls_path}/balancing/research/IdsEdwAplLvlDRowToRowResearch.dat.{RunID}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)