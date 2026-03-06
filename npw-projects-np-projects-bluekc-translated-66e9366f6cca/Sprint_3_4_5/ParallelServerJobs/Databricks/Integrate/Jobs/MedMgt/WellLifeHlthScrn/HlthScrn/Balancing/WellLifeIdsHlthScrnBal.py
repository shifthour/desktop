# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 02/14/08 10:54:44 Batch  14655_39498 PROMOTE bckcetl ids20 dsadm bls for sa
# MAGIC ^1_1 02/14/08 10:45:23 Batch  14655_38725 INIT bckcett testIDS dsadm bls for sa
# MAGIC ^1_2 02/12/08 08:52:08 Batch  14653_31936 PROMOTE bckcett testIDS u03651 steph for Sharon
# MAGIC ^1_2 02/12/08 08:49:32 Batch  14653_31775 INIT bckcett devlIDS u03651 steffy
# MAGIC ^1_1 01/31/08 10:56:11 Batch  14641_39374 INIT bckcett devlIDS u03651 steffy
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:    WellLifeIdsHlthScrnBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS: 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                       Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                   ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              2008-01-23        3036                              Originally Programmed                                      devlIDS30                     Steph Goddard            01/28/2008

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
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lit, row_number, rpad
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


RunID = get_widget_value("RunID","")
ToleranceCd = get_widget_value("ToleranceCd","")
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
ExtrRunCycle = get_widget_value("ExtrRunCycle","")

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"""
SELECT 
HLTH_SCRN.SRC_SYS_CD_SK AS SRC_SRC_SYS_CD_SK,
HLTH_SCRN.MBR_UNIQ_KEY AS SRC_MBR_UNIQ_KEY,
HLTH_SCRN.SCRN_DT_SK AS SRC_SCRN_DT_SK,
B_HLTH_SCRN.SRC_SYS_CD_SK AS TRGT_SRC_SYS_CD_SK,
B_HLTH_SCRN.MBR_UNIQ_KEY AS TRGT_MBR_UNIQ_KEY,
B_HLTH_SCRN.SCRN_DT_SK AS TRGT_SCRN_DT_SK
FROM {IDSOwner}.HLTH_SCRN HLTH_SCRN
FULL OUTER JOIN {IDSOwner}.B_HLTH_SCRN B_HLTH_SCRN
ON HLTH_SCRN.SRC_SYS_CD_SK = B_HLTH_SCRN.SRC_SYS_CD_SK
AND HLTH_SCRN.MBR_UNIQ_KEY = B_HLTH_SCRN.MBR_UNIQ_KEY
AND HLTH_SCRN.SCRN_DT_SK = B_HLTH_SCRN.SCRN_DT_SK
WHERE HLTH_SCRN.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
"""
df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_Research = df_SrcTrgtComp.filter(
    (col("SRC_SRC_SYS_CD_SK").isNull())
    | (col("SRC_MBR_UNIQ_KEY").isNull())
    | (col("SRC_SCRN_DT_SK").isNull())
    | (col("TRGT_SRC_SYS_CD_SK").isNull())
    | (col("TRGT_MBR_UNIQ_KEY").isNull())
    | (col("TRGT_SCRN_DT_SK").isNull())
)
df_Research = df_Research.withColumn("TRGT_SCRN_DT_SK", rpad(col("TRGT_SCRN_DT_SK"), 10, " "))
df_Research = df_Research.withColumn("SRC_SCRN_DT_SK", rpad(col("SRC_SCRN_DT_SK"), 10, " "))
df_Research_final = df_Research.select(
    "TRGT_SRC_SYS_CD_SK",
    "TRGT_MBR_UNIQ_KEY",
    "TRGT_SCRN_DT_SK",
    "SRC_SRC_SYS_CD_SK",
    "SRC_MBR_UNIQ_KEY",
    "SRC_SCRN_DT_SK"
)

df_SrcTrgtComp_ordered = df_SrcTrgtComp.withColumn("_rownum", row_number().over(Window.orderBy(lit(1))))
df_Notify = df_SrcTrgtComp_ordered.filter(
    (col("_rownum") == 1) & (lit(ToleranceCd) == lit("OUT"))
).select(
    rpad(lit("ROW COUNT BALANCING WELL LIFE - IDS HLTH SCRN OUT OF TOLERANCE"), 70, " ").alias("NOTIFICATION")
)
df_Notify_final = df_Notify.select("NOTIFICATION")

write_files(
    df_Research_final,
    f"{adls_path}/balancing/research/WellLifeIdsHlthScrnResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

write_files(
    df_Notify_final,
    f"{adls_path}/balancing/notify/MedMgtBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)