# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 01/30/08 05:00:10 Batch  14640_18019 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 10/31/07 14:11:20 Batch  14549_51083 PROMOTE bckcetl ids20 dsadm bls for on
# MAGIC ^1_1 10/31/07 12:55:15 Batch  14549_46521 PROMOTE bckcetl ids20 dsadm bls for on
# MAGIC ^1_1 10/31/07 12:32:28 Batch  14549_45152 INIT bckcett testIDS30 dsadm bls for on
# MAGIC ^1_1 10/26/07 08:07:18 Batch  14544_29241 PROMOTE bckcett testIDS30 u03651 steffy
# MAGIC ^1_1 10/26/07 07:46:17 Batch  14544_27982 INIT bckcett devlIDS30 u03651 steffy
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:    FctsIdsComsnSchdTierBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS: 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                      Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                 ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              2007-05-31          3264                              Originally Programmed                                    devlIDS30       
# MAGIC 
# MAGIC Parikshith Chada               8/22/2007         3264                              Modified the balancing process,                     devlIDS30                   Steph Goddard             09/18/2007
# MAGIC                                                                                                           by changing snapshot file to snapshot table

# MAGIC File checked later for rows and email to on-call
# MAGIC Detail file for on-call to research errors
# MAGIC Rows with Failed Comparison on Specified columns
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import StructType, StructField, StringType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


RunID = get_widget_value("RunID", "")
ToleranceCd = get_widget_value("ToleranceCd", "")
IDSOwner = get_widget_value("IDSOwner", "")
ExtrRunCycle = get_widget_value("ExtrRunCycle", "")
ids_secret_name = get_widget_value("ids_secret_name", "")

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"""
SELECT
 COMSN_SCHD_TIER.SRC_SYS_CD_SK AS SRC_SRC_SYS_CD_SK,
 COMSN_SCHD_TIER.COMSN_SCHD_ID AS SRC_COMSN_SCHD_ID,
 COMSN_SCHD_TIER.DURATN_EFF_DT_SK AS SRC_DURATN_EFF_DT_SK,
 COMSN_SCHD_TIER.DURATN_STRT_PERD_NO AS SRC_DURATN_STRT_PERD_NO,
 COMSN_SCHD_TIER.PRM_FROM_THRSHLD_AMT AS SRC_PRM_FROM_THRSHLD_AMT,
 B_COMSN_SCHD_TIER.SRC_SYS_CD_SK AS TRGT_SRC_SYS_CD_SK,
 B_COMSN_SCHD_TIER.COMSN_SCHD_ID AS TRGT_COMSN_SCHD_ID,
 B_COMSN_SCHD_TIER.DURATN_EFF_DT_SK AS TRGT_DURATN_EFF_DT_SK,
 B_COMSN_SCHD_TIER.DURATN_STRT_PERD_NO AS TRGT_DURATN_STRT_PERD_NO,
 B_COMSN_SCHD_TIER.PRM_FROM_THRSHLD_AMT AS TRGT_PRM_FROM_THRSHLD_AMT
FROM
 {IDSOwner}.COMSN_SCHD_TIER COMSN_SCHD_TIER
 FULL OUTER JOIN {IDSOwner}.B_COMSN_SCHD_TIER B_COMSN_SCHD_TIER
   ON COMSN_SCHD_TIER.SRC_SYS_CD_SK = B_COMSN_SCHD_TIER.SRC_SYS_CD_SK
   AND COMSN_SCHD_TIER.COMSN_SCHD_ID = B_COMSN_SCHD_TIER.COMSN_SCHD_ID
   AND COMSN_SCHD_TIER.DURATN_EFF_DT_SK = B_COMSN_SCHD_TIER.DURATN_EFF_DT_SK
   AND COMSN_SCHD_TIER.DURATN_STRT_PERD_NO = B_COMSN_SCHD_TIER.DURATN_STRT_PERD_NO
   AND COMSN_SCHD_TIER.PRM_FROM_THRSHLD_AMT = B_COMSN_SCHD_TIER.PRM_FROM_THRSHLD_AMT
WHERE
 COMSN_SCHD_TIER.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
"""

df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_Research = df_SrcTrgtComp.filter(
    (F.isnull("SRC_COMSN_SCHD_ID"))
    | (F.isnull("SRC_DURATN_EFF_DT_SK"))
    | (F.isnull("SRC_DURATN_STRT_PERD_NO"))
    | (F.isnull("SRC_PRM_FROM_THRSHLD_AMT"))
    | (F.isnull("SRC_SRC_SYS_CD_SK"))
    | (F.isnull("TRGT_COMSN_SCHD_ID"))
    | (F.isnull("TRGT_DURATN_EFF_DT_SK"))
    | (F.isnull("TRGT_DURATN_STRT_PERD_NO"))
    | (F.isnull("TRGT_PRM_FROM_THRSHLD_AMT"))
    | (F.isnull("TRGT_SRC_SYS_CD_SK"))
)

df_Research = df_Research.withColumn(
    "TRGT_DURATN_EFF_DT_SK", F.rpad(F.col("TRGT_DURATN_EFF_DT_SK"), 10, " ")
).withColumn(
    "SRC_DURATN_EFF_DT_SK", F.rpad(F.col("SRC_DURATN_EFF_DT_SK"), 10, " ")
).select(
    "TRGT_SRC_SYS_CD_SK",
    "TRGT_COMSN_SCHD_ID",
    "TRGT_DURATN_EFF_DT_SK",
    "TRGT_DURATN_STRT_PERD_NO",
    "TRGT_PRM_FROM_THRSHLD_AMT",
    "SRC_SRC_SYS_CD_SK",
    "SRC_COMSN_SCHD_ID",
    "SRC_DURATN_EFF_DT_SK",
    "SRC_DURATN_STRT_PERD_NO",
    "SRC_PRM_FROM_THRSHLD_AMT"
)

write_files(
    df_Research,
    f"{adls_path}/balancing/research/FctsIdsComsnSchdTierResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

w = Window.orderBy(F.monotonically_increasing_id())
df_temp_notify = df_SrcTrgtComp.withColumn("row_number", F.row_number().over(w))
df_Notify = df_temp_notify.filter((F.col("row_number") == 1) & (F.lit(ToleranceCd) == F.lit("OUT"))).select(
    F.lit("ROW COUNT BALANCING FACETS - IDS COMSN SCHD TIER OUT OF TOLERANCE").alias("NOTIFICATION")
)
df_Notify = df_Notify.withColumn("NOTIFICATION", F.rpad(F.col("NOTIFICATION"), 70, " "))

write_files(
    df_Notify,
    f"{adls_path}/balancing/notify/CommissionsBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)