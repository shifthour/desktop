# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 02/01/08 12:08:08 Batch  14642_43698 INIT bckcetl edw10 dsadm dsadm
# MAGIC ^1_1 11/02/07 14:51:45 Batch  14551_53511 PROMOTE bckcetl edw10 dsadm bls for on
# MAGIC ^1_1 11/02/07 14:44:15 Batch  14551_53058 INIT bckcett testEDW10 dsadm bls for on
# MAGIC ^1_1 11/01/07 12:56:04 Batch  14550_46572 PROMOTE bckcett testEDW10 u03651 steffy
# MAGIC ^1_1 11/01/07 12:51:30 Batch  14550_46292 INIT bckcett devlEDW10 u03651 steffy
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:          IdsEdwRowToRowUmIpFBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target tables are compared on the specified column to see if there are any discrepancies
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               07/12/2007          3264                              Originally Programmed                           devlEDW10              Steph Goddard             10/26/2007

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
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
RunID = get_widget_value('RunID','')

jdbc_url, jdbc_props = get_db_config(edw_secret_name)

extract_query_missing = f"""
SELECT
UM_IP_F.SRC_SYS_CD,
UM_IP_F.UM_REF_ID,
UM_IP_F.UM_IP_CUR_TREAT_CAT_CD,
UM_IP_F.UM_IP_STTUS_CD,
UM_IP_F.IP_PRI_DIAG_CD
FROM {EDWOwner}.UM_IP_F UM_IP_F 
FULL OUTER JOIN {EDWOwner}.B_UM_IP_F B_UM_IP_F
ON UM_IP_F.SRC_SYS_CD = B_UM_IP_F.SRC_SYS_CD
AND UM_IP_F.UM_REF_ID = B_UM_IP_F.UM_REF_ID
WHERE UM_IP_F.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
AND (
    UM_IP_F.UM_IP_CUR_TREAT_CAT_CD <> B_UM_IP_F.UM_IP_CUR_TREAT_CAT_CD
    OR UM_IP_F.UM_IP_STTUS_CD <> B_UM_IP_F.UM_IP_STTUS_CD
    OR UM_IP_F.IP_PRI_DIAG_CD <> B_UM_IP_F.PRI_DIAG_CD
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
UM_IP_F.UM_IP_CUR_TREAT_CAT_CD,
UM_IP_F.UM_IP_STTUS_CD,
UM_IP_F.IP_PRI_DIAG_CD
FROM {EDWOwner}.UM_IP_F UM_IP_F
INNER JOIN {EDWOwner}.B_UM_IP_F B_UM_IP_F
ON UM_IP_F.SRC_SYS_CD = B_UM_IP_F.SRC_SYS_CD
AND UM_IP_F.UM_REF_ID = B_UM_IP_F.UM_REF_ID
WHERE UM_IP_F.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
AND UM_IP_F.UM_IP_CUR_TREAT_CAT_CD = B_UM_IP_F.UM_IP_CUR_TREAT_CAT_CD
AND UM_IP_F.UM_IP_STTUS_CD = B_UM_IP_F.UM_IP_STTUS_CD
AND UM_IP_F.IP_PRI_DIAG_CD = B_UM_IP_F.PRI_DIAG_CD
"""

df_SrcTrgtRowComp_Match = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_match)
    .load()
)

df_SrcTrgtSyncFile = df_SrcTrgtRowComp_Match.select(
    "UM_IP_CUR_TREAT_CAT_CD",
    "UM_IP_STTUS_CD",
    "IP_PRI_DIAG_CD"
)

write_files(
    df_SrcTrgtSyncFile,
    f"{adls_path}/balancing/sync/UmIpFBalancingTotalMatch.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

df_TransformLogic_Research = df_SrcTrgtRowComp_Missing.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("UM_REF_ID").alias("UM_REF_ID"),
    F.col("UM_IP_CUR_TREAT_CAT_CD").alias("UM_IP_CUR_TREAT_CAT_CD"),
    F.col("UM_IP_STTUS_CD").alias("UM_IP_STTUS_CD"),
    F.col("IP_PRI_DIAG_CD").alias("IP_PRI_DIAG_CD")
)

df_TransformLogic_Notify = df_SrcTrgtRowComp_Missing.limit(1).select(
    rpad(
        lit("ROW TO ROW BALANCING IDS - EDW UM IP F OUT OF TOLERANCE"), 
        70, 
        " "
    ).alias("NOTIFICATION")
)

write_files(
    df_TransformLogic_Notify,
    f"{adls_path}/balancing/notify/MedMgtBalancingNotification.txt",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

write_files(
    df_TransformLogic_Research,
    f"{adls_path}/balancing/research/IdsEdwUmIpFRowToRowResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)