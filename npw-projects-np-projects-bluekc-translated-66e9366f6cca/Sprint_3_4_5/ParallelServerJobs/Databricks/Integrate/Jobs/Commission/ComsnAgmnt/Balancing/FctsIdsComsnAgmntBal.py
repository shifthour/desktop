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
# MAGIC CALLED BY:     FctsIdsComsnAgmntBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                        Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ---------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              05/29/2007           3264                              Originally Programmed                                    devlIDS30           
# MAGIC 
# MAGIC Parikshith Chada               8/22/2007         3264                              Modified the balancing process,                       devlIDS30                    Steph Goddard            09/17/2007
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
from pyspark.sql.functions import col, lit, row_number, rpad
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


RunID = get_widget_value('RunID','')
ToleranceCd = get_widget_value('ToleranceCd','')
IDSOwner = get_widget_value('IDSOwner','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"""
SELECT
COMSN_AGMNT.SRC_SYS_CD_SK,
COMSN_AGMNT.COMSN_ARGMT_ID,
COMSN_AGMNT.EFF_DT_SK,
COMSN_AGMNT.AGNT_ID,
B_COMSN_AGMNT.SRC_SYS_CD_SK,
B_COMSN_AGMNT.COMSN_ARGMT_ID,
B_COMSN_AGMNT.EFF_DT_SK,
B_COMSN_AGMNT.AGNT_ID
FROM {IDSOwner}.COMSN_AGMNT COMSN_AGMNT
FULL OUTER JOIN {IDSOwner}.B_COMSN_AGMNT B_COMSN_AGMNT
ON COMSN_AGMNT.SRC_SYS_CD_SK = B_COMSN_AGMNT.SRC_SYS_CD_SK
AND COMSN_AGMNT.COMSN_ARGMT_ID = B_COMSN_AGMNT.COMSN_ARGMT_ID
AND COMSN_AGMNT.EFF_DT_SK = B_COMSN_AGMNT.EFF_DT_SK
AND COMSN_AGMNT.AGNT_ID = B_COMSN_AGMNT.AGNT_ID
WHERE COMSN_AGMNT.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
"""
df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_missing = df_SrcTrgtComp.select(
    col("SRC_SYS_CD_SK").alias("SRC_SRC_SYS_CD_SK"),
    col("COMSN_ARGMT_ID").alias("SRC_COMSN_ARGMT_ID"),
    col("EFF_DT_SK").alias("SRC_EFF_DT_SK"),
    col("AGNT_ID").alias("SRC_AGNT_ID"),
    col("SRC_SYS_CD_SK_1").alias("TRGT_SRC_SYS_CD_SK"),
    col("COMSN_ARGMT_ID_1").alias("TRGT_COMSN_ARGMT_ID"),
    col("EFF_DT_SK_1").alias("TRGT_EFF_DT_SK"),
    col("AGNT_ID_1").alias("TRGT_AGNT_ID")
)

df_research = df_missing.filter(
    col("SRC_SRC_SYS_CD_SK").isNull()
    | col("SRC_COMSN_ARGMT_ID").isNull()
    | col("SRC_EFF_DT_SK").isNull()
    | col("SRC_AGNT_ID").isNull()
    | col("TRGT_SRC_SYS_CD_SK").isNull()
    | col("TRGT_COMSN_ARGMT_ID").isNull()
    | col("TRGT_EFF_DT_SK").isNull()
    | col("TRGT_AGNT_ID").isNull()
)
df_research_final = df_research.select(
    col("TRGT_SRC_SYS_CD_SK"),
    col("TRGT_COMSN_ARGMT_ID"),
    rpad(col("TRGT_EFF_DT_SK"), 10, " ").alias("TRGT_EFF_DT_SK"),
    col("TRGT_AGNT_ID"),
    col("SRC_SRC_SYS_CD_SK"),
    col("SRC_COMSN_ARGMT_ID"),
    rpad(col("SRC_EFF_DT_SK"), 10, " ").alias("SRC_EFF_DT_SK"),
    col("SRC_AGNT_ID")
)
write_files(
    df_research_final,
    f"{adls_path}/balancing/research/FctsIdsComsnAgmntResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_notify_temp = df_missing.withColumn(
    "rn",
    row_number().over(Window.orderBy(lit(1)))
)
df_notify = df_notify_temp.filter(
    (col("rn") == 1) & (ToleranceCd == "OUT")
)
df_notify_final = df_notify.select(
    rpad(
        lit("ROW COUNT BALANCING FACETS - IDS COMSN AGMNT OUT OF TOLERANCE"),
        70,
        " "
    ).alias("NOTIFICATION")
)
write_files(
    df_notify_final,
    f"{adls_path}/balancing/notify/CommissionsBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)