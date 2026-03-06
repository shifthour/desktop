# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 11/01/07 16:40:40 Batch  14550_60068 PROMOTE bckcetl ids20 dsadm rc for brent
# MAGIC ^1_1 11/01/07 16:19:50 Batch  14550_58802 INIT bckcett testIDS30 dsadm rc for brent
# MAGIC ^1_2 10/29/07 15:58:44 Batch  14547_57529 PROMOTE bckcett testIDS30 u08717 Brent
# MAGIC ^1_2 10/29/07 15:35:38 Batch  14547_56143 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_1 10/29/07 14:48:31 Batch  14547_53315 INIT bckcett devlIDS30 u08717 Brent
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:          FctsIdsCustSvcTaskNoteLnBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                      Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               06/22/2007          3264                              Originally Programmed                                   devlIDS30     
# MAGIC 
# MAGIC Parikshith Chada               8/24/2007         3264                              Modified the balancing process,                       devlIDS30                 Steph Goddard              09/14/2007
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
from pyspark.sql.types import StructType, StructField, StringType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


RunID = get_widget_value("RunID", "")
ToleranceCd = get_widget_value("ToleranceCd", "")
ExtrRunCycle = get_widget_value("ExtrRunCycle", "")
IDSOwner = get_widget_value("IDSOwner", "")
ids_secret_name = get_widget_value("ids_secret_name", "")

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = f"""
SELECT
CUST_SVC_TASK_NOTE_LN.SRC_SYS_CD_SK AS SRC_SRC_SYS_CD_SK,
CUST_SVC_TASK_NOTE_LN.CUST_SVC_ID AS SRC_CUST_SVC_ID,
CUST_SVC_TASK_NOTE_LN.TASK_SEQ_NO AS SRC_TASK_SEQ_NO,
CUST_SVC_TASK_NOTE_LN.CUST_SVC_TASK_NOTE_LOC_CD_SK AS SRC_CUST_SVC_TASK_NOTE_LOC_CD_SK,
CUST_SVC_TASK_NOTE_LN.NOTE_SEQ_NO AS SRC_NOTE_SEQ_NO,
CUST_SVC_TASK_NOTE_LN.LAST_UPDT_DTM AS SRC_LAST_UPDT_DTM,
CUST_SVC_TASK_NOTE_LN.LN_SEQ_NO AS SRC_LN_SEQ_NO,
B_CUST_SVC_TASK_NOTE_LN.SRC_SYS_CD_SK AS TRGT_SRC_SYS_CD_SK,
B_CUST_SVC_TASK_NOTE_LN.CUST_SVC_ID AS TRGT_CUST_SVC_ID,
B_CUST_SVC_TASK_NOTE_LN.TASK_SEQ_NO AS TRGT_TASK_SEQ_NO,
B_CUST_SVC_TASK_NOTE_LN.CUST_SVC_TASK_NOTE_LOC_CD_SK AS TRGT_CUST_SVC_TASK_NOTE_LOC_CD_SK,
B_CUST_SVC_TASK_NOTE_LN.NOTE_SEQ_NO AS TRGT_NOTE_SEQ_NO,
B_CUST_SVC_TASK_NOTE_LN.LAST_UPDT_DTM AS TRGT_LAST_UPDT_DTM,
B_CUST_SVC_TASK_NOTE_LN.LN_SEQ_NO AS TRGT_LN_SEQ_NO
FROM {IDSOwner}.CUST_SVC_TASK_NOTE_LN CUST_SVC_TASK_NOTE_LN
FULL OUTER JOIN {IDSOwner}.B_CUST_SVC_TASK_NOTE_LN B_CUST_SVC_TASK_NOTE_LN
ON CUST_SVC_TASK_NOTE_LN.SRC_SYS_CD_SK = B_CUST_SVC_TASK_NOTE_LN.SRC_SYS_CD_SK
AND CUST_SVC_TASK_NOTE_LN.CUST_SVC_ID = B_CUST_SVC_TASK_NOTE_LN.CUST_SVC_ID
AND CUST_SVC_TASK_NOTE_LN.TASK_SEQ_NO = B_CUST_SVC_TASK_NOTE_LN.TASK_SEQ_NO
AND CUST_SVC_TASK_NOTE_LN.CUST_SVC_TASK_NOTE_LOC_CD_SK = B_CUST_SVC_TASK_NOTE_LN.CUST_SVC_TASK_NOTE_LOC_CD_SK
AND CUST_SVC_TASK_NOTE_LN.NOTE_SEQ_NO = B_CUST_SVC_TASK_NOTE_LN.NOTE_SEQ_NO
AND CUST_SVC_TASK_NOTE_LN.LAST_UPDT_DTM = B_CUST_SVC_TASK_NOTE_LN.LAST_UPDT_DTM
AND CUST_SVC_TASK_NOTE_LN.LN_SEQ_NO = B_CUST_SVC_TASK_NOTE_LN.LN_SEQ_NO
WHERE CUST_SVC_TASK_NOTE_LN.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
"""

df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_Research = df_SrcTrgtComp.filter(
    F.col("SRC_SRC_SYS_CD_SK").isNull()
    | F.col("SRC_CUST_SVC_ID").isNull()
    | F.col("SRC_TASK_SEQ_NO").isNull()
    | F.col("SRC_CUST_SVC_TASK_NOTE_LOC_CD_SK").isNull()
    | F.col("SRC_NOTE_SEQ_NO").isNull()
    | F.col("SRC_LAST_UPDT_DTM").isNull()
    | F.col("SRC_LN_SEQ_NO").isNull()
    | F.col("TRGT_CUST_SVC_ID").isNull()
    | F.col("TRGT_CUST_SVC_TASK_NOTE_LOC_CD_SK").isNull()
    | F.col("TRGT_LAST_UPDT_DTM").isNull()
    | F.col("TRGT_NOTE_SEQ_NO").isNull()
    | F.col("TRGT_SRC_SYS_CD_SK").isNull()
    | F.col("TRGT_TASK_SEQ_NO").isNull()
    | F.col("TRGT_LN_SEQ_NO").isNull()
)

df_Research = df_Research.select(
    "TRGT_SRC_SYS_CD_SK",
    "TRGT_CUST_SVC_ID",
    "TRGT_TASK_SEQ_NO",
    "TRGT_CUST_SVC_TASK_NOTE_LOC_CD_SK",
    "TRGT_NOTE_SEQ_NO",
    "TRGT_LAST_UPDT_DTM",
    "TRGT_LN_SEQ_NO",
    "SRC_SRC_SYS_CD_SK",
    "SRC_CUST_SVC_ID",
    "SRC_TASK_SEQ_NO",
    "SRC_CUST_SVC_TASK_NOTE_LOC_CD_SK",
    "SRC_NOTE_SEQ_NO",
    "SRC_LAST_UPDT_DTM",
    "SRC_LN_SEQ_NO"
)

write_files(
    df_Research,
    f"{adls_path}/balancing/research/FctsIdsCustSvcTaskNoteLnResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

schema_notify = StructType([StructField("NOTIFICATION", StringType(), True)])
if ToleranceCd == "OUT":
    df_Notify_temp = spark.createDataFrame(
        [("ROW COUNT BALANCING FACETS - IDS CUST SVC TASK NOTE LN OUT OF TOLERANCE",)],
        schema_notify
    )
    df_Notify = df_Notify_temp.withColumn("NOTIFICATION", F.rpad("NOTIFICATION", 70, " "))
else:
    df_Notify = spark.createDataFrame([], schema=schema_notify).withColumn("NOTIFICATION", F.lit(None).cast(StringType()))

write_files(
    df_Notify,
    f"{adls_path}/balancing/notify/CustSvcBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)