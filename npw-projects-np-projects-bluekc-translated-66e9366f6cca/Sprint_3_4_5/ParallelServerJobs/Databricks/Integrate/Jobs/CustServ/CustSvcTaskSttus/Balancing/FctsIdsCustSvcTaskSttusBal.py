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
# MAGIC CALLED BY:          FctsIdsCustSvcTaskSttusBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                  ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               06/24/2007          3264                              Originally Programmed                                    devlIDS30   
# MAGIC 
# MAGIC Parikshith Chada               8/24/2007         3264                              Modified the balancing process,                        devlID30                      Steph Goddard            09/14/2007
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
from pyspark.sql import types as T
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Parameter retrieval
RunID = get_widget_value('RunID','')
ToleranceCd = get_widget_value('ToleranceCd','')
IDSOwner = get_widget_value('IDSOwner','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
ids_secret_name = get_widget_value('ids_secret_name','')

# DB connection config
jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# Read from IDS database (SrcTrgtComp stage)
extract_query = f"""
SELECT 
  CUST_SVC_TASK_STTUS.SRC_SYS_CD_SK AS SRC_SRC_SYS_CD_SK,
  CUST_SVC_TASK_STTUS.CUST_SVC_ID AS SRC_CUST_SVC_ID,
  CUST_SVC_TASK_STTUS.TASK_SEQ_NO AS SRC_TASK_SEQ_NO,
  CUST_SVC_TASK_STTUS.STTUS_SEQ_NO AS SRC_STTUS_SEQ_NO,
  B_CUST_SVC_TASK_STTUS.SRC_SYS_CD_SK AS TRGT_SRC_SYS_CD_SK,
  B_CUST_SVC_TASK_STTUS.CUST_SVC_ID AS TRGT_CUST_SVC_ID,
  B_CUST_SVC_TASK_STTUS.TASK_SEQ_NO AS TRGT_TASK_SEQ_NO,
  B_CUST_SVC_TASK_STTUS.STTUS_SEQ_NO AS TRGT_STTUS_SEQ_NO
FROM {IDSOwner}.CD_MPPNG MPPNG,
     {IDSOwner}.CUST_SVC_TASK_STTUS CUST_SVC_TASK_STTUS
     FULL OUTER JOIN {IDSOwner}.B_CUST_SVC_TASK_STTUS B_CUST_SVC_TASK_STTUS
       ON CUST_SVC_TASK_STTUS.SRC_SYS_CD_SK = B_CUST_SVC_TASK_STTUS.SRC_SYS_CD_SK
       AND CUST_SVC_TASK_STTUS.CUST_SVC_ID = B_CUST_SVC_TASK_STTUS.CUST_SVC_ID
       AND CUST_SVC_TASK_STTUS.TASK_SEQ_NO = B_CUST_SVC_TASK_STTUS.TASK_SEQ_NO
       AND CUST_SVC_TASK_STTUS.STTUS_SEQ_NO = B_CUST_SVC_TASK_STTUS.STTUS_SEQ_NO
WHERE 
  CUST_SVC_TASK_STTUS.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
  AND CUST_SVC_TASK_STTUS.SRC_SYS_CD_SK = MPPNG.CD_MPPNG_SK
  AND MPPNG.TRGT_CD = 'FACETS'
"""
df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# TransformLogic stage
df_Research = df_SrcTrgtComp.filter(
    F.col("SRC_CUST_SVC_ID").isNull() |
    F.col("SRC_SRC_SYS_CD_SK").isNull() |
    F.col("SRC_TASK_SEQ_NO").isNull() |
    F.col("SRC_STTUS_SEQ_NO").isNull() |
    F.col("TRGT_CUST_SVC_ID").isNull() |
    F.col("TRGT_SRC_SYS_CD_SK").isNull() |
    F.col("TRGT_TASK_SEQ_NO").isNull() |
    F.col("TRGT_STTUS_SEQ_NO").isNull()
)
df_Research = df_Research.select(
    "TRGT_SRC_SYS_CD_SK",
    "TRGT_CUST_SVC_ID",
    "TRGT_TASK_SEQ_NO",
    "TRGT_STTUS_SEQ_NO",
    "SRC_SRC_SYS_CD_SK",
    "SRC_CUST_SVC_ID",
    "SRC_TASK_SEQ_NO",
    "SRC_STTUS_SEQ_NO"
)

if ToleranceCd == "OUT":
    w = Window.orderBy(F.lit(1))
    df_temp = df_SrcTrgtComp.withColumn("rn", F.row_number().over(w))
    df_Notify = df_temp.filter(F.col("rn") == 1).select(
        F.lit("ROW COUNT BALANCING FACETS - IDS CUST SVC TASK STTUS OUT OF TOLERANCE").alias("NOTIFICATION")
    )
    df_Notify = df_Notify.withColumn("NOTIFICATION", F.rpad(F.col("NOTIFICATION"), 70, ' '))
else:
    df_Notify = spark.createDataFrame([], T.StructType([T.StructField("NOTIFICATION", T.StringType(), True)]))

# ResearchFile stage
write_files(
    df_Research,
    f"{adls_path}/balancing/research/FctsIdsCustSvcTaskSttusResearch.dat.{RunID}",
    ",",
    "overwrite",
    False,
    False,
    '"',
    None
)

# ErrorNotificationFile stage
df_Notify = df_Notify.select("NOTIFICATION")
write_files(
    df_Notify,
    f"{adls_path}/balancing/notify/CustSvcBalancingNotification.dat",
    ",",
    "append",
    False,
    False,
    '"',
    None
)