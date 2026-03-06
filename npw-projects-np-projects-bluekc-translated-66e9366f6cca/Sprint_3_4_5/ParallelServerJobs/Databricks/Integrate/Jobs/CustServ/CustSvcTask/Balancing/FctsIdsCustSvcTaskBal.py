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
# MAGIC CALLED BY:          FctsIdsCustSvcTaskBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                           Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                     ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               06/22/2007          3264                              Originally Programmed                                      devlIDS30          
# MAGIC 
# MAGIC Parikshith Chada               8/24/2007         3264                              Modified the balancing process,                          devlIDS30                   Steph Goddard             09/14/2007
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
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


RunID = get_widget_value("RunID","")
ToleranceCd = get_widget_value("ToleranceCd","")
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
ExtrRunCycle = get_widget_value("ExtrRunCycle","")

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = f"""SELECT 
CUST_SVC_TASK.SRC_SYS_CD_SK,
CUST_SVC_TASK.CUST_SVC_ID,
CUST_SVC_TASK.TASK_SEQ_NO,
B_CUST_SVC_TASK.SRC_SYS_CD_SK,
B_CUST_SVC_TASK.CUST_SVC_ID,
B_CUST_SVC_TASK.TASK_SEQ_NO

FROM {IDSOwner}.CD_MPPNG MPPNG,
     {IDSOwner}.CUST_SVC_TASK CUST_SVC_TASK
     FULL OUTER JOIN {IDSOwner}.B_CUST_SVC_TASK B_CUST_SVC_TASK
       ON CUST_SVC_TASK.SRC_SYS_CD_SK = B_CUST_SVC_TASK.SRC_SYS_CD_SK
      AND CUST_SVC_TASK.CUST_SVC_ID = B_CUST_SVC_TASK.CUST_SVC_ID
      AND CUST_SVC_TASK.TASK_SEQ_NO = B_CUST_SVC_TASK.TASK_SEQ_NO

WHERE CUST_SVC_TASK.LAST_UPDT_RUN_CYC_EXCTN_SK = {ExtrRunCycle}
  AND CUST_SVC_TASK.SRC_SYS_CD_SK = MPPNG.CD_MPPNG_SK
  AND MPPNG.TRGT_CD = 'FACETS'
"""

df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_SrcTrgtComp = (
    df_SrcTrgtComp
    .withColumnRenamed("SRC_SYS_CD_SK", "SRC_SRC_SYS_CD_SK")
    .withColumnRenamed("CUST_SVC_ID", "SRC_CUST_SVC_ID")
    .withColumnRenamed("TASK_SEQ_NO", "SRC_TASK_SEQ_NO")
    .withColumnRenamed("SRC_SYS_CD_SK_1", "TRGT_SRC_SYS_CD_SK")
    .withColumnRenamed("CUST_SVC_ID_1", "TRGT_CUST_SVC_ID")
    .withColumnRenamed("TASK_SEQ_NO_1", "TRGT_TASK_SEQ_NO")
)

df_Research = df_SrcTrgtComp.filter(
    col("SRC_SRC_SYS_CD_SK").isNull() |
    col("SRC_CUST_SVC_ID").isNull() |
    col("SRC_TASK_SEQ_NO").isNull() |
    col("TRGT_SRC_SYS_CD_SK").isNull() |
    col("TRGT_CUST_SVC_ID").isNull() |
    col("TRGT_TASK_SEQ_NO").isNull()
)
df_Research = df_Research.select(
    "TRGT_SRC_SYS_CD_SK",
    "TRGT_CUST_SVC_ID",
    "TRGT_TASK_SEQ_NO",
    "SRC_SRC_SYS_CD_SK",
    "SRC_CUST_SVC_ID",
    "SRC_TASK_SEQ_NO"
)

df_Notify_all = df_SrcTrgtComp.filter(lit(ToleranceCd) == "OUT")
df_Notify = df_Notify_all.limit(1)
df_Notify = df_Notify.withColumn("NOTIFICATION", lit("ROW COUNT BALANCING FACETS - IDS CUST SVC TASK OUT OF TOLERANCE"))
df_Notify = df_Notify.withColumn("NOTIFICATION", rpad(col("NOTIFICATION"), 70, " "))
df_Notify = df_Notify.select("NOTIFICATION")

df_Research_final = df_Research.select(
    "TRGT_SRC_SYS_CD_SK",
    "TRGT_CUST_SVC_ID",
    "TRGT_TASK_SEQ_NO",
    "SRC_SRC_SYS_CD_SK",
    "SRC_CUST_SVC_ID",
    "SRC_TASK_SEQ_NO"
)
write_files(
    df_Research_final,
    f"{adls_path}/balancing/research/FctsIdsCustSvcTaskResearch.dat.{RunID}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)

df_ErrorNotificationFile = df_Notify.select("NOTIFICATION")
write_files(
    df_ErrorNotificationFile,
    f"{adls_path}/balancing/notify/CustSvcBalancingNotification.dat",
    ",",
    "append",
    False,
    False,
    "\"",
    None
)