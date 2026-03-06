# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 01/30/08 05:00:10 Batch  14640_18019 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_2 11/12/07 10:00:11 Batch  14561_36015 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_1 11/02/07 13:07:35 Batch  14551_47275 PROMOTE bckcetl ids20 dsadm bls for on
# MAGIC ^1_1 11/02/07 12:52:45 Batch  14551_46368 INIT bckcett testIDS30 dsadm bls for on
# MAGIC ^1_1 10/31/07 13:17:35 Batch  14549_47857 PROMOTE bckcett testIDS30 u03651 steffy
# MAGIC ^1_1 10/31/07 13:09:37 Batch  14549_47387 INIT bckcett devlIDS30 u03651 steffy
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:      FctsIdsInvcBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                          Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                     ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               06/13/2007          3264                              Originally Programmed                                    devlIDS30 
# MAGIC 
# MAGIC Parikshith Chada               8/23/2007         3264                              Modified the balancing process,                        devlIDS30                     Steph Goddard             09/15/2007
# MAGIC                                                                                                           by changing snapshot file to snapshot table

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
from pyspark.sql import functions as F
from pyspark.sql import Window
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


# Parameters
RunID = get_widget_value('RunID','')
ToleranceCd = get_widget_value('ToleranceCd','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')

# DB2Connector Stage: SrcTrgtComp
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"""
SELECT
INVC.SRC_SYS_CD_SK AS SRC_SRC_SYS_CD_SK,
INVC.BILL_INVC_ID AS SRC_BILL_INVC_ID,
B_INVC.SRC_SYS_CD_SK AS TRGT_SRC_SYS_CD_SK,
B_INVC.BILL_INVC_ID AS TRGT_BILL_INVC_ID
FROM {IDSOwner}.INVC INVC
FULL OUTER JOIN {IDSOwner}.B_INVC B_INVC
ON INVC.SRC_SYS_CD_SK = B_INVC.SRC_SYS_CD_SK
AND INVC.BILL_INVC_ID = B_INVC.BILL_INVC_ID
WHERE INVC.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
"""
df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# Transformer Stage: TransformLogic
# Research link filter
df_Research = df_SrcTrgtComp.filter(
    F.col("SRC_BILL_INVC_ID").isNull()
    | F.col("SRC_SRC_SYS_CD_SK").isNull()
    | F.col("TRGT_BILL_INVC_ID").isNull()
    | F.col("TRGT_SRC_SYS_CD_SK").isNull()
).select(
    F.col("TRGT_SRC_SYS_CD_SK"),
    F.col("TRGT_BILL_INVC_ID"),
    F.col("SRC_SRC_SYS_CD_SK"),
    F.col("SRC_BILL_INVC_ID")
)

# Notify link filter (@INROWNUM=1 and ToleranceCd='OUT')
w = Window.orderBy(F.lit(1))
df_SrcTrgtCompRN = df_SrcTrgtComp.withColumn("row_num", F.row_number().over(w))
df_Notify = df_SrcTrgtCompRN.filter(
    (F.col("row_num") == 1) & (F.lit(ToleranceCd) == "OUT")
).select(
    F.lit("ROW COUNT BALANCING FACETS - IDS INVC OUT OF TOLERANCE").alias("NOTIFICATION")
)

# ResearchFile Stage (CSeqFileStage)
write_files(
    df_Research,
    f"{adls_path}/balancing/research/FctsIdsInvcResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# ErrorNotificationFile Stage (CSeqFileStage)
df_Notify_final = df_Notify.withColumn("NOTIFICATION", F.rpad(F.col("NOTIFICATION"), 70, " "))
write_files(
    df_Notify_final,
    f"{adls_path}/balancing/notify/IncomeBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)