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
# MAGIC CALLED BY:          FctsIdsCustSvcTaskCstmDtlBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                        Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               06/22/2007          3264                              Originally Programmed                                     devlIDS30                  
# MAGIC 
# MAGIC Parikshith Chada               8/24/2007         3264                              Modified the balancing process,                         devlIDS30                  Steph Goddard            9/14/07
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
from pyspark.sql.functions import col, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
RunID = get_widget_value('RunID','')
ToleranceCd = get_widget_value('ToleranceCd','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = f"""
SELECT 
CUST_SVC_TASK_CSTM_DTL.SRC_SYS_CD_SK AS SRC_SRC_SYS_CD_SK,
CUST_SVC_TASK_CSTM_DTL.CUST_SVC_ID AS SRC_CUST_SVC_ID,
CUST_SVC_TASK_CSTM_DTL.TASK_SEQ_NO AS SRC_TASK_SEQ_NO,
CUST_SVC_TASK_CSTM_DTL.CUST_SVC_TASK_CSTM_DTL_CD_SK AS SRC_CUST_SVC_TASK_CSTM_DTL_CD_SK,
CUST_SVC_TASK_CSTM_DTL.CSTM_DTL_UNIQ_ID AS SRC_CSTM_DTL_UNIQ_ID,
CUST_SVC_TASK_CSTM_DTL.CSTM_DTL_SEQ_NO AS SRC_CSTM_DTL_SEQ_NO,
B_CUST_SVC_TASK_CSTM_DTL.SRC_SYS_CD_SK AS TRGT_SRC_SYS_CD_SK,
B_CUST_SVC_TASK_CSTM_DTL.CUST_SVC_ID AS TRGT_CUST_SVC_ID,
B_CUST_SVC_TASK_CSTM_DTL.TASK_SEQ_NO AS TRGT_TASK_SEQ_NO,
B_CUST_SVC_TASK_CSTM_DTL.CUST_SVC_TASK_CSTM_DTL_CD_SK AS TRGT_CUST_SVC_TASK_CSTM_DTL_CD_SK,
B_CUST_SVC_TASK_CSTM_DTL.CSTM_DTL_UNIQ_ID AS TRGT_CSTM_DTL_UNIQ_ID,
B_CUST_SVC_TASK_CSTM_DTL.CSTM_DTL_SEQ_NO AS TRGT_CSTM_DTL_SEQ_NO
FROM {IDSOwner}.CUST_SVC_TASK_CSTM_DTL CUST_SVC_TASK_CSTM_DTL
FULL OUTER JOIN {IDSOwner}.B_CUST_SVC_TASK_CSTM_DTL B_CUST_SVC_TASK_CSTM_DTL
ON CUST_SVC_TASK_CSTM_DTL.SRC_SYS_CD_SK = B_CUST_SVC_TASK_CSTM_DTL.SRC_SYS_CD_SK
AND CUST_SVC_TASK_CSTM_DTL.CUST_SVC_ID = B_CUST_SVC_TASK_CSTM_DTL.CUST_SVC_ID
AND CUST_SVC_TASK_CSTM_DTL.TASK_SEQ_NO = B_CUST_SVC_TASK_CSTM_DTL.TASK_SEQ_NO
AND CUST_SVC_TASK_CSTM_DTL.CUST_SVC_TASK_CSTM_DTL_CD_SK = B_CUST_SVC_TASK_CSTM_DTL.CUST_SVC_TASK_CSTM_DTL_CD_SK
AND CUST_SVC_TASK_CSTM_DTL.CSTM_DTL_UNIQ_ID = B_CUST_SVC_TASK_CSTM_DTL.CSTM_DTL_UNIQ_ID
AND CUST_SVC_TASK_CSTM_DTL.CSTM_DTL_SEQ_NO = B_CUST_SVC_TASK_CSTM_DTL.CSTM_DTL_SEQ_NO
WHERE CUST_SVC_TASK_CSTM_DTL.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
"""

df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_ResearchCondition = (
    col("SRC_CSTM_DTL_SEQ_NO").isNull()
    | col("SRC_CSTM_DTL_UNIQ_ID").isNull()
    | col("SRC_CUST_SVC_ID").isNull()
    | col("SRC_CUST_SVC_TASK_CSTM_DTL_CD_SK").isNull()
    | col("SRC_SRC_SYS_CD_SK").isNull()
    | col("SRC_TASK_SEQ_NO").isNull()
    | col("TRGT_CSTM_DTL_SEQ_NO").isNull()
    | col("TRGT_CSTM_DTL_UNIQ_ID").isNull()
    | col("TRGT_CUST_SVC_ID").isNull()
    | col("TRGT_CUST_SVC_TASK_CSTM_DTL_CD_SK").isNull()
    | col("TRGT_SRC_SYS_CD_SK").isNull()
    | col("TRGT_TASK_SEQ_NO").isNull()
)

df_Research = df_SrcTrgtComp.filter(df_ResearchCondition).select(
    "TRGT_SRC_SYS_CD_SK",
    "TRGT_CUST_SVC_ID",
    "TRGT_TASK_SEQ_NO",
    "TRGT_CUST_SVC_TASK_CSTM_DTL_CD_SK",
    "TRGT_CSTM_DTL_UNIQ_ID",
    "TRGT_CSTM_DTL_SEQ_NO",
    "SRC_SRC_SYS_CD_SK",
    "SRC_CUST_SVC_ID",
    "SRC_TASK_SEQ_NO",
    "SRC_CUST_SVC_TASK_CSTM_DTL_CD_SK",
    "SRC_CSTM_DTL_UNIQ_ID",
    "SRC_CSTM_DTL_SEQ_NO"
)

df_Notify = None
if ToleranceCd == "OUT":
    rdd_notify = spark.sparkContext.parallelize(
        [("ROW COUNT BALANCING FACETS - IDS CUST SVC TASK CSTM DTL OUT OF TOLERANCE",)]
    )
    df_Notify = spark.createDataFrame(rdd_notify, ["NOTIFICATION"])
    df_Notify = df_Notify.withColumn("NOTIFICATION", rpad("NOTIFICATION", 70, " "))

write_files(
    df_Research,
    f"{adls_path}/balancing/research/FctsIdsCustSvcTaskCstmDtlResearch.dat.{RunID}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)

if df_Notify is not None:
    write_files(
        df_Notify,
        f"{adls_path}/balancing/notify/CustSvcBalancingNotification.dat",
        ",",
        "append",
        False,
        False,
        "\"",
        None
    )