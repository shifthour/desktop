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
# MAGIC CALLED BY:     FctsIdsInvcSubBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               06/14/2007          3264                              Originally Programmed                                     devlIDS30     
# MAGIC 
# MAGIC Parikshith Chada               8/23/2007         3264                              Modified the balancing process,                        devlIDS30                   Steph Goddard              09/15/2007
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
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


RunID = get_widget_value('RunID','')
ToleranceCd = get_widget_value('ToleranceCd','')
IDSOwner = get_widget_value('IDSOwner','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = f"""SELECT 
INVC_SUB.SRC_SYS_CD_SK,
INVC_SUB.BILL_INVC_ID,
INVC_SUB.SUB_UNIQ_KEY,
INVC_SUB.CLS_PLN_ID,
INVC_SUB.PROD_ID,
INVC_SUB.PROD_BILL_CMPNT_ID,
INVC_SUB.COV_DUE_DT_SK,
INVC_SUB.COV_STRT_DT_SK,
INVC_SUB.INVC_SUB_PRM_TYP_CD_SK,
INVC_SUB.CRT_TS,
INVC_SUB.INVC_SUB_BILL_DISP_CD_SK,
B_INVC_SUB.SRC_SYS_CD_SK,
B_INVC_SUB.BILL_INVC_ID,
B_INVC_SUB.SUB_UNIQ_KEY,
B_INVC_SUB.CLS_PLN_ID,
B_INVC_SUB.PROD_ID,
B_INVC_SUB.PROD_BILL_CMPNT_ID,
B_INVC_SUB.COV_DUE_DT_SK,
B_INVC_SUB.COV_STRT_DT_SK,
B_INVC_SUB.INVC_SUB_PRM_TYP_CD_SK,
B_INVC_SUB.CRT_TS,
B_INVC_SUB.INVC_SUB_BILL_DISP_CD_SK
FROM {IDSOwner}.INVC_SUB INVC_SUB
FULL OUTER JOIN {IDSOwner}.B_INVC_SUB B_INVC_SUB
ON INVC_SUB.SRC_SYS_CD_SK = B_INVC_SUB.SRC_SYS_CD_SK
AND INVC_SUB.BILL_INVC_ID = B_INVC_SUB.BILL_INVC_ID
AND INVC_SUB.SUB_UNIQ_KEY = B_INVC_SUB.SUB_UNIQ_KEY
AND INVC_SUB.CLS_PLN_ID = B_INVC_SUB.CLS_PLN_ID
AND INVC_SUB.PROD_ID = B_INVC_SUB.PROD_ID
AND INVC_SUB.PROD_BILL_CMPNT_ID = B_INVC_SUB.PROD_BILL_CMPNT_ID
AND INVC_SUB.COV_DUE_DT_SK = B_INVC_SUB.COV_DUE_DT_SK
AND INVC_SUB.COV_STRT_DT_SK = B_INVC_SUB.COV_STRT_DT_SK
AND INVC_SUB.INVC_SUB_PRM_TYP_CD_SK = B_INVC_SUB.INVC_SUB_PRM_TYP_CD_SK
AND INVC_SUB.CRT_TS = B_INVC_SUB.CRT_TS
AND INVC_SUB.INVC_SUB_BILL_DISP_CD_SK = B_INVC_SUB.INVC_SUB_BILL_DISP_CD_SK
WHERE INVC_SUB.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
"""

df_SrcTrgtComp_temp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_SrcTrgtComp = df_SrcTrgtComp_temp.select(
    F.col("SRC_SYS_CD_SK").alias("SRC_SRC_SYS_CD_SK"),
    F.col("BILL_INVC_ID").alias("SRC_BILL_INVC_ID"),
    F.col("SUB_UNIQ_KEY").alias("SRC_SUB_UNIQ_KEY"),
    F.col("CLS_PLN_ID").alias("SRC_CLS_PLN_ID"),
    F.col("PROD_ID").alias("SRC_PROD_ID"),
    F.col("PROD_BILL_CMPNT_ID").alias("SRC_PROD_BILL_CMPNT_ID"),
    F.col("COV_DUE_DT_SK").alias("SRC_COV_DUE_DT_SK"),
    F.col("COV_STRT_DT_SK").alias("SRC_COV_STRT_DT_SK"),
    F.col("INVC_SUB_PRM_TYP_CD_SK").alias("SRC_INVC_SUB_PRM_TYP_CD_SK"),
    F.col("CRT_TS").alias("SRC_CRT_TS"),
    F.col("INVC_SUB_BILL_DISP_CD_SK").alias("SRC_INVC_SUB_BILL_DISP_CD_SK"),
    F.col("SRC_SYS_CD_SK_1").alias("TRGT_SRC_SYS_CD_SK"),
    F.col("BILL_INVC_ID_1").alias("TRGT_BILL_INVC_ID"),
    F.col("SUB_UNIQ_KEY_1").alias("TRGT_SUB_UNIQ_KEY"),
    F.col("CLS_PLN_ID_1").alias("TRGT_CLS_PLN_ID"),
    F.col("PROD_ID_1").alias("TRGT_PROD_ID"),
    F.col("PROD_BILL_CMPNT_ID_1").alias("TRGT_PROD_BILL_CMPNT_ID"),
    F.col("COV_DUE_DT_SK_1").alias("TRGT_COV_DUE_DT_SK"),
    F.col("COV_STRT_DT_SK_1").alias("TRGT_COV_STRT_DT_SK"),
    F.col("INVC_SUB_PRM_TYP_CD_SK_1").alias("TRGT_INVC_SUB_PRM_TYP_CD_SK"),
    F.col("CRT_TS_1").alias("TRGT_CRT_TS"),
    F.col("INVC_SUB_BILL_DISP_CD_SK_1").alias("TRGT_INVC_SUB_BILL_DISP_CD_SK")
)

df_ResearchFile = df_SrcTrgtComp.filter(
    F.col("SRC_BILL_INVC_ID").isNull()
    | F.col("SRC_CLS_PLN_ID").isNull()
    | F.col("SRC_COV_DUE_DT_SK").isNull()
    | F.col("SRC_COV_STRT_DT_SK").isNull()
    | F.col("SRC_CRT_TS").isNull()
    | F.col("SRC_INVC_SUB_BILL_DISP_CD_SK").isNull()
    | F.col("SRC_INVC_SUB_PRM_TYP_CD_SK").isNull()
    | F.col("SRC_PROD_BILL_CMPNT_ID").isNull()
    | F.col("SRC_PROD_ID").isNull()
    | F.col("SRC_SRC_SYS_CD_SK").isNull()
    | F.col("SRC_SUB_UNIQ_KEY").isNull()
    | F.col("TRGT_BILL_INVC_ID").isNull()
    | F.col("TRGT_CLS_PLN_ID").isNull()
    | F.col("TRGT_COV_DUE_DT_SK").isNull()
    | F.col("TRGT_COV_STRT_DT_SK").isNull()
    | F.col("TRGT_CRT_TS").isNull()
    | F.col("TRGT_INVC_SUB_BILL_DISP_CD_SK").isNull()
    | F.col("TRGT_INVC_SUB_PRM_TYP_CD_SK").isNull()
    | F.col("TRGT_PROD_BILL_CMPNT_ID").isNull()
    | F.col("TRGT_PROD_ID").isNull()
    | F.col("TRGT_SRC_SYS_CD_SK").isNull()
    | F.col("TRGT_SUB_UNIQ_KEY").isNull()
)

df_ResearchFile = df_ResearchFile.select(
    F.col("TRGT_SRC_SYS_CD_SK"),
    F.col("TRGT_BILL_INVC_ID"),
    F.col("TRGT_SUB_UNIQ_KEY"),
    F.col("TRGT_CLS_PLN_ID"),
    F.col("TRGT_PROD_ID"),
    F.col("TRGT_PROD_BILL_CMPNT_ID"),
    F.rpad(F.col("TRGT_COV_DUE_DT_SK"), 10, " ").alias("TRGT_COV_DUE_DT_SK"),
    F.rpad(F.col("TRGT_COV_STRT_DT_SK"), 10, " ").alias("TRGT_COV_STRT_DT_SK"),
    F.col("TRGT_INVC_SUB_PRM_TYP_CD_SK"),
    F.col("TRGT_CRT_TS"),
    F.col("TRGT_INVC_SUB_BILL_DISP_CD_SK"),
    F.col("SRC_SRC_SYS_CD_SK"),
    F.col("SRC_BILL_INVC_ID"),
    F.col("SRC_SUB_UNIQ_KEY"),
    F.col("SRC_CLS_PLN_ID"),
    F.col("SRC_PROD_ID"),
    F.col("SRC_PROD_BILL_CMPNT_ID"),
    F.rpad(F.col("SRC_COV_DUE_DT_SK"), 10, " ").alias("SRC_COV_DUE_DT_SK"),
    F.rpad(F.col("SRC_COV_STRT_DT_SK"), 10, " ").alias("SRC_COV_STRT_DT_SK"),
    F.col("SRC_INVC_SUB_PRM_TYP_CD_SK"),
    F.col("SRC_CRT_TS"),
    F.col("SRC_INVC_SUB_BILL_DISP_CD_SK")
)

write_files(
    df_ResearchFile,
    f"{adls_path}/balancing/research/FctsIdsInvcSubResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

if ToleranceCd == "OUT":
    df_Notify_temp = df_SrcTrgtComp.limit(1).select(
        F.lit("ROW COUNT BALANCING FACETS - IDS INVC SUB OUT OF TOLERANCE").alias("NOTIFICATION")
    )
    df_Notify = df_Notify_temp.select(
        F.rpad(F.col("NOTIFICATION"), 70, " ").alias("NOTIFICATION")
    )
else:
    df_Notify = spark.createDataFrame([], "NOTIFICATION STRING")

write_files(
    df_Notify,
    f"{adls_path}/balancing/notify/IncomeBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)