# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 01/28/08 09:39:22 Batch  14638_34767 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 12/27/07 09:46:56 Batch  14606_35219 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/24/07 09:35:50 Batch  14573_34558 INIT bckcetl ids20 dsadm dsadm
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
# MAGIC CALLED BY:     FctsIdsBillIncmRcptBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                       Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                   ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              06/04/2007          3264                              Originally Programmed                                    devlIDS30                  
# MAGIC 
# MAGIC Parikshith Chada               8/22/2007         3264                              Modified the balancing process,                      devlIDS30                    Steph Goddard           09/18/2007
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


RunID = get_widget_value('RunID','')
ToleranceCd = get_widget_value('ToleranceCd','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        "SELECT \nBILL_INCM_RCPT.SRC_SYS_CD_SK,\nBILL_INCM_RCPT.BILL_ENTY_UNIQ_KEY,\nBILL_INCM_RCPT.BILL_DUE_DT_SK,\nBILL_INCM_RCPT.BILL_CNTR_ID,\nBILL_INCM_RCPT.CRT_DTM,\nB_BILL_INCM_RCPT.SRC_SYS_CD_SK,\nB_BILL_INCM_RCPT.BILL_ENTY_UNIQ_KEY,\nB_BILL_INCM_RCPT.BILL_DUE_DT_SK,\nB_BILL_INCM_RCPT.BILL_CNTR_ID,\nB_BILL_INCM_RCPT.CRT_DTM \n\nFROM \n"
        + f"{IDSOwner}.BILL_INCM_RCPT BILL_INCM_RCPT FULL OUTER JOIN {IDSOwner}.B_BILL_INCM_RCPT B_BILL_INCM_RCPT\n"
        + "ON BILL_INCM_RCPT.SRC_SYS_CD_SK = B_BILL_INCM_RCPT.SRC_SYS_CD_SK \n"
        + "AND BILL_INCM_RCPT.BILL_ENTY_UNIQ_KEY = B_BILL_INCM_RCPT.BILL_ENTY_UNIQ_KEY \n"
        + "AND BILL_INCM_RCPT.BILL_DUE_DT_SK = B_BILL_INCM_RCPT.BILL_DUE_DT_SK \n"
        + "AND BILL_INCM_RCPT.BILL_CNTR_ID = B_BILL_INCM_RCPT.BILL_CNTR_ID \n"
        + "AND BILL_INCM_RCPT.CRT_DTM = B_BILL_INCM_RCPT.CRT_DTM\n\nWHERE \n"
        + f"BILL_INCM_RCPT.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}"
    )
    .load()
)

df_TransformLogicIn = df_SrcTrgtComp.select(
    F.col(df_SrcTrgtComp.columns[0]).alias("SRC_SRC_SYS_CD_SK"),
    F.col(df_SrcTrgtComp.columns[1]).alias("SRC_BILL_ENTY_UNIQ_KEY"),
    F.col(df_SrcTrgtComp.columns[2]).alias("SRC_BILL_DUE_DT_SK"),
    F.col(df_SrcTrgtComp.columns[3]).alias("SRC_BILL_CNTR_ID"),
    F.col(df_SrcTrgtComp.columns[4]).alias("SRC_CRT_DTM"),
    F.col(df_SrcTrgtComp.columns[5]).alias("TRGT_SRC_SYS_CD_SK"),
    F.col(df_SrcTrgtComp.columns[6]).alias("TRGT_BILL_ENTY_UNIQ_KEY"),
    F.col(df_SrcTrgtComp.columns[7]).alias("TRGT_BILL_DUE_DT_SK"),
    F.col(df_SrcTrgtComp.columns[8]).alias("TRGT_BILL_CNTR_ID"),
    F.col(df_SrcTrgtComp.columns[9]).alias("TRGT_CRT_DTM")
)

df_Research = df_TransformLogicIn.filter(
    F.col("SRC_BILL_CNTR_ID").isNull()
    | F.col("SRC_BILL_DUE_DT_SK").isNull()
    | F.col("SRC_BILL_ENTY_UNIQ_KEY").isNull()
    | F.col("SRC_CRT_DTM").isNull()
    | F.col("SRC_SRC_SYS_CD_SK").isNull()
    | F.col("TRGT_BILL_CNTR_ID").isNull()
    | F.col("TRGT_BILL_DUE_DT_SK").isNull()
    | F.col("TRGT_BILL_ENTY_UNIQ_KEY").isNull()
    | F.col("TRGT_CRT_DTM").isNull()
    | F.col("TRGT_SRC_SYS_CD_SK").isNull()
)

df_ResearchFinal = df_Research.select(
    F.col("TRGT_SRC_SYS_CD_SK"),
    F.col("TRGT_BILL_ENTY_UNIQ_KEY"),
    F.rpad(F.col("TRGT_BILL_DUE_DT_SK"), 10, " ").alias("TRGT_BILL_DUE_DT_SK"),
    F.col("TRGT_BILL_CNTR_ID"),
    F.col("TRGT_CRT_DTM"),
    F.col("SRC_SRC_SYS_CD_SK"),
    F.col("SRC_BILL_ENTY_UNIQ_KEY"),
    F.rpad(F.col("SRC_BILL_DUE_DT_SK"), 10, " ").alias("SRC_BILL_DUE_DT_SK"),
    F.col("SRC_BILL_CNTR_ID"),
    F.col("SRC_CRT_DTM")
)

write_files(
    df_ResearchFinal,
    f"{adls_path}/balancing/research/FctsIdsBillIncmRcptResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

if ToleranceCd == 'OUT':
    df_NotifyIn = df_TransformLogicIn.limit(1)
else:
    df_NotifyIn = spark.createDataFrame([], StructType([StructField("NOTIFICATION", StringType(), True)]))

df_Notify = df_NotifyIn.withColumn(
    "NOTIFICATION",
    F.lit("ROW COUNT BALANCING FACETS - IDS BILL INCM RCPT OUT OF TOLERANCE")
)

df_NotifyFinal = df_Notify.select(
    F.rpad(F.col("NOTIFICATION"), 70, " ").alias("NOTIFICATION")
)

write_files(
    df_NotifyFinal,
    f"{adls_path}/balancing/notify/CommissionsBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)