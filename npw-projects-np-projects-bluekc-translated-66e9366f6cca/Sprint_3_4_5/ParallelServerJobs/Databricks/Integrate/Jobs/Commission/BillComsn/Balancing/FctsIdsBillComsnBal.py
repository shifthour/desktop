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
# MAGIC CALLED BY:        FctsIdsBillComsnBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                       Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                  ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              06/01/2007          3264                              Originally Programmed                                  devlIDS30         
# MAGIC 
# MAGIC Parikshith Chada               8/22/2007         3264                              Modified the balancing process,                     devlIDS30                    Steph Goddard            09/17/2007
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
from pyspark.sql.functions import col, lit, isnull, rpad

RunID = get_widget_value("RunID","")
ToleranceCd = get_widget_value("ToleranceCd","")
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
ExtrRunCycle = get_widget_value("ExtrRunCycle","")

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = f"""
SELECT
BILL_COMSN.SRC_SYS_CD_SK AS SRC_SRC_SYS_CD_SK,
BILL_COMSN.BILL_ENTY_UNIQ_KEY AS SRC_BILL_ENTY_UNIQ_KEY,
BILL_COMSN.CLS_PLN_ID AS SRC_CLS_PLN_ID,
BILL_COMSN.FEE_DSCNT_ID AS SRC_FEE_DSCNT_ID,
BILL_COMSN.SEQ_NO AS SRC_SEQ_NO,
B_BILL_COMSN.SRC_SYS_CD_SK AS TRGT_SRC_SYS_CD_SK,
B_BILL_COMSN.BILL_ENTY_UNIQ_KEY AS TRGT_BILL_ENTY_UNIQ_KEY,
B_BILL_COMSN.CLS_PLN_ID AS TRGT_CLS_PLN_ID,
B_BILL_COMSN.FEE_DSCNT_ID AS TRGT_FEE_DSCNT_ID,
B_BILL_COMSN.SEQ_NO AS TRGT_SEQ_NO
FROM {IDSOwner}.BILL_COMSN BILL_COMSN
FULL OUTER JOIN {IDSOwner}.B_BILL_COMSN B_BILL_COMSN
ON BILL_COMSN.SRC_SYS_CD_SK = B_BILL_COMSN.SRC_SYS_CD_SK
AND BILL_COMSN.BILL_ENTY_UNIQ_KEY = B_BILL_COMSN.BILL_ENTY_UNIQ_KEY
AND BILL_COMSN.CLS_PLN_ID = B_BILL_COMSN.CLS_PLN_ID
AND BILL_COMSN.FEE_DSCNT_ID = B_BILL_COMSN.FEE_DSCNT_ID
AND BILL_COMSN.SEQ_NO = B_BILL_COMSN.SEQ_NO
WHERE BILL_COMSN.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
"""

df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_Research = df_SrcTrgtComp.filter(
    (col("SRC_SRC_SYS_CD_SK").isNull())
    | (col("SRC_BILL_ENTY_UNIQ_KEY").isNull())
    | (col("SRC_CLS_PLN_ID").isNull())
    | (col("SRC_FEE_DSCNT_ID").isNull())
    | (col("SRC_SEQ_NO").isNull())
    | (col("TRGT_SRC_SYS_CD_SK").isNull())
    | (col("TRGT_BILL_ENTY_UNIQ_KEY").isNull())
    | (col("TRGT_CLS_PLN_ID").isNull())
    | (col("TRGT_FEE_DSCNT_ID").isNull())
    | (col("TRGT_SEQ_NO").isNull())
)

df_Research = df_Research.select(
    "TRGT_SRC_SYS_CD_SK",
    "TRGT_BILL_ENTY_UNIQ_KEY",
    "TRGT_CLS_PLN_ID",
    "TRGT_FEE_DSCNT_ID",
    "TRGT_SEQ_NO",
    "SRC_SRC_SYS_CD_SK",
    "SRC_BILL_ENTY_UNIQ_KEY",
    "SRC_CLS_PLN_ID",
    "SRC_FEE_DSCNT_ID",
    "SRC_SEQ_NO"
)

if ToleranceCd == "OUT":
    df_temp = df_SrcTrgtComp.limit(1).select(
        lit("ROW COUNT BALANCING FACETS - IDS BILL COMSN OUT OF TOLERANCE").alias("NOTIFICATION")
    )
    df_Notify = df_temp.select(rpad(col("NOTIFICATION"), 70, " ").alias("NOTIFICATION"))
else:
    from pyspark.sql.types import StructType, StructField, StringType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------

    empty_schema = StructType([StructField("NOTIFICATION", StringType(), True)])
    df_Notify = spark.createDataFrame([], empty_schema)

write_files(
    df_Research,
    f"{adls_path}/balancing/research/FctsIdsBillComsnResearch.dat.{RunID}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)

write_files(
    df_Notify,
    f"{adls_path}/balancing/notify/CommissionsBalancingNotification.dat",
    ",",
    "append",
    False,
    False,
    "\"",
    None
)