# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:      PsiIdsJrnlEntryTransComsnBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                           Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                       ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada             05/09/2007          3264                              Originally Programmed                                        devlIDS30    
# MAGIC 
# MAGIC Parikshith Chada               8/21/2007         3264                              Modified the balancing process,                         devlIDS30                     Steph Goddard             09/15/2007
# MAGIC                                                                                                           by changing snapshot file to snapshot table   
# MAGIC 
# MAGIC Manasa Andru                  2017-06-26            TFS - 19354               Updated the performance parameters with         IntegrateDev1                Jag Yelavarthi             2017-06-30
# MAGIC                                                                                                      the right values to avoid job abend due to mutex error.

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
from pyspark.sql import Window
from pyspark.sql.functions import col, lit, row_number, rpad
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


ids_secret_name = get_widget_value('ids_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
RunID = get_widget_value('RunID','')
ToleranceCd = get_widget_value('ToleranceCd','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"""
SELECT 
JRNL_ENTRY_TRANS.SRC_SYS_CD_SK as SRC_SRC_SYS_CD_SK,
JRNL_ENTRY_TRANS.SRC_TRANS_CK as SRC_SRC_TRANS_CK,
JRNL_ENTRY_TRANS.SRC_TRANS_TYP_CD_SK as SRC_SRC_TRANS_TYP_CD_SK,
JRNL_ENTRY_TRANS.ACCTG_DT_SK as SRC_ACCTG_DT_SK,
B_JRNL_ENTRY_TRANS.SRC_SYS_CD_SK as TRGT_SRC_SYS_CD_SK,
B_JRNL_ENTRY_TRANS.SRC_TRANS_CK as TRGT_SRC_TRANS_CK,
B_JRNL_ENTRY_TRANS.SRC_TRANS_TYP_CD_SK as TRGT_SRC_TRANS_TYP_CD_SK,
B_JRNL_ENTRY_TRANS.ACCTG_DT_SK as TRGT_ACCTG_DT_SK
FROM {IDSOwner}.CD_MPPNG MPPNG
,{IDSOwner}.JRNL_ENTRY_TRANS JRNL_ENTRY_TRANS
FULL OUTER JOIN {IDSOwner}.B_JRNL_ENTRY_TRANS B_JRNL_ENTRY_TRANS
  ON JRNL_ENTRY_TRANS.SRC_SYS_CD_SK = B_JRNL_ENTRY_TRANS.SRC_SYS_CD_SK 
  AND JRNL_ENTRY_TRANS.SRC_TRANS_CK = B_JRNL_ENTRY_TRANS.SRC_TRANS_CK 
  AND JRNL_ENTRY_TRANS.SRC_TRANS_TYP_CD_SK = B_JRNL_ENTRY_TRANS.SRC_TRANS_TYP_CD_SK
  AND JRNL_ENTRY_TRANS.ACCTG_DT_SK = B_JRNL_ENTRY_TRANS.ACCTG_DT_SK
WHERE
  JRNL_ENTRY_TRANS.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
  AND JRNL_ENTRY_TRANS.SRC_TRANS_TYP_CD_SK = MPPNG.CD_MPPNG_SK
  AND MPPNG.TRGT_CD = 'COMSN'
"""
df_SrcTrgtComp = (
    spark.read.format("jdbc")
        .option("url", jdbc_url)
        .options(**jdbc_props)
        .option("query", extract_query)
        .load()
)

df_missing = df_SrcTrgtComp

research_condition = (
    col("SRC_ACCTG_DT_SK").isNull() |
    col("SRC_SRC_TRANS_CK").isNull() |
    col("SRC_SRC_TRANS_TYP_CD_SK").isNull() |
    col("SRC_SRC_SYS_CD_SK").isNull() |
    col("TRGT_ACCTG_DT_SK").isNull() |
    col("TRGT_SRC_TRANS_CK").isNull() |
    col("TRGT_SRC_TRANS_TYP_CD_SK").isNull() |
    col("TRGT_SRC_SYS_CD_SK").isNull()
)
df_Research = df_missing.filter(research_condition).select(
    "TRGT_SRC_SYS_CD_SK",
    "TRGT_SRC_TRANS_CK",
    "TRGT_SRC_TRANS_TYP_CD_SK",
    "TRGT_ACCTG_DT_SK",
    "SRC_SRC_SYS_CD_SK",
    "SRC_SRC_TRANS_CK",
    "SRC_SRC_TRANS_TYP_CD_SK",
    "SRC_ACCTG_DT_SK"
)

windowSpec = Window.orderBy(lit(1))
df_notify_in = df_missing.withColumn("_row_number", row_number().over(windowSpec))
df_Notify = df_notify_in.filter(
    (col("_row_number") == 1) & (lit(ToleranceCd) == lit("OUT"))
).select(
    lit("ROW COUNT BALANCING PSI - IDS JRNL ENTRY TRANS COMSN OUT OF TOLERANCE").alias("NOTIFICATION")
)

finalDF_Research = (
    df_Research
    .withColumn("TRGT_ACCTG_DT_SK", rpad("TRGT_ACCTG_DT_SK", 10, " "))
    .withColumn("SRC_ACCTG_DT_SK", rpad("SRC_ACCTG_DT_SK", 10, " "))
    .select(
        "TRGT_SRC_SYS_CD_SK",
        "TRGT_SRC_TRANS_CK",
        "TRGT_SRC_TRANS_TYP_CD_SK",
        "TRGT_ACCTG_DT_SK",
        "SRC_SRC_SYS_CD_SK",
        "SRC_SRC_TRANS_CK",
        "SRC_SRC_TRANS_TYP_CD_SK",
        "SRC_ACCTG_DT_SK"
    )
)
write_files(
    finalDF_Research,
    f"{adls_path}/balancing/research/PsiIdsJrnlEntryTransComsnResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

finalDF_Notify = df_Notify.withColumn("NOTIFICATION", rpad("NOTIFICATION", 70, " ")).select("NOTIFICATION")
write_files(
    finalDF_Notify,
    f"{adls_path}/balancing/notify/FinanceBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)