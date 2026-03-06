# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:   PsiIdsJrnlEntryTransDrugBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ---------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              05/09/2007          3264                              Originally Programmed                                     devlIDS30       
# MAGIC 
# MAGIC Parikshith Chada               8/21/2007         3264                              Modified the balancing process,                        devlIDS30                    Steph Goddard             09/15/2007
# MAGIC                                                                                                           by changing snapshot file to snapshot table  
# MAGIC 
# MAGIC Manasa Andru                  2017-06-26            TFS - 19354               Updated the performance parameters with         IntegrateDev1              Jag Yelavarthi             2017-06-30
# MAGIC                                                                                                       the right values to avoid job abend due to mutex error.

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
from pyspark.sql.functions import col, lit, rpad, isnull
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


RunID = get_widget_value('RunID','')
ToleranceCd = get_widget_value('ToleranceCd','')
IDSOwner = get_widget_value('IDSOwner','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
ids_secret_name = get_widget_value('ids_secret_name','')

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
FROM
{IDSOwner}.CD_MPPNG MPPNG,{IDSOwner}.JRNL_ENTRY_TRANS JRNL_ENTRY_TRANS
FULL OUTER JOIN {IDSOwner}.B_JRNL_ENTRY_TRANS B_JRNL_ENTRY_TRANS
ON JRNL_ENTRY_TRANS.SRC_SYS_CD_SK = B_JRNL_ENTRY_TRANS.SRC_SYS_CD_SK
AND JRNL_ENTRY_TRANS.SRC_TRANS_CK = B_JRNL_ENTRY_TRANS.SRC_TRANS_CK
AND JRNL_ENTRY_TRANS.SRC_TRANS_TYP_CD_SK = B_JRNL_ENTRY_TRANS.SRC_TRANS_TYP_CD_SK
AND JRNL_ENTRY_TRANS.ACCTG_DT_SK = B_JRNL_ENTRY_TRANS.ACCTG_DT_SK
WHERE
JRNL_ENTRY_TRANS.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
AND JRNL_ENTRY_TRANS.SRC_TRANS_TYP_CD_SK = MPPNG.CD_MPPNG_SK
AND MPPNG.TRGT_CD = 'DRUG'
"""

df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_Research = df_SrcTrgtComp.filter(
    (col("SRC_ACCTG_DT_SK").isNull()) |
    (col("SRC_SRC_TRANS_CK").isNull()) |
    (col("SRC_SRC_TRANS_TYP_CD_SK").isNull()) |
    (col("SRC_SRC_SYS_CD_SK").isNull()) |
    (col("TRGT_ACCTG_DT_SK").isNull()) |
    (col("TRGT_SRC_TRANS_CK").isNull()) |
    (col("TRGT_SRC_TRANS_TYP_CD_SK").isNull()) |
    (col("TRGT_SRC_SYS_CD_SK").isNull())
)

df_Research_out = df_Research.select(
    col("TRGT_SRC_SYS_CD_SK"),
    col("TRGT_SRC_TRANS_CK"),
    col("TRGT_SRC_TRANS_TYP_CD_SK"),
    rpad(col("TRGT_ACCTG_DT_SK"), 10, " ").alias("TRGT_ACCTG_DT_SK"),
    col("SRC_SRC_SYS_CD_SK"),
    col("SRC_SRC_TRANS_CK"),
    col("SRC_SRC_TRANS_TYP_CD_SK"),
    rpad(col("SRC_ACCTG_DT_SK"), 10, " ").alias("SRC_ACCTG_DT_SK")
)

df_Notify = df_SrcTrgtComp.filter(ToleranceCd == 'OUT').limit(1).select(
    lit("ROW COUNT BALANCING PSI - IDS JRNL ENTRY TRANS DRUG OUT OF TOLERANCE").alias("NOTIFICATION")
)

df_Notify_out = df_Notify.select(
    rpad(col("NOTIFICATION"), 70, " ").alias("NOTIFICATION")
)

write_files(
    df_Research_out,
    f"{adls_path}/balancing/research/PsiIdsJrnlEntryTransDrugResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

write_files(
    df_Notify_out,
    f"{adls_path}/balancing/notify/FinanceBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)