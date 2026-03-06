# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 10/10/07 14:54:28 Batch  14528_53717 PROMOTE bckcetl ids20 dsadm bls for bl
# MAGIC ^1_1 10/10/07 14:22:50 Batch  14528_51775 INIT bckcett testIDS30 dsadm bls for bl
# MAGIC ^1_1 10/08/07 17:14:50 Batch  14526_62095 PROMOTE bckcett testIDS30 u08717 Brent
# MAGIC ^1_1 10/08/07 16:58:04 Batch  14526_61088 INIT bckcett devlIDS30 u08717 Brent
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:    PsiIdsJrnlEntryTransCapBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                           Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada             05/08/2007           3264                              Originally Programmed                                      devlIDS30                     
# MAGIC 
# MAGIC Parikshith Chada               8/21/2007         3264                              Modified the balancing process,                         devlIDS30                     Steph Goddard            09/15/2007
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
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql.functions import col, lit, row_number, rpad
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


# Parameters
RunID = get_widget_value('RunID','')
ToleranceCd = get_widget_value('ToleranceCd','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')

# DB2Connector (Stage: SrcTrgtComp)
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"""
SELECT 
  JRNL_ENTRY_TRANS.SRC_SYS_CD_SK AS SRC_SRC_SYS_CD_SK,
  JRNL_ENTRY_TRANS.SRC_TRANS_CK AS SRC_SRC_TRANS_CK,
  JRNL_ENTRY_TRANS.SRC_TRANS_TYP_CD_SK AS SRC_SRC_TRANS_TYP_CD_SK,
  JRNL_ENTRY_TRANS.ACCTG_DT_SK AS SRC_ACCTG_DT_SK,
  B_JRNL_ENTRY_TRANS.SRC_SYS_CD_SK AS TRGT_SRC_SYS_CD_SK,
  B_JRNL_ENTRY_TRANS.SRC_TRANS_CK AS TRGT_SRC_TRANS_CK,
  B_JRNL_ENTRY_TRANS.SRC_TRANS_TYP_CD_SK AS TRGT_SRC_TRANS_TYP_CD_SK,
  B_JRNL_ENTRY_TRANS.ACCTG_DT_SK AS TRGT_ACCTG_DT_SK
FROM {IDSOwner}.CD_MPPNG MPPNG,
     {IDSOwner}.JRNL_ENTRY_TRANS JRNL_ENTRY_TRANS
     FULL OUTER JOIN {IDSOwner}.B_JRNL_ENTRY_TRANS B_JRNL_ENTRY_TRANS
       ON JRNL_ENTRY_TRANS.SRC_SYS_CD_SK = B_JRNL_ENTRY_TRANS.SRC_SYS_CD_SK 
      AND JRNL_ENTRY_TRANS.SRC_TRANS_CK = B_JRNL_ENTRY_TRANS.SRC_TRANS_CK 
      AND JRNL_ENTRY_TRANS.SRC_TRANS_TYP_CD_SK = B_JRNL_ENTRY_TRANS.SRC_TRANS_TYP_CD_SK
      AND JRNL_ENTRY_TRANS.ACCTG_DT_SK = B_JRNL_ENTRY_TRANS.ACCTG_DT_SK
WHERE JRNL_ENTRY_TRANS.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
  AND JRNL_ENTRY_TRANS.SRC_TRANS_TYP_CD_SK = MPPNG.CD_MPPNG_SK
  AND MPPNG.TRGT_CD = 'CAP'
"""
df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# Transformer (Stage: TransformLogic) - "Research" link
df_ResearchCondition = (
    (col("SRC_SRC_SYS_CD_SK").isNull())
    | (col("SRC_SRC_TRANS_CK").isNull())
    | (col("SRC_SRC_TRANS_TYP_CD_SK").isNull())
    | (col("SRC_ACCTG_DT_SK").isNull())
    | (col("TRGT_SRC_SYS_CD_SK").isNull())
    | (col("TRGT_SRC_TRANS_CK").isNull())
    | (col("TRGT_SRC_TRANS_TYP_CD_SK").isNull())
    | (col("TRGT_ACCTG_DT_SK").isNull())
)
df_Research = df_SrcTrgtComp.filter(df_ResearchCondition)
df_Research = df_Research.withColumn(
    "TRGT_ACCTG_DT_SK", rpad(col("TRGT_ACCTG_DT_SK"), 10, " ")
)
df_Research = df_Research.withColumn(
    "SRC_ACCTG_DT_SK", rpad(col("SRC_ACCTG_DT_SK"), 10, " ")
)
df_Research = df_Research.select(
    "TRGT_SRC_SYS_CD_SK",
    "TRGT_SRC_TRANS_CK",
    "TRGT_SRC_TRANS_TYP_CD_SK",
    "TRGT_ACCTG_DT_SK",
    "SRC_SRC_SYS_CD_SK",
    "SRC_SRC_TRANS_CK",
    "SRC_SRC_TRANS_TYP_CD_SK",
    "SRC_ACCTG_DT_SK"
)

# CSeqFileStage (Stage: ResearchFile)
write_files(
    df_Research,
    f"{adls_path}/balancing/research/PsiIdsJrnlEntryTransCapResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

# Transformer (Stage: TransformLogic) - "Notify" link
windowSpec = Window.orderBy(lit(1))
df_SrcTrgtCompWithRowNum = df_SrcTrgtComp.withColumn(
    "row_number", row_number().over(windowSpec)
)
df_Notify = df_SrcTrgtCompWithRowNum.filter(
    (col("row_number") == 1) & (lit(ToleranceCd) == "OUT")
)
df_Notify = df_Notify.withColumn(
    "NOTIFICATION",
    rpad(
        lit("ROW COUNT BALANCING PSI - IDS JRNL ENTRY TRANS CAP OUT OF TOLERANCE"),
        70,
        " "
    )
)
df_Notify = df_Notify.select("NOTIFICATION")

# CSeqFileStage (Stage: ErrorNotificationFile)
write_files(
    df_Notify,
    f"{adls_path}/balancing/notify/FinanceBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)