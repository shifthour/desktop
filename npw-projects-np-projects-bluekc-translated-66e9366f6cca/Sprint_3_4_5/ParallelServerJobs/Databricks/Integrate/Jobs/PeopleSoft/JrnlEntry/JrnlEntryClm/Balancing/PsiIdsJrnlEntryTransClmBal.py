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
# MAGIC CALLED BY:     PsiIdsJrnlEntryTransClmBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                     ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              05/08/2007           3264                              Originally Programmed                                     devlIDS30          
# MAGIC 
# MAGIC Parikshith Chada               8/21/2007         3264                              Modified the balancing process,                         devlIDS30                   Steph Goddard             09/15/2007
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
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


# Parameters
RunID = get_widget_value("RunID", "")
ToleranceCd = get_widget_value("ToleranceCd", "")
IDSOwner = get_widget_value("IDSOwner", "")
ExtrRunCycle = get_widget_value("ExtrRunCycle", "")
ids_secret_name = get_widget_value("ids_secret_name", "")

# DB Config
jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# Stage: SrcTrgtComp (DB2Connector)
extract_query = f"""SELECT
JRNL_ENTRY_TRANS.SRC_SYS_CD_SK,
JRNL_ENTRY_TRANS.SRC_TRANS_CK,
JRNL_ENTRY_TRANS.SRC_TRANS_TYP_CD_SK,
JRNL_ENTRY_TRANS.ACCTG_DT_SK,
B_JRNL_ENTRY_TRANS.SRC_SYS_CD_SK AS B_JRNL_ENTRY_TRANS_SRC_SYS_CD_SK,
B_JRNL_ENTRY_TRANS.SRC_TRANS_CK AS B_JRNL_ENTRY_TRANS_SRC_TRANS_CK,
B_JRNL_ENTRY_TRANS.SRC_TRANS_TYP_CD_SK AS B_JRNL_ENTRY_TRANS_SRC_TRANS_TYP_CD_SK,
B_JRNL_ENTRY_TRANS.ACCTG_DT_SK AS B_JRNL_ENTRY_TRANS_ACCTG_DT_SK
FROM {IDSOwner}.CD_MPPNG MPPNG,{IDSOwner}.JRNL_ENTRY_TRANS JRNL_ENTRY_TRANS 
FULL OUTER JOIN {IDSOwner}.B_JRNL_ENTRY_TRANS B_JRNL_ENTRY_TRANS
ON JRNL_ENTRY_TRANS.SRC_SYS_CD_SK = B_JRNL_ENTRY_TRANS.SRC_SYS_CD_SK 
AND JRNL_ENTRY_TRANS.SRC_TRANS_CK = B_JRNL_ENTRY_TRANS.SRC_TRANS_CK 
AND JRNL_ENTRY_TRANS.SRC_TRANS_TYP_CD_SK = B_JRNL_ENTRY_TRANS.SRC_TRANS_TYP_CD_SK
AND JRNL_ENTRY_TRANS.ACCTG_DT_SK = B_JRNL_ENTRY_TRANS.ACCTG_DT_SK
WHERE 
JRNL_ENTRY_TRANS.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
AND JRNL_ENTRY_TRANS.SRC_TRANS_TYP_CD_SK = MPPNG.CD_MPPNG_SK 
AND MPPNG.TRGT_CD = 'CLM'
"""

df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# Rename columns to match TransformLogic references
df_TransformLogic = (
    df_SrcTrgtComp
    .withColumnRenamed("SRC_SYS_CD_SK", "SRC_SRC_SYS_CD_SK")
    .withColumnRenamed("SRC_TRANS_CK", "SRC_SRC_TRANS_CK")
    .withColumnRenamed("SRC_TRANS_TYP_CD_SK", "SRC_SRC_TRANS_TYP_CD_SK")
    .withColumnRenamed("ACCTG_DT_SK", "SRC_ACCTG_DT_SK")
    .withColumnRenamed("B_JRNL_ENTRY_TRANS_SRC_SYS_CD_SK", "TRGT_SRC_SYS_CD_SK")
    .withColumnRenamed("B_JRNL_ENTRY_TRANS_SRC_TRANS_CK", "TRGT_SRC_TRANS_CK")
    .withColumnRenamed("B_JRNL_ENTRY_TRANS_SRC_TRANS_TYP_CD_SK", "TRGT_SRC_TRANS_TYP_CD_SK")
    .withColumnRenamed("B_JRNL_ENTRY_TRANS_ACCTG_DT_SK", "TRGT_ACCTG_DT_SK")
)

# Output link: Research
df_Research = df_TransformLogic.filter(
    (F.col("SRC_ACCTG_DT_SK").isNull())
    | (F.col("SRC_SRC_TRANS_CK").isNull())
    | (F.col("SRC_SRC_TRANS_TYP_CD_SK").isNull())
    | (F.col("SRC_SRC_SYS_CD_SK").isNull())
    | (F.col("TRGT_ACCTG_DT_SK").isNull())
    | (F.col("TRGT_SRC_TRANS_CK").isNull())
    | (F.col("TRGT_SRC_TRANS_TYP_CD_SK").isNull())
    | (F.col("TRGT_SRC_SYS_CD_SK").isNull())
)

df_ResearchFile = (
    df_Research
    .select(
        F.col("TRGT_SRC_SYS_CD_SK"),
        F.col("TRGT_SRC_TRANS_CK"),
        F.col("TRGT_SRC_TRANS_TYP_CD_SK"),
        F.rpad("TRGT_ACCTG_DT_SK", 10, " ").alias("TRGT_ACCTG_DT_SK"),
        F.col("SRC_SRC_SYS_CD_SK"),
        F.col("SRC_SRC_TRANS_CK"),
        F.col("SRC_SRC_TRANS_TYP_CD_SK"),
        F.rpad("SRC_ACCTG_DT_SK", 10, " ").alias("SRC_ACCTG_DT_SK")
    )
)

write_files(
    df_ResearchFile,
    f"{adls_path}/balancing/research/PsiIdsJrnlEntryTransClmResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\""
)

# Output link: Notify
df_count_TransformLogic = df_TransformLogic.limit(1).count()
if ToleranceCd == "OUT" and df_count_TransformLogic > 0:
    df_Notify = spark.createDataFrame(
        [("ROW COUNT BALANCING PSI - IDS JRNL ENTRY TRANS CLM OUT OF TOLERANCE",)],
        "NOTIFICATION: string"
    )
else:
    df_Notify = spark.createDataFrame([], "NOTIFICATION: string")

df_ErrorNotificationFile = df_Notify.select(
    F.rpad("NOTIFICATION", 70, " ").alias("NOTIFICATION")
)

write_files(
    df_ErrorNotificationFile,
    f"{adls_path}/balancing/notify/FinanceBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\""
)