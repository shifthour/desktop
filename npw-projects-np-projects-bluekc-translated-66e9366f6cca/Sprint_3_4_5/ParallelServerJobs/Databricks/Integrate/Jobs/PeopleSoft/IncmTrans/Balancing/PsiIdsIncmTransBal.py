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
# MAGIC CALLED BY:  PsiIdsIncmTransBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                       Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                   ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              05/08/2007         3264                              Originally Programmed                                       devlIDS30     
# MAGIC 
# MAGIC Parikshith Chada               8/21/2007         3264                              Modified the balancing process,                         devlIDS30                  Steph Goddard            09/15/2007
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


RunID = get_widget_value("RunID","")
ToleranceCd = get_widget_value("ToleranceCd","")
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
ExtrRunCycle = get_widget_value("ExtrRunCycle","")

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = f"""
SELECT
INCM_TRANS.SRC_SYS_CD_SK AS SRC_SRC_SYS_CD_SK,
INCM_TRANS.INCM_TRANS_CK AS SRC_INCM_TRANS_CK,
INCM_TRANS.ACCTG_DT_SK AS SRC_ACCTG_DT_SK,
B_INCM_TRANS.SRC_SYS_CD_SK AS TRGT_SRC_SYS_CD_SK,
B_INCM_TRANS.INCM_TRANS_CK AS TRGT_INCM_TRANS_CK,
B_INCM_TRANS.ACCTG_DT_SK AS TRGT_ACCTG_DT_SK
FROM {IDSOwner}.INCM_TRANS INCM_TRANS
FULL OUTER JOIN {IDSOwner}.B_INCM_TRANS B_INCM_TRANS
ON INCM_TRANS.SRC_SYS_CD_SK = B_INCM_TRANS.SRC_SYS_CD_SK
AND INCM_TRANS.INCM_TRANS_CK = B_INCM_TRANS.INCM_TRANS_CK
AND INCM_TRANS.ACCTG_DT_SK = B_INCM_TRANS.ACCTG_DT_SK
WHERE INCM_TRANS.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
"""

df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_research = df_SrcTrgtComp.filter(
    F.col("SRC_SRC_SYS_CD_SK").isNull()
    | F.col("SRC_INCM_TRANS_CK").isNull()
    | F.col("SRC_ACCTG_DT_SK").isNull()
    | F.col("TRGT_SRC_SYS_CD_SK").isNull()
    | F.col("TRGT_INCM_TRANS_CK").isNull()
    | F.col("TRGT_ACCTG_DT_SK").isNull()
)

df_research_select = df_research.select(
    "TRGT_SRC_SYS_CD_SK",
    "TRGT_INCM_TRANS_CK",
    "TRGT_ACCTG_DT_SK",
    "SRC_SRC_SYS_CD_SK",
    "SRC_INCM_TRANS_CK",
    "SRC_ACCTG_DT_SK"
)

df_research_final = (
    df_research_select
    .withColumn("TRGT_ACCTG_DT_SK", F.rpad(F.col("TRGT_ACCTG_DT_SK"), 10, " "))
    .withColumn("SRC_ACCTG_DT_SK", F.rpad(F.col("SRC_ACCTG_DT_SK"), 10, " "))
)

write_files(
    df_research_final,
    f"{adls_path}/balancing/research/PsiIdsIncmTransResearch.dat.{RunID}",
    ',',
    'overwrite',
    False,
    False,
    '\"',
    None
)

df_notify_temp = df_SrcTrgtComp.filter(F.lit(ToleranceCd) == 'OUT').limit(1)
df_notify_temp = df_notify_temp.withColumn("NOTIFICATION", F.lit("ROW COUNT BALANCING PSI - IDS INCM TRANS OUT OF TOLERANCE"))
df_notify_temp = df_notify_temp.select("NOTIFICATION")
df_notify_final = df_notify_temp.withColumn("NOTIFICATION", F.rpad(F.col("NOTIFICATION"), 70, " "))

write_files(
    df_notify_final,
    f"{adls_path}/balancing/notify/FinanceBalancingNotification.dat",
    ',',
    'append',
    False,
    False,
    '\"',
    None
)