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
# MAGIC CALLED BY:       PsiIdsCapTransBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                              Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                        ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              05/04/2007         3264                              Originally Programmed                                         devlIDS30               
# MAGIC 
# MAGIC Parikshith Chada               8/21/2007         3264                              Modified the balancing process,                           devlIDS30                       Steph Goddard            09/15/2007
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


ids_secret_name = get_widget_value('ids_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
RunID = get_widget_value('RunID','')
ToleranceCd = get_widget_value('ToleranceCd','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"SELECT CAP_TRANS.SRC_SYS_CD_SK as SRC_SRC_SYS_CD_SK, CAP_TRANS.CAP_TRANS_CK as SRC_CAP_TRANS_CK, CAP_TRANS.ACCTG_DT_SK as SRC_ACCTG_DT_SK, B_CAP_TRANS.SRC_SYS_CD_SK as TRGT_SRC_SYS_CD_SK, B_CAP_TRANS.CAP_TRANS_CK as TRGT_CAP_TRANS_CK, B_CAP_TRANS.ACCTG_DT_SK as TRGT_ACCTG_DT_SK FROM {IDSOwner}.CAP_TRANS CAP_TRANS FULL OUTER JOIN {IDSOwner}.B_CAP_TRANS B_CAP_TRANS ON CAP_TRANS.SRC_SYS_CD_SK = B_CAP_TRANS.SRC_SYS_CD_SK AND CAP_TRANS.CAP_TRANS_CK = B_CAP_TRANS.CAP_TRANS_CK AND CAP_TRANS.ACCTG_DT_SK = B_CAP_TRANS.ACCTG_DT_SK WHERE CAP_TRANS.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}"

df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_TransformLogic = df_SrcTrgtComp

df_Research = df_TransformLogic.filter(
    F.col("SRC_SRC_SYS_CD_SK").isNull()
    | F.col("SRC_CAP_TRANS_CK").isNull()
    | F.col("SRC_ACCTG_DT_SK").isNull()
    | F.col("TRGT_SRC_SYS_CD_SK").isNull()
    | F.col("TRGT_CAP_TRANS_CK").isNull()
    | F.col("TRGT_ACCTG_DT_SK").isNull()
)

df_Research = df_Research.select(
    F.col("TRGT_SRC_SYS_CD_SK"),
    F.col("TRGT_CAP_TRANS_CK"),
    F.col("TRGT_ACCTG_DT_SK"),
    F.col("SRC_SRC_SYS_CD_SK"),
    F.col("SRC_CAP_TRANS_CK"),
    F.col("SRC_ACCTG_DT_SK")
)

df_Research = (
    df_Research
    .withColumn("TRGT_ACCTG_DT_SK", rpad("TRGT_ACCTG_DT_SK", 10, " "))
    .withColumn("SRC_ACCTG_DT_SK", rpad("SRC_ACCTG_DT_SK", 10, " "))
)

if ToleranceCd == 'OUT':
    df_NotifyCandidate = df_TransformLogic.limit(1)
    df_NotifyCandidate = df_NotifyCandidate.withColumn("NOTIFICATION", F.lit("ROW COUNT BALANCING PSI - IDS CAP TRANS OUT OF TOLERANCE"))
    df_NotifyCandidate = df_NotifyCandidate.withColumn("NOTIFICATION", rpad("NOTIFICATION", 70, " "))
    df_Notify = df_NotifyCandidate.select("NOTIFICATION")
else:
    empty_schema = StructType([StructField("NOTIFICATION", StringType(), True)])
    df_Notify = spark.createDataFrame([], empty_schema)

write_files(
    df_Research,
    f"{adls_path}/balancing/research/PsiIdsCapTransResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

write_files(
    df_Notify,
    f"{adls_path}/balancing/notify/FinanceBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)