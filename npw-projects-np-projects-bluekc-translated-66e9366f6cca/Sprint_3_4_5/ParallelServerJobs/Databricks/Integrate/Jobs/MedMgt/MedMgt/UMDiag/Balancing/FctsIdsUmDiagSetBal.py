# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 01/30/08 05:00:10 Batch  14640_18019 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 10/31/07 13:54:42 Batch  14549_50100 PROMOTE bckcetl ids20 dsadm bls for on
# MAGIC ^1_1 10/31/07 13:39:50 Batch  14549_49193 INIT bckcett testIDS30 dsadm bls for on
# MAGIC ^1_1 10/30/07 07:53:48 Batch  14548_28437 PROMOTE bckcett testIDS30 u03651 steffy
# MAGIC ^1_1 10/30/07 07:44:34 Batch  14548_27877 INIT bckcett devlIDS30 u03651 steffy
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:      FctsIdsUmDiagSetBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                      Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                 ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              2007-04-11          3264                              Originally Programmed                                   devlIDS30                     
# MAGIC 
# MAGIC Parikshith Chada               8/23/2007         3264                              Modified the balancing process,                    devliDS30                      Steph Goddard             9/14/07
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
from pyspark.sql import Row
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
ToleranceCd = get_widget_value('ToleranceCd','')
RunID = get_widget_value('RunID','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = (
    f"SELECT "
    f"UM_DIAG_SET.SRC_SYS_CD_SK, "
    f"UM_DIAG_SET.UM_REF_ID, "
    f"UM_DIAG_SET.DIAG_SET_CRT_DTM, "
    f"UM_DIAG_SET.SEQ_NO, "
    f"B_UM_DIAG_SET.SRC_SYS_CD_SK, "
    f"B_UM_DIAG_SET.UM_REF_ID, "
    f"B_UM_DIAG_SET.DIAG_SET_CRT_DTM, "
    f"B_UM_DIAG_SET.SEQ_NO "
    f"FROM {IDSOwner}.UM_DIAG_SET UM_DIAG_SET "
    f"FULL OUTER JOIN {IDSOwner}.B_UM_DIAG_SET B_UM_DIAG_SET "
    f"ON UM_DIAG_SET.SRC_SYS_CD_SK = B_UM_DIAG_SET.SRC_SYS_CD_SK "
    f"AND UM_DIAG_SET.UM_REF_ID = B_UM_DIAG_SET.UM_REF_ID "
    f"AND UM_DIAG_SET.DIAG_SET_CRT_DTM = B_UM_DIAG_SET.DIAG_SET_CRT_DTM "
    f"AND UM_DIAG_SET.SEQ_NO = B_UM_DIAG_SET.SEQ_NO "
    f"WHERE UM_DIAG_SET.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}"
)
df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_SrcTrgtComp = (
    df_SrcTrgtComp
    .withColumnRenamed("SRC_SYS_CD_SK", "SRC_SRC_SYS_CD_SK")
    .withColumnRenamed("UM_REF_ID", "SRC_UM_REF_ID")
    .withColumnRenamed("DIAG_SET_CRT_DTM", "SRC_DIAG_SET_CRT_DTM")
    .withColumnRenamed("SEQ_NO", "SRC_SEQ_NO")
    .withColumnRenamed("SRC_SYS_CD_SK_1", "TRGT_SRC_SYS_CD_SK")
    .withColumnRenamed("UM_REF_ID_1", "TRGT_UM_REF_ID")
    .withColumnRenamed("DIAG_SET_CRT_DTM_1", "TRGT_DIAG_SET_CRT_DTM")
    .withColumnRenamed("SEQ_NO_1", "TRGT_SEQ_NO")
)

df_research = df_SrcTrgtComp.filter(
    F.isnull("SRC_SRC_SYS_CD_SK")
    | F.isnull("SRC_UM_REF_ID")
    | F.isnull("SRC_DIAG_SET_CRT_DTM")
    | F.isnull("SRC_SEQ_NO")
    | F.isnull("TRGT_SRC_SYS_CD_SK")
    | F.isnull("TRGT_UM_REF_ID")
    | F.isnull("TRGT_DIAG_SET_CRT_DTM")
    | F.isnull("TRGT_SEQ_NO")
).select(
    "TRGT_SRC_SYS_CD_SK",
    "TRGT_UM_REF_ID",
    "TRGT_DIAG_SET_CRT_DTM",
    "TRGT_SEQ_NO",
    "SRC_SRC_SYS_CD_SK",
    "SRC_UM_REF_ID",
    "SRC_DIAG_SET_CRT_DTM",
    "SRC_SEQ_NO"
)

if ToleranceCd == 'OUT':
    df_notify_init = df_SrcTrgtComp.limit(1)
    df_notify = df_notify_init.withColumn(
        "NOTIFICATION",
        F.lit("ROW COUNT BALANCING FACETS - IDS UM DIAG SET OUT OF TOLERANCE")
    )
else:
    df_notify = spark.createDataFrame([], "NOTIFICATION STRING")

df_notify = df_notify.select(
    F.rpad(F.col("NOTIFICATION"), 70, " ").alias("NOTIFICATION")
)

write_files(
    df_research,
    f"{adls_path}/balancing/research/FacetsIdsUmDiagSetResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

write_files(
    df_notify,
    f"{adls_path}/balancing/notify/MedMgtBalancingNotification.txt",
    delimiter=",",
    mode="append",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)