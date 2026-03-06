# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 10/24/07 09:59:24 Batch  14542_35967 PROMOTE bckcetl ids20 dsadm bls for on
# MAGIC ^1_1 10/24/07 09:54:15 Batch  14542_35658 INIT bckcett testIDS30 dsadm bls for on
# MAGIC ^1_2 10/17/07 13:59:14 Batch  14535_50357 PROMOTE bckcett testIDS30 u03651 steffy
# MAGIC ^1_2 10/17/07 13:54:06 Batch  14535_50077 INIT bckcett devlIDS30 u03651 steffy
# MAGIC ^1_1 10/16/07 09:44:13 Batch  14534_35065 INIT bckcett devlIDS30 u03651 steffy
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:   FctsIdsCapBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                     ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              04/19/2007          3264                              Originally Programmed                                     devlIDS30         
# MAGIC 
# MAGIC Parikshith Chada               8/28/2007         3264                              Modified the balancing process,                       devlIDS30                    Steph Goddard              09/19/2007
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


RunID = get_widget_value('RunID', '')
ToleranceCd = get_widget_value('ToleranceCd', '')
IDSOwner = get_widget_value('IDSOwner', '')
ExtrRunCycle = get_widget_value('ExtrRunCycle', '')
ids_secret_name = get_widget_value('ids_secret_name', '')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"""SELECT
  CAP.SRC_SYS_CD_SK AS SRC_SRC_SYS_CD_SK,
  CAP.MBR_UNIQ_KEY AS SRC_MBR_UNIQ_KEY,
  CAP.CAP_PROV_ID AS SRC_CAP_PROV_ID,
  CAP.ERN_DT_SK AS SRC_ERN_DT_SK,
  CAP.PD_DT_SK AS SRC_PD_DT_SK,
  CAP.CAP_FUND_ID AS SRC_CAP_FUND_ID,
  CAP.CAP_POOL_CD_SK AS SRC_CAP_POOL_CD_SK,
  CAP.SEQ_NO AS SRC_SEQ_NO,
  B_CAP.SRC_SYS_CD_SK AS TRGT_SRC_SYS_CD_SK,
  B_CAP.MBR_UNIQ_KEY AS TRGT_MBR_UNIQ_KEY,
  B_CAP.CAP_PROV_ID AS TRGT_CAP_PROV_ID,
  B_CAP.ERN_DT_SK AS TRGT_ERN_DT_SK,
  B_CAP.PD_DT_SK AS TRGT_PD_DT_SK,
  B_CAP.CAP_FUND_ID AS TRGT_CAP_FUND_ID,
  B_CAP.CAP_POOL_CD_SK AS TRGT_CAP_POOL_CD_SK,
  B_CAP.SEQ_NO AS TRGT_SEQ_NO
FROM {IDSOwner}.CAP CAP
FULL OUTER JOIN {IDSOwner}.B_CAP B_CAP
  ON CAP.SRC_SYS_CD_SK = B_CAP.SRC_SYS_CD_SK
  AND CAP.MBR_UNIQ_KEY = B_CAP.MBR_UNIQ_KEY
  AND CAP.CAP_PROV_ID = B_CAP.CAP_PROV_ID
  AND CAP.ERN_DT_SK = B_CAP.ERN_DT_SK
  AND CAP.PD_DT_SK = B_CAP.PD_DT_SK
  AND CAP.CAP_FUND_ID = B_CAP.CAP_FUND_ID
  AND CAP.CAP_POOL_CD_SK = B_CAP.CAP_POOL_CD_SK
  AND CAP.SEQ_NO = B_CAP.SEQ_NO
WHERE CAP.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
"""

df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_Research = df_SrcTrgtComp.filter(
    F.col("SRC_CAP_FUND_ID").isNull()
    | F.col("SRC_CAP_POOL_CD_SK").isNull()
    | F.col("SRC_CAP_PROV_ID").isNull()
    | F.col("SRC_ERN_DT_SK").isNull()
    | F.col("SRC_MBR_UNIQ_KEY").isNull()
    | F.col("SRC_PD_DT_SK").isNull()
    | F.col("SRC_SEQ_NO").isNull()
    | F.col("SRC_SRC_SYS_CD_SK").isNull()
    | F.col("TRGT_CAP_FUND_ID").isNull()
    | F.col("TRGT_CAP_POOL_CD_SK").isNull()
    | F.col("TRGT_CAP_PROV_ID").isNull()
    | F.col("TRGT_ERN_DT_SK").isNull()
    | F.col("TRGT_MBR_UNIQ_KEY").isNull()
    | F.col("TRGT_PD_DT_SK").isNull()
    | F.col("TRGT_SEQ_NO").isNull()
    | F.col("TRGT_SRC_SYS_CD_SK").isNull()
)

df_Research = df_Research.withColumn(
    "TRGT_ERN_DT_SK", F.rpad(F.col("TRGT_ERN_DT_SK"), 10, " ")
).withColumn(
    "TRGT_PD_DT_SK", F.rpad(F.col("TRGT_PD_DT_SK"), 10, " ")
).withColumn(
    "SRC_ERN_DT_SK", F.rpad(F.col("SRC_ERN_DT_SK"), 10, " ")
).withColumn(
    "SRC_PD_DT_SK", F.rpad(F.col("SRC_PD_DT_SK"), 10, " ")
)

df_Research_final = df_Research.select(
    "TRGT_SRC_SYS_CD_SK",
    "TRGT_MBR_UNIQ_KEY",
    "TRGT_CAP_PROV_ID",
    "TRGT_ERN_DT_SK",
    "TRGT_PD_DT_SK",
    "TRGT_CAP_FUND_ID",
    "TRGT_CAP_POOL_CD_SK",
    "TRGT_SEQ_NO",
    "SRC_SRC_SYS_CD_SK",
    "SRC_MBR_UNIQ_KEY",
    "SRC_CAP_PROV_ID",
    "SRC_ERN_DT_SK",
    "SRC_PD_DT_SK",
    "SRC_CAP_FUND_ID",
    "SRC_CAP_POOL_CD_SK",
    "SRC_SEQ_NO"
)

write_files(
    df_Research_final,
    f"{adls_path}/balancing/research/FacetsIdsCapResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

if ToleranceCd == 'OUT':
    single_row_data = [("ROW COUNT BALANCING FACETS - IDS CAP OUT OF TOLERANCE",)]
    df_Notify = spark.createDataFrame(single_row_data, ["NOTIFICATION"])
    df_Notify = df_Notify.withColumn("NOTIFICATION", F.rpad(F.col("NOTIFICATION"), 70, " "))
else:
    empty_schema = StructType([StructField("NOTIFICATION", StringType(), True)])
    df_Notify = spark.createDataFrame([], empty_schema)

df_Notify_final = df_Notify.select("NOTIFICATION")

write_files(
    df_Notify_final,
    f"{adls_path}/balancing/notify/CapitationBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)