# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 01/28/08 13:39:53 Batch  14638_49197 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 01/10/08 10:01:22 Batch  14620_36086 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 12/26/07 15:33:58 Batch  14605_56043 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/23/07 15:42:37 Batch  14572_56566 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 09/25/07 16:07:19 Batch  14513_58043 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 09/20/07 14:24:47 Batch  14508_51893 PROMOTE bckcetl ids20 dsadm bls for bl
# MAGIC ^1_1 09/20/07 10:51:09 Batch  14508_39073 INIT bckcett testIDS30 dsadm bls for bl
# MAGIC ^1_1 09/19/07 08:11:02 Batch  14507_29467 PROMOTE bckcett testIDS30 u08717 brent
# MAGIC ^1_1 09/19/07 07:44:31 Batch  14507_27876 INIT bckcett devlIDS30 u08717 Brent
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:       FctsIdGrpAtchmtBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                     Development Project           Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                ---------------------------------          ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              3/23/2007          3264                              Originally Programmed                                      devlIDS30                        Brent Leland                08-30-2007
# MAGIC                                                                                                           Modified the balancing process, 
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
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
ToleranceCd = get_widget_value('ToleranceCd','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
RunID = get_widget_value('RunID','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = f"""
SELECT 
GRP_ATCHMT.SRC_SYS_CD_SK as SRC_SRC_SYS_CD_SK,
GRP_ATCHMT.GRP_ID as SRC_GRP_ID,
GRP_ATCHMT.GRP_ATCHMT_ID as SRC_GRP_ATCHMT_ID,
GRP_ATCHMT.GRP_ATCHMT_DTM as SRC_GRP_ATCHMT_DTM,
GRP_ATCHMT.GRP_ATCHMT_LAST_UPDT_DTM as SRC_GRP_ATCHMT_LAST_UPDT_DTM,
B_GRP_ATCHMT.SRC_SYS_CD_SK as TRGT_SRC_SYS_CD_SK,
B_GRP_ATCHMT.GRP_ID as TRGT_GRP_ID,
B_GRP_ATCHMT.GRP_ATCHMT_ID as TRGT_GRP_ATCHMT_ID,
B_GRP_ATCHMT.GRP_ATCHMT_DTM as TRGT_GRP_ATCHMT_DTM,
B_GRP_ATCHMT.GRP_ATCHMT_LAST_UPDT_DTM as TRGT_GRP_ATCHMT_LAST_UPDT_DTM
FROM {IDSOwner}.GRP_ATCHMT GRP_ATCHMT
FULL OUTER JOIN {IDSOwner}.B_GRP_ATCHMT B_GRP_ATCHMT
      ON GRP_ATCHMT.SRC_SYS_CD_SK = B_GRP_ATCHMT.SRC_SYS_CD_SK
      AND GRP_ATCHMT.GRP_ID = B_GRP_ATCHMT.GRP_ID
      AND GRP_ATCHMT.GRP_ATCHMT_ID = B_GRP_ATCHMT.GRP_ATCHMT_ID
      AND GRP_ATCHMT.GRP_ATCHMT_DTM = B_GRP_ATCHMT.GRP_ATCHMT_DTM
      AND GRP_ATCHMT.GRP_ATCHMT_LAST_UPDT_DTM = B_GRP_ATCHMT.GRP_ATCHMT_LAST_UPDT_DTM
WHERE GRP_ATCHMT.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
"""

df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_Research = df_SrcTrgtComp.filter(
    col('SRC_SRC_SYS_CD_SK').isNull() |
    col('SRC_GRP_ID').isNull() |
    col('SRC_GRP_ATCHMT_ID').isNull() |
    col('SRC_GRP_ATCHMT_DTM').isNull() |
    col('SRC_GRP_ATCHMT_LAST_UPDT_DTM').isNull() |
    col('TRGT_SRC_SYS_CD_SK').isNull() |
    col('TRGT_GRP_ID').isNull() |
    col('TRGT_GRP_ATCHMT_ID').isNull() |
    col('TRGT_GRP_ATCHMT_DTM').isNull() |
    col('TRGT_GRP_ATCHMT_LAST_UPDT_DTM').isNull()
)

df_Research = df_Research.select(
    'TRGT_SRC_SYS_CD_SK',
    'TRGT_GRP_ID',
    'TRGT_GRP_ATCHMT_ID',
    'TRGT_GRP_ATCHMT_DTM',
    'TRGT_GRP_ATCHMT_LAST_UPDT_DTM',
    'SRC_SRC_SYS_CD_SK',
    'SRC_GRP_ID',
    'SRC_GRP_ATCHMT_ID',
    'SRC_GRP_ATCHMT_DTM',
    'SRC_GRP_ATCHMT_LAST_UPDT_DTM'
)

write_files(
    df_Research,
    f"{adls_path}/balancing/research/FacetsIdsGrpAtchmtResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

rowCount = df_SrcTrgtComp.limit(1).count()
if rowCount > 0 and ToleranceCd == 'OUT':
    df_Notify = spark.createDataFrame(
        [("ROW COUNT BALANCING FACETS - IDS GRP ATCHMT OUT OF TOLERANCE",)],
        ["NOTIFICATION"]
    )
else:
    df_Notify = spark.createDataFrame([], StructType([StructField("NOTIFICATION", StringType(), True)]))

df_Notify = df_Notify.withColumn("NOTIFICATION", rpad("NOTIFICATION", 70, " "))

write_files(
    df_Notify,
    f"{adls_path}/balancing/notify/MbrshpBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)