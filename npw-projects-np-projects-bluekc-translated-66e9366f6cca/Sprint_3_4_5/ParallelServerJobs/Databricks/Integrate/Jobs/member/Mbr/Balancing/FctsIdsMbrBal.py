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
# MAGIC CALLED BY:      FctsIdsMbrBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                      Development Project         Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                ----------------------------------       ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              04/06/2007          3264                              Originally Programmed                                   devlIDS30                        Brent Leland               08-30-2007 
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


RunID = get_widget_value('RunID','')
ToleranceCd = get_widget_value('ToleranceCd','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
query_SrcTrgtComp = f"""
SELECT 
  MBR.SRC_SYS_CD_SK AS SRC_SRC_SYS_CD_SK,
  MBR.MBR_UNIQ_KEY AS SRC_MBR_UNIQ_KEY,
  B_MBR.SRC_SYS_CD_SK AS TRGT_SRC_SYS_CD_SK,
  B_MBR.MBR_UNIQ_KEY AS TRGT_MBR_UNIQ_KEY
FROM {IDSOwner}.MBR MBR
FULL OUTER JOIN {IDSOwner}.B_MBR B_MBR
  ON MBR.SRC_SYS_CD_SK = B_MBR.SRC_SYS_CD_SK
    AND MBR.MBR_UNIQ_KEY = B_MBR.MBR_UNIQ_KEY
WHERE 
  MBR.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
"""
df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_SrcTrgtComp)
    .load()
)

df_Research = df_SrcTrgtComp.filter(
    (col("SRC_MBR_UNIQ_KEY").isNull())
    | (col("SRC_SRC_SYS_CD_SK").isNull())
    | (col("TRGT_MBR_UNIQ_KEY").isNull())
    | (col("TRGT_SRC_SYS_CD_SK").isNull())
).select(
    col("TRGT_SRC_SYS_CD_SK"),
    col("TRGT_MBR_UNIQ_KEY"),
    col("SRC_SRC_SYS_CD_SK"),
    col("SRC_MBR_UNIQ_KEY")
)

if ToleranceCd == 'OUT':
    df_temp = df_SrcTrgtComp.limit(1)
    if df_temp.count() > 0:
        df_Notify = spark.createDataFrame(
            [("ROW COUNT BALANCING FACETS - IDS MBR OUT OF TOLERANCE",)],
            ["NOTIFICATION"]
        )
    else:
        df_Notify = spark.createDataFrame([], StructType([StructField("NOTIFICATION", StringType(), True)]))
else:
    df_Notify = spark.createDataFrame([], StructType([StructField("NOTIFICATION", StringType(), True)]))

df_Notify = df_Notify.select(rpad(col("NOTIFICATION"), 70, " ").alias("NOTIFICATION"))

write_files(
    df_Research,
    f"{adls_path}/balancing/research/FacetsIdsMbrResearch.dat.{RunID}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)

write_files(
    df_Notify,
    f"{adls_path}/balancing/notify/MbrshpBalancingNotification.dat",
    ",",
    "append",
    False,
    False,
    "\"",
    None
)