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
# MAGIC CALLED BY:    FctsIdsSubBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                        Development Project       Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                  ----------------------------------     ---------------------------------      -------------------------   
# MAGIC Parikshith Chada              04/03/2007          3264                              Originally Programmed                                     devlIDS30                    Brent Leland                   08-30-2007
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
from pyspark.sql import functions as F
from pyspark.sql import Window

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
ToleranceCd = get_widget_value('ToleranceCd','')
RunID = get_widget_value('RunID','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = f"""
SELECT
  SUB.SRC_SYS_CD_SK as SRC_SRC_SYS_CD_SK,
  SUB.SUB_UNIQ_KEY as SRC_SUB_UNIQ_KEY,
  B_SUB.SRC_SYS_CD_SK as TRGT_SRC_SYS_CD_SK,
  B_SUB.SUB_UNIQ_KEY as TRGT_SUB_UNIQ_KEY
FROM {IDSOwner}.SUB SUB
FULL OUTER JOIN {IDSOwner}.B_SUB B_SUB
  ON SUB.SRC_SYS_CD_SK = B_SUB.SRC_SYS_CD_SK
  AND SUB.SUB_UNIQ_KEY = B_SUB.SUB_UNIQ_KEY
WHERE SUB.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
"""

df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

w = Window.orderBy(F.lit(1))
df_SrcTrgtComp_withrownum = df_SrcTrgtComp.withColumn("_row_num", F.row_number().over(w))

df_research = df_SrcTrgtComp_withrownum.filter(
    F.col("SRC_SRC_SYS_CD_SK").isNull()
    | F.col("SRC_SUB_UNIQ_KEY").isNull()
    | F.col("TRGT_SRC_SYS_CD_SK").isNull()
    | F.col("TRGT_SUB_UNIQ_KEY").isNull()
)

df_ResearchFile = df_research.select(
    "TRGT_SRC_SYS_CD_SK",
    "TRGT_SUB_UNIQ_KEY",
    "SRC_SRC_SYS_CD_SK",
    "SRC_SUB_UNIQ_KEY"
)

if ToleranceCd == 'OUT':
    df_notify_temp = df_SrcTrgtComp_withrownum.filter(F.col("_row_num") == 1)
else:
    from pyspark.sql import types as T
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------

    df_notify_temp = spark.createDataFrame([], T.StructType([T.StructField("NOTIFICATION", T.StringType(), True)]))

df_NotifyFile = df_notify_temp.select(
    F.rpad(
        F.lit("ROW COUNT BALANCING FACETS - IDS SUB OUT OF TOLERANCE"),
        70,
        " "
    ).alias("NOTIFICATION")
)

write_files(
    df_ResearchFile,
    f"{adls_path}/balancing/research/FacetsIdsSubResearch.dat.{RunID}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)

write_files(
    df_NotifyFile,
    f"{adls_path}/balancing/notify/MbrshpBalancingNotification.dat",
    ",",
    "append",
    False,
    False,
    "\"",
    None
)