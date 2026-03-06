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
# MAGIC ^1_1 09/20/07 14:41:37 Batch  14508_52916 PROMOTE bckcetl ids20 dsadm bls for bl
# MAGIC ^1_1 09/20/07 13:58:24 Batch  14508_50316 INIT bckcett testIDS30 dsadm bls for bl
# MAGIC ^1_1 09/19/07 08:11:02 Batch  14507_29467 PROMOTE bckcett testIDS30 u08717 brent
# MAGIC ^1_1 09/19/07 07:44:31 Batch  14507_27876 INIT bckcett devlIDS30 u08717 Brent
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:        FctsIdsSubGrpRateBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                        Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                   ---------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              04/05/2007         3264                              Originally Programmed                                            devlIDS30            Brent Leland                 08-30-2007
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
from pyspark.sql.functions import col, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


ids_secret_name = get_widget_value('ids_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
ToleranceCd = get_widget_value('ToleranceCd','')
RunID = get_widget_value('RunID','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = f"""
SELECT
    SUBGRP_RATE.SRC_SYS_CD_SK AS SRC_SRC_SYS_CD_SK,
    SUBGRP_RATE.GRP_ID AS SRC_GRP_ID,
    SUBGRP_RATE.SUBGRP_ID AS SRC_SUBGRP_ID,
    SUBGRP_RATE.EFF_DT_SK AS SRC_EFF_DT_SK,
    B_SUBGRP_RATE.SRC_SYS_CD_SK AS TRGT_SRC_SYS_CD_SK,
    B_SUBGRP_RATE.GRP_ID AS TRGT_GRP_ID,
    B_SUBGRP_RATE.SUBGRP_ID AS TRGT_SUBGRP_ID,
    B_SUBGRP_RATE.EFF_DT_SK AS TRGT_EFF_DT_SK
FROM {IDSOwner}.SUBGRP_RATE SUBGRP_RATE
FULL OUTER JOIN {IDSOwner}.B_SUBGRP_RATE B_SUBGRP_RATE
  ON SUBGRP_RATE.SRC_SYS_CD_SK = B_SUBGRP_RATE.SRC_SYS_CD_SK
  AND SUBGRP_RATE.GRP_ID = B_SUBGRP_RATE.GRP_ID
  AND SUBGRP_RATE.SUBGRP_ID = B_SUBGRP_RATE.SUBGRP_ID
  AND SUBGRP_RATE.EFF_DT_SK = B_SUBGRP_RATE.EFF_DT_SK
WHERE SUBGRP_RATE.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
"""

df_missing = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_Research = df_missing.filter(
    col("SRC_EFF_DT_SK").isNull()
    | col("SRC_GRP_ID").isNull()
    | col("SRC_SRC_SYS_CD_SK").isNull()
    | col("SRC_SUBGRP_ID").isNull()
    | col("TRGT_EFF_DT_SK").isNull()
    | col("TRGT_GRP_ID").isNull()
    | col("TRGT_SRC_SYS_CD_SK").isNull()
    | col("TRGT_SUBGRP_ID").isNull()
).select(
    "TRGT_SRC_SYS_CD_SK",
    "TRGT_GRP_ID",
    "TRGT_SUBGRP_ID",
    "TRGT_EFF_DT_SK",
    "SRC_SRC_SYS_CD_SK",
    "SRC_GRP_ID",
    "SRC_SUBGRP_ID",
    "SRC_EFF_DT_SK"
)

df_Research = df_Research.withColumn("TRGT_EFF_DT_SK", rpad(col("TRGT_EFF_DT_SK"), 10, " "))
df_Research = df_Research.withColumn("SRC_EFF_DT_SK", rpad(col("SRC_EFF_DT_SK"), 10, " "))

write_files(
    df_Research,
    f"{adls_path}/balancing/research/FacetsIdsSubGrpRateResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

if ToleranceCd == 'OUT':
    df_NotifyCandidate = df_missing.limit(1)
else:
    df_NotifyCandidate = spark.createDataFrame([], df_missing.schema)

df_NotifyCandidate = df_NotifyCandidate.withColumn(
    "NOTIFICATION",
    lit("ROW COUNT BALANCING FACETS - IDS SUBGRP RATE OUT OF TOLERANCE")
)

df_Notify = df_NotifyCandidate.select("NOTIFICATION")
df_Notify = df_Notify.withColumn("NOTIFICATION", rpad(col("NOTIFICATION"), 70, " "))

write_files(
    df_Notify,
    f"{adls_path}/balancing/notify/MbrshpBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)