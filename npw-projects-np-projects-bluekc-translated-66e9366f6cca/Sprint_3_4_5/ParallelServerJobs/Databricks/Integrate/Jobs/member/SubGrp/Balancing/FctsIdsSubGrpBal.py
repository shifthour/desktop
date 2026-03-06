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
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:       FctsIdsSubGrpBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                      Development Project     Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                 ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              04/04/2007          3264                              Originally Programmed                                    devlIDS30                  Brent Leland                 08-30-2007
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
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
RunID = get_widget_value('RunID','')
ToleranceCd = get_widget_value('ToleranceCd','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"""
SELECT 
   SUBGRP.SRC_SYS_CD_SK as SRC_SRC_SYS_CD_SK,
   SUBGRP.GRP_ID as SRC_GRP_ID,
   SUBGRP.SUBGRP_ID as SRC_SUBGRP_ID,
   B_SUBGRP.SRC_SYS_CD_SK as TRGT_SRC_SYS_CD_SK,
   B_SUBGRP.GRP_ID as TRGT_GRP_ID,
   B_SUBGRP.SUBGRP_ID as TRGT_SUBGRP_ID
FROM {IDSOwner}.SUBGRP SUBGRP
FULL OUTER JOIN {IDSOwner}.B_SUBGRP B_SUBGRP
  ON SUBGRP.SRC_SYS_CD_SK = B_SUBGRP.SRC_SYS_CD_SK
  AND SUBGRP.GRP_ID = B_SUBGRP.GRP_ID
  AND SUBGRP.SUBGRP_ID = B_SUBGRP.SUBGRP_ID
WHERE SUBGRP.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
"""

df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_Research = df_SrcTrgtComp.filter(
    F.col("SRC_SRC_SYS_CD_SK").isNull()
    | F.col("SRC_GRP_ID").isNull()
    | F.col("SRC_SUBGRP_ID").isNull()
    | F.col("TRGT_SRC_SYS_CD_SK").isNull()
    | F.col("TRGT_GRP_ID").isNull()
    | F.col("TRGT_SUBGRP_ID").isNull()
)
df_ResearchSelected = df_Research.select(
    "TRGT_SRC_SYS_CD_SK",
    "TRGT_GRP_ID",
    "TRGT_SUBGRP_ID",
    "SRC_SRC_SYS_CD_SK",
    "SRC_GRP_ID",
    "SRC_SUBGRP_ID"
)

df_Notify_temp = df_SrcTrgtComp.withColumn("rn", F.row_number().over(Window.orderBy(F.lit(1))))
df_Notify = df_Notify_temp.filter(
    (F.col("rn") == 1) & (F.lit(ToleranceCd) == F.lit("OUT"))
).drop("rn")
df_Notify = df_Notify.select(
    F.lit("ROW COUNT BALANCING FACETS - IDS SUBGRP OUT OF TOLERANCE").alias("NOTIFICATION")
)
df_Notify = df_Notify.withColumn("NOTIFICATION", F.rpad(F.col("NOTIFICATION"), 70, " "))

write_files(
    df_ResearchSelected,
    f"{adls_path}/balancing/research/FacetsIdsSubGrpResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

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