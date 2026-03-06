# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 01/30/08 05:00:10 Batch  14640_18019 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 10/31/07 14:11:20 Batch  14549_51083 PROMOTE bckcetl ids20 dsadm bls for on
# MAGIC ^1_1 10/31/07 12:55:15 Batch  14549_46521 PROMOTE bckcetl ids20 dsadm bls for on
# MAGIC ^1_1 10/31/07 12:32:28 Batch  14549_45152 INIT bckcett testIDS30 dsadm bls for on
# MAGIC ^1_1 10/26/07 08:07:18 Batch  14544_29241 PROMOTE bckcett testIDS30 u03651 steffy
# MAGIC ^1_1 10/26/07 07:46:17 Batch  14544_27982 INIT bckcett devlIDS30 u03651 steffy
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:     FctsIdsAgntAddrBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              6/7/2007          3264                              Originally Programmed                                        devlIDS30      
# MAGIC 
# MAGIC Parikshith Chada               8/22/2007         3264                              Modified the balancing process,                      devlIDS30                      Steph Goddard            09/17/2007
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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


# Parameter retrieval
RunID = get_widget_value('RunID', '')
ToleranceCd = get_widget_value('ToleranceCd', '')
IDSOwner = get_widget_value('IDSOwner', '')
ExtrRunCycle = get_widget_value('ExtrRunCycle', '')
ids_secret_name = get_widget_value('ids_secret_name', '')

# DB2 Connector - Stage: SrcTrgtComp
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"""
SELECT
  AGNT_ADDR.SRC_SYS_CD_SK AS SRC_SRC_SYS_CD_SK,
  AGNT_ADDR.AGNT_ID AS SRC_AGNT_ID,
  AGNT_ADDR.ADDR_TYP_CD_SK AS SRC_ADDR_TYP_CD_SK,
  B_AGNT_ADDR.SRC_SYS_CD_SK AS TRGT_SRC_SYS_CD_SK,
  B_AGNT_ADDR.AGNT_ID AS TRGT_AGNT_ID,
  B_AGNT_ADDR.ADDR_TYP_CD_SK AS TRGT_ADDR_TYP_CD_SK
FROM {IDSOwner}.AGNT_ADDR AGNT_ADDR
FULL OUTER JOIN {IDSOwner}.B_AGNT_ADDR B_AGNT_ADDR
  ON AGNT_ADDR.SRC_SYS_CD_SK = B_AGNT_ADDR.SRC_SYS_CD_SK
  AND AGNT_ADDR.AGNT_ID = B_AGNT_ADDR.AGNT_ID
  AND AGNT_ADDR.ADDR_TYP_CD_SK = B_AGNT_ADDR.ADDR_TYP_CD_SK
WHERE AGNT_ADDR.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
"""
df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# Transformer - Stage: TransformLogic
df_research = df_SrcTrgtComp.filter(
    (F.col("SRC_ADDR_TYP_CD_SK").isNull())
    | (F.col("SRC_AGNT_ID").isNull())
    | (F.col("SRC_SRC_SYS_CD_SK").isNull())
    | (F.col("TRGT_ADDR_TYP_CD_SK").isNull())
    | (F.col("TRGT_AGNT_ID").isNull())
    | (F.col("TRGT_SRC_SYS_CD_SK").isNull())
).select(
    "TRGT_SRC_SYS_CD_SK",
    "TRGT_AGNT_ID",
    "TRGT_ADDR_TYP_CD_SK",
    "SRC_SRC_SYS_CD_SK",
    "SRC_AGNT_ID",
    "SRC_ADDR_TYP_CD_SK"
)

if ToleranceCd == "OUT":
    df_notify_temp = df_SrcTrgtComp.limit(1).select(
        F.lit("ROW COUNT BALANCING FACETS - IDS AGNT ADDR OUT OF TOLERANCE").alias("NOTIFICATION")
    )
else:
    df_notify_temp = df_SrcTrgtComp.limit(0).select(
        F.lit("").alias("NOTIFICATION")
    )

df_notify = df_notify_temp.withColumn(
    "NOTIFICATION",
    rpad("NOTIFICATION", 70, " ")
).select("NOTIFICATION")

# ResearchFile - CSeqFileStage
write_files(
    df_research.select(
        "TRGT_SRC_SYS_CD_SK",
        "TRGT_AGNT_ID",
        "TRGT_ADDR_TYP_CD_SK",
        "SRC_SRC_SYS_CD_SK",
        "SRC_AGNT_ID",
        "SRC_ADDR_TYP_CD_SK"
    ),
    f"{adls_path}/balancing/research/FctsIdsAgntAddrResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# ErrorNotificationFile - CSeqFileStage
write_files(
    df_notify.select("NOTIFICATION"),
    f"{adls_path}/balancing/notify/CommissionsBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)