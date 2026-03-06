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
# MAGIC CALLED BY:    FctsIdsMedMgtNoteTxBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                        Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                   ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada             2007-04-10          3264                              Originally Programmed                                      devlIDS30     
# MAGIC 
# MAGIC Parikshith Chada               8/23/2007         3264                              Modified the balancing process,                       devlIDS30                    Steph Goddard             9/14/07
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
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, lit, rpad, row_number
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


# Parameter retrieval
RunID = get_widget_value('RunID', '')
ToleranceCd = get_widget_value('ToleranceCd', '')
IDSOwner = get_widget_value('IDSOwner', '')
ids_secret_name = get_widget_value('ids_secret_name', '')
ExtrRunCycle = get_widget_value('ExtrRunCycle', '')

# Database read (SrcTrgtComp - DB2Connector)
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"""
SELECT 
  MED_MGT_NOTE_TX.SRC_SYS_CD_SK AS SRC_SRC_SYS_CD_SK,
  MED_MGT_NOTE_TX.MED_MGT_NOTE_DTM AS SRC_MED_MGT_NOTE_DTM,
  MED_MGT_NOTE_TX.MED_MGT_NOTE_INPT_DTM AS SRC_MED_MGT_NOTE_INPT_DTM,
  MED_MGT_NOTE_TX.NOTE_TX_SEQ_NO AS SRC_NOTE_TX_SEQ_NO,
  B_MED_MGT_NOTE_TX.SRC_SYS_CD_SK AS TRGT_SRC_SYS_CD_SK,
  B_MED_MGT_NOTE_TX.MED_MGT_NOTE_DTM AS TRGT_MED_MGT_NOTE_DTM,
  B_MED_MGT_NOTE_TX.MED_MGT_NOTE_INPT_DTM AS TRGT_MED_MGT_NOTE_INPT_DTM,
  B_MED_MGT_NOTE_TX.NOTE_TX_SEQ_NO AS TRGT_NOTE_TX_SEQ_NO
FROM {IDSOwner}.MED_MGT_NOTE_TX MED_MGT_NOTE_TX
FULL OUTER JOIN {IDSOwner}.B_MED_MGT_NOTE_TX B_MED_MGT_NOTE_TX
  ON MED_MGT_NOTE_TX.SRC_SYS_CD_SK = B_MED_MGT_NOTE_TX.SRC_SYS_CD_SK
  AND MED_MGT_NOTE_TX.MED_MGT_NOTE_DTM = B_MED_MGT_NOTE_TX.MED_MGT_NOTE_DTM
  AND MED_MGT_NOTE_TX.MED_MGT_NOTE_INPT_DTM = B_MED_MGT_NOTE_TX.MED_MGT_NOTE_INPT_DTM
  AND MED_MGT_NOTE_TX.NOTE_TX_SEQ_NO = B_MED_MGT_NOTE_TX.NOTE_TX_SEQ_NO
WHERE MED_MGT_NOTE_TX.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
"""
df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# TransformLogic - Output "Research"
df_research = df_SrcTrgtComp.filter(
    col("SRC_NOTE_TX_SEQ_NO").isNull()
    | col("SRC_MED_MGT_NOTE_DTM").isNull()
    | col("SRC_MED_MGT_NOTE_INPT_DTM").isNull()
    | col("SRC_SRC_SYS_CD_SK").isNull()
    | col("TRGT_NOTE_TX_SEQ_NO").isNull()
    | col("TRGT_MED_MGT_NOTE_DTM").isNull()
    | col("TRGT_MED_MGT_NOTE_INPT_DTM").isNull()
    | col("TRGT_SRC_SYS_CD_SK").isNull()
)
df_research = df_research.select(
    "TRGT_SRC_SYS_CD_SK",
    "TRGT_MED_MGT_NOTE_DTM",
    "TRGT_MED_MGT_NOTE_INPT_DTM",
    "TRGT_NOTE_TX_SEQ_NO",
    "SRC_SRC_SYS_CD_SK",
    "SRC_MED_MGT_NOTE_DTM",
    "SRC_MED_MGT_NOTE_INPT_DTM",
    "SRC_NOTE_TX_SEQ_NO"
)

# TransformLogic - Output "Notify" (@INROWNUM=1 AND ToleranceCd='OUT')
df_SrcTrgtComp_rn = df_SrcTrgtComp.withColumn("row_num", row_number().over(Window.orderBy(lit(1))))
if ToleranceCd == 'OUT':
    df_notify_temp = df_SrcTrgtComp_rn.filter(col("row_num") == 1)
    df_notify_temp = df_notify_temp.withColumn(
        "NOTIFICATION", 
        lit("ROW COUNT BALANCING FACETS - IDS MED MGT NOTE TX OUT OF TOLERANCE")
    )
    df_notify = df_notify_temp.select(
        rpad(col("NOTIFICATION"), 70, " ").alias("NOTIFICATION")
    )
else:
    empty_schema = StructType([StructField("NOTIFICATION", StringType(), True)])
    df_notify = spark.createDataFrame([], empty_schema)

# ResearchFile - CSeqFileStage
write_files(
    df_research,
    f"{adls_path}/balancing/research/FacetsIdsMedMgtNoteTxResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

# ErrorNotificationFile - CSeqFileStage
write_files(
    df_notify,
    f"{adls_path}/balancing/notify/MedMgtBalancingNotification.txt",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)