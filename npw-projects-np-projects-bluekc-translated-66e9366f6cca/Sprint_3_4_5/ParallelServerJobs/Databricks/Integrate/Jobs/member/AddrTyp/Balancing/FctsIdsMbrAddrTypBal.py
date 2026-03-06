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
# MAGIC CALLED BY:      FctsIdsMbrAddrTypBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                      Development Project          Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                 ----------------------------------      ---------------------------------      -------------------------   
# MAGIC Parikshith Chada              3/19/2007          3264                              Originally Programmed                                       devlIDS30                    Brent Leland                   08-29-2007        
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
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

# Parameters
RunID = get_widget_value('RunID','')
ToleranceCd = get_widget_value('ToleranceCd','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

# DB2Connector Stage: SrcTrgtComp
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"""
SELECT
  b.SRC_SYS_CD_SK AS SRC_SRC_SYS_CD_SK,
  b.MBR_UNIQ_KEY AS SRC_MBR_UNIQ_KEY,
  b.MBR_ADDR_ROLE_TYP_CD_SK AS SRC_MBR_ADDR_ROLE_TYP_CD_SK,
  m.SRC_SYS_CD_SK AS TRGT_SRC_SYS_CD_SK,
  m.MBR_UNIQ_KEY AS TRGT_MBR_UNIQ_KEY,
  m.MBR_ADDR_ROLE_TYP_CD_SK AS TRGT_MBR_ADDR_ROLE_TYP_CD_SK
FROM {IDSOwner}.CD_MPPNG c,
     {IDSOwner}.MBR_ADDR_TYP m
     FULL OUTER JOIN {IDSOwner}.B_MBR_ADDR_TYP b
       ON m.SRC_SYS_CD_SK = b.SRC_SYS_CD_SK
       AND m.MBR_UNIQ_KEY = b.MBR_UNIQ_KEY
       AND m.MBR_ADDR_ROLE_TYP_CD_SK = b.MBR_ADDR_ROLE_TYP_CD_SK
WHERE m.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
  AND m.MBR_ADDR_ROLE_TYP_CD_SK = c.CD_MPPNG_SK
  AND c.TRGT_CD = 'HOME'
"""
df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_SrcTrgtComp = df_SrcTrgtComp.select(
    "SRC_SRC_SYS_CD_SK",
    "SRC_MBR_UNIQ_KEY",
    "SRC_MBR_ADDR_ROLE_TYP_CD_SK",
    "TRGT_SRC_SYS_CD_SK",
    "TRGT_MBR_UNIQ_KEY",
    "TRGT_MBR_ADDR_ROLE_TYP_CD_SK",
)

# Transformer Stage: TransformLogic
# Create row number to emulate @INROWNUM
w = Window.orderBy(F.monotonically_increasing_id())
df_with_rn = df_SrcTrgtComp.withColumn("row_num", F.row_number().over(w))

# Research link constraint
df_research = df_with_rn.filter(
    F.col("SRC_SRC_SYS_CD_SK").isNull()
    | F.col("SRC_MBR_UNIQ_KEY").isNull()
    | F.col("SRC_MBR_ADDR_ROLE_TYP_CD_SK").isNull()
    | F.col("TRGT_SRC_SYS_CD_SK").isNull()
    | F.col("TRGT_MBR_UNIQ_KEY").isNull()
    | F.col("TRGT_MBR_ADDR_ROLE_TYP_CD_SK").isNull()
).select(
    "SRC_SRC_SYS_CD_SK",
    "SRC_MBR_UNIQ_KEY",
    "SRC_MBR_ADDR_ROLE_TYP_CD_SK",
    "TRGT_SRC_SYS_CD_SK",
    "TRGT_MBR_UNIQ_KEY",
    "TRGT_MBR_ADDR_ROLE_TYP_CD_SK",
)

# Notify link constraint
if ToleranceCd == 'OUT':
    df_notify_input = df_with_rn.filter(F.col("row_num") == 1)
else:
    df_notify_input = spark.createDataFrame([], df_with_rn.schema)

df_notify = df_notify_input.select(
    F.lit("ROW COUNT BALANCING FACETS - IDS MBR ADDR TYP OUT OF TOLERANCE").alias("NOTIFICATION")
)

# Because NOTIFICATION is char(70), apply rpad
df_notify = df_notify.select(
    F.rpad(F.col("NOTIFICATION"), 70, " ").alias("NOTIFICATION")
)

# CSeqFileStage: ResearchFile
research_file_path = f"{adls_path}/balancing/research/FacetsIdsMbrAddrTypResearch.dat.{RunID}"
write_files(
    df_research,
    research_file_path,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# CSeqFileStage: ErrorNotificationFile
notify_file_path = f"{adls_path}/balancing/notify/MbrshpBalancingNotification.dat"
write_files(
    df_notify,
    notify_file_path,
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)