# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 01/28/08 08:28:47 Batch  14638_30532 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 12/26/07 11:16:35 Batch  14605_40601 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/21/07 14:41:36 Batch  14570_52900 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 10/10/07 13:50:33 Batch  14528_49851 PROMOTE bckcetl ids20 dsadm bls for bl
# MAGIC ^1_1 10/10/07 13:38:24 Batch  14528_49120 INIT bckcett testIDS30 dsadm bls for bl
# MAGIC ^1_1 10/08/07 17:37:28 Batch  14526_63461 PROMOTE bckcett testIDS30 u08717 Brent
# MAGIC ^1_1 10/08/07 17:30:51 Batch  14526_63055 INIT bckcett devlIDS30 u08717 Brent
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:     FdbIdsAhfsTccBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                     ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              04/16/2007          3264                              Originally Programmed                                      devlIDS30           
# MAGIC 
# MAGIC Parikshith Chada               8/28/2007         3264                              Modified the balancing process,                        devlIDS30                    Steph Goddard            09/27/2007
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
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------

# MAGIC %run ../../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

# Parameters
RunID = get_widget_value('RunID','')
ToleranceCd = get_widget_value('ToleranceCd','')
IDSOwner = get_widget_value('IDSOwner','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
ids_secret_name = get_widget_value('ids_secret_name','')

# DB Configuration
jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# Stage: SrcTrgtComp (DB2Connector)
extract_query = f"""
SELECT
  AHFS_TCC.AHFS_TCC,
  B_AHFS_TCC.AHFS_TCC
FROM {IDSOwner}.AHFS_TCC AHFS_TCC
FULL OUTER JOIN {IDSOwner}.B_AHFS_TCC B_AHFS_TCC
  ON AHFS_TCC.AHFS_TCC = B_AHFS_TCC.AHFS_TCC
WHERE
  AHFS_TCC.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
"""
df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# Rename the duplicate columns to match DataStage naming
# First column => SRC_AHFS_TCC, second => TRGT_AHFS_TCC
cols_SrcTrgtComp = df_SrcTrgtComp.columns
df_SrcTrgtComp = (
    df_SrcTrgtComp
    .withColumnRenamed(cols_SrcTrgtComp[0], "SRC_AHFS_TCC")
    .withColumnRenamed(cols_SrcTrgtComp[1], "TRGT_AHFS_TCC")
)

# Stage: TransformLogic (CTransformerStage)
df_TransformLogic_in = df_SrcTrgtComp

# Output pin "Research": ISNULL(SRC_AHFS_TCC) OR ISNULL(TRGT_AHFS_TCC)
df_Research = df_TransformLogic_in.filter(
    (F.col("SRC_AHFS_TCC").isNull()) | (F.col("TRGT_AHFS_TCC").isNull())
).select("TRGT_AHFS_TCC", "SRC_AHFS_TCC")

# Output pin "Notify": @INROWNUM = 1 And ToleranceCd = 'OUT'
window_spec = Window.orderBy(F.monotonically_increasing_id())
df_temp = df_TransformLogic_in.withColumn("row_num", F.row_number().over(window_spec))
df_Notify_temp = df_temp.filter((F.col("row_num") == 1) & (F.lit(ToleranceCd) == F.lit("OUT")))
df_Notify = df_Notify_temp.select(
    F.lit("ROW COUNT BALANCING FIRST DATA BANK - IDS AHFS TCC OUT OF TOLERANCE").alias("NOTIFICATION")
)

# "NOTIFICATION" is char(70), apply rpad
df_Notify = df_Notify.withColumn("NOTIFICATION", F.rpad(F.col("NOTIFICATION"), 70, " "))

# Stage: ResearchFile (CSeqFileStage)
# Write df_Research to FdbIdsAhfsTccResearch.dat.#RunID#
write_files(
    df_Research.select("TRGT_AHFS_TCC", "SRC_AHFS_TCC"),
    f"{adls_path}/balancing/research/FdbIdsAhfsTccResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Stage: ErrorNotificationFile (CSeqFileStage)
write_files(
    df_Notify.select("NOTIFICATION"),
    f"{adls_path}/balancing/notify/NdcBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)