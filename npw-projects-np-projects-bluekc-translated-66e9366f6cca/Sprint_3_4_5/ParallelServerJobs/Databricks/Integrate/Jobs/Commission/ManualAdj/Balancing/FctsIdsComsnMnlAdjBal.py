# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
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
# MAGIC CALLED BY:     FctsIdsComsnMnlAdjBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                        Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                   ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada             05/31/2007          3264                              Originally Programmed                                    devlIDS30      
# MAGIC 
# MAGIC 7Parikshith Chada               8/22/2007         3264                              Modified the balancing process,                     devlIDS30                      Steph Goddard            09/18/2007
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
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
RunID = get_widget_value('RunID','')
ToleranceCd = get_widget_value('ToleranceCd','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"""
SELECT
COMSN_MNL_ADJ.SRC_SYS_CD_SK AS SRC_SRC_SYS_CD_SK,
COMSN_MNL_ADJ.AGNT_ID AS SRC_AGNT_ID,
COMSN_MNL_ADJ.SEQ_NO AS SRC_SEQ_NO,
B_COMSN_MNL_ADJ.SRC_SYS_CD_SK AS TRGT_SRC_SYS_CD_SK,
B_COMSN_MNL_ADJ.AGNT_ID AS TRGT_AGNT_ID,
B_COMSN_MNL_ADJ.SEQ_NO AS TRGT_SEQ_NO
FROM {IDSOwner}.COMSN_MNL_ADJ COMSN_MNL_ADJ
FULL OUTER JOIN {IDSOwner}.B_COMSN_MNL_ADJ B_COMSN_MNL_ADJ
ON COMSN_MNL_ADJ.SRC_SYS_CD_SK = B_COMSN_MNL_ADJ.SRC_SYS_CD_SK
AND COMSN_MNL_ADJ.AGNT_ID = B_COMSN_MNL_ADJ.AGNT_ID
AND COMSN_MNL_ADJ.SEQ_NO = B_COMSN_MNL_ADJ.SEQ_NO
WHERE COMSN_MNL_ADJ.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
"""
df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_Research = df_SrcTrgtComp.filter(
    F.isnull("SRC_SRC_SYS_CD_SK")
    | F.isnull("SRC_AGNT_ID")
    | F.isnull("SRC_SEQ_NO")
    | F.isnull("TRGT_SRC_SYS_CD_SK")
    | F.isnull("TRGT_AGNT_ID")
    | F.isnull("TRGT_SEQ_NO")
).select(
    F.col("TRGT_SRC_SYS_CD_SK"),
    F.col("TRGT_AGNT_ID"),
    F.col("TRGT_SEQ_NO"),
    F.col("SRC_SRC_SYS_CD_SK"),
    F.col("SRC_AGNT_ID"),
    F.col("SRC_SEQ_NO")
)

if ToleranceCd == 'OUT':
    w = Window.orderBy(F.lit(1))
    df_temp = df_SrcTrgtComp.withColumn("rnum", F.row_number().over(w))
    df_Notify_unpadded = df_temp.filter(F.col("rnum") == 1).select(
        F.lit("ROW COUNT BALANCING FACETS - IDS COMSN_MNL_ADJ OUT OF TOLERANCE").alias("NOTIFICATION")
    )
else:
    schema_notify = StructType([StructField("NOTIFICATION", StringType(), True)])
    df_Notify_unpadded = spark.createDataFrame([], schema_notify)

df_Notify = df_Notify_unpadded.select(
    F.rpad(F.col("NOTIFICATION"), 70, " ").alias("NOTIFICATION")
)

write_files(
    df_Research,
    f"{adls_path}/balancing/research/FctsIdsComsnMnlAdjResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

write_files(
    df_Notify,
    f"{adls_path}/balancing/notify/CommissionsBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)