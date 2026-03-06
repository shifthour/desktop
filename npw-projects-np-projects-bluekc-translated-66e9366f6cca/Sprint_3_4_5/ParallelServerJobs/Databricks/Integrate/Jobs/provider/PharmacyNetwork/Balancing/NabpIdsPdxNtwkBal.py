# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 04/01/09 11:15:41 Batch  15067_40549 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 01/28/08 13:39:53 Batch  14638_49197 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 12/26/07 13:00:58 Batch  14605_46863 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/23/07 09:25:41 Batch  14572_33950 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_3 10/09/07 10:10:10 Batch  14527_36614 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 10/01/07 10:17:35 Batch  14519_37064 PROMOTE bckcetl ids20 dsadm bls for bl
# MAGIC ^1_1 10/01/07 09:47:40 Batch  14519_35267 INIT bckcett testIDS30 dsadm bls for bl
# MAGIC ^1_2 09/26/07 17:31:58 Batch  14514_63125 PROMOTE bckcett testIDS30 u08717 Brent
# MAGIC ^1_2 09/26/07 17:07:37 Batch  14514_61663 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_1 09/25/07 15:06:34 Batch  14513_54400 INIT bckcett devlIDS30 u08717 Brent
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:      NabpIdsPdxNtwkBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                   ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              05/18/2007          3264                              Originally Programmed                                   devlIDS30        
# MAGIC 
# MAGIC Parikshith Chada               8/17/2007         3264                              Modified the balancing process,                      devlIDS30                     Steph Goddard             9/6/07
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


RunID = get_widget_value('RunID','')
ToleranceCd = get_widget_value('ToleranceCd','')
IDSOwner = get_widget_value('IDSOwner','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = f"""SELECT 
PDX_NTWK.SRC_SYS_CD_SK AS SRC_SRC_SYS_CD_SK,
PDX_NTWK.PROV_ID AS SRC_PROV_ID,
PDX_NTWK.PDX_NTWK_CD_SK AS SRC_PDX_NTWK_CD_SK,
B_PDX_NTWK.SRC_SYS_CD_SK AS TRGT_SRC_SYS_CD_SK,
B_PDX_NTWK.PROV_ID AS TRGT_PROV_ID,
B_PDX_NTWK.PDX_NTWK_CD_SK AS TRGT_PDX_NTWK_CD_SK
FROM {IDSOwner}.PDX_NTWK PDX_NTWK
FULL OUTER JOIN {IDSOwner}.B_PDX_NTWK B_PDX_NTWK
ON PDX_NTWK.SRC_SYS_CD_SK = B_PDX_NTWK.SRC_SYS_CD_SK
AND PDX_NTWK.PROV_ID = B_PDX_NTWK.PROV_ID
AND PDX_NTWK.PDX_NTWK_CD_SK = B_PDX_NTWK.PDX_NTWK_CD_SK
WHERE PDX_NTWK.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}"""

df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_Research = df_SrcTrgtComp.filter(
    F.col("SRC_PDX_NTWK_CD_SK").isNull()
    | F.col("SRC_PROV_ID").isNull()
    | F.col("SRC_SRC_SYS_CD_SK").isNull()
    | F.col("TRGT_PDX_NTWK_CD_SK").isNull()
    | F.col("TRGT_PROV_ID").isNull()
    | F.col("TRGT_SRC_SYS_CD_SK").isNull()
).select(
    "TRGT_SRC_SYS_CD_SK",
    "TRGT_PROV_ID",
    "TRGT_PDX_NTWK_CD_SK",
    "SRC_SRC_SYS_CD_SK",
    "SRC_PROV_ID",
    "SRC_PDX_NTWK_CD_SK"
)

write_files(
    df_Research,
    f"{adls_path}/balancing/research/NabpIdsPdxNtwkResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

if ToleranceCd == 'OUT':
    w = Window.orderBy(F.lit(1))
    df_notify_temp = df_SrcTrgtComp.withColumn("inrownum", F.row_number().over(w))
    df_notify_temp = df_notify_temp.filter(F.col("inrownum") == 1).withColumn(
        "NOTIFICATION", F.lit("ROW COUNT BALANCING NABP - IDS PDX NTWK OUT OF TOLERANCE")
    )
    df_notify_temp = df_notify_temp.drop("inrownum")
    schema_notify = df_notify_temp.select("NOTIFICATION").schema
    df_Notify = df_notify_temp.select(
        F.rpad(F.col("NOTIFICATION"), 70, " ").alias("NOTIFICATION")
    )
else:
    schema_notify = StructType([StructField("NOTIFICATION", StringType(), True)])
    df_empty = spark.createDataFrame([], schema_notify)
    df_Notify = df_empty.select(
        F.rpad(F.col("NOTIFICATION"), 70, " ").alias("NOTIFICATION")
    )

write_files(
    df_Notify,
    f"{adls_path}/balancing/notify/ProvidersBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)