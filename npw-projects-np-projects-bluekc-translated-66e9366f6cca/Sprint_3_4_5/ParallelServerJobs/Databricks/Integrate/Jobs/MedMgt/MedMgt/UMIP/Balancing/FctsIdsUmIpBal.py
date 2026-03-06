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
# MAGIC CALLED BY:       FctsIdsUmIpBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                   ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              04/12/2007          3264                              Originally Programmed                                     devlIDS30    
# MAGIC 
# MAGIC Parikshith Chada               8/23/2007         3264                              Modified the balancing process, 
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
from pyspark.sql.functions import col, rpad
from pyspark.sql.types import StructType, StructField, StringType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
ToleranceCd = get_widget_value('ToleranceCd','')
RunID = get_widget_value('RunID','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"SELECT UM_IP.SRC_SYS_CD_SK AS SRC_SRC_SYS_CD_SK, UM_IP.UM_REF_ID AS SRC_UM_REF_ID, B_UM_IP.SRC_SYS_CD_SK AS TRGT_SRC_SYS_CD_SK, B_UM_IP.UM_REF_ID AS TRGT_UM_REF_ID FROM {IDSOwner}.UM_IP UM_IP FULL OUTER JOIN {IDSOwner}.B_UM_IP B_UM_IP ON UM_IP.SRC_SYS_CD_SK = B_UM_IP.SRC_SYS_CD_SK AND UM_IP.UM_REF_ID = B_UM_IP.UM_REF_ID WHERE UM_IP.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}"

df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_Research = (
    df_SrcTrgtComp.filter(
        col("SRC_SRC_SYS_CD_SK").isNull()
        | col("SRC_UM_REF_ID").isNull()
        | col("TRGT_SRC_SYS_CD_SK").isNull()
        | col("TRGT_UM_REF_ID").isNull()
    )
    .select(
        col("TRGT_SRC_SYS_CD_SK").alias("TRGT_SRC_SYS_CD_SK"),
        col("TRGT_UM_REF_ID").alias("TRGT_UM_REF_ID"),
        col("SRC_SRC_SYS_CD_SK").alias("SRC_SRC_SYS_CD_SK"),
        col("SRC_UM_REF_ID").alias("SRC_UM_REF_ID")
    )
)

if ToleranceCd == "OUT":
    df_Notify = spark.createDataFrame(
        [("ROW COUNT BALANCING FACETS - IDS UM IP OUT OF TOLERANCE",)],
        StructType([StructField("NOTIFICATION", StringType(), True)])
    )
else:
    df_Notify = spark.createDataFrame(
        [],
        StructType([StructField("NOTIFICATION", StringType(), True)])
    )

df_Notify = df_Notify.select(rpad(col("NOTIFICATION"), 70, " ").alias("NOTIFICATION"))

write_files(
    df_Research,
    f"{adls_path}/balancing/research/FacetsIdsUmIpResearch.dat.{RunID}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)

write_files(
    df_Notify,
    f"{adls_path}/balancing/notify/MedMgtBalancingNotification.txt",
    ",",
    "append",
    False,
    False,
    "\"",
    None
)