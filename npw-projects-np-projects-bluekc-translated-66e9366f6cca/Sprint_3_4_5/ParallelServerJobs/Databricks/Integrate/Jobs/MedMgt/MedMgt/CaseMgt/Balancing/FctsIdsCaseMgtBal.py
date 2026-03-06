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
# MAGIC CALLED BY:    FctsIdsCaseMgtBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS: 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                       Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                   ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              2007-04-09        3264                              Originally Programmed                                      devlIDS30      
# MAGIC 
# MAGIC Parikshith Chada               8/23/2007         3264                              Modified the balancing process,                      devlIDS30                     Steph Goddard             9/14/07
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
from pyspark.sql.types import StructType, StructField, StringType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


RunID = get_widget_value('RunID','')
ToleranceCd = get_widget_value('ToleranceCd','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = f"""SELECT
CASE_MGT.SRC_SYS_CD_SK AS SRC_SRC_SYS_CD_SK,
CASE_MGT.CASE_MGT_ID AS SRC_CASE_MGT_ID,
B_CASE_MGT.SRC_SYS_CD_SK AS TRGT_SRC_SYS_CD_SK,
B_CASE_MGT.CASE_MGT_ID AS TRGT_CASE_MGT_ID
FROM {IDSOwner}.CASE_MGT CASE_MGT
FULL OUTER JOIN {IDSOwner}.B_CASE_MGT B_CASE_MGT
ON CASE_MGT.SRC_SYS_CD_SK = B_CASE_MGT.SRC_SYS_CD_SK
AND CASE_MGT.CASE_MGT_ID = B_CASE_MGT.CASE_MGT_ID
WHERE CASE_MGT.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}"""

df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_Research = df_SrcTrgtComp.filter(
    F.col("SRC_CASE_MGT_ID").isNull()
    | F.col("SRC_SRC_SYS_CD_SK").isNull()
    | F.col("TRGT_CASE_MGT_ID").isNull()
    | F.col("TRGT_SRC_SYS_CD_SK").isNull()
)

df_ResearchSelected = df_Research.select(
    "TRGT_SRC_SYS_CD_SK",
    "TRGT_CASE_MGT_ID",
    "SRC_SRC_SYS_CD_SK",
    "SRC_CASE_MGT_ID"
)

df_Notify = (
    df_SrcTrgtComp.limit(1)
    .filter(F.lit(ToleranceCd) == F.lit("OUT"))
    .selectExpr("'ROW COUNT BALANCING FACETS - IDS CASE MGT OUT OF TOLERANCE' as NOTIFICATION")
)

df_Notify_Final = df_Notify.withColumn(
    "NOTIFICATION",
    F.rpad(F.col("NOTIFICATION"), 70, " ")
)

write_files(
    df_ResearchSelected,
    f"{adls_path}/balancing/research/FacetsIdsCaseMgtResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

write_files(
    df_Notify_Final.select("NOTIFICATION"),
    f"{adls_path}/balancing/notify/MedMgtBalancingNotification.txt",
    delimiter=",",
    mode="append",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)