# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     EdwMedMgtNoteTxDExtr
# MAGIC 
# MAGIC DESCRIPTION:  Extracts from IDS MED_MGT_NOTE_TX to flatfile MED_MGT_NOTE_TX_D.dat
# MAGIC       
# MAGIC 
# MAGIC INPUTS:
# MAGIC The following IDS table is used:
# MAGIC MED_MGT_NOTE_TX
# MAGIC 
# MAGIC 
# MAGIC   
# MAGIC HASH FILES:
# MAGIC                 hf_cdma
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                   TRIM all the fields that are extracted or used in lookups
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                     Flatfile: MED_MGT_NOTE_TX_D.dat
# MAGIC 
# MAGIC               
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                          Date               Project/Altiris #               Change Description                                                                            Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------                                                      ---------------------------------   ---------------------------------    -------------------------   
# MAGIC Suzanne Saylor              2/28/2006          MedMgmt/                  Original Programming.                                                                               devlEDW10
# MAGIC Bhoomi Dasari                2/08/2007          MedMgmt/                  Added LAST_UPDT_RUN_CYC_EXCTN_SK  >= ExtractRunCycle        devlEDW                   Steph Goddard             01/23/2008        
# MAGIC                                                                                                      rule based upon which we extract records now.  
# MAGIC                                                                                                      Checked null's for lkups
# MAGIC Ralph Tucker                  09/15/2009     TTR-583                    Added EdwRunCycle to last updt run cyc                                                   devlEDW                    Steph Goddard             09/16/2009
# MAGIC Sravya Gorla                   10/02/2019     US# 140167                 Updated the datatype of MED_MGT_NOTE_DTM                                EnterpriseDev2
# MAGIC                                                                                                       from Timestamp(26.3) to timestamp(26.6)

# MAGIC Extract IDS  Data
# MAGIC Apply business logic.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.functions import rpad, length, lit, col
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrentDate = get_widget_value('CurrentDate','')
EdwRunCycle = get_widget_value('EdwRunCycle','')
ExtractRunCycle = get_widget_value('ExtractRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"SELECT tx.MED_MGT_NOTE_TX_SK as MED_MGT_NOTE_TX_SK,tx.SRC_SYS_CD_SK as SRC_SYS_CD_SK,tx.MED_MGT_NOTE_DTM as MED_MGT_NOTE_DTM,tx.MED_MGT_NOTE_INPT_DTM as MED_MGT_NOTE_INPT_DTM,tx.NOTE_TX_SEQ_NO as NOTE_TX_SEQ_NO,tx.CRT_RUN_CYC_EXCTN_SK as CRT_RUN_CYC_EXCTN_SK,tx.LAST_UPDT_RUN_CYC_EXCTN_SK as LAST_UPDT_RUN_CYC_EXCTN_SK,tx.MED_MGT_NOTE_SK as MED_MGT_NOTE_SK,tx.NOTE_TX as NOTE_TX FROM {IDSOwner}.MED_MGT_NOTE_TX tx WHERE tx.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtractRunCycle}"
df_IDS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_hf_cdma_codes = spark.read.parquet(f"{adls_path}/hf_cdma_codes.parquet")

df_joined = df_IDS.alias("Extract").join(
    df_hf_cdma_codes.alias("refSrcSysCd"),
    col("Extract.SRC_SYS_CD_SK") == col("refSrcSysCd.CD_MPPNG_SK"),
    "left"
)

df_enriched = df_joined.select(
    col("Extract.MED_MGT_NOTE_TX_SK").alias("MED_MGT_NOTE_TX_SK"),
    F.when(
        (col("refSrcSysCd.TRGT_CD").isNull()) | (length(trim(col("refSrcSysCd.TRGT_CD"))) == 0),
        lit("NA")
    ).otherwise(col("refSrcSysCd.TRGT_CD")).alias("SRC_SYS_CD"),
    col("Extract.MED_MGT_NOTE_DTM").alias("MED_MGT_NOTE_DTM"),
    col("Extract.MED_MGT_NOTE_INPT_DTM").alias("MED_MGT_NOTE_INPT_DTM"),
    col("Extract.NOTE_TX_SEQ_NO").alias("MED_MGT_NOTE_TX_SEQ_NO"),
    rpad(current_date(), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad(current_date(), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("Extract.NOTE_TX").alias("MED_MGT_NOTE_TX"),
    col("Extract.MED_MGT_NOTE_SK").alias("MED_MGT_NOTE_SK"),
    col("Extract.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(EdwRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

write_files(
    df_enriched,
    f"{adls_path}/load/MED_MGT_NOTE_TX_D.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote='"',
    nullValue=None
)