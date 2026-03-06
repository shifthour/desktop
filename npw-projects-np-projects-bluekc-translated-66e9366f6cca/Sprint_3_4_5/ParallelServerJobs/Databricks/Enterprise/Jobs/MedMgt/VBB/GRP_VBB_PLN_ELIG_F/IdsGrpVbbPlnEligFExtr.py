# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2013 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  EdwVbbExtrSeq
# MAGIC 
# MAGIC PROCESSING: Extracts data from IDS GRP_VBB_PLN_ELIG table and creates a load file for EDW GRP_VBB_PLN_ELIG_D
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date               Project/Altiris #                  Change Description                               Development Project      Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------      ---------------------------------------------------------       ----------------------------------     ---------------------------------    -------------------------   
# MAGIC Kalyan Neelam                2013-06-11    4963 VBB Phase III              Initial Programming                                EnterpriseNewDevl        Bhoomi Dasari                 7/9/2013

# MAGIC EDW GRP_VBB_PLN_ELIG extract from IDS
# MAGIC Business Rules that determine Edw Output
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


# Parameters
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrRunCycleDate = get_widget_value('CurrRunCycleDate','2007-12-12')
EDWRunCycle = get_widget_value('EDWRunCycle','0')
ExtractRunCycle = get_widget_value('ExtractRunCycle','')

# Read from IDS database (DB2Connector)
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"""
SELECT
    GRP_VBB_PLN_ELIG.VBB_PLN_ELIG_SK,
    GRP_VBB_PLN_ELIG.VBB_PLN_UNIQ_KEY,
    GRP_VBB_PLN_ELIG.GRP_ID,
    GRP_VBB_PLN_ELIG.CLS_ID,
    GRP_VBB_PLN_ELIG.CLS_PLN_ID,
    GRP_VBB_PLN_ELIG.SRC_SYS_CD_SK,
    GRP_VBB_PLN_ELIG.CRT_RUN_CYC_EXCTN_SK,
    GRP_VBB_PLN_ELIG.LAST_UPDT_RUN_CYC_EXCTN_SK,
    GRP_VBB_PLN_ELIG.CLS_SK,
    GRP_VBB_PLN_ELIG.CLS_PLN_SK,
    GRP_VBB_PLN_ELIG.GRP_SK,
    GRP_VBB_PLN_ELIG.VBB_PLN_SK,
    GRP_VBB_PLN_ELIG.VBB_PLN_ELIG_STRT_DT_SK,
    GRP_VBB_PLN_ELIG.VBB_PLN_ELIG_END_DT_SK
FROM {IDSOwner}.GRP_VBB_PLN_ELIG GRP_VBB_PLN_ELIG
WHERE GRP_VBB_PLN_ELIG.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtractRunCycle}
"""
df_GRP_VBB_PLN_ELIG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# Read from hashed file (CHashedFileStage) - Scenario C => read parquet
df_hf_cdma_codes = spark.read.parquet(f"{adls_path}/hf_cdma_codes.parquet")

# Transformer (BussinessLogic) - Left join for SrcSysCdLkup
df_businesslogic = df_GRP_VBB_PLN_ELIG.alias("Extract").join(
    df_hf_cdma_codes.alias("SrcSysCdLkup"),
    on=col("Extract.SRC_SYS_CD_SK") == col("SrcSysCdLkup.CD_MPPNG_SK"),
    how="left"
)

# Select and transform columns for the output link (LoadFile)
df_bl_output = df_businesslogic.select(
    col("Extract.VBB_PLN_ELIG_SK").alias("VBB_PLN_ELIG_SK"),
    col("Extract.VBB_PLN_UNIQ_KEY").alias("VBB_PLN_UNIQ_KEY"),
    col("Extract.GRP_ID").alias("GRP_ID"),
    col("Extract.CLS_ID").alias("CLS_ID"),
    col("Extract.CLS_PLN_ID").alias("CLS_PLN_ID"),
    when(col("SrcSysCdLkup.TRGT_CD").isNull(), lit("UNK")).otherwise(col("SrcSysCdLkup.TRGT_CD")).alias("SRC_SYS_CD"),
    rpad(lit(CurrRunCycleDate), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad(lit(CurrRunCycleDate), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("Extract.CLS_SK").alias("CLS_SK"),
    col("Extract.CLS_PLN_SK").alias("CLS_PLN_SK"),
    col("Extract.GRP_SK").alias("GRP_SK"),
    col("Extract.VBB_PLN_SK").alias("VBB_PLN_SK"),
    rpad(col("Extract.VBB_PLN_ELIG_END_DT_SK"), 10, " ").alias("GRP_VBB_PLN_ELIG_END_DT_SK"),
    rpad(col("Extract.VBB_PLN_ELIG_STRT_DT_SK"), 10, " ").alias("GRP_VBB_PLN_ELIG_STRT_DT_SK"),
    lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("Extract.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK")
)

# Write to CSeqFileStage
write_files(
    df_bl_output,
    f"{adls_path}/load/GRP_VBB_PLN_ELIG_F.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote='"',
    nullValue=None
)