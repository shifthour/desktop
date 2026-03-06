# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2013 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  EdwVbbExtrSeq
# MAGIC 
# MAGIC PROCESSING: Extracts data from IDS VBB_PLN_CMPNT_DPNDC table and creates a load file for EDW VBB_PLN_CMPNT_DPNDC_D
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date               Project/Altiris #                  Change Description                               Development Project      Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------      ---------------------------------------------------------       ----------------------------------     ---------------------------------    -------------------------   
# MAGIC Kalyan Neelam                2013-05-11    4963 VBB Phase III           Initial Programming                                    EnterpriseNewdevl         Bhoomi Dasari             5/17/2013

# MAGIC EDW VBB_PLN_CMPNT_DPNDC extract from IDS
# MAGIC Business Rules that determine Edw Output
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, when, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------

jdbc_url, jdbc_props = None, None  # Placeholder variables to satisfy script continuity

# Parameters
CurrRunCycleDate = get_widget_value('CurrRunCycleDate','2007-12-12')
EDWRunCycle = get_widget_value('EDWRunCycle','0')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
ExtractRunCycle = get_widget_value('ExtractRunCycle','')

# DB2Connector: VBB_PLN_CMPNT_DPNDC
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = (
    "SELECT VBB_PLN_CMPNT_DPNDC.VBB_PLN_CMPNT_DPNDC_SK,"
    "VBB_PLN_CMPNT_DPNDC.VBB_PLN_UNIQ_KEY,"
    "VBB_PLN_CMPNT_DPNDC.VBB_CMPNT_UNIQ_KEY,"
    "VBB_PLN_CMPNT_DPNDC.RQRD_VBB_CMPNT_UNIQ_KEY,"
    "VBB_PLN_CMPNT_DPNDC.SRC_SYS_CD_SK,"
    "VBB_PLN_CMPNT_DPNDC.CRT_RUN_CYC_EXCTN_SK,"
    "VBB_PLN_CMPNT_DPNDC.LAST_UPDT_RUN_CYC_EXCTN_SK,"
    "VBB_PLN_CMPNT_DPNDC.RQRD_VBB_CMPNT_SK,"
    "VBB_PLN_CMPNT_DPNDC.VBB_CMPNT_SK,"
    "VBB_PLN_CMPNT_DPNDC.VBB_PLN_SK,"
    "VBB_PLN_CMPNT_DPNDC.MUTLLY_XCLSVE_DPNDC_IN,"
    "VBB_PLN_CMPNT_DPNDC.SRC_SYS_CRT_DTM,"
    "VBB_PLN_CMPNT_DPNDC.SRC_SYS_UPDT_DTM,"
    "VBB_PLN_CMPNT_DPNDC.WAIT_PERD_DAYS_NO "
    f"FROM {IDSOwner}.VBB_PLN_CMPNT_DPNDC VBB_PLN_CMPNT_DPNDC "
    f"WHERE VBB_PLN_CMPNT_DPNDC.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtractRunCycle}\n"
    f"{IDSOwner}.WELNS_CLS"
)
df_VBB_PLN_CMPNT_DPNDC = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# CHashedFileStage: hf_cdma_codes (Scenario C)
df_hf_cdma_codes = spark.read.parquet(f"{adls_path}/hf_cdma_codes.parquet")

# CTransformerStage: BussinessLogic
df_BussinessLogic = df_VBB_PLN_CMPNT_DPNDC.alias("Extract").join(
    df_hf_cdma_codes.alias("Src_sys_cd"),
    col("Extract.SRC_SYS_CD_SK") == col("Src_sys_cd.CD_MPPNG_SK"),
    how="left"
)

df_BussinessLogic = df_BussinessLogic.select(
    col("Extract.VBB_PLN_CMPNT_DPNDC_SK").alias("VBB_PLN_CMPNT_DPNDC_SK"),
    col("Extract.VBB_PLN_UNIQ_KEY").alias("VBB_PLN_UNIQ_KEY"),
    col("Extract.VBB_CMPNT_UNIQ_KEY").alias("VBB_CMPNT_UNIQ_KEY"),
    col("Extract.RQRD_VBB_CMPNT_UNIQ_KEY").alias("RQRD_VBB_CMPNT_UNIQ_KEY"),
    when(col("Src_sys_cd.TRGT_CD").isNull(), "UNK").otherwise(col("Src_sys_cd.TRGT_CD")).alias("SRC_SYS_CD"),
    lit(CurrRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    lit(CurrRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("Extract.RQRD_VBB_CMPNT_SK").alias("RQRD_VBB_CMPNT_SK"),
    col("Extract.VBB_CMPNT_SK").alias("VBB_CMPNT_SK"),
    col("Extract.VBB_PLN_SK").alias("VBB_PLN_SK"),
    col("Extract.MUTLLY_XCLSVE_DPNDC_IN").alias("MUTLLY_XCLSVE_DPNDC_IN"),
    col("Extract.SRC_SYS_CRT_DTM").alias("SRC_SYS_CRT_DTM"),
    col("Extract.SRC_SYS_UPDT_DTM").alias("SRC_SYS_UPDT_DTM"),
    col("Extract.WAIT_PERD_DAYS_NO").alias("WAIT_PERD_DAYS_NO"),
    lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("Extract.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK")
)

df_BussinessLogic = df_BussinessLogic.withColumn(
    "CRT_RUN_CYC_EXCTN_DT_SK", rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK", rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
).withColumn(
    "MUTLLY_XCLSVE_DPNDC_IN", rpad(col("MUTLLY_XCLSVE_DPNDC_IN"), 1, " ")
)

# CSeqFileStage: VBB_PLN_CMPNT_DPNDC_D
write_files(
    df_BussinessLogic,
    f"{adls_path}/load/VBB_PLN_CMPNT_DPNDC_D.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)