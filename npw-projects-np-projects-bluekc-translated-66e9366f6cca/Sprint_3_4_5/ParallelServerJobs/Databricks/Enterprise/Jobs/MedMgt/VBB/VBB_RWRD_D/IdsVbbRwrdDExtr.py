# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2013 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  EdwVbbExtrSeq
# MAGIC 
# MAGIC PROCESSING: Extracts data from IDS VBB_RWRD table and creates a load file for EDW VBB_RWRD_D
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date               Project/Altiris #                  Change Description                               Development Project      Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------      ---------------------------------------------------------       ----------------------------------     ---------------------------------    -------------------------   
# MAGIC Kalyan Neelam                2013-05-11    4963 VBB Phase III           Initial Programming                                    EnterpriseNewDevl         Bhoomi Dasari            5/17/2013

# MAGIC EDW VBB_RWRD extract from IDS
# MAGIC Business Rules that determine Edw Output
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, when, rpad
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


CurrRunCycleDate = get_widget_value("CurrRunCycleDate","2007-12-12")
EDWRunCycle = get_widget_value("EDWRunCycle","0")
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
ExtractRunCycle = get_widget_value("ExtractRunCycle","")

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"""SELECT VBB_RWRD.VBB_RWRD_SK,
       VBB_RWRD.VBB_VNDR_UNIQ_KEY,
       VBB_RWRD.VBB_VNDR_RWRD_SEQ_NO,
       VBB_RWRD.SRC_SYS_CD_SK,
       VBB_RWRD.CRT_RUN_CYC_EXCTN_SK,
       VBB_RWRD.LAST_UPDT_RUN_CYC_EXCTN_SK,
       VBB_RWRD.VBB_RWRD_BEG_DT_SK,
       VBB_RWRD.VBB_RWRD_END_DT_SK,
       VBB_RWRD.SRC_SYS_CRT_DTM,
       VBB_RWRD.SRC_SYS_UPDT_DTM,
       VBB_RWRD.VBB_RWRD_DESC,
       VBB_RWRD.VBB_RWRD_TYP_NM,
       VBB_RWRD.VBB_VNDR_NM
FROM {IDSOwner}.VBB_RWRD VBB_RWRD
WHERE VBB_RWRD.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtractRunCycle};

{IDSOwner}.WELNS_CLS
"""

df_VBB_RWRD = (
    spark.read.format("jdbc")
        .option("url", jdbc_url)
        .options(**jdbc_props)
        .option("query", extract_query)
        .load()
)

df_hf_cdma_codes = spark.read.parquet(f"{adls_path}/hf_cdma_codes.parquet")

e = df_VBB_RWRD.alias("Extract")
c = df_hf_cdma_codes.alias("Src_sys_cd")

df_enriched = (
    e.join(c, e["SRC_SYS_CD_SK"] == c["CD_MPPNG_SK"], "left")
     .select(
         e["VBB_RWRD_SK"].alias("VBB_RWRD_SK"),
         e["VBB_VNDR_UNIQ_KEY"].alias("VBB_VNDR_UNIQ_KEY"),
         e["VBB_VNDR_RWRD_SEQ_NO"].alias("VBB_VNDR_RWRD_SEQ_NO"),
         when(c["TRGT_CD"].isNull(), lit("UNK")).otherwise(c["TRGT_CD"]).alias("SRC_SYS_CD"),
         rpad(lit(CurrRunCycleDate), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
         rpad(lit(CurrRunCycleDate), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
         rpad(e["VBB_RWRD_BEG_DT_SK"], 10, " ").alias("VBB_RWRD_BEG_DT_SK"),
         rpad(e["VBB_RWRD_END_DT_SK"], 10, " ").alias("VBB_RWRD_END_DT_SK"),
         e["SRC_SYS_CRT_DTM"].alias("SRC_SYS_CRT_DTM"),
         e["SRC_SYS_UPDT_DTM"].alias("SRC_SYS_UPDT_DTM"),
         e["VBB_RWRD_DESC"].alias("VBB_RWRD_DESC"),
         e["VBB_RWRD_TYP_NM"].alias("VBB_RWRD_TYP_NM"),
         e["VBB_VNDR_NM"].alias("VBB_VNDR_NM"),
         lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
         lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
         e["LAST_UPDT_RUN_CYC_EXCTN_SK"].alias("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK")
     )
)

write_files(
    df_enriched,
    f"{adls_path}/load/VBB_RWRD_D.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)