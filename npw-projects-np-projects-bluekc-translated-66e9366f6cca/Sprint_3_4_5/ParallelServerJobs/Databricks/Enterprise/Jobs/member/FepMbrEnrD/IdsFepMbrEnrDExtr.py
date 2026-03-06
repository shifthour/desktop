# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2013 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC 	   Pulls data from IDS and codes hash file to load into the EDW
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                                     Date                      Description                    Environment                      Code Reviewer                      Review Date
# MAGIC ----------------------------------         --------------------------------       ------------------------------------  -----------------------------        ------------------------------------       -----------------------------------
# MAGIC Santosh Bokka                       09/12/2013               - Originally Program       - EdwNewDevl                    Kalyan Neelam                     2013-10-08

# MAGIC Extracts source data from the FEP_MBR_ENR  table in the IDS and Load into FEP_MBR_ENR_D
# MAGIC IDS FEP_MBR_ENR Extract
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.functions import rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
IDSRunCycle = get_widget_value('IDSRunCycle','101')
EDWRunCycle = get_widget_value('EDWRunCycle','100')
CurrentDate = get_widget_value('CurrentDate','')
RunID = get_widget_value('RunID','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"SELECT MBR_SK,LGCY_FEP_MBR_ID,SRC_SYS_CD_SK,CRT_RUN_CYC_EXCTN_SK,LAST_UPDT_RUN_CYC_EXCTN_SK FROM {IDSOwner}.FEP_MBR_ENR where LAST_UPDT_RUN_CYC_EXCTN_SK >= {IDSRunCycle}"

df_IDSFepMbrEnr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_hf_cdma_codes = spark.read.parquet(f"{adls_path}/hf_cdma_codes.parquet")

df_enriched = (
    df_IDSFepMbrEnr.alias("Fep_Mer_Enr")
    .join(
        df_hf_cdma_codes.alias("SrcCdLkup"),
        F.col("Fep_Mer_Enr.SRC_SYS_CD_SK") == F.col("SrcCdLkup.CD_MPPNG_SK"),
        "left"
    )
    .select(
        F.col("Fep_Mer_Enr.MBR_SK").alias("MBR_SK"),
        F.col("Fep_Mer_Enr.LGCY_FEP_MBR_ID").alias("LGCY_FEP_MBR_ID"),
        F.when(F.col("SrcCdLkup.TRGT_CD").isNull(), F.lit("UNK")).otherwise(F.col("SrcCdLkup.TRGT_CD")).alias("SRC_SYS_CD"),
        F.lit(CurrentDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.lit(CurrentDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.col("Fep_Mer_Enr.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("Fep_Mer_Enr.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK")
    )
)

df_enriched = df_enriched.withColumn(
    "CRT_RUN_CYC_EXCTN_DT_SK",
    rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
)

df_final = df_enriched.select(
    "MBR_SK",
    "LGCY_FEP_MBR_ID",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"
)

write_files(
    df_final,
    f"{adls_path}/load/FEP_MBR_ENR_D.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)