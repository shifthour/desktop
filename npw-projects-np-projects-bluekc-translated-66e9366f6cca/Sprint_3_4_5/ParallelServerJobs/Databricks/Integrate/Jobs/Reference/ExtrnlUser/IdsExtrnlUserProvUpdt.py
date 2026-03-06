# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC ***************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2011 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:   Updates EXTRNL_USER_ID from EXTRNL_USER (IDS) table, as IDS table runs after PROVIDER ZENA job
# MAGIC 
# MAGIC PROCESSING:    
# MAGIC       
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer             Date                         Change Description                                    Project #                     Development Project          Code Reviewer           Date Reviewed  
# MAGIC ------------------           ----------------------------     ---------------------------------------------------------------   ----------------                    ------------------------------------       ----------------------------          ----------------
# MAGIC Bhoomi Dasari       02/10/2011             Update EXTRNL_USER_ID field                4574 - Availity              IntegrateNewDevl               Steph Goddard           02/11/2011
# MAGIC 
# MAGIC Manasa Andru      2017-02-27               Corrected the hashed file name in the          TFS - 16028               IntegrateDev1                    Kalyan Neelam              2017-03-03
# MAGIC                                                           output link in the Hashed_Files stage to match
# MAGIC                                                                           with the input link.

# MAGIC Updates EXTRNL_USER_ID field after IDS run of EXTRNL_USER table
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


ClmMartOwner = get_widget_value('ClmMartOwner','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
df_IDSProv_extract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", "SELECT SRC_SYS_CD_SK,PROV_ID,PROV_SK FROM #$IDSOwner#.PROV WHERE PROV_SK < 0 OR PROV_SK > 1")
    .load()
)

df_IDSProv_lodExtrnlUserId = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", "SELECT PROV_SK,EXTRNL_USER_ID FROM #$IDSOwner#.EXTRNL_USER WHERE EXTRNL_USER_CNSTTNT_TYP_CD IN ('PROVIDER','PROVGRP','PROVIPA')")
    .load()
)

df_hf_etrnl_cd_mppng = spark.read.parquet(f"{adls_path}/hf_etrnl_cd_mppng.parquet")
df_hf_etrnl_cd_mppng = df_hf_etrnl_cd_mppng.select(
    "CD_MPPNG_SK",
    "TRGT_CD",
    "TRGT_CD_NM",
    "SRC_CD",
    "SRC_CD_NM"
)

df_Hashed_Files_refExtrnlUserId = dedup_sort(
    df_IDSProv_lodExtrnlUserId,
    ["PROV_SK"],
    []
).select("PROV_SK", "EXTRNL_USER_ID")

df_Trans1_joined_1 = df_IDSProv_extract.alias("Extract").join(
    df_hf_etrnl_cd_mppng.alias("refSrcSys"),
    F.col("Extract.SRC_SYS_CD_SK") == F.col("refSrcSys.CD_MPPNG_SK"),
    "left"
)

df_Trans1_joined_2 = df_Trans1_joined_1.join(
    df_Hashed_Files_refExtrnlUserId.alias("refExtrnlUserId"),
    F.col("Extract.PROV_SK") == F.col("refExtrnlUserId.PROV_SK"),
    "left"
)

df_Trans1_filtered = df_Trans1_joined_2.filter(
    F.col("refExtrnlUserId.EXTRNL_USER_ID").isNotNull()
)

df_Trans1_out = df_Trans1_filtered.select(
    F.when(F.col("refSrcSys.TRGT_CD").isNull(), F.lit("UNK ")).otherwise(F.col("refSrcSys.TRGT_CD")).alias("SRC_SYS_CD"),
    F.col("Extract.PROV_ID").alias("PROV_ID"),
    F.when(
        F.col("refExtrnlUserId.PROV_SK").isNull() | (F.col("refExtrnlUserId.EXTRNL_USER_ID") == "NA"),
        F.lit(" ")
    ).otherwise(F.col("refExtrnlUserId.EXTRNL_USER_ID")).alias("EXTRNL_USER_ID")
)

df_final = (
    df_Trans1_out
    .withColumn("SRC_SYS_CD", F.rpad(F.col("SRC_SYS_CD"), <...>, " "))
    .withColumn("PROV_ID", F.rpad(F.col("PROV_ID"), <...>, " "))
    .withColumn("EXTRNL_USER_ID", F.rpad(F.col("EXTRNL_USER_ID"), <...>, " "))
    .select("SRC_SYS_CD", "PROV_ID", "EXTRNL_USER_ID")
)

jdbc_url_clmmart, jdbc_props_clmmart = get_db_config(clmmart_secret_name)
execute_dml(
    "DROP TABLE IF EXISTS STAGING.IdsExtrnlUserProvUpdt_PROV_DM_PROV_temp",
    jdbc_url_clmmart,
    jdbc_props_clmmart
)

df_final.write.format("jdbc") \
    .option("url", jdbc_url_clmmart) \
    .options(**jdbc_props_clmmart) \
    .option("dbtable", "STAGING.IdsExtrnlUserProvUpdt_PROV_DM_PROV_temp") \
    .mode("append") \
    .save()

merge_sql = """
MERGE #$ClmMartOwner#.PROV_DM_PROV AS T
USING STAGING.IdsExtrnlUserProvUpdt_PROV_DM_PROV_temp AS S
ON T.SRC_SYS_CD = S.SRC_SYS_CD
AND T.PROV_ID = S.PROV_ID
WHEN MATCHED THEN
  UPDATE SET T.EXTRNL_USER_ID = S.EXTRNL_USER_ID
WHEN NOT MATCHED THEN
  INSERT (SRC_SYS_CD, PROV_ID, EXTRNL_USER_ID)
  VALUES(S.SRC_SYS_CD, S.PROV_ID, S.EXTRNL_USER_ID);
"""

execute_dml(merge_sql, jdbc_url_clmmart, jdbc_props_clmmart)