# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 01/29/08 14:29:13 Batch  14639_52158 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 01/09/08 13:32:13 Batch  14619_48738 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/24/07 09:35:50 Batch  14573_34558 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_3 05/24/07 13:29:41 Batch  14389_48653 PROMOTE bckcetl ids20 dsadm Break fix per Ralph Tucker 05/24/07 JHHII
# MAGIC ^1_3 05/24/07 13:21:57 Batch  14389_48178 INIT bckcett testIDS30 dsadm Breakfix migrate per Ralph Tucker 05/24/07
# MAGIC ^1_1 05/23/07 14:48:53 Batch  14388_53337 PROMOTE bckcett testIDS30 u06640 Ralph
# MAGIC ^1_1 05/23/07 14:02:50 Batch  14388_50579 PROMOTE bckcett testIDS30 u06640 Ralph
# MAGIC ^1_1 05/23/07 14:00:42 Batch  14388_50449 PROMOTE bckcett testIDS30 u06640 Ralph
# MAGIC ^1_1 05/23/07 13:59:12 Batch  14388_50364 INIT bckcett devlIDS30 u06640 Ralph
# MAGIC ^1_1 05/16/07 16:01:31 Batch  14381_57693 INIT bckcett devlIDS30 u10157 sa
# MAGIC 
# MAGIC ***************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:   Pulls data from CLM_EXTRNL_PROV for  loading into Data Mart
# MAGIC 
# MAGIC PROCESSING:    
# MAGIC       
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer             Date                         Change Description                                    Project #          Development Project          Code Reviewer           Date Reviewed  
# MAGIC ------------------           ----------------------------     ---------------------------------------------------------------   ----------------         ------------------------------------       ----------------------------          ----------------
# MAGIC Naren Garapaty    11/2006                    Initial program                                            3228                 devlIDS30                          Steph Goddard         
# MAGIC Ralph Tucker        05/23/2007             Changed output file to clear before insert

# MAGIC Transform and output link names are used by job control to get link count.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


runCycle = get_widget_value('RunCycle','100')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
ClmMartOwner = get_widget_value('ClmMartOwner','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"SELECT PROV.SRC_SYS_CD_SK, PROV.PROV_ID, PROV.NTNL_PROV_ID FROM {IDSOwner}.PROV PROV, {IDSOwner}.CD_MPPNG CD_MPPNG WHERE PROV.SRC_SYS_CD_SK=CD_MPPNG.CD_MPPNG_SK AND CD_MPPNG.SRC_CD='FACETS'"
df_IDSProv = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_hf_etrnl_cd_mppng = spark.read.parquet(f"{adls_path}/hf_etrnl_cd_mppng.parquet")

df_Trans1 = (
    df_IDSProv.alias("Extract")
    .join(
        df_hf_etrnl_cd_mppng.alias("refSrcSys"),
        col("Extract.SRC_SYS_CD_SK") == col("refSrcSys.CD_MPPNG_SK"),
        "left"
    )
    .select(
        col("refSrcSys.TRGT_CD").alias("SRC_SYS_CD"),
        col("Extract.PROV_ID").alias("PROV_ID"),
        col("Extract.NTNL_PROV_ID").alias("PROV_NPI")
    )
)

df_Trans1Enriched = (
    df_Trans1
    .withColumn("SRC_SYS_CD", rpad(col("SRC_SYS_CD"), <...>, " "))
    .withColumn("PROV_NPI", rpad(col("PROV_NPI"), <...>, " "))
)

jdbc_url_clmmart, jdbc_props_clmmart = get_db_config(clmmart_secret_name)
drop_temp_sql = "DROP TABLE IF EXISTS STAGING.IdsClmMartPNpiXRefExtr_P_NPI_XREF_temp"
execute_dml(drop_temp_sql, jdbc_url_clmmart, jdbc_props_clmmart)

df_Trans1Enriched.write.format("jdbc") \
    .option("url", jdbc_url_clmmart) \
    .options(**jdbc_props_clmmart) \
    .option("dbtable", "STAGING.IdsClmMartPNpiXRefExtr_P_NPI_XREF_temp") \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO {ClmMartOwner}.P_NPI_XREF AS T
USING STAGING.IdsClmMartPNpiXRefExtr_P_NPI_XREF_temp AS S
ON (T.SRC_SYS_CD = S.SRC_SYS_CD AND T.PROV_ID = S.PROV_ID)
WHEN MATCHED THEN
  UPDATE SET T.PROV_NPI = S.PROV_NPI
WHEN NOT MATCHED THEN
  INSERT (SRC_SYS_CD, PROV_ID, PROV_NPI)
  VALUES (S.SRC_SYS_CD, S.PROV_ID, S.PROV_NPI);
"""
execute_dml(merge_sql, jdbc_url_clmmart, jdbc_props_clmmart)