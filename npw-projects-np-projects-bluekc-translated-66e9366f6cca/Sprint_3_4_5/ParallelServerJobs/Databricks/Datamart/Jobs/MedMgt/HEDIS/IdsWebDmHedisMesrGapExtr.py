# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2005 - 2023 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:  IdsWebDmHedisMesrGapExtr 
# MAGIC CALLED BY : IdsWebDmHedisMesrGapSeq
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                This Job extracts Data from IDS and creates a Load file for DataMartTable MED_MGT_DM_HEDIS_MESR_GAP
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                              Project/Altiris #                                        Change Description                                                                Development Project                                              Code Reviewer            Date Reviewed
# MAGIC ==========================================================================================================================================================================================================
# MAGIC Goutham Kalidindi           9/16/2024                    US-628702                                           New Development                                                                      DatamartDev3                                                         Reddy Sanam               09/30/2024

# MAGIC Job Name: IdsEdwMbrHedisMesrGapExtr
# MAGIC Write MBR_HEDIS_MESR_GAP Data into a Sequential file for Load Job IdsEdwMbrHedisMesrGapLoad.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_DataMart3
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"Select HEDIS_MESR_NM, HEDIS_SUB_MESR_NM, HEDIS_MBR_BUCKET_ID, SRC_SYS_CD, HEDIS_MESR_ABBR_ID, LAST_UPDT_RUN_CYC_EXCTN_SK AS IDS_LAST_UPDT_RUN_CYC_EXCTN_SK from {IDSOwner}.HEDIS_MESR_GAP"
df_db2_MBR_HEDIS_MESR_GRP_in = (
    spark.read.format("jdbc")
        .option("url", jdbc_url)
        .options(**jdbc_props)
        .option("query", extract_query)
        .load()
)

df_xfrm_BusinessLogic = df_db2_MBR_HEDIS_MESR_GRP_in.select(
    col("HEDIS_MESR_NM").alias("HEDIS_MESR_NM"),
    col("HEDIS_SUB_MESR_NM").alias("HEDIS_SUB_MESR_NM"),
    col("HEDIS_MBR_BUCKET_ID").alias("HEDIS_MBR_BUCKET_ID"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("HEDIS_MESR_ABBR_ID").alias("HEDIS_MESR_ABBR_ID"),
    lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_NO"),
    col("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK").alias("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK")
)

df_seq_MED_MGT_DM_HEDIS_MESR_GAP = (
    df_xfrm_BusinessLogic
    .withColumn("HEDIS_MESR_NM", rpad(col("HEDIS_MESR_NM"), <...>, " "))
    .withColumn("HEDIS_SUB_MESR_NM", rpad(col("HEDIS_SUB_MESR_NM"), <...>, " "))
    .withColumn("HEDIS_MBR_BUCKET_ID", rpad(col("HEDIS_MBR_BUCKET_ID"), <...>, " "))
    .withColumn("SRC_SYS_CD", rpad(col("SRC_SYS_CD"), <...>, " "))
    .withColumn("HEDIS_MESR_ABBR_ID", rpad(col("HEDIS_MESR_ABBR_ID"), <...>, " "))
    .withColumn("LAST_UPDT_RUN_CYC_NO", rpad(col("LAST_UPDT_RUN_CYC_NO"), <...>, " "))
    .withColumn("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK", rpad(col("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"), <...>, " "))
    .select(
        "HEDIS_MESR_NM",
        "HEDIS_SUB_MESR_NM",
        "HEDIS_MBR_BUCKET_ID",
        "SRC_SYS_CD",
        "HEDIS_MESR_ABBR_ID",
        "LAST_UPDT_RUN_CYC_NO",
        "IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"
    )
)

write_files(
    df_seq_MED_MGT_DM_HEDIS_MESR_GAP,
    f"{adls_path}/load/MED_MGT_DM_HEDIS_MESR_GAP.dat",
    delimiter='^',
    mode='overwrite',
    is_pqruet=False,
    header=False,
    quote=None,
    nullValue=None
)