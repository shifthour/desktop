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
# MAGIC Goutham Kalidindi           9/16/2024                    US-628702                                           New Development                                                                      DatamartDev3                                                           Reddy Sanam              09-30-2024

# MAGIC Job Name: IdsEdwMbrHedisMesrGapExtr
# MAGIC Write MBR_HEDIS_MESR_GAP_DPLY  Data into a Sequential file for Load Job IdsEdwMbrHedisMesrGapLoad.
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


# Parameters
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
IDSRunCycle = get_widget_value('IDSRunCycle','')

# Read from DB2ConnectorPX Stage: db2_MBR_HEDIS_MESR_GRP_in
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = (
    "Select "
    "HEDIS_MESR_NM, "
    "HEDIS_SUB_MESR_NM, "
    "HEDIS_MBR_BUCKET_ID, "
    "MOD_ID, "
    "HEDIS_MESR_GAP_DPLY_MOD_EFF_DT, "
    "SRC_SYS_CD, "
    "HEDIS_MESR_ABBR_ID, "
    "HEDIS_MESR_GAP_DPLY_MOD_TERM_DT AS HEDIS_MESR_GAP_DPLY_MOD_TERMDT, "
    "HEDIS_MESR_GAP_MOD_CAT_ID, "
    "HEDIS_MESR_MOD_KEY_ID, "
    "HEDIS_MESR_GAP_MOD_DPLY_TX, "
    "HEDIS_MESR_GAP_MOD_PRTY_NO, "
    "HEDIS_MESR_GAP_MOD_SCRIPT_TX, "
    "LAST_UPDT_RUN_CYC_EXCTN_SK AS IDS_LAST_UPDT_RUN_CYC_EXCTN_SK "
    "from " + IDSOwner + ".HEDIS_MESR_GAP_DPLY"
)
df_db2_MBR_HEDIS_MESR_GRP_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# Transformer Stage: xfrm_BusinessLogic
df_xfrm_BusinessLogic = df_db2_MBR_HEDIS_MESR_GRP_in.select(
    col("HEDIS_MESR_NM").alias("HEDIS_MESR_NM"),
    col("HEDIS_SUB_MESR_NM").alias("HEDIS_SUB_MESR_NM"),
    col("HEDIS_MBR_BUCKET_ID").alias("HEDIS_MBR_BUCKET_ID"),
    col("MOD_ID").alias("MOD_ID"),
    col("HEDIS_MESR_GAP_DPLY_MOD_EFF_DT").alias("HEDIS_MESR_GAP_DPLY_MOD_EFF_DT"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("HEDIS_MESR_ABBR_ID").alias("HEDIS_MESR_ABBR_ID"),
    col("HEDIS_MESR_GAP_DPLY_MOD_TERMDT").alias("HEDIS_MESR_GAP_DPLY_MOD_TERMDT"),
    col("HEDIS_MESR_GAP_MOD_CAT_ID").alias("HEDIS_MESR_GAP_MOD_CAT_ID"),
    col("HEDIS_MESR_GAP_MOD_DPLY_TX").alias("HEDIS_MESR_GAP_MOD_DPLY_TX"),
    col("HEDIS_MESR_GAP_MOD_PRTY_NO").alias("HEDIS_MESR_GAP_MOD_PRTY_NO"),
    col("HEDIS_MESR_GAP_MOD_SCRIPT_TX").alias("HEDIS_MESR_GAP_MOD_SCRIPT_TX"),
    col("HEDIS_MESR_MOD_KEY_ID").alias("HEDIS_MESR_MOD_KEY_ID"),
    lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_NO"),
    col("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK").alias("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK")
)

# Prepare final DataFrame for writing (PxSequentialFile): seq_MED_MGT_DM_HEDIS_MESR_GAP
df_final = df_xfrm_BusinessLogic.select(
    rpad(col("HEDIS_MESR_NM"), <...>, " ").alias("HEDIS_MESR_NM"),
    rpad(col("HEDIS_SUB_MESR_NM"), <...>, " ").alias("HEDIS_SUB_MESR_NM"),
    rpad(col("HEDIS_MBR_BUCKET_ID"), <...>, " ").alias("HEDIS_MBR_BUCKET_ID"),
    rpad(col("MOD_ID"), <...>, " ").alias("MOD_ID"),
    rpad(col("HEDIS_MESR_GAP_DPLY_MOD_EFF_DT"), <...>, " ").alias("HEDIS_MESR_GAP_DPLY_MOD_EFF_DT"),
    rpad(col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    rpad(col("HEDIS_MESR_ABBR_ID"), <...>, " ").alias("HEDIS_MESR_ABBR_ID"),
    rpad(col("HEDIS_MESR_GAP_DPLY_MOD_TERMDT"), <...>, " ").alias("HEDIS_MESR_GAP_DPLY_MOD_TERMDT"),
    rpad(col("HEDIS_MESR_GAP_MOD_CAT_ID"), <...>, " ").alias("HEDIS_MESR_GAP_MOD_CAT_ID"),
    rpad(col("HEDIS_MESR_GAP_MOD_DPLY_TX"), <...>, " ").alias("HEDIS_MESR_GAP_MOD_DPLY_TX"),
    rpad(col("HEDIS_MESR_GAP_MOD_PRTY_NO"), <...>, " ").alias("HEDIS_MESR_GAP_MOD_PRTY_NO"),
    rpad(col("HEDIS_MESR_GAP_MOD_SCRIPT_TX"), <...>, " ").alias("HEDIS_MESR_GAP_MOD_SCRIPT_TX"),
    rpad(col("HEDIS_MESR_MOD_KEY_ID"), <...>, " ").alias("HEDIS_MESR_MOD_KEY_ID"),
    rpad(col("LAST_UPDT_RUN_CYC_NO"), <...>, " ").alias("LAST_UPDT_RUN_CYC_NO"),
    rpad(col("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"), <...>, " ").alias("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK")
)

write_files(
    df_final,
    f"{adls_path}/load/MED_MGT_DM_HEDIS_MESR_GAP_DPLY.dat",
    delimiter="^",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote=None,
    nullValue=None
)