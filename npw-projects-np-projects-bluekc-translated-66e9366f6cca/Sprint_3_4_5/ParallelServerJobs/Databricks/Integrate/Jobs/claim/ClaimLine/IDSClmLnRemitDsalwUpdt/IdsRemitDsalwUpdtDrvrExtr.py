# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC *
# MAGIC COPYRIGHT 2004 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC hf_clmln_dsalw_refresh_drvr_not_zero;
# MAGIC hf_clmln_dsalw_refresh_drvr_null
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   Pulls data from ids  and update edw.
# MAGIC 
# MAGIC                
# MAGIC TRANSFORMS:  
# MAGIC                   TRIM all the fields that are extracted or used in lookups
# MAGIC                   TRIM claim numbers from tables to match to clm_ln
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                   IDS - read from source, lookup all code SK values and get the natural codes
# MAGIC  
# MAGIC                   Individual transforms trim claim number so they can match up to clm_ln claim number
# MAGIC                   First main transform matches cdma codes on claim line table, gets values from other IDS tables
# MAGIC                   Second main transform matches other codes from cdma codes hash file and gets date mo/yr fields
# MAGIC   
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                     CLM_LN_F
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer               		   Date                 Project/Altiris #      Change Description                                                                                                              Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------            		  --------------------     ------------------------      -----------------------------------------------------------------------                                                                       --------------------------------       -------------------------------   ----------------------------       
# MAGIC Shanmugam Annamalai 	 2017-03-02         5321                     SQL in stage 'OverCharge_DIFF' will be aliased to match with stage meta data	      IntegrateDev2                   Jag Yelavarthi            2017-03-07

# MAGIC IDS  Claim Line Remit Disallow Amount Compare for Driver
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import rpad
from pyspark.sql.functions import col
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------

# Parameters
RunID = get_widget_value('RunID','')
FacetsRunCyc = get_widget_value('FacetsRunCyc','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

# Get DB config
jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# OverCharge_DIFF - Read from IDS
extract_query_OverCharge_DIFF = f"""
SELECT 
  ALT.SRC_SYS_CD_SK,
  ALT.CLM_ID,
  ALT.CLM_LN_SEQ_NO,
  ALT.CLM_LN_DSALW_TYP_CD_SK,
  CLM.CRT_RUN_CYC_EXCTN_SK,
  CLM.LAST_UPDT_RUN_CYC_EXCTN_SK,
  ALT.CLM_LN_SK,
  ALT.REMIT_DSALW_AMT AS ALT_REMIT_DSALW_AMT,
  REMIT.REMIT_DSALW_AMT AS REMIT_REMIT_DSALW_AMT,
  (ALT.REMIT_DSALW_AMT - REMIT.REMIT_DSALW_AMT) AS DIFF_REMIT_DSALW_AMT,
  'DIFF' AS OVERCHARGE_TYPE
FROM {IDSOwner}.CLM_LN_ALT_CHRG_REMIT_DSALW ALT
JOIN {IDSOwner}.CLM_LN_REMIT_DSALW REMIT
  ON ALT.CLM_LN_ALT_CHRG_REMIT_DSALW_SK = REMIT.CLM_LN_REMIT_DSALW_SK
JOIN {IDSOwner}.CLM CLM
  ON CLM.SRC_SYS_CD_SK = ALT.SRC_SYS_CD_SK
  AND CLM.CLM_ID = ALT.CLM_ID
WHERE CLM.LAST_UPDT_RUN_CYC_EXCTN_SK >= {FacetsRunCyc}
  AND ALT.REMIT_DSALW_AMT <> REMIT.REMIT_DSALW_AMT
"""
df_OverCharge_DIFF = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_OverCharge_DIFF)
    .load()
)
df_OverCharge_DIFF = df_OverCharge_DIFF.withColumnRenamed("CLM_LN_DSALW_TYP_CD_SK", "ALT_CHRG_CLM_LN_DSALW_TYP_CD_SK")

df_amt_diff = dedup_sort(
    df_OverCharge_DIFF,
    ["SRC_SYS_CD_SK","CLM_ID","CLM_LN_SEQ_NO","ALT_CHRG_CLM_LN_DSALW_TYP_CD_SK"],
    []
)

# OverCharge_NULL - Read from IDS
extract_query_OverCharge_NULL = f"""
SELECT 
  ALT.SRC_SYS_CD_SK,
  ALT.CLM_ID,
  ALT.CLM_LN_SEQ_NO,
  ALT.CLM_LN_DSALW_TYP_CD_SK,
  CLM.CRT_RUN_CYC_EXCTN_SK,
  CLM.LAST_UPDT_RUN_CYC_EXCTN_SK,
  ALT.CLM_LN_SK,
  ALT.REMIT_DSALW_AMT AS ALT_REMIT_DSALW_AMT,
  0 AS REMIT_REMIT_DSALW_AMT,
  ALT.REMIT_DSALW_AMT AS DIFF_REMIT_DSALW_AMT,
  'NULL' AS OVERCHARGE_TYPE
FROM {IDSOwner}.CLM_LN_ALT_CHRG_REMIT_DSALW ALT
JOIN {IDSOwner}.CLM CLM
  ON CLM.SRC_SYS_CD_SK = ALT.SRC_SYS_CD_SK
  AND CLM.CLM_ID = ALT.CLM_ID
WHERE CLM.LAST_UPDT_RUN_CYC_EXCTN_SK >= {FacetsRunCyc}
  AND NOT EXISTS (
    SELECT REMIT.CLM_LN_REMIT_DSALW_SK 
    FROM {IDSOwner}.CLM_LN_REMIT_DSALW REMIT
    WHERE REMIT.CLM_LN_REMIT_DSALW_SK = ALT.CLM_LN_ALT_CHRG_REMIT_DSALW_SK
  )
"""
df_OverCharge_NULL = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_OverCharge_NULL)
    .load()
)
df_OverCharge_NULL = df_OverCharge_NULL.withColumnRenamed("CLM_LN_DSALW_TYP_CD_SK", "ALT_CHRG_CLM_LN_DSALW_TYP_CD_SK")

df_amt_missing = dedup_sort(
    df_OverCharge_NULL,
    ["SRC_SYS_CD_SK","CLM_ID","CLM_LN_SEQ_NO","ALT_CHRG_CLM_LN_DSALW_TYP_CD_SK"],
    []
)

# Link_Collector_388 - Union
df_overcharges = df_amt_diff.unionByName(df_amt_missing)

# Select final columns in correct order
df_overcharges = df_overcharges.select(
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "CLM_LN_SEQ_NO",
    "ALT_CHRG_CLM_LN_DSALW_TYP_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CLM_LN_SK",
    "ALT_REMIT_DSALW_AMT",
    "REMIT_REMIT_DSALW_AMT",
    "DIFF_REMIT_DSALW_AMT",
    "OVERCHARGE_TYPE"
)

# RPad for char/varchar columns
df_overcharges = df_overcharges.withColumn("CLM_ID", rpad(col("CLM_ID"), <...>, " "))
df_overcharges = df_overcharges.withColumn("OVERCHARGE_TYPE", rpad(col("OVERCHARGE_TYPE"), 4, " "))

# W_CLM_LN_REMIT_DSALW_UPDT - Write to .dat
write_files(
    df_overcharges,
    f"{adls_path}/load/W_CLM_LN_REMIT_DSALW_UPDT.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)