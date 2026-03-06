# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2013 - 2023 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  EdwBhiMbrCobStep3Load
# MAGIC CALLED BY:  EdwBhiCOBItemSplitSteps2to7Seq
# MAGIC 
# MAGIC DESCRIPTION:  step 3 in item split 
# MAGIC 
# MAGIC HASH FILES:  
# MAGIC 
# MAGIC 
# MAGIC RERUN INFORMATION: 
# MAGIC 
# MAGIC              PREVIOUS RUN SUCCESSFUL:   Nothing to reset. 
# MAGIC              PREVIOUS RUN ABORTED:         Nothing to reset, just restart.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                 Date                    Project/Ticket #\(9)           Change Description\(9)\(9)\(9)              Development Project\(9)      Code Reviewer\(9)        Date Reviewed       
# MAGIC ------------------               -------------------         ---------------------------\(9)           -----------------------------------------------------------------------            --------------------------------\(9)      -------------------------         -------------------------    
# MAGIC Praveen Annam              2013-08-28          5115 BHI                               Original programming                                            EnterpriseNewDevl           Kalyan Neelam          2013-11-25             
# MAGIC 
# MAGIC Mrudula Kodali        2023-10-18            596836          Added parameter $APT_CONFIG_FILE  to use node_1.apt Configuration file    EnterpriseDev2\(9)Ken Bradmon\(9)2023-10-27

# MAGIC Step 3 on ItemSpit Table for COB
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, rpad
from pyspark.sql.types import StringType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')

jdbc_url, jdbc_props = get_db_config(edw_secret_name)
extract_query = (
    f"select "
    f"A.MBR_UNIQ_KEY, "
    f"A.MBR_EFF_DT_SK, "
    f"A.MBR_TERM_DT_SK, "
    f"A.MBR_COB_TYP_CD, "
    f"A.MBR_COB_EFF_DT_SK, "
    f"A.MBR_COB_TERM_DT_SK, "
    f"A.BEG_EFF_DT_SK, "
    f"A.BEG_TERM_DT_SK, "
    f"A.MID_EFF_DT_SK, "
    f"A.MID_TERM_DT_SK, "
    f"A.END_EFF_DT_SK, "
    f"A.END_TERM_DT_SK "
    f"FROM {EDWOwner}.W_BHI_MBR_COB_SPLT A, "
    f"{EDWOwner}.W_BHI_MBR_COB_SPLT B "
    f"WHERE A.MBR_UNIQ_KEY = B.MBR_UNIQ_KEY "
    f"AND A.BEG_TERM_DT_SK = B.MID_TERM_DT_SK"
)

df_W_BHI_MBR_COB_SPLT_1_And_2 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_Trans = df_W_BHI_MBR_COB_SPLT_1_And_2.select(
    col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("MBR_EFF_DT_SK").alias("MBR_EFF_DT_SK"),
    col("MBR_TERM_DT_SK").alias("MBR_TERM_DT_SK"),
    col("MBR_COB_TYP_CD").alias("MBR_COB_TYP_CD"),
    col("MBR_COB_EFF_DT_SK").alias("MBR_COB_EFF_DT_SK"),
    col("MBR_COB_TERM_DT_SK").alias("MBR_COB_TERM_DT_SK"),
    lit(None).cast(StringType()).alias("BEG_EFF_DT_SK"),
    lit(None).cast(StringType()).alias("BEG_TERM_DT_SK"),
    col("MID_EFF_DT_SK").alias("MID_EFF_DT_SK"),
    col("MID_TERM_DT_SK").alias("MID_TERM_DT_SK"),
    col("END_EFF_DT_SK").alias("END_EFF_DT_SK"),
    col("END_TERM_DT_SK").alias("END_TERM_DT_SK"),
    lit(None).cast(StringType()).alias("UPDT_IN")
)

df_final = df_Trans.select(
    col("MBR_UNIQ_KEY"),
    rpad("MBR_EFF_DT_SK", 10, " ").alias("MBR_EFF_DT_SK"),
    rpad("MBR_TERM_DT_SK", 10, " ").alias("MBR_TERM_DT_SK"),
    col("MBR_COB_TYP_CD"),
    rpad("MBR_COB_EFF_DT_SK", 10, " ").alias("MBR_COB_EFF_DT_SK"),
    rpad("MBR_COB_TERM_DT_SK", 10, " ").alias("MBR_COB_TERM_DT_SK"),
    rpad("BEG_EFF_DT_SK", 10, " ").alias("BEG_EFF_DT_SK"),
    rpad("BEG_TERM_DT_SK", 10, " ").alias("BEG_TERM_DT_SK"),
    rpad("MID_EFF_DT_SK", 10, " ").alias("MID_EFF_DT_SK"),
    rpad("MID_TERM_DT_SK", 10, " ").alias("MID_TERM_DT_SK"),
    rpad("END_EFF_DT_SK", 10, " ").alias("END_EFF_DT_SK"),
    rpad("END_TERM_DT_SK", 10, " ").alias("END_TERM_DT_SK"),
    rpad("UPDT_IN", 1, " ").alias("UPDT_IN")
)

write_files(
    df_final,
    f"{adls_path}/ds/BhiMbrCobStep3.parquet",
    ',',
    'overwrite',
    True,
    True,
    '"',
    None
)