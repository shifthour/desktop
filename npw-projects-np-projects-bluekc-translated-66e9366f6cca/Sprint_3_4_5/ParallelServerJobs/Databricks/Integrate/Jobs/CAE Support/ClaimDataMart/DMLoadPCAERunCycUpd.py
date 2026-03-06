# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 01/29/08 14:29:13 Batch  14639_52158 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 03/07/07 16:30:43 Batch  14311_59451 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 01/31/07 10:57:41 Batch  14276_39482 PROMOTE bckcetl ids20 dsadm RC
# MAGIC ^1_1 01/31/07 10:53:40 Batch  14276_39257 INIT bckcett testIDS30 dsadm RC 
# MAGIC ^1_2 01/29/07 14:13:46 Batch  14274_51230 PROMOTE bckcett testIDS30 u150129 Laurel
# MAGIC ^1_2 01/29/07 14:11:38 Batch  14274_51101 INIT bckcett devlIDS30 u150129 Laurel
# MAGIC ^1_1 01/26/07 09:43:57 Batch  14271_35041 INIT bckcett devlIDS30 u150129 Laurel
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     DMLoadPCAERunCycUpd
# MAGIC 
# MAGIC DESCRIPTION:  Update the P_RUN_CYC table CDM load indicator fields to show those records have been copied to the Claim Data Mart.
# MAGIC       
# MAGIC INPUTS:
# MAGIC The following IDS table is used:
# MAGIC P_RUN_CYC
# MAGIC  
# MAGIC HASH FILES:
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                  
# MAGIC                           
# MAGIC PROCESSING:  Extracts the P_RUN_CYC records for P_CAE_MBR_PRMCY with the CDM_LOAD_IN = 'N'.  These rows are used to update the P_RUN_CYCLE, load indicator field for the particular source and subject.  All run cycles >= to the current run cycle where the indicator is "N" are updated to "Y".
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                     IDS P_RUN_CYC
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC               
# MAGIC MODIFICATIONS:
# MAGIC Date                 Developer                Change Description
# MAGIC ------------------      ----------------------------     --------------------------------------------------------------------------------------------------------
# MAGIC 2007-01-25      Laurel Kindley           Original Programming.

# MAGIC Update the P_RUN_CYC table to indicate source system data has been loaded to the EDW for each of the run cycles processed.
# MAGIC Set CDM_LOAD_IN to 'Y'
# MAGIC Extract P_RUN_CYC records for P_CAE_MBR_PRMCY  with a value \"N\" in the  CDM_LOAD_IN where the run cycle is >= to the current run cycle used for extracting.
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
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


BeginCycle = get_widget_value("BeginCycle","100")
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = f"SELECT P_RUN_CYC.RUN_CYC_SK as RUN_CYC_SK, P_RUN_CYC.TRGT_SYS_CD as TRGT_SYS_CD, P_RUN_CYC.SRC_SYS_CD as SRC_SYS_CD, P_RUN_CYC.SUBJ_CD as SUBJ_CD, P_RUN_CYC.RUN_CYC_NO as RUN_CYC_NO FROM {IDSOwner}.P_RUN_CYC P_RUN_CYC WHERE P_RUN_CYC.TRGT_SYS_CD = 'CDM' AND P_RUN_CYC.SUBJ_CD = 'P_CAE_MBR_PRMCY' AND P_RUN_CYC.CDM_LOAD_IN = 'N' AND P_RUN_CYC.RUN_CYC_NO >= {BeginCycle}"

df_IDS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_BusinessRules = df_IDS.select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("SUBJ_CD").alias("SUBJ_CD"),
    col("TRGT_SYS_CD").alias("TRGT_SYS_CD"),
    col("RUN_CYC_NO").alias("RUN_CYC_NO"),
    lit("Y").alias("CDM_LOAD_IN")
)

df_BusinessRules_final = df_BusinessRules.select(
    col("SRC_SYS_CD"),
    col("SUBJ_CD"),
    col("TRGT_SYS_CD"),
    col("RUN_CYC_NO"),
    rpad(col("CDM_LOAD_IN"), 1, " ").alias("CDM_LOAD_IN")
)

temp_table_name = "STAGING.DMLoadPCAERunCycUpd_P_RUN_CYC_temp"

execute_dml(f"DROP TABLE IF EXISTS {temp_table_name}", jdbc_url, jdbc_props)

(
    df_BusinessRules_final
    .write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", temp_table_name)
    .mode("append")
    .save()
)

merge_sql = f"""
MERGE {IDSOwner}.P_RUN_CYC AS T
USING {temp_table_name} AS S
ON
    T.SRC_SYS_CD = S.SRC_SYS_CD
    AND T.SUBJ_CD = S.SUBJ_CD
    AND T.TRGT_SYS_CD = S.TRGT_SYS_CD
    AND T.RUN_CYC_NO >= S.RUN_CYC_NO
    AND T.CDM_LOAD_IN = 'N'
WHEN MATCHED THEN
    UPDATE SET
      T.CDM_LOAD_IN = S.CDM_LOAD_IN
WHEN NOT MATCHED THEN
    INSERT
    (
      SRC_SYS_CD,
      SUBJ_CD,
      TRGT_SYS_CD,
      RUN_CYC_NO,
      CDM_LOAD_IN
    )
    VALUES
    (
      S.SRC_SYS_CD,
      S.SUBJ_CD,
      S.TRGT_SYS_CD,
      S.RUN_CYC_NO,
      S.CDM_LOAD_IN
    );
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)