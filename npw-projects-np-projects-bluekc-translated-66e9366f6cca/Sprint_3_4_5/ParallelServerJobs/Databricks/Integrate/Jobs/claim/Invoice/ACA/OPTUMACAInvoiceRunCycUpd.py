# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_2 03/10/09 09:25:21 Batch  15045_33943 INIT bckcetl ids20 dcg01 sa Bringing ALL Claim code down to devlIDS
# MAGIC ^1_2 02/10/09 11:16:45 Batch  15017_40611 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/12/08 13:19:45 Batch  14927_47989 PROMOTE bckcetl ids20 dsadm bls for sa/sg
# MAGIC ^1_1 11/12/08 13:12:55 Batch  14927_47581 INIT bckcett testIDSnew dsadm bls for sa/sg
# MAGIC ^1_1 11/10/08 15:58:46 Batch  14925_57530 PROMOTE bckcett testIDSnew u03651 steph for Sharon
# MAGIC ^1_1 11/10/08 15:53:35 Batch  14925_57218 INIT bckcett devlIDSnew u03651 steffy
# MAGIC 
# MAGIC COPYRIGHT 2020 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC DESCRIPTION:    Update the P_RUN_CYC table CDM load indicator fields to show those records have been copied to the Claim Data Mart.
# MAGIC       
# MAGIC PROCESSING:  Each IDS Claim source has a seperate SQL to extract the maximum run cycle value.  These run cycle values are used to update the IDS P_RUN_CYCLE, load indicator field for the particular source and subject.  All run cycles less than the maximum where the indicator is "N" are updated to "Y".  
# MAGIC 
# MAGIC Drug update only occurs when this job is called by ESI processing (IdsWebDMDrugLoadSeq).
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Date                 Developer                Change Description
# MAGIC ------------------      ----------------------------     --------------------------------------------------------------------------------------------------------
# MAGIC 2020-11-01   Velmani Kondappan           Original programming                                      Reddy Sanam  12/11/2020

# MAGIC Run Cycle Extract Update Called from OptumACADrugInvoiceCntl
# MAGIC Update the P_RUN_CYC table to indicate source system data has been loaded to the Web Data Mart for each of the run cycles processed.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
RunCycle = get_widget_value('RunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

df_SYSDUMMY1 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", "SELECT IBMREQD  FROM sysibm.sysdummy1")
    .load()
)

df_Trans1 = df_SYSDUMMY1.select(
    F.lit("OPTUMRX").alias("SRC_SYS_CD"),
    F.lit("CLAIM").alias("SUBJ_CD"),
    F.lit("IDS").alias("TRGT_SYS_CD"),
    F.lit(RunCycle).alias("RUN_CYC_NO"),
    F.lit("Y").alias("CDM_LOAD_IN"),
    F.lit("Y").alias("EDW_LOAD_IN")
)

df_Trans1 = df_Trans1.withColumn("CDM_LOAD_IN", F.rpad(F.col("CDM_LOAD_IN"), 1, " ")) \
    .withColumn("EDW_LOAD_IN", F.rpad(F.col("EDW_LOAD_IN"), 1, " "))

df_Trans1 = df_Trans1.select(
    "SRC_SYS_CD",
    "SUBJ_CD",
    "TRGT_SYS_CD",
    "RUN_CYC_NO",
    "CDM_LOAD_IN",
    "EDW_LOAD_IN"
)

temp_table = "STAGING.OPTUMACAInvoiceRunCycUpd_P_RUN_CYC_temp"

execute_dml(f"DROP TABLE IF EXISTS {temp_table}", jdbc_url, jdbc_props)

df_Trans1.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", temp_table) \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE {IDSOwner}.P_RUN_CYC AS T
USING {temp_table} AS S
ON T.SRC_SYS_CD = S.SRC_SYS_CD
   AND T.SUBJ_CD = S.SUBJ_CD
   AND T.TRGT_SYS_CD = S.TRGT_SYS_CD
   AND T.RUN_CYC_NO <= S.RUN_CYC_NO
   AND T.CDM_LOAD_IN = 'N'
WHEN MATCHED THEN
  UPDATE SET T.CDM_LOAD_IN = S.CDM_LOAD_IN,
             T.EDW_LOAD_IN = S.EDW_LOAD_IN
WHEN NOT MATCHED THEN
  INSERT (SRC_SYS_CD, SUBJ_CD, TRGT_SYS_CD, RUN_CYC_NO, CDM_LOAD_IN, EDW_LOAD_IN)
  VALUES (S.SRC_SYS_CD, S.SUBJ_CD, S.TRGT_SYS_CD, S.RUN_CYC_NO, S.CDM_LOAD_IN, S.EDW_LOAD_IN);
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)