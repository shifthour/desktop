# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC COPYRIGHT 2020 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC CALLED BY: OptumMedDDrugInvoiceCntl
# MAGIC 
# MAGIC DESCRIPTION:    Update the P_RUN_CYC table CDM load indicator fields to show those records have been copied.
# MAGIC       
# MAGIC PROCESSING:  Each IDS Claim source has a seperate SQL to extract the maximum run cycle value.  These run cycle values are used to update the IDS P_RUN_CYCLE, load indicator field for the particular source and subject.  All run cycles less than the maximum where the indicator is "N" are updated to "Y".  
# MAGIC 
# MAGIC Drug update only occurs when this job is called by OptumMedDDrugInvoiceCntl
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                                    Date                        Change Description                                      Project #                                                                     Development Project          Code Reviewer               Date Reviewed  
# MAGIC ------------------                          ----------------------------        -----------------------------------------------------------------        ----------------                                                                  ------------------------------------       ----------------------------           -------------
# MAGIC Velmani Kondappan                  2020-10-20                  Initial Development                                    6264 - PBM PHASE II - Government Programs           IntegrateDev2	Abhiram Dasarathy	2020-12-11

# MAGIC Run Cycle Extract Update
# MAGIC Update the P_RUN_CYC table to indicate source system data has been loaded to the Web Data Mart for each of the run cycles processed.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Retrieve parameter values
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
RunCycle = get_widget_value('RunCycle','')

# SYSDUMMY1 (DB2Connector)
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
df_SYSDUMMY1 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", "SELECT IBMREQD FROM sysibm.sysdummy1")
    .load()
)

# Trans1 (CTransformerStage)
df_Trans1 = df_SYSDUMMY1
df_Trans1 = df_Trans1.withColumn("SRC_SYS_CD", F.lit("OPTUMRX"))
df_Trans1 = df_Trans1.withColumn("SUBJ_CD", F.lit("CLAIM"))
df_Trans1 = df_Trans1.withColumn("TRGT_SYS_CD", F.lit("IDS"))
df_Trans1 = df_Trans1.withColumn("RUN_CYC_NO", F.lit(RunCycle))
df_Trans1 = df_Trans1.withColumn("CDM_LOAD_IN", F.lit("Y"))
df_Trans1 = df_Trans1.withColumn("EDW_LOAD_IN", F.lit("Y"))
df_Trans1 = df_Trans1.select(
    F.col("SRC_SYS_CD"),
    F.col("SUBJ_CD"),
    F.col("TRGT_SYS_CD"),
    F.col("RUN_CYC_NO"),
    F.rpad(F.col("CDM_LOAD_IN"), 1, " ").alias("CDM_LOAD_IN"),
    F.rpad(F.col("EDW_LOAD_IN"), 1, " ").alias("EDW_LOAD_IN")
)

# P_RUN_CYC (CODBCStage) - Merge/Upsert
execute_dml(
    "DROP TABLE IF EXISTS STAGING.OPTUMMedDInvoiceRunCycUpd_P_RUN_CYC_temp",
    jdbc_url,
    jdbc_props
)
(
    df_Trans1.write.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", "STAGING.OPTUMMedDInvoiceRunCycUpd_P_RUN_CYC_temp")
    .mode("overwrite")
    .save()
)
merge_sql = f"""
MERGE INTO {IDSOwner}.P_RUN_CYC AS T
USING STAGING.OPTUMMedDInvoiceRunCycUpd_P_RUN_CYC_temp AS S
ON 
    T.SRC_SYS_CD = S.SRC_SYS_CD
    AND T.SUBJ_CD = S.SUBJ_CD
    AND T.TRGT_SYS_CD = S.TRGT_SYS_CD
    AND T.RUN_CYC_NO <= S.RUN_CYC_NO
    AND T.CDM_LOAD_IN = 'N'
WHEN MATCHED THEN
    UPDATE
        SET T.CDM_LOAD_IN = S.CDM_LOAD_IN,
            T.EDW_LOAD_IN = S.EDW_LOAD_IN
WHEN NOT MATCHED THEN
    INSERT (SRC_SYS_CD, SUBJ_CD, TRGT_SYS_CD, RUN_CYC_NO, CDM_LOAD_IN, EDW_LOAD_IN)
    VALUES (S.SRC_SYS_CD, S.SUBJ_CD, S.TRGT_SYS_CD, S.RUN_CYC_NO, S.CDM_LOAD_IN, S.EDW_LOAD_IN);
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)