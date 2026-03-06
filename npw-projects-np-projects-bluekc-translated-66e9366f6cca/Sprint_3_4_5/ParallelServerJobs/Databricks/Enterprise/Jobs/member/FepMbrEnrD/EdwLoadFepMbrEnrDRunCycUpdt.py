# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2013 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by:
# MAGIC                     IdsFepMbrEnrDCntl
# MAGIC 
# MAGIC Processing:
# MAGIC                     Extracts the P_RUN_CYC records for IDS PCMH with the EDW_LOAD_IN = 'N'.  
# MAGIC                     These rows are used to update the P_RUN_CYCLE load indicator field for the particular source and subject.  
# MAGIC                     All run cycles >= to the current run cycle where the indicator is "N" are updated to "Y"
# MAGIC 
# MAGIC Control Job Rerun Information: 
# MAGIC                     Previous Run Successful:    Restart, no other steps necessary
# MAGIC                     Previous Run Aborted:         Restart, no other steps necessary
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                                          Project/                                                                                                                                         Code                   Date
# MAGIC Developer                Date                 Altiris #                    Change Description                                                                                     Reviewer            Reviewed
# MAGIC -------------------------     -------------------        -------------                 -----------------------------------------------------------------------------------------------------------------   -------------------------  -------------------
# MAGIC Santosh Bokka       2013-09-21       5056 FEP                     Initial Programming                                                                                 Kalyan Neelam     2013-10-08

# MAGIC Extract P_RUN_CYC records for IDS FEP_MBR_ENR with a value \"N\" in the  EDW_LOAD_IN where the run cycle is >= to the current run cycle used for extracting.
# MAGIC Set EDW_LOAD_IN to 'Y'
# MAGIC Update the P_RUN_CYC table to indicate source system data has been loaded to the EDW for each of the run cycles processed.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
BeginCycle = get_widget_value('BeginCycle','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = """SELECT 
     SRC_SYS_CD,
     TRGT_SYS_CD,
     SUBJ_CD,
     MIN( RUN_CYC_NO)

FROM 
     #$IDSOwner#.P_RUN_CYC

WHERE 
              TRGT_SYS_CD = 'IDS'
     AND  SUBJ_CD = 'FEP'
     AND  EDW_LOAD_IN = 'N'
AND SRC_SYS_CD = 'BCA'
AND  RUN_CYC_NO >= #BeginCycle#

GROUP BY
     SRC_SYS_CD,
     TRGT_SYS_CD,
     SUBJ_CD

#$IDSOwner#.P_RUN_CYC
"""

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
    col("RUN_CYC_NO").alias("RUN_CYC_NO")
)

drop_sql = "DROP TABLE IF EXISTS STAGING.EdwLoadFepMbrEnrDRunCycUpdt_P_RUN_CYC_temp"
execute_dml(drop_sql, jdbc_url, jdbc_props)

(
    df_BusinessRules.write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", "STAGING.EdwLoadFepMbrEnrDRunCycUpdt_P_RUN_CYC_temp")
    .mode("overwrite")
    .save()
)

merge_sql = """
MERGE INTO #$IDSOwner#.P_RUN_CYC AS T
USING STAGING.EdwLoadFepMbrEnrDRunCycUpdt_P_RUN_CYC_temp AS S
ON T.SRC_SYS_CD = S.SRC_SYS_CD
AND T.SUBJ_CD = S.SUBJ_CD
AND T.TRGT_SYS_CD = S.TRGT_SYS_CD
AND T.RUN_CYC_NO = S.RUN_CYC_NO
WHEN MATCHED AND T.RUN_CYC_NO >= S.RUN_CYC_NO AND T.EDW_LOAD_IN='N'
THEN UPDATE SET T.EDW_LOAD_IN = 'Y'
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)