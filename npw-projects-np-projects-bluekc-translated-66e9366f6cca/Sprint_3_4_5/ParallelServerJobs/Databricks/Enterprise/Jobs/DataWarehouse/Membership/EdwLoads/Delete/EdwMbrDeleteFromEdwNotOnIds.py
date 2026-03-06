# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2006 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:  IdsMbrDeleteFromIdsNotOnFacets
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  Using the  X_RCRD_DEL table as a driver table delete claims no longer in IDS that are no longer in the source system.
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:   xxxSeq
# MAGIC 
# MAGIC 
# MAGIC INPUTS: See "Parameters" tab
# MAGIC               UV LoadDates Entry:  None
# MAGIC               UV RunDate Entry:     None
# MAGIC                
# MAGIC 
# MAGIC EMAIL FILES:  None
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:   After data is loaded delete claim records matching the driver table where the run cycle update is older than the current run.
# MAGIC 
# MAGIC 
# MAGIC UNIX SCRIPTS USED:  none
# MAGIC 
# MAGIC 
# MAGIC DATABASE TABLES USED:
# MAGIC                  Database                    Table Name
# MAGIC                  ----------------------------       ---------------------------------------------------------------------
# MAGIC                  IDS_PROD                CLM_LN
# MAGIC                  IDS_PROD                CLM_DIAG
# MAGIC                  IDS_PROD                CLM_PROV
# MAGIC                  IDS_PROD                CLM_COB
# MAGIC                  IDS_PROD                FCLTY_CLM_PROC
# MAGIC 
# MAGIC 
# MAGIC BCBSKC TRANSFORMS/ROUTINES USED:    None
# MAGIC 
# MAGIC 
# MAGIC HASH FILES:  None
# MAGIC 
# MAGIC 
# MAGIC OUTPUTS:   None
# MAGIC 
# MAGIC 
# MAGIC RERUN INFORMATION: 
# MAGIC              PREVIOUS RUN SUCCESSFUL:   Nothing to reset just rerun. 
# MAGIC              PREVIOUS RUN ABORTED:         Nothing to reset just rerun.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Brent Leland             04-06-2005                                               Initial programming.
# MAGIC 
# MAGIC Srikanth Mettpalli      07/24/2013        5114                              Original Programming                                                                             EnterpriseWrhsDevl       
# MAGIC 
# MAGIC Krishnakanth            01-17-2018          60037                            Added logic to delete records                                                                EnterpriseDevl2           Kalyan Neelam          2018-02-02
# MAGIC     Manivannan                                                                           from the EDW table MBR_LFSTYL_BNF_D

# MAGIC Job Name: EdwMbrDeleteFromEdwNotOnIds
# MAGIC EDW Member Delete from EDW because No Longer On IDS.
# MAGIC ** Deletes records that are no longer in the source system.
# MAGIC **  W_MBR_DEL has two (2) types of key information:  Member Uniq Key or Subscriber Uniq Key.
# MAGIC **  EDW MEmbership tables that contain MBR_UNIQ_KEY or SUB_UNIQ_KEY as part of the natural keys are subjected to this job.
# MAGIC **  This job should run after all of the loads are completed successfully.
# MAGIC ** Every table in this job is also subject to the MonthEnd and DBLoadAction parameters.   If the Extract/Load steps is looking the MonthEnd/DBLoadAction to determine if doing an update or replace, then it needs to be included in this job.
# MAGIC ** All of these tables are keyed by MBR_UNIQ_KEY
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
EDWRunCycle = get_widget_value('EDWRunCycle','')

jdbc_url, jdbc_props = get_db_config(edw_secret_name)

extract_query_db2_MBR_COB_D_out = f"""SELECT MBR_COB_SK
FROM {EDWOwner}.MBR_COB_D edwTbl,
     {EDWOwner}.W_MBR_DEL del
WHERE del.SUBJ_NM = 'MBR_UNIQ_KEY'
  AND del.KEY_VAL_INT = edwTbl.MBR_UNIQ_KEY
  AND del.SRC_SYS_CD = edwTbl.SRC_SYS_CD
  AND edwTbl.LAST_UPDT_RUN_CYC_EXCTN_SK < {EDWRunCycle}
"""
df_db2_MBR_COB_D_out = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_MBR_COB_D_out)
    .load()
)
execute_dml("DROP TABLE IF EXISTS STAGING.EdwMbrDeleteFromEdwNotOnIds_db2_MBR_COB_D_del_temp", jdbc_url, jdbc_props)
df_db2_MBR_COB_D_out.select("MBR_COB_SK").write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.EdwMbrDeleteFromEdwNotOnIds_db2_MBR_COB_D_del_temp") \
    .mode("overwrite") \
    .save()
merge_sql_db2_MBR_COB_D_del = f"""
MERGE INTO {EDWOwner}.MBR_COB_D AS T
USING STAGING.EdwMbrDeleteFromEdwNotOnIds_db2_MBR_COB_D_del_temp AS S
ON T.MBR_COB_SK = S.MBR_COB_SK
WHEN MATCHED THEN DELETE;
"""
execute_dml(merge_sql_db2_MBR_COB_D_del, jdbc_url, jdbc_props)

extract_query_db2_MBR_ENR_D_out = f"""SELECT MBR_ENR_SK
FROM {EDWOwner}.MBR_ENR_D edwTbl,
     {EDWOwner}.W_MBR_DEL del
WHERE del.SUBJ_NM = 'MBR_UNIQ_KEY'
  AND del.KEY_VAL_INT = edwTbl.MBR_UNIQ_KEY
  AND del.SRC_SYS_CD = edwTbl.SRC_SYS_CD
  AND edwTbl.LAST_UPDT_RUN_CYC_EXCTN_SK < {EDWRunCycle}
"""
df_db2_MBR_ENR_D_out = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_MBR_ENR_D_out)
    .load()
)
execute_dml("DROP TABLE IF EXISTS STAGING.EdwMbrDeleteFromEdwNotOnIds_db2_MBR_ENR_D_del_temp", jdbc_url, jdbc_props)
df_db2_MBR_ENR_D_out.select("MBR_ENR_SK").write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.EdwMbrDeleteFromEdwNotOnIds_db2_MBR_ENR_D_del_temp") \
    .mode("overwrite") \
    .save()
merge_sql_db2_MBR_ENR_D_del = f"""
MERGE INTO {EDWOwner}.MBR_ENR_D AS T
USING STAGING.EdwMbrDeleteFromEdwNotOnIds_db2_MBR_ENR_D_del_temp AS S
ON T.MBR_ENR_SK = S.MBR_ENR_SK
WHEN MATCHED THEN DELETE;
"""
execute_dml(merge_sql_db2_MBR_ENR_D_del, jdbc_url, jdbc_props)

extract_query_db2_MBR_PCP_D_out = f"""SELECT MBR_PCP_SK
FROM {EDWOwner}.MBR_PCP_D edwTbl,
     {EDWOwner}.W_MBR_DEL del
WHERE del.SUBJ_NM = 'MBR_UNIQ_KEY'
  AND del.KEY_VAL_INT = edwTbl.MBR_UNIQ_KEY
  AND del.SRC_SYS_CD = edwTbl.SRC_SYS_CD
  AND edwTbl.LAST_UPDT_RUN_CYC_EXCTN_SK < {EDWRunCycle}
"""
df_db2_MBR_PCP_D_out = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_MBR_PCP_D_out)
    .load()
)
execute_dml("DROP TABLE IF EXISTS STAGING.EdwMbrDeleteFromEdwNotOnIds_db2_MBR_PCP_D_del_temp", jdbc_url, jdbc_props)
df_db2_MBR_PCP_D_out.select("MBR_PCP_SK").write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.EdwMbrDeleteFromEdwNotOnIds_db2_MBR_PCP_D_del_temp") \
    .mode("overwrite") \
    .save()
merge_sql_db2_MBR_PCP_D_del = f"""
MERGE INTO {EDWOwner}.MBR_PCP_D AS T
USING STAGING.EdwMbrDeleteFromEdwNotOnIds_db2_MBR_PCP_D_del_temp AS S
ON T.MBR_PCP_SK = S.MBR_PCP_SK
WHEN MATCHED THEN DELETE;
"""
execute_dml(merge_sql_db2_MBR_PCP_D_del, jdbc_url, jdbc_props)

extract_query_db2_MBR_MCARE_EVT_D_out = f"""SELECT MBR_MCARE_EVT_SK
FROM {EDWOwner}.MBR_MCARE_EVT_D edwTbl,
     {EDWOwner}.W_MBR_DEL del
WHERE del.SUBJ_NM = 'MBR_UNIQ_KEY'
  AND del.KEY_VAL_INT = edwTbl.MBR_UNIQ_KEY
  AND del.SRC_SYS_CD = edwTbl.SRC_SYS_CD
  AND edwTbl.LAST_UPDT_RUN_CYC_EXCTN_SK < {EDWRunCycle}
"""
df_db2_MBR_MCARE_EVT_D_out = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_MBR_MCARE_EVT_D_out)
    .load()
)
execute_dml("DROP TABLE IF EXISTS STAGING.EdwMbrDeleteFromEdwNotOnIds_db2_MBR_MCARE_EVT_D_del_temp", jdbc_url, jdbc_props)
df_db2_MBR_MCARE_EVT_D_out.select("MBR_MCARE_EVT_SK").write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.EdwMbrDeleteFromEdwNotOnIds_db2_MBR_MCARE_EVT_D_del_temp") \
    .mode("overwrite") \
    .save()
merge_sql_db2_MBR_MCARE_EVT_D_del = f"""
MERGE INTO {EDWOwner}.MBR_MCARE_EVT_D AS T
USING STAGING.EdwMbrDeleteFromEdwNotOnIds_db2_MBR_MCARE_EVT_D_del_temp AS S
ON T.MBR_MCARE_EVT_SK = S.MBR_MCARE_EVT_SK
WHEN MATCHED THEN DELETE;
"""
execute_dml(merge_sql_db2_MBR_MCARE_EVT_D_del, jdbc_url, jdbc_props)

extract_query_db2_Select_MBR_LIFESTYLE_BNF_D_out = f"""SELECT MBR_LFSTYL_BNF_SK
FROM {EDWOwner}.MBR_LFSTYL_BNF_D edwTbl,
     {EDWOwner}.W_MBR_DEL del
WHERE del.SUBJ_NM = 'MBR_UNIQ_KEY'
  AND del.KEY_VAL_INT = edwTbl.MBR_UNIQ_KEY
  AND del.SRC_SYS_CD = edwTbl.SRC_SYS_CD
  AND edwTbl.LAST_UPDT_RUN_CYC_EXCTN_SK < {EDWRunCycle}
"""
df_db2_Select_MBR_LIFESTYLE_BNF_D_out = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_Select_MBR_LIFESTYLE_BNF_D_out)
    .load()
)
execute_dml("DROP TABLE IF EXISTS STAGING.EdwMbrDeleteFromEdwNotOnIds_Select_MBR_LIFESTYLE_BNF_D_del_temp", jdbc_url, jdbc_props)
df_db2_Select_MBR_LIFESTYLE_BNF_D_out.select("MBR_LFSTYL_BNF_SK").write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.EdwMbrDeleteFromEdwNotOnIds_Select_MBR_LIFESTYLE_BNF_D_del_temp") \
    .mode("overwrite") \
    .save()
merge_sql_Select_MBR_LIFESTYLE_BNF_D_del = f"""
MERGE INTO {EDWOwner}.MBR_LFSTYL_BNF_D AS T
USING STAGING.EdwMbrDeleteFromEdwNotOnIds_Select_MBR_LIFESTYLE_BNF_D_del_temp AS S
ON T.MBR_LFSTYL_BNF_SK = S.MBR_LFSTYL_BNF_SK
WHEN MATCHED THEN DELETE;
"""
execute_dml(merge_sql_Select_MBR_LIFESTYLE_BNF_D_del, jdbc_url, jdbc_props)