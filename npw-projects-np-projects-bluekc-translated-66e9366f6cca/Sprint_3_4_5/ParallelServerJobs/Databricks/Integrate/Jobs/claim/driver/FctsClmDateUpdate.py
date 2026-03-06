# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2006 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  FctsClmDateUpdate
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:   Update LOAD_DATES Universe table for daily IDS claims with the most recent audit date in the batch of claims processed.  This date will be used as the begin date for the next days processing.
# MAGIC 
# MAGIC 
# MAGIC CALLED BY: ClmPreReqJc
# MAGIC 
# MAGIC 
# MAGIC INPUTS: See "Parameters" tab
# MAGIC                 tempbd..TMP_IDS_CLAIM<timestamp>
# MAGIC                
# MAGIC 
# MAGIC EMAIL FILES:  None
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:  Select max date from current day's Facets claim driver table and update the LOAD_DATES table with that date for the correct entry.  Also writes dates to balancing table IAD_CLM_DM_BAL.
# MAGIC 
# MAGIC 
# MAGIC UNIX SCRIPTS USED:  None
# MAGIC 
# MAGIC 
# MAGIC DATABASE TABLES USED:
# MAGIC                  Database                    Table Name
# MAGIC                  ----------------------------       ---------------------------------------------------------------------
# MAGIC                  tempdb..                     TMP_IDS_CLAIM<timestamp>
# MAGIC                  BCBS_repl                  IAD_CLM_DM_BAL
# MAGIC 
# MAGIC BCBSKC TRANSFORMS/ROUTINES USED: none
# MAGIC 
# MAGIC 
# MAGIC HASH FILES: none
# MAGIC 
# MAGIC 
# MAGIC OUTPUTS:  
# MAGIC                 Universal table LOAD_DATES
# MAGIC 
# MAGIC 
# MAGIC RERUN INFORMATION: 
# MAGIC              PREVIOUS RUN SUCCESSFUL:   Reset the UV LoadDates entry "DidYouReadThis" to the proper end-of-quarter date. 
# MAGIC              PREVIOUS RUN ABORTED:         Verify the LoadDates entry and restart.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Date                 Developer                Change Description
# MAGIC ------------------      ----------------------------     --------------------------------------------------------------------------------------------------------
# MAGIC 2005-02-01      Brent Leland             Original Programming.
# MAGIC 
# MAGIC 2005-05-26      Sharon Andrew        Copied FctsClmdateUpdateOrg to this. 
# MAGIC                                                          Removed BAL parameters.
# MAGIC                                                         Added new load step to the LOAD_DATE table that is to be used by Claim Mart's end date.  new entry =   IdsClaimExtrJcRunTime
# MAGIC                                                          Changed parameter LkupValue to "JobNm_IdsClmExtrJcBegin"
# MAGIC                                                         Added      parameter                        "JobNm_IdsClmExtrJcRunTm"
# MAGIC 2006-04-13     Brent Leland             Removed update of "JobNm_IdsClmExtrJcRunTm" since Claim mart will pull based on the P_RUN_CYC table.
# MAGIC 2022-02-28     Prabhu ES                 S2S Remediation  - MSSQL connection parameters added in the project IntegrateDev5     Manasa Andru         2022-06-08
# MAGIC 
# MAGIC 2023-10-20     Brent Leland             Changed LOAD_DATES end date from max(driver) to the control job end date so no gap in time added to end date.

# MAGIC Extract of max(date) for daily claims processed
# MAGIC Extract max(date) from Facets claim driver table.
# MAGIC To be removed when Claim Mart starts using the P_RUN_CYC table for extracting
# MAGIC Update Universe LOAD_DATES table with date
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


CntlJobName = get_widget_value('CntlJobName','')
DriverTable = get_widget_value('DriverTable','')
BeginDate = get_widget_value('BeginDate','')
EndDate = get_widget_value('EndDate','')
$FacetsOwner = get_widget_value('$FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
tempdb_secret_name = get_widget_value('tempdb_secret_name','')

jdbc_url, jdbc_props = get_db_config(tempdb_secret_name)
extract_query = f"SELECT MAX(CLCL_LAST_ACT_DTM) CLCL_LAST_ACT_DTM FROM tempdb..{DriverTable}"
df_tmp_claim = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_update_max_date = df_tmp_claim.select(
    F.lit(f"{CntlJobName}BeginDate").alias("JOBNAME"),
    F.lit(EndDate).alias("BEGINDATE"),
    F.lit(BeginDate).alias("PREVBEGINDATE")
)

df_update_run_time = df_tmp_claim.select(
    F.lit("IdsClaimExtrJcRunTime").alias("JOBNAME"),
    F.lit(EndDate).alias("BEGINDATE"),
    F.lit(BeginDate).alias("PREVBEGINDATE")
)

drop_sql = "DROP TABLE IF EXISTS tempdb.FctsClmDateUpdate_load_dates_temp"
execute_dml(drop_sql, jdbc_url, jdbc_props)

df_update_max_date.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "tempdb.FctsClmDateUpdate_load_dates_temp") \
    .mode("append") \
    .save()

merge_sql = '''
MERGE LOAD_DATES as T
USING tempdb.FctsClmDateUpdate_load_dates_temp as S
ON T.JOBNAME = S.JOBNAME
WHEN MATCHED THEN UPDATE SET
  T.BEGINDATE = S.BEGINDATE,
  T.PREVBEGINDATE = S.PREVBEGINDATE
WHEN NOT MATCHED THEN
  INSERT (JOBNAME, BEGINDATE, PREVBEGINDATE)
  VALUES (S.JOBNAME, S.BEGINDATE, S.PREVBEGINDATE);
'''
execute_dml(merge_sql, jdbc_url, jdbc_props)

drop_sql = "DROP TABLE IF EXISTS tempdb.FctsClmDateUpdate_Copy_of_load_dates_temp"
execute_dml(drop_sql, jdbc_url, jdbc_props)

df_update_run_time.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "tempdb.FctsClmDateUpdate_Copy_of_load_dates_temp") \
    .mode("append") \
    .save()

merge_sql = '''
MERGE LOAD_DATES as T
USING tempdb.FctsClmDateUpdate_Copy_of_load_dates_temp as S
ON T.JOBNAME = S.JOBNAME
WHEN MATCHED THEN UPDATE SET
  T.BEGINDATE = S.BEGINDATE,
  T.PREVBEGINDATE = S.PREVBEGINDATE
WHEN NOT MATCHED THEN
  INSERT (JOBNAME, BEGINDATE, PREVBEGINDATE)
  VALUES (S.JOBNAME, S.BEGINDATE, S.PREVBEGINDATE);
'''
execute_dml(merge_sql, jdbc_url, jdbc_props)