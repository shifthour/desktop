# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This pulls the maximum CSTK_LAST_UPD_DTM date to update the LOAD_DATES table.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                       Date               Project/Altiris #               Change Description                                            Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------     -------------------   -----------------------------------    ---------------------------------------------------------                 ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Ralph Tucker                 12/20/2007   15                                   Originally Programmed                                       devlIDS30                     Steph Goddard              01/09/2008
# MAGIC Brent Leland                   03/17/2008   3567 Primary Key           Changed update parameter name from              devlIDScur                    Steph Goddard              05/06/2008
# MAGIC                                                                                                     EndDate to BeginDate   devlIDScur                    
# MAGIC Prabhu ES                      2022-03-01      S2S Remediation     MSSQL connection parameters added                  IntegrateDev5                Kalyan Neelam               2022-06-08

# MAGIC Extract of max(date) for daily Customer Service processing
# MAGIC Extract max(date) from Facets claim driver table.
# MAGIC Update Universe LOAD_DATES table with date
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


CntlJobName = get_widget_value('CntlJobName', 'FctsCustSvcCntl')
BeginDate = get_widget_value('BeginDate', '')
$FacetsOwner = get_widget_value('$FacetsOwner', '$PROJDEF')
facets_secret_name = get_widget_value('facets_secret_name', '')

jdbc_url, jdbc_props = get_db_config(<...>)
df_tmp_claim = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", "Select max(CSTK_LAST_UPD_DTM) as CSTK_LAST_UPD_DTM FROM tempdb..TMP_IDS_CUST_SVC_DRVR")
    .load()
)

df_update_max_date = df_tmp_claim.select(
    F.concat(F.lit(CntlJobName), F.lit("BeginDate")).alias("JOBNAME"),
    F.col("CSTK_LAST_UPD_DTM").alias("BEGINDATE"),
    F.lit(BeginDate).alias("PREVBEGINDATE")
).select("JOBNAME", "BEGINDATE", "PREVBEGINDATE")

execute_dml("DROP TABLE IF EXISTS tempdb.FctsCustSvcDateUpdt_load_dates_temp", jdbc_url, jdbc_props)

df_update_max_date.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "tempdb.FctsCustSvcDateUpdt_load_dates_temp") \
    .mode("append") \
    .save()

merge_sql = """
MERGE tempdb.LOAD_DATES as T
USING tempdb.FctsCustSvcDateUpdt_load_dates_temp as S
ON T.JOBNAME = S.JOBNAME
WHEN MATCHED THEN
    UPDATE SET
        T.BEGINDATE = S.BEGINDATE,
        T.PREVBEGINDATE = S.PREVBEGINDATE
WHEN NOT MATCHED THEN
    INSERT (JOBNAME, BEGINDATE, PREVBEGINDATE)
    VALUES(S.JOBNAME, S.BEGINDATE, S.PREVBEGINDATE);
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)