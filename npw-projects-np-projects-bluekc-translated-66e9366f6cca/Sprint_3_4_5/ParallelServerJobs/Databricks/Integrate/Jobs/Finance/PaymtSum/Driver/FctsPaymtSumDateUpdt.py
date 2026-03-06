# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC Â© Copyright 2008 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  FctsPaymtSumLoadSeq
# MAGIC                
# MAGIC 
# MAGIC PROCESSING:   Update LOAD_DATES Universe table for daily IDS Payment Summary with the most recent audit date in the batch of keys processed.  This date will be used as the begin date for the next days processing.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC 2005-02-01              Brent Leland     3567 Primary Key  Original Programming.                                                                   devlIDScur                     Steph Goddard          02/22/2008
# MAGIC 
# MAGIC Manasa Andru         2014-07-21           TFS - 9548        Changed the condition in the BEGINDATE field                           IntegrateNewDevl          Kalyan Neelam           2014-07-22
# MAGIC                                                                                       to extract the data based on the CurrentDate
# MAGIC                                                                                            and MaxDate comparison.
# MAGIC Prabhu ES               2022-02-24        S2S Remediation  MSSQL connection parameters added                                         IntegrateDev5\(9)Harsha Ravuri\(9)06-10-2022

# MAGIC Extract of max(date) for daily payment summary records processed
# MAGIC Extract max(date) from Facets payment summary driver table.
# MAGIC Update Universe LOAD_DATES table with date
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, when, to_date, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


CntlJobName = get_widget_value('CntlJobName','')
TableTimeStamp = get_widget_value('TableTimeStamp','0808141400')
PrevBeginDate = get_widget_value('PrevBeginDate','2014-02-20')
FacetsOwner = get_widget_value('FacetsOwner','$PROJDEF')
facets_secret_name = get_widget_value('facets_secret_name','')
CurrentDate = get_widget_value('CurrentDate','2014-08-05')
tempdb_secret_name = get_widget_value('tempdb_secret_name','')

jdbc_url, jdbc_props = get_db_config(tempdb_secret_name)
df_TMP_IDS_PMTSUM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", "SELECT max(CKPY_PAY_DT) as CKPY_PAY_DT FROM tempdb..TMP_IDS_PMTSUM")
    .load()
)

begindate_expr = when(col("CKPY_PAY_DT").isNull(), to_date(lit(PrevBeginDate), 'yyyy-MM-dd')).otherwise(
    when(col("CKPY_PAY_DT") < to_date(lit(CurrentDate), 'yyyy-MM-dd'), col("CKPY_PAY_DT")).otherwise(
        to_date(lit(CurrentDate), 'yyyy-MM-dd')
    )
)

df_max_date = (
    df_TMP_IDS_PMTSUM
    .withColumn("JOBNAME", lit(CntlJobName + "BeginDate"))
    .withColumn("BEGINDATE", begindate_expr)
    .withColumn("PREVBEGINDATE", to_date(lit(PrevBeginDate), 'yyyy-MM-dd'))
)

df_final = (
    df_max_date
    .withColumn("JOBNAME", rpad(col("JOBNAME"),  <...>, " "))
    .select("JOBNAME", "BEGINDATE", "PREVBEGINDATE")
)

jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
execute_dml("DROP TABLE IF EXISTS STAGING.FctsPaymtSumDateUpdt_load_dates_temp", jdbc_url_facets, jdbc_props_facets)
(
    df_final.write.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("dbtable", "STAGING.FctsPaymtSumDateUpdt_load_dates_temp")
    .mode("append")
    .save()
)

merge_sql = """
MERGE INTO LOAD_DATES AS T
USING STAGING.FctsPaymtSumDateUpdt_load_dates_temp AS S
    ON T.JOBNAME = S.JOBNAME
WHEN MATCHED THEN
    UPDATE SET
        T.BEGINDATE = S.BEGINDATE,
        T.PREVBEGINDATE = S.PREVBEGINDATE
WHEN NOT MATCHED THEN
    INSERT (JOBNAME, BEGINDATE, PREVBEGINDATE)
    VALUES (S.JOBNAME, S.BEGINDATE, S.PREVBEGINDATE);
"""

execute_dml(merge_sql, jdbc_url_facets, jdbc_props_facets)