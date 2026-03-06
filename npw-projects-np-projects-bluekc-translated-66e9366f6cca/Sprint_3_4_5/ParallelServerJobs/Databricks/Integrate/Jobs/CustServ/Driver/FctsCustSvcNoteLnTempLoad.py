# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007, 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  FctsCustSvcPrerecSeq
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:  Load tempdb driver table for customer service note line extract.  Facets source table is joined with regular customer service driver table
# MAGIC                           to get changed data.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date               Project/Altiris #               Change Description                              Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------       ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               2/8/2007          3028                          Originally Programmed                             devlIDS30                     Steph Goddard              02/21/2007
# MAGIC Ralph Tucker                   12/3/2007       15                                Using tempdb instead of begin and         devlIDS                         Steph Goddard              01/09/2008
# MAGIC                                                                                                      end dates for selection.             
# MAGIC Ralph Tucker                   1/15/2008       15                                Changed driver table name                     devlIDS
# MAGIC Brent Leland                    02-9-2008       3567 Primary Key           Removed table time stamp naming         devlIDScur                    Steph Goddard              05/06/2008        
# MAGIC Brent Leland                    07-07-2010       Prod Support               Added cast to conf.CSCF_TEXT column 
# MAGIC                                                                                                      due to Sybase varbinary data type  
# MAGIC Prabhu ES                        2022-03-01      S2S Remediation         MSSQL connection parameters added  IntegrateDev5                 Kalyan Neelam              2022-06-08

# MAGIC Extract Customer Service Task Note Line changed keys and load driver data
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, locate, substring, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------

FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
jdbc_url, jdbc_props = get_db_config(facets_secret_name)

extract_query = f"""
SELECT 
conf.CSSC_ID AS CSSC_ID,
conf.CSTK_SEQ_NO AS CSTK_SEQ_NO,
conf.CSCF_SEQ_NO AS CSCF_SEQ_NO,
cast(Trim(conf.CSCF_TEXT) as varchar(255)) AS CSCF_TEXT
FROM {FacetsOwner}.CMC_CSTK_TASK cstk,
{FacetsOwner}.CMC_CSCF_CONF conf,
tempdb..TMP_IDS_CUST_SVC_DRVR DRVR
WHERE cstk.CSSC_ID = DRVR.CSSC_ID
  AND cstk.CSTK_SEQ_NO = DRVR.CSTK_SEQ_NO
  AND cstk.CSSC_ID = conf.CSSC_ID
  AND cstk.CSTK_SEQ_NO = conf.CSTK_SEQ_NO
  AND conf.CSCF_TYPE='1'
"""

df_FACETS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_Trans1 = (
    df_FACETS
    .withColumn(
        "STRT_SEQ_NO",
        substring(
            col("CSCF_TEXT"),
            locate("<CSTN_CSCF_SEQ_S>", col("CSCF_TEXT")) + 17,
            locate("</CSTN_CSCF_SEQ_S>", col("CSCF_TEXT")) - (locate("<CSTN_CSCF_SEQ_S>", col("CSCF_TEXT")) + 17)
        )
    )
    .withColumn(
        "END_SEQ_NO",
        substring(
            col("CSCF_TEXT"),
            locate("<CSTN_CSCF_SEQ_E>", col("CSCF_TEXT")) + 17,
            locate("</CSTN_CSCF_SEQ_E>", col("CSCF_TEXT")) - (locate("<CSTN_CSCF_SEQ_E>", col("CSCF_TEXT")) + 17)
        )
    )
)

df_TMP_CUST_SVC_TASK_NOTE_LN_in = (
    df_Trans1
    .select(
        rpad(col("CSSC_ID"), 20, " ").alias("CSSC_ID"),
        col("CSTK_SEQ_NO").alias("CSTK_SEQ_NO"),
        col("CSCF_SEQ_NO").alias("CSCF_SEQ_NO"),
        col("STRT_SEQ_NO").alias("STRT_SEQ_NO"),
        col("END_SEQ_NO").alias("END_SEQ_NO"),
        rpad(col("CSCF_TEXT"), 255, " ").alias("CSCF_TEXT")
    )
)

drop_temp_table_sql = "DROP TABLE IF EXISTS tempdb.FctsCustSvcNoteLnTempLoad_TMP_CUST_SVC_TASK_NOTE_LN_temp"
execute_dml(drop_temp_table_sql, jdbc_url, jdbc_props)

(
    df_TMP_CUST_SVC_TASK_NOTE_LN_in
    .write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", "tempdb.FctsCustSvcNoteLnTempLoad_TMP_CUST_SVC_TASK_NOTE_LN_temp")
    .mode("overwrite")
    .save()
)

merge_sql = """
MERGE tempdb..TMP_CUST_SVC_TASK_NOTE_LN AS T
USING tempdb.FctsCustSvcNoteLnTempLoad_TMP_CUST_SVC_TASK_NOTE_LN_temp AS S
ON (T.CSSC_ID = S.CSSC_ID AND T.CSTK_SEQ_NO = S.CSTK_SEQ_NO AND T.CSCF_SEQ_NO = S.CSCF_SEQ_NO)
WHEN MATCHED THEN
  UPDATE SET
    T.STRT_SEQ_NO = S.STRT_SEQ_NO,
    T.END_SEQ_NO = S.END_SEQ_NO,
    T.CSCF_TEXT = S.CSCF_TEXT
WHEN NOT MATCHED THEN
  INSERT (CSSC_ID, CSTK_SEQ_NO, CSCF_SEQ_NO, STRT_SEQ_NO, END_SEQ_NO, CSCF_TEXT)
  VALUES (S.CSSC_ID, S.CSTK_SEQ_NO, S.CSCF_SEQ_NO, S.STRT_SEQ_NO, S.END_SEQ_NO, S.CSCF_TEXT);
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)