# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 12/02/09 12:38:39 Batch  15312_45563 INIT bckcett:31540 devlIDSnew u150906 3556-MbrCDC_Ralph_devlIDSnew       Maddy
# MAGIC 
# MAGIC  
# MAGIC COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:  FctsMbrAccumDrvrAllExtr
# MAGIC CALLED BY: FctsMbrAccumExtrSeq
# MAGIC 
# MAGIC                            
# MAGIC PROCESSING: Extract all keys from Facets CDC_CMC_MEAC_ACCUM and load to tempdb driver table.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #               Change Description                             	Development Project      Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------     ---------------------------------------------------------      	----------------------------------   ---------------------------------    -------------------------   
# MAGIC Ralph Tucker                  11/11/2009    3556 CDC                        Originally Programmed                            	devlIDSnew                   Steph Goddard             11/19/2009
# MAGIC Jaideep Mankala              2017-08-22     5630                             Modified column names from MEAC to MATX	IntegrateDev1               Kalyan Neelam             2017-08-30
# MAGIC Prabhu ES                        2022-03-03     S2S Remediation          MSSQL ODBC conn params added                   IntegrateDev5		Ken Bradmon	2022-06-02

# MAGIC Facets Member Accum Driver Extract for All Records
# MAGIC This link is referenced by multiple sequencers for the row count
# MAGIC Load driver table with CDC keys
# MAGIC (TMP_PROD_CDC_MA_DRVR$TIMESTAMP)
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import SparkSession
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
CurrDate = get_widget_value('CurrDate','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
TableTimestamp = get_widget_value('TableTimestamp','')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')

jdbc_url_1, jdbc_props_1 = get_db_config(facets_secret_name)
extract_query = f"SELECT DISTINCT MEME_CK FROM {FacetsOwner}.CMC_MATX_ACCUM_TXN"
df_CMC_MATX_ACCUM_TXN = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_1)
    .options(**jdbc_props_1)
    .option("query", extract_query)
    .load()
)

df_Trans2 = df_CMC_MATX_ACCUM_TXN.select("MEME_CK")

jdbc_url_2, jdbc_props_2 = get_db_config(facets_secret_name)
temp_table_name = "tempdb.FctsMbrAccumDrvrAllExtr_TMP_PROD_CDC_MA_DRVR_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_name}", jdbc_url_2, jdbc_props_2)

df_Trans2.select("MEME_CK").write.format("jdbc") \
    .option("url", jdbc_url_2) \
    .options(**jdbc_props_2) \
    .option("dbtable", temp_table_name) \
    .mode("overwrite") \
    .save()

merge_sql = """
MERGE INTO tempdb.TMP_PROD_CDC_MA_DRVR AS T
USING tempdb.FctsMbrAccumDrvrAllExtr_TMP_PROD_CDC_MA_DRVR_temp AS S
ON T.MEME_CK = S.MEME_CK
WHEN MATCHED THEN
  UPDATE SET T.MEME_CK = S.MEME_CK
WHEN NOT MATCHED THEN
  INSERT (MEME_CK) VALUES (S.MEME_CK);
"""
execute_dml(merge_sql, jdbc_url_2, jdbc_props_2)