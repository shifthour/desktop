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
# MAGIC JOB NAME:  FctsFmlyAccumDrvrAllExtr
# MAGIC CALLED BY: FctsFmlyAccumExtrSeq
# MAGIC 
# MAGIC                            
# MAGIC PROCESSING: Extract all keys from Facets CDC_CMC_FEAC_ACCUM and load to tempdb driver table.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #               Change Description                             Development Project      Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------     ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Ralph Tucker                  11/11/2009    3556 CDC                        Originally Programmed                                   devlIDSnew                    Steph Goddard             11/19/2009
# MAGIC Jaideep Mankala             2017-08-22      5630                               Modified Field names from FAAC to FATX     IntegrateDev1          Kalyan Neelam            2017-08-30
# MAGIC Prabhu ES                       2022-03-03      S2S Remediation            MSSQL ODBC conn params added              IntegrateDev5	Ken Bradmon	2022-06-02

# MAGIC Facets Family Accum Driver Extract for All Records
# MAGIC This link is referenced by multiple sequencers for the row count
# MAGIC Load driver table with CDC keys
# MAGIC (TMP_PROD_CDC_FA_DRVR$TIMESTAMP)
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import types as T
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
CurrDate = get_widget_value('CurrDate','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
TableTimestamp = get_widget_value('TableTimestamp','31100330')
FacetsOwner = get_widget_value('$FacetsOwner','$PROJDEF')
facets_secret_name = get_widget_value('facets_secret_name','')

jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)

df_CMC_FATX_ACCUM_TXN = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", f"SELECT DISTINCT SBSB_CK FROM {FacetsOwner}.CMC_FATX_ACCUM_TXN")
    .load()
)

df_Trans2 = df_CMC_FATX_ACCUM_TXN.select(F.col("SBSB_CK").alias("SBSB_CK"))

jdbc_url_tempdb, jdbc_props_tempdb = get_db_config(facets_secret_name)
execute_dml("DROP TABLE IF EXISTS tempdb.FctsFmlyAccumDrvrAllExtr_TMP_PROD_CDC_FA_DRVR_temp", jdbc_url_tempdb, jdbc_props_tempdb)

df_Trans2.write.format("jdbc") \
    .option("url", jdbc_url_tempdb) \
    .options(**jdbc_props_tempdb) \
    .option("dbtable", "tempdb.FctsFmlyAccumDrvrAllExtr_TMP_PROD_CDC_FA_DRVR_temp") \
    .mode("overwrite") \
    .save()

execute_dml("TRUNCATE TABLE tempdb..TMP_PROD_CDC_FA_DRVR", jdbc_url_tempdb, jdbc_props_tempdb)
execute_dml("INSERT INTO tempdb..TMP_PROD_CDC_FA_DRVR (SBSB_CK) SELECT SBSB_CK FROM tempdb.FctsFmlyAccumDrvrAllExtr_TMP_PROD_CDC_FA_DRVR_temp", jdbc_url_tempdb, jdbc_props_tempdb)