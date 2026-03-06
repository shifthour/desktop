# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:  FctsPrvcyExtrnlEntyDrvrAllExtr
# MAGIC CALLED BY: IdsPrvcyExtrSeq
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #               Change Description                             Development Project      Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------     ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Steph Goddard                  11/24/2009      3556                                Originally Programmed                            devlIDSnew                Brent Leland               12/29/2009   
# MAGIC 
# MAGIC Anoop Nair                        2022-03-11         S2S Remediation      Added FACETS DSN Connection parameters      IntegrateDev5
# MAGIC Vikas Abbu                        2022-05-25         S2S Remediation      Updated target delete and Insert to Insert and Truncate      IntegrateDev5	Ken Bradmon	2022-06-03

# MAGIC Facets Privacy Extrnl Enty Driver Extract for All Records
# MAGIC This link is referenced by multiple sequencers for the row count
# MAGIC Load driver table with CDC keys
# MAGIC (TMP_PRVCY_CDC_EE_DRVR$TIMESTAMP)
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


TableTimestamp = get_widget_value('TableTimestamp','')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')

jdbc_url, jdbc_props = get_db_config(facets_secret_name)

extract_query = f"SELECT \n   ENEN_CKE     \n\nFROM  \n     {FacetsOwner}.FHD_ENEN_ENTITY_D"
df_FacetsPrvcyExtrnlEnty = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_xfmDrvrUpd = df_FacetsPrvcyExtrnlEnty.select(
    col("ENEN_CKE").alias("ENEN_CKE"),
    lit("I").alias("OP")
)

df_xfmDrvrUpd = df_xfmDrvrUpd.select(
    col("ENEN_CKE"),
    rpad(col("OP"), 1, " ").alias("OP")
)

execute_dml("DROP TABLE IF EXISTS tempdb.FctsPrvcyExtrnlEntyDrvrAllExtr_TMP_PRVCY_CDC_EE_DRVR_temp", jdbc_url, jdbc_props)

df_xfmDrvrUpd.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "tempdb.FctsPrvcyExtrnlEntyDrvrAllExtr_TMP_PRVCY_CDC_EE_DRVR_temp") \
    .mode("overwrite") \
    .save()

execute_dml("TRUNCATE TABLE tempdb.TMP_PRVCY_CDC_EE_DRVR", jdbc_url, jdbc_props)

insert_sql = (
    "INSERT INTO tempdb.TMP_PRVCY_CDC_EE_DRVR (ENEN_CKE, OP) "
    "SELECT ENEN_CKE, OP FROM tempdb.FctsPrvcyExtrnlEntyDrvrAllExtr_TMP_PRVCY_CDC_EE_DRVR_temp"
)
execute_dml(insert_sql, jdbc_url, jdbc_props)