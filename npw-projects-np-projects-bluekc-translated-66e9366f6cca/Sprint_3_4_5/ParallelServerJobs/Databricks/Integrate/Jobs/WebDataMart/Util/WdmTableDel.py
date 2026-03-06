# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC Â© Copyright 2010 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  WdmTableClearSeq
# MAGIC                
# MAGIC 
# MAGIC PROCESSING:   Clear Data Mart table passed in as parameter.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 \(9)Project/Altiris #\(9)Change Description\(9)\(9)\(9)\(9)Development Project\(9)Code Reviewer\(9)Date Reviewed       
# MAGIC ------------------              --------------------     \(9)------------------------\(9)-----------------------------------------------------------------------\(9)--------------------------------\(9)-------------------------------\(9)----------------------------       
# MAGIC Brent Leland            2010-02-11\(9)Prod Support           Original Programming                                                IntegrateWrhsDevl                  Steph Goddard        02/26/2010

# MAGIC Clear Data Mart Tables
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


TableName = get_widget_value('TableName','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
ClmMartOwner = get_widget_value('ClmMartOwner','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')

jdbc_url_IDS, jdbc_props_IDS = get_db_config(ids_secret_name)
extract_query_SYSDUMMY1 = "SELECT IBMREQD FROM sysibm.sysdummy1"
df_SYSDUMMY1 = (
    spark.read.format("jdbc")
        .option("url", jdbc_url_IDS)
        .options(**jdbc_props_IDS)
        .option("query", extract_query_SYSDUMMY1)
        .load()
)

df_Trans1 = df_SYSDUMMY1.select(df_SYSDUMMY1["IBMREQD"].alias("IBMREQD"))

jdbc_url_ClmMart, jdbc_props_ClmMart = get_db_config(clmmart_secret_name)
delete_sql = f"DELETE FROM {ClmMartOwner}.{TableName}"
execute_dml(delete_sql, jdbc_url_ClmMart, jdbc_props_ClmMart)