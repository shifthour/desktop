# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2012 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  Extract records from tempdb  TEMP IDS CLAIM to get a Link Count.  
# MAGIC                           
# MAGIC PROCESSING:  
# MAGIC   
# MAGIC MODIFICATIONS:
# MAGIC           02/28/2012 - Rick Henry  Originally Programmed
# MAGIC           
# MAGIC NAme            DAte                 Project                                           Description                                                                                                                         environment      Code Review    Date Reviewed
# MAGIC RHenry         2012-02-29       TTR-1262                                       Extract records from Current run's tempdb..TMP_IDS_CLAIM table in order to                  idsnewdevl          
# MAGIC                                                                                                      retrieve Link Counts
# MAGIC Prabhu ES    2022-02-26       S2S Remediation                            MSSQL connection parameters added                                                                              IntegrateDev5


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


DriverTable = get_widget_value('DriverTable', 'DriverTable')
$FacetsOwner = get_widget_value('$FacetsOwner', '')
facets_secret_name = get_widget_value('facets_secret_name', '')

jdbc_url, jdbc_props = get_db_config(facets_secret_name)
extract_query = f"SELECT CLM_ID FROM tempdb..{DriverTable}"
df_CMC_CLCL_CLAIM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_Trans1 = df_CMC_CLCL_CLAIM.select(
    rpad(col("CLM_ID"), 12, " ").alias("CLM_ID")
)

write_files(
    df_Trans1,
    f"{adls_path}/dev/null",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)