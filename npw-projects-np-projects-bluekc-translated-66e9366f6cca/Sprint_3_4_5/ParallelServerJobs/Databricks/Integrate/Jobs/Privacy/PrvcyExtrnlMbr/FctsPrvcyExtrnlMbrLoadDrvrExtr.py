# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:  FctsPrvcyExtrnlMbrLoadDrvrExtr
# MAGIC CALLED BY:  IdsPrvcyExtrSeq
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #               Change Description                             Development Project      Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------     ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Ralph Tucker                  2010-04-06     3556                                Originally Programmed                            IntegrateCurDevl           Brent Leland                 04/07/2010
# MAGIC 
# MAGIC Anoop Nair                2022-03-11         S2S Remediation      Added FACETS DSN Connection parameters      IntegrateDev5		Ken Bradmon	2022-06-03

# MAGIC Facets Privacy Extrnl Mbr Driver Combine for all rows
# MAGIC This link is referenced by multiple sequencers for the row count
# MAGIC Load driver file with CDC keys
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
TableTimestamp = get_widget_value('TableTimestamp','')
tempdb_secret_name = get_widget_value('tempdb_secret_name','')

schema_FctsPrvcyCdcPmedDrvr = StructType([
    StructField("PMED_CKE", IntegerType(), nullable=False),
    StructField("OP", StringType(), nullable=False)
])
df_FctsPrvcyCdcPmedDrvr = (
    spark.read.format("csv")
    .option("header", False)
    .option("delimiter", ",")
    .option("quote", '"')
    .schema(schema_FctsPrvcyCdcPmedDrvr)
    .load(f"{adls_path_raw}/landing/TMP_PRVCY_CDC_PMED.dat")
)

schema_FctsPrvcyCdcEnenDrvr = StructType([
    StructField("PMED_CKE", IntegerType(), nullable=False),
    StructField("OP", StringType(), nullable=False)
])
df_FctsPrvcyCdcEnenDrvr = (
    spark.read.format("csv")
    .option("header", False)
    .option("delimiter", ",")
    .option("quote", '"')
    .schema(schema_FctsPrvcyCdcEnenDrvr)
    .load(f"{adls_path_raw}/landing/TMP_PRVCY_CDC_ENEN.dat")
)

df_Link_Collector_98 = df_FctsPrvcyCdcPmedDrvr.select("PMED_CKE","OP").union(df_FctsPrvcyCdcEnenDrvr.select("PMED_CKE","OP"))
df_DeDup = df_Link_Collector_98.dropDuplicates(["PMED_CKE"])
df_xfmDrvrUpd = df_DeDup.select("PMED_CKE").withColumn("OP", F.lit("I"))

jdbc_url, jdbc_props = get_db_config(tempdb_secret_name)
execute_dml("DROP TABLE IF EXISTS tempdb.FctsPrvcyExtrnlMbrLoadDrvrExtr_TMP_PRVCY_CDC_EM_DRVR_temp", jdbc_url, jdbc_props)
execute_dml(
    """
    CREATE TABLE tempdb.FctsPrvcyExtrnlMbrLoadDrvrExtr_TMP_PRVCY_CDC_EM_DRVR_temp (
      PMED_CKE INT NOT NULL,
      OP CHAR(1) NOT NULL
    )
    """,
    jdbc_url,
    jdbc_props
)
df_xfmDrvrUpd.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "tempdb.FctsPrvcyExtrnlMbrLoadDrvrExtr_TMP_PRVCY_CDC_EM_DRVR_temp") \
    .mode("append") \
    .save()

merge_sql = """
MERGE INTO tempdb..TMP_PRVCY_CDC_EM_DRVR AS target
USING tempdb.FctsPrvcyExtrnlMbrLoadDrvrExtr_TMP_PRVCY_CDC_EM_DRVR_temp AS source
ON (target.PMED_CKE = source.PMED_CKE)
WHEN MATCHED THEN
  UPDATE SET target.OP = source.OP
WHEN NOT MATCHED THEN
  INSERT (PMED_CKE, OP)
  VALUES (source.PMED_CKE, source.OP);
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)