# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:  FctsPrvcyExtrnlMbrEnenDrvrAllExtr
# MAGIC CALLED BY: IdsPrvcyExtrSeq
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #               Change Description                             Development Project      Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------     ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Steph Goddard                11/24/09             3556                                Originally Programmed                            devlIDSnew             Brent/Steph                  12/29/2009       
# MAGIC Ralph Tucker                  2010-04-06     3556 CDS                        Ouput to flat file for merge with                IntegrateCurDevl          Brent Leland                  04/07/2010
# MAGIC                                                                                                        the Pmed flat driver file
# MAGIC Anoop Nair                2022-03-08         S2S Remediation      Added FACETS DSN Connection parameters      IntegrateDev5		Ken Bradmon	2022-06-03

# MAGIC Facets Privacy Extrnl Mbr Driver Extract for All Records
# MAGIC This link is referenced by multiple sequencers for the row count
# MAGIC Load driver flat file with CDC keys
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


FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
TableTimestamp = get_widget_value('TableTimestamp','')

jdbc_url, jdbc_props = get_db_config(facets_secret_name)
extract_query = (
    "SELECT  \n"
    "     fhp.PMED_CKE\n"
    "FROM  \n"
    + FacetsOwner
    + ".FHP_PMED_MEMBER_D fhp"
)

df_FacetsPrvcyExtrnMbr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_xfmDrvrUpd = df_FacetsPrvcyExtrnMbr.select("PMED_CKE").withColumn("OP", lit("I"))
df_xfmDrvrUpd = df_xfmDrvrUpd.withColumn("OP", rpad("OP", 1, " "))
df_xfmDrvrUpd = df_xfmDrvrUpd.select("PMED_CKE", "OP")

write_files(
    df_xfmDrvrUpd,
    f"{adls_path_raw}/landing/TMP_PRVCY_CDC_ENEN.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)