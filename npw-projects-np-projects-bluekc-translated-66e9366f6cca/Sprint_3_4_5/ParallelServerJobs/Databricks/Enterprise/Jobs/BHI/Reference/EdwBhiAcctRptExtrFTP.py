# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2013 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  This Job FTP's account_reporting_ref file
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  
# MAGIC 
# MAGIC HASH FILES:  
# MAGIC 
# MAGIC 
# MAGIC RERUN INFORMATION: 
# MAGIC 
# MAGIC              PREVIOUS RUN SUCCESSFUL:   Nothing to reset. 
# MAGIC              PREVIOUS RUN ABORTED:         Nothing to reset, just restart.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                          Date                    Project/Ticket #\(9)           Change Description\(9)\(9)\(9)              Development Project\(9)      Code Reviewer\(9)        Date Reviewed       
# MAGIC ------------------                    -------------------            ---------------------------\(9)           -----------------------------------------------------------------------            --------------------------------\(9)      -------------------------         -------------------------    
# MAGIC Praveen  Annam             2014-07-14              5115 BHI                                       Original programming                                                  EnterpriseNewDevl        Bhoomi Dasari           7/15/2014  
# MAGIC 
# MAGIC Bhoomi Dasari                10/7/2014               5115-BHI                                       Changes to FTP stage to remove extra bytes             EnterpriseNewDevl         Kalyan Neelam          2014-10-08

# MAGIC Job Name: account_reporting_ref File FTP
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType
import pyspark.sql.functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


ProdIn = get_widget_value('ProdIn','')

schema_account_reporting_ref = StructType([
    StructField("BHI_HOME_PLN_ID", StringType(), False),
    StructField("HOME_PLN_PROD_ID", StringType(), False),
    StructField("ACCT", StringType(), False),
    StructField("RPTNG_ACCT_ID", StringType(), False),
    StructField("RPTNG_ACCT_NM", StringType(), False)
])

df_account_reporting_ref = (
    spark.read
    .option("header", False)
    .option("quote", None)
    .schema(schema_account_reporting_ref)
    .csv(f"{adls_path_publish}/external/account_reporting_ref")
)

df_FTP_account_reporting_ref = df_account_reporting_ref.select(
    F.rpad(F.col("BHI_HOME_PLN_ID"), 3, " ").alias("BHI_HOME_PLN_ID"),
    F.rpad(F.col("HOME_PLN_PROD_ID"), 15, " ").alias("HOME_PLN_PROD_ID"),
    F.rpad(F.col("ACCT"), 14, " ").alias("ACCT"),
    F.rpad(F.col("RPTNG_ACCT_ID"), 14, " ").alias("RPTNG_ACCT_ID"),
    F.rpad(F.col("RPTNG_ACCT_NM"), 50, " ").alias("RPTNG_ACCT_NM")
)