# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2013 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  This Job FTP's traceability_ref file
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
# MAGIC Bhoomi Dasari               10/7/2014                 5115-BHI                                      Changes to FTP stage to remove extra bytes             EnterpriseNewDevl         Kalyan Neelam           2014-10-08

# MAGIC Job Name: traceability_ref File FTP
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


ProdIn = get_widget_value('ProdIn','')

schema_traceability_ref = StructType([
    StructField("BHI_HOME_PLN_ID", StringType(), False),
    StructField("TRACEABILITY_FLD", StringType(), False),
    StructField("TRACEABILITY_DESC", StringType(), False)
])

df_traceability_ref = (
    spark.read
    .option("header", False)
    .option("quote", "")
    .option("delimiter", ",")
    .schema(schema_traceability_ref)
    .csv(f"{adls_path_publish}/external/traceability_ref")
)

df_FTP_traceability_ref = (
    df_traceability_ref
    .withColumn("BHI_HOME_PLN_ID", rpad(col("BHI_HOME_PLN_ID"), 3, " "))
    .withColumn("TRACEABILITY_FLD", rpad(col("TRACEABILITY_FLD"), 5, " "))
    .withColumn("TRACEABILITY_DESC", rpad(col("TRACEABILITY_DESC"), 30, " "))
    .select("BHI_HOME_PLN_ID", "TRACEABILITY_FLD", "TRACEABILITY_DESC")
)

write_files(
    df_FTP_traceability_ref,
    f"{adls_path_publish}/external/traceability_ref_ftp",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="",
    nullValue=None
)