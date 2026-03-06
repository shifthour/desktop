# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2013 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  Std product
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  This Job FTP's Std product file
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
# MAGIC Bhoomi Dasari                10/7/2014               5115-BHI                                   Changes to FTP stage to remove extra bytes                 EnterpriseNewDevl         Kalyan Neelam           2014-10-08

# MAGIC Job Name: Std product File FTP
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


ProdIn = get_widget_value('ProdIn','')

schema_std_product = StructType([
    StructField("BHI_HOME_PLN_ID", StringType(), False),
    StructField("HOME_PLN_PROD_ID", StringType(), False),
    StructField("HOME_PLN_PROD_NM", StringType(), False),
    StructField("BHI_PROD_CAT", StringType(), False),
    StructField("TRACEABILITY_FLD", StringType(), False)
])

df_std_product_unpadded = (
    spark.read
    .schema(schema_std_product)
    .option("header", "false")
    .option("quote", None)
    .csv(f"{adls_path_publish}/external/std_product")
)

df_std_product = df_std_product_unpadded.select(
    rpad("BHI_HOME_PLN_ID", 3, " ").alias("BHI_HOME_PLN_ID"),
    rpad("HOME_PLN_PROD_ID", 15, " ").alias("HOME_PLN_PROD_ID"),
    rpad("HOME_PLN_PROD_NM", 50, " ").alias("HOME_PLN_PROD_NM"),
    rpad("BHI_PROD_CAT", 3, " ").alias("BHI_PROD_CAT"),
    rpad("TRACEABILITY_FLD", 5, " ").alias("TRACEABILITY_FLD")
)

df_FTP_std_product = df_std_product.select(
    "BHI_HOME_PLN_ID",
    "HOME_PLN_PROD_ID",
    "HOME_PLN_PROD_NM",
    "BHI_PROD_CAT",
    "TRACEABILITY_FLD"
)