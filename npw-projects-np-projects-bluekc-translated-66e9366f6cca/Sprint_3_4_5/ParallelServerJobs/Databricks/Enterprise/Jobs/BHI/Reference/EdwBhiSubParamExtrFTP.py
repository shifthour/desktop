# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2013 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME: 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  This Job FTP's submission_parameter file
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
# MAGIC Bhoomi Dasari                10/7/2014               5115-BHI                                     Changes to FTP stage to remove extra bytes               EnterpriseNewDevl         Kalyan Neelam           2014-10-08

# MAGIC Job Name: submission_parameter File FTP
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import rpad, col
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


ProdIn = get_widget_value('ProdIn','')

schema_submission_parameter = StructType([
    StructField("SUBMSN_MODE", StringType(), False)
])

df_submission_parameter = (
    spark.read
        .option("header", "false")
        .option("sep", ",")
        .schema(schema_submission_parameter)
        .csv(f"{adls_path_publish}/external/submission_parameter")
)

df_FTP_submission_parameter = df_submission_parameter.select(
    rpad(col("SUBMSN_MODE"), 1, " ").alias("SUBMSN_MODE")
)

write_files(
    df_FTP_submission_parameter,
    f"{adls_path_publish}/external/submission_parameter_ftp",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)