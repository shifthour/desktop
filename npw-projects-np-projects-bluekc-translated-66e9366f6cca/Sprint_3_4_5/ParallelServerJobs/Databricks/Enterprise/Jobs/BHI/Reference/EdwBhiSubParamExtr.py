# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2013 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  EdwBhiSubParamExtr
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  Extract Submission Parameter to be sent to BCBSA for BHI (Blue Health Intelligence)
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  None
# MAGIC 
# MAGIC HASH FILES:  None
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
# MAGIC Developer                                                    Date                    Project/Ticket #\(9)           Change Description\(9)\(9)\(9)              Development Project\(9)      Code Reviewer\(9)        Date Reviewed       
# MAGIC ------------------                                                 -------------------         ---------------------------\(9)           -----------------------------------------------------------------------            --------------------------------\(9)      -------------------------         -------------------------    
# MAGIC Praveen Annam                                          2013-08-22           5115 BHI                               Original programming                                                  EnterpriseNewDevl        Kalyan Neelam           2013-11-17
# MAGIC Praveen Annam                                          2014-07-14           5115 BHI                               FTP stage added to transfer in binary mode               EnterpriseNewDevl        Bhoomi Dasari            7/15/2014

# MAGIC Create one row with one column for Submission Parameter Extract,
# MAGIC Create fixed length data and provide monthly file to BHI
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
ProdIn = get_widget_value('ProdIn','')

df_Row_Generator_15 = spark.createDataFrame([(None,)], "SUBMSN_MODE: string")

Trns_stageVariable_LoopCondition = None

val_for_SUBMSN_MODE = 'D' if ProdIn[:1] == 'T' else 'P'
df_Trns = df_Row_Generator_15.withColumn("SUBMSN_MODE", lit(val_for_SUBMSN_MODE))

df_submission_parameter = (
    df_Trns
    .select("SUBMSN_MODE")
    .withColumn("SUBMSN_MODE", rpad("SUBMSN_MODE", 1, " "))
)

write_files(
    df_submission_parameter,
    f"{adls_path_publish}/external/submission_parameter",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote=None,
    nullValue=None
)