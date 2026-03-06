# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2013 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  EdwBhiSubCntrFTP
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  This Job FTP's Submission control file
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
# MAGIC Bhoomi Dasari               10/7/2014                 5115-BHI                                     Changes to FTP stage to remove extra bytes              EnterpriseNewDevl         Kalyan Neelam           2014-10-08

# MAGIC Job Name: Submission Control File FTP
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


ProdIn = get_widget_value('ProdIn','')

schema_Ftp_Submission_Control_File = StructType([
    StructField("BHI_HOME_PLN_ID", IntegerType(), False),
    StructField("EXTR_NM", StringType(), False),
    StructField("MIN_CLM_PROCESSED_DT", StringType(), False),
    StructField("MAX_CLM_PRCS_DT", StringType(), False),
    StructField("SUBMSN_DT", StringType(), False),
    StructField("RCRD_CT", StringType(), False),
    StructField("TOT_SUBMT_AMT", StringType(), False),
    StructField("TOT_NONCOV_AMT", StringType(), False),
    StructField("TOT_ALW_AMT", StringType(), False),
    StructField("TOT_PD_AMT", StringType(), False),
    StructField("TOT_COB_TPL_AMT", StringType(), False),
    StructField("TOT_COINS_AMT", StringType(), False),
    StructField("TOT_COPAY_AMT", StringType(), False),
    StructField("TOT_DEDCT_AMT", StringType(), False),
    StructField("TOT_FFS_EQVLNT_AMT", StringType(), False)
])

df_Ftp_Submission_Control_File = (
    spark.read.format("csv")
    .option("header", "false")
    .option("sep", ",")
    .schema(schema_Ftp_Submission_Control_File)
    .load(f"{adls_path_publish}/external/submission_control")
)

df_FTP_Enterprise_49 = df_Ftp_Submission_Control_File

df_final = df_FTP_Enterprise_49.select(
    "BHI_HOME_PLN_ID",
    "EXTR_NM",
    "MIN_CLM_PROCESSED_DT",
    "MAX_CLM_PRCS_DT",
    "SUBMSN_DT",
    "RCRD_CT",
    "TOT_SUBMT_AMT",
    "TOT_NONCOV_AMT",
    "TOT_ALW_AMT",
    "TOT_PD_AMT",
    "TOT_COB_TPL_AMT",
    "TOT_COINS_AMT",
    "TOT_COPAY_AMT",
    "TOT_DEDCT_AMT",
    "TOT_FFS_EQVLNT_AMT"
)

df_final = df_final.withColumn("EXTR_NM", rpad("EXTR_NM", 30, " "))
df_final = df_final.withColumn("MIN_CLM_PROCESSED_DT", rpad("MIN_CLM_PROCESSED_DT", 8, " "))
df_final = df_final.withColumn("MAX_CLM_PRCS_DT", rpad("MAX_CLM_PRCS_DT", 8, " "))
df_final = df_final.withColumn("SUBMSN_DT", rpad("SUBMSN_DT", 8, " "))
df_final = df_final.withColumn("RCRD_CT", rpad("RCRD_CT", 10, " "))
df_final = df_final.withColumn("TOT_SUBMT_AMT", rpad("TOT_SUBMT_AMT", 15, " "))
df_final = df_final.withColumn("TOT_NONCOV_AMT", rpad("TOT_NONCOV_AMT", 15, " "))
df_final = df_final.withColumn("TOT_ALW_AMT", rpad("TOT_ALW_AMT", 15, " "))
df_final = df_final.withColumn("TOT_PD_AMT", rpad("TOT_PD_AMT", 15, " "))
df_final = df_final.withColumn("TOT_COB_TPL_AMT", rpad("TOT_COB_TPL_AMT", 15, " "))
df_final = df_final.withColumn("TOT_COINS_AMT", rpad("TOT_COINS_AMT", 15, " "))
df_final = df_final.withColumn("TOT_COPAY_AMT", rpad("TOT_COPAY_AMT", 15, " "))
df_final = df_final.withColumn("TOT_DEDCT_AMT", rpad("TOT_DEDCT_AMT", 15, " "))
df_final = df_final.withColumn("TOT_FFS_EQVLNT_AMT", rpad("TOT_FFS_EQVLNT_AMT", 15, " "))

# Below simulates the FTP logic of PxFTP with df_final
df_ftp_enterprise_49 = df_final  # Replace or extend with any actual FTP transfer logic as needed.