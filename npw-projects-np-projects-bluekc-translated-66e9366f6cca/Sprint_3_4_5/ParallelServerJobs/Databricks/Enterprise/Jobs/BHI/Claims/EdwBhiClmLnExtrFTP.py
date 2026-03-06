# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2013 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  This Job FTP's std_inp_claim_lines file
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
# MAGIC Bhoomi Dasari               10/7/2014                 5115-BHI                                      Changes to FTP stage to remove extra bytes             EnterpriseNewDevl         Kalyan Neelam          2014-10-08

# MAGIC Job Name: std_inp_claim_lines File FTP
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

schema_std_inp_claim_lines = StructType([
    StructField("BHI_HOME_PLN_ID", StringType(), nullable=False),
    StructField("CLM_ID", StringType(), nullable=False),
    StructField("CLM_LN_NO", StringType(), nullable=False),
    StructField("TRACEABILITY_FLD", StringType(), nullable=False),
    StructField("ADJ_SEQ_NO", StringType(), nullable=False),
    StructField("CPT_AND_HCPCS_CD", StringType(), nullable=False),
    StructField("PROC_MOD", StringType(), nullable=False),
    StructField("BNF_PAYMT_STTUS_CD", StringType(), nullable=False),
    StructField("CLM_PAYMT_STTUS", StringType(), nullable=False),
    StructField("NON_COV_RSN_CD_PRI", StringType(), nullable=False),
    StructField("NON_COV_RSN_CD_2", StringType(), nullable=False),
    StructField("NON_COV_RSN_CD_3", StringType(), nullable=False),
    StructField("NON_COV_RSN_CD_4", StringType(), nullable=False),
    StructField("RMBRMT_TYP_CD", StringType(), nullable=False),
    StructField("RVNU_CD", StringType(), nullable=False),
    StructField("TOT_UNIT", StringType(), nullable=False),
    StructField("SVC_POST_DT", StringType(), nullable=False),
    StructField("SVC_FROM_DT", StringType(), nullable=False),
    StructField("SUBMT_AMT", StringType(), nullable=False),
    StructField("NON_COV_AMT", StringType(), nullable=False),
    StructField("ALW_AMT", StringType(), nullable=False),
    StructField("PAYMT_AMT", StringType(), nullable=False),
    StructField("COB_TPL_AMT", StringType(), nullable=False),
    StructField("COINS", StringType(), nullable=False),
    StructField("COPAY", StringType(), nullable=False),
    StructField("DEDCT", StringType(), nullable=False)
])

df_std_inp_claim_lines = (
    spark.read.format("csv")
    .option("header", "false")
    .option("sep", ",")
    .option("quote", "\u0000")
    .schema(schema_std_inp_claim_lines)
    .load(f"{adls_path_publish}/external/std_inp_claim_lines.dat")
)

df_std_inp_claim_lines_rpad = df_std_inp_claim_lines
df_std_inp_claim_lines_rpad = df_std_inp_claim_lines_rpad.withColumn("BHI_HOME_PLN_ID", rpad(col("BHI_HOME_PLN_ID"), 3, " "))
df_std_inp_claim_lines_rpad = df_std_inp_claim_lines_rpad.withColumn("CLM_ID", rpad(col("CLM_ID"), 25, " "))
df_std_inp_claim_lines_rpad = df_std_inp_claim_lines_rpad.withColumn("CLM_LN_NO", rpad(col("CLM_LN_NO"), 3, " "))
df_std_inp_claim_lines_rpad = df_std_inp_claim_lines_rpad.withColumn("TRACEABILITY_FLD", rpad(col("TRACEABILITY_FLD"), 5, " "))
df_std_inp_claim_lines_rpad = df_std_inp_claim_lines_rpad.withColumn("ADJ_SEQ_NO", rpad(col("ADJ_SEQ_NO"), 8, " "))
df_std_inp_claim_lines_rpad = df_std_inp_claim_lines_rpad.withColumn("CPT_AND_HCPCS_CD", rpad(col("CPT_AND_HCPCS_CD"), 6, " "))
df_std_inp_claim_lines_rpad = df_std_inp_claim_lines_rpad.withColumn("PROC_MOD", rpad(col("PROC_MOD"), 2, " "))
df_std_inp_claim_lines_rpad = df_std_inp_claim_lines_rpad.withColumn("BNF_PAYMT_STTUS_CD", rpad(col("BNF_PAYMT_STTUS_CD"), 1, " "))
df_std_inp_claim_lines_rpad = df_std_inp_claim_lines_rpad.withColumn("CLM_PAYMT_STTUS", rpad(col("CLM_PAYMT_STTUS"), 1, " "))
df_std_inp_claim_lines_rpad = df_std_inp_claim_lines_rpad.withColumn("NON_COV_RSN_CD_PRI", rpad(col("NON_COV_RSN_CD_PRI"), 2, " "))
df_std_inp_claim_lines_rpad = df_std_inp_claim_lines_rpad.withColumn("NON_COV_RSN_CD_2", rpad(col("NON_COV_RSN_CD_2"), 2, " "))
df_std_inp_claim_lines_rpad = df_std_inp_claim_lines_rpad.withColumn("NON_COV_RSN_CD_3", rpad(col("NON_COV_RSN_CD_3"), 2, " "))
df_std_inp_claim_lines_rpad = df_std_inp_claim_lines_rpad.withColumn("NON_COV_RSN_CD_4", rpad(col("NON_COV_RSN_CD_4"), 2, " "))
df_std_inp_claim_lines_rpad = df_std_inp_claim_lines_rpad.withColumn("RMBRMT_TYP_CD", rpad(col("RMBRMT_TYP_CD"), 5, " "))
df_std_inp_claim_lines_rpad = df_std_inp_claim_lines_rpad.withColumn("RVNU_CD", rpad(col("RVNU_CD"), 4, " "))
df_std_inp_claim_lines_rpad = df_std_inp_claim_lines_rpad.withColumn("TOT_UNIT", rpad(col("TOT_UNIT"), 9, " "))
df_std_inp_claim_lines_rpad = df_std_inp_claim_lines_rpad.withColumn("SVC_POST_DT", rpad(col("SVC_POST_DT"), 8, " "))
df_std_inp_claim_lines_rpad = df_std_inp_claim_lines_rpad.withColumn("SVC_FROM_DT", rpad(col("SVC_FROM_DT"), 8, " "))
df_std_inp_claim_lines_rpad = df_std_inp_claim_lines_rpad.withColumn("SUBMT_AMT", rpad(col("SUBMT_AMT"), 10, " "))
df_std_inp_claim_lines_rpad = df_std_inp_claim_lines_rpad.withColumn("NON_COV_AMT", rpad(col("NON_COV_AMT"), 10, " "))
df_std_inp_claim_lines_rpad = df_std_inp_claim_lines_rpad.withColumn("ALW_AMT", rpad(col("ALW_AMT"), 10, " "))
df_std_inp_claim_lines_rpad = df_std_inp_claim_lines_rpad.withColumn("PAYMT_AMT", rpad(col("PAYMT_AMT"), 10, " "))
df_std_inp_claim_lines_rpad = df_std_inp_claim_lines_rpad.withColumn("COB_TPL_AMT", rpad(col("COB_TPL_AMT"), 10, " "))
df_std_inp_claim_lines_rpad = df_std_inp_claim_lines_rpad.withColumn("COINS", rpad(col("COINS"), 10, " "))
df_std_inp_claim_lines_rpad = df_std_inp_claim_lines_rpad.withColumn("COPAY", rpad(col("COPAY"), 10, " "))
df_std_inp_claim_lines_rpad = df_std_inp_claim_lines_rpad.withColumn("DEDCT", rpad(col("DEDCT"), 10, " "))

df_std_inp_claim_lines_final = df_std_inp_claim_lines_rpad.select(
    "BHI_HOME_PLN_ID",
    "CLM_ID",
    "CLM_LN_NO",
    "TRACEABILITY_FLD",
    "ADJ_SEQ_NO",
    "CPT_AND_HCPCS_CD",
    "PROC_MOD",
    "BNF_PAYMT_STTUS_CD",
    "CLM_PAYMT_STTUS",
    "NON_COV_RSN_CD_PRI",
    "NON_COV_RSN_CD_2",
    "NON_COV_RSN_CD_3",
    "NON_COV_RSN_CD_4",
    "RMBRMT_TYP_CD",
    "RVNU_CD",
    "TOT_UNIT",
    "SVC_POST_DT",
    "SVC_FROM_DT",
    "SUBMT_AMT",
    "NON_COV_AMT",
    "ALW_AMT",
    "PAYMT_AMT",
    "COB_TPL_AMT",
    "COINS",
    "COPAY",
    "DEDCT"
)

df_ftp_std_inp_claim_lines = df_std_inp_claim_lines_final

write_files(
    df_ftp_std_inp_claim_lines,
    f"{adls_path_publish}/external/std_inp_claim_lines_ftp.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)