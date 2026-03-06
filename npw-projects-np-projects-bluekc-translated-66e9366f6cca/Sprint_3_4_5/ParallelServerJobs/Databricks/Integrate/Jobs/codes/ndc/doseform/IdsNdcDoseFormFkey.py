# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2005, 2021 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsNdcDoseFormFKey
# MAGIC 
# MAGIC DESCRIPTION:    Assigns foreign keys to DoseForm records
# MAGIC       
# MAGIC 
# MAGIC INPUTS:	File from extract process with primary key assigned
# MAGIC 
# MAGIC HASH FILES:  hf_recycle
# MAGIC 
# MAGIC TRANSFORMS:  none
# MAGIC 
# MAGIC                            
# MAGIC PROCESSING:     
# MAGIC   
# MAGIC 
# MAGIC OUTPUTS:   DOSE_FORM.dat
# MAGIC MODIFICATIONS:
# MAGIC BJ Luce                10/31/2005   -               Original program  
# MAGIC BJ Luce                4/2006                           use environment parameters, hard code input and output
# MAGIC Reddy Sanam       06/03/2021  360429     Remove 'NA' step from transformer                                       IntegrateDev2      Hugh Sisson     2021-06-04

# MAGIC Read common record format file from extract job.
# MAGIC Set all foreign surragote keys
# MAGIC Merge source data with default rows
# MAGIC Writing Sequential File to /load
# MAGIC Recycle hash not necessary because there is no foreign key lookup
# MAGIC Default Entry for 'NA' has been removed since 'NA' is a valid value for DOSE_FORM_CD. If we create 'NA' entry, rest of the attributes are being set to NA. So the link for making 'NA' entry is removed.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    TimestampType,
    DecimalType
)
from pyspark.sql.functions import col, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


Logging = get_widget_value('Logging', 'Y')

schema_DoseFormExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38, 10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("DOSE_FORM_SK", IntegerType(), nullable=False),
    StructField("DOSE_FORM_CD", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("DOSE_FORM_DESC", StringType(), nullable=False),
    StructField("DOSE_FORM_CD_DESC", StringType(), nullable=False)
])

df_DoseFormExtr = (
    spark.read
    .option("header", "false")
    .option("quote", "\"")
    .option("escape", "\"")
    .option("sep", ",")
    .schema(schema_DoseFormExtr)
    .csv(f"{adls_path}/key/FdbNdcDoseFormExtr.dat")
)

df_Fkey = df_DoseFormExtr.select(
    col("DOSE_FORM_SK").alias("DOSE_FORM_SK"),
    col("DOSE_FORM_CD").alias("DOSE_FORM_CD"),
    col("DOSE_FORM_DESC").alias("DOSE_FORM_DESC"),
    col("DOSE_FORM_CD_DESC").alias("DOSE_FORM_CD_DESC")
)

df_DefaultUNK = (
    df_DoseFormExtr.limit(1).select(
        lit(0).alias("DOSE_FORM_SK"),
        lit("UNK").alias("DOSE_FORM_CD"),
        lit("UNK").alias("DOSE_FORM_DESC"),
        lit("UNK").alias("DOSE_FORM_CD_DESC")
    )
)

df_Collector = df_Fkey.union(df_DefaultUNK)

df_final = df_Collector.select(
    col("DOSE_FORM_SK").cast("int").alias("DOSE_FORM_SK"),
    rpad(col("DOSE_FORM_CD"), 3, " ").alias("DOSE_FORM_CD"),
    col("DOSE_FORM_DESC"),
    col("DOSE_FORM_CD_DESC")
)

write_files(
    df_final,
    f"{adls_path}/load/DOSE_FORM.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)