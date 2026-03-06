# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2012 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by:
# MAGIC                     MpiBcbsLoadSeq
# MAGIC 
# MAGIC Processing:
# MAGIC                     Assign foreign keys surrogate value
# MAGIC 
# MAGIC Control Job Rerun Information: 
# MAGIC                     Previous Run Successful:    Restore key file
# MAGIC                     Previous Run Aborted:         Restart, no other steps necessary
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                                  Project/                                                                                                                          Code                   Date
# MAGIC Developer           Date              Altiris #        Change Description                                                                                     Reviewer            Reviewed
# MAGIC -------------------------  -------------------   ----------------   -----------------------------------------------------------------------------------------------------------------   -------------------------  -------------------
# MAGIC Rick Henry         2012-07-25    4426           Original Programming                                                                                   Bhoomi Dasari     09/19/2012
# MAGIC Hugh Sisson       2012-10-24    4426           Moved logic for creating FCTS_BE_INPT to  MpiBcbsIndvBeExtr job        Bhoomi Dasari     11/13/2012

# MAGIC Bekey Assignment to New Members
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
)
from pyspark.sql.functions import col, lit, when, expr, rpad, trim
from decimal import Decimal
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/GetFkeyCodes
# COMMAND ----------
# MAGIC %run ../../../../shared_containers/PrimaryKey/GetFkeyErrorCnt
# COMMAND ----------
# MAGIC %run ../../../../shared_containers/PrimaryKey/GetRecycleKey
# COMMAND ----------

RunCycle = get_widget_value('RunCycle','6666')
Logging = get_widget_value('Logging','X')
CurrDate = get_widget_value('CurrDate','2012-07-27')
InFile = get_widget_value('InFile','')

schema_IdsBekeyPkey = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38,10), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("INDV_BE_KEY", DecimalType(38,10), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("COV_STTUS", StringType(), False),
    StructField("INDV_BE_GNDR_CD", StringType(), False),
    StructField("SRC_SYS_CD", IntegerType(), False),
    StructField("MBR_IN", StringType(), False),
    StructField("BRTH_DT_SK", StringType(), False),
    StructField("DCSD_DT", StringType(), False),
    StructField("ORIG_EFF_DT_SK", StringType(), False),
    StructField("FIRST_NM", StringType(), True),
    StructField("MIDINIT", StringType(), True),
    StructField("LAST_NM", StringType(), True),
    StructField("SSN", StringType(), False)
])

df_IdsBekeyPkey = (
    spark.read.format("csv")
    .option("header", "false")
    .option("quote", "\"")
    .option("delimiter", ",")
    .schema(schema_IdsBekeyPkey)
    .load(f"{adls_path}/key/{InFile}")
)

df_transform = (
    df_IdsBekeyPkey
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn("SrcSysCdSK", GetFkeyCodes(lit('IDS'), col("INDV_BE_KEY"), lit("SOURCE SYSTEM"), col("SRC_SYS_CD"), lit(Logging)))
    .withColumn("CovSttusCdSK", GetFkeyCodes(lit('CDS'), col("INDV_BE_KEY"), lit("COVERAGE STATUS"), col("COV_STTUS"), lit('X')))
    .withColumn("GndrCdSK", GetFkeyCodes(lit('FACETS'), col("INDV_BE_KEY"), lit("GENDER"), col("INDV_BE_GNDR_CD"), lit(Logging)))
    .withColumn("ErrCount", GetFkeyErrorCnt(col("INDV_BE_KEY")))
)

df_Fkey = (
    df_transform
    .filter((col("ErrCount") == 0) | (col("PassThru") == 'Y'))
    .select(
        col("INDV_BE_KEY").alias("INDV_BE_KEY"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("CovSttusCdSK").alias("INDV_BE_COV_STTUS_CD_SK"),
        col("GndrCdSK").alias("INDV_BE_GNDR_CD_SK"),
        col("SrcSysCdSK").alias("SRC_SYS_CD_SK"),
        col("MBR_IN").alias("MBR_IN"),
        col("BRTH_DT_SK").alias("BRTH_DT_SK"),
        col("DCSD_DT").alias("DCSD_DT_SK"),
        col("ORIG_EFF_DT_SK").alias("ORIG_EFF_DT_SK"),
        col("FIRST_NM").alias("FIRST_NM"),
        col("MIDINIT").alias("MIDINIT"),
        col("LAST_NM").alias("LAST_NM"),
        expr("CASE WHEN trim(SSN) = '' THEN '0' ELSE SSN END").alias("SSN")
    )
)

df_Recycle = (
    df_transform
    .filter(col("ErrCount") > 0)
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", GetRecycleKey(col("INDV_BE_KEY")))
    .withColumn("INSRT_UPDT_CD", col("INSRT_UPDT_CD"))
    .withColumn("DISCARD_IN", col("DISCARD_IN"))
    .withColumn("PASS_THRU_IN", col("PASS_THRU_IN"))
    .withColumn("FIRST_RECYC_DT", col("FIRST_RECYC_DT"))
    .withColumn("ERR_CT", col("ERR_CT") + lit(1))
    .withColumn("RECYCLE_CT", col("RECYCLE_CT"))
    .withColumn("PRI_KEY_STRING", col("PRI_KEY_STRING"))
    .withColumn("INDV_BE_KEY", col("INDV_BE_KEY"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", col("CRT_RUN_CYC_EXCTN_SK"))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", col("LAST_UPDT_RUN_CYC_EXCTN_SK"))
    .withColumn("COV_STTUS", col("COV_STTUS"))
    .withColumn("INDV_BE_GNDR_CD", col("INDV_BE_GNDR_CD"))
    .withColumn("SRC_SYS_CD", col("SRC_SYS_CD"))
    .withColumn("MBR_IN", col("MBR_IN"))
    .withColumn("BRTH_DT_SK", col("BRTH_DT_SK"))
    .withColumn("DCSD_DT", col("DCSD_DT"))
    .withColumn("ORIG_EFF_DT_SK", col("ORIG_EFF_DT_SK"))
    .withColumn("SSN", col("SSN"))
    .withColumn("FIRST_NM", col("FIRST_NM"))
    .withColumn("MIDINIT", col("MIDINIT"))
    .withColumn("LAST_NM", col("LAST_NM"))
    .select(
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "PASS_THRU_IN",
        "FIRST_RECYC_DT",
        "ERR_CT",
        "RECYCLE_CT",
        "PRI_KEY_STRING",
        "INDV_BE_KEY",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "COV_STTUS",
        "INDV_BE_GNDR_CD",
        "SRC_SYS_CD",
        "MBR_IN",
        "BRTH_DT_SK",
        "DCSD_DT",
        "ORIG_EFF_DT_SK",
        "SSN",
        "FIRST_NM",
        "MIDINIT",
        "LAST_NM"
    )
)

write_files(
    df_Recycle,
    f"{adls_path}/hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

data_DefaultNA = [(
    Decimal('1'),
    1,
    1,
    1,
    1,
    1,
    'N',
    'NA',
    'NA',
    'NA',
    'NA',
    ' ',
    'NA',
    '1'
)]

columns_DefaultNA = [
    "INDV_BE_KEY",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "INDV_BE_COV_STTUS_CD_SK",
    "INDV_BE_GNDR_CD_SK",
    "SRC_SYS_CD_SK",
    "MBR_IN",
    "BRTH_DT_SK",
    "DCSD_DT_SK",
    "ORIG_EFF_DT_SK",
    "FIRST_NM",
    "MIDINIT",
    "LAST_NM",
    "SSN"
]

df_DefaultNA = spark.createDataFrame(data_DefaultNA, columns_DefaultNA)

data_DefaultUNK = [(
    Decimal('0'),
    0,
    0,
    0,
    0,
    0,
    'N',
    'UNK',
    'UNK',
    'UNK',
    'UNK',
    'UNK',
    'UNK',
    '0'
)]

columns_DefaultUNK = [
    "INDV_BE_KEY",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "INDV_BE_COV_STTUS_CD_SK",
    "INDV_BE_GNDR_CD_SK",
    "SRC_SYS_CD_SK",
    "MBR_IN",
    "BRTH_DT_SK",
    "DCSD_DT_SK",
    "ORIG_EFF_DT_SK",
    "FIRST_NM",
    "MIDINIT",
    "LAST_NM",
    "SSN"
]

df_DefaultUNK = spark.createDataFrame(data_DefaultUNK, columns_DefaultUNK)

df_Collector = (
    df_Fkey
    .unionByName(df_DefaultNA)
    .unionByName(df_DefaultUNK)
)

df_final = df_Collector.select(
    col("INDV_BE_KEY"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("INDV_BE_COV_STTUS_CD_SK"),
    col("INDV_BE_GNDR_CD_SK"),
    col("SRC_SYS_CD_SK"),
    rpad(col("MBR_IN"), 1, ' ').alias("MBR_IN"),
    rpad(col("BRTH_DT_SK"), 10, ' ').alias("BRTH_DT_SK"),
    rpad(col("DCSD_DT_SK"), 10, ' ').alias("DCSD_DT_SK"),
    rpad(col("ORIG_EFF_DT_SK"), 10, ' ').alias("ORIG_EFF_DT_SK"),
    rpad(col("FIRST_NM"), 255, ' ').alias("FIRST_NM"),
    rpad(col("MIDINIT"), 255, ' ').alias("MIDINIT"),
    rpad(col("LAST_NM"), 255, ' ').alias("LAST_NM"),
    rpad(col("SSN"), 255, ' ').alias("SSN")
)

write_files(
    df_final,
    f"{adls_path}/load/INDV_BE.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)