# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 01/30/08 05:00:10 Batch  14640_18019 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 08/27/07 08:55:46 Batch  14484_32151 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 03/27/07 13:53:11 Batch  14331_49995 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 05/26/06 14:39:03 Batch  14026_52751 PROMOTE bckcetl ids20 dsadm Jim Hart
# MAGIC ^1_1 05/26/06 14:15:58 Batch  14026_51366 INIT bckcett testIDS30 dsadm Jim Hart
# MAGIC ^1_2 05/19/06 12:52:46 Batch  14019_46370 PROMOTE bckcett testIDS30 u03651 steffy
# MAGIC ^1_2 05/19/06 12:51:26 Batch  14019_46292 PROMOTE bckcett testIDS30 u03651 steffy
# MAGIC ^1_2 05/19/06 12:49:55 Batch  14019_46199 INIT bckcett devlIDS30 u03651 steffy
# MAGIC ^1_1 05/12/06 15:18:48 Batch  14012_55131 INIT bckcett devlIDS30 u03651 steffy
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsLoincCdFkey
# MAGIC 
# MAGIC DESCRIPTION:     Takes the sorted file and applies the primary key.   Preps the file for the foreign key lookup process that follows.
# MAGIC       
# MAGIC 
# MAGIC INPUTS:  Primary Key output file
# MAGIC 	
# MAGIC   
# MAGIC HASH FILES:     hf_recycle
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  GetFkeyCodes
# MAGIC                             
# MAGIC PROCESSING:  Natural keys are converted to surrogate keys
# MAGIC 
# MAGIC OUTPUTS:   table load file
# MAGIC                      hf_recycle - If recycling was used for this job records would be written to this hash file that had errors.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC             Suzanne Saylor  -  04/06/2006  -  Originally programmed

# MAGIC Set all foreign surragote keys
# MAGIC Merge source data with default rows
# MAGIC Writing Sequential File to /load
# MAGIC Read common record format file from extract job.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DecimalType
from pyspark.sql.functions import col, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


Logging = get_widget_value("Logging","X")

schema_IdsLoincCdExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38,10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("LOINC_CD_SK", IntegerType(), nullable=False),
    StructField("LOINC_CD", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LOINC_CD_REL_NM", StringType(), nullable=False),
    StructField("LOINC_CD_SH_NM", StringType(), nullable=False)
])

df_IdsLoincCdExtr = (
    spark.read.format("csv")
    .option("header", False)
    .option("inferSchema", False)
    .option("quote", "\"")
    .option("delimiter", ",")
    .schema(schema_IdsLoincCdExtr)
    .load(f"{adls_path}/key/LoincLoincCdExtr.LoincCd.uniq")
)

df_foreignKey_base = (
    df_IdsLoincCdExtr
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn("ErrCount", GetFkeyErrorCnt(col("LOINC_CD_SK")))
)

df_Fkey = (
    df_foreignKey_base
    .filter((col("ErrCount") == 0) | (col("PassThru") == "Y"))
    .select(
        col("LOINC_CD_SK").alias("LOINC_CD_SK"),
        col("LOINC_CD").alias("LOINC_CD"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("LOINC_CD_REL_NM").alias("LOINC_CD_REL_NM"),
        col("LOINC_CD_SH_NM").alias("LOINC_CD_SH_NM")
    )
)

df_Recycle_temp = df_foreignKey_base.filter(col("ErrCount") > 0)
df_Recycle = (
    df_Recycle_temp
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", GetRecycleKey(col("LOINC_CD_SK")))
    .withColumn("RECYCLE_CT", col("RECYCLE_CT") + lit(1))
)

df_Recycle_final = df_Recycle.select(
    col("JOB_EXCTN_RCRD_ERR_SK"),
    col("INSRT_UPDT_CD"),
    col("DISCARD_IN"),
    col("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ErrCount").alias("ERR_CT"),
    col("RECYCLE_CT"),
    col("SRC_SYS_CD"),
    col("PRI_KEY_STRING"),
    col("LOINC_CD_SK"),
    col("LOINC_CD"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("LOINC_CD_REL_NM"),
    col("LOINC_CD_SH_NM")
)

df_Recycle_final_padded = df_Recycle_final.select(
    col("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    rpad(col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    rpad(col("PRI_KEY_STRING"), <...>, " ").alias("PRI_KEY_STRING"),
    col("LOINC_CD_SK"),
    rpad(col("LOINC_CD"), <...>, " ").alias("LOINC_CD"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(col("LOINC_CD_REL_NM"), <...>, " ").alias("LOINC_CD_REL_NM"),
    rpad(col("LOINC_CD_SH_NM"), <...>, " ").alias("LOINC_CD_SH_NM")
)

write_files(
    df_Recycle_final_padded,
    "hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

defaultUNK_schema = StructType([
    StructField("LOINC_CD_SK", IntegerType(), True),
    StructField("LOINC_CD", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LOINC_CD_REL_NM", StringType(), True),
    StructField("LOINC_CD_SH_NM", StringType(), True)
])

df_DefaultUNK = spark.createDataFrame(
    [(0, "UNK", 0, 0, "UNK", "UNK")],
    schema=defaultUNK_schema
)

defaultNA_schema = StructType([
    StructField("LOINC_CD_SK", IntegerType(), True),
    StructField("LOINC_CD", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LOINC_CD_REL_NM", StringType(), True),
    StructField("LOINC_CD_SH_NM", StringType(), True)
])

df_DefaultNA = spark.createDataFrame(
    [(1, "NA", 1, 1, "NA", "NA")],
    schema=defaultNA_schema
)

df_Fkey_final = df_Fkey.select(
    col("LOINC_CD_SK"),
    col("LOINC_CD"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("LOINC_CD_REL_NM"),
    col("LOINC_CD_SH_NM")
)

df_Collector = df_Fkey_final.union(df_DefaultUNK).union(df_DefaultNA)

df_Final = df_Collector.select(
    col("LOINC_CD_SK"),
    rpad(col("LOINC_CD"), <...>, " ").alias("LOINC_CD"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(col("LOINC_CD_REL_NM"), <...>, " ").alias("LOINC_CD_REL_NM"),
    rpad(col("LOINC_CD_SH_NM"), <...>, " ").alias("LOINC_CD_SH_NM")
)

write_files(
    df_Final,
    f"{adls_path}/load/LOINC_CD.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)