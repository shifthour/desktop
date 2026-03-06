# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2011 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  F2FMbrSrvyQstnAnswrExtrSeq
# MAGIC                
# MAGIC 
# MAGIC PROCESSING: Processes records from Primary key process and assigns foreingn keys and loads into IDS MBR_SRVY table
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 \(9)Project/Altiris #\(9)Change Description\(9)\(9)\(9)\(9)Development Project\(9)Code Reviewer\(9)Date Reviewed       
# MAGIC ------------------              --------------------     \(9)------------------------\(9)-----------------------------------------------------------------------\(9)--------------------------------\(9)-------------------------------\(9)----------------------------       
# MAGIC Bhoomi Dasari         2011-04-06               4673                       Original Programming                                                IntegrateWrhsDevl                    SAndrew              2011-04-20

# MAGIC MBR_SRVY Foreign Key job
# MAGIC Writing Sequential File to /load
# MAGIC Merge source data with default rows
# MAGIC Set all foreign surrogate keys
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
from pyspark.sql import functions as F
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Retrieve parameter values
InFile = get_widget_value("InFile", "")
Logging = get_widget_value("Logging", "")
SrcSysCd = get_widget_value("SrcSysCd", "")

# Schema for reading the MbrSrvy file
schema_MbrSrvy = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),   # char(10)
    StructField("DISCARD_IN", StringType(), False),       # char(1)
    StructField("PASS_THRU_IN", StringType(), False),     # char(1)
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38, 10), False),  # numeric
    StructField("SRC_SYS_CD", StringType(), False),       # varchar
    StructField("PRI_KEY_STRING", StringType(), False),   # varchar
    StructField("MBR_SRVY_SK", IntegerType(), False),
    StructField("SRC_SYS_CD_SK", IntegerType(), False),
    StructField("MBR_SRVY_TYP_CD", StringType(), False),  # varchar
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("MBR_SRVY_CAT_CD_SK", IntegerType(), True),
    StructField("MBR_SRVY_TYP_CD_SK", IntegerType(), True),
    StructField("EFF_DT_SK", StringType(), False),        # char(10)
    StructField("TERM_DT_SK", StringType(), False),       # char(10)
    StructField("MBR_SRVY_DESC", StringType(), False)     # varchar
])

# Read the MbrSrvy sequential file
df_MbrSrvy = (
    spark.read
    .option("sep", ",")
    .option("quote", '"')
    .schema(schema_MbrSrvy)
    .csv(f"{adls_path}/key/{InFile}")
)

# Add columns from the Transformer stage (ForeignKey)
df_transformed = (
    df_MbrSrvy
    .withColumn("svSrcSysCdSk", GetFkeyCodes(F.lit("IDS"), F.col("MBR_SRVY_SK"), F.lit("SOURCE SYSTEM"), F.col("SRC_SYS_CD"), F.lit(Logging)))
    .withColumn("svMbrSrvyTypCdSk", GetFkeyCodes(F.lit("BCBSKCCOMMON"), F.col("MBR_SRVY_SK"), F.lit("MEMBER SURVEY TYPE"), F.col("MBR_SRVY_TYP_CD"), F.lit(Logging)))
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("MBR_SRVY_SK")))
)

# Recycle link (ErrCount > 0)
df_Recycle = df_transformed.filter(F.col("ErrCount") > 0).select(
    F.expr("GetRecycleKey(MBR_SRVY_SK)").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD"),
    F.col("DISCARD_IN"),
    F.col("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ErrCount").alias("ERR_CT"),
    (F.col("RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("MBR_SRVY_SK"),
    F.col("svSrcSysCdSk").alias("SRC_SYS_CD_SK"),
    F.col("MBR_SRVY_TYP_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("MBR_SRVY_CAT_CD_SK"),
    F.col("MBR_SRVY_TYP_CD_SK"),
    F.col("EFF_DT_SK"),
    F.col("TERM_DT_SK"),
    F.col("MBR_SRVY_DESC")
)

# Rpad any char columns in Recycle link
df_Recycle = (
    df_Recycle
    .withColumn("INSRT_UPDT_CD", rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", rpad(F.col("PASS_THRU_IN"), 1, " "))
    .withColumn("EFF_DT_SK", rpad(F.col("EFF_DT_SK"), 10, " "))
    .withColumn("TERM_DT_SK", rpad(F.col("TERM_DT_SK"), 10, " "))
)

# Write Recycle link to parquet (CHashedFileStage -> scenario C)
write_files(
    df_Recycle.select(
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "PASS_THRU_IN",
        "FIRST_RECYC_DT",
        "ERR_CT",
        "RECYCLE_CT",
        "SRC_SYS_CD",
        "PRI_KEY_STRING",
        "MBR_SRVY_SK",
        "SRC_SYS_CD_SK",
        "MBR_SRVY_TYP_CD",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "MBR_SRVY_CAT_CD_SK",
        "MBR_SRVY_TYP_CD_SK",
        "EFF_DT_SK",
        "TERM_DT_SK",
        "MBR_SRVY_DESC"
    ),
    f"{adls_path}/hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote='"',
    nullValue=None
)

# Fkey link (ErrCount=0 or PassThru='Y')
df_Fkey = df_transformed.filter(
    (F.col("ErrCount") == 0) | (F.col("PassThru") == 'Y')
).select(
    F.col("MBR_SRVY_SK"),
    F.col("svSrcSysCdSk").alias("SRC_SYS_CD_SK"),
    F.col("MBR_SRVY_TYP_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("MBR_SRVY_CAT_CD_SK"),
    F.col("svMbrSrvyTypCdSk").alias("MBR_SRVY_TYP_CD_SK"),
    F.col("EFF_DT_SK"),
    F.col("TERM_DT_SK"),
    F.col("MBR_SRVY_DESC")
)

# DefaultNA link (@INROWNUM=1):
# Produce a single row with literal values
schema_default = StructType([
    StructField("MBR_SRVY_SK", IntegerType(), True),
    StructField("SRC_SYS_CD_SK", IntegerType(), True),
    StructField("MBR_SRVY_TYP_CD", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("MBR_SRVY_CAT_CD_SK", IntegerType(), True),
    StructField("MBR_SRVY_TYP_CD_SK", IntegerType(), True),
    StructField("EFF_DT_SK", StringType(), True),
    StructField("TERM_DT_SK", StringType(), True),
    StructField("MBR_SRVY_DESC", StringType(), True)
])

df_DefaultNA = spark.createDataFrame(
    [
        (1, 1, "NA", 1, 1, 1, 1, "1753-01-01", "2199-12-31", "NA")
    ],
    schema_default
)

# DefaultUNK link (@INROWNUM=1):
df_DefaultUNK = spark.createDataFrame(
    [
        (0, 0, "UNK", 0, 0, 0, 0, "1753-01-01", "2199-12-31", "UNK")
    ],
    schema_default
)

# Collector stage -> Union all (Fkey, DefaultNA, DefaultUNK)
df_collected = (
    df_Fkey.unionByName(df_DefaultNA)
           .unionByName(df_DefaultUNK)
)

# Rpad char(10) columns in final output
df_collected = (
    df_collected
    .withColumn("EFF_DT_SK", rpad(F.col("EFF_DT_SK"), 10, " "))
    .withColumn("TERM_DT_SK", rpad(F.col("TERM_DT_SK"), 10, " "))
)

# Final write to MBR_SRVY.#SrcSysCd#.dat
write_files(
    df_collected.select(
        "MBR_SRVY_SK",
        "SRC_SYS_CD_SK",
        "MBR_SRVY_TYP_CD",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "MBR_SRVY_CAT_CD_SK",
        "MBR_SRVY_TYP_CD_SK",
        "EFF_DT_SK",
        "TERM_DT_SK",
        "MBR_SRVY_DESC"
    ),
    f"{adls_path}/load/MBR_SRVY.{SrcSysCd}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)