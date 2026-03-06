# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2013 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  
# MAGIC 
# MAGIC PROCESSING:   Assigns Foreign Keys for the IDS table VBB_RWRD.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                         Date                 	Project/Altiris #	Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------                       --------------------	               -----------------------	-----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Karthik Chintalapani         2013-04-30                      4963 VBB Phase III     Initial Programming                                                                      IntegrateNewDevl       Bhoomi Dasari           5/16/2013

# MAGIC Set all foreign surragote keys
# MAGIC Merge source data with default rows
# MAGIC Writing Sequential File to /load
# MAGIC Read common record format file from extract job.
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
    StringType,
    IntegerType,
    TimestampType,
    DecimalType
)
from pyspark.sql.functions import (
    col,
    lit,
    row_number,
    monotonically_increasing_id,
    rpad
)
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


InFile = get_widget_value('InFile','IdsVbbRwrdExtr.VbbRwrd.dat.100')
Logging = get_widget_value('Logging','X')
RunID = get_widget_value('RunID','100')

schema_IdsVbbPlnExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38,10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("VBB_RWRD_SK", IntegerType(), False),
    StructField("VBB_VNDR_UNIQ_KEY", IntegerType(), False),
    StructField("VBB_VNDR_RWRD_SEQ_NO", IntegerType(), False),
    StructField("SRC_SYS_CD_SK", IntegerType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("VBB_RWRD_BEG_DT_SK", StringType(), False),
    StructField("VBB_RWRD_END_DT_SK", StringType(), False),
    StructField("SRC_SYS_CRT_DTM", TimestampType(), False),
    StructField("SRC_SYS_UPDT_DTM", TimestampType(), False),
    StructField("VBB_RWRD_DESC", IntegerType(), False),
    StructField("VBB_RWRD_TYP_NM", StringType(), False),
    StructField("VBB_VNDR_NM", StringType(), False),
])

df_key = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "\"")
    .schema(schema_IdsVbbPlnExtr)
    .load(f"{adls_path}/key/{InFile}")
)

w = Window.orderBy(monotonically_increasing_id())
df_stagevars = (
    df_key
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn("ErrCount", GetFkeyErrorCnt(col("VBB_RWRD_SK")))
    .withColumn("SvSrcSysCrtDt", FORMAT.DATE(col("SRC_SYS_CRT_DTM"), lit("SYBASE"), lit("TIMESTAMP"), lit("DB2TIMESTAMP")))
    .withColumn("SvSrcSysUpdDt", FORMAT.DATE(col("SRC_SYS_UPDT_DTM"), lit("SYBASE"), lit("TIMESTAMP"), lit("DB2TIMESTAMP")))
    .withColumn("rownum", row_number().over(w))
)

df_foreignkey_fkey = df_stagevars.filter((col("ErrCount") == 0) | (col("PassThru") == 'Y')).select(
    col("VBB_RWRD_SK").alias("VBB_RWRD_SK"),
    col("VBB_VNDR_UNIQ_KEY").alias("VBB_VNDR_UNIQ_KEY"),
    col("VBB_VNDR_RWRD_SEQ_NO").alias("VBB_VNDR_RWRD_SEQ_NO"),
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(col("VBB_RWRD_BEG_DT_SK"), 10, " ").alias("VBB_RWRD_BEG_DT_SK"),
    rpad(col("VBB_RWRD_END_DT_SK"), 10, " ").alias("VBB_RWRD_END_DT_SK"),
    col("SvSrcSysCrtDt").alias("SRC_SYS_CRT_DTM"),
    col("SvSrcSysUpdDt").alias("SRC_SYS_UPDT_DTM"),
    col("VBB_RWRD_DESC").alias("VBB_RWRD_DESC"),
    col("VBB_RWRD_TYP_NM").alias("VBB_RWRD_TYP_NM"),
    col("VBB_VNDR_NM").alias("VBB_VNDR_NM")
)

df_foreignkey_recycle = df_stagevars.filter(col("ErrCount") > 0).select(
    GetRecycleKey(col("VBB_RWRD_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    col("ErrCount").alias("ERR_CT"),
    (col("RECYCLE_CT") + lit(1)).alias("RECYCLE_CT"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    col("VBB_RWRD_SK").alias("VBB_RWRD_SK"),
    col("VBB_VNDR_UNIQ_KEY").alias("VBB_VNDR_UNIQ_KEY"),
    col("VBB_VNDR_RWRD_SEQ_NO").alias("VBB_VNDR_RWRD_SEQ_NO"),
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(col("VBB_RWRD_BEG_DT_SK"), 10, " ").alias("VBB_RWRD_BEG_DT_SK"),
    rpad(col("VBB_RWRD_END_DT_SK"), 10, " ").alias("VBB_RWRD_END_DT_SK"),
    col("SRC_SYS_CRT_DTM").alias("SRC_SYS_CRT_DTM"),
    col("SRC_SYS_UPDT_DTM").alias("SRC_SYS_UPDT_DTM"),
    col("VBB_RWRD_DESC").alias("VBB_RWRD_DESC"),
    col("VBB_RWRD_TYP_NM").alias("VBB_RWRD_TYP_NM"),
    col("VBB_VNDR_NM").alias("VBB_VNDR_NM")
)

df_foreignkey_defaultunk = df_stagevars.filter(col("rownum") == 1).select(
    lit(0).alias("VBB_RWRD_SK"),
    lit(0).alias("VBB_VNDR_UNIQ_KEY"),
    lit(0).alias("VBB_VNDR_RWRD_SEQ_NO"),
    lit(0).alias("SRC_SYS_CD_SK"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(lit("UNK"), 10, " ").alias("VBB_RWRD_BEG_DT_SK"),
    rpad(lit("UNK"), 10, " ").alias("VBB_RWRD_END_DT_SK"),
    lit("1753-01-01-00.00.00.000000").alias("SRC_SYS_CRT_DTM"),
    lit("1753-01-01-00.00.00.000000").alias("SRC_SYS_UPDT_DTM"),
    lit(0).alias("VBB_RWRD_DESC"),
    lit("UNK").alias("VBB_RWRD_TYP_NM"),
    lit("UNK").alias("VBB_VNDR_NM")
)

df_foreignkey_defaultna = df_stagevars.filter(col("rownum") == 1).select(
    lit(1).alias("VBB_RWRD_SK"),
    lit(1).alias("VBB_VNDR_UNIQ_KEY"),
    lit(1).alias("VBB_VNDR_RWRD_SEQ_NO"),
    lit(1).alias("SRC_SYS_CD_SK"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(lit("NA"), 10, " ").alias("VBB_RWRD_BEG_DT_SK"),
    rpad(lit("NA"), 10, " ").alias("VBB_RWRD_END_DT_SK"),
    lit("1753-01-01-00.00.00.000000").alias("SRC_SYS_CRT_DTM"),
    lit("1753-01-01-00.00.00.000000").alias("SRC_SYS_UPDT_DTM"),
    lit(1).alias("VBB_RWRD_DESC"),
    lit("NA").alias("VBB_RWRD_TYP_NM"),
    lit("NA").alias("VBB_VNDR_NM")
)

write_files(
    df_foreignkey_recycle.select(
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "PASS_THRU_IN",
        "FIRST_RECYC_DT",
        "ERR_CT",
        "RECYCLE_CT",
        "SRC_SYS_CD",
        "PRI_KEY_STRING",
        "VBB_RWRD_SK",
        "VBB_VNDR_UNIQ_KEY",
        "VBB_VNDR_RWRD_SEQ_NO",
        "SRC_SYS_CD_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "VBB_RWRD_BEG_DT_SK",
        "VBB_RWRD_END_DT_SK",
        "SRC_SYS_CRT_DTM",
        "SRC_SYS_UPDT_DTM",
        "VBB_RWRD_DESC",
        "VBB_RWRD_TYP_NM",
        "VBB_VNDR_NM"
    ),
    f"{adls_path}/hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=False,
    quote="\"",
    nullValue=None
)

df_collector = (
    df_foreignkey_fkey.unionByName(df_foreignkey_defaultunk)
    .unionByName(df_foreignkey_defaultna)
)

df_final = df_collector.select(
    "VBB_RWRD_SK",
    "VBB_VNDR_UNIQ_KEY",
    "VBB_VNDR_RWRD_SEQ_NO",
    "SRC_SYS_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "VBB_RWRD_BEG_DT_SK",
    "VBB_RWRD_END_DT_SK",
    "SRC_SYS_CRT_DTM",
    "SRC_SYS_UPDT_DTM",
    "VBB_RWRD_DESC",
    "VBB_RWRD_TYP_NM",
    "VBB_VNDR_NM"
)

write_files(
    df_final,
    f"{adls_path}/load/VBB_RWRD.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)