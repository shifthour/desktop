# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2010 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by: IdsEvtLoadSeq
# MAGIC 
# MAGIC Processing : Foreign Key job for IDS EVT_STAFF_TYP
# MAGIC                     
# MAGIC Modifications:                         
# MAGIC                                                    Project/                                                                                                            Code                        Date
# MAGIC Developer             Date              Altiris #        Change Description                                                                      Reviewer                  Reviewed
# MAGIC ---------------------------  -------------------   -----------------  ---------------------------------------------------------------------------------------------------   ---------------------------     -------------------   
# MAGIC Kalyan Neelam      2010-11-02    4529            Initial Programming                                                                              IntegrateNewDevl       SAndrew                           12/07/2010

# MAGIC Set all foreign surrogate keys
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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DecimalType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


Logging = get_widget_value('Logging','X')
InFile = get_widget_value('InFile','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
RunCycle = get_widget_value('RunCycle','100')

schema_IdsEvtStaffTypExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38,10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("EVT_STAFF_TYP_SK", IntegerType(), False),
    StructField("EVT_STAFF_TYP_ID", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("EVT_STAFF_TYP_DESC", StringType(), False),
    StructField("LAST_UPDT_DTM", TimestampType(), False),
    StructField("LAST_UPDT_USER_ID", StringType(), False)
])

df_IdsEvtStaffTypExtr = (
    spark.read
    .option("quote", "\"")
    .option("sep", ",")
    .schema(schema_IdsEvtStaffTypExtr)
    .csv(f"{adls_path}/key/{InFile}", header=False)
)

df_ForeignKey_pre = (
    df_IdsEvtStaffTypExtr
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("EVT_STAFF_TYP_SK")))
)

w = Window.orderBy(F.lit(1))
df_ForeignKey_pre = df_ForeignKey_pre.withColumn("_row_num", F.row_number().over(w))

df_ForeignKey_Fkey = df_ForeignKey_pre.filter(
    (F.col("ErrCount") == 0) | (F.col("PassThru") == "Y")
).select(
    F.col("EVT_STAFF_TYP_SK").alias("EVT_STAFF_TYP_SK"),
    F.col("EVT_STAFF_TYP_ID").alias("EVT_STAFF_TYP_ID"),
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("EVT_STAFF_TYP_DESC").alias("EVT_STAFF_TYP_DESC"),
    F.col("LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
    F.col("LAST_UPDT_USER_ID").alias("LAST_UPDT_USER_ID")
)

df_ForeignKey_Recycle = df_ForeignKey_pre.filter(
    F.col("ErrCount") > 0
).select(
    GetRecycleKey(F.col("EVT_STAFF_TYP_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ErrCount").alias("ERR_CT"),
    (F.col("RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("EVT_STAFF_TYP_SK").alias("EVT_STAFF_TYP_SK"),
    F.col("EVT_STAFF_TYP_ID").alias("EVT_STAFF_TYP_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("EVT_STAFF_TYP_DESC").alias("EVT_STAFF_TYP_DESC"),
    F.col("LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
    F.col("LAST_UPDT_USER_ID").alias("LAST_UPDT_USER_ID")
)

df_ForeignKey_DefaultNA = df_ForeignKey_pre.filter(
    F.col("_row_num") == 1
).select(
    F.lit(1).alias("EVT_STAFF_TYP_SK"),
    F.lit("NA").alias("EVT_STAFF_TYP_ID"),
    F.lit(1).alias("SRC_SYS_CD_SK"),
    F.lit(RunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit("NA").alias("EVT_STAFF_TYP_DESC"),
    F.lit("1753-01-01-00.00.00.000000").alias("LAST_UPDT_DTM"),
    F.lit("NA").alias("LAST_UPDT_USER_ID")
)

df_ForeignKey_DefaultUNK = df_ForeignKey_pre.filter(
    F.col("_row_num") == 1
).select(
    F.lit(0).alias("EVT_STAFF_TYP_SK"),
    F.lit("UNK").alias("EVT_STAFF_TYP_ID"),
    F.lit(0).alias("SRC_SYS_CD_SK"),
    F.lit(RunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit("UNK").alias("EVT_STAFF_TYP_DESC"),
    F.lit("1753-01-01-00.00.00.000000").alias("LAST_UPDT_DTM"),
    F.lit("UNK").alias("LAST_UPDT_USER_ID")
)

write_files(
    df_ForeignKey_Recycle,
    f"{adls_path}/hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_Collector_Fkey = df_ForeignKey_Fkey.select(
    "EVT_STAFF_TYP_SK",
    "EVT_STAFF_TYP_ID",
    "SRC_SYS_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "EVT_STAFF_TYP_DESC",
    "LAST_UPDT_DTM",
    "LAST_UPDT_USER_ID"
)

df_Collector_DefaultNA = df_ForeignKey_DefaultNA.select(
    "EVT_STAFF_TYP_SK",
    "EVT_STAFF_TYP_ID",
    "SRC_SYS_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "EVT_STAFF_TYP_DESC",
    "LAST_UPDT_DTM",
    "LAST_UPDT_USER_ID"
)

df_Collector_DefaultUNK = df_ForeignKey_DefaultUNK.select(
    "EVT_STAFF_TYP_SK",
    "EVT_STAFF_TYP_ID",
    "SRC_SYS_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "EVT_STAFF_TYP_DESC",
    "LAST_UPDT_DTM",
    "LAST_UPDT_USER_ID"
)

df_Collector = df_Collector_Fkey.union(df_Collector_DefaultNA).union(df_Collector_DefaultUNK)

df_final = df_Collector.select(
    "EVT_STAFF_TYP_SK",
    "EVT_STAFF_TYP_ID",
    "SRC_SYS_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "EVT_STAFF_TYP_DESC",
    "LAST_UPDT_DTM",
    "LAST_UPDT_USER_ID"
)

write_files(
    df_final,
    f"{adls_path}/load/EVT_STAFF_TYP.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)