# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2010 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by: IdsEvtLoadSeq
# MAGIC 
# MAGIC Processing : Foreign Key job for IDS EVT_APPT_RSN
# MAGIC                     
# MAGIC Modifications:                         
# MAGIC                                                    Project/                                                                                                                       Development               Code                        Date
# MAGIC Developer             Date              Altiris #        Change Description                                                                                     Project                    Reviewer                  Reviewed
# MAGIC ---------------------------  -------------------   -----------------  ---------------------------------------------------------------------------------------------------      ---------------------------------      ---------------------------     -------------------   
# MAGIC Kalyan Neelam      2011-01-03    4529            Initial Programming                                                                            IntegrateNewDevl        Steph Goddard        01/10/2011

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
from pyspark.sql.functions import col, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


Logging = get_widget_value("Logging","X")
InFile = get_widget_value("InFile","")
SrcSysCdSk = get_widget_value("SrcSysCdSk","")
SrcSysCd = get_widget_value("SrcSysCd","")
RunCycle = get_widget_value("RunCycle","")

schema_IdsEvtApptRsnExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38, 10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("EVT_APPT_RSN_SK", IntegerType(), False),
    StructField("EVT_TYP_ID", StringType(), False),
    StructField("EVT_APPT_RSN_NM", StringType(), False),
    StructField("EFF_DT_SK", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_DTM", TimestampType(), False),
    StructField("TERM_DT_SK", StringType(), False),
    StructField("EVT_APPT_RSN_DESC", StringType(), False),
    StructField("EVT_APPT_RSN_ID", IntegerType(), False),
    StructField("LAST_UPDT_USER_ID", StringType(), False)
])

df_IdsEvtApptRsnExtr = (
    spark.read
    .option("header", False)
    .option("quote", "\"")
    .option("delimiter", ",")
    .schema(schema_IdsEvtApptRsnExtr)
    .csv(f"{adls_path}/key/{InFile}")
)

df_ForeignKeyIn = (
    df_IdsEvtApptRsnExtr
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn("ErrCount", GetFkeyErrorCnt(col("EVT_APPT_RSN_SK")))
    .withColumn("svEvtTypSk", GetFkeyEvtTyp(lit(SrcSysCd), col("EVT_APPT_RSN_SK"), col("EVT_TYP_ID"), lit(Logging)))
    .withColumn("svEffDtSk", GetFkeyDate(lit("IDS"), col("EVT_APPT_RSN_SK"), col("EFF_DT_SK"), lit(Logging)))
    .withColumn("svTermDtSk", GetFkeyDate(lit("IDS"), col("EVT_APPT_RSN_SK"), col("TERM_DT_SK"), lit(Logging)))
)

df_Fkey = (
    df_ForeignKeyIn
    .filter((col("ErrCount") == 0) | (col("PassThru") == "Y"))
    .select(
        col("EVT_APPT_RSN_SK").alias("EVT_APPT_RSN_SK"),
        col("EVT_TYP_ID").alias("EVT_TYP_ID"),
        col("EVT_APPT_RSN_NM").alias("EVT_APPT_RSN_NM"),
        col("svEffDtSk").alias("EFF_DT_SK"),
        lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("svEvtTypSk").alias("EVT_TYP_SK"),
        col("LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
        col("svTermDtSk").alias("TERM_DT_SK"),
        col("EVT_APPT_RSN_DESC").alias("EVT_APPT_RSN_DESC"),
        col("EVT_APPT_RSN_ID").alias("EVT_APPT_RSN_ID"),
        col("LAST_UPDT_USER_ID").alias("LAST_UPDT_USER_ID")
    )
)

df_Recycle = (
    df_ForeignKeyIn
    .filter(col("ErrCount") > 0)
    .select(
        GetRecycleKey(col("EVT_APPT_RSN_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        col("DISCARD_IN").alias("DISCARD_IN"),
        col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        col("ErrCount").alias("ERR_CT"),
        col("RECYCLE_CT").alias("RECYCLE_CT"),
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        col("EVT_APPT_RSN_SK").alias("EVT_APPT_RSN_SK"),
        col("EVT_TYP_ID").alias("EVT_TYP_ID"),
        col("EVT_APPT_RSN_NM").alias("EVT_APPT_RSN_NM"),
        col("EFF_DT_SK").alias("EFF_DT_SK"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
        col("TERM_DT_SK").alias("TERM_DT_SK"),
        col("EVT_APPT_RSN_DESC").alias("EVT_APPT_RSN_DESC"),
        col("EVT_APPT_RSN_ID").alias("EVT_APPT_RSN_ID"),
        col("LAST_UPDT_USER_ID").alias("LAST_UPDT_USER_ID")
    )
)

df_DefaultNA = spark.createDataFrame(
    [
        (
            1, "NA", "NA", "1753-01-01", 1, RunCycle, RunCycle, 1,
            "1753-01-01-00.00.00.000000", "2199-12-31", "NA", "NA", "NA"
        )
    ],
    [
        "EVT_APPT_RSN_SK",
        "EVT_TYP_ID",
        "EVT_APPT_RSN_NM",
        "EFF_DT_SK",
        "SRC_SYS_CD_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "EVT_TYP_SK",
        "LAST_UPDT_DTM",
        "TERM_DT_SK",
        "EVT_APPT_RSN_DESC",
        "EVT_APPT_RSN_ID",
        "LAST_UPDT_USER_ID"
    ]
)

df_DefaultUNK = spark.createDataFrame(
    [
        (
            0, "UNK", "UNK", "1753-01-01", 0, RunCycle, RunCycle, 0,
            "1753-01-01-00.00.00.000000", "2199-12-31", "UNK", "UNK", "UNK"
        )
    ],
    [
        "EVT_APPT_RSN_SK",
        "EVT_TYP_ID",
        "EVT_APPT_RSN_NM",
        "EFF_DT_SK",
        "SRC_SYS_CD_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "EVT_TYP_SK",
        "LAST_UPDT_DTM",
        "TERM_DT_SK",
        "EVT_APPT_RSN_DESC",
        "EVT_APPT_RSN_ID",
        "LAST_UPDT_USER_ID"
    ]
)

df_Collector = (
    df_Fkey
    .unionByName(df_DefaultNA)
    .unionByName(df_DefaultUNK)
)

df_Recycle_s = df_Recycle.select(
    col("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    col("SRC_SYS_CD"),
    col("PRI_KEY_STRING"),
    col("EVT_APPT_RSN_SK"),
    col("EVT_TYP_ID"),
    col("EVT_APPT_RSN_NM"),
    rpad(col("EFF_DT_SK"), 10, " ").alias("EFF_DT_SK"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_DTM"),
    rpad(col("TERM_DT_SK"), 10, " ").alias("TERM_DT_SK"),
    col("EVT_APPT_RSN_DESC"),
    col("EVT_APPT_RSN_ID"),
    col("LAST_UPDT_USER_ID")
)

write_files(
    df_Recycle_s,
    "hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_Collector_s = df_Collector.select(
    col("EVT_APPT_RSN_SK"),
    col("EVT_TYP_ID"),
    col("EVT_APPT_RSN_NM"),
    rpad(col("EFF_DT_SK"), 10, " ").alias("EFF_DT_SK"),
    col("SRC_SYS_CD_SK"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("EVT_TYP_SK"),
    col("LAST_UPDT_DTM"),
    rpad(col("TERM_DT_SK"), 10, " ").alias("TERM_DT_SK"),
    col("EVT_APPT_RSN_DESC"),
    col("EVT_APPT_RSN_ID"),
    col("LAST_UPDT_USER_ID")
)

write_files(
    df_Collector_s,
    f"{adls_path}/load/EVT_APPT_RSN.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)