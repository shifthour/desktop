# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2010 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by: IdsEvtLoadSeq
# MAGIC 
# MAGIC Processing : Foreign Key job for IDS EVT_LOC
# MAGIC                     
# MAGIC Modifications:                         
# MAGIC                                                    Project/                                                                                                                                                    Code                        Date
# MAGIC Developer             Date              Altiris #        Change Description                                                                                                               Reviewer                  Reviewed
# MAGIC ---------------------------  -------------------   -----------------  ---------------------------------------------------------------------------------------------------                                             ---------------------------     -------------------   
# MAGIC Kalyan Neelam      2010-11-02    4529            Initial Programming                                                                            IntegrateNewDevl       SAndrew                           12/07/2010

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
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DecimalType
from pyspark.sql.functions import col, lit, row_number, rpad
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


Logging = get_widget_value("Logging","X")
InFile = get_widget_value("InFile","")
SrcSysCdSk = get_widget_value("SrcSysCdSk","")
SrcSysCd = get_widget_value("SrcSysCd","")
RunCycle = get_widget_value("RunCycle","")

schema_IdsEvtLocExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38,10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("EVT_LOC_SK", IntegerType(), nullable=False),
    StructField("EVT_LOC_ID", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("EFF_DT_SK", StringType(), nullable=False),
    StructField("TERM_DT_SK", StringType(), nullable=False),
    StructField("EVT_LOC_CNTCT_NM", StringType(), nullable=False),
    StructField("EVT_LOC_NM", StringType(), nullable=False),
    StructField("EVT_LOC_ADDR_LN_1", StringType(), nullable=False),
    StructField("EVT_LOC_ROOM_ID", StringType(), nullable=False),
    StructField("EVT_LOC_CITY_NM", StringType(), nullable=False),
    StructField("LOC_ST_CD", StringType(), nullable=False),
    StructField("EVT_LOC_ZIP_CD", StringType(), nullable=False),
    StructField("EVT_LOC_PHN_NO", StringType(), nullable=False),
    StructField("LAST_UPDT_DTM", TimestampType(), nullable=False),
    StructField("LAST_UPDT_USER_ID", StringType(), nullable=False)
])

df_idsEvtLocExtr = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "\"")
    .schema(schema_IdsEvtLocExtr)
    .load(f"{adls_path}/key/{InFile}")
)

df_foreignkey = (
    df_idsEvtLocExtr
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn("svEffDtSk", GetFkeyDate(lit("IDS"), col("EVT_LOC_SK"), col("EFF_DT_SK"), lit(Logging)))
    .withColumn("svTermDtSk", GetFkeyDate(lit("IDS"), col("EVT_LOC_SK"), col("TERM_DT_SK"), lit(Logging)))
    .withColumn("svEvtLocStCdSk", GetFkeyCodes(lit("IDS"), col("EVT_LOC_SK"), lit("STATE"), col("LOC_ST_CD"), lit(Logging)))
    .withColumn("ErrCount", GetFkeyErrorCnt(col("EVT_LOC_SK")))
)

w = Window.orderBy(lit(1))
df_foreignkey_with_rnum = df_foreignkey.withColumn("INROWNUM", row_number().over(w))

df_fkey = (
    df_foreignkey
    .filter((col("ErrCount") == 0) | (col("PassThru") == "Y"))
    .select(
        col("EVT_LOC_SK").alias("EVT_LOC_SK"),
        col("EVT_LOC_ID").alias("EVT_LOC_ID"),
        lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("svEffDtSk").alias("EFF_DT_SK"),
        col("svTermDtSk").alias("TERM_DT_SK"),
        col("EVT_LOC_CNTCT_NM").alias("EVT_LOC_CNTCT_NM"),
        col("EVT_LOC_NM").alias("EVT_LOC_NM"),
        col("EVT_LOC_ADDR_LN_1").alias("EVT_LOC_ADDR_LN_1"),
        col("EVT_LOC_ROOM_ID").alias("EVT_LOC_ROOM_ID"),
        col("EVT_LOC_CITY_NM").alias("EVT_LOC_CITY_NM"),
        col("svEvtLocStCdSk").alias("EVT_LOC_ST_CD_SK"),
        col("EVT_LOC_ZIP_CD").alias("EVT_LOC_ZIP_CD"),
        col("EVT_LOC_PHN_NO").alias("EVT_LOC_PHN_NO"),
        col("LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
        col("LAST_UPDT_USER_ID").alias("LAST_UPDT_USER_ID")
    )
)

df_recycle = (
    df_foreignkey
    .filter(col("ErrCount") > 0)
    .select(
        GetRecycleKey(col("EVT_LOC_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        col("DISCARD_IN").alias("DISCARD_IN"),
        col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        col("ErrCount").alias("ERR_CT"),
        (col("RECYCLE_CT") + lit(1)).alias("RECYCLE_CT"),
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        col("EVT_LOC_SK").alias("EVT_LOC_SK"),
        col("EVT_LOC_ID").alias("EVT_LOC_ID"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("EFF_DT_SK").alias("EFF_DT_SK"),
        col("TERM_DT_SK").alias("TERM_DT_SK"),
        col("EVT_LOC_CNTCT_NM").alias("EVT_LOC_CNTCT_NM"),
        col("EVT_LOC_NM").alias("EVT_LOC_NM"),
        col("EVT_LOC_ADDR_LN_1").alias("EVT_LOC_ADDR_LN_1"),
        col("EVT_LOC_ROOM_ID").alias("EVT_LOC_ROOM_ID"),
        col("EVT_LOC_CITY_NM").alias("EVT_LOC_CITY_NM"),
        col("LOC_ST_CD").alias("LOC_ST_CD"),
        col("EVT_LOC_ZIP_CD").alias("EVT_LOC_ZIP_CD"),
        col("EVT_LOC_PHN_NO").alias("EVT_LOC_PHN_NO"),
        col("LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
        col("LAST_UPDT_USER_ID").alias("LAST_UPDT_USER_ID")
    )
)

df_recycle_char_prep = (
    df_recycle
    .withColumn("INSRT_UPDT_CD", rpad(col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", rpad(col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", rpad(col("PASS_THRU_IN"), 1, " "))
    .withColumn("EFF_DT_SK", rpad(col("EFF_DT_SK"), 10, " "))
    .withColumn("TERM_DT_SK", rpad(col("TERM_DT_SK"), 10, " "))
    .withColumn("EVT_LOC_ZIP_CD", rpad(col("EVT_LOC_ZIP_CD"), 5, " "))
    .withColumn("SRC_SYS_CD", rpad(col("SRC_SYS_CD"), <...>, " "))
    .withColumn("PRI_KEY_STRING", rpad(col("PRI_KEY_STRING"), <...>, " "))
    .withColumn("EVT_LOC_ID", rpad(col("EVT_LOC_ID"), <...>, " "))
    .withColumn("EVT_LOC_CNTCT_NM", rpad(col("EVT_LOC_CNTCT_NM"), <...>, " "))
    .withColumn("EVT_LOC_NM", rpad(col("EVT_LOC_NM"), <...>, " "))
    .withColumn("EVT_LOC_ADDR_LN_1", rpad(col("EVT_LOC_ADDR_LN_1"), <...>, " "))
    .withColumn("EVT_LOC_ROOM_ID", rpad(col("EVT_LOC_ROOM_ID"), <...>, " "))
    .withColumn("EVT_LOC_CITY_NM", rpad(col("EVT_LOC_CITY_NM"), <...>, " "))
    .withColumn("LOC_ST_CD", rpad(col("LOC_ST_CD"), <...>, " "))
    .withColumn("EVT_LOC_PHN_NO", rpad(col("EVT_LOC_PHN_NO"), <...>, " "))
    .withColumn("LAST_UPDT_USER_ID", rpad(col("LAST_UPDT_USER_ID"), <...>, " "))
)

df_recycle_final = df_recycle_char_prep.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "EVT_LOC_SK",
    "EVT_LOC_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "EFF_DT_SK",
    "TERM_DT_SK",
    "EVT_LOC_CNTCT_NM",
    "EVT_LOC_NM",
    "EVT_LOC_ADDR_LN_1",
    "EVT_LOC_ROOM_ID",
    "EVT_LOC_CITY_NM",
    "LOC_ST_CD",
    "EVT_LOC_ZIP_CD",
    "EVT_LOC_PHN_NO",
    "LAST_UPDT_DTM",
    "LAST_UPDT_USER_ID"
)

write_files(
    df_recycle_final,
    "hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_defaultNA = (
    df_foreignkey_with_rnum
    .filter(col("INROWNUM") == 1)
    .select(
        lit(1).alias("EVT_LOC_SK"),
        lit("NA").alias("EVT_LOC_ID"),
        lit(1).alias("SRC_SYS_CD_SK"),
        lit(RunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit("1753-01-01").alias("EFF_DT_SK"),
        lit("2199-12-31").alias("TERM_DT_SK"),
        lit("NA").alias("EVT_LOC_CNTCT_NM"),
        lit("NA").alias("EVT_LOC_NM"),
        lit("NA").alias("EVT_LOC_ADDR_LN_1"),
        lit("NA").alias("EVT_LOC_ROOM_ID"),
        lit("NA").alias("EVT_LOC_CITY_NM"),
        lit(1).alias("EVT_LOC_ST_CD_SK"),
        lit("NA").alias("EVT_LOC_ZIP_CD"),
        lit("NA").alias("EVT_LOC_PHN_NO"),
        lit("1753-01-01-00.00.00.000000").alias("LAST_UPDT_DTM"),
        lit("NA").alias("LAST_UPDT_USER_ID")
    )
)

df_defaultUNK = (
    df_foreignkey_with_rnum
    .filter(col("INROWNUM") == 1)
    .select(
        lit(0).alias("EVT_LOC_SK"),
        lit("UNK").alias("EVT_LOC_ID"),
        lit(0).alias("SRC_SYS_CD_SK"),
        lit(RunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit("1753-01-01").alias("EFF_DT_SK"),
        lit("2199-12-31").alias("TERM_DT_SK"),
        lit("UNK").alias("EVT_LOC_CNTCT_NM"),
        lit("UNK").alias("EVT_LOC_NM"),
        lit("UNK").alias("EVT_LOC_ADDR_LN_1"),
        lit("UNK").alias("EVT_LOC_ROOM_ID"),
        lit("UNK").alias("EVT_LOC_CITY_NM"),
        lit(0).alias("EVT_LOC_ST_CD_SK"),
        lit("UNK").alias("EVT_LOC_ZIP_CD"),
        lit("UNK").alias("EVT_LOC_PHN_NO"),
        lit("1753-01-01-00.00.00.000000").alias("LAST_UPDT_DTM"),
        lit("UNK").alias("LAST_UPDT_USER_ID")
    )
)

df_collector_fkey = df_fkey.select(
    "EVT_LOC_SK",
    "EVT_LOC_ID",
    "SRC_SYS_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "EFF_DT_SK",
    "TERM_DT_SK",
    "EVT_LOC_CNTCT_NM",
    "EVT_LOC_NM",
    "EVT_LOC_ADDR_LN_1",
    "EVT_LOC_ROOM_ID",
    "EVT_LOC_CITY_NM",
    "EVT_LOC_ST_CD_SK",
    "EVT_LOC_ZIP_CD",
    "EVT_LOC_PHN_NO",
    "LAST_UPDT_DTM",
    "LAST_UPDT_USER_ID"
)

df_collector_na = df_defaultNA.select(
    "EVT_LOC_SK",
    "EVT_LOC_ID",
    "SRC_SYS_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "EFF_DT_SK",
    "TERM_DT_SK",
    "EVT_LOC_CNTCT_NM",
    "EVT_LOC_NM",
    "EVT_LOC_ADDR_LN_1",
    "EVT_LOC_ROOM_ID",
    "EVT_LOC_CITY_NM",
    "EVT_LOC_ST_CD_SK",
    "EVT_LOC_ZIP_CD",
    "EVT_LOC_PHN_NO",
    "LAST_UPDT_DTM",
    "LAST_UPDT_USER_ID"
)

df_collector_unk = df_defaultUNK.select(
    "EVT_LOC_SK",
    "EVT_LOC_ID",
    "SRC_SYS_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "EFF_DT_SK",
    "TERM_DT_SK",
    "EVT_LOC_CNTCT_NM",
    "EVT_LOC_NM",
    "EVT_LOC_ADDR_LN_1",
    "EVT_LOC_ROOM_ID",
    "EVT_LOC_CITY_NM",
    "EVT_LOC_ST_CD_SK",
    "EVT_LOC_ZIP_CD",
    "EVT_LOC_PHN_NO",
    "LAST_UPDT_DTM",
    "LAST_UPDT_USER_ID"
)

df_collector = df_collector_fkey.unionByName(df_collector_na).unionByName(df_collector_unk)

df_evt_loc = (
    df_collector
    .withColumn("EVT_LOC_ID", rpad(col("EVT_LOC_ID"), <...>, " "))
    .withColumn("EFF_DT_SK", rpad(col("EFF_DT_SK"), 10, " "))
    .withColumn("TERM_DT_SK", rpad(col("TERM_DT_SK"), 10, " "))
    .withColumn("EVT_LOC_CNTCT_NM", rpad(col("EVT_LOC_CNTCT_NM"), <...>, " "))
    .withColumn("EVT_LOC_NM", rpad(col("EVT_LOC_NM"), <...>, " "))
    .withColumn("EVT_LOC_ADDR_LN_1", rpad(col("EVT_LOC_ADDR_LN_1"), <...>, " "))
    .withColumn("EVT_LOC_ROOM_ID", rpad(col("EVT_LOC_ROOM_ID"), <...>, " "))
    .withColumn("EVT_LOC_CITY_NM", rpad(col("EVT_LOC_CITY_NM"), <...>, " "))
    .withColumn("EVT_LOC_ST_CD_SK", rpad(col("EVT_LOC_ST_CD_SK"), <...>, " "))
    .withColumn("EVT_LOC_ZIP_CD", rpad(col("EVT_LOC_ZIP_CD"), 5, " "))
    .withColumn("EVT_LOC_PHN_NO", rpad(col("EVT_LOC_PHN_NO"), <...>, " "))
    .withColumn("LAST_UPDT_USER_ID", rpad(col("LAST_UPDT_USER_ID"), <...>, " "))
)

df_evt_loc_final = df_evt_loc.select(
    "EVT_LOC_SK",
    "EVT_LOC_ID",
    "SRC_SYS_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "EFF_DT_SK",
    "TERM_DT_SK",
    "EVT_LOC_CNTCT_NM",
    "EVT_LOC_NM",
    "EVT_LOC_ADDR_LN_1",
    "EVT_LOC_ROOM_ID",
    "EVT_LOC_CITY_NM",
    "EVT_LOC_ST_CD_SK",
    "EVT_LOC_ZIP_CD",
    "EVT_LOC_PHN_NO",
    "LAST_UPDT_DTM",
    "LAST_UPDT_USER_ID"
)

write_files(
    df_evt_loc_final,
    f"{adls_path}/load/EVT_LOC.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)