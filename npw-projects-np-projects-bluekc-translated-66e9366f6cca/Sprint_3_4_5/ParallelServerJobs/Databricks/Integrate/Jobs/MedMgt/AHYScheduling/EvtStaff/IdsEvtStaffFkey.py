# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2010 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by: IdsEvtLoadSeq
# MAGIC 
# MAGIC Processing : Foreign Key job for IDS EVT_STAFF
# MAGIC                     
# MAGIC Modifications:                         
# MAGIC                                                    Project/                                                                                                            Code                        Date
# MAGIC Developer             Date              Altiris #        Change Description                                                                      Reviewer                  Reviewed
# MAGIC ---------------------------  -------------------   -----------------  ---------------------------------------------------------------------------------------------------   ---------------------------     -------------------   
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
from pyspark.sql.functions import col, lit, row_number, monotonically_increasing_id, rpad
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


Logging = get_widget_value('Logging','X')
InFile = get_widget_value('InFile','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunCycle = get_widget_value('RunCycle','')

schema_IdsEvtStaffExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38,10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("EVT_STAFF_SK", IntegerType(), False),
    StructField("EVT_STAFF_ID", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("STAFF_TYP_ID", StringType(), False),
    StructField("EVT_STAFF_ACTV_IN", StringType(), False),
    StructField("EFF_DT_SK", StringType(), False),
    StructField("TERM_DT_SK", StringType(), False),
    StructField("EVT_STAFF_EMAIL_ADDR", StringType(), False),
    StructField("EVT_STAFF_NM", StringType(), False),
    StructField("EVT_STAFF_PHN_NO", StringType(), False),
    StructField("LAST_UPDT_DTM", TimestampType(), False),
    StructField("LAST_UPDT_USER_ID", StringType(), False)
])

df_IdsEvtStaffExtr = (
    spark.read
    .format("csv")
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "\"")
    .schema(schema_IdsEvtStaffExtr)
    .load(f"{adls_path}/key/" + InFile)
)

df_foreignkey_base = (
    df_IdsEvtStaffExtr
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn("svEvtStaffTypSk", GetFkeyEvtStaffTyp(SrcSysCd, col("EVT_STAFF_SK"), col("STAFF_TYP_ID"), Logging))
    .withColumn("svEffDtSk", GetFkeyDate('IDS', col("EVT_STAFF_SK"), col("EFF_DT_SK"), Logging))
    .withColumn("svTermDtSk", GetFkeyDate('IDS', col("EVT_STAFF_SK"), col("TERM_DT_SK"), Logging))
    .withColumn("ErrCount", GetFkeyErrorCnt(col("EVT_STAFF_SK")))
)

df_foreignkey_fkey = df_foreignkey_base.filter((col("ErrCount") == 0) | (col("PassThru") == 'Y'))
df_foreignkey_recycle = df_foreignkey_base.filter(col("ErrCount") > 0)

windowSpec = Window.orderBy(monotonically_increasing_id())
df_with_rn = df_foreignkey_base.withColumn("rownum", row_number().over(windowSpec))
df_foreignkey_defaultna = df_with_rn.filter(col("rownum") == 1)
df_foreignkey_defaultunk = df_with_rn.filter(col("rownum") == 1)

df_foreignkey_fkey_final = (
    df_foreignkey_fkey
    .select(
        col("EVT_STAFF_SK").alias("EVT_STAFF_SK"),
        col("EVT_STAFF_ID").alias("EVT_STAFF_ID"),
        col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("svEvtStaffTypSk").alias("EVT_STAFF_TYP_SK"),
        col("EVT_STAFF_ACTV_IN").alias("EVT_STAFF_ACTV_IN"),
        col("svEffDtSk").alias("EFF_DT_SK"),
        col("svTermDtSk").alias("TERM_DT_SK"),
        col("EVT_STAFF_EMAIL_ADDR").alias("EVT_STAFF_EMAIL_ADDR"),
        col("EVT_STAFF_NM").alias("EVT_STAFF_NM"),
        col("EVT_STAFF_PHN_NO").alias("EVT_STAFF_PHN_NO"),
        col("LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
        col("LAST_UPDT_USER_ID").alias("LAST_UPDT_USER_ID")
    )
)

df_foreignkey_recycle_final = (
    df_foreignkey_recycle
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", GetRecycleKey(col("EVT_STAFF_SK")))
    .withColumn("INSRT_UPDT_CD", col("INSRT_UPDT_CD"))
    .withColumn("DISCARD_IN", col("DISCARD_IN"))
    .withColumn("PASS_THRU_IN", col("PASS_THRU_IN"))
    .withColumn("FIRST_RECYC_DT", col("FIRST_RECYC_DT"))
    .withColumn("ERR_CT", col("ErrCount"))
    .withColumn("RECYCLE_CT", col("RECYCLE_CT") + lit(1))
    .withColumn("SRC_SYS_CD", col("SRC_SYS_CD"))
    .withColumn("PRI_KEY_STRING", col("PRI_KEY_STRING"))
    .withColumn("EVT_STAFF_SK", col("EVT_STAFF_SK"))
    .withColumn("EVT_STAFF_ID", col("EVT_STAFF_ID"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", col("CRT_RUN_CYC_EXCTN_SK"))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", col("LAST_UPDT_RUN_CYC_EXCTN_SK"))
    .withColumn("STAFF_TYP_ID", col("STAFF_TYP_ID"))
    .withColumn("EVT_STAFF_ACTV_IN", col("EVT_STAFF_ACTV_IN"))
    .withColumn("EFF_DT_SK", col("EFF_DT_SK"))
    .withColumn("TERM_DT_SK", col("TERM_DT_SK"))
    .withColumn("EVT_STAFF_EMAIL_ADDR", col("EVT_STAFF_EMAIL_ADDR"))
    .withColumn("EVT_STAFF_NM", col("EVT_STAFF_NM"))
    .withColumn("EVT_STAFF_PHN_NO", col("EVT_STAFF_PHN_NO"))
    .withColumn("LAST_UPDT_DTM", col("LAST_UPDT_DTM"))
    .withColumn("LAST_UPDT_USER_ID", col("LAST_UPDT_USER_ID"))
    .select(
        rpad(col("JOB_EXCTN_RCRD_ERR_SK").cast(StringType()), 0, " ").cast(IntegerType()).alias("JOB_EXCTN_RCRD_ERR_SK"),
        rpad(col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
        rpad(col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
        rpad(col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
        col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        col("ERR_CT").alias("ERR_CT"),
        col("RECYCLE_CT").alias("RECYCLE_CT"),
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        col("EVT_STAFF_SK").alias("EVT_STAFF_SK"),
        col("EVT_STAFF_ID").alias("EVT_STAFF_ID"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("STAFF_TYP_ID").alias("STAFF_TYP_ID"),
        rpad(col("EVT_STAFF_ACTV_IN"), 1, " ").alias("EVT_STAFF_ACTV_IN"),
        rpad(col("EFF_DT_SK"), 10, " ").alias("EFF_DT_SK"),
        rpad(col("TERM_DT_SK"), 10, " ").alias("TERM_DT_SK"),
        col("EVT_STAFF_EMAIL_ADDR").alias("EVT_STAFF_EMAIL_ADDR"),
        col("EVT_STAFF_NM").alias("EVT_STAFF_NM"),
        col("EVT_STAFF_PHN_NO").alias("EVT_STAFF_PHN_NO"),
        col("LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
        col("LAST_UPDT_USER_ID").alias("LAST_UPDT_USER_ID")
    )
)

write_files(
    df_foreignkey_recycle_final,
    "hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_parquet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_foreignkey_defaultna_final = (
    df_foreignkey_defaultna
    .select(
        lit(1).cast(IntegerType()).alias("EVT_STAFF_SK"),
        lit("NA").alias("EVT_STAFF_ID"),
        lit(1).cast(IntegerType()).alias("SRC_SYS_CD_SK"),
        lit(RunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(1).cast(IntegerType()).alias("EVT_STAFF_TYP_SK"),
        lit("X").alias("EVT_STAFF_ACTV_IN"),
        lit("1753-01-01").alias("EFF_DT_SK"),
        lit("2199-12-31").alias("TERM_DT_SK"),
        lit("NA").alias("EVT_STAFF_EMAIL_ADDR"),
        lit("NA").alias("EVT_STAFF_NM"),
        lit("NA").alias("EVT_STAFF_PHN_NO"),
        lit("1753-01-01-00.00.00.000000").alias("LAST_UPDT_DTM"),
        lit("NA").alias("LAST_UPDT_USER_ID")
    )
)

df_foreignkey_defaultunk_final = (
    df_foreignkey_defaultunk
    .select(
        lit(0).cast(IntegerType()).alias("EVT_STAFF_SK"),
        lit("UNK").alias("EVT_STAFF_ID"),
        lit(0).cast(IntegerType()).alias("SRC_SYS_CD_SK"),
        lit(RunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(0).cast(IntegerType()).alias("EVT_STAFF_TYP_SK"),
        lit("U").alias("EVT_STAFF_ACTV_IN"),
        lit("1753-01-01").alias("EFF_DT_SK"),
        lit("2199-12-31").alias("TERM_DT_SK"),
        lit("UNK").alias("EVT_STAFF_EMAIL_ADDR"),
        lit("UNK").alias("EVT_STAFF_NM"),
        lit("UNK").alias("EVT_STAFF_PHN_NO"),
        lit("1753-01-01-00.00.00.000000").alias("LAST_UPDT_DTM"),
        lit("UNK").alias("LAST_UPDT_USER_ID")
    )
)

df_collector = (
    df_foreignkey_fkey_final
    .unionByName(df_foreignkey_defaultna_final)
    .unionByName(df_foreignkey_defaultunk_final)
)

df_collector_final = df_collector.select(
    col("EVT_STAFF_SK"),
    col("EVT_STAFF_ID"),
    col("SRC_SYS_CD_SK"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("EVT_STAFF_TYP_SK"),
    rpad(col("EVT_STAFF_ACTV_IN"), 1, " ").alias("EVT_STAFF_ACTV_IN"),
    rpad(col("EFF_DT_SK"), 10, " ").alias("EFF_DT_SK"),
    rpad(col("TERM_DT_SK"), 10, " ").alias("TERM_DT_SK"),
    col("EVT_STAFF_EMAIL_ADDR"),
    col("EVT_STAFF_NM"),
    col("EVT_STAFF_PHN_NO"),
    col("LAST_UPDT_DTM"),
    col("LAST_UPDT_USER_ID")
)

write_files(
    df_collector_final,
    f"EVT_STAFF.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)