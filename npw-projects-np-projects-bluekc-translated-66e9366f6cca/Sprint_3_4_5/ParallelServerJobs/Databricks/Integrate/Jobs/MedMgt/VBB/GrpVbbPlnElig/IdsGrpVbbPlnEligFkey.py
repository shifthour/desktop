# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2013 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  
# MAGIC 
# MAGIC PROCESSING:   Assigns Foreign Keys for the IDS table GRP_VBB_PLN_ELIG
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 	Project/Altiris #	Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------	------------------------	-----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Kalyan Neelam         2013-06-11          4963 VBB Phase III     Initial Programming                                                                      IntegrateNewDevl

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
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DecimalType
from pyspark.sql.functions import col, lit, coalesce, row_number, rpad
from pyspark.sql import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


InFile = get_widget_value("InFile","")
Logging = get_widget_value("Logging","")
RunID = get_widget_value("RunID","")

schema_IdsGrpVbbPlnEligExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38,10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("VBB_PLN_ELIG_SK", IntegerType(), False),
    StructField("VBB_PLN_UNIQ_KEY", IntegerType(), False),
    StructField("GRP_ID", StringType(), False),
    StructField("CLS_ID", StringType(), False),
    StructField("CLS_PLN_ID", StringType(), False),
    StructField("SRC_SYS_CD_SK", IntegerType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("VBB_PLN_ELIG_STRT_DT", StringType(), False),
    StructField("VBB_PLN_ELIG_END_DT", StringType(), False)
])

df_IdsGrpVbbPlnEligExtr = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "\"")
    .schema(schema_IdsGrpVbbPlnEligExtr)
    .load(f"{adls_path}/key/{InFile}")
)

df_ForeignKey = (
    df_IdsGrpVbbPlnEligExtr
    .withColumn("PassThru", coalesce(col("PASS_THRU_IN"), lit("Y")))
    .withColumn("svGrpSk", GetFkeyGrp(lit("FACETS"), col("VBB_PLN_ELIG_SK"), col("GRP_ID"), lit(Logging)))
    .withColumn("svClsSk", GetFkeyCls(lit("FACETS"), col("VBB_PLN_ELIG_SK"), col("GRP_ID"), col("CLS_ID"), lit(Logging)))
    .withColumn("svClsPlnSk", GetFkeyClsPln(lit("FACETS"), col("VBB_PLN_ELIG_SK"), col("CLS_PLN_ID"), lit(Logging)))
    .withColumn("svVbbPlnSk", GetFkeyVbbPln(col("SRC_SYS_CD"), col("VBB_PLN_ELIG_SK"), col("VBB_PLN_UNIQ_KEY"), lit(Logging)))
    .withColumn("svVbbPlneLIGStrtDtSk", coalesce(GetFkeyDate(lit("IDS"), col("VBB_PLN_ELIG_SK"), col("VBB_PLN_ELIG_STRT_DT"), lit(Logging)), lit("-1")))
    .withColumn("svVbbPlneLIGEndDtSk", coalesce(GetFkeyDate(lit("IDS"), col("VBB_PLN_ELIG_SK"), col("VBB_PLN_ELIG_END_DT"), lit(Logging)), lit("-1")))
    .withColumn("ErrCount", coalesce(GetFkeyErrorCnt(col("VBB_PLN_ELIG_SK")), lit("-1")))
)

df_ForeignKeyWithRowNum = df_ForeignKey.withColumn("rownum", row_number().over(Window.orderBy(lit(1))))

df_fkey = (
    df_ForeignKeyWithRowNum
    .filter((col("ErrCount") == 0) | (col("PassThru") == "Y"))
    .select(
        col("VBB_PLN_ELIG_SK").alias("VBB_PLN_ELIG_SK"),
        col("VBB_PLN_UNIQ_KEY").alias("VBB_PLN_UNIQ_KEY"),
        col("GRP_ID").alias("GRP_ID"),
        col("CLS_ID").alias("CLS_ID"),
        col("CLS_PLN_ID").alias("CLS_PLN_ID"),
        col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("svClsSk").alias("CLS_SK"),
        col("svClsPlnSk").alias("CLS_PLN_SK"),
        col("svGrpSk").alias("GRP_SK"),
        col("svVbbPlnSk").alias("VBB_PLN_SK"),
        col("svVbbPlneLIGStrtDtSk").alias("VBB_PLN_ELIG_STRT_DT_SK"),
        col("svVbbPlneLIGEndDtSk").alias("VBB_PLN_ELIG_END_DT_SK")
    )
)

df_recycle_pre = df_ForeignKeyWithRowNum.filter(col("ErrCount") > 0)
df_recycle = (
    df_recycle_pre
    .select(
        GetRecycleKey(col("VBB_PLN_ELIG_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        col("DISCARD_IN").alias("DISCARD_IN"),
        col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        col("ErrCount").alias("ERR_CT"),
        (col("RECYCLE_CT") + lit(1)).alias("RECYCLE_CT"),
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        col("VBB_PLN_ELIG_SK").alias("VBB_PLN_ELIG_SK"),
        col("VBB_PLN_UNIQ_KEY").alias("VBB_PLN_UNIQ_KEY"),
        col("GRP_ID").alias("GRP_ID"),
        col("CLS_ID").alias("CLS_ID"),
        col("CLS_PLN_ID").alias("CLS_PLN_ID"),
        col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("VBB_PLN_ELIG_STRT_DT").alias("VBB_PLN_ELIG_STRT_DT"),
        col("VBB_PLN_ELIG_END_DT").alias("VBB_PLN_ELIG_END_DT")
    )
)

df_recycle_final = (
    df_recycle
    .withColumn("INSRT_UPDT_CD", rpad(col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", rpad(col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", rpad(col("PASS_THRU_IN"), 1, " "))
    .withColumn("VBB_PLN_ELIG_STRT_DT", rpad(col("VBB_PLN_ELIG_STRT_DT"), 10, " "))
    .withColumn("VBB_PLN_ELIG_END_DT", rpad(col("VBB_PLN_ELIG_END_DT"), 10, " "))
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

df_defaultUNK = (
    df_ForeignKeyWithRowNum
    .filter(col("rownum") == 1)
    .select(
        lit(0).alias("VBB_PLN_ELIG_SK"),
        lit(0).alias("VBB_PLN_UNIQ_KEY"),
        lit("UNK").alias("GRP_ID"),
        lit("UNK").alias("CLS_ID"),
        lit("UNK").alias("CLS_PLN_ID"),
        lit(0).alias("SRC_SYS_CD_SK"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(0).alias("CLS_SK"),
        lit(0).alias("CLS_PLN_SK"),
        lit(0).alias("GRP_SK"),
        lit(0).alias("VBB_PLN_SK"),
        lit("1753-01-01").alias("VBB_PLN_ELIG_STRT_DT_SK"),
        lit("2199-12-31").alias("VBB_PLN_ELIG_END_DT_SK")
    )
)

df_defaultNA = (
    df_ForeignKeyWithRowNum
    .filter(col("rownum") == 1)
    .select(
        lit(1).alias("VBB_PLN_ELIG_SK"),
        lit(1).alias("VBB_PLN_UNIQ_KEY"),
        lit("NA").alias("GRP_ID"),
        lit("NA").alias("CLS_ID"),
        lit("NA").alias("CLS_PLN_ID"),
        lit(1).alias("SRC_SYS_CD_SK"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(1).alias("CLS_SK"),
        lit(1).alias("CLS_PLN_SK"),
        lit(1).alias("GRP_SK"),
        lit(1).alias("VBB_PLN_SK"),
        lit("1753-01-01").alias("VBB_PLN_ELIG_STRT_DT_SK"),
        lit("2199-12-31").alias("VBB_PLN_ELIG_END_DT_SK")
    )
)

df_collector = (
    df_fkey.select(
        "VBB_PLN_ELIG_SK",
        "VBB_PLN_UNIQ_KEY",
        "GRP_ID",
        "CLS_ID",
        "CLS_PLN_ID",
        "SRC_SYS_CD_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CLS_SK",
        "CLS_PLN_SK",
        "GRP_SK",
        "VBB_PLN_SK",
        "VBB_PLN_ELIG_STRT_DT_SK",
        "VBB_PLN_ELIG_END_DT_SK"
    )
    .union(
        df_defaultUNK.select(
            "VBB_PLN_ELIG_SK",
            "VBB_PLN_UNIQ_KEY",
            "GRP_ID",
            "CLS_ID",
            "CLS_PLN_ID",
            "SRC_SYS_CD_SK",
            "CRT_RUN_CYC_EXCTN_SK",
            "LAST_UPDT_RUN_CYC_EXCTN_SK",
            "CLS_SK",
            "CLS_PLN_SK",
            "GRP_SK",
            "VBB_PLN_SK",
            "VBB_PLN_ELIG_STRT_DT_SK",
            "VBB_PLN_ELIG_END_DT_SK"
        )
    )
    .union(
        df_defaultNA.select(
            "VBB_PLN_ELIG_SK",
            "VBB_PLN_UNIQ_KEY",
            "GRP_ID",
            "CLS_ID",
            "CLS_PLN_ID",
            "SRC_SYS_CD_SK",
            "CRT_RUN_CYC_EXCTN_SK",
            "LAST_UPDT_RUN_CYC_EXCTN_SK",
            "CLS_SK",
            "CLS_PLN_SK",
            "GRP_SK",
            "VBB_PLN_SK",
            "VBB_PLN_ELIG_STRT_DT_SK",
            "VBB_PLN_ELIG_END_DT_SK"
        )
    )
)

df_collector_final = (
    df_collector
    .withColumn("VBB_PLN_ELIG_STRT_DT_SK", rpad(col("VBB_PLN_ELIG_STRT_DT_SK"), 10, " "))
    .withColumn("VBB_PLN_ELIG_END_DT_SK", rpad(col("VBB_PLN_ELIG_END_DT_SK"), 10, " "))
)

write_files(
    df_collector_final,
    f"{adls_path}/load/GRP_VBB_PLN_ELIG.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)