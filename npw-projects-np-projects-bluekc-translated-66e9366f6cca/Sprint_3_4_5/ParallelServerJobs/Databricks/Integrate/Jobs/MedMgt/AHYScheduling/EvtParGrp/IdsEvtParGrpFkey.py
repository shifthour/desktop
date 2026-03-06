# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2010 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by: WebUserInfoAHYEvtSchedulingLoadSeq
# MAGIC 
# MAGIC Processing : Foreign Key job for WebUserInfoEvtParGrpExr
# MAGIC                     
# MAGIC Modifications:                         
# MAGIC                                                   
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Manasa Andru        2012-03-25        4830 - AHY 3.0      Initial Programming                                                                         IntegrateCurDevl             SAndrew                    2012-04-24

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
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType,
    TimestampType
)
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

Logging = get_widget_value("Logging", "X")
InFile = get_widget_value("InFile", "")
SrcSysCdSk = get_widget_value("SrcSysCdSk", "")
RunCycle = get_widget_value("RunCycle", "100")

schema_IdsEvtParGrpExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38, 10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("EVT_PAR_GRP_SK", IntegerType(), False),
    StructField("GRP_ID", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("GRP_ACTV_IN", StringType(), False),
    StructField("EFF_DT_SK", StringType(), False),
    StructField("TERM_DT_SK", StringType(), False),
    StructField("GRP_COLOR_KEY_DESC", StringType(), False),
    StructField("GRP_CNTCT_NM", StringType(), False),
    StructField("GRP_CNTCT_CELL_PHN_NO", StringType(), False),
    StructField("GRP_CNTCT_WORK_PHN_NO", StringType(), False),
    StructField("LAST_UPDT_DTM", TimestampType(), False),
    StructField("LAST_UPDT_USER_ID", StringType(), False)
])

df_IdsEvtParGrpExtr = (
    spark.read
    .schema(schema_IdsEvtParGrpExtr)
    .option("sep", ",")
    .option("quote", "\"")
    .csv(f"{adls_path}/key/{InFile}")
)

df_foreignKey = (
    df_IdsEvtParGrpExtr
    .withColumn("svEffDt", GetFkeyDate(F.lit("IDS"), F.col("EVT_PAR_GRP_SK"), F.col("EFF_DT_SK"), F.lit(Logging)))
    .withColumn("svTermDt", GetFkeyDate(F.lit("IDS"), F.col("EVT_PAR_GRP_SK"), F.col("TERM_DT_SK"), F.lit(Logging)))
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("EVT_PAR_GRP_SK")))
)

df_ForeignKey_Fkey = (
    df_foreignKey
    .filter((F.col("ErrCount") == 0) | (F.col("PassThru") == "Y"))
    .select(
        F.col("EVT_PAR_GRP_SK").alias("EVT_PAR_GRP_SK"),
        F.col("GRP_ID").alias("GRP_ID"),
        F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("GRP_ACTV_IN").alias("GRP_ACTV_IN"),
        F.col("svEffDt").alias("EFF_DT_SK"),
        F.col("svTermDt").alias("TERM_DT_SK"),
        F.col("GRP_COLOR_KEY_DESC").alias("GRP_COLOR_KEY_DESC"),
        F.col("GRP_CNTCT_NM").alias("GRP_CNTCT_NM"),
        F.col("GRP_CNTCT_CELL_PHN_NO").alias("GRP_CNTCT_CELL_PHN_NO"),
        F.col("GRP_CNTCT_WORK_PHN_NO").alias("GRP_CNTCT_WORK_PHN_NO"),
        F.col("LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
        F.col("LAST_UPDT_USER_ID").alias("LAST_UPDT_USER_ID")
    )
)

df_ForeignKey_Recycle = (
    df_foreignKey
    .filter(F.col("ErrCount") > 0)
    .select(
        GetRecycleKey(F.col("EVT_PAR_GRP_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("DISCARD_IN").alias("DISCARD_IN"),
        F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("ErrCount").alias("ERR_CT"),
        (F.col("RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("EVT_PAR_GRP_SK").alias("EVT_PAR_GRP_SK"),
        F.col("GRP_ID").alias("GRP_ID"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("GRP_ACTV_IN").alias("GRP_ACTV_IN"),
        F.col("EFF_DT_SK").alias("EFF_DT_SK"),
        F.col("TERM_DT_SK").alias("TERM_DT_SK"),
        F.col("GRP_COLOR_KEY_DESC").alias("GRP_COLOR_KEY_DESC"),
        F.col("GRP_CNTCT_NM").alias("GRP_CNTCT_NM"),
        F.col("GRP_CNTCT_CELL_PHN_NO").alias("GRP_CNTCT_CELL_PHN_NO"),
        F.col("GRP_CNTCT_WORK_PHN_NO").alias("GRP_CNTCT_WORK_PHN_NO"),
        F.col("LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
        F.col("LAST_UPDT_USER_ID").alias("LAST_UPDT_USER_ID")
    )
)

df_ForeignKey_DefaultNA_source = df_foreignKey.limit(1)
df_ForeignKey_DefaultNA = (
    df_ForeignKey_DefaultNA_source
    .select(
        F.lit(1).alias("EVT_PAR_GRP_SK"),
        F.lit("NA").alias("GRP_ID"),
        F.lit(1).alias("SRC_SYS_CD_SK"),
        F.lit(RunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit("X").alias("GRP_ACTV_IN"),
        F.lit("1753-01-01").alias("EFF_DT_SK"),
        F.lit("2199-12-31").alias("TERM_DT_SK"),
        F.lit("NA").alias("GRP_COLOR_KEY_DESC"),
        F.lit("NA").alias("GRP_CNTCT_NM"),
        F.lit("NA").alias("GRP_CNTCT_CELL_PHN_NO"),
        F.lit("NA").alias("GRP_CNTCT_WORK_PHN_NO"),
        F.lit("1753-01-01-00.00.00.000000").alias("LAST_UPDT_DTM"),
        F.lit("NA").alias("LAST_UPDT_USER_ID")
    )
)

df_ForeignKey_DefaultUNK_source = df_foreignKey.limit(1)
df_ForeignKey_DefaultUNK = (
    df_ForeignKey_DefaultUNK_source
    .select(
        F.lit(0).alias("EVT_PAR_GRP_SK"),
        F.lit("UNK").alias("GRP_ID"),
        F.lit(0).alias("SRC_SYS_CD_SK"),
        F.lit(RunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit("X").alias("GRP_ACTV_IN"),
        F.lit("1753-01-01").alias("EFF_DT_SK"),
        F.lit("2199-12-31").alias("TERM_DT_SK"),
        F.lit("UNK").alias("GRP_COLOR_KEY_DESC"),
        F.lit("UNK").alias("GRP_CNTCT_NM"),
        F.lit("UNK").alias("GRP_CNTCT_CELL_PHN_NO"),
        F.lit("UNK").alias("GRP_CNTCT_WORK_PHN_NO"),
        F.lit("1753-01-01-00.00.00.000000").alias("LAST_UPDT_DTM"),
        F.lit("UNK").alias("LAST_UPDT_USER_ID")
    )
)

df_Collector = (
    df_ForeignKey_Fkey
    .unionByName(df_ForeignKey_DefaultNA)
    .unionByName(df_ForeignKey_DefaultUNK)
)

df_ForeignKey_Recycle_final = (
    df_ForeignKey_Recycle
    .select(
        F.col("JOB_EXCTN_RCRD_ERR_SK"),
        F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
        F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
        F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT"),
        F.col("ERR_CT"),
        F.col("RECYCLE_CT"),
        F.rpad(F.col("SRC_SYS_CD"), 0, " ").alias("SRC_SYS_CD"),  # varchar with unknown length => rpad length=0 signals no padding
        F.rpad(F.col("PRI_KEY_STRING"), 0, " ").alias("PRI_KEY_STRING"),
        F.col("EVT_PAR_GRP_SK"),
        F.rpad(F.col("GRP_ID"), 0, " ").alias("GRP_ID"),
        F.col("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.rpad(F.col("GRP_ACTV_IN"), 1, " ").alias("GRP_ACTV_IN"),
        F.rpad(F.col("EFF_DT_SK"), 10, " ").alias("EFF_DT_SK"),
        F.rpad(F.col("TERM_DT_SK"), 10, " ").alias("TERM_DT_SK"),
        F.rpad(F.col("GRP_COLOR_KEY_DESC"), 0, " ").alias("GRP_COLOR_KEY_DESC"),
        F.rpad(F.col("GRP_CNTCT_NM"), 0, " ").alias("GRP_CNTCT_NM"),
        F.rpad(F.col("GRP_CNTCT_CELL_PHN_NO"), 0, " ").alias("GRP_CNTCT_CELL_PHN_NO"),
        F.rpad(F.col("GRP_CNTCT_WORK_PHN_NO"), 0, " ").alias("GRP_CNTCT_WORK_PHN_NO"),
        F.col("LAST_UPDT_DTM"),
        F.rpad(F.col("LAST_UPDT_USER_ID"), 0, " ").alias("LAST_UPDT_USER_ID")
    )
)

write_files(
    df_ForeignKey_Recycle_final,
    "hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_Collector_final = (
    df_Collector
    .select(
        F.col("EVT_PAR_GRP_SK"),
        F.col("GRP_ID"),
        F.col("SRC_SYS_CD_SK"),
        F.col("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.rpad(F.col("GRP_ACTV_IN"), 1, " ").alias("GRP_ACTV_IN"),
        F.rpad(F.col("EFF_DT_SK"), 10, " ").alias("EFF_DT_SK"),
        F.rpad(F.col("TERM_DT_SK"), 10, " ").alias("TERM_DT_SK"),
        F.col("GRP_COLOR_KEY_DESC"),
        F.col("GRP_CNTCT_NM"),
        F.col("GRP_CNTCT_CELL_PHN_NO"),
        F.col("GRP_CNTCT_WORK_PHN_NO"),
        F.col("LAST_UPDT_DTM"),
        F.col("LAST_UPDT_USER_ID")
    )
)

write_files(
    df_Collector_final,
    f"{adls_path}/load/EVT_PAR_GRP.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)