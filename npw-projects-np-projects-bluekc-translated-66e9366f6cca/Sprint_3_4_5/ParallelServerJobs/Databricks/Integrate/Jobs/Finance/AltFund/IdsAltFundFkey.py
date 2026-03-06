# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 01/28/08 09:39:22 Batch  14638_34767 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 12/27/07 09:46:56 Batch  14606_35219 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/24/07 09:35:50 Batch  14573_34558 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/07/07 14:46:06 Batch  14556_53195 PROMOTE bckcetl ids20 dsadm bls for rt
# MAGIC ^1_1 11/07/07 14:33:10 Batch  14556_52396 INIT bckcett testIDScur dsadm bls for rt
# MAGIC ^1_1 10/17/07 13:27:15 Batch  14535_48445 PROMOTE bckcett testIDScur u06640 Ralph
# MAGIC ^1_1 10/17/07 13:17:55 Batch  14535_47880 INIT bckcett devlIDS30 u06640 Ralph
# MAGIC 
# MAGIC 
# MAGIC ****************************************************************************************************************************************************************************
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:   Foriegn keying process
# MAGIC 
# MAGIC       
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer             Date                         Change Description                                                                           Project #          Development Project          Code Reviewer          Date Reviewed  
# MAGIC ------------------           ----------------------------     -----------------------------------------------------------------------                                   ----------------         ------------------------------------       ----------------------------      ----------------
# MAGIC Naren Garapaty     09/20/2007                Initial program                                                                                  3259                           devlIDS30               Steph Goddard          09/27/2007

# MAGIC Read common record format file.
# MAGIC Writing Sequential File to /load
# MAGIC Assign all foreign keys and create default rows for UNK and NA.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
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
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


InFile = get_widget_value("InFile","IdsAltFundPkey.AltFundTmp.dat")
Logging = get_widget_value("Logging","N")

schema_IdsAltFund = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38, 10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("ALT_FUND_SK", IntegerType(), nullable=False),
    StructField("ALT_FUND_UNIQ_KEY", IntegerType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("ALT_FUND_ID", StringType(), nullable=False),
    StructField("ALT_FUND_NM", StringType(), nullable=True),
    StructField("EFF_DT", StringType(), nullable=False),
    StructField("TERM_DT", StringType(), nullable=False),
    StructField("ADDR_LN_1", StringType(), nullable=True),
    StructField("ADDR_LN_2", StringType(), nullable=True),
    StructField("ADDR_LN_3", StringType(), nullable=True),
    StructField("CITY_NM", StringType(), nullable=True),
    StructField("ALT_FUND_ST_CD", IntegerType(), nullable=False),
    StructField("POSTAL_CD", StringType(), nullable=True),
    StructField("EMAIL_ADDR_TX", StringType(), nullable=True)
])

df_IdsAltFund = (
    spark.read.format("csv")
    .option("header", "false")
    .option("sep", ",")
    .option("quote", "\"")
    .schema(schema_IdsAltFund)
    .load(f"{adls_path}/key/" + InFile)
)

df_PurgeTrn_base = (
    df_IdsAltFund
    .withColumn("svSrcSysCdSk", GetFkeyCodes("IDS", F.col("ALT_FUND_SK"), "SOURCE SYSTEM", F.col("SRC_SYS_CD"), Logging))
    .withColumn("svEffDtSk", GetFkeyDate("IDS", F.col("ALT_FUND_SK"), F.col("EFF_DT"), Logging))
    .withColumn("svTermDtSk", GetFkeyDate("IDS", F.col("ALT_FUND_SK"), F.col("TERM_DT"), Logging))
    .withColumn("svStCdSk", GetFkeyCodes("IDS", F.col("ALT_FUND_SK"), "STATE", F.col("ALT_FUND_ST_CD"), Logging))
    .withColumn("svPassThru", F.col("PASS_THRU_IN"))
    .withColumn("svErrCount", GetFkeyErrorCnt(F.col("ALT_FUND_SK")))
)

df_Fkey = (
    df_PurgeTrn_base
    .filter((F.col("svErrCount") == 0) | (F.col("svPassThru") == "Y"))
    .select(
        F.col("ALT_FUND_SK").alias("ALT_FUND_SK"),
        F.col("svSrcSysCdSk").alias("SRC_SYS_CD_SK"),
        F.col("ALT_FUND_UNIQ_KEY").alias("ALT_FUND_UNIQ_KEY"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("ALT_FUND_ID").alias("ALT_FUND_ID"),
        F.col("ALT_FUND_NM").alias("ALT_FUND_NM"),
        F.col("svEffDtSk").alias("EFF_DT_SK"),
        F.col("svTermDtSk").alias("TERM_DT_SK"),
        F.col("ADDR_LN_1").alias("ADDR_LN_1"),
        F.col("ADDR_LN_2").alias("ADDR_LN_2"),
        F.col("ADDR_LN_3").alias("ADDR_LN_3"),
        F.col("CITY_NM").alias("CITY_NM"),
        F.col("svStCdSk").alias("ALT_FUND_ST_CD_SK"),
        F.col("POSTAL_CD").alias("POSTAL_CD"),
        F.col("EMAIL_ADDR_TX").alias("EMAIL_ADDR_TX")
    )
)

df_lnkRecycle = (
    df_PurgeTrn_base
    .filter(F.col("svErrCount") > 0)
    .select(
        GetRecycleKey(F.col("ALT_FUND_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("DISCARD_IN").alias("DISCARD_IN"),
        F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("svErrCount").alias("ERR_CT"),
        (F.col("RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("ALT_FUND_SK").alias("ALT_FUND_SK"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD_SK"),
        F.col("ALT_FUND_UNIQ_KEY").alias("ALT_FUND_UNIQ_KEY"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("ALT_FUND_ID").alias("ALT_FUND_ID"),
        F.col("ALT_FUND_NM").alias("ALT_FUND_NM"),
        F.col("EFF_DT").alias("EFF_DT_SK"),
        F.col("TERM_DT").alias("TERM_DT_SK"),
        F.col("ADDR_LN_1").alias("ADDR_LN_1"),
        F.col("ADDR_LN_2").alias("ADDR_LN_2"),
        F.col("ADDR_LN_3").alias("ADDR_LN_3"),
        F.col("CITY_NM").alias("CITY_NM"),
        F.col("ALT_FUND_ST_CD").alias("ALT_FUND_ST_CD_SK"),
        F.col("POSTAL_CD").alias("POSTAL_CD"),
        F.col("EMAIL_ADDR_TX").alias("EMAIL_ADDR_TX")
    )
)

df_DefaultBase = df_PurgeTrn_base.limit(1)

df_DefaultUNK = df_DefaultBase.select(
    F.lit(0).alias("ALT_FUND_SK"),
    F.lit(0).alias("SRC_SYS_CD_SK"),
    F.lit(0).alias("ALT_FUND_UNIQ_KEY"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit("UNK").alias("ALT_FUND_ID"),
    F.lit("UNK").alias("ALT_FUND_NM"),
    F.lit("UNK").alias("EFF_DT_SK"),
    F.lit("UNK").alias("TERM_DT_SK"),
    F.lit("UNK").alias("ADDR_LN_1"),
    F.lit("UNK").alias("ADDR_LN_2"),
    F.lit("UNK").alias("ADDR_LN_3"),
    F.lit("UNK").alias("CITY_NM"),
    F.lit(0).alias("ALT_FUND_ST_CD_SK"),
    F.lit("UNK").alias("POSTAL_CD"),
    F.lit("UNK").alias("EMAIL_ADDR_TX")
)

df_DefaultNA = df_DefaultBase.select(
    F.lit(1).alias("ALT_FUND_SK"),
    F.lit(1).alias("SRC_SYS_CD_SK"),
    F.lit(1).alias("ALT_FUND_UNIQ_KEY"),
    F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit("NA").alias("ALT_FUND_ID"),
    F.lit("NA").alias("ALT_FUND_NM"),
    F.lit("NA").alias("EFF_DT_SK"),
    F.lit("NA").alias("TERM_DT_SK"),
    F.lit("NA").alias("ADDR_LN_1"),
    F.lit("NA").alias("ADDR_LN_2"),
    F.lit("NA").alias("ADDR_LN_3"),
    F.lit("NA").alias("CITY_NM"),
    F.lit(0).alias("ALT_FUND_ST_CD_SK"),
    F.lit("NA").alias("POSTAL_CD"),
    F.lit("NA").alias("EMAIL_ADDR_TX")
)

write_files(
    df_lnkRecycle,
    "hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_collector = df_Fkey.unionByName(df_DefaultUNK).unionByName(df_DefaultNA)

df_collector = (
    df_collector
    .withColumn("EFF_DT_SK", rpad("EFF_DT_SK", 10, " "))
    .withColumn("TERM_DT_SK", rpad("TERM_DT_SK", 10, " "))
    .select(
        "ALT_FUND_SK",
        "SRC_SYS_CD_SK",
        "ALT_FUND_UNIQ_KEY",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "ALT_FUND_ID",
        "ALT_FUND_NM",
        "EFF_DT_SK",
        "TERM_DT_SK",
        "ADDR_LN_1",
        "ADDR_LN_2",
        "ADDR_LN_3",
        "CITY_NM",
        "ALT_FUND_ST_CD_SK",
        "POSTAL_CD",
        "EMAIL_ADDR_TX"
    )
)

write_files(
    df_collector,
    f"{adls_path}/load/ALT_FUND.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)