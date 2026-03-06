# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:
# MAGIC 
# MAGIC PROCESSING: PRVCY_EXTRNL_ENTY_SK from the IDS PRVCY_EXTRNL_ENTY table is used in the foriegn key process.Should run after the PRVCY_EXTRNL_ENTY table.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #               Change Description                             Development Project      Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------     ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Naren Garapaty               2/25/2007          FHP/3028                      Originally Programmed                              devlIDS30                        Steph Goddard          3/29/2007  
# MAGIC              
# MAGIC Bhoomi Dasari                 2/14/2013          TTR-1534                      Changing the length of ADDR fields          IntegrateNewDevl        Kalyan Neelam          2013-03-01
# MAGIC                                                                                                           from 40 to 80.

# MAGIC Set all foreign surrogate keys
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
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    TimestampType,
    DecimalType
)
import pyspark.sql.functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Retrieve parameter values
InFile = get_widget_value('InFile','IdsPrvcyExtrnlEntyAddr.dat')
Logging = get_widget_value('Logging','X')
RunID = get_widget_value('RunID','2007010106')

# Define schema for IdsPrvcyExtrnlEntyAddrExtr
schema_IdsPrvcyExtrnlEntyAddrExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38,10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("PRVCY_EXTRNL_ENTY_ADDR_SK", IntegerType(), False),
    StructField("PRVCY_EXTRNL_ENTY_UNIQ_KEY", IntegerType(), False),
    StructField("PRVCY_EXTL_ENTY_ADDR_TYP_CD", IntegerType(), False),
    StructField("SEQ_NO", IntegerType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("PRVCY_EXTRNL_ENTY", IntegerType(), False),
    StructField("ADDR_LN_1", StringType(), False),
    StructField("ADDR_LN_2", StringType(), False),
    StructField("ADDR_LN_3", StringType(), False),
    StructField("CITY_NM", StringType(), False),
    StructField("PRVCY_EXTL_ENTY_ADDR_ST_CD", IntegerType(), False),
    StructField("POSTAL_CD", StringType(), False),
    StructField("CNTY_NM", StringType(), False),
    StructField("SRC_SYS_LAST_UPDT_DT", StringType(), False),
    StructField("SRC_SYS_LAST_UPDT_USER", IntegerType(), False)
])

# Read file for IdsPrvcyExtrnlEntyAddrExtr
df_IdsPrvcyExtrnlEntyAddrExtr = (
    spark.read
    .option("header", "false")
    .option("quote", "\"")
    .option("escape", "\"")
    .option("delimiter", ",")
    .schema(schema_IdsPrvcyExtrnlEntyAddrExtr)
    .csv(f"{adls_path}/key/{InFile}")
)

# Create columns (stage variables) for ForeignKey transformer
df_foreignkey = (
    df_IdsPrvcyExtrnlEntyAddrExtr
    .withColumn(
        "SrcSysCdSk",
        GetFkeyCodes(
            trim(F.lit("IDS")),
            F.col("PRVCY_EXTRNL_ENTY_ADDR_SK"),
            trim(F.lit("SOURCE SYSTEM")),
            trim(F.col("SRC_SYS_CD")),
            F.lit(Logging)
        )
    )
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn(
        "svPrvcyExtrnlEntyAddrTypCdSk",
        GetFkeyCodes(
            F.col("SRC_SYS_CD"),
            F.col("PRVCY_EXTRNL_ENTY_ADDR_SK"),
            F.lit("EXTERNAL ENTITY ADDRESS TYPE"),
            F.col("PRVCY_EXTL_ENTY_ADDR_TYP_CD"),
            F.lit(Logging)
        )
    )
    .withColumn(
        "svExtrnlEntySk",
        GetFkeyPrvcyExtrnlEnty(
            F.col("SRC_SYS_CD"),
            F.col("PRVCY_EXTRNL_ENTY_ADDR_SK"),
            F.col("PRVCY_EXTRNL_ENTY"),
            F.lit(Logging)
        )
    )
    .withColumn(
        "svPrvcyExtlEntyAddrStCdSk",
        GetFkeyCodes(
            F.col("SRC_SYS_CD"),
            F.col("PRVCY_EXTRNL_ENTY_ADDR_SK"),
            F.lit("STATE"),
            F.col("PRVCY_EXTL_ENTY_ADDR_ST_CD"),
            F.lit(Logging)
        )
    )
    .withColumn(
        "svSrcSysLastUpdtDtSk",
        GetFkeyDate(
            F.lit("IDS"),
            F.col("PRVCY_EXTRNL_ENTY_ADDR_SK"),
            F.col("SRC_SYS_LAST_UPDT_DT"),
            F.lit(Logging)
        )
    )
    .withColumn(
        "svSrcSysLastUpdtUserSk",
        GetFkeyAppUsr(
            F.col("SRC_SYS_CD"),
            F.col("PRVCY_EXTRNL_ENTY_ADDR_SK"),
            F.col("SRC_SYS_LAST_UPDT_USER"),
            F.lit(Logging)
        )
    )
    .withColumn(
        "ErrCount",
        GetFkeyErrorCnt(F.col("PRVCY_EXTRNL_ENTY_ADDR_SK"))
    )
)

# Build output links from ForeignKey:
df_foreignkey_fkey = (
    df_foreignkey
    .filter((F.col("ErrCount") == 0) | (F.col("PassThru") == 'Y'))
    .select(
        F.col("PRVCY_EXTRNL_ENTY_ADDR_SK").alias("PRVCY_EXTRNL_ENTY_ADDR_SK"),
        F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
        F.col("PRVCY_EXTRNL_ENTY_UNIQ_KEY").alias("PRVCY_EXTRNL_ENTY_UNIQ_KEY"),
        F.col("svPrvcyExtrnlEntyAddrTypCdSk").alias("PRVCY_EXTL_ENTY_ADDR_TYP_CD_SK"),
        F.col("SEQ_NO").alias("SEQ_NO"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("svExtrnlEntySk").alias("PRVCY_EXTRNL_ENTY_SK"),
        F.col("ADDR_LN_1").alias("ADDR_LN_1"),
        F.col("ADDR_LN_2").alias("ADDR_LN_2"),
        F.col("ADDR_LN_3").alias("ADDR_LN_3"),
        F.col("CITY_NM").alias("CITY_NM"),
        F.col("svPrvcyExtlEntyAddrStCdSk").alias("PRVCY_EXTL_ENTY_ADDR_ST_CD_SK"),
        F.col("POSTAL_CD").alias("POSTAL_CD"),
        F.col("CNTY_NM").alias("CNTY_NM"),
        F.col("svSrcSysLastUpdtDtSk").alias("SRC_SYS_LAST_UPDT_DT_SK"),
        F.col("svSrcSysLastUpdtUserSk").alias("SRC_SYS_LAST_UPDT_USER_SK")
    )
)

df_foreignkey_recycle = (
    df_foreignkey
    .filter(F.col("ErrCount") > 0)
    .select(
        GetRecycleKey(F.col("PRVCY_EXTRNL_ENTY_ADDR_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("DISCARD_IN").alias("DISCARD_IN"),
        F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("ErrCount").alias("ERR_CT"),
        (F.col("RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("PRVCY_EXTRNL_ENTY_ADDR_SK").alias("PRVCY_EXTRNL_ENTY_ADDR_SK"),
        F.col("PRVCY_EXTRNL_ENTY_UNIQ_KEY").alias("PRVCY_EXTRNL_ENTY_UNIQ_KEY"),
        F.col("PRVCY_EXTL_ENTY_ADDR_TYP_CD").alias("PRVCY_EXTL_ENTY_ADDR_TYP_CD_SK"),
        F.col("SEQ_NO").alias("SEQ_NO"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("PRVCY_EXTRNL_ENTY").alias("PRVCY_EXTRNL_ENTY_SK"),
        F.col("ADDR_LN_1").alias("ADDR_LN_1"),
        F.col("ADDR_LN_2").alias("ADDR_LN_2"),
        F.col("ADDR_LN_3").alias("ADDR_LN_3"),
        F.col("CITY_NM").alias("CITY_NM"),
        F.col("PRVCY_EXTL_ENTY_ADDR_ST_CD").alias("PRVCY_EXTL_ENTY_ADDR_ST_CD_SK"),
        F.col("POSTAL_CD").alias("POSTAL_CD"),
        F.col("CNTY_NM").alias("CNTY_NM"),
        F.col("SRC_SYS_LAST_UPDT_DT").alias("SRC_SYS_LAST_UPDT_DT_SK"),
        F.col("SRC_SYS_LAST_UPDT_USER").alias("SRC_SYS_LAST_UPDT_USER_SK")
    )
)

# Write the hashed file "hf_recycle" as parquet (Scenario C)
write_files(
    df_foreignkey_recycle,
    f"{adls_path}/hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# DefaultUNK link => @INROWNUM=1 means take the first row only
# Apply WhereExpression for columns
df_foreignkey_defaultUNK = (
    df_foreignkey
    .limit(1)
    .select(
        F.lit(0).alias("PRVCY_EXTRNL_ENTY_ADDR_SK"),
        F.lit(0).alias("SRC_SYS_CD_SK"),
        F.lit(0).alias("PRVCY_EXTRNL_ENTY_UNIQ_KEY"),
        F.lit(0).alias("PRVCY_EXTL_ENTY_ADDR_TYP_CD_SK"),
        F.lit(0).alias("SEQ_NO"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("PRVCY_EXTRNL_ENTY_SK"),
        F.lit("UNK").alias("ADDR_LN_1"),
        F.lit("UNK").alias("ADDR_LN_2"),
        F.lit("UNK").alias("ADDR_LN_3"),
        F.lit("UNK").alias("CITY_NM"),
        F.lit(0).alias("PRVCY_EXTL_ENTY_ADDR_ST_CD_SK"),
        F.lit("UNK").alias("POSTAL_CD"),
        F.lit("UNK").alias("CNTY_NM"),
        F.lit("UNK").alias("SRC_SYS_LAST_UPDT_DT_SK"),
        F.lit(0).alias("SRC_SYS_LAST_UPDT_USER_SK")
    )
)

# DefaultNA link => @INROWNUM=1 means take the first row only
df_foreignkey_defaultNA = (
    df_foreignkey
    .limit(1)
    .select(
        F.lit(1).alias("PRVCY_EXTRNL_ENTY_ADDR_SK"),
        F.lit(1).alias("SRC_SYS_CD_SK"),
        F.lit(1).alias("PRVCY_EXTRNL_ENTY_UNIQ_KEY"),
        F.lit(1).alias("PRVCY_EXTL_ENTY_ADDR_TYP_CD_SK"),
        F.lit(1).alias("SEQ_NO"),
        F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("PRVCY_EXTRNL_ENTY_SK"),
        F.lit("NA").alias("ADDR_LN_1"),
        F.lit("NA").alias("ADDR_LN_2"),
        F.lit("NA").alias("ADDR_LN_3"),
        F.lit("NA").alias("CITY_NM"),
        F.lit(1).alias("PRVCY_EXTL_ENTY_ADDR_ST_CD_SK"),
        F.lit("NA").alias("POSTAL_CD"),
        F.lit("NA").alias("CNTY_NM"),
        F.lit("NA").alias("SRC_SYS_LAST_UPDT_DT_SK"),
        F.lit(1).alias("SRC_SYS_LAST_UPDT_USER_SK")
    )
)

# Collector => union of fkey, defaultUNK, defaultNA
df_collector = (
    df_foreignkey_fkey.unionByName(df_foreignkey_defaultUNK)
    .unionByName(df_foreignkey_defaultNA)
)

# Apply rpad to char/varchar columns for final output
df_collector_final = (
    df_collector
    .withColumn("ADDR_LN_1", rpad(F.col("ADDR_LN_1"), <...>, " "))
    .withColumn("ADDR_LN_2", rpad(F.col("ADDR_LN_2"), <...>, " "))
    .withColumn("ADDR_LN_3", rpad(F.col("ADDR_LN_3"), <...>, " "))
    .withColumn("CITY_NM", rpad(F.col("CITY_NM"), <...>, " "))
    .withColumn("POSTAL_CD", rpad(F.col("POSTAL_CD"), <...>, " "))
    .withColumn("CNTY_NM", rpad(F.col("CNTY_NM"), <...>, " "))
    .withColumn("SRC_SYS_LAST_UPDT_DT_SK", rpad(F.col("SRC_SYS_LAST_UPDT_DT_SK"), 10, " "))
    .select(
        "PRVCY_EXTRNL_ENTY_ADDR_SK",
        "SRC_SYS_CD_SK",
        "PRVCY_EXTRNL_ENTY_UNIQ_KEY",
        "PRVCY_EXTL_ENTY_ADDR_TYP_CD_SK",
        "SEQ_NO",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "PRVCY_EXTRNL_ENTY_SK",
        "ADDR_LN_1",
        "ADDR_LN_2",
        "ADDR_LN_3",
        "CITY_NM",
        "PRVCY_EXTL_ENTY_ADDR_ST_CD_SK",
        "POSTAL_CD",
        "CNTY_NM",
        "SRC_SYS_LAST_UPDT_DT_SK",
        "SRC_SYS_LAST_UPDT_USER_SK"
    )
)

# Write final file
write_files(
    df_collector_final,
    f"{adls_path}/load/PRVCY_EXTRNL_ENTY_ADDR.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)