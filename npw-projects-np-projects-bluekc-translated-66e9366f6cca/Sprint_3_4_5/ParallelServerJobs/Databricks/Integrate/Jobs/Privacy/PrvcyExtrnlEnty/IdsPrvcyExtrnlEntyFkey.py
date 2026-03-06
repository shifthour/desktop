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
# MAGIC PROCESSING:
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #               Change Description                             Development Project      Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------     ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Naren Garapaty               2/25/2007           FHP/3028                        Originally Programmed                              devlIDS30                Steph Goddard            3/29/2007
# MAGIC 
# MAGIC Steph Goddard                7/15/10             TTR-630                       changed field name from                       RebuildIntNewDevl           sANDREW                 2010-09-30
# MAGIC                                                                                                         PRVCY_EXTRNL_ENTY_CLS_TYP_CD_S to PRVCY_EXTL_ENTY_CLS_TYP_CD_SK
# MAGIC 
# MAGIC Manasa Andru                 6/26/2013           TTR - 778                  Removed RunID from parameters as         IntegrateCurDevl              Kalyan Neelam           2013-07-02
# MAGIC                                                                                                      it is not being used anywhere in the job

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
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    TimestampType,
    DecimalType
)
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


InFile = get_widget_value("InFile","")
Logging = get_widget_value("Logging","X")
SrcSysCdSk = get_widget_value("SrcSysCdSk","")

schema_IdsPrvcyExtrnlEntyExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38,10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("PRVCY_EXTRNL_ENTY_SK", IntegerType(), nullable=False),
    StructField("PRVCY_EXTRNL_ENTY_UNIQ_KEY", IntegerType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("PRVCY_EXTRNL_ENTY_CLS_TYP_CD", IntegerType(), nullable=False),
    StructField("SRC_SYS_LAST_UPDT_DT", StringType(), nullable=False),
    StructField("SRC_SYS_LAST_UPDT_USER", IntegerType(), nullable=False)
])

df_IdsPrvcyExtrnlEntyExtr = (
    spark.read
    .option("quote", '"')
    .option("header", False)
    .option("sep", ",")
    .schema(schema_IdsPrvcyExtrnlEntyExtr)
    .csv(f"{adls_path}/key/{InFile}")
)

df_enriched = (
    df_IdsPrvcyExtrnlEntyExtr
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("svPrvcyExtrnlEntyClsTypCdSk",
                GetFkeyCodes(
                    F.col("SRC_SYS_CD"),
                    F.col("PRVCY_EXTRNL_ENTY_SK"),
                    F.lit("EXTERNAL ENTITY CLASS TYPE"),
                    F.col("PRVCY_EXTRNL_ENTY_CLS_TYP_CD"),
                    Logging
                ))
    .withColumn("svSrcSysLastUpdtDtSk",
                GetFkeyDate(
                    F.lit("IDS"),
                    F.col("PRVCY_EXTRNL_ENTY_SK"),
                    F.col("SRC_SYS_LAST_UPDT_DT"),
                    Logging
                ))
    .withColumn("svSrcSysLastUpdtUserSk",
                GetFkeyAppUsr(
                    F.col("SRC_SYS_CD"),
                    F.col("PRVCY_EXTRNL_ENTY_SK"),
                    F.col("SRC_SYS_LAST_UPDT_USER"),
                    Logging
                ))
    .withColumn("ErrCount",
                GetFkeyErrorCnt(
                    F.col("PRVCY_EXTRNL_ENTY_SK")
                ))
)

df_fkey = (
    df_enriched
    .filter((F.col("ErrCount") == 0) | (F.col("PassThru") == "Y"))
    .select(
        F.col("PRVCY_EXTRNL_ENTY_SK").alias("PRVCY_EXTRNL_ENTY_SK"),
        F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        F.col("PRVCY_EXTRNL_ENTY_UNIQ_KEY").alias("PRVCY_EXTRNL_ENTY_UNIQ_KEY"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("svPrvcyExtrnlEntyClsTypCdSk").alias("PRVCY_EXTL_ENTY_CLS_TYP_CD_SK"),
        F.col("svSrcSysLastUpdtDtSk").alias("SRC_SYS_LAST_UPDT_DT_SK"),
        F.col("svSrcSysLastUpdtUserSk").alias("SRC_SYS_LAST_UPDT_USER_SK")
    )
)

df_recycle = (
    df_enriched
    .filter(F.col("ErrCount") > 0)
    .select(
        GetRecycleKey(F.col("PRVCY_EXTRNL_ENTY_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("DISCARD_IN").alias("DISCARD_IN"),
        F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("ErrCount").alias("ERR_CT"),
        (F.col("RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("PRVCY_EXTRNL_ENTY_SK").alias("PRVCY_EXTRNL_ENTY_SK"),
        F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("PRVCY_EXTRNL_ENTY_UNIQ_KEY").alias("PRVCY_EXTRNL_ENTY_UNIQ_KEY"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("PRVCY_EXTRNL_ENTY_CLS_TYP_CD").alias("PRVCY_EXTL_ENTY_CLS_TYP_CD_SK"),
        F.col("SRC_SYS_LAST_UPDT_DT").alias("SRC_SYS_LAST_UPDT_DT_SK"),
        F.col("SRC_SYS_LAST_UPDT_USER").alias("SRC_SYS_LAST_UPDT_USER_SK")
    )
)

df_temp_unk = df_enriched.limit(1)
df_defaultunk = df_temp_unk.select(
    F.lit(0).alias("PRVCY_EXTRNL_ENTY_SK"),
    F.lit(0).alias("SRC_SYS_CD_SK"),
    F.lit(0).alias("PRVCY_EXTRNL_ENTY_UNIQ_KEY"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("PRVCY_EXTL_ENTY_CLS_TYP_CD_SK"),
    F.lit("UNK").alias("SRC_SYS_LAST_UPDT_DT_SK"),
    F.lit(0).alias("SRC_SYS_LAST_UPDT_USER_SK")
)

df_temp_na = df_enriched.limit(1)
df_defaultna = df_temp_na.select(
    F.lit(1).alias("PRVCY_EXTRNL_ENTY_SK"),
    F.lit(1).alias("SRC_SYS_CD_SK"),
    F.lit(1).alias("PRVCY_EXTRNL_ENTY_UNIQ_KEY"),
    F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("PRVCY_EXTL_ENTY_CLS_TYP_CD_SK"),
    F.lit("NA").alias("SRC_SYS_LAST_UPDT_DT_SK"),
    F.lit(1).alias("SRC_SYS_LAST_UPDT_USER_SK")
)

df_collector = df_fkey.unionByName(df_defaultunk).unionByName(df_defaultna)

# Final output for hf_recycle (Scenario C -> write to Parquet).
# Apply rpad for char columns with known lengths:
df_recycle_final = df_recycle.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRVCY_EXTRNL_ENTY_SK"),
    F.col("PRI_KEY_STRING"),
    F.col("PRVCY_EXTRNL_ENTY_UNIQ_KEY"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("PRVCY_EXTL_ENTY_CLS_TYP_CD_SK"),
    F.rpad(F.col("SRC_SYS_LAST_UPDT_DT_SK"), 10, " ").alias("SRC_SYS_LAST_UPDT_DT_SK"),
    F.col("SRC_SYS_LAST_UPDT_USER_SK")
)

write_files(
    df_recycle_final,
    f"{adls_path}/hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

# Final output for PRVCY_EXTRNL_ENTY.dat
df_collector_final = df_collector.select(
    F.col("PRVCY_EXTRNL_ENTY_SK"),
    F.col("SRC_SYS_CD_SK"),
    F.col("PRVCY_EXTRNL_ENTY_UNIQ_KEY"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("PRVCY_EXTL_ENTY_CLS_TYP_CD_SK"),
    F.rpad(F.col("SRC_SYS_LAST_UPDT_DT_SK"), 10, " ").alias("SRC_SYS_LAST_UPDT_DT_SK"),
    F.col("SRC_SYS_LAST_UPDT_USER_SK")
)

write_files(
    df_collector_final,
    f"{adls_path}/load/PRVCY_EXTRNL_ENTY.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)