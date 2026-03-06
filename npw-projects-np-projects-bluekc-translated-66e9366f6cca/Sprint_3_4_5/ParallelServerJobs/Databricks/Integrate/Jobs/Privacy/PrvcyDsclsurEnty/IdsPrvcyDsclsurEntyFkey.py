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
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #               Change Description                             Development Project      Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------     ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Naren Garapaty               2/25/2007          FHP/3028                      Originally Programmed                              devlIDS30                        Steph Goddard     3/29/2007      
# MAGIC              
# MAGIC Manasa Andru                 6/26/2013           TTR - 778                  Removed RunID from parameters as         IntegrateCurDevl                  Kalyan Neelam     2013-07-02
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
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DecimalType
from pyspark.sql.functions import col, lit, when, rpad, expr, monotonically_increasing_id, concat
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


InFile = get_widget_value('InFile','')
Logging = get_widget_value('Logging','X')

schema_IdsPrvcyDsclsurEntyExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38,10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("PRVCY_DSCLSUR_ENTY_SK", IntegerType(), nullable=False),
    StructField("PRVCY_DSCLSUR_ENTY_UNIQ_KEY", IntegerType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("PRVCY_DSCLSUR_ENTY_CAT_CD", IntegerType(), nullable=False),
    StructField("PRVCY_DSCLSUR_ENTY_TYP_CD", IntegerType(), nullable=False),
    StructField("PRVCY_DSCLSUR_ENTY_ID", StringType(), nullable=False),
    StructField("SRC_SYS_LAST_UPDT_DT", StringType(), nullable=False),
    StructField("SRC_SYS_LAST_UPDT_USER", IntegerType(), nullable=False)
])

df_IdsPrvcyDsclsurEntyExtr = (
    spark.read.format("csv")
    .option("sep", ",")
    .option("quote", "\"")
    .option("header", "false")
    .schema(schema_IdsPrvcyDsclsurEntyExtr)
    .load(f"{adls_path}/key/IdsPrvcyDsclsurEntyExtr.PrvcyDsclsurEnty.uniq")
)

df_tx = df_IdsPrvcyDsclsurEntyExtr.withColumn(
    "SrcSysCdSk",
    GetFkeyCodes(
        trim(lit("IDS")),
        col("PRVCY_DSCLSUR_ENTY_SK"),
        trim(lit("SOURCE SYSTEM")),
        trim(col("SRC_SYS_CD")),
        Logging
    )
).withColumn(
    "PassThru",
    col("PASS_THRU_IN")
).withColumn(
    "svPrvcyDsclsurEntyCatCdSk",
    GetFkeyCodes(
        col("SRC_SYS_CD"),
        col("PRVCY_DSCLSUR_ENTY_SK"),
        lit("PRIVACY DISCLOSURE ENTITY CATEGORY"),
        col("PRVCY_DSCLSUR_ENTY_CAT_CD"),
        Logging
    )
).withColumn(
    "svPrvcyDsclsurEntyTypCdSk",
    GetFkeyCodes(
        col("SRC_SYS_CD"),
        col("PRVCY_DSCLSUR_ENTY_SK"),
        lit("PRIVACY AUTHORIZATION RECIPIENT TYPE"),
        col("PRVCY_DSCLSUR_ENTY_TYP_CD"),
        Logging
    )
).withColumn(
    "svSrcSysLastUpdtDtSk",
    GetFkeyDate(
        lit("IDS"),
        col("PRVCY_DSCLSUR_ENTY_SK"),
        col("SRC_SYS_LAST_UPDT_DT"),
        Logging
    )
).withColumn(
    "svSrcSysLastUpdtUserSk",
    GetFkeyAppUsr(
        col("SRC_SYS_CD"),
        col("PRVCY_DSCLSUR_ENTY_SK"),
        col("SRC_SYS_LAST_UPDT_USER"),
        Logging
    )
).withColumn(
    "ErrCount",
    GetFkeyErrorCnt(
        col("PRVCY_DSCLSUR_ENTY_SK")
    )
)

df_fkey = df_tx.filter(
    (col("ErrCount") == lit(0)) | (col("PassThru") == lit("Y"))
).select(
    col("PRVCY_DSCLSUR_ENTY_SK"),
    col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
    col("PRVCY_DSCLSUR_ENTY_UNIQ_KEY"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("svPrvcyDsclsurEntyCatCdSk").alias("PRVCY_DSCLSUR_ENTY_CAT_CD_SK"),
    col("svPrvcyDsclsurEntyTypCdSk").alias("PRVCY_DSCLSUR_ENTY_TYP_CD_SK"),
    col("PRVCY_DSCLSUR_ENTY_ID"),
    col("svSrcSysLastUpdtDtSk").alias("SRC_SYS_LAST_UPDT_DT_SK"),
    col("svSrcSysLastUpdtUserSk").alias("SRC_SYS_LAST_UPDT_USER_SK")
)

df_recycle = df_tx.filter(
    col("ErrCount") > lit(0)
).select(
    GetRecycleKey(col("PRVCY_DSCLSUR_ENTY_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
    col("INSRT_UPDT_CD"),
    col("DISCARD_IN"),
    col("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ErrCount").alias("ERR_CT"),
    (col("RECYCLE_CT") + lit(1)).alias("RECYCLE_CT"),
    col("SRC_SYS_CD"),
    col("PRI_KEY_STRING"),
    col("PRVCY_DSCLSUR_ENTY_SK"),
    col("PRVCY_DSCLSUR_ENTY_UNIQ_KEY"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("PRVCY_DSCLSUR_ENTY_CAT_CD").alias("PRVCY_DSCLSUR_ENTY_CAT_CD_SK"),
    col("PRVCY_DSCLSUR_ENTY_TYP_CD").alias("PRVCY_DSCLSUR_ENTY_TYP_CD_SK"),
    col("PRVCY_DSCLSUR_ENTY_ID"),
    col("SRC_SYS_LAST_UPDT_DT").alias("SRC_SYS_LAST_UPDT_DT_SK"),
    col("SRC_SYS_LAST_UPDT_USER").alias("SRC_SYS_LAST_UPDT_USER_SK")
)

df_recycle = df_recycle.withColumn(
    "INSRT_UPDT_CD", rpad(col("INSRT_UPDT_CD"), 10, " ")
).withColumn(
    "DISCARD_IN", rpad(col("DISCARD_IN"), 1, " ")
).withColumn(
    "PASS_THRU_IN", rpad(col("PASS_THRU_IN"), 1, " ")
).withColumn(
    "SRC_SYS_LAST_UPDT_DT_SK", rpad(col("SRC_SYS_LAST_UPDT_DT_SK"), 10, " ")
)

write_files(
    df_recycle.select(
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "PASS_THRU_IN",
        "FIRST_RECYC_DT",
        "ERR_CT",
        "RECYCLE_CT",
        "SRC_SYS_CD",
        "PRI_KEY_STRING",
        "PRVCY_DSCLSUR_ENTY_SK",
        "PRVCY_DSCLSUR_ENTY_UNIQ_KEY",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "PRVCY_DSCLSUR_ENTY_CAT_CD_SK",
        "PRVCY_DSCLSUR_ENTY_TYP_CD_SK",
        "PRVCY_DSCLSUR_ENTY_ID",
        "SRC_SYS_LAST_UPDT_DT_SK",
        "SRC_SYS_LAST_UPDT_USER_SK"
    ),
    "hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_defaultUNK = df_tx.limit(1).select(
    lit(0).alias("PRVCY_DSCLSUR_ENTY_SK"),
    lit(0).alias("SRC_SYS_CD_SK"),
    lit(0).alias("PRVCY_DSCLSUR_ENTY_UNIQ_KEY"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("PRVCY_DSCLSUR_ENTY_CAT_CD_SK"),
    lit(0).alias("PRVCY_DSCLSUR_ENTY_TYP_CD_SK"),
    lit("UNK").alias("PRVCY_DSCLSUR_ENTY_ID"),
    lit("UNK").alias("SRC_SYS_LAST_UPDT_DT_SK"),
    lit(0).alias("SRC_SYS_LAST_UPDT_USER_SK")
)

df_defaultNA = df_tx.limit(1).select(
    lit(1).alias("PRVCY_DSCLSUR_ENTY_SK"),
    lit(1).alias("SRC_SYS_CD_SK"),
    lit(1).alias("PRVCY_DSCLSUR_ENTY_UNIQ_KEY"),
    lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(1).alias("PRVCY_DSCLSUR_ENTY_CAT_CD_SK"),
    lit(1).alias("PRVCY_DSCLSUR_ENTY_TYP_CD_SK"),
    lit("NA").alias("PRVCY_DSCLSUR_ENTY_ID"),
    lit("NA").alias("SRC_SYS_LAST_UPDT_DT_SK"),
    lit(0).alias("SRC_SYS_LAST_UPDT_USER_SK")
)

df_collector = df_fkey.unionByName(df_defaultUNK).unionByName(df_defaultNA)

df_collector = df_collector.withColumn(
    "SRC_SYS_LAST_UPDT_DT_SK", rpad(col("SRC_SYS_LAST_UPDT_DT_SK"), 10, " ")
)

df_collector_final = df_collector.select(
    "PRVCY_DSCLSUR_ENTY_SK",
    "SRC_SYS_CD_SK",
    "PRVCY_DSCLSUR_ENTY_UNIQ_KEY",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "PRVCY_DSCLSUR_ENTY_CAT_CD_SK",
    "PRVCY_DSCLSUR_ENTY_TYP_CD_SK",
    "PRVCY_DSCLSUR_ENTY_ID",
    "SRC_SYS_LAST_UPDT_DT_SK",
    "SRC_SYS_LAST_UPDT_USER_SK"
)

write_files(
    df_collector_final,
    f"{adls_path}/load/PRVCY_DSCLSUR_ENTY.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)