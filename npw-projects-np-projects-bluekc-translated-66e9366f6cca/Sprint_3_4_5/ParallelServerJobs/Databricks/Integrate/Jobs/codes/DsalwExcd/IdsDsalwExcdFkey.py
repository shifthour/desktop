# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2020 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsDsalwExcdFkey
# MAGIC 
# MAGIC DESCRIPTION:     Takes the sorted file and applies the primary key.   Preps the file for the foreign key lookup process that follows.
# MAGIC       
# MAGIC 
# MAGIC INPUTS:  Primary Key output file ../key/FctsDsalwExcdExtr.DsalwExcd.uniq
# MAGIC 	
# MAGIC   
# MAGIC HASH FILES:     hf_recycle
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  GetFkeyCodes, GetFkeyExcd
# MAGIC                             
# MAGIC PROCESSING:  Natural keys are converted to surrogate keys
# MAGIC 
# MAGIC OUTPUTS:   Load file .../load/DSALW_EXCD.dat
# MAGIC                      hf_recycle - If recycling was used for this job records would be written to this hash file that had errors.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC             Suzanne Saylor  -  11/21/2005  -  Originally programmed
# MAGIC             Brent Leland          03/14/2006 -  Hard code file name, removed file parameters.
# MAGIC           
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                 Change Description                                                                   Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------      --------------------------------------------------------------------------------                      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Ralph Tucker                  1/9/2012       TTR-1269                         Updated Defaults for Excd_Id, and Eff & Term Dates                    IntegrateCurDevl      
# MAGIC 
# MAGIC 
# MAGIC Emran.Mohammad               2020-10-12                                           brought up to standards

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
from pyspark.sql.functions import col, lit, rpad, trim
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


Logging = get_widget_value('Logging','X')

schema_IdsDsalwExcd = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38,10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("DSALW_EXCD_SK", IntegerType(), nullable=False),
    StructField("SRC_SYS_CD_SK", IntegerType(), nullable=False),
    StructField("EXCD_ID", StringType(), nullable=False),
    StructField("EFF_DT_SK", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("EXCD_SK", IntegerType(), nullable=False),
    StructField("EXCD_RESP_CD_SK", StringType(), nullable=False),
    StructField("EXCD_STTUS_CD_SK", StringType(), nullable=False),
    StructField("BYPS_IN", StringType(), nullable=False),
    StructField("DSALW_IN", StringType(), nullable=False),
    StructField("TERM_DT_SK", StringType(), nullable=False),
    StructField("EXCD_LONG_TX1", StringType(), nullable=True),
    StructField("EXCD_LONG_TX2", StringType(), nullable=True)
])

df_IdsDsalwExcd = (
    spark.read.format("csv")
    .option("quote", '"')
    .option("header", "false")
    .schema(schema_IdsDsalwExcd)
    .load(f"{adls_path}/key/FctsDsalwExcdExtr.DsalwExcd.uniq")
)

df_ForeignKey = df_IdsDsalwExcd.withColumn(
    "SrcSysCdSk",
    GetFkeyCodes(
        trim(col("SRC_SYS_CD")),
        col("DSALW_EXCD_SK"),
        lit("SOURCE SYSTEM"),
        trim(col("SRC_SYS_CD")),
        Logging
    )
).withColumn(
    "PassThru",
    col("PASS_THRU_IN")
).withColumn(
    "svExcdSk",
    GetFkeyExcd(
        col("SRC_SYS_CD"),
        col("DSALW_EXCD_SK"),
        col("EXCD_ID"),
        Logging
    )
).withColumn(
    "svExcdRespCdSk",
    GetFkeyCodes(
        col("SRC_SYS_CD"),
        col("DSALW_EXCD_SK"),
        lit("EXPLANATION CODE LIABILITY"),
        col("EXCD_RESP_CD_SK"),
        Logging
    )
).withColumn(
    "svExcdSttusCdSk",
    GetFkeyCodes(
        col("SRC_SYS_CD"),
        col("DSALW_EXCD_SK"),
        lit("EXPLANATION CODE STATUS"),
        col("EXCD_STTUS_CD_SK"),
        Logging
    )
).withColumn(
    "ErrCount",
    GetFkeyErrorCnt(
        col("DSALW_EXCD_SK")
    )
)

df_Fkey = df_ForeignKey.filter(
    (col("ErrCount") == 0) | (col("PassThru") == 'Y')
).select(
    col("DSALW_EXCD_SK").alias("DSALW_EXCD_SK"),
    col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
    col("EXCD_ID").alias("EXCD_ID"),
    col("EFF_DT_SK").alias("EFF_DT_SK"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("svExcdSk").alias("EXCD_SK"),
    col("svExcdRespCdSk").alias("EXCD_RESP_CD_SK"),
    col("svExcdSttusCdSk").alias("EXCD_STTUS_CD_SK"),
    col("BYPS_IN").alias("BYPS_IN"),
    col("DSALW_IN").alias("DSALW_IN"),
    col("TERM_DT_SK").alias("TERM_DT_SK"),
    col("EXCD_LONG_TX1").alias("EXCD_LONG_TX1"),
    col("EXCD_LONG_TX2").alias("EXCD_LONG_TX2")
)

df_Recycle = df_ForeignKey.filter(
    col("ErrCount") > 0
).select(
    GetRecycleKey(col("DSALW_EXCD_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
    col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    col("DISCARD_IN").alias("DISCARD_IN"),
    col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    col("ERR_CT").alias("ERR_CT"),
    col("RECYCLE_CT").alias("RECYCLE_CT"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    col("DSALW_EXCD_SK").alias("DSALW_EXCD_SK"),
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("EXCD_ID").alias("EXCD_ID"),
    col("EFF_DT_SK").alias("EFF_DT_SK"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("EXCD_SK").alias("EXCD_SK"),
    col("EXCD_RESP_CD_SK").alias("EXCD_RESP_CD_SK"),
    col("EXCD_STTUS_CD_SK").alias("EXCD_STTUS_CD_SK"),
    col("BYPS_IN").alias("BYPS_IN"),
    col("DSALW_IN").alias("DSALW_IN"),
    col("TERM_DT_SK").alias("TERM_DT_SK"),
    col("EXCD_LONG_TX1").alias("EXCD_LONG_TX1"),
    col("EXCD_LONG_TX2").alias("EXCD_LONG_TX2")
)

df_Recycle_final = df_Recycle.select(
    col("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    rpad(col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    rpad(col("PRI_KEY_STRING"), <...>, " ").alias("PRI_KEY_STRING"),
    col("DSALW_EXCD_SK"),
    col("SRC_SYS_CD_SK"),
    rpad(col("EXCD_ID"), 3, " ").alias("EXCD_ID"),
    rpad(col("EFF_DT_SK"), 10, " ").alias("EFF_DT_SK"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("EXCD_SK"),
    rpad(col("EXCD_RESP_CD_SK"), 1, " ").alias("EXCD_RESP_CD_SK"),
    rpad(col("EXCD_STTUS_CD_SK"), 1, " ").alias("EXCD_STTUS_CD_SK"),
    rpad(col("BYPS_IN"), 1, " ").alias("BYPS_IN"),
    rpad(col("DSALW_IN"), 1, " ").alias("DSALW_IN"),
    rpad(col("TERM_DT_SK"), 10, " ").alias("TERM_DT_SK"),
    rpad(col("EXCD_LONG_TX1"), <...>, " ").alias("EXCD_LONG_TX1"),
    rpad(col("EXCD_LONG_TX2"), <...>, " ").alias("EXCD_LONG_TX2")
)

write_files(
    df_Recycle_final,
    "hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_DefaultUNK = spark.createDataFrame(
    [(0, 0, "UNK", "1753-01-01", 0, 0, 0, 0, 0, "U", "U", "2199-12-31", 0, 0)],
    [
        "DSALW_EXCD_SK",
        "SRC_SYS_CD_SK",
        "EXCD_ID",
        "EFF_DT_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "EXCD_SK",
        "EXCD_RESP_CD_SK",
        "EXCD_STTUS_CD_SK",
        "BYPS_IN",
        "DSALW_IN",
        "TERM_DT_SK",
        "EXCD_LONG_TX1",
        "EXCD_LONG_TX2"
    ]
)

df_DefaultNA = spark.createDataFrame(
    [(1, 1, "NA", "1753-01-01", 1, 1, 1, 1, 1, "X", "X", "2199-12-31", 1, 1)],
    [
        "DSALW_EXCD_SK",
        "SRC_SYS_CD_SK",
        "EXCD_ID",
        "EFF_DT_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "EXCD_SK",
        "EXCD_RESP_CD_SK",
        "EXCD_STTUS_CD_SK",
        "BYPS_IN",
        "DSALW_IN",
        "TERM_DT_SK",
        "EXCD_LONG_TX1",
        "EXCD_LONG_TX2"
    ]
)

df_Collector = df_Fkey.unionByName(df_DefaultUNK).unionByName(df_DefaultNA)

df_Collector_final = df_Collector.select(
    col("DSALW_EXCD_SK"),
    col("SRC_SYS_CD_SK"),
    col("EXCD_ID"),
    col("EFF_DT_SK"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("EXCD_SK"),
    col("EXCD_RESP_CD_SK"),
    col("EXCD_STTUS_CD_SK"),
    col("BYPS_IN"),
    col("DSALW_IN"),
    col("TERM_DT_SK"),
    col("EXCD_LONG_TX1"),
    col("EXCD_LONG_TX2")
)

df_Collector_final = df_Collector_final.select(
    col("DSALW_EXCD_SK"),
    col("SRC_SYS_CD_SK"),
    rpad(col("EXCD_ID"), 4, " ").alias("EXCD_ID"),
    rpad(col("EFF_DT_SK"), 10, " ").alias("EFF_DT_SK"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("EXCD_SK"),
    rpad(col("EXCD_RESP_CD_SK"), 1, " ").alias("EXCD_RESP_CD_SK"),
    rpad(col("EXCD_STTUS_CD_SK"), 1, " ").alias("EXCD_STTUS_CD_SK"),
    rpad(col("BYPS_IN"), 1, " ").alias("BYPS_IN"),
    rpad(col("DSALW_IN"), 1, " ").alias("DSALW_IN"),
    rpad(col("TERM_DT_SK"), 10, " ").alias("TERM_DT_SK"),
    rpad(col("EXCD_LONG_TX1"), <...>, " ").alias("EXCD_LONG_TX1"),
    rpad(col("EXCD_LONG_TX2"), <...>, " ").alias("EXCD_LONG_TX2")
)

write_files(
    df_Collector_final,
    f"{adls_path}/load/DSALW_EXCD.dat",
    ",",
    "overwrite",
    False,
    True,
    "\"",
    None
)