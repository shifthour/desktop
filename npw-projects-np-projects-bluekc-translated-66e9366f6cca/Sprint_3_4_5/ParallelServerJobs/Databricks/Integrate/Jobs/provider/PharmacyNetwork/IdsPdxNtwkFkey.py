# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 20019 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC DESCRIPTION:     Takes the CRF file and applies the foreign key and creates the load file for PDX_NTWK
# MAGIC INPUTS:                Output file from NABPPdxNtwkPkey
# MAGIC HASH FILES:         hf_recycle - writes recycle rows
# MAGIC  Called by:               OptumProvLoadSeq
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #                               Change Description                  Development Project      Code Reviewer          Date Reviewed       
# MAGIC --------------------            --------------------     ------------------------                              ----------------------------------------           --------------------------------       -------------------------------   ----------------------------       
# MAGIC Giri Mallavaram          10/16/2019      6131 PBM Replacement         Initial Programming                       IntegrateDevl                 Kalyan Neelam          2019-11-20
# MAGIC Velmani Kondappan   2020-08-26   6  264 - PBM Phase II -              Mapped ALWS_90_DAY_RX_IN  IntegrateDev5                                        
# MAGIC                                                          Government Programs              Column

# MAGIC Read common record format file from extract job.
# MAGIC Set all foreign surragote keys
# MAGIC Merge source data with default rows
# MAGIC Writing Sequential File to /load
# MAGIC Capture records generating translation errors to be uploaded to the IDS recycle table
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


InFile = get_widget_value('InFile', 'IdsPdxNtwkExtr.PdxNtwk.uniq')
Logging = get_widget_value('Logging', 'N')
SourceSystem = get_widget_value('SourceSystem', 'OPTUMRX')

schema_IdsMedDPdxNtwkExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), True),
    StructField("INSRT_UPDT_CD", StringType(), True),
    StructField("DISCARD_IN", StringType(), True),
    StructField("PASS_THRU_IN", StringType(), True),
    StructField("FIRST_RECYC_DT", TimestampType(), True),
    StructField("ERR_CT", IntegerType(), True),
    StructField("RECYCLE_CT", DecimalType(38, 10), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("PRI_KEY_STRING", StringType(), True),
    StructField("PDX_NTWK_SK", IntegerType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("PROV_ID", StringType(), True),
    StructField("PDX_NTWK_CD", IntegerType(), True),
    StructField("DIR_IN", StringType(), True),
    StructField("EFF_DT", StringType(), True),
    StructField("TERM_DT", StringType(), True),
    StructField("PDX_NTWK_PRFRD_PROV_IN", StringType(), True),
    StructField("ALWS_90_DAY_RX_IN", StringType(), True)
])

df_IdsMedDPdxNtwkExtr = (
    spark.read
    .option("header", "false")
    .option("quote", "\"")
    .option("delimiter", ",")
    .schema(schema_IdsMedDPdxNtwkExtr)
    .csv(f"{adls_path}/key/{InFile}")
)

df_foreignkey = (
    df_IdsMedDPdxNtwkExtr
    .withColumn("svEffDtSk", GetFkeyDate("IDS", F.col("PDX_NTWK_SK"), F.col("EFF_DT"), Logging))
    .withColumn("svTermDtSk", GetFkeyDate("IDS", F.col("PDX_NTWK_SK"), F.col("TERM_DT"), Logging))
    .withColumn("svSrcSysCdSk", GetFkeyCodes("IDS", F.col("PDX_NTWK_SK"), F.lit("SOURCE SYSTEM"), F.col("SRC_SYS_CD"), Logging))
    .withColumn("svProvSk", GetFkeyProv(F.col("SRC_SYS_CD"), F.col("PDX_NTWK_SK"), F.col("PROV_ID"), Logging))
)

if SourceSystem == "OPTUMRX":
    df_foreignkey = df_foreignkey.withColumn(
        "svProvNtwkCdSk",
        GetFkeyCodes("OPTUMRX", F.col("PDX_NTWK_SK"), F.lit("PHARMACY NETWORK"), F.col("PDX_NTWK_CD"), Logging)
    )
else:
    df_foreignkey = df_foreignkey.withColumn(
        "svProvNtwkCdSk",
        GetFkeyCodes("ESI", F.col("PDX_NTWK_SK"), F.lit("PHARMACY NETWORK"), F.col("PDX_NTWK_CD"), Logging)
    )

df_foreignkey = (
    df_foreignkey
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("PDX_NTWK_SK")))
)

df_foreignkey_fkey = df_foreignkey.filter((F.col("ErrCount") == 0) | (F.col("PassThru") == "Y"))
df_foreignkey_recycle = df_foreignkey.filter(F.col("ErrCount") > 0)

df_foreignkey_defaultUNK = spark.createDataFrame(
    [(0, 0, "UNK", 0, 0, 0, 0, "U", "UNK", "UNK", "U", "U")],
    [
        "PDX_NTWK_SK",
        "SRC_SYS_CD_SK",
        "PROV_ID",
        "PDX_NTWK_CD_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "PROV_SK",
        "DIR_IN",
        "EFF_DT_SK",
        "TERM_DT_SK",
        "PDX_NTWK_PRFRD_PROV_IN",
        "ALWS_90_DAY_RX_IN"
    ]
).limit(1)

df_foreignkey_defaultNA = spark.createDataFrame(
    [(1, 1, "NA", 1, 1, 1, 1, "N", "NA", "NA", "N", "N")],
    [
        "PDX_NTWK_SK",
        "SRC_SYS_CD_SK",
        "PROV_ID",
        "PDX_NTWK_CD_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "PROV_SK",
        "DIR_IN",
        "EFF_DT_SK",
        "TERM_DT_SK",
        "PDX_NTWK_PRFRD_PROV_IN",
        "ALWS_90_DAY_RX_IN"
    ]
).limit(1)

df_foreignkey_recycle_final = (
    df_foreignkey_recycle
    .withColumn("INSRT_UPDT_CD", rpad("INSRT_UPDT_CD", 10, " "))
    .withColumn("DISCARD_IN", rpad("DISCARD_IN", 1, " "))
    .withColumn("PASS_THRU_IN", rpad("PASS_THRU_IN", 1, " "))
    .withColumn("DIR_IN", rpad("DIR_IN", 1, " "))
    .withColumn("EFF_DT", rpad("EFF_DT", 10, " "))
    .withColumn("TERM_DT", rpad("TERM_DT", 10, " "))
    .withColumn("PDX_NTWK_PRFRD_PROV_IN", rpad("PDX_NTWK_PRFRD_PROV_IN", 1, " "))
    .withColumn("ALWS_90_DAY_RX_IN", rpad("ALWS_90_DAY_RX_IN", 1, " "))
    .select(
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "PASS_THRU_IN",
        "FIRST_RECYC_DT",
        "ErrCount",
        (F.col("RECYCLE_CT") + 1).alias("RECYCLE_CT"),
        "SRC_SYS_CD",
        "PRI_KEY_STRING",
        "PDX_NTWK_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "PROV_ID",
        "PDX_NTWK_CD",
        "DIR_IN",
        "EFF_DT",
        "TERM_DT",
        "PDX_NTWK_PRFRD_PROV_IN",
        "ALWS_90_DAY_RX_IN"
    )
)

write_files(
    df_foreignkey_recycle_final,
    "hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_collector_fkey = df_foreignkey_fkey.select(
    F.col("PDX_NTWK_SK").alias("PDX_NTWK_SK"),
    F.col("svSrcSysCdSk").alias("SRC_SYS_CD_SK"),
    F.col("PROV_ID").alias("PROV_ID"),
    F.col("svProvNtwkCdSk").alias("PDX_NTWK_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("svProvSk").alias("PROV_SK"),
    F.col("DIR_IN").alias("DIR_IN"),
    F.col("svEffDtSk").alias("EFF_DT_SK"),
    F.col("svTermDtSk").alias("TERM_DT_SK"),
    F.col("PDX_NTWK_PRFRD_PROV_IN").alias("PDX_NTWK_PRFRD_PROV_IN"),
    F.col("ALWS_90_DAY_RX_IN").alias("ALWS_90_DAY_RX_IN")
)

df_collector = df_collector_fkey.union(df_foreignkey_defaultUNK).union(df_foreignkey_defaultNA)

df_collector_final = (
    df_collector
    .withColumn("DIR_IN", rpad("DIR_IN", 1, " "))
    .withColumn("EFF_DT_SK", rpad("EFF_DT_SK", 10, " "))
    .withColumn("TERM_DT_SK", rpad("TERM_DT_SK", 10, " "))
    .withColumn("PDX_NTWK_PRFRD_PROV_IN", rpad("PDX_NTWK_PRFRD_PROV_IN", 1, " "))
    .withColumn("ALWS_90_DAY_RX_IN", rpad("ALWS_90_DAY_RX_IN", 1, " "))
    .select(
        "PDX_NTWK_SK",
        "SRC_SYS_CD_SK",
        "PROV_ID",
        "PDX_NTWK_CD_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "PROV_SK",
        "DIR_IN",
        "EFF_DT_SK",
        "TERM_DT_SK",
        "PDX_NTWK_PRFRD_PROV_IN",
        "ALWS_90_DAY_RX_IN"
    )
)

write_files(
    df_collector_final,
    f"PDX_NTWK.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)