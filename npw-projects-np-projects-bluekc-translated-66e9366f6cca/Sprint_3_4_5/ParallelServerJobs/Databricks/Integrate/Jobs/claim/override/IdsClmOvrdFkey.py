# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2020 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsClmAtchmntFkey
# MAGIC 
# MAGIC DESCRIPTION:     Takes the sorted file and applies the foreign keys.  Output is final table format for CLM_ATCHMT
# MAGIC       
# MAGIC 
# MAGIC INPUTS:   Common record format file from Primary key assignment job
# MAGIC 	
# MAGIC   
# MAGIC HASH FILES:     hf_recycle - used to keep records with an error in surrogate key assignment
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  none
# MAGIC                            
# MAGIC 
# MAGIC PROCESSING:  assign foreign (surrogate) keys to record
# MAGIC 
# MAGIC OUTPUTS:  file ready to load to CLM_OVRD table in IDS
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC             Steph Goddard  06/2004        -   Originally Programmed
# MAGIC             Brent Leland      09/09/2004  -   Added default rows for UNK and NA
# MAGIC             Steph Goddard  02/16/2006      Changes for sequencer
# MAGIC 
# MAGIC 
# MAGIC Emran.Mohammad               2020-10-12                                              brought up to standards

# MAGIC Read common record format file.
# MAGIC Writing Sequential File to /load
# MAGIC This hash file is written to by all claim foriegn key jobs.
# MAGIC check for foreign keys - write out record to recycle file if errors
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


# MAGIC %run ../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

# Retrieve parameter values
Source = get_widget_value('Source', '')
InFile = get_widget_value('InFile', 'IdsClmOvrdPkey.TMP')
Logging = get_widget_value('Logging', 'Y')

# Define schema for the input file read by ClmOvrdExtr
schema_ClmOvrdExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38, 10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("CLM_OVRD_SK", IntegerType(), False),
    StructField("SRC_SYS_CD_SK", IntegerType(), False),
    StructField("CLM_ID", StringType(), False),
    StructField("CLM_OVRD_ID", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("CLM_SK", IntegerType(), False),
    StructField("USER_ID", IntegerType(), False),
    StructField("CLM_OVRD_EXCD", StringType(), False),
    StructField("OVERRIDE_DT", StringType(), False),
    StructField("OVERRIDE_AMT", DecimalType(38, 10), False),
    StructField("OVERRIDE_VAL_DESC", StringType(), False)
])

# Read the input file for ClmOvrdExtr (CSeqFileStage)
df_ClmOvrdExtr = (
    spark.read
    .option("header", "false")
    .option("quote", "\"")
    .option("delimiter", ",")
    .schema(schema_ClmOvrdExtr)
    .csv(f"{adls_path}/key/{InFile}")
)

# Apply the ForeignKey Transformer logic (CTransformerStage) with stage variables
df_ForeignKeyVars = (
    df_ClmOvrdExtr
    .withColumn("OvrdDt", GetFkeyDate(F.lit("IDS"), F.col("CLM_OVRD_SK"), F.col("OVERRIDE_DT"), F.lit(Logging)))
    .withColumn("UserID", GetFkeyAppUsr(F.col("SRC_SYS_CD"), F.col("CLM_OVRD_SK"), F.col("USER_ID"), F.lit(Logging)))
    .withColumn("ClmOvrdExpl", GetFkeyExcd(F.col("SRC_SYS_CD"), F.col("CLM_OVRD_SK"), F.col("CLM_OVRD_EXCD"), F.lit(Logging)))
    .withColumn("SrcSysCdSk", GetFkeyCodes(F.lit("IDS"), F.col("CLM_OVRD_SK"), F.lit("SOURCE SYSTEM"), F.col("SRC_SYS_CD"), F.lit(Logging)))
    .withColumn("ClmSk", GetFkeyClm(F.col("SRC_SYS_CD"), F.col("CLM_OVRD_SK"), F.col("CLM_ID"), F.lit(Logging)))
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("CLM_OVRD_SK")))
)

# Split into multiple outputs based on constraints:

# Fkey output link (ErrCount = 0 Or PassThru = 'Y')
dfFkey = (
    df_ForeignKeyVars
    .filter((F.col("ErrCount") == 0) | (F.col("PassThru") == 'Y'))
    .select(
        F.col("CLM_OVRD_SK").alias("CLM_OVRD_SK"),
        F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
        F.col("CLM_ID").alias("CLM_ID"),
        F.col("CLM_OVRD_ID").alias("CLM_OVRD_ID"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("ClmSk").alias("CLM_SK"),
        F.col("UserID").alias("USER_ID_SK"),
        F.col("ClmOvrdExpl").alias("CLM_OVRD_EXCD_SK"),
        F.col("OvrdDt").alias("OVRD_DT_SK"),
        F.col("OVERRIDE_AMT").alias("OVRD_AMT"),
        F.col("OVERRIDE_VAL_DESC").alias("OVRD_VAL_DESC")
    )
)

# recycle output link (ErrCount > 0)
dfRecycle = (
    df_ForeignKeyVars
    .filter(F.col("ErrCount") > 0)
    .select(
        GetRecycleKey(F.col("CLM_OVRD_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("DISCARD_IN").alias("DISCARD_IN"),
        F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("ErrCount").alias("ERR_CT"),
        (F.col("RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("CLM_OVRD_SK").alias("CLM_OVRD_SK"),
        F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("CLM_ID").alias("CLM_ID"),
        F.col("CLM_OVRD_ID").alias("CLM_OVRD_ID"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("CLM_SK").alias("CLM_SK"),
        F.col("USER_ID").alias("USER_ID"),
        F.col("CLM_OVRD_EXCD").alias("CLM_OVRD_EXCD"),
        F.col("OVERRIDE_DT").alias("OVERRIDE_DT"),
        F.col("OVERRIDE_AMT").alias("OVERRIDE_AMT"),
        F.col("OVERRIDE_VAL_DESC").alias("OVERRIDE_VAL_DESC")
    )
)

# DefaultUNK output link (@INROWNUM = 1) - one row of defaults
dfDefaultUNK = spark.createDataFrame(
    [
        (0, 0, "UNK", "UNK", 0, 0, 0, 0, 0, "NA", 0, "UNK")
    ],
    [
        "CLM_OVRD_SK",
        "SRC_SYS_CD_SK",
        "CLM_ID",
        "CLM_OVRD_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CLM_SK",
        "USER_ID_SK",
        "CLM_OVRD_EXCD_SK",
        "OVRD_DT_SK",
        "OVRD_AMT",
        "OVRD_VAL_DESC"
    ]
)

# DefaultNA output link (@INROWNUM = 1) - one row of defaults
dfDefaultNA = spark.createDataFrame(
    [
        (1, 1, "NA", "NA", 1, 1, 1, 1, 1, "NA", 0, "NA")
    ],
    [
        "CLM_OVRD_SK",
        "SRC_SYS_CD_SK",
        "CLM_ID",
        "CLM_OVRD_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CLM_SK",
        "USER_ID_SK",
        "CLM_OVRD_EXCD_SK",
        "OVRD_DT_SK",
        "OVRD_AMT",
        "OVRD_VAL_DESC"
    ]
)

# Recycle_Clms output link (ErrCount > 0)
dfRecycleClms = (
    df_ForeignKeyVars
    .filter(F.col("ErrCount") > 0)
    .select(
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("CLM_ID").alias("CLM_ID")
    )
)

# Write the recycle hashed file (hf_recycle) as parquet (Scenario C)
# Apply rpad for char columns
dfRecycleRpad = (
    dfRecycle
    .withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " "))
    .withColumn("CLM_ID", F.rpad(F.col("CLM_ID"), 18, " "))
    .withColumn("CLM_OVRD_EXCD", F.rpad(F.col("CLM_OVRD_EXCD"), 10, " "))
    .withColumn("OVERRIDE_DT", F.rpad(F.col("OVERRIDE_DT"), 10, " "))
)

write_files(
    dfRecycleRpad.select(
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "PASS_THRU_IN",
        "FIRST_RECYC_DT",
        "ERR_CT",
        "RECYCLE_CT",
        "SRC_SYS_CD",
        "PRI_KEY_STRING",
        "CLM_OVRD_SK",
        "SRC_SYS_CD_SK",
        "CLM_ID",
        "CLM_OVRD_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CLM_SK",
        "USER_ID",
        "CLM_OVRD_EXCD",
        "OVERRIDE_DT",
        "OVERRIDE_AMT",
        "OVERRIDE_VAL_DESC"
    ),
    f"{adls_path}/hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

# Write the claim recycle keys hashed file (hf_claim_recycle_keys) as parquet (Scenario C)
dfRecycleClmsRpad = (
    dfRecycleClms
    .withColumn("CLM_ID", F.rpad(F.col("CLM_ID"), 18, " "))
)

write_files(
    dfRecycleClmsRpad.select("SRC_SYS_CD", "CLM_ID"),
    f"{adls_path}/hf_claim_recycle_keys.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

# Collector (CCollector) merges DefaultNA, DefaultUNK, and Fkey
dfCollector = dfDefaultNA.unionByName(dfDefaultUNK).unionByName(dfFkey)

# Final rpad for char columns in the collector output
dfCollectorRpad = (
    dfCollector
    .withColumn("CLM_ID", F.rpad(F.col("CLM_ID"), 18, " "))
    .withColumn("OVRD_DT_SK", F.rpad(F.col("OVRD_DT_SK"), 10, " "))
)

# Write out to ClmOvrd (CSeqFileStage) as a delimited file
write_files(
    dfCollectorRpad.select(
        "CLM_OVRD_SK",
        "SRC_SYS_CD_SK",
        "CLM_ID",
        "CLM_OVRD_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CLM_SK",
        "USER_ID_SK",
        "CLM_OVRD_EXCD_SK",
        "OVRD_DT_SK",
        "OVRD_AMT",
        "OVRD_VAL_DESC"
    ),
    f"{adls_path}/load/CLM_OVRD.{Source}.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)