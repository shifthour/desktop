# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2020 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsClmLnOvrdFkey
# MAGIC 
# MAGIC DESCRIPTION:     Takes the sorted file and applies the foreign keys.  Output is final table format for CLM_LN_OVRD
# MAGIC       
# MAGIC 
# MAGIC INPUTS:              Common record format fromt the IdsClmLnOvrdPkey job.   /ids/key/IdsClmLnOvrdPkey.*ClmLnOvrd.*
# MAGIC 	
# MAGIC   
# MAGIC HASH FILES:      hf_recycle
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:   
# MAGIC                            GetFkeyCodes()
# MAGIC                           GetFkeyClmLn()
# MAGIC                           GetFkeyAppUsr()
# MAGIC                           GetFkeyExcd()
# MAGIC                           GetFkeyDate()
# MAGIC                           GetFkeyErrorCnt)
# MAGIC 
# MAGIC PROCESSING:    read in the sorted sequential file and do foreign key lookups.   
# MAGIC 
# MAGIC                             No date parameters or ODBC connections are made within this job.   Just sequential and hash file processing.
# MAGIC 
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC             Steph Goddard  06/2004  -   Originally Programmed
# MAGIC             Sharon Andrew   06/29/2004 - Converted the EXCD SK lookup from a CDMA lookup to a hash file lookup.    Added trims to string parameters within the Foreign key functions.
# MAGIC             Suzanne Saylor 3/1/2006 - Removed unused parameters and renamed links
# MAGIC 
# MAGIC 
# MAGIC Reddy Sanam       2020-10-10                                      Created stage variable "svSrcSysCd" to map FACETS
# MAGIC                                                                                      when the source is LUMERIS                                                      IntegrateDev2
# MAGIC 
# MAGIC Sunitha Ganta         10-12-2020                                           Brought up to standards

# MAGIC Read common record format file.
# MAGIC Writing Sequential File to /load
# MAGIC Merge source data with default rows
# MAGIC Set all foreign surragote keys
# MAGIC This hash file is written to by all claim foriegn key jobs.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DecimalType,
    TimestampType
)
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


Logging = get_widget_value("Logging", "Y")
InFile = get_widget_value("InFile", "IdsClmLineOvrPkey.TMP")
Source = get_widget_value("Source", "")

schema_ClmLnOvrdExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38, 10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("CLM_LN_OVRD_SK", IntegerType(), nullable=False),
    StructField("CLM_ID", StringType(), nullable=False),
    StructField("CLM_LN_SEQ_NO", IntegerType(), nullable=False),
    StructField("CLM_LN_OVRD_ID", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("USER_ID", StringType(), nullable=False),
    StructField("CLM_LN_OVRD_EXCD", StringType(), nullable=False),
    StructField("OVRD_DT", StringType(), nullable=False),
    StructField("OVRD_AMT", DecimalType(38, 10), nullable=False),
    StructField("OVRD_VAL_DESC", StringType(), nullable=False)
])

df_ClmLnOvrdExtr = (
    spark.read
    .schema(schema_ClmLnOvrdExtr)
    .option("sep", ",")
    .option("quote", "\"")
    .option("header", False)
    .csv(f"{adls_path}/key/{InFile}")
)

df_ForeignKey = (
    df_ClmLnOvrdExtr
    .withColumn("svSrcSysCd", F.when(trim(F.col("SRC_SYS_CD")) == 'LUMERIS', F.lit("FACETS"))
                .otherwise(trim(F.col("SRC_SYS_CD"))))
    .withColumn(
        "SrcSysCdSk",
        GetFkeyCodes(
            "IDS",
            F.col("CLM_LN_OVRD_SK"),
            F.lit("SOURCE SYSTEM"),
            trim(F.col("SRC_SYS_CD")),
            Logging
        )
    )
    .withColumn(
        "ClmLnSk",
        GetFkeyClmLn(
            trim(F.col("SRC_SYS_CD")),
            F.col("CLM_LN_OVRD_SK"),
            F.col("CLM_ID"),
            F.col("CLM_LN_SEQ_NO"),
            Logging
        )
    )
    .withColumn(
        "UserID",
        GetFkeyAppUsr(
            trim(F.col("SRC_SYS_CD")),
            F.col("CLM_LN_OVRD_SK"),
            trim(F.col("USER_ID")),
            Logging
        )
    )
    .withColumn(
        "ClmOvrdExpl",
        GetFkeyExcd(
            F.col("svSrcSysCd"),
            F.col("CLM_LN_OVRD_SK"),
            trim(F.col("CLM_LN_OVRD_EXCD")),
            Logging
        )
    )
    .withColumn(
        "OvrdDt",
        GetFkeyDate(
            "IDS",
            F.col("CLM_LN_OVRD_SK"),
            F.col("OVRD_DT"),
            Logging
        )
    )
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("CLM_LN_OVRD_SK")))
)

df_Fkey = (
    df_ForeignKey
    .filter((F.col("ErrCount") == 0) | (F.col("PassThru") == 'Y'))
    .select(
        F.col("CLM_LN_OVRD_SK").alias("CLM_LN_OVRD_SK"),
        F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
        F.col("CLM_ID").alias("CLM_ID"),
        F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.col("CLM_LN_OVRD_ID").alias("CLM_LN_OVRD_ID"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("ClmLnSk").alias("CLM_LN_SK"),
        F.col("UserID").alias("USER_ID_SK"),
        F.col("ClmOvrdExpl").alias("CLM_LN_OVRD_EXCD_SK"),
        F.col("OvrdDt").alias("OVRD_DT_SK"),
        F.col("OVRD_AMT").alias("OVRD_AMT"),
        F.col("OVRD_VAL_DESC").alias("OVRD_VAL_DESC")
    )
)

df_DefaultUNK = (
    df_ForeignKey
    .limit(1)
    .select(
        F.lit(0).alias("CLM_LN_OVRD_SK"),
        F.lit(0).alias("SRC_SYS_CD_SK"),
        F.lit("UNK").alias("CLM_ID"),
        F.lit(0).alias("CLM_LN_SEQ_NO"),
        F.lit("UNK").alias("CLM_LN_OVRD_ID"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("CLM_LN_SK"),
        F.lit(0).alias("USER_ID_SK"),
        F.lit(0).alias("CLM_LN_OVRD_EXCD_SK"),
        F.lit("NA").alias("OVRD_DT_SK"),
        F.lit(0).alias("OVRD_AMT"),
        F.lit("UNK").alias("OVRD_VAL_DESC")
    )
)

df_DefaultNA = (
    df_ForeignKey
    .limit(1)
    .select(
        F.lit(1).alias("CLM_LN_OVRD_SK"),
        F.lit(1).alias("SRC_SYS_CD_SK"),
        F.lit("NA").alias("CLM_ID"),
        F.lit(1).alias("CLM_LN_SEQ_NO"),
        F.lit("NA").alias("CLM_LN_OVRD_ID"),
        F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("CLM_LN_SK"),
        F.lit(1).alias("USER_ID_SK"),
        F.lit(1).alias("CLM_LN_OVRD_EXCD_SK"),
        F.lit("NA").alias("OVRD_DT_SK"),
        F.lit(0).alias("OVRD_AMT"),
        F.lit("NA").alias("OVRD_VAL_DESC")
    )
)

df_Recycle = (
    df_ForeignKey
    .filter(F.col("ErrCount") > 0)
    .select(
        GetRecycleKey(F.col("CLM_LN_OVRD_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("DISCARD_IN").alias("DISCARD_IN"),
        F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("ErrCount").alias("ERR_CT"),
        (F.col("RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("CLM_LN_OVRD_SK").alias("CLM_LN_OVRD_SK"),
        F.col("CLM_ID").alias("CLM_ID"),
        F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.col("CLM_LN_OVRD_ID").alias("CLM_LN_OVRD_ID"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("USER_ID").alias("USER_ID"),
        F.col("CLM_LN_OVRD_EXCD").alias("CLM_LN_OVRD_EXCD"),
        F.col("OVRD_DT").alias("OVRD_DT"),
        F.col("OVRD_AMT").alias("OVRD_AMT"),
        F.col("OVRD_VAL_DESC").alias("OVRD_VAL_DESC")
    )
)

df_Recycle_out = (
    df_Recycle
    .select(
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "PASS_THRU_IN",
        "FIRST_RECYC_DT",
        "ERR_CT",
        "RECYCLE_CT",
        "SRC_SYS_CD",
        "PRI_KEY_STRING",
        "CLM_LN_OVRD_SK",
        "CLM_ID",
        "CLM_LN_SEQ_NO",
        "CLM_LN_OVRD_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "USER_ID",
        "CLM_LN_OVRD_EXCD",
        "OVRD_DT",
        "OVRD_AMT",
        "OVRD_VAL_DESC"
    )
    .withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " "))
    .withColumn("SRC_SYS_CD", F.rpad(F.col("SRC_SYS_CD"), 50, " "))
    .withColumn("PRI_KEY_STRING", F.rpad(F.col("PRI_KEY_STRING"), 50, " "))
    .withColumn("CLM_ID", F.rpad(F.col("CLM_ID"), 18, " "))
    .withColumn("CLM_LN_OVRD_ID", F.rpad(F.col("CLM_LN_OVRD_ID"), 50, " "))
    .withColumn("USER_ID", F.rpad(F.col("USER_ID"), 50, " "))
    .withColumn("CLM_LN_OVRD_EXCD", F.rpad(F.col("CLM_LN_OVRD_EXCD"), 50, " "))
    .withColumn("OVRD_DT", F.rpad(F.col("OVRD_DT"), 50, " "))
    .withColumn("OVRD_VAL_DESC", F.rpad(F.col("OVRD_VAL_DESC"), 50, " "))
)

write_files(
    df_Recycle_out,
    f"{adls_path}/hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_Recycle_Clms = (
    df_ForeignKey
    .filter(F.col("ErrCount") > 0)
    .select(
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("CLM_ID").alias("CLM_ID")
    )
)

df_Recycle_Clms_out = (
    df_Recycle_Clms
    .select("SRC_SYS_CD", "CLM_ID")
    .withColumn("SRC_SYS_CD", F.rpad(F.col("SRC_SYS_CD"), 50, " "))
    .withColumn("CLM_ID", F.rpad(F.col("CLM_ID"), 18, " "))
)

write_files(
    df_Recycle_Clms_out,
    f"{adls_path}/hf_claim_recycle_keys.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_collector = df_Fkey.unionByName(df_DefaultUNK).unionByName(df_DefaultNA)

df_collector_out = (
    df_collector
    .select(
        "CLM_LN_OVRD_SK",
        "SRC_SYS_CD_SK",
        "CLM_ID",
        "CLM_LN_SEQ_NO",
        "CLM_LN_OVRD_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CLM_LN_SK",
        "USER_ID_SK",
        "CLM_LN_OVRD_EXCD_SK",
        "OVRD_DT_SK",
        "OVRD_AMT",
        "OVRD_VAL_DESC"
    )
    .withColumn("CLM_ID", F.rpad(F.col("CLM_ID"), 18, " "))
    .withColumn("CLM_LN_OVRD_ID", F.rpad(F.col("CLM_LN_OVRD_ID"), 50, " "))
    .withColumn("OVRD_DT_SK", F.rpad(F.col("OVRD_DT_SK"), 10, " "))
    .withColumn("OVRD_VAL_DESC", F.rpad(F.col("OVRD_VAL_DESC"), 50, " "))
    .withColumn("CLM_LN_OVRD_EXCD_SK", F.rpad(F.col("CLM_LN_OVRD_EXCD_SK"), 50, " "))
    .withColumn("USER_ID_SK", F.rpad(F.col("USER_ID_SK"), 50, " "))
)

write_files(
    df_collector_out,
    f"{adls_path}/load/CLM_LN_OVRD.{Source}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)