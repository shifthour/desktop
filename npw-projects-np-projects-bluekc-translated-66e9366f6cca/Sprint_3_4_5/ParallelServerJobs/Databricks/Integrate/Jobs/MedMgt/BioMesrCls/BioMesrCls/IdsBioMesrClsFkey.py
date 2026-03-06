# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2011 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME: IdsBioMesrClsFkey
# MAGIC 
# MAGIC CALLED BY: IdsBioMesrClsLoadSeq
# MAGIC 
# MAGIC PROCESSING:   Creates a File "BIO_MESR_CLS.dat" after performing FKey Lookups to load the data into table BIO_MESR_CLS
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:             
# MAGIC                                                                                                                                                                                    
# MAGIC 
# MAGIC Developer                          Date               Project/Altiris #               Change Description                                                                            Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------                                                      ---------------------------------   ---------------------------------    -------------------------   
# MAGIC Raja Gummadi                12-20-2011      4765  - CDM                    Original Programming                                                                            IntegrateCurDevl          SAndrew                     2012-01-12

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
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType,
    TimestampType
)
from pyspark.sql.functions import col, lit, when, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Parameter retrieval
InFile = get_widget_value("InFile","")
Logging = get_widget_value("Logging","")
RunID = get_widget_value("RunID","")
RunCycle = get_widget_value("RunCycle","")
SrcSysCdSk = get_widget_value("SrcSysCdSk","")

# Schema for BioMesrClsCf (CSeqFileStage) input
schema_BioMesrClsCf = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38,10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("BIO_MESR_CLS_SK", IntegerType(), nullable=False),
    StructField("BIO_MESR_TYP_CD", StringType(), nullable=False),
    StructField("GNDR_CD", StringType(), nullable=False),
    StructField("AGE_RNG_MIN_YR_NO", IntegerType(), nullable=False),
    StructField("AGE_RNG_MAX_YR_NO", IntegerType(), nullable=False),
    StructField("BIO_MESR_RNG_LOW_NO", DecimalType(38,10), nullable=False),
    StructField("BIO_MESR_RNG_HI_NO", DecimalType(38,10), nullable=False),
    StructField("EFF_DT_SK", StringType(), nullable=False),
    StructField("SRC_SYS_CD_SK", IntegerType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("TERM_DT_SK", StringType(), nullable=False),
    StructField("BIO_CLS_CD", StringType(), nullable=False),
    StructField("BIO_CLS_RANK_NO", IntegerType(), nullable=False),
    StructField("USER_ID", StringType(), nullable=False),
    StructField("LAST_UPDT_DT_SK", StringType(), nullable=False)
])

# Read from BioMesrClsCf (CSeqFileStage)
df_BioMesrClsCf = (
    spark.read.format("csv")
    .schema(schema_BioMesrClsCf)
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "\"")
    .load(f"{adls_path}/key/" + InFile)
)

# Enrich (CTransformerStage: PurgeTrn) - Add stage variables
df_enriched = (
    df_BioMesrClsCf
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn("svBioClsCd", GetFkeyCodes(lit("UWS"), col("BIO_MESR_CLS_SK"), lit("BIO CLASS"), col("BIO_CLS_CD"), lit(Logging)))
    .withColumn("svBioMesrTypCd", GetFkeyCodes(lit("UWS"), col("BIO_MESR_CLS_SK"), lit("BIO MEASURE TYPE"), col("BIO_MESR_TYP_CD"), lit(Logging)))
    .withColumn("svGndrCd", GetFkeyCodes(lit("UWS"), col("BIO_MESR_CLS_SK"), lit("GENDER"), col("GNDR_CD"), lit(Logging)))
    .withColumn("ErrCount", GetFkeyErrorCnt(col("BIO_MESR_CLS_SK")))
)

# Output Link: ClsFkeyOut => Constraint: ErrCount = 0 OR PassThru = 'Y'
df_ClsFkeyOut = (
    df_enriched
    .filter((col("ErrCount") == 0) | (col("PassThru") == "Y"))
    .select(
        col("BIO_MESR_CLS_SK").alias("BIO_MESR_CLS_SK"),
        col("BIO_MESR_TYP_CD").alias("BIO_MESR_TYP_CD"),
        col("GNDR_CD").alias("GNDR_CD"),
        col("AGE_RNG_MIN_YR_NO").alias("AGE_RNG_MIN_YR_NO"),
        col("AGE_RNG_MAX_YR_NO").alias("AGE_RNG_MAX_YR_NO"),
        col("BIO_MESR_RNG_LOW_NO").alias("BIO_MESR_RNG_LOW_NO"),
        col("BIO_MESR_RNG_HI_NO").alias("BIO_MESR_RNG_HI_NO"),
        rpad(col("EFF_DT_SK"), 10, " ").alias("EFF_DT_SK"),
        col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("svBioClsCd").alias("BIO_CLS_CD_SK"),
        col("svBioMesrTypCd").alias("BIO_MESR_TYP_CD_SK"),
        col("svGndrCd").alias("GNDR_CD_SK"),
        rpad(col("TERM_DT_SK"), 10, " ").alias("TERM_DT_SK"),
        col("BIO_CLS_RANK_NO").alias("BIO_CLS_RANK_NO"),
        col("USER_ID").alias("LAST_UPDT_USER_ID"),
        rpad(col("LAST_UPDT_DT_SK"), 10, " ").alias("LAST_UPDT_DT_SK")
    )
)

# Output Link: lnkRecycle => Constraint: ErrCount > 0
df_lnkRecycle = (
    df_enriched
    .filter(col("ErrCount") > 0)
    .select(
        GetRecycleKey(col("BIO_MESR_CLS_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        rpad(col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
        rpad(col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
        rpad(col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
        col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        col("ErrCount").alias("ERR_CT"),
        (col("RECYCLE_CT") + lit(1)).alias("RECYCLE_CT"),
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        col("BIO_MESR_CLS_SK").alias("BIO_MESR_CLS_SK"),
        col("BIO_MESR_TYP_CD").alias("BIO_MESR_TYP_CD"),
        col("GNDR_CD").alias("GNDR_CD"),
        col("AGE_RNG_MIN_YR_NO").alias("AGE_RNG_MIN_YR_NO"),
        col("AGE_RNG_MAX_YR_NO").alias("AGE_RNG_MAX_YR_NO"),
        col("BIO_MESR_RNG_LOW_NO").alias("BIO_MESR_RNG_LOW_NO"),
        col("BIO_MESR_RNG_HI_NO").alias("BIO_MESR_RNG_HI_NO"),
        rpad(col("EFF_DT_SK"), 10, " ").alias("EFF_DT_SK"),
        col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXTCN_SK"),
        lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXTCN_SK"),
        rpad(col("TERM_DT_SK"), 10, " ").alias("TERM_DT_SK"),
        col("BIO_CLS_CD").alias("BIO_CLS_CD"),
        col("BIO_CLS_RANK_NO").alias("BIO_CLS_RANK_NO"),
        col("USER_ID").alias("USER_ID"),
        rpad(col("LAST_UPDT_DT_SK"), 10, " ").alias("LAST_UPDT_DT_SK")
    )
)

# Output Link: DefaultUNK => Constraint: @INROWNUM = 1
df_firstrow_count = df_enriched.limit(1).count()
if df_firstrow_count > 0:
    df_DefaultUNK = (
        df_enriched.limit(1)
        .select(
            lit(0).alias("BIO_MESR_CLS_SK"),
            lit("UNK").alias("BIO_MESR_TYP_CD"),
            lit("U").alias("GNDR_CD"),
            lit(0).alias("AGE_RNG_MIN_YR_NO"),
            lit(0).alias("AGE_RNG_MAX_YR_NO"),
            lit(0).alias("BIO_MESR_RNG_LOW_NO"),
            lit(0).alias("BIO_MESR_RNG_HI_NO"),
            rpad(lit("1753-01-01"), 10, " ").alias("EFF_DT_SK"),
            lit(0).alias("SRC_SYS_CD_SK"),
            lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
            lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            lit(0).alias("BIO_CLS_CD_SK"),
            lit(0).alias("BIO_MESR_TYP_CD_SK"),
            lit(0).alias("GNDR_CD_SK"),
            rpad(lit("2199-12-31"), 10, " ").alias("TERM_DT_SK"),
            lit(0).alias("BIO_CLS_RANK_NO"),
            lit("UNK").alias("LAST_UPDT_USER_ID"),
            rpad(lit("2199-12-31"), 10, " ").alias("LAST_UPDT_DT_SK")
        )
    )
else:
    df_DefaultUNK = spark.createDataFrame([], df_ClsFkeyOut.schema)

# Output Link: DefaultNA => Constraint: @INROWNUM = 1
if df_firstrow_count > 0:
    df_DefaultNA = (
        df_enriched.limit(1)
        .select(
            lit(1).alias("BIO_MESR_CLS_SK"),
            lit("NA").alias("BIO_MESR_TYP_CD"),
            lit("X").alias("GNDR_CD"),
            lit(1).alias("AGE_RNG_MIN_YR_NO"),
            lit(1).alias("AGE_RNG_MAX_YR_NO"),
            lit(1).alias("BIO_MESR_RNG_LOW_NO"),
            lit(1).alias("BIO_MESR_RNG_HI_NO"),
            rpad(lit("1753-01-01"), 10, " ").alias("EFF_DT_SK"),
            lit(1).alias("SRC_SYS_CD_SK"),
            lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
            lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            lit(1).alias("BIO_CLS_CD_SK"),
            lit(1).alias("BIO_MESR_TYP_CD_SK"),
            lit(1).alias("GNDR_CD_SK"),
            rpad(lit("2199-12-31"), 10, " ").alias("TERM_DT_SK"),
            lit(1).alias("BIO_CLS_RANK_NO"),
            lit("NA").alias("LAST_UPDT_USER_ID"),
            rpad(lit("2199-12-31"), 10, " ").alias("LAST_UPDT_DT_SK")
        )
    )
else:
    df_DefaultNA = spark.createDataFrame([], df_ClsFkeyOut.schema)

# Write the hashed file (hf_recycle) to parquet (Scenario C)
write_files(
    df_lnkRecycle, 
    f"{adls_path}/hf_recycle.parquet", 
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# Collector stage => union of ClsFkeyOut, DefaultUNK, DefaultNA
df_Collector = df_ClsFkeyOut.unionByName(df_DefaultUNK).unionByName(df_DefaultNA)

# Final output => BioMesrCls (CSeqFileStage)
write_files(
    df_Collector.select(
        "BIO_MESR_CLS_SK",
        "BIO_MESR_TYP_CD",
        "GNDR_CD",
        "AGE_RNG_MIN_YR_NO",
        "AGE_RNG_MAX_YR_NO",
        "BIO_MESR_RNG_LOW_NO",
        "BIO_MESR_RNG_HI_NO",
        "EFF_DT_SK",
        "SRC_SYS_CD_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "BIO_CLS_CD_SK",
        "BIO_MESR_TYP_CD_SK",
        "GNDR_CD_SK",
        "TERM_DT_SK",
        "BIO_CLS_RANK_NO",
        "LAST_UPDT_USER_ID",
        "LAST_UPDT_DT_SK"
    ),
    f"{adls_path}/load/BIO_MESR_CLS.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)