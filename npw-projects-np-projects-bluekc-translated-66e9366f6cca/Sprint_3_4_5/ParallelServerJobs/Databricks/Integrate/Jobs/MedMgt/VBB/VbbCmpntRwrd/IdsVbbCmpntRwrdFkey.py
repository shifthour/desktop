# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2013 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  IhmfConstituentVbbExtrSeq
# MAGIC 
# MAGIC PROCESSING:   Assigns Foreign Keys for the IDS table VBB_CMPNT_RWRD 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 	Project/Altiris #	Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------	------------------------	-----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Kalyan Neelam         2013-06-11           4963 VBB Phase III     Initial Programming                                                                      IntegrateNewDevl

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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DecimalType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Retrieve job parameters
InFile = get_widget_value('InFile','')
Logging = get_widget_value('Logging','')
RunID = get_widget_value('RunID','')

# --------------------------------------------------------------------------------
# Stage: IdsVbbCmpntRwrdExtr (CSeqFileStage)
# --------------------------------------------------------------------------------
schema_IdsVbbCmpntRwrdExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38,10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("VBB_CMPNT_RWRD_SK", IntegerType(), False),
    StructField("VBB_CMPNT_UNIQ_KEY", IntegerType(), False),
    StructField("VBB_CMPNT_RWRD_SEQ_NO", IntegerType(), False),
    StructField("SRC_SYS_CD_SK", IntegerType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("VNVN_ID", IntegerType(), False),
    StructField("VNRW_SEQ_NO", IntegerType(), False),
    StructField("ERN_RWRD_END_DT_TYP_CD", StringType(), False),
    StructField("ERN_RWRD_STRT_DT_TYP_CD", StringType(), False),
    StructField("VBB_CMPNT_RWRD_BEG_DT_SK", StringType(), False),
    StructField("VBB_CMPNT_RWRD_END_DT_SK", StringType(), False),
    StructField("ERN_RWRD_END_DT_SK", StringType(), False),
    StructField("ERN_RWRD_STRT_DT_SK", StringType(), False),
    StructField("SRC_SYS_CRT_DTM", TimestampType(), False),
    StructField("SRC_SYS_UPDT_DTM", TimestampType(), False),
    StructField("ERN_RWRD_AMT", DecimalType(38,10), False),
    StructField("RQRD_ACHV_LVL_NO", IntegerType(), False),
    StructField("VBB_CMPNT_RWRD_DESC", StringType(), False)
])

df_IdsVbbCmpntRwrdExtr = (
    spark.read.format("csv")
    .option("header", "false")
    .option("quote", "\"")
    .schema(schema_IdsVbbCmpntRwrdExtr)
    .load(f"{adls_path}/key/{InFile}")
)

# --------------------------------------------------------------------------------
# Stage: ForeignKey (CTransformerStage)
# --------------------------------------------------------------------------------
df_ForeignKey = (
    df_IdsVbbCmpntRwrdExtr
    .withColumn("PassThru", F.lit("Y"))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("VBB_CMPNT_RWRD_SK")))
    .withColumn(
        "svVbbCmpntSk",
        GetFkeyVbbCmpnt(
            F.col("SRC_SYS_CD"),
            F.col("VBB_CMPNT_RWRD_SK"),
            F.col("VBB_CMPNT_UNIQ_KEY"),
            Logging
        )
    )
    .withColumn(
        "svVbbRwrdSk",
        GetFkeyVbbRwrd(
            F.col("SRC_SYS_CD"),
            F.col("VBB_CMPNT_RWRD_SK"),
            F.col("VNVN_ID"),
            F.col("VNRW_SEQ_NO"),
            Logging
        )
    )
    .withColumn(
        "svErnRwrdEndDtTypCdSk",
        GetFkeyClctnDomainCodes(
            F.col("SRC_SYS_CD"),
            F.col("VBB_CMPNT_RWRD_SK"),
            F.lit("VBB REWARD END DATE TYPE"),
            F.lit("IHMF CONSTITUENT"),
            F.lit("VBB REWARD END DATE TYPE"),
            F.lit("IDS"),
            F.col("ERN_RWRD_END_DT_TYP_CD"),
            Logging
        )
    )
    .withColumn(
        "svErnRwrdStrtDtTypCdSk",
        GetFkeyClctnDomainCodes(
            F.col("SRC_SYS_CD"),
            F.col("VBB_CMPNT_RWRD_SK"),
            F.lit("VBB REWARD START DATE TYPE"),
            F.lit("IHMF CONSTITUENT"),
            F.lit("VBB REWARD START DATE TYPE"),
            F.lit("IDS"),
            F.col("ERN_RWRD_STRT_DT_TYP_CD"),
            Logging
        )
    )
    .withColumn(
        "svVbbCmpntRwrdBegDtSk",
        GetFkeyDate(
            F.lit("IDS"),
            F.col("VBB_CMPNT_RWRD_SK"),
            F.col("VBB_CMPNT_RWRD_BEG_DT_SK"),
            Logging
        )
    )
    .withColumn(
        "svVbbCmpntRwrdEndDtSk",
        GetFkeyDate(
            F.lit("IDS"),
            F.col("VBB_CMPNT_RWRD_SK"),
            F.col("VBB_CMPNT_RWRD_END_DT_SK"),
            Logging
        )
    )
    .withColumn(
        "svErntRwrdBegDtSk",
        GetFkeyDate(
            F.lit("IDS"),
            F.col("VBB_CMPNT_RWRD_SK"),
            F.col("ERN_RWRD_STRT_DT_SK"),
            Logging
        )
    )
    .withColumn(
        "svErntRwrdEndDtSk",
        GetFkeyDate(
            F.lit("IDS"),
            F.col("VBB_CMPNT_RWRD_SK"),
            F.col("ERN_RWRD_END_DT_SK"),
            Logging
        )
    )
)

df_foreignKey_Fkey = (
    df_ForeignKey
    .filter((F.col("ErrCount") == 0) | (F.col("PassThru") == "Y"))
    .select(
        F.col("VBB_CMPNT_RWRD_SK").alias("VBB_CMPNT_RWRD_SK"),
        F.col("VBB_CMPNT_UNIQ_KEY").alias("VBB_CMPNT_UNIQ_KEY"),
        F.col("VBB_CMPNT_RWRD_SEQ_NO").alias("VBB_CMPNT_RWRD_SEQ_NO"),
        F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("svVbbCmpntSk").alias("VBB_CMPNT_SK"),
        F.col("svVbbRwrdSk").alias("VBB_RWRD_SK"),
        F.col("svErnRwrdEndDtTypCdSk").alias("ERN_RWRD_END_DT_TYP_CD_SK"),
        F.col("svErnRwrdStrtDtTypCdSk").alias("ERN_RWRD_STRT_DT_TYP_CD_SK"),
        F.col("svVbbCmpntRwrdBegDtSk").alias("VBB_CMPNT_RWRD_BEG_DT_SK"),
        F.col("svVbbCmpntRwrdEndDtSk").alias("VBB_CMPNT_RWRD_END_DT_SK"),
        F.col("svErntRwrdEndDtSk").alias("ERN_RWRD_END_DT_SK"),
        F.col("svErntRwrdBegDtSk").alias("ERN_RWRD_STRT_DT_SK"),
        F.col("SRC_SYS_CRT_DTM").alias("SRC_SYS_CRT_DTM"),
        F.col("SRC_SYS_UPDT_DTM").alias("SRC_SYS_UPDT_DTM"),
        F.col("ERN_RWRD_AMT").alias("ERN_RWRD_AMT"),
        F.col("RQRD_ACHV_LVL_NO").alias("RQRD_ACHV_LVL_NO"),
        F.col("VBB_CMPNT_RWRD_DESC").alias("VBB_CMPNT_RWRD_DESC")
    )
)

df_foreignKey_Recycle = (
    df_ForeignKey
    .filter(F.col("ErrCount") > 0)
    .select(
        F.col("JOB_EXCTN_RCRD_ERR_SK"),
        F.expr("GetRecycleKey(VBB_CMPNT_RWRD_SK)").alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("DISCARD_IN").alias("DISCARD_IN"),
        F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("ErrCount").alias("ERR_CT"),
        (F.col("RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("VBB_CMPNT_RWRD_SK").alias("VBB_CMPNT_RWRD_SK"),
        F.col("VBB_CMPNT_UNIQ_KEY").alias("VBB_CMPNT_UNIQ_KEY"),
        F.col("VBB_CMPNT_RWRD_SEQ_NO").alias("VBB_CMPNT_RWRD_SEQ_NO"),
        F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("VNVN_ID").alias("VNVN_ID"),
        F.col("VNRW_SEQ_NO").alias("VNRW_SEQ_NO"),
        F.col("ERN_RWRD_END_DT_TYP_CD").alias("ERN_RWRD_END_DT_TYP_CD"),
        F.col("ERN_RWRD_STRT_DT_TYP_CD").alias("ERN_RWRD_STRT_DT_TYP_CD"),
        F.col("VBB_CMPNT_RWRD_BEG_DT_SK").alias("VBB_CMPNT_RWRD_BEG_DT_SK"),
        F.col("VBB_CMPNT_RWRD_END_DT_SK").alias("VBB_CMPNT_RWRD_END_DT_SK"),
        F.col("ERN_RWRD_END_DT_SK").alias("ERN_RWRD_END_DT_SK"),
        F.col("ERN_RWRD_STRT_DT_SK").alias("ERN_RWRD_STRT_DT_SK"),
        F.col("SRC_SYS_CRT_DTM").alias("SRC_SYS_CRT_DTM"),
        F.col("SRC_SYS_UPDT_DTM").alias("SRC_SYS_UPDT_DTM"),
        F.col("ERN_RWRD_AMT").alias("ERN_RWRD_AMT"),
        F.col("RQRD_ACHV_LVL_NO").alias("RQRD_ACHV_LVL_NO"),
        F.col("VBB_CMPNT_RWRD_DESC").alias("VBB_CMPNT_RWRD_DESC")
    )
)

# --------------------------------------------------------------------------------
# Stage: hf_recycle (CHashedFileStage) => Scenario C => write to parquet
# --------------------------------------------------------------------------------
# Prepare final select for hf_recycle with rpad for char columns
df_foreignKey_Recycle_final = df_foreignKey_Recycle.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("VBB_CMPNT_RWRD_SK"),
    F.col("VBB_CMPNT_UNIQ_KEY"),
    F.col("VBB_CMPNT_RWRD_SEQ_NO"),
    F.col("SRC_SYS_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("VNVN_ID"),
    F.col("VNRW_SEQ_NO"),
    F.col("ERN_RWRD_END_DT_TYP_CD"),
    F.col("ERN_RWRD_STRT_DT_TYP_CD"),
    F.rpad(F.col("VBB_CMPNT_RWRD_BEG_DT_SK"), 10, " ").alias("VBB_CMPNT_RWRD_BEG_DT_SK"),
    F.rpad(F.col("VBB_CMPNT_RWRD_END_DT_SK"), 10, " ").alias("VBB_CMPNT_RWRD_END_DT_SK"),
    F.rpad(F.col("ERN_RWRD_END_DT_SK"), 10, " ").alias("ERN_RWRD_END_DT_SK"),
    F.rpad(F.col("ERN_RWRD_STRT_DT_SK"), 10, " ").alias("ERN_RWRD_STRT_DT_SK"),
    F.col("SRC_SYS_CRT_DTM"),
    F.col("SRC_SYS_UPDT_DTM"),
    F.col("ERN_RWRD_AMT"),
    F.col("RQRD_ACHV_LVL_NO"),
    F.col("VBB_CMPNT_RWRD_DESC")
)

write_files(
    df_foreignKey_Recycle_final,
    "hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# --------------------------------------------------------------------------------
# Create single-row DataFrames for DefaultUNK and DefaultNA
# (Retrieve LAST_UPDT_RUN_CYC_EXCTN_SK from first row if available)
# --------------------------------------------------------------------------------
default_val = df_ForeignKey.select("LAST_UPDT_RUN_CYC_EXCTN_SK").limit(1).collect()
if default_val:
    last_updt_val = default_val[0][0]
else:
    last_updt_val = None

cols_collector_schema = StructType([
    StructField("VBB_CMPNT_RWRD_SK", IntegerType(), True),
    StructField("VBB_CMPNT_UNIQ_KEY", IntegerType(), True),
    StructField("VBB_CMPNT_RWRD_SEQ_NO", IntegerType(), True),
    StructField("SRC_SYS_CD_SK", IntegerType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("VBB_CMPNT_SK", IntegerType(), True),
    StructField("VBB_RWRD_SK", IntegerType(), True),
    StructField("ERN_RWRD_END_DT_TYP_CD_SK", IntegerType(), True),
    StructField("ERN_RWRD_STRT_DT_TYP_CD_SK", IntegerType(), True),
    StructField("VBB_CMPNT_RWRD_BEG_DT_SK", StringType(), True),
    StructField("VBB_CMPNT_RWRD_END_DT_SK", StringType(), True),
    StructField("ERN_RWRD_END_DT_SK", StringType(), True),
    StructField("ERN_RWRD_STRT_DT_SK", StringType(), True),
    StructField("SRC_SYS_CRT_DTM", StringType(), True),
    StructField("SRC_SYS_UPDT_DTM", StringType(), True),
    StructField("ERN_RWRD_AMT", DecimalType(38,10), True),
    StructField("RQRD_ACHV_LVL_NO", IntegerType(), True),
    StructField("VBB_CMPNT_RWRD_DESC", StringType(), True)
])

df_foreignKey_DefaultUNK = spark.createDataFrame(
    [
        (
            0,
            0,
            0,
            0,
            100,
            last_updt_val,
            0,
            0,
            0,
            0,
            "1753-01-01",
            "2199-12-31",
            "2199-12-31",
            "1753-01-01",
            "1753-01-01 00:00:00.000000",
            "1753-01-01 00:00:00.000000",
            0,
            0,
            "UNK"
        )
    ],
    cols_collector_schema
)

df_foreignKey_DefaultNA = spark.createDataFrame(
    [
        (
            1,
            1,
            1,
            1,
            100,
            last_updt_val,
            1,
            1,
            1,
            1,
            "1753-01-01",
            "2199-12-31",
            "2199-12-31",
            "1753-01-01",
            "1753-01-01 00:00:00.000000",
            "1753-01-01 00:00:00.000000",
            0,
            0,
            "NA"
        )
    ],
    cols_collector_schema
)

# --------------------------------------------------------------------------------
# Stage: Collector (CCollector) => union of Fkey, DefaultUNK, DefaultNA
# --------------------------------------------------------------------------------
df_Collector = (
    df_foreignKey_Fkey
    .union(df_foreignKey_DefaultUNK)
    .union(df_foreignKey_DefaultNA)
)

# --------------------------------------------------------------------------------
# Stage: VBB_CMPNT_RWRD (CSeqFileStage) => write to .dat
# --------------------------------------------------------------------------------
# Apply rpad on char(10) columns
df_Collector_final = df_Collector.select(
    F.col("VBB_CMPNT_RWRD_SK"),
    F.col("VBB_CMPNT_UNIQ_KEY"),
    F.col("VBB_CMPNT_RWRD_SEQ_NO"),
    F.col("SRC_SYS_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("VBB_CMPNT_SK"),
    F.col("VBB_RWRD_SK"),
    F.col("ERN_RWRD_END_DT_TYP_CD_SK"),
    F.col("ERN_RWRD_STRT_DT_TYP_CD_SK"),
    F.rpad(F.col("VBB_CMPNT_RWRD_BEG_DT_SK"), 10, " ").alias("VBB_CMPNT_RWRD_BEG_DT_SK"),
    F.rpad(F.col("VBB_CMPNT_RWRD_END_DT_SK"), 10, " ").alias("VBB_CMPNT_RWRD_END_DT_SK"),
    F.rpad(F.col("ERN_RWRD_END_DT_SK"), 10, " ").alias("ERN_RWRD_END_DT_SK"),
    F.rpad(F.col("ERN_RWRD_STRT_DT_SK"), 10, " ").alias("ERN_RWRD_STRT_DT_SK"),
    F.col("SRC_SYS_CRT_DTM"),
    F.col("SRC_SYS_UPDT_DTM"),
    F.col("ERN_RWRD_AMT"),
    F.col("RQRD_ACHV_LVL_NO"),
    F.col("VBB_CMPNT_RWRD_DESC")
)

write_files(
    df_Collector_final,
    f"{adls_path}/load/VBB_CMPNT_RWRD.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)