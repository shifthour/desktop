# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2013 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:   Assigns Foreign Keys for the IDS table MBR_VBB_PLN_ENR.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 	Project/Altiris #	Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------	------------------------	-----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Raja Gummadi         2013-05-25           4963 VBB Phase III     Initial Programming                                                                      IntegrateNewDevl           Bhoomi Dasari           7/8/2013

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
    TimestampType,
    DecimalType
)
from pyspark.sql.functions import col, lit, when, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

InFile = get_widget_value("InFile","IdsMbrVbbPlnEnrExtr.MbrVbbPlnEnr.dat.100")
Logging = get_widget_value("Logging","X")
RunID = get_widget_value("RunID","100")

schema_IdsMbrVbbPlnEnrExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38,10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("MBR_VBB_PLN_ENR_SK", IntegerType(), nullable=False),
    StructField("MBR_UNIQ_KEY", IntegerType(), nullable=False),
    StructField("VBB_PLN_UNIQ_KEY", IntegerType(), nullable=False),
    StructField("SRC_SYS_CD_SK", IntegerType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=True),
    StructField("HIPL_ID", IntegerType(), nullable=False),
    StructField("HIEL_LOG_LEVEL1", StringType(), nullable=True),
    StructField("HIEL_LOG_LEVEL2", StringType(), nullable=True),
    StructField("HIEL_LOG_LEVEL3", StringType(), nullable=True),
    StructField("MEHP_ENROLLED_BY", StringType(), nullable=True),
    StructField("MEHP_STATUS", StringType(), nullable=False),
    StructField("MEHP_TERMED_BY", StringType(), nullable=True),
    StructField("MEHP_TERM_REASON", StringType(), nullable=True),
    StructField("MBR_VBB_PLN_CMPLTN_DT_SK", StringType(), nullable=False),
    StructField("MBR_VBB_PLN_ENR_DT_SK", StringType(), nullable=False),
    StructField("MBR_VBB_PLN_TERM_DT_SK", StringType(), nullable=False),
    StructField("SRC_SYS_CRT_DTM", TimestampType(), nullable=False),
    StructField("SRC_SYS_UPDT_DTM", TimestampType(), nullable=False),
    StructField("TRZ_MBR_UNVRS_ID", StringType(), nullable=False)
])

df_IdsMbrVbbPlnEnrExtr = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "\"")
    .schema(schema_IdsMbrVbbPlnEnrExtr)
    .load(f"{adls_path}/key/{InFile}")
)

df_ForeignKey = (
    df_IdsMbrVbbPlnEnrExtr
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn("svClsSk", GetFkeyCls('FACETS', col("MBR_VBB_PLN_ENR_SK"), col("HIEL_LOG_LEVEL1"), col("HIEL_LOG_LEVEL2"), Logging))
    .withColumn("svClsPlnSk", GetFkeyClsPln('FACETS', col("MBR_VBB_PLN_ENR_SK"), col("HIEL_LOG_LEVEL3"), Logging))
    .withColumn("svGrpSk", GetFkeyGrp('FACETS', col("MBR_VBB_PLN_ENR_SK"), col("HIEL_LOG_LEVEL1"), Logging))
    .withColumn("svMbrSk", GetFkeyMbr('FACETS', col("MBR_VBB_PLN_ENR_SK"), col("MBR_UNIQ_KEY"), Logging))
    .withColumn("svVbbPlnSk", GetFkeyVbbPln(col("SRC_SYS_CD"), col("MBR_VBB_PLN_ENR_SK"), col("HIPL_ID"), Logging))
    .withColumn("svMbrVbbPlnEnrMethCdSk", GetFkeyClctnDomainCodes(
        col("SRC_SYS_CD"),
        col("MBR_VBB_PLN_ENR_SK"),
        lit("VBB ENROLLMENT TERMINATION METHOD"),
        lit("IHMF CONSTITUENT"),
        lit("VBB ENROLLMENT TERMINATION METHOD"),
        lit("IDS"),
        col("MEHP_ENROLLED_BY"),
        Logging
    ))
    .withColumn("svMbrVbbPlnSttusCdSk", GetFkeyClctnDomainCodes(
        col("SRC_SYS_CD"),
        col("MBR_VBB_PLN_ENR_SK"),
        lit("VBB STATUS"),
        lit("IHMF CONSTITUENT"),
        lit("VBB STATUS"),
        lit("IDS"),
        col("MEHP_STATUS"),
        Logging
    ))
    .withColumn("svMbrVbbPlnTermMethCdSk", GetFkeyClctnDomainCodes(
        col("SRC_SYS_CD"),
        col("MBR_VBB_PLN_ENR_SK"),
        lit("VBB ENROLLMENT TERMINATION METHOD"),
        lit("IHMF CONSTITUENT"),
        lit("VBB ENROLLMENT TERMINATION METHOD"),
        lit("IDS"),
        col("MEHP_TERMED_BY"),
        Logging
    ))
    .withColumn("svMbrVbbPlnTermRsnCdSk", GetFkeyClctnDomainCodes(
        col("SRC_SYS_CD"),
        col("MBR_VBB_PLN_ENR_SK"),
        lit("VBB TERMINATION REASON"),
        lit("IHMF CONSTITUENT"),
        lit("VBB TERMINATION REASON"),
        lit("IDS"),
        col("MEHP_TERM_REASON"),
        Logging
    ))
    .withColumn("ErrCount", GetFkeyErrorCnt(col("MBR_VBB_PLN_ENR_SK")))
)

df_ForeignKey_Fkey = df_ForeignKey.filter(
    (col("ErrCount") == 0) | (col("PassThru") == 'Y')
).select(
    col("MBR_VBB_PLN_ENR_SK").alias("MBR_VBB_PLN_ENR_SK"),
    col("MBR_UNIQ_KEY"),
    col("VBB_PLN_UNIQ_KEY"),
    col("SRC_SYS_CD_SK"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("svClsSk").alias("CLS_SK"),
    col("svClsPlnSk").alias("CLS_PLN_SK"),
    col("svGrpSk").alias("GRP_SK"),
    col("svMbrSk").alias("MBR_SK"),
    col("svVbbPlnSk").alias("VBB_PLN_SK"),
    col("svMbrVbbPlnEnrMethCdSk").alias("MBR_VBB_PLN_ENR_METH_CD_SK"),
    col("svMbrVbbPlnSttusCdSk").alias("MBR_VBB_PLN_STTUS_CD_SK"),
    col("svMbrVbbPlnTermMethCdSk").alias("MBR_VBB_PLN_TERM_METH_CD_SK"),
    col("svMbrVbbPlnTermRsnCdSk").alias("MBR_VBB_PLN_TERM_RSN_CD_SK"),
    col("MBR_VBB_PLN_CMPLTN_DT_SK"),
    col("MBR_VBB_PLN_ENR_DT_SK"),
    col("MBR_VBB_PLN_TERM_DT_SK"),
    col("SRC_SYS_CRT_DTM"),
    col("SRC_SYS_UPDT_DTM"),
    col("TRZ_MBR_UNVRS_ID")
)

df_ForeignKey_Recycle = df_ForeignKey.filter(
    col("ErrCount") > 0
).select(
    GetRecycleKey(col("MBR_VBB_PLN_ENR_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
    col("INSRT_UPDT_CD"),
    col("DISCARD_IN"),
    col("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ErrCount").alias("ERR_CT"),
    (col("RECYCLE_CT") + 1).alias("RECYCLE_CT"),
    col("SRC_SYS_CD"),
    col("PRI_KEY_STRING"),
    col("MBR_VBB_PLN_ENR_SK"),
    col("MBR_UNIQ_KEY"),
    col("VBB_PLN_UNIQ_KEY"),
    col("SRC_SYS_CD_SK"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("HIPL_ID"),
    col("HIEL_LOG_LEVEL1"),
    col("HIEL_LOG_LEVEL2"),
    col("HIEL_LOG_LEVEL3"),
    col("MEHP_ENROLLED_BY"),
    col("MEHP_STATUS"),
    col("MEHP_TERMED_BY"),
    col("MEHP_TERM_REASON"),
    col("MBR_VBB_PLN_CMPLTN_DT_SK"),
    col("MBR_VBB_PLN_ENR_DT_SK"),
    col("MBR_VBB_PLN_TERM_DT_SK"),
    col("SRC_SYS_CRT_DTM"),
    col("SRC_SYS_UPDT_DTM"),
    col("TRZ_MBR_UNVRS_ID")
)

# Write CHashedFileStage (hf_recycle) - Scenario C => parquet
df_recycle_final = df_ForeignKey_Recycle.select(
    rpad(col("JOB_EXCTN_RCRD_ERR_SK").cast(StringType()), 0, " ").cast(IntegerType()).alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    col("SRC_SYS_CD"),
    col("PRI_KEY_STRING"),
    col("MBR_VBB_PLN_ENR_SK"),
    col("MBR_UNIQ_KEY"),
    col("VBB_PLN_UNIQ_KEY"),
    col("SRC_SYS_CD_SK"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("HIPL_ID"),
    col("HIEL_LOG_LEVEL1"),
    col("HIEL_LOG_LEVEL2"),
    col("HIEL_LOG_LEVEL3"),
    col("MEHP_ENROLLED_BY"),
    col("MEHP_STATUS"),
    col("MEHP_TERMED_BY"),
    col("MEHP_TERM_REASON"),
    rpad(col("MBR_VBB_PLN_CMPLTN_DT_SK"), 10, " ").alias("MBR_VBB_PLN_CMPLTN_DT_SK"),
    rpad(col("MBR_VBB_PLN_ENR_DT_SK"), 10, " ").alias("MBR_VBB_PLN_ENR_DT_SK"),
    rpad(col("MBR_VBB_PLN_TERM_DT_SK"), 10, " ").alias("MBR_VBB_PLN_TERM_DT_SK"),
    col("SRC_SYS_CRT_DTM"),
    col("SRC_SYS_UPDT_DTM"),
    col("TRZ_MBR_UNVRS_ID")
)

write_files(
    df_recycle_final,
    "hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# DefaultUNK - constraint: @INROWNUM=1 => produce exactly one row with specified values
df_ForeignKey_limit1 = df_ForeignKey.limit(1).select(
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK_temp"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK_temp")
)

df_ForeignKey_DefaultUNK = df_ForeignKey_limit1.select(
    lit(0).alias("MBR_VBB_PLN_ENR_SK"),
    lit(0).alias("MBR_UNIQ_KEY"),
    lit(0).alias("VBB_PLN_UNIQ_KEY"),
    lit(0).alias("SRC_SYS_CD_SK"),
    col("CRT_RUN_CYC_EXCTN_SK_temp").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK_temp").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("CLS_SK"),
    lit(0).alias("CLS_PLN_SK"),
    lit(0).alias("GRP_SK"),
    lit(0).alias("MBR_SK"),
    lit(0).alias("VBB_PLN_SK"),
    lit(0).alias("MBR_VBB_PLN_ENR_METH_CD_SK"),
    lit(0).alias("MBR_VBB_PLN_STTUS_CD_SK"),
    lit(0).alias("MBR_VBB_PLN_TERM_METH_CD_SK"),
    lit(0).alias("MBR_VBB_PLN_TERM_RSN_CD_SK"),
    rpad(lit("1753-01-01"), 10, " ").alias("MBR_VBB_PLN_CMPLTN_DT_SK"),
    rpad(lit("1753-01-01"), 10, " ").alias("MBR_VBB_PLN_ENR_DT_SK"),
    rpad(lit("1753-01-01"), 10, " ").alias("MBR_VBB_PLN_TERM_DT_SK"),
    lit("1753-01-01 00:00:00.000").alias("SRC_SYS_CRT_DTM"),
    lit("1753-01-01 00:00:00.000").alias("SRC_SYS_UPDT_DTM"),
    lit("UNK").alias("TRZ_MBR_UNVRS_ID")
)

# DefaultNA - constraint: @INROWNUM=1 => produce exactly one row with specified values
df_ForeignKey_DefaultNA = df_ForeignKey_limit1.select(
    lit(1).alias("MBR_VBB_PLN_ENR_SK"),
    lit(1).alias("MBR_UNIQ_KEY"),
    lit(1).alias("VBB_PLN_UNIQ_KEY"),
    lit(1).alias("SRC_SYS_CD_SK"),
    col("CRT_RUN_CYC_EXCTN_SK_temp").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK_temp").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(1).alias("CLS_SK"),
    lit(1).alias("CLS_PLN_SK"),
    lit(1).alias("GRP_SK"),
    lit(1).alias("MBR_SK"),
    lit(1).alias("VBB_PLN_SK"),
    lit(1).alias("MBR_VBB_PLN_ENR_METH_CD_SK"),
    lit(1).alias("MBR_VBB_PLN_STTUS_CD_SK"),
    lit(1).alias("MBR_VBB_PLN_TERM_METH_CD_SK"),
    lit(1).alias("MBR_VBB_PLN_TERM_RSN_CD_SK"),
    rpad(lit("1753-01-01"), 10, " ").alias("MBR_VBB_PLN_CMPLTN_DT_SK"),
    rpad(lit("1753-01-01"), 10, " ").alias("MBR_VBB_PLN_ENR_DT_SK"),
    rpad(lit("1753-01-01"), 10, " ").alias("MBR_VBB_PLN_TERM_DT_SK"),
    lit("1753-01-01 00:00:00.000").alias("SRC_SYS_CRT_DTM"),
    lit("1753-01-01 00:00:00.000").alias("SRC_SYS_UPDT_DTM"),
    lit("NA").alias("TRZ_MBR_UNVRS_ID")
)

# Collector stage - union the three inputs: Fkey, DefaultUNK, DefaultNA
df_Collector = (
    df_ForeignKey_Fkey.select(
        "MBR_VBB_PLN_ENR_SK",
        "MBR_UNIQ_KEY",
        "VBB_PLN_UNIQ_KEY",
        "SRC_SYS_CD_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CLS_SK",
        "CLS_PLN_SK",
        "GRP_SK",
        "MBR_SK",
        "VBB_PLN_SK",
        "MBR_VBB_PLN_ENR_METH_CD_SK",
        "MBR_VBB_PLN_STTUS_CD_SK",
        "MBR_VBB_PLN_TERM_METH_CD_SK",
        "MBR_VBB_PLN_TERM_RSN_CD_SK",
        "MBR_VBB_PLN_CMPLTN_DT_SK",
        "MBR_VBB_PLN_ENR_DT_SK",
        "MBR_VBB_PLN_TERM_DT_SK",
        "SRC_SYS_CRT_DTM",
        "SRC_SYS_UPDT_DTM",
        "TRZ_MBR_UNVRS_ID"
    )
    .unionByName(
        df_ForeignKey_DefaultUNK.select(
            "MBR_VBB_PLN_ENR_SK",
            "MBR_UNIQ_KEY",
            "VBB_PLN_UNIQ_KEY",
            "SRC_SYS_CD_SK",
            "CRT_RUN_CYC_EXCTN_SK",
            "LAST_UPDT_RUN_CYC_EXCTN_SK",
            "CLS_SK",
            "CLS_PLN_SK",
            "GRP_SK",
            "MBR_SK",
            "VBB_PLN_SK",
            "MBR_VBB_PLN_ENR_METH_CD_SK",
            "MBR_VBB_PLN_STTUS_CD_SK",
            "MBR_VBB_PLN_TERM_METH_CD_SK",
            "MBR_VBB_PLN_TERM_RSN_CD_SK",
            "MBR_VBB_PLN_CMPLTN_DT_SK",
            "MBR_VBB_PLN_ENR_DT_SK",
            "MBR_VBB_PLN_TERM_DT_SK",
            "SRC_SYS_CRT_DTM",
            "SRC_SYS_UPDT_DTM",
            "TRZ_MBR_UNVRS_ID"
        )
    )
    .unionByName(
        df_ForeignKey_DefaultNA.select(
            "MBR_VBB_PLN_ENR_SK",
            "MBR_UNIQ_KEY",
            "VBB_PLN_UNIQ_KEY",
            "SRC_SYS_CD_SK",
            "CRT_RUN_CYC_EXCTN_SK",
            "LAST_UPDT_RUN_CYC_EXCTN_SK",
            "CLS_SK",
            "CLS_PLN_SK",
            "GRP_SK",
            "MBR_SK",
            "VBB_PLN_SK",
            "MBR_VBB_PLN_ENR_METH_CD_SK",
            "MBR_VBB_PLN_STTUS_CD_SK",
            "MBR_VBB_PLN_TERM_METH_CD_SK",
            "MBR_VBB_PLN_TERM_RSN_CD_SK",
            "MBR_VBB_PLN_CMPLTN_DT_SK",
            "MBR_VBB_PLN_ENR_DT_SK",
            "MBR_VBB_PLN_TERM_DT_SK",
            "SRC_SYS_CRT_DTM",
            "SRC_SYS_UPDT_DTM",
            "TRZ_MBR_UNVRS_ID"
        )
    )
)

# Final output to MBR_VBB_PLN_ENR.dat
df_MBR_VBB_PLN_ENR = df_Collector.select(
    col("MBR_VBB_PLN_ENR_SK"),
    col("MBR_UNIQ_KEY"),
    col("VBB_PLN_UNIQ_KEY"),
    col("SRC_SYS_CD_SK"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("CLS_SK"),
    col("CLS_PLN_SK"),
    col("GRP_SK"),
    col("MBR_SK"),
    col("VBB_PLN_SK"),
    col("MBR_VBB_PLN_ENR_METH_CD_SK"),
    col("MBR_VBB_PLN_STTUS_CD_SK"),
    col("MBR_VBB_PLN_TERM_METH_CD_SK"),
    col("MBR_VBB_PLN_TERM_RSN_CD_SK"),
    rpad(col("MBR_VBB_PLN_CMPLTN_DT_SK"), 10, " ").alias("MBR_VBB_PLN_CMPLTN_DT_SK"),
    rpad(col("MBR_VBB_PLN_ENR_DT_SK"), 10, " ").alias("MBR_VBB_PLN_ENR_DT_SK"),
    rpad(col("MBR_VBB_PLN_TERM_DT_SK"), 10, " ").alias("MBR_VBB_PLN_TERM_DT_SK"),
    col("SRC_SYS_CRT_DTM"),
    col("SRC_SYS_UPDT_DTM"),
    col("TRZ_MBR_UNVRS_ID")
)

write_files(
    df_MBR_VBB_PLN_ENR,
    f"{adls_path}/load/MBR_VBB_PLN_ENR.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)