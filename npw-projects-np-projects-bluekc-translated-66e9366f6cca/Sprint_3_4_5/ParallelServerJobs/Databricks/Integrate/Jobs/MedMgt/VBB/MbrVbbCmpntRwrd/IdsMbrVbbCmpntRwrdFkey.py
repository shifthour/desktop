# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2013 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:   Assigns Foreign Keys for the IDS table MBR_VBB_CMPNT_RWRD.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 	Project/Altiris #	Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------	------------------------	-----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Raja Gummadi         2013-05-25           4963 VBB Phase III     Initial Programming                                                                      IntegrateNewDevl          Bhoomi Dasari           7/3/2013

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
    IntegerType,
    StringType,
    TimestampType,
    DecimalType
)
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


InFile = get_widget_value("InFile", "IdsMbrVbbPlnEnrExtr.MbrVbbPlnEnr.dat.100")
Logging = get_widget_value("Logging", "X")
RunID = get_widget_value("RunID", "100")

schema = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38, 10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("MBR_VBB_CMPNT_RWRD_SK", IntegerType(), False),
    StructField("MBR_UNIQ_KEY", IntegerType(), False),
    StructField("VBB_CMPNT_UNIQ_KEY", IntegerType(), False),
    StructField("VBB_CMPNT_RWRD_SEQ_NO", IntegerType(), False),
    StructField("SRC_SYS_CD_SK", IntegerType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("MERP_LIMIT_REASON", StringType(), True),
    StructField("ERN_RWRD_END_DT_SK", StringType(), False),
    StructField("ERN_RWRD_STRT_DT_SK", StringType(), False),
    StructField("SRC_SYS_CRT_DTM", TimestampType(), False),
    StructField("SRC_SYS_UPDT_DTM", TimestampType(), False),
    StructField("CMPLD_ACHV_LVL_NO", IntegerType(), False),
    StructField("ERN_RWRD_AMT", DecimalType(38, 10), False),
    StructField("PD_RWRD_AMT", DecimalType(38, 10), False),
    StructField("TRZ_MBR_UNVRS_ID", StringType(), False)
])

df_ids = (
    spark.read
    .option("delimiter", ",")
    .option("quote", "\"")
    .option("header", "false")
    .schema(schema)
    .csv(f"{adls_path}/key/{InFile}")
)

df_ids = (
    df_ids
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn(
        "svMbrVbbCmpntEnrSk",
        GetFkeyMbrVbbCmpntEnr(
            F.col("SRC_SYS_CD"),
            F.col("MBR_VBB_CMPNT_RWRD_SK"),
            F.col("MBR_UNIQ_KEY"),
            F.col("VBB_CMPNT_UNIQ_KEY"),
            Logging
        )
    )
    .withColumn(
        "svVbbCmpntRwrdSk",
        GetFkeyVbbCmpntRwrd(
            F.col("SRC_SYS_CD"),
            F.col("MBR_VBB_CMPNT_RWRD_SK"),
            F.col("VBB_CMPNT_UNIQ_KEY"),
            F.col("VBB_CMPNT_RWRD_SEQ_NO"),
            Logging
        )
    )
    .withColumn(
        "svRwrdLmtedRsnCdSk",
        GetFkeyClctnDomainCodes(
            F.col("SRC_SYS_CD"),
            F.col("MBR_VBB_CMPNT_RWRD_SK"),
            F.lit("VBB REWARD LIMIT REASON"),
            F.lit("IHMF CONSTITUENT"),
            F.lit("VBB REWARD LIMIT REASON"),
            F.lit("IDS"),
            F.col("MERP_LIMIT_REASON"),
            Logging
        )
    )
    .withColumn(
        "ErrCount",
        GetFkeyErrorCnt(F.col("MBR_VBB_CMPNT_RWRD_SK"))
    )
)

df_recycle = (
    df_ids
    .filter("ErrCount > 0")
    .select(
        GetRecycleKey(F.col("MBR_VBB_CMPNT_RWRD_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
        F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
        F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("ErrCount").alias("ERR_CT"),
        (F.col("RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("MBR_VBB_CMPNT_RWRD_SK").alias("MBR_VBB_CMPNT_RWRD_SK"),
        F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.col("VBB_CMPNT_UNIQ_KEY").alias("VBB_CMPNT_UNIQ_KEY"),
        F.col("VBB_CMPNT_RWRD_SEQ_NO").alias("VBB_CMPNT_RWRD_SEQ_NO"),
        F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("MERP_LIMIT_REASON").alias("MERP_LIMIT_REASON"),
        F.rpad(F.col("ERN_RWRD_END_DT_SK"), 10, " ").alias("ERN_RWRD_END_DT_SK"),
        F.rpad(F.col("ERN_RWRD_STRT_DT_SK"), 10, " ").alias("ERN_RWRD_STRT_DT_SK"),
        F.col("SRC_SYS_CRT_DTM").alias("SRC_SYS_CRT_DTM"),
        F.col("SRC_SYS_UPDT_DTM").alias("SRC_SYS_UPDT_DTM"),
        F.col("CMPLD_ACHV_LVL_NO").alias("CMPLD_ACHV_LVL_NO"),
        F.col("ERN_RWRD_AMT").alias("ERN_RWRD_AMT"),
        F.col("PD_RWRD_AMT").alias("PD_RWRD_AMT"),
        F.col("TRZ_MBR_UNVRS_ID").alias("TRZ_MBR_UNVRS_ID")
    )
)

write_files(
    df_recycle,
    f"{adls_path}/hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_fkey = (
    df_ids
    .filter("ErrCount = 0 or PassThru = 'Y'")
    .select(
        F.col("MBR_VBB_CMPNT_RWRD_SK").alias("MBR_VBB_CMPNT_RWRD_SK"),
        F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.col("VBB_CMPNT_UNIQ_KEY").alias("VBB_CMPNT_UNIQ_KEY"),
        F.col("VBB_CMPNT_RWRD_SEQ_NO").alias("VBB_CMPNT_RWRD_SEQ_NO"),
        F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("svMbrVbbCmpntEnrSk").alias("MBR_VBB_CMPNT_ENR_SK"),
        F.col("svVbbCmpntRwrdSk").alias("VBB_CMPNT_RWRD_SK"),
        F.col("svRwrdLmtedRsnCdSk").alias("RWRD_LMTED_RSN_CD_SK"),
        F.col("ERN_RWRD_END_DT_SK").alias("ERN_RWRD_END_DT_SK"),
        F.col("ERN_RWRD_STRT_DT_SK").alias("ERN_RWRD_STRT_DT_SK"),
        F.col("SRC_SYS_CRT_DTM").alias("SRC_SYS_CRT_DTM"),
        F.col("SRC_SYS_UPDT_DTM").alias("SRC_SYS_UPDT_DTM"),
        F.col("CMPLD_ACHV_LVL_NO").alias("CMPLD_ACHV_LVL_NO"),
        F.col("ERN_RWRD_AMT").alias("ERN_RWRD_AMT"),
        F.col("PD_RWRD_AMT").alias("PD_RWRD_AMT"),
        F.col("TRZ_MBR_UNVRS_ID").alias("TRZ_MBR_UNVRS_ID")
    )
)

df_first = df_ids.limit(1)

df_defaultUNK = df_first.select(
    F.lit(0).alias("MBR_VBB_CMPNT_RWRD_SK"),
    F.lit(0).alias("MBR_UNIQ_KEY"),
    F.lit(0).alias("VBB_CMPNT_UNIQ_KEY"),
    F.lit(0).alias("VBB_CMPNT_RWRD_SEQ_NO"),
    F.lit(0).alias("SRC_SYS_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("MBR_VBB_CMPNT_ENR_SK"),
    F.lit(0).alias("VBB_CMPNT_RWRD_SK"),
    F.lit(0).alias("RWRD_LMTED_RSN_CD_SK"),
    F.rpad(F.lit("1753-01-01"), 10, " ").alias("ERN_RWRD_END_DT_SK"),
    F.rpad(F.lit("1753-01-01"), 10, " ").alias("ERN_RWRD_STRT_DT_SK"),
    F.lit("1753-01-01 00:00:00.000").alias("SRC_SYS_CRT_DTM"),
    F.lit("1753-01-01 00:00:00.000").alias("SRC_SYS_UPDT_DTM"),
    F.lit(0).alias("CMPLD_ACHV_LVL_NO"),
    F.lit(0).alias("ERN_RWRD_AMT"),
    F.lit(0).alias("PD_RWRD_AMT"),
    F.lit("UNK").alias("TRZ_MBR_UNVRS_ID")
)

df_defaultNA = df_first.select(
    F.lit(1).alias("MBR_VBB_CMPNT_RWRD_SK"),
    F.lit(1).alias("MBR_UNIQ_KEY"),
    F.lit(1).alias("VBB_CMPNT_UNIQ_KEY"),
    F.lit(1).alias("VBB_CMPNT_RWRD_SEQ_NO"),
    F.lit(1).alias("SRC_SYS_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("MBR_VBB_CMPNT_ENR_SK"),
    F.lit(1).alias("VBB_CMPNT_RWRD_SK"),
    F.lit(1).alias("RWRD_LMTED_RSN_CD_SK"),
    F.rpad(F.lit("1753-01-01"), 10, " ").alias("ERN_RWRD_END_DT_SK"),
    F.rpad(F.lit("1753-01-01"), 10, " ").alias("ERN_RWRD_STRT_DT_SK"),
    F.lit("1753-01-01 00:00:00.000").alias("SRC_SYS_CRT_DTM"),
    F.lit("1753-01-01 00:00:00.000").alias("SRC_SYS_UPDT_DTM"),
    F.lit(1).alias("CMPLD_ACHV_LVL_NO"),
    F.lit(1).alias("ERN_RWRD_AMT"),
    F.lit(1).alias("PD_RWRD_AMT"),
    F.lit("NA").alias("TRZ_MBR_UNVRS_ID")
)

df_collector = df_fkey.unionByName(df_defaultUNK).unionByName(df_defaultNA)

df_collector = (
    df_collector
    .withColumn("ERN_RWRD_END_DT_SK", F.rpad(F.col("ERN_RWRD_END_DT_SK"), 10, " "))
    .withColumn("ERN_RWRD_STRT_DT_SK", F.rpad(F.col("ERN_RWRD_STRT_DT_SK"), 10, " "))
    .select(
        "MBR_VBB_CMPNT_RWRD_SK",
        "MBR_UNIQ_KEY",
        "VBB_CMPNT_UNIQ_KEY",
        "VBB_CMPNT_RWRD_SEQ_NO",
        "SRC_SYS_CD_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "MBR_VBB_CMPNT_ENR_SK",
        "VBB_CMPNT_RWRD_SK",
        "RWRD_LMTED_RSN_CD_SK",
        "ERN_RWRD_END_DT_SK",
        "ERN_RWRD_STRT_DT_SK",
        "SRC_SYS_CRT_DTM",
        "SRC_SYS_UPDT_DTM",
        "CMPLD_ACHV_LVL_NO",
        "ERN_RWRD_AMT",
        "PD_RWRD_AMT",
        "TRZ_MBR_UNVRS_ID"
    )
)

write_files(
    df_collector,
    f"{adls_path}/load/MBR_VBB_CMPNT_RWRD.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)