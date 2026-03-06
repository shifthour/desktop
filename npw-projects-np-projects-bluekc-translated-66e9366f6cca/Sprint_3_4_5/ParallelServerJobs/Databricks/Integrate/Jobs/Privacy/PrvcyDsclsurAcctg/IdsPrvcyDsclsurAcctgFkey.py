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
# MAGIC Parikshith Chada               2/25/2007                                              Originally Programmed                              devlIDS30                        Steph Goddard            
# MAGIC              
# MAGIC Manasa Andru                  6/26/2013           TTR - 778                  Removed RunID from parameters as         IntegrateCurDevl               Kalyan Neelam       2013-07-02
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
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


InFile = get_widget_value("InFile", "FctsPrvcyDsclsurAcctgExtr.PrvcyDsclsurAcctg.dat")
Logging = get_widget_value("Logging", "Y")

schema_IdsPrvcyDsclsurAcctgExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38,10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("PRVCY_DSCLSUR_ACCTG_SK", IntegerType(), nullable=False),
    StructField("PRVCY_MBR_UNIQ_KEY", IntegerType(), nullable=False),
    StructField("RQST_NO", IntegerType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("MBR_SK", IntegerType(), nullable=False),
    StructField("PRVCY_EXTRNL_MBR_SK", IntegerType(), nullable=False),
    StructField("PRVCY_DSCLSUR_ACCTG_RQST_CD_SK", IntegerType(), nullable=False),
    StructField("PRVCY_DSCLS_ACCT_STS_RSN_CD_SK", IntegerType(), nullable=False),
    StructField("PRVCY_MBR_SRC_CD_SK", IntegerType(), nullable=False),
    StructField("CRT_DT_SK", StringType(), nullable=False),
    StructField("FROM_DT_SK", StringType(), nullable=False),
    StructField("RCVD_DT_SK", StringType(), nullable=False),
    StructField("RQST_DT_SK", StringType(), nullable=False),
    StructField("STTUS_DT_SK", StringType(), nullable=False),
    StructField("TO_DT_SK", StringType(), nullable=False),
    StructField("RQST_DESC", StringType(), nullable=False),
    StructField("STTUS_CD_TX", StringType(), nullable=False),
    StructField("SRC_SYS_LAST_UPDT_DT_SK", StringType(), nullable=False),
    StructField("SRC_SYS_LAST_UPDT_USER_SK", IntegerType(), nullable=False),
])

df_IdsPrvcyDsclsurAcctgExtr = (
    spark.read.format("csv")
    .option("header", "false")
    .option("quote", "\"")
    .option("delimiter", ",")
    .schema(schema_IdsPrvcyDsclsurAcctgExtr)
    .load(f"{adls_path}/key/{InFile}")
)

df_ForeignKey = (
    df_IdsPrvcyDsclsurAcctgExtr
    .withColumn("SrcSysCdSk", GetFkeyCodes(
        trim(F.lit("IDS")),
        F.col("PRVCY_DSCLSUR_ACCTG_SK"),
        trim(F.lit("SOURCE SYSTEM")),
        trim(F.col("SRC_SYS_CD")),
        F.lit(Logging)
    ))
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("svPrvcyMbrSrcCdSk", GetFkeyCodes(
        F.col("SRC_SYS_CD"),
        F.col("PRVCY_DSCLSUR_ACCTG_SK"),
        F.lit("PRIVACY MEMBER SOURCE"),
        F.col("PRVCY_MBR_SRC_CD_SK"),
        F.lit(Logging)
    ))
    .withColumn("svMbrSk", GetFkeyMbr(
        F.col("SRC_SYS_CD"),
        F.col("PRVCY_DSCLSUR_ACCTG_SK"),
        F.col("MBR_SK"),
        F.lit(Logging)
    ))
    .withColumn("svPrvcyExtrnlMbrsK", GetFkeyPrvcyExtrnlMbr(
        F.col("SRC_SYS_CD"),
        F.col("PRVCY_DSCLSUR_ACCTG_SK"),
        F.col("PRVCY_EXTRNL_MBR_SK"),
        F.lit(Logging)
    ))
    .withColumn("svPrvcyDsclsurAcctgRqstCdSk", GetFkeyCodes(
        F.col("SRC_SYS_CD"),
        F.col("PRVCY_DSCLSUR_ACCTG_SK"),
        F.lit("PRIVACY DISCLOSURE ACCOUNTING REQUEST"),
        F.col("PRVCY_DSCLSUR_ACCTG_RQST_CD_SK"),
        F.lit(Logging)
    ))
    .withColumn("svPrvcyDsclsAcctStsRsnCdSk", GetFkeyCodes(
        F.col("SRC_SYS_CD"),
        F.col("PRVCY_DSCLSUR_ACCTG_SK"),
        F.lit("PRIVACY DISCLOSURE ACCOUNTING STATUS REASON"),
        F.col("PRVCY_DSCLS_ACCT_STS_RSN_CD_SK"),
        F.lit(Logging)
    ))
    .withColumn("svCrtDtSk", GetFkeyDate(
        F.lit("IDS"),
        F.col("PRVCY_DSCLSUR_ACCTG_SK"),
        F.col("CRT_DT_SK"),
        F.lit(Logging)
    ))
    .withColumn("svFromDtSk", GetFkeyDate(
        F.lit("IDS"),
        F.col("PRVCY_DSCLSUR_ACCTG_SK"),
        F.col("FROM_DT_SK"),
        F.lit(Logging)
    ))
    .withColumn("svRcvdDtSk", GetFkeyDate(
        F.lit("IDS"),
        F.col("PRVCY_DSCLSUR_ACCTG_SK"),
        F.col("RCVD_DT_SK"),
        F.lit(Logging)
    ))
    .withColumn("svRqstDtSk", GetFkeyDate(
        F.lit("IDS"),
        F.col("PRVCY_DSCLSUR_ACCTG_SK"),
        F.col("RQST_DT_SK"),
        F.lit(Logging)
    ))
    .withColumn("svSttusDtSk", GetFkeyDate(
        F.lit("IDS"),
        F.col("PRVCY_DSCLSUR_ACCTG_SK"),
        F.col("STTUS_DT_SK"),
        F.lit(Logging)
    ))
    .withColumn("svToDtSk", GetFkeyDate(
        F.lit("IDS"),
        F.col("PRVCY_DSCLSUR_ACCTG_SK"),
        F.col("TO_DT_SK"),
        F.lit(Logging)
    ))
    .withColumn("svSrcSysLastUpdtDtSk", GetFkeyDate(
        F.lit("IDS"),
        F.col("PRVCY_DSCLSUR_ACCTG_SK"),
        F.col("SRC_SYS_LAST_UPDT_DT_SK"),
        F.lit(Logging)
    ))
    .withColumn("svSrcSysLastUpdtUserSk", GetFkeyAppUsr(
        F.col("SRC_SYS_CD"),
        F.col("PRVCY_DSCLSUR_ACCTG_SK"),
        F.col("SRC_SYS_LAST_UPDT_USER_SK"),
        F.lit(Logging)
    ))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("PRVCY_DSCLSUR_ACCTG_SK")))
)

df_foreignkey_fkey = df_ForeignKey.filter(
    (F.col("ErrCount") == 0) | (F.col("PassThru") == "Y")
).select(
    F.col("PRVCY_DSCLSUR_ACCTG_SK").alias("PRVCY_DSCLSUR_ACCTG_SK"),
    F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
    F.col("PRVCY_MBR_UNIQ_KEY").alias("PRVCY_MBR_UNIQ_KEY"),
    F.col("RQST_NO").alias("RQST_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("svMbrSk").alias("MBR_SK"),
    F.col("svPrvcyExtrnlMbrsK").alias("PRVCY_EXTRNL_MBR_SK"),
    F.col("svPrvcyDsclsurAcctgRqstCdSk").alias("PRVCY_DSCLSUR_ACCTG_RQST_CD_SK"),
    F.col("svPrvcyDsclsAcctStsRsnCdSk").alias("PRVCY_DSCLS_ACCT_STS_RSN_CD_SK"),
    F.col("svPrvcyMbrSrcCdSk").alias("PRVCY_MBR_SRC_CD_SK"),
    F.col("svCrtDtSk").alias("CRT_DT_SK"),
    F.col("svFromDtSk").alias("FROM_DT_SK"),
    F.col("svRcvdDtSk").alias("RCVD_DT_SK"),
    F.col("svRqstDtSk").alias("RQST_DT_SK"),
    F.col("svSttusDtSk").alias("STTUS_DT_SK"),
    F.col("svToDtSk").alias("TO_DT_SK"),
    F.col("RQST_DESC").alias("RQST_DESC"),
    F.col("STTUS_CD_TX").alias("STTUS_CD_TX"),
    F.col("svSrcSysLastUpdtDtSk").alias("SRC_SYS_LAST_UPDT_DT_SK"),
    F.col("svSrcSysLastUpdtUserSk").alias("SRC_SYS_LAST_UPDT_USER_SK")
)

df_foreignkey_recycle = df_ForeignKey.filter(
    F.col("ErrCount") > 0
).select(
    F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    GetRecycleKey(F.col("JOB_EXCTN_RCRD_ERR_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),  # Overwriting as per expression
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ErrCount").alias("ERR_CT"),
    (F.col("RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("PRVCY_DSCLSUR_ACCTG_SK").alias("PRVCY_DSCLSUR_ACCTG_SK"),
    F.col("PRVCY_MBR_UNIQ_KEY").alias("PRVCY_MBR_UNIQ_KEY"),
    F.col("RQST_NO").alias("RQST_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("MBR_SK").alias("MBR_SK"),
    F.col("PRVCY_EXTRNL_MBR_SK").alias("PRVCY_EXTRNL_MBR_SK"),
    F.col("PRVCY_DSCLSUR_ACCTG_RQST_CD_SK").alias("PRVCY_DSCLSUR_ACCTG_RQST_CD_SK"),
    F.col("PRVCY_DSCLS_ACCT_STS_RSN_CD_SK").alias("PRVCY_DSCLS_ACCT_STS_RSN_CD_SK"),
    F.col("PRVCY_MBR_SRC_CD_SK").alias("PRVCY_MBR_SRC_CD_SK"),
    F.col("CRT_DT_SK").alias("CRT_DT_SK"),
    F.col("FROM_DT_SK").alias("FROM_DT_SK"),
    F.col("RCVD_DT_SK").alias("RCVD_DT_SK"),
    F.col("RQST_DT_SK").alias("RQST_DT_SK"),
    F.col("STTUS_DT_SK").alias("STTUS_DT_SK"),
    F.col("TO_DT_SK").alias("TO_DT_SK"),
    F.col("RQST_DESC").alias("RQST_DESC"),
    F.col("STTUS_CD_TX").alias("STTUS_CD_TX"),
    F.col("SRC_SYS_LAST_UPDT_DT_SK").alias("SRC_SYS_LAST_UPDT_DT_SK"),
    F.col("SRC_SYS_LAST_UPDT_USER_SK").alias("SRC_SYS_LAST_UPDT_USER_SK")
)

df_defaultUNK = spark.createDataFrame(
    [
        (
            0,  # PRVCY_DSCLSUR_ACCTG_SK
            0,  # SRC_SYS_CD_SK
            0,  # PRVCY_MBR_UNIQ_KEY
            0,  # RQST_NO
            0,  # CRT_RUN_CYC_EXCTN_SK
            0,  # LAST_UPDT_RUN_CYC_EXCTN_SK
            0,  # MBR_SK
            0,  # PRVCY_EXTRNL_MBR_SK
            0,  # PRVCY_DSCLSUR_ACCTG_RQST_CD_SK
            0,  # PRVCY_DSCLS_ACCT_STS_RSN_CD_SK
            0,  # PRVCY_MBR_SRC_CD_SK
            "UNK",  # CRT_DT_SK
            "UNK",  # FROM_DT_SK
            "UNK",  # RCVD_DT_SK
            "UNK",  # RQST_DT_SK
            "UNK",  # STTUS_DT_SK
            "UNK",  # TO_DT_SK
            "UNK",  # RQST_DESC
            "UNK",  # STTUS_CD_TX
            "UNK",  # SRC_SYS_LAST_UPDT_DT_SK
            0   # SRC_SYS_LAST_UPDT_USER_SK
        )
    ],
    [
        "PRVCY_DSCLSUR_ACCTG_SK",
        "SRC_SYS_CD_SK",
        "PRVCY_MBR_UNIQ_KEY",
        "RQST_NO",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "MBR_SK",
        "PRVCY_EXTRNL_MBR_SK",
        "PRVCY_DSCLSUR_ACCTG_RQST_CD_SK",
        "PRVCY_DSCLS_ACCT_STS_RSN_CD_SK",
        "PRVCY_MBR_SRC_CD_SK",
        "CRT_DT_SK",
        "FROM_DT_SK",
        "RCVD_DT_SK",
        "RQST_DT_SK",
        "STTUS_DT_SK",
        "TO_DT_SK",
        "RQST_DESC",
        "STTUS_CD_TX",
        "SRC_SYS_LAST_UPDT_DT_SK",
        "SRC_SYS_LAST_UPDT_USER_SK"
    ]
)

df_defaultNA = spark.createDataFrame(
    [
        (
            1,  # PRVCY_DSCLSUR_ACCTG_SK
            1,  # SRC_SYS_CD_SK
            1,  # PRVCY_MBR_UNIQ_KEY
            1,  # RQST_NO
            1,  # CRT_RUN_CYC_EXCTN_SK
            1,  # LAST_UPDT_RUN_CYC_EXCTN_SK
            1,  # MBR_SK
            1,  # PRVCY_EXTRNL_MBR_SK
            1,  # PRVCY_DSCLSUR_ACCTG_RQST_CD_SK
            1,  # PRVCY_DSCLS_ACCT_STS_RSN_CD_SK
            1,  # PRVCY_MBR_SRC_CD_SK
            "NA",  # CRT_DT_SK
            "NA",  # FROM_DT_SK
            "NA",  # RCVD_DT_SK
            "NA",  # RQST_DT_SK
            "NA",  # STTUS_DT_SK
            "NA",  # TO_DT_SK
            "NA",  # RQST_DESC
            "NA",  # STTUS_CD_TX
            "NA",  # SRC_SYS_LAST_UPDT_DT_SK
            1   # SRC_SYS_LAST_UPDT_USER_SK
        )
    ],
    [
        "PRVCY_DSCLSUR_ACCTG_SK",
        "SRC_SYS_CD_SK",
        "PRVCY_MBR_UNIQ_KEY",
        "RQST_NO",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "MBR_SK",
        "PRVCY_EXTRNL_MBR_SK",
        "PRVCY_DSCLSUR_ACCTG_RQST_CD_SK",
        "PRVCY_DSCLS_ACCT_STS_RSN_CD_SK",
        "PRVCY_MBR_SRC_CD_SK",
        "CRT_DT_SK",
        "FROM_DT_SK",
        "RCVD_DT_SK",
        "RQST_DT_SK",
        "STTUS_DT_SK",
        "TO_DT_SK",
        "RQST_DESC",
        "STTUS_CD_TX",
        "SRC_SYS_LAST_UPDT_DT_SK",
        "SRC_SYS_LAST_UPDT_USER_SK"
    ]
)

df_collector_pre = (
    df_foreignkey_fkey
    .unionByName(df_defaultUNK)
    .unionByName(df_defaultNA)
)

df_collector = (
    df_collector_pre
    .withColumn("CRT_DT_SK", F.rpad(F.col("CRT_DT_SK"), 10, " "))
    .withColumn("FROM_DT_SK", F.rpad(F.col("FROM_DT_SK"), 10, " "))
    .withColumn("RCVD_DT_SK", F.rpad(F.col("RCVD_DT_SK"), 10, " "))
    .withColumn("RQST_DT_SK", F.rpad(F.col("RQST_DT_SK"), 10, " "))
    .withColumn("STTUS_DT_SK", F.rpad(F.col("STTUS_DT_SK"), 10, " "))
    .withColumn("TO_DT_SK", F.rpad(F.col("TO_DT_SK"), 10, " "))
    .withColumn("RQST_DESC", F.rpad(F.col("RQST_DESC"), <...>, " "))
    .withColumn("STTUS_CD_TX", F.rpad(F.col("STTUS_CD_TX"), <...>, " "))
    .withColumn("SRC_SYS_LAST_UPDT_DT_SK", F.rpad(F.col("SRC_SYS_LAST_UPDT_DT_SK"), 10, " "))
)

df_collector_final = df_collector.select(
    "PRVCY_DSCLSUR_ACCTG_SK",
    "SRC_SYS_CD_SK",
    "PRVCY_MBR_UNIQ_KEY",
    "RQST_NO",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "MBR_SK",
    "PRVCY_EXTRNL_MBR_SK",
    "PRVCY_DSCLSUR_ACCTG_RQST_CD_SK",
    "PRVCY_DSCLS_ACCT_STS_RSN_CD_SK",
    "PRVCY_MBR_SRC_CD_SK",
    "CRT_DT_SK",
    "FROM_DT_SK",
    "RCVD_DT_SK",
    "RQST_DT_SK",
    "STTUS_DT_SK",
    "TO_DT_SK",
    "RQST_DESC",
    "STTUS_CD_TX",
    "SRC_SYS_LAST_UPDT_DT_SK",
    "SRC_SYS_LAST_UPDT_USER_SK"
)

write_files(
    df_collector_final,
    f"{adls_path}/load/PRVCY_DSCLSUR_ACCTG.dat",
    delimiter=",",
    mode="overwrite",
    is_parqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_foreignkey_recycle_rpad = (
    df_foreignkey_recycle
    .withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " "))
    .withColumn("CRT_DT_SK", F.rpad(F.col("CRT_DT_SK"), 10, " "))
    .withColumn("FROM_DT_SK", F.rpad(F.col("FROM_DT_SK"), 10, " "))
    .withColumn("RCVD_DT_SK", F.rpad(F.col("RCVD_DT_SK"), 10, " "))
    .withColumn("RQST_DT_SK", F.rpad(F.col("RQST_DT_SK"), 10, " "))
    .withColumn("STTUS_DT_SK", F.rpad(F.col("STTUS_DT_SK"), 10, " "))
    .withColumn("TO_DT_SK", F.rpad(F.col("TO_DT_SK"), 10, " "))
    .withColumn("SRC_SYS_CD", F.rpad(F.col("SRC_SYS_CD"), <...>, " "))
    .withColumn("RQST_DESC", F.rpad(F.col("RQST_DESC"), <...>, " "))
    .withColumn("STTUS_CD_TX", F.rpad(F.col("STTUS_CD_TX"), <...>, " "))
    .withColumn("SRC_SYS_LAST_UPDT_DT_SK", F.rpad(F.col("SRC_SYS_LAST_UPDT_DT_SK"), 10, " "))
)

df_foreignkey_recycle_final = df_foreignkey_recycle_rpad.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "PRVCY_DSCLSUR_ACCTG_SK",
    "PRVCY_MBR_UNIQ_KEY",
    "RQST_NO",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "MBR_SK",
    "PRVCY_EXTRNL_MBR_SK",
    "PRVCY_DSCLSUR_ACCTG_RQST_CD_SK",
    "PRVCY_DSCLS_ACCT_STS_RSN_CD_SK",
    "PRVCY_MBR_SRC_CD_SK",
    "CRT_DT_SK",
    "FROM_DT_SK",
    "RCVD_DT_SK",
    "RQST_DT_SK",
    "STTUS_DT_SK",
    "TO_DT_SK",
    "RQST_DESC",
    "STTUS_CD_TX",
    "SRC_SYS_LAST_UPDT_DT_SK",
    "SRC_SYS_LAST_UPDT_USER_SK"
)

write_files(
    df_foreignkey_recycle_final,
    f"{adls_path}/hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_parqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)