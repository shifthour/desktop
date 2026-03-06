# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 01/28/08 09:39:22 Batch  14638_34767 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 12/27/07 09:46:56 Batch  14606_35219 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/24/07 09:35:50 Batch  14573_34558 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/07/07 14:46:06 Batch  14556_53195 PROMOTE bckcetl ids20 dsadm bls for rt
# MAGIC ^1_1 11/07/07 14:33:10 Batch  14556_52396 INIT bckcett testIDScur dsadm bls for rt
# MAGIC ^1_1 10/17/07 13:27:15 Batch  14535_48445 PROMOTE bckcett testIDScur u06640 Ralph
# MAGIC ^1_1 10/17/07 13:17:55 Batch  14535_47880 INIT bckcett devlIDS30 u06640 Ralph
# MAGIC 
# MAGIC 
# MAGIC ****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:   Foriegn keying process
# MAGIC 
# MAGIC       
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer             Date                         Change Description                                                                           Project #          Development Project          Code Reviewer          Date Reviewed  
# MAGIC ------------------           ----------------------------     -----------------------------------------------------------------------                                   ----------------         ------------------------------------       ----------------------------      ----------------
# MAGIC Naren Garapaty     09/20/2007                Initial program                                                                                   3259                             devlIDS30            Steph Goddard          09/27/2007

# MAGIC Read common record format file.
# MAGIC Writing Sequential File to /load
# MAGIC Assign all foreign keys and create default rows for UNK and NA.
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
    DecimalType,
    TimestampType
)
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


InFile = get_widget_value('InFile','IdsAltFundInvcPkey.AltFundInvcTmp.dat')
Logging = get_widget_value('Logging','N')

schema_IdsAltFundInvc = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38,10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("ALT_FUND_INVC_SK", IntegerType(), nullable=False),
    StructField("ALT_FUND_INVC_ID", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("ALT_FUND", IntegerType(), nullable=False),
    StructField("BILL_ENTY", IntegerType(), nullable=False),
    StructField("ALT_FUND_INVC_STYLE_CD", IntegerType(), nullable=False),
    StructField("ALT_FUND_INVC_PAYMT_CD", IntegerType(), nullable=False),
    StructField("BCBS_BILL_DT", StringType(), nullable=False),
    StructField("BCBS_DUE_DT", StringType(), nullable=False),
    StructField("BILL_DUE_DT", StringType(), nullable=False),
    StructField("BILL_END_DT", StringType(), nullable=False),
    StructField("CRT_DT", StringType(), nullable=False),
    StructField("FUND_FROM_DT", StringType(), nullable=False),
    StructField("FUND_THRU_DT", StringType(), nullable=False),
    StructField("BILL_AMT", DecimalType(38,10), nullable=False),
    StructField("NET_DUE_AMT", DecimalType(38,10), nullable=False),
    StructField("OUTSTND_BAL_AMT", DecimalType(38,10), nullable=False)
])

df_IdsAltFundInvc = (
    spark.read
    .schema(schema_IdsAltFundInvc)
    .option("header", "false")
    .option("quote", "\"")
    .option("sep", ",")
    .csv(f"{adls_path}/key/{InFile}")
)

df_IdsAltFundInvc = (
    df_IdsAltFundInvc
    .withColumn("SrcSysCdSk", GetFkeyCodes(F.lit("IDS"), F.col("ALT_FUND_INVC_SK"), F.lit("SOURCE SYSTEM"), F.col("SRC_SYS_CD"), Logging))
    .withColumn("svAltFundSk", GetFkeyAltFund(F.col("SRC_SYS_CD"), F.col("ALT_FUND_INVC_SK"), F.col("ALT_FUND"), Logging))
    .withColumn("svAltFundInvcStyleCdSk", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("ALT_FUND_INVC_SK"), F.lit("INVOICE STYLE"), F.col("ALT_FUND_INVC_STYLE_CD"), Logging))
    .withColumn("svAltFundInvcPaymtCdSk", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("ALT_FUND_INVC_SK"), F.lit("INVOICE PAYMENT"), F.col("ALT_FUND_INVC_PAYMT_CD"), Logging))
    .withColumn("svBillEntySk", GetFkeyBillEnty(F.col("SRC_SYS_CD"), F.col("ALT_FUND_INVC_SK"), F.col("BILL_ENTY"), Logging))
    .withColumn("svBillDueDtSk", GetFkeyDate(F.lit("IDS"), F.col("ALT_FUND_INVC_SK"), F.col("BILL_DUE_DT"), Logging))
    .withColumn("svBillEndDtSk", GetFkeyDate(F.lit("IDS"), F.col("ALT_FUND_INVC_SK"), F.col("BILL_END_DT"), Logging))
    .withColumn("svCrtDtSk", GetFkeyDate(F.lit("IDS"), F.col("ALT_FUND_INVC_SK"), F.col("CRT_DT"), Logging))
    .withColumn("svFundFromDtSk", GetFkeyDate(F.lit("IDS"), F.col("ALT_FUND_INVC_SK"), F.col("FUND_FROM_DT"), Logging))
    .withColumn("svFundThruDtSk", GetFkeyDate(F.lit("IDS"), F.col("ALT_FUND_INVC_SK"), F.col("FUND_THRU_DT"), Logging))
    .withColumn("svBCBSBillDtSk", GetFkeyDate(F.lit("IDS"), F.col("ALT_FUND_INVC_SK"), F.col("BCBS_BILL_DT"), Logging))
    .withColumn("svBCBSDueDtSk", GetFkeyDate(F.lit("IDS"), F.col("ALT_FUND_INVC_SK"), F.col("BCBS_DUE_DT"), Logging))
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("ALT_FUND_INVC_SK")))
)

df_PaymtSumkeyOut = (
    df_IdsAltFundInvc
    .filter((F.col("ErrCount") == 0) | (F.col("PassThru") == "Y"))
    .select(
        F.col("ALT_FUND_INVC_SK").alias("ALT_FUND_INVC_SK"),
        F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
        F.col("ALT_FUND_INVC_ID").alias("ALT_FUND_INVC_ID"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("svAltFundSk").alias("ALT_FUND_SK"),
        F.col("svBillEntySk").alias("BILL_ENTY_SK"),
        F.col("svAltFundInvcStyleCdSk").alias("ALT_FUND_INVC_STYLE_CD_SK"),
        F.col("svAltFundInvcPaymtCdSk").alias("ALT_FUND_INVC_PAYMT_CD_SK"),
        F.col("svBCBSBillDtSk").alias("BCBS_BILL_DT_SK"),
        F.col("svBCBSDueDtSk").alias("BCBS_DUE_DT_SK"),
        F.col("svBillDueDtSk").alias("BILL_DUE_DT_SK"),
        F.col("svBillEndDtSk").alias("BILL_END_DT_SK"),
        F.col("svCrtDtSk").alias("CRT_DT_SK"),
        F.col("svFundFromDtSk").alias("FUND_FROM_DT_SK"),
        F.col("svFundThruDtSk").alias("FUND_THRU_DT_SK"),
        F.col("BILL_AMT").alias("BILL_AMT"),
        F.col("NET_DUE_AMT").alias("NET_DUE_AMT"),
        F.col("OUTSTND_BAL_AMT").alias("OUTSTND_BAL_AMT")
    )
)

df_lnkRecycle_tmp = (
    df_IdsAltFundInvc
    .filter(F.col("ErrCount") > 0)
    .select(
        GetRecycleKey(F.col("ALT_FUND_INVC_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("DISCARD_IN").alias("DISCARD_IN"),
        F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("ErrCount").alias("ERR_CT"),
        (F.col("RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("ALT_FUND_INVC_SK").alias("ALT_FUND_INVC_SK"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD_SK"),
        F.col("ALT_FUND_INVC_ID").alias("ALT_FUND_INVC_ID"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("ALT_FUND").alias("ALT_FUND_SK"),
        F.col("BILL_ENTY").alias("BILL_ENTY_SK"),
        F.col("ALT_FUND_INVC_STYLE_CD").alias("ALT_FUND_INVC_STYLE_CD_SK"),
        F.col("ALT_FUND_INVC_PAYMT_CD").alias("ALT_FUND_INVC_PAYMT_CD_SK"),
        F.col("BCBS_BILL_DT").alias("BCBS_BILL_DT_SK"),
        F.col("BCBS_DUE_DT").alias("BCBS_DUE_DT_SK"),
        F.col("BILL_DUE_DT").alias("BILL_DUE_DT_SK"),
        F.col("BILL_END_DT").alias("BILL_END_DT_SK"),
        F.col("CRT_DT").alias("CRT_DT_SK"),
        F.col("FUND_FROM_DT").alias("FUND_FROM_DT_SK"),
        F.col("FUND_THRU_DT").alias("FUND_THRU_DT_SK"),
        F.col("BILL_AMT").alias("BILL_AMT"),
        F.col("NET_DUE_AMT").alias("NET_DUE_AMT"),
        F.col("OUTSTND_BAL_AMT").alias("OUTSTND_BAL_AMT")
    )
)

df_lnkRecycle = df_lnkRecycle_tmp \
    .withColumn("INSRT_UPDT_CD", F.rpad("INSRT_UPDT_CD", 10, " ")) \
    .withColumn("DISCARD_IN", F.rpad("DISCARD_IN", 1, " ")) \
    .withColumn("PASS_THRU_IN", F.rpad("PASS_THRU_IN", 1, " ")) \
    .withColumn("BCBS_BILL_DT_SK", F.rpad("BCBS_BILL_DT_SK", 10, " ")) \
    .withColumn("BCBS_DUE_DT_SK", F.rpad("BCBS_DUE_DT_SK", 10, " ")) \
    .withColumn("BILL_DUE_DT_SK", F.rpad("BILL_DUE_DT_SK", 10, " ")) \
    .withColumn("BILL_END_DT_SK", F.rpad("BILL_END_DT_SK", 10, " ")) \
    .withColumn("CRT_DT_SK", F.rpad("CRT_DT_SK", 10, " ")) \
    .withColumn("FUND_FROM_DT_SK", F.rpad("FUND_FROM_DT_SK", 10, " ")) \
    .withColumn("FUND_THRU_DT_SK", F.rpad("FUND_THRU_DT_SK", 10, " "))

write_files(
    df_lnkRecycle.select(
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "PASS_THRU_IN",
        "FIRST_RECYC_DT",
        "ERR_CT",
        "RECYCLE_CT",
        "SRC_SYS_CD",
        "PRI_KEY_STRING",
        "ALT_FUND_INVC_SK",
        "SRC_SYS_CD_SK",
        "ALT_FUND_INVC_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "ALT_FUND_SK",
        "BILL_ENTY_SK",
        "ALT_FUND_INVC_STYLE_CD_SK",
        "ALT_FUND_INVC_PAYMT_CD_SK",
        "BCBS_BILL_DT_SK",
        "BCBS_DUE_DT_SK",
        "BILL_DUE_DT_SK",
        "BILL_END_DT_SK",
        "CRT_DT_SK",
        "FUND_FROM_DT_SK",
        "FUND_THRU_DT_SK",
        "BILL_AMT",
        "NET_DUE_AMT",
        "OUTSTND_BAL_AMT"
    ),
    f"{adls_path}/hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_DefaultUNK = spark.createDataFrame(
    [
        (
            0, 
            0, 
            "UNK", 
            0, 
            0, 
            0, 
            0, 
            0, 
            0, 
            "UNK", 
            "UNK", 
            "UNK", 
            "UNK", 
            "UNK", 
            "UNK", 
            "UNK",
            0.0,
            0.0,
            0.0
        )
    ],
    [
        "ALT_FUND_INVC_SK",
        "SRC_SYS_CD_SK",
        "ALT_FUND_INVC_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "ALT_FUND_SK",
        "BILL_ENTY_SK",
        "ALT_FUND_INVC_STYLE_CD_SK",
        "ALT_FUND_INVC_PAYMT_CD_SK",
        "BCBS_BILL_DT_SK",
        "BCBS_DUE_DT_SK",
        "BILL_DUE_DT_SK",
        "BILL_END_DT_SK",
        "CRT_DT_SK",
        "FUND_FROM_DT_SK",
        "FUND_THRU_DT_SK",
        "BILL_AMT",
        "NET_DUE_AMT",
        "OUTSTND_BAL_AMT"
    ]
)

df_DefaultNA = spark.createDataFrame(
    [
        (
            1,
            1,
            "NA",
            1,
            1,
            1,
            1,
            1,
            1,
            "NA",
            "NA",
            "NA",
            "NA",
            "NA",
            "NA",
            "NA",
            0.0,
            0.0,
            0.0
        )
    ],
    [
        "ALT_FUND_INVC_SK",
        "SRC_SYS_CD_SK",
        "ALT_FUND_INVC_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "ALT_FUND_SK",
        "BILL_ENTY_SK",
        "ALT_FUND_INVC_STYLE_CD_SK",
        "ALT_FUND_INVC_PAYMT_CD_SK",
        "BCBS_BILL_DT_SK",
        "BCBS_DUE_DT_SK",
        "BILL_DUE_DT_SK",
        "BILL_END_DT_SK",
        "CRT_DT_SK",
        "FUND_FROM_DT_SK",
        "FUND_THRU_DT_SK",
        "BILL_AMT",
        "NET_DUE_AMT",
        "OUTSTND_BAL_AMT"
    ]
)

df_Collector = df_PaymtSumkeyOut.unionByName(df_DefaultUNK).unionByName(df_DefaultNA)

df_Collector = (
    df_Collector
    .withColumn("BCBS_BILL_DT_SK", F.rpad("BCBS_BILL_DT_SK", 10, " "))
    .withColumn("BCBS_DUE_DT_SK", F.rpad("BCBS_DUE_DT_SK", 10, " "))
    .withColumn("BILL_DUE_DT_SK", F.rpad("BILL_DUE_DT_SK", 10, " "))
    .withColumn("BILL_END_DT_SK", F.rpad("BILL_END_DT_SK", 10, " "))
    .withColumn("CRT_DT_SK", F.rpad("CRT_DT_SK", 10, " "))
    .withColumn("FUND_FROM_DT_SK", F.rpad("FUND_FROM_DT_SK", 10, " "))
    .withColumn("FUND_THRU_DT_SK", F.rpad("FUND_THRU_DT_SK", 10, " "))
)

write_files(
    df_Collector.select(
        "ALT_FUND_INVC_SK",
        "SRC_SYS_CD_SK",
        "ALT_FUND_INVC_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "ALT_FUND_SK",
        "BILL_ENTY_SK",
        "ALT_FUND_INVC_STYLE_CD_SK",
        "ALT_FUND_INVC_PAYMT_CD_SK",
        "BCBS_BILL_DT_SK",
        "BCBS_DUE_DT_SK",
        "BILL_DUE_DT_SK",
        "BILL_END_DT_SK",
        "CRT_DT_SK",
        "FUND_FROM_DT_SK",
        "FUND_THRU_DT_SK",
        "BILL_AMT",
        "NET_DUE_AMT",
        "OUTSTND_BAL_AMT"
    ),
    f"{adls_path}/load/ALT_FUND_INVC.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)