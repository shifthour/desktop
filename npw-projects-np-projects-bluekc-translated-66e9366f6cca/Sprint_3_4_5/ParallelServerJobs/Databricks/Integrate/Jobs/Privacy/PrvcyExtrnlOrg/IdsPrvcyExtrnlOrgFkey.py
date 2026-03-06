# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 01/30/08 05:00:10 Batch  14640_18019 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 05/25/07 11:56:51 Batch  14390_43020 PROMOTE bckcetl ids20 dsadm bls for on
# MAGIC ^1_1 05/25/07 11:52:00 Batch  14390_42723 INIT bckcett testIDSnew dsadm bls for on
# MAGIC ^1_1 05/17/07 12:03:56 Batch  14382_43445 PROMOTE bckcett testIDSnew u10913 Ollie move from devl to test
# MAGIC ^1_1 05/17/07 09:42:32 Batch  14382_34957 PROMOTE bckcett testIDSnew u10913 Ollie move from devl to test
# MAGIC ^1_1 05/17/07 09:38:32 Batch  14382_34718 INIT bckcett devlIDS30 u10913 Ollie Move from devl to test
# MAGIC ^1_1 04/27/07 12:54:12 Batch  14362_46467 INIT bckcett devlIDS30 u10913 O. Nielsen move from devl to 4.3 Environment
# MAGIC 
# MAGIC 
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
# MAGIC Naren Garapaty               2/25/2007         FHP /3028                       Originally Programmed                              devlIDS30             Steph Goddard              3/29/2007

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
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Parameters
InFile = get_widget_value('InFile','')
Logging = get_widget_value('Logging','X')
RunID = get_widget_value('RunID','2007010191')

# Schema for IdsPrvcyExtrnOrgExtr (CSeqFileStage)
schema_IdsPrvcyExtrnOrgExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38, 10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("PRVCY_EXTRNL_ENTY_SK", IntegerType(), False),
    StructField("PRVCY_EXTRNL_ENTY_UNIQ_KEY", IntegerType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("PRVCY_EXTL_ORG_ADDR_TYP_CD", IntegerType(), False),
    StructField("PRVCY_EXTRNL_ORG_PHN_TYP_CD", IntegerType(), False),
    StructField("ORG_NM", StringType(), False),
    StructField("SRC_SYS_LAST_UPDT_DT", StringType(), False),
    StructField("SRC_SYS_LAST_UPDT_USER", IntegerType(), False)
])

df_ids_prvcy_extrn_org_extr = (
    spark.read.format("csv")
    .option("header", "false")
    .option("quote", "\"")
    .schema(schema_IdsPrvcyExtrnOrgExtr)
    .load(f"{adls_path}/key/{InFile}")
)

df_foreignkey = (
    df_ids_prvcy_extrn_org_extr
    .withColumn(
        "SrcSysCdSk",
        GetFkeyCodes(
            trim(F.lit("IDS")),
            F.col("PRVCY_EXTRNL_ENTY_SK"),
            trim(F.lit("SOURCE SYSTEM")),
            trim(F.col("SRC_SYS_CD")),
            Logging
        )
    )
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn(
        "svPrvcyExtlOrgAddrTypCdSk",
        GetFkeyCodes(
            F.col("SRC_SYS_CD"),
            F.col("PRVCY_EXTRNL_ENTY_SK"),
            F.lit("EXTERNAL ENTITY ADDRESS TYPE "),
            F.col("PRVCY_EXTL_ORG_ADDR_TYP_CD"),
            Logging
        )
    )
    .withColumn(
        "svPrvcyExtrnlOrgPhnTypCdSk",
        GetFkeyCodes(
            F.col("SRC_SYS_CD"),
            F.col("PRVCY_EXTRNL_ENTY_SK"),
            F.lit("EXTERNAL ENTITY ADDRESS TYPE"),
            F.col("PRVCY_EXTRNL_ORG_PHN_TYP_CD"),
            Logging
        )
    )
    .withColumn(
        "svSrcSysLastUpdtDtSk",
        GetFkeyDate(
            F.lit("IDS"),
            F.col("PRVCY_EXTRNL_ENTY_SK"),
            F.col("SRC_SYS_LAST_UPDT_DT"),
            Logging
        )
    )
    .withColumn(
        "svSrcSysLastUpdtUserSk",
        GetFkeyAppUsr(
            F.col("SRC_SYS_CD"),
            F.col("PRVCY_EXTRNL_ENTY_SK"),
            F.col("SRC_SYS_LAST_UPDT_USER"),
            Logging
        )
    )
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("PRVCY_EXTRNL_ENTY_SK")))
)

df_fkey = (
    df_foreignkey
    .filter((F.col("ErrCount") == 0) | (F.col("PassThru") == 'Y'))
    .select(
        F.col("PRVCY_EXTRNL_ENTY_SK").alias("PRVCY_EXTRNL_ENTY_SK"),
        F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
        F.col("PRVCY_EXTRNL_ENTY_UNIQ_KEY").alias("PRVCY_EXTRNL_ENTY_UNIQ_KEY"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("svPrvcyExtlOrgAddrTypCdSk").alias("PRVCY_EXTL_ORG_ADDR_TYP_CD_SK"),
        F.col("svPrvcyExtrnlOrgPhnTypCdSk").alias("PRVCY_EXTRNL_ORG_PHN_TYP_CD_SK"),
        F.col("ORG_NM").alias("ORG_NM"),
        F.col("svSrcSysLastUpdtDtSk").alias("SRC_SYS_LAST_UPDT_DT_SK"),
        F.col("svSrcSysLastUpdtUserSk").alias("SRC_SYS_LAST_UPDT_USER_SK")
    )
)

df_recycle = (
    df_foreignkey
    .filter(F.col("ErrCount") > 0)
    .select(
        GetRecycleKey(F.col("PRVCY_EXTRNL_ENTY_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("DISCARD_IN").alias("DISCARD_IN"),
        F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("ErrCount").alias("ERR_CT"),
        (F.col("RECYCLE_CT") + 1).alias("RECYCLE_CT"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("PRVCY_EXTRNL_ENTY_SK").alias("PRVCY_EXTRNL_ENTY_SK"),
        F.col("PRVCY_EXTRNL_ENTY_UNIQ_KEY").alias("PRVCY_EXTRNL_ENTY_UNIQ_KEY"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("PRVCY_EXTL_ORG_ADDR_TYP_CD").alias("PRVCY_EXTL_ORG_ADDR_TYP_CD_SK"),
        F.col("PRVCY_EXTRNL_ORG_PHN_TYP_CD").alias("PRVCY_EXTRNL_ORG_PHN_TYP_CD_SK"),
        F.col("ORG_NM").alias("ORG_NM"),
        F.col("SRC_SYS_LAST_UPDT_DT").alias("SRC_SYS_LAST_UPDT_DT_SK"),
        F.col("SRC_SYS_LAST_UPDT_USER").alias("SRC_SYS_LAST_UPDT_USER_SK")
    )
)

write_files(
    df_recycle,
    f"{adls_path}/hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_default_unk = (
    df_foreignkey
    .limit(1)
    .select(
        F.lit(0).alias("PRVCY_EXTRNL_ENTY_SK"),
        F.lit(0).alias("SRC_SYS_CD_SK"),
        F.lit(0).alias("PRVCY_EXTRNL_ENTY_UNIQ_KEY"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("PRVCY_EXTL_ORG_ADDR_TYP_CD_SK"),
        F.lit(0).alias("PRVCY_EXTRNL_ORG_PHN_TYP_CD_SK"),
        F.lit("UNK").alias("ORG_NM"),
        F.lit("UNK").alias("SRC_SYS_LAST_UPDT_DT_SK"),
        F.lit(0).alias("SRC_SYS_LAST_UPDT_USER_SK")
    )
)

df_default_na = (
    df_foreignkey
    .limit(1)
    .select(
        F.lit(1).alias("PRVCY_EXTRNL_ENTY_SK"),
        F.lit(1).alias("SRC_SYS_CD_SK"),
        F.lit(1).alias("PRVCY_EXTRNL_ENTY_UNIQ_KEY"),
        F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("PRVCY_EXTL_ORG_ADDR_TYP_CD_SK"),
        F.lit(1).alias("PRVCY_EXTRNL_ORG_PHN_TYP_CD_SK"),
        F.lit("NA").alias("ORG_NM"),
        F.lit("NA").alias("SRC_SYS_LAST_UPDT_DT_SK"),
        F.lit(1).alias("SRC_SYS_LAST_UPDT_USER_SK")
    )
)

df_collector = df_fkey.unionByName(df_default_unk).unionByName(df_default_na)

df_final = df_collector.select(
    F.col("PRVCY_EXTRNL_ENTY_SK"),
    F.col("SRC_SYS_CD_SK"),
    F.col("PRVCY_EXTRNL_ENTY_UNIQ_KEY"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("PRVCY_EXTL_ORG_ADDR_TYP_CD_SK"),
    F.col("PRVCY_EXTRNL_ORG_PHN_TYP_CD_SK"),
    F.rpad(F.col("ORG_NM"), 100, " ").alias("ORG_NM"),
    F.rpad(F.col("SRC_SYS_LAST_UPDT_DT_SK"), 10, " ").alias("SRC_SYS_LAST_UPDT_DT_SK"),
    F.col("SRC_SYS_LAST_UPDT_USER_SK")
)

write_files(
    df_final,
    f"{adls_path}/load/PRVCY_EXTRNL_ORG.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)