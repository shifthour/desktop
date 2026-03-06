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
# MAGIC Naren Garapaty               2/25/2007              FHP                                Originally Programmed                              devlIDS30                        Steph Goddard     3/29/2007

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
    StringType,
    IntegerType,
    DecimalType,
    TimestampType
)
from pyspark.sql.functions import (
    col,
    lit,
    row_number,
    rpad
)
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Retrieve job parameters
InFile = get_widget_value("InFile", "")
Logging = get_widget_value("Logging", "X")
RunID = get_widget_value("RunID", "2007010104")

# -------------------------------------------------------------------------
# Stage: IdsPrvcyExtrnlPrsnExtr (CSeqFileStage) - Read the file
# -------------------------------------------------------------------------
schema_IdsPrvcyExtrnlPrsnExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(10, 0), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("PRVCY_EXTRNL_ENTY_SK", IntegerType(), nullable=False),
    StructField("PRVCY_EXTRNL_ENTY_UNIQ_KEY", IntegerType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("PRVCY_EXTL_PRSN_ADDR_TYP_CD", IntegerType(), nullable=False),
    StructField("PRVCY_EXTRNL_PRSN_GNDR_CD", IntegerType(), nullable=False),
    StructField("PRVCY_EXTL_PRSN_MRTL_STS_CD", IntegerType(), nullable=False),
    StructField("PRVCY_EXTL_PRSN_PHN_TYP_CD", IntegerType(), nullable=False),
    StructField("BRTH_DT", StringType(), nullable=False),
    StructField("FIRST_NM", StringType(), nullable=True),
    StructField("MIDINIT", StringType(), nullable=True),
    StructField("LAST_NM", StringType(), nullable=True),
    StructField("SRC_SYS_LAST_UPDT_DT", StringType(), nullable=False),
    StructField("SRC_SYS_LAST_UPDT_USER", IntegerType(), nullable=False)
])

df_IdsPrvcyExtrnlPrsnExtr = (
    spark.read
    .option("header", "false")
    .option("quote", "\"")
    .option("delimiter", ",")
    .schema(schema_IdsPrvcyExtrnlPrsnExtr)
    .csv(f"{adls_path}/key/IdsPrvcyExtrnlPrsnExtr.PrvcyExtrnlPrsn.uniq")
)

# -------------------------------------------------------------------------
# Stage: ForeignKey (CTransformerStage)
# -------------------------------------------------------------------------
windowSpec = Window.orderBy(lit(1))

df_tfm = (
    df_IdsPrvcyExtrnlPrsnExtr
    .withColumn("rownum", row_number().over(windowSpec))
    .withColumn("SrcSysCdSk",
                GetFkeyCodes(
                    trim(lit("IDS")),
                    col("PRVCY_EXTRNL_ENTY_SK"),
                    trim(lit("SOURCE SYSTEM")),
                    trim(col("SRC_SYS_CD")),
                    lit(Logging)
                )
    )
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn("svExtrnlPrsnAddrTypCdSk",
                GetFkeyCodes(
                    col("SRC_SYS_CD"),
                    col("PRVCY_EXTRNL_ENTY_SK"),
                    lit("EXTERNAL ENTITY ADDRESS TYPE"),
                    col("PRVCY_EXTL_PRSN_ADDR_TYP_CD"),
                    lit(Logging)
                )
    )
    .withColumn("svExtrnlPrsnGndrCdSk",
                GetFkeyCodes(
                    col("SRC_SYS_CD"),
                    col("PRVCY_EXTRNL_ENTY_SK"),
                    lit("GENDER"),
                    col("PRVCY_EXTRNL_PRSN_GNDR_CD"),
                    lit(Logging)
                )
    )
    .withColumn("svExtrnlPrsnPhnTypCdSk",
                GetFkeyCodes(
                    col("SRC_SYS_CD"),
                    col("PRVCY_EXTRNL_ENTY_SK"),
                    lit("EXTERNAL ENTITY ADDRESS TYPE"),
                    col("PRVCY_EXTL_PRSN_PHN_TYP_CD"),
                    lit(Logging)
                )
    )
    .withColumn("svExtrnlPrsnMrtlSttusCdSk",
                GetFkeyCodes(
                    col("SRC_SYS_CD"),
                    col("PRVCY_EXTRNL_ENTY_SK"),
                    lit("MARITAL STATUS"),
                    col("PRVCY_EXTL_PRSN_MRTL_STS_CD"),
                    lit(Logging)
                )
    )
    .withColumn("svBrthDtSk",
                GetFkeyDate(
                    lit("IDS"),
                    col("PRVCY_EXTRNL_ENTY_SK"),
                    col("BRTH_DT"),
                    lit(Logging)
                )
    )
    .withColumn("svSrcSysLastUpdtDtSk",
                GetFkeyDate(
                    lit("IDS"),
                    col("PRVCY_EXTRNL_ENTY_SK"),
                    col("SRC_SYS_LAST_UPDT_DT"),
                    lit(Logging)
                )
    )
    .withColumn("svSrcSysLastUpdtUserSk",
                GetFkeyAppUsr(
                    col("SRC_SYS_CD"),
                    col("PRVCY_EXTRNL_ENTY_SK"),
                    col("SRC_SYS_LAST_UPDT_USER"),
                    lit(Logging)
                )
    )
    .withColumn("ErrCount",
                GetFkeyErrorCnt(col("PRVCY_EXTRNL_ENTY_SK"))
    )
)

# -------------------------------------------------------------------------
# ForeignKey Output Link 1 (Fkey): Constraint => ErrCount = 0 Or PassThru = 'Y'
# -------------------------------------------------------------------------
df_fkey = (
    df_tfm
    .filter((col("ErrCount") == lit(0)) | (col("PassThru") == lit("Y")))
    .select(
        col("PRVCY_EXTRNL_ENTY_SK").alias("PRVCY_EXTRNL_ENTY_SK"),
        col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
        col("PRVCY_EXTRNL_ENTY_UNIQ_KEY").alias("PRVCY_EXTRNL_ENTY_UNIQ_KEY"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("svExtrnlPrsnAddrTypCdSk").alias("PRVCY_EXTL_PRSN_ADDR_TYP_CD_SK"),
        col("svExtrnlPrsnGndrCdSk").alias("PRVCY_EXTRNL_PRSN_GNDR_CD_SK"),
        col("svExtrnlPrsnMrtlSttusCdSk").alias("PRVCY_EXTL_PRSN_MRTL_STS_CD_SK"),
        col("svExtrnlPrsnPhnTypCdSk").alias("PRVCY_EXTL_PRSN_PHN_TYP_CD_SK"),
        col("svBrthDtSk").alias("BRTH_DT_SK"),
        col("FIRST_NM").alias("FIRST_NM"),
        col("MIDINIT").alias("MIDINIT"),
        col("LAST_NM").alias("LAST_NM"),
        col("svSrcSysLastUpdtDtSk").alias("SRC_SYS_LAST_UPDT_DT_SK"),
        col("svSrcSysLastUpdtUserSk").alias("SRC_SYS_LAST_UPDT_USER_SK")
    )
)

# -------------------------------------------------------------------------
# ForeignKey Output Link 2 (Recycle): Constraint => ErrCount > 0
# -------------------------------------------------------------------------
df_recycle = (
    df_tfm
    .filter(col("ErrCount") > lit(0))
    .select(
        GetRecycleKey(col("PRVCY_EXTRNL_ENTY_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        col("DISCARD_IN").alias("DISCARD_IN"),
        col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        col("ErrCount").alias("ERR_CT"),
        (col("RECYCLE_CT") + lit(1)).alias("RECYCLE_CT"),
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        col("PRVCY_EXTRNL_ENTY_SK").alias("PRVCY_EXTRNL_ENTY_SK"),
        col("PRVCY_EXTRNL_ENTY_UNIQ_KEY").alias("PRVCY_EXTRNL_ENTY_UNIQ_KEY"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("PRVCY_EXTL_PRSN_ADDR_TYP_CD").alias("PRVCY_EXTL_PRSN_ADDR_TYP_CD_SK"),
        col("PRVCY_EXTRNL_PRSN_GNDR_CD").alias("PRVCY_EXTRNL_PRSN_GNDR_CD_SK"),
        col("PRVCY_EXTL_PRSN_MRTL_STS_CD").alias("PRVCY_EXTL_PRSN_MRTL_STS_CD_SK"),
        col("PRVCY_EXTL_PRSN_PHN_TYP_CD").alias("PRVCY_EXTL_PRSN_PHN_TYP_CD_SK"),
        col("BRTH_DT").alias("BRTH_DT_SK"),
        col("FIRST_NM").alias("FIRST_NM"),
        col("MIDINIT").alias("MIDINIT"),
        col("LAST_NM").alias("LAST_NM"),
        col("SRC_SYS_LAST_UPDT_DT").alias("SRC_SYS_LAST_UPDT_DT_SK"),
        col("SRC_SYS_LAST_UPDT_USER").alias("SRC_SYS_LAST_UPDT_USER_SK")
    )
)

# -------------------------------------------------------------------------
# ForeignKey Output Link 3 (DefaultUNK): Constraint => @INROWNUM = 1
# -------------------------------------------------------------------------
df_defaultUNK = (
    df_tfm
    .filter(col("rownum") == 1)
    .select(
        lit(0).alias("PRVCY_EXTRNL_ENTY_SK"),
        lit(0).alias("SRC_SYS_CD_SK"),
        lit(0).alias("PRVCY_EXTRNL_ENTY_UNIQ_KEY"),
        lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(0).alias("PRVCY_EXTL_PRSN_ADDR_TYP_CD_SK"),
        lit(0).alias("PRVCY_EXTRNL_PRSN_GNDR_CD_SK"),
        lit(0).alias("PRVCY_EXTL_PRSN_MRTL_STS_CD_SK"),
        lit(0).alias("PRVCY_EXTL_PRSN_PHN_TYP_CD_SK"),
        lit("UNK").alias("BRTH_DT_SK"),
        lit("UNK").alias("FIRST_NM"),
        lit("U").alias("MIDINIT"),
        lit("UNK").alias("LAST_NM"),
        lit("UNK").alias("SRC_SYS_LAST_UPDT_DT_SK"),
        lit(0).alias("SRC_SYS_LAST_UPDT_USER_SK")
    )
)

# -------------------------------------------------------------------------
# ForeignKey Output Link 4 (DefaultNA): Constraint => @INROWNUM = 1
# -------------------------------------------------------------------------
df_defaultNA = (
    df_tfm
    .filter(col("rownum") == 1)
    .select(
        lit(1).alias("PRVCY_EXTRNL_ENTY_SK"),
        lit(1).alias("SRC_SYS_CD_SK"),
        lit(1).alias("PRVCY_EXTRNL_ENTY_UNIQ_KEY"),
        lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(1).alias("PRVCY_EXTL_PRSN_ADDR_TYP_CD_SK"),
        lit(1).alias("PRVCY_EXTRNL_PRSN_GNDR_CD_SK"),
        lit(1).alias("PRVCY_EXTL_PRSN_MRTL_STS_CD_SK"),
        lit(1).alias("PRVCY_EXTL_PRSN_PHN_TYP_CD_SK"),
        lit("NA").alias("BRTH_DT_SK"),
        lit("NA").alias("FIRST_NM"),
        lit("X").alias("MIDINIT"),
        lit("NA").alias("LAST_NM"),
        lit("NA").alias("SRC_SYS_LAST_UPDT_DT_SK"),
        lit(1).alias("SRC_SYS_LAST_UPDT_USER_SK")
    )
)

# -------------------------------------------------------------------------
# Stage: hf_recycle (CHashedFileStage) - Scenario C => Write to Parquet
# -------------------------------------------------------------------------
# Reorder columns as in the DataStage link
df_recycle_ordered = df_recycle.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "PRVCY_EXTRNL_ENTY_SK",
    "PRVCY_EXTRNL_ENTY_UNIQ_KEY",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "PRVCY_EXTL_PRSN_ADDR_TYP_CD_SK",
    "PRVCY_EXTRNL_PRSN_GNDR_CD_SK",
    "PRVCY_EXTL_PRSN_MRTL_STS_CD_SK",
    "PRVCY_EXTL_PRSN_PHN_TYP_CD_SK",
    "BRTH_DT_SK",
    "FIRST_NM",
    "MIDINIT",
    "LAST_NM",
    "SRC_SYS_LAST_UPDT_DT_SK",
    "SRC_SYS_LAST_UPDT_USER_SK"
)

# Apply rpad for char/varchar columns (where length is known)
df_recycle_enriched = (
    df_recycle_ordered
    .withColumn("INSRT_UPDT_CD", rpad(col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", rpad(col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", rpad(col("PASS_THRU_IN"), 1, " "))
    .withColumn("BRTH_DT_SK", rpad(col("BRTH_DT_SK"), 10, " "))
    .withColumn("MIDINIT", rpad(col("MIDINIT"), 1, " "))
    .withColumn("SRC_SYS_LAST_UPDT_DT_SK", rpad(col("SRC_SYS_LAST_UPDT_DT_SK"), 10, " "))
)

write_files(
    df_recycle_enriched,
    f"{adls_path}/hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# -------------------------------------------------------------------------
# Stage: Collector (CCollector) => merges df_fkey, df_defaultUNK, df_defaultNA
# -------------------------------------------------------------------------
df_collected = (
    df_fkey.select(
        "PRVCY_EXTRNL_ENTY_SK",
        "SRC_SYS_CD_SK",
        "PRVCY_EXTRNL_ENTY_UNIQ_KEY",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "PRVCY_EXTL_PRSN_ADDR_TYP_CD_SK",
        "PRVCY_EXTRNL_PRSN_GNDR_CD_SK",
        "PRVCY_EXTL_PRSN_MRTL_STS_CD_SK",
        "PRVCY_EXTL_PRSN_PHN_TYP_CD_SK",
        "BRTH_DT_SK",
        "FIRST_NM",
        "MIDINIT",
        "LAST_NM",
        "SRC_SYS_LAST_UPDT_DT_SK",
        "SRC_SYS_LAST_UPDT_USER_SK"
    )
    .union(
        df_defaultUNK.select(
            "PRVCY_EXTRNL_ENTY_SK",
            "SRC_SYS_CD_SK",
            "PRVCY_EXTRNL_ENTY_UNIQ_KEY",
            "CRT_RUN_CYC_EXCTN_SK",
            "LAST_UPDT_RUN_CYC_EXCTN_SK",
            "PRVCY_EXTL_PRSN_ADDR_TYP_CD_SK",
            "PRVCY_EXTRNL_PRSN_GNDR_CD_SK",
            "PRVCY_EXTL_PRSN_MRTL_STS_CD_SK",
            "PRVCY_EXTL_PRSN_PHN_TYP_CD_SK",
            "BRTH_DT_SK",
            "FIRST_NM",
            "MIDINIT",
            "LAST_NM",
            "SRC_SYS_LAST_UPDT_DT_SK",
            "SRC_SYS_LAST_UPDT_USER_SK"
        )
    )
    .union(
        df_defaultNA.select(
            "PRVCY_EXTRNL_ENTY_SK",
            "SRC_SYS_CD_SK",
            "PRVCY_EXTRNL_ENTY_UNIQ_KEY",
            "CRT_RUN_CYC_EXCTN_SK",
            "LAST_UPDT_RUN_CYC_EXCTN_SK",
            "PRVCY_EXTL_PRSN_ADDR_TYP_CD_SK",
            "PRVCY_EXTRNL_PRSN_GNDR_CD_SK",
            "PRVCY_EXTL_PRSN_MRTL_STS_CD_SK",
            "PRVCY_EXTL_PRSN_PHN_TYP_CD_SK",
            "BRTH_DT_SK",
            "FIRST_NM",
            "MIDINIT",
            "LAST_NM",
            "SRC_SYS_LAST_UPDT_DT_SK",
            "SRC_SYS_LAST_UPDT_USER_SK"
        )
    )
)

# -------------------------------------------------------------------------
# Stage: PRVCY_EXTRNL_PRSN (CSeqFileStage) => Write the final file
# -------------------------------------------------------------------------
# Reorder columns as in DataStage
df_final_ordered = df_collected.select(
    "PRVCY_EXTRNL_ENTY_SK",
    "SRC_SYS_CD_SK",
    "PRVCY_EXTRNL_ENTY_UNIQ_KEY",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "PRVCY_EXTL_PRSN_ADDR_TYP_CD_SK",
    "PRVCY_EXTRNL_PRSN_GNDR_CD_SK",
    "PRVCY_EXTL_PRSN_MRTL_STS_CD_SK",
    "PRVCY_EXTL_PRSN_PHN_TYP_CD_SK",
    "BRTH_DT_SK",
    "FIRST_NM",
    "MIDINIT",
    "LAST_NM",
    "SRC_SYS_LAST_UPDT_DT_SK",
    "SRC_SYS_LAST_UPDT_USER_SK"
)

# Apply rpad for those columns that are declared as char(...) or known char sizes
df_final_enriched = (
    df_final_ordered
    .withColumn("BRTH_DT_SK", rpad(col("BRTH_DT_SK"), 10, " "))
    .withColumn("MIDINIT", rpad(col("MIDINIT"), 1, " "))
    .withColumn("SRC_SYS_LAST_UPDT_DT_SK", rpad(col("SRC_SYS_LAST_UPDT_DT_SK"), 10, " "))
)

write_files(
    df_final_enriched,
    f"{adls_path}/load/PRVCY_EXTRNL_PRSN.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)