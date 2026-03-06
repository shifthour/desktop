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
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:
# MAGIC 
# MAGIC PROCESSING: PRVCY_EXTRNL_ENTY_SK from the IDS PRVCY_EXTRNL_ENTY table is used in the foriegn key process.Should run after the PRVCY_EXTRNL_ENTY table.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #               Change Description                                                                                                                 Development Project      Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------     ---------------------------------------------------------                                                                                         ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Naren Garapaty               2/25/2007             FHP/3028                         Originally Programmed                                                                                                                 devlIDS30              Steph Goddard            3/29/2007
# MAGIC Steph Goddard               7/15/10              TTR-630                      changed field                                                                                                                              RebuildIntNewDevl        SANDrew                      2010-09-30
# MAGIC                                                                                                        PRVCY_EXTRNL_ENTY_PHN_TYP_CD_S to PRVCY_EXTL_ENTY_PHN_TYP_CD_SK 
# MAGIC                                                                                                        also changed to use SrcSysCdSk as parameter

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
    StructType, StructField, IntegerType, StringType, TimestampType, DecimalType
)
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# ----------------------------------------------------------------
# Retrieve job parameters
# ----------------------------------------------------------------
InFile = get_widget_value('InFile','')
Logging = get_widget_value('Logging','X')
RunID = get_widget_value('RunID','2007010107')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')

# ----------------------------------------------------------------
# Read from CSeqFileStage: IdsPrvcyExtrnlEntyPhnExtr
# ----------------------------------------------------------------
schema_IdsPrvcyExtrnlEntyPhnExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38,10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("PRVCY_EXTRNL_ENTY_PHN_SK", IntegerType(), nullable=False),
    StructField("PRVCY_EXTRNL_ENTY_UNIQ_KEY", IntegerType(), nullable=False),
    StructField("PRVCY_EXTRNL_ENTY_PHN_TYP_CD", IntegerType(), nullable=False),
    StructField("SEQ_NO", IntegerType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("PRVCY_EXTRNL_ENTY", IntegerType(), nullable=False),
    StructField("PHN_NO", StringType(), nullable=False),
    StructField("PHN_NO_EXT", StringType(), nullable=False),
    StructField("SRC_SYS_LAST_UPDT_DT", StringType(), nullable=False),
    StructField("SRC_SYS_LAST_UPDT_USER", IntegerType(), nullable=False)
])

df_IdsPrvcyExtrnlEntyPhnExtr = (
    spark.read.format("csv")
    .option("header", "false")
    .option("quote", "\"")
    .schema(schema_IdsPrvcyExtrnlEntyPhnExtr)
    .load(f"{adls_path}/key/IdsPrvcyExtrnlEntyPhnExtr.PrvcyExtrnlEntyPhn.uniq")
)

# ----------------------------------------------------------------
# Transformer Stage: ForeignKey
# ----------------------------------------------------------------
df_enriched = (
    df_IdsPrvcyExtrnlEntyPhnExtr
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("svExtrnlEntyPhnTypCdSk",
                GetFkeyCodes(
                    F.col("SRC_SYS_CD"),
                    F.col("PRVCY_EXTRNL_ENTY_PHN_SK"),
                    "PRIVACY EXTERNAL ENTITY PHONE FAX TYPE",
                    F.col("PRVCY_EXTRNL_ENTY_PHN_TYP_CD"),
                    F.col("Logging")
                )
    )
    .withColumn("svExtrnlEntySk",
                GetFkeyPrvcyExtrnlEnty(
                    F.col("SRC_SYS_CD"),
                    F.col("PRVCY_EXTRNL_ENTY_PHN_SK"),
                    F.col("PRVCY_EXTRNL_ENTY"),
                    F.col("Logging")
                )
    )
    .withColumn("svSrcSysLastUpdtDtSk",
                GetFkeyDate(
                    "IDS",
                    F.col("PRVCY_EXTRNL_ENTY_PHN_SK"),
                    F.col("SRC_SYS_LAST_UPDT_DT"),
                    F.col("Logging")
                )
    )
    .withColumn("svSrcSysLastUpdtUserSk",
                GetFkeyAppUsr(
                    F.col("SRC_SYS_CD"),
                    F.col("PRVCY_EXTRNL_ENTY_PHN_SK"),
                    F.col("SRC_SYS_LAST_UPDT_USER"),
                    F.col("Logging")
                )
    )
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("PRVCY_EXTRNL_ENTY_PHN_SK")))
    .withColumn("SrcSysCdSk", F.lit(SrcSysCdSk))
)

# ----------------------------------------------------------------
# Fkey output link (ErrCount = 0 or PassThru = 'Y')
# ----------------------------------------------------------------
df_Fkey = (
    df_enriched
    .filter((F.col("ErrCount") == 0) | (F.col("PassThru") == "Y"))
    .select(
        F.col("PRVCY_EXTRNL_ENTY_PHN_SK").alias("PRVCY_EXTRNL_ENTY_PHN_SK"),
        F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
        F.col("PRVCY_EXTRNL_ENTY_UNIQ_KEY").alias("PRVCY_EXTRNL_ENTY_UNIQ_KEY"),
        F.col("svExtrnlEntyPhnTypCdSk").alias("PRVCY_EXTL_ENTY_PHN_TYP_CD_SK"),
        F.col("SEQ_NO").alias("SEQ_NO"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("svExtrnlEntySk").alias("PRVCY_EXTRNL_ENTY_SK"),
        F.col("PHN_NO").alias("PHN_NO"),
        F.col("PHN_NO_EXT").alias("PHN_NO_EXT"),
        F.col("svSrcSysLastUpdtDtSk").alias("SRC_SYS_LAST_UPDT_DT_SK"),
        F.col("svSrcSysLastUpdtUserSk").alias("SRC_SYS_LAST_UPDT_USER_SK")
    )
)

# ----------------------------------------------------------------
# Recycle output link (ErrCount > 0)
# ----------------------------------------------------------------
df_Recycle = (
    df_enriched
    .filter(F.col("ErrCount") > 0)
    .select(
        GetRecycleKey(F.col("JOB_EXCTN_RCRD_ERR_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("DISCARD_IN").alias("DISCARD_IN"),
        F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("ErrCount").alias("ERR_CT"),
        (F.col("RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("PRVCY_EXTRNL_ENTY_PHN_SK").alias("PRVCY_EXTRNL_ENTY_PHN_SK"),
        F.col("PRVCY_EXTRNL_ENTY_UNIQ_KEY").alias("PRVCY_EXTRNL_ENTY_UNIQ_KEY"),
        F.col("PRVCY_EXTRNL_ENTY_PHN_TYP_CD").alias("PRVCY_EXTL_ENTY_PHN_TYP_CD_SK"),
        F.col("SEQ_NO").alias("SEQ_NO"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("PRVCY_EXTRNL_ENTY").alias("PRVCY_EXTRNL_ENTY_SK"),
        F.col("PHN_NO").alias("PHN_NO"),
        F.col("PHN_NO_EXT").alias("PHN_NO_EXT"),
        F.col("SRC_SYS_LAST_UPDT_DT").alias("SRC_SYS_LAST_UPDT_DT"),
        F.col("SRC_SYS_LAST_UPDT_USER").alias("SRC_SYS_LAST_UPDT_USER")
    )
)

# ----------------------------------------------------------------
# Write Hashed File (CHashedFileStage) - scenario C => parquet
# ----------------------------------------------------------------
df_Recycle_for_write = df_Recycle.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.rpad(F.col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    F.rpad(F.col("PRI_KEY_STRING"), <...>, " ").alias("PRI_KEY_STRING"),
    F.col("PRVCY_EXTRNL_ENTY_PHN_SK"),
    F.col("PRVCY_EXTRNL_ENTY_UNIQ_KEY"),
    F.col("PRVCY_EXTL_ENTY_PHN_TYP_CD_SK"),
    F.col("SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("PRVCY_EXTRNL_ENTY_SK"),
    F.rpad(F.col("PHN_NO"), <...>, " ").alias("PHN_NO"),
    F.rpad(F.col("PHN_NO_EXT"), 5, " ").alias("PHN_NO_EXT"),
    F.rpad(F.col("SRC_SYS_LAST_UPDT_DT"), 10, " ").alias("SRC_SYS_LAST_UPDT_DT"),
    F.col("SRC_SYS_LAST_UPDT_USER")
)

write_files(
    df_Recycle_for_write,
    "hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# ----------------------------------------------------------------
# DefaultUNK link (@INROWNUM = 1)
# ----------------------------------------------------------------
df_DefaultUNK = (
    df_enriched.orderBy(F.lit(1)).limit(1)
    .select(
        F.lit(0).alias("PRVCY_EXTRNL_ENTY_PHN_SK"),
        F.lit(0).alias("SRC_SYS_CD_SK"),
        F.lit(0).alias("PRVCY_EXTRNL_ENTY_UNIQ_KEY"),
        F.lit(0).alias("PRVCY_EXTL_ENTY_PHN_TYP_CD_SK"),
        F.lit(0).alias("SEQ_NO"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("PRVCY_EXTRNL_ENTY_SK"),
        F.lit("UNK").alias("PHN_NO"),
        F.lit("U").alias("PHN_NO_EXT"),
        F.lit("UNK").alias("SRC_SYS_LAST_UPDT_DT_SK"),
        F.lit(0).alias("SRC_SYS_LAST_UPDT_USER_SK")
    )
)

# ----------------------------------------------------------------
# DefaultNA link (@INROWNUM = 1)
# ----------------------------------------------------------------
df_DefaultNA = (
    df_enriched.orderBy(F.lit(1)).limit(1)
    .select(
        F.lit(1).alias("PRVCY_EXTRNL_ENTY_PHN_SK"),
        F.lit(1).alias("SRC_SYS_CD_SK"),
        F.lit(1).alias("PRVCY_EXTRNL_ENTY_UNIQ_KEY"),
        F.lit(1).alias("PRVCY_EXTL_ENTY_PHN_TYP_CD_SK"),
        F.lit(1).alias("SEQ_NO"),
        F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("PRVCY_EXTRNL_ENTY_SK"),
        F.lit("NA").alias("PHN_NO"),
        F.lit("X").alias("PHN_NO_EXT"),
        F.lit("NA").alias("SRC_SYS_LAST_UPDT_DT_SK"),
        F.lit(1).alias("SRC_SYS_LAST_UPDT_USER_SK")
    )
)

# ----------------------------------------------------------------
# Collector Stage: Union the three inputs (Fkey, DefaultUNK, DefaultNA)
# ----------------------------------------------------------------
df_Collector = df_Fkey.unionByName(df_DefaultUNK).unionByName(df_DefaultNA)

# ----------------------------------------------------------------
# Final select with column order and rpad for char/varchar fields
# ----------------------------------------------------------------
df_Collector_final = df_Collector.select(
    F.col("PRVCY_EXTRNL_ENTY_PHN_SK"),
    F.col("SRC_SYS_CD_SK"),
    F.col("PRVCY_EXTRNL_ENTY_UNIQ_KEY"),
    F.col("PRVCY_EXTL_ENTY_PHN_TYP_CD_SK"),
    F.col("SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("PRVCY_EXTRNL_ENTY_SK"),
    F.rpad(F.col("PHN_NO"), <...>, " ").alias("PHN_NO"),
    F.rpad(F.col("PHN_NO_EXT"), 5, " ").alias("PHN_NO_EXT"),
    F.rpad(F.col("SRC_SYS_LAST_UPDT_DT_SK"), 10, " ").alias("SRC_SYS_LAST_UPDT_DT_SK"),
    F.col("SRC_SYS_LAST_UPDT_USER_SK")
)

# ----------------------------------------------------------------
# Write final file CSeqFileStage: PRVCY_EXTRNL_ENTY_PHN
# ----------------------------------------------------------------
write_files(
    df_Collector_final,
    f"{adls_path}/load/PRVCY_EXTRNL_ENTY_PHN.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)