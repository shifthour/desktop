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
# MAGIC Naren Garapaty               2/25/2007           FHP /3028                       Originally Programmed                              devlIDS30                Steph Goddard            3/29/2007

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
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DecimalType
from pyspark.sql.functions import col, lit, row_number, rpad
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Retrieve job parameters
InFile = get_widget_value("InFile", "")
Logging = get_widget_value("Logging", "X")
RunID = get_widget_value("RunID", "2007010110")

# --------------------------------------------------------------------------------
# Read from CSeqFileStage: IdsPrvcyExtrnlMbrExtr
# --------------------------------------------------------------------------------
schema_IdsPrvcyExtrnlMbrExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38,10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("PRVCY_EXTRNL_MBR_SK", IntegerType(), False),
    StructField("PRVCY_EXTRNL_MBR_UNIQ_KEY", IntegerType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("PRVCY_EXTRNL_MBR_RELSHP_CD", IntegerType(), False),
    StructField("ALPHA_PFX_TX", StringType(), False),
    StructField("CNTR_NO", StringType(), False),
    StructField("GRP_NO", StringType(), False),
    StructField("MBR_EXT_NO", StringType(), True),
    StructField("MBR_ID", StringType(), False),
    StructField("SRC_SYS_LAST_UPDT_DT", StringType(), False),
    StructField("SRC_SYS_LAST_UPDT_USER", IntegerType(), False)
])

df_IdsPrvcyExtrnlMbrExtr = (
    spark.read
    .option("header", "false")
    .option("quote", "\"")
    .option("sep", ",")
    .schema(schema_IdsPrvcyExtrnlMbrExtr)
    .csv(f"{adls_path}/key/{InFile}")
)

# --------------------------------------------------------------------------------
# Transformer: ForeignKey
# --------------------------------------------------------------------------------
dfForeignKey = (
    df_IdsPrvcyExtrnlMbrExtr
    .withColumn("SrcSysCdSk", GetFkeyCodes(trim(lit("IDS")), col("PRVCY_EXTRNL_MBR_SK"), trim(lit("SOURCE SYSTEM")), trim(col("SRC_SYS_CD")), Logging))
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn("svPrvcyExtrnlMbrRelshpCdSk", GetFkeyCodes(col("SRC_SYS_CD"), col("PRVCY_EXTRNL_MBR_SK"), lit("MEMBER RELATIONSHIP"), col("PRVCY_EXTRNL_MBR_RELSHP_CD"), Logging))
    .withColumn("svSrcSysLastUpdtDtSk", GetFkeyDate(lit("IDS"), col("PRVCY_EXTRNL_MBR_SK"), col("SRC_SYS_LAST_UPDT_DT"), Logging))
    .withColumn("svSrcSysLastUpdtUserSk", GetFkeyAppUsr(col("SRC_SYS_CD"), col("PRVCY_EXTRNL_MBR_SK"), col("SRC_SYS_LAST_UPDT_USER"), Logging))
    .withColumn("ErrCount", GetFkeyErrorCnt(col("PRVCY_EXTRNL_MBR_SK")))
)

# We need a row number for the @INROWNUM usage
w = Window.orderBy(lit(1))
dfForeignKeyWithRN = dfForeignKey.withColumn("row_num", row_number().over(w))

# Output link "Fkey" (ErrCount = 0 Or PassThru = 'Y')
dfFkeySel = dfForeignKeyWithRN.filter(
    (col("ErrCount") == lit(0)) | (col("PassThru") == lit("Y"))
).select(
    col("PRVCY_EXTRNL_MBR_SK").alias("PRVCY_EXTRNL_MBR_SK"),
    col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
    col("PRVCY_EXTRNL_MBR_UNIQ_KEY").alias("PRVCY_EXTRNL_MBR_UNIQ_KEY"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("svPrvcyExtrnlMbrRelshpCdSk").alias("PRVCY_EXTRNL_MBR_RELSHP_CD_SK"),
    col("ALPHA_PFX_TX").alias("ALPHA_PFX_TX"),
    col("CNTR_NO").alias("CNTR_NO"),
    col("GRP_NO").alias("GRP_NO"),
    col("MBR_EXT_NO").alias("MBR_EXT_NO"),
    col("MBR_ID").alias("MBR_ID"),
    col("svSrcSysLastUpdtDtSk").alias("SRC_SYS_LAST_UPDT_DT_SK"),
    col("svSrcSysLastUpdtUserSk").alias("SRC_SYS_LAST_UPDT_USER_SK")
)

# Output link "Recycle" (ErrCount > 0) => hashed file "hf_recycle"
dfRecycle = dfForeignKeyWithRN.filter(
    col("ErrCount") > lit(0)
).select(
    GetRecycleKey(col("PRVCY_EXTRNL_MBR_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
    col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    col("DISCARD_IN").alias("DISCARD_IN"),
    col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    col("ErrCount").alias("ERR_CT"),
    (col("RECYCLE_CT") + lit(1)).alias("RECYCLE_CT"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    col("PRVCY_EXTRNL_MBR_SK").alias("PRVCY_EXTRNL_MBR_SK"),
    col("PRVCY_EXTRNL_MBR_UNIQ_KEY").alias("PRVCY_EXTRNL_MBR_UNIQ_KEY"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("PRVCY_EXTRNL_MBR_RELSHP_CD").alias("PRVCY_EXTRNL_MBR_RELSHP_CD"),
    col("ALPHA_PFX_TX").alias("ALPHA_PFX_TX"),
    col("CNTR_NO").alias("CNTR_NO"),
    col("GRP_NO").alias("GRP_NO"),
    col("MBR_EXT_NO").alias("MBR_EXT_NO"),
    col("MBR_ID").alias("MBR_ID"),
    col("SRC_SYS_LAST_UPDT_DT").alias("SRC_SYS_LAST_UPDT_DT_SK"),
    col("SRC_SYS_LAST_UPDT_USER").alias("SRC_SYS_LAST_UPDT_USER_SK")
)

# --------------------------------------------------------------------------------
# Write to Hashed File Stage: hf_recycle (Scenario C => write parquet)
# --------------------------------------------------------------------------------
write_files(
    dfRecycle,
    f"{adls_path}/hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# Output link "DefaultUNK": @INROWNUM = 1
dfDefaultUNKSel = dfForeignKeyWithRN.filter(col("row_num") == lit(1)).select(
    lit(0).alias("PRVCY_EXTRNL_MBR_SK"),
    lit(0).alias("SRC_SYS_CD_SK"),
    lit(0).alias("PRVCY_EXTRNL_MBR_UNIQ_KEY"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("PRVCY_EXTRNL_MBR_RELSHP_CD_SK"),
    lit("UNK").alias("ALPHA_PFX_TX"),
    lit("UNK").alias("CNTR_NO"),
    lit("UNK").alias("GRP_NO"),
    lit("U").alias("MBR_EXT_NO"),
    lit(0).alias("MBR_ID"),
    lit("UNK").alias("SRC_SYS_LAST_UPDT_DT_SK"),
    lit(0).alias("SRC_SYS_LAST_UPDT_USER_SK")
)

# Output link "DefaultNA": @INROWNUM = 1
dfDefaultNASel = dfForeignKeyWithRN.filter(col("row_num") == lit(1)).select(
    lit(1).alias("PRVCY_EXTRNL_MBR_SK"),
    lit(1).alias("SRC_SYS_CD_SK"),
    lit(1).alias("PRVCY_EXTRNL_MBR_UNIQ_KEY"),
    lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(1).alias("PRVCY_EXTRNL_MBR_RELSHP_CD_SK"),
    lit("NA").alias("ALPHA_PFX_TX"),
    lit("NA").alias("CNTR_NO"),
    lit("NA").alias("GRP_NO"),
    lit("X").alias("MBR_EXT_NO"),
    lit(1).alias("MBR_ID"),
    lit("NA").alias("SRC_SYS_LAST_UPDT_DT_SK"),
    lit(1).alias("SRC_SYS_LAST_UPDT_USER_SK")
)

# --------------------------------------------------------------------------------
# Collector Stage: Union of Fkey, DefaultUNK, and DefaultNA
# --------------------------------------------------------------------------------
dfCollector = (
    dfFkeySel.unionByName(dfDefaultUNKSel)
             .unionByName(dfDefaultNASel)
)

# --------------------------------------------------------------------------------
# Write final output file: PRVCY_EXTRNL_MBR.dat (CSeqFileStage)
# --------------------------------------------------------------------------------
dfFinal = dfCollector.select(
    col("PRVCY_EXTRNL_MBR_SK"),
    col("SRC_SYS_CD_SK"),
    col("PRVCY_EXTRNL_MBR_UNIQ_KEY"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("PRVCY_EXTRNL_MBR_RELSHP_CD_SK"),
    col("ALPHA_PFX_TX"),
    col("CNTR_NO"),
    col("GRP_NO"),
    rpad(col("MBR_EXT_NO"), 2, " ").alias("MBR_EXT_NO"),
    col("MBR_ID"),
    rpad(col("SRC_SYS_LAST_UPDT_DT_SK"), 10, " ").alias("SRC_SYS_LAST_UPDT_DT_SK"),
    col("SRC_SYS_LAST_UPDT_USER_SK")
)

write_files(
    dfFinal,
    f"{adls_path}/load/PRVCY_EXTRNL_MBR.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)