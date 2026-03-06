# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2012 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  IdsMbrSrvyQstnFltrExtrSeq
# MAGIC                
# MAGIC 
# MAGIC PROCESSING: Foreign Key job for MBR_SRVY_QSTN_FLTR
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 \(9)Project/Altiris #\(9)Change Description\(9)\(9)\(9)\(9)Development Project\(9)Code Reviewer\(9)Date Reviewed       
# MAGIC ------------------              --------------------     \(9)------------------------\(9)-----------------------------------------------------------------------\(9)--------------------------------\(9)-------------------------------\(9)----------------------------       
# MAGIC Kalyan Neelam        2012-09-19              4830 & 4735              Initial Programming                                                   IntegrateNewDevl                 Bhoomi Dasari          09/25/2012
# MAGIC Kalyan Neelam        2012-11-12              4830                          Removed an unused stager variable for                  IntegrateNewDevl                 Bhoomi Dasari          11/12/2012
# MAGIC                                                                                                  MBR_SRVY_TYP_CD_SK in the transformer

# MAGIC MBR_SRVY_QSTN_FLTR Foreign Key job
# MAGIC Writing Sequential File to /load
# MAGIC Merge source data with default rows
# MAGIC Set all foreign surrogate keys
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DecimalType
from pyspark.sql import Window
from pyspark.sql.functions import col, lit, row_number, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


InFile = get_widget_value("InFile", "")
Logging = get_widget_value("Logging", "")
SrcSysCd = get_widget_value("SrcSysCd", "")

# Schema for MbrSrvyQstnFltr (CSeqFileStage)
MbrSrvyQstnFltr_schema = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38, 10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("MBR_SRVY_QUESTON_FLTR_SK", IntegerType(), nullable=False),
    StructField("MBR_SRVY_TYP_CD", StringType(), nullable=False),
    StructField("MBR_SRVY_QSTN_CD_TX", StringType(), nullable=False),
    StructField("EFF_DT_SK", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("MBR_SRVY_QSTN_RQRD_RSN_CD", StringType(), nullable=False),
    StructField("RQRD_QSTN_IN", StringType(), nullable=False),
    StructField("TERM_DT_SK", StringType(), nullable=False),
    StructField("LAST_UPDT_DT_SK", StringType(), nullable=False),
    StructField("LAST_UPDT_USER_ID", StringType(), nullable=False),
    StructField("INCLD_RSPN_IN", StringType(), nullable=False)
])

df_MbrSrvyQstnFltr = (
    spark.read
    .format("csv")
    .option("header", False)
    .option("quote", "\"")
    .option("delimiter", ",")
    .schema(MbrSrvyQstnFltr_schema)
    .load(f"{adls_path}/key/{InFile}")
)

# ForeignKey (CTransformerStage) - create stage variables
df_ForeignKeyStageVars = (
    df_MbrSrvyQstnFltr
    .withColumn("svSrcSysCdSk", GetFkeyCodes(lit("IDS"), col("MBR_SRVY_QUESTON_FLTR_SK"), lit("SOURCE SYSTEM"), col("SRC_SYS_CD"), lit(Logging)))
    .withColumn("svMbrSrvyQstnSk", GetFkeyMbrSrvyQstn(col("SRC_SYS_CD"), col("MBR_SRVY_QUESTON_FLTR_SK"), col("MBR_SRVY_TYP_CD"), col("MBR_SRVY_QSTN_CD_TX"), lit(Logging)))
    .withColumn("svMbrSrvyQstnRqrdRsnCdSk", GetFkeyCodes(lit("UWS"), col("MBR_SRVY_QUESTON_FLTR_SK"), lit("MEMBER SURVEY QUESTION REQUIRED REASON"), col("MBR_SRVY_QSTN_RQRD_RSN_CD"), lit(Logging)))
    .withColumn("svEffDtSk", GetFkeyDate(lit("IDS"), col("MBR_SRVY_QUESTON_FLTR_SK"), col("EFF_DT_SK"), lit(Logging)))
    .withColumn("svTermDtSk", GetFkeyDate(lit("IDS"), col("MBR_SRVY_QUESTON_FLTR_SK"), col("TERM_DT_SK"), lit(Logging)))
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn("ErrCount", GetFkeyErrorCnt(col("MBR_SRVY_QUESTON_FLTR_SK")))
)

# Add row number to replicate @INROWNUM usage
w = Window.orderBy(col("MBR_SRVY_QUESTON_FLTR_SK"))
df_ForeignKeyStageVars = df_ForeignKeyStageVars.withColumn("INROWNUM", row_number().over(w))

# Split outputs from ForeignKey

# Fkey link: Constraint: ErrCount = 0 Or PassThru = 'Y'
df_Fkey = df_ForeignKeyStageVars.filter((col("ErrCount") == 0) | (col("PassThru") == "Y"))

# Recycle link: Constraint: ErrCount > 0
df_Recycle = df_ForeignKeyStageVars.filter(col("ErrCount") > 0)

# DefaultNA link: Constraint: @INROWNUM = 1
df_DefaultNA = df_ForeignKeyStageVars.filter(col("INROWNUM") == 1)

# DefaultUNK link: Constraint: @INROWNUM = 1
df_DefaultUNK = df_ForeignKeyStageVars.filter(col("INROWNUM") == 1)

# hf_recycle (CHashedFileStage) - scenario C => write to parquet
df_RecycleSelected = df_Recycle.select(
    GetRecycleKey(col("MBR_SRVY_QUESTON_FLTR_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
    col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    col("DISCARD_IN").alias("DISCARD_IN"),
    col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    col("ERR_CT").alias("ERR_CT"),
    (col("RECYCLE_CT") + lit(1)).alias("RECYCLE_CT"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    col("MBR_SRVY_QUESTON_FLTR_SK").alias("MBR_SRVY_QUESTON_FLTR_SK"),
    col("MBR_SRVY_TYP_CD").alias("MBR_SRVY_TYP_CD"),
    col("MBR_SRVY_QSTN_CD_TX").alias("MBR_SRVY_QSTN_CD_TX"),
    col("EFF_DT_SK").alias("EFF_DT_SK"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("MBR_SRVY_QSTN_RQRD_RSN_CD").alias("MBR_SRVY_QSTN_RQRD_RSN_CD"),
    col("RQRD_QSTN_IN").alias("RQRD_QSTN_IN"),
    col("TERM_DT_SK").alias("TERM_DT_SK"),
    col("LAST_UPDT_DT_SK").alias("LAST_UPDT_DT_SK"),
    col("LAST_UPDT_USER_ID").alias("LAST_UPDT_USER_ID"),
    col("INCLD_RSPN_IN").alias("INCLD_RSPN_IN")
)

write_files(
    df_RecycleSelected,
    "hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

# Collector (CCollector) - collects Fkey, DefaultNA, DefaultUNK into one dataframe
df_FkeySelected = df_Fkey.select(
    col("MBR_SRVY_QUESTON_FLTR_SK").alias("MBR_SRVY_QUESTON_FLTR_SK"),
    col("svSrcSysCdSk").alias("SRC_SYS_CD_SK"),
    col("MBR_SRVY_TYP_CD").alias("MBR_SRVY_TYP_CD"),
    col("MBR_SRVY_QSTN_CD_TX").alias("MBR_SRVY_QSTN_CD_TX"),
    col("svEffDtSk").alias("EFF_DT_SK"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("svMbrSrvyQstnSk").alias("MBR_SRVY_QSTN_SK"),
    col("svMbrSrvyQstnRqrdRsnCdSk").alias("MBR_SRVY_QSTN_RQRD_RSN_CD_SK"),
    col("RQRD_QSTN_IN").alias("RQRD_QSTN_IN"),
    col("svTermDtSk").alias("TERM_DT_SK"),
    col("LAST_UPDT_DT_SK").alias("LAST_UPDT_DT_SK"),
    col("LAST_UPDT_USER_ID").alias("LAST_UPDT_USER_ID"),
    col("INCLD_RSPN_IN").alias("INCLD_RSPN_IN")
)

df_DefaultNASelected = df_DefaultNA.select(
    lit(1).alias("MBR_SRVY_QUESTON_FLTR_SK"),
    lit(1).alias("SRC_SYS_CD_SK"),
    lit("NA").alias("MBR_SRVY_TYP_CD"),
    lit("NA").alias("MBR_SRVY_QSTN_CD_TX"),
    lit("1753-01-01").alias("EFF_DT_SK"),
    lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(1).alias("MBR_SRVY_QSTN_SK"),
    lit(1).alias("MBR_SRVY_QSTN_RQRD_RSN_CD_SK"),
    lit("N").alias("RQRD_QSTN_IN"),
    lit("2199-12-31").alias("TERM_DT_SK"),
    lit("1753-01-01").alias("LAST_UPDT_DT_SK"),
    lit("NA").alias("LAST_UPDT_USER_ID"),
    lit("N").alias("INCLD_RSPN_IN")
)

df_DefaultUNKSelected = df_DefaultUNK.select(
    lit(0).alias("MBR_SRVY_QUESTON_FLTR_SK"),
    lit(0).alias("SRC_SYS_CD_SK"),
    lit("UNK").alias("MBR_SRVY_TYP_CD"),
    lit("UNK").alias("MBR_SRVY_QSTN_CD_TX"),
    lit("1753-01-01").alias("EFF_DT_SK"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("MBR_SRVY_QSTN_SK"),
    lit(0).alias("MBR_SRVY_QSTN_RQRD_RSN_CD_SK"),
    lit("N").alias("RQRD_QSTN_IN"),
    lit("2199-12-31").alias("TERM_DT_SK"),
    lit("1753-01-01").alias("LAST_UPDT_DT_SK"),
    lit("UNK").alias("LAST_UPDT_USER_ID"),
    lit("N").alias("INCLD_RSPN_IN")
)

df_Collector = df_FkeySelected.union(df_DefaultNASelected).union(df_DefaultUNKSelected)

# Final dataframe for MBR_SRVY_QSTN_FLTR (CSeqFileStage)
df_final = df_Collector.select(
    col("MBR_SRVY_QUESTON_FLTR_SK"),
    col("SRC_SYS_CD_SK"),
    col("MBR_SRVY_TYP_CD"),
    col("MBR_SRVY_QSTN_CD_TX"),
    rpad(col("EFF_DT_SK"), 10, " ").alias("EFF_DT_SK"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("MBR_SRVY_QSTN_SK"),
    col("MBR_SRVY_QSTN_RQRD_RSN_CD_SK"),
    rpad(col("RQRD_QSTN_IN"), 1, " ").alias("RQRD_QSTN_IN"),
    rpad(col("TERM_DT_SK"), 10, " ").alias("TERM_DT_SK"),
    rpad(col("LAST_UPDT_DT_SK"), 10, " ").alias("LAST_UPDT_DT_SK"),
    col("LAST_UPDT_USER_ID"),
    rpad(col("INCLD_RSPN_IN"), 1, " ").alias("INCLD_RSPN_IN")
)

write_files(
    df_final,
    f"{adls_path}/load/MBR_SRVY_QSTN_FLTR.{SrcSysCd}.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)