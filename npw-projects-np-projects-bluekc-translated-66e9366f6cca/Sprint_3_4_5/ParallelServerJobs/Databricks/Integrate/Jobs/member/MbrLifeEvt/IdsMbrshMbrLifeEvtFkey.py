# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC CALLED BY:IdsMbrshMbrLifeEvtLoadSeq
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                 Date                 Project/Altiris #                                   Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------                                   -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Jagadesh Yelavarthi   2010-12-15                                                                    New ETL                                                                                     IntegrateNewDevl     Steph Goddard          01/06/2011

# MAGIC Job to load IDS-MBR_LIFE_EVT table; Main source data for this table comes from Claim and claim line tables
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DecimalType
from pyspark.sql import Window
from pyspark.sql.functions import col, lit, row_number, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Retrieve Job Parameters
CurrRunCycle = get_widget_value("CurrRunCycle","100")
CurrDate = get_widget_value("CurrDate","")
RunID = get_widget_value("RunID","")
InFile = get_widget_value("InFile","WebInfoMbrLifeEvt.uniq")
Logging = get_widget_value("Logging","Y")

# Define schema for MbrLifeEvtCrf (CSeqFileStage)
schema_MbrLifeEvtCrf = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(10,0), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("MBR_LIFE_EVT_SK", IntegerType(), nullable=False),
    StructField("MBR_UNIQ_KEY", IntegerType(), nullable=False),
    StructField("MBR_LIFE_EVT_TYP_CD", StringType(), nullable=False),
    StructField("CLM_SVC_STRT_DT_SK", StringType(), nullable=False),
    StructField("SRC_SYS_CD_SK", IntegerType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("CLM_SK", IntegerType(), nullable=False),
    StructField("MBR_SK", IntegerType(), nullable=False),
    StructField("SBR_SK", IntegerType(), nullable=False),
    StructField("CLM_ID", StringType(), nullable=False),
    StructField("LIFE_EVT_SRC_TYP_CD", StringType(), nullable=False),
    StructField("LIFE_EVT_TYP", StringType(), nullable=False)
])

# Read from MbrLifeEvtCrf
df_MbrLifeEvtCrf = (
    spark.read
    .schema(schema_MbrLifeEvtCrf)
    .option("delimiter", ",")
    .option("quote", "\"")
    .option("header", "false")
    .csv(f"{adls_path}/key/" + InFile)
)

# Apply Transformer logic (FK_Trn) with stage variables
df_fk_trn_temp = (
    df_MbrLifeEvtCrf
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn("svLifeEvtSrcTypCdSK", GetFkeyCodes(col("SRC_SYS_CD"), col("MBR_LIFE_EVT_SK"), lit("MEMBER LIFE EVENT SOURCE"), col("LIFE_EVT_SRC_TYP_CD"), lit(Logging)))
    .withColumn("svMbrLifeEvtTypCdSK", GetFkeyCodes(col("SRC_SYS_CD"), col("MBR_LIFE_EVT_SK"), lit("MEMBER LIFE EVENT"), col("LIFE_EVT_TYP"), lit(Logging)))
    .withColumn("ErrCount", GetFkeyErrorCnt(col("MBR_LIFE_EVT_SK")))
)

# Add a row number for @INROWNUM = 1 constraints
w = Window.orderBy(lit(1))
df_fk_trn = df_fk_trn_temp.withColumn("rownum", row_number().over(w))

# NA link (Constraint: @INROWNUM=1)
df_na = df_fk_trn.filter("rownum=1").select(
    col("MBR_LIFE_EVT_SK"),
    col("MBR_UNIQ_KEY"),
    lit("NA").alias("MBR_LIFE_EVT_TYP_CD"),
    lit("1").alias("CLM_SVC_STRT_DT_SK"),
    col("SRC_SYS_CD_SK"),
    lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("CLM_SK"),
    col("MBR_SK"),
    col("SBR_SK").alias("SUB_SK"),
    col("svLifeEvtSrcTypCdSK").alias("LIFE_EVT_SRC_TYP_CD_SK"),
    col("svMbrLifeEvtTypCdSK").alias("MBR_LIFE_EVT_TYP_CD_SK"),
    lit(" ").alias("CLM_ID")
)

# UNK link (Constraint: @INROWNUM=1)
df_unk = df_fk_trn.filter("rownum=1").select(
    lit(0).alias("MBR_LIFE_EVT_SK"),
    lit(0).alias("MBR_UNIQ_KEY"),
    lit("UNK").alias("MBR_LIFE_EVT_TYP_CD"),
    lit("0").alias("CLM_SVC_STRT_DT_SK"),
    lit(0).alias("SRC_SYS_CD_SK"),
    lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("CLM_SK"),
    lit(0).alias("MBR_SK"),
    lit(0).alias("SUB_SK"),
    lit(0).alias("LIFE_EVT_SRC_TYP_CD_SK"),
    lit(0).alias("MBR_LIFE_EVT_TYP_CD_SK"),
    lit(" ").alias("CLM_ID")
)

# MbrLifeEvt link (Constraint: ErrCount=0 Or PassThru='Y')
df_mbrLifeEvt = df_fk_trn.filter((col("ErrCount") == 0) | (col("PassThru") == "Y")).select(
    col("MBR_LIFE_EVT_SK"),
    col("MBR_UNIQ_KEY"),
    col("MBR_LIFE_EVT_TYP_CD"),
    col("CLM_SVC_STRT_DT_SK"),
    col("SRC_SYS_CD_SK"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("CLM_SK"),
    col("MBR_SK"),
    col("SBR_SK").alias("SUB_SK"),
    col("svLifeEvtSrcTypCdSK").alias("LIFE_EVT_SRC_TYP_CD_SK"),
    col("svMbrLifeEvtTypCdSK").alias("MBR_LIFE_EVT_TYP_CD_SK"),
    col("CLM_ID")
)

# lnkRecycle link (Constraint: ErrCount>0)
df_lnkRecycle = df_fk_trn.filter(col("ErrCount") > 0).select(
    GetRecycleKey(col("JOB_EXCTN_RCRD_ERR_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
    col("INSRT_UPDT_CD"),
    col("DISCARD_IN"),
    col("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ErrCount").alias("ERR_CT"),
    (col("RECYCLE_CT") + lit(1)).alias("RECYCLE_CT"),
    col("SRC_SYS_CD"),
    col("PRI_KEY_STRING"),
    col("MBR_LIFE_EVT_SK"),
    col("MBR_UNIQ_KEY"),
    col("MBR_LIFE_EVT_TYP_CD"),
    col("CLM_SVC_STRT_DT_SK"),
    col("SRC_SYS_CD_SK"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("CLM_SK"),
    col("MBR_SK"),
    col("SBR_SK"),
    col("CLM_ID")
)

# Write to hf_recycle (CHashedFileStage => Scenario C => Write parquet)
write_files(
    df_lnkRecycle,
    "hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

# Collector stage: union NA, UNK, and MbrLifeEvt
df_na_mapped = df_na.select(
    col("MBR_LIFE_EVT_SK"),
    col("MBR_UNIQ_KEY"),
    col("MBR_LIFE_EVT_TYP_CD"),
    col("CLM_SVC_STRT_DT_SK").alias("LIFE_EVT_STRT_DT_SK"),
    col("SRC_SYS_CD_SK"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("CLM_SK"),
    col("MBR_SK"),
    col("SUB_SK"),
    col("LIFE_EVT_SRC_TYP_CD_SK"),
    col("MBR_LIFE_EVT_TYP_CD_SK"),
    col("CLM_ID")
)

df_unk_mapped = df_unk.select(
    col("MBR_LIFE_EVT_SK"),
    col("MBR_UNIQ_KEY"),
    col("MBR_LIFE_EVT_TYP_CD"),
    col("CLM_SVC_STRT_DT_SK").alias("LIFE_EVT_STRT_DT_SK"),
    col("SRC_SYS_CD_SK"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("CLM_SK"),
    col("MBR_SK"),
    col("SUB_SK"),
    col("LIFE_EVT_SRC_TYP_CD_SK"),
    col("MBR_LIFE_EVT_TYP_CD_SK"),
    col("CLM_ID")
)

df_mbrLifeEvt_mapped = df_mbrLifeEvt.select(
    col("MBR_LIFE_EVT_SK"),
    col("MBR_UNIQ_KEY"),
    col("MBR_LIFE_EVT_TYP_CD"),
    col("CLM_SVC_STRT_DT_SK").alias("LIFE_EVT_STRT_DT_SK"),
    col("SRC_SYS_CD_SK"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("CLM_SK"),
    col("MBR_SK"),
    col("SUB_SK"),
    col("LIFE_EVT_SRC_TYP_CD_SK"),
    col("MBR_LIFE_EVT_TYP_CD_SK"),
    col("CLM_ID")
)

df_collector = df_na_mapped.unionByName(df_unk_mapped).unionByName(df_mbrLifeEvt_mapped)

# Final output to IdsMbrLifeEvt (CSeqFileStage)
# For char(10) column: LIFE_EVT_STRT_DT_SK => rpad with length=10
df_final = df_collector.withColumn(
    "LIFE_EVT_STRT_DT_SK",
    rpad(col("LIFE_EVT_STRT_DT_SK"), 10, " ")
).select(
    "MBR_LIFE_EVT_SK",
    "MBR_UNIQ_KEY",
    "MBR_LIFE_EVT_TYP_CD",
    "LIFE_EVT_STRT_DT_SK",
    "SRC_SYS_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CLM_SK",
    "MBR_SK",
    "SUB_SK",
    "LIFE_EVT_SRC_TYP_CD_SK",
    "MBR_LIFE_EVT_TYP_CD_SK",
    "CLM_ID"
)

write_files(
    df_final,
    f"{adls_path}/load/MBR_LIFE_EVT.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)