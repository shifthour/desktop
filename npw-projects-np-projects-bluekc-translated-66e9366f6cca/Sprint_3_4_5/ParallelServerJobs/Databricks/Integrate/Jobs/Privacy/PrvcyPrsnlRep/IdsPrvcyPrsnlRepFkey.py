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
# MAGIC PROCESSING:Takes the primary key file and use it in the foriegn key process.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #               Change Description                             Development Project      Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------     ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Bhoomi Dasari               3/10/2007            CDS Sunset/3279          Originally Programmed                              devlIDS30                 Steph Goddard            3/29/2007

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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DecimalType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

InFile = get_widget_value("InFile", "FctsPrvcyPrsnlRepExtr.PrvcyPrsnlRep.dat")
Logging = get_widget_value("Logging", "Y")
RunID = get_widget_value("RunID", "200701011")

# ----------------------------------------------------------------------------
# Stage: IdsPrvcyPrsnlRepExtr (CSeqFileStage)
# ----------------------------------------------------------------------------
schema_IdsPrvcyPrsnlRepExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(10, 0), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("PRVCY_PRSNL_REP_SK", IntegerType(), False),
    StructField("PRVCY_PRSNL_REP_UNIQ_KEY", IntegerType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("PRVCY_PRSNL_REP_TYP_CD", IntegerType(), False),
    StructField("PRVCY_PRSNL_REP_ID", StringType(), False),
    StructField("SRC_SYS_LAST_UPDT_DT", StringType(), False),
    StructField("SRC_SYS_LAST_UPDT_USER", IntegerType(), False)
])

df_IdsPrvcyPrsnlRepExtr = (
    spark.read.format("csv")
    .option("header", "false")
    .option("quote", "\"")
    .option("delimiter", ",")
    .schema(schema_IdsPrvcyPrsnlRepExtr)
    .load(f"{adls_path}/key/{InFile}")
)

# ----------------------------------------------------------------------------
# Stage: ForeignKey (CTransformerStage)
# ----------------------------------------------------------------------------
df_ForeignKey = (
    df_IdsPrvcyPrsnlRepExtr
    .withColumn(
        "SrcSysCdSk",
        GetFkeyCodes(
            F.lit("IDS"),
            F.col("PRVCY_PRSNL_REP_SK"),
            trim(F.lit("SOURCE SYSTEM")),
            trim(F.col("SRC_SYS_CD")),
            Logging
        )
    )
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn(
        "svPrvcyPrsnlRepRelshpCdSk",
        GetFkeyCodes(
            F.col("SRC_SYS_CD"),
            F.col("PRVCY_PRSNL_REP_SK"),
            F.lit(" PRIVACY PERSONAL REPRESENTATIVE TYPE "),
            F.col("PRVCY_PRSNL_REP_TYP_CD"),
            Logging
        )
    )
    .withColumn(
        "svSrcSysLastUpdtDtSk",
        GetFkeyDate(
            F.lit("IDS"),
            F.col("PRVCY_PRSNL_REP_SK"),
            F.col("SRC_SYS_LAST_UPDT_DT"),
            Logging
        )
    )
    .withColumn(
        "svSrcSysLastUpdtUserSk",
        GetFkeyAppUsr(
            F.col("SRC_SYS_CD"),
            F.col("PRVCY_PRSNL_REP_SK"),
            F.col("SRC_SYS_LAST_UPDT_USER"),
            Logging
        )
    )
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("PRVCY_PRSNL_REP_SK")))
)

# ----------------------------------------------------------------------------
# Output link "Fkey" from ForeignKey
# Constraint: ErrCount = 0 OR PassThru = 'Y'
# Columns:
#   PRVCY_PRSNL_REP_SK, SRC_SYS_CD_SK, PRVCY_PRSNL_REP_UNIQ_KEY,
#   CRT_RUN_CYC_EXCTN_SK, LAST_UPDT_RUN_CYC_EXCTN_SK,
#   PRVCY_PRSNL_REP_TYP_CD_SK, PRVCY_PRSNL_REP_ID,
#   SRC_SYS_LAST_UPDT_DT_SK, SRC_SYS_LAST_UPDT_USER_SK
# ----------------------------------------------------------------------------
df_Fkey = (
    df_ForeignKey
    .filter((F.col("ErrCount") == 0) | (F.col("PassThru") == "Y"))
    .select(
        F.col("PRVCY_PRSNL_REP_SK").alias("PRVCY_PRSNL_REP_SK"),
        F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
        F.col("PRVCY_PRSNL_REP_UNIQ_KEY").alias("PRVCY_PRSNL_REP_UNIQ_KEY"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("svPrvcyPrsnlRepRelshpCdSk").alias("PRVCY_PRSNL_REP_TYP_CD_SK"),
        F.col("PRVCY_PRSNL_REP_ID").alias("PRVCY_PRSNL_REP_ID"),
        F.col("svSrcSysLastUpdtDtSk").alias("SRC_SYS_LAST_UPDT_DT_SK"),
        F.col("svSrcSysLastUpdtUserSk").alias("SRC_SYS_LAST_UPDT_USER_SK")
    )
)

# ----------------------------------------------------------------------------
# Output link "Recycle" from ForeignKey
# Constraint: ErrCount > 0
# ----------------------------------------------------------------------------
df_Recycle = (
    df_ForeignKey
    .filter(F.col("ErrCount") > 0)
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", GetRecycleKey(F.col("PRVCY_PRSNL_REP_SK")))
    .select(
        F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("DISCARD_IN").alias("DISCARD_IN"),
        F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("ErrCount").alias("ERR_CT"),
        (F.col("RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("PRVCY_PRSNL_REP_SK").alias("PRVCY_PRSNL_REP_SK"),
        F.col("PRVCY_PRSNL_REP_UNIQ_KEY").alias("PRVCY_PRSNL_REP_UNIQ_KEY"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("PRVCY_PRSNL_REP_TYP_CD").alias("PRVCY_PRSNL_REP_TYP_CD_SK"),
        F.col("PRVCY_PRSNL_REP_ID").alias("PRVCY_PRSNL_REP_ID"),
        F.col("SRC_SYS_LAST_UPDT_DT").alias("SRC_SYS_LAST_UPDT_DT_SK"),
        F.col("SRC_SYS_LAST_UPDT_USER").alias("SRC_SYS_LAST_UPDT_USER_SK")
    )
)

# ----------------------------------------------------------------------------
# Output link "DefaultUNK" from ForeignKey
# Constraint: @INROWNUM = 1
# Produces a single row with specific literal values
# ----------------------------------------------------------------------------
schema_Collector = StructType([
    StructField("PRVCY_PRSNL_REP_SK", IntegerType(), True),
    StructField("SRC_SYS_CD_SK", IntegerType(), True),
    StructField("PRVCY_PRSNL_REP_UNIQ_KEY", IntegerType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("PRVCY_PRSNL_REP_TYP_CD_SK", IntegerType(), True),
    StructField("PRVCY_PRSNL_REP_ID", StringType(), True),
    StructField("SRC_SYS_LAST_UPDT_DT_SK", StringType(), True),
    StructField("SRC_SYS_LAST_UPDT_USER_SK", IntegerType(), True)
])

df_DefaultUNK = spark.createDataFrame(
    [
        (
            0,    # PRVCY_PRSNL_REP_SK
            0,    # SRC_SYS_CD_SK
            0,    # PRVCY_PRSNL_REP_UNIQ_KEY
            0,    # CRT_RUN_CYC_EXCTN_SK
            0,    # LAST_UPDT_RUN_CYC_EXCTN_SK
            0,    # PRVCY_PRSNL_REP_TYP_CD_SK
            "UNK", # PRVCY_PRSNL_REP_ID
            "UNK", # SRC_SYS_LAST_UPDT_DT_SK
            0     # SRC_SYS_LAST_UPDT_USER_SK
        )
    ],
    schema_Collector
)

# ----------------------------------------------------------------------------
# Output link "DefaultNA" from ForeignKey
# Constraint: @INROWNUM = 1
# Produces a single row with specific literal values
# ----------------------------------------------------------------------------
df_DefaultNA = spark.createDataFrame(
    [
        (
            1,   # PRVCY_PRSNL_REP_SK
            1,   # SRC_SYS_CD_SK
            1,   # PRVCY_PRSNL_REP_UNIQ_KEY
            1,   # CRT_RUN_CYC_EXCTN_SK
            1,   # LAST_UPDT_RUN_CYC_EXCTN_SK
            1,   # PRVCY_PRSNL_REP_TYP_CD_SK
            "NA",# PRVCY_PRSNL_REP_ID
            "NA",# SRC_SYS_LAST_UPDT_DT_SK
            1    # SRC_SYS_LAST_UPDT_USER_SK
        )
    ],
    schema_Collector
)

# ----------------------------------------------------------------------------
# Stage: hf_recycle (CHashedFileStage) - Scenario C => write as Parquet
# ----------------------------------------------------------------------------
# Maintain column order exactly as pinned
df_Recycle_final = df_Recycle.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "PRVCY_PRSNL_REP_SK",
    "PRVCY_PRSNL_REP_UNIQ_KEY",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "PRVCY_PRSNL_REP_TYP_CD_SK",
    "PRVCY_PRSNL_REP_ID",
    "SRC_SYS_LAST_UPDT_DT_SK",
    "SRC_SYS_LAST_UPDT_USER_SK"
)

write_files(
    df_Recycle_final,
    "hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

# ----------------------------------------------------------------------------
# Stage: Collector (CCollector)
# Union the three inputs: Fkey, DefaultUNK, DefaultNA
# ----------------------------------------------------------------------------
df_Collector = (
    df_Fkey
    .unionByName(df_DefaultUNK)
    .unionByName(df_DefaultNA)
)

# ----------------------------------------------------------------------------
# Output link "LoadFile" from Collector => PRVCY_PRSNL_REP (CSeqFileStage)
# ----------------------------------------------------------------------------
df_Collector_final = (
    df_Collector
    .select(
        "PRVCY_PRSNL_REP_SK",
        "SRC_SYS_CD_SK",
        "PRVCY_PRSNL_REP_UNIQ_KEY",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "PRVCY_PRSNL_REP_TYP_CD_SK",
        "PRVCY_PRSNL_REP_ID",
        "SRC_SYS_LAST_UPDT_DT_SK",
        "SRC_SYS_LAST_UPDT_USER_SK"
    )
    .withColumn(
        "PRVCY_PRSNL_REP_ID",
        F.rpad(F.col("PRVCY_PRSNL_REP_ID"), 100, " ")
    )
    .withColumn(
        "SRC_SYS_LAST_UPDT_DT_SK",
        F.rpad(F.col("SRC_SYS_LAST_UPDT_DT_SK"), 10, " ")
    )
)

write_files(
    df_Collector_final,
    f"{adls_path}/load/PRVCY_PRSNL_REP.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)

# ----------------------------------------------------------------------------
# AfterJobRoutine (indicated by "AfterJobRoutine": "1")
# ----------------------------------------------------------------------------
params = {
    "EnvProjectPath": f"dap/<...>/PrvcyPrsnlRep",
    "File_Path": "load",
    "File_Name": "PRVCY_PRSNL_REP.dat"
}
dbutils.notebook.run("../../../../sequencer_routines/Move_File", timeout_seconds=3600, arguments=params)