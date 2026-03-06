# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 05/12/09 13:30:39 Batch  15108_48642 PROMOTE bckcetl ids20 dsadm bls for rt
# MAGIC ^1_1 05/12/09 13:07:49 Batch  15108_47273 INIT bckcett:31540 testIDS dsadm BLS FOR RT
# MAGIC ^1_1 04/28/09 15:30:57 Batch  15094_55861 PROMOTE bckcett testIDS u03651 steph for Ralph
# MAGIC ^1_1 04/28/09 15:26:41 Batch  15094_55604 INIT bckcett devlIDS u03651 steffy
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsUMActvtyFkey
# MAGIC 
# MAGIC DESCRIPTION:     Takes the sorted file and applies the primary key.   Preps the file for the foreign key lookup process that follows.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC       
# MAGIC Developer                          Date                    Project/Altiris #                     Change Description                                             Development Project               Code Reviewer                      Date Reviewed
# MAGIC ----------------------------------      -------------------      -----------------------------------           ---------------------------------------------------------                       ----------------------------------              ---------------------------------               -------------------------
# MAGIC Bhoomi Dasari                 03/06/2009              3808                                Originally Programmed                                                      devlIDS                         Steph Goddard                       03/30/2009

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
    StructType, StructField, IntegerType, StringType, TimestampType, DecimalType
)
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Retrieve job parameters
InFile = get_widget_value('InFile','IdsUmActvtyExtr.tmp')
Logging = get_widget_value('Logging','X')
OutFile = get_widget_value('OutFile','UM_ACTVTY.dat')
SrcSysCdSk = get_widget_value('SrcSysCdSk','105838')

# Define schema for IdsUMActvtyExtr (CSeqFileStage)
schema_IdsUMActvtyExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),    # char(10)
    StructField("DISCARD_IN", StringType(), False),       # char(1)
    StructField("PASS_THRU_IN", StringType(), False),     # char(1)
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(10, 0), False), # numeric
    StructField("SRC_SYS_CD", StringType(), False),       # varchar
    StructField("PRI_KEY_STRING", StringType(), False),   # varchar
    StructField("UM_ACTVTY_SK", IntegerType(), False),
    StructField("UM_REF_ID", StringType(), False),        # varchar
    StructField("UM_ACTVTY_SEQ_NO", IntegerType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("USUS_ID", StringType(), False),          # char(10)
    StructField("UMAC_USID_ROUTE", StringType(), False),  # char(10)
    StructField("UMAC_MCTR_REAS", StringType(), False),   # char(4)
    StructField("UMAC_MCTR_CPLX", StringType(), False),   # char(4)
    StructField("ACTVTY_DT_SK", StringType(), False),     # char(10)
    StructField("SVC_ROWS_ADD_NO", IntegerType(), False),
    StructField("IP_ROWS_ADD_NO", IntegerType(), False),
    StructField("RVW_ROWS_ADD_NO", IntegerType(), False),
    StructField("NOTE_ROWS_ADD_NO", IntegerType(), False)
])

# Read the input file (IdsUMActvtyExtr)
df_IdsUMActvtyExtr = (
    spark.read.format("csv")
    .option("header", False)
    .option("sep", ",")
    .option("quote", "\"")
    .schema(schema_IdsUMActvtyExtr)
    .load(f"{adls_path}/key/{InFile}")
)

# Augment columns for Transformer stage "ForeignKey"
df_ForeignKey = (
    df_IdsUMActvtyExtr
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("svActvtyUsrSk", GetFkeyAppUsr(F.col("SRC_SYS_CD"), F.col("UM_ACTVTY_SK"), F.col("USUS_ID"), Logging))
    .withColumn("svRteUsrSk", GetFkeyAppUsr(F.col("SRC_SYS_CD"), F.col("UM_ACTVTY_SK"), F.col("UMAC_USID_ROUTE"), Logging))
    .withColumn("svUmActvtyRsnCd", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("UM_ACTVTY_SK"), F.lit("UTILIZATION MANAGEMENT ACTIVITY REASON "), F.col("UMAC_MCTR_REAS"), Logging))
    .withColumn("svUmActvtyCmpCd", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("UM_ACTVTY_SK"), F.lit("UTILIZATION MANAGEMENT ACTIVITY COMPLEXITY LEVEL"), F.col("UMAC_MCTR_CPLX"), Logging))
    .withColumn("svActvtyDt", GetFkeyDate(F.lit("IDS"), F.col("UM_ACTVTY_SK"), F.col("ACTVTY_DT_SK"), Logging))
    .withColumn("svUm", GetFkeyUm(F.col("SRC_SYS_CD"), F.col("UM_ACTVTY_SK"), F.col("UM_REF_ID"), Logging))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("UM_ACTVTY_SK")))
    .withColumn("SrcSysCdSk", F.lit(SrcSysCdSk).cast(IntegerType()))
)

# Output link "Fkey" (Constraint: ErrCount = 0 Or PassThru = 'Y')
df_Fkey = (
    df_ForeignKey
    .filter((F.col("ErrCount") == 0) | (F.col("PassThru") == 'Y'))
    .select(
        F.col("UM_ACTVTY_SK").alias("UM_ACTVTY_SK"),
        F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
        F.col("UM_REF_ID").alias("UM_REF_ID"),
        F.col("UM_ACTVTY_SEQ_NO").alias("UM_ACTVTY_SEQ_NO"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("svActvtyUsrSk").alias("ACTVTY_USER_SK"),
        F.col("svRteUsrSk").alias("RTE_USER_SK"),
        F.col("svUm").alias("UM_SK"),
        F.col("svActvtyDt").alias("ACTVTY_DT_SK"),
        F.col("svUmActvtyCmpCd").alias("UM_ACTVTY_CMPLXTY_LVL_CD_SK"),
        F.col("svUmActvtyRsnCd").alias("UM_ACTVTY_RSN_CD_SK"),
        F.col("SVC_ROWS_ADD_NO").alias("SVC_ROWS_ADD_NO"),
        F.col("IP_ROWS_ADD_NO").alias("IP_ROWS_ADD_NO"),
        F.col("RVW_ROWS_ADD_NO").alias("RVW_ROWS_ADD_NO"),
        F.col("NOTE_ROWS_ADD_NO").alias("NOTE_ROWS_ADD_NO")
    )
)

# Output link "Recycle" (Constraint: ErrCount > 0)
df_Recycle = (
    df_ForeignKey
    .filter(F.col("ErrCount") > 0)
    .select(
        GetRecycleKey(F.col("UM_ACTVTY_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("DISCARD_IN").alias("DISCARD_IN"),
        F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("ERR_CT").alias("ERR_CT"),
        (F.col("RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("UM_ACTVTY_SK").alias("UM_ACTVTY_SK"),
        F.col("UM_REF_ID").alias("UM_REF_ID"),
        F.col("UM_ACTVTY_SEQ_NO").alias("UM_ACTVTY_SEQ_NO"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("USUS_ID").alias("USUS_ID"),
        F.col("UMAC_USID_ROUTE").alias("UMAC_USID_ROUTE"),
        F.col("UMAC_MCTR_REAS").alias("UMAC_MCTR_REAS"),
        F.col("UMAC_MCTR_CPLX").alias("UMAC_MCTR_CPLX"),
        F.col("ACTVTY_DT_SK").alias("ACTVTY_DT_SK"),
        F.col("SVC_ROWS_ADD_NO").alias("SVC_ROWS_ADD_NO"),
        F.col("IP_ROWS_ADD_NO").alias("IP_ROWS_ADD_NO"),
        F.col("RVW_ROWS_ADD_NO").alias("RVW_ROWS_ADD_NO"),
        F.col("NOTE_ROWS_ADD_NO").alias("NOTE_ROWS_ADD_NO")
    )
)

# Write hashed file "hf_recycle" -> scenario C => Write parquet
write_files(
    df_Recycle,
    f"{adls_path}/hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# Output link "DefaultUNK" (Constraint: @INROWNUM = 1)
df_DefaultUNK = (
    df_ForeignKey.limit(1)
    .select(
        F.lit(0).alias("UM_ACTVTY_SK"),
        F.lit(0).alias("SRC_SYS_CD_SK"),
        F.lit("UNK").alias("UM_REF_ID"),
        F.lit(0).alias("UM_ACTVTY_SEQ_NO"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("ACTVTY_USER_SK"),
        F.lit(0).alias("RTE_USER_SK"),
        F.lit(0).alias("UM_SK"),
        F.lit("1753-01-01").alias("ACTVTY_DT_SK"),  # char(10)
        F.lit(0).alias("UM_ACTVTY_CMPLXTY_LVL_CD_SK"),
        F.lit(0).alias("UM_ACTVTY_RSN_CD_SK"),
        F.lit(0).alias("SVC_ROWS_ADD_NO"),
        F.lit(0).alias("IP_ROWS_ADD_NO"),
        F.lit(0).alias("RVW_ROWS_ADD_NO"),
        F.lit(0).alias("NOTE_ROWS_ADD_NO")
    )
)

# Output link "DefaultNA" (Constraint: @INROWNUM = 1)
df_DefaultNA = (
    df_ForeignKey.limit(1)
    .select(
        F.lit(1).alias("UM_ACTVTY_SK"),
        F.lit(1).alias("SRC_SYS_CD_SK"),
        F.lit("NA").alias("UM_REF_ID"),
        F.lit(1).alias("UM_ACTVTY_SEQ_NO"),
        F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("ACTVTY_USER_SK"),
        F.lit(1).alias("RTE_USER_SK"),
        F.lit(1).alias("UM_SK"),
        F.lit("1753-01-01").alias("ACTVTY_DT_SK"),  # char(10)
        F.lit(1).alias("UM_ACTVTY_CMPLXTY_LVL_CD_SK"),
        F.lit(1).alias("UM_ACTVTY_RSN_CD_SK"),
        F.lit(0).alias("SVC_ROWS_ADD_NO"),
        F.lit(0).alias("IP_ROWS_ADD_NO"),
        F.lit(0).alias("RVW_ROWS_ADD_NO"),
        F.lit(0).alias("NOTE_ROWS_ADD_NO")
    )
)

# Collector Stage: union the data from Fkey, DefaultUNK, DefaultNA
df_Collector = df_Fkey.unionByName(df_DefaultUNK).unionByName(df_DefaultNA)

# Final select with column order for output and apply rpad for char/varchar columns as required
df_Final = df_Collector.select(
    F.col("UM_ACTVTY_SK").alias("UM_ACTVTY_SK"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    # UM_REF_ID is varchar, unknown length -> use <...> per instructions
    F.rpad(F.col("UM_REF_ID"), <...>, " ").alias("UM_REF_ID"),
    F.col("UM_ACTVTY_SEQ_NO").alias("UM_ACTVTY_SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("ACTVTY_USER_SK").alias("ACTVTY_USER_SK"),
    F.col("RTE_USER_SK").alias("RTE_USER_SK"),
    F.col("UM_SK").alias("UM_SK"),
    # ACTVTY_DT_SK is char(10) -> rpad length=10
    F.rpad(F.col("ACTVTY_DT_SK"), 10, " ").alias("ACTVTY_DT_SK"),
    F.col("UM_ACTVTY_CMPLXTY_LVL_CD_SK").alias("UM_ACTVTY_CMPLXTY_LVL_CD_SK"),
    F.col("UM_ACTVTY_RSN_CD_SK").alias("UM_ACTVTY_RSN_CD_SK"),
    F.col("SVC_ROWS_ADD_NO").alias("SVC_ROWS_ADD_NO"),
    F.col("IP_ROWS_ADD_NO").alias("IP_ROWS_ADD_NO"),
    F.col("RVW_ROWS_ADD_NO").alias("RVW_ROWS_ADD_NO"),
    F.col("NOTE_ROWS_ADD_NO").alias("NOTE_ROWS_ADD_NO")
)

# Write final output (UM_Actvty) to .dat
write_files(
    df_Final,
    f"{adls_path}/load/{OutFile}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)