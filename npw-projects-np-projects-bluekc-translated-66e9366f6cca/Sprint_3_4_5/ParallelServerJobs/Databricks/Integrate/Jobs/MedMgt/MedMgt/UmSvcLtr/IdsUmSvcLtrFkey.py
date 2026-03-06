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
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC Developer                          Date                    Project/Altiris #                     Change Description                                                       Development Project               Code Reviewer                      Date Reviewed
# MAGIC ----------------------------------      -------------------      -----------------------------------           ---------------------------------------------------------                                 ----------------------------------              ---------------------------------               -------------------------
# MAGIC Ralph Tucker                  03/18/2009      3808 - BICC                           Initial development                                                            devlIDS

# MAGIC Set all foreign surrogate keys
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
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
    DecimalType
)
from pyspark.sql import functions as F
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Retrieve job parameters
InFile = get_widget_value('InFile','IdsUmSvcLtrExtr.dat')
OutFile = get_widget_value('OutFile','UM_SVC_LTR.dat')
Logging = get_widget_value('Logging','$PROJDEF')
SrcSysCdSk = get_widget_value('SrcSysCdSk','105838')

# Define schema for the input file
schema_IdsUmIpLtrExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38,10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("UM_IP_LTR_SK", IntegerType(), nullable=False),
    StructField("UM_REF_ID", StringType(), nullable=False),
    StructField("UM_SVC_SEQ_NO", IntegerType(), nullable=False),
    StructField("LTR_SEQ_NO", IntegerType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("UM_SK", IntegerType(), nullable=False),
    StructField("UMUM_REF_ID", StringType(), nullable=False),
    StructField("UMSV_SEQ_NO", IntegerType(), nullable=False),
    StructField("ATXR_DEST_ID", TimestampType(), nullable=False),
    StructField("ATSY_ID_TRGT", StringType(), nullable=False),
    StructField("ATSY_ID", StringType(), nullable=False),
    StructField("ATXR_CREATE_USUS", StringType(), nullable=False),
    StructField("ATXR_LAST_UPD_USUS", StringType(), nullable=False),
    StructField("UM_ID", StringType(), nullable=False),
    StructField("ATLD_ID", StringType(), nullable=False),
    StructField("ATCHMT_SRC_DTM", TimestampType(), nullable=False),
    StructField("ATXR_CREATE_DT", StringType(), nullable=False),
    StructField("ATXR_LAST_UPD_DT", StringType(), nullable=False)
])

# Read input file (CSeqFileStage: IdsUmIpLtrExtr)
df_IdsUmIpLtrExtr = (
    spark.read.format("csv")
    .option("header", "false")
    .option("inferschema", "false")
    .option("sep", ",")
    .option("quote", "\"")
    .schema(schema_IdsUmIpLtrExtr)
    .load(f"{adls_path}/key/{InFile}")
)

# Apply Transformer logic (ForeignKey)
df_fk = (
    df_IdsUmIpLtrExtr
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("svDefaultDate", F.lit("1753-01-01-00.00.00.000000"))
    .withColumn("svUmSvcSk", GetFkeyUmSvc(F.col("SRC_SYS_CD"), F.col("UM_IP_LTR_SK"), F.col("UM_REF_ID"), F.col("UM_SVC_SEQ_NO"), Logging))
    .withColumn("svUmIpLtrStyleCdSk", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("UM_IP_LTR_SK"), F.lit("ATTACHMENT TYPE"), F.col("ATSY_ID"), Logging))
    .withColumn("svUmIpLtrTypCdSk", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("UM_IP_LTR_SK"), F.lit("CLAIM LETTER TYPE"), F.col("ATLD_ID"), Logging))
    .withColumn("svCrtByUser", GetFkeyAppUsr(F.col("SRC_SYS_CD"), F.col("UM_IP_LTR_SK"), F.col("ATXR_CREATE_USUS"), Logging))
    .withColumn("svLastUpdtUser", GetFkeyAppUsr(F.col("SRC_SYS_CD"), F.col("UM_IP_LTR_SK"), F.col("ATXR_LAST_UPD_USUS"), Logging))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("UM_SK")))
)

# Recycle link: constraint => ErrCount > 0
df_recycle = (
    df_fk
    .filter(F.col("ErrCount") > 0)
    .select(
        F.col("JOB_EXCTN_RCRD_ERR_SK"),
        F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
        F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
        F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT"),
        F.col("ErrCount").alias("ERR_CT"),
        F.col("RECYCLE_CT"),
        F.col("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING"),
        F.col("UM_IP_LTR_SK"),
        F.col("UM_REF_ID"),
        F.col("UM_SVC_SEQ_NO"),
        F.col("LTR_SEQ_NO"),
        F.col("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("svUmSvcSk").alias("UM_SVC_SK"),
        F.rpad(F.col("UMUM_REF_ID"), 9, " ").alias("UMUM_REF_ID"),
        F.col("UMSV_SEQ_NO"),
        F.col("ATXR_DEST_ID"),
        F.rpad(F.col("ATSY_ID_TRGT"), 4, " ").alias("ATSY_ID"),
        F.rpad(F.col("ATXR_CREATE_USUS"), 10, " ").alias("ATXR_CREATE_USUS"),
        F.rpad(F.col("ATXR_LAST_UPD_USUS"), 10, " ").alias("ATXR_LAST_UPD_USUS"),
        F.rpad(F.col("UM_ID"), 9, " ").alias("UM_ID"),
        F.rpad(F.col("ATLD_ID"), 8, " ").alias("ATLD_ID"),
        F.col("ATCHMT_SRC_DTM"),
        F.rpad(F.col("ATXR_CREATE_DT"), 10, " ").alias("ATXR_CREATE_DT"),
        F.rpad(F.col("ATXR_LAST_UPD_DT"), 10, " ").alias("ATXR_LAST_UPD_DT")
    )
)

# CHashedFileStage: hf_recycle --> Scenario C: write to parquet
write_files(
    df_recycle,
    "hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# DefaultUNK link: constraint => @INROWNUM = 1
df_DefaultUNK_temp = df_fk.withColumn("_row_num", F.row_number().over(Window.orderBy(F.lit(1)))).filter(F.col("_row_num") == 1)
df_DefaultUNK = (
    df_DefaultUNK_temp
    .select(
        F.lit(0).alias("UM_IP_LTR_SK"),
        F.lit(0).alias("SRC_SYS_CD_SK"),
        F.lit("UNK").alias("UM_REF_ID"),
        F.lit(0).alias("UM_SVC_SEQ_NO"),
        F.lit(0).alias("LTR_SEQ_NO"),
        F.lit("UNK").alias("UM_IP_LTR_STYLE_CD"),
        F.col("svDefaultDate").alias("LTR_DEST_ID"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("CRT_BY_USER_SK"),
        F.lit(0).alias("LAST_UPDT_USER_SK"),
        F.lit(0).alias("UM_SK"),
        F.lit(0).alias("UM_IP_LTR_TYP_CD_SK"),
        F.col("svDefaultDate").alias("ATCHMT_SRC_DTM"),
        F.rpad(F.lit("1753-01-01"), 10, " ").alias("CRT_DT_SK"),
        F.rpad(F.lit("1753-01-01"), 10, " ").alias("LAST_UPDT_DT_SK"),
        F.lit(0).alias("UM_SVC_LTR_STYLE_CD_SK")
    )
)

# DefaultNA link: constraint => @INROWNUM = 1
df_DefaultNA_temp = df_fk.withColumn("_row_num2", F.row_number().over(Window.orderBy(F.lit(1)))).filter(F.col("_row_num2") == 1)
df_DefaultNA = (
    df_DefaultNA_temp
    .select(
        F.lit(1).alias("UM_IP_LTR_SK"),
        F.lit(1).alias("SRC_SYS_CD_SK"),
        F.lit("NA").alias("UM_REF_ID"),
        F.lit(1).alias("UM_SVC_SEQ_NO"),
        F.lit(0).alias("LTR_SEQ_NO"),
        F.lit("NA").alias("UM_IP_LTR_STYLE_CD"),
        F.col("svDefaultDate").alias("LTR_DEST_ID"),
        F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("CRT_BY_USER_SK"),
        F.lit(1).alias("LAST_UPDT_USER_SK"),
        F.lit(1).alias("UM_SK"),
        F.lit(1).alias("UM_IP_LTR_TYP_CD_SK"),
        F.col("svDefaultDate").alias("ATCHMT_SRC_DTM"),
        F.rpad(F.lit("1753-01-01"), 10, " ").alias("CRT_DT_SK"),
        F.rpad(F.lit("1753-01-01"), 10, " ").alias("LAST_UPDT_DT_SK"),
        F.lit(1).alias("UM_SVC_LTR_STYLE_CD_SK")
    )
)

# Load link: no constraint
df_Load = (
    df_fk
    .select(
        F.col("UM_IP_LTR_SK"),
        F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        F.col("UM_REF_ID"),
        F.col("UM_SVC_SEQ_NO"),
        F.col("LTR_SEQ_NO"),
        F.col("ATSY_ID_TRGT").alias("UM_IP_LTR_STYLE_CD"),
        F.col("ATXR_DEST_ID").alias("LTR_DEST_ID"),
        F.col("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("svCrtByUser").alias("CRT_BY_USER_SK"),
        F.col("svLastUpdtUser").alias("LAST_UPDT_USER_SK"),
        F.col("svUmSvcSk").alias("UM_SK"),
        F.col("svUmIpLtrTypCdSk").alias("UM_IP_LTR_TYP_CD_SK"),
        FORMAT_DATE(
            F.col("ATCHMT_SRC_DTM"),
            F.lit("SYBASE"),
            F.lit("TIMESTAMP"),
            F.lit("DB2TIMESTAMP")
        ).alias("ATCHMT_SRC_DTM"),
        F.rpad(F.col("ATXR_CREATE_DT"), 10, " ").alias("CRT_DT_SK"),
        F.rpad(F.col("ATXR_LAST_UPD_DT"), 10, " ").alias("LAST_UPDT_DT_SK"),
        F.col("svUmIpLtrStyleCdSk").alias("UM_SVC_LTR_STYLE_CD_SK")
    )
)

# Collector (Round-Robin) => union all three dataframes
df_Collector = df_DefaultUNK.unionByName(df_DefaultNA).unionByName(df_Load)

# Final select in the order specified by the Collector -> LoadFile
df_Collector_final = df_Collector.select([
    "UM_IP_LTR_SK",
    "SRC_SYS_CD_SK",
    "UM_REF_ID",
    "UM_SVC_SEQ_NO",
    "LTR_SEQ_NO",
    "UM_IP_LTR_STYLE_CD",
    "LTR_DEST_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CRT_BY_USER_SK",
    "LAST_UPDT_USER_SK",
    "UM_SK",
    "UM_IP_LTR_TYP_CD_SK",
    "ATCHMT_SRC_DTM",
    "CRT_DT_SK",
    "LAST_UPDT_DT_SK",
    "UM_SVC_LTR_STYLE_CD_SK"
])

# Write final output file (CSeqFileStage: UM_IP_LTR)
write_files(
    df_Collector_final,
    f"{adls_path}/load/{OutFile}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)