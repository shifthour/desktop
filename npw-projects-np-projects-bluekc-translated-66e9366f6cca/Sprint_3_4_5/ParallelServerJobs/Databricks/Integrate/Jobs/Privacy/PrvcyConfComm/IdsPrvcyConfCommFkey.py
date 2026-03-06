# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
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
# MAGIC Naren Garapaty               2/25/2007           FHP/3028                      Originally Programmed                              devlIDS30                  Steph Goddard            
# MAGIC              
# MAGIC Manasa Andru                 6/26/2013           TTR - 778                  Removed RunID from parameters as         IntegrateCurDevl           Kalyan Neelam          2013-07-02
# MAGIC                                                                                                      it is not being used anywhere in the job

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
    IntegerType,
    StringType,
    TimestampType,
    DecimalType
)
from pyspark.sql import functions as F
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Parameter retrieval
InFile = get_widget_value('InFile','FctsPrvcyConfCommExtr.PrvcyConfComm.dat')
Logging = get_widget_value('Logging','Y')

# Define schema for IdsPrvcyConfCommExtr (CSeqFileStage)
schema_IdsPrvcyConfCommExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38,10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("PRVCY_CONF_COMM_SK", IntegerType(), nullable=False),
    StructField("PRVCY_MBR_UNIQ_KEY", IntegerType(), nullable=False),
    StructField("SEQ_NO", IntegerType(), nullable=False),
    StructField("PRVCY_MBR_SRC_CD_SK", IntegerType(), nullable=False),
    StructField("PRVCY_CONF_COMM_TYP_CD_SK", IntegerType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("MBR_SK", IntegerType(), nullable=False),
    StructField("PRVCY_EXTRNL_MBR_SK", IntegerType(), nullable=False),
    StructField("PRVCY_CONF_COMM_RQST_RSN_CD_SK", IntegerType(), nullable=False),
    StructField("PRVCY_CONF_COMM_STTUS_CD_SK", IntegerType(), nullable=False),
    StructField("EFF_DT_SK", StringType(), nullable=False),
    StructField("RCVD_DT_SK", StringType(), nullable=False),
    StructField("TERM_DT_SK", StringType(), nullable=False),
    StructField("ADDR_SEQ_NO", IntegerType(), nullable=False),
    StructField("ADDREE_FIRST_NM", StringType(), nullable=False),
    StructField("ADDREE_LAST_NM", StringType(), nullable=False),
    StructField("CONF_COMM_DESC", StringType(), nullable=False),
    StructField("EMAIL_ADDR_TX", StringType(), nullable=False)
])

# Read the source file (IdsPrvcyConfCommExtr - CSeqFileStage)
df_IdsPrvcyConfCommExtr = (
    spark.read.format("csv")
    .schema(schema_IdsPrvcyConfCommExtr)
    .option("sep", ",")
    .option("quote", "\"")
    .option("header", "false")
    .load(f"{adls_path}/key/{InFile}")
)

# Apply Transformer logic (ForeignKey - CTransformerStage) with stage variables
df_transformed = (
    df_IdsPrvcyConfCommExtr
    .withColumn(
        "SrcSysCdSk",
        GetFkeyCodes(
            trim(F.lit("IDS")),
            F.col("PRVCY_CONF_COMM_SK"),
            trim(F.lit("SOURCE SYSTEM")),
            trim(F.col("SRC_SYS_CD")),
            Logging
        )
    )
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn(
        "svPrvcyMbrSrcCdSk",
        GetFkeyCodes(
            F.col("SRC_SYS_CD"),
            F.col("PRVCY_CONF_COMM_SK"),
            F.lit("PRIVACY MEMBER SOURCE"),
            F.col("PRVCY_MBR_SRC_CD_SK"),
            Logging
        )
    )
    .withColumn(
        "svPrvcyConfCommTypCdSk",
        GetFkeyCodes(
            F.col("SRC_SYS_CD"),
            F.col("PRVCY_CONF_COMM_SK"),
            F.lit("CONFIDENTIAL COMMUNICATION TYPE"),
            F.col("PRVCY_CONF_COMM_TYP_CD_SK"),
            Logging
        )
    )
    .withColumn(
        "svMbrSk",
        GetFkeyMbr(
            F.col("SRC_SYS_CD"),
            F.col("PRVCY_CONF_COMM_SK"),
            F.col("MBR_SK"),
            Logging
        )
    )
    .withColumn(
        "svPrvcyExtrnlMbrSk",
        GetFkeyPrvcyExtrnlMbr(
            F.col("SRC_SYS_CD"),
            F.col("PRVCY_CONF_COMM_SK"),
            F.col("PRVCY_EXTRNL_MBR_SK"),
            Logging
        )
    )
    .withColumn(
        "svPrvcyConfCommRqstRsnCdSk",
        GetFkeyCodes(
            F.col("SRC_SYS_CD"),
            F.col("PRVCY_CONF_COMM_SK"),
            F.lit("CONFIDENTIAL COMMUNICATION REQUEST REASON"),
            F.col("PRVCY_CONF_COMM_RQST_RSN_CD_SK"),
            Logging
        )
    )
    .withColumn(
        "svPrvcyConfCommSttusCdSk",
        GetFkeyCodes(
            F.col("SRC_SYS_CD"),
            F.col("PRVCY_CONF_COMM_SK"),
            F.lit("CONFIDENTIAL COMMUNICATION STATUS"),
            F.col("PRVCY_CONF_COMM_STTUS_CD_SK"),
            Logging
        )
    )
    .withColumn(
        "svEffDtSk",
        GetFkeyDate(
            F.lit("IDS"),
            F.col("PRVCY_CONF_COMM_SK"),
            F.col("EFF_DT_SK"),
            Logging
        )
    )
    .withColumn(
        "svRcvdDtSk",
        GetFkeyDate(
            F.lit("IDS"),
            F.col("PRVCY_CONF_COMM_SK"),
            F.col("RCVD_DT_SK"),
            Logging
        )
    )
    .withColumn(
        "svTermDtSk",
        GetFkeyDate(
            F.lit("IDS"),
            F.col("PRVCY_CONF_COMM_SK"),
            F.col("TERM_DT_SK"),
            Logging
        )
    )
    .withColumn(
        "ErrCount",
        GetFkeyErrorCnt(
            F.col("PRVCY_CONF_COMM_SK")
        )
    )
)

# Create a row-number column for handling @INROWNUM = 1 constraints
df_rownum = df_transformed.withColumn("rn", F.row_number().over(Window.orderBy(F.lit(1))))

# Split flows based on constraints

# Fkey output link - Constraint: ErrCount = 0 Or PassThru = 'Y'
df_fkey = (
    df_rownum.filter((F.col("ErrCount") == 0) | (F.col("PassThru") == 'Y'))
    .select(
        F.col("PRVCY_CONF_COMM_SK").alias("PRVCY_CONF_COMM_SK"),
        F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
        F.col("PRVCY_MBR_UNIQ_KEY").alias("PRVCY_MBR_UNIQ_KEY"),
        F.col("SEQ_NO").alias("SEQ_NO"),
        F.col("svPrvcyMbrSrcCdSk").alias("PRVCY_MBR_SRC_CD_SK"),
        F.col("svPrvcyConfCommTypCdSk").alias("PRVCY_CONF_COMM_TYP_CD_SK"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("svMbrSk").alias("MBR_SK"),
        F.col("svPrvcyExtrnlMbrSk").alias("PRVCY_EXTRNL_MBR_SK"),
        F.col("svPrvcyConfCommRqstRsnCdSk").alias("PRVCY_CONF_COMM_RQST_RSN_CD_SK"),
        F.col("svPrvcyConfCommSttusCdSk").alias("PRVCY_CONF_COMM_STTUS_CD_SK"),
        F.col("svEffDtSk").alias("EFF_DT_SK"),
        F.col("svRcvdDtSk").alias("RCVD_DT_SK"),
        F.col("svTermDtSk").alias("TERM_DT_SK"),
        F.col("ADDR_SEQ_NO").alias("ADDR_SEQ_NO"),
        F.col("ADDREE_FIRST_NM").alias("ADDREE_FIRST_NM"),
        F.col("ADDREE_LAST_NM").alias("ADDREE_LAST_NM"),
        F.col("CONF_COMM_DESC").alias("CONF_COMM_DESC"),
        F.col("EMAIL_ADDR_TX").alias("EMAIL_ADDR_TX")
    )
)

# Recycle output link - Constraint: ErrCount > 0
df_recycle = (
    df_rownum.filter(F.col("ErrCount") > 0)
    .select(
        GetRecycleKey(F.col("PRVCY_CONF_COMM_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("DISCARD_IN").alias("DISCARD_IN"),
        F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("ErrCount").alias("ERR_CT"),
        (F.col("RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("PRVCY_CONF_COMM_SK").alias("PRVCY_CONF_COMM_SK"),
        F.col("PRVCY_MBR_UNIQ_KEY").alias("PRVCY_MBR_UNIQ_KEY"),
        F.col("SEQ_NO").alias("SEQ_NO"),
        F.col("PRVCY_MBR_SRC_CD_SK").alias("PRVCY_MBR_SRC_CD_SK"),
        F.col("PRVCY_CONF_COMM_TYP_CD_SK").alias("PRVCY_CONF_COMM_TYP_CD_SK"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("MBR_SK").alias("MBR_SK"),
        F.col("PRVCY_EXTRNL_MBR_SK").alias("PRVCY_EXTRNL_MBR_SK"),
        F.col("PRVCY_CONF_COMM_RQST_RSN_CD_SK").alias("PRVCY_CONF_COMM_RQST_RSN_CD_SK"),
        F.col("PRVCY_CONF_COMM_STTUS_CD_SK").alias("PRVCY_CONF_COMM_STTUS_CD_SK"),
        F.col("EFF_DT_SK").alias("EFF_DT_SK"),
        F.col("RCVD_DT_SK").alias("RCVD_DT_SK"),
        F.col("TERM_DT_SK").alias("TERM_DT_SK"),
        F.col("ADDR_SEQ_NO").alias("ADDR_SEQ_NO"),
        F.col("ADDREE_FIRST_NM").alias("ADDREE_FIRST_NM"),
        F.col("ADDREE_LAST_NM").alias("ADDREE_LAST_NM"),
        F.col("CONF_COMM_DESC").alias("CONF_COMM_DESC"),
        F.col("EMAIL_ADDR_TX").alias("EMAIL_ADDR_TX")
    )
)

# DefaultUNK link - Constraint: @INROWNUM = 1
df_defaultUNK = (
    df_rownum.filter(F.col("rn") == 1)
    .select(
        F.lit(0).alias("PRVCY_CONF_COMM_SK"),
        F.lit(0).alias("SRC_SYS_CD_SK"),
        F.lit(0).alias("PRVCY_MBR_UNIQ_KEY"),
        F.lit(0).alias("SEQ_NO"),
        F.lit(0).alias("PRVCY_MBR_SRC_CD_SK"),
        F.lit(0).alias("PRVCY_CONF_COMM_TYP_CD_SK"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("MBR_SK"),
        F.lit(0).alias("PRVCY_EXTRNL_MBR_SK"),
        F.lit(0).alias("PRVCY_CONF_COMM_RQST_RSN_CD_SK"),
        F.lit(0).alias("PRVCY_CONF_COMM_STTUS_CD_SK"),
        F.lit("UNK").alias("EFF_DT_SK"),
        F.lit("UNK").alias("RCVD_DT_SK"),
        F.lit("UNK").alias("TERM_DT_SK"),
        F.lit(0).alias("ADDR_SEQ_NO"),
        F.lit("UNK").alias("ADDREE_FIRST_NM"),
        F.lit("UNK").alias("ADDREE_LAST_NM"),
        F.lit("UNK").alias("CONF_COMM_DESC"),
        F.lit("UNK").alias("EMAIL_ADDR_TX")
    )
)

# DefaultNA link - Constraint: @INROWNUM = 1
df_defaultNA = (
    df_rownum.filter(F.col("rn") == 1)
    .select(
        F.lit(1).alias("PRVCY_CONF_COMM_SK"),
        F.lit(1).alias("SRC_SYS_CD_SK"),
        F.lit(1).alias("PRVCY_MBR_UNIQ_KEY"),
        F.lit(1).alias("SEQ_NO"),
        F.lit(1).alias("PRVCY_MBR_SRC_CD_SK"),
        F.lit(1).alias("PRVCY_CONF_COMM_TYP_CD_SK"),
        F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("MBR_SK"),
        F.lit(1).alias("PRVCY_EXTRNL_MBR_SK"),
        F.lit(1).alias("PRVCY_CONF_COMM_RQST_RSN_CD_SK"),
        F.lit(1).alias("PRVCY_CONF_COMM_STTUS_CD_SK"),
        F.lit("NA").alias("EFF_DT_SK"),
        F.lit("NA").alias("RCVD_DT_SK"),
        F.lit("NA").alias("TERM_DT_SK"),
        F.lit(1).alias("ADDR_SEQ_NO"),
        F.lit("NA").alias("ADDREE_FIRST_NM"),
        F.lit("NA").alias("ADDREE_LAST_NM"),
        F.lit("NA").alias("CONF_COMM_DESC"),
        F.lit("NA").alias("EMAIL_ADDR_TX")
    )
)

# Collector (CCollector) - union of Fkey, DefaultUNK, DefaultNA
df_collector = df_fkey.unionByName(df_defaultUNK).unionByName(df_defaultNA)

# Prepare final collector DataFrame with rpad for char columns
df_collector_final = (
    df_collector
    .withColumn("EFF_DT_SK", F.rpad(F.col("EFF_DT_SK"), 10, " "))
    .withColumn("RCVD_DT_SK", F.rpad(F.col("RCVD_DT_SK"), 10, " "))
    .withColumn("TERM_DT_SK", F.rpad(F.col("TERM_DT_SK"), 10, " "))
)

# Write the final collector output to PRVCY_CONF_COMM.dat (CSeqFileStage)
write_files(
    df_collector_final.select(
        "PRVCY_CONF_COMM_SK",
        "SRC_SYS_CD_SK",
        "PRVCY_MBR_UNIQ_KEY",
        "SEQ_NO",
        "PRVCY_MBR_SRC_CD_SK",
        "PRVCY_CONF_COMM_TYP_CD_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "MBR_SK",
        "PRVCY_EXTRNL_MBR_SK",
        "PRVCY_CONF_COMM_RQST_RSN_CD_SK",
        "PRVCY_CONF_COMM_STTUS_CD_SK",
        "EFF_DT_SK",
        "RCVD_DT_SK",
        "TERM_DT_SK",
        "ADDR_SEQ_NO",
        "ADDREE_FIRST_NM",
        "ADDREE_LAST_NM",
        "CONF_COMM_DESC",
        "EMAIL_ADDR_TX"
    ),
    f"{adls_path}/load/PRVCY_CONF_COMM.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# hf_recycle (CHashedFileStage) -> Scenario C: write to Parquet
df_recycle_final = (
    df_recycle
    .withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " "))
    .withColumn("EFF_DT_SK", F.rpad(F.col("EFF_DT_SK"), 10, " "))
    .withColumn("RCVD_DT_SK", F.rpad(F.col("RCVD_DT_SK"), 10, " "))
    .withColumn("TERM_DT_SK", F.rpad(F.col("TERM_DT_SK"), 10, " "))
)

df_recycle_final_select = df_recycle_final.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "PRVCY_CONF_COMM_SK",
    "PRVCY_MBR_UNIQ_KEY",
    "SEQ_NO",
    "PRVCY_MBR_SRC_CD_SK",
    "PRVCY_CONF_COMM_TYP_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "MBR_SK",
    "PRVCY_EXTRNL_MBR_SK",
    "PRVCY_CONF_COMM_RQST_RSN_CD_SK",
    "PRVCY_CONF_COMM_STTUS_CD_SK",
    "EFF_DT_SK",
    "RCVD_DT_SK",
    "TERM_DT_SK",
    "ADDR_SEQ_NO",
    "ADDREE_FIRST_NM",
    "ADDREE_LAST_NM",
    "CONF_COMM_DESC",
    "EMAIL_ADDR_TX"
)

write_files(
    df_recycle_final_select,
    f"{adls_path}/hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)