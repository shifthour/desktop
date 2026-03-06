# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC Â© Copyright 2020 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by:   IdsFctsClmLoad3Seq
# MAGIC 
# MAGIC 
# MAGIC Control Job Rerun Information: 
# MAGIC                     Previous Run Successful:    Restart, no other steps necessary
# MAGIC                     Previous Run Aborted:         Restart, no other steps necessary
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                                  Project/                                                                                                 Code                  Date
# MAGIC Developer           Date              Altiris #           Change Description                                                        Reviewer            Reviewed
# MAGIC -------------------------  -------------------   -------------------   ------------------------------------------------------------------------------------   ----------------------     -------------------   
# MAGIC SAndrew             08/2004                              Originally Programmed
# MAGIC Brent Leland       09/09/2004                         Fixed default values for UNK and NA
# MAGIC Steph Goddard   02/15/2006                         Changes for sequencer
# MAGIC Hugh Sisson       07/22/2008   3567              Added SrcSysCdSk to parameter list  
# MAGIC 
# MAGIC Reddy Sanam    10/09/2020                        Added stage variable "svSrcSysCd"
# MAGIC                                                                       to pass 'FACETS' for LUMERIS' source
# MAGIC                                                                       for type codes
# MAGIC 
# MAGIC Sunitha Ganta         10-12-2020                   Brought up to standards

# MAGIC Read common record format file.
# MAGIC Writing Sequential File to /load
# MAGIC This hash file is written to by all claim foriegn key jobs.
# MAGIC Assign foreign keys and create default rows for unknown and not applicable.
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


Logging = get_widget_value("Logging","Y")
Source = get_widget_value("Source","FACETS")
SrcSysCdSk = get_widget_value("SrcSysCdSk","")
InFile = get_widget_value("InFile","FctsClmLetterExtr.FctsClmLetter.dat.")

schema_ClmLetterCRF = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38,10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("CLM_LTR_SK", IntegerType(), nullable=False),
    StructField("CLM_ID", StringType(), nullable=False),
    StructField("CLM_LTR_SEQ_NO", IntegerType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("CLM_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_USER_ID", StringType(), nullable=False),
    StructField("CLM_LTR_REPRT_STTUS_CD", StringType(), nullable=False),
    StructField("CLM_LTR_TYP_CD", StringType(), nullable=False),
    StructField("FLW_UP_IN", StringType(), nullable=False),
    StructField("MAIL_IN", StringType(), nullable=False),
    StructField("PRT_IN", StringType(), nullable=False),
    StructField("RCV_IN", StringType(), nullable=False),
    StructField("RQST_IN", StringType(), nullable=False),
    StructField("SUBMT_IN", StringType(), nullable=False),
    StructField("LAST_UPDT_DT", StringType(), nullable=False),
    StructField("MAIL_DT", StringType(), nullable=False),
    StructField("PRT_DT", StringType(), nullable=False),
    StructField("RCV_DT", StringType(), nullable=False),
    StructField("RQST_DT", StringType(), nullable=False),
    StructField("SUBMT_DT", StringType(), nullable=False),
    StructField("FROM_NM", StringType(), nullable=False),
    StructField("LTR_DESC", StringType(), nullable=False),
    StructField("REF_NM", StringType(), nullable=False),
    StructField("SUBJ_TX", StringType(), nullable=False),
    StructField("TO_NM", StringType(), nullable=False)
])

df_ClmLetterCRF = (
    spark.read
    .format("csv")
    .option("header", False)
    .option("delimiter", ",")
    .option("quote", "\"")
    .schema(schema_ClmLetterCRF)
    .load(f"{adls_path}/key/{InFile}")
)

df_ForeignKey = (
    df_ClmLetterCRF
    .withColumn("svSrcSysCd", F.when(F.col("SRC_SYS_CD")=="LUMERIS","FACETS").otherwise(F.col("SRC_SYS_CD")))
    .withColumn("ClmSk", GetFkeyClm(F.col("SRC_SYS_CD"), F.col("CLM_LTR_SK"), F.col("CLM_ID"), F.col("Logging")))
    .withColumn("LastUpdtUserIdSk", GetFkeyAppUsr(F.col("SRC_SYS_CD"), F.col("CLM_LTR_SK"), trim("LAST_UPDT_USER_ID"), F.col("Logging")))
    .withColumn("ClmLetReprtStatusCdSk", GetFkeyCodes(F.col("svSrcSysCd"), F.col("CLM_LTR_SK"), F.lit("CLAIM LETTER REPRINT STATUS"), F.col("CLM_LTR_REPRT_STTUS_CD"), F.col("Logging")))
    .withColumn("ClmLetTypeCdSk", GetFkeyCodes(F.col("svSrcSysCd"), F.col("CLM_LTR_SK"), F.lit("CLAIM LETTER TYPE"), F.col("CLM_LTR_TYP_CD"), F.col("Logging")))
    .withColumn("LastUpdtDtSk", GetFkeyDate(F.lit("IDS"), F.col("CLM_LTR_SK"), F.col("LAST_UPDT_DT"), F.col("Logging")))
    .withColumn("MailDtSk", GetFkeyDate(F.lit("IDS"), F.col("CLM_LTR_SK"), F.col("MAIL_DT"), F.col("Logging")))
    .withColumn("PrtDtSk", GetFkeyDate(F.lit("IDS"), F.col("CLM_LTR_SK"), F.col("PRT_DT"), F.col("Logging")))
    .withColumn("RvcDtSk", GetFkeyDate(F.lit("IDS"), F.col("CLM_LTR_SK"), F.col("RCV_DT"), F.col("Logging")))
    .withColumn("RqstDtSk", GetFkeyDate(F.lit("IDS"), F.col("CLM_LTR_SK"), F.col("RQST_DT"), F.col("Logging")))
    .withColumn("SubmtDtSk", GetFkeyDate(F.lit("IDS"), F.col("CLM_LTR_SK"), F.col("SUBMT_DT"), F.col("Logging")))
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("CLM_LTR_SK")))
)

df_fkey = (
    df_ForeignKey
    .filter((F.col("ErrCount") == 0) | (F.col("PassThru") == "Y"))
    .select(
        F.col("CLM_LTR_SK").alias("CLM_LTR_SK"),
        F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        F.col("CLM_ID").alias("CLM_ID"),
        F.col("CLM_LTR_SEQ_NO").alias("CLM_LTR_SEQ_NO"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("ClmSk").alias("CLM_SK"),
        F.col("LastUpdtUserIdSk").alias("LAST_UPDT_USER_ID_SK"),
        F.col("ClmLetReprtStatusCdSk").alias("CLM_LTR_REPRT_STTUS_CD_SK"),
        F.col("ClmLetTypeCdSk").alias("CLM_LTR_TYP_CD_SK"),
        F.col("FLW_UP_IN").alias("FLW_UP_IN"),
        F.col("MAIL_IN").alias("MAIL_IN"),
        F.col("PRT_IN").alias("PRT_IN"),
        F.col("RCV_IN").alias("RCV_IN"),
        F.col("RQST_IN").alias("RQST_IN"),
        F.col("SUBMT_IN").alias("SUBMT_IN"),
        F.col("LastUpdtDtSk").alias("LAST_UPDT_DT_SK"),
        F.col("MailDtSk").alias("MAIL_DT_SK"),
        F.col("PrtDtSk").alias("PRT_DT_SK"),
        F.col("RvcDtSk").alias("RCV_DT_SK"),
        F.col("RqstDtSk").alias("RQST_DT_SK"),
        F.col("SubmtDtSk").alias("SUBMT_DT_SK"),
        F.col("FROM_NM").alias("FROM_NM"),
        F.col("LTR_DESC").alias("LTR_DESC"),
        F.col("REF_NM").alias("REF_NM"),
        F.col("SUBJ_TX").alias("SUBJ_TX"),
        F.col("TO_NM").alias("TO_NM")
    )
)

df_recycle = (
    df_ForeignKey
    .filter(F.col("ErrCount") > 0)
    .select(
        F.expr("GetRecycleKey(CLM_LTR_SK)").alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("DISCARD_IN").alias("DISCARD_IN"),
        F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("ErrCount").alias("ERR_CT"),
        (F.col("RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("CLM_LTR_SK").alias("CLM_LTR_SK"),
        F.col("CLM_ID").alias("CLM_ID"),
        F.col("CLM_LTR_SEQ_NO").alias("CLM_LTR_SEQ_NO"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("CLM_SK").alias("CLM_SK"),
        F.col("LAST_UPDT_USER_ID").alias("LAST_UPDT_USER_ID"),
        F.col("CLM_LTR_REPRT_STTUS_CD").alias("CLM_LTR_REPRT_STTUS_CD"),
        F.col("CLM_LTR_TYP_CD").alias("CLM_LTR_TYP_CD"),
        F.col("FLW_UP_IN").alias("FLW_UP_IN"),
        F.col("MAIL_IN").alias("MAIL_IN"),
        F.col("PRT_IN").alias("PRT_IN"),
        F.col("RCV_IN").alias("RCV_IN"),
        F.col("RQST_IN").alias("RQST_IN"),
        F.col("SUBMT_IN").alias("SUBMT_IN"),
        F.col("LAST_UPDT_DT").alias("LAST_UPDT_DT"),
        F.col("MAIL_DT").alias("MAIL_DT"),
        F.col("PRT_DT").alias("PRT_DT"),
        F.col("RCV_DT").alias("RCV_DT"),
        F.col("RQST_DT").alias("RQST_DT"),
        F.col("SUBMT_DT").alias("SUBMT_DT"),
        F.col("FROM_NM").alias("FROM_NM"),
        F.col("LTR_DESC").alias("LTR_DESC"),
        F.col("REF_NM").alias("REF_NM"),
        F.col("SUBJ_TX").alias("SUBJ_TX"),
        F.col("TO_NM").alias("TO_NM")
    )
)

df_recycle_clms = (
    df_ForeignKey
    .filter(F.col("ErrCount") > 0)
    .select(
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("CLM_ID").alias("CLM_ID")
    )
)

write_files(
    df_recycle.select(
        "JOB_EXCTN_RCRD_ERR_SK","INSRT_UPDT_CD","DISCARD_IN","PASS_THRU_IN","FIRST_RECYC_DT","ERR_CT","RECYCLE_CT","SRC_SYS_CD","PRI_KEY_STRING","CLM_LTR_SK","CLM_ID","CLM_LTR_SEQ_NO","CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK","CLM_SK","LAST_UPDT_USER_ID","CLM_LTR_REPRT_STTUS_CD","CLM_LTR_TYP_CD","FLW_UP_IN","MAIL_IN","PRT_IN","RCV_IN","RQST_IN","SUBMT_IN","LAST_UPDT_DT","MAIL_DT","PRT_DT","RCV_DT","RQST_DT","SUBMT_DT","FROM_NM","LTR_DESC","REF_NM","SUBJ_TX","TO_NM"
    ),
    f"{adls_path}/hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

write_files(
    df_recycle_clms.select("SRC_SYS_CD","CLM_ID"),
    f"{adls_path}/hf_claim_recycle_keys.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_defaultUNK = spark.createDataFrame(
    [
        (
            0,0,"UNK",0,0,0,0,0,0,0,"U","U","U","U","U","U","NA","NA","NA","NA","NA","NA","UNK","UNK","UNK","UNK","UNK"
        )
    ],
    [
        "CLM_LTR_SK","SRC_SYS_CD_SK","CLM_ID","CLM_LTR_SEQ_NO","CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CLM_SK","LAST_UPDT_USER_ID_SK","CLM_LTR_REPRT_STTUS_CD_SK","CLM_LTR_TYP_CD_SK","FLW_UP_IN","MAIL_IN","PRT_IN",
        "RCV_IN","RQST_IN","SUBMT_IN","LAST_UPDT_DT_SK","MAIL_DT_SK","PRT_DT_SK","RCV_DT_SK","RQST_DT_SK","SUBMT_DT_SK",
        "FROM_NM","LTR_DESC","REF_NM","SUBJ_TX","TO_NM"
    ]
)

df_defaultNA = spark.createDataFrame(
    [
        (
            1,1,"NA",0,1,1,1,1,1,1,"X","X","X","X","X","X","NA","NA","NA","NA","NA","NA","NA","NA","NA","NA","NA"
        )
    ],
    [
        "CLM_LTR_SK","SRC_SYS_CD_SK","CLM_ID","CLM_LTR_SEQ_NO","CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CLM_SK","LAST_UPDT_USER_ID_SK","CLM_LTR_REPRT_STTUS_CD_SK","CLM_LTR_TYP_CD_SK","FLW_UP_IN","MAIL_IN","PRT_IN",
        "RCV_IN","RQST_IN","SUBMT_IN","LAST_UPDT_DT_SK","MAIL_DT_SK","PRT_DT_SK","RCV_DT_SK","RQST_DT_SK","SUBMT_DT_SK",
        "FROM_NM","LTR_DESC","REF_NM","SUBJ_TX","TO_NM"
    ]
)

df_collector = (
    df_fkey
    .unionByName(df_defaultUNK)
    .unionByName(df_defaultNA)
)

df_final = df_collector.select(
    F.col("CLM_LTR_SK"),
    F.col("SRC_SYS_CD_SK"),
    F.col("CLM_ID"),
    F.col("CLM_LTR_SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_SK"),
    F.col("LAST_UPDT_USER_ID_SK"),
    F.col("CLM_LTR_REPRT_STTUS_CD_SK"),
    F.col("CLM_LTR_TYP_CD_SK"),
    F.rpad(F.col("FLW_UP_IN"), 1, " ").alias("FLW_UP_IN"),
    F.rpad(F.col("MAIL_IN"), 1, " ").alias("MAIL_IN"),
    F.rpad(F.col("PRT_IN"), 1, " ").alias("PRT_IN"),
    F.rpad(F.col("RCV_IN"), 1, " ").alias("RCV_IN"),
    F.rpad(F.col("RQST_IN"), 1, " ").alias("RQST_IN"),
    F.rpad(F.col("SUBMT_IN"), 1, " ").alias("SUBMT_IN"),
    F.rpad(F.col("LAST_UPDT_DT_SK"), 10, " ").alias("LAST_UPDT_DT_SK"),
    F.rpad(F.col("MAIL_DT_SK"), 10, " ").alias("MAIL_DT_SK"),
    F.rpad(F.col("PRT_DT_SK"), 10, " ").alias("PRT_DT_SK"),
    F.rpad(F.col("RCV_DT_SK"), 10, " ").alias("RCV_DT_SK"),
    F.rpad(F.col("RQST_DT_SK"), 10, " ").alias("RQST_DT_SK"),
    F.rpad(F.col("SUBMT_DT_SK"), 10, " ").alias("SUBMT_DT_SK"),
    F.col("FROM_NM"),
    F.col("LTR_DESC"),
    F.col("REF_NM"),
    F.col("SUBJ_TX"),
    F.col("TO_NM")
)

write_files(
    df_final,
    f"{adls_path}/load/CLM_LTR.{Source}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)