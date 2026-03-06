# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2020 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:     IdsFctsClmLoad3Seq
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:  assign foreign (surrogate) keys to record
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer           Date                        Project/Altius #     Change Description                                                     Development Project          Code Reviewer          Date Reviewed  
# MAGIC -----------------------    ----------------------------    ---------------------------  ----------------------------------------------------------------------------        ------------------------------------       ----------------------------      -------------------------
# MAGIC SAndrew             08/2004                                               Originally Programmed
# MAGIC Brent Leland       09/09/2004                                          Fixed default values for UNK and NA
# MAGIC Steph Goddard   02/16/2006                                          Changes for sequencer
# MAGIC Brent Leland       08/03/2006                                          Corrected last update run cycle column, was 
# MAGIC                                                                                         assigned create run cycle.
# MAGIC                                                                                          Assigned last update run cycle from input to NA 
# MAGIC                                                                                          and UNK rows so delete process would not 
# MAGIC                                                                                          remove them.
# MAGIC Ralph Tucker      2008-7-25              3657 Primary Key   Added SrcSysCdSk parameter                                     devlIDS                               Steph Goddard          07/27/2008
# MAGIC 
# MAGIC Reddy Sanam     2020-10-09                                           Changed stage variable -ClmNoteTypeCdSk
# MAGIC                                                                                         Derivation to pass 'FACETS' when source
# MAGIC                                                                                         is 'LUMERIS'
# MAGIC 
# MAGIC Emran.Mohammad               2020-10-12                                              brought up to standards

# MAGIC Read common record format file.
# MAGIC Writing Sequential File to /load
# MAGIC This hash file is written to by all claim foriegn key jobs.
# MAGIC Assign foreign keys and create default records for unknown and not applicable.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
from pyspark.sql.functions import col, lit, when, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


Logging = get_widget_value("Logging","Y")
Source = get_widget_value("Source","")
SrcSysCdSk = get_widget_value("SrcSysCdSk","105838")
InFile = get_widget_value("InFile","IdsClmNotePkey.ClmNoteTMP.dat")

schema_IdsClmNoteExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(10, 0), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("CLM_NOTE_SK", IntegerType(), False),
    StructField("CLM_ID", StringType(), False),
    StructField("CLM_NOTE_SEQ_NO", IntegerType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_USER_ID", StringType(), False),
    StructField("CLM_NOTE_TYP_CD", StringType(), False),
    StructField("LAST_UPDT_DT", StringType(), False),
    StructField("NOTE_DESC", StringType(), True),
    StructField("NOTE_TX", StringType(), True)
])

df_IdsClmNoteExtr = (
    spark.read.format("csv")
    .option("sep", ",")
    .option("quote", "\"")
    .option("header", "false")
    .schema(schema_IdsClmNoteExtr)
    .load(f"{adls_path}/key/{InFile}")
)

df_Transformed = (
    df_IdsClmNoteExtr
    .withColumn("ClmSk", GetFkeyClm(col("SRC_SYS_CD"), col("CLM_NOTE_SK"), col("CLM_ID"), Logging))
    .withColumn("LastUpdtUserIdSk", GetFkeyAppUsr(col("SRC_SYS_CD"), col("CLM_NOTE_SK"), col("LAST_UPDT_USER_ID"), Logging))
    .withColumn(
        "ClmNoteTypeCdSk",
        GetFkeyCodes(
            when(col("SRC_SYS_CD") == "LUMERIS", lit("FACETS")).otherwise(col("SRC_SYS_CD")),
            col("CLM_NOTE_SK"),
            lit("CLAIM NOTE TYPE"),
            col("CLM_NOTE_TYP_CD"),
            Logging
        )
    )
    .withColumn("LastUpdtDtSk", GetFkeyDate(lit("IDS"), col("CLM_NOTE_SK"), col("LAST_UPDT_DT"), Logging))
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn("ErrCount", GetFkeyErrorCnt(col("CLM_NOTE_SK")))
)

df_ForeignKey_Pkey = (
    df_Transformed
    .filter((col("ErrCount") == 0) | (col("PassThru") == "Y"))
    .select(
        col("CLM_NOTE_SK").alias("CLM_NOTE_SK"),
        lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        col("CLM_ID").alias("CLM_ID"),
        col("CLM_NOTE_SEQ_NO").alias("CLM_NOTE_SEQ_NO"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("ClmSk").alias("CLM_SK"),
        col("LastUpdtUserIdSk").alias("LAST_UPDT_USER_ID_SK"),
        col("ClmNoteTypeCdSk").alias("CLM_NOTE_TYP_CD_SK"),
        col("LastUpdtDtSk").alias("LAST_UPDT_DT_SK"),
        col("NOTE_DESC").alias("NOTE_DESC"),
        col("NOTE_TX").alias("NOTE_TX")
    )
)

df_ForeignKey_recycle = (
    df_Transformed
    .filter(col("ErrCount") > 0)
    .select(
        GetRecycleKey(col("CLM_NOTE_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        col("DISCARD_IN").alias("DISCARD_IN"),
        col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        col("ErrCount").alias("ERR_CT"),
        (col("RECYCLE_CT") + lit(1)).alias("RECYCLE_CT"),
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        col("CLM_NOTE_SK").alias("CLM_NOTE_SK"),
        col("CLM_ID").alias("CLM_ID"),
        col("CLM_NOTE_SEQ_NO").alias("CLM_NOTE_SEQ_NO"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_USER_ID").alias("LAST_UPDT_USER_ID"),
        col("CLM_NOTE_TYP_CD").alias("CLM_NOTE_TYP_CD"),
        col("LAST_UPDT_DT").alias("LAST_UPDT_DT"),
        col("NOTE_DESC").alias("NOTE_DESC"),
        col("NOTE_TX").alias("NOTE_TX")
    )
)

df_ForeignKey_Recycle_Clms = (
    df_Transformed
    .filter(col("ErrCount") > 0)
    .select(
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("CLM_ID").alias("CLM_ID")
    )
)

df_OneRow = df_Transformed.limit(1)

df_ForeignKey_DefaultUNK = df_OneRow.select(
    lit(0).alias("CLM_NOTE_SK"),
    lit(0).alias("SRC_SYS_CD_SK"),
    lit("UNK").alias("CLM_ID"),
    lit(0).alias("CLM_NOTE_SEQ_NO"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("CLM_SK"),
    lit(0).alias("LAST_UPDT_USER_ID_SK"),
    lit(0).alias("CLM_NOTE_TYP_CD_SK"),
    lit("NA").alias("LAST_UPDT_DT_SK"),
    lit("UNK").alias("NOTE_DESC"),
    lit("UNK").alias("NOTE_TX")
)

df_ForeignKey_DefaultNA = df_OneRow.select(
    lit(1).alias("CLM_NOTE_SK"),
    lit(1).alias("SRC_SYS_CD_SK"),
    lit("NA").alias("CLM_ID"),
    lit(0).alias("CLM_NOTE_SEQ_NO"),
    lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(1).alias("CLM_SK"),
    lit(1).alias("LAST_UPDT_USER_ID_SK"),
    lit(1).alias("CLM_NOTE_TYP_CD_SK"),
    lit("NA").alias("LAST_UPDT_DT_SK"),
    lit("NA").alias("NOTE_DESC"),
    lit("NA").alias("NOTE_TX")
)

df_Collector = (
    df_ForeignKey_DefaultUNK
    .unionByName(df_ForeignKey_DefaultNA)
    .unionByName(df_ForeignKey_Pkey)
)

df_Collector_final = df_Collector.select(
    col("CLM_NOTE_SK"),
    col("SRC_SYS_CD_SK"),
    col("CLM_ID"),
    col("CLM_NOTE_SEQ_NO"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("CLM_SK"),
    col("LAST_UPDT_USER_ID_SK"),
    col("CLM_NOTE_TYP_CD_SK"),
    rpad(col("LAST_UPDT_DT_SK"), 10, " ").alias("LAST_UPDT_DT_SK"),
    col("NOTE_DESC"),
    col("NOTE_TX")
)

write_files(
    df_Collector_final,
    f"{adls_path}/load/CLM_NOTE.{Source}.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_recycle_final = df_ForeignKey_recycle.select(
    col("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    col("SRC_SYS_CD"),
    col("PRI_KEY_STRING"),
    col("CLM_NOTE_SK"),
    col("CLM_ID"),
    col("CLM_NOTE_SEQ_NO"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(col("LAST_UPDT_USER_ID"), 10, " ").alias("LAST_UPDT_USER_ID"),
    rpad(col("CLM_NOTE_TYP_CD"), 10, " ").alias("CLM_NOTE_TYP_CD"),
    rpad(col("LAST_UPDT_DT"), 10, " ").alias("LAST_UPDT_DT"),
    col("NOTE_DESC"),
    col("NOTE_TX")
)

write_files(
    df_recycle_final,
    f"{adls_path}/hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_parquet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_claim_recycle_keys_final = df_ForeignKey_Recycle_Clms.select(
    col("SRC_SYS_CD"),
    col("CLM_ID")
)

write_files(
    df_claim_recycle_keys_final,
    f"{adls_path}/hf_claim_recycle_keys.parquet",
    delimiter=",",
    mode="overwrite",
    is_parquet=True,
    header=True,
    quote="\"",
    nullValue=None
)