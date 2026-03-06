# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_2 10/26/07 13:38:47 Batch  14544_49159 PROMOTE bckcetl ids20 dsadm bls for hs
# MAGIC ^1_2 10/26/07 13:19:46 Batch  14544_47992 INIT bckcett testIDS30 dsadm bls for hs
# MAGIC ^1_1 09/28/07 14:58:08 Batch  14516_53893 INIT bckcett testIDS30 dsadm bls for hs
# MAGIC ^1_3 09/13/07 17:24:15 Batch  14501_62661 PROMOTE bckcett testIDS30 u11141 Hugh Sisson
# MAGIC ^1_3 09/13/07 17:10:39 Batch  14501_61844 INIT bckcett devlIDS30 u11141 Hugh Sisson
# MAGIC ^1_2 09/12/07 11:51:28 Batch  14500_42704 INIT bckcett devlIDS30 u03651 deploy to test steffy
# MAGIC ^1_1 09/05/07 12:58:39 Batch  14493_46724 INIT bckcett devlIDS30 u11141 Hugh Sisson
# MAGIC 
# MAGIC Â© Copyright 2007 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by:
# MAGIC                     FctsAplLoadSeq
# MAGIC 
# MAGIC Processing:
# MAGIC                     *
# MAGIC 
# MAGIC Control Job Rerun Information: 
# MAGIC              Previous Run Successful:    Restore common record format file from extract job, if necessary
# MAGIC              Previous Run Aborted:         Restart, no other steps necessary
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                               Project/                                                                                                 Code                  Date
# MAGIC Developer        Date              Altiris #           Change Description                                                        Reviewer            Reviewed
# MAGIC ----------------------  -------------------   -------------------   ------------------------------------------------------------------------------------   ----------------------     -------------------   
# MAGIC Hugh Sisson    08/09/2007   3028              Original program                                                              Steph Goddard  9/6/07

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
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


Logging = get_widget_value("Logging","X")
InFile = get_widget_value("InFile","IdsAplLvlNoteExtr.AplLvlNote.dat.200708140933")

# Schema for IdsAplLvlNoteExtr
schema_IdsAplLvlNoteExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38,10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("APL_LVL_NOTE_SK", IntegerType(), nullable=False),
    StructField("APL_ID", StringType(), nullable=False),
    StructField("LVL_SEQ_NO", IntegerType(), nullable=False),
    StructField("NOTE_SEQ_NO", IntegerType(), nullable=False),
    StructField("ATXR_DEST_ID", TimestampType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("APL_LVL_SK", IntegerType(), nullable=False),
    StructField("NOTE_SUM_TX", StringType(), nullable=True)
])

df_IdsAplLvlNoteExtr = (
    spark.read
    .schema(schema_IdsAplLvlNoteExtr)
    .option("header", "false")
    .option("quote", "\"")
    .csv(f"{adls_path}/key/{InFile}")
)

df_transformed = (
    df_IdsAplLvlNoteExtr
    .withColumn("SrcSysCdSk", GetFkeyCodes(F.lit("IDS"), F.col("APL_LVL_SK"), F.lit("SOURCE SYSTEM"), F.col("SRC_SYS_CD"), F.lit(Logging)))
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("svAplLvlSk", GetFkeyAplLvl(F.col("SRC_SYS_CD"), F.col("APL_LVL_NOTE_SK"), F.col("APL_ID"), F.col("LVL_SEQ_NO"), F.lit(Logging)))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("APL_LVL_SK")))
)

df_fkey = (
    df_transformed
    .filter((F.col("ErrCount") == 0) | (F.col("PassThru") == 'Y'))
    .select(
        F.col("APL_LVL_NOTE_SK").alias("APL_LVL_NOTE_SK"),
        F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
        F.col("APL_ID").alias("APL_ID"),
        F.col("LVL_SEQ_NO").alias("LVL_SEQ_NO"),
        F.col("NOTE_SEQ_NO").alias("NOTE_SEQ_NO"),
        F.col("ATXR_DEST_ID").alias("ATXR_DEST_ID"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("svAplLvlSk").alias("APL_LVL_SK"),
        F.col("NOTE_SUM_TX").alias("NOTE_SUM_TX")
    )
)

df_recycle = (
    df_transformed
    .filter(F.col("ErrCount") > 0)
    .select(
        GetRecycleKey(F.col("APL_LVL_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("DISCARD_IN").alias("DISCARD_IN"),
        F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("ErrCount").alias("ERR_CT"),
        (F.col("RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("APL_LVL_NOTE_SK").alias("APL_LVL_NOTE_SK"),
        F.col("APL_ID").alias("APL_ID"),
        F.col("LVL_SEQ_NO").alias("LVL_SEQ_NO"),
        F.col("NOTE_SEQ_NO").alias("NOTE_SEQ_NO"),
        F.col("ATXR_DEST_ID").alias("ATXR_DEST_ID"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("APL_LVL_SK").alias("APL_LVL_SK"),
        F.col("NOTE_SUM_TX").alias("NOTE_SUM_TX")
    )
)

# DefaultUNK
df_defaultUNK_source = df_transformed.limit(1)
df_defaultUNK = df_defaultUNK_source.select(
    F.lit(0).alias("APL_LVL_NOTE_SK"),
    F.lit(0).alias("SRC_SYS_CD_SK"),
    F.lit("UNK").alias("APL_ID"),
    F.lit(0).alias("LVL_SEQ_NO"),
    F.lit(0).alias("NOTE_SEQ_NO"),
    F.lit("2199-12-31 23:59:59.999").alias("ATXR_DEST_ID"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("APL_LVL_SK"),
    F.lit("UNK").alias("NOTE_SUM_TX")
)

# DefaultNA
df_defaultNA_source = df_transformed.limit(1)
df_defaultNA = df_defaultNA_source.select(
    F.lit(1).alias("APL_LVL_NOTE_SK"),
    F.lit(1).alias("SRC_SYS_CD_SK"),
    F.lit("NA").alias("APL_ID"),
    F.lit(1).alias("LVL_SEQ_NO"),
    F.lit(1).alias("NOTE_SEQ_NO"),
    F.lit("1753-01-01 00:00:00.000").alias("ATXR_DEST_ID"),
    F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("APL_LVL_SK"),
    F.lit("NA").alias("NOTE_SUM_TX")
)

# Collector (union)
df_collector = (
    df_fkey
    .unionByName(df_defaultUNK)
    .unionByName(df_defaultNA)
)

# Write hf_recycle as parquet (Scenario C)
df_recycle_out = df_recycle.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.rpad(F.col("SRC_SYS_CD"), 255, " ").alias("SRC_SYS_CD"),
    F.rpad(F.col("PRI_KEY_STRING"), 255, " ").alias("PRI_KEY_STRING"),
    F.col("APL_LVL_NOTE_SK"),
    F.rpad(F.col("APL_ID"), 255, " ").alias("APL_ID"),
    F.col("LVL_SEQ_NO"),
    F.col("NOTE_SEQ_NO"),
    F.col("ATXR_DEST_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("APL_LVL_SK"),
    F.rpad(F.col("NOTE_SUM_TX"), 255, " ").alias("NOTE_SUM_TX")
)

write_files(
    df_recycle_out,
    "hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

# Final output: APL_LVL_NOTE (sequential file)
df_collector_out = df_collector.select(
    F.col("APL_LVL_NOTE_SK"),
    F.col("SRC_SYS_CD_SK"),
    F.rpad(F.col("APL_ID"), 255, " ").alias("APL_ID"),
    F.col("LVL_SEQ_NO"),
    F.col("NOTE_SEQ_NO"),
    F.col("ATXR_DEST_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("APL_LVL_SK"),
    F.rpad(F.col("NOTE_SUM_TX"), 255, " ").alias("NOTE_SUM_TX")
)

write_files(
    df_collector_out,
    f"{adls_path}/load/APL_LVL_NOTE.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)