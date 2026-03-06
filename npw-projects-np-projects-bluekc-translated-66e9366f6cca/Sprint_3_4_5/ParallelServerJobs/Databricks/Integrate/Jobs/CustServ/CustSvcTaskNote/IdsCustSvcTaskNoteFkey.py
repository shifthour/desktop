# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2007, 2008, 2014 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by:
# MAGIC                     FctsCustSvcLoadSeq
# MAGIC 
# MAGIC Processing:
# MAGIC                     Read in CRF key file, perform foreign key lookups, and create NA and UNK rows.  Write output to CUST_SVC_TASK_NOTE.dat load file.
# MAGIC 
# MAGIC Control Job Rerun Information: 
# MAGIC                     Previous Run Successful:    Restore the key file then rerun
# MAGIC                     Previous Run Aborted:         Restart, no other steps necessary
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                                    Project/                                                                                                                            Code                   Date
# MAGIC Developer             Date              Altiris #          Change Description                                                                                     Reviewer            Reviewed
# MAGIC ---------------------------  -------------------   ------------------   -----------------------------------------------------------------------------------------------------------------   -------------------------  -------------------
# MAGIC Parikshith Chada  02/2/2007     3028             Originally Programmed                                                                                 Steph Goddard   02/21/2007
# MAGIC Brent Leland         02/29/2008   3567 PKey   Added error recycle                                                                                     Steph Goddard   05/06/2008
# MAGIC                                                                         Added source system to load file name
# MAGIC Hugh Sisson         2014-06-16    TFS4126      Changed key file column from LAST_UPDT_USER_SK Integer to              Kalyan Neelam   2014-06-23
# MAGIC                                                                         LAST_UPDT_USER VarChar(255)

# MAGIC Read common record format file from extract job.
# MAGIC Set all foreign surrogate keys
# MAGIC Merge source data with default rows
# MAGIC Writing Sequential File to /load
# MAGIC This hash file is written to by all Customer Service foriegn key jobs.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DecimalType
from pyspark.sql import functions as F
from datetime import datetime
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


Logging = get_widget_value("Logging","Y")
InFile = get_widget_value("InFile","FctsCustSvcTaskNoteExtr.CustSvcTaskNote.dat")
SrcSysCdSk = get_widget_value("SrcSysCdSk","")
SrcSysCd = get_widget_value("SrcSysCd","")

schema_IdsCustSvcTaskNoteFkey = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38,10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("CUST_SVC_TASK_NOTE_SK", IntegerType(), False),
    StructField("CUST_SVC_ID", StringType(), False),
    StructField("TASK_SEQ_NO", IntegerType(), False),
    StructField("CUST_SVC_TASK_NOTE_LOC_CD_SK", IntegerType(), False),
    StructField("NOTE_SEQ_NO", IntegerType(), False),
    StructField("LAST_UPDT_DTM", TimestampType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("CUST_SVC_TASK_SK", IntegerType(), False),
    StructField("LAST_UPDT_USER", StringType(), False),
    StructField("NOTE_SUM_TX", StringType(), True)
])

df_IdsCustSvcTaskNoteFkey = (
    spark.read.format("csv")
    .option("quote", "\"")
    .option("header", "false")
    .option("delimiter", ",")
    .schema(schema_IdsCustSvcTaskNoteFkey)
    .load(f"{adls_path}/key/{InFile}")
)

df_tfm = (
    df_IdsCustSvcTaskNoteFkey
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("svLastUpdtUserSk", GetFkeyAppUsr(F.col("SRC_SYS_CD"), F.col("CUST_SVC_TASK_NOTE_SK"), F.col("LAST_UPDT_USER"), F.lit(Logging)))
    .withColumn("svCustSvcTaskNoteSrcCdSk", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("CUST_SVC_TASK_NOTE_SK"), F.lit("CUSTOMER SERVICE TASK NOTE SOURCE"), F.col("CUST_SVC_TASK_NOTE_LOC_CD_SK"), F.lit(Logging)))
    .withColumn("svCustSvcTaskSk", GetFkeyCustSvcTask(F.col("SRC_SYS_CD"), F.col("CUST_SVC_TASK_NOTE_SK"), F.col("CUST_SVC_ID"), F.col("TASK_SEQ_NO"), F.lit(Logging)))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("CUST_SVC_TASK_NOTE_SK")))
)

df_CustSvcTaskNote = (
    df_tfm
    .filter((F.col("ErrCount") == 0) | (F.col("PassThru") == "Y"))
    .select(
        F.col("CUST_SVC_TASK_NOTE_SK").alias("CUST_SVC_TASK_NOTE_SK"),
        F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        F.col("CUST_SVC_ID").alias("CUST_SVC_ID"),
        F.col("TASK_SEQ_NO").alias("TASK_SEQ_NO"),
        F.col("svCustSvcTaskNoteSrcCdSk").alias("CUST_SVC_TASK_NOTE_LOC_CD_SK"),
        F.col("NOTE_SEQ_NO").alias("NOTE_SEQ_NO"),
        F.col("LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("svCustSvcTaskSk").alias("CUST_SVC_TASK_SK"),
        F.col("svLastUpdtUserSk").alias("LAST_UPDT_USER_SK"),
        F.col("NOTE_SUM_TX").alias("NOTE_SUM_TX")
    )
)

df_lnkRecycle = (
    df_tfm
    .filter(F.col("ErrCount") > 0)
    .select(
        GetRecycleKey(F.col("CUST_SVC_TASK_NOTE_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("DISCARD_IN").alias("DISCARD_IN"),
        F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        (F.col("ERR_CT") + F.lit(1)).alias("ERR_CT"),
        F.col("RECYCLE_CT").alias("RECYCLE_CT"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("CUST_SVC_TASK_NOTE_SK").alias("CUST_SVC_TASK_NOTE_SK"),
        F.col("CUST_SVC_ID").alias("CUST_SVC_ID"),
        F.col("TASK_SEQ_NO").alias("TASK_SEQ_NO"),
        F.col("CUST_SVC_TASK_NOTE_LOC_CD_SK").alias("CUST_SVC_TASK_NOTE_LOC_CD_SK"),
        F.col("NOTE_SEQ_NO").alias("NOTE_SEQ_NO"),
        F.col("LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("CUST_SVC_TASK_SK").alias("CUST_SVC_TASK_SK"),
        F.col("LAST_UPDT_USER").alias("LAST_UPDT_USER"),
        F.col("NOTE_SUM_TX").alias("NOTE_SUM_TX")
    )
)

df_RecycleKeys = (
    df_tfm
    .filter(F.col("ErrCount") > 0)
    .select(
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("CUST_SVC_ID").alias("CUST_SVC_ID"),
        F.col("TASK_SEQ_NO").alias("TASK_SEQ_NO")
    )
)

df_lnkRecycle_final = df_lnkRecycle.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "CUST_SVC_TASK_NOTE_SK",
    "CUST_SVC_ID",
    "TASK_SEQ_NO",
    "CUST_SVC_TASK_NOTE_LOC_CD_SK",
    "NOTE_SEQ_NO",
    "LAST_UPDT_DTM",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CUST_SVC_TASK_SK",
    "LAST_UPDT_USER",
    "NOTE_SUM_TX"
)

write_files(
    df_lnkRecycle_final,
    "hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_RecycleKeys_final = df_RecycleKeys.select(
    "SRC_SYS_CD",
    "CUST_SVC_ID",
    "TASK_SEQ_NO"
)

write_files(
    df_RecycleKeys_final,
    "hf_custsvc_recycle_keys.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_DefaultUNK = spark.createDataFrame(
    [
        (0, 0, "UNK", 0, 0, 0, "1753-01-01 00:00:00", 0, 0, 0, 0, "UNK")
    ],
    [
        "CUST_SVC_TASK_NOTE_SK",
        "SRC_SYS_CD_SK",
        "CUST_SVC_ID",
        "TASK_SEQ_NO",
        "CUST_SVC_TASK_NOTE_LOC_CD_SK",
        "NOTE_SEQ_NO",
        "LAST_UPDT_DTM",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CUST_SVC_TASK_SK",
        "LAST_UPDT_USER_SK",
        "NOTE_SUM_TX"
    ]
).withColumn("LAST_UPDT_DTM", F.col("LAST_UPDT_DTM").cast(TimestampType()))

df_DefaultNA = spark.createDataFrame(
    [
        (1, 1, "NA", 1, 1, 1, "1753-01-01 00:00:00", 1, 1, 1, 1, "NA")
    ],
    [
        "CUST_SVC_TASK_NOTE_SK",
        "SRC_SYS_CD_SK",
        "CUST_SVC_ID",
        "TASK_SEQ_NO",
        "CUST_SVC_TASK_NOTE_LOC_CD_SK",
        "NOTE_SEQ_NO",
        "LAST_UPDT_DTM",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CUST_SVC_TASK_SK",
        "LAST_UPDT_USER_SK",
        "NOTE_SUM_TX"
    ]
).withColumn("LAST_UPDT_DTM", F.col("LAST_UPDT_DTM").cast(TimestampType()))

df_collector = df_CustSvcTaskNote.union(df_DefaultUNK).union(df_DefaultNA)

df_collector_final = df_collector.select(
    "CUST_SVC_TASK_NOTE_SK",
    "SRC_SYS_CD_SK",
    "CUST_SVC_ID",
    "TASK_SEQ_NO",
    "CUST_SVC_TASK_NOTE_LOC_CD_SK",
    "NOTE_SEQ_NO",
    "LAST_UPDT_DTM",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CUST_SVC_TASK_SK",
    "LAST_UPDT_USER_SK",
    "NOTE_SUM_TX"
).withColumn(
    "CUST_SVC_ID", rpad(F.col("CUST_SVC_ID"), F.lit("<...>"), F.lit(" "))
).withColumn(
    "NOTE_SUM_TX", rpad(F.col("NOTE_SUM_TX"), F.lit("<...>"), F.lit(" "))
)

write_files(
    df_collector_final,
    f"{adls_path}/load/CUST_SVC_TASK_NOTE.{SrcSysCd}.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)