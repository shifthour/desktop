# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 01/29/08 08:55:59 Batch  14639_32164 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_3 03/28/07 06:17:04 Batch  14332_22629 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 03/07/07 16:30:43 Batch  14311_59451 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 12/28/06 11:25:48 Batch  14242_41157 PROMOTE bckcetl ids20 dsadm Keith for Steph
# MAGIC ^1_1 12/28/06 11:19:58 Batch  14242_40806 INIT bckcett testIDS30 dsadm KEith for Steph
# MAGIC ^1_2 12/12/06 14:20:31 Batch  14226_51635 PROMOTE bckcett testIDS30 u03651 steffy
# MAGIC ^1_2 12/12/06 14:18:18 Batch  14226_51500 INIT bckcett devlIDS30 u03651 steffy
# MAGIC ^1_1 12/08/06 10:45:42 Batch  14222_38754 INIT bckcett devlIDS30 u10913 Ollie move from Devl to Test for Steph
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsSubClsAuditFkey
# MAGIC 
# MAGIC DESCRIPTION:     Takes the sorted file and applies the primary key.   Preps the file for the foreign key lookup process that follows.
# MAGIC       
# MAGIC 
# MAGIC INPUTS:  Primary key job output
# MAGIC 	
# MAGIC   
# MAGIC HASH FILES:     hf_recycle
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  GetFkeyCodes
# MAGIC                             GetFkeyErrorCnt
# MAGIC                             GetFkeySub
# MAGIC                             GetFkeyCls
# MAGIC                             GetFkeyAppUsr
# MAGIC                             GetFKeyDate
# MAGIC 
# MAGIC PROCESSING:
# MAGIC 
# MAGIC 
# MAGIC OUTPUTS:  SUB_CLS_AUDIT table load file
# MAGIC                      hf_recycle - If recycling was used for this job records would be written to this hash file that had errors.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC            Parikshith Chada  10/24/2006      Originally Programmed

# MAGIC Read common record format file from extract job.
# MAGIC Set all foreign surragote keys
# MAGIC Merge source data with default rows
# MAGIC Writing Sequential File to /load
# MAGIC #$FilePath#/landing/FctsMbrRecycleList.SubRecycle.dat.#RunID#
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
    StringType,
    IntegerType,
    DecimalType,
    TimestampType,
    DateType
)
from pyspark.sql.functions import col, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


Logging = get_widget_value("Logging", "Y")
RunID = get_widget_value("RunID", "")
RunCycle = get_widget_value("RunCycle", "100")

schema_SubClsAuditCrf = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(10, 0), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("SUB_CLS_AUDIT_SK", IntegerType(), False),
    StructField("SUB_CLS_AUDIT_ROW_ID", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("CLS_SK", IntegerType(), False),
    StructField("SRC_SYS_CRT_USER_SK", IntegerType(), False),
    StructField("SUB_SK", IntegerType(), False),
    StructField("SUB_CLS_AUDIT_ACTN_CD_SK", IntegerType(), False),
    StructField("SRC_SYS_CRT_DT_SK", StringType(), False),
    StructField("SUB_UNIQ_KEY", IntegerType(), False),
    StructField("SUB_CLS_AUDIT_EFF_DT", DateType(), False),
    StructField("SUB_CLS_AUDIT_TERM_DT", DateType(), False),
    StructField("GRGR_ID", StringType(), False)
])

df_SubClsAuditCrf = (
    spark.read
    .option("header", False)
    .option("quote", "\"")
    .option("delimiter", ",")
    .schema(schema_SubClsAuditCrf)
    .csv(f"{adls_path}/key/FctsSubClsAuditExtr.SubClsAudit.uniq")
)

df_purgeTrnSubClsAudit = (
    df_SubClsAuditCrf
    .withColumn(
        "SrcSysCdSk",
        GetFkeyCodes(
            lit("IDS"),
            col("SUB_CLS_AUDIT_SK"),
            lit("SOURCE SYSTEM"),
            trim(col("SRC_SYS_CD")),
            lit(Logging)
        )
    )
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn(
        "svClsSk",
        GetFkeyCls(
            col("SRC_SYS_CD"),
            col("SUB_CLS_AUDIT_SK"),
            col("GRGR_ID"),
            col("CLS_SK"),
            lit(Logging)
        )
    )
    .withColumn(
        "svAppUsrSk",
        GetFkeyAppUsr(
            col("SRC_SYS_CD"),
            col("SUB_CLS_AUDIT_SK"),
            col("SRC_SYS_CRT_USER_SK"),
            lit(Logging)
        )
    )
    .withColumn(
        "svSubSk",
        GetFkeySub(
            col("SRC_SYS_CD"),
            col("SUB_CLS_AUDIT_SK"),
            col("SUB_SK"),
            lit(Logging)
        )
    )
    .withColumn(
        "svSubClsAuditActnSk",
        GetFkeyCodes(
            col("SRC_SYS_CD"),
            col("SUB_CLS_AUDIT_SK"),
            lit("AUDIT ACTION"),
            trim(col("SUB_CLS_AUDIT_ACTN_CD_SK")),
            lit(Logging)
        )
    )
    .withColumn(
        "svSrcSysCrtDtSk",
        GetFkeyDate(
            lit("IDS"),
            col("SUB_CLS_AUDIT_SK"),
            col("SRC_SYS_CRT_DT_SK"),
            lit(Logging)
        )
    )
    .withColumn("ErrCount", GetFkeyErrorCnt(col("SUB_CLS_AUDIT_SK")))
)

df_SubClsAuditOut = (
    df_purgeTrnSubClsAudit
    .filter((col("ErrCount") == 0) | (col("PassThru") == 'Y'))
    .select(
        col("SUB_CLS_AUDIT_SK"),
        col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
        col("SUB_CLS_AUDIT_ROW_ID"),
        col("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("svClsSk").alias("CLS_SK"),
        col("svAppUsrSk").alias("SRC_SYS_CRT_USER_SK"),
        col("svSubSk").alias("SUB_SK"),
        col("svSubClsAuditActnSk").alias("SUB_CLS_AUDIT_ACTN_CD_SK"),
        col("svSrcSysCrtDtSk").alias("SRC_SYS_CRT_DT_SK"),
        col("SUB_UNIQ_KEY"),
        col("SUB_CLS_AUDIT_EFF_DT"),
        col("SUB_CLS_AUDIT_TERM_DT")
    )
)

df_lnkRecycle = (
    df_purgeTrnSubClsAudit
    .filter(col("ErrCount") > 0)
    .select(
        GetRecycleKey(col("SUB_CLS_AUDIT_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        col("INSRT_UPDT_CD"),
        col("DISCARD_IN"),
        col("PASS_THRU_IN"),
        col("FIRST_RECYC_DT"),
        col("ERR_CT"),
        col("RECYCLE_CT"),
        col("SRC_SYS_CD"),
        col("PRI_KEY_STRING"),
        col("SUB_CLS_AUDIT_SK"),
        col("SUB_CLS_AUDIT_ROW_ID"),
        col("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("CLS_SK"),
        col("SRC_SYS_CRT_USER_SK"),
        col("SUB_SK"),
        col("SUB_CLS_AUDIT_ACTN_CD_SK"),
        col("SRC_SYS_CRT_DT_SK"),
        col("SUB_UNIQ_KEY"),
        col("SUB_CLS_AUDIT_EFF_DT"),
        col("SUB_CLS_AUDIT_TERM_DT")
    )
)

df_lnkRecycle_final = df_lnkRecycle.select(
    rpad(col("JOB_EXCTN_RCRD_ERR_SK"), <...>, " ").alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    (col("ERR_CT") + 1).alias("ERR_CT"),
    col("RECYCLE_CT").alias("RECYCLE_CT"),
    rpad(col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    rpad(col("PRI_KEY_STRING"), <...>, " ").alias("PRI_KEY_STRING"),
    col("SUB_CLS_AUDIT_SK").alias("SUB_CLS_AUDIT_SK"),
    rpad(col("SUB_CLS_AUDIT_ROW_ID"), <...>, " ").alias("SUB_CLS_AUDIT_ROW_ID"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("CLS_SK").alias("CLS_SK"),
    col("SRC_SYS_CRT_USER_SK").alias("SRC_SYS_CRT_USER_SK"),
    col("SUB_SK").alias("SUB_SK"),
    col("SUB_CLS_AUDIT_ACTN_CD_SK").alias("SUB_CLS_AUDIT_ACTN_CD_SK"),
    rpad(col("SRC_SYS_CRT_DT_SK"), 10, " ").alias("SRC_SYS_CRT_DT_SK"),
    col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    col("SUB_CLS_AUDIT_EFF_DT").alias("SUB_CLS_AUDIT_EFF_DT"),
    col("SUB_CLS_AUDIT_TERM_DT").alias("SUB_CLS_AUDIT_TERM_DT")
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

df_recycle_sub_list = (
    df_purgeTrnSubClsAudit
    .filter(col("ErrCount") > 0)
    .select(
        col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
        lit("SUB_CLS_AUDIT").alias("TABLE")
    )
)

df_recycle_sub_list_final = df_recycle_sub_list.select(
    col("SUB_UNIQ_KEY"),
    rpad(col("TABLE"), <...>, " ").alias("TABLE")
)

write_files(
    df_recycle_sub_list_final,
    f"{adls_path_raw}/landing/FctsMbrRecycleList.SubRecycle.dat.{RunID}",
    ",",
    "append",
    False,
    False,
    "\"",
    None
)

df_defaultUNK = df_purgeTrnSubClsAudit.limit(1).select(
    lit(0).alias("SUB_CLS_AUDIT_SK"),
    lit(0).alias("SRC_SYS_CD_SK"),
    lit("UNK").alias("SUB_CLS_AUDIT_ROW_ID"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("CLS_SK"),
    lit(0).alias("SRC_SYS_CRT_USER_SK"),
    lit(0).alias("SUB_SK"),
    lit(0).alias("SUB_CLS_AUDIT_ACTN_CD_SK"),
    lit("UNK").alias("SRC_SYS_CRT_DT_SK"),
    lit(0).alias("SUB_UNIQ_KEY"),
    lit("1756-01-01").cast(DateType()).alias("SUB_CLS_AUDIT_EFF_DT"),
    lit("2199-12-31").cast(DateType()).alias("SUB_CLS_AUDIT_TERM_DT")
)

df_defaultNA = df_purgeTrnSubClsAudit.limit(1).select(
    lit(1).alias("SUB_CLS_AUDIT_SK"),
    lit(1).alias("SRC_SYS_CD_SK"),
    lit("NA").alias("SUB_CLS_AUDIT_ROW_ID"),
    lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(1).alias("CLS_SK"),
    lit(1).alias("SRC_SYS_CRT_USER_SK"),
    lit(1).alias("SUB_SK"),
    lit(1).alias("SUB_CLS_AUDIT_ACTN_CD_SK"),
    lit("NA").alias("SRC_SYS_CRT_DT_SK"),
    lit(1).alias("SUB_UNIQ_KEY"),
    lit("1756-01-01").cast(DateType()).alias("SUB_CLS_AUDIT_EFF_DT"),
    lit("2199-12-31").cast(DateType()).alias("SUB_CLS_AUDIT_TERM_DT")
)

df_collector = (
    df_SubClsAuditOut
    .unionByName(df_defaultUNK)
    .unionByName(df_defaultNA)
)

df_collector_final = df_collector.select(
    col("SUB_CLS_AUDIT_SK"),
    col("SRC_SYS_CD_SK"),
    rpad(col("SUB_CLS_AUDIT_ROW_ID"), <...>, " ").alias("SUB_CLS_AUDIT_ROW_ID"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("CLS_SK"),
    col("SRC_SYS_CRT_USER_SK"),
    col("SUB_SK"),
    col("SUB_CLS_AUDIT_ACTN_CD_SK"),
    rpad(col("SRC_SYS_CRT_DT_SK"), 10, " ").alias("SRC_SYS_CRT_DT_SK"),
    col("SUB_UNIQ_KEY"),
    col("SUB_CLS_AUDIT_EFF_DT"),
    col("SUB_CLS_AUDIT_TERM_DT")
)

write_files(
    df_collector_final,
    f"{adls_path}/load/SUB_CLS_AUDIT.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)