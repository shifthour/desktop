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
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsSubAuditFkey
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
# MAGIC                             GetFkeyAppUsr
# MAGIC                             GetFKeyDate
# MAGIC 
# MAGIC PROCESSING:
# MAGIC 
# MAGIC 
# MAGIC OUTPUTS:  SUB_AUDIT table load file
# MAGIC                      hf_recycle - If recycling was used for this job records would be written to this hash file that had errors.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC            Parikshith Chada  10/23/2006      Originally Programmed

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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
from pyspark.sql.functions import col, lit, when, rpad, trim
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


Logging = get_widget_value("Logging", "Y")
RunID = get_widget_value("RunID", "")
RunCycle = get_widget_value("RunCycle", "100")

subAuditCrfSchema = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38,10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("SUB_AUDIT_SK", IntegerType(), False),
    StructField("SUB_AUDIT_ROW_ID", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("SRC_SYS_CRT_USER_SK", IntegerType(), False),
    StructField("SUB_SK", IntegerType(), False),
    StructField("SUB_AUDIT_ACTN_CD_SK", IntegerType(), False),
    StructField("SUB_FMLY_CNTR_CD_SK", IntegerType(), False),
    StructField("SCRD_IN", StringType(), False),
    StructField("ORIG_EFF_DT_SK", StringType(), False),
    StructField("RETR_DT_SK", StringType(), False),
    StructField("SRC_SYS_CRT_DT_SK", StringType(), False),
    StructField("SUB_UNIQ_KEY", IntegerType(), False),
    StructField("SUB_FIRST_NM", StringType(), True),
    StructField("SUB_MIDINIT", StringType(), True),
    StructField("SUB_LAST_NM", StringType(), True),
    StructField("SUB_AUDIT_HIRE_DT_TX", StringType(), False),
    StructField("SUB_AUDIT_RCVD_DT_TX", StringType(), False),
    StructField("SUB_AUDIT_STTUS_TX", StringType(), False)
])

df_SubAuditCrf = (
    spark.read
    .csv(
        path=f"{adls_path}/key/FctsSubAuditExtr.SubAudit.uniq",
        schema=subAuditCrfSchema,
        sep=",",
        quote="\"",
        header=False
    )
)

dfKey = df_SubAuditCrf

df_enriched = (
    dfKey
    .withColumn("SrcSysCdSk", GetFkeyCodes("IDS", col("SUB_AUDIT_SK"), "SOURCE SYSTEM", trim(col("SRC_SYS_CD")), Logging))
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn("svAppUsrSk", GetFkeyAppUsr(col("SRC_SYS_CD"), col("SUB_AUDIT_SK"), col("SRC_SYS_CRT_USER_SK"), Logging))
    .withColumn("svSubSk", GetFkeySub(col("SRC_SYS_CD"), col("SUB_AUDIT_SK"), col("SUB_UNIQ_KEY"), Logging))
    .withColumn("svSubAuditActnSk", GetFkeyCodes(col("SRC_SYS_CD"), col("SUB_AUDIT_SK"), "AUDIT ACTION", col("SUB_AUDIT_ACTN_CD_SK"), Logging))
    .withColumn("svSubFmlyCntrSk", GetFkeyCodes(col("SRC_SYS_CD"), col("SUB_AUDIT_SK"), "SUBSCRIBER FAMILY INDICATOR", col("SUB_FMLY_CNTR_CD_SK"), Logging))
    .withColumn("svOrigEffDtSk", GetFkeyDate("IDS", col("SUB_AUDIT_SK"), col("ORIG_EFF_DT_SK"), Logging))
    .withColumn("svRetrDtSk", GetFkeyDate("IDS", col("SUB_AUDIT_SK"), col("RETR_DT_SK"), Logging))
    .withColumn("svSrcSysCrtDtSk", GetFkeyDate("IDS", col("SUB_AUDIT_SK"), col("SRC_SYS_CRT_DT_SK"), Logging))
    .withColumn("svSubAuditHireDtTx", GetFkeyDate("IDS", col("SUB_AUDIT_SK"), col("SUB_AUDIT_HIRE_DT_TX"), Logging))
    .withColumn("svSubAuditRcvdDtTx", GetFkeyDate("IDS", col("SUB_AUDIT_SK"), col("SUB_AUDIT_RCVD_DT_TX"), Logging))
    .withColumn("ErrCount", GetFkeyErrorCnt(col("SUB_AUDIT_SK")))
)

df_SubAuditOut = (
    df_enriched
    .filter((col("ErrCount") == 0) | (col("PassThru") == 'Y'))
    .withColumn(
        "SUB_AUDIT_HIRE_DT_TX",
        when(col("svSubAuditHireDtTx") == lit(" "), lit("NA")).otherwise(col("svSubAuditHireDtTx"))
    )
    .select(
        col("SUB_AUDIT_SK").alias("SUB_AUDIT_SK"),
        col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
        col("SUB_AUDIT_ROW_ID").alias("SUB_AUDIT_ROW_ID"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("svAppUsrSk").alias("SRC_SYS_CRT_USER_SK"),
        col("svSubSk").alias("SUB_SK"),
        col("svSubAuditActnSk").alias("SUB_AUDIT_ACTN_CD_SK"),
        col("svSubFmlyCntrSk").alias("SUB_FMLY_CNTR_CD_SK"),
        col("SCRD_IN").alias("SCRD_IN"),
        col("svOrigEffDtSk").alias("ORIG_EFF_DT_SK"),
        col("svRetrDtSk").alias("RETR_DT_SK"),
        col("svSrcSysCrtDtSk").alias("SRC_SYS_CRT_DT_SK"),
        col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
        col("SUB_FIRST_NM").alias("SUB_FIRST_NM"),
        col("SUB_MIDINIT").alias("SUB_MIDINIT"),
        col("SUB_LAST_NM").alias("SUB_LAST_NM"),
        col("SUB_AUDIT_HIRE_DT_TX").alias("SUB_AUDIT_HIRE_DT_TX"),
        col("svSubAuditRcvdDtTx").alias("SUB_AUDIT_RCVD_DT_TX"),
        col("SUB_AUDIT_STTUS_TX").alias("SUB_AUDIT_STTUS_TX")
    )
)

df_lnkRecycle_pre = df_enriched.filter(col("ErrCount") > 0)

df_lnkRecycle = (
    df_lnkRecycle_pre
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", GetRecycleKey(col("SUB_AUDIT_SK")))
    .select(
        col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
        col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        col("DISCARD_IN").alias("DISCARD_IN"),
        col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        (col("ERR_CT") + lit(1)).alias("ERR_CT"),
        col("RECYCLE_CT").alias("RECYCLE_CT"),
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        col("SUB_AUDIT_SK").alias("SUB_AUDIT_SK"),
        col("SUB_AUDIT_ROW_ID").alias("SUB_AUDIT_ROW_ID"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("SRC_SYS_CRT_USER_SK").alias("SRC_SYS_CRT_USER_SK"),
        col("SUB_SK").alias("SUB_SK"),
        col("SUB_AUDIT_ACTN_CD_SK").alias("SUB_AUDIT_ACTN_CD_SK"),
        col("SUB_FMLY_CNTR_CD_SK").alias("SUB_FMLY_CNTR_CD_SK"),
        col("SCRD_IN").alias("SCRD_IN"),
        col("ORIG_EFF_DT_SK").alias("ORIG_EFF_DT_SK"),
        col("RETR_DT_SK").alias("RETR_DT_SK"),
        col("SRC_SYS_CRT_DT_SK").alias("SRC_SYS_CRT_DT_SK"),
        col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
        col("SUB_FIRST_NM").alias("SUB_FIRST_NM"),
        col("SUB_MIDINIT").alias("SUB_MIDINIT"),
        col("SUB_LAST_NM").alias("SUB_LAST_NM"),
        col("SUB_AUDIT_HIRE_DT_TX").alias("SUB_AUDIT_HIRE_DT_TX"),
        col("SUB_AUDIT_RCVD_DT_TX").alias("SUB_AUDIT_RCVD_DT_TX"),
        col("SUB_AUDIT_STTUS_TX").alias("SUB_AUDIT_STTUS_TX")
    )
)

df_recycle_sub_list = (
    df_lnkRecycle_pre
    .select(
        col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
        lit("SUB_AUDIT").alias("TABLE")
    )
)

defaultSchema = StructType([
    StructField("SUB_AUDIT_SK", IntegerType(), True),
    StructField("SRC_SYS_CD_SK", IntegerType(), True),
    StructField("SUB_AUDIT_ROW_ID", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("SRC_SYS_CRT_USER_SK", IntegerType(), True),
    StructField("SUB_SK", IntegerType(), True),
    StructField("SUB_AUDIT_ACTN_CD_SK", IntegerType(), True),
    StructField("SUB_FMLY_CNTR_CD_SK", IntegerType(), True),
    StructField("SCRD_IN", StringType(), True),
    StructField("ORIG_EFF_DT_SK", StringType(), True),
    StructField("RETR_DT_SK", StringType(), True),
    StructField("SRC_SYS_CRT_DT_SK", StringType(), True),
    StructField("SUB_UNIQ_KEY", IntegerType(), True),
    StructField("SUB_FIRST_NM", StringType(), True),
    StructField("SUB_MIDINIT", StringType(), True),
    StructField("SUB_LAST_NM", StringType(), True),
    StructField("SUB_AUDIT_HIRE_DT_TX", StringType(), True),
    StructField("SUB_AUDIT_RCVD_DT_TX", StringType(), True),
    StructField("SUB_AUDIT_STTUS_TX", StringType(), True)
])

df_DefaultUNK = spark.createDataFrame(
    [
        (
            0, 0, "UNK", 0, 0, 0, 0, 0, 0,
            "U", "UNK", "UNK", "UNK", 0, "UNK",
            "U", "UNK", "UNK", "UNK", "UNK"
        )
    ],
    defaultSchema
)

df_DefaultNA = spark.createDataFrame(
    [
        (
            1, 1, "NA", 1, 1, 1, 1, 1, 1,
            "X", "NA", "NA", "NA", 1, "NA",
            "X", "NA", "NA", "NA", "NA"
        )
    ],
    defaultSchema
)

df_collector = (
    df_SubAuditOut
    .unionByName(df_DefaultUNK)
    .unionByName(df_DefaultNA)
)

df_collector_final = (
    df_collector
    .withColumn("SCRD_IN", rpad(col("SCRD_IN"), 1, " "))
    .withColumn("ORIG_EFF_DT_SK", rpad(col("ORIG_EFF_DT_SK"), 10, " "))
    .withColumn("RETR_DT_SK", rpad(col("RETR_DT_SK"), 10, " "))
    .withColumn("SRC_SYS_CRT_DT_SK", rpad(col("SRC_SYS_CRT_DT_SK"), 10, " "))
    .withColumn("SUB_MIDINIT", rpad(col("SUB_MIDINIT"), 1, " "))
    .select(
        "SUB_AUDIT_SK",
        "SRC_SYS_CD_SK",
        "SUB_AUDIT_ROW_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "SRC_SYS_CRT_USER_SK",
        "SUB_SK",
        "SUB_AUDIT_ACTN_CD_SK",
        "SUB_FMLY_CNTR_CD_SK",
        "SCRD_IN",
        "ORIG_EFF_DT_SK",
        "RETR_DT_SK",
        "SRC_SYS_CRT_DT_SK",
        "SUB_UNIQ_KEY",
        "SUB_FIRST_NM",
        "SUB_MIDINIT",
        "SUB_LAST_NM",
        "SUB_AUDIT_HIRE_DT_TX",
        "SUB_AUDIT_RCVD_DT_TX",
        "SUB_AUDIT_STTUS_TX"
    )
)

df_hf_recycle_final = (
    df_lnkRecycle
    .withColumn("INSRT_UPDT_CD", rpad(col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", rpad(col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", rpad(col("PASS_THRU_IN"), 1, " "))
    .withColumn("SCRD_IN", rpad(col("SCRD_IN"), 1, " "))
    .withColumn("ORIG_EFF_DT_SK", rpad(col("ORIG_EFF_DT_SK"), 10, " "))
    .withColumn("RETR_DT_SK", rpad(col("RETR_DT_SK"), 10, " "))
    .withColumn("SRC_SYS_CRT_DT_SK", rpad(col("SRC_SYS_CRT_DT_SK"), 10, " "))
    .withColumn("SUB_MIDINIT", rpad(col("SUB_MIDINIT"), 1, " "))
    .select(
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "PASS_THRU_IN",
        "FIRST_RECYC_DT",
        "ERR_CT",
        "RECYCLE_CT",
        "SRC_SYS_CD",
        "PRI_KEY_STRING",
        "SUB_AUDIT_SK",
        "SUB_AUDIT_ROW_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "SRC_SYS_CRT_USER_SK",
        "SUB_SK",
        "SUB_AUDIT_ACTN_CD_SK",
        "SUB_FMLY_CNTR_CD_SK",
        "SCRD_IN",
        "ORIG_EFF_DT_SK",
        "RETR_DT_SK",
        "SRC_SYS_CRT_DT_SK",
        "SUB_UNIQ_KEY",
        "SUB_FIRST_NM",
        "SUB_MIDINIT",
        "SUB_LAST_NM",
        "SUB_AUDIT_HIRE_DT_TX",
        "SUB_AUDIT_RCVD_DT_TX",
        "SUB_AUDIT_STTUS_TX"
    )
)

write_files(
    df_hf_recycle_final,
    "hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_recycle_sub_list_final = df_recycle_sub_list.select("SUB_UNIQ_KEY", "TABLE")

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

write_files(
    df_collector_final,
    f"{adls_path}/load/SUB_AUDIT.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)