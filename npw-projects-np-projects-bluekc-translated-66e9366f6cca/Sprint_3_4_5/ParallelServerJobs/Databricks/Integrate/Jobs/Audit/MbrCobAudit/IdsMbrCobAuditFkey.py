# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 09/01/08 11:32:17 Batch  14855_41541 PROMOTE bckcetl ids20 dsadm bls for on'
# MAGIC ^1_1 09/01/08 10:35:59 Batch  14855_38164 INIT bckcett devlIDSnew dsadm bls for on
# MAGIC ^1_1 08/26/08 10:50:12 Batch  14849_39015 INIT bckcett devlIDSnew u03651 steffy
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
# MAGIC JOB NAME:     IdsMbrCobAuditFkey
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
# MAGIC                             GetFkeyMbr
# MAGIC                             GetFkeyAppUsr
# MAGIC                             GetFKeyDate
# MAGIC 
# MAGIC PROCESSING:
# MAGIC 
# MAGIC 
# MAGIC OUTPUTS:  MBR_COB_AUDIT table load file
# MAGIC                      hf_recycle - If recycling was used for this job records would be written to this hash file that had errors.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC            Parikshith Chada  10/25/2006      Originally Programmed
# MAGIC 
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC O. Nielsen                7/25/2008          Facets 4.5.1         changed OTHR_CAR_POL_ID to varchar(40)                           devlIDSnew                    Steph Goddard          08/25/2008

# MAGIC Read common record format file from extract job.
# MAGIC Set all foreign surragote keys
# MAGIC Merge source data with default rows
# MAGIC Writing Sequential File to /load
# MAGIC #$FilePath#/landing/FctsMbrRecycleList.MbrRecycle.dat.#RunID#
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
    DecimalType
)
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


Logging = get_widget_value("Logging","Y")
RunID = get_widget_value("RunID","")
RunCycle = get_widget_value("RunCycle","100")

schema_MbrCobAuditCrf = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38,10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("MBR_COB_AUDIT_SK", IntegerType(), nullable=False),
    StructField("MBR_COB_AUDIT_ROW_ID", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("MBR_SK", IntegerType(), nullable=False),
    StructField("SRC_SYS_CRT_USER_SK", IntegerType(), nullable=False),
    StructField("MBR_COB_AUDIT_ACTN_CD_SK", IntegerType(), nullable=False),
    StructField("MBR_COB_LAST_VER_METH_CD_SK", IntegerType(), nullable=False),
    StructField("MBR_COB_OTHR_CAR_ID_CD_SK", IntegerType(), nullable=False),
    StructField("MBR_COB_PAYMT_PRTY_CD_SK", IntegerType(), nullable=False),
    StructField("MBR_COB_TERM_RSN_CD_SK", IntegerType(), nullable=False),
    StructField("MBR_COB_TYP_CD_SK", IntegerType(), nullable=False),
    StructField("MBR_UNIQ_KEY", IntegerType(), nullable=False),
    StructField("COB_LTR_TRGR_DT_SK", StringType(), nullable=False),
    StructField("EFF_DT_SK", StringType(), nullable=False),
    StructField("LACK_OF_COB_INFO_STRT_DT_SK", StringType(), nullable=False),
    StructField("SRC_SYS_CRT_DT_SK", StringType(), nullable=False),
    StructField("TERM_DT_SK", StringType(), nullable=False),
    StructField("LAST_VERIFIER_TX", StringType(), nullable=False),
    StructField("OTHR_CAR_POL_ID", StringType(), nullable=False)
])

df_MbrCobAuditCrf = (
    spark.read.format("csv")
    .option("sep", ",")
    .option("quote", "\"")
    .option("header", "false")
    .schema(schema_MbrCobAuditCrf)
    .load(f"{adls_path}/key/FctsMbrCobAuditExtr.MbrCobAudit.uniq")
)

df_stageVar = (
    df_MbrCobAuditCrf
    .withColumn(
        "SrcSysCdSk",
        GetFkeyCodes(
            "IDS",
            F.col("MBR_COB_AUDIT_SK"),
            "SOURCE SYSTEM",
            F.col("SRC_SYS_CD"),
            Logging
        )
    )
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn(
        "svMbrSk",
        GetFkeyMbr(
            F.col("SRC_SYS_CD"),
            F.col("MBR_COB_AUDIT_SK"),
            F.col("MBR_UNIQ_KEY"),
            Logging
        )
    )
    .withColumn(
        "svAppUsrSk",
        GetFkeyAppUsr(
            F.col("SRC_SYS_CD"),
            F.col("MBR_COB_AUDIT_SK"),
            F.col("SRC_SYS_CRT_USER_SK"),
            Logging
        )
    )
    .withColumn(
        "svMbrCobAuditActnSk",
        GetFkeyCodes(
            F.col("SRC_SYS_CD"),
            F.col("MBR_COB_AUDIT_SK"),
            "AUDIT ACTION",
            F.col("MBR_COB_AUDIT_ACTN_CD_SK"),
            Logging
        )
    )
    .withColumn(
        "svMbrCobLstVerMethSk",
        GetFkeyCodes(
            F.col("SRC_SYS_CD"),
            F.col("MBR_COB_AUDIT_SK"),
            "MEMBER COB LAST VERIFICATION METHOD",
            F.col("MBR_COB_LAST_VER_METH_CD_SK"),
            Logging
        )
    )
    .withColumn(
        "svMbrCobOthrCarIdSk",
        GetFkeyCodes(
            F.col("SRC_SYS_CD"),
            F.col("MBR_COB_AUDIT_SK"),
            "MEMBER COB OTHER CARRIER IDENTIFIER",
            F.col("MBR_COB_OTHR_CAR_ID_CD_SK"),
            Logging
        )
    )
    .withColumn(
        "svMbrCobPaymtPrtySk",
        GetFkeyCodes(
            F.col("SRC_SYS_CD"),
            F.col("MBR_COB_AUDIT_SK"),
            "MEMBER COB PAYMENT PRIORITY",
            F.col("MBR_COB_PAYMT_PRTY_CD_SK"),
            Logging
        )
    )
    .withColumn(
        "svMbrCobTermRsnSk",
        GetFkeyCodes(
            F.col("SRC_SYS_CD"),
            F.col("MBR_COB_AUDIT_SK"),
            "MEMBER COB TERMINATION REASON CODE",
            F.col("MBR_COB_TERM_RSN_CD_SK"),
            Logging
        )
    )
    .withColumn(
        "svMbrCobTypSk",
        GetFkeyCodes(
            F.col("SRC_SYS_CD"),
            F.col("MBR_COB_AUDIT_SK"),
            "MEMBER COB TYPE",
            F.col("MBR_COB_TYP_CD_SK"),
            Logging
        )
    )
    .withColumn(
        "svCobLtrTrgrDtSk",
        GetFkeyDate(
            "IDS",
            F.col("MBR_COB_AUDIT_SK"),
            F.col("COB_LTR_TRGR_DT_SK"),
            Logging
        )
    )
    .withColumn(
        "svEffDtSk",
        GetFkeyDate(
            "IDS",
            F.col("MBR_COB_AUDIT_SK"),
            F.col("EFF_DT_SK"),
            Logging
        )
    )
    .withColumn(
        "svLackOfCobInfoStrtDtSk",
        GetFkeyDate(
            "IDS",
            F.col("MBR_COB_AUDIT_SK"),
            F.col("LACK_OF_COB_INFO_STRT_DT_SK"),
            Logging
        )
    )
    .withColumn(
        "svSrcSysCrtDtSk",
        GetFkeyDate(
            "IDS",
            F.col("MBR_COB_AUDIT_SK"),
            F.col("SRC_SYS_CRT_DT_SK"),
            Logging
        )
    )
    .withColumn(
        "svTermDtSk",
        GetFkeyDate(
            "IDS",
            F.col("MBR_COB_AUDIT_SK"),
            F.col("TERM_DT_SK"),
            Logging
        )
    )
    .withColumn(
        "ErrCount",
        GetFkeyErrorCnt(F.col("MBR_COB_AUDIT_SK"))
    )
)

df_mbrCobAuditOut = df_stageVar.filter(
    (F.col("ErrCount") == 0) | (F.col("PassThru") == "Y")
)

df_mbrCobAuditOut = (
    df_mbrCobAuditOut
    .withColumn(
        "MBR_COB_AUDIT_ACTN_CD_SK",
        F.when(F.col("svMbrCobAuditActnSk") == " ", F.lit("NA"))
         .otherwise(F.col("svMbrCobAuditActnSk"))
    )
    .withColumn(
        "MBR_COB_LAST_VER_METH_CD_SK",
        F.when(F.col("svMbrCobLstVerMethSk") == " ", F.lit("NA"))
         .otherwise(F.col("svMbrCobLstVerMethSk"))
    )
    .withColumn(
        "MBR_COB_OTHR_CAR_ID_CD_SK",
        F.when(F.col("svMbrCobOthrCarIdSk") == " ", F.lit("NA"))
         .otherwise(F.col("svMbrCobOthrCarIdSk"))
    )
    .withColumn(
        "MBR_COB_PAYMT_PRTY_CD_SK",
        F.when(F.col("svMbrCobPaymtPrtySk") == " ", F.lit("NA"))
         .otherwise(F.col("svMbrCobPaymtPrtySk"))
    )
    .withColumn(
        "MBR_COB_TERM_RSN_CD_SK",
        F.when(F.col("svMbrCobTermRsnSk") == " ", F.lit("NA"))
         .otherwise(F.col("svMbrCobTermRsnSk"))
    )
    .withColumn(
        "MBR_COB_TYP_CD_SK",
        F.when(F.col("svMbrCobTypSk") == " ", F.lit("NA"))
         .otherwise(F.col("svMbrCobTypSk"))
    )
    .select(
        F.col("MBR_COB_AUDIT_SK"),
        F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
        F.col("MBR_COB_AUDIT_ROW_ID"),
        F.col("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("svMbrSk").alias("MBR_SK"),
        F.col("svAppUsrSk").alias("SRC_SYS_CRT_USER_SK"),
        F.col("MBR_COB_AUDIT_ACTN_CD_SK"),
        F.col("MBR_COB_LAST_VER_METH_CD_SK"),
        F.col("MBR_COB_OTHR_CAR_ID_CD_SK"),
        F.col("MBR_COB_PAYMT_PRTY_CD_SK"),
        F.col("MBR_COB_TERM_RSN_CD_SK"),
        F.col("MBR_COB_TYP_CD_SK"),
        F.col("MBR_UNIQ_KEY"),
        F.col("svCobLtrTrgrDtSk").alias("COB_LTR_TRGR_DT_SK"),
        F.col("svEffDtSk").alias("EFF_DT_SK"),
        F.col("svLackOfCobInfoStrtDtSk").alias("LACK_OF_COB_INFO_STRT_DT_SK"),
        F.col("svSrcSysCrtDtSk").alias("SRC_SYS_CRT_DT_SK"),
        F.col("svTermDtSk").alias("TERM_DT_SK"),
        F.col("LAST_VERIFIER_TX"),
        F.col("OTHR_CAR_POL_ID")
    )
)

df_lnkRecycle = df_stageVar.filter(F.col("ErrCount") > 0)
df_lnkRecycle = (
    df_lnkRecycle
    .withColumn(
        "JOB_EXCTN_RCRD_ERR_SK",
        GetRecycleKey(F.col("MBR_COB_AUDIT_SK"))
    )
    .withColumn("ERR_CT", F.col("ERR_CT") + F.lit(1))
    .select(
        F.col("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD"),
        F.col("DISCARD_IN"),
        F.col("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT"),
        F.col("ERR_CT"),
        F.col("RECYCLE_CT"),
        F.col("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING"),
        F.col("MBR_COB_AUDIT_SK"),
        F.col("MBR_COB_AUDIT_ROW_ID"),
        F.col("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("MBR_SK"),
        F.col("SRC_SYS_CRT_USER_SK"),
        F.col("MBR_COB_AUDIT_ACTN_CD_SK"),
        F.col("MBR_COB_LAST_VER_METH_CD_SK"),
        F.col("MBR_COB_OTHR_CAR_ID_CD_SK"),
        F.col("MBR_COB_PAYMT_PRTY_CD_SK"),
        F.col("MBR_COB_TERM_RSN_CD_SK"),
        F.col("MBR_COB_TYP_CD_SK"),
        F.col("MBR_UNIQ_KEY"),
        F.col("COB_LTR_TRGR_DT_SK"),
        F.col("EFF_DT_SK"),
        F.col("LACK_OF_COB_INFO_STRT_DT_SK"),
        F.col("SRC_SYS_CRT_DT_SK"),
        F.col("TERM_DT_SK"),
        F.col("LAST_VERIFIER_TX"),
        F.col("OTHR_CAR_POL_ID")
    )
)

df_defaultUNK_base = df_stageVar.limit(1)
df_defaultUNK = df_defaultUNK_base.selectExpr(
    "0 as MBR_COB_AUDIT_SK",
    "0 as SRC_SYS_CD_SK",
    "'UNK' as MBR_COB_AUDIT_ROW_ID",
    "0 as CRT_RUN_CYC_EXCTN_SK",
    "0 as LAST_UPDT_RUN_CYC_EXCTN_SK",
    "0 as MBR_SK",
    "0 as SRC_SYS_CRT_USER_SK",
    "0 as MBR_COB_AUDIT_ACTN_CD_SK",
    "0 as MBR_COB_LAST_VER_METH_CD_SK",
    "0 as MBR_COB_OTHR_CAR_ID_CD_SK",
    "0 as MBR_COB_PAYMT_PRTY_CD_SK",
    "0 as MBR_COB_TERM_RSN_CD_SK",
    "0 as MBR_COB_TYP_CD_SK",
    "0 as MBR_UNIQ_KEY",
    "'UNK' as COB_LTR_TRGR_DT_SK",
    "'UNK' as EFF_DT_SK",
    "'UNK' as LACK_OF_COB_INFO_STRT_DT_SK",
    "'UNK' as SRC_SYS_CRT_DT_SK",
    "'UNK' as TERM_DT_SK",
    "'UNK' as LAST_VERIFIER_TX",
    "'UNK' as OTHR_CAR_POL_ID"
)

df_defaultNA_base = df_stageVar.limit(1)
df_defaultNA = df_defaultNA_base.selectExpr(
    "1 as MBR_COB_AUDIT_SK",
    "1 as SRC_SYS_CD_SK",
    "'NA' as MBR_COB_AUDIT_ROW_ID",
    "1 as CRT_RUN_CYC_EXCTN_SK",
    "1 as LAST_UPDT_RUN_CYC_EXCTN_SK",
    "1 as MBR_SK",
    "1 as SRC_SYS_CRT_USER_SK",
    "1 as MBR_COB_AUDIT_ACTN_CD_SK",
    "1 as MBR_COB_LAST_VER_METH_CD_SK",
    "1 as MBR_COB_OTHR_CAR_ID_CD_SK",
    "1 as MBR_COB_PAYMT_PRTY_CD_SK",
    "1 as MBR_COB_TERM_RSN_CD_SK",
    "1 as MBR_COB_TYP_CD_SK",
    "1 as MBR_UNIQ_KEY",
    "'NA' as COB_LTR_TRGR_DT_SK",
    "'NA' as EFF_DT_SK",
    "'NA' as LACK_OF_COB_INFO_STRT_DT_SK",
    "'NA' as SRC_SYS_CRT_DT_SK",
    "'NA' as TERM_DT_SK",
    "'NA' as LAST_VERIFIER_TX",
    "'NA' as OTHR_CAR_POL_ID"
)

df_recycle_mbr_list = df_stageVar.filter(F.col("ErrCount") > 0).selectExpr(
    "MBR_UNIQ_KEY",
    "'MBR_COB_AUDIT' as TABLE"
)

write_files(
    df_lnkRecycle,
    "hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_parquet=True,
    header=True,
    quote="\"",
    nullValue=None
)

write_files(
    df_recycle_mbr_list,
    f"{adls_path_raw}/landing/FctsMbrRecycleList.MbrRecycle.dat.{RunID}",
    delimiter=",",
    mode="append",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_collector = (
    df_mbrCobAuditOut.select(
        "MBR_COB_AUDIT_SK",
        "SRC_SYS_CD_SK",
        "MBR_COB_AUDIT_ROW_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "MBR_SK",
        "SRC_SYS_CRT_USER_SK",
        "MBR_COB_AUDIT_ACTN_CD_SK",
        "MBR_COB_LAST_VER_METH_CD_SK",
        "MBR_COB_OTHR_CAR_ID_CD_SK",
        "MBR_COB_PAYMT_PRTY_CD_SK",
        "MBR_COB_TERM_RSN_CD_SK",
        "MBR_COB_TYP_CD_SK",
        "MBR_UNIQ_KEY",
        "COB_LTR_TRGR_DT_SK",
        "EFF_DT_SK",
        "LACK_OF_COB_INFO_STRT_DT_SK",
        "SRC_SYS_CRT_DT_SK",
        "TERM_DT_SK",
        "LAST_VERIFIER_TX",
        "OTHR_CAR_POL_ID"
    )
    .unionByName(
        df_defaultUNK.select(
            "MBR_COB_AUDIT_SK",
            "SRC_SYS_CD_SK",
            "MBR_COB_AUDIT_ROW_ID",
            "CRT_RUN_CYC_EXCTN_SK",
            "LAST_UPDT_RUN_CYC_EXCTN_SK",
            "MBR_SK",
            "SRC_SYS_CRT_USER_SK",
            "MBR_COB_AUDIT_ACTN_CD_SK",
            "MBR_COB_LAST_VER_METH_CD_SK",
            "MBR_COB_OTHR_CAR_ID_CD_SK",
            "MBR_COB_PAYMT_PRTY_CD_SK",
            "MBR_COB_TERM_RSN_CD_SK",
            "MBR_COB_TYP_CD_SK",
            "MBR_UNIQ_KEY",
            "COB_LTR_TRGR_DT_SK",
            "EFF_DT_SK",
            "LACK_OF_COB_INFO_STRT_DT_SK",
            "SRC_SYS_CRT_DT_SK",
            "TERM_DT_SK",
            "LAST_VERIFIER_TX",
            "OTHR_CAR_POL_ID"
        )
    )
    .unionByName(
        df_defaultNA.select(
            "MBR_COB_AUDIT_SK",
            "SRC_SYS_CD_SK",
            "MBR_COB_AUDIT_ROW_ID",
            "CRT_RUN_CYC_EXCTN_SK",
            "LAST_UPDT_RUN_CYC_EXCTN_SK",
            "MBR_SK",
            "SRC_SYS_CRT_USER_SK",
            "MBR_COB_AUDIT_ACTN_CD_SK",
            "MBR_COB_LAST_VER_METH_CD_SK",
            "MBR_COB_OTHR_CAR_ID_CD_SK",
            "MBR_COB_PAYMT_PRTY_CD_SK",
            "MBR_COB_TERM_RSN_CD_SK",
            "MBR_COB_TYP_CD_SK",
            "MBR_UNIQ_KEY",
            "COB_LTR_TRGR_DT_SK",
            "EFF_DT_SK",
            "LACK_OF_COB_INFO_STRT_DT_SK",
            "SRC_SYS_CRT_DT_SK",
            "TERM_DT_SK",
            "LAST_VERIFIER_TX",
            "OTHR_CAR_POL_ID"
        )
    )
)

df_collector = (
    df_collector
    .withColumn("COB_LTR_TRGR_DT_SK", F.rpad(F.col("COB_LTR_TRGR_DT_SK"), 20, " "))
    .withColumn("EFF_DT_SK", F.rpad(F.col("EFF_DT_SK"), 20, " "))
    .withColumn("LACK_OF_COB_INFO_STRT_DT_SK", F.rpad(F.col("LACK_OF_COB_INFO_STRT_DT_SK"), 20, " "))
    .withColumn("SRC_SYS_CRT_DT_SK", F.rpad(F.col("SRC_SYS_CRT_DT_SK"), 20, " "))
    .withColumn("TERM_DT_SK", F.rpad(F.col("TERM_DT_SK"), 20, " "))
    .select(
        "MBR_COB_AUDIT_SK",
        "SRC_SYS_CD_SK",
        "MBR_COB_AUDIT_ROW_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "MBR_SK",
        "SRC_SYS_CRT_USER_SK",
        "MBR_COB_AUDIT_ACTN_CD_SK",
        "MBR_COB_LAST_VER_METH_CD_SK",
        "MBR_COB_OTHR_CAR_ID_CD_SK",
        "MBR_COB_PAYMT_PRTY_CD_SK",
        "MBR_COB_TERM_RSN_CD_SK",
        "MBR_COB_TYP_CD_SK",
        "MBR_UNIQ_KEY",
        "COB_LTR_TRGR_DT_SK",
        "EFF_DT_SK",
        "LACK_OF_COB_INFO_STRT_DT_SK",
        "SRC_SYS_CRT_DT_SK",
        "TERM_DT_SK",
        "LAST_VERIFIER_TX",
        "OTHR_CAR_POL_ID"
    )
)

write_files(
    df_collector,
    f"{adls_path}/load/MBR_COB_AUDIT.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)