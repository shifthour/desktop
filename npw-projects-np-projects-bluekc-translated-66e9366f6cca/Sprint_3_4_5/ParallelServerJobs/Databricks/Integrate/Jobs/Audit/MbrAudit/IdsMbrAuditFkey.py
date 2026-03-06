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
# MAGIC JOB NAME:     IdsMbrAuditFkey
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
# MAGIC OUTPUTS:  MBR_AUDIT table load file
# MAGIC                      hf_recycle - If recycling was used for this job records would be written to this hash file that had errors.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC            Parikshith Chada  10/24/2006      Originally Programmed

# MAGIC Read common record format file from extract job.
# MAGIC Set all foreign surrogate keys
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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DecimalType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


Logging = get_widget_value("Logging","Y")
RunID = get_widget_value("RunID","")
RunCycle = get_widget_value("RunCycle","100")

# 1) Read from MbrAuditCrf (CSeqFileStage)
schema_MbrAuditCrf = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(20,10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("MBR_AUDIT_SK", IntegerType(), nullable=False),
    StructField("MBR_AUDIT_ROW_ID", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("MBR_SK", IntegerType(), nullable=False),
    StructField("SRC_SYS_CRT_USER_SK", IntegerType(), nullable=False),
    StructField("MBR_AUDIT_ACTN_CD_SK", IntegerType(), nullable=False),
    StructField("MBR_GNDR_CD_SK", IntegerType(), nullable=False),
    StructField("SCRD_IN", StringType(), nullable=False),
    StructField("BRTH_DT_SK", StringType(), nullable=False),
    StructField("PREX_COND_EFF_DT_SK", StringType(), nullable=False),
    StructField("SRC_SYS_CRT_DT_SK", StringType(), nullable=False),
    StructField("MBR_UNIQ_KEY", IntegerType(), nullable=False),
    StructField("FIRST_NM", StringType(), nullable=True),
    StructField("MIDINIT", StringType(), nullable=True),
    StructField("LAST_NM", StringType(), nullable=True),
    StructField("SSN", StringType(), nullable=False)
])

df_MbrAuditCrf = (
    spark.read
    .option("header", False)
    .option("sep", ",")
    .option("quote", "\"")
    .schema(schema_MbrAuditCrf)
    .csv(f"{adls_path}/key/FctsMbrAuditExtr.MbrAudit.uniq")
)

# 2) Transformer: PurgeTrnMbrAudit (CTransformerStage)
df_WithVars = (
    df_MbrAuditCrf
    .withColumn("SrcSysCdSk", GetFkeyCodes(F.lit("IDS"), F.col("MBR_AUDIT_SK"), F.lit("SOURCE SYSTEM"), trim(F.col("SRC_SYS_CD")), F.lit(Logging)))
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("svMbrSk", GetFkeyMbr(F.col("SRC_SYS_CD"), F.col("MBR_AUDIT_SK"), F.col("MBR_UNIQ_KEY"), F.lit(Logging)))
    .withColumn("svAppUserSk", GetFkeyAppUsr(F.col("SRC_SYS_CD"), F.col("MBR_AUDIT_SK"), F.col("SRC_SYS_CRT_USER_SK"), F.lit(Logging)))
    .withColumn("svMbrAuditActnSk", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("MBR_AUDIT_SK"), F.lit("AUDIT ACTION"), F.col("MBR_AUDIT_ACTN_CD_SK"), F.lit(Logging)))
    .withColumn("svMbrGndrSk", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("MBR_AUDIT_SK"), F.lit("GENDER"), F.col("MBR_GNDR_CD_SK"), F.lit(Logging)))
    .withColumn("svBrthDtSk", GetFkeyDate(F.lit("IDS"), F.col("MBR_AUDIT_SK"), F.col("BRTH_DT_SK"), F.lit(Logging)))
    .withColumn("svPrexCondEffDtSk", GetFkeyDate(F.lit("IDS"), F.col("MBR_AUDIT_SK"), F.col("PREX_COND_EFF_DT_SK"), F.lit(Logging)))
    .withColumn("svSrcSysCrtDtSk", GetFkeyDate(F.lit("IDS"), F.col("MBR_AUDIT_SK"), F.col("SRC_SYS_CRT_DT_SK"), F.lit(Logging)))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("MBR_AUDIT_SK")))
)

# Outputs from PurgeTrnMbrAudit:

# (a) MbrAuditOut => Constraint: ErrCount = 0 Or PassThru = 'Y'
df_MbrAuditOut = (
    df_WithVars
    .filter((F.col("ErrCount") == 0) | (F.col("PassThru") == "Y"))
    .withColumn("MBR_AUDIT_ACTN_CD_SK",
                F.when(F.col("svMbrAuditActnSk") == " ", F.lit("NA"))
                 .otherwise(F.col("svMbrAuditActnSk")))
    .withColumn("MBR_GNDR_CD_SK",
                F.when(F.col("svMbrGndrSk") == " ", F.lit("NA"))
                 .otherwise(F.col("svMbrGndrSk")))
    .withColumn("BRTH_DT_SK",
                F.when(F.length(trim(F.col("svBrthDtSk"))) == 0, F.lit("NA"))
                 .otherwise(F.col("svBrthDtSk")))
    .select(
        F.col("MBR_AUDIT_SK").alias("MBR_AUDIT_SK"),
        F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
        F.col("MBR_AUDIT_ROW_ID").alias("MBR_AUDIT_ROW_ID"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("svMbrSk").alias("MBR_SK"),
        F.col("svAppUserSk").alias("SRC_SYS_CRT_USER_SK"),
        F.col("MBR_AUDIT_ACTN_CD_SK").alias("MBR_AUDIT_ACTN_CD_SK"),
        F.col("MBR_GNDR_CD_SK").alias("MBR_GNDR_CD_SK"),
        F.col("SCRD_IN").alias("SCRD_IN"),
        F.col("BRTH_DT_SK").alias("BRTH_DT_SK"),
        F.col("svPrexCondEffDtSk").alias("PREX_COND_EFF_DT_SK"),
        F.col("svSrcSysCrtDtSk").alias("SRC_SYS_CRT_DT_SK"),
        F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.col("FIRST_NM").alias("FIRST_NM"),
        F.col("MIDINIT").alias("MIDINIT"),
        F.col("LAST_NM").alias("LAST_NM"),
        F.col("SSN").alias("SSN")
    )
)

# (b) lnkRecycle => Constraint: ErrCount > 0
df_lnkRecycle = (
    df_WithVars
    .filter(F.col("ErrCount") > 0)
    .select(
        GetRecycleKey(F.col("MBR_AUDIT_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("DISCARD_IN").alias("DISCARD_IN"),
        F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        (F.col("ERR_CT") + F.lit(1)).alias("ERR_CT"),
        F.col("RECYCLE_CT").alias("RECYCLE_CT"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("MBR_AUDIT_SK").alias("MBR_AUDIT_SK"),
        F.col("MBR_AUDIT_ROW_ID").alias("MBR_AUDIT_ROW_ID"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("MBR_SK").alias("MBR_SK"),
        F.col("SRC_SYS_CRT_USER_SK").alias("SRC_SYS_CRT_USER_SK"),
        F.col("MBR_AUDIT_ACTN_CD_SK").alias("MBR_AUDIT_ACTN_CD_SK"),
        F.col("MBR_GNDR_CD_SK").alias("MBR_GNDR_CD_SK"),
        F.col("SCRD_IN").alias("SCRD_IN"),
        F.col("BRTH_DT_SK").alias("BRTH_DT_SK"),
        F.col("PREX_COND_EFF_DT_SK").alias("PREX_COND_EFF_DT_SK"),
        F.col("SRC_SYS_CRT_DT_SK").alias("SRC_SYS_CRT_DT_SK"),
        F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.col("FIRST_NM").alias("FIRST_NM"),
        F.col("MIDINIT").alias("MIDINIT"),
        F.col("LAST_NM").alias("LAST_NM"),
        F.col("SSN").alias("SSN")
    )
)

# (c) DefaultUNK => Constraint: @INROWNUM = 1 (single row of constants)
df_DefaultUNK = spark.createDataFrame(
    [
        (
            0, 0, "UNK", 0, 0, 0, 0, 0, 0, "U",
            "UNK", "UNK", "UNK", 0, "UNK", "U",
            "UNK", "UNK"
        )
    ],
    StructType([
        StructField("MBR_AUDIT_SK", IntegerType(), True),
        StructField("SRC_SYS_CD_SK", IntegerType(), True),
        StructField("MBR_AUDIT_ROW_ID", StringType(), True),
        StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
        StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
        StructField("MBR_SK", IntegerType(), True),
        StructField("SRC_SYS_CRT_USER_SK", IntegerType(), True),
        StructField("MBR_AUDIT_ACTN_CD_SK", IntegerType(), True),
        StructField("MBR_GNDR_CD_SK", IntegerType(), True),
        StructField("SCRD_IN", StringType(), True),
        StructField("BRTH_DT_SK", StringType(), True),
        StructField("PREX_COND_EFF_DT_SK", StringType(), True),
        StructField("SRC_SYS_CRT_DT_SK", StringType(), True),
        StructField("MBR_UNIQ_KEY", IntegerType(), True),
        StructField("FIRST_NM", StringType(), True),
        StructField("MIDINIT", StringType(), True),
        StructField("LAST_NM", StringType(), True),
        StructField("SSN", StringType(), True)
    ])
)

# (d) DefaultNA => Constraint: @INROWNUM = 1 (single row of constants)
df_DefaultNA = spark.createDataFrame(
    [
        (
            1, 1, "NA", 1, 1, 1, 1, 1, 1, "X",
            "NA", "NA", "NA", 1, "NA", "X",
            "NA", "NA"
        )
    ],
    StructType([
        StructField("MBR_AUDIT_SK", IntegerType(), True),
        StructField("SRC_SYS_CD_SK", IntegerType(), True),
        StructField("MBR_AUDIT_ROW_ID", StringType(), True),
        StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
        StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
        StructField("MBR_SK", IntegerType(), True),
        StructField("SRC_SYS_CRT_USER_SK", IntegerType(), True),
        StructField("MBR_AUDIT_ACTN_CD_SK", IntegerType(), True),
        StructField("MBR_GNDR_CD_SK", IntegerType(), True),
        StructField("SCRD_IN", StringType(), True),
        StructField("BRTH_DT_SK", StringType(), True),
        StructField("PREX_COND_EFF_DT_SK", StringType(), True),
        StructField("SRC_SYS_CRT_DT_SK", StringType(), True),
        StructField("MBR_UNIQ_KEY", IntegerType(), True),
        StructField("FIRST_NM", StringType(), True),
        StructField("MIDINIT", StringType(), True),
        StructField("LAST_NM", StringType(), True),
        StructField("SSN", StringType(), True)
    ])
)

# (e) recycle_mbr_list => Constraint: ErrCount > 0
df_recycle_mbr_list = (
    df_WithVars
    .filter(F.col("ErrCount") > 0)
    .select(
        F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.lit("MBR_AUDIT").alias("TABLE")
    )
)

# 3) hf_recycle (CHashedFileStage) => Scenario C => write parquet
df_hf_recycle = (
    df_lnkRecycle
    .withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " "))
    .withColumn("SCRD_IN", F.rpad(F.col("SCRD_IN"), 1, " "))
    .withColumn("BRTH_DT_SK", F.rpad(F.col("BRTH_DT_SK"), 10, " "))
    .withColumn("PREX_COND_EFF_DT_SK", F.rpad(F.col("PREX_COND_EFF_DT_SK"), 10, " "))
    .withColumn("SRC_SYS_CRT_DT_SK", F.rpad(F.col("SRC_SYS_CRT_DT_SK"), 10, " "))
    .withColumn("MIDINIT", F.rpad(F.col("MIDINIT"), 1, " "))
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
        "MBR_AUDIT_SK",
        "MBR_AUDIT_ROW_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "MBR_SK",
        "SRC_SYS_CRT_USER_SK",
        "MBR_AUDIT_ACTN_CD_SK",
        "MBR_GNDR_CD_SK",
        "SCRD_IN",
        "BRTH_DT_SK",
        "PREX_COND_EFF_DT_SK",
        "SRC_SYS_CRT_DT_SK",
        "MBR_UNIQ_KEY",
        "FIRST_NM",
        "MIDINIT",
        "LAST_NM",
        "SSN"
    )
)
write_files(
    df_hf_recycle,
    "hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# 4) MbrRecycle (CSeqFileStage)
df_recycle_mbr_list_final = df_recycle_mbr_list.select("MBR_UNIQ_KEY", "TABLE")
write_files(
    df_recycle_mbr_list_final,
    f"{adls_path_raw}/landing/FctsMbrRecycleList.MbrRecycle.dat.{RunID}",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# 5) Collector (CCollector)
df_MbrAuditOut_sel = df_MbrAuditOut.select(
    "MBR_AUDIT_SK",
    "SRC_SYS_CD_SK",
    "MBR_AUDIT_ROW_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "MBR_SK",
    "SRC_SYS_CRT_USER_SK",
    "MBR_AUDIT_ACTN_CD_SK",
    "MBR_GNDR_CD_SK",
    "SCRD_IN",
    "BRTH_DT_SK",
    "PREX_COND_EFF_DT_SK",
    "SRC_SYS_CRT_DT_SK",
    "MBR_UNIQ_KEY",
    "FIRST_NM",
    "MIDINIT",
    "LAST_NM",
    "SSN"
)

df_DefaultUNK_sel = df_DefaultUNK.select(
    "MBR_AUDIT_SK",
    "SRC_SYS_CD_SK",
    "MBR_AUDIT_ROW_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "MBR_SK",
    "SRC_SYS_CRT_USER_SK",
    "MBR_AUDIT_ACTN_CD_SK",
    "MBR_GNDR_CD_SK",
    "SCRD_IN",
    "BRTH_DT_SK",
    "PREX_COND_EFF_DT_SK",
    "SRC_SYS_CRT_DT_SK",
    "MBR_UNIQ_KEY",
    "FIRST_NM",
    "MIDINIT",
    "LAST_NM",
    "SSN"
)

df_DefaultNA_sel = df_DefaultNA.select(
    "MBR_AUDIT_SK",
    "SRC_SYS_CD_SK",
    "MBR_AUDIT_ROW_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "MBR_SK",
    "SRC_SYS_CRT_USER_SK",
    "MBR_AUDIT_ACTN_CD_SK",
    "MBR_GNDR_CD_SK",
    "SCRD_IN",
    "BRTH_DT_SK",
    "PREX_COND_EFF_DT_SK",
    "SRC_SYS_CRT_DT_SK",
    "MBR_UNIQ_KEY",
    "FIRST_NM",
    "MIDINIT",
    "LAST_NM",
    "SSN"
)

df_collector = df_MbrAuditOut_sel.unionByName(df_DefaultUNK_sel).unionByName(df_DefaultNA_sel)

# 6) MbrAudit (CSeqFileStage)
df_mbrAudit_final = (
    df_collector
    .withColumn("SCRD_IN", F.rpad(F.col("SCRD_IN"), 1, " "))
    .withColumn("BRTH_DT_SK", F.rpad(F.col("BRTH_DT_SK"), 10, " "))
    .withColumn("PREX_COND_EFF_DT_SK", F.rpad(F.col("PREX_COND_EFF_DT_SK"), 10, " "))
    .withColumn("SRC_SYS_CRT_DT_SK", F.rpad(F.col("SRC_SYS_CRT_DT_SK"), 10, " "))
    .withColumn("MIDINIT", F.rpad(F.col("MIDINIT"), 1, " "))
    .select(
        "MBR_AUDIT_SK",
        "SRC_SYS_CD_SK",
        "MBR_AUDIT_ROW_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "MBR_SK",
        "SRC_SYS_CRT_USER_SK",
        "MBR_AUDIT_ACTN_CD_SK",
        "MBR_GNDR_CD_SK",
        "SCRD_IN",
        "BRTH_DT_SK",
        "PREX_COND_EFF_DT_SK",
        "SRC_SYS_CRT_DT_SK",
        "MBR_UNIQ_KEY",
        "FIRST_NM",
        "MIDINIT",
        "LAST_NM",
        "SSN"
    )
)

write_files(
    df_mbrAudit_final,
    f"{adls_path}/load/MBR_AUDIT.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)