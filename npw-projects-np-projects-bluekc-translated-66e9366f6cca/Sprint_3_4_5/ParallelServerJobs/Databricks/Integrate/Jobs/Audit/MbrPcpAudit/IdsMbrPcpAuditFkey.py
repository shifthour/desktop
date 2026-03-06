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
# MAGIC JOB NAME:     IdsMbrPcpAuditFkey
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
# MAGIC                             GetFkeyProv
# MAGIC                             GetFkeyAppUsr
# MAGIC                             GetFKeyDate
# MAGIC 
# MAGIC PROCESSING:
# MAGIC 
# MAGIC 
# MAGIC OUTPUTS:  MBR_PCP_AUDIT table load file
# MAGIC                      hf_recycle - If recycling was used for this job records would be written to this hash file that had errors.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC            Parikshith Chada  10/19/2006      Originally Programmed

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
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DecimalType
from pyspark.sql.functions import col, lit, row_number, rpad
from pyspark.sql import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


Logging = get_widget_value("Logging","Y")
RunID = get_widget_value("RunID","")
RunCycle = get_widget_value("RunCycle","100")

# -------------------------------------------------------------------------
# Read from MbrPcpAuditCrf (CSeqFileStage)
# -------------------------------------------------------------------------
schema_MbrPcpAuditCrf = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38,10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("MBR_PCP_AUDIT_SK", IntegerType(), nullable=False),
    StructField("MBR_PCP_AUDIT_ROW_ID", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("MBR_SK", IntegerType(), nullable=False),
    StructField("PROV_SK", IntegerType(), nullable=False),
    StructField("SRC_SYS_CRT_USER_SK", IntegerType(), nullable=False),
    StructField("MBR_PCP_AUDIT_ACTN_CD_SK", IntegerType(), nullable=False),
    StructField("SRC_SYS_CRT_DT_SK", StringType(), nullable=False),
    StructField("MBR_UNIQ_KEY", IntegerType(), nullable=False)
])

df_MbrPcpAuditCrf = (
    spark.read
    .option("header", "false")
    .option("sep", ",")
    .option("quote", "\"")
    .schema(schema_MbrPcpAuditCrf)
    .csv(f"{adls_path}/key/FctsMbrPcpAuditExtr.MbrPcpAudit.uniq")
)

# -------------------------------------------------------------------------
# PurgeTrnMbrPcpAudit (CTransformerStage)
# -------------------------------------------------------------------------
df_stagevars = (
    df_MbrPcpAuditCrf
    .withColumn("SrcSysCdSk", GetFkeyCodes(lit("IDS"), col("MBR_PCP_AUDIT_SK"), lit("SOURCE SYSTEM"), trim(col("SRC_SYS_CD")), lit(Logging)))
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn("svMbrSk", GetFkeyMbr(col("SRC_SYS_CD"), col("MBR_PCP_AUDIT_SK"), trim(col("MBR_UNIQ_KEY")), lit(Logging)))
    .withColumn("svProvSk", GetFkeyProv(col("SRC_SYS_CD"), col("MBR_PCP_AUDIT_SK"), col("PROV_SK"), lit(Logging)))
    .withColumn("svAppUsrSk", GetFkeyAppUsr(col("SRC_SYS_CD"), col("MBR_PCP_AUDIT_SK"), col("SRC_SYS_CRT_USER_SK"), lit(Logging)))
    .withColumn("svMbrPcpAuditActnSk", GetFkeyCodes(col("SRC_SYS_CD"), col("MBR_PCP_AUDIT_SK"), lit("AUDIT ACTION"), col("MBR_PCP_AUDIT_ACTN_CD_SK"), lit(Logging)))
    .withColumn("svSrcSysCrtDtSk", GetFkeyDate(lit("IDS"), col("MBR_PCP_AUDIT_SK"), col("SRC_SYS_CRT_DT_SK"), lit(Logging)))
    .withColumn("ErrCount", GetFkeyErrorCnt(col("MBR_PCP_AUDIT_SK")))
)

# Add row number for handling @INROWNUM=1 constraints
w = Window.orderBy(lit(1))
df_stagevars_with_rn = df_stagevars.withColumn("rownum", row_number().over(w))

# Output link "MbrPcpAuditOut" (constraint: ErrCount = 0 Or PassThru = 'Y')
df_MbrPcpAuditOut = (
    df_stagevars_with_rn
    .filter((col("ErrCount") == lit(0)) | (col("PassThru") == lit("Y")))
    .select(
        col("MBR_PCP_AUDIT_SK").alias("MBR_PCP_AUDIT_SK"),
        col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
        col("MBR_PCP_AUDIT_ROW_ID").alias("MBR_PCP_AUDIT_ROW_ID"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("svMbrSk").alias("MBR_SK"),
        col("svProvSk").alias("PROV_SK"),
        col("svAppUsrSk").alias("SRC_SYS_CRT_USER_SK"),
        col("svMbrPcpAuditActnSk").alias("MBR_PCP_AUDIT_ACTN_CD_SK"),
        col("svSrcSysCrtDtSk").alias("SRC_SYS_CRT_DT_SK"),
        col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY")
    )
)

# Output link "lnkRecycle" (constraint: ErrCount > 0)
df_lnkRecycle = (
    df_stagevars_with_rn
    .filter(col("ErrCount") > lit(0))
    .select(
        GetRecycleKey(col("MBR_PCP_AUDIT_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        col("DISCARD_IN").alias("DISCARD_IN"),
        col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        (col("ERR_CT") + lit(1)).alias("ERR_CT"),
        col("RECYCLE_CT").alias("RECYCLE_CT"),
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        col("MBR_PCP_AUDIT_SK").alias("MBR_PCP_AUDIT_SK"),
        col("MBR_PCP_AUDIT_ROW_ID").alias("MBR_PCP_AUDIT_ROW_ID"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("MBR_SK").alias("MBR_SK"),
        col("PROV_SK").alias("PROV_SK"),
        col("SRC_SYS_CRT_USER_SK").alias("SRC_SYS_CRT_USER_SK"),
        col("MBR_PCP_AUDIT_ACTN_CD_SK").alias("MBR_PCP_AUDIT_ACTN_CD_SK"),
        col("SRC_SYS_CRT_DT_SK").alias("SRC_SYS_CRT_DT_SK"),
        col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY")
    )
)

# Output link "DefaultUNK" (constraint: @INROWNUM = 1)
df_DefaultUNK = (
    df_stagevars_with_rn
    .filter(col("rownum") == 1)
    .select(
        lit(0).alias("MBR_PCP_AUDIT_SK"),
        lit(0).alias("SRC_SYS_CD_SK"),
        lit("UNK").alias("MBR_PCP_AUDIT_ROW_ID"),
        lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(0).alias("MBR_SK"),
        lit(0).alias("PROV_SK"),
        lit(0).alias("SRC_SYS_CRT_USER_SK"),
        lit(0).alias("MBR_PCP_AUDIT_ACTN_CD_SK"),
        lit("UNK").alias("SRC_SYS_CRT_DT_SK"),
        lit(0).alias("MBR_UNIQ_KEY")
    )
)

# Output link "DefaultNA" (constraint: @INROWNUM = 1)
df_DefaultNA = (
    df_stagevars_with_rn
    .filter(col("rownum") == 1)
    .select(
        lit(1).alias("MBR_PCP_AUDIT_SK"),
        lit(1).alias("SRC_SYS_CD_SK"),
        lit("NA").alias("MBR_PCP_AUDIT_ROW_ID"),
        lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(1).alias("MBR_SK"),
        lit(1).alias("PROV_SK"),
        lit(1).alias("SRC_SYS_CRT_USER_SK"),
        lit(1).alias("MBR_PCP_AUDIT_ACTN_CD_SK"),
        lit("NA").alias("SRC_SYS_CRT_DT_SK"),
        lit(1).alias("MBR_UNIQ_KEY")
    )
)

# Output link "recycle_mbr_list" (constraint: ErrCount > 0)
df_recycle_mbr_list = (
    df_stagevars_with_rn
    .filter(col("ErrCount") > lit(0))
    .select(
        col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        lit("MBR_PCP_AUDIT").alias("TABLE")
    )
)

# -------------------------------------------------------------------------
# hf_recycle (CHashedFileStage) - Scenario C => write to parquet
# -------------------------------------------------------------------------
write_files(
    df_lnkRecycle,
    "hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

# -------------------------------------------------------------------------
# MbrRecycle (CSeqFileStage)
# -------------------------------------------------------------------------
write_files(
    df_recycle_mbr_list,
    f"{adls_path_raw}/landing/FctsMbrRecycleList.MbrRecycle.dat.{RunID}",
    ",",
    "append",
    False,
    True,
    "\"",
    None
)

# -------------------------------------------------------------------------
# Collector (CCollector) merges MbrPcpAuditOut, DefaultUNK, DefaultNA
# -------------------------------------------------------------------------
col_list = [
    "MBR_PCP_AUDIT_SK",
    "SRC_SYS_CD_SK",
    "MBR_PCP_AUDIT_ROW_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "MBR_SK",
    "PROV_SK",
    "SRC_SYS_CRT_USER_SK",
    "MBR_PCP_AUDIT_ACTN_CD_SK",
    "SRC_SYS_CRT_DT_SK",
    "MBR_UNIQ_KEY"
]

df_collector = (
    df_MbrPcpAuditOut.select(col_list)
    .unionByName(df_DefaultUNK.select(col_list))
    .unionByName(df_DefaultNA.select(col_list))
)

# -------------------------------------------------------------------------
# MbrPcpAudit (CSeqFileStage)
# -------------------------------------------------------------------------
df_final = df_collector.select(
    col("MBR_PCP_AUDIT_SK"),
    col("SRC_SYS_CD_SK"),
    col("MBR_PCP_AUDIT_ROW_ID"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("MBR_SK"),
    col("PROV_SK"),
    col("SRC_SYS_CRT_USER_SK"),
    col("MBR_PCP_AUDIT_ACTN_CD_SK"),
    rpad(col("SRC_SYS_CRT_DT_SK"), 10, " ").alias("SRC_SYS_CRT_DT_SK"),
    col("MBR_UNIQ_KEY")
)

write_files(
    df_final,
    f"{adls_path}/load/MBR_PCP_AUDIT.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)