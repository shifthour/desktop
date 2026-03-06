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
# MAGIC JOB NAME:     IdsMbrEligAuditFkey
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
# MAGIC                             GetFkeyExcd
# MAGIC                             GetFkeyAppUsr
# MAGIC                             GetFKeyDate
# MAGIC 
# MAGIC PROCESSING:
# MAGIC 
# MAGIC 
# MAGIC OUTPUTS:  MBR_ELIG_AUDIT table load file
# MAGIC                      hf_recycle - If recycling was used for this job records would be written to this hash file that had errors.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC            Parikshith Chada  10/18/2006      Originally Programmed
# MAGIC          
# MAGIC 
# MAGIC Developer                Date                 	Project/Altiris #	Change Description                                                                     Development Project      Code Reviewer          Date Reviewed      
# MAGIC ------------------              --------------------	------------------------	-----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------   
# MAGIC Kalyan Neelam        2011-11-04                TTR-456                 Added new column CLS_PLN_SK on end.                                  IntegrateCurDevl          Sandrew                     2011-11-09
# MAGIC                                                                                                  Removed all the data elements

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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DecimalType
from pyspark.sql import functions as F
from pyspark.sql import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


Logging = get_widget_value("Logging","Y")
RunID = get_widget_value("RunID","")
RunCycle = get_widget_value("RunCycle","100")

schema_MbrEligAuditCrf = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38,10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("MBR_ELIG_AUDIT_SK", IntegerType(), nullable=False),
    StructField("MBR_ELIG_AUDIT_ROW_ID", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("EXCD_SK", IntegerType(), nullable=False),
    StructField("MBR_SK", IntegerType(), nullable=False),
    StructField("SRC_SYS_CRT_USER_SK", IntegerType(), nullable=False),
    StructField("MBR_ELIG_AUDIT_ACTN_CD_SK", IntegerType(), nullable=False),
    StructField("MBR_ELIG_CLS_PROD_CAT_CD_SK", IntegerType(), nullable=False),
    StructField("MBR_ELIG_TYP_CD_SK", IntegerType(), nullable=False),
    StructField("VOID_IN", StringType(), nullable=False),
    StructField("EFF_DT_SK", StringType(), nullable=False),
    StructField("SRC_SYS_CRT_DT_SK", StringType(), nullable=False),
    StructField("MBR_UNIQ_KEY", IntegerType(), nullable=False),
    StructField("CSPI_ID", StringType(), nullable=False)
])

df_MbrEligAuditCrf = (
    spark.read
    .option("header", False)
    .option("quote", "\"")
    .schema(schema_MbrEligAuditCrf)
    .csv(f"{adls_path}/key/FctsMbrEligAuditExtr.MbrEligAudit.uniq")
)

w = Window.orderBy(F.lit(1))
df_stagevars = (
    df_MbrEligAuditCrf
    .withColumn("_rownum", F.row_number().over(w))
    .withColumn("SrcSysCdSk", GetFkeyCodes(F.lit("IDS"), F.col("MBR_ELIG_AUDIT_SK"), F.lit("SOURCE SYSTEM"), trim(F.col("SRC_SYS_CD")), Logging))
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("svExcdSk", GetFkeyExcd(F.col("SRC_SYS_CD"), F.col("MBR_ELIG_AUDIT_SK"), trim(F.col("EXCD_SK")), Logging))
    .withColumn("svMbrSk", GetFkeyMbr(F.col("SRC_SYS_CD"), F.col("MBR_ELIG_AUDIT_SK"), F.col("MBR_UNIQ_KEY"), Logging))
    .withColumn("svAppUserSk", GetFkeyAppUsr(F.col("SRC_SYS_CD"), F.col("MBR_ELIG_AUDIT_SK"), F.col("SRC_SYS_CRT_USER_SK"), Logging))
    .withColumn("svMbrEligAuditSk", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("MBR_ELIG_AUDIT_SK"), F.lit("AUDIT ACTION"), F.col("MBR_ELIG_AUDIT_ACTN_CD_SK"), Logging))
    .withColumn("svMbrEligClsSk", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("MBR_ELIG_AUDIT_SK"), F.lit("MEMBER ENROLLMENT CLASS PLAN PRODUCT CATEGORY"), F.col("MBR_ELIG_CLS_PROD_CAT_CD_SK"), Logging))
    .withColumn("svMbrEligTypSk", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("MBR_ELIG_AUDIT_SK"), F.lit("MEMBER ENROLLMENT ELIGIBILITY EVENT TYPE"), F.col("MBR_ELIG_TYP_CD_SK"), Logging))
    .withColumn("svEffDtSk", GetFkeyDate(F.lit("IDS"), F.col("MBR_ELIG_AUDIT_SK"), F.col("EFF_DT_SK"), Logging))
    .withColumn("svSrcSysCrtDtSk", GetFkeyDate(F.lit("IDS"), F.col("MBR_ELIG_AUDIT_SK"), F.col("SRC_SYS_CRT_DT_SK"), Logging))
    .withColumn("svClsPlnSk", GetFkeyClsPln(F.col("SRC_SYS_CD"), F.col("MBR_ELIG_AUDIT_SK"), F.col("CSPI_ID"), Logging))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("MBR_ELIG_AUDIT_SK")))
)

df_mbrEligAuditOut = (
    df_stagevars
    .filter((F.col("ErrCount") == 0) | (F.col("PassThru") == 'Y'))
    .select(
        F.col("MBR_ELIG_AUDIT_SK"),
        F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
        F.col("MBR_ELIG_AUDIT_ROW_ID"),
        F.col("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("svExcdSk").alias("EXCD_SK"),
        F.col("svMbrSk").alias("MBR_SK"),
        F.col("svAppUserSk").alias("SRC_SYS_CRT_USER_SK"),
        F.col("svMbrEligAuditSk").alias("MBR_ELIG_AUDIT_ACTN_CD_SK"),
        F.col("svMbrEligClsSk").alias("MBR_ELIG_CLS_PROD_CAT_CD_SK"),
        F.col("svMbrEligTypSk").alias("MBR_ELIG_TYP_CD_SK"),
        F.col("VOID_IN"),
        F.col("svEffDtSk").alias("EFF_DT_SK"),
        F.col("svSrcSysCrtDtSk").alias("SRC_SYS_CRT_DT_SK"),
        F.col("MBR_UNIQ_KEY"),
        F.col("svClsPlnSk").alias("CLS_PLN_SK")
    )
)

df_lnkRecycle = (
    df_stagevars
    .filter(F.col("ErrCount") > 0)
    .select(
        GetRecycleKey(F.col("MBR_ELIG_AUDIT_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.rpad(F.col("INSRT_UPDT_CD"), 10, ' ').alias("INSRT_UPDT_CD"),
        F.rpad(F.col("DISCARD_IN"), 1, ' ').alias("DISCARD_IN"),
        F.rpad(F.col("PASS_THRU_IN"), 1, ' ').alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT"),
        (F.col("ERR_CT") + F.lit(1)).alias("ERR_CT"),
        F.col("RECYCLE_CT"),
        F.col("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING"),
        F.col("MBR_ELIG_AUDIT_SK"),
        F.col("MBR_ELIG_AUDIT_ROW_ID"),
        F.col("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("EXCD_SK"),
        F.col("MBR_SK"),
        F.col("SRC_SYS_CRT_USER_SK"),
        F.col("MBR_ELIG_AUDIT_ACTN_CD_SK"),
        F.col("MBR_ELIG_CLS_PROD_CAT_CD_SK"),
        F.col("MBR_ELIG_TYP_CD_SK"),
        F.rpad(F.col("VOID_IN"), 1, ' ').alias("VOID_IN"),
        F.rpad(F.col("EFF_DT_SK"), 10, ' ').alias("EFF_DT_SK"),
        F.rpad(F.col("SRC_SYS_CRT_DT_SK"), 10, ' ').alias("SRC_SYS_CRT_DT_SK"),
        F.col("MBR_UNIQ_KEY"),
        F.rpad(F.col("CSPI_ID"), 8, ' ').alias("CSPI_ID")
    )
)

df_DefaultUNK = (
    df_stagevars
    .filter(F.col("_rownum") == 1)
    .select(
        F.lit(0).alias("MBR_ELIG_AUDIT_SK"),
        F.lit(0).alias("SRC_SYS_CD_SK"),
        F.lit("UNK").alias("MBR_ELIG_AUDIT_ROW_ID"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("EXCD_SK"),
        F.lit(0).alias("MBR_SK"),
        F.lit(0).alias("SRC_SYS_CRT_USER_SK"),
        F.lit(0).alias("MBR_ELIG_AUDIT_ACTN_CD_SK"),
        F.lit(0).alias("MBR_ELIG_CLS_PROD_CAT_CD_SK"),
        F.lit(0).alias("MBR_ELIG_TYP_CD_SK"),
        F.rpad(F.lit("U"), 1, ' ').alias("VOID_IN"),
        F.rpad(F.lit("UNK"), 10, ' ').alias("EFF_DT_SK"),
        F.rpad(F.lit("UNK"), 10, ' ').alias("SRC_SYS_CRT_DT_SK"),
        F.lit(0).alias("MBR_UNIQ_KEY"),
        F.lit(0).alias("CLS_PLN_SK")
    )
)

df_DefaultNA = (
    df_stagevars
    .filter(F.col("_rownum") == 1)
    .select(
        F.lit(1).alias("MBR_ELIG_AUDIT_SK"),
        F.lit(1).alias("SRC_SYS_CD_SK"),
        F.lit("NA").alias("MBR_ELIG_AUDIT_ROW_ID"),
        F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("EXCD_SK"),
        F.lit(1).alias("MBR_SK"),
        F.lit(1).alias("SRC_SYS_CRT_USER_SK"),
        F.lit(1).alias("MBR_ELIG_AUDIT_ACTN_CD_SK"),
        F.lit(1).alias("MBR_ELIG_CLS_PROD_CAT_CD_SK"),
        F.lit(1).alias("MBR_ELIG_TYP_CD_SK"),
        F.rpad(F.lit("X"), 1, ' ').alias("VOID_IN"),
        F.rpad(F.lit("NA"), 10, ' ').alias("EFF_DT_SK"),
        F.rpad(F.lit("NA"), 10, ' ').alias("SRC_SYS_CRT_DT_SK"),
        F.lit(1).alias("MBR_UNIQ_KEY"),
        F.lit(1).alias("CLS_PLN_SK")
    )
)

df_recycle_mbr_list = (
    df_stagevars
    .filter(F.col("ErrCount") > 0)
    .select(
        F.col("MBR_UNIQ_KEY"),
        F.lit("MBR_ELIG_AUDIT").alias("TABLE")
    )
)

df_lnkRecycle_selected = df_lnkRecycle.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "MBR_ELIG_AUDIT_SK",
    "MBR_ELIG_AUDIT_ROW_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "EXCD_SK",
    "MBR_SK",
    "SRC_SYS_CRT_USER_SK",
    "MBR_ELIG_AUDIT_ACTN_CD_SK",
    "MBR_ELIG_CLS_PROD_CAT_CD_SK",
    "MBR_ELIG_TYP_CD_SK",
    "VOID_IN",
    "EFF_DT_SK",
    "SRC_SYS_CRT_DT_SK",
    "MBR_UNIQ_KEY",
    "CSPI_ID"
)

write_files(
    df_lnkRecycle_selected,
    "hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_recycle_mbr_list_selected = df_recycle_mbr_list.select("MBR_UNIQ_KEY", "TABLE")
write_files(
    df_recycle_mbr_list_selected,
    f"{adls_path_raw}/landing/FctsMbrRecycleList.MbrRecycle.dat.{RunID}",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_collector = (
    df_mbrEligAuditOut
    .unionByName(df_DefaultUNK)
    .unionByName(df_DefaultNA)
)

df_collector_selected = df_collector.select(
    "MBR_ELIG_AUDIT_SK",
    "SRC_SYS_CD_SK",
    "MBR_ELIG_AUDIT_ROW_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "EXCD_SK",
    "MBR_SK",
    "SRC_SYS_CRT_USER_SK",
    "MBR_ELIG_AUDIT_ACTN_CD_SK",
    "MBR_ELIG_CLS_PROD_CAT_CD_SK",
    "MBR_ELIG_TYP_CD_SK",
    F.rpad(F.col("VOID_IN"), 1, ' ').alias("VOID_IN"),
    F.rpad(F.col("EFF_DT_SK"), 10, ' ').alias("EFF_DT_SK"),
    F.rpad(F.col("SRC_SYS_CRT_DT_SK"), 10, ' ').alias("SRC_SYS_CRT_DT_SK"),
    "MBR_UNIQ_KEY",
    "CLS_PLN_SK"
)

write_files(
    df_collector_selected,
    f"{adls_path}/load/MBR_ELIG_AUDIT.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)