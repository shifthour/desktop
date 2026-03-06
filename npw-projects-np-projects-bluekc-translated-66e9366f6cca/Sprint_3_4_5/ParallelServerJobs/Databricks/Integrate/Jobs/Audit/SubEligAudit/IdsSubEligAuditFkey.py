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
# MAGIC JOB NAME:     IdsSubEligAuditFkey
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
# MAGIC                             GetFkeyExcd
# MAGIC                             GetFkeyAppUsr
# MAGIC                             GetFKeyDate
# MAGIC 
# MAGIC PROCESSING:
# MAGIC 
# MAGIC 
# MAGIC OUTPUTS:  SUB_ELIG_AUDIT table load file
# MAGIC                      hf_recycle - If recycling was used for this job records would be written to this hash file that had errors.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC            Parikshith Chada  10/26/2006      Originally Programmed
# MAGIC            
# MAGIC Developer                Date                 	Project/Altiris #	Change Description                                                                     Development Project      Code Reviewer          Date Reviewed      
# MAGIC ------------------              --------------------	------------------------	-----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------   
# MAGIC Kalyan Neelam        2011-11-04                TTR-456                 Added new column CLS_PLN_SK on end.                                  IntegrateCurDevl           Sandrew                    2011-11-09
# MAGIC                                                                                                  Removed all the data elements

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
from pyspark.sql.functions import col, lit, rpad, expr, trim
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------

Logging = get_widget_value("Logging","Y")
RunID = get_widget_value("RunID","")
RunCycle = get_widget_value("RunCycle","100")

schema_SubEligAuditCrf = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38,10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("SUB_ELIG_AUDIT_SK", IntegerType(), False),
    StructField("SUB_ELIG_AUDIT_ROW_ID", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("EXCD_SK", IntegerType(), False),
    StructField("SRC_SYS_CRT_USER_SK", IntegerType(), False),
    StructField("SUB_SK", IntegerType(), False),
    StructField("SUB_ELIG_AUDIT_ACTN_CD_SK", IntegerType(), False),
    StructField("SUB_ELIG_CLS_PROD_CAT_CD_SK", IntegerType(), False),
    StructField("SUB_ELIG_TYP_CD_SK", IntegerType(), False),
    StructField("VOID_IN", StringType(), False),
    StructField("EFF_DT_SK", StringType(), False),
    StructField("SRC_SYS_CRT_DT_SK", StringType(), False),
    StructField("SUB_UNIQ_KEY", IntegerType(), False),
    StructField("CSPI_ID", StringType(), False)
])

df_SubEligAuditCrf = (
    spark.read
    .option("sep", ",")
    .option("quote", "\"")
    .schema(schema_SubEligAuditCrf)
    .csv(f"{adls_path}/key/FctsSubEligAuditExtr.SubEligAudit.uniq")
)

df_purgeTrnSubEligAudit = (
    df_SubEligAuditCrf
    .withColumn("Logging", lit(Logging))
    .withColumn("svPassThru", col("PASS_THRU_IN"))
    .withColumn("svErrCount", GetFkeyErrorCnt(col("SUB_ELIG_AUDIT_SK")))
    .withColumn("SrcSysCdSk", GetFkeyCodes(lit("IDS"), col("SUB_ELIG_AUDIT_SK"), lit("SOURCE SYSTEM"), trim(col("SRC_SYS_CD")), col("Logging")))
    .withColumn("svExcdSk", GetFkeyExcd(col("SRC_SYS_CD"), col("SUB_ELIG_AUDIT_SK"), trim(col("EXCD_SK")), col("Logging")))
    .withColumn("svAppUsrSk", GetFkeyAppUsr(col("SRC_SYS_CD"), col("SUB_ELIG_AUDIT_SK"), col("SRC_SYS_CRT_USER_SK"), col("Logging")))
    .withColumn("svSubSk", GetFkeySub(col("SRC_SYS_CD"), col("SUB_ELIG_AUDIT_SK"), col("SUB_SK"), col("Logging")))
    .withColumn("svSubEligAuditSk", GetFkeyCodes(col("SRC_SYS_CD"), col("SUB_ELIG_AUDIT_SK"), lit("AUDIT ACTION"), col("SUB_ELIG_AUDIT_ACTN_CD_SK"), col("Logging")))
    .withColumn("svSubEligClsSk", GetFkeyCodes(col("SRC_SYS_CD"), col("SUB_ELIG_AUDIT_SK"), lit("MEMBER ENROLLMENT CLASS PLAN PRODUCT CATEGORY"), col("SUB_ELIG_CLS_PROD_CAT_CD_SK"), col("Logging")))
    .withColumn("svSubEligTypSk", GetFkeyCodes(col("SRC_SYS_CD"), col("SUB_ELIG_AUDIT_SK"), lit("MEMBER ENROLLMENT ELIGIBILITY EVENT TYPE"), col("SUB_ELIG_TYP_CD_SK"), col("Logging")))
    .withColumn("svEffDtSk", GetFkeyDate(lit("IDS"), col("SUB_ELIG_AUDIT_SK"), col("EFF_DT_SK"), col("Logging")))
    .withColumn("svSrcSysCrtDtSk", GetFkeyDate(lit("IDS"), col("SUB_ELIG_AUDIT_SK"), col("SRC_SYS_CRT_DT_SK"), col("Logging")))
    .withColumn("svClsPlnSk", GetFkeyClsPln(col("SRC_SYS_CD"), col("SUB_ELIG_AUDIT_SK"), col("CSPI_ID"), col("Logging")))
)

df_SubEligAuditOut = (
    df_purgeTrnSubEligAudit
    .filter((col("svErrCount") == lit(0)) | (col("svPassThru") == lit("Y")))
    .select(
        col("SUB_ELIG_AUDIT_SK").alias("SUB_ELIG_AUDIT_SK"),
        col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
        col("SUB_ELIG_AUDIT_ROW_ID").alias("SUB_ELIG_AUDIT_ROW_ID"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("svExcdSk").alias("EXCD_SK"),
        col("svAppUsrSk").alias("SRC_SYS_CRT_USER_SK"),
        col("svSubSk").alias("SUB_SK"),
        col("svSubEligAuditSk").alias("SUB_ELIG_AUDIT_ACTN_CD_SK"),
        col("svSubEligClsSk").alias("SUB_ELIG_CLS_PROD_CAT_CD_SK"),
        col("svSubEligTypSk").alias("SUB_ELIG_TYP_CD_SK"),
        col("VOID_IN").alias("VOID_IN"),
        expr("CASE WHEN length(trim(svEffDtSk))=0 THEN 'NA' ELSE svEffDtSk END").alias("EFF_DT_SK"),
        col("svSrcSysCrtDtSk").alias("SRC_SYS_CRT_DT_SK"),
        col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
        col("svClsPlnSk").alias("CLS_PLN_SK")
    )
)

df_lnkRecycle = (
    df_purgeTrnSubEligAudit
    .filter(col("svErrCount") > lit(0))
    .select(
        GetRecycleKey(col("SUB_ELIG_AUDIT_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        col("INSRT_UPDT_CD"),
        col("DISCARD_IN"),
        col("PASS_THRU_IN"),
        col("FIRST_RECYC_DT"),
        (col("ERR_CT") + lit(1)).alias("ERR_CT"),
        col("RECYCLE_CT"),
        col("SRC_SYS_CD"),
        col("PRI_KEY_STRING"),
        col("SUB_ELIG_AUDIT_SK"),
        col("SUB_ELIG_AUDIT_ROW_ID"),
        col("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("EXCD_SK"),
        col("SRC_SYS_CRT_USER_SK"),
        col("SUB_SK"),
        col("SUB_ELIG_AUDIT_ACTN_CD_SK"),
        col("SUB_ELIG_CLS_PROD_CAT_CD_SK"),
        col("SUB_ELIG_TYP_CD_SK"),
        col("VOID_IN"),
        col("EFF_DT_SK"),
        col("SRC_SYS_CRT_DT_SK"),
        col("SUB_UNIQ_KEY"),
        col("CSPI_ID")
    )
)

df_recycle_sub_list = (
    df_purgeTrnSubEligAudit
    .filter(col("svErrCount") > lit(0))
    .select(
        col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
        lit("SUB_ELIG_AUDIT").alias("TABLE")
    )
)

df_defaultUNK_temp = df_purgeTrnSubEligAudit.limit(1)
df_defaultUNK = df_defaultUNK_temp.select(
    lit(0).alias("SUB_ELIG_AUDIT_SK"),
    lit(0).alias("SRC_SYS_CD_SK"),
    lit("UNK").alias("SUB_ELIG_AUDIT_ROW_ID"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("EXCD_SK"),
    lit(0).alias("SRC_SYS_CRT_USER_SK"),
    lit(0).alias("SUB_SK"),
    lit(0).alias("SUB_ELIG_AUDIT_ACTN_CD_SK"),
    lit(0).alias("SUB_ELIG_CLS_PROD_CAT_CD_SK"),
    lit(0).alias("SUB_ELIG_TYP_CD_SK"),
    lit("U").alias("VOID_IN"),
    lit("UNK").alias("EFF_DT_SK"),
    lit("UNK").alias("SRC_SYS_CRT_DT_SK"),
    lit(0).alias("SUB_UNIQ_KEY"),
    lit(0).alias("CLS_PLN_SK")
)

df_defaultNA_temp = df_purgeTrnSubEligAudit.limit(1)
df_defaultNA = df_defaultNA_temp.select(
    lit(1).alias("SUB_ELIG_AUDIT_SK"),
    lit(1).alias("SRC_SYS_CD_SK"),
    lit("NA").alias("SUB_ELIG_AUDIT_ROW_ID"),
    lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(1).alias("EXCD_SK"),
    lit(1).alias("SRC_SYS_CRT_USER_SK"),
    lit(1).alias("SUB_SK"),
    lit(1).alias("SUB_ELIG_AUDIT_ACTN_CD_SK"),
    lit(1).alias("SUB_ELIG_CLS_PROD_CAT_CD_SK"),
    lit(1).alias("SUB_ELIG_TYP_CD_SK"),
    lit("X").alias("VOID_IN"),
    lit("NA").alias("EFF_DT_SK"),
    lit("NA").alias("SRC_SYS_CRT_DT_SK"),
    lit(1).alias("SUB_UNIQ_KEY"),
    lit(1).alias("CLS_PLN_SK")
)

df_collector = (
    df_SubEligAuditOut
    .unionByName(df_defaultUNK)
    .unionByName(df_defaultNA)
)

df_collector_final = df_collector.select(
    col("SUB_ELIG_AUDIT_SK"),
    col("SRC_SYS_CD_SK"),
    col("SUB_ELIG_AUDIT_ROW_ID"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("EXCD_SK"),
    col("SRC_SYS_CRT_USER_SK"),
    col("SUB_SK"),
    col("SUB_ELIG_AUDIT_ACTN_CD_SK"),
    col("SUB_ELIG_CLS_PROD_CAT_CD_SK"),
    col("SUB_ELIG_TYP_CD_SK"),
    rpad(col("VOID_IN"), 1, " ").alias("VOID_IN"),
    rpad(col("EFF_DT_SK"), 10, " ").alias("EFF_DT_SK"),
    rpad(col("SRC_SYS_CRT_DT_SK"), 10, " ").alias("SRC_SYS_CRT_DT_SK"),
    col("SUB_UNIQ_KEY"),
    col("CLS_PLN_SK")
)

df_hf_recycle = df_lnkRecycle.select(
    col("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    col("SRC_SYS_CD"),
    col("PRI_KEY_STRING"),
    col("SUB_ELIG_AUDIT_SK"),
    col("SUB_ELIG_AUDIT_ROW_ID"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("EXCD_SK"),
    col("SRC_SYS_CRT_USER_SK"),
    col("SUB_SK"),
    col("SUB_ELIG_AUDIT_ACTN_CD_SK"),
    col("SUB_ELIG_CLS_PROD_CAT_CD_SK"),
    col("SUB_ELIG_TYP_CD_SK"),
    rpad(col("VOID_IN"), 1, " ").alias("VOID_IN"),
    rpad(col("EFF_DT_SK"), 10, " ").alias("EFF_DT_SK"),
    rpad(col("SRC_SYS_CRT_DT_SK"), 10, " ").alias("SRC_SYS_CRT_DT_SK"),
    col("SUB_UNIQ_KEY"),
    rpad(col("CSPI_ID"), 8, " ").alias("CSPI_ID")
)

write_files(
    df_hf_recycle,
    f"{adls_path}/hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

write_files(
    df_recycle_sub_list,
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
    f"{adls_path}/load/SUB_ELIG_AUDIT.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)