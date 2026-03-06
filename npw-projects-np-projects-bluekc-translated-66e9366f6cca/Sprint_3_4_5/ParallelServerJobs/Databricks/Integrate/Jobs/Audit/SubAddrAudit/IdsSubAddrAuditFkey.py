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
# MAGIC JOB NAME:     IdsSubAddrAuditFkey
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
# MAGIC OUTPUTS:  SUB_ADDR_AUDIT table load file
# MAGIC                      hf_recycle - If recycling was used for this job records would be written to this hash file that had errors.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC            Parikshith Chada  10/20/2006      Originally Programmed

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
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
from pyspark.sql import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


Logging = get_widget_value("Logging","Y")
RunID = get_widget_value("RunID","2006102512345")
RunCycle = get_widget_value("RunCycle","100")

schema_subaddr_audit_crf = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38,10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("SUB_ADDR_AUDIT_SK", IntegerType(), nullable=False),
    StructField("SUB_ADDR_AUDIT_ROW_ID", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("SRC_SYS_CRT_USER_SK", IntegerType(), nullable=False),
    StructField("SUB_SK", IntegerType(), nullable=False),
    StructField("SUB_ADDR_AUDIT_ACTN_CD_SK", IntegerType(), nullable=False),
    StructField("SUB_ADDR_CD_SK", IntegerType(), nullable=False),
    StructField("SRC_SYS_CRT_DT_SK", StringType(), nullable=False),
    StructField("SUB_UNIQ_KEY", IntegerType(), nullable=False),
    StructField("ADDR_LN_1", StringType(), nullable=True),
    StructField("ADDR_LN_2", StringType(), nullable=True),
    StructField("ADDR_LN_3", StringType(), nullable=True),
    StructField("CITY_NM", StringType(), nullable=True),
    StructField("SUB_ADDR_ST_CD_SK", IntegerType(), nullable=False),
    StructField("POSTAL_CD", StringType(), nullable=True),
    StructField("CNTY_NM", StringType(), nullable=True),
    StructField("SUB_ADDR_CTRY_CD_SK", IntegerType(), nullable=False),
    StructField("PHN_NO", StringType(), nullable=True),
])

df_subaddr_audit_crf = (
    spark.read
    .option("header", "false")
    .option("quote", "\"")
    .option("delimiter", ",")
    .schema(schema_subaddr_audit_crf)
    .csv(f"{adls_path}/key/FctsSubAddrAuditExtr.SubAddrAudit.uniq")
)

df_purgeTrnSubAddrAudit = (
    df_subaddr_audit_crf
    .withColumn("SrcSysCdSk", GetFkeyCodes("IDS", F.col("SUB_ADDR_AUDIT_SK"), "SOURCE SYSTEM", trim(F.col("SRC_SYS_CD")), Logging))
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("svAppUser", GetFkeyAppUsr(F.col("SRC_SYS_CD"), F.col("SUB_ADDR_AUDIT_SK"), trim(F.col("SRC_SYS_CRT_USER_SK")), Logging))
    .withColumn("svSubSk", GetFkeySub(F.col("SRC_SYS_CD"), F.col("SUB_ADDR_AUDIT_SK"), F.col("SUB_UNIQ_KEY"), Logging))
    .withColumn("svSubAddrAuditSk", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("SUB_ADDR_AUDIT_SK"), "AUDIT ACTION", F.col("SUB_ADDR_AUDIT_ACTN_CD_SK"), Logging))
    .withColumn("svSubAddrCdSk", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("SUB_ADDR_AUDIT_SK"), "SUBSCRIBER ADDRESS", F.col("SUB_ADDR_CD_SK"), Logging))
    .withColumn("svSrcSysCrtDtSk", GetFkeyDate("IDS", F.col("SUB_ADDR_AUDIT_SK"), F.col("SRC_SYS_CRT_DT_SK"), Logging))
    .withColumn("svSubAddrStSk", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("SUB_ADDR_AUDIT_SK"), "STATE", F.col("SUB_ADDR_ST_CD_SK"), Logging))
    .withColumn("svSubAddrCtrySk", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("SUB_ADDR_AUDIT_SK"), "COUNTRY", trim(F.col("SUB_ADDR_CTRY_CD_SK")), Logging))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("SUB_ADDR_AUDIT_SK")))
)

w = Window.orderBy(F.lit(1))
df_purgeTrnSubAddrAudit = df_purgeTrnSubAddrAudit.withColumn("row_num", F.row_number().over(w))

df_SubAddrAuditOut = (
    df_purgeTrnSubAddrAudit
    .filter((F.col("ErrCount") == 0) | (F.col("PassThru") == F.lit("Y")))
    .select(
        F.col("SUB_ADDR_AUDIT_SK").alias("SUB_ADDR_AUDIT_SK"),
        F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
        F.col("SUB_ADDR_AUDIT_ROW_ID").alias("SUB_ADDR_AUDIT_ROW_ID"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("svAppUser").alias("SRC_SYS_CRT_USER_SK"),
        F.col("svSubSk").alias("SUB_SK"),
        F.when(F.col("svSubAddrAuditSk") == F.lit(" "), F.lit("NA")).otherwise(F.col("svSubAddrAuditSk")).alias("SUB_ADDR_AUDIT_ACTN_CD_SK"),
        F.when(F.col("svSubAddrCdSk") == F.lit(" "), F.lit("NA")).otherwise(F.col("svSubAddrCdSk")).alias("SUB_ADDR_CD_SK"),
        F.col("svSrcSysCrtDtSk").alias("SRC_SYS_CRT_DT_SK"),
        F.col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
        F.col("ADDR_LN_1").alias("ADDR_LN_1"),
        F.col("ADDR_LN_2").alias("ADDR_LN_2"),
        F.col("ADDR_LN_3").alias("ADDR_LN_3"),
        F.col("CITY_NM").alias("CITY_NM"),
        F.when(F.col("svSubAddrStSk") == F.lit(" "), F.lit("NA")).otherwise(F.col("svSubAddrStSk")).alias("SUB_ADDR_ST_CD_SK"),
        F.col("POSTAL_CD").alias("POSTAL_CD"),
        F.col("CNTY_NM").alias("CNTY_NM"),
        F.col("svSubAddrCtrySk").alias("SUB_ADDR_CTRY_CD_SK"),
        F.col("PHN_NO").alias("PHN_NO")
    )
)

df_lnkRecycle = (
    df_purgeTrnSubAddrAudit
    .filter(F.col("ErrCount") > 0)
    .select(
        GetRecycleKey(F.col("SUB_ADDR_AUDIT_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("DISCARD_IN").alias("DISCARD_IN"),
        F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        (F.col("ERR_CT") + F.lit(1)).alias("ERR_CT"),
        F.col("RECYCLE_CT").alias("RECYCLE_CT"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("SUB_ADDR_AUDIT_SK").alias("SUB_ADDR_AUDIT_SK"),
        F.col("SUB_ADDR_AUDIT_ROW_ID").alias("SUB_ADDR_AUDIT_ROW_ID"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("SRC_SYS_CRT_USER_SK").alias("SRC_SYS_CRT_USER_SK"),
        F.col("SUB_SK").alias("SUB_SK"),
        F.col("SUB_ADDR_AUDIT_ACTN_CD_SK").alias("SUB_ADDR_AUDIT_ACTN_CD_SK"),
        F.col("SUB_ADDR_CD_SK").alias("SUB_ADDR_CD_SK"),
        F.col("SRC_SYS_CRT_DT_SK").alias("SRC_SYS_CRT_DT_SK"),
        F.col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
        F.col("ADDR_LN_1").alias("ADDR_LN_1"),
        F.col("ADDR_LN_2").alias("ADDR_LN_2"),
        F.col("ADDR_LN_3").alias("ADDR_LN_3"),
        F.col("CITY_NM").alias("CITY_NM"),
        F.col("SUB_ADDR_ST_CD_SK").alias("SUB_ADDR_ST_CD_SK"),
        F.col("POSTAL_CD").alias("POSTAL_CD"),
        F.col("CNTY_NM").alias("CNTY_NM"),
        F.col("SUB_ADDR_CTRY_CD_SK").alias("SUB_ADDR_CTRY_CD_SK"),
        F.col("PHN_NO").alias("PHN_NO")
    )
)

df_DefaultUNK = (
    df_purgeTrnSubAddrAudit
    .filter(F.col("row_num") == 1)
    .select(
        F.lit(0).alias("SUB_ADDR_AUDIT_SK"),
        F.lit(0).alias("SRC_SYS_CD_SK"),
        F.lit("UNK").alias("SUB_ADDR_AUDIT_ROW_ID"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("SRC_SYS_CRT_USER_SK"),
        F.lit(0).alias("SUB_SK"),
        F.lit(0).alias("SUB_ADDR_AUDIT_ACTN_CD_SK"),
        F.lit(0).alias("SUB_ADDR_CD_SK"),
        F.lit("UNK").alias("SRC_SYS_CRT_DT_SK"),
        F.lit(0).alias("SUB_UNIQ_KEY"),
        F.lit("UNK").alias("ADDR_LN_1"),
        F.lit("UNK").alias("ADDR_LN_2"),
        F.lit("UNK").alias("ADDR_LN_3"),
        F.lit("UNK").alias("CITY_NM"),
        F.lit(0).alias("SUB_ADDR_ST_CD_SK"),
        F.lit("UNK").alias("POSTAL_CD"),
        F.lit("UNK").alias("CNTY_NM"),
        F.lit(0).alias("SUB_ADDR_CTRY_CD_SK"),
        F.lit("UNK").alias("PHN_NO")
    )
)

df_DefaultNA = (
    df_purgeTrnSubAddrAudit
    .filter(F.col("row_num") == 1)
    .select(
        F.lit(1).alias("SUB_ADDR_AUDIT_SK"),
        F.lit(1).alias("SRC_SYS_CD_SK"),
        F.lit("NA").alias("SUB_ADDR_AUDIT_ROW_ID"),
        F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("SRC_SYS_CRT_USER_SK"),
        F.lit(1).alias("SUB_SK"),
        F.lit(1).alias("SUB_ADDR_AUDIT_ACTN_CD_SK"),
        F.lit(1).alias("SUB_ADDR_CD_SK"),
        F.lit("NA").alias("SRC_SYS_CRT_DT_SK"),
        F.lit(1).alias("SUB_UNIQ_KEY"),
        F.lit("NA").alias("ADDR_LN_1"),
        F.lit("NA").alias("ADDR_LN_2"),
        F.lit("NA").alias("ADDR_LN_3"),
        F.lit("NA").alias("CITY_NM"),
        F.lit(1).alias("SUB_ADDR_ST_CD_SK"),
        F.lit("NA").alias("POSTAL_CD"),
        F.lit("NA").alias("CNTY_NM"),
        F.lit(1).alias("SUB_ADDR_CTRY_CD_SK"),
        F.lit("NA").alias("PHN_NO")
    )
)

df_recycle_sub_list = (
    df_purgeTrnSubAddrAudit
    .filter(F.col("ErrCount") > 0)
    .select(
        F.col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
        F.lit("SUB_ADDR_AUDIT").alias("TABLE")
    )
)

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

path_subRecycle = f"{adls_path_raw}/landing/FctsMbrRecycleList.SubRecycle.dat.{RunID}"
write_files(
    df_recycle_sub_list,
    path_subRecycle,
    ",",
    "append",
    False,
    False,
    "\"",
    None
)

df_collector = (
    df_SubAddrAuditOut
    .unionByName(df_DefaultUNK)
    .unionByName(df_DefaultNA)
)

df_collector_enriched = (
    df_collector
    .withColumn("SUB_ADDR_AUDIT_ROW_ID", rpad(F.col("SUB_ADDR_AUDIT_ROW_ID"), <...>, " "))
    .withColumn("SRC_SYS_CRT_DT_SK", rpad(F.col("SRC_SYS_CRT_DT_SK"), 10, " "))
    .withColumn("ADDR_LN_1", rpad(F.col("ADDR_LN_1"), <...>, " "))
    .withColumn("ADDR_LN_2", rpad(F.col("ADDR_LN_2"), <...>, " "))
    .withColumn("ADDR_LN_3", rpad(F.col("ADDR_LN_3"), <...>, " "))
    .withColumn("CITY_NM", rpad(F.col("CITY_NM"), <...>, " "))
    .withColumn("POSTAL_CD", rpad(F.col("POSTAL_CD"), <...>, " "))
    .withColumn("CNTY_NM", rpad(F.col("CNTY_NM"), <...>, " "))
    .withColumn("PHN_NO", rpad(F.col("PHN_NO"), <...>, " "))
)

df_final = df_collector_enriched.select(
    "SUB_ADDR_AUDIT_SK",
    "SRC_SYS_CD_SK",
    "SUB_ADDR_AUDIT_ROW_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "SRC_SYS_CRT_USER_SK",
    "SUB_SK",
    "SUB_ADDR_AUDIT_ACTN_CD_SK",
    "SUB_ADDR_CD_SK",
    "SRC_SYS_CRT_DT_SK",
    "SUB_UNIQ_KEY",
    "ADDR_LN_1",
    "ADDR_LN_2",
    "ADDR_LN_3",
    "CITY_NM",
    "SUB_ADDR_ST_CD_SK",
    "POSTAL_CD",
    "CNTY_NM",
    "SUB_ADDR_CTRY_CD_SK",
    "PHN_NO"
)

path_subAddrAudit = f"{adls_path}/load/SUB_ADDR_AUDIT.dat"
write_files(
    df_final,
    path_subAddrAudit,
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)