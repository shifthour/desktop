# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 12/06/07 10:30:05 Batch  14585_37808 INIT bckcetl ids20 dsadm dadm
# MAGIC ^1_1 10/03/07 10:51:00 Batch  14521_39063 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 03/07/07 16:30:43 Batch  14311_59451 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 10/09/06 14:25:59 Batch  14162_51974 PROMOTE bckcetl ids20 dsadm Keith for Sharon
# MAGIC ^1_1 10/09/06 14:14:37 Batch  14162_51294 INIT bckcett testIDS30 dsadm Keith for Sharon
# MAGIC ^1_6 10/05/06 14:35:41 Batch  14158_52545 PROMOTE bckcett testIDS30 u10157 sa
# MAGIC ^1_6 10/05/06 14:34:04 Batch  14158_52446 INIT bckcett devlIDS30 u10157 SA
# MAGIC ^1_5 09/28/06 18:10:23 Batch  14151_65432 PROMOTE bckcett devlIDS30 u10157 sa
# MAGIC ^1_5 09/28/06 18:00:51 Batch  14151_64855 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_4 09/22/06 12:01:15 Batch  14145_43279 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_3 09/21/06 16:08:28 Batch  14144_58111 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_2 09/20/06 23:19:38 Batch  14143_83979 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_1 09/20/06 22:15:19 Batch  14143_80124 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_1 09/19/06 09:53:29 Batch  14142_35614 INIT bckcett devlIDS30 u03651 steffy
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY  
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsAgntTerrFkey
# MAGIC 
# MAGIC DESCRIPTION:     Takes the sorted file and applies the primary key.   Preps the file for the foreign key lookup process that follows.
# MAGIC       
# MAGIC 
# MAGIC INPUTS:  Primary Key output file
# MAGIC 	
# MAGIC   
# MAGIC HASH FILES:     hf_recycle
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  GetFkeyCodes
# MAGIC                             GetFkeyDates
# MAGIC                             GetFkeyErrorCnt
# MAGIC 
# MAGIC PROCESSING:  Natural keys are converted to surrogate keys
# MAGIC 
# MAGIC OUTPUTS:   AGNT_ADDR.dat
# MAGIC                      hf_recycle - If recycling was used for this job records would be written to this hash file that had errors.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC            Sharon Andrew   -  08/08/2006  -  Originally programmed

# MAGIC Set all foreign surragote keys
# MAGIC Merge source data with default rows
# MAGIC Writing Sequential File to /load
# MAGIC Read common record format file from extract job.
# MAGIC End of IDS Marketing Territory Foreign Key
# MAGIC Builds table records with source system BBBAGNT
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DecimalType, TimestampType
)
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


Logging = get_widget_value('Logging','Y')
InFile = get_widget_value('InFile','BBBMktngTerritoryExtr.MktngTerr.dat.2006070123456')

schema_BBBMktngTerrExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38,10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("MKTNG_TERR_SK", IntegerType(), nullable=False),
    StructField("MKTNG_TERR_CD", StringType(), nullable=False),
    StructField("MKTNG_MKT_SEG", StringType(), nullable=False),
    StructField("SRC_SYS_CD_SK", IntegerType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("CRT_DT", TimestampType(), nullable=False),
    StructField("EFF_DT", TimestampType(), nullable=False),
    StructField("LAST_UPDT_DT", TimestampType(), nullable=True),
    StructField("TERM_DT", TimestampType(), nullable=True),
    StructField("TERR_CK", IntegerType(), nullable=True),
    StructField("ACCT_COORDINATOR_NM", StringType(), nullable=False),
    StructField("ACCT_EXEC_NM", StringType(), nullable=True),
    StructField("SLS_REP_NM", StringType(), nullable=False),
    StructField("TERR_DESC", StringType(), nullable=True)
])

df_BBBMktngTerrExtr = (
    spark.read
    .format("csv")
    .option("header", False)
    .option("sep", ",")
    .option("quote", '"')
    .schema(schema_BBBMktngTerrExtr)
    .load(f"{adls_path}/key/{InFile}")
)

df_BBBMktngTerrExtr = df_BBBMktngTerrExtr.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "MKTNG_TERR_SK",
    "MKTNG_TERR_CD",
    "MKTNG_MKT_SEG",
    "SRC_SYS_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CRT_DT",
    "EFF_DT",
    "LAST_UPDT_DT",
    "TERM_DT",
    "TERR_CK",
    "ACCT_COORDINATOR_NM",
    "ACCT_EXEC_NM",
    "SLS_REP_NM",
    "TERR_DESC"
)

df_ForeignKey_StageVars = (
    df_BBBMktngTerrExtr
    .withColumn("PassThru", F.col("JOB_EXCTN_RCRD_ERR_SK"))
    .withColumn("SrcSysCdSk", GetFkeyCodes("IDS", F.col("MKTNG_TERR_SK"), "SOURCE SYSTEM", F.col("SRC_SYS_CD"), Logging))
    .withColumn("MarketingTerritoryCategory", GetFkeyCodes("BBBAGNT", F.col("MKTNG_TERR_SK"), "TERRITORY LEVEL", trim(F.col("MKTNG_MKT_SEG")), Logging))
    .withColumn("CreateDateSK", GetFkeyDate("IDS", F.col("MKTNG_TERR_SK"), F.col("CRT_DT"), Logging))
    .withColumn("EffectDateSK", GetFkeyDate("IDS", F.col("MKTNG_TERR_SK"), F.col("EFF_DT"), Logging))
    .withColumn("LastUpdateDateSK", GetFkeyDate("IDS", F.col("MKTNG_TERR_SK"), F.col("LAST_UPDT_DT"), Logging))
    .withColumn("TerminationDateSK", GetFkeyDate("IDS", F.col("MKTNG_TERR_SK"), F.col("TERM_DT"), Logging))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("MKTNG_TERR_SK")))
)

w = Window.orderBy(F.monotonically_increasing_id())
df_indexed = df_ForeignKey_StageVars.withColumn("ROWNUM", F.row_number().over(w))

df_fkey = df_indexed.filter("(ErrCount = 0) OR (PassThru = 'Y')").select(
    F.col("MKTNG_TERR_SK").alias("MKTNG_TERR_SK"),
    F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
    F.col("MKTNG_TERR_CD").alias("MKTNG_TERR_ID"),
    F.col("MarketingTerritoryCategory").alias("MKTNG_TERR_CAT_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CreateDateSK").alias("CRT_DT_SK"),
    F.col("EffectDateSK").alias("EFF_DT_SK"),
    F.col("LastUpdateDateSK").alias("LAST_UPDT_DT_SK"),
    F.col("TerminationDateSK").alias("TERM_DT_SK"),
    F.col("TERR_CK").alias("TERR_UNIQ_KEY"),
    F.col("ACCT_COORDINATOR_NM").alias("ACCT_CRDNTR_NM"),
    F.col("ACCT_EXEC_NM").alias("ACCT_EXEC_NM"),
    F.col("TERR_DESC").alias("TERR_DESC"),
    F.col("SLS_REP_NM").alias("REP_NM")
)

df_recycle = df_indexed.filter("ErrCount > 0").select(
    F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("MKTNG_TERR_SK").alias("MKTNG_TERR_SK"),
    F.col("MKTNG_TERR_CD").alias("MKTNG_TERR_ID"),
    F.col("MKTNG_MKT_SEG").alias("MKT_SEG"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CRT_DT").alias("CRT_DT"),
    F.col("EFF_DT").alias("EFF_DT"),
    F.col("LAST_UPDT_DT").alias("LAST_UPDT_DT"),
    F.col("TERM_DT").alias("TERM_DT"),
    F.col("TERR_CK").alias("TERR_CK"),
    F.col("ACCT_COORDINATOR_NM").alias("ACCT_COORDINATOR_NM"),
    F.col("ACCT_EXEC_NM").alias("ACCT_EXEC_NM"),
    F.col("SLS_REP_NM").alias("SLS_REP_NM"),
    F.col("TERR_DESC").alias("TERR_DESC")
)

df_defaultunk = df_indexed.filter("ROWNUM=1").select(
    F.lit(0).alias("MKTNG_TERR_SK"),
    F.lit(0).alias("SRC_SYS_CD_SK"),
    F.lit("UNK").alias("MKTNG_TERR_ID"),
    F.lit(0).alias("MKTNG_TERR_CAT_CD_SK"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit("UNK").alias("CRT_DT_SK"),
    F.lit("UNK").alias("EFF_DT_SK"),
    F.lit("UNK").alias("LAST_UPDT_DT_SK"),
    F.lit("UNK").alias("TERM_DT_SK"),
    F.lit(0).alias("TERR_UNIQ_KEY"),
    F.lit("UNK").alias("ACCT_CRDNTR_NM"),
    F.lit("UNK").alias("ACCT_EXEC_NM"),
    F.lit("UNK").alias("TERR_DESC"),
    F.lit("UNK").alias("REP_NM")
)

df_defaultna = df_indexed.filter("ROWNUM=1").select(
    F.lit(1).alias("MKTNG_TERR_SK"),
    F.lit(1).alias("SRC_SYS_CD_SK"),
    F.lit("NA").alias("MKTNG_TERR_ID"),
    F.lit(1).alias("MKTNG_TERR_CAT_CD_SK"),
    F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit("NA").alias("CRT_DT_SK"),
    F.lit("NA").alias("EFF_DT_SK"),
    F.lit("NA").alias("LAST_UPDT_DT_SK"),
    F.lit("NA").alias("TERM_DT_SK"),
    F.lit(1).alias("TERR_UNIQ_KEY"),
    F.lit("NA").alias("ACCT_CRDNTR_NM"),
    F.lit("NA").alias("ACCT_EXEC_NM"),
    F.lit("NA").alias("TERR_DESC"),
    F.lit("NA").alias("REP_NM")
)

df_collector = df_fkey.unionByName(df_defaultunk).unionByName(df_defaultna)

df_recycle_ordered = df_recycle.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "MKTNG_TERR_SK",
    "MKTNG_TERR_ID",
    "MKT_SEG",
    "SRC_SYS_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CRT_DT",
    "EFF_DT",
    "LAST_UPDT_DT",
    "TERM_DT",
    "TERR_CK",
    "ACCT_COORDINATOR_NM",
    "ACCT_EXEC_NM",
    "SLS_REP_NM",
    "TERR_DESC"
)

df_recycle_rpad = df_recycle_ordered
df_recycle_rpad = df_recycle_rpad.withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " "))
df_recycle_rpad = df_recycle_rpad.withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " "))
df_recycle_rpad = df_recycle_rpad.withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " "))
df_recycle_rpad = df_recycle_rpad.withColumn("MKTNG_TERR_ID", F.rpad(F.col("MKTNG_TERR_ID"), 3, " "))

write_files(
    df_recycle_rpad,
    f"{adls_path}/hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_parquet=True,
    header=True,
    quote='"',
    nullValue=None
)

df_mt = df_collector.select(
    "MKTNG_TERR_SK",
    "SRC_SYS_CD_SK",
    "MKTNG_TERR_ID",
    "MKTNG_TERR_CAT_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CRT_DT_SK",
    "EFF_DT_SK",
    "LAST_UPDT_DT_SK",
    "TERM_DT_SK",
    "TERR_UNIQ_KEY",
    "ACCT_CRDNTR_NM",
    "ACCT_EXEC_NM",
    "TERR_DESC",
    "REP_NM"
)

df_mt_rpad = df_mt.withColumn("MKTNG_TERR_ID", F.rpad(F.col("MKTNG_TERR_ID"), 3, " "))
df_mt_rpad = df_mt_rpad.withColumn("CRT_DT_SK", F.rpad(F.col("CRT_DT_SK"), 10, " "))
df_mt_rpad = df_mt_rpad.withColumn("EFF_DT_SK", F.rpad(F.col("EFF_DT_SK"), 10, " "))
df_mt_rpad = df_mt_rpad.withColumn("LAST_UPDT_DT_SK", F.rpad(F.col("LAST_UPDT_DT_SK"), 10, " "))
df_mt_rpad = df_mt_rpad.withColumn("TERM_DT_SK", F.rpad(F.col("TERM_DT_SK"), 10, " "))

write_files(
    df_mt_rpad,
    f"{adls_path}/load/MKTNG_TERR.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote='"',
    nullValue=None
)