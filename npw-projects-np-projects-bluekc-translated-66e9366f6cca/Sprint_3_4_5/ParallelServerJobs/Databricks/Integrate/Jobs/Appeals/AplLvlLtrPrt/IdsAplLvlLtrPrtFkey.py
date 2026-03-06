# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_2 10/26/07 13:38:47 Batch  14544_49159 PROMOTE bckcetl ids20 dsadm bls for hs
# MAGIC ^1_2 10/26/07 13:19:46 Batch  14544_47992 INIT bckcett testIDS30 dsadm bls for hs
# MAGIC ^1_1 09/28/07 14:58:08 Batch  14516_53893 INIT bckcett testIDS30 dsadm bls for hs
# MAGIC ^1_3 09/13/07 17:24:15 Batch  14501_62661 PROMOTE bckcett testIDS30 u11141 Hugh Sisson
# MAGIC ^1_3 09/13/07 17:10:39 Batch  14501_61844 INIT bckcett devlIDS30 u11141 Hugh Sisson
# MAGIC ^1_2 09/12/07 11:51:28 Batch  14500_42704 INIT bckcett devlIDS30 u03651 deploy to test steffy
# MAGIC ^1_1 09/05/07 12:58:39 Batch  14493_46724 INIT bckcett devlIDS30 u11141 Hugh Sisson
# MAGIC 
# MAGIC ***************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:   Takes the primary key file and applies the foreign keys.   
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer             Date                         Change Description                                                                           Project #          Development Project          Code Reviewer          Date Reviewed  
# MAGIC ------------------           ----------------------------     -----------------------------------------------------------------------                                   ----------------         ------------------------------------       ----------------------------      ----------------
# MAGIC Bhoomi Dasari    07/11/2007                Initial program                                                                                    3028                  devlIDS30                         Steph Goddard          8/23/07

# MAGIC Set all foreign surrogate keys
# MAGIC Merge source data with default rows
# MAGIC Writing Sequential File to /load
# MAGIC Read common record format file from extract job.
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
    IntegerType,
    DecimalType,
    StringType,
    TimestampType
)
from pyspark.sql import functions as F
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


Logging = get_widget_value("Logging", "")
InFile = get_widget_value("InFile", "")

schema_IdsAplLvlLtrPrtExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(28,10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("APL_LVL_LTR_PRT_SK", IntegerType(), False),
    StructField("APL_ID", StringType(), False),
    StructField("SEQ_NO", IntegerType(), False),
    StructField("APL_LVL_LTR_STYLE_CD_SK", IntegerType(), False),
    StructField("LTR_SEQ_NO", IntegerType(), False),
    StructField("LTR_DEST_ID", StringType(), False),
    StructField("PRT_SEQ_NO", IntegerType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("APL_LVL_LTR_SK", IntegerType(), False),
    StructField("RQST_DT_SK", StringType(), False),
    StructField("RQST_USER_SK", IntegerType(), False),
    StructField("SUBMT_DT_SK", StringType(), False),
    StructField("PRT_DT_SK", StringType(), False),
    StructField("LAST_UPDT_DTM", TimestampType(), False),
    StructField("LAST_UPDT_USER_SK", IntegerType(), False),
    StructField("EXPL_TX", StringType(), True),
    StructField("PRT_DESC", StringType(), False)
])

df_IdsAplLvlLtrPrtExtr = (
    spark.read.format("csv")
    .option("header", "false")
    .option("sep", ",")
    .option("quote", "\"")
    .schema(schema_IdsAplLvlLtrPrtExtr)
    .load(f"{adls_path}/key/{InFile}")
)

df_base = (
    df_IdsAplLvlLtrPrtExtr
    .withColumn("SrcSysCdSk", GetFkeyCodes(F.lit("IDS"), F.col("APL_LVL_LTR_PRT_SK"), F.lit("SOURCE SYSTEM"), F.col("SRC_SYS_CD"), F.lit(Logging)))
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("svAplLLtrStyCdSkSk", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("APL_LVL_LTR_PRT_SK"), F.lit("ATTACHMENT TYPE"), F.col("APL_LVL_LTR_STYLE_CD_SK"), F.lit(Logging)))
    .withColumn("svAplLvlLtrCdSkSk", GetFkeyAplLvlLtr(F.col("SRC_SYS_CD"), F.col("APL_LVL_LTR_PRT_SK"), F.col("APL_ID"), F.col("SEQ_NO"), F.col("APL_LVL_LTR_STYLE_CD_SK"), F.col("LTR_SEQ_NO"), F.col("LTR_DEST_ID"), F.lit(Logging)))
    .withColumn("svRqstDtSk", GetFkeyDate(F.lit("IDS"), F.col("APL_LVL_LTR_PRT_SK"), F.col("RQST_DT_SK"), F.lit(Logging)))
    .withColumn("svRqstUsrSk", GetFkeyAppUsr(F.col("SRC_SYS_CD"), F.col("APL_LVL_LTR_PRT_SK"), F.col("RQST_USER_SK"), F.lit(Logging)))
    .withColumn("svSubmtDtSk", GetFkeyDate(F.lit("IDS"), F.col("APL_LVL_LTR_PRT_SK"), F.col("SUBMT_DT_SK"), F.lit(Logging)))
    .withColumn("svPrtDtSk", GetFkeyDate(F.lit("IDS"), F.col("APL_LVL_LTR_PRT_SK"), F.col("PRT_DT_SK"), F.lit(Logging)))
    .withColumn("svLstUpdDtSk", GetFkeyAppUsr(F.col("SRC_SYS_CD"), F.col("APL_LVL_LTR_PRT_SK"), F.col("LAST_UPDT_USER_SK"), F.lit(Logging)))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("APL_LVL_LTR_PRT_SK")))
)

df_fkey = (
    df_base
    .filter((F.col("ErrCount") == 0) | (F.col("PassThru") == "Y"))
    .select(
        F.col("APL_LVL_LTR_PRT_SK"),
        F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
        F.col("APL_ID"),
        F.col("SEQ_NO"),
        F.col("svAplLLtrStyCdSkSk").alias("APL_LVL_LTR_STYLE_CD_SK"),
        F.col("LTR_SEQ_NO"),
        F.col("LTR_DEST_ID"),
        F.col("PRT_SEQ_NO"),
        F.col("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("svAplLvlLtrCdSkSk").alias("APL_LVL_LTR_SK"),
        F.col("svRqstDtSk").alias("RQST_DT_SK"),
        F.col("svRqstUsrSk").alias("RQST_USER_SK"),
        F.col("svSubmtDtSk").alias("SUBMT_DT_SK"),
        F.col("svPrtDtSk").alias("PRT_DT_SK"),
        F.col("LAST_UPDT_DTM"),
        F.col("svLstUpdDtSk").alias("LAST_UPDT_USER_SK"),
        F.col("EXPL_TX"),
        F.col("PRT_DESC")
    )
)

df_recycle = (
    df_base
    .filter(F.col("ErrCount") > 0)
    .select(
        GetRecycleKey(F.col("APL_LVL_LTR_PRT_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD"),
        F.col("DISCARD_IN"),
        F.col("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT"),
        F.col("ErrCount").alias("ERR_CT"),
        (F.col("RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
        F.col("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING"),
        F.col("APL_LVL_LTR_PRT_SK"),
        F.col("APL_ID"),
        F.col("SEQ_NO"),
        F.col("APL_LVL_LTR_STYLE_CD_SK"),
        F.col("LTR_SEQ_NO"),
        F.col("LTR_DEST_ID"),
        F.col("PRT_SEQ_NO"),
        F.col("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("APL_LVL_LTR_SK"),
        F.col("RQST_DT_SK"),
        F.col("RQST_USER_SK"),
        F.col("SUBMT_DT_SK"),
        F.col("PRT_DT_SK"),
        F.col("LAST_UPDT_DTM"),
        F.col("LAST_UPDT_USER_SK"),
        F.col("EXPL_TX"),
        F.col("PRT_DESC")
    )
)

write_files(
    df_recycle,
    f"{adls_path}/hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

window_spec = Window.orderBy(F.monotonically_increasing_id())
df_with_rn = df_base.withColumn("rownum", F.row_number().over(window_spec))

df_defaultUnk = (
    df_with_rn
    .filter(F.col("rownum") == 1)
    .select(
        F.lit(0).alias("APL_LVL_LTR_PRT_SK"),
        F.lit(0).alias("SRC_SYS_CD_SK"),
        F.lit("UNK").alias("APL_ID"),
        F.lit(0).alias("SEQ_NO"),
        F.lit(0).alias("APL_LVL_LTR_STYLE_CD_SK"),
        F.lit(0).alias("LTR_SEQ_NO"),
        F.lit("UNK").alias("LTR_DEST_ID"),
        F.lit(0).alias("PRT_SEQ_NO"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("APL_LVL_LTR_SK"),
        F.lit("UNK").alias("RQST_DT_SK"),
        F.lit(0).alias("RQST_USER_SK"),
        F.lit("UNK").alias("SUBMT_DT_SK"),
        F.lit("UNK").alias("PRT_DT_SK"),
        F.lit("1753-01-01").cast(TimestampType()).alias("LAST_UPDT_DTM"),
        F.lit(0).alias("LAST_UPDT_USER_SK"),
        F.lit("UNK").alias("EXPL_TX"),
        F.lit("UNK").alias("PRT_DESC")
    )
)

df_defaultNa = (
    df_with_rn
    .filter(F.col("rownum") == 1)
    .select(
        F.lit(1).alias("APL_LVL_LTR_PRT_SK"),
        F.lit(1).alias("SRC_SYS_CD_SK"),
        F.lit("NA").alias("APL_ID"),
        F.lit(1).alias("SEQ_NO"),
        F.lit(1).alias("APL_LVL_LTR_STYLE_CD_SK"),
        F.lit(1).alias("LTR_SEQ_NO"),
        F.lit("NA").alias("LTR_DEST_ID"),
        F.lit(1).alias("PRT_SEQ_NO"),
        F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("APL_LVL_LTR_SK"),
        F.lit("NA").alias("RQST_DT_SK"),
        F.lit(1).alias("RQST_USER_SK"),
        F.lit("NA").alias("SUBMT_DT_SK"),
        F.lit("NA").alias("PRT_DT_SK"),
        F.lit("1753-01-01").cast(TimestampType()).alias("LAST_UPDT_DTM"),
        F.lit(1).alias("LAST_UPDT_USER_SK"),
        F.lit("NA").alias("EXPL_TX"),
        F.lit("NA").alias("PRT_DESC")
    )
)

df_collector = df_fkey.unionByName(df_defaultUnk).unionByName(df_defaultNa)

df_collector_final = (
    df_collector
    .select(
        F.col("APL_LVL_LTR_PRT_SK"),
        F.col("SRC_SYS_CD_SK"),
        F.col("APL_ID"),
        F.col("SEQ_NO"),
        F.col("APL_LVL_LTR_STYLE_CD_SK"),
        F.col("LTR_SEQ_NO"),
        F.col("LTR_DEST_ID"),
        F.col("PRT_SEQ_NO"),
        F.col("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("APL_LVL_LTR_SK"),
        F.rpad(F.col("RQST_DT_SK"), 10, " ").alias("RQST_DT_SK"),
        F.col("RQST_USER_SK"),
        F.rpad(F.col("SUBMT_DT_SK"), 10, " ").alias("SUBMT_DT_SK"),
        F.rpad(F.col("PRT_DT_SK"), 10, " ").alias("PRT_DT_SK"),
        F.col("LAST_UPDT_DTM"),
        F.col("LAST_UPDT_USER_SK"),
        F.col("EXPL_TX"),
        F.col("PRT_DESC")
    )
)

write_files(
    df_collector_final,
    f"{adls_path}/load/APL_LVL_LTR_PRT.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)