# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 05/12/09 13:30:39 Batch  15108_48642 PROMOTE bckcetl ids20 dsadm bls for rt
# MAGIC ^1_1 05/12/09 13:07:49 Batch  15108_47273 INIT bckcett:31540 testIDS dsadm BLS FOR RT
# MAGIC ^1_1 04/28/09 15:30:57 Batch  15094_55861 PROMOTE bckcett testIDS u03651 steph for Ralph
# MAGIC ^1_1 04/28/09 15:26:41 Batch  15094_55604 INIT bckcett devlIDS u03651 steffy
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC Developer                          Date                    Project/Altiris #                     Change Description                                                       Development Project               Code Reviewer                      Date Reviewed
# MAGIC ----------------------------------      -------------------      -----------------------------------           ---------------------------------------------------------                                 ----------------------------------              ---------------------------------               -------------------------
# MAGIC Ralph Tucker                  03/18/2009      3808 - BICC                           Initial development                                                            devlIDS

# MAGIC Set all foreign surrogate keys
# MAGIC Merge source data with default rows
# MAGIC Writing Sequential File to /load
# MAGIC Read common record format file from extract job.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
    DoubleType
)
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


InFile = get_widget_value("InFile","IdsUmIpLtrExtr.dat")
OutFile = get_widget_value("OutFile","UM_IP_LTR.dat")
Logging = get_widget_value("Logging","$PROJDEF")
SrcSysCdSk = get_widget_value("SrcSysCdSk","105838")

schema_idsumipltrextr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DoubleType(), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("UM_IP_LTR_SK", IntegerType(), False),
    StructField("UM_REF_ID", StringType(), False),
    StructField("SEQ_NO", IntegerType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("UM_SK", IntegerType(), False),
    StructField("UMUM_REF_ID", StringType(), False),
    StructField("UMSV_SEQ_NO", IntegerType(), False),
    StructField("ATXR_DEST_ID", TimestampType(), False),
    StructField("ATSY_ID_TRGT", StringType(), False),
    StructField("ATSY_ID", StringType(), False),
    StructField("ATXR_CREATE_USUS", StringType(), False),
    StructField("ATXR_LAST_UPD_USUS", StringType(), False),
    StructField("UM_ID", StringType(), False),
    StructField("ATLD_ID", StringType(), False),
    StructField("ATCHMT_SRC_DTM", TimestampType(), False),
    StructField("ATXR_CREATE_DT", StringType(), False),
    StructField("ATXR_LAST_UPD_DT", StringType(), False)
])

df_idsumipltrextr = (
    spark.read.format("csv")
    .option("header", "false")
    .option("quote", "\"")
    .schema(schema_idsumipltrextr)
    .load(f"{adls_path}/key/{InFile}")
)

df_foreignkey_base = (
    df_idsumipltrextr
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("svDefaultDate", F.lit("1753-01-01-00.00.00.000000"))
    .withColumn("svUmSk", GetFkeyUm(F.col("SRC_SYS_CD"), F.col("UM_IP_LTR_SK"), F.col("UM_ID"), Logging))
    .withColumn("svUmIpLtrStyleCdSk", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("UM_IP_LTR_SK"), F.lit("ATTACHMENT TYPE"), F.col("ATSY_ID"), Logging))
    .withColumn("svUmIpLtrTypCdSk", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("UM_IP_LTR_SK"), F.lit("CLAIM LETTER TYPE"), F.col("ATLD_ID"), Logging))
    .withColumn("svCrtByUser", GetFkeyAppUsr(F.col("SRC_SYS_CD"), F.col("UM_IP_LTR_SK"), F.col("ATXR_CREATE_USUS"), Logging))
    .withColumn("svLastUpdtUser", GetFkeyAppUsr(F.col("SRC_SYS_CD"), F.col("UM_IP_LTR_SK"), F.col("ATXR_LAST_UPD_USUS"), Logging))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("UM_SK")))
)

win = Window.orderBy(F.monotonically_increasing_id())
df_foreignkey = df_foreignkey_base.withColumn("_row_num", F.row_number().over(win))

df_recycle = df_foreignkey.filter(F.col("ErrCount") > 0).select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ErrCount").alias("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.rpad(F.col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    F.rpad(F.col("PRI_KEY_STRING"), <...>, " ").alias("PRI_KEY_STRING"),
    F.col("UM_IP_LTR_SK"),
    F.rpad(F.col("UM_REF_ID"), <...>, " ").alias("UM_REF_ID"),
    F.col("SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("svUmSk").alias("UM_SK"),
    F.rpad(F.col("UMUM_REF_ID"), 9, " ").alias("UMUM_REF_ID"),
    F.col("UMSV_SEQ_NO"),
    F.col("ATXR_DEST_ID"),
    F.rpad(F.col("ATSY_ID"), 4, " ").alias("ATSY_ID"),
    F.rpad(F.col("ATXR_CREATE_USUS"), 10, " ").alias("ATXR_CREATE_USUS"),
    F.rpad(F.col("ATXR_LAST_UPD_USUS"), 10, " ").alias("ATXR_LAST_UPD_USUS"),
    F.rpad(F.col("UM_ID"), 9, " ").alias("UM_ID"),
    F.rpad(F.col("ATLD_ID"), 8, " ").alias("ATLD_ID"),
    F.col("ATCHMT_SRC_DTM"),
    F.rpad(F.col("ATXR_CREATE_DT"), 10, " ").alias("ATXR_CREATE_DT"),
    F.rpad(F.col("ATXR_LAST_UPD_DT"), 10, " ").alias("ATXR_LAST_UPD_DT")
)

write_files(
    df_recycle,
    f"{adls_path}/hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_defaultunk = df_foreignkey.filter(F.col("_row_num") == 1).select(
    F.lit(0).alias("UM_IP_LTR_SK"),
    F.lit(0).alias("SRC_SYS_CD_SK"),
    F.lit("UNK").alias("UM_REF_ID"),
    F.lit(0).alias("LTR_SEQ_NO"),
    F.lit("UNK").alias("UM_IP_LTR_STYLE_CD"),
    F.col("svDefaultDate").alias("LTR_DEST_ID"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("CRT_BY_USER_SK"),
    F.lit(0).alias("LAST_UPDT_USER_SK"),
    F.lit(0).alias("UM_SK"),
    F.lit(0).alias("UM_IP_LTR_TYP_CD_SK"),
    F.col("svDefaultDate").alias("ATCHMT_SRC_DTM"),
    F.rpad(F.lit("1753-01-01"), 10, " ").alias("CRT_DT_SK"),
    F.rpad(F.lit("1753-01-01"), 10, " ").alias("LAST_UPDT_DT_SK"),
    F.lit(0).alias("UM_IP_LTR_STYLE_CD_SK")
)

df_defaultna = df_foreignkey.filter(F.col("_row_num") == 1).select(
    F.lit(1).alias("UM_IP_LTR_SK"),
    F.lit(1).alias("SRC_SYS_CD_SK"),
    F.lit("NA").alias("UM_REF_ID"),
    F.lit(0).alias("LTR_SEQ_NO"),
    F.lit("NA").alias("UM_IP_LTR_STYLE_CD"),
    F.col("svDefaultDate").alias("LTR_DEST_ID"),
    F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("CRT_BY_USER_SK"),
    F.lit(1).alias("LAST_UPDT_USER_SK"),
    F.lit(1).alias("UM_SK"),
    F.lit(1).alias("UM_IP_LTR_TYP_CD_SK"),
    F.col("svDefaultDate").alias("ATCHMT_SRC_DTM"),
    F.rpad(F.lit("1753-01-01"), 10, " ").alias("CRT_DT_SK"),
    F.rpad(F.lit("1753-01-01"), 10, " ").alias("LAST_UPDT_DT_SK"),
    F.lit(1).alias("UM_IP_LTR_STYLE_CD_SK")
)

df_load = df_foreignkey.select(
    F.col("UM_IP_LTR_SK"),
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("UM_REF_ID"),
    F.col("SEQ_NO").alias("LTR_SEQ_NO"),
    F.col("ATSY_ID_TRGT").alias("UM_IP_LTR_STYLE_CD"),
    F.col("ATXR_DEST_ID").alias("LTR_DEST_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("svCrtByUser").alias("CRT_BY_USER_SK"),
    F.col("svLastUpdtUser").alias("LAST_UPDT_USER_SK"),
    F.col("svUmSk").alias("UM_SK"),
    F.col("svUmIpLtrTypCdSk").alias("UM_IP_LTR_TYP_CD_SK"),
    FORMAT_DATE(F.col("ATCHMT_SRC_DTM"), F.lit("SYBASE"), F.lit("TIMESTAMP"), F.lit("DB2TIMESTAMP")).alias("ATCHMT_SRC_DTM"),
    F.rpad(F.col("ATXR_CREATE_DT"), 10, " ").alias("CRT_DT_SK"),
    F.rpad(F.col("ATXR_LAST_UPD_DT"), 10, " ").alias("LAST_UPDT_DT_SK"),
    F.col("svUmIpLtrStyleCdSk").alias("UM_IP_LTR_STYLE_CD_SK")
)

df_collector = df_defaultunk.unionByName(df_defaultna).unionByName(df_load)

write_files(
    df_collector.select(
        F.col("UM_IP_LTR_SK"),
        F.col("SRC_SYS_CD_SK"),
        F.col("UM_REF_ID"),
        F.col("LTR_SEQ_NO"),
        F.col("UM_IP_LTR_STYLE_CD"),
        F.col("LTR_DEST_ID"),
        F.col("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("CRT_BY_USER_SK"),
        F.col("LAST_UPDT_USER_SK"),
        F.col("UM_SK"),
        F.col("UM_IP_LTR_TYP_CD_SK"),
        F.col("ATCHMT_SRC_DTM"),
        F.col("CRT_DT_SK"),
        F.col("LAST_UPDT_DT_SK"),
        F.col("UM_IP_LTR_STYLE_CD_SK")
    ),
    f"{adls_path}/load/{OutFile}",
    ",",
    "overwrite",
    False,
    True,
    "\"",
    None
)