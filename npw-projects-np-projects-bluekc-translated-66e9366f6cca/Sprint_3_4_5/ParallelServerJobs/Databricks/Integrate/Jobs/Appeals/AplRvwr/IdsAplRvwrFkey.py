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
# MAGIC Bhoomi Dasari    07/11/2007                Initial program                                                                                   3028                  devlIDS30

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
    StringType,
    IntegerType,
    TimestampType,
    DecimalType
)
from pyspark.sql.functions import col, lit, rpad, to_timestamp
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


Logging = get_widget_value('Logging','X')
InFile = get_widget_value('InFile','')

schema_IdsAplRvwrExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38,10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("APL_RVWR_SK", IntegerType(), nullable=False),
    StructField("APL_RVWR_ID", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_USER_SK", IntegerType(), nullable=False),
    StructField("APL_RVWR_CAT_CD_SK", IntegerType(), nullable=False),
    StructField("APL_RVWR_SUBCAT_CD_SK", IntegerType(), nullable=False),
    StructField("EFF_DT_SK", StringType(), nullable=False),
    StructField("LAST_UPDT_DTM", TimestampType(), nullable=False),
    StructField("APL_RVWR_NM", StringType(), nullable=True),
    StructField("CRDTL_SUM_DESC", StringType(), nullable=True)
])

df_IdsAplRvwrExtr = (
    spark.read
    .option("header", "false")
    .option("quote", "\"")
    .option("delimiter", ",")
    .schema(schema_IdsAplRvwrExtr)
    .csv(f"{adls_path}/key/{InFile}")
)

df_ForeignKey = (
    df_IdsAplRvwrExtr
    .withColumn("SrcSysCdSk", GetFkeyCodes("IDS", col("APL_RVWR_SK"), "SOURCE SYSTEM", col("SRC_SYS_CD"), Logging))
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn("svLstUpdUsrSk", GetFkeyAppUsr(col("SRC_SYS_CD"), col("APL_RVWR_SK"), col("LAST_UPDT_USER_SK"), Logging))
    .withColumn("svAplRvwrCtCdSk", GetFkeyCodes(col("SRC_SYS_CD"), col("APL_RVWR_SK"), "APPEAL REPRESENTATIVE CATEGORY", col("APL_RVWR_CAT_CD_SK"), Logging))
    .withColumn("svAplRvwrSubCatCdSk", GetFkeyCodes(col("SRC_SYS_CD"), col("APL_RVWR_SK"), "APPEAL REPRESENTATIVE SUBCATEGORY", col("APL_RVWR_SUBCAT_CD_SK"), Logging))
    .withColumn("sveffDtSk", GetFkeyDate("IDS", col("APL_RVWR_SK"), col("EFF_DT_SK"), Logging))
    .withColumn("ErrCount", GetFkeyErrorCnt(col("APL_RVWR_SK")))
)

df_ForeignKey_Fkey = df_ForeignKey.filter((col("ErrCount") == 0) | (col("PassThru") == 'Y')).select(
    col("APL_RVWR_SK"),
    col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
    col("APL_RVWR_ID"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("svLstUpdUsrSk").alias("LAST_UPDT_USER_SK"),
    col("svAplRvwrCtCdSk").alias("APL_RVWR_CAT_CD_SK"),
    col("svAplRvwrSubCatCdSk").alias("APL_RVWR_SUBCAT_CD_SK"),
    col("sveffDtSk").alias("EFF_DT_SK"),
    col("LAST_UPDT_DTM"),
    col("APL_RVWR_NM"),
    col("CRDTL_SUM_DESC")
)

df_ForeignKey_RecycleCore = df_ForeignKey.filter(col("ErrCount") > 0).withColumn(
    "JOB_EXCTN_RCRD_ERR_SK", GetRecycleKey(col("APL_RVWR_SK"))
).withColumn(
    "ERR_CT", col("ErrCount")
).withColumn(
    "RECYCLE_CT", col("RECYCLE_CT") + lit(1)
)

df_Recycle_final = df_ForeignKey_RecycleCore.select(
    col("JOB_EXCTN_RCRD_ERR_SK"),
    col("INSRT_UPDT_CD"),
    col("DISCARD_IN"),
    col("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    col("SRC_SYS_CD"),
    col("PRI_KEY_STRING"),
    col("APL_RVWR_SK"),
    col("APL_RVWR_ID"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_USER_SK"),
    col("APL_RVWR_CAT_CD_SK"),
    col("APL_RVWR_SUBCAT_CD_SK"),
    col("EFF_DT_SK"),
    col("LAST_UPDT_DTM"),
    col("APL_RVWR_NM"),
    col("CRDTL_SUM_DESC")
).withColumn(
    "INSRT_UPDT_CD", rpad(col("INSRT_UPDT_CD"), 10, " ")
).withColumn(
    "DISCARD_IN", rpad(col("DISCARD_IN"), 1, " ")
).withColumn(
    "PASS_THRU_IN", rpad(col("PASS_THRU_IN"), 1, " ")
).withColumn(
    "EFF_DT_SK", rpad(col("EFF_DT_SK"), 10, " ")
).withColumn(
    "SRC_SYS_CD", rpad(col("SRC_SYS_CD"), 255, " ")
).withColumn(
    "PRI_KEY_STRING", rpad(col("PRI_KEY_STRING"), 255, " ")
).withColumn(
    "APL_RVWR_ID", rpad(col("APL_RVWR_ID"), 255, " ")
).withColumn(
    "APL_RVWR_NM", rpad(col("APL_RVWR_NM"), 255, " ")
).withColumn(
    "CRDTL_SUM_DESC", rpad(col("CRDTL_SUM_DESC"), 255, " ")
)

write_files(
    df_Recycle_final,
    "hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_DefaultUNK = spark.createDataFrame(
    [
        (
            0, 0, "UNK", 0, 0, 0, 0, 0, "UNK",
            to_timestamp(lit("1753-01-01"), "yyyy-MM-dd"),
            "UNK", "UNK"
        )
    ],
    [
        "APL_RVWR_SK",
        "SRC_SYS_CD_SK",
        "APL_RVWR_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_USER_SK",
        "APL_RVWR_CAT_CD_SK",
        "APL_RVWR_SUBCAT_CD_SK",
        "EFF_DT_SK",
        "LAST_UPDT_DTM",
        "APL_RVWR_NM",
        "CRDTL_SUM_DESC"
    ]
)

df_DefaultNA = spark.createDataFrame(
    [
        (
            1, 1, "NA", 1, 1, 1, 1, 1, "NA",
            to_timestamp(lit("1753-01-01"), "yyyy-MM-dd"),
            "NA", "NA"
        )
    ],
    [
        "APL_RVWR_SK",
        "SRC_SYS_CD_SK",
        "APL_RVWR_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_USER_SK",
        "APL_RVWR_CAT_CD_SK",
        "APL_RVWR_SUBCAT_CD_SK",
        "EFF_DT_SK",
        "LAST_UPDT_DTM",
        "APL_RVWR_NM",
        "CRDTL_SUM_DESC"
    ]
)

df_Collector = (
    df_ForeignKey_Fkey.unionByName(df_DefaultUNK)
    .unionByName(df_DefaultNA)
)

df_Collector_write = (
    df_Collector
    .withColumn("APL_RVWR_ID", rpad(col("APL_RVWR_ID"), 255, " "))
    .withColumn("EFF_DT_SK", rpad(col("EFF_DT_SK"), 10, " "))
    .withColumn("APL_RVWR_NM", rpad(col("APL_RVWR_NM"), 255, " "))
    .withColumn("CRDTL_SUM_DESC", rpad(col("CRDTL_SUM_DESC"), 255, " "))
)

write_files(
    df_Collector_write.select(
        "APL_RVWR_SK",
        "SRC_SYS_CD_SK",
        "APL_RVWR_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_USER_SK",
        "APL_RVWR_CAT_CD_SK",
        "APL_RVWR_SUBCAT_CD_SK",
        "EFF_DT_SK",
        "LAST_UPDT_DTM",
        "APL_RVWR_NM",
        "CRDTL_SUM_DESC"
    ),
    f"{adls_path}/load/APL_RVWR.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)