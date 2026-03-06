# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 06/18/08 14:12:14 Batch  14780_51145 PROMOTE bckcetl ids20 dsadm bls for sa
# MAGIC ^1_1 06/18/08 13:36:55 Batch  14780_49086 PROMOTE bckcetl ids20 dsadm bls for sa
# MAGIC ^1_1 06/18/08 13:02:50 Batch  14780_46975 PROMOTE bckcetl ids20 dsadm bls for sa
# MAGIC ^1_1 06/18/08 12:43:53 Batch  14780_45838 PROMOTE bckcetl VIDS dsadm BLS FOR SA
# MAGIC ^1_1 06/18/08 12:20:19 Batch  14780_44422 INIT bckcett testIDS dsadm bls for sa
# MAGIC ^1_6 06/17/08 13:24:03 Batch  14779_48263 PROMOTE bckcett testIDS u03651 Steffs promote
# MAGIC ^1_6 06/17/08 12:43:46 Batch  14779_45832 INIT bckcett devlIDS u03651 steffy
# MAGIC ^1_5 06/02/08 11:13:03 Batch  14764_40386 INIT bckcett devlIDS u03651 steffy
# MAGIC ^1_3 01/31/08 13:26:22 Batch  14641_48385 INIT bckcett devlIDS u03651 steffy
# MAGIC ^1_2 01/31/08 13:18:25 Batch  14641_47911 INIT bckcett devlIDS u03651 steffy
# MAGIC ^1_1 01/04/08 12:48:16 Batch  14614_46132 PROMOTE bckcett devlIDS u150247 BhoomiD
# MAGIC ^1_1 01/04/08 12:41:40 Batch  14614_45710 INIT bckcett devlIDS30 u150247 Bhoomi
# MAGIC 
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
# MAGIC Bhoomi Dasari    12/20/2007                Initial program                                                                                   3036                  devlIDS30                          Steph Goddard           01/08/2008
# MAGIC Kalyan Neelam    07/30/2010               Changed logic to calculate the SrcSysCdSk from each record         4297                  RebuildIntNewDevl            Steph Goddard          08/12/2010
# MAGIC                                                               than passing the SK from parms as the UWS Risk Cat extract 
# MAGIC                                                               will have multiple sources

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
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DecimalType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


InFile = get_widget_value("InFile","")
SourceSK = get_widget_value("SourceSK","")

schema_IdsRiskCatExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38,10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("RISK_CAT_SK", IntegerType(), False),
    StructField("RISK_CAT_ID", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("MAJ_PRCTC_CAT_CD", StringType(), False),
    StructField("RISK_CAT_DESC", StringType(), False),
    StructField("RISK_CAT_LABEL", StringType(), False),
    StructField("RISK_CAT_LONG_DESC", StringType(), False),
    StructField("RISK_CAT_NM", StringType(), False),
])

df_IdsRiskCatExtr = (
    spark.read.csv(
        f"{adls_path}/key/{InFile}",
        schema=schema_IdsRiskCatExtr,
        sep=",",
        header=False,
        quote='"'
    )
)

df_stagevars = (
    df_IdsRiskCatExtr
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("svMajPrctcCatCdSk", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("RISK_CAT_SK"), F.lit("MAJOR PRACTICE CATEGORY"), F.col("MAJ_PRCTC_CAT_CD"), F.lit("X")))
    .withColumn("svSrcSysCdSk", GetFkeyCodes(F.lit("IDS"), F.col("RISK_CAT_SK"), F.lit("SOURCE SYSTEM"), F.col("SRC_SYS_CD"), F.lit("X")))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("RISK_CAT_SK")))
    .withColumn("rownum_temp", F.row_number().over(Window.orderBy(F.lit(1))))
)

df_Fkey = (
    df_stagevars
    .filter((F.col("ErrCount") == 0) | (F.col("PassThru") == "Y"))
    .select(
        F.col("RISK_CAT_SK").alias("RISK_CAT_SK"),
        F.col("svSrcSysCdSk").alias("SRC_SYS_CD_SK"),
        F.col("RISK_CAT_ID").alias("RISK_CAT_ID"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("svMajPrctcCatCdSk").alias("MAJ_PRCTC_CAT_CD_SK"),
        F.col("RISK_CAT_DESC").alias("RISK_CAT_DESC"),
        F.col("RISK_CAT_LABEL").alias("RISK_CAT_LABEL"),
        F.col("RISK_CAT_LONG_DESC").alias("RISK_CAT_LONG_DESC"),
        F.col("RISK_CAT_NM").alias("RISK_CAT_NM")
    )
)

df_Recycle_pre = (
    df_stagevars
    .filter(F.col("ErrCount") > 0)
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", GetRecycleKey(F.col("RISK_CAT_SK")))
    .withColumn("RECYCLE_CT", F.col("RECYCLE_CT") + F.lit(1))
    .select(
        F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("DISCARD_IN").alias("DISCARD_IN"),
        F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("ErrCount").alias("ERR_CT"),
        F.col("RECYCLE_CT").alias("RECYCLE_CT"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("RISK_CAT_SK").alias("RISK_CAT_SK"),
        F.col("RISK_CAT_ID").alias("RISK_CAT_ID"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("MAJ_PRCTC_CAT_CD").alias("MAJ_PRCTC_CAT_CD"),
        F.col("RISK_CAT_DESC").alias("RISK_CAT_DESC"),
        F.col("RISK_CAT_LABEL").alias("RISK_CAT_LABEL"),
        F.col("RISK_CAT_LONG_DESC").alias("RISK_CAT_LONG_DESC"),
        F.col("RISK_CAT_NM").alias("RISK_CAT_NM")
    )
)

df_Recycle = (
    df_Recycle_pre
    .withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " "))
)

write_files(
    df_Recycle,
    "hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote='"',
    nullValue=None
)

df_DefaultUNK = (
    df_stagevars
    .filter(F.col("rownum_temp") == 1)
    .select(
        F.lit(0).alias("RISK_CAT_SK"),
        F.lit(0).alias("SRC_SYS_CD_SK"),
        F.lit("UNK").alias("RISK_CAT_ID"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("MAJ_PRCTC_CAT_CD_SK"),
        F.lit("UNK").alias("RISK_CAT_DESC"),
        F.lit("UNK").alias("RISK_CAT_LABEL"),
        F.lit("UNK").alias("RISK_CAT_LONG_DESC"),
        F.lit("UNK").alias("RISK_CAT_NM")
    )
)

df_DefaultNA = (
    df_stagevars
    .filter(F.col("rownum_temp") == 1)
    .select(
        F.lit(1).alias("RISK_CAT_SK"),
        F.lit(1).alias("SRC_SYS_CD_SK"),
        F.lit("NA").alias("RISK_CAT_ID"),
        F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("MAJ_PRCTC_CAT_CD_SK"),
        F.lit("NA").alias("RISK_CAT_DESC"),
        F.lit("NA").alias("RISK_CAT_LABEL"),
        F.lit("NA").alias("RISK_CAT_LONG_DESC"),
        F.lit("NA").alias("RISK_CAT_NM")
    )
)

df_Collector = (
    df_Fkey.unionByName(df_DefaultUNK)
           .unionByName(df_DefaultNA)
    .select(
        "RISK_CAT_SK",
        "SRC_SYS_CD_SK",
        "RISK_CAT_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "MAJ_PRCTC_CAT_CD_SK",
        "RISK_CAT_DESC",
        "RISK_CAT_LABEL",
        "RISK_CAT_LONG_DESC",
        "RISK_CAT_NM"
    )
)

write_files(
    df_Collector,
    f"{adls_path}/load/RISK_CAT.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)