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
# MAGIC Bhoomi Dasari     07/11/2007               Initial program                                                                                     3028                  devlIDS30                         Steph Goddard          8/23/07

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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DecimalType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


Logging = get_widget_value("Logging", "X")
InFile = get_widget_value("InFile", "FctsAplCntctExtr.AplCntct.dat.2007010125")

schema_IdsAplCntctExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(10, 0), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("APL_CNTCT_SK", IntegerType(), False),
    StructField("APL_ID", StringType(), False),
    StructField("SEQ_NO", IntegerType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("APL_SK", IntegerType(), False),
    StructField("APL_CNTCT_CAT_CD_SK", IntegerType(), False),
    StructField("APL_CNTCT_ID", StringType(), False),
    StructField("APL_CNTCT_NM", StringType(), False)
])

df_IdsAplCntctExtr = (
    spark.read
    .format("csv")
    .option("sep", ",")
    .option("quote", "\"")
    .option("header", "false")
    .schema(schema_IdsAplCntctExtr)
    .load(f"{adls_path}/key/{InFile}")
)

df_t = (
    df_IdsAplCntctExtr
    .withColumn("SrcSysCdSk", GetFkeyCodes(F.lit("IDS"), F.col("APL_CNTCT_SK"), F.lit("SOURCE SYSTEM"), F.col("SRC_SYS_CD"), F.lit(Logging)))
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("svAplSk", GetFkeyApl(F.col("SRC_SYS_CD"), F.col("APL_CNTCT_SK"), F.col("APL_SK"), F.lit(Logging)))
    .withColumn("svAplCntCatCdSk", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("APL_CNTCT_SK"), F.lit("APPEAL CONTACT CATEGORY"), F.col("APL_CNTCT_CAT_CD_SK"), F.lit(Logging)))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("APL_CNTCT_SK")))
)

df_fkey = (
    df_t
    .filter((F.col("ErrCount") == 0) | (F.col("PassThru") == 'Y'))
    .select(
        F.col("APL_CNTCT_SK").alias("APL_CNTCT_SK"),
        F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
        F.col("APL_ID").alias("APL_ID"),
        F.col("SEQ_NO").alias("SEQ_NO"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("svAplSk").alias("APL_SK"),
        F.col("svAplCntCatCdSk").alias("APL_CNTCT_CAT_CD_SK"),
        F.col("APL_CNTCT_ID").alias("APL_CNTCT_ID"),
        F.col("APL_CNTCT_NM").alias("APL_CNTCT_NM")
    )
)

df_recycle = (
    df_t
    .filter(F.col("ErrCount") > 0)
    .select(
        GetRecycleKey(F.col("APL_CNTCT_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("DISCARD_IN").alias("DISCARD_IN"),
        F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("ErrCount").alias("ERR_CT"),
        (F.col("RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("APL_CNTCT_SK").alias("APL_CNTCT_SK"),
        F.col("APL_ID").alias("APL_ID"),
        F.col("SEQ_NO").alias("SEQ_NO"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("APL_SK").alias("APL_SK"),
        F.col("APL_CNTCT_CAT_CD_SK").alias("APL_CNTCT_CAT_CD_SK"),
        F.col("APL_CNTCT_ID").alias("APL_CNTCT_ID"),
        F.col("APL_CNTCT_NM").alias("APL_CNTCT_NM")
    )
)

write_files(
    df_recycle,
    "hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_defaultUNK = (
    df_t
    .limit(1)
    .select(
        F.lit(0).alias("APL_CNTCT_SK"),
        F.lit(0).alias("SRC_SYS_CD_SK"),
        F.lit("UNK").alias("APL_ID"),
        F.lit(0).alias("SEQ_NO"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("APL_SK"),
        F.lit(0).alias("APL_CNTCT_CAT_CD_SK"),
        F.lit("UNK").alias("APL_CNTCT_ID"),
        F.lit("UNK").alias("APL_CNTCT_NM")
    )
)

df_defaultNA = (
    df_t
    .limit(1)
    .select(
        F.lit(1).alias("APL_CNTCT_SK"),
        F.lit(1).alias("SRC_SYS_CD_SK"),
        F.lit("NA").alias("APL_ID"),
        F.lit(1).alias("SEQ_NO"),
        F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("APL_SK"),
        F.lit(1).alias("APL_CNTCT_CAT_CD_SK"),
        F.lit("NA").alias("APL_CNTCT_ID"),
        F.lit("NA").alias("APL_CNTCT_NM")
    )
)

df_collector = df_fkey.unionByName(df_defaultUNK).unionByName(df_defaultNA)

df_final = df_collector.select(
    F.col("APL_CNTCT_SK"),
    F.col("SRC_SYS_CD_SK"),
    rpad(F.col("APL_ID"), F.lit(<...>), F.lit(" ")).alias("APL_ID"),
    F.col("SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("APL_SK"),
    F.col("APL_CNTCT_CAT_CD_SK"),
    rpad(F.col("APL_CNTCT_ID"), F.lit(<...>), F.lit(" ")).alias("APL_CNTCT_ID"),
    rpad(F.col("APL_CNTCT_NM"), F.lit(<...>), F.lit(" ")).alias("APL_CNTCT_NM")
)

write_files(
    df_final,
    f"{adls_path}/load/APL_CNTCT.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)