# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2013 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  
# MAGIC 
# MAGIC PROCESSING:   Assigns Foreign Keys for the IDS table VBB_PLN.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 	Project/Altiris #	Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------	------------------------	-----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Kalyan Neelam         2013-05-13          4963 VBB Phase III     Initial Programming                                                                      IntegrateNewDevl          Bhoomi Dasari           5/16/2013

# MAGIC Set all foreign surragote keys
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
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DecimalType
from pyspark.sql.window import Window
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


InFile = get_widget_value("InFile", "")
Logging = get_widget_value("Logging", "")
RunID = get_widget_value("RunID", "")

schema_idsvbbplnextr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38,10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("VBB_PLN_SK", IntegerType(), False),
    StructField("VBB_PLN_UNIQ_KEY", IntegerType(), False),
    StructField("SRC_SYS_CD_SK", IntegerType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("VBB_MDL_CD", StringType(), False),
    StructField("VBB_PLN_CAT_CD", StringType(), False),
    StructField("VBB_PLN_PROD_CAT_CD", StringType(), False),
    StructField("VBB_PLN_TERM_RSN_CD", StringType(), False),
    StructField("VBB_PLN_STRT_YR_IN", StringType(), False),
    StructField("VBB_PLN_STRT_YR_NO", IntegerType(), False),
    StructField("VBB_PLN_STRT_DT_SK", StringType(), False),
    StructField("VBB_PLN_END_DT_SK", StringType(), False),
    StructField("VBB_PLN_TERM_DT_SK", StringType(), False),
    StructField("SRC_SYS_CRT_DTM", TimestampType(), False),
    StructField("SRC_SYS_UPDT_DTM", TimestampType(), False),
    StructField("VBB_PLN_NM", StringType(), False)
])

df_idsvbbplnextr = (
    spark.read
    .option("header", "false")
    .option("sep", ",")
    .option("quote", "\"")
    .schema(schema_idsvbbplnextr)
    .csv(f"{adls_path}/key/{InFile}.dat")
)

df_transform = (
    df_idsvbbplnextr
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn(
        "svVbbMdlCdSk",
        GetFkeyClctnDomainCodes(
            F.col("SRC_SYS_CD"),
            F.col("VBB_PLN_SK"),
            F.lit("VBB MODEL TYPE"),
            F.lit("IHMF CONSTITUENT"),
            F.lit("VBB MODEL TYPE"),
            F.lit("IDS"),
            F.col("VBB_MDL_CD"),
            F.lit(Logging)
        )
    )
    .withColumn(
        "svVbbPlnCatCdSk",
        GetFkeyClctnDomainCodes(
            F.col("SRC_SYS_CD"),
            F.col("VBB_PLN_SK"),
            F.lit("VBB PLAN CATEGORY"),
            F.lit("IHMF CONSTITUENT"),
            F.lit("VBB PLAN CATEGORY"),
            F.lit("IDS"),
            F.col("VBB_PLN_CAT_CD"),
            F.lit(Logging)
        )
    )
    .withColumn(
        "svVbbPlnProdCatCdSk",
        GetFkeyClctnDomainCodes(
            F.lit("FACETS"),
            F.col("VBB_PLN_SK"),
            F.lit("CLASS PLAN PRODUCT CATEGORY"),
            F.lit("FACETS DBO"),
            F.lit("CLASS PLAN PRODUCT CATEGORY"),
            F.lit("IDS"),
            F.col("VBB_PLN_PROD_CAT_CD"),
            F.lit(Logging)
        )
    )
    .withColumn(
        "svVbbPlnTermRsnCdSk",
        GetFkeyClctnDomainCodes(
            F.col("SRC_SYS_CD"),
            F.col("VBB_PLN_SK"),
            F.lit("VBB TERMINATION REASON"),
            F.lit("IHMF CONSTITUENT"),
            F.lit("VBB TERMINATION REASON"),
            F.lit("IDS"),
            F.col("VBB_PLN_TERM_RSN_CD"),
            F.lit(Logging)
        )
    )
    .withColumn(
        "svVbbPlnStrtDtSk",
        GetFkeyDate(
            F.lit("IDS"),
            F.col("VBB_PLN_SK"),
            F.col("VBB_PLN_STRT_DT_SK"),
            F.lit(Logging)
        )
    )
    .withColumn(
        "svVbbPlnEndDtSk",
        GetFkeyDate(
            F.lit("IDS"),
            F.col("VBB_PLN_SK"),
            F.col("VBB_PLN_END_DT_SK"),
            F.lit(Logging)
        )
    )
    .withColumn(
        "svVbbPlnTermDtSk",
        GetFkeyDate(
            F.lit("IDS"),
            F.col("VBB_PLN_SK"),
            F.col("VBB_PLN_TERM_DT_SK"),
            F.lit(Logging)
        )
    )
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("VBB_PLN_SK")))
)

df_fkey = (
    df_transform
    .filter((F.col("ErrCount") == 0) | (F.col("PassThru") == "Y"))
    .select(
        F.col("VBB_PLN_SK").alias("VBB_PLN_SK"),
        F.col("VBB_PLN_UNIQ_KEY").alias("VBB_PLN_UNIQ_KEY"),
        F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("svVbbMdlCdSk").alias("VBB_MDL_CD_SK"),
        F.col("svVbbPlnCatCdSk").alias("VBB_PLN_CAT_CD_SK"),
        F.col("svVbbPlnProdCatCdSk").alias("VBB_PLN_PROD_CAT_CD_SK"),
        F.col("svVbbPlnTermRsnCdSk").alias("VBB_PLN_TERM_RSN_CD_SK"),
        F.col("VBB_PLN_STRT_YR_IN").alias("VBB_PLN_STRT_YR_IN"),
        F.col("VBB_PLN_STRT_YR_NO").alias("VBB_PLN_STRT_YR_NO"),
        F.col("svVbbPlnStrtDtSk").alias("VBB_PLN_STRT_DT_SK"),
        F.col("svVbbPlnEndDtSk").alias("VBB_PLN_END_DT_SK"),
        F.col("svVbbPlnTermDtSk").alias("VBB_PLN_TERM_DT_SK"),
        F.col("SRC_SYS_CRT_DTM").alias("SRC_SYS_CRT_DTM"),
        F.col("SRC_SYS_UPDT_DTM").alias("SRC_SYS_UPDT_DTM"),
        F.col("VBB_PLN_NM").alias("VBB_PLN_NM")
    )
)

df_recycle = (
    df_transform
    .filter(F.col("ErrCount") > 0)
    .select(
        GetRecycleKey(F.col("VBB_PLN_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("DISCARD_IN").alias("DISCARD_IN"),
        F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("ErrCount").alias("ERR_CT"),
        (F.col("RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("VBB_PLN_SK").alias("VBB_PLN_SK"),
        F.col("VBB_PLN_UNIQ_KEY").alias("VBB_PLN_UNIQ_KEY"),
        F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("VBB_MDL_CD").alias("VBB_MDL_CD"),
        F.col("VBB_PLN_CAT_CD").alias("VBB_PLN_CAT_CD"),
        F.col("VBB_PLN_PROD_CAT_CD").alias("VBB_PLN_PROD_CAT_CD"),
        F.col("VBB_PLN_TERM_RSN_CD").alias("VBB_PLN_TERM_RSN_CD"),
        F.col("VBB_PLN_STRT_YR_IN").alias("VBB_PLN_STRT_YR_IN"),
        F.col("VBB_PLN_STRT_YR_NO").alias("VBB_PLN_STRT_YR_NO"),
        F.col("VBB_PLN_STRT_DT_SK").alias("VBB_PLN_STRT_DT_SK"),
        F.col("VBB_PLN_END_DT_SK").alias("VBB_PLN_END_DT_SK"),
        F.col("VBB_PLN_TERM_DT_SK").alias("VBB_PLN_TERM_DT_SK"),
        F.col("SRC_SYS_CRT_DTM").alias("SRC_SYS_CRT_DTM"),
        F.col("SRC_SYS_UPDT_DTM").alias("SRC_SYS_UPDT_DTM"),
        F.col("VBB_PLN_NM").alias("VBB_PLN_NM")
    )
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

w = Window.orderBy(F.lit(1))
df_transform_with_rn = df_transform.withColumn("rownum", F.row_number().over(w))

df_defaultUNK = (
    df_transform_with_rn
    .filter(F.col("rownum") == 1)
    .select(
        F.lit(0).alias("VBB_PLN_SK"),
        F.lit(0).alias("VBB_PLN_UNIQ_KEY"),
        F.lit(0).alias("SRC_SYS_CD_SK"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("VBB_MDL_CD_SK"),
        F.lit(0).alias("VBB_PLN_CAT_CD_SK"),
        F.lit(0).alias("VBB_PLN_PROD_CAT_CD_SK"),
        F.lit(0).alias("VBB_PLN_TERM_RSN_CD_SK"),
        F.lit("N").alias("VBB_PLN_STRT_YR_IN"),
        F.lit(0).alias("VBB_PLN_STRT_YR_NO"),
        F.lit("1753-01-01").alias("VBB_PLN_STRT_DT_SK"),
        F.lit("2199-12-31").alias("VBB_PLN_END_DT_SK"),
        F.lit("2199-12-31").alias("VBB_PLN_TERM_DT_SK"),
        F.lit("1753-01-01-00.00.00.000000").alias("SRC_SYS_CRT_DTM"),
        F.lit("1753-01-01-00.00.00.000000").alias("SRC_SYS_UPDT_DTM"),
        F.lit("NA").alias("VBB_PLN_NM")
    )
)

df_defaultNA = (
    df_transform_with_rn
    .filter(F.col("rownum") == 1)
    .select(
        F.lit(1).alias("VBB_PLN_SK"),
        F.lit(1).alias("VBB_PLN_UNIQ_KEY"),
        F.lit(1).alias("SRC_SYS_CD_SK"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("VBB_MDL_CD_SK"),
        F.lit(1).alias("VBB_PLN_CAT_CD_SK"),
        F.lit(1).alias("VBB_PLN_PROD_CAT_CD_SK"),
        F.lit(1).alias("VBB_PLN_TERM_RSN_CD_SK"),
        F.lit("N").alias("VBB_PLN_STRT_YR_IN"),
        F.lit(1).alias("VBB_PLN_STRT_YR_NO"),
        F.lit("1753-01-01").alias("VBB_PLN_STRT_DT_SK"),
        F.lit("2199-12-31").alias("VBB_PLN_END_DT_SK"),
        F.lit("2199-12-31").alias("VBB_PLN_TERM_DT_SK"),
        F.lit("1753-01-01-00.00.00.000000").alias("SRC_SYS_CRT_DTM"),
        F.lit("1753-01-01-00.00.00.000000").alias("SRC_SYS_UPDT_DTM"),
        F.lit("UNK").alias("VBB_PLN_NM")
    )
)

df_collector = df_fkey.unionByName(df_defaultUNK).unionByName(df_defaultNA)

df_final = df_collector.select(
    "VBB_PLN_SK",
    "VBB_PLN_UNIQ_KEY",
    "SRC_SYS_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "VBB_MDL_CD_SK",
    "VBB_PLN_CAT_CD_SK",
    "VBB_PLN_PROD_CAT_CD_SK",
    "VBB_PLN_TERM_RSN_CD_SK",
    "VBB_PLN_STRT_YR_IN",
    "VBB_PLN_STRT_YR_NO",
    "VBB_PLN_STRT_DT_SK",
    "VBB_PLN_END_DT_SK",
    "VBB_PLN_TERM_DT_SK",
    "SRC_SYS_CRT_DTM",
    "SRC_SYS_UPDT_DTM",
    "VBB_PLN_NM"
)

df_final = df_final.withColumn("VBB_PLN_STRT_YR_IN", F.rpad(F.col("VBB_PLN_STRT_YR_IN"), 1, " "))
df_final = df_final.withColumn("VBB_PLN_STRT_DT_SK", F.rpad(F.col("VBB_PLN_STRT_DT_SK"), 10, " "))
df_final = df_final.withColumn("VBB_PLN_END_DT_SK", F.rpad(F.col("VBB_PLN_END_DT_SK"), 10, " "))
df_final = df_final.withColumn("VBB_PLN_TERM_DT_SK", F.rpad(F.col("VBB_PLN_TERM_DT_SK"), 10, " "))

write_files(
    df_final,
    f"{adls_path}/load/VBB_PLN.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)