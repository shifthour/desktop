# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 03/11/09 15:13:12 Batch  15046_54866 PROMOTE bckcetl ids20 dsadm rc for ralph 
# MAGIC ^1_1 03/11/09 14:40:56 Batch  15046_52880 INIT bckcett testIDScur dsadm rc for ralph 
# MAGIC ^1_6 03/09/09 11:24:23 Batch  15044_41067 PROMOTE bckcett testIDScur u03651 steph for Ralph
# MAGIC ^1_6 03/09/09 11:01:22 Batch  15044_39685 INIT bckcett devlIDScur u03651 steffy
# MAGIC ^1_5 03/06/09 14:38:31 Batch  15041_52722 INIT bckcett devlIDScur u08717 Brent for Ralph
# MAGIC ^1_4 03/05/09 20:44:06 Batch  15040_74649 INIT bckcett devlIDScur u03651 steffy
# MAGIC ^1_3 03/05/09 13:26:54 Batch  15040_48421 INIT bckcett devlIDScur u03651 steffy
# MAGIC ^1_2 02/23/09 10:29:31 Batch  15030_37773 INIT bckcett devlIDScur u03651 steffy
# MAGIC ^1_1 02/23/09 10:20:32 Batch  15030_37236 INIT bckcett devlIDScur u03651 steffy
# MAGIC 
# MAGIC 
# MAGIC ***************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:   Takes the primary key file and applies the foreign keys.   
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer               Date                         Change Description                                                                           Project #          Development Project          Code Reviewer          Date Reviewed  
# MAGIC ------------------             ----------------------------     -----------------------------------------------------------------------                                   ----------------         ------------------------------------       ----------------------------      ----------------
# MAGIC Bhoomi Dasari       2009-01-12                Initial programming                                                                               3863                devlIDScur                        Steph Goddard          02/12/2009

# MAGIC Set all foreign surrogate keys
# MAGIC Merge source data with default rows
# MAGIC Writing Sequential File to /load
# MAGIC Read records from extract job - BCBSAhyEligTypExtr
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


Logging = get_widget_value("Logging","X")
InFile = get_widget_value("InFile","IdsAhyEligTypExtr.AhyEligTyp.dat.20090211")
SourceSK = get_widget_value("SourceSK","99860")

schema_IdsAhyEligTypExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38,10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("AHY_ELIG_TYP_SK", IntegerType(), False),
    StructField("AHY_ELIG_TYP_ID", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("AHY_ELIG_TYP_DESC", StringType(), True),
    StructField("AUDIT_TS", TimestampType(), False),
    StructField("USER_ID", StringType(), False)
])

df_IdsAhyEligTypExtr = (
    spark.read.format("csv")
    .option("sep", ",")
    .option("quote", "\"")
    .option("header", "false")
    .schema(schema_IdsAhyEligTypExtr)
    .load(f"{adls_path}/key/{InFile}")
)

df_foreignkey_input = (
    df_IdsAhyEligTypExtr
    .withColumn("svSrcSysLastUpdtDtSk", GetFkeyDate(F.lit("IDS"), F.col("AHY_ELIG_TYP_SK"), F.col("AUDIT_TS"), F.lit(Logging)))
    .withColumn("svSrcSysLastUpdtUserSk", GetFkeyAppUsr(F.lit("FACETS"), F.col("AHY_ELIG_TYP_SK"), F.col("USER_ID"), F.lit(Logging)))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("AHY_ELIG_TYP_SK")))
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
)

df_fkey = (
    df_foreignkey_input
    .filter((F.col("ErrCount") == 0) | (F.col("PassThru") == 'Y'))
    .select(
        F.col("AHY_ELIG_TYP_SK").alias("AHY_ELIG_TYP_SK"),
        F.lit(SourceSK).alias("SRC_SYS_CD_SK"),
        F.col("AHY_ELIG_TYP_ID").alias("AHY_ELIG_TYP_ID"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("AHY_ELIG_TYP_DESC").alias("AHY_ELIG_TYP_DESC"),
        F.col("svSrcSysLastUpdtDtSk").alias("SRC_SYS_LAST_UPDT_DT_SK"),
        F.col("svSrcSysLastUpdtUserSk").alias("SRC_SYS_LAST_UPDT_USER_SK")
    )
)

df_recycle = (
    df_foreignkey_input
    .filter(F.col("ErrCount") > 0)
    .select(
        GetRecycleKey(F.col("AHY_ELIG_TYP_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("DISCARD_IN").alias("DISCARD_IN"),
        F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("ErrCount").alias("ERR_CT"),
        (F.col("RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("AHY_ELIG_TYP_SK").alias("AHY_ELIG_TYP_SK"),
        F.col("AHY_ELIG_TYP_ID").alias("AHY_ELIG_TYP_ID"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("AHY_ELIG_TYP_DESC").alias("AHY_ELIG_TYP_DESC"),
        F.col("AUDIT_TS").alias("AUDIT_TS"),
        F.col("USER_ID").alias("USER_ID")
    )
)

df_recycle_final = (
    df_recycle
    .select(
        F.col("JOB_EXCTN_RCRD_ERR_SK"),
        F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
        F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
        F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT"),
        F.col("ERR_CT"),
        F.col("RECYCLE_CT"),
        F.col("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING"),
        F.col("AHY_ELIG_TYP_SK"),
        F.col("AHY_ELIG_TYP_ID"),
        F.col("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("AHY_ELIG_TYP_DESC"),
        F.col("AUDIT_TS"),
        F.rpad(F.col("USER_ID"), 15, " ").alias("USER_ID")
    )
)

write_files(
    df_recycle_final,
    "hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_parquet=True,
    header=True,
    quote="\"",
    nullValue=None
)

windowSpec = Window.orderBy(F.lit(1))
df_foreignkey_input_with_rn = df_foreignkey_input.withColumn("row_number", F.row_number().over(windowSpec))

df_defaultUNK = (
    df_foreignkey_input_with_rn
    .filter(F.col("row_number") == 1)
    .select(
        F.lit(0).alias("AHY_ELIG_TYP_SK"),
        F.lit(0).alias("SRC_SYS_CD_SK"),
        F.lit("UNK").alias("AHY_ELIG_TYP_ID"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit("UNK").alias("AHY_ELIG_TYP_DESC"),
        F.lit("1753-01-01").alias("SRC_SYS_LAST_UPDT_DT_SK"),
        F.lit(0).alias("SRC_SYS_LAST_UPDT_USER_SK")
    )
)

df_defaultNA = (
    df_foreignkey_input_with_rn
    .filter(F.col("row_number") == 1)
    .select(
        F.lit(1).alias("AHY_ELIG_TYP_SK"),
        F.lit(1).alias("SRC_SYS_CD_SK"),
        F.lit("NA").alias("AHY_ELIG_TYP_ID"),
        F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit("NA").alias("AHY_ELIG_TYP_DESC"),
        F.lit("1753-01-01").alias("SRC_SYS_LAST_UPDT_DT_SK"),
        F.lit(1).alias("SRC_SYS_LAST_UPDT_USER_SK")
    )
)

df_collector = (
    df_fkey
    .unionByName(df_defaultUNK)
    .unionByName(df_defaultNA)
)

df_collector_final = df_collector.select(
    F.col("AHY_ELIG_TYP_SK"),
    F.col("SRC_SYS_CD_SK"),
    F.col("AHY_ELIG_TYP_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("AHY_ELIG_TYP_DESC"),
    F.rpad(F.col("SRC_SYS_LAST_UPDT_DT_SK"), 10, " ").alias("SRC_SYS_LAST_UPDT_DT_SK"),
    F.col("SRC_SYS_LAST_UPDT_USER_SK")
)

write_files(
    df_collector_final,
    f"{adls_path}/load/AHY_ELIG_TYP.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)