# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC ****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2004, 2005, 2006, 2007, 2008,2020 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC CALLED BY:     IdsFctsClmLoad2Seq
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:  Takes the sorted file and applies the primary key.   Preps the file for the foreign key lookup process that follows.
# MAGIC       
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 	Project/Altiris #	Change Description				Development Project	Code Reviewer	Date Reviewed       
# MAGIC ------------------              --------------------     	------------------------	-----------------------------------------------------------------------	--------------------------------	-------------------------------	----------------------------       
# MAGIC Steph Goddard        04/2004                                                  Originally Programmed
# MAGIC SharonAnrew           06/20/2004                                            Standardized
# MAGIC Brent Leland            09/07/2004                                            Added link partitioner
# MAGIC                                                                                                Added default rows or UNK and NA
# MAGIC Steph Goddard        03/2006                                                 sequencer changes
# MAGIC Bhoomi                    08-13-2008             3567 Primary Key     Added source system SK to job parameters              devlIDS                                    Steph Goddard         08/15/2008
# MAGIC 
# MAGIC Reddy Sanam          10-09-2020                                            changed derivation for stage variable
# MAGIC                                                                                                StateCdSk to pass 'FACETS' for 'LUMERIS'
# MAGIC  
# MAGIC Sunitha Ganta         10-11-2020                                             Brought up to standards

# MAGIC Writing Sequential File to /load
# MAGIC Read common record format file from extract job.
# MAGIC Set all foreign surragote keys
# MAGIC Merge source data with default rows
# MAGIC This hash file is written to by all claim foriegn key jobs.
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
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


Logging = get_widget_value('Logging','Y')
Source = get_widget_value('Source','FACETS')
SrcSysCdSk = get_widget_value('SrcSysCdSk','105859')
InFile = get_widget_value('InFile','')

schema_idsclmaltpayeeextr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38, 10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("CLM_ALT_PAYE_SK", IntegerType(), False),
    StructField("SRC_SYS_CD_SK", IntegerType(), False),
    StructField("CLM_ID", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("CLM_SK", IntegerType(), False),
    StructField("ALT_PAYE_NM", StringType(), True),
    StructField("ADDR_LN_1", StringType(), True),
    StructField("ADDR_LN_2", StringType(), True),
    StructField("ADDR_LN_3", StringType(), True),
    StructField("CITY_NM", StringType(), True),
    StructField("CLM_ALT_PAYE_ST_CD", StringType(), False),
    StructField("POSTAL_CD", StringType(), True),
    StructField("CNTY_NM", StringType(), True),
    StructField("CLM_ALT_PAYE_CTRY_CD", StringType(), False),
    StructField("PHN_NO", StringType(), True),
    StructField("PHN_NO_EXT", StringType(), True),
    StructField("FAX_NO", StringType(), True),
    StructField("FAX_NO_EXT", StringType(), True),
    StructField("EMAIL_ADDR", StringType(), True)
])

df_idsclmaltpayeeextr = (
    spark.read
    .schema(schema_idsclmaltpayeeextr)
    .option("header", "false")
    .option("quote", "\"")
    .option("escape", "\"")
    .option("delimiter", ",")
    .csv(f"{adls_path}/key/{InFile}")
)

df_enriched = (
    df_idsclmaltpayeeextr
    .withColumn(
        "StateCdSk",
        GetFkeyCodes(
            F.when(F.col("SRC_SYS_CD") == "LUMERIS", "FACETS").otherwise(F.col("SRC_SYS_CD")),
            F.col("CLM_ALT_PAYE_SK"),
            F.lit("STATE"),
            F.col("CLM_ALT_PAYE_ST_CD"),
            Logging
        )
    )
    .withColumn(
        "ClmSk",
        GetFkeyClm(
            F.col("SRC_SYS_CD"),
            F.col("CLM_ALT_PAYE_SK"),
            F.col("CLM_ID"),
            Logging
        )
    )
    .withColumn(
        "CountryCdSk",
        GetFkeyCodes(
            F.when(F.col("SRC_SYS_CD") == "LUMERIS", "FACETS").otherwise(F.col("SRC_SYS_CD")),
            F.col("CLM_ALT_PAYE_SK"),
            F.lit("COUNTRY"),
            F.col("CLM_ALT_PAYE_CTRY_CD"),
            Logging
        )
    )
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("CLM_ALT_PAYE_SK")))
)

df_fkey1 = df_enriched.filter(
    (F.col("ErrCount") == 0) | (F.col("PassThru") == "Y")
).select(
    F.col("CLM_ALT_PAYE_SK").alias("CLM_ALT_PAYE_SK"),
    F.lit(SrcSysCdSk).cast("int").alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("ClmSk").alias("CLM_SK"),
    F.col("ALT_PAYE_NM").alias("ALT_PAYE_NM"),
    F.col("ADDR_LN_1").alias("ADDR_LN_1"),
    F.col("ADDR_LN_2").alias("ADDR_LN_2"),
    F.col("ADDR_LN_3").alias("ADDR_LN_3"),
    F.col("CITY_NM").alias("CITY_NM"),
    F.col("StateCdSk").alias("CLM_ALT_PAYE_ST_CD_SK"),
    F.col("POSTAL_CD").alias("POSTAL_CD"),
    F.col("CNTY_NM").alias("CNTY_NM"),
    F.col("CountryCdSk").alias("CLM_ALT_PAYE_CTRY_CD_SK"),
    F.col("PHN_NO").alias("PHN_NO"),
    F.col("PHN_NO_EXT").alias("PHN_NO_EXT"),
    F.col("FAX_NO").alias("FAX_NO"),
    F.col("FAX_NO_EXT").alias("FAX_NO_EXT"),
    F.col("EMAIL_ADDR").alias("EMAIL_ADDR")
)

df_fkey1 = df_fkey1.withColumn("CLM_ID", F.rpad(F.col("CLM_ID"), 18, " "))
df_fkey1 = df_fkey1.withColumn("POSTAL_CD", F.rpad(F.col("POSTAL_CD"), 11, " "))

df_recyle1 = df_enriched.filter(F.col("ErrCount") > 0)
df_recyle1 = df_recyle1.withColumn(
    "JOB_EXCTN_RCRD_ERR_SK",
    GetRecycleKey(F.col("CLM_ALT_PAYE_SK"))
).withColumn(
    "RECYCLE_CT",
    F.col("RECYCLE_CT") + F.lit(1)
)

df_recyle1 = df_recyle1.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ErrCount").alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("CLM_ALT_PAYE_SK").alias("CLM_ALT_PAYE_SK"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_SK").alias("CLM_SK"),
    F.col("ALT_PAYE_NM").alias("ALT_PAYE_NM"),
    F.col("ADDR_LN_1").alias("ADDR_LN_1"),
    F.col("ADDR_LN_2").alias("ADDR_LN_2"),
    F.col("ADDR_LN_3").alias("ADDR_LN_3"),
    F.col("CITY_NM").alias("CITY_NM"),
    F.col("CLM_ALT_PAYE_ST_CD").alias("CLM_ALT_PAYE_ST_CD"),
    F.col("POSTAL_CD").alias("POSTAL_CD"),
    F.col("CNTY_NM").alias("CNTY_NM"),
    F.col("CLM_ALT_PAYE_CTRY_CD").alias("CLM_ALT_PAYE_CTRY_CD"),
    F.col("PHN_NO").alias("PHN_NO"),
    F.col("PHN_NO_EXT").alias("PHN_NO_EXT"),
    F.col("FAX_NO").alias("FAX_NO"),
    F.col("FAX_NO_EXT").alias("FAX_NO_EXT"),
    F.col("EMAIL_ADDR").alias("EMAIL_ADDR")
)

df_recyle1 = df_recyle1.withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " "))
df_recyle1 = df_recyle1.withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " "))
df_recyle1 = df_recyle1.withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " "))
df_recyle1 = df_recyle1.withColumn("CLM_ID", F.rpad(F.col("CLM_ID"), 18, " "))
df_recyle1 = df_recyle1.withColumn("CLM_ALT_PAYE_ST_CD", F.rpad(F.col("CLM_ALT_PAYE_ST_CD"), 2, " "))
df_recyle1 = df_recyle1.withColumn("POSTAL_CD", F.rpad(F.col("POSTAL_CD"), 11, " "))
df_recyle1 = df_recyle1.withColumn("CLM_ALT_PAYE_CTRY_CD", F.rpad(F.col("CLM_ALT_PAYE_CTRY_CD"), 4, " "))

write_files(
    df_recyle1,
    "hf_recycle.parquet",
    ',',
    'overwrite',
    True,
    True,
    '"',
    None
)

df_recycle_clms = df_enriched.filter(F.col("ErrCount") > 0).select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CLM_ID").alias("CLM_ID")
)

df_recycle_clms = df_recycle_clms.withColumn("CLM_ID", F.rpad(F.col("CLM_ID"), 18, " "))

write_files(
    df_recycle_clms,
    "hf_claim_recycle_keys.parquet",
    ',',
    'overwrite',
    True,
    True,
    '"',
    None
)

df_defaultunk = df_idsclmaltpayeeextr.limit(1).select(
    F.lit(0).alias("CLM_ALT_PAYE_SK"),
    F.lit(0).alias("SRC_SYS_CD_SK"),
    F.lit("UNK").alias("CLM_ID"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("CLM_SK"),
    F.lit("UNK").alias("ALT_PAYE_NM"),
    F.lit("UNK").alias("ADDR_LN_1"),
    F.lit("UNK").alias("ADDR_LN_2"),
    F.lit("UNK").alias("ADDR_LN_3"),
    F.lit("UNK").alias("CITY_NM"),
    F.lit(0).alias("CLM_ALT_PAYE_ST_CD_SK"),
    F.lit("UNK").alias("POSTAL_CD"),
    F.lit("UNK").alias("CNTY_NM"),
    F.lit(0).alias("CLM_ALT_PAYE_CTRY_CD_SK"),
    F.lit("UNK").alias("PHN_NO"),
    F.lit("UNK").alias("PHN_NO_EXT"),
    F.lit("UNK").alias("FAX_NO"),
    F.lit("UNK").alias("FAX_NO_EXT"),
    F.lit("UNK").alias("EMAIL_ADDR")
)
df_defaultunk = df_defaultunk.withColumn("CLM_ID", F.rpad(F.col("CLM_ID"), 18, " "))
df_defaultunk = df_defaultunk.withColumn("POSTAL_CD", F.rpad(F.col("POSTAL_CD"), 11, " "))

df_defaultna = df_idsclmaltpayeeextr.limit(1).select(
    F.lit(1).alias("CLM_ALT_PAYE_SK"),
    F.lit(1).alias("SRC_SYS_CD_SK"),
    F.lit("NA").alias("CLM_ID"),
    F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("CLM_SK"),
    F.lit("NA").alias("ALT_PAYE_NM"),
    F.lit("NA").alias("ADDR_LN_1"),
    F.lit("NA").alias("ADDR_LN_2"),
    F.lit("NA").alias("ADDR_LN_3"),
    F.lit("NA").alias("CITY_NM"),
    F.lit(1).alias("CLM_ALT_PAYE_ST_CD_SK"),
    F.lit("NA").alias("POSTAL_CD"),
    F.lit("NA").alias("CNTY_NM"),
    F.lit(1).alias("CLM_ALT_PAYE_CTRY_CD_SK"),
    F.lit("NA").alias("PHN_NO"),
    F.lit("NA").alias("PHN_NO_EXT"),
    F.lit("NA").alias("FAX_NO"),
    F.lit("NA").alias("FAX_NO_EXT"),
    F.lit("NA").alias("EMAIL_ADDR")
)
df_defaultna = df_defaultna.withColumn("CLM_ID", F.rpad(F.col("CLM_ID"), 18, " "))
df_defaultna = df_defaultna.withColumn("POSTAL_CD", F.rpad(F.col("POSTAL_CD"), 11, " "))

df_collector = df_fkey1.unionByName(df_defaultunk).unionByName(df_defaultna).select(
    "CLM_ALT_PAYE_SK",
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CLM_SK",
    "ALT_PAYE_NM",
    "ADDR_LN_1",
    "ADDR_LN_2",
    "ADDR_LN_3",
    "CITY_NM",
    "CLM_ALT_PAYE_ST_CD_SK",
    "POSTAL_CD",
    "CNTY_NM",
    "CLM_ALT_PAYE_CTRY_CD_SK",
    "PHN_NO",
    "PHN_NO_EXT",
    "FAX_NO",
    "FAX_NO_EXT",
    "EMAIL_ADDR"
)

write_files(
    df_collector,
    f"{adls_path}/load/CLM_ALT_PAYEE.{Source}.dat",
    ',',
    'overwrite',
    False,
    False,
    '"',
    None
)