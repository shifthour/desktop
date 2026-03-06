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
# MAGIC Bhoomi Dasari       2009-01-16                Initial programming                                                                               3863                devlIDScur                         Steph Goddard          02/12/2009

# MAGIC Set all foreign surrogate keys
# MAGIC Merge source data with default rows
# MAGIC Writing Sequential File to /load
# MAGIC Read records from extract job - BCBSAhyEligExtr
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
    StringType,
    TimestampType,
    DecimalType
)
from pyspark.sql.functions import col, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


Logging = get_widget_value("Logging","X")
InFile = get_widget_value("InFile","IdsAhyEligExtr.AhyElig.dat.20090211")
SourceSK = get_widget_value("SourceSK","99860")

schema_IdsAhyEligExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38,10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("AHY_ELIG_SK", IntegerType(), nullable=False),
    StructField("CLNT_SRC_ID", StringType(), nullable=False),
    StructField("CLNT_GRP_NO", StringType(), nullable=False),
    StructField("CLNT_SUBGRP_NO", StringType(), nullable=False),
    StructField("EFF_DT_SK", StringType(), nullable=False),
    StructField("CLS_ID", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("ELIG_TYP_CD", StringType(), nullable=False),
    StructField("CLS_SK", IntegerType(), nullable=False),
    StructField("CLS_PLN_SK", IntegerType(), nullable=False),
    StructField("GRP_SK", IntegerType(), nullable=False),
    StructField("SUBGRP_SK", IntegerType(), nullable=False),
    StructField("INCLD_NON_MBR_IN", StringType(), nullable=False),
    StructField("TERM_DT_SK", StringType(), nullable=False),
    StructField("AUDIT_TS", StringType(), nullable=False),
    StructField("SRC_SYS_LAST_UPDT_USER_SK", IntegerType(), nullable=False)
])

df_IdsAhyEligExtr = (
    spark.read.format("csv")
    .option("header", False)
    .option("sep", ",")
    .option("quote", "\"")
    .schema(schema_IdsAhyEligExtr)
    .load(f"{adls_path}/key/{InFile}")
)

df_ForeignKey_base = (
    df_IdsAhyEligExtr
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn("svSrcSysLastUpdtDtSk", GetFkeyDate(lit("IDS"), col("AHY_ELIG_SK"), col("AUDIT_TS"), Logging))
    .withColumn("svTermDt", GetFkeyDate(lit("IDS"), col("AHY_ELIG_SK"), col("TERM_DT_SK"), Logging))
    .withColumn("svAhyEligTyp", GetFkeyAhyEligTyp(col("SRC_SYS_CD"), col("AHY_ELIG_SK"), col("ELIG_TYP_CD"), Logging))
    .withColumn("ErrCount", GetFkeyErrorCnt(col("AHY_ELIG_SK")))
)

# Recycle link
df_recycle = df_ForeignKey_base.filter(col("ErrCount") > 0)
df_recycle = (
    df_recycle
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", GetRecycleKey(col("AHY_ELIG_SK")))
    .withColumn("INSRT_UPDT_CD", rpad(col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", rpad(col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", rpad(col("PASS_THRU_IN"), 1, " "))
    .withColumn("FIRST_RECYC_DT", col("FIRST_RECYC_DT"))
    .withColumn("ERR_CT", col("ErrCount"))
    .withColumn("RECYCLE_CT", col("RECYCLE_CT") + lit(1))
    .withColumn("SRC_SYS_CD", rpad(col("SRC_SYS_CD"), 50, " "))
    .withColumn("PRI_KEY_STRING", rpad(col("PRI_KEY_STRING"), 50, " "))
    .withColumn("AHY_ELIG_SK", col("AHY_ELIG_SK"))
    .withColumn("CLNT_SRC_ID", rpad(col("CLNT_SRC_ID"), 50, " "))
    .withColumn("CLNT_GRP_NO", rpad(col("CLNT_GRP_NO"), 50, " "))
    .withColumn("CLNT_SUBGRP_NO", rpad(col("CLNT_SUBGRP_NO"), 50, " "))
    .withColumn("EFF_DT_SK", rpad(col("EFF_DT_SK"), 10, " "))
    .withColumn("CLS_ID", rpad(col("CLS_ID"), 50, " "))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", col("CRT_RUN_CYC_EXCTN_SK"))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", col("LAST_UPDT_RUN_CYC_EXCTN_SK"))
    .withColumn("ELIG_TYP_CD", rpad(col("ELIG_TYP_CD"), 1, " "))
    .withColumn("CLS_SK", col("CLS_SK"))
    .withColumn("CLS_PLN_SK", col("CLS_PLN_SK"))
    .withColumn("GRP_SK", col("GRP_SK"))
    .withColumn("SUBGRP_SK", col("SUBGRP_SK"))
    .withColumn("INCLD_NON_MBR_IN", rpad(col("INCLD_NON_MBR_IN"), 1, " "))
    .withColumn("TERM_DT_SK", rpad(col("TERM_DT_SK"), 10, " "))
    .withColumn("AUDIT_TS", rpad(col("AUDIT_TS"), 10, " "))
    .withColumn("SRC_SYS_LAST_UPDT_USER_SK", col("SRC_SYS_LAST_UPDT_USER_SK"))
)

df_recycle = df_recycle.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "AHY_ELIG_SK",
    "CLNT_SRC_ID",
    "CLNT_GRP_NO",
    "CLNT_SUBGRP_NO",
    "EFF_DT_SK",
    "CLS_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "ELIG_TYP_CD",
    "CLS_SK",
    "CLS_PLN_SK",
    "GRP_SK",
    "SUBGRP_SK",
    "INCLD_NON_MBR_IN",
    "TERM_DT_SK",
    "AUDIT_TS",
    "SRC_SYS_LAST_UPDT_USER_SK"
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

# Fkey link
df_Fkey = df_ForeignKey_base.filter((col("ErrCount") == 0) | (col("PassThru") == 'Y'))
df_Fkey = (
    df_Fkey
    .withColumn("AHY_ELIG_SK", col("AHY_ELIG_SK"))
    .withColumn("SRC_SYS_CD_SK", lit(SourceSK))
    .withColumn("CLNT_SRC_ID", rpad(col("CLNT_SRC_ID"), 50, " "))
    .withColumn("CLNT_GRP_NO", rpad(col("CLNT_GRP_NO"), 50, " "))
    .withColumn("CLNT_SUBGRP_NO", rpad(col("CLNT_SUBGRP_NO"), 50, " "))
    .withColumn("EFF_DT_SK", rpad(col("EFF_DT_SK"), 10, " "))
    .withColumn("CLS_ID", rpad(col("CLS_ID"), 50, " "))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", col("CRT_RUN_CYC_EXCTN_SK"))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", col("LAST_UPDT_RUN_CYC_EXCTN_SK"))
    .withColumn("AHY_ELIG_TYP_SK", col("svAhyEligTyp"))
    .withColumn("CLS_SK", col("CLS_SK"))
    .withColumn("CLS_PLN_SK", col("CLS_PLN_SK"))
    .withColumn("GRP_SK", col("GRP_SK"))
    .withColumn("SUBGRP_SK", col("SUBGRP_SK"))
    .withColumn("INCLD_NON_MBR_IN", rpad(col("INCLD_NON_MBR_IN"), 1, " "))
    .withColumn("TERM_DT_SK", rpad(col("svTermDt"), 10, " "))
    .withColumn("SRC_SYS_LAST_UPDT_DT_SK", rpad(col("svSrcSysLastUpdtDtSk"), 10, " "))
    .withColumn("SRC_SYS_LAST_UPDT_USER_SK", col("SRC_SYS_LAST_UPDT_USER_SK"))
)

df_Fkey = df_Fkey.select(
    "AHY_ELIG_SK",
    "SRC_SYS_CD_SK",
    "CLNT_SRC_ID",
    "CLNT_GRP_NO",
    "CLNT_SUBGRP_NO",
    "EFF_DT_SK",
    "CLS_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "AHY_ELIG_TYP_SK",
    "CLS_SK",
    "CLS_PLN_SK",
    "GRP_SK",
    "SUBGRP_SK",
    "INCLD_NON_MBR_IN",
    "TERM_DT_SK",
    "SRC_SYS_LAST_UPDT_DT_SK",
    "SRC_SYS_LAST_UPDT_USER_SK"
)

# DefaultUNK link
df_IdsAhyEligExtr_firstRow = df_IdsAhyEligExtr.limit(1)

df_DefaultUNK = (
    df_IdsAhyEligExtr_firstRow
    .withColumn("AHY_ELIG_SK", lit(0))
    .withColumn("SRC_SYS_CD_SK", lit(0))
    .withColumn("CLNT_SRC_ID", rpad(lit("UNK"), 3, " "))
    .withColumn("CLNT_GRP_NO", rpad(lit("UNK"), 3, " "))
    .withColumn("CLNT_SUBGRP_NO", rpad(lit("UNK"), 3, " "))
    .withColumn("EFF_DT_SK", rpad(lit("1753-01-01"), 10, " "))
    .withColumn("CLS_ID", rpad(lit("UNK"), 3, " "))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(0))
    .withColumn("AHY_ELIG_TYP_SK", lit(0))
    .withColumn("CLS_SK", lit(0))
    .withColumn("CLS_PLN_SK", lit(0))
    .withColumn("GRP_SK", lit(0))
    .withColumn("SUBGRP_SK", lit(0))
    .withColumn("INCLD_NON_MBR_IN", rpad(lit("X"), 1, " "))
    .withColumn("TERM_DT_SK", rpad(lit("9999-12-31"), 10, " "))
    .withColumn("SRC_SYS_LAST_UPDT_DT_SK", rpad(lit("UNK"), 3, " "))
    .withColumn("SRC_SYS_LAST_UPDT_USER_SK", lit(0))
)

df_DefaultUNK = df_DefaultUNK.select(
    "AHY_ELIG_SK",
    "SRC_SYS_CD_SK",
    "CLNT_SRC_ID",
    "CLNT_GRP_NO",
    "CLNT_SUBGRP_NO",
    "EFF_DT_SK",
    "CLS_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "AHY_ELIG_TYP_SK",
    "CLS_SK",
    "CLS_PLN_SK",
    "GRP_SK",
    "SUBGRP_SK",
    "INCLD_NON_MBR_IN",
    "TERM_DT_SK",
    "SRC_SYS_LAST_UPDT_DT_SK",
    "SRC_SYS_LAST_UPDT_USER_SK"
)

# DefaultNA link
df_DefaultNA = (
    df_IdsAhyEligExtr_firstRow
    .withColumn("AHY_ELIG_SK", lit(1))
    .withColumn("SRC_SYS_CD_SK", lit(1))
    .withColumn("CLNT_SRC_ID", rpad(lit("NA"), 2, " "))
    .withColumn("CLNT_GRP_NO", rpad(lit("NA"), 2, " "))
    .withColumn("CLNT_SUBGRP_NO", rpad(lit("NA"), 2, " "))
    .withColumn("EFF_DT_SK", rpad(lit("1753-01-01"), 10, " "))
    .withColumn("CLS_ID", rpad(lit("NA"), 2, " "))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", lit(1))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(1))
    .withColumn("AHY_ELIG_TYP_SK", lit(1))
    .withColumn("CLS_SK", lit(1))
    .withColumn("CLS_PLN_SK", lit(1))
    .withColumn("GRP_SK", lit(1))
    .withColumn("SUBGRP_SK", lit(1))
    .withColumn("INCLD_NON_MBR_IN", rpad(lit("X"), 1, " "))
    .withColumn("TERM_DT_SK", rpad(lit("9999-12-31"), 10, " "))
    .withColumn("SRC_SYS_LAST_UPDT_DT_SK", rpad(lit("NA"), 2, " "))
    .withColumn("SRC_SYS_LAST_UPDT_USER_SK", lit(1))
)

df_DefaultNA = df_DefaultNA.select(
    "AHY_ELIG_SK",
    "SRC_SYS_CD_SK",
    "CLNT_SRC_ID",
    "CLNT_GRP_NO",
    "CLNT_SUBGRP_NO",
    "EFF_DT_SK",
    "CLS_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "AHY_ELIG_TYP_SK",
    "CLS_SK",
    "CLS_PLN_SK",
    "GRP_SK",
    "SUBGRP_SK",
    "INCLD_NON_MBR_IN",
    "TERM_DT_SK",
    "SRC_SYS_LAST_UPDT_DT_SK",
    "SRC_SYS_LAST_UPDT_USER_SK"
)

df_Collector = (
    df_Fkey
    .unionByName(df_DefaultUNK)
    .unionByName(df_DefaultNA)
)

df_Collector = df_Collector.select(
    "AHY_ELIG_SK",
    "SRC_SYS_CD_SK",
    "CLNT_SRC_ID",
    "CLNT_GRP_NO",
    "CLNT_SUBGRP_NO",
    "EFF_DT_SK",
    "CLS_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "AHY_ELIG_TYP_SK",
    "CLS_SK",
    "CLS_PLN_SK",
    "GRP_SK",
    "SUBGRP_SK",
    "INCLD_NON_MBR_IN",
    "TERM_DT_SK",
    "SRC_SYS_LAST_UPDT_DT_SK",
    "SRC_SYS_LAST_UPDT_USER_SK"
)

write_files(
    df_Collector,
    f"{adls_path}/load/AHY_ELIG.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)