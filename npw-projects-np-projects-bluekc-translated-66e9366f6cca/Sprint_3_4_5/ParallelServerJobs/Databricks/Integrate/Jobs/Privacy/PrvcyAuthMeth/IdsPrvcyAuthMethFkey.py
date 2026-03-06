# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 01/30/08 05:00:10 Batch  14640_18019 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 05/25/07 11:56:51 Batch  14390_43020 PROMOTE bckcetl ids20 dsadm bls for on
# MAGIC ^1_1 05/25/07 11:52:00 Batch  14390_42723 INIT bckcett testIDSnew dsadm bls for on
# MAGIC ^1_1 05/17/07 12:03:56 Batch  14382_43445 PROMOTE bckcett testIDSnew u10913 Ollie move from devl to test
# MAGIC ^1_1 05/17/07 09:42:32 Batch  14382_34957 PROMOTE bckcett testIDSnew u10913 Ollie move from devl to test
# MAGIC ^1_1 05/17/07 09:38:32 Batch  14382_34718 INIT bckcett devlIDS30 u10913 Ollie Move from devl to test
# MAGIC ^1_1 04/27/07 12:54:12 Batch  14362_46467 INIT bckcett devlIDS30 u10913 O. Nielsen move from devl to 4.3 Environment
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:          IdsCustSvcFkey
# MAGIC 
# MAGIC DESCRIPTION:   Takes the primary key file and applies the foreign keys.   
# MAGIC       
# MAGIC INPUTS:              Primary Key output file
# MAGIC 	
# MAGIC HASH FILES:       hf_recycle
# MAGIC 
# MAGIC TRANSFORMS:  GetFkeyCodes
# MAGIC                             GetFkeyErrorCnt
# MAGIC                             
# MAGIC PROCESSING:   Natural keys are converted to surrogate keys
# MAGIC 
# MAGIC OUTPUTS:         CUST_SVC table load file
# MAGIC                            hf_recycle - If recycling was used for this job records would be written to this hash file that had errors.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                            Hugh Sisson - 06/2006 - Originally programmed

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
from pyspark.sql.functions import col, lit, row_number, rpad
from pyspark.sql import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


InFile = get_widget_value("InFile", "")
Logging = get_widget_value("Logging", "")

schema_IdsPrvcyAuthMethExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38, 10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("PRVCY_AUTH_METH_SK", IntegerType(), False),
    StructField("PRVCY_AUTH_ID", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("PRVCY_AUTH_METH_TYP_CD_SK", IntegerType(), False),
    StructField("AUTH_METH_DESC", StringType(), False)
])

df_IdsPrvcyAuthMethExtr = (
    spark.read
    .option("sep", ",")
    .option("quote", "\"")
    .schema(schema_IdsPrvcyAuthMethExtr)
    .csv(f"{adls_path}/key/{InFile}")
)

df_temp = (
    df_IdsPrvcyAuthMethExtr
    .withColumn("SrcSysCdSk", GetFkeyCodes(trim(lit("IDS")), col("PRVCY_AUTH_METH_SK"), trim(lit("SOURCE SYSTEM")), trim(col("SRC_SYS_CD")), Logging))
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn("svPrvcyMbrSrcCdSk", GetFkeyCodes(col("SRC_SYS_CD"), col("PRVCY_AUTH_METH_SK"), lit("PRIVACY AUTHORIZATION TYPE"), col("PRVCY_AUTH_METH_TYP_CD_SK"), Logging))
    .withColumn("ErrCount", GetFkeyErrorCnt(col("PRVCY_AUTH_METH_SK")))
)

w = Window.orderBy(lit(1))
df_foreignkey = df_temp.withColumn("tmpRowId", row_number().over(w))

df_ForeignKey_Fkey = df_foreignkey.filter((col("ErrCount") == 0) | (col("PassThru") == lit("Y")))
df_ForeignKey_Recycle = df_foreignkey.filter(col("ErrCount") > 0)
df_ForeignKey_DefaultUNK = df_foreignkey.filter(col("tmpRowId") == 1)
df_ForeignKey_DefaultNA = df_foreignkey.filter(col("tmpRowId") == 1)

df_ForeignKey_Fkey2 = df_ForeignKey_Fkey.select(
    col("PRVCY_AUTH_METH_SK").alias("PRVCY_AUTH_METH_SK"),
    col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
    col("PRVCY_AUTH_ID").alias("PRVCY_AUTH_ID"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("svPrvcyMbrSrcCdSk").alias("PRVCY_AUTH_METH_TYP_CD_SK"),
    col("AUTH_METH_DESC").alias("AUTH_METH_DESC")
)

df_ForeignKey_Recycle2 = (
    df_ForeignKey_Recycle
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", GetRecycleKey(col("JOB_EXCTN_RCRD_ERR_SK")))
    .withColumn("RECYCLE_CT", col("RECYCLE_CT") + lit(1))
    .select(
        col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
        col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        col("DISCARD_IN").alias("DISCARD_IN"),
        col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        col("ErrCount").alias("ERR_CT"),
        col("RECYCLE_CT").alias("RECYCLE_CT"),
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        col("PRVCY_AUTH_METH_SK").alias("PRVCY_AUTH_METH_SK"),
        col("PRVCY_AUTH_ID").alias("PRVCY_AUTH_ID"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("PRVCY_AUTH_METH_TYP_CD_SK").alias("PRVCY_AUTH_METH_TYP_CD_SK"),
        col("AUTH_METH_DESC").alias("AUTH_METH_DESC")
    )
)

df_ForeignKey_DefaultUNK2 = df_ForeignKey_DefaultUNK.select(
    lit(0).alias("PRVCY_AUTH_METH_SK"),
    lit(0).alias("SRC_SYS_CD_SK"),
    lit("UNK").alias("PRVCY_AUTH_ID"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("PRVCY_AUTH_METH_TYP_CD_SK"),
    lit("UNK").alias("AUTH_METH_DESC")
)

df_ForeignKey_DefaultNA2 = df_ForeignKey_DefaultNA.select(
    lit(1).alias("PRVCY_AUTH_METH_SK"),
    lit(1).alias("SRC_SYS_CD_SK"),
    lit("NA").alias("PRVCY_AUTH_ID"),
    lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(1).alias("PRVCY_AUTH_METH_TYP_CD_SK"),
    lit("NA").alias("AUTH_METH_DESC")
)

df_ForeignKey_Recycle3 = (
    df_ForeignKey_Recycle2
    .withColumn("INSRT_UPDT_CD", rpad(col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", rpad(col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", rpad(col("PASS_THRU_IN"), 1, " "))
    .withColumn("SRC_SYS_CD", rpad(col("SRC_SYS_CD"), <...>, " "))
    .withColumn("PRI_KEY_STRING", rpad(col("PRI_KEY_STRING"), <...>, " "))
    .withColumn("PRVCY_AUTH_ID", rpad(col("PRVCY_AUTH_ID"), <...>, " "))
    .withColumn("AUTH_METH_DESC", rpad(col("AUTH_METH_DESC"), <...>, " "))
)

df_ForeignKey_Recycle_final = df_ForeignKey_Recycle3.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "PRVCY_AUTH_METH_SK",
    "PRVCY_AUTH_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "PRVCY_AUTH_METH_TYP_CD_SK",
    "AUTH_METH_DESC"
)

write_files(
    df_ForeignKey_Recycle_final,
    f"{adls_path}/hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_Collector = (
    df_ForeignKey_Fkey2
    .unionByName(df_ForeignKey_DefaultUNK2)
    .unionByName(df_ForeignKey_DefaultNA2)
)

df_Collector2 = (
    df_Collector
    .withColumn("PRVCY_AUTH_ID", rpad(col("PRVCY_AUTH_ID"), <...>, " "))
    .withColumn("AUTH_METH_DESC", rpad(col("AUTH_METH_DESC"), <...>, " "))
)

df_Collector_final = df_Collector2.select(
    "PRVCY_AUTH_METH_SK",
    "SRC_SYS_CD_SK",
    "PRVCY_AUTH_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "PRVCY_AUTH_METH_TYP_CD_SK",
    "AUTH_METH_DESC"
)

write_files(
    df_Collector_final,
    f"{adls_path}/load/PRVCY_AUTH_METH.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)