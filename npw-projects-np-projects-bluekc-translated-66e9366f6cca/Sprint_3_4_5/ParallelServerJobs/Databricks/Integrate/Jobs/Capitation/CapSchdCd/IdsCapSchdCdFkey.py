# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2011 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsSchedCdFkey
# MAGIC 
# MAGIC DESCRIPTION:     Takes the sorted file and applies the foreign keys.   
# MAGIC       
# MAGIC 
# MAGIC INPUTS:   File created by IdsSchedCdExtr
# MAGIC 	
# MAGIC   
# MAGIC PROCESSING:   Finds foreign keys for surrogate key fields
# MAGIC 
# MAGIC MODIFICATIONS:                                                                                                                                                                                                                                                                                         
# MAGIC                                                                                                                                                                                                                                                                                                                                                             
# MAGIC Date                 Developer                Project                     Change Description                                                                                                                Environment              Reviewer                Date
# MAGIC ------------------      ----------------------------     --------------------             --------------------------------------------------------------------------------------------------------------------                          ----------------------------   ---------------------         -------------------
# MAGIC 2011-03-11     Ralph Tucker            TTR-1058                 Origionally Programmed                                                                                                         IntegrateNewDevl     SAndrew              2011-04-11

# MAGIC Read common record format file.
# MAGIC Writing Sequential File to /load
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


InFile = get_widget_value("InFile", "")
Logging = get_widget_value("Logging", "")
SrcSysCdSk = get_widget_value("SrcSysCdSk", "")
CurrRunCyc = get_widget_value("CurrRunCyc", "")

schema_IdsSchdCd = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38,10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("CAP_SCHD_CD_SK", IntegerType(), False),
    StructField("CAP_SCHD_CD", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("CAP_SCHD_NM", StringType(), False)
])

df_IdsSchdCd = (
    spark.read
    .option("sep", ",")
    .option("header", False)
    .option("quote", "\"")
    .schema(schema_IdsSchdCd)
    .csv(f"{adls_path}/key/{InFile}")
)

df_Fkey = df_IdsSchdCd.select(
    F.col("CAP_SCHD_CD_SK").alias("CAP_SCHD_CD_SK"),
    F.col("CAP_SCHD_CD").alias("CAP_SCHD_CD"),
    F.lit(SrcSysCdSk).cast("int").alias("SRC_SYS_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CAP_SCHD_NM").alias("CAP_SCHD_NM")
)

df_DefaultUNK = df_IdsSchdCd.limit(1).select(
    F.lit(0).alias("CAP_SCHD_CD_SK"),
    F.lit("UNK").alias("CAP_SCHD_CD"),
    F.lit(0).alias("SRC_SYS_CD_SK"),
    F.lit(CurrRunCyc).cast("int").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCyc).cast("int").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit("UNK").alias("CAP_SCHD_NM")
)

df_DefaultNA = df_IdsSchdCd.limit(1).select(
    F.lit(1).alias("CAP_SCHD_CD_SK"),
    F.lit("NA").alias("CAP_SCHD_CD"),
    F.lit(1).alias("SRC_SYS_CD_SK"),
    F.lit(CurrRunCyc).cast("int").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCyc).cast("int").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit("NA").alias("CAP_SCHD_NM")
)

df_collector = df_Fkey.unionByName(df_DefaultUNK).unionByName(df_DefaultNA)

df_final = (
    df_collector
    .withColumn("CAP_SCHD_CD", F.rpad(F.col("CAP_SCHD_CD"), 50, " "))
    .withColumn("CAP_SCHD_NM", F.rpad(F.col("CAP_SCHD_NM"), 50, " "))
    .select(
        "CAP_SCHD_CD_SK",
        "CAP_SCHD_CD",
        "SRC_SYS_CD_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CAP_SCHD_NM"
    )
)

write_files(
    df_final,
    f"{adls_path}/load/CAP_SCHD_CD.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)