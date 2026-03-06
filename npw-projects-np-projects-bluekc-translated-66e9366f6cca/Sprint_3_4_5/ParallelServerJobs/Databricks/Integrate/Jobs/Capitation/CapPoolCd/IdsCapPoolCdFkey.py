# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2011 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsPoolCdFkey
# MAGIC 
# MAGIC DESCRIPTION:     Takes the sorted file and applies the foreign keys.   
# MAGIC       
# MAGIC 
# MAGIC INPUTS:   File created by IdsPoolCdExtr
# MAGIC 	
# MAGIC   
# MAGIC PROCESSING:   Finds foreign keys for surrogate key fields
# MAGIC 
# MAGIC MODIFICATIONS:                                                                                                                                                                                                                                                                                         
# MAGIC                                                                                                                                                                                                                                                                                                                                                             
# MAGIC Date                 Developer                Project                     Change Description                                                                                                                Environment              Reviewer                Date
# MAGIC ------------------      ----------------------------     --------------------             --------------------------------------------------------------------------------------------------------------------                          ----------------------------   ---------------------         -------------------
# MAGIC 2011-03-21     Ralph Tucker            TTR-1058                 Origionally Programmed                                                                                                         IntegrateNewDevl    SAndrew               2011-04-07

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
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DecimalType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


InFile = get_widget_value('InFile', 'FctsPoolCdExtr.PoolCd.dat')
Logging = get_widget_value('Logging', 'Y')
SrcSysCdSk = get_widget_value('SrcSysCdSk', '1581')
CurrRunCyc = get_widget_value('CurrRunCyc', '100')

schema_IdsPoolCd = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38, 10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("CAP_POOL_CD_SK", IntegerType(), False),
    StructField("CAP_POOL_CD", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("CAP_POOL_NM", StringType(), False)
])

df_IdsPoolCd = (
    spark.read.format("csv")
    .option("quote", "\"")
    .option("header", "false")
    .schema(schema_IdsPoolCd)
    .load(f"{adls_path}/key/{InFile}")
)

windowSpec = Window.orderBy(F.lit(1))
df_IdsPoolCd = df_IdsPoolCd.withColumn("rownum", F.row_number().over(windowSpec))

dfFkey = df_IdsPoolCd.select(
    F.col("CAP_POOL_CD_SK").alias("CAP_POOL_CD_SK"),
    F.col("CAP_POOL_CD").alias("CAP_POOL_CD"),
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.lit(CurrRunCyc).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCyc).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CAP_POOL_NM").alias("CAP_POOL_NM")
)

dfDefaultUNK = df_IdsPoolCd.filter(F.col("rownum") == 1).select(
    F.lit(0).alias("CAP_POOL_CD_SK"),
    F.lit("UNK").alias("CAP_POOL_CD"),
    F.lit(0).alias("SRC_SYS_CD_SK"),
    F.lit(CurrRunCyc).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCyc).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit("UNK").alias("CAP_POOL_NM")
)

dfDefaultNA = df_IdsPoolCd.filter(F.col("rownum") == 1).select(
    F.lit(1).alias("CAP_POOL_CD_SK"),
    F.lit("NA").alias("CAP_POOL_CD"),
    F.lit(1).alias("SRC_SYS_CD_SK"),
    F.lit(CurrRunCyc).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCyc).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit("NA").alias("CAP_POOL_NM")
)

dfCollector = dfFkey.union(dfDefaultUNK).union(dfDefaultNA)

df_final = dfCollector.select(
    "CAP_POOL_CD_SK",
    "CAP_POOL_CD",
    "SRC_SYS_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CAP_POOL_NM"
)

write_files(
    df_final,
    f"{adls_path}/load/CAP_POOL_CD.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)