# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsFeeDscntFkey
# MAGIC 
# MAGIC DESCRIPTION:     Takes the sorted file and applies the foreign keys.   
# MAGIC       
# MAGIC 
# MAGIC INPUTS:   File created by IdsFeeDscntExtr
# MAGIC 	
# MAGIC   
# MAGIC HASH FILES:     hf_recycle
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  Fkey lookups
# MAGIC                            
# MAGIC 
# MAGIC PROCESSING:   Finds foreign keys for surrogate key fields
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC             Ralph Tucker   10/05/2005  -   Originally Programmed
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC 
# MAGIC Manasa Andru                 9/28/2013          TFS-1272                    Bringing up to standards                                       IntegrateNewDevl          Kalyan Neelam           2013-10-03

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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
from pyspark.sql.functions import col, lit
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


InFile = get_widget_value('InFile','')
Logging = get_widget_value('Logging','Y')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')

schema_IdsBillEnty = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38, 10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("BILL_ENTY_SK", IntegerType(), False),
    StructField("BILL_ENTY_UNIQ_KEY", IntegerType(), False),
    StructField("GRP_ID", StringType(), False),
    StructField("SUBGRP_ID", StringType(), False),
    StructField("SUB_ID", StringType(), False),
    StructField("BILL_ENTY_LVL_CD", StringType(), False),
    StructField("BILL_ENTY_LVL_UNIQ_KEY", IntegerType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("SUB_CK", IntegerType(), False)
])

df_IdsBillEnty = spark.read.csv(
    path=f"{adls_path}/key/{InFile}",
    schema=schema_IdsBillEnty,
    sep=",",
    quote="\"",
    header=False
)

df_foreignKey = (
    df_IdsBillEnty
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn("svGrpSk", GetFkeyGrp(col("SRC_SYS_CD"), col("BILL_ENTY_SK"), col("GRP_ID"), Logging))
    .withColumn("svSubGrpSk", GetFkeySubgrp(col("SRC_SYS_CD"), col("BILL_ENTY_SK"), col("GRP_ID"), col("SUBGRP_ID"), Logging))
    .withColumn("svSubSk", GetFkeySub(col("SRC_SYS_CD"), col("BILL_ENTY_SK"), col("SUB_CK"), Logging))
    .withColumn("svBillEntyLvlCdSk", GetFkeyCodes(col("SRC_SYS_CD"), col("BILL_ENTY_SK"), lit("BILLING ENTITY LEVEL"), col("BILL_ENTY_LVL_CD"), Logging))
    .withColumn("ErrCount", GetFkeyErrorCnt(col("BILL_ENTY_SK")))
    .withColumn("SRC_SYS_CD_SK", lit(SrcSysCdSk))
)

df_foreignKey_fkey = (
    df_foreignKey
    .filter((col("ErrCount") == 0) | (col("PassThru") == 'Y'))
    .select(
        col("BILL_ENTY_SK"),
        col("SRC_SYS_CD_SK"),
        col("BILL_ENTY_UNIQ_KEY"),
        col("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("svGrpSk").alias("GRP_SK"),
        col("svSubGrpSk").alias("SUBGRP_SK"),
        col("svSubSk").alias("SUB_SK"),
        col("svBillEntyLvlCdSk").alias("BILL_ENTY_LVL_CD_SK"),
        col("BILL_ENTY_LVL_UNIQ_KEY")
    )
)

df_temp_unk = df_foreignKey.limit(1)
df_foreignKey_defaultUNK = df_temp_unk.select(
    lit(0).alias("BILL_ENTY_SK"),
    lit(0).alias("SRC_SYS_CD_SK"),
    lit(0).alias("BILL_ENTY_UNIQ_KEY"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("GRP_SK"),
    lit(0).alias("SUBGRP_SK"),
    lit(0).alias("SUB_SK"),
    lit(0).alias("BILL_ENTY_LVL_CD_SK"),
    lit(0).alias("BILL_ENTY_LVL_UNIQ_KEY")
)

df_temp_na = df_foreignKey.limit(1)
df_foreignKey_defaultNA = df_temp_na.select(
    lit(1).alias("BILL_ENTY_SK"),
    lit(1).alias("SRC_SYS_CD_SK"),
    lit(1).alias("BILL_ENTY_UNIQ_KEY"),
    lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(1).alias("GRP_SK"),
    lit(1).alias("SUBGRP_SK"),
    lit(1).alias("SUB_SK"),
    lit(1).alias("BILL_ENTY_LVL_CD_SK"),
    lit(1).alias("BILL_ENTY_LVL_UNIQ_KEY")
)

df_foreignKey_recycle = (
    df_foreignKey
    .filter(col("ErrCount") > 0)
    .select(
        GetRecycleKey(col("BILL_ENTY_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        col("INSRT_UPDT_CD"),
        col("DISCARD_IN"),
        col("PASS_THRU_IN"),
        col("FIRST_RECYC_DT"),
        col("ErrCount").alias("ERR_CT"),
        col("RECYCLE_CT"),
        col("SRC_SYS_CD"),
        col("PRI_KEY_STRING"),
        col("BILL_ENTY_SK"),
        col("BILL_ENTY_UNIQ_KEY"),
        col("GRP_ID"),
        col("SUBGRP_ID"),
        col("SUB_ID"),
        col("BILL_ENTY_LVL_CD"),
        col("BILL_ENTY_LVL_UNIQ_KEY"),
        col("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK")
    )
)

write_files(
    df_foreignKey_recycle,
    f"{adls_path}/hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_parquet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_collector = df_foreignKey_fkey.unionByName(df_foreignKey_defaultUNK).unionByName(df_foreignKey_defaultNA)

df_final = df_collector.select(
    "BILL_ENTY_SK",
    "SRC_SYS_CD_SK",
    "BILL_ENTY_UNIQ_KEY",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "GRP_SK",
    "SUBGRP_SK",
    "SUB_SK",
    "BILL_ENTY_LVL_CD_SK",
    "BILL_ENTY_LVL_UNIQ_KEY"
)

write_files(
    df_final,
    f"{adls_path}/load/BILL_ENTY.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)