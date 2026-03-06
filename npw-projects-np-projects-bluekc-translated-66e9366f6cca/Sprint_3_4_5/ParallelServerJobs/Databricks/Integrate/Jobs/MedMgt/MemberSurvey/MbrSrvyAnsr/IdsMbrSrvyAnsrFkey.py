# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2011 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  F2FMbrSrvyQstnAnswrExtrSeq
# MAGIC                
# MAGIC 
# MAGIC PROCESSING: Processes records from Primary key process and assigns foreingn keys and loads into IDS MBR_SRVY_ANSWER table
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 \(9)Project/Altiris #\(9)Change Description\(9)\(9)\(9)\(9)Development Project\(9)Code Reviewer\(9)Date Reviewed       
# MAGIC ------------------              --------------------     \(9)------------------------\(9)-----------------------------------------------------------------------\(9)--------------------------------\(9)-------------------------------\(9)----------------------------       
# MAGIC Bhoomi Dasari         2011-04-07               4673                       Original Programming                                                IntegrateWrhsDevl                  SAndrew                2011-04-20    
# MAGIC 
# MAGIC Reddy Sanam         2022-06-15              US492429              Changed column Length from 100 to 255 for              IntegrateDev2                        Goutham Kalidindi  2022-07-14
# MAGIC                                                                                               "MBR_SRVY_ANSWER_CD_TX" field

# MAGIC MBR_SRVY_ANSWER Foreign Key job
# MAGIC Writing Sequential File to /load
# MAGIC Merge source data with default rows
# MAGIC Set all foreign surrogate keys
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
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
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


InFile = get_widget_value("InFile","")
Logging = get_widget_value("Logging","")
SrcSysCd = get_widget_value("SrcSysCd","")

schema_MbrSrvyAnsr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38, 10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("MBR_SRVY_ANSWER_SK", IntegerType(), nullable=False),
    StructField("SRC_SYS_CD_SK", IntegerType(), nullable=False),
    StructField("MBR_SRVY_TYP_CD", StringType(), nullable=False),
    StructField("MBR_SRVY_QSTN_CD_TX", StringType(), nullable=True),
    StructField("MBR_SRVY_ANSWER_CD_TX", StringType(), nullable=True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("MBR_SRVY_QSTN_SK", IntegerType(), nullable=False)
])

df_MbrSrvyAnsr = (
    spark.read.format("csv")
    .option("header", "false")
    .option("quote", "\"")
    .schema(schema_MbrSrvyAnsr)
    .load(f"{adls_path}/key/{InFile}")
)

df_foreign_key = (
    df_MbrSrvyAnsr
    .withColumn(
        "svSrcSysCdSk",
        GetFkeyCodes(
            "IDS",
            F.col("MBR_SRVY_ANSWER_SK"),
            "SOURCE SYSTEM",
            F.col("SRC_SYS_CD"),
            Logging
        )
    )
    .withColumn(
        "svMbrSrvyQstnSk",
        GetFkeyMbrSrvyQstn(
            F.col("SRC_SYS_CD"),
            F.col("MBR_SRVY_ANSWER_SK"),
            F.col("MBR_SRVY_TYP_CD"),
            F.col("MBR_SRVY_QSTN_CD_TX"),
            Logging
        )
    )
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("MBR_SRVY_ANSWER_SK")))
)

w = Window.orderBy(F.lit(1))
df_foreign_key = df_foreign_key.withColumn("RN", F.row_number().over(w))

df_Fkey = (
    df_foreign_key
    .filter((F.col("ErrCount") == 0) | (F.col("PassThru") == "Y"))
    .select(
        F.col("MBR_SRVY_ANSWER_SK"),
        F.col("svSrcSysCdSk").alias("SRC_SYS_CD_SK"),
        F.col("MBR_SRVY_TYP_CD"),
        F.col("MBR_SRVY_QSTN_CD_TX"),
        F.col("MBR_SRVY_ANSWER_CD_TX"),
        F.col("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("svMbrSrvyQstnSk").alias("MBR_SRVY_QSTN_SK")
    )
)

df_Recycle = (
    df_foreign_key
    .filter(F.col("ErrCount") > 0)
    .select(
        GetRecycleKey(F.col("MBR_SRVY_ANSWER_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("DISCARD_IN").alias("DISCARD_IN"),
        F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("ErrCount").alias("ERR_CT"),
        (F.col("RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("MBR_SRVY_ANSWER_SK").alias("MBR_SRVY_ANSWER_SK"),
        F.col("svSrcSysCdSk").alias("SRC_SYS_CD_SK"),
        F.col("MBR_SRVY_TYP_CD").alias("MBR_SRVY_TYP_CD"),
        F.col("MBR_SRVY_QSTN_CD_TX").alias("MBR_SRVY_QSTN_CD_TX"),
        F.col("MBR_SRVY_ANSWER_CD_TX").alias("MBR_SRVY_ANSWER_CD_TX"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("MBR_SRVY_QSTN_SK").alias("MBR_SRVY_QSTN_SK")
    )
)

df_Recycle = df_Recycle.withColumn(
    "INSRT_UPDT_CD",
    rpad(F.col("INSRT_UPDT_CD"), 10, " ")
).withColumn(
    "DISCARD_IN",
    rpad(F.col("DISCARD_IN"), 1, " ")
).withColumn(
    "PASS_THRU_IN",
    rpad(F.col("PASS_THRU_IN"), 1, " ")
).withColumn(
    "SRC_SYS_CD",
    rpad(F.col("SRC_SYS_CD"), 200, " ")
).withColumn(
    "PRI_KEY_STRING",
    rpad(F.col("PRI_KEY_STRING"), 200, " ")
).withColumn(
    "MBR_SRVY_TYP_CD",
    rpad(F.col("MBR_SRVY_TYP_CD"), 200, " ")
).withColumn(
    "MBR_SRVY_QSTN_CD_TX",
    rpad(F.col("MBR_SRVY_QSTN_CD_TX"), 200, " ")
).withColumn(
    "MBR_SRVY_ANSWER_CD_TX",
    rpad(F.col("MBR_SRVY_ANSWER_CD_TX"), 200, " ")
)

write_files(
    df_Recycle.select(
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "PASS_THRU_IN",
        "FIRST_RECYC_DT",
        "ERR_CT",
        "RECYCLE_CT",
        "SRC_SYS_CD",
        "PRI_KEY_STRING",
        "MBR_SRVY_ANSWER_SK",
        "SRC_SYS_CD_SK",
        "MBR_SRVY_TYP_CD",
        "MBR_SRVY_QSTN_CD_TX",
        "MBR_SRVY_ANSWER_CD_TX",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "MBR_SRVY_QSTN_SK"
    ),
    "hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_DefaultNA = (
    df_foreign_key
    .filter(F.col("RN") == 1)
    .select(
        F.lit(1).alias("MBR_SRVY_ANSWER_SK"),
        F.lit(1).alias("SRC_SYS_CD_SK"),
        F.lit("NA").alias("MBR_SRVY_TYP_CD"),
        F.lit("NA").alias("MBR_SRVY_QSTN_CD_TX"),
        F.lit("NA").alias("MBR_SRVY_ANSWER_CD_TX"),
        F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("MBR_SRVY_QSTN_SK")
    )
)

df_DefaultUNK = (
    df_foreign_key
    .filter(F.col("RN") == 1)
    .select(
        F.lit(0).alias("MBR_SRVY_ANSWER_SK"),
        F.lit(0).alias("SRC_SYS_CD_SK"),
        F.lit("UNK").alias("MBR_SRVY_TYP_CD"),
        F.lit("UNK").alias("MBR_SRVY_QSTN_CD_TX"),
        F.lit("UNK").alias("MBR_SRVY_ANSWER_CD_TX"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("MBR_SRVY_QSTN_SK")
    )
)

df_collector = df_Fkey.unionByName(df_DefaultNA).unionByName(df_DefaultUNK)

df_collector = df_collector.withColumn(
    "MBR_SRVY_TYP_CD",
    rpad(F.col("MBR_SRVY_TYP_CD"), 200, " ")
).withColumn(
    "MBR_SRVY_QSTN_CD_TX",
    rpad(F.col("MBR_SRVY_QSTN_CD_TX"), 200, " ")
).withColumn(
    "MBR_SRVY_ANSWER_CD_TX",
    rpad(F.col("MBR_SRVY_ANSWER_CD_TX"), 200, " ")
)

df_collector = df_collector.select(
    "MBR_SRVY_ANSWER_SK",
    "SRC_SYS_CD_SK",
    "MBR_SRVY_TYP_CD",
    "MBR_SRVY_QSTN_CD_TX",
    "MBR_SRVY_ANSWER_CD_TX",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "MBR_SRVY_QSTN_SK"
)

write_files(
    df_collector,
    f"{adls_path}/load/MBR_SRVY_ANSWER.{SrcSysCd}.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)