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
# MAGIC PROCESSING: Processes records from Primary key process and assigns foreingn keys and loads into IDS MBR_SRVY_QSTN table
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 \(9)Project/Altiris #\(9)Change Description\(9)\(9)\(9)\(9)Development Project\(9)Code Reviewer\(9)Date Reviewed       
# MAGIC ------------------              --------------------     \(9)------------------------\(9)-----------------------------------------------------------------------\(9)--------------------------------\(9)-------------------------------\(9)----------------------------       
# MAGIC Bhoomi Dasari         2011-04-07               4673                       Original Programming                                                IntegrateWrhsDevl                   SAndrew                2011-04-20  
# MAGIC Kalyan Neelam        2012-09-19               4830 & 4735           Added new column on end                                       IntegrateNewDevl                  Bhoomi Dasari          2012-09-24 
# MAGIC                                                                                                MBR_SRVY_QSTN_RSPN_STORED_IN

# MAGIC MBR_SRVY_QSTN Foreign Key job
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
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DecimalType
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


InFile = get_widget_value("InFile","")
Logging = get_widget_value("Logging","")
SrcSysCd = get_widget_value("SrcSysCd","")

schema_MbrSrvyQstn = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38,10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("MBR_SRVY_QSTN_SK", IntegerType(), False),
    StructField("SRC_SYS_CD_SK", IntegerType(), False),
    StructField("MBR_SRVY_TYP_CD", StringType(), False),
    StructField("QSTN_CD_TX", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("MBR_SRVY_SK", IntegerType(), True),
    StructField("MBR_SRVY_TYP_CD_SK", IntegerType(), False),
    StructField("EFF_YR_MO_SK", StringType(), False),
    StructField("TERM_YR_MO_SK", StringType(), False),
    StructField("ANSWER_DTYP_TX", StringType(), True),
    StructField("MBR_SRVY_QSTN_ANSWER_TYP_TX", StringType(), False),
    StructField("QSTN_TX", StringType(), True),
    StructField("MBR_SRVY_TYP_CD_SRC_CD", StringType(), False),
    StructField("MBR_SRVY_QSTN_RSPN_STORED_IN", StringType(), False)
])

df_MbrSrvyQstn = (
    spark.read
    .option("header", "false")
    .option("sep", ",")
    .option("quote", "\"")
    .schema(schema_MbrSrvyQstn)
    .csv(f"{adls_path}/key/{InFile}")
)

df_withvars = (
    df_MbrSrvyQstn
    .withColumn(
        "svSrcSysCdSk",
        GetFkeyCodes(
            F.lit("IDS"),
            F.col("MBR_SRVY_QSTN_SK"),
            F.lit("SOURCE SYSTEM"),
            F.col("SRC_SYS_CD"),
            get_widget_value("Logging","")
        )
    )
    .withColumn(
        "svMbrSrvyTypCdSk",
        GetFkeyCodes(
            F.lit("BCBSKCCOMMON"),
            F.col("MBR_SRVY_QSTN_SK"),
            F.lit("MEMBER SURVEY TYPE"),
            F.col("MBR_SRVY_TYP_CD_SRC_CD"),
            get_widget_value("Logging","")
        )
    )
    .withColumn(
        "svMbrSrvySk",
        GetFkeyMbrSrvy(
            F.col("SRC_SYS_CD"),
            F.col("MBR_SRVY_QSTN_SK"),
            F.col("MBR_SRVY_TYP_CD"),
            get_widget_value("Logging","")
        )
    )
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("MBR_SRVY_QSTN_SK")))
)

df_Fkey = (
    df_withvars
    .filter((F.col("ErrCount") == 0) | (F.col("PassThru") == "Y"))
    .select(
        F.col("MBR_SRVY_QSTN_SK").alias("MBR_SRVY_QSTN_SK"),
        F.col("svSrcSysCdSk").alias("SRC_SYS_CD_SK"),
        F.col("MBR_SRVY_TYP_CD").alias("MBR_SRVY_TYP_CD"),
        F.col("QSTN_CD_TX").alias("QSTN_CD_TX"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("svMbrSrvySk").alias("MBR_SRVY_SK"),
        F.col("svMbrSrvyTypCdSk").alias("MBR_SRVY_TYP_CD_SK"),
        F.col("EFF_YR_MO_SK").alias("EFF_YR_MO_SK"),
        F.col("TERM_YR_MO_SK").alias("TERM_YR_MO_SK"),
        F.col("ANSWER_DTYP_TX").alias("ANSWER_DTYP_TX"),
        F.col("MBR_SRVY_QSTN_ANSWER_TYP_TX").alias("MBR_SRVY_QSTN_ANSWER_TYP_TX"),
        F.col("QSTN_TX").alias("QSTN_TX"),
        F.col("MBR_SRVY_QSTN_RSPN_STORED_IN").alias("MBR_SRVY_QSTN_RSPN_STORED_IN")
    )
)

df_Recycle = (
    df_withvars
    .filter(F.col("ErrCount") > 0)
    .select(
        GetRecycleKey(F.col("MBR_SRVY_QSTN_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("DISCARD_IN").alias("DISCARD_IN"),
        F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("ErrCount").alias("ERR_CT"),
        (F.col("RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("MBR_SRVY_QSTN_SK").alias("MBR_SRVY_QSTN_SK"),
        F.col("svSrcSysCdSk").alias("SRC_SYS_CD_SK"),
        F.col("MBR_SRVY_TYP_CD").alias("MBR_SRVY_TYP_CD"),
        F.col("QSTN_CD_TX").alias("QSTN_CD_TX"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("MBR_SRVY_SK").alias("MBR_SRVY_SK"),
        F.col("MBR_SRVY_TYP_CD_SK").alias("MBR_SRVY_TYP_CD_SK"),
        F.col("EFF_YR_MO_SK").alias("EFF_YR_MO_SK"),
        F.col("TERM_YR_MO_SK").alias("TERM_YR_MO_SK"),
        F.col("ANSWER_DTYP_TX").alias("ANSWER_DTYP_TX"),
        F.col("MBR_SRVY_QSTN_ANSWER_TYP_TX").alias("MBR_SRVY_QSTN_ANSWER_TYP_TX"),
        F.col("QSTN_TX").alias("QSTN_TX"),
        F.col("MBR_SRVY_QSTN_RSPN_STORED_IN").alias("MBR_SRVY_QSTN_RSPN_STORED_IN")
    )
)

df_Recycle_final = df_Recycle.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "MBR_SRVY_QSTN_SK",
    "SRC_SYS_CD_SK",
    "MBR_SRVY_TYP_CD",
    "QSTN_CD_TX",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "MBR_SRVY_SK",
    "MBR_SRVY_TYP_CD_SK",
    "EFF_YR_MO_SK",
    "TERM_YR_MO_SK",
    "ANSWER_DTYP_TX",
    "MBR_SRVY_QSTN_ANSWER_TYP_TX",
    "QSTN_TX",
    "MBR_SRVY_QSTN_RSPN_STORED_IN"
)

write_files(
    df_Recycle_final,
    "hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

w = Window.orderBy(F.lit(1))
df_with_rn = df_withvars.withColumn("rn", F.row_number().over(w))

df_DefaultNA = df_with_rn.filter(F.col("rn") == 1).select(
    F.lit(1).alias("MBR_SRVY_QSTN_SK"),
    F.lit(1).alias("SRC_SYS_CD_SK"),
    F.lit("NA").alias("MBR_SRVY_TYP_CD"),
    F.lit("NA").alias("QSTN_CD_TX"),
    F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("MBR_SRVY_SK"),
    F.lit(1).alias("MBR_SRVY_TYP_CD_SK"),
    F.lit("175301").alias("EFF_YR_MO_SK"),
    F.lit("219912").alias("TERM_YR_MO_SK"),
    F.lit("NA").alias("ANSWER_DTYP_TX"),
    F.lit("NA").alias("MBR_SRVY_QSTN_ANSWER_TYP_TX"),
    F.lit("NA").alias("QSTN_TX"),
    F.lit("N").alias("MBR_SRVY_QSTN_RSPN_STORED_IN")
)

df_DefaultUNK = df_with_rn.filter(F.col("rn") == 1).select(
    F.lit(0).alias("MBR_SRVY_QSTN_SK"),
    F.lit(0).alias("SRC_SYS_CD_SK"),
    F.lit("UNK").alias("MBR_SRVY_TYP_CD"),
    F.lit("UNK").alias("QSTN_CD_TX"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("MBR_SRVY_SK"),
    F.lit(0).alias("MBR_SRVY_TYP_CD_SK"),
    F.lit("175301").alias("EFF_YR_MO_SK"),
    F.lit("219912").alias("TERM_YR_MO_SK"),
    F.lit("UNK").alias("ANSWER_DTYP_TX"),
    F.lit("UNK").alias("MBR_SRVY_QSTN_ANSWER_TYP_TX"),
    F.lit("UNK").alias("QSTN_TX"),
    F.lit("N").alias("MBR_SRVY_QSTN_RSPN_STORED_IN")
)

df_Fkey_aligned = df_Fkey.select(
    "MBR_SRVY_QSTN_SK",
    "SRC_SYS_CD_SK",
    "MBR_SRVY_TYP_CD",
    "QSTN_CD_TX",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "MBR_SRVY_SK",
    "MBR_SRVY_TYP_CD_SK",
    "EFF_YR_MO_SK",
    "TERM_YR_MO_SK",
    "ANSWER_DTYP_TX",
    "MBR_SRVY_QSTN_ANSWER_TYP_TX",
    "QSTN_TX",
    "MBR_SRVY_QSTN_RSPN_STORED_IN"
)

df_Collector = (
    df_Fkey_aligned
    .unionByName(df_DefaultNA)
    .unionByName(df_DefaultUNK)
)

df_Final = df_Collector.select(
    F.col("MBR_SRVY_QSTN_SK"),
    F.col("SRC_SYS_CD_SK"),
    F.col("MBR_SRVY_TYP_CD"),
    F.col("QSTN_CD_TX"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("MBR_SRVY_SK"),
    F.col("MBR_SRVY_TYP_CD_SK"),
    F.rpad(F.col("EFF_YR_MO_SK"), 6, " ").alias("EFF_YR_MO_SK"),
    F.rpad(F.col("TERM_YR_MO_SK"), 6, " ").alias("TERM_YR_MO_SK"),
    F.col("ANSWER_DTYP_TX"),
    F.col("MBR_SRVY_QSTN_ANSWER_TYP_TX"),
    F.col("QSTN_TX"),
    F.rpad(F.col("MBR_SRVY_QSTN_RSPN_STORED_IN"), 1, " ").alias("MBR_SRVY_QSTN_RSPN_STORED_IN")
)

write_files(
    df_Final,
    f"{adls_path}/load/MBR_SRVY_QSTN.{SrcSysCd}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)