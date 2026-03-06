# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2020 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC DESCRIPTION:     Foreign Key Building for load file
# MAGIC       
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer             Date                         Change Description                                                     Project #                  Development Project          Code Reviewer          Date Reviewed  
# MAGIC ------------------           ----------------------------     ----------------------------------------------------------------------------       ----------------                ------------------------------------       ----------------------------      ----------------
# MAGIC Bhoomi D              03/27/2008            New Job                                                                       3255                         devlIDScur                          Steph Goddard          03/31/2008
# MAGIC 
# MAGIC Parik                    2008-08-13              Added the source system code surrogate key as           3567(Primary key)     devlIDS                              Steph Goddard          08/14/2008
# MAGIC                                                             parameter        
# MAGIC Reddy Sanam                                      Edited the derivation for this stage variable 
# MAGIC                                                             svItsClmMsgFmtCdSk to set to 'FACETS'
# MAGIC                                                             if the source code is 'LUMERIS' when 
# MAGIC                                                             calling the routine
# MAGIC 
# MAGIC Emran.Mohammad       2020-10-12                          Brought Up to Standards

# MAGIC This hash file is written to by all claim foriegn key jobs.
# MAGIC Read common record format file from extract job.
# MAGIC Set all foreign surrogate keys
# MAGIC Writing Sequential File to /load
# MAGIC Merge source data with default rows
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


Source = get_widget_value('Source','FACETS')
InFile = get_widget_value('InFile','FctsITSClmMsgExtr.FctsITSClmMsg.dat.20080331')
Logging = get_widget_value('Logging','N')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')

schema_ClmCrf = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38, 10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("ITS_CLM_MSG_SK", IntegerType(), nullable=False),
    StructField("CLM_ID", StringType(), nullable=False),
    StructField("ITS_CLM_MSG_FMT_CD", StringType(), nullable=False),
    StructField("ITS_CLM_MSG_ID", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("ITS_CLM_MSG_DESC", StringType(), nullable=True)
])

df_ClmCrf = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "\"")
    .schema(schema_ClmCrf)
    .load(f"{adls_path}/key/{InFile}")
)

df_ForeignKeyVars = (
    df_ClmCrf
    .withColumn(
        "svItsClmMsgFmtCdSk",
        GetFkeyCodes(
            F.when(F.col("SRC_SYS_CD") == "LUMERIS", "FACETS").otherwise(F.col("SRC_SYS_CD")),
            F.col("ITS_CLM_MSG_SK"),
            F.lit("ITS CLAIM MESSAGE FORMAT"),
            F.col("ITS_CLM_MSG_FMT_CD"),
            F.lit(Logging)
        )
    )
    .withColumn(
        "svItsClmSk",
        GetFkeyClm(
            F.col("SRC_SYS_CD"),
            F.col("ITS_CLM_MSG_SK"),
            F.col("CLM_ID"),
            F.lit(Logging)
        )
    )
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("ITS_CLM_MSG_SK")))
    .withColumn("SrcSysCdSk", F.lit(SrcSysCdSk))
)

df_ForeignKey_Fkey = (
    df_ForeignKeyVars
    .filter((F.col("ErrCount") == 0) | (F.col("PassThru") == 'Y'))
    .select(
        F.col("ITS_CLM_MSG_SK"),
        F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
        F.col("CLM_ID"),
        F.col("svItsClmMsgFmtCdSk").alias("ITS_CLM_MSG_FMT_CD_SK"),
        F.col("ITS_CLM_MSG_ID"),
        F.col("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("svItsClmSk").alias("ITS_CLM_SK"),
        F.col("ITS_CLM_MSG_DESC")
    )
)

df_ForeignKey_DefaultUNK = spark.createDataFrame(
    [(0, 0, "UNK", 0, "UNK", 0, 0, 0, "UNK")],
    [
        "ITS_CLM_MSG_SK",
        "SRC_SYS_CD_SK",
        "CLM_ID",
        "ITS_CLM_MSG_FMT_CD_SK",
        "ITS_CLM_MSG_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "ITS_CLM_SK",
        "ITS_CLM_MSG_DESC"
    ]
)

df_ForeignKey_DefaultNA = spark.createDataFrame(
    [(1, 1, "NA", 1, "NA", 1, 1, 1, "NA")],
    [
        "ITS_CLM_MSG_SK",
        "SRC_SYS_CD_SK",
        "CLM_ID",
        "ITS_CLM_MSG_FMT_CD_SK",
        "ITS_CLM_MSG_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "ITS_CLM_SK",
        "ITS_CLM_MSG_DESC"
    ]
)

df_ForeignKey_Recycle = (
    df_ForeignKeyVars
    .filter(F.col("ErrCount") > 0)
    .select(
        GetRecycleKey(F.col("ITS_CLM_MSG_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD"),
        F.col("DISCARD_IN"),
        F.col("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT"),
        F.col("ErrCount").alias("ERR_CT"),
        (F.col("RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
        F.col("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING"),
        F.col("ITS_CLM_MSG_SK"),
        F.col("CLM_ID"),
        F.col("ITS_CLM_MSG_FMT_CD"),
        F.col("ITS_CLM_MSG_ID"),
        F.col("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("ITS_CLM_MSG_DESC")
    )
)

df_ForeignKey_Recycle_Clms = (
    df_ForeignKeyVars
    .filter(F.col("ErrCount") > 0)
    .select(
        F.col("SRC_SYS_CD"),
        F.col("CLM_ID")
    )
)

df_hf_recycle = (
    df_ForeignKey_Recycle
    .withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " "))
    .withColumn("SRC_SYS_CD", F.rpad(F.col("SRC_SYS_CD"), 256, " "))
    .withColumn("PRI_KEY_STRING", F.rpad(F.col("PRI_KEY_STRING"), 256, " "))
    .withColumn("CLM_ID", F.rpad(F.col("CLM_ID"), 256, " "))
    .withColumn("ITS_CLM_MSG_FMT_CD", F.rpad(F.col("ITS_CLM_MSG_FMT_CD"), 256, " "))
    .withColumn("ITS_CLM_MSG_ID", F.rpad(F.col("ITS_CLM_MSG_ID"), 256, " "))
    .withColumn("ITS_CLM_MSG_DESC", F.rpad(F.col("ITS_CLM_MSG_DESC"), 256, " "))
    .select(
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "PASS_THRU_IN",
        "FIRST_RECYC_DT",
        "ERR_CT",
        "RECYCLE_CT",
        "SRC_SYS_CD",
        "PRI_KEY_STRING",
        "ITS_CLM_MSG_SK",
        "CLM_ID",
        "ITS_CLM_MSG_FMT_CD",
        "ITS_CLM_MSG_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "ITS_CLM_MSG_DESC"
    )
)

write_files(
    df_hf_recycle,
    f"{adls_path}/hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_hf_claim_recycle_keys = (
    df_ForeignKey_Recycle_Clms
    .withColumn("SRC_SYS_CD", F.rpad(F.col("SRC_SYS_CD"), 256, " "))
    .withColumn("CLM_ID", F.rpad(F.col("CLM_ID"), 256, " "))
    .select("SRC_SYS_CD", "CLM_ID")
)

write_files(
    df_hf_claim_recycle_keys,
    f"{adls_path}/hf_claim_recycle_keys.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_Collector = (
    df_ForeignKey_Fkey
    .unionByName(df_ForeignKey_DefaultUNK)
    .unionByName(df_ForeignKey_DefaultNA)
)

df_Collector_final = (
    df_Collector
    .withColumn("CLM_ID", F.rpad(F.col("CLM_ID"), 256, " "))
    .withColumn("ITS_CLM_MSG_ID", F.rpad(F.col("ITS_CLM_MSG_ID"), 256, " "))
    .withColumn("ITS_CLM_MSG_DESC", F.rpad(F.col("ITS_CLM_MSG_DESC"), 256, " "))
    .select(
        "ITS_CLM_MSG_SK",
        "SRC_SYS_CD_SK",
        "CLM_ID",
        "ITS_CLM_MSG_FMT_CD_SK",
        "ITS_CLM_MSG_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "ITS_CLM_SK",
        "ITS_CLM_MSG_DESC"
    )
)

write_files(
    df_Collector_final,
    f"{adls_path}/load/ITS_CLM_MSG.{Source}.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)