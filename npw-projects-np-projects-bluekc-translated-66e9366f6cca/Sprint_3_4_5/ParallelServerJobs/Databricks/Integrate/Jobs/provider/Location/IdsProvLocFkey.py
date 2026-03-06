# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC COPYRIGHT 2019 BLUE CROSS/BLUE SHIELD OF KANSAS CITY
# MAGIC 
# MAGIC DESCRIPTION:     IDS ProvLocation foreign key job.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #                               Change Description                  Development Project      Code Reviewer          Date Reviewed       
# MAGIC --------------------            --------------------     ------------------------                              ----------------------------------------           --------------------------------       -------------------------------   ----------------------------       
# MAGIC Giri Mallavaram        10/16/2019      6131 PBM Replacement               Initial Programming                       IntegrateDevl                 Kalyan Neelam          2019-11-20

# MAGIC This lookup is done to retrieve the provider address SK and is extracted from the primary key process in job name  ESIProvAddrExtr, FctsProvAddrExtr
# MAGIC Read PROV_LOC for foreign key look-up in a common record format
# MAGIC Perform foreign-key and code translation via corresponding lookup routines
# MAGIC Capture records generating translation errors to be uploaded to the IDS recycle table
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
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


InFile = get_widget_value('InFile','')
Logging = get_widget_value('Logging','Y')

# ----------------------------------------------------------------------------
# Stage: IdsProvLocExtr (CSeqFileStage) - Read Input File
# ----------------------------------------------------------------------------
schema_IdsProvLocExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38, 10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("PROV_LOC_SK", IntegerType(), nullable=False),
    StructField("SRC_SYS_CD_SK", IntegerType(), nullable=False),
    StructField("PROV_ID", StringType(), nullable=False),
    StructField("PROV_ADDR_ID", StringType(), nullable=False),
    StructField("PROV_ADDR_TYP_CD", StringType(), nullable=False),
    StructField("PROV_ADDR_EFF_DT", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("PROV_ADDR_SK", IntegerType(), nullable=False),
    StructField("PROV_SK", IntegerType(), nullable=False),
    StructField("PRI_ADDR_IN", StringType(), nullable=False),
    StructField("REMIT_ADDR_IN", StringType(), nullable=False)
])

df_IdsProvLocExtr = (
    spark.read.format("csv")
    .option("header", "false")
    .option("quote", "\"")
    .option("sep", ",")
    .schema(schema_IdsProvLocExtr)
    .load(f"{adls_path}/key/{InFile}")
)

# ----------------------------------------------------------------------------
# Stage: hf_prov_addr_lkup (CHashedFileStage) - Read from Parquet (Scenario C)
# ----------------------------------------------------------------------------
df_hf_prov_addr_lkup = spark.read.parquet(f"{adls_path}/hf_prov_addr.parquet")

# ----------------------------------------------------------------------------
# Stage: ForeignKey (CTransformerStage)
# ----------------------------------------------------------------------------
df_ForeignKey = (
    df_IdsProvLocExtr.alias("Key")
    .join(
        df_hf_prov_addr_lkup.alias("lkup"),
        (F.col("Key.SRC_SYS_CD") == F.col("lkup.SRC_SYS_CD"))
        & (F.col("Key.PROV_ADDR_ID") == F.col("lkup.PROV_ADDR_ID"))
        & (F.col("Key.PROV_ADDR_TYP_CD") == F.col("lkup.PROV_ADDR_TYP_CD"))
        & (F.col("Key.PROV_ADDR_EFF_DT") == F.col("lkup.PROV_ADDR_EFF_DT")),
        how="left"
    )
    .withColumn("svEffDt", GetFkeyDate(F.lit("IDS"), F.col("Key.PROV_LOC_SK"), F.col("Key.PROV_ADDR_EFF_DT"), F.lit(Logging)))
    .withColumn("svSrcSysCd", GetFkeyCodes(F.lit("IDS"), F.col("Key.PROV_LOC_SK"), F.lit("SOURCE SYSTEM"), F.col("Key.SRC_SYS_CD"), F.lit(Logging)))
    .withColumn("svProvSk", GetFkeyProv(F.col("Key.SRC_SYS_CD"), F.col("Key.PROV_LOC_SK"), F.col("Key.PROV_ID"), F.lit(Logging)))
    .withColumn("svAddrTypCdSk", GetFkeyCodes(F.col("Key.SRC_SYS_CD"), F.col("Key.PROV_LOC_SK"), F.lit("PROVIDER ADDRESS TYPE"), F.col("Key.PROV_ADDR_TYP_CD"), F.lit(Logging)))
    .withColumn("PassThru", F.lit("Y"))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("Key.PROV_LOC_SK")))
)

# OutputLink: Fkey
df_ForeignKey_Fkey = df_ForeignKey.filter(
    (F.col("ErrCount") == 0) | (F.col("PassThru") == "Y")
).select(
    F.col("Key.PROV_LOC_SK").alias("PROV_LOC_SK"),
    F.col("svSrcSysCd").alias("SRC_SYS_CD_SK"),
    F.col("Key.PROV_ID").alias("PROV_ID"),
    F.col("Key.PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    F.col("svAddrTypCdSk").alias("PROV_ADDR_TYP_CD_SK"),
    F.col("Key.PROV_ADDR_EFF_DT").alias("PROV_ADDR_EFF_DT_SK"),
    F.col("Key.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Key.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(F.col("lkup.PROV_ADDR_SK").isNull(), F.lit(0)).otherwise(F.col("lkup.PROV_ADDR_SK")).alias("PROV_ADDR_SK"),
    F.col("svProvSk").alias("PROV_SK"),
    F.col("Key.PRI_ADDR_IN").alias("PRI_ADDR_IN"),
    F.col("Key.REMIT_ADDR_IN").alias("REMIT_ADDR_IN")
)

# OutputLink: Recycle
df_ForeignKey_Recycle = df_ForeignKey.filter(F.col("ErrCount") > 0).select(
    GetRecycleKey(F.col("Key.PROV_LOC_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("Key.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("Key.DISCARD_IN").alias("DISCARD_IN"),
    F.col("Key.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("Key.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ErrCount").alias("ERR_CT"),
    (F.col("Key.RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
    F.col("Key.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Key.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("Key.PROV_LOC_SK").alias("PROV_LOC_SK"),
    F.col("Key.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("Key.PROV_ID").alias("PROV_ID"),
    F.col("Key.PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    F.col("Key.PROV_ADDR_TYP_CD").alias("PROV_ADDR_TYP_CD"),
    F.col("Key.PROV_ADDR_EFF_DT").alias("PROV_ADDR_EFF_DT"),
    F.col("Key.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Key.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Key.PROV_ADDR_SK").alias("PROV_ADDR_SK"),
    F.col("Key.PROV_SK").alias("PROV_SK"),
    F.col("Key.PRI_ADDR_IN").alias("PRI_ADDR_IN"),
    F.col("Key.REMIT_ADDR_IN").alias("REMIT_ADDR_IN")
)

# OutputLink: DefaultNA (@INROWNUM = 1 → single row)
df_ForeignKey_DefaultNA = spark.createDataFrame(
    [
        (1, 1, "NA", "NA", 1, "NA", 1, 1, 1, 1, "X", "X")
    ],
    StructType([
        StructField("PROV_LOC_SK", IntegerType(), True),
        StructField("SRC_SYS_CD_SK", IntegerType(), True),
        StructField("PROV_ID", StringType(), True),
        StructField("PROV_ADDR_ID", StringType(), True),
        StructField("PROV_ADDR_TYP_CD_SK", IntegerType(), True),
        StructField("PROV_ADDR_EFF_DT_SK", StringType(), True),
        StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
        StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
        StructField("PROV_ADDR_SK", IntegerType(), True),
        StructField("PROV_SK", IntegerType(), True),
        StructField("PRI_ADDR_IN", StringType(), True),
        StructField("REMIT_ADDR_IN", StringType(), True)
    ])
)

# OutputLink: DefaultUNK (@INROWNUM = 1 → single row)
df_ForeignKey_DefaultUNK = spark.createDataFrame(
    [
        (0, 0, "UNK", "UNK", 0, "UNK", 0, 0, 0, 0, "X", "X")
    ],
    StructType([
        StructField("PROV_LOC_SK", IntegerType(), True),
        StructField("SRC_SYS_CD_SK", IntegerType(), True),
        StructField("PROV_ID", StringType(), True),
        StructField("PROV_ADDR_ID", StringType(), True),
        StructField("PROV_ADDR_TYP_CD_SK", IntegerType(), True),
        StructField("PROV_ADDR_EFF_DT_SK", StringType(), True),
        StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
        StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
        StructField("PROV_ADDR_SK", IntegerType(), True),
        StructField("PROV_SK", IntegerType(), True),
        StructField("PRI_ADDR_IN", StringType(), True),
        StructField("REMIT_ADDR_IN", StringType(), True)
    ])
)

# ----------------------------------------------------------------------------
# Stage: hf_recycle (CHashedFileStage) - Write to Parquet (Scenario C)
# ----------------------------------------------------------------------------
write_files(
    df_ForeignKey_Recycle,
    f"{adls_path}/hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# ----------------------------------------------------------------------------
# Stage: Collector (CCollector) - Union (Fkey, DefaultNA, DefaultUNK)
# ----------------------------------------------------------------------------
df_Collector_Fkey = df_ForeignKey_Fkey.select(
    "PROV_LOC_SK",
    "SRC_SYS_CD_SK",
    "PROV_ID",
    "PROV_ADDR_ID",
    "PROV_ADDR_TYP_CD_SK",
    "PROV_ADDR_EFF_DT_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "PROV_ADDR_SK",
    "PROV_SK",
    "PRI_ADDR_IN",
    "REMIT_ADDR_IN"
)

df_Collector_DefaultNA = df_ForeignKey_DefaultNA.select(
    "PROV_LOC_SK",
    "SRC_SYS_CD_SK",
    "PROV_ID",
    "PROV_ADDR_ID",
    "PROV_ADDR_TYP_CD_SK",
    "PROV_ADDR_EFF_DT_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "PROV_ADDR_SK",
    "PROV_SK",
    "PRI_ADDR_IN",
    "REMIT_ADDR_IN"
)

df_Collector_DefaultUNK = df_ForeignKey_DefaultUNK.select(
    "PROV_LOC_SK",
    "SRC_SYS_CD_SK",
    "PROV_ID",
    "PROV_ADDR_ID",
    "PROV_ADDR_TYP_CD_SK",
    "PROV_ADDR_EFF_DT_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "PROV_ADDR_SK",
    "PROV_SK",
    "PRI_ADDR_IN",
    "REMIT_ADDR_IN"
)

df_Collector = (
    df_Collector_Fkey
    .unionByName(df_Collector_DefaultNA)
    .unionByName(df_Collector_DefaultUNK)
)

# ----------------------------------------------------------------------------
# Stage: ProvLoc (CSeqFileStage) - Write to .dat with final column order
# ----------------------------------------------------------------------------
df_ProvLoc = df_Collector.select(
    F.col("PROV_LOC_SK"),
    F.col("SRC_SYS_CD_SK"),
    F.col("PROV_ID"),
    F.col("PROV_ADDR_ID"),
    F.col("PROV_ADDR_TYP_CD_SK"),
    F.rpad(F.col("PROV_ADDR_EFF_DT_SK"), 10, " ").alias("PROV_ADDR_EFF_DT_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("PROV_ADDR_SK"),
    F.col("PROV_SK"),
    F.rpad(F.col("PRI_ADDR_IN"), 1, " ").alias("PRI_ADDR_IN"),
    F.rpad(F.col("REMIT_ADDR_IN"), 1, " ").alias("REMIT_ADDR_IN")
)

write_files(
    df_ProvLoc,
    f"{adls_path}/load/PROV_LOC.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)