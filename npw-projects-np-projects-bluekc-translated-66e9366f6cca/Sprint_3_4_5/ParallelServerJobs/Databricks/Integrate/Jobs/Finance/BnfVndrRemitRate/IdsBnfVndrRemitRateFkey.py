# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 11/04/09 08:20:06 Batch  15284_30013 PROMOTE bckcetl:31540 updt dsadm dsadm
# MAGIC ^1_1 10/29/09 10:49:49 Batch  15278_39003 PROMOTE bckcetl:31540 ids20 dsadm bls for sg
# MAGIC ^1_1 10/29/09 10:46:58 Batch  15278_38823 INIT bckcett:31540 testIDSnew dsadm bls for sg
# MAGIC ^1_1 10/14/09 13:57:39 Batch  15263_50281 PROMOTE bckcett:31540 testIDSnew u150906 3500-DNOA_Bhoomi_testIDSnew                Maddy
# MAGIC ^1_1 10/14/09 13:41:03 Batch  15263_49741 INIT bckcett:31540 devlIDSnew u150906 3500-DNOA_Bhoomi_devlIDSnew                Maddy
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC Â© Copyright 2009 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  UwsBnfVndrRemitExtrSeq
# MAGIC                
# MAGIC 
# MAGIC PROCESSING:   Takes the file  from primary key data and does foreign key lookups.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Bhoomi Dasari       10/01/2009          3500                    Originally Programmed.                                                                 devlIDSnew                    Steph Goddard          10/12/2009

# MAGIC Read common record format file.
# MAGIC Writing Sequential File to /load
# MAGIC Assign all foreign keys and create default rows for UNK and NA.
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
    DecimalType,
    TimestampType
)
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


Logging = get_widget_value('Logging', 'N')
SrcSysCdSK = get_widget_value('SrcSysCdSK', '69560')
InFile = get_widget_value('InFile', 'UwsBnfVndrRemitRateExtr.BnfVndrRemitRate.dat.9999')

# 1) Define the schema for the input file
schema_Extr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38, 10), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("BNF_VNDR_REMIT_RATE_SK", IntegerType(), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("BNF_VNDR_ID", StringType(), nullable=False),
    StructField("BNF_SUM_DTL_TYP_CD", StringType(), nullable=False),
    StructField("EFF_DT_SK", StringType(), nullable=False),
    StructField("TERM_DT_SK", StringType(), nullable=False),
    StructField("BNF_VNDR_REMIT_RATE", DecimalType(38, 10), nullable=False),
    StructField("USER_ID", StringType(), nullable=False),
    StructField("LAST_UPDT_DT_SK", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", StringType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", StringType(), nullable=False),
    StructField("BNF_SUM_DTL_TYP_CD_SK", IntegerType(), nullable=False)
])

# 2) Read the input file (CSeqFileStage)
df_Extr = (
    spark.read.csv(
        path=f"{adls_path}/key/{InFile}",
        schema=schema_Extr,
        sep=",",
        quote='"',
        header=False
    )
)

# 3) Add stage variables (transformer logic: PurgeTrn)
df_stagevars = (
    df_Extr
    .withColumn("svBnfSumDtlCd",
        GetFkeyCodes(
            F.col("SRC_SYS_CD"),
            F.col("BNF_VNDR_REMIT_RATE_SK"),
            F.lit("CAPITATION COPAYMENT TYPE"),
            F.col("BNF_SUM_DTL_TYP_CD"),
            F.lit(Logging)
        )
    )
    .withColumn("svEffDt",
        GetFkeyDate(
            F.lit("IDS"),
            F.col("BNF_VNDR_REMIT_RATE_SK"),
            F.col("EFF_DT_SK"),
            F.lit(Logging)
        )
    )
    .withColumn("svTermDt",
        GetFkeyDate(
            F.lit("IDS"),
            F.col("BNF_VNDR_REMIT_RATE_SK"),
            F.col("TERM_DT_SK"),
            F.lit(Logging)
        )
    )
    .withColumn("svLastUpdtDt",
        GetFkeyDate(
            F.lit("IDS"),
            F.col("BNF_VNDR_REMIT_RATE_SK"),
            F.col("LAST_UPDT_DT_SK"),
            F.lit(Logging)
        )
    )
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("BNF_VNDR_REMIT_RATE_SK")))
)

# 4) BnfVndrRemitRatekeyOut link (ErrCount = 0 or PassThru = 'Y')
df_BnfVndrRemitRatekeyOut = (
    df_stagevars
    .filter((F.col("ErrCount") == 0) | (F.col("PassThru") == 'Y'))
    .select(
        F.col("BNF_VNDR_REMIT_RATE_SK").alias("BNF_VNDR_REMIT_RATE_SK"),
        F.lit(SrcSysCdSK).alias("SRC_SYS_CD_SK"),
        F.col("BNF_VNDR_ID").alias("BNF_VNDR_ID"),
        F.col("BNF_SUM_DTL_TYP_CD").alias("BNF_SUM_DTL_TYP_CD"),
        F.col("svEffDt").alias("EFF_DT_SK"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("svBnfSumDtlCd").alias("BNF_SUM_DTL_TYP_CD_SK"),
        F.col("svTermDt").alias("TERM_DT_SK"),
        F.col("BNF_VNDR_REMIT_RATE").alias("BNF_VNDR_REMIT_RATE"),
        F.col("USER_ID").alias("USER_ID"),
        F.col("svLastUpdtDt").alias("LAST_UPDT_DT_SK")
    )
)

# 5) lnkRecycle link (ErrCount > 0) => hf_recycle hashed file (Scenario C, write to parquet)
df_lnkrecycle = (
    df_stagevars
    .filter(F.col("ErrCount") > 0)
    .select(
        GetRecycleKey(F.col("BNF_VNDR_REMIT_RATE_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("DISCARD_IN").alias("DISCARD_IN"),
        F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("ErrCount").alias("ERR_CT"),
        (F.col("RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("BNF_VNDR_ID").alias("BNF_VNDR_ID"),
        F.col("BNF_SUM_DTL_TYP_CD").alias("BNF_SUM_DTL_TYP_CD"),
        F.col("EFF_DT_SK").alias("EFF_DT_SK"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("TERM_DT_SK").alias("TERM_DT_SK"),
        F.col("BNF_VNDR_REMIT_RATE").alias("BNF_VNDR_REMIT_RATE"),
        F.col("USER_ID").alias("USER_ID"),
        F.col("LAST_UPDT_DT_SK").alias("LAST_UPDT_DT_SK")
    )
)

write_files(
    df_lnkrecycle,
    f"{adls_path}/hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote='"',
    nullValue=None
)

# 6) DefaultUNK link (constraint @INROWNUM = 1)
#    DefaultNA link (constraint @INROWNUM = 1)
#    Both produce single rows with the same schema as BnfVndrRemitRatekeyOut/Collector

collector_schema = StructType([
    StructField("BNF_VNDR_REMIT_RATE_SK", IntegerType(), nullable=True),
    StructField("SRC_SYS_CD_SK", IntegerType(), nullable=True),
    StructField("BNF_VNDR_ID", StringType(), nullable=True),
    StructField("BNF_SUM_DTL_TYP_CD", StringType(), nullable=True),
    StructField("EFF_DT_SK", StringType(), nullable=True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=True),
    StructField("BNF_SUM_DTL_TYP_CD_SK", IntegerType(), nullable=True),
    StructField("TERM_DT_SK", StringType(), nullable=True),
    StructField("BNF_VNDR_REMIT_RATE", DecimalType(38, 10), nullable=True),
    StructField("USER_ID", StringType(), nullable=True),
    StructField("LAST_UPDT_DT_SK", StringType(), nullable=True)
])

df_DefaultUNK = spark.createDataFrame(
    [
        (
            0,        # BNF_VNDR_REMIT_RATE_SK
            0,        # SRC_SYS_CD_SK
            "UNK",    # BNF_VNDR_ID
            "UNK",    # BNF_SUM_DTL_TYP_CD
            "UNK",    # EFF_DT_SK
            0,        # CRT_RUN_CYC_EXCTN_SK
            0,        # LAST_UPDT_RUN_CYC_EXCTN_SK
            0,        # BNF_SUM_DTL_TYP_CD_SK
            "UNK",    # TERM_DT_SK
            0,        # BNF_VNDR_REMIT_RATE
            "UNK",    # USER_ID
            "UNK"     # LAST_UPDT_DT_SK
        )
    ],
    collector_schema
)

df_DefaultNA = spark.createDataFrame(
    [
        (
            1,        # BNF_VNDR_REMIT_RATE_SK
            1,        # SRC_SYS_CD_SK
            "NA",     # BNF_VNDR_ID
            "NA",     # BNF_SUM_DTL_TYP_CD
            "NA",     # EFF_DT_SK
            1,        # CRT_RUN_CYC_EXCTN_SK
            1,        # LAST_UPDT_RUN_CYC_EXCTN_SK
            1,        # BNF_SUM_DTL_TYP_CD_SK
            "NA",     # TERM_DT_SK
            0,        # BNF_VNDR_REMIT_RATE
            "NA",     # USER_ID
            "NA"      # LAST_UPDT_DT_SK
        )
    ],
    collector_schema
)

# 7) Collector: union of BnfVndrRemitRatekeyOut, DefaultUNK, DefaultNA
df_collector = df_BnfVndrRemitRatekeyOut.unionByName(df_DefaultUNK).unionByName(df_DefaultNA)

# 8) Final select with rpad for any char/varchar columns, preserving the column order
#    Columns in final stage: 
#    [BNF_VNDR_REMIT_RATE_SK, SRC_SYS_CD_SK, BNF_VNDR_ID, BNF_SUM_DTL_TYP_CD, EFF_DT_SK, 
#     CRT_RUN_CYC_EXCTN_SK, LAST_UPDT_RUN_CYC_EXCTN_SK, BNF_SUM_DTL_TYP_CD_SK, TERM_DT_SK,
#     BNF_VNDR_REMIT_RATE, USER_ID, LAST_UPDT_DT_SK]

df_final = (
    df_collector
    .withColumn("BNF_VNDR_ID", rpad(F.col("BNF_VNDR_ID"), 255, " "))
    .withColumn("BNF_SUM_DTL_TYP_CD", rpad(F.col("BNF_SUM_DTL_TYP_CD"), 255, " "))
    .withColumn("EFF_DT_SK", rpad(F.col("EFF_DT_SK"), 10, " "))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", rpad(F.col("CRT_RUN_CYC_EXCTN_SK").cast(StringType()), 10, " "))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").cast(StringType()), 10, " "))
    .withColumn("TERM_DT_SK", rpad(F.col("TERM_DT_SK"), 10, " "))
    .withColumn("USER_ID", rpad(F.col("USER_ID"), 255, " "))
    .withColumn("LAST_UPDT_DT_SK", rpad(F.col("LAST_UPDT_DT_SK"), 10, " "))
    .select(
        F.col("BNF_VNDR_REMIT_RATE_SK"),
        F.col("SRC_SYS_CD_SK"),
        F.col("BNF_VNDR_ID"),
        F.col("BNF_SUM_DTL_TYP_CD"),
        F.col("EFF_DT_SK"),
        F.col("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("BNF_SUM_DTL_TYP_CD_SK"),
        F.col("TERM_DT_SK"),
        F.col("BNF_VNDR_REMIT_RATE"),
        F.col("USER_ID"),
        F.col("LAST_UPDT_DT_SK")
    )
)

# 9) Write final file (BNF_VNDR_REMIT_RATE.dat) (CSeqFileStage)
write_files(
    df_final,
    f"{adls_path}/load/BNF_VNDR_REMIT_RATE.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)