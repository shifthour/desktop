# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 05/12/09 13:30:39 Batch  15108_48642 PROMOTE bckcetl ids20 dsadm bls for rt
# MAGIC ^1_1 05/12/09 13:07:49 Batch  15108_47273 INIT bckcett:31540 testIDS dsadm BLS FOR RT
# MAGIC ^1_1 04/28/09 15:30:57 Batch  15094_55861 PROMOTE bckcett testIDS u03651 steph for Ralph
# MAGIC ^1_1 04/28/09 15:26:41 Batch  15094_55604 INIT bckcett devlIDS u03651 steffy
# MAGIC 
# MAGIC 
# MAGIC COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsUMSvcSttusFkey
# MAGIC 
# MAGIC DESCRIPTION:    Takes the primary key file and applies the foreign keys.   
# MAGIC       
# MAGIC 
# MAGIC 
# MAGIC Developer	Date		Project/Altiris #	Change Description				Development Project	Code Reviewer		Date Reviewed
# MAGIC -----------------------	-------------------	-----------------------	---------------------------------------------------------		----------------------------------	---------------------------------	-------------------------
# MAGIC    
# MAGIC Tracy Davis	03/6/2009	3808		Created job				devlIDS			Steph Goddard                         03/30/2009

# MAGIC Set all foreign surragote keys
# MAGIC Merge source data with default rows
# MAGIC Writing Sequential File to /load
# MAGIC Read common record format file from extract job.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DecimalType
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


InFile = get_widget_value("InFile","IdsUmSvcSttusExtr.dat.pkey")
Logging = get_widget_value("Logging","X")
OutFile = get_widget_value("OutFile","UM_SVC_STTUS.dat")
SrcSysCdSk = get_widget_value("SrcSysCdSk","105838")

schema_IdsUMSvcSttusExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38,10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("UMUM_REF_ID", StringType(), False),
    StructField("UMSV_SEQ_NO", IntegerType(), False),
    StructField("UMVT_SEQ_NO", IntegerType(), False),
    StructField("USUS_ID", StringType(), False),
    StructField("UMVT_STS", StringType(), False),
    StructField("UMVT_STS_DTM", TimestampType(), False),
    StructField("UMVT_MCTR_REAS", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("UM_SVC_STTUS_SK", IntegerType(), False)
])

df_IdsUMSvcSttusExtr = (
    spark.read.format("csv")
    .option("header", "false")
    .option("quote", "\"")
    .option("escape", "\"")
    .schema(schema_IdsUMSvcSttusExtr)
    .load(f"{adls_path}/key/{InFile}")
)

df_with_vars = (
    df_IdsUMSvcSttusExtr
    .withColumn("svPassThru", col("PASS_THRU_IN"))
    .withColumn("svUserSk", GetFkeyAppUsr(col("SRC_SYS_CD"), col("UM_SVC_STTUS_SK"), col("USUS_ID"), Logging))
    .withColumn("svSttusDt", GetFkeyDate(lit("IDS"), col("UM_SVC_STTUS_SK"), col("UMVT_STS_DTM"), Logging))
    .withColumn("svUmSvc", GetFkeyUmSvc(col("SRC_SYS_CD"), col("UM_SVC_STTUS_SK"), col("UMUM_REF_ID"), col("UMSV_SEQ_NO"), Logging))
    .withColumn("svReason", GetFkeyCodes(col("SRC_SYS_CD"), col("UM_SVC_STTUS_SK"), lit("UTILIZATION MANAGEMENT SERVICE STATUS REASON"), col("UMVT_MCTR_REAS"), Logging))
    .withColumn("svSvcSttusCd", GetFkeyCodes(col("SRC_SYS_CD"), col("UM_SVC_STTUS_SK"), lit("UTILIZATION MANAGEMENT STATUS"), col("UMVT_STS"), Logging))
    .withColumn("svErrCount", GetFkeyErrorCnt(col("UM_SVC_STTUS_SK")))
)

df_FKey = (
    df_with_vars
    .filter((col("svErrCount") == 0) | (col("svPassThru") == 'Y'))
    .select(
        col("UM_SVC_STTUS_SK").alias("UM_SVC_STTUS_SK"),
        lit(SrcSysCdSk).cast(IntegerType()).alias("SRC_SYS_CD_SK"),
        col("UMUM_REF_ID").alias("UM_REF_ID"),
        col("UMSV_SEQ_NO").alias("UM_SVC_SEQ_NO"),
        col("UMVT_SEQ_NO").alias("UM_SVC_STTUS_SEQ_NO"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("svUserSk").alias("STTUS_USER_SK"),
        col("svUmSvc").alias("UM_SVC_SK"),
        col("svSvcSttusCd").alias("UM_SVC_STTUS_CD_SK"),
        col("svReason").alias("UM_SVC_STTUS_RSN_CD_SK"),
        col("svSttusDt").alias("STTUS_DT_SK")
    )
)

df_Recycle = (
    df_with_vars
    .filter(col("svErrCount") > 0)
    .select(
        GetRecycleKey(col("UM_SVC_STTUS_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        col("DISCARD_IN").alias("DISCARD_IN"),
        col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        col("svErrCount").alias("ERR_CT"),
        (col("RECYCLE_CT") + lit(1)).alias("RECYCLE_CT"),
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        col("UM_SVC_STTUS_SK").alias("UM_SVC_STTUS_SK"),
        col("UMUM_REF_ID").alias("UMUM_REF_ID"),
        col("UMSV_SEQ_NO").alias("UMSV_SEQ_NO"),
        col("UMVT_SEQ_NO").alias("UMVT_SEQ_NO"),
        col("USUS_ID").alias("USUS_ID"),
        col("UMVT_STS").alias("UMVT_STS"),
        col("UMVT_MCTR_REAS").alias("UMVT_MCTR_REAS"),
        col("UMVT_STS_DTM").alias("UMVT_STS_DTM"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
    )
)

df_Recycle_charPadded = (
    df_Recycle
    .withColumn("INSRT_UPDT_CD", rpad("INSRT_UPDT_CD", 10, " "))
    .withColumn("DISCARD_IN", rpad("DISCARD_IN", 1, " "))
    .withColumn("PASS_THRU_IN", rpad("PASS_THRU_IN", 1, " "))
    .withColumn("USUS_ID", rpad("USUS_ID", 10, " "))
    .withColumn("UMVT_STS", rpad("UMVT_STS", 2, " "))
    .withColumn("UMVT_MCTR_REAS", rpad("UMVT_MCTR_REAS", 4, " "))
)

df_Recycle_final = df_Recycle_charPadded.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "UM_SVC_STTUS_SK",
    "UMUM_REF_ID",
    "UMSV_SEQ_NO",
    "UMVT_SEQ_NO",
    "USUS_ID",
    "UMVT_STS",
    "UMVT_MCTR_REAS",
    "UMVT_STS_DTM",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK"
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

df_DefaultUNK = df_with_vars.limit(1).select(
    lit(0).alias("UM_SVC_STTUS_SK"),
    lit(0).alias("SRC_SYS_CD_SK"),
    lit("UNK").alias("UM_REF_ID"),
    lit(0).alias("UM_SVC_SEQ_NO"),
    lit(0).alias("UM_SVC_STTUS_SEQ_NO"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("STTUS_USER_SK"),
    lit(0).alias("UM_SVC_SK"),
    lit(0).alias("UM_SVC_STTUS_CD_SK"),
    lit(0).alias("UM_SVC_STTUS_RSN_CD_SK"),
    lit("1753-01-01").alias("STTUS_DT_SK")
)

df_DefaultNA = df_with_vars.limit(1).select(
    lit(1).alias("UM_SVC_STTUS_SK"),
    lit(1).alias("SRC_SYS_CD_SK"),
    lit("NA").alias("UM_REF_ID"),
    lit(1).alias("UM_SVC_SEQ_NO"),
    lit(1).alias("UM_SVC_STTUS_SEQ_NO"),
    lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(1).alias("STTUS_USER_SK"),
    lit(1).alias("UM_SVC_SK"),
    lit(1).alias("UM_SVC_STTUS_CD_SK"),
    lit(1).alias("UM_SVC_STTUS_RSN_CD_SK"),
    lit("1753-01-01").alias("STTUS_DT_SK")
)

df_collector = (
    df_FKey.select(
        "UM_SVC_STTUS_SK",
        "SRC_SYS_CD_SK",
        "UM_REF_ID",
        "UM_SVC_SEQ_NO",
        "UM_SVC_STTUS_SEQ_NO",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "STTUS_USER_SK",
        "UM_SVC_SK",
        "UM_SVC_STTUS_CD_SK",
        "UM_SVC_STTUS_RSN_CD_SK",
        "STTUS_DT_SK"
    )
    .unionByName(
        df_DefaultUNK.select(
            "UM_SVC_STTUS_SK",
            "SRC_SYS_CD_SK",
            "UM_REF_ID",
            "UM_SVC_SEQ_NO",
            "UM_SVC_STTUS_SEQ_NO",
            "CRT_RUN_CYC_EXCTN_SK",
            "LAST_UPDT_RUN_CYC_EXCTN_SK",
            "STTUS_USER_SK",
            "UM_SVC_SK",
            "UM_SVC_STTUS_CD_SK",
            "UM_SVC_STTUS_RSN_CD_SK",
            "STTUS_DT_SK"
        )
    )
    .unionByName(
        df_DefaultNA.select(
            "UM_SVC_STTUS_SK",
            "SRC_SYS_CD_SK",
            "UM_REF_ID",
            "UM_SVC_SEQ_NO",
            "UM_SVC_STTUS_SEQ_NO",
            "CRT_RUN_CYC_EXCTN_SK",
            "LAST_UPDT_RUN_CYC_EXCTN_SK",
            "STTUS_USER_SK",
            "UM_SVC_SK",
            "UM_SVC_STTUS_CD_SK",
            "UM_SVC_STTUS_RSN_CD_SK",
            "STTUS_DT_SK"
        )
    )
)

df_collector_final = df_collector.withColumn("STTUS_DT_SK", rpad("STTUS_DT_SK", 10, " "))

df_finalSelect = df_collector_final.select(
    "UM_SVC_STTUS_SK",
    "SRC_SYS_CD_SK",
    "UM_REF_ID",
    "UM_SVC_SEQ_NO",
    "UM_SVC_STTUS_SEQ_NO",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "STTUS_USER_SK",
    "UM_SVC_SK",
    "UM_SVC_STTUS_CD_SK",
    "UM_SVC_STTUS_RSN_CD_SK",
    "STTUS_DT_SK"
)

write_files(
    df_finalSelect,
    f"{adls_path}/load/{OutFile}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)