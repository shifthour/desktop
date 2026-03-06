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
# MAGIC JOB NAME:     IdsUMIpSttusFkey
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
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DecimalType
from pyspark.sql.functions import col, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------

InFile = get_widget_value("InFile","IdsUmIpSttusExtr.dat.pkey")
Logging = get_widget_value("Logging","X")
OutFile = get_widget_value("OutFile","UM_IP_STTUS.dat")
SrcSysCdSk = get_widget_value("SrcSysCdSk","105838")

schema_IdsUMIpSttusExtr = StructType([
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
    StructField("UMIT_SEQ_NO", IntegerType(), False),
    StructField("USUS_ID", StringType(), False),
    StructField("UMIT_STS", StringType(), False),
    StructField("UMIT_STS_DTM", TimestampType(), False),
    StructField("UMIT_MCTR_REAS", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("UM_IP_STTUS_SK", IntegerType(), False)
])

df_IdsUMIpSttusExtr = (
    spark.read.format("csv")
    .option("header", "false")
    .option("quote", "\"")
    .schema(schema_IdsUMIpSttusExtr)
    .load(f"{adls_path}/key/{InFile}")
)

df_IdsUMIpSttusExtr_enriched = (
    df_IdsUMIpSttusExtr
    .withColumn("svPassThru", col("PASS_THRU_IN"))
    .withColumn("svUserSk", GetFkeyAppUsr(col("SRC_SYS_CD"), col("UM_IP_STTUS_SK"), col("USUS_ID"), Logging))
    .withColumn("svSttusDt", GetFkeyDate(lit("IDS"), col("UM_IP_STTUS_SK"), col("UMIT_STS_DTM"), Logging))
    .withColumn("svUm", GetFkeyUm(col("SRC_SYS_CD"), col("UM_IP_STTUS_SK"), col("UMUM_REF_ID"), Logging))
    .withColumn("svReason", GetFkeyCodes(col("SRC_SYS_CD"), col("UM_IP_STTUS_SK"), lit("UTILIZATION MANAGEMENT INPATIENT STATUS REASON"), col("UMIT_MCTR_REAS"), Logging))
    .withColumn("svSvcSttusCd", GetFkeyCodes(col("SRC_SYS_CD"), col("UM_IP_STTUS_SK"), lit("UTILIZATION MANAGEMENT STATUS"), col("UMIT_STS"), Logging))
    .withColumn("svErrCount", GetFkeyErrorCnt(col("UM_IP_STTUS_SK")))
)

df_Fkey = (
    df_IdsUMIpSttusExtr_enriched
    .filter((col("svErrCount") == 0) | (col("svPassThru") == "Y"))
    .select(
        col("UM_IP_STTUS_SK").alias("UM_IP_STTUS_SK"),
        lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        col("UMUM_REF_ID").alias("UM_REF_ID"),
        col("UMIT_SEQ_NO").alias("UM_IP_STTUS_SEQ_NO"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("svUserSk").alias("STTUS_USER_SK"),
        col("svUm").alias("UM_SK"),
        col("svSvcSttusCd").alias("UM_IP_STTUS_CD_SK"),
        col("svReason").alias("UM_IP_STTUS_RSN_CD_SK"),
        col("svSttusDt").alias("STTUS_DT_SK")
    )
)

df_Recycle = (
    df_IdsUMIpSttusExtr_enriched
    .filter(col("svErrCount") > 0)
    .select(
        GetRecycleKey(col("UM_IP_STTUS_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        col("DISCARD_IN").alias("DISCARD_IN"),
        col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        col("svErrCount").alias("ERR_CT"),
        (col("RECYCLE_CT") + lit(1)).alias("RECYCLE_CT"),
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        col("UM_IP_STTUS_SK").alias("UM_IP_STTUS_SK"),
        col("UMUM_REF_ID").alias("UMUM_REF_ID"),
        col("UMIT_SEQ_NO").alias("UMIT_SEQ_NO"),
        col("USUS_ID").alias("USUS_ID"),
        col("UMIT_STS").alias("UMIT_STS"),
        col("UMIT_MCTR_REAS").alias("UMIT_MCTR_REAS"),
        col("UMIT_STS_DTM").alias("UMIT_STS_DTM"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
    )
)

df_DefaultUNK = (
    df_IdsUMIpSttusExtr_enriched
    .limit(1)
    .select(
        lit(0).alias("UM_IP_STTUS_SK"),
        lit(0).alias("SRC_SYS_CD_SK"),
        lit("UNK").alias("UM_REF_ID"),
        lit(0).alias("UM_IP_STTUS_SEQ_NO"),
        lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(0).alias("STTUS_USER_SK"),
        lit(0).alias("UM_SK"),
        lit(0).alias("UM_IP_STTUS_CD_SK"),
        lit(0).alias("UM_IP_STTUS_RSN_CD_SK"),
        lit("1753-01-01").alias("STTUS_DT_SK")
    )
)

df_DefaultNA = (
    df_IdsUMIpSttusExtr_enriched
    .limit(1)
    .select(
        lit(1).alias("UM_IP_STTUS_SK"),
        lit(1).alias("SRC_SYS_CD_SK"),
        lit("NA").alias("UM_REF_ID"),
        lit(1).alias("UM_IP_STTUS_SEQ_NO"),
        lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(1).alias("STTUS_USER_SK"),
        lit(1).alias("UM_SK"),
        lit(1).alias("UM_IP_STTUS_CD_SK"),
        lit(1).alias("UM_IP_STTUS_RSN_CD_SK"),
        lit("1753-01-01").alias("STTUS_DT_SK")
    )
)

df_Collector = (
    df_Fkey
    .unionByName(df_DefaultUNK)
    .unionByName(df_DefaultNA)
)

df_Recycle_out = (
    df_Recycle
    .withColumn("INSRT_UPDT_CD", rpad(col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", rpad(col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", rpad(col("PASS_THRU_IN"), 1, " "))
    .withColumn("USUS_ID", rpad(col("USUS_ID"), 10, " "))
    .withColumn("UMIT_STS", rpad(col("UMIT_STS"), 2, " "))
    .withColumn("UMIT_MCTR_REAS", rpad(col("UMIT_MCTR_REAS"), 4, " "))
    .select(
        col("JOB_EXCTN_RCRD_ERR_SK"),
        col("INSRT_UPDT_CD"),
        col("DISCARD_IN"),
        col("PASS_THRU_IN"),
        col("FIRST_RECYC_DT"),
        col("ERR_CT"),
        col("RECYCLE_CT"),
        col("SRC_SYS_CD"),
        col("PRI_KEY_STRING"),
        col("UM_IP_STTUS_SK"),
        col("UMUM_REF_ID"),
        col("UMIT_SEQ_NO"),
        col("USUS_ID"),
        col("UMIT_STS"),
        col("UMIT_MCTR_REAS"),
        col("UMIT_STS_DTM"),
        col("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK")
    )
)

write_files(
    df_Recycle_out,
    "hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_Collector_out = (
    df_Collector
    .withColumn("STTUS_DT_SK", rpad(col("STTUS_DT_SK"), 10, " "))
    .select(
        col("UM_IP_STTUS_SK"),
        col("SRC_SYS_CD_SK"),
        col("UM_REF_ID"),
        col("UM_IP_STTUS_SEQ_NO"),
        col("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("STTUS_USER_SK"),
        col("UM_SK"),
        col("UM_IP_STTUS_CD_SK"),
        col("UM_IP_STTUS_RSN_CD_SK"),
        col("STTUS_DT_SK")
    )
)

write_files(
    df_Collector_out,
    f"{adls_path}/load/{OutFile}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)