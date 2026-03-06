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
# MAGIC JOB NAME:     IdsUMIpRvwLosFkey
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
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


InFile = get_widget_value('InFile','IdsUmIpRvwLosExtr.tmp')
Logging = get_widget_value('Logging','X')
OutFile = get_widget_value('OutFile','UM_IP_RVW_LOS.dat')
SrcSysCdSk = get_widget_value('SrcSysCdSk','105838')

# Read Stage: IdsUMIpRvwLosExtr (CSeqFileStage)
schema_idsumiprvwlosextr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(10, 0), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("UM_IP_RVW_LOS_SK", IntegerType(), False),
    StructField("UMUM_REF_ID", StringType(), False),
    StructField("UMIR_SEQ_NO", IntegerType(), False),
    StructField("UMLS_SEQ_NO", IntegerType(), False),
    StructField("UMLS_LOS_REQ", IntegerType(), False),
    StructField("UMLS_LOS_AUTH", IntegerType(), False),
    StructField("UMLS_MCTR_RDNY", StringType(), False),
    StructField("UMLS_USID_RDNY", StringType(), False),
    StructField("UMLS_PAID_DAYS", IntegerType(), False),
    StructField("UMLS_ALLOW_DAYS", IntegerType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False)
])

df_idsumiprvwlosextr = (
    spark.read
    .option("delimiter", ",")
    .option("quote", "\"")
    .option("header", "false")
    .schema(schema_idsumiprvwlosextr)
    .csv(f"{adls_path}/key/{InFile}")
)

# Transformer Stage: ForeignKey (CTransformerStage)
df_foreignkey = (
    df_idsumiprvwlosextr
    .withColumn("svPassThru", F.col("PASS_THRU_IN"))
    .withColumn(
        "svUmIpRvwLosDenialRsnCd",
        GetFkeyCodes(
            F.col("SRC_SYS_CD"),
            F.col("UM_IP_RVW_LOS_SK"),
            F.lit("UTILIZATION MANAGEMENT INPATIENT LOS DENIAL REASON "),
            F.col("UMLS_MCTR_RDNY"),
            F.lit(Logging)
        )
    )
    .withColumn(
        "svUmIpRvw",
        GetFkeyUmIpRvw(
            F.col("SRC_SYS_CD"),
            F.col("UM_IP_RVW_LOS_SK"),
            F.col("UMUM_REF_ID"),
            F.col("UMIR_SEQ_NO"),
            F.lit(Logging)
        )
    )
    .withColumn(
        "svDenialUserSk",
        GetFkeyAppUsr(
            F.col("SRC_SYS_CD"),
            F.col("UM_IP_RVW_LOS_SK"),
            F.col("UMLS_USID_RDNY"),
            F.lit(Logging)
        )
    )
    .withColumn(
        "svErrCount",
        GetFkeyErrorCnt(
            F.col("UM_IP_RVW_LOS_SK")
        )
    )
)

# Link "Fkey": (svErrCount = 0 or svPassThru = 'Y')
df_fkey = (
    df_foreignkey
    .filter((F.col("svErrCount") == 0) | (F.col("svPassThru") == 'Y'))
    .select(
        F.col("UM_IP_RVW_LOS_SK").alias("UM_IP_RVW_LOS_SK"),
        F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        F.col("UMUM_REF_ID").alias("UM_REF_ID"),
        F.col("UMIR_SEQ_NO").alias("UM_IP_RVW_SEQ_NO"),
        F.col("UMLS_SEQ_NO").alias("UM_IP_RVW_LOS_SEQ_NO"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("svUmIpRvw").alias("UM_IP_RVW_SK"),
        F.col("UMLS_LOS_AUTH").alias("AUTH_LOS_NO"),
        F.col("UMLS_LOS_REQ").alias("RQST_LOS_NO"),
        F.col("svUmIpRvwLosDenialRsnCd").alias("UM_IP_RVW_LOS_DENIAL_RSN_CD_SK"),
        F.col("svDenialUserSk").alias("DENIAL_USER_SK"),
        F.col("UMLS_PAID_DAYS").alias("PD_DAYS_NO"),
        F.col("UMLS_ALLOW_DAYS").alias("ALW_DAYS_NO")
    )
)

# Link "Recycle": (svErrCount > 0) -> hf_recycle (CHashedFileStage)
df_recycle = df_foreignkey.filter(F.col("svErrCount") > 0)
df_recycle = df_recycle.withColumn(
    "JOB_EXCTN_RCRD_ERR_SK",
    GetRecycleKey(F.col("UM_IP_RVW_LOS_SK"))
).withColumn(
    "ERR_CT",
    F.col("svErrCount")
).withColumn(
    "RECYCLE_CT",
    F.col("RECYCLE_CT") + F.lit(1)
)

df_recycle_final = df_recycle.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD"),
    F.col("DISCARD_IN"),
    F.col("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("UM_IP_RVW_LOS_SK"),
    F.col("UMUM_REF_ID"),
    F.col("UMIR_SEQ_NO"),
    F.col("UMLS_SEQ_NO"),
    F.col("UMLS_LOS_REQ"),
    F.col("UMLS_LOS_AUTH"),
    F.col("UMLS_MCTR_RDNY"),
    F.col("UMLS_USID_RDNY"),
    F.col("UMLS_PAID_DAYS"),
    F.col("UMLS_ALLOW_DAYS"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

write_files(
    df_recycle_final,
    f"{adls_path}/hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

# Link "DefaultUNK": (@INROWNUM = 1)
df_defaultunk = df_foreignkey.limit(1).select(
    F.lit(0).alias("UM_IP_RVW_LOS_SK"),
    F.lit(0).alias("SRC_SYS_CD_SK"),
    F.lit("UNK").alias("UM_REF_ID"),
    F.lit(0).alias("UM_IP_RVW_SEQ_NO"),
    F.lit(0).alias("UM_IP_RVW_LOS_SEQ_NO"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("UM_IP_RVW_SK"),
    F.lit(0).alias("AUTH_LOS_NO"),
    F.lit(0).alias("RQST_LOS_NO"),
    F.lit(0).alias("UM_IP_RVW_LOS_DENIAL_RSN_CD_SK"),
    F.lit(0).alias("DENIAL_USER_SK"),
    F.lit(0).alias("PD_DAYS_NO"),
    F.lit(0).alias("ALW_DAYS_NO")
)

# Link "DefaultNA": (@INROWNUM = 1)
df_defaultna = df_foreignkey.limit(1).select(
    F.lit(1).alias("UM_IP_RVW_LOS_SK"),
    F.lit(1).alias("SRC_SYS_CD_SK"),
    F.lit("NA").alias("UM_REF_ID"),
    F.lit(1).alias("UM_IP_RVW_SEQ_NO"),
    F.lit(1).alias("UM_IP_RVW_LOS_SEQ_NO"),
    F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("UM_IP_RVW_SK"),
    F.lit(1).alias("AUTH_LOS_NO"),
    F.lit(1).alias("RQST_LOS_NO"),
    F.lit(1).alias("UM_IP_RVW_LOS_DENIAL_RSN_CD_SK"),
    F.lit(1).alias("DENIAL_USER_SK"),
    F.lit(0).alias("PD_DAYS_NO"),
    F.lit(0).alias("ALW_DAYS_NO")
)

# Collector Stage: CCollector
df_collector = df_fkey.union(df_defaultunk).union(df_defaultna)

# Output Stage: UM_IP_RVW_LOS (CSeqFileStage)
df_collector_final = df_collector.select(
    "UM_IP_RVW_LOS_SK",
    "SRC_SYS_CD_SK",
    "UM_REF_ID",
    "UM_IP_RVW_SEQ_NO",
    "UM_IP_RVW_LOS_SEQ_NO",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "UM_IP_RVW_SK",
    "AUTH_LOS_NO",
    "RQST_LOS_NO",
    "UM_IP_RVW_LOS_DENIAL_RSN_CD_SK",
    "DENIAL_USER_SK",
    "PD_DAYS_NO",
    "ALW_DAYS_NO"
)

write_files(
    df_collector_final,
    f"{adls_path}/load/{OutFile}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)