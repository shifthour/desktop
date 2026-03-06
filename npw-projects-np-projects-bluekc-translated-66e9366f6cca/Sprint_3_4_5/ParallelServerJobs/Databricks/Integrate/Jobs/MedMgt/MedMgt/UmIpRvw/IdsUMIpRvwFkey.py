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
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsUMIpRvwFkey
# MAGIC 
# MAGIC DESCRIPTION:     Takes the sorted file and applies the primary key.   Preps the file for the foreign key lookup process that follows.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC       
# MAGIC Developer                          Date                    Project/Altiris #                     Change Description                                              Development Project               Code Reviewer                      Date Reviewed
# MAGIC ----------------------------------      -------------------      -----------------------------------           -------------------------------------------------------                          ----------------------------------              ---------------------------------               -------------------------
# MAGIC Bhoomi Dasari                 03/09/2009              3808                                Originally Programmed                                                 devlIDS                                 Steph Goddard                     03/30/2009

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
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DecimalType
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Parameters
InFile = get_widget_value('InFile','IdsUmIpRvwExtr.tmp')
Logging = get_widget_value('Logging','X')
OutFile = get_widget_value('OutFile','UM_IP_RVW.dat')
SrcSysCdSk = get_widget_value('SrcSysCdSk','105838')

# Schema for IdsUMIpRvwExtr (CSeqFileStage)
schema_IdsUMIpRvwExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),     # char(10)
    StructField("DISCARD_IN", StringType(), nullable=False),        # char(1)
    StructField("PASS_THRU_IN", StringType(), nullable=False),      # char(1)
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38, 10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),        # varchar
    StructField("PRI_KEY_STRING", StringType(), nullable=False),    # varchar
    StructField("UM_IP_RVW_SK", IntegerType(), nullable=False),
    StructField("UM_REF_ID", StringType(), nullable=False),         # varchar
    StructField("UM_IP_RVW_SEQ_NO", IntegerType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("UMIR_MCTR_LSOR", StringType(), nullable=False),    # char(4)
    StructField("UMIR_CAT_CURR", StringType(), nullable=False),     # char(1)
    StructField("DIAG_SET_CRT_DTM", TimestampType(), nullable=False),
    StructField("RVW_DT_SK", StringType(), nullable=False),         # char(10)
    StructField("AUTH_TOT_LOS_NO", IntegerType(), nullable=False),
    StructField("AVG_TOT_LOS_NO", IntegerType(), nullable=False),
    StructField("NRMTV_TOT_LOS_NO", IntegerType(), nullable=False),
    StructField("RQST_TOT_LOS_NO", IntegerType(), nullable=False)
])

# Read from IdsUMIpRvwExtr
df_IdsUMIpRvwExtr = (
    spark.read
    .option("sep", ",")
    .option("header", "false")
    .option("quote", '"')
    .schema(schema_IdsUMIpRvwExtr)
    .csv(f"{adls_path}/key/{InFile}")
)

# Enrich with Transformer Stage Variables (ForeignKey stage)
df_Transformer = (
    df_IdsUMIpRvwExtr
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("svDefaultDate", F.lit("1753-01-01-00.00.00.000000"))
    .withColumn(
        "svUmIpRvwLosExcptCd",
        GetFkeyCodes(
            F.col("SRC_SYS_CD"),
            F.col("UM_IP_RVW_SK"),
            F.lit("UTILIZATION MANAGEMENT REVIEW LOS EXCEPTION"),
            F.col("UMIR_MCTR_LSOR"),
            F.lit(Logging)
        )
    )
    .withColumn(
        "svUmIpRvwTreatCatCd",
        GetFkeyCodes(
            F.col("SRC_SYS_CD"),
            F.col("UM_IP_RVW_SK"),
            F.lit("UTILIZATION MANAGEMENT TREATMENT"),
            F.col("UMIR_CAT_CURR"),
            F.lit(Logging)
        )
    )
    .withColumn(
        "svRvwDt",
        GetFkeyDate(
            F.lit("IDS"),
            F.col("UM_IP_RVW_SK"),
            F.col("RVW_DT_SK"),
            F.lit(Logging)
        )
    )
    .withColumn(
        "svUm",
        GetFkeyUm(
            F.col("SRC_SYS_CD"),
            F.col("UM_IP_RVW_SK"),
            F.col("UM_REF_ID"),
            F.lit(Logging)
        )
    )
    .withColumn(
        "ErrCount",
        GetFkeyErrorCnt(F.col("UM_IP_RVW_SK"))
    )
)

# Fkey link (ErrCount = 0 or PassThru = 'Y')
df_Fkey = (
    df_Transformer
    .filter((F.col("ErrCount") == 0) | (F.col("PassThru") == "Y"))
    .select(
        F.col("UM_IP_RVW_SK").alias("UM_IP_RVW_SK"),
        F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        F.col("UM_REF_ID").alias("UM_REF_ID"),
        F.col("UM_IP_RVW_SEQ_NO").alias("UM_IP_RVW_SEQ_NO"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("svUm").alias("UM_SK"),
        F.col("svUmIpRvwLosExcptCd").alias("UM_IP_RVW_LOS_EXCPT_CD_SK"),
        F.col("svUmIpRvwTreatCatCd").alias("UM_IP_RVW_TREAT_CAT_CD_SK"),
        F.col("DIAG_SET_CRT_DTM").alias("DIAG_SET_CRT_DTM"),
        F.col("svRvwDt").alias("RVW_DT_SK"),
        F.col("AUTH_TOT_LOS_NO").alias("AUTH_TOT_LOS_NO"),
        F.col("AVG_TOT_LOS_NO").alias("AVG_TOT_LOS_NO"),
        F.col("NRMTV_TOT_LOS_NO").alias("NRMTV_TOT_LOS_NO"),
        F.col("RQST_TOT_LOS_NO").alias("RQST_TOT_LOS_NO")
    )
)

# Recycle link (ErrCount > 0) => hf_recycle (CHashedFileStage => scenario C => write to parquet)
df_Recycle = (
    df_Transformer
    .filter(F.col("ErrCount") > 0)
    .select(
        GetRecycleKey(F.col("UM_IP_RVW_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("DISCARD_IN").alias("DISCARD_IN"),
        F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("ERR_CT").alias("ERR_CT"),
        (F.col("RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("UM_IP_RVW_SK").alias("UM_IP_RVW_SK"),
        F.col("UM_REF_ID").alias("UM_REF_ID"),
        F.col("UM_IP_RVW_SEQ_NO").alias("UM_IP_RVW_SEQ_NO"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("UMIR_MCTR_LSOR").alias("UMIR_MCTR_LSOR"),
        F.col("UMIR_CAT_CURR").alias("UMIR_CAT_CURR"),
        F.col("DIAG_SET_CRT_DTM").alias("DIAG_SET_CRT_DTM"),
        F.col("RVW_DT_SK").alias("RVW_DT_SK"),
        F.col("AUTH_TOT_LOS_NO").alias("AUTH_TOT_LOS_NO"),
        F.col("AVG_TOT_LOS_NO").alias("AVG_TOT_LOS_NO"),
        F.col("NRMTV_TOT_LOS_NO").alias("NRMTV_TOT_LOS_NO"),
        F.col("RQST_TOT_LOS_NO").alias("RQST_TOT_LOS_NO")
    )
)

write_files(
    df_Recycle,
    "hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_parquet=True,
    header=True,
    quote='"',
    nullValue=None
)

# DefaultUNK link (@INROWNUM = 1)
w = Window.orderBy(F.lit(0))
df_rn = df_Transformer.withColumn("rownum", F.row_number().over(w))

df_DefaultUNK = (
    df_rn
    .filter(F.col("rownum") == 1)
    .select(
        F.lit(0).alias("UM_IP_RVW_SK"),
        F.lit(0).alias("SRC_SYS_CD_SK"),
        F.lit("UNK").alias("UM_REF_ID"),
        F.lit(0).alias("UM_IP_RVW_SEQ_NO"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("UM_SK"),
        F.lit(0).alias("UM_IP_RVW_LOS_EXCPT_CD_SK"),
        F.lit(0).alias("UM_IP_RVW_TREAT_CAT_CD_SK"),
        F.col("svDefaultDate").alias("DIAG_SET_CRT_DTM"),
        F.lit("1753-01-01").alias("RVW_DT_SK"),
        F.lit(0).alias("AUTH_TOT_LOS_NO"),
        F.lit(0).alias("AVG_TOT_LOS_NO"),
        F.lit(0).alias("NRMTV_TOT_LOS_NO"),
        F.lit(0).alias("RQST_TOT_LOS_NO")
    )
)

# DefaultNA link (@INROWNUM = 1)
df_DefaultNA = (
    df_rn
    .filter(F.col("rownum") == 1)
    .select(
        F.lit(1).alias("UM_IP_RVW_SK"),
        F.lit(1).alias("SRC_SYS_CD_SK"),
        F.lit("NA").alias("UM_REF_ID"),
        F.lit(1).alias("UM_IP_RVW_SEQ_NO"),
        F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("UM_SK"),
        F.lit(1).alias("UM_IP_RVW_LOS_EXCPT_CD_SK"),
        F.lit(1).alias("UM_IP_RVW_TREAT_CAT_CD_SK"),
        F.col("svDefaultDate").alias("DIAG_SET_CRT_DTM"),
        F.lit("1753-01-01").alias("RVW_DT_SK"),
        F.lit(0).alias("AUTH_TOT_LOS_NO"),
        F.lit(0).alias("AVG_TOT_LOS_NO"),
        F.lit(0).alias("NRMTV_TOT_LOS_NO"),
        F.lit(0).alias("RQST_TOT_LOS_NO")
    )
)

# Collector stage (CCollector), union the three links from the Transformer
df_Collector = (
    df_Fkey.select(
        "UM_IP_RVW_SK",
        "SRC_SYS_CD_SK",
        "UM_REF_ID",
        "UM_IP_RVW_SEQ_NO",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "UM_SK",
        "UM_IP_RVW_LOS_EXCPT_CD_SK",
        "UM_IP_RVW_TREAT_CAT_CD_SK",
        "DIAG_SET_CRT_DTM",
        "RVW_DT_SK",
        "AUTH_TOT_LOS_NO",
        "AVG_TOT_LOS_NO",
        "NRMTV_TOT_LOS_NO",
        "RQST_TOT_LOS_NO"
    )
    .unionByName(
        df_DefaultUNK.select(
            "UM_IP_RVW_SK",
            "SRC_SYS_CD_SK",
            "UM_REF_ID",
            "UM_IP_RVW_SEQ_NO",
            "CRT_RUN_CYC_EXCTN_SK",
            "LAST_UPDT_RUN_CYC_EXCTN_SK",
            "UM_SK",
            "UM_IP_RVW_LOS_EXCPT_CD_SK",
            "UM_IP_RVW_TREAT_CAT_CD_SK",
            "DIAG_SET_CRT_DTM",
            "RVW_DT_SK",
            "AUTH_TOT_LOS_NO",
            "AVG_TOT_LOS_NO",
            "NRMTV_TOT_LOS_NO",
            "RQST_TOT_LOS_NO"
        )
    )
    .unionByName(
        df_DefaultNA.select(
            "UM_IP_RVW_SK",
            "SRC_SYS_CD_SK",
            "UM_REF_ID",
            "UM_IP_RVW_SEQ_NO",
            "CRT_RUN_CYC_EXCTN_SK",
            "LAST_UPDT_RUN_CYC_EXCTN_SK",
            "UM_SK",
            "UM_IP_RVW_LOS_EXCPT_CD_SK",
            "UM_IP_RVW_TREAT_CAT_CD_SK",
            "DIAG_SET_CRT_DTM",
            "RVW_DT_SK",
            "AUTH_TOT_LOS_NO",
            "AVG_TOT_LOS_NO",
            "NRMTV_TOT_LOS_NO",
            "RQST_TOT_LOS_NO"
        )
    )
)

# Apply rpad for char(10) columns
df_CollectorFinal = df_Collector.withColumn(
    "RVW_DT_SK",
    F.rpad(F.col("RVW_DT_SK"), 10, " ")
)

# UM_IP_RVW (CSeqFileStage) => Write to #OutFile#
write_files(
    df_CollectorFinal.select(
        "UM_IP_RVW_SK",
        "SRC_SYS_CD_SK",
        "UM_REF_ID",
        "UM_IP_RVW_SEQ_NO",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "UM_SK",
        "UM_IP_RVW_LOS_EXCPT_CD_SK",
        "UM_IP_RVW_TREAT_CAT_CD_SK",
        "DIAG_SET_CRT_DTM",
        "RVW_DT_SK",
        "AUTH_TOT_LOS_NO",
        "AVG_TOT_LOS_NO",
        "NRMTV_TOT_LOS_NO",
        "RQST_TOT_LOS_NO"
    ),
    f"{adls_path}/load/{OutFile}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote='"',
    nullValue=None
)