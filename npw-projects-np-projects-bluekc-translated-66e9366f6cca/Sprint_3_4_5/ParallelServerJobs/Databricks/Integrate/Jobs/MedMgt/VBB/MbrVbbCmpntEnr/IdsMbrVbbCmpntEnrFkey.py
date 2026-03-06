# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2013 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:   Assigns Foreign Keys for the IDS table MBR_VBB_CMPNT_ENR.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 	Project/Altiris #	Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------	------------------------	-----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Raja Gummadi        2013-03-25           4963 VBB Phase III     Initial Programming                                                                      IntegrateNewDevl          Bhoomi Dasari          7/9/2013
# MAGIC 
# MAGIC Dan Long               2014-10-10                    TFS-9769           Change the values of the CRT_RUN_CYC_EXCTN_SK            IntegrateNewDevl          Kalyan Neelam        2014-10-06
# MAGIC                                                                                                and LAST_UPDT_RUN_CYC_EXCTN_SK in the 
# MAGIC                                                                                                Foreign Key stage for the NA and UNK default files to 100
# MAGIC                                                                                                adhere to the Data Governance standards.

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
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


InFile = get_widget_value('InFile','IdsMbrVbbCmpntEnrExtr.MbrVbbCmpntEnr.dat.100')
Logging = get_widget_value('Logging','X')
RunID = get_widget_value('RunID','100')

schema_IdsMbrVbbCmpntEnrExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38,10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("MBR_VBB_CMPNT_ENR_SK", IntegerType(), False),
    StructField("MBR_UNIQ_KEY", IntegerType(), False),
    StructField("VBB_CMPNT_UNIQ_KEY", IntegerType(), False),
    StructField("SRC_SYS_CD_SK", IntegerType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("HIEL_LOG_LEVEL1", StringType(), True),
    StructField("HIEL_LOG_LEVEL2", StringType(), True),
    StructField("HIEL_LOG_LEVEL3", StringType(), True),
    StructField("MEIP_ENROLLED_BY", StringType(), True),
    StructField("MEIP_STATUS", StringType(), False),
    StructField("MEIP_TERMED_BY", StringType(), True),
    StructField("MEIP_TERM_REASON", StringType(), True),
    StructField("MBR_CNTCT_IN", StringType(), False),
    StructField("MBR_CNTCT_LAST_UPDT_DT_SK", StringType(), False),
    StructField("MBR_VBB_CMPNT_CMPLTN_DT_SK", StringType(), False),
    StructField("MBR_VBB_CMPNT_ENR_DT_SK", StringType(), False),
    StructField("MBR_VBB_CMPNT_TERM_DT_SK", StringType(), False),
    StructField("SRC_SYS_CRT_DTM", TimestampType(), False),
    StructField("SRC_SYS_UPDT_DTM", TimestampType(), False),
    StructField("VBB_VNDR_UNIQ_KEY", IntegerType(), False),
    StructField("VNDR_PGM_SEQ_NO", IntegerType(), False),
    StructField("MBR_CMPLD_ACHV_LVL_CT", IntegerType(), False),
    StructField("TRZ_MBR_UNVRS_ID", StringType(), False),
    StructField("MBR_VBB_PLN_ENR_SK", IntegerType(), False)
])

df_IdsMbrVbbCmpntEnrExtr = (
    spark.read.format("csv")
    .option("header", "false")
    .option("quote", "\"")
    .option("delimiter", ",")
    .schema(schema_IdsMbrVbbCmpntEnrExtr)
    .load(f"{adls_path}/key/{InFile}")
)

df_foreignKey = (
    df_IdsMbrVbbCmpntEnrExtr
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("svClsSk", GetFkeyCls(F.lit("FACETS"), F.col("MBR_VBB_CMPNT_ENR_SK"), F.col("HIEL_LOG_LEVEL1"), F.col("HIEL_LOG_LEVEL2"), F.lit(Logging)))
    .withColumn("svClsPlnSk", GetFkeyClsPln(F.lit("FACETS"), F.col("MBR_VBB_CMPNT_ENR_SK"), F.col("HIEL_LOG_LEVEL3"), F.lit(Logging)))
    .withColumn("svGrpSk", GetFkeyGrp(F.lit("FACETS"), F.col("MBR_VBB_CMPNT_ENR_SK"), F.col("HIEL_LOG_LEVEL1"), F.lit(Logging)))
    .withColumn("svMbrSk", GetFkeyMbr(F.lit("FACETS"), F.col("MBR_VBB_CMPNT_ENR_SK"), F.col("MBR_UNIQ_KEY"), F.lit(Logging)))
    .withColumn("svVbbCmpntSk", GetFkeyVbbCmpnt(F.col("SRC_SYS_CD"), F.col("MBR_VBB_CMPNT_ENR_SK"), F.col("VBB_CMPNT_UNIQ_KEY"), F.lit(Logging)))
    .withColumn("svMbrVbbCmpntEnrMethCdSk", GetFkeyClctnDomainCodes(F.col("SRC_SYS_CD"), F.col("MBR_VBB_CMPNT_ENR_SK"), F.lit("VBB ENROLLMENT TERMINATION METHOD"), F.lit("IHMF CONSTITUENT"), F.lit("VBB ENROLLMENT TERMINATION METHOD"), F.lit("IDS"), F.col("MEIP_ENROLLED_BY"), F.lit(Logging)))
    .withColumn("svMbrVbbCmpntSttusCdSk", GetFkeyClctnDomainCodes(F.col("SRC_SYS_CD"), F.col("MBR_VBB_CMPNT_ENR_SK"), F.lit("VBB STATUS"), F.lit("IHMF CONSTITUENT"), F.lit("VBB STATUS"), F.lit("IDS"), F.col("MEIP_STATUS"), F.lit(Logging)))
    .withColumn("svMbrVbbCmpntTermMethCdSk", GetFkeyClctnDomainCodes(F.col("SRC_SYS_CD"), F.col("MBR_VBB_CMPNT_ENR_SK"), F.lit("VBB ENROLLMENT TERMINATION METHOD"), F.lit("IHMF CONSTITUENT"), F.lit("VBB ENROLLMENT TERMINATION METHOD"), F.lit("IDS"), F.col("MEIP_TERMED_BY"), F.lit(Logging)))
    .withColumn("svMbrVbbCmpntTermRsnCdSk", GetFkeyClctnDomainCodes(F.col("SRC_SYS_CD"), F.col("MBR_VBB_CMPNT_ENR_SK"), F.lit("ENROLLMENT TERMINATION REASON"), F.lit("IHMF CONSTITUENT"), F.lit("ENROLLMENT TERMINATION REASON"), F.lit("IDS"), F.col("MEIP_TERM_REASON"), F.lit(Logging)))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("MBR_VBB_CMPNT_ENR_SK")))
)

w = Window.orderBy(F.lit(1))
df_foreignKey_with_rn = df_foreignKey.withColumn("rownum", F.row_number().over(w))

df_ForeignKey_FkeyOut = df_foreignKey.filter((F.col("ErrCount") == 0) | (F.col("PassThru") == 'Y'))
df_ForeignKey_RecycleOut = df_foreignKey.filter(F.col("ErrCount") > 0)
df_ForeignKey_DefaultUNKOut = df_foreignKey_with_rn.filter(F.col("rownum") == 1)
df_ForeignKey_DefaultNAOut = df_foreignKey_with_rn.filter(F.col("rownum") == 1)

df_ForeignKey_RecycleOut_sel = df_ForeignKey_RecycleOut.select(
    GetRecycleKey(F.col("MBR_VBB_CMPNT_ENR_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ErrCount").alias("ERR_CT"),
    (F.col("RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("MBR_VBB_CMPNT_ENR_SK").alias("MBR_VBB_CMPNT_ENR_SK"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("VBB_CMPNT_UNIQ_KEY").alias("VBB_CMPNT_UNIQ_KEY"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("HIEL_LOG_LEVEL1").alias("HIEL_LOG_LEVEL1"),
    F.col("HIEL_LOG_LEVEL2").alias("HIEL_LOG_LEVEL2"),
    F.col("HIEL_LOG_LEVEL3").alias("HIEL_LOG_LEVEL3"),
    F.col("MEIP_ENROLLED_BY").alias("MEIP_ENROLLED_BY"),
    F.col("MEIP_STATUS").alias("MEIP_STATUS"),
    F.col("MEIP_TERMED_BY").alias("MEIP_TERMED_BY"),
    F.col("MEIP_TERM_REASON").alias("MEIP_TERM_REASON"),
    F.col("MBR_CNTCT_IN").alias("MBR_CNTCT_IN"),
    F.col("MBR_CNTCT_LAST_UPDT_DT_SK").alias("MBR_CNTCT_LAST_UPDT_DT_SK"),
    F.col("MBR_VBB_CMPNT_CMPLTN_DT_SK").alias("MBR_VBB_CMPNT_CMPLTN_DT_SK"),
    F.col("MBR_VBB_CMPNT_ENR_DT_SK").alias("MBR_VBB_CMPNT_ENR_DT_SK"),
    F.col("MBR_VBB_CMPNT_TERM_DT_SK").alias("MBR_VBB_CMPNT_TERM_DT_SK"),
    F.col("SRC_SYS_CRT_DTM").alias("SRC_SYS_CRT_DTM"),
    F.col("SRC_SYS_UPDT_DTM").alias("SRC_SYS_UPDT_DTM"),
    F.col("VBB_VNDR_UNIQ_KEY").alias("VBB_VNDR_UNIQ_KEY"),
    F.col("VNDR_PGM_SEQ_NO").alias("VNDR_PGM_SEQ_NO"),
    F.col("MBR_CMPLD_ACHV_LVL_CT").alias("MBR_CMPLD_ACHV_LVL_CT"),
    F.col("TRZ_MBR_UNVRS_ID").alias("TRZ_MBR_UNVRS_ID")
)

df_ForeignKey_RecycleOut_write = (
    df_ForeignKey_RecycleOut_sel
    .withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " "))
    .withColumn("MBR_CNTCT_IN", F.rpad(F.col("MBR_CNTCT_IN"), 1, " "))
    .withColumn("MBR_CNTCT_LAST_UPDT_DT_SK", F.rpad(F.col("MBR_CNTCT_LAST_UPDT_DT_SK"), 10, " "))
    .withColumn("MBR_VBB_CMPNT_CMPLTN_DT_SK", F.rpad(F.col("MBR_VBB_CMPNT_CMPLTN_DT_SK"), 10, " "))
    .withColumn("MBR_VBB_CMPNT_ENR_DT_SK", F.rpad(F.col("MBR_VBB_CMPNT_ENR_DT_SK"), 10, " "))
    .withColumn("MBR_VBB_CMPNT_TERM_DT_SK", F.rpad(F.col("MBR_VBB_CMPNT_TERM_DT_SK"), 10, " "))
)

write_files(
    df_ForeignKey_RecycleOut_write,
    f"{adls_path}/hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_Collector_Fkey = df_ForeignKey_FkeyOut.select(
    F.col("MBR_VBB_CMPNT_ENR_SK").alias("MBR_VBB_CMPNT_ENR_SK"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("VBB_CMPNT_UNIQ_KEY").alias("VBB_CMPNT_UNIQ_KEY"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("svClsSk").alias("CLS_SK"),
    F.col("svClsPlnSk").alias("CLS_PLN_SK"),
    F.col("svGrpSk").alias("GRP_SK"),
    F.col("svMbrSk").alias("MBR_SK"),
    F.col("MBR_VBB_PLN_ENR_SK").alias("MBR_VBB_PLN_ENR_SK"),
    F.col("svVbbCmpntSk").alias("VBB_CMPNT_SK"),
    F.col("svMbrVbbCmpntEnrMethCdSk").alias("MBR_VBB_CMPNT_ENR_METH_CD_SK"),
    F.col("svMbrVbbCmpntSttusCdSk").alias("MBR_VBB_CMPNT_STTUS_CD_SK"),
    F.col("svMbrVbbCmpntTermMethCdSk").alias("MBR_VBB_CMPNT_TERM_METH_CD_SK"),
    F.col("svMbrVbbCmpntTermRsnCdSk").alias("MBR_VBB_CMPNT_TERM_RSN_CD_SK"),
    F.col("MBR_CNTCT_IN").alias("MBR_CNTCT_IN"),
    F.col("MBR_CNTCT_LAST_UPDT_DT_SK").alias("MBR_CNTCT_LAST_UPDT_DT_SK"),
    F.col("MBR_VBB_CMPNT_CMPLTN_DT_SK").alias("MBR_VBB_CMPNT_CMPLTN_DT_SK"),
    F.col("MBR_VBB_CMPNT_ENR_DT_SK").alias("MBR_VBB_CMPNT_ENR_DT_SK"),
    F.col("MBR_VBB_CMPNT_TERM_DT_SK").alias("MBR_VBB_CMPNT_TERM_DT_SK"),
    F.col("SRC_SYS_CRT_DTM").alias("SRC_SYS_CRT_DTM"),
    F.col("SRC_SYS_UPDT_DTM").alias("SRC_SYS_UPDT_DTM"),
    F.col("VBB_VNDR_UNIQ_KEY").alias("VBB_VNDR_UNIQ_KEY"),
    F.col("VNDR_PGM_SEQ_NO").alias("VNDR_PGM_SEQ_NO"),
    F.col("MBR_CMPLD_ACHV_LVL_CT").alias("MBR_CMPLD_ACHV_LVL_CT"),
    F.col("TRZ_MBR_UNVRS_ID").alias("TRZ_MBR_UNVRS_ID")
)

df_Collector_DefaultUNK = df_ForeignKey_DefaultUNKOut.select(
    F.lit(0).alias("MBR_VBB_CMPNT_ENR_SK"),
    F.lit(0).alias("MBR_UNIQ_KEY"),
    F.lit(0).alias("VBB_CMPNT_UNIQ_KEY"),
    F.lit(0).alias("SRC_SYS_CD_SK"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("CLS_SK"),
    F.lit(0).alias("CLS_PLN_SK"),
    F.lit(0).alias("GRP_SK"),
    F.lit(0).alias("MBR_SK"),
    F.lit(0).alias("MBR_VBB_PLN_ENR_SK"),
    F.lit(0).alias("VBB_CMPNT_SK"),
    F.lit(0).alias("MBR_VBB_CMPNT_ENR_METH_CD_SK"),
    F.lit(0).alias("MBR_VBB_CMPNT_STTUS_CD_SK"),
    F.lit(0).alias("MBR_VBB_CMPNT_TERM_METH_CD_SK"),
    F.lit(0).alias("MBR_VBB_CMPNT_TERM_RSN_CD_SK"),
    F.lit("U").alias("MBR_CNTCT_IN"),
    F.lit("1753-01-01").alias("MBR_CNTCT_LAST_UPDT_DT_SK"),
    F.lit("1753-01-01").alias("MBR_VBB_CMPNT_CMPLTN_DT_SK"),
    F.lit("1753-01-01").alias("MBR_VBB_CMPNT_ENR_DT_SK"),
    F.lit("1753-01-01").alias("MBR_VBB_CMPNT_TERM_DT_SK"),
    FORMAT_DATE("1753-01-01 00:00:00.000","SYBASE","TIMESTAMP","DB2TIMESTAMP").alias("SRC_SYS_CRT_DTM"),
    FORMAT_DATE("1753-01-01 00:00:00.000","SYBASE","TIMESTAMP","DB2TIMESTAMP").alias("SRC_SYS_UPDT_DTM"),
    F.lit(0).alias("VBB_VNDR_UNIQ_KEY"),
    F.lit(0).alias("VNDR_PGM_SEQ_NO"),
    F.lit(0).alias("MBR_CMPLD_ACHV_LVL_CT"),
    F.lit("UNK").alias("TRZ_MBR_UNVRS_ID")
)

df_Collector_DefaultNA = df_ForeignKey_DefaultNAOut.select(
    F.lit(1).alias("MBR_VBB_CMPNT_ENR_SK"),
    F.lit(1).alias("MBR_UNIQ_KEY"),
    F.lit(1).alias("VBB_CMPNT_UNIQ_KEY"),
    F.lit(1).alias("SRC_SYS_CD_SK"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("CLS_SK"),
    F.lit(1).alias("CLS_PLN_SK"),
    F.lit(1).alias("GRP_SK"),
    F.lit(1).alias("MBR_SK"),
    F.lit(1).alias("MBR_VBB_PLN_ENR_SK"),
    F.lit(1).alias("VBB_CMPNT_SK"),
    F.lit(1).alias("MBR_VBB_CMPNT_ENR_METH_CD_SK"),
    F.lit(1).alias("MBR_VBB_CMPNT_STTUS_CD_SK"),
    F.lit(1).alias("MBR_VBB_CMPNT_TERM_METH_CD_SK"),
    F.lit(1).alias("MBR_VBB_CMPNT_TERM_RSN_CD_SK"),
    F.lit("X").alias("MBR_CNTCT_IN"),
    F.lit("1753-01-01").alias("MBR_CNTCT_LAST_UPDT_DT_SK"),
    F.lit("1753-01-01").alias("MBR_VBB_CMPNT_CMPLTN_DT_SK"),
    F.lit("1753-01-01").alias("MBR_VBB_CMPNT_ENR_DT_SK"),
    F.lit("1753-01-01").alias("MBR_VBB_CMPNT_TERM_DT_SK"),
    FORMAT_DATE("1753-01-01 00:00:00.000","SYBASE","TIMESTAMP","DB2TIMESTAMP").alias("SRC_SYS_CRT_DTM"),
    FORMAT_DATE("1753-01-01 00:00:00.000","SYBASE","TIMESTAMP","DB2TIMESTAMP").alias("SRC_SYS_UPDT_DTM"),
    F.lit(1).alias("VBB_VNDR_UNIQ_KEY"),
    F.lit(1).alias("VNDR_PGM_SEQ_NO"),
    F.lit(1).alias("MBR_CMPLD_ACHV_LVL_CT"),
    F.lit("NA").alias("TRZ_MBR_UNVRS_ID")
)

df_Collector = (
    df_Collector_Fkey
    .unionByName(df_Collector_DefaultUNK)
    .unionByName(df_Collector_DefaultNA)
)

df_Collector_final = (
    df_Collector
    .withColumn("MBR_CNTCT_IN", F.rpad(F.col("MBR_CNTCT_IN"), 1, " "))
    .withColumn("MBR_CNTCT_LAST_UPDT_DT_SK", F.rpad(F.col("MBR_CNTCT_LAST_UPDT_DT_SK"), 10, " "))
    .withColumn("MBR_VBB_CMPNT_CMPLTN_DT_SK", F.rpad(F.col("MBR_VBB_CMPNT_CMPLTN_DT_SK"), 10, " "))
    .withColumn("MBR_VBB_CMPNT_ENR_DT_SK", F.rpad(F.col("MBR_VBB_CMPNT_ENR_DT_SK"), 10, " "))
    .withColumn("MBR_VBB_CMPNT_TERM_DT_SK", F.rpad(F.col("MBR_VBB_CMPNT_TERM_DT_SK"), 10, " "))
)

write_files(
    df_Collector_final.select(
        "MBR_VBB_CMPNT_ENR_SK",
        "MBR_UNIQ_KEY",
        "VBB_CMPNT_UNIQ_KEY",
        "SRC_SYS_CD_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CLS_SK",
        "CLS_PLN_SK",
        "GRP_SK",
        "MBR_SK",
        "MBR_VBB_PLN_ENR_SK",
        "VBB_CMPNT_SK",
        "MBR_VBB_CMPNT_ENR_METH_CD_SK",
        "MBR_VBB_CMPNT_STTUS_CD_SK",
        "MBR_VBB_CMPNT_TERM_METH_CD_SK",
        "MBR_VBB_CMPNT_TERM_RSN_CD_SK",
        "MBR_CNTCT_IN",
        "MBR_CNTCT_LAST_UPDT_DT_SK",
        "MBR_VBB_CMPNT_CMPLTN_DT_SK",
        "MBR_VBB_CMPNT_ENR_DT_SK",
        "MBR_VBB_CMPNT_TERM_DT_SK",
        "SRC_SYS_CRT_DTM",
        "SRC_SYS_UPDT_DTM",
        "VBB_VNDR_UNIQ_KEY",
        "VNDR_PGM_SEQ_NO",
        "MBR_CMPLD_ACHV_LVL_CT",
        "TRZ_MBR_UNVRS_ID"
    ),
    f"{adls_path}/load/MBR_VBB_CMPNT_ENR.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)