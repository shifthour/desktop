# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:
# MAGIC 
# MAGIC PROCESSING:
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #               Change Description                                                          Development Project      Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------     ---------------------------------------------------------                                  ----------------------------------   ---------------------------------  -------------------------------
# MAGIC Parikshith Chada               2/25/2007                                              Originally Programmed                                                          devlIDS30                  Steph Goddard            
# MAGIC              
# MAGIC Bhoomi Dasari                   3/18/2009       Prodsupp/15                   Updated Mbr_sk logic                                                          devlIDS                     Steph Goddard              04/01/2009
# MAGIC 
# MAGIC Manasa Andru                  7/2/2013          TTR - 901                Changed the field name PRVCY_MBR_SRC_CD_SK to          IntegrateCurDevl           Kalyan Neelam               2013-07-05
# MAGIC                                                                                               PRVCY_MBR_SRC_CD and datatype from Integer to Char
# MAGIC                                                                                                                in Foreign Key transformer

# MAGIC Read common record format file from extract job.
# MAGIC Writing Sequential File to /load
# MAGIC Merge source data with default rows
# MAGIC Set all foreign surrogate keys
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, TimestampType, DecimalType
)
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Retrieve job parameters
InFile = get_widget_value('InFile', 'FctsPrvcyAuthExtr.PrvcyAuth.dat')
Logging = get_widget_value('Logging', 'Y')

# Define schema for IdsPrvcyAuthExtr stage
schema_IdsPrvcyAuthExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),     # char(10)
    StructField("DISCARD_IN", StringType(), False),        # char(1)
    StructField("PASS_THRU_IN", StringType(), False),      # char(1)
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(10, 0), False),
    StructField("SRC_SYS_CD", StringType(), False),        # varchar
    StructField("PRI_KEY_STRING", StringType(), False),    # varchar
    StructField("PRVCY_AUTH_SK", IntegerType(), False),
    StructField("PRVCY_MBR_UNIQ_KEY", IntegerType(), False),
    StructField("SEQ_NO", IntegerType(), False),
    StructField("PRVCY_MBR_SRC_CD", StringType(), False),  # char(10)
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("MBR_SK", IntegerType(), False),
    StructField("PMAT_ID", StringType(), False),           # char(8)
    StructField("PMAX_PZCD_RQR_RTYP", StringType(), False),# char(4)
    StructField("PMAX_ENEN_CTYP", StringType(), False),    # char(4)
    StructField("PMAX_PZCD_TRSN", StringType(), False),    # char(4)
    StructField("PRVCY_EXTRNL_MBR_SK", IntegerType(), False),
    StructField("RECPNT_EXTRNL_ENTY_SK", IntegerType(), False),
    StructField("AUTH_RVKD_IN", StringType(), False),      # char(1)
    StructField("PMAX_CREATE_DTM", StringType(), False),   # char(10)
    StructField("PMAX_EFF_DT", StringType(), False),       # char(10)
    StructField("PMAX_ORIG_END_DT", StringType(), False),  # char(10)
    StructField("PMAX_TERM_DTM", StringType(), False),     # char(10)
    StructField("AUTH_DESC", StringType(), False),         # varchar
    StructField("RECPNT_ID", StringType(), False),         # varchar
    StructField("PMAX_LAST_UPD_DTM", StringType(), False), # char(10)
    StructField("PMAX_LAST_USUS_ID", StringType(), False)  # char(10)
])

# Read file for IdsPrvcyAuthExtr (CSeqFileStage)
df_IdsPrvcyAuthExtr = (
    spark.read.format("csv")
    .option("header", False)
    .option("quote", '"')
    .option("delimiter", ",")
    .schema(schema_IdsPrvcyAuthExtr)
    .load(f"{adls_path}/key/{InFile}")
)

# Apply Transformer logic (ForeignKey stage), defining stage variables
df_ForeignKey_stagevar = (
    df_IdsPrvcyAuthExtr
    .withColumn("SrcSysCdSk", GetFkeyCodes(F.lit("IDS"), F.col("PRVCY_AUTH_SK"), F.lit("SOURCE SYSTEM"), F.col("SRC_SYS_CD"), Logging))
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("svPrvcyMbrSrcCdSk", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("PRVCY_AUTH_SK"), F.lit("PRIVACY MEMBER SOURCE"), F.col("PRVCY_MBR_SRC_CD"), Logging))
    .withColumn("svMbrSk", GetFkeyMbr(F.col("SRC_SYS_CD"), F.col("PRVCY_AUTH_SK"), F.col("MBR_SK"), Logging))
    .withColumn("svPrvcyExtrnlMbrsK", GetFkeyPrvcyExtrnlMbr(F.col("SRC_SYS_CD"), F.col("PRVCY_AUTH_SK"), F.col("PRVCY_EXTRNL_MBR_SK"), Logging))
    .withColumn("svRecpntExtrnlEntySk", GetFkeyPrvcyExtrnlEnty(F.col("SRC_SYS_CD"), F.col("PRVCY_AUTH_SK"), F.col("RECPNT_EXTRNL_ENTY_SK"), Logging))
    .withColumn("svPrvcyAuthRqstrTypCdSk", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("PRVCY_AUTH_SK"), F.lit("PRIVACY AUTHORIZATION REQUESTOR TYPE"), F.col("PMAX_PZCD_RQR_RTYP"), Logging))
    .withColumn("svPrcyAuthRecpntTypCdSk", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("PRVCY_AUTH_SK"), F.lit("PRIVACY AUTHORIZATION RECIPIENT TYPE"), F.col("PMAX_ENEN_CTYP"), Logging))
    .withColumn("svPrvcyAuthTermRsnCdSk", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("PRVCY_AUTH_SK"), F.lit("PRIVACY AUTHORIZATION TERMINATION REASON"), F.col("PMAX_PZCD_TRSN"), Logging))
    .withColumn("svCrtDtSk", GetFkeyDate(F.lit("IDS"), F.col("PRVCY_AUTH_SK"), F.col("PMAX_CREATE_DTM"), Logging))
    .withColumn("svEffDtSk", GetFkeyDate(F.lit("IDS"), F.col("PRVCY_AUTH_SK"), F.col("PMAX_EFF_DT"), Logging))
    .withColumn("svOrigEnDtSk", GetFkeyDate(F.lit("IDS"), F.col("PRVCY_AUTH_SK"), F.col("PMAX_ORIG_END_DT"), Logging))
    .withColumn("svTermDtSk", GetFkeyDate(F.lit("IDS"), F.col("PRVCY_AUTH_SK"), F.col("PMAX_TERM_DTM"), Logging))
    .withColumn("svSrcSysLastUpdtDtSk", GetFkeyDate(F.lit("IDS"), F.col("PRVCY_AUTH_SK"), F.col("PMAX_LAST_UPD_DTM"), Logging))
    .withColumn("svSrcSysLastUpdtUserSk", GetFkeyAppUsr(F.col("SRC_SYS_CD"), F.col("PRVCY_AUTH_SK"), F.col("PMAX_LAST_USUS_ID"), Logging))
    .withColumn("svPrvcyAuthMethSk", GetFkeyPrvcyAuthMeth(F.col("SRC_SYS_CD"), F.col("PRVCY_AUTH_SK"), F.col("PMAX_LAST_USUS_ID"), Logging))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("PRVCY_AUTH_SK")))
)

# Output link "Fkey": ErrCount = 0 or PassThru = 'Y'
df_ForeignKey_fkey = (
    df_ForeignKey_stagevar
    .filter((F.col("ErrCount") == 0) | (F.col("PassThru") == 'Y'))
    .select(
        F.col("PRVCY_AUTH_SK").alias("PRVCY_AUTH_SK"),
        F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
        F.col("PRVCY_MBR_UNIQ_KEY").alias("PRVCY_MBR_UNIQ_KEY"),
        F.col("SEQ_NO").alias("SEQ_NO"),
        F.col("svPrvcyMbrSrcCdSk").alias("PRVCY_MBR_SRC_CD_SK"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.when(F.col("PRVCY_MBR_SRC_CD") == 'F', F.col("svMbrSk")).otherwise(F.lit(1)).alias("MBR_SK"),
        F.col("svPrvcyAuthMethSk").alias("PRVCY_AUTH_METH_SK"),
        F.when(F.col("PRVCY_MBR_SRC_CD") == 'N', F.col("svPrvcyExtrnlMbrsK")).otherwise(F.lit(1)).alias("PRVCY_EXTRNL_MBR_SK"),
        F.col("svRecpntExtrnlEntySk").alias("RECPNT_EXTRNL_ENTY_SK"),
        F.col("svPrvcyAuthRqstrTypCdSk").alias("PRVCY_AUTH_RQSTR_TYP_CD_SK"),
        F.col("svPrcyAuthRecpntTypCdSk").alias("PRVCY_AUTH_RECPNT_TYP_CD_SK"),
        F.col("svPrvcyAuthTermRsnCdSk").alias("PRVCY_AUTH_TERM_RSN_CD_SK"),
        F.col("AUTH_RVKD_IN").alias("AUTH_RVKD_IN"),
        F.col("svCrtDtSk").alias("CRT_DT_SK"),
        F.col("svEffDtSk").alias("EFF_DT_SK"),
        F.col("svOrigEnDtSk").alias("ORIG_END_DT_SK"),
        F.col("svTermDtSk").alias("TERM_DT_SK"),
        F.col("AUTH_DESC").alias("AUTH_DESC"),
        F.col("RECPNT_ID").alias("RECPNT_ID"),
        F.col("svSrcSysLastUpdtDtSk").alias("SRC_SYS_LAST_UPDT_DT_SK"),
        F.col("svSrcSysLastUpdtUserSk").alias("SRC_SYS_LAST_UPDT_USER_SK")
    )
)

# Output link "Recycle": ErrCount > 0
df_ForeignKey_recycle = (
    df_ForeignKey_stagevar
    .filter(F.col("ErrCount") > 0)
    .select(
        GetRecycleKey(F.col("PRVCY_AUTH_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("DISCARD_IN").alias("DISCARD_IN"),
        F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("ErrCount").alias("ERR_CT"),
        (F.col("RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("PRVCY_AUTH_SK").alias("PRVCY_AUTH_SK"),
        F.col("PRVCY_MBR_UNIQ_KEY").alias("PRVCY_MBR_UNIQ_KEY"),
        F.col("SEQ_NO").alias("SEQ_NO"),
        F.col("PRVCY_MBR_SRC_CD").alias("PRVCY_MBR_SRC_CD"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("MBR_SK").alias("MBR_SK"),
        F.col("PMAT_ID").alias("PMAT_ID"),
        F.col("PMAX_PZCD_RQR_RTYP").alias("PMAX_PZCD_RQR_RTYP"),
        F.col("PMAX_ENEN_CTYP").alias("PMAX_ENEN_CTYP"),
        F.col("PMAX_PZCD_TRSN").alias("PMAX_PZCD_TRSN"),
        F.col("PRVCY_EXTRNL_MBR_SK").alias("PRVCY_EXTRNL_MBR_SK"),
        F.col("RECPNT_EXTRNL_ENTY_SK").alias("RECPNT_EXTRNL_ENTY_SK"),
        F.col("AUTH_RVKD_IN").alias("AUTH_RVKD_IN"),
        F.col("PMAX_CREATE_DTM").alias("PMAX_CREATE_DTM"),
        F.col("PMAX_EFF_DT").alias("PMAX_EFF_DT"),
        F.col("PMAX_ORIG_END_DT").alias("PMAX_ORIG_END_DT"),
        F.col("PMAX_TERM_DTM").alias("PMAX_TERM_DTM"),
        F.col("AUTH_DESC").alias("AUTH_DESC"),
        F.col("RECPNT_ID").alias("RECPNT_ID"),
        F.col("PMAX_LAST_UPD_DTM").alias("PMAX_LAST_UPD_DTM"),
        F.col("PMAX_LAST_USUS_ID").alias("PMAX_LAST_USUS_ID")
    )
)

# Output link "DefaultUNK": single row
data_defaultUNK = [(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 'U', 'UNK', 'UNK', 'UNK', 'UNK', 'UNK', 'UNK', 'UNK', 'UNK', 0)]
schema_defaultUNK = StructType([
    StructField("PRVCY_AUTH_SK", IntegerType(), False),
    StructField("SRC_SYS_CD_SK", IntegerType(), False),
    StructField("PRVCY_MBR_UNIQ_KEY", IntegerType(), False),
    StructField("SEQ_NO", IntegerType(), False),
    StructField("PRVCY_MBR_SRC_CD_SK", IntegerType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("MBR_SK", IntegerType(), False),
    StructField("PRVCY_AUTH_METH_SK", IntegerType(), False),
    StructField("PRVCY_EXTRNL_MBR_SK", IntegerType(), False),
    StructField("RECPNT_EXTRNL_ENTY_SK", IntegerType(), False),
    StructField("PRVCY_AUTH_RQSTR_TYP_CD_SK", IntegerType(), False),
    StructField("PRVCY_AUTH_RECPNT_TYP_CD_SK", IntegerType(), False),
    StructField("PRVCY_AUTH_TERM_RSN_CD_SK", IntegerType(), False),
    StructField("AUTH_RVKD_IN", StringType(), False),
    StructField("CRT_DT_SK", StringType(), False),
    StructField("EFF_DT_SK", StringType(), False),
    StructField("ORIG_END_DT_SK", StringType(), False),
    StructField("TERM_DT_SK", StringType(), False),
    StructField("AUTH_DESC", StringType(), False),
    StructField("RECPNT_ID", StringType(), False),
    StructField("SRC_SYS_LAST_UPDT_DT_SK", StringType(), False),
    StructField("SRC_SYS_LAST_UPDT_USER_SK", IntegerType(), False)
])
df_ForeignKey_defaultUNK = spark.createDataFrame(data_defaultUNK, schema_defaultUNK)

# Output link "DefaultNA": single row
data_defaultNA = [(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 'X', 'NA', 'NA', 'NA', 'NA', 'NA', 'NA', 'NA', 'NA', 1)]
schema_defaultNA = schema_defaultUNK
df_ForeignKey_defaultNA = spark.createDataFrame(data_defaultNA, schema_defaultNA)

# Collector stage: union the three links for final output
columns_collector = [
    "PRVCY_AUTH_SK",
    "SRC_SYS_CD_SK",
    "PRVCY_MBR_UNIQ_KEY",
    "SEQ_NO",
    "PRVCY_MBR_SRC_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "MBR_SK",
    "PRVCY_AUTH_METH_SK",
    "PRVCY_EXTRNL_MBR_SK",
    "RECPNT_EXTRNL_ENTY_SK",
    "PRVCY_AUTH_RQSTR_TYP_CD_SK",
    "PRVCY_AUTH_RECPNT_TYP_CD_SK",
    "PRVCY_AUTH_TERM_RSN_CD_SK",
    "AUTH_RVKD_IN",
    "CRT_DT_SK",
    "EFF_DT_SK",
    "ORIG_END_DT_SK",
    "TERM_DT_SK",
    "AUTH_DESC",
    "RECPNT_ID",
    "SRC_SYS_LAST_UPDT_DT_SK",
    "SRC_SYS_LAST_UPDT_USER_SK"
]

df_collector = (
    df_ForeignKey_fkey.select(columns_collector)
    .unionByName(df_ForeignKey_defaultUNK.select(columns_collector))
    .unionByName(df_ForeignKey_defaultNA.select(columns_collector))
)

# Write the recycle link to hashed file -> scenario C => convert to parquet
# First add rpad for char/varchar columns
df_ForeignKey_recycle_rpad = (
    df_ForeignKey_recycle
    .withColumn("INSRT_UPDT_CD", rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", rpad(F.col("PASS_THRU_IN"), 1, " "))
    .withColumn("PRVCY_MBR_SRC_CD", rpad(F.col("PRVCY_MBR_SRC_CD"), 10, " "))
    .withColumn("PMAT_ID", rpad(F.col("PMAT_ID"), 8, " "))
    .withColumn("PMAX_PZCD_RQR_RTYP", rpad(F.col("PMAX_PZCD_RQR_RTYP"), 4, " "))
    .withColumn("PMAX_ENEN_CTYP", rpad(F.col("PMAX_ENEN_CTYP"), 4, " "))
    .withColumn("PMAX_PZCD_TRSN", rpad(F.col("PMAX_PZCD_TRSN"), 4, " "))
    .withColumn("AUTH_RVKD_IN", rpad(F.col("AUTH_RVKD_IN"), 1, " "))
    .withColumn("PMAX_CREATE_DTM", rpad(F.col("PMAX_CREATE_DTM"), 10, " "))
    .withColumn("PMAX_EFF_DT", rpad(F.col("PMAX_EFF_DT"), 10, " "))
    .withColumn("PMAX_ORIG_END_DT", rpad(F.col("PMAX_ORIG_END_DT"), 10, " "))
    .withColumn("PMAX_TERM_DTM", rpad(F.col("PMAX_TERM_DTM"), 10, " "))
    .withColumn("PMAX_LAST_UPD_DTM", rpad(F.col("PMAX_LAST_UPD_DTM"), 10, " "))
    .withColumn("PMAX_LAST_USUS_ID", rpad(F.col("PMAX_LAST_USUS_ID"), 10, " "))
    .withColumn("SRC_SYS_CD", rpad(F.col("SRC_SYS_CD"), 255, " "))
    .withColumn("PRI_KEY_STRING", rpad(F.col("PRI_KEY_STRING"), 255, " "))
    .withColumn("AUTH_DESC", rpad(F.col("AUTH_DESC"), 255, " "))
    .withColumn("RECPNT_ID", rpad(F.col("RECPNT_ID"), 255, " "))
)

# Select columns in the correct order for the recycle output
recycle_columns = [
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "PRVCY_AUTH_SK",
    "PRVCY_MBR_UNIQ_KEY",
    "SEQ_NO",
    "PRVCY_MBR_SRC_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "MBR_SK",
    "PMAT_ID",
    "PMAX_PZCD_RQR_RTYP",
    "PMAX_ENEN_CTYP",
    "PMAX_PZCD_TRSN",
    "PRVCY_EXTRNL_MBR_SK",
    "RECPNT_EXTRNL_ENTY_SK",
    "AUTH_RVKD_IN",
    "PMAX_CREATE_DTM",
    "PMAX_EFF_DT",
    "PMAX_ORIG_END_DT",
    "PMAX_TERM_DTM",
    "AUTH_DESC",
    "RECPNT_ID",
    "PMAX_LAST_UPD_DTM",
    "PMAX_LAST_USUS_ID"
]

df_ForeignKey_recycle_final = df_ForeignKey_recycle_rpad.select(recycle_columns)

write_files(
    df_ForeignKey_recycle_final,
    f"{adls_path}/hf_bhoomi_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote='"',
    nullValue=None
)

# For the final collector output to PRVCY_AUTH.dat, rpad for char/varchar columns
df_collector_final = (
    df_collector
    .withColumn("AUTH_RVKD_IN", rpad(F.col("AUTH_RVKD_IN"), 1, " "))
    .withColumn("CRT_DT_SK", rpad(F.col("CRT_DT_SK"), 10, " "))
    .withColumn("EFF_DT_SK", rpad(F.col("EFF_DT_SK"), 10, " "))
    .withColumn("ORIG_END_DT_SK", rpad(F.col("ORIG_END_DT_SK"), 10, " "))
    .withColumn("TERM_DT_SK", rpad(F.col("TERM_DT_SK"), 10, " "))
    .withColumn("AUTH_DESC", rpad(F.col("AUTH_DESC"), 255, " "))
    .withColumn("RECPNT_ID", rpad(F.col("RECPNT_ID"), 255, " "))
    .withColumn("SRC_SYS_LAST_UPDT_DT_SK", rpad(F.col("SRC_SYS_LAST_UPDT_DT_SK"), 10, " "))
)

write_files(
    df_collector_final.select(columns_collector),
    f"{adls_path}/load/PRVCY_AUTH.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)