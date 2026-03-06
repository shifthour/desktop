# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Called By: FctsMedMgtCntl
# MAGIC 
# MAGIC 
# MAGIC Process :  Case Mgt Contact Fkey Process  which is processed and loaded to CASE_MGT_CNTCT table
# MAGIC 
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                              		  Project/                                                                                                    	Code                  Date
# MAGIC Developer         	Date             	 Altiris #     	Change Description                                                   	Reviewer            Reviewed
# MAGIC -----------------------  	-------------------   	-------------   	-----------------------------------------------------------------------------   	-------------------------  -------------------
# MAGIC AKASH PARSHA     2019-09-26             US140165               Case Mgt Contact FKey Process                                          Jaideep Mankala    10/10/2019

# MAGIC This is a load ready file that will go into Load job
# MAGIC Code lookups operation to get the needed FKEY values
# MAGIC FKEY failures are written into this flat file.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------

SrcSysCd = get_widget_value("SrcSysCd","")
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
RunID = get_widget_value("RunID","")
PrefixFkeyFailedFileName = get_widget_value("PrefixFkeyFailedFileName","")
RunDateTime = get_widget_value("RunDateTime","")
DSJobName = "IdsCaseMgtContactDtlFkey"

jdbc_url_IDS, jdbc_props_IDS = get_db_config(ids_secret_name)

extract_query = f"SELECT DISTINCT USER_SK, USER_ID FROM {IDSOwner}.APP_USER WHERE USER_SK NOT IN (0,1)"
df_APP_USER = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS)
    .options(**jdbc_props_IDS)
    .option("query", extract_query)
    .load()
)

schema_CASE_MGT_CONTACT_DtlPKEY = StructType([
    StructField("CASE_MGT_ID", StringType(), False),
    StructField("CNTCT_SEQ_NO", IntegerType(), False),
    StructField("LOG_SEQ_NO", IntegerType(), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("CASE_MGT_CNTCT_DTL_LOG_SK", IntegerType(), False),
    StructField("PRI_NAT_KEY_STRING", StringType(), False),
    StructField("FIRST_RECYC_TS", TimestampType(), False),
    StructField("LOG_DTM", TimestampType(), False),
    StructField("LOG_SUM_TX", StringType(), False),
    StructField("CMLG_MCTR_CALL", StringType(), False),
    StructField("CMLG_METHOD", StringType(), False),
    StructField("CMLG_USID", StringType(), False)
])
file_path_CASE_MGT_CONTACT_DtlPKEY = f"{adls_path}/key/CASE_MGT_CNT_DTL_LOG.{SrcSysCd}.pkey.{RunID}.dat"
df_CASE_MGT_CONTACT_DtlPKEY = (
    spark.read
    .option("delimiter", ",")
    .option("quote", "^")
    .option("header", "false")
    .schema(schema_CASE_MGT_CONTACT_DtlPKEY)
    .csv(file_path_CASE_MGT_CONTACT_DtlPKEY)
)

extract_query = f"SELECT DISTINCT CM.CASE_MGT_ID, CM.CASE_MGT_SK FROM {IDSOwner}.CASE_MGT CM, {IDSOwner}.CD_MPPNG CD WHERE CD.CD_MPPNG_SK = CM.SRC_SYS_CD_SK AND CD.TRGT_CD = 'FACETS'"
df_CASE_MGT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS)
    .options(**jdbc_props_IDS)
    .option("query", extract_query)
    .load()
)

extract_query = f"SELECT CASE_MGT_CNTCT_SK, CASE_MGT_ID, CNTCT_SEQ_NO FROM {IDSOwner}.CASE_MGT_CNTCT"
df_Contact_Seq = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS)
    .options(**jdbc_props_IDS)
    .option("query", extract_query)
    .load()
)

extract_query = (
    f"SELECT SRC_CD, CD_MPPNG_SK FROM {IDSOwner}.CD_MPPNG "
    f"WHERE TRGT_DOMAIN_NM = 'DISEASE MANAGEMENT CONTACT TYPE' "
    f"AND TRGT_CLCTN_CD = 'IDS' "
    f"AND SRC_SYS_CD = 'FACETS' "
    f"AND SRC_DOMAIN_NM = 'DISEASE MANAGEMENT CONTACT TYPE' "
    f"AND SRC_CLCTN_CD = 'FACETS DBO'"
)
df_METH_CD_LKP = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS)
    .options(**jdbc_props_IDS)
    .option("query", extract_query)
    .load()
)

extract_query = (
    f"SELECT SRC_CD, CD_MPPNG_SK FROM {IDSOwner}.CD_MPPNG "
    f"WHERE TRGT_DOMAIN_NM = 'CASE MANAGEMENT CONTACT TYPE' "
    f"AND TRGT_CLCTN_CD = 'IDS' "
    f"AND SRC_SYS_CD = 'FACETS' "
    f"AND SRC_DOMAIN_NM = 'CASE MANAGEMENT CONTACT TYPE' "
    f"AND SRC_CLCTN_CD = 'FACETS DBO'"
)
df_CNTCT_TYP_CD = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS)
    .options(**jdbc_props_IDS)
    .option("query", extract_query)
    .load()
)

df_CASE_MGT_CONTACT_DtlPKEY_withcol = df_CASE_MGT_CONTACT_DtlPKEY.withColumn("PRI_CASE_MGR_USER_SK", F.lit(None).cast(IntegerType()))

df_lkp_stage1 = df_CASE_MGT_CONTACT_DtlPKEY_withcol.alias("pm").join(
    df_CNTCT_TYP_CD.alias("ct"),
    F.col("pm.CMLG_MCTR_CALL") == F.col("ct.SRC_CD"),
    "left"
)
df_lkp_stage2 = df_lkp_stage1.join(
    df_METH_CD_LKP.alias("mc"),
    F.col("pm.CMLG_METHOD") == F.col("mc.SRC_CD"),
    "left"
)
df_lkp_stage3 = df_lkp_stage2.join(
    df_Contact_Seq.alias("cs"),
    (F.col("pm.CASE_MGT_ID") == F.col("cs.CASE_MGT_ID")) & (F.col("pm.CNTCT_SEQ_NO") == F.col("cs.CNTCT_SEQ_NO")),
    "left"
)
df_lkp_stage4 = df_lkp_stage3.join(
    df_CASE_MGT.alias("cm"),
    F.col("pm.CASE_MGT_ID") == F.col("cm.CASE_MGT_ID"),
    "left"
)
df_lkp_stage5 = df_lkp_stage4.join(
    df_APP_USER.alias("au"),
    (F.col("pm.PRI_CASE_MGR_USER_SK") == F.col("au.USER_SK")) & (F.col("pm.CMLG_USID") == F.col("au.USER_ID")),
    "left"
)

df_lkp_Code_SKs = df_lkp_stage5.select(
    F.col("pm.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("pm.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("pm.CASE_MGT_CNTCT_DTL_LOG_SK").alias("CASE_MGT_CNTCT_DTL_LOG_SK"),
    F.col("pm.CASE_MGT_ID").alias("CASE_MGT_ID"),
    F.col("pm.CNTCT_SEQ_NO").alias("CNTCT_SEQ_NO"),
    F.col("pm.LOG_SEQ_NO").alias("LOG_SEQ_NO"),
    F.col("pm.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("pm.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("pm.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("cs.CASE_MGT_CNTCT_SK").alias("CASE_MGT_CNTCT_SK"),
    F.col("pm.LOG_DTM").alias("LOG_DTM"),
    F.col("pm.LOG_SUM_TX").alias("LOG_SUM_TX"),
    F.col("pm.CMLG_MCTR_CALL").alias("CMLG_MCTR_CALL"),
    F.col("pm.CMLG_METHOD").alias("CMLG_METHOD"),
    F.col("pm.CMLG_USID").alias("CMLG_USID"),
    F.col("cm.CASE_MGT_SK").alias("CASE_MGT_SK"),
    F.col("ct.CD_MPPNG_SK").alias("LOG_CNTCT_TYP_CD_SK"),
    F.col("mc.CD_MPPNG_SK").alias("LOG_CNTCT_METH_CD_SK"),
    F.col("au.USER_SK").alias("LOG_USER_SK")
)

df_xfm_CheckLkpResults = (
    df_lkp_Code_SKs
    .withColumn(
        "svCaseMgtLkpFailCheck",
        F.when(
            F.col("CASE_MGT_SK").isNull() & (trim(F.col("CASE_MGT_ID")) != F.lit("NA")),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "svCntctLkpFailCheck",
        F.when(
            F.col("CASE_MGT_CNTCT_SK").isNull(),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "svCntctTypCdLkupFailCheck",
        F.when(
            F.col("LOG_CNTCT_TYP_CD_SK").isNull() & (F.col("CMLG_MCTR_CALL") != F.lit("NA")),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "svCntctMethCdLkupFailCheck",
        F.when(
            F.col("LOG_CNTCT_METH_CD_SK").isNull() & (F.col("CMLG_METHOD") != F.lit("NA")),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "svCntctLogUserLkupFailCheck",
        F.when(
            F.col("LOG_USER_SK").isNull() & (F.col("CMLG_USID") != F.lit("NA")),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn("DSJobName", F.lit(DSJobName))
)

df_Lnk_CNTCTDtlFkey_Main = df_xfm_CheckLkpResults.select(
    F.col("CASE_MGT_CNTCT_DTL_LOG_SK").alias("CASE_MGT_CNTCT_DTL_LOG_SK"),
    F.when(F.col("CASE_MGT_CNTCT_SK").isNull(), F.lit(0)).otherwise(F.col("CASE_MGT_CNTCT_SK")).alias("CASE_MGT_CNTCT_SK"),
    F.col("CASE_MGT_ID").alias("CASE_MGT_ID"),
    F.col("LOG_SEQ_NO").alias("LOG_SEQ_NO"),
    F.col("CNTCT_SEQ_NO").alias("CNTCT_SEQ_NO"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(F.col("CASE_MGT_SK").isNull(), F.lit(0)).otherwise(F.col("CASE_MGT_SK")).alias("CASE_MGT_SK"),
    F.col("LOG_DTM").alias("LOG_DTM"),
    F.col("LOG_SUM_TX").alias("LOG_SUM_TX"),
    F.when(F.col("LOG_CNTCT_TYP_CD_SK").isNull(), F.lit(0)).otherwise(F.col("LOG_CNTCT_TYP_CD_SK")).alias("LOG_CNTCT_TYP_CD_SK"),
    F.when(F.col("LOG_CNTCT_METH_CD_SK").isNull(), F.lit(0)).otherwise(F.col("LOG_CNTCT_METH_CD_SK")).alias("LOG_CNTCT_METH_CD_SK"),
    F.when(F.col("LOG_USER_SK").isNull(), F.lit(0)).otherwise(F.col("LOG_USER_SK")).alias("LOG_USER_SK")
)

df_Lnk_CMCONTACTDTLUNK_input = df_xfm_CheckLkpResults.limit(1)
df_Lnk_CMCONTACTDTLUNK = df_Lnk_CMCONTACTDTLUNK_input.select(
    F.lit(0).alias("CASE_MGT_CNTCT_DTL_LOG_SK"),
    F.lit(0).alias("CASE_MGT_CNTCT_SK"),
    F.lit("UNK").alias("CASE_MGT_ID"),
    F.lit(0).alias("CNTCT_SEQ_NO"),
    F.lit(0).alias("LOG_SEQ_NO"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("CASE_MGT_SK"),
    F.lit("1753-01-01 00:00:00").alias("LOG_DTM"),
    rpad(F.lit("UNK"), 70, " ").alias("LOG_SUM_TX"),
    F.lit(0).alias("LOG_CNTCT_TYP_CD_SK"),
    F.lit(0).alias("LOG_CNTCT_METH_CD_SK"),
    F.lit(0).alias("LOG_USER_SK")
)

df_Lnk_CMcontactDtl_NA_input = df_xfm_CheckLkpResults.limit(1)
df_Lnk_CMcontactDtl_NA = df_Lnk_CMcontactDtl_NA_input.select(
    F.lit(1).alias("CASE_MGT_CNTCT_DTL_LOG_SK"),
    F.lit(1).alias("CASE_MGT_CNTCT_SK"),
    F.lit("NA").alias("CASE_MGT_ID"),
    F.lit(1).alias("CNTCT_SEQ_NO"),
    F.lit(1).alias("LOG_SEQ_NO"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("CASE_MGT_SK"),
    F.lit("1753-01-01 00:00:00").alias("LOG_DTM"),
    rpad(F.lit("NA"), 70, " ").alias("LOG_SUM_TX"),
    F.lit(1).alias("LOG_CNTCT_TYP_CD_SK"),
    F.lit(1).alias("LOG_CNTCT_METH_CD_SK"),
    F.lit(1).alias("LOG_USER_SK")
)

df_caseMgtFail = df_xfm_CheckLkpResults.filter(F.col("svCaseMgtLkpFailCheck") == F.lit("Y")).select(
    F.col("CASE_MGT_CNTCT_DTL_LOG_SK").alias("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD_1"),
    F.col("DSJobName").alias("JOB_NM"),
    F.lit("FKLOOKUP").alias("ERROR_TYP"),
    F.lit("CASE_MGT").alias("PHYSCL_FILE_NM"),
    F.concat(F.col("SRC_SYS_CD"), F.lit(";"), F.col("CASE_MGT_ID")).alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

df_cntctSeqFail = df_xfm_CheckLkpResults.filter(F.col("svCntctLkpFailCheck") == F.lit("Y")).select(
    F.col("CASE_MGT_CNTCT_DTL_LOG_SK").alias("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD_1"),
    F.col("DSJobName").alias("JOB_NM"),
    F.lit("FKLOOKUP").alias("ERROR_TYP"),
    F.lit("CD_MPPNG").alias("PHYSCL_FILE_NM"),
    F.concat(F.col("SRC_SYS_CD"), F.lit(";"), F.col("CNTCT_SEQ_NO")).alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

df_cntctMethCdFail = df_xfm_CheckLkpResults.filter(F.col("svCntctMethCdLkupFailCheck") == F.lit("Y")).select(
    F.col("CASE_MGT_CNTCT_DTL_LOG_SK").alias("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD_1"),
    F.col("DSJobName").alias("JOB_NM"),
    F.lit("FKLOOKUP").alias("ERROR_TYP"),
    F.lit("CD_MPPNG").alias("PHYSCL_FILE_NM"),
    F.concat(F.col("SRC_SYS_CD"), F.lit(";"), F.col("CMLG_METHOD")).alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

df_cntctLogUserFail = df_xfm_CheckLkpResults.filter(F.col("svCntctLogUserLkupFailCheck") == F.lit("Y")).select(
    F.col("CASE_MGT_CNTCT_DTL_LOG_SK").alias("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD_1"),
    F.col("DSJobName").alias("JOB_NM"),
    F.lit("FKLOOKUP").alias("ERROR_TYP"),
    F.lit("APP_USER").alias("PHYSCL_FILE_NM"),
    F.concat(F.col("SRC_SYS_CD"), F.lit(";"), F.col("CMLG_USID")).alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

df_FnlFkeyFailures = df_caseMgtFail.unionByName(df_cntctSeqFail).unionByName(df_cntctMethCdFail).unionByName(df_cntctLogUserFail)

file_path_seq_FkeyFailedFile = f"{adls_path}/fkey_failures/{PrefixFkeyFailedFileName}.{DSJobName}.dat"
write_files(
    df_FnlFkeyFailures.select(
        rpad(F.col("PRI_SK").cast(StringType()), len("PRI_SK"), " ").alias("PRI_SK"),
        rpad(F.col("PRI_NAT_KEY_STRING").cast(StringType()), len("PRI_NAT_KEY_STRING"), " ").alias("PRI_NAT_KEY_STRING"),
        rpad(F.col("SRC_SYS_CD_1").cast(StringType()), len("SRC_SYS_CD_1"), " ").alias("SRC_SYS_CD_1"),
        rpad(F.col("JOB_NM").cast(StringType()), len("JOB_NM"), " ").alias("JOB_NM"),
        rpad(F.col("ERROR_TYP").cast(StringType()), len("ERROR_TYP"), " ").alias("ERROR_TYP"),
        rpad(F.col("PHYSCL_FILE_NM").cast(StringType()), len("PHYSCL_FILE_NM"), " ").alias("PHYSCL_FILE_NM"),
        rpad(F.col("FRGN_NAT_KEY_STRING").cast(StringType()), len("FRGN_NAT_KEY_STRING"), " ").alias("FRGN_NAT_KEY_STRING"),
        rpad(F.col("FIRST_RECYC_TS").cast(StringType()), len("FIRST_RECYC_TS"), " ").alias("FIRST_RECYC_TS"),
        rpad(F.col("JOB_EXCTN_SK").cast(StringType()), len("JOB_EXCTN_SK"), " ").alias("JOB_EXCTN_SK")
    ),
    file_path_seq_FkeyFailedFile,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

df_fnl_NA_UNK_Streams = df_Lnk_CNTCTDtlFkey_Main.unionByName(df_Lnk_CMCONTACTDTLUNK).unionByName(df_Lnk_CMcontactDtl_NA)

df_fnl_NA_UNK_Streams_sel = df_fnl_NA_UNK_Streams.select(
    F.col("CASE_MGT_CNTCT_DTL_LOG_SK"),
    F.col("CASE_MGT_ID"),
    F.col("CNTCT_SEQ_NO"),
    F.col("LOG_SEQ_NO"),
    F.col("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CASE_MGT_CNTCT_SK"),
    F.col("CASE_MGT_SK"),
    F.col("LOG_USER_SK"),
    F.col("LOG_CNTCT_METH_CD_SK"),
    F.col("LOG_CNTCT_TYP_CD_SK"),
    F.col("LOG_DTM"),
    F.col("LOG_SUM_TX")
)

file_path_CASE_MGT_CONTACT_DTLFKEY = f"{adls_path}/load/CASE_MGT_CNTCT_LOG.{SrcSysCd}.{RunID}.dat"
write_files(
    df_fnl_NA_UNK_Streams_sel.select(
        rpad(F.col("CASE_MGT_CNTCT_DTL_LOG_SK").cast(StringType()), len("CASE_MGT_CNTCT_DTL_LOG_SK"), " ").alias("CASE_MGT_CNTCT_DTL_LOG_SK"),
        rpad(F.col("CASE_MGT_ID").cast(StringType()), len("CASE_MGT_ID"), " ").alias("CASE_MGT_ID"),
        rpad(F.col("CNTCT_SEQ_NO").cast(StringType()), len("CNTCT_SEQ_NO"), " ").alias("CNTCT_SEQ_NO"),
        rpad(F.col("LOG_SEQ_NO").cast(StringType()), len("LOG_SEQ_NO"), " ").alias("LOG_SEQ_NO"),
        rpad(F.col("SRC_SYS_CD").cast(StringType()), len("SRC_SYS_CD"), " ").alias("SRC_SYS_CD"),
        rpad(F.col("CRT_RUN_CYC_EXCTN_SK").cast(StringType()), len("CRT_RUN_CYC_EXCTN_SK"), " ").alias("CRT_RUN_CYC_EXCTN_SK"),
        rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").cast(StringType()), len("LAST_UPDT_RUN_CYC_EXCTN_SK"), " ").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        rpad(F.col("CASE_MGT_CNTCT_SK").cast(StringType()), len("CASE_MGT_CNTCT_SK"), " ").alias("CASE_MGT_CNTCT_SK"),
        rpad(F.col("CASE_MGT_SK").cast(StringType()), len("CASE_MGT_SK"), " ").alias("CASE_MGT_SK"),
        rpad(F.col("LOG_USER_SK").cast(StringType()), len("LOG_USER_SK"), " ").alias("LOG_USER_SK"),
        rpad(F.col("LOG_CNTCT_METH_CD_SK").cast(StringType()), len("LOG_CNTCT_METH_CD_SK"), " ").alias("LOG_CNTCT_METH_CD_SK"),
        rpad(F.col("LOG_CNTCT_TYP_CD_SK").cast(StringType()), len("LOG_CNTCT_TYP_CD_SK"), " ").alias("LOG_CNTCT_TYP_CD_SK"),
        rpad(F.col("LOG_DTM").cast(StringType()), len("LOG_DTM"), " ").alias("LOG_DTM"),
        rpad(F.col("LOG_SUM_TX").cast(StringType()), len("LOG_SUM_TX"), " ").alias("LOG_SUM_TX")
    ),
    file_path_CASE_MGT_CONTACT_DTLFKEY,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)