# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Called By: FctsMedMgtCntl
# MAGIC 
# MAGIC 
# MAGIC Process :  Case Mgt Status Fkey Process  which is processed and loaded to CASE_MGT_STTUS table
# MAGIC 
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                              		  Project/                                                                                                    	Code                  Date
# MAGIC Developer         	Date             	 Altiris #     	Change Description                                                   	Reviewer            Reviewed
# MAGIC -----------------------  	-------------------   	-------------   	-----------------------------------------------------------------------------   	-------------------------  -------------------
# MAGIC Jaideep Mankala         2019-07-09          US115013               Case Mgt Status FKey Process        			Abhiram Dasarathy	2019-07-15

# MAGIC FKEY failures are written into this flat file.
# MAGIC Code lookups operation to get the needed FKEY values
# MAGIC This is a load ready file that will go into Load job
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame, functions as F, types as T
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


DSJobName = "IdsCaseMgtStatusFkey"
SrcSysCd = get_widget_value("SrcSysCd","")
IDSOwner = get_widget_value("IDSOwner","")
RunID = get_widget_value("RunID","")
PrefixFkeyFailedFileName = get_widget_value("PrefixFkeyFailedFileName","")
RunDateTime = get_widget_value("RunDateTime","")
ids_secret_name = get_widget_value("ids_secret_name","")

schema_seq_CMST = StructType([
    StructField("PRI_NAT_KEY_STRING", StringType(), nullable=False),
    StructField("FIRST_RECYC_TS", TimestampType(), nullable=False),
    StructField("CASE_MGT_ID", StringType(), nullable=False),
    StructField("CASE_MGT_STTUS_SEQ_NO", IntegerType(), nullable=False),
    StructField("SRC_SYS_CD_1", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("CASE_MGT_STTUS_SK", IntegerType(), nullable=False),
    StructField("USUS_ID", StringType(), nullable=False),
    StructField("CMST_STS", StringType(), nullable=False),
    StructField("CMST_STS_DTM", TimestampType(), nullable=False),
    StructField("CMST_MCTR_REAS", StringType(), nullable=False),
    StructField("CMST_USID_ROUTE", StringType(), nullable=False)
])

df_seq_CMST = (
    spark.read
    .option("sep", ",")
    .option("quote", "^")
    .option("header", False)
    .option("nullValue", None)
    .schema(schema_seq_CMST)
    .csv(f"{adls_path}/key/CASE_MGT_STTUS.{SrcSysCd}.pkey.{RunID}.dat")
)

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query_CASE_MGT = f"""Select distinct 
CM.CASE_MGT_ID,
CM.CASE_MGT_SK
 from {IDSOwner}.CASE_MGT CM, {IDSOwner}.CD_MPPNG CD
WHERE CD.CD_MPPNG_SK = CM.SRC_SYS_CD_SK 
AND CD.TRGT_CD = 'FACETS'"""
df_CASE_MGT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_CASE_MGT)
    .load()
)

extract_query_CASE_STTS = f"""Select DISTINCT 
SRC_CD, 
CD_MPPNG_SK 
from {IDSOwner}.CD_MPPNG 
where 
TRGT_DOMAIN_NM = 'CASE MANAGEMENT STATUS' 
and TRGT_CLCTN_CD = 'IDS' 
and SRC_SYS_CD = 'FACETS'"""
df_CASE_STTS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_CASE_STTS)
    .load()
)

extract_query_CASE_REASON = f"""Select DISTINCT 
SRC_CD, 
CD_MPPNG_SK 
from {IDSOwner}.CD_MPPNG 
where 
TRGT_DOMAIN_NM = 'CASE MANAGEMENT STATUS REASON' 
and TRGT_CLCTN_CD = 'IDS' 
and SRC_SYS_CD = 'FACETS'
AND SRC_DOMAIN_NM = 'CASE MANAGEMENT STATUS REASON' 
AND SRC_CLCTN_CD = 'FACETS DBO'"""
df_CASE_REASON = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_CASE_REASON)
    .load()
)

extract_query_APP_USER = f"""Select distinct 
USER_SK,
USER_ID
 from {IDSOwner}.APP_USER
WHERE USER_SK NOT IN (0,1)"""
df_APP_USER = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_APP_USER)
    .load()
)

df_user_lkup = df_APP_USER.select(
    F.rpad(F.col("USER_ID"), 10, " ").alias("USER_ID"),
    F.col("USER_SK").alias("USER_SK")
)

df_route_lkup = df_APP_USER.select(
    F.rpad(F.col("USER_ID"), 10, " ").alias("USER_ID"),
    F.col("USER_SK").alias("USER_SK")
)

df_seq_CMST_aug = df_seq_CMST.withColumn("PRI_CASE_MGR_USER_SK", F.lit(None).cast(IntegerType()))

df_lkp_Code_SKs = (
    df_seq_CMST_aug.alias("Lnk_IdsCMSTFkey_LKp")
    .join(
        df_user_lkup.alias("user_lkup"),
        (
            (F.col("Lnk_IdsCMSTFkey_LKp.USUS_ID") == F.col("user_lkup.USER_ID"))
            & (F.col("Lnk_IdsCMSTFkey_LKp.PRI_CASE_MGR_USER_SK") == F.col("user_lkup.USER_SK"))
        ),
        "left"
    )
    .join(
        df_route_lkup.alias("Route_lkup"),
        (
            (F.col("Lnk_IdsCMSTFkey_LKp.CMST_USID_ROUTE") == F.col("Route_lkup.USER_ID"))
            & (F.col("Lnk_IdsCMSTFkey_LKp.PRI_CASE_MGR_USER_SK") == F.col("Route_lkup.USER_SK"))
        ),
        "left"
    )
    .join(
        df_CASE_MGT.alias("casemgt_lkup"),
        F.col("Lnk_IdsCMSTFkey_LKp.CASE_MGT_ID") == F.col("casemgt_lkup.CASE_MGT_ID"),
        "left"
    )
    .join(
        df_CASE_STTS.alias("sttus_lkup"),
        F.col("Lnk_IdsCMSTFkey_LKp.CMST_STS") == F.col("sttus_lkup.SRC_CD"),
        "left"
    )
    .join(
        df_CASE_REASON.alias("reason_lkup"),
        F.col("Lnk_IdsCMSTFkey_LKp.CMST_MCTR_REAS") == F.col("reason_lkup.SRC_CD"),
        "left"
    )
    .select(
        F.col("Lnk_IdsCMSTFkey_LKp.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
        F.col("Lnk_IdsCMSTFkey_LKp.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
        F.col("Lnk_IdsCMSTFkey_LKp.CASE_MGT_ID").alias("CASE_MGT_ID"),
        F.col("Lnk_IdsCMSTFkey_LKp.CASE_MGT_STTUS_SEQ_NO").alias("CASE_MGT_STTUS_SEQ_NO"),
        F.col("Lnk_IdsCMSTFkey_LKp.SRC_SYS_CD_1").alias("SRC_SYS_CD_1"),
        F.col("Lnk_IdsCMSTFkey_LKp.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("Lnk_IdsCMSTFkey_LKp.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("Lnk_IdsCMSTFkey_LKp.CASE_MGT_STTUS_SK").alias("CASE_MGT_STTUS_SK"),
        F.col("Lnk_IdsCMSTFkey_LKp.USUS_ID").alias("USUS_ID"),
        F.col("Lnk_IdsCMSTFkey_LKp.CMST_STS").alias("CMST_STS"),
        F.col("Lnk_IdsCMSTFkey_LKp.CMST_STS_DTM").alias("CMST_STS_DTM"),
        F.col("Lnk_IdsCMSTFkey_LKp.CMST_MCTR_REAS").alias("CMST_MCTR_REAS"),
        F.col("Lnk_IdsCMSTFkey_LKp.CMST_USID_ROUTE").alias("CMST_USID_ROUTE"),
        F.col("user_lkup.USER_SK").alias("USUS_SK"),
        F.col("Route_lkup.USER_SK").alias("ROUTE_SK"),
        F.col("casemgt_lkup.CASE_MGT_SK").alias("CASE_MGT_SK"),
        F.col("sttus_lkup.CD_MPPNG_SK").alias("CM_STTS_CD_SK"),
        F.col("reason_lkup.CD_MPPNG_SK").alias("CM_REASON_CD_SK")
    )
)

df_xfm = (
    df_lkp_Code_SKs
    .withColumn(
        "svCaseMgtLkpFailCheck",
        F.when(
            F.col("CASE_MGT_SK").isNull() & (trim(F.col("CASE_MGT_ID")) != F.lit("NA")),
            "Y"
        ).otherwise("N")
    )
    .withColumn(
        "svCaseStsCdFailCheck",
        F.when(
            F.col("CM_STTS_CD_SK").isNull() & (F.col("CMST_STS") != F.lit("NA")),
            "Y"
        ).otherwise("N")
    )
    .withColumn(
        "svCaseReasnLkpFailCheck",
        F.when(
            F.col("CM_REASON_CD_SK").isNull() & (F.col("CMST_MCTR_REAS") != F.lit("NA")),
            "Y"
        ).otherwise("N")
    )
    .withColumn(
        "svUsUsLkupFailCheck",
        F.when(
            F.col("USUS_SK").isNull() & (F.col("USUS_ID") != F.lit("NA")),
            "Y"
        ).otherwise("N")
    )
    .withColumn(
        "svRouteLkupFailCheck",
        F.when(
            F.col("ROUTE_SK").isNull() & (F.col("CMST_USID_ROUTE") != F.lit("NA")),
            "Y"
        ).otherwise("N")
    )
)

df_Lnk_SttsFkey_Main = df_xfm.select(
    F.col("CASE_MGT_STTUS_SK").alias("CASE_MGT_STTUS_SK"),
    F.col("CASE_MGT_ID").alias("CASE_MGT_ID"),
    F.col("CASE_MGT_STTUS_SEQ_NO").alias("CASE_MGT_STTUS_SEQ_NO"),
    F.col("SRC_SYS_CD_1").alias("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(F.col("CASE_MGT_SK").isNull(), F.lit(0)).otherwise(F.col("CASE_MGT_SK")).alias("CASE_MGT_SK"),
    F.when(F.col("USUS_SK").isNull(), F.lit(0)).otherwise(F.col("USUS_SK")).alias("CRT_BY_USER_SK"),
    F.when(F.col("ROUTE_SK").isNull(), F.lit(1)).otherwise(F.col("ROUTE_SK")).alias("RTE_TO_USER_SK"),
    F.when(F.col("CM_STTS_CD_SK").isNull(), F.lit(0)).otherwise(F.col("CM_STTS_CD_SK")).alias("CASE_MGT_STTUS_CD_SK"),
    F.when(F.col("CM_REASON_CD_SK").isNull(), F.lit(0)).otherwise(F.col("CM_REASON_CD_SK")).alias("CASE_MGT_STTUS_RSN_CD_SK"),
    F.col("CMST_STS_DTM").alias("CASE_MGT_STTUS_DTM")
)

df_Lnk_CMSttsUNK_src = df_xfm.limit(1)
df_Lnk_CMSttsUNK = df_Lnk_CMSttsUNK_src.select(
    F.lit(0).alias("CASE_MGT_STTUS_SK"),
    F.lit("UNK").alias("CASE_MGT_ID"),
    F.lit(0).alias("CASE_MGT_STTUS_SEQ_NO"),
    F.col("SRC_SYS_CD_1").alias("SRC_SYS_CD"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("CASE_MGT_SK"),
    F.lit(0).alias("CRT_BY_USER_SK"),
    F.lit(0).alias("RTE_TO_USER_SK"),
    F.lit(0).alias("CASE_MGT_STTUS_CD_SK"),
    F.lit(0).alias("CASE_MGT_STTUS_RSN_CD_SK"),
    F.lit(RunDateTime).alias("CASE_MGT_STTUS_DTM")
)

df_Lnk_CMstts_NA_src = df_xfm.limit(1)
df_Lnk_CMstts_NA = df_Lnk_CMstts_NA_src.select(
    F.lit(1).alias("CASE_MGT_STTUS_SK"),
    F.lit("NA").alias("CASE_MGT_ID"),
    F.lit(1).alias("CASE_MGT_STTUS_SEQ_NO"),
    F.col("SRC_SYS_CD_1").alias("SRC_SYS_CD"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("CASE_MGT_SK"),
    F.lit(1).alias("CRT_BY_USER_SK"),
    F.lit(1).alias("RTE_TO_USER_SK"),
    F.lit(1).alias("CASE_MGT_STTUS_CD_SK"),
    F.lit(1).alias("CASE_MGT_STTUS_RSN_CD_SK"),
    F.lit(RunDateTime).alias("CASE_MGT_STTUS_DTM")
)

df_fnl_NA_UNK_Streams = (
    df_Lnk_SttsFkey_Main
    .unionByName(df_Lnk_CMSttsUNK)
    .unionByName(df_Lnk_CMstts_NA)
)

df_fnl_NA_UNK_Streams_rpad = df_fnl_NA_UNK_Streams.select(
    F.col("CASE_MGT_STTUS_SK"),
    F.rpad(F.col("CASE_MGT_ID"), 255, " ").alias("CASE_MGT_ID"),
    F.col("CASE_MGT_STTUS_SEQ_NO"),
    F.rpad(F.col("SRC_SYS_CD"), 255, " ").alias("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CASE_MGT_SK"),
    F.col("CRT_BY_USER_SK"),
    F.col("RTE_TO_USER_SK"),
    F.col("CASE_MGT_STTUS_CD_SK"),
    F.col("CASE_MGT_STTUS_RSN_CD_SK"),
    F.col("CASE_MGT_STTUS_DTM")
)

write_files(
    df_fnl_NA_UNK_Streams_rpad,
    f"{adls_path}/load/CASE_MGT_STTUS.{SrcSysCd}.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

df_CaseMgt_Fail = df_xfm.filter(F.col("svCaseMgtLkpFailCheck") == "Y").select(
    F.col("CASE_MGT_STTUS_SK").alias("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD_1").alias("SRC_SYS_CD_1"),
    F.lit(DSJobName).alias("JOB_NM"),
    F.lit("FKLOOKUP").alias("ERROR_TYP"),
    F.lit("CASE_MGT").alias("PHYSCL_FILE_NM"),
    F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("CASE_MGT_ID")).alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

df_CaseStts_Fail = df_xfm.filter(F.col("svCaseStsCdFailCheck") == "Y").select(
    F.col("CASE_MGT_STTUS_SK").alias("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD_1").alias("SRC_SYS_CD_1"),
    F.lit(DSJobName).alias("JOB_NM"),
    F.lit("FKLOOKUP").alias("ERROR_TYP"),
    F.lit("CD_MPPNG").alias("PHYSCL_FILE_NM"),
    F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("CMST_STS")).alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

df_CaseReasn_Fail = df_xfm.filter(F.col("svCaseReasnLkpFailCheck") == "Y").select(
    F.col("CASE_MGT_STTUS_SK").alias("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD_1").alias("SRC_SYS_CD_1"),
    F.lit(DSJobName).alias("JOB_NM"),
    F.lit("FKLOOKUP").alias("ERROR_TYP"),
    F.lit("CD_MPPNG").alias("PHYSCL_FILE_NM"),
    F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("CMST_MCTR_REAS")).alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

df_UsUs_Fail = df_xfm.filter(F.col("svUsUsLkupFailCheck") == "Y").select(
    F.col("CASE_MGT_STTUS_SK").alias("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD_1").alias("SRC_SYS_CD_1"),
    F.lit(DSJobName).alias("JOB_NM"),
    F.lit("FKLOOKUP").alias("ERROR_TYP"),
    F.lit("APP_USER").alias("PHYSCL_FILE_NM"),
    F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("USUS_ID")).alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

df_FnlFkeyFailures = (
    df_CaseReasn_Fail
    .unionByName(df_CaseMgt_Fail)
    .unionByName(df_CaseStts_Fail)
    .unionByName(df_UsUs_Fail)
)

df_FnlFkeyFailures_rpad = df_FnlFkeyFailures.select(
    F.col("PRI_SK"),
    F.rpad(F.col("PRI_NAT_KEY_STRING"), 255, " ").alias("PRI_NAT_KEY_STRING"),
    F.rpad(F.col("SRC_SYS_CD_1"), 255, " ").alias("SRC_SYS_CD_1"),
    F.rpad(F.col("JOB_NM"), 255, " ").alias("JOB_NM"),
    F.rpad(F.col("ERROR_TYP"), 255, " ").alias("ERROR_TYP"),
    F.rpad(F.col("PHYSCL_FILE_NM"), 255, " ").alias("PHYSCL_FILE_NM"),
    F.rpad(F.col("FRGN_NAT_KEY_STRING"), 255, " ").alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS"),
    F.col("JOB_EXCTN_SK")
)

write_files(
    df_FnlFkeyFailures_rpad,
    f"{adls_path}/fkey_failures/{PrefixFkeyFailedFileName}.{DSJobName}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)