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
# MAGIC AKASH PARSHA     2019-09-26             US140165               Case Mgt Contact FKey Process        			Jaideep Mankala  10/10/2019

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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


jobName = "IdsCaseMgtContactFkey"

SrcSysCd = get_widget_value("SrcSysCd","")
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
RunID = get_widget_value("RunID","")
PrefixFkeyFailedFileName = get_widget_value("PrefixFkeyFailedFileName","")
RunDateTime = get_widget_value("RunDateTime","")

schema_CASE_MGT_CONTACT_PKEY = StructType([
    StructField("CASE_MGT_ID", StringType(), nullable=False),
    StructField("CNTCT_SEQ_NO", IntegerType(), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("CASE_MGT_CNTCT_SK", IntegerType(), nullable=False),
    StructField("PRI_NAT_KEY_STRING", StringType(), nullable=False),
    StructField("FIRST_RECYC_TS", TimestampType(), nullable=False),
    StructField("CNTCT_FIRST_NM", StringType(), nullable=False),
    StructField("CNTCT_LAST_NM", StringType(), nullable=False),
    StructField("CNTCT_MIDINIT", StringType(), nullable=False),
    StructField("CNTCT_TTL_NM", StringType(), nullable=False),
    StructField("CNTCT_ADDR_LN_1", StringType(), nullable=False),
    StructField("CNTCT_ADDR_LN_2", StringType(), nullable=False),
    StructField("CNTCT_ADDR_LN_3", StringType(), nullable=False),
    StructField("CMCT_CITY", StringType(), nullable=False),
    StructField("CMCT_ZIP", StringType(), nullable=False),
    StructField("CNTCT_CNTY_NM", StringType(), nullable=False),
    StructField("CNTCT_FAX_NO", StringType(), nullable=False),
    StructField("CNTCT_FAX_EXT_NO", StringType(), nullable=False),
    StructField("CNTCT_PHN_NO", StringType(), nullable=False),
    StructField("CNTCT_PHN_EXT_NO", StringType(), nullable=False),
    StructField("CNTCT_EMAIL_ADDR", StringType(), nullable=False),
    StructField("CMCT_CTRY_CD", StringType(), nullable=False),
    StructField("CMCT_STATE", StringType(), nullable=False),
    StructField("CMCT_MTCR_LANG", StringType(), nullable=False)
])

df_CASE_MGT_CONTACT_PKEY = (
    spark.read.format("csv")
    .option("sep", ",")
    .option("header", "false")
    .option("quote", "^")
    .option("nullValue", None)
    .schema(schema_CASE_MGT_CONTACT_PKEY)
    .load(f"{adls_path}/key/CASE_MGT_CNT.{SrcSysCd}.pkey.{RunID}.dat")
)

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
query_LANGUAGE = f"""
Select DISTINCT 
SRC_CD, 
min(CD_MPPNG_SK) as CD_MPPNG_SK 
from {IDSOwner}.CD_MPPNG 
where 
TRGT_DOMAIN_NM = 'LANGUAGE' 
AND TRGT_CLCTN_CD = 'IDS' 
AND SRC_SYS_CD = 'FACETS'
AND SRC_DOMAIN_NM = 'LANGUAGE' 
AND SRC_CLCTN_CD = 'FACETS DBO'
GROUP BY SRC_CD
"""
df_LANGUAGE = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_LANGUAGE)
    .load()
)

query_CNTY_CD_LKP = f"""
Select DISTINCT 
SRC_CD,
min(CD_MPPNG_SK) as CD_MPPNG_SK 
from {IDSOwner}.CD_MPPNG 
where 
TRGT_DOMAIN_NM = 'COUNTRY' 
and TRGT_CLCTN_CD = 'IDS' 
and SRC_SYS_CD = 'FACETS'
AND SRC_DOMAIN_NM = 'COUNTRY' 
AND SRC_CLCTN_CD = 'FACETS DBO'
GROUP BY SRC_CD
"""
df_CNTY_CD_LKP = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_CNTY_CD_LKP)
    .load()
)

query_STATE_CD = f"""
Select DISTINCT 
SRC_CD, 
min(CD_MPPNG_SK) as CD_MPPNG_SK 
from {IDSOwner}.CD_MPPNG 
where 
TRGT_DOMAIN_NM = 'STATE' 
AND TRGT_CLCTN_CD = 'IDS' 
AND SRC_SYS_CD = 'FACETS'
AND SRC_DOMAIN_NM = 'STATE' 
AND SRC_CLCTN_CD = 'FACETS DBO'
GROUP BY SRC_CD
"""
df_STATE_CD = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_STATE_CD)
    .load()
)

query_CASE_MGT = f"""
Select distinct 
CM.CASE_MGT_ID,
CM.CASE_MGT_SK
 from {IDSOwner}.CASE_MGT CM, {IDSOwner}.CD_MPPNG CD
WHERE CD.CD_MPPNG_SK = CM.SRC_SYS_CD_SK 
AND CD.TRGT_CD = 'FACETS'
"""
df_CASE_MGT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_CASE_MGT)
    .load()
)

df_lkp_Code_SKs = (
    df_CASE_MGT_CONTACT_PKEY.alias("lnk_casemgtContactJnData_out")
    .join(
        df_LANGUAGE.alias("Language"),
        F.col("lnk_casemgtContactJnData_out.CMCT_MTCR_LANG") == F.col("Language.SRC_CD"),
        "left"
    )
    .join(
        df_CNTY_CD_LKP.alias("CNTY_SK_MAP"),
        F.col("lnk_casemgtContactJnData_out.CMCT_CTRY_CD") == F.col("CNTY_SK_MAP.SRC_CD"),
        "left"
    )
    .join(
        df_STATE_CD.alias("STATE_SK_MAP"),
        F.col("lnk_casemgtContactJnData_out.CMCT_STATE") == F.col("STATE_SK_MAP.SRC_CD"),
        "left"
    )
    .join(
        df_CASE_MGT.alias("casemgt_lkup"),
        F.col("lnk_casemgtContactJnData_out.CASE_MGT_ID") == F.col("casemgt_lkup.CASE_MGT_ID"),
        "left"
    )
    .select(
        F.col("lnk_casemgtContactJnData_out.CASE_MGT_ID").alias("CASE_MGT_ID"),
        F.col("lnk_casemgtContactJnData_out.CNTCT_SEQ_NO").alias("CNTCT_SEQ_NO"),
        F.col("lnk_casemgtContactJnData_out.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnk_casemgtContactJnData_out.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("lnk_casemgtContactJnData_out.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("lnk_casemgtContactJnData_out.CASE_MGT_CNTCT_SK").alias("CASE_MGT_CNTCT_SK"),
        F.col("lnk_casemgtContactJnData_out.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
        F.col("lnk_casemgtContactJnData_out.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
        F.col("lnk_casemgtContactJnData_out.CNTCT_FIRST_NM").alias("CNTCT_FIRST_NM"),
        F.col("lnk_casemgtContactJnData_out.CNTCT_LAST_NM").alias("CNTCT_LAST_NM"),
        F.col("lnk_casemgtContactJnData_out.CNTCT_MIDINIT").alias("CNTCT_MIDINIT"),
        F.col("lnk_casemgtContactJnData_out.CNTCT_TTL_NM").alias("CNTCT_TTL_NM"),
        F.col("lnk_casemgtContactJnData_out.CNTCT_ADDR_LN_1").alias("CNTCT_ADDR_LN_1"),
        F.col("lnk_casemgtContactJnData_out.CNTCT_ADDR_LN_2").alias("CNTCT_ADDR_LN_2"),
        F.col("lnk_casemgtContactJnData_out.CNTCT_ADDR_LN_3").alias("CNTCT_ADDR_LN_3"),
        F.col("lnk_casemgtContactJnData_out.CMCT_CITY").alias("CMCT_CITY"),
        F.col("lnk_casemgtContactJnData_out.CMCT_ZIP").alias("CMCT_ZIP"),
        F.col("lnk_casemgtContactJnData_out.CNTCT_CNTY_NM").alias("CNTCT_CNTY_NM"),
        F.col("lnk_casemgtContactJnData_out.CNTCT_FAX_NO").alias("CNTCT_FAX_NO"),
        F.col("lnk_casemgtContactJnData_out.CNTCT_FAX_EXT_NO").alias("CNTCT_FAX_EXT_NO"),
        F.col("lnk_casemgtContactJnData_out.CNTCT_PHN_NO").alias("CNTCT_PHN_NO"),
        F.col("lnk_casemgtContactJnData_out.CNTCT_PHN_EXT_NO").alias("CNTCT_PHN_EXT_NO"),
        F.col("lnk_casemgtContactJnData_out.CNTCT_EMAIL_ADDR").alias("CNTCT_EMAIL_ADDR"),
        F.col("lnk_casemgtContactJnData_out.CMCT_CTRY_CD").alias("CMCT_CTRY_CD"),
        F.col("lnk_casemgtContactJnData_out.CMCT_STATE").alias("CMCT_STATE"),
        F.col("lnk_casemgtContactJnData_out.CMCT_MTCR_LANG").alias("CMCT_MTCR_LANG"),
        F.col("casemgt_lkup.CASE_MGT_SK").alias("CASE_MGT_SK"),
        F.col("Language.CD_MPPNG_SK").alias("LANGUAGE_SK"),
        F.col("CNTY_SK_MAP.CD_MPPNG_SK").alias("COUNTRY_SK"),
        F.col("STATE_SK_MAP.CD_MPPNG_SK").alias("STATE_SK")
    )
)

df_xfm_CheckLkpResults = (
    df_lkp_Code_SKs
    .withColumn(
        "svCaseMgtLkpFailCheck",
        F.when(
            (F.col("CASE_MGT_SK").isNull()) & (trim(F.col("CASE_MGT_ID")) != "NA"),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "svLanguageLkpFailCheck",
        F.when(
            (F.col("LANGUAGE_SK").isNull()) & (F.col("CMCT_MTCR_LANG") != "NA"),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "svCountryLkupFailCheck",
        F.when(
            (F.col("COUNTRY_SK").isNull()) & (F.col("CMCT_CTRY_CD") != "NA"),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "svStateLkupFailCheck",
        F.when(
            (F.col("STATE_SK").isNull()) & (F.col("CMCT_STATE") != "NA"),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
)

df_Lnk_CNTCTFkey_Main = df_xfm_CheckLkpResults.select(
    F.col("CASE_MGT_CNTCT_SK").alias("CASE_MGT_CNTCT_SK"),
    F.col("CASE_MGT_ID").alias("CASE_MGT_ID"),
    F.col("CNTCT_SEQ_NO").alias("CNTCT_SEQ_NO"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(F.col("CASE_MGT_SK").isNull(), F.lit(0)).otherwise(F.col("CASE_MGT_SK")).alias("CASE_MGT_SK"),
    F.when(F.col("LANGUAGE_SK").isNull(), F.lit(0)).otherwise(F.col("LANGUAGE_SK")).alias("CNTCT_LANG_CD_SK"),
    F.col("CNTCT_FIRST_NM").alias("CNTCT_FIRST_NM"),
    F.col("CNTCT_LAST_NM").alias("CNTCT_LAST_NM"),
    F.col("CNTCT_MIDINIT").alias("CNTCT_MIDINIT"),
    F.col("CNTCT_TTL_NM").alias("CNTCT_TTL_NM"),
    F.col("CNTCT_ADDR_LN_1").alias("CNTCT_ADDR_LN_1"),
    F.col("CNTCT_ADDR_LN_2").alias("CNTCT_ADDR_LN_2"),
    F.col("CNTCT_ADDR_LN_3").alias("CNTCT_ADDR_LN_3"),
    F.col("CMCT_CITY").alias("CNTCT_CITY_NM"),
    F.when(F.col("STATE_SK").isNull(), F.lit(0)).otherwise(F.col("STATE_SK")).alias("CNTCT_ST_CD_SK"),
    F.col("CMCT_ZIP").alias("CNTCT_ZIP_CD"),
    F.when(F.col("COUNTRY_SK").isNull(), F.lit(0)).otherwise(F.col("COUNTRY_SK")).alias("CNTCT_CTRY_CD_SK"),
    F.col("CNTCT_CNTY_NM").alias("CNTCT_CNTY_NM"),
    F.col("CNTCT_FAX_NO").alias("CNTCT_FAX_NO"),
    F.col("CNTCT_FAX_EXT_NO").alias("CNTCT_FAX_EXT_NO"),
    F.col("CNTCT_PHN_NO").alias("CNTCT_PHN_NO"),
    F.col("CNTCT_PHN_EXT_NO").alias("CNTCT_PHN_EXT_NO"),
    F.col("CNTCT_EMAIL_ADDR").alias("CNTCT_EMAIL_ADDR")
)

df_tmp_CMCONTACTUNK = df_xfm_CheckLkpResults.limit(1)
df_Lnk_CMCONTACTUNK = df_tmp_CMCONTACTUNK.select(
    F.lit(0).alias("CASE_MGT_CNTCT_SK"),
    F.lit("UNK").alias("CASE_MGT_ID"),
    F.lit(0).alias("CNTCT_SEQ_NO"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("CASE_MGT_SK"),
    F.lit(0).alias("CNTCT_LANG_CD_SK"),
    F.lit("UNK").alias("CNTCT_FIRST_NM"),
    F.lit("UNK").alias("CNTCT_LAST_NM"),
    F.lit("U").alias("CNTCT_MIDINIT"),
    F.lit("UNK").alias("CNTCT_TTL_NM"),
    F.lit("UNK").alias("CNTCT_ADDR_LN_1"),
    F.lit("UNK").alias("CNTCT_ADDR_LN_2"),
    F.lit("UNK").alias("CNTCT_ADDR_LN_3"),
    F.lit("UNK").alias("CNTCT_CITY_NM"),
    F.lit(0).alias("CNTCT_ST_CD_SK"),
    F.lit("UNK").alias("CNTCT_ZIP_CD"),
    F.lit(0).alias("CNTCT_CTRY_CD_SK"),
    F.lit("UNK").alias("CNTCT_CNTY_NM"),
    F.lit("UNK").alias("CNTCT_FAX_NO"),
    F.lit("UNK").alias("CNTCT_FAX_EXT_NO"),
    F.lit("UNK").alias("CNTCT_PHN_NO"),
    F.lit("UNK").alias("CNTCT_PHN_EXT_NO"),
    F.lit("UNK").alias("CNTCT_EMAIL_ADDR")
)

df_tmp_CMcontact_NA = df_xfm_CheckLkpResults.limit(1)
df_Lnk_CMcontact_NA = df_tmp_CMcontact_NA.select(
    F.lit(1).alias("CASE_MGT_CNTCT_SK"),
    F.lit("NA").alias("CASE_MGT_ID"),
    F.lit(1).alias("CNTCT_SEQ_NO"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("CASE_MGT_SK"),
    F.lit(1).alias("CNTCT_LANG_CD_SK"),
    F.lit("NA").alias("CNTCT_FIRST_NM"),
    F.lit("NA").alias("CNTCT_LAST_NM"),
    F.lit("NA").alias("CNTCT_MIDINIT"),
    F.lit("NA").alias("CNTCT_TTL_NM"),
    F.lit("NA").alias("CNTCT_ADDR_LN_1"),
    F.lit("NA").alias("CNTCT_ADDR_LN_2"),
    F.lit("NA").alias("CNTCT_ADDR_LN_3"),
    F.lit("NA").alias("CNTCT_CITY_NM"),
    F.lit(1).alias("CNTCT_ST_CD_SK"),
    F.lit("NA").alias("CNTCT_ZIP_CD"),
    F.lit(1).alias("CNTCT_CTRY_CD_SK"),
    F.lit("NA").alias("CNTCT_CNTY_NM"),
    F.lit("NA").alias("CNTCT_FAX_NO"),
    F.lit("NA").alias("CNTCT_FAX_EXT_NO"),
    F.lit("NA").alias("CNTCT_PHN_NO"),
    F.lit("NA").alias("CNTCT_PHN_EXT_NO"),
    F.lit("NA").alias("CNTCT_EMAIL_ADDR")
)

common_cols_funnel_main = [
    "CASE_MGT_CNTCT_SK","CASE_MGT_ID","CNTCT_SEQ_NO","SRC_SYS_CD","CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CASE_MGT_SK","CNTCT_LANG_CD_SK","CNTCT_FIRST_NM","CNTCT_LAST_NM","CNTCT_MIDINIT","CNTCT_TTL_NM",
    "CNTCT_ADDR_LN_1","CNTCT_ADDR_LN_2","CNTCT_ADDR_LN_3","CNTCT_CITY_NM","CNTCT_ST_CD_SK","CNTCT_ZIP_CD",
    "CNTCT_CTRY_CD_SK","CNTCT_CNTY_NM","CNTCT_FAX_NO","CNTCT_FAX_EXT_NO","CNTCT_PHN_NO","CNTCT_PHN_EXT_NO",
    "CNTCT_EMAIL_ADDR"
]

df_fnl_NA_UNK_Streams = (
    df_Lnk_CNTCTFkey_Main.select(common_cols_funnel_main)
    .unionByName(df_Lnk_CMCONTACTUNK.select(common_cols_funnel_main))
    .unionByName(df_Lnk_CMcontact_NA.select(common_cols_funnel_main))
)

# rpad for CNTCT_MIDINIT which is char(1)
df_fnl_NA_UNK_Streams = df_fnl_NA_UNK_Streams.withColumn(
    "CNTCT_MIDINIT", F.rpad(F.col("CNTCT_MIDINIT"), 1, " ")
)

write_files(
    df_fnl_NA_UNK_Streams.select(common_cols_funnel_main),
    f"{adls_path}/load/CASE_MGT_CNTCT.{SrcSysCd}.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

df_LNk_CaseMgt_Fail = (
    df_xfm_CheckLkpResults
    .filter(F.col("svCaseMgtLkpFailCheck") == "Y")
    .select(
        F.col("CASE_MGT_CNTCT_SK").alias("PRI_SK"),
        F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD_1"),
        F.lit(jobName).alias("JOB_NM"),
        F.lit("FKLOOKUP").alias("ERROR_TYP"),
        F.lit("CASE_MGT").alias("PHYSCL_FILE_NM"),
        F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("CASE_MGT_ID")).alias("FRGN_NAT_KEY_STRING"),
        F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
    )
)

df_Lnk_CaseCNTRY_Fail = (
    df_xfm_CheckLkpResults
    .filter(F.col("svCountryLkupFailCheck") == "Y")
    .select(
        F.col("CASE_MGT_CNTCT_SK").alias("PRI_SK"),
        F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD_1"),
        F.lit(jobName).alias("JOB_NM"),
        F.lit("FKLOOKUP").alias("ERROR_TYP"),
        F.lit("CD_MPPNG").alias("PHYSCL_FILE_NM"),
        F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("CMCT_CTRY_CD")).alias("FRGN_NAT_KEY_STRING"),
        F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
    )
)

df_Lnk_CaseSTATE_Fail = (
    df_xfm_CheckLkpResults
    .filter(F.col("svStateLkupFailCheck") == "Y")
    .select(
        F.col("CASE_MGT_CNTCT_SK").alias("PRI_SK"),
        F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD_1"),
        F.lit(jobName).alias("JOB_NM"),
        F.lit("FKLOOKUP").alias("ERROR_TYP"),
        F.lit("CD_MPPNG").alias("PHYSCL_FILE_NM"),
        F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("CMCT_STATE")).alias("FRGN_NAT_KEY_STRING"),
        F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
    )
)

df_Lnk_LANG_Fail = (
    df_xfm_CheckLkpResults
    .filter(F.col("svLanguageLkpFailCheck") == "Y")
    .select(
        F.col("CASE_MGT_CNTCT_SK").alias("PRI_SK"),
        F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD_1"),
        F.lit(jobName).alias("JOB_NM"),
        F.lit("FKLOOKUP").alias("ERROR_TYP"),
        F.lit("APP_USER").alias("PHYSCL_FILE_NM"),
        F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("CMCT_MTCR_LANG")).alias("FRGN_NAT_KEY_STRING"),
        F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
    )
)

common_cols_fkeyfail = [
    "PRI_SK","PRI_NAT_KEY_STRING","SRC_SYS_CD_1","JOB_NM","ERROR_TYP",
    "PHYSCL_FILE_NM","FRGN_NAT_KEY_STRING","FIRST_RECYC_TS","JOB_EXCTN_SK"
]

df_FnlFkeyFailures = (
    df_Lnk_CaseSTATE_Fail.select(common_cols_fkeyfail)
    .unionByName(df_Lnk_CaseMgt_Fail.select(common_cols_fkeyfail))
    .unionByName(df_Lnk_CaseCNTRY_Fail.select(common_cols_fkeyfail))
    .unionByName(df_Lnk_LANG_Fail.select(common_cols_fkeyfail))
)

write_files(
    df_FnlFkeyFailures.select(common_cols_fkeyfail),
    f"{adls_path}/fkey_failures/{PrefixFkeyFailedFileName}.{jobName}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)