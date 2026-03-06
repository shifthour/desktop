# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC 
# MAGIC Bhoomi Dasari        2014-04-18               5345                             Original Programming                                                                             IntegrateWrhsDevl      Sharon Andrew       2014-10-21

# MAGIC Job name: IdsPrmRateFkey_EE
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
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType
)
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')
PrefixFkeyFailedFileName = get_widget_value('PrefixFkeyFailedFileName','')

# --------------------------------------------------------------------------------
# Stage: db2_CLNDR_DT_EffDt_Lkp (DB2ConnectorPX) -> df_db2_CLNDR_DT_EffDt_Lkp
# --------------------------------------------------------------------------------
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
query_db2_CLNDR_DT_EffDt_Lkp = f"SELECT DISTINCT CLNDR_DT_SK FROM {IDSOwner}.CLNDR_DT WHERE CLNDR_DT_SK NOT IN ('INVALID','NA','NULL','UNK')"
df_db2_CLNDR_DT_EffDt_Lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_db2_CLNDR_DT_EffDt_Lkp)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: db2_CLNDR_DT_TermDt_Lkp (DB2ConnectorPX) -> df_db2_CLNDR_DT_TermDt_Lkp
# --------------------------------------------------------------------------------
query_db2_CLNDR_DT_TermDt_Lkp = f"SELECT DISTINCT CLNDR_DT_SK FROM {IDSOwner}.CLNDR_DT WHERE CLNDR_DT_SK NOT IN ('INVALID','NA','NULL','UNK')"
df_db2_CLNDR_DT_TermDt_Lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_db2_CLNDR_DT_TermDt_Lkp)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: seq_PRM_RATE_Pkey (PxSequentialFile) -> df_seq_PRM_RATE_Pkey
# Reading file: #$FilePath#/key/PRM_RATE.#SrcSysCd#.pkey.#RunID#.dat
# --------------------------------------------------------------------------------
schema_seq_PRM_RATE_Pkey = StructType([
    StructField("PRM_RATE_SK", IntegerType(), False),
    StructField("PRI_NAT_KEY_STRING", StringType(), False),
    StructField("FIRST_RECYC_TS", TimestampType(), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRM_RATE_UNIQ_KEY", IntegerType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("SRC_SYS_CD_SK", IntegerType(), False),
    StructField("PRM_RATE_AGE_ENTY_CD", StringType(), False),
    StructField("PRM_RATE_PRM_TYP_CD", StringType(), False),
    StructField("EFF_DT", StringType(), False),
    StructField("TERM_DT", StringType(), False),
    StructField("PRM_RATE_PFX", StringType(), False),
    StructField("PRM_RATE_DESC", StringType(), True),
    StructField("PRM_RATE_MOD_ID", StringType(), False),
    StructField("MAX_CHLDRN_RATED_NO", IntegerType(), False),
    StructField("PRM_RATE_BSS_NO", IntegerType(), False),
    StructField("PRM_RATE_KEY_STRCT_NO", IntegerType(), False),
    StructField("PRM_RATE_METH", StringType(), False),
    StructField("PRM_RATE_COL_STRUCT", StringType(), False),
    StructField("PRM_RATE_MOD_TYPE", StringType(), False),
    StructField("TIER_AGE_BAND_FROM_NO", IntegerType(), False),
    StructField("TIER_AGE_BAND_THRU_NO", IntegerType(), False)
])
file_path_seq_PRM_RATE_Pkey = f"{adls_path}/key/PRM_RATE.{SrcSysCd}.{RunID}.dat"
df_seq_PRM_RATE_Pkey = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "^")
    .option("escape", "^")
    .schema(schema_seq_PRM_RATE_Pkey)
    .load(file_path_seq_PRM_RATE_Pkey)
)

# --------------------------------------------------------------------------------
# Stage: ds_CD_MPPNG_LkpData (PxDataSet) -> df_ds_CD_MPPNG_LkpData
# Reading dataset => use parquet read at path: #$FilePath#/ds/CD_MPPNG.parquet
# --------------------------------------------------------------------------------
file_path_ds_CD_MPPNG_LkpData = f"{adls_path}/ds/CD_MPPNG.parquet"
df_ds_CD_MPPNG_LkpData = spark.read.parquet(file_path_ds_CD_MPPNG_LkpData)

# --------------------------------------------------------------------------------
# Stage: fltr_CdMppngData (PxFilter) -> multiple outputs
# Filter conditions -> produce 5 DataFrames
# --------------------------------------------------------------------------------
condition_0 = (
    (F.col("SRC_SYS_CD") == "FACETS") &
    (F.col("SRC_CLCTN_CD") == "FACETS DBO") &
    (F.col("TRGT_CLCTN_CD") == "IDS") &
    (F.col("SRC_DOMAIN_NM") == "PREMIUM RATE AGE ENTITY") &
    (F.col("TRGT_DOMAIN_NM") == "PREMIUM RATE AGE ENTITY")
)
df_fltr_CdMppngData_lnkAgeLkpData = df_ds_CD_MPPNG_LkpData.filter(condition_0).select(
    F.col("SRC_CD").alias("SRC_CD"),
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
)

condition_1 = (
    (F.col("SRC_SYS_CD") == "FACETS") &
    (F.col("SRC_CLCTN_CD") == "FACETS DBO") &
    (F.col("TRGT_CLCTN_CD") == "IDS") &
    (F.col("SRC_DOMAIN_NM") == "PREMIUM RATE PREMIUM TYPE") &
    (F.col("TRGT_DOMAIN_NM") == "PREMIUM RATE PREMIUM TYPE")
)
df_fltr_CdMppngData_lnkPrmTypLkpData = df_ds_CD_MPPNG_LkpData.filter(condition_1).select(
    F.col("SRC_CD").alias("SRC_CD"),
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
)

condition_2 = (
    (F.col("SRC_SYS_CD") == "FACETS") &
    (F.col("SRC_CLCTN_CD") == "FACETS DBO") &
    (F.col("TRGT_CLCTN_CD") == "IDS") &
    (F.col("SRC_DOMAIN_NM") == "PREMIUM RATE METHOD") &
    (F.col("TRGT_DOMAIN_NM") == "PREMIUM RATE METHOD")
)
df_fltr_CdMppngData_lnkMethCdLkpData = df_ds_CD_MPPNG_LkpData.filter(condition_2).select(
    F.col("SRC_CD").alias("SRC_CD"),
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
)

condition_3 = (
    (F.col("SRC_SYS_CD") == "FACETS") &
    (F.col("SRC_CLCTN_CD") == "FACETS DBO") &
    (F.col("TRGT_CLCTN_CD") == "IDS") &
    (F.col("SRC_DOMAIN_NM") == "PREMIUM RATE TIER COLUMN STRUCTURE") &
    (F.col("TRGT_DOMAIN_NM") == "PREMIUM RATE TIER COLUMN STRUCTURE")
)
df_fltr_CdMppngData_lnkTierClmnLkpData = df_ds_CD_MPPNG_LkpData.filter(condition_3).select(
    F.col("SRC_CD").alias("SRC_CD"),
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
)

condition_4 = (
    (F.col("SRC_SYS_CD") == "FACETS") &
    (F.col("SRC_CLCTN_CD") == "FACETS DBO") &
    (F.col("TRGT_CLCTN_CD") == "IDS") &
    (F.col("SRC_DOMAIN_NM") == "PREMIUM RATE TIER MODIFIER TYPE") &
    (F.col("TRGT_DOMAIN_NM") == "PREMIUM RATE TIER MODIFIER TYPE")
)
df_fltr_CdMppngData_lnkTierModLkpData = df_ds_CD_MPPNG_LkpData.filter(condition_4).select(
    F.col("SRC_CD").alias("SRC_CD"),
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
)

# --------------------------------------------------------------------------------
# Stage: lkp_Code_SKs (PxLookup) -> df_lkp_Code_SKs
# Primary link: df_seq_PRM_RATE_Pkey
# Multiple left joins
# --------------------------------------------------------------------------------
df_lkp_Code_SKs = (
    df_seq_PRM_RATE_Pkey.alias("lnk_IdsPrmRateFkey_EE_InAbc")
    .join(
        df_fltr_CdMppngData_lnkAgeLkpData.alias("lnkAgeLkpData"),
        F.col("lnk_IdsPrmRateFkey_EE_InAbc.PRM_RATE_AGE_ENTY_CD") == F.col("lnkAgeLkpData.SRC_CD"),
        "left"
    )
    .join(
        df_fltr_CdMppngData_lnkPrmTypLkpData.alias("lnkPrmTypLkpData"),
        F.col("lnk_IdsPrmRateFkey_EE_InAbc.PRM_RATE_PRM_TYP_CD") == F.col("lnkPrmTypLkpData.SRC_CD"),
        "left"
    )
    .join(
        df_fltr_CdMppngData_lnkMethCdLkpData.alias("lnkMethCdLkpData"),
        F.col("lnk_IdsPrmRateFkey_EE_InAbc.PRM_RATE_METH") == F.col("lnkMethCdLkpData.SRC_CD"),
        "left"
    )
    .join(
        df_fltr_CdMppngData_lnkTierClmnLkpData.alias("lnkTierClmnLkpData"),
        F.col("lnk_IdsPrmRateFkey_EE_InAbc.PRM_RATE_COL_STRUCT") == F.col("lnkTierClmnLkpData.SRC_CD"),
        "left"
    )
    .join(
        df_fltr_CdMppngData_lnkTierModLkpData.alias("lnkTierModLkpData"),
        F.col("lnk_IdsPrmRateFkey_EE_InAbc.PRM_RATE_MOD_TYPE") == F.col("lnkTierModLkpData.SRC_CD"),
        "left"
    )
    .join(
        df_db2_CLNDR_DT_TermDt_Lkp.alias("Ref_TermDtSk"),
        F.col("lnk_IdsPrmRateFkey_EE_InAbc.TERM_DT") == F.col("Ref_TermDtSk.CLNDR_DT_SK"),
        "left"
    )
    .join(
        df_db2_CLNDR_DT_EffDt_Lkp.alias("Ref_EffDtSk"),
        F.col("lnk_IdsPrmRateFkey_EE_InAbc.EFF_DT") == F.col("Ref_EffDtSk.CLNDR_DT_SK"),
        "left"
    )
    .select(
        F.col("lnk_IdsPrmRateFkey_EE_InAbc.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
        F.col("lnk_IdsPrmRateFkey_EE_InAbc.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
        F.col("lnk_IdsPrmRateFkey_EE_InAbc.PRM_RATE_SK").alias("PRM_RATE_SK"),
        F.col("lnk_IdsPrmRateFkey_EE_InAbc.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnk_IdsPrmRateFkey_EE_InAbc.PRM_RATE_UNIQ_KEY").alias("PRM_RATE_UNIQ_KEY"),
        F.col("lnk_IdsPrmRateFkey_EE_InAbc.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("lnk_IdsPrmRateFkey_EE_InAbc.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("lnk_IdsPrmRateFkey_EE_InAbc.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("lnk_IdsPrmRateFkey_EE_InAbc.PRM_RATE_AGE_ENTY_CD").alias("PRM_RATE_AGE_ENTY_CD"),
        F.col("lnkAgeLkpData.CD_MPPNG_SK").alias("PRM_RATE_AGE_ENTY_CD_SK"),
        F.col("lnk_IdsPrmRateFkey_EE_InAbc.PRM_RATE_PRM_TYP_CD").alias("PRM_RATE_PRM_TYP_CD"),
        F.col("lnkPrmTypLkpData.CD_MPPNG_SK").alias("PRM_RATE_PRM_TYP_CD_SK"),
        F.col("lnk_IdsPrmRateFkey_EE_InAbc.EFF_DT").alias("EFF_DT"),
        F.col("Ref_EffDtSk.CLNDR_DT_SK").alias("EFF_DT_SK"),
        F.col("lnk_IdsPrmRateFkey_EE_InAbc.TERM_DT").alias("TERM_DT"),
        F.col("Ref_TermDtSk.CLNDR_DT_SK").alias("TERM_DT_SK"),
        F.col("lnk_IdsPrmRateFkey_EE_InAbc.PRM_RATE_PFX").alias("PRM_RATE_PFX"),
        F.col("lnk_IdsPrmRateFkey_EE_InAbc.PRM_RATE_DESC").alias("PRM_RATE_DESC"),
        F.col("lnk_IdsPrmRateFkey_EE_InAbc.PRM_RATE_MOD_ID").alias("PRM_RATE_MOD_ID"),
        F.col("lnk_IdsPrmRateFkey_EE_InAbc.MAX_CHLDRN_RATED_NO").alias("MAX_CHLDRN_RATED_NO"),
        F.col("lnk_IdsPrmRateFkey_EE_InAbc.PRM_RATE_BSS_NO").alias("PRM_RATE_BSS_NO"),
        F.col("lnk_IdsPrmRateFkey_EE_InAbc.PRM_RATE_KEY_STRCT_NO").alias("PRM_RATE_KEY_STRCT_NO"),
        F.col("lnk_IdsPrmRateFkey_EE_InAbc.PRM_RATE_METH").alias("PRM_RATE_METH"),
        F.col("lnkMethCdLkpData.CD_MPPNG_SK").alias("PRM_RATE_METH_CD_SK"),
        F.col("lnk_IdsPrmRateFkey_EE_InAbc.PRM_RATE_COL_STRUCT").alias("PRM_RATE_COL_STRUCT"),
        F.col("lnkTierClmnLkpData.CD_MPPNG_SK").alias("PRM_RATE_TIER_CLMN_STRCT_CD_SK"),
        F.col("lnk_IdsPrmRateFkey_EE_InAbc.PRM_RATE_MOD_TYPE").alias("PRM_RATE_MOD_TYPE"),
        F.col("lnkTierModLkpData.CD_MPPNG_SK").alias("PRM_RATE_TIER_MOD_TYP_CD_SK"),
        F.col("lnk_IdsPrmRateFkey_EE_InAbc.TIER_AGE_BAND_FROM_NO").alias("TIER_AGE_BAND_FROM_NO"),
        F.col("lnk_IdsPrmRateFkey_EE_InAbc.TIER_AGE_BAND_THRU_NO").alias("TIER_AGE_BAND_THRU_NO")
    )
)

# --------------------------------------------------------------------------------
# Stage: xfm_CheckLkpResults (CTransformerStage)
# Creates stage variables => output to multiple links
# --------------------------------------------------------------------------------
df_xfm_CheckLkpResults = (
    df_lkp_Code_SKs
    .withColumn("svPrmRateAgeEntyCdFKeyLkupCheck",
        F.when(F.col("PRM_RATE_AGE_ENTY_CD_SK").isNull(), F.lit("Y")).otherwise(F.lit("N"))
    )
    .withColumn("svPrmRatePrmTypCdFKeyLkupCheck",
        F.when(F.col("PRM_RATE_PRM_TYP_CD_SK").isNull(), F.lit("Y")).otherwise(F.lit("N"))
    )
    .withColumn("svPrmRateMethCdFKeyLkupCheck",
        F.when(F.col("PRM_RATE_METH_CD_SK").isNull(), F.lit("Y")).otherwise(F.lit("N"))
    )
    .withColumn("svPrmRateTierClmnStrctFKeyLkupCheck",
        F.when(F.col("PRM_RATE_TIER_CLMN_STRCT_CD_SK").isNull(), F.lit("Y")).otherwise(F.lit("N"))
    )
    .withColumn("svPrmRateTierModTypCdFKeyLkupCheck",
        F.when(F.col("PRM_RATE_TIER_MOD_TYP_CD_SK").isNull(), F.lit("Y")).otherwise(F.lit("N"))
    )
    .withColumn("svTermDtFkeyLkupCheck",
        F.when(F.col("TERM_DT_SK").isNull(), F.lit("Y")).otherwise(F.lit("N"))
    )
    .withColumn("svEffDtFkeyLkupCheck",
        F.when(F.col("EFF_DT_SK").isNull(), F.lit("Y")).otherwise(F.lit("N"))
    )
)

df_xfm_CheckLkpResults_Lnk_PrmRate_Main = df_xfm_CheckLkpResults.select(
    F.col("PRM_RATE_SK").alias("PRM_RATE_SK"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("PRM_RATE_UNIQ_KEY").alias("PRM_RATE_UNIQ_KEY"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(F.col("PRM_RATE_AGE_ENTY_CD_SK").isNull(), F.lit(0)).otherwise(F.col("PRM_RATE_AGE_ENTY_CD_SK")).alias("PRM_RATE_AGE_ENTY_CD_SK"),
    F.when(F.col("PRM_RATE_PRM_TYP_CD_SK").isNull(), F.lit(0)).otherwise(F.col("PRM_RATE_PRM_TYP_CD_SK")).alias("PRM_RATE_PRM_TYP_CD_SK"),
    F.when(F.col("EFF_DT_SK").isNull(), F.lit("1753-01-01")).otherwise(F.col("EFF_DT_SK")).alias("EFF_DT_SK"),
    F.when(F.col("TERM_DT_SK").isNull(), F.lit("2199-01-01")).otherwise(F.col("TERM_DT_SK")).alias("TERM_DT_SK"),
    F.col("PRM_RATE_PFX").alias("PRM_RATE_PFX"),
    F.col("PRM_RATE_DESC").alias("PRM_RATE_DESC"),
    F.col("PRM_RATE_MOD_ID").alias("PRM_RATE_MOD_ID"),
    F.col("MAX_CHLDRN_RATED_NO").alias("MAX_CHLDRN_RATED_NO"),
    F.col("PRM_RATE_BSS_NO").alias("PRM_RATE_BSS_NO"),
    F.col("PRM_RATE_KEY_STRCT_NO").alias("PRM_RATE_KEY_STRCT_NO"),
    F.when(F.col("PRM_RATE_METH_CD_SK").isNull(), F.lit(0)).otherwise(F.col("PRM_RATE_METH_CD_SK")).alias("PRM_RATE_METH_CD_SK"),
    F.when(F.col("PRM_RATE_TIER_CLMN_STRCT_CD_SK").isNull(), F.lit(0)).otherwise(F.col("PRM_RATE_TIER_CLMN_STRCT_CD_SK")).alias("PRM_RATE_TIER_CLMN_STRCT_CD_SK"),
    F.when(F.col("PRM_RATE_TIER_MOD_TYP_CD_SK").isNull(), F.lit(0)).otherwise(F.col("PRM_RATE_TIER_MOD_TYP_CD_SK")).alias("PRM_RATE_TIER_MOD_TYP_CD_SK"),
    F.col("TIER_AGE_BAND_FROM_NO").alias("TIER_AGE_BAND_FROM_NO"),
    F.col("TIER_AGE_BAND_THRU_NO").alias("TIER_AGE_BAND_THRU_NO"),
    F.col("PRI_NAT_KEY_STRING").alias("_TMP_PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD").alias("_TMP_SRC_SYS_CD"),
    F.col("FIRST_RECYC_TS").alias("_TMP_FIRST_RECYC_TS")
)

df_xfm_CheckLkpResults_lnkMethCdLkupFail = (
    df_xfm_CheckLkpResults
    .filter(F.col("svPrmRateMethCdFKeyLkupCheck") == "Y")
    .select(
        F.col("PRM_RATE_SK").alias("PRI_SK"),
        F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
        F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.lit("IdsPrmRateFkey_EE").alias("JOB_NM"),
        F.lit("CDLOOKUP").alias("ERROR_TYP"),
        F.lit("CD_MPPNG").alias("PHYSCL_FILE_NM"),
        F.concat(
            F.col("SRC_SYS_CD"),
            F.lit(";FACETS DBO;IDS;PREMIUM RATE METHOD;PREMIUM RATE METHOD;"),
            F.col("PRM_RATE_METH")
        ).alias("FRGN_NAT_KEY_STRING"),
        F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
    )
)

df_xfm_CheckLkpResults_lnkTierModLkpFail = (
    df_xfm_CheckLkpResults
    .filter(F.col("svPrmRateTierModTypCdFKeyLkupCheck") == "Y")
    .select(
        F.col("PRM_RATE_SK").alias("PRI_SK"),
        F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
        F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.lit("IdsPrmRateFkey_EE").alias("JOB_NM"),
        F.lit("CDLOOKUP").alias("ERROR_TYP"),
        F.lit("CD_MPPNG").alias("PHYSCL_FILE_NM"),
        F.concat(
            F.col("SRC_SYS_CD"),
            F.lit(";FACETS DBO;IDS;PREMIUM RATE TIER MODIFIER TYPE;PREMIUM RATE TIER MODIFIER TYPE;"),
            F.col("PRM_RATE_MOD_TYPE")
        ).alias("FRGN_NAT_KEY_STRING"),
        F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
    )
)

df_xfm_CheckLkpResults_lnkTierClmLkpFail = (
    df_xfm_CheckLkpResults
    .filter(F.col("svPrmRateTierClmnStrctFKeyLkupCheck") == "Y")
    .select(
        F.col("PRM_RATE_SK").alias("PRI_SK"),
        F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
        F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.lit("IdsPrmRateFkey_EE").alias("JOB_NM"),
        F.lit("CDLOOKUP").alias("ERROR_TYP"),
        F.lit("CD_MPPNG").alias("PHYSCL_FILE_NM"),
        F.concat(
            F.col("SRC_SYS_CD"),
            F.lit(";FACETS DBO;IDS;PREMIUM RATE TIER COLUMN STRUCTURE;PREMIUM RATE TIER COLUMN STRUCTURE;"),
            F.col("PRM_RATE_COL_STRUCT")
        ).alias("FRGN_NAT_KEY_STRING"),
        F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
    )
)

df_xfm_CheckLkpResults_lnkPrmTypLkupFail = (
    df_xfm_CheckLkpResults
    .filter(F.col("svPrmRatePrmTypCdFKeyLkupCheck") == "Y")
    .select(
        F.col("PRM_RATE_SK").alias("PRI_SK"),
        F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
        F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.lit("IdsPrmRateFkey_EE").alias("JOB_NM"),
        F.lit("CDLOOKUP").alias("ERROR_TYP"),
        F.lit("CD_MPPNG").alias("PHYSCL_FILE_NM"),
        F.concat(
            F.col("SRC_SYS_CD"),
            F.lit(";FACETS DBO;IDS;PREMIUM RATE PREMIUM TYPE;PREMIUM RATE PREMIUM TYPE;"),
            F.col("PRM_RATE_PRM_TYP_CD")
        ).alias("FRGN_NAT_KEY_STRING"),
        F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
    )
)

df_xfm_CheckLkpResults_lnkAgeLkupFail = (
    df_xfm_CheckLkpResults
    .filter(F.col("svPrmRateAgeEntyCdFKeyLkupCheck") == "Y")
    .select(
        F.col("PRM_RATE_SK").alias("PRI_SK"),
        F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
        F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.lit("IdsPrmRateFkey_EE").alias("JOB_NM"),
        F.lit("CDLOOKUP").alias("ERROR_TYP"),
        F.lit("CD_MPPNG").alias("PHYSCL_FILE_NM"),
        F.concat(
            F.col("SRC_SYS_CD"),
            F.lit(";FACETS DBO;IDS;PREMIUM RATE AGE ENTITY;PREMIUM RATE AGE ENTITY;"),
            F.col("PRM_RATE_AGE_ENTY_CD")
        ).alias("FRGN_NAT_KEY_STRING"),
        F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
    )
)

df_xfm_CheckLkpResults_lnkTermDtLkupFail = (
    df_xfm_CheckLkpResults
    .filter(F.col("svTermDtFkeyLkupCheck") == "Y")
    .select(
        F.col("PRM_RATE_SK").alias("PRI_SK"),
        F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
        F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.lit("IdsPrmRateFkey_EE").alias("JOB_NM"),
        F.lit("FKLOOKUP").alias("ERROR_TYP"),
        F.lit("CLNDR_DT").alias("PHYSCL_FILE_NM"),
        F.concat(
            F.col("SRC_SYS_CD"),
            F.lit(";"),
            F.col("TERM_DT")
        ).alias("FRGN_NAT_KEY_STRING"),
        F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
    )
)

df_xfm_CheckLkpResults_lnkEffDtLkpFail = (
    df_xfm_CheckLkpResults
    .filter(F.col("svEffDtFkeyLkupCheck") == "Y")
    .select(
        F.col("PRM_RATE_SK").alias("PRI_SK"),
        F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
        F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.lit("IdsPrmRateFkey_EE").alias("JOB_NM"),
        F.lit("FKLOOKUP").alias("ERROR_TYP"),
        F.lit("CLNDR_DT").alias("PHYSCL_FILE_NM"),
        F.concat(
            F.col("SRC_SYS_CD"),
            F.lit(";"),
            F.col("EFF_DT")
        ).alias("FRGN_NAT_KEY_STRING"),
        F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
    )
)

# --------------------------------------------------------------------------------
# Stage: FnlFkeyFailures (PxFunnel) -> union all fail DataFrames
# --------------------------------------------------------------------------------
fkey_fail_columns = [
    "PRI_SK",
    "PRI_NAT_KEY_STRING",
    "SRC_SYS_CD_SK",
    "JOB_NM",
    "ERROR_TYP",
    "PHYSCL_FILE_NM",
    "FRGN_NAT_KEY_STRING",
    "FIRST_RECYC_TS",
    "JOB_EXCTN_SK"
]
df_FnlFkeyFailures = (
    df_xfm_CheckLkpResults_lnkMethCdLkupFail.select(fkey_fail_columns)
    .union(df_xfm_CheckLkpResults_lnkTierClmLkpFail.select(fkey_fail_columns))
    .union(df_xfm_CheckLkpResults_lnkPrmTypLkupFail.select(fkey_fail_columns))
    .union(df_xfm_CheckLkpResults_lnkAgeLkupFail.select(fkey_fail_columns))
    .union(df_xfm_CheckLkpResults_lnkTierModLkpFail.select(fkey_fail_columns))
    .union(df_xfm_CheckLkpResults_lnkTermDtLkupFail.select(fkey_fail_columns))
    .union(df_xfm_CheckLkpResults_lnkEffDtLkpFail.select(fkey_fail_columns))
)

# Before writing FnlFkeyFailures, apply rpad if char/varchar
# We'll assume unknown varchar lengths => use 255
df_FnlFkeyFailures_rpad = (
    df_FnlFkeyFailures
    .withColumn("PRI_NAT_KEY_STRING", F.rpad("PRI_NAT_KEY_STRING", 255, " "))
    .withColumn("JOB_NM", F.rpad("JOB_NM", 255, " "))
    .withColumn("ERROR_TYP", F.rpad("ERROR_TYP", 255, " "))
    .withColumn("PHYSCL_FILE_NM", F.rpad("PHYSCL_FILE_NM", 255, " "))
    .withColumn("FRGN_NAT_KEY_STRING", F.rpad("FRGN_NAT_KEY_STRING", 255, " "))
)

# --------------------------------------------------------------------------------
# Stage: seq_FkeyFailedFile_csv (PxSequentialFile) -> write df_FnlFkeyFailures_rpad
# #$FilePath#/fkey_failures/#PrefixFkeyFailedFileName#.#DSJobName#.dat
# DSJobName=IdsPrmRateFkey_EE
# --------------------------------------------------------------------------------
file_path_seq_FkeyFailedFile_csv = f"{adls_path}/fkey_failures/{PrefixFkeyFailedFileName}.IdsPrmRateFkey_EE.dat"
write_files(
    df_FnlFkeyFailures_rpad.select(fkey_fail_columns),
    file_path_seq_FkeyFailedFile_csv,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

# --------------------------------------------------------------------------------
# Next funnel: fnl_NA_UNK_Streams
# Inputs: Lnk_PrmRate_Main, Lnk_PrmRateUNK, Lnk_PrmRateNA
# The latter two each output a single row with the specified constants
# --------------------------------------------------------------------------------

# Lnk_PrmRateUNK => constraint: "((@INROWNUM - 1) * @NUMPARTITIONS + @PARTITIONNUM + 1) = 1"
# We replicate this by taking a single row approach for a special row of constants:
df_xfm_CheckLkpResults_lnkPrmRateUNK = (
    df_xfm_CheckLkpResults.limit(1)  # single row
    .select(
        F.lit(0).alias("PRM_RATE_SK"),
        F.lit(0).alias("SRC_SYS_CD_SK"),
        F.lit(0).alias("PRM_RATE_UNIQ_KEY"),
        F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("PRM_RATE_AGE_ENTY_CD_SK"),
        F.lit(0).alias("PRM_RATE_PRM_TYP_CD_SK"),
        F.lit("1753-01-01").alias("EFF_DT_SK"),
        F.lit("1753-01-01").alias("TERM_DT_SK"),
        F.lit("").alias("PRM_RATE_PFX"),
        F.lit(None).alias("PRM_RATE_DESC"),
        F.lit("UNK").alias("PRM_RATE_MOD_ID"),
        F.lit(0).alias("MAX_CHLDRN_RATED_NO"),
        F.lit(0).alias("PRM_RATE_BSS_NO"),
        F.lit(0).alias("PRM_RATE_KEY_STRCT_NO"),
        F.lit(0).alias("PRM_RATE_METH_CD_SK"),
        F.lit(0).alias("PRM_RATE_TIER_CLMN_STRCT_CD_SK"),
        F.lit(0).alias("PRM_RATE_TIER_MOD_TYP_CD_SK"),
        F.lit(0).alias("TIER_AGE_BAND_FROM_NO"),
        F.lit(0).alias("TIER_AGE_BAND_THRU_NO"),
        F.lit("").alias("_TMP_PRI_NAT_KEY_STRING"),
        F.lit("").alias("_TMP_SRC_SYS_CD"),
        F.lit(None).alias("_TMP_FIRST_RECYC_TS")
    )
)

# Lnk_PrmRateNA => constraint: same row-limiting approach
df_xfm_CheckLkpResults_lnkPrmRateNA = (
    df_xfm_CheckLkpResults.limit(1)
    .select(
        F.lit(1).alias("PRM_RATE_SK"),
        F.lit(1).alias("SRC_SYS_CD_SK"),
        F.lit(1).alias("PRM_RATE_UNIQ_KEY"),
        F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("PRM_RATE_AGE_ENTY_CD_SK"),
        F.lit(1).alias("PRM_RATE_PRM_TYP_CD_SK"),
        F.lit("1753-01-01").alias("EFF_DT_SK"),
        F.lit("1753-01-01").alias("TERM_DT_SK"),
        F.lit("").alias("PRM_RATE_PFX"),
        F.lit(None).alias("PRM_RATE_DESC"),
        F.lit("NA").alias("PRM_RATE_MOD_ID"),
        F.lit(0).alias("MAX_CHLDRN_RATED_NO"),
        F.lit(0).alias("PRM_RATE_BSS_NO"),
        F.lit(0).alias("PRM_RATE_KEY_STRCT_NO"),
        F.lit(1).alias("PRM_RATE_METH_CD_SK"),
        F.lit(1).alias("PRM_RATE_TIER_CLMN_STRCT_CD_SK"),
        F.lit(1).alias("PRM_RATE_TIER_MOD_TYP_CD_SK"),
        F.lit(0).alias("TIER_AGE_BAND_FROM_NO"),
        F.lit(0).alias("TIER_AGE_BAND_THRU_NO"),
        F.lit("").alias("_TMP_PRI_NAT_KEY_STRING"),
        F.lit("").alias("_TMP_SRC_SYS_CD"),
        F.lit(None).alias("_TMP_FIRST_RECYC_TS")
    )
)

# Final union for fnl_NA_UNK_Streams
cols_fnl_NA_UNK_Streams = [
    "PRM_RATE_SK",
    "SRC_SYS_CD_SK",
    "PRM_RATE_UNIQ_KEY",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "PRM_RATE_AGE_ENTY_CD_SK",
    "PRM_RATE_PRM_TYP_CD_SK",
    "EFF_DT_SK",
    "TERM_DT_SK",
    "PRM_RATE_PFX",
    "PRM_RATE_DESC",
    "PRM_RATE_MOD_ID",
    "MAX_CHLDRN_RATED_NO",
    "PRM_RATE_BSS_NO",
    "PRM_RATE_KEY_STRCT_NO",
    "PRM_RATE_METH_CD_SK",
    "PRM_RATE_TIER_CLMN_STRCT_CD_SK",
    "PRM_RATE_TIER_MOD_TYP_CD_SK",
    "TIER_AGE_BAND_FROM_NO",
    "TIER_AGE_BAND_THRU_NO",
    "_TMP_PRI_NAT_KEY_STRING",
    "_TMP_SRC_SYS_CD",
    "_TMP_FIRST_RECYC_TS"
]

df_fnl_NA_UNK_Streams = (
    df_xfm_CheckLkpResults_Lnk_PrmRate_Main.select(cols_fnl_NA_UNK_Streams)
    .union(df_xfm_CheckLkpResults_lnkPrmRateUNK.select(cols_fnl_NA_UNK_Streams))
    .union(df_xfm_CheckLkpResults_lnkPrmRateNA.select(cols_fnl_NA_UNK_Streams))
)

# --------------------------------------------------------------------------------
# Stage: seq_PRM_RATE_FKey_csv (PxSequentialFile) -> final write
# #$FilePath#/load/PRM_RATE.#SrcSysCd#.#RunID#.dat
# --------------------------------------------------------------------------------

# Apply rpad for char/varchar columns that have known lengths
# EFF_DT_SK => char(10), TERM_DT_SK => char(10)
# Others declared as varchar => use 255 if not specified
df_fnl_NA_UNK_Streams_rpad = (
    df_fnl_NA_UNK_Streams
    .withColumn("EFF_DT_SK", F.rpad("EFF_DT_SK", 10, " "))
    .withColumn("TERM_DT_SK", F.rpad("TERM_DT_SK", 10, " "))
    .withColumn("PRM_RATE_PFX", F.rpad("PRM_RATE_PFX", 255, " "))
    .withColumn("PRM_RATE_DESC", F.rpad("PRM_RATE_DESC", 255, " "))
    .withColumn("PRM_RATE_MOD_ID", F.rpad("PRM_RATE_MOD_ID", 255, " "))
)

final_cols = [
    "PRM_RATE_SK",
    "SRC_SYS_CD_SK",
    "PRM_RATE_UNIQ_KEY",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "PRM_RATE_AGE_ENTY_CD_SK",
    "PRM_RATE_PRM_TYP_CD_SK",
    "EFF_DT_SK",
    "TERM_DT_SK",
    "PRM_RATE_PFX",
    "PRM_RATE_DESC",
    "PRM_RATE_MOD_ID",
    "MAX_CHLDRN_RATED_NO",
    "PRM_RATE_BSS_NO",
    "PRM_RATE_KEY_STRCT_NO",
    "PRM_RATE_METH_CD_SK",
    "PRM_RATE_TIER_CLMN_STRCT_CD_SK",
    "PRM_RATE_TIER_MOD_TYP_CD_SK",
    "TIER_AGE_BAND_FROM_NO",
    "TIER_AGE_BAND_THRU_NO"
]

file_path_seq_PRM_RATE_FKey_csv = f"{adls_path}/load/PRM_RATE.{SrcSysCd}.{RunID}.dat"
write_files(
    df_fnl_NA_UNK_Streams_rpad.select(final_cols),
    file_path_seq_PRM_RATE_FKey_csv,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)