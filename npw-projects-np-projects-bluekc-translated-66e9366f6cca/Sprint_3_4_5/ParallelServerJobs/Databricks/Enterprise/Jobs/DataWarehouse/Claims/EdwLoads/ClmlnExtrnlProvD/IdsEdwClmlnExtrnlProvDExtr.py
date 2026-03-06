# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2024 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:     IdsEdwClmlnExtrnlProvDExtr
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   Pulls data from IDS clmln_extrnl_prov table and EDW clmln_extrnl_prov_d table
# MAGIC   Loads the data from EDW and IDS into two hash files for subsequent comparison.
# MAGIC   The comparison will determine rows that have changed and whether to generate a new history record.
# MAGIC       
# MAGIC INPUTS:
# MAGIC 	IDS - CLM_LN_EXTRNL_PROV
# MAGIC                 IDS - W_EDW_ETL_DRVR
# MAGIC                 EDW - CLM_LN_EXTRNL_PROV_D
# MAGIC                 EDW - W_EDW_ETL_DRVR
# MAGIC                
# MAGIC TRANSFORMS:  
# MAGIC                   TRIM all the fields that are extracted or used in lookups
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                   IDS - read from source, lookup all code SK values and get the natural codes
# MAGIC                   EDW - Read from source and dump to hf. Do nothing but trim the fields.
# MAGIC             
# MAGIC OUTPUTS: 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC HariKrishnaRao Yadav 24/04/2024    US616525                             Original Programming                                                                    EnterpriseDev1          Jeyaprasanna               2024-06-27

# MAGIC Extracts all data from IDS reference table CD_MPPNG,
# MAGIC 
# MAGIC 
# MAGIC Code SK lookups for Denormalization
# MAGIC Lookup Keys:
# MAGIC SRC_SYS_CD_SK
# MAGIC SVC_FCLTY_LOC_ST_CD_SK
# MAGIC SVC_PROV_TXNMY_CD_SK
# MAGIC SVC_PROV_CLS_CD_SK
# MAGIC Add CRT_RUN_CYC_EXCTN_DT_SK ,LAST_UPDT_RUN_CYC_EXCTN_DT_SK,NA and UNK rows
# MAGIC Job name:
# MAGIC IdsEdwClmlnExtrnlProvDExtr
# MAGIC EDW Claim Ln External Provider Extact from IDS
# MAGIC Write CLM_LN_EXTRNL_PROV_D Data into a Sequential file for Load Ready Job.
# MAGIC Read data from source tables CLM_LN_EXTRNL_PROV
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query_db2_CLM_LN_EXTRNL_PROV_in = f"""SELECT 
PROV.CLM_LN_EXTRNL_PROV_SK,
PROV.CLM_ID,
PROV.CLM_LN_SEQ_NO,
PROV.SRC_SYS_CD_SK,
PROV.CRT_RUN_CYC_EXCTN_SK,
PROV.LAST_UPDT_RUN_CYC_EXCTN_SK,
PROV.CLM_LN_SK,
PROV.SVC_PROV_TXNMY_CD_SK,
PROV.SVC_FCLTY_LOC_ST_CD_SK,
PROV.SVC_PROV_CLS_CD_SK,
PROV.SVC_PROV_INDN_HLTH_SVC_IN,
PROV.SVC_PROV_TYP_PPO_AVLBL_IN,
PROV.ATCHMT_SRC_ID_DTM,
PROV.SVC_PROV_ID,
PROV.ADTNL_DATA_ELE_TX,
PROV.FLEX_NTWK_PROV_CST_GRPNG_TX,
PROV.HOST_PROV_ITS_TIER_DSGTN_TX,
PROV.MKT_ID,
PROV.SVC_FCLTY_LOC_NO,
PROV.SVC_FCLTY_LOC_NO_QLFR_TX,
PROV.SVC_FCLTY_LOC_ZIP_CD,
PROV.SVC_PROV_NPI,
PROV.SVC_PROV_NM,
PROV.SVC_PROV_SPEC_TX,
PROV.SVC_PROV_TYP_TX,
PROV.SVC_PROV_ZIP_CD
FROM 
{IDSOwner}.CLM_LN_EXTRNL_PROV PROV,
{IDSOwner}.W_EDW_ETL_DRVR DRVR
WHERE 
PROV.SRC_SYS_CD_SK = DRVR.SRC_SYS_CD_SK 
AND PROV.CLM_ID = DRVR.CLM_ID
"""
df_db2_CLM_LN_EXTRNL_PROV_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CLM_LN_EXTRNL_PROV_in)
    .load()
)

extract_query_db2_CD_MPPNG_Svc_Fclty_Loc_St = f"""SELECT 
CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') TRGT_CD,
COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM
FROM
{IDSOwner}.CD_MPPNG
WHERE SRC_CLCTN_CD = 'FACETS DBO'
  AND SRC_DOMAIN_NM = 'STATE'
  AND SRC_SYS_CD = 'FACETS'
  AND TRGT_SRC_SYS_CD = 'IDS'
"""
df_db2_CD_MPPNG_Svc_Fclty_Loc_St = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CD_MPPNG_Svc_Fclty_Loc_St)
    .load()
)

extract_query_db2_TXNMY_CD_in = f"""SELECT 
TXNMY_CD_SK,
TXNMY_CD
FROM
{IDSOwner}.TXNMY_CD
"""
df_db2_TXNMY_CD_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_TXNMY_CD_in)
    .load()
)

extract_query_db2_CD_MPPNG_Src_Sys_cd = f"""SELECT 
CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') TRGT_CD,
COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM
FROM
{IDSOwner}.CD_MPPNG
WHERE SRC_DRVD_LKUP_VAL = 'FACETS'
  AND SRC_SYS_CD = 'IDS'
  AND SRC_CLCTN_CD = 'IDS'
  AND SRC_DOMAIN_NM = 'SOURCE SYSTEM'
  AND TRGT_CLCTN_CD = 'IDS'
  AND TRGT_DOMAIN_NM = 'SOURCE SYSTEM'
"""
df_db2_CD_MPPNG_Src_Sys_cd = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CD_MPPNG_Src_Sys_cd)
    .load()
)

extract_query_db2_CD_MPPNG2_Svc_Prov_cls_Cd = f"""SELECT 
CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') TRGT_CD,
COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM
FROM
{IDSOwner}.CD_MPPNG
WHERE SRC_SYS_CD='FACETS'
  AND SRC_CLCTN_CD='FACETS DBO'
  AND SRC_DOMAIN_NM='PROVIDER CLASSIFICATION'
  AND TRGT_DOMAIN_NM='PROVIDER CLASSIFICATION'
  AND TRGT_CLCTN_CD='IDS'
"""
df_db2_CD_MPPNG2_Svc_Prov_cls_Cd = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CD_MPPNG2_Svc_Prov_cls_Cd)
    .load()
)

df_Lkp_CdmaCodes = (
    df_db2_CLM_LN_EXTRNL_PROV_in.alias("Ink_IdsEdwClmlnExtrnlProvDExtr_inABC")
    .join(
        df_db2_CD_MPPNG_Src_Sys_cd.alias("ref_Src_Sys_cd_Sk"),
        F.col("Ink_IdsEdwClmlnExtrnlProvDExtr_inABC.SRC_SYS_CD_SK")
        == F.col("ref_Src_Sys_cd_Sk.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_db2_CD_MPPNG_Svc_Fclty_Loc_St.alias("ref_Svc_Fclty_Loc_St_Cd_Sk"),
        F.col("Ink_IdsEdwClmlnExtrnlProvDExtr_inABC.SVC_FCLTY_LOC_ST_CD_SK")
        == F.col("ref_Svc_Fclty_Loc_St_Cd_Sk.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_db2_TXNMY_CD_in.alias("ref_Svc_Prov_Txnmy_Cd_Sk"),
        F.col("Ink_IdsEdwClmlnExtrnlProvDExtr_inABC.SVC_PROV_TXNMY_CD_SK")
        == F.col("ref_Svc_Prov_Txnmy_Cd_Sk.TXNMY_CD_SK"),
        "left",
    )
    .join(
        df_db2_CD_MPPNG2_Svc_Prov_cls_Cd.alias("ref_Svc_Prov_cls_Cd_Sk"),
        F.col("Ink_IdsEdwClmlnExtrnlProvDExtr_inABC.SVC_PROV_CLS_CD_SK")
        == F.col("ref_Svc_Prov_cls_Cd_Sk.CD_MPPNG_SK"),
        "left",
    )
    .select(
        F.col("Ink_IdsEdwClmlnExtrnlProvDExtr_inABC.CLM_LN_EXTRNL_PROV_SK").alias(
            "CLM_LN_EXTRNL_PROV_SK"
        ),
        F.col("Ink_IdsEdwClmlnExtrnlProvDExtr_inABC.CLM_ID").alias("CLM_ID"),
        F.col("Ink_IdsEdwClmlnExtrnlProvDExtr_inABC.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.col("Ink_IdsEdwClmlnExtrnlProvDExtr_inABC.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("Ink_IdsEdwClmlnExtrnlProvDExtr_inABC.CRT_RUN_CYC_EXCTN_SK").alias(
            "CRT_RUN_CYC_EXCTN_SK"
        ),
        F.col("Ink_IdsEdwClmlnExtrnlProvDExtr_inABC.LAST_UPDT_RUN_CYC_EXCTN_SK").alias(
            "LAST_UPDT_RUN_CYC_EXCTN_SK"
        ),
        F.col("Ink_IdsEdwClmlnExtrnlProvDExtr_inABC.CLM_LN_SK").alias("CLM_LN_SK"),
        F.col("Ink_IdsEdwClmlnExtrnlProvDExtr_inABC.SVC_PROV_TXNMY_CD_SK").alias(
            "SVC_PROV_TXNMY_CD_SK"
        ),
        F.col("Ink_IdsEdwClmlnExtrnlProvDExtr_inABC.SVC_FCLTY_LOC_ST_CD_SK").alias(
            "SVC_FCLTY_LOC_ST_CD_SK"
        ),
        F.col("Ink_IdsEdwClmlnExtrnlProvDExtr_inABC.SVC_PROV_CLS_CD_SK").alias(
            "SVC_PROV_CLS_CD_SK"
        ),
        F.col("Ink_IdsEdwClmlnExtrnlProvDExtr_inABC.SVC_PROV_INDN_HLTH_SVC_IN").alias(
            "SVC_PROV_INDN_HLTH_SVC_IN"
        ),
        F.col("Ink_IdsEdwClmlnExtrnlProvDExtr_inABC.SVC_PROV_TYP_PPO_AVLBL_IN").alias(
            "SVC_PROV_TYP_PPO_AVLBL_IN"
        ),
        F.col("Ink_IdsEdwClmlnExtrnlProvDExtr_inABC.ATCHMT_SRC_ID_DTM").alias("ATCHMT_SRC_ID_DTM"),
        F.col("Ink_IdsEdwClmlnExtrnlProvDExtr_inABC.SVC_PROV_ID").alias("SVC_PROV_ID"),
        F.col("Ink_IdsEdwClmlnExtrnlProvDExtr_inABC.ADTNL_DATA_ELE_TX").alias(
            "ADTNL_DATA_ELE_TX"
        ),
        F.col("Ink_IdsEdwClmlnExtrnlProvDExtr_inABC.FLEX_NTWK_PROV_CST_GRPNG_TX").alias(
            "FLEX_NTWK_PROV_CST_GRPNG_TX"
        ),
        F.col("Ink_IdsEdwClmlnExtrnlProvDExtr_inABC.HOST_PROV_ITS_TIER_DSGTN_TX").alias(
            "HOST_PROV_ITS_TIER_DSGTN_TX"
        ),
        F.col("Ink_IdsEdwClmlnExtrnlProvDExtr_inABC.MKT_ID").alias("MKT_ID"),
        F.col("Ink_IdsEdwClmlnExtrnlProvDExtr_inABC.SVC_FCLTY_LOC_NO").alias("SVC_FCLTY_LOC_NO"),
        F.col("Ink_IdsEdwClmlnExtrnlProvDExtr_inABC.SVC_FCLTY_LOC_NO_QLFR_TX").alias(
            "SVC_FCLTY_LOC_NO_QLFR_TX"
        ),
        F.col("Ink_IdsEdwClmlnExtrnlProvDExtr_inABC.SVC_FCLTY_LOC_ZIP_CD").alias(
            "SVC_FCLTY_LOC_ZIP_CD"
        ),
        F.col("Ink_IdsEdwClmlnExtrnlProvDExtr_inABC.SVC_PROV_NPI").alias("SVC_PROV_NPI"),
        F.col("Ink_IdsEdwClmlnExtrnlProvDExtr_inABC.SVC_PROV_NM").alias("SVC_PROV_NM"),
        F.col("Ink_IdsEdwClmlnExtrnlProvDExtr_inABC.SVC_PROV_SPEC_TX").alias("SVC_PROV_SPEC_TX"),
        F.col("Ink_IdsEdwClmlnExtrnlProvDExtr_inABC.SVC_PROV_TYP_TX").alias("SVC_PROV_TYP_TX"),
        F.col("Ink_IdsEdwClmlnExtrnlProvDExtr_inABC.SVC_PROV_ZIP_CD").alias("SVC_PROV_ZIP_CD"),
        F.col("ref_Src_Sys_cd_Sk.TRGT_CD").alias("SRC_SYS_CD"),
        F.col("ref_Src_Sys_cd_Sk.TRGT_CD_NM"),  # Not mapped to output but brought in. 
        F.col("ref_Svc_Fclty_Loc_St_Cd_Sk.TRGT_CD").alias("SVC_FCLTY_LOC_ST_CD"),
        F.col("ref_Svc_Fclty_Loc_St_Cd_Sk.TRGT_CD_NM").alias("SVC_FCLTY_LOC_ST_CD_NM"),
        F.col("ref_Svc_Prov_Txnmy_Cd_Sk.TXNMY_CD").alias("SVC_PROV_TXNMY_CD_NM"),
        F.col("ref_Svc_Prov_cls_Cd_Sk.TRGT_CD").alias("SVC_PROV_CLS_CD"),
        F.col("ref_Svc_Prov_cls_Cd_Sk.TRGT_CD_NM").alias("SVC_PROV_CLS_CD_NM"),
    )
)

df_xmf_input = df_Lkp_CdmaCodes.withColumn(
    "IDS_ORIG_LAST_UPDT_RUN_CYC_EXCTN_SK", F.col("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

df_main = df_xmf_input.filter(
    (F.col("CLM_LN_EXTRNL_PROV_SK") != 0) & (F.col("CLM_LN_EXTRNL_PROV_SK") != 1)
).select(
    F.col("CLM_LN_EXTRNL_PROV_SK").alias("CLM_LN_EXTRNL_PROV_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.when(
        F.col("SRC_SYS_CD").isNull() | (trim(F.col("SRC_SYS_CD")) == ""), "UNK"
    ).otherwise(F.col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
    F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("CLM_LN_SK").alias("CLM_LN_SK"),
    F.col("SVC_PROV_TXNMY_CD_SK").alias("SVC_PROV_TXNMY_CD_SK"),
    F.when(
        F.col("SVC_FCLTY_LOC_ST_CD").isNull() | (trim(F.col("SVC_FCLTY_LOC_ST_CD")) == ""), "UNK"
    ).otherwise(F.col("SVC_FCLTY_LOC_ST_CD")).alias("SVC_FCLTY_LOC_ST_CD"),
    F.when(
        F.col("SVC_FCLTY_LOC_ST_CD_NM").isNull()
        | (trim(F.col("SVC_FCLTY_LOC_ST_CD_NM")) == ""),
        "UNK",
    ).otherwise(F.col("SVC_FCLTY_LOC_ST_CD_NM")).alias("SVC_FCLTY_LOC_ST_CD_NM"),
    F.when(
        F.col("SVC_PROV_TXNMY_CD_NM").isNull()
        | (trim(F.col("SVC_PROV_TXNMY_CD_NM")) == ""),
        "UNK",
    ).otherwise(F.col("SVC_PROV_TXNMY_CD_NM")).alias("SVC_PROV_TXNMY_CD_NM"),
    F.when(
        F.col("SVC_PROV_CLS_CD").isNull() | (trim(F.col("SVC_PROV_CLS_CD")) == ""), "UNK"
    ).otherwise(F.col("SVC_PROV_CLS_CD")).alias("SVC_PROV_CLS_CD"),
    F.when(
        F.col("SVC_PROV_CLS_CD_NM").isNull() | (trim(F.col("SVC_PROV_CLS_CD_NM")) == ""), "UNK"
    ).otherwise(F.col("SVC_PROV_CLS_CD_NM")).alias("SVC_PROV_CLS_CD_NM"),
    F.col("SVC_PROV_INDN_HLTH_SVC_IN").alias("SVC_PROV_INDN_HLTH_SVC_IN"),
    F.col("SVC_PROV_TYP_PPO_AVLBL_IN").alias("SVC_PROV_TYP_PPO_AVLBL_IN"),
    F.col("ATCHMT_SRC_ID_DTM").alias("ATCHMT_SRC_ID_DTM"),
    F.col("SVC_PROV_ID").alias("SVC_PROV_ID"),
    F.col("ADTNL_DATA_ELE_TX").alias("ADTNL_DATA_ELE_TX"),
    F.col("FLEX_NTWK_PROV_CST_GRPNG_TX").alias("FLEX_NTWK_PROV_CST_GRPNG_TX"),
    F.col("HOST_PROV_ITS_TIER_DSGTN_TX").alias("HOST_PROV_ITS_TIER_DSGTN_TX"),
    F.col("MKT_ID").alias("MKT_ID"),
    F.col("SVC_FCLTY_LOC_NO").alias("SVC_FCLTY_LOC_NO"),
    F.col("SVC_FCLTY_LOC_NO_QLFR_TX").alias("SVC_FCLTY_LOC_NO_QLFR_TX"),
    F.col("SVC_FCLTY_LOC_ZIP_CD").alias("SVC_FCLTY_LOC_ZIP_CD"),
    F.col("SVC_PROV_NPI").alias("SVC_PROV_NPI"),
    F.col("SVC_PROV_NM").alias("SVC_PROV_NM"),
    F.col("SVC_PROV_SPEC_TX").alias("SVC_PROV_SPEC_TX"),
    F.col("SVC_PROV_TYP_TX").alias("SVC_PROV_TYP_TX"),
    F.col("SVC_PROV_ZIP_CD").alias("SVC_PROV_ZIP_CD"),
    F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("IDS_ORIG_LAST_UPDT_RUN_CYC_EXCTN_SK").alias("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("SVC_FCLTY_LOC_ST_CD_SK").alias("SVC_FCLTY_LOC_ST_CD_SK"),
    F.col("SVC_PROV_CLS_CD_SK").alias("SVC_PROV_CLS_CD_SK"),
)

df_unk = spark.range(1).select(
    F.lit(0).alias("CLM_LN_EXTRNL_PROV_SK"),
    F.lit("UNK").alias("CLM_ID"),
    F.lit(0).alias("CLM_LN_SEQ_NO"),
    F.lit("UNK").alias("SRC_SYS_CD"),
    F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(0).alias("CLM_LN_SK"),
    F.lit(0).alias("SVC_PROV_TXNMY_CD_SK"),
    F.lit("UNK").alias("SVC_FCLTY_LOC_ST_CD"),
    F.lit("UNK").alias("SVC_FCLTY_LOC_ST_CD_NM"),
    F.lit("UNK").alias("SVC_PROV_TXNMY_CD_NM"),
    F.lit("UNK").alias("SVC_PROV_CLS_CD"),
    F.lit("UNK").alias("SVC_PROV_CLS_CD_NM"),
    F.lit("N").alias("SVC_PROV_INDN_HLTH_SVC_IN"),
    F.lit("N").alias("SVC_PROV_TYP_PPO_AVLBL_IN"),
    F.lit("1753-01-01 00:00:0000").alias("ATCHMT_SRC_ID_DTM"),
    F.lit("UNK").alias("SVC_PROV_ID"),
    F.lit("UNK").alias("ADTNL_DATA_ELE_TX"),
    F.lit("UNK").alias("FLEX_NTWK_PROV_CST_GRPNG_TX"),
    F.lit("UNK").alias("HOST_PROV_ITS_TIER_DSGTN_TX"),
    F.lit("UNK").alias("MKT_ID"),
    F.lit("UNK").alias("SVC_FCLTY_LOC_NO"),
    F.lit("UNK").alias("SVC_FCLTY_LOC_NO_QLFR_TX"),
    F.lit("UNK").alias("SVC_FCLTY_LOC_ZIP_CD"),
    F.lit("UNK").alias("SVC_PROV_NPI"),
    F.lit("UNK").alias("SVC_PROV_NM"),
    F.lit("UNK").alias("SVC_PROV_SPEC_TX"),
    F.lit("UNK").alias("SVC_PROV_TYP_TX"),
    F.lit("UNK").alias("SVC_PROV_ZIP_CD"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("SVC_FCLTY_LOC_ST_CD_SK"),
    F.lit(0).alias("SVC_PROV_CLS_CD_SK"),
)

df_na = spark.range(1).select(
    F.lit(1).alias("CLM_LN_EXTRNL_PROV_SK"),
    F.lit("NA").alias("CLM_ID"),
    F.lit(1).alias("CLM_LN_SEQ_NO"),
    F.lit("NA").alias("SRC_SYS_CD"),
    F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(1).alias("CLM_LN_SK"),
    F.lit(1).alias("SVC_PROV_TXNMY_CD_SK"),
    F.lit("NA").alias("SVC_FCLTY_LOC_ST_CD"),
    F.lit("NA").alias("SVC_FCLTY_LOC_ST_CD_NM"),
    F.lit("NA").alias("SVC_PROV_TXNMY_CD_NM"),
    F.lit("NA").alias("SVC_PROV_CLS_CD"),
    F.lit("NA").alias("SVC_PROV_CLS_CD_NM"),
    F.lit("N").alias("SVC_PROV_INDN_HLTH_SVC_IN"),
    F.lit("N").alias("SVC_PROV_TYP_PPO_AVLBL_IN"),
    F.lit("1753-01-01 00:00:0000").alias("ATCHMT_SRC_ID_DTM"),
    F.lit("NA").alias("SVC_PROV_ID"),
    F.lit("NA").alias("ADTNL_DATA_ELE_TX"),
    F.lit("NA").alias("FLEX_NTWK_PROV_CST_GRPNG_TX"),
    F.lit("NA").alias("HOST_PROV_ITS_TIER_DSGTN_TX"),
    F.lit("NA").alias("MKT_ID"),
    F.lit("NA").alias("SVC_FCLTY_LOC_NO"),
    F.lit("NA").alias("SVC_FCLTY_LOC_NO_QLFR_TX"),
    F.lit("NA").alias("SVC_FCLTY_LOC_ZIP_CD"),
    F.lit("NA").alias("SVC_PROV_NPI"),
    F.lit("NA").alias("SVC_PROV_NM"),
    F.lit("NA").alias("SVC_PROV_SPEC_TX"),
    F.lit("NA").alias("SVC_PROV_TYP_TX"),
    F.lit("NA").alias("SVC_PROV_ZIP_CD"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("SVC_FCLTY_LOC_ST_CD_SK"),
    F.lit(1).alias("SVC_PROV_CLS_CD_SK"),
)

df_funnel = (
    df_main.unionByName(df_unk)
    .unionByName(df_na)
)

df_final = df_funnel.select(
    "CLM_LN_EXTRNL_PROV_SK",
    "CLM_ID",
    "CLM_LN_SEQ_NO",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "CLM_LN_SK",
    "SVC_PROV_TXNMY_CD_SK",
    "SVC_FCLTY_LOC_ST_CD",
    "SVC_FCLTY_LOC_ST_CD_NM",
    "SVC_PROV_TXNMY_CD_NM",
    "SVC_PROV_CLS_CD",
    "SVC_PROV_CLS_CD_NM",
    "SVC_PROV_INDN_HLTH_SVC_IN",
    "SVC_PROV_TYP_PPO_AVLBL_IN",
    "ATCHMT_SRC_ID_DTM",
    "SVC_PROV_ID",
    "ADTNL_DATA_ELE_TX",
    "FLEX_NTWK_PROV_CST_GRPNG_TX",
    "HOST_PROV_ITS_TIER_DSGTN_TX",
    "MKT_ID",
    "SVC_FCLTY_LOC_NO",
    "SVC_FCLTY_LOC_NO_QLFR_TX",
    "SVC_FCLTY_LOC_ZIP_CD",
    "SVC_PROV_NPI",
    "SVC_PROV_NM",
    "SVC_PROV_SPEC_TX",
    "SVC_PROV_TYP_TX",
    "SVC_PROV_ZIP_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "IDS_LAST_UPDT_RUN_CYC_EXCTN_SK",
    "SVC_FCLTY_LOC_ST_CD_SK",
    "SVC_PROV_CLS_CD_SK",
).withColumn(
    "SVC_PROV_INDN_HLTH_SVC_IN", F.rpad("SVC_PROV_INDN_HLTH_SVC_IN", 1, " ")
).withColumn(
    "SVC_PROV_TYP_PPO_AVLBL_IN", F.rpad("SVC_PROV_TYP_PPO_AVLBL_IN", 1, " ")
).withColumn(
    "CRT_RUN_CYC_EXCTN_DT_SK", F.rpad("CRT_RUN_CYC_EXCTN_DT_SK", 10, " ")
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.rpad("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", 10, " ")
)

write_files(
    df_final,
    f"{adls_path}/load/CLM_LN_EXTRNL_PROV_D.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)