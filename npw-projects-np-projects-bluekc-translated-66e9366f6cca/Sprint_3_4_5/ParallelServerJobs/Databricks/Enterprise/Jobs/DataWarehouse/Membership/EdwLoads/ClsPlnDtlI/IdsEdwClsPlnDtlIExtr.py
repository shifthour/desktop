# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC DESCRIPTION:   Pulls data from CLS_PLN_DTL and creates CLS_PLN_DTL_I file
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer               Date                         Change Description                                                                           Project #          Development Project          Code Reviewer          Date Reviewed  
# MAGIC ------------------             ----------------------------     -----------------------------------------------------------------------                                   ----------------         ------------------------------------       ----------------------------      ----------------
# MAGIC Bhoomi Dasari       2009-02-06                Initial programming                                                                               3863                devlEDWcur                       Steph Goddard         02/12/2009
# MAGIC 
# MAGIC Rama Kamjula       2013-06-24                Converted from Server job to parallel job                                              5114               EnterpriseWrhsDevl           Bhoomi Dasari           11/28/2013
# MAGIC Kalyan Neelam       2013-11-18             Added new column CMPSS_COV_IN on end                                    5234 Compass    EnterpriseNewDevl            Bhoomi Dasari           11/28/2013

# MAGIC Extract PROD_ID
# MAGIC Extract ALPHA_PFX_CD and NM
# MAGIC lookup with CdMapping table to get code and name details based on given keys
# MAGIC Null Handling
# MAGIC Creates load file for CLS_PLN_DTL_I
# MAGIC Extract Data from CLS_PLN_DTL table
# MAGIC Extract  Data from CD_Mapping table
# MAGIC JobName: IdsEdwClsPlnDtlIExtr
# MAGIC 
# MAGIC This job joins tablesCLS_PLN_DTL, CLS_PLN, GRP, PCA_ADM, ALPHA_PFX and map with 
# MAGIC cd mapping table to get Code and Name from code mapping table.
# MAGIC Extract GRP_NM
# MAGIC Extract CLS_PLN_DESC
# MAGIC Extract Cls_Pln_Coverage related data from IDS
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


# Retrieve job parameters
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
EDWRunCycle = get_widget_value("EDWRunCycle","")
EDWRunCycleDate = get_widget_value("EDWRunCycleDate","")

# ----------------------------------------------------------------------------------------------------------------------
# Stage: db2_CLS_PLN_COV_RULE (DB2ConnectorPX)
# ----------------------------------------------------------------------------------------------------------------------
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query_db2_CLS_PLN_COV_RULE = f"""
SELECT 
CLS_PLN_COV_RULE_SK, 
CLS_PLN_COV_RULE_ID, 
DPNDT_STOP_EXCD_SK, 
SPOUSE_STOP_EXCD_SK, 
STDNT_STOP_EXCD_SK, 
SUB_STOP_EXCD_SK, 
CLS_PLN_COV_DPNDT_STOP_CD_SK, 
CLS_PLN_COV_SPOUSE_STOP_CD_SK, 
CLS_PLN_COV_STDNT_STOP_CD_SK, 
CLS_PLN_COV_SUB_STOP_CD_SK, 
CLS_PLN_COV_WTPRD_BEG_CD_SK, 
CLS_PLN_COV_WTPRD_BEGTYP_CD_SK, 
CLS_PLN_COV_WTPRD_TYP_CD_SK, 
DPNDT_STOP_AGE, 
SPOUSE_STOP_AGE, 
STDNT_STOP_AGE, 
SUB_STOP_AGE, 
WAIT_PERD_QTY, 
CLS_PLN_COV_RULE_DESC
FROM {IDSOwner}.CLS_PLN_COV_RULE
"""
df_db2_CLS_PLN_COV_RULE = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CLS_PLN_COV_RULE)
    .load()
)

# ----------------------------------------------------------------------------------------------------------------------
# Stage: db2_CLS_PLN_DTL (DB2ConnectorPX)
# ----------------------------------------------------------------------------------------------------------------------
extract_query_db2_CLS_PLN_DTL = f"""
SELECT 
CLS_PLN_DTL_SK, 
GRP_ID, 
CLS_ID, 
CLS_PLN_DTL_PROD_CAT_CD_SK, 
CLS_PLN_ID, 
EFF_DT_SK, 
CLS_PLN_DTL.CRT_RUN_CYC_EXCTN_SK, 
CLS_SK, 
CLS_PLN_SK, 
CLS_PLN_COV_RULE_SK, 
GRP_SK, PCA_ADM_SK, 
PROD_SK, 
CLS_PLN_DTL_COV_PROV_PFX_CD_SK, 
COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD, 
CLS_PLN_DTL_NTWK_SET_PFX_CD_SK, 
TERM_DT_SK, 
ALPHA_PFX_SK, 
CAROVR_PERD_BEG_MO, 
CLS_PLN_DTL_AGE_CALC_CD_SK, 
CLS_PLN_DTL_CE_BREAK_CD_SK, 
CLS_PLN_DTL_FMLY_CNTR_CD_SK, 
CLS_PLN_DTL_ID_CARD_TYP_CD_SK, 
CLS_PLN_DTL_RATE_GUAR_CD_SK, 
CLS_PLN_DTL_SELABLTY_IN, 
CONT_ENR_BREAK_ALW_DAYS, 
DR_CARD_BANK_RELSHP_PFX_ID, 
ENR_BEG_DT_MO_DAY, 
ENR_END_DT_MO_DAY, 
ID_CARD_STOCK_ID, 
PLN_BEG_DT_MO_DAY, 
PROD_VOL_RDUCTN_PFX_ID, 
RATE_GUAR_DT_SK, 
RATE_GUAR_PERD_MO, 
WARN_MSG_SEQ_NO,
CMPSS_COV_IN
FROM 
{IDSOwner}.CLS_PLN_DTL CLS_PLN_DTL
LEFT JOIN {IDSOwner}.CD_MPPNG CD 
    ON CLS_PLN_DTL.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
"""
df_db2_CLS_PLN_DTL = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CLS_PLN_DTL)
    .load()
)

# ----------------------------------------------------------------------------------------------------------------------
# Stage: db2_CLS_PLN (DB2ConnectorPX)
# ----------------------------------------------------------------------------------------------------------------------
extract_query_db2_CLS_PLN = f"""
SELECT 
CLS_PLN.CLS_PLN_SK,
CLS_PLN.CLS_PLN_DESC
FROM {IDSOwner}.CLS_PLN CLS_PLN
"""
df_db2_CLS_PLN = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CLS_PLN)
    .load()
)

# ----------------------------------------------------------------------------------------------------------------------
# Stage: db2_GRP (DB2ConnectorPX)
# ----------------------------------------------------------------------------------------------------------------------
extract_query_db2_GRP = f"""
SELECT 
GRP.GRP_SK,
GRP.GRP_NM
FROM
{IDSOwner}.GRP GRP
"""
df_db2_GRP = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_GRP)
    .load()
)

# ----------------------------------------------------------------------------------------------------------------------
# Stage: db2_PCA_ADM (DB2ConnectorPX)
# ----------------------------------------------------------------------------------------------------------------------
extract_query_db2_PCA_ADM = f"""
SELECT 
PCA_ADM_SK,
PCA_ADM_ID 
FROM {IDSOwner}.PCA_ADM
"""
df_db2_PCA_ADM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_PCA_ADM)
    .load()
)

# ----------------------------------------------------------------------------------------------------------------------
# Stage: db2_ALPHA_PFX (DB2ConnectorPX)
# ----------------------------------------------------------------------------------------------------------------------
extract_query_db2_ALPHA_PFX = f"""
SELECT 
ALPHA_PFX_SK,
ALPHA_PFX_CD,
ALPHA_PFX_NM 
FROM {IDSOwner}.ALPHA_PFX
"""
df_db2_ALPHA_PFX = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_ALPHA_PFX)
    .load()
)

# ----------------------------------------------------------------------------------------------------------------------
# Stage: db2_PROD (DB2ConnectorPX)
# ----------------------------------------------------------------------------------------------------------------------
extract_query_db2_PROD = f"""
SELECT 
PROD.PROD_SK,
PROD.PROD_ID 
FROM
{IDSOwner}.PROD PROD
"""
df_db2_PROD = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_PROD)
    .load()
)

# ----------------------------------------------------------------------------------------------------------------------
# Stage: lkp_codes (PxLookup): Primary link = df_db2_CLS_PLN_DTL, lookups = df_db2_CLS_PLN_COV_RULE, df_db2_CLS_PLN, df_db2_GRP, df_db2_PCA_ADM, df_db2_PROD, df_db2_ALPHA_PFX
# ----------------------------------------------------------------------------------------------------------------------
df_lkp_codes = (
    df_db2_CLS_PLN_DTL.alias("lnk_CLS_PLN_DTL_In")
    .join(
        df_db2_CLS_PLN_COV_RULE.alias("lnk_CLS_PLN_COV_RULE"),
        F.col("lnk_CLS_PLN_DTL_In.CLS_PLN_COV_RULE_SK") == F.col("lnk_CLS_PLN_COV_RULE.CLS_PLN_COV_RULE_SK"),
        how="left",
    )
    .join(
        df_db2_CLS_PLN.alias("lnk_CLS_PLN"),
        F.col("lnk_CLS_PLN_DTL_In.CLS_PLN_SK") == F.col("lnk_CLS_PLN.CLS_PLN_SK"),
        how="left",
    )
    .join(
        df_db2_GRP.alias("lnk_GRP"),
        F.col("lnk_CLS_PLN_DTL_In.GRP_SK") == F.col("lnk_GRP.GRP_SK"),
        how="left",
    )
    .join(
        df_db2_PCA_ADM.alias("lnk_PCA_ADM"),
        F.col("lnk_CLS_PLN_DTL_In.PCA_ADM_SK") == F.col("lnk_PCA_ADM.PCA_ADM_SK"),
        how="left",
    )
    .join(
        df_db2_PROD.alias("lnk_PROD"),
        F.col("lnk_CLS_PLN_DTL_In.PROD_SK") == F.col("lnk_PROD.PROD_SK"),
        how="left",
    )
    .join(
        df_db2_ALPHA_PFX.alias("lnk_ALPHA_PFX"),
        F.col("lnk_CLS_PLN_DTL_In.ALPHA_PFX_SK") == F.col("lnk_ALPHA_PFX.ALPHA_PFX_SK"),
        how="left",
    )
    .select(
        F.col("lnk_CLS_PLN_DTL_In.CLS_PLN_DTL_SK").alias("CLS_PLN_DTL_SK"),
        F.col("lnk_CLS_PLN_DTL_In.GRP_ID").alias("GRP_ID"),
        F.col("lnk_CLS_PLN_DTL_In.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnk_CLS_PLN_DTL_In.CLS_ID").alias("CLS_ID"),
        F.col("lnk_CLS_PLN_DTL_In.CLS_PLN_ID").alias("CLS_PLN_ID"),
        F.col("lnk_CLS_PLN_DTL_In.CLS_PLN_DTL_PROD_CAT_CD_SK").alias("CLS_PLN_DTL_PROD_CAT_CD_SK"),
        F.col("lnk_CLS_PLN_DTL_In.EFF_DT_SK").alias("EFF_DT_SK"),
        F.col("lnk_CLS_PLN_DTL_In.CLS_SK").alias("CLS_SK"),
        F.col("lnk_CLS_PLN_DTL_In.CLS_PLN_SK").alias("CLS_PLN_SK"),
        F.col("lnk_CLS_PLN_COV_RULE.DPNDT_STOP_EXCD_SK").alias("DPNDT_STOP_EXCD_SK"),
        F.col("lnk_CLS_PLN_COV_RULE.SPOUSE_STOP_EXCD_SK").alias("SPOUSE_STOP_EXCD_SK"),
        F.col("lnk_CLS_PLN_COV_RULE.STDNT_STOP_EXCD_SK").alias("STDNT_STOP_EXCD_SK"),
        F.col("lnk_CLS_PLN_COV_RULE.SUB_STOP_EXCD_SK").alias("SUB_STOP_EXCD_SK"),
        F.col("lnk_CLS_PLN_DTL_In.GRP_SK").alias("GRP_SK"),
        F.col("lnk_CLS_PLN_DTL_In.PCA_ADM_SK").alias("PCA_ADM_SK"),
        F.col("lnk_CLS_PLN_DTL_In.PROD_SK").alias("PROD_SK"),
        F.col("lnk_CLS_PLN_COV_RULE.CLS_PLN_COV_DPNDT_STOP_CD_SK").alias("CLS_PLN_COV_DPNDT_STOP_CD_SK"),
        F.col("lnk_CLS_PLN_COV_RULE.CLS_PLN_COV_SPOUSE_STOP_CD_SK").alias("CLS_PLN_COV_SPOUSE_STOP_CD_SK"),
        F.col("lnk_CLS_PLN_COV_RULE.CLS_PLN_COV_STDNT_STOP_CD_SK").alias("CLS_PLN_COV_STDNT_STOP_CD_SK"),
        F.col("lnk_CLS_PLN_COV_RULE.CLS_PLN_COV_SUB_STOP_CD_SK").alias("CLS_PLN_COV_SUB_STOP_CD_SK"),
        F.col("lnk_CLS_PLN_COV_RULE.CLS_PLN_COV_WTPRD_BEG_CD_SK").alias("CLS_PLN_COV_WTPRD_BEG_CD_SK"),
        F.col("lnk_CLS_PLN_COV_RULE.CLS_PLN_COV_WTPRD_BEGTYP_CD_SK").alias("CLS_PLN_COV_WTPRD_BEGTYP_CD_SK"),
        F.col("lnk_CLS_PLN_COV_RULE.CLS_PLN_COV_WTPRD_TYP_CD_SK").alias("CLS_PLN_COV_WTPRD_TYP_CD_SK"),
        F.col("lnk_CLS_PLN_COV_RULE.DPNDT_STOP_AGE").alias("DPNDT_STOP_AGE"),
        F.col("lnk_CLS_PLN_COV_RULE.CLS_PLN_COV_RULE_DESC").alias("CLS_PLN_COV_RULE_DESC"),
        F.col("lnk_CLS_PLN_COV_RULE.CLS_PLN_COV_RULE_ID").alias("CLS_PLN_COV_RULE_ID"),
        F.col("lnk_CLS_PLN_COV_RULE.SPOUSE_STOP_AGE").alias("SPOUSE_STOP_AGE"),
        F.col("lnk_CLS_PLN_COV_RULE.STDNT_STOP_AGE").alias("STDNT_STOP_AGE"),
        F.col("lnk_CLS_PLN_COV_RULE.SUB_STOP_AGE").alias("SUB_STOP_AGE"),
        F.col("lnk_CLS_PLN_COV_RULE.WAIT_PERD_QTY").alias("WAIT_PERD_QTY"),
        F.col("lnk_CLS_PLN_DTL_In.CLS_PLN_DTL_COV_PROV_PFX_CD_SK").alias("CLS_PLN_DTL_COV_PROV_PFX_CD_SK"),
        F.col("lnk_CLS_PLN_DTL_In.CLS_PLN_DTL_NTWK_SET_PFX_CD_SK").alias("CLS_PLN_DTL_NTWK_SET_PFX_CD_SK"),
        F.col("lnk_CLS_PLN_DTL_In.TERM_DT_SK").alias("TERM_DT_SK"),
        F.col("lnk_CLS_PLN.CLS_PLN_DESC").alias("CLS_PLN_DESC"),
        F.col("lnk_GRP.GRP_NM").alias("GRP_NM"),
        F.col("lnk_PCA_ADM.PCA_ADM_ID").alias("PCA_ADM_ID"),
        F.col("lnk_PROD.PROD_ID").alias("PROD_ID"),
        F.col("lnk_CLS_PLN_DTL_In.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("lnk_ALPHA_PFX.ALPHA_PFX_CD").alias("ALPHA_PFX_CD"),
        F.col("lnk_ALPHA_PFX.ALPHA_PFX_NM").alias("ALPHA_PFX_NM"),
        F.col("lnk_CLS_PLN_DTL_In.ALPHA_PFX_SK").alias("ALPHA_PFX_SK"),
        F.col("lnk_CLS_PLN_DTL_In.CLS_PLN_DTL_AGE_CALC_CD_SK").alias("CLS_PLN_DTL_AGE_CALC_CD_SK"),
        F.col("lnk_CLS_PLN_DTL_In.CAROVR_PERD_BEG_MO").alias("CAROVR_PERD_BEG_MO"),
        F.col("lnk_CLS_PLN_DTL_In.CONT_ENR_BREAK_ALW_DAYS").alias("CONT_ENR_BREAK_ALW_DAYS"),
        F.col("lnk_CLS_PLN_DTL_In.CLS_PLN_DTL_CE_BREAK_CD_SK").alias("CLS_PLN_DTL_CE_BREAK_CD_SK"),
        F.col("lnk_CLS_PLN_DTL_In.ENR_BEG_DT_MO_DAY").alias("ENR_BEG_DT_MO_DAY"),
        F.col("lnk_CLS_PLN_DTL_In.ENR_END_DT_MO_DAY").alias("ENR_END_DT_MO_DAY"),
        F.col("lnk_CLS_PLN_DTL_In.CLS_PLN_DTL_FMLY_CNTR_CD_SK").alias("CLS_PLN_DTL_FMLY_CNTR_CD_SK"),
        F.col("lnk_CLS_PLN_DTL_In.ID_CARD_STOCK_ID").alias("ID_CARD_STOCK_ID"),
        F.col("lnk_CLS_PLN_DTL_In.CLS_PLN_DTL_ID_CARD_TYP_CD_SK").alias("CLS_PLN_DTL_ID_CARD_TYP_CD_SK"),
        F.col("lnk_CLS_PLN_DTL_In.PLN_BEG_DT_MO_DAY").alias("PLN_BEG_DT_MO_DAY"),
        F.col("lnk_CLS_PLN_DTL_In.CLS_PLN_DTL_RATE_GUAR_CD_SK").alias("CLS_PLN_DTL_RATE_GUAR_CD_SK"),
        F.col("lnk_CLS_PLN_DTL_In.RATE_GUAR_DT_SK").alias("RATE_GUAR_DT_SK"),
        F.col("lnk_CLS_PLN_DTL_In.RATE_GUAR_PERD_MO").alias("RATE_GUAR_PERD_MO"),
        F.col("lnk_CLS_PLN_DTL_In.CLS_PLN_DTL_SELABLTY_IN").alias("CLS_PLN_DTL_SELABLTY_IN"),
        F.col("lnk_CLS_PLN_DTL_In.WARN_MSG_SEQ_NO").alias("WARN_MSG_SEQ_NO"),
        F.col("lnk_CLS_PLN_DTL_In.DR_CARD_BANK_RELSHP_PFX_ID").alias("DR_CARD_BANK_RELSHP_PFX_ID"),
        F.col("lnk_CLS_PLN_DTL_In.PROD_VOL_RDUCTN_PFX_ID").alias("PROD_VOL_RDUCTN_PFX_ID"),
        F.col("lnk_CLS_PLN_DTL_In.CMPSS_COV_IN").alias("CMPSS_COV_IN"),
    )
)

# ----------------------------------------------------------------------------------------------------------------------
# Stage: db2_CD_MPPNG (DB2ConnectorPX)
# ----------------------------------------------------------------------------------------------------------------------
extract_query_db2_CD_MPPNG = f"""
SELECT
CD.CD_MPPNG_SK,
COALESCE(CD.TRGT_CD,'UNK') TRGT_CD,
CD.TRGT_CD_NM
FROM 
{IDSOwner}.CD_MPPNG CD
"""
df_db2_CD_MPPNG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CD_MPPNG)
    .load()
)

# ----------------------------------------------------------------------------------------------------------------------
# Stage: cpy_CD_MPPNG (PxCopy) - multiple output links from the same input
# ----------------------------------------------------------------------------------------------------------------------
# lnk_ClsPlnDtlSk_In
df_cpy_CD_MPPNG_ClsPlnDtlSk_In = df_db2_CD_MPPNG.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

# lnk_CovDpndtStopCd_In
df_cpy_CD_MPPNG_CovDpndtStopCd_In = df_db2_CD_MPPNG.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD_CovDpndtStopCd"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM_CovDpndtStopNm")
)

# lnk_CovSpouseStopCd_In
df_cpy_CD_MPPNG_CovSpouseStopCd_In = df_db2_CD_MPPNG.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD_CovSpouseStopCd"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM_CovSpouseStopNm")
)

# lnk_CovStdntStopCd_In
df_cpy_CD_MPPNG_CovStdntStopCd_In = df_db2_CD_MPPNG.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD_CovStdntStopCd"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM_CovStdntStopNm")
)

# lnk_CovSubStopCd_In
df_cpy_CD_MPPNG_CovSubStopCd_In = df_db2_CD_MPPNG.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD_CovSubStopCd"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM_CovSubStopNm")
)

# lnk_CovWtprdBegCd_In
df_cpy_CD_MPPNG_CovWtprdBegCd_In = df_db2_CD_MPPNG.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD_CovWtprdBegCd"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM_CovWtprdBegNm")
)

# lnk_CovWtprdBegtypCd_In
df_cpy_CD_MPPNG_CovWtprdBegtypCd_In = df_db2_CD_MPPNG.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD_CovWtprdBegtypCd"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM_CovWtprdBegtypNm")
)

# lnk_CovWtprdTypCd_In
df_cpy_CD_MPPNG_CovWtprdTypCd_In = df_db2_CD_MPPNG.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD_CovWtprdTypCd"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM_CovWtprdTypNm")
)

# lnk_ClsPlnDtlRateGuardCdSk_In
df_cpy_CD_MPPNG_ClsPlnDtlRateGuardCdSk_In = df_db2_CD_MPPNG.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD_ClsPlnDtlRateGuardCd"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM_ClsPlnDtlRateGuardNm")
)

# lnk_ClsPlnDtlIdCardTypCdSk_In
df_cpy_CD_MPPNG_ClsPlnDtlIdCardTypCdSk_In = df_db2_CD_MPPNG.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD_ClsPlnDtlIdCardTypCd"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM_ClsPlnDtlIdCardTypNm")
)

# lnk_ClsPlnDtlCeBreakCdSk_In
df_cpy_CD_MPPNG_ClsPlnDtlCeBreakCdSk_In = df_db2_CD_MPPNG.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD_ClsPlnDtlCeBreakCd"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM_ClsPlnDtlCeBreakNm")
)

# lnk_ClsPlnDtlFmlyCntlCdSk_In
df_cpy_CD_MPPNG_ClsPlnDtlFmlyCntlCdSk_In = df_db2_CD_MPPNG.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD_ClsPlnDtlFmlyCntlCd"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM_ClsPlnDtlFmlyCntlNm")
)

# lnk_ClsPlnDtlAgeCalcCdSk_In
df_cpy_CD_MPPNG_ClsPlnDtlAgeCalcCdSk_In = df_db2_CD_MPPNG.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD_ClsPlnDtlAgeCalcCd"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM_ClsPlnDtlAgeCalcNm")
)

# lnk_ClsPlnDtlNtwkSetPfxCdSk_In
df_cpy_CD_MPPNG_ClsPlnDtlNtwkSetPfxCdSk_In = df_db2_CD_MPPNG.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD_ClsPlnDtlNtwkSetPfxCd"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM_ClsPlnDtlNtwkSetPfxNm")
)

# lnk_ClsPlnDtlCovProvPfxCdSk_In
df_cpy_CD_MPPNG_ClsPlnDtlCovProvPfxCdSk_In = df_db2_CD_MPPNG.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD_ClsPlnDtlCovProvPfxCd"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM_ClsPlnDtlCovProvPfxNm")
)

# ----------------------------------------------------------------------------------------------------------------------
# Stage: lkp_CdMppng (PxLookup): Primary link = df_lkp_codes, lookups from each of the cpy_CD_MPPNG dataframes
# ----------------------------------------------------------------------------------------------------------------------
df_lkp_CdMppng = (
    df_lkp_codes.alias("lnk_CLS_PLN_DTL_Out")
    .join(
        df_cpy_CD_MPPNG_ClsPlnDtlSk_In.alias("lnk_ClsPlnDtlSk_In"),
        F.col("lnk_CLS_PLN_DTL_Out.CLS_PLN_DTL_PROD_CAT_CD_SK") == F.col("lnk_ClsPlnDtlSk_In.CD_MPPNG_SK"),
        how="left",
    )
    .join(
        df_cpy_CD_MPPNG_CovDpndtStopCd_In.alias("lnk_CovDpndtStopCd_In"),
        F.col("lnk_CLS_PLN_DTL_Out.CLS_PLN_COV_DPNDT_STOP_CD_SK") == F.col("lnk_CovDpndtStopCd_In.CD_MPPNG_SK"),
        how="left",
    )
    .join(
        df_cpy_CD_MPPNG_CovSpouseStopCd_In.alias("lnk_CovSpouseStopCd_In"),
        F.col("lnk_CLS_PLN_DTL_Out.CLS_PLN_COV_SPOUSE_STOP_CD_SK") == F.col("lnk_CovSpouseStopCd_In.CD_MPPNG_SK"),
        how="left",
    )
    .join(
        df_cpy_CD_MPPNG_CovStdntStopCd_In.alias("lnk_CovStdntStopCd_In"),
        F.col("lnk_CLS_PLN_DTL_Out.CLS_PLN_COV_STDNT_STOP_CD_SK") == F.col("lnk_CovStdntStopCd_In.CD_MPPNG_SK"),
        how="left",
    )
    .join(
        df_cpy_CD_MPPNG_CovSubStopCd_In.alias("lnk_CovSubStopCd_In"),
        F.col("lnk_CLS_PLN_DTL_Out.CLS_PLN_COV_SUB_STOP_CD_SK") == F.col("lnk_CovSubStopCd_In.CD_MPPNG_SK"),
        how="left",
    )
    .join(
        df_cpy_CD_MPPNG_CovWtprdBegCd_In.alias("lnk_CovWtprdBegCd_In"),
        F.col("lnk_CLS_PLN_DTL_Out.CLS_PLN_COV_WTPRD_BEG_CD_SK") == F.col("lnk_CovWtprdBegCd_In.CD_MPPNG_SK"),
        how="left",
    )
    .join(
        df_cpy_CD_MPPNG_CovWtprdBegtypCd_In.alias("lnk_CovWtprdBegtypCd_In"),
        F.col("lnk_CLS_PLN_DTL_Out.CLS_PLN_COV_WTPRD_BEGTYP_CD_SK") == F.col("lnk_CovWtprdBegtypCd_In.CD_MPPNG_SK"),
        how="left",
    )
    .join(
        df_cpy_CD_MPPNG_CovWtprdTypCd_In.alias("lnk_CovWtprdTypCd_In"),
        F.col("lnk_CLS_PLN_DTL_Out.CLS_PLN_COV_WTPRD_TYP_CD_SK") == F.col("lnk_CovWtprdTypCd_In.CD_MPPNG_SK"),
        how="left",
    )
    .join(
        df_cpy_CD_MPPNG_ClsPlnDtlRateGuardCdSk_In.alias("lnk_ClsPlnDtlRateGuardCdSk_In"),
        F.col("lnk_CLS_PLN_DTL_Out.CLS_PLN_DTL_RATE_GUAR_CD_SK") == F.col("lnk_ClsPlnDtlRateGuardCdSk_In.CD_MPPNG_SK"),
        how="left",
    )
    .join(
        df_cpy_CD_MPPNG_ClsPlnDtlIdCardTypCdSk_In.alias("lnk_ClsPlnDtlIdCardTypCdSk_In"),
        F.col("lnk_CLS_PLN_DTL_Out.CLS_PLN_DTL_ID_CARD_TYP_CD_SK") == F.col("lnk_ClsPlnDtlIdCardTypCdSk_In.CD_MPPNG_SK"),
        how="left",
    )
    .join(
        df_cpy_CD_MPPNG_ClsPlnDtlCeBreakCdSk_In.alias("lnk_ClsPlnDtlCeBreakCdSk_In"),
        F.col("lnk_CLS_PLN_DTL_Out.CLS_PLN_DTL_CE_BREAK_CD_SK") == F.col("lnk_ClsPlnDtlCeBreakCdSk_In.CD_MPPNG_SK"),
        how="left",
    )
    .join(
        df_cpy_CD_MPPNG_ClsPlnDtlFmlyCntlCdSk_In.alias("lnk_ClsPlnDtlFmlyCntlCdSk_In"),
        F.col("lnk_CLS_PLN_DTL_Out.CLS_PLN_DTL_FMLY_CNTR_CD_SK") == F.col("lnk_ClsPlnDtlFmlyCntlCdSk_In.CD_MPPNG_SK"),
        how="left",
    )
    .join(
        df_cpy_CD_MPPNG_ClsPlnDtlAgeCalcCdSk_In.alias("lnk_ClsPlnDtlAgeCalcCdSk_In"),
        F.col("lnk_CLS_PLN_DTL_Out.CLS_PLN_DTL_AGE_CALC_CD_SK") == F.col("lnk_ClsPlnDtlAgeCalcCdSk_In.CD_MPPNG_SK"),
        how="left",
    )
    .join(
        df_cpy_CD_MPPNG_ClsPlnDtlNtwkSetPfxCdSk_In.alias("lnk_ClsPlnDtlNtwkSetPfxCdSk_In"),
        F.col("lnk_CLS_PLN_DTL_Out.CLS_PLN_DTL_NTWK_SET_PFX_CD_SK") == F.col("lnk_ClsPlnDtlNtwkSetPfxCdSk_In.CD_MPPNG_SK"),
        how="left",
    )
    .join(
        df_cpy_CD_MPPNG_ClsPlnDtlCovProvPfxCdSk_In.alias("lnk_ClsPlnDtlCovProvPfxCdSk_In"),
        F.col("lnk_CLS_PLN_DTL_Out.CLS_PLN_DTL_COV_PROV_PFX_CD_SK") == F.col("lnk_ClsPlnDtlCovProvPfxCdSk_In.CD_MPPNG_SK"),
        how="left",
    )
    .select(
        # Primary Key
        F.col("lnk_CLS_PLN_DTL_Out.CLS_PLN_DTL_SK").alias("CLS_PLN_DTL_SK"),
        F.col("lnk_CLS_PLN_DTL_Out.GRP_ID").alias("GRP_ID"),
        F.col("lnk_CLS_PLN_DTL_Out.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnk_CLS_PLN_DTL_Out.CLS_ID").alias("CLS_ID"),
        F.col("lnk_CLS_PLN_DTL_Out.CLS_PLN_ID").alias("CLS_PLN_ID"),
        F.col("lnk_CLS_PLN_DTL_Out.CLS_PLN_DTL_PROD_CAT_CD_SK").alias("CLS_PLN_DTL_PROD_CAT_CD_SK"),
        F.col("lnk_CLS_PLN_DTL_Out.EFF_DT_SK").alias("EFF_DT_SK"),
        F.col("lnk_CLS_PLN_DTL_Out.CLS_SK").alias("CLS_SK"),
        F.col("lnk_CLS_PLN_DTL_Out.CLS_PLN_SK").alias("CLS_PLN_SK"),
        F.col("lnk_CLS_PLN_DTL_Out.DPNDT_STOP_EXCD_SK").alias("DPNDT_STOP_EXCD_SK"),
        F.col("lnk_CLS_PLN_DTL_Out.SPOUSE_STOP_EXCD_SK").alias("SPOUSE_STOP_EXCD_SK"),
        F.col("lnk_CLS_PLN_DTL_Out.STDNT_STOP_EXCD_SK").alias("STDNT_STOP_EXCD_SK"),
        F.col("lnk_CLS_PLN_DTL_Out.SUB_STOP_EXCD_SK").alias("SUB_STOP_EXCD_SK"),
        F.col("lnk_CLS_PLN_DTL_Out.GRP_SK").alias("GRP_SK"),
        F.col("lnk_CLS_PLN_DTL_Out.PCA_ADM_SK").alias("PCA_ADM_SK"),
        F.col("lnk_CLS_PLN_DTL_Out.PROD_SK").alias("PROD_SK"),
        F.col("lnk_CLS_PLN_DTL_Out.CLS_PLN_COV_DPNDT_STOP_CD_SK").alias("CLS_PLN_COV_DPNDT_STOP_CD_SK"),
        F.col("lnk_CLS_PLN_DTL_Out.CLS_PLN_COV_SPOUSE_STOP_CD_SK").alias("CLS_PLN_COV_SPOUSE_STOP_CD_SK"),
        F.col("lnk_CLS_PLN_DTL_Out.CLS_PLN_COV_STDNT_STOP_CD_SK").alias("CLS_PLN_COV_STDNT_STOP_CD_SK"),
        F.col("lnk_CLS_PLN_DTL_Out.CLS_PLN_COV_SUB_STOP_CD_SK").alias("CLS_PLN_COV_SUB_STOP_CD_SK"),
        F.col("lnk_CLS_PLN_DTL_Out.CLS_PLN_COV_WTPRD_BEG_CD_SK").alias("CLS_PLN_COV_WTPRD_BEG_CD_SK"),
        F.col("lnk_CLS_PLN_DTL_Out.CLS_PLN_COV_WTPRD_BEGTYP_CD_SK").alias("CLS_PLN_COV_WTPRD_BEGTYP_CD_SK"),
        F.col("lnk_CLS_PLN_DTL_Out.CLS_PLN_COV_WTPRD_TYP_CD_SK").alias("CLS_PLN_COV_WTPRD_TYP_CD_SK"),
        F.col("lnk_CLS_PLN_DTL_Out.DPNDT_STOP_AGE").alias("DPNDT_STOP_AGE"),
        F.col("lnk_CLS_PLN_DTL_Out.CLS_PLN_COV_RULE_DESC").alias("CLS_PLN_COV_RULE_DESC"),
        F.col("lnk_CLS_PLN_DTL_Out.CLS_PLN_COV_RULE_ID").alias("CLS_PLN_COV_RULE_ID"),
        F.col("lnk_CLS_PLN_DTL_Out.SPOUSE_STOP_AGE").alias("SPOUSE_STOP_AGE"),
        F.col("lnk_CLS_PLN_DTL_Out.STDNT_STOP_AGE").alias("STDNT_STOP_AGE"),
        F.col("lnk_CLS_PLN_DTL_Out.SUB_STOP_AGE").alias("SUB_STOP_AGE"),
        F.col("lnk_CLS_PLN_DTL_Out.WAIT_PERD_QTY").alias("WAIT_PERD_QTY"),
        F.col("lnk_CLS_PLN_DTL_Out.CLS_PLN_DTL_COV_PROV_PFX_CD_SK").alias("CLS_PLN_DTL_COV_PROV_PFX_CD_SK"),
        F.col("lnk_CLS_PLN_DTL_Out.CLS_PLN_DTL_NTWK_SET_PFX_CD_SK").alias("CLS_PLN_DTL_NTWK_SET_PFX_CD_SK"),
        F.col("lnk_CLS_PLN_DTL_Out.TERM_DT_SK").alias("TERM_DT_SK"),
        F.col("lnk_CLS_PLN_DTL_Out.CLS_PLN_DESC").alias("CLS_PLN_DESC"),
        F.col("lnk_CLS_PLN_DTL_Out.GRP_NM").alias("GRP_NM"),
        F.col("lnk_CLS_PLN_DTL_Out.PCA_ADM_ID").alias("PCA_ADM_ID"),
        F.col("lnk_CLS_PLN_DTL_Out.PROD_ID").alias("PROD_ID"),
        F.col("lnk_CLS_PLN_DTL_Out.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("lnk_CLS_PLN_DTL_Out.ALPHA_PFX_CD").alias("ALPHA_PFX_CD"),
        F.col("lnk_CLS_PLN_DTL_Out.ALPHA_PFX_NM").alias("ALPHA_PFX_NM"),
        F.col("lnk_CLS_PLN_DTL_Out.ALPHA_PFX_SK").alias("ALPHA_PFX_SK"),
        F.col("lnk_CLS_PLN_DTL_Out.CLS_PLN_DTL_AGE_CALC_CD_SK").alias("CLS_PLN_DTL_AGE_CALC_CD_SK"),
        F.col("lnk_CLS_PLN_DTL_Out.CAROVR_PERD_BEG_MO").alias("CAROVR_PERD_BEG_MO"),
        F.col("lnk_CLS_PLN_DTL_Out.CONT_ENR_BREAK_ALW_DAYS").alias("CONT_ENR_BREAK_ALW_DAYS"),
        F.col("lnk_CLS_PLN_DTL_Out.CLS_PLN_DTL_CE_BREAK_CD_SK").alias("CLS_PLN_DTL_CE_BREAK_CD_SK"),
        F.col("lnk_CLS_PLN_DTL_Out.ENR_BEG_DT_MO_DAY").alias("ENR_BEG_DT_MO_DAY"),
        F.col("lnk_CLS_PLN_DTL_Out.ENR_END_DT_MO_DAY").alias("ENR_END_DT_MO_DAY"),
        F.col("lnk_CLS_PLN_DTL_Out.CLS_PLN_DTL_FMLY_CNTR_CD_SK").alias("CLS_PLN_DTL_FMLY_CNTR_CD_SK"),
        F.col("lnk_CLS_PLN_DTL_Out.ID_CARD_STOCK_ID").alias("ID_CARD_STOCK_ID"),
        F.col("lnk_CLS_PLN_DTL_Out.CLS_PLN_DTL_ID_CARD_TYP_CD_SK").alias("CLS_PLN_DTL_ID_CARD_TYP_CD_SK"),
        F.col("lnk_CLS_PLN_DTL_Out.PLN_BEG_DT_MO_DAY").alias("PLN_BEG_DT_MO_DAY"),
        F.col("lnk_CLS_PLN_DTL_Out.CLS_PLN_DTL_RATE_GUAR_CD_SK").alias("CLS_PLN_DTL_RATE_GUAR_CD_SK"),
        F.col("lnk_CLS_PLN_DTL_Out.RATE_GUAR_DT_SK").alias("RATE_GUAR_DT_SK"),
        F.col("lnk_CLS_PLN_DTL_Out.RATE_GUAR_PERD_MO").alias("RATE_GUAR_PERD_MO"),
        F.col("lnk_CLS_PLN_DTL_Out.CLS_PLN_DTL_SELABLTY_IN").alias("CLS_PLN_DTL_SELABLTY_IN"),
        F.col("lnk_CLS_PLN_DTL_Out.WARN_MSG_SEQ_NO").alias("WARN_MSG_SEQ_NO"),
        F.col("lnk_CLS_PLN_DTL_Out.DR_CARD_BANK_RELSHP_PFX_ID").alias("DR_CARD_BANK_RELSHP_PFX_ID"),
        F.col("lnk_CLS_PLN_DTL_Out.PROD_VOL_RDUCTN_PFX_ID").alias("PROD_VOL_RDUCTN_PFX_ID"),
        F.col("lnk_ClsPlnDtlSk_In.TRGT_CD").alias("TRGT_CD"),
        F.col("lnk_ClsPlnDtlSk_In.TRGT_CD_NM").alias("TRGT_CD_NM"),
        F.col("lnk_CovDpndtStopCd_In.TRGT_CD_CovDpndtStopCd").alias("TRGT_CD_CovDpndtStopCd"),
        F.col("lnk_CovDpndtStopCd_In.TRGT_CD_NM_CovDpndtStopNm").alias("TRGT_CD_NM_CovDpndtStopNm"),
        F.col("lnk_CovSpouseStopCd_In.TRGT_CD_CovSpouseStopCd").alias("TRGT_CD_CovSpouseStopCd"),
        F.col("lnk_CovSpouseStopCd_In.TRGT_CD_NM_CovSpouseStopNm").alias("TRGT_CD_NM_CovSpouseStopNm"),
        F.col("lnk_CovStdntStopCd_In.TRGT_CD_CovStdntStopCd").alias("TRGT_CD_CovStdntStopCd"),
        F.col("lnk_CovStdntStopCd_In.TRGT_CD_NM_CovStdntStopNm").alias("TRGT_CD_NM_CovStdntStopNm"),
        F.col("lnk_CovSubStopCd_In.TRGT_CD_CovSubStopCd").alias("TRGT_CD_CovSubStopCd"),
        F.col("lnk_CovSubStopCd_In.TRGT_CD_NM_CovSubStopNm").alias("TRGT_CD_NM_CovSubStopNm"),
        F.col("lnk_CovWtprdBegCd_In.TRGT_CD_CovWtprdBegCd").alias("TRGT_CD_CovWtprdBegCd"),
        F.col("lnk_CovWtprdBegCd_In.TRGT_CD_NM_CovWtprdBegNm").alias("TRGT_CD_NM_CovWtprdBegNm"),
        F.col("lnk_CovWtprdBegtypCd_In.TRGT_CD_CovWtprdBegtypCd").alias("TRGT_CD_CovWtprdBegtypCd"),
        F.col("lnk_CovWtprdBegtypCd_In.TRGT_CD_NM_CovWtprdBegtypNm").alias("TRGT_CD_NM_CovWtprdBegtypNm"),
        F.col("lnk_CovWtprdTypCd_In.TRGT_CD_CovWtprdTypCd").alias("TRGT_CD_CovWtprdTypCd"),
        F.col("lnk_CovWtprdTypCd_In.TRGT_CD_NM_CovWtprdTypNm").alias("TRGT_CD_NM_CovWtprdTypNm"),
        F.col("lnk_ClsPlnDtlCovProvPfxCdSk_In.TRGT_CD_ClsPlnDtlCovProvPfxCd").alias("TRGT_CD_ClsPlnDtlCovProvPfxCd"),
        F.col("lnk_ClsPlnDtlCovProvPfxCdSk_In.TRGT_CD_NM_ClsPlnDtlCovProvPfxNm").alias("TRGT_CD_NM_ClsPlnDtlCovProvPfxNm"),
        F.col("lnk_ClsPlnDtlNtwkSetPfxCdSk_In.TRGT_CD_ClsPlnDtlNtwkSetPfxCd").alias("TRGT_CD_ClsPlnDtlNtwkSetPfxCd"),
        F.col("lnk_ClsPlnDtlNtwkSetPfxCdSk_In.TRGT_CD_NM_ClsPlnDtlNtwkSetPfxNm").alias("TRGT_CD_NM_ClsPlnDtlNtwkSetPfxNm"),
        F.col("lnk_ClsPlnDtlAgeCalcCdSk_In.TRGT_CD_ClsPlnDtlAgeCalcCd").alias("TRGT_CD_ClsPlnDtlAgeCalcCd"),
        F.col("lnk_ClsPlnDtlAgeCalcCdSk_In.TRGT_CD_NM_ClsPlnDtlAgeCalcNm").alias("TRGT_CD_NM_ClsPlnDtlAgeCalcNm"),
        F.col("lnk_ClsPlnDtlFmlyCntlCdSk_In.TRGT_CD_ClsPlnDtlFmlyCntlCd").alias("TRGT_CD_ClsPlnDtlFmlyCntlCd"),
        F.col("lnk_ClsPlnDtlFmlyCntlCdSk_In.TRGT_CD_NM_ClsPlnDtlFmlyCntlNm").alias("TRGT_CD_NM_ClsPlnDtlFmlyCntlNm"),
        F.col("lnk_ClsPlnDtlCeBreakCdSk_In.TRGT_CD_ClsPlnDtlCeBreakCd").alias("TRGT_CD_ClsPlnDtlCeBreakCd"),
        F.col("lnk_ClsPlnDtlCeBreakCdSk_In.TRGT_CD_NM_ClsPlnDtlCeBreakNm").alias("TRGT_CD_NM_ClsPlnDtlCeBreakNm"),
        F.col("lnk_ClsPlnDtlIdCardTypCdSk_In.TRGT_CD_ClsPlnDtlIdCardTypCd").alias("TRGT_CD_ClsPlnDtlIdCardTypCd"),
        F.col("lnk_ClsPlnDtlIdCardTypCdSk_In.TRGT_CD_NM_ClsPlnDtlIdCardTypNm").alias("TRGT_CD_NM_ClsPlnDtlIdCardTypNm"),
        F.col("lnk_ClsPlnDtlRateGuardCdSk_In.TRGT_CD_ClsPlnDtlRateGuardCd").alias("TRGT_CD_ClsPlnDtlRateGuardCd"),
        F.col("lnk_ClsPlnDtlRateGuardCdSk_In.TRGT_CD_NM_ClsPlnDtlRateGuardNm").alias("TRGT_CD_NM_ClsPlnDtlRateGuardNm"),
        F.col("lnk_CLS_PLN_DTL_Out.CMPSS_COV_IN").alias("CMPSS_COV_IN"),
    )
)

# ----------------------------------------------------------------------------------------------------------------------
# Stage: xfm_BusinessLogic (CTransformerStage)
# ----------------------------------------------------------------------------------------------------------------------
# Note: We transform columns per the expressions in the stage. Using trim(...) and F.when(F.col(...).isNull(),...) patterns.
df_xfm_BusinessLogic_pre = df_lkp_CdMppng.withColumn(
    "SRC_SYS_CD",
    trim(F.col("SRC_SYS_CD"))
).withColumn(
    "CLS_PLN_DTL_PROD_CAT_CD",
    F.when(F.col("TRGT_CD").isNull(), F.lit("UNK")).otherwise(F.col("TRGT_CD"))
).withColumn(
    "CLS_PLN_DTL_EFF_DT_SK",
    F.col("EFF_DT_SK")
).withColumn(
    "CRT_RUN_CYC_EXCTN_DT_SK",
    F.lit(EDWRunCycleDate)
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    F.lit(EDWRunCycleDate)
).withColumn(
    "CLS_PLN_COV_DPNDT_STOP_EXCD_SK",
    F.when(F.col("DPNDT_STOP_EXCD_SK").isNull(), F.lit(0)).otherwise(F.col("DPNDT_STOP_EXCD_SK"))
).withColumn(
    "CLS_PLN_COV_SPOUS_STOP_EXCD_SK",
    F.when(F.col("SPOUSE_STOP_EXCD_SK").isNull(), F.lit(0)).otherwise(F.col("SPOUSE_STOP_EXCD_SK"))
).withColumn(
    "CLS_PLN_COV_STDNT_STOP_EXCD_SK",
    F.when(F.col("STDNT_STOP_EXCD_SK").isNull(), F.lit(0)).otherwise(F.col("STDNT_STOP_EXCD_SK"))
).withColumn(
    "CLS_PLN_COV_SUB_STOP_EXCD_SK",
    F.when(F.col("SUB_STOP_EXCD_SK").isNull(), F.lit(0)).otherwise(F.col("SUB_STOP_EXCD_SK"))
).withColumn(
    "CLS_PLN_COV_DPNDT_STOP_CD",
    F.when(F.col("TRGT_CD_CovDpndtStopCd").isNull(), F.lit("UNK")).otherwise(F.col("TRGT_CD_CovDpndtStopCd"))
).withColumn(
    "CLS_PLN_COV_DPNDT_STOP_NM",
    F.when(F.col("TRGT_CD_NM_CovDpndtStopNm").isNull(), F.lit("UNK")).otherwise(F.col("TRGT_CD_NM_CovDpndtStopNm"))
).withColumn(
    "CLS_PLN_COV_SPOUSE_STOP_CD",
    F.when(F.col("TRGT_CD_CovSpouseStopCd").isNull(), F.lit("UNK")).otherwise(F.col("TRGT_CD_CovSpouseStopCd"))
).withColumn(
    "CLS_PLN_COV_SPOUSE_STOP_NM",
    F.when(F.col("TRGT_CD_NM_CovSpouseStopNm").isNull(), F.lit("UNK")).otherwise(F.col("TRGT_CD_NM_CovSpouseStopNm"))
).withColumn(
    "CLS_PLN_COV_STDNT_STOP_CD",
    F.when(F.col("TRGT_CD_CovStdntStopCd").isNull(), F.lit("UNK")).otherwise(F.col("TRGT_CD_CovStdntStopCd"))
).withColumn(
    "CLS_PLN_COV_STDNT_STOP_NM",
    F.when(F.col("TRGT_CD_NM_CovStdntStopNm").isNull(), F.lit("UNK")).otherwise(F.col("TRGT_CD_NM_CovStdntStopNm"))
).withColumn(
    "CLS_PLN_COV_SUB_STOP_EVT_CD",
    F.when(F.col("TRGT_CD_CovSubStopCd").isNull(), F.lit("UNK")).otherwise(F.col("TRGT_CD_CovSubStopCd"))
).withColumn(
    "CLS_PLN_COV_SUB_STOP_EVT_NM",
    F.when(F.col("TRGT_CD_NM_CovSubStopNm").isNull(), F.lit("UNK")).otherwise(F.col("TRGT_CD_NM_CovSubStopNm"))
).withColumn(
    "CLS_PLN_COV_WTPRD_BEG_CD",
    F.when(F.col("TRGT_CD_CovWtprdBegCd").isNull(), F.lit("UNK")).otherwise(F.col("TRGT_CD_CovWtprdBegCd"))
).withColumn(
    "CLS_PLN_COV_WTPRD_BEG_NM",
    F.when(F.col("TRGT_CD_NM_CovWtprdBegNm").isNull(), F.lit("UNK")).otherwise(F.col("TRGT_CD_NM_CovWtprdBegNm"))
).withColumn(
    "CLS_PLN_COV_WTPRD_BEGTYP_CD",
    F.when(F.col("TRGT_CD_CovWtprdBegtypCd").isNull(), F.lit("UNK")).otherwise(F.col("TRGT_CD_CovWtprdBegtypCd"))
).withColumn(
    "CLS_PLN_COV_WTPRD_BEGTYP_NM",
    F.when(F.col("TRGT_CD_NM_CovWtprdBegtypNm").isNull(), F.lit("UNK")).otherwise(F.col("TRGT_CD_NM_CovWtprdBegtypNm"))
).withColumn(
    "CLS_PLN_COV_WTPRD_TYP_CD",
    F.when(F.col("TRGT_CD_CovWtprdTypCd").isNull(), F.lit("UNK")).otherwise(F.col("TRGT_CD_CovWtprdTypCd"))
).withColumn(
    "CLS_PLN_COV_WTPRD_TYP_NM",
    F.when(F.col("TRGT_CD_NM_CovWtprdTypNm").isNull(), F.lit("UNK")).otherwise(F.col("TRGT_CD_NM_CovWtprdTypNm"))
).withColumn(
    "CLS_PLN_COV_DPNDT_STOP_AGE",
    F.col("DPNDT_STOP_AGE")
).withColumn(
    "CLS_PLN_COV_RULE_DESC",
    F.when(F.col("CLS_PLN_COV_RULE_DESC").isNull(), F.lit("UNK")).otherwise(F.col("CLS_PLN_COV_RULE_DESC"))
).withColumn(
    "CLS_PLN_COV_RULE_ID",
    F.col("CLS_PLN_COV_RULE_ID")
).withColumn(
    "CLS_PLN_COV_SPOUSE_STOP_AGE",
    F.col("SPOUSE_STOP_AGE")
).withColumn(
    "CLS_PLN_COV_STDNT_STOP_AGE",
    F.col("STDNT_STOP_AGE")
).withColumn(
    "CLS_PLN_COV_SUB_STOP_AGE",
    F.col("SUB_STOP_AGE")
).withColumn(
    "CLS_PLN_COV_RULE_WAIT_PERD_QTY",
    F.col("WAIT_PERD_QTY")
).withColumn(
    "CLS_PLN_DTL_PROD_CAT_NM",
    F.when(F.col("TRGT_CD_NM").isNull(), F.lit("UNK")).otherwise(F.col("TRGT_CD_NM"))
).withColumn(
    "CLS_PLN_DTL_COV_PROV_PFX_CD",
    F.when(F.col("TRGT_CD_ClsPlnDtlCovProvPfxCd").isNull(), F.lit("UNK")).otherwise(F.col("TRGT_CD_ClsPlnDtlCovProvPfxCd"))
).withColumn(
    "CLS_PLN_DTL_COV_PROV_PFX_NM",
    F.when(F.col("TRGT_CD_NM_ClsPlnDtlCovProvPfxNm").isNull(), F.lit("UNK")).otherwise(F.col("TRGT_CD_NM_ClsPlnDtlCovProvPfxNm"))
).withColumn(
    "CLS_PLN_DTL_NTWK_SET_PFX_CD",
    F.when(F.col("TRGT_CD_ClsPlnDtlNtwkSetPfxCd").isNull(), F.lit("UNK")).otherwise(F.col("TRGT_CD_ClsPlnDtlNtwkSetPfxCd"))
).withColumn(
    "CLS_PLN_DTL_NTWK_SET_PFX_NM",
    F.when(F.col("TRGT_CD_NM_ClsPlnDtlNtwkSetPfxNm").isNull(), F.lit("UNK")).otherwise(F.col("TRGT_CD_NM_ClsPlnDtlNtwkSetPfxNm"))
).withColumn(
    "CLS_PLN_DTL_TERM_DT_SK",
    F.col("TERM_DT_SK")
).withColumn(
    "CLS_PLN_DESC",
    F.when(F.col("CLS_PLN_DESC").isNull(), F.lit("UNK")).otherwise(F.col("CLS_PLN_DESC"))
).withColumn(
    "GRP_NM",
    F.when(F.col("GRP_NM").isNull(), F.lit("UNK")).otherwise(F.col("GRP_NM"))
).withColumn(
    "PCA_ADM_ID",
    F.when(F.col("PCA_ADM_ID").isNull(), F.lit("UNK")).otherwise(F.col("PCA_ADM_ID"))
).withColumn(
    "PROD_ID",
    F.when(F.col("PROD_ID").isNull(), F.lit("UNK")).otherwise(F.col("PROD_ID"))
).withColumn(
    "CRT_RUN_CYC_EXCTN_SK",
    F.lit(EDWRunCycle)
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    F.lit(EDWRunCycle)
).withColumn(
    "CLS_PLN_COV_DPNDT_STOP_CD_SK",
    F.when(F.col("CLS_PLN_COV_DPNDT_STOP_CD_SK").isNull(), F.lit(0)).otherwise(F.col("CLS_PLN_COV_DPNDT_STOP_CD_SK"))
).withColumn(
    "CLS_PLN_COV_SPOUSE_STOP_CD_SK",
    F.when(F.col("CLS_PLN_COV_SPOUSE_STOP_CD_SK").isNull(), F.lit(0)).otherwise(F.col("CLS_PLN_COV_SPOUSE_STOP_CD_SK"))
).withColumn(
    "CLS_PLN_COV_STDNT_STOP_CD_SK",
    F.when(F.col("CLS_PLN_COV_STDNT_STOP_CD_SK").isNull(), F.lit(0)).otherwise(F.col("CLS_PLN_COV_STDNT_STOP_CD_SK"))
).withColumn(
    "CLS_PLN_COV_SUB_STOP_CD_SK",
    F.when(F.col("CLS_PLN_COV_SUB_STOP_CD_SK").isNull(), F.lit(0)).otherwise(F.col("CLS_PLN_COV_SUB_STOP_CD_SK"))
).withColumn(
    "CLS_PLN_COV_WTPRD_BEG_CD_SK",
    F.when(F.col("CLS_PLN_COV_WTPRD_BEG_CD_SK").isNull(), F.lit(0)).otherwise(F.col("CLS_PLN_COV_WTPRD_BEG_CD_SK"))
).withColumn(
    "CLS_PLN_COV_WTPRD_BEGTYP_CD_SK",
    F.when(F.col("CLS_PLN_COV_WTPRD_BEGTYP_CD_SK").isNull(), F.lit(0)).otherwise(F.col("CLS_PLN_COV_WTPRD_BEGTYP_CD_SK"))
).withColumn(
    "CLS_PLN_COV_WTPRD_TYP_CD_SK",
    F.when(F.col("CLS_PLN_COV_WTPRD_TYP_CD_SK").isNull(), F.lit(0)).otherwise(F.col("CLS_PLN_COV_WTPRD_TYP_CD_SK"))
).withColumn(
    "ALPHA_PFX_CD",
    F.when(F.col("ALPHA_PFX_CD").isNull(), F.lit("UNK")).otherwise(F.col("ALPHA_PFX_CD"))
).withColumn(
    "ALPHA_PFX_NM",
    F.col("ALPHA_PFX_NM")
).withColumn(
    "ALPHA_PFX_SK",
    F.when(F.col("ALPHA_PFX_SK").isNull(), F.lit(0)).otherwise(F.col("ALPHA_PFX_SK"))
).withColumn(
    "CLS_PLN_DTL_AGE_CALC_CD",
    F.when(F.col("TRGT_CD_ClsPlnDtlAgeCalcCd").isNull(), F.lit("UNK")).otherwise(F.col("TRGT_CD_ClsPlnDtlAgeCalcCd"))
).withColumn(
    "CLS_PLN_DTL_AGE_CALC_NM",
    F.when(F.col("TRGT_CD_NM_ClsPlnDtlAgeCalcNm").isNull(), F.lit("UNK")).otherwise(F.col("TRGT_CD_NM_ClsPlnDtlAgeCalcNm"))
).withColumn(
    "CLS_PLN_DTL_CAROVR_PERD_BEG_MO",
    F.col("CAROVR_PERD_BEG_MO")
).withColumn(
    "CLS_PLN_DTL_CE_BREAK_ALW_DAYS",
    F.col("CONT_ENR_BREAK_ALW_DAYS")
).withColumn(
    "CLS_PLN_DTL_CE_BREAK_CD",
    F.when(F.col("TRGT_CD_ClsPlnDtlCeBreakCd").isNull(), F.lit("UNK")).otherwise(F.col("TRGT_CD_ClsPlnDtlCeBreakCd"))
).withColumn(
    "CLS_PLN_DTL_CE_BREAK_NM",
    F.when(F.col("TRGT_CD_NM_ClsPlnDtlCeBreakNm").isNull(), F.lit("UNK")).otherwise(F.col("TRGT_CD_NM_ClsPlnDtlCeBreakNm"))
).withColumn(
    "CLS_PLN_DTL_ENR_BEG_DT_MO_DAY",
    F.col("ENR_BEG_DT_MO_DAY")
).withColumn(
    "CLS_PLN_DTL_ENR_END_DT_MO_DAY",
    F.col("ENR_END_DT_MO_DAY")
).withColumn(
    "CLS_PLN_DTL_FMLY_CNTR_CD",
    F.when(F.col("TRGT_CD_ClsPlnDtlFmlyCntlCd").isNull(), F.lit("UNK")).otherwise(F.col("TRGT_CD_ClsPlnDtlFmlyCntlCd"))
).withColumn(
    "CLS_PLN_DTL_FMLY_CNTR_NM",
    F.when(F.col("TRGT_CD_NM_ClsPlnDtlFmlyCntlNm").isNull(), F.lit("UNK")).otherwise(F.col("TRGT_CD_NM_ClsPlnDtlFmlyCntlNm"))
).withColumn(
    "CLS_PLN_DTL_ID_CARD_STOCK_ID",
    F.col("ID_CARD_STOCK_ID")
).withColumn(
    "CLS_PLN_DTL_ID_CARD_TYP_CD",
    F.when(F.col("TRGT_CD_ClsPlnDtlIdCardTypCd").isNull(), F.lit("UNK")).otherwise(F.col("TRGT_CD_ClsPlnDtlIdCardTypCd"))
).withColumn(
    "CLS_PLN_DTL_ID_CARD_TYP_NM",
    F.when(F.col("TRGT_CD_NM_ClsPlnDtlIdCardTypNm").isNull(), F.lit("UNK")).otherwise(F.col("TRGT_CD_NM_ClsPlnDtlIdCardTypNm"))
).withColumn(
    "CLS_PLN_DTL_PLN_BEG_DT_MO_DAY",
    F.col("PLN_BEG_DT_MO_DAY")
).withColumn(
    "CLS_PLN_DTL_RATE_GUAR_CD",
    F.when(F.col("TRGT_CD_ClsPlnDtlRateGuardCd").isNull(), F.lit("UNK")).otherwise(F.col("TRGT_CD_ClsPlnDtlRateGuardCd"))
).withColumn(
    "CLS_PLN_DTL_RATE_GUAR_NM",
    F.when(F.col("TRGT_CD_NM_ClsPlnDtlRateGuardNm").isNull(), F.lit("UNK")).otherwise(F.col("TRGT_CD_NM_ClsPlnDtlRateGuardNm"))
).withColumn(
    "CLS_PLN_DTL_RATE_GUAR_DT_SK",
    F.col("RATE_GUAR_DT_SK")
).withColumn(
    "CLS_PLN_DTL_RATE_GUAR_PERD_MO",
    F.col("RATE_GUAR_PERD_MO")
).withColumn(
    "CLS_PLN_DTL_SELABLTY_IN",
    F.col("CLS_PLN_DTL_SELABLTY_IN")
).withColumn(
    "CLS_PLN_DTL_WARN_MSG_SEQ_NO",
    F.col("WARN_MSG_SEQ_NO")
).withColumn(
    "DR_CARD_BANK_RELSHP_PFX_ID",
    F.col("DR_CARD_BANK_RELSHP_PFX_ID")
).withColumn(
    "PROD_VOL_RDUCTN_PFX_ID",
    F.col("PROD_VOL_RDUCTN_PFX_ID")
).withColumn(
    "CLS_PLN_DTL_AGE_CALC_CD_SK",
    F.col("CLS_PLN_DTL_AGE_CALC_CD_SK")
).withColumn(
    "CLS_PLN_DTL_CE_BREAK_CD_SK",
    F.col("CLS_PLN_DTL_CE_BREAK_CD_SK")
).withColumn(
    "CLS_PLN_DTL_FMLY_CNTR_CD_SK",
    F.col("CLS_PLN_DTL_FMLY_CNTR_CD_SK")
).withColumn(
    "CLS_PLN_DTL_ID_CARD_TYP_CD_SK",
    F.col("CLS_PLN_DTL_ID_CARD_TYP_CD_SK")
).withColumn(
    "CLS_PLN_DTL_RATE_GUAR_CD_SK",
    F.col("CLS_PLN_DTL_RATE_GUAR_CD_SK")
).withColumn(
    "CMPSS_COV_IN",
    F.col("CMPSS_COV_IN")
)

# Now select in final order and apply rpad for char columns
df_xfm_BusinessLogic = df_xfm_BusinessLogic_pre.select(
    F.col("CLS_PLN_DTL_SK"),
    F.col("SRC_SYS_CD"),
    F.col("GRP_ID"),
    F.col("CLS_ID"),
    F.col("CLS_PLN_ID"),
    F.col("CLS_PLN_DTL_PROD_CAT_CD"),
    F.rpad(F.col("CLS_PLN_DTL_EFF_DT_SK"), 10, " ").alias("CLS_PLN_DTL_EFF_DT_SK"),
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("CLS_SK"),
    F.col("CLS_PLN_SK"),
    F.col("CLS_PLN_COV_DPNDT_STOP_EXCD_SK"),
    F.col("CLS_PLN_COV_SPOUS_STOP_EXCD_SK"),
    F.col("CLS_PLN_COV_STDNT_STOP_EXCD_SK"),
    F.col("CLS_PLN_COV_SUB_STOP_EXCD_SK"),
    F.col("GRP_SK"),
    F.col("PCA_ADM_SK"),
    F.col("PROD_SK"),
    F.col("CLS_PLN_COV_DPNDT_STOP_CD"),
    F.col("CLS_PLN_COV_DPNDT_STOP_NM"),
    F.col("CLS_PLN_COV_SPOUSE_STOP_CD"),
    F.col("CLS_PLN_COV_SPOUSE_STOP_NM"),
    F.col("CLS_PLN_COV_STDNT_STOP_CD"),
    F.col("CLS_PLN_COV_STDNT_STOP_NM"),
    F.col("CLS_PLN_COV_SUB_STOP_EVT_CD"),
    F.col("CLS_PLN_COV_SUB_STOP_EVT_NM"),
    F.col("CLS_PLN_COV_WTPRD_BEG_CD"),
    F.col("CLS_PLN_COV_WTPRD_BEG_NM"),
    F.col("CLS_PLN_COV_WTPRD_BEGTYP_CD"),
    F.col("CLS_PLN_COV_WTPRD_BEGTYP_NM"),
    F.col("CLS_PLN_COV_WTPRD_TYP_CD"),
    F.col("CLS_PLN_COV_WTPRD_TYP_NM"),
    F.col("CLS_PLN_COV_DPNDT_STOP_AGE"),
    F.col("CLS_PLN_COV_RULE_DESC"),
    F.col("CLS_PLN_COV_RULE_ID"),
    F.col("CLS_PLN_COV_SPOUSE_STOP_AGE"),
    F.col("CLS_PLN_COV_STDNT_STOP_AGE"),
    F.col("CLS_PLN_COV_SUB_STOP_AGE"),
    F.col("CLS_PLN_COV_RULE_WAIT_PERD_QTY"),
    F.col("CLS_PLN_DTL_PROD_CAT_NM"),
    F.col("CLS_PLN_DTL_COV_PROV_PFX_CD"),
    F.col("CLS_PLN_DTL_COV_PROV_PFX_NM"),
    F.col("CLS_PLN_DTL_NTWK_SET_PFX_CD"),
    F.col("CLS_PLN_DTL_NTWK_SET_PFX_NM"),
    F.rpad(F.col("CLS_PLN_DTL_TERM_DT_SK"), 10, " ").alias("CLS_PLN_DTL_TERM_DT_SK"),
    F.col("CLS_PLN_DESC"),
    F.col("GRP_NM"),
    F.col("PCA_ADM_ID"),
    F.rpad(F.col("PROD_ID"), 8, " ").alias("PROD_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLS_PLN_COV_DPNDT_STOP_CD_SK"),
    F.col("CLS_PLN_COV_SPOUSE_STOP_CD_SK"),
    F.col("CLS_PLN_COV_STDNT_STOP_CD_SK"),
    F.col("CLS_PLN_COV_SUB_STOP_CD_SK"),
    F.col("CLS_PLN_COV_WTPRD_BEG_CD_SK"),
    F.col("CLS_PLN_COV_WTPRD_BEGTYP_CD_SK"),
    F.col("CLS_PLN_COV_WTPRD_TYP_CD_SK"),
    F.col("CLS_PLN_DTL_PROD_CAT_CD_SK"),
    F.col("CLS_PLN_DTL_COV_PROV_PFX_CD_SK"),
    F.col("CLS_PLN_DTL_NTWK_SET_PFX_CD_SK"),
    F.col("ALPHA_PFX_CD"),
    F.col("ALPHA_PFX_NM"),
    F.col("ALPHA_PFX_SK"),
    F.col("CLS_PLN_DTL_AGE_CALC_CD"),
    F.col("CLS_PLN_DTL_AGE_CALC_NM"),
    F.col("CLS_PLN_DTL_CAROVR_PERD_BEG_MO"),
    F.col("CLS_PLN_DTL_CE_BREAK_ALW_DAYS"),
    F.col("CLS_PLN_DTL_CE_BREAK_CD"),
    F.col("CLS_PLN_DTL_CE_BREAK_NM"),
    F.rpad(F.col("CLS_PLN_DTL_ENR_BEG_DT_MO_DAY"), 4, " ").alias("CLS_PLN_DTL_ENR_BEG_DT_MO_DAY"),
    F.rpad(F.col("CLS_PLN_DTL_ENR_END_DT_MO_DAY"), 4, " ").alias("CLS_PLN_DTL_ENR_END_DT_MO_DAY"),
    F.col("CLS_PLN_DTL_FMLY_CNTR_CD"),
    F.col("CLS_PLN_DTL_FMLY_CNTR_NM"),
    F.col("CLS_PLN_DTL_ID_CARD_STOCK_ID"),
    F.col("CLS_PLN_DTL_ID_CARD_TYP_CD"),
    F.col("CLS_PLN_DTL_ID_CARD_TYP_NM"),
    F.rpad(F.col("CLS_PLN_DTL_PLN_BEG_DT_MO_DAY"), 4, " ").alias("CLS_PLN_DTL_PLN_BEG_DT_MO_DAY"),
    F.col("CLS_PLN_DTL_RATE_GUAR_CD"),
    F.col("CLS_PLN_DTL_RATE_GUAR_NM"),
    F.rpad(F.col("CLS_PLN_DTL_RATE_GUAR_DT_SK"), 10, " ").alias("CLS_PLN_DTL_RATE_GUAR_DT_SK"),
    F.col("CLS_PLN_DTL_RATE_GUAR_PERD_MO"),
    F.rpad(F.col("CLS_PLN_DTL_SELABLTY_IN"), 1, " ").alias("CLS_PLN_DTL_SELABLTY_IN"),
    F.col("CLS_PLN_DTL_WARN_MSG_SEQ_NO"),
    F.col("DR_CARD_BANK_RELSHP_PFX_ID"),
    F.col("PROD_VOL_RDUCTN_PFX_ID"),
    F.col("CLS_PLN_DTL_AGE_CALC_CD_SK"),
    F.col("CLS_PLN_DTL_CE_BREAK_CD_SK"),
    F.col("CLS_PLN_DTL_FMLY_CNTR_CD_SK"),
    F.col("CLS_PLN_DTL_ID_CARD_TYP_CD_SK"),
    F.col("CLS_PLN_DTL_RATE_GUAR_CD_SK"),
    F.rpad(F.col("CMPSS_COV_IN"), 1, " ").alias("CMPSS_COV_IN"),
)

# ----------------------------------------------------------------------------------------------------------------------
# Stage: seq_CLS_PLN_DTL_I (PxSequentialFile) - Write to CLS_PLN_DTL_I.dat
# ----------------------------------------------------------------------------------------------------------------------
write_files(
    df_xfm_BusinessLogic,
    f"{adls_path}/load/CLS_PLN_DTL_I.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)