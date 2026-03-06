# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC DEVELOPER                         DATE                 PROJECT                                                  DESCRIPTION                                        DATASTAGE   ENVIRONMENT                     CODE  REVIEW            REVIEW DATE                    
# MAGIC -----------------------------                 ----------------------      ------------------------------                                  -----------------------------------                               ------------------------------                                          ------------------------------      --------------------------
# MAGIC Aditya Raju                             06/13/2013        5114-Enterprise Efficiencies                 Move edw to edw for PROV DIR D                EnterpriseWrhseDevl                                      Peter Marshall               8/28/2013

# MAGIC Read from source table PROV_DIR_D from EDW. Apply Run Cycle filters to get just the needed rows forward
# MAGIC This is a load ready file conforms to the target table structure.
# MAGIC 
# MAGIC Write PROV_DIR_D Data into a Sequential file for Load Job.
# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys: 
# MAGIC CD_MPPNG_SK
# MAGIC Add Defaults and Null Handling
# MAGIC Funnel NA UNK Rows
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, when, rpad, trim, row_number, isin, isnull
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


# Shared Container references (none present in this job)

# Retrieve parameter values
EDWRunCycle = get_widget_value('EDWRunCycle','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')

# DB2ConnectorPX: db2_EXTRACT_in
jdbc_url, jdbc_props = get_db_config(edw_secret_name)
extract_query_db2_EXTRACT_in = f"""SELECT 
PROV_D.PROV_SK,
PROV_D.PROV_ID,
PROV_D.PROV_ADDR_ID,
PROV_NTWK_D.NTWK_ID,
PROV_D.SRC_SYS_CD,
PROV_D.CMN_PRCT_ID,
PROV_D.PROV_FCLTY_TYP_CD,
PROV_D.PROV_FCLTY_TYP_NM,
PROV_D.PROV_REL_GRP_PROV_ID,
PROV_D.PROV_REL_GRP_PROV_NM,
PROV_D.PROV_SPEC_CD,
PROV_D.PROV_SPEC_NM,
PROV_D.PROV_TAX_ID,
PROV_D.PROV_TYP_CD,
PROV_D.PROV_TYP_NM,
PROV_D.CMN_PRCT_SK,
PROV_D.PROV_NM,
PROV_D.PROV_REL_IPA_PROV_ID,
PROV_NTWK_D.PROV_NTWK_PFX_ID,
PROV_NTWK_D.PROV_NTWK_EFF_DT_SK,
PROV_NTWK_D.NTWK_SH_NM,
PROV_NTWK_D.PROV_NTWK_ACPTNG_MCAID_PATN_IN,
PROV_NTWK_D.PROV_NTWK_ACPTNG_MCARE_PATN_IN,
PROV_NTWK_D.PROV_NTWK_ACPTNG_PATN_IN,
PROV_NTWK_D.PROV_NTWK_MAX_PATN_AGE,
PROV_NTWK_D.PROV_NTWK_MIN_PATN_AGE,
PROV_NTWK_D.PROV_NTWK_PCP_IN,
PROV_NTWK_D.PROV_NTWK_TERM_DT_SK,
PROV_NTWK_D.PROV_NTWK_MAX_PATN_QTY,
PROV_NTWK_D.PROV_NTWK_GNDR_ACPTD_CD,
PROV_ADDR_D.PROV_ADDR_TYP_CD,
PROV_ADDR_D.PROV_ADDR_EFF_DT_SK,
PROV_ADDR_D.PROV_ADDR_HCAP_IN,
PROV_ADDR_D.PROV_ADDR_TERM_DT_SK,
PROV_ADDR_D.PROV_ADDR_LN_1,
PROV_ADDR_D.PROV_ADDR_LN_2,
PROV_ADDR_D.PROV_ADDR_LN_3,
PROV_ADDR_D.PROV_ADDR_CITY_NM,
PROV_ADDR_D.PROV_ADDR_ST_CD,
PROV_ADDR_D.PROV_ADDR_ZIP_CD_5,
PROV_ADDR_D.PROV_ADDR_ZIP_CD_4,
PROV_ADDR_D.PROV_ADDR_CNTY_NM,
PROV_ADDR_D.PROV_ADDR_PHN_NO,
PROV_ADDR_D.PROV_ADDR_FAX_NO,
PROV_ADDR_D.PROV_ADDR_EMAIL_ADDR_TX,
PROV_ADDR_D.PROV_ADDR_FAX_NO_EXT,
PROV_ADDR_D.PROV_ADDR_PHN_NO_EXT,
PROV_ADDR_D.PROV_ADDR_PRCTC_LOC_IN,
PROV_ADDR_D.PROV_MAIL_ADDR_EFF_DT_SK,
CD_MPPNG.SRC_CD AS PROV_ENTY_CD,
CD_MPPNG2.SRC_CD AS PROV_PRCTC_TYP_CD,
PROV_ADDR_D.PROV_ADDR_GEO_ACES_RTRN_CD_TX,
PROV_ADDR_D.PROV_ADDR_LAT_TX,
PROV_ADDR_D.PROV_ADDR_LONG_TX,
PROV_D.CRT_RUN_CYC_EXCTN_SK
FROM {EDWOwner}.prov_d PROV_D,
     {EDWOwner}.prov_ntwk_d PROV_NTWK_D,
     {EDWOwner}.ntwk_d NTWK_D,
     {EDWOwner}.prov_addr_d PROV_ADDR_D,
     {EDWOwner}.cd_mppng CD_MPPNG,
     {EDWOwner}.cd_mppng CD_MPPNG2
WHERE PROV_D.SRC_SYS_CD = PROV_ADDR_D.SRC_SYS_CD
  AND PROV_D.PROV_ADDR_ID = PROV_ADDR_D.PROV_ADDR_ID
  AND PROV_D.PROV_SK = PROV_NTWK_D.PROV_SK
  AND PROV_NTWK_D.NTWK_SK = NTWK_D.NTWK_SK
  AND PROV_D.PROV_ENTY_CD = CD_MPPNG.TRGT_CD
  AND CD_MPPNG.TRGT_DOMAIN_NM = 'PROVIDER ENTITY'
  AND PROV_D.PROV_PRCTC_TYP_CD = CD_MPPNG2.TRGT_CD
  AND CD_MPPNG2.TRGT_DOMAIN_NM = 'PROVIDER PRACTICE TYPE'
  AND NTWK_D.NTWK_DIR_CD IN('ALLDIR','WEBDIR','PRNTDIR')
  AND PROV_NTWK_D.PROV_NTWK_DIR_IN = 'Y'
  AND PROV_NTWK_D.PROV_NTWK_EFF_DT_SK <= '{EDWRunCycleDate}'
  AND PROV_NTWK_D.PROV_NTWK_TERM_DT_SK > '{EDWRunCycleDate}'
  AND PROV_D.PROV_TERM_DT_SK > '{EDWRunCycleDate}'
  AND PROV_ADDR_D.PROV_ADDR_TERM_DT_SK > '{EDWRunCycleDate}'
  AND PROV_ADDR_D.PROV_ADDR_PRCTC_LOC_IN = 'Y'
  AND PROV_ADDR_D.PROV_ADDR_CNTY_CLS_CD IN ('CONTG','32CNTY')
  AND NOT PROV_D.PROV_ID IN('NA','UNK')"""
df_db2_EXTRACT_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_EXTRACT_in)
    .load()
)

# DB2ConnectorPX: db2_CMN_PRCT_in
extract_query_db2_CMN_PRCT_in = f"""SELECT 
CMN_PRCT_D.CMN_PRCT_SK,
CMN_PRCT_D.CMN_PRCT_BRTH_DT_SK,
CMN_PRCT_D.CMN_PRCT_GNDR_CD,
CMN_PRCT_D.CMN_PRCT_SSN,
CMN_PRCT_D.CMN_PRCT_TTL,
CMN_PRCT_D.CMN_PRCT_FIRST_NM,
CMN_PRCT_D.CMN_PRCT_MIDINIT,
CMN_PRCT_D.CMN_PRCT_LAST_NM,
CMN_PRCT_D.CMN_PRCT_LAST_CRDTL_DT_SK
FROM {EDWOwner}.CMN_PRCT_D CMN_PRCT_D"""
df_db2_CMN_PRCT_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CMN_PRCT_in)
    .load()
)

# DB2ConnectorPX: db2_PCMH_EXTR_in
extract_query_db2_PCMH_EXTR_in = f"""SELECT
PROV_SK,
NTWK_ID,
PROV_NTWK_PFX_ID,
PROV_NTWK_EFF_DT_SK,
PROV_NTWK_TERM_DT_SK
FROM {EDWOwner}.PROV_NTWK_D
WHERE PROV_NTWK_PFX_ID = 'PCMH'
  AND PROV_NTWK_DIR_IN = 'Y'
  AND PROV_NTWK_EFF_DT_SK <= '{EDWRunCycleDate}'
  AND PROV_NTWK_TERM_DT_SK > '{EDWRunCycleDate}'"""
df_db2_PCMH_EXTR_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_PCMH_EXTR_in)
    .load()
)

# DB2ConnectorPX: db2_ENTY_REG_in
extract_query_db2_ENTY_REG_in = f"""SELECT DISTINCT
ENTY_RGSTRN_D.PROV_SK,
ENTY_RGSTRN_D.ENTY_RGSTRN_TYP_CD
FROM {EDWOwner}.enty_rgstrn_d ENTY_RGSTRN_D
WHERE ENTY_RGSTRN_D.ENTY_RGSTRN_TYP_CD = 'LEAPFROG'
  AND ENTY_RGSTRN_D.ENTY_RGSTRN_TERM_DT_SK >= '{EDWRunCycleDate}'"""
df_db2_ENTY_REG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_ENTY_REG_in)
    .load()
)

# DB2ConnectorPX: db2_ENTY_LIC_in
extract_query_db2_ENTY_LIC_in = f"""SELECT 
ENTY_LIC_D.CMN_PRCT_SK,
ENTY_LIC_D.ENTY_LIC_SEQ_NO,
ENTY_LIC_D.ENTY_LIC_NO,
ENTY_LIC_D.ENTY_LIC_ST_CD
FROM {EDWOwner}.enty_lic_d ENTY_LIC_D
WHERE ENTY_LIC_D.ENTY_LIC_SEQ_NO IN (1,2)"""
df_db2_ENTY_LIC_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_ENTY_LIC_in)
    .load()
)

# PxFilter: ftr_Enty_Lic
df_ftr_Enty_Lic_condition0 = df_db2_ENTY_LIC_in.filter(col("ENTY_LIC_SEQ_NO") == 1)
df_ftr_Enty_Lic_condition1 = df_db2_ENTY_LIC_in.filter(col("ENTY_LIC_SEQ_NO") == 2)

df_lnk_Enty_Lic1_out = df_ftr_Enty_Lic_condition0.select(
    col("CMN_PRCT_SK").alias("CMN_PRCT_SK"),
    col("ENTY_LIC_SEQ_NO").alias("ENTY_LIC_SEQ_NO"),
    col("ENTY_LIC_NO").alias("ENTY_LIC_NO"),
    col("ENTY_LIC_ST_CD").alias("ENTY_LIC_ST_CD")
)

df_lnk_Enty_Lic2_out = df_ftr_Enty_Lic_condition1.select(
    col("CMN_PRCT_SK").alias("CMN_PRCT_SK"),
    col("ENTY_LIC_SEQ_NO").alias("ENTY_LIC_SEQ_NO"),
    col("ENTY_LIC_NO").alias("ENTY_LIC_NO"),
    col("ENTY_LIC_ST_CD").alias("ENTY_LIC_ST_CD")
)

# PxRemDup: rmd_Enty_Lic1
df_lnk_rmd_Enty_Lic1_out = dedup_sort(
    df_lnk_Enty_Lic1_out,
    ["CMN_PRCT_SK"],
    []
)

# PxRemDup: rmd_Enty_Lic2
df_lnk_rmd_Enty_Lic2_out = dedup_sort(
    df_lnk_Enty_Lic2_out,
    ["CMN_PRCT_SK"],
    []
)

# PxLookup: lkp_Codes (multiple left joins)
df_lkp_joined = (
    df_db2_EXTRACT_in.alias("lnk_Prov_Dir_D_inABC")
    .join(
        df_db2_CMN_PRCT_in.alias("lnk_EdwCmn_Prct_Extr_InABC"),
        col("lnk_Prov_Dir_D_inABC.CMN_PRCT_SK") == col("lnk_EdwCmn_Prct_Extr_InABC.CMN_PRCT_SK"),
        how="left"
    )
    .join(
        df_db2_ENTY_REG_in.alias("lnk_EdwEnty_Reg_Extr_InABC"),
        col("lnk_Prov_Dir_D_inABC.PROV_SK") == col("lnk_EdwEnty_Reg_Extr_InABC.PROV_SK"),
        how="left"
    )
    .join(
        df_lnk_rmd_Enty_Lic1_out.alias("lnk_rmd_Enty_Lic1_out"),
        col("lnk_Prov_Dir_D_inABC.CMN_PRCT_SK") == col("lnk_rmd_Enty_Lic1_out.CMN_PRCT_SK"),
        how="left"
    )
    .join(
        df_lnk_rmd_Enty_Lic2_out.alias("lnk_rmd_Enty_Lic2_out"),
        col("lnk_Prov_Dir_D_inABC.CMN_PRCT_SK") == col("lnk_rmd_Enty_Lic2_out.CMN_PRCT_SK"),
        how="left"
    )
    .join(
        df_db2_PCMH_EXTR_in.alias("lnk_EdwPcmh_Extr_InABC"),
        (
            (col("lnk_Prov_Dir_D_inABC.PROV_SK") == col("lnk_EdwPcmh_Extr_InABC.PROV_SK"))
            & (col("lnk_Prov_Dir_D_inABC.NTWK_ID") == col("lnk_EdwPcmh_Extr_InABC.NTWK_ID"))
        ),
        how="left"
    )
)

df_lnk_CodesLkpData_out = df_lkp_joined.select(
    col("lnk_Prov_Dir_D_inABC.PROV_SK").alias("PROV_SK"),
    col("lnk_Prov_Dir_D_inABC.PROV_ID").alias("PROV_ID"),
    col("lnk_Prov_Dir_D_inABC.PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    col("lnk_Prov_Dir_D_inABC.NTWK_ID").alias("NTWK_ID"),
    col("lnk_Prov_Dir_D_inABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("lnk_Prov_Dir_D_inABC.CMN_PRCT_ID").alias("CMN_PRCT_ID"),
    col("lnk_Prov_Dir_D_inABC.PROV_FCLTY_TYP_CD").alias("PROV_FCLTY_TYP_CD"),
    col("lnk_Prov_Dir_D_inABC.PROV_FCLTY_TYP_NM").alias("PROV_FCLTY_TYP_NM"),
    col("lnk_Prov_Dir_D_inABC.PROV_REL_GRP_PROV_ID").alias("PROV_REL_GRP_PROV_ID"),
    col("lnk_Prov_Dir_D_inABC.PROV_REL_GRP_PROV_NM").alias("PROV_REL_GRP_PROV_NM"),
    col("lnk_Prov_Dir_D_inABC.PROV_SPEC_CD").alias("PROV_SPEC_CD"),
    col("lnk_Prov_Dir_D_inABC.PROV_SPEC_NM").alias("PROV_SPEC_NM"),
    col("lnk_Prov_Dir_D_inABC.PROV_TAX_ID").alias("PROV_TAX_ID"),
    col("lnk_Prov_Dir_D_inABC.PROV_TYP_CD").alias("PROV_TYP_CD"),
    col("lnk_Prov_Dir_D_inABC.PROV_TYP_NM").alias("PROV_TYP_NM"),
    col("lnk_Prov_Dir_D_inABC.CMN_PRCT_SK").alias("CMN_PRCT_SK"),
    col("lnk_Prov_Dir_D_inABC.PROV_NM").alias("PROV_NM"),
    col("lnk_Prov_Dir_D_inABC.PROV_REL_IPA_PROV_ID").alias("PROV_REL_IPA_PROV_ID"),
    col("lnk_Prov_Dir_D_inABC.PROV_NTWK_PFX_ID").alias("PROV_NTWK_PFX_ID"),
    col("lnk_Prov_Dir_D_inABC.PROV_NTWK_EFF_DT_SK").alias("PROV_NTWK_EFF_DT_SK"),
    col("lnk_Prov_Dir_D_inABC.NTWK_SH_NM").alias("NTWK_SH_NM"),
    col("lnk_Prov_Dir_D_inABC.PROV_NTWK_ACPTNG_MCAID_PATN_IN").alias("PROV_NTWK_ACPTNG_MCAID_PATN_IN"),
    col("lnk_Prov_Dir_D_inABC.PROV_NTWK_ACPTNG_MCARE_PATN_IN").alias("PROV_NTWK_ACPTNG_MCARE_PATN_IN"),
    col("lnk_Prov_Dir_D_inABC.PROV_NTWK_ACPTNG_PATN_IN").alias("PROV_NTWK_ACPTNG_PATN_IN"),
    col("lnk_Prov_Dir_D_inABC.PROV_NTWK_MAX_PATN_AGE").alias("PROV_NTWK_MAX_PATN_AGE"),
    col("lnk_Prov_Dir_D_inABC.PROV_NTWK_MIN_PATN_AGE").alias("PROV_NTWK_MIN_PATN_AGE"),
    col("lnk_Prov_Dir_D_inABC.PROV_NTWK_PCP_IN").alias("PROV_NTWK_PCP_IN"),
    col("lnk_Prov_Dir_D_inABC.PROV_NTWK_TERM_DT_SK").alias("PROV_NTWK_TERM_DT_SK"),
    col("lnk_Prov_Dir_D_inABC.PROV_NTWK_MAX_PATN_QTY").alias("PROV_NTWK_MAX_PATN_QTY"),
    col("lnk_Prov_Dir_D_inABC.PROV_NTWK_GNDR_ACPTD_CD").alias("PROV_NTWK_GNDR_ACPTD_CD"),
    col("lnk_Prov_Dir_D_inABC.PROV_ADDR_TYP_CD").alias("PROV_ADDR_TYP_CD"),
    col("lnk_Prov_Dir_D_inABC.PROV_ADDR_EFF_DT_SK").alias("PROV_ADDR_EFF_DT_SK"),
    col("lnk_Prov_Dir_D_inABC.PROV_ADDR_HCAP_IN").alias("PROV_ADDR_HCAP_IN"),
    col("lnk_Prov_Dir_D_inABC.PROV_ADDR_TERM_DT_SK").alias("PROV_ADDR_TERM_DT_SK"),
    col("lnk_Prov_Dir_D_inABC.PROV_ADDR_LN_1").alias("PROV_ADDR_LN_1"),
    col("lnk_Prov_Dir_D_inABC.PROV_ADDR_LN_2").alias("PROV_ADDR_LN_2"),
    col("lnk_Prov_Dir_D_inABC.PROV_ADDR_LN_3").alias("PROV_ADDR_LN_3"),
    col("lnk_Prov_Dir_D_inABC.PROV_ADDR_CITY_NM").alias("PROV_ADDR_CITY_NM"),
    col("lnk_Prov_Dir_D_inABC.PROV_ADDR_ST_CD").alias("PROV_ADDR_ST_CD"),
    col("lnk_Prov_Dir_D_inABC.PROV_ADDR_ZIP_CD_5").alias("PROV_ADDR_ZIP_CD_5"),
    col("lnk_Prov_Dir_D_inABC.PROV_ADDR_ZIP_CD_4").alias("PROV_ADDR_ZIP_CD_4"),
    col("lnk_Prov_Dir_D_inABC.PROV_ADDR_CNTY_NM").alias("PROV_ADDR_CNTY_NM"),
    col("lnk_Prov_Dir_D_inABC.PROV_ADDR_PHN_NO").alias("PROV_ADDR_PHN_NO"),
    col("lnk_Prov_Dir_D_inABC.PROV_ADDR_FAX_NO").alias("PROV_ADDR_FAX_NO"),
    col("lnk_Prov_Dir_D_inABC.PROV_ADDR_EMAIL_ADDR_TX").alias("PROV_ADDR_EMAIL_ADDR_TX"),
    col("lnk_Prov_Dir_D_inABC.PROV_ADDR_FAX_NO_EXT").alias("PROV_ADDR_FAX_NO_EXT"),
    col("lnk_Prov_Dir_D_inABC.PROV_ADDR_PHN_NO_EXT").alias("PROV_ADDR_PHN_NO_EXT"),
    col("lnk_Prov_Dir_D_inABC.PROV_ADDR_PRCTC_LOC_IN").alias("PROV_ADDR_PRCTC_LOC_IN"),
    col("lnk_Prov_Dir_D_inABC.PROV_MAIL_ADDR_EFF_DT_SK").alias("PROV_MAIL_ADDR_EFF_DT_SK"),
    col("lnk_Prov_Dir_D_inABC.PROV_PRCTC_TYP_CD").alias("PROV_PRCTC_TYP_CD"),
    col("lnk_Prov_Dir_D_inABC.PROV_ADDR_GEO_ACES_RTRN_CD_TX").alias("PROV_ADDR_GEO_ACES_RTRN_CD_TX"),
    col("lnk_Prov_Dir_D_inABC.PROV_ADDR_LAT_TX").alias("PROV_ADDR_LAT_TX"),
    col("lnk_Prov_Dir_D_inABC.PROV_ADDR_LONG_TX").alias("PROV_ADDR_LONG_TX"),
    col("lnk_Prov_Dir_D_inABC.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("lnk_EdwCmn_Prct_Extr_InABC.CMN_PRCT_BRTH_DT_SK").alias("CMN_PRCT_BRTH_DT_SK"),
    col("lnk_EdwCmn_Prct_Extr_InABC.CMN_PRCT_GNDR_CD").alias("CMN_PRCT_GNDR_CD"),
    col("lnk_EdwCmn_Prct_Extr_InABC.CMN_PRCT_SSN").alias("CMN_PRCT_SSN"),
    col("lnk_EdwCmn_Prct_Extr_InABC.CMN_PRCT_TTL").alias("CMN_PRCT_TTL"),
    col("lnk_EdwCmn_Prct_Extr_InABC.CMN_PRCT_FIRST_NM").alias("CMN_PRCT_FIRST_NM"),
    col("lnk_EdwCmn_Prct_Extr_InABC.CMN_PRCT_MIDINIT").alias("CMN_PRCT_MIDINIT"),
    col("lnk_EdwCmn_Prct_Extr_InABC.CMN_PRCT_LAST_NM").alias("CMN_PRCT_LAST_NM"),
    col("lnk_EdwCmn_Prct_Extr_InABC.CMN_PRCT_LAST_CRDTL_DT_SK").alias("CMN_PRCT_LAST_CRDTL_DT_SK"),
    col("lnk_EdwPcmh_Extr_InABC.PROV_SK").alias("PROV_SK_pcmh"),
    col("lnk_EdwPcmh_Extr_InABC.NTWK_ID").alias("NTWK_ID_pcmh"),
    col("lnk_EdwEnty_Reg_Extr_InABC.ENTY_RGSTRN_TYP_CD").alias("ENTY_RGSTRN_TYP_CD"),
    col("lnk_rmd_Enty_Lic1_out.ENTY_LIC_NO").alias("ENTY_LIC_NO"),
    col("lnk_rmd_Enty_Lic1_out.ENTY_LIC_ST_CD").alias("ENTY_LIC_ST_CD"),
    col("lnk_rmd_Enty_Lic2_out.ENTY_LIC_NO").alias("ENTY_LIC_NO_1"),
    col("lnk_rmd_Enty_Lic2_out.ENTY_LIC_ST_CD").alias("ENTY_LIC_ST_CD_1"),
    col("lnk_Prov_Dir_D_inABC.PROV_ENTY_CD").alias("PROV_ENTY_CD")
)

# xfrm_BusinessLogic
# Create base transformations
df_xfrm_base = df_lnk_CodesLkpData_out.withColumn(
    "svPcmh",
    when(
        isnull(col("PROV_SK_pcmh")) | isnull(col("NTWK_ID_pcmh")),
        lit("N")
    ).otherwise(lit("Y"))
)

# Create the three output links from xfrm_BusinessLogic

# Common column transformations for xfrm_BusinessLogic
# We'll build them once, then apply the different constraints.
df_with_columns = df_xfrm_base.withColumn("PROV_DIR_SK", lit(0)) \
.withColumn("PROV_ID", col("PROV_ID")) \
.withColumn("PROV_ADDR_TYP_CD", col("PROV_ADDR_TYP_CD")) \
.withColumn("PROV_ADDR_EFF_DT_SK", col("PROV_ADDR_EFF_DT_SK")) \
.withColumn("PROV_ADDR_TERM_DT_SK", col("PROV_ADDR_TERM_DT_SK")) \
.withColumn("NTWK_ID", col("NTWK_ID")) \
.withColumn("PROV_NTWK_TERM_DT_SK", col("PROV_NTWK_TERM_DT_SK")) \
.withColumn("PROV_NTWK_PFX_ID", col("PROV_NTWK_PFX_ID")) \
.withColumn("SRC_SYS_CD", col("SRC_SYS_CD")) \
.withColumn("CRT_RUN_CYC_EXCTN_DT_SK", lit(EDWRunCycleDate)) \
.withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", lit(EDWRunCycleDate)) \
.withColumn("PROV_SPEC_NM", col("PROV_SPEC_NM")) \
.withColumn("PROV_REL_GRP_PROV_ID",
    when(
        isnull(col("PROV_REL_GRP_PROV_ID"))
        | (col("PROV_REL_GRP_PROV_ID") == lit("NA"))
        | (col("PROV_REL_GRP_PROV_ID") == lit("UNK")),
        lit(" ")
    ).otherwise(col("PROV_REL_GRP_PROV_ID"))
) \
.withColumn("PROV_REL_GRP_PROV_NM", col("PROV_REL_GRP_PROV_NM")) \
.withColumn("PROV_NM", col("PROV_NM")) \
.withColumn("CMN_PRCT_TTL", col("CMN_PRCT_TTL")) \
.withColumn("CMN_PRCT_SSN",
    when(
        isnull(col("CMN_PRCT_SSN"))
        | (col("CMN_PRCT_SSN") == lit("NA"))
        | (col("CMN_PRCT_SSN") == lit("UNK")),
        lit(" ")
    ).otherwise(col("CMN_PRCT_SSN"))
) \
.withColumn("CMN_PRCT_GNDR_CD",
    when(isnull(col("CMN_PRCT_GNDR_CD")), lit("NA"))
    .otherwise(col("CMN_PRCT_GNDR_CD"))
) \
.withColumn("CMN_PRCT_BRTH_DT_SK",
    when(
        isnull(col("CMN_PRCT_BRTH_DT_SK"))
        | (trim(col("CMN_PRCT_BRTH_DT_SK")) == lit("NA"))
        | (trim(col("CMN_PRCT_BRTH_DT_SK")) == lit("UNK"))
        | (trim(col("CMN_PRCT_BRTH_DT_SK")) == lit("")),
        lit("1753-01-01")
    ).otherwise(col("CMN_PRCT_BRTH_DT_SK"))
) \
.withColumn("PROV_ADDR_LN_1", col("PROV_ADDR_LN_1")) \
.withColumn("PROV_ADDR_LN_2", col("PROV_ADDR_LN_2")) \
.withColumn("PROV_ADDR_LN_3", col("PROV_ADDR_LN_3")) \
.withColumn("PROV_ADDR_CITY_NM", col("PROV_ADDR_CITY_NM")) \
.withColumn("PROV_ADDR_ST_CD", col("PROV_ADDR_ST_CD")) \
.withColumn("PROV_ADDR_CNTY_NM", col("PROV_ADDR_CNTY_NM")) \
.withColumn("PROV_ADDR_ZIP_CD_5", col("PROV_ADDR_ZIP_CD_5")) \
.withColumn("PROV_ADDR_ZIP_CD_4", col("PROV_ADDR_ZIP_CD_4")) \
.withColumn("PROV_ADDR_PHN_NO",
    when(isnull(col("PROV_ADDR_PHN_NO")), lit(""))
    .otherwise(trim(col("PROV_ADDR_PHN_NO")))
) \
.withColumn("PROV_ADDR_FAX_NO",
    when(isnull(col("PROV_ADDR_FAX_NO")), lit(""))
    .otherwise(trim(col("PROV_ADDR_FAX_NO")))
) \
.withColumn("PROV_ADDR_HCAP_IN", col("PROV_ADDR_HCAP_IN")) \
.withColumn("PROV_ENTY_CD", col("PROV_ENTY_CD")) \
.withColumn("PAR_PROV_IN", lit("Y")) \
.withColumn("PROV_NTWK_PCP_IN", col("PROV_NTWK_PCP_IN")) \
.withColumn("PROV_TAX_ID", col("PROV_TAX_ID")) \
.withColumn("ENTY_LIC_ST_CD_1",
    when(
        (col("PROV_ENTY_CD") == lit("F")) | (col("PROV_ENTY_CD") == lit("G")),
        lit(" ")
    ).otherwise(
        when(
            (trim(col("ENTY_LIC_ST_CD")) == lit(""))
            | (col("ENTY_LIC_ST_CD") == lit("NA"))
            | (col("ENTY_LIC_ST_CD") == lit("UNK")),
            lit(" ")
        ).otherwise(col("ENTY_LIC_ST_CD"))
    )
) \
.withColumn("ENTY_LIC_NO_1",
    when(
        (col("PROV_ENTY_CD") == lit("F")) | (col("PROV_ENTY_CD") == lit("G")),
        lit(" ")
    ).otherwise(
        when(
            (trim(col("ENTY_LIC_NO")) == lit(""))
            | (col("ENTY_LIC_NO") == lit("NA"))
            | (col("ENTY_LIC_NO") == lit("UNK")),
            lit(" ")
        ).otherwise(col("ENTY_LIC_NO"))
    )
) \
.withColumn("ENTY_LIC_ST_CD_2",
    when(
        (col("PROV_ENTY_CD") == lit("F")) | (col("PROV_ENTY_CD") == lit("G")),
        lit(" ")
    ).otherwise(
        when(
            (trim(col("ENTY_LIC_ST_CD_1")) == lit(""))
            | (col("ENTY_LIC_ST_CD_1") == lit("NA"))
            | (col("ENTY_LIC_ST_CD_1") == lit("UNK")),
            lit(" ")
        ).otherwise(col("ENTY_LIC_ST_CD_1"))
    )
) \
.withColumn("ENTY_LIC_NO_2",
    when(
        (col("PROV_ENTY_CD") == lit("F")) | (col("PROV_ENTY_CD") == lit("G")),
        lit(" ")
    ).otherwise(
        when(
            (trim(col("ENTY_LIC_NO_1")) == lit(""))
            | (col("ENTY_LIC_NO_1") == lit("NA"))
            | (col("ENTY_LIC_NO_1") == lit("UNK")),
            lit(" ")
        ).otherwise(col("ENTY_LIC_NO_1"))
    )
) \
.withColumn("PROV_NTWK_ACPTNG_PATN_IN", col("PROV_NTWK_ACPTNG_PATN_IN")) \
.withColumn("PROV_NTWK_ACPTNG_MCAID_PATN_IN", col("PROV_NTWK_ACPTNG_MCAID_PATN_IN")) \
.withColumn("PROV_NTWK_ACPTNG_MCARE_PATN_IN", col("PROV_NTWK_ACPTNG_MCARE_PATN_IN")) \
.withColumn("PROV_NTWK_MAX_PATN_QTY",
    when(col("PROV_NTWK_MAX_PATN_QTY") > lit(999), lit(999))
    .otherwise(col("PROV_NTWK_MAX_PATN_QTY"))
) \
.withColumn("PROV_NTWK_MAX_PATN_AGE", col("PROV_NTWK_MAX_PATN_AGE")) \
.withColumn("PROV_TYP_CD", col("PROV_TYP_CD")) \
.withColumn("PROV_TYP_NM", col("PROV_TYP_NM")) \
.withColumn("PROV_FCLTY_TYP_CD", col("PROV_FCLTY_TYP_CD")) \
.withColumn("PROV_FCLTY_TYP_NM", col("PROV_FCLTY_TYP_NM")) \
.withColumn("PROV_SPEC_CD", col("PROV_SPEC_CD")) \
.withColumn("PROV_NTWK_MIN_PATN_AGE", col("PROV_NTWK_MIN_PATN_AGE")) \
.withColumn("PROV_NTWK_GNDR_ACPTD_CD", col("PROV_NTWK_GNDR_ACPTD_CD")) \
.withColumn("EXTR_DT", lit("1753-01-01")) \
.withColumn("CMN_PRCT_ID", col("CMN_PRCT_ID")) \
.withColumn("LEAPFROG_IN",
    when(trim(col("ENTY_RGSTRN_TYP_CD")) == lit(""), lit("N"))
    .otherwise(lit("Y"))
) \
.withColumn("PROV_SK", col("PROV_SK")) \
.withColumn("PROV_ADDR_GEO_ACES_RTRN_CD_TX", col("PROV_ADDR_GEO_ACES_RTRN_CD_TX")) \
.withColumn("PROV_ADDR_LAT_TX", col("PROV_ADDR_LAT_TX")) \
.withColumn("PROV_ADDR_LONG_TX", col("PROV_ADDR_LONG_TX")) \
.withColumn("CRT_RUN_CYC_EXCTN_SK", lit(EDWRunCycle)) \
.withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(EDWRunCycle)) \
.withColumn("PCMH_IN",
    when(col("svPcmh") == lit("Y"), lit("Y")).otherwise(lit("N"))
)

# Link lnk_Main_Data_out => constraint: IsNotNull(PROV_ADDR_TYP_CD)
df_lnk_Main_Data_out = df_with_columns.filter(col("PROV_ADDR_TYP_CD").isNotNull())

# For the other two outputs, each picks the first row. We replicate the constraints exactly.
w_xfrm = Window.orderBy(lit(1))
df_numbered = df_with_columns.withColumn("row_num", row_number().over(w_xfrm))

df_UNK_Row = df_numbered.filter(col("row_num") == 1).drop("row_num")
df_NA_Row = df_numbered.filter(col("row_num") == 1).drop("row_num")

# Adjust columns for UNK_Row
# The JSON specifies a literal for each column, so we override them on a separate select
df_UNK_Row = df_UNK_Row.select(
    lit(0).alias("PROV_DIR_SK"),
    lit("UNK").alias("PROV_ID"),
    lit("UNK").alias("PROV_ADDR_TYP_CD"),
    rpad(lit("1753-01-01"),10," ").alias("PROV_ADDR_EFF_DT_SK"),
    rpad(lit("1753-01-01"),10," ").alias("PROV_ADDR_TERM_DT_SK"),
    lit("UNK").alias("NTWK_ID"),
    rpad(lit("1753-01-01"),10," ").alias("PROV_NTWK_TERM_DT_SK"),
    lit("UNK").alias("PROV_NTWK_PFX_ID"),
    lit("UNK").alias("SRC_SYS_CD"),
    rpad(lit("1753-01-01"),10," ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad(lit(EDWRunCycleDate),10," ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    lit("UNK").alias("PROV_SPEC_NM"),
    lit("UNK").alias("PROV_REL_GRP_PROV_ID"),
    lit("UNK").alias("PROV_REL_GRP_PROV_NM"),
    lit("UNK").alias("PROV_NM"),
    lit("UNK").alias("CMN_PRCT_TTL"),
    lit("UNK").alias("CMN_PRCT_SSN"),
    lit("UNK").alias("CMN_PRCT_GNDR_CD"),
    rpad(lit("1753-01-01"),10," ").alias("CMN_PRCT_BRTH_DT_SK"),
    lit("UNK").alias("PROV_ADDR_LN_1"),
    lit("UNK").alias("PROV_ADDR_LN_2"),
    lit("UNK").alias("PROV_ADDR_LN_3"),
    lit("UNK").alias("PROV_ADDR_CITY_NM"),
    lit("UNK").alias("PROV_ADDR_ST_CD"),
    lit("UNK").alias("PROV_ADDR_CNTY_NM"),
    rpad(lit("UNK"),5," ").alias("PROV_ADDR_ZIP_CD_5"),
    rpad(lit("UNK"),4," ").alias("PROV_ADDR_ZIP_CD_4"),
    lit("UNK").alias("PROV_ADDR_PHN_NO"),
    lit("UNK").alias("PROV_ADDR_FAX_NO"),
    rpad(lit("N"),1," ").alias("PROV_ADDR_HCAP_IN"),
    lit("UNK").alias("PROV_ENTY_CD"),
    rpad(lit("N"),1," ").alias("PAR_PROV_IN"),
    rpad(lit("N"),1," ").alias("PROV_NTWK_PCP_IN"),
    lit("UNK").alias("PROV_TAX_ID"),
    lit("UNK").alias("ENTY_LIC_ST_CD_1"),
    lit("UNK").alias("ENTY_LIC_NO_1"),
    lit("UNK").alias("ENTY_LIC_ST_CD_2"),
    lit("UNK").alias("ENTY_LIC_NO_2"),
    rpad(lit("N"),1," ").alias("PROV_NTWK_ACPTNG_PATN_IN"),
    rpad(lit("N"),1," ").alias("PROV_NTWK_ACPTNG_MCAID_PATN_IN"),
    rpad(lit("N"),1," ").alias("PROV_NTWK_ACPTNG_MCARE_PATN_IN"),
    lit("UNK").alias("PROV_NTWK_MAX_PATN_QTY"),
    lit("UNK").alias("PROV_NTWK_MAX_PATN_AGE"),
    lit("UNK").alias("PROV_TYP_CD"),
    lit("UNK").alias("PROV_TYP_NM"),
    lit("UNK").alias("PROV_FCLTY_TYP_CD"),
    lit("UNK").alias("PROV_FCLTY_TYP_NM"),
    lit("UNK").alias("PROV_SPEC_CD"),
    lit("UNK").alias("PROV_NTWK_MIN_PATN_AGE"),
    lit("UNK").alias("PROV_NTWK_GNDR_ACPTD_CD"),
    lit("1753-01-01").alias("EXTR_DT"),
    lit("UNK").alias("CMN_PRCT_ID"),
    rpad(lit("N"),1," ").alias("LEAPFROG_IN"),
    lit(0).alias("PROV_SK"),
    lit("UNK").alias("PROV_ADDR_GEO_ACES_RTRN_CD_TX"),
    lit(0).alias("PROV_ADDR_LAT_TX"),
    lit(0).alias("PROV_ADDR_LONG_TX"),
    lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(lit("N"),1," ").alias("PCMH_IN")
)

df_NA_Row = df_NA_Row.select(
    lit(1).alias("PROV_DIR_SK"),
    lit("NA").alias("PROV_ID"),
    lit("NA").alias("PROV_ADDR_TYP_CD"),
    rpad(lit("1753-01-01"),10," ").alias("PROV_ADDR_EFF_DT_SK"),
    rpad(lit("1753-01-01"),10," ").alias("PROV_ADDR_TERM_DT_SK"),
    lit("NA").alias("NTWK_ID"),
    rpad(lit("1753-01-01"),10," ").alias("PROV_NTWK_TERM_DT_SK"),
    lit("NA").alias("PROV_NTWK_PFX_ID"),
    lit("NA").alias("SRC_SYS_CD"),
    rpad(lit("1753-01-01"),10," ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad(lit(EDWRunCycleDate),10," ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    lit("NA").alias("PROV_SPEC_NM"),
    lit("NA").alias("PROV_REL_GRP_PROV_ID"),
    lit("NA").alias("PROV_REL_GRP_PROV_NM"),
    lit("NA").alias("PROV_NM"),
    lit("NA").alias("CMN_PRCT_TTL"),
    lit("NA").alias("CMN_PRCT_SSN"),
    lit("NA").alias("CMN_PRCT_GNDR_CD"),
    rpad(lit("1753-01-01"),10," ").alias("CMN_PRCT_BRTH_DT_SK"),
    lit("NA").alias("PROV_ADDR_LN_1"),
    lit("NA").alias("PROV_ADDR_LN_2"),
    lit("NA").alias("PROV_ADDR_LN_3"),
    lit("NA").alias("PROV_ADDR_CITY_NM"),
    lit("NA").alias("PROV_ADDR_ST_CD"),
    lit("NA").alias("PROV_ADDR_CNTY_NM"),
    rpad(lit("NA"),5," ").alias("PROV_ADDR_ZIP_CD_5"),
    rpad(lit("NA"),4," ").alias("PROV_ADDR_ZIP_CD_4"),
    lit("NA").alias("PROV_ADDR_PHN_NO"),
    lit("NA").alias("PROV_ADDR_FAX_NO"),
    rpad(lit("N"),1," ").alias("PROV_ADDR_HCAP_IN"),
    lit("NA").alias("PROV_ENTY_CD"),
    rpad(lit("N"),1," ").alias("PAR_PROV_IN"),
    rpad(lit("N"),1," ").alias("PROV_NTWK_PCP_IN"),
    lit("NA").alias("PROV_TAX_ID"),
    lit("NA").alias("ENTY_LIC_ST_CD_1"),
    lit("NA").alias("ENTY_LIC_NO_1"),
    lit("NA").alias("ENTY_LIC_ST_CD_2"),
    lit("NA").alias("ENTY_LIC_NO_2"),
    rpad(lit("N"),1," ").alias("PROV_NTWK_ACPTNG_PATN_IN"),
    rpad(lit("N"),1," ").alias("PROV_NTWK_ACPTNG_MCAID_PATN_IN"),
    rpad(lit("N"),1," ").alias("PROV_NTWK_ACPTNG_MCARE_PATN_IN"),
    lit("NA").alias("PROV_NTWK_MAX_PATN_QTY"),
    lit("NA").alias("PROV_NTWK_MAX_PATN_AGE"),
    lit("NA").alias("PROV_TYP_CD"),
    lit("NA").alias("PROV_TYP_NM"),
    lit("NA").alias("PROV_FCLTY_TYP_CD"),
    lit("NA").alias("PROV_FCLTY_TYP_NM"),
    lit("NA").alias("PROV_SPEC_CD"),
    lit("NA").alias("PROV_NTWK_MIN_PATN_AGE"),
    lit("NA").alias("PROV_NTWK_GNDR_ACPTD_CD"),
    lit("1753-01-01").alias("EXTR_DT"),
    lit("NA").alias("CMN_PRCT_ID"),
    rpad(lit("N"),1," ").alias("LEAPFROG_IN"),
    lit(1).alias("PROV_SK"),
    lit("NA").alias("PROV_ADDR_GEO_ACES_RTRN_CD_TX"),
    lit(0).alias("PROV_ADDR_LAT_TX"),
    lit(0).alias("PROV_ADDR_LONG_TX"),
    lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(lit("N"),1," ").alias("PCMH_IN")
)

# Funnel: fnl_NA_UNK => union these three
df_fnl_NA_UNK = df_UNK_Row.unionByName(df_NA_Row).unionByName(df_lnk_Main_Data_out)

# Now we must produce the final select with rpad for the columns that have SqlType=char or varchar in the funnel's definition.

df_fnl_select = df_fnl_NA_UNK.select(
    col("PROV_DIR_SK").alias("PROV_DIR_SK"),
    col("PROV_ID").alias("PROV_ID"),
    col("PROV_ADDR_TYP_CD").alias("PROV_ADDR_TYP_CD"),
    rpad(col("PROV_ADDR_EFF_DT_SK"),10," ").alias("PROV_ADDR_EFF_DT_SK"),
    rpad(col("PROV_ADDR_TERM_DT_SK"),10," ").alias("PROV_ADDR_TERM_DT_SK"),
    col("NTWK_ID").alias("NTWK_ID"),
    rpad(col("PROV_NTWK_TERM_DT_SK"),10," ").alias("PROV_NTWK_TERM_DT_SK"),
    col("PROV_NTWK_PFX_ID").alias("PROV_NTWK_PFX_ID"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"),10," ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),10," ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("PROV_SPEC_NM").alias("PROV_SPEC_NM"),
    col("PROV_REL_GRP_PROV_ID").alias("PROV_REL_GRP_PROV_ID"),
    col("PROV_REL_GRP_PROV_NM").alias("PROV_REL_GRP_PROV_NM"),
    col("PROV_NM").alias("PROV_NM"),
    col("CMN_PRCT_TTL").alias("CMN_PRCT_TTL"),
    col("CMN_PRCT_SSN").alias("CMN_PRCT_SSN"),
    col("CMN_PRCT_GNDR_CD").alias("CMN_PRCT_GNDR_CD"),
    rpad(col("CMN_PRCT_BRTH_DT_SK"),10," ").alias("CMN_PRCT_BRTH_DT_SK"),
    col("PROV_ADDR_LN_1").alias("PROV_ADDR_LN_1"),
    col("PROV_ADDR_LN_2").alias("PROV_ADDR_LN_2"),
    col("PROV_ADDR_LN_3").alias("PROV_ADDR_LN_3"),
    col("PROV_ADDR_CITY_NM").alias("PROV_ADDR_CITY_NM"),
    col("PROV_ADDR_ST_CD").alias("PROV_ADDR_ST_CD"),
    col("PROV_ADDR_CNTY_NM").alias("PROV_ADDR_CNTY_NM"),
    rpad(col("PROV_ADDR_ZIP_CD_5"),5," ").alias("PROV_ADDR_ZIP_CD_5"),
    rpad(col("PROV_ADDR_ZIP_CD_4"),4," ").alias("PROV_ADDR_ZIP_CD_4"),
    col("PROV_ADDR_PHN_NO").alias("PROV_ADDR_PHN_NO"),
    col("PROV_ADDR_FAX_NO").alias("PROV_ADDR_FAX_NO"),
    rpad(col("PROV_ADDR_HCAP_IN"),1," ").alias("PROV_ADDR_HCAP_IN"),
    col("PROV_ENTY_CD").alias("PROV_ENTY_CD"),
    rpad(col("PAR_PROV_IN"),1," ").alias("PAR_PROV_IN"),
    rpad(col("PROV_NTWK_PCP_IN"),1," ").alias("PROV_NTWK_PCP_IN"),
    col("PROV_TAX_ID").alias("PROV_TAX_ID"),
    col("ENTY_LIC_ST_CD_1").alias("ENTY_LIC_ST_CD_1"),
    col("ENTY_LIC_NO_1").alias("ENTY_LIC_NO_1"),
    col("ENTY_LIC_ST_CD_2").alias("ENTY_LIC_ST_CD_2"),
    col("ENTY_LIC_NO_2").alias("ENTY_LIC_NO_2"),
    rpad(col("PROV_NTWK_ACPTNG_PATN_IN"),1," ").alias("PROV_NTWK_ACPTNG_PATN_IN"),
    rpad(col("PROV_NTWK_ACPTNG_MCAID_PATN_IN"),1," ").alias("PROV_NTWK_ACPTNG_MCAID_PATN_IN"),
    rpad(col("PROV_NTWK_ACPTNG_MCARE_PATN_IN"),1," ").alias("PROV_NTWK_ACPTNG_MCARE_PATN_IN"),
    col("PROV_NTWK_MAX_PATN_QTY").alias("PROV_NTWK_MAX_PATN_QTY"),
    col("PROV_NTWK_MAX_PATN_AGE").alias("PROV_NTWK_MAX_PATN_AGE"),
    col("PROV_TYP_CD").alias("PROV_TYP_CD"),
    col("PROV_TYP_NM").alias("PROV_TYP_NM"),
    col("PROV_FCLTY_TYP_CD").alias("PROV_FCLTY_TYP_CD"),
    col("PROV_FCLTY_TYP_NM").alias("PROV_FCLTY_TYP_NM"),
    col("PROV_SPEC_CD").alias("PROV_SPEC_CD"),
    col("PROV_NTWK_MIN_PATN_AGE").alias("PROV_NTWK_MIN_PATN_AGE"),
    col("PROV_NTWK_GNDR_ACPTD_CD").alias("PROV_NTWK_GNDR_ACPTD_CD"),
    col("EXTR_DT").alias("EXTR_DT"),
    col("CMN_PRCT_ID").alias("CMN_PRCT_ID"),
    rpad(col("LEAPFROG_IN"),1," ").alias("LEAPFROG_IN"),
    col("PROV_SK").alias("PROV_SK"),
    col("PROV_ADDR_GEO_ACES_RTRN_CD_TX").alias("PROV_ADDR_GEO_ACES_RTRN_CD_TX"),
    col("PROV_ADDR_LAT_TX").alias("PROV_ADDR_LAT_TX"),
    col("PROV_ADDR_LONG_TX").alias("PROV_ADDR_LONG_TX"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(col("PCMH_IN"),1," ").alias("PCMH_IN")
)

# PxDataSet: seq_PROV_DIR_D => translate to parquet
write_files(
    df_fnl_select,
    f"{adls_path}/ds/PROV_DIR_D.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)