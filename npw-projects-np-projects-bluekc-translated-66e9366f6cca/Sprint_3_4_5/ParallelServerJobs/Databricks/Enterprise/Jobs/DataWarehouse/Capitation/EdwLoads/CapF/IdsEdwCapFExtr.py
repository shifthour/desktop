# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2005, 2011 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by:
# MAGIC                     EdwCapExtrSeq
# MAGIC 
# MAGIC Processing:
# MAGIC                     *
# MAGIC 
# MAGIC Control Job Rerun Information: 
# MAGIC                     Previous Run Successful:    Restart, no other steps necessary
# MAGIC                     Previous Run Aborted:         Restart, no other steps necessary
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                                  Project/                                                                                                                                                                                                          Code                  Date
# MAGIC Developer           Date              Altiris #                    Change Description                                                                                                               Project                              Reviewer            Reviewed
# MAGIC -------------------------  -------------------   -------------                   --------------------------------------------------------------------------------------------------------------------------------------------  ---------------------------------------- -------------------------  -------------------
# MAGIC Ralph Tucker     08/02/2004                                   Originally Programmed
# MAGIC Ralph Tucker     01/30/2006                                   Took out the MBR & GRP Start Date Hash file build and lookup; replaced dates with 'NA'
# MAGIC Ralph Tucker     05/31/2006                                  If invalid lookup on Exprnce_Cat_Lookup then move space to Experence Category Code
# MAGIC Ralph Tucker     2010-12-23    TTR-556                 Removed fields: GRP_EDW_RCRD_ST_DT_SK 
# MAGIC                                                                                            and MBR_EDW_RCRD_ST_DT_SK                           
# MAGIC Hugh Sisson       2011-02-16    4663                       Added new output stream to flat file to prevent PCMH data from being loaded to CAP_F                                             Steph Goddard     02/22/2011  
# MAGIC                                                                                  Restored the two fields, added null-checking to numerous fields,
# MAGIC                                                                                 Updated Performance values - Buffer Size to 512 and Timeout to 300
# MAGIC                                                                                 Changed Prefetch to 10000 from 50, 
# MAGIC                                                                                  Added Job Parameter SrcSysCd with default of FACETS
# MAGIC                                                                                 Deleted two used Job Parameters - BeginDate and EndDate
# MAGIC                                                                                 Added 3 hashed files to HASH.CLEAR - hf_cap_grp, hf_cap_mbr, and  hf_clndr_dt
# MAGIC                                                                                 Update column definitons for NTWK and PROV queries
# MAGIC 
# MAGIC Ralph Tucker   2011-03-01    TTR-1031                  Select IDS Cap rows if > or = extract run cycle                                                                       EnterpriseNewDevl       Steph Goddard       03/08/2011
# MAGIC 
# MAGIC Ralph Tucker   2011-03-22    TTR-1058               Add IDS/EDW Run Cycle parameters; change Schd/Pool code lookups to IDS                   EnterpriseNewDevl
# MAGIC 
# MAGIC Rishi Reddy      2011-06-20    4663                        Added CAP_FUND_ACCTG_CAT_CD, CAP_FUND_ACCTG_CAT_NM,                              EnterpsierCurDevl          Brent Leland          07-12-2011
# MAGIC                                                                               GL_CAT_CD, GL_CAT_NM, CAP_FUND_ACCTG_CAT_CD_SK, GL_CAT_CD_SK
# MAGIC 
# MAGIC Lee Moore       2013-07-26      5114                        Rewrite in parallel                                                                                                                  EnterpriseWrhsDevl      Bhoomi Dasari        9/6/2013 
# MAGIC 
# MAGIC Jag Yelavarthi  2014-03-19   TFS#1100                Added PROD_SH_NM_SK column to the extract process to target table                              EnterpriseNewDevl          Kalyan Neelam       2014-05-12
# MAGIC 
# MAGIC Santosh Bokka  2014-06-11         4917                     Added BCBSKC Runcyle to load TREO Historical file into CAP_F table.                             EnterpriseNewDevl         Bhoomi Dasari      6/23/2014 
# MAGIC 
# MAGIC 
# MAGIC Karthik Chintalapani  2016-01-11  5212     Added the BCBSA Runcycle job to process the Inter Plan Bill Detail load into the CAP_F table.        EnterpriseDev1          Kalyan Neelam       2016-01-19      
# MAGIC                                                                   Also removed the join and filters for the P_RUN_CYC table since it is not needed     
# MAGIC 
# MAGIC Karthik Chintalapani  2016-10-07  5212     Changed the SRC_CD to \(2018)9999\(2019) in the BCBSA_db2_CD_MPPNG_in stage to retrieve                     EnterpriseDev1           Kalyan Neelam      2016-10-11
# MAGIC                                                                   EXCPT2GNRLPRCS  for CAP_FUND_ACCTG_CAT_CD column population.
# MAGIC 
# MAGIC                                                                                  
# MAGIC 
# MAGIC 
# MAGIC Manasa Andru        2018-08-20    60037                Added LFSTYL_RATE_FCTR_SK field at the end.                                                                 EnterpriseDev2 \(9)Abhiram Dasarathy\(9)2018-08-24
# MAGIC                                           Capitation/Attribution Support
# MAGIC 
# MAGIC Tim Sieg                  2019-04-04          101615           Adding new column, CAP_RECOV_CAP_AMT, in xfm_BusinessLogic stage to be      EnterpriseDev2                 Kalyan Neelam     2019-04-09
# MAGIC                                                                                             included in the load file to match the table definition of the target table, CAP_F.
# MAGIC 
# MAGIC Manasa Andru       2019-04-24         60037              Corrrected the extract SQL by passing the #IDSBCBSKCRunCycle# which was accidentally   EnterpriseDev2      Jaideep Mankala    04/24/2019
# MAGIC                                                      Capitation/Attribution Support                hard-coded in previous version.
# MAGIC 
# MAGIC Sravya Gorla        2019-05-23       Capitation/Attribution Support      Updated cap_prov_id and prob_sk fileds logic to populate pcp data             EnterpriseDev2        Kalyan Neelam      2019-05-28
# MAGIC                                                                                                         when cap_prov_id like 'MHA%'     
# MAGIC 
# MAGIC /Ashok kumar           2020-07-22      Capitation/Attribution Support    Added new extraction for Mntlhlth from facets                                                EnterpriseDev2           Jaideep Mankala       07/23/2020
# MAGIC  /                                                                                             
# MAGIC Sharon Andrew         2021-02-02        MA             Added LUMERIS as source system to process  for Cap and Cap Fund table                                EnterpriseDev2     Kalyan Neelam      2021-02-25
# MAGIC                                                                              None of the other tables will contain LHO / Lumeris Capitation data.
# MAGIC                                                                              new controller  job activity Variables.IDSLumerisRunCycle added to IdsEdwCapExtrSeq, passed down to IdsEdwCapFExtr  
# MAGIC                                                                               IDSLumerisRunCycle added to IdsEdwCapFExtr  main Where clause when extracting from IDS.CAP and IDS.CAP_FUND

# MAGIC Adding new CAP_RECOV_CAP_AMT column, set to null, will be updated in EdwCapFSpiraBillUpdtCntl
# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC CD_MPPNG_SK
# MAGIC refCapCatCd
# MAGIC refFundAcctCatCd
# MAGIC refAdjTypCd
# MAGIC refCapAdjRsnCd
# MAGIC refAdjSttusCd
# MAGIC refMbrGndrCd
# MAGIC refPerdCd
# MAGIC refCapTypCd
# MAGIC refCatCd
# MAGIC refCopayTypCd
# MAGIC refGlCatCd
# MAGIC refSrcSys
# MAGIC refLobCd
# MAGIC Write CAP_F  Data into a Sequential file for Load Job IdsEdwCapFundDLoad.
# MAGIC Read all the Data from IDS CAP Table;
# MAGIC Add Defaults and Null Handling.
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Job Name: IdsEdwCapFExtr
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType, LongType, DateType, TimestampType, DoubleType
from pyspark.sql import Row
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


# MAGIC %run ../../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

# Retrieve parameters
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
IDSBCBSKCRunCycle = get_widget_value('IDSBCBSKCRunCycle','')
IDSBCBSARunCycle = get_widget_value('IDSBCBSARunCycle','')
IDSLumerisRunCycle = get_widget_value('IDSLumerisRunCycle','')

# Acquire JDBC configuration once for IDS
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

# db2_CD_MPPNG_in
extract_query_db2_CD_MPPNG_in = f"""
SELECT 
  CD_MPPNG_SK,
  COALESCE(TRGT_CD,'UNK') as TRGT_CD,
  COALESCE(TRGT_CD_NM,'UNK') as TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG
"""
df_db2_CD_MPPNG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_CD_MPPNG_in)
    .load()
)

# cpy_cd_mppng (PxCopy) - replicate 12 output links
df_cpy_cd_mppng = df_db2_CD_MPPNG_in

df_lnk_refCatCd = df_cpy_cd_mppng.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_lnk_refGlCatCd = df_cpy_cd_mppng.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_lnk_refLobCd = df_cpy_cd_mppng.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_lnk_refMbrGndrCd = df_cpy_cd_mppng.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_lnk_refAdjTypCd = df_cpy_cd_mppng.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_lnk_refFundAcctCatCd = df_cpy_cd_mppng.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_lnk_refCapCatCd = df_cpy_cd_mppng.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_lnk_refCapAdjRsnCd = df_cpy_cd_mppng.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_lnk_refAdjSttusCd = df_cpy_cd_mppng.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_lnk_refPerdCd = df_cpy_cd_mppng.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_lnk_refCapTypCd = df_cpy_cd_mppng.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_lnk_refSrcSys = df_cpy_cd_mppng.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_lnk_refCopayTypCd = df_cpy_cd_mppng.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

# db2_refGrpNm
extract_query_db2_refGrpNm = f"""
SELECT
  GRP_SK,
  GRP_ID,
  GRP_NM
FROM {IDSOwner}.GRP
"""
df_lnk_refGrpNm = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_refGrpNm)
    .load()
)

# db2_refFnclLobCd
extract_query_db2_refFnclLobCd = f"""
SELECT 
  FNCL_LOB_SK,
  FNCL_LOB_CD
FROM {IDSOwner}.FNCL_LOB
"""
df_lnk_refFnclLobCd = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_refFnclLobCd)
    .load()
)

# db2_refCapSchdCd
extract_query_db2_refCapSchdCd = f"""
SELECT 
  CAP_SCHD_CD_SK,
  CAP_SCHD_CD,
  SRC_SYS_CD_SK,
  CRT_RUN_CYC_EXCTN_SK,
  LAST_UPDT_RUN_CYC_EXCTN_SK,
  CAP_SCHD_NM
FROM {IDSOwner}.CAP_SCHD_CD
"""
df_lnk_refCapSchdCd = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_refCapSchdCd)
    .load()
)

# db2_refCapPoolCd
extract_query_db2_refCapPoolCd = f"""
SELECT
  CAP_POOL_CD_SK,
  CAP_POOL_CD,
  SRC_SYS_CD_SK,
  CRT_RUN_CYC_EXCTN_SK,
  LAST_UPDT_RUN_CYC_EXCTN_SK,
  CAP_POOL_NM
FROM {IDSOwner}.CAP_POOL_CD
"""
df_lnk_refCapPoolCd = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_refCapPoolCd)
    .load()
)

# db2_refExprncCatCd
extract_query_db2_refExprncCatCd = f"""
SELECT 
  PROD.PROD_SK,
  PROD.EXPRNC_CAT_SK,
  EXP.EXPRNC_CAT_CD
FROM {IDSOwner}.PROD PROD,
     {IDSOwner}.EXPRNC_CAT EXP
WHERE PROD.EXPRNC_CAT_SK = EXP.EXPRNC_CAT_SK
"""
df_lnk_refExprncCatCd = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_refExprncCatCd)
    .load()
)

# db2_refNtwkId
extract_query_db2_refNtwkId = f"""
SELECT 
  NTWK.NTWK_SK,
  NTWK.NTWK_ID,
  NTWK.NTWK_SH_NM
FROM {IDSOwner}.NTWK NTWK
"""
df_lnk_refNtwkId = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_refNtwkId)
    .load()
)

# db2_refClsMcaidEligTtl
extract_query_db2_refClsMcaidEligTtl = f"""
SELECT 
  CLS_SK,
  MCAID_ELIG_TTL
FROM {IDSOwner}.CLS
"""
df_lnk_refClsMcaidEligTtl = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_refClsMcaidEligTtl)
    .load()
)

# db2_refClsId
extract_query_db2_refClsId = f"""
SELECT 
  CLS_SK,
  CLS_ID,
  CLS_DESC
FROM {IDSOwner}.CLS
"""
df_lnk_CLASS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_refClsId)
    .load()
)

# db2_Prodshnm
extract_query_db2_Prodshnm = f"""
SELECT 
  PROD.PROD_SK,
  PSM.PROD_SH_NM
FROM {IDSOwner}.PROD PROD,
     {IDSOwner}.PROD_SH_NM PSM
WHERE PROD.PROD_SH_NM_SK = PSM.PROD_SH_NM_SK
"""
df_lnk_Prodshnm = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_Prodshnm)
    .load()
)

# db2_refProv => cpy_Prov => 2 outputs
extract_query_db2_refProv = f"""
SELECT 
  PROV.PROV_SK,
  PROV.PROV_ID,
  PROV.PROV_NM
FROM {IDSOwner}.PROV PROV
"""
df_db2_refProv = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_refProv)
    .load()
)

df_cpy_Prov = df_db2_refProv
df_lnk_Pcp_Prov_Lookup = df_cpy_Prov.select(
    F.col("PROV_SK").alias("PROV_SK"),
    F.col("PROV_ID").alias("PROV_ID"),
    F.col("PROV_NM").alias("PROV_NM")
)
df_lnk_Paid_Prov_Lookup = df_cpy_Prov.select(
    F.col("PROV_SK").alias("PROV_SK"),
    F.col("PROV_ID").alias("PROV_ID"),
    F.col("PROV_NM").alias("PROV_NM")
)

# db2_refclndrdt => cpy_Yr_mo => 2 outputs
extract_query_db2_refclndrdt = f"""
SELECT 
  CLNDR_DT_SK,
  YR_MO_SK
FROM {IDSOwner}.CLNDR_DT
"""
df_db2_refclndrdt = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_refclndrdt)
    .load()
)
df_cpy_Yr_mo = df_db2_refclndrdt
df_lnk_Paid_Yr_Mo_Lookup = df_cpy_Yr_mo.select(
    F.col("CLNDR_DT_SK").alias("CLNDR_DT_SK"),
    F.col("YR_MO_SK").alias("YR_MO_SK")
)
df_lnk_Ern_Yr_Mo_Lookup = df_cpy_Yr_mo.select(
    F.col("CLNDR_DT_SK").alias("CLNDR_DT_SK"),
    F.col("YR_MO_SK").alias("YR_MO_SK")
)

# db2_refMbrDob_gnder => cpy_dob_gnder => 2 outputs
extract_query_db2_refMbrDob_gnder = f"""
SELECT
  MBR_SK,
  BRTH_DT_SK,
  MBR_GNDR_CD_SK
FROM {IDSOwner}.MBR
"""
df_db2_refMbrDob_gnder = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_refMbrDob_gnder)
    .load()
)
df_cpy_dob_gnder = df_db2_refMbrDob_gnder
df_lnk_refMbrDob = df_cpy_dob_gnder.select(
    F.col("MBR_SK").alias("MBR_SK"),
    F.col("BRTH_DT_SK").alias("BRTH_DT_SK")
)
df_lnk_gndr = df_cpy_dob_gnder.select(
    F.col("MBR_SK").alias("MBR_SK"),
    F.col("MBR_GNDR_CD_SK").alias("MBR_GNDR_CD_SK")
)

# BCBSA_db2_CD_MPPNG_in
extract_query_BCBSA_db2_CD_MPPNG_in = f"""
SELECT 
  CD_MPPNG_SK,
  'BCBSA' AS TRGT_CD
FROM {IDSOwner}.CD_MPPNG CD
WHERE 
  CD.SRC_CD = '9999'
  AND CD.SRC_CLCTN_CD = 'FACETS DBO'
  AND CD.SRC_DOMAIN_NM = 'CAPITATION FUND ACCOUNTING CATEGORY'
  AND CD.TRGT_CLCTN_CD = 'IDS'
  AND CD.TRGT_DOMAIN_NM = 'CAPITATION FUND ACCOUNTING CATEGORY'
"""
df_lnk_CdMppng_out = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_BCBSA_db2_CD_MPPNG_in)
    .load()
)

# db2_CAP_in
extract_query_db2_CAP_in = f"""
SELECT
  CAP.CAP_SK,
  CAP.SRC_SYS_CD_SK,
  CAP.MBR_UNIQ_KEY,
  CAP.CAP_PROV_ID,
  CAP.ERN_DT_SK,
  CAP.PD_DT_SK,
  CAP.CAP_FUND_ID,
  CAP.CAP_POOL_CD_SK,
  CAP.SEQ_NO,
  CAP.CRT_RUN_CYC_EXCTN_SK,
  CAP.LAST_UPDT_RUN_CYC_EXCTN_SK,
  CAP.CAP_FUND_SK,
  CAP.CAP_PROV_SK,
  CAP.CLS_SK,
  CAP.CLS_PLN_SK,
  CAP.FNCL_LOB_SK,
  CAP.GRP_SK,
  CAP.MBR_SK,
  CAP.NTWK_SK,
  CAP.PD_PROV_SK,
  CAP.PCP_PROV_SK,
  CAP.PROD_SK,
  CAP.SUBGRP_SK,
  CAP.SUB_SK,
  CAP.CAP_ADJ_RSN_CD_SK,
  CAP.CAP_ADJ_STTUS_CD_SK,
  CAP.CAP_ADJ_TYP_CD_SK,
  CAP.CAP_CAT_CD_SK,
  CAP.CAP_COPAY_TYP_CD_SK,
  CAP.CAP_LOB_CD_SK,
  CAP.CAP_PERD_CD_SK,
  CAP.CAP_SCHD_CD_SK,
  CAP.CAP_TYP_CD_SK,
  CAP.ADJ_AMT,
  CAP.CAP_AMT,
  CAP.COPAY_AMT,
  CAP.FUND_RATE_AMT,
  CAP.MBR_AGE,
  CAP.MBR_MO_CT,
  CAP.LFSTYL_RATE_FCTR_SK,
  MBR.BRTH_DT_SK,
  MBR.FIRST_NM,
  MBR.MIDINIT,
  MBR.LAST_NM,
  SUB.SUB_ID,
  PROD.PROD_ID,
  PROD.PROD_SH_NM_SK,
  CAP_FUND.CAP_FUND_ACCTG_CAT_CD_SK,
  CAP.GL_CAT_CD_SK,
  CD.TRGT_CD,
  CD1.TRGT_CD AS TRGT_CD1,
  CD2.TRGT_CD AS GL_CAT_CD
FROM {IDSOwner}.CAP CAP
INNER JOIN {IDSOwner}.MBR MBR ON CAP.MBR_SK = MBR.MBR_SK
INNER JOIN {IDSOwner}.SUB SUB ON MBR.SUB_SK = SUB.SUB_SK
INNER JOIN {IDSOwner}.PROD PROD ON CAP.PROD_SK = PROD.PROD_SK
INNER JOIN {IDSOwner}.CD_MPPNG CD ON CAP.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
INNER JOIN {IDSOwner}.CAP_FUND CAP_FUND ON CAP.CAP_FUND_SK = CAP_FUND.CAP_FUND_SK
INNER JOIN {IDSOwner}.CD_MPPNG CD1 ON CAP_FUND.CAP_FUND_ACCTG_CAT_CD_SK=CD1.CD_MPPNG_SK
INNER JOIN {IDSOwner}.CD_MPPNG CD2 ON CAP.GL_CAT_CD_SK = CD2.CD_MPPNG_SK
WHERE 
(
   (CD.TRGT_CD = 'FACETS' AND CAP.LAST_UPDT_RUN_CYC_EXCTN_SK >= {IDSRunCycle})
   OR (CD.TRGT_CD = 'BCBSKC' AND CAP.LAST_UPDT_RUN_CYC_EXCTN_SK >= {IDSBCBSKCRunCycle})
   OR (CD.TRGT_CD = 'BCBSA' AND CAP.LAST_UPDT_RUN_CYC_EXCTN_SK >= {IDSBCBSARunCycle})
   OR (CD.TRGT_CD = 'LUMERIS' AND CAP.LAST_UPDT_RUN_CYC_EXCTN_SK >= {IDSLumerisRunCycle})
)
"""
df_lnk_IdsEdwCapFExtr_InABC = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_CAP_in)
    .load()
)

# PSYBMED_db2_CD_MPPNG_in
extract_query_PSYBMED_db2_CD_MPPNG_in = f"""
SELECT
  CD_MPPNG_SK AS CD_MPPNG_MED,
  'PSYB' AS TRGT_CD
FROM {IDSOwner}.CD_MPPNG CD
WHERE 
   SRC_CD = 'PSYB'
   AND SRC_DOMAIN_NM = 'CUSTOM DOMAIN MAPPING'
   AND TRGT_DOMAIN_NM = 'CAPITATION FUND ACCOUNTING CATEGORY'
   AND TRGT_CLCTN_CD = 'IDS'
   AND TRGT_CD='MNTLHLTHMED'
"""
df_lnk_CdMppng_out_PsybMed = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_PSYBMED_db2_CD_MPPNG_in)
    .load()
)

# PSYBADM_db2_CD_MPPNG_in
extract_query_PSYBADM_db2_CD_MPPNG_in = f"""
SELECT 
  CD_MPPNG_SK AS CD_MPPNG_ADM,
  'PSYB' AS TRGT_CD
FROM {IDSOwner}.CD_MPPNG CD
WHERE
   SRC_CD = 'PSYB'
   AND SRC_DOMAIN_NM = 'CUSTOM DOMAIN MAPPING'
   AND TRGT_DOMAIN_NM = 'CAPITATION FUND ACCOUNTING CATEGORY'
   AND TRGT_CLCTN_CD = 'IDS'
   AND TRGT_CD='MNTLHLTHADM'
"""
df_lnk_CdMppng_out_PsybAdm = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_PSYBADM_db2_CD_MPPNG_in)
    .load()
)

# Lkp_Cap_Fund_Cat (PxLookup) => primary link: df_lnk_IdsEdwCapFExtr_InABC, left joins with df_lnk_CdMppng_out, df_lnk_CdMppng_out_PsybMed, df_lnk_CdMppng_out_PsybAdm
df_Lkp_Cap_Fund_Cat = (
    df_lnk_IdsEdwCapFExtr_InABC.alias("lnk_IdsEdwCapFExtr_InABC")
    .join(
        df_lnk_CdMppng_out.alias("lnk_CdMppng_out"),
        (
            (F.col("lnk_IdsEdwCapFExtr_InABC.SRC_SYS_CD_SK") == F.col("lnk_CdMppng_out.CD_MPPNG_SK"))
            & (F.col("lnk_IdsEdwCapFExtr_InABC.TRGT_CD") == F.col("lnk_CdMppng_out.TRGT_CD"))
        ),
        "left"
    )
    .join(
        df_lnk_CdMppng_out_PsybMed.alias("lnk_CdMppng_out_PsybMed"),
        (
            F.col("lnk_IdsEdwCapFExtr_InABC.TRGT_CD1") == F.col("lnk_CdMppng_out_PsybMed.TRGT_CD")
        ),
        "left"
    )
    .join(
        df_lnk_CdMppng_out_PsybAdm.alias("lnk_CdMppng_out_PsybAdm"),
        (
            F.col("lnk_IdsEdwCapFExtr_InABC.TRGT_CD1") == F.col("lnk_CdMppng_out_PsybAdm.TRGT_CD")
        ),
        "left"
    )
    .select(
        F.col("lnk_IdsEdwCapFExtr_InABC.CAP_SK").alias("CAP_SK"),
        F.col("lnk_IdsEdwCapFExtr_InABC.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("lnk_IdsEdwCapFExtr_InABC.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.col("lnk_IdsEdwCapFExtr_InABC.CAP_PROV_ID").alias("CAP_PROV_ID"),
        F.col("lnk_IdsEdwCapFExtr_InABC.ERN_DT_SK").alias("ERN_DT_SK"),
        F.col("lnk_IdsEdwCapFExtr_InABC.PD_DT_SK").alias("PD_DT_SK"),
        F.col("lnk_IdsEdwCapFExtr_InABC.CAP_FUND_ID").alias("CAP_FUND_ID"),
        F.col("lnk_IdsEdwCapFExtr_InABC.CAP_POOL_CD_SK").alias("CAP_POOL_CD_SK"),
        F.col("lnk_IdsEdwCapFExtr_InABC.SEQ_NO").alias("SEQ_NO"),
        F.col("lnk_IdsEdwCapFExtr_InABC.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("lnk_IdsEdwCapFExtr_InABC.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("lnk_IdsEdwCapFExtr_InABC.CAP_FUND_SK").alias("CAP_FUND_SK"),
        F.col("lnk_IdsEdwCapFExtr_InABC.CAP_PROV_SK").alias("CAP_PROV_SK"),
        F.col("lnk_IdsEdwCapFExtr_InABC.CLS_SK").alias("CLS_SK"),
        F.col("lnk_IdsEdwCapFExtr_InABC.CLS_PLN_SK").alias("CLS_PLN_SK"),
        F.col("lnk_IdsEdwCapFExtr_InABC.FNCL_LOB_SK").alias("FNCL_LOB_SK"),
        F.col("lnk_IdsEdwCapFExtr_InABC.GRP_SK").alias("GRP_SK"),
        F.col("lnk_IdsEdwCapFExtr_InABC.MBR_SK").alias("MBR_SK"),
        F.col("lnk_IdsEdwCapFExtr_InABC.NTWK_SK").alias("NTWK_SK"),
        F.col("lnk_IdsEdwCapFExtr_InABC.PD_PROV_SK").alias("PD_PROV_SK"),
        F.col("lnk_IdsEdwCapFExtr_InABC.PCP_PROV_SK").alias("PCP_PROV_SK"),
        F.col("lnk_IdsEdwCapFExtr_InABC.PROD_SK").alias("PROD_SK"),
        F.col("lnk_IdsEdwCapFExtr_InABC.SUBGRP_SK").alias("SUBGRP_SK"),
        F.col("lnk_IdsEdwCapFExtr_InABC.SUB_SK").alias("SUB_SK"),
        F.col("lnk_IdsEdwCapFExtr_InABC.CAP_ADJ_RSN_CD_SK").alias("CAP_ADJ_RSN_CD_SK"),
        F.col("lnk_IdsEdwCapFExtr_InABC.CAP_ADJ_STTUS_CD_SK").alias("CAP_ADJ_STTUS_CD_SK"),
        F.col("lnk_IdsEdwCapFExtr_InABC.CAP_ADJ_TYP_CD_SK").alias("CAP_ADJ_TYP_CD_SK"),
        F.col("lnk_IdsEdwCapFExtr_InABC.CAP_CAT_CD_SK").alias("CAP_CAT_CD_SK"),
        F.col("lnk_IdsEdwCapFExtr_InABC.CAP_COPAY_TYP_CD_SK").alias("CAP_COPAY_TYP_CD_SK"),
        F.col("lnk_IdsEdwCapFExtr_InABC.CAP_LOB_CD_SK").alias("CAP_LOB_CD_SK"),
        F.col("lnk_IdsEdwCapFExtr_InABC.CAP_PERD_CD_SK").alias("CAP_PERD_CD_SK"),
        F.col("lnk_IdsEdwCapFExtr_InABC.CAP_SCHD_CD_SK").alias("CAP_SCHD_CD_SK"),
        F.col("lnk_IdsEdwCapFExtr_InABC.CAP_TYP_CD_SK").alias("CAP_TYP_CD_SK"),
        F.col("lnk_IdsEdwCapFExtr_InABC.ADJ_AMT").alias("ADJ_AMT"),
        F.col("lnk_IdsEdwCapFExtr_InABC.CAP_AMT").alias("CAP_AMT"),
        F.col("lnk_IdsEdwCapFExtr_InABC.COPAY_AMT").alias("COPAY_AMT"),
        F.col("lnk_IdsEdwCapFExtr_InABC.FUND_RATE_AMT").alias("FUND_RATE_AMT"),
        F.col("lnk_IdsEdwCapFExtr_InABC.MBR_AGE").alias("MBR_AGE"),
        F.col("lnk_IdsEdwCapFExtr_InABC.MBR_MO_CT").alias("MBR_MO_CT"),
        F.col("lnk_IdsEdwCapFExtr_InABC.BRTH_DT_SK").alias("BRTH_DT_SK"),
        F.col("lnk_IdsEdwCapFExtr_InABC.FIRST_NM").alias("FIRST_NM"),
        F.col("lnk_IdsEdwCapFExtr_InABC.MIDINIT").alias("MIDINIT"),
        F.col("lnk_IdsEdwCapFExtr_InABC.LAST_NM").alias("LAST_NM"),
        F.col("lnk_IdsEdwCapFExtr_InABC.SUB_ID").alias("SUB_ID"),
        F.col("lnk_IdsEdwCapFExtr_InABC.PROD_ID").alias("PROD_ID"),
        F.col("lnk_IdsEdwCapFExtr_InABC.PROD_SH_NM_SK").alias("PROD_SH_NM_SK"),
        F.col("lnk_IdsEdwCapFExtr_InABC.CAP_FUND_ACCTG_CAT_CD_SK").alias("CAP_FUND_ACCTG_CAT_CD_SK"),
        F.col("lnk_IdsEdwCapFExtr_InABC.GL_CAT_CD_SK").alias("GL_CAT_CD_SK"),
        F.col("lnk_CdMppng_out.CD_MPPNG_SK").alias("CD_MPPNG_SK"),
        F.col("lnk_IdsEdwCapFExtr_InABC.TRGT_CD").alias("TRGT_CD"),
        F.col("lnk_IdsEdwCapFExtr_InABC.LFSTYL_RATE_FCTR_SK").alias("LFSTYL_RATE_FCTR_SK"),
        F.col("lnk_IdsEdwCapFExtr_InABC.GL_CAT_CD").alias("GL_CAT_CD"),
        F.col("lnk_CdMppng_out_PsybMed.CD_MPPNG_MED").alias("CD_MPPNG_MED"),
        F.col("lnk_CdMppng_out_PsybAdm.CD_MPPNG_ADM").alias("CD_MPPNG_ADM"),
        F.col("lnk_IdsEdwCapFExtr_InABC.TRGT_CD1").alias("TRGT_CD1")
    )
)

# Xfm_Cap_Fund_Cat (CTransformerStage) - emulate stage variables
df_Xfm_Cap_Fund_Cat_sv = df_Lkp_Cap_Fund_Cat \
    .withColumn(
        "SvNullCheck",
        F.when(F.col("CAP_FUND_ACCTG_CAT_CD_SK").isNull(), F.lit(0)).otherwise(F.col("CAP_FUND_ACCTG_CAT_CD_SK"))
    )

df_Xfm_Cap_Fund_Cat = df_Xfm_Cap_Fund_Cat_sv \
    .withColumn(
        "SvCapFundActgCatCdSk",
        F.when(F.col("TRGT_CD") == F.lit("BCBSA"), F.col("CD_MPPNG_SK"))
         .when((F.col("TRGT_CD1") == F.lit("PSYB")) & (F.col("GL_CAT_CD") == F.lit("ADM")), F.col("CD_MPPNG_ADM"))
         .when((F.col("TRGT_CD1") == F.lit("PSYB")) & (F.col("GL_CAT_CD") == F.lit("MED")), F.col("CD_MPPNG_MED"))
         .otherwise(F.col("SvNullCheck"))
    )

df_Xfm_Cap_Fund_Cat_out = df_Xfm_Cap_Fund_Cat.select(
    F.col("CAP_SK").alias("CAP_SK"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("CAP_PROV_ID").alias("CAP_PROV_ID"),
    F.col("ERN_DT_SK").alias("ERN_DT_SK"),
    F.col("PD_DT_SK").alias("PD_DT_SK"),
    F.col("CAP_FUND_ID").alias("CAP_FUND_ID"),
    F.col("CAP_POOL_CD_SK").alias("CAP_POOL_CD_SK"),
    F.col("SEQ_NO").alias("SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CAP_FUND_SK").alias("CAP_FUND_SK"),
    F.col("CAP_PROV_SK").alias("CAP_PROV_SK"),
    F.col("CLS_SK").alias("CLS_SK"),
    F.col("CLS_PLN_SK").alias("CLS_PLN_SK"),
    F.col("FNCL_LOB_SK").alias("FNCL_LOB_SK"),
    F.col("GRP_SK").alias("GRP_SK"),
    F.col("MBR_SK").alias("MBR_SK"),
    F.col("NTWK_SK").alias("NTWK_SK"),
    F.col("PD_PROV_SK").alias("PD_PROV_SK"),
    F.col("PCP_PROV_SK").alias("PCP_PROV_SK"),
    F.col("PROD_SK").alias("PROD_SK"),
    F.col("SUBGRP_SK").alias("SUBGRP_SK"),
    F.col("SUB_SK").alias("SUB_SK"),
    F.col("CAP_ADJ_RSN_CD_SK").alias("CAP_ADJ_RSN_CD_SK"),
    F.col("CAP_ADJ_STTUS_CD_SK").alias("CAP_ADJ_STTUS_CD_SK"),
    F.col("CAP_ADJ_TYP_CD_SK").alias("CAP_ADJ_TYP_CD_SK"),
    F.col("CAP_CAT_CD_SK").alias("CAP_CAT_CD_SK"),
    F.col("CAP_COPAY_TYP_CD_SK").alias("CAP_COPAY_TYP_CD_SK"),
    F.col("CAP_LOB_CD_SK").alias("CAP_LOB_CD_SK"),
    F.col("CAP_PERD_CD_SK").alias("CAP_PERD_CD_SK"),
    F.col("CAP_SCHD_CD_SK").alias("CAP_SCHD_CD_SK"),
    F.col("CAP_TYP_CD_SK").alias("CAP_TYP_CD_SK"),
    F.col("ADJ_AMT").alias("ADJ_AMT"),
    F.col("CAP_AMT").alias("CAP_AMT"),
    F.col("COPAY_AMT").alias("COPAY_AMT"),
    F.col("FUND_RATE_AMT").alias("FUND_RATE_AMT"),
    F.col("MBR_AGE").alias("MBR_AGE"),
    F.col("MBR_MO_CT").alias("MBR_MO_CT"),
    F.col("BRTH_DT_SK").alias("BRTH_DT_SK"),
    F.col("FIRST_NM").alias("FIRST_NM"),
    F.col("MIDINIT").alias("MIDINIT"),
    F.col("LAST_NM").alias("LAST_NM"),
    F.col("SUB_ID").alias("SUB_ID"),
    F.col("PROD_ID").alias("PROD_ID"),
    F.col("PROD_SH_NM_SK").alias("PROD_SH_NM_SK"),
    F.col("SvCapFundActgCatCdSk").alias("CAP_FUND_ACCTG_CAT_CD_SK"),
    F.col("GL_CAT_CD_SK").alias("GL_CAT_CD_SK"),
    F.col("LFSTYL_RATE_FCTR_SK").alias("LFSTYL_RATE_FCTR_SK")
)

# lkp_FKeys (PxLookup) => the primary link is df_Xfm_Cap_Fund_Cat_out (alias lnk_IdsEdwCapFExtr_InGHI)
temp_lkpFKeys = df_Xfm_Cap_Fund_Cat_out.alias("lnk_IdsEdwCapFExtr_InGHI")

df_lkpFKeys = temp_lkpFKeys \
    .join(
        df_lnk_refClsMcaidEligTtl.alias("lnk_refClsMcaidEligTtl"),
        F.col("lnk_IdsEdwCapFExtr_InGHI.CLS_SK") == F.col("lnk_refClsMcaidEligTtl.CLS_SK"),
        "left"
    ) \
    .join(
        df_lnk_refNtwkId.alias("lnk_refNtwkId"),
        F.col("lnk_IdsEdwCapFExtr_InGHI.NTWK_SK") == F.col("lnk_refNtwkId.NTWK_SK"),
        "left"
    ) \
    .join(
        df_lnk_refExprncCatCd.alias("lnk_refExprncCatCd"),
        F.col("lnk_IdsEdwCapFExtr_InGHI.PROD_SK") == F.col("lnk_refExprncCatCd.PROD_SK"),
        "left"
    ) \
    .join(
        df_lnk_refCapPoolCd.alias("lnk_refCapPoolCd"),
        F.col("lnk_IdsEdwCapFExtr_InGHI.CAP_POOL_CD_SK") == F.col("lnk_refCapPoolCd.CAP_POOL_CD_SK"),
        "left"
    ) \
    .join(
        df_lnk_refCapSchdCd.alias("lnk_refCapSchdCd"),
        F.col("lnk_IdsEdwCapFExtr_InGHI.CAP_SCHD_CD_SK") == F.col("lnk_refCapSchdCd.CAP_SCHD_CD_SK"),
        "left"
    ) \
    .join(
        df_lnk_refFnclLobCd.alias("lnk_refFnclLobCd"),
        F.col("lnk_IdsEdwCapFExtr_InGHI.FNCL_LOB_SK") == F.col("lnk_refFnclLobCd.FNCL_LOB_SK"),
        "left"
    ) \
    .join(
        df_lnk_refGrpNm.alias("lnk_refGrpNm"),
        F.col("lnk_IdsEdwCapFExtr_InGHI.GRP_SK") == F.col("lnk_refGrpNm.GRP_SK"),
        "left"
    ) \
    .join(
        df_lnk_CLASS.alias("lnk_CLASS"),
        F.col("lnk_IdsEdwCapFExtr_InGHI.CLS_SK") == F.col("lnk_CLASS.CLS_SK"),
        "left"
    ) \
    .join(
        df_lnk_refMbrDob.alias("lnk_refMbrDob"),
        F.col("lnk_IdsEdwCapFExtr_InGHI.MBR_SK") == F.col("lnk_refMbrDob.MBR_SK"),
        "left"
    ) \
    .join(
        df_lnk_Paid_Yr_Mo_Lookup.alias("lnk_Paid_Yr_Mo_Lookup"),
        F.col("lnk_IdsEdwCapFExtr_InGHI.PD_DT_SK") == F.col("lnk_Paid_Yr_Mo_Lookup.CLNDR_DT_SK"),
        "left"
    ) \
    .join(
        df_lnk_Ern_Yr_Mo_Lookup.alias("lnk_Ern_Yr_Mo_Lookup"),
        F.col("lnk_IdsEdwCapFExtr_InGHI.ERN_DT_SK") == F.col("lnk_Ern_Yr_Mo_Lookup.CLNDR_DT_SK"),
        "left"
    ) \
    .join(
        df_lnk_Pcp_Prov_Lookup.alias("lnk_Pcp_Prov_Lookup"),
        F.col("lnk_IdsEdwCapFExtr_InGHI.PCP_PROV_SK") == F.col("lnk_Pcp_Prov_Lookup.PROV_SK"),
        "left"
    ) \
    .join(
        df_lnk_Paid_Prov_Lookup.alias("lnk_Paid_Prov_Lookup"),
        F.col("lnk_IdsEdwCapFExtr_InGHI.PD_PROV_SK") == F.col("lnk_Paid_Prov_Lookup.PROV_SK"),
        "left"
    ) \
    .join(
        df_lnk_gndr.alias("lnk_gndr"),
        F.col("lnk_IdsEdwCapFExtr_InGHI.MBR_SK") == F.col("lnk_gndr.MBR_SK"),
        "left"
    ) \
    .join(
        df_lnk_Prodshnm.alias("lnk_Prodshnm"),
        F.col("lnk_IdsEdwCapFExtr_InGHI.PROD_SK") == F.col("lnk_Prodshnm.PROD_SK"),
        "left"
    ) \
    .select(
        F.col("lnk_refClsMcaidEligTtl.MCAID_ELIG_TTL").alias("MCAID_ELIG_TTL"),
        F.col("lnk_Pcp_Prov_Lookup.PROV_ID").alias("PCP_PROV_ID"),
        F.col("lnk_Paid_Prov_Lookup.PROV_ID").alias("PD_PROV_ID"),
        F.col("lnk_refNtwkId.NTWK_ID").alias("NTWK_ID"),
        F.col("lnk_refNtwkId.NTWK_SH_NM").alias("NTWK_SH_NM"),
        F.col("lnk_refExprncCatCd.EXPRNC_CAT_CD").alias("EXPRNC_CAT_CD"),
        F.col("lnk_Prodshnm.PROD_SH_NM").alias("PROD_SH_NM"),
        F.col("lnk_refCapPoolCd.CAP_POOL_CD").alias("CAP_POOL_CD"),
        F.col("lnk_refCapSchdCd.CAP_SCHD_CD").alias("CAP_SCHD_CD"),
        F.col("lnk_refFnclLobCd.FNCL_LOB_CD").alias("FNCL_LOB_CD"),
        F.col("lnk_refGrpNm.GRP_ID").alias("GRP_ID"),
        F.col("lnk_IdsEdwCapFExtr_InGHI.CAP_SK").alias("CAP_SK"),
        F.col("lnk_IdsEdwCapFExtr_InGHI.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("lnk_IdsEdwCapFExtr_InGHI.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.col("lnk_IdsEdwCapFExtr_InGHI.CAP_PROV_ID").alias("CAP_PROV_ID"),
        F.col("lnk_IdsEdwCapFExtr_InGHI.ERN_DT_SK").alias("ERN_DT_SK"),
        F.col("lnk_IdsEdwCapFExtr_InGHI.PD_DT_SK").alias("PD_DT_SK"),
        F.col("lnk_IdsEdwCapFExtr_InGHI.CAP_FUND_ID").alias("CAP_FUND_ID"),
        F.col("lnk_IdsEdwCapFExtr_InGHI.CAP_POOL_CD_SK").alias("CAP_POOL_CD_SK"),
        F.col("lnk_IdsEdwCapFExtr_InGHI.SEQ_NO").alias("SEQ_NO"),
        F.col("lnk_IdsEdwCapFExtr_InGHI.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("lnk_IdsEdwCapFExtr_InGHI.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("lnk_IdsEdwCapFExtr_InGHI.CAP_FUND_SK").alias("CAP_FUND_SK"),
        F.col("lnk_IdsEdwCapFExtr_InGHI.CAP_PROV_SK").alias("CAP_PROV_SK"),
        F.col("lnk_IdsEdwCapFExtr_InGHI.CLS_SK").alias("CLS_SK"),
        F.col("lnk_IdsEdwCapFExtr_InGHI.CLS_PLN_SK").alias("CLS_PLN_SK"),
        F.col("lnk_IdsEdwCapFExtr_InGHI.FNCL_LOB_SK").alias("FNCL_LOB_SK"),
        F.col("lnk_IdsEdwCapFExtr_InGHI.GRP_SK").alias("GRP_SK"),
        F.col("lnk_IdsEdwCapFExtr_InGHI.MBR_SK").alias("MBR_SK"),
        F.col("lnk_IdsEdwCapFExtr_InGHI.NTWK_SK").alias("NTWK_SK"),
        F.col("lnk_IdsEdwCapFExtr_InGHI.PD_PROV_SK").alias("PD_PROV_SK"),
        F.col("lnk_IdsEdwCapFExtr_InGHI.PCP_PROV_SK").alias("PCP_PROV_SK"),
        F.col("lnk_IdsEdwCapFExtr_InGHI.PROD_SK").alias("PROD_SK"),
        F.col("lnk_IdsEdwCapFExtr_InGHI.SUBGRP_SK").alias("SUBGRP_SK"),
        F.col("lnk_IdsEdwCapFExtr_InGHI.SUB_SK").alias("SUB_SK"),
        F.col("lnk_IdsEdwCapFExtr_InGHI.CAP_ADJ_RSN_CD_SK").alias("CAP_ADJ_RSN_CD_SK"),
        F.col("lnk_IdsEdwCapFExtr_InGHI.CAP_ADJ_STTUS_CD_SK").alias("CAP_ADJ_STTUS_CD_SK"),
        F.col("lnk_IdsEdwCapFExtr_InGHI.CAP_ADJ_TYP_CD_SK").alias("CAP_ADJ_TYP_CD_SK"),
        F.col("lnk_IdsEdwCapFExtr_InGHI.CAP_CAT_CD_SK").alias("CAP_CAT_CD_SK"),
        F.col("lnk_IdsEdwCapFExtr_InGHI.CAP_COPAY_TYP_CD_SK").alias("CAP_COPAY_TYP_CD_SK"),
        F.col("lnk_IdsEdwCapFExtr_InGHI.CAP_LOB_CD_SK").alias("CAP_LOB_CD_SK"),
        F.col("lnk_IdsEdwCapFExtr_InGHI.CAP_PERD_CD_SK").alias("CAP_PERD_CD_SK"),
        F.col("lnk_IdsEdwCapFExtr_InGHI.CAP_SCHD_CD_SK").alias("CAP_SCHD_CD_SK"),
        F.col("lnk_IdsEdwCapFExtr_InGHI.CAP_TYP_CD_SK").alias("CAP_TYP_CD_SK"),
        F.col("lnk_IdsEdwCapFExtr_InGHI.ADJ_AMT").alias("ADJ_AMT"),
        F.col("lnk_IdsEdwCapFExtr_InGHI.CAP_AMT").alias("CAP_AMT"),
        F.col("lnk_IdsEdwCapFExtr_InGHI.COPAY_AMT").alias("COPAY_AMT"),
        F.col("lnk_IdsEdwCapFExtr_InGHI.FUND_RATE_AMT").alias("FUND_RATE_AMT"),
        F.col("lnk_IdsEdwCapFExtr_InGHI.MBR_AGE").alias("MBR_AGE"),
        F.col("lnk_IdsEdwCapFExtr_InGHI.MBR_MO_CT").alias("MBR_MO_CT"),
        F.col("lnk_gndr.MBR_GNDR_CD_SK").alias("MBR_GNDR_CD_SK"),
        F.col("lnk_IdsEdwCapFExtr_InGHI.BRTH_DT_SK").alias("BRTH_DT_SK"),
        F.col("lnk_IdsEdwCapFExtr_InGHI.FIRST_NM").alias("FIRST_NM"),
        F.col("lnk_IdsEdwCapFExtr_InGHI.MIDINIT").alias("MIDINIT"),
        F.col("lnk_IdsEdwCapFExtr_InGHI.LAST_NM").alias("LAST_NM"),
        F.col("lnk_IdsEdwCapFExtr_InGHI.SUB_ID").alias("SUB_ID"),
        F.col("lnk_IdsEdwCapFExtr_InGHI.PROD_ID").alias("PROD_ID"),
        F.col("lnk_IdsEdwCapFExtr_InGHI.PROD_SH_NM_SK").alias("PROD_SH_NM_SK"),
        F.col("lnk_IdsEdwCapFExtr_InGHI.CAP_FUND_ACCTG_CAT_CD_SK").alias("CAP_FUND_ACCTG_CAT_CD_SK"),
        F.col("lnk_IdsEdwCapFExtr_InGHI.GL_CAT_CD_SK").alias("GL_CAT_CD_SK"),
        F.col("lnk_CLASS.CLS_ID").alias("CLS_ID"),
        F.col("lnk_Ern_Yr_Mo_Lookup.YR_MO_SK").alias("ERM_YR_MO_SK"),
        F.col("lnk_Paid_Yr_Mo_Lookup.YR_MO_SK").alias("PD_YR_MO_SK"),
        F.col("lnk_refMbrDob.BRTH_DT_SK").alias("MBR_BRTH_DT_SK"),
        F.col("lnk_IdsEdwCapFExtr_InGHI.LFSTYL_RATE_FCTR_SK").alias("LFSTYL_RATE_FCTR_SK")
    )

# lkp_Codes (PxLookup) - primary link: df_lkpFKeys (ink_Fkeys_Lkup)
df_lkpCodes = df_lkpFKeys.alias("ink_Fkeys_Lkup") \
    .join(
        df_lnk_refAdjTypCd.alias("lnk_refAdjTypCd"),
        F.col("ink_Fkeys_Lkup.CAP_ADJ_TYP_CD_SK") == F.col("lnk_refAdjTypCd.CD_MPPNG_SK"),
        "left"
    ) \
    .join(
        df_lnk_refFundAcctCatCd.alias("lnk_refFundAcctCatCd"),
        F.col("ink_Fkeys_Lkup.CAP_FUND_ACCTG_CAT_CD_SK") == F.col("lnk_refFundAcctCatCd.CD_MPPNG_SK"),
        "left"
    ) \
    .join(
        df_lnk_refCapCatCd.alias("lnk_refCapCatCd"),
        F.col("ink_Fkeys_Lkup.CAP_CAT_CD_SK") == F.col("lnk_refCapCatCd.CD_MPPNG_SK"),
        "left"
    ) \
    .join(
        df_lnk_refCapAdjRsnCd.alias("lnk_refCapAdjRsnCd"),
        F.col("ink_Fkeys_Lkup.CAP_ADJ_RSN_CD_SK") == F.col("lnk_refCapAdjRsnCd.CD_MPPNG_SK"),
        "left"
    ) \
    .join(
        df_lnk_refAdjSttusCd.alias("lnk_refAdjSttusCd"),
        F.col("ink_Fkeys_Lkup.CAP_ADJ_STTUS_CD_SK") == F.col("lnk_refAdjSttusCd.CD_MPPNG_SK"),
        "left"
    ) \
    .join(
        df_lnk_refMbrGndrCd.alias("lnk_refMbrGndrCd"),
        F.col("ink_Fkeys_Lkup.MBR_GNDR_CD_SK") == F.col("lnk_refMbrGndrCd.CD_MPPNG_SK"),
        "left"
    ) \
    .join(
        df_lnk_refPerdCd.alias("lnk_refPerdCd"),
        F.col("ink_Fkeys_Lkup.CAP_PERD_CD_SK") == F.col("lnk_refPerdCd.CD_MPPNG_SK"),
        "left"
    ) \
    .join(
        df_lnk_refCapTypCd.alias("lnk_refCapTypCd"),
        F.col("ink_Fkeys_Lkup.CAP_TYP_CD_SK") == F.col("lnk_refCapTypCd.CD_MPPNG_SK"),
        "left"
    ) \
    .join(
        df_lnk_refLobCd.alias("lnk_refLobCd"),
        F.col("ink_Fkeys_Lkup.CAP_LOB_CD_SK") == F.col("lnk_refLobCd.CD_MPPNG_SK"),
        "left"
    ) \
    .join(
        df_lnk_refSrcSys.alias("lnk_refSrcSys"),
        F.col("ink_Fkeys_Lkup.SRC_SYS_CD_SK") == F.col("lnk_refSrcSys.CD_MPPNG_SK"),
        "left"
    ) \
    .join(
        df_lnk_refGlCatCd.alias("lnk_refGlCatCd"),
        F.col("ink_Fkeys_Lkup.GL_CAT_CD_SK") == F.col("lnk_refGlCatCd.CD_MPPNG_SK"),
        "left"
    ) \
    .join(
        df_lnk_refCopayTypCd.alias("lnk_refCopayTypCd"),
        F.col("ink_Fkeys_Lkup.CAP_COPAY_TYP_CD_SK") == F.col("lnk_refCopayTypCd.CD_MPPNG_SK"),
        "left"
    ) \
    .join(
        df_lnk_refCatCd.alias("lnk_refCatCd"),  # Just for completeness, though it's the same CAP_CAT_CD?
        F.col("ink_Fkeys_Lkup.CAP_CAT_CD_SK") == F.col("lnk_refCatCd.CD_MPPNG_SK"),
        "left"
    ) \
    .select(
        F.col("ink_Fkeys_Lkup.MCAID_ELIG_TTL").alias("MCAID_ELIG_TTL"),
        F.col("ink_Fkeys_Lkup.PCP_PROV_ID").alias("PCP_PROV_ID"),
        F.col("ink_Fkeys_Lkup.PD_PROV_ID").alias("PD_PROV_ID"),
        F.col("ink_Fkeys_Lkup.NTWK_ID").alias("NTWK_ID"),
        F.col("ink_Fkeys_Lkup.NTWK_SH_NM").alias("NTWK_SH_NM"),
        F.col("ink_Fkeys_Lkup.EXPRNC_CAT_CD").alias("EXPRNC_CAT_CD"),
        F.col("ink_Fkeys_Lkup.PROD_SH_NM").alias("PROD_SH_NM"),
        F.col("ink_Fkeys_Lkup.CAP_POOL_CD").alias("CAP_POOL_CD"),
        F.col("ink_Fkeys_Lkup.CAP_SCHD_CD").alias("CAP_SCHD_CD"),
        F.col("ink_Fkeys_Lkup.FNCL_LOB_CD").alias("FNCL_LOB_CD"),
        F.col("ink_Fkeys_Lkup.GRP_ID").alias("GRP_ID"),
        F.col("ink_Fkeys_Lkup.CAP_SK").alias("CAP_SK"),
        F.col("ink_Fkeys_Lkup.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("ink_Fkeys_Lkup.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.col("ink_Fkeys_Lkup.CAP_PROV_ID").alias("CAP_PROV_ID"),
        F.col("ink_Fkeys_Lkup.ERN_DT_SK").alias("ERN_DT_SK"),
        F.col("ink_Fkeys_Lkup.PD_DT_SK").alias("PD_DT_SK"),
        F.col("ink_Fkeys_Lkup.CAP_FUND_ID").alias("CAP_FUND_ID"),
        F.col("ink_Fkeys_Lkup.CAP_POOL_CD_SK").alias("CAP_POOL_CD_SK"),
        F.col("ink_Fkeys_Lkup.SEQ_NO").alias("SEQ_NO"),
        F.col("ink_Fkeys_Lkup.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("ink_Fkeys_Lkup.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("ink_Fkeys_Lkup.CAP_FUND_SK").alias("CAP_FUND_SK"),
        F.col("ink_Fkeys_Lkup.CAP_PROV_SK").alias("CAP_PROV_SK"),
        F.col("ink_Fkeys_Lkup.CLS_SK").alias("CLS_SK"),
        F.col("ink_Fkeys_Lkup.CLS_PLN_SK").alias("CLS_PLN_SK"),
        F.col("ink_Fkeys_Lkup.FNCL_LOB_SK").alias("FNCL_LOB_SK"),
        F.col("ink_Fkeys_Lkup.GRP_SK").alias("GRP_SK"),
        F.col("ink_Fkeys_Lkup.MBR_SK").alias("MBR_SK"),
        F.col("ink_Fkeys_Lkup.NTWK_SK").alias("NTWK_SK"),
        F.col("ink_Fkeys_Lkup.PD_PROV_SK").alias("PD_PROV_SK"),
        F.col("ink_Fkeys_Lkup.PCP_PROV_SK").alias("PCP_PROV_SK"),
        F.col("ink_Fkeys_Lkup.PROD_SK").alias("PROD_SK"),
        F.col("ink_Fkeys_Lkup.SUBGRP_SK").alias("SUBGRP_SK"),
        F.col("ink_Fkeys_Lkup.SUB_SK").alias("SUB_SK"),
        F.col("ink_Fkeys_Lkup.CAP_ADJ_RSN_CD_SK").alias("CAP_ADJ_RSN_CD_SK"),
        F.col("ink_Fkeys_Lkup.CAP_ADJ_STTUS_CD_SK").alias("CAP_ADJ_STTUS_CD_SK"),
        F.col("ink_Fkeys_Lkup.CAP_ADJ_TYP_CD_SK").alias("CAP_ADJ_TYP_CD_SK"),
        F.col("ink_Fkeys_Lkup.CAP_CAT_CD_SK").alias("CAP_CAT_CD_SK"),
        F.col("ink_Fkeys_Lkup.CAP_COPAY_TYP_CD_SK").alias("CAP_COPAY_TYP_CD_SK"),
        F.col("ink_Fkeys_Lkup.CAP_LOB_CD_SK").alias("CAP_LOB_CD_SK"),
        F.col("ink_Fkeys_Lkup.CAP_PERD_CD_SK").alias("CAP_PERD_CD_SK"),
        F.col("ink_Fkeys_Lkup.CAP_SCHD_CD_SK").alias("CAP_SCHD_CD_SK"),
        F.col("ink_Fkeys_Lkup.CAP_TYP_CD_SK").alias("CAP_TYP_CD_SK"),
        F.col("ink_Fkeys_Lkup.ADJ_AMT").alias("ADJ_AMT"),
        F.col("ink_Fkeys_Lkup.CAP_AMT").alias("CAP_AMT"),
        F.col("ink_Fkeys_Lkup.COPAY_AMT").alias("COPAY_AMT"),
        F.col("ink_Fkeys_Lkup.FUND_RATE_AMT").alias("FUND_RATE_AMT"),
        F.col("ink_Fkeys_Lkup.MBR_AGE").alias("MBR_AGE"),
        F.col("ink_Fkeys_Lkup.MBR_MO_CT").alias("MBR_MO_CT"),
        F.col("ink_Fkeys_Lkup.MBR_GNDR_CD_SK").alias("MBR_GNDR_CD_SK"),
        F.col("ink_Fkeys_Lkup.BRTH_DT_SK").alias("BRTH_DT_SK"),
        F.col("ink_Fkeys_Lkup.FIRST_NM").alias("FIRST_NM"),
        F.col("ink_Fkeys_Lkup.MIDINIT").alias("MIDINIT"),
        F.col("ink_Fkeys_Lkup.LAST_NM").alias("LAST_NM"),
        F.col("ink_Fkeys_Lkup.SUB_ID").alias("SUB_ID"),
        F.col("ink_Fkeys_Lkup.PROD_ID").alias("PROD_ID"),
        F.col("ink_Fkeys_Lkup.PROD_SH_NM_SK").alias("PROD_SH_NM_SK"),
        F.col("ink_Fkeys_Lkup.CAP_FUND_ACCTG_CAT_CD_SK").alias("CAP_FUND_ACCTG_CAT_CD_SK"),
        F.col("ink_Fkeys_Lkup.GL_CAT_CD_SK").alias("GL_CAT_CD_SK"),
        F.col("ink_Fkeys_Lkup.CLS_ID").alias("CLS_ID"),
        F.col("ink_Fkeys_Lkup.ERM_YR_MO_SK").alias("ERM_YR_MO_SK"),
        F.col("ink_Fkeys_Lkup.PD_YR_MO_SK").alias("PD_YR_MO_SK"),
        F.col("ink_Fkeys_Lkup.MBR_BRTH_DT_SK").alias("MBR_BRTH_DT_SK"),
        F.col("lnk_refAdjTypCd.TRGT_CD").alias("ADJTYPCD"),
        F.col("lnk_refCapCatCd.TRGT_CD").alias("CAPMNLADJIN"),
        F.col("lnk_refCapAdjRsnCd.TRGT_CD").alias("CAPADJCD"),
        F.col("lnk_refAdjSttusCd.TRGT_CD").alias("ADJSTTUSCD"),
        F.col("lnk_refMbrGndrCd.TRGT_CD").alias("MBRGNDRCD"),
        F.col("lnk_refPerdCd.TRGT_CD").alias("PERDCD"),
        F.col("lnk_refCapTypCd.TRGT_CD").alias("CAPTYPCD"),
        F.col("lnk_refLobCd.TRGT_CD").alias("CAPLOBCD"),
        F.col("lnk_refSrcSys.TRGT_CD").alias("SRCSYSCD"),
        F.col("lnk_refCopayTypCd.TRGT_CD").alias("COPAYTYPCD"),
        F.col("lnk_refCatCd.TRGT_CD").alias("CAPCATCD"),
        F.col("lnk_refFundAcctCatCd.TRGT_CD").alias("CAPFUNDACCCAT"),
        F.col("lnk_refFundAcctCatCd.TRGT_CD_NM").alias("CAPFUNACCTNM"),
        F.col("lnk_refGlCatCd.TRGT_CD").alias("GICATCD"),
        F.col("lnk_refGlCatCd.TRGT_CD_NM").alias("GICATNAME"),
        F.col("ink_Fkeys_Lkup.LFSTYL_RATE_FCTR_SK").alias("LFSTYL_RATE_FCTR_SK")
    )

# xfrm_BusinessLogic (CTransformerStage) => produce three outputs: lnk_Detail, lnk_Unk, lnk_Na
df_xfrm = df_lkpCodes

# Constraint logic for lnk_Detail
df_lnk_Detail = df_xfrm.filter((F.col("CAP_SK") != 0) & (F.col("CAP_SK") != 1))

# "lnk_Unk" => single row with all columns, as specified by WhereExpression
# We'll build a row dictionary:
row_unk = {
  "CAP_SK": 0,
  "SRC_SYS_CD": "UNK",
  "MBR_UNIQ_KEY": 0,
  "CAP_PROV_ID": "UNK",
  "CAP_ERN_YR_MO_SK": "0",
  "CAP_PD_YR_MO_SK": "0",
  "CAP_FUND_ID": "UNK",
  "CAP_POOL_CD": "UNK",
  "CAP_SEQ_NO": 0,
  "CRT_RUN_CYC_EXCTN_DT_SK": "1753-01-01",
  "LAST_UPDT_RUN_CYC_EXCTN_DT_SK": EDWRunCycleDate,  # Expression: "EDWRunCycleDate"
  "PROV_SK": 0,
  "CAP_FUND_SK": 0,
  "CLS_SK": 0,
  "CLS_PLN_SK": 0,
  "FNCL_LOB_SK": 0,
  "GRP_SK": 0,
  "MBR_SK": 0,
  "NTWK_SK": 0,
  "PD_PROV_SK": 0,
  "PCP_PROV_SK": 0,
  "PROD_SK": 0,
  "SUBGRP_SK": 0,
  "CAP_ADJ_RSN_CD": "UNK",
  "CAP_ADJ_STTUS_CD": "UNK",
  "CAP_ADJ_TYP_CD": "UNK",
  "CAP_CAT_CD": "UNK",
  "CAP_COPAY_TYP_CD": "UNK",
  "CAP_LOB_CD": "UNK",
  "CAP_PERD_CD": "UNK",
  "CAP_SCHD_CD": "UNK",
  "CAP_TYP_CD": "UNK",
  "CAP_MNL_ADJ_IN": "N",
  "CAP_ERN_DT_SK": "1753-01-01",
  "CAP_PD_DT_SK": "1753-01-01",
  "MBR_BRTH_DT_SK": "1753-01-01",
  "CAP_ADJ_AMT": 0,
  "CAP_CAP_AMT": 0,
  "CAP_COPAY_AMT": 0,
  "CAP_FUND_RATE_AMT": 0,
  "MBR_AGE": 0,
  "MBR_MO_CT": 0,
  "CLS_ID": "UNK",
  "CLS_MCAID_ELIG_TTL": "UNK",
  "EXPRNC_CAT_CD": "UNK",
  "FNCL_LOB_CD": "UNK",
  "GRP_ID": "UNK",
  "MBR_GNDR_CD": "UNK",
  "NTWK_ID": "UNK",
  "NTWK_SH_NM": "UNK",
  "PD_PROV_ID": "UNK",
  "PCP_PROV_ID": "UNK",
  "PROD_ID": "UNK",
  "PROD_SH_NM": "UNK",
  "CRT_RUN_CYC_EXCTN_SK": 100,
  "LAST_UPDT_RUN_CYC_EXCTN_SK": EDWRunCycle,  # Expression: "EDWRunCycle"
  "CAP_ADJ_RSN_CD_SK": 0,
  "CAP_ADJ_STTUS_CD_SK": 0,
  "CAP_ADJ_TYP_CD_SK": 0,
  "CAP_CAT_CD_SK": 0,
  "CAP_COPAY_TYP_CD_SK": 0,
  "CAP_LOB_CD_SK": 0,
  "CAP_POOL_CD_SK": 0,
  "CAP_PERD_CD_SK": 0,
  "CAP_SCHD_CD_SK": 0,
  "CAP_TYP_CD_SK": 0,
  "CAP_FUND_ACCTG_CAT_CD": "UNK",
  "CAP_FUND_ACCTG_CAT_NM": "UNK",
  "GL_CAT_CD": "UNK",
  "GL_CAT_NM": "UNK",
  "CAP_FUND_ACCTG_CAT_CD_SK": 0,
  "GL_CAT_CD_SK": 0,
  "PROD_SH_NM_SK": 0,
  "LFSTYL_RATE_FCTR_SK": 0,
  "CAP_RECOV_CAP_AMT": 0
}
df_lnk_Unk = spark.createDataFrame([Row(**row_unk)])

# "lnk_Na" => single row with "NA"
row_na = {
  "CAP_SK": 1,
  "SRC_SYS_CD": "NA",
  "MBR_UNIQ_KEY": 1,
  "CAP_PROV_ID": "NA",
  "CAP_ERN_YR_MO_SK": "1",
  "CAP_PD_YR_MO_SK": "1",
  "CAP_FUND_ID": "NA",
  "CAP_POOL_CD": "NA",
  "CAP_SEQ_NO": 1,
  "CRT_RUN_CYC_EXCTN_DT_SK": "1753-01-01",
  "LAST_UPDT_RUN_CYC_EXCTN_DT_SK": EDWRunCycleDate,  # "EDWRunCycleDate"
  "PROV_SK": 1,
  "CAP_FUND_SK": 1,
  "CLS_SK": 1,
  "CLS_PLN_SK": 1,
  "FNCL_LOB_SK": 1,
  "GRP_SK": 1,
  "MBR_SK": 1,
  "NTWK_SK": 1,
  "PD_PROV_SK": 1,
  "PCP_PROV_SK": 1,
  "PROD_SK": 1,
  "SUBGRP_SK": 1,
  "CAP_ADJ_RSN_CD": "NA",
  "CAP_ADJ_STTUS_CD": "NA",
  "CAP_ADJ_TYP_CD": "NA",
  "CAP_CAT_CD": "NA",
  "CAP_COPAY_TYP_CD": "NA",
  "CAP_LOB_CD": "NA",
  "CAP_PERD_CD": "NA",
  "CAP_SCHD_CD": "NA",
  "CAP_TYP_CD": "NA",
  "CAP_MNL_ADJ_IN": "N",
  "CAP_ERN_DT_SK": "1753-01-01",
  "CAP_PD_DT_SK": "1753-01-01",
  "MBR_BRTH_DT_SK": "1753-01-01",
  "CAP_ADJ_AMT": 0,
  "CAP_CAP_AMT": 0,
  "CAP_COPAY_AMT": 0,
  "CAP_FUND_RATE_AMT": 0,
  "MBR_AGE": 0,
  "MBR_MO_CT": 0,
  "CLS_ID": "NA",
  "CLS_MCAID_ELIG_TTL": "NA",
  "EXPRNC_CAT_CD": "NA",
  "FNCL_LOB_CD": "NA",
  "GRP_ID": "NA",
  "MBR_GNDR_CD": "NA",
  "NTWK_ID": "NA",
  "NTWK_SH_NM": "NA",
  "PD_PROV_ID": "NA",
  "PCP_PROV_ID": "NA",
  "PROD_ID": "NA",
  "PROD_SH_NM": "NA",
  "CRT_RUN_CYC_EXCTN_SK": 100,
  "LAST_UPDT_RUN_CYC_EXCTN_SK": EDWRunCycle,  # "EDWRunCycle"
  "CAP_ADJ_RSN_CD_SK": 1,
  "CAP_ADJ_STTUS_CD_SK": 1,
  "CAP_ADJ_TYP_CD_SK": 1,
  "CAP_CAT_CD_SK": 1,
  "CAP_COPAY_TYP_CD_SK": 1,
  "CAP_LOB_CD_SK": 1,
  "CAP_POOL_CD_SK": 1,
  "CAP_PERD_CD_SK": 1,
  "CAP_SCHD_CD_SK": 1,
  "CAP_TYP_CD_SK": 1,
  "CAP_FUND_ACCTG_CAT_CD": "NA",
  "CAP_FUND_ACCTG_CAT_NM": "NA",
  "GL_CAT_CD": "NA",
  "GL_CAT_NM": "NA",
  "CAP_FUND_ACCTG_CAT_CD_SK": 1,
  "GL_CAT_CD_SK": 1,
  "PROD_SH_NM_SK": 1,
  "LFSTYL_RATE_FCTR_SK": 1,
  "CAP_RECOV_CAP_AMT": 0
}
df_lnk_Na = spark.createDataFrame([Row(**row_na)])

# The xfrm_BusinessLogic data columns (with transformations) are in df_lnk_Detail. We must union them with df_lnk_Unk and df_lnk_Na.
# First, ensure the columns match (we must select them in the same order).
final_columns = [
    "CAP_SK","SRC_SYS_CD","MBR_UNIQ_KEY","CAP_PROV_ID","CAP_ERN_YR_MO_SK","CAP_PD_YR_MO_SK",
    "CAP_FUND_ID","CAP_POOL_CD","CAP_SEQ_NO","CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK","PROV_SK","CAP_FUND_SK","CLS_SK","CLS_PLN_SK",
    "FNCL_LOB_SK","GRP_SK","MBR_SK","NTWK_SK","PD_PROV_SK","PCP_PROV_SK","PROD_SK","SUBGRP_SK",
    "CAP_ADJ_RSN_CD","CAP_ADJ_STTUS_CD","CAP_ADJ_TYP_CD","CAP_CAT_CD","CAP_COPAY_TYP_CD",
    "CAP_LOB_CD","CAP_PERD_CD","CAP_SCHD_CD","CAP_TYP_CD","CAP_MNL_ADJ_IN","CAP_ERN_DT_SK",
    "CAP_PD_DT_SK","MBR_BRTH_DT_SK","CAP_ADJ_AMT","CAP_CAP_AMT","CAP_COPAY_AMT","CAP_FUND_RATE_AMT",
    "MBR_AGE","MBR_MO_CT","CLS_ID","CLS_MCAID_ELIG_TTL","EXPRNC_CAT_CD","FNCL_LOB_CD","GRP_ID",
    "MBR_GNDR_CD","NTWK_ID","NTWK_SH_NM","PD_PROV_ID","PCP_PROV_ID","PROD_ID","PROD_SH_NM",
    "CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK","CAP_ADJ_RSN_CD_SK","CAP_ADJ_STTUS_CD_SK","CAP_ADJ_TYP_CD_SK",
    "CAP_CAT_CD_SK","CAP_COPAY_TYP_CD_SK","CAP_LOB_CD_SK","CAP_POOL_CD_SK","CAP_PERD_CD_SK","CAP_SCHD_CD_SK","CAP_TYP_CD_SK",
    "CAP_FUND_ACCTG_CAT_CD","CAP_FUND_ACCTG_CAT_NM","GL_CAT_CD","GL_CAT_NM","CAP_FUND_ACCTG_CAT_CD_SK","GL_CAT_CD_SK",
    "PROD_SH_NM_SK","LFSTYL_RATE_FCTR_SK","CAP_RECOV_CAP_AMT"
]

# Build df_lnk_Detail by mapping from the columns in df_xfrm
df_lnk_Detail_out = df_lnk_Detail.select(
    F.col("CAP_SK").alias("CAP_SK"),
    F.col("SRCSYSCD").alias("SRC_SYS_CD"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.expr("CASE WHEN substring(CAP_PROV_ID,1,3)='MHA' AND CAPTYPCD='SEC' THEN PCP_PROV_ID ELSE CAP_PROV_ID END").alias("CAP_PROV_ID"),
    F.expr("""IF IsNull(ERM_YR_MO_SK)=true THEN '175301' ELSE ERM_YR_MO_SK END""").alias("CAP_ERN_YR_MO_SK"),
    F.expr("""IF IsNull(PD_YR_MO_SK)=true THEN '175301' ELSE PD_YR_MO_SK END""").alias("CAP_PD_YR_MO_SK"),
    F.col("CAP_FUND_ID").alias("CAP_FUND_ID"),
    F.expr("""IF IsNull(CAP_POOL_CD)=true THEN 'UNK' ELSE CAP_POOL_CD END""").alias("CAP_POOL_CD"),
    F.col("SEQ_NO").alias("CAP_SEQ_NO"),
    F.lit("EDWRunCycleDate").alias("CRT_RUN_CYC_EXCTN_DT_SK"),  # DataStage expression
    F.lit("EDWRunCycleDate").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.expr("CASE WHEN substring(CAP_PROV_ID,1,3)='MHA' AND CAPTYPCD='SEC' THEN PCP_PROV_SK ELSE CAP_PROV_SK END").alias("PROV_SK"),
    F.col("CAP_FUND_SK").alias("CAP_FUND_SK"),
    F.col("CLS_SK").alias("CLS_SK"),
    F.col("CLS_PLN_SK").alias("CLS_PLN_SK"),
    F.col("FNCL_LOB_SK").alias("FNCL_LOB_SK"),
    F.col("GRP_SK").alias("GRP_SK"),
    F.col("MBR_SK").alias("MBR_SK"),
    F.col("NTWK_SK").alias("NTWK_SK"),
    F.col("PD_PROV_SK").alias("PD_PROV_SK"),
    F.col("PCP_PROV_SK").alias("PCP_PROV_SK"),
    F.col("PROD_SK").alias("PROD_SK"),
    F.col("SUBGRP_SK").alias("SUBGRP_SK"),
    F.expr("""IF IsNull(CAPADJCD)=true THEN 'UNK' ELSE CAPADJCD END""").alias("CAP_ADJ_RSN_CD"),
    F.expr("""IF IsNull(ADJSTTUSCD)=true THEN 'UNK' ELSE ADJSTTUSCD END""").alias("CAP_ADJ_STTUS_CD"),
    F.expr("""IF IsNull(ADJTYPCD)=true THEN 'UNK' ELSE ADJTYPCD END""").alias("CAP_ADJ_TYP_CD"),
    F.expr("""IF IsNull(CAPCATCD)=true THEN 'UNK' ELSE CAPCATCD END""").alias("CAP_CAT_CD"),
    F.expr("""IF IsNull(COPAYTYPCD)=true THEN 'UNK' ELSE COPAYTYPCD END""").alias("CAP_COPAY_TYP_CD"),
    F.expr("""IF IsNull(CAPLOBCD)=true THEN 'UNK' ELSE CAPLOBCD END""").alias("CAP_LOB_CD"),
    F.expr("""IF IsNull(PERDCD)=true THEN 'UNK' ELSE PERDCD END""").alias("CAP_PERD_CD"),
    F.expr("""IF IsNull(CAP_SCHD_CD)=true THEN 'UNK' ELSE CAP_SCHD_CD END""").alias("CAP_SCHD_CD"),
    F.expr("""IF IsNull(CAPTYPCD)=true THEN 'UNK' ELSE CAPTYPCD END""").alias("CAP_TYP_CD"),
    F.expr("""IF trim(CAPMNLADJIN) <> 'MNL' OR IsNull(CAPMNLADJIN)=true THEN 'N' ELSE 'Y' END""").alias("CAP_MNL_ADJ_IN"),
    F.col("ERN_DT_SK").alias("CAP_ERN_DT_SK"),
    F.col("PD_DT_SK").alias("CAP_PD_DT_SK"),
    F.expr("""IF IsNull(MBR_BRTH_DT_SK)=true THEN '1753-01-01' ELSE MBR_BRTH_DT_SK END""").alias("MBR_BRTH_DT_SK"),
    F.col("ADJ_AMT").alias("CAP_ADJ_AMT"),
    F.col("CAP_AMT").alias("CAP_CAP_AMT"),
    F.col("COPAY_AMT").alias("CAP_COPAY_AMT"),
    F.col("FUND_RATE_AMT").alias("CAP_FUND_RATE_AMT"),
    F.col("MBR_AGE").alias("MBR_AGE"),
    F.col("MBR_MO_CT").alias("MBR_MO_CT"),
    F.expr("""IF IsNull(CLS_ID)=true THEN 'UNK' ELSE CLS_ID END""").alias("CLS_ID"),
    F.expr("""IF IsNull(MCAID_ELIG_TTL)=true THEN 'UNK' ELSE MCAID_ELIG_TTL END""").alias("CLS_MCAID_ELIG_TTL"),
    F.expr("""IF IsNull(EXPRNC_CAT_CD)=true THEN 'UNK' ELSE EXPRNC_CAT_CD END""").alias("EXPRNC_CAT_CD"),
    F.expr("""IF IsNull(FNCL_LOB_CD)=true THEN 'UNK' ELSE FNCL_LOB_CD END""").alias("FNCL_LOB_CD"),
    F.expr("""IF IsNull(GRP_ID)=true THEN 'UNK' ELSE GRP_ID END""").alias("GRP_ID"),
    F.expr("""IF IsNull(MBRGNDRCD)=true THEN 'UNK' ELSE MBRGNDRCD END""").alias("MBR_GNDR_CD"),
    F.expr("""IF IsNull(NTWK_ID)=true THEN 'UNK' ELSE NTWK_ID END""").alias("NTWK_ID"),
    F.expr("""IF IsNull(NTWK_SH_NM)=true THEN 'UNK' ELSE NTWK_SH_NM END""").alias("NTWK_SH_NM"),
    F.expr("""IF IsNull(PD_PROV_ID)=true THEN 'UNK' ELSE PD_PROV_ID END""").alias("PD_PROV_ID"),
    F.expr("""IF IsNull(PCP_PROV_ID)=true THEN 'UNK' ELSE PCP_PROV_ID END""").alias("PCP_PROV_ID"),
    F.expr("""IF IsNull(PROD_ID)=true THEN 'UNK' ELSE PROD_ID END""").alias("PROD_ID"),
    F.expr("""IF IsNull(PROD_SH_NM)=true THEN 'UNK' ELSE PROD_SH_NM END""").alias("PROD_SH_NM"),
    F.lit("EDWRunCycle").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit("EDWRunCycle").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CAP_ADJ_RSN_CD_SK").alias("CAP_ADJ_RSN_CD_SK"),
    F.col("CAP_ADJ_STTUS_CD_SK").alias("CAP_ADJ_STTUS_CD_SK"),
    F.col("CAP_ADJ_TYP_CD_SK").alias("CAP_ADJ_TYP_CD_SK"),
    F.col("CAP_CAT_CD_SK").alias("CAP_CAT_CD_SK"),
    F.col("CAP_COPAY_TYP_CD_SK").alias("CAP_COPAY_TYP_CD_SK"),
    F.col("CAP_LOB_CD_SK").alias("CAP_LOB_CD_SK"),
    F.col("CAP_POOL_CD_SK").alias("CAP_POOL_CD_SK"),
    F.col("CAP_PERD_CD_SK").alias("CAP_PERD_CD_SK"),
    F.col("CAP_SCHD_CD_SK").alias("CAP_SCHD_CD_SK"),
    F.col("CAP_TYP_CD_SK").alias("CAP_TYP_CD_SK"),
    F.expr("""IF IsNull(CAPFUNDACCCAT)=true THEN 'UNK' ELSE CAPFUNDACCCAT END""").alias("CAP_FUND_ACCTG_CAT_CD"),
    F.expr("""IF IsNull(CAPFUNACCTNM)=true THEN 'UNK' ELSE CAPFUNACCTNM END""").alias("CAP_FUND_ACCTG_CAT_NM"),
    F.expr("""IF IsNull(GICATCD)=true THEN 'UNK' ELSE GICATCD END""").alias("GL_CAT_CD"),
    F.expr("""IF IsNull(GICATNAME)=true THEN 'UNK' ELSE GICATNAME END""").alias("GL_CAT_NM"),
    F.col("CAP_FUND_ACCTG_CAT_CD_SK").alias("CAP_FUND_ACCTG_CAT_CD_SK"),
    F.col("GL_CAT_CD_SK").alias("GL_CAT_CD_SK"),
    F.col("PROD_SH_NM_SK").alias("PROD_SH_NM_SK"),
    F.col("LFSTYL_RATE_FCTR_SK").alias("LFSTYL_RATE_FCTR_SK"),
    F.lit("").alias("CAP_RECOV_CAP_AMT")  # WhereExpression was '', treat as empty string or 0. The job used "''"
)

df_lnk_Unk_out = df_lnk_Unk.select(final_columns)
df_lnk_Na_out = df_lnk_Na.select(final_columns)

# Funnel: Union the three dataframes in the specified order
df_funnel = df_lnk_Detail_out.unionByName(df_lnk_Unk_out).unionByName(df_lnk_Na_out)

# Now apply the string padding (rpad) for all columns in the final that are "char" type in the last stage:
# The final stage "seq_CAP_F_load_csv" has these columns with SqlType=char:
#  CAP_ERN_YR_MO_SK (6), CAP_PD_YR_MO_SK (6),
#  CRT_RUN_CYC_EXCTN_DT_SK (10), LAST_UPDT_RUN_CYC_EXCTN_DT_SK (10),
#  CAP_MNL_ADJ_IN (1), CAP_ERN_DT_SK (10), CAP_PD_DT_SK (10), MBR_BRTH_DT_SK (10).

df_final = df_funnel
df_final = df_final.withColumn("CAP_ERN_YR_MO_SK", F.rpad(F.col("CAP_ERN_YR_MO_SK"), 6, " "))
df_final = df_final.withColumn("CAP_PD_YR_MO_SK", F.rpad(F.col("CAP_PD_YR_MO_SK"), 6, " "))
df_final = df_final.withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
df_final = df_final.withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
df_final = df_final.withColumn("CAP_MNL_ADJ_IN", F.rpad(F.col("CAP_MNL_ADJ_IN"), 1, " "))
df_final = df_final.withColumn("CAP_ERN_DT_SK", F.rpad(F.col("CAP_ERN_DT_SK"), 10, " "))
df_final = df_final.withColumn("CAP_PD_DT_SK", F.rpad(F.col("CAP_PD_DT_SK"), 10, " "))
df_final = df_final.withColumn("MBR_BRTH_DT_SK", F.rpad(F.col("MBR_BRTH_DT_SK"), 10, " "))

df_final = df_final.select(final_columns)

# seq_CAP_F_load_csv (PxSequentialFile) => write to CAP_F.dat
write_files(
    df_final,
    f"{adls_path}/load/CAP_F.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)