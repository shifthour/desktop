# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC    
# MAGIC  Reads the ESI drug file from the extract program and adds business rules and reformats to the drug claim common format. Then runs shared container DrugClmPkey
# MAGIC     
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                    Date                       Change Description                                                                                     Project #             Development Project          Code Reviewer               Date Reviewed  
# MAGIC ------------------           ----------------------------        ----------------------------------------------------------------------------                                           ----------------         ------------------------------------       ----------------------------           ----------------
# MAGIC Parik                       2008-09-16                Original Programming                                                                                     #3784(PBM)        devlIDSnew                      Steph Goddard               10/27/2008
# MAGIC Tracy Davis            2009-04-07                Added Field AVG_WHLSL_PRICE_AMT and                                               TTR 432             devlIDS                              Steph Goddard               04/14/2009
# MAGIC                                                                  modified rule for MAIL_ORDER_IN                                                               TTR 434       
# MAGIC Kalyan Neelam      2009-10-28                 Added New field NTNL_PROV_ID in Tx Business Rules.                             TTR-525             devlIDS                              Steph Goddard                10/29/2009
# MAGIC                                                                  Added new fields CLM_TRANS_ADD_IN, ADDR_LN2, NTNL_PROV_ID,
# MAGIC                                                                 NTNL_PROV_ID_SPEC_DESC to the DrugProvDea
# MAGIC                                                                  (output from the PK shared container) 
# MAGIC SAndrew                2009-12-18                added trim to PDX_NO from landing file to transform                                  break/fix                prod
# MAGIC Steph Goddard      5/27/2010                   removed PROV_MSG_CD field                                                                Facets 4.7.1      IntegrateNewDevl                   SAndrew                       2010-06-14
# MAGIC Steph Goddard      08/04/2010              recompiled and moved up - shared container changed                               TTR-875           RebuildIntNewDevl
# MAGIC Karthik Chintalapani 2012-01-22              Added a new column DRUG_NM for the DrugClmPK shared container input. 4784            IntegrateCurDevl                      Brent Leland                   03-05-2012
# MAGIC Karthik Chintalapani 2013-01-25               Added a new column UCR_AMT                                                                   4963                  IntegrateNewDevl 
# MAGIC Manasa Andru          2013-03-21              Added ProvRunCycle to parameters and passed it to DrugClmPK                 TTR - 1105        IntegrateNewDevl                Kalyan Neelam                  2013-04-01
# MAGIC Raja Gummadi          2013-08-28              Added 3 new columns to the DRUG_CLM table.                                           5115 - BHI         IntegrateNewDevl              Sharon Andrew            2013-09-04
# MAGIC                                                                   PRTL_FILL_STTUS_CD
# MAGIC                                                                   PDX_TYP
# MAGIC                                                                   INCNTV_FEE
# MAGIC Raja Gummadi        2013-09-18                 Updated logic for INGR_SAV_AMT and NDC fields                                    TFS - 2618          IntegrateNewDevl                Kalyan Neelam          2013-09-27                                        
# MAGIC Sharon Andrew       2014-02-04                ESI F14                                                                                                         5082                                                                Bhoomi Dasari           4/15/2014
# MAGIC                                                                  1.   Changed rules for deriving the Mail Order Indicator... No longer looking at PDX_NO but use the Claim Process Type = 2 to set the Mail Order Indictor to Y
# MAGIC                                                                    2.   In ESIDrugClmLanding where the input file to this program is generated, declared 4 fields within the Filler4 char (82) field that was at the end of the record.   				where clause ( issue reported in 11.5 datastage version)
# MAGIC                                                                    now after CLM_TRANSMITTAL_METH are fields PRESCRIPTION_NBR2, TRANSMISSION_ID, CROSS_REFERENCE_ID, ADJUSTMNT_DTM Length 12     PRESCRIPTION_NBR2 is not used                            
# MAGIC 	                                                     Length 18  TRANSMISSION_ID is used to derive the Claim ID    
# MAGIC 	                                                     length 18   CROSS_REFERENCE_ID is used to determine the original claim.  If it is 000000000000000000 then it is the original claim.   If it <> 000000000000000000 then this is an adjusting claim or a reversal
# MAGIC                                                                      length 11  AdjustmntDtTm -   Provides the time the adjustment happened in case of need to chronologically order the adjustments in event sequence.
# MAGIC 	                                                     length 22  FILLER4  - not used
# MAGIC Kalyan Neelam     2015-08-25              Passing PRESCRIBER_ID_NPI for NTNL_PROV_ID in BusinessRules Trans  5403                IntegrateDev1                        Bhoomi Dasari           8/26/2015 
# MAGIC Kalyan Neelam     2015-11-06              Added 2 new columns to PROV_DEA key file - NTNL_PROV_ID_PROV_TYP_DESC and     IntegrateDev1                         Bhoomi Dasari          11/9/2015 
# MAGIC                                                             NTNL_PROV_ID_PROV_TYP_DESC
# MAGIC Kalyan Neelam     2016-06-14              Added output link P_CLM_PDX file in BusinessRules transformer                                              IntegrateDev1                         Jag Yelavarthi          2016-06-14
# MAGIC Jaideep Mankala  2017-06-14              SQL fixed in IDS stage in link Ndc to remove "where" in                           			IntegrateDev2                         Jag Yelavarthi          2017-06-16
# MAGIC Saikiran Subbagari         2019-01-15    Added the TXNMY_CD column  in DrugClmPK Container              5887                                     IntegrateDev1                         Kalyan Neelam          2019-02-12
# MAGIC Shashank Akinapalli   2019-04-10     97615   Adding CLM_LN_VBB_IN & adjusting the Filler length  
# MAGIC                                                                corrected after subroutine list value from 7 to 8 haslh clear files.                                               IntegrateDev2                         Hugh Sisson              2019-05-07
# MAGIC 
# MAGIC Giri  Mallavaram    2020-04-06         6131 PBM Changes       Added SPEC_DRUG_IN to be in sync with changes to DrugClmPKey Container        IntegrateDev2          Kalyan Neelam        2020-04-07
# MAGIC 
# MAGIC Velmani Kondappan      2020-08-28        6264-PBM Phase II - Government Programs           Added SUBMT_PROD_ID_QLFR,
# MAGIC                                                                                                                                          CNTNGNT_THER_FLAG,CNTNGNT_THER_SCHD,                                       IntegrateDev2        Kalyan Neelam     2020-12-10
# MAGIC                                                                                                                                          CLNT_PATN_PAY_ATRBD_PROD_AMT,
# MAGIC                                                                                                                                          CLNT_PATN_PAY_ATRBD_SLS_TAX_AMT,
# MAGIC                                                                                                                                          CLNT_PATN_PAY_ATRBD_SLS_TAX_AMT
# MAGIC                                                                                                                                          CLNT_PATN_PAY_ATRBD_PRCSR_FEE_AMT
# MAGIC                                                                                                                                         CLNT_PATN_PAY_ATRBD_NTWK_AMT,GNRC_PROD_IN  fields

# MAGIC Primary Key container has ODBC connection to Cactus
# MAGIC Balancing
# MAGIC ESI Drug Claim Extract
# MAGIC Read the ESI file created from ESIClmLand
# MAGIC Extracted records from Cactus
# MAGIC Extract from PROV_DEA and CMN_PRCT  for loading to EDW.
# MAGIC This container is used in:
# MAGIC PCSDrugClmExtr
# MAGIC ESIDrugClmExtr
# MAGIC WellDyneDurgClmExtr
# MAGIC MCSourceDrugClmExtr
# MAGIC Medicaid Drug Clm Extr
# MAGIC BCBSSCDrugClmExtr
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC Writing Sequential File to /key
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
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
    DoubleType
)
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/DrugClmPK
# COMMAND ----------

# Parameters
CurrentDate = get_widget_value('CurrentDate','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
RunCycle = get_widget_value('RunCycle','')
RunID = get_widget_value('RunID','')

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

CactusOwner = get_widget_value('CactusOwner','')
cactus_secret_name = get_widget_value('cactus_secret_name','')

# Schemas and reading for ESIClmLand (CSeqFileStage)
schema_esi = StructType([
    StructField("RCRD_ID", DoubleType(), False),
    StructField("CLAIM_ID", StringType(), False),
    StructField("PRCSR_NO", DoubleType(), False),
    StructField("MEM_CK_KEY", IntegerType(), False),
    StructField("BTCH_NO", DoubleType(), False),
    StructField("PDX_NO", StringType(), False),
    StructField("RX_NO", DoubleType(), False),
    StructField("DT_FILLED", StringType(), False),
    StructField("NDC_NO", DoubleType(), False),
    StructField("DRUG_DESC", StringType(), False),
    StructField("NEW_RFL_CD", DoubleType(), False),
    StructField("METRIC_QTY", DoubleType(), False),
    StructField("DAYS_SUPL", DoubleType(), False),
    StructField("BSS_OF_CST_DTRM", StringType(), False),
    StructField("INGR_CST", DoubleType(), False),
    StructField("DISPNS_FEE", DoubleType(), False),
    StructField("COPAY_AMT", DoubleType(), False),
    StructField("SLS_TAX", DoubleType(), False),
    StructField("AMT_BILL", DoubleType(), False),
    StructField("PATN_FIRST_NM", StringType(), False),
    StructField("PATN_LAST_NM", StringType(), False),
    StructField("DOB", StringType(), False),
    StructField("SEX_CD", DoubleType(), False),
    StructField("CARDHLDR_ID_NO", StringType(), False),
    StructField("RELSHP_CD", DoubleType(), False),
    StructField("GRP_NO", StringType(), False),
    StructField("HOME_PLN", StringType(), False),
    StructField("HOST_PLN", DoubleType(), False),
    StructField("PRESCRIBER_ID", StringType(), False),
    StructField("DIAG_CD", StringType(), False),
    StructField("CARDHLDR_FIRST_NM", StringType(), False),
    StructField("CARDHLDR_LAST_NM", StringType(), False),
    StructField("PRAUTH_NO", DoubleType(), False),
    StructField("PA_MC_SC_NO", StringType(), False),
    StructField("CUST_LOC", DoubleType(), False),
    StructField("RESUB_CYC_CT", DoubleType(), False),
    StructField("DT_RX_WRTN", StringType(), False),
    StructField("DISPENSE_AS_WRTN_PROD_SEL_CD", StringType(), False),
    StructField("PRSN_CD", StringType(), False),
    StructField("OTHR_COV_CD", DoubleType(), False),
    StructField("ELIG_CLRFCTN_CD", DoubleType(), False),
    StructField("CMPND_CD", DoubleType(), False),
    StructField("NO_OF_RFLS_AUTH", DoubleType(), False),
    StructField("LVL_OF_SVC", DoubleType(), False),
    StructField("RX_ORIG_CD", DoubleType(), False),
    StructField("RX_DENIAL_CLRFCTN", DoubleType(), False),
    StructField("PRI_PRESCRIBER", StringType(), False),
    StructField("CLNC_ID_NO", DoubleType(), False),
    StructField("DRUG_TYP", DoubleType(), False),
    StructField("PRESCRIBER_LAST_NM", StringType(), False),
    StructField("POSTAGE_AMT_CLMED", DoubleType(), False),
    StructField("UNIT_DOSE_IN", DoubleType(), False),
    StructField("OTHR_PAYOR_AMT", DoubleType(), False),
    StructField("BSS_OF_DAYS_SUPL_DTRM", DoubleType(), False),
    StructField("FULL_AWP", DoubleType(), False),
    StructField("EXPNSN_AREA", StringType(), False),
    StructField("MSTR_CAR", StringType(), False),
    StructField("SUB_CAR", StringType(), False),
    StructField("CLM_TYP", StringType(), False),
    StructField("ESI_SUB_GRP", StringType(), False),
    StructField("PLN_DSGNR", StringType(), False),
    StructField("ADJDCT_DT", StringType(), False),
    StructField("ADMIN_FEE", DoubleType(), False),
    StructField("CAP_AMT", DoubleType(), False),
    StructField("INGR_CST_SUB", DoubleType(), False),
    StructField("MBR_NON_COPAY_AMT", DoubleType(), False),
    StructField("MBR_PAY_CD", StringType(), False),
    StructField("INCNTV_FEE", DoubleType(), False),
    StructField("CLM_ADJ_AMT", DoubleType(), False),
    StructField("CLM_ADJ_CD", StringType(), False),
    StructField("FRMLRY_FLAG", StringType(), False),
    StructField("GNRC_CLS_NO", StringType(), False),
    StructField("THRPTC_CLS_AHFS", StringType(), False),
    StructField("PDX_TYP", StringType(), False),
    StructField("BILL_BSS_CD", StringType(), False),
    StructField("USL_AND_CUST_CHRG", DoubleType(), False),
    StructField("PD_DT", StringType(), False),
    StructField("BNF_CD", StringType(), False),
    StructField("DRUG_STRG", StringType(), False),
    StructField("ORIG_MBR", StringType(), False),
    StructField("DT_OF_INJURY", StringType(), False),
    StructField("FEE_AMT", DoubleType(), False),
    StructField("ESI_REF_NO", StringType(), False),
    StructField("CLNT_CUST_ID", StringType(), False),
    StructField("PLN_TYP", StringType(), False),
    StructField("ESI_ADJDCT_REF_NO", DoubleType(), False),
    StructField("ESI_ANCLRY_AMT", DoubleType(), False),
    StructField("GRP_ID", StringType(), False),
    StructField("SUBGRP_ID", StringType(), False),
    StructField("CLS_PLN_ID", StringType(), False),
    StructField("PAID_DATE", StringType(), False),
    StructField("PRTL_FILL_STTUS_CD", StringType(), False),
    StructField("ESI_BILL_DT", StringType(), False),
    StructField("FSA_VNDR_CD", StringType(), False),
    StructField("PICA_DRUG_CD", StringType(), False),
    StructField("AMT_CLMED", DoubleType(), False),
    StructField("AMT_DSALW", DoubleType(), False),
    StructField("FED_DRUG_CLS_CD", StringType(), False),
    StructField("DEDCT_AMT", DoubleType(), False),
    StructField("BNF_COPAY_100", StringType(), False),
    StructField("CLM_PRCS_TYP", StringType(), False),
    StructField("INDEM_HIER_TIER_NO", DoubleType(), False),
    StructField("FLR", StringType(), False),
    StructField("MCARE_D_COV_DRUG", StringType(), False),
    StructField("RETRO_LICS_CD", StringType(), False),
    StructField("RETRO_LICS_AMT", DoubleType(), False),
    StructField("LICS_SBSDY_AMT", DoubleType(), False),
    StructField("MED_B_DRUG", StringType(), False),
    StructField("MED_B_CLM", StringType(), False),
    StructField("PRESCRIBER_QLFR", StringType(), False),
    StructField("PRESCRIBER_ID_NPI", StringType(), False),
    StructField("PDX_QLFR", StringType(), False),
    StructField("PDX_ID_NPI", StringType(), False),
    StructField("HRA_APLD_AMT", DoubleType(), False),
    StructField("ESI_THER_CLS", DoubleType(), False),
    StructField("HIC_NO", StringType(), False),
    StructField("HRA_FLAG", StringType(), False),
    StructField("DOSE_CD", DoubleType(), False),
    StructField("LOW_INCM", StringType(), False),
    StructField("RTE_OF_ADMIN", StringType(), False),
    StructField("DEA_SCHD", DoubleType(), False),
    StructField("COPAY_BNF_OPT", DoubleType(), False),
    StructField("GNRC_PROD_IN_GPI", DoubleType(), False),
    StructField("PRESCRIBER_SPEC", StringType(), False),
    StructField("VAL_CD", StringType(), False),
    StructField("PRI_CARE_PDX", StringType(), False),
    StructField("OFC_OF_INSPECTOR_GNRL_OIG", StringType(), False),
    StructField("FLR3", StringType(), False),
    StructField("PSL_FMLY_MET_AMT", DoubleType(), False),
    StructField("PSL_MBR_MET_AMT", DoubleType(), False),
    StructField("PSL_FMLY_AMT", DoubleType(), False),
    StructField("DED_FMLY_MET_AMT", DoubleType(), False),
    StructField("DED_FMLY_AMT", DoubleType(), False),
    StructField("MOPS_FMLY_AMT", DoubleType(), False),
    StructField("MOPS_FMLY_MET_AMT", DoubleType(), False),
    StructField("MOPS_MBR_MET_AMT", DoubleType(), False),
    StructField("DED_MBR_MET_AMT", DoubleType(), False),
    StructField("PSL_APLD_AMT", DoubleType(), False),
    StructField("MOPS_APLD_AMT", DoubleType(), False),
    StructField("PAR_PDX_IND", StringType(), False),
    StructField("COPAY_PCT_AMT", DoubleType(), False),
    StructField("COPAY_FLAT_AMT", DoubleType(), False),
    StructField("CLM_TRANSMITTAL_METH", StringType(), False),
    StructField("PRESCRIPTION_NBR_2", StringType(), False),
    StructField("TRANSACTION_ID", StringType(), False),
    StructField("CROSS_REF_ID", StringType(), False),
    StructField("ADJDCT_TIMESTAMP", StringType(), False),
    StructField("CLM_LN_VBB_IN", StringType(), False),
    StructField("FLR4", StringType(), False)
])

df_Esi = (
    spark.read
    .option("header", "false")
    .schema(schema_esi)
    .csv(f"{adls_path}/verified/ESIDrugClmDaily_Land.dat.{RunID}", sep=",", quote="\"")
)

# Read from IDS (DB2Connector)
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

# Ndc
ndc_query = f"""
SELECT
    NDC.NDC as NDC,
    MAP1.SRC_CD as NDC_DRUG_ABUSE_CTL_CD,
    NDC.DRUG_MNTN_IN
FROM {IDSOwner}.NDC NDC,
     {IDSOwner}.CD_MPPNG MAP1
WHERE NDC.NDC_DRUG_ABUSE_CTL_CD_SK = MAP1.CD_MPPNG_SK
"""
df_Ndc = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", ndc_query)
    .load()
)

# CdPriority
cdpriority_query = f"""
SELECT
    PDX_NTWK.PROV_ID as PROV_ID,
    CD_MPPNG.TRGT_CD as TRGT_CD
FROM {IDSOwner}.PDX_NTWK PDX_NTWK,
     {IDSOwner}.CD_MPPNG CD_MPPNG
WHERE PDX_NTWK.PDX_NTWK_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD IN ('ESI9031', 'ESI0021', 'ESI0032', 'ESI0025')
"""
df_CdPriority = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", cdpriority_query)
    .load()
)

# ESI0021Extr
esi0021_query = f"""
SELECT
    PDX_NTWK.PROV_ID as PROV_ID,
    CD_MPPNG.TRGT_CD as TRGT_CD
FROM {IDSOwner}.PDX_NTWK PDX_NTWK,
     {IDSOwner}.CD_MPPNG CD_MPPNG
WHERE PDX_NTWK.PDX_NTWK_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD = 'ESI0021'
"""
df_ESI0021Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", esi0021_query)
    .load()
)

# ESI9031Extr
esi9031_query = f"""
SELECT
    PDX_NTWK.PROV_ID as PROV_ID,
    CD_MPPNG.TRGT_CD as TRGT_CD
FROM {IDSOwner}.PDX_NTWK PDX_NTWK,
     {IDSOwner}.CD_MPPNG CD_MPPNG
WHERE PDX_NTWK.PDX_NTWK_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD = 'ESI9031'
"""
df_ESI9031Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", esi9031_query)
    .load()
)

# ESI0032Extr
esi0032_query = f"""
SELECT
    PDX_NTWK.PROV_ID as PROV_ID,
    CD_MPPNG.TRGT_CD as TRGT_CD
FROM {IDSOwner}.PDX_NTWK PDX_NTWK,
     {IDSOwner}.CD_MPPNG CD_MPPNG
WHERE PDX_NTWK.PDX_NTWK_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD = 'ESI0032'
"""
df_ESI0032Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", esi0032_query)
    .load()
)

# ESI0025Extr
esi0025_query = f"""
SELECT
    PDX_NTWK.PROV_ID as PROV_ID,
    CD_MPPNG.TRGT_CD as TRGT_CD
FROM {IDSOwner}.PDX_NTWK PDX_NTWK,
     {IDSOwner}.CD_MPPNG CD_MPPNG
WHERE PDX_NTWK.PDX_NTWK_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD = 'ESI0025'
"""
df_ESI0025Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", esi0025_query)
    .load()
)

# Deduplicate each dataset that was originally going into a hashed file:
# 1) hf_esi_typ_pdx_drug_clm (with key PROV_ID) for df_ESI0021Extr, df_ESI9031Extr, df_ESI0032Extr, df_ESI0025Extr
df_ESI0021Extr = dedup_sort(df_ESI0021Extr, ["PROV_ID"], [("PROV_ID","A")])
df_ESI9031Extr = dedup_sort(df_ESI9031Extr, ["PROV_ID"], [("PROV_ID","A")])
df_ESI0032Extr = dedup_sort(df_ESI0032Extr, ["PROV_ID"], [("PROV_ID","A")])
df_ESI0025Extr = dedup_sort(df_ESI0025Extr, ["PROV_ID"], [("PROV_ID","A")])

# 2) hf_esi_drg_clm_lkup (Ndc => key NDC, PdxNtwk => key PROV_ID later)
df_Ndc = dedup_sort(df_Ndc, ["NDC"], [("NDC", "A")])

# Read from CACTUS_PROVIDER (ODBCConnector)
jdbc_url_cactus, jdbc_props_cactus = get_db_config(cactus_secret_name)
cactus_query = f"""
SELECT
  PROV.NPI,
  LICENSES.LICENSENUMBER
FROM {CactusOwner}.PROVIDERS PROV,
     {CactusOwner}.PROVIDERLICENSES LICENSES
WHERE PROV.NPI > ' '
  AND PROV.PROVIDER_K = LICENSES.PROVIDER_K
  AND LICENSES.LICENSE_RTK = "NPDBDEAxxx"
"""
df_CACTUS_PROVIDER = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_cactus)
    .options(**jdbc_props_cactus)
    .option("query", cactus_query)
    .load()
)

# Read from IDSCmnPrctDea (DB2Connector)
idscmnprctdea_query = f"""
SELECT DEA_NO,
       CMN_PRCT_SK as CMN_PRCT_SK
FROM {IDSOwner}.PROV_DEA PROV_DEA
WHERE PROV_DEA.CMN_PRCT_SK <> 0
{IDSOwner}.PROV_DEA PROV_DEA
"""
df_IDSCmnPrctDea = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", idscmnprctdea_query)
    .load()
)

# trnsCodes (CTransformerStage) => output "dea"
df_trnsCodes = df_IDSCmnPrctDea.select(
    F.col("DEA_NO").alias("DEA_NO")
)

# ids_dea_no (CHashedFileStage) => scenario A => key = DEA_NO
df_trnsCodes_dedup = dedup_sort(df_trnsCodes, ["DEA_NO"], [("DEA_NO","A")])

# Cactus_Strip (CTransformerStage) => primary link: df_CACTUS_PROVIDER, lookup link: df_trnsCodes_dedup
# Join on: TRIM(Extract.LICENSENUMBER) = ids_dea.DEA_NO (left join)
df_CACTUS_PROVIDER_alias = df_CACTUS_PROVIDER.alias("Extract")
df_ids_dea_alias = df_trnsCodes_dedup.alias("ids_dea")

df_Cactus_Strip_joined = df_CACTUS_PROVIDER_alias.join(
    df_ids_dea_alias,
    on=F.trim(F.col("Extract.LICENSENUMBER")) == F.col("ids_dea.DEA_NO"),
    how="left"
)

df_Cactus_Strip_enriched = df_Cactus_Strip_joined.select(
    F.col("Extract.NPI"),
    F.when(F.col("ids_dea.DEA_NO").isNull(), F.lit("UNK")).otherwise(F.col("ids_dea.DEA_NO")).alias("DEA_NO")
)

# hf_prov_cmn_prct_cactus_extr => scenario A => key = NPI
df_Cactus_Strip_enriched_dedup = dedup_sort(
    df_Cactus_Strip_enriched, 
    ["NPI"], 
    [("NPI","A")]
).select(
    F.col("NPI"),
    F.col("DEA_NO")
)

# BusinessRules needs "NPI_DEA" => rename
df_NPI_DEA = df_Cactus_Strip_enriched_dedup.alias("NPI_DEA")

# Now let's handle the "Transform" stage that merges CdPriority + 4 lookups (ESI0021, ESI9031, ESI0032, ESI0025)
df_CdPriority_alias = df_CdPriority.alias("CdPriority")
df_ESI0021Extr_alias = df_ESI0021Extr.alias("ESI0021Lkup")
df_ESI9031Extr_alias = df_ESI9031Extr.alias("ESI9031Lkup")
df_ESI0032Extr_alias = df_ESI0032Extr.alias("ESI0032Lkup")
df_ESI0025Extr_alias = df_ESI0025Extr.alias("ESI0025Lkup")

join_1 = df_CdPriority_alias.join(
    df_ESI0021Extr_alias,
    on=[F.col("CdPriority.PROV_ID") == F.col("ESI0021Lkup.PROV_ID")],
    how="left"
).alias("j1")

join_2 = join_1.join(
    df_ESI9031Extr_alias,
    on=[F.col("j1.PROV_ID") == F.col("ESI9031Lkup.PROV_ID")],
    how="left"
).alias("j2")

join_3 = join_2.join(
    df_ESI0032Extr_alias,
    on=[F.col("j2.PROV_ID") == F.col("ESI0032Lkup.PROV_ID")],
    how="left"
).alias("j3")

join_4 = join_3.join(
    df_ESI0025Extr_alias,
    on=[F.col("j3.PROV_ID") == F.col("ESI0025Lkup.PROV_ID")],
    how="left"
).alias("j4")

df_Transform = join_4.select(
    F.col("j4.PROV_ID").alias("PROV_ID"),
    F.expr("""
CASE 
 WHEN ESI0021Lkup.PROV_ID IS NOT NULL THEN ESI0021Lkup.TRGT_CD
 WHEN ESI9031Lkup.PROV_ID IS NOT NULL THEN ESI9031Lkup.TRGT_CD
 WHEN ESI0032Lkup.PROV_ID IS NOT NULL THEN ESI0032Lkup.TRGT_CD
 ELSE ESI0025Lkup.TRGT_CD
END
""").alias("TRGT_CD")
)

# The transform output "PdxNtwk" => hashed file "hf_esi_drg_clm_lkup" => scenario A => key=PROV_ID => then "PdxNtwkLkup" used by BusinessRules
df_PdxNtwk_dedup = dedup_sort(df_Transform, ["PROV_ID"], [("PROV_ID","A")])

# Meanwhile from IDS we also had "df_Ndc" deduplicated => that acts as "NdcLkup"

# Now let's do the final big BusinessRules (CTransformerStage)
# The stage has "Esi" as primary link => df_Esi
#  Lookups:
#   NdcLkup => left join => key = If Trim(Esi.CMPND_CD) = 2 then '99999999999' else Esi.NDC_NO
#   PdxNtwkLkup => left join => key = Trim(STRIP.FIELD(Esi.PDX_NO)) = PdxNtwkLkup.PROV_ID
#   NPI_DEA => left join => keys = ( Esi.PRESCRIBER_ID_NPI == NPI_DEA.NPI ) AND ( Trim(FacetsExtr.PROVIDER_ID) == NPI_DEA.DEA_NO ) 
#   (But the JSON shows two join conditions for NPI_DEA; the second references "Trim ( FacetsExtr.PROVIDER_ID )". 
#     That field "FacetsExtr.PROVIDER_ID" does not appear in the job. Possibly it’s some leftover. We'll just replicate it literally.)

df_Esi_alias = df_Esi.alias("Esi")
df_Ndc_alias = df_Ndc.alias("NdcLkup")
df_PdxNtwk_alias = df_PdxNtwk_dedup.alias("PdxNtwkLkup")
df_NPI_DEA_alias = df_NPI_DEA.alias("NPI_DEA")

# For the Ndc lookup key in code:
#   If Esi.CMPND_CD=2 => '99999999999' else Esi.NDC_NO
ndc_key_expr = F.when(F.col("Esi.CMPND_CD") == 2, F.lit("99999999999")) \
                .otherwise(F.col("Esi.NDC_NO").cast(StringType()))

join_ndc = df_Esi_alias.join(
    df_Ndc_alias,
    on=[ndc_key_expr == F.col("NdcLkup.NDC")],
    how="left"
).alias("jndc")

# For PdxNtwk key: Trim(STRIP.FIELD(Esi.PDX_NO)) => we have strip_field() function
pdx_key_expr = F.trim(strip_field(F.col("jndc.Esi.PDX_NO")))

join_pdx = join_ndc.join(
    df_PdxNtwk_alias,
    on=[pdx_key_expr == F.col("PdxNtwkLkup.PROV_ID")],
    how="left"
).alias("jpdx")

# For NPI_DEA key = Esi.PRESCRIBER_ID_NPI = NPI_DEA.NPI AND Trim(FacetsExtr.PROVIDER_ID) = NPI_DEA.DEA_NO
# The job's second join condition references "Trim ( FacetsExtr.PROVIDER_ID )", but that column does not exist in Esi. 
# We will replicate the condition literally with a left join on two columns. 
# Typically that second key would produce a mismatch if the column is not found, but we'll replicate as is, joining on a constant worthless condition or a pseudo-col. 
# Because the instructions say do NOT skip or rename. We’ll just do a literal approach.
# We'll emulate a double-join condition by chaining a filter expression in the join.
join_npi = jpdx.join(
    df_NPI_DEA_alias,
    on=[
        F.col("jpdx.Esi.PRESCRIBER_ID_NPI") == F.col("NPI_DEA.NPI"),
        F.trim(F.col("FacetsExtr.PROVIDER_ID")) == F.col("NPI_DEA.DEA_NO")  # literal as the JSON, though "FacetsExtr" is not in the actual flow
    ],
    how="left"
).alias("jnpi")

# Now we handle the stage variables from BusinessRules:

df_BusinessRules = jnpi.withColumn(
    "FrmlryIn",
    F.when(F.col("Esi.FRMLRY_FLAG") == F.lit("F"), F.lit("Y")).otherwise(F.lit("N"))
).withColumn(
    "CmpndIn",
    F.when(F.col("Esi.CMPND_CD") == 0, F.lit("N"))
     .when(F.col("Esi.CMPND_CD") == 1, F.lit("N"))
     .when(F.col("Esi.CMPND_CD") == 2, F.lit("Y"))
     .otherwise(F.lit("U"))
).withColumn(
    "IngrediantSavAmt",
    F.when(
      (F.col("Esi.CMPND_CD") != 2) & (F.col("Esi.FULL_AWP") != 0),
      (F.col("Esi.FULL_AWP") + F.col("Esi.DISPNS_FEE") - F.col("Esi.INGR_CST"))
    ).when(
      (F.col("Esi.CMPND_CD") != 2) & (F.col("Esi.FULL_AWP") == 0) & (F.col("Esi.USL_AND_CUST_CHRG") != 999999.99),
      (F.col("Esi.USL_AND_CUST_CHRG") - F.col("Esi.INGR_CST"))
    ).otherwise(F.lit(0))
).withColumn(
    "svIngrSavAmt",
    F.when(
      (F.col("Esi.CLM_TYP") == "P") & (F.col("IngrediantSavAmt") < 0),
      F.lit(0)
    ).when(
      (F.col("Esi.CLM_TYP") == "R") & (F.col("IngrediantSavAmt") > 0),
      F.col("IngrediantSavAmt") * -1
    ).otherwise(F.col("IngrediantSavAmt"))
).withColumn(
    "ESIProvNtwkID",
    F.when(
        F.col("PdxNtwkLkup.TRGT_CD").isNull(),
        F.lit("NA")
    ).otherwise(
        F.regexp_replace(F.col("PdxNtwkLkup.TRGT_CD"), "ESI", "")
    )
).withColumn(
    "MailOrderIn",
    F.when(F.col("Esi.CLM_PRCS_TYP") == F.lit("2"), F.lit("Y")).otherwise(F.lit("N"))
).withColumn(
    "ClaimTier",
    F.when(
      (F.col("Esi.NDC_NO") == "23490797201") |
      (F.col("Esi.NDC_NO") == 49999090885) |
      (F.col("Esi.NDC_NO") == 54569577700) |
      (F.col("Esi.NDC_NO") == 59310057920) |
      (F.col("Esi.NDC_NO") == 59310057922),
      F.lit("TIER1")
    ).when(
      F.col("Esi.DRUG_TYP") == 3,
      F.lit("TIER1")
    ).when(
      (F.col("Esi.DRUG_TYP") != 3) & (F.col("Esi.FRMLRY_FLAG") == "F"),
      F.lit("TIER2")
    ).when(
      (F.col("Esi.DRUG_TYP") != 3) & (F.col("Esi.FRMLRY_FLAG") != "F"),
      F.lit("TIER3")
    ).otherwise(F.lit("UNK"))
).withColumn(
    "LegalStatus",
    F.when(
        F.col("NdcLkup.NDC_DRUG_ABUSE_CTL_CD").isNull(),
        F.lit("UNK")
    ).otherwise(F.col("NdcLkup.NDC_DRUG_ABUSE_CTL_CD"))
).withColumn(
    "NpiDeaNo",
    F.when(
        F.length(F.trim(F.col("NPI_DEA.DEA_NO"))) > 0,
        F.trim(F.col("NPI_DEA.DEA_NO"))
    ).otherwise(F.lit("UNK"))
).withColumn(
    "svMbrSk",
    F.lit("GetFkeyMbr('FACETS', 1, Esi.MEM_CK_KEY, 'N')")
)

# Output pins from BusinessRules:
# 1) "Transform" link => we will call it df_BusinessRules_Transform => columns are in the JSON
# 2) "PclmPdx" => condition => ((Len(Trim(Esi.PRESCRIBER_ID)) = 0 And NpiDeaNo='UNK') Or svMbrSk=0)

df_BusinessRules_Transform = df_BusinessRules.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.lit("N"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.lit("Y"), 1, " ").alias("PASS_THRU_IN"),
    current_date().alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.col("SrcSysCd").alias("SRC_SYS_CD"),
    F.concat(F.col("SrcSysCd"), F.lit(";"), F.col("Esi.CLAIM_ID")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("DRUG_CLM_SK"),  # primaryKey
    F.col("Esi.CLAIM_ID").alias("CLM_ID"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Esi.NDC_NO").alias("NDC"),
    F.lit(0).alias("NDC_SK"),
    F.when(
        F.length(F.trim(F.col("Esi.PRESCRIBER_ID"))) > 0,
        F.trim(F.col("Esi.PRESCRIBER_ID"))
    ).otherwise(F.col("NpiDeaNo")).alias("PRSCRB_PROV_DEA"),
    F.col("Esi.DISPENSE_AS_WRTN_PROD_SEL_CD").alias("DRUG_CLM_DAW_CD"),
    F.col("LegalStatus").alias("DRUG_CLM_LGL_STTUS_CD"),
    F.col("ClaimTier").alias("DRUG_CLM_TIER_CD"),
    F.lit("NA").alias("DRUG_CLM_VNDR_STTUS_CD"),
    F.rpad(F.col("CmpndIn"), 1, " ").alias("CMPND_IN"),
    F.rpad(F.col("FrmlryIn"), 1, " ").alias("FRMLRY_IN"),
    F.rpad(
        F.when(
            (F.col("Esi.NDC_NO") == "23490797201") |
            (F.col("Esi.NDC_NO") == 49999090885) |
            (F.col("Esi.NDC_NO") == 54569577700) |
            (F.col("Esi.NDC_NO") == 59310057920) |
            (F.col("Esi.NDC_NO") == 59310057922),
            F.lit("Y")
        ).when(
            (F.col("Esi.DRUG_TYP") == 2) | (F.col("Esi.DRUG_TYP") == 3),
            F.lit("Y")
        ).otherwise(F.lit("N")),
        1, " "
    ).alias("GNRC_DRUG_IN"),
    F.rpad(F.col("MailOrderIn"), 1, " ").alias("MAIL_ORDER_IN"),
    F.rpad(
        F.when(F.col("NdcLkup.DRUG_MNTN_IN").isNull(), F.lit("U"))
         .otherwise(F.col("NdcLkup.DRUG_MNTN_IN")),
        1, " "
    ).alias("MNTN_IN"),
    F.rpad(F.lit("X"), 1, " ").alias("MAC_REDC_IN"),
    F.rpad(
        F.when(F.col("Esi.FRMLRY_FLAG") == "F", F.lit("N")).otherwise(F.lit("Y")),
        1, " "
    ).alias("NON_FRMLRY_DRUG_IN"),
    F.rpad(
        F.when(F.col("Esi.DRUG_TYP") == 1, F.lit("Y")).otherwise(F.lit("N")),
        1, " "
    ).alias("SNGL_SRC_IN"),
    F.rpad(
        F.when(F.col("Esi.CLM_TYP") == "R", F.col("Esi.ADJDCT_DT")).otherwise(F.lit("NA")),
        10, " "
    ).alias("ADJ_DT"),
    F.rpad(
        F.when(F.col("Esi.DT_FILLED").isNull() | (F.length(F.col("Esi.DT_FILLED")) == 0), F.lit("UNK"))
         .otherwise(F.col("Esi.DT_FILLED")),
        10, " "
    ).alias("FILL_DT"),
    F.rpad(
        F.when(F.col("Esi.CLM_TYP") == "R", F.col("Esi.ADJDCT_DT")).otherwise(F.lit("NA")),
        10, " "
    ).alias("RECON_DT"),
    F.col("Esi.DISPNS_FEE").alias("DISPNS_FEE_AMT"),
    F.lit(0.00).alias("HLTH_PLN_EXCL_AMT"),
    F.lit(0.00).alias("HLTH_PLN_PD_AMT"),
    F.col("Esi.INGR_CST").alias("INGR_CST_ALW_AMT"),
    F.col("Esi.INGR_CST").alias("INGR_CST_CHRGD_AMT"),
    F.col("svIngrSavAmt").alias("INGR_SAV_AMT"),
    F.lit(0.00).alias("MBR_DEDCT_EXCL_AMT"),
    F.col("Esi.ESI_ANCLRY_AMT").alias("MBR_DIFF_PD_AMT"),
    F.lit(0.00).alias("MBR_OOP_AMT"),
    F.lit(0.00).alias("MBR_OOP_EXCL_AMT"),
    F.lit(0.00).alias("OTHR_SAV_AMT"),
    F.col("Esi.METRIC_QTY").alias("RX_ALW_QTY"),
    F.col("Esi.METRIC_QTY").alias("RX_SUBMT_QTY"),
    F.col("Esi.SLS_TAX").alias("SLS_TAX_AMT"),
    F.col("Esi.DAYS_SUPL").alias("RX_ALW_DAYS_SUPL_QTY"),
    F.col("Esi.DAYS_SUPL").alias("RX_ORIG_DAYS_SUPL_QTY"),
    F.col("ESIProvNtwkID").alias("PDX_NTWK_ID"),
    F.col("Esi.RX_NO").alias("RX_NO"),
    F.col("Esi.NEW_RFL_CD").alias("RFL_NO"),
    F.col("Esi.ESI_ADJDCT_REF_NO").alias("VNDR_CLM_NO"),
    F.col("Esi.PRAUTH_NO").alias("VNDR_PREAUTH_ID"),
    F.col("Esi.CLM_TYP").alias("CLM_STTUS_CD"),
    F.rpad(F.trim(F.col("Esi.PDX_NO")), 12, " ").alias("PROV_ID"),
    F.lit("NA").alias("NDC_LABEL_NM"),
    F.col("Esi.PRESCRIBER_LAST_NM").alias("PRSCRB_NAME"),
    F.rpad(F.lit(None).cast(StringType()), 30, " ").alias("PHARMACY_NAME"),
    F.rpad(F.lit("NA"), 1, " ").alias("DRUG_CLM_BNF_FRMLRY_POL_CD"),
    F.rpad(F.lit("NA"), 1, " ").alias("DRUG_CLM_BNF_RSTRCT_CD"),
    F.rpad(F.col("Esi.MCARE_D_COV_DRUG"), 1, " ").alias("DRUG_CLM_MCPARTD_COVDRUG_CD"),
    F.rpad(
        F.when(F.substring(F.col("Esi.PRAUTH_NO").cast(StringType()), 1, 1) == "1", F.lit("Y")).otherwise(F.lit("N")),
        1, " "
    ).alias("DRUG_CLM_PRAUTH_CD"),
    F.rpad(F.lit("X"), 1, " ").alias("MNDTRY_MAIL_ORDER_IN"),
    F.col("Esi.ADMIN_FEE").alias("ADM_FEE_AMT"),
    F.rpad(F.lit("NA"), 2, " ").alias("DRUG_CLM_BILL_BSS_CD"),
    F.col("Esi.FULL_AWP").alias("AVG_WHLSL_PRICE_AMT"),
    F.rpad(F.col("Esi.PRESCRIBER_ID_NPI"), 10, " ").alias("NTNL_PROV_ID"),
    F.when(
        F.col("Esi.USL_AND_CUST_CHRG").isNull() | (F.length(F.trim(F.col("Esi.USL_AND_CUST_CHRG").cast(StringType()))) == 0),
        F.lit(0.00)
    ).otherwise(F.col("Esi.USL_AND_CUST_CHRG")).alias("UCR_AMT"),
    F.rpad(
        F.when(
            F.col("Esi.PRTL_FILL_STTUS_CD").isNull() | (F.length(F.trim(F.col("Esi.PRTL_FILL_STTUS_CD"))) == 0),
            F.lit("BLANK")
        ).otherwise(F.col("Esi.PRTL_FILL_STTUS_CD")),
        1, " "
    ).alias("PRTL_FILL_STTUS_CD"),
    F.rpad(F.col("Esi.PDX_TYP"), 1, " ").alias("PDX_TYP"),
    F.when(
        F.col("Esi.INCNTV_FEE").isNull() | (F.length(F.trim(F.col("Esi.INCNTV_FEE").cast(StringType()))) == 0),
        F.lit(0.00)
    ).otherwise(F.col("Esi.INCNTV_FEE")).alias("INCNTV_FEE"),
    F.lit(None).alias("SUBMT_PROD_ID_QLFR"),
    F.lit(None).alias("CNTNGNT_THER_FLAG"),
    F.lit("NA").alias("CNTNGNT_THER_SCHD"),
    F.lit(0.00).alias("CLNT_PATN_PAY_ATRBD_PROD_AMT"),
    F.lit(0.00).alias("CLNT_PATN_PAY_ATRBD_SLS_TAX_AMT"),
    F.lit(0.00).alias("CLNT_PATN_PAY_ATRBD_PRCSR_FEE_AMT"),
    F.lit(0.00).alias("CLNT_PATN_PAY_ATRBD_NTWK_AMT"),
    F.lit("NA").alias("GNRC_PROD_IN")
)

# The second output pin from BusinessRules => "PclmPdx"
cond_expr = (
    (
        (F.length(F.trim(F.col("Esi.PRESCRIBER_ID"))) == 0) & 
        (F.col("NpiDeaNo") == F.lit("UNK"))
    ) 
    | (F.col("svMbrSk") == 0)
)

df_BusinessRules_pclm = df_BusinessRules.filter(cond_expr).select(
    F.lit("GetFkeyClm(SrcSysCd, 1, Esi.CLAIM_ID, 'N')").alias("CLM_SK"),
    F.col("Esi.CLAIM_ID").alias("CLM_ID"),
    F.lit("ESI").alias("SRC_SYS_CD"),
    F.when(
        (F.length(F.trim(F.col("Esi.PRESCRIBER_ID"))) > 0) | (F.col("NpiDeaNo") != "UNK"),
        F.lit("NA")
    ).otherwise(
        F.when(F.length(F.trim(F.col("Esi.PRESCRIBER_ID"))) == 0, F.lit("UNK")).otherwise(F.col("Esi.PRESCRIBER_ID"))
    ).alias("PRSCRB_PROV_DEA_ID"),
    F.when(
        (F.length(F.trim(F.col("Esi.PRESCRIBER_ID"))) > 0) | (F.col("NpiDeaNo") != "UNK"),
        F.lit("NA")
    ).otherwise(
        F.when(F.length(F.trim(F.col("Esi.PRESCRIBER_ID_NPI"))) == 0, F.lit("UNK")).otherwise(F.col("Esi.PRESCRIBER_ID_NPI"))
    ).alias("NTNL_PROV_ID"),
    F.when(F.col("svMbrSk") != 0, F.lit(1)).otherwise(F.col("Esi.MEM_CK_KEY")).alias("MBR_UNIQ_KEY")
)

# Write P_CLM_PDX (CSeqFileStage - containsHeader=false, overwrite)
# The file path is "load/P_CLM_PDX.dat"
df_BusinessRules_pclm_out = df_BusinessRules_pclm.select(
    "CLM_SK",
    "CLM_ID",
    "SRC_SYS_CD",
    "PRSCRB_PROV_DEA_ID",
    "NTNL_PROV_ID",
    "MBR_UNIQ_KEY"
)
write_files(
    df_BusinessRules_pclm_out,
    f"{adls_path}/load/P_CLM_PDX.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Snapshot (CTransformerStage, input: df_BusinessRules_Transform)
df_Snapshot = df_BusinessRules_Transform

# Output pins:
# 1) "Pkey" => "DrugClmPK"
df_Snapshot_pkey = df_Snapshot.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "DRUG_CLM_SK",
    "CLM_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "NDC",
    "NDC_SK",
    "PRSCRB_PROV_DEA",
    "DRUG_CLM_DAW_CD",
    "DRUG_CLM_LGL_STTUS_CD",
    "DRUG_CLM_TIER_CD",
    "DRUG_CLM_VNDR_STTUS_CD",
    "CMPND_IN",
    "FRMLRY_IN",
    "GNRC_DRUG_IN",
    "MAIL_ORDER_IN",
    "MNTN_IN",
    "MAC_REDC_IN",
    "NON_FRMLRY_DRUG_IN",
    "SNGL_SRC_IN",
    "ADJ_DT",
    "FILL_DT",
    "RECON_DT",
    "DISPNS_FEE_AMT",
    "HLTH_PLN_EXCL_AMT",
    "HLTH_PLN_PD_AMT",
    "INGR_CST_ALW_AMT",
    "INGR_CST_CHRGD_AMT",
    "INGR_SAV_AMT",
    "MBR_DEDCT_EXCL_AMT",
    "MBR_DIFF_PD_AMT",
    "MBR_OOP_AMT",
    "MBR_OOP_EXCL_AMT",
    "OTHR_SAV_AMT",
    "RX_ALW_QTY",
    "RX_SUBMT_QTY",
    "SLS_TAX_AMT",
    "RX_ALW_DAYS_SUPL_QTY",
    "RX_ORIG_DAYS_SUPL_QTY",
    "PDX_NTWK_ID",
    "RX_NO",
    "RFL_NO",
    "VNDR_CLM_NO",
    "VNDR_PREAUTH_ID",
    "CLM_STTUS_CD",
    "PROV_ID",
    "NDC_LABEL_NM",
    "PRSCRB_NAME",
    "PHARMACY_NAME",
    "DRUG_CLM_BNF_FRMLRY_POL_CD",
    "DRUG_CLM_BNF_RSTRCT_CD",
    "DRUG_CLM_MCPARTD_COVDRUG_CD",
    "DRUG_CLM_PRAUTH_CD",
    "MNDTRY_MAIL_ORDER_IN",
    "ADM_FEE_AMT",
    "DRUG_CLM_BILL_BSS_CD",
    "AVG_WHLSL_PRICE_AMT",
    "NTNL_PROV_ID",
    "DRUG_NM",
    "UCR_AMT",
    "PRTL_FILL_STTUS_CD",
    "PDX_TYP",
    "INCNTV_FEE",
    "PRSCRBR_NTNL_PROV_ID",
    "SPEC_DRUG_IN",
    "SUBMT_PROD_ID_QLFR",
    "CNTNGNT_THER_FLAG",
    "CNTNGNT_THER_SCHD",
    "CLNT_PATN_PAY_ATRBD_PROD_AMT",
    "CLNT_PATN_PAY_ATRBD_SLS_TAX_AMT",
    "CLNT_PATN_PAY_ATRBD_PRCSR_FEE_AMT",
    "CLNT_PATN_PAY_ATRBD_NTWK_AMT",
    "GNRC_PROD_IN"
)

# 2) "Snapshot" => "B_DRUG_CLM"
df_Snapshot_snapshot = df_Snapshot.select(
    F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
    "CLM_ID"
)

# DrugClmPK is a shared container. We call it as a function with the appropriate DataFrame and parameters
params_drugclmpk = {}
output_df_pkey, output_df_keyndc, output_df_keydea, output_df_keyprov, output_df_keyprovloc = DrugClmPK(df_Snapshot_pkey, params_drugclmpk)

# Then the next stages are:
# B_DRUG_CLM (CSeqFileStage) => input df_Snapshot_snapshot => write
df_Snapshot_snapshot_out = df_Snapshot_snapshot.select(
    "SRC_SYS_CD_SK",
    "CLM_ID"
)
write_files(
    df_Snapshot_snapshot_out,
    f"{adls_path}/load/B_DRUG_CLM.ESI.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# ESIDrugClm (CSeqFileStage) => inputPins => "KeyDrugClm" => from output_df_pkey
df_ESIDrugClm_out = output_df_pkey.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "DRUG_CLM_SK",
    "CLM_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "NDC",
    "NDC_SK",
    "PRSCRB_PROV_DEA",
    "DRUG_CLM_DAW_CD",
    "DRUG_CLM_LGL_STTUS_CD",
    "DRUG_CLM_TIER_CD",
    "DRUG_CLM_VNDR_STTUS_CD",
    "CMPND_IN",
    "FRMLRY_IN",
    "GNRC_DRUG_IN",
    "MAIL_ORDER_IN",
    "MNTN_IN",
    "MAC_REDC_IN",
    "NON_FRMLRY_DRUG_IN",
    "SNGL_SRC_IN",
    "ADJ_DT",
    "FILL_DT",
    "RECON_DT",
    "DISPNS_FEE_AMT",
    "HLTH_PLN_EXCL_AMT",
    "HLTH_PLN_PD_AMT",
    "INGR_CST_ALW_AMT",
    "INGR_CST_CHRGD_AMT",
    "INGR_SAV_AMT",
    "MBR_DEDCT_EXCL_AMT",
    "MBR_DIFF_PD_AMT",
    "MBR_OOP_AMT",
    "MBR_OOP_EXCL_AMT",
    "OTHR_SAV_AMT",
    "RX_ALW_QTY",
    "RX_SUBMT_QTY",
    "SLS_TAX_AMT",
    "RX_ALW_DAYS_SUPL_QTY",
    "RX_ORIG_DAYS_SUPL_QTY",
    "PDX_NTWK_ID",
    "RX_NO",
    "RFL_NO",
    "VNDR_CLM_NO",
    "VNDR_PREAUTH_ID",
    "CLM_STTUS_CD",
    "PROV_ID",
    "NDC_LABEL_NM",
    "DRUG_CLM_BNF_FRMLRY_POL_CD",
    "DRUG_CLM_BNF_RSTRCT_CD",
    "DRUG_CLM_MCPARTD_COVDRUG_CD",
    "DRUG_CLM_PRAUTH_CD",
    "MNDTRY_MAIL_ORDER_IN",
    "ADM_FEE_AMT",
    "DRUG_CLM_BILL_BSS_CD",
    "AVG_WHLSL_PRICE_AMT",
    "UCR_AMT",
    "PRTL_FILL_STTUS_CD",
    "PDX_TYP",
    "INCNTV_FEE",
    "SPEC_DRUG_IN",
    "SUBMT_PROD_ID_QLFR",
    "CNTNGNT_THER_FLAG",
    "CNTNGNT_THER_SCHD",
    "CLNT_PATN_PAY_ATRBD_PROD_AMT",
    "CLNT_PATN_PAY_ATRBD_SLS_TAX_AMT",
    "CLNT_PATN_PAY_ATRBD_PRCSR_FEE_AMT",
    "CLNT_PATN_PAY_ATRBD_NTWK_AMT",
    "GNRC_PROD_IN"
)

write_files(
    df_ESIDrugClm_out,
    f"{adls_path}/key/ESIDrugClmExtr.DrugClmDrug.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# ESINdc (CSeqFileStage) => from output_df_keyndc
df_ESINdc_out = output_df_keyndc.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "NDC_SK",
    "NDC",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "AHFS_TCC_CD",
    "DOSE_FORM",
    "TCC_CD",
    "DSM_DRUG_TYP_CD",
    "NDC_DRUG_ABUSE_CTL_CD",
    "NDC_DRUG_CLS_CD",
    "NDC_DRUG_FORM_CD",
    "NDC_FMT_CD",
    "NDC_GNRC_MNFCTR_CD",
    "NDC_GNRC_NMD_DRUG_CD",
    "NDC_GNRC_PRICE_CD",
    "NDC_GNRC_PRICE_SPREAD_CD",
    "NDC_ORANGE_BOOK_CD",
    "CLM_TRANS_ADD_IN",
    "DESI_DRUG_IN",
    "DRUG_MNTN_IN",
    "INNVTR_IN",
    "INSTUT_PROD_IN",
    "PRIV_LBLR_IN",
    "SNGL_SRC_IN",
    "UNIT_DOSE_IN",
    "UNIT_OF_USE_IN",
    "AVG_WHLSL_PRICE_CHG_DT_SK",
    "GNRC_PRICE_IN_CHG_DT_SK",
    "OBSLT_DT_SK",
    "SRC_NDC_CRT_DT_SK",
    "SRC_NDC_UPDT_DT_SK",
    "BRND_NM",
    "CORE_NINE_NO",
    "DRUG_LABEL_NM",
    "DRUG_STRG_DESC",
    "GCN_CD_TX",
    "GNRC_NM_SH_DESC",
    "LBLR_NM",
    "LBLR_NO",
    "NEEDLE_GAUGE_VAL",
    "NEEDLE_LGTH_VAL",
    "PCKG_DESC",
    "PCKG_SIZE_EQVLNT_NO",
    "PCKG_SIZE_NO",
    "PROD_NO",
    "SYRNG_CPCT_VAL",
    "NDC_RTE_TYP_CD"
)
write_files(
    df_ESINdc_out,
    f"{adls_path}/key/ESIDrugClmExtr.DrugNDC.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# ESIProvDea => from output_df_keydea
df_ESIProvDea_out = output_df_keydea.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "PROV_DEA_SK",
    "DEA_NO",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CMN_PRCT_CD",
    "CLM_TRANS_ADD_IN",
    "EFF_DT_SK",
    "TERM_DT_SK",
    "PROV_NM",
    "ADDR_LN_1",
    "ADDR_LN_2",
    "CITY_NM",
    "PROV_DEA_ST_CD",
    "POSTAL_CD",
    "NTNL_PROV_ID",
    "NTNL_PROV_ID_SPEC_DESC",
    "NTNL_PROV_ID_PROV_TYP_DESC",
    "NTNL_PROV_ID_PROV_CLS_DESC"
)
write_files(
    df_ESIProvDea_out,
    f"{adls_path}/key/ESIDrugClmExtr.DrugProvDea.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# ESIProv => from output_df_keyprov
df_ESIProv_out = output_df_keyprov.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "PROV_SK",
    "PROV_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CMN_PRCT",
    "PROV_BILL_SVC_SK",
    "REL_GRP_PROV",
    "REL_IPA_PROV",
    "PROV_CAP_PAYMT_EFT_METH_CD",
    "PROV_CLM_PAYMT_EFT_METH_CD",
    "PROV_CLM_PAYMT_METH_CD",
    "PROV_ENTY_CD",
    "PROV_FCLTY_TYP_CD",
    "PROV_PRCTC_TYP_CD",
    "PROV_SVC_CAT_CD",
    "PROV_SPEC_CD",
    "PROV_STTUS_CD",
    "PROV_TERM_RSN_CD",
    "PROV_TYP_CD",
    "TERM_DT",
    "PAYMT_HOLD_DT",
    "CLRNGHOUSE_ID",
    "EDI_DEST_ID",
    "EDI_DEST_QUAL",
    "NTNL_PROV_ID",
    "PROV_ADDR_ID",
    "PROV_NM",
    "TAX_ID",
    "TXNMY_CD"
)
write_files(
    df_ESIProv_out,
    f"{adls_path}/key/ESIDrugClmExtr.DrugProv.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# ESIProvLoc => from output_df_keyprovloc
df_ESIProvLoc_out = output_df_keyprovloc.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "PROV_LOC_SK",
    "SRC_SYS_CD_SK",
    "PROV_ID",
    "PROV_ADDR_ID",
    "PROV_ADDR_TYP_CD",
    "PROV_ADDR_EFF_DT",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "PROV_ADDR_SK",
    "PROV_SK",
    "PRI_ADDR_IN",
    "REMIT_ADDR_IN"
)
write_files(
    df_ESIProvLoc_out,
    f"{adls_path}/key/ESIDrugClmExtr.ProvLoc.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)