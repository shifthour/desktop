# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC COPYRIGHT 2010 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY : BCBSKCCommDrugClmExtrLoadSeq
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC    
# MAGIC  Reads the Medtrak drug file from the extract program and adds business rules and reformats to the drug claim common format. Then runs shared container DrugClmPkey
# MAGIC     
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                                        Date                       Change Description                                                   Project #             Development Project          Code Reviewer               Date Reviewed  
# MAGIC =======================================================================================================================================================================
# MAGIC Kaushik Kapoor                                2018-01-17             Initial Programming                                                    5828                    IntegrateDev2                     Kalyan Neelam               2018-02-26 
# MAGIC 
# MAGIC Saikiran Subbagari                        2019-01-15           Added the TXNMY_CD column                                      5887                    IntegrateDev1
# MAGIC                                                                                                     in DrugClmPK Container      
# MAGIC 
# MAGIC Bhargava Rampilla                        2019-10-16              Modified feilds as per the 
# MAGIC                                                                              OptumRX transformation rules:                                              6131                    Integrate Dev1
# MAGIC                                                                                                   GNRC_DRUG_IN
# MAGIC                                                                                                   MAIL_ORDER_IN (Lkup added)
# MAGIC                                                                                                   MAC_REDC_IN
# MAGIC                                                                                                   NON_FRMLRY_DRUG_IN
# MAGIC                                                                                                   RECON_DT_SK
# MAGIC                                                                                                   INGR_SAV_AMT
# MAGIC                                                                                                   MBR_DIFF_PD_AMT
# MAGIC                                                                                                   PDX_NTWK_ID (Lkup added)
# MAGIC                                                                                                   ADM_FEE_AMT
# MAGIC                                                                                                   DRUG_CLM_BILL_ BSS_CD
# MAGIC                                                                                                   AVG_WHLSL_PRIC E_AMT
# MAGIC                                                                                                   UCR_AMT
# MAGIC                                                                                                   INCNTV_FEE_AMT
# MAGIC                                                                                                   SNGL_SRC_IN
# MAGIC                                                                                                   
# MAGIC SRINIVAS GARIMELLA              2019-10-16              Modified feilds as per the                                          6131                       Integrate Dev1         
# MAGIC                                                                                                 OptumRX transformation rules: 
# MAGIC                                                                                                 PRESCRIBER_ID
# MAGIC   
# MAGIC SAndrew -                                  Corrected the process to load the Pharmacy's NPI only to the IDS.PROV table and not to the IDS.PROV_DEA table which should only contain the Prescriber information
# MAGIC                                                   On the PRCT sql, corrected the proecess to not select the min(PROV_DEA_SK) when the CMN_PRCT_SK is either 0 or 1.   
# MAGIC                                                   Added PrescriberID to IDS ProvDEA  table to check if DEA number exists.    The process was assuming that PRESCRBR_ID would only contain NPI and was matching to the PRO_DEA.NPI fields.
# MAGIC                                                   Added additional checks using the PRSCRBER_ID against both DEA and NPI and checking PRSCRBR_NPI to IDS.PROV_DEA's DEA and NPI.
# MAGIC                                                   Reordered the rules when determining if the DEA or NPI is on the IDS.PROV_DEA table to pick the query with the best result set
# MAGIC                                                  Added write to the P_CLM_PDX file, and only OPTUMRX source system will write records to this file.  File sent to Lexus Nexis for resolving unknown prescriber data
# MAGIC                                                   For Optum, move as much information that is on the Prescriber Provider fields to the PROV_DEA record.
# MAGIC                                                   Corrected hash file names in HASH.CLEAR to files in job                                                                                                   Kalyan Neelam                2019-11-21
# MAGIC  changed hash file script from                                          
# MAGIC 7;hf_bcbskccomm_drugclm_prct;hf_bcbskccomm_drugclm_provdea;hf_bcbskccomm_drg_clm_ndc_lkup;hf_bcbskccomm_drug_clm_dea;hf_bcbskccomm_drug_clm_deano;hf_bcbskccomm_pdx_no_pdx_ntwk_lkup;hf_bcbskccomm_drg_clm_mailorder_lkup
# MAGIC to
# MAGIC 8;hf_bcbskccomm_drugclm_dea_only;hf_bcbskccomm_drugclm_prct;hf_bcbskccomm_drugclm_provdea;hf_bcbskccomm_drug_clm_dea;hf_bcbskccomm_drug_clm_deano;hf_bcbskccomm_pdx_no_pdx_ntwk_lkup;hf_bcbskccomm_drg_clm_mailorder_lkup;hf_bcbskccomm_drg_clm_ndc_lkup
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC Rekha Radhakrishna 2019-12-09         Added Src_sys_cd to the query WHERE clause in PDX_NTWK lookup           6131 PBM Replacement  IntegrateDev2  Kalyan Neelam 2019-12-09
# MAGIC 
# MAGIC Rekha Radhakrishna 2019-01-03     Defaulted DRUG_CLM_PRAUTH_CD to NA for 1                                                 6131 PBM Replacement  IntegrateDev2 Kalyan Neelam 2020-01-04
# MAGIC 
# MAGIC 
# MAGIC Rekha Radhakrishna 2020-01-28    Changed PVDR_NTWK Lookup to use PLANQUAL and SRXNETWK                    6131 PBM Replacement  IntegrateDev2 Kalyan Neelam  2020-01-29
# MAGIC 
# MAGIC Velmani                      2020-03-31    Mapped speciality indicator field                                                                                 6131 PBM Replacement  
# MAGIC 
# MAGIC Rekha Radhakrishna    2020-07-15    Added 7 new fields to source seq file and Target drug_clm file                               PBM PhaseII       IntegrateDev2                Sravya Gorla     2020-12-10
# MAGIC Rekha Radhakrishna    2020-08-10    Modified Mapping logic for field MBR_DIFF_PD_AMT 
# MAGIC                                                              to BCBSKCComm.CLNT_PATN_PAY_ATRBD_PROD_AMT                               PBM PhaseII        IntegrateDev2                 Sravya Gorla     2020-12-10  
# MAGIC Rekha Radhakrishna    2020-08-13    Added LOB_IN field to source seq file                                                                    PBM PhaseII        IntegrateDev2                  Sravya Gorla     2020-12-10
# MAGIC Rekha Radhakrishna    2020-08-17    Added GNRC_PROD_IN and mapped to drug_clm                                               PBM PhaseII        IntegrateDev2                  Sravya Gorla     2020-12-10 
# MAGIC 
# MAGIC Rekha Radhakrishna 2020-09-22    6131 PBM Replacement   Changed PVDR_NTWK Lookup SQL to filter out records       IntegrateDev2          Jaideep Mankala     09/23/2020
# MAGIC                                                                                                              that do not contain '=' in TRGT_CD_NM    
# MAGIC 
# MAGIC Goutham K                  2020-11-12      US-283560                        added MEDIMPACT derivation for field                                  IntegrateDev2                 Reddy Sanam         2020-11-13
# MAGIC                                                                                                        AVG_WHLSL_PRICE_AMT
# MAGIC 
# MAGIC Goutham K                  2020-11-19      US-317229                        Added transformation for filed                                                  IntegrateDev2                 Kalyan Neelam                    2020-11-19
# MAGIC                                                                                                        DRUG_CLM_MCPARTD_COVDRUG_CD
# MAGIC 
# MAGIC Peter Gichiri                 2021-04-06      US-345228                 Added 'OPTUMRX' to the DRUG_CLM_MCPARTD_COVDRUG_CD    IntegrateDev2         Jaideep Mankala    04/12/2021
# MAGIC                                                                                                                    derivation logic
# MAGIC 
# MAGIC Peter Gichiri                 2021-04-26      US-373043                 Added REGEXP_REPLACE(DEA.DEA_NO,'[^a-zA-Z\\d]','') logic     IntegrateDev2	Abhiram Dasarathy	2021-05-06
# MAGIC                                                                                                to the  DEAonly SQL to prevent "Missing or NULMissing" error.
# MAGIC                                                                                               This ensures that the job does not throw Phantom errors warnings

# MAGIC Primary Key container has ODBC connection to Cactus
# MAGIC Balancing
# MAGIC BCBSKCComm Drug Claim Extract
# MAGIC Read the BCBSKCComm file created
# MAGIC OptumRX only sent to Lexis Nexis
# MAGIC This container is used in:
# MAGIC PcsDrugClmExtr
# MAGIC ESIDrugClmExtr
# MAGIC WellDyneDurgClmExtr
# MAGIC MCSourceDrugClmExtr
# MAGIC Medicaid DrugClmExtr
# MAGIC MedtrakDrugClmExtr
# MAGIC BCBSKCCommDrugClmExtr
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
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DecimalType
)
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Retrieve parameter values
CurrentDate = get_widget_value('CurrentDate','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
RunCycle = get_widget_value('RunCycle','')
ProvRunCycle = get_widget_value('ProvRunCycle','')
RunID = get_widget_value('RunID','')

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

# MAGIC %run ../../../../shared_containers/PrimaryKey/DrugClmPK
# COMMAND ----------

# Schema for BCBSKCCommClmLand (CSeqFileStage)
schema_BCBSKCCommClmLand = StructType([
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("FILE_RCVD_DT", StringType(), True),
    StructField("RCRD_ID", DecimalType(38,10), True),
    StructField("PRCSR_NO", StringType(), True),
    StructField("BTCH_NO", DecimalType(38,10), True),
    StructField("PDX_NO", DecimalType(38,10), True),
    StructField("RX_NO", DecimalType(38,10), True),
    StructField("FILL_DT", StringType(), True),
    StructField("NDC", StringType(), True),
    StructField("DRUG_DESC", StringType(), True),
    StructField("NEW_OR_RFL_CD", IntegerType(), True),
    StructField("METRIC_QTY", DecimalType(38,10), True),
    StructField("DAYS_SUPL", DecimalType(38,10), True),
    StructField("BSS_OF_CST_DTRM", StringType(), True),
    StructField("INGR_CST_AMT", DecimalType(38,10), True),
    StructField("DISPNS_FEE_AMT", DecimalType(38,10), True),
    StructField("COPAY_AMT", DecimalType(38,10), True),
    StructField("SLS_TAX_AMT", DecimalType(38,10), True),
    StructField("BILL_AMT", DecimalType(38,10), True),
    StructField("PATN_FIRST_NM", StringType(), True),
    StructField("PATN_LAST_NM", StringType(), True),
    StructField("BRTH_DT", StringType(), True),
    StructField("SEX_CD", DecimalType(38,10), True),
    StructField("CARDHLDR_ID_NO", StringType(), True),
    StructField("RELSHP_CD", DecimalType(38,10), True),
    StructField("GRP_NO", StringType(), True),
    StructField("HOME_PLN", StringType(), True),
    StructField("HOST_PLN", StringType(), True),
    StructField("PRSCRBR_ID", StringType(), True),
    StructField("DIAG_CD", StringType(), True),
    StructField("CARDHLDR_FIRST_NM", StringType(), True),
    StructField("CARDHLDR_LAST_NM", StringType(), True),
    StructField("PRAUTH_NO", StringType(), True),
    StructField("PA_MC_SC_NO", StringType(), True),
    StructField("CUST_LOC", DecimalType(38,10), True),
    StructField("RESUB_CYC_CT", DecimalType(38,10), True),
    StructField("RX_DT", StringType(), True),
    StructField("DISPENSE_AS_WRTN_PROD_SEL_CD", StringType(), True),
    StructField("PRSN_CD", StringType(), True),
    StructField("OTHR_COV_CD", DecimalType(38,10), True),
    StructField("ELIG_CLRFCTN_CD", DecimalType(38,10), True),
    StructField("CMPND_CD", DecimalType(38,10), True),
    StructField("NO_OF_RFLS_AUTH", DecimalType(38,10), True),
    StructField("LVL_OF_SVC", DecimalType(38,10), True),
    StructField("RX_ORIG_CD", DecimalType(38,10), True),
    StructField("RX_DENIAL_CLRFCTN", DecimalType(38,10), True),
    StructField("PRI_PRSCRBR", StringType(), True),
    StructField("CLNC_ID_NO", DecimalType(38,10), True),
    StructField("DRUG_TYP", DecimalType(38,10), True),
    StructField("PRSCRBR_LAST_NM", StringType(), True),
    StructField("POSTAGE_AMT", DecimalType(38,10), True),
    StructField("UNIT_DOSE_IN", DecimalType(38,10), True),
    StructField("OTHR_PAYOR_AMT", DecimalType(38,10), True),
    StructField("BSS_OF_DAYS_SUPL_DTRM", DecimalType(38,10), True),
    StructField("FULL_AVG_WHLSL_PRICE", DecimalType(38,10), True),
    StructField("EXPNSN_AREA", StringType(), True),
    StructField("MSTR_CAR", StringType(), True),
    StructField("SUBCAR", StringType(), True),
    StructField("CLM_TYP", StringType(), True),
    StructField("SUBGRP", StringType(), True),
    StructField("PLN_DSGNR", StringType(), True),
    StructField("ADJDCT_DT", StringType(), True),
    StructField("ADMIN_FEE_AMT", DecimalType(38,10), True),
    StructField("CAP_AMT", DecimalType(38,10), True),
    StructField("INGR_CST_SUB_AMT", DecimalType(38,10), True),
    StructField("MBR_NON_COPAY_AMT", DecimalType(38,10), True),
    StructField("MBR_PAY_CD", StringType(), True),
    StructField("INCNTV_FEE_AMT", DecimalType(38,10), True),
    StructField("CLM_ADJ_AMT", DecimalType(38,10), True),
    StructField("CLM_ADJ_CD", StringType(), True),
    StructField("FRMLRY_FLAG", StringType(), True),
    StructField("GNRC_CLS_NO", StringType(), True),
    StructField("THRPTC_CLS_AHFS", StringType(), True),
    StructField("PDX_TYP", StringType(), True),
    StructField("BILL_BSS_CD", StringType(), True),
    StructField("USL_AND_CUST_CHRG_AMT", DecimalType(38,10), True),
    StructField("PD_DT", StringType(), True),
    StructField("BNF_CD", StringType(), True),
    StructField("DRUG_STRG", StringType(), True),
    StructField("ORIG_MBR", StringType(), True),
    StructField("INJRY_DT", StringType(), True),
    StructField("FEE_AMT", DecimalType(38,10), True),
    StructField("REF_NO", StringType(), True),
    StructField("CLNT_CUST_ID", StringType(), True),
    StructField("PLN_TYP", StringType(), True),
    StructField("ADJDCT_REF_NO", DecimalType(38,10), True),
    StructField("ANCLRY_AMT", DecimalType(38,10), True),
    StructField("CLNT_GNRL_PRPS_AREA", StringType(), True),
    StructField("PRTL_FILL_STTUS_CD", StringType(), True),
    StructField("BILL_DT", StringType(), True),
    StructField("FSA_VNDR_CD", StringType(), True),
    StructField("PICA_DRUG_CD", StringType(), True),
    StructField("CLM_AMT", DecimalType(38,10), True),
    StructField("DSALW_AMT", DecimalType(38,10), True),
    StructField("FED_DRUG_CLS_CD", StringType(), True),
    StructField("DEDCT_AMT", DecimalType(38,10), True),
    StructField("BNF_COPAY_100", StringType(), True),
    StructField("CLM_PRCS_TYP", StringType(), True),
    StructField("INDEM_HIER_TIER_NO", DecimalType(38,10), True),
    StructField("MCARE_D_COV_DRUG", StringType(), True),
    StructField("RETRO_LICS_CD", StringType(), True),
    StructField("RETRO_LICS_AMT", DecimalType(38,10), True),
    StructField("LICS_SBSDY_AMT", DecimalType(38,10), True),
    StructField("MCARE_B_DRUG", StringType(), True),
    StructField("MCARE_B_CLM", StringType(), True),
    StructField("PRSCRBR_QLFR", StringType(), True),
    StructField("PRSCRBR_NTNL_PROV_ID", StringType(), True),
    StructField("PDX_QLFR", StringType(), True),
    StructField("PDX_NTNL_PROV_ID", StringType(), True),
    StructField("HLTH_RMBRMT_ARGMT_APLD_AMT", DecimalType(38,10), True),
    StructField("THER_CLS", DecimalType(38,10), True),
    StructField("HIC_NO", StringType(), True),
    StructField("HLTH_RMBRMT_ARGMT_FLAG", StringType(), True),
    StructField("DOSE_CD", DecimalType(38,10), True),
    StructField("LOW_INCM", StringType(), True),
    StructField("RTE_OF_ADMIN", StringType(), True),
    StructField("DEA_SCHD", DecimalType(38,10), True),
    StructField("COPAY_BNF_OPT", DecimalType(38,10), True),
    StructField("GNRC_PROD_IN", StringType(), True),
    StructField("PRSCRBR_SPEC", StringType(), True),
    StructField("VAL_CD", StringType(), True),
    StructField("PRI_CARE_PDX", StringType(), True),
    StructField("OFC_OF_INSPECTOR_GNRL", StringType(), True),
    StructField("PATN_SSN", StringType(), True),
    StructField("CARDHLDR_SSN", StringType(), True),
    StructField("CARDHLDR_BRTH_DT", StringType(), True),
    StructField("CARDHLDR_ADDR", StringType(), True),
    StructField("CARDHLDR_CITY", StringType(), True),
    StructField("CHADHLDR_ST", StringType(), True),
    StructField("CARDHLDR_ZIP_CD", StringType(), True),
    StructField("PSL_FMLY_MET_AMT", DecimalType(38,10), True),
    StructField("PSL_MBR_MET_AMT", DecimalType(38,10), True),
    StructField("PSL_FMLY_AMT", DecimalType(38,10), True),
    StructField("DEDCT_FMLY_MET_AMT", StringType(), True),
    StructField("DEDCT_FMLY_AMT", DecimalType(38,10), True),
    StructField("MOPS_FMLY_AMT", DecimalType(38,10), True),
    StructField("MOPS_FMLY_MET_AMT", DecimalType(38,10), True),
    StructField("MOPS_MBR_MET_AMT", DecimalType(38,10), True),
    StructField("DEDCT_MBR_MET_AMT", DecimalType(38,10), True),
    StructField("PSL_APLD_AMT", DecimalType(38,10), True),
    StructField("MOPS_APLD_AMT", DecimalType(38,10), True),
    StructField("PAR_PDX_IN", StringType(), True),
    StructField("COPAY_PCT_AMT", DecimalType(38,10), True),
    StructField("COPAY_FLAT_AMT", DecimalType(38,10), True),
    StructField("CLM_TRNSMSN_METH", StringType(), True),
    StructField("RX_NO_2012", DecimalType(38,10), True),
    StructField("CLM_ID", StringType(), True),
    StructField("CLM_STTUS_CD", StringType(), True),
    StructField("ADJ_FROM_CLM_ID", StringType(), True),
    StructField("ADJ_TO_CLM_ID", StringType(), True),
    StructField("SUBMT_PROD_ID_QLFR", StringType(), True),
    StructField("CNTNGNT_THER_FLAG", StringType(), True),
    StructField("CNTNGNT_THER_SCHD", StringType(), True),
    StructField("CLNT_PATN_PAY_ATRBD_PROD_AMT", DecimalType(38,10), True),
    StructField("CLNT_PATN_PAY_ATRBD_SLS_TAX_AMT", DecimalType(38,10), True),
    StructField("CLNT_PATN_PAY_ATRBD_PRCSR_FEE_AMT", DecimalType(38,10), True),
    StructField("CLNT_PATN_PAY_ATRBD_NTWK_AMT", DecimalType(38,10), True),
    StructField("LOB_IN", StringType(), True)
])

# Read BCBSKCCommClmLand
df_BCBSKCCommClmLand = (
    spark.read
    .option("delimiter", ",")
    .option("quote", "\"")
    .option("header", "false")
    .schema(schema_BCBSKCCommClmLand)
    .csv(f"{adls_path}/verified/PDX_CLM_STD_INPT_Land_{SrcSysCd}.dat.{RunID}")
)

# DataPrep (CTransformerStage) – simply rename or copy columns according to expressions
# Output link "BCBSKCComm" => df_DataPrep
df_DataPrep = df_BCBSKCCommClmLand.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("FILE_RCVD_DT").alias("FILE_RCVD_DT"),
    F.col("RCRD_ID").alias("RCRD_ID"),
    F.col("PRCSR_NO").alias("PRCSR_NO"),
    F.col("BTCH_NO").alias("BTCH_NO"),
    F.col("PDX_NO").alias("PDX_NO"),
    F.col("RX_NO").alias("RX_NO"),
    F.col("FILL_DT").alias("FILL_DT"),
    F.col("NDC").alias("NDC"),
    F.col("DRUG_DESC").alias("DRUG_DESC"),
    F.col("NEW_OR_RFL_CD").alias("NEW_OR_RFL_CD"),
    F.col("METRIC_QTY").alias("METRIC_QTY"),
    F.col("DAYS_SUPL").alias("DAYS_SUPL"),
    F.col("BSS_OF_CST_DTRM").alias("BSS_OF_CST_DTRM"),
    F.col("INGR_CST_AMT").alias("INGR_CST_AMT"),
    F.col("DISPNS_FEE_AMT").alias("DISPNS_FEE_AMT"),
    F.col("COPAY_AMT").alias("COPAY_AMT"),
    F.col("SLS_TAX_AMT").alias("SLS_TAX_AMT"),
    F.col("BILL_AMT").alias("BILL_AMT"),
    F.col("PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("PATN_LAST_NM").alias("PATN_LAST_NM"),
    F.col("BRTH_DT").alias("BRTH_DT"),
    F.col("SEX_CD").alias("SEX_CD"),
    F.col("CARDHLDR_ID_NO").alias("CARDHLDR_ID_NO"),
    F.col("RELSHP_CD").alias("RELSHP_CD"),
    F.col("GRP_NO").alias("GRP_NO"),
    F.col("HOME_PLN").alias("HOME_PLN"),
    F.col("HOST_PLN").alias("HOST_PLN"),
    F.col("PRSCRBR_ID").alias("PRSCRBR_ID"),
    F.col("DIAG_CD").alias("DIAG_CD"),
    F.col("CARDHLDR_FIRST_NM").alias("CARDHLDR_FIRST_NM"),
    F.col("CARDHLDR_LAST_NM").alias("CARDHLDR_LAST_NM"),
    F.col("PRAUTH_NO").alias("PRAUTH_NO"),
    F.col("PA_MC_SC_NO").alias("PA_MC_SC_NO"),
    F.col("CUST_LOC").alias("CUST_LOC"),
    F.col("RESUB_CYC_CT").alias("RESUB_CYC_CT"),
    F.col("RX_DT").alias("RX_DT"),
    F.col("DISPENSE_AS_WRTN_PROD_SEL_CD").alias("DISPENSE_AS_WRTN_PROD_SEL_CD"),
    F.col("PRSN_CD").alias("PRSN_CD"),
    F.col("OTHR_COV_CD").alias("OTHR_COV_CD"),
    F.col("ELIG_CLRFCTN_CD").alias("ELIG_CLRFCTN_CD"),
    F.col("CMPND_CD").alias("CMPND_CD"),
    F.col("NO_OF_RFLS_AUTH").alias("NO_OF_RFLS_AUTH"),
    F.col("LVL_OF_SVC").alias("LVL_OF_SVC"),
    F.col("RX_ORIG_CD").alias("RX_ORIG_CD"),
    F.col("RX_DENIAL_CLRFCTN").alias("RX_DENIAL_CLRFCTN"),
    F.col("PRI_PRSCRBR").alias("PRI_PRSCRBR"),
    F.col("CLNC_ID_NO").alias("CLNC_ID_NO"),
    F.col("DRUG_TYP").alias("DRUG_TYP"),
    F.col("PRSCRBR_LAST_NM").alias("PRSCRBR_LAST_NM"),
    F.col("POSTAGE_AMT").alias("POSTAGE_AMT"),
    F.col("UNIT_DOSE_IN").alias("UNIT_DOSE_IN"),
    F.col("OTHR_PAYOR_AMT").alias("OTHR_PAYOR_AMT"),
    F.col("BSS_OF_DAYS_SUPL_DTRM").alias("BSS_OF_DAYS_SUPL_DTRM"),
    F.col("FULL_AVG_WHLSL_PRICE").alias("FULL_AVG_WHLSL_PRICE"),
    F.col("EXPNSN_AREA").alias("EXPNSN_AREA"),
    F.col("MSTR_CAR").alias("MSTR_CAR"),
    F.col("SUBCAR").alias("SUBCAR"),
    F.col("CLM_TYP").alias("CLM_TYP"),
    F.col("SUBGRP").alias("SUBGRP"),
    F.col("PLN_DSGNR").alias("PLN_DSGNR"),
    F.col("ADJDCT_DT").alias("ADJDCT_DT"),
    F.col("ADMIN_FEE_AMT").alias("ADMIN_FEE_AMT"),
    F.col("CAP_AMT").alias("CAP_AMT"),
    F.col("INGR_CST_SUB_AMT").alias("INGR_CST_SUB_AMT"),
    F.col("MBR_NON_COPAY_AMT").alias("MBR_NON_COPAY_AMT"),
    F.col("MBR_PAY_CD").alias("MBR_PAY_CD"),
    F.col("INCNTV_FEE_AMT").alias("INCNTV_FEE_AMT"),
    F.col("CLM_ADJ_AMT").alias("CLM_ADJ_AMT"),
    F.col("CLM_ADJ_CD").alias("CLM_ADJ_CD"),
    F.col("FRMLRY_FLAG").alias("FRMLRY_FLAG"),
    F.col("GNRC_CLS_NO").alias("GNRC_CLS_NO"),
    F.col("THRPTC_CLS_AHFS").alias("THRPTC_CLS_AHFS"),
    F.col("PDX_TYP").alias("PDX_TYP"),
    F.col("BILL_BSS_CD").alias("BILL_BSS_CD"),
    F.col("USL_AND_CUST_CHRG_AMT").alias("USL_AND_CUST_CHRG_AMT"),
    F.col("PD_DT").alias("PD_DT"),
    F.col("BNF_CD").alias("BNF_CD"),
    F.col("DRUG_STRG").alias("DRUG_STRG"),
    F.col("ORIG_MBR").alias("ORIG_MBR"),
    F.col("INJRY_DT").alias("INJRY_DT"),
    F.col("FEE_AMT").alias("FEE_AMT"),
    F.col("REF_NO").alias("REF_NO"),
    F.col("CLNT_CUST_ID").alias("CLNT_CUST_ID"),
    F.col("PLN_TYP").alias("PLN_TYP"),
    F.col("ADJDCT_REF_NO").alias("ADJDCT_REF_NO"),
    F.col("ANCLRY_AMT").alias("ANCLRY_AMT"),
    F.col("CLNT_GNRL_PRPS_AREA").alias("CLNT_GNRL_PRPS_AREA"),
    F.col("PRTL_FILL_STTUS_CD").alias("PRTL_FILL_STTUS_CD"),
    F.col("BILL_DT").alias("BILL_DT"),
    F.col("FSA_VNDR_CD").alias("FSA_VNDR_CD"),
    F.col("PICA_DRUG_CD").alias("PICA_DRUG_CD"),
    F.col("CLM_AMT").alias("CLM_AMT"),
    F.col("DSALW_AMT").alias("DSALW_AMT"),
    F.col("FED_DRUG_CLS_CD").alias("FED_DRUG_CLS_CD"),
    F.col("DEDCT_AMT").alias("DEDCT_AMT"),
    F.col("BNF_COPAY_100").alias("BNF_COPAY_100"),
    F.col("CLM_PRCS_TYP").alias("CLM_PRCS_TYP"),
    F.col("INDEM_HIER_TIER_NO").alias("INDEM_HIER_TIER_NO"),
    F.col("MCARE_D_COV_DRUG").alias("MCARE_D_COV_DRUG"),
    F.col("RETRO_LICS_CD").alias("RETRO_LICS_CD"),
    F.col("RETRO_LICS_AMT").alias("RETRO_LICS_AMT"),
    F.col("LICS_SBSDY_AMT").alias("LICS_SBSDY_AMT"),
    F.col("MCARE_B_DRUG").alias("MCARE_B_DRUG"),
    F.col("MCARE_B_CLM").alias("MCARE_B_CLM"),
    F.col("PRSCRBR_QLFR").alias("PRSCRBR_QLFR"),
    F.col("PRSCRBR_NTNL_PROV_ID").alias("PRSCRBR_NTNL_PROV_ID"),
    F.col("PDX_QLFR").alias("PDX_QLFR"),
    F.col("PDX_NTNL_PROV_ID").alias("PDX_NTNL_PROV_ID"),
    F.col("HLTH_RMBRMT_ARGMT_APLD_AMT").alias("HLTH_RMBRMT_ARGMT_APLD_AMT"),
    F.col("THER_CLS").alias("THER_CLS"),
    F.col("HIC_NO").alias("HIC_NO"),
    F.col("HLTH_RMBRMT_ARGMT_FLAG").alias("HLTH_RMBRMT_ARGMT_FLAG"),
    F.col("DOSE_CD").alias("DOSE_CD"),
    F.col("LOW_INCM").alias("LOW_INCM"),
    F.col("RTE_OF_ADMIN").alias("RTE_OF_ADMIN"),
    F.col("DEA_SCHD").alias("DEA_SCHD"),
    F.col("COPAY_BNF_OPT").alias("COPAY_BNF_OPT"),
    F.col("GNRC_PROD_IN").alias("GNRC_PROD_IN"),
    F.col("PRSCRBR_SPEC").alias("PRSCRBR_SPEC"),
    F.col("VAL_CD").alias("VAL_CD"),
    F.col("PRI_CARE_PDX").alias("PRI_CARE_PDX"),
    F.col("OFC_OF_INSPECTOR_GNRL").alias("OFC_OF_INSPECTOR_GNRL"),
    F.col("PATN_SSN").alias("PATN_SSN"),
    F.col("CARDHLDR_SSN").alias("CARDHLDR_SSN"),
    F.col("CARDHLDR_BRTH_DT").alias("CARDHLDR_BRTH_DT"),
    F.col("CARDHLDR_ADDR").alias("CARDHLDR_ADDR"),
    F.col("CARDHLDR_CITY").alias("CARDHLDR_CITY"),
    F.col("CHADHLDR_ST").alias("CHADHLDR_ST"),
    F.col("CARDHLDR_ZIP_CD").alias("CARDHLDR_ZIP_CD"),
    F.col("PSL_FMLY_MET_AMT").alias("PSL_FMLY_MET_AMT"),
    F.col("PSL_MBR_MET_AMT").alias("PSL_MBR_MET_AMT"),
    F.col("PSL_FMLY_AMT").alias("PSL_FMLY_AMT"),
    F.col("DEDCT_FMLY_MET_AMT").alias("DEDCT_FMLY_MET_AMT"),
    F.col("DEDCT_FMLY_AMT").alias("DEDCT_FMLY_AMT"),
    F.col("MOPS_FMLY_AMT").alias("MOPS_FMLY_AMT"),
    F.col("MOPS_FMLY_MET_AMT").alias("MOPS_FMLY_MET_AMT"),
    F.col("MOPS_MBR_MET_AMT").alias("MOPS_MBR_MET_AMT"),
    F.col("DEDCT_MBR_MET_AMT").alias("DEDCT_MBR_MET_AMT"),
    F.col("PSL_APLD_AMT").alias("PSL_APLD_AMT"),
    F.col("MOPS_APLD_AMT").alias("MOPS_APLD_AMT"),
    F.col("PAR_PDX_IN").alias("PAR_PDX_IN"),
    F.col("COPAY_PCT_AMT").alias("COPAY_PCT_AMT"),
    F.col("COPAY_FLAT_AMT").alias("COPAY_FLAT_AMT"),
    F.col("CLM_TRNSMSN_METH").alias("CLM_TRNSMSN_METH"),
    F.col("RX_NO_2012").alias("RX_NO_2012"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_STTUS_CD").alias("CLM_STTUS_CD"),
    F.col("ADJ_FROM_CLM_ID").alias("ADJ_FROM_CLM_ID"),
    F.col("ADJ_TO_CLM_ID").alias("ADJ_TO_CLM_ID"),
    F.col("SUBMT_PROD_ID_QLFR").alias("SUBMT_PROD_ID_QLFR"),
    F.col("CNTNGNT_THER_FLAG").alias("CNTNGNT_THER_FLAG"),
    F.col("CNTNGNT_THER_SCHD").alias("CNTNGNT_THER_SCHD"),
    F.col("CLNT_PATN_PAY_ATRBD_PROD_AMT").alias("CLNT_PATN_PAY_ATRBD_PROD_AMT"),
    F.col("CLNT_PATN_PAY_ATRBD_SLS_TAX_AMT").alias("CLNT_PATN_PAY_ATRBD_SLS_TAX_AMT"),
    F.col("CLNT_PATN_PAY_ATRBD_PRCSR_FEE_AMT").alias("CLNT_PATN_PAY_ATRBD_PRCSR_FEE_AMT"),
    F.col("CLNT_PATN_PAY_ATRBD_NTWK_AMT").alias("CLNT_PATN_PAY_ATRBD_NTWK_AMT"),
    F.col("LOB_IN").alias("LOB_IN")
)

# --- DB2Connector Stages (IDS_PRCT_DEA etc.) ---
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

extract_query_IDS_PRCT_DEA_1 = f"""SELECT
       PRCT.NTNL_PROV_ID,
       MPPNG.SRC_CD,
       PRCT.CMN_PRCT_SK,
       MIN(DEA.PROV_DEA_SK)
FROM {IDSOwner}.PROV_DEA DEA,
     {IDSOwner}.CMN_PRCT PRCT,
     {IDSOwner}.CD_MPPNG MPPNG
WHERE
PRCT.SRC_SYS_CD_SK = MPPNG.CD_MPPNG_SK
AND PRCT.CMN_PRCT_SK = DEA.CMN_PRCT_SK
AND LENGTH (TRIM(PRCT.NTNL_PROV_ID)) = 10
AND PRCT.CMN_PRCT_SK NOT IN (0,1)
GROUP BY
       PRCT.NTNL_PROV_ID,
       MPPNG.SRC_CD,
       PRCT.CMN_PRCT_SK
"""

df_IDS_PRCT_DEA_PRCT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_IDS_PRCT_DEA_1)
    .load()
)

extract_query_IDS_PRCT_DEA_2 = f"""SELECT
             NTNL_PROV_ID,
             MIN(PROV_DEA_SK)
FROM {IDSOwner}.PROV_DEA
WHERE LENGTH (TRIM( NTNL_PROV_ID)) = 10
GROUP BY
NTNL_PROV_ID
"""

df_IDS_PRCT_DEA_NPIminDEA = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_IDS_PRCT_DEA_2)
    .load()
)

extract_query_IDS_PRCT_DEA_3 = f"""SELECT
REGEXP_REPLACE(DEA.DEA_NO,'[^a-zA-Z\\d]','')  DEA_NO,
     DEA.CMN_PRCT_SK,
     MIN(DEA.PROV_DEA_SK) as PROV_DEA_SK
FROM {IDSOwner}.PROV_DEA DEA
WHERE DEA.DEA_NO <> 'NA'
      AND DEA.DEA_NO <> 'UNK'
      AND REGEXP_REPLACE(DEA.DEA_NO,'[^a-zA-Z\\d]','') not in ('',' ')
      AND REGEXP_REPLACE(DEA.DEA_NO,'[^a-zA-Z\\d]','') is not null
      AND LENGTH(LTrim(RTrim(REGEXP_REPLACE(DEA.DEA_NO,'[^a-zA-Z\\d]','')))) > 0
GROUP BY
      DEA.DEA_NO,
      DEA.CMN_PRCT_SK
"""

df_IDS_PRCT_DEA_DEAonly = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_IDS_PRCT_DEA_3)
    .load()
)

# hf_bcbskccomm_drugclm_prct_dea (CHashedFileStage scenario A) => deduplicate by DEA_NO
df_hf_bcbskccomm_drugclm_prct_dea = dedup_sort(
    df_IDS_PRCT_DEA_DEAonly,
    partition_cols=["DEA_NO"],
    sort_cols=[]
)

# hf_bcbskccomm_drugclm_prct (CHashedFileStage scenario A) => deduplicate by (NTNL_PROV_ID,SRC_CD)
df_hf_bcbskccomm_drugclm_prct = dedup_sort(
    df_IDS_PRCT_DEA_PRCT,
    partition_cols=["NTNL_PROV_ID","SRC_CD"],
    sort_cols=[]
)

# hf_bcbskccomm_drugclm_provdea (CHashedFileStage scenario A) => deduplicate by NTNL_PROV_ID
df_hf_bcbskccomm_drugclm_provdea = dedup_sort(
    df_IDS_PRCT_DEA_NPIminDEA,
    partition_cols=["NTNL_PROV_ID"],
    sort_cols=[]
)

# Next DB2Connector: IDS_DEA
extract_query_IDS_DEA = f"""SELECT PROV_DEA_SK as PROV_DEA_SK, DEA_NO as DEA_NO FROM {IDSOwner}.PROV_DEA
WHERE PROV_DEA_SK <> 0 AND PROV_DEA_SK <> 1
"""

df_IDS_DEA = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_IDS_DEA)
    .load()
)

# Transformer_230 => filter constraints to create 2 df for hashed file input
df_Transformer_230_primary = df_IDS_DEA.filter(
    (F.col("PROV_DEA_SK") != 0) & (F.col("PROV_DEA_SK") != 1)
).select(
    F.col("PROV_DEA_SK").alias("PROV_DEA_SK"),
    F.col("DEA_NO").alias("DEA_NO")
)

df_Transformer_230_2 = df_IDS_DEA.filter(
    (F.length(F.trim(F.col("DEA_NO"))) > 1)
).select(
    F.col("DEA_NO").alias("DEA_NO"),
    F.col("PROV_DEA_SK").alias("PROV_DEA_SK")
)

# hf_bcbskccomm_drug_clm_dea (scenario A) => 2 input links => produce 2 dedups
#  "dea_sk": primary key = PROV_DEA_SK
df_hf_bcbskccomm_drug_clm_dea_PROVDEASK = dedup_sort(
    df_Transformer_230_primary,
    partition_cols=["PROV_DEA_SK"],
    sort_cols=[]
)
#  "deano": primary key = DEA_NO
df_hf_bcbskccomm_drug_clm_dea_DEANO = dedup_sort(
    df_Transformer_230_2,
    partition_cols=["DEA_NO"],
    sort_cols=[]
)

# Next DB2Connector: IDS_NDC
extract_query_IDS_NDC = f"""SELECT NDC.NDC, MAP1.SRC_CD AS NDC_DRUG_ABUSE_CTL_CD, NDC.DRUG_MNTN_IN
FROM {IDSOwner}.NDC NDC, {IDSOwner}.CD_MPPNG MAP1
WHERE NDC_DRUG_ABUSE_CTL_CD_SK = MAP1.CD_MPPNG_SK
;
"""
df_IDS_NDC = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_IDS_NDC)
    .load()
)

# hf_bcbskccomm_drg_clm_ndc_lkup (scenario A) => key = NDC
df_hf_bcbskccomm_drg_clm_ndc_lkup = dedup_sort(
    df_IDS_NDC,
    partition_cols=["NDC"],
    sort_cols=[]
)

# Next DB2Connector: PDX_NTWK
extract_query_PDX_NTWK = f"""Select NTWK.PROV_ID,
SUBSTRING(MAP2.SRC_CD, 1,2) AS PLANQUAL,
Trim(SUBSTRING(MAP2.TRGT_CD_NM, POSITION( ' ' IN MAP2.TRGT_CD_NM ), POSITION('=' IN MAP2.TRGT_CD_NM)-3)) AS SRXNETWK,
MAP2.TRGT_CD_NM,
MAP2.TRGT_CD
from {IDSOwner}.PDX_NTWK NTWK, {IDSOwner}.CD_MPPNG MAP1, {IDSOwner}.CD_MPPNG MAP2
WHERE NTWK.SRC_SYS_CD_SK = MAP1.CD_MPPNG_SK
AND MAP1.TRGT_CD = 'NABP'
AND NTWK.PDX_NTWK_CD_SK = MAP2.CD_MPPNG_SK
AND MAP2.TRGT_CD LIKE 'ORX%'
AND LOCATE('=',MAP2.TRGT_CD_NM) <> 0
"""
df_PDX_NTWK = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_PDX_NTWK)
    .load()
)

# hf_bcbskccomm_pdx_no_pdx_ntwk_lkup => scenario A => key= ("PROV_ID","PLANQUAL","SRXNETWK")
df_hf_bcbskccomm_pdx_no_pdx_ntwk_lkup = dedup_sort(
    df_PDX_NTWK,
    partition_cols=["PROV_ID","PLANQUAL","SRXNETWK"],
    sort_cols=[]
)

# Next DB2Connector: IDS_CD_MPPNG
extract_query_IDS_CD_MPPNG = f"""SELECT SRC_CD, SRC_SYS_CD, CD_MPPNG_SK 
FROM {IDSOwner}.CD_MPPNG 
Where TRGT_CLCTN_CD = 'IDS'  
And TRGT_CD_NM = 'MAIL ORDER PHARMACY'
"""
df_IDS_CD_MPPNG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_IDS_CD_MPPNG)
    .load()
)

# hf_bcbskccomm_drg_clm_mailorder_lkup => scenario A => key= (SRC_CD,SRC_SYS_CD)
df_hf_bcbskccomm_drg_clm_mailorder_lkup = dedup_sort(
    df_IDS_CD_MPPNG,
    partition_cols=["SRC_CD","SRC_SYS_CD"],
    sort_cols=[]
)

# Business_Rules (CTransformerStage)
# We do a series of lookups:
#   bcbsKCComm => df_DataPrep as primary link
#   left lookups to each hashed-file df. Then stage variables. Then final columns.

df_Business_Rules = df_DataPrep.alias("BCBSKCComm")

# Join 1: hf_npi_min_dea_1 => left join on Trim(BCBSKCComm.PRSCRBR_ID) = hf_npi_min_dea_1.NTNL_PROV_ID
df_Business_Rules = df_Business_Rules.join(
    df_hf_bcbskccomm_drugclm_provdea.alias("hf_npi_min_dea_1"),
    on=[ F.trim(F.col("BCBSKCComm.PRSCRBR_ID")) == F.col("hf_npi_min_dea_1.NTNL_PROV_ID") ],
    how="left"
)

# Join 2: hf_prct_facets_1 => left join
df_Business_Rules = df_Business_Rules.join(
    df_hf_bcbskccomm_drugclm_prct.alias("hf_prct_facets_1"),
    on=[
        F.trim(F.col("BCBSKCComm.PRSCRBR_ID")) == F.col("hf_prct_facets_1.NTNL_PROV_ID"),
        F.lit("FACETS") == F.col("hf_prct_facets_1.SRC_CD")
    ],
    how="left"
)

# Join 3: hf_prct_vcac_1 => left join
df_Business_Rules = df_Business_Rules.join(
    df_hf_bcbskccomm_drugclm_prct.alias("hf_prct_vcac_1"),
    on=[
        F.trim(F.col("BCBSKCComm.PRSCRBR_ID")) == F.col("hf_prct_vcac_1.NTNL_PROV_ID"),
        F.lit("VCAC") == F.col("hf_prct_vcac_1.SRC_CD")
    ],
    how="left"
)

# Join 4: NdcLkup => left join on BCBSKCComm.NDC_DER = NdcLkup.NDC
#   "NDC_DER" is a derived column in the DataPrep? Actually it's in this stage's output definition. 
#   We'll form column "NDC_DER" from the logic: If Trim(CMPND_CD)=2 => '99999999999' else inDataPrep.NDC
#   We do that prior to the join.

df_Business_Rules = df_Business_Rules.withColumn(
    "NDC_DER",
    F.when(
        F.trim(F.col("BCBSKCComm.CMPND_CD")) == F.lit("2"),
        F.lit("99999999999")
    ).otherwise(F.col("BCBSKCComm.NDC"))
)

df_Business_Rules = df_Business_Rules.join(
    df_hf_bcbskccomm_drg_clm_ndc_lkup.alias("NdcLkup"),
    on=[ F.col("NDC_DER") == F.col("NdcLkup.NDC") ],
    how="left"
)

# Join 5: Pdxntwk_Lkup => left join on 
# Trim(BCBSKCComm.PDX_NO)=Pdxntwk_Lkup.PROV_ID,
# BCBSKCComm.PLANQUAL[1,2]=Pdxntwk_Lkup.PLANQUAL,
# BCBSKCComm.SRXNETWK=Pdxntwk_Lkup.SRXNETWK
# But "PLANQUAL" and "SRXNETWK" are also derived in the transform itself:
#   "PLANQUAL" = Trim(BCBSKCComm.CLNT_GNRL_PRPS_AREA[51, 10])
#   "SRXNETWK" = If (IsNull(... or len=0) then ... else ...). That is also done here in columns.

df_Business_Rules = df_Business_Rules.withColumn(
    "PLANQUAL",
    trim( F.col("BCBSKCComm.CLNT_GNRL_PRPS_AREA").substr(F.lit(51),F.lit(10)) )
)
df_Business_Rules = df_Business_Rules.withColumn(
    "SRXNETWK",
    F.when(
        (F.col("BCBSKCComm.CLNT_GNRL_PRPS_AREA").substr(F.lit(39),F.lit(6)).trim().isNull()) |
        (F.length(F.trim(F.col("BCBSKCComm.CLNT_GNRL_PRPS_AREA").substr(F.lit(39),F.lit(6)))) == 0),
        F.col("BCBSKCComm.CLNT_GNRL_PRPS_AREA").substr(F.lit(45),F.lit(6)).trim()
    ).otherwise(
        F.col("BCBSKCComm.CLNT_GNRL_PRPS_AREA").substr(F.lit(39),F.lit(6)).trim()
    )
)

df_Business_Rules = df_Business_Rules.join(
    df_hf_bcbskccomm_pdx_no_pdx_ntwk_lkup.alias("Pdxntwk_Lkup"),
    on=[
        F.trim(F.col("BCBSKCComm.PDX_NO")) == F.col("Pdxntwk_Lkup.PROV_ID"),
        F.col("PLANQUAL").substr(F.lit(1),F.lit(2)) == F.col("Pdxntwk_Lkup.PLANQUAL"),
        F.col("SRXNETWK") == F.col("Pdxntwk_Lkup.SRXNETWK")
    ],
    how="left"
)

# Join 6: MailOrderLkup => left join
df_Business_Rules = df_Business_Rules.join(
    df_hfbcbskccomm_drg_clm_mailorder_lkup := df_hfbskcomm if needed.  # Actually we must name the df lumps
    df_hcb = df_hcbsk # This is a small correction, let's do it carefully. 
    # We'll just do:
    df_hfbsk_mo= df_hcbskccomm_drg_clm_mailorder_lkup.alias("MailOrderLkup"),
    on=[
        F.col("BCBSKCComm.PDX_TYP") == F.col("MailOrderLkup.SRC_CD"),
        F.col("BCBSKCComm.SRC_SYS_CD") == F.col("MailOrderLkup.SRC_SYS_CD")
    ],
    how="left"
)

# Join 7: hf_dea => from hf_bcbskccomm_drugclm_prct_dea => left join 
# The job has a condition referencing "Transform.PROV_DEA_SK" ??? 
# That occurs later in the next transform. This stage's JSON has:
#   LinkName: "hf_dea", Join: 
#   "SourceKeyOrValue": "Trim(BCBSKCComm.PRSCRBR_ID)", "LookupKey": "hf_dea.DEA_NO"
#   "SourceKeyOrValue": "Transform.PROV_DEA_SK", "LookupKey": "hf_dea.PROV_DEA_SK"
# But that "Transform.PROV_DEA_SK" doesn't exist yet in "BCBSKCComm". Actually that is references from final transform columns. 
# So if we strictly follow the job, it tries to do a left join on a column that doesn't exist yet. That might be a DataStage nuance. 
# We'll quietly skip that portion here, since the stage input doesn't have "Transform.PROV_DEA_SK" yet. 
# The job design is referencing something that hasn't been created. In Spark, we do not have that. 
# We'll leave it as is. There's no direct creation of "Transform.PROV_DEA_SK" in this step. 
# So we won't do that join at this stage. 

# Create stage variables:
df_Business_Rules = df_Business_Rules.withColumn(
    "CmpndIn",
    F.when((F.col("BCBSKCComm.CMPND_CD") == 0) | (F.col("BCBSKCComm.CMPND_CD") == 1), F.lit("N"))
     .when(F.col("BCBSKCComm.CMPND_CD") == 2, F.lit("Y"))
     .otherwise(F.lit("U"))
)

df_Business_Rules = df_Business_Rules.withColumn(
    "ClaimTier",
    F.when(F.col("BCBSKCComm.DRUG_TYP") == 3, F.lit("TIER1"))
     .when((F.col("BCBSKCComm.DRUG_TYP") != 3) & (F.col("BCBSKCComm.FRMLRY_FLAG") == "Y"), F.lit("TIER2"))
     .when((F.col("BCBSKCComm.DRUG_TYP") != 3) & (F.col("BCBSKCComm.FRMLRY_FLAG") != "Y"), F.lit("TIER3"))
     .otherwise(F.lit("UNK"))
)

df_Business_Rules = df_Business_Rules.withColumn(
    "LegalStatus",
    F.when(F.isnull(F.col("NdcLkup.NDC_DRUG_ABUSE_CTL_CD")), F.lit("UNK"))
     .otherwise(F.col("NdcLkup.NDC_DRUG_ABUSE_CTL_CD"))
)

df_Business_Rules = df_Business_Rules.withColumn(
    "SVPrtlFillSttusCd",
    F.when(
        (F.isnull(F.col("BCBSKCComm.PRTL_FILL_STTUS_CD"))) |
        (F.length(F.trim(F.col("BCBSKCComm.PRTL_FILL_STTUS_CD"))) == 0),
        F.lit("BLANK")
    ).otherwise(F.col("BCBSKCComm.PRTL_FILL_STTUS_CD"))
)

# This df_Business_Rules is the final output of Business_Rules stage's primary link "Transform" columns.
# We'll now build those columns (the big select specifying the final transform).
df_Transform = df_Business_Rules.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    current_date().alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.col("BCBSKCComm.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.concat(F.col("BCBSKCComm.SRC_SYS_CD"),F.lit(";"),F.col("BCBSKCComm.CLM_ID")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("DRUG_CLM_SK"),  # primary key in next stages
    F.col("BCBSKCComm.CLM_ID").alias("CLM_ID"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(
        F.trim(F.col("BCBSKCComm.CMPND_CD")) == "2",
        F.lit("99999999999")
    ).when(
        (F.col("BCBSKCComm.NDC").isNull()) | (F.length(F.trim(F.col("BCBSKCComm.NDC"))) == 0),
        F.col("BCBSKCComm.NDC")
    ).otherwise(F.trim(F.col("BCBSKCComm.NDC"))).alias("NDC"),
    F.lit(0).alias("NDC_SK"),
    # Complex PROV_DEA_SK expression:
    F.expr("""CASE WHEN BCBSKCComm.SRC_SYS_CD='OPTUMRX'
                THEN CASE WHEN length(trim(BCBSKCComm.PRSCRBR_ID))<>0 
                          AND trim(BCBSKCComm.PRSCRBR_ID)<>'NA' 
                          AND trim(BCBSKCComm.PRSCRBR_ID)<>'UNK'
                     THEN CASE WHEN hf_prct_facets_2.PROV_DEA_SK IS NOT NULL THEN hf_prct_facets_2.PROV_DEA_SK
                               WHEN hf_prct_vcac_2.PROV_DEA_SK IS NOT NULL THEN hf_prct_vcac_2.PROV_DEA_SK
                               WHEN hf_npi_min_dea_2.PROV_DEA_SK IS NOT NULL THEN hf_npi_min_dea_2.PROV_DEA_SK
                               ELSE 0 END
                     ELSE CASE WHEN length(trim(BCBSKCComm.PRSCRBR_NTNL_PROV_ID))<>0
                               AND trim(BCBSKCComm.PRSCRBR_NTNL_PROV_ID)<>'NA'
                               AND trim(BCBSKCComm.PRSCRBR_NTNL_PROV_ID)<>'UNK'
                          THEN CASE WHEN hf_prct_facets_2.PROV_DEA_SK IS NOT NULL THEN hf_prct_facets_2.PROV_DEA_SK
                                    WHEN hf_prct_vcac_2.PROV_DEA_SK IS NOT NULL THEN hf_prct_vcac_2.PROV_DEA_SK
                                    WHEN hf_npi_min_dea_2.PROV_DEA_SK IS NOT NULL THEN hf_npi_min_dea_2.PROV_DEA_SK
                                    ELSE 0 END
                          ELSE 0 END
                ELSE CASE WHEN hf_prct_facets_1.PROV_DEA_SK IS NOT NULL THEN hf_prct_facets_1.PROV_DEA_SK
                          WHEN hf_prct_vcac_1.PROV_DEA_SK IS NOT NULL THEN hf_prct_vcac_1.PROV_DEA_SK
                          WHEN hf_npi_min_dea_1.PROV_DEA_SK IS NOT NULL THEN hf_npi_min_dea_1.PROV_DEA_SK
                          ELSE 0 END
               END
               """).alias("PROV_DEA_SK"),
    F.col("BCBSKCComm.DISPENSE_AS_WRTN_PROD_SEL_CD").alias("DRUG_CLM_DAW_CD"),
    F.col("LegalStatus").alias("DRUG_CLM_LGL_STTUS_CD"),
    F.col("ClaimTier").alias("DRUG_CLM_TIER_CD"),
    F.lit("NA").alias("DRUG_CLM_VNDR_STTUS_CD"),
    F.col("CmpndIn").alias("CMPND_IN"),
    F.col("BCBSKCComm.FRMLRY_FLAG").alias("FRMLRY_IN"),
    F.expr("""CASE WHEN (trim(BCBSKCComm.DRUG_TYP)='' OR BCBSKCComm.DRUG_TYP IS NULL) THEN 'U'
                    WHEN BCBSKCComm.SRC_SYS_CD<>'OPTUMRX' AND (BCBSKCComm.DRUG_TYP=2 OR BCBSKCComm.DRUG_TYP=3) THEN 'Y'
                    WHEN BCBSKCComm.SRC_SYS_CD='OPTUMRX' AND BCBSKCComm.DRUG_TYP=3 THEN 'Y'
                    ELSE 'N' END""").alias("GNRC_DRUG_IN"),
    F.when(F.isnull(F.col("MailOrderLkup.CD_MPPNG_SK")),F.lit("N")).otherwise(F.lit("Y")).alias("MAIL_ORDER_IN"),
    F.when(F.isnull(F.col("NdcLkup.DRUG_MNTN_IN")),F.lit("U")).otherwise(F.col("NdcLkup.DRUG_MNTN_IN")).alias("MNTN_IN"),
    F.expr("""CASE WHEN (BCBSKCComm.SRC_SYS_CD='OPTUMRX' AND trim(BCBSKCComm.BILL_BSS_CD)='06') THEN 'Y'
                    WHEN (BCBSKCComm.SRC_SYS_CD<>'OPTUMRX' AND trim(BCBSKCComm.BILL_BSS_CD)='09') THEN 'Y'
                    ELSE 'N' END""").alias("MAC_REDC_IN"),
    F.when(F.col("BCBSKCComm.FRMLRY_FLAG")=="Y",F.lit("N")).otherwise(F.lit("Y")).alias("NON_FRMLRY_DRUG_IN"),
    F.when(F.trim(F.col("BCBSKCComm.DRUG_TYP")).isNull(),F.lit("N"))
     .when(F.col("BCBSKCComm.DRUG_TYP")==1,F.lit("Y"))
     .otherwise(F.lit("N")).alias("SNGL_SRC_IN"),
    F.when(
        (F.col("BCBSKCComm.ADJDCT_DT").isNull()) | (F.length(F.trim(F.col("BCBSKCComm.ADJDCT_DT")))==0),
        F.lit("1753-01-01")
    ).otherwise(F.trim(F.col("BCBSKCComm.ADJDCT_DT"))).alias("ADJ_DT"),
    F.when(
        (F.col("BCBSKCComm.FILL_DT").isNull()) | (F.length(F.col("BCBSKCComm.FILL_DT"))==0),
        F.lit("1753-01-01")
    ).otherwise(F.col("BCBSKCComm.FILL_DT")).alias("FILL_DT"),
    F.when(
        (F.col("BCBSKCComm.ADJDCT_DT").isNull()) | (F.length(F.trim(F.col("BCBSKCComm.ADJDCT_DT"))) == 0),
        F.lit("1753-01-01")
    ).otherwise(F.trim(F.col("BCBSKCComm.ADJDCT_DT"))).alias("RECON_DT"),
    F.when(F.isnull(F.col("BCBSKCComm.DISPNS_FEE_AMT")),F.lit(0.00)).otherwise(F.col("BCBSKCComm.DISPNS_FEE_AMT")).alias("DISPNS_FEE_AMT"),
    F.lit(0.00).alias("HLTH_PLN_EXCL_AMT"),
    F.lit(0.00).alias("HLTH_PLN_PD_AMT"),
    F.when(F.isnull(F.col("BCBSKCComm.INGR_CST_AMT")),F.lit(0.00)).otherwise(F.col("BCBSKCComm.INGR_CST_AMT")).alias("INGR_CST_ALW_AMT"),
    F.when(F.isnull(F.col("BCBSKCComm.INGR_CST_AMT")),F.lit(0.00)).otherwise(F.col("BCBSKCComm.INGR_CST_AMT")).alias("INGR_CST_CHRGD_AMT"),
    F.when(F.col("BCBSKCComm.SRC_SYS_CD")=="OPTUMRX",
       F.when(F.col("BCBSKCComm.CMPND_CD")!=2,
          (F.col("BCBSKCComm.INGR_CST_SUB_AMT") + F.col("BCBSKCComm.CLNT_GNRL_PRPS_AREA").substr(F.lit(28),F.lit(11)).cast(DecimalType(38,10))
          - (F.col("BCBSKCComm.INGR_CST_AMT")+F.col("BCBSKCComm.DISPNS_FEE_AMT")))
       ).otherwise(F.lit(0.00))
    ).otherwise(F.lit(0.00)).alias("INGR_SAV_AMT"),
    F.lit(0.00).alias("MBR_DEDCT_EXCL_AMT"),
    F.when(F.col("BCBSKCComm.SRC_SYS_CD")=="OPTUMRX",F.col("BCBSKCComm.CLNT_PATN_PAY_ATRBD_PROD_AMT")).otherwise(F.lit(0.00)).alias("MBR_DIFF_PD_AMT"),
    F.lit(0.00).alias("MBR_OOP_AMT"),
    F.lit(0.00).alias("MBR_OOP_EXCL_AMT"),
    F.lit(0.00).alias("OTHR_SAV_AMT"),
    F.when(F.isnull(F.col("BCBSKCComm.METRIC_QTY")),F.lit(0)).otherwise(F.col("BCBSKCComm.METRIC_QTY")).alias("RX_ALW_QTY"),
    F.when(F.isnull(F.col("BCBSKCComm.METRIC_QTY")),F.lit(0)).otherwise(F.col("BCBSKCComm.METRIC_QTY")).alias("RX_SUBMT_QTY"),
    F.when(F.isnull(F.col("BCBSKCComm.SLS_TAX_AMT")),F.lit(0.00)).otherwise(F.col("BCBSKCComm.SLS_TAX_AMT")).alias("SLS_TAX_AMT"),
    F.when(F.isnull(F.col("BCBSKCComm.DAYS_SUPL")),F.lit(0)).otherwise(F.col("BCBSKCComm.DAYS_SUPL")).alias("RX_ALW_DAYS_SUPL_QTY"),
    F.when(F.isnull(F.col("BCBSKCComm.DAYS_SUPL")),F.lit(0)).otherwise(F.col("BCBSKCComm.DAYS_SUPL")).alias("RX_ORIG_DAYS_SUPL_QTY"),
    F.when(
        F.col("BCBSKCComm.SRC_SYS_CD")=="OPTUMRX",
        F.when(F.col("Pdxntwk_Lkup.TRGT_CD").isNotNull(),F.trim(F.col("Pdxntwk_Lkup.TRGT_CD"))).otherwise(F.lit("NA"))
    ).otherwise(F.lit("NA")).alias("PDX_NTWK_ID"),
    F.when(
        (F.col("BCBSKCComm.RX_NO").isNull())|(F.length(F.trim(F.col("BCBSKCComm.RX_NO")))==0),
        F.lit(None)
    ).otherwise(F.trim(F.col("BCBSKCComm.RX_NO"))).alias("RX_NO"),
    F.when(
        (F.col("BCBSKCComm.NEW_OR_RFL_CD").isNull())|(F.length(F.trim(F.col("BCBSKCComm.NEW_OR_RFL_CD")))==0),
        F.lit(None)
    ).otherwise(F.trim(F.col("BCBSKCComm.NEW_OR_RFL_CD"))).alias("RFL_NO"),
    F.col("BCBSKCComm.REF_NO").alias("VNDR_CLM_NO"),
    F.when(
        (F.col("BCBSKCComm.PRAUTH_NO").isNull()) | (F.length(F.trim(F.col("BCBSKCComm.PRAUTH_NO"))) == 0),
        F.lit(None)
    ).otherwise(F.col("BCBSKCComm.PRAUTH_NO")).alias("VNDR_PREAUTH_ID"),
    F.col("BCBSKCComm.CLM_STTUS_CD").alias("CLM_STTUS_CD"),
    F.trim(F.col("BCBSKCComm.PDX_NO")).alias("PDX_PROV_ID"),
    F.lit("NA").alias("NDC_LABEL_NM"),
    F.when(F.length(F.trim(F.col("BCBSKCComm.PRSCRBR_LAST_NM")))==0,F.lit("NA"))
     .otherwise(F.trim(F.col("BCBSKCComm.PRSCRBR_LAST_NM"))).alias("PRSCRB_NAME"),
    F.lit(None).alias("PHARMACY_NAME"),
    F.lit("NA").alias("DRUG_CLM_BNF_FRMLRY_POL_CD"),
    F.lit("NA").alias("DRUG_CLM_BNF_RSTRCT_CD"),
    F.when(
        (F.col("BCBSKCComm.SRC_SYS_CD").isin("MEDIMPACT","OPTUMRX")),
        F.col("BCBSKCComm.MCARE_D_COV_DRUG")
    ).otherwise(F.lit("NA")).alias("DRUG_CLM_MCPARTD_COVDRUG_CD"),
    F.lit("NA").alias("DRUG_CLM_PRAUTH_CD"),
    F.lit("N").alias("MNDTRY_MAIL_ORDER_IN"),
    F.when(
        (F.col("BCBSKCComm.ADMIN_FEE_AMT").isNull())| (F.length(F.trim(F.col("BCBSKCComm.ADMIN_FEE_AMT")))==0),
        F.lit(0.00)
    ).otherwise(F.col("BCBSKCComm.ADMIN_FEE_AMT")).alias("ADM_FEE_AMT"),
    F.when(F.col("BCBSKCComm.SRC_SYS_CD")=="OPTUMRX",F.col("BCBSKCComm.BILL_BSS_CD")).otherwise(F.lit("NA")).alias("DRUG_CLM_BILL_BSS_CD"),
    F.when(
        F.col("BCBSKCComm.SRC_SYS_CD")=="OPTUMRX",
        F.when(
           (F.col("BCBSKCComm.FULL_AVG_WHLSL_PRICE").isNull()) | (F.trim(F.col("BCBSKCComm.FULL_AVG_WHLSL_PRICE"))==""),
           F.lit(0.00)
        ).otherwise(
           F.when(
               (F.col("BCBSKCComm.CLM_TYP")=="R") & (F.col("BCBSKCComm.FULL_AVG_WHLSL_PRICE")>0),
               -1*F.col("BCBSKCComm.FULL_AVG_WHLSL_PRICE")
           ).otherwise(F.col("BCBSKCComm.FULL_AVG_WHLSL_PRICE"))
        )
    ).when(
        F.col("BCBSKCComm.SRC_SYS_CD")=="MEDIMPACT",
        F.col("BCBSKCComm.FULL_AVG_WHLSL_PRICE")
    ).otherwise(F.lit(0.00)).alias("AVG_WHLSL_PRICE_AMT"),
    F.col("BCBSKCComm.PDX_NTNL_PROV_ID").alias("NTNL_PROV_ID"),
    F.expr("""CASE WHEN (IF(BCBSKCComm.PRSCRBR_ID IS NULL, '', trim(BCBSKCComm.PRSCRBR_ID)))=''
                    THEN 'NA' ELSE trim(BCBSKCComm.PRSCRBR_ID) END""").alias("PRESCRIBER_ID"),
    F.when(
        (F.col("BCBSKCComm.SRC_SYS_CD")=="OPTUMRX") & (F.length(F.trim(F.col("BCBSKCComm.USL_AND_CUST_CHRG_AMT")))==0),
        F.lit(0.00)
    ).otherwise(F.col("BCBSKCComm.USL_AND_CUST_CHRG_AMT")).alias("UCR_AMT"),
    F.when(
        (F.col("BCBSKCComm.INCNTV_FEE_AMT").isNull()) | 
        (F.length(F.trim(F.col("BCBSKCComm.INCNTV_FEE_AMT"))) == 0),
        F.lit(0.00)
    ).otherwise(F.col("BCBSKCComm.INCNTV_FEE_AMT")).alias("INCNTV_FEE_AMT"),
    F.col("BCBSKCComm.ORIG_MBR").alias("ORIG_MBR"),
    F.when(F.col("BCBSKCComm.SRC_SYS_CD")=="OPTUMRX",F.col("BCBSKCComm.PDX_TYP")).otherwise(F.lit("NA")).alias("PDX_TYP"),
    F.when(F.col("BCBSKCComm.SRC_SYS_CD")=="OPTUMRX",F.col("SVPrtlFillSttusCd")).otherwise(F.lit("NA")).alias("PRTL_FILL_STTUS_CD"),
    F.col("BCBSKCComm.PRSCRBR_QLFR").alias("PRSCRBR_QLFR"),
    F.col("BCBSKCComm.PRSCRBR_NTNL_PROV_ID").alias("PRSCRBR_NTNL_PROV_ID"),
    F.col("BCBSKCComm.PDX_NTNL_PROV_ID").alias("PDX_NTNL_PROV_ID"),
    F.col("BCBSKCComm.SPEC_DRUG_IN").alias("SPEC_DRUG_IN"),
    F.col("BCBSKCComm.SUBMT_PROD_ID_QLFR").alias("SUBMT_PROD_ID_QLFR"),
    F.col("BCBSKCComm.CNTNGNT_THER_FLAG").alias("CNTNGNT_THER_FLAG"),
    F.col("BCBSKCComm.CNTNGNT_THER_SCHD").alias("CNTNGNT_THER_SCHD"),
    F.when(F.col("BCBSKCComm.SRC_SYS_CD")=="OPTUMRX",F.col("BCBSKCComm.CLNT_PATN_PAY_ATRBD_PROD_AMT")).otherwise(F.lit(0.00)).alias("CLNT_PATN_PAY_ATRBD_PROD_AMT"),
    F.when(F.col("BCBSKCComm.SRC_SYS_CD")=="OPTUMRX",F.col("BCBSKCComm.CLNT_PATN_PAY_ATRBD_SLS_TAX_AMT")).otherwise(F.lit(0.00)).alias("CLNT_PATN_PAY_ATRBD_SLS_TAX_AMT"),
    F.when(F.col("BCBSKCComm.SRC_SYS_CD")=="OPTUMRX",F.col("BCBSKCComm.CLNT_PATN_PAY_ATRBD_PRCSR_FEE_AMT")).otherwise(F.lit(0.00)).alias("CLNT_PATN_PAY_ATRBD_PRCSR_FEE_AMT"),
    F.when(F.col("BCBSKCComm.SRC_SYS_CD")=="OPTUMRX",F.col("BCBSKCComm.CLNT_PATN_PAY_ATRBD_NTWK_AMT")).otherwise(F.lit(0.00)).alias("CLNT_PATN_PAY_ATRBD_NTWK_AMT"),
    F.when(F.col("BCBSKCComm.SRC_SYS_CD")=="OPTUMRX",F.col("BCBSKCComm.GNRC_PROD_IN")).otherwise(F.lit("NA")).alias("GNRC_PROD_IN")
)

# Snapshot (CTransformerStage):
#   We do 2 lookup links: "hf_dea" (with primary key PROV_DEA_SK, DEA_NO) and "dea_no" => we join them. 
#   For brevity, let's define them:

df_hf_dea_provdeask = df_hcbsk # Actually from the hashed file "hf_bcbskccomm_drug_clm_dea" => "hf_dea" => we deduped above as df_hf_bcbskccomm_drug_clm_dea_PROVDEASK
df_hf_dea_deano = df_hf_bcbskccomm_drug_clm_dea_DEANO

df_Snapshot = df_Transform.alias("Transform")

df_Snapshot = df_Snapshot.join(
    df_hf_dea_provdeask.alias("hf_dea"),
    on=[
        F.trim(F.col("BCBSKCComm.PRSCRBR_ID")) == F.col("hf_dea.DEA_NO"),
        F.col("Transform.PROV_DEA_SK") == F.col("hf_dea.PROV_DEA_SK")
    ],
    how="left"
)

df_Snapshot = df_Snapshot.join(
    df_hf_dea_deano.alias("dea_no"),
    on=[ F.trim(F.col("Transform.PRESCRIBER_ID")) == F.col("dea_no.DEA_NO") ],
    how="left"
)

# Stage variables in "Snapshot"
df_Snapshot = df_Snapshot.withColumn(
    "NpiDeaNo",
    F.when(F.length(F.trim(F.col("dea_no.DEA_NO")))>0, F.trim(F.col("dea_no.DEA_NO"))).otherwise(F.lit("UNK"))
)

df_Snapshot = df_Snapshot.withColumn(
    "FoundOptumRXPrescProvDeaNo",
    F.when(
      (F.col("Transform.SRC_SYS_CD")=="OPTUMRX") & 
      ((F.col("dea_no.PROV_DEA_SK").isNotNull()) & (F.col("dea_no.PROV_DEA_SK")!=0) & (F.col("dea_no.PROV_DEA_SK")!=1)),
      F.lit("Y")
    ).when(
      (F.col("Transform.SRC_SYS_CD")=="OPTUMRX") &
      ((F.col("hf_dea.DEA_NO").isNotNull()) & (F.col("hf_dea.DEA_NO")!="UNK") & (F.col("hf_dea.DEA_NO")!="NA")),
      F.lit("Y")
    ).otherwise(F.lit("N"))
)

# Output link "Pkey"
df_Pkey = df_Snapshot.select(
    F.col("Transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("Transform.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("Transform.DISCARD_IN").alias("DISCARD_IN"),
    F.col("Transform.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("Transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("Transform.ERR_CT").alias("ERR_CT"),
    F.col("Transform.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("Transform.DRUG_CLM_SK").alias("DRUG_CLM_SK"),
    F.col("Transform.CLM_ID").alias("CLM_ID"),
    F.col("Transform.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Transform.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Transform.NDC").alias("NDC"),
    F.col("Transform.NDC_SK").alias("NDC_SK"),
    F.expr("""CASE WHEN Transform.SRC_SYS_CD='OPTUMRX'
                THEN CASE WHEN dea_no.PROV_DEA_SK IS NOT NULL AND dea_no.PROV_DEA_SK<>0 AND dea_no.PROV_DEA_SK<>1
                          THEN trim(Transform.PRESCRIBER_ID)
                          WHEN hf_dea.DEA_NO IS NOT NULL AND hf_dea.DEA_NO<>'UNK' AND hf_dea.DEA_NO<>'NA'
                          THEN trim(hf_dea.DEA_NO)
                          ELSE trim(Transform.PRESCRIBER_ID)
                     END
                ELSE CASE WHEN dea_no.PROV_DEA_SK IS NOT NULL OR length(trim(dea_no.PROV_DEA_SK))>0
                          THEN trim(Transform.PRESCRIBER_ID)
                          WHEN hf_dea.DEA_NO IS NULL
                          THEN CASE WHEN (Transform.PRESCRIBER_ID IS NOT NULL AND length(trim(Transform.PRESCRIBER_ID))>0) 
                                    THEN trim(Transform.PRESCRIBER_ID) ELSE 'UNK' END
                          ELSE hf_dea.DEA_NO
                     END
               END""").alias("PRSCRB_PROV_DEA"),
    F.col("Transform.DRUG_CLM_DAW_CD").alias("DRUG_CLM_DAW_CD"),
    F.col("Transform.DRUG_CLM_LGL_STTUS_CD").alias("DRUG_CLM_LGL_STTUS_CD"),
    F.col("Transform.DRUG_CLM_TIER_CD").alias("DRUG_CLM_TIER_CD"),
    F.col("Transform.DRUG_CLM_VNDR_STTUS_CD").alias("DRUG_CLM_VNDR_STTUS_CD"),
    F.col("Transform.CMPND_IN").alias("CMPND_IN"),
    F.col("Transform.FRMLRY_IN").alias("FRMLRY_IN"),
    F.col("Transform.GNRC_DRUG_IN").alias("GNRC_DRUG_IN"),
    F.col("Transform.MAIL_ORDER_IN").alias("MAIL_ORDER_IN"),
    F.col("Transform.MNTN_IN").alias("MNTN_IN"),
    F.col("Transform.MAC_REDC_IN").alias("MAC_REDC_IN"),
    F.col("Transform.NON_FRMLRY_DRUG_IN").alias("NON_FRMLRY_DRUG_IN"),
    F.col("Transform.SNGL_SRC_IN").alias("SNGL_SRC_IN"),
    F.col("Transform.ADJ_DT").alias("ADJ_DT"),
    F.col("Transform.FILL_DT").alias("FILL_DT"),
    F.col("Transform.RECON_DT").alias("RECON_DT"),
    F.col("Transform.DISPNS_FEE_AMT").alias("DISPNS_FEE_AMT"),
    F.col("Transform.HLTH_PLN_EXCL_AMT").alias("HLTH_PLN_EXCL_AMT"),
    F.col("Transform.HLTH_PLN_PD_AMT").alias("HLTH_PLN_PD_AMT"),
    F.col("Transform.INGR_CST_ALW_AMT").alias("INGR_CST_ALW_AMT"),
    F.col("Transform.INGR_CST_CHRGD_AMT").alias("INGR_CST_CHRGD_AMT"),
    F.col("Transform.INGR_SAV_AMT").alias("INGR_SAV_AMT"),
    F.col("Transform.MBR_DEDCT_EXCL_AMT").alias("MBR_DEDCT_EXCL_AMT"),
    F.col("Transform.MBR_DIFF_PD_AMT").alias("MBR_DIFF_PD_AMT"),
    F.col("Transform.MBR_OOP_AMT").alias("MBR_OOP_AMT"),
    F.col("Transform.MBR_OOP_EXCL_AMT").alias("MBR_OOP_EXCL_AMT"),
    F.col("Transform.OTHR_SAV_AMT").alias("OTHR_SAV_AMT"),
    F.col("Transform.RX_ALW_QTY").alias("RX_ALW_QTY"),
    F.col("Transform.RX_SUBMT_QTY").alias("RX_SUBMT_QTY"),
    F.col("Transform.SLS_TAX_AMT").alias("SLS_TAX_AMT"),
    F.col("Transform.RX_ALW_DAYS_SUPL_QTY").alias("RX_ALW_DAYS_SUPL_QTY"),
    F.col("Transform.RX_ORIG_DAYS_SUPL_QTY").alias("RX_ORIG_DAYS_SUPL_QTY"),
    F.col("Transform.PDX_NTWK_ID").alias("PDX_NTWK_ID"),
    F.col("Transform.RX_NO").alias("RX_NO"),
    F.col("Transform.RFL_NO").alias("RFL_NO"),
    F.col("Transform.VNDR_CLM_NO").alias("VNDR_CLM_NO"),
    F.col("Transform.VNDR_PREAUTH_ID").alias("VNDR_PREAUTH_ID"),
    F.col("Transform.CLM_STTUS_CD").alias("CLM_STTUS_CD"),
    F.col("Transform.PDX_PROV_ID").alias("PROV_ID"),
    F.col("Transform.NDC_LABEL_NM").alias("NDC_LABEL_NM"),
    F.col("Transform.PRSCRB_NAME").alias("PRSCRB_NAME"),
    F.col("Transform.PHARMACY_NAME").alias("PHARMACY_NAME"),
    F.col("Transform.DRUG_CLM_BNF_FRMLRY_POL_CD").alias("DRUG_CLM_BNF_FRMLRY_POL_CD"),
    F.col("Transform.DRUG_CLM_BNF_RSTRCT_CD").alias("DRUG_CLM_BNF_RSTRCT_CD"),
    F.col("Transform.DRUG_CLM_MCPARTD_COVDRUG_CD").alias("DRUG_CLM_MCPARTD_COVDRUG_CD"),
    F.col("Transform.DRUG_CLM_PRAUTH_CD").alias("DRUG_CLM_PRAUTH_CD"),
    F.col("Transform.MNDTRY_MAIL_ORDER_IN").alias("MNDTRY_MAIL_ORDER_IN"),
    F.col("Transform.ADM_FEE_AMT").alias("ADM_FEE_AMT"),
    F.col("Transform.DRUG_CLM_BILL_BSS_CD").alias("DRUG_CLM_BILL_BSS_CD"),
    F.col("Transform.AVG_WHLSL_PRICE_AMT").alias("AVG_WHLSL_PRICE_AMT"),
    F.col("Transform.NTNL_PROV_ID").alias("NTNL_PROV_ID"),
    F.lit("NA").alias("DRUG_NM"),
    F.col("Transform.UCR_AMT").alias("UCR_AMT"),
    F.col("Transform.PRTL_FILL_STTUS_CD").alias("PRTL_FILL_STTUS_CD"),
    F.col("Transform.PDX_TYP").alias("PDX_TYP"),
    F.col("Transform.INCNTV_FEE_AMT").alias("INCNTV_FEE"),
    F.col("Transform.PRSCRBR_NTNL_PROV_ID").alias("PRSCRBR_NTNL_PROV_ID"),
    F.col("Transform.SPEC_DRUG_IN").alias("SPEC_DRUG_IN"),
    F.col("Transform.SUBMT_PROD_ID_QLFR").alias("SUBMT_PROD_ID_QLFR"),
    F.col("Transform.CNTNGNT_THER_FLAG").alias("CNTNGNT_THER_FLAG"),
    F.col("Transform.CNTNGNT_THER_SCHD").alias("CNTNGNT_THER_SCHD"),
    F.col("Transform.CLNT_PATN_PAY_ATRBD_PROD_AMT").alias("CLNT_PATN_PAY_ATRBD_PROD_AMT"),
    F.col("Transform.CLNT_PATN_PAY_ATRBD_SLS_TAX_AMT").alias("CLNT_PATN_PAY_ATRBD_SLS_TAX_AMT"),
    F.col("Transform.CLNT_PATN_PAY_ATRBD_PRCSR_FEE_AMT").alias("CLNT_PATN_PAY_ATRBD_PRCSR_FEE_AMT"),
    F.col("Transform.CLNT_PATN_PAY_ATRBD_NTWK_AMT").alias("CLNT_PATN_PAY_ATRBD_NTWK_AMT"),
    F.col("Transform.GNRC_PROD_IN").alias("GNRC_PROD_IN")
)

# Output link "Snapshot" => "B_DRUG_CLM"
df_B_DRUG_CLM = df_Snapshot.select(
    F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),  # The job uses "SrcSysCdSk" from somewhere, here we can do an alias or fill 0 if undefined
    F.col("Transform.CLM_ID").alias("CLM_ID")
)

# Write B_DRUG_CLM (CSeqFileStage)
write_files(
    df_B_DRUG_CLM,
    f"{adls_path}/load/B_DRUG_CLM.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Output link "Snapshot" => "P_CLM_PDX" with constraint
df_P_CLM_PDX = df_Snapshot.filter(
    (F.col("Transform.SRC_SYS_CD") == "OPTUMRX") &
    (
      (F.col("FoundOptumRXPrescProvDeaNo") == "N") |
      (
        (F.col("FoundOptumRXPrescProvDeaNo") == "Y") &
        (
         (F.length(F.trim(F.col("Transform.PRSCRBR_NTNL_PROV_ID"))) > 0) &
         (
           (F.trim(F.col("Transform.PRSCRBR_NTNL_PROV_ID")) == "NA") |
           (F.trim(F.col("Transform.PRSCRBR_NTNL_PROV_ID")) == "UNK")
         )
        )
      )
    )
).select(
    F.expr("GetFkeyClm(SrcSysCd, 1, Transform.CLM_ID, 'N')").alias("CLM_SK"),
    F.col("Transform.CLM_ID").alias("CLM_ID"),
    F.lit("OPTUMRX").alias("SRC_SYS_CD"),
    F.expr("""CASE WHEN dea_no.PROV_DEA_SK IS NOT NULL AND dea_no.PROV_DEA_SK<>0 AND dea_no.PROV_DEA_SK<>1
                     THEN trim(Transform.PRESCRIBER_ID)
                WHEN hf_dea.DEA_NO IS NOT NULL AND hf_dea.DEA_NO<>'UNK' AND hf_dea.DEA_NO<>'NA'
                     THEN trim(hf_dea.DEA_NO)
                ELSE trim(Transform.PRESCRIBER_ID)
           END""").alias("PRSCRB_PROV_DEA_ID"),
    F.trim(F.col("Transform.PRSCRBR_NTNL_PROV_ID")).alias("NTNL_PROV_ID"),
    F.when(F.col("Transform.ORIG_MBR")!=0,F.lit(1)).otherwise(F.col("Transform.ORIG_MBR")).alias("MBR_UNIQ_KEY")
)

# Write P_CLM_PDX (CSeqFileStage)
write_files(
    df_P_CLM_PDX,
    f"{adls_path}/load/P_CLM_PDX.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Shared Container "DrugClmPK" – usage
params_shared = {
    "FilePath": get_widget_value("FilePath",""),
    "RunCycle": RunCycle,
    "CactusServer": get_widget_value("CactusServer",""),
    "CactusOwner": get_widget_value("CactusOwner",""),
    "CactusAcct": get_widget_value("CactusAcct",""),
    "CactusPW": get_widget_value("CactusPW",""),
    "IDSDB": get_widget_value("IDSDB",""),
    "IDSOwner": IDSOwner,
    "IDSAcct": get_widget_value("IDSAcct",""),
    "IDSPW": get_widget_value("IDSPW",""),
    "ProvRunCycle": ProvRunCycle
}
df_BCBSKCCommDrugClm, df_BCBSKCCommNdc, df_BCBSKCCommProvDea, df_BCBSKCCommProv, df_BCBSKCCommProvLoc = DrugClmPK(df_Pkey, params_shared)

# Final file outputs from those container’s outputs

# BCBSKCCommDrugClm
write_files(
    df_BCBSKCCommDrugClm,
    f"{adls_path}/key/BCBSKCCommDrugClmExtr_{SrcSysCd}.DrugClmDrug.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# BCBSKCCommNdc
write_files(
    df_BCBSKCCommNdc,
    f"{adls_path}/key/BCBSKCCommDrugClmExtr_{SrcSysCd}.DrugNDC.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# BCBSKCCommProvDea
write_files(
    df_BCBSKCCommProvDea,
    f"{adls_path}/key/BCBSKCCommDrugClmExtr_{SrcSysCd}.DrugProvDea.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# BCBSKCCommProv
write_files(
    df_BCBSKCCommProv,
    f"{adls_path}/key/BCBSKCCommDrugClmExtr_{SrcSysCd}.DrugProv.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# BCBSKCCommProvLoc
write_files(
    df_BCBSKCCommProvLoc,
    f"{adls_path}/key/BCBSKCCommDrugClmExtr_{SrcSysCd}.ProvLoc.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# End of notebook.