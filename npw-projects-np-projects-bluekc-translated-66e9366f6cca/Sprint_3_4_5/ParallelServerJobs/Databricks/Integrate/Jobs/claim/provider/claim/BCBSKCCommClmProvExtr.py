# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC COPYRIGHT 2010 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC CALLED BY:  MedtrakDrugExtrSeq
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:     Reads the ESIDrugFile.dat created in  ESIClmLand  job and puts the data into the claim  provider common record format and runs through primary key using Shared container ClmProvPkey
# MAGIC     
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Kaushik Kapoor       2018-01-22        5828                      Initial Programming                                                                      IntegrateDev2                 Kalyan Neelam          2018-02-26 
# MAGIC 
# MAGIC Sudhir Bomshetty    2018-04-13        5781 - HEDIS        Added the NTNL_PROV_ID                                                       IntegrateDev2                 Kalyan Neelam          2018-04-25
# MAGIC 
# MAGIC Peter Gichiri -  2019-10 -17   Added the stage Variables svSvcNtnlProvId  and svPcpNtnlProvId for SVC and PCP to populate NTNL_PROV_ID ,  
# MAGIC                                        For the PRSCRB mapped PROV.TAX_ID for the PROV_ID associated with the row, udated the logic for the PROV_ID 
# MAGIC                                       as per OptumRx requirements                                                                                                   IntegrateDev1
# MAGIC 
# MAGIC 
# MAGIC Rekha Radhakrishna 2019-11-04    6131- PBM Replacement modified  NTNL_PROV_ID logic modified for PRSCRB, SVC and PCP output links
# MAGIC 
# MAGIC 
# MAGIC Sharon Anderw           2019-11-04     OptumRX             modified all provider reference queries to provide the right keys for provider matching        Kalyan Neelam          2019-11-21
# MAGIC                                                                                       exluded  providers with NTNL_PROV_ID UNK and NA when the NTNL_PROV_ID was the field matched to
# MAGIC                                                                                       included new functions that check if the Prescriber ID and Prescriber NPI are valid DEA and NPI numbers
# MAGIC                                                                                       consolidated the Servicing and Prescriber look ups to the same hash files since they are all derived on same IDS Provider result sets.
# MAGIC                                                                                       constructed a new hierarchy of rules for determining the ServicingProviderID/NPI and PrescribingProviderID/NPI
# MAGIC changed
# MAGIC 8;hf_bcbskccomm_clm_mbr_pcp;hf_bcbskccomm_clm_hmo_mbr_pcp;hf_bcbskccomm_clm_provnpi_lkup;hf_bcbskccomm_clm_prov_lkup;hf_bcbskccomm_prscrb_prov_npi_lkup;hf_min_bcbskccomm_clm_prov_prscrb;hf_bcbskccomm_prov_dea_lkup;hf_bcbskccomm_provdea_npi_lkup
# MAGIC to 
# MAGIC 7:hf_bcbskccomm_clm_mbr_pcp;hf_bcbskccomm_clm_hmo_mbr_pcp;hf_bcbskccomm_prscrb_prov_dea1;hf_bcbskccomm_prscrb_prov_dea2;hf_bcbskccomm_clm_prov_prscrb3;hf_bcbskccomm_clm_prov_prscrb4;hf_bcbskccomm_clm_prov_prscrb5
# MAGIC 
# MAGIC Sri Nannapaneni           2020-07-15      6131 PBM replacement      Added 7 new fields to source seq file                                          IntegrateDev2                 
# MAGIC Sri Nannapaneni           08/13/2020    6131- PBM Replacement    Added LOB_IN field to source seq file                                          IntegrateDev2      Sravya Gorla         2020-12-09
# MAGIC 
# MAGIC Sudeep Reddy             2024-03-20       US608683           Replace MAX logic based on requiremnt in StageName - ProvExtr            IntegrateDev2       Jeyaprasanna      2024-03-24
# MAGIC                                                                                           of Link Name - PidNPINPI4,PidDeaProvNbr2,PidDeanCmnPrctProv1

# MAGIC Read the Common file created
# MAGIC Driver Table data created in MedtrakClmDailyLand
# MAGIC This container is used in:
# MAGIC ESIClmProvExtr
# MAGIC FctsClmProvExtr
# MAGIC MCSourceClmProvExtr
# MAGIC MedicaidClmProvExtr
# MAGIC NascoClmProvExtr
# MAGIC PcsClmProvExtr
# MAGIC WellDyneClmProvExtr
# MAGIC MedtrakClmProvExtr
# MAGIC BCBSKCCommClmProvExtr
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC Assign primary surrogate key
# MAGIC Writing Sequential File to /key
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
    DecimalType,
    IntegerType
)
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Acquire all parameter values
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
CurrentDate = get_widget_value('CurrentDate','')
RunID = get_widget_value('RunID','')
RunCycle = get_widget_value('RunCycle','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

# ------------------------------------------------------------------------------
# Stage: PDX_CLM_STD_INPT_Land  (CSeqFileStage)
# Read a delimited .dat file with no header
# ------------------------------------------------------------------------------
schema_PDX_CLM_STD_INPT_Land = StructType([
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("FILE_RCVD_DT", StringType(), True),
    StructField("RCRD_ID", DecimalType(38,10), True),
    StructField("PRCSR_NO", StringType(), True),
    StructField("BTCH_NO", DecimalType(38,10), True),
    StructField("PDX_NO", DecimalType(38,10), True),
    StructField("RX_NO", DecimalType(38,10), True),
    StructField("FILL_DT", StringType(), True),
    StructField("NDC", DecimalType(38,10), True),
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
    StructField("GNRC_PROD_IN", DecimalType(38,10), True),
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

df_Extract = (
    spark.read.format("csv")
    .schema(schema_PDX_CLM_STD_INPT_Land)
    .option("header", False)
    .option("quote", "\"")
    .load(f"{adls_path}/verified/PDX_CLM_STD_INPT_Land_{SrcSysCd}.dat.{RunID}")
)

# ------------------------------------------------------------------------------
# Stage: ProvExtr (DB2Connector, Database=IDS) with multiple output pins
# ------------------------------------------------------------------------------
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

# -- Output pin 1 -> "PidDeaProvNbr2"
query_PidDeaProvNbr2 = f"""
SELECT 
DEA.DEA_NO,
DEA.NTNL_PROV_ID,
PROV.PROV_ID,
PROV.NTNL_PROV_ID,
PROV.TAX_ID  
FROM 
{IDSOwner}.PROV_DEA DEA INNER JOIN
{IDSOwner}.PROV PROV
ON     DEA.NTNL_PROV_ID  = PROV.NTNL_PROV_ID
INNER JOIN 
(
  select PROV_ID as MAX_PROV_ID , NTNL_PROV_ID from 
        (select PROV_ID,Type_Prov_Id,NTNL_PROV_ID ,
            RANK() OVER (PARTITION BY NTNL_PROV_ID order by Type_Prov_Id) as rank 
               FROM 
               (
                   Select CAST(MAX( CAST(MAX_PROV.PROV_ID as BIGINT)) as VARCHAR(12)) as  PROV_ID,  '1_Numeric_PROV_ID' as Type_Prov_Id ,NTNL_PROV_ID 
                    from {IDSOwner}.PROV MAX_PROV
                       JOIN {IDSOwner}.CD_MPPNG MPPNG ON MAX_PROV.SRC_SYS_CD_SK = MPPNG.CD_MPPNG_SK
                      where 
                      MAX_PROV.TERM_DT_SK >= '{CurrentDate}' and 
                      TRANSLATE(PROV_ID,'','0123456789') ='' AND
                      MPPNG.SRC_CD = 'FACETS'  AND NTNL_PROV_ID <>'NA' 
                      GROUP BY MAX_PROV.NTNL_PROV_ID
                      union all
                      Select MAX( MAX_PROV.PROV_ID) PROV_ID, '2_AlphaNumeric_PROV_ID' as  Type_Prov_Id ,NTNL_PROV_ID
                      from {IDSOwner}.PROV MAX_PROV
                      JOIN {IDSOwner}.CD_MPPNG MPPNG ON MAX_PROV.SRC_SYS_CD_SK = MPPNG.CD_MPPNG_SK
                      where 
                     MAX_PROV.TERM_DT_SK >= '{CurrentDate}' and 
                      TRANSLATE(PROV_ID,'','0123456789') <>'' AND
                      MPPNG.SRC_CD = 'FACETS'  AND NTNL_PROV_ID <>'NA' 
                      GROUP BY MAX_PROV.NTNL_PROV_ID
                      union all
                      Select  CAST(MAX( CAST(MAX_PROV.PROV_ID as BIGINT)) as VARCHAR(12)) as PROV_ID,  '3_Numeric_BCA_PROV_ID' as Type_Prov_Id  ,NTNL_PROV_ID
                      from {IDSOwner}.PROV MAX_PROV
                      JOIN {IDSOwner}.CD_MPPNG MPPNG ON MAX_PROV.SRC_SYS_CD_SK = MPPNG.CD_MPPNG_SK
                      where 
                      MAX_PROV.TERM_DT_SK >= '{CurrentDate}' and 
\t\t\t\t\t      TRANSLATE(PROV_ID,'','0123456789') ='' AND
                      MPPNG.SRC_CD = 'BCA'  AND NTNL_PROV_ID <>'NA' 
                      GROUP BY MAX_PROV.NTNL_PROV_ID
                      union all
                      Select  MAX(MAX_PROV.PROV_ID ) as PROV_ID,  '4_AlphaNumeric_BCA_PROV_ID' as Type_Prov_Id  ,NTNL_PROV_ID
                      from {IDSOwner}.PROV MAX_PROV
                      JOIN {IDSOwner}.CD_MPPNG MPPNG ON MAX_PROV.SRC_SYS_CD_SK = MPPNG.CD_MPPNG_SK
                      where 
                     MAX_PROV.TERM_DT_SK >= '{CurrentDate}' and 
\t\t\t\t\t  TRANSLATE(PROV_ID,'','0123456789') <>'' AND
                      MPPNG.SRC_CD = 'BCA'  AND NTNL_PROV_ID <>'NA' 
                      GROUP BY MAX_PROV.NTNL_PROV_ID
                      union all
                      Select  CAST(MAX( CAST(MAX_PROV.PROV_ID as BIGINT)) as VARCHAR(12)) as PROV_ID, '5_Numeric_NABP_PROV_ID' as Type_Prov_Id ,NTNL_PROV_ID
                      from {IDSOwner}.PROV MAX_PROV
                      JOIN {IDSOwner}.CD_MPPNG MPPNG ON MAX_PROV.SRC_SYS_CD_SK = MPPNG.CD_MPPNG_SK
                      where 
                      MAX_PROV.TERM_DT_SK >= '{CurrentDate}' and 
                      MPPNG.SRC_CD = 'NABP' AND
\t\t\t\t\t  TRANSLATE(PROV_ID,'','0123456789') =''  AND NTNL_PROV_ID <>'NA' 
                      GROUP BY MAX_PROV.NTNL_PROV_ID
                      union all
                      Select  MAX( MAX_PROV.PROV_ID ) as PROV_ID, '6_AlphaNumeric_NABP_PROV_ID' as Type_Prov_Id ,NTNL_PROV_ID
                      from {IDSOwner}.PROV MAX_PROV
                      JOIN {IDSOwner}.CD_MPPNG MPPNG ON MAX_PROV.SRC_SYS_CD_SK = MPPNG.CD_MPPNG_SK
                      where 
                      MAX_PROV.TERM_DT_SK >= '{CurrentDate}' and 
                      MPPNG.SRC_CD = 'NABP' AND
\t\t\t\t\t  TRANSLATE(PROV_ID,'','0123456789') <>''  AND NTNL_PROV_ID <>'NA' 
                      GROUP BY MAX_PROV.NTNL_PROV_ID
                      union all 
                      Select  CAST(MAX( CAST(MAX_PROV.PROV_ID as BIGINT)) as VARCHAR(12)) as  PROV_ID, '7_Numeric_MAX_PROV_ID' as Type_Prov_Id ,NTNL_PROV_ID
                      from {IDSOwner}.PROV MAX_PROV
                      where 
\t      TRANSLATE(PROV_ID,'','0123456789') ='' AND
                      NTNL_PROV_ID <>'NA' 
                      GROUP BY MAX_PROV.NTNL_PROV_ID
                      union all 
                      Select  MAX( MAX_PROV.PROV_ID) PROV_ID, '8_AlphaNumeric_MAX_PROV_ID' as Type_Prov_Id ,NTNL_PROV_ID
                      from {IDSOwner}.PROV MAX_PROV
                      where 
                      NTNL_PROV_ID <>'NA' 
                      GROUP BY MAX_PROV.NTNL_PROV_ID
               ) T
        ) T2 where rank = 1
)     MAX_PROV
ON MAX_PROV.NTNL_PROV_ID =  PROV.NTNL_PROV_ID 
where (case when TRANSLATE(PROV.PROV_ID,'','0123456789') = '' and PROV.NTNL_PROV_ID <>'NA'  
          then cast(CAST(PROV.PROV_ID as BIGINT) as varchar(12)) else PROV.PROV_ID end) = MAX_PROV.MAX_PROV_ID
and DEA.NTNL_PROV_ID  not in ('UNK', 'NA', ' ')
AND LENGTH ( TRIM ( DEA.NTNL_PROV_ID )) = 10
AND PROV.PROV_ID not in ('UNK' , 'NA') 
AND DEA.CMN_PRCT_SK  in (0, 1)
"""

df_ProvExtr_PidDeaProvNbr2 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_PidDeaProvNbr2)
    .load()
)

# -- Output pin 2 -> "PidDeaCmnPrctProv1"
query_PidDeaCmnPrctProv1 = f"""
SELECT 
DEA.DEA_NO,
DEA.NTNL_PROV_ID,
PROV.PROV_ID,
PROV.NTNL_PROV_ID,
PROV.TAX_ID

FROM 
{IDSOwner}.PROV_DEA DEA INNER JOIN
{IDSOwner}.PROV PROV
ON DEA.CMN_PRCT_SK   =  PROV.CMN_PRCT_SK
INNER JOIN 
(
  select PROV_ID as MAX_PROV_ID , NTNL_PROV_ID from 
        (select PROV_ID,Type_Prov_Id,NTNL_PROV_ID ,
            RANK() OVER (PARTITION BY NTNL_PROV_ID order by Type_Prov_Id) as rank 
               FROM 
               (
                   Select CAST(MAX( CAST(MAX_PROV.PROV_ID as BIGINT)) as VARCHAR(12)) as  PROV_ID,  '1_Numeric_PROV_ID' as Type_Prov_Id ,NTNL_PROV_ID 
                    from {IDSOwner}.PROV MAX_PROV
                       JOIN {IDSOwner}.CD_MPPNG MPPNG ON MAX_PROV.SRC_SYS_CD_SK = MPPNG.CD_MPPNG_SK
                      where 
                      MAX_PROV.TERM_DT_SK >= '{CurrentDate}' and 
                      TRANSLATE(PROV_ID,'','0123456789') ='' AND
                      MPPNG.SRC_CD = 'FACETS'  AND NTNL_PROV_ID <>'NA' 
                      GROUP BY MAX_PROV.NTNL_PROV_ID
                      union all
                      Select MAX( MAX_PROV.PROV_ID) PROV_ID, '2_AlphaNumeric_PROV_ID' as  Type_Prov_Id ,NTNL_PROV_ID
                      from {IDSOwner}.PROV MAX_PROV
                      JOIN {IDSOwner}.CD_MPPNG MPPNG ON MAX_PROV.SRC_SYS_CD_SK = MPPNG.CD_MPPNG_SK
                      where 
                      MAX_PROV.TERM_DT_SK >= '{CurrentDate}' and 
                      TRANSLATE(PROV_ID,'','0123456789') <>'' AND
                      MPPNG.SRC_CD = 'FACETS'  AND NTNL_PROV_ID <>'NA' 
                      GROUP BY MAX_PROV.NTNL_PROV_ID
                      union all
                      Select  CAST(MAX( CAST(MAX_PROV.PROV_ID as BIGINT)) as VARCHAR(12)) as PROV_ID,  '3_Numeric_BCA_PROV_ID' as Type_Prov_Id  ,NTNL_PROV_ID
                      from {IDSOwner}.PROV MAX_PROV
                      JOIN {IDSOwner}.CD_MPPNG MPPNG ON MAX_PROV.SRC_SYS_CD_SK = MPPNG.CD_MPPNG_SK
                      where 
                      MAX_PROV.TERM_DT_SK >= '{CurrentDate}' and 
\t\t\t\t\t  TRANSLATE(PROV_ID,'','0123456789') ='' AND
                      MPPNG.SRC_CD = 'BCA'  AND NTNL_PROV_ID <>'NA' 
                      GROUP BY MAX_PROV.NTNL_PROV_ID
                      union all
                      Select  MAX(MAX_PROV.PROV_ID ) as PROV_ID,  '4_AlphaNumeric_BCA_PROV_ID' as Type_Prov_Id  ,NTNL_PROV_ID
                      from {IDSOwner}.PROV MAX_PROV
                      JOIN {IDSOwner}.CD_MPPNG MPPNG ON MAX_PROV.SRC_SYS_CD_SK = MPPNG.CD_MPPNG_SK
                      where 
                      MAX_PROV.TERM_DT_SK >= '{CurrentDate}' and 
\t\t\t\t\t  TRANSLATE(PROV_ID,'','0123456789') <>'' AND
                      MPPNG.SRC_CD = 'BCA'  AND NTNL_PROV_ID <>'NA' 
                      GROUP BY MAX_PROV.NTNL_PROV_ID
                      union all
                      Select  CAST(MAX( CAST(MAX_PROV.PROV_ID as BIGINT)) as VARCHAR(12)) as PROV_ID, '5_Numeric_NABP_PROV_ID' as Type_Prov_Id ,NTNL_PROV_ID
                      from {IDSOwner}.PROV MAX_PROV
                      JOIN {IDSOwner}.CD_MPPNG MPPNG ON MAX_PROV.SRC_SYS_CD_SK = MPPNG.CD_MPPNG_SK
                      where 
                      MAX_PROV.TERM_DT_SK >= '{CurrentDate}' and 
                      MPPNG.SRC_CD = 'NABP' AND
\t\t\t\t\t  TRANSLATE(PROV_ID,'','0123456789') =''  AND NTNL_PROV_ID <>'NA' 
                      GROUP BY MAX_PROV.NTNL_PROV_ID
                      union all
                      Select  MAX( MAX_PROV.PROV_ID ) as PROV_ID, '6_AlphaNumeric_NABP_PROV_ID' as Type_Prov_Id ,NTNL_PROV_ID
                      from {IDSOwner}.PROV MAX_PROV
                      JOIN {IDSOwner}.CD_MPPNG MPPNG ON MAX_PROV.SRC_SYS_CD_SK = MPPNG.CD_MPPNG_SK
                      where 
                      MAX_PROV.TERM_DT_SK >= '{CurrentDate}' and 
                      MPPNG.SRC_CD = 'NABP' AND
\t      TRANSLATE(PROV_ID,'','0123456789') <>''  AND NTNL_PROV_ID <>'NA' 
                      GROUP BY MAX_PROV.NTNL_PROV_ID
                      union all 
                      Select  CAST(MAX( CAST(MAX_PROV.PROV_ID as BIGINT)) as VARCHAR(12)) as  PROV_ID, '7_Numeric_MAX_PROV_ID' as Type_Prov_Id ,NTNL_PROV_ID
                      from {IDSOwner}.PROV MAX_PROV
                      where 
\t      TRANSLATE(PROV_ID,'','0123456789') ='' AND
                      NTNL_PROV_ID <>'NA' 
                      GROUP BY MAX_PROV.NTNL_PROV_ID
                      union all 
                      Select  MAX( MAX_PROV.PROV_ID) PROV_ID, '8_AlphaNumeric_MAX_PROV_ID' as Type_Prov_Id ,NTNL_PROV_ID
                      from {IDSOwner}.PROV MAX_PROV
                      where 
                      NTNL_PROV_ID <>'NA' 
                      GROUP BY MAX_PROV.NTNL_PROV_ID
               ) T
        ) T2 where rank = 1
)     MAX_PROV
ON MAX_PROV.NTNL_PROV_ID =  PROV.NTNL_PROV_ID 
where (case when TRANSLATE(PROV.PROV_ID,'','0123456789') = '' and PROV.NTNL_PROV_ID <>'NA'  
          then cast(CAST(PROV.PROV_ID as BIGINT) as varchar(12)) else PROV.PROV_ID end) = MAX_PROV.MAX_PROV_ID
and PROV.PROV_ID          not in ('UNK' , 'NA') 
AND DEA.CMN_PRCT_SK   not in (0, 1)
"""

df_ProvExtr_PidDeaCmnPrctProv1 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_PidDeaCmnPrctProv1)
    .load()
)

# -- Output pin 3 -> "PidNPIProv3"
query_PidNPIProv3 = f"""
SELECT  PROV.PROV_ID, 
        PROV.NTNL_PROV_ID,  
        PROV.TAX_ID
FROM 
{IDSOwner}.PROV PROV
WHERE PROV.PROV_ID <> 'NA' 
AND   PROV.PROV_ID <> 'UNK'
"""

df_ProvExtr_PidNPIProv3 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_PidNPIProv3)
    .load()
)

# -- Output pin 4 -> "PidNPINPI4"
query_PidNPINPI4 = f"""
SELECT  
     PROV.NTNL_PROV_ID as PROV_NTNL_PROV_ID, 
     PROV.PROV_ID,
     PROV.TAX_ID 
FROM 
{IDSOwner}.PROV PROV
Inner join
(
  select PROV_ID as MAX_PROV_ID , NTNL_PROV_ID from 
        (select PROV_ID,Type_Prov_Id,NTNL_PROV_ID ,
            RANK() OVER (PARTITION BY NTNL_PROV_ID order by Type_Prov_Id) as rank 
               FROM 
               (
                   Select CAST(MAX( CAST(MAX_PROV.PROV_ID as BIGINT)) as VARCHAR(12)) as  PROV_ID,  '1_Numeric_PROV_ID' as Type_Prov_Id ,NTNL_PROV_ID 
                    from {IDSOwner}.PROV MAX_PROV
                       JOIN {IDSOwner}.CD_MPPNG MPPNG ON MAX_PROV.SRC_SYS_CD_SK = MPPNG.CD_MPPNG_SK
                      where 
                      MAX_PROV.TERM_DT_SK >= '{CurrentDate}' and 
                      TRANSLATE(PROV_ID,'','0123456789') ='' AND
                      MPPNG.SRC_CD = 'FACETS'  AND NTNL_PROV_ID <>'NA' 
                      GROUP BY MAX_PROV.NTNL_PROV_ID
                      union all
                      Select MAX( MAX_PROV.PROV_ID) PROV_ID, '2_AlphaNumeric_PROV_ID' as  Type_Prov_Id ,NTNL_PROV_ID
                      from {IDSOwner}.PROV MAX_PROV
                      JOIN {IDSOwner}.CD_MPPNG MPPNG ON MAX_PROV.SRC_SYS_CD_SK = MPPNG.CD_MPPNG_SK
                      where 
                      MAX_PROV.TERM_DT_SK >= '{CurrentDate}' and 
                      TRANSLATE(PROV_ID,'','0123456789') <>'' AND
                      MPPNG.SRC_CD = 'FACETS'  AND NTNL_PROV_ID <>'NA' 
                      GROUP BY MAX_PROV.NTNL_PROV_ID
                      union all
                      Select  CAST(MAX( CAST(MAX_PROV.PROV_ID as BIGINT)) as VARCHAR(12)) as PROV_ID,  '3_Numeric_BCA_PROV_ID' as Type_Prov_Id  ,NTNL_PROV_ID
                      from {IDSOwner}.PROV MAX_PROV
                      JOIN {IDSOwner}.CD_MPPNG MPPNG ON MAX_PROV.SRC_SYS_CD_SK = MPPNG.CD_MPPNG_SK
                      where 
                      MAX_PROV.TERM_DT_SK >= '{CurrentDate}' and 
\t\t\t\t\t  TRANSLATE(PROV_ID,'','0123456789') ='' AND
                      MPPNG.SRC_CD = 'BCA'  AND NTNL_PROV_ID <>'NA' 
                      GROUP BY MAX_PROV.NTNL_PROV_ID
                      union all
                      Select  MAX(MAX_PROV.PROV_ID ) as PROV_ID,  '4_AlphaNumeric_BCA_PROV_ID' as Type_Prov_Id  ,NTNL_PROV_ID
                      from {IDSOwner}.PROV MAX_PROV
                      JOIN {IDSOwner}.CD_MPPNG MPPNG ON MAX_PROV.SRC_SYS_CD_SK = MPPNG.CD_MPPNG_SK
                      where 
                      MAX_PROV.TERM_DT_SK >= '{CurrentDate}' and 
\t\t\t\t\t  TRANSLATE(PROV_ID,'','0123456789') <>'' AND
                      MPPNG.SRC_CD = 'BCA'  AND NTNL_PROV_ID <>'NA' 
                      GROUP BY MAX_PROV.NTNL_PROV_ID
                      union all
                      Select  CAST(MAX( CAST(MAX_PROV.PROV_ID as BIGINT)) as VARCHAR(12)) as PROV_ID, '5_Numeric_NABP_PROV_ID' as Type_Prov_Id ,NTNL_PROV_ID
                      from {IDSOwner}.PROV MAX_PROV
                      JOIN {IDSOwner}.CD_MPPNG MPPNG ON MAX_PROV.SRC_SYS_CD_SK = MPPNG.CD_MPPNG_SK
                      where 
                      MAX_PROV.TERM_DT_SK >= '{CurrentDate}' and 
                      MPPNG.SRC_CD = 'NABP' AND
\t\t\t\t\t  TRANSLATE(PROV_ID,'','0123456789') =''  AND NTNL_PROV_ID <>'NA' 
                      GROUP BY MAX_PROV.NTNL_PROV_ID
                      union all
                      Select  MAX( MAX_PROV.PROV_ID ) as PROV_ID, '6_AlphaNumeric_NABP_PROV_ID' as Type_Prov_Id ,NTNL_PROV_ID
                      from {IDSOwner}.PROV MAX_PROV
                      JOIN {IDSOwner}.CD_MPPNG MPPNG ON MAX_PROV.SRC_SYS_CD_SK = MPPNG.CD_MPPNG_SK
                      where 
                      MAX_PROV.TERM_DT_SK >= '{CurrentDate}' and 
                      MPPNG.SRC_CD = 'NABP' AND
                      TRANSLATE(PROV_ID,'','0123456789') <>''  AND NTNL_PROV_ID <>'NA' 
                      GROUP BY MAX_PROV.NTNL_PROV_ID
                      union all 
                      Select  CAST(MAX( CAST(MAX_PROV.PROV_ID as BIGINT)) as VARCHAR(12)) as  PROV_ID, '7_Numeric_MAX_PROV_ID' as Type_Prov_Id ,NTNL_PROV_ID
                      from {IDSOwner}.PROV MAX_PROV
                      where 
\t      TRANSLATE(PROV_ID,'','0123456789') ='' AND
                      NTNL_PROV_ID <>'NA' 
                      GROUP BY MAX_PROV.NTNL_PROV_ID
                      union all 
                      Select  MAX( MAX_PROV.PROV_ID) PROV_ID, '8_AlphaNumeric_MAX_PROV_ID' as Type_Prov_Id ,NTNL_PROV_ID
                      from {IDSOwner}.PROV MAX_PROV
                      where 
                      NTNL_PROV_ID <>'NA' 
                      GROUP BY MAX_PROV.NTNL_PROV_ID
               ) T
        ) T2 where rank = 1
)     MAX_PROV
ON MAX_PROV.NTNL_PROV_ID =  PROV.NTNL_PROV_ID 
where LENGTH ( TRIM (PROV.NTNL_PROV_ID)) = 10 
  and (case when TRANSLATE(PROV.PROV_ID,'','0123456789') = '' and PROV.NTNL_PROV_ID <>'NA'  
          then cast(CAST(PROV.PROV_ID as BIGINT) as varchar(12)) else PROV.PROV_ID end) = MAX_PROV.MAX_PROV_ID
  AND  PROV.PROV_ID <> 'UNK'   
  AND  PROV.PROV_ID <> 'NA'
"""

df_ProvExtr_PidNPINPI4 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_PidNPINPI4)
    .load()
)

# -- Output pin 5 -> "PrscrbDeaNPI5"
query_PrscrbDeaNPI5 = f"""
SELECT 
DEA.NTNL_PROV_ID
FROM 
{IDSOwner}.PROV_DEA DEA
"""

df_ProvExtr_PrscrbDeaNPI5 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_PrscrbDeaNPI5)
    .load()
)

# ------------------------------------------------------------------------------
# CHashedFileStage: hf_bcbskccomm_prscrb_prov_dea2
# Scenario A (intermediate), Key column: DEA_NO
# ------------------------------------------------------------------------------
df_hf_bcbskccomm_prscrb_prov_dea2 = dedup_sort(
    df_ProvExtr_PidDeaProvNbr2,
    ["DEA_NO"],
    [("DEA_NO","A")]
)

# ------------------------------------------------------------------------------
# CHashedFileStage: hf_bcbskccomm_prscrb_prov_dea1
# Scenario A, Key column: DEA_NO
# ------------------------------------------------------------------------------
df_hf_bcbskccomm_prscrb_prov_dea1 = dedup_sort(
    df_ProvExtr_PidDeaCmnPrctProv1,
    ["DEA_NO"],
    [("DEA_NO","A")]
)

# ------------------------------------------------------------------------------
# CHashedFileStage: hf_bcbskccomm_clm_prov_prscrb3
# Scenario C (written & read as parquet). Key column: PROV_ID
# ------------------------------------------------------------------------------
df_hf_bcbskccomm_clm_prov_prscrb3_write = dedup_sort(
    df_ProvExtr_PidNPIProv3,
    ["PROV_ID"],
    [("PROV_ID","A")]
)

write_files(
    df_hf_bcbskccomm_clm_prov_prscrb3_write,
    "hf_bcbskccomm_clm_prov_prscrb3.parquet",
    delimiter=",",
    mode="overwrite",
    is_parqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# Read back the same data for reference links
df_hf_bcbskccomm_clm_prov_prscrb3_read = spark.read.parquet("hf_bcbskccomm_clm_prov_prscrb3.parquet")

# We will create separate views (in-memory DataFrames) for each output pin
# PidNPIProvLkup3 => (PROV_ID, PROV_NTNL_PROV_ID, TAX_ID)
df_PidNPIProvLkup3 = df_hf_bcbskccomm_clm_prov_prscrb3_read.select(
    F.col("PROV_ID"),
    F.col("NTNL_PROV_ID").alias("PROV_NTNL_PROV_ID"),
    F.col("TAX_ID")
)

# PnpiNPIProvLkup5 => (PROV_ID, PROV_NTNL_PROV_ID, TAX_ID)
df_PnpiNPIProvLkup5 = df_hf_bcbskccomm_clm_prov_prscrb3_read.select(
    F.col("PROV_ID"),
    F.col("NTNL_PROV_ID").alias("PROV_NTNL_PROV_ID"),
    F.col("TAX_ID")
)

# SvcProvIdLkup1 => (PROV_ID, NTNL_PROV_ID, TAX_ID)
df_SvcProvIdLkup1 = df_hf_bcbskccomm_clm_prov_prscrb3_read.select(
    F.col("PROV_ID"),
    F.col("NTNL_PROV_ID").alias("NTNL_PROV_ID"),
    F.col("TAX_ID")
)

# ------------------------------------------------------------------------------
# CHashedFileStage: hf_bcbskccomm_clm_prov_prscrb4
# Scenario C. Key column: PROV_NTNL_PROV_ID
# ------------------------------------------------------------------------------
df_hf_bcbskccomm_clm_prov_prscrb4_write = dedup_sort(
    df_ProvExtr_PidNPINPI4,
    ["PROV_NTNL_PROV_ID"],
    [("PROV_NTNL_PROV_ID","A")]
)

write_files(
    df_hfbcbskccomm_clm_prov_prscrb4_write := df_hf_bcbskccomm_clm_prov_prscrb4_write,  # inline assignment just to keep naming consistent
    "hf_bcbskccomm_clm_prov_prscrb4.parquet",
    delimiter=",",
    mode="overwrite",
    is_parqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_hf_bcbskccomm_clm_prov_prscrb4_read = spark.read.parquet("hf_bcbskccomm_clm_prov_prscrb4.parquet")

# Pin outputs:
# PidNPINPILkup4 => (PROV_NTNL_PROV_ID, PROV_ID, TAX_ID)
df_PidNPINPILkup4 = df_hf_bcbskccomm_clm_prov_prscrb4_read.select(
    F.col("PROV_NTNL_PROV_ID"),
    F.col("PROV_ID"),
    F.col("TAX_ID")
)
# PnpiNPINPILkup6 => (PROV_NTNL_PROV_ID, PROV_ID, TAX_ID)
df_PnpiNPINPILkup6 = df_hf_bcbskccomm_clm_prov_prscrb4_read.select(
    F.col("PROV_NTNL_PROV_ID"),
    F.col("PROV_ID"),
    F.col("TAX_ID")
)
# SvcPdxNoToNPILkp3 => (PROV_NTNL_PROV_ID, PROV_ID, TAX_ID)
df_SvcPdxNoToNPILkp3 = df_hf_bcbskccomm_clm_prov_prscrb4_read.select(
    F.col("PROV_NTNL_PROV_ID"),
    F.col("PROV_ID"),
    F.col("TAX_ID")
)
# SvcNtnlProvIdLkup2 => (NTNL_PROV_ID, PROV_ID, TAX_ID)
df_SvcNtnlProvIdLkup2 = df_hf_bcbskccomm_clm_prov_prscrb4_read.select(
    F.col("PROV_NTNL_PROV_ID").alias("NTNL_PROV_ID"),
    F.col("PROV_ID"),
    F.col("TAX_ID")
)

# ------------------------------------------------------------------------------
# CHashedFileStage: hf_bcbskccomm_clm_prov_prscrb5
# Scenario C. Key column: DEA_NTNL_PROV_ID
# ------------------------------------------------------------------------------
df_hf_bcbskccomm_clm_prov_prscrb5_write = dedup_sort(
    df_ProvExtr_PrscrbDeaNPI5,
    ["NTNL_PROV_ID"],
    [("NTNL_PROV_ID","A")]
)

write_files(
    df_hcbskccomm_clm_prov_prscrb5_write := df_hf_bcbskccomm_clm_prov_prscrb5_write,
    "hf_bcbskccomm_clm_prov_prscrb5.parquet",
    delimiter=",",
    mode="overwrite",
    is_parqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_hf_bcbskccomm_clm_prov_prscrb5_read = spark.read.parquet("hf_bcbskccomm_clm_prov_prscrb5.parquet")

# Output pin: PrscrbDeaNPILkup5 => (DEA_NTNL_PROV_ID)
df_PrscrbDeaNPILkup5 = df_hf_bcbskccomm_clm_prov_prscrb5_read.select(
    F.col("NTNL_PROV_ID").alias("DEA_NTNL_PROV_ID")
)

# ------------------------------------------------------------------------------
# Stage: MbrHmoExtr (DB2Connector, Database=IDS)
# Two output pins
# ------------------------------------------------------------------------------
query_HmoExtr = f"""
SELECT
DRVR.MBR_UNIQ_KEY,
DRVR.DRUG_FILL_DT_SK,
MAX(MBR_ENR.EFF_DT_SK)
FROM
{IDSOwner}.W_DRUG_CLM_PCP DRVR, 
{IDSOwner}.MBR_ENR MBR_ENR,
{IDSOwner}.CD_MPPNG MAP1,
{IDSOwner}.PROD PROD,
{IDSOwner}.PROD_SH_NM PRODSH,
{IDSOwner}.CD_MPPNG MAP2
WHERE   DRVR.MBR_UNIQ_KEY=  MBR_ENR.MBR_UNIQ_KEY 
AND     MBR_ENR.SRC_SYS_CD_SK  = MAP1.CD_MPPNG_SK
AND     MAP1.TRGT_CD  = 'FACETS'
AND     DRVR.DRUG_FILL_DT_SK BETWEEN MBR_ENR.EFF_DT_SK AND MBR_ENR.TERM_DT_SK 
AND     MBR_ENR.ELIG_IN='Y'
AND     MBR_ENR.PROD_SK=PROD.PROD_SK 
AND     PROD.PROD_SH_NM_SK=PRODSH.PROD_SH_NM_SK 
AND     PRODSH.PROD_SH_NM_DLVRY_METH_CD_SK=MAP2.CD_MPPNG_SK 
AND     MAP2.TRGT_CD =  'HMO'
GROUP BY
DRVR.MBR_UNIQ_KEY,
DRVR.DRUG_FILL_DT_SK
"""

df_MbrHmoExtr_HmoExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_HmoExtr)
    .load()
)

query_MbrPcpEffDt = f"""
SELECT 
DRVR.MBR_UNIQ_KEY,
DRVR.DRUG_FILL_DT_SK,
MAX(MBR_PCP.EFF_DT_SK),
PROV.PROV_ID,
PROV.NTNL_PROV_ID,
PROV.TAX_ID,
MPPNG.TRGT_CD
FROM 
{IDSOwner}.W_DRUG_CLM_PCP DRVR,
{IDSOwner}.MBR_PCP MBR_PCP,
{IDSOwner}.CD_MPPNG MPPNG,
{IDSOwner}.PROV PROV
WHERE 
DRVR.MBR_UNIQ_KEY=MBR_PCP.MBR_UNIQ_KEY AND
DRVR.DRUG_FILL_DT_SK BETWEEN MBR_PCP.EFF_DT_SK AND MBR_PCP.TERM_DT_SK  AND
MBR_PCP.MBR_PCP_TYP_CD_SK=MPPNG.CD_MPPNG_SK  AND
MBR_PCP.PROV_SK=PROV.PROV_SK 
GROUP BY 
DRVR.MBR_UNIQ_KEY,
DRVR.DRUG_FILL_DT_SK,
PROV.PROV_ID,
PROV.NTNL_PROV_ID,
PROV.TAX_ID,
MPPNG.TRGT_CD
"""

df_MbrHmoExtr_MbrPcpEffDt = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_MbrPcpEffDt)
    .load()
)

# ------------------------------------------------------------------------------
# CHashedFileStage: hf_bcbskccomm_clm_mbr_pcp
# Scenario C
# Key columns: MBR_UNIQ_KEY, ARGUS_FILL_DT_SK
# ------------------------------------------------------------------------------
df_hf_bcbskccomm_clm_mbr_pcp_write = dedup_sort(
    df_MbrHmoExtr_MbrPcpEffDt,
    ["MBR_UNIQ_KEY","DRUG_FILL_DT_SK"],  # The link calls ARGUS_FILL_DT_SK but in DS the third column is DRUG_FILL_DT_SK
    [("MBR_UNIQ_KEY","A"),("DRUG_FILL_DT_SK","A")]
)

write_files(
    df_hf_bcbskccomm_clm_mbr_pcp_write,
    "hf_bcbskccomm_clm_mbr_pcp.parquet",
    delimiter=",",
    mode="overwrite",
    is_parqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_hf_bcbskccomm_clm_mbr_pcp_read = spark.read.parquet("hf_bcbskccomm_clm_mbr_pcp.parquet")

# ------------------------------------------------------------------------------
# Stage: Transform (CTransformerStage)
#   PrimaryLink input: df_MbrHmoExtr_HmoExtr as "HmoExtr"
#   LookupLink input: df_hf_bcbskccomm_clm_mbr_pcp_read as "MbrPcpLkup"
#   Left join on (HmoExtr.MBR_UNIQ_KEY = MbrPcpLkup.MBR_UNIQ_KEY AND 
#                 HmoExtr.DRUG_FILL_DT_SK = MbrPcpLkup.DRUG_FILL_DT_SK) 
#   plus DS references an EFF_DT_SK join, which the group query gave as "MAX(MBR_PCP.EFF_DT_SK)" 
#   so in df MbrPcpLkup it is that third column. We will rename for join.
# ------------------------------------------------------------------------------
df_HmoExtr = df_MbrHmoExtr_HmoExtr.select(
    F.col("MBR_UNIQ_KEY"),
    F.col("DRUG_FILL_DT_SK").alias("ARGUS_FILL_DT_SK"),
    F.col("MAX(MBR_ENR.EFF_DT_SK)").alias("EFF_DT_SK")
)

df_MbrPcpLkup = df_hf_bcbskccomm_clm_mbr_pcp_read.select(
    F.col("MBR_UNIQ_KEY"),
    F.col("DRUG_FILL_DT_SK").alias("ARGUS_FILL_DT_SK"),
    F.col("MAX(MBR_PCP.EFF_DT_SK)").alias("EFF_DT_SK"),
    F.col("PROV_ID"),
    F.col("NTNL_PROV_ID"),
    F.col("TAX_ID"),
    F.col("TRGT_CD")
)

df_Transform_joined = (
    df_HmoExtr.alias("HmoExtr")
    .join(
        df_MbrPcpLkup.alias("MbrPcpLkup"),
        on=[
            F.col("HmoExtr.MBR_UNIQ_KEY")==F.col("MbrPcpLkup.MBR_UNIQ_KEY"),
            F.col("HmoExtr.ARGUS_FILL_DT_SK")==F.col("MbrPcpLkup.ARGUS_FILL_DT_SK"),
            F.col("HmoExtr.EFF_DT_SK")==F.col("MbrPcpLkup.EFF_DT_SK")
        ],
        how="left"
    )
)

# Produce output columns: "PCP" link
# According to DS code, we apply expressions; similarly replicate:
df_Transform_PCP = df_Transform_joined.select(
    F.col("HmoExtr.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("HmoExtr.ARGUS_FILL_DT_SK").alias("ARGUS_FILL_DT_SK"),
    F.when( (F.col("MbrPcpLkup.PROV_ID").isNull()) | (F.length(F.trim(F.col("MbrPcpLkup.PROV_ID")))==0), F.lit("UNK")).otherwise(F.col("MbrPcpLkup.PROV_ID")).alias("PROV_ID"),
    F.when( (F.col("MbrPcpLkup.TAX_ID").isNull()) | (F.length(F.trim(F.col("MbrPcpLkup.TAX_ID")))==0), F.lit("NA")).otherwise(F.col("MbrPcpLkup.TAX_ID")).alias("TAX_ID"),
    F.when(
        (F.col("MbrPcpLkup.TRGT_CD").isNull()) 
        | (F.length(F.trim(F.col("MbrPcpLkup.TRGT_CD")))==0) 
        | (F.col("MbrPcpLkup.TRGT_CD")!=F.lit("PRI")), 
        F.lit("UNK")
    ).otherwise(F.lit("PCP")).alias("TRGT_CD"),
    F.col("MbrPcpLkup.NTNL_PROV_ID").alias("NTNL_PROV_ID")
)

# ------------------------------------------------------------------------------
# CHashedFileStage: hf_bcbskccomm_clm_hmo_mbr_pcp (Scenario C)
# Key Columns: MBR_UNIQ_KEY, ARGUS_FILL_DT_SK
# ------------------------------------------------------------------------------
df_hf_bcbskccomm_clm_hmo_mbr_pcp_write = dedup_sort(
    df_Transform_PCP,
    ["MBR_UNIQ_KEY","ARGUS_FILL_DT_SK"],
    [("MBR_UNIQ_KEY","A"),("ARGUS_FILL_DT_SK","A")]
)

write_files(
    df_hcbskccomm_clm_hmo_mbr_pcp_write := df_hf_bcbskccomm_clm_hmo_mbr_pcp_write,
    "hf_bcbskccomm_clm_hmo_mbr_pcp.parquet",
    delimiter=",",
    mode="overwrite",
    is_parqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_hf_bcbskccomm_clm_hmo_mbr_pcp_read = spark.read.parquet("hf_bcbskccomm_clm_hmo_mbr_pcp.parquet")

# ------------------------------------------------------------------------------
# Stage: BusinessRules (CTransformerStage)
# Multiple reference lookups against df_hf_bcbskccomm_clm_hmo_mbr_pcp_read, 
# df_hf_bcbskccomm_clm_prov_prscrb3_read, df_hf_bcbskccomm_clm_prov_prscrb4_read, etc.
# Primary link: df_Extract
# ------------------------------------------------------------------------------
# First, define base as df_Extract
df_br_base = df_Extract.alias("Extract")

# Left join with PCPLkup (hf_bcbskccomm_clm_hmo_mbr_pcp)
df_PCPLkup = df_hf_bcbskccomm_clm_hmo_mbr_pcp_read.select(
    F.col("MBR_UNIQ_KEY"),
    F.col("ARGUS_FILL_DT_SK"),
    F.col("PROV_ID"),
    F.col("TAX_ID"),
    F.col("TRGT_CD"),
    F.col("NTNL_PROV_ID")
).alias("PCPLkup")

joined_br = (
    df_br_base
    .join(
        df_PCPLkup,
        on=[
            df_br_base["ORIG_MBR"]==df_PCPLkup["MBR_UNIQ_KEY"],
            df_br_base["FILL_DT"]==df_PCPLkup["ARGUS_FILL_DT_SK"]
        ],
        how="left"
    )
)

# Then join all the reference links from the hashed-file dataframes, each with left join:
df_SvcProvIdLkup1_alias = df_SvcProvIdLkup1.alias("SvcProvIdLkup1")
joined_br = joined_br.join(
    df_SvcProvIdLkup1_alias,
    on=[joined_br["Extract.PDX_NO"]==df_SvcProvIdLkup1_alias["PROV_ID"]],
    how="left"
)

df_SvcNtnlProvIdLkup2_alias = df_SvcNtnlProvIdLkup2.alias("SvcNtnlProvIdLkup2")
joined_br = joined_br.join(
    df_SvcNtnlProvIdLkup2_alias,
    on=[joined_br["Extract.PDX_NTNL_PROV_ID"]==df_SvcNtnlProvIdLkup2_alias["NTNL_PROV_ID"]],
    how="left"
)

df_PidDeaProvNbrLkup2_alias = df_hf_bcbskccomm_prscrb_prov_dea2.alias("PidDeaProvNbrLkup2")
joined_br = joined_br.join(
    df_PidDeaProvNbrLkup2_alias,
    on=[joined_br["Extract.PRSCRBR_ID"]==df_PidDeaProvNbrLkup2_alias["DEA_NO"]],
    how="left"
)

df_PidDeaCmnPrctProvLkup1_alias = df_hf_bcbskccomm_prscrb_prov_dea1.alias("PidDeaCmnPrctProvLkup1")
joined_br = joined_br.join(
    df_PidDeaCmnPrctProvLkup1_alias,
    on=[joined_br["Extract.PRSCRBR_ID"]==df_PidDeaCmnPrctProvLkup1_alias["DEA_NO"]],
    how="left"
)

df_PidNPIProvLkup3_alias = df_PidNPIProvLkup3.alias("PidNPIProvLkup3")
joined_br = joined_br.join(
    df_PidNPIProvLkup3_alias,
    on=[joined_br["Extract.PRSCRBR_ID"]==df_PidNPIProvLkup3_alias["PROV_ID"]],
    how="left"
)

df_PidNPINPILkup4_alias = df_PidNPINPILkup4.alias("PidNPINPILkup4")
joined_br = joined_br.join(
    df_PidNPINPILkup4_alias,
    on=[joined_br["Extract.PRSCRBR_ID"]==df_PidNPINPILkup4_alias["PROV_NTNL_PROV_ID"]],
    how="left"
)

df_SvcPdxNoToNPILkp3_alias = df_SvcPdxNoToNPILkp3.alias("SvcPdxNoToNPILkp3")
joined_br = joined_br.join(
    df_SvcPdxNoToNPILkp3_alias,
    on=[joined_br["Extract.PDX_NO"]==df_SvcPdxNoToNPILkp3_alias["PROV_NTNL_PROV_ID"]],
    how="left"
)

df_PrscrbDeaNPILkup5_alias = df_PrscrbDeaNPILkup5.alias("PrscrbDeaNPILkup5")
joined_br = joined_br.join(
    df_PrscrbDeaNPILkup5_alias,
    on=[joined_br["Extract.PRSCRBR_ID"]==df_PrscrbDeaNPILkup5_alias["DEA_NTNL_PROV_ID"]],
    how="left"
)

df_PnpiNPIProvLkup5_alias = df_PnpiNPIProvLkup5.alias("PnpiNPIProvLkup5")
joined_br = joined_br.join(
    df_PnpiNPIProvLkup5_alias,
    on=[joined_br["Extract.PRSCRBR_NTNL_PROV_ID"]==df_PnpiNPIProvLkup5_alias["PROV_ID"]],
    how="left"
)

df_PnpiNPINPILkup6_alias = df_PnpiNPINPILkup6.alias("PnpiNPINPILkup6")
joined_br = joined_br.join(
    df_PnpiNPINPILkup6_alias,
    on=[joined_br["Extract.PRSCRBR_NTNL_PROV_ID"]==df_PnpiNPINPILkup6_alias["PROV_NTNL_PROV_ID"]],
    how="left"
)

# According to DataStage, define stage variables:
# We'll mimic them as columns for final output manipulations.
# svClmId = Extract.CLM_ID
# svPkString = SrcSysCd + ";" + svClmId
# etc. We'll directly compute them in a select expression.

# We replicate each expression from the StageVariables:
df_br_enriched = joined_br.select(
    # We'll pick all columns from "Extract" that we might need. Then the transformer variables:
    F.col("Extract.*"),
    F.col("SrcSysCd").alias("SrcSysCd"),  # from parameter, or can use literal?
    (F.col("Extract.CLM_ID")).alias("ClmId"),
    (
        F.lit("") +
        F.col("SrcSysCd") + 
        F.lit(";") + 
        F.col("Extract.CLM_ID")
    ).alias("PkString"),
    F.lit(CurrentDate).alias("CurDateTime"),
    F.lit("Y").alias("PassThru"),
    # Svc Prov ID expression:
    F.when(
        (F.col("SvcProvIdLkup1.PROV_ID").isNotNull()),
        F.col("Extract.PDX_NO")
    ).otherwise(
        F.when(
            (F.col("SvcNtnlProvIdLkup2.PROV_ID").isNotNull()),
            F.trim(F.col("SvcNtnlProvIdLkup2.PROV_ID"))
        ).otherwise(
            F.when(
                (F.col("SvcPdxNoToNPILkp3.PROV_ID").isNotNull()),
                F.trim(F.col("SvcPdxNoToNPILkp3.PROV_ID"))
            ).otherwise(
                F.when(
                    (F.length(F.trim(F.col("Extract.PDX_NO")))>0) & (F.trim(F.col("Extract.PDX_NO"))!=F.lit("NA")),
                    F.trim(F.col("Extract.PDX_NO"))
                ).otherwise(
                    F.when(
                        F.expr("ProviderNPIValidator(Extract.PDX_NTNL_PROV_ID) = 'Y'"),
                        F.col("Extract.PDX_NTNL_PROV_ID")
                    ).otherwise(
                        F.when(
                            F.length(F.trim(F.col("Extract.PDX_NO")))>0,
                            F.trim(F.col("Extract.PDX_NO"))
                        ).otherwise(
                            F.when(
                                F.length(F.trim(F.col("Extract.PDX_NTNL_PROV_ID")))>0,
                                F.trim(F.col("Extract.PDX_NTNL_PROV_ID"))
                            ).otherwise(F.lit("NA"))
                        )
                    )
                )
            )
        )
    ).alias("svSvcProvID"),
    # Svc Ntnl Prov ID expression:
    F.when(
        F.expr("ProviderNPIValidator(Extract.PDX_NTNL_PROV_ID) = 'Y'"),
        F.col("Extract.PDX_NTNL_PROV_ID")
    ).otherwise(
        F.when(
            (F.col("SvcProvIdLkup1.NTNL_PROV_ID").isNotNull()) & (F.expr("ProviderNPIValidator(SvcProvIdLkup1.NTNL_PROV_ID) = 'Y'")),
            F.col("SvcProvIdLkup1.NTNL_PROV_ID")
        ).otherwise(
            F.when(
                F.expr("ProviderNPIValidator(Extract.PDX_NO) = 'Y'"),
                F.col("Extract.PDX_NO")
            ).otherwise(F.lit("NA"))
        )
    ).alias("svSvcNtnlProvId"),
    # PrscrbIDDEA = ProviderDEAValidator(Trim(Extract.PRSCRBR_ID)) => we'll just do a lit placeholder check (since we assume the function is available).
    F.expr("ProviderDEAValidator(trim(Extract.PRSCRBR_ID))").alias("svPrscrbIDDEA"),
    # PrscrbIDNPI = ProviderNPIValidator(Trim(Extract.PRSCRBR_ID))
    F.expr("ProviderNPIValidator(trim(Extract.PRSCRBR_ID))").alias("svPrscrbIDNPI"),
    # svPrscrbNtnlProvIDNPI = ProviderNPIValidator(Trim(Extract.PRSCRBR_NTNL_PROV_ID))
    F.expr("ProviderNPIValidator(trim(Extract.PRSCRBR_NTNL_PROV_ID))").alias("svPrscrbNtnlProvIDNPI"),
    # svPrscrbrProviderIDtoUse
    F.when(
        (F.col("svPrscrbIDDEA")==F.lit("Y")) & (F.col("PidDeaCmnPrctProvLkup1.DEA_NO").isNotNull()),
        F.trim(F.col("PidDeaCmnPrctProvLkup1.PROV_ID"))
    ).otherwise(
        F.when(
            (F.col("svPrscrbIDDEA")==F.lit("Y")) & (F.col("PidDeaProvNbrLkup2.DEA_NO").isNotNull()),
            F.trim(F.col("PidDeaProvNbrLkup2.PROV_ID"))
        ).otherwise(
            F.when(
                (F.col("svPrscrbIDNPI")==F.lit("Y")) & (F.col("PidNPIProvLkup3.PROV_ID").isNotNull()),
                F.col("PidNPIProvLkup3.PROV_ID")
            ).otherwise(
                F.when(
                    (F.col("svPrscrbIDNPI")==F.lit("Y")) & (F.col("PidNPINPILkup4.PROV_NTNL_PROV_ID").isNotNull()),
                    F.col("PidNPINPILkup4.PROV_ID")
                ).otherwise(
                    F.when(
                        (F.col("svPrscrbNtnlProvIDNPI")==F.lit("Y")) & (F.col("PnpiNPIProvLkup5.PROV_ID").isNotNull()),
                        F.col("PnpiNPIProvLkup5.PROV_ID")
                    ).otherwise(
                        F.when(
                            (F.col("svPrscrbNtnlProvIDNPI")==F.lit("Y")) & (F.col("PnpiNPINPILkup6.PROV_NTNL_PROV_ID").isNotNull()),
                            F.col("PnpiNPINPILkup6.PROV_ID")
                        ).otherwise(
                            F.when(
                                (F.col("svPrscrbIDDEA")==F.lit("Y")) 
                                & (F.col("PrscrbDeaNPILkup5.DEA_NTNL_PROV_ID").isNotNull())
                                & (F.expr("ProviderNPIValidator(trim(PrscrbDeaNPILkup5.DEA_NTNL_PROV_ID)) = 'Y'")),
                                F.trim(F.col("PrscrbDeaNPILkup5.DEA_NTNL_PROV_ID"))
                            ).otherwise(
                                F.when(
                                    F.col("svPrscrbIDNPI")==F.lit("Y"),
                                    F.col("Extract.PRSCRBR_ID")
                                ).otherwise(
                                    F.when(
                                        F.col("svPrscrbNtnlProvIDNPI")==F.lit("Y"),
                                        F.col("Extract.PRSCRBR_NTNL_PROV_ID")
                                    ).otherwise(F.lit("NA"))
                                )
                            )
                        )
                    )
                )
            )
        )
    ).alias("svPrscrbrProviderIDtoUse"),
    # svPrscrbNtnlProvIDtoUse
    F.when(
        F.col("svPrscrbNtnlProvIDNPI")==F.lit("Y"),
        F.col("Extract.PRSCRBR_NTNL_PROV_ID")
    ).otherwise(
        F.when(
            F.col("svPrscrbIDNPI")==F.lit("Y"),
            F.col("Extract.PRSCRBR_ID")
        ).otherwise(
            F.when(
                (F.col("svPrscrbIDDEA")==F.lit("Y")) 
                & (F.col("PidDeaCmnPrctProvLkup1.DEA_NO").isNotNull())
                & (F.expr("ProviderNPIValidator(PidDeaCmnPrctProvLkup1.DEA_NTNL_PROV_ID) = 'Y'")),
                F.col("PidDeaCmnPrctProvLkup1.DEA_NTNL_PROV_ID")
            ).otherwise(
                F.when(
                    (F.col("svPrscrbIDDEA")==F.lit("Y")) 
                    & (F.col("PidDeaCmnPrctProvLkup1.DEA_NO").isNotNull())
                    & (F.expr("ProviderNPIValidator(PidDeaCmnPrctProvLkup1.PROV_NTNL_PROV_ID) = 'Y'")),
                    F.col("PidDeaCmnPrctProvLkup1.PROV_NTNL_PROV_ID")
                ).otherwise(
                    F.when(
                        (F.col("svPrscrbIDDEA")==F.lit("Y"))
                        & (F.col("PidDeaProvNbrLkup2.DEA_NO").isNotNull())
                        & (F.expr("ProviderNPIValidator(PidDeaProvNbrLkup2.DEA_NTNL_PROV_ID) = 'Y'")),
                        F.col("PidDeaProvNbrLkup2.DEA_NTNL_PROV_ID")
                    ).otherwise(F.lit("NA"))
                )
            )
        )
    ).alias("svPrscrbNtnlProvIDtoUse"),
    # svPrscrbTaxIDtoUse
    F.when(
        (F.col("svPrscrbIDDEA")==F.lit("Y")) & (F.col("PidDeaCmnPrctProvLkup1.DEA_NO").isNotNull()),
        F.trim(F.col("PidDeaCmnPrctProvLkup1.TAX_ID"))
    ).otherwise(
        F.when(
            (F.col("svPrscrbIDDEA")==F.lit("Y")) & (F.col("PidDeaProvNbrLkup2.DEA_NO").isNotNull()),
            F.trim(F.col("PidDeaProvNbrLkup2.TAX_ID"))
        ).otherwise(
            F.when(
                (F.col("svPrscrbIDNPI")==F.lit("Y")) & (F.col("PidNPIProvLkup3.PROV_ID").isNotNull()),
                F.col("PidNPIProvLkup3.TAX_ID")
            ).otherwise(
                F.when(
                    (F.col("svPrscrbIDNPI")==F.lit("Y")) & (F.col("PidNPINPILkup4.PROV_NTNL_PROV_ID").isNotNull()),
                    F.col("PidNPINPILkup4.TAX_ID")
                ).otherwise(
                    F.when(
                        (F.col("svPrscrbNtnlProvIDNPI")==F.lit("Y")) & (F.col("PnpiNPIProvLkup5.PROV_ID").isNotNull()),
                        F.col("PnpiNPIProvLkup5.TAX_ID")
                    ).otherwise(
                        F.when(
                            (F.col("svPrscrbNtnlProvIDNPI")==F.lit("Y")) & (F.col("PnpiNPINPILkup6.PROV_NTNL_PROV_ID").isNotNull()),
                            F.col("PnpiNPINPILkup6.TAX_ID")
                        ).otherwise(
                            F.lit("NA")
                        )
                    )
                )
            )
        )
    ).alias("svPrscrbTaxIDtoUse")
)

# From df_br_enriched, create 3 output sets: SVC, PCP, PRSCRB.
# Each link in DS has a set of columns:
#   SVC => "SVC"
df_BR_SVC = df_br_enriched.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.col("PassThru").alias("PASS_THRU_IN"),
    F.col("CurDateTime").alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.col("SrcSysCd").alias("SRC_SYS_CD"),
    (F.col("PkString") + F.lit(";") + F.lit("SVC")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_PROV_SK"),
    F.col("ClmId").cast(StringType()).alias("CLM_ID"),
    F.lit("SVC").alias("CLM_PROV_ROLE_TYP_CD"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("svSvcProvID").cast(StringType()).alias("PROV_ID"),
    F.lit("NA").alias("TAX_ID"),
    F.col("svSvcNtnlProvId").alias("NTNL_PROV_ID")
)

#   PCP => only rows with Not(IsNull(PCPLkup.MBR_UNIQ_KEY)) And Len(Trim(PCPLkup.MBR_UNIQ_KEY)) > 0
df_BR_PCP_filtered = df_br_enriched.filter(
    (F.col("PCPLkup.MBR_UNIQ_KEY").isNotNull()) & (F.length(F.trim(F.col("PCPLkup.MBR_UNIQ_KEY")))>0)
)

df_BR_PCP = df_BR_PCP_filtered.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.col("PassThru").alias("PASS_THRU_IN"),
    F.col("CurDateTime").alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.col("SrcSysCd").alias("SRC_SYS_CD"),
    (F.col("PkString") + F.lit(";") + F.lit("PCP")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_PROV_SK"),
    F.col("ClmId").cast(StringType()).alias("CLM_ID"),
    F.lit("PCP").alias("CLM_PROV_ROLE_TYP_CD"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(
        (F.col("PCPLkup.PROV_ID").isNull()) | (F.length(F.trim(F.col("PCPLkup.PROV_ID")))==0),
        F.lit("NA")
    ).otherwise(F.trim(F.col("PCPLkup.PROV_ID"))).cast(StringType()).alias("PROV_ID"),
    F.when(
        (F.col("PCPLkup.TAX_ID").isNull()) | (F.length(F.trim(F.col("PCPLkup.TAX_ID")))==0),
        F.lit("NA")
    ).otherwise(F.trim(F.col("PCPLkup.TAX_ID"))).alias("TAX_ID"),
    F.when(
        (F.col("PCPLkup.NTNL_PROV_ID").isNotNull()) & (F.expr("ProviderNPIValidator(PCPLkup.NTNL_PROV_ID) = 'Y'")),
        F.col("PCPLkup.NTNL_PROV_ID")
    ).otherwise(F.lit("NA")).alias("NTNL_PROV_ID")
)

#   PRSCRB
df_BR_PRSCRB = df_br_enriched.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.col("PassThru").alias("PASS_THRU_IN"),
    F.col("CurDateTime").alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.col("SrcSysCd").alias("SRC_SYS_CD"),
    (F.col("PkString") + F.lit(";") + F.lit("PRSCRB")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_PROV_SK"),
    F.col("ClmId").cast(StringType()).alias("CLM_ID"),
    F.lit("PRSCRB").alias("CLM_PROV_ROLE_TYP_CD"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("svPrscrbrProviderIDtoUse").cast(StringType()).alias("PROV_ID"),
    F.col("svPrscrbTaxIDtoUse").cast(StringType()).alias("TAX_ID"),
    F.col("svPrscrbNtnlProvIDtoUse").alias("NTNL_PROV_ID")
)

# ------------------------------------------------------------------------------
# Stage: Link_Collector (CCollector) Round-Robin => combine SVC, PCP, PRSCRB
# ------------------------------------------------------------------------------
df_link_collector = df_BR_SVC.unionByName(df_BR_PCP).unionByName(df_BR_PRSCRB)

# ------------------------------------------------------------------------------
# Stage: Snapshot (CTransformerStage)
# Input: df_link_collector
# 2 output pins: "SnapShot" (with 3 columns) and "AllCol" (with many columns)
# ------------------------------------------------------------------------------
df_Snapshot_input = df_link_collector.alias("Trans")

df_Snapshot = df_Snapshot_input.select(
    F.col("Trans.CLM_ID").cast(StringType()).alias("CLM_ID"),
    F.col("Trans.CLM_PROV_ROLE_TYP_CD").alias("CLM_PROV_ROLE_TYP_CD"),
    F.col("Trans.PROV_ID").alias("PROV_ID")
)

df_AllCol = df_Snapshot_input.select(
    F.lit("").alias("SrcSysCdSk").alias("SRC_SYS_CD_SK"),  # DataStage references "SrcSysCdSk" from outside; here we store in a column
    F.col("Trans.CLM_ID"),
    F.col("Trans.CLM_PROV_ROLE_TYP_CD"),
    F.col("Trans.JOB_EXCTN_RCRD_ERR_SK"),
    F.col("Trans.INSRT_UPDT_CD"),
    F.col("Trans.DISCARD_IN"),
    F.col("Trans.PASS_THRU_IN"),
    F.col("Trans.FIRST_RECYC_DT"),
    F.col("Trans.ERR_CT"),
    F.col("Trans.RECYCLE_CT"),
    F.col("Trans.SRC_SYS_CD"),
    F.col("Trans.PRI_KEY_STRING"),
    F.col("Trans.CLM_PROV_SK"),
    F.col("Trans.CRT_RUN_CYC_EXCTN_SK"),
    F.col("Trans.LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Trans.PROV_ID"),
    F.col("Trans.TAX_ID"),
    F.lit("NA").alias("SVC_FCLTY_LOC_NTNL_PROV_ID"),
    F.when(F.col("Trans.NTNL_PROV_ID").isNull(), F.lit("NA")).otherwise(F.col("Trans.NTNL_PROV_ID")).alias("NTNL_PROV_ID")
)

# Another output pin "Transform" => has 3 primary key columns
df_TransformPK = df_Snapshot_input.select(
    F.lit("").alias("SRC_SYS_CD_SK"),
    F.col("Trans.CLM_ID"),
    F.col("Trans.CLM_PROV_ROLE_TYP_CD")
)

# ------------------------------------------------------------------------------
# Stage: Transformer (CTransformerStage) with input pin "SnapShot"
# We see "GetFkeyCodes(SrcSysCd,0,'CLAIM PROVIDER ROLE TYPE',SnapShot.CLM_PROV_ROLE_TYP_CD,'X')"
# We'll replicate that as a new column "svClmProvRole"
# Output => "RowCount" => B_CLM_PROV
# ------------------------------------------------------------------------------
df_Transformer_join = df_Snapshot.alias("SnapShot")

df_Transformer_output = df_Transformer_join.select(
    F.lit("").alias("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
    F.col("SnapShot.CLM_ID").alias("CLM_ID"),
    F.expr("GetFkeyCodes(SrcSysCd, 0, 'CLAIM PROVIDER ROLE TYPE', SnapShot.CLM_PROV_ROLE_TYP_CD, 'X')").alias("CLM_PROV_ROLE_TYP_CD_SK"),
    F.col("SnapShot.PROV_ID").alias("PROV_ID")
)

# ------------------------------------------------------------------------------
# Stage: B_CLM_PROV (CSeqFileStage)
# Input: "RowCount" => df_Transformer_output
# Write to load/B_CLM_PROV.#SrcSysCd#.dat.#RunID#
# ------------------------------------------------------------------------------
df_B_CLM_PROV_select = df_Transformer_output.select(
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "CLM_PROV_ROLE_TYP_CD_SK",
    "PROV_ID"
)

write_files(
    df_B_CLM_PROV_select,
    f"{adls_path}/load/B_CLM_PROV.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# ------------------------------------------------------------------------------
# Stage: ClmProvPK (CContainerStage) - Shared Container "ClmProvPK"
# 2 inputs => "AllCol" => df_AllCol, "Transform" => df_TransformPK
# 1 output => "Key"
# This is a shared container. We call it as instructed:
# ------------------------------------------------------------------------------
# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmProvPK
# COMMAND ----------

params_ClmProvPK = {}
df_output_Key = ClmProvPK(df_AllCol, df_TransformPK, params_ClmProvPK)

# ------------------------------------------------------------------------------
# Stage: BcbskccommClmProvExtr (CSeqFileStage)
# Input pin -> "Key" => df_output_Key
# Write to key/BCBSKCCommClmProvExtr.DrugClmProv.dat.#SrcSysCd#.#RunID#
# ------------------------------------------------------------------------------
df_BcbskccommClmProvExtr = df_output_Key.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "CLM_PROV_SK",
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "CLM_PROV_ROLE_TYP_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "PROV_ID",
    "TAX_ID",
    "SVC_FCLTY_LOC_NTNL_PROV_ID",
    "NTNL_PROV_ID"
)

write_files(
    df_BcbskccommClmProvExtr,
    f"{adls_path}/key/BCBSKCCommClmProvExtr.DrugClmProv.dat.{SrcSysCd}.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# End of Job.