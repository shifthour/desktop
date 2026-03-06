# Databricks notebook source
# MAGIC %md
# MAGIC """
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:     Populate the adjusted claims in CLM, DRUG_CLM, CLM_LN, CLM_REMIT_HIST with data from the original claim
# MAGIC                               Update original claim with adjusted to clm id, sk, and change to status code A09
# MAGIC                               Build driver table data for Claim Mart for Argus claims with fill date > 2004-12-31
# MAGIC  
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                                          Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                                                  --------------------------------       -------------------------------   ----------------------------       
# MAGIC BJ Luce                  2005/05                                          originally programmed
# MAGIC BJ Luce                  2005/08                                          changed for new file format DRUG_CLM for rx deductibles
# MAGIC                                2005-08-02                                      add recon dt to drug_clm
# MAGIC                                2005-09-10                                      build driver tables for CLM_MART if ARGUS and fill date > '2004-12-01
# MAGIC BJ Luce                  2005-09-28                                      add CALC_PD_AMT_IN to clm remit historyt
# MAGIC 
# MAGIC SGoddard               2005-12-13                                      add calculated payable amount indicator to claim remit history
# MAGIC BJ Luce                  2005-12-31                                     add reversal for CLM_COB for Medicare Part D staff tracker 1736
# MAGIC                                                                                       add new columns to DRUG_CLM
# MAGIC                                                                                       set ACTL_PD_AMT on adjustments to the original ACTL_PD_AMT * 1
# MAGIC BJ Luce                  2006-01-23                                     add CLM_LN_SVC_LOC_TYP_CD_SK, CLM_LN_SVC_PRICE_RULE_CD_SK 
# MAGIC                                                                                      and NON_PAR_SAV_AMT to claim line adjustments
# MAGIC SGoddard               2006-03-21                                    add CLM_REMIT_HIST_PAYMTMETH_CD_SK to claim remit history
# MAGIC Brent Leland           2006-04-25                                    Changed parameters to environment values
# MAGIC                                                                                      Specified in SQL that FILL_DT_SK column comes from DRUG_CLM.
# MAGIC                                                                                      Changed name of W_CLM_RVRSLto P_CLM_RVRSL
# MAGIC Brent Leland          2006-05-08                                     Changed file name and column structure for claim mart files.
# MAGIC Brent Leland          2006-11-13                                     Set original claim last update run cycle with current cycle.
# MAGIC Brent Leland          2007-03-16     IAD Prod. Sup.        Checked for null amount fields in claim remit history lookup transform.              devlIDS30                    Steph Goddard     4/5/07
# MAGIC                                                                                      Input and output columns for clm_remit_hist_hash did not match.
# MAGIC                                                                                      Added PCA_PD_AMT column to remit history output.  Set to 0.00.
# MAGIC                                                                                      Added PCA_TYP_CD_SK, REL_PCA_CLM_SK, and REL_BASE_CLM_SK
# MAGIC                                                                                      columns to claim table output with default surrogate key of NA.
# MAGIC Sandrew                 2008-10-03       3784(PBM)            Added fields ADM_FEE_AMT and DRUG_CLM_BILL_BSS_CD_SK 
# MAGIC                                                                                        to the adj_drug_clm extract to load to DRUG_CLM                                      devlIDSnew                   Steph Goddard       10/28/2008
# MAGIC Tracy Davis            2009-04-09       3784(PBM)            New field AVG_WHLSL_PRICE_AMT                                                            devlIDS                        Steph Goddard        04/14/2009
# MAGIC 
# MAGIC SAndrew                 2009-06-23      #3833 Remit Alt Chrg      Added two new fields to end of file                                                      devlIDSnew                    Steph Goddard        07/06/2009
# MAGIC                                                                                                ALT_CHRG_IN and ALT_CHRG_PROV_WRTOFF_AMT
# MAGIC Ralph Tucker          2009-11-03      15 - Prod Sup           Added new field: REMIT_SUPRSION_AMT                                              devlIDS                            
# MAGIC Kalyan Neelam        2011-01-18      4616                        Updated the default value to NULL from Space for 3 fields                        IntegrateNewDevl            Steph Goddard       01/18/2011
# MAGIC                                                                                         (MCAID_RESUB_NO, PATN_ACCT_NO, RFRNG_PROV_TX) in clm Transformer
# MAGIC Kalyan Neelam        2011-02-02      4616                        Added 'MEDTRAK' source check for VNDR_CLM_NO                             IntegrateNewDevl            Steph Goddard        02/15/2011
# MAGIC                                                                                        in adj_drug_clm SQL.
# MAGIC                                                                                        Added Balancing snapshots for CLM
# MAGIC Karthik Chintalaphani  2012-02-21    4784                    Added logic to check if the SRC_SYS_CD ='BCBSSC' to populate           IntegrateCurDevl                 Brent Leland            03-05-2012
# MAGIC                                                                                       the INPUT_DT_SK and RCVD_DT_SK for adjusted claim.
# MAGIC Kalyan Neelam        2012-10-17      4784                       Added separate query for BCBSSC as a union in orig_clm_hash link          IntegrateWrhsDevl          Bhoomi Dasari         10/20/2012
# MAGIC Kalyan Neelam         2013-03-11      4963                Added a new column UCR_AMT in DRUG_CLM and 3 new columns          IntegrateNewDevl               Bhoomi Dasari         3/15/2013
# MAGIC                                                                                  VBB_RULE_ID, VBB_EXCD_ID, CLM_LN_VBB_IN in CLM_LN
# MAGIC                                                                                      Removed Data Elememts in all places
# MAGIC  Raja Gummadi        2013-08-28      5115 BHI           1.  Added 3 new columns to the Drug Clm Extract.                                         IntegrateNewDevl              SAndrew                 2013-09-04
# MAGIC                                                                                            DRUG_CLM_PRTL_FILL_CD_SK
# MAGIC                                                                                            DRUG_CLM_PDX_TYP_CD_SK
# MAGIC                                                                                             INCNTV_FEE_AMT
# MAGIC                                                                                   2.    Included TTR work to fix reversal amounts for DrugClm Admin Fee, UCR Amount and AWG fields.  they were not being negated.
# MAGIC 
# MAGIC 
# MAGIC Manasa Andru        2014-10-17       TFS - 9580     Added 2 new fields(ITS_SUPLMT_DSCNT_AMT and  ITS_SRCHRG_AMT)      IntegrateCurDevl         Kalyan Neelam           2014-10-27
# MAGIC                                                                                at the end. For the ClmLn File. 
# MAGIC 
# MAGIC Manasa Andru        2017-06-21       Project -5321   Updated the extract SQL in the ids Stage, orig_clm_hash link by aliasing             IntegrateDev1                   Jag Yelavarthi        2017-08-02       
# MAGIC                                                                                  R.RVRSL_CLM_SK as CLM_SK, R.RVRSL_CLM_ID as CLM_ID, 
# MAGIC                                                                               CLM.CLM_SK as ORIG_CLM_SK and CLM.CLM_ID as ORIG_CLM_ID 
# MAGIC 
# MAGIC Hari Pinnaka          2017-08-07      5792                      Added 3 new fields (NDC_SK ,NDC_DRUG_FORM_CD_SK,                        IntegrateDev1             Kalyan Neelam           2017-09-06
# MAGIC                                                                                     NDC_UNIT_CT) at the end
# MAGIC 
# MAGIC Madhavan B          2018-02-06      5792 	                     Changed the datatype of the column                                                              IntegrateDev1             Kalyan Neelam          2018-02-12
# MAGIC                                                       		     NDC_UNIT_CT from SMALLINT to DECIMAL (21,10)
# MAGIC 
# MAGIC Mohan Karnati       2019-07-02      ADO-73034          Adding CLM_TXNMY_CD filed and pushing till target stage                            IntegrateDev1             Kalyan Neelam          2019-07-03
# MAGIC 
# MAGIC Rekha Radhakrishna       2020-02-07      6131-PBM replacement      Added 5 AMT fields to reversal logic for DRUG_CLM_PRICE           IntegrateDev2             Kalyan Neelam          2020-02-08   
# MAGIC Rekha Radhakrishna       2020-02-13      6131-PBM replacement      Corrected the order of fields  for DRUG_CLM_PRICE               IntegrateDev2              Jaideep Mankala      03/10/2020 
# MAGIC Rekha Radhakrishna     2020-02-29       6131-PBM Replacement     Added SrcSysCd 'OPTUMRX'  claims to be written 
# MAGIC                                                                                                            to W_WEBDM_ETL_DRUG.* file
# MAGIC 
# MAGIC Sagar Sayam              2020-03-28      6131-PBM replacement      Added BILL_PAYMT_EXCL_IN filed  in CLM                                                IntegrateDev2          
# MAGIC 
# MAGIC Sagar Sayam              2020-03-31      6131-PBM replacement      Added SPEC_DRUG_IN filed in DRUG_CLM                                                IntegrateDev2          
# MAGIC Sri Nannapaneni         2020-07-15      6131 PBM replacement      Added 7 new fields to database unload for drug_clm                                     IntegrateDev2                 
# MAGIC Sri Nannapaneni         2020-08-13      6131 PBM replacement      Added GNRC_PROD_ID  new fields to database unload for drug_clm         IntegrateDev2            Sravya Gorla   12/09/2020     
# MAGIC Rekha radhakrishna   2021-06-19      PBM Phase II Carryover      Added the fields DRUG_TYP_CD_SK, TIER_ID and FRMLRY_SK            IntegrateDev2	Abhiram Dasarathy	2021-06-23
# MAGIC 
# MAGIC Ashok kumar Baskaran 2023-12-22   600693-Plan drug status cd  added new field Plan drug status cd                                                              IntegrateDev2           Jeyaprasanna         2024-01-01
# MAGIC 
# MAGIC Ashok kumar B            2024-02-01        US 608682                        Added  PLAN_TYPE  to the DrugClmPrice Landing file                                 IntegrateDev2           Jeyaprasanna          2024-03-14

# MAGIC Load file for Claim Balancing. The data is appended to the Claim Balancing file created in the Extract job
# MAGIC Writing Sequential Files to ../load
# MAGIC Pull original claim and drug claim paid data for adjusted claims
# MAGIC W_DRUG_CLM Created in IdsDrugClmFkey
# MAGIC 
# MAGIC W_CLM_RVRSL created in IdsDrugReversalBuild
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Utility_DS_Integrate
# COMMAND ----------
#!/usr/bin/python

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when, rpad
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------


# --------------------------------------------------------------------------------
# Retrieve parameter values
# --------------------------------------------------------------------------------
RunID = get_widget_value('RunID','')
Source = get_widget_value('Source','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

# --------------------------------------------------------------------------------
# Get DB config for IDS
# --------------------------------------------------------------------------------
jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# --------------------------------------------------------------------------------
# Read from IDS (Stage: ids), Output pin: adj_drug_clm
# --------------------------------------------------------------------------------
extract_query_adj_drug_clm = f"""
SELECT 
ADJ.DRUG_CLM_SK,
ADJ.SRC_SYS_CD_SK,
ADJ.CLM_ID,
CLM.CRT_RUN_CYC_EXCTN_SK,
CLM.LAST_UPDT_RUN_CYC_EXCTN_SK,
ADJ.CLM_SK,
CLM.NDC_SK,
CLM.PRSCRB_PROV_DEA_SK,
CLM.DRUG_CLM_DISPNS_AS_WRTN_CD_SK,
CLM.DRUG_CLM_LGL_STTUS_CD_SK,
CLM.DRUG_CLM_TIER_CD_SK,
CLM.DRUG_CLM_VNDR_STTUS_CD_SK,
CLM.CMPND_IN,
CLM.FRMLRY_IN,
CLM.GNRC_DRUG_IN,
CLM.MAIL_ORDER_IN,
CLM.MNTN_IN,
CLM.MAX_ALW_CST_REDC_IN,
CLM.NON_FRMLRY_DRUG_IN,
CLM.SNGL_SRC_IN,
CLM.ADJ_DT_SK,
CLM.FILL_DT_SK,
CLM.RECON_DT_SK,
CLM.DISPNS_FEE_AMT,
CLM.HLTH_PLN_EXCL_AMT,
CLM.HLTH_PLN_PD_AMT,
CLM.INGR_CST_ALW_AMT,
CLM.INGR_CST_CHRGD_AMT,
CLM.INGR_SAV_AMT,
CLM.MBR_DEDCT_EXCL_AMT,
CLM.MBR_DIFF_PD_AMT,
CLM.MBR_OOP_AMT,
CLM.MBR_OOP_EXCL_AMT,
CLM.OTHR_SAV_AMT,
CLM.SLS_TAX_AMT,
CLM.RX_ALW_QTY,
CLM.RX_SUBMT_QTY,
CLM.RX_ALW_DAYS_SUPL_QTY,
CLM.RX_ORIG_DAYS_SUPL_QTY,
CLM.PDX_NTWK_ID,
ADJ.RX_NO,
CLM.RFL_NO,
CASE WHEN ADJ.SRC_SYS_CD = 'MEDTRAK' THEN CLM.VNDR_CLM_NO ELSE ADJ.VNDR_CLM_NO END as VNDR_CLM_NO,
CLM.VNDR_PREAUTH_ID,
CLM.DRUG_CLM_BNF_FRMLRY_POL_CD_SK,
CLM.DRUG_CLM_BNF_RSTRCT_CD_SK,
CLM.DRUG_CLM_MCPARTD_COVDRUG_CD_SK,
CLM.DRUG_CLM_PDX_NTWK_CD_SK,
CLM.DRUG_CLM_PRAUTH_CD_SK,
CLM.MNDTRY_MAIL_ORDER_IN,
CLM.ADM_FEE_AMT,
CLM.DRUG_CLM_BILL_BSS_CD_SK,
CLM.AVG_WHLSL_PRICE_AMT,
CLM.UCR_AMT,
CLM.DRUG_CLM_PRTL_FILL_CD_SK,
CLM.DRUG_CLM_PDX_TYP_CD_SK,
CLM.INCNTV_FEE_AMT,
CLM.SPEC_DRUG_IN,
CLM.SUBMT_PROD_ID_QLFR_CD_SK,
CLM.CNTNGNT_THER_CD_SK,
CLM.CNTNGNT_THER_SCHD_ID,
CLM.MBR_NTWK_DIFF_PD_AMT,
CLM.MBR_SLS_TAX_AMT,
CLM.MBR_PRCS_FEE_AMT,
CLM.GNRC_PROD_ID
FROM {IDSOwner}.DRUG_CLM CLM,
     {IDSOwner}.W_DRUG_CLM ADJ 
WHERE CLM.DRUG_CLM_SK = ADJ.DRUG_CLM_SK
"""

df_adj_drug_clm = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_adj_drug_clm)
    .load()
)

# --------------------------------------------------------------------------------
# Read from IDS (Stage: ids), Output pin: orig_clm_hash
# --------------------------------------------------------------------------------
extract_query_orig_clm_hash = f"""
SELECT 
R.RVRSL_CLM_SK as CLM_SK, 
NG.SRC_SYS_CD,
R.SRC_SYS_CD_SK,
R.RVRSL_CLM_ID as CLM_ID, 
CLM.CRT_RUN_CYC_EXCTN_SK,
CLM.LAST_UPDT_RUN_CYC_EXCTN_SK,
CLM.CLM_SK as ORIG_CLM_SK,
CLM.CLM_ID as ORIG_CLM_ID,
ADJ_FROM_CLM_SK,ADJ_TO_CLM_SK,CLS_SK,CLS_PLN_SK,EXPRNC_CAT_SK,FNCL_LOB_SK,GRP_SK,MBR_SK,NTWK_SK,PROD_SK,SUBGRP_SK,SUB_SK,CLM_ACDNT_ST_CD_SK,CLM_ACTV_BCBS_PLN_CD_SK,CLM_AGMNT_SRC_CD_SK,CLM_BTCH_ACTN_CD_SK,CLM_CAP_CD_SK,CLM_CAT_CD_SK,CLM_CHK_CYC_OVRD_CD_SK,CLM_COB_CD_SK,CLM_EOB_EXCD_SK,CLM_FINL_DISP_CD_SK,CLM_INPT_METH_CD_SK,CLM_INPT_SRC_CD_SK,CLM_INTER_PLN_PGM_CD_SK,CLM_NTWK_STTUS_CD_SK,CLM_NONPAR_PROV_PFX_CD_SK,CLM_OTHR_BNF_CD_SK,CLM_PAYE_CD_SK,CLM_PRCS_CTL_AGNT_PFX_CD_SK,CLM_SVC_DEFN_PFX_CD_SK,CLM_SVC_PROV_SPEC_CD_SK,CLM_SVC_PROV_TYP_CD_SK,CLM_STTUS_CD_SK,CLM_SUBMTTING_BCBS_PLN_CD_SK,CLM_SUB_BCBS_PLN_CD_SK,CLM_SUBTYP_CD_SK,CLM_TYP_CD_SK,ATCHMT_IN,CLNCL_EDIT_IN,COBRA_CLM_IN,FIRST_PASS_IN,LTR_IN,MCARE_ASG_IN,NOTE_IN,PCA_AUDIT_IN,PCP_SUBMT_IN,PROD_OOA_IN,ACDNT_DT_SK,INPT_DT_SK,MBR_PLN_ELIG_DT_SK,NEXT_RVW_DT_SK,PD_DT_SK,PAYMT_DRAG_CYC_DT_SK,PRCS_DT_SK,RCVD_DT_SK,SVC_STRT_DT_SK,SVC_END_DT_SK,SMLR_ILNS_DT_SK,STTUS_DT_SK,WORK_UNABLE_BEG_DT_SK,WORK_UNABLE_END_DT_SK,ACDNT_AMT,ACTL_PD_AMT,ALW_AMT,DSALW_AMT,COINS_AMT,CNSD_CHRG_AMT,COPAY_AMT,CHRG_AMT,DEDCT_AMT,PAYBL_AMT,CLM_CT,MBR_AGE,ADJ_FROM_CLM_ID,ADJ_TO_CLM_ID,DOC_TX_ID,MCAID_RESUB_NO,MCARE_ID,MBR_SFX_NO,PATN_ACCT_NO,PAYMT_REF_ID,PROV_AGMNT_ID,RFRNG_PROV_TX,SUB_ID,ALPHA_PFX_SK,CLM_ACDNT_CD_SK,HOST_IN,D.DRUG_CLM_SK,NDC_SK,PRSCRB_PROV_DEA_SK,DRUG_CLM_DISPNS_AS_WRTN_CD_SK,DRUG_CLM_LGL_STTUS_CD_SK,DRUG_CLM_TIER_CD_SK,DRUG_CLM_VNDR_STTUS_CD_SK,CMPND_IN,FRMLRY_IN,GNRC_DRUG_IN,MAIL_ORDER_IN,MNTN_IN,MAX_ALW_CST_REDC_IN,NON_FRMLRY_DRUG_IN,SNGL_SRC_IN,ADJ_DT_SK,D.FILL_DT_SK,DISPNS_FEE_AMT,HLTH_PLN_EXCL_AMT,HLTH_PLN_PD_AMT,INGR_CST_ALW_AMT,INGR_CST_CHRGD_AMT,INGR_SAV_AMT,MBR_DEDCT_EXCL_AMT,MBR_DIFF_PD_AMT,MBR_OOP_AMT,MBR_OOP_EXCL_AMT,OTHR_SAV_AMT,RX_ALW_QTY,RX_SUBMT_QTY,SLS_TAX_AMT,RX_ALW_DAYS_SUPL_QTY,RX_ORIG_DAYS_SUPL_QTY,PDX_NTWK_ID,D.RX_NO,RFL_NO,D.VNDR_CLM_NO,VNDR_PREAUTH_ID,DRUG_CLM_BNF_FRMLRY_POL_CD_SK,DRUG_CLM_BNF_RSTRCT_CD_SK,DRUG_CLM_MCPARTD_COVDRUG_CD_SK,DRUG_CLM_PDX_NTWK_CD_SK,DRUG_CLM_PRAUTH_CD_SK,MNDTRY_MAIL_ORDER_IN,D.ADM_FEE_AMT,D.DRUG_CLM_BILL_BSS_CD_SK,D.AVG_WHLSL_PRICE_AMT,CLM.MCAID_STTUS_ID,CLM.PATN_PD_AMT, D.UCR_AMT, D.DRUG_CLM_PRTL_FILL_CD_SK, D.DRUG_CLM_PDX_TYP_CD_SK, D.INCNTV_FEE_AMT,CLM.CLM_TXNMY_CD,CLM.BILL_PAYMT_EXCL_IN,D.SPEC_DRUG_IN
FROM {IDSOwner}.CLM CLM, 
     {IDSOwner}.W_CLM_RVRSL  R, 
     {IDSOwner}.DRUG_CLM D, 
     {IDSOwner}.CD_MPPNG NG, 
     {IDSOwner}.CLM_PROV PROV,
     {IDSOwner}.W_DRUG_CLM ADJ 
WHERE R.ORIG_CLM_SK = CLM.CLM_SK
  and R.ORIG_CLM_SK = D.CLM_SK
  and R.SRC_SYS_CD_SK = NG.CD_MPPNG_SK
  and R.RVRSL_CLM_SK = ADJ.CLM_SK
  and CLM.CLM_SK = PROV.CLM_SK
  and PROV.PROV_SK = ADJ.PROV_SK
  and NG.TRGT_CD <> 'BCBSSC'

UNION

SELECT 
R.RVRSL_CLM_SK as CLM_SK, 
NG.SRC_SYS_CD,
R.SRC_SYS_CD_SK,
R.RVRSL_CLM_ID as CLM_ID,
CLM.CRT_RUN_CYC_EXCTN_SK,
CLM.LAST_UPDT_RUN_CYC_EXCTN_SK,
CLM.CLM_SK as ORIG_CLM_SK,
CLM.CLM_ID as ORIG_CLM_ID,
ADJ_FROM_CLM_SK,ADJ_TO_CLM_SK,CLS_SK,CLS_PLN_SK,EXPRNC_CAT_SK,FNCL_LOB_SK,GRP_SK,MBR_SK,NTWK_SK,PROD_SK,SUBGRP_SK,SUB_SK,CLM_ACDNT_ST_CD_SK,CLM_ACTV_BCBS_PLN_CD_SK,CLM_AGMNT_SRC_CD_SK,CLM_BTCH_ACTN_CD_SK,CLM_CAP_CD_SK,CLM_CAT_CD_SK,CLM_CHK_CYC_OVRD_CD_SK,CLM_COB_CD_SK,CLM_EOB_EXCD_SK,CLM_FINL_DISP_CD_SK,CLM_INPT_METH_CD_SK,CLM_INPT_SRC_CD_SK,CLM_INTER_PLN_PGM_CD_SK,CLM_NTWK_STTUS_CD_SK,CLM_NONPAR_PROV_PFX_CD_SK,CLM_OTHR_BNF_CD_SK,CLM_PAYE_CD_SK,CLM_PRCS_CTL_AGNT_PFX_CD_SK,CLM_SVC_DEFN_PFX_CD_SK,CLM_SVC_PROV_SPEC_CD_SK,CLM_SVC_PROV_TYP_CD_SK,CLM_STTUS_CD_SK,CLM_SUBMTTING_BCBS_PLN_CD_SK,CLM_SUB_BCBS_PLN_CD_SK,CLM_SUBTYP_CD_SK,CLM_TYP_CD_SK,ATCHMT_IN,CLNCL_EDIT_IN,COBRA_CLM_IN,FIRST_PASS_IN,LTR_IN,MCARE_ASG_IN,NOTE_IN,PCA_AUDIT_IN,PCP_SUBMT_IN,PROD_OOA_IN,ACDNT_DT_SK,INPT_DT_SK,MBR_PLN_ELIG_DT_SK,NEXT_RVW_DT_SK,PD_DT_SK,PAYMT_DRAG_CYC_DT_SK,PRCS_DT_SK,RCVD_DT_SK,SVC_STRT_DT_SK,SVC_END_DT_SK,SMLR_ILNS_DT_SK,STTUS_DT_SK,WORK_UNABLE_BEG_DT_SK,WORK_UNABLE_END_DT_SK,ACDNT_AMT,ACTL_PD_AMT,ALW_AMT,DSALW_AMT,COINS_AMT,CNSD_CHRG_AMT,COPAY_AMT,CHRG_AMT,DEDCT_AMT,PAYBL_AMT,CLM_CT,MBR_AGE,ADJ_FROM_CLM_ID,ADJ_TO_CLM_ID,DOC_TX_ID,MCAID_RESUB_NO,MCARE_ID,MBR_SFX_NO,PATN_ACCT_NO,PAYMT_REF_ID,PROV_AGMNT_ID,RFRNG_PROV_TX,SUB_ID,ALPHA_PFX_SK,CLM_ACDNT_CD_SK,HOST_IN,D.DRUG_CLM_SK,NDC_SK,PRSCRB_PROV_DEA_SK,DRUG_CLM_DISPNS_AS_WRTN_CD_SK,DRUG_CLM_LGL_STTUS_CD_SK,DRUG_CLM_TIER_CD_SK,DRUG_CLM_VNDR_STTUS_CD_SK,CMPND_IN,FRMLRY_IN,GNRC_DRUG_IN,MAIL_ORDER_IN,MNTN_IN,MAX_ALW_CST_REDC_IN,NON_FRMLRY_DRUG_IN,SNGL_SRC_IN,ADJ_DT_SK,D.FILL_DT_SK,DISPNS_FEE_AMT,HLTH_PLN_EXCL_AMT,HLTH_PLN_PD_AMT,INGR_CST_ALW_AMT,INGR_CST_CHRGD_AMT,INGR_SAV_AMT,MBR_DEDCT_EXCL_AMT,MBR_DIFF_PD_AMT,MBR_OOP_AMT,MBR_OOP_EXCL_AMT,OTHR_SAV_AMT,RX_ALW_QTY,RX_SUBMT_QTY,SLS_TAX_AMT,RX_ALW_DAYS_SUPL_QTY,RX_ORIG_DAYS_SUPL_QTY,PDX_NTWK_ID,D.RX_NO,RFL_NO,D.VNDR_CLM_NO,VNDR_PREAUTH_ID,DRUG_CLM_BNF_FRMLRY_POL_CD_SK,DRUG_CLM_BNF_RSTRCT_CD_SK,DRUG_CLM_MCPARTD_COVDRUG_CD_SK,DRUG_CLM_PDX_NTWK_CD_SK,DRUG_CLM_PRAUTH_CD_SK,MNDTRY_MAIL_ORDER_IN,D.ADM_FEE_AMT,D.DRUG_CLM_BILL_BSS_CD_SK,D.AVG_WHLSL_PRICE_AMT,CLM.MCAID_STTUS_ID,CLM.PATN_PD_AMT, D.UCR_AMT, D.DRUG_CLM_PRTL_FILL_CD_SK, D.DRUG_CLM_PDX_TYP_CD_SK, D.INCNTV_FEE_AMT,CLM.CLM_TXNMY_CD,CLM.BILL_PAYMT_EXCL_IN,D.SPEC_DRUG_IN
FROM {IDSOwner}.CLM CLM, 
     {IDSOwner}.W_CLM_RVRSL  R, 
     {IDSOwner}.DRUG_CLM D, 
     {IDSOwner}.CD_MPPNG NG, 
     {IDSOwner}.CLM_PROV PROV,
     {IDSOwner}.W_DRUG_CLM ADJ 
WHERE R.ORIG_CLM_SK = CLM.CLM_SK
  and R.ORIG_CLM_SK = D.CLM_SK
  and R.SRC_SYS_CD_SK = NG.CD_MPPNG_SK
  and R.RVRSL_CLM_SK = ADJ.CLM_SK
  and CLM.CLM_SK = PROV.CLM_SK
  and NG.TRGT_CD = 'BCBSSC'
"""

df_orig_clm_hash = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_orig_clm_hash)
    .load()
)

# --------------------------------------------------------------------------------
# clm_and_drug_clm_hash (StageType: CHashedFileStage) - Scenario A
# Deduplicate on key: CLM_SK
# Output pin: orig_drug_clm_lkup
# --------------------------------------------------------------------------------
df_orig_clm_hash_dedup = dedup_sort(df_orig_clm_hash, ["CLM_SK"], [("CLM_SK", "A")])

# Apply rpad for char columns, then select columns in the same order as defined:
df_orig_drug_clm_lkup = df_orig_clm_hash_dedup.select(
    col("CLM_SK"),
    col("SRC_SYS_CD"),
    col("SRC_SYS_CD_SK"),
    col("CLM_ID"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("ORIG_CLM_SK"),
    col("ORIG_CLM_ID"),
    col("ADJ_FROM_CLM_SK"),
    col("ADJ_TO_CLM_SK"),
    col("CLS_SK"),
    col("CLS_PLN_SK"),
    col("EXPRNC_CAT_SK"),
    col("FNCL_LOB_SK"),
    col("GRP_SK"),
    col("MBR_SK"),
    col("NTWK_SK"),
    col("PROD_SK"),
    col("SUBGRP_SK"),
    col("SUB_SK"),
    col("CLM_ACDNT_ST_CD_SK"),
    col("CLM_ACTV_BCBS_PLN_CD_SK"),
    col("CLM_AGMNT_SRC_CD_SK"),
    col("CLM_BTCH_ACTN_CD_SK"),
    col("CLM_CAP_CD_SK"),
    col("CLM_CAT_CD_SK"),
    col("CLM_CHK_CYC_OVRD_CD_SK"),
    col("CLM_COB_CD_SK"),
    col("CLM_EOB_EXCD_SK"),
    col("CLM_FINL_DISP_CD_SK"),
    col("CLM_INPT_METH_CD_SK"),
    col("CLM_INPT_SRC_CD_SK"),
    col("CLM_INTER_PLN_PGM_CD_SK"),
    col("CLM_NTWK_STTUS_CD_SK"),
    col("CLM_NONPAR_PROV_PFX_CD_SK"),
    col("CLM_OTHR_BNF_CD_SK"),
    col("CLM_PAYE_CD_SK"),
    col("CLM_PRCS_CTL_AGNT_PFX_CD_SK"),
    col("CLM_SVC_DEFN_PFX_CD_SK"),
    col("CLM_SVC_PROV_SPEC_CD_SK"),
    col("CLM_SVC_PROV_TYP_CD_SK"),
    col("CLM_STTUS_CD_SK"),
    col("CLM_SUBMTTING_BCBS_PLN_CD_SK"),
    col("CLM_SUB_BCBS_PLN_CD_SK"),
    col("CLM_SUBTYP_CD_SK"),
    col("CLM_TYP_CD_SK"),
    rpad(col("ATCHMT_IN"), 1, " ").alias("ATCHMT_IN"),
    rpad(col("CLNCL_EDIT_IN"), 1, " ").alias("CLNCL_EDIT_IN"),
    rpad(col("COBRA_CLM_IN"), 1, " ").alias("COBRA_CLM_IN"),
    rpad(col("FIRST_PASS_IN"), 1, " ").alias("FIRST_PASS_IN"),
    rpad(col("LTR_IN"), 1, " ").alias("LTR_IN"),
    rpad(col("MCARE_ASG_IN"), 1, " ").alias("MCARE_ASG_IN"),
    rpad(col("NOTE_IN"), 1, " ").alias("NOTE_IN"),
    rpad(col("PCA_AUDIT_IN"), 1, " ").alias("PCA_AUDIT_IN"),
    rpad(col("PCP_SUBMT_IN"), 1, " ").alias("PCP_SUBMT_IN"),
    rpad(col("PROD_OOA_IN"), 1, " ").alias("PROD_OOA_IN"),
    rpad(col("ACDNT_DT_SK"), 10, " ").alias("ACDNT_DT_SK"),
    rpad(col("INPT_DT_SK"), 10, " ").alias("INPT_DT_SK"),
    rpad(col("MBR_PLN_ELIG_DT_SK"), 10, " ").alias("MBR_PLN_ELIG_DT_SK"),
    rpad(col("NEXT_RVW_DT_SK"), 10, " ").alias("NEXT_RVW_DT_SK"),
    rpad(col("PD_DT_SK"), 10, " ").alias("PD_DT_SK"),
    rpad(col("PAYMT_DRAG_CYC_DT_SK"), 10, " ").alias("PAYMT_DRAG_CYC_DT_SK"),
    rpad(col("PRCS_DT_SK"), 10, " ").alias("PRCS_DT_SK"),
    rpad(col("RCVD_DT_SK"), 10, " ").alias("RCVD_DT_SK"),
    rpad(col("SVC_STRT_DT_SK"), 10, " ").alias("SVC_STRT_DT_SK"),
    rpad(col("SVC_END_DT_SK"), 10, " ").alias("SVC_END_DT_SK"),
    rpad(col("SMLR_ILNS_DT_SK"), 10, " ").alias("SMLR_ILNS_DT_SK"),
    rpad(col("STTUS_DT_SK"), 10, " ").alias("STTUS_DT_SK"),
    rpad(col("WORK_UNABLE_BEG_DT_SK"), 10, " ").alias("WORK_UNABLE_BEG_DT_SK"),
    rpad(col("WORK_UNABLE_END_DT_SK"), 10, " ").alias("WORK_UNABLE_END_DT_SK"),
    col("ACDNT_AMT"),
    col("ACTL_PD_AMT"),
    col("ALW_AMT"),
    col("DSALW_AMT"),
    col("COINS_AMT"),
    col("CNSD_CHRG_AMT"),
    col("COPAY_AMT"),
    col("CHRG_AMT"),
    col("DEDCT_AMT"),
    col("PAYBL_AMT"),
    col("CLM_CT"),
    col("MBR_AGE"),
    col("ADJ_FROM_CLM_ID"),
    col("ADJ_TO_CLM_ID"),
    rpad(col("DOC_TX_ID"), 18, " ").alias("DOC_TX_ID"),
    rpad(col("MCAID_RESUB_NO"), 15, " ").alias("MCAID_RESUB_NO"),
    rpad(col("MCARE_ID"), 12, " ").alias("MCARE_ID"),
    rpad(col("MBR_SFX_NO"), 2, " ").alias("MBR_SFX_NO"),
    col("PATN_ACCT_NO"),
    rpad(col("PAYMT_REF_ID"), 16, " ").alias("PAYMT_REF_ID"),
    rpad(col("PROV_AGMNT_ID"), 12, " ").alias("PROV_AGMNT_ID"),
    col("RFRNG_PROV_TX"),
    rpad(col("SUB_ID"), 14, " ").alias("SUB_ID"),
    col("ALPHA_PFX_SK"),
    col("CLM_ACDNT_CD_SK"),
    rpad(col("HOST_IN"), 1, " ").alias("HOST_IN"),
    col("DRUG_CLM_SK"),
    col("NDC_SK"),
    col("PRSCRB_PROV_DEA_SK"),
    col("DRUG_CLM_DISPNS_AS_WRTN_CD_SK"),
    col("DRUG_CLM_LGL_STTUS_CD_SK"),
    col("DRUG_CLM_TIER_CD_SK"),
    col("DRUG_CLM_VNDR_STTUS_CD_SK"),
    rpad(col("CMPND_IN"), 1, " ").alias("CMPND_IN"),
    rpad(col("FRMLRY_IN"), 1, " ").alias("FRMLRY_IN"),
    rpad(col("GNRC_DRUG_IN"), 1, " ").alias("GNRC_DRUG_IN"),
    rpad(col("MAIL_ORDER_IN"), 1, " ").alias("MAIL_ORDER_IN"),
    rpad(col("MNTN_IN"), 1, " ").alias("MNTN_IN"),
    rpad(col("MAX_ALW_CST_REDC_IN"), 1, " ").alias("MAX_ALW_CST_REDC_IN"),
    rpad(col("NON_FRMLRY_DRUG_IN"), 1, " ").alias("NON_FRMLRY_DRUG_IN"),
    rpad(col("SNGL_SRC_IN"), 1, " ").alias("SNGL_SRC_IN"),
    rpad(col("ADJ_DT_SK"), 10, " ").alias("ADJ_DT_SK"),
    rpad(col("FILL_DT_SK"), 10, " ").alias("FILL_DT_SK"),
    col("DISPNS_FEE_AMT"),
    col("HLTH_PLN_EXCL_AMT"),
    col("HLTH_PLN_PD_AMT"),
    col("INGR_CST_ALW_AMT"),
    col("INGR_CST_CHRGD_AMT"),
    col("INGR_SAV_AMT"),
    col("MBR_DEDCT_EXCL_AMT"),
    col("MBR_DIFF_PD_AMT"),
    col("MBR_OOP_AMT"),
    col("MBR_OOP_EXCL_AMT"),
    col("OTHR_SAV_AMT"),
    col("RX_ALW_QTY"),
    col("RX_SUBMT_QTY"),
    col("SLS_TAX_AMT"),
    col("RX_ALW_DAYS_SUPL_QTY"),
    col("RX_ORIG_DAYS_SUPL_QTY"),
    col("PDX_NTWK_ID"),
    col("RX_NO"),
    col("RFL_NO"),
    col("VNDR_CLM_NO"),
    col("VNDR_PREAUTH_ID"),
    col("DRUG_CLM_BNF_FRMLRY_POL_CD_SK"),
    col("DRUG_CLM_BNF_RSTRCT_CD_SK"),
    col("DRUG_CLM_MCPARTD_COVDRUG_CD_SK"),
    col("DRUG_CLM_PDX_NTWK_CD_SK"),
    col("DRUG_CLM_PRAUTH_CD_SK"),
    rpad(col("MNDTRY_MAIL_ORDER_IN"), 1, " ").alias("MNDTRY_MAIL_ORDER_IN"),
    col("ADM_FEE_AMT"),
    col("DRUG_CLM_BILL_BSS_CD_SK"),
    col("AVG_WHLSL_PRICE_AMT"),
    col("MCAID_STTUS_ID"),
    col("PATN_PD_AMT"),
    col("UCR_AMT"),
    col("DRUG_CLM_PRTL_FILL_CD_SK"),
    col("DRUG_CLM_PDX_TYP_CD_SK"),
    col("INCNTV_FEE_AMT"),
    col("CLM_TXNMY_CD"),
    rpad(col("BILL_PAYMT_EXCL_IN"), 1, " ").alias("BILL_PAYMT_EXCL_IN"),
    rpad(col("SPEC_DRUG_IN"), 1, " ").alias("SPEC_DRUG_IN")
)

# --------------------------------------------------------------------------------
# drug_clm (Transformer Stage) - two inputs:
#   - adj_drug_clm
#   - orig_drug_clm_lkup
# Constraint: IsNull(orig_drug_clm_lkup.CLM_ID) = @FALSE ==> only matched rows
# Join on DRUG_CLM_SK (implied). Output pin: adj_drug_clm_out
# --------------------------------------------------------------------------------
df_adj_drug_clm_out = (
    df_adj_drug_clm.alias("adj_drug_clm")
    .join(
        df_orig_drug_clm_lkup.alias("orig_drug_clm_lkup"),
        col("adj_drug_clm.DRUG_CLM_SK") == col("orig_drug_clm_lkup.DRUG_CLM_SK"),
        "inner"
    )
    .select(
        col("adj_drug_clm.DRUG_CLM_SK").alias("DRUG_CLM_SK"),
        col("adj_drug_clm.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        col("adj_drug_clm.CLM_ID").alias("CLM_ID"),
        col("adj_drug_clm.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("adj_drug_clm.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("adj_drug_clm.CLM_SK").alias("CLM_SK"),
        col("orig_drug_clm_lkup.NDC_SK").alias("NDC_SK"),
        col("orig_drug_clm_lkup.PRSCRB_PROV_DEA_SK").alias("PRSCRB_PROV_DEA_SK"),
        col("orig_drug_clm_lkup.DRUG_CLM_DISPNS_AS_WRTN_CD_SK").alias("DRUG_CLM_DISPNS_AS_WRTN_CD_SK"),
        col("orig_drug_clm_lkup.DRUG_CLM_LGL_STTUS_CD_SK").alias("DRUG_CLM_LGL_STTUS_CD_SK"),
        col("orig_drug_clm_lkup.DRUG_CLM_TIER_CD_SK").alias("DRUG_CLM_TIER_CD_SK"),
        col("orig_drug_clm_lkup.DRUG_CLM_VNDR_STTUS_CD_SK").alias("DRUG_CLM_VNDR_STTUS_CD_SK"),
        rpad(col("orig_drug_clm_lkup.CMPND_IN"), 1, " ").alias("CMPND_IN"),
        rpad(col("orig_drug_clm_lkup.FRMLRY_IN"), 1, " ").alias("FRMLRY_IN"),
        rpad(col("orig_drug_clm_lkup.GNRC_DRUG_IN"), 1, " ").alias("GNRC_DRUG_IN"),
        rpad(col("orig_drug_clm_lkup.MAIL_ORDER_IN"), 1, " ").alias("MAIL_ORDER_IN"),
        rpad(col("orig_drug_clm_lkup.MNTN_IN"), 1, " ").alias("MNTN_IN"),
        rpad(col("orig_drug_clm_lkup.MAX_ALW_CST_REDC_IN"), 1, " ").alias("MAX_ALW_CST_REDC_IN"),
        rpad(col("orig_drug_clm_lkup.NON_FRMLRY_DRUG_IN"), 1, " ").alias("NON_FRMLRY_DRUG_IN"),
        rpad(col("orig_drug_clm_lkup.SNGL_SRC_IN"), 1, " ").alias("SNGL_SRC_IN"),
        rpad(col("adj_drug_clm.ADJ_DT_SK"), 10, " ").alias("ADJ_DT_SK"),
        rpad(col("orig_drug_clm_lkup.FILL_DT_SK"), 10, " ").alias("FILL_DT_SK"),
        rpad(col("adj_drug_clm.RECON_DT_SK"), 10, " ").alias("RECON_DT_SK"),
        (col("orig_drug_clm_lkup.DISPNS_FEE_AMT") * -1).alias("DISPNS_FEE_AMT"),
        (col("orig_drug_clm_lkup.HLTH_PLN_EXCL_AMT") * -1).alias("HLTH_PLN_EXCL_AMT"),
        (col("orig_drug_clm_lkup.HLTH_PLN_PD_AMT") * -1).alias("HLTH_PLN_PD_AMT"),
        (col("orig_drug_clm_lkup.INGR_CST_ALW_AMT") * -1).alias("INGR_CST_ALW_AMT"),
        (col("orig_drug_clm_lkup.INGR_CST_CHRGD_AMT") * -1).alias("INGR_CST_CHRGD_AMT"),
        (col("orig_drug_clm_lkup.INGR_SAV_AMT") * -1).alias("INGR_SAV_AMT"),
        (col("orig_drug_clm_lkup.MBR_DEDCT_EXCL_AMT") * -1).alias("MBR_DEDCT_EXCL_AMT"),
        (col("orig_drug_clm_lkup.MBR_DIFF_PD_AMT") * -1).alias("MBR_DIFF_PD_AMT"),
        (col("orig_drug_clm_lkup.MBR_OOP_AMT") * -1).alias("MBR_OOP_AMT"),
        (col("orig_drug_clm_lkup.MBR_OOP_EXCL_AMT") * -1).alias("MBR_OOP_EXCL_AMT"),
        (col("orig_drug_clm_lkup.OTHR_SAV_AMT") * -1).alias("OTHR_SAV_AMT"),
        (col("orig_drug_clm_lkup.RX_ALW_QTY") * -1).alias("RX_ALW_QTY"),
        (col("orig_drug_clm_lkup.RX_SUBMT_QTY") * -1).alias("RX_SUBMT_QTY"),
        (col("orig_drug_clm_lkup.SLS_TAX_AMT") * -1).alias("SLS_TAX_AMT"),
        (col("orig_drug_clm_lkup.RX_ALW_DAYS_SUPL_QTY") * -1).alias("RX_ALW_DAYS_SUPL_QTY"),
        (col("orig_drug_clm_lkup.RX_ORIG_DAYS_SUPL_QTY") * -1).alias("RX_ORIG_DAYS_SUPL_QTY"),
        col("orig_drug_clm_lkup.PDX_NTWK_ID").alias("PDX_NTWK_ID"),
        col("orig_drug_clm_lkup.RX_NO").alias("RX_NO"),
        col("orig_drug_clm_lkup.RFL_NO").alias("RFL_NO"),
        col("adj_drug_clm.VNDR_CLM_NO").alias("VNDR_CLM_NO"),
        col("orig_drug_clm_lkup.VNDR_PREAUTH_ID").alias("VNDR_PREAUTH_ID"),
        col("orig_drug_clm_lkup.DRUG_CLM_BNF_FRMLRY_POL_CD_SK").alias("DRUG_CLM_BNF_FRMLRY_POL_CD_SK"),
        col("orig_drug_clm_lkup.DRUG_CLM_BNF_RSTRCT_CD_SK").alias("DRUG_CLM_BNF_RSTRCT_CD_SK"),
        col("orig_drug_clm_lkup.DRUG_CLM_MCPARTD_COVDRUG_CD_SK").alias("DRUG_CLM_MCPARTD_COVDRUG_CD_SK"),
        col("orig_drug_clm_lkup.DRUG_CLM_PDX_NTWK_CD_SK").alias("DRUG_CLM_PDX_NTWK_CD_SK"),
        col("orig_drug_clm_lkup.DRUG_CLM_PRAUTH_CD_SK").alias("DRUG_CLM_PRAUTH_CD_SK"),
        rpad(col("orig_drug_clm_lkup.MNDTRY_MAIL_ORDER_IN"), 1, " ").alias("MNDTRY_MAIL_ORDER_IN"),
        when(col("orig_drug_clm_lkup.ADM_FEE_AMT").isNull(), lit(0.00))
        .otherwise(col("orig_drug_clm_lkup.ADM_FEE_AMT") * -1)
        .alias("ADM_FEE_AMT"),
        when(col("orig_drug_clm_lkup.DRUG_CLM_BILL_BSS_CD_SK").isNull(), lit(1))
        .otherwise(col("orig_drug_clm_lkup.DRUG_CLM_BILL_BSS_CD_SK"))
        .alias("DRUG_CLM_BILL_BSS_CD_SK"),
        (col("orig_drug_clm_lkup.AVG_WHLSL_PRICE_AMT") * -1).alias("AVG_WHLSL_PRICE_AMT"),
        (col("orig_drug_clm_lkup.UCR_AMT") * -1).alias("UCR_AMT"),
        col("orig_drug_clm_lkup.DRUG_CLM_PRTL_FILL_CD_SK").alias("DRUG_CLM_PRTL_FILL_CD_SK"),
        col("orig_drug_clm_lkup.DRUG_CLM_PDX_TYP_CD_SK").alias("DRUG_CLM_PDX_TYP_CD_SK"),
        (col("orig_drug_clm_lkup.INCNTV_FEE_AMT") * -1).alias("INCNTV_FEE_AMT"),
        rpad(col("adj_drug_clm.SPEC_DRUG_IN"), 1, " ").alias("SPEC_DRUG_IN"),
        col("adj_drug_clm.SUBMT_PROD_ID_QLFR_CD_SK").alias("SUBMT_PROD_ID_QLFR_CD_SK"),
        col("adj_drug_clm.CNTNGNT_THER_CD_SK").alias("CNTNGNT_THER_CD_SK"),
        col("adj_drug_clm.CNTNGNT_THER_SCHD_ID").alias("CNTNGNT_THER_SCHD_ID"),
        col("adj_drug_clm.MBR_NTWK_DIFF_PD_AMT").alias("MBR_NTWK_DIFF_PD_AMT"),
        col("adj_drug_clm.MBR_SLS_TAX_AMT").alias("MBR_SLS_TAX_AMT"),
        col("adj_drug_clm.MBR_PRCS_FEE_AMT").alias("MBR_PRCS_FEE_AMT"),
        col("adj_drug_clm.GNRC_PROD_ID").alias("GNRC_PROD_ID")
    )
)

# --------------------------------------------------------------------------------
# DrugClm (CSeqFileStage): Write to file DRUG_CLM.#Source#.dat in directory "load"
# Defaults to CSV. We must not include the header. Overwrite mode.
# --------------------------------------------------------------------------------
write_files(
    df_adj_drug_clm_out,
    f"{adls_path}/load/DRUG_CLM.{Source}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)


# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when, isnull, coalesce, rpad, length, trim
from pyspark.sql.types import IntegerType, StringType, DoubleType

IDSOwner = get_widget_value("IDSOwner", "")
ids_secret_name = get_widget_value("ids_secret_name", "")
RunID = get_widget_value("RunID", "")
Source = get_widget_value("Source", "")

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

adj_clm_sql = """
SELECT 
 ADJ.CLM_SK as CLM_SK,
 ADJ.SRC_SYS_CD_SK as SRC_SYS_CD_SK,
 ADJ.CLM_ID as CLM_ID,
 CLM.CRT_RUN_CYC_EXCTN_SK as CRT_RUN_CYC_EXCTN_SK,
 CLM.LAST_UPDT_RUN_CYC_EXCTN_SK as LAST_UPDT_RUN_CYC_EXCTN_SK,
 ADJ_FROM_CLM_SK as ADJ_FROM_CLM_SK,
 ADJ_TO_CLM_SK as ADJ_TO_CLM_SK,
 CLS_SK as CLS_SK,
 CLS_PLN_SK as CLS_PLN_SK,
 EXPRNC_CAT_SK as EXPRNC_CAT_SK,
 GRP_SK as GRP_SK,
 MBR_SK as MBR_SK,
 NTWK_SK as NTWK_SK,
 PROD_SK as PROD_SK,
 SUBGRP_SK as SUBGRP_SK,
 SUB_SK as SUB_SK,
 CLM_ACDNT_ST_CD_SK as CLM_ACDNT_ST_CD_SK,
 CLM_ACTV_BCBS_PLN_CD_SK as CLM_ACTV_BCBS_PLN_CD_SK,
 CLM_AGMNT_SRC_CD_SK as CLM_AGMNT_SRC_CD_SK,
 CLM_BTCH_ACTN_CD_SK as CLM_BTCH_ACTN_CD_SK,
 CLM_CAP_CD_SK as CLM_CAP_CD_SK,
 CLM_CAT_CD_SK as CLM_CAT_CD_SK,
 CLM_CHK_CYC_OVRD_CD_SK as CLM_CHK_CYC_OVRD_CD_SK,
 CLM_COB_CD_SK as CLM_COB_CD_SK,
 CLM_EOB_EXCD_SK as CLM_EOB_EXCD_SK,
 CLM_FINL_DISP_CD_SK as CLM_FINL_DISP_CD_SK,
 CLM_INPT_METH_CD_SK as CLM_INPT_METH_CD_SK,
 CLM_INPT_SRC_CD_SK as CLM_INPT_SRC_CD_SK,
 CLM_INTER_PLN_PGM_CD_SK as CLM_INTER_PLN_PGM_CD_SK,
 CLM_NTWK_STTUS_CD_SK as CLM_NTWK_STTUS_CD_SK,
 CLM_NONPAR_PROV_PFX_CD_SK as CLM_NONPAR_PROV_PFX_CD_SK,
 CLM_OTHR_BNF_CD_SK as CLM_OTHR_BNF_CD_SK,
 CLM_PAYE_CD_SK as CLM_PAYE_CD_SK,
 CLM_PRCS_CTL_AGNT_PFX_CD_SK as CLM_PRCS_CTL_AGNT_PFX_CD_SK,
 CLM_SVC_DEFN_PFX_CD_SK as CLM_SVC_DEFN_PFX_CD_SK,
 CLM_SVC_PROV_SPEC_CD_SK as CLM_SVC_PROV_SPEC_CD_SK,
 CLM_SVC_PROV_TYP_CD_SK as CLM_SVC_PROV_TYP_CD_SK,
 CLM_STTUS_CD_SK as CLM_STTUS_CD_SK,
 CLM_SUBMTTING_BCBS_PLN_CD_SK as CLM_SUBMTTING_BCBS_PLN_CD_SK,
 CLM_SUB_BCBS_PLN_CD_SK as CLM_SUB_BCBS_PLN_CD_SK,
 CLM_SUBTYP_CD_SK as CLM_SUBTYP_CD_SK,
 CLM_TYP_CD_SK as CLM_TYP_CD_SK,
 ATCHMT_IN as ATCHMT_IN,
 CLNCL_EDIT_IN as CLNCL_EDIT_IN,
 COBRA_CLM_IN as COBRA_CLM_IN,
 FIRST_PASS_IN as FIRST_PASS_IN,
 LTR_IN as LTR_IN,
 MCARE_ASG_IN as MCARE_ASG_IN,
 NOTE_IN as NOTE_IN,
 PCA_AUDIT_IN as PCA_AUDIT_IN,
 PCP_SUBMT_IN as PCP_SUBMT_IN,
 PROD_OOA_IN as PROD_OOA_IN,
 ACDNT_DT_SK as ACDNT_DT_SK,
 INPT_DT_SK as INPT_DT_SK,
 MBR_PLN_ELIG_DT_SK as MBR_PLN_ELIG_DT_SK,
 NEXT_RVW_DT_SK as NEXT_RVW_DT_SK,
 PD_DT_SK as PD_DT_SK,
 PAYMT_DRAG_CYC_DT_SK as PAYMT_DRAG_CYC_DT_SK,
 PRCS_DT_SK as PRCS_DT_SK,
 RCVD_DT_SK as RCVD_DT_SK,
 SVC_STRT_DT_SK as SVC_STRT_DT_SK,
 SVC_END_DT_SK as SVC_END_DT_SK,
 SMLR_ILNS_DT_SK as SMLR_ILNS_DT_SK,
 STTUS_DT_SK as STTUS_DT_SK,
 WORK_UNABLE_BEG_DT_SK as WORK_UNABLE_BEG_DT_SK,
 WORK_UNABLE_END_DT_SK as WORK_UNABLE_END_DT_SK,
 ACDNT_AMT as ACDNT_AMT,
 ACTL_PD_AMT as ACTL_PD_AMT,
 ALW_AMT as ALW_AMT,
 DSALW_AMT as DSALW_AMT,
 COINS_AMT as COINS_AMT,
 CNSD_CHRG_AMT as CNSD_CHRG_AMT,
 COPAY_AMT as COPAY_AMT,
 CHRG_AMT as CHRG_AMT,
 DEDCT_AMT as DEDCT_AMT,
 PAYBL_AMT as PAYBL_AMT,
 CLM_CT as CLM_CT,
 MBR_AGE as MBR_AGE,
 ADJ_FROM_CLM_ID as ADJ_FROM_CLM_ID,
 ADJ_TO_CLM_ID as ADJ_TO_CLM_ID,
 DOC_TX_ID as DOC_TX_ID,
 MCAID_RESUB_NO as MCAID_RESUB_NO,
 MCARE_ID as MCARE_ID,
 MBR_SFX_NO as MBR_SFX_NO,
 PATN_ACCT_NO as PATN_ACCT_NO,
 PAYMT_REF_ID as PAYMT_REF_ID,
 PROV_AGMNT_ID as PROV_AGMNT_ID,
 RFRNG_PROV_TX as RFRNG_PROV_TX,
 SUB_ID as SUB_ID,
 FNCL_LOB_SK as FNCL_LOB_SK,
 ALPHA_PFX_SK as ALPHA_PFX_SK,
 CLM_ACDNT_CD_SK as CLM_ACDNT_CD_SK,
 HOST_IN as HOST_IN,
 NG1.TRGT_CD as SRC_SYS_CD,
 CLM.MCAID_STTUS_ID as MCAID_STTUS_ID,
 CLM.PATN_PD_AMT as PATN_PD_AMT,
 CLM.CLM_SUBMT_ICD_VRSN_CD_SK as CLM_SUBMT_ICD_VRSN_CD_SK,
 CLM.CLM_TXNMY_CD,
 CLM.BILL_PAYMT_EXCL_IN
FROM 
 """ + IDSOwner + """.CLM CLM,
 """ + IDSOwner + """.W_DRUG_CLM ADJ,
 """ + IDSOwner + """.CD_MPPNG NG1
WHERE 
 ADJ.CLM_SK = CLM.CLM_SK
 AND ADJ.SRC_SYS_CD_SK = NG1.CD_MPPNG_SK
"""

df_adj_clm = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", adj_clm_sql)
    .load()
)

extract_query_orig_clm_hash = f"""
SELECT 
R.RVRSL_CLM_SK as CLM_SK, 
NG.SRC_SYS_CD,
R.SRC_SYS_CD_SK,
R.RVRSL_CLM_ID as CLM_ID, 
CLM.CRT_RUN_CYC_EXCTN_SK,
CLM.LAST_UPDT_RUN_CYC_EXCTN_SK,
CLM.CLM_SK as ORIG_CLM_SK,
CLM.CLM_ID as ORIG_CLM_ID,
ADJ_FROM_CLM_SK,ADJ_TO_CLM_SK,CLS_SK,CLS_PLN_SK,EXPRNC_CAT_SK,FNCL_LOB_SK,GRP_SK,MBR_SK,NTWK_SK,PROD_SK,SUBGRP_SK,SUB_SK,CLM_ACDNT_ST_CD_SK,CLM_ACTV_BCBS_PLN_CD_SK,CLM_AGMNT_SRC_CD_SK,CLM_BTCH_ACTN_CD_SK,CLM_CAP_CD_SK,CLM_CAT_CD_SK,CLM_CHK_CYC_OVRD_CD_SK,CLM_COB_CD_SK,CLM_EOB_EXCD_SK,CLM_FINL_DISP_CD_SK,CLM_INPT_METH_CD_SK,CLM_INPT_SRC_CD_SK,CLM_INTER_PLN_PGM_CD_SK,CLM_NTWK_STTUS_CD_SK,CLM_NONPAR_PROV_PFX_CD_SK,CLM_OTHR_BNF_CD_SK,CLM_PAYE_CD_SK,CLM_PRCS_CTL_AGNT_PFX_CD_SK,CLM_SVC_DEFN_PFX_CD_SK,CLM_SVC_PROV_SPEC_CD_SK,CLM_SVC_PROV_TYP_CD_SK,CLM_STTUS_CD_SK,CLM_SUBMTTING_BCBS_PLN_CD_SK,CLM_SUB_BCBS_PLN_CD_SK,CLM_SUBTYP_CD_SK,CLM_TYP_CD_SK,ATCHMT_IN,CLNCL_EDIT_IN,COBRA_CLM_IN,FIRST_PASS_IN,LTR_IN,MCARE_ASG_IN,NOTE_IN,PCA_AUDIT_IN,PCP_SUBMT_IN,PROD_OOA_IN,ACDNT_DT_SK,INPT_DT_SK,MBR_PLN_ELIG_DT_SK,NEXT_RVW_DT_SK,PD_DT_SK,PAYMT_DRAG_CYC_DT_SK,PRCS_DT_SK,RCVD_DT_SK,SVC_STRT_DT_SK,SVC_END_DT_SK,SMLR_ILNS_DT_SK,STTUS_DT_SK,WORK_UNABLE_BEG_DT_SK,WORK_UNABLE_END_DT_SK,ACDNT_AMT,ACTL_PD_AMT,ALW_AMT,DSALW_AMT,COINS_AMT,CNSD_CHRG_AMT,COPAY_AMT,CHRG_AMT,DEDCT_AMT,PAYBL_AMT,CLM_CT,MBR_AGE,ADJ_FROM_CLM_ID,ADJ_TO_CLM_ID,DOC_TX_ID,MCAID_RESUB_NO,MCARE_ID,MBR_SFX_NO,PATN_ACCT_NO,PAYMT_REF_ID,PROV_AGMNT_ID,RFRNG_PROV_TX,SUB_ID,ALPHA_PFX_SK,CLM_ACDNT_CD_SK,HOST_IN,D.DRUG_CLM_SK,NDC_SK,PRSCRB_PROV_DEA_SK,DRUG_CLM_DISPNS_AS_WRTN_CD_SK,DRUG_CLM_LGL_STTUS_CD_SK,DRUG_CLM_TIER_CD_SK,DRUG_CLM_VNDR_STTUS_CD_SK,CMPND_IN,FRMLRY_IN,GNRC_DRUG_IN,MAIL_ORDER_IN,MNTN_IN,MAX_ALW_CST_REDC_IN,NON_FRMLRY_DRUG_IN,SNGL_SRC_IN,ADJ_DT_SK,D.FILL_DT_SK,DISPNS_FEE_AMT,HLTH_PLN_EXCL_AMT,HLTH_PLN_PD_AMT,INGR_CST_ALW_AMT,INGR_CST_CHRGD_AMT,INGR_SAV_AMT,MBR_DEDCT_EXCL_AMT,MBR_DIFF_PD_AMT,MBR_OOP_AMT,MBR_OOP_EXCL_AMT,OTHR_SAV_AMT,RX_ALW_QTY,RX_SUBMT_QTY,SLS_TAX_AMT,RX_ALW_DAYS_SUPL_QTY,RX_ORIG_DAYS_SUPL_QTY,PDX_NTWK_ID,D.RX_NO,RFL_NO,D.VNDR_CLM_NO,VNDR_PREAUTH_ID,DRUG_CLM_BNF_FRMLRY_POL_CD_SK,DRUG_CLM_BNF_RSTRCT_CD_SK,DRUG_CLM_MCPARTD_COVDRUG_CD_SK,DRUG_CLM_PDX_NTWK_CD_SK,DRUG_CLM_PRAUTH_CD_SK,MNDTRY_MAIL_ORDER_IN,D.ADM_FEE_AMT,D.DRUG_CLM_BILL_BSS_CD_SK,D.AVG_WHLSL_PRICE_AMT,CLM.MCAID_STTUS_ID,CLM.PATN_PD_AMT, D.UCR_AMT, D.DRUG_CLM_PRTL_FILL_CD_SK, D.DRUG_CLM_PDX_TYP_CD_SK, D.INCNTV_FEE_AMT,CLM.CLM_TXNMY_CD,CLM.BILL_PAYMT_EXCL_IN,D.SPEC_DRUG_IN
FROM {IDSOwner}.CLM CLM, 
     {IDSOwner}.W_CLM_RVRSL  R, 
     {IDSOwner}.DRUG_CLM D, 
     {IDSOwner}.CD_MPPNG NG, 
     {IDSOwner}.CLM_PROV PROV,
     {IDSOwner}.W_DRUG_CLM ADJ 
WHERE R.ORIG_CLM_SK = CLM.CLM_SK
  and R.ORIG_CLM_SK = D.CLM_SK
  and R.SRC_SYS_CD_SK = NG.CD_MPPNG_SK
  and R.RVRSL_CLM_SK = ADJ.CLM_SK
  and CLM.CLM_SK = PROV.CLM_SK
  and PROV.PROV_SK = ADJ.PROV_SK
  and NG.TRGT_CD <> 'BCBSSC'

UNION

SELECT 
R.RVRSL_CLM_SK as CLM_SK, 
NG.SRC_SYS_CD,
R.SRC_SYS_CD_SK,
R.RVRSL_CLM_ID as CLM_ID,
CLM.CRT_RUN_CYC_EXCTN_SK,
CLM.LAST_UPDT_RUN_CYC_EXCTN_SK,
CLM.CLM_SK as ORIG_CLM_SK,
CLM.CLM_ID as ORIG_CLM_ID,
ADJ_FROM_CLM_SK,ADJ_TO_CLM_SK,CLS_SK,CLS_PLN_SK,EXPRNC_CAT_SK,FNCL_LOB_SK,GRP_SK,MBR_SK,NTWK_SK,PROD_SK,SUBGRP_SK,SUB_SK,CLM_ACDNT_ST_CD_SK,CLM_ACTV_BCBS_PLN_CD_SK,CLM_AGMNT_SRC_CD_SK,CLM_BTCH_ACTN_CD_SK,CLM_CAP_CD_SK,CLM_CAT_CD_SK,CLM_CHK_CYC_OVRD_CD_SK,CLM_COB_CD_SK,CLM_EOB_EXCD_SK,CLM_FINL_DISP_CD_SK,CLM_INPT_METH_CD_SK,CLM_INPT_SRC_CD_SK,CLM_INTER_PLN_PGM_CD_SK,CLM_NTWK_STTUS_CD_SK,CLM_NONPAR_PROV_PFX_CD_SK,CLM_OTHR_BNF_CD_SK,CLM_PAYE_CD_SK,CLM_PRCS_CTL_AGNT_PFX_CD_SK,CLM_SVC_DEFN_PFX_CD_SK,CLM_SVC_PROV_SPEC_CD_SK,CLM_SVC_PROV_TYP_CD_SK,CLM_STTUS_CD_SK,CLM_SUBMTTING_BCBS_PLN_CD_SK,CLM_SUB_BCBS_PLN_CD_SK,CLM_SUBTYP_CD_SK,CLM_TYP_CD_SK,ATCHMT_IN,CLNCL_EDIT_IN,COBRA_CLM_IN,FIRST_PASS_IN,LTR_IN,MCARE_ASG_IN,NOTE_IN,PCA_AUDIT_IN,PCP_SUBMT_IN,PROD_OOA_IN,ACDNT_DT_SK,INPT_DT_SK,MBR_PLN_ELIG_DT_SK,NEXT_RVW_DT_SK,PD_DT_SK,PAYMT_DRAG_CYC_DT_SK,PRCS_DT_SK,RCVD_DT_SK,SVC_STRT_DT_SK,SVC_END_DT_SK,SMLR_ILNS_DT_SK,STTUS_DT_SK,WORK_UNABLE_BEG_DT_SK,WORK_UNABLE_END_DT_SK,ACDNT_AMT,ACTL_PD_AMT,ALW_AMT,DSALW_AMT,COINS_AMT,CNSD_CHRG_AMT,COPAY_AMT,CHRG_AMT,DEDCT_AMT,PAYBL_AMT,CLM_CT,MBR_AGE,ADJ_FROM_CLM_ID,ADJ_TO_CLM_ID,DOC_TX_ID,MCAID_RESUB_NO,MCARE_ID,MBR_SFX_NO,PATN_ACCT_NO,PAYMT_REF_ID,PROV_AGMNT_ID,RFRNG_PROV_TX,SUB_ID,ALPHA_PFX_SK,CLM_ACDNT_CD_SK,HOST_IN,D.DRUG_CLM_SK,NDC_SK,PRSCRB_PROV_DEA_SK,DRUG_CLM_DISPNS_AS_WRTN_CD_SK,DRUG_CLM_LGL_STTUS_CD_SK,DRUG_CLM_TIER_CD_SK,DRUG_CLM_VNDR_STTUS_CD_SK,CMPND_IN,FRMLRY_IN,GNRC_DRUG_IN,MAIL_ORDER_IN,MNTN_IN,MAX_ALW_CST_REDC_IN,NON_FRMLRY_DRUG_IN,SNGL_SRC_IN,ADJ_DT_SK,D.FILL_DT_SK,DISPNS_FEE_AMT,HLTH_PLN_EXCL_AMT,HLTH_PLN_PD_AMT,INGR_CST_ALW_AMT,INGR_CST_CHRGD_AMT,INGR_SAV_AMT,MBR_DEDCT_EXCL_AMT,MBR_DIFF_PD_AMT,MBR_OOP_AMT,MBR_OOP_EXCL_AMT,OTHR_SAV_AMT,RX_ALW_QTY,RX_SUBMT_QTY,SLS_TAX_AMT,RX_ALW_DAYS_SUPL_QTY,RX_ORIG_DAYS_SUPL_QTY,PDX_NTWK_ID,D.RX_NO,RFL_NO,D.VNDR_CLM_NO,VNDR_PREAUTH_ID,DRUG_CLM_BNF_FRMLRY_POL_CD_SK,DRUG_CLM_BNF_RSTRCT_CD_SK,DRUG_CLM_MCPARTD_COVDRUG_CD_SK,DRUG_CLM_PDX_NTWK_CD_SK,DRUG_CLM_PRAUTH_CD_SK,MNDTRY_MAIL_ORDER_IN,D.ADM_FEE_AMT,D.DRUG_CLM_BILL_BSS_CD_SK,D.AVG_WHLSL_PRICE_AMT,CLM.MCAID_STTUS_ID,CLM.PATN_PD_AMT, D.UCR_AMT, D.DRUG_CLM_PRTL_FILL_CD_SK, D.DRUG_CLM_PDX_TYP_CD_SK, D.INCNTV_FEE_AMT,CLM.CLM_TXNMY_CD,CLM.BILL_PAYMT_EXCL_IN,D.SPEC_DRUG_IN
FROM {IDSOwner}.CLM CLM, 
     {IDSOwner}.W_CLM_RVRSL  R, 
     {IDSOwner}.DRUG_CLM D, 
     {IDSOwner}.CD_MPPNG NG, 
     {IDSOwner}.CLM_PROV PROV,
     {IDSOwner}.W_DRUG_CLM ADJ 
WHERE R.ORIG_CLM_SK = CLM.CLM_SK
  and R.ORIG_CLM_SK = D.CLM_SK
  and R.SRC_SYS_CD_SK = NG.CD_MPPNG_SK
  and R.RVRSL_CLM_SK = ADJ.CLM_SK
  and CLM.CLM_SK = PROV.CLM_SK
  and NG.TRGT_CD = 'BCBSSC'
"""

df_orig_clm_hash = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_orig_clm_hash)
    .load()
)

# --------------------------------------------------------------------------------
# clm_and_drug_clm_hash (StageType: CHashedFileStage) - Scenario A
# Deduplicate on key: CLM_SK
# Output pin: orig_drug_clm_lkup
# --------------------------------------------------------------------------------
df_orig_clm_hash_dedup = dedup_sort(df_orig_clm_hash, ["CLM_SK"], [("CLM_SK", "A")])

# Apply rpad for char columns, then select columns in the same order as defined:
df_orig_clm_lkup = df_orig_clm_hash_dedup.select(
    col("CLM_SK"),
    col("SRC_SYS_CD"),
    col("SRC_SYS_CD_SK"),
    col("CLM_ID"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("ORIG_CLM_SK"),
    col("ORIG_CLM_ID"),
    col("ADJ_FROM_CLM_SK"),
    col("ADJ_TO_CLM_SK"),
    col("CLS_SK"),
    col("CLS_PLN_SK"),
    col("EXPRNC_CAT_SK"),
    col("FNCL_LOB_SK"),
    col("GRP_SK"),
    col("MBR_SK"),
    col("NTWK_SK"),
    col("PROD_SK"),
    col("SUBGRP_SK"),
    col("SUB_SK"),
    col("CLM_ACDNT_ST_CD_SK"),
    col("CLM_ACTV_BCBS_PLN_CD_SK"),
    col("CLM_AGMNT_SRC_CD_SK"),
    col("CLM_BTCH_ACTN_CD_SK"),
    col("CLM_CAP_CD_SK"),
    col("CLM_CAT_CD_SK"),
    col("CLM_CHK_CYC_OVRD_CD_SK"),
    col("CLM_COB_CD_SK"),
    col("CLM_EOB_EXCD_SK"),
    col("CLM_FINL_DISP_CD_SK"),
    col("CLM_INPT_METH_CD_SK"),
    col("CLM_INPT_SRC_CD_SK"),
    col("CLM_INTER_PLN_PGM_CD_SK"),
    col("CLM_NTWK_STTUS_CD_SK"),
    col("CLM_NONPAR_PROV_PFX_CD_SK"),
    col("CLM_OTHR_BNF_CD_SK"),
    col("CLM_PAYE_CD_SK"),
    col("CLM_PRCS_CTL_AGNT_PFX_CD_SK"),
    col("CLM_SVC_DEFN_PFX_CD_SK"),
    col("CLM_SVC_PROV_SPEC_CD_SK"),
    col("CLM_SVC_PROV_TYP_CD_SK"),
    col("CLM_STTUS_CD_SK"),
    col("CLM_SUBMTTING_BCBS_PLN_CD_SK"),
    col("CLM_SUB_BCBS_PLN_CD_SK"),
    col("CLM_SUBTYP_CD_SK"),
    col("CLM_TYP_CD_SK"),
    rpad(col("ATCHMT_IN"), 1, " ").alias("ATCHMT_IN"),
    rpad(col("CLNCL_EDIT_IN"), 1, " ").alias("CLNCL_EDIT_IN"),
    rpad(col("COBRA_CLM_IN"), 1, " ").alias("COBRA_CLM_IN"),
    rpad(col("FIRST_PASS_IN"), 1, " ").alias("FIRST_PASS_IN"),
    rpad(col("LTR_IN"), 1, " ").alias("LTR_IN"),
    rpad(col("MCARE_ASG_IN"), 1, " ").alias("MCARE_ASG_IN"),
    rpad(col("NOTE_IN"), 1, " ").alias("NOTE_IN"),
    rpad(col("PCA_AUDIT_IN"), 1, " ").alias("PCA_AUDIT_IN"),
    rpad(col("PCP_SUBMT_IN"), 1, " ").alias("PCP_SUBMT_IN"),
    rpad(col("PROD_OOA_IN"), 1, " ").alias("PROD_OOA_IN"),
    rpad(col("ACDNT_DT_SK"), 10, " ").alias("ACDNT_DT_SK"),
    rpad(col("INPT_DT_SK"), 10, " ").alias("INPT_DT_SK"),
    rpad(col("MBR_PLN_ELIG_DT_SK"), 10, " ").alias("MBR_PLN_ELIG_DT_SK"),
    rpad(col("NEXT_RVW_DT_SK"), 10, " ").alias("NEXT_RVW_DT_SK"),
    rpad(col("PD_DT_SK"), 10, " ").alias("PD_DT_SK"),
    rpad(col("PAYMT_DRAG_CYC_DT_SK"), 10, " ").alias("PAYMT_DRAG_CYC_DT_SK"),
    rpad(col("PRCS_DT_SK"), 10, " ").alias("PRCS_DT_SK"),
    rpad(col("RCVD_DT_SK"), 10, " ").alias("RCVD_DT_SK"),
    rpad(col("SVC_STRT_DT_SK"), 10, " ").alias("SVC_STRT_DT_SK"),
    rpad(col("SVC_END_DT_SK"), 10, " ").alias("SVC_END_DT_SK"),
    rpad(col("SMLR_ILNS_DT_SK"), 10, " ").alias("SMLR_ILNS_DT_SK"),
    rpad(col("STTUS_DT_SK"), 10, " ").alias("STTUS_DT_SK"),
    rpad(col("WORK_UNABLE_BEG_DT_SK"), 10, " ").alias("WORK_UNABLE_BEG_DT_SK"),
    rpad(col("WORK_UNABLE_END_DT_SK"), 10, " ").alias("WORK_UNABLE_END_DT_SK"),
    col("ACDNT_AMT"),
    col("ACTL_PD_AMT"),
    col("ALW_AMT"),
    col("DSALW_AMT"),
    col("COINS_AMT"),
    col("CNSD_CHRG_AMT"),
    col("COPAY_AMT"),
    col("CHRG_AMT"),
    col("DEDCT_AMT"),
    col("PAYBL_AMT"),
    col("CLM_CT"),
    col("MBR_AGE"),
    col("ADJ_FROM_CLM_ID"),
    col("ADJ_TO_CLM_ID"),
    rpad(col("DOC_TX_ID"), 18, " ").alias("DOC_TX_ID"),
    rpad(col("MCAID_RESUB_NO"), 15, " ").alias("MCAID_RESUB_NO"),
    rpad(col("MCARE_ID"), 12, " ").alias("MCARE_ID"),
    rpad(col("MBR_SFX_NO"), 2, " ").alias("MBR_SFX_NO"),
    col("PATN_ACCT_NO"),
    rpad(col("PAYMT_REF_ID"), 16, " ").alias("PAYMT_REF_ID"),
    rpad(col("PROV_AGMNT_ID"), 12, " ").alias("PROV_AGMNT_ID"),
    col("RFRNG_PROV_TX"),
    rpad(col("SUB_ID"), 14, " ").alias("SUB_ID"),
    col("ALPHA_PFX_SK"),
    col("CLM_ACDNT_CD_SK"),
    rpad(col("HOST_IN"), 1, " ").alias("HOST_IN"),
    col("DRUG_CLM_SK"),
    col("NDC_SK"),
    col("PRSCRB_PROV_DEA_SK"),
    col("DRUG_CLM_DISPNS_AS_WRTN_CD_SK"),
    col("DRUG_CLM_LGL_STTUS_CD_SK"),
    col("DRUG_CLM_TIER_CD_SK"),
    col("DRUG_CLM_VNDR_STTUS_CD_SK"),
    rpad(col("CMPND_IN"), 1, " ").alias("CMPND_IN"),
    rpad(col("FRMLRY_IN"), 1, " ").alias("FRMLRY_IN"),
    rpad(col("GNRC_DRUG_IN"), 1, " ").alias("GNRC_DRUG_IN"),
    rpad(col("MAIL_ORDER_IN"), 1, " ").alias("MAIL_ORDER_IN"),
    rpad(col("MNTN_IN"), 1, " ").alias("MNTN_IN"),
    rpad(col("MAX_ALW_CST_REDC_IN"), 1, " ").alias("MAX_ALW_CST_REDC_IN"),
    rpad(col("NON_FRMLRY_DRUG_IN"), 1, " ").alias("NON_FRMLRY_DRUG_IN"),
    rpad(col("SNGL_SRC_IN"), 1, " ").alias("SNGL_SRC_IN"),
    rpad(col("ADJ_DT_SK"), 10, " ").alias("ADJ_DT_SK"),
    rpad(col("FILL_DT_SK"), 10, " ").alias("FILL_DT_SK"),
    col("DISPNS_FEE_AMT"),
    col("HLTH_PLN_EXCL_AMT"),
    col("HLTH_PLN_PD_AMT"),
    col("INGR_CST_ALW_AMT"),
    col("INGR_CST_CHRGD_AMT"),
    col("INGR_SAV_AMT"),
    col("MBR_DEDCT_EXCL_AMT"),
    col("MBR_DIFF_PD_AMT"),
    col("MBR_OOP_AMT"),
    col("MBR_OOP_EXCL_AMT"),
    col("OTHR_SAV_AMT"),
    col("RX_ALW_QTY"),
    col("RX_SUBMT_QTY"),
    col("SLS_TAX_AMT"),
    col("RX_ALW_DAYS_SUPL_QTY"),
    col("RX_ORIG_DAYS_SUPL_QTY"),
    col("PDX_NTWK_ID"),
    col("RX_NO"),
    col("RFL_NO"),
    col("VNDR_CLM_NO"),
    col("VNDR_PREAUTH_ID"),
    col("DRUG_CLM_BNF_FRMLRY_POL_CD_SK"),
    col("DRUG_CLM_BNF_RSTRCT_CD_SK"),
    col("DRUG_CLM_MCPARTD_COVDRUG_CD_SK"),
    col("DRUG_CLM_PDX_NTWK_CD_SK"),
    col("DRUG_CLM_PRAUTH_CD_SK"),
    rpad(col("MNDTRY_MAIL_ORDER_IN"), 1, " ").alias("MNDTRY_MAIL_ORDER_IN"),
    col("ADM_FEE_AMT"),
    col("DRUG_CLM_BILL_BSS_CD_SK"),
    col("AVG_WHLSL_PRICE_AMT"),
    col("MCAID_STTUS_ID"),
    col("PATN_PD_AMT"),
    col("UCR_AMT"),
    col("DRUG_CLM_PRTL_FILL_CD_SK"),
    col("DRUG_CLM_PDX_TYP_CD_SK"),
    col("INCNTV_FEE_AMT"),
    col("CLM_TXNMY_CD"),
    rpad(col("BILL_PAYMT_EXCL_IN"), 1, " ").alias("BILL_PAYMT_EXCL_IN"),
    rpad(col("SPEC_DRUG_IN"), 1, " ").alias("SPEC_DRUG_IN")
)

df_clm = (
    df_adj_clm.alias("adj_clm")
    .join(df_orig_clm_lkup.alias("orig_clm_lkup"), col("adj_clm.CLM_SK") == col("orig_clm_lkup.CLM_SK"), "left")
    .withColumn("svStatusCd", GetFkeyCodes(col("adj_clm.SRC_SYS_CD"), col("orig_clm_lkup.ORIG_CLM_SK"), lit("CLAIM STATUS"), lit("A09"), lit("X")))
)

df_adj_claims_out = (
    df_clm
    .filter(~isnull(col("orig_clm_lkup.CLM_SK")))
    .select(
        col("adj_clm.CLM_SK").alias("CLM_SK"),
        col("adj_clm.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        col("adj_clm.CLM_ID").alias("CLM_ID"),
        col("adj_clm.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("adj_clm.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        when(isnull(col("orig_clm_lkup.ORIG_CLM_SK")), lit(0)).otherwise(col("orig_clm_lkup.ORIG_CLM_SK")).alias("ADJ_FROM_CLM_SK"),
        col("adj_clm.ADJ_TO_CLM_SK").alias("ADJ_TO_CLM_SK"),
        when(isnull(col("orig_clm_lkup.ALPHA_PFX_SK")), lit(0)).otherwise(col("orig_clm_lkup.ALPHA_PFX_SK")).alias("ALPHA_PFX_SK"),
        when(isnull(col("orig_clm_lkup.CLM_EOB_EXCD_SK")), lit(0)).otherwise(col("orig_clm_lkup.CLM_EOB_EXCD_SK")).alias("CLM_EOB_EXCD_SK"),
        when(isnull(col("orig_clm_lkup.CLS_SK")), lit(0)).otherwise(col("orig_clm_lkup.CLS_SK")).alias("CLS_SK"),
        when(isnull(col("orig_clm_lkup.CLS_PLN_SK")), lit(0)).otherwise(col("orig_clm_lkup.CLS_PLN_SK")).alias("CLS_PLN_SK"),
        when(isnull(col("orig_clm_lkup.EXPRNC_CAT_SK")), lit(0)).otherwise(col("orig_clm_lkup.EXPRNC_CAT_SK")).alias("EXPRNC_CAT_SK"),
        when(isnull(col("orig_clm_lkup.FNCL_LOB_SK")), lit(0)).otherwise(col("orig_clm_lkup.FNCL_LOB_SK")).alias("FNCL_LOB_SK"),
        when(isnull(col("orig_clm_lkup.GRP_SK")), lit(0)).otherwise(col("orig_clm_lkup.GRP_SK")).alias("GRP_SK"),
        when(isnull(col("orig_clm_lkup.MBR_SK")), lit(0)).otherwise(col("orig_clm_lkup.MBR_SK")).alias("MBR_SK"),
        when(isnull(col("orig_clm_lkup.NTWK_SK")), lit(0)).otherwise(col("orig_clm_lkup.NTWK_SK")).alias("NTWK_SK"),
        when(isnull(col("orig_clm_lkup.PROD_SK")), lit(0)).otherwise(col("orig_clm_lkup.PROD_SK")).alias("PROD_SK"),
        when(isnull(col("orig_clm_lkup.SUBGRP_SK")), lit(0)).otherwise(col("orig_clm_lkup.SUBGRP_SK")).alias("SUBGRP_SK"),
        when(isnull(col("orig_clm_lkup.SUB_SK")), lit(0)).otherwise(col("orig_clm_lkup.SUB_SK")).alias("SUB_SK"),
        when(isnull(col("orig_clm_lkup.CLM_ACDNT_CD_SK")), lit(0)).otherwise(col("orig_clm_lkup.CLM_ACDNT_CD_SK")).alias("CLM_ACDNT_CD_SK"),
        when(isnull(col("orig_clm_lkup.CLM_ACDNT_ST_CD_SK")), lit(0)).otherwise(col("orig_clm_lkup.CLM_ACDNT_ST_CD_SK")).alias("CLM_ACDNT_ST_CD_SK"),
        when(isnull(col("orig_clm_lkup.CLM_ACTV_BCBS_PLN_CD_SK")), lit(0)).otherwise(col("orig_clm_lkup.CLM_ACTV_BCBS_PLN_CD_SK")).alias("CLM_ACTV_BCBS_PLN_CD_SK"),
        when(isnull(col("orig_clm_lkup.CLM_AGMNT_SRC_CD_SK")), lit(0)).otherwise(col("orig_clm_lkup.CLM_AGMNT_SRC_CD_SK")).alias("CLM_AGMNT_SRC_CD_SK"),
        when(isnull(col("orig_clm_lkup.CLM_BTCH_ACTN_CD_SK")), lit(0)).otherwise(col("orig_clm_lkup.CLM_BTCH_ACTN_CD_SK")).alias("CLM_BTCH_ACTN_CD_SK"),
        when(isnull(col("orig_clm_lkup.CLM_CAP_CD_SK")), lit(0)).otherwise(col("orig_clm_lkup.CLM_CAP_CD_SK")).alias("CLM_CAP_CD_SK"),
        when(isnull(col("orig_clm_lkup.CLM_CAT_CD_SK")), lit(0)).otherwise(col("orig_clm_lkup.CLM_CAT_CD_SK")).alias("CLM_CAT_CD_SK"),
        when(isnull(col("orig_clm_lkup.CLM_CHK_CYC_OVRD_CD_SK")), lit(0)).otherwise(col("orig_clm_lkup.CLM_CHK_CYC_OVRD_CD_SK")).alias("CLM_CHK_CYC_OVRD_CD_SK"),
        when(isnull(col("orig_clm_lkup.CLM_COB_CD_SK")), lit(0)).otherwise(col("orig_clm_lkup.CLM_COB_CD_SK")).alias("CLM_COB_CD_SK"),
        when(isnull(col("orig_clm_lkup.CLM_FINL_DISP_CD_SK")), lit(0)).otherwise(col("orig_clm_lkup.CLM_FINL_DISP_CD_SK")).alias("CLM_FINL_DISP_CD_SK"),
        when(isnull(col("orig_clm_lkup.CLM_INPT_METH_CD_SK")), lit(0)).otherwise(col("orig_clm_lkup.CLM_INPT_METH_CD_SK")).alias("CLM_INPT_METH_CD_SK"),
        when(isnull(col("orig_clm_lkup.CLM_INPT_SRC_CD_SK")), lit(0)).otherwise(col("orig_clm_lkup.CLM_INPT_SRC_CD_SK")).alias("CLM_INPT_SRC_CD_SK"),
        when(isnull(col("orig_clm_lkup.CLM_INTER_PLN_PGM_CD_SK")), lit(0)).otherwise(col("orig_clm_lkup.CLM_INTER_PLN_PGM_CD_SK")).alias("CLM_INTER_PLN_PGM_CD_SK"),
        when(isnull(col("orig_clm_lkup.CLM_NTWK_STTUS_CD_SK")), lit(0)).otherwise(col("orig_clm_lkup.CLM_NTWK_STTUS_CD_SK")).alias("CLM_NTWK_STTUS_CD_SK"),
        when(isnull(col("orig_clm_lkup.CLM_NONPAR_PROV_PFX_CD_SK")), lit(0)).otherwise(col("orig_clm_lkup.CLM_NONPAR_PROV_PFX_CD_SK")).alias("CLM_NONPAR_PROV_PFX_CD_SK"),
        when(isnull(col("orig_clm_lkup.CLM_OTHR_BNF_CD_SK")), lit(0)).otherwise(col("orig_clm_lkup.CLM_OTHR_BNF_CD_SK")).alias("CLM_OTHR_BNF_CD_SK"),
        when(isnull(col("orig_clm_lkup.CLM_PAYE_CD_SK")), lit(0)).otherwise(col("orig_clm_lkup.CLM_PAYE_CD_SK")).alias("CLM_PAYE_CD_SK"),
        when(isnull(col("orig_clm_lkup.CLM_PRCS_CTL_AGNT_PFX_CD_SK")), lit(0)).otherwise(col("orig_clm_lkup.CLM_PRCS_CTL_AGNT_PFX_CD_SK")).alias("CLM_PRCS_CTL_AGNT_PFX_CD_SK"),
        when(isnull(col("orig_clm_lkup.CLM_SVC_DEFN_PFX_CD_SK")), lit(0)).otherwise(col("orig_clm_lkup.CLM_SVC_DEFN_PFX_CD_SK")).alias("CLM_SVC_DEFN_PFX_CD_SK"),
        when(isnull(col("orig_clm_lkup.CLM_SVC_PROV_SPEC_CD_SK")), lit(0)).otherwise(col("orig_clm_lkup.CLM_SVC_PROV_SPEC_CD_SK")).alias("CLM_SVC_PROV_SPEC_CD_SK"),
        when(isnull(col("orig_clm_lkup.CLM_SVC_PROV_TYP_CD_SK")), lit(0)).otherwise(col("orig_clm_lkup.CLM_SVC_PROV_TYP_CD_SK")).alias("CLM_SVC_PROV_TYP_CD_SK"),
        when(isnull(col("adj_clm.CLM_STTUS_CD_SK")), lit(0)).otherwise(col("adj_clm.CLM_STTUS_CD_SK")).alias("CLM_STTUS_CD_SK"),
        when(isnull(col("orig_clm_lkup.CLM_SUBMTTING_BCBS_PLN_CD_SK")), lit(0)).otherwise(col("orig_clm_lkup.CLM_SUBMTTING_BCBS_PLN_CD_SK")).alias("CLM_SUBMTTING_BCBS_PLN_CD_SK"),
        when(isnull(col("orig_clm_lkup.CLM_SUB_BCBS_PLN_CD_SK")), lit(0)).otherwise(col("orig_clm_lkup.CLM_SUB_BCBS_PLN_CD_SK")).alias("CLM_SUB_BCBS_PLN_CD_SK"),
        when(isnull(col("orig_clm_lkup.CLM_SUBTYP_CD_SK")), lit(0)).otherwise(col("orig_clm_lkup.CLM_SUBTYP_CD_SK")).alias("CLM_SUBTYP_CD_SK"),
        when(isnull(col("orig_clm_lkup.CLM_TYP_CD_SK")), lit(0)).otherwise(col("orig_clm_lkup.CLM_TYP_CD_SK")).alias("CLM_TYP_CD_SK"),
        when(isnull(col("orig_clm_lkup.ATCHMT_IN")), lit(" ")).otherwise(col("orig_clm_lkup.ATCHMT_IN")).alias("ATCHMT_IN"),
        when(isnull(col("orig_clm_lkup.CLNCL_EDIT_IN")), lit(" ")).otherwise(col("orig_clm_lkup.CLNCL_EDIT_IN")).alias("CLNCL_EDIT_IN"),
        when(isnull(col("orig_clm_lkup.COBRA_CLM_IN")), lit(" ")).otherwise(col("orig_clm_lkup.COBRA_CLM_IN")).alias("COBRA_CLM_IN"),
        when(isnull(col("orig_clm_lkup.FIRST_PASS_IN")), lit(" ")).otherwise(col("orig_clm_lkup.FIRST_PASS_IN")).alias("FIRST_PASS_IN"),
        when(isnull(col("orig_clm_lkup.HOST_IN")), lit(" ")).otherwise(col("orig_clm_lkup.HOST_IN")).alias("HOST_IN"),
        when(isnull(col("orig_clm_lkup.LTR_IN")), lit(" ")).otherwise(col("orig_clm_lkup.LTR_IN")).alias("LTR_IN"),
        when(isnull(col("orig_clm_lkup.MCARE_ASG_IN")), lit(" ")).otherwise(col("orig_clm_lkup.MCARE_ASG_IN")).alias("MCARE_ASG_IN"),
        when(isnull(col("orig_clm_lkup.NOTE_IN")), lit(" ")).otherwise(col("orig_clm_lkup.NOTE_IN")).alias("NOTE_IN"),
        when(isnull(col("orig_clm_lkup.PCA_AUDIT_IN")), lit(" ")).otherwise(col("orig_clm_lkup.PCA_AUDIT_IN")).alias("PCA_AUDIT_IN"),
        when(isnull(col("orig_clm_lkup.PCP_SUBMT_IN")), lit(" ")).otherwise(col("orig_clm_lkup.PCP_SUBMT_IN")).alias("PCP_SUBMT_IN"),
        when(isnull(col("orig_clm_lkup.PROD_OOA_IN")), lit(" ")).otherwise(col("orig_clm_lkup.PROD_OOA_IN")).alias("PROD_OOA_IN"),
        when(isnull(col("orig_clm_lkup.ACDNT_DT_SK")), lit(0)).otherwise(col("orig_clm_lkup.ACDNT_DT_SK")).alias("ACDNT_DT_SK"),
        when(col("adj_clm.SRC_SYS_CD") == lit("BCBSSC"), col("adj_clm.INPT_DT_SK"))
            .otherwise(coalesce(col("orig_clm_lkup.INPT_DT_SK"), lit(0))).alias("INPT_DT_SK"),
        when(isnull(col("orig_clm_lkup.MBR_PLN_ELIG_DT_SK")), lit(0)).otherwise(col("orig_clm_lkup.MBR_PLN_ELIG_DT_SK")).alias("MBR_PLN_ELIG_DT_SK"),
        when(isnull(col("orig_clm_lkup.NEXT_RVW_DT_SK")), lit(0)).otherwise(col("orig_clm_lkup.NEXT_RVW_DT_SK")).alias("NEXT_RVW_DT_SK"),
        when(isnull(col("adj_clm.PD_DT_SK")), lit(0)).otherwise(col("adj_clm.PD_DT_SK")).alias("PD_DT_SK"),
        when(isnull(col("orig_clm_lkup.PAYMT_DRAG_CYC_DT_SK")), lit(0)).otherwise(col("orig_clm_lkup.PAYMT_DRAG_CYC_DT_SK")).alias("PAYMT_DRAG_CYC_DT_SK"),
        when(isnull(col("adj_clm.PRCS_DT_SK")), lit(0)).otherwise(col("adj_clm.PRCS_DT_SK")).alias("PRCS_DT_SK"),
        when(col("adj_clm.SRC_SYS_CD") == lit("BCBSSC"), col("adj_clm.RCVD_DT_SK"))
            .otherwise(coalesce(col("orig_clm_lkup.RCVD_DT_SK"), lit(0))).alias("RCVD_DT_SK"),
        when(isnull(col("orig_clm_lkup.SVC_STRT_DT_SK")), lit(0)).otherwise(col("orig_clm_lkup.SVC_STRT_DT_SK")).alias("SVC_STRT_DT_SK"),
        when(isnull(col("orig_clm_lkup.SVC_END_DT_SK")), lit(0)).otherwise(col("orig_clm_lkup.SVC_END_DT_SK")).alias("SVC_END_DT_SK"),
        when(isnull(col("orig_clm_lkup.SMLR_ILNS_DT_SK")), lit(0)).otherwise(col("orig_clm_lkup.SMLR_ILNS_DT_SK")).alias("SMLR_ILNS_DT_SK"),
        when(isnull(col("adj_clm.STTUS_DT_SK")), lit(0)).otherwise(col("adj_clm.STTUS_DT_SK")).alias("STTUS_DT_SK"),
        when(isnull(col("orig_clm_lkup.WORK_UNABLE_BEG_DT_SK")), lit(0)).otherwise(col("orig_clm_lkup.WORK_UNABLE_BEG_DT_SK")).alias("WORK_UNABLE_BEG_DT_SK"),
        when(isnull(col("orig_clm_lkup.WORK_UNABLE_END_DT_SK")), lit(0)).otherwise(col("orig_clm_lkup.WORK_UNABLE_END_DT_SK")).alias("WORK_UNABLE_END_DT_SK"),
        when(isnull(col("orig_clm_lkup.ACDNT_AMT")), lit(0)).otherwise(col("orig_clm_lkup.ACDNT_AMT") * lit(-1)).alias("ACDNT_AMT"),
        when(isnull(col("orig_clm_lkup.ACTL_PD_AMT")), lit(0)).otherwise(col("orig_clm_lkup.ACTL_PD_AMT") * lit(-1)).alias("ACTL_PD_AMT"),
        when(isnull(col("orig_clm_lkup.ALW_AMT")), lit(0)).otherwise(col("orig_clm_lkup.ALW_AMT") * lit(-1)).alias("ALW_AMT"),
        when(isnull(col("orig_clm_lkup.DSALW_AMT")), lit(0)).otherwise(col("orig_clm_lkup.DSALW_AMT") * lit(-1)).alias("DSALW_AMT"),
        when(isnull(col("orig_clm_lkup.COINS_AMT")), lit(0)).otherwise(col("orig_clm_lkup.COINS_AMT") * lit(-1)).alias("COINS_AMT"),
        when(isnull(col("orig_clm_lkup.CNSD_CHRG_AMT")), lit(0)).otherwise(col("orig_clm_lkup.CNSD_CHRG_AMT") * lit(-1)).alias("CNSD_CHRG_AMT"),
        when(isnull(col("orig_clm_lkup.COPAY_AMT")), lit(0)).otherwise(col("orig_clm_lkup.COPAY_AMT") * lit(-1)).alias("COPAY_AMT"),
        when(isnull(col("orig_clm_lkup.CHRG_AMT")), lit(0)).otherwise(col("orig_clm_lkup.CHRG_AMT") * lit(-1)).alias("CHRG_AMT"),
        when(isnull(col("orig_clm_lkup.DEDCT_AMT")), lit(0)).otherwise(col("orig_clm_lkup.DEDCT_AMT") * lit(-1)).alias("DEDCT_AMT"),
        when(isnull(col("orig_clm_lkup.PAYBL_AMT")), lit(0)).otherwise(col("orig_clm_lkup.PAYBL_AMT") * lit(-1)).alias("PAYBL_AMT"),
        when(isnull(col("orig_clm_lkup.CLM_CT")), lit(0)).otherwise(col("orig_clm_lkup.CLM_CT") * lit(-1)).alias("CLM_CT"),
        when(isnull(col("orig_clm_lkup.MBR_AGE")), lit(0)).otherwise(col("orig_clm_lkup.MBR_AGE")).alias("MBR_AGE"),
        when(isnull(col("orig_clm_lkup.ORIG_CLM_ID")), lit(" ")).otherwise(col("orig_clm_lkup.ORIG_CLM_ID")).alias("ADJ_FROM_CLM_ID"),
        when(isnull(col("adj_clm.ADJ_TO_CLM_ID")), lit(" ")).otherwise(col("adj_clm.ADJ_TO_CLM_ID")).alias("ADJ_TO_CLM_ID"),
        when(isnull(col("orig_clm_lkup.DOC_TX_ID")), lit(" ")).otherwise(col("orig_clm_lkup.DOC_TX_ID")).alias("DOC_TX_ID"),
        when(isnull(col("orig_clm_lkup.MCAID_RESUB_NO")), None).otherwise(col("orig_clm_lkup.MCAID_RESUB_NO")).alias("MCAID_RESUB_NO"),
        when(isnull(col("orig_clm_lkup.MCARE_ID")), lit(" ")).otherwise(col("orig_clm_lkup.MCARE_ID")).alias("MCARE_ID"),
        when(isnull(col("orig_clm_lkup.MBR_SFX_NO")), lit(" ")).otherwise(col("orig_clm_lkup.MBR_SFX_NO")).alias("MBR_SFX_NO"),
        when(isnull(col("orig_clm_lkup.PATN_ACCT_NO")), None).otherwise(col("orig_clm_lkup.PATN_ACCT_NO")).alias("PATN_ACCT_NO"),
        when(isnull(col("orig_clm_lkup.PAYMT_REF_ID")), lit(" ")).otherwise(col("orig_clm_lkup.PAYMT_REF_ID")).alias("PAYMT_REF_ID"),
        when(isnull(col("orig_clm_lkup.PROV_AGMNT_ID")), lit(" ")).otherwise(col("orig_clm_lkup.PROV_AGMNT_ID")).alias("PROV_AGMNT_ID"),
        when(isnull(col("orig_clm_lkup.RFRNG_PROV_TX")), None).otherwise(col("orig_clm_lkup.RFRNG_PROV_TX")).alias("RFRNG_PROV_TX"),
        when(isnull(col("orig_clm_lkup.SUB_ID")), lit(" ")).otherwise(col("orig_clm_lkup.SUB_ID")).alias("SUB_ID"),
        lit("1").alias("PCA_TYP_CD_SK"),
        lit("1").alias("REL_PCA_CLM_SK"),
        lit("1").alias("REL_BASE_CLM_SK"),
        lit("0").alias("REMIT_SUPRSION_AMT"),
        when(isnull(col("orig_clm_lkup.SUB_ID")), lit(" ")).otherwise(col("adj_clm.MCAID_STTUS_ID")).alias("MCAID_STTUS_ID"),
        when(isnull(col("orig_clm_lkup.PATN_PD_AMT")), lit(0.00)).otherwise(col("orig_clm_lkup.PATN_PD_AMT") * lit(-1)).alias("PATN_PD_AMT"),
        col("adj_clm.CLM_SUBMT_ICD_VRSN_CD_SK").alias("CLM_SUBMT_ICD_VRSN_CD_SK"),
        when(isnull(col("orig_clm_lkup.CLM_TXNMY_CD")), None).otherwise(col("orig_clm_lkup.CLM_TXNMY_CD")).alias("CLM_TXNMY_CD"),
        col("orig_clm_lkup.BILL_PAYMT_EXCL_IN").alias("BILL_PAYMT_EXCL_IN")
    )
)

df_adj_claims_out = (
    df_adj_claims_out
    .withColumn("ATCHMT_IN", rpad(col("ATCHMT_IN"), 1, " "))
    .withColumn("CLNCL_EDIT_IN", rpad(col("CLNCL_EDIT_IN"), 1, " "))
    .withColumn("COBRA_CLM_IN", rpad(col("COBRA_CLM_IN"), 1, " "))
    .withColumn("FIRST_PASS_IN", rpad(col("FIRST_PASS_IN"), 1, " "))
    .withColumn("HOST_IN", rpad(col("HOST_IN"), 1, " "))
    .withColumn("LTR_IN", rpad(col("LTR_IN"), 1, " "))
    .withColumn("MCARE_ASG_IN", rpad(col("MCARE_ASG_IN"), 1, " "))
    .withColumn("NOTE_IN", rpad(col("NOTE_IN"), 1, " "))
    .withColumn("PCA_AUDIT_IN", rpad(col("PCA_AUDIT_IN"), 1, " "))
    .withColumn("PCP_SUBMT_IN", rpad(col("PCP_SUBmt_IN"), 1, " "))
    .withColumn("PROD_OOA_IN", rpad(col("PROD_OOA_IN"), 1, " "))
    .withColumn("DOC_TX_ID", rpad(col("DOC_TX_ID"), 18, " "))
    .withColumn("MCAID_RESUB_NO", col("MCAID_RESUB_NO"))  # no explicit length
    .withColumn("MCARE_ID", rpad(col("MCARE_ID"), 12, " "))
    .withColumn("MBR_SFX_NO", rpad(col("MBR_SFX_NO"), 2, " "))
    .withColumn("PAYMT_REF_ID", rpad(col("PAYMT_REF_ID"), 16, " "))
    .withColumn("PROV_AGMNT_ID", rpad(col("PROV_AGMNT_ID"), 12, " "))
    .withColumn("SUB_ID", rpad(col("SUB_ID"), 14, " "))
    .withColumn("BILL_PAYMT_EXCL_IN", rpad(col("BILL_PAYMT_EXCL_IN"), 1, " "))
)

df_orig_clm_out = (
    df_clm
    .filter(~isnull(col("orig_clm_lkup.CLM_SK")))
    .select(
        when(isnull(col("orig_clm_lkup.ORIG_CLM_SK")), lit(0)).otherwise(col("orig_clm_lkup.ORIG_CLM_SK")).alias("CLM_SK"),
        when(isnull(col("orig_clm_lkup.SRC_SYS_CD_SK")), lit(0)).otherwise(col("orig_clm_lkup.SRC_SYS_CD_SK")).alias("SRC_SYS_CD_SK"),
        when(isnull(col("orig_clm_lkup.ORIG_CLM_ID")), lit(" ")).otherwise(col("orig_clm_lkup.ORIG_CLM_ID")).alias("CLM_ID"),
        when(isnull(col("orig_clm_lkup.CRT_RUN_CYC_EXCTN_SK")), lit(0)).otherwise(col("orig_clm_lkup.CRT_RUN_CYC_EXCTN_SK")).alias("CRT_RUN_CYC_EXCTN_SK"),
        col("adj_clm.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        when(isnull(col("orig_clm_lkup.ADJ_FROM_CLM_SK")), lit(0)).otherwise(col("orig_clm_lkup.ADJ_FROM_CLM_SK")).alias("ADJ_FROM_CLM_SK"),
        col("adj_clm.CLM_SK").alias("ADJ_TO_CLM_SK"),
        when(isnull(col("orig_clm_lkup.ALPHA_PFX_SK")), lit(0)).otherwise(col("orig_clm_lkup.ALPHA_PFX_SK")).alias("ALPHA_PFX_SK"),
        when(isnull(col("orig_clm_lkup.CLM_EOB_EXCD_SK")), lit(0)).otherwise(col("orig_clm_lkup.CLM_EOB_EXCD_SK")).alias("CLM_EOB_EXCD_SK"),
        when(isnull(col("orig_clm_lkup.CLS_SK")), lit(0)).otherwise(col("orig_clm_lkup.CLS_SK")).alias("CLS_SK"),
        when(isnull(col("orig_clm_lkup.CLS_PLN_SK")), lit(0)).otherwise(col("orig_clm_lkup.CLS_PLN_SK")).alias("CLS_PLN_SK"),
        when(isnull(col("orig_clm_lkup.EXPRNC_CAT_SK")), lit(0)).otherwise(col("orig_clm_lkup.EXPRNC_CAT_SK")).alias("EXPRNC_CAT_SK"),
        when(isnull(col("orig_clm_lkup.FNCL_LOB_SK")), lit(0)).otherwise(col("orig_clm_lkup.FNCL_LOB_SK")).alias("FNCL_LOB_SK"),
        when(isnull(col("orig_clm_lkup.GRP_SK")), lit(0)).otherwise(col("orig_clm_lkup.GRP_SK")).alias("GRP_SK"),
        when(isnull(col("orig_clm_lkup.MBR_SK")), lit(0)).otherwise(col("orig_clm_lkup.MBR_SK")).alias("MBR_SK"),
        when(isnull(col("orig_clm_lkup.NTWK_SK")), lit(0)).otherwise(col("orig_clm_lkup.NTWK_SK")).alias("NTWK_SK"),
        when(isnull(col("orig_clm_lkup.PROD_SK")), lit(0)).otherwise(col("orig_clm_lkup.PROD_SK")).alias("PROD_SK"),
        when(isnull(col("orig_clm_lkup.SUBGRP_SK")), lit(0)).otherwise(col("orig_clm_lkup.SUBGRP_SK")).alias("SUBGRP_SK"),
        when(isnull(col("orig_clm_lkup.SUB_SK")), lit(0)).otherwise(col("orig_clm_lkup.SUB_SK")).alias("SUB_SK"),
        when(isnull(col("orig_clm_lkup.CLM_ACDNT_CD_SK")), lit(0)).otherwise(col("orig_clm_lkup.CLM_ACDNT_CD_SK")).alias("CLM_ACDNT_CD_SK"),
        when(isnull(col("orig_clm_lkup.CLM_ACDNT_ST_CD_SK")), lit(0)).otherwise(col("orig_clm_lkup.CLM_ACDNT_ST_CD_SK")).alias("CLM_ACDNT_ST_CD_SK"),
        when(isnull(col("orig_clm_lkup.CLM_ACTV_BCBS_PLN_CD_SK")), lit(0)).otherwise(col("orig_clm_lkup.CLM_ACTV_BCBS_PLN_CD_SK")).alias("CLM_ACTV_BCBS_PLN_CD_SK"),
        when(isnull(col("orig_clm_lkup.CLM_AGMNT_SRC_CD_SK")), lit(0)).otherwise(col("orig_clm_lkup.CLM_AGMNT_SRC_CD_SK")).alias("CLM_AGMNT_SRC_CD_SK"),
        when(isnull(col("orig_clm_lkup.CLM_BTCH_ACTN_CD_SK")), lit(0)).otherwise(col("orig_clm_lkup.CLM_BTCH_ACTN_CD_SK")).alias("CLM_BTCH_ACTN_CD_SK"),
        when(isnull(col("orig_clm_lkup.CLM_CAP_CD_SK")), lit(0)).otherwise(col("orig_clm_lkup.CLM_CAP_CD_SK")).alias("CLM_CAP_CD_SK"),
        when(isnull(col("orig_clm_lkup.CLM_CAT_CD_SK")), lit(0)).otherwise(col("orig_clm_lkup.CLM_CAT_CD_SK")).alias("CLM_CAT_CD_SK"),
        when(isnull(col("orig_clm_lkup.CLM_CHK_CYC_OVRD_CD_SK")), lit(0)).otherwise(col("orig_clm_lkup.CLM_CHK_CYC_OVRD_CD_SK")).alias("CLM_CHK_CYC_OVRD_CD_SK"),
        when(isnull(col("orig_clm_lkup.CLM_COB_CD_SK")), lit(0)).otherwise(col("orig_clm_lkup.CLM_COB_CD_SK")).alias("CLM_COB_CD_SK"),
        when(isnull(col("orig_clm_lkup.CLM_FINL_DISP_CD_SK")), lit(0)).otherwise(col("orig_clm_lkup.CLM_FINL_DISP_CD_SK")).alias("CLM_FINL_DISP_CD_SK"),
        when(isnull(col("orig_clm_lkup.CLM_INPT_METH_CD_SK")), lit(0)).otherwise(col("orig_clm_lkup.CLM_INPT_METH_CD_SK")).alias("CLM_INPT_METH_CD_SK"),
        when(isnull(col("orig_clm_lkup.CLM_INPT_SRC_CD_SK")), lit(0)).otherwise(col("orig_clm_lkup.CLM_INPT_SRC_CD_SK")).alias("CLM_INPT_SRC_CD_SK"),
        when(isnull(col("orig_clm_lkup.CLM_INTER_PLN_PGM_CD_SK")), lit(0)).otherwise(col("orig_clm_lkup.CLM_INTER_PLN_PGM_CD_SK")).alias("CLM_INTER_PLN_PGM_CD_SK"),
        when(isnull(col("orig_clm_lkup.CLM_NTWK_STTUS_CD_SK")), lit(0)).otherwise(col("orig_clm_lkup.CLM_NTWK_STTUS_CD_SK")).alias("CLM_NTWK_STTUS_CD_SK"),
        when(isnull(col("orig_clm_lkup.CLM_NONPAR_PROV_PFX_CD_SK")), lit(0)).otherwise(col("orig_clm_lkup.CLM_NONPAR_PROV_PFX_CD_SK")).alias("CLM_NONPAR_PROV_PFX_CD_SK"),
        when(isnull(col("orig_clm_lkup.CLM_OTHR_BNF_CD_SK")), lit(0)).otherwise(col("orig_clm_lkup.CLM_OTHR_BNF_CD_SK")).alias("CLM_OTHR_BNF_CD_SK"),
        when(isnull(col("orig_clm_lkup.CLM_PAYE_CD_SK")), lit(0)).otherwise(col("orig_clm_lkup.CLM_PAYE_CD_SK")).alias("CLM_PAYE_CD_SK"),
        when(isnull(col("orig_clm_lkup.CLM_PRCS_CTL_AGNT_PFX_CD_SK")), lit(0)).otherwise(col("orig_clm_lkup.CLM_PRCS_CTL_AGNT_PFX_CD_SK")).alias("CLM_PRCS_CTL_AGNT_PFX_CD_SK"),
        when(isnull(col("orig_clm_lkup.CLM_SVC_DEFN_PFX_CD_SK")), lit(0)).otherwise(col("orig_clm_lkup.CLM_SVC_DEFN_PFX_CD_SK")).alias("CLM_SVC_DEFN_PFX_CD_SK"),
        when(isnull(col("orig_clm_lkup.CLM_SVC_PROV_SPEC_CD_SK")), lit(0)).otherwise(col("orig_clm_lkup.CLM_SVC_PROV_SPEC_CD_SK")).alias("CLM_SVC_PROV_SPEC_CD_SK"),
        when(isnull(col("orig_clm_lkup.CLM_SVC_PROV_TYP_CD_SK")), lit(0)).otherwise(col("orig_clm_lkup.CLM_SVC_PROV_TYP_CD_SK")).alias("CLM_SVC_PROV_TYP_CD_SK"),
        col("svStatusCd").alias("CLM_STTUS_CD_SK"),
        when(isnull(col("orig_clm_lkup.CLM_SUBMTTING_BCBS_PLN_CD_SK")), lit(0)).otherwise(col("orig_clm_lkup.CLM_SUBMTTING_BCBS_PLN_CD_SK")).alias("CLM_SUBMTTING_BCBS_PLN_CD_SK"),
        when(isnull(col("orig_clm_lkup.CLM_SUB_BCBS_PLN_CD_SK")), lit(0)).otherwise(col("orig_clm_lkup.CLM_SUB_BCBS_PLN_CD_SK")).alias("CLM_SUB_BCBS_PLN_CD_SK"),
        when(isnull(col("orig_clm_lkup.CLM_SUBTYP_CD_SK")), lit(0)).otherwise(col("orig_clm_lkup.CLM_SUBTYP_CD_SK")).alias("CLM_SUBTYP_CD_SK"),
        when(isnull(col("orig_clm_lkup.CLM_TYP_CD_SK")), lit(0)).otherwise(col("orig_clm_lkup.CLM_TYP_CD_SK")).alias("CLM_TYP_CD_SK"),
        when(isnull(col("orig_clm_lkup.ATCHMT_IN")), lit(" ")).otherwise(col("orig_clm_lkup.ATCHMT_IN")).alias("ATCHMT_IN"),
        when(isnull(col("orig_clm_lkup.CLNCL_EDIT_IN")), lit(" ")).otherwise(col("orig_clm_lkup.CLNCL_EDIT_IN")).alias("CLNCL_EDIT_IN"),
        when(isnull(col("orig_clm_lkup.COBRA_CLM_IN")), lit(" ")).otherwise(col("orig_clm_lkup.COBRA_CLM_IN")).alias("COBRA_CLM_IN"),
        when(isnull(col("orig_clm_lkup.FIRST_PASS_IN")), lit(" ")).otherwise(col("orig_clm_lkup.FIRST_PASS_IN")).alias("FIRST_PASS_IN"),
        when(isnull(col("orig_clm_lkup.HOST_IN")), lit(" ")).otherwise(col("orig_clm_lkup.HOST_IN")).alias("HOST_IN"),
        when(isnull(col("orig_clm_lkup.LTR_IN")), lit(" ")).otherwise(col("orig_clm_lkup.LTR_IN")).alias("LTR_IN"),
        when(isnull(col("orig_clm_lkup.MCARE_ASG_IN")), lit(" ")).otherwise(col("orig_clm_lkup.MCARE_ASG_IN")).alias("MCARE_ASG_IN"),
        when(isnull(col("orig_clm_lkup.NOTE_IN")), lit(" ")).otherwise(col("orig_clm_lkup.NOTE_IN")).alias("NOTE_IN"),
        when(isnull(col("orig_clm_lkup.PCA_AUDIT_IN")), lit(" ")).otherwise(col("orig_clm_lkup.PCA_AUDIT_IN")).alias("PCA_AUDIT_IN"),
        when(isnull(col("orig_clm_lkup.PCP_SUBMT_IN")), lit(" ")).otherwise(col("orig_clm_lkup.PCP_SUBMT_IN")).alias("PCP_SUBMT_IN"),
        when(isnull(col("orig_clm_lkup.PROD_OOA_IN")), lit(" ")).otherwise(col("orig_clm_lkup.PROD_OOA_IN")).alias("PROD_OOA_IN"),
        when(isnull(col("orig_clm_lkup.ACDNT_DT_SK")), lit(0)).otherwise(col("orig_clm_lkup.ACDNT_DT_SK")).alias("ACDNT_DT_SK"),
        when(isnull(col("orig_clm_lkup.INPT_DT_SK")), lit(0)).otherwise(col("orig_clm_lkup.INPT_DT_SK")).alias("INPT_DT_SK"),
        when(isnull(col("orig_clm_lkup.MBR_PLN_ELIG_DT_SK")), lit(0)).otherwise(col("orig_clm_lkup.MBR_PLN_ELIG_DT_SK")).alias("MBR_PLN_ELIG_DT_SK"),
        when(isnull(col("orig_clm_lkup.NEXT_RVW_DT_SK")), lit(0)).otherwise(col("orig_clm_lkup.NEXT_RVW_DT_SK")).alias("NEXT_RVW_DT_SK"),
        when(isnull(col("orig_clm_lkup.PD_DT_SK")), lit(0)).otherwise(col("orig_clm_lkup.PD_DT_SK")).alias("PD_DT_SK"),
        when(isnull(col("orig_clm_lkup.PAYMT_DRAG_CYC_DT_SK")), lit(0)).otherwise(col("orig_clm_lkup.PAYMT_DRAG_CYC_DT_SK")).alias("PAYMT_DRAG_CYC_DT_SK"),
        when(isnull(col("orig_clm_lkup.PRCS_DT_SK")), lit(0)).otherwise(col("orig_clm_lkup.PRCS_DT_SK")).alias("PRCS_DT_SK"),
        when(isnull(col("orig_clm_lkup.RCVD_DT_SK")), lit(0)).otherwise(col("orig_clm_lkup.RCVD_DT_SK")).alias("RCVD_DT_SK"),
        when(isnull(col("orig_clm_lkup.SVC_STRT_DT_SK")), lit(0)).otherwise(col("orig_clm_lkup.SVC_STRT_DT_SK")).alias("SVC_STRT_DT_SK"),
        when(isnull(col("orig_clm_lkup.SVC_END_DT_SK")), lit(0)).otherwise(col("orig_clm_lkup.SVC_END_DT_SK")).alias("SVC_END_DT_SK"),
        when(isnull(col("orig_clm_lkup.SMLR_ILNS_DT_SK")), lit(0)).otherwise(col("orig_clm_lkup.SMLR_ILNS_DT_SK")).alias("SMLR_ILNS_DT_SK"),
        when(isnull(col("orig_clm_lkup.STTUS_DT_SK")), lit(0)).otherwise(col("orig_clm_lkup.STTUS_DT_SK")).alias("STTUS_DT_SK"),
        when(isnull(col("orig_clm_lkup.WORK_UNABLE_BEG_DT_SK")), lit(0)).otherwise(col("orig_clm_lkup.WORK_UNABLE_BEG_DT_SK")).alias("WORK_UNABLE_BEG_DT_SK"),
        when(isnull(col("orig_clm_lkup.WORK_UNABLE_END_DT_SK")), lit(0)).otherwise(col("orig_clm_lkup.WORK_UNABLE_END_DT_SK")).alias("WORK_UNABLE_END_DT_SK"),
        when(isnull(col("orig_clm_lkup.ACDNT_AMT")), lit(0)).otherwise(col("orig_clm_lkup.ACDNT_AMT")).alias("ACDNT_AMT"),
        when(isnull(col("orig_clm_lkup.ACTL_PD_AMT")), lit(0)).otherwise(col("orig_clm_lkup.ACTL_PD_AMT")).alias("ACTL_PD_AMT"),
        when(isnull(col("orig_clm_lkup.ALW_AMT")), lit(0)).otherwise(col("orig_clm_lkup.ALW_AMT")).alias("ALW_AMT"),
        when(isnull(col("orig_clm_lkup.DSALW_AMT")), lit(0)).otherwise(col("orig_clm_lkup.DSALW_AMT")).alias("DSALW_AMT"),
        when(isnull(col("orig_clm_lkup.COINS_AMT")), lit(0)).otherwise(col("orig_clm_lkup.COINS_AMT")).alias("COINS_AMT"),
        when(isnull(col("orig_clm_lkup.CNSD_CHRG_AMT")), lit(0)).otherwise(col("orig_clm_lkup.CNSD_CHRG_AMT")).alias("CNSD_CHRG_AMT"),
        when(isnull(col("orig_clm_lkup.COPAY_AMT")), lit(0)).otherwise(col("orig_clm_lkup.COPAY_AMT")).alias("COPAY_AMT"),
        when(isnull(col("orig_clm_lkup.CHRG_AMT")), lit(0)).otherwise(col("orig_clm_lkup.CHRG_AMT")).alias("CHRG_AMT"),
        when(isnull(col("orig_clm_lkup.DEDCT_AMT")), lit(0)).otherwise(col("orig_clm_lkup.DEDCT_AMT")).alias("DEDCT_AMT"),
        when(isnull(col("orig_clm_lkup.PAYBL_AMT")), lit(0)).otherwise(col("orig_clm_lkup.PAYBL_AMT")).alias("PAYBL_AMT"),
        when(isnull(col("orig_clm_lkup.CLM_CT")), lit(0)).otherwise(col("orig_clm_lkup.CLM_CT")).alias("CLM_CT"),
        when(isnull(col("orig_clm_lkup.MBR_AGE")), lit(0)).otherwise(col("orig_clm_lkup.MBR_AGE")).alias("MBR_AGE"),
        when(isnull(col("orig_clm_lkup.ADJ_FROM_CLM_ID")), lit(" ")).otherwise(col("orig_clm_lkup.ADJ_FROM_CLM_ID")).alias("ADJ_FROM_CLM_ID"),
        when(isnull(col("adj_clm.CLM_ID")), lit(" ")).otherwise(col("adj_clm.CLM_ID")).alias("ADJ_TO_CLM_ID"),
        when(isnull(col("orig_clm_lkup.DOC_TX_ID")), lit(" ")).otherwise(col("orig_clm_lkup.DOC_TX_ID")).alias("DOC_TX_ID"),
        when(isnull(col("orig_clm_lkup.MCAID_RESUB_NO")), None).otherwise(col("orig_clm_lkup.MCAID_RESUB_NO")).alias("MCAID_RESUB_NO"),
        when(isnull(col("orig_clm_lkup.MCARE_ID")), lit(" ")).otherwise(col("orig_clm_lkup.MCARE_ID")).alias("MCARE_ID"),
        when(isnull(col("orig_clm_lkup.MBR_SFX_NO")), lit(" ")).otherwise(col("orig_clm_lkup.MBR_SFX_NO")).alias("MBR_SFX_NO"),
        when(isnull(col("orig_clm_lkup.PATN_ACCT_NO")), None).otherwise(col("orig_clm_lkup.PATN_ACCT_NO")).alias("PATN_ACCT_NO"),
        when(isnull(col("orig_clm_lkup.PAYMT_REF_ID")), lit(" ")).otherwise(col("orig_clm_lkup.PAYMT_REF_ID")).alias("PAYMT_REF_ID"),
        when(isnull(col("orig_clm_lkup.PROV_AGMNT_ID")), lit(" ")).otherwise(col("orig_clm_lkup.PROV_AGMNT_ID")).alias("PROV_AGMNT_ID"),
        when(isnull(col("orig_clm_lkup.RFRNG_PROV_TX")), None).otherwise(col("orig_clm_lkup.RFRNG_PROV_TX")).alias("RFRNG_PROV_TX"),
        when(isnull(col("orig_clm_lkup.SUB_ID")), lit(" ")).otherwise(col("orig_clm_lkup.SUB_ID")).alias("SUB_ID"),
        lit("1").alias("PCA_TYP_CD_SK"),
        lit("1").alias("REL_PCA_CLM_SK"),
        lit("1").alias("REL_BASE_CLM_SK"),
        lit("0").alias("REMIT_SUPRSION_AMT"),
        when(isnull(col("orig_clm_lkup.SUB_ID")), lit("  "))
            .otherwise(when(trim(length(col("orig_clm_lkup.MCAID_STTUS_ID"))) == lit(0), lit("NA"))
                       .otherwise(col("orig_clm_lkup.MCAID_STTUS_ID"))
                      ).alias("MCAID_STTUS_ID"),
        when(isnull(col("orig_clm_lkup.PATN_PD_AMT")), lit(0.00)).otherwise(col("orig_clm_lkup.PATN_PD_AMT")).alias("PATN_PD_AMT"),
        col("adj_clm.CLM_SUBMT_ICD_VRSN_CD_SK").alias("CLM_SUBMT_ICD_VRSN_CD_SK"),
        when(isnull(col("orig_clm_lkup.CLM_TXNMY_CD")), None).otherwise(col("orig_clm_lkup.CLM_TXNMY_CD")).alias("CLM_TXNMY_CD"),
        col("adj_clm.BILL_PAYMT_EXCL_IN").alias("BILL_PAYMT_EXCL_IN")
    )
)

df_orig_clm_out = (
    df_orig_clm_out
    .withColumn("ATCHMT_IN", rpad(col("ATCHMT_IN"), 1, " "))
    .withColumn("CLNCL_EDIT_IN", rpad(col("CLNCL_EDIT_IN"), 1, " "))
    .withColumn("COBRA_CLM_IN", rpad(col("COBRA_CLM_IN"), 1, " "))
    .withColumn("FIRST_PASS_IN", rpad(col("FIRST_PASS_IN"), 1, " "))
    .withColumn("HOST_IN", rpad(col("HOST_IN"), 1, " "))
    .withColumn("LTR_IN", rpad(col("LTR_IN"), 1, " "))
    .withColumn("MCARE_ASG_IN", rpad(col("MCARE_ASG_IN"), 1, " "))
    .withColumn("NOTE_IN", rpad(col("NOTE_IN"), 1, " "))
    .withColumn("PCA_AUDIT_IN", rpad(col("PCA_AUDIT_IN"), 1, " "))
    .withColumn("PCP_SUBMT_IN", rpad(col("PCP_SUBMT_IN"), 1, " "))
    .withColumn("PROD_OOA_IN", rpad(col("PROD_OOA_IN"), 1, " "))
    .withColumn("DOC_TX_ID", rpad(col("DOC_TX_ID"), 18, " "))
    .withColumn("MCAID_RESUB_NO", col("MCAID_RESUB_NO"))
    .withColumn("MCARE_ID", rpad(col("MCARE_ID"), 12, " "))
    .withColumn("MBR_SFX_NO", rpad(col("MBR_SFX_NO"), 2, " "))
    .withColumn("PAYMT_REF_ID", rpad(col("PAYMT_REF_ID"), 16, " "))
    .withColumn("PROV_AGMNT_ID", rpad(col("PROV_AGMNT_ID"), 12, " "))
    .withColumn("SUB_ID", rpad(col("SUB_ID"), 14, " "))
    .withColumn("BILL_PAYMT_EXCL_IN", rpad(col("BILL_PAYMT_EXCL_IN"), 1, " "))
)

df_ClmMartAdj = (
    df_clm
    .filter(
        (col("adj_clm.SVC_STRT_DT_SK") > lit("#FillDtMin#")) &
        (~isnull(col("orig_clm_lkup.CLM_SK"))) &
        ((col("adj_clm.SRC_SYS_CD") == lit("ARGUS")) | (col("adj_clm.SRC_SYS_CD") == lit("ESI")) | (col("adj_clm.SRC_SYS_CD") == lit("OPTUMRX")))
    )
    .select(
        col("adj_clm.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        col("adj_clm.CLM_ID").alias("CLM_ID"),
        col("adj_clm.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("adj_clm.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
    )
)

df_ClmMartOrig = (
    df_clm
    .filter(
        (col("orig_clm_lkup.SVC_STRT_DT_SK") > lit("#FillDtMin#")) &
        (~isnull(col("orig_clm_lkup.CLM_SK"))) &
        ((col("adj_clm.SRC_SYS_CD") == lit("ARGUS")) | (col("adj_clm.SRC_SYS_CD") == lit("ESI")) | (col("adj_clm.SRC_SYS_CD") == lit("OPTUMRX")))
    )
    .select(
        when(isnull(col("orig_clm_lkup.SRC_SYS_CD_SK")), lit(0)).otherwise(col("orig_clm_lkup.SRC_SYS_CD_SK")).alias("SRC_SYS_CD_SK"),
        when(isnull(col("orig_clm_lkup.ORIG_CLM_ID")), lit(" ")).otherwise(col("orig_clm_lkup.ORIG_CLM_ID")).alias("CLM_ID"),
        col("adj_clm.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("adj_clm.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
    )
)

df_RowCount = (
    df_clm
    .filter(~isnull(col("orig_clm_lkup.CLM_SK")))
    .select(
        when(isnull(col("orig_clm_lkup.SRC_SYS_CD_SK")), lit(0)).otherwise(col("orig_clm_lkup.SRC_SYS_CD_SK")).alias("SRC_SYS_CD_SK"),
        when(isnull(col("orig_clm_lkup.ORIG_CLM_ID")), lit(" ")).otherwise(col("orig_clm_lkup.ORIG_CLM_ID")).alias("CLM_ID"),
        col("svStatusCd").alias("CLM_STTUS_CD_SK"),
        when(isnull(col("orig_clm_lkup.CLM_CAT_CD_SK")), lit(0)).otherwise(col("orig_clm_lkup.CLM_CAT_CD_SK")).alias("CLM_CAT_CD_SK"),
        when(isnull(col("orig_clm_lkup.EXPRNC_CAT_SK")), lit(0)).otherwise(col("orig_clm_lkup.EXPRNC_CAT_SK")).alias("EXPRNC_CAT_SK"),
        when(isnull(col("orig_clm_lkup.FNCL_LOB_SK")), lit(0)).otherwise(col("orig_clm_lkup.FNCL_LOB_SK")).alias("FNCL_LOB_SK"),
        when(isnull(col("orig_clm_lkup.GRP_SK")), lit(0)).otherwise(col("orig_clm_lkup.GRP_SK")).alias("GRP_SK"),
        when(isnull(col("orig_clm_lkup.MBR_SK")), lit(0)).otherwise(col("orig_clm_lkup.MBR_SK")).alias("MBR_SK"),
        when(isnull(col("orig_clm_lkup.SVC_STRT_DT_SK")), lit(0)).otherwise(col("orig_clm_lkup.SVC_STRT_DT_SK")).alias("SVC_STRT_DT_SK"),
        when(isnull(col("orig_clm_lkup.CHRG_AMT")), lit(0)).otherwise(col("orig_clm_lkup.CHRG_AMT")).alias("CHRG_AMT"),
        when(isnull(col("orig_clm_lkup.PAYBL_AMT")), lit(0)).otherwise(col("orig_clm_lkup.PAYBL_AMT")).alias("PAYBL_AMT"),
        when(isnull(col("orig_clm_lkup.CLM_CT")), lit(0)).otherwise(col("orig_clm_lkup.CLM_CT")).alias("CLM_CT"),
        lit("1").alias("PCA_TYP_CD_SK")
    )
)

write_files(
    df_RowCount,
    f"{adls_path}/load/B_CLM.{Source}.dat.{RunID}",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

write_files(
    df_ClmMartOrig,
    f"{adls_path}/load/W_WEBDM_ETL_DRVR.dat.DrugReversalOrig.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

write_files(
    df_ClmMartAdj,
    f"{adls_path}/load/W_WEBDM_ETL_DRVR.dat.DrugReversalAdj.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

write_files(
    df_orig_clm_out,
    f"{adls_path}/load/CLM_REVERSAL_DRUG_ORIG_CLM.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

write_files(
    df_adj_claims_out,
    f"{adls_path}/load/CLM_REVERSAL_DRUG_ADJ_CLM.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)


# COMMAND ----------
from pyspark.sql.functions import col, lit, rpad

# Parameter handling
RunID = get_widget_value('RunID','')
Source = get_widget_value('Source','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

# Get DB config for IDS
jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# Read from IDS: adj_clm_ln
extract_query1 = f"""
SELECT CLM_LN.CLM_LN_SK as CLM_LN_SK,
       CLM_LN.SRC_SYS_CD_SK as SRC_SYS_CD_SK,
       CLM_LN.CLM_ID as CLM_ID,
       CLM_LN.CLM_LN_SEQ_NO as CLM_LN_SEQ_NO,
       CLM_LN.CRT_RUN_CYC_EXCTN_SK as CRT_RUN_CYC_EXCTN_SK,
       CLM_LN.LAST_UPDT_RUN_CYC_EXCTN_SK as LAST_UPDT_RUN_CYC_EXCTN_SK,
       CLM_LN.CLM_SK as CLM_SK,
       CLM_LN.PROC_CD_SK as PROC_CD_SK,
       CLM_LN.SVC_PROV_SK as SVC_PROV_SK,
       CLM_LN.CLM_LN_DSALW_EXCD_SK as CLM_LN_DSALW_EXCD_SK,
       CLM_LN.CLM_LN_EOB_EXCD_SK as CLM_LN_EOB_EXCD_SK,
       CLM_LN.CLM_LN_FINL_DISP_CD_SK as CLM_LN_FINL_DISP_CD_SK,
       CLM_LN.CLM_LN_LOB_CD_SK as CLM_LN_LOB_CD_SK,
       CLM_LN.CLM_LN_POS_CD_SK as CLM_LN_POS_CD_SK,
       CLM_LN.CLM_LN_PREAUTH_CD_SK as CLM_LN_PREAUTH_CD_SK,
       CLM_LN.CLM_LN_PREAUTH_SRC_CD_SK as CLM_LN_PREAUTH_SRC_CD_SK,
       CLM_LN.CLM_LN_PRICE_SRC_CD_SK as CLM_LN_PRICE_SRC_CD_SK,
       CLM_LN.CLM_LN_RFRL_CD_SK as CLM_LN_RFRL_CD_SK,
       CLM_LN.CLM_LN_RVNU_CD_SK as CLM_LN_RVNU_CD_SK,
       CLM_LN.CLM_LN_ROOM_PRICE_METH_CD_SK as CLM_LN_ROOM_PRICE_METH_CD_SK,
       CLM_LN.CLM_LN_ROOM_TYP_CD_SK as CLM_LN_ROOM_TYP_CD_SK,
       CLM_LN.CLM_LN_TOS_CD_SK as CLM_LN_TOS_CD_SK,
       CLM_LN.CLM_LN_UNIT_TYP_CD_SK as CLM_LN_UNIT_TYP_CD_SK,
       CLM_LN.CAP_LN_IN as CAP_LN_IN,
       CLM_LN.PRI_LOB_IN as PRI_LOB_IN,
       CLM_LN.SVC_END_DT_SK as SVC_END_DT_SK,
       CLM_LN.SVC_STRT_DT_SK as SVC_STRT_DT_SK,
       CLM_LN.AGMNT_PRICE_AMT as AGMNT_PRICE_AMT,
       CLM_LN.ALW_AMT as ALW_AMT,
       CLM_LN.CHRG_AMT as CHRG_AMT,
       CLM_LN.COINS_AMT as COINS_AMT,
       CLM_LN.CNSD_CHRG_AMT as CNSD_CHRG_AMT,
       CLM_LN.COPAY_AMT as COPAY_AMT,
       CLM_LN.DEDCT_AMT as DEDCT_AMT,
       CLM_LN.DSALW_AMT as DSALW_AMT,
       CLM_LN.ITS_HOME_DSCNT_AMT as ITS_HOME_DSCNT_AMT,
       CLM_LN.NO_RESP_AMT as NO_RESP_AMT,
       CLM_LN.MBR_LIAB_BSS_AMT as MBR_LIAB_BSS_AMT,
       CLM_LN.PATN_RESP_AMT as PATN_RESP_AMT,
       CLM_LN.PAYBL_AMT as PAYBL_AMT,
       CLM_LN.PAYBL_TO_PROV_AMT as PAYBL_TO_PROV_AMT,
       CLM_LN.PAYBL_TO_SUB_AMT as PAYBL_TO_SUB_AMT,
       CLM_LN.PROC_TBL_PRICE_AMT as PROC_TBL_PRICE_AMT,
       CLM_LN.PROFL_PRICE_AMT as PROFL_PRICE_AMT,
       CLM_LN.PROV_WRT_OFF_AMT as PROV_WRT_OFF_AMT,
       CLM_LN.RISK_WTHLD_AMT as RISK_WTHLD_AMT,
       CLM_LN.SVC_PRICE_AMT as SVC_PRICE_AMT,
       CLM_LN.SUPLMT_DSCNT_AMT as SUPLMT_DSCNT_AMT,
       CLM_LN.ALW_PRICE_UNIT_CT as ALW_PRICE_UNIT_CT,
       CLM_LN.UNIT_CT as UNIT_CT,
       CLM_LN.DEDCT_AMT_ACCUM_ID as DEDCT_AMT_ACCUM_ID,
       CLM_LN.PREAUTH_SVC_SEQ_NO as PREAUTH_SVC_SEQ_NO,
       CLM_LN.RFRL_SVC_SEQ_NO as RFRL_SVC_SEQ_NO,
       CLM_LN.LMT_PFX_ID as LMT_PFX_ID,
       CLM_LN.PREAUTH_ID as PREAUTH_ID,
       CLM_LN.PROD_CMPNT_DEDCT_PFX_ID as PROD_CMPNT_DEDCT_PFX_ID,
       CLM_LN.PROD_CMPNT_SVC_PAYMT_ID as PROD_CMPNT_SVC_PAYMT_ID,
       CLM_LN.RFRL_ID_TX as RFRL_ID_TX,
       CLM_LN.SVC_ID as SVC_ID,
       CLM_LN.SVC_PRICE_RULE_ID as SVC_PRICE_RULE_ID,
       CLM_LN.SVC_RULE_TYP_TX as SVC_RULE_TYP_TX,
       CLM_LN.CLM_LN_SVC_LOC_TYP_CD_SK as CLM_LN_SVC_LOC_TYP_CD_SK,
       CLM_LN.CLM_LN_SVC_PRICE_RULE_CD_SK as CLM_LN_SVC_PRICE_RULE_CD_SK,
       CLM_LN.NON_PAR_SAV_AMT as NON_PAR_SAV_AMT,
       CLM_LN.VBB_RULE_SK as VBB_RULE_SK,
       CLM_LN.VBB_EXCD_SK as VBB_EXCD_SK,
       CLM_LN.CLM_LN_VBB_IN as CLM_LN_VBB_IN,
       ITS_SUPLMT_DSCNT_AMT as ITS_SUPLMT_DSCNT_AMT,
       ITS_SRCHRG_AMT as ITS_SRCHRG_AMT,
       CLM_LN.NDC_SK,
       CLM_LN.NDC_DRUG_FORM_CD_SK,
       CLM_LN.NDC_UNIT_CT
  FROM
    {IDSOwner}.CLM_LN CLM_LN,
    {IDSOwner}.W_DRUG_CLM ADJ
 WHERE
    ADJ.CLM_SK = CLM_LN.CLM_SK
"""

df_adj_clm_ln = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query1)
    .load()
)

# Read from IDS: orig_clm_ln_hash
extract_query2 = f"""
SELECT R.RVRSL_CLM_SK as CLM_SK,
       CLM_LN.CLM_LN_SEQ_NO as CLM_LN_SEQ_NO,
       CLM_LN.CLM_LN_SK as CLM_LN_SK,
       CLM_LN.SRC_SYS_CD_SK as SRC_SYS_CD_SK,
       R.RVRSL_CLM_ID as CLM_ID,
       CLM_LN.CRT_RUN_CYC_EXCTN_SK as CRT_RUN_CYC_EXCTN_SK,
       CLM_LN.LAST_UPDT_RUN_CYC_EXCTN_SK as LAST_UPDT_RUN_CYC_EXCTN_SK,
       CLM_LN.PROC_CD_SK as PROC_CD_SK,
       CLM_LN.SVC_PROV_SK as SVC_PROV_SK,
       CLM_LN.CLM_LN_DSALW_EXCD_SK as CLM_LN_DSALW_EXCD_SK,
       CLM_LN.CLM_LN_EOB_EXCD_SK as CLM_LN_EOB_EXCD_SK,
       CLM_LN.CLM_LN_FINL_DISP_CD_SK as CLM_LN_FINL_DISP_CD_SK,
       CLM_LN.CLM_LN_LOB_CD_SK as CLM_LN_LOB_CD_SK,
       CLM_LN.CLM_LN_POS_CD_SK as CLM_LN_POS_CD_SK,
       CLM_LN.CLM_LN_PREAUTH_CD_SK as CLM_LN_PREAUTH_CD_SK,
       CLM_LN.CLM_LN_PREAUTH_SRC_CD_SK as CLM_LN_PREAUTH_SRC_CD_SK,
       CLM_LN.CLM_LN_PRICE_SRC_CD_SK as CLM_LN_PRICE_SRC_CD_SK,
       CLM_LN.CLM_LN_RFRL_CD_SK as CLM_LN_RFRL_CD_SK,
       CLM_LN.CLM_LN_RVNU_CD_SK as CLM_LN_RVNU_CD_SK,
       CLM_LN.CLM_LN_ROOM_PRICE_METH_CD_SK as CLM_LN_ROOM_PRICE_METH_CD_SK,
       CLM_LN.CLM_LN_ROOM_TYP_CD_SK as CLM_LN_ROOM_TYP_CD_SK,
       CLM_LN.CLM_LN_TOS_CD_SK as CLM_LN_TOS_CD_SK,
       CLM_LN.CLM_LN_UNIT_TYP_CD_SK as CLM_LN_UNIT_TYP_CD_SK,
       CLM_LN.CAP_LN_IN as CAP_LN_IN,
       CLM_LN.PRI_LOB_IN as PRI_LOB_IN,
       CLM_LN.SVC_END_DT_SK as SVC_END_DT_SK,
       CLM_LN.SVC_STRT_DT_SK as SVC_STRT_DT_SK,
       CLM_LN.AGMNT_PRICE_AMT as AGMNT_PRICE_AMT,
       CLM_LN.ALW_AMT as ALW_AMT,
       CLM_LN.CHRG_AMT as CHRG_AMT,
       CLM_LN.COINS_AMT as COINS_AMT,
       CLM_LN.CNSD_CHRG_AMT as CNSD_CHRG_AMT,
       CLM_LN.COPAY_AMT as COPAY_AMT,
       CLM_LN.DEDCT_AMT as DEDCT_AMT,
       CLM_LN.DSALW_AMT as DSALW_AMT,
       CLM_LN.ITS_HOME_DSCNT_AMT as ITS_HOME_DSCNT_AMT,
       CLM_LN.NO_RESP_AMT as NO_RESP_AMT,
       CLM_LN.MBR_LIAB_BSS_AMT as MBR_LIAB_BSS_AMT,
       CLM_LN.PATN_RESP_AMT as PATN_RESP_AMT,
       CLM_LN.PAYBL_AMT as PAYBL_AMT,
       CLM_LN.PAYBL_TO_PROV_AMT as PAYBL_TO_PROV_AMT,
       CLM_LN.PAYBL_TO_SUB_AMT as PAYBL_TO_SUB_AMT,
       CLM_LN.PROC_TBL_PRICE_AMT as PROC_TBL_PRICE_AMT,
       CLM_LN.PROFL_PRICE_AMT as PROFL_PRICE_AMT,
       CLM_LN.PROV_WRT_OFF_AMT as PROV_WRT_OFF_AMT,
       CLM_LN.RISK_WTHLD_AMT as RISK_WTHLD_AMT,
       CLM_LN.SVC_PRICE_AMT as SVC_PRICE_AMT,
       CLM_LN.SUPLMT_DSCNT_AMT as SUPLMT_DSCNT_AMT,
       CLM_LN.ALW_PRICE_UNIT_CT as ALW_PRICE_UNIT_CT,
       CLM_LN.UNIT_CT as UNIT_CT,
       CLM_LN.DEDCT_AMT_ACCUM_ID as DEDCT_AMT_ACCUM_ID,
       CLM_LN.PREAUTH_SVC_SEQ_NO as PREAUTH_SVC_SEQ_NO,
       CLM_LN.RFRL_SVC_SEQ_NO as RFRL_SVC_SEQ_NO,
       CLM_LN.LMT_PFX_ID as LMT_PFX_ID,
       CLM_LN.PREAUTH_ID as PREAUTH_ID,
       CLM_LN.PROD_CMPNT_DEDCT_PFX_ID as PROD_CMPNT_DEDCT_PFX_ID,
       CLM_LN.PROD_CMPNT_SVC_PAYMT_ID as PROD_CMPNT_SVC_PAYMT_ID,
       CLM_LN.RFRL_ID_TX as RFRL_ID_TX,
       CLM_LN.SVC_ID as SVC_ID,
       CLM_LN.SVC_PRICE_RULE_ID as SVC_PRICE_RULE_ID,
       CLM_LN.SVC_RULE_TYP_TX as SVC_RULE_TYP_TX,
       CLM_LN.CLM_LN_SVC_LOC_TYP_CD_SK as CLM_LN_SVC_LOC_TYP_CD_SK,
       CLM_LN.CLM_LN_SVC_PRICE_RULE_CD_SK as CLM_LN_SVC_PRICE_RULE_CD_SK,
       CLM_LN.NON_PAR_SAV_AMT as NON_PAR_SAV_AMT,
       CLM_LN.VBB_RULE_SK as VBB_RULE_SK,
       CLM_LN.VBB_EXCD_SK as VBB_EXCD_SK,
       CLM_LN.CLM_LN_VBB_IN as CLM_LN_VBB_IN,
       CLM_LN.ITS_SUPLMT_DSCNT_AMT as ITS_SUPLMT_DSCNT_AMT,
       CLM_LN.ITS_SRCHRG_AMT as ITS_SRCHRG_AMT,
       CLM_LN.NDC_SK as NDC_SK,
       CLM_LN.NDC_DRUG_FORM_CD_SK as NDC_DRUG_FORM_CD_SK,
       CLM_LN.NDC_UNIT_CT as NDC_UNIT_CT
  FROM
    {IDSOwner}.CLM_LN CLM_LN,
    {IDSOwner}.W_CLM_RVRSL R
 WHERE
    R.ORIG_CLM_SK = CLM_LN.CLM_SK
"""

df_orig_clm_ln_hash = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query2)
    .load()
)

# Deduplicate df_orig_clm_ln_hash across the hashed-file key columns
df_orig_clm_ln_lkup = dedup_sort(
    df_orig_clm_ln_hash,
    partition_cols=["CLM_SK","CLM_LN_SEQ_NO"],
    sort_cols=[("CLM_SK","A"),("CLM_LN_SEQ_NO","A")]
)

# Join in Transformer "clm_ln" with constraint IsNull(orig_clm_ln_lkup.CLM_ID) = @FALSE => inner join
df_join = df_adj_clm_ln.alias("adj_clm_ln").join(
    df_orig_clm_ln_lkup.alias("orig_clm_ln_lkup"),
    [
        col("adj_clm_ln.CLM_SK") == col("orig_clm_ln_lkup.CLM_SK"),
        col("adj_clm_ln.CLM_LN_SEQ_NO") == col("orig_clm_ln_lkup.CLM_LN_SEQ_NO")
    ],
    "inner"
)

# Select and transform columns
df_adj_clm_ln_out = df_join.select(
    col("adj_clm_ln.CLM_LN_SK").alias("CLM_LN_SK"),
    col("adj_clm_ln.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("adj_clm_ln.CLM_ID").alias("CLM_ID"),
    col("adj_clm_ln.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    col("adj_clm_ln.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("adj_clm_ln.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("adj_clm_ln.CLM_SK").alias("CLM_SK"),
    col("orig_clm_ln_lkup.PROC_CD_SK").alias("PROC_CD_SK"),
    col("orig_clm_ln_lkup.SVC_PROV_SK").alias("SVC_PROV_SK"),
    col("orig_clm_ln_lkup.CLM_LN_DSALW_EXCD_SK").alias("CLM_LN_DSALW_EXCD_SK"),
    col("orig_clm_ln_lkup.CLM_LN_EOB_EXCD_SK").alias("CLM_LN_EOB_EXCD_SK"),
    col("orig_clm_ln_lkup.CLM_LN_FINL_DISP_CD_SK").alias("CLM_LN_FINL_DISP_CD_SK"),
    col("orig_clm_ln_lkup.CLM_LN_LOB_CD_SK").alias("CLM_LN_LOB_CD_SK"),
    col("orig_clm_ln_lkup.CLM_LN_POS_CD_SK").alias("CLM_LN_POS_CD_SK"),
    col("orig_clm_ln_lkup.CLM_LN_PREAUTH_CD_SK").alias("CLM_LN_PREAUTH_CD_SK"),
    col("orig_clm_ln_lkup.CLM_LN_PREAUTH_SRC_CD_SK").alias("CLM_LN_PREAUTH_SRC_CD_SK"),
    col("orig_clm_ln_lkup.CLM_LN_PRICE_SRC_CD_SK").alias("CLM_LN_PRICE_SRC_CD_SK"),
    col("orig_clm_ln_lkup.CLM_LN_RFRL_CD_SK").alias("CLM_LN_RFRL_CD_SK"),
    col("orig_clm_ln_lkup.CLM_LN_RVNU_CD_SK").alias("CLM_LN_RVNU_CD_SK"),
    col("orig_clm_ln_lkup.CLM_LN_ROOM_PRICE_METH_CD_SK").alias("CLM_LN_ROOM_PRICE_METH_CD_SK"),
    col("orig_clm_ln_lkup.CLM_LN_ROOM_TYP_CD_SK").alias("CLM_LN_ROOM_TYP_CD_SK"),
    col("orig_clm_ln_lkup.CLM_LN_TOS_CD_SK").alias("CLM_LN_TOS_CD_SK"),
    col("orig_clm_ln_lkup.CLM_LN_UNIT_TYP_CD_SK").alias("CLM_LN_UNIT_TYP_CD_SK"),
    rpad(col("orig_clm_ln_lkup.CAP_LN_IN"), 1, " ").alias("CAP_LN_IN"),
    rpad(col("orig_clm_ln_lkup.PRI_LOB_IN"), 1, " ").alias("PRI_LOB_IN"),
    rpad(col("orig_clm_ln_lkup.SVC_END_DT_SK"), 10, " ").alias("SVC_END_DT_SK"),
    rpad(col("orig_clm_ln_lkup.SVC_STRT_DT_SK"), 10, " ").alias("SVC_STRT_DT_SK"),
    (col("orig_clm_ln_lkup.AGMNT_PRICE_AMT") * lit(-1)).alias("AGMNT_PRICE_AMT"),
    (col("orig_clm_ln_lkup.ALW_AMT") * lit(-1)).alias("ALW_AMT"),
    (col("orig_clm_ln_lkup.CHRG_AMT") * lit(-1)).alias("CHRG_AMT"),
    (col("orig_clm_ln_lkup.COINS_AMT") * lit(-1)).alias("COINS_AMT"),
    (col("orig_clm_ln_lkup.CNSD_CHRG_AMT") * lit(-1)).alias("CNSD_CHRG_AMT"),
    (col("orig_clm_ln_lkup.COPAY_AMT") * lit(-1)).alias("COPAY_AMT"),
    (col("orig_clm_ln_lkup.DEDCT_AMT") * lit(-1)).alias("DEDCT_AMT"),
    (col("orig_clm_ln_lkup.DSALW_AMT") * lit(-1)).alias("DSALW_AMT"),
    (col("orig_clm_ln_lkup.ITS_HOME_DSCNT_AMT") * lit(-1)).alias("ITS_HOME_DSCNT_AMT"),
    (col("orig_clm_ln_lkup.NO_RESP_AMT") * lit(-1)).alias("NO_RESP_AMT"),
    (col("orig_clm_ln_lkup.MBR_LIAB_BSS_AMT") * lit(-1)).alias("MBR_LIAB_BSS_AMT"),
    (col("orig_clm_ln_lkup.PATN_RESP_AMT") * lit(-1)).alias("PATN_RESP_AMT"),
    (col("orig_clm_ln_lkup.PAYBL_AMT") * lit(-1)).alias("PAYBL_AMT"),
    (col("orig_clm_ln_lkup.PAYBL_TO_PROV_AMT") * lit(-1)).alias("PAYBL_TO_PROV_AMT"),
    (col("orig_clm_ln_lkup.PAYBL_TO_SUB_AMT") * lit(-1)).alias("PAYBL_TO_SUB_AMT"),
    (col("orig_clm_ln_lkup.PROC_TBL_PRICE_AMT") * lit(-1)).alias("PROC_TBL_PRICE_AMT"),
    (col("orig_clm_ln_lkup.PROFL_PRICE_AMT") * lit(-1)).alias("PROFL_PRICE_AMT"),
    (col("orig_clm_ln_lkup.PROV_WRT_OFF_AMT") * lit(-1)).alias("PROV_WRT_OFF_AMT"),
    (col("orig_clm_ln_lkup.RISK_WTHLD_AMT") * lit(-1)).alias("RISK_WTHLD_AMT"),
    (col("orig_clm_ln_lkup.SVC_PRICE_AMT") * lit(-1)).alias("SVC_PRICE_AMT"),
    (col("orig_clm_ln_lkup.SUPLMT_DSCNT_AMT") * lit(-1)).alias("SUPLMT_DSCNT_AMT"),
    (col("orig_clm_ln_lkup.ALW_PRICE_UNIT_CT") * lit(-1)).alias("ALW_PRICE_UNIT_CT"),
    (col("orig_clm_ln_lkup.UNIT_CT") * lit(-1)).alias("UNIT_CT"),
    rpad(col("orig_clm_ln_lkup.DEDCT_AMT_ACCUM_ID"), 4, " ").alias("DEDCT_AMT_ACCUM_ID"),
    rpad(col("orig_clm_ln_lkup.PREAUTH_SVC_SEQ_NO"), 4, " ").alias("PREAUTH_SVC_SEQ_NO"),
    rpad(col("orig_clm_ln_lkup.RFRL_SVC_SEQ_NO"), 4, " ").alias("RFRL_SVC_SEQ_NO"),
    rpad(col("orig_clm_ln_lkup.LMT_PFX_ID"), 4, " ").alias("LMT_PFX_ID"),
    col("orig_clm_ln_lkup.PREAUTH_ID").alias("PREAUTH_ID"),
    rpad(col("orig_clm_ln_lkup.PROD_CMPNT_DEDCT_PFX_ID"), 4, " ").alias("PROD_CMPNT_DEDCT_PFX_ID"),
    rpad(col("orig_clm_ln_lkup.PROD_CMPNT_SVC_PAYMT_ID"), 4, " ").alias("PROD_CMPNT_SVC_PAYMT_ID"),
    rpad(col("orig_clm_ln_lkup.RFRL_ID_TX"), 9, " ").alias("RFRL_ID_TX"),
    rpad(col("orig_clm_ln_lkup.SVC_ID"), 4, " ").alias("SVC_ID"),
    rpad(col("orig_clm_ln_lkup.SVC_PRICE_RULE_ID"), 4, " ").alias("SVC_PRICE_RULE_ID"),
    rpad(col("orig_clm_ln_lkup.SVC_RULE_TYP_TX"), 20, " ").alias("SVC_RULE_TYP_TX"),
    col("orig_clm_ln_lkup.CLM_LN_SVC_LOC_TYP_CD_SK").alias("CLM_LN_SVC_LOC_TYP_CD_SK"),
    col("orig_clm_ln_lkup.CLM_LN_SVC_PRICE_RULE_CD_SK").alias("CLM_LN_SVC_PRICE_RULE_CD_SK"),
    (col("orig_clm_ln_lkup.NON_PAR_SAV_AMT") * lit(-1)).alias("NON_PAR_SAV_AMT"),
    col("orig_clm_ln_lkup.VBB_RULE_SK").alias("VBB_RULE_SK"),
    col("orig_clm_ln_lkup.VBB_EXCD_SK").alias("VBB_EXCD_SK"),
    rpad(col("orig_clm_ln_lkup.CLM_LN_VBB_IN"), 1, " ").alias("CLM_LN_VBB_IN"),
    (col("orig_clm_ln_lkup.ITS_SUPLMT_DSCNT_AMT") * lit(-1)).alias("ITS_SUPLMT_DSCNT_AMT"),
    (col("orig_clm_ln_lkup.ITS_SRCHRG_AMT") * lit(-1)).alias("ITS_SRCHRG_AMT"),
    col("orig_clm_ln_lkup.NDC_SK").alias("NDC_SK"),
    col("orig_clm_ln_lkup.NDC_DRUG_FORM_CD_SK").alias("NDC_DRUG_FORM_CD_SK"),
    (col("orig_clm_ln_lkup.NDC_UNIT_CT") * lit(-1)).alias("NDC_UNIT_CT")
)

# Write final output to a CSV file
write_files(
    df_adj_clm_ln_out,
    f"{adls_path}/load/CLM_LN.{Source}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
# Retrieve parameter values
RunID = get_widget_value('RunID', '')
Source = get_widget_value('Source', '')
IDSOwner = get_widget_value('IDSOwner', '')
ids_secret_name = get_widget_value('ids_secret_name', '')

# Setup DB connection
jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# -----------------------------------------------------------------------------
# Read from IDS (DB2Connector) - "ids" stage
# -----------------------------------------------------------------------------

# 1) adj_clm_remit_hist
extract_query_adj_clm_remit_hist = f"""
SELECT 
CLM_REMIT_HIST.CLM_REMIT_HIST_SK,
CLM_REMIT_HIST.SRC_SYS_CD_SK,
CLM_REMIT_HIST.CLM_ID,
CLM_REMIT_HIST.CRT_RUN_CYC_EXCTN_SK,
CLM_REMIT_HIST.LAST_UPDT_RUN_CYC_EXCTN_SK,
CLM_REMIT_HIST.CLM_SK,
CLM_REMIT_HIST.SUPRESS_EOB_IN,
CLM_REMIT_HIST.SUPRESS_REMIT_IN,
CLM_REMIT_HIST.ACTL_PD_AMT,
CLM_REMIT_HIST.COB_PD_AMT,
CLM_REMIT_HIST.COINS_AMT,
CLM_REMIT_HIST.CNSD_CHRG_AMT,
CLM_REMIT_HIST.COPAY_AMT,
CLM_REMIT_HIST.DEDCT_AMT,
CLM_REMIT_HIST.DSALW_AMT,
CLM_REMIT_HIST.ER_COPAY_AMT,
CLM_REMIT_HIST.INTRST_AMT,
CLM_REMIT_HIST.NO_RESP_AMT,
CLM_REMIT_HIST.PATN_RESP_AMT,
CLM_REMIT_HIST.PROV_WRTOFF_AMT,
CLM_REMIT_HIST.CALC_ACTL_PD_AMT_IN,
CLM_REMIT_HIST.ALT_CHRG_IN,
CLM_REMIT_HIST.ALT_CHRG_PROV_WRTOFF_AMT
FROM {IDSOwner}.CLM_REMIT_HIST  CLM_REMIT_HIST, 
     {IDSOwner}.W_DRUG_CLM ADJ
WHERE CLM_REMIT_HIST.CLM_SK = ADJ.CLM_SK
"""
df_adj_clm_remit_hist = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_adj_clm_remit_hist)
    .load()
)

# 2) orig_clm_remit_hist_hash
extract_query_orig_clm_remit_hist_hash = f"""
SELECT 
R.RVRSL_CLM_SK as CLM_SK,
CLM_REMIT_HIST.CLM_REMIT_HIST_SK,
CLM_REMIT_HIST.SRC_SYS_CD_SK,
R.RVRSL_CLM_ID as CLM_ID,
CLM_REMIT_HIST.CRT_RUN_CYC_EXCTN_SK,
CLM_REMIT_HIST.LAST_UPDT_RUN_CYC_EXCTN_SK,
CLM_REMIT_HIST.SUPRESS_EOB_IN,
CLM_REMIT_HIST.SUPRESS_REMIT_IN,
CLM_REMIT_HIST.ACTL_PD_AMT,
CLM_REMIT_HIST.COB_PD_AMT,
CLM_REMIT_HIST.COINS_AMT,
CLM_REMIT_HIST.CNSD_CHRG_AMT,
CLM_REMIT_HIST.COPAY_AMT,
CLM_REMIT_HIST.DEDCT_AMT,
CLM_REMIT_HIST.DSALW_AMT,
CLM_REMIT_HIST.ER_COPAY_AMT,
CLM_REMIT_HIST.INTRST_AMT,
CLM_REMIT_HIST.NO_RESP_AMT,
CLM_REMIT_HIST.PATN_RESP_AMT,
CLM_REMIT_HIST.PROV_WRTOFF_AMT,
CLM_REMIT_HIST.CALC_ACTL_PD_AMT_IN,
CLM_REMIT_HIST.ALT_CHRG_IN,
CLM_REMIT_HIST.ALT_CHRG_PROV_WRTOFF_AMT
FROM
{IDSOwner}.CLM_REMIT_HIST CLM_REMIT_HIST, 
{IDSOwner}.W_CLM_RVRSL  R
WHERE 
R.ORIG_CLM_SK = CLM_REMIT_HIST.CLM_SK
"""
df_orig_clm_remit_hist_hash = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_orig_clm_remit_hist_hash)
    .load()
)

# 3) orig_clm_cob_hash
extract_query_orig_clm_cob_hash = f"""
SELECT R.RVRSL_CLM_SK as CLM_SK,
CLM_COB.CLM_COB_SK as CLM_COB_SK,
CLM_COB.SRC_SYS_CD_SK as SRC_SYS_CD_SK,
R.RVRSL_CLM_ID as CLM_ID,
CLM_COB.CLM_COB_TYP_CD_SK as CLM_COB_TYP_CD_SK,
CLM_COB.CRT_RUN_CYC_EXCTN_SK as CRT_RUN_CYC_EXCTN_SK,
CLM_COB.LAST_UPDT_RUN_CYC_EXCTN_SK as LAST_UPDT_RUN_CYC_EXCTN_SK,
CLM_COB.CLM_COB_LIAB_TYP_CD_SK as CLM_COB_LIAB_TYP_CD_SK,
CLM_COB.ALW_AMT as ALW_AMT,
CLM_COB.COPAY_AMT as COPAY_AMT,
CLM_COB.DEDCT_AMT as DEDCT_AMT,
CLM_COB.DSALW_AMT as DSALW_AMT,
CLM_COB.MED_COINS_AMT as MED_COINS_AMT,
CLM_COB.MNTL_HLTH_COINS_AMT as MNTL_HLTH_COINS_AMT,
CLM_COB.PD_AMT as PD_AMT,
CLM_COB.SANC_AMT as SANC_AMT,
CLM_COB.COB_CAR_RSN_CD_TX as COB_CAR_RSN_CD_TX,
CLM_COB.COB_CAR_RSN_TX as COB_CAR_RSN_TX
FROM {IDSOwner}.CLM_COB CLM_COB, 
     {IDSOwner}.W_CLM_RVRSL  R
WHERE
 R.ORIG_CLM_SK = CLM_COB.CLM_SK
"""
df_orig_clm_cob_hash = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_orig_clm_cob_hash)
    .load()
)

# 4) adj_clm_cob
extract_query_adj_clm_cob = f"""
SELECT CLM_COB.CLM_COB_SK as CLM_COB_SK,
CLM_COB.SRC_SYS_CD_SK as SRC_SYS_CD_SK,
CLM_COB.CLM_ID as CLM_ID,
CLM_COB.CLM_COB_TYP_CD_SK as CLM_COB_TYP_CD_SK,
CLM_COB.CRT_RUN_CYC_EXCTN_SK as CRT_RUN_CYC_EXCTN_SK,
CLM_COB.LAST_UPDT_RUN_CYC_EXCTN_SK as LAST_UPDT_RUN_CYC_EXCTN_SK,
CLM_COB.CLM_SK as CLM_SK,
CLM_COB.CLM_COB_LIAB_TYP_CD_SK as CLM_COB_LIAB_TYP_CD_SK,
CLM_COB.ALW_AMT as ALW_AMT,
CLM_COB.COPAY_AMT as COPAY_AMT,
CLM_COB.DEDCT_AMT as DEDCT_AMT,
CLM_COB.DSALW_AMT as DSALW_AMT,
CLM_COB.MED_COINS_AMT as MED_COINS_AMT,
CLM_COB.MNTL_HLTH_COINS_AMT as MNTL_HLTH_COINS_AMT,
CLM_COB.PD_AMT as PD_AMT,
CLM_COB.SANC_AMT as SANC_AMT,
CLM_COB.COB_CAR_RSN_CD_TX as COB_CAR_RSN_CD_TX,
CLM_COB.COB_CAR_RSN_TX as COB_CAR_RSN_TX
FROM {IDSOwner}.CLM_COB CLM_COB,
     {IDSOwner}.W_DRUG_CLM ADJ
WHERE
 CLM_COB.CLM_SK = ADJ.CLM_SK
"""
df_adj_clm_cob = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_adj_clm_cob)
    .load()
)

# 5) orig_DrugClmPrice_hash
extract_query_orig_DrugClmPrice_hash = f"""
SELECT
RVRSL.RVRSL_CLM_SK as CLM_SK,
DCF.DRUG_CLM_SK,
DCF.SRC_SYS_CD_SK,
RVRSL.RVRSL_CLM_ID as CLM_ID,
CRT_RUN_CYC_EXCTN_SK,
LAST_UPDT_RUN_CYC_EXCTN_SK,
BUY_CST_SRC_CD_SK,
GNRC_OVRD_CD_SK,
PDX_NTWK_QLFR_CD_SK,
FRMLRY_PROTOCOL_IN,
RECON_IN,
SPEC_PGM_IN,
FINL_PLN_EFF_DT_SK,
ORIG_PD_TRANS_SUBMT_DT_SK,
PRORTD_DISPNS_QTY,
SUBMT_DISPNS_METRIC_QTY,
AVG_WHLSL_PRICE_UNIT_CST_AMT,
WHLSL_ACQSTN_CST_UNIT_CST_AMT,
BUY_DISPNS_FEE_AMT,
BUY_INGR_CST_AMT,
BUY_RATE_PCT,
BUY_SLS_TAX_AMT,
BUY_TOT_DUE_AMT,
BUY_TOT_OTHR_AMT,
CST_TYP_UNIT_CST_AMT,
INVC_TOT_DUE_AMT,
SELL_CST_TYP_UNIT_CST_AMT,
SELL_RATE_PCT,
SPREAD_DISPNS_FEE_AMT,
SPREAD_INGR_CST_AMT,
SPREAD_SLS_TAX_AMT,
BUY_CST_TYP_ID,
BUY_PRICE_TYP_ID,
CST_TYP_ID,
DRUG_TYP_ID,
FINL_PLN_ID,
GNRC_PROD_ID,
SELL_PRICE_TYP_ID,
SPEC_PGM_ID,
BUY_INCNTV_FEE_AMT,
SELL_INCNTV_FEE_AMT,
SELL_OTHR_AMT,
SELL_OTHR_PAYER_AMT,
SPREAD_INCNTV_FEE_AMT,
FRMLRY_SK,
TIER_ID,
DRUG_TYP_CD_SK
FROM {IDSOwner}.DRUG_CLM_PRICE DCF, {IDSOwner}.W_CLM_RVRSL RVRSL 
WHERE DCF.DRUG_CLM_SK = RVRSL.ORIG_CLM_SK
"""
df_orig_DrugClmPrice_hash = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_orig_DrugClmPrice_hash)
    .load()
)

# 6) adj_DrugClmPrice
extract_query_adj_DrugClmPrice = f"""
SELECT 
DCF.DRUG_CLM_SK,
DCF.SRC_SYS_CD_SK,
DCF.CLM_ID,
CRT_RUN_CYC_EXCTN_SK,
LAST_UPDT_RUN_CYC_EXCTN_SK,
AVG_WHLSL_PRICE_UNIT_CST_AMT,
WHLSL_ACQSTN_CST_UNIT_CST_AMT,
BUY_DISPNS_FEE_AMT,
BUY_INGR_CST_AMT,
BUY_SLS_TAX_AMT,
BUY_TOT_DUE_AMT,
CST_TYP_UNIT_CST_AMT,
INVC_TOT_DUE_AMT,
SELL_CST_TYP_UNIT_CST_AMT,
SPREAD_DISPNS_FEE_AMT,
SPREAD_INGR_CST_AMT,
SPREAD_SLS_TAX_AMT,
PLN_DRUG_STTUS_CD_SK,
DRUG_PLN_TYP_ID
FROM {IDSOwner}.DRUG_CLM_PRICE DCF, {IDSOwner}.W_DRUG_CLM ADJ 
WHERE DCF.DRUG_CLM_SK = ADJ.DRUG_CLM_SK
"""
df_adj_DrugClmPrice = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_adj_DrugClmPrice)
    .load()
)

# -----------------------------------------------------------------------------
# Deduplicate hashed file dataframes (Scenario A) using "dedup_sort"
# -----------------------------------------------------------------------------
df_orig_clm_remit_hist_hash_dedup = dedup_sort(df_orig_clm_remit_hist_hash, ["CLM_SK"], [])
df_orig_clm_cob_hash_dedup = dedup_sort(df_orig_clm_cob_hash, ["CLM_SK"], [])
df_orig_DrugClmPrice_hash_dedup = dedup_sort(df_orig_DrugClmPrice_hash, ["CLM_SK"], [])

# -----------------------------------------------------------------------------
# clm_cob_hash => clm_cob (Transformer) => "ClmCob" (CSeqFileStage)
# -----------------------------------------------------------------------------
df_clm_cob = (
    df_adj_clm_cob.alias("adj")
    .join(df_orig_clm_cob_hash_dedup.alias("orig"), F.col("adj.CLM_SK") == F.col("orig.CLM_SK"), "inner")
    .select(
        F.col("adj.CLM_COB_SK").alias("CLM_COB_SK"),
        F.col("adj.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("adj.CLM_ID").alias("CLM_ID"),
        F.col("adj.CLM_COB_TYP_CD_SK").alias("CLM_COB_TYP_CD_SK"),
        F.col("adj.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("adj.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("adj.CLM_SK").alias("CLM_SK"),
        F.col("orig.CLM_COB_LIAB_TYP_CD_SK").alias("CLM_COB_LIAB_TYP_CD_SK"),
        (F.col("orig.ALW_AMT") * -1).alias("ALW_AMT"),
        (F.col("orig.COPAY_AMT") * -1).alias("COPAY_AMT"),
        (F.col("orig.DEDCT_AMT") * -1).alias("DEDCT_AMT"),
        (F.col("orig.DSALW_AMT") * -1).alias("DSALW_AMT"),
        (F.col("orig.MED_COINS_AMT") * -1).alias("MED_COINS_AMT"),
        (F.col("orig.MNTL_HLTH_COINS_AMT") * -1).alias("MNTL_HLTH_COINS_AMT"),
        (F.col("orig.PD_AMT") * -1).alias("PD_AMT"),
        (F.col("orig.SANC_AMT") * -1).alias("SANC_AMT"),
        F.col("orig.COB_CAR_RSN_CD_TX").alias("COB_CAR_RSN_CD_TX"),
        F.col("orig.COB_CAR_RSN_TX").alias("COB_CAR_RSN_TX"),
    )
)

write_files(
    df_clm_cob,
    f"{adls_path}/load/CLM_COB.{Source}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

# -----------------------------------------------------------------------------
# clm_remit_hist_hash => clm_remit_hist (Transformer) => "ClmRemitHist" (CSeqFileStage)
# -----------------------------------------------------------------------------
df_clm_remit_hist = (
    df_adj_clm_remit_hist.alias("adj")
    .join(df_orig_clm_remit_hist_hash_dedup.alias("orig"), F.col("adj.CLM_SK") == F.col("orig.CLM_SK"), "inner")
    .select(
        F.col("adj.CLM_REMIT_HIST_SK").alias("CLM_REMIT_HIST_SK"),
        F.col("adj.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("adj.CLM_ID").alias("CLM_ID"),
        F.col("adj.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("adj.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("adj.CLM_SK").alias("CLM_SK"),
        F.col("orig.SUPRESS_EOB_IN").alias("SUPRESS_EOB_IN"),
        F.col("orig.SUPRESS_REMIT_IN").alias("SUPRESS_REMIT_IN"),
        F.when(F.col("orig.ACTL_PD_AMT").isNull(), F.lit(0.00)).otherwise(F.col("orig.ACTL_PD_AMT") * -1).alias("ACTL_PD_AMT"),
        F.when(F.col("orig.COB_PD_AMT").isNull(), F.lit(0.00)).otherwise(F.col("orig.COB_PD_AMT") * -1).alias("COB_PD_AMT"),
        F.when(F.col("orig.COINS_AMT").isNull(), F.lit(0.00)).otherwise(F.col("orig.COINS_AMT") * -1).alias("COINS_AMT"),
        F.when(F.col("orig.CNSD_CHRG_AMT").isNull(), F.lit(0.00)).otherwise(F.col("orig.CNSD_CHRG_AMT") * -1).alias("CNSD_CHRG_AMT"),
        F.when(F.col("orig.COPAY_AMT").isNull(), F.lit(0.00)).otherwise(F.col("orig.COPAY_AMT") * -1).alias("COPAY_AMT"),
        F.when(F.col("orig.DEDCT_AMT").isNull(), F.lit(0.00)).otherwise(F.col("orig.DEDCT_AMT") * -1).alias("DEDCT_AMT"),
        F.when(F.col("orig.DSALW_AMT").isNull(), F.lit(0.00)).otherwise(F.col("orig.DSALW_AMT") * -1).alias("DSALW_AMT"),
        F.when(F.col("orig.ER_COPAY_AMT").isNull(), F.lit(0.00)).otherwise(F.col("orig.ER_COPAY_AMT") * -1).alias("ER_COPAY_AMT"),
        F.when(F.col("orig.INTRST_AMT").isNull(), F.lit(0.00)).otherwise(F.col("orig.INTRST_AMT") * -1).alias("INTRST_AMT"),
        F.when(F.col("orig.NO_RESP_AMT").isNull(), F.lit(0.00)).otherwise(F.col("orig.NO_RESP_AMT") * -1).alias("NO_RESP_AMT"),
        F.when(F.col("orig.PATN_RESP_AMT").isNull(), F.lit(0.00)).otherwise(F.col("orig.PATN_RESP_AMT") * -1).alias("PATN_RESP_AMT"),
        F.when(F.col("orig.PROV_WRTOFF_AMT").isNull(), F.lit(0.00)).otherwise(F.col("orig.PROV_WRTOFF_AMT") * -1).alias("PROV_WRTOFF_AMT"),
        F.when(F.col("orig.CALC_ACTL_PD_AMT_IN").isNull(), F.lit("0.00")).otherwise(F.col("orig.CALC_ACTL_PD_AMT_IN")).alias("CALC_ACTL_PD_AMT_IN"),
        F.lit(0.00).alias("PCA_PD_AMT"),
        F.when(F.col("orig.ALT_CHRG_IN").isNull(), F.lit("N")).otherwise(F.col("orig.ALT_CHRG_IN")).alias("ALT_CHRG_IN"),
        F.when(F.col("orig.ALT_CHRG_PROV_WRTOFF_AMT").isNull(), F.lit(0.00)).otherwise(F.col("orig.ALT_CHRG_PROV_WRTOFF_AMT") * -1).alias("ALT_CHRG_PROV_WRTOFF_AMT"),
    )
)

# Apply rpad for char columns: CLM_ID length=20, SUPRESS_EOB_IN=1, SUPRESS_REMIT_IN=1, CALC_ACTL_PD_AMT_IN=1, ALT_CHRG_IN=1
df_clm_remit_hist = df_clm_remit_hist.withColumn(
    "CLM_ID", F.rpad(F.col("CLM_ID"), 20, " ")
).withColumn(
    "SUPRESS_EOB_IN", F.rpad(F.col("SUPRESS_EOB_IN"), 1, " ")
).withColumn(
    "SUPRESS_REMIT_IN", F.rpad(F.col("SUPRESS_REMIT_IN"), 1, " ")
).withColumn(
    "CALC_ACTL_PD_AMT_IN", F.rpad(F.col("CALC_ACTL_PD_AMT_IN"), 1, " ")
).withColumn(
    "ALT_CHRG_IN", F.rpad(F.col("ALT_CHRG_IN"), 1, " ")
)

write_files(
    df_clm_remit_hist,
    f"{adls_path}/load/CLM_REMIT_HIST.{Source}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

# -----------------------------------------------------------------------------
# drug_clm_price_hash => xfm_drug_clm_price (Transformer) => "drug_clm_price" (CSeqFileStage)
# -----------------------------------------------------------------------------
# For this join, note that "orig_DrugClmPrice_hash" includes columns DRUG_CLM_SK, so we join on DRUG_CLM_SK

df_orig_DrugClmPrice_hash_dedup_alias = df_orig_DrugClmPrice_hash_dedup.alias("orig")
df_adj_DrugClmPrice_alias = df_adj_DrugClmPrice.alias("adj")

df_drug_clm_price_pre = (
    df_adj_DrugClmPrice_alias.join(
        df_orig_DrugClmPrice_hash_dedup_alias,
        F.col("adj.DRUG_CLM_SK") == F.col("orig.DRUG_CLM_SK"),
        "inner"
    )
    .select(
        F.col("adj.DRUG_CLM_SK").alias("DRUG_CLM_SK"),
        F.col("adj.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("adj.CLM_ID").alias("CLM_ID"),
        F.col("adj.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("adj.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("orig.BUY_CST_SRC_CD_SK").alias("BUY_CST_SRC_CD_SK"),
        F.col("orig.GNRC_OVRD_CD_SK").alias("GNRC_OVRD_CD_SK"),
        F.col("orig.PDX_NTWK_QLFR_CD_SK").alias("PDX_NTWK_QLFR_CD_SK"),
        F.col("orig.FRMLRY_PROTOCOL_IN").alias("FRMLRY_PROTOCOL_IN"),
        F.lit(None).alias("RECON_IN"),  # @NULL in DataStage => None
        F.col("orig.SPEC_PGM_IN").alias("SPEC_PGM_IN"),
        F.col("orig.FINL_PLN_EFF_DT_SK").alias("FINL_PLN_EFF_DT_SK"),
        F.col("orig.ORIG_PD_TRANS_SUBMT_DT_SK").alias("ORIG_PD_TRANS_SUBMT_DT_SK"),
        F.col("orig.PRORTD_DISPNS_QTY").alias("PRORTD_DISPNS_QTY"),
        F.col("orig.SUBMT_DISPNS_METRIC_QTY").alias("SUBMT_DISPNS_METRIC_QTY"),
        F.when(F.col("orig.AVG_WHLSL_PRICE_UNIT_CST_AMT").isNull(), F.lit(0.00))
         .otherwise(F.col("orig.AVG_WHLSL_PRICE_UNIT_CST_AMT") * -1)
         .alias("AVG_WHLSL_PRICE_UNIT_CST_AMT"),
        F.when(F.col("orig.WHLSL_ACQSTN_CST_UNIT_CST_AMT").isNull(), F.lit(0.00))
         .otherwise(F.col("orig.WHLSL_ACQSTN_CST_UNIT_CST_AMT") * -1)
         .alias("WHLSL_ACQSTN_CST_UNIT_CST_AMT"),
        F.when(F.col("orig.BUY_DISPNS_FEE_AMT").isNull(), F.lit(0.00))
         .otherwise(F.col("orig.BUY_DISPNS_FEE_AMT") * -1)
         .alias("BUY_DISPNS_FEE_AMT"),
        F.when(F.col("orig.BUY_INGR_CST_AMT").isNull(), F.lit(0.00))
         .otherwise(F.col("orig.BUY_INGR_CST_AMT") * -1)
         .alias("BUY_INGR_CST_AMT"),
        F.col("orig.BUY_RATE_PCT").alias("BUY_RATE_PCT"),
        F.when(F.col("orig.BUY_SLS_TAX_AMT").isNull(), F.lit(0.00))
         .otherwise(F.col("orig.BUY_SLS_TAX_AMT") * -1)
         .alias("BUY_SLS_TAX_AMT"),
        F.when(F.col("orig.BUY_TOT_DUE_AMT").isNull(), F.lit(0.00))
         .otherwise(F.col("orig.BUY_TOT_DUE_AMT") * -1)
         .alias("BUY_TOT_DUE_AMT"),
        F.when(F.col("orig.BUY_TOT_OTHR_AMT").isNull(), F.lit(0.00))
         .otherwise(F.col("orig.BUY_TOT_OTHR_AMT") * -1)
         .alias("BUY_TOT_OTHR_AMT"),
        F.when(F.col("orig.CST_TYP_UNIT_CST_AMT").isNull(), F.lit(0.00))
         .otherwise(F.col("orig.CST_TYP_UNIT_CST_AMT") * -1)
         .alias("CST_TYP_UNIT_CST_AMT"),
        F.lit(0.00).alias("INVC_TOT_DUE_AMT"),
        F.when(F.col("orig.SELL_CST_TYP_UNIT_CST_AMT").isNull(), F.lit(0.00))
         .otherwise(F.col("orig.SELL_CST_TYP_UNIT_CST_AMT") * -1)
         .alias("SELL_CST_TYP_UNIT_CST_AMT"),
        F.col("orig.SELL_RATE_PCT").alias("SELL_RATE_PCT"),
        F.when(F.col("orig.SPREAD_DISPNS_FEE_AMT").isNull(), F.lit(0.00))
         .otherwise(F.col("orig.SPREAD_DISPNS_FEE_AMT") * -1)
         .alias("SPREAD_DISPNS_FEE_AMT"),
        F.when(F.col("orig.SPREAD_INGR_CST_AMT").isNull(), F.lit(0.00))
         .otherwise(F.col("orig.SPREAD_INGR_CST_AMT") * -1)
         .alias("SPREAD_INGR_CST_AMT"),
        F.when(F.col("orig.SPREAD_SLS_TAX_AMT").isNull(), F.lit(0.00))
         .otherwise(F.col("orig.SPREAD_SLS_TAX_AMT") * -1)
         .alias("SPREAD_SLS_TAX_AMT"),
        F.col("orig.BUY_CST_TYP_ID").alias("BUY_CST_TYP_ID"),
        F.col("orig.BUY_PRICE_TYP_ID").alias("BUY_PRICE_TYP_ID"),
        F.col("orig.CST_TYP_ID").alias("CST_TYP_ID"),
        F.col("orig.DRUG_TYP_ID").alias("DRUG_TYP_ID"),
        F.col("orig.FINL_PLN_ID").alias("FINL_PLN_ID"),
        F.col("orig.GNRC_PROD_ID").alias("GNRC_PROD_ID"),
        F.col("orig.SELL_PRICE_TYP_ID").alias("SELL_PRICE_TYP_ID"),
        F.col("orig.SPEC_PGM_ID").alias("SPEC_PGM_ID"),
        F.when(F.col("orig.BUY_INCNTV_FEE_AMT").isNull(), F.lit(0.00))
         .otherwise(F.col("orig.BUY_INCNTV_FEE_AMT") * -1)
         .alias("BUY_INCNTV_FEE_AMT"),
        F.when(F.col("orig.SELL_OTHR_AMT").isNull(), F.lit(0.00))
         .otherwise(F.col("orig.SELL_OTHR_AMT") * -1)
         .alias("SELL_OTHR_AMT"),
        F.when(F.col("orig.SELL_OTHR_PAYER_AMT").isNull(), F.lit(0.00))
         .otherwise(F.col("orig.SELL_OTHR_PAYER_AMT") * -1)
         .alias("SELL_OTHR_PAYER_AMT"),
        F.when(F.col("orig.SELL_INCNTV_FEE_AMT").isNull(), F.lit(0.00))
         .otherwise(F.col("orig.SELL_INCNTV_FEE_AMT") * -1)
         .alias("SELL_INCNTV_FEE_AMT"),
        F.when(F.col("orig.SPREAD_INCNTV_FEE_AMT").isNull(), F.lit(0.00))
         .otherwise(F.col("orig.SPREAD_INCNTV_FEE_AMT") * -1)
         .alias("SPREAD_INCNTV_FEE_AMT"),
        F.col("orig.FRMLRY_SK").alias("FRMLRY_SK"),
        F.col("orig.TIER_ID").alias("TIER_ID"),
        F.col("orig.DRUG_TYP_CD_SK").alias("DRUG_TYP_CD_SK"),
        F.col("adj.PLN_DRUG_STTUS_CD_SK").alias("PLN_DRUG_STTUS_CD_SK"),
        F.col("adj.DRUG_PLN_TYP_ID").alias("DRUG_PLN_TYP_ID"),
    )
)

# Apply rpad for columns with SqlType=char in the reference:
# FRMLRY_PROTOCOL_IN (1), RECON_IN (1), SPEC_PGM_IN (1), FINL_PLN_EFF_DT_SK (10), ORIG_PD_TRANS_SUBMT_DT_SK (10)
df_drug_clm_price = df_drug_clm_price_pre.withColumn(
    "FRMLRY_PROTOCOL_IN", F.rpad(F.col("FRMLRY_PROTOCOL_IN"), 1, " ")
).withColumn(
    "RECON_IN", F.rpad(F.col("RECON_IN").cast(StringType()), 1, " ")
).withColumn(
    "SPEC_PGM_IN", F.rpad(F.col("SPEC_PGM_IN"), 1, " ")
).withColumn(
    "FINL_PLN_EFF_DT_SK", F.rpad(F.col("FINL_PLN_EFF_DT_SK"), 10, " ")
).withColumn(
    "ORIG_PD_TRANS_SUBMT_DT_SK", F.rpad(F.col("ORIG_PD_TRANS_SUBMT_DT_SK"), 10, " ")
)

write_files(
    df_drug_clm_price,
    f"{adls_path}/load/DRUG_CLM_PRICE.{Source}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)