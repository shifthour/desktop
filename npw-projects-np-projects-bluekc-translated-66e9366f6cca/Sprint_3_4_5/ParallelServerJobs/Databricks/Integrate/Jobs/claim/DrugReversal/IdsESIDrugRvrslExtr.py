# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
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
# MAGIC Shanmugam A 	 2017-03-02         5321                    Modify DB2 stage "ids" link "orig_clm_hash" Columns - Column 1 and             IntegrateDev2            Jag Yelavarthi         2017-03-07
# MAGIC 					 column 4 needs to be changed to RVRSL_CLM_SK and RVRSL_CLM_ID
# MAGIC 					SQL - Column 9 and 10 needs to be aliased to ORIG_CLM_SK and ORIG_CLM_ID

# MAGIC Load file for Claim Balancing. The data is appended to the Claim Balancing file created in the Extract job
# MAGIC Pull original claim and drug claim paid data for adjusted claims
# MAGIC W_DRUG_CLM Created in IdsDrugClmFkey
# MAGIC 
# MAGIC W_CLM_RVRSL created in IdsDrugReversalBuild
# MAGIC Writing Sequential Files to ../load
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Parameters
RunID = get_widget_value('RunID','')
Source = get_widget_value('Source','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

# Obtain JDBC configuration
jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# ---------------------------------------------------------------------------
# Stage: ids (DB2Connector)
#   Output Pin 1 (adj_clm) -> link "V165S7P1" -> next stage "clm" (as df_adj_clm)
#   Output Pin 2 (orig_clm_hash) -> link "V165S7P2" -> next stage "clm_and_drug_clm_hash" (as df_orig_clm_hash)
# ---------------------------------------------------------------------------

extract_query_adj_clm = """SELECT 
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
 CLM.CLM_SUBMT_ICD_VRSN_CD_SK as CLM_SUBMT_ICD_VRSN_CD_SK
FROM #$IDSOwner#.CLM CLM, #$IDSOwner#.W_DRUG_CLM ADJ, #$IDSOwner#.CD_MPPNG NG1 
WHERE ADJ.CLM_SK = CLM.CLM_SK
  and ADJ.SRC_SYS_CD_SK = NG1.CD_MPPNG_SK
"""
extract_query_adj_clm = extract_query_adj_clm.replace("#$IDSOwner#", IDSOwner)

df_adj_clm = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_adj_clm)
    .load()
)

extract_query_orig_clm_hash = """SELECT 
 R.RVRSL_CLM_SK,
 NG.SRC_SYS_CD,
 R.SRC_SYS_CD_SK,
 R.RVRSL_CLM_ID,
 CLM.CRT_RUN_CYC_EXCTN_SK,
 CLM.LAST_UPDT_RUN_CYC_EXCTN_SK,
 CLM.CLM_SK AS ORIG_CLM_SK,
 CLM.CLM_ID AS ORIG_CLM_ID,
 ADJ_FROM_CLM_SK,
 ADJ_TO_CLM_SK,
 CLS_SK,
 CLS_PLN_SK,
 EXPRNC_CAT_SK,
 FNCL_LOB_SK,
 GRP_SK,
 MBR_SK,
 NTWK_SK,
 PROD_SK,
 SUBGRP_SK,
 SUB_SK,
 CLM_ACDNT_ST_CD_SK,
 CLM_ACTV_BCBS_PLN_CD_SK,
 CLM_AGMNT_SRC_CD_SK,
 CLM_BTCH_ACTN_CD_SK,
 CLM_CAP_CD_SK,
 CLM_CAT_CD_SK,
 CLM_CHK_CYC_OVRD_CD_SK,
 CLM_COB_CD_SK,
 CLM_EOB_EXCD_SK,
 CLM_FINL_DISP_CD_SK,
 CLM_INPT_METH_CD_SK,
 CLM_INPT_SRC_CD_SK,
 CLM_INTER_PLN_PGM_CD_SK,
 CLM_NTWK_STTUS_CD_SK,
 CLM_NONPAR_PROV_PFX_CD_SK,
 CLM_OTHR_BNF_CD_SK,
 CLM_PAYE_CD_SK,
 CLM_PRCS_CTL_AGNT_PFX_CD_SK,
 CLM_SVC_DEFN_PFX_CD_SK,
 CLM_SVC_PROV_SPEC_CD_SK,
 CLM_SVC_PROV_TYP_CD_SK,
 CLM_STTUS_CD_SK,
 CLM_SUBMTTING_BCBS_PLN_CD_SK,
 CLM_SUB_BCBS_PLN_CD_SK,
 CLM_SUBTYP_CD_SK,
 CLM_TYP_CD_SK,
 ATCHMT_IN,
 CLNCL_EDIT_IN,
 COBRA_CLM_IN,
 FIRST_PASS_IN,
 LTR_IN,
 MCARE_ASG_IN,
 NOTE_IN,
 PCA_AUDIT_IN,
 PCP_SUBMT_IN,
 PROD_OOA_IN,
 ACDNT_DT_SK,
 INPT_DT_SK,
 MBR_PLN_ELIG_DT_SK,
 NEXT_RVW_DT_SK,
 PD_DT_SK,
 PAYMT_DRAG_CYC_DT_SK,
 PRCS_DT_SK,
 RCVD_DT_SK,
 SVC_STRT_DT_SK,
 SVC_END_DT_SK,
 SMLR_ILNS_DT_SK,
 STTUS_DT_SK,
 WORK_UNABLE_BEG_DT_SK,
 WORK_UNABLE_END_DT_SK,
 ACDNT_AMT,
 ACTL_PD_AMT,
 ALW_AMT,
 DSALW_AMT,
 COINS_AMT,
 CNSD_CHRG_AMT,
 COPAY_AMT,
 CHRG_AMT,
 DEDCT_AMT,
 PAYBL_AMT,
 CLM_CT,
 MBR_AGE,
 ADJ_FROM_CLM_ID,
 ADJ_TO_CLM_ID,
 DOC_TX_ID,
 MCAID_RESUB_NO,
 MCARE_ID,
 MBR_SFX_NO,
 PATN_ACCT_NO,
 PAYMT_REF_ID,
 PROV_AGMNT_ID,
 RFRNG_PROV_TX,
 SUB_ID,
 ALPHA_PFX_SK,
 CLM_ACDNT_CD_SK,
 HOST_IN,
 D.DRUG_CLM_SK,
 NDC_SK,
 PRSCRB_PROV_DEA_SK,
 DRUG_CLM_DISPNS_AS_WRTN_CD_SK,
 DRUG_CLM_LGL_STTUS_CD_SK,
 DRUG_CLM_TIER_CD_SK,
 DRUG_CLM_VNDR_STTUS_CD_SK,
 CMPND_IN,
 FRMLRY_IN,
 GNRC_DRUG_IN,
 MAIL_ORDER_IN,
 MNTN_IN,
 MAX_ALW_CST_REDC_IN,
 NON_FRMLRY_DRUG_IN,
 SNGL_SRC_IN,
 ADJ_DT_SK,
 D.FILL_DT_SK,
 DISPNS_FEE_AMT,
 HLTH_PLN_EXCL_AMT,
 HLTH_PLN_PD_AMT,
 INGR_CST_ALW_AMT,
 INGR_CST_CHRGD_AMT,
 INGR_SAV_AMT,
 MBR_DEDCT_EXCL_AMT,
 MBR_DIFF_PD_AMT,
 MBR_OOP_AMT,
 MBR_OOP_EXCL_AMT,
 OTHR_SAV_AMT,
 RX_ALW_QTY,
 RX_SUBMT_QTY,
 SLS_TAX_AMT,
 RX_ALW_DAYS_SUPL_QTY,
 RX_ORIG_DAYS_SUPL_QTY,
 PDX_NTWK_ID,
 D.RX_NO,
 RFL_NO,
 D.VNDR_CLM_NO,
 VNDR_PREAUTH_ID,
 DRUG_CLM_BNF_FRMLRY_POL_CD_SK,
 DRUG_CLM_BNF_RSTRCT_CD_SK,
 DRUG_CLM_MCPARTD_COVDRUG_CD_SK,
 DRUG_CLM_PDX_NTWK_CD_SK,
 DRUG_CLM_PRAUTH_CD_SK,
 MNDTRY_MAIL_ORDER_IN,
 D.ADM_FEE_AMT,
 D.DRUG_CLM_BILL_BSS_CD_SK,
 D.AVG_WHLSL_PRICE_AMT,
 CLM.MCAID_STTUS_ID,
 CLM.PATN_PD_AMT,
 D.UCR_AMT,
 D.DRUG_CLM_PRTL_FILL_CD_SK,
 D.DRUG_CLM_PDX_TYP_CD_SK,
 D.INCNTV_FEE_AMT
FROM #$IDSOwner#.CLM CLM,
     #$IDSOwner#.W_CLM_RVRSL  R,
     #$IDSOwner#.DRUG_CLM D,
     #$IDSOwner#.CD_MPPNG NG,
     #$IDSOwner#.CLM_PROV PROV,
     #$IDSOwner#.W_DRUG_CLM ADJ
WHERE R.ORIG_CLM_SK = CLM.CLM_SK
  and R.ORIG_CLM_SK = D.CLM_SK
  and R.SRC_SYS_CD_SK = NG.CD_MPPNG_SK
  and R.RVRSL_CLM_SK = ADJ.CLM_SK
  and CLM.CLM_SK = PROV.CLM_SK
  and PROV.PROV_SK = ADJ.PROV_SK
  and NG.TRGT_CD <> 'BCBSSC'

UNION

SELECT 
 R.RVRSL_CLM_SK,
 NG.SRC_SYS_CD,
 R.SRC_SYS_CD_SK,
 R.RVRSL_CLM_ID,
 CLM.CRT_RUN_CYC_EXCTN_SK,
 CLM.LAST_UPDT_RUN_CYC_EXCTN_SK,
 CLM.CLM_SK AS ORIG_CLM_SK,
 CLM.CLM_ID AS ORIG_CLM_ID,
 ADJ_FROM_CLM_SK,
 ADJ_TO_CLM_SK,
 CLS_SK,
 CLS_PLN_SK,
 EXPRNC_CAT_SK,
 FNCL_LOB_SK,
 GRP_SK,
 MBR_SK,
 NTWK_SK,
 PROD_SK,
 SUBGRP_SK,
 SUB_SK,
 CLM_ACDNT_ST_CD_SK,
 CLM_ACTV_BCBS_PLN_CD_SK,
 CLM_AGMNT_SRC_CD_SK,
 CLM_BTCH_ACTN_CD_SK,
 CLM_CAP_CD_SK,
 CLM_CAT_CD_SK,
 CLM_CHK_CYC_OVRD_CD_SK,
 CLM_COB_CD_SK,
 CLM_EOB_EXCD_SK,
 CLM_FINL_DISP_CD_SK,
 CLM_INPT_METH_CD_SK,
 CLM_INPT_SRC_CD_SK,
 CLM_INTER_PLN_PGM_CD_SK,
 CLM_NTWK_STTUS_CD_SK,
 CLM_NONPAR_PROV_PFX_CD_SK,
 CLM_OTHR_BNF_CD_SK,
 CLM_PAYE_CD_SK,
 CLM_PRCS_CTL_AGNT_PFX_CD_SK,
 CLM_SVC_DEFN_PFX_CD_SK,
 CLM_SVC_PROV_SPEC_CD_SK,
 CLM_SVC_PROV_TYP_CD_SK,
 CLM_STTUS_CD_SK,
 CLM_SUBMTTING_BCBS_PLN_CD_SK,
 CLM_SUB_BCBS_PLN_CD_SK,
 CLM_SUBTYP_CD_SK,
 CLM_TYP_CD_SK,
 ATCHMT_IN,
 CLNCL_EDIT_IN,
 COBRA_CLM_IN,
 FIRST_PASS_IN,
 LTR_IN,
 MCARE_ASG_IN,
 NOTE_IN,
 PCA_AUDIT_IN,
 PCP_SUBMT_IN,
 PROD_OOA_IN,
 ACDNT_DT_SK,
 INPT_DT_SK,
 MBR_PLN_ELIG_DT_SK,
 NEXT_RVW_DT_SK,
 PD_DT_SK,
 PAYMT_DRAG_CYC_DT_SK,
 PRCS_DT_SK,
 RCVD_DT_SK,
 SVC_STRT_DT_SK,
 SVC_END_DT_SK,
 SMLR_ILNS_DT_SK,
 STTUS_DT_SK,
 WORK_UNABLE_BEG_DT_SK,
 WORK_UNABLE_END_DT_SK,
 ACDNT_AMT,
 ACTL_PD_AMT,
 ALW_AMT,
 DSALW_AMT,
 COINS_AMT,
 CNSD_CHRG_AMT,
 COPAY_AMT,
 CHRG_AMT,
 DEDCT_AMT,
 PAYBL_AMT,
 CLM_CT,
 MBR_AGE,
 ADJ_FROM_CLM_ID,
 ADJ_TO_CLM_ID,
 DOC_TX_ID,
 MCAID_RESUB_NO,
 MCARE_ID,
 MBR_SFX_NO,
 PATN_ACCT_NO,
 PAYMT_REF_ID,
 PROV_AGMNT_ID,
 RFRNG_PROV_TX,
 SUB_ID,
 ALPHA_PFX_SK,
 CLM_ACDNT_CD_SK,
 HOST_IN,
 D.DRUG_CLM_SK,
 NDC_SK,
 PRSCRB_PROV_DEA_SK,
 DRUG_CLM_DISPNS_AS_WRTN_CD_SK,
 DRUG_CLM_LGL_STTUS_CD_SK,
 DRUG_CLM_TIER_CD_SK,
 DRUG_CLM_VNDR_STTUS_CD_SK,
 CMPND_IN,
 FRMLRY_IN,
 GNRC_DRUG_IN,
 MAIL_ORDER_IN,
 MNTN_IN,
 MAX_ALW_CST_REDC_IN,
 NON_FRMLRY_DRUG_IN,
 SNGL_SRC_IN,
 ADJ_DT_SK,
 D.FILL_DT_SK,
 DISPNS_FEE_AMT,
 HLTH_PLN_EXCL_AMT,
 HLTH_PLN_PD_AMT,
 INGR_CST_ALW_AMT,
 INGR_CST_CHRGD_AMT,
 INGR_SAV_AMT,
 MBR_DEDCT_EXCL_AMT,
 MBR_DIFF_PD_AMT,
 MBR_OOP_AMT,
 MBR_OOP_EXCL_AMT,
 OTHR_SAV_AMT,
 RX_ALW_QTY,
 RX_SUBMT_QTY,
 SLS_TAX_AMT,
 RX_ALW_DAYS_SUPL_QTY,
 RX_ORIG_DAYS_SUPL_QTY,
 PDX_NTWK_ID,
 D.RX_NO,
 RFL_NO,
 D.VNDR_CLM_NO,
 VNDR_PREAUTH_ID,
 DRUG_CLM_BNF_FRMLRY_POL_CD_SK,
 DRUG_CLM_BNF_RSTRCT_CD_SK,
 DRUG_CLM_MCPARTD_COVDRUG_CD_SK,
 DRUG_CLM_PDX_NTWK_CD_SK,
 DRUG_CLM_PRAUTH_CD_SK,
 MNDTRY_MAIL_ORDER_IN,
 D.ADM_FEE_AMT,
 D.DRUG_CLM_BILL_BSS_CD_SK,
 D.AVG_WHLSL_PRICE_AMT,
 CLM.MCAID_STTUS_ID,
 CLM.PATN_PD_AMT,
 D.UCR_AMT,
 D.DRUG_CLM_PRTL_FILL_CD_SK,
 D.DRUG_CLM_PDX_TYP_CD_SK,
 D.INCNTV_FEE_AMT
FROM #$IDSOwner#.CLM CLM,
     #$IDSOwner#.W_CLM_RVRSL  R,
     #$IDSOwner#.DRUG_CLM D,
     #$IDSOwner#.CD_MPPNG NG,
     #$IDSOwner#.CLM_PROV PROV,
     #$IDSOwner#.W_DRUG_CLM ADJ
WHERE R.ORIG_CLM_SK = CLM.CLM_SK
  and R.ORIG_CLM_SK = D.CLM_SK
  and R.SRC_SYS_CD_SK = NG.CD_MPPNG_SK
  and R.RVRSL_CLM_SK = ADJ.CLM_SK
  and CLM.CLM_SK = PROV.CLM_SK
  and NG.TRGT_CD = 'BCBSSC'
"""
extract_query_orig_clm_hash = extract_query_orig_clm_hash.replace("#$IDSOwner#", IDSOwner)

df_orig_clm_hash = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_orig_clm_hash)
    .load()
)

# ---------------------------------------------------------------------------
# Stage: clm_and_drug_clm_hash (CHashedFileStage) - Scenario A
#   => We deduplicate by the primary key columns (CLM_SK).
# ---------------------------------------------------------------------------
df_clm_and_drug_clm_hash = dedup_sort(
    df_orig_clm_hash,
    partition_cols=["RVRSL_CLM_SK"],  # The physical column in the data is RVRSL_CLM_SK but mapped to "CLM_SK" in DS
    sort_cols=[]
).withColumnRenamed("RVRSL_CLM_SK", "CLM_SK")

# Note: The DS layout indicates "CLM_SK" as the primary key in the hashed file.
# We rename "RVRSL_CLM_SK" -> "CLM_SK" to match usage in next stage.

# ---------------------------------------------------------------------------
# Stage: clm (CTransformerStage)
#   Primary input -> df_adj_clm
#   Lookup link -> df_clm_and_drug_clm_hash on (adj_clm.CLM_SK = orig_clm_lkup.CLM_SK) left join
#   Stage variable: svStatusCd = GetFkeyCodes(adj_clm.SRC_SYS_CD, orig_clm_lkup.ORIG_CLM_SK, "CLAIM STATUS", "A09", "X")
#   Outputs: orig_clm_out, ClmMartAdj, ClmMartOrig, RowCount
# ---------------------------------------------------------------------------

df_clm_joined = (
    df_adj_clm.alias("adj_clm")
    .join(
        df_clm_and_drug_clm_hash.alias("orig_clm_lkup"),
        F.col("adj_clm.CLM_SK") == F.col("orig_clm_lkup.CLM_SK"),
        how="left"
    )
)

df_clm_joined = df_clm_joined.withColumn(
    "svStatusCd",
    GetFkeyCodes(
        F.col("adj_clm.SRC_SYS_CD"),
        F.col("orig_clm_lkup.ORIG_CLM_SK"),
        F.lit("CLAIM STATUS"),
        F.lit("A09"),
        F.lit("X")
    )
)

# ----------------------
# Output 1: orig_clm_out => CLM_REVERSAL_DRUG_ORIG_CLM
# Constraint: IsNull(orig_clm_lkup.CLM_SK) = @FALSE
# ----------------------
df_orig_clm_out = df_clm_joined.filter(F.col("orig_clm_lkup.CLM_SK").isNotNull())

df_orig_clm_out = df_orig_clm_out.select(
    F.when(F.col("orig_clm_lkup.ORIG_CLM_SK").isNull(), F.lit(0)).otherwise(F.col("orig_clm_lkup.ORIG_CLM_SK")).alias("CLM_SK"),
    F.when(F.col("orig_clm_lkup.SRC_SYS_CD_SK").isNull(), F.lit(0)).otherwise(F.col("orig_clm_lkup.SRC_SYS_CD_SK")).alias("SRC_SYS_CD_SK"),
    F.when(F.col("orig_clm_lkup.ORIG_CLM_ID").isNull(), F.lit(' ')).otherwise(F.col("orig_clm_lkup.ORIG_CLM_ID")).alias("CLM_ID"),
    F.when(F.col("orig_clm_lkup.CRT_RUN_CYC_EXCTN_SK").isNull(), F.lit(0)).otherwise(F.col("orig_clm_lkup.CRT_RUN_CYC_EXCTN_SK")).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("adj_clm.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(F.col("orig_clm_lkup.ADJ_FROM_CLM_SK").isNull(), F.lit(0)).otherwise(F.col("orig_clm_lkup.ADJ_FROM_CLM_SK")).alias("ADJ_FROM_CLM_SK"),
    F.col("adj_clm.CLM_SK").alias("ADJ_TO_CLM_SK"),
    F.when(F.col("orig_clm_lkup.ALPHA_PFX_SK").isNull(), F.lit(0)).otherwise(F.col("orig_clm_lkup.ALPHA_PFX_SK")).alias("ALPHA_PFX_SK"),
    F.when(F.col("orig_clm_lkup.CLM_EOB_EXCD_SK").isNull(), F.lit(0)).otherwise(F.col("orig_clm_lkup.CLM_EOB_EXCD_SK")).alias("CLM_EOB_EXCD_SK"),
    F.when(F.col("orig_clm_lkup.CLS_SK").isNull(), F.lit(0)).otherwise(F.col("orig_clm_lkup.CLS_SK")).alias("CLS_SK"),
    F.when(F.col("orig_clm_lkup.CLS_PLN_SK").isNull(), F.lit(0)).otherwise(F.col("orig_clm_lkup.CLS_PLN_SK")).alias("CLS_PLN_SK"),
    F.when(F.col("orig_clm_lkup.EXPRNC_CAT_SK").isNull(), F.lit(0)).otherwise(F.col("orig_clm_lkup.EXPRNC_CAT_SK")).alias("EXPRNC_CAT_SK"),
    F.when(F.col("orig_clm_lkup.FNCL_LOB_SK").isNull(), F.lit(0)).otherwise(F.col("orig_clm_lkup.FNCL_LOB_SK")).alias("FNCL_LOB_SK"),
    F.when(F.col("orig_clm_lkup.GRP_SK").isNull(), F.lit(0)).otherwise(F.col("orig_clm_lkup.GRP_SK")).alias("GRP_SK"),
    F.when(F.col("orig_clm_lkup.MBR_SK").isNull(), F.lit(0)).otherwise(F.col("orig_clm_lkup.MBR_SK")).alias("MBR_SK"),
    F.when(F.col("orig_clm_lkup.NTWK_SK").isNull(), F.lit(0)).otherwise(F.col("orig_clm_lkup.NTWK_SK")).alias("NTWK_SK"),
    F.when(F.col("orig_clm_lkup.PROD_SK").isNull(), F.lit(0)).otherwise(F.col("orig_clm_lkup.PROD_SK")).alias("PROD_SK"),
    F.when(F.col("orig_clm_lkup.SUBGRP_SK").isNull(), F.lit(0)).otherwise(F.col("orig_clm_lkup.SUBGRP_SK")).alias("SUBGRP_SK"),
    F.when(F.col("orig_clm_lkup.SUB_SK").isNull(), F.lit(0)).otherwise(F.col("orig_clm_lkup.SUB_SK")).alias("SUB_SK"),
    F.when(F.col("orig_clm_lkup.CLM_ACDNT_CD_SK").isNull(), F.lit(0)).otherwise(F.col("orig_clm_lkup.CLM_ACDNT_CD_SK")).alias("CLM_ACDNT_CD_SK"),
    F.when(F.col("orig_clm_lkup.CLM_ACDNT_ST_CD_SK").isNull(), F.lit(0)).otherwise(F.col("orig_clm_lkup.CLM_ACDNT_ST_CD_SK")).alias("CLM_ACDNT_ST_CD_SK"),
    F.when(F.col("orig_clm_lkup.CLM_ACTV_BCBS_PLN_CD_SK").isNull(), F.lit(0)).otherwise(F.col("orig_clm_lkup.CLM_ACTV_BCBS_PLN_CD_SK")).alias("CLM_ACTV_BCBS_PLN_CD_SK"),
    F.when(F.col("orig_clm_lkup.CLM_AGMNT_SRC_CD_SK").isNull(), F.lit(0)).otherwise(F.col("orig_clm_lkup.CLM_AGMNT_SRC_CD_SK")).alias("CLM_AGMNT_SRC_CD_SK"),
    F.when(F.col("orig_clm_lkup.CLM_BTCH_ACTN_CD_SK").isNull(), F.lit(0)).otherwise(F.col("orig_clm_lkup.CLM_BTCH_ACTN_CD_SK")).alias("CLM_BTCH_ACTN_CD_SK"),
    F.when(F.col("orig_clm_lkup.CLM_CAP_CD_SK").isNull(), F.lit(0)).otherwise(F.col("orig_clm_lkup.CLM_CAP_CD_SK")).alias("CLM_CAP_CD_SK"),
    F.when(F.col("orig_clm_lkup.CLM_CAT_CD_SK").isNull(), F.lit(0)).otherwise(F.col("orig_clm_lkup.CLM_CAT_CD_SK")).alias("CLM_CAT_CD_SK"),
    F.when(F.col("orig_clm_lkup.CLM_CHK_CYC_OVRD_CD_SK").isNull(), F.lit(0)).otherwise(F.col("orig_clm_lkup.CLM_CHK_CYC_OVRD_CD_SK")).alias("CLM_CHK_CYC_OVRD_CD_SK"),
    F.when(F.col("orig_clm_lkup.CLM_COB_CD_SK").isNull(), F.lit(0)).otherwise(F.col("orig_clm_lkup.CLM_COB_CD_SK")).alias("CLM_COB_CD_SK"),
    F.when(F.col("orig_clm_lkup.CLM_FINL_DISP_CD_SK").isNull(), F.lit(0)).otherwise(F.col("orig_clm_lkup.CLM_FINL_DISP_CD_SK")).alias("CLM_FINL_DISP_CD_SK"),
    F.when(F.col("orig_clm_lkup.CLM_INPT_METH_CD_SK").isNull(), F.lit(0)).otherwise(F.col("orig_clm_lkup.CLM_INPT_METH_CD_SK")).alias("CLM_INPT_METH_CD_SK"),
    F.when(F.col("orig_clm_lkup.CLM_INPT_SRC_CD_SK").isNull(), F.lit(0)).otherwise(F.col("orig_clm_lkup.CLM_INPT_SRC_CD_SK")).alias("CLM_INPT_SRC_CD_SK"),
    F.when(F.col("orig_clm_lkup.CLM_INTER_PLN_PGM_CD_SK").isNull(), F.lit(0)).otherwise(F.col("orig_clm_lkup.CLM_INTER_PLN_PGM_CD_SK")).alias("CLM_INTER_PLN_PGM_CD_SK"),
    F.when(F.col("orig_clm_lkup.CLM_NTWK_STTUS_CD_SK").isNull(), F.lit(0)).otherwise(F.col("orig_clm_lkup.CLM_NTWK_STTUS_CD_SK")).alias("CLM_NTWK_STTUS_CD_SK"),
    F.when(F.col("orig_clm_lkup.CLM_NONPAR_PROV_PFX_CD_SK").isNull(), F.lit(0)).otherwise(F.col("orig_clm_lkup.CLM_NONPAR_PROV_PFX_CD_SK")).alias("CLM_NONPAR_PROV_PFX_CD_SK"),
    F.when(F.col("orig_clm_lkup.CLM_OTHR_BNF_CD_SK").isNull(), F.lit(0)).otherwise(F.col("orig_clm_lkup.CLM_OTHR_BNF_CD_SK")).alias("CLM_OTHR_BNF_CD_SK"),
    F.when(F.col("orig_clm_lkup.CLM_PAYE_CD_SK").isNull(), F.lit(0)).otherwise(F.col("orig_clm_lkup.CLM_PAYE_CD_SK")).alias("CLM_PAYE_CD_SK"),
    F.when(F.col("orig_clm_lkup.CLM_PRCS_CTL_AGNT_PFX_CD_SK").isNull(), F.lit(0)).otherwise(F.col("orig_clm_lkup.CLM_PRCS_CTL_AGNT_PFX_CD_SK")).alias("CLM_PRCS_CTL_AGNT_PFX_CD_SK"),
    F.when(F.col("orig_clm_lkup.CLM_SVC_DEFN_PFX_CD_SK").isNull(), F.lit(0)).otherwise(F.col("orig_clm_lkup.CLM_SVC_DEFN_PFX_CD_SK")).alias("CLM_SVC_DEFN_PFX_CD_SK"),
    F.when(F.col("orig_clm_lkup.CLM_SVC_PROV_SPEC_CD_SK").isNull(), F.lit(0)).otherwise(F.col("orig_clm_lkup.CLM_SVC_PROV_SPEC_CD_SK")).alias("CLM_SVC_PROV_SPEC_CD_SK"),
    F.when(F.col("orig_clm_lkup.CLM_SVC_PROV_TYP_CD_SK").isNull(), F.lit(0)).otherwise(F.col("orig_clm_lkup.CLM_SVC_PROV_TYP_CD_SK")).alias("CLM_SVC_PROV_TYP_CD_SK"),
    F.col("svStatusCd").alias("CLM_STTUS_CD_SK"),
    F.when(F.col("orig_clm_lkup.CLM_SUBMTTING_BCBS_PLN_CD_SK").isNull(), F.lit(0)).otherwise(F.col("orig_clm_lkup.CLM_SUBMTTING_BCBS_PLN_CD_SK")).alias("CLM_SUBMTTING_BCBS_PLN_CD_SK"),
    F.when(F.col("orig_clm_lkup.CLM_SUB_BCBS_PLN_CD_SK").isNull(), F.lit(0)).otherwise(F.col("orig_clm_lkup.CLM_SUB_BCBS_PLN_CD_SK")).alias("CLM_SUB_BCBS_PLN_CD_SK"),
    F.when(F.col("orig_clm_lkup.CLM_SUBTYP_CD_SK").isNull(), F.lit(0)).otherwise(F.col("orig_clm_lkup.CLM_SUBTYP_CD_SK")).alias("CLM_SUBTYP_CD_SK"),
    F.when(F.col("orig_clm_lkup.CLM_TYP_CD_SK").isNull(), F.lit(0)).otherwise(F.col("orig_clm_lkup.CLM_TYP_CD_SK")).alias("CLM_TYP_CD_SK"),
    F.rpad(F.when(F.col("orig_clm_lkup.ATCHMT_IN").isNull(), F.lit(' ')).otherwise(F.col("orig_clm_lkup.ATCHMT_IN")), 1, " ").alias("ATCHMT_IN"),
    F.rpad(F.when(F.col("orig_clm_lkup.CLNCL_EDIT_IN").isNull(), F.lit(' ')).otherwise(F.col("orig_clm_lkup.CLNCL_EDIT_IN")), 1, " ").alias("CLNCL_EDIT_IN"),
    F.rpad(F.when(F.col("orig_clm_lkup.COBRA_CLM_IN").isNull(), F.lit(' ')).otherwise(F.col("orig_clm_lkup.COBRA_CLM_IN")), 1, " ").alias("COBRA_CLM_IN"),
    F.rpad(F.when(F.col("orig_clm_lkup.FIRST_PASS_IN").isNull(), F.lit(' ')).otherwise(F.col("orig_clm_lkup.FIRST_PASS_IN")), 1, " ").alias("FIRST_PASS_IN"),
    F.rpad(F.when(F.col("orig_clm_lkup.LTR_IN").isNull(), F.lit(' ')).otherwise(F.col("orig_clm_lkup.LTR_IN")), 1, " ").alias("LTR_IN"),
    F.rpad(F.when(F.col("orig_clm_lkup.MCARE_ASG_IN").isNull(), F.lit(' ')).otherwise(F.col("orig_clm_lkup.MCARE_ASG_IN")), 1, " ").alias("MCARE_ASG_IN"),
    F.rpad(F.when(F.col("orig_clm_lkup.NOTE_IN").isNull(), F.lit(' ')).otherwise(F.col("orig_clm_lkup.NOTE_IN")), 1, " ").alias("NOTE_IN"),
    F.rpad(F.when(F.col("orig_clm_lkup.PCA_AUDIT_IN").isNull(), F.lit(' ')).otherwise(F.col("orig_clm_lkup.PCA_AUDIT_IN")), 1, " ").alias("PCA_AUDIT_IN"),
    F.rpad(F.when(F.col("orig_clm_lkup.PCP_SUBMT_IN").isNull(), F.lit(' ')).otherwise(F.col("orig_clm_lkup.PCP_SUBMT_IN")), 1, " ").alias("PCP_SUBMT_IN"),
    F.rpad(F.when(F.col("orig_clm_lkup.PROD_OOA_IN").isNull(), F.lit(' ')).otherwise(F.col("orig_clm_lkup.PROD_OOA_IN")), 1, " ").alias("PROD_OOA_IN"),
    F.rpad(F.when(F.col("orig_clm_lkup.ACDNT_DT_SK").isNull(), F.lit('0')).otherwise(F.col("orig_clm_lkup.ACDNT_DT_SK")), 10, " ").alias("ACDNT_DT_SK"),
    F.rpad(F.when(F.col("orig_clm_lkup.INPT_DT_SK").isNull(), F.lit('0')).otherwise(F.col("orig_clm_lkup.INPT_DT_SK")), 10, " ").alias("INPT_DT_SK"),
    F.rpad(F.when(F.col("orig_clm_lkup.MBR_PLN_ELIG_DT_SK").isNull(), F.lit('0')).otherwise(F.col("orig_clm_lkup.MBR_PLN_ELIG_DT_SK")), 10, " ").alias("MBR_PLN_ELIG_DT_SK"),
    F.rpad(F.when(F.col("orig_clm_lkup.NEXT_RVW_DT_SK").isNull(), F.lit('0')).otherwise(F.col("orig_clm_lkup.NEXT_RVW_DT_SK")), 10, " ").alias("NEXT_RVW_DT_SK"),
    F.rpad(F.when(F.col("orig_clm_lkup.PD_DT_SK").isNull(), F.lit('0')).otherwise(F.col("orig_clm_lkup.PD_DT_SK")), 10, " ").alias("PD_DT_SK"),
    F.rpad(F.when(F.col("orig_clm_lkup.PAYMT_DRAG_CYC_DT_SK").isNull(), F.lit('0')).otherwise(F.col("orig_clm_lkup.PAYMT_DRAG_CYC_DT_SK")), 10, " ").alias("PAYMT_DRAG_CYC_DT_SK"),
    F.rpad(F.when(F.col("orig_clm_lkup.PRCS_DT_SK").isNull(), F.lit('0')).otherwise(F.col("orig_clm_lkup.PRCS_DT_SK")), 10, " ").alias("PRCS_DT_SK"),
    F.rpad(F.when(F.col("orig_clm_lkup.RCVD_DT_SK").isNull(), F.lit('0')).otherwise(F.col("orig_clm_lkup.RCVD_DT_SK")), 10, " ").alias("RCVD_DT_SK"),
    F.rpad(F.when(F.col("orig_clm_lkup.SVC_STRT_DT_SK").isNull(), F.lit('0')).otherwise(F.col("orig_clm_lkup.SVC_STRT_DT_SK")), 10, " ").alias("SVC_STRT_DT_SK"),
    F.rpad(F.when(F.col("orig_clm_lkup.SVC_END_DT_SK").isNull(), F.lit('0')).otherwise(F.col("orig_clm_lkup.SVC_END_DT_SK")), 10, " ").alias("SVC_END_DT_SK"),
    F.rpad(F.when(F.col("orig_clm_lkup.SMLR_ILNS_DT_SK").isNull(), F.lit('0')).otherwise(F.col("orig_clm_lkup.SMLR_ILNS_DT_SK")), 10, " ").alias("SMLR_ILNS_DT_SK"),
    F.rpad(F.when(F.col("orig_clm_lkup.STTUS_DT_SK").isNull(), F.lit('0')).otherwise(F.col("orig_clm_lkup.STTUS_DT_SK")), 10, " ").alias("STTUS_DT_SK"),
    F.rpad(F.when(F.col("orig_clm_lkup.WORK_UNABLE_BEG_DT_SK").isNull(), F.lit('0')).otherwise(F.col("orig_clm_lkup.WORK_UNABLE_BEG_DT_SK")), 10, " ").alias("WORK_UNABLE_BEG_DT_SK"),
    F.rpad(F.when(F.col("orig_clm_lkup.WORK_UNABLE_END_DT_SK").isNull(), F.lit('0')).otherwise(F.col("orig_clm_lkup.WORK_UNABLE_END_DT_SK")), 10, " ").alias("WORK_UNABLE_END_DT_SK"),
    F.when(F.col("orig_clm_lkup.ACDNT_AMT").isNull(), F.lit(0)).otherwise(F.col("orig_clm_lkup.ACDNT_AMT")).alias("ACDNT_AMT"),
    F.when(F.col("orig_clm_lkup.ACTL_PD_AMT").isNull(), F.lit(0)).otherwise(F.col("orig_clm_lkup.ACTL_PD_AMT")).alias("ACTL_PD_AMT"),
    F.when(F.col("orig_clm_lkup.ALW_AMT").isNull(), F.lit(0)).otherwise(F.col("orig_clm_lkup.ALW_AMT")).alias("ALW_AMT"),
    F.when(F.col("orig_clm_lkup.DSALW_AMT").isNull(), F.lit(0)).otherwise(F.col("orig_clm_lkup.DSALW_AMT")).alias("DSALW_AMT"),
    F.when(F.col("orig_clm_lkup.COINS_AMT").isNull(), F.lit(0)).otherwise(F.col("orig_clm_lkup.COINS_AMT")).alias("COINS_AMT"),
    F.when(F.col("orig_clm_lkup.CNSD_CHRG_AMT").isNull(), F.lit(0)).otherwise(F.col("orig_clm_lkup.CNSD_CHRG_AMT")).alias("CNSD_CHRG_AMT"),
    F.when(F.col("orig_clm_lkup.COPAY_AMT").isNull(), F.lit(0)).otherwise(F.col("orig_clm_lkup.COPAY_AMT")).alias("COPAY_AMT"),
    F.when(F.col("orig_clm_lkup.CHRG_AMT").isNull(), F.lit(0)).otherwise(F.col("orig_clm_lkup.CHRG_AMT")).alias("CHRG_AMT"),
    F.when(F.col("orig_clm_lkup.DEDCT_AMT").isNull(), F.lit(0)).otherwise(F.col("orig_clm_lkup.DEDCT_AMT")).alias("DEDCT_AMT"),
    F.when(F.col("orig_clm_lkup.PAYBL_AMT").isNull(), F.lit(0)).otherwise(F.col("orig_clm_lkup.PAYBL_AMT")).alias("PAYBL_AMT"),
    F.when(F.col("orig_clm_lkup.CLM_CT").isNull(), F.lit(0)).otherwise(F.col("orig_clm_lkup.CLM_CT")).alias("CLM_CT"),
    F.when(F.col("orig_clm_lkup.MBR_AGE").isNull(), F.lit(0)).otherwise(F.col("orig_clm_lkup.MBR_AGE")).alias("MBR_AGE"),
    F.when(F.col("orig_clm_lkup.ADJ_FROM_CLM_ID").isNull(), F.lit(' ')).otherwise(F.col("orig_clm_lkup.ADJ_FROM_CLM_ID")).alias("ADJ_FROM_CLM_ID"),
    F.when(F.col("adj_clm.CLM_ID").isNull(), F.lit(' ')).otherwise(F.col("adj_clm.CLM_ID")).alias("ADJ_TO_CLM_ID"),
    F.rpad(F.when(F.col("orig_clm_lkup.DOC_TX_ID").isNull(), F.lit(' ')).otherwise(F.col("orig_clm_lkup.DOC_TX_ID")), 18, " ").alias("DOC_TX_ID"),
    F.when(F.col("orig_clm_lkup.MCAID_RESUB_NO").isNull(), F.lit(None)).otherwise(F.col("orig_clm_lkup.MCAID_RESUB_NO")).alias("MCAID_RESUB_NO"),
    F.rpad(F.when(F.col("orig_clm_lkup.MCARE_ID").isNull(), F.lit(' ')).otherwise(F.col("orig_clm_lkup.MCARE_ID")), 12, " ").alias("MCARE_ID"),
    F.rpad(F.when(F.col("orig_clm_lkup.MBR_SFX_NO").isNull(), F.lit(' ')).otherwise(F.col("orig_clm_lkup.MBR_SFX_NO")), 2, " ").alias("MBR_SFX_NO"),
    F.when(F.col("orig_clm_lkup.PATN_ACCT_NO").isNull(), F.lit(None)).otherwise(F.col("orig_clm_lkup.PATN_ACCT_NO")).alias("PATN_ACCT_NO"),
    F.rpad(F.when(F.col("orig_clm_lkup.PAYMT_REF_ID").isNull(), F.lit(' ')).otherwise(F.col("orig_clm_lkup.PAYMT_REF_ID")), 16, " ").alias("PAYMT_REF_ID"),
    F.rpad(F.when(F.col("orig_clm_lkup.PROV_AGMNT_ID").isNull(), F.lit(' ')).otherwise(F.col("orig_clm_lkup.PROV_AGMNT_ID")), 12, " ").alias("PROV_AGMNT_ID"),
    F.when(F.col("orig_clm_lkup.RFRNG_PROV_TX").isNull(), F.lit(None)).otherwise(F.col("orig_clm_lkup.RFRNG_PROV_TX")).alias("RFRNG_PROV_TX"),
    F.rpad(F.when(F.col("orig_clm_lkup.SUB_ID").isNull(), F.lit(' ')).otherwise(F.col("orig_clm_lkup.SUB_ID")), 14, " ").alias("SUB_ID"),
    F.lit(1).alias("PCA_TYP_CD_SK"),
    F.lit(1).alias("REL_PCA_CLM_SK"),
    F.lit(1).alias("REL_BASE_CLM_SK"),
    F.lit(0).alias("REMIT_SUPRSION_AMT"),
    F.when(
        F.col("orig_clm_lkup.SUB_ID").isNull(),
        F.lit('  ')
    ).otherwise(
        F.when(
            F.length(F.trim(F.col("orig_clm_lkup.MCAID_STTUS_ID"))) == 0,
            F.lit('NA')
        ).otherwise(F.col("orig_clm_lkup.MCAID_STTUS_ID"))
    ).alias("MCAID_STTUS_ID"),
    F.when(F.col("orig_clm_lkup.PATN_PD_AMT").isNull(), F.lit(0.00)).otherwise(F.col("orig_clm_lkup.PATN_PD_AMT")).alias("PATN_PD_AMT"),
    F.col("adj_clm.CLM_SUBMT_ICD_VRSN_CD_SK").alias("CLM_SUBMT_ICD_VRSN_CD_SK")
)

# Write to CLM_REVERSAL_DRUG_ORIG_CLM.dat (CSeqFileStage)
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

# ----------------------
# Output 2: ClmMartAdj => W_WEBDM_ETL_DRVR_adj
# Constraint: adj_clm.SVC_STRT_DT_SK > '#FillDtMin#' 
#             AND IsNull(orig_clm_lkup.CLM_SK)=@FALSE
#             AND (adj_clm.SRC_SYS_CD='ARGUS' or adj_clm.SRC_SYS_CD='ESI')
# ----------------------
df_ClmMartAdj = df_clm_joined.filter(
    (F.col("adj_clm.SVC_STRT_DT_SK") > F.lit("#FillDtMin#"))
    & (F.col("orig_clm_lkup.CLM_SK").isNotNull())
    & ((F.col("adj_clm.SRC_SYS_CD") == F.lit("ARGUS")) | (F.col("adj_clm.SRC_SYS_CD") == F.lit("ESI")))
)

df_ClmMartAdj = df_ClmMartAdj.select(
    F.col("adj_clm.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("adj_clm.CLM_ID").alias("CLM_ID"),
    F.col("adj_clm.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("adj_clm.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
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

# ----------------------
# Output 3: ClmMartOrig => W_WEBDM_ETL_DRVR_orig
# Constraint: orig_clm_lkup.SVC_STRT_DT_SK > '#FillDtMin#'
#             AND IsNull(orig_clm_lkup.CLM_SK)=@FALSE
#             AND (adj_clm.SRC_SYS_CD='ARGUS' OR adj_clm.SRC_SYS_CD='ESI')
# ----------------------
df_ClmMartOrig = df_clm_joined.filter(
    (F.col("orig_clm_lkup.SVC_STRT_DT_SK") > F.lit("#FillDtMin#"))
    & (F.col("orig_clm_lkup.CLM_SK").isNotNull())
    & ((F.col("adj_clm.SRC_SYS_CD") == F.lit("ARGUS")) | (F.col("adj_clm.SRC_SYS_CD") == F.lit("ESI")))
)

df_ClmMartOrig = df_ClmMartOrig.select(
    F.when(F.col("orig_clm_lkup.SRC_SYS_CD_SK").isNull(), F.lit(0)).otherwise(F.col("orig_clm_lkup.SRC_SYS_CD_SK")).alias("SRC_SYS_CD_SK"),
    F.when(F.col("orig_clm_lkup.ORIG_CLM_ID").isNull(), F.lit(' ')).otherwise(F.col("orig_clm_lkup.ORIG_CLM_ID")).alias("CLM_ID"),
    F.col("orig_clm_lkup.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("adj_clm.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
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

# ----------------------
# Output 4: RowCount => B_CLM
# Constraint: IsNull(orig_clm_lkup.CLM_SK)=@FALSE
# ----------------------
df_RowCount = df_clm_joined.filter(F.col("orig_clm_lkup.CLM_SK").isNotNull())

df_RowCount = df_RowCount.select(
    F.when(F.col("orig_clm_lkup.SRC_SYS_CD_SK").isNull(), F.lit(0)).otherwise(F.col("orig_clm_lkup.SRC_SYS_CD_SK")).alias("SRC_SYS_CD_SK"),
    F.when(F.col("orig_clm_lkup.ORIG_CLM_ID").isNull(), F.lit(' ')).otherwise(F.col("orig_clm_lkup.ORIG_CLM_ID")).alias("CLM_ID"),
    F.col("svStatusCd").alias("CLM_STTUS_CD_SK"),
    F.when(F.col("orig_clm_lkup.CLM_CAT_CD_SK").isNull(), F.lit(0)).otherwise(F.col("orig_clm_lkup.CLM_CAT_CD_SK")).alias("CLM_CAT_CD_SK"),
    F.when(F.col("orig_clm_lkup.EXPRNC_CAT_SK").isNull(), F.lit(0)).otherwise(F.col("orig_clm_lkup.EXPRNC_CAT_SK")).alias("EXPRNC_CAT_SK"),
    F.when(F.col("orig_clm_lkup.FNCL_LOB_SK").isNull(), F.lit(0)).otherwise(F.col("orig_clm_lkup.FNCL_LOB_SK")).alias("FNCL_LOB_SK"),
    F.when(F.col("orig_clm_lkup.GRP_SK").isNull(), F.lit(0)).otherwise(F.col("orig_clm_lkup.GRP_SK")).alias("GRP_SK"),
    F.when(F.col("orig_clm_lkup.MBR_SK").isNull(), F.lit(0)).otherwise(F.col("orig_clm_lkup.MBR_SK")).alias("MBR_SK"),
    F.rpad(F.when(F.col("orig_clm_lkup.SVC_STRT_DT_SK").isNull(), F.lit('0')).otherwise(F.col("orig_clm_lkup.SVC_STRT_DT_SK")), 10, " ").alias("SVC_STRT_DT_SK"),
    F.when(F.col("orig_clm_lkup.CHRG_AMT").isNull(), F.lit(0)).otherwise(F.col("orig_clm_lkup.CHRG_AMT")).alias("CHRG_AMT"),
    F.when(F.col("orig_clm_lkup.PAYBL_AMT").isNull(), F.lit(0)).otherwise(F.col("orig_clm_lkup.PAYBL_AMT")).alias("PAYBL_AMT"),
    F.when(F.col("orig_clm_lkup.CLM_CT").isNull(), F.lit(0)).otherwise(F.col("orig_clm_lkup.CLM_CT")).alias("CLM_CT"),
    F.lit(1).alias("PCA_TYP_CD_SK")
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

# ---------------------------------------------------------------------------
# Stage: B_CLM (CSeqFileStage) - already written above to B_CLM.{Source}.dat.{RunID}
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Stage: W_WEBDM_ETL_DRVR_orig (CSeqFileStage) - already written above
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Stage: W_WEBDM_ETL_DRVR_adj (CSeqFileStage) - already written above
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Stage: CLM_REVERSAL_DRUG_ORIG_CLM (CSeqFileStage) - already written above
# ---------------------------------------------------------------------------

# AfterJobRoutine = "1" (no specific routine calls shown in JSON beyond that). 
# End of job. All stages processed.