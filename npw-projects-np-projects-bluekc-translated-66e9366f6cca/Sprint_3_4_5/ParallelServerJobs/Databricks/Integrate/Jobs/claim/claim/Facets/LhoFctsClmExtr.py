# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   Pulls data from CMC_CLCL_CLAIM to a landing file for the IDS
# MAGIC   Creates an output file in the verified directory for input to the IDS transform job.
# MAGIC       
# MAGIC 33;hf_clm_exp_cat;hf_clm_mctr_type;hf_clm_mctr_spec;hf_clm_snap_lob;hf_clm_trad_pard;hf_clm_hsa_clhs;hf_clm_line_info_dntl;hf_clm_line_info_med;hf_claim_cob_med_sup;hf_cddo_ovr;hf_clm_cmc_cdsm_li_msupp;hf_clm_clcb_cl_cob_info;hf_clm_cmc_cdcb_li_cob;hf_clm_cmc_cddc_dnli_cob;hf_clm_status_audit_for_dt;hf_clm_line_info;hf_clm_chk_info;hf_clm_hosp_med_no;hf_clm_clmi_misc;hf_clm_letter_ind;hf_clm_note_ind;hf_cdml_disall;hf_cdor_ovr;hf_cddl_disall;hf_clm_micro_id;hf_clm_adj_from_to;hf_clm_rev_pca_excpt;hf_clm_remit_supr;hf_clm_remit_supr_dntl;hf_clm_adj_remit_suprsn_med;hf_clm_adj_remit_suprsn_dntl;hf_clm_lob_id;hf_clm_lobd_id2
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Oliver Nielsen     05/07/2004-   Originally Programmed
# MAGIC Oliver Nielsen     05/19/2004-   Added logic to the hash file build to add CDDL claim line summing to carry Dental Claim Line totals on the Claim
# MAGIC Oliver Nielsen     05/20/2004-   Added Claim COB Code actual information for Claims Quest.
# MAGIC Oliver Nielsen     06/18/2004-   IDS Version 1.1 and additional documentation
# MAGIC Oliver Nielsen     07/01/2004-   IDS 1.1 Implementation and added documentation.
# MAGIC SharonAndrew   07/13/2004     Changed primary key to contain the source system code
# MAGIC Ralph Tucker    10/20/2004     Added hash files and logic for disallow & writeoffs
# MAGIC Ralph Tucker    10/27/2004    Facets 4.11 changes made.
# MAGIC Oliver Nielsen    11/01/2004    Added NullOptAmt Logic to COB amt
# MAGIC Sharon Andrew 11/10/2004   issue 2253 - gave default values to indicators Clm_Cobra_clm_in, Clm_Mcare_Asg_In, Clm_OOA_In, Clm_Pca_Audit_in, and Clm_Prod_OOA, In
# MAGIC Sharon Andrew  12/01/2004     Added the claim status code to the hash file hf_clm_stts.  This hash file also used by FctsClnLnTrns, FctsClnTrns and they read the claim status code.
# MAGIC                                                                    For claims that had dental clain lines only, the finalized dispositon code was getting set to UNK cause there was no status code to use for setting it.
# MAGIC Sharon Andrew  2/01/2005      IDS 3.0 - removed ITS and COB fields.  
# MAGIC                                                                  Renamed CLM_ALPHA_PFX_CD_SK to ALPHA_PFX_SK
# MAGIC                                                                  Renamed ACDNT_ID to CLM_ACDNT_CD_SK
# MAGIC Brent Leland      02/11/2005    Move Facets SQL to FctsClmExtr job to conform to architecture.
# MAGIC SAndrew            02/14/2005   Cleaned up after Brent, as usual.   Added hash file to the input list.  Added BCBS_REPL database parameters.
# MAGIC                                                                 Added facets extract links  CMC_CLCB_CL_COB and  CMC_CLSM_MED_SUPP
# MAGIC                                                                Made each facets link its own hash file icon so can see the hash file name.
# MAGIC                                                                Changed the number of hash files to delete from 5 to 4 in the Input Value parameter list.
# MAGIC                                                                Noticed the parameters used for the BCBS database connections were all FAcets.  Converted to BCBS.
# MAGIC Brent Leland       03/10/2005     Changed logic for STTUS_DT to use actual date instead of current date.
# MAGIC Sharon Andrew   3/22/2005      Product value changed if claim status code is 99, 81, 15 or 11 which is in the transform that checks the status is NA, ClosedDeleted, or Active.  Right after Setup_CRF
# MAGIC                                                              Tightened logix for network code.  
# MAGIC  BJ Luce             03/22/2005      select all provider types, not just for for professionals, in MCTR_TYPE, include provider spec for subtyp D in MCTR_SPEC 
# MAGIC Landon Hall        3/23/2005      Added to logic for ITS Host processing.  May be identified by the use of the ITSHost stage variable.
# MAGIC Sharon Andrew   4/4/2005        Added claim reversal logix.    A09- A08 - A02
# MAGIC                                                              Changed stage rule for claim cob cd in SETUP_CRF
# MAGIC BJ Luce              4/6/2005          Included 93, 97 and 82 into the product logic added by Sharon on 3/22
# MAGIC Hall                     04/15/2005      Added attributes to clmi hash file and added the extract of clio hash file to support ITS Host processing in Facets
# MAGIC BJ Luce             04/27/2005      remove ; from build of hf_clm_line_info because dental lines were not being summed.
# MAGIC SAndrew           06/14/2005      added dental cob extract into hash file hf_clm_cmc_cddc_dnli_cob.  used in transform when calculating the cob_type field.
# MAGIC SAndrew           06/14/2005    Changed the reversal logix when building the paid and status date to look at the adjusting to claim if either 02 or 91.   used to look at it only if an 02
# MAGIC Ralph Tucker    6/24/2005      Changed the ETL for CLM_ACDNT_CD.  If spaces then 'NA'   
# MAGIC                                                              Changed the ETL for CLM_ACDNT_ST.  If spaces then 'NA'
# MAGIC Brent Leland      07/27/2005      Added index indicator to SQL for First Pass query.
# MAGIC SAndrew            08/03/2005    COB TYPE code changes for ITS.   Test for spaces
# MAGIC Brent Leland      09/15/2005    Changed hash file name hf_clm_cob_info to hf_clm_clcb_cl_cob_info.  FctsClmCOBExtr uses hf_clm_cob_info.
# MAGIC Steph Goddard  09/16/2005      Deleted hash file for actual paid amount from BCBS_Remit History.  Now created in BCBSClmRemitHistExtr.
# MAGIC SAndrew            12/5/2005        Changed the source of the IDS Claim's field Status Date to be from the CLST STATUS table and not the claims last activity date to account for claim status 99.
# MAGIC                                                             Added hash file to the 7TH file deleted via HASH.CLEAR      hf_clm_status_audit_for_dt
# MAGIC SAndrew           12/162005       Changed the source of the IDS Claim's field Status Date to be the max of ( max date  CLST STATUS  or CLCL last activity date) 
# MAGIC SAndrew           12/19/2005       Changed last activity to write out a date and not a timestamp
# MAGIC Steph Goddard  02/27/2006    Changed for sequencer - WHEW!  Created local containers for most of the logic to clean it  up.  Copied log from transform and inserted in date order
# MAGIC BJ Luce                                    correct lookup from adj_from in claim reversal logic.
# MAGIC BJ Luce            03/20/2006     add hf_clm_nasco_dup_bypass, identifies claims that are nasco dups. If the claim is on the file, a row is not generated for it in IDS. However, an R row will be build for it if the status if '91'
# MAGIC Steph Goddard 03/29/2006     changed logic for CLM_ACTV_BCBS_PLAN_CD_SK and CLM_SUBMITTING_BCBS_PLN_CD_SK since there are CLMI rows for non-ITS claims
# MAGIC Steph Goddard 03/31/2006     retrieved PRPR_ENTITY to help determine provider type code in foreign key job for Facets
# MAGIC                                                           changed calculation for disallow amount to just add up line items per Michelle Lang
# MAGIC                                                            changed default for experience category to NA instead of UNK
# MAGIC Steph Goddard  04/19/2006   added lookup to BCBS table GRP_PROD_BCBSA_PLN and deleted lookup from flat file to get alpha prefix
# MAGIC Steph Goddard  05/04/2006   deleted lookup for first pass indicator and defaulted to X
# MAGIC Steph Goddard  05/08/2006   added a hash file of denied/rejected claims to go to clm_prov extract
# MAGIC                                                           check claim category code (clcl_cur_sts) for null or blank before moving - null or blank should be UNK
# MAGIC                                                           changed default for clm_cob_cd to 'NA' instead of 'UNK'
# MAGIC                                                           set default for Subscriber BCBS plan code to 'NA' if null or spaces
# MAGIC Steph Goddard 05/11/2006    moved split of hf_clm_sts to FctsDntlClmLnExtr - file updated here.  DeniedRej hash file also used in ClmProvExtr
# MAGIC Steph Goddard 10/02/2006    Trimmed claim number on lookup to hash file from claim remit history
# MAGIC Steph Goddard 10/12/2006    Checked sub id field in clmi record for nulls in cols 4-17.  Logic needs to be reviewed, but this will keep the load from abending when the claim id is not there.
# MAGIC                                                           production support issue - load has abended two days in a row because of a bad claim in Facets  Altiris ticket 51295.    In container conHashFiles; stage variables ITSSBAct and ITSSBiD
# MAGIC Ralph Tucker    11/07/2006   Added new fields to the end of the table:  PCA_TYP_CD, REL_PCA_CLM_SK, CLCL_MICRO_ID, CLM_UPDT_SW
# MAGIC Sanderw           12/08/2006   Project 1576  - Reversal logix added for new status codes 89 and  and 99                                                        
# MAGIC Ralph Tucker    01/30/2007   Added hashfile (hf_clm_pca_clms) to popuate REL_PCA_CLM_ID  on adjustments only.
# MAGIC SAndrew            04/02/2007   TT 42:   Fncl_Lob is NA when it shouldn't be.   To fix, in SNAP_LOB extract, added criteria that prod componet term dates can be equal to or greater than the claim service date instead of just greater than.                                  code walkthru by Steph Goddard  4/5/07
# MAGIC Naren                 06/27/2007    Put a cap on the MBR_AGE field for Members with more than the age of 90.The age is set to 90 if it is greater than 90.
# MAGIC Ralph Tucker     06/27/2007  Added logic for ARGUS to handle PCA claims.
# MAGIC 
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Oliver Nielsen          08/15/2007       Balancing              Added Snapshot extract for balancing                                          devlIDS30                     Steph Goddard          8/30/07 
# MAGIC SAndrew                  11/14/2007      TTR4492             Added MED and DEN to SNAP_LOB ODBC sql criteria                 devlIDS30                     Steph Goddard          11/14/07
# MAGIC Bhoomi D                 03/21/2008      3255                    Modified rules for CLM_SUBMITTING_BCBS_PLN_CD_SK          devlIDSCur                    Steph Goddard          03/31/2008
# MAGIC                                                                                       and CLM_SUB_BCBS_PLN_CD_SK
# MAGIC SAndrew                 04/20/2008      3255 ITSHome   - changed rules for CLM_INTER_PLN_PGM_CD in container          devlIDScur                     Steph Goddard          04/24/2008
# MAGIC                                                                                           contHashFiles   
# MAGIC SAndrew                 04/28/2008      3255 ITSHome  - rules changed for                                                                              devlIDScur                     
# MAGIC                                                                                          clm_actv_bcbs_pln_cd_sk, 
# MAGIC                                                                                          clm_inter_pln_pgm_cd_sk 
# MAGIC                                                                                          clm_input_src_cd_sk, 
# MAGIC                                                                                          clm_submitting_bcbs_pln_cd_sk, 
# MAGIC                                                                                          clm_sub_bcbs_pln_cd_sk
# MAGIC                                                                                         Added first pass ind. lookup into IDS
# MAGIC O. Nielsen                07/25/2008        Facets 4.5.1         Changed logic in Shared container (contHashFiles) for the 
# MAGIC                                                                                          ClaimSubTypCd Stage variable to account for 2
# MAGIC                                                                                           digit CLHP_FAC_TYPE                                                             devlIDSnew                     Steph Goddard        08/20/2008
# MAGIC Bhoomi Dasari         2008-08-12      3567(Primary Key)    Changed primary key process and added new container            devlIDS                           Steph Goddard          08/14/2008
# MAGIC Bhoomi D                2009-06-17      4202                         Changing logic for PCA_TYP_CD, REL_BASE_CLM_SK           devlIDSnew                     Steph Goddard         06/23/2009
# MAGIC                                                                                         and REL_PCA_CLM_SK. Created new routine to get rid of 
# MAGIC                                                                                         code in PCA_TYP_CD (PCA.TYP.CD.ROUTINE)
# MAGIC                                                                                         Changed hash file name hf_clm_cob_med_sup to 
# MAGIC                                                                                         hf_claim_cob_med_sup as it was also used in FctcClmCOBExtr
# MAGIC Ralph Tucker          2009-08-12      15 - Prod Sup           Added new field: REMIT_SUPRSION_AMT                              devlIDS                            Steph Goddard         08/21/2009
# MAGIC Kalyan Neelam         2010-02-09        4278                     Added two new fields - MCAID_STTUS_ID,                  
# MAGIC                                                                                        PATN_PD_AMT                                                                          IntegrateCurDevl
# MAGIC Judy Reynolds         2011-03-25      TTR_1086              Modified to add UpCase check to stage variables that                IntegrateWrhsDevl
# MAGIC                                                                                        check the 6th position of the claim number for uppercase
# MAGIC                                                                                        values in all_the_rules_here transform
# MAGIC                                                                                        Also corrected name on hashfile clear from hf_clm_lob_id
# MAGIC                                                                                        to hf_clm_lobd_id to prevent warning error.  Also added UPCASE 
# MAGIC                                                                                        check to logic in ClmOUT on CLM_IPP_CD
# MAGIC Ralph Tucker          2012-05-01      4896                       Added CMC_CLMF_MULT_FUNC for ICD9/ICD10 selection      IntegrateNewDevl                     SAndrew       2012-05-16
# MAGIC 
# MAGIC Manasa Andru         2014-04-10      TFS - 8502             Added DEN2 to the extract sql in the SNAP_LOB O/P link         IntegrateNewDevl                    Kalyan Neelam     2014-04-10
# MAGIC                                                                                                of Facets_Claim
# MAGIC Abhiram Dasarathy	2014-11-17       TFS - 9540	      Added DEN2 to the extract sql in the PDBL_EXP_CAT output    IntegrateNewDevl                    Kalyan Neelam     2014-11-21
# MAGIC                                                                                       link of Facets_Claim
# MAGIC Manasa Andru        2016-08-25       TFS - 12286          Updated the business rule for the field                                           IntegrateDev2                         Jag Yelavarthi       2016-09-05
# MAGIC                                                                                      CLM_INTER_PLN_PGM_CD_SK(CLM_IPP_CD) in the
# MAGIC                                                                                      all_the_rules_here transformer in the local container - contHashFiles 
# MAGIC                                                                                      as we have new pre-priced indicator 'E' for the Medicare Advantage Claims.
# MAGIC Mohan Karnati       06/13/2019    ADO-73034               Adding CLM_TXNMY_CD filed in all_the_rules_herestage and    
# MAGIC                                                                                         passing till Output stage                                                               IntegrateDev1                        Kalyan Neelam       2019-07-01
# MAGIC Mohan Karnati       08/12/2019    ADO-73034               Adding CLM_TXNMY_CD filed in all_the_rules_herestage and    
# MAGIC                                                                                         passing till Output stage and left joining the table                        IntegrateDev1                         Abhiram Dasarathy 08/12/2019
# MAGIC                                                                                          and modified the SQL
# MAGIC Mohan Karnati       08/13/2019    ADO-73034               Modified the source extraction part to    
# MAGIC                                                                                         Left JOIN #$FacetsOwner#.CMC_CDML_CL_LINE G               IntegrateDev2                         Hugh Sisson            2019-08-13                    
# MAGIC Mohan Karnati       08/14/2019    ADO-73034               Adding G.CDML_SEQ_NO is null in the source extraction sql    IntegrateDev1                          Hugh Sisson            2019-08-14
# MAGIC                                                                                        in the where condition
# MAGIC Giri  Mallavaram    2020-04-06         6131 PBM Changes       Added SPEC_DRUG_IN to be in sync with changes to DrugClmPKey Container        IntegrateDev2          Kalyan Neelam     2020-04-07
# MAGIC 
# MAGIC 
# MAGIC Goutham K            2020-11-17        US-317532               Changed Tranformation/caliculation for ACTL_PD_AMT                                                      IntegrateDev2       Kalyan Neelam     2020-11-23
# MAGIC                                                                                          for Medical and Dental Claims
# MAGIC 
# MAGIC 
# MAGIC Emran.Mohammad  2020-12-21        US-318408             Change of source  extraction query to update the ITShome case                                         IntegrateDev2       Kalyan Neelam     2020-12-29
# MAGIC                                                                                         codition to update ITSHOME condition as either to 'T' or 'NA' 
# MAGIC                                                                                                                 Cloumn Name:CLM_IPP_CD
# MAGIC                                                                                                                            
# MAGIC 
# MAGIC GouthamK              2021-07-13           US-386524           Made Facets Upgrade REAS_CD changes to point the job compatable to run                    IntegrateDev2       Jeyaprasanna       2021-07-14
# MAGIC                                                                                          Pointing to SYBATCH (Facets)
# MAGIC 
# MAGIC 
# MAGIC Goutham K            2021-07-28            US-412575           Logic change in the Reversal transformer for Field ADJ_FROM_CLM if the CLM_ID          IntegrateDev2        Reddy Sanam       2021-07-28
# MAGIC                                                                                          and ADJ_FROM_CLM are Lumeris and status is 81
# MAGIC 
# MAGIC 
# MAGIC Goutham K            2022-01-17            US-477638          LHO Claims - FINAL DISP CODE CORRECTION Updates FnlDspCode Stage Variable       IntegrateDev2       Reddy Sanam        2022-01-18
# MAGIC                                                                                         In ContTransform container 
# MAGIC 
# MAGIC Prabhu ES            2022-03-29             S2S                     MSSQL ODBC conn params added                                                                                      IntegrateDev5

# MAGIC All hash files match back to claim data based on CLM_ID
# MAGIC Balancing snapshot of source table
# MAGIC Apply business logic
# MAGIC Strip Carriage Return, Line Feed, and  Tab
# MAGIC Extract Facets Claim Data
# MAGIC Assign primary surrogate key
# MAGIC Writing Sequential File to ../key
# MAGIC This container is used in:
# MAGIC ArgusClmExtr
# MAGIC PCSClmExtr
# MAGIC PseudoClmPkey
# MAGIC FctsClmExtr
# MAGIC NascoClmTrns
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC hf_clm_adj_from is needed to provide the rule for creating the adjusted from claim id in the fctsClmTrns job.  if the status of the adjusted claim is 97 then a normal claim id is used for the adjusted claim id else a claim id R is used for the adjusted claim id.
# MAGIC hash file built in FctsClmDriverBuild
# MAGIC 
# MAGIC bypass claims on hf_clm_nasco_dup_bypass, do not build a regular claim row.
# MAGIC Hash files created in FctsDntlClmLnExtr
# MAGIC Do Not Clear the hf_clm_pca_clms hashfFile
# MAGIC Lookup first pass indictor in IDS to cover hit list claims
# MAGIC 
# MAGIC Uses W_FCTS_RCRD_DEL table
# MAGIC All hash files match back to claim data based on CLM_ID
# MAGIC Hash file hf_clm_sts created in FctsClmLnMedExtr and updated in FctsClmLnDntlExtr
# MAGIC Do Not Clear the hf_first_pass_ind hashfFile.  This hash file is created in another process, FctsFirstPassIndCntl and is used by the nightly Facets Claim program
# MAGIC Final disposition codes are linked by CLM_ID and loaded into has files in the FctsClmLnDispCd and FctsClmLnDntlExtr jobs, which run in the Pre-requisite step.
# MAGIC Collect rows after Disposition code has been assigned.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Parameters (including special handling for known database owners)
BCBSOwner = get_widget_value("BCBSOwner","")
bcbs_secret_name = get_widget_value("bcbs_secret_name","")
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
LhoFacetsStgOwner = get_widget_value("LhoFacetsStgOwner","")
lhofacetsstg_secret_name = get_widget_value("lhofacetsstg_secret_name","")
DriverTable = get_widget_value("DriverTable","")
AdjFromTable = get_widget_value("AdjFromTable","")
LowSvcDate = get_widget_value("LowSvcDate","")
CurrRunCycle = get_widget_value("CurrRunCycle","")
RunID = get_widget_value("RunID","")
SrcSysCdSk = get_widget_value("SrcSysCdSk","")
CurrentDate = get_widget_value("CurrentDate","")
LhoFacetsStgAcct = get_widget_value("LhoFacetsStgAcct","")
LhoFacetsStgPW = get_widget_value("LhoFacetsStgPW","")
SrcSysCd = get_widget_value("SrcSysCd","")
LhoFacetsStgDSN = get_widget_value("LhoFacetsStgDSN","")

# Obtain DB config for LhoFacetsStgOwner-based connection
jdbc_url_lhofacetsstg, jdbc_props_lhofacetsstg = get_db_config(lhofacetsstg_secret_name)

# Read from ODBCConnector: Facets_Claim - multiple output links

extract_query_Claims = f"""
SELECT A.CLCL_ID, 
A.MEME_CK, A.GRGR_CK, A.SBSB_CK, A.SGSG_CK, A.CLCL_CL_TYPE, A.CLCL_CL_SUB_TYPE, A.CLCL_PRE_PRICE_IND, A.CLCL_CUR_STS,
A.CLST_SEQ_NO, A.CLCL_SITE, A.CLCL_LAST_ACT_DTM, A.CLCL_INPUT_DT, A.CLCL_RECD_DT, A.CLCL_ACPT_DTM, A.CLCL_PAID_DT,
A.CLCL_NEXT_REV_DT, A.CLCL_LOW_SVC_DT, A.CLCL_HIGH_SVC_DT, A.CLCL_ID_ADJ_TO, A.CLCL_ID_ADJ_FROM, A.CSPD_CAT, A.PZAP_ID,
A.CSCS_ID, A.CSPI_ID, A.PDPD_ID, A.MEPE_FI, A.MEPE_PLAN_ENTRY_DT, A.CLCL_COBRA_IND, A.CLCL_ME_AGE, A.MEME_SEX,
A.MEME_RECORD_NO, A.MEME_HICN, A.NWPE_PFX, A.NWPR_PFX, A.NWCR_PFX, A.PDBC_PFX_NPPR, A.PDBC_PFX_SEDF, A.PRAC_PFX,
A.PCAG_PFX, A.NWNW_ID, A.AGAG_ID, A.CLCL_MED_ASSGN_IND, A.CLCL_PAY_PR_IND, A.CLCL_PAYEE_PR_ID, A.CLCL_REL_INFO_IND,
A.CLCL_OTHER_BN_IND, A.CLCL_ACD_IND,
A.CLCL_ACD_STATE, A.CLCL_ACD_DT, A.CLCL_ACD_AMT, A.CLCL_CURR_ILL_DT, A.CLCL_SIMI_ILL_DT, A.PRPR_ID, A.CLCL_PR_SS_EIN_IND,
A.CLCL_NTWK_IND, A.CLCL_PCP_IND, A.CLCL_PRPR_ID_PCP, A.CLCL_PRPR_ID_REF, A.CLCL_PA_ACCT_NO, A.CLCL_EXT_AUTH_NO,
A.CLCL_PA_PAID_AMT, A.CLCL_TOT_CHG, A.CLCL_TOT_PAYABLE, A.CLCL_DRAG_OR_IND, A.CLCL_DRAG_DT, A.CLCL_INPUT_METH,
A.CLCL_AIAI_EOB_IND, A.CLCL_EOB_EXCD_ID, A.CLCL_BATCH_ID, A.CLCL_BATCH_ACTION, A.CLCL_CE_IND, A.CLCL_CAP_IND,
A.CLCL_AG_SOURCE, A.PDDS_PROD_TYPE, A.PDDS_MCTR_BCAT, A.CLCL_MICRO_ID, A.CLCL_UNABL_FROM_DT, A.CLCL_UNABL_TO_DT,
A.CLCL_RELHP_FROM_DT, A.CLCL_RELHP_TO_DT, A.CLCL_OUT_LAB_IND, A.CLCL_MECD_RESUB_NO, A.PRAD_TYPE, A.CLCL_PCA_IND,
A.CLCL_OOA_IND, A.CLCL_EXT_REF_IND, A.CLCL_RAD_ENC_IND, A.CLCL_REC_ENC_IND, A.CLCL_COB_EOB_IND, A.CLCL_LOCK_TOKEN,
A.ATXR_SOURCE_ID, 
C.GRGR_ID, 
D.SGSG_ID, 
E.MEME_SFX, 
F.SBSB_ID,CLCL_HSA_IND,G.SESE_ID,
G.CDML_SEQ_NO,
G.SESE_RULE,
H.CLPP_TAXONOMY_CD,
I.CLMF_INPUT_TXNM_CD,
case when SUBSTRING(A.CLCL_ID, 6, 1) = 'H' then 1 else 0 end as TAXONOMY_INDICATOR
FROM {LhoFacetsStgOwner}.CMC_CLCL_CLAIM A  
INNER JOIN tempdb..{DriverTable} B 
  ON A.CLCL_ID=B.CLM_ID
LEFT JOIN {LhoFacetsStgOwner}.CMC_GRGR_GROUP C
  ON A.GRGR_CK = C.GRGR_CK
LEFT JOIN {LhoFacetsStgOwner}.CMC_SGSG_SUB_GROUP D 
  ON A.SGSG_CK = D.SGSG_CK
LEFT JOIN {LhoFacetsStgOwner}.CMC_MEME_MEMBER E 
  ON A.MEME_CK=E.MEME_CK
LEFT JOIN {LhoFacetsStgOwner}.CMC_SBSB_SUBSC F
  ON A.SBSB_CK=F.SBSB_CK
LEFT JOIN {LhoFacetsStgOwner}.CMC_CDML_CL_LINE G 
  ON A.CLCL_ID = G.CLCL_ID
LEFT JOIN {LhoFacetsStgOwner}.CMC_CLPP_ITS_PROV H
  ON A.CLCL_ID=H.CLCL_ID
LEFT JOIN {LhoFacetsStgOwner}.CMC_CLMF_MULT_FUNC I
  ON A.CLCL_ID=I.CLCL_ID
WHERE 
(G.CDML_SEQ_NO = 1 or G.CDML_SEQ_NO is null)
"""

df_Claims = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_lhofacetsstg)
    .options(**jdbc_props_lhofacetsstg)
    .option("query", extract_query_Claims)
    .load()
)

extract_query_PDBL_EXP_CAT = f"""
SELECT CLAIM.CLCL_ID,
       BILL.PDBL_EXP_CAT
FROM {LhoFacetsStgOwner}.CMC_CLCL_CLAIM CLAIM INNER JOIN (
  {LhoFacetsStgOwner}.CMC_PDBC_PROD_COMP COMP 
  INNER JOIN {LhoFacetsStgOwner}.CMC_PDBL_PROD_BILL BILL 
    ON COMP.PDBC_PFX = BILL.PDBC_PFX
) ON CLAIM.PDPD_ID = COMP.PDPD_ID, tempdb..{DriverTable} TMP
WHERE CLAIM.CLCL_ID= TMP.CLM_ID
  AND BILL.PDBL_ID In ('MED','MED1','DEN','DEN1','DEN2')
  AND BILL.PDBL_EXP_CAT Not In (' ','')
  AND COMP.PDBC_TYPE='PDBL'
  AND COMP.PDBC_EFF_DT<= CLAIM.CLCL_LOW_SVC_DT
  AND COMP.PDBC_TERM_DT>= CLAIM.CLCL_LOW_SVC_DT
  AND BILL.PDBL_EFF_DT<= CLAIM.CLCL_LOW_SVC_DT
  AND BILL.PDBL_TERM_DT>= CLAIM.CLCL_LOW_SVC_DT
"""

df_PDBL_EXP_CAT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_lhofacetsstg)
    .options(**jdbc_props_lhofacetsstg)
    .option("query", extract_query_PDBL_EXP_CAT)
    .load()
)

extract_query_MCTR_TYPE = f"""
SELECT 
clm.CLCL_ID,
prov.PRPR_MCTR_TYPE,
prov.PRPR_ENTITY
FROM  tempdb..{DriverTable} TMP
      INNER JOIN  {LhoFacetsStgOwner}.CMC_CLCL_CLAIM clm
       ON  TMP.CLM_ID = clm.CLCL_ID
          INNER JOIN {LhoFacetsStgOwner}.CMC_PRPR_PROV prov
          ON clm.PRPR_ID = prov.PRPR_ID
"""

df_MCTR_TYPE = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_lhofacetsstg)
    .options(**jdbc_props_lhofacetsstg)
    .option("query", extract_query_MCTR_TYPE)
    .load()
)

extract_query_MCTR_SPEC = f"""
SELECT  CLAIM.CLCL_ID,
        PROV.PRCF_MCTR_SPEC
FROM tempdb..{DriverTable} TMP,
     {LhoFacetsStgOwner}.CMC_CLCL_CLAIM CLAIM,
     {LhoFacetsStgOwner}.CMC_PRPR_PROV PROV
WHERE TMP.CLM_ID = CLAIM.CLCL_ID
  AND CLAIM.CLCL_CL_SUB_TYPE in ('M','D')
  AND CLAIM.PRPR_ID = PROV.PRPR_ID
"""

df_MCTR_SPEC = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_lhofacetsstg)
    .options(**jdbc_props_lhofacetsstg)
    .option("query", extract_query_MCTR_SPEC)
    .load()
)

extract_query_SNAP_LOB = f"""
SELECT CLAIM.CLCL_ID,
       BILL.PDBL_ACCT_CAT
FROM {LhoFacetsStgOwner}.CMC_CLCL_CLAIM CLAIM 
     INNER JOIN (
       {LhoFacetsStgOwner}.CMC_PDBC_PROD_COMP COMP 
       INNER JOIN {LhoFacetsStgOwner}.CMC_PDBL_PROD_BILL BILL 
         ON COMP.PDBC_PFX = BILL.PDBC_PFX
     ) ON CLAIM.PDPD_ID = COMP.PDPD_ID, tempdb..{DriverTable} TMP
WHERE COMP.PDBC_TYPE = 'PDBL'
  AND COMP.PDBC_EFF_DT <= CLAIM.CLCL_LOW_SVC_DT
  AND COMP.PDBC_TERM_DT >= CLAIM.CLCL_LOW_SVC_DT
  AND BILL.PDBL_ID In ('MED1','DEN1', 'MED', 'DEN', 'DEN2')
  AND BILL.PDBL_EFF_DT <= CLAIM.CLCL_LOW_SVC_DT
  AND BILL.PDBL_TERM_DT >= CLAIM.CLCL_LOW_SVC_DT
  AND CLAIM.CLCL_ID= TMP.CLM_ID
"""

df_SNAP_LOB = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_lhofacetsstg)
    .options(**jdbc_props_lhofacetsstg)
    .option("query", extract_query_SNAP_LOB)
    .load()
)

extract_query_TRAD_PARD = f"""
SELECT CLED.CLCL_ID,
       CLED.CLED_TRAD_PARTNER
FROM {LhoFacetsStgOwner}.CMC_CLED_EDI_DATA CLED, 
     tempdb..{DriverTable} TMP
WHERE CLED.CLCL_ID= TMP.CLM_ID
"""

df_TRAD_PARD = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_lhofacetsstg)
    .options(**jdbc_props_lhofacetsstg)
    .option("query", extract_query_TRAD_PARD)
    .load()
)

extract_query_CLHS = f"""
Select CLCL.CLCL_ID,
       CLHS.MEME_CK
FROM {LhoFacetsStgOwner}.CMC_CLCL_CLAIM CLCL,
     {LhoFacetsStgOwner}.CMC_CLHS_HSA_CLAIM CLHS,
     tempdb..{DriverTable} DRVR
WHERE CLCL.CLCL_ID=DRVR.CLM_ID 
  and CLCL.CLCL_ID = CLHS.CLCL_ID
"""

df_CLHS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_lhofacetsstg)
    .options(**jdbc_props_lhofacetsstg)
    .option("query", extract_query_CLHS)
    .load()
)

extract_query_REMIT_SUPR_MED = f"""
select DRVR.CLM_ID CLCL_ID,
       SUM(CDML.CDML_SB_PYMT_AMT) REMIT_SB_SUPRSION_AMT,
       SUM(CDML.CDML_PR_PYMT_AMT) REMIT_PR_SUPRSION_AMT
from tempdb..{DriverTable} DRVR,
     {LhoFacetsStgOwner}.CMC_CLOR_CL_OVR CLOR,
     {LhoFacetsStgOwner}.CMC_CDML_CL_LINE CDML
WHERE DRVR.CLM_ID = CLOR.CLCL_ID
  AND DRVR.CLM_ID = CDML.CLCL_ID
  AND CLOR.CLOR_OR_ID = 'SC'
  AND (CDML.CDML_SB_PYMT_AMT > 0 or CDML.CDML_PR_PYMT_AMT > 0)
GROUP BY DRVR.CLM_ID
"""

df_REMIT_SUPR_MED = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_lhofacetsstg)
    .options(**jdbc_props_lhofacetsstg)
    .option("query", extract_query_REMIT_SUPR_MED)
    .load()
)

extract_query_REMIT_SUPR_DNTL = f"""
select DRVR.CLM_ID CLCL_ID,
       SUM(CDDL.CDDL_SB_PYMT_AMT) REMIT_SB_SUPRSION_AMT,
       SUM(CDDL.CDDL_PR_PYMT_AMT) REMIT_PR_SUPRSION_AMT
from tempdb..{DriverTable} DRVR,
     {LhoFacetsStgOwner}.CMC_CLOR_CL_OVR CLOR,
     {LhoFacetsStgOwner}.CMC_CDDL_CL_LINE CDDL
WHERE DRVR.CLM_ID = CLOR.CLCL_ID
  AND DRVR.CLM_ID = CDDL.CLCL_ID
  AND CLOR.CLOR_OR_ID = 'SC'
  AND (CDDL.CDDL_SB_PYMT_AMT > 0 or CDDL.CDDL_PR_PYMT_AMT > 0)
GROUP BY DRVR.CLM_ID
"""

df_REMIT_SUPR_DNTL = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_lhofacetsstg)
    .options(**jdbc_props_lhofacetsstg)
    .option("query", extract_query_REMIT_SUPR_DNTL)
    .load()
)

extract_query_lodLOBD_ID = f"""
SELECT RTRIM(PDPD_ID) PDPD_ID, RTRIM(LOBD_ID) LOBD_ID 
FROM {LhoFacetsStgOwner}.CMC_PDPD_PRODUCT
"""

df_lodLOBD_ID = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_lhofacetsstg)
    .options(**jdbc_props_lhofacetsstg)
    .option("query", extract_query_lodLOBD_ID)
    .load()
)

# CHashedFileStage: Lookups => scenario C for all hashed files.
df_hf_clm_exp_cat = spark.read.parquet(f"{adls_path}/hf_clm_exp_cat.parquet")
df_hf_clm_mctr_type = spark.read.parquet(f"{adls_path}/hf_clm_mctr_type.parquet")
df_hf_clm_mctr_spec = spark.read.parquet(f"{adls_path}/hf_clm_mctr_spec.parquet")
df_hf_clm_snap_lob = spark.read.parquet(f"{adls_path}/hf_clm_snap_lob.parquet")
df_hf_clm_trad_pard = spark.read.parquet(f"{adls_path}/hf_clm_trad_pard.parquet")
df_hf_clm_hsa_clhs = spark.read.parquet(f"{adls_path}/hf_clm_hsa_clhs.parquet")
df_hf_clm_remit_supr = spark.read.parquet(f"{adls_path}/hf_clm_remit_supr.parquet")
df_hf_clm_remit_supr_dntl = spark.read.parquet(f"{adls_path}/hf_clm_remit_supr_dntl.parquet")
df_hf_clm_lobd_id = spark.read.parquet(f"{adls_path}/hf_clm_lobd_id.parquet")

# MAGIC %run ../../../../../shared_containers/PrimaryKey/contHashFiles
# COMMAND ----------
# MAGIC %run ../../../../../shared_containers/PrimaryKey/contTransform
# COMMAND ----------
# MAGIC %run ../../../../../shared_containers/PrimaryKey/contReversalLogic
# COMMAND ----------
# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmPK
# COMMAND ----------

# Prepare for StripFields (CTransformerStage) logic

df_Claims_alias = df_Claims.alias("Claims")
df_exp_cat_alias = df_PDBL_EXP_CAT.alias("EXP_CAT")
df_mctr_type_alias = df_MCTR_TYPE.alias("MCTR_TYP_LU")
df_mctr_spec_alias = df_MCTR_SPEC.alias("MCTR_SPEC_LU")
df_snap_lob_alias = df_SNAP_LOB.alias("SNAP_LOB_LU")
df_trad_pard_alias = df_TRAD_PARD.alias("TRAD_PARD_LU")
df_clhs_alias = df_CLHS.alias("CLHS_LU")
df_rsupr_med_alias = df_REMIT_SUPR_MED.alias("REMIT_SUPR_MED_LU")
df_rsupr_dntl_alias = df_REMIT_SUPR_DNTL.alias("REMIT_SUPR_DNTL_LU")
df_lodLOBD_alias = df_lodLOBD_ID.alias("refLOBD_ID")

df_stripfields_joined = (
    df_Claims_alias.join(df_exp_cat_alias, F.col("Claims.CLCL_ID") == F.col("EXP_CAT.CLCL_ID"), "left")
    .join(df_mctr_type_alias, F.col("Claims.CLCL_ID") == F.col("MCTR_TYP_LU.CLCL_ID"), "left")
    .join(df_mctr_spec_alias, F.col("Claims.CLCL_ID") == F.col("MCTR_SPEC_LU.CLCL_ID"), "left")
    .join(df_snap_lob_alias, F.col("Claims.CLCL_ID") == F.col("SNAP_LOB_LU.CLCL_ID"), "left")
    .join(df_trad_pard_alias, F.col("Claims.CLCL_ID") == F.col("TRAD_PARD_LU.CLCL_ID"), "left")
    .join(df_clhs_alias, F.col("Claims.CLCL_ID") == F.col("CLHS_LU.CLCL_ID"), "left")
    .join(df_rsupr_med_alias, F.col("Claims.CLCL_ID") == F.col("REMIT_SUPR_MED_LU.CLCL_ID"), "left")
    .join(df_rsupr_dntl_alias, F.col("Claims.CLCL_ID") == F.col("REMIT_SUPR_DNTL_LU.CLCL_ID"), "left")
    .join(df_lodLOBD_alias, F.col("Claims.PDPD_ID") == F.col("refLOBD_ID.PDPD_ID"), "left")
)

svLowSvcDt_col = F.date_format(F.col("Claims.CLCL_LOW_SVC_DT"), "yyyy-MM-dd")
svhsaclaim_col = F.when(F.col("CLHS_LU.MEME_CK").isNotNull(), "Y").otherwise("N")

df_stripfields_enriched = df_stripfields_joined.withColumn("svLowSvcDt", svLowSvcDt_col)\
    .withColumn("svhsaclaim", svhsaclaim_col)

# Build the output link "Extract" columns (Constraint: Len(Trim(Claims.CLCL_ID)) > 0)
cond_extract = (F.length(trim(F.col("Claims.CLCL_ID"))) > 0)

df_Extract_unordered = df_stripfields_enriched.select(
    F.col("Claims.CLCL_ID").alias("CLCL_ID"),
    F.col("Claims.MEME_CK").alias("MEME_CK"),
    F.col("Claims.GRGR_CK").alias("GRGR_CK"),
    F.col("Claims.SBSB_CK").alias("SBSB_CK"),
    F.col("Claims.SGSG_CK").alias("SGSG_CK"),
    F.col("Claims.CLCL_CL_TYPE").alias("CLCL_CL_TYPE"),
    F.col("Claims.CLCL_CL_SUB_TYPE").alias("CLCL_CL_SUB_TYPE"),
    F.col("Claims.CLCL_PRE_PRICE_IND").alias("CLCL_PRE_PRICE_IND"),
    F.col("Claims.CLCL_CUR_STS").alias("CLCL_CUR_STS"),
    F.col("Claims.CLST_SEQ_NO").alias("CLST_SEQ_NO"),
    F.col("Claims.CLCL_SITE").alias("CLCL_SITE"),
    F.col("Claims.CLCL_LAST_ACT_DTM").alias("CLCL_LAST_ACT_DT"),
    F.col("Claims.CLCL_INPUT_DT").alias("CLCL_INPUT_DT"),
    F.col("Claims.CLCL_RECD_DT").alias("CLCL_RECD_DT"),
    F.col("Claims.CLCL_ACPT_DTM").alias("CLCL_ACPT_DTM"),
    F.col("Claims.CLCL_PAID_DT").alias("CLCL_PAID_DT"),
    F.col("Claims.CLCL_NEXT_REV_DT").alias("CLCL_NEXT_REV_DT"),
    F.col("Claims.CLCL_LOW_SVC_DT").alias("CLCL_LOW_SVC_DT"),
    F.col("Claims.CLCL_HIGH_SVC_DT").alias("CLCL_HIGH_SVC_DT"),
    F.col("Claims.CLCL_ID_ADJ_TO").alias("CLCL_ID_ADJ_TO"),
    F.col("Claims.CLCL_ID_ADJ_FROM").alias("CLCL_ID_ADJ_FROM"),
    F.col("Claims.CSPD_CAT").alias("CSPD_CAT"),
    F.col("Claims.PZAP_ID").alias("PZAP_ID"),
    F.col("Claims.CSCS_ID").alias("CSCS_ID"),
    F.col("Claims.CSPI_ID").alias("CSPI_ID"),
    F.col("Claims.PDPD_ID").alias("PDPD_ID"),
    F.col("Claims.MEPE_FI").alias("MEPE_FI"),
    F.col("Claims.MEPE_PLAN_ENTRY_DT").alias("MEPE_PLAN_ENTRY_DT"),
    F.col("Claims.CLCL_COBRA_IND").alias("CLCL_COBRA_IND"),
    F.col("Claims.CLCL_ME_AGE").alias("CLCL_ME_AGE"),
    F.col("Claims.MEME_SEX").alias("MEME_SEX"),
    F.col("Claims.MEME_RECORD_NO").alias("MEME_RECORD_NO"),
    F.col("Claims.MEME_HICN").alias("MEME_HICN"),
    F.col("Claims.NWPE_PFX").alias("NWPE_PFX"),
    F.col("Claims.NWPR_PFX").alias("NWPR_PFX"),
    F.col("Claims.NWCR_PFX").alias("NWCR_PFX"),
    F.col("Claims.PDBC_PFX_NPPR").alias("PDBC_PFX_NPPR"),
    F.col("Claims.PDBC_PFX_SEDF").alias("PDBC_PFX_SEDF"),
    F.col("Claims.PRAC_PFX").alias("PRAC_PFX"),
    F.col("Claims.PCAG_PFX").alias("PCAG_PFX"),
    F.col("Claims.NWNW_ID").alias("NWNW_ID"),
    F.col("Claims.AGAG_ID").alias("AGAG_ID"),
    F.col("Claims.CLCL_MED_ASSGN_IND").alias("CLCL_MED_ASSIGN_IND"),
    F.col("Claims.CLCL_PAY_PR_IND").alias("CLCL_PAY_PR_IND"),
    F.expr("REGEXP_REPLACE(COALESCE(Claims.CLCL_PAYEE_PR_ID,''), '[\\x0D\\x0A\\x09]', '')").alias("CLCL_PAYEE_PR_ID"),
    F.col("Claims.CLCL_REL_INFO_IND").alias("CLCL_REL_INFO_IND"),
    F.col("Claims.CLCL_OTHER_BN_IND").alias("CLCL_OTHER_BN_IND"),
    F.col("Claims.CLCL_ACD_IND").alias("CLCL_ACD_IND"),
    F.col("Claims.CLCL_ACD_STATE").alias("CLCL_ACD_STATE"),
    F.col("Claims.CLCL_ACD_DT").alias("CLCL_ACD_DT"),
    F.col("Claims.CLCL_ACD_AMT").alias("CLCL_ACD_AMT"),
    F.col("Claims.CLCL_CURR_ILL_DT").alias("CLCL_CURR_ILL_DT"),
    F.col("Claims.CLCL_SIMI_ILL_DT").alias("CLCL_SIMI_ILL_DT"),
    F.col("Claims.PRPR_ID").alias("PRPR_ID"),
    F.col("Claims.CLCL_PR_SS_EIN_IND").alias("CLCL_PR_SS_EIN_IND"),
    F.col("Claims.CLCL_NTWK_IND").alias("CLCL_NTWK_IND"),
    F.col("Claims.CLCL_PCP_IND").alias("CLCL_PCP_IND"),
    F.col("Claims.CLCL_PRPR_ID_PCP").alias("CLCL_PRPR_ID_PCP"),
    F.col("Claims.CLCL_PRPR_ID_REF").alias("CLCL_PRPR_ID_REF"),
    F.col("Claims.CLCL_PA_ACCT_NO").alias("CLCL_PA_ACCT_NO"),
    F.col("Claims.CLCL_EXT_AUTH_NO").alias("CLCL_EXT_AUTH_NO"),
    F.col("Claims.CLCL_PA_PAID_AMT").alias("CLCL_PA_PAID_AMT"),
    F.col("Claims.CLCL_TOT_CHG").alias("CLCL_TOT_CHG"),
    F.col("Claims.CLCL_TOT_PAYABLE").alias("CLCL_TOT_PAYABLE"),
    F.col("Claims.CLCL_DRAG_OR_IND").alias("CLCL_DRAG_OR_IND"),
    F.col("Claims.CLCL_DRAG_DT").alias("CLCL_DRAG_DT"),
    F.col("Claims.CLCL_INPUT_METH").alias("CLCL_INPUT_METH"),
    F.col("Claims.CLCL_AIAI_EOB_IND").alias("CLCL_AIAI_EOB_IND"),
    F.col("Claims.CLCL_EOB_EXCD_ID").alias("CLCL_EOB_EXCD_ID"),
    F.col("Claims.CLCL_BATCH_ID").alias("CLCL_BATCH_ID"),
    F.col("Claims.CLCL_BATCH_ACTION").alias("CLCL_BATCH_ACTION"),
    F.col("Claims.CLCL_CE_IND").alias("CLCL_CE_IND"),
    F.col("Claims.CLCL_CAP_IND").alias("CLCL_CAP_IND"),
    F.col("Claims.CLCL_AG_SOURCE").alias("CLCL_AG_SOURCE"),
    F.col("Claims.PDDS_PROD_TYPE").alias("PDDS_PROD_TYPE"),
    F.col("Claims.PDDS_MCTR_BCAT").alias("PDDS_MCTR_BCAT"),
    F.expr("REGEXP_REPLACE(COALESCE(Claims.CLCL_MICRO_ID,''), '[\\x0D\\x0A\\x09]', '')").alias("CLCL_MICRO_ID"),
    F.col("Claims.CLCL_UNABL_FROM_DT").alias("CLCL_UNABL_FROM_DT"),
    F.col("Claims.CLCL_UNABL_TO_DT").alias("CLCL_UNABL_TO_DT"),
    F.col("Claims.CLCL_RELHP_FROM_DT").alias("CLCL_RELHP_FROM_DT"),
    F.col("Claims.CLCL_RELHP_TO_DT").alias("CLCL_RELHP_TO_DT"),
    F.col("Claims.CLCL_OUT_LAB_IND").alias("CLCL_OUT_LAB_IND"),
    F.col("Claims.CLCL_MECD_RESUB_NO").alias("CLCL_MECD_RESUB_NO"),
    F.col("Claims.PRAD_TYPE").alias("PRAD_TYPE"),
    F.col("Claims.CLCL_PCA_IND").alias("CLCL_PCA_IND"),
    F.col("Claims.CLCL_OOA_IND").alias("CLCL_OOA_IND"),
    F.col("Claims.CLCL_EXT_REF_IND").alias("CLCL_EXT_REF_IND"),
    F.col("Claims.CLCL_RAD_ENC_IND").alias("CLCL_RAD_ENC_IND"),
    F.col("Claims.CLCL_REC_ENC_IND").alias("CLCL_REC_ENC_IND"),
    F.col("Claims.CLCL_COB_EOB_IND").alias("CLCL_COB_EOB_IND"),
    F.col("Claims.CLCL_LOCK_TOKEN").alias("CLCL_LOCK_TOKEN"),
    F.col("Claims.ATXR_SOURCE_ID").alias("ATXR_SOURCE_ID"),
    F.when(F.col("Claims.GRGR_ID").isNull(), "UNK").otherwise(F.col("Claims.GRGR_ID")).alias("GRGR_ID"),
    F.when(F.col("Claims.SGSG_ID").isNull(), "UNK").otherwise(F.col("Claims.SGSG_ID")).alias("SGSG_ID"),
    F.when(F.col("Claims.MEME_SFX").isNull(), F.lit(0)).otherwise(F.col("Claims.MEME_SFX")).alias("MEME_SFX"),
    F.when(F.col("Claims.SBSB_ID").isNull(), "UNK").otherwise(F.col("Claims.SBSB_ID")).alias("SBSB_ID"),
    F.when(F.col("EXP_CAT.PDBL_EXP_CAT").isNull(),"NA").otherwise(F.col("EXP_CAT.PDBL_EXP_CAT")).alias("PDBL_EXP_CAT"),
    F.when(F.col("MCTR_TYP_LU.PRPR_MCTR_TYPE").isNotNull(), trim(F.col("MCTR_TYP_LU.PRPR_MCTR_TYPE"))).otherwise(F.lit("")).alias("PRPR_MCTR_TYPE"),
    F.when(F.col("MCTR_SPEC_LU.PRCF_MCTR_SPEC").isNotNull(), trim(F.col("MCTR_SPEC_LU.PRCF_MCTR_SPEC"))).otherwise(F.lit("")).alias("PRCF_MCTR_SPEC"),
    F.when(F.col("SNAP_LOB_LU.PDBL_ACCT_CAT").isNull(), "UNK").otherwise(trim(F.col("SNAP_LOB_LU.PDBL_ACCT_CAT"))).alias("PDBL_ACCT_CAT"),
    F.when(F.col("MCTR_TYP_LU.PRPR_ENTITY").isNull(), " ").otherwise(F.col("MCTR_TYP_LU.PRPR_ENTITY")).alias("PRPR_ENTITY"),
    F.lit("<...PCA.TYP.CD.ROUTINE...>").alias("PCA_TYP_CD"),  # Placeholder as a direct function call unwrapped
    F.lit("<...REL_PCA_CLM_SK...>").alias("REL_PCA_CLM_SK"),
    F.when(F.col("TRAD_PARD_LU.CLED_TRAD_PARTNER").isNull(), " ").otherwise(F.col("TRAD_PARD_LU.CLED_TRAD_PARTNER")).alias("CLED_TRAD_PARTNER"),
    F.when(F.col("Claims.SESE_ID").isNull(), "UNK").otherwise(F.col("Claims.SESE_ID")).alias("SESE_ID"),
    F.when(F.col("Claims.CDML_SEQ_NO").isNull(), F.lit(0)).otherwise(F.col("Claims.CDML_SEQ_NO")).alias("CDML_SEQ_NO"),
    F.when(
        F.col("Claims.CLCL_CL_TYPE") == "D",
        F.when(F.col("REMIT_SUPR_DNTL_LU.REMIT_PR_SUPRSION_AMT").isNull(), F.lit(0)).otherwise(F.col("REMIT_SUPR_DNTL_LU.REMIT_PR_SUPRSION_AMT") + F.col("REMIT_SUPR_DNTL_LU.REMIT_SB_SUPRSION_AMT"))
    ).otherwise(
        F.when(F.col("REMIT_SUPR_MED_LU.REMIT_PR_SUPRSION_AMT").isNull(), F.lit(0)).otherwise(F.col("REMIT_SUPR_MED_LU.REMIT_PR_SUPRSION_AMT") + F.col("REMIT_SUPR_MED_LU.REMIT_SB_SUPRSION_AMT"))
    ).alias("REMIT_SUPRSION_AMT"),
    F.when(F.col("refLOBD_ID.LOBD_ID").isNull(), " ").otherwise(F.col("refLOBD_ID.LOBD_ID")).alias("LOBD_ID"),
    F.when(
        F.col("Claims.TAXONOMY_INDICATOR") == 1,
        F.when(
            (trim(F.col("Claims.CLPP_TAXONOMY_CD")) == "") | 
            (trim(F.col("Claims.CLPP_TAXONOMY_CD")) == "NA") | 
            (trim(F.col("Claims.CLPP_TAXONOMY_CD")) == "UNK") | 
            F.col("Claims.CLPP_TAXONOMY_CD").isNull(), 
            ""
        ).otherwise(trim(F.col("Claims.CLPP_TAXONOMY_CD")))
    ).otherwise(
        F.when(
            F.col("Claims.TAXONOMY_INDICATOR") == 0,
            F.when(
                (trim(F.col("Claims.CLMF_INPUT_TXNM_CD")) == "") | 
                (trim(F.col("Claims.CLMF_INPUT_TXNM_CD")) == "NA") | 
                (trim(F.col("Claims.CLMF_INPUT_TXNM_CD")) == "UNK") | 
                F.col("Claims.CLMF_INPUT_TXNM_CD").isNull(), 
                ""
            ).otherwise(trim(F.col("Claims.CLMF_INPUT_TXNM_CD")))
        ).otherwise("")
    ).alias("CLM_TXNMY_CD")
)

df_Extract = df_Extract_unordered.filter(cond_extract)

# Container calls
params_empty = {}

df_ClmOUT = contHashFiles(df_Extract, params_empty)
df_populated_CRF = contTransform(df_ClmOUT, params_empty)
df_Transform = contReversalLogic(df_populated_CRF, params_empty)

# Snapshot Transformer: produces two output links => "Pkey", "Snapshot"

df_Snapshot_in = df_Transform.alias("Transform")

# Build "Pkey" link
df_Pkey_unordered = df_Snapshot_in.select(
    F.col("Transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("Transform.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("Transform.DISCARD_IN").alias("DISCARD_IN"),
    F.col("Transform.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("Transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("Transform.ERR_CT").alias("ERR_CT"),
    F.col("Transform.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("Transform.CLM_SK").alias("CLM_SK"),
    F.col("Transform.SrcSysCdSk").alias("SRC_SYS_CD_SK"),
    F.col("Transform.CLM_ID").alias("CLM_ID"),
    F.col("Transform.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Transform.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Transform.ADJ_FROM_CLM").alias("ADJ_FROM_CLM"),
    F.col("Transform.ADJ_TO_CLM").alias("ADJ_TO_CLM"),
    F.col("Transform.ALPHA_PFX_CD").alias("ALPHA_PFX_CD"),
    F.col("Transform.CLM_EOB_EXCD").alias("CLM_EOB_EXCD"),
    F.col("Transform.CLS").alias("CLS"),
    F.col("Transform.CLS_PLN").alias("CLS_PLN"),
    F.col("Transform.EXPRNC_CAT").alias("EXPRNC_CAT"),
    F.col("Transform.FNCL_LOB_NO").alias("FNCL_LOB_NO"),
    F.col("Transform.GRP").alias("GRP"),
    F.col("Transform.MBR_CK").alias("MBR_CK"),
    F.col("Transform.NTWK").alias("NTWK"),
    F.col("Transform.PROD").alias("PROD"),
    F.col("Transform.SUBGRP").alias("SUBGRP"),
    F.col("Transform.SUB_CK").alias("SUB_CK"),
    F.col("Transform.CLM_ACDNT_CD").alias("CLM_ACDNT_CD"),
    F.col("Transform.CLM_ACDNT_ST_CD").alias("CLM_ACDNT_ST_CD"),
    F.col("Transform.CLM_ACTIVATING_BCBS_PLN_CD").alias("CLM_ACTIVATING_BCBS_PLN_CD"),
    F.col("Transform.CLM_AGMNT_SRC_CD").alias("CLM_AGMNT_SRC_CD"),
    F.col("Transform.CLM_BTCH_ACTN_CD").alias("CLM_BTCH_ACTN_CD"),
    F.col("Transform.CLM_CAP_CD").alias("CLM_CAP_CD"),
    F.col("Transform.CLM_CAT_CD").alias("CLM_CAT_CD"),
    F.col("Transform.CLM_CHK_CYC_OVRD_CD").alias("CLM_CHK_CYC_OVRD_CD"),
    F.col("Transform.CLM_COB_CD").alias("CLM_COB_CD"),
    F.col("Transform.FINL_DISP_CD").alias("FINL_DISP_CD"),
    F.col("Transform.CLM_INPT_METH_CD").alias("CLM_INPT_METH_CD"),
    F.col("Transform.CLM_INPT_SRC_CD").alias("CLM_INPT_SRC_CD"),
    F.col("Transform.CLM_IPP_CD").alias("CLM_IPP_CD"),
    F.col("Transform.CLM_NTWK_STTUS_CD").alias("CLM_NTWK_STTUS_CD"),
    F.col("Transform.CLM_NONPAR_PROV_PFX_CD").alias("CLM_NONPAR_PROV_PFX_CD"),
    F.col("Transform.CLM_OTHER_BNF_CD").alias("CLM_OTHER_BNF_CD"),
    F.col("Transform.CLM_PAYE_CD").alias("CLM_PAYE_CD"),
    F.col("Transform.CLM_PRCS_CTL_AGNT_PFX_CD").alias("CLM_PRCS_CTL_AGNT_PFX_CD"),
    F.col("Transform.CLM_SVC_DEFN_PFX_CD").alias("CLM_SVC_DEFN_PFX_CD"),
    F.col("Transform.CLM_SVC_PROV_SPEC_CD").alias("CLM_SVC_PROV_SPEC_CD"),
    F.col("Transform.CLM_SVC_PROV_TYP_CD").alias("CLM_SVC_PROV_TYP_CD"),
    F.col("Transform.CLM_STTUS_CD").alias("CLM_STTUS_CD"),
    F.col("Transform.CLM_SUBMT_BCBS_PLN_CD").alias("CLM_SUBMT_BCBS_PLN_CD"),
    F.col("Transform.CLM_SUB_BCBS_PLN_CD").alias("CLM_SUB_BCBS_PLN_CD"),
    F.col("Transform.CLM_SUBTYP_CD").alias("CLM_SUBTYP_CD"),
    F.col("Transform.CLM_TYP_CD").alias("CLM_TYP_CD"),
    F.col("Transform.ATCHMT_IN").alias("ATCHMT_IN"),
    F.col("Transform.CLM_CLNCL_EDIT_CD").alias("CLM_CLNCL_EDIT_CD"),
    F.col("Transform.COBRA_CLM_IN").alias("COBRA_CLM_IN"),
    F.col("Transform.FIRST_PASS_IN").alias("FIRST_PASS_IN"),
    F.col("Transform.HOST_IN").alias("HOST_IN"),
    F.col("Transform.LTR_IN").alias("LTR_IN"),
    F.col("Transform.MCARE_ASG_IN").alias("MCARE_ASG_IN"),
    F.col("Transform.NOTE_IN").alias("NOTE_IN"),
    F.col("Transform.PCA_AUDIT_IN").alias("PCA_AUDIT_IN"),
    F.col("Transform.PCP_SUBMT_IN").alias("PCP_SUBMT_IN"),
    F.col("Transform.PROD_OOA_IN").alias("PROD_OOA_IN"),
    F.col("Transform.ACDNT_DT").alias("ACDNT_DT"),
    F.col("Transform.INPT_DT").alias("INPT_DT"),
    F.col("Transform.MBR_PLN_ELIG_DT").alias("MBR_PLN_ELIG_DT"),
    F.col("Transform.NEXT_RVW_DT").alias("NEXT_RVW_DT"),
    F.col("Transform.PD_DT").alias("PD_DT"),
    F.col("Transform.PAYMT_DRAG_CYC_DT").alias("PAYMT_DRAG_CYC_DT"),
    F.col("Transform.PRCS_DT").alias("PRCS_DT"),
    F.col("Transform.RCVD_DT").alias("RCVD_DT"),
    F.col("Transform.SVC_STRT_DT").alias("SVC_STRT_DT"),
    F.col("Transform.SVC_END_DT").alias("SVC_END_DT"),
    F.col("Transform.SMLR_ILNS_DT").alias("SMLR_ILNS_DT"),
    F.col("Transform.STTUS_DT").alias("STTUS_DT"),
    F.col("Transform.WORK_UNABLE_BEG_DT").alias("WORK_UNABLE_BEG_DT"),
    F.col("Transform.WORK_UNABLE_END_DT").alias("WORK_UNABLE_END_DT"),
    F.col("Transform.ACDNT_AMT").alias("ACDNT_AMT"),
    F.col("Transform.ACTL_PD_AMT").alias("ACTL_PD_AMT"),
    F.col("Transform.ALLOW_AMT").alias("ALLOW_AMT"),
    F.col("Transform.DSALW_AMT").alias("DSALW_AMT"),
    F.col("Transform.COINS_AMT").alias("COINS_AMT"),
    F.col("Transform.CNSD_CHRG_AMT").alias("CNSD_CHRG_AMT"),
    F.col("Transform.COPAY_AMT").alias("COPAY_AMT"),
    F.col("Transform.CHRG_AMT").alias("CHRG_AMT"),
    F.col("Transform.DEDCT_AMT").alias("DEDCT_AMT"),
    F.col("Transform.PAYBL_AMT").alias("PAYBL_AMT"),
    F.col("Transform.CLM_CT").alias("CLM_CT"),
    F.col("Transform.MBR_AGE").alias("MBR_AGE"),
    F.col("Transform.ADJ_FROM_CLM_ID").alias("ADJ_FROM_CLM_ID"),
    F.col("Transform.ADJ_TO_CLM_ID").alias("ADJ_TO_CLM_ID"),
    F.col("Transform.DOC_TX_ID").alias("DOC_TX_ID"),
    F.col("Transform.MCAID_RESUB_NO").alias("MCAID_RESUB_NO"),
    F.col("Transform.MCARE_ID").alias("MCARE_ID"),
    F.col("Transform.MBR_SFX_NO").alias("MBR_SFX_NO"),
    F.col("Transform.PATN_ACCT_NO").alias("PATN_ACCT_NO"),
    F.col("Transform.PAYMT_REF_ID").alias("PAYMT_REF_ID"),
    F.col("Transform.PROV_AGMNT_ID").alias("PROV_AGMNT_ID"),
    F.col("Transform.RFRNG_PROV_TX").alias("RFRNG_PROV_TX"),
    F.col("Transform.SUB_ID").alias("SUB_ID"),
    F.col("Transform.PRPR_ENTITY").alias("PRPR_ENTITY"),
    F.col("Transform.PCA_TYP_CD").alias("PCA_TYP_CD"),
    F.col("Transform.REL_PCA_CLM_ID").alias("REL_PCA_CLM_ID"),
    F.col("Transform.CLCL_MICRO_ID").alias("CLCL_MICRO_ID"),
    F.col("Transform.CLM_UPDT_SW").alias("CLM_UPDT_SW"),
    F.col("Transform.REMIT_SUPRSION_AMT").alias("REMIT_SUPRSION_AMT"),
    F.col("Transform.MCAID_STTUS_ID").alias("MCAID_STTUS_ID"),
    F.col("Transform.PATN_PD_AMT").alias("PATN_PD_AMT"),
    F.col("Transform.CLM_SUBMT_ICD_VRSN_CD").alias("CLM_SUBMT_ICD_VRSN_CD"),
    F.col("Transform.CLM_TXNMY_CD").alias("CLM_TXNMY_CD"),
    F.lit("N").alias("BILL_PAYMT_EXCL_IN")
)

df_Pkey = df_Pkey_unordered

# Build "Snapshot" link
df_Snapshot_unordered = df_Snapshot_in.select(
    F.col("Transform.CLM_ID").alias("CLM_ID"),
    F.col("Transform.MBR_CK").alias("MBR_CK"),
    F.col("Transform.GRP").alias("GRP"),
    F.col("Transform.SVC_STRT_DT").alias("SVC_STRT_DT"),
    F.col("Transform.CHRG_AMT").alias("CHRG_AMT"),
    F.col("Transform.PAYBL_AMT").alias("PAYBL_AMT"),
    F.col("Transform.EXPRNC_CAT").alias("EXPRNC_CAT"),
    F.col("Transform.FNCL_LOB_NO").alias("FNCL_LOB_NO"),
    F.col("Transform.CLM_CT").alias("CLM_CT"),
    F.col("Transform.PCA_TYP_CD").alias("PCA_TYP_CD"),
    F.col("Transform.CLM_STTUS_CD").alias("CLM_STTUS_CD"),
    F.col("Transform.CLM_CAT_CD").alias("CLM_CAT_CD")
)

df_Snapshot = df_Snapshot_unordered

# Next Transformer stage with input = df_Snapshot => output "RowCount"

df_Transformer_in = df_Snapshot.alias("Snapshot")

ExpCatCdSk_col = F.lit("<...GetFkeyExprncCat('FACETS', 0, Snapshot.EXPRNC_CAT, 'N')...>")
GrpSk_col = F.lit("<...GetFkeyGrp('FACETS', 0, Snapshot.GRP, 'N')...>")
MbrSk_col = F.lit("<...GetFkeyMbr('FACETS', 0, Snapshot.MBR_CK, 'N')...>")
FnclLobSk_col = F.lit("<...GetFkeyFnclLob('PSI', 0, Snapshot.FNCL_LOB_NO, 'N')...>")
PcaTypCdSk_col = F.lit("<...GetFkeyCodes('FACETS', 0, 'PERSONAL CARE ACCOUNT PROCESSING', Snapshot.PCA_TYP_CD, 'N')...>")
ClmSttusCdSk_col = F.lit("<...GetFkeyCodes('FACETS', 0, 'CLAIM STATUS', Snapshot.CLM_STTUS_CD, 'N')...>")
ClmCatCdSk_col = F.lit("<...GetFkeyCodes('FACETS', 0, 'CLAIM CATEGORY', Snapshot.CLM_CAT_CD, 'X')...>")

df_Transformer_enriched = df_Transformer_in.select(
    F.col("Snapshot.CLM_ID").alias("CLM_ID"),
    ExpCatCdSk_col.alias("EXPRNC_CAT_SK"),
    GrpSk_col.alias("GRP_SK"),
    MbrSk_col.alias("MBR_SK"),
    FnclLobSk_col.alias("FNCL_LOB_SK"),
    PcaTypCdSk_col.alias("PCA_TYP_CD_SK"),
    ClmSttusCdSk_col.alias("CLM_STTUS_CD_SK"),
    ClmCatCdSk_col.alias("CLM_CAT_CD_SK"),
    F.col("Snapshot.SVC_STRT_DT").alias("SVC_STRT_DT_SK"),
    F.col("Snapshot.CHRG_AMT").alias("CHRG_AMT"),
    F.col("Snapshot.PAYBL_AMT").alias("PAYBL_AMT"),
    F.col("Snapshot.CLM_CT").alias("CLM_CT"),
    F.lit("SrcSysCdSk").alias("SRC_SYS_CD_SK")  # Assume we keep the original from job param if needed
)

df_RowCount = df_Transformer_enriched.select(
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_STTUS_CD_SK").alias("CLM_STTUS_CD_SK"),
    F.col("CLM_CAT_CD_SK").alias("CLM_CAT_CD_SK"),
    F.col("EXPRNC_CAT_SK").alias("EXPRNC_CAT_SK"),
    F.col("FNCL_LOB_SK").alias("FNCL_LOB_SK"),
    F.col("GRP_SK").alias("GRP_SK"),
    F.col("MBR_SK").alias("MBR_SK"),
    F.col("SVC_STRT_DT_SK").alias("SVC_STRT_DT_SK"),
    F.col("CHRG_AMT").alias("CHRG_AMT"),
    F.col("PAYBL_AMT").alias("PAYBL_AMT"),
    F.col("CLM_CT").alias("CLM_CT"),
    F.col("PCA_TYP_CD_SK").alias("PCA_TYP_CD_SK")
)

# B_CLM (CSeqFileStage) write
# The specification:
# File path => "load/B_CLM.#SrcSysCd#.dat.#RunID#", no "landing" or "external" => use adls_path
out_b_clm = f"{adls_path}/load/B_CLM.{SrcSysCd}.dat.{RunID}"

write_files(
    df_RowCount,
    out_b_clm,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# ClmPK Container => one input => one output
df_ClmPK_in = df_Pkey.alias("Snapshot")

df_ClmPK_out = ClmPK(df_ClmPK_in, params_empty)

# FctsClmExtr => final CSeqFileStage write
# The specification:
# File path => "key/LhoFctsClmExtr.LhoFctsClm.dat.#RunID#", no "landing"/"external" => use adls_path
out_fctsclm_file = f"{adls_path}/key/LhoFctsClmExtr.LhoFctsClm.dat.{RunID}"

write_files(
    df_ClmPK_out,
    out_fctsclm_file,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)