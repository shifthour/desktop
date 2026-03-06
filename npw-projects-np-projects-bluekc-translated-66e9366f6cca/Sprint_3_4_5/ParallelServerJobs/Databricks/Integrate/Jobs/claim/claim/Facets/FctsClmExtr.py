# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC COPYRIGHT 2021 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
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
# MAGIC Giri  Mallavaram    2020-04-06      6131 PBM Changes  Added SPEC_DRUG_IN to be in sync with changes to DrugClmPKey Container        IntegrateDev2          Kalyan Neelam     2020-04-07
# MAGIC 
# MAGIC outham Kalidnidi   2021-03-17                                       Facets Upgrade added JOIN to get CLCB_COB_REAS_CD       IntegrateDev1                         Kalyan Neelam      2021-03-18
# MAGIC Hugh Sisson         2021-04-20                                       In the contHashFiles container the logic to lookup Alpha Prefix  IntegrateDev2                    Jaideep Mankala         04/22/2021
# MAGIC                                                                                       to use the facets_repl.CMC_CSPI_CS_PLAN table instead of 
# MAGIC                                                                                       the bcbs_repl.GRP_PROD_BCBSA_PLN table.  This brings
# MAGIC                                                                                       the Claims process into alignment with the Membership process.
# MAGIC GouthamK  2021-06-10     US-372568                          LHO Conversion - modified SQL to use CLCL_ID_CRTE_FROM                      IntegrateDev2     Jeyaprasanna       2021-06-16
# MAGIC Krishan      2021-06-15     US-374712                           updated mapping for the column -CLM_IPP_CD in the shared container          IntegrateSitf        Jeyaprasanna       2021-06-16
# MAGIC                                                                                      -contHashFiles stage name(all_the_rules_here) to include additional 
# MAGIC                                                                                       else if caluse ELSE IF(CLMI_MISC_INFO.CLMI_ITS_SUB_TYPE = 'T'
# MAGIC                                                                                       OR Extract.CLCL_PRE_PRICE_IND = 'T') THEN 'H'
# MAGIC Prabhu ES          2022-02-28      S2S Remediation        MSSQL connection parameters added                                                            IntegrateDev5            Kalyan Neelam         2022-06-10
# MAGIC Ken Bradmon     2022-12-14       ProdSupport               Added WITH (INDEX to force the use of particular inex with the                     IntegrateDev2
# MAGIC                                                                                          PDBL_EXP_CAT and SNAP_LOB queries

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
# MAGIC Do Not Clear the hf_clm_pca_clms hashed file
# MAGIC Lookup first pass indictor in IDS to cover hit list claims
# MAGIC 
# MAGIC Uses W_FCTS_RCRD_DEL table
# MAGIC All hash files match back to claim data based on CLM_ID
# MAGIC Hashed file hf_clm_sts created in FctsClmLnMedExtr and updated in FctsClmLnDntlExtr
# MAGIC Do Not Clear the hf_first_pass_ind hashed file.  This hashed file is created in another process, FctsFirstPassIndCntl and is used by the nightly Facets Claim program
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
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------

# MAGIC %run ../../../../../shared_containers/PrimaryKey/contHashFiles
# COMMAND ----------
# MAGIC %run ../../../../../shared_containers/PrimaryKey/contTransform
# COMMAND ----------
# MAGIC %run ../../../../../shared_containers/PrimaryKey/contReversalLogic
# COMMAND ----------
# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmPK
# COMMAND ----------

DriverTable = get_widget_value("DriverTable", "")
AdjFromTable = get_widget_value("AdjFromTable", "")
LowSvcDate = get_widget_value("LowSvcDate", "")
CurrRunCycle = get_widget_value("CurrRunCycle", "")
RunID = get_widget_value("RunID", "")
CurrentDate = get_widget_value("CurrentDate", "")
FacetsOwner = get_widget_value("FacetsOwner","")
facets_secret_name = get_widget_value("facets_secret_name","")
BCBSOwner = get_widget_value("BCBSOwner","")
bcbs_secret_name = get_widget_value("bcbs_secret_name","")
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
SrcSysCdSk = get_widget_value("SrcSysCdSk","")

jdbc_url, jdbc_props = get_db_config(facets_secret_name)

df_Claims = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query", 
        f"""
SELECT cast(Trim(A.CLCL_ID) as char(12)) as CLCL_ID, 
       A.MEME_CK, A.GRGR_CK, A.SBSB_CK, A.SGSG_CK, A.CLCL_CL_TYPE, A.CLCL_CL_SUB_TYPE, A.CLCL_PRE_PRICE_IND, A.CLCL_CUR_STS,
       A.CLST_SEQ_NO, A.CLCL_SITE, A.CLCL_LAST_ACT_DTM, A.CLCL_INPUT_DT, A.CLCL_RECD_DT, A.CLCL_ACPT_DTM, A.CLCL_PAID_DT,
       A.CLCL_NEXT_REV_DT, A.CLCL_LOW_SVC_DT, A.CLCL_HIGH_SVC_DT, A.CLCL_ID_ADJ_TO, 
       CASE WHEN SUBSTRING(A.CLCL_ID_ADJ_FROM ,1,1) = 'L' and SUBSTRING(A.CLCL_ID_CRTE_FROM ,1,1) = 'L' THEN A.CLCL_ID_CRTE_FROM
            WHEN (A.CLCL_ID_ADJ_FROM = ''  OR LEN(RTRIM(LTRIM(A.CLCL_ID_ADJ_FROM))) IS NULL) THEN A.CLCL_ID_CRTE_FROM
            ELSE A.CLCL_ID_ADJ_FROM END as CLCL_ID_ADJ_FROM,
       A.CSPD_CAT, A.PZAP_ID,
       A.CSCS_ID, A.CSPI_ID, 
       cast(Trim(A.PDPD_ID) as char(8)) as PDPD_ID, A.MEPE_FI, A.MEPE_PLAN_ENTRY_DT, A.CLCL_COBRA_IND, A.CLCL_ME_AGE, A.MEME_SEX,
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
       F.SBSB_ID,
       A.CLCL_HSA_IND,
       G.SESE_ID,
       G.CDML_SEQ_NO,
       G.SESE_RULE,
       H.CLPP_TAXONOMY_CD,
       I.CLMF_INPUT_TXNM_CD,
       case when SUBSTRING(A.CLCL_ID, 6, 1) = 'H' then 1 else 0 end as TAXONOMY_INDICATOR
FROM {FacetsOwner}.CMC_CLCL_CLAIM A
INNER JOIN tempdb..{DriverTable} B
  ON A.CLCL_ID=B.CLM_ID
INNER JOIN {FacetsOwner}.CMC_GRGR_GROUP C
  ON A.GRGR_CK = C.GRGR_CK
INNER JOIN {FacetsOwner}.CMC_SGSG_SUB_GROUP D 
  ON A.SGSG_CK = D.SGSG_CK
INNER JOIN {FacetsOwner}.CMC_MEME_MEMBER E 
  ON A.MEME_CK=E.MEME_CK
INNER JOIN {FacetsOwner}.CMC_SBSB_SUBSC F
  ON A.SBSB_CK=F.SBSB_CK
LEFT JOIN {FacetsOwner}.CMC_CDML_CL_LINE G 
  ON A.CLCL_ID = G.CLCL_ID
LEFT JOIN {FacetsOwner}.CMC_CLPP_ITS_PROV H
  ON A.CLCL_ID=H.CLCL_ID
LEFT JOIN {FacetsOwner}.CMC_CLMF_MULT_FUNC I
  ON A.CLCL_ID=I.CLCL_ID
WHERE (G.CDML_SEQ_NO = 1 or G.CDML_SEQ_NO is null)
"""
    )
    .load()
)

df_PDBL_EXP_CAT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
SELECT cast(Trim(CLAIM.CLCL_ID) as CHAR(12)) AS CLCL_ID,
       BILL.PDBL_EXP_CAT
FROM {FacetsOwner}.CMC_CLCL_CLAIM CLAIM  WITH (INDEX(bcbsix_cmc_clcl_claim_fepeob))
INNER JOIN ({FacetsOwner}.CMC_PDBC_PROD_COMP COMP 
            INNER JOIN {FacetsOwner}.CMC_PDBL_PROD_BILL BILL 
             ON COMP.PDBC_PFX = BILL.PDBC_PFX)
  ON CLAIM.PDPD_ID = COMP.PDPD_ID, tempdb..{DriverTable} TMP
WHERE CLAIM.CLCL_ID= TMP.CLM_ID
  AND BILL.PDBL_ID In ("MED","MED1","DEN","DEN1","DEN2")
  AND BILL.PDBL_EXP_CAT Not In (" ","")
  AND COMP.PDBC_TYPE="PDBL"
  AND COMP.PDBC_EFF_DT<= CLAIM.CLCL_LOW_SVC_DT
  AND COMP.PDBC_TERM_DT>= CLAIM.CLCL_LOW_SVC_DT
  AND BILL.PDBL_EFF_DT<= CLAIM.CLCL_LOW_SVC_DT
  AND BILL.PDBL_TERM_DT>= CLAIM.CLCL_LOW_SVC_DT
"""
    )
    .load()
)

df_MCTR_TYPE = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
SELECT 
CAST(Trim(clm.CLCL_ID) as char(12)) as CLCL_ID ,
prov.PRPR_MCTR_TYPE,
prov.PRPR_ENTITY
FROM  tempdb..{DriverTable} TMP
INNER JOIN  {FacetsOwner}.CMC_CLCL_CLAIM clm
  ON  TMP.CLM_ID = clm.CLCL_ID
INNER JOIN {FacetsOwner}.CMC_PRPR_PROV prov
  ON clm.PRPR_ID = prov.PRPR_ID
"""
    )
    .load()
)

df_SNAP_LOB = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
SELECT cast(Trim(CLAIM.CLCL_ID) as char(12)) as CLCL_ID,
       BILL.PDBL_ACCT_CAT
FROM {FacetsOwner}.CMC_CLCL_CLAIM CLAIM  WITH (INDEX(bcbsix_cmc_clcl_claim_fepeob))
INNER JOIN ({FacetsOwner}.CMC_PDBC_PROD_COMP COMP 
            INNER JOIN {FacetsOwner}.CMC_PDBL_PROD_BILL BILL 
            ON COMP.PDBC_PFX = BILL.PDBC_PFX) 
  ON CLAIM.PDPD_ID = COMP.PDPD_ID, tempdb..{DriverTable} TMP
WHERE COMP.PDBC_TYPE        =    'PDBL'
  AND COMP.PDBC_EFF_DT      <=   CLAIM.CLCL_LOW_SVC_DT
  AND COMP.PDBC_TERM_DT     >=   CLAIM.CLCL_LOW_SVC_DT
  AND BILL.PDBL_ID          In    ('MED1','DEN1','MED','DEN','DEN2')
  AND BILL.PDBL_EFF_DT      <=   CLAIM.CLCL_LOW_SVC_DT
  AND BILL.PDBL_TERM_DT     >=   CLAIM.CLCL_LOW_SVC_DT
  AND CLAIM.CLCL_ID         =    TMP.CLM_ID
"""
    )
    .load()
)

df_MCTR_SPEC = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
SELECT CAST(TRIM(CLAIM.CLCL_ID) AS CHAR(12)) AS CLCL_ID,
       PROV.PRCF_MCTR_SPEC
FROM tempdb..{DriverTable} TMP,
     {FacetsOwner}.CMC_CLCL_CLAIM CLAIM,
     {FacetsOwner}.CMC_PRPR_PROV PROV,
     {FacetsOwner}.CMC_MCTR_CD_TRANS TRANS
WHERE TMP.CLM_ID = CLAIM.CLCL_ID
  AND CLAIM.CLCL_CL_SUB_TYPE in ('M','D')
  AND CLAIM.PRPR_ID = PROV.PRPR_ID
  AND PROV.PRCF_MCTR_SPEC = TRANS.MCTR_VALUE
  AND TRANS.MCTR_ENTITY='PRAC'
  AND TRANS.MCTR_TYPE='SPEC'
"""
    )
    .load()
)

df_TRAD_PARD = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
SELECT CAST(TRIM(CLED.CLCL_ID) AS CHAR(12)) AS CLCL_ID,
       CLED.CLED_TRAD_PARTNER
FROM {FacetsOwner}.CMC_CLED_EDI_DATA CLED,
     tempdb..{DriverTable} TMP
WHERE CLED.CLCL_ID= TMP.CLM_ID
"""
    )
    .load()
)

df_CLHS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
Select CAST(TRIM(CLCL.CLCL_ID) AS CHAR(12)) AS CLCL_ID,
       CLHS.MEME_CK
FROM {FacetsOwner}.CMC_CLCL_CLAIM CLCL,
     {FacetsOwner}.CMC_CLHS_HSA_CLAIM CLHS,
     tempdb..{DriverTable} DRVR
WHERE CLCL.CLCL_ID=DRVR.CLM_ID 
  AND CLCL.CLCL_ID = CLHS.CLCL_ID
"""
    )
    .load()
)

df_REMIT_SUPR_MED = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
select CAST(Trim(DRVR.CLM_ID)  AS CHAR(12)) AS CLCL_ID,
       SUM(CDML.CDML_SB_PYMT_AMT) as REMIT_SB_SUPRSION_AMT,
       SUM(CDML.CDML_PR_PYMT_AMT) as REMIT_PR_SUPRSION_AMT
from tempdb..{DriverTable} DRVR,
     {FacetsOwner}.CMC_CLOR_CL_OVR CLOR,
     {FacetsOwner}.CMC_CDML_CL_LINE CDML
WHERE DRVR.CLM_ID = CLOR.CLCL_ID
  AND DRVR.CLM_ID = CDML.CLCL_ID
  AND CLOR.CLOR_OR_ID = 'SC'
  AND (CDML.CDML_SB_PYMT_AMT > 0 or CDML.CDML_PR_PYMT_AMT > 0)
GROUP BY DRVR.CLM_ID
"""
    )
    .load()
)

df_REMIT_SUPR_DNTL = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
select CAST(Trim(DRVR.CLM_ID)  AS CHAR(12)) as CLCL_ID,
       SUM(CDDL.CDDL_SB_PYMT_AMT) AS REMIT_SB_SUPRSION_AMT,
       SUM(CDDL.CDDL_PR_PYMT_AMT) AS REMIT_PR_SUPRSION_AMT
from tempdb..{DriverTable} DRVR,
     {FacetsOwner}.CMC_CLOR_CL_OVR CLOR,
     {FacetsOwner}.CMC_CDDL_CL_LINE CDDL
WHERE DRVR.CLM_ID = CLOR.CLCL_ID
  AND DRVR.CLM_ID = CDDL.CLCL_ID
  AND CLOR.CLOR_OR_ID = 'SC'
  AND (CDDL.CDDL_SB_PYMT_AMT > 0 or CDDL.CDDL_PR_PYMT_AMT > 0)
GROUP BY DRVR.CLM_ID
"""
    )
    .load()
)

df_lodLOBD_ID = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
SELECT RTRIM(PDPD_ID) AS PDPD_ID, Trim(RTRIM(LOBD_ID)) as LOBD_ID 
FROM {FacetsOwner}.CMC_PDPD_PRODUCT
"""
    )
    .load()
)


# Below: Convert each input link in the CHashedFileStage ("Lookups") to Parquet (Scenario C).
# The JSON shows each output link with ordered columns, then writes to hashed file. We replicate that by:
# 1) Selecting columns in correct order with rpad if char/varchar
# 2) Writing out to Parquet
# 3) Reading them back to use in the "StripFields" stage.


# 1) refLOBD_ID => from df_lodLOBD_ID => hf_clm_lobd_id.hf -> hf_clm_lobd_id.parquet
df_refLOBD_ID = df_lodLOBD_ID.select(
    F.rpad(F.col("PDPD_ID"), 8, " ").alias("PDPD_ID"),
    F.rpad(F.col("LOBD_ID"), 4, " ").alias("LOBD_ID")
)
write_files(
    df_refLOBD_ID,
    "hf_clm_lobd_id.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)
df_refLOBD_ID = spark.read.parquet("hf_clm_lobd_id.parquet")


# 2) REMIT_SUPR_DNTL_LU => from df_REMIT_SUPR_DNTL => hf_clm_remit_supr_dntl
df_remit_supr_dntl_lu = df_REMIT_SUPR_DNTL.select(
    F.rpad(F.col("CLCL_ID"), 12, " ").alias("CLCL_ID"),
    F.col("REMIT_SB_SUPRSION_AMT").alias("REMIT_SB_SUPRSION_AMT"),
    F.col("REMIT_PR_SUPRSION_AMT").alias("REMIT_PR_SUPRSION_AMT")
)
write_files(
    df_remit_supr_dntl_lu,
    "hf_clm_remit_supr_dntl.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)
df_remit_supr_dntl_lu = spark.read.parquet("hf_clm_remit_supr_dntl.parquet")


# 3) REMIT_SUPR_MED_LU => from df_REMIT_SUPR_MED => hf_clm_remit_supr
df_remit_supr_med_lu = df_REMIT_SUPR_MED.select(
    F.rpad(F.col("CLCL_ID"), 12, " ").alias("CLCL_ID"),
    F.col("REMIT_SB_SUPRSION_AMT").alias("REMIT_SB_SUPRSION_AMT"),
    F.col("REMIT_PR_SUPRSION_AMT").alias("REMIT_PR_SUPRSION_AMT")
)
write_files(
    df_remit_supr_med_lu,
    "hf_clm_remit_supr.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)
df_remit_supr_med_lu = spark.read.parquet("hf_clm_remit_supr.parquet")


# 4) CLHS_LU => from df_CLHS => hf_clm_hsa_clhs
df_clhs_lu = df_CLHS.select(
    F.rpad(F.col("CLCL_ID"), 12, " ").alias("CLCL_ID"),
    F.col("MEME_CK").cast(IntegerType()).alias("MEME_CK")
)
write_files(
    df_clhs_lu,
    "hf_clm_hsa_clhs.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)
df_clhs_lu = spark.read.parquet("hf_clm_hsa_clhs.parquet")


# 5) TRAD_PARD_LU => from df_TRAD_PARD => hf_clm_trad_pard
df_trad_pard_lu = df_TRAD_PARD.select(
    F.rpad(F.col("CLCL_ID"), 12, " ").alias("CLCL_ID"),
    F.rpad(F.col("CLED_TRAD_PARTNER"), 15, " ").alias("CLED_TRAD_PARTNER")
)
write_files(
    df_trad_pard_lu,
    "hf_clm_trad_pard.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)
df_trad_pard_lu = spark.read.parquet("hf_clm_trad_pard.parquet")


# 6) SNAP_LOB_LU => from df_SNAP_LOB => hf_clm_snap_lob
df_snap_lob_lu = df_SNAP_LOB.select(
    F.rpad(F.col("CLCL_ID"), 12, " ").alias("CLCL_ID"),
    F.rpad(F.col("PDBL_ACCT_CAT"), 4, " ").alias("PDBL_ACCT_CAT")
)
write_files(
    df_snap_lob_lu,
    "hf_clm_snap_lob.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)
df_snap_lob_lu = spark.read.parquet("hf_clm_snap_lob.parquet")


# 7) MCTR_SPEC_LU => from df_MCTR_SPEC => hf_clm_mctr_spec
df_mctr_spec_lu = df_MCTR_SPEC.select(
    F.rpad(F.col("CLCL_ID"), 12, " ").alias("CLCL_ID"),
    F.rpad(F.col("PRCF_MCTR_SPEC"), 4, " ").alias("PRCF_MCTR_SPEC")
)
write_files(
    df_mctr_spec_lu,
    "hf_clm_mctr_spec.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)
df_mctr_spec_lu = spark.read.parquet("hf_clm_mctr_spec.parquet")


# 8) MCTR_TYP_LU => from df_MCTR_TYPE => hf_clm_mctr_type
df_mctr_typ_lu = df_MCTR_TYPE.select(
    F.rpad(F.col("CLCL_ID"), 12, " ").alias("CLCL_ID"),
    F.rpad(F.col("PRPR_MCTR_TYPE"), 4, " ").alias("PRPR_MCTR_TYPE"),
    F.rpad(F.col("PRPR_ENTITY"), 1, " ").alias("PRPR_ENTITY")
)
write_files(
    df_mctr_typ_lu,
    "hf_clm_mctr_type.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)
df_mctr_typ_lu = spark.read.parquet("hf_clm_mctr_type.parquet")


# 9) EXP_CAT => from df_PDBL_EXP_CAT => hf_clm_exp_cat
df_exp_cat = df_PDBL_EXP_CAT.select(
    F.rpad(F.col("CLCL_ID"), 12, " ").alias("CLCL_ID"),
    F.rpad(F.col("PDBL_EXP_CAT"), 4, " ").alias("PDBL_EXP_CAT")
)
write_files(
    df_exp_cat,
    "hf_clm_exp_cat.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)
df_exp_cat = spark.read.parquet("hf_clm_exp_cat.parquet")


# Now implement the "StripFields" stage logic:
# 1) Primary link: df_Claims
# 2) Left joins with the 9 lookup DFs
df_stripfields_joined = (
    df_Claims.alias("Claims")
    .join(df_exp_cat.alias("EXP_CAT"), F.col("Claims.CLCL_ID") == F.col("EXP_CAT.CLCL_ID"), "left")
    .join(df_mctr_typ_lu.alias("MCTR_TYP_LU"), F.col("Claims.CLCL_ID") == F.col("MCTR_TYP_LU.CLCL_ID"), "left")
    .join(df_mctr_spec_lu.alias("MCTR_SPEC_LU"), F.col("Claims.CLCL_ID") == F.col("MCTR_SPEC_LU.CLCL_ID"), "left")
    .join(df_snap_lob_lu.alias("SNAP_LOB_LU"), F.col("Claims.CLCL_ID") == F.col("SNAP_LOB_LU.CLCL_ID"), "left")
    .join(df_trad_pard_lu.alias("TRAD_PARD_LU"), F.col("Claims.CLCL_ID") == F.col("TRAD_PARD_LU.CLCL_ID"), "left")
    .join(df_clhs_lu.alias("CLHS_LU"), F.col("Claims.CLCL_ID") == F.col("CLHS_LU.CLCL_ID"), "left")
    .join(df_remit_supr_med_lu.alias("REMIT_SUPR_MED_LU"), F.col("Claims.CLCL_ID") == F.col("REMIT_SUPR_MED_LU.CLCL_ID"), "left")
    .join(df_remit_supr_dntl_lu.alias("REMIT_SUPR_DNTL_LU"), F.col("Claims.CLCL_ID") == F.col("REMIT_SUPR_DNTL_LU.CLCL_ID"), "left")
    .join(df_refLOBD_ID.alias("refLOBD_ID"), F.col("Claims.PDPD_ID") == F.col("refLOBD_ID.PDPD_ID"), "left")
)

df_stripfields_filtered = df_stripfields_joined.filter(
    F.length(F.trim(F.col("Claims.CLCL_ID"))) > 0
)

svLowSvcDt_expr = F.col("Claims.CLCL_LOW_SVC_DT")
svhsaclaim_expr = F.when(F.col("CLHS_LU.MEME_CK").isNotNull(), F.lit("Y")).otherwise(F.lit("N"))

df_stripfields_out = df_stripfields_filtered.select(
    F.rpad(F.col("Claims.CLCL_ID"), 12, " ").alias("CLCL_ID"),
    F.col("Claims.MEME_CK").alias("MEME_CK"),
    F.col("Claims.GRGR_CK").alias("GRGR_CK"),
    F.col("Claims.SBSB_CK").alias("SBSB_CK"),
    F.col("Claims.SGSG_CK").alias("SGSG_CK"),
    F.rpad(F.col("Claims.CLCL_CL_TYPE"), 1, " ").alias("CLCL_CL_TYPE"),
    F.rpad(F.col("Claims.CLCL_CL_SUB_TYPE"), 1, " ").alias("CLCL_CL_SUB_TYPE"),
    F.rpad(F.col("Claims.CLCL_PRE_PRICE_IND"), 1, " ").alias("CLCL_PRE_PRICE_IND"),
    F.rpad(F.col("Claims.CLCL_CUR_STS"), 2, " ").alias("CLCL_CUR_STS"),
    F.col("Claims.CLST_SEQ_NO").alias("CLST_SEQ_NO"),
    F.rpad(F.col("Claims.CLCL_SITE"), 2, " ").alias("CLCL_SITE"),
    F.col("Claims.CLCL_LAST_ACT_DTM").alias("CLCL_LAST_ACT_DT"),
    F.col("Claims.CLCL_INPUT_DT").alias("CLCL_INPUT_DT"),
    F.col("Claims.CLCL_RECD_DT").alias("CLCL_RECD_DT"),
    F.col("Claims.CLCL_ACPT_DTM").alias("CLCL_ACPT_DTM"),
    F.col("Claims.CLCL_PAID_DT").alias("CLCL_PAID_DT"),
    F.col("Claims.CLCL_NEXT_REV_DT").alias("CLCL_NEXT_REV_DT"),
    F.col("Claims.CLCL_LOW_SVC_DT").alias("CLCL_LOW_SVC_DT"),
    F.col("Claims.CLCL_HIGH_SVC_DT").alias("CLCL_HIGH_SVC_DT"),
    F.rpad(F.col("Claims.CLCL_ID_ADJ_TO"), 12, " ").alias("CLCL_ID_ADJ_TO"),
    F.rpad(F.col("Claims.CLCL_ID_ADJ_FROM"), 12, " ").alias("CLCL_ID_ADJ_FROM"),
    F.rpad(F.col("Claims.CSPD_CAT"), 1, " ").alias("CSPD_CAT"),
    F.rpad(F.col("Claims.PZAP_ID"), 4, " ").alias("PZAP_ID"),
    F.rpad(F.col("Claims.CSCS_ID"), 4, " ").alias("CSCS_ID"),
    F.rpad(F.col("Claims.CSPI_ID"), 8, " ").alias("CSPI_ID"),
    F.rpad(F.col("Claims.PDPD_ID"), 8, " ").alias("PDPD_ID"),
    F.rpad(F.col("Claims.MEPE_FI"), 1, " ").alias("MEPE_FI"),
    F.col("Claims.MEPE_PLAN_ENTRY_DT").alias("MEPE_PLAN_ENTRY_DT"),
    F.rpad(F.col("Claims.CLCL_COBRA_IND"), 1, " ").alias("CLCL_COBRA_IND"),
    F.col("Claims.CLCL_ME_AGE").alias("CLCL_ME_AGE"),
    F.rpad(F.col("Claims.MEME_SEX"), 1, " ").alias("MEME_SEX"),
    F.rpad(F.col("Claims.MEME_RECORD_NO"), 11, " ").alias("MEME_RECORD_NO"),
    F.rpad(F.col("Claims.MEME_HICN"), 12, " ").alias("MEME_HICN"),
    F.rpad(F.col("Claims.NWPE_PFX"), 4, " ").alias("NWPE_PFX"),
    F.rpad(F.col("Claims.NWPR_PFX"), 4, " ").alias("NWPR_PFX"),
    F.rpad(F.col("Claims.NWCR_PFX"), 4, " ").alias("NWCR_PFX"),
    F.rpad(F.col("Claims.PDBC_PFX_NPPR"), 4, " ").alias("PDBC_PFX_NPPR"),
    F.rpad(F.col("Claims.PDBC_PFX_SEDF"), 4, " ").alias("PDBC_PFX_SEDF"),
    F.rpad(F.col("Claims.PRAC_PFX"), 4, " ").alias("PRAC_PFX"),
    F.rpad(F.col("Claims.PCAG_PFX"), 4, " ").alias("PCAG_PFX"),
    F.rpad(F.col("Claims.NWNW_ID"), 12, " ").alias("NWNW_ID"),
    F.rpad(F.col("Claims.AGAG_ID"), 12, " ").alias("AGAG_ID"),
    F.rpad(F.col("Claims.CLCL_MED_ASSGN_IND"), 1, " ").alias("CLCL_MED_ASSIGN_IND"),
    F.rpad(F.col("Claims.CLCL_PAY_PR_IND"), 1, " ").alias("CLCL_PAY_PR_IND"),
    F.rpad(F.col("Claims.CLCL_PAYEE_PR_ID"), 12, " ").alias("CLCL_PAYEE_PR_ID"),
    F.rpad(F.col("Claims.CLCL_REL_INFO_IND"), 1, " ").alias("CLCL_REL_INFO_IND"),
    F.rpad(F.col("Claims.CLCL_OTHER_BN_IND"), 1, " ").alias("CLCL_OTHER_BN_IND"),
    F.rpad(F.col("Claims.CLCL_ACD_IND"), 1, " ").alias("CLCL_ACD_IND"),
    F.rpad(F.col("Claims.CLCL_ACD_STATE"), 2, " ").alias("CLCL_ACD_STATE"),
    F.col("Claims.CLCL_ACD_DT").alias("CLCL_ACD_DT"),
    F.col("Claims.CLCL_ACD_AMT").alias("CLCL_ACD_AMT"),
    F.col("Claims.CLCL_CURR_ILL_DT").alias("CLCL_CURR_ILL_DT"),
    F.col("Claims.CLCL_SIMI_ILL_DT").alias("CLCL_SIMI_ILL_DT"),
    F.rpad(F.col("Claims.PRPR_ID"), 12, " ").alias("PRPR_ID"),
    F.rpad(F.col("Claims.CLCL_PR_SS_EIN_IND"), 1, " ").alias("CLCL_PR_SS_EIN_IND"),
    F.rpad(F.col("Claims.CLCL_NTWK_IND"), 1, " ").alias("CLCL_NTWK_IND"),
    F.rpad(F.col("Claims.CLCL_PCP_IND"), 1, " ").alias("CLCL_PCP_IND"),
    F.rpad(F.col("Claims.CLCL_PRPR_ID_PCP"), 12, " ").alias("CLCL_PRPR_ID_PCP"),
    F.rpad(F.col("Claims.CLCL_PRPR_ID_REF"), 12, " ").alias("CLCL_PRPR_ID_REF"),
    F.col("Claims.CLCL_PA_ACCT_NO").alias("CLCL_PA_ACCT_NO"),
    F.rpad(F.col("Claims.CLCL_EXT_AUTH_NO"), 18, " ").alias("CLCL_EXT_AUTH_NO"),
    F.col("Claims.CLCL_PA_PAID_AMT").alias("CLCL_PA_PAID_AMT"),
    F.col("Claims.CLCL_TOT_CHG").alias("CLCL_TOT_CHG"),
    F.col("Claims.CLCL_TOT_PAYABLE").alias("CLCL_TOT_PAYABLE"),
    F.rpad(F.col("Claims.CLCL_DRAG_OR_IND"), 1, " ").alias("CLCL_DRAG_OR_IND"),
    F.col("Claims.CLCL_DRAG_DT").alias("CLCL_DRAG_DT"),
    F.rpad(F.col("Claims.CLCL_INPUT_METH"), 1, " ").alias("CLCL_INPUT_METH"),
    F.rpad(F.col("Claims.CLCL_AIAI_EOB_IND"), 1, " ").alias("CLCL_AIAI_EOB_IND"),
    F.rpad(F.col("Claims.CLCL_EOB_EXCD_ID"), 3, " ").alias("CLCL_EOB_EXCD_ID"),
    F.rpad(F.col("Claims.CLCL_BATCH_ID"), 8, " ").alias("CLCL_BATCH_ID"),
    F.rpad(F.col("Claims.CLCL_BATCH_ACTION"), 1, " ").alias("CLCL_BATCH_ACTION"),
    F.rpad(F.col("Claims.CLCL_CE_IND"), 1, " ").alias("CLCL_CE_IND"),
    F.rpad(F.col("Claims.CLCL_CAP_IND"), 1, " ").alias("CLCL_CAP_IND"),
    F.rpad(F.col("Claims.CLCL_AG_SOURCE"), 1, " ").alias("CLCL_AG_SOURCE"),
    F.rpad(F.col("Claims.PDDS_PROD_TYPE"), 1, " ").alias("PDDS_PROD_TYPE"),
    F.rpad(F.col("Claims.PDDS_MCTR_BCAT"), 4, " ").alias("PDDS_MCTR_BCAT"),
    F.rpad(F.col("Claims.CLCL_MICRO_ID"), 18, " ").alias("CLCL_MICRO_ID"),
    F.col("Claims.CLCL_UNABL_FROM_DT").alias("CLCL_UNABL_FROM_DT"),
    F.col("Claims.CLCL_UNABL_TO_DT").alias("CLCL_UNABL_TO_DT"),
    F.col("Claims.CLCL_RELHP_FROM_DT").alias("CLCL_RELHP_FROM_DT"),
    F.col("Claims.CLCL_RELHP_TO_DT").alias("CLCL_RELHP_TO_DT"),
    F.rpad(F.col("Claims.CLCL_OUT_LAB_IND"), 1, " ").alias("CLCL_OUT_LAB_IND"),
    F.rpad(F.col("Claims.CLCL_MECD_RESUB_NO"), 15, " ").alias("CLCL_MECD_RESUB_NO"),
    F.rpad(F.col("Claims.PRAD_TYPE"), 3, " ").alias("PRAD_TYPE"),
    F.rpad(F.col("Claims.CLCL_PCA_IND"), 1, " ").alias("CLCL_PCA_IND"),
    F.rpad(F.col("Claims.CLCL_OOA_IND"), 1, " ").alias("CLCL_OOA_IND"),
    F.rpad(F.col("Claims.CLCL_EXT_REF_IND"), 1, " ").alias("CLCL_EXT_REF_IND"),
    F.rpad(F.col("Claims.CLCL_RAD_ENC_IND"), 1, " ").alias("CLCL_RAD_ENC_IND"),
    F.rpad(F.col("Claims.CLCL_REC_ENC_IND"), 1, " ").alias("CLCL_REC_ENC_IND"),
    F.rpad(F.col("Claims.CLCL_COB_EOB_IND"), 1, " ").alias("CLCL_COB_EOB_IND"),
    F.col("Claims.CLCL_LOCK_TOKEN").alias("CLCL_LOCK_TOKEN"),
    F.col("Claims.ATXR_SOURCE_ID").alias("ATXR_SOURCE_ID"),
    F.rpad(F.col("Claims.GRGR_ID"), 8, " ").alias("GRGR_ID"),
    F.rpad(F.col("Claims.SGSG_ID"), 4, " ").alias("SGSG_ID"),
    F.col("Claims.MEME_SFX").alias("MEME_SFX"),
    F.rpad(F.col("Claims.SBSB_ID"), 9, " ").alias("SBSB_ID"),
    F.rpad(
        F.when(F.col("EXP_CAT.PDBL_EXP_CAT").isNull(), F.lit("NA")).otherwise(F.col("EXP_CAT.PDBL_EXP_CAT")),
        4, " "
    ).alias("PDBL_EXP_CAT"),
    F.rpad(
        F.when(F.col("MCTR_TYP_LU.PRPR_MCTR_TYPE").isNotNull(), F.trim(F.col("MCTR_TYP_LU.PRPR_MCTR_TYPE"))).otherwise(F.lit("")),
        4, " "
    ).alias("PRPR_MCTR_TYPE"),
    F.rpad(
        F.when(F.col("MCTR_SPEC_LU.PRCF_MCTR_SPEC").isNotNull(), F.trim(F.col("MCTR_SPEC_LU.PRCF_MCTR_SPEC"))).otherwise(F.lit("")),
        4, " "
    ).alias("PRCF_MCTR_SPEC"),
    F.rpad(
        F.when(F.col("SNAP_LOB_LU.PDBL_ACCT_CAT").isNull(), F.lit("UNK")).otherwise(F.trim(F.col("SNAP_LOB_LU.PDBL_ACCT_CAT"))),
        4, " "
    ).alias("PDBL_ACCT_CAT"),
    F.rpad(
        F.when(F.col("MCTR_TYP_LU.PRPR_ENTITY").isNull(), F.lit(" ")).otherwise(F.col("MCTR_TYP_LU.PRPR_ENTITY")),
        1, " "
    ).alias("PRPR_ENTITY"),
    F.expr("PCA.TYP.CD.ROUTINE(svLowSvcDt_expr, Claims.CLCL_ID, Claims.SESE_ID, Claims.CDML_SEQ_NO, Claims.SESE_RULE, Claims.PRPR_ID, svhsaclaim_expr)").alias("PCA_TYP_CD"),
    F.lit("1").alias("REL_PCA_CLM_SK"),  # DS used "WhereExpression": 1
    F.rpad(
        F.when(F.col("TRAD_PARD_LU.CLED_TRAD_PARTNER").isNull(), F.lit(" ")).otherwise(F.col("TRAD_PARD_LU.CLED_TRAD_PARTNER")),
        15, " "
    ).alias("CLED_TRAD_PARTNER"),
    F.rpad(F.col("Claims.SESE_ID"), 4, " ").alias("SESE_ID"),
    F.col("Claims.CDML_SEQ_NO").alias("CDML_SEQ_NO"),
    F.when(
        F.col("Claims.CLCL_CL_TYPE") == F.lit("D"),
        F.when(
            F.col("REMIT_SUPR_DNTL_LU.REMIT_PR_SUPRSION_AMT").isNull(),
            F.lit(0)
        ).otherwise(F.col("REMIT_SUPR_DNTL_LU.REMIT_PR_SUPRSION_AMT") + F.col("REMIT_SUPR_DNTL_LU.REMIT_SB_SUPRSION_AMT"))
    ).otherwise(
        F.when(
            F.col("REMIT_SUPR_MED_LU.REMIT_PR_SUPRSION_AMT").isNull(),
            F.lit(0)
        ).otherwise(F.col("REMIT_SUPR_MED_LU.REMIT_PR_SUPRSION_AMT") + F.col("REMIT_SUPR_MED_LU.REMIT_SB_SUPRSION_AMT"))
    ).alias("REMIT_SUPRSION_AMT"),
    F.rpad(
        F.when(F.col("refLOBD_ID.LOBD_ID").isNull(), F.lit(" ")).otherwise(F.col("refLOBD_ID.LOBD_ID")),
        4, " "
    ).alias("LOBD_ID"),
    F.when(
        F.col("Claims.TAXONOMY_INDICATOR") == 1,
        F.when(
            (F.trim(F.col("Claims.CLPP_TAXONOMY_CD")) == "") | 
            (F.trim(F.col("Claims.CLPP_TAXONOMY_CD")) == "NA") |
            (F.trim(F.col("Claims.CLPP_TAXONOMY_CD")) == "UNK") |
            (F.trim(F.col("Claims.CLPP_TAXONOMY_CD")).isNull()),
            F.lit("")
        ).otherwise(F.trim(F.col("Claims.CLPP_TAXONOMY_CD")))
    ).otherwise(
        F.when(
            F.col("Claims.TAXONOMY_INDICATOR") == 0,
            F.when(
                (F.trim(F.col("Claims.CLMF_INPUT_TXNM_CD")) == "") |
                (F.trim(F.col("Claims.CLMF_INPUT_TXNM_CD")) == "NA") |
                (F.trim(F.col("Claims.CLMF_INPUT_TXNM_CD")) == "UNK") |
                (F.trim(F.col("Claims.CLMF_INPUT_TXNM_CD")).isNull()),
                F.lit("")
            ).otherwise(F.trim(F.col("Claims.CLMF_INPUT_TXNM_CD")))
        ).otherwise(F.lit(""))
    ).alias("CLM_TXNMY_CD")
)

# Send this output to the container stage "contHashFiles" (named "contHashFiles" in the JSON):
params_contHashFiles = {}
df_Extract = contHashFiles(df_stripfields_out, params_contHashFiles)

# Next container "contTransform"
params_contTransform = {}
df_populated_CRF = contTransform(df_Extract, params_contTransform)

# Next container "contReversalLogic"
params_contReversalLogic = {}
df_Transform = contReversalLogic(df_populated_CRF, params_contReversalLogic)

# Next: "Snapshot" stage with 2 outputs: "Pkey" -> "ClmPK" container, and "Snapshot" -> next Transformer
# We'll build two DataFrames from df_Transform. 
df_snapshot_pkey = df_Transform.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.lit("FACETS").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("CLM_SK").alias("CLM_SK"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.rpad(F.col("CLM_ID"), 18, " ").alias("CLM_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.rpad(F.col("ADJ_FROM_CLM"), 12, " ").alias("ADJ_FROM_CLM"),
    F.rpad(F.col("ADJ_TO_CLM"), 12, " ").alias("ADJ_TO_CLM"),
    F.rpad(F.col("ALPHA_PFX_CD"), 3, " ").alias("ALPHA_PFX_CD"),
    F.rpad(F.col("CLM_EOB_EXCD"), 3, " ").alias("CLM_EOB_EXCD"),
    F.rpad(F.col("CLS"), 4, " ").alias("CLS"),
    F.rpad(F.col("CLS_PLN"), 8, " ").alias("CLS_PLN"),
    F.rpad(F.col("EXPRNC_CAT"), 4, " ").alias("EXPRNC_CAT"),
    F.rpad(F.col("FNCL_LOB_NO"), 4, " ").alias("FNCL_LOB_NO"),
    F.rpad(F.col("GRP"), 8, " ").alias("GRP"),
    F.col("MBR_CK").alias("MBR_CK"),
    F.rpad(F.col("NTWK"), 12, " ").alias("NTWK"),
    F.rpad(F.col("PROD"), 8, " ").alias("PROD"),
    F.rpad(F.col("SUBGRP"), 4, " ").alias("SUBGRP"),
    F.col("SUB_CK").alias("SUB_CK"),
    F.rpad(F.col("CLM_ACDNT_CD"), 10, " ").alias("CLM_ACDNT_CD"),
    F.rpad(F.col("CLM_ACDNT_ST_CD"), 2, " ").alias("CLM_ACDNT_ST_CD"),
    F.rpad(F.col("CLM_ACTIVATING_BCBS_PLN_CD"), 10, " ").alias("CLM_ACTIVATING_BCBS_PLN_CD"),
    F.rpad(F.col("CLM_AGMNT_SRC_CD"), 10, " ").alias("CLM_AGMNT_SRC_CD"),
    F.rpad(F.col("CLM_BTCH_ACTN_CD"), 1, " ").alias("CLM_BTCH_ACTN_CD"),
    F.rpad(F.col("CLM_CAP_CD"), 10, " ").alias("CLM_CAP_CD"),
    F.rpad(F.col("CLM_CAT_CD"), 10, " ").alias("CLM_CAT_CD"),
    F.rpad(F.col("CLM_CHK_CYC_OVRD_CD"), 1, " ").alias("CLM_CHK_CYC_OVRD_CD"),
    F.rpad(F.col("CLM_COB_CD"), 1, " ").alias("CLM_COB_CD"),
    F.col("FINL_DISP_CD").alias("FINL_DISP_CD"),
    F.rpad(F.col("CLM_INPT_METH_CD"), 1, " ").alias("CLM_INPT_METH_CD"),
    F.rpad(F.col("CLM_INPT_SRC_CD"), 10, " ").alias("CLM_INPT_SRC_CD"),
    F.rpad(F.col("CLM_IPP_CD"), 10, " ").alias("CLM_IPP_CD"),
    F.rpad(F.col("CLM_NTWK_STTUS_CD"), 2, " ").alias("CLM_NTWK_STTUS_CD"),
    F.rpad(F.col("CLM_NONPAR_PROV_PFX_CD"), 4, " ").alias("CLM_NONPAR_PROV_PFX_CD"),
    F.rpad(F.col("CLM_OTHER_BNF_CD"), 1, " ").alias("CLM_OTHER_BNF_CD"),
    F.rpad(F.col("CLM_PAYE_CD"), 1, " ").alias("CLM_PAYE_CD"),
    F.rpad(F.col("CLM_PRCS_CTL_AGNT_PFX_CD"), 4, " ").alias("CLM_PRCS_CTL_AGNT_PFX_CD"),
    F.rpad(F.col("CLM_SVC_DEFN_PFX_CD"), 4, " ").alias("CLM_SVC_DEFN_PFX_CD"),
    F.rpad(F.col("CLM_SVC_PROV_SPEC_CD"), 10, " ").alias("CLM_SVC_PROV_SPEC_CD"),
    F.rpad(F.col("CLM_SVC_PROV_TYP_CD"), 10, " ").alias("CLM_SVC_PROV_TYP_CD"),
    F.rpad(F.col("CLM_STTUS_CD"), 2, " ").alias("CLM_STTUS_CD"),
    F.rpad(F.col("CLM_SUBMT_BCBS_PLN_CD"), 10, " ").alias("CLM_SUBMT_BCBS_PLN_CD"),
    F.rpad(F.col("CLM_SUB_BCBS_PLN_CD"), 10, " ").alias("CLM_SUB_BCBS_PLN_CD"),
    F.rpad(F.col("CLM_SUBTYP_CD"), 10, " ").alias("CLM_SUBTYP_CD"),
    F.rpad(F.col("CLM_TYP_CD"), 1, " ").alias("CLM_TYP_CD"),
    F.rpad(F.col("ATCHMT_IN"), 1, " ").alias("ATCHMT_IN"),
    F.rpad(F.col("CLM_CLNCL_EDIT_CD"), 1, " ").alias("CLM_CLNCL_EDIT_CD"),
    F.rpad(F.col("COBRA_CLM_IN"), 1, " ").alias("COBRA_CLM_IN"),
    F.rpad(F.col("FIRST_PASS_IN"), 1, " ").alias("FIRST_PASS_IN"),
    F.rpad(F.col("HOST_IN"), 1, " ").alias("HOST_IN"),
    F.rpad(F.col("LTR_IN"), 1, " ").alias("LTR_IN"),
    F.rpad(F.col("MCARE_ASG_IN"), 1, " ").alias("MCARE_ASG_IN"),
    F.rpad(F.col("NOTE_IN"), 1, " ").alias("NOTE_IN"),
    F.rpad(F.col("PCA_AUDIT_IN"), 1, " ").alias("PCA_AUDIT_IN"),
    F.rpad(F.col("PCP_SUBMT_IN"), 1, " ").alias("PCP_SUBMT_IN"),
    F.rpad(F.col("PROD_OOA_IN"), 1, " ").alias("PROD_OOA_IN"),
    F.rpad(F.col("ACDNT_DT"), 10, " ").alias("ACDNT_DT"),
    F.rpad(F.col("INPT_DT"), 10, " ").alias("INPT_DT"),
    F.rpad(F.col("MBR_PLN_ELIG_DT"), 10, " ").alias("MBR_PLN_ELIG_DT"),
    F.rpad(F.col("NEXT_RVW_DT"), 10, " ").alias("NEXT_RVW_DT"),
    F.rpad(F.col("PD_DT"), 10, " ").alias("PD_DT"),
    F.rpad(F.col("PAYMT_DRAG_CYC_DT"), 10, " ").alias("PAYMT_DRAG_CYC_DT"),
    F.rpad(F.col("PRCS_DT"), 10, " ").alias("PRCS_DT"),
    F.rpad(F.col("RCVD_DT"), 10, " ").alias("RCVD_DT"),
    F.rpad(F.col("SVC_STRT_DT"), 10, " ").alias("SVC_STRT_DT"),
    F.rpad(F.col("SVC_END_DT"), 10, " ").alias("SVC_END_DT"),
    F.rpad(F.col("SMLR_ILNS_DT"), 10, " ").alias("SMLR_ILNS_DT"),
    F.rpad(F.col("STTUS_DT"), 10, " ").alias("STTUS_DT"),
    F.rpad(F.col("WORK_UNABLE_BEG_DT"), 10, " ").alias("WORK_UNABLE_BEG_DT"),
    F.rpad(F.col("WORK_UNABLE_END_DT"), 10, " ").alias("WORK_UNABLE_END_DT"),
    F.col("ACDNT_AMT").alias("ACDNT_AMT"),
    F.col("ACTL_PD_AMT").alias("ACTL_PD_AMT"),
    F.col("ALLOW_AMT").alias("ALLOW_AMT"),
    F.col("DSALW_AMT").alias("DSALW_AMT"),
    F.col("COINS_AMT").alias("COINS_AMT"),
    F.col("CNSD_CHRG_AMT").alias("CNSD_CHRG_AMT"),
    F.col("COPAY_AMT").alias("COPAY_AMT"),
    F.col("CHRG_AMT").alias("CHRG_AMT"),
    F.col("DEDCT_AMT").alias("DEDCT_AMT"),
    F.col("PAYBL_AMT").alias("PAYBL_AMT"),
    F.col("CLM_CT").alias("CLM_CT"),
    F.col("MBR_AGE").alias("MBR_AGE"),
    F.col("ADJ_FROM_CLM_ID").alias("ADJ_FROM_CLM_ID"),
    F.col("ADJ_TO_CLM_ID").alias("ADJ_TO_CLM_ID"),
    F.rpad(F.col("DOC_TX_ID"), 18, " ").alias("DOC_TX_ID"),
    F.rpad(F.col("MCAID_RESUB_NO"), 15, " ").alias("MCAID_RESUB_NO"),
    F.rpad(F.col("MCARE_ID"), 12, " ").alias("MCARE_ID"),
    F.rpad(F.col("MBR_SFX_NO"), 2, " ").alias("MBR_SFX_NO"),
    F.col("PATN_ACCT_NO").alias("PATN_ACCT_NO"),
    F.rpad(F.col("PAYMT_REF_ID"), 16, " ").alias("PAYMT_REF_ID"),
    F.rpad(F.col("PROV_AGMNT_ID"), 12, " ").alias("PROV_AGMNT_ID"),
    F.col("RFRNG_PROV_TX").alias("RFRNG_PROV_TX"),
    F.rpad(F.col("SUB_ID"), 14, " ").alias("SUB_ID"),
    F.rpad(F.col("PRPR_ENTITY"), 1, " ").alias("PRPR_ENTITY"),
    F.rpad(F.col("PCA_TYP_CD"), 18, " ").alias("PCA_TYP_CD"),
    F.col("REL_PCA_CLM_ID").alias("REL_PCA_CLM_ID"),
    F.col("CLCL_MICRO_ID").alias("CLCL_MICRO_ID"),
    F.rpad(F.col("CLM_UPDT_SW"), 1, " ").alias("CLM_UPDT_SW"),
    F.col("REMIT_SUPRSION_AMT").alias("REMIT_SUPRSION_AMT"),
    F.col("MCAID_STTUS_ID").alias("MCAID_STTUS_ID"),
    F.col("PATN_PD_AMT").alias("PATN_PD_AMT"),
    F.col("CLM_SUBMT_ICD_VRSN_CD").alias("CLM_SUBMT_ICD_VRSN_CD"),
    F.col("CLM_TXNMY_CD").alias("CLM_TXNMY_CD"),
    F.rpad(F.lit("N"), 1, " ").alias("BILL_PAYMT_EXCL_IN")
)

df_snapshot_snapshot = df_Transform.select(
    F.rpad(F.col("CLM_ID"), 12, " ").alias("CLM_ID"),
    F.col("MBR_CK").alias("MBR_CK"),
    F.rpad(F.col("GRP"), 8, " ").alias("GRP"),
    F.col("SVC_STRT_DT").alias("SVC_STRT_DT"),
    F.col("CHRG_AMT").alias("CHRG_AMT"),
    F.col("PAYBL_AMT").alias("PAYBL_AMT"),
    F.rpad(F.col("EXPRNC_CAT"), 4, " ").alias("EXPRNC_CAT"),
    F.rpad(F.col("FNCL_LOB_NO"), 4, " ").alias("FNCL_LOB_NO"),
    F.col("CLM_CT").alias("CLM_CT"),
    F.rpad(F.col("PCA_TYP_CD"), 18, " ").alias("PCA_TYP_CD"),
    F.rpad(F.col("CLM_STTUS_CD"), 2, " ").alias("CLM_STTUS_CD"),
    F.rpad(F.col("CLM_CAT_CD"), 10, " ").alias("CLM_CAT_CD")
)

# Now we have two output streams. Next stage: "ClmPK" container consumes df_snapshot_pkey => "Key"
params_clmPK = {"CurrRunCycle": CurrRunCycle}
df_key = ClmPK(df_snapshot_pkey, params_clmPK)

# Next stage writes "FctsClmExtr"
write_files(
    df_key,
    f"{adls_path}/key/FctsClmExtr.FctsClm.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# The other link from snapshot is "df_snapshot_snapshot" to "Transformer" => "RowCount" => "B_CLM"
# The Transformer logic:
df_transformer_out = df_snapshot_snapshot.select(
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("MBR_CK").alias("MBR_CK"),
    F.col("GRP").alias("GRP"),
    F.col("SVC_STRT_DT").alias("SVC_STRT_DT"),
    F.col("CHRG_AMT").alias("CHRG_AMT"),
    F.col("PAYBL_AMT").alias("PAYBL_AMT"),
    F.col("EXPRNC_CAT").alias("EXPRNC_CAT"),
    F.col("FNCL_LOB_NO").alias("FNCL_LOB_NO"),
    F.col("CLM_CT").alias("CLM_CT"),
    F.col("PCA_TYP_CD").alias("PCA_TYP_CD"),
    F.col("CLM_STTUS_CD").alias("CLM_STTUS_CD"),
    F.col("CLM_CAT_CD").alias("CLM_CAT_CD")
)
df_B_CLM = df_transformer_out.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.lit(None).cast("int").alias("CLM_STTUS_CD_SK"),  # Because of stage variables we do not have direct mapping, so null or special
    F.lit(None).cast("int").alias("CLM_CAT_CD_SK"),
    F.lit(None).cast("int").alias("EXPRNC_CAT_SK"),
    F.lit(None).cast("int").alias("FNCL_LOB_SK"),
    F.lit(None).cast("int").alias("GRP_SK"),
    F.lit(None).cast("int").alias("MBR_SK"),
    F.rpad(F.col("SVC_STRT_DT"), 10, " ").alias("SVC_STRT_DT_SK"),
    F.col("CHRG_AMT").alias("CHRG_AMT"),
    F.col("PAYBL_AMT").alias("PAYBL_AMT"),
    F.col("CLM_CT").alias("CLM_CT"),
    F.lit(None).cast("int").alias("PCA_TYP_CD_SK")
)

write_files(
    df_B_CLM,
    f"{adls_path}/load/B_CLM.FACETS.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# No spark.stop() call as instructed.
# End of job.
# AfterJobRoutine = "1" was indicated, but not identified as DB2StatsSubroutine or any other named routine, so nothing more is called.