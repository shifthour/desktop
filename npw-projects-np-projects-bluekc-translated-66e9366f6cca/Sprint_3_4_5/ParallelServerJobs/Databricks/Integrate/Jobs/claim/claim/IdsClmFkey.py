# Databricks notebook source
# MAGIC %md
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2004, 2005, 2006, 2007, 2008, 2009, 2021, 2023  BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:  IdsClmFkey
# MAGIC CALLED BY:   IdsFctsClmLoad1Seq
# MAGIC                        IdsNascoClmLoad1Seq
# MAGIC                        IdsDrugLoadSeq   
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                Massive quantities of Stage Variables with CDMA lookups for surrogate key assignement.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer	Date		Project/Altiris #	Change Description							Development Project		Code Reviewer		Date Reviewed       
# MAGIC ======================================================================================================================================================================================================   
# MAGIC Oliver Nielsen	05/00/2004                                    		Originally Programmed
# MAGIC Oliver Nielsen	07/01/2004			Phase 1.1 Implementation - Added ALLOW_AMT to File format
# MAGIC S.Andrew		07/13/2004			Took out criteria Err Count = 0 on output
# MAGIC BJ Luce		08/2004				Phase 2.0 - add transform SrcSysCdMbrFkey for foreign keying of class 
# MAGIC 						plan, class, group, member, subgroup and subscriber
# MAGIC 						Add transform SrcSysCdProdFkey for foreign keying of product and
# MAGIC 						experience category
# MAGIC Ralph Tucker	09/29/2006			Added LAST_UPDT_RUN_CYC_EXCTN_SK to the 
# MAGIC 						Nasco_Edw_Rvrsl_Clm_Hit_List with a default value of Zero
# MAGIC Brent Leland	09/09/2004                                  		Added InterProc stage for 4 streams
# MAGIC                                                                                     			Added default rows for UNK and NA
# MAGIC O. Nielsen		09/24/2004                                  		Hardcoded ClmAcdntStCd Lookup to 'IDS" as the source.
# MAGIC O. Nielsen		10/01/2004                                   		Change FKey Lookup for SVC_PROV_TYP to look at CLM_SUBTYPE to 
# MAGIC 						determine domain to look up in CDMA
# MAGIC Brent Leland	10/08/2004                                   		Changed the logging indicator to X for class, class plan, and subgroup foriegn
# MAGIC 						key lookups.
# MAGIC Brent Leland	12/27/2004                                    		Changed ACDNT_IN field from char(1) to char(2) since it has changed from
# MAGIC 						indicator to code.
# MAGIC                                                                                      			Removed COB_PD_AMT, COB_OTHR_CAR_ALW_AMT, ITS_ACES_FEE_AMT, 
# MAGIC 						ITS_ADM_FEE_AMT, ITS_DRG_AMT, ITS_SCCF_NO columns.
# MAGIC Steph Goddard	01/06/2005                                  		changes for IDS 3.0 - moved fields around
# MAGIC Ralph Tucker	01/27/2005                                   		Changed GetFkeyClsPln reducing the number of parameters to accomadate
# MAGIC 						Version 3.0 changes.
# MAGIC SAndrew		02/16/2005                                    		Removed warning text box '******  ATTENTION *****
# MAGIC                                                                                      			Code was put in to handle Service Provider Type and Spec.   This should be 
# MAGIC 						changed when Version 3.0 is worked on!!!!    Look at Stage Variables 
# MAGIC 
# MAGIC Developer	Date		Project/Altiris #	Change Description							Development Project		Code Reviewer		Date Reviewed       
# MAGIC ======================================================================================================================================================================================================   
# MAGIC Steph Goddard	03/02/2005                                    		Changed Fkey lookup for Fncl_Lob to use "PSI" for source system
# MAGIC BJ Luce		03/15/2005                                    		create stage variable svAlphaPfxSrcSysCd to use FACETS for ARGUS and PCS in
# MAGIC 						foreign key lookup
# MAGIC BJ Luce		03/22/2005                                    		Add HOST_IN
# MAGIC Hall		03/23/2005                                    		Add lookup for ITS Host Codes for COB type
# MAGIC Ralph Tucker	05/18/2005                                    		Took out field key indicator on Partitioner output (CLM_SK).
# MAGIC BJ Luce		05/21/2005                                    		remove status code sk as part of the key for claim reversal updates and put in reference 
# MAGIC 						lookups to only pass claims that exist on CLM to the reversal update 
# MAGIC Steph Goddard	05/26/2005                                    		Made BJ's change to the last transform
# MAGIC Ralph Tucker	09/07/2005                                    		Chaged Alpha Prefix from CDMA to table lookup for FKey SK.
# MAGIC Steph Goddard	03/11/2006                                   		changes for sequencer
# MAGIC BJ Luce		03/13/2006                                   		add hf_clm_nasco_dup to get the Nasco claim id for claims on this hash file
# MAGIC                                                                                     			add hf_clm_nasco_orig to get the facets data needed to update the Nasco claim that 
# MAGIC 						has been adjusted in Facets - update direct to CLM 
# MAGIC Ralph Tucker	03/22/2006                                   		Added two hit lists of reversal claims to be appended to the Claim Mart and Edw 
# MAGIC 						claims hit lists.
# MAGIC Steph Goddard	03/31/2006                                  		changes for service provider type code - different logic for facets and nasco - 
# MAGIC 						corrected facets logic
# MAGIC BJ Luce		04/11/2006                                  		change logic for updating the Nasco claim reversed.
# MAGIC Steph Goddard	04/27/2006                                 		remove cdma lookup stage variable for alpha prefix - not being used as it's doing 
# MAGIC 						fkey lookup now (see 9/7/2005 comment)
# MAGIC Brent Leland	05/22/2006                                  		Put missing hit list logic back in
# MAGIC Brent Leland	07/19/2006                                  		Removed Claim mart hit list creation.  Not required with run cycle processing.
# MAGIC Brent Leland	08/07/2006                                  		Added current run cycle to output records.  Without the current run cycle, error recycle 
# MAGIC 						records would have an old run cycle and would be removed in delete process.
# MAGIC                                                                                    			Added RunID parameter for temp file name output.
# MAGIC Ralph Tucker	10/19/2006                                  		Added new fields with default 'NA' values:  PCA_TYPE_CD
# MAGIC                                                                                                                                 REL_PCA_CLM_SK
# MAGIC                                                                                                                                 CLCL_MICRO_ID
# MAGIC                                                                                                                                 CLM_UPDT_SW
# MAGIC Ralph Tucker	12/08/2006                                    		Added logic to update the old PCA claim with new REL_BASE_CLM_SK based up 
# MAGIC 						CLCL_MICRO_ID prefixed with a PCA in cols 1-3. 
# MAGIC 
# MAGIC Developer	Date		Project/Altiris #	Change Description							Development Project		Code Reviewer		Date Reviewed       
# MAGIC ======================================================================================================================================================================================================
# MAGIC Brent Leland	06/26/2007			Took out parallel processing to simplify the job.
# MAGIC 						Added error recycle file that also require additional job parameter.
# MAGIC Ralph Tucker	06/27/2007			Added logic for ARGUS to handle PCA claims.							Steph Goddard		07/18/07
# MAGIC Brent Leland	08/15/2007	IAD Prod. Supp.	Added error recycle hash file ( hf_claim_recycle_keys )			devlIDS30			Steph Goddard		08/30/07                       
# MAGIC Steph Goddard	10/23/2007	IAD Prod. Supp.	re-added logic for prov spec and prov type - it went missing!			devlIDS30			Ralph Tucker		10/18/2007
# MAGIC Laurel Kindley	12/17/2007	TTR-90		Modified source system code being passed in for lookup of sk on experience
# MAGIC 						category code and financial line of business on Nasco claims.			devlIDS30			Steph Goddard		12/18/2007
# MAGIC SAndrew		2008-10-01	#3784 PBM Vnfr	Changed called routines SrcSysCdMbrFkey and SrcSysCdProdFkey
# MAGIC 						Changed rule for stage var svProvDomain by added check for ESI		devlIDSnew		Steph Goddard		10/27/2008
# MAGIC SAndrew		2008-10-01       	#3784 PBM Vnfr	Changed called routines SrcSysCdMbrFkey and SrcSysCdProdFkey
# MAGIC 						Changed rule for stage var svProvDomain by added check for WellDyne		devlIDS			Steph Goddard		03/30/2009
# MAGIC Bhoomi D		2009-06-17	#4202		Changed logic for REL_PCA_CLM_SK & REL_BASE_CLM_SK			devlIDSnew		Steph Goddard		06/23/2009
# MAGIC Bhoomi D		2009-07-02	4202		Added PCA_Claims infront of Fkey for PCA claims missing on hf_clm		devlIDSnew		Steph Goddard		07/06/2009
# MAGIC Ralph Tucker	2009-08-13	15 - Prod Suprt	Added REMIT_SUPRSION_AMT					devlIDS			Steph Goddard		08/21/2009
# MAGIC Kalyan Neelam	2009-12-31	 4110		Added ClmCrfIn1.SRC_SYS_CD = "MCAID" in the stage variable			IntegrateCurDevl		Steph Goddard		01/11/2010
# MAGIC                                                                                   			SvcProvDomain
# MAGIC Kalyan Neelam	2010-02-09	4278		Added two new fields - MCAID_STTUS_ID,					IntegrateCurDevl		Steph Goddard		03/08/2010                  
# MAGIC 						PATN_PD_AMT                                                                                            
# MAGIC Steph Goddard	2010-11-30	ProdSupp		added hash files between transforms and link collector			IntegrateNewDevl		Brent Leland		12-08-2010
# MAGIC Kalyan Neelam	2010-12-22	4616		Added ClmCrfIn1.SRC_SYS_CD = "MEDTRAK" in the stage variable		IntegrateNewDevl		Steph Goddard		12/23/2010
# MAGIC 						SvcProvDomain
# MAGIC Manasa Andru	2012-05-04	4896 ICD10	Added new field CLM_SUBMT_ICD_VRSN_CD_SK				IntegrateNewDevl      
# MAGIC 						Remediation and defaulted to NA     
# MAGIC Kalyan Neelam 	2012-10-17	4784		Update rule for Network status code in stagte variable ClmInNtwkSk		IntegrateWrhsDevl		Bhoomi Dasari		10/20/2012
# MAGIC Kalyan Neelam 	2013-11-18	5056 FEP Claims	Added ClmCrfIn1.SRC_SYS_CD = "BCA" in the stage variable			IntegrateNewDevl		Bhoomi Dasari		11/30/2013 
# MAGIC 						SvcProvDomain
# MAGIC Kalyan Neelam	2014-12-17	5212		Added If SRC_SYS_CD = 'BCBSA' Then 'BCA' Else SRC_SYS_CD		IntegrateCurDevl		Bhoomi Dasari		02/04/2015
# MAGIC 						in the stage variables and pass it to GetFkeyCodes because code sets are created 
# MAGIC 						under BCA for BCBSA
# MAGIC 
# MAGIC Developer	Date		Project/Altiris #	Change Description							Development Project		Code Reviewer		Date Reviewed       
# MAGIC ======================================================================================================================================================================================================
# MAGIC Sudhir Bomshetty	2017-10-23	5781		Added check for SRC_SYS_CD = 'BCA' for column				IntegrateDev2		Kalyan Neelam		2017-10-23
# MAGIC 						CLM_SUBMTTING_BCBS_PLN_CD_SK
# MAGIC Manasa Andru	2017-10-30	TFS - 19834	Added condition in the Stage variables for fields -					IntegrateDev2		Kalyan Neelam		2017-10-30
# MAGIC 						CLM_ACTV_BCBS_PLN_CD_SK, CLM_SUB_BCBS_PLN_CD_SK to use
# MAGIC 						the source FACETS if it is BCA or BCBSA while populating the SK values.
# MAGIC Sudhir Bomshetty	2018-01-12	5781		Added SRC_SYS_CD = 'BCA' check for SVC_END_DT_SK				IntegrateDev2		Kalyan Neelam		2018-01-16
# MAGIC Nagesh Bandi	2018-01-10	TFS - 16786	Changed FK lookup for  CLM_PAYE_CD_SK					IntegrateDev3		Kalyan Neelam		2018-01-17
# MAGIC 						from GetFkeyCodes(svCdMpngSrcSysCd, ClmCrfIn1.CLM_SK, "CLAIM PAYEE", 
# MAGIC 						ClmCrfIn1.CLM_PAYE_CD, Logging) to 
# MAGIC 						GetFkeySrcTrgtDomainCodes(svCdMpngSrcSysCd, ClmCrfIn1.CLM_SK, "CLAIM PAYEE", 
# MAGIC 						ClmCrfIn1.CLM_PAYE_CD, "CLAIM PAYEE", Logging)
# MAGIC Manasa Andru	2018-03-05	TFS-21153	Changed staqe variable logic to check for UNK and NA before getting Fkey Value	IntegrateDev2		Kalyan Neelam		2018-03-12
# MAGIC 						svProvTypSk, svProvSpecCdSk and NtwkSk
# MAGIC Kaushik Kapoor	2018-03-16	5828		Added SrcSysCd = SAVRX, BCBSA and LDI in SvcProvDomain stage variable		IntegrateDev2		Jaideep Mankala		03/21/2018
# MAGIC Kaushik Kapoor	2018-09-20	5828		Added SrcSysCd = CVS in SvcProvDomain stage variable				IntegrateDev2		Kalyan Neelam		2018-10-01
# MAGIC Mohan Karnati	06/17/2019	ADO-73034	Adding CLM_TXNMY_CD filed in ClmCrf, hf_recycle,ClmLoadFile stageS and		IntegrateDev1		Kalyan Neelam		2019-07-01 
# MAGIC 						passing till Output stage                                                                                    
# MAGIC Karthik Chintalapani	2019-08-27	5884		Added new logic for adj_from_clm_sk field					IntegrateDev1		Kalyan Neelam		2019-09-05      
# MAGIC SRINIVAS GARIMELLA    2019-10-16	6131		Modified feilds as per the							Integrate Dev1
# MAGIC 						OptumRX transformation rules: 
# MAGIC 						ADJ_FROM_CLM_ID
# MAGIC Sagar Sayam	2019-10-21	6131 PBM Replace	Added OptumRx value under'SvcProvDomain'under the stage variables to include	Integrate Dev5		Kalyan Neelam		2019-11-20
# MAGIC 						OPTUMRX in 'Trns1' Transformer Phase.  t                                                                                                                                
# MAGIC Karthik Chintalapani	2020-03-16	5884		Modified the adj_from_clm_sk logic for BCA Medical claims in the Trns1 stage		IntegrateDev1		Kalyan Neelam		2020-03-18
# MAGIC Rekha		2020-04-13	6131  - PBM Replace	Mapped new column BILL_PAYMT_EXCL_IN				IntegrateDev2      
# MAGIC Sunitha Ganta	10-12-2020			Brought up to standards       
# MAGIC Reddy Sanam	10-13-2020			changed derivation to this Stage variable 
# MAGIC 						svCdMpngSrcSysCd to pass FACETS for
# MAGIC 						LUMERIS for type codes in the Trns1 stage               
# MAGIC Goutham Kalidindi	11/2/2020		US-283560	Added MEDIMPACT to svPROVDomain					IntegrateDev2		Reddy Sanam		2020-11-04
# MAGIC 						In Xfrm 'Trns1'
# MAGIC 
# MAGIC Developer	Date		Project/Altiris #	Change Description								Development Project	Code Reviewer		Date Reviewed       
# MAGIC ======================================================================================================================================================================================================
# MAGIC Vikas Abbu	02/04/2021	RA-DentaQuest	Added Conditions in CLM_SUBTYP_CD_SK,CLM_CAP_CD_SK,			IntegrateDev2		Kalyan Neelam		2021-02-04
# MAGIC 						CLM_TYPE_CD_SK  Column Stage Variables
# MAGIC Vikas Abbu	02/09/2021	RA-Solutran	Added SRC_SYS_CD ='SOLUTRAN' in stage variable				IntegrateDev2		Reddy Sanam		2021-02-10
# MAGIC Vikas Abbu	03/02/2021	RA-DentaQuest	Added  ClmCrfIn1.SRC_SYS_CD = 'DENTAQUEST in stagevariable			IntegrateDev2		Abhiram Dasarathy		2021-03-03   
# MAGIC Mrudula Kodali	03/07/2021	RA-EMR Procedure	Added   stage variable svEMRSrcSysCd and used in other stage variables		IntegrateDev2		Reddy Sanam		2021-03-08
# MAGIC Mrudula Kodali	03/11/2021	RA-Livongo	Added Conditions in CapClmCdSk,ClmCatCdSk,ClmSubTypCdSk			IntegrateDev2		Jaideep Mankala		03/18/2021      
# MAGIC                                                                                       	,ClmTypCdSk,ClmSttusCdSk, SvcProvDomain    Column Stage Variables    
# MAGIC Mrudula Kodali	03/24/2021	RA-Livongo	Updated the stage variable ClmFinlDispCdk and removed LVNGHLTH in this variable.	 Integrate Dev2		Hugh Sisson		2021-03-25
# MAGIC Mrudula Kodali	05/03/2021	US-373652	Updated the stage variables ClmCapCdSk, ClmPayeCdSk, ClmSubTypCdSk, ClmTypCdSk.				Jaideep Mankala		05/10/2021
# MAGIC 						Removed usages of FACETS for EYEMED, DENTAQUEST, SOLUTRON, LVNGHLTH, EMR
# MAGIC Goutham K	09/08/2021	US-428592	Lho and Facets Adjusted TO and FROM SK fix, Added a new lookup to the		IntegrateDev2		Reddy Sanam		09/08/2021              
# MAGIC 						K_CLM table to get the SK when a Lho claim gets ajusted TO a Facets claim or 
# MAGIC 						when a Facets claim gets adjusted from Lho Claim
# MAGIC Ken Bradmon	09/23/2021	us426738		In the stage "Trns1" I added a new row "MOSAICLIFECARE" for the			IntegrateDev1		Harsha Ravuri		2021-09-28	
# MAGIC 						stage variable "svEMRSrcSysCd."
# MAGIC 						That is the only change.
# MAGIC Venkata Y             10/27/2021            us438375                 In the stage "Trns1" I added a new row "HCA" for the		              		IntegrateDev2		Jeyaprasanna                         2021-10-27	
# MAGIC 						stage variable "svEMRSrcSysCd."
# MAGIC 						That is the only change.
# MAGIC 
# MAGIC Venkata Y            11/09/2021        US413785                    In the stage "Trns1" I added a new row "NONSTDSUPLMTDATA",               
# MAGIC                                                                                             "CHILDRENSMERCY"  for the	stage variable "svEMRSrcSysCd."
# MAGIC 					             That is the only change.							IntegrateDev2		Harsha Ravuri		2021-11-11		
# MAGIC 
# MAGIC Manisha Gandra    2021-11-29               US 459610            Added Nations Source System Code in "Trns1" stage variables			IntegrateDev2                    	Jeyaprasanna                          2022-01-05  
# MAGIC 
# MAGIC Ken Bradmon	2023-08-07	us559895		Added 8 new Providers to the svEMRSrcSysCd variable of the Trns1 stage.		IntegrateDev2	               Reddy Sanam                          2023-10-27	
# MAGIC 						CENTRUSHEALTH, MOHEALTH, GOLDENVALLEY, JEFFERSON, 
# MAGIC 						WESTMOMEDCNTR, BLUESPRINGS, EXCELSIOR, and HARRISONVILLE.
# MAGIC 						This is the only change.
# MAGIC 
# MAGIC Sham Shankaranarayana 2023-12-21    US 599810           Added new Stage variable svSBVSrcSysCd and updated StageVar existing 
# MAGIC                                                                                             SrcSysCdMbr, SrcSysCdProd in Transformer Trns1                                                                     IntegrateDev1                        Jeyaprasanna                         2023-12-21
# MAGIC 
# MAGIC Kshema H K         30-04-2024           US 616929                Added  filter condition in stage variable SvcProvDomain for NATIONS source system code       IntegrateDev1                        Jeyaprasanna                        2024-05-01
# MAGIC                                                                                                       in "Trns 1" Transformer stage

# MAGIC This hash file is written to by all claim foriegn key jobs.
# MAGIC Read common record format file created in primary key job.
# MAGIC Lookup the IDS claim for ADJ_FROM_CLM_SK update for  BCA  drug reversal claims
# MAGIC hf_clm_nasco_dup created in FctsNascoDupExtr
# MAGIC 
# MAGIC identifies NASCO claim that was adjusted by the Facets Nasco dup claim
# MAGIC Claim hit lists are created for appending to the  Edw hit lists.
# MAGIC This is for the 'R' rows
# MAGIC Called By  IdsFctsClmLoad1Seq
# MAGIC                      IdsNascoClmLoad1Seq
# MAGIC                      IdsDrugLoadSeq
# MAGIC                      IdsEmrProcedureClmLoadSeq
# MAGIC Update PCA field REL_BASE_CLM_SK with CLM_SK by input CLM_ID
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Utility_DS_Integrate
# COMMAND ----------
#!/usr/bin/env python

from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, when, lit, rpad, regexp_replace
from pyspark.sql import DataFrame
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
Source = get_widget_value('Source','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
InFile = get_widget_value('InFile','')
Logging = get_widget_value('Logging','')
RunCycle = get_widget_value('RunCycle','')
RunID = get_widget_value('RunID','')

# Existing code for df_clmcrf is assumed, as shown in the prompt:
column_names = [
    "JOB_EXCTN_RCRD_ERR_SK","INSRT_UPDT_CD","DISCARD_IN","PASS_THRU_IN",
    "FIRST_RECYC_DT","ERR_CT","RECYCLE_CT","SRC_SYS_CD","PRI_KEY_STRING",
    "CLM_SK","SRC_SYS_CD_SK","CLM_ID","CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK","ADJ_FROM_CLM","ADJ_TO_CLM","ALPHA_PFX_CD",
    "CLM_EOB_EXCD","CLS","CLS_PLN","EXPRNC_CAT","FNCL_LOB_NO","GRP","MBR_CK",
    "NTWK","PROD","SUBGRP","SUB_CK","CLM_ACDNT_CD","CLM_ACDNT_ST_CD",
    "CLM_ACTIVATING_BCBS_PLN_CD","CLM_AGMNT_SRC_CD","CLM_BTCH_ACTN_CD",
    "CLM_CAP_CD","CLM_CAT_CD","CLM_CHK_CYC_OVRD_CD","CLM_COB_CD","FINL_DISP_CD",
    "CLM_INPT_METH_CD","CLM_INPT_SRC_CD","CLM_IPP_CD","CLM_NTWK_STTUS_CD",
    "CLM_NONPAR_PROV_PFX_CD","CLM_OTHER_BNF_CD","CLM_PAYE_CD",
    "CLM_PRCS_CTL_AGNT_PFX_CD","CLM_SVC_DEFN_PFX_CD","CLM_SVC_PROV_SPEC_CD",
    "CLM_SVC_PROV_TYP_CD","CLM_STTUS_CD","CLM_SUBMT_BCBS_PLN_CD",
    "CLM_SUB_BCBS_PLN_CD","CLM_SUBTYP_CD","CLM_TYP_CD","ATCHMT_IN",
    "CLM_CLNCL_EDIT_CD","COBRA_CLM_IN","FIRST_PASS_IN","HOST_IN","LTR_IN",
    "MCARE_ASG_IN","NOTE_IN","PCA_AUDIT_IN","PCP_SUBMT_IN","PROD_OOA_IN",
    "ACDNT_DT","INPT_DT","MBR_PLN_ELIG_DT","NEXT_RVW_DT","PD_DT","PAYMT_DRAG_CYC_DT",
    "PRCS_DT","RCVD_DT","SVC_STRT_DT","SVC_END_DT","SMLR_ILNS_DT","STTUS_DT",
    "WORK_UNABLE_BEG_DT","WORK_UNABLE_END_DT","ACDNT_AMT","ACTL_PD_AMT",
    "ALLOW_AMT","DSALW_AMT","COINS_AMT","CNSD_CHRG_AMT","COPAY_AMT","CHRG_AMT",
    "DEDCT_AMT","PAYBL_AMT","CLM_CT","MBR_AGE","ADJ_FROM_CLM_ID","ADJ_TO_CLM_ID",
    "DOC_TX_ID","MCAID_RESUB_NO","MCARE_ID","MBR_SFX_NO","PATN_ACCT_NO","PAYMT_REF_ID",
    "PROV_AGMNT_ID","RFRNG_PROV_TX","SUB_ID","PRPR_ENTITY","PCA_TYP_CD",
    "REL_PCA_CLM_ID","CLCL_MICRO_ID","CLM_UPDT_SW","REMIT_SUPRSION_AMT","MCAID_STTUS_ID",
    "PATN_PD_AMT","CLM_SUBMT_ICD_VRSN_CD","CLM_TXNMY_CD","BILL_PAYMT_EXCL_IN"
]
schema_clmcrf = StructType([StructField(c, StringType(), True) for c in column_names])
df_clmcrf = (
    spark.read
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", '"')
    .option("escape", '"')
    .schema(schema_clmcrf)
    .csv(f"{adls_path}/{InFile}")
)
padding_spec = {
    "INSRT_UPDT_CD": 10,"DISCARD_IN": 1,"PASS_THRU_IN": 1,"CLM_ID": 18,
    "ADJ_FROM_CLM": 12,"ADJ_TO_CLM": 12,"ALPHA_PFX_CD": 3,"CLM_EOB_EXCD": 3,
    "CLS": 4,"CLS_PLN": 8,"EXPRNC_CAT": 4,"FNCL_LOB_NO": 4,"GRP": 8,
    "NTWK": 12,"PROD": 8,"SUBGRP": 4,"CLM_ACDNT_CD": 10,"CLM_ACDNT_ST_CD": 2,
    "CLM_ACTIVATING_BCBS_PLN_CD": 10,"CLM_AGMNT_SRC_CD": 10,"CLM_BTCH_ACTN_CD": 1,
    "CLM_CAP_CD": 10,"CLM_CAT_CD": 10,"CLM_CHK_CYC_OVRD_CD": 1,"CLM_COB_CD": 1,
    "CLM_INPT_METH_CD": 1,"CLM_INPT_SRC_CD": 10,"CLM_IPP_CD": 10,"CLM_NTWK_STTUS_CD": 2,
    "CLM_NONPAR_PROV_PFX_CD": 4,"CLM_OTHER_BNF_CD": 1,"CLM_PAYE_CD": 1,
    "CLM_PRCS_CTL_AGNT_PFX_CD": 4,"CLM_SVC_DEFN_PFX_CD": 4,"CLM_SVC_PROV_SPEC_CD": 10,
    "CLM_SVC_PROV_TYP_CD": 10,"CLM_STTUS_CD": 2,"CLM_SUBMT_BCBS_PLN_CD": 10,
    "CLM_SUB_BCBS_PLN_CD": 10,"CLM_SUBTYP_CD": 10,"CLM_TYP_CD": 1,"ATCHMT_IN": 1,
    "CLM_CLNCL_EDIT_CD": 1,"COBRA_CLM_IN": 1,"FIRST_PASS_IN": 1,"HOST_IN": 1,
    "LTR_IN": 1,"MCARE_ASG_IN": 1,"NOTE_IN": 1,"PCA_AUDIT_IN": 1,"PCP_SUBMT_IN": 1,
    "PROD_OOA_IN": 1,"ACDNT_DT": 10,"INPT_DT": 10,"MBR_PLN_ELIG_DT": 10,
    "NEXT_RVW_DT": 10,"PD_DT": 10,"PAYMT_DRAG_CYC_DT": 10,"PRCS_DT": 10,
    "RCVD_DT": 10,"SVC_STRT_DT": 10,"SVC_END_DT": 10,"SMLR_ILNS_DT": 10,
    "STTUS_DT": 10,"WORK_UNABLE_BEG_DT": 10,"WORK_UNABLE_END_DT": 10
}
for col_name, length in padding_spec.items():
    df_clmcrf = df_clmcrf.withColumn(col_name, rpad(col(col_name), length, " "))

# Read from nasco_dup hashed file (Scenario C => read from parquet)
df_nasco_dup = spark.read.parquet(f"{adls_path}/hf_clm_nasco_dup.parquet")
df_nasco_dup_alias = df_nasco_dup.alias("dup")

# Read from IDS_CLM (DB2Connector) - removing parameterized conditions for a full set
jdbc_url_ids, jdbc_props_ids = get_db_config("ids_secret_name")
extract_query_ids_clm = """
SELECT 
  C.CLM_ID,
  C.CLM_SK,
  CD2.TRGT_CD AS TRGT_CD
FROM #$IDSOwner#.CLM C
JOIN #$IDSOwner#.CD_MPPNG CD  ON C.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
JOIN #$IDSOwner#.CD_MPPNG CD1 ON C.CLM_STTUS_CD_SK = CD1.CD_MPPNG_SK
JOIN #$IDSOwner#.CD_MPPNG CD2 ON C.CLM_SUBTYP_CD_SK = CD2.CD_MPPNG_SK
WHERE CD.TRGT_CD = 'BCA'
  AND CD1.TRGT_CD IN ('A02','A09')
  AND CD2.TRGT_CD = 'RX'
"""
df_ids_clm = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_ids_clm)
    .load()
)
df_ids_clm_alias = df_ids_clm.alias("ic")

# Alias for df_clmcrf
df_clmcrf_alias = df_clmcrf.alias("crf")

# Join the reference DataFrame (nasco_dup) on CLM_ID
df_joined = df_clmcrf_alias.join(df_nasco_dup_alias, col("crf.CLM_ID") == col("dup.CLM_ID"), "left")

# Join the IDS_CLM reference on CLM_ID (named "Exist_Ids" in the Transformer)
df_joined = df_joined.join(df_ids_clm_alias, col("crf.CLM_ID") == col("ic.CLM_ID"), "left")

# Build all Transformer stage variables as additional columns.
df_Trns1 = (
    df_joined
    .withColumn(
        "svEMRSrcSysCd",
        when(
            (
                trim(col("crf.SRC_SYS_CD")).isin(
                    "CLAYPLATTE","JAYHAWK","ENCOMPASS","LIBERTYHOSP","MERITAS","NORTHLAND","OLATHEMED",
                    "PROVIDENCE","PROVSTLUKES","SUNFLOWER","TRUMAN","UNTDMEDGRP","MOSAIC","MOSAICLIFECARE",
                    "BARRYPOINTE","PRIMEMO","SPIRA","LEAWOODFMLYCARE","HCA","NONSTDSUPLMTDATA",
                    "CHILDRENSMERCY","CENTRUSHEALTH","MOHEALTH","GOLDENVALLEY","JEFFERSON","WESTMOMEDCNTR",
                    "BLUESPRINGS","EXCELSIOR","HARRISONVILLE"
                )
            ),
            lit("EMR")
        ).otherwise(lit(""))
    )
    .withColumn(
        "svSBVSrcSysCd",
        when(
            trim(col("crf.SRC_SYS_CD")).isin("DOMINION","MARC"),
            lit("SBV")
        ).otherwise(lit(""))
    )
    .withColumn(
        "svCdMpngSrcSysCd",
        when(col("crf.SRC_SYS_CD") == lit("BCBSA"), lit("BCA"))
        .when(col("crf.SRC_SYS_CD") == lit("LUMERIS"), lit("FACETS"))
        .otherwise(col("crf.SRC_SYS_CD"))
    )
    .withColumn(
        "NascoDupClm",
        when(
            (col("crf.SRC_SYS_CD") == lit("FACETS")) & (col("dup.CLM_ID").isNotNull()), 
            lit("Y")
        ).otherwise(lit("N"))
    )
    .withColumn(
        "SrcSysCdMbr",
        when(
            (
                (col("crf.SRC_SYS_CD").isin("DENTAQUEST","SOLUTRAN","LVNGHLTH")) |
                (col("svEMRSrcSysCd") == lit("EMR")) |
                (col("crf.SRC_SYS_CD").substr(lit(1),lit(6)) == lit("NATION")) |
                (col("svSBVSrcSysCd") == lit("SBV"))
            ),
            lit("FACETS")
        )
        .when( (col("crf.SRC_SYS_CD").isNull()) | (trim(col("crf.SRC_SYS_CD")) == lit("")), lit("UNK"))
        .when( trim(col("crf.SRC_SYS_CD")).isin(
            "PCT","WELLDYNERX","PCS","CAREMARK","ARGUS","EDC","OT@2","ADOL","MOHSAIC","CAREADVANCE","ESI","OPTUMRX",
            "MCSOURCE","MCAID","MEDTRAK","BCBSSC","BCA","BCBSA","SAVRX","LDI","EYEMED","CVS","LUMERIS","MEDIMPACT"
        ), lit("FACETS"))
        .otherwise(col("crf.SRC_SYS_CD"))
    )
    .withColumn(
        "SrcSysCdProd",
        when(
            (
                (col("crf.SRC_SYS_CD").isin("DENTAQUEST","SOLUTRAN","LVNGHLTH")) |
                (col("svEMRSrcSysCd") == lit("EMR")) |
                (col("crf.SRC_SYS_CD").substr(lit(1),lit(6)) == lit("NATION")) |
                (col("svSBVSrcSysCd") == lit("SBV"))
            ),
            lit("FACETS")
        )
        .when( (col("crf.SRC_SYS_CD").isNull()) | (trim(col("crf.SRC_SYS_CD")) == lit("")), lit("UNK"))
        .when( trim(col("crf.SRC_SYS_CD")).isin(
            "PCT","WELLDYNERX","PCS","CAREMARK","ARGUS","EDC","OT@2","ADOL","ESI","OPTUMRX","MCSOURCE","MCAID","MEDTRAK",
            "BCBSSC","BCA","BCBSA","SAVRX","LDI","EYEMED","CVS","LUMERIS","MEDIMPACT"
        ), lit("FACETS"))
        .otherwise(col("crf.SRC_SYS_CD"))
    )
    .withColumn(
        "ClmAdjFromClmSk",
        when(
            (col("NascoDupClm") == lit("N")) & 
            (col("crf.SRC_SYS_CD") == lit("BCA")) &
            (regexp_replace(col("crf.CLM_ID"), r"\s+$","").endswith("R")) &
            (col("ic.TRGT_CD") == lit("RX")),
            col("ic.CLM_SK")
        )
        .otherwise(
            when(
                col("NascoDupClm") == lit("N"),
                GetFkeyClm(col("crf.SRC_SYS_CD"),col("crf.CLM_SK"),col("crf.ADJ_FROM_CLM_ID"),Logging)
            ).otherwise(
                GetFkeyClm(lit("NPS"),col("crf.CLM_SK"),col("dup.NASCO_CLM_ID"),Logging)
            )
        )
    )
    .withColumn(
        "ClmAdjToClmCdSk",
        GetFkeyClm(col("crf.SRC_SYS_CD"),col("crf.CLM_SK"),col("crf.ADJ_TO_CLM_ID"),Logging)
    )
    .withColumn(
        "ClsPlnSk",
        when(trim(col("crf.CLS_PLN")) == lit("UNK"), lit("0"))
        .when(trim(col("crf.CLS_PLN")) == lit("NA"), lit("1"))
        .otherwise(
            GetFkeyClsPln(col("SrcSysCdMbr"),col("crf.CLM_SK"),col("crf.CLS_PLN"),Logging)
        )
    )
    .withColumn(
        "ClsSk",
        when(trim(col("crf.CLS")) == lit("UNK"), lit("0"))
        .when(trim(col("crf.CLS")) == lit("NA"), lit("1"))
        .otherwise(
            GetFkeyCls(col("SrcSysCdMbr"),col("crf.CLM_SK"),col("crf.GRP"),col("crf.CLS"),Logging)
        )
    )
    .withColumn(
        "ExpCatCdSk",
        when(trim(col("crf.EXPRNC_CAT")) == lit("UNK"), lit("0"))
        .when(trim(col("crf.EXPRNC_CAT")) == lit("NA"), lit("1"))
        .otherwise(
            when(
                col("crf.SRC_SYS_CD") == lit("NPS"),
                GetFkeyExprncCat(lit("FACETS"),col("crf.CLM_SK"),col("crf.EXPRNC_CAT"),Logging)
            ).when(
                col("crf.SRC_SYS_CD") == lit("LVNGHLTH"),
                GetFkeyExprncCat(lit("FACETS"),col("crf.CLM_SK"),col("crf.EXPRNC_CAT"),Logging)
            ).otherwise(
                GetFkeyExprncCat(col("SrcSysCdProd"),col("crf.CLM_SK"),col("crf.EXPRNC_CAT"),Logging)
            )
        )
    )
    .withColumn(
        "GrpSk",
        when(trim(col("crf.GRP")) == lit("UNK"), lit("0"))
        .when(trim(col("crf.GRP")) == lit("NA"), lit("1"))
        .otherwise(
            GetFkeyGrp(col("SrcSysCdMbr"),col("crf.CLM_SK"),col("crf.GRP"),Logging)
        )
    )
    .withColumn(
        "MbrSk",
        when(trim(col("crf.MBR_CK")) == lit("UNK"), lit("0"))
        .when(trim(col("crf.MBR_CK")) == lit("NA"), lit("1"))
        .otherwise(
            GetFkeyMbr(col("SrcSysCdMbr"),col("crf.CLM_SK"),col("crf.MBR_CK"),Logging)
        )
    )
    .withColumn(
        "NtwkSk",
        when(trim(col("crf.NTWK")) == lit("UNK"), lit("0"))
        .when(trim(col("crf.NTWK")) == lit("NA"), lit("1"))
        .otherwise(
            when(
                (trim(col("crf.NTWK")) == lit("")) & (trim(col("crf.SRC_SYS_CD")) == lit("FACETS")),
                GetFkeyNtwk(lit("FACETS"),col("crf.CLM_SK"),lit(" "),Logging)
            ).otherwise(
                GetFkeyNtwk(lit("FACETS"),col("crf.CLM_SK"),col("crf.NTWK"),Logging)
            )
        )
    )
    .withColumn(
        "ProdSk",
        when(trim(col("crf.PROD")) == lit("UNK"), lit("0"))
        .when(trim(col("crf.PROD")) == lit("NA"), lit("1"))
        .otherwise(
            GetFkeyProd(col("SrcSysCdProd"),col("crf.CLM_SK"),col("crf.PROD"),Logging)
        )
    )
    .withColumn(
        "SubGrpSk",
        when(trim(col("crf.SUBGRP")) == lit("UNK"), lit("0"))
        .when(trim(col("crf.SUBGRP")) == lit("NA"), lit("1"))
        .otherwise(
            GetFkeySubgrp(col("SrcSysCdMbr"),col("crf.CLM_SK"),col("crf.GRP"),col("crf.SUBGRP"),Logging)
        )
    )
    .withColumn(
        "SubSk",
        when(trim(col("crf.SUB_CK")) == lit("UNK"), lit("0"))
        .when(trim(col("crf.SUB_CK")) == lit("NA"), lit("1"))
        .otherwise(
            GetFkeySub(col("SrcSysCdMbr"),col("crf.CLM_SK"),col("crf.SUB_CK"),Logging)
        )
    )
    .withColumn(
        "ActBcbsSk",
        when(
            (col("crf.SRC_SYS_CD") == lit("BCA")) | (col("svCdMpngSrcSysCd") == lit("BCA")),
            GetFkeyCodes(lit("FACETS"),col("crf.CLM_SK"),lit("ACTIVATING BCBS PLAN"),col("crf.CLM_ACTIVATING_BCBS_PLN_CD"),Logging)
        ).otherwise(
            GetFkeyCodes(col("svCdMpngSrcSysCd"),col("crf.CLM_SK"),lit("ACTIVATING BCBS PLAN"),col("crf.CLM_ACTIVATING_BCBS_PLN_CD"),Logging)
        )
    )
    .withColumn(
        "CapClmCdSk",
        GetFkeyCodes(col("svCdMpngSrcSysCd"),col("crf.CLM_SK"),lit("CAPITATED CLAIM"),col("crf.CLM_CAP_CD"),Logging)
    )
    .withColumn(
        "ClmAcdtStCdSk",
        GetFkeyCodes(lit("FACETS"),col("crf.CLM_SK"),lit("STATE"),col("crf.CLM_ACDNT_ST_CD"),Logging)
    )
    .withColumn(
        "ClmAgmntSrcCdSk",
        GetFkeyCodes(col("svCdMpngSrcSysCd"),col("crf.CLM_SK"),lit("CLAIM AGREEMENT SOURCE"),col("crf.CLM_AGMNT_SRC_CD"),Logging)
    )
    .withColumn(
        "ClmBtchActnCdSk",
        GetFkeyCodes(col("svCdMpngSrcSysCd"),col("crf.CLM_SK"),lit("CLAIM BATCH ACTION"),col("crf.CLM_BTCH_ACTN_CD"),Logging)
    )
    .withColumn(
        "ClmCatCdSk",
        GetFkeyCodes(col("svCdMpngSrcSysCd"),col("crf.CLM_SK"),lit("CLAIM CATEGORY"),col("crf.CLM_CAT_CD"),Logging)
    )
    .withColumn(
        "ClmChkCycOvdSk",
        GetFkeyCodes(col("svCdMpngSrcSysCd"),col("crf.CLM_SK"),lit("CLAIM CHECK CYCLE OVERRIDE"),col("crf.CLM_CHK_CYC_OVRD_CD"),Logging)
    )
    .withColumn(
        "ClmCobCdSk",
        GetFkeyCodes(col("svCdMpngSrcSysCd"),col("crf.CLM_SK"),lit("CLAIM COB"),col("crf.CLM_COB_CD"),Logging)
    )
    .withColumn(
        "ClmEOBExcdSk",
        when(col("crf.SRC_SYS_CD") == lit("LVNGHLTH"),
             GetFkeyExcd(lit("FACETS"),col("crf.CLM_SK"),col("crf.CLM_EOB_EXCD"),Logging)
        ).otherwise(
            GetFkeyExcd(col("crf.SRC_SYS_CD"),col("crf.CLM_SK"),col("crf.CLM_EOB_EXCD"),Logging)
        )
    )
    .withColumn(
        "ClmFinlDispCdSk",
        GetFkeyCodes(col("svCdMpngSrcSysCd"),col("crf.CLM_SK"),lit("CLAIM FINALIZE DISPOSITION"),col("crf.FINL_DISP_CD"),Logging)
    )
    .withColumn(
        "ClmInNtwkSk",
        when(
            col("crf.SRC_SYS_CD") == lit("BCBSSC"),
            when(
                GetFkeyCodes(col("svCdMpngSrcSysCd"),col("crf.CLM_SK"),lit("CLAIM IN NETWORK"),col("crf.CLM_NTWK_STTUS_CD"),lit("X")) == lit("0"),
                GetFkeyCodes(lit("FACETS"),col("crf.CLM_SK"),lit("CLAIM IN NETWORK"),col("crf.CLM_NTWK_STTUS_CD"),Logging)
            ).otherwise(
                GetFkeyCodes(col("svCdMpngSrcSysCd"),col("crf.CLM_SK"),lit("CLAIM IN NETWORK"),col("crf.CLM_NTWK_STTUS_CD"),Logging)
            )
        ).otherwise(
            GetFkeyCodes(col("svCdMpngSrcSysCd"),col("crf.CLM_SK"),lit("CLAIM IN NETWORK"),col("crf.CLM_NTWK_STTUS_CD"),Logging)
        )
    )
    .withColumn(
        "ClmInptMethSk",
        GetFkeyCodes(col("svCdMpngSrcSysCd"),col("crf.CLM_SK"),lit("CLAIM INPUT METHOD"),col("crf.CLM_INPT_METH_CD"),Logging)
    )
    .withColumn(
        "ClmInptSrcSk",
        GetFkeyCodes(col("svCdMpngSrcSysCd"),col("crf.CLM_SK"),lit("CLAIM INPUT SOURCE"),col("crf.CLM_INPT_SRC_CD"),Logging)
    )
    .withColumn(
        "ClmIppCdSk",
        GetFkeyCodes(col("svCdMpngSrcSysCd"),col("crf.CLM_SK"),lit("CLAIM INTER PLAN PROGRAM CODE"),col("crf.CLM_IPP_CD"),Logging)
    )
    .withColumn(
        "ClmOtherBnfSk",
        GetFkeyCodes(col("svCdMpngSrcSysCd"),col("crf.CLM_SK"),lit("CLAIM OTHER BENEFIT"),col("crf.CLM_OTHER_BNF_CD"),Logging)
    )
    .withColumn(
        "ClmPayeCdSk",
        GetFkeySrcTrgtDomainCodes(col("svCdMpngSrcSysCd"),col("crf.CLM_SK"),lit("CLAIM PAYEE"),col("crf.CLM_PAYE_CD"),lit("CLAIM PAYEE"),Logging)
    )
    .withColumn(
        "ClmPrcsCtlAgntPfxSk",
        GetFkeyCodes(col("svCdMpngSrcSysCd"),col("crf.CLM_SK"),lit("CLAIM PROCESSING CONTROL AGENT PREFIX"),col("crf.CLM_PRCS_CTL_AGNT_PFX_CD"),Logging)
    )
    .withColumn(
        "ClmSvcDefnPfxCdSk",
        GetFkeyCodes(col("svCdMpngSrcSysCd"),col("crf.CLM_SK"),lit("CLAIM SERVICE DEFINITION PREFIX"),col("crf.CLM_SVC_DEFN_PFX_CD"),Logging)
    )
    .withColumn(
        "ClmSttusCdSk",
        GetFkeyCodes(col("svCdMpngSrcSysCd"),col("crf.CLM_SK"),lit("CLAIM STATUS"),col("crf.CLM_STTUS_CD"),Logging)
    )
    .withColumn(
        "ClmSubTypCdSk",
        GetFkeyCodes(col("svCdMpngSrcSysCd"),col("crf.CLM_SK"),lit("CLAIM SUBTYPE"),col("crf.CLM_SUBTYP_CD"),Logging)
    )
    .withColumn(
        "ClmTypCdSk",
        GetFkeyCodes(col("svCdMpngSrcSysCd"),col("crf.CLM_SK"),lit("CLAIM TYPE"),col("crf.CLM_TYP_CD"),Logging)
    )
    .withColumn(
        "SvcProvDomain",
        when(
            col("crf.SRC_SYS_CD") == lit("NPS"),
            when(
                trim(col("crf.CLM_SUBTYP_CD")).isin("PR","PR "),
                lit("SERVICE PROVIDER TYPE")
            ).when(
                trim(col("crf.CLM_SUBTYP_CD")).isin("IP","OP","I","O"),
                lit("PROVIDER FACILITY TYPE")
            ).otherwise(lit("NA"))
        ).when(
            col("crf.SRC_SYS_CD") == lit("FACETS"),
            when(col("crf.PRPR_ENTITY") == lit("F"), lit("PROVIDER FACILITY TYPE"))
            .when(col("crf.PRPR_ENTITY").isin("I","P","G"), lit("SERVICE PROVIDER TYPE"))
            .otherwise(lit("NA"))
        ).when(
            col("crf.SRC_SYS_CD").isin(
                "ESI","OPTUMRX","MEDIMPACT","WELLDYNERX","MCAID","MEDTRAK","BCBSSC","BCA","BCBSA","SAVRX","LDI","CVS",
                "LVNGHLTH","SOLUTRAN"
            ) | (col("crf.SRC_SYS_CD").substr(lit(1),lit(9)) == lit("NATIONBFT")),
            lit("SERVICE PROVIDER TYPE")
        ).otherwise(lit("NA"))
    )
    .withColumn(
        "SvcProvTypCdSk",
        when(col("SvcProvDomain") == lit("NA"), lit("1"))
        .when(trim(col("crf.CLM_SVC_PROV_TYP_CD")) == lit("UNK"), lit("0"))
        .when(trim(col("crf.CLM_SVC_PROV_TYP_CD")) == lit("NA"), lit("1"))
        .otherwise(
            when(
                col("SvcProvDomain") == lit("SERVICE PROVIDER TYPE"),
                GetFkeyProvTyp(lit("FACETS"),col("crf.CLM_SK"),col("crf.CLM_SVC_PROV_TYP_CD"),Logging)
            ).otherwise(
                GetFkeyFcltyTyp(lit("FACETS"),col("crf.CLM_SK"),col("crf.CLM_SVC_PROV_TYP_CD"),Logging)
            )
        )
    )
    .withColumn(
        "SvcProvSpecCdSk",
        when((col("crf.SRC_SYS_CD") == lit("BCBSSC")) & (trim(col("crf.CLM_SVC_PROV_SPEC_CD")) == lit("UNK")), lit("0"))
        .when((col("crf.SRC_SYS_CD") == lit("BCBSSC")) & (trim(col("crf.CLM_SVC_PROV_SPEC_CD")) == lit("NA")), lit("1"))
        .when(col("crf.SRC_SYS_CD") == lit("BCBSSC"),
              GetFkeyProvSpec(col("crf.SRC_SYS_CD"),col("crf.CLM_SK"),col("crf.CLM_SVC_PROV_SPEC_CD"),Logging)
        )
        .when( trim(col("crf.CLM_SUBTYP_CD")).isin("I","O","IP","OP"), lit("1"))
        .when(col("crf.SRC_SYS_CD") == lit("LVNGHLTH"),
              GetFkeyProvSpec(lit("FACETS"),col("crf.CLM_SK"),col("crf.CLM_SVC_PROV_SPEC_CD"),Logging)
        )
        .when(col("svSBVSrcSysCd") == lit("SBV"), lit("1"))
        .otherwise(
            GetFkeyProvSpec(col("crf.SRC_SYS_CD"),col("crf.CLM_SK"),col("crf.CLM_SVC_PROV_SPEC_CD"),Logging)
        )
    )
    .withColumn(
        "SubmtBcbsPlnSk",
        when(
            (col("crf.SRC_SYS_CD") == lit("BCA")) | (col("svCdMpngSrcSysCd") == lit("BCA")) | (col("svEMRSrcSysCd") == lit("EMR")),
            GetFkeyCodes(lit("FACETS"),col("crf.CLM_SK"),lit("SUBMIT BCBS PLAN"),col("crf.CLM_SUBMT_BCBS_PLN_CD"),Logging)
        ).otherwise(
            GetFkeyCodes(col("svCdMpngSrcSysCd"),col("crf.CLM_SK"),lit("SUBMIT BCBS PLAN"),col("crf.CLM_SUBMT_BCBS_PLN_CD"),Logging)
        )
    )
    .withColumn(
        "SubBcbsPlnSk",
        when(
            (col("crf.SRC_SYS_CD") == lit("BCA")) | (col("svCdMpngSrcSysCd") == lit("BCA")) | (col("svEMRSrcSysCd") == lit("EMR")),
            GetFkeyCodes(lit("FACETS"),col("crf.CLM_SK"),lit("SUBSCRIBER BCBS PLAN"),col("crf.CLM_SUB_BCBS_PLN_CD"),Logging)
        ).otherwise(
            GetFkeyCodes(col("svCdMpngSrcSysCd"),col("crf.CLM_SK"),lit("SUBSCRIBER BCBS PLAN"),col("crf.CLM_SUB_BCBS_PLN_CD"),Logging)
        )
    )
    .withColumn("AcdntDtSk",       GetFkeyDate(lit("IDS"),col("crf.CLM_SK"),col("crf.ACDNT_DT"),Logging))
    .withColumn("InputDtSk",       GetFkeyDate(lit("IDS"),col("crf.CLM_SK"),col("crf.INPT_DT"),Logging))
    .withColumn("MbrPlnEligDtSk",  GetFkeyDate(lit("IDS"),col("crf.CLM_SK"),col("crf.MBR_PLN_ELIG_DT"),Logging))
    .withColumn("NextRvwDtSk",     GetFkeyDate(lit("IDS"),col("crf.CLM_SK"),col("crf.NEXT_RVW_DT"),Logging))
    .withColumn("PdDtSk",          GetFkeyDate(lit("IDS"),col("crf.CLM_SK"),col("crf.PD_DT"),Logging))
    .withColumn("PaymtDragDtSk",   GetFkeyDate(lit("IDS"),col("crf.CLM_SK"),col("crf.PAYMT_DRAG_CYC_DT"),Logging))
    .withColumn("PrcsDtSk",        GetFkeyDate(lit("IDS"),col("crf.CLM_SK"),col("crf.PRCS_DT"),Logging))
    .withColumn("RcvdDtSk",        GetFkeyDate(lit("IDS"),col("crf.CLM_SK"),col("crf.RCVD_DT"),Logging))
    .withColumn("SvcStrtDtSk",     GetFkeyDate(lit("IDS"),col("crf.CLM_SK"),col("crf.SVC_STRT_DT"),Logging))
    .withColumn("SvcEndDtSk",      GetFkeyDate(lit("IDS"),col("crf.CLM_SK"),col("crf.SVC_END_DT"),Logging))
    .withColumn("SttusDtSk",       GetFkeyDate(lit("IDS"),col("crf.CLM_SK"),col("crf.STTUS_DT"),Logging))
    .withColumn("WorkUnblBegDtSk", GetFkeyDate(lit("IDS"),col("crf.CLM_SK"),col("crf.WORK_UNABLE_BEG_DT"),Logging))
    .withColumn("WorkUnblEndDtSk", GetFkeyDate(lit("IDS"),col("crf.CLM_SK"),col("crf.WORK_UNABLE_END_DT"),Logging))
    .withColumn(
        "NonParPfxSk",
        GetFkeyCodes(col("svCdMpngSrcSysCd"),col("crf.CLM_SK"),lit("CLAIM NON PARTICIPATING PROVIDER PREFIX"),col("crf.CLM_NONPAR_PROV_PFX_CD"),Logging)
    )
    .withColumn(
        "PlnAlphPfxSk",
        GetFkeyAlphaPfx(lit("FACETS"),col("crf.CLM_SK"),col("crf.ALPHA_PFX_CD"),Logging)
    )
    .withColumn(
        "SmlrIlnsDtSk",
        GetFkeyDate(lit("IDS"),col("crf.CLM_SK"),col("crf.SMLR_ILNS_DT"),Logging)
    )
    .withColumn("PassThru", col("crf.PASS_THRU_IN"))
    .withColumn("FinancialLOB", GetFkeyFnclLob(lit("PSI"),col("crf.CLM_SK"),col("crf.FNCL_LOB_NO"),Logging))
    .withColumn(
        "AcdntCd",
        GetFkeyCodes(col("svCdMpngSrcSysCd"),col("crf.CLM_SK"),lit("CLAIM ACCIDENT"),col("crf.CLM_ACDNT_CD"),Logging)
    )
    .withColumn(
        "AdjustedFacetsClaimStatus91",
        GetFkeyCodes(lit("FACETS"),col("crf.CLM_SK"),lit("CLAIM STATUS"),lit("R"),Logging)
    )
    .withColumn(
        "svPcaTypCdSk",
        GetFkeyCodes(col("svCdMpngSrcSysCd"),col("crf.CLM_SK"),lit("PERSONAL CARE ACCOUNT PROCESSING"),col("crf.PCA_TYP_CD"),Logging)
    )
    .withColumn(
        "svRelBaseClmSk",
        GetFkeyClm(col("crf.SRC_SYS_CD"),col("crf.CLM_SK"),trim(col("crf.CLCL_MICRO_ID")),Logging)
    )
    .withColumn(
        "svRelPcaClmSk",
        GetFkeyClm(col("crf.SRC_SYS_CD"),col("crf.CLM_SK"),trim(col("crf.REL_PCA_CLM_ID")),Logging)
    )
    .withColumn(
        "svClmSubmtICDVrsnCdSK",
        GetFkeyCodes(col("svCdMpngSrcSysCd"),col("crf.CLM_SK"),lit("DIAGNOSIS CODE TYPE"),col("crf.CLM_SUBMT_ICD_VRSN_CD"),Logging)
    )
    .withColumn(
        "ErrCount",
        GetFkeyErrorCnt(col("crf.CLM_SK"))
    )
)

# Create the three output sets from Transformer logic:
df_ClmFkeyOut1 = df_Trns1.filter((col("ErrCount") == lit("0")) | (col("PassThru") == lit("Y")))

# DefaultUNK => a single row that has the specified columns with "UNK","0","'U', etc.
# The stage has about 100+ columns. We replicate them carefully:
schema_DefaultUNK = df_Trns1.select(
    "crf.CLM_SK","crf.SRC_SYS_CD_SK","crf.CLM_ID","crf.CRT_RUN_CYC_EXCTN_SK",
    "RunCycle","ClmAdjFromClmSk","ClmAdjToClmCdSk","PlnAlphPfxSk","ClmEOBExcdSk","ClsSk",
    "ClsPlnSk","ExpCatCdSk","FinancialLOB","GrpSk","MbrSk","NtwkSk","ProdSk","SubGrpSk","SubSk",
    "AcdntCd","ClmAcdtStCdSk","ActBcbsSk","ClmAgmntSrcCdSk","ClmBtchActnCdSk","CapClmCdSk",
    "ClmCatCdSk","ClmChkCycOvdSk","ClmCobCdSk","ClmFinlDispCdSk","ClmInptMethSk","ClmInptSrcSk",
    "ClmIppCdSk","ClmInNtwkSk","NonParPfxSk","ClmOtherBnfSk","ClmPayeCdSk","ClmPrcsCtlAgntPfxSk",
    "ClmSvcDefnPfxCdSk","SvcProvSpecCdSk","SvcProvTypCdSk","ClmSttusCdSk","SubmtBcbsPlnSk","SubBcbsPlnSk",
    "ClmSubTypCdSk","ClmTypCdSk","crf.ATCHMT_IN","crf.CLM_CLNCL_EDIT_CD","crf.COBRA_CLM_IN","crf.FIRST_PASS_IN",
    "crf.HOST_IN","crf.LTR_IN","crf.MCARE_ASG_IN","crf.NOTE_IN","crf.PCA_AUDIT_IN","crf.PCP_SUBMT_IN",
    "crf.PROD_OOA_IN","AcdntDtSk","InputDtSk","MbrPlnEligDtSk","NextRvwDtSk","PdDtSk","PaymtDragDtSk","PrcsDtSk",
    "RcvdDtSk","SvcStrtDtSk","SvcEndDtSk","SmlrIlnsDtSk","SttusDtSk","WorkUnblBegDtSk","WorkUnblEndDtSk",
    "crf.ACDNT_AMT","crf.ACTL_PD_AMT","crf.ALLOW_AMT","crf.DSALW_AMT","crf.COINS_AMT","crf.CNSD_CHRG_AMT",
    "crf.COPAY_AMT","crf.CHRG_AMT","crf.DEDCT_AMT","crf.PAYBL_AMT","crf.CLM_CT","crf.MBR_AGE",
    "crf.ADJ_FROM_CLM_ID","crf.ADJ_TO_CLM_ID","crf.DOC_TX_ID","crf.MCAID_RESUB_NO","crf.MCARE_ID","crf.MBR_SFX_NO",
    "crf.PATN_ACCT_NO","crf.PAYMT_REF_ID","crf.PROV_AGMNT_ID","crf.RFRNG_PROV_TX","crf.SUB_ID","svPcaTypCdSk",
    "svRelPcaClmSk","svRelBaseClmSk","crf.CLM_UPDT_SW","crf.CLCL_MICRO_ID","crf.REMIT_SUPRSION_AMT","crf.MCAID_STTUS_ID",
    "crf.PATN_PD_AMT","svClmSubmtICDVrsnCdSK","crf.CLM_TXNMY_CD","crf.BILL_PAYMT_EXCL_IN","crf.ADJ_FROM_CLM","crf.ADJ_TO_CLM"
).schema

df_DefaultUNK = spark.createDataFrame(
    [
        (
            0,0,"UNK",0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,'U','U','U','U','U','U','U','U','U','U','U','U','NA','NA','NA','NA','NA','NA','NA','NA','NA','NA','NA','NA','NA','NA','NA','NA',0,0,0,0,0,0,0,0,0,0,0,0,"UNK","UNK","UNK","UNK","UNK","UN","UNK","UNK","UNK","UNK","UNK",0,0,0,'N',"UNK","UNK",0,"UNK",0,"UNK",0,"UNK","UNK",'N',"UNK","UNK"
        )
    ],
    schema_DefaultUNK
)

# DefaultNA => similarly, a single row with "NA" or 1
schema_DefaultNA = schema_DefaultUNK
df_DefaultNA = spark.createDataFrame(
    [
        (
            1,1,"NA",1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,'X','X','X','X','X','X','X','X','X','X','X','X','NA','NA','NA','NA','NA','NA','NA','NA','NA','NA','NA','NA','NA','NA','NA','NA',0,0,0,0,0,0,0,0,0,0,0,0,"NA","NA","NA","NA","NA","NA","NA","NA","NA","NA","NA",1,1,1,'N',"NA","NA",0,"NA",0,"NA",0,"NA","NA",'N',"NA","NA"
        )
    ],
    schema_DefaultNA
)

# Select final columns for each DataFrame identical to what the collector needs:
columns_for_merge_load = [
    "CLM_SK","SRC_SYS_CD_SK","CLM_ID","CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK","ADJ_FROM_CLM_SK","ADJ_TO_CLM_SK",
    "ALPHA_PFX_SK","CLM_EOB_EXCD_SK","CLS_SK","CLS_PLN_SK","EXPRNC_CAT_SK","FNCL_LOB_SK","GRP_SK","MBR_SK","NTWK_SK","PROD_SK",
    "SUBGRP_SK","SUB_SK","CLM_ACDNT_CD_SK","CLM_ACDNT_ST_CD_SK","CLM_ACTV_BCBS_PLN_CD_SK","CLM_AGMNT_SRC_CD_SK","CLM_BTCH_ACTN_CD_SK",
    "CLM_CAP_CD_SK","CLM_CAT_CD_SK","CLM_CHK_CYC_OVRD_CD_SK","CLM_COB_CD_SK","CLM_FINL_DISP_CD_SK","CLM_INPT_METH_CD_SK","CLM_INPT_SRC_CD_SK",
    "CLM_INTER_PLN_PGM_CD_SK","CLM_NTWK_STTUS_CD_SK","CLM_NONPAR_PROV_PFX_CD_SK","CLM_OTHR_BNF_CD_SK","CLM_PAYE_CD_SK","CLM_PRCS_CTL_AGNT_PFX_CD_SK",
    "CLM_SVC_DEFN_PFX_CD_SK","CLM_SVC_PROV_SPEC_CD_SK","CLM_SVC_PROV_TYP_CD_SK","CLM_STTUS_CD_SK","CLM_SUBMTTING_BCBS_PLN_CD_SK","CLM_SUB_BCBS_PLN_CD_SK",
    "CLM_SUBTYP_CD_SK","CLM_TYP_CD_SK","ATCHMT_IN","CLNCL_EDIT_IN","COBRA_CLM_IN","FIRST_PASS_IN","HOST_IN","LTR_IN","MCARE_ASG_IN",
    "NOTE_IN","PCA_AUDIT_IN","PCP_SUBMT_IN","PROD_OOA_IN","ACDNT_DT_SK","INPT_DT_SK","MBR_PLN_ELIG_DT_SK","NEXT_RVW_DT_SK","PD_DT_SK","PAYMT_DRAG_CYC_DT_SK",
    "PRCS_DT_SK","RCVD_DT_SK","SVC_STRT_DT_SK","SVC_END_DT_SK","SMLR_ILNS_DT_SK","STTUS_DT_SK","WORK_UNABLE_BEG_DT_SK","WORK_UNABLE_END_DT_SK","ACDNT_AMT",
    "ACTL_PD_AMT","ALW_AMT","DSALW_AMT","COINS_AMT","CNSD_CHRG_AMT","COPAY_AMT","CHRG_AMT","DEDCT_AMT","PAYBL_AMT","CLM_CT","MBR_AGE","ADJ_FROM_CLM_ID",
    "ADJ_TO_CLM_ID","DOC_TX_ID","MCAID_RESUB_NO","MCARE_ID","MBR_SFX_NO","PATN_ACCT_NO","PAYMT_REF_ID","PROV_AGMNT_ID","RFRNG_PROV_TX","SUB_ID",
    "PCA_TYP_CD_SK","REL_PCA_CLM_SK","REL_BASE_CLM_SK","CLM_UPDT_SW","CLCL_MICRO_ID","REMIT_SUPRSION_AMT","MCAID_STTUS_ID","PATN_PD_AMT",
    "CLM_SUBMT_ICD_VRSN_CD_SK","CLM_TXNMY_CD","BILL_PAYMT_EXCL_IN","ADJ_FROM_CLM","ADJ_TO_CLM"
]

df_ClmFkeyOut1_sel = df_ClmFkeyOut1.selectExpr(
    "crf.CLM_SK AS CLM_SK",
    "crf.SRC_SYS_CD_SK AS SRC_SYS_CD_SK",
    "crf.CLM_ID AS CLM_ID",
    "crf.CRT_RUN_CYC_EXCTN_SK AS CRT_RUN_CYC_EXCTN_SK",
    f"{RunCycle} AS LAST_UPDT_RUN_CYC_EXCTN_SK",
    "ClmAdjFromClmSk AS ADJ_FROM_CLM_SK",
    "ClmAdjToClmCdSk AS ADJ_TO_CLM_SK",
    "PlnAlphPfxSk AS ALPHA_PFX_SK",
    "ClmEOBExcdSk AS CLM_EOB_EXCD_SK",
    "ClsSk AS CLS_SK",
    "ClsPlnSk AS CLS_PLN_SK",
    "ExpCatCdSk AS EXPRNC_CAT_SK",
    "FinancialLOB AS FNCL_LOB_SK",
    "GrpSk AS GRP_SK",
    "MbrSk AS MBR_SK",
    "NtwkSk AS NTWK_SK",
    "ProdSk AS PROD_SK",
    "SubGrpSk AS SUBGRP_SK",
    "SubSk AS SUB_SK",
    "AcdntCd AS CLM_ACDNT_CD_SK",
    "ClmAcdtStCdSk AS CLM_ACDNT_ST_CD_SK",
    "ActBcbsSk AS CLM_ACTV_BCBS_PLN_CD_SK",
    "ClmAgmntSrcCdSk AS CLM_AGMNT_SRC_CD_SK",
    "ClmBtchActnCdSk AS CLM_BTCH_ACTN_CD_SK",
    "CapClmCdSk AS CLM_CAP_CD_SK",
    "ClmCatCdSk AS CLM_CAT_CD_SK",
    "ClmChkCycOvdSk AS CLM_CHK_CYC_OVRD_CD_SK",
    "ClmCobCdSk AS CLM_COB_CD_SK",
    "ClmFinlDispCdSk AS CLM_FINL_DISP_CD_SK",
    "ClmInptMethSk AS CLM_INPT_METH_CD_SK",
    "ClmInptSrcSk AS CLM_INPT_SRC_CD_SK",
    "ClmIppCdSk AS CLM_INTER_PLN_PGM_CD_SK",
    "ClmInNtwkSk AS CLM_NTWK_STTUS_CD_SK",
    "NonParPfxSk AS CLM_NONPAR_PROV_PFX_CD_SK",
    "ClmOtherBnfSk AS CLM_OTHR_BNF_CD_SK",
    "ClmPayeCdSk AS CLM_PAYE_CD_SK",
    "ClmPrcsCtlAgntPfxSk AS CLM_PRCS_CTL_AGNT_PFX_CD_SK",
    "ClmSvcDefnPfxCdSk AS CLM_SVC_DEFN_PFX_CD_SK",
    "SvcProvSpecCdSk AS CLM_SVC_PROV_SPEC_CD_SK",
    "SvcProvTypCdSk AS CLM_SVC_PROV_TYP_CD_SK",
    "ClmSttusCdSk AS CLM_STTUS_CD_SK",
    "SubmtBcbsPlnSk AS CLM_SUBMTTING_BCBS_PLN_CD_SK",
    "SubBcbsPlnSk AS CLM_SUB_BCBS_PLN_CD_SK",
    "ClmSubTypCdSk AS CLM_SUBTYP_CD_SK",
    "ClmTypCdSk AS CLM_TYP_CD_SK",
    "crf.ATCHMT_IN AS ATCHMT_IN",
    "crf.CLM_CLNCL_EDIT_CD AS CLNCL_EDIT_IN",
    "crf.COBRA_CLM_IN AS COBRA_CLM_IN",
    "crf.FIRST_PASS_IN AS FIRST_PASS_IN",
    "crf.HOST_IN AS HOST_IN",
    "crf.LTR_IN AS LTR_IN",
    "crf.MCARE_ASG_IN AS MCARE_ASG_IN",
    "crf.NOTE_IN AS NOTE_IN",
    "crf.PCA_AUDIT_IN AS PCA_AUDIT_IN",
    "crf.PCP_SUBMT_IN AS PCP_SUBMT_IN",
    "crf.PROD_OOA_IN AS PROD_OOA_IN",
    "AcdntDtSk AS ACDNT_DT_SK",
    "InputDtSk AS INPT_DT_SK",
    "MbrPlnEligDtSk AS MBR_PLN_ELIG_DT_SK",
    "NextRvwDtSk AS NEXT_RVW_DT_SK",
    "PdDtSk AS PD_DT_SK",
    "PaymtDragDtSk AS PAYMT_DRAG_CYC_DT_SK",
    "PrcsDtSk AS PRCS_DT_SK",
    "RcvdDtSk AS RCVD_DT_SK",
    "SvcStrtDtSk AS SVC_STRT_DT_SK",
    "CASE WHEN crf.SRC_SYS_CD = 'BCA' AND SvcEndDtSk = 'UNK' THEN '2199-12-31' ELSE SvcEndDtSk END AS SVC_END_DT_SK",
    "SmlrIlnsDtSk AS SMLR_ILNS_DT_SK",
    "SttusDtSk AS STTUS_DT_SK",
    "WorkUnblBegDtSk AS WORK_UNABLE_BEG_DT_SK",
    "WorkUnblEndDtSk AS WORK_UNABLE_END_DT_SK",
    "crf.ACDNT_AMT AS ACDNT_AMT",
    "crf.ACTL_PD_AMT AS ACTL_PD_AMT",
    "crf.ALLOW_AMT AS ALW_AMT",
    "crf.DSALW_AMT AS DSALW_AMT",
    "crf.COINS_AMT AS COINS_AMT",
    "crf.CNSD_CHRG_AMT AS CNSD_CHRG_AMT",
    "crf.COPAY_AMT AS COPAY_AMT",
    "crf.CHRG_AMT AS CHRG_AMT",
    "crf.DEDCT_AMT AS DEDCT_AMT",
    "crf.PAYBL_AMT AS PAYBL_AMT",
    "crf.CLM_CT AS CLM_CT",
    "crf.MBR_AGE AS MBR_AGE",
    "CASE WHEN crf.SRC_SYS_CD = 'OPTUMRX' THEN crf.ADJ_FROM_CLM_ID WHEN NascoDupClm = 'N' THEN crf.ADJ_FROM_CLM_ID ELSE dup.NASCO_CLM_ID END AS ADJ_FROM_CLM_ID",
    "crf.ADJ_TO_CLM_ID AS ADJ_TO_CLM_ID",
    "crf.DOC_TX_ID AS DOC_TX_ID",
    "crf.MCAID_RESUB_NO AS MCAID_RESUB_NO",
    "crf.MCARE_ID AS MCARE_ID",
    "crf.MBR_SFX_NO AS MBR_SFX_NO",
    "crf.PATN_ACCT_NO AS PATN_ACCT_NO",
    "crf.PAYMT_REF_ID AS PAYMT_REF_ID",
    "crf.PROV_AGMNT_ID AS PROV_AGMNT_ID",
    "crf.RFRNG_PROV_TX AS RFRNG_PROV_TX",
    "crf.SUB_ID AS SUB_ID",
    "svPcaTypCdSk AS PCA_TYP_CD_SK",
    "svRelPcaClmSk AS REL_PCA_CLM_SK",
    "svRelBaseClmSk AS REL_BASE_CLM_SK",
    "crf.CLM_UPDT_SW AS CLM_UPDT_SW",
    "crf.CLCL_MICRO_ID AS CLCL_MICRO_ID",
    "crf.REMIT_SUPRSION_AMT AS REMIT_SUPRSION_AMT",
    "crf.MCAID_STTUS_ID AS MCAID_STTUS_ID",
    "crf.PATN_PD_AMT AS PATN_PD_AMT",
    "svClmSubmtICDVrsnCdSK AS CLM_SUBMT_ICD_VRSN_CD_SK",
    "crf.CLM_TXNMY_CD AS CLM_TXNMY_CD",
    "crf.BILL_PAYMT_EXCL_IN AS BILL_PAYMT_EXCL_IN",
    "crf.ADJ_FROM_CLM AS ADJ_FROM_CLM",
    "crf.ADJ_TO_CLM AS ADJ_TO_CLM"
)

df_DefaultUNK_sel = df_DefaultUNK.select(columns_for_merge_load)
df_DefaultNA_sel = df_DefaultNA.select(columns_for_merge_load)

# Union (Round-Robin collector) in the order DefaultUNK -> DefaultNA -> ClmFkeyOut1
df_merge_load = df_DefaultUNK_sel.unionByName(df_DefaultNA_sel).unionByName(df_ClmFkeyOut1_sel)

# For all char/varchar columns in the final output, apply rpad to match the declared length
char_pad = {
    "ATCHMT_IN":1,"CLNCL_EDIT_IN":1,"COBRA_CLM_IN":1,"FIRST_PASS_IN":1,"HOST_IN":1,"LTR_IN":1,"MCARE_ASG_IN":1,"NOTE_IN":1,"PCA_AUDIT_IN":1,"PCP_SUBMT_IN":1,
    "PROD_OOA_IN":1,"BILL_PAYMT_EXCL_IN":1,"CLM_UPDT_SW":1,
    "DOC_TX_ID":18,"MCAID_RESUB_NO":15,"MCARE_ID":12,"MBR_SFX_NO":2,"PAYMT_REF_ID":16,"PROV_AGMNT_ID":12,"SUB_ID":14,"CLCL_MICRO_ID":18,
    "ADJ_FROM_CLM":12,"ADJ_TO_CLM":12
}
df_final = df_merge_load
for c, ln in char_pad.items():
    df_final = df_final.withColumn(c, rpad(col(c), ln, " "))


# Produce outputs from Trns1
df_Recycle1 = df_Trns1.filter("ErrCount > 0").select(
    when(col("ErrCount")>0, GetRecycleKey(col("ClmCrfIn1.CLM_SK"))).otherwise(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    col("ClmCrfIn1.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    col("ClmCrfIn1.DISCARD_IN").alias("DISCARD_IN"),
    col("ClmCrfIn1.PASS_THRU_IN").alias("PASS_THRU_IN"),
    col("ClmCrfIn1.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    col("ErrCount").alias("ERR_CT"),
    (col("ClmCrfIn1.RECYCLE_CT")+1).alias("RECYCLE_CT"),
    col("ClmCrfIn1.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("ClmCrfIn1.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    col("ClmCrfIn1.CLM_SK").alias("CLM_SK"),
    col("ClmCrfIn1.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("ClmCrfIn1.CLM_ID").alias("CLM_ID"),
    col("ClmCrfIn1.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("RunCycle").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("ClmCrfIn1.ADJ_FROM_CLM").alias("ADJ_FROM_CLM"),
    col("ClmCrfIn1.ADJ_TO_CLM").alias("ADJ_TO_CLM"),
    col("ClmCrfIn1.ALPHA_PFX_CD").alias("ALPHA_PFX_CD"),
    col("ClmCrfIn1.CLM_EOB_EXCD").alias("CLM_EOB_EXCD"),
    col("ClmCrfIn1.CLS").alias("CLS"),
    col("ClmCrfIn1.CLS_PLN").alias("CLS_PLN"),
    col("ClmCrfIn1.EXPRNC_CAT").alias("EXPRNC_CAT"),
    col("ClmCrfIn1.FNCL_LOB_NO").alias("FNCL_LOB_NO"),
    col("ClmCrfIn1.GRP").alias("GRP"),
    col("ClmCrfIn1.MBR_CK").alias("MBR_CK"),
    col("ClmCrfIn1.NTWK").alias("NTWK"),
    col("ClmCrfIn1.PROD").alias("PROD"),
    col("ClmCrfIn1.SUBGRP").alias("SUBGRP"),
    col("ClmCrfIn1.SUB_CK").alias("SUB_CK"),
    col("ClmCrfIn1.CLM_ACDNT_CD").substr(1,2).alias("CLM_ACDNT_CD"),  # the JSON length=2
    col("ClmCrfIn1.CLM_ACDNT_ST_CD").alias("CLM_ACDNT_ST_CD"),
    col("ClmCrfIn1.CLM_ACTIVATING_BCBS_PLN_CD").alias("CLM_ACTIVATING_BCBS_PLN_CD"),
    col("ClmCrfIn1.CLM_AGMNT_SRC_CD").alias("CLM_AGMNT_SRC_CD"),
    col("ClmCrfIn1.CLM_BTCH_ACTN_CD").alias("CLM_BTCH_ACTN_CD"),
    col("ClmCrfIn1.CLM_CAP_CD").alias("CLM_CAP_CD"),
    col("ClmCrfIn1.CLM_CAT_CD").alias("CLM_CAT_CD"),
    col("ClmCrfIn1.CLM_CHK_CYC_OVRD_CD").alias("CLM_CHK_CYC_OVRD_CD"),
    col("ClmCrfIn1.CLM_COB_CD").alias("CLM_COB_CD"),
    col("ClmCrfIn1.FINL_DISP_CD").alias("FINL_DISP_CD"),
    col("ClmCrfIn1.CLM_INPT_METH_CD").alias("CLM_INPT_METH_CD"),
    col("ClmCrfIn1.CLM_INPT_SRC_CD").alias("CLM_INPT_SRC_CD"),
    col("ClmCrfIn1.CLM_IPP_CD").alias("CLM_IPP_CD"),
    col("ClmCrfIn1.CLM_NTWK_STTUS_CD").alias("CLM_NTWK_STTUS_CD"),
    col("ClmCrfIn1.CLM_NONPAR_PROV_PFX_CD").alias("CLM_NONPAR_PROV_PFX_CD"),
    col("ClmCrfIn1.CLM_OTHER_BNF_CD").alias("CLM_OTHER_BNF_CD"),
    col("ClmCrfIn1.CLM_PAYE_CD").alias("CLM_PAYE_CD"),
    col("ClmCrfIn1.CLM_PRCS_CTL_AGNT_PFX_CD").alias("CLM_PRCS_CTL_AGNT_PFX_CD"),
    col("ClmCrfIn1.CLM_SVC_DEFN_PFX_CD").alias("CLM_SVC_DEFN_PFX_CD"),
    col("ClmCrfIn1.CLM_SVC_PROV_SPEC_CD").alias("CLM_SVC_PROV_SPEC_CD"),
    col("ClmCrfIn1.CLM_SVC_PROV_TYP_CD").alias("CLM_SVC_PROV_TYP_CD"),
    col("ClmCrfIn1.CLM_STTUS_CD").alias("CLM_STTUS_CD"),
    col("ClmCrfIn1.CLM_SUBMT_BCBS_PLN_CD").alias("CLM_SUBMT_BCBS_PLN_CD"),
    col("ClmCrfIn1.CLM_SUB_BCBS_PLN_CD").alias("CLM_SUB_BCBS_PLN_CD"),
    col("ClmCrfIn1.CLM_SUBTYP_CD").alias("CLM_SUBTYP_CD"),
    col("ClmCrfIn1.CLM_TYP_CD").alias("CLM_TYP_CD"),
    col("ClmCrfIn1.ATCHMT_IN").alias("ATCHMT_IN"),
    col("ClmCrfIn1.CLM_CLNCL_EDIT_CD").alias("CLM_CLNCL_EDIT_CD"),
    col("ClmCrfIn1.COBRA_CLM_IN").alias("COBRA_CLM_IN"),
    col("ClmCrfIn1.FIRST_PASS_IN").alias("FIRST_PASS_IN"),
    col("ClmCrfIn1.HOST_IN").alias("HOST_IN"),
    col("ClmCrfIn1.LTR_IN").alias("LTR_IN"),
    col("ClmCrfIn1.MCARE_ASG_IN").alias("MCARE_ASG_IN"),
    col("ClmCrfIn1.NOTE_IN").alias("NOTE_IN"),
    col("ClmCrfIn1.PCA_AUDIT_IN").alias("PCA_AUDIT_IN"),
    col("ClmCrfIn1.PCP_SUBMT_IN").alias("PCP_SUBMT_IN"),
    col("ClmCrfIn1.PROD_OOA_IN").alias("PROD_OOA_IN"),
    col("ClmCrfIn1.ACDNT_DT").alias("ACDNT_DT"),
    col("ClmCrfIn1.INPT_DT").alias("INPT_DT"),
    col("ClmCrfIn1.MBR_PLN_ELIG_DT").alias("MBR_PLN_ELIG_DT"),
    col("ClmCrfIn1.NEXT_RVW_DT").alias("NEXT_RVW_DT"),
    col("ClmCrfIn1.PD_DT").alias("PD_DT"),
    col("ClmCrfIn1.PAYMT_DRAG_CYC_DT").alias("PAYMT_DRAG_CYC_DT"),
    col("ClmCrfIn1.PRCS_DT").alias("PRCS_DT"),
    col("ClmCrfIn1.RCVD_DT").alias("RCVD_DT"),
    col("ClmCrfIn1.SVC_STRT_DT").alias("SVC_STRT_DT"),
    col("ClmCrfIn1.SVC_END_DT").alias("SVC_END_DT"),
    col("ClmCrfIn1.SMLR_ILNS_DT").alias("SMLR_ILNS_DT"),
    col("ClmCrfIn1.STTUS_DT").alias("STTUS_DT"),
    col("ClmCrfIn1.WORK_UNABLE_BEG_DT").alias("WORK_UNABLE_BEG_DT"),
    col("ClmCrfIn1.WORK_UNABLE_END_DT").alias("WORK_UNABLE_END_DT"),
    col("ClmCrfIn1.ACDNT_AMT").alias("ACDNT_AMT"),
    col("ClmCrfIn1.ACTL_PD_AMT").alias("ACTL_PD_AMT"),
    col("ClmCrfIn1.ALLOW_AMT").alias("ALLOW_AMT"),
    col("ClmCrfIn1.DSALW_AMT").alias("DSALW_AMT"),
    col("ClmCrfIn1.COINS_AMT").alias("COINS_AMT"),
    col("ClmCrfIn1.CNSD_CHRG_AMT").alias("CNSD_CHRG_AMT"),
    col("ClmCrfIn1.COPAY_AMT").alias("COPAY_AMT"),
    col("ClmCrfIn1.CHRG_AMT").alias("CHRG_AMT"),
    col("ClmCrfIn1.DEDCT_AMT").alias("DEDCT_AMT"),
    col("ClmCrfIn1.PAYBL_AMT").alias("PAYBL_AMT"),
    col("ClmCrfIn1.CLM_CT").alias("CLM_CT"),
    col("ClmCrfIn1.MBR_AGE").alias("MBR_AGE"),
    col("ClmCrfIn1.ADJ_FROM_CLM_ID").alias("ADJ_FROM_CLM_ID"),
    col("ClmCrfIn1.ADJ_TO_CLM_ID").alias("ADJ_TO_CLM_ID"),
    col("ClmCrfIn1.DOC_TX_ID").alias("DOC_TX_ID"),
    col("ClmCrfIn1.MCAID_RESUB_NO").alias("MCAID_RESUB_NO"),
    col("ClmCrfIn1.MCARE_ID").alias("MCARE_ID"),
    col("ClmCrfIn1.MBR_SFX_NO").alias("MBR_SFX_NO"),
    col("ClmCrfIn1.PATN_ACCT_NO").alias("PATN_ACCT_NO"),
    col("ClmCrfIn1.PAYMT_REF_ID").alias("PAYMT_REF_ID"),
    col("ClmCrfIn1.PROV_AGMNT_ID").alias("PROV_AGMNT_ID"),
    col("ClmCrfIn1.RFRNG_PROV_TX").alias("RFRNG_PROV_TX"),
    col("ClmCrfIn1.SUB_ID").alias("SUB_ID"),
    col("ClmCrfIn1.PRPR_ENTITY").alias("PRPR_ENTITY"),
    col("ClmCrfIn1.PCA_TYP_CD").alias("PCA_TYP_CD"),
    col("ClmCrfIn1.REL_PCA_CLM_ID").alias("REL_PCA_CLM_ID"),
    col("ClmCrfIn1.CLCL_MICRO_ID").alias("CLCL_MICRO_ID"),
    col("ClmCrfIn1.CLM_UPDT_SW").alias("CLM_UPDT_SW"),
    col("ClmCrfIn1.REMIT_SUPRSION_AMT").alias("REMIT_SUPRSION_AMT"),
    col("ClmCrfIn1.MCAID_STTUS_ID").alias("MCAID_STTUS_ID"),
    col("ClmCrfIn1.PATN_PD_AMT").alias("PATN_PD_AMT"),
    col("ClmCrfIn1.CLM_SUBMT_ICD_VRSN_CD").alias("CLM_SUBMT_ICD_VRSN_CD"),
    col("ClmCrfIn1.CLM_TXNMY_CD").alias("CLM_TXNMY_CD")
)

df_ClmRvversalPaidDtUpdate = df_Trns1.filter(
    "trim(ClmCrfIn1.SRC_SYS_CD)='FACETS' AND "
    "((trim(ClmCrfIn1.CLM_STTUS_CD)='02') OR (trim(ClmCrfIn1.CLM_STTUS_CD)='91')) "
    "AND ClmAdjFromClmSk>1"
).select(
    col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
    col("ClmAdjFromClmSk").alias("CLM_SK"),
    col("ClmCrfIn1.ADJ_FROM_CLM").alias("CLM_ID"),
    col("AdjustedFacetsClaimStatus91").alias("CLM_STTUS_CD_SK"),
    col("RunCycle").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("PdDtSk").alias("PD_DT_SK"),
    col("ClmCrfIn1.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("ClmCrfIn1.STTUS_DT").alias("STTUS_DT")
)

df_Recycle_Clms = df_Trns1.filter("ErrCount > 0").select(
    col("ClmCrfIn1.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("ClmCrfIn1.CLM_ID").alias("CLM_ID")
)

# Write hf_recycle (scenario C → parquet)
df_Recycle1_ordered = df_Recycle1
write_files(
    df_Recycle1_ordered,
    f"{adls_path}/hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote='"',
    nullValue=None
)

# Write hf_claim_recycle_keys (scenario C → parquet)
df_Recycle_Clms_ordered = df_Recycle_Clms
write_files(
    df_Recycle_Clms_ordered,
    f"{adls_path}/hf_claim_recycle_keys.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote='"',
    nullValue=None
)

# Trans2 logic

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query_for_reversals = f"""
SELECT 
  CLM.SRC_SYS_CD_SK AS SRC_SYS_CD_SK,
  CLM.CLM_SK AS CLM_SK,
  CLM.CLM_ID AS CLM_ID,
  CLM.CLM_STTUS_CD_SK AS CLM_STTUS_CD_SK,
  CLM.PD_DT_SK AS PD_DT_SK
FROM {IDSOwner}.CLM CLM 
WHERE 
  CLM.SRC_SYS_CD_SK=? 
  AND CLM.CLM_SK=? 
  AND CLM.CLM_ID=?
"""
df_IDSClm_for_reversals = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_for_reversals)
    .load()
)

df_Trans2_input = (
    df_IDSClm_for_reversals.alias("Clm1")
    .join(
        df_ClmRvversalPaidDtUpdate.alias("ClmRvversalPaidDtUpdate"),
        [
          col("Clm1.SRC_SYS_CD_SK")==col("ClmRvversalPaidDtUpdate.SRC_SYS_CD_SK"),
          col("Clm1.CLM_SK")==col("ClmRvversalPaidDtUpdate.CLM_SK"),
          col("Clm1.CLM_ID")==col("ClmRvversalPaidDtUpdate.CLM_ID")
        ],
        "left"
    )
)

df_Trans2_vars = (
    df_Trans2_input
    .withColumn(
        "svClmMnths",
        when(
            (col("ClmRvversalPaidDtUpdate.STTUS_DT").isNull()) |
            (length(trim_col(col("ClmRvversalPaidDtUpdate.STTUS_DT")))==0),
            1
        ).otherwise(
            MonthDiff(
                col("ClmRvversalPaidDtUpdate.STTUS_DT"),
                FORMAT_DATE(lit("@Date"),lit("DATE"),lit("CURRENT"),lit("CCYY-MM-DD"))
            )
        )
    )
    .withColumn(
        "UpdateAdjFromFound",
        when(
            (col("Clm1.CLM_SK").isNotNull()) &
            (length(trim_col(col("Clm1.CLM_ID")))>0) &
            (
              col("Clm1.CLM_STTUS_CD_SK")==
              GetFkeyCodes(lit("FACETS"),col("ClmRvversalPaidDtUpdate.CLM_SK"),lit("CLAIM STATUS"),lit("R"),lit("X"))
            ) &
            (trim_col(col("Clm1.PD_DT_SK"))=="NA"),
            True
        ).otherwise(False)
    )
)

df_ClmRvversalUpdates = df_Trans2_vars.filter("UpdateAdjFromFound = true").select(
    col("ClmRvversalPaidDtUpdate.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("ClmRvversalPaidDtUpdate.CLM_SK").alias("CLM_SK"),
    col("ClmRvversalPaidDtUpdate.CLM_ID").alias("CLM_ID"),
    col("ClmRvversalPaidDtUpdate.CLM_STTUS_CD_SK").alias("CLM_STTUS_CD_SK"),
    col("ClmRvversalPaidDtUpdate.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("ClmRvversalPaidDtUpdate.PD_DT_SK").alias("PD_DT_SK")
)

df_EdwRevrslClmHitList = df_Trans2_vars.select(
    col("ClmRvversalPaidDtUpdate.CLM_SK").alias("CLM_SK"),
    col("ClmRvversalPaidDtUpdate.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("ClmRvversalPaidDtUpdate.CLM_ID").alias("CLM_ID")
)

# Write found_clms (CSeqFileStage => CSV) in "load" folder
df_ClmRvversalUpdates_ordered = df_ClmRvversalUpdates

# Write Edw_Rvrsl_Clm_Hit_List (CSeqFileStage => CSV) in "landing" => use adls_path_raw
df_EdwRevrslClmHitList_ordered = df_EdwRevrslClmHitList
write_files(
    df_EdwRevrslClmHitList_ordered,
    f"{adls_path_raw}/landing/CLM.RVRSLHITLIST.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

# COMMAND ----------
from pyspark.sql.functions import col, when, lit, rpad, coalesce
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import DataFrame
# Parameter retrieval
Source = get_widget_value('Source', '')
SrcSysCdSk = get_widget_value('SrcSysCdSk', '')
InFile = get_widget_value('InFile', '')
Logging = get_widget_value('Logging', '')
RunCycle = get_widget_value('RunCycle', '')
RunID = get_widget_value('RunID', '')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

# Acquire JDBC config for IDS
jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# Read from DB2Connector: IDSClm_for_Pca_Update
extract_query_IDSClm_for_Pca_Update = f"""
SELECT
  CLM.CLM_SK as CLM_SK,
  CLM.CLM_ID as CLM_ID,
  CLM.LAST_UPDT_RUN_CYC_EXCTN_SK as LAST_UPDT_RUN_CYC_EXCTN_SK,
  CLM.REL_BASE_CLM_SK as REL_BASE_CLM_SK
FROM {IDSOwner}.CLM CLM
WHERE CLM.CLM_SK=?
"""
df_IDSClm_for_Pca_Update = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_IDSClm_for_Pca_Update)
    .load()
)

# Read from DB2Connector: IDS_K_CLM_FROM
extract_query_IDS_K_CLM_FROM = f"""
SELECT
  CLM.SRC_SYS_CD_SK as SRC_SYS_CD_SK,
  CLM.CLM_ID as CLM_ID,
  CLM.CLM_SK as CLM_SK
FROM {IDSOwner}.K_CLM CLM
WHERE CLM.SRC_SYS_CD_SK=? AND CLM.CLM_ID=?
"""
df_IDS_K_CLM_FROM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_IDS_K_CLM_FROM)
    .load()
)

# Read from DB2Connector: IDS_K_CLM_TO
extract_query_IDS_K_CLM_TO = f"""
SELECT
  CLM.SRC_SYS_CD_SK as SRC_SYS_CD_SK,
  CLM.CLM_ID as CLM_ID,
  CLM.CLM_SK as CLM_SK
FROM {IDSOwner}.K_CLM CLM
WHERE CLM.SRC_SYS_CD_SK=? AND CLM.CLM_ID=?
"""
df_IDS_K_CLM_TO = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_IDS_K_CLM_TO)
    .load()
)

# Read from DB2Connector: PCA_Claims
extract_query_PCA_Claims = f"""
SELECT
  CLM.SRC_SYS_CD_SK as SRC_SYS_CD_SK,
  CLM.CLM_ID as CLM_ID,
  CLM.CLM_SK as CLM_SK
FROM {IDSOwner}.CLM CLM
WHERE CLM.SRC_SYS_CD_SK=? AND CLM.CLM_ID=?
"""
df_PCA_Claims = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_PCA_Claims)
    .load()
)

# Read from CSeqFileStage: IdsClmFkey_output_1 (scenario C: read as parquet)
df_IdsClmFkey_output_1 = df_final.select(columns_for_merge_load)

# -------------------------------------------------------------------
# Trans6 logic: two output links with constraints
# We first select all columns as they appear in DataStage, applying rpad to char columns
df_Trans6_allcols = (
    df_IdsClmFkey_output_1
    .select(
        col("CLM_SK"),
        col("SRC_SYS_CD_SK"),
        col("CLM_ID"),
        col("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("ADJ_FROM_CLM_SK"),
        col("ADJ_TO_CLM_SK"),
        col("ALPHA_PFX_SK"),
        col("CLM_EOB_EXCD_SK"),
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
        col("CLM_ACDNT_CD_SK"),
        col("CLM_ACDNT_ST_CD_SK"),
        col("CLM_ACTV_BCBS_PLN_CD_SK"),
        col("CLM_AGMNT_SRC_CD_SK"),
        col("CLM_BTCH_ACTN_CD_SK"),
        col("CLM_CAP_CD_SK"),
        col("CLM_CAT_CD_SK"),
        col("CLM_CHK_CYC_OVRD_CD_SK"),
        col("CLM_COB_CD_SK"),
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
        rpad(col("HOST_IN"), 1, " ").alias("HOST_IN"),
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
        col("PCA_TYP_CD_SK"),
        col("REL_PCA_CLM_SK"),
        col("REL_BASE_CLM_SK"),
        rpad(col("CLM_UPDT_SW"), 1, " ").alias("CLM_UPDT_SW"),
        col("REMIT_SUPRSION_AMT"),
        col("MCAID_STTUS_ID"),
        col("PATN_PD_AMT"),
        col("CLM_SUBMT_ICD_VRSN_CD_SK"),
        col("CLM_TXNMY_CD"),
        rpad(col("BILL_PAYMT_EXCL_IN"), 1, " ").alias("BILL_PAYMT_EXCL_IN"),
        rpad(col("ADJ_FROM_CLM"), 12, " ").alias("ADJ_FROM_CLM"),
        rpad(col("ADJ_TO_CLM"), 12, " ").alias("ADJ_TO_CLM")
    )
)

# Split the data into two frames based on constraints
df_Trans6_link1 = df_Trans6_allcols.filter((col("PCA_TYP_CD_SK") == 0) | (col("PCA_TYP_CD_SK") == 1))
df_Trans6_link2 = df_Trans6_allcols.filter(~((col("PCA_TYP_CD_SK") == 0) | (col("PCA_TYP_CD_SK") == 1)))

# ---------------------------------------------------------------------------------
# ER stage is a hashed file with key = CLM_SK => scenario A => remove duplicates on CLM_SK
df_Trans6_link1_dedup = dedup_sort(df_Trans6_link1, ["CLM_SK"], [])

# Next stage after ER => merge_load1 (via link "col1")
df_merge_load1_input_col1 = df_Trans6_link1_dedup

# ---------------------------------------------------------------------------------
# Trans7: has two input pins: Link2 (primary), baseid (reference from df_PCA_Claims)
# We replicate a left join on baseid with condition (Link2.SRC_SYS_CD_SK == baseid.SRC_SYS_CD_SK) & (Link2.CLM_ID == baseid.CLM_ID)
primary_T7 = df_Trans6_link2.alias("l")
ref_baseid = df_PCA_Claims.alias("r")

join_cond_T7 = [
    col("l.SRC_SYS_CD_SK") == col("r.SRC_SYS_CD_SK"),
    col("l.CLM_ID") == col("r.CLM_ID")
]

df_Trans7_joined = primary_T7.join(ref_baseid, join_cond_T7, how="left")

# Apply transformations for "Link3" => output to "hf_idsclmfkey_col2"
# "REL_BASE_CLM_SK" => "If IsNull(baseid.CLM_SK)=false then baseid.CLM_SK else Link2.REL_BASE_CLM_SK"
df_Trans7_Link3 = df_Trans7_joined.select(
    col("l.CLM_SK").alias("CLM_SK"),
    col("l.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("l.CLM_ID").alias("CLM_ID"),
    col("l.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("l.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("l.ADJ_FROM_CLM_SK").alias("ADJ_FROM_CLM_SK"),
    col("l.ADJ_TO_CLM_SK").alias("ADJ_TO_CLM_SK"),
    col("l.ALPHA_PFX_SK").alias("ALPHA_PFX_SK"),
    col("l.CLM_EOB_EXCD_SK").alias("CLM_EOB_EXCD_SK"),
    col("l.CLS_SK").alias("CLS_SK"),
    col("l.CLS_PLN_SK").alias("CLS_PLN_SK"),
    col("l.EXPRNC_CAT_SK").alias("EXPRNC_CAT_SK"),
    col("l.FNCL_LOB_SK").alias("FNCL_LOB_SK"),
    col("l.GRP_SK").alias("GRP_SK"),
    col("l.MBR_SK").alias("MBR_SK"),
    col("l.NTWK_SK").alias("NTWK_SK"),
    col("l.PROD_SK").alias("PROD_SK"),
    col("l.SUBGRP_SK").alias("SUBGRP_SK"),
    col("l.SUB_SK").alias("SUB_SK"),
    col("l.CLM_ACDNT_CD_SK").alias("CLM_ACDNT_CD_SK"),
    col("l.CLM_ACDNT_ST_CD_SK").alias("CLM_ACDNT_ST_CD_SK"),
    col("l.CLM_ACTV_BCBS_PLN_CD_SK").alias("CLM_ACTV_BCBS_PLN_CD_SK"),
    col("l.CLM_AGMNT_SRC_CD_SK").alias("CLM_AGMNT_SRC_CD_SK"),
    col("l.CLM_BTCH_ACTN_CD_SK").alias("CLM_BTCH_ACTN_CD_SK"),
    col("l.CLM_CAP_CD_SK").alias("CLM_CAP_CD_SK"),
    col("l.CLM_CAT_CD_SK").alias("CLM_CAT_CD_SK"),
    col("l.CLM_CHK_CYC_OVRD_CD_SK").alias("CLM_CHK_CYC_OVRD_CD_SK"),
    col("l.CLM_COB_CD_SK").alias("CLM_COB_CD_SK"),
    col("l.CLM_FINL_DISP_CD_SK").alias("CLM_FINL_DISP_CD_SK"),
    col("l.CLM_INPT_METH_CD_SK").alias("CLM_INPT_METH_CD_SK"),
    col("l.CLM_INPT_SRC_CD_SK").alias("CLM_INPT_SRC_CD_SK"),
    col("l.CLM_INTER_PLN_PGM_CD_SK").alias("CLM_INTER_PLN_PGM_CD_SK"),
    col("l.CLM_NTWK_STTUS_CD_SK").alias("CLM_NTWK_STTUS_CD_SK"),
    col("l.CLM_NONPAR_PROV_PFX_CD_SK").alias("CLM_NONPAR_PROV_PFX_CD_SK"),
    col("l.CLM_OTHR_BNF_CD_SK").alias("CLM_OTHR_BNF_CD_SK"),
    col("l.CLM_PAYE_CD_SK").alias("CLM_PAYE_CD_SK"),
    col("l.CLM_PRCS_CTL_AGNT_PFX_CD_SK").alias("CLM_PRCS_CTL_AGNT_PFX_CD_SK"),
    col("l.CLM_SVC_DEFN_PFX_CD_SK").alias("CLM_SVC_DEFN_PFX_CD_SK"),
    col("l.CLM_SVC_PROV_SPEC_CD_SK").alias("CLM_SVC_PROV_SPEC_CD_SK"),
    col("l.CLM_SVC_PROV_TYP_CD_SK").alias("CLM_SVC_PROV_TYP_CD_SK"),
    col("l.CLM_STTUS_CD_SK").alias("CLM_STTUS_CD_SK"),
    col("l.CLM_SUBMTTING_BCBS_PLN_CD_SK").alias("CLM_SUBMTTING_BCBS_PLN_CD_SK"),
    col("l.CLM_SUB_BCBS_PLN_CD_SK").alias("CLM_SUB_BCBS_PLN_CD_SK"),
    col("l.CLM_SUBTYP_CD_SK").alias("CLM_SUBTYP_CD_SK"),
    col("l.CLM_TYP_CD_SK").alias("CLM_TYP_CD_SK"),
    col("l.ATCHMT_IN").alias("ATCHMT_IN"),
    col("l.CLNCL_EDIT_IN").alias("CLNCL_EDIT_IN"),
    col("l.COBRA_CLM_IN").alias("COBRA_CLM_IN"),
    col("l.FIRST_PASS_IN").alias("FIRST_PASS_IN"),
    col("l.HOST_IN").alias("HOST_IN"),
    col("l.LTR_IN").alias("LTR_IN"),
    col("l.MCARE_ASG_IN").alias("MCARE_ASG_IN"),
    col("l.NOTE_IN").alias("NOTE_IN"),
    col("l.PCA_AUDIT_IN").alias("PCA_AUDIT_IN"),
    col("l.PCP_SUBMT_IN").alias("PCP_SUBMT_IN"),
    col("l.PROD_OOA_IN").alias("PROD_OOA_IN"),
    col("l.ACDNT_DT_SK").alias("ACDNT_DT_SK"),
    col("l.INPT_DT_SK").alias("INPT_DT_SK"),
    col("l.MBR_PLN_ELIG_DT_SK").alias("MBR_PLN_ELIG_DT_SK"),
    col("l.NEXT_RVW_DT_SK").alias("NEXT_RVW_DT_SK"),
    col("l.PD_DT_SK").alias("PD_DT_SK"),
    col("l.PAYMT_DRAG_CYC_DT_SK").alias("PAYMT_DRAG_CYC_DT_SK"),
    col("l.PRCS_DT_SK").alias("PRCS_DT_SK"),
    col("l.RCVD_DT_SK").alias("RCVD_DT_SK"),
    col("l.SVC_STRT_DT_SK").alias("SVC_STRT_DT_SK"),
    col("l.SVC_END_DT_SK").alias("SVC_END_DT_SK"),
    col("l.SMLR_ILNS_DT_SK").alias("SMLR_ILNS_DT_SK"),
    col("l.STTUS_DT_SK").alias("STTUS_DT_SK"),
    col("l.WORK_UNABLE_BEG_DT_SK").alias("WORK_UNABLE_BEG_DT_SK"),
    col("l.WORK_UNABLE_END_DT_SK").alias("WORK_UNABLE_END_DT_SK"),
    col("l.ACDNT_AMT").alias("ACDNT_AMT"),
    col("l.ACTL_PD_AMT").alias("ACTL_PD_AMT"),
    col("l.ALW_AMT").alias("ALW_AMT"),
    col("l.DSALW_AMT").alias("DSALW_AMT"),
    col("l.COINS_AMT").alias("COINS_AMT"),
    col("l.CNSD_CHRG_AMT").alias("CNSD_CHRG_AMT"),
    col("l.COPAY_AMT").alias("COPAY_AMT"),
    col("l.CHRG_AMT").alias("CHRG_AMT"),
    col("l.DEDCT_AMT").alias("DEDCT_AMT"),
    col("l.PAYBL_AMT").alias("PAYBL_AMT"),
    col("l.CLM_CT").alias("CLM_CT"),
    col("l.MBR_AGE").alias("MBR_AGE"),
    col("l.ADJ_FROM_CLM_ID").alias("ADJ_FROM_CLM_ID"),
    col("l.ADJ_TO_CLM_ID").alias("ADJ_TO_CLM_ID"),
    col("l.DOC_TX_ID").alias("DOC_TX_ID"),
    col("l.MCAID_RESUB_NO").alias("MCAID_RESUB_NO"),
    col("l.MCARE_ID").alias("MCARE_ID"),
    col("l.MBR_SFX_NO").alias("MBR_SFX_NO"),
    col("l.PATN_ACCT_NO").alias("PATN_ACCT_NO"),
    col("l.PAYMT_REF_ID").alias("PAYMT_REF_ID"),
    col("l.PROV_AGMNT_ID").alias("PROV_AGMNT_ID"),
    col("l.RFRNG_PROV_TX").alias("RFRNG_PROV_TX"),
    col("l.SUB_ID").alias("SUB_ID"),
    col("l.PCA_TYP_CD_SK").alias("PCA_TYP_CD_SK"),
    col("l.REL_PCA_CLM_SK").alias("REL_PCA_CLM_SK"),
    when(col("r.CLM_SK").isNotNull(), col("r.CLM_SK")).otherwise(col("l.REL_BASE_CLM_SK")).alias("REL_BASE_CLM_SK"),
    col("l.CLM_UPDT_SW").alias("CLM_UPDT_SW"),
    col("l.REMIT_SUPRSION_AMT").alias("REMIT_SUPRSION_AMT"),
    col("l.MCAID_STTUS_ID").alias("MCAID_STTUS_ID"),
    col("l.PATN_PD_AMT").alias("PATN_PD_AMT"),
    col("l.CLM_SUBMT_ICD_VRSN_CD_SK").alias("CLM_SUBMT_ICD_VRSN_CD_SK"),
    col("l.CLM_TXNMY_CD").alias("CLM_TXNMY_CD"),
    col("l.BILL_PAYMT_EXCL_IN").alias("BILL_PAYMT_EXCL_IN"),
    col("l.ADJ_FROM_CLM").alias("ADJ_FROM_CLM"),
    col("l.ADJ_TO_CLM").alias("ADJ_TO_CLM")
)

# hf_idsclmfkey_col2 => scenario A => remove duplicates on CLM_SK
df_Trans7_Link3_dedup = dedup_sort(df_Trans7_Link3, ["CLM_SK"], [])

# Both col1 and col2 feed into merge_load1 => collector => so we union them
df_merge_load1_input_col2 = df_Trans7_Link3_dedup

df_merge_load1 = df_merge_load1_input_col1.unionByName(df_merge_load1_input_col2)

# -------------------------------------------------------------------
# merge_load1 => Output pin => "OutFile" => goes to Trans3
df_Trans3_input = df_merge_load1.select(
    col("CLM_SK"),
    col("SRC_SYS_CD_SK"),
    col("CLM_ID"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("ADJ_FROM_CLM_SK"),
    col("ADJ_TO_CLM_SK"),
    col("ALPHA_PFX_SK"),
    col("CLM_EOB_EXCD_SK"),
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
    col("CLM_ACDNT_CD_SK"),
    col("CLM_ACDNT_ST_CD_SK"),
    col("CLM_ACTV_BCBS_PLN_CD_SK"),
    col("CLM_AGMNT_SRC_CD_SK"),
    col("CLM_BTCH_ACTN_CD_SK"),
    col("CLM_CAP_CD_SK"),
    col("CLM_CAT_CD_SK"),
    col("CLM_CHK_CYC_OVRD_CD_SK"),
    col("CLM_COB_CD_SK"),
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
    col("ATCHMT_IN"),
    col("CLNCL_EDIT_IN"),
    col("COBRA_CLM_IN"),
    col("FIRST_PASS_IN"),
    col("HOST_IN"),
    col("LTR_IN"),
    col("MCARE_ASG_IN"),
    col("NOTE_IN"),
    col("PCA_AUDIT_IN"),
    col("PCP_SUBMT_IN"),
    col("PROD_OOA_IN"),
    col("ACDNT_DT_SK"),
    col("INPT_DT_SK"),
    col("MBR_PLN_ELIG_DT_SK"),
    col("NEXT_RVW_DT_SK"),
    col("PD_DT_SK"),
    col("PAYMT_DRAG_CYC_DT_SK"),
    col("PRCS_DT_SK"),
    col("RCVD_DT_SK"),
    col("SVC_STRT_DT_SK"),
    col("SVC_END_DT_SK"),
    col("SMLR_ILNS_DT_SK"),
    col("STTUS_DT_SK"),
    col("WORK_UNABLE_BEG_DT_SK"),
    col("WORK_UNABLE_END_DT_SK"),
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
    col("DOC_TX_ID"),
    col("MCAID_RESUB_NO"),
    col("MCARE_ID"),
    col("MBR_SFX_NO"),
    col("PATN_ACCT_NO"),
    col("PAYMT_REF_ID"),
    col("PROV_AGMNT_ID"),
    col("RFRNG_PROV_TX"),
    col("SUB_ID"),
    col("PCA_TYP_CD_SK"),
    col("REL_PCA_CLM_SK"),
    col("REL_BASE_CLM_SK"),
    col("CLM_UPDT_SW"),
    col("REMIT_SUPRSION_AMT"),
    col("MCAID_STTUS_ID"),
    col("PATN_PD_AMT"),
    col("CLM_SUBMT_ICD_VRSN_CD_SK"),
    col("CLM_TXNMY_CD"),
    col("BILL_PAYMT_EXCL_IN"),
    col("ADJ_FROM_CLM"),
    col("ADJ_TO_CLM")
)

# Trans3 has a stage variable: svAdjTOandFROM => 
#  "If (Source = 'LUMERIS' and OutFile.ADJ_TO_CLM_ID[1,1] <> 'L') Or (Source='FACETS' and OutFile.ADJ_FROM_CLM_ID[1,1] = 'L') Then 'Y' Else 'N'"
# We'll replicate that logic
df_Trans3_sv = df_Trans3_input.withColumn(
    "svAdjTOandFROM",
    when(
        ((lit(Source) == lit("LUMERIS")) & (col("ADJ_TO_CLM_ID").substr(1,1) != lit("L"))) |
        ((lit(Source) == lit("FACETS")) & (col("ADJ_FROM_CLM_ID").substr(1,1) == lit("L"))),
        lit("Y")
    ).otherwise(lit("N"))
)

# Now we produce 3 outputs from Trans3:
# 1) "LoadFile_In" => constraint: svAdjTOandFROM='N'
df_Trans3_LoadFile_In = df_Trans3_sv.filter(col("svAdjTOandFROM") == lit("N"))

# 2) "Lnk_Adj_From_TO" => constraint: svAdjTOandFROM='Y'
#   plus two extra columns at transformation's end: "SRC_SYS_CD_SK_FROM", "SRC_SYS_CD_SK_TO" => 
#   "GetFkeyCodes("IDS",1,"SOURCE SYSTEM","LUMERIS","X")", "GetFkeyCodes("IDS",1,"SOURCE SYSTEM","FACETS","X")"
#   We treat them as already-defined functions returning some integer, so let's just assume the columns are set to that expression.
df_Trans3_Lnk_Adj_From_TO = df_Trans3_sv.filter(col("svAdjTOandFROM") == lit("Y")).select(
    col("CLM_SK"),
    col("SRC_SYS_CD_SK"),
    col("CLM_ID"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("ADJ_FROM_CLM_SK"),
    col("ADJ_TO_CLM_SK"),
    col("ALPHA_PFX_SK"),
    col("CLM_EOB_EXCD_SK"),
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
    col("CLM_ACDNT_CD_SK"),
    col("CLM_ACDNT_ST_CD_SK"),
    col("CLM_ACTV_BCBS_PLN_CD_SK"),
    col("CLM_AGMNT_SRC_CD_SK"),
    col("CLM_BTCH_ACTN_CD_SK"),
    col("CLM_CAP_CD_SK"),
    col("CLM_CAT_CD_SK"),
    col("CLM_CHK_CYC_OVRD_CD_SK"),
    col("CLM_COB_CD_SK"),
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
    col("ATCHMT_IN"),
    col("CLNCL_EDIT_IN"),
    col("COBRA_CLM_IN"),
    col("FIRST_PASS_IN"),
    col("HOST_IN"),
    col("LTR_IN"),
    col("MCARE_ASG_IN"),
    col("NOTE_IN"),
    col("PCA_AUDIT_IN"),
    col("PCP_SUBMT_IN"),
    col("PROD_OOA_IN"),
    col("ACDNT_DT_SK"),
    col("INPT_DT_SK"),
    col("MBR_PLN_ELIG_DT_SK"),
    col("NEXT_RVW_DT_SK"),
    col("PD_DT_SK"),
    col("PAYMT_DRAG_CYC_DT_SK"),
    col("PRCS_DT_SK"),
    col("RCVD_DT_SK"),
    col("SVC_STRT_DT_SK"),
    col("SVC_END_DT_SK"),
    col("SMLR_ILNS_DT_SK"),
    col("STTUS_DT_SK"),
    col("WORK_UNABLE_BEG_DT_SK"),
    col("WORK_UNABLE_END_DT_SK"),
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
    col("DOC_TX_ID"),
    col("MCAID_RESUB_NO"),
    col("MCARE_ID"),
    col("MBR_SFX_NO"),
    col("PATN_ACCT_NO"),
    col("PAYMT_REF_ID"),
    col("PROV_AGMNT_ID"),
    col("RFRNG_PROV_TX"),
    col("SUB_ID"),
    col("PCA_TYP_CD_SK"),
    col("REL_PCA_CLM_SK"),
    col("REL_BASE_CLM_SK"),
    col("REMIT_SUPRSION_AMT"),
    col("MCAID_STTUS_ID"),
    col("PATN_PD_AMT"),
    col("CLM_SUBMT_ICD_VRSN_CD_SK"),
    col("CLM_TXNMY_CD"),
    col("BILL_PAYMT_EXCL_IN"),
    col("ADJ_FROM_CLM"),
    col("ADJ_TO_CLM"),
    # two new columns for reference codes
    GetFkeyCodes("IDS", 1, "SOURCE SYSTEM", "LUMERIS", "X").alias("SRC_SYS_CD_SK_FROM"),
    GetFkeyCodes("IDS", 1, "SOURCE SYSTEM", "FACETS", "X").alias("SRC_SYS_CD_SK_TO")
)

# 3) "ClmPcaUpdt" => constraint OutFile.CLM_UPDT_SW <> 'N'
#   columns: REL_BASE_CLM_SK (PK?), CLM_SK, LAST_UPDT_RUN_CYC_EXCTN_SK
df_Trans3_ClmPcaUpdt = df_Trans3_sv.filter(col("CLM_UPDT_SW") != lit("N")).select(
    col("REL_BASE_CLM_SK").alias("REL_BASE_CLM_SK"),
    col("CLM_SK").alias("CLM_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

# -------------------------------------------------------------------
# "ClmPcaUpdts" => CSeqFileStage => scenario C => write then read back
# Write the df_Trans3_ClmPcaUpdt as "CLM_PCA_UPDT.parquet"
write_files(
    df_Trans3_ClmPcaUpdt,
    f"{adls_path}/load/CLM_PCA_UPDT.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote='"',
    nullValue=None
)

# Now read it back
df_ClmPcaUpdts = spark.read.format("parquet").load(f"{adls_path}/load/CLM_PCA_UPDT.parquet")

# Trans5: input pins => "PcaClm2Updt" from df_ClmPcaUpdts, "Clm2" from df_IDSClm_for_Pca_Update
# constraint: "IsNull(Clm2.CLM_SK)=false" => effectively an inner join on CLM_SK
left_Trans5 = df_ClmPcaUpdts.alias("p")
right_Trans5 = df_IDSClm_for_Pca_Update.alias("c")

df_Trans5_join = left_Trans5.join(right_Trans5, (col("p.CLM_SK") == col("c.CLM_SK")), "inner")

# Output => "updtCLAIM" => columns
df_Trans5_updtCLAIM = df_Trans5_join.select(
    col("c.CLM_SK").alias("CLM_SK"),
    col("p.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("p.CLM_SK").alias("REL_PCA_CLM_SK"),
    lit("1").alias("REL_BASE_CLM_SK")
)

# -------------------------------------------------------------------
# "PcaClmUpdate" => DB2Connector => We'll do a MERGE into IDS.CLM
# The DataStage SQL was an UPDATE. We'll replicate as a MERGE using columns:
# Key: CLM_SK
# Updating: LAST_UPDT_RUN_CYC_EXCTN_SK, REL_PCA_CLM_SK, REL_BASE_CLM_SK
df_PcaClmUpdate = df_Trans5_updtCLAIM

# Create staging table STAGING.IdsClmFkey_1_PcaClmUpdate_temp
df_PcaClmUpdate.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.IdsClmFkey_1_PcaClmUpdate_temp") \
    .mode("overwrite") \
    .save()

merge_sql_PcaClmUpdate = """
MERGE INTO {0}.CLM AS T
USING STAGING.IdsClmFkey_1_PcaClmUpdate_temp AS S
ON T.CLM_SK = S.CLM_SK
WHEN MATCHED THEN
  UPDATE SET
    T.LAST_UPDT_RUN_CYC_EXCTN_SK = S.LAST_UPDT_RUN_CYC_EXCTN_SK,
    T.REL_PCA_CLM_SK = S.REL_PCA_CLM_SK,
    T.REL_BASE_CLM_SK = S.REL_BASE_CLM_SK
WHEN NOT MATCHED THEN
  INSERT (CLM_SK, LAST_UPDT_RUN_CYC_EXCTN_SK, REL_PCA_CLM_SK, REL_BASE_CLM_SK)
  VALUES (S.CLM_SK, S.LAST_UPDT_RUN_CYC_EXCTN_SK, S.REL_PCA_CLM_SK, S.REL_BASE_CLM_SK);
""".format(IDSOwner)

execute_dml(merge_sql_PcaClmUpdate, jdbc_url, jdbc_props)

# -------------------------------------------------------------------
# Tfrm_SrcSK => we handled above as df_Tfrm_SrcSK is effectively the logic done for Lnk_Adj_From_TO with two reference lookups.
# We produce df_Tfrm_SrcSK => "Lnk_Adj_to_SK" => union => done.

# Link_Collector_237 => merges df_Trans3_LoadFile_In and df_Tfrm_SrcSK result => we do a union
# In our code, df_Trans3_LoadFile_In is one piece, df_Tfrm_SrcSK is the other piece. Before that, in the job
# "Tfrm_SrcSK" => "Lnk_Adj_to_SK" => we already created df, parted to "Link_Collector_237".
df_Link_Collector_237_Input1 = df_Trans3_LoadFile_In
df_Link_Collector_237_Input2 = df_Trans3_Lnk_Adj_From_TO

df_Link_Collector_237 = df_Link_Collector_237_Input1.unionByName(df_Link_Collector_237_Input2)

# Finally => "ClmLoadFile" => CSeqFileStage => scenario C => we write to "load/CLM.#Source#.dat" as parquet
df_ClmLoadFile = df_Link_Collector_237.select(
    col("CLM_SK"),
    col("SRC_SYS_CD_SK"),
    col("CLM_ID"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("ADJ_FROM_CLM_SK"),
    col("ADJ_TO_CLM_SK"),
    col("ALPHA_PFX_SK"),
    col("CLM_EOB_EXCD_SK"),
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
    col("CLM_ACDNT_CD_SK"),
    col("CLM_ACDNT_ST_CD_SK"),
    col("CLM_ACTV_BCBS_PLN_CD_SK"),
    col("CLM_AGMNT_SRC_CD_SK"),
    col("CLM_BTCH_ACTN_CD_SK"),
    col("CLM_CAP_CD_SK"),
    col("CLM_CAT_CD_SK"),
    col("CLM_CHK_CYC_OVRD_CD_SK"),
    col("CLM_COB_CD_SK"),
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
    col("ATCHMT_IN"),
    col("CLNCL_EDIT_IN"),
    col("COBRA_CLM_IN"),
    col("FIRST_PASS_IN"),
    col("HOST_IN"),
    col("LTR_IN"),
    col("MCARE_ASG_IN"),
    col("NOTE_IN"),
    col("PCA_AUDIT_IN"),
    col("PCP_SUBMT_IN"),
    col("PROD_OOA_IN"),
    col("ACDNT_DT_SK"),
    col("INPT_DT_SK"),
    col("MBR_PLN_ELIG_DT_SK"),
    col("NEXT_RVW_DT_SK"),
    col("PD_DT_SK"),
    col("PAYMT_DRAG_CYC_DT_SK"),
    col("PRCS_DT_SK"),
    col("RCVD_DT_SK"),
    col("SVC_STRT_DT_SK"),
    col("SVC_END_DT_SK"),
    col("SMLR_ILNS_DT_SK"),
    col("STTUS_DT_SK"),
    col("WORK_UNABLE_BEG_DT_SK"),
    col("WORK_UNABLE_END_DT_SK"),
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
    col("DOC_TX_ID"),
    col("MCAID_RESUB_NO"),
    col("MCARE_ID"),
    col("MBR_SFX_NO"),
    col("PATN_ACCT_NO"),
    col("PAYMT_REF_ID"),
    col("PROV_AGMNT_ID"),
    col("RFRNG_PROV_TX"),
    col("SUB_ID"),
    col("PCA_TYP_CD_SK"),
    col("REL_PCA_CLM_SK"),
    col("REL_BASE_CLM_SK"),
    col("REMIT_SUPRSION_AMT"),
    col("MCAID_STTUS_ID"),
    col("PATN_PD_AMT"),
    col("CLM_SUBMT_ICD_VRSN_CD_SK"),
    col("CLM_TXNMY_CD"),
    col("BILL_PAYMT_EXCL_IN")
)

# Write the final ClmLoadFile to "CLM.{Source}.dat" => scenario C => .parquet
write_files(
    df_ClmLoadFile,
    f"{adls_path}/load/CLM.{Source}.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote='"',
    nullValue=None
)


# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when, rpad
from pyspark.sql.types import StringType, IntegerType

Source = get_widget_value('Source','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
InFile = get_widget_value('InFile','')
Logging = get_widget_value('Logging','')
RunCycle = get_widget_value('RunCycle','')
RunID = get_widget_value('RunID','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

df_nasco_orig = spark.read.parquet(f"{adls_path}/hf_clm_nasco_orig.parquet")
df_hf_Clm_LkupHsh = spark.read.parquet(f"{adls_path}/hf_clm.parquet")

df_found_clms = df_ClmRvversalUpdates_ordered

df_found_clms = df_found_clms.withColumn("PD_DT_SK", rpad(col("PD_DT_SK"), 10, " "))

execute_dml("DROP TABLE IF EXISTS STAGING.IdsClmFkey_2_CLM_temp", jdbc_url, jdbc_props)

df_found_clms.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.IdsClmFkey_2_CLM_temp") \
    .mode("overwrite") \
    .save()

merge_sql_0 = f"""
MERGE INTO {IDSOwner}.CLM as target
USING STAGING.IdsClmFkey_2_CLM_temp as source
ON target.SRC_SYS_CD_SK=source.SRC_SYS_CD_SK
AND target.CLM_SK=source.CLM_SK
AND target.CLM_ID=source.CLM_ID
AND target.CLM_STTUS_CD_SK=source.CLM_STTUS_CD_SK
WHEN MATCHED THEN
  UPDATE SET target.LAST_UPDT_RUN_CYC_EXCTN_SK=source.LAST_UPDT_RUN_CYC_EXCTN_SK,
             target.PD_DT_SK=source.PD_DT_SK
WHEN NOT MATCHED THEN
  INSERT (SRC_SYS_CD_SK, CLM_SK, CLM_ID, CLM_STTUS_CD_SK, LAST_UPDT_RUN_CYC_EXCTN_SK, PD_DT_SK)
  VALUES (source.SRC_SYS_CD_SK, source.CLM_SK, source.CLM_ID, source.CLM_STTUS_CD_SK, source.LAST_UPDT_RUN_CYC_EXCTN_SK, source.PD_DT_SK);
"""
execute_dml(merge_sql_0, jdbc_url, jdbc_props)

df_IdsClm = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"SELECT CLM_SK, SRC_SYS_CD_SK, CLM_ID, CLM_STTUS_CD_SK, PD_DT_SK FROM {IDSOwner}.CLM")
    .load()
)

df_ClmNascoLkup = df_IdsClm.alias("c").join(
    df_found_clms.alias("f"),
    [
        col("c.SRC_SYS_CD_SK")==col("f.SRC_SYS_CD_SK"),
        col("c.CLM_ID")==col("f.CLM_ID")
    ],
    "inner"
).select(
    col("c.CLM_SK").alias("CLM_SK"),
    col("c.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("c.CLM_ID").alias("CLM_ID"),
    col("c.CLM_STTUS_CD_SK").alias("CLM_STTUS_CD_SK"),
    col("c.PD_DT_SK").alias("PD_DT_SK")
)

df_trans4 = (
    df_nasco_orig.alias("n")
    .join(
        df_ClmNascoLkup.alias("l"),
        [
            col("n.SRC_SYS_CD_SK")==col("l.SRC_SYS_CD_SK"),
            col("n.CLM_ID")==col("l.CLM_ID")
        ],
        "left"
    )
    .join(
        df_hf_Clm_LkupHsh.alias("r"),
        [
            col("n.SRC_SYS_CD_SK")==col("r.SRC_SYS_CD"),
            col("n.CLM_ID")==col("r.CLM_ID")
        ],
        "left"
    )
)

df_trans4 = (
    df_trans4
    .withColumn("svSttusCdSk", GetFkeyCodes("NPS", col("l.CLM_SK"), "CLAIM STATUS", "X", "N"))
    .withColumn("svSttusDtSk", GetFkeyDate("IDS", col("l.CLM_SK"), col("n.CLCL_LAST_ACT_DTM"), "X"))
    .withColumn("UpdateAdjFromFound", when(col("l.CLM_SK").isNull(), lit("N")).otherwise(lit("Y")))
)

df_ClmNascoUpdates = (
    df_trans4
    .filter(col("UpdateAdjFromFound")==lit("Y"))
    .select(
        col("l.CLM_SK").alias("CLM_SK"),
        lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        when(col("r.CLM_SK").isNull(), lit(0)).otherwise(col("r.CLM_SK")).alias("ADJ_TO_CLM_SK"),
        col("svSttusCdSk").alias("CLM_STTUS_CD_SK"),
        when(col("r.CLM_SK").isNull(), lit(" ")).otherwise(col("r.CLM_ID")).alias("ADJ_TO_CLM_ID")
    )
)

df_edwRevrslClmHitList = (
    df_trans4
    .select(
        col("l.CLM_SK").alias("CLM_SK"),
        col("l.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        col("l.CLM_ID").alias("CLM_ID"),
        lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
    )
)
df_edwRevrslClmHitList = df_edwRevrslClmHitList.withColumn("CLM_ID", rpad(col("CLM_ID"), 20, " "))

write_files(
    df_edwRevrslClmHitList.select("CLM_SK","SRC_SYS_CD_SK","CLM_ID","LAST_UPDT_RUN_CYC_EXCTN_SK"),
    f"{adls_path_raw}/landing/CLM.RVRSLHITLIST.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

write_files(
    df_ClmNascoUpdates.select("CLM_SK","LAST_UPDT_RUN_CYC_EXCTN_SK","ADJ_TO_CLM_SK","CLM_STTUS_CD_SK","ADJ_TO_CLM_ID"),
    f"{adls_path}/load/NascoClmRvversalUpdates.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

df_nasco_found_clms_2 = (
    spark.read.format("csv")
    .option("sep", ",")
    .option("quote", '"')
    .option("header", "false")
    .load(f"{adls_path}/load/NascoClmRvversalUpdates.{RunID}")
    .toDF("CLM_SK","LAST_UPDT_RUN_CYC_EXCTN_SK","ADJ_TO_CLM_SK","CLM_STTUS_CD_SK","ADJ_TO_CLM_ID")
)

execute_dml("DROP TABLE IF EXISTS STAGING.IdsClmFkey_2_NascoClmUpdate_temp", jdbc_url, jdbc_props)

df_nasco_found_clms_2.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.IdsClmFkey_2_NascoClmUpdate_temp") \
    .mode("overwrite") \
    .save()

merge_sql_1 = f"""
MERGE INTO {IDSOwner}.CLM as target
USING STAGING.IdsClmFkey_2_NascoClmUpdate_temp as source
ON target.CLM_SK = source.CLM_SK
WHEN MATCHED THEN
  UPDATE SET target.LAST_UPDT_RUN_CYC_EXCTN_SK = source.LAST_UPDT_RUN_CYC_EXCTN_SK,
             target.ADJ_TO_CLM_SK = source.ADJ_TO_CLM_SK,
             target.CLM_STTUS_CD_SK = source.CLM_STTUS_CD_SK,
             target.ADJ_TO_CLM_ID = source.ADJ_TO_CLM_ID
WHEN NOT MATCHED THEN
  INSERT (CLM_SK, LAST_UPDT_RUN_CYC_EXCTN_SK, ADJ_TO_CLM_SK, CLM_STTUS_CD_SK, ADJ_TO_CLM_ID)
  VALUES (source.CLM_SK, source.LAST_UPDT_RUN_CYC_EXCTN_SK, source.ADJ_TO_CLM_SK, source.CLM_STTUS_CD_SK, source.ADJ_TO_CLM_ID);
"""
execute_dml(merge_sql_1, jdbc_url, jdbc_props)