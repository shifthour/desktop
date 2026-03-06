model:
  config:
    origin: datastage
  jobs:
    - name: IdsClmFkey
      origin: datastage
      type: dataflow
      folder: "claim/claim"
      runAfterSubrOnJobSuccess: enabled
      description: "IdsClmFkey - IDS Claim Foreign Keying Job"
      fullDescription: "*****************************************************************************************************************************************************************************\r\nCOPYRIGHT 2004, 2005, 2006, 2007, 2008, 2009, 2021, 2023  BLUE CROSS/BLUE SHIELD OF KANSAS CITY \r\n\r\nJOB NAME:  IdsClmFkey\r\nCALLED BY:   IdsFctsClmLoad1Seq\r\n                       IdsNascoClmLoad1Seq\r\n                       IdsDrugLoadSeq   \r\n\r\nPROCESSING:\r\n               Massive quantities of Stage Variables with CDMA lookups for surrogate key assignement.\r\n\r\n\r\nMODIFICATIONS:\r\nDeveloper\x09Date\x09\x09Project/Altiris #\x09Change Description\x09\x09\x09\x09\x09\x09\x09Development Project\x09\x09Code Reviewer\x09\x09Date Reviewed       \r\n======================================================================================================================================================================================================   \r\nOliver Nielsen\x0905/00/2004                                    \x09\x09Originally Programmed\r\nOliver Nielsen\x0907/01/2004\x09\x09\x09Phase 1.1 Implementation - Added ALLOW_AMT to File format\r\nS.Andrew\x09\x0907/13/2004\x09\x09\x09Took out criteria Err Count = 0 on output\r\nBJ Luce\x09\x0908/2004\x09\x09\x09\x09Phase 2.0 - add transform SrcSysCdMbrFkey for foreign keying of class \r\n\x09\x09\x09\x09\x09\x09plan, class, group, member, subgroup and subscriber\r\n\x09\x09\x09\x09\x09\x09Add transform SrcSysCdProdFkey for foreign keying of product and\r\n\x09\x09\x09\x09\x09\x09experience category\r\nRalph Tucker\x0909/29/2006\x09\x09\x09Added LAST_UPDT_RUN_CYC_EXCTN_SK to the \r\n\x09\x09\x09\x09\x09\x09Nasco_Edw_Rvrsl_Clm_Hit_List with a default value of Zero\r\nBrent Leland\x0909/09/2004                                  \x09\x09Added InterProc stage for 4 streams\r\n                                                                                    \x09\x09\x09Added default rows for UNK and NA\r\nO. Nielsen\x09\x0909/24/2004                                  \x09\x09Hardcoded ClmAcdntStCd Lookup to 'IDS\" as the source.\r\nO. Nielsen\x09\x0910/01/2004                                   \x09\x09Change FKey Lookup for SVC_PROV_TYP to look at CLM_SUBTYPE to \r\n\x09\x09\x09\x09\x09\x09determine domain to look up in CDMA\r\nBrent Leland\x0910/08/2004                                   \x09\x09Changed the logging indicator to X for class, class plan, and subgroup foriegn\r\n\x09\x09\x09\x09\x09\x09key lookups.\r\nBrent Leland\x0912/27/2004                                    \x09\x09Changed ACDNT_IN field from char(1) to char(2) since it has changed from\r\n\x09\x09\x09\x09\x09\x09indicator to code.\r\n                                                                                     \x09\x09\x09Removed COB_PD_AMT, COB_OTHR_CAR_ALW_AMT, ITS_ACES_FEE_AMT, \r\n\x09\x09\x09\x09\x09\x09ITS_ADM_FEE_AMT, ITS_DRG_AMT, ITS_SCCF_NO columns.\r\nSteph Goddard\x0901/06/2005                                  \x09\x09changes for IDS 3.0 - moved fields around\r\nRalph Tucker\x0901/27/2005                                   \x09\x09Changed GetFkeyClsPln reducing the number of parameters to accomadate\r\n\x09\x09\x09\x09\x09\x09Version 3.0 changes.\r\nSAndrew\x09\x0902/16/2005                                    \x09\x09Removed warning text box '******  ATTENTION *****\r\n                                                                                     \x09\x09\x09Code was put in to handle Service Provider Type and Spec.   This should be \r\n\x09\x09\x09\x09\x09\x09changed when Version 3.0 is worked on!!!!    Look at Stage Variables \r\n\r\nDeveloper\x09Date\x09\x09Project/Altiris #\x09Change Description\x09\x09\x09\x09\x09\x09\x09Development Project\x09\x09Code Reviewer\x09\x09Date Reviewed       \r\n======================================================================================================================================================================================================   \r\nSteph Goddard\x0903/02/2005                                    \x09\x09Changed Fkey lookup for Fncl_Lob to use \"PSI\" for source system\r\nBJ Luce\x09\x0903/15/2005                                    \x09\x09create stage variable svAlphaPfxSrcSysCd to use FACETS for ARGUS and PCS in\r\n\x09\x09\x09\x09\x09\x09foreign key lookup\r\nBJ Luce\x09\x0903/22/2005                                    \x09\x09Add HOST_IN\r\nHall\x09\x0903/23/2005                                    \x09\x09Add lookup for ITS Host Codes for COB type\r\nRalph Tucker\x0905/18/2005                                    \x09\x09Took out field key indicator on Partitioner output (CLM_SK).\r\nBJ Luce\x09\x0905/21/2005                                    \x09\x09remove status code sk as part of the key for claim reversal updates and put in reference \r\n\x09\x09\x09\x09\x09\x09lookups to only pass claims that exist on CLM to the reversal update \r\nSteph Goddard\x0905/26/2005                                    \x09\x09Made BJ's change to the last transform\r\nRalph Tucker\x0909/07/2005                                    \x09\x09Chaged Alpha Prefix from CDMA to table lookup for FKey SK.\r\nSteph Goddard\x0903/11/2006                                   \x09\x09changes for sequencer\r\nBJ Luce\x09\x0903/13/2006                                   \x09\x09add hf_clm_nasco_dup to get the Nasco claim id for claims on this hash file\r\n                                                                                    \x09\x09\x09add hf_clm_nasco_orig to get the facets data needed to update the Nasco claim that \r\n\x09\x09\x09\x09\x09\x09has been adjusted in Facets - update direct to CLM \r\nRalph Tucker\x0903/22/2006                                   \x09\x09Added two hit lists of reversal claims to be appended to the Claim Mart and Edw \r\n\x09\x09\x09\x09\x09\x09claims hit lists.\r\nSteph Goddard\x0903/31/2006                                  \x09\x09changes for service provider type code - different logic for facets and nasco - \r\n\x09\x09\x09\x09\x09\x09corrected facets logic\r\nBJ Luce\x09\x0904/11/2006                                  \x09\x09change logic for updating the Nasco claim reversed.\r\nSteph Goddard\x0904/27/2006                                 \x09\x09remove cdma lookup stage variable for alpha prefix - not being used as it's doing \r\n\x09\x09\x09\x09\x09\x09fkey lookup now (see 9/7/2005 comment)\r\nBrent Leland\x0905/22/2006                                  \x09\x09Put missing hit list logic back in\r\nBrent Leland\x0907/19/2006                                  \x09\x09Removed Claim mart hit list creation.  Not required with run cycle processing.\r\nBrent Leland\x0908/07/2006                                  \x09\x09Added current run cycle to output records.  Without the current run cycle, error recycle \r\n\x09\x09\x09\x09\x09\x09records would have an old run cycle and would be removed in delete process.\r\n                                                                                   \x09\x09\x09Added RunID parameter for temp file name output.\r\nRalph Tucker\x0910/19/2006                                  \x09\x09Added new fields with default 'NA' values:  PCA_TYPE_CD\r\n                                                                                                                                REL_PCA_CLM_SK\r\n                                                                                                                                CLCL_MICRO_ID\r\n                                                                                                                                CLM_UPDT_SW\r\nRalph Tucker\x0912/08/2006                                    \x09\x09Added logic to update the old PCA claim with new REL_BASE_CLM_SK based up \r\n\x09\x09\x09\x09\x09\x09CLCL_MICRO_ID prefixed with a PCA in cols 1-3. \r\n\r\nDeveloper\x09Date\x09\x09Project/Altiris #\x09Change Description\x09\x09\x09\x09\x09\x09\x09Development Project\x09\x09Code Reviewer\x09\x09Date Reviewed       \r\n======================================================================================================================================================================================================\r\nBrent Leland\x0906/26/2007\x09\x09\x09Took out parallel processing to simplify the job.\r\n\x09\x09\x09\x09\x09\x09Added error recycle file that also require additional job parameter.\r\nRalph Tucker\x0906/27/2007\x09\x09\x09Added logic for ARGUS to handle PCA claims.\x09\x09\x09\x09\x09\x09\x09Steph Goddard\x09\x0907/18/07\r\nBrent Leland\x0908/15/2007\x09IAD Prod. Supp.\x09Added error recycle hash file ( hf_claim_recycle_keys )\x09\x09\x09devlIDS30\x09\x09\x09Steph Goddard\x09\x0908/30/07                       \r\nSteph Goddard\x0910/23/2007\x09IAD Prod. Supp.\x09re-added logic for prov spec and prov type - it went missing!\x09\x09\x09devlIDS30\x09\x09\x09Ralph Tucker\x09\x0910/18/2007\r\nLaurel Kindley\x0912/17/2007\x09TTR-90\x09\x09Modified source system code being passed in for lookup of sk on experience\r\n\x09\x09\x09\x09\x09\x09category code and financial line of business on Nasco claims.\x09\x09\x09devlIDS30\x09\x09\x09Steph Goddard\x09\x0912/18/2007\r\nSAndrew\x09\x092008-10-01\x09#3784 PBM Vnfr\x09Changed called routines SrcSysCdMbrFkey and SrcSysCdProdFkey\r\n\x09\x09\x09\x09\x09\x09Changed rule for stage var svProvDomain by added check for ESI\x09\x09devlIDSnew\x09\x09Steph Goddard\x09\x0910/27/2008\r\nSAndrew\x09\x092008-10-01       \x09#3784 PBM Vnfr\x09Changed called routines SrcSysCdMbrFkey and SrcSysCdProdFkey\r\n\x09\x09\x09\x09\x09\x09Changed rule for stage var svProvDomain by added check for WellDyne\x09\x09devlIDS\x09\x09\x09Steph Goddard\x09\x0903/30/2009\r\nBhoomi D\x09\x092009-06-17\x09#4202\x09\x09Changed logic for REL_PCA_CLM_SK & REL_BASE_CLM_SK\x09\x09\x09devlIDSnew\x09\x09Steph Goddard\x09\x0906/23/2009\r\nBhoomi D\x09\x092009-07-02\x094202\x09\x09Added PCA_Claims infront of Fkey for PCA claims missing on hf_clm\x09\x09devlIDSnew\x09\x09Steph Goddard\x09\x0907/06/2009\r\nRalph Tucker\x092009-08-13\x0915 - Prod Suprt\x09Added REMIT_SUPRSION_AMT\x09\x09\x09\x09\x09devlIDS\x09\x09\x09Steph Goddard\x09\x0908/21/2009\r\nKalyan Neelam\x092009-12-31\x09 4110\x09\x09Added ClmCrfIn1.SRC_SYS_CD = \"MCAID\" in the stage variable\x09\x09\x09IntegrateCurDevl\x09\x09Steph Goddard\x09\x0901/11/2010\r\n                                                                                  \x09\x09\x09SvcProvDomain\r\nKalyan Neelam\x092010-02-09\x094278\x09\x09Added two new fields - MCAID_STTUS_ID,\x09\x09\x09\x09\x09IntegrateCurDevl\x09\x09Steph Goddard\x09\x0903/08/2010                  \r\n\x09\x09\x09\x09\x09\x09PATN_PD_AMT                                                                                            \r\nSteph Goddard\x092010-11-30\x09ProdSupp\x09\x09added hash files between transforms and link collector\x09\x09\x09IntegrateNewDevl\x09\x09Brent Leland\x09\x0912-08-2010\r\nKalyan Neelam\x092010-12-22\x094616\x09\x09Added ClmCrfIn1.SRC_SYS_CD = \"MEDTRAK\" in the stage variable\x09\x09IntegrateNewDevl\x09\x09Steph Goddard\x09\x0912/23/2010\r\n\x09\x09\x09\x09\x09\x09SvcProvDomain\r\nManasa Andru\x092012-05-04\x094896 ICD10\x09Added new field CLM_SUBMT_ICD_VRSN_CD_SK\x09\x09\x09\x09IntegrateNewDevl      \r\n\x09\x09\x09\x09\x09\x09Remediation and defaulted to NA     \r\nKalyan Neelam \x092012-10-17\x094784\x09\x09Update rule for Network status code in stagte variable ClmInNtwkSk\x09\x09IntegrateWrhsDevl\x09\x09Bhoomi Dasari\x09\x0910/20/2012\r\nKalyan Neelam \x092013-11-18\x095056 FEP Claims\x09Added ClmCrfIn1.SRC_SYS_CD = \"BCA\" in the stage variable\x09\x09\x09IntegrateNewDevl\x09\x09Bhoomi Dasari\x09\x0911/30/2013 \r\n\x09\x09\x09\x09\x09\x09SvcProvDomain\r\nKalyan Neelam\x092014-12-17\x095212\x09\x09Added If SRC_SYS_CD = 'BCBSA' Then 'BCA' Else SRC_SYS_CD\x09\x09IntegrateCurDevl\x09\x09Bhoomi Dasari\x09\x0902/04/2015\r\n\x09\x09\x09\x09\x09\x09in the stage variables and pass it to GetFkeyCodes because code sets are created \r\n\x09\x09\x09\x09\x09\x09under BCA for BCBSA\r\n\r\nDeveloper\x09Date\x09\x09Project/Altiris #\x09Change Description\x09\x09\x09\x09\x09\x09\x09Development Project\x09\x09Code Reviewer\x09\x09Date Reviewed       \r\n======================================================================================================================================================================================================\r\nSudhir Bomshetty\x092017-10-23\x095781\x09\x09Added check for SRC_SYS_CD = 'BCA' for column\x09\x09\x09\x09IntegrateDev2\x09\x09Kalyan Neelam\x09\x092017-10-23\r\n\x09\x09\x09\x09\x09\x09CLM_SUBMTTING_BCBS_PLN_CD_SK\r\nManasa Andru\x092017-10-30\x09TFS - 19834\x09Added condition in the Stage variables for fields -\x09\x09\x09\x09\x09IntegrateDev2\x09\x09Kalyan Neelam\x09\x092017-10-30\r\n\x09\x09\x09\x09\x09\x09CLM_ACTV_BCBS_PLN_CD_SK, CLM_SUB_BCBS_PLN_CD_SK to use\r\n\x09\x09\x09\x09\x09\x09the source FACETS if it is BCA or BCBSA while populating the SK values.\r\nSudhir Bomshetty\x092018-01-12\x095781\x09\x09Added SRC_SYS_CD = 'BCA' check for SVC_END_DT_SK\x09\x09\x09\x09IntegrateDev2\x09\x09Kalyan Neelam\x09\x092018-01-16\r\nNagesh Bandi\x092018-01-10\x09TFS - 16786\x09Changed FK lookup for  CLM_PAYE_CD_SK\x09\x09\x09\x09\x09IntegrateDev3\x09\x09Kalyan Neelam\x09\x092018-01-17\r\n\x09\x09\x09\x09\x09\x09from GetFkeyCodes(svCdMpngSrcSysCd, ClmCrfIn1.CLM_SK, \"CLAIM PAYEE\", \r\n\x09\x09\x09\x09\x09\x09ClmCrfIn1.CLM_PAYE_CD, Logging) to \r\n\x09\x09\x09\x09\x09\x09GetFkeySrcTrgtDomainCodes(svCdMpngSrcSysCd, ClmCrfIn1.CLM_SK, \"CLAIM PAYEE\", \r\n\x09\x09\x09\x09\x09\x09ClmCrfIn1.CLM_PAYE_CD, \"CLAIM PAYEE\", Logging)\r\nManasa Andru\x092018-03-05\x09TFS-21153\x09Changed staqe variable logic to check for UNK and NA before getting Fkey Value\x09IntegrateDev2\x09\x09Kalyan Neelam\x09\x092018-03-12\r\n\x09\x09\x09\x09\x09\x09svProvTypSk, svProvSpecCdSk and NtwkSk\r\nKaushik Kapoor\x092018-03-16\x095828\x09\x09Added SrcSysCd = SAVRX, BCBSA and LDI in SvcProvDomain stage variable\x09\x09IntegrateDev2\x09\x09Jaideep Mankala\x09\x0903/21/2018\r\nKaushik Kapoor\x092018-09-20\x095828\x09\x09Added SrcSysCd = CVS in SvcProvDomain stage variable\x09\x09\x09\x09IntegrateDev2\x09\x09Kalyan Neelam\x09\x092018-10-01\r\nMohan Karnati\x0906/17/2019\x09ADO-73034\x09Adding CLM_TXNMY_CD filed in ClmCrf, hf_recycle,ClmLoadFile stageS and\x09\x09IntegrateDev1\x09\x09Kalyan Neelam\x09\x092019-07-01 \r\n\x09\x09\x09\x09\x09\x09passing till Output stage                                                                                    \r\nKarthik Chintalapani\x092019-08-27\x095884\x09\x09Added new logic for adj_from_clm_sk field\x09\x09\x09\x09\x09IntegrateDev1\x09\x09Kalyan Neelam\x09\x092019-09-05      \r\nSRINIVAS GARIMELLA    2019-10-16\x096131\x09\x09Modified feilds as per the\x09\x09\x09\x09\x09\x09\x09Integrate Dev1\r\n\x09\x09\x09\x09\x09\x09OptumRX transformation rules: \r\n\x09\x09\x09\x09\x09\x09ADJ_FROM_CLM_ID\r\nSagar Sayam\x092019-10-21\x096131 PBM Replace\x09Added OptumRx value under'SvcProvDomain'under the stage variables to include\x09Integrate Dev5\x09\x09Kalyan Neelam\x09\x092019-11-20\r\n\x09\x09\x09\x09\x09\x09OPTUMRX in 'Trns1' Transformer Phase.  t                                                                                                                                \r\nKarthik Chintalapani\x092020-03-16\x095884\x09\x09Modified the adj_from_clm_sk logic for BCA Medical claims in the Trns1 stage\x09\x09IntegrateDev1\x09\x09Kalyan Neelam\x09\x092020-03-18\r\nRekha\x09\x092020-04-13\x096131  - PBM Replace\x09Mapped new column BILL_PAYMT_EXCL_IN\x09\x09\x09\x09IntegrateDev2      \r\nSunitha Ganta\x0910-12-2020\x09\x09\x09Brought up to standards       \r\nReddy Sanam\x0910-13-2020\x09\x09\x09changed derivation to this Stage variable \r\n\x09\x09\x09\x09\x09\x09svCdMpngSrcSysCd to pass FACETS for\r\n\x09\x09\x09\x09\x09\x09LUMERIS for type codes in the Trns1 stage               \r\nGoutham Kalidindi\x0911/2/2020\x09\x09US-283560\x09Added MEDIMPACT to svPROVDomain\x09\x09\x09\x09\x09IntegrateDev2\x09\x09Reddy Sanam\x09\x092020-11-04\r\n\x09\x09\x09\x09\x09\x09In Xfrm 'Trns1'\r\n\r\nDeveloper\x09Date\x09\x09Project/Altiris #\x09Change Description\x09\x09\x09\x09\x09\x09\x09\x09Development Project\x09Code Reviewer\x09\x09Date Reviewed       \r\n======================================================================================================================================================================================================\r\nVikas Abbu\x0902/04/2021\x09RA-DentaQuest\x09Added Conditions in CLM_SUBTYP_CD_SK,CLM_CAP_CD_SK,\x09\x09\x09IntegrateDev2\x09\x09Kalyan Neelam\x09\x092021-02-04\r\n\x09\x09\x09\x09\x09\x09CLM_TYPE_CD_SK  Column Stage Variables\r\nVikas Abbu\x0902/09/2021\x09RA-Solutran\x09Added SRC_SYS_CD ='SOLUTRAN' in stage variable\x09\x09\x09\x09IntegrateDev2\x09\x09Reddy Sanam\x09\x092021-02-10\r\nVikas Abbu\x0903/02/2021\x09RA-DentaQuest\x09Added  ClmCrfIn1.SRC_SYS_CD = 'DENTAQUEST in stagevariable\x09\x09\x09IntegrateDev2\x09\x09Abhiram Dasarathy\x09\x092021-03-03   \r\nMrudula Kodali\x0903/07/2021\x09RA-EMR Procedure\x09Added   stage variable svEMRSrcSysCd and used in other stage variables\x09\x09IntegrateDev2\x09\x09Reddy Sanam\x09\x092021-03-08\r\nMrudula Kodali\x0903/11/2021\x09RA-Livongo\x09Added Conditions in CapClmCdSk,ClmCatCdSk,ClmSubTypCdSk\x09\x09\x09IntegrateDev2\x09\x09Jaideep Mankala\x09\x0903/18/2021      \r\n                                                                                      \x09,ClmTypCdSk,ClmSttusCdSk, SvcProvDomain    Column Stage Variables    \r\nMrudula Kodali\x0903/24/2021\x09RA-Livongo\x09Updated the stage variable ClmFinlDispCdk and removed LVNGHLTH in this variable.\x09 Integrate Dev2\x09\x09Hugh Sisson\x09\x092021-03-25\r\nMrudula Kodali\x0905/03/2021\x09US-373652\x09Updated the stage variables ClmCapCdSk, ClmPayeCdSk, ClmSubTypCdSk, ClmTypCdSk.\x09\x09\x09\x09Jaideep Mankala\x09\x0905/10/2021\r\n\x09\x09\x09\x09\x09\x09Removed usages of FACETS for EYEMED, DENTAQUEST, SOLUTRON, LVNGHLTH, EMR\r\nGoutham K\x0909/08/2021\x09US-428592\x09Lho and Facets Adjusted TO and FROM SK fix, Added a new lookup to the\x09\x09IntegrateDev2\x09\x09Reddy Sanam\x09\x0909/08/2021              \r\n\x09\x09\x09\x09\x09\x09K_CLM table to get the SK when a Lho claim gets ajusted TO a Facets claim or \r\n\x09\x09\x09\x09\x09\x09when a Facets claim gets adjusted from Lho Claim\r\nKen Bradmon\x0909/23/2021\x09us426738\x09\x09In the stage \"Trns1\" I added a new row \"MOSAICLIFECARE\" for the\x09\x09\x09IntegrateDev1\x09\x09Harsha Ravuri\x09\x092021-09-28\x09\r\n\x09\x09\x09\x09\x09\x09stage variable \"svEMRSrcSysCd.\"\r\n\x09\x09\x09\x09\x09\x09That is the only change.\r\nVenkata Y             10/27/2021            us438375                 In the stage \"Trns1\" I added a new row \"HCA\" for the\x09\x09              \x09\x09IntegrateDev2\x09\x09Jeyaprasanna                         2021-10-27\x09\r\n\x09\x09\x09\x09\x09\x09stage variable \"svEMRSrcSysCd.\"\r\n\x09\x09\x09\x09\x09\x09That is the only change.\r\n\r\nVenkata Y            11/09/2021        US413785                    In the stage \"Trns1\" I added a new row \"NONSTDSUPLMTDATA\",               \r\n                                                                                            \"CHILDRENSMERCY\"  for the\x09stage variable \"svEMRSrcSysCd.\"\r\n\x09\x09\x09\x09\x09             That is the only change.\x09\x09\x09\x09\x09\x09\x09IntegrateDev2\x09\x09Harsha Ravuri\x09\x092021-11-11\x09\x09\r\n\r\nManisha Gandra    2021-11-29               US 459610            Added Nations Source System Code in \"Trns1\" stage variables\x09\x09\x09IntegrateDev2                    \x09Jeyaprasanna                          2022-01-05  \r\n\r\nKen Bradmon\x092023-08-07\x09us559895\x09\x09Added 8 new Providers to the svEMRSrcSysCd variable of the Trns1 stage.\x09\x09IntegrateDev2\x09               Reddy Sanam                          2023-10-27\x09\r\n\x09\x09\x09\x09\x09\x09CENTRUSHEALTH, MOHEALTH, GOLDENVALLEY, JEFFERSON, \r\n\x09\x09\x09\x09\x09\x09WESTMOMEDCNTR, BLUESPRINGS, EXCELSIOR, and HARRISONVILLE.\r\n\x09\x09\x09\x09\x09\x09This is the only change.\r\n\r\nSham Shankaranarayana 2023-12-21    US 599810           Added new Stage variable svSBVSrcSysCd and updated StageVar existing \r\n                                                                                            SrcSysCdMbr, SrcSysCdProd in Transformer Trns1                                                                     IntegrateDev1                        Jeyaprasanna                         2023-12-21\r\n\r\nKshema H K         30-04-2024           US 616929                Added  filter condition in stage variable SvcProvDomain for NATIONS source system code       IntegrateDev1                        Jeyaprasanna                        2024-05-01\r\n                                                                                                      in \"Trns 1\" Transformer stage\r\nHarsha Ravuri\x0905-06-2025\x09US#649651\x09Added new provider 'ASCENTIST' to the svEMRSrcSysCd variable of the Trns1 stage.\x09IntegrateDev2                        Jeyaprasanna                        2025-05-20"
      parameters:
        - name: FilePath
          type: string
          description: "IDS File Path"
          defaultType: project
        - name: Source
          type: string
          description: "source system"
        - name: SrcSysCdSk
          type: integer
          description: "Source System SK"
        - name: InFile
          type: string
          description: InFile
        - name: Logging
          type: string
          description: Logging
        - name: RunCycle
          type: integer
          description: "Current run cycle"
        - name: RunID
          type: string
          description: "Run ID"
        - name: IDSInstance
          type: string
          description: "IDS Instance"
          defaultType: project
        - name: IDSDB
          type: string
          description: "IDS Database"
          defaultType: project
        - name: IDSOwner
          type: string
          description: "IDS Table Owner"
          defaultType: project
        - name: IDSAcct
          type: string
          description: "IDS Account"
        - name: IDSPW
          type: encrypted
          description: "IDS Password"
      afterSubr:
        subrType: "DSU.HASH.CLEAR\\\\2;hf_idsclmfkey_col1;hf_idsclmfkey_col2"
        subr: "2;hf_idsclmfkey_col1;hf_idsclmfkey_col2"
      steps:
        - name: ClmCrf
          type: fileReader
          urls: 
            - "#$FilePath#/key/#InFile#"
          rejectMode: continue
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: ","
          schema: 
            - name: JOB_EXCTN_RCRD_ERR_SK
              type: int
              width: 4
              nullable: N
            - name: INSRT_UPDT_CD
              type: char
              width: 10
              nullable: N
            - name: DISCARD_IN
              type: char
              width: 1
              nullable: N
            - name: PASS_THRU_IN
              type: char
              width: 1
              nullable: N
            - name: FIRST_RECYC_DT
              type: timestamp
              nullable: N
            - name: ERR_CT
              type: smallint
              width: 2
              nullable: N
            - name: RECYCLE_CT
              type: decimal
              width: 6
              nullable: N
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
            - name: PRI_KEY_STRING
              type: varchar
              width: 1023
              nullable: N
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_ID
              type: char
              width: 18
              nullable: N
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: ADJ_FROM_CLM
              type: char
              width: 12
              nullable: N
            - name: ADJ_TO_CLM
              type: char
              width: 12
              nullable: N
            - name: ALPHA_PFX_CD
              type: char
              width: 3
            - name: CLM_EOB_EXCD
              type: char
              width: 3
              nullable: N
            - name: CLS
              type: char
              width: 4
              nullable: N
            - name: CLS_PLN
              type: char
              width: 8
              nullable: N
            - name: EXPRNC_CAT
              type: char
              width: 4
              nullable: N
            - name: FNCL_LOB_NO
              type: char
              width: 4
            - name: GRP
              type: char
              width: 8
              nullable: N
            - name: MBR_CK
              type: int
              width: 4
              nullable: N
            - name: NTWK
              type: char
              width: 12
              nullable: N
            - name: PROD
              type: char
              width: 8
              nullable: N
            - name: SUBGRP
              type: char
              width: 4
              nullable: N
            - name: SUB_CK
              type: int
              width: 4
              nullable: N
            - name: CLM_ACDNT_CD
              type: char
              width: 10
              nullable: N
            - name: CLM_ACDNT_ST_CD
              type: char
              width: 2
              nullable: N
            - name: CLM_ACTIVATING_BCBS_PLN_CD
              type: char
              width: 10
              nullable: N
            - name: CLM_AGMNT_SRC_CD
              type: char
              width: 10
              nullable: N
            - name: CLM_BTCH_ACTN_CD
              type: char
              width: 1
              nullable: N
            - name: CLM_CAP_CD
              type: char
              width: 10
              nullable: N
            - name: CLM_CAT_CD
              type: char
              width: 10
              nullable: N
            - name: CLM_CHK_CYC_OVRD_CD
              type: char
              width: 1
              nullable: N
            - name: CLM_COB_CD
              type: char
              width: 1
              nullable: N
            - name: FINL_DISP_CD
              type: varchar
              width: 20
              nullable: N
            - name: CLM_INPT_METH_CD
              type: char
              width: 1
              nullable: N
            - name: CLM_INPT_SRC_CD
              type: char
              width: 10
              nullable: N
            - name: CLM_IPP_CD
              type: char
              width: 10
              nullable: N
            - name: CLM_NTWK_STTUS_CD
              type: char
              width: 2
              nullable: N
            - name: CLM_NONPAR_PROV_PFX_CD
              type: char
              width: 4
              nullable: N
            - name: CLM_OTHER_BNF_CD
              type: char
              width: 1
              nullable: N
            - name: CLM_PAYE_CD
              type: char
              width: 1
              nullable: N
            - name: CLM_PRCS_CTL_AGNT_PFX_CD
              type: char
              width: 4
              nullable: N
            - name: CLM_SVC_DEFN_PFX_CD
              type: char
              width: 4
              nullable: N
            - name: CLM_SVC_PROV_SPEC_CD
              type: char
              width: 10
              nullable: N
            - name: CLM_SVC_PROV_TYP_CD
              type: char
              width: 10
              nullable: N
            - name: CLM_STTUS_CD
              type: char
              width: 2
              nullable: N
            - name: CLM_SUBMT_BCBS_PLN_CD
              type: char
              width: 10
              nullable: N
            - name: CLM_SUB_BCBS_PLN_CD
              type: char
              width: 10
              nullable: N
            - name: CLM_SUBTYP_CD
              type: char
              width: 10
              nullable: N
            - name: CLM_TYP_CD
              type: char
              width: 1
              nullable: N
            - name: ATCHMT_IN
              type: char
              width: 1
              nullable: N
            - name: CLM_CLNCL_EDIT_CD
              type: char
              width: 1
              nullable: N
            - name: COBRA_CLM_IN
              type: char
              width: 1
              nullable: N
            - name: FIRST_PASS_IN
              type: char
              width: 1
              nullable: N
            - name: HOST_IN
              type: char
              width: 1
              nullable: N
            - name: LTR_IN
              type: char
              width: 1
              nullable: N
            - name: MCARE_ASG_IN
              type: char
              width: 1
              nullable: N
            - name: NOTE_IN
              type: char
              width: 1
              nullable: N
            - name: PCA_AUDIT_IN
              type: char
              width: 1
              nullable: N
            - name: PCP_SUBMT_IN
              type: char
              width: 1
              nullable: N
            - name: PROD_OOA_IN
              type: char
              width: 1
              nullable: N
            - name: ACDNT_DT
              type: char
              width: 10
              nullable: N
            - name: INPT_DT
              type: char
              width: 10
              nullable: N
            - name: MBR_PLN_ELIG_DT
              type: char
              width: 10
              nullable: N
            - name: NEXT_RVW_DT
              type: char
              width: 10
              nullable: N
            - name: PD_DT
              type: char
              width: 10
              nullable: N
            - name: PAYMT_DRAG_CYC_DT
              type: char
              width: 10
              nullable: N
            - name: PRCS_DT
              type: char
              width: 10
              nullable: N
            - name: RCVD_DT
              type: char
              width: 10
              nullable: N
            - name: SVC_STRT_DT
              type: char
              width: 10
              nullable: N
            - name: SVC_END_DT
              type: char
              width: 10
              nullable: N
            - name: SMLR_ILNS_DT
              type: char
              width: 10
              nullable: N
            - name: STTUS_DT
              type: char
              width: 10
              nullable: N
            - name: WORK_UNABLE_BEG_DT
              type: char
              width: 10
              nullable: N
            - name: WORK_UNABLE_END_DT
              type: char
              width: 10
              nullable: N
            - name: ACDNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: ACTL_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: ALLOW_AMT
              type: decimal
              width: 19
              precision: 4
              nullable: N
            - name: DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: CLM_CT
              type: smallint
              width: 2
              nullable: N
            - name: MBR_AGE
              type: decimal
              width: 3
              nullable: N
            - name: ADJ_FROM_CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: ADJ_TO_CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: DOC_TX_ID
              type: char
              width: 18
              nullable: N
            - name: MCAID_RESUB_NO
              type: char
              width: 15
            - name: MCARE_ID
              type: char
              width: 12
              nullable: N
            - name: MBR_SFX_NO
              type: char
              width: 2
            - name: PATN_ACCT_NO
              type: varchar
              width: 20
            - name: PAYMT_REF_ID
              type: char
              width: 16
              nullable: N
            - name: PROV_AGMNT_ID
              type: char
              width: 12
              nullable: N
            - name: RFRNG_PROV_TX
              type: varchar
              width: 70
            - name: SUB_ID
              type: char
              width: 14
              nullable: N
            - name: PRPR_ENTITY
              type: char
              width: 1
            - name: PCA_TYP_CD
              type: char
              width: 18
              nullable: N
            - name: REL_PCA_CLM_ID
              type: varchar
              width: 18
              nullable: N
            - name: CLCL_MICRO_ID
              type: char
              width: 18
              nullable: N
            - name: CLM_UPDT_SW
              type: char
              width: 1
              nullable: N
            - name: REMIT_SUPRSION_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: MCAID_STTUS_ID
              type: varchar
              width: 20
              nullable: N
            - name: PATN_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: CLM_SUBMT_ICD_VRSN_CD
              type: varchar
              width: 20
              nullable: N
            - name: CLM_TXNMY_CD
              type: varchar
              width: 20
            - name: BILL_PAYMT_EXCL_IN
              type: char
              width: 1
              nullable: N
        - name: ClmPcaUpdts.PcaClm2Updt
          type: fileReader
          urls: 
            - "#$FilePath#/load/CLM_PCA_UPDT.dat"
          rejectMode: continue
          executeAfter:
            - ClmPcaUpdts.ClmPcaUpdt
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: ","
          schema: 
            - name: REL_BASE_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
          keys:
            - REL_BASE_CLM_SK
        - name: found_clms.updtCLAIM
          type: fileReader
          urls: 
            - "#$FilePath#/load/ClmRvversalUpdates.#RunID#"
          rejectMode: continue
          executeAfter:
            - found_clms.ClmRvversalUpdates
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: ","
          schema: 
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: CLM_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: PD_DT_SK
              type: char
              width: 10
              nullable: N
          keys:
            - SRC_SYS_CD_SK
            - CLM_SK
            - CLM_ID
            - CLM_STTUS_CD_SK
        - name: nasco_found_clms.updtCLAIM
          type: fileReader
          urls: 
            - "#$FilePath#/load/NascoClmRvversalUpdates.#RunID#"
          rejectMode: continue
          executeAfter:
            - nasco_found_clms.ClmNascoUpdates
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: ","
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: ADJ_TO_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: ADJ_TO_CLM_ID
              type: varchar
              width: 20
              nullable: N
          keys:
            - CLM_SK
        - name: ClmPcaUpdts.ClmPcaUpdt
          type: fileWriter
          urls: 
            - "#$FilePath#/load/CLM_PCA_UPDT.dat"
          executeBefore:
            - ClmPcaUpdts.PcaClm2Updt
          source: "Trans3.V0S141P3"
          option: 
            updateMode: overwrite
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: ","
        - name: found_clms.ClmRvversalUpdates
          type: fileWriter
          urls: 
            - "#$FilePath#/load/ClmRvversalUpdates.#RunID#"
          executeBefore:
            - found_clms.updtCLAIM
          source: "Trans2.V25S1P3"
          option: 
            updateMode: overwrite
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: ","
        - name: nasco_found_clms.ClmNascoUpdates
          type: fileWriter
          urls: 
            - "#$FilePath#/load/NascoClmRvversalUpdates.#RunID#"
          executeBefore:
            - nasco_found_clms.updtCLAIM
          source: "Trans4.V26S0P3"
          option: 
            updateMode: overwrite
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: ","
        - name: Nasco_Edw_Rvrsl_Clm_Hit_List
          type: fileWriter
          urls: 
            - "#$FilePath#/landing/CLM.RVRSLHITLIST.dat"
          source: "Trans4.V26S0P5"
          option: 
            updateMode: append
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: ","
        - name: Edw_Rvrsl_Clm_Hit_List
          type: fileWriter
          urls: 
            - "#$FilePath#/landing/CLM.RVRSLHITLIST.dat"
          source: "Trans2.V25S1P10"
          option: 
            updateMode: overwrite
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: ","
        - name: ClmLoadFile
          type: fileWriter
          urls: 
            - "#$FilePath#/load/CLM.#Source#.dat"
          source: Link_Collector_237
          option: 
            updateMode: overwrite
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: ","
        - name: nasco_dup
          type: dbReader
          originalDb: hashedFile
          table: "/hf_clm_nasco_dup"
          schema: 
            - name: CLM_ID
              type: char
              width: 12
              nullable: N
            - name: NASCO_CLM_ID
              type: char
              width: 70
              nullable: N
          keys:
            - CLM_ID
            # Hashed File has no directory specified
        - name: nasco_orig
          type: dbReader
          originalDb: hashedFile
          table: "/hf_clm_nasco_orig"
          schema: 
            - name: NASCO_CLM_ID
              type: char
              width: 70
              nullable: N
            - name: CLM_ID
              type: char
              width: 12
              nullable: N
            - name: CLCL_LAST_ACT_DTM
              type: char
              width: 10
              nullable: N
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
          keys:
            - NASCO_CLM_ID
            # Hashed File has no directory specified
        - name: hf_Clm_LkupHsh
          type: dbReader
          originalDb: hashedFile
          table: "/hf_clm"
          schema: 
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
            - name: CLM_ID
              type: char
              width: 18
              nullable: N
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
          keys:
            - SRC_SYS_CD
            - CLM_ID
            # Hashed File has no directory specified
        - name: hf_recycle
          type: dbWriter
          originalDb: hashedFile
          table: "/hf_recycle"
          tableAction: append
          source: "Trns1.V0S20P9"
            # Hashed File has no directory specified
        - name: hf_claim_recycle_keys
          type: dbWriter
          originalDb: hashedFile
          table: "/hf_claim_recycle_keys"
          tableAction: append
          source: "Trns1.V0S20P22"
            # Hashed File has no directory specified
        - name: hf_idsclmfkey_col2
          type: map
          source: Trans7
          alias: Link3
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: "Link3.CLM_SK"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link3.SRC_SYS_CD_SK"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "Link3.CLM_ID"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "Link3.CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "Link3.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: ADJ_FROM_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "Link3.ADJ_FROM_CLM_SK"
            - name: ADJ_TO_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "Link3.ADJ_TO_CLM_SK"
            - name: ALPHA_PFX_SK
              type: int
              width: 4
              nullable: N
              map: "Link3.ALPHA_PFX_SK"
            - name: CLM_EOB_EXCD_SK
              type: int
              width: 4
              nullable: N
              map: "Link3.CLM_EOB_EXCD_SK"
            - name: CLS_SK
              type: int
              width: 4
              nullable: N
              map: "Link3.CLS_SK"
            - name: CLS_PLN_SK
              type: int
              width: 4
              nullable: N
              map: "Link3.CLS_PLN_SK"
            - name: EXPRNC_CAT_SK
              type: int
              width: 4
              nullable: N
              map: "Link3.EXPRNC_CAT_SK"
            - name: FNCL_LOB_SK
              type: int
              width: 4
              nullable: N
              map: "Link3.FNCL_LOB_SK"
            - name: GRP_SK
              type: int
              width: 4
              nullable: N
              map: "Link3.GRP_SK"
            - name: MBR_SK
              type: int
              width: 4
              nullable: N
              map: "Link3.MBR_SK"
            - name: NTWK_SK
              type: int
              width: 4
              nullable: N
              map: "Link3.NTWK_SK"
            - name: PROD_SK
              type: int
              width: 4
              nullable: N
              map: "Link3.PROD_SK"
            - name: SUBGRP_SK
              type: int
              width: 4
              nullable: N
              map: "Link3.SUBGRP_SK"
            - name: SUB_SK
              type: int
              width: 4
              nullable: N
              map: "Link3.SUB_SK"
            - name: CLM_ACDNT_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link3.CLM_ACDNT_CD_SK"
            - name: CLM_ACDNT_ST_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link3.CLM_ACDNT_ST_CD_SK"
            - name: CLM_ACTV_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link3.CLM_ACTV_BCBS_PLN_CD_SK"
            - name: CLM_AGMNT_SRC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link3.CLM_AGMNT_SRC_CD_SK"
            - name: CLM_BTCH_ACTN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link3.CLM_BTCH_ACTN_CD_SK"
            - name: CLM_CAP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link3.CLM_CAP_CD_SK"
            - name: CLM_CAT_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link3.CLM_CAT_CD_SK"
            - name: CLM_CHK_CYC_OVRD_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link3.CLM_CHK_CYC_OVRD_CD_SK"
            - name: CLM_COB_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link3.CLM_COB_CD_SK"
            - name: CLM_FINL_DISP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link3.CLM_FINL_DISP_CD_SK"
            - name: CLM_INPT_METH_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link3.CLM_INPT_METH_CD_SK"
            - name: CLM_INPT_SRC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link3.CLM_INPT_SRC_CD_SK"
            - name: CLM_INTER_PLN_PGM_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link3.CLM_INTER_PLN_PGM_CD_SK"
            - name: CLM_NTWK_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link3.CLM_NTWK_STTUS_CD_SK"
            - name: CLM_NONPAR_PROV_PFX_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link3.CLM_NONPAR_PROV_PFX_CD_SK"
            - name: CLM_OTHR_BNF_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link3.CLM_OTHR_BNF_CD_SK"
            - name: CLM_PAYE_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link3.CLM_PAYE_CD_SK"
            - name: CLM_PRCS_CTL_AGNT_PFX_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link3.CLM_PRCS_CTL_AGNT_PFX_CD_SK"
            - name: CLM_SVC_DEFN_PFX_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link3.CLM_SVC_DEFN_PFX_CD_SK"
            - name: CLM_SVC_PROV_SPEC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link3.CLM_SVC_PROV_SPEC_CD_SK"
            - name: CLM_SVC_PROV_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link3.CLM_SVC_PROV_TYP_CD_SK"
            - name: CLM_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link3.CLM_STTUS_CD_SK"
            - name: CLM_SUBMTTING_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link3.CLM_SUBMTTING_BCBS_PLN_CD_SK"
            - name: CLM_SUB_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link3.CLM_SUB_BCBS_PLN_CD_SK"
            - name: CLM_SUBTYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link3.CLM_SUBTYP_CD_SK"
            - name: CLM_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link3.CLM_TYP_CD_SK"
            - name: ATCHMT_IN
              type: char
              width: 1
              nullable: N
              map: "Link3.ATCHMT_IN"
            - name: CLNCL_EDIT_IN
              type: char
              width: 1
              nullable: N
              map: "Link3.CLNCL_EDIT_IN"
            - name: COBRA_CLM_IN
              type: char
              width: 1
              nullable: N
              map: "Link3.COBRA_CLM_IN"
            - name: FIRST_PASS_IN
              type: char
              width: 1
              nullable: N
              map: "Link3.FIRST_PASS_IN"
            - name: HOST_IN
              type: char
              width: 1
              nullable: N
              map: "Link3.HOST_IN"
            - name: LTR_IN
              type: char
              width: 1
              nullable: N
              map: "Link3.LTR_IN"
            - name: MCARE_ASG_IN
              type: char
              width: 1
              nullable: N
              map: "Link3.MCARE_ASG_IN"
            - name: NOTE_IN
              type: char
              width: 1
              nullable: N
              map: "Link3.NOTE_IN"
            - name: PCA_AUDIT_IN
              type: char
              width: 1
              nullable: N
              map: "Link3.PCA_AUDIT_IN"
            - name: PCP_SUBMT_IN
              type: char
              width: 1
              nullable: N
              map: "Link3.PCP_SUBMT_IN"
            - name: PROD_OOA_IN
              type: char
              width: 1
              nullable: N
              map: "Link3.PROD_OOA_IN"
            - name: ACDNT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Link3.ACDNT_DT_SK"
            - name: INPT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Link3.INPT_DT_SK"
            - name: MBR_PLN_ELIG_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Link3.MBR_PLN_ELIG_DT_SK"
            - name: NEXT_RVW_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Link3.NEXT_RVW_DT_SK"
            - name: PD_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Link3.PD_DT_SK"
            - name: PAYMT_DRAG_CYC_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Link3.PAYMT_DRAG_CYC_DT_SK"
            - name: PRCS_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Link3.PRCS_DT_SK"
            - name: RCVD_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Link3.RCVD_DT_SK"
            - name: SVC_STRT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Link3.SVC_STRT_DT_SK"
            - name: SVC_END_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Link3.SVC_END_DT_SK"
            - name: SMLR_ILNS_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Link3.SMLR_ILNS_DT_SK"
            - name: STTUS_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Link3.STTUS_DT_SK"
            - name: WORK_UNABLE_BEG_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Link3.WORK_UNABLE_BEG_DT_SK"
            - name: WORK_UNABLE_END_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Link3.WORK_UNABLE_END_DT_SK"
            - name: ACDNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Link3.ACDNT_AMT"
            - name: ACTL_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Link3.ACTL_PD_AMT"
            - name: ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Link3.ALW_AMT"
            - name: DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Link3.DSALW_AMT"
            - name: COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Link3.COINS_AMT"
            - name: CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Link3.CNSD_CHRG_AMT"
            - name: COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Link3.COPAY_AMT"
            - name: CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Link3.CHRG_AMT"
            - name: DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Link3.DEDCT_AMT"
            - name: PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Link3.PAYBL_AMT"
            - name: CLM_CT
              type: smallint
              width: 2
              nullable: N
              map: "Link3.CLM_CT"
            - name: MBR_AGE
              type: decimal
              width: 3
              map: "Link3.MBR_AGE"
            - name: ADJ_FROM_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "Link3.ADJ_FROM_CLM_ID"
            - name: ADJ_TO_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "Link3.ADJ_TO_CLM_ID"
            - name: DOC_TX_ID
              type: char
              width: 18
              nullable: N
              map: "Link3.DOC_TX_ID"
            - name: MCAID_RESUB_NO
              type: char
              width: 15
              map: "Link3.MCAID_RESUB_NO"
            - name: MCARE_ID
              type: char
              width: 12
              nullable: N
              map: "Link3.MCARE_ID"
            - name: MBR_SFX_NO
              type: char
              width: 2
              map: "Link3.MBR_SFX_NO"
            - name: PATN_ACCT_NO
              type: varchar
              width: 35
              map: "Link3.PATN_ACCT_NO"
            - name: PAYMT_REF_ID
              type: char
              width: 16
              nullable: N
              map: "Link3.PAYMT_REF_ID"
            - name: PROV_AGMNT_ID
              type: char
              width: 12
              nullable: N
              map: "Link3.PROV_AGMNT_ID"
            - name: RFRNG_PROV_TX
              type: varchar
              width: 70
              map: "Link3.RFRNG_PROV_TX"
            - name: SUB_ID
              type: char
              width: 14
              nullable: N
              map: "Link3.SUB_ID"
            - name: PCA_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link3.PCA_TYP_CD_SK"
            - name: REL_PCA_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "Link3.REL_PCA_CLM_SK"
            - name: REL_BASE_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "Link3.REL_BASE_CLM_SK"
            - name: CLM_UPDT_SW
              type: char
              width: 1
              nullable: N
              map: "Link3.CLM_UPDT_SW"
            - name: REMIT_SUPRSION_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Link3.REMIT_SUPRSION_AMT"
            - name: MCAID_STTUS_ID
              type: varchar
              width: 20
              nullable: N
              map: "Link3.MCAID_STTUS_ID"
            - name: PATN_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Link3.PATN_PD_AMT"
            - name: CLM_SUBMT_ICD_VRSN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link3.CLM_SUBMT_ICD_VRSN_CD_SK"
            - name: CLM_TXNMY_CD
              type: varchar
              width: 20
              map: "Link3.CLM_TXNMY_CD"
            - name: BILL_PAYMT_EXCL_IN
              type: char
              width: 1
              nullable: N
              map: "Link3.BILL_PAYMT_EXCL_IN"
            - name: ADJ_FROM_CLM
              type: char
              width: 12
              nullable: N
              map: "Link3.ADJ_FROM_CLM"
            - name: ADJ_TO_CLM
              type: char
              width: 12
              nullable: N
              map: "Link3.ADJ_TO_CLM"
        - name: ER
          type: map
          source: "Trans6.V0S198P2"
          alias: Link1
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: "Link1.CLM_SK"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link1.SRC_SYS_CD_SK"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "Link1.CLM_ID"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "Link1.CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "Link1.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: ADJ_FROM_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "Link1.ADJ_FROM_CLM_SK"
            - name: ADJ_TO_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "Link1.ADJ_TO_CLM_SK"
            - name: ALPHA_PFX_SK
              type: int
              width: 4
              nullable: N
              map: "Link1.ALPHA_PFX_SK"
            - name: CLM_EOB_EXCD_SK
              type: int
              width: 4
              nullable: N
              map: "Link1.CLM_EOB_EXCD_SK"
            - name: CLS_SK
              type: int
              width: 4
              nullable: N
              map: "Link1.CLS_SK"
            - name: CLS_PLN_SK
              type: int
              width: 4
              nullable: N
              map: "Link1.CLS_PLN_SK"
            - name: EXPRNC_CAT_SK
              type: int
              width: 4
              nullable: N
              map: "Link1.EXPRNC_CAT_SK"
            - name: FNCL_LOB_SK
              type: int
              width: 4
              nullable: N
              map: "Link1.FNCL_LOB_SK"
            - name: GRP_SK
              type: int
              width: 4
              nullable: N
              map: "Link1.GRP_SK"
            - name: MBR_SK
              type: int
              width: 4
              nullable: N
              map: "Link1.MBR_SK"
            - name: NTWK_SK
              type: int
              width: 4
              nullable: N
              map: "Link1.NTWK_SK"
            - name: PROD_SK
              type: int
              width: 4
              nullable: N
              map: "Link1.PROD_SK"
            - name: SUBGRP_SK
              type: int
              width: 4
              nullable: N
              map: "Link1.SUBGRP_SK"
            - name: SUB_SK
              type: int
              width: 4
              nullable: N
              map: "Link1.SUB_SK"
            - name: CLM_ACDNT_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link1.CLM_ACDNT_CD_SK"
            - name: CLM_ACDNT_ST_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link1.CLM_ACDNT_ST_CD_SK"
            - name: CLM_ACTV_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link1.CLM_ACTV_BCBS_PLN_CD_SK"
            - name: CLM_AGMNT_SRC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link1.CLM_AGMNT_SRC_CD_SK"
            - name: CLM_BTCH_ACTN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link1.CLM_BTCH_ACTN_CD_SK"
            - name: CLM_CAP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link1.CLM_CAP_CD_SK"
            - name: CLM_CAT_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link1.CLM_CAT_CD_SK"
            - name: CLM_CHK_CYC_OVRD_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link1.CLM_CHK_CYC_OVRD_CD_SK"
            - name: CLM_COB_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link1.CLM_COB_CD_SK"
            - name: CLM_FINL_DISP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link1.CLM_FINL_DISP_CD_SK"
            - name: CLM_INPT_METH_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link1.CLM_INPT_METH_CD_SK"
            - name: CLM_INPT_SRC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link1.CLM_INPT_SRC_CD_SK"
            - name: CLM_INTER_PLN_PGM_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link1.CLM_INTER_PLN_PGM_CD_SK"
            - name: CLM_NTWK_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link1.CLM_NTWK_STTUS_CD_SK"
            - name: CLM_NONPAR_PROV_PFX_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link1.CLM_NONPAR_PROV_PFX_CD_SK"
            - name: CLM_OTHR_BNF_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link1.CLM_OTHR_BNF_CD_SK"
            - name: CLM_PAYE_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link1.CLM_PAYE_CD_SK"
            - name: CLM_PRCS_CTL_AGNT_PFX_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link1.CLM_PRCS_CTL_AGNT_PFX_CD_SK"
            - name: CLM_SVC_DEFN_PFX_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link1.CLM_SVC_DEFN_PFX_CD_SK"
            - name: CLM_SVC_PROV_SPEC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link1.CLM_SVC_PROV_SPEC_CD_SK"
            - name: CLM_SVC_PROV_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link1.CLM_SVC_PROV_TYP_CD_SK"
            - name: CLM_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link1.CLM_STTUS_CD_SK"
            - name: CLM_SUBMTTING_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link1.CLM_SUBMTTING_BCBS_PLN_CD_SK"
            - name: CLM_SUB_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link1.CLM_SUB_BCBS_PLN_CD_SK"
            - name: CLM_SUBTYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link1.CLM_SUBTYP_CD_SK"
            - name: CLM_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link1.CLM_TYP_CD_SK"
            - name: ATCHMT_IN
              type: char
              width: 1
              nullable: N
              map: "Link1.ATCHMT_IN"
            - name: CLNCL_EDIT_IN
              type: char
              width: 1
              nullable: N
              map: "Link1.CLNCL_EDIT_IN"
            - name: COBRA_CLM_IN
              type: char
              width: 1
              nullable: N
              map: "Link1.COBRA_CLM_IN"
            - name: FIRST_PASS_IN
              type: char
              width: 1
              nullable: N
              map: "Link1.FIRST_PASS_IN"
            - name: HOST_IN
              type: char
              width: 1
              nullable: N
              map: "Link1.HOST_IN"
            - name: LTR_IN
              type: char
              width: 1
              nullable: N
              map: "Link1.LTR_IN"
            - name: MCARE_ASG_IN
              type: char
              width: 1
              nullable: N
              map: "Link1.MCARE_ASG_IN"
            - name: NOTE_IN
              type: char
              width: 1
              nullable: N
              map: "Link1.NOTE_IN"
            - name: PCA_AUDIT_IN
              type: char
              width: 1
              nullable: N
              map: "Link1.PCA_AUDIT_IN"
            - name: PCP_SUBMT_IN
              type: char
              width: 1
              nullable: N
              map: "Link1.PCP_SUBMT_IN"
            - name: PROD_OOA_IN
              type: char
              width: 1
              nullable: N
              map: "Link1.PROD_OOA_IN"
            - name: ACDNT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Link1.ACDNT_DT_SK"
            - name: INPT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Link1.INPT_DT_SK"
            - name: MBR_PLN_ELIG_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Link1.MBR_PLN_ELIG_DT_SK"
            - name: NEXT_RVW_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Link1.NEXT_RVW_DT_SK"
            - name: PD_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Link1.PD_DT_SK"
            - name: PAYMT_DRAG_CYC_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Link1.PAYMT_DRAG_CYC_DT_SK"
            - name: PRCS_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Link1.PRCS_DT_SK"
            - name: RCVD_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Link1.RCVD_DT_SK"
            - name: SVC_STRT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Link1.SVC_STRT_DT_SK"
            - name: SVC_END_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Link1.SVC_END_DT_SK"
            - name: SMLR_ILNS_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Link1.SMLR_ILNS_DT_SK"
            - name: STTUS_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Link1.STTUS_DT_SK"
            - name: WORK_UNABLE_BEG_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Link1.WORK_UNABLE_BEG_DT_SK"
            - name: WORK_UNABLE_END_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Link1.WORK_UNABLE_END_DT_SK"
            - name: ACDNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Link1.ACDNT_AMT"
            - name: ACTL_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Link1.ACTL_PD_AMT"
            - name: ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Link1.ALW_AMT"
            - name: DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Link1.DSALW_AMT"
            - name: COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Link1.COINS_AMT"
            - name: CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Link1.CNSD_CHRG_AMT"
            - name: COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Link1.COPAY_AMT"
            - name: CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Link1.CHRG_AMT"
            - name: DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Link1.DEDCT_AMT"
            - name: PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Link1.PAYBL_AMT"
            - name: CLM_CT
              type: smallint
              width: 2
              nullable: N
              map: "Link1.CLM_CT"
            - name: MBR_AGE
              type: decimal
              width: 3
              map: "Link1.MBR_AGE"
            - name: ADJ_FROM_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "Link1.ADJ_FROM_CLM_ID"
            - name: ADJ_TO_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "Link1.ADJ_TO_CLM_ID"
            - name: DOC_TX_ID
              type: char
              width: 18
              nullable: N
              map: "Link1.DOC_TX_ID"
            - name: MCAID_RESUB_NO
              type: char
              width: 15
              map: "Link1.MCAID_RESUB_NO"
            - name: MCARE_ID
              type: char
              width: 12
              nullable: N
              map: "Link1.MCARE_ID"
            - name: MBR_SFX_NO
              type: char
              width: 2
              map: "Link1.MBR_SFX_NO"
            - name: PATN_ACCT_NO
              type: varchar
              width: 35
              map: "Link1.PATN_ACCT_NO"
            - name: PAYMT_REF_ID
              type: char
              width: 16
              nullable: N
              map: "Link1.PAYMT_REF_ID"
            - name: PROV_AGMNT_ID
              type: char
              width: 12
              nullable: N
              map: "Link1.PROV_AGMNT_ID"
            - name: RFRNG_PROV_TX
              type: varchar
              width: 70
              map: "Link1.RFRNG_PROV_TX"
            - name: SUB_ID
              type: char
              width: 14
              nullable: N
              map: "Link1.SUB_ID"
            - name: PCA_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link1.PCA_TYP_CD_SK"
            - name: REL_PCA_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "Link1.REL_PCA_CLM_SK"
            - name: REL_BASE_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "Link1.REL_BASE_CLM_SK"
            - name: CLM_UPDT_SW
              type: char
              width: 1
              nullable: N
              map: "Link1.CLM_UPDT_SW"
            - name: REMIT_SUPRSION_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Link1.REMIT_SUPRSION_AMT"
            - name: MCAID_STTUS_ID
              type: varchar
              width: 20
              nullable: N
              map: "Link1.MCAID_STTUS_ID"
            - name: PATN_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Link1.PATN_PD_AMT"
            - name: CLM_SUBMT_ICD_VRSN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link1.CLM_SUBMT_ICD_VRSN_CD_SK"
            - name: CLM_TXNMY_CD
              type: varchar
              width: 20
              map: "Link1.CLM_TXNMY_CD"
            - name: BILL_PAYMT_EXCL_IN
              type: char
              width: 1
              nullable: N
              map: "Link1.BILL_PAYMT_EXCL_IN"
            - name: ADJ_FROM_CLM
              type: char
              width: 12
              nullable: N
              map: "Link1.ADJ_FROM_CLM"
            - name: ADJ_TO_CLM
              type: char
              width: 12
              nullable: N
              map: "Link1.ADJ_TO_CLM"
        - name: IDSClm_for_reversals
          type: dbReader
          originalDb: db2
          query: "SELECT CLM.SRC_SYS_CD_SK as SRC_SYS_CD_SK,CLM.CLM_SK as CLM_SK,CLM.CLM_ID as CLM_ID,CLM.CLM_STTUS_CD_SK as CLM_STTUS_CD_SK,CLM.PD_DT_SK as PD_DT_SK FROM #$IDSOwner#.CLM CLM WHERE CLM.SRC_SYS_CD_SK=? AND CLM.CLM_SK=? AND CLM.CLM_ID=?"
          schema: 
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: CLM_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PD_DT_SK
              type: char
              width: 10
              nullable: N
          keys:
            - SRC_SYS_CD_SK
            - CLM_SK
            - CLM_ID
        - name: IDS_CLM
          type: dbReader
          originalDb: db2
          query: "\r\nSELECT \r\nC.CLM_ID,\r\nC.CLM_SK,\r\nCD2.TRGT_CD AS TRGT_CD\r\n\r\nFROM #$IDSOwner#.CLM C, #$IDSOwner#.CD_MPPNG CD, #$IDSOwner#.CD_MPPNG CD1,#$IDSOwner#.CD_MPPNG CD2\r\nwhere\r\nC.SRC_SYS_CD_SK = CD.CD_MPPNG_SK AND\r\n           CD.TRGT_CD = 'BCA' AND\r\n           C.CLM_STTUS_CD_SK = CD1.CD_MPPNG_SK AND\r\n           CD1.TRGT_CD IN( 'A02', 'A09') AND\r\n           C.CLM_SUBTYP_CD_SK = CD2.CD_MPPNG_SK AND\r\n           CD2.TRGT_CD = 'RX'  AND C.CLM_ID=?"
          schema: 
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
            - name: TRGT_CD
              type: varchar
              width: 20
          keys:
            - CLM_ID
        - name: IDS_K_CLM_FROM
          type: dbReader
          originalDb: db2
          query: "SELECT CLM.SRC_SYS_CD_SK as SRC_SYS_CD_SK,CLM.CLM_ID as CLM_ID,CLM.CLM_SK as CLM_SK FROM #$IDSOwner#.K_CLM CLM WHERE CLM.SRC_SYS_CD_SK=? AND CLM.CLM_ID=?"
          schema: 
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
          keys:
            - SRC_SYS_CD_SK
            - CLM_ID
        - name: IDS_K_CLM_TO
          type: dbReader
          originalDb: db2
          query: "SELECT CLM.SRC_SYS_CD_SK as SRC_SYS_CD_SK,CLM.CLM_ID as CLM_ID,CLM.CLM_SK as CLM_SK FROM #$IDSOwner#.K_CLM CLM WHERE CLM.SRC_SYS_CD_SK=? AND CLM.CLM_ID=?"
          schema: 
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
          keys:
            - SRC_SYS_CD_SK
            - CLM_ID
        - name: IDSClm_for_Pca_Update
          type: dbReader
          originalDb: db2
          query: "SELECT CLM.CLM_SK as CLM_SK,CLM.CLM_ID as CLM_ID,CLM.LAST_UPDT_RUN_CYC_EXCTN_SK as LAST_UPDT_RUN_CYC_EXCTN_SK,CLM.REL_BASE_CLM_SK as REL_BASE_CLM_SK FROM #$IDSOwner#.CLM CLM WHERE CLM.CLM_SK=?"
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: REL_BASE_CLM_SK
              type: int
              width: 4
              nullable: N
          keys:
            - CLM_SK
        - name: PCA_Claims
          type: dbReader
          originalDb: db2
          query: "SELECT CLM.SRC_SYS_CD_SK as SRC_SYS_CD_SK,CLM.CLM_ID as CLM_ID,CLM.CLM_SK as CLM_SK FROM #$IDSOwner#.CLM CLM WHERE CLM.SRC_SYS_CD_SK=? AND CLM.CLM_ID=?"
          schema: 
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
          keys:
            - SRC_SYS_CD_SK
            - CLM_ID
        - name: NascoClmUpdate
          type: dbWriter
          originalDb: db2
          table: "#$IDSOwner#.CLM"
          tableAction: append
          writeMode: update
          table: "#$IDSOwner#.CLM"
          source: "nasco_found_clms.updtCLAIM"
          updateExpr:
          - name: LAST_UPDT_RUN_CYC_EXCTN_SK
            expr: "ORCHESTRATE.LAST_UPDT_RUN_CYC_EXCTN_SK"
          - name: ADJ_TO_CLM_SK
            expr: "ORCHESTRATE.ADJ_TO_CLM_SK"
          - name: CLM_STTUS_CD_SK
            expr: "ORCHESTRATE.CLM_STTUS_CD_SK"
          - name: ADJ_TO_CLM_ID
            expr: "ORCHESTRATE.ADJ_TO_CLM_ID"
          updateCondition: "(CLM_SK=ORCHESTRATE.CLM_SK)"
        - name: PcaClmUpdate
          type: dbWriter
          originalDb: db2
          table: "#$IDSOwner#.CLM"
          tableAction: append
          writeMode: update
          table: "#$IDSOwner#.CLM"
          source: Trans5
          updateExpr:
          - name: LAST_UPDT_RUN_CYC_EXCTN_SK
            expr: "ORCHESTRATE.LAST_UPDT_RUN_CYC_EXCTN_SK"
          - name: REL_PCA_CLM_SK
            expr: "ORCHESTRATE.REL_PCA_CLM_SK"
          - name: REL_BASE_CLM_SK
            expr: "ORCHESTRATE.REL_BASE_CLM_SK"
          updateCondition: "(CLM_SK=ORCHESTRATE.CLM_SK)"
        - name: Trans5.lookup
          type: lookup
          option:
            joinType: left
          sources: 
            - name: ClmPcaUpdts.PcaClm2Updt
              alias: PcaClm2Updt
              joinFilter: ""
              joinExpression: []
            - name: IDSClm_for_Pca_Update
              alias: Clm2
              joinFilter: ""
              joinExpression:
              - fromExpr: "Clm2.CLM_SK"
                toExpr: "PcaClm2Updt.REL_BASE_CLM_SK"
                caseInSensitive: true
          joinCondition: 
            - expression: "Clm2.CLM_SK = PcaClm2Updt.REL_BASE_CLM_SK"
              caseInSensitive: true
          schema: 
            - name: updtCLAIM_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "Clm2.CLM_SK"
            - name: updtCLAIM_LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "PcaClm2Updt.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: updtCLAIM_REL_PCA_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "PcaClm2Updt.CLM_SK"
            - name: updtCLAIM_REL_BASE_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "'1'"
            - name: filter_updtCLAIM
              type: smallint
              width: 2
              nullable: N
              map: "if (IsNull(Clm2.CLM_SK) = @FALSE) then 1 else 0"
        - name: Trans5.filter
          type: filter
          source: Trans5.lookup
          filter: "filter_updtCLAIM = 1"
          schema: 
            - name: updtCLAIM_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: updtCLAIM_LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: updtCLAIM_REL_PCA_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: updtCLAIM_REL_BASE_CLM_SK
              type: int
              width: 4
              nullable: N
        - name: Trans5
          type: map
          source: Trans5.filter
          alias: updtCLAIM
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: "updtCLAIM.updtCLAIM_CLM_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "updtCLAIM.updtCLAIM_LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: REL_PCA_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "updtCLAIM.updtCLAIM_REL_PCA_CLM_SK"
            - name: REL_BASE_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "updtCLAIM.updtCLAIM_REL_BASE_CLM_SK"
        - name: Trns1.lookup
          type: lookup
          stageVars:
            - name: svEMRSrcSysCd
              type: decimal
              width: 18
              precision: 4
              map: " If (Trim(ClmCrfIn1.SRC_SYS_CD) = 'CLAYPLATTE' OR Trim(ClmCrfIn1.SRC_SYS_CD) = 'JAYHAWK' OR Trim(ClmCrfIn1.SRC_SYS_CD) = 'ENCOMPASS' OR Trim(ClmCrfIn1.SRC_SYS_CD) = 'LIBERTYHOSP' OR Trim(ClmCrfIn1.SRC_SYS_CD) = 'MERITAS' OR Trim(ClmCrfIn1.SRC_SYS_CD) = 'NORTHLAND' OR Trim(ClmCrfIn1.SRC_SYS_CD) = 'OLATHEMED' OR Trim(ClmCrfIn1.SRC_SYS_CD) = 'PROVIDENCE' OR Trim(ClmCrfIn1.SRC_SYS_CD) = 'PROVSTLUKES' OR Trim(ClmCrfIn1.SRC_SYS_CD) = 'SUNFLOWER' OR Trim(ClmCrfIn1.SRC_SYS_CD) = 'TRUMAN' OR Trim(ClmCrfIn1.SRC_SYS_CD) = 'UNTDMEDGRP' OR Trim(ClmCrfIn1.SRC_SYS_CD) = 'MOSAIC' OR Trim(ClmCrfIn1.SRC_SYS_CD) = 'MOSAICLIFECARE' OR Trim(ClmCrfIn1.SRC_SYS_CD) = 'BARRYPOINTE' OR Trim(ClmCrfIn1.SRC_SYS_CD) = 'PRIMEMO' OR Trim(ClmCrfIn1.SRC_SYS_CD) = 'SPIRA' OR Trim(ClmCrfIn1.SRC_SYS_CD) = 'LEAWOODFMLYCARE' OR Trim(ClmCrfIn1.SRC_SYS_CD) = 'HCA' OR Trim(ClmCrfIn1.SRC_SYS_CD) = 'NONSTDSUPLMTDATA' OR Trim(ClmCrfIn1.SRC_SYS_CD) = 'CHILDRENSMERCY' OR Trim(ClmCrfIn1.SRC_SYS_CD) = 'CENTRUSHEALTH' OR Trim(ClmCrfIn1.SRC_SYS_CD) = 'MOHEALTH' OR Trim(ClmCrfIn1.SRC_SYS_CD) = 'GOLDENVALLEY' OR Trim(ClmCrfIn1.SRC_SYS_CD) = 'JEFFERSON' OR Trim(ClmCrfIn1.SRC_SYS_CD) = 'WESTMOMEDCNTR' OR Trim(ClmCrfIn1.SRC_SYS_CD) = 'BLUESPRINGS' OR Trim(ClmCrfIn1.SRC_SYS_CD) = 'EXCELSIOR' OR Trim(ClmCrfIn1.SRC_SYS_CD) = 'HARRISONVILLE' OR Trim(ClmCrfIn1.SRC_SYS_CD) = 'ASCENTIST') THEN 'EMR' ELSE ''"
              initValue: "0"
            - name: svSBVSrcSysCd
              type: decimal
              width: 18
              precision: 4
              map: " IF (Trim(ClmCrfIn1.SRC_SYS_CD) = 'DOMINION' OR Trim(ClmCrfIn1.SRC_SYS_CD) = 'MARC') THEN 'SBV' ELSE ''"
              initValue: "0"
            - name: svCdMpngSrcSysCd
              type: decimal
              width: 18
              precision: 4
              map: " If ClmCrfIn1.SRC_SYS_CD = 'BCBSA' Then 'BCA' Else  if (ClmCrfIn1.SRC_SYS_CD = 'LUMERIS') then 'FACETS' else ClmCrfIn1.SRC_SYS_CD"
              initValue: "0"
            - name: NascoDupClm
              type: decimal
              width: 18
              precision: 4
              map: " If ClmCrfIn1.SRC_SYS_CD = \"FACETS\" and IsNull(nasco_dup_lkup1.CLM_ID) = @FALSE then 'Y' else 'N'"
              initValue: "0"
            - name: SrcSysCdMbr
              type: decimal
              width: 18
              precision: 4
              map: " if (ClmCrfIn1.SRC_SYS_CD = 'DENTAQUEST' or ClmCrfIn1.SRC_SYS_CD = 'SOLUTRAN' or svEMRSrcSysCd = 'EMR' or ClmCrfIn1.SRC_SYS_CD = 'LVNGHLTH' or ClmCrfIn1.SRC_SYS_CD[1, 6] = 'NATION' or svSBVSrcSysCd = 'SBV') Then 'FACETS' Else  If IsNull((ClmCrfIn1.SRC_SYS_CD)) or Len(Trim((ClmCrfIn1.SRC_SYS_CD))) = 0 Then \"UNK\" Else  If Trim((ClmCrfIn1.SRC_SYS_CD)) = 'PCT' then 'FACETS' ELSE  If Trim((ClmCrfIn1.SRC_SYS_CD)) = 'WELLDYNERX' then 'FACETS' else  If Trim((ClmCrfIn1.SRC_SYS_CD)) = 'PCS' then 'FACETS' else  If Trim((ClmCrfIn1.SRC_SYS_CD)) = 'CAREMARK' then 'FACETS' else  if Trim((ClmCrfIn1.SRC_SYS_CD)) = 'ARGUS' then 'FACETS' else  If Trim((ClmCrfIn1.SRC_SYS_CD)) = 'EDC' then 'FACETS' else  If Trim((ClmCrfIn1.SRC_SYS_CD)) = 'OT@2' then 'FACETS' else  If Trim((ClmCrfIn1.SRC_SYS_CD)) = 'ADOL' then 'FACETS' else  If Trim((ClmCrfIn1.SRC_SYS_CD)) = 'MOHSAIC' then 'FACETS' else  If Trim((ClmCrfIn1.SRC_SYS_CD)) = 'CAREADVANCE' then 'FACETS' ELSE  If Trim((ClmCrfIn1.SRC_SYS_CD)) = 'ESI' then 'FACETS' ELSE  If Trim((ClmCrfIn1.SRC_SYS_CD)) = 'OPTUMRX' then 'FACETS' else  If Trim((ClmCrfIn1.SRC_SYS_CD)) = 'MCSOURCE' then 'FACETS' else  If Trim((ClmCrfIn1.SRC_SYS_CD)) = 'MCAID' then 'FACETS' else  If Trim((ClmCrfIn1.SRC_SYS_CD)) = 'MEDTRAK' then 'FACETS' else  If Trim((ClmCrfIn1.SRC_SYS_CD)) = 'BCBSSC' then 'FACETS' else  If Trim((ClmCrfIn1.SRC_SYS_CD)) = 'BCA' then 'FACETS' else  If Trim((ClmCrfIn1.SRC_SYS_CD)) = 'BCBSA' then 'FACETS' else  If Trim((ClmCrfIn1.SRC_SYS_CD)) = 'SAVRX' then 'FACETS' else  If Trim((ClmCrfIn1.SRC_SYS_CD)) = 'LDI' then 'FACETS' Else  If Trim((ClmCrfIn1.SRC_SYS_CD)) = 'EYEMED' Then 'FACETS' Else  If Trim((ClmCrfIn1.SRC_SYS_CD)) = 'CVS' Then 'FACETS' Else  If Trim((ClmCrfIn1.SRC_SYS_CD)) = 'LUMERIS' Then 'FACETS' else  If Trim((ClmCrfIn1.SRC_SYS_CD)) = 'MEDIMPACT' Then 'FACETS' Else (ClmCrfIn1.SRC_SYS_CD)"
              initValue: "-1"
            - name: SrcSysCdProd
              type: decimal
              width: 18
              precision: 4
              map: " if (ClmCrfIn1.SRC_SYS_CD = 'DENTAQUEST' or svEMRSrcSysCd = 'EMR' or ClmCrfIn1.SRC_SYS_CD = 'SOLUTRAN' or ClmCrfIn1.SRC_SYS_CD = 'LVNGHLTH' or ClmCrfIn1.SRC_SYS_CD[1, 6] = 'NATION' or svSBVSrcSysCd = 'SBV') Then 'FACETS' Else  If IsNull((ClmCrfIn1.SRC_SYS_CD)) or Len(Trim((ClmCrfIn1.SRC_SYS_CD))) = 0 Then \"UNK\" Else  If Trim((ClmCrfIn1.SRC_SYS_CD)) = 'PCT' then 'FACETS' else  If Trim((ClmCrfIn1.SRC_SYS_CD)) = 'WELLDYNERX' then 'FACETS' else  If Trim((ClmCrfIn1.SRC_SYS_CD)) = 'PCS' then 'FACETS' else  If Trim((ClmCrfIn1.SRC_SYS_CD)) = 'CAREMARK' then 'FACETS' else  if Trim((ClmCrfIn1.SRC_SYS_CD)) = 'ARGUS' then 'FACETS' else  If Trim((ClmCrfIn1.SRC_SYS_CD)) = 'EDC' then 'FACETS' else  If Trim((ClmCrfIn1.SRC_SYS_CD)) = 'OT@2' then 'FACETS' else  If Trim((ClmCrfIn1.SRC_SYS_CD)) = 'ADOL' then 'FACETS' else  If Trim((ClmCrfIn1.SRC_SYS_CD)) = 'ESI' then 'FACETS' else  If Trim((ClmCrfIn1.SRC_SYS_CD)) = 'OPTUMRX' then 'FACETS' else  If Trim((ClmCrfIn1.SRC_SYS_CD)) = 'MCSOURCE' then 'FACETS' else  If Trim((ClmCrfIn1.SRC_SYS_CD)) = 'MCAID' then 'FACETS' else  If Trim((ClmCrfIn1.SRC_SYS_CD)) = 'MEDTRAK' then 'FACETS' else  If Trim((ClmCrfIn1.SRC_SYS_CD)) = 'BCBSSC' then 'FACETS' else  If Trim((ClmCrfIn1.SRC_SYS_CD)) = 'BCA' then 'FACETS' else  If Trim((ClmCrfIn1.SRC_SYS_CD)) = 'BCBSA' then 'FACETS' else  If Trim((ClmCrfIn1.SRC_SYS_CD)) = 'SAVRX' then 'FACETS' else  If Trim((ClmCrfIn1.SRC_SYS_CD)) = 'LDI' then 'FACETS' Else  If Trim((ClmCrfIn1.SRC_SYS_CD)) = 'EYEMED' Then 'FACETS' Else  If Trim((ClmCrfIn1.SRC_SYS_CD)) = 'CVS' Then 'FACETS' Else  If Trim((ClmCrfIn1.SRC_SYS_CD)) = 'LUMERIS' Then 'FACETS' else  If Trim((ClmCrfIn1.SRC_SYS_CD)) = 'MEDIMPACT' Then 'FACETS' Else (ClmCrfIn1.SRC_SYS_CD)"
              initValue: "-1"
            - name: ClmAdjFromClmSk
              type: decimal
              width: 18
              precision: 4
              map: " If NascoDupClm = 'N' and ClmCrfIn1.SRC_SYS_CD = 'BCA' and Right(ClmCrfIn1.CLM_ID, 1) = 'R' And Exist_Ids.TRGT_CD = 'RX' then Exist_Ids.CLM_SK Else  If NascoDupClm = 'N' then GetFkeyClm(ClmCrfIn1.SRC_SYS_CD, ClmCrfIn1.CLM_SK, ClmCrfIn1.ADJ_FROM_CLM_ID, Logging) else GetFkeyClm('NPS', ClmCrfIn1.CLM_SK, nasco_dup_lkup1.NASCO_CLM_ID, Logging)"
              initValue: "1"
            - name: ClmAdjToClmCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyClm(ClmCrfIn1.SRC_SYS_CD, ClmCrfIn1.CLM_SK, ClmCrfIn1.ADJ_TO_CLM_ID, Logging)"
              initValue: "1"
            - name: ClsPlnSk
              type: decimal
              width: 18
              precision: 4
              map: " If Trim(ClmCrfIn1.CLS_PLN) = 'UNK' Then 0 Else  If Trim(ClmCrfIn1.CLS_PLN) = 'NA' Then 1 Else GetFkeyClsPln(SrcSysCdMbr, ClmCrfIn1.CLM_SK, ClmCrfIn1.CLS_PLN, Logging)"
              initValue: "1"
            - name: ClsSk
              type: decimal
              width: 18
              precision: 4
              map: " If Trim(ClmCrfIn1.CLS) = 'UNK' Then 0 Else  If Trim(ClmCrfIn1.CLS) = 'NA' Then 1 Else GetFkeyCls(SrcSysCdMbr, ClmCrfIn1.CLM_SK, ClmCrfIn1.GRP, ClmCrfIn1.CLS, Logging)"
              initValue: "-1"
            - name: ExpCatCdSk
              type: decimal
              width: 18
              precision: 4
              map: " If Trim(ClmCrfIn1.EXPRNC_CAT) = 'UNK' Then 0 Else  If Trim(ClmCrfIn1.EXPRNC_CAT) = 'NA' Then 1 Else  If ClmCrfIn1.SRC_SYS_CD = 'NPS' then GetFkeyExprncCat('FACETS', ClmCrfIn1.CLM_SK, ClmCrfIn1.EXPRNC_CAT, Logging) Else  If ClmCrfIn1.SRC_SYS_CD = 'LVNGHLTH' then GetFkeyExprncCat('FACETS', ClmCrfIn1.CLM_SK, ClmCrfIn1.EXPRNC_CAT, Logging) else GetFkeyExprncCat(SrcSysCdProd, ClmCrfIn1.CLM_SK, ClmCrfIn1.EXPRNC_CAT, Logging)"
              initValue: "1"
            - name: GrpSk
              type: decimal
              width: 18
              precision: 4
              map: " If Trim(ClmCrfIn1.GRP) = 'UNK' Then 0 Else  If Trim(ClmCrfIn1.GRP) = 'NA' Then 1 Else GetFkeyGrp(SrcSysCdMbr, ClmCrfIn1.CLM_SK, ClmCrfIn1.GRP, Logging)"
              initValue: "1"
            - name: MbrSk
              type: decimal
              width: 18
              precision: 4
              map: " If Trim(ClmCrfIn1.MBR_CK) = 'UNK' Then 0 Else  If Trim(ClmCrfIn1.MBR_CK) = 'NA' Then 1 Else GetFkeyMbr(SrcSysCdMbr, ClmCrfIn1.CLM_SK, ClmCrfIn1.MBR_CK, Logging)"
              initValue: "1"
            - name: NtwkSk
              type: decimal
              width: 18
              precision: 4
              map: " If Trim(ClmCrfIn1.NTWK) = 'UNK' Then 0 Else  If Trim(ClmCrfIn1.NTWK) = 'NA' Then 1 Else  If (Len(Trim(ClmCrfIn1.NTWK)) = 0 and Trim(ClmCrfIn1.SRC_SYS_CD) = \"FACETS\") then GetFkeyNtwk(\"FACETS\", ClmCrfIn1.CLM_SK, \" \", Logging) else GetFkeyNtwk(\"FACETS\", ClmCrfIn1.CLM_SK, ClmCrfIn1.NTWK, Logging)"
              initValue: "1"
            - name: ProdSk
              type: decimal
              width: 18
              precision: 4
              map: " If Trim(ClmCrfIn1.PROD) = 'UNK' Then 0 Else  If Trim(ClmCrfIn1.PROD) = 'NA' Then 1 Else GetFkeyProd(SrcSysCdProd, ClmCrfIn1.CLM_SK, ClmCrfIn1.PROD, Logging)"
              initValue: "1"
            - name: SubGrpSk
              type: decimal
              width: 18
              precision: 4
              map: " If Trim(ClmCrfIn1.SUBGRP) = 'UNK' Then 0 Else  If Trim(ClmCrfIn1.SUBGRP) = 'NA' Then 1 Else GetFkeySubgrp(SrcSysCdMbr, ClmCrfIn1.CLM_SK, ClmCrfIn1.GRP, ClmCrfIn1.SUBGRP, Logging)"
              initValue: "1"
            - name: SubSk
              type: decimal
              width: 18
              precision: 4
              map: " If Trim(ClmCrfIn1.SUB_CK) = 'UNK' Then 0 Else  If Trim(ClmCrfIn1.SUB_CK) = 'NA' Then 1 Else GetFkeySub(SrcSysCdMbr, ClmCrfIn1.CLM_SK, ClmCrfIn1.SUB_CK, Logging)"
              initValue: "1"
            - name: ActBcbsSk
              type: decimal
              width: 18
              precision: 4
              map: " If (ClmCrfIn1.SRC_SYS_CD = 'BCA' Or svCdMpngSrcSysCd = 'BCA') Then GetFkeyCodes('FACETS', ClmCrfIn1.CLM_SK, \"ACTIVATING BCBS PLAN\", ClmCrfIn1.CLM_ACTIVATING_BCBS_PLN_CD, Logging) Else GetFkeyCodes(svCdMpngSrcSysCd, ClmCrfIn1.CLM_SK, \"ACTIVATING BCBS PLAN\", ClmCrfIn1.CLM_ACTIVATING_BCBS_PLN_CD, Logging)"
              initValue: "1"
            - name: CapClmCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(svCdMpngSrcSysCd, ClmCrfIn1.CLM_SK, \"CAPITATED CLAIM\", ClmCrfIn1.CLM_CAP_CD, Logging)"
              initValue: "1"
            - name: ClmAcdtStCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(\"FACETS\", ClmCrfIn1.CLM_SK, \"STATE\", ClmCrfIn1.CLM_ACDNT_ST_CD, Logging)"
              initValue: "1"
            - name: ClmAgmntSrcCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(svCdMpngSrcSysCd, ClmCrfIn1.CLM_SK, \"CLAIM AGREEMENT SOURCE\", ClmCrfIn1.CLM_AGMNT_SRC_CD, Logging)"
              initValue: "1"
            - name: ClmBtchActnCdSk
              type: decimal
              width: 0
              map: "GetFkeyCodes(svCdMpngSrcSysCd, ClmCrfIn1.CLM_SK, \"CLAIM BATCH ACTION\", ClmCrfIn1.CLM_BTCH_ACTN_CD, Logging)"
              initValue: "1"
            - name: ClmCatCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(svCdMpngSrcSysCd, ClmCrfIn1.CLM_SK, \"CLAIM CATEGORY\", ClmCrfIn1.CLM_CAT_CD, Logging)"
              initValue: "1"
            - name: ClmChkCycOvdSk
              type: decimal
              width: 0
              map: "GetFkeyCodes(svCdMpngSrcSysCd, ClmCrfIn1.CLM_SK, \"CLAIM CHECK CYCLE OVERRIDE\", ClmCrfIn1.CLM_CHK_CYC_OVRD_CD, Logging)"
              initValue: "1"
            - name: ClmCobCdSk
              type: decimal
              width: 0
              map: "GetFkeyCodes(svCdMpngSrcSysCd, ClmCrfIn1.CLM_SK, \"CLAIM COB\", ClmCrfIn1.CLM_COB_CD, Logging)"
              initValue: "1"
            - name: ClmEOBExcdSk
              type: decimal
              width: 0
              map: " If ClmCrfIn1.SRC_SYS_CD = \"LVNGHLTH\" Then GetFkeyExcd(\"FACETS\", ClmCrfIn1.CLM_SK, ClmCrfIn1.CLM_EOB_EXCD, Logging) Else GetFkeyExcd(ClmCrfIn1.SRC_SYS_CD, ClmCrfIn1.CLM_SK, ClmCrfIn1.CLM_EOB_EXCD, Logging)"
              initValue: "1"
            - name: ClmFinlDispCdSk
              type: decimal
              width: 0
              map: "GetFkeyCodes(svCdMpngSrcSysCd, ClmCrfIn1.CLM_SK, \"CLAIM FINALIZE DISPOSITION\", ClmCrfIn1.FINL_DISP_CD, Logging)"
              initValue: "1"
            - name: ClmInNtwkSk
              type: decimal
              width: 0
              map: " If ClmCrfIn1.SRC_SYS_CD = 'BCBSSC' Then  If (GetFkeyCodes(svCdMpngSrcSysCd, ClmCrfIn1.CLM_SK, \"CLAIM IN NETWORK\", ClmCrfIn1.CLM_NTWK_STTUS_CD, 'X') = 0) Then GetFkeyCodes('FACETS', ClmCrfIn1.CLM_SK, \"CLAIM IN NETWORK\", ClmCrfIn1.CLM_NTWK_STTUS_CD, Logging) Else GetFkeyCodes(svCdMpngSrcSysCd, ClmCrfIn1.CLM_SK, \"CLAIM IN NETWORK\", ClmCrfIn1.CLM_NTWK_STTUS_CD, Logging) Else GetFkeyCodes(svCdMpngSrcSysCd, ClmCrfIn1.CLM_SK, \"CLAIM IN NETWORK\", ClmCrfIn1.CLM_NTWK_STTUS_CD, Logging)"
              initValue: "1"
            - name: ClmInptMethSk
              type: decimal
              width: 0
              map: "GetFkeyCodes(svCdMpngSrcSysCd, ClmCrfIn1.CLM_SK, \"CLAIM INPUT METHOD\", ClmCrfIn1.CLM_INPT_METH_CD, Logging)"
              initValue: "1"
            - name: ClmInptSrcSk
              type: decimal
              width: 0
              map: "GetFkeyCodes(svCdMpngSrcSysCd, ClmCrfIn1.CLM_SK, \"CLAIM INPUT SOURCE\", ClmCrfIn1.CLM_INPT_SRC_CD, Logging)"
              initValue: "1"
            - name: ClmIppCdSk
              type: decimal
              width: 0
              map: "GetFkeyCodes(svCdMpngSrcSysCd, ClmCrfIn1.CLM_SK, \"CLAIM INTER PLAN PROGRAM CODE\", ClmCrfIn1.CLM_IPP_CD, Logging)"
              initValue: "-1"
            - name: ClmOtherBnfSk
              type: decimal
              width: 0
              map: "GetFkeyCodes(svCdMpngSrcSysCd, ClmCrfIn1.CLM_SK, \"CLAIM OTHER BENEFIT\", ClmCrfIn1.CLM_OTHER_BNF_CD, Logging)"
              initValue: "1"
            - name: ClmPayeCdSk
              type: decimal
              width: 0
              map: "GetFkeySrcTrgtDomainCodes(svCdMpngSrcSysCd, ClmCrfIn1.CLM_SK, \"CLAIM PAYEE\", ClmCrfIn1.CLM_PAYE_CD, \"CLAIM PAYEE\", Logging)"
              initValue: "1"
            - name: ClmPrcsCtlAgntPfxSk
              type: decimal
              width: 0
              map: "GetFkeyCodes(svCdMpngSrcSysCd, ClmCrfIn1.CLM_SK, \"CLAIM PROCESSING CONTROL AGENT PREFIX\", ClmCrfIn1.CLM_PRCS_CTL_AGNT_PFX_CD, Logging)"
              initValue: "1"
            - name: ClmSvcDefnPfxCdSk
              type: decimal
              width: 0
              map: "GetFkeyCodes(svCdMpngSrcSysCd, ClmCrfIn1.CLM_SK, \"CLAIM SERVICE DEFINITION PREFIX\", ClmCrfIn1.CLM_SVC_DEFN_PFX_CD, Logging)"
              initValue: "1"
            - name: ClmSttusCdSk
              type: decimal
              width: 0
              map: "GetFkeyCodes(svCdMpngSrcSysCd, ClmCrfIn1.CLM_SK, \"CLAIM STATUS\", ClmCrfIn1.CLM_STTUS_CD, Logging)"
              initValue: "1"
            - name: ClmSubTypCdSk
              type: decimal
              width: 0
              map: "GetFkeyCodes(svCdMpngSrcSysCd, ClmCrfIn1.CLM_SK, \"CLAIM SUBTYPE\", ClmCrfIn1.CLM_SUBTYP_CD, Logging)"
              initValue: "1"
            - name: ClmTypCdSk
              type: decimal
              width: 0
              map: "GetFkeyCodes(svCdMpngSrcSysCd, ClmCrfIn1.CLM_SK, \"CLAIM TYPE\", ClmCrfIn1.CLM_TYP_CD, Logging)"
              initValue: "1"
            - name: SvcProvDomain
              type: decimal
              width: 18
              precision: 4
              map: " If ClmCrfIn1.SRC_SYS_CD = \"NPS\" then  If Upcase(ClmCrfIn1.CLM_SUBTYP_CD) = 'PR' Then \"SERVICE PROVIDER TYPE\" Else  if (Upcase(ClmCrfIn1.CLM_SUBTYP_CD) = \"IP\" or Upcase(ClmCrfIn1.CLM_SUBTYP_CD) = \"OP\") Then \"PROVIDER FACILITY TYPE\" Else \"NA\" else  If ClmCrfIn1.SRC_SYS_CD = \"FACETS\" then  if ClmCrfIn1.PRPR_ENTITY = \"F\" then \"PROVIDER FACILITY TYPE\" else  if (ClmCrfIn1.PRPR_ENTITY = \"I\" or ClmCrfIn1.PRPR_ENTITY = \"P\" or ClmCrfIn1.PRPR_ENTITY = \"G\") then \"SERVICE PROVIDER TYPE\" else \"NA\" else  If (ClmCrfIn1.SRC_SYS_CD = \"ESI\" or ClmCrfIn1.SRC_SYS_CD = \"OPTUMRX\" or ClmCrfIn1.SRC_SYS_CD = \"MEDIMPACT\" or ClmCrfIn1.SRC_SYS_CD = \"WELLDYNERX\" Or ClmCrfIn1.SRC_SYS_CD = \"MCAID\" Or ClmCrfIn1.SRC_SYS_CD = \"MEDTRAK\" Or ClmCrfIn1.SRC_SYS_CD = \"BCBSSC\" Or ClmCrfIn1.SRC_SYS_CD = \"BCA\" Or ClmCrfIn1.SRC_SYS_CD = \"BCBSA\" Or ClmCrfIn1.SRC_SYS_CD = \"SAVRX\" Or ClmCrfIn1.SRC_SYS_CD = \"LDI\" Or ClmCrfIn1.SRC_SYS_CD = \"CVS\" Or ClmCrfIn1.SRC_SYS_CD = \"LVNGHLTH\" Or ClmCrfIn1.SRC_SYS_CD = \"SOLUTRAN\" Or ClmCrfIn1.SRC_SYS_CD[1, 9] = \"NATIONBFT\") then \"SERVICE PROVIDER TYPE\" else \"NA\""
              initValue: "1"
            - name: SvcProvTypCdSk
              type: decimal
              width: 0
              map: " If SvcProvDomain = 'NA' then 1 Else  If Trim(ClmCrfIn1.CLM_SVC_PROV_TYP_CD) = 'UNK' then 0 Else  If Trim(ClmCrfIn1.CLM_SVC_PROV_TYP_CD) = 'NA' then 1 else  if SvcProvDomain = 'SERVICE PROVIDER TYPE' then GetFkeyProvTyp(\"FACETS\", ClmCrfIn1.CLM_SK, ClmCrfIn1.CLM_SVC_PROV_TYP_CD, Logging) else GetFkeyFcltyTyp(\"FACETS\", ClmCrfIn1.CLM_SK, ClmCrfIn1.CLM_SVC_PROV_TYP_CD, Logging)"
              initValue: "1"
            - name: SvcProvSpecCdSk
              type: decimal
              width: 0
              map: " If ClmCrfIn1.SRC_SYS_CD = 'BCBSSC' and Trim(ClmCrfIn1.CLM_SVC_PROV_SPEC_CD) = 'UNK' Then 0 Else  If ClmCrfIn1.SRC_SYS_CD = 'BCBSSC' and Trim(ClmCrfIn1.CLM_SVC_PROV_SPEC_CD) = 'NA' Then 1 Else  If ClmCrfIn1.SRC_SYS_CD = 'BCBSSC' Then GetFkeyProvSpec(ClmCrfIn1.SRC_SYS_CD, ClmCrfIn1.CLM_SK, ClmCrfIn1.CLM_SVC_PROV_SPEC_CD, Logging) Else  If (Upcase(ClmCrfIn1.CLM_SUBTYP_CD) = 'I' OR Upcase(ClmCrfIn1.CLM_SUBTYP_CD) = 'O' OR Upcase(ClmCrfIn1.CLM_SUBTYP_CD) = 'IP' OR Upcase(ClmCrfIn1.CLM_SUBTYP_CD) = 'OP') Then 1 Else  If ClmCrfIn1.SRC_SYS_CD = 'LVNGHLTH' Then GetFkeyProvSpec(\"FACETS\", ClmCrfIn1.CLM_SK, ClmCrfIn1.CLM_SVC_PROV_SPEC_CD, Logging) Else  If svSBVSrcSysCd = 'SBV' Then 1 Else GetFkeyProvSpec(ClmCrfIn1.SRC_SYS_CD, ClmCrfIn1.CLM_SK, ClmCrfIn1.CLM_SVC_PROV_SPEC_CD, Logging)"
              initValue: "1"
            - name: SubmtBcbsPlnSk
              type: decimal
              width: 0
              map: " If (ClmCrfIn1.SRC_SYS_CD = 'BCA' Or svCdMpngSrcSysCd = 'BCA' Or svEMRSrcSysCd = 'EMR') Then GetFkeyCodes('FACETS', ClmCrfIn1.CLM_SK, \"SUBMIT BCBS PLAN\", ClmCrfIn1.CLM_SUBMT_BCBS_PLN_CD, Logging) Else GetFkeyCodes(svCdMpngSrcSysCd, ClmCrfIn1.CLM_SK, \"SUBMIT BCBS PLAN\", ClmCrfIn1.CLM_SUBMT_BCBS_PLN_CD, Logging)"
              initValue: "1"
            - name: SubBcbsPlnSk
              type: decimal
              width: 0
              map: " If (ClmCrfIn1.SRC_SYS_CD = 'BCA' Or svCdMpngSrcSysCd = 'BCA' Or svEMRSrcSysCd = 'EMR') Then GetFkeyCodes('FACETS', ClmCrfIn1.CLM_SK, \"SUBSCRIBER BCBS PLAN\", ClmCrfIn1.CLM_SUB_BCBS_PLN_CD, Logging) Else GetFkeyCodes(svCdMpngSrcSysCd, ClmCrfIn1.CLM_SK, \"SUBSCRIBER BCBS PLAN\", ClmCrfIn1.CLM_SUB_BCBS_PLN_CD, Logging)"
              initValue: "1"
            - name: AcdntDtSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyDate('IDS', ClmCrfIn1.CLM_SK, ClmCrfIn1.ACDNT_DT, Logging)"
              initValue: "0"
            - name: InputDtSk
              type: decimal
              width: 0
              map: "GetFkeyDate('IDS', ClmCrfIn1.CLM_SK, ClmCrfIn1.INPT_DT, Logging)"
              initValue: "0"
            - name: MbrPlnEligDtSk
              type: decimal
              width: 0
              map: "GetFkeyDate('IDS', ClmCrfIn1.CLM_SK, ClmCrfIn1.MBR_PLN_ELIG_DT, Logging)"
              initValue: "0"
            - name: NextRvwDtSk
              type: decimal
              width: 0
              map: "GetFkeyDate('IDS', ClmCrfIn1.CLM_SK, ClmCrfIn1.NEXT_RVW_DT, Logging)"
              initValue: "0"
            - name: PdDtSk
              type: decimal
              width: 0
              map: "GetFkeyDate('IDS', ClmCrfIn1.CLM_SK, ClmCrfIn1.PD_DT, Logging)"
              initValue: "-1"
            - name: PaymtDragDtSk
              type: decimal
              width: 0
              map: "GetFkeyDate('IDS', ClmCrfIn1.CLM_SK, ClmCrfIn1.PAYMT_DRAG_CYC_DT, Logging)"
              initValue: "0"
            - name: PrcsDtSk
              type: decimal
              width: 0
              map: "GetFkeyDate('IDS', ClmCrfIn1.CLM_SK, ClmCrfIn1.PRCS_DT, Logging)"
              initValue: "0"
            - name: RcvdDtSk
              type: decimal
              width: 0
              map: "GetFkeyDate('IDS', ClmCrfIn1.CLM_SK, ClmCrfIn1.RCVD_DT, Logging)"
              initValue: "0"
            - name: SvcStrtDtSk
              type: decimal
              width: 0
              map: "GetFkeyDate('IDS', ClmCrfIn1.CLM_SK, ClmCrfIn1.SVC_STRT_DT, Logging)"
              initValue: "-1"
            - name: SvcEndDtSk
              type: decimal
              width: 0
              map: "GetFkeyDate('IDS', ClmCrfIn1.CLM_SK, ClmCrfIn1.SVC_END_DT, Logging)"
              initValue: "0"
            - name: SttusDtSk
              type: decimal
              width: 0
              map: "GetFkeyDate('IDS', ClmCrfIn1.CLM_SK, ClmCrfIn1.STTUS_DT, Logging)"
              initValue: "-1"
            - name: WorkUnblBegDtSk
              type: decimal
              width: 0
              map: "GetFkeyDate('IDS', ClmCrfIn1.CLM_SK, ClmCrfIn1.WORK_UNABLE_BEG_DT, Logging)"
              initValue: "-1"
            - name: WorkUnblEndDtSk
              type: decimal
              width: 0
              map: "GetFkeyDate('IDS', ClmCrfIn1.CLM_SK, ClmCrfIn1.WORK_UNABLE_END_DT, Logging)"
              initValue: "-1"
            - name: NonParPfxSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(svCdMpngSrcSysCd, ClmCrfIn1.CLM_SK, \"CLAIM NON PARTICIPATING PROVIDER PREFIX\", ClmCrfIn1.CLM_NONPAR_PROV_PFX_CD, Logging)"
              initValue: "-1"
            - name: PlnAlphPfxSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyAlphaPfx('FACETS', ClmCrfIn1.CLM_SK, ClmCrfIn1.ALPHA_PFX_CD, Logging)"
              initValue: "0"
            - name: SmlrIlnsDtSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyDate('IDS', ClmCrfIn1.CLM_SK, ClmCrfIn1.SMLR_ILNS_DT, Logging)"
              initValue: "-1"
            - name: PassThru
              type: decimal
              width: 0
              map: "ClmCrfIn1.PASS_THRU_IN"
              initValue: "'Y'"
            - name: FinancialLOB
              type: decimal
              width: 0
              map: "GetFkeyFnclLob(\"PSI\", ClmCrfIn1.CLM_SK, ClmCrfIn1.FNCL_LOB_NO, Logging)"
              initValue: "-1"
            - name: AcdntCd
              type: decimal
              width: 0
              map: "GetFkeyCodes(svCdMpngSrcSysCd, ClmCrfIn1.CLM_SK, \"CLAIM ACCIDENT\", ClmCrfIn1.CLM_ACDNT_CD, Logging)"
              initValue: "1"
            - name: AdjustedFacetsClaimStatus91
              type: decimal
              width: 0
              map: "GetFkeyCodes(\"FACETS\", ClmCrfIn1.CLM_SK, \"CLAIM STATUS\", \"R\", Logging)"
              initValue: "0"
            - name: svPcaTypCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(svCdMpngSrcSysCd, ClmCrfIn1.CLM_SK, \"PERSONAL CARE ACCOUNT PROCESSING\", ClmCrfIn1.PCA_TYP_CD, Logging)"
              initValue: "-1"
            - name: svRelBaseClmSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyClm(ClmCrfIn1.SRC_SYS_CD, ClmCrfIn1.CLM_SK, trim(ClmCrfIn1.CLCL_MICRO_ID), Logging)"
              initValue: "-1"
            - name: svRelPcaClmSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyClm(ClmCrfIn1.SRC_SYS_CD, ClmCrfIn1.CLM_SK, trim(ClmCrfIn1.REL_PCA_CLM_ID), Logging)"
              initValue: "-1"
            - name: svClmSubmtICDVrsnCdSK
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(svCdMpngSrcSysCd, ClmCrfIn1.CLM_SK, \"DIAGNOSIS CODE TYPE\", ClmCrfIn1.CLM_SUBMT_ICD_VRSN_CD, Logging)"
              initValue: "0"
            - name: ErrCount
              type: decimal
              width: 0
              map: "GetFkeyErrorCnt(ClmCrfIn1.CLM_SK)"
              initValue: "-1"
          option:
            joinType: left
          sources: 
            - name: ClmCrf
              alias: ClmCrfIn1
              joinFilter: ""
              joinExpression: []
            - name: nasco_dup
              alias: nasco_dup_lkup1
              joinFilter: ""
              joinExpression:
              - fromExpr: "nasco_dup_lkup1.CLM_ID"
                toExpr: "ClmCrfIn1.CLM_ID[1,12]"
                caseInSensitive: true
            - name: IDS_CLM
              alias: Exist_Ids
              joinFilter: ""
              joinExpression:
              - fromExpr: "Exist_Ids.CLM_ID"
                toExpr: "ClmCrfIn1.ADJ_FROM_CLM_ID"
                caseInSensitive: true
          joinCondition: 
            - expression: "nasco_dup_lkup1.CLM_ID = ClmCrfIn1.CLM_ID[1,12]"
              caseInSensitive: true
            - expression: "Exist_Ids.CLM_ID = ClmCrfIn1.ADJ_FROM_CLM_ID"
              caseInSensitive: true
          schema: 
            - name: ClmFkeyOut1_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "ClmCrfIn1.CLM_SK"
            - name: ClmFkeyOut1_SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "ClmCrfIn1.SRC_SYS_CD_SK"
            - name: ClmFkeyOut1_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "ClmCrfIn1.CLM_ID"
            - name: ClmFkeyOut1_CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "ClmCrfIn1.CRT_RUN_CYC_EXCTN_SK"
            - name: ClmFkeyOut1_LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: RunCycle
            - name: ClmFkeyOut1_ADJ_FROM_CLM_SK
              type: int
              width: 4
              nullable: N
              map: ClmAdjFromClmSk
            - name: ClmFkeyOut1_ADJ_TO_CLM_SK
              type: int
              width: 4
              nullable: N
              map: ClmAdjToClmCdSk
            - name: ClmFkeyOut1_ALPHA_PFX_SK
              type: int
              width: 4
              nullable: N
              map: PlnAlphPfxSk
            - name: ClmFkeyOut1_CLM_EOB_EXCD_SK
              type: int
              width: 4
              nullable: N
              map: ClmEOBExcdSk
            - name: ClmFkeyOut1_CLS_SK
              type: int
              width: 4
              nullable: N
              map: ClsSk
            - name: ClmFkeyOut1_CLS_PLN_SK
              type: int
              width: 4
              nullable: N
              map: ClsPlnSk
            - name: ClmFkeyOut1_EXPRNC_CAT_SK
              type: int
              width: 4
              nullable: N
              map: ExpCatCdSk
            - name: ClmFkeyOut1_FNCL_LOB_SK
              type: int
              width: 4
              nullable: N
              map: FinancialLOB
            - name: ClmFkeyOut1_GRP_SK
              type: int
              width: 4
              nullable: N
              map: GrpSk
            - name: ClmFkeyOut1_MBR_SK
              type: int
              width: 4
              nullable: N
              map: MbrSk
            - name: ClmFkeyOut1_NTWK_SK
              type: int
              width: 4
              nullable: N
              map: NtwkSk
            - name: ClmFkeyOut1_PROD_SK
              type: int
              width: 4
              nullable: N
              map: ProdSk
            - name: ClmFkeyOut1_SUBGRP_SK
              type: int
              width: 4
              nullable: N
              map: SubGrpSk
            - name: ClmFkeyOut1_SUB_SK
              type: int
              width: 4
              nullable: N
              map: SubSk
            - name: ClmFkeyOut1_CLM_ACDNT_CD_SK
              type: int
              width: 4
              nullable: N
              map: AcdntCd
            - name: ClmFkeyOut1_CLM_ACDNT_ST_CD_SK
              type: int
              width: 4
              nullable: N
              map: ClmAcdtStCdSk
            - name: ClmFkeyOut1_CLM_ACTV_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
              map: ActBcbsSk
            - name: ClmFkeyOut1_CLM_AGMNT_SRC_CD_SK
              type: int
              width: 4
              nullable: N
              map: ClmAgmntSrcCdSk
            - name: ClmFkeyOut1_CLM_BTCH_ACTN_CD_SK
              type: int
              width: 4
              nullable: N
              map: ClmBtchActnCdSk
            - name: ClmFkeyOut1_CLM_CAP_CD_SK
              type: int
              width: 4
              nullable: N
              map: CapClmCdSk
            - name: ClmFkeyOut1_CLM_CAT_CD_SK
              type: int
              width: 4
              nullable: N
              map: ClmCatCdSk
            - name: ClmFkeyOut1_CLM_CHK_CYC_OVRD_CD_SK
              type: int
              width: 4
              nullable: N
              map: ClmChkCycOvdSk
            - name: ClmFkeyOut1_CLM_COB_CD_SK
              type: int
              width: 4
              nullable: N
              map: ClmCobCdSk
            - name: ClmFkeyOut1_CLM_FINL_DISP_CD_SK
              type: int
              width: 4
              nullable: N
              map: ClmFinlDispCdSk
            - name: ClmFkeyOut1_CLM_INPT_METH_CD_SK
              type: int
              width: 4
              nullable: N
              map: ClmInptMethSk
            - name: ClmFkeyOut1_CLM_INPT_SRC_CD_SK
              type: int
              width: 4
              nullable: N
              map: ClmInptSrcSk
            - name: ClmFkeyOut1_CLM_INTER_PLN_PGM_CD_SK
              type: int
              width: 4
              nullable: N
              map: ClmIppCdSk
            - name: ClmFkeyOut1_CLM_NTWK_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: ClmInNtwkSk
            - name: ClmFkeyOut1_CLM_NONPAR_PROV_PFX_CD_SK
              type: int
              width: 4
              nullable: N
              map: NonParPfxSk
            - name: ClmFkeyOut1_CLM_OTHR_BNF_CD_SK
              type: int
              width: 4
              nullable: N
              map: ClmOtherBnfSk
            - name: ClmFkeyOut1_CLM_PAYE_CD_SK
              type: int
              width: 4
              nullable: N
              map: ClmPayeCdSk
            - name: ClmFkeyOut1_CLM_PRCS_CTL_AGNT_PFX_CD_SK
              type: int
              width: 4
              nullable: N
              map: ClmPrcsCtlAgntPfxSk
            - name: ClmFkeyOut1_CLM_SVC_DEFN_PFX_CD_SK
              type: int
              width: 4
              nullable: N
              map: ClmSvcDefnPfxCdSk
            - name: ClmFkeyOut1_CLM_SVC_PROV_SPEC_CD_SK
              type: int
              width: 4
              nullable: N
              map: SvcProvSpecCdSk
            - name: ClmFkeyOut1_CLM_SVC_PROV_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: SvcProvTypCdSk
            - name: ClmFkeyOut1_CLM_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: ClmSttusCdSk
            - name: ClmFkeyOut1_CLM_SUBMTTING_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
              map: SubmtBcbsPlnSk
            - name: ClmFkeyOut1_CLM_SUB_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
              map: SubBcbsPlnSk
            - name: ClmFkeyOut1_CLM_SUBTYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: ClmSubTypCdSk
            - name: ClmFkeyOut1_CLM_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: ClmTypCdSk
            - name: ClmFkeyOut1_ATCHMT_IN
              type: char
              width: 1
              nullable: N
              map: "ClmCrfIn1.ATCHMT_IN"
            - name: ClmFkeyOut1_CLNCL_EDIT_IN
              type: char
              width: 1
              nullable: N
              map: "ClmCrfIn1.CLM_CLNCL_EDIT_CD"
            - name: ClmFkeyOut1_COBRA_CLM_IN
              type: char
              width: 1
              nullable: N
              map: "ClmCrfIn1.COBRA_CLM_IN"
            - name: ClmFkeyOut1_FIRST_PASS_IN
              type: char
              width: 1
              nullable: N
              map: "ClmCrfIn1.FIRST_PASS_IN"
            - name: ClmFkeyOut1_HOST_IN
              type: char
              width: 1
              nullable: N
              map: "ClmCrfIn1.HOST_IN"
            - name: ClmFkeyOut1_LTR_IN
              type: char
              width: 1
              nullable: N
              map: "ClmCrfIn1.LTR_IN"
            - name: ClmFkeyOut1_MCARE_ASG_IN
              type: char
              width: 1
              nullable: N
              map: "ClmCrfIn1.MCARE_ASG_IN"
            - name: ClmFkeyOut1_NOTE_IN
              type: char
              width: 1
              nullable: N
              map: "ClmCrfIn1.NOTE_IN"
            - name: ClmFkeyOut1_PCA_AUDIT_IN
              type: char
              width: 1
              nullable: N
              map: "ClmCrfIn1.PCA_AUDIT_IN"
            - name: ClmFkeyOut1_PCP_SUBMT_IN
              type: char
              width: 1
              nullable: N
              map: "ClmCrfIn1.PCP_SUBMT_IN"
            - name: ClmFkeyOut1_PROD_OOA_IN
              type: char
              width: 1
              nullable: N
              map: "ClmCrfIn1.PROD_OOA_IN"
            - name: ClmFkeyOut1_ACDNT_DT_SK
              type: char
              width: 10
              nullable: N
              map: AcdntDtSk
            - name: ClmFkeyOut1_INPT_DT_SK
              type: char
              width: 10
              nullable: N
              map: InputDtSk
            - name: ClmFkeyOut1_MBR_PLN_ELIG_DT_SK
              type: char
              width: 10
              nullable: N
              map: MbrPlnEligDtSk
            - name: ClmFkeyOut1_NEXT_RVW_DT_SK
              type: char
              width: 10
              nullable: N
              map: NextRvwDtSk
            - name: ClmFkeyOut1_PD_DT_SK
              type: char
              width: 10
              nullable: N
              map: PdDtSk
            - name: ClmFkeyOut1_PAYMT_DRAG_CYC_DT_SK
              type: char
              width: 10
              nullable: N
              map: PaymtDragDtSk
            - name: ClmFkeyOut1_PRCS_DT_SK
              type: char
              width: 10
              nullable: N
              map: PrcsDtSk
            - name: ClmFkeyOut1_RCVD_DT_SK
              type: char
              width: 10
              nullable: N
              map: RcvdDtSk
            - name: ClmFkeyOut1_SVC_STRT_DT_SK
              type: char
              width: 10
              nullable: N
              map: SvcStrtDtSk
            - name: ClmFkeyOut1_SVC_END_DT_SK
              type: char
              width: 10
              nullable: N
              map: " If ClmCrfIn1.SRC_SYS_CD = 'BCA' Then  if SvcEndDtSk = 'UNK' Then '2199-12-31' Else SvcEndDtSk Else SvcEndDtSk"
            - name: ClmFkeyOut1_SMLR_ILNS_DT_SK
              type: char
              width: 10
              nullable: N
              map: SmlrIlnsDtSk
            - name: ClmFkeyOut1_STTUS_DT_SK
              type: char
              width: 10
              nullable: N
              map: SttusDtSk
            - name: ClmFkeyOut1_WORK_UNABLE_BEG_DT_SK
              type: char
              width: 10
              nullable: N
              map: WorkUnblBegDtSk
            - name: ClmFkeyOut1_WORK_UNABLE_END_DT_SK
              type: char
              width: 10
              nullable: N
              map: WorkUnblEndDtSk
            - name: ClmFkeyOut1_ACDNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmCrfIn1.ACDNT_AMT"
            - name: ClmFkeyOut1_ACTL_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmCrfIn1.ACTL_PD_AMT"
            - name: ClmFkeyOut1_ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmCrfIn1.ALLOW_AMT"
            - name: ClmFkeyOut1_DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmCrfIn1.DSALW_AMT"
            - name: ClmFkeyOut1_COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmCrfIn1.COINS_AMT"
            - name: ClmFkeyOut1_CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmCrfIn1.CNSD_CHRG_AMT"
            - name: ClmFkeyOut1_COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmCrfIn1.COPAY_AMT"
            - name: ClmFkeyOut1_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmCrfIn1.CHRG_AMT"
            - name: ClmFkeyOut1_DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmCrfIn1.DEDCT_AMT"
            - name: ClmFkeyOut1_PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmCrfIn1.PAYBL_AMT"
            - name: ClmFkeyOut1_CLM_CT
              type: smallint
              width: 2
              nullable: N
              map: "ClmCrfIn1.CLM_CT"
            - name: ClmFkeyOut1_MBR_AGE
              type: decimal
              width: 3
              map: "ClmCrfIn1.MBR_AGE"
            - name: ClmFkeyOut1_ADJ_FROM_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: " If ClmCrfIn1.SRC_SYS_CD = 'OPTUMRX' then ClmCrfIn1.ADJ_FROM_CLM_ID Else  If NascoDupClm = 'N' then ClmCrfIn1.ADJ_FROM_CLM_ID else nasco_dup_lkup1.NASCO_CLM_ID"
            - name: ClmFkeyOut1_ADJ_TO_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "ClmCrfIn1.ADJ_TO_CLM_ID"
            - name: ClmFkeyOut1_DOC_TX_ID
              type: char
              width: 18
              nullable: N
              map: "ClmCrfIn1.DOC_TX_ID"
            - name: ClmFkeyOut1_MCAID_RESUB_NO
              type: char
              width: 15
              map: "ClmCrfIn1.MCAID_RESUB_NO"
            - name: ClmFkeyOut1_MCARE_ID
              type: char
              width: 12
              nullable: N
              map: "ClmCrfIn1.MCARE_ID"
            - name: ClmFkeyOut1_MBR_SFX_NO
              type: char
              width: 2
              map: "ClmCrfIn1.MBR_SFX_NO"
            - name: ClmFkeyOut1_PATN_ACCT_NO
              type: varchar
              width: 35
              map: "ClmCrfIn1.PATN_ACCT_NO"
            - name: ClmFkeyOut1_PAYMT_REF_ID
              type: char
              width: 16
              nullable: N
              map: "ClmCrfIn1.PAYMT_REF_ID"
            - name: ClmFkeyOut1_PROV_AGMNT_ID
              type: char
              width: 12
              nullable: N
              map: "ClmCrfIn1.PROV_AGMNT_ID"
            - name: ClmFkeyOut1_RFRNG_PROV_TX
              type: varchar
              width: 70
              map: "ClmCrfIn1.RFRNG_PROV_TX"
            - name: ClmFkeyOut1_SUB_ID
              type: char
              width: 14
              nullable: N
              map: "ClmCrfIn1.SUB_ID"
            - name: ClmFkeyOut1_PCA_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: svPcaTypCdSk
            - name: ClmFkeyOut1_REL_PCA_CLM_SK
              type: int
              width: 4
              nullable: N
              map: svRelPcaClmSk
            - name: ClmFkeyOut1_REL_BASE_CLM_SK
              type: int
              width: 4
              nullable: N
              map: svRelBaseClmSk
            - name: ClmFkeyOut1_CLM_UPDT_SW
              type: char
              width: 1
              nullable: N
              map: "ClmCrfIn1.CLM_UPDT_SW"
            - name: ClmFkeyOut1_CLCL_MICRO_ID
              type: char
              width: 18
              nullable: N
              map: "ClmCrfIn1.CLCL_MICRO_ID"
            - name: ClmFkeyOut1_REMIT_SUPRSION_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmCrfIn1.REMIT_SUPRSION_AMT"
            - name: ClmFkeyOut1_MCAID_STTUS_ID
              type: varchar
              width: 20
              nullable: N
              map: "ClmCrfIn1.MCAID_STTUS_ID"
            - name: ClmFkeyOut1_PATN_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmCrfIn1.PATN_PD_AMT"
            - name: ClmFkeyOut1_CLM_SUBMT_ICD_VRSN_CD_SK
              type: int
              width: 4
              nullable: N
              map: svClmSubmtICDVrsnCdSK
            - name: ClmFkeyOut1_CLM_TXNMY_CD
              type: varchar
              width: 20
              map: "ClmCrfIn1.CLM_TXNMY_CD"
            - name: ClmFkeyOut1_BILL_PAYMT_EXCL_IN
              type: char
              width: 1
              nullable: N
              map: "ClmCrfIn1.BILL_PAYMT_EXCL_IN"
            - name: ClmFkeyOut1_ADJ_FROM_CLM
              type: char
              width: 12
              nullable: N
              map: "ClmCrfIn1.ADJ_FROM_CLM"
            - name: ClmFkeyOut1_ADJ_TO_CLM
              type: char
              width: 12
              nullable: N
              map: "ClmCrfIn1.ADJ_TO_CLM"
            - name: DefaultUNK_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "'UNK'"
            - name: DefaultUNK_CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_ADJ_FROM_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_ADJ_TO_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_ALPHA_PFX_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_CLM_EOB_EXCD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_CLS_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_CLS_PLN_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_EXPRNC_CAT_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_FNCL_LOB_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_GRP_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_MBR_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_NTWK_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_PROD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_SUBGRP_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_SUB_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_CLM_ACDNT_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_CLM_ACDNT_ST_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_CLM_ACTV_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_CLM_AGMNT_SRC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_CLM_BTCH_ACTN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_CLM_CAP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_CLM_CAT_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_CLM_CHK_CYC_OVRD_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_CLM_COB_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_CLM_FINL_DISP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_CLM_INPT_METH_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_CLM_INPT_SRC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_CLM_INTER_PLN_PGM_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_CLM_NTWK_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_CLM_NONPAR_PROV_PFX_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_CLM_OTHR_BNF_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_CLM_PAYE_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_CLM_PRCS_CTL_AGNT_PFX_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_CLM_SVC_DEFN_PFX_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_CLM_SVC_PROV_SPEC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_CLM_SVC_PROV_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_CLM_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_CLM_SUBMTTING_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_CLM_SUB_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_CLM_SUBTYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_CLM_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_ATCHMT_IN
              type: char
              width: 1
              nullable: N
              map: "'U'"
            - name: DefaultUNK_CLNCL_EDIT_IN
              type: char
              width: 1
              nullable: N
              map: "'U'"
            - name: DefaultUNK_COBRA_CLM_IN
              type: char
              width: 1
              nullable: N
              map: "'U'"
            - name: DefaultUNK_FIRST_PASS_IN
              type: char
              width: 1
              nullable: N
              map: "'U'"
            - name: DefaultUNK_HOST_IN
              type: char
              width: 1
              nullable: N
              map: "'U'"
            - name: DefaultUNK_LTR_IN
              type: char
              width: 1
              nullable: N
              map: "'U'"
            - name: DefaultUNK_MCARE_ASG_IN
              type: char
              width: 1
              nullable: N
              map: "'U'"
            - name: DefaultUNK_NOTE_IN
              type: char
              width: 1
              nullable: N
              map: "'U'"
            - name: DefaultUNK_PCA_AUDIT_IN
              type: char
              width: 1
              nullable: N
              map: "'U'"
            - name: DefaultUNK_PCP_SUBMT_IN
              type: char
              width: 1
              nullable: N
              map: "'U'"
            - name: DefaultUNK_PROD_OOA_IN
              type: char
              width: 1
              nullable: N
              map: "'U'"
            - name: DefaultUNK_ACDNT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "'NA'"
            - name: DefaultUNK_INPT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "'NA'"
            - name: DefaultUNK_MBR_PLN_ELIG_DT_SK
              type: char
              width: 10
              nullable: N
              map: "'NA'"
            - name: DefaultUNK_NEXT_RVW_DT_SK
              type: char
              width: 10
              nullable: N
              map: "'NA'"
            - name: DefaultUNK_PD_DT_SK
              type: char
              width: 10
              nullable: N
              map: "'NA'"
            - name: DefaultUNK_PAYMT_DRAG_CYC_DT_SK
              type: char
              width: 10
              nullable: N
              map: "'NA'"
            - name: DefaultUNK_PRCS_DT_SK
              type: char
              width: 10
              nullable: N
              map: "'NA'"
            - name: DefaultUNK_RCVD_DT_SK
              type: char
              width: 10
              nullable: N
              map: "'NA'"
            - name: DefaultUNK_SVC_STRT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "'NA'"
            - name: DefaultUNK_SVC_END_DT_SK
              type: char
              width: 10
              nullable: N
              map: "'NA'"
            - name: DefaultUNK_SMLR_ILNS_DT_SK
              type: char
              width: 10
              nullable: N
              map: "'NA'"
            - name: DefaultUNK_STTUS_DT_SK
              type: char
              width: 10
              nullable: N
              map: "'NA'"
            - name: DefaultUNK_WORK_UNABLE_BEG_DT_SK
              type: char
              width: 10
              nullable: N
              map: "'NA'"
            - name: DefaultUNK_WORK_UNABLE_END_DT_SK
              type: char
              width: 10
              nullable: N
              map: "'NA'"
            - name: DefaultUNK_ACDNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: DefaultUNK_ACTL_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: DefaultUNK_ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: DefaultUNK_DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: DefaultUNK_COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: DefaultUNK_CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: DefaultUNK_COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: DefaultUNK_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: DefaultUNK_DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: DefaultUNK_PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: DefaultUNK_CLM_CT
              type: smallint
              width: 2
              nullable: N
              map: "0"
            - name: DefaultUNK_MBR_AGE
              type: decimal
              width: 3
              map: "0"
            - name: DefaultUNK_ADJ_FROM_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "'UNK'"
            - name: DefaultUNK_ADJ_TO_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "'UNK'"
            - name: DefaultUNK_DOC_TX_ID
              type: char
              width: 18
              nullable: N
              map: "'UNK'"
            - name: DefaultUNK_MCAID_RESUB_NO
              type: char
              width: 15
              map: "'UNK'"
            - name: DefaultUNK_MCARE_ID
              type: char
              width: 12
              nullable: N
              map: "'UNK'"
            - name: DefaultUNK_MBR_SFX_NO
              type: char
              width: 2
              map: "\"UN\""
            - name: DefaultUNK_PATN_ACCT_NO
              type: varchar
              width: 35
              map: "'UNK'"
            - name: DefaultUNK_PAYMT_REF_ID
              type: char
              width: 16
              nullable: N
              map: "'UNK'"
            - name: DefaultUNK_PROV_AGMNT_ID
              type: char
              width: 12
              nullable: N
              map: "'UNK'"
            - name: DefaultUNK_RFRNG_PROV_TX
              type: varchar
              width: 70
              map: "'UNK'"
            - name: DefaultUNK_SUB_ID
              type: char
              width: 14
              nullable: N
              map: "'UNK'"
            - name: DefaultUNK_PCA_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_REL_PCA_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_REL_BASE_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_CLM_UPDT_SW
              type: char
              width: 1
              nullable: N
              map: "'N'"
            - name: DefaultUNK_CLCL_MICRO_ID
              type: char
              width: 18
              nullable: N
              map: "'UNK'"
            - name: DefaultUNK_REMIT_SUPRSION_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: DefaultUNK_MCAID_STTUS_ID
              type: varchar
              width: 20
              nullable: N
              map: "'UNK'"
            - name: DefaultUNK_PATN_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: DefaultUNK_CLM_SUBMT_ICD_VRSN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DefaultUNK_CLM_TXNMY_CD
              type: varchar
              width: 20
              map: "'UNK'"
            - name: DefaultUNK_BILL_PAYMT_EXCL_IN
              type: char
              width: 1
              nullable: N
              map: "'N'"
            - name: DefaultUNK_ADJ_FROM_CLM
              type: char
              width: 12
              nullable: N
              map: "'UNK'"
            - name: DefaultUNK_ADJ_TO_CLM
              type: char
              width: 12
              nullable: N
              map: "'UNK'"
            - name: DefaultNA_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "'NA'"
            - name: DefaultNA_CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_ADJ_FROM_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_ADJ_TO_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_ALPHA_PFX_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_CLM_EOB_EXCD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_CLS_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_CLS_PLN_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_EXPRNC_CAT_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_FNCL_LOB_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_GRP_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_MBR_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_NTWK_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_PROD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_SUBGRP_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_SUB_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_CLM_ACDNT_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_CLM_ACDNT_ST_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_CLM_ACTV_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_CLM_AGMNT_SRC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_CLM_BTCH_ACTN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_CLM_CAP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_CLM_CAT_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_CLM_CHK_CYC_OVRD_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_CLM_COB_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_CLM_FINL_DISP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_CLM_INPT_METH_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_CLM_INPT_SRC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_CLM_INTER_PLN_PGM_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_CLM_NTWK_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_CLM_NONPAR_PROV_PFX_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_CLM_OTHR_BNF_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_CLM_PAYE_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_CLM_PRCS_CTL_AGNT_PFX_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_CLM_SVC_DEFN_PFX_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_CLM_SVC_PROV_SPEC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_CLM_SVC_PROV_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_CLM_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_CLM_SUBMTTING_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_CLM_SUB_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_CLM_SUBTYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_CLM_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_ATCHMT_IN
              type: char
              width: 1
              nullable: N
              map: "'X'"
            - name: DefaultNA_CLNCL_EDIT_IN
              type: char
              width: 1
              nullable: N
              map: "'X'"
            - name: DefaultNA_COBRA_CLM_IN
              type: char
              width: 1
              nullable: N
              map: "'X'"
            - name: DefaultNA_FIRST_PASS_IN
              type: char
              width: 1
              nullable: N
              map: "'X'"
            - name: DefaultNA_HOST_IN
              type: char
              width: 1
              nullable: N
              map: "'X'"
            - name: DefaultNA_LTR_IN
              type: char
              width: 1
              nullable: N
              map: "'X'"
            - name: DefaultNA_MCARE_ASG_IN
              type: char
              width: 1
              nullable: N
              map: "'X'"
            - name: DefaultNA_NOTE_IN
              type: char
              width: 1
              nullable: N
              map: "'X'"
            - name: DefaultNA_PCA_AUDIT_IN
              type: char
              width: 1
              nullable: N
              map: "'X'"
            - name: DefaultNA_PCP_SUBMT_IN
              type: char
              width: 1
              nullable: N
              map: "'X'"
            - name: DefaultNA_PROD_OOA_IN
              type: char
              width: 1
              nullable: N
              map: "'X'"
            - name: DefaultNA_ACDNT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "'NA'"
            - name: DefaultNA_INPT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "'NA'"
            - name: DefaultNA_MBR_PLN_ELIG_DT_SK
              type: char
              width: 10
              nullable: N
              map: "'NA'"
            - name: DefaultNA_NEXT_RVW_DT_SK
              type: char
              width: 10
              nullable: N
              map: "'NA'"
            - name: DefaultNA_PD_DT_SK
              type: char
              width: 10
              nullable: N
              map: "'NA'"
            - name: DefaultNA_PAYMT_DRAG_CYC_DT_SK
              type: char
              width: 10
              nullable: N
              map: "'NA'"
            - name: DefaultNA_PRCS_DT_SK
              type: char
              width: 10
              nullable: N
              map: "'NA'"
            - name: DefaultNA_RCVD_DT_SK
              type: char
              width: 10
              nullable: N
              map: "'NA'"
            - name: DefaultNA_SVC_STRT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "'NA'"
            - name: DefaultNA_SVC_END_DT_SK
              type: char
              width: 10
              nullable: N
              map: "'NA'"
            - name: DefaultNA_SMLR_ILNS_DT_SK
              type: char
              width: 10
              nullable: N
              map: "'NA'"
            - name: DefaultNA_STTUS_DT_SK
              type: char
              width: 10
              nullable: N
              map: "'NA'"
            - name: DefaultNA_WORK_UNABLE_BEG_DT_SK
              type: char
              width: 10
              nullable: N
              map: "'NA'"
            - name: DefaultNA_WORK_UNABLE_END_DT_SK
              type: char
              width: 10
              nullable: N
              map: "'NA'"
            - name: DefaultNA_ACDNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: DefaultNA_ACTL_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: DefaultNA_ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: DefaultNA_DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: DefaultNA_COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: DefaultNA_CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: DefaultNA_COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: DefaultNA_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: DefaultNA_DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: DefaultNA_PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: DefaultNA_CLM_CT
              type: smallint
              width: 2
              nullable: N
              map: "0"
            - name: DefaultNA_MBR_AGE
              type: decimal
              width: 3
              map: "0"
            - name: DefaultNA_ADJ_FROM_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "'NA'"
            - name: DefaultNA_ADJ_TO_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "'NA'"
            - name: DefaultNA_DOC_TX_ID
              type: char
              width: 18
              nullable: N
              map: "'NA'"
            - name: DefaultNA_MCAID_RESUB_NO
              type: char
              width: 15
              map: "'NA'"
            - name: DefaultNA_MCARE_ID
              type: char
              width: 12
              nullable: N
              map: "'NA'"
            - name: DefaultNA_MBR_SFX_NO
              type: char
              width: 2
              map: "'NA'"
            - name: DefaultNA_PATN_ACCT_NO
              type: varchar
              width: 35
              map: "'NA'"
            - name: DefaultNA_PAYMT_REF_ID
              type: char
              width: 16
              nullable: N
              map: "'NA'"
            - name: DefaultNA_PROV_AGMNT_ID
              type: char
              width: 12
              nullable: N
              map: "'NA'"
            - name: DefaultNA_RFRNG_PROV_TX
              type: varchar
              width: 70
              map: "'NA'"
            - name: DefaultNA_SUB_ID
              type: char
              width: 14
              nullable: N
              map: "'NA'"
            - name: DefaultNA_PCA_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_REL_PCA_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_REL_BASE_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_CLM_UPDT_SW
              type: char
              width: 1
              nullable: N
              map: "'N'"
            - name: DefaultNA_CLCL_MICRO_ID
              type: char
              width: 18
              nullable: N
              map: "'NA'"
            - name: DefaultNA_REMIT_SUPRSION_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: DefaultNA_MCAID_STTUS_ID
              type: varchar
              width: 20
              nullable: N
              map: "'NA'"
            - name: DefaultNA_PATN_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: DefaultNA_CLM_SUBMT_ICD_VRSN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DefaultNA_CLM_TXNMY_CD
              type: varchar
              width: 20
              map: "'NA'"
            - name: DefaultNA_BILL_PAYMT_EXCL_IN
              type: char
              width: 1
              nullable: N
              map: "'N'"
            - name: DefaultNA_ADJ_FROM_CLM
              type: char
              width: 12
              nullable: N
              map: "'NA'"
            - name: DefaultNA_ADJ_TO_CLM
              type: char
              width: 12
              nullable: N
              map: "'NA'"
            - name: Recycle1_JOB_EXCTN_RCRD_ERR_SK
              type: int
              width: 4
              nullable: N
              map: " If ErrCount > 0 Then GetRecycleKey(ClmCrfIn1.CLM_SK) Else 0"
            - name: Recycle1_INSRT_UPDT_CD
              type: char
              width: 10
              nullable: N
              map: "ClmCrfIn1.INSRT_UPDT_CD"
            - name: Recycle1_DISCARD_IN
              type: char
              width: 1
              nullable: N
              map: "ClmCrfIn1.DISCARD_IN"
            - name: Recycle1_PASS_THRU_IN
              type: char
              width: 1
              nullable: N
              map: "ClmCrfIn1.PASS_THRU_IN"
            - name: Recycle1_FIRST_RECYC_DT
              type: timestamp
              nullable: N
              map: "ClmCrfIn1.FIRST_RECYC_DT"
            - name: Recycle1_ERR_CT
              type: smallint
              width: 2
              nullable: N
              map: ErrCount
            - name: Recycle1_RECYCLE_CT
              type: decimal
              width: 6
              nullable: N
              map: "ClmCrfIn1.RECYCLE_CT + 1"
            - name: Recycle1_SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmCrfIn1.SRC_SYS_CD"
            - name: Recycle1_PRI_KEY_STRING
              type: varchar
              width: 1023
              nullable: N
              map: "ClmCrfIn1.PRI_KEY_STRING"
            - name: Recycle1_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "ClmCrfIn1.CLM_SK"
            - name: Recycle1_SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "ClmCrfIn1.SRC_SYS_CD_SK"
            - name: Recycle1_CLM_ID
              type: char
              width: 18
              nullable: N
              map: "ClmCrfIn1.CLM_ID"
            - name: Recycle1_CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "ClmCrfIn1.CRT_RUN_CYC_EXCTN_SK"
            - name: Recycle1_LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: RunCycle
            - name: Recycle1_ADJ_FROM_CLM
              type: char
              width: 12
              nullable: N
              map: "ClmCrfIn1.ADJ_FROM_CLM"
            - name: Recycle1_ADJ_TO_CLM
              type: char
              width: 12
              nullable: N
              map: "ClmCrfIn1.ADJ_TO_CLM"
            - name: Recycle1_ALPHA_PFX_CD
              type: char
              width: 3
              map: "ClmCrfIn1.ALPHA_PFX_CD"
            - name: Recycle1_CLM_EOB_EXCD
              type: char
              width: 3
              nullable: N
              map: "ClmCrfIn1.CLM_EOB_EXCD"
            - name: Recycle1_CLS
              type: char
              width: 4
              nullable: N
              map: "ClmCrfIn1.CLS"
            - name: Recycle1_CLS_PLN
              type: char
              width: 8
              nullable: N
              map: "ClmCrfIn1.CLS_PLN"
            - name: Recycle1_EXPRNC_CAT
              type: char
              width: 4
              nullable: N
              map: "ClmCrfIn1.EXPRNC_CAT"
            - name: Recycle1_FNCL_LOB_NO
              type: char
              width: 4
              map: "ClmCrfIn1.FNCL_LOB_NO"
            - name: Recycle1_GRP
              type: char
              width: 8
              nullable: N
              map: "ClmCrfIn1.GRP"
            - name: Recycle1_MBR_CK
              type: int
              width: 4
              nullable: N
              map: "ClmCrfIn1.MBR_CK"
            - name: Recycle1_NTWK
              type: char
              width: 12
              nullable: N
              map: "ClmCrfIn1.NTWK"
            - name: Recycle1_PROD
              type: char
              width: 8
              nullable: N
              map: "ClmCrfIn1.PROD"
            - name: Recycle1_SUBGRP
              type: char
              width: 4
              nullable: N
              map: "ClmCrfIn1.SUBGRP"
            - name: Recycle1_SUB_CK
              type: int
              width: 4
              nullable: N
              map: "ClmCrfIn1.SUB_CK"
            - name: Recycle1_CLM_ACDNT_CD
              type: char
              width: 2
              nullable: N
              map: "ClmCrfIn1.CLM_ACDNT_CD"
            - name: Recycle1_CLM_ACDNT_ST_CD
              type: char
              width: 2
              nullable: N
              map: "ClmCrfIn1.CLM_ACDNT_ST_CD"
            - name: Recycle1_CLM_ACTIVATING_BCBS_PLN_CD
              type: char
              width: 10
              nullable: N
              map: "ClmCrfIn1.CLM_ACTIVATING_BCBS_PLN_CD"
            - name: Recycle1_CLM_AGMNT_SRC_CD
              type: char
              width: 10
              nullable: N
              map: "ClmCrfIn1.CLM_AGMNT_SRC_CD"
            - name: Recycle1_CLM_BTCH_ACTN_CD
              type: char
              width: 1
              nullable: N
              map: "ClmCrfIn1.CLM_BTCH_ACTN_CD"
            - name: Recycle1_CLM_CAP_CD
              type: char
              width: 10
              nullable: N
              map: "ClmCrfIn1.CLM_CAP_CD"
            - name: Recycle1_CLM_CAT_CD
              type: char
              width: 10
              nullable: N
              map: "ClmCrfIn1.CLM_CAT_CD"
            - name: Recycle1_CLM_CHK_CYC_OVRD_CD
              type: char
              width: 1
              nullable: N
              map: "ClmCrfIn1.CLM_CHK_CYC_OVRD_CD"
            - name: Recycle1_CLM_COB_CD
              type: char
              width: 1
              nullable: N
              map: "ClmCrfIn1.CLM_COB_CD"
            - name: Recycle1_FINL_DISP_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmCrfIn1.FINL_DISP_CD"
            - name: Recycle1_CLM_INPT_METH_CD
              type: char
              width: 1
              nullable: N
              map: "ClmCrfIn1.CLM_INPT_METH_CD"
            - name: Recycle1_CLM_INPT_SRC_CD
              type: char
              width: 10
              nullable: N
              map: "ClmCrfIn1.CLM_INPT_SRC_CD"
            - name: Recycle1_CLM_IPP_CD
              type: char
              width: 10
              nullable: N
              map: "ClmCrfIn1.CLM_IPP_CD"
            - name: Recycle1_CLM_NTWK_STTUS_CD
              type: char
              width: 2
              nullable: N
              map: "ClmCrfIn1.CLM_NTWK_STTUS_CD"
            - name: Recycle1_CLM_NONPAR_PROV_PFX_CD
              type: char
              width: 4
              nullable: N
              map: "ClmCrfIn1.CLM_NONPAR_PROV_PFX_CD"
            - name: Recycle1_CLM_OTHER_BNF_CD
              type: char
              width: 1
              nullable: N
              map: "ClmCrfIn1.CLM_OTHER_BNF_CD"
            - name: Recycle1_CLM_PAYE_CD
              type: char
              width: 1
              nullable: N
              map: "ClmCrfIn1.CLM_PAYE_CD"
            - name: Recycle1_CLM_PRCS_CTL_AGNT_PFX_CD
              type: char
              width: 4
              nullable: N
              map: "ClmCrfIn1.CLM_PRCS_CTL_AGNT_PFX_CD"
            - name: Recycle1_CLM_SVC_DEFN_PFX_CD
              type: char
              width: 4
              nullable: N
              map: "ClmCrfIn1.CLM_SVC_DEFN_PFX_CD"
            - name: Recycle1_CLM_SVC_PROV_SPEC_CD
              type: char
              width: 10
              nullable: N
              map: "ClmCrfIn1.CLM_SVC_PROV_SPEC_CD"
            - name: Recycle1_CLM_SVC_PROV_TYP_CD
              type: char
              width: 10
              nullable: N
              map: "ClmCrfIn1.CLM_SVC_PROV_TYP_CD"
            - name: Recycle1_CLM_STTUS_CD
              type: char
              width: 2
              nullable: N
              map: "ClmCrfIn1.CLM_STTUS_CD"
            - name: Recycle1_CLM_SUBMT_BCBS_PLN_CD
              type: char
              width: 10
              nullable: N
              map: "ClmCrfIn1.CLM_SUBMT_BCBS_PLN_CD"
            - name: Recycle1_CLM_SUB_BCBS_PLN_CD
              type: char
              width: 10
              nullable: N
              map: "ClmCrfIn1.CLM_SUB_BCBS_PLN_CD"
            - name: Recycle1_CLM_SUBTYP_CD
              type: char
              width: 10
              nullable: N
              map: "ClmCrfIn1.CLM_SUBTYP_CD"
            - name: Recycle1_CLM_TYP_CD
              type: char
              width: 1
              nullable: N
              map: "ClmCrfIn1.CLM_TYP_CD"
            - name: Recycle1_ATCHMT_IN
              type: char
              width: 1
              nullable: N
              map: "ClmCrfIn1.ATCHMT_IN"
            - name: Recycle1_CLM_CLNCL_EDIT_CD
              type: char
              width: 1
              nullable: N
              map: "ClmCrfIn1.CLM_CLNCL_EDIT_CD"
            - name: Recycle1_COBRA_CLM_IN
              type: char
              width: 1
              nullable: N
              map: "ClmCrfIn1.COBRA_CLM_IN"
            - name: Recycle1_FIRST_PASS_IN
              type: char
              width: 1
              nullable: N
              map: "ClmCrfIn1.FIRST_PASS_IN"
            - name: Recycle1_HOST_IN
              type: char
              width: 1
              nullable: N
              map: "ClmCrfIn1.HOST_IN"
            - name: Recycle1_LTR_IN
              type: char
              width: 1
              nullable: N
              map: "ClmCrfIn1.LTR_IN"
            - name: Recycle1_MCARE_ASG_IN
              type: char
              width: 1
              nullable: N
              map: "ClmCrfIn1.MCARE_ASG_IN"
            - name: Recycle1_NOTE_IN
              type: char
              width: 1
              nullable: N
              map: "ClmCrfIn1.NOTE_IN"
            - name: Recycle1_PCA_AUDIT_IN
              type: char
              width: 1
              nullable: N
              map: "ClmCrfIn1.PCA_AUDIT_IN"
            - name: Recycle1_PCP_SUBMT_IN
              type: char
              width: 1
              nullable: N
              map: "ClmCrfIn1.PCP_SUBMT_IN"
            - name: Recycle1_PROD_OOA_IN
              type: char
              width: 1
              nullable: N
              map: "ClmCrfIn1.PROD_OOA_IN"
            - name: Recycle1_ACDNT_DT
              type: char
              width: 10
              nullable: N
              map: "ClmCrfIn1.ACDNT_DT"
            - name: Recycle1_INPT_DT
              type: char
              width: 10
              nullable: N
              map: "ClmCrfIn1.INPT_DT"
            - name: Recycle1_MBR_PLN_ELIG_DT
              type: char
              width: 10
              nullable: N
              map: "ClmCrfIn1.MBR_PLN_ELIG_DT"
            - name: Recycle1_NEXT_RVW_DT
              type: char
              width: 10
              nullable: N
              map: "ClmCrfIn1.NEXT_RVW_DT"
            - name: Recycle1_PD_DT
              type: char
              width: 10
              nullable: N
              map: "ClmCrfIn1.PD_DT"
            - name: Recycle1_PAYMT_DRAG_CYC_DT
              type: char
              width: 10
              nullable: N
              map: "ClmCrfIn1.PAYMT_DRAG_CYC_DT"
            - name: Recycle1_PRCS_DT
              type: char
              width: 10
              nullable: N
              map: "ClmCrfIn1.PRCS_DT"
            - name: Recycle1_RCVD_DT
              type: char
              width: 10
              nullable: N
              map: "ClmCrfIn1.RCVD_DT"
            - name: Recycle1_SVC_STRT_DT
              type: char
              width: 10
              nullable: N
              map: "ClmCrfIn1.SVC_STRT_DT"
            - name: Recycle1_SVC_END_DT
              type: char
              width: 10
              nullable: N
              map: "ClmCrfIn1.SVC_END_DT"
            - name: Recycle1_SMLR_ILNS_DT
              type: char
              width: 10
              nullable: N
              map: "ClmCrfIn1.SMLR_ILNS_DT"
            - name: Recycle1_STTUS_DT
              type: char
              width: 10
              nullable: N
              map: "ClmCrfIn1.STTUS_DT"
            - name: Recycle1_WORK_UNABLE_BEG_DT
              type: char
              width: 10
              nullable: N
              map: "ClmCrfIn1.WORK_UNABLE_BEG_DT"
            - name: Recycle1_WORK_UNABLE_END_DT
              type: char
              width: 10
              nullable: N
              map: "ClmCrfIn1.WORK_UNABLE_END_DT"
            - name: Recycle1_ACDNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmCrfIn1.ACDNT_AMT"
            - name: Recycle1_ACTL_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmCrfIn1.ACTL_PD_AMT"
            - name: Recycle1_ALLOW_AMT
              type: decimal
              width: 19
              precision: 4
              nullable: N
              map: "ClmCrfIn1.ALLOW_AMT"
            - name: Recycle1_DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmCrfIn1.DSALW_AMT"
            - name: Recycle1_COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmCrfIn1.COINS_AMT"
            - name: Recycle1_CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmCrfIn1.CNSD_CHRG_AMT"
            - name: Recycle1_COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmCrfIn1.COPAY_AMT"
            - name: Recycle1_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmCrfIn1.CHRG_AMT"
            - name: Recycle1_DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmCrfIn1.DEDCT_AMT"
            - name: Recycle1_PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmCrfIn1.PAYBL_AMT"
            - name: Recycle1_CLM_CT
              type: smallint
              width: 2
              nullable: N
              map: "ClmCrfIn1.CLM_CT"
            - name: Recycle1_MBR_AGE
              type: decimal
              width: 3
              nullable: N
              map: "ClmCrfIn1.MBR_AGE"
            - name: Recycle1_ADJ_FROM_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "ClmCrfIn1.ADJ_FROM_CLM_ID"
            - name: Recycle1_ADJ_TO_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "ClmCrfIn1.ADJ_TO_CLM_ID"
            - name: Recycle1_DOC_TX_ID
              type: char
              width: 18
              nullable: N
              map: "ClmCrfIn1.DOC_TX_ID"
            - name: Recycle1_MCAID_RESUB_NO
              type: char
              width: 15
              map: "ClmCrfIn1.MCAID_RESUB_NO"
            - name: Recycle1_MCARE_ID
              type: char
              width: 12
              nullable: N
              map: "ClmCrfIn1.MCARE_ID"
            - name: Recycle1_MBR_SFX_NO
              type: char
              width: 2
              map: "ClmCrfIn1.MBR_SFX_NO"
            - name: Recycle1_PATN_ACCT_NO
              type: varchar
              width: 20
              map: "ClmCrfIn1.PATN_ACCT_NO"
            - name: Recycle1_PAYMT_REF_ID
              type: char
              width: 16
              nullable: N
              map: "ClmCrfIn1.PAYMT_REF_ID"
            - name: Recycle1_PROV_AGMNT_ID
              type: char
              width: 12
              nullable: N
              map: "ClmCrfIn1.PROV_AGMNT_ID"
            - name: Recycle1_RFRNG_PROV_TX
              type: varchar
              width: 70
              map: "ClmCrfIn1.RFRNG_PROV_TX"
            - name: Recycle1_SUB_ID
              type: char
              width: 14
              nullable: N
              map: "ClmCrfIn1.SUB_ID"
            - name: Recycle1_PRPR_ENTITY
              type: char
              width: 1
              map: "ClmCrfIn1.PRPR_ENTITY"
            - name: Recycle1_PCA_TYP_CD
              type: char
              width: 18
              nullable: N
              map: "ClmCrfIn1.PCA_TYP_CD"
            - name: Recycle1_REL_PCA_CLM_ID
              type: varchar
              width: 18
              nullable: N
              map: "ClmCrfIn1.REL_PCA_CLM_ID"
            - name: Recycle1_CLCL_MICRO_ID
              type: char
              width: 18
              nullable: N
              map: "ClmCrfIn1.CLCL_MICRO_ID"
            - name: Recycle1_CLM_UPDT_SW
              type: char
              width: 1
              nullable: N
              map: "ClmCrfIn1.CLM_UPDT_SW"
            - name: Recycle1_REMIT_SUPRSION_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmCrfIn1.REMIT_SUPRSION_AMT"
            - name: Recycle1_MCAID_STTUS_ID
              type: varchar
              width: 20
              nullable: N
              map: "ClmCrfIn1.MCAID_STTUS_ID"
            - name: Recycle1_PATN_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmCrfIn1.PATN_PD_AMT"
            - name: Recycle1_CLM_SUBMT_ICD_VRSN_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmCrfIn1.CLM_SUBMT_ICD_VRSN_CD"
            - name: Recycle1_CLM_TXNMY_CD
              type: varchar
              width: 20
              map: "ClmCrfIn1.CLM_TXNMY_CD"
            - name: ClmRvversalPaidDtUpdate_SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: SrcSysCdSk
            - name: ClmRvversalPaidDtUpdate_CLM_SK
              type: int
              width: 4
              nullable: N
              map: ClmAdjFromClmSk
            - name: ClmRvversalPaidDtUpdate_CLM_ID
              type: char
              width: 20
              nullable: N
              map: "ClmCrfIn1.ADJ_FROM_CLM"
            - name: ClmRvversalPaidDtUpdate_CLM_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: AdjustedFacetsClaimStatus91
            - name: ClmRvversalPaidDtUpdate_LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: RunCycle
            - name: ClmRvversalPaidDtUpdate_PD_DT_SK
              type: char
              width: 10
              nullable: N
              map: PdDtSk
            - name: ClmRvversalPaidDtUpdate_SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmCrfIn1.SRC_SYS_CD"
            - name: ClmRvversalPaidDtUpdate_STTUS_DT
              type: char
              width: 10
              nullable: N
              map: "ClmCrfIn1.STTUS_DT"
            - name: Recycle_Clms_SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmCrfIn1.SRC_SYS_CD"
            - name: Recycle_Clms_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "ClmCrfIn1.CLM_ID"
            - name: filter_ClmFkeyOut1
              type: smallint
              width: 2
              nullable: N
              map: "if (ErrCount = 0 Or PassThru = 'Y') then 1 else 0"
            - name: filter_DefaultUNK
              type: smallint
              width: 2
              nullable: N
              map: "if (@INROWNUM = 1) then 1 else 0"
            - name: filter_DefaultNA
              type: smallint
              width: 2
              nullable: N
              map: "if (@INROWNUM = 1) then 1 else 0"
            - name: filter_Recycle1
              type: smallint
              width: 2
              nullable: N
              map: "if (ErrCount > 0) then 1 else 0"
            - name: filter_ClmRvversalPaidDtUpdate
              type: smallint
              width: 2
              nullable: N
              map: "if (Trim(ClmCrfIn1.SRC_SYS_CD) = \"FACETS\" and ((Trim(ClmCrfIn1.CLM_STTUS_CD) = '02') or (Trim(ClmCrfIn1.CLM_STTUS_CD) = '91')) and ClmAdjFromClmSk > 1) then 1 else 0"
            - name: filter_Recycle_Clms
              type: smallint
              width: 2
              nullable: N
              map: "if (ErrCount > 0) then 1 else 0"
        - name: Trns1.V0S20P5.filter
          type: filter
          source: Trns1.lookup
          filter: "filter_ClmFkeyOut1 = 1"
          schema: 
            - name: ClmFkeyOut1_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: ClmFkeyOut1_SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: ClmFkeyOut1_CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: ClmFkeyOut1_CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: ClmFkeyOut1_LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: ClmFkeyOut1_ADJ_FROM_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: ClmFkeyOut1_ADJ_TO_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: ClmFkeyOut1_ALPHA_PFX_SK
              type: int
              width: 4
              nullable: N
            - name: ClmFkeyOut1_CLM_EOB_EXCD_SK
              type: int
              width: 4
              nullable: N
            - name: ClmFkeyOut1_CLS_SK
              type: int
              width: 4
              nullable: N
            - name: ClmFkeyOut1_CLS_PLN_SK
              type: int
              width: 4
              nullable: N
            - name: ClmFkeyOut1_EXPRNC_CAT_SK
              type: int
              width: 4
              nullable: N
            - name: ClmFkeyOut1_FNCL_LOB_SK
              type: int
              width: 4
              nullable: N
            - name: ClmFkeyOut1_GRP_SK
              type: int
              width: 4
              nullable: N
            - name: ClmFkeyOut1_MBR_SK
              type: int
              width: 4
              nullable: N
            - name: ClmFkeyOut1_NTWK_SK
              type: int
              width: 4
              nullable: N
            - name: ClmFkeyOut1_PROD_SK
              type: int
              width: 4
              nullable: N
            - name: ClmFkeyOut1_SUBGRP_SK
              type: int
              width: 4
              nullable: N
            - name: ClmFkeyOut1_SUB_SK
              type: int
              width: 4
              nullable: N
            - name: ClmFkeyOut1_CLM_ACDNT_CD_SK
              type: int
              width: 4
              nullable: N
            - name: ClmFkeyOut1_CLM_ACDNT_ST_CD_SK
              type: int
              width: 4
              nullable: N
            - name: ClmFkeyOut1_CLM_ACTV_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
            - name: ClmFkeyOut1_CLM_AGMNT_SRC_CD_SK
              type: int
              width: 4
              nullable: N
            - name: ClmFkeyOut1_CLM_BTCH_ACTN_CD_SK
              type: int
              width: 4
              nullable: N
            - name: ClmFkeyOut1_CLM_CAP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: ClmFkeyOut1_CLM_CAT_CD_SK
              type: int
              width: 4
              nullable: N
            - name: ClmFkeyOut1_CLM_CHK_CYC_OVRD_CD_SK
              type: int
              width: 4
              nullable: N
            - name: ClmFkeyOut1_CLM_COB_CD_SK
              type: int
              width: 4
              nullable: N
            - name: ClmFkeyOut1_CLM_FINL_DISP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: ClmFkeyOut1_CLM_INPT_METH_CD_SK
              type: int
              width: 4
              nullable: N
            - name: ClmFkeyOut1_CLM_INPT_SRC_CD_SK
              type: int
              width: 4
              nullable: N
            - name: ClmFkeyOut1_CLM_INTER_PLN_PGM_CD_SK
              type: int
              width: 4
              nullable: N
            - name: ClmFkeyOut1_CLM_NTWK_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: ClmFkeyOut1_CLM_NONPAR_PROV_PFX_CD_SK
              type: int
              width: 4
              nullable: N
            - name: ClmFkeyOut1_CLM_OTHR_BNF_CD_SK
              type: int
              width: 4
              nullable: N
            - name: ClmFkeyOut1_CLM_PAYE_CD_SK
              type: int
              width: 4
              nullable: N
            - name: ClmFkeyOut1_CLM_PRCS_CTL_AGNT_PFX_CD_SK
              type: int
              width: 4
              nullable: N
            - name: ClmFkeyOut1_CLM_SVC_DEFN_PFX_CD_SK
              type: int
              width: 4
              nullable: N
            - name: ClmFkeyOut1_CLM_SVC_PROV_SPEC_CD_SK
              type: int
              width: 4
              nullable: N
            - name: ClmFkeyOut1_CLM_SVC_PROV_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: ClmFkeyOut1_CLM_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: ClmFkeyOut1_CLM_SUBMTTING_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
            - name: ClmFkeyOut1_CLM_SUB_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
            - name: ClmFkeyOut1_CLM_SUBTYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: ClmFkeyOut1_CLM_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: ClmFkeyOut1_ATCHMT_IN
              type: char
              width: 1
              nullable: N
            - name: ClmFkeyOut1_CLNCL_EDIT_IN
              type: char
              width: 1
              nullable: N
            - name: ClmFkeyOut1_COBRA_CLM_IN
              type: char
              width: 1
              nullable: N
            - name: ClmFkeyOut1_FIRST_PASS_IN
              type: char
              width: 1
              nullable: N
            - name: ClmFkeyOut1_HOST_IN
              type: char
              width: 1
              nullable: N
            - name: ClmFkeyOut1_LTR_IN
              type: char
              width: 1
              nullable: N
            - name: ClmFkeyOut1_MCARE_ASG_IN
              type: char
              width: 1
              nullable: N
            - name: ClmFkeyOut1_NOTE_IN
              type: char
              width: 1
              nullable: N
            - name: ClmFkeyOut1_PCA_AUDIT_IN
              type: char
              width: 1
              nullable: N
            - name: ClmFkeyOut1_PCP_SUBMT_IN
              type: char
              width: 1
              nullable: N
            - name: ClmFkeyOut1_PROD_OOA_IN
              type: char
              width: 1
              nullable: N
            - name: ClmFkeyOut1_ACDNT_DT_SK
              type: char
              width: 10
              nullable: N
            - name: ClmFkeyOut1_INPT_DT_SK
              type: char
              width: 10
              nullable: N
            - name: ClmFkeyOut1_MBR_PLN_ELIG_DT_SK
              type: char
              width: 10
              nullable: N
            - name: ClmFkeyOut1_NEXT_RVW_DT_SK
              type: char
              width: 10
              nullable: N
            - name: ClmFkeyOut1_PD_DT_SK
              type: char
              width: 10
              nullable: N
            - name: ClmFkeyOut1_PAYMT_DRAG_CYC_DT_SK
              type: char
              width: 10
              nullable: N
            - name: ClmFkeyOut1_PRCS_DT_SK
              type: char
              width: 10
              nullable: N
            - name: ClmFkeyOut1_RCVD_DT_SK
              type: char
              width: 10
              nullable: N
            - name: ClmFkeyOut1_SVC_STRT_DT_SK
              type: char
              width: 10
              nullable: N
            - name: ClmFkeyOut1_SVC_END_DT_SK
              type: char
              width: 10
              nullable: N
            - name: ClmFkeyOut1_SMLR_ILNS_DT_SK
              type: char
              width: 10
              nullable: N
            - name: ClmFkeyOut1_STTUS_DT_SK
              type: char
              width: 10
              nullable: N
            - name: ClmFkeyOut1_WORK_UNABLE_BEG_DT_SK
              type: char
              width: 10
              nullable: N
            - name: ClmFkeyOut1_WORK_UNABLE_END_DT_SK
              type: char
              width: 10
              nullable: N
            - name: ClmFkeyOut1_ACDNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: ClmFkeyOut1_ACTL_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: ClmFkeyOut1_ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: ClmFkeyOut1_DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: ClmFkeyOut1_COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: ClmFkeyOut1_CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: ClmFkeyOut1_COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: ClmFkeyOut1_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: ClmFkeyOut1_DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: ClmFkeyOut1_PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: ClmFkeyOut1_CLM_CT
              type: smallint
              width: 2
              nullable: N
            - name: ClmFkeyOut1_MBR_AGE
              type: decimal
              width: 3
            - name: ClmFkeyOut1_ADJ_FROM_CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: ClmFkeyOut1_ADJ_TO_CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: ClmFkeyOut1_DOC_TX_ID
              type: char
              width: 18
              nullable: N
            - name: ClmFkeyOut1_MCAID_RESUB_NO
              type: char
              width: 15
            - name: ClmFkeyOut1_MCARE_ID
              type: char
              width: 12
              nullable: N
            - name: ClmFkeyOut1_MBR_SFX_NO
              type: char
              width: 2
            - name: ClmFkeyOut1_PATN_ACCT_NO
              type: varchar
              width: 35
            - name: ClmFkeyOut1_PAYMT_REF_ID
              type: char
              width: 16
              nullable: N
            - name: ClmFkeyOut1_PROV_AGMNT_ID
              type: char
              width: 12
              nullable: N
            - name: ClmFkeyOut1_RFRNG_PROV_TX
              type: varchar
              width: 70
            - name: ClmFkeyOut1_SUB_ID
              type: char
              width: 14
              nullable: N
            - name: ClmFkeyOut1_PCA_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: ClmFkeyOut1_REL_PCA_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: ClmFkeyOut1_REL_BASE_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: ClmFkeyOut1_CLM_UPDT_SW
              type: char
              width: 1
              nullable: N
            - name: ClmFkeyOut1_CLCL_MICRO_ID
              type: char
              width: 18
              nullable: N
            - name: ClmFkeyOut1_REMIT_SUPRSION_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: ClmFkeyOut1_MCAID_STTUS_ID
              type: varchar
              width: 20
              nullable: N
            - name: ClmFkeyOut1_PATN_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: ClmFkeyOut1_CLM_SUBMT_ICD_VRSN_CD_SK
              type: int
              width: 4
              nullable: N
            - name: ClmFkeyOut1_CLM_TXNMY_CD
              type: varchar
              width: 20
            - name: ClmFkeyOut1_BILL_PAYMT_EXCL_IN
              type: char
              width: 1
              nullable: N
            - name: ClmFkeyOut1_ADJ_FROM_CLM
              type: char
              width: 12
              nullable: N
            - name: ClmFkeyOut1_ADJ_TO_CLM
              type: char
              width: 12
              nullable: N
        - name: Trns1.V0S20P7.filter
          type: filter
          source: Trns1.lookup
          filter: "filter_DefaultUNK = 1"
          schema: 
            - name: DefaultUNK_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: DefaultUNK_CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_ADJ_FROM_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_ADJ_TO_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_ALPHA_PFX_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_CLM_EOB_EXCD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_CLS_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_CLS_PLN_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_EXPRNC_CAT_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_FNCL_LOB_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_GRP_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_MBR_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_NTWK_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_PROD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_SUBGRP_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_SUB_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_CLM_ACDNT_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_CLM_ACDNT_ST_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_CLM_ACTV_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_CLM_AGMNT_SRC_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_CLM_BTCH_ACTN_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_CLM_CAP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_CLM_CAT_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_CLM_CHK_CYC_OVRD_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_CLM_COB_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_CLM_FINL_DISP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_CLM_INPT_METH_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_CLM_INPT_SRC_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_CLM_INTER_PLN_PGM_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_CLM_NTWK_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_CLM_NONPAR_PROV_PFX_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_CLM_OTHR_BNF_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_CLM_PAYE_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_CLM_PRCS_CTL_AGNT_PFX_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_CLM_SVC_DEFN_PFX_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_CLM_SVC_PROV_SPEC_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_CLM_SVC_PROV_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_CLM_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_CLM_SUBMTTING_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_CLM_SUB_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_CLM_SUBTYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_CLM_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_ATCHMT_IN
              type: char
              width: 1
              nullable: N
            - name: DefaultUNK_CLNCL_EDIT_IN
              type: char
              width: 1
              nullable: N
            - name: DefaultUNK_COBRA_CLM_IN
              type: char
              width: 1
              nullable: N
            - name: DefaultUNK_FIRST_PASS_IN
              type: char
              width: 1
              nullable: N
            - name: DefaultUNK_HOST_IN
              type: char
              width: 1
              nullable: N
            - name: DefaultUNK_LTR_IN
              type: char
              width: 1
              nullable: N
            - name: DefaultUNK_MCARE_ASG_IN
              type: char
              width: 1
              nullable: N
            - name: DefaultUNK_NOTE_IN
              type: char
              width: 1
              nullable: N
            - name: DefaultUNK_PCA_AUDIT_IN
              type: char
              width: 1
              nullable: N
            - name: DefaultUNK_PCP_SUBMT_IN
              type: char
              width: 1
              nullable: N
            - name: DefaultUNK_PROD_OOA_IN
              type: char
              width: 1
              nullable: N
            - name: DefaultUNK_ACDNT_DT_SK
              type: char
              width: 10
              nullable: N
            - name: DefaultUNK_INPT_DT_SK
              type: char
              width: 10
              nullable: N
            - name: DefaultUNK_MBR_PLN_ELIG_DT_SK
              type: char
              width: 10
              nullable: N
            - name: DefaultUNK_NEXT_RVW_DT_SK
              type: char
              width: 10
              nullable: N
            - name: DefaultUNK_PD_DT_SK
              type: char
              width: 10
              nullable: N
            - name: DefaultUNK_PAYMT_DRAG_CYC_DT_SK
              type: char
              width: 10
              nullable: N
            - name: DefaultUNK_PRCS_DT_SK
              type: char
              width: 10
              nullable: N
            - name: DefaultUNK_RCVD_DT_SK
              type: char
              width: 10
              nullable: N
            - name: DefaultUNK_SVC_STRT_DT_SK
              type: char
              width: 10
              nullable: N
            - name: DefaultUNK_SVC_END_DT_SK
              type: char
              width: 10
              nullable: N
            - name: DefaultUNK_SMLR_ILNS_DT_SK
              type: char
              width: 10
              nullable: N
            - name: DefaultUNK_STTUS_DT_SK
              type: char
              width: 10
              nullable: N
            - name: DefaultUNK_WORK_UNABLE_BEG_DT_SK
              type: char
              width: 10
              nullable: N
            - name: DefaultUNK_WORK_UNABLE_END_DT_SK
              type: char
              width: 10
              nullable: N
            - name: DefaultUNK_ACDNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DefaultUNK_ACTL_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DefaultUNK_ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DefaultUNK_DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DefaultUNK_COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DefaultUNK_CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DefaultUNK_COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DefaultUNK_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DefaultUNK_DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DefaultUNK_PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DefaultUNK_CLM_CT
              type: smallint
              width: 2
              nullable: N
            - name: DefaultUNK_MBR_AGE
              type: decimal
              width: 3
            - name: DefaultUNK_ADJ_FROM_CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: DefaultUNK_ADJ_TO_CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: DefaultUNK_DOC_TX_ID
              type: char
              width: 18
              nullable: N
            - name: DefaultUNK_MCAID_RESUB_NO
              type: char
              width: 15
            - name: DefaultUNK_MCARE_ID
              type: char
              width: 12
              nullable: N
            - name: DefaultUNK_MBR_SFX_NO
              type: char
              width: 2
            - name: DefaultUNK_PATN_ACCT_NO
              type: varchar
              width: 35
            - name: DefaultUNK_PAYMT_REF_ID
              type: char
              width: 16
              nullable: N
            - name: DefaultUNK_PROV_AGMNT_ID
              type: char
              width: 12
              nullable: N
            - name: DefaultUNK_RFRNG_PROV_TX
              type: varchar
              width: 70
            - name: DefaultUNK_SUB_ID
              type: char
              width: 14
              nullable: N
            - name: DefaultUNK_PCA_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_REL_PCA_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_REL_BASE_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_CLM_UPDT_SW
              type: char
              width: 1
              nullable: N
            - name: DefaultUNK_CLCL_MICRO_ID
              type: char
              width: 18
              nullable: N
            - name: DefaultUNK_REMIT_SUPRSION_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DefaultUNK_MCAID_STTUS_ID
              type: varchar
              width: 20
              nullable: N
            - name: DefaultUNK_PATN_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DefaultUNK_CLM_SUBMT_ICD_VRSN_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultUNK_CLM_TXNMY_CD
              type: varchar
              width: 20
            - name: DefaultUNK_BILL_PAYMT_EXCL_IN
              type: char
              width: 1
              nullable: N
            - name: DefaultUNK_ADJ_FROM_CLM
              type: char
              width: 12
              nullable: N
            - name: DefaultUNK_ADJ_TO_CLM
              type: char
              width: 12
              nullable: N
        - name: Trns1.V0S20P8.filter
          type: filter
          source: Trns1.lookup
          filter: "filter_DefaultNA = 1"
          schema: 
            - name: DefaultNA_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: DefaultNA_CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_ADJ_FROM_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_ADJ_TO_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_ALPHA_PFX_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_CLM_EOB_EXCD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_CLS_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_CLS_PLN_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_EXPRNC_CAT_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_FNCL_LOB_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_GRP_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_MBR_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_NTWK_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_PROD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_SUBGRP_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_SUB_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_CLM_ACDNT_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_CLM_ACDNT_ST_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_CLM_ACTV_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_CLM_AGMNT_SRC_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_CLM_BTCH_ACTN_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_CLM_CAP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_CLM_CAT_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_CLM_CHK_CYC_OVRD_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_CLM_COB_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_CLM_FINL_DISP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_CLM_INPT_METH_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_CLM_INPT_SRC_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_CLM_INTER_PLN_PGM_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_CLM_NTWK_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_CLM_NONPAR_PROV_PFX_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_CLM_OTHR_BNF_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_CLM_PAYE_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_CLM_PRCS_CTL_AGNT_PFX_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_CLM_SVC_DEFN_PFX_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_CLM_SVC_PROV_SPEC_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_CLM_SVC_PROV_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_CLM_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_CLM_SUBMTTING_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_CLM_SUB_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_CLM_SUBTYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_CLM_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_ATCHMT_IN
              type: char
              width: 1
              nullable: N
            - name: DefaultNA_CLNCL_EDIT_IN
              type: char
              width: 1
              nullable: N
            - name: DefaultNA_COBRA_CLM_IN
              type: char
              width: 1
              nullable: N
            - name: DefaultNA_FIRST_PASS_IN
              type: char
              width: 1
              nullable: N
            - name: DefaultNA_HOST_IN
              type: char
              width: 1
              nullable: N
            - name: DefaultNA_LTR_IN
              type: char
              width: 1
              nullable: N
            - name: DefaultNA_MCARE_ASG_IN
              type: char
              width: 1
              nullable: N
            - name: DefaultNA_NOTE_IN
              type: char
              width: 1
              nullable: N
            - name: DefaultNA_PCA_AUDIT_IN
              type: char
              width: 1
              nullable: N
            - name: DefaultNA_PCP_SUBMT_IN
              type: char
              width: 1
              nullable: N
            - name: DefaultNA_PROD_OOA_IN
              type: char
              width: 1
              nullable: N
            - name: DefaultNA_ACDNT_DT_SK
              type: char
              width: 10
              nullable: N
            - name: DefaultNA_INPT_DT_SK
              type: char
              width: 10
              nullable: N
            - name: DefaultNA_MBR_PLN_ELIG_DT_SK
              type: char
              width: 10
              nullable: N
            - name: DefaultNA_NEXT_RVW_DT_SK
              type: char
              width: 10
              nullable: N
            - name: DefaultNA_PD_DT_SK
              type: char
              width: 10
              nullable: N
            - name: DefaultNA_PAYMT_DRAG_CYC_DT_SK
              type: char
              width: 10
              nullable: N
            - name: DefaultNA_PRCS_DT_SK
              type: char
              width: 10
              nullable: N
            - name: DefaultNA_RCVD_DT_SK
              type: char
              width: 10
              nullable: N
            - name: DefaultNA_SVC_STRT_DT_SK
              type: char
              width: 10
              nullable: N
            - name: DefaultNA_SVC_END_DT_SK
              type: char
              width: 10
              nullable: N
            - name: DefaultNA_SMLR_ILNS_DT_SK
              type: char
              width: 10
              nullable: N
            - name: DefaultNA_STTUS_DT_SK
              type: char
              width: 10
              nullable: N
            - name: DefaultNA_WORK_UNABLE_BEG_DT_SK
              type: char
              width: 10
              nullable: N
            - name: DefaultNA_WORK_UNABLE_END_DT_SK
              type: char
              width: 10
              nullable: N
            - name: DefaultNA_ACDNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DefaultNA_ACTL_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DefaultNA_ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DefaultNA_DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DefaultNA_COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DefaultNA_CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DefaultNA_COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DefaultNA_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DefaultNA_DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DefaultNA_PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DefaultNA_CLM_CT
              type: smallint
              width: 2
              nullable: N
            - name: DefaultNA_MBR_AGE
              type: decimal
              width: 3
            - name: DefaultNA_ADJ_FROM_CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: DefaultNA_ADJ_TO_CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: DefaultNA_DOC_TX_ID
              type: char
              width: 18
              nullable: N
            - name: DefaultNA_MCAID_RESUB_NO
              type: char
              width: 15
            - name: DefaultNA_MCARE_ID
              type: char
              width: 12
              nullable: N
            - name: DefaultNA_MBR_SFX_NO
              type: char
              width: 2
            - name: DefaultNA_PATN_ACCT_NO
              type: varchar
              width: 35
            - name: DefaultNA_PAYMT_REF_ID
              type: char
              width: 16
              nullable: N
            - name: DefaultNA_PROV_AGMNT_ID
              type: char
              width: 12
              nullable: N
            - name: DefaultNA_RFRNG_PROV_TX
              type: varchar
              width: 70
            - name: DefaultNA_SUB_ID
              type: char
              width: 14
              nullable: N
            - name: DefaultNA_PCA_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_REL_PCA_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_REL_BASE_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_CLM_UPDT_SW
              type: char
              width: 1
              nullable: N
            - name: DefaultNA_CLCL_MICRO_ID
              type: char
              width: 18
              nullable: N
            - name: DefaultNA_REMIT_SUPRSION_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DefaultNA_MCAID_STTUS_ID
              type: varchar
              width: 20
              nullable: N
            - name: DefaultNA_PATN_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DefaultNA_CLM_SUBMT_ICD_VRSN_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DefaultNA_CLM_TXNMY_CD
              type: varchar
              width: 20
            - name: DefaultNA_BILL_PAYMT_EXCL_IN
              type: char
              width: 1
              nullable: N
            - name: DefaultNA_ADJ_FROM_CLM
              type: char
              width: 12
              nullable: N
            - name: DefaultNA_ADJ_TO_CLM
              type: char
              width: 12
              nullable: N
        - name: Trns1.V0S20P9.filter
          type: filter
          source: Trns1.lookup
          filter: "filter_Recycle1 = 1"
          schema: 
            - name: Recycle1_JOB_EXCTN_RCRD_ERR_SK
              type: int
              width: 4
              nullable: N
            - name: Recycle1_INSRT_UPDT_CD
              type: char
              width: 10
              nullable: N
            - name: Recycle1_DISCARD_IN
              type: char
              width: 1
              nullable: N
            - name: Recycle1_PASS_THRU_IN
              type: char
              width: 1
              nullable: N
            - name: Recycle1_FIRST_RECYC_DT
              type: timestamp
              nullable: N
            - name: Recycle1_ERR_CT
              type: smallint
              width: 2
              nullable: N
            - name: Recycle1_RECYCLE_CT
              type: decimal
              width: 6
              nullable: N
            - name: Recycle1_SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
            - name: Recycle1_PRI_KEY_STRING
              type: varchar
              width: 1023
              nullable: N
            - name: Recycle1_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: Recycle1_SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: Recycle1_CLM_ID
              type: char
              width: 18
              nullable: N
            - name: Recycle1_CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: Recycle1_LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: Recycle1_ADJ_FROM_CLM
              type: char
              width: 12
              nullable: N
            - name: Recycle1_ADJ_TO_CLM
              type: char
              width: 12
              nullable: N
            - name: Recycle1_ALPHA_PFX_CD
              type: char
              width: 3
            - name: Recycle1_CLM_EOB_EXCD
              type: char
              width: 3
              nullable: N
            - name: Recycle1_CLS
              type: char
              width: 4
              nullable: N
            - name: Recycle1_CLS_PLN
              type: char
              width: 8
              nullable: N
            - name: Recycle1_EXPRNC_CAT
              type: char
              width: 4
              nullable: N
            - name: Recycle1_FNCL_LOB_NO
              type: char
              width: 4
            - name: Recycle1_GRP
              type: char
              width: 8
              nullable: N
            - name: Recycle1_MBR_CK
              type: int
              width: 4
              nullable: N
            - name: Recycle1_NTWK
              type: char
              width: 12
              nullable: N
            - name: Recycle1_PROD
              type: char
              width: 8
              nullable: N
            - name: Recycle1_SUBGRP
              type: char
              width: 4
              nullable: N
            - name: Recycle1_SUB_CK
              type: int
              width: 4
              nullable: N
            - name: Recycle1_CLM_ACDNT_CD
              type: char
              width: 2
              nullable: N
            - name: Recycle1_CLM_ACDNT_ST_CD
              type: char
              width: 2
              nullable: N
            - name: Recycle1_CLM_ACTIVATING_BCBS_PLN_CD
              type: char
              width: 10
              nullable: N
            - name: Recycle1_CLM_AGMNT_SRC_CD
              type: char
              width: 10
              nullable: N
            - name: Recycle1_CLM_BTCH_ACTN_CD
              type: char
              width: 1
              nullable: N
            - name: Recycle1_CLM_CAP_CD
              type: char
              width: 10
              nullable: N
            - name: Recycle1_CLM_CAT_CD
              type: char
              width: 10
              nullable: N
            - name: Recycle1_CLM_CHK_CYC_OVRD_CD
              type: char
              width: 1
              nullable: N
            - name: Recycle1_CLM_COB_CD
              type: char
              width: 1
              nullable: N
            - name: Recycle1_FINL_DISP_CD
              type: varchar
              width: 20
              nullable: N
            - name: Recycle1_CLM_INPT_METH_CD
              type: char
              width: 1
              nullable: N
            - name: Recycle1_CLM_INPT_SRC_CD
              type: char
              width: 10
              nullable: N
            - name: Recycle1_CLM_IPP_CD
              type: char
              width: 10
              nullable: N
            - name: Recycle1_CLM_NTWK_STTUS_CD
              type: char
              width: 2
              nullable: N
            - name: Recycle1_CLM_NONPAR_PROV_PFX_CD
              type: char
              width: 4
              nullable: N
            - name: Recycle1_CLM_OTHER_BNF_CD
              type: char
              width: 1
              nullable: N
            - name: Recycle1_CLM_PAYE_CD
              type: char
              width: 1
              nullable: N
            - name: Recycle1_CLM_PRCS_CTL_AGNT_PFX_CD
              type: char
              width: 4
              nullable: N
            - name: Recycle1_CLM_SVC_DEFN_PFX_CD
              type: char
              width: 4
              nullable: N
            - name: Recycle1_CLM_SVC_PROV_SPEC_CD
              type: char
              width: 10
              nullable: N
            - name: Recycle1_CLM_SVC_PROV_TYP_CD
              type: char
              width: 10
              nullable: N
            - name: Recycle1_CLM_STTUS_CD
              type: char
              width: 2
              nullable: N
            - name: Recycle1_CLM_SUBMT_BCBS_PLN_CD
              type: char
              width: 10
              nullable: N
            - name: Recycle1_CLM_SUB_BCBS_PLN_CD
              type: char
              width: 10
              nullable: N
            - name: Recycle1_CLM_SUBTYP_CD
              type: char
              width: 10
              nullable: N
            - name: Recycle1_CLM_TYP_CD
              type: char
              width: 1
              nullable: N
            - name: Recycle1_ATCHMT_IN
              type: char
              width: 1
              nullable: N
            - name: Recycle1_CLM_CLNCL_EDIT_CD
              type: char
              width: 1
              nullable: N
            - name: Recycle1_COBRA_CLM_IN
              type: char
              width: 1
              nullable: N
            - name: Recycle1_FIRST_PASS_IN
              type: char
              width: 1
              nullable: N
            - name: Recycle1_HOST_IN
              type: char
              width: 1
              nullable: N
            - name: Recycle1_LTR_IN
              type: char
              width: 1
              nullable: N
            - name: Recycle1_MCARE_ASG_IN
              type: char
              width: 1
              nullable: N
            - name: Recycle1_NOTE_IN
              type: char
              width: 1
              nullable: N
            - name: Recycle1_PCA_AUDIT_IN
              type: char
              width: 1
              nullable: N
            - name: Recycle1_PCP_SUBMT_IN
              type: char
              width: 1
              nullable: N
            - name: Recycle1_PROD_OOA_IN
              type: char
              width: 1
              nullable: N
            - name: Recycle1_ACDNT_DT
              type: char
              width: 10
              nullable: N
            - name: Recycle1_INPT_DT
              type: char
              width: 10
              nullable: N
            - name: Recycle1_MBR_PLN_ELIG_DT
              type: char
              width: 10
              nullable: N
            - name: Recycle1_NEXT_RVW_DT
              type: char
              width: 10
              nullable: N
            - name: Recycle1_PD_DT
              type: char
              width: 10
              nullable: N
            - name: Recycle1_PAYMT_DRAG_CYC_DT
              type: char
              width: 10
              nullable: N
            - name: Recycle1_PRCS_DT
              type: char
              width: 10
              nullable: N
            - name: Recycle1_RCVD_DT
              type: char
              width: 10
              nullable: N
            - name: Recycle1_SVC_STRT_DT
              type: char
              width: 10
              nullable: N
            - name: Recycle1_SVC_END_DT
              type: char
              width: 10
              nullable: N
            - name: Recycle1_SMLR_ILNS_DT
              type: char
              width: 10
              nullable: N
            - name: Recycle1_STTUS_DT
              type: char
              width: 10
              nullable: N
            - name: Recycle1_WORK_UNABLE_BEG_DT
              type: char
              width: 10
              nullable: N
            - name: Recycle1_WORK_UNABLE_END_DT
              type: char
              width: 10
              nullable: N
            - name: Recycle1_ACDNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: Recycle1_ACTL_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: Recycle1_ALLOW_AMT
              type: decimal
              width: 19
              precision: 4
              nullable: N
            - name: Recycle1_DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: Recycle1_COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: Recycle1_CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: Recycle1_COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: Recycle1_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: Recycle1_DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: Recycle1_PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: Recycle1_CLM_CT
              type: smallint
              width: 2
              nullable: N
            - name: Recycle1_MBR_AGE
              type: decimal
              width: 3
              nullable: N
            - name: Recycle1_ADJ_FROM_CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: Recycle1_ADJ_TO_CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: Recycle1_DOC_TX_ID
              type: char
              width: 18
              nullable: N
            - name: Recycle1_MCAID_RESUB_NO
              type: char
              width: 15
            - name: Recycle1_MCARE_ID
              type: char
              width: 12
              nullable: N
            - name: Recycle1_MBR_SFX_NO
              type: char
              width: 2
            - name: Recycle1_PATN_ACCT_NO
              type: varchar
              width: 20
            - name: Recycle1_PAYMT_REF_ID
              type: char
              width: 16
              nullable: N
            - name: Recycle1_PROV_AGMNT_ID
              type: char
              width: 12
              nullable: N
            - name: Recycle1_RFRNG_PROV_TX
              type: varchar
              width: 70
            - name: Recycle1_SUB_ID
              type: char
              width: 14
              nullable: N
            - name: Recycle1_PRPR_ENTITY
              type: char
              width: 1
            - name: Recycle1_PCA_TYP_CD
              type: char
              width: 18
              nullable: N
            - name: Recycle1_REL_PCA_CLM_ID
              type: varchar
              width: 18
              nullable: N
            - name: Recycle1_CLCL_MICRO_ID
              type: char
              width: 18
              nullable: N
            - name: Recycle1_CLM_UPDT_SW
              type: char
              width: 1
              nullable: N
            - name: Recycle1_REMIT_SUPRSION_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: Recycle1_MCAID_STTUS_ID
              type: varchar
              width: 20
              nullable: N
            - name: Recycle1_PATN_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: Recycle1_CLM_SUBMT_ICD_VRSN_CD
              type: varchar
              width: 20
              nullable: N
            - name: Recycle1_CLM_TXNMY_CD
              type: varchar
              width: 20
        - name: Trns1.V0S20P19.filter
          type: filter
          source: Trns1.lookup
          filter: "filter_ClmRvversalPaidDtUpdate = 1"
          schema: 
            - name: ClmRvversalPaidDtUpdate_SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: ClmRvversalPaidDtUpdate_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: ClmRvversalPaidDtUpdate_CLM_ID
              type: char
              width: 20
              nullable: N
            - name: ClmRvversalPaidDtUpdate_CLM_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: ClmRvversalPaidDtUpdate_LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: ClmRvversalPaidDtUpdate_PD_DT_SK
              type: char
              width: 10
              nullable: N
            - name: ClmRvversalPaidDtUpdate_SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
            - name: ClmRvversalPaidDtUpdate_STTUS_DT
              type: char
              width: 10
              nullable: N
        - name: Trns1.V0S20P22.filter
          type: filter
          source: Trns1.lookup
          filter: "filter_Recycle_Clms = 1"
          schema: 
            - name: Recycle_Clms_SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
            - name: Recycle_Clms_CLM_ID
              type: varchar
              width: 20
              nullable: N
        - name: Trns1.V0S20P5
          type: map
          source: Trns1.V0S20P5.filter
          alias: ClmFkeyOut1
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_CLM_SK"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_SRC_SYS_CD_SK"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_CLM_ID"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: ADJ_FROM_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_ADJ_FROM_CLM_SK"
            - name: ADJ_TO_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_ADJ_TO_CLM_SK"
            - name: ALPHA_PFX_SK
              type: int
              width: 4
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_ALPHA_PFX_SK"
            - name: CLM_EOB_EXCD_SK
              type: int
              width: 4
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_CLM_EOB_EXCD_SK"
            - name: CLS_SK
              type: int
              width: 4
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_CLS_SK"
            - name: CLS_PLN_SK
              type: int
              width: 4
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_CLS_PLN_SK"
            - name: EXPRNC_CAT_SK
              type: int
              width: 4
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_EXPRNC_CAT_SK"
            - name: FNCL_LOB_SK
              type: int
              width: 4
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_FNCL_LOB_SK"
            - name: GRP_SK
              type: int
              width: 4
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_GRP_SK"
            - name: MBR_SK
              type: int
              width: 4
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_MBR_SK"
            - name: NTWK_SK
              type: int
              width: 4
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_NTWK_SK"
            - name: PROD_SK
              type: int
              width: 4
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_PROD_SK"
            - name: SUBGRP_SK
              type: int
              width: 4
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_SUBGRP_SK"
            - name: SUB_SK
              type: int
              width: 4
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_SUB_SK"
            - name: CLM_ACDNT_CD_SK
              type: int
              width: 4
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_CLM_ACDNT_CD_SK"
            - name: CLM_ACDNT_ST_CD_SK
              type: int
              width: 4
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_CLM_ACDNT_ST_CD_SK"
            - name: CLM_ACTV_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_CLM_ACTV_BCBS_PLN_CD_SK"
            - name: CLM_AGMNT_SRC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_CLM_AGMNT_SRC_CD_SK"
            - name: CLM_BTCH_ACTN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_CLM_BTCH_ACTN_CD_SK"
            - name: CLM_CAP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_CLM_CAP_CD_SK"
            - name: CLM_CAT_CD_SK
              type: int
              width: 4
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_CLM_CAT_CD_SK"
            - name: CLM_CHK_CYC_OVRD_CD_SK
              type: int
              width: 4
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_CLM_CHK_CYC_OVRD_CD_SK"
            - name: CLM_COB_CD_SK
              type: int
              width: 4
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_CLM_COB_CD_SK"
            - name: CLM_FINL_DISP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_CLM_FINL_DISP_CD_SK"
            - name: CLM_INPT_METH_CD_SK
              type: int
              width: 4
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_CLM_INPT_METH_CD_SK"
            - name: CLM_INPT_SRC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_CLM_INPT_SRC_CD_SK"
            - name: CLM_INTER_PLN_PGM_CD_SK
              type: int
              width: 4
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_CLM_INTER_PLN_PGM_CD_SK"
            - name: CLM_NTWK_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_CLM_NTWK_STTUS_CD_SK"
            - name: CLM_NONPAR_PROV_PFX_CD_SK
              type: int
              width: 4
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_CLM_NONPAR_PROV_PFX_CD_SK"
            - name: CLM_OTHR_BNF_CD_SK
              type: int
              width: 4
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_CLM_OTHR_BNF_CD_SK"
            - name: CLM_PAYE_CD_SK
              type: int
              width: 4
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_CLM_PAYE_CD_SK"
            - name: CLM_PRCS_CTL_AGNT_PFX_CD_SK
              type: int
              width: 4
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_CLM_PRCS_CTL_AGNT_PFX_CD_SK"
            - name: CLM_SVC_DEFN_PFX_CD_SK
              type: int
              width: 4
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_CLM_SVC_DEFN_PFX_CD_SK"
            - name: CLM_SVC_PROV_SPEC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_CLM_SVC_PROV_SPEC_CD_SK"
            - name: CLM_SVC_PROV_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_CLM_SVC_PROV_TYP_CD_SK"
            - name: CLM_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_CLM_STTUS_CD_SK"
            - name: CLM_SUBMTTING_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_CLM_SUBMTTING_BCBS_PLN_CD_SK"
            - name: CLM_SUB_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_CLM_SUB_BCBS_PLN_CD_SK"
            - name: CLM_SUBTYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_CLM_SUBTYP_CD_SK"
            - name: CLM_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_CLM_TYP_CD_SK"
            - name: ATCHMT_IN
              type: char
              width: 1
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_ATCHMT_IN"
            - name: CLNCL_EDIT_IN
              type: char
              width: 1
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_CLNCL_EDIT_IN"
            - name: COBRA_CLM_IN
              type: char
              width: 1
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_COBRA_CLM_IN"
            - name: FIRST_PASS_IN
              type: char
              width: 1
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_FIRST_PASS_IN"
            - name: HOST_IN
              type: char
              width: 1
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_HOST_IN"
            - name: LTR_IN
              type: char
              width: 1
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_LTR_IN"
            - name: MCARE_ASG_IN
              type: char
              width: 1
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_MCARE_ASG_IN"
            - name: NOTE_IN
              type: char
              width: 1
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_NOTE_IN"
            - name: PCA_AUDIT_IN
              type: char
              width: 1
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_PCA_AUDIT_IN"
            - name: PCP_SUBMT_IN
              type: char
              width: 1
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_PCP_SUBMT_IN"
            - name: PROD_OOA_IN
              type: char
              width: 1
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_PROD_OOA_IN"
            - name: ACDNT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_ACDNT_DT_SK"
            - name: INPT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_INPT_DT_SK"
            - name: MBR_PLN_ELIG_DT_SK
              type: char
              width: 10
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_MBR_PLN_ELIG_DT_SK"
            - name: NEXT_RVW_DT_SK
              type: char
              width: 10
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_NEXT_RVW_DT_SK"
            - name: PD_DT_SK
              type: char
              width: 10
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_PD_DT_SK"
            - name: PAYMT_DRAG_CYC_DT_SK
              type: char
              width: 10
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_PAYMT_DRAG_CYC_DT_SK"
            - name: PRCS_DT_SK
              type: char
              width: 10
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_PRCS_DT_SK"
            - name: RCVD_DT_SK
              type: char
              width: 10
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_RCVD_DT_SK"
            - name: SVC_STRT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_SVC_STRT_DT_SK"
            - name: SVC_END_DT_SK
              type: char
              width: 10
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_SVC_END_DT_SK"
            - name: SMLR_ILNS_DT_SK
              type: char
              width: 10
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_SMLR_ILNS_DT_SK"
            - name: STTUS_DT_SK
              type: char
              width: 10
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_STTUS_DT_SK"
            - name: WORK_UNABLE_BEG_DT_SK
              type: char
              width: 10
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_WORK_UNABLE_BEG_DT_SK"
            - name: WORK_UNABLE_END_DT_SK
              type: char
              width: 10
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_WORK_UNABLE_END_DT_SK"
            - name: ACDNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_ACDNT_AMT"
            - name: ACTL_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_ACTL_PD_AMT"
            - name: ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_ALW_AMT"
            - name: DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_DSALW_AMT"
            - name: COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_COINS_AMT"
            - name: CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_CNSD_CHRG_AMT"
            - name: COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_COPAY_AMT"
            - name: CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_CHRG_AMT"
            - name: DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_DEDCT_AMT"
            - name: PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_PAYBL_AMT"
            - name: CLM_CT
              type: smallint
              width: 2
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_CLM_CT"
            - name: MBR_AGE
              type: decimal
              width: 3
              map: "ClmFkeyOut1.ClmFkeyOut1_MBR_AGE"
            - name: ADJ_FROM_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_ADJ_FROM_CLM_ID"
            - name: ADJ_TO_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_ADJ_TO_CLM_ID"
            - name: DOC_TX_ID
              type: char
              width: 18
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_DOC_TX_ID"
            - name: MCAID_RESUB_NO
              type: char
              width: 15
              map: "ClmFkeyOut1.ClmFkeyOut1_MCAID_RESUB_NO"
            - name: MCARE_ID
              type: char
              width: 12
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_MCARE_ID"
            - name: MBR_SFX_NO
              type: char
              width: 2
              map: "ClmFkeyOut1.ClmFkeyOut1_MBR_SFX_NO"
            - name: PATN_ACCT_NO
              type: varchar
              width: 35
              map: "ClmFkeyOut1.ClmFkeyOut1_PATN_ACCT_NO"
            - name: PAYMT_REF_ID
              type: char
              width: 16
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_PAYMT_REF_ID"
            - name: PROV_AGMNT_ID
              type: char
              width: 12
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_PROV_AGMNT_ID"
            - name: RFRNG_PROV_TX
              type: varchar
              width: 70
              map: "ClmFkeyOut1.ClmFkeyOut1_RFRNG_PROV_TX"
            - name: SUB_ID
              type: char
              width: 14
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_SUB_ID"
            - name: PCA_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_PCA_TYP_CD_SK"
            - name: REL_PCA_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_REL_PCA_CLM_SK"
            - name: REL_BASE_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_REL_BASE_CLM_SK"
            - name: CLM_UPDT_SW
              type: char
              width: 1
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_CLM_UPDT_SW"
            - name: CLCL_MICRO_ID
              type: char
              width: 18
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_CLCL_MICRO_ID"
            - name: REMIT_SUPRSION_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_REMIT_SUPRSION_AMT"
            - name: MCAID_STTUS_ID
              type: varchar
              width: 20
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_MCAID_STTUS_ID"
            - name: PATN_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_PATN_PD_AMT"
            - name: CLM_SUBMT_ICD_VRSN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_CLM_SUBMT_ICD_VRSN_CD_SK"
            - name: CLM_TXNMY_CD
              type: varchar
              width: 20
              map: "ClmFkeyOut1.ClmFkeyOut1_CLM_TXNMY_CD"
            - name: BILL_PAYMT_EXCL_IN
              type: char
              width: 1
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_BILL_PAYMT_EXCL_IN"
            - name: ADJ_FROM_CLM
              type: char
              width: 12
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_ADJ_FROM_CLM"
            - name: ADJ_TO_CLM
              type: char
              width: 12
              nullable: N
              map: "ClmFkeyOut1.ClmFkeyOut1_ADJ_TO_CLM"
        - name: Trns1.V0S20P7
          type: map
          source: Trns1.V0S20P7.filter
          alias: DefaultUNK
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_CLM_SK"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_SRC_SYS_CD_SK"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "DefaultUNK.DefaultUNK_CLM_ID"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: ADJ_FROM_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_ADJ_FROM_CLM_SK"
            - name: ADJ_TO_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_ADJ_TO_CLM_SK"
            - name: ALPHA_PFX_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_ALPHA_PFX_SK"
            - name: CLM_EOB_EXCD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_CLM_EOB_EXCD_SK"
            - name: CLS_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_CLS_SK"
            - name: CLS_PLN_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_CLS_PLN_SK"
            - name: EXPRNC_CAT_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_EXPRNC_CAT_SK"
            - name: FNCL_LOB_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_FNCL_LOB_SK"
            - name: GRP_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_GRP_SK"
            - name: MBR_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_MBR_SK"
            - name: NTWK_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_NTWK_SK"
            - name: PROD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_PROD_SK"
            - name: SUBGRP_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_SUBGRP_SK"
            - name: SUB_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_SUB_SK"
            - name: CLM_ACDNT_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_CLM_ACDNT_CD_SK"
            - name: CLM_ACDNT_ST_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_CLM_ACDNT_ST_CD_SK"
            - name: CLM_ACTV_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_CLM_ACTV_BCBS_PLN_CD_SK"
            - name: CLM_AGMNT_SRC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_CLM_AGMNT_SRC_CD_SK"
            - name: CLM_BTCH_ACTN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_CLM_BTCH_ACTN_CD_SK"
            - name: CLM_CAP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_CLM_CAP_CD_SK"
            - name: CLM_CAT_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_CLM_CAT_CD_SK"
            - name: CLM_CHK_CYC_OVRD_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_CLM_CHK_CYC_OVRD_CD_SK"
            - name: CLM_COB_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_CLM_COB_CD_SK"
            - name: CLM_FINL_DISP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_CLM_FINL_DISP_CD_SK"
            - name: CLM_INPT_METH_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_CLM_INPT_METH_CD_SK"
            - name: CLM_INPT_SRC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_CLM_INPT_SRC_CD_SK"
            - name: CLM_INTER_PLN_PGM_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_CLM_INTER_PLN_PGM_CD_SK"
            - name: CLM_NTWK_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_CLM_NTWK_STTUS_CD_SK"
            - name: CLM_NONPAR_PROV_PFX_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_CLM_NONPAR_PROV_PFX_CD_SK"
            - name: CLM_OTHR_BNF_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_CLM_OTHR_BNF_CD_SK"
            - name: CLM_PAYE_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_CLM_PAYE_CD_SK"
            - name: CLM_PRCS_CTL_AGNT_PFX_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_CLM_PRCS_CTL_AGNT_PFX_CD_SK"
            - name: CLM_SVC_DEFN_PFX_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_CLM_SVC_DEFN_PFX_CD_SK"
            - name: CLM_SVC_PROV_SPEC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_CLM_SVC_PROV_SPEC_CD_SK"
            - name: CLM_SVC_PROV_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_CLM_SVC_PROV_TYP_CD_SK"
            - name: CLM_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_CLM_STTUS_CD_SK"
            - name: CLM_SUBMTTING_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_CLM_SUBMTTING_BCBS_PLN_CD_SK"
            - name: CLM_SUB_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_CLM_SUB_BCBS_PLN_CD_SK"
            - name: CLM_SUBTYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_CLM_SUBTYP_CD_SK"
            - name: CLM_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_CLM_TYP_CD_SK"
            - name: ATCHMT_IN
              type: char
              width: 1
              nullable: N
              map: "DefaultUNK.DefaultUNK_ATCHMT_IN"
            - name: CLNCL_EDIT_IN
              type: char
              width: 1
              nullable: N
              map: "DefaultUNK.DefaultUNK_CLNCL_EDIT_IN"
            - name: COBRA_CLM_IN
              type: char
              width: 1
              nullable: N
              map: "DefaultUNK.DefaultUNK_COBRA_CLM_IN"
            - name: FIRST_PASS_IN
              type: char
              width: 1
              nullable: N
              map: "DefaultUNK.DefaultUNK_FIRST_PASS_IN"
            - name: HOST_IN
              type: char
              width: 1
              nullable: N
              map: "DefaultUNK.DefaultUNK_HOST_IN"
            - name: LTR_IN
              type: char
              width: 1
              nullable: N
              map: "DefaultUNK.DefaultUNK_LTR_IN"
            - name: MCARE_ASG_IN
              type: char
              width: 1
              nullable: N
              map: "DefaultUNK.DefaultUNK_MCARE_ASG_IN"
            - name: NOTE_IN
              type: char
              width: 1
              nullable: N
              map: "DefaultUNK.DefaultUNK_NOTE_IN"
            - name: PCA_AUDIT_IN
              type: char
              width: 1
              nullable: N
              map: "DefaultUNK.DefaultUNK_PCA_AUDIT_IN"
            - name: PCP_SUBMT_IN
              type: char
              width: 1
              nullable: N
              map: "DefaultUNK.DefaultUNK_PCP_SUBMT_IN"
            - name: PROD_OOA_IN
              type: char
              width: 1
              nullable: N
              map: "DefaultUNK.DefaultUNK_PROD_OOA_IN"
            - name: ACDNT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "DefaultUNK.DefaultUNK_ACDNT_DT_SK"
            - name: INPT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "DefaultUNK.DefaultUNK_INPT_DT_SK"
            - name: MBR_PLN_ELIG_DT_SK
              type: char
              width: 10
              nullable: N
              map: "DefaultUNK.DefaultUNK_MBR_PLN_ELIG_DT_SK"
            - name: NEXT_RVW_DT_SK
              type: char
              width: 10
              nullable: N
              map: "DefaultUNK.DefaultUNK_NEXT_RVW_DT_SK"
            - name: PD_DT_SK
              type: char
              width: 10
              nullable: N
              map: "DefaultUNK.DefaultUNK_PD_DT_SK"
            - name: PAYMT_DRAG_CYC_DT_SK
              type: char
              width: 10
              nullable: N
              map: "DefaultUNK.DefaultUNK_PAYMT_DRAG_CYC_DT_SK"
            - name: PRCS_DT_SK
              type: char
              width: 10
              nullable: N
              map: "DefaultUNK.DefaultUNK_PRCS_DT_SK"
            - name: RCVD_DT_SK
              type: char
              width: 10
              nullable: N
              map: "DefaultUNK.DefaultUNK_RCVD_DT_SK"
            - name: SVC_STRT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "DefaultUNK.DefaultUNK_SVC_STRT_DT_SK"
            - name: SVC_END_DT_SK
              type: char
              width: 10
              nullable: N
              map: "DefaultUNK.DefaultUNK_SVC_END_DT_SK"
            - name: SMLR_ILNS_DT_SK
              type: char
              width: 10
              nullable: N
              map: "DefaultUNK.DefaultUNK_SMLR_ILNS_DT_SK"
            - name: STTUS_DT_SK
              type: char
              width: 10
              nullable: N
              map: "DefaultUNK.DefaultUNK_STTUS_DT_SK"
            - name: WORK_UNABLE_BEG_DT_SK
              type: char
              width: 10
              nullable: N
              map: "DefaultUNK.DefaultUNK_WORK_UNABLE_BEG_DT_SK"
            - name: WORK_UNABLE_END_DT_SK
              type: char
              width: 10
              nullable: N
              map: "DefaultUNK.DefaultUNK_WORK_UNABLE_END_DT_SK"
            - name: ACDNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "DefaultUNK.DefaultUNK_ACDNT_AMT"
            - name: ACTL_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "DefaultUNK.DefaultUNK_ACTL_PD_AMT"
            - name: ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "DefaultUNK.DefaultUNK_ALW_AMT"
            - name: DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "DefaultUNK.DefaultUNK_DSALW_AMT"
            - name: COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "DefaultUNK.DefaultUNK_COINS_AMT"
            - name: CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "DefaultUNK.DefaultUNK_CNSD_CHRG_AMT"
            - name: COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "DefaultUNK.DefaultUNK_COPAY_AMT"
            - name: CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "DefaultUNK.DefaultUNK_CHRG_AMT"
            - name: DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "DefaultUNK.DefaultUNK_DEDCT_AMT"
            - name: PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "DefaultUNK.DefaultUNK_PAYBL_AMT"
            - name: CLM_CT
              type: smallint
              width: 2
              nullable: N
              map: "DefaultUNK.DefaultUNK_CLM_CT"
            - name: MBR_AGE
              type: decimal
              width: 3
              map: "DefaultUNK.DefaultUNK_MBR_AGE"
            - name: ADJ_FROM_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "DefaultUNK.DefaultUNK_ADJ_FROM_CLM_ID"
            - name: ADJ_TO_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "DefaultUNK.DefaultUNK_ADJ_TO_CLM_ID"
            - name: DOC_TX_ID
              type: char
              width: 18
              nullable: N
              map: "DefaultUNK.DefaultUNK_DOC_TX_ID"
            - name: MCAID_RESUB_NO
              type: char
              width: 15
              map: "DefaultUNK.DefaultUNK_MCAID_RESUB_NO"
            - name: MCARE_ID
              type: char
              width: 12
              nullable: N
              map: "DefaultUNK.DefaultUNK_MCARE_ID"
            - name: MBR_SFX_NO
              type: char
              width: 2
              map: "DefaultUNK.DefaultUNK_MBR_SFX_NO"
            - name: PATN_ACCT_NO
              type: varchar
              width: 35
              map: "DefaultUNK.DefaultUNK_PATN_ACCT_NO"
            - name: PAYMT_REF_ID
              type: char
              width: 16
              nullable: N
              map: "DefaultUNK.DefaultUNK_PAYMT_REF_ID"
            - name: PROV_AGMNT_ID
              type: char
              width: 12
              nullable: N
              map: "DefaultUNK.DefaultUNK_PROV_AGMNT_ID"
            - name: RFRNG_PROV_TX
              type: varchar
              width: 70
              map: "DefaultUNK.DefaultUNK_RFRNG_PROV_TX"
            - name: SUB_ID
              type: char
              width: 14
              nullable: N
              map: "DefaultUNK.DefaultUNK_SUB_ID"
            - name: PCA_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_PCA_TYP_CD_SK"
            - name: REL_PCA_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_REL_PCA_CLM_SK"
            - name: REL_BASE_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_REL_BASE_CLM_SK"
            - name: CLM_UPDT_SW
              type: char
              width: 1
              nullable: N
              map: "DefaultUNK.DefaultUNK_CLM_UPDT_SW"
            - name: CLCL_MICRO_ID
              type: char
              width: 18
              nullable: N
              map: "DefaultUNK.DefaultUNK_CLCL_MICRO_ID"
            - name: REMIT_SUPRSION_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "DefaultUNK.DefaultUNK_REMIT_SUPRSION_AMT"
            - name: MCAID_STTUS_ID
              type: varchar
              width: 20
              nullable: N
              map: "DefaultUNK.DefaultUNK_MCAID_STTUS_ID"
            - name: PATN_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "DefaultUNK.DefaultUNK_PATN_PD_AMT"
            - name: CLM_SUBMT_ICD_VRSN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultUNK.DefaultUNK_CLM_SUBMT_ICD_VRSN_CD_SK"
            - name: CLM_TXNMY_CD
              type: varchar
              width: 20
              map: "DefaultUNK.DefaultUNK_CLM_TXNMY_CD"
            - name: BILL_PAYMT_EXCL_IN
              type: char
              width: 1
              nullable: N
              map: "DefaultUNK.DefaultUNK_BILL_PAYMT_EXCL_IN"
            - name: ADJ_FROM_CLM
              type: char
              width: 12
              nullable: N
              map: "DefaultUNK.DefaultUNK_ADJ_FROM_CLM"
            - name: ADJ_TO_CLM
              type: char
              width: 12
              nullable: N
              map: "DefaultUNK.DefaultUNK_ADJ_TO_CLM"
        - name: Trns1.V0S20P8
          type: map
          source: Trns1.V0S20P8.filter
          alias: DefaultNA
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_CLM_SK"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_SRC_SYS_CD_SK"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "DefaultNA.DefaultNA_CLM_ID"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: ADJ_FROM_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_ADJ_FROM_CLM_SK"
            - name: ADJ_TO_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_ADJ_TO_CLM_SK"
            - name: ALPHA_PFX_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_ALPHA_PFX_SK"
            - name: CLM_EOB_EXCD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_CLM_EOB_EXCD_SK"
            - name: CLS_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_CLS_SK"
            - name: CLS_PLN_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_CLS_PLN_SK"
            - name: EXPRNC_CAT_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_EXPRNC_CAT_SK"
            - name: FNCL_LOB_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_FNCL_LOB_SK"
            - name: GRP_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_GRP_SK"
            - name: MBR_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_MBR_SK"
            - name: NTWK_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_NTWK_SK"
            - name: PROD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_PROD_SK"
            - name: SUBGRP_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_SUBGRP_SK"
            - name: SUB_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_SUB_SK"
            - name: CLM_ACDNT_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_CLM_ACDNT_CD_SK"
            - name: CLM_ACDNT_ST_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_CLM_ACDNT_ST_CD_SK"
            - name: CLM_ACTV_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_CLM_ACTV_BCBS_PLN_CD_SK"
            - name: CLM_AGMNT_SRC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_CLM_AGMNT_SRC_CD_SK"
            - name: CLM_BTCH_ACTN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_CLM_BTCH_ACTN_CD_SK"
            - name: CLM_CAP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_CLM_CAP_CD_SK"
            - name: CLM_CAT_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_CLM_CAT_CD_SK"
            - name: CLM_CHK_CYC_OVRD_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_CLM_CHK_CYC_OVRD_CD_SK"
            - name: CLM_COB_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_CLM_COB_CD_SK"
            - name: CLM_FINL_DISP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_CLM_FINL_DISP_CD_SK"
            - name: CLM_INPT_METH_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_CLM_INPT_METH_CD_SK"
            - name: CLM_INPT_SRC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_CLM_INPT_SRC_CD_SK"
            - name: CLM_INTER_PLN_PGM_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_CLM_INTER_PLN_PGM_CD_SK"
            - name: CLM_NTWK_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_CLM_NTWK_STTUS_CD_SK"
            - name: CLM_NONPAR_PROV_PFX_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_CLM_NONPAR_PROV_PFX_CD_SK"
            - name: CLM_OTHR_BNF_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_CLM_OTHR_BNF_CD_SK"
            - name: CLM_PAYE_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_CLM_PAYE_CD_SK"
            - name: CLM_PRCS_CTL_AGNT_PFX_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_CLM_PRCS_CTL_AGNT_PFX_CD_SK"
            - name: CLM_SVC_DEFN_PFX_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_CLM_SVC_DEFN_PFX_CD_SK"
            - name: CLM_SVC_PROV_SPEC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_CLM_SVC_PROV_SPEC_CD_SK"
            - name: CLM_SVC_PROV_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_CLM_SVC_PROV_TYP_CD_SK"
            - name: CLM_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_CLM_STTUS_CD_SK"
            - name: CLM_SUBMTTING_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_CLM_SUBMTTING_BCBS_PLN_CD_SK"
            - name: CLM_SUB_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_CLM_SUB_BCBS_PLN_CD_SK"
            - name: CLM_SUBTYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_CLM_SUBTYP_CD_SK"
            - name: CLM_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_CLM_TYP_CD_SK"
            - name: ATCHMT_IN
              type: char
              width: 1
              nullable: N
              map: "DefaultNA.DefaultNA_ATCHMT_IN"
            - name: CLNCL_EDIT_IN
              type: char
              width: 1
              nullable: N
              map: "DefaultNA.DefaultNA_CLNCL_EDIT_IN"
            - name: COBRA_CLM_IN
              type: char
              width: 1
              nullable: N
              map: "DefaultNA.DefaultNA_COBRA_CLM_IN"
            - name: FIRST_PASS_IN
              type: char
              width: 1
              nullable: N
              map: "DefaultNA.DefaultNA_FIRST_PASS_IN"
            - name: HOST_IN
              type: char
              width: 1
              nullable: N
              map: "DefaultNA.DefaultNA_HOST_IN"
            - name: LTR_IN
              type: char
              width: 1
              nullable: N
              map: "DefaultNA.DefaultNA_LTR_IN"
            - name: MCARE_ASG_IN
              type: char
              width: 1
              nullable: N
              map: "DefaultNA.DefaultNA_MCARE_ASG_IN"
            - name: NOTE_IN
              type: char
              width: 1
              nullable: N
              map: "DefaultNA.DefaultNA_NOTE_IN"
            - name: PCA_AUDIT_IN
              type: char
              width: 1
              nullable: N
              map: "DefaultNA.DefaultNA_PCA_AUDIT_IN"
            - name: PCP_SUBMT_IN
              type: char
              width: 1
              nullable: N
              map: "DefaultNA.DefaultNA_PCP_SUBMT_IN"
            - name: PROD_OOA_IN
              type: char
              width: 1
              nullable: N
              map: "DefaultNA.DefaultNA_PROD_OOA_IN"
            - name: ACDNT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "DefaultNA.DefaultNA_ACDNT_DT_SK"
            - name: INPT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "DefaultNA.DefaultNA_INPT_DT_SK"
            - name: MBR_PLN_ELIG_DT_SK
              type: char
              width: 10
              nullable: N
              map: "DefaultNA.DefaultNA_MBR_PLN_ELIG_DT_SK"
            - name: NEXT_RVW_DT_SK
              type: char
              width: 10
              nullable: N
              map: "DefaultNA.DefaultNA_NEXT_RVW_DT_SK"
            - name: PD_DT_SK
              type: char
              width: 10
              nullable: N
              map: "DefaultNA.DefaultNA_PD_DT_SK"
            - name: PAYMT_DRAG_CYC_DT_SK
              type: char
              width: 10
              nullable: N
              map: "DefaultNA.DefaultNA_PAYMT_DRAG_CYC_DT_SK"
            - name: PRCS_DT_SK
              type: char
              width: 10
              nullable: N
              map: "DefaultNA.DefaultNA_PRCS_DT_SK"
            - name: RCVD_DT_SK
              type: char
              width: 10
              nullable: N
              map: "DefaultNA.DefaultNA_RCVD_DT_SK"
            - name: SVC_STRT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "DefaultNA.DefaultNA_SVC_STRT_DT_SK"
            - name: SVC_END_DT_SK
              type: char
              width: 10
              nullable: N
              map: "DefaultNA.DefaultNA_SVC_END_DT_SK"
            - name: SMLR_ILNS_DT_SK
              type: char
              width: 10
              nullable: N
              map: "DefaultNA.DefaultNA_SMLR_ILNS_DT_SK"
            - name: STTUS_DT_SK
              type: char
              width: 10
              nullable: N
              map: "DefaultNA.DefaultNA_STTUS_DT_SK"
            - name: WORK_UNABLE_BEG_DT_SK
              type: char
              width: 10
              nullable: N
              map: "DefaultNA.DefaultNA_WORK_UNABLE_BEG_DT_SK"
            - name: WORK_UNABLE_END_DT_SK
              type: char
              width: 10
              nullable: N
              map: "DefaultNA.DefaultNA_WORK_UNABLE_END_DT_SK"
            - name: ACDNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "DefaultNA.DefaultNA_ACDNT_AMT"
            - name: ACTL_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "DefaultNA.DefaultNA_ACTL_PD_AMT"
            - name: ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "DefaultNA.DefaultNA_ALW_AMT"
            - name: DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "DefaultNA.DefaultNA_DSALW_AMT"
            - name: COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "DefaultNA.DefaultNA_COINS_AMT"
            - name: CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "DefaultNA.DefaultNA_CNSD_CHRG_AMT"
            - name: COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "DefaultNA.DefaultNA_COPAY_AMT"
            - name: CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "DefaultNA.DefaultNA_CHRG_AMT"
            - name: DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "DefaultNA.DefaultNA_DEDCT_AMT"
            - name: PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "DefaultNA.DefaultNA_PAYBL_AMT"
            - name: CLM_CT
              type: smallint
              width: 2
              nullable: N
              map: "DefaultNA.DefaultNA_CLM_CT"
            - name: MBR_AGE
              type: decimal
              width: 3
              map: "DefaultNA.DefaultNA_MBR_AGE"
            - name: ADJ_FROM_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "DefaultNA.DefaultNA_ADJ_FROM_CLM_ID"
            - name: ADJ_TO_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "DefaultNA.DefaultNA_ADJ_TO_CLM_ID"
            - name: DOC_TX_ID
              type: char
              width: 18
              nullable: N
              map: "DefaultNA.DefaultNA_DOC_TX_ID"
            - name: MCAID_RESUB_NO
              type: char
              width: 15
              map: "DefaultNA.DefaultNA_MCAID_RESUB_NO"
            - name: MCARE_ID
              type: char
              width: 12
              nullable: N
              map: "DefaultNA.DefaultNA_MCARE_ID"
            - name: MBR_SFX_NO
              type: char
              width: 2
              map: "DefaultNA.DefaultNA_MBR_SFX_NO"
            - name: PATN_ACCT_NO
              type: varchar
              width: 35
              map: "DefaultNA.DefaultNA_PATN_ACCT_NO"
            - name: PAYMT_REF_ID
              type: char
              width: 16
              nullable: N
              map: "DefaultNA.DefaultNA_PAYMT_REF_ID"
            - name: PROV_AGMNT_ID
              type: char
              width: 12
              nullable: N
              map: "DefaultNA.DefaultNA_PROV_AGMNT_ID"
            - name: RFRNG_PROV_TX
              type: varchar
              width: 70
              map: "DefaultNA.DefaultNA_RFRNG_PROV_TX"
            - name: SUB_ID
              type: char
              width: 14
              nullable: N
              map: "DefaultNA.DefaultNA_SUB_ID"
            - name: PCA_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_PCA_TYP_CD_SK"
            - name: REL_PCA_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_REL_PCA_CLM_SK"
            - name: REL_BASE_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_REL_BASE_CLM_SK"
            - name: CLM_UPDT_SW
              type: char
              width: 1
              nullable: N
              map: "DefaultNA.DefaultNA_CLM_UPDT_SW"
            - name: CLCL_MICRO_ID
              type: char
              width: 18
              nullable: N
              map: "DefaultNA.DefaultNA_CLCL_MICRO_ID"
            - name: REMIT_SUPRSION_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "DefaultNA.DefaultNA_REMIT_SUPRSION_AMT"
            - name: MCAID_STTUS_ID
              type: varchar
              width: 20
              nullable: N
              map: "DefaultNA.DefaultNA_MCAID_STTUS_ID"
            - name: PATN_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "DefaultNA.DefaultNA_PATN_PD_AMT"
            - name: CLM_SUBMT_ICD_VRSN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DefaultNA.DefaultNA_CLM_SUBMT_ICD_VRSN_CD_SK"
            - name: CLM_TXNMY_CD
              type: varchar
              width: 20
              map: "DefaultNA.DefaultNA_CLM_TXNMY_CD"
            - name: BILL_PAYMT_EXCL_IN
              type: char
              width: 1
              nullable: N
              map: "DefaultNA.DefaultNA_BILL_PAYMT_EXCL_IN"
            - name: ADJ_FROM_CLM
              type: char
              width: 12
              nullable: N
              map: "DefaultNA.DefaultNA_ADJ_FROM_CLM"
            - name: ADJ_TO_CLM
              type: char
              width: 12
              nullable: N
              map: "DefaultNA.DefaultNA_ADJ_TO_CLM"
        - name: Trns1.V0S20P9
          type: map
          source: Trns1.V0S20P9.filter
          alias: Recycle1
          schema: 
            - name: JOB_EXCTN_RCRD_ERR_SK
              type: int
              width: 4
              nullable: N
              map: "Recycle1.Recycle1_JOB_EXCTN_RCRD_ERR_SK"
            - name: INSRT_UPDT_CD
              type: char
              width: 10
              nullable: N
              map: "Recycle1.Recycle1_INSRT_UPDT_CD"
            - name: DISCARD_IN
              type: char
              width: 1
              nullable: N
              map: "Recycle1.Recycle1_DISCARD_IN"
            - name: PASS_THRU_IN
              type: char
              width: 1
              nullable: N
              map: "Recycle1.Recycle1_PASS_THRU_IN"
            - name: FIRST_RECYC_DT
              type: timestamp
              nullable: N
              map: "Recycle1.Recycle1_FIRST_RECYC_DT"
            - name: ERR_CT
              type: smallint
              width: 2
              nullable: N
              map: "Recycle1.Recycle1_ERR_CT"
            - name: RECYCLE_CT
              type: decimal
              width: 6
              nullable: N
              map: "Recycle1.Recycle1_RECYCLE_CT"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "Recycle1.Recycle1_SRC_SYS_CD"
            - name: PRI_KEY_STRING
              type: varchar
              width: 1023
              nullable: N
              map: "Recycle1.Recycle1_PRI_KEY_STRING"
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: "Recycle1.Recycle1_CLM_SK"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Recycle1.Recycle1_SRC_SYS_CD_SK"
            - name: CLM_ID
              type: char
              width: 18
              nullable: N
              map: "Recycle1.Recycle1_CLM_ID"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "Recycle1.Recycle1_CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "Recycle1.Recycle1_LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: ADJ_FROM_CLM
              type: char
              width: 12
              nullable: N
              map: "Recycle1.Recycle1_ADJ_FROM_CLM"
            - name: ADJ_TO_CLM
              type: char
              width: 12
              nullable: N
              map: "Recycle1.Recycle1_ADJ_TO_CLM"
            - name: ALPHA_PFX_CD
              type: char
              width: 3
              map: "Recycle1.Recycle1_ALPHA_PFX_CD"
            - name: CLM_EOB_EXCD
              type: char
              width: 3
              nullable: N
              map: "Recycle1.Recycle1_CLM_EOB_EXCD"
            - name: CLS
              type: char
              width: 4
              nullable: N
              map: "Recycle1.Recycle1_CLS"
            - name: CLS_PLN
              type: char
              width: 8
              nullable: N
              map: "Recycle1.Recycle1_CLS_PLN"
            - name: EXPRNC_CAT
              type: char
              width: 4
              nullable: N
              map: "Recycle1.Recycle1_EXPRNC_CAT"
            - name: FNCL_LOB_NO
              type: char
              width: 4
              map: "Recycle1.Recycle1_FNCL_LOB_NO"
            - name: GRP
              type: char
              width: 8
              nullable: N
              map: "Recycle1.Recycle1_GRP"
            - name: MBR_CK
              type: int
              width: 4
              nullable: N
              map: "Recycle1.Recycle1_MBR_CK"
            - name: NTWK
              type: char
              width: 12
              nullable: N
              map: "Recycle1.Recycle1_NTWK"
            - name: PROD
              type: char
              width: 8
              nullable: N
              map: "Recycle1.Recycle1_PROD"
            - name: SUBGRP
              type: char
              width: 4
              nullable: N
              map: "Recycle1.Recycle1_SUBGRP"
            - name: SUB_CK
              type: int
              width: 4
              nullable: N
              map: "Recycle1.Recycle1_SUB_CK"
            - name: CLM_ACDNT_CD
              type: char
              width: 2
              nullable: N
              map: "Recycle1.Recycle1_CLM_ACDNT_CD"
            - name: CLM_ACDNT_ST_CD
              type: char
              width: 2
              nullable: N
              map: "Recycle1.Recycle1_CLM_ACDNT_ST_CD"
            - name: CLM_ACTIVATING_BCBS_PLN_CD
              type: char
              width: 10
              nullable: N
              map: "Recycle1.Recycle1_CLM_ACTIVATING_BCBS_PLN_CD"
            - name: CLM_AGMNT_SRC_CD
              type: char
              width: 10
              nullable: N
              map: "Recycle1.Recycle1_CLM_AGMNT_SRC_CD"
            - name: CLM_BTCH_ACTN_CD
              type: char
              width: 1
              nullable: N
              map: "Recycle1.Recycle1_CLM_BTCH_ACTN_CD"
            - name: CLM_CAP_CD
              type: char
              width: 10
              nullable: N
              map: "Recycle1.Recycle1_CLM_CAP_CD"
            - name: CLM_CAT_CD
              type: char
              width: 10
              nullable: N
              map: "Recycle1.Recycle1_CLM_CAT_CD"
            - name: CLM_CHK_CYC_OVRD_CD
              type: char
              width: 1
              nullable: N
              map: "Recycle1.Recycle1_CLM_CHK_CYC_OVRD_CD"
            - name: CLM_COB_CD
              type: char
              width: 1
              nullable: N
              map: "Recycle1.Recycle1_CLM_COB_CD"
            - name: FINL_DISP_CD
              type: varchar
              width: 20
              nullable: N
              map: "Recycle1.Recycle1_FINL_DISP_CD"
            - name: CLM_INPT_METH_CD
              type: char
              width: 1
              nullable: N
              map: "Recycle1.Recycle1_CLM_INPT_METH_CD"
            - name: CLM_INPT_SRC_CD
              type: char
              width: 10
              nullable: N
              map: "Recycle1.Recycle1_CLM_INPT_SRC_CD"
            - name: CLM_IPP_CD
              type: char
              width: 10
              nullable: N
              map: "Recycle1.Recycle1_CLM_IPP_CD"
            - name: CLM_NTWK_STTUS_CD
              type: char
              width: 2
              nullable: N
              map: "Recycle1.Recycle1_CLM_NTWK_STTUS_CD"
            - name: CLM_NONPAR_PROV_PFX_CD
              type: char
              width: 4
              nullable: N
              map: "Recycle1.Recycle1_CLM_NONPAR_PROV_PFX_CD"
            - name: CLM_OTHER_BNF_CD
              type: char
              width: 1
              nullable: N
              map: "Recycle1.Recycle1_CLM_OTHER_BNF_CD"
            - name: CLM_PAYE_CD
              type: char
              width: 1
              nullable: N
              map: "Recycle1.Recycle1_CLM_PAYE_CD"
            - name: CLM_PRCS_CTL_AGNT_PFX_CD
              type: char
              width: 4
              nullable: N
              map: "Recycle1.Recycle1_CLM_PRCS_CTL_AGNT_PFX_CD"
            - name: CLM_SVC_DEFN_PFX_CD
              type: char
              width: 4
              nullable: N
              map: "Recycle1.Recycle1_CLM_SVC_DEFN_PFX_CD"
            - name: CLM_SVC_PROV_SPEC_CD
              type: char
              width: 10
              nullable: N
              map: "Recycle1.Recycle1_CLM_SVC_PROV_SPEC_CD"
            - name: CLM_SVC_PROV_TYP_CD
              type: char
              width: 10
              nullable: N
              map: "Recycle1.Recycle1_CLM_SVC_PROV_TYP_CD"
            - name: CLM_STTUS_CD
              type: char
              width: 2
              nullable: N
              map: "Recycle1.Recycle1_CLM_STTUS_CD"
            - name: CLM_SUBMT_BCBS_PLN_CD
              type: char
              width: 10
              nullable: N
              map: "Recycle1.Recycle1_CLM_SUBMT_BCBS_PLN_CD"
            - name: CLM_SUB_BCBS_PLN_CD
              type: char
              width: 10
              nullable: N
              map: "Recycle1.Recycle1_CLM_SUB_BCBS_PLN_CD"
            - name: CLM_SUBTYP_CD
              type: char
              width: 10
              nullable: N
              map: "Recycle1.Recycle1_CLM_SUBTYP_CD"
            - name: CLM_TYP_CD
              type: char
              width: 1
              nullable: N
              map: "Recycle1.Recycle1_CLM_TYP_CD"
            - name: ATCHMT_IN
              type: char
              width: 1
              nullable: N
              map: "Recycle1.Recycle1_ATCHMT_IN"
            - name: CLM_CLNCL_EDIT_CD
              type: char
              width: 1
              nullable: N
              map: "Recycle1.Recycle1_CLM_CLNCL_EDIT_CD"
            - name: COBRA_CLM_IN
              type: char
              width: 1
              nullable: N
              map: "Recycle1.Recycle1_COBRA_CLM_IN"
            - name: FIRST_PASS_IN
              type: char
              width: 1
              nullable: N
              map: "Recycle1.Recycle1_FIRST_PASS_IN"
            - name: HOST_IN
              type: char
              width: 1
              nullable: N
              map: "Recycle1.Recycle1_HOST_IN"
            - name: LTR_IN
              type: char
              width: 1
              nullable: N
              map: "Recycle1.Recycle1_LTR_IN"
            - name: MCARE_ASG_IN
              type: char
              width: 1
              nullable: N
              map: "Recycle1.Recycle1_MCARE_ASG_IN"
            - name: NOTE_IN
              type: char
              width: 1
              nullable: N
              map: "Recycle1.Recycle1_NOTE_IN"
            - name: PCA_AUDIT_IN
              type: char
              width: 1
              nullable: N
              map: "Recycle1.Recycle1_PCA_AUDIT_IN"
            - name: PCP_SUBMT_IN
              type: char
              width: 1
              nullable: N
              map: "Recycle1.Recycle1_PCP_SUBMT_IN"
            - name: PROD_OOA_IN
              type: char
              width: 1
              nullable: N
              map: "Recycle1.Recycle1_PROD_OOA_IN"
            - name: ACDNT_DT
              type: char
              width: 10
              nullable: N
              map: "Recycle1.Recycle1_ACDNT_DT"
            - name: INPT_DT
              type: char
              width: 10
              nullable: N
              map: "Recycle1.Recycle1_INPT_DT"
            - name: MBR_PLN_ELIG_DT
              type: char
              width: 10
              nullable: N
              map: "Recycle1.Recycle1_MBR_PLN_ELIG_DT"
            - name: NEXT_RVW_DT
              type: char
              width: 10
              nullable: N
              map: "Recycle1.Recycle1_NEXT_RVW_DT"
            - name: PD_DT
              type: char
              width: 10
              nullable: N
              map: "Recycle1.Recycle1_PD_DT"
            - name: PAYMT_DRAG_CYC_DT
              type: char
              width: 10
              nullable: N
              map: "Recycle1.Recycle1_PAYMT_DRAG_CYC_DT"
            - name: PRCS_DT
              type: char
              width: 10
              nullable: N
              map: "Recycle1.Recycle1_PRCS_DT"
            - name: RCVD_DT
              type: char
              width: 10
              nullable: N
              map: "Recycle1.Recycle1_RCVD_DT"
            - name: SVC_STRT_DT
              type: char
              width: 10
              nullable: N
              map: "Recycle1.Recycle1_SVC_STRT_DT"
            - name: SVC_END_DT
              type: char
              width: 10
              nullable: N
              map: "Recycle1.Recycle1_SVC_END_DT"
            - name: SMLR_ILNS_DT
              type: char
              width: 10
              nullable: N
              map: "Recycle1.Recycle1_SMLR_ILNS_DT"
            - name: STTUS_DT
              type: char
              width: 10
              nullable: N
              map: "Recycle1.Recycle1_STTUS_DT"
            - name: WORK_UNABLE_BEG_DT
              type: char
              width: 10
              nullable: N
              map: "Recycle1.Recycle1_WORK_UNABLE_BEG_DT"
            - name: WORK_UNABLE_END_DT
              type: char
              width: 10
              nullable: N
              map: "Recycle1.Recycle1_WORK_UNABLE_END_DT"
            - name: ACDNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Recycle1.Recycle1_ACDNT_AMT"
            - name: ACTL_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Recycle1.Recycle1_ACTL_PD_AMT"
            - name: ALLOW_AMT
              type: decimal
              width: 19
              precision: 4
              nullable: N
              map: "Recycle1.Recycle1_ALLOW_AMT"
            - name: DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Recycle1.Recycle1_DSALW_AMT"
            - name: COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Recycle1.Recycle1_COINS_AMT"
            - name: CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Recycle1.Recycle1_CNSD_CHRG_AMT"
            - name: COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Recycle1.Recycle1_COPAY_AMT"
            - name: CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Recycle1.Recycle1_CHRG_AMT"
            - name: DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Recycle1.Recycle1_DEDCT_AMT"
            - name: PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Recycle1.Recycle1_PAYBL_AMT"
            - name: CLM_CT
              type: smallint
              width: 2
              nullable: N
              map: "Recycle1.Recycle1_CLM_CT"
            - name: MBR_AGE
              type: decimal
              width: 3
              nullable: N
              map: "Recycle1.Recycle1_MBR_AGE"
            - name: ADJ_FROM_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "Recycle1.Recycle1_ADJ_FROM_CLM_ID"
            - name: ADJ_TO_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "Recycle1.Recycle1_ADJ_TO_CLM_ID"
            - name: DOC_TX_ID
              type: char
              width: 18
              nullable: N
              map: "Recycle1.Recycle1_DOC_TX_ID"
            - name: MCAID_RESUB_NO
              type: char
              width: 15
              map: "Recycle1.Recycle1_MCAID_RESUB_NO"
            - name: MCARE_ID
              type: char
              width: 12
              nullable: N
              map: "Recycle1.Recycle1_MCARE_ID"
            - name: MBR_SFX_NO
              type: char
              width: 2
              map: "Recycle1.Recycle1_MBR_SFX_NO"
            - name: PATN_ACCT_NO
              type: varchar
              width: 20
              map: "Recycle1.Recycle1_PATN_ACCT_NO"
            - name: PAYMT_REF_ID
              type: char
              width: 16
              nullable: N
              map: "Recycle1.Recycle1_PAYMT_REF_ID"
            - name: PROV_AGMNT_ID
              type: char
              width: 12
              nullable: N
              map: "Recycle1.Recycle1_PROV_AGMNT_ID"
            - name: RFRNG_PROV_TX
              type: varchar
              width: 70
              map: "Recycle1.Recycle1_RFRNG_PROV_TX"
            - name: SUB_ID
              type: char
              width: 14
              nullable: N
              map: "Recycle1.Recycle1_SUB_ID"
            - name: PRPR_ENTITY
              type: char
              width: 1
              map: "Recycle1.Recycle1_PRPR_ENTITY"
            - name: PCA_TYP_CD
              type: char
              width: 18
              nullable: N
              map: "Recycle1.Recycle1_PCA_TYP_CD"
            - name: REL_PCA_CLM_ID
              type: varchar
              width: 18
              nullable: N
              map: "Recycle1.Recycle1_REL_PCA_CLM_ID"
            - name: CLCL_MICRO_ID
              type: char
              width: 18
              nullable: N
              map: "Recycle1.Recycle1_CLCL_MICRO_ID"
            - name: CLM_UPDT_SW
              type: char
              width: 1
              nullable: N
              map: "Recycle1.Recycle1_CLM_UPDT_SW"
            - name: REMIT_SUPRSION_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Recycle1.Recycle1_REMIT_SUPRSION_AMT"
            - name: MCAID_STTUS_ID
              type: varchar
              width: 20
              nullable: N
              map: "Recycle1.Recycle1_MCAID_STTUS_ID"
            - name: PATN_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Recycle1.Recycle1_PATN_PD_AMT"
            - name: CLM_SUBMT_ICD_VRSN_CD
              type: varchar
              width: 20
              nullable: N
              map: "Recycle1.Recycle1_CLM_SUBMT_ICD_VRSN_CD"
            - name: CLM_TXNMY_CD
              type: varchar
              width: 20
              map: "Recycle1.Recycle1_CLM_TXNMY_CD"
        - name: Trns1.V0S20P19
          type: map
          source: Trns1.V0S20P19.filter
          alias: ClmRvversalPaidDtUpdate
          schema: 
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "ClmRvversalPaidDtUpdate.ClmRvversalPaidDtUpdate_SRC_SYS_CD_SK"
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: "ClmRvversalPaidDtUpdate.ClmRvversalPaidDtUpdate_CLM_SK"
            - name: CLM_ID
              type: char
              width: 20
              nullable: N
              map: "ClmRvversalPaidDtUpdate.ClmRvversalPaidDtUpdate_CLM_ID"
            - name: CLM_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "ClmRvversalPaidDtUpdate.ClmRvversalPaidDtUpdate_CLM_STTUS_CD_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "ClmRvversalPaidDtUpdate.ClmRvversalPaidDtUpdate_LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: PD_DT_SK
              type: char
              width: 10
              nullable: N
              map: "ClmRvversalPaidDtUpdate.ClmRvversalPaidDtUpdate_PD_DT_SK"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmRvversalPaidDtUpdate.ClmRvversalPaidDtUpdate_SRC_SYS_CD"
            - name: STTUS_DT
              type: char
              width: 10
              nullable: N
              map: "ClmRvversalPaidDtUpdate.ClmRvversalPaidDtUpdate_STTUS_DT"
        - name: Trns1.V0S20P22
          type: map
          source: Trns1.V0S20P22.filter
          alias: Recycle_Clms
          schema: 
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "Recycle_Clms.Recycle_Clms_SRC_SYS_CD"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "Recycle_Clms.Recycle_Clms_CLM_ID"
        - name: Trans7
          type: lookup
          option:
            joinType: left
          sources: 
            - name: Trans6.V0S198P3
              alias: Link2
              joinFilter: ""
              joinExpression: []
            - name: PCA_Claims
              alias: baseid
              joinFilter: ""
              joinExpression:
              - fromExpr: "baseid.SRC_SYS_CD_SK"
                toExpr: "Link2.SRC_SYS_CD_SK"
                caseInSensitive: true
              - fromExpr: "baseid.CLM_ID"
                toExpr: "Link2.CLCL_MICRO_ID"
                caseInSensitive: true
          joinCondition: 
            - expression: "baseid.SRC_SYS_CD_SK = Link2.SRC_SYS_CD_SK"
              caseInSensitive: true
            - expression: "baseid.CLM_ID = Link2.CLCL_MICRO_ID"
              caseInSensitive: true
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: "Link2.CLM_SK"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link2.SRC_SYS_CD_SK"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "Link2.CLM_ID"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "Link2.CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "Link2.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: ADJ_FROM_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "Link2.ADJ_FROM_CLM_SK"
            - name: ADJ_TO_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "Link2.ADJ_TO_CLM_SK"
            - name: ALPHA_PFX_SK
              type: int
              width: 4
              nullable: N
              map: "Link2.ALPHA_PFX_SK"
            - name: CLM_EOB_EXCD_SK
              type: int
              width: 4
              nullable: N
              map: "Link2.CLM_EOB_EXCD_SK"
            - name: CLS_SK
              type: int
              width: 4
              nullable: N
              map: "Link2.CLS_SK"
            - name: CLS_PLN_SK
              type: int
              width: 4
              nullable: N
              map: "Link2.CLS_PLN_SK"
            - name: EXPRNC_CAT_SK
              type: int
              width: 4
              nullable: N
              map: "Link2.EXPRNC_CAT_SK"
            - name: FNCL_LOB_SK
              type: int
              width: 4
              nullable: N
              map: "Link2.FNCL_LOB_SK"
            - name: GRP_SK
              type: int
              width: 4
              nullable: N
              map: "Link2.GRP_SK"
            - name: MBR_SK
              type: int
              width: 4
              nullable: N
              map: "Link2.MBR_SK"
            - name: NTWK_SK
              type: int
              width: 4
              nullable: N
              map: "Link2.NTWK_SK"
            - name: PROD_SK
              type: int
              width: 4
              nullable: N
              map: "Link2.PROD_SK"
            - name: SUBGRP_SK
              type: int
              width: 4
              nullable: N
              map: "Link2.SUBGRP_SK"
            - name: SUB_SK
              type: int
              width: 4
              nullable: N
              map: "Link2.SUB_SK"
            - name: CLM_ACDNT_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link2.CLM_ACDNT_CD_SK"
            - name: CLM_ACDNT_ST_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link2.CLM_ACDNT_ST_CD_SK"
            - name: CLM_ACTV_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link2.CLM_ACTV_BCBS_PLN_CD_SK"
            - name: CLM_AGMNT_SRC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link2.CLM_AGMNT_SRC_CD_SK"
            - name: CLM_BTCH_ACTN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link2.CLM_BTCH_ACTN_CD_SK"
            - name: CLM_CAP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link2.CLM_CAP_CD_SK"
            - name: CLM_CAT_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link2.CLM_CAT_CD_SK"
            - name: CLM_CHK_CYC_OVRD_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link2.CLM_CHK_CYC_OVRD_CD_SK"
            - name: CLM_COB_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link2.CLM_COB_CD_SK"
            - name: CLM_FINL_DISP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link2.CLM_FINL_DISP_CD_SK"
            - name: CLM_INPT_METH_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link2.CLM_INPT_METH_CD_SK"
            - name: CLM_INPT_SRC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link2.CLM_INPT_SRC_CD_SK"
            - name: CLM_INTER_PLN_PGM_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link2.CLM_INTER_PLN_PGM_CD_SK"
            - name: CLM_NTWK_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link2.CLM_NTWK_STTUS_CD_SK"
            - name: CLM_NONPAR_PROV_PFX_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link2.CLM_NONPAR_PROV_PFX_CD_SK"
            - name: CLM_OTHR_BNF_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link2.CLM_OTHR_BNF_CD_SK"
            - name: CLM_PAYE_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link2.CLM_PAYE_CD_SK"
            - name: CLM_PRCS_CTL_AGNT_PFX_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link2.CLM_PRCS_CTL_AGNT_PFX_CD_SK"
            - name: CLM_SVC_DEFN_PFX_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link2.CLM_SVC_DEFN_PFX_CD_SK"
            - name: CLM_SVC_PROV_SPEC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link2.CLM_SVC_PROV_SPEC_CD_SK"
            - name: CLM_SVC_PROV_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link2.CLM_SVC_PROV_TYP_CD_SK"
            - name: CLM_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link2.CLM_STTUS_CD_SK"
            - name: CLM_SUBMTTING_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link2.CLM_SUBMTTING_BCBS_PLN_CD_SK"
            - name: CLM_SUB_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link2.CLM_SUB_BCBS_PLN_CD_SK"
            - name: CLM_SUBTYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link2.CLM_SUBTYP_CD_SK"
            - name: CLM_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link2.CLM_TYP_CD_SK"
            - name: ATCHMT_IN
              type: char
              width: 1
              nullable: N
              map: "Link2.ATCHMT_IN"
            - name: CLNCL_EDIT_IN
              type: char
              width: 1
              nullable: N
              map: "Link2.CLNCL_EDIT_IN"
            - name: COBRA_CLM_IN
              type: char
              width: 1
              nullable: N
              map: "Link2.COBRA_CLM_IN"
            - name: FIRST_PASS_IN
              type: char
              width: 1
              nullable: N
              map: "Link2.FIRST_PASS_IN"
            - name: HOST_IN
              type: char
              width: 1
              nullable: N
              map: "Link2.HOST_IN"
            - name: LTR_IN
              type: char
              width: 1
              nullable: N
              map: "Link2.LTR_IN"
            - name: MCARE_ASG_IN
              type: char
              width: 1
              nullable: N
              map: "Link2.MCARE_ASG_IN"
            - name: NOTE_IN
              type: char
              width: 1
              nullable: N
              map: "Link2.NOTE_IN"
            - name: PCA_AUDIT_IN
              type: char
              width: 1
              nullable: N
              map: "Link2.PCA_AUDIT_IN"
            - name: PCP_SUBMT_IN
              type: char
              width: 1
              nullable: N
              map: "Link2.PCP_SUBMT_IN"
            - name: PROD_OOA_IN
              type: char
              width: 1
              nullable: N
              map: "Link2.PROD_OOA_IN"
            - name: ACDNT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Link2.ACDNT_DT_SK"
            - name: INPT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Link2.INPT_DT_SK"
            - name: MBR_PLN_ELIG_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Link2.MBR_PLN_ELIG_DT_SK"
            - name: NEXT_RVW_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Link2.NEXT_RVW_DT_SK"
            - name: PD_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Link2.PD_DT_SK"
            - name: PAYMT_DRAG_CYC_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Link2.PAYMT_DRAG_CYC_DT_SK"
            - name: PRCS_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Link2.PRCS_DT_SK"
            - name: RCVD_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Link2.RCVD_DT_SK"
            - name: SVC_STRT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Link2.SVC_STRT_DT_SK"
            - name: SVC_END_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Link2.SVC_END_DT_SK"
            - name: SMLR_ILNS_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Link2.SMLR_ILNS_DT_SK"
            - name: STTUS_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Link2.STTUS_DT_SK"
            - name: WORK_UNABLE_BEG_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Link2.WORK_UNABLE_BEG_DT_SK"
            - name: WORK_UNABLE_END_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Link2.WORK_UNABLE_END_DT_SK"
            - name: ACDNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Link2.ACDNT_AMT"
            - name: ACTL_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Link2.ACTL_PD_AMT"
            - name: ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Link2.ALW_AMT"
            - name: DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Link2.DSALW_AMT"
            - name: COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Link2.COINS_AMT"
            - name: CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Link2.CNSD_CHRG_AMT"
            - name: COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Link2.COPAY_AMT"
            - name: CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Link2.CHRG_AMT"
            - name: DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Link2.DEDCT_AMT"
            - name: PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Link2.PAYBL_AMT"
            - name: CLM_CT
              type: smallint
              width: 2
              nullable: N
              map: "Link2.CLM_CT"
            - name: MBR_AGE
              type: decimal
              width: 3
              map: "Link2.MBR_AGE"
            - name: ADJ_FROM_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "Link2.ADJ_FROM_CLM_ID"
            - name: ADJ_TO_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "Link2.ADJ_TO_CLM_ID"
            - name: DOC_TX_ID
              type: char
              width: 18
              nullable: N
              map: "Link2.DOC_TX_ID"
            - name: MCAID_RESUB_NO
              type: char
              width: 15
              map: "Link2.MCAID_RESUB_NO"
            - name: MCARE_ID
              type: char
              width: 12
              nullable: N
              map: "Link2.MCARE_ID"
            - name: MBR_SFX_NO
              type: char
              width: 2
              map: "Link2.MBR_SFX_NO"
            - name: PATN_ACCT_NO
              type: varchar
              width: 35
              map: "Link2.PATN_ACCT_NO"
            - name: PAYMT_REF_ID
              type: char
              width: 16
              nullable: N
              map: "Link2.PAYMT_REF_ID"
            - name: PROV_AGMNT_ID
              type: char
              width: 12
              nullable: N
              map: "Link2.PROV_AGMNT_ID"
            - name: RFRNG_PROV_TX
              type: varchar
              width: 70
              map: "Link2.RFRNG_PROV_TX"
            - name: SUB_ID
              type: char
              width: 14
              nullable: N
              map: "Link2.SUB_ID"
            - name: PCA_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Link2.PCA_TYP_CD_SK"
            - name: REL_PCA_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "Link2.REL_PCA_CLM_SK"
            - name: REL_BASE_CLM_SK
              type: int
              width: 4
              nullable: N
              map: " If IsNull(baseid.CLM_SK) = @FALSE then baseid.CLM_SK else Link2.REL_BASE_CLM_SK"
            - name: CLM_UPDT_SW
              type: char
              width: 1
              nullable: N
              map: "Link2.CLM_UPDT_SW"
            - name: REMIT_SUPRSION_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Link2.REMIT_SUPRSION_AMT"
            - name: MCAID_STTUS_ID
              type: varchar
              width: 20
              nullable: N
              map: "Link2.MCAID_STTUS_ID"
            - name: PATN_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Link2.PATN_PD_AMT"
            - name: CLM_SUBMT_ICD_VRSN_CD_SK
              type: float
              nullable: N
              map: "Link2.CLM_SUBMT_ICD_VRSN_CD_SK"
            - name: CLM_TXNMY_CD
              type: varchar
              width: 20
              map: "Link2.CLM_TXNMY_CD"
            - name: BILL_PAYMT_EXCL_IN
              type: char
              width: 1
              nullable: N
              map: "Link2.BILL_PAYMT_EXCL_IN"
            - name: ADJ_FROM_CLM
              type: char
              width: 12
              nullable: N
              map: "Link2.ADJ_FROM_CLM"
            - name: ADJ_TO_CLM
              type: char
              width: 12
              nullable: N
              map: "Link2.ADJ_TO_CLM"
        - name: Tfrm_SrcSK
          type: lookup
          option:
            joinType: left
          sources: 
            - name: Trans3.V0S141P8
              alias: Lnk_Adj_From_TO
              joinFilter: ""
              joinExpression: []
            - name: IDS_K_CLM_FROM
              alias: Lkp_SK_FROM
              joinFilter: ""
              joinExpression:
              - fromExpr: "Lkp_SK_FROM.SRC_SYS_CD_SK"
                toExpr: "Lnk_Adj_From_TO.SRC_SYS_CD_SK_FROM"
                caseInSensitive: true
              - fromExpr: "Lkp_SK_FROM.CLM_ID"
                toExpr: "Lnk_Adj_From_TO.ADJ_FROM_CLM"
                caseInSensitive: true
            - name: IDS_K_CLM_TO
              alias: Lkp_SK_TO
              joinFilter: ""
              joinExpression:
              - fromExpr: "Lkp_SK_TO.SRC_SYS_CD_SK"
                toExpr: "Lnk_Adj_From_TO.SRC_SYS_CD_SK_TO"
                caseInSensitive: true
              - fromExpr: "Lkp_SK_TO.CLM_ID"
                toExpr: "Lnk_Adj_From_TO.ADJ_TO_CLM"
                caseInSensitive: true
          joinCondition: 
            - expression: "Lkp_SK_FROM.SRC_SYS_CD_SK = Lnk_Adj_From_TO.SRC_SYS_CD_SK_FROM"
              caseInSensitive: true
            - expression: "Lkp_SK_FROM.CLM_ID = Lnk_Adj_From_TO.ADJ_FROM_CLM"
              caseInSensitive: true
            - expression: "Lkp_SK_TO.SRC_SYS_CD_SK = Lnk_Adj_From_TO.SRC_SYS_CD_SK_TO"
              caseInSensitive: true
            - expression: "Lkp_SK_TO.CLM_ID = Lnk_Adj_From_TO.ADJ_TO_CLM"
              caseInSensitive: true
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_Adj_From_TO.CLM_SK"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_Adj_From_TO.SRC_SYS_CD_SK"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "Lnk_Adj_From_TO.CLM_ID"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_Adj_From_TO.CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_Adj_From_TO.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: ADJ_FROM_CLM_SK
              type: int
              width: 4
              nullable: N
              map: " If IsNull(Lkp_SK_FROM.CLM_SK) Then 1 Else  If Lnk_Adj_From_TO.CLM_ID = 'UNK' Then 0 Else  If Lnk_Adj_From_TO.CLM_ID = 'NA' then 1 Else Lkp_SK_FROM.CLM_SK"
            - name: ADJ_TO_CLM_SK
              type: int
              width: 4
              nullable: N
              map: " If IsNull(Lkp_SK_TO.CLM_SK) Then 1 Else  If Lnk_Adj_From_TO.CLM_ID = 'UNK' Then 0 Else  If Lnk_Adj_From_TO.CLM_ID = 'NA' then 1 Else Lkp_SK_TO.CLM_SK"
            - name: ALPHA_PFX_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_Adj_From_TO.ALPHA_PFX_SK"
            - name: CLM_EOB_EXCD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_Adj_From_TO.CLM_EOB_EXCD_SK"
            - name: CLS_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_Adj_From_TO.CLS_SK"
            - name: CLS_PLN_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_Adj_From_TO.CLS_PLN_SK"
            - name: EXPRNC_CAT_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_Adj_From_TO.EXPRNC_CAT_SK"
            - name: FNCL_LOB_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_Adj_From_TO.FNCL_LOB_SK"
            - name: GRP_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_Adj_From_TO.GRP_SK"
            - name: MBR_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_Adj_From_TO.MBR_SK"
            - name: NTWK_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_Adj_From_TO.NTWK_SK"
            - name: PROD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_Adj_From_TO.PROD_SK"
            - name: SUBGRP_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_Adj_From_TO.SUBGRP_SK"
            - name: SUB_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_Adj_From_TO.SUB_SK"
            - name: CLM_ACDNT_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_Adj_From_TO.CLM_ACDNT_CD_SK"
            - name: CLM_ACDNT_ST_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_Adj_From_TO.CLM_ACDNT_ST_CD_SK"
            - name: CLM_ACTV_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_Adj_From_TO.CLM_ACTV_BCBS_PLN_CD_SK"
            - name: CLM_AGMNT_SRC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_Adj_From_TO.CLM_AGMNT_SRC_CD_SK"
            - name: CLM_BTCH_ACTN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_Adj_From_TO.CLM_BTCH_ACTN_CD_SK"
            - name: CLM_CAP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_Adj_From_TO.CLM_CAP_CD_SK"
            - name: CLM_CAT_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_Adj_From_TO.CLM_CAT_CD_SK"
            - name: CLM_CHK_CYC_OVRD_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_Adj_From_TO.CLM_CHK_CYC_OVRD_CD_SK"
            - name: CLM_COB_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_Adj_From_TO.CLM_COB_CD_SK"
            - name: CLM_FINL_DISP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_Adj_From_TO.CLM_FINL_DISP_CD_SK"
            - name: CLM_INPT_METH_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_Adj_From_TO.CLM_INPT_METH_CD_SK"
            - name: CLM_INPT_SRC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_Adj_From_TO.CLM_INPT_SRC_CD_SK"
            - name: CLM_INTER_PLN_PGM_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_Adj_From_TO.CLM_INTER_PLN_PGM_CD_SK"
            - name: CLM_NTWK_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_Adj_From_TO.CLM_NTWK_STTUS_CD_SK"
            - name: CLM_NONPAR_PROV_PFX_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_Adj_From_TO.CLM_NONPAR_PROV_PFX_CD_SK"
            - name: CLM_OTHR_BNF_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_Adj_From_TO.CLM_OTHR_BNF_CD_SK"
            - name: CLM_PAYE_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_Adj_From_TO.CLM_PAYE_CD_SK"
            - name: CLM_PRCS_CTL_AGNT_PFX_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_Adj_From_TO.CLM_PRCS_CTL_AGNT_PFX_CD_SK"
            - name: CLM_SVC_DEFN_PFX_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_Adj_From_TO.CLM_SVC_DEFN_PFX_CD_SK"
            - name: CLM_SVC_PROV_SPEC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_Adj_From_TO.CLM_SVC_PROV_SPEC_CD_SK"
            - name: CLM_SVC_PROV_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_Adj_From_TO.CLM_SVC_PROV_TYP_CD_SK"
            - name: CLM_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_Adj_From_TO.CLM_STTUS_CD_SK"
            - name: CLM_SUBMTTING_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_Adj_From_TO.CLM_SUBMTTING_BCBS_PLN_CD_SK"
            - name: CLM_SUB_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_Adj_From_TO.CLM_SUB_BCBS_PLN_CD_SK"
            - name: CLM_SUBTYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_Adj_From_TO.CLM_SUBTYP_CD_SK"
            - name: CLM_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_Adj_From_TO.CLM_TYP_CD_SK"
            - name: ATCHMT_IN
              type: char
              width: 1
              nullable: N
              map: "Lnk_Adj_From_TO.ATCHMT_IN"
            - name: CLNCL_EDIT_IN
              type: char
              width: 1
              nullable: N
              map: "Lnk_Adj_From_TO.CLNCL_EDIT_IN"
            - name: COBRA_CLM_IN
              type: char
              width: 1
              nullable: N
              map: "Lnk_Adj_From_TO.COBRA_CLM_IN"
            - name: FIRST_PASS_IN
              type: char
              width: 1
              nullable: N
              map: "Lnk_Adj_From_TO.FIRST_PASS_IN"
            - name: HOST_IN
              type: char
              width: 1
              nullable: N
              map: "Lnk_Adj_From_TO.HOST_IN"
            - name: LTR_IN
              type: char
              width: 1
              nullable: N
              map: "Lnk_Adj_From_TO.LTR_IN"
            - name: MCARE_ASG_IN
              type: char
              width: 1
              nullable: N
              map: "Lnk_Adj_From_TO.MCARE_ASG_IN"
            - name: NOTE_IN
              type: char
              width: 1
              nullable: N
              map: "Lnk_Adj_From_TO.NOTE_IN"
            - name: PCA_AUDIT_IN
              type: char
              width: 1
              nullable: N
              map: "Lnk_Adj_From_TO.PCA_AUDIT_IN"
            - name: PCP_SUBMT_IN
              type: char
              width: 1
              nullable: N
              map: "Lnk_Adj_From_TO.PCP_SUBMT_IN"
            - name: PROD_OOA_IN
              type: char
              width: 1
              nullable: N
              map: "Lnk_Adj_From_TO.PROD_OOA_IN"
            - name: ACDNT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Lnk_Adj_From_TO.ACDNT_DT_SK"
            - name: INPT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Lnk_Adj_From_TO.INPT_DT_SK"
            - name: MBR_PLN_ELIG_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Lnk_Adj_From_TO.MBR_PLN_ELIG_DT_SK"
            - name: NEXT_RVW_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Lnk_Adj_From_TO.NEXT_RVW_DT_SK"
            - name: PD_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Lnk_Adj_From_TO.PD_DT_SK"
            - name: PAYMT_DRAG_CYC_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Lnk_Adj_From_TO.PAYMT_DRAG_CYC_DT_SK"
            - name: PRCS_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Lnk_Adj_From_TO.PRCS_DT_SK"
            - name: RCVD_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Lnk_Adj_From_TO.RCVD_DT_SK"
            - name: SVC_STRT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Lnk_Adj_From_TO.SVC_STRT_DT_SK"
            - name: SVC_END_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Lnk_Adj_From_TO.SVC_END_DT_SK"
            - name: SMLR_ILNS_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Lnk_Adj_From_TO.SMLR_ILNS_DT_SK"
            - name: STTUS_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Lnk_Adj_From_TO.STTUS_DT_SK"
            - name: WORK_UNABLE_BEG_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Lnk_Adj_From_TO.WORK_UNABLE_BEG_DT_SK"
            - name: WORK_UNABLE_END_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Lnk_Adj_From_TO.WORK_UNABLE_END_DT_SK"
            - name: ACDNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_Adj_From_TO.ACDNT_AMT"
            - name: ACTL_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_Adj_From_TO.ACTL_PD_AMT"
            - name: ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_Adj_From_TO.ALW_AMT"
            - name: DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_Adj_From_TO.DSALW_AMT"
            - name: COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_Adj_From_TO.COINS_AMT"
            - name: CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_Adj_From_TO.CNSD_CHRG_AMT"
            - name: COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_Adj_From_TO.COPAY_AMT"
            - name: CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_Adj_From_TO.CHRG_AMT"
            - name: DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_Adj_From_TO.DEDCT_AMT"
            - name: PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_Adj_From_TO.PAYBL_AMT"
            - name: CLM_CT
              type: smallint
              width: 2
              nullable: N
              map: "Lnk_Adj_From_TO.CLM_CT"
            - name: MBR_AGE
              type: decimal
              width: 3
              map: "Lnk_Adj_From_TO.MBR_AGE"
            - name: ADJ_FROM_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "Lnk_Adj_From_TO.ADJ_FROM_CLM_ID"
            - name: ADJ_TO_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "Lnk_Adj_From_TO.ADJ_TO_CLM_ID"
            - name: DOC_TX_ID
              type: char
              width: 18
              nullable: N
              map: "Lnk_Adj_From_TO.DOC_TX_ID"
            - name: MCAID_RESUB_NO
              type: char
              width: 15
              map: "Lnk_Adj_From_TO.MCAID_RESUB_NO"
            - name: MCARE_ID
              type: char
              width: 12
              nullable: N
              map: "Lnk_Adj_From_TO.MCARE_ID"
            - name: MBR_SFX_NO
              type: char
              width: 2
              map: "Lnk_Adj_From_TO.MBR_SFX_NO"
            - name: PATN_ACCT_NO
              type: varchar
              width: 35
              map: "Lnk_Adj_From_TO.PATN_ACCT_NO"
            - name: PAYMT_REF_ID
              type: char
              width: 16
              nullable: N
              map: "Lnk_Adj_From_TO.PAYMT_REF_ID"
            - name: PROV_AGMNT_ID
              type: char
              width: 12
              nullable: N
              map: "Lnk_Adj_From_TO.PROV_AGMNT_ID"
            - name: RFRNG_PROV_TX
              type: varchar
              width: 70
              map: "Lnk_Adj_From_TO.RFRNG_PROV_TX"
            - name: SUB_ID
              type: char
              width: 14
              nullable: N
              map: "Lnk_Adj_From_TO.SUB_ID"
            - name: PCA_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_Adj_From_TO.PCA_TYP_CD_SK"
            - name: REL_PCA_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_Adj_From_TO.REL_PCA_CLM_SK"
            - name: REL_BASE_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_Adj_From_TO.REL_BASE_CLM_SK"
            - name: REMIT_SUPRSION_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_Adj_From_TO.REMIT_SUPRSION_AMT"
            - name: MCAID_STTUS_ID
              type: varchar
              width: 20
              nullable: N
              map: "Lnk_Adj_From_TO.MCAID_STTUS_ID"
            - name: PATN_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_Adj_From_TO.PATN_PD_AMT"
            - name: CLM_SUBMT_ICD_VRSN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_Adj_From_TO.CLM_SUBMT_ICD_VRSN_CD_SK"
            - name: CLM_TXNMY_CD
              type: varchar
              width: 20
              map: "Lnk_Adj_From_TO.CLM_TXNMY_CD"
            - name: BILL_PAYMT_EXCL_IN
              type: char
              width: 1
              nullable: N
              map: "Lnk_Adj_From_TO.BILL_PAYMT_EXCL_IN"
        - name: Trans2.lookup
          type: lookup
          stageVars:
            - name: svClmMnths
              type: decimal
              width: 0
              map: " If IsNull(ClmRvversalPaidDtUpdate.STTUS_DT) Or Len(Trim(ClmRvversalPaidDtUpdate.STTUS_DT)) <= 0 Then 1 Else MonthDiff(ClmRvversalPaidDtUpdate.STTUS_DT, FORMAT.DATE(@Date, 'DATE', 'CURRENT', 'CCYY-MM-DD'))"
              initValue: "0"
            - name: UpdateAdjFromFound
              type: decimal
              width: 0
              map: " If (IsNull(Clm1.CLM_SK) = @FALSE) and (Len(Trim(Clm1.CLM_ID)) > 0) and (Clm1.CLM_STTUS_CD_SK = GetFkeyCodes(\"FACETS\", ClmRvversalPaidDtUpdate.CLM_SK, \"CLAIM STATUS\", \"R\", \"X\")) then @TRUE else @FALSE and Trim(Clm1.PD_DT_SK) = \"NA\""
              initValue: "0"
          option:
            joinType: left
          sources: 
            - name: Trns1.V0S20P19
              alias: ClmRvversalPaidDtUpdate
              joinFilter: ""
              joinExpression: []
            - name: IDSClm_for_reversals
              alias: Clm1
              joinFilter: ""
              joinExpression:
              - fromExpr: "Clm1.SRC_SYS_CD_SK"
                toExpr: "ClmRvversalPaidDtUpdate.SRC_SYS_CD_SK"
                caseInSensitive: true
              - fromExpr: "Clm1.CLM_SK"
                toExpr: "ClmRvversalPaidDtUpdate.CLM_SK"
                caseInSensitive: true
              - fromExpr: "Clm1.CLM_ID"
                toExpr: "ClmRvversalPaidDtUpdate.CLM_ID"
                caseInSensitive: true
          joinCondition: 
            - expression: "Clm1.SRC_SYS_CD_SK = ClmRvversalPaidDtUpdate.SRC_SYS_CD_SK"
              caseInSensitive: true
            - expression: "Clm1.CLM_SK = ClmRvversalPaidDtUpdate.CLM_SK"
              caseInSensitive: true
            - expression: "Clm1.CLM_ID = ClmRvversalPaidDtUpdate.CLM_ID"
              caseInSensitive: true
          schema: 
            - name: ClmRvversalUpdates_SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "ClmRvversalPaidDtUpdate.SRC_SYS_CD_SK"
            - name: ClmRvversalUpdates_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "ClmRvversalPaidDtUpdate.CLM_SK"
            - name: ClmRvversalUpdates_CLM_ID
              type: char
              width: 20
              nullable: N
              map: "ClmRvversalPaidDtUpdate.CLM_ID"
            - name: ClmRvversalUpdates_CLM_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "ClmRvversalPaidDtUpdate.CLM_STTUS_CD_SK"
            - name: ClmRvversalUpdates_LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: RunCycle
            - name: ClmRvversalUpdates_PD_DT_SK
              type: char
              width: 10
              nullable: N
              map: "ClmRvversalPaidDtUpdate.PD_DT_SK"
            - name: EdwRevrslClmHitList_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "ClmRvversalPaidDtUpdate.CLM_SK"
            - name: EdwRevrslClmHitList_SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "ClmRvversalPaidDtUpdate.SRC_SYS_CD_SK"
            - name: EdwRevrslClmHitList_CLM_ID
              type: char
              width: 20
              nullable: N
              map: "ClmRvversalPaidDtUpdate.CLM_ID"
            - name: filter_ClmRvversalUpdates
              type: smallint
              width: 2
              nullable: N
              map: "if (UpdateAdjFromFound = @TRUE) then 1 else 0"
        - name: Trans2.V25S1P3.filter
          type: filter
          source: Trans2.lookup
          filter: "filter_ClmRvversalUpdates = 1"
          schema: 
            - name: ClmRvversalUpdates_SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: ClmRvversalUpdates_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: ClmRvversalUpdates_CLM_ID
              type: char
              width: 20
              nullable: N
            - name: ClmRvversalUpdates_CLM_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: ClmRvversalUpdates_LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: ClmRvversalUpdates_PD_DT_SK
              type: char
              width: 10
              nullable: N
        - name: Trans2.V25S1P3
          type: map
          source: Trans2.V25S1P3.filter
          alias: ClmRvversalUpdates
          schema: 
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "ClmRvversalUpdates.ClmRvversalUpdates_SRC_SYS_CD_SK"
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: "ClmRvversalUpdates.ClmRvversalUpdates_CLM_SK"
            - name: CLM_ID
              type: char
              width: 20
              nullable: N
              map: "ClmRvversalUpdates.ClmRvversalUpdates_CLM_ID"
            - name: CLM_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "ClmRvversalUpdates.ClmRvversalUpdates_CLM_STTUS_CD_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "ClmRvversalUpdates.ClmRvversalUpdates_LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: PD_DT_SK
              type: char
              width: 10
              nullable: N
              map: "ClmRvversalUpdates.ClmRvversalUpdates_PD_DT_SK"
        - name: Trans2.V25S1P10
          type: map
          source: Trans2.lookup
          alias: EdwRevrslClmHitList
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: "EdwRevrslClmHitList.EdwRevrslClmHitList_CLM_SK"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "EdwRevrslClmHitList.EdwRevrslClmHitList_SRC_SYS_CD_SK"
            - name: CLM_ID
              type: char
              width: 20
              nullable: N
              map: "EdwRevrslClmHitList.EdwRevrslClmHitList_CLM_ID"
        - name: Trans4.lookup
          type: lookup
          stageVars:
            - name: svSttusCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(\"NPS\", ClmNascoLkup.CLM_SK, \"CLAIM STATUS\", \"X\", \"N\")"
              initValue: "0"
            - name: svSttusDtSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyDate('IDS', ClmNascoLkup.CLM_SK, ClmNasco.CLCL_LAST_ACT_DTM, 'X')"
              initValue: "0"
            - name: UpdateAdjFromFound
              type: decimal
              width: 0
              map: " If IsNull(ClmNascoLkup.CLM_SK) = @TRUE then 'N' else 'Y'"
              initValue: "0"
          option:
            joinType: left
          sources: 
            - name: nasco_orig
              alias: ClmNasco
              joinFilter: ""
              joinExpression: []
            - name: 
              alias: ClmNascoLkup
              joinFilter: ""
              joinExpression:
              - fromExpr: "1"
                toExpr: "1"
                caseInSensitive: true
            - name: hf_Clm_LkupHsh
              alias: refClm
              joinFilter: "refClm.SRC_SYS_CD = 'FACETS'"
              joinExpression:
              - fromExpr: "refClm.CLM_ID"
                toExpr: "ClmNasco.CLM_ID:'R'"
                caseInSensitive: true
          joinCondition: 
            - expression: refClm.SRC_SYS_CD = 'FACETS'
            - expression: "refClm.CLM_ID = ClmNasco.CLM_ID:'R'"
              caseInSensitive: true
          schema: 
            - name: ClmNascoUpdates_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "ClmNascoLkup.CLM_SK"
            - name: ClmNascoUpdates_LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: RunCycle
            - name: ClmNascoUpdates_ADJ_TO_CLM_SK
              type: int
              width: 4
              nullable: N
              map: " If IsNull(refClm.CLM_SK) = @TRUE then 0 else refClm.CLM_SK"
            - name: ClmNascoUpdates_CLM_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: svSttusCdSk
            - name: ClmNascoUpdates_ADJ_TO_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: " If IsNull(refClm.CLM_SK) = @TRUE then space(1) else refClm.CLM_ID"
            - name: EdwRevrslClmHitList_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "ClmNascoLkup.CLM_SK"
            - name: EdwRevrslClmHitList_SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "ClmNascoLkup.SRC_SYS_CD_SK"
            - name: EdwRevrslClmHitList_CLM_ID
              type: char
              width: 20
              nullable: N
              map: "ClmNascoLkup.CLM_ID"
            - name: EdwRevrslClmHitList_LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: RunCycle
            - name: filter_ClmNascoUpdates
              type: smallint
              width: 2
              nullable: N
              map: "if (UpdateAdjFromFound = 'Y') then 1 else 0"
        - name: Trans4.V26S0P3.filter
          type: filter
          source: Trans4.lookup
          filter: "filter_ClmNascoUpdates = 1"
          schema: 
            - name: ClmNascoUpdates_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: ClmNascoUpdates_LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: ClmNascoUpdates_ADJ_TO_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: ClmNascoUpdates_CLM_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: ClmNascoUpdates_ADJ_TO_CLM_ID
              type: varchar
              width: 20
              nullable: N
        - name: Trans4.V26S0P3
          type: map
          source: Trans4.V26S0P3.filter
          alias: ClmNascoUpdates
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: "ClmNascoUpdates.ClmNascoUpdates_CLM_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "ClmNascoUpdates.ClmNascoUpdates_LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: ADJ_TO_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "ClmNascoUpdates.ClmNascoUpdates_ADJ_TO_CLM_SK"
            - name: CLM_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "ClmNascoUpdates.ClmNascoUpdates_CLM_STTUS_CD_SK"
            - name: ADJ_TO_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "ClmNascoUpdates.ClmNascoUpdates_ADJ_TO_CLM_ID"
        - name: Trans4.V26S0P5
          type: map
          source: Trans4.lookup
          alias: EdwRevrslClmHitList
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: "EdwRevrslClmHitList.EdwRevrslClmHitList_CLM_SK"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "EdwRevrslClmHitList.EdwRevrslClmHitList_SRC_SYS_CD_SK"
            - name: CLM_ID
              type: char
              width: 20
              nullable: N
              map: "EdwRevrslClmHitList.EdwRevrslClmHitList_CLM_ID"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "EdwRevrslClmHitList.EdwRevrslClmHitList_LAST_UPDT_RUN_CYC_EXCTN_SK"
        - name: Trans3.V0S141P9.map
          type: map
          source: merge_load1
          alias: OutFile
          stageVars: 
            - name: svAdjTOandFROM
              type: decimal
              width: 0
              map: " If (Source = 'LUMERIS' and OutFile.ADJ_TO_CLM_ID[1, 1] <> 'L') Or (Source = 'FACETS' and OutFile.ADJ_FROM_CLM_ID[1, 1] = 'L') Then 'Y' Else 'N'"
              initValue: "0"
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_SK"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.SRC_SYS_CD_SK"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "OutFile.CLM_ID"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: ADJ_FROM_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.ADJ_FROM_CLM_SK"
            - name: ADJ_TO_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.ADJ_TO_CLM_SK"
            - name: ALPHA_PFX_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.ALPHA_PFX_SK"
            - name: CLM_EOB_EXCD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_EOB_EXCD_SK"
            - name: CLS_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLS_SK"
            - name: CLS_PLN_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLS_PLN_SK"
            - name: EXPRNC_CAT_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.EXPRNC_CAT_SK"
            - name: FNCL_LOB_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.FNCL_LOB_SK"
            - name: GRP_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.GRP_SK"
            - name: MBR_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.MBR_SK"
            - name: NTWK_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.NTWK_SK"
            - name: PROD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.PROD_SK"
            - name: SUBGRP_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.SUBGRP_SK"
            - name: SUB_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.SUB_SK"
            - name: CLM_ACDNT_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_ACDNT_CD_SK"
            - name: CLM_ACDNT_ST_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_ACDNT_ST_CD_SK"
            - name: CLM_ACTV_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_ACTV_BCBS_PLN_CD_SK"
            - name: CLM_AGMNT_SRC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_AGMNT_SRC_CD_SK"
            - name: CLM_BTCH_ACTN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_BTCH_ACTN_CD_SK"
            - name: CLM_CAP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_CAP_CD_SK"
            - name: CLM_CAT_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_CAT_CD_SK"
            - name: CLM_CHK_CYC_OVRD_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_CHK_CYC_OVRD_CD_SK"
            - name: CLM_COB_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_COB_CD_SK"
            - name: CLM_FINL_DISP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_FINL_DISP_CD_SK"
            - name: CLM_INPT_METH_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_INPT_METH_CD_SK"
            - name: CLM_INPT_SRC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_INPT_SRC_CD_SK"
            - name: CLM_INTER_PLN_PGM_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_INTER_PLN_PGM_CD_SK"
            - name: CLM_NTWK_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_NTWK_STTUS_CD_SK"
            - name: CLM_NONPAR_PROV_PFX_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_NONPAR_PROV_PFX_CD_SK"
            - name: CLM_OTHR_BNF_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_OTHR_BNF_CD_SK"
            - name: CLM_PAYE_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_PAYE_CD_SK"
            - name: CLM_PRCS_CTL_AGNT_PFX_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_PRCS_CTL_AGNT_PFX_CD_SK"
            - name: CLM_SVC_DEFN_PFX_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_SVC_DEFN_PFX_CD_SK"
            - name: CLM_SVC_PROV_SPEC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_SVC_PROV_SPEC_CD_SK"
            - name: CLM_SVC_PROV_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_SVC_PROV_TYP_CD_SK"
            - name: CLM_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_STTUS_CD_SK"
            - name: CLM_SUBMTTING_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_SUBMTTING_BCBS_PLN_CD_SK"
            - name: CLM_SUB_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_SUB_BCBS_PLN_CD_SK"
            - name: CLM_SUBTYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_SUBTYP_CD_SK"
            - name: CLM_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_TYP_CD_SK"
            - name: ATCHMT_IN
              type: char
              width: 1
              nullable: N
              map: "OutFile.ATCHMT_IN"
            - name: CLNCL_EDIT_IN
              type: char
              width: 1
              nullable: N
              map: "OutFile.CLNCL_EDIT_IN"
            - name: COBRA_CLM_IN
              type: char
              width: 1
              nullable: N
              map: "OutFile.COBRA_CLM_IN"
            - name: FIRST_PASS_IN
              type: char
              width: 1
              nullable: N
              map: "OutFile.FIRST_PASS_IN"
            - name: HOST_IN
              type: char
              width: 1
              nullable: N
              map: "OutFile.HOST_IN"
            - name: LTR_IN
              type: char
              width: 1
              nullable: N
              map: "OutFile.LTR_IN"
            - name: MCARE_ASG_IN
              type: char
              width: 1
              nullable: N
              map: "OutFile.MCARE_ASG_IN"
            - name: NOTE_IN
              type: char
              width: 1
              nullable: N
              map: "OutFile.NOTE_IN"
            - name: PCA_AUDIT_IN
              type: char
              width: 1
              nullable: N
              map: "OutFile.PCA_AUDIT_IN"
            - name: PCP_SUBMT_IN
              type: char
              width: 1
              nullable: N
              map: "OutFile.PCP_SUBMT_IN"
            - name: PROD_OOA_IN
              type: char
              width: 1
              nullable: N
              map: "OutFile.PROD_OOA_IN"
            - name: ACDNT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile.ACDNT_DT_SK"
            - name: INPT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile.INPT_DT_SK"
            - name: MBR_PLN_ELIG_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile.MBR_PLN_ELIG_DT_SK"
            - name: NEXT_RVW_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile.NEXT_RVW_DT_SK"
            - name: PD_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile.PD_DT_SK"
            - name: PAYMT_DRAG_CYC_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile.PAYMT_DRAG_CYC_DT_SK"
            - name: PRCS_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile.PRCS_DT_SK"
            - name: RCVD_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile.RCVD_DT_SK"
            - name: SVC_STRT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile.SVC_STRT_DT_SK"
            - name: SVC_END_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile.SVC_END_DT_SK"
            - name: SMLR_ILNS_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile.SMLR_ILNS_DT_SK"
            - name: STTUS_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile.STTUS_DT_SK"
            - name: WORK_UNABLE_BEG_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile.WORK_UNABLE_BEG_DT_SK"
            - name: WORK_UNABLE_END_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile.WORK_UNABLE_END_DT_SK"
            - name: ACDNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile.ACDNT_AMT"
            - name: ACTL_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile.ACTL_PD_AMT"
            - name: ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile.ALW_AMT"
            - name: DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile.DSALW_AMT"
            - name: COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile.COINS_AMT"
            - name: CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile.CNSD_CHRG_AMT"
            - name: COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile.COPAY_AMT"
            - name: CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile.CHRG_AMT"
            - name: DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile.DEDCT_AMT"
            - name: PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile.PAYBL_AMT"
            - name: CLM_CT
              type: smallint
              width: 2
              nullable: N
              map: "OutFile.CLM_CT"
            - name: MBR_AGE
              type: decimal
              width: 3
              map: "OutFile.MBR_AGE"
            - name: ADJ_FROM_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "OutFile.ADJ_FROM_CLM_ID"
            - name: ADJ_TO_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "OutFile.ADJ_TO_CLM_ID"
            - name: DOC_TX_ID
              type: char
              width: 18
              nullable: N
              map: "OutFile.DOC_TX_ID"
            - name: MCAID_RESUB_NO
              type: char
              width: 15
              map: "OutFile.MCAID_RESUB_NO"
            - name: MCARE_ID
              type: char
              width: 12
              nullable: N
              map: "OutFile.MCARE_ID"
            - name: MBR_SFX_NO
              type: char
              width: 2
              map: "OutFile.MBR_SFX_NO"
            - name: PATN_ACCT_NO
              type: varchar
              width: 35
              map: "OutFile.PATN_ACCT_NO"
            - name: PAYMT_REF_ID
              type: char
              width: 16
              nullable: N
              map: "OutFile.PAYMT_REF_ID"
            - name: PROV_AGMNT_ID
              type: char
              width: 12
              nullable: N
              map: "OutFile.PROV_AGMNT_ID"
            - name: RFRNG_PROV_TX
              type: varchar
              width: 70
              map: "OutFile.RFRNG_PROV_TX"
            - name: SUB_ID
              type: char
              width: 14
              nullable: N
              map: "OutFile.SUB_ID"
            - name: PCA_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.PCA_TYP_CD_SK"
            - name: REL_PCA_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.REL_PCA_CLM_SK"
            - name: REL_BASE_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.REL_BASE_CLM_SK"
            - name: CLM_UPDT_SW
              type: char
              width: 1
              nullable: N
              map: "OutFile.CLM_UPDT_SW"
            - name: REMIT_SUPRSION_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile.REMIT_SUPRSION_AMT"
            - name: MCAID_STTUS_ID
              type: varchar
              width: 20
              nullable: N
              map: "OutFile.MCAID_STTUS_ID"
            - name: PATN_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile.PATN_PD_AMT"
            - name: CLM_SUBMT_ICD_VRSN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_SUBMT_ICD_VRSN_CD_SK"
            - name: CLM_TXNMY_CD
              type: varchar
              width: 20
              map: "OutFile.CLM_TXNMY_CD"
            - name: BILL_PAYMT_EXCL_IN
              type: char
              width: 1
              nullable: N
              map: "OutFile.BILL_PAYMT_EXCL_IN"
            - name: ADJ_FROM_CLM
              type: char
              width: 12
              nullable: N
              map: "OutFile.ADJ_FROM_CLM"
            - name: ADJ_TO_CLM
              type: char
              width: 12
              nullable: N
              map: "OutFile.ADJ_TO_CLM"
            - name: svAdjTOandFROM
              type: decimal
              width: 0
              map: svAdjTOandFROM
              initValue: "0"
        - name: Trans3.V0S141P9
          type: filter
          source: Trans3.V0S141P9.map
          filter: "svAdjTOandFROM = 'N'"
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: ADJ_FROM_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: ADJ_TO_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: ALPHA_PFX_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_EOB_EXCD_SK
              type: int
              width: 4
              nullable: N
            - name: CLS_SK
              type: int
              width: 4
              nullable: N
            - name: CLS_PLN_SK
              type: int
              width: 4
              nullable: N
            - name: EXPRNC_CAT_SK
              type: int
              width: 4
              nullable: N
            - name: FNCL_LOB_SK
              type: int
              width: 4
              nullable: N
            - name: GRP_SK
              type: int
              width: 4
              nullable: N
            - name: MBR_SK
              type: int
              width: 4
              nullable: N
            - name: NTWK_SK
              type: int
              width: 4
              nullable: N
            - name: PROD_SK
              type: int
              width: 4
              nullable: N
            - name: SUBGRP_SK
              type: int
              width: 4
              nullable: N
            - name: SUB_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_ACDNT_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_ACDNT_ST_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_ACTV_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_AGMNT_SRC_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_BTCH_ACTN_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_CAP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_CAT_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_CHK_CYC_OVRD_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_COB_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_FINL_DISP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_INPT_METH_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_INPT_SRC_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_INTER_PLN_PGM_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_NTWK_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_NONPAR_PROV_PFX_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_OTHR_BNF_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_PAYE_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_PRCS_CTL_AGNT_PFX_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_SVC_DEFN_PFX_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_SVC_PROV_SPEC_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_SVC_PROV_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_SUBMTTING_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_SUB_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_SUBTYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: ATCHMT_IN
              type: char
              width: 1
              nullable: N
            - name: CLNCL_EDIT_IN
              type: char
              width: 1
              nullable: N
            - name: COBRA_CLM_IN
              type: char
              width: 1
              nullable: N
            - name: FIRST_PASS_IN
              type: char
              width: 1
              nullable: N
            - name: HOST_IN
              type: char
              width: 1
              nullable: N
            - name: LTR_IN
              type: char
              width: 1
              nullable: N
            - name: MCARE_ASG_IN
              type: char
              width: 1
              nullable: N
            - name: NOTE_IN
              type: char
              width: 1
              nullable: N
            - name: PCA_AUDIT_IN
              type: char
              width: 1
              nullable: N
            - name: PCP_SUBMT_IN
              type: char
              width: 1
              nullable: N
            - name: PROD_OOA_IN
              type: char
              width: 1
              nullable: N
            - name: ACDNT_DT_SK
              type: char
              width: 10
              nullable: N
            - name: INPT_DT_SK
              type: char
              width: 10
              nullable: N
            - name: MBR_PLN_ELIG_DT_SK
              type: char
              width: 10
              nullable: N
            - name: NEXT_RVW_DT_SK
              type: char
              width: 10
              nullable: N
            - name: PD_DT_SK
              type: char
              width: 10
              nullable: N
            - name: PAYMT_DRAG_CYC_DT_SK
              type: char
              width: 10
              nullable: N
            - name: PRCS_DT_SK
              type: char
              width: 10
              nullable: N
            - name: RCVD_DT_SK
              type: char
              width: 10
              nullable: N
            - name: SVC_STRT_DT_SK
              type: char
              width: 10
              nullable: N
            - name: SVC_END_DT_SK
              type: char
              width: 10
              nullable: N
            - name: SMLR_ILNS_DT_SK
              type: char
              width: 10
              nullable: N
            - name: STTUS_DT_SK
              type: char
              width: 10
              nullable: N
            - name: WORK_UNABLE_BEG_DT_SK
              type: char
              width: 10
              nullable: N
            - name: WORK_UNABLE_END_DT_SK
              type: char
              width: 10
              nullable: N
            - name: ACDNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: ACTL_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: CLM_CT
              type: smallint
              width: 2
              nullable: N
            - name: MBR_AGE
              type: decimal
              width: 3
            - name: ADJ_FROM_CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: ADJ_TO_CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: DOC_TX_ID
              type: char
              width: 18
              nullable: N
            - name: MCAID_RESUB_NO
              type: char
              width: 15
            - name: MCARE_ID
              type: char
              width: 12
              nullable: N
            - name: MBR_SFX_NO
              type: char
              width: 2
            - name: PATN_ACCT_NO
              type: varchar
              width: 35
            - name: PAYMT_REF_ID
              type: char
              width: 16
              nullable: N
            - name: PROV_AGMNT_ID
              type: char
              width: 12
              nullable: N
            - name: RFRNG_PROV_TX
              type: varchar
              width: 70
            - name: SUB_ID
              type: char
              width: 14
              nullable: N
            - name: PCA_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: REL_PCA_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: REL_BASE_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: REMIT_SUPRSION_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: MCAID_STTUS_ID
              type: varchar
              width: 20
              nullable: N
            - name: PATN_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: CLM_SUBMT_ICD_VRSN_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_TXNMY_CD
              type: varchar
              width: 20
            - name: BILL_PAYMT_EXCL_IN
              type: char
              width: 1
              nullable: N
          keys:
            - CLM_SK
        - name: Trans3.V0S141P8.map
          type: map
          source: merge_load1
          alias: OutFile
          stageVars: 
            - name: svAdjTOandFROM
              type: decimal
              width: 0
              map: " If (Source = 'LUMERIS' and OutFile.ADJ_TO_CLM_ID[1, 1] <> 'L') Or (Source = 'FACETS' and OutFile.ADJ_FROM_CLM_ID[1, 1] = 'L') Then 'Y' Else 'N'"
              initValue: "0"
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_SK"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.SRC_SYS_CD_SK"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "OutFile.CLM_ID"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: ADJ_FROM_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.ADJ_FROM_CLM_SK"
            - name: ADJ_TO_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.ADJ_TO_CLM_SK"
            - name: ALPHA_PFX_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.ALPHA_PFX_SK"
            - name: CLM_EOB_EXCD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_EOB_EXCD_SK"
            - name: CLS_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLS_SK"
            - name: CLS_PLN_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLS_PLN_SK"
            - name: EXPRNC_CAT_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.EXPRNC_CAT_SK"
            - name: FNCL_LOB_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.FNCL_LOB_SK"
            - name: GRP_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.GRP_SK"
            - name: MBR_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.MBR_SK"
            - name: NTWK_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.NTWK_SK"
            - name: PROD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.PROD_SK"
            - name: SUBGRP_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.SUBGRP_SK"
            - name: SUB_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.SUB_SK"
            - name: CLM_ACDNT_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_ACDNT_CD_SK"
            - name: CLM_ACDNT_ST_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_ACDNT_ST_CD_SK"
            - name: CLM_ACTV_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_ACTV_BCBS_PLN_CD_SK"
            - name: CLM_AGMNT_SRC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_AGMNT_SRC_CD_SK"
            - name: CLM_BTCH_ACTN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_BTCH_ACTN_CD_SK"
            - name: CLM_CAP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_CAP_CD_SK"
            - name: CLM_CAT_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_CAT_CD_SK"
            - name: CLM_CHK_CYC_OVRD_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_CHK_CYC_OVRD_CD_SK"
            - name: CLM_COB_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_COB_CD_SK"
            - name: CLM_FINL_DISP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_FINL_DISP_CD_SK"
            - name: CLM_INPT_METH_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_INPT_METH_CD_SK"
            - name: CLM_INPT_SRC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_INPT_SRC_CD_SK"
            - name: CLM_INTER_PLN_PGM_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_INTER_PLN_PGM_CD_SK"
            - name: CLM_NTWK_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_NTWK_STTUS_CD_SK"
            - name: CLM_NONPAR_PROV_PFX_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_NONPAR_PROV_PFX_CD_SK"
            - name: CLM_OTHR_BNF_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_OTHR_BNF_CD_SK"
            - name: CLM_PAYE_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_PAYE_CD_SK"
            - name: CLM_PRCS_CTL_AGNT_PFX_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_PRCS_CTL_AGNT_PFX_CD_SK"
            - name: CLM_SVC_DEFN_PFX_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_SVC_DEFN_PFX_CD_SK"
            - name: CLM_SVC_PROV_SPEC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_SVC_PROV_SPEC_CD_SK"
            - name: CLM_SVC_PROV_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_SVC_PROV_TYP_CD_SK"
            - name: CLM_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_STTUS_CD_SK"
            - name: CLM_SUBMTTING_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_SUBMTTING_BCBS_PLN_CD_SK"
            - name: CLM_SUB_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_SUB_BCBS_PLN_CD_SK"
            - name: CLM_SUBTYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_SUBTYP_CD_SK"
            - name: CLM_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_TYP_CD_SK"
            - name: ATCHMT_IN
              type: char
              width: 1
              nullable: N
              map: "OutFile.ATCHMT_IN"
            - name: CLNCL_EDIT_IN
              type: char
              width: 1
              nullable: N
              map: "OutFile.CLNCL_EDIT_IN"
            - name: COBRA_CLM_IN
              type: char
              width: 1
              nullable: N
              map: "OutFile.COBRA_CLM_IN"
            - name: FIRST_PASS_IN
              type: char
              width: 1
              nullable: N
              map: "OutFile.FIRST_PASS_IN"
            - name: HOST_IN
              type: char
              width: 1
              nullable: N
              map: "OutFile.HOST_IN"
            - name: LTR_IN
              type: char
              width: 1
              nullable: N
              map: "OutFile.LTR_IN"
            - name: MCARE_ASG_IN
              type: char
              width: 1
              nullable: N
              map: "OutFile.MCARE_ASG_IN"
            - name: NOTE_IN
              type: char
              width: 1
              nullable: N
              map: "OutFile.NOTE_IN"
            - name: PCA_AUDIT_IN
              type: char
              width: 1
              nullable: N
              map: "OutFile.PCA_AUDIT_IN"
            - name: PCP_SUBMT_IN
              type: char
              width: 1
              nullable: N
              map: "OutFile.PCP_SUBMT_IN"
            - name: PROD_OOA_IN
              type: char
              width: 1
              nullable: N
              map: "OutFile.PROD_OOA_IN"
            - name: ACDNT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile.ACDNT_DT_SK"
            - name: INPT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile.INPT_DT_SK"
            - name: MBR_PLN_ELIG_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile.MBR_PLN_ELIG_DT_SK"
            - name: NEXT_RVW_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile.NEXT_RVW_DT_SK"
            - name: PD_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile.PD_DT_SK"
            - name: PAYMT_DRAG_CYC_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile.PAYMT_DRAG_CYC_DT_SK"
            - name: PRCS_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile.PRCS_DT_SK"
            - name: RCVD_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile.RCVD_DT_SK"
            - name: SVC_STRT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile.SVC_STRT_DT_SK"
            - name: SVC_END_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile.SVC_END_DT_SK"
            - name: SMLR_ILNS_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile.SMLR_ILNS_DT_SK"
            - name: STTUS_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile.STTUS_DT_SK"
            - name: WORK_UNABLE_BEG_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile.WORK_UNABLE_BEG_DT_SK"
            - name: WORK_UNABLE_END_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile.WORK_UNABLE_END_DT_SK"
            - name: ACDNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile.ACDNT_AMT"
            - name: ACTL_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile.ACTL_PD_AMT"
            - name: ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile.ALW_AMT"
            - name: DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile.DSALW_AMT"
            - name: COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile.COINS_AMT"
            - name: CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile.CNSD_CHRG_AMT"
            - name: COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile.COPAY_AMT"
            - name: CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile.CHRG_AMT"
            - name: DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile.DEDCT_AMT"
            - name: PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile.PAYBL_AMT"
            - name: CLM_CT
              type: smallint
              width: 2
              nullable: N
              map: "OutFile.CLM_CT"
            - name: MBR_AGE
              type: decimal
              width: 3
              map: "OutFile.MBR_AGE"
            - name: ADJ_FROM_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "OutFile.ADJ_FROM_CLM_ID"
            - name: ADJ_TO_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "OutFile.ADJ_TO_CLM_ID"
            - name: DOC_TX_ID
              type: char
              width: 18
              nullable: N
              map: "OutFile.DOC_TX_ID"
            - name: MCAID_RESUB_NO
              type: char
              width: 15
              map: "OutFile.MCAID_RESUB_NO"
            - name: MCARE_ID
              type: char
              width: 12
              nullable: N
              map: "OutFile.MCARE_ID"
            - name: MBR_SFX_NO
              type: char
              width: 2
              map: "OutFile.MBR_SFX_NO"
            - name: PATN_ACCT_NO
              type: varchar
              width: 35
              map: "OutFile.PATN_ACCT_NO"
            - name: PAYMT_REF_ID
              type: char
              width: 16
              nullable: N
              map: "OutFile.PAYMT_REF_ID"
            - name: PROV_AGMNT_ID
              type: char
              width: 12
              nullable: N
              map: "OutFile.PROV_AGMNT_ID"
            - name: RFRNG_PROV_TX
              type: varchar
              width: 70
              map: "OutFile.RFRNG_PROV_TX"
            - name: SUB_ID
              type: char
              width: 14
              nullable: N
              map: "OutFile.SUB_ID"
            - name: PCA_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.PCA_TYP_CD_SK"
            - name: REL_PCA_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.REL_PCA_CLM_SK"
            - name: REL_BASE_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.REL_BASE_CLM_SK"
            - name: CLM_UPDT_SW
              type: char
              width: 1
              nullable: N
              map: "OutFile.CLM_UPDT_SW"
            - name: REMIT_SUPRSION_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile.REMIT_SUPRSION_AMT"
            - name: MCAID_STTUS_ID
              type: varchar
              width: 20
              nullable: N
              map: "OutFile.MCAID_STTUS_ID"
            - name: PATN_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile.PATN_PD_AMT"
            - name: CLM_SUBMT_ICD_VRSN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_SUBMT_ICD_VRSN_CD_SK"
            - name: CLM_TXNMY_CD
              type: varchar
              width: 20
              map: "OutFile.CLM_TXNMY_CD"
            - name: BILL_PAYMT_EXCL_IN
              type: char
              width: 1
              nullable: N
              map: "OutFile.BILL_PAYMT_EXCL_IN"
            - name: ADJ_FROM_CLM
              type: char
              width: 12
              nullable: N
              map: "OutFile.ADJ_FROM_CLM"
            - name: ADJ_TO_CLM
              type: char
              width: 12
              nullable: N
              map: "OutFile.ADJ_TO_CLM"
            - name: SRC_SYS_CD_SK_FROM
              type: int
              width: 4
              nullable: N
              map: "GetFkeyCodes(\"IDS\", 1, \"SOURCE SYSTEM\", \"LUMERIS\", \"X\")"
            - name: SRC_SYS_CD_SK_TO
              type: int
              width: 4
              nullable: N
              map: "GetFkeyCodes(\"IDS\", 1, \"SOURCE SYSTEM\", \"FACETS\", \"X\")"
            - name: svAdjTOandFROM
              type: decimal
              width: 0
              map: svAdjTOandFROM
              initValue: "0"
        - name: Trans3.V0S141P8
          type: filter
          source: Trans3.V0S141P8.map
          filter: "svAdjTOandFROM = 'Y'"
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: ADJ_FROM_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: ADJ_TO_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: ALPHA_PFX_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_EOB_EXCD_SK
              type: int
              width: 4
              nullable: N
            - name: CLS_SK
              type: int
              width: 4
              nullable: N
            - name: CLS_PLN_SK
              type: int
              width: 4
              nullable: N
            - name: EXPRNC_CAT_SK
              type: int
              width: 4
              nullable: N
            - name: FNCL_LOB_SK
              type: int
              width: 4
              nullable: N
            - name: GRP_SK
              type: int
              width: 4
              nullable: N
            - name: MBR_SK
              type: int
              width: 4
              nullable: N
            - name: NTWK_SK
              type: int
              width: 4
              nullable: N
            - name: PROD_SK
              type: int
              width: 4
              nullable: N
            - name: SUBGRP_SK
              type: int
              width: 4
              nullable: N
            - name: SUB_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_ACDNT_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_ACDNT_ST_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_ACTV_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_AGMNT_SRC_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_BTCH_ACTN_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_CAP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_CAT_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_CHK_CYC_OVRD_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_COB_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_FINL_DISP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_INPT_METH_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_INPT_SRC_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_INTER_PLN_PGM_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_NTWK_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_NONPAR_PROV_PFX_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_OTHR_BNF_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_PAYE_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_PRCS_CTL_AGNT_PFX_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_SVC_DEFN_PFX_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_SVC_PROV_SPEC_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_SVC_PROV_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_SUBMTTING_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_SUB_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_SUBTYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: ATCHMT_IN
              type: char
              width: 1
              nullable: N
            - name: CLNCL_EDIT_IN
              type: char
              width: 1
              nullable: N
            - name: COBRA_CLM_IN
              type: char
              width: 1
              nullable: N
            - name: FIRST_PASS_IN
              type: char
              width: 1
              nullable: N
            - name: HOST_IN
              type: char
              width: 1
              nullable: N
            - name: LTR_IN
              type: char
              width: 1
              nullable: N
            - name: MCARE_ASG_IN
              type: char
              width: 1
              nullable: N
            - name: NOTE_IN
              type: char
              width: 1
              nullable: N
            - name: PCA_AUDIT_IN
              type: char
              width: 1
              nullable: N
            - name: PCP_SUBMT_IN
              type: char
              width: 1
              nullable: N
            - name: PROD_OOA_IN
              type: char
              width: 1
              nullable: N
            - name: ACDNT_DT_SK
              type: char
              width: 10
              nullable: N
            - name: INPT_DT_SK
              type: char
              width: 10
              nullable: N
            - name: MBR_PLN_ELIG_DT_SK
              type: char
              width: 10
              nullable: N
            - name: NEXT_RVW_DT_SK
              type: char
              width: 10
              nullable: N
            - name: PD_DT_SK
              type: char
              width: 10
              nullable: N
            - name: PAYMT_DRAG_CYC_DT_SK
              type: char
              width: 10
              nullable: N
            - name: PRCS_DT_SK
              type: char
              width: 10
              nullable: N
            - name: RCVD_DT_SK
              type: char
              width: 10
              nullable: N
            - name: SVC_STRT_DT_SK
              type: char
              width: 10
              nullable: N
            - name: SVC_END_DT_SK
              type: char
              width: 10
              nullable: N
            - name: SMLR_ILNS_DT_SK
              type: char
              width: 10
              nullable: N
            - name: STTUS_DT_SK
              type: char
              width: 10
              nullable: N
            - name: WORK_UNABLE_BEG_DT_SK
              type: char
              width: 10
              nullable: N
            - name: WORK_UNABLE_END_DT_SK
              type: char
              width: 10
              nullable: N
            - name: ACDNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: ACTL_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: CLM_CT
              type: smallint
              width: 2
              nullable: N
            - name: MBR_AGE
              type: decimal
              width: 3
            - name: ADJ_FROM_CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: ADJ_TO_CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: DOC_TX_ID
              type: char
              width: 18
              nullable: N
            - name: MCAID_RESUB_NO
              type: char
              width: 15
            - name: MCARE_ID
              type: char
              width: 12
              nullable: N
            - name: MBR_SFX_NO
              type: char
              width: 2
            - name: PATN_ACCT_NO
              type: varchar
              width: 35
            - name: PAYMT_REF_ID
              type: char
              width: 16
              nullable: N
            - name: PROV_AGMNT_ID
              type: char
              width: 12
              nullable: N
            - name: RFRNG_PROV_TX
              type: varchar
              width: 70
            - name: SUB_ID
              type: char
              width: 14
              nullable: N
            - name: PCA_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: REL_PCA_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: REL_BASE_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: REMIT_SUPRSION_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: MCAID_STTUS_ID
              type: varchar
              width: 20
              nullable: N
            - name: PATN_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: CLM_SUBMT_ICD_VRSN_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_TXNMY_CD
              type: varchar
              width: 20
            - name: BILL_PAYMT_EXCL_IN
              type: char
              width: 1
              nullable: N
            - name: ADJ_FROM_CLM
              type: char
              width: 12
              nullable: N
            - name: ADJ_TO_CLM
              type: char
              width: 12
              nullable: N
            - name: SRC_SYS_CD_SK_FROM
              type: int
              width: 4
              nullable: N
            - name: SRC_SYS_CD_SK_TO
              type: int
              width: 4
              nullable: N
          keys:
            - CLM_SK
        - name: Trans3.V0S141P3.map
          type: map
          source: merge_load1
          alias: OutFile
          stageVars: 
            - name: svAdjTOandFROM
              type: decimal
              width: 0
              map: " If (Source = 'LUMERIS' and OutFile.ADJ_TO_CLM_ID[1, 1] <> 'L') Or (Source = 'FACETS' and OutFile.ADJ_FROM_CLM_ID[1, 1] = 'L') Then 'Y' Else 'N'"
              initValue: "0"
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_SK"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.SRC_SYS_CD_SK"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "OutFile.CLM_ID"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: ADJ_FROM_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.ADJ_FROM_CLM_SK"
            - name: ADJ_TO_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.ADJ_TO_CLM_SK"
            - name: ALPHA_PFX_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.ALPHA_PFX_SK"
            - name: CLM_EOB_EXCD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_EOB_EXCD_SK"
            - name: CLS_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLS_SK"
            - name: CLS_PLN_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLS_PLN_SK"
            - name: EXPRNC_CAT_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.EXPRNC_CAT_SK"
            - name: FNCL_LOB_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.FNCL_LOB_SK"
            - name: GRP_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.GRP_SK"
            - name: MBR_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.MBR_SK"
            - name: NTWK_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.NTWK_SK"
            - name: PROD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.PROD_SK"
            - name: SUBGRP_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.SUBGRP_SK"
            - name: SUB_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.SUB_SK"
            - name: CLM_ACDNT_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_ACDNT_CD_SK"
            - name: CLM_ACDNT_ST_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_ACDNT_ST_CD_SK"
            - name: CLM_ACTV_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_ACTV_BCBS_PLN_CD_SK"
            - name: CLM_AGMNT_SRC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_AGMNT_SRC_CD_SK"
            - name: CLM_BTCH_ACTN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_BTCH_ACTN_CD_SK"
            - name: CLM_CAP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_CAP_CD_SK"
            - name: CLM_CAT_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_CAT_CD_SK"
            - name: CLM_CHK_CYC_OVRD_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_CHK_CYC_OVRD_CD_SK"
            - name: CLM_COB_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_COB_CD_SK"
            - name: CLM_FINL_DISP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_FINL_DISP_CD_SK"
            - name: CLM_INPT_METH_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_INPT_METH_CD_SK"
            - name: CLM_INPT_SRC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_INPT_SRC_CD_SK"
            - name: CLM_INTER_PLN_PGM_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_INTER_PLN_PGM_CD_SK"
            - name: CLM_NTWK_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_NTWK_STTUS_CD_SK"
            - name: CLM_NONPAR_PROV_PFX_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_NONPAR_PROV_PFX_CD_SK"
            - name: CLM_OTHR_BNF_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_OTHR_BNF_CD_SK"
            - name: CLM_PAYE_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_PAYE_CD_SK"
            - name: CLM_PRCS_CTL_AGNT_PFX_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_PRCS_CTL_AGNT_PFX_CD_SK"
            - name: CLM_SVC_DEFN_PFX_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_SVC_DEFN_PFX_CD_SK"
            - name: CLM_SVC_PROV_SPEC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_SVC_PROV_SPEC_CD_SK"
            - name: CLM_SVC_PROV_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_SVC_PROV_TYP_CD_SK"
            - name: CLM_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_STTUS_CD_SK"
            - name: CLM_SUBMTTING_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_SUBMTTING_BCBS_PLN_CD_SK"
            - name: CLM_SUB_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_SUB_BCBS_PLN_CD_SK"
            - name: CLM_SUBTYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_SUBTYP_CD_SK"
            - name: CLM_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_TYP_CD_SK"
            - name: ATCHMT_IN
              type: char
              width: 1
              nullable: N
              map: "OutFile.ATCHMT_IN"
            - name: CLNCL_EDIT_IN
              type: char
              width: 1
              nullable: N
              map: "OutFile.CLNCL_EDIT_IN"
            - name: COBRA_CLM_IN
              type: char
              width: 1
              nullable: N
              map: "OutFile.COBRA_CLM_IN"
            - name: FIRST_PASS_IN
              type: char
              width: 1
              nullable: N
              map: "OutFile.FIRST_PASS_IN"
            - name: HOST_IN
              type: char
              width: 1
              nullable: N
              map: "OutFile.HOST_IN"
            - name: LTR_IN
              type: char
              width: 1
              nullable: N
              map: "OutFile.LTR_IN"
            - name: MCARE_ASG_IN
              type: char
              width: 1
              nullable: N
              map: "OutFile.MCARE_ASG_IN"
            - name: NOTE_IN
              type: char
              width: 1
              nullable: N
              map: "OutFile.NOTE_IN"
            - name: PCA_AUDIT_IN
              type: char
              width: 1
              nullable: N
              map: "OutFile.PCA_AUDIT_IN"
            - name: PCP_SUBMT_IN
              type: char
              width: 1
              nullable: N
              map: "OutFile.PCP_SUBMT_IN"
            - name: PROD_OOA_IN
              type: char
              width: 1
              nullable: N
              map: "OutFile.PROD_OOA_IN"
            - name: ACDNT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile.ACDNT_DT_SK"
            - name: INPT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile.INPT_DT_SK"
            - name: MBR_PLN_ELIG_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile.MBR_PLN_ELIG_DT_SK"
            - name: NEXT_RVW_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile.NEXT_RVW_DT_SK"
            - name: PD_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile.PD_DT_SK"
            - name: PAYMT_DRAG_CYC_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile.PAYMT_DRAG_CYC_DT_SK"
            - name: PRCS_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile.PRCS_DT_SK"
            - name: RCVD_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile.RCVD_DT_SK"
            - name: SVC_STRT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile.SVC_STRT_DT_SK"
            - name: SVC_END_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile.SVC_END_DT_SK"
            - name: SMLR_ILNS_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile.SMLR_ILNS_DT_SK"
            - name: STTUS_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile.STTUS_DT_SK"
            - name: WORK_UNABLE_BEG_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile.WORK_UNABLE_BEG_DT_SK"
            - name: WORK_UNABLE_END_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile.WORK_UNABLE_END_DT_SK"
            - name: ACDNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile.ACDNT_AMT"
            - name: ACTL_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile.ACTL_PD_AMT"
            - name: ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile.ALW_AMT"
            - name: DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile.DSALW_AMT"
            - name: COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile.COINS_AMT"
            - name: CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile.CNSD_CHRG_AMT"
            - name: COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile.COPAY_AMT"
            - name: CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile.CHRG_AMT"
            - name: DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile.DEDCT_AMT"
            - name: PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile.PAYBL_AMT"
            - name: CLM_CT
              type: smallint
              width: 2
              nullable: N
              map: "OutFile.CLM_CT"
            - name: MBR_AGE
              type: decimal
              width: 3
              map: "OutFile.MBR_AGE"
            - name: ADJ_FROM_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "OutFile.ADJ_FROM_CLM_ID"
            - name: ADJ_TO_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "OutFile.ADJ_TO_CLM_ID"
            - name: DOC_TX_ID
              type: char
              width: 18
              nullable: N
              map: "OutFile.DOC_TX_ID"
            - name: MCAID_RESUB_NO
              type: char
              width: 15
              map: "OutFile.MCAID_RESUB_NO"
            - name: MCARE_ID
              type: char
              width: 12
              nullable: N
              map: "OutFile.MCARE_ID"
            - name: MBR_SFX_NO
              type: char
              width: 2
              map: "OutFile.MBR_SFX_NO"
            - name: PATN_ACCT_NO
              type: varchar
              width: 35
              map: "OutFile.PATN_ACCT_NO"
            - name: PAYMT_REF_ID
              type: char
              width: 16
              nullable: N
              map: "OutFile.PAYMT_REF_ID"
            - name: PROV_AGMNT_ID
              type: char
              width: 12
              nullable: N
              map: "OutFile.PROV_AGMNT_ID"
            - name: RFRNG_PROV_TX
              type: varchar
              width: 70
              map: "OutFile.RFRNG_PROV_TX"
            - name: SUB_ID
              type: char
              width: 14
              nullable: N
              map: "OutFile.SUB_ID"
            - name: PCA_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.PCA_TYP_CD_SK"
            - name: REL_PCA_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.REL_PCA_CLM_SK"
            - name: REL_BASE_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.REL_BASE_CLM_SK"
            - name: CLM_UPDT_SW
              type: char
              width: 1
              nullable: N
              map: "OutFile.CLM_UPDT_SW"
            - name: REMIT_SUPRSION_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile.REMIT_SUPRSION_AMT"
            - name: MCAID_STTUS_ID
              type: varchar
              width: 20
              nullable: N
              map: "OutFile.MCAID_STTUS_ID"
            - name: PATN_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile.PATN_PD_AMT"
            - name: CLM_SUBMT_ICD_VRSN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile.CLM_SUBMT_ICD_VRSN_CD_SK"
            - name: CLM_TXNMY_CD
              type: varchar
              width: 20
              map: "OutFile.CLM_TXNMY_CD"
            - name: BILL_PAYMT_EXCL_IN
              type: char
              width: 1
              nullable: N
              map: "OutFile.BILL_PAYMT_EXCL_IN"
            - name: ADJ_FROM_CLM
              type: char
              width: 12
              nullable: N
              map: "OutFile.ADJ_FROM_CLM"
            - name: ADJ_TO_CLM
              type: char
              width: 12
              nullable: N
              map: "OutFile.ADJ_TO_CLM"
            - name: svAdjTOandFROM
              type: decimal
              width: 0
              map: svAdjTOandFROM
              initValue: "0"
        - name: Trans3.V0S141P3
          type: filter
          source: Trans3.V0S141P3.map
          filter: "CLM_UPDT_SW <> 'N'"
          schema: 
            - name: REL_BASE_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
          keys:
            - REL_BASE_CLM_SK
        - name: Trans6.V0S198P2
          type: map
          source: Trans6.V0S198P2.filter
          alias: OutFile1
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.CLM_SK"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.SRC_SYS_CD_SK"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "OutFile1.CLM_ID"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: ADJ_FROM_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.ADJ_FROM_CLM_SK"
            - name: ADJ_TO_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.ADJ_TO_CLM_SK"
            - name: ALPHA_PFX_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.ALPHA_PFX_SK"
            - name: CLM_EOB_EXCD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.CLM_EOB_EXCD_SK"
            - name: CLS_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.CLS_SK"
            - name: CLS_PLN_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.CLS_PLN_SK"
            - name: EXPRNC_CAT_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.EXPRNC_CAT_SK"
            - name: FNCL_LOB_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.FNCL_LOB_SK"
            - name: GRP_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.GRP_SK"
            - name: MBR_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.MBR_SK"
            - name: NTWK_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.NTWK_SK"
            - name: PROD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.PROD_SK"
            - name: SUBGRP_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.SUBGRP_SK"
            - name: SUB_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.SUB_SK"
            - name: CLM_ACDNT_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.CLM_ACDNT_CD_SK"
            - name: CLM_ACDNT_ST_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.CLM_ACDNT_ST_CD_SK"
            - name: CLM_ACTV_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.CLM_ACTV_BCBS_PLN_CD_SK"
            - name: CLM_AGMNT_SRC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.CLM_AGMNT_SRC_CD_SK"
            - name: CLM_BTCH_ACTN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.CLM_BTCH_ACTN_CD_SK"
            - name: CLM_CAP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.CLM_CAP_CD_SK"
            - name: CLM_CAT_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.CLM_CAT_CD_SK"
            - name: CLM_CHK_CYC_OVRD_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.CLM_CHK_CYC_OVRD_CD_SK"
            - name: CLM_COB_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.CLM_COB_CD_SK"
            - name: CLM_FINL_DISP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.CLM_FINL_DISP_CD_SK"
            - name: CLM_INPT_METH_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.CLM_INPT_METH_CD_SK"
            - name: CLM_INPT_SRC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.CLM_INPT_SRC_CD_SK"
            - name: CLM_INTER_PLN_PGM_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.CLM_INTER_PLN_PGM_CD_SK"
            - name: CLM_NTWK_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.CLM_NTWK_STTUS_CD_SK"
            - name: CLM_NONPAR_PROV_PFX_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.CLM_NONPAR_PROV_PFX_CD_SK"
            - name: CLM_OTHR_BNF_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.CLM_OTHR_BNF_CD_SK"
            - name: CLM_PAYE_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.CLM_PAYE_CD_SK"
            - name: CLM_PRCS_CTL_AGNT_PFX_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.CLM_PRCS_CTL_AGNT_PFX_CD_SK"
            - name: CLM_SVC_DEFN_PFX_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.CLM_SVC_DEFN_PFX_CD_SK"
            - name: CLM_SVC_PROV_SPEC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.CLM_SVC_PROV_SPEC_CD_SK"
            - name: CLM_SVC_PROV_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.CLM_SVC_PROV_TYP_CD_SK"
            - name: CLM_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.CLM_STTUS_CD_SK"
            - name: CLM_SUBMTTING_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.CLM_SUBMTTING_BCBS_PLN_CD_SK"
            - name: CLM_SUB_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.CLM_SUB_BCBS_PLN_CD_SK"
            - name: CLM_SUBTYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.CLM_SUBTYP_CD_SK"
            - name: CLM_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.CLM_TYP_CD_SK"
            - name: ATCHMT_IN
              type: char
              width: 1
              nullable: N
              map: "OutFile1.ATCHMT_IN"
            - name: CLNCL_EDIT_IN
              type: char
              width: 1
              nullable: N
              map: "OutFile1.CLNCL_EDIT_IN"
            - name: COBRA_CLM_IN
              type: char
              width: 1
              nullable: N
              map: "OutFile1.COBRA_CLM_IN"
            - name: FIRST_PASS_IN
              type: char
              width: 1
              nullable: N
              map: "OutFile1.FIRST_PASS_IN"
            - name: HOST_IN
              type: char
              width: 1
              nullable: N
              map: "OutFile1.HOST_IN"
            - name: LTR_IN
              type: char
              width: 1
              nullable: N
              map: "OutFile1.LTR_IN"
            - name: MCARE_ASG_IN
              type: char
              width: 1
              nullable: N
              map: "OutFile1.MCARE_ASG_IN"
            - name: NOTE_IN
              type: char
              width: 1
              nullable: N
              map: "OutFile1.NOTE_IN"
            - name: PCA_AUDIT_IN
              type: char
              width: 1
              nullable: N
              map: "OutFile1.PCA_AUDIT_IN"
            - name: PCP_SUBMT_IN
              type: char
              width: 1
              nullable: N
              map: "OutFile1.PCP_SUBMT_IN"
            - name: PROD_OOA_IN
              type: char
              width: 1
              nullable: N
              map: "OutFile1.PROD_OOA_IN"
            - name: ACDNT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile1.ACDNT_DT_SK"
            - name: INPT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile1.INPT_DT_SK"
            - name: MBR_PLN_ELIG_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile1.MBR_PLN_ELIG_DT_SK"
            - name: NEXT_RVW_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile1.NEXT_RVW_DT_SK"
            - name: PD_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile1.PD_DT_SK"
            - name: PAYMT_DRAG_CYC_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile1.PAYMT_DRAG_CYC_DT_SK"
            - name: PRCS_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile1.PRCS_DT_SK"
            - name: RCVD_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile1.RCVD_DT_SK"
            - name: SVC_STRT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile1.SVC_STRT_DT_SK"
            - name: SVC_END_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile1.SVC_END_DT_SK"
            - name: SMLR_ILNS_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile1.SMLR_ILNS_DT_SK"
            - name: STTUS_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile1.STTUS_DT_SK"
            - name: WORK_UNABLE_BEG_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile1.WORK_UNABLE_BEG_DT_SK"
            - name: WORK_UNABLE_END_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile1.WORK_UNABLE_END_DT_SK"
            - name: ACDNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile1.ACDNT_AMT"
            - name: ACTL_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile1.ACTL_PD_AMT"
            - name: ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile1.ALW_AMT"
            - name: DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile1.DSALW_AMT"
            - name: COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile1.COINS_AMT"
            - name: CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile1.CNSD_CHRG_AMT"
            - name: COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile1.COPAY_AMT"
            - name: CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile1.CHRG_AMT"
            - name: DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile1.DEDCT_AMT"
            - name: PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile1.PAYBL_AMT"
            - name: CLM_CT
              type: smallint
              width: 2
              nullable: N
              map: "OutFile1.CLM_CT"
            - name: MBR_AGE
              type: decimal
              width: 3
              map: "OutFile1.MBR_AGE"
            - name: ADJ_FROM_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "OutFile1.ADJ_FROM_CLM_ID"
            - name: ADJ_TO_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "OutFile1.ADJ_TO_CLM_ID"
            - name: DOC_TX_ID
              type: char
              width: 18
              nullable: N
              map: "OutFile1.DOC_TX_ID"
            - name: MCAID_RESUB_NO
              type: char
              width: 15
              map: "OutFile1.MCAID_RESUB_NO"
            - name: MCARE_ID
              type: char
              width: 12
              nullable: N
              map: "OutFile1.MCARE_ID"
            - name: MBR_SFX_NO
              type: char
              width: 2
              map: "OutFile1.MBR_SFX_NO"
            - name: PATN_ACCT_NO
              type: varchar
              width: 35
              map: "OutFile1.PATN_ACCT_NO"
            - name: PAYMT_REF_ID
              type: char
              width: 16
              nullable: N
              map: "OutFile1.PAYMT_REF_ID"
            - name: PROV_AGMNT_ID
              type: char
              width: 12
              nullable: N
              map: "OutFile1.PROV_AGMNT_ID"
            - name: RFRNG_PROV_TX
              type: varchar
              width: 70
              map: "OutFile1.RFRNG_PROV_TX"
            - name: SUB_ID
              type: char
              width: 14
              nullable: N
              map: "OutFile1.SUB_ID"
            - name: PCA_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.PCA_TYP_CD_SK"
            - name: REL_PCA_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.REL_PCA_CLM_SK"
            - name: REL_BASE_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.REL_BASE_CLM_SK"
            - name: CLM_UPDT_SW
              type: char
              width: 1
              nullable: N
              map: "OutFile1.CLM_UPDT_SW"
            - name: REMIT_SUPRSION_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile1.REMIT_SUPRSION_AMT"
            - name: MCAID_STTUS_ID
              type: varchar
              width: 20
              nullable: N
              map: "OutFile1.MCAID_STTUS_ID"
            - name: PATN_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile1.PATN_PD_AMT"
            - name: CLM_SUBMT_ICD_VRSN_CD_SK
              type: float
              nullable: N
              map: "OutFile1.CLM_SUBMT_ICD_VRSN_CD_SK"
            - name: CLM_TXNMY_CD
              type: varchar
              width: 20
              map: "OutFile1.CLM_TXNMY_CD"
            - name: BILL_PAYMT_EXCL_IN
              type: char
              width: 1
              nullable: N
              map: "OutFile1.BILL_PAYMT_EXCL_IN"
            - name: ADJ_FROM_CLM
              type: char
              width: 12
              nullable: N
              map: "OutFile1.ADJ_FROM_CLM"
            - name: ADJ_TO_CLM
              type: char
              width: 12
              nullable: N
              map: "OutFile1.ADJ_TO_CLM"
        - name: Trans6.V0S198P2.filter
          type: filter
          source: merge_load
          filter: "PCA_TYP_CD_SK = 0 or PCA_TYP_CD_SK = 1"
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: ADJ_FROM_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: ADJ_TO_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: ALPHA_PFX_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_EOB_EXCD_SK
              type: int
              width: 4
              nullable: N
            - name: CLS_SK
              type: int
              width: 4
              nullable: N
            - name: CLS_PLN_SK
              type: int
              width: 4
              nullable: N
            - name: EXPRNC_CAT_SK
              type: int
              width: 4
              nullable: N
            - name: FNCL_LOB_SK
              type: int
              width: 4
              nullable: N
            - name: GRP_SK
              type: int
              width: 4
              nullable: N
            - name: MBR_SK
              type: int
              width: 4
              nullable: N
            - name: NTWK_SK
              type: int
              width: 4
              nullable: N
            - name: PROD_SK
              type: int
              width: 4
              nullable: N
            - name: SUBGRP_SK
              type: int
              width: 4
              nullable: N
            - name: SUB_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_ACDNT_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_ACDNT_ST_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_ACTV_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_AGMNT_SRC_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_BTCH_ACTN_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_CAP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_CAT_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_CHK_CYC_OVRD_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_COB_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_FINL_DISP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_INPT_METH_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_INPT_SRC_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_INTER_PLN_PGM_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_NTWK_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_NONPAR_PROV_PFX_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_OTHR_BNF_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_PAYE_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_PRCS_CTL_AGNT_PFX_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_SVC_DEFN_PFX_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_SVC_PROV_SPEC_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_SVC_PROV_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_SUBMTTING_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_SUB_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_SUBTYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: ATCHMT_IN
              type: char
              width: 1
              nullable: N
            - name: CLNCL_EDIT_IN
              type: char
              width: 1
              nullable: N
            - name: COBRA_CLM_IN
              type: char
              width: 1
              nullable: N
            - name: FIRST_PASS_IN
              type: char
              width: 1
              nullable: N
            - name: HOST_IN
              type: char
              width: 1
              nullable: N
            - name: LTR_IN
              type: char
              width: 1
              nullable: N
            - name: MCARE_ASG_IN
              type: char
              width: 1
              nullable: N
            - name: NOTE_IN
              type: char
              width: 1
              nullable: N
            - name: PCA_AUDIT_IN
              type: char
              width: 1
              nullable: N
            - name: PCP_SUBMT_IN
              type: char
              width: 1
              nullable: N
            - name: PROD_OOA_IN
              type: char
              width: 1
              nullable: N
            - name: ACDNT_DT_SK
              type: char
              width: 10
              nullable: N
            - name: INPT_DT_SK
              type: char
              width: 10
              nullable: N
            - name: MBR_PLN_ELIG_DT_SK
              type: char
              width: 10
              nullable: N
            - name: NEXT_RVW_DT_SK
              type: char
              width: 10
              nullable: N
            - name: PD_DT_SK
              type: char
              width: 10
              nullable: N
            - name: PAYMT_DRAG_CYC_DT_SK
              type: char
              width: 10
              nullable: N
            - name: PRCS_DT_SK
              type: char
              width: 10
              nullable: N
            - name: RCVD_DT_SK
              type: char
              width: 10
              nullable: N
            - name: SVC_STRT_DT_SK
              type: char
              width: 10
              nullable: N
            - name: SVC_END_DT_SK
              type: char
              width: 10
              nullable: N
            - name: SMLR_ILNS_DT_SK
              type: char
              width: 10
              nullable: N
            - name: STTUS_DT_SK
              type: char
              width: 10
              nullable: N
            - name: WORK_UNABLE_BEG_DT_SK
              type: char
              width: 10
              nullable: N
            - name: WORK_UNABLE_END_DT_SK
              type: char
              width: 10
              nullable: N
            - name: ACDNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: ACTL_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: CLM_CT
              type: smallint
              width: 2
              nullable: N
            - name: MBR_AGE
              type: decimal
              width: 3
            - name: ADJ_FROM_CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: ADJ_TO_CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: DOC_TX_ID
              type: char
              width: 18
              nullable: N
            - name: MCAID_RESUB_NO
              type: char
              width: 15
            - name: MCARE_ID
              type: char
              width: 12
              nullable: N
            - name: MBR_SFX_NO
              type: char
              width: 2
            - name: PATN_ACCT_NO
              type: varchar
              width: 35
            - name: PAYMT_REF_ID
              type: char
              width: 16
              nullable: N
            - name: PROV_AGMNT_ID
              type: char
              width: 12
              nullable: N
            - name: RFRNG_PROV_TX
              type: varchar
              width: 70
            - name: SUB_ID
              type: char
              width: 14
              nullable: N
            - name: PCA_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: REL_PCA_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: REL_BASE_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_UPDT_SW
              type: char
              width: 1
              nullable: N
            - name: CLCL_MICRO_ID
              type: char
              width: 18
              nullable: N
            - name: REMIT_SUPRSION_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: MCAID_STTUS_ID
              type: varchar
              width: 20
              nullable: N
            - name: PATN_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: CLM_SUBMT_ICD_VRSN_CD_SK
              type: float
              nullable: N
            - name: CLM_TXNMY_CD
              type: varchar
              width: 20
            - name: BILL_PAYMT_EXCL_IN
              type: char
              width: 1
              nullable: N
            - name: ADJ_FROM_CLM
              type: char
              width: 12
              nullable: N
            - name: ADJ_TO_CLM
              type: char
              width: 12
              nullable: N
          keys:
            - CLM_SK
        - name: Trans6.V0S198P3
          type: map
          source: Trans6.V0S198P3.filter
          alias: OutFile1
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.CLM_SK"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.SRC_SYS_CD_SK"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "OutFile1.CLM_ID"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: ADJ_FROM_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.ADJ_FROM_CLM_SK"
            - name: ADJ_TO_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.ADJ_TO_CLM_SK"
            - name: ALPHA_PFX_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.ALPHA_PFX_SK"
            - name: CLM_EOB_EXCD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.CLM_EOB_EXCD_SK"
            - name: CLS_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.CLS_SK"
            - name: CLS_PLN_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.CLS_PLN_SK"
            - name: EXPRNC_CAT_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.EXPRNC_CAT_SK"
            - name: FNCL_LOB_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.FNCL_LOB_SK"
            - name: GRP_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.GRP_SK"
            - name: MBR_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.MBR_SK"
            - name: NTWK_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.NTWK_SK"
            - name: PROD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.PROD_SK"
            - name: SUBGRP_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.SUBGRP_SK"
            - name: SUB_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.SUB_SK"
            - name: CLM_ACDNT_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.CLM_ACDNT_CD_SK"
            - name: CLM_ACDNT_ST_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.CLM_ACDNT_ST_CD_SK"
            - name: CLM_ACTV_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.CLM_ACTV_BCBS_PLN_CD_SK"
            - name: CLM_AGMNT_SRC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.CLM_AGMNT_SRC_CD_SK"
            - name: CLM_BTCH_ACTN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.CLM_BTCH_ACTN_CD_SK"
            - name: CLM_CAP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.CLM_CAP_CD_SK"
            - name: CLM_CAT_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.CLM_CAT_CD_SK"
            - name: CLM_CHK_CYC_OVRD_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.CLM_CHK_CYC_OVRD_CD_SK"
            - name: CLM_COB_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.CLM_COB_CD_SK"
            - name: CLM_FINL_DISP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.CLM_FINL_DISP_CD_SK"
            - name: CLM_INPT_METH_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.CLM_INPT_METH_CD_SK"
            - name: CLM_INPT_SRC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.CLM_INPT_SRC_CD_SK"
            - name: CLM_INTER_PLN_PGM_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.CLM_INTER_PLN_PGM_CD_SK"
            - name: CLM_NTWK_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.CLM_NTWK_STTUS_CD_SK"
            - name: CLM_NONPAR_PROV_PFX_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.CLM_NONPAR_PROV_PFX_CD_SK"
            - name: CLM_OTHR_BNF_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.CLM_OTHR_BNF_CD_SK"
            - name: CLM_PAYE_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.CLM_PAYE_CD_SK"
            - name: CLM_PRCS_CTL_AGNT_PFX_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.CLM_PRCS_CTL_AGNT_PFX_CD_SK"
            - name: CLM_SVC_DEFN_PFX_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.CLM_SVC_DEFN_PFX_CD_SK"
            - name: CLM_SVC_PROV_SPEC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.CLM_SVC_PROV_SPEC_CD_SK"
            - name: CLM_SVC_PROV_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.CLM_SVC_PROV_TYP_CD_SK"
            - name: CLM_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.CLM_STTUS_CD_SK"
            - name: CLM_SUBMTTING_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.CLM_SUBMTTING_BCBS_PLN_CD_SK"
            - name: CLM_SUB_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.CLM_SUB_BCBS_PLN_CD_SK"
            - name: CLM_SUBTYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.CLM_SUBTYP_CD_SK"
            - name: CLM_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.CLM_TYP_CD_SK"
            - name: ATCHMT_IN
              type: char
              width: 1
              nullable: N
              map: "OutFile1.ATCHMT_IN"
            - name: CLNCL_EDIT_IN
              type: char
              width: 1
              nullable: N
              map: "OutFile1.CLNCL_EDIT_IN"
            - name: COBRA_CLM_IN
              type: char
              width: 1
              nullable: N
              map: "OutFile1.COBRA_CLM_IN"
            - name: FIRST_PASS_IN
              type: char
              width: 1
              nullable: N
              map: "OutFile1.FIRST_PASS_IN"
            - name: HOST_IN
              type: char
              width: 1
              nullable: N
              map: "OutFile1.HOST_IN"
            - name: LTR_IN
              type: char
              width: 1
              nullable: N
              map: "OutFile1.LTR_IN"
            - name: MCARE_ASG_IN
              type: char
              width: 1
              nullable: N
              map: "OutFile1.MCARE_ASG_IN"
            - name: NOTE_IN
              type: char
              width: 1
              nullable: N
              map: "OutFile1.NOTE_IN"
            - name: PCA_AUDIT_IN
              type: char
              width: 1
              nullable: N
              map: "OutFile1.PCA_AUDIT_IN"
            - name: PCP_SUBMT_IN
              type: char
              width: 1
              nullable: N
              map: "OutFile1.PCP_SUBMT_IN"
            - name: PROD_OOA_IN
              type: char
              width: 1
              nullable: N
              map: "OutFile1.PROD_OOA_IN"
            - name: ACDNT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile1.ACDNT_DT_SK"
            - name: INPT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile1.INPT_DT_SK"
            - name: MBR_PLN_ELIG_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile1.MBR_PLN_ELIG_DT_SK"
            - name: NEXT_RVW_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile1.NEXT_RVW_DT_SK"
            - name: PD_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile1.PD_DT_SK"
            - name: PAYMT_DRAG_CYC_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile1.PAYMT_DRAG_CYC_DT_SK"
            - name: PRCS_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile1.PRCS_DT_SK"
            - name: RCVD_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile1.RCVD_DT_SK"
            - name: SVC_STRT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile1.SVC_STRT_DT_SK"
            - name: SVC_END_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile1.SVC_END_DT_SK"
            - name: SMLR_ILNS_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile1.SMLR_ILNS_DT_SK"
            - name: STTUS_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile1.STTUS_DT_SK"
            - name: WORK_UNABLE_BEG_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile1.WORK_UNABLE_BEG_DT_SK"
            - name: WORK_UNABLE_END_DT_SK
              type: char
              width: 10
              nullable: N
              map: "OutFile1.WORK_UNABLE_END_DT_SK"
            - name: ACDNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile1.ACDNT_AMT"
            - name: ACTL_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile1.ACTL_PD_AMT"
            - name: ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile1.ALW_AMT"
            - name: DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile1.DSALW_AMT"
            - name: COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile1.COINS_AMT"
            - name: CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile1.CNSD_CHRG_AMT"
            - name: COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile1.COPAY_AMT"
            - name: CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile1.CHRG_AMT"
            - name: DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile1.DEDCT_AMT"
            - name: PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile1.PAYBL_AMT"
            - name: CLM_CT
              type: smallint
              width: 2
              nullable: N
              map: "OutFile1.CLM_CT"
            - name: MBR_AGE
              type: decimal
              width: 3
              map: "OutFile1.MBR_AGE"
            - name: ADJ_FROM_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "OutFile1.ADJ_FROM_CLM_ID"
            - name: ADJ_TO_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "OutFile1.ADJ_TO_CLM_ID"
            - name: DOC_TX_ID
              type: char
              width: 18
              nullable: N
              map: "OutFile1.DOC_TX_ID"
            - name: MCAID_RESUB_NO
              type: char
              width: 15
              map: "OutFile1.MCAID_RESUB_NO"
            - name: MCARE_ID
              type: char
              width: 12
              nullable: N
              map: "OutFile1.MCARE_ID"
            - name: MBR_SFX_NO
              type: char
              width: 2
              map: "OutFile1.MBR_SFX_NO"
            - name: PATN_ACCT_NO
              type: varchar
              width: 35
              map: "OutFile1.PATN_ACCT_NO"
            - name: PAYMT_REF_ID
              type: char
              width: 16
              nullable: N
              map: "OutFile1.PAYMT_REF_ID"
            - name: PROV_AGMNT_ID
              type: char
              width: 12
              nullable: N
              map: "OutFile1.PROV_AGMNT_ID"
            - name: RFRNG_PROV_TX
              type: varchar
              width: 70
              map: "OutFile1.RFRNG_PROV_TX"
            - name: SUB_ID
              type: char
              width: 14
              nullable: N
              map: "OutFile1.SUB_ID"
            - name: PCA_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.PCA_TYP_CD_SK"
            - name: REL_PCA_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.REL_PCA_CLM_SK"
            - name: REL_BASE_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "OutFile1.REL_BASE_CLM_SK"
            - name: CLM_UPDT_SW
              type: char
              width: 1
              nullable: N
              map: "OutFile1.CLM_UPDT_SW"
            - name: CLCL_MICRO_ID
              type: char
              width: 18
              nullable: N
              map: "OutFile1.CLCL_MICRO_ID"
            - name: REMIT_SUPRSION_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile1.REMIT_SUPRSION_AMT"
            - name: MCAID_STTUS_ID
              type: varchar
              width: 20
              nullable: N
              map: "OutFile1.MCAID_STTUS_ID"
            - name: PATN_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile1.PATN_PD_AMT"
            - name: CLM_SUBMT_ICD_VRSN_CD_SK
              type: float
              nullable: N
              map: "OutFile1.CLM_SUBMT_ICD_VRSN_CD_SK"
            - name: CLM_TXNMY_CD
              type: varchar
              width: 20
              map: "OutFile1.CLM_TXNMY_CD"
            - name: BILL_PAYMT_EXCL_IN
              type: char
              width: 1
              nullable: N
              map: "OutFile1.BILL_PAYMT_EXCL_IN"
            - name: ADJ_FROM_CLM
              type: char
              width: 12
              nullable: N
              map: "OutFile1.ADJ_FROM_CLM"
            - name: ADJ_TO_CLM
              type: char
              width: 12
              nullable: N
              map: "OutFile1.ADJ_TO_CLM"
        - name: Trans6.V0S198P3.filter
          type: filter
          source: merge_load
          filter: "PCA_TYP_CD_SK <> 0 and PCA_TYP_CD_SK <> 1"
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: ADJ_FROM_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: ADJ_TO_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: ALPHA_PFX_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_EOB_EXCD_SK
              type: int
              width: 4
              nullable: N
            - name: CLS_SK
              type: int
              width: 4
              nullable: N
            - name: CLS_PLN_SK
              type: int
              width: 4
              nullable: N
            - name: EXPRNC_CAT_SK
              type: int
              width: 4
              nullable: N
            - name: FNCL_LOB_SK
              type: int
              width: 4
              nullable: N
            - name: GRP_SK
              type: int
              width: 4
              nullable: N
            - name: MBR_SK
              type: int
              width: 4
              nullable: N
            - name: NTWK_SK
              type: int
              width: 4
              nullable: N
            - name: PROD_SK
              type: int
              width: 4
              nullable: N
            - name: SUBGRP_SK
              type: int
              width: 4
              nullable: N
            - name: SUB_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_ACDNT_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_ACDNT_ST_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_ACTV_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_AGMNT_SRC_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_BTCH_ACTN_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_CAP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_CAT_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_CHK_CYC_OVRD_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_COB_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_FINL_DISP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_INPT_METH_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_INPT_SRC_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_INTER_PLN_PGM_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_NTWK_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_NONPAR_PROV_PFX_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_OTHR_BNF_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_PAYE_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_PRCS_CTL_AGNT_PFX_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_SVC_DEFN_PFX_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_SVC_PROV_SPEC_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_SVC_PROV_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_SUBMTTING_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_SUB_BCBS_PLN_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_SUBTYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: ATCHMT_IN
              type: char
              width: 1
              nullable: N
            - name: CLNCL_EDIT_IN
              type: char
              width: 1
              nullable: N
            - name: COBRA_CLM_IN
              type: char
              width: 1
              nullable: N
            - name: FIRST_PASS_IN
              type: char
              width: 1
              nullable: N
            - name: HOST_IN
              type: char
              width: 1
              nullable: N
            - name: LTR_IN
              type: char
              width: 1
              nullable: N
            - name: MCARE_ASG_IN
              type: char
              width: 1
              nullable: N
            - name: NOTE_IN
              type: char
              width: 1
              nullable: N
            - name: PCA_AUDIT_IN
              type: char
              width: 1
              nullable: N
            - name: PCP_SUBMT_IN
              type: char
              width: 1
              nullable: N
            - name: PROD_OOA_IN
              type: char
              width: 1
              nullable: N
            - name: ACDNT_DT_SK
              type: char
              width: 10
              nullable: N
            - name: INPT_DT_SK
              type: char
              width: 10
              nullable: N
            - name: MBR_PLN_ELIG_DT_SK
              type: char
              width: 10
              nullable: N
            - name: NEXT_RVW_DT_SK
              type: char
              width: 10
              nullable: N
            - name: PD_DT_SK
              type: char
              width: 10
              nullable: N
            - name: PAYMT_DRAG_CYC_DT_SK
              type: char
              width: 10
              nullable: N
            - name: PRCS_DT_SK
              type: char
              width: 10
              nullable: N
            - name: RCVD_DT_SK
              type: char
              width: 10
              nullable: N
            - name: SVC_STRT_DT_SK
              type: char
              width: 10
              nullable: N
            - name: SVC_END_DT_SK
              type: char
              width: 10
              nullable: N
            - name: SMLR_ILNS_DT_SK
              type: char
              width: 10
              nullable: N
            - name: STTUS_DT_SK
              type: char
              width: 10
              nullable: N
            - name: WORK_UNABLE_BEG_DT_SK
              type: char
              width: 10
              nullable: N
            - name: WORK_UNABLE_END_DT_SK
              type: char
              width: 10
              nullable: N
            - name: ACDNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: ACTL_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: CLM_CT
              type: smallint
              width: 2
              nullable: N
            - name: MBR_AGE
              type: decimal
              width: 3
            - name: ADJ_FROM_CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: ADJ_TO_CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: DOC_TX_ID
              type: char
              width: 18
              nullable: N
            - name: MCAID_RESUB_NO
              type: char
              width: 15
            - name: MCARE_ID
              type: char
              width: 12
              nullable: N
            - name: MBR_SFX_NO
              type: char
              width: 2
            - name: PATN_ACCT_NO
              type: varchar
              width: 35
            - name: PAYMT_REF_ID
              type: char
              width: 16
              nullable: N
            - name: PROV_AGMNT_ID
              type: char
              width: 12
              nullable: N
            - name: RFRNG_PROV_TX
              type: varchar
              width: 70
            - name: SUB_ID
              type: char
              width: 14
              nullable: N
            - name: PCA_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: REL_PCA_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: REL_BASE_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_UPDT_SW
              type: char
              width: 1
              nullable: N
            - name: CLCL_MICRO_ID
              type: char
              width: 18
              nullable: N
            - name: REMIT_SUPRSION_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: MCAID_STTUS_ID
              type: varchar
              width: 20
              nullable: N
            - name: PATN_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: CLM_SUBMT_ICD_VRSN_CD_SK
              type: float
              nullable: N
            - name: CLM_TXNMY_CD
              type: varchar
              width: 20
            - name: BILL_PAYMT_EXCL_IN
              type: char
              width: 1
              nullable: N
            - name: ADJ_FROM_CLM
              type: char
              width: 12
              nullable: N
            - name: ADJ_TO_CLM
              type: char
              width: 12
              nullable: N
          keys:
            - CLM_SK
        - name: merge_load1
          type: unknown
          source: ClmPcaUpdts.ClmPcaUpdtTrans5.lookupTrns1.lookupTrans7Tfrm_SrcSKhf_recycleTrans2.lookupfound_clms.ClmRvversalUpdatesTrans4.lookupnasco_found_clms.ClmNascoUpdatesNascoClmUpdateNasco_Edw_Rvrsl_Clm_Hit_ListEdw_Rvrsl_Clm_Hit_ListClmLoadFilePcaClmUpdatehf_claim_recycle_keys
          alias: 
          origType: CCollector
        - name: Link_Collector_237
          type: unknown
          source: ClmPcaUpdts.ClmPcaUpdtTrans5.lookupTrns1.lookupTrans7Tfrm_SrcSKhf_recycleTrans2.lookupfound_clms.ClmRvversalUpdatesTrans4.lookupnasco_found_clms.ClmNascoUpdatesNascoClmUpdateNasco_Edw_Rvrsl_Clm_Hit_ListEdw_Rvrsl_Clm_Hit_ListClmLoadFilePcaClmUpdatehf_claim_recycle_keys
          alias: 
          origType: CCollector
        - name: merge_load
          type: unknown
          source: ClmPcaUpdts.ClmPcaUpdtTrans5.lookupTrns1.lookupTrans7Tfrm_SrcSKhf_recycleTrans2.lookupfound_clms.ClmRvversalUpdatesTrans4.lookupnasco_found_clms.ClmNascoUpdatesNascoClmUpdateNasco_Edw_Rvrsl_Clm_Hit_ListEdw_Rvrsl_Clm_Hit_ListClmLoadFilePcaClmUpdatehf_claim_recycle_keys
          alias: 
          origType: CCollector


