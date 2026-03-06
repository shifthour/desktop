# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC ********************************************************************************
# MAGIC © Copyright 2005 Blue Cross/Blue Shield of Kansas City
# MAGIC © Copyright 2009 Blue Cross/Blue Shield of Kansas City
# MAGIC © Copyright 2021 Blue Cross/Blue Shield of Kansas City
# MAGIC © Copyright 2023 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  IdsClmLnFkey
# MAGIC CALLED BY:  IdsEmrProcedureClmLnLoadSeq
# MAGIC DESCRIPTION:
# MAGIC \(9)Foreign key and code lookup for Claim Line data in 
# MAGIC \(9)preparation for load into IDS table CLM_LN
# MAGIC 
# MAGIC PRIMARY SOURCE:   .../key/FctsClmLnExtr.FctsClmLn.uniq
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Programmer  \(9)Date\(9)\(9)Project\(9)\(9)Comment\(9)\(9)\(9)\(9)\(9)\(9)\(9)\(9)\(9)Environment                Reviewer               Date
# MAGIC =========================================================================================================================================================================
# MAGIC Kevin Soderlund\(9)03/09/2004\(9)\(9)\(9)Originally Programmed             
# MAGIC Kevin Soderlund\(9)05/01/2004\(9)\(9)\(9)Coding complete for release 1.0
# MAGIC Kevin Soderlund\(9)06/28/2004\(9)\(9)\(9)Updated with changes for release 1.1
# MAGIC Kevin Soderlund\(9)09/01/2004\(9)\(9)\(9)Added claim type to metadata; Updated call for procedure code
# MAGIC Brent Leland            09/07/2004             \(9)\(9)Added default rows for UNK and NA
# MAGIC Kevin Soderlund\(9)09/29/2004\(9)\(9)\(9)Claim Line pricing source - changed "FACETS" source to check source code (verified by Mike)
# MAGIC SHaron Andrew \(9)02/01/2005\(9)IDS 3.0
# MAGIC  BJ Luce                   1/23/2006               \(9)\(9)add columns CLM_LN_SVC_LOC_TYP_CD_SK, CLM_LN_SVC_PRICE_RULE_CD_SK 
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)and NON_PAR_SAV_AMT
# MAGIC                                                                 \(9)\(9) set SVC_PRICE_RULE_ID to spaces
# MAGIC                                                                  \(9)\(9)change format of CRF and the recycle records for SVC_LOC_TYP_CD and NON_PAR_SAV_AMT
# MAGIC Brent Leland           03/28/2006              \(9)\(9)Removed unneed logic in Trans4 for GetRecycleKey() call
# MAGIC Brent Leland           08/07/2006              \(9)\(9)Added current run cycle to output records.  Without the current run cycle, error recycle records 
# MAGIC                                                                  \(9)\(9)would have an old run cycle and would be removed in delete process.
# MAGIC                                                                 \(9)\(9)Changed file path parameter to evironment variable. 
# MAGIC Bhoomi Dasari        03/12/2007              \(9)\(9)Using PCS_PHAR_NBR and SRC _SYS_CD - 'NABP' for looking up the provider sk on the 
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)PROV table for SVC_PROV_SK
# MAGIC                                                                 \(9)\(9)Retrieve sk for 'ACPTD' with target domain - IDS.Claim.finalize.diposition for 
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)CLM_LN_FINL_DISP_CD_SK
# MAGIC                                                                 \(9)\(9) Code review by Steph Goddard 4/6/07
# MAGIC SAndrew               2008-10-13              #3784 PBM Vndr       Added SrcSysCd =ESI conditions for SrvcProvSK\(9)\(9)\(9)\(9)\(9)devlIDSnew               Steph Goddard  10/27/2008
# MAGIC SAndrew               2009-02-24              #3494 Labor Accnt     Added SrcSysCd =WellDyneRX conditions for SrvcProvSK\(9)\(9)\(9)\(9)devlIDS                      Steph Goddard  03/30/2009
# MAGIC                                                                  \(9)\(9)IF (\(9)ClmLn1.SRC_SYS_CD = "ESI" \(9)\(9)or 
# MAGIC \(9)                                                                 \(9)\(9)ClmLn1.SRC_SYS_CD = "WELLDYNERX"  \(9)or 
# MAGIC \(9)                                                                 \(9)\(9)ClmLn1.SRC_SYS_CD = "PCS" \(9)\(9)OR 
# MAGIC \(9)                                                                 \(9)\(9)ClmLn1.SRC_SYS_CD = "ARGUS"
# MAGIC \(9)                                                                 \(9)\(9))  then GetFkeyProv("NABP", ClmLn1.CLM_LN_SK,  trim ( ClmLn1.SVC_PROV_ID) , Logging) 
# MAGIC                                                                  \(9)\(9)else  
# MAGIC                                                                  \(9)\(9)    GetFkeyProv("FACETS", ClmLn1.CLM_LN_SK,  trim ( ClmLn1.SVC_PROV_ID) , Logging)
# MAGIC Kalyan Neelam     2009-10-07             4098                             Added SrcSysCd =MCSOURCE conditions for SrvcProvSK\(9)\(9)\(9)\(9)devlIDS                    Steph Goddard      10/15/2009
# MAGIC Kalyan Neelam     2009-12-29             4110                             Added SrcSysCd =MCAID conditions for SrvcProvSK\(9)\(9)\(9)\(9)IntegrateCurDevl \(9)  Steph Goddard       01/11/2010\(9)
# MAGIC Brent/Kalyan        2010-02-17            TTR-729                        Added Caremark to service provider logic\(9)\(9)\(9)\(9)\(9)IntegrateCurDevl    Steph Goddard       02/24/2010
# MAGIC Kalyan Neelam     2010-12-22             4616                            Added SrcSysCd =MEDTRAK conditions for SrvcProvSK\(9)\(9)\(9)\(9)IntegrateNewDevl    Steph Goddard       12/23/2010
# MAGIC Rick Henry           2012-05-02              4896                            Proc_Cd_Typ_Cd and Proc_Cd_Cat_Cd  for Proc_Cd_Sk lookup\(9)\(9)\(9)NewDevl                    SAndrew               2012-05-18
# MAGIC Kalyan Neelam     2013-01-28    4963 VBB Phase 3              Added 3 new columns on end - VBB_RULE_ID,\(9)\(9)\(9)\(9)\(9)IntegrateNewDevl        Bhoomi Dasari       3/14/2013
# MAGIC                                                                                                                       VBB_EXCD_ID, CLM_LN_VBB_IN
# MAGIC Kalyan Neelam     2013-11-12     5056 FEP Claims               Added SrcSysCd =BCA conditions for SrvcProvSK\(9)\(9)\(9)\(9)\(9)IntegrateNewDevl            Bhoomi Dasari      11/30/2013
# MAGIC Manasa Andru      2014-10-17     TFS - 9580                        Added 2 new fields(ITS_SUPLMT_DSCNT_AMT and\(9)\(9)\(9)\(9)IntegrateCurDevl              Kalyan Neelam        2014-10-22
# MAGIC                                                                                                             ITS_SRCHRG_AMT) at the end.
# MAGIC Manasa Andru      2014-11-17   TFS - 9580(Post Prod Work)                 Added scale of 2 for the 2 new fields\(9)\(9)\(9)\(9)\(9)IntegrateCurDevl              Kalyan Neelam        2014-11-19
# MAGIC                                                                                                (ITS_SUPLMT_DSCNT_AMT and ITS_SRCHRG_AMT) 
# MAGIC Kalyan Neelam       2014-12-17           5212                  Added If SRC_SYS_CD = 'BCBSA' Then 'BCA' Else SRC_SYS_CD\(9)\(9)\(9)\(9)IntegrateCurDevl              Bhoomi Dasari             02/04/2015                                                     
# MAGIC                                                                                      in the stage variables and pass it to GetFkeyCodes because code sets are created under 
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)BCA for BCBSA
# MAGIC Hari Pinnaka          2017-08-07      5792                      Added 3 new fields (NDC_SK ,NDC_DRUG_FORM_CD_SK,\(9)\(9)\(9)\(9)IntegrateDev1               Kalyan Neelam        2017-08-29
# MAGIC                                                                                     NDC_UNIT_CT) at the end
# MAGIC Sudhir Bomshetty    2017-10-20        5781                          Added SRC_SYS_CD = 'BCA' check for SvcProvSk stage variable\(9)\(9)\(9)IntegrateDev2               Kalyan Neelam        2017-10-23  
# MAGIC Jaideep Mankala      2017-11-20      5828 \(9)   Added new field to identify MED / PDX claim\(9)\(9)\(9)\(9)\(9)\(9)IntegrateDev2                 Kalyan Neelam        2017-11-20
# MAGIC                                                       \(9)\(9)     when passing to Fkey job
# MAGIC Sudhir Bomshetty   2018-01-12        5781                          Added SRC_SYS_CD = 'BCA' check for SVC_END_DT_SK\(9)\(9)\(9)\(9)IntegrateDev2                          Kalyan Neelam        2018-01-16
# MAGIC 
# MAGIC Programmer  \(9)\(9)Date\(9)\(9)Project\(9)\(9)Comment\(9)\(9)\(9)\(9)\(9)\(9)\(9)\(9)\(9)\(9)Environment\(9)Reviewer               \(9)Date
# MAGIC ====================================================================================================================================================================================
# MAGIC Manasa Andru\(9)\(9)2018-03-04\(9)TFS21142\(9)Modified SvcProvSk stage variable to not log errors when looking Fkey in If logic\(9)\(9)\(9)IntegrateDev2\(9)Kalyan Neelam\(9)2018-03-12
# MAGIC Madhavan B\(9)\(9)2018-02-06\(9)5792\(9)\(9)Changed the datatype of the column\(9)\(9)\(9)\(9)\(9)\(9)\(9)IntegrateDev1\(9)Kalyan Neelam\(9)2018-02-08
# MAGIC                                                       \(9)\(9)     \(9)\(9)NDC_UNIT_CT from SMALLINT to DECIMAL (21,10)
# MAGIC Kaushik Kapoor\(9)\(9)2018-03-16\(9)5828\(9)\(9)Adding SrcSysCd = SAVRX and LDI to SvcProvSk stage variable\(9)\(9)\(9)\(9)IntegrateDev2\(9)Jaideep Mankala\(9)03/21/2018
# MAGIC Sudhir Bomshetty\(9)\(9)2018-03-30\(9)5781\(9)\(9)Modified stage variable "SvcProvSk" for BCA to get values for BCA if NABP retruning 0 for Fkey\(9)IntegrateDev2\(9)Jaideep Mankala\(9)04/02/2018      
# MAGIC Kaushik Kapoor\(9)\(9)2018-09-20\(9)5828\(9)\(9)Adding SrcSysCd = CVS to SvcProvSk stage variable\(9)\(9)\(9)\(9)\(9)IntegrateDev2\(9)Kalyan Neelam\(9)2018-10-01   
# MAGIC Deepa Bajaj\(9)\(9)2019-10-11\(9)6131\(9)\(9)Added "OPTUMRX" Value to 'SvcProvsk' field under the stage variables in 'Trns1' \(9)\(9)IntegrateDev5\(9)Kalyan Neelam\(9)2019-11-20
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)Transformer phase.  PBM Replacement 
# MAGIC Goutham Kalidindi\(9)\(9)2020-11-12    \(9)US-283560\(9)Added MEDIMPACT to stageVariable SVC_PROV_SK\(9)\(9)\(9)\(9)\(9)IntegrateDev2\(9)Kalyan Neelam\(9)2021-02-04
# MAGIC Rekha Radhakrishna\(9)2020-08-17 \(9)6131\(9)\(9)Added new fields APC_ID and APC_STTUS_ID\(9)\(9)\(9)\(9)\(9)\(9)IntegrateDev2           
# MAGIC Goutham Kalidindi\(9)\(9)2021-01-14     \(9)US-318408\(9)LUMERIS (FACETS) Codes lookup\(9)\(9)\(9)\(9)\(9)\(9)\(9)IntegrateDev2\(9)Kalyan Neelam\(9)2021-01-14
# MAGIC Vikas Abbu\(9)\(9)2021-02-04        \(9)RA\(9)\(9)Added DentaQuest and EyeMed src cd in SVC_PRICE_RULE_ID logic\(9)\(9)\(9)IntegrateDev2\(9)Reddy Sanam\(9)2021-02-10
# MAGIC Goutham Kalidindi\(9)\(9)2021-02-16\(9)US-352900\(9)LHO Lumeris Codes lookup Conditions\(9)\(9)\(9)\(9)\(9)\(9)\(9)IntegrateDev2\(9)Kalyan Neelam\(9)2021-02-18
# MAGIC                                                                                   \(9)\(9)Changes for "CLAIM LINE ROOM TYPE PRICING METHOD"
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)"CLAIM LINE TYPE OF SERVICE"
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)"SERVICE PRICING RULE"
# MAGIC Mrudula Kodali\(9)\(9)2021-03-07   \(9)US-356530\(9)Added Source Sys cd values for EMR Procedures\(9)\(9)\(9)\(9)\(9)\(9)IntegrateDev2\(9)Reddy Sanam\(9)2021-03-08
# MAGIC Mrudula Kodali\(9)\(9)2021-03-11    \(9)US-311337\(9)Added Livongo Source system code in clmlneobexcdsk, clmlnposcdsk, clmlnunittypcdsk,\(9)\(9)IntegrateDev2\(9)Kalyan Neelam\(9)021-03-17
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)ClmLnLobCdSk               
# MAGIC Mrudula Kodali\(9)\(9)2021-03-23   \(9)US-311337\(9)Updated svCdMpngSrcSysCd and removed LVNGHLTH in this variable not to use FACETS\(9)\(9)IntegrateDev2\(9)Hugh Sisson\(9)2021-03-25
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)Updated ClmLnunitTypCdSk to use LVNGHLTH 
# MAGIC Vikas Abbu\(9)\(9)2021-03-29  \(9)US-311872\(9)Added SOLUTRAN Default value  'NA' in SVC_PRICE_RULE_ID Column\(9)\(9)\(9)IntegrateDev2\(9)Jaideep Mankala\(9)03/29/2021
# MAGIC Mrudula Kodali\(9)\(9)05/03/2021  \(9)US-373652\(9)Updated the stage variables\(9)\(9)\(9)\(9)\(9)\(9)\(9)\(9)IntegrateDev2\(9)Jaideep Mankala\(9)05/10/2021
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)Removed usages of FACETS for EYEMED, DENTAQUEST, SOLUTRON, LVNGHLTH, EMR
# MAGIC Goutham K\(9)\(9)2021-06-02\(9)US-366403\(9)Added 'EYEMED' for SVC_PROV_ID Fkey Stage Variable\(9)\(9)\(9)\(9)\(9)IntegrateDev1\(9)Jeyaprasanna\(9)2021-06-08
# MAGIC Vikas Abbu\(9)\(9)2021-06-21\(9)RA\(9)\(9)Updated CLM_LN_LOB_CD logic for EYEMED to use FACETS\(9)\(9)\(9)\(9)IntegrateDev1\(9)Manasa Andru\(9)2021-06-30     
# MAGIC Venkata Yama\(9)\(9)2022-01-12\(9)us480876\(9)\(9)Added src_sys_cd='HCA'\(9)\(9)\(9)\(9)\(9)\(9)\(9)\(9)IntegrateDev2\(9)Harsha Ravuri\(9)2022-01-13 
# MAGIC Manisha Gandra\(9)\(9)2022-01-15\(9)US 459610\(9)Added Nations Source System Code in "Trns1" stage ClmLnLobCdSK variable\(9)\(9)\(9)IntegrateDev2\(9)Jeyaprasanna\(9)2022-01-20
# MAGIC Ken Bradmon\(9)\(9)2023-08-07\(9)us559895\(9)\(9)Added 8 new Providers to the svEMRSrcSysCd variable of the Trns1 stage.\(9)\(9)\(9)IntegrateDev2         Reddy Sanam          2023-10-27\(9)\(9)
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)CENTRUSHEALTH, MOHEALTH, GOLDENVALLEY, JEFFERSON, 
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)WESTMOMEDCNTR, BLUESPRINGS, EXCELSIOR, and HARRISONVILLE.
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)This is the only change.
# MAGIC Deepika C               \(9)\(9)2023-08-01      \(9)US 589700             Added two new fields SNOMED_CT_CD,CVX_VCCN_CD                                                       \(9)IntegrateDevB\(9)Harsha Ravuri\(9)2023-09-20
# MAGIC 
# MAGIC Sham Shankaranarayana       2023-12-21               US 599810             Added new Stage variable svSBVSrcSysCd and updated existing StageVar 
# MAGIC                                                                                                                 ClmLnLobCdSk,SvcProvSk in Transformer Trns1                                                                                    IntegrateDev1         Jeyaprasanna         2023-12-21
# MAGIC Deepika C                            2024-04-09               US 614484               Updated StageVar ClmLnLobCdSk,SvcProvSk in Transformer Trns1 - added NATIONBFTSFLEX         IntegrateDev2         Jeyaprasanna         2024-04-10
# MAGIC Harsha Ravuri\(9)\(9)2025-05-06\(9)US#649651\(9)Added new provider 'ASCENTIST' to the svEMRSrcSysCd variable of the Trns1 stage.\(9)\(9)IntegrateDev2         Jeyaprasanna         2025-05-20

# MAGIC Added new field MED_PDX_IND to the common layout file that is sent to IdsClmLnFkey job
# MAGIC which indicates whether the claim is Medical or Pharmacy
# MAGIC Indicator used :
# MAGIC MED - medical
# MAGIC PDX - Pharmacy
# MAGIC Read common record format file from extract job.
# MAGIC Merge source data with default rows
# MAGIC SrcSysCdSk lookup done here because of pseudo claim's multiple sources per batch
# MAGIC Writing Sequential File to /load
# MAGIC This hash file is written to by all claim foriegn key jobs.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, TimestampType,
    DecimalType, NumericType
)
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


RunCycle = get_widget_value("RunCycle","")
Logging = get_widget_value("Logging","")
Source = get_widget_value("Source","")
InFile = get_widget_value("InFile","")

schema_ClmLnExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", NumericType(38,10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("CLM_LN_SK", IntegerType(), False),
    StructField("CLM_ID", StringType(), False),
    StructField("CLM_LN_SEQ_NO", IntegerType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("PROC_CD", StringType(), False),
    StructField("SVC_PROV_ID", StringType(), False),
    StructField("CLM_LN_DSALW_EXCD", StringType(), False),
    StructField("CLM_LN_EOB_EXCD", StringType(), False),
    StructField("CLM_LN_FINL_DISP_CD", StringType(), False),
    StructField("CLM_LN_LOB_CD", StringType(), False),
    StructField("CLM_LN_POS_CD", StringType(), False),
    StructField("CLM_LN_PREAUTH_CD", StringType(), False),
    StructField("CLM_LN_PREAUTH_SRC_CD", StringType(), False),
    StructField("CLM_LN_PRICE_SRC_CD", StringType(), False),
    StructField("CLM_LN_RFRL_CD", StringType(), False),
    StructField("CLM_LN_RVNU_CD", StringType(), False),
    StructField("CLM_LN_ROOM_PRICE_METH_CD", StringType(), False),
    StructField("CLM_LN_ROOM_TYP_CD", StringType(), False),
    StructField("CLM_LN_TOS_CD", StringType(), False),
    StructField("CLM_LN_UNIT_TYP_CD", StringType(), False),
    StructField("CAP_LN_IN", StringType(), False),
    StructField("PRI_LOB_IN", StringType(), False),
    StructField("SVC_END_DT", TimestampType(), False),
    StructField("SVC_STRT_DT", TimestampType(), False),
    StructField("AGMNT_PRICE_AMT", DecimalType(38,10), False),
    StructField("ALW_AMT", DecimalType(38,10), False),
    StructField("CHRG_AMT", DecimalType(38,10), False),
    StructField("COINS_AMT", DecimalType(38,10), False),
    StructField("CNSD_CHRG_AMT", DecimalType(38,10), False),
    StructField("COPAY_AMT", DecimalType(38,10), False),
    StructField("DEDCT_AMT", DecimalType(38,10), False),
    StructField("DSALW_AMT", DecimalType(38,10), False),
    StructField("ITS_HOME_DSCNT_AMT", DecimalType(38,10), False),
    StructField("NO_RESP_AMT", DecimalType(38,10), False),
    StructField("MBR_LIAB_BSS_AMT", DecimalType(38,10), False),
    StructField("PATN_RESP_AMT", DecimalType(38,10), False),
    StructField("PAYBL_AMT", DecimalType(38,10), False),
    StructField("PAYBL_TO_PROV_AMT", DecimalType(38,10), False),
    StructField("PAYBL_TO_SUB_AMT", DecimalType(38,10), False),
    StructField("PROC_TBL_PRICE_AMT", DecimalType(38,10), False),
    StructField("PROFL_PRICE_AMT", DecimalType(38,10), False),
    StructField("PROV_WRT_OFF_AMT", DecimalType(38,10), False),
    StructField("RISK_WTHLD_AMT", DecimalType(38,10), False),
    StructField("SVC_PRICE_AMT", DecimalType(38,10), False),
    StructField("SUPLMT_DSCNT_AMT", DecimalType(38,10), False),
    StructField("ALW_PRICE_UNIT_CT", IntegerType(), False),
    StructField("UNIT_CT", IntegerType(), False),
    StructField("DEDCT_AMT_ACCUM_ID", StringType(), False),
    StructField("PREAUTH_SVC_SEQ_NO", StringType(), False),
    StructField("RFRL_SVC_SEQ_NO", StringType(), False),
    StructField("LMT_PFX_ID", StringType(), False),
    StructField("PREAUTH_ID", StringType(), False),
    StructField("PROD_CMPNT_DEDCT_PFX_ID", StringType(), False),
    StructField("PROD_CMPNT_SVC_PAYMT_ID", StringType(), False),
    StructField("RFRL_ID_TX", StringType(), False),
    StructField("SVC_ID", StringType(), False),
    StructField("SVC_PRICE_RULE_ID", StringType(), False),
    StructField("SVC_RULE_TYP_TX", StringType(), False),
    StructField("SVC_LOC_TYP_CD", StringType(), False),
    StructField("NON_PAR_SAV_AMT", DecimalType(38,10), False),
    StructField("PROC_CD_TYP_CD", StringType(), False),
    StructField("PROC_CD_CAT_CD", StringType(), False),
    StructField("VBB_RULE_ID", StringType(), False),
    StructField("VBB_EXCD_ID", StringType(), False),
    StructField("CLM_LN_VBB_IN", StringType(), False),
    StructField("ITS_SUPLMT_DSCNT_AMT", DecimalType(38,10), False),
    StructField("ITS_SRCHRG_AMT", DecimalType(38,10), False),
    StructField("NDC", StringType(), True),
    StructField("NDC_DRUG_FORM_CD", StringType(), True),
    StructField("NDC_UNIT_CT", DecimalType(38,10), True),
    StructField("MED_PDX_IND", StringType(), True),
    StructField("APC_ID", StringType(), True),
    StructField("APC_STTUS_ID", StringType(), True),
    StructField("SNOMED_CT_CD", StringType(), True),
    StructField("CVX_VCCN_CD", StringType(), True)
])

file_path_ClmLnExtr = f"{adls_path}/{InFile}"
df_ClmLnExtr = (
    spark.read.format("csv")
    .option("header", False)
    .option("quote", '"')
    .schema(schema_ClmLnExtr)
    .load(file_path_ClmLnExtr)
)

df_stagevars = (
    df_ClmLnExtr
    .withColumn(
        "svEMRSrcSysCd",
        F.when(
            trim(F.col("SRC_SYS_CD")).isin(
                'CLAYPLATTE','JAYHAWK','ENCOMPASS','LIBERTYHOSP','MERITAS','NORTHLAND',
                'OLATHEMED','PROVIDENCE','PROVSTLUKES','SUNFLOWER','TRUMAN','UNTDMEDGRP',
                'MOSAIC','BARRYPOINTE','PRIMEMO','SPIRA','NONSTDSUPLMTDATA','HCA',
                'LEAWOODFMLYCARE','CHILDRENSMERCY','CENTRUSHEALTH','MOHEALTH','GOLDENVALLEY',
                'JEFFERSON','WESTMOMEDCNTR','BLUESPRINGS','EXCELSIOR','HARRISONVILLE','ASCENTIST'
            ),
            F.lit("EMR")
        ).otherwise(F.lit(""))
    )
    .withColumn(
        "svSBVSrcSysCd",
        F.when(
            trim(F.col("SRC_SYS_CD")).isin('DOMINION','MARC'),
            F.lit("SBV")
        ).otherwise(F.lit(""))
    )
    .withColumn(
        "svCdMpngSrcSysCd",
        F.when(F.col("SRC_SYS_CD")=="BCBSA","BCA")
        .when(F.col("SRC_SYS_CD")=="LUMERIS","FACETS")
        .otherwise(trim(F.col("SRC_SYS_CD")))
    )
    .withColumn("SrcSysCd", trim(F.col("SRC_SYS_CD")))
    .withColumn(
        "SrcSysCdSk",
        GetFkeyCodes(
            F.lit("IDS"), 
            F.col("CLM_LN_SK"), 
            F.lit("SOURCE SYSTEM"), 
            F.col("SRC_SYS_CD"), 
            F.col("Logging")
        )
    )
    .withColumn(
        "ClmSk",
        GetFkeyClm(
            F.col("SrcSysCd"),
            F.col("CLM_LN_SK"),
            trim(F.col("CLM_ID")),
            F.col("Logging")
        )
    )
    .withColumn(
        "sProcCd",
        F.when(
            F.col("PROC_CD_TYP_CD") != "ICD10",
            F.col("PROC_CD").substr(F.lit(1),F.lit(5))
        ).otherwise(F.col("PROC_CD"))
    )
    .withColumn(
        "ProcCdSk",
        F.when(trim(F.col("sProcCd"))=="UNK", F.lit(0))
        .when(trim(F.col("sProcCd"))=="NA", F.lit(1))
        .otherwise(
            GetFkeyProcCd(
                F.lit("FACETS"),
                F.col("CLM_LN_SK"),
                F.col("sProcCd"),
                F.col("PROC_CD_TYP_CD"),
                F.col("PROC_CD_CAT_CD"),
                F.col("Logging")
            )
        )
    )
    .withColumn(
        "SvcProvSk",
        F.when(trim(F.col("SVC_PROV_ID"))=="UNK", F.lit(0))
        .when(trim(F.col("SVC_PROV_ID"))=="NA", F.lit(1))
        .otherwise(
            F.when(F.col("SRC_SYS_CD")=="NATIONBFTSOTC",
                GetFkeyProv("NATIONBFTSOTC",F.col("CLM_LN_SK"),trim(F.col("SVC_PROV_ID")),F.col("Logging"))
            ).when(F.col("SRC_SYS_CD")=="NATIONBFTSBBB",
                GetFkeyProv("NATIONBFTSBBB",F.col("CLM_LN_SK"),trim(F.col("SVC_PROV_ID")),F.col("Logging"))
            ).when(F.col("SRC_SYS_CD")=="NATIONBFTSRWD",
                GetFkeyProv("NATIONBFTSRWD",F.col("CLM_LN_SK"),trim(F.col("SVC_PROV_ID")),F.col("Logging"))
            ).when(F.col("SRC_SYS_CD")=="NATIONBFTSHRNG",
                GetFkeyProv("NATIONBFTSHRNG",F.col("CLM_LN_SK"),trim(F.col("SVC_PROV_ID")),F.col("Logging"))
            ).when(F.col("SRC_SYS_CD")=="NATIONBFTSFLEX",
                GetFkeyProv("NATIONBFTSFLEX",F.col("CLM_LN_SK"),trim(F.col("SVC_PROV_ID")),F.col("Logging"))
            ).when(F.col("SRC_SYS_CD")=="DOMINION",
                GetFkeyProv("DOMINION",F.col("CLM_LN_SK"),trim(Convert('ABCDEFGHIJKLMNOPQRSTUVWXYZ','',F.col("SVC_PROV_ID"))),F.col("Logging"))
            ).when(F.col("SRC_SYS_CD")=="MARC",
                GetFkeyProv("MARC",F.col("CLM_LN_SK"),trim(F.col("SVC_PROV_ID")),F.col("Logging"))
            ).when(F.col("SRC_SYS_CD")=="EYEMED",
                GetFkeyProv("EYEMED",F.col("CLM_LN_SK"),trim(F.col("SVC_PROV_ID")),F.col("Logging"))
            ).when(F.col("SRC_SYS_CD")=="SOLUTRAN",
                GetFkeyProv("SOLUTRAN",F.col("CLM_LN_SK"),trim(F.col("SVC_PROV_ID")),F.col("Logging"))
            ).when((F.col("SRC_SYS_CD")=="BCBSSC") & (F.col("MED_PDX_IND")=="MED"),
                GetFkeyProv("FACETS",F.col("CLM_LN_SK"),trim(F.col("SVC_PROV_ID")),F.col("Logging"))
            ).when(F.col("SRC_SYS_CD")=="BCBSSC",
                GetFkeyProv("NABP",F.col("CLM_LN_SK"),trim(F.col("SVC_PROV_ID")),F.col("Logging"))
            ).when((F.col("SRC_SYS_CD")=="BCA") & (GetFkeyProv("NABP",F.col("CLM_LN_SK"),trim(F.col("SVC_PROV_ID")),F.lit("X"))!=0),
                GetFkeyProv("NABP",F.col("CLM_LN_SK"),trim(F.col("SVC_PROV_ID")),F.col("Logging"))
            ).when((F.col("SRC_SYS_CD")=="BCA") & (GetFkeyProv("BCA",F.col("CLM_LN_SK"),trim(F.col("SVC_PROV_ID")),F.lit("X"))!=0),
                GetFkeyProv("BCA",F.col("CLM_LN_SK"),trim(F.col("SVC_PROV_ID")),F.col("Logging"))
            ).when((F.col("SRC_SYS_CD")=="BCA") & (GetFkeyProv("FACETS",F.col("CLM_LN_SK"),trim(F.col("SVC_PROV_ID")),F.lit("X"))!=0),
                GetFkeyProv("FACETS",F.col("CLM_LN_SK"),trim(F.col("SVC_PROV_ID")),F.col("Logging"))
            ).when((F.col("SRC_SYS_CD")=="BCBSA") & (F.col("MED_PDX_IND")=="MED"),
                GetFkeyProv("FACETS",F.col("CLM_LN_SK"),trim(F.col("SVC_PROV_ID")),F.col("Logging"))
            ).when(F.col("SRC_SYS_CD")=="BCBSA",
                GetFkeyProv("NABP",F.col("CLM_LN_SK"),trim(F.col("SVC_PROV_ID")),F.col("Logging"))
            ).when(
                F.col("SRC_SYS_CD").isin("ESI","OPTUMRX","MEDIMPACT","WELLDYNERX","PCS","CAREMARK","ARGUS","MCSOURCE","MCAID","MEDTRAK","SAVRX","LDI","CVS"),
                F.when(
                    ((F.col("SRC_SYS_CD").isin("MCAID","PCS","CAREMARK")) &
                     (GetFkeyProv("NABP",F.col("CLM_LN_SK"),trim(F.col("SVC_PROV_ID")),F.lit("X"))==0)),
                    GetFkeyProv("FACETS",F.col("CLM_LN_SK"),trim(F.col("SVC_PROV_ID")),F.col("Logging"))
                ).otherwise(
                    GetFkeyProv("NABP",F.col("CLM_LN_SK"),trim(F.col("SVC_PROV_ID")),F.col("Logging"))
                )
            ).otherwise(
                GetFkeyProv("FACETS",F.col("CLM_LN_SK"),trim(F.col("SVC_PROV_ID")),F.col("Logging"))
            )
        )
    )
    .withColumn(
        "ClmLnDsalwExcdSk",
        GetFkeyExcd(
            F.col("svCdMpngSrcSysCd"),
            F.col("CLM_LN_SK"),
            F.col("CLM_LN_DSALW_EXCD"),
            F.col("Logging")
        )
    )
    .withColumn(
        "ClmLnFinlDispCdSk",
        GetFkeyCodes(
            F.col("svCdMpngSrcSysCd"),
            F.col("CLM_LN_SK"),
            F.lit("CLAIM FINALIZE DISPOSITION"),
            F.col("CLM_LN_FINL_DISP_CD"),
            F.col("Logging")
        )
    )
    .withColumn(
        "ClmLnEobExcdSk",
        F.when(
            F.col("SRC_SYS_CD")=="LVNGHLTH",
            GetFkeyExcd("FACETS",F.col("CLM_LN_SK"),F.col("CLM_LN_EOB_EXCD"),F.col("Logging"))
        ).otherwise(
            GetFkeyExcd(F.col("SrcSysCd"),F.col("CLM_LN_SK"),F.col("CLM_LN_EOB_EXCD"),F.col("Logging"))
        )
    )
    .withColumn(
        "ClmLnLobCdSk",
        F.when(
            F.col("SRC_SYS_CD").isin("DENTAQUEST","SOLUTRAN","LVNGHLTH","EYEMED","NATIONBFTSBBB",
                                     "NATIONBFTSOTC","NATIONBFTSRWD","NATIONBFTSHRNG","NATIONBFTSFLEX")
            | (F.col("svSBVSrcSysCd")=="SBV"),
            GetFkeyCodes(
                F.lit("FACETS"),
                F.col("CLM_LN_SK"),
                F.lit("CLAIM LINE LOB"),
                F.col("CLM_LN_LOB_CD"),
                F.col("Logging")
            )
        ).otherwise(
            GetFkeyCodes(
                F.col("svCdMpngSrcSysCd"),
                F.col("CLM_LN_SK"),
                F.lit("CLAIM LINE LOB"),
                F.col("CLM_LN_LOB_CD"),
                F.col("Logging")
            )
        )
    )
    .withColumn(
        "ClmLnPosCdSk",
        GetFkeyCodes(
            F.col("svCdMpngSrcSysCd"),
            F.col("CLM_LN_SK"),
            F.lit("CLAIM LINE PLACE OF SERVICE"),
            F.col("CLM_LN_POS_CD"),
            F.col("Logging")
        )
    )
    .withColumn(
        "ClmLnPreauthCdSk",
        GetFkeyCodes(
            F.col("svCdMpngSrcSysCd"),
            F.col("CLM_LN_SK"),
            F.lit("CLAIM LINE PREAUTHORIZATION"),
            F.col("CLM_LN_PREAUTH_CD"),
            F.col("Logging")
        )
    )
    .withColumn(
        "ClmLnPreauthSrcCd",
        GetFkeyCodes(
            F.col("svCdMpngSrcSysCd"),
            F.col("CLM_LN_SK"),
            F.lit("CLAIM LINE PREAUTHORIZATION SOURCE"),
            F.col("CLM_LN_PREAUTH_SRC_CD"),
            F.col("Logging")
        )
    )
    .withColumn(
        "ClmLnPriceSrcCdSk",
        GetFkeyCodes(
            F.col("svCdMpngSrcSysCd"),
            F.col("CLM_LN_SK"),
            F.lit("CLAIM LINE PRICING SOURCE"),
            F.col("CLM_LN_PRICE_SRC_CD"),
            F.col("Logging")
        )
    )
    .withColumn(
        "ClmLnRflCdSk",
        GetFkeyCodes(
            F.col("svCdMpngSrcSysCd"),
            F.col("CLM_LN_SK"),
            F.lit("CLAIM LINE REFERRAL"),
            F.col("CLM_LN_RFRL_CD"),
            F.col("Logging")
        )
    )
    .withColumn(
        "ClmLnRvnuCdSk",
        GetFkeyRvnu(
            F.lit("FACETS"),
            F.col("CLM_LN_SK"),
            F.col("CLM_LN_RVNU_CD"),
            F.col("Logging")
        )
    )
    .withColumn(
        "ClmLnRoomPriceMethSk",
        F.when(
            (F.col("SRC_SYS_CD")=="LUMERIS") &
            (GetFkeyCodes(
                F.col("svCdMpngSrcSysCd"),
                F.col("CLM_LN_SK"),
                F.lit("CLAIM LINE ROOM TYPE PRICING METHOD"),
                F.col("CLM_LN_ROOM_PRICE_METH_CD"),
                F.col("Logging")
            )==0),
            GetFkeyCodes(
                F.lit("LUMERIS"),
                F.col("CLM_LN_SK"),
                F.lit("CLAIM LINE ROOM TYPE PRICING METHOD"),
                F.col("CLM_LN_ROOM_PRICE_METH_CD"),
                F.col("Logging")
            )
        ).otherwise(
            GetFkeyCodes(
                F.col("svCdMpngSrcSysCd"),
                F.col("CLM_LN_SK"),
                F.lit("CLAIM LINE ROOM TYPE PRICING METHOD"),
                F.col("CLM_LN_ROOM_PRICE_METH_CD"),
                F.col("Logging")
            )
        )
    )
    .withColumn(
        "ClmLnRoomTypCdSk",
        GetFkeyCodes(
            F.col("svCdMpngSrcSysCd"),
            F.col("CLM_LN_SK"),
            F.lit("CLAIM LINE ROOM TYPE"),
            F.col("CLM_LN_ROOM_TYP_CD"),
            F.col("Logging")
        )
    )
    .withColumn(
        "ClmLnTosCdSk",
        F.when(
            (F.col("SRC_SYS_CD")=="LUMERIS") &
            (GetFkeyCodes(
                F.col("svCdMpngSrcSysCd"),
                F.col("CLM_LN_SK"),
                F.lit("CLAIM LINE TYPE OF SERVICE"),
                F.col("CLM_LN_TOS_CD"),
                F.col("Logging")
            )==0),
            GetFkeyCodes(
                F.lit("LUMERIS"),
                F.col("CLM_LN_SK"),
                F.lit("CLAIM LINE TYPE OF SERVICE"),
                F.col("CLM_LN_TOS_CD"),
                F.col("Logging")
            )
        ).otherwise(
            GetFkeyCodes(
                F.col("svCdMpngSrcSysCd"),
                F.col("CLM_LN_SK"),
                F.lit("CLAIM LINE TYPE OF SERVICE"),
                F.col("CLM_LN_TOS_CD"),
                F.col("Logging")
            )
        )
    )
    .withColumn(
        "ClmLnunitTypCdSk",
        F.when(
            (F.col("SRC_SYS_CD")=="LVNGHLTH") | (F.col("svEMRSrcSysCd")=="EMR"),
            GetFkeyCodes(
                F.col("SRC_SYS_CD"),
                F.col("CLM_LN_SK"),
                F.lit("CLAIM LINE UNIT TYPE"),
                F.col("CLM_LN_UNIT_TYP_CD"),
                F.col("Logging")
            )
        ).otherwise(
            GetFkeyCodes(
                F.lit("FACETS"),
                F.col("CLM_LN_SK"),
                F.lit("CLAIM LINE UNIT TYPE"),
                F.col("CLM_LN_UNIT_TYP_CD"),
                F.col("Logging")
            )
        )
    )
    .withColumn(
        "ClmLnSvcLocTypCdSk",
        GetFkeyCodes(
            F.col("svCdMpngSrcSysCd"),
            F.col("CLM_LN_SK"),
            F.lit("SERVICE LOCATION TYPE"),
            F.col("SVC_LOC_TYP_CD"),
            F.col("Logging")
        )
    )
    .withColumn(
        "ClmLnSvcPriceRuleCdSk",
        F.when(
            (F.col("SRC_SYS_CD")=="LUMERIS") &
            (GetFkeyCodes(
                F.col("svCdMpngSrcSysCd"),
                F.col("CLM_LN_SK"),
                F.lit("SERVICE PRICING RULE"),
                F.col("SVC_PRICE_RULE_ID"),
                F.col("Logging")
            )==0),
            GetFkeyCodes(
                F.lit("LUMERIS"),
                F.col("CLM_LN_SK"),
                F.lit("SERVICE PRICING RULE"),
                F.col("SVC_PRICE_RULE_ID"),
                F.col("Logging")
            )
        ).otherwise(
            GetFkeyCodes(
                F.col("svCdMpngSrcSysCd"),
                F.col("CLM_LN_SK"),
                F.lit("SERVICE PRICING RULE"),
                F.col("SVC_PRICE_RULE_ID"),
                F.col("Logging")
            )
        )
    )
    .withColumn(
        "PriLobIn",
        F.when(F.col("PRI_LOB_IN")=="NA", "X").otherwise(F.col("PRI_LOB_IN"))
    )
    .withColumn(
        "SvcEndDt",
        GetFkeyDate(
            F.lit("IDS"),
            F.col("CLM_LN_SK"),
            F.col("SVC_END_DT"),
            F.col("Logging")
        )
    )
    .withColumn(
        "SvcStrtDt",
        GetFkeyDate(
            F.lit("IDS"),
            F.col("CLM_LN_SK"),
            F.col("SVC_STRT_DT"),
            F.col("Logging")
        )
    )
    .withColumn(
        "svVbbRuleSk",
        GetFkeyVbbRule(
            F.col("SrcSysCd"),
            F.col("CLM_LN_SK"),
            F.col("VBB_RULE_ID"),
            F.col("Logging")
        )
    )
    .withColumn(
        "svVbbExcdSk",
        GetFkeyExcd(
            F.col("SrcSysCd"),
            F.col("CLM_LN_SK"),
            F.col("VBB_EXCD_ID"),
            F.col("Logging")
        )
    )
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("CLM_LN_SK")))
    .withColumn(
        "svNDCSk",
        GetFkeyNDC(
            F.col("CLM_LN_SK"),
            F.col("NDC"),
            F.col("Logging")
        )
    )
    .withColumn(
        "svNDCDrugCDFormSk",
        GetFkeyClctnDomainCodes(
            F.col("svCdMpngSrcSysCd"),
            F.col("CLM_LN_SK"),
            F.lit("FACETS DBO"),
            F.lit("CLAIM SUBTYPE"),
            F.lit("IDS"),
            F.lit("CLAIM SUBTYPE"),
            F.col("NDC_DRUG_FORM_CD"),
            F.lit("Logging")
        )
    )
)

df_enriched = df_stagevars

df_output1 = (
    df_enriched
    .filter((F.col("ErrCount")==0) | (F.col("PassThru")=="Y"))
)

df_recycle1 = (
    df_enriched
    .filter(F.col("ErrCount")>0)
)

df_recycle_clms = (
    df_enriched
    .filter(F.col("ErrCount")>0)
    .select(
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("CLM_ID").alias("CLM_ID")
    )
)

row_window = Window.orderBy(F.lit(1))
df_enriched_withrow = df_enriched.withColumn("row_num", F.row_number().over(row_window))

df_defaultUNK_base = df_enriched_withrow.filter(F.col("row_num")==1)
df_defaultNA_base = df_enriched_withrow.filter(F.col("row_num")==1)

df_defaultUNK = df_defaultUNK_base.select(
    F.lit(0).alias("CLM_LN_SK"),
    F.lit(0).alias("SRC_SYS_CD_SK"),
    F.lit("UNK").alias("CLM_ID"),
    F.lit(0).alias("CLM_LN_SEQ_NO"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("CLM_SK"),
    F.lit(0).alias("PROC_CD_SK"),
    F.lit(0).alias("SVC_PROV_SK"),
    F.lit(0).alias("CLM_LN_DSALW_EXCD_SK"),
    F.lit(0).alias("CLM_LN_EOB_EXCD_SK"),
    F.lit(0).alias("CLM_LN_FINL_DISP_CD_SK"),
    F.lit(0).alias("CLM_LN_LOB_CD_SK"),
    F.lit(0).alias("CLM_LN_POS_CD_SK"),
    F.lit(0).alias("CLM_LN_PREAUTH_CD_SK"),
    F.lit(0).alias("CLM_LN_PREAUTH_SRC_CD_SK"),
    F.lit(0).alias("CLM_LN_PRICE_SRC_CD_SK"),
    F.lit(0).alias("CLM_LN_RFRL_CD_SK"),
    F.lit(0).alias("CLM_LN_RVNU_CD_SK"),
    F.lit(0).alias("CLM_LN_ROOM_PRICE_METH_CD_SK"),
    F.lit(0).alias("CLM_LN_ROOM_TYP_CD_SK"),
    F.lit(0).alias("CLM_LN_TOS_CD_SK"),
    F.lit(0).alias("CLM_LN_UNIT_TYP_CD_SK"),
    F.rpad(F.lit("U"),1," ").alias("CAP_LN_IN"),
    F.rpad(F.lit("U"),1," ").alias("PRI_LOB_IN"),
    F.rpad(F.lit("NA"),10," ").alias("SVC_END_DT_SK"),
    F.rpad(F.lit("NA"),10," ").alias("SVC_STRT_DT_SK"),
    F.lit(0).alias("AGMNT_PRICE_AMT"),
    F.lit(0).alias("ALW_AMT"),
    F.lit(0).alias("CHRG_AMT"),
    F.lit(0).alias("COINS_AMT"),
    F.lit(0).alias("CNSD_CHRG_AMT"),
    F.lit(0).alias("COPAY_AMT"),
    F.lit(0).alias("DEDCT_AMT"),
    F.lit(0).alias("DSALW_AMT"),
    F.lit(0).alias("ITS_HOME_DSCNT_AMT"),
    F.lit(0).alias("NO_RESP_AMT"),
    F.lit(0).alias("MBR_LIAB_BSS_AMT"),
    F.lit(0).alias("PATN_RESP_AMT"),
    F.lit(0).alias("PAYBL_AMT"),
    F.lit(0).alias("PAYBL_TO_PROV_AMT"),
    F.lit(0).alias("PAYBL_TO_SUB_AMT"),
    F.lit(0).alias("PROC_TBL_PRICE_AMT"),
    F.lit(0).alias("PROFL_PRICE_AMT"),
    F.lit(0).alias("PROV_WRT_OFF_AMT"),
    F.lit(0).alias("RISK_WTHLD_AMT"),
    F.lit(0).alias("SVC_PRICE_AMT"),
    F.lit(0).alias("SUPLMT_DSCNT_AMT"),
    F.lit(0).alias("ALW_PRICE_UNIT_CT"),
    F.lit(0).alias("UNIT_CT"),
    F.lit("UNK").alias("DEDCT_AMT_ACCUM_ID"),
    F.lit("UNK").alias("PREAUTH_SVC_SEQ_NO"),
    F.lit("UNK").alias("RFRL_SVC_SEQ_NO"),
    F.lit("UNK").alias("LMT_PFX_ID"),
    F.lit("UNK").alias("PREAUTH_ID"),
    F.lit("UNK").alias("PROD_CMPNT_DEDCT_PFX_ID"),
    F.lit("UNK").alias("PROD_CMPNT_SVC_PAYMT_ID"),
    F.lit("UNK").alias("RFRL_ID_TX"),
    F.lit("UNK").alias("SVC_ID"),
    F.rpad(F.lit(" "),4," ").alias("SVC_PRICE_RULE_ID"),
    F.lit("UNK").alias("SVC_RULE_TYP_TX"),
    F.lit(0).alias("CLM_LN_SVC_LOC_TYP_CD_SK"),
    F.lit(0).alias("CLM_LN_SVC_PRICE_RULE_CD_SK"),
    F.lit(0).alias("NON_PAR_SAV_AMT"),
    F.lit(0).alias("VBB_RULE_SK"),
    F.lit(0).alias("VBB_EXCD_SK"),
    F.rpad(F.lit("N"),1," ").alias("CLM_LN_VBB_IN"),
    F.lit(0).alias("ITS_SUPLMT_DSCNT_AMT"),
    F.lit(0).alias("ITS_SRCHRG_AMT"),
    F.lit(0).alias("NDC_SK"),
    F.lit(0).alias("NDC_DRUG_FORM_CD_SK"),
    F.lit(None).alias("NDC_UNIT_CT"),
    F.col("SNOMED_CT_CD").alias("APC_ID"),  # The DS job for these columns uses 'UNK' or 'NA' for others, but keeps SNOMED_CT_CD & CVX_VCCN_CD from ClmLn1
    F.col("CVX_VCCN_CD").alias("APC_STTUS_ID"),
    F.col("SNOMED_CT_CD").alias("SNOMED_CT_CD"),
    F.col("CVX_VCCN_CD").alias("CVX_VCCN_CD")
)

df_defaultNA = df_defaultNA_base.select(
    F.lit(1).alias("CLM_LN_SK"),
    F.lit(1).alias("SRC_SYS_CD_SK"),
    F.lit("NA").alias("CLM_ID"),
    F.lit(1).alias("CLM_LN_SEQ_NO"),
    F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("CLM_SK"),
    F.lit(1).alias("PROC_CD_SK"),
    F.lit(1).alias("SVC_PROV_SK"),
    F.lit(1).alias("CLM_LN_DSALW_EXCD_SK"),
    F.lit(1).alias("CLM_LN_EOB_EXCD_SK"),
    F.lit(1).alias("CLM_LN_FINL_DISP_CD_SK"),
    F.lit(1).alias("CLM_LN_LOB_CD_SK"),
    F.lit(1).alias("CLM_LN_POS_CD_SK"),
    F.lit(1).alias("CLM_LN_PREAUTH_CD_SK"),
    F.lit(1).alias("CLM_LN_PREAUTH_SRC_CD_SK"),
    F.lit(1).alias("CLM_LN_PRICE_SRC_CD_SK"),
    F.lit(1).alias("CLM_LN_RFRL_CD_SK"),
    F.lit(1).alias("CLM_LN_RVNU_CD_SK"),
    F.lit(1).alias("CLM_LN_ROOM_PRICE_METH_CD_SK"),
    F.lit(1).alias("CLM_LN_ROOM_TYP_CD_SK"),
    F.lit(1).alias("CLM_LN_TOS_CD_SK"),
    F.lit(1).alias("CLM_LN_UNIT_TYP_CD_SK"),
    F.rpad(F.lit("X"),1," ").alias("CAP_LN_IN"),
    F.rpad(F.lit("X"),1," ").alias("PRI_LOB_IN"),
    F.rpad(F.lit("NA"),10," ").alias("SVC_END_DT_SK"),
    F.rpad(F.lit("NA"),10," ").alias("SVC_STRT_DT_SK"),
    F.lit(0).alias("AGMNT_PRICE_AMT"),
    F.lit(0).alias("ALW_AMT"),
    F.lit(0).alias("CHRG_AMT"),
    F.lit(0).alias("COINS_AMT"),
    F.lit(0).alias("CNSD_CHRG_AMT"),
    F.lit(0).alias("COPAY_AMT"),
    F.lit(0).alias("DEDCT_AMT"),
    F.lit(0).alias("DSALW_AMT"),
    F.lit(0).alias("ITS_HOME_DSCNT_AMT"),
    F.lit(0).alias("NO_RESP_AMT"),
    F.lit(0).alias("MBR_LIAB_BSS_AMT"),
    F.lit(0).alias("PATN_RESP_AMT"),
    F.lit(0).alias("PAYBL_AMT"),
    F.lit(0).alias("PAYBL_TO_PROV_AMT"),
    F.lit(0).alias("PAYBL_TO_SUB_AMT"),
    F.lit(0).alias("PROC_TBL_PRICE_AMT"),
    F.lit(0).alias("PROFL_PRICE_AMT"),
    F.lit(0).alias("PROV_WRT_OFF_AMT"),
    F.lit(0).alias("RISK_WTHLD_AMT"),
    F.lit(0).alias("SVC_PRICE_AMT"),
    F.lit(0).alias("SUPLMT_DSCNT_AMT"),
    F.lit(0).alias("ALW_PRICE_UNIT_CT"),
    F.lit(0).alias("UNIT_CT"),
    F.lit("NA").alias("DEDCT_AMT_ACCUM_ID"),
    F.lit("NA").alias("PREAUTH_SVC_SEQ_NO"),
    F.lit("NA").alias("RFRL_SVC_SEQ_NO"),
    F.lit("NA").alias("LMT_PFX_ID"),
    F.lit("NA").alias("PREAUTH_ID"),
    F.lit("NA").alias("PROD_CMPNT_DEDCT_PFX_ID"),
    F.lit("NA").alias("PROD_CMPNT_SVC_PAYMT_ID"),
    F.lit("NA").alias("RFRL_ID_TX"),
    F.lit("NA").alias("SVC_ID"),
    F.rpad(F.lit(" "),4," ").alias("SVC_PRICE_RULE_ID"),
    F.lit("NA").alias("SVC_RULE_TYP_TX"),
    F.lit(1).alias("CLM_LN_SVC_LOC_TYP_CD_SK"),
    F.lit(1).alias("CLM_LN_SVC_PRICE_RULE_CD_SK"),
    F.lit(0).alias("NON_PAR_SAV_AMT"),
    F.lit(1).alias("VBB_RULE_SK"),
    F.lit(1).alias("VBB_EXCD_SK"),
    F.rpad(F.lit("N"),1," ").alias("CLM_LN_VBB_IN"),
    F.lit(0).alias("ITS_SUPLMT_DSCNT_AMT"),
    F.lit(0).alias("ITS_SRCHRG_AMT"),
    F.lit(1).alias("NDC_SK"),
    F.lit(1).alias("NDC_DRUG_FORM_CD_SK"),
    F.lit(None).alias("NDC_UNIT_CT"),
    F.col("SNOMED_CT_CD").alias("APC_ID"),
    F.col("CVX_VCCN_CD").alias("APC_STTUS_ID"),
    F.col("SNOMED_CT_CD").alias("SNOMED_CT_CD"),
    F.col("CVX_VCCN_CD").alias("CVX_VCCN_CD")
)

df_output1_sel = df_output1.select(
    F.col("CLM_LN_SK"),
    F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID"),
    F.col("CLM_LN_SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("ClmSk").alias("CLM_SK"),
    F.col("ProcCdSk").alias("PROC_CD_SK"),
    F.col("SvcProvSk").alias("SVC_PROV_SK"),
    F.col("ClmLnDsalwExcdSk").alias("CLM_LN_DSALW_EXCD_SK"),
    F.col("ClmLnEobExcdSk").alias("CLM_LN_EOB_EXCD_SK"),
    F.col("ClmLnFinlDispCdSk").alias("CLM_LN_FINL_DISP_CD_SK"),
    F.col("ClmLnLobCdSk").alias("CLM_LN_LOB_CD_SK"),
    F.col("ClmLnPosCdSk").alias("CLM_LN_POS_CD_SK"),
    F.col("ClmLnPreauthCdSk").alias("CLM_LN_PREAUTH_CD_SK"),
    F.col("ClmLnPreauthSrcCd").alias("CLM_LN_PREAUTH_SRC_CD_SK"),
    F.col("ClmLnPriceSrcCdSk").alias("CLM_LN_PRICE_SRC_CD_SK"),
    F.col("ClmLnRflCdSk").alias("CLM_LN_RFRL_CD_SK"),
    F.col("ClmLnRvnuCdSk").alias("CLM_LN_RVNU_CD_SK"),
    F.col("ClmLnRoomPriceMethSk").alias("CLM_LN_ROOM_PRICE_METH_CD_SK"),
    F.col("ClmLnRoomTypCdSk").alias("CLM_LN_ROOM_TYP_CD_SK"),
    F.col("ClmLnTosCdSk").alias("CLM_LN_TOS_CD_SK"),
    F.col("ClmLnunitTypCdSk").alias("CLM_LN_UNIT_TYP_CD_SK"),
    F.rpad(F.col("CAP_LN_IN"),1," ").alias("CAP_LN_IN"),
    F.rpad(F.col("PriLobIn"),1," ").alias("PRI_LOB_IN"),
    F.rpad(
        F.when(
            (F.col("SRC_SYS_CD").isin("BCA","EYEMED")) & (F.col("SvcEndDt")=="UNK"),
            F.lit("2199-12-31")
        ).otherwise(F.col("SvcEndDt")),
        10," "
    ).alias("SVC_END_DT_SK"),
    F.rpad(F.col("SvcStrtDt"),10," ").alias("SVC_STRT_DT_SK"),
    F.col("AGMNT_PRICE_AMT"),
    F.col("ALW_AMT"),
    F.col("CHRG_AMT"),
    F.col("COINS_AMT"),
    F.col("CNSD_CHRG_AMT"),
    F.col("COPAY_AMT"),
    F.col("DEDCT_AMT"),
    F.col("DSALW_AMT"),
    F.col("ITS_HOME_DSCNT_AMT"),
    F.col("NO_RESP_AMT"),
    F.col("MBR_LIAB_BSS_AMT"),
    F.col("PATN_RESP_AMT"),
    F.col("PAYBL_AMT"),
    F.col("PAYBL_TO_PROV_AMT"),
    F.col("PAYBL_TO_SUB_AMT"),
    F.col("PROC_TBL_PRICE_AMT"),
    F.col("PROFL_PRICE_AMT"),
    F.col("PROV_WRT_OFF_AMT"),
    F.col("RISK_WTHLD_AMT"),
    F.col("SVC_PRICE_AMT"),
    F.col("SUPLMT_DSCNT_AMT"),
    F.col("ALW_PRICE_UNIT_CT"),
    F.col("UNIT_CT"),
    F.col("DEDCT_AMT_ACCUM_ID"),
    F.col("PREAUTH_SVC_SEQ_NO"),
    F.col("RFRL_SVC_SEQ_NO"),
    F.col("LMT_PFX_ID"),
    F.col("PREAUTH_ID"),
    F.col("PROD_CMPNT_DEDCT_PFX_ID"),
    F.col("PROD_CMPNT_SVC_PAYMT_ID"),
    F.col("RFRL_ID_TX"),
    F.col("SVC_ID"),
    F.rpad(
        F.when(
            F.col("SRC_SYS_CD").isin("BCBSSC","OPTUMRX","BCBSA","BCA","EYEMED","DENTAQUEST","LVNGHLTH","SOLUTRAN"),
            F.lit("NA")
        ).otherwise(F.lit(" "))
        ,4," "
    ).alias("SVC_PRICE_RULE_ID"),
    F.col("SVC_RULE_TYP_TX"),
    F.col("ClmLnSvcLocTypCdSk").alias("CLM_LN_SVC_LOC_TYP_CD_SK"),
    F.col("ClmLnSvcPriceRuleCdSk").alias("CLM_LN_SVC_PRICE_RULE_CD_SK"),
    F.col("NON_PAR_SAV_AMT"),
    F.col("svVbbRuleSk").alias("VBB_RULE_SK"),
    F.col("svVbbExcdSk").alias("VBB_EXCD_SK"),
    F.rpad(F.col("CLM_LN_VBB_IN"),1," ").alias("CLM_LN_VBB_IN"),
    F.col("ITS_SUPLMT_DSCNT_AMT"),
    F.col("ITS_SRCHRG_AMT"),
    F.col("svNDCSk").alias("NDC_SK"),
    F.col("svNDCDrugCDFormSk").alias("NDC_DRUG_FORM_CD_SK"),
    F.col("NDC_UNIT_CT"),
    F.col("APC_ID"),
    F.col("APC_STTUS_ID"),
    F.col("SNOMED_CT_CD"),
    F.col("CVX_VCCN_CD")
)

df_collector = df_output1_sel.unionByName(df_defaultUNK).unionByName(df_defaultNA)

file_path_recycle = "hf_recycle.parquet"
write_files(
    df_recycle1.select(
        F.col("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD"),
        F.rpad(F.col("DISCARD_IN"),1," ").alias("DISCARD_IN"),
        F.rpad(F.col("PASS_THRU_IN"),1," ").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT"),
        F.col("ErrCount").alias("ERR_CT"),
        (F.col("RECYCLE_CT") + F.lit(1)).alias("RECYC_CT"),
        F.col("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
        F.col("CLM_LN_SK"),
        F.col("CLM_ID"),
        F.col("CLM_LN_SEQ_NO"),
        F.col("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("PROC_CD"),
        F.col("SVC_PROV_ID"),
        F.col("CLM_LN_DSALW_EXCD"),
        F.col("CLM_LN_EOB_EXCD"),
        F.col("CLM_LN_FINL_DISP_CD"),
        F.col("CLM_LN_LOB_CD"),
        F.col("CLM_LN_POS_CD"),
        F.col("CLM_LN_PREAUTH_CD"),
        F.col("CLM_LN_PREAUTH_SRC_CD"),
        F.col("CLM_LN_PRICE_SRC_CD"),
        F.col("CLM_LN_RFRL_CD"),
        F.col("CLM_LN_RVNU_CD"),
        F.rpad(F.col("CLM_LN_ROOM_PRICE_METH_CD"),2," ").alias("CLM_LN_ROOM_PRICE_METH_CD"),
        F.col("CLM_LN_ROOM_TYP_CD"),
        F.col("CLM_LN_TOS_CD"),
        F.col("CLM_LN_UNIT_TYP_CD"),
        F.rpad(F.col("CAP_LN_IN"),1," ").alias("CAP_LN_IN"),
        F.rpad(F.col("PRI_LOB_IN"),1," ").alias("PRI_LOB_IN"),
        F.col("SVC_END_DT"),
        F.col("SVC_STRT_DT"),
        F.col("AGMNT_PRICE_AMT"),
        F.col("ALW_AMT"),
        F.col("CHRG_AMT"),
        F.col("COINS_AMT"),
        F.col("CNSD_CHRG_AMT"),
        F.col("COPAY_AMT"),
        F.col("DEDCT_AMT"),
        F.col("DSALW_AMT"),
        F.col("ITS_HOME_DSCNT_AMT"),
        F.col("NO_RESP_AMT"),
        F.col("MBR_LIAB_BSS_AMT"),
        F.col("PATN_RESP_AMT"),
        F.col("PAYBL_AMT"),
        F.col("PAYBL_TO_PROV_AMT"),
        F.col("PAYBL_TO_SUB_AMT"),
        F.col("PROC_TBL_PRICE_AMT"),
        F.col("PROFL_PRICE_AMT"),
        F.col("PROV_WRT_OFF_AMT"),
        F.col("RISK_WTHLD_AMT"),
        F.col("SVC_PRICE_AMT"),
        F.col("SUPLMT_DSCNT_AMT"),
        F.col("ALW_PRICE_UNIT_CT"),
        F.col("UNIT_CT"),
        F.rpad(F.col("DEDCT_AMT_ACCUM_ID"),4," ").alias("DEDCT_AMT_ACCUM_ID"),
        F.rpad(F.col("PREAUTH_SVC_SEQ_NO"),4," ").alias("PREAUTH_SVC_SEQ_NO"),
        F.rpad(F.col("RFRL_SVC_SEQ_NO"),4," ").alias("RFRL_SVC_SEQ_NO"),
        F.rpad(F.col("LMT_PFX_ID"),4," ").alias("LMT_PFX_ID"),
        F.rpad(F.col("PREAUTH_ID"),9," ").alias("PREAUTH_ID"),
        F.rpad(F.col("PROD_CMPNT_DEDCT_PFX_ID"),4," ").alias("PROD_CMPNT_DEDCT_PFX_ID"),
        F.rpad(F.col("PROD_CMPNT_SVC_PAYMT_ID"),4," ").alias("PROD_CMPNT_SVC_PAYMT_ID"),
        F.rpad(F.col("RFRL_ID_TX"),9," ").alias("RFRL_ID_TX"),
        F.rpad(F.col("SVC_ID"),4," ").alias("SVC_ID"),
        F.rpad(F.col("SVC_PRICE_RULE_ID"),4," ").alias("SVC_PRICE_RULE_ID"),
        F.rpad(F.col("SVC_RULE_TYP_TX"),3," ").alias("SVC_RULE_TYP_TX"),
        F.rpad(F.col("SVC_LOC_TYP_CD"),20," ").alias("SVC_LOC_TYP_CD"),
        F.col("NON_PAR_SAV_AMT"),
        F.col("PROC_CD_TYP_CD"),
        F.col("PROC_CD_CAT_CD"),
        F.col("VBB_RULE_ID"),
        F.col("VBB_EXCD_ID"),
        F.rpad(F.col("CLM_LN_VBB_IN"),1," ").alias("CLM_LN_VBB_IN"),
        F.col("ITS_SUPLMT_DSCNT_AMT"),
        F.col("ITS_SRCHRG_AMT"),
        F.col("NDC").alias("NDC_SK"),
        F.col("NDC_DRUG_FORM_CD").alias("NDC_DRUG_FORM_CD_SK"),
        F.col("NDC_UNIT_CT")
    ),
    f"{file_path_recycle}",
    ",",
    "overwrite",
    True,
    False,
    "\"",
    None
)

file_path_recycle_clms = "hf_claim_recycle_keys.parquet"
write_files(
    df_recycle_clms,
    f"{file_path_recycle_clms}",
    ",",
    "overwrite",
    True,
    False,
    "\"",
    None
)

file_path_clm_ln = f"{adls_path}/load/CLM_LN.{Source}.dat"
write_files(
    df_collector,
    file_path_clm_ln,
    ",",
    "overwrite",
    False,
    True,   # We do not set the header, instructions say "ContainsHeader":false for SeqFile
    "\"",
    None
)