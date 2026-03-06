# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2006, 2009, 2010, 2012, 2013, 2014, 2015, 2018, 2019, 2020, 2021, 2023 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Job Name:  IdsClmDriverExtr_EE
# MAGIC Called by:  IdsClaimPrereqSeq
# MAGIC 
# MAGIC Processing:
# MAGIC                     Each IDS Claim source has a seperate SQL to extract the records that changed since the last EDW processing.  
# MAGIC                     Last run cycle update is used to track record changes.  
# MAGIC                     All parts of the claim are extracted when a change is found in the CLM table.
# MAGIC 
# MAGIC                     Load file for IDS     - W_EDW_ETL_DRVR 
# MAGIC                                                    - W_EDW_PCA_ETL_DRVR.dat
# MAGIC                                        EDW   - W_CLM_DEL
# MAGIC                                                    - W_PCA_CLM_DEL.dat
# MAGIC 
# MAGIC Control Job Rerun Information: 
# MAGIC                     Previous Run Successful:    What needs to happen before a special run can occur?
# MAGIC                     Previous Run Aborted:         Restart, no other steps necessary
# MAGIC 
# MAGIC Modifications:
# MAGIC \(9)\(9)\(9)\(9)\(9)Project/\(9)\(9)\(9)\(9)\(9)\(9)\(9)\(9)\(9)\(9)\(9)DataStage\(9)\(9)Code\(9)\(9)\(9)Date
# MAGIC Developer\(9)\(9)Date\(9)\(9)Altiris #\(9)\(9)Change Description\(9)\(9)\(9)\(9)\(9)\(9)\(9)\(9)Project\(9)\(9)\(9)Reviewer\(9)\(9)\(9)Reviewed
# MAGIC ==================================================================================================================================================================================================
# MAGIC Brent Leland\(9)\(9)2006-03-21\(9)\(9)\(9)Original Programming.
# MAGIC Steph Goddard\(9)\(9)2006-07-24\(9)\(9)\(9)Add check for pseudoclaims - look for them and do a dummy write - 
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)sequencer will check to see if there are pseudoclaims to delete
# MAGIC Brent Leland\(9)\(9)2006-10-30\(9)\(9)\(9)Corrected column order for W_CLM_DEL file.
# MAGIC Ralph Tucker\(9)\(9)2006-11-20\(9)\(9)\(9)Added PCA Claim Driver & PCA Claim Delete file
# MAGIC Steph Goddard\(9)\(9)2007-02-08\(9)\(9)\(9)Added MOHSAIC and CAREADVANCE source systems for pseudos\(9)\(9)\(9)\(9)\(9)\(9)Brent Leland\(9)\(9)2007-02-22
# MAGIC Parik\(9)\(9)\(9)2008-09-12\(9)3784(PBM)\(9)Added ESI claims to the driver build\(9)\(9)\(9)\(9)\(9)\(9)\(9)\(9)\(9)Steph Goddard\(9)\(9)11/10/2008
# MAGIC Sharon Andrew\(9)\(9)2009-03-30\(9)LaborAccts\(9)Added WellDyne claims to the driver build.  Removed Argus\(9)\(9)\(9)\(9)\(9)\(9)\(9)Steph Goddard\(9)\(9)03/31/2009
# MAGIC Hugh Sisson\(9)\(9)2009-06-17\(9)4202\(9)\(9)Added additional criteria to PCA CLM driver                                                                              \(9)\(9)\(9)\(9)
# MAGIC Kalyan Neelam\(9)\(9)2009-10-12\(9)4098\(9)\(9)Added MCSource claims to the driver build.\(9)\(9)\(9)\(9)\(9)\(9)\(9)\(9)Steph Goddard\(9)\(9)10/15/2009
# MAGIC Brent Leland\(9)\(9)2009-11-06\(9)Prod Supp\(9)Added Caremark source to driver build.\(9)\(9)\(9)\(9)\(9)\(9)\(9)\(9)\(9)Steph Goddard\(9)\(9)11/10/2009
# MAGIC Kalyan Neelam\(9)\(9)2010-01-05\(9)4110\(9)\(9)Added Medicaid Source to driver build\(9)\(9)\(9)\(9)\(9)\(9)\(9)\(9)\(9)Steph Goddard\(9)\(9)01/11/2010
# MAGIC Kalyan Neelam\(9)\(9)2010-09-28\(9)4297\(9)\(9)Added 'PCT' in the extract SQL for Pseudo_Claim\(9)\(9)\(9)\(9)\(9)\(9)\(9)\(9)Sharon Andrew\(9)\(9)2010-09-28
# MAGIC Kalyan Neelam\(9)\(9)2010-01-05\(9)4110\(9)\(9)Added Medicaid Source to driver build\(9)\(9)\(9)\(9)\(9)\(9)\(9)\(9)\(9)Steph Goddard\(9)\(9)01/11/2010
# MAGIC Karthik Chintpni\(9)\(9)2012-06-28\(9)4784\(9)\(9)Added BCBSSC Source to driver build\(9)\(9)\(9)\(9)\(9)\(9)\(9)\(9)\(9)Sharon Andrew\(9)\(9)2012-07-11
# MAGIC Lee Moore\(9)\(9)2013-09-05\(9)5114\(9)\(9)rewrite in parallel\(9)\(9)\(9)\(9)\(9)\(9)\(9)\(9)EnterpriseWrhsDevl\(9)\(9)Peter Marshall\(9)\(9)12/19/2013
# MAGIC Bhoomi Dasari\(9)\(9)6/24/2014\(9)5345\(9)\(9)Added BCA source to driver build\(9)\(9)\(9)\(9)\(9)\(9)EnterpriseNewDevl\(9)\(9)Kalyan Neelam\(9)\(9)2014-06-24
# MAGIC Karthik Chintalapani\(9)\(9)2015-01-20\(9)5212\(9)\(9)Added BCBSA source to drive build\(9)\(9)\(9)\(9)\(9)\(9)EnterpriseCurDevl\(9)\(9)Bhoomi Dasari\(9)\(9)02/04/2015
# MAGIC Raja Gummadi\(9)\(9)03-24-2015\(9)5125\(9)\(9)Added Examone source to driver build\(9)\(9)\(9)\(9)\(9)\(9)EnterpriseNewDevl\(9)\(9)Kalyan Neelam\(9)\(9)2015-03-25  
# MAGIC Jaideep M\(9)\(9)2018-03-06\(9)5828\(9)\(9)Added SavRx and LDI source to driver build\(9)\(9)\(9)\(9)\(9)EnterpriseDev2\(9)\(9)Kalyan Neelam\(9)\(9)2018-03-06
# MAGIC Sethuraman R\(9)\(9)2018-04-02\(9)5744\(9)\(9)Added EyeMed source to driver build\(9)\(9)\(9)\(9)\(9)\(9)EnterpriseDev2\(9)\(9)Kalyan Neelam\(9)\(9)2018-04-09                
# MAGIC Kaushik K\(9)\(9)2018-09-23\(9)5828\(9)\(9)Added CVS source to driver build\(9)\(9)\(9)\(9)\(9)\(9)EnterpriseDev2\(9)\(9)Kalyan Neelam\(9)\(9)2018-10-01
# MAGIC Sagar\(9)\(9)\(9)2019-10-08\(9)6131-         \(9)Added PBM Parameters for ESI, OPTUMRX\(9)\(9)\(9)\(9)\(9)EnterpriseDev5\(9)\(9)Kalyan Neelam\(9)\(9)2019-11-22
# MAGIC \(9)\(9)\(9)\(9)\(9)PBM Replacement
# MAGIC Reddy Sanam\(9)\(9)2020-09-11\(9)\(9)\(9)Added Lumeris and Medimpact soure to\(9)\(9)\(9)\(9)\(9)\(9)EnterpriseDev2\(9)\(9)Kalyan Neelam\(9)\(9)2020-09-15
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)driver build 
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)Added parameter   LumerisRunCycle,
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)MedImpactRunCycle
# MAGIC SravyaSreeYarlagadda\(9)2020-11-16\(9)\(9)\(9)Added 13 claims sources to  db2 stages\(9)\(9)\(9)\(9)\(9)\(9)EnterpriseDev2\(9)\(9)Harsha Ravuri\(9)\(9)2020-11-17
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)Added 13 parameters to job parameters
# MAGIC Jeyaprasanna\(9)\(9)2020-11-23\(9)\(9)\(9)Added 2 claims sources to  db2 stages\(9)\(9)\(9)\(9)\(9)\(9)EnterpriseDev2\(9)\(9)Jaideep Mankala\(9)\(9)11/23/2020\(9)
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)Added 2  parameters to job parameters
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)(DENTAQUEST, ASH)
# MAGIC Vikas Abbu\(9)\(9)2021-03-22\(9)\(9)\(9)Added Solutran/Livongo  claims source to db2 stage\(9)\(9)\(9)\(9)EnterpriseDev2\(9)\(9)Kalyan Neelam\(9)\(9)2021-03-22
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)Added Solutran/Livongo  parameter  to job parameters\(9)\(9)
# MAGIC Vikas Abbu\(9)\(9)2021-03-24\(9)\(9)\(9)Added BARRYPOINTE/LEAWOODFMLYCARE\(9)\(9)\(9)\(9)\(9)EnterpriseDev2\(9)\(9)Kalyan Neelam\(9)\(9)2021-03-25     
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)MOSAICLIFECARE / PRIMEMO / SPIRA /TRUMAN 
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)claims source to db2 stage\(9)\(9)\(9)\(9)\(9)\(9)\(9)  
# MAGIC Venkata Yama\(9)\(9)2021-10-27\(9)\(9)\(9)Added HCA  claims source to db2 stage\(9)\(9)\(9)\(9)\(9)\(9)EnterpriseDev2\(9)\(9)Jeyaprasanna\(9)\(9)2021-10-27
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)Added HCA  parameter  to job parameters\(9)\(9)\(9)\(9)
# MAGIC Venkata Yama\(9)\(9)2021-11-09\(9)\(9)\(9)Added Nonstnd,CHILDRENSMERCY
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9) claims source to db2 stage\(9)\(9)\(9)\(9)\(9)\(9)\(9)EnterpriseDev2      \(9)\(9)Harsha Ravuri\(9)\(9)2021-11-10                                   
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)Added Nonstnd, CHILDRENSMERCY
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)parameter  to job parameters\(9)
# MAGIC Lokesh\(9)\(9)\(9)2021-12-05\(9)US 459610\(9)Added NationsBenefits Claims sources to Variables Stage\(9)\(9)\(9)\(9)EnterpriseDev2\(9)\(9)Jeyaprasanna\(9)\(9)2022-01-10
# MAGIC 
# MAGIC \(9)\(9)\(9)\(9)\(9)Project/\(9)\(9)\(9)\(9)\(9)\(9)\(9)\(9)\(9)\(9)\(9)DataStage\(9)\(9)Code\(9)\(9)\(9)Date
# MAGIC Developer\(9)\(9)Date\(9)\(9)Altiris #\(9)\(9)Change Description\(9)\(9)\(9)\(9)\(9)\(9)\(9)\(9)Project\(9)\(9)\(9)Reviewer\(9)\(9)\(9)Reviewed
# MAGIC ==================================================================================================================================================================================================
# MAGIC Ken Bradmon\(9)\(9)2023-08-08\(9)us559895\(9)\(9)Added 8 new source codes to the stage db2_EMRProcedureClm_Extr.\(9)\(9)\(9)EnterpriseDev2\(9)             Reddy Sanam                           2023-10-27\(9)
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)CENTRUSHEALTH, MOHEALTH, GOLDENVALLEY, JEFFERSON
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)WESTMOMEDCNTR, BLUESPRINGS, EXCELSIOR, HARRISONVILLE
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)And added 8 corresponding job parameters for the run cycles.
# MAGIC 
# MAGIC Saranya                                  2023-12-22               US 599810             Added new DB2 Stages for MARC and DOMINION.                                                              EnterpriseDev1                       Jeyaprasanna                          2023-12-26
# MAGIC                                                                                                                Added corresponding job parameters for run cycles
# MAGIC 
# MAGIC Arpitha V\(9)\(9)              2024-03-21\(9)US 614486            Added parameter NationBftsFLEXRunCycle and updated query in                                          EnterpriseDev2                     Jeyaprasanna                            2024-03-28
# MAGIC                                                                                                               db2_NationBenefitsClm_Extr stage to include NATIONBFTSFLEX 
# MAGIC Harsha Ravuri\(9)\(9)2025-05-08\(9)US#649173\(9)Added ASCENTIST source codes to the stage db2_EMRProcedureClm_Extr.\(9)\(9)EnterpriseDev2                     Jeyaprasanna                           2025-05-20\(9)\(9)\(9)

# MAGIC MERITAS,NORTHLAND,OLATHEMED,
# MAGIC PROVIDENCE,PROVSTLUKES,
# MAGIC SUNFLOWER,UNTDMEDGRP
# MAGIC CLAYPLATTE,ENCOMPASS,
# MAGIC HEDIS,JAYHAWK,KCINTRNLMEDS,
# MAGIC LIBERTYHOSP.
# MAGIC HCA
# MAGIC CHILDRENSMERCY
# MAGIC DENTAQUEST, ASH
# MAGIC LVNGHLTH
# MAGIC SOLUTRAN
# MAGIC BARRYPOINTE
# MAGIC LEAWOODFMLYCARE
# MAGIC MOSAICLIFECARE 
# MAGIC PRIMEMO 
# MAGIC SPIRA 
# MAGIC TRUMAN
# MAGIC CENTRUSHEALTH
# MAGIC MOHEALTH
# MAGIC GOLDENVALLEY
# MAGIC JEFFERSON
# MAGIC WESTMOMEDCNTR
# MAGIC BLUESPRINGS
# MAGIC EXCELSIOR
# MAGIC HARRISONVILLE
# MAGIC ASCENTIST
# MAGIC This is Source extract data from an IDS table and apply Code denormalization, create a load ready file.
# MAGIC 
# MAGIC Job Name:
# MAGIC IdsClmDriverExtr_EE
# MAGIC 
# MAGIC Table:
# MAGIC W_PCA_CLM_DEL                    W_CLM_DEL    W_EDW_PCA_ETL_DRVR                W_EDW_ETL_DRVR
# MAGIC NONSTDSUPLMTDATA
# MAGIC NATIONBFTSBBB
# MAGIC NATIONBFTSOTC
# MAGIC NATIONBFTSRWD
# MAGIC NATIONBFTSHRNG
# MAGIC NATIONBFTSFLEX
# MAGIC This is a load ready file conforms to the target table structure.
# MAGIC 
# MAGIC Write data into a Sequential file for Load Job.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


# Collecting parameter values
IDSOwner = get_widget_value('IDSOwner','')
IDSFilePath = get_widget_value('IDSFilePath','')
ids_secret_name = get_widget_value('ids_secret_name','')

FctsRunCycle = get_widget_value('FctsRunCycle','1')
WellDyneRunCycle = get_widget_value('WellDyneRunCycle','1')
NascoRunCycle = get_widget_value('NascoRunCycle','1')
PCSRunCycle = get_widget_value('PCSRunCycle','1')
PSEUDORunCycle = get_widget_value('PSEUDORunCycle','1')
PHPRunCycle = get_widget_value('PHPRunCycle','1')
ESIRunCycle = get_widget_value('ESIRunCycle','1')
OPTUMRXRunCycle = get_widget_value('OPTUMRXRunCycle','1')
MCSourceRunCycle = get_widget_value('MCSourceRunCycle','1')
CaremarkRunCycle = get_widget_value('CaremarkRunCycle','1')
MedicaidRunCycle = get_widget_value('MedicaidRunCycle','1')
MedtrakRunCycle = get_widget_value('MedtrakRunCycle','1')
BCBSSCRunCycle = get_widget_value('BCBSSCRunCycle','1')
BCARunCycle = get_widget_value('BCARunCycle','1')
BCBSARunCycle = get_widget_value('BCBSARunCycle','1')
ExamoneRunCycle = get_widget_value('ExamoneRunCycle','1')
SAVRXRunCycle = get_widget_value('SAVRXRunCycle','1')
LDIRunCycle = get_widget_value('LDIRunCycle','1')
EyeMedRunCycle = get_widget_value('EyeMedRunCycle','1')
CVSRunCycle = get_widget_value('CVSRunCycle','1')
LumerisRunCycle = get_widget_value('LumerisRunCycle','1')
MedImpactRunCycle = get_widget_value('MedImpactRunCycle','1')
ClayPlatteRunCycle = get_widget_value('ClayPlatteRunCycle','1')
EncompassRunCycle = get_widget_value('EncompassRunCycle','1')
HedisRunCycle = get_widget_value('HedisRunCycle','1')
JayHawkRunCycle = get_widget_value('JayHawkRunCycle','1')
KcintrnlmedsRunCycle = get_widget_value('KcintrnlmedsRunCycle','1')
LibertyHospRunCycle = get_widget_value('LibertyHospRunCycle','1')
MeritasRunCycle = get_widget_value('MeritasRunCycle','1')
NorthLandRunCycle = get_widget_value('NorthLandRunCycle','1')
OlathemedRunCycle = get_widget_value('OlathemedRunCycle','1')
ProvidenceRunCycle = get_widget_value('ProvidenceRunCycle','1')
ProvstlukesRunCycle = get_widget_value('ProvstlukesRunCycle','1')
SunflowerRunCycle = get_widget_value('SunflowerRunCycle','1')
UntdmedgrpRunCycle = get_widget_value('UntdmedgrpRunCycle','1')
DentaquestRunCycle = get_widget_value('DentaquestRunCycle','1')
AshRunCycle = get_widget_value('AshRunCycle','1')
LivongoRunCycle = get_widget_value('LivongoRunCycle','1')
SolutranRunCycle = get_widget_value('SolutranRunCycle','1')
BarryPointeRunCycle = get_widget_value('BarryPointeRunCycle','1')
LeawoodRunCycle = get_widget_value('LeawoodRunCycle','1')
MosaicRunCycle = get_widget_value('MosaicRunCycle','1')
PrimeMoRunCycle = get_widget_value('PrimeMoRunCycle','1')
SpiraRunCycle = get_widget_value('SpiraRunCycle','1')
TrumanRunCycle = get_widget_value('TrumanRunCycle','1')
HCARunCycle = get_widget_value('HCARunCycle','1')
NONSTDSUPLMTDATARunCycle = get_widget_value('NONSTDSUPLMTDATARunCycle','1')
CHILDRENSMERCYRunCycle = get_widget_value('CHILDRENSMERCYRunCycle','1')
NationBftsBBBRunCycle = get_widget_value('NationBftsBBBRunCycle','1')
NationBftsOTCRunCycle = get_widget_value('NationBftsOTCRunCycle','1')
NationBftsRWDRunCycle = get_widget_value('NationBftsRWDRunCycle','1')
NationBftsHRNGRunCycle = get_widget_value('NationBftsHRNGRunCycle','1')
NationBftsFLEXRunCycle = get_widget_value('NationBftsFLEXRunCycle','1')
CENTRUSHEALTHRunCycle = get_widget_value('CENTRUSHEALTHRunCycle','1')
MOHEALTHRunCycle = get_widget_value('MOHEALTHRunCycle','1')
GOLDENVALLEYRunCycle = get_widget_value('GOLDENVALLEYRunCycle','1')
JEFFERSONRunCycle = get_widget_value('JEFFERSONRunCycle','1')
WESTMOMEDCNTRRunCycle = get_widget_value('WESTMOMEDCNTRRunCycle','1')
BLUESPRINGSRunCycle = get_widget_value('BLUESPRINGSRunCycle','1')
EXCELSIORRunCycle = get_widget_value('EXCELSIORRunCycle','1')
HARRISONVILLERunCycle = get_widget_value('HARRISONVILLERunCycle','1')
MarcRunCycle = get_widget_value('MarcRunCycle','1')
DominionRunCycle = get_widget_value('DominionRunCycle','1')
ASCENTISTRunCycle = get_widget_value('ASCENTISTRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

df_db2_Facets_Clm_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"""
SELECT cl.SRC_SYS_CD_SK,
       COALESCE(cd.TRGT_CD,'UNK') AS SRC_SYS_CD,
       cl.CLM_ID,
       cl.LAST_UPDT_RUN_CYC_EXCTN_SK,
       COALESCE(cd2.TRGT_CD,'UNK') AS PCA_TYP_CD
FROM {IDSOwner}.CLM cl,
     {IDSOwner}.CD_MPPNG cd,
     {IDSOwner}.CD_MPPNG cd2
WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'FACETS'
  AND cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {FctsRunCycle}
  AND cl.PCA_TYP_CD_SK = cd2.CD_MPPNG_SK
""")
    .load()
)

df_db2_ESI_Clm_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"""
SELECT cl.SRC_SYS_CD_SK,
       COALESCE(cd.TRGT_CD,'UNK') AS SRC_SYS_CD,
       cl.CLM_ID,
       cl.LAST_UPDT_RUN_CYC_EXCTN_SK,
       'NA' AS PCA_TYP_CD
FROM {IDSOwner}.CLM cl,
     {IDSOwner}.CD_MPPNG cd
WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'ESI'
  AND cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ESIRunCycle}
""")
    .load()
)

df_db2_WellDyne_Clm_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"""
SELECT cl.SRC_SYS_CD_SK,
       COALESCE(cd.TRGT_CD,'UNK') AS SRC_SYS_CD,
       cl.CLM_ID,
       cl.LAST_UPDT_RUN_CYC_EXCTN_SK,
       'NA' AS PCA_TYP_CD
FROM {IDSOwner}.CLM cl,
     {IDSOwner}.CD_MPPNG cd
WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'WELLDYNERX'
  AND cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {WellDyneRunCycle}
""")
    .load()
)

df_db2_Nasco_Clm_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"""
SELECT cl.SRC_SYS_CD_SK,
       COALESCE(cd.TRGT_CD,'UNK') AS SRC_SYS_CD,
       cl.CLM_ID,
       cl.LAST_UPDT_RUN_CYC_EXCTN_SK,
       'NA' AS PCA_TYP_CD
FROM {IDSOwner}.CLM cl,
     {IDSOwner}.CD_MPPNG cd
WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'NPS'
  AND cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {NascoRunCycle}
""")
    .load()
)

df_db2_PSEUDO_claim_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"""
SELECT cl.SRC_SYS_CD_SK,
       COALESCE(cd.TRGT_CD,'UNK') AS SRC_SYS_CD,
       cl.CLM_ID,
       cl.LAST_UPDT_RUN_CYC_EXCTN_SK,
       'NA' AS PCA_TYP_CD
FROM {IDSOwner}.CLM cl,
     {IDSOwner}.CD_MPPNG cd
WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD in ('EDC','OT@2','ADOL','MOHSAIC','CAREADVANCE','PCT')
  AND cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {PSEUDORunCycle}
""")
    .load()
)

df_db2_PHP_Clm_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"""
SELECT cl.SRC_SYS_CD_SK,
       COALESCE(cd.TRGT_CD,'UNK') AS SRC_SYS_CD,
       cl.CLM_ID,
       cl.LAST_UPDT_RUN_CYC_EXCTN_SK,
       'NA' AS PCA_TYP_CD
FROM {IDSOwner}.CLM cl,
     {IDSOwner}.CD_MPPNG cd
WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'PHP'
  AND cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {PHPRunCycle}
""")
    .load()
)

df_db2_MCSource_Clm_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"""
SELECT cl.SRC_SYS_CD_SK,
       COALESCE(cd.TRGT_CD,'UNK') AS SRC_SYS_CD,
       cl.CLM_ID,
       cl.LAST_UPDT_RUN_CYC_EXCTN_SK,
       'NA' AS PCA_TYP_CD
FROM {IDSOwner}.CLM cl,
     {IDSOwner}.CD_MPPNG cd
WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'MCSOURCE'
  AND cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {MCSourceRunCycle}
""")
    .load()
)

df_db2_PCS_Clm_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"""
SELECT cl.SRC_SYS_CD_SK,
       COALESCE(cd.TRGT_CD,'UNK') AS SRC_SYS_CD,
       cl.CLM_ID,
       cl.LAST_UPDT_RUN_CYC_EXCTN_SK,
       'NA' AS PCA_TYP_CD
FROM {IDSOwner}.CLM cl,
     {IDSOwner}.CD_MPPNG cd
WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'PCS'
  AND cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {PCSRunCycle}
""")
    .load()
)

df_db2_Medtrak_Clm_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"""
SELECT cl.SRC_SYS_CD_SK,
       COALESCE(cd.TRGT_CD,'UNK') AS SRC_SYS_CD,
       cl.CLM_ID,
       cl.LAST_UPDT_RUN_CYC_EXCTN_SK,
       'NA' AS PCA_TYP_CD
FROM {IDSOwner}.CLM cl,
     {IDSOwner}.CD_MPPNG cd
WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'MEDTRAK'
  AND cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {MedtrakRunCycle}
""")
    .load()
)

df_db2_Medicaid_Clm_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"""
SELECT cl.SRC_SYS_CD_SK,
       COALESCE(cd.TRGT_CD,'UNK') AS SRC_SYS_CD,
       cl.CLM_ID,
       cl.LAST_UPDT_RUN_CYC_EXCTN_SK,
       'NA' AS PCA_TYP_CD
FROM {IDSOwner}.CLM cl,
     {IDSOwner}.CD_MPPNG cd
WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'MCAID'
  AND cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {MedicaidRunCycle}
""")
    .load()
)

df_db2_Caremark_Clm_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"""
SELECT cl.SRC_SYS_CD_SK,
       COALESCE(cd.TRGT_CD,'UNK') AS SRC_SYS_CD,
       cl.CLM_ID,
       cl.LAST_UPDT_RUN_CYC_EXCTN_SK,
       'NA' AS PCA_TYP_CD
FROM {IDSOwner}.CLM cl,
     {IDSOwner}.CD_MPPNG cd
WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'CAREMARK'
  AND cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {CaremarkRunCycle}
""")
    .load()
)

df_db2_BCBSSC_Clm_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"""
SELECT cl.SRC_SYS_CD_SK,
       COALESCE(cd.TRGT_CD,'UNK') AS SRC_SYS_CD,
       cl.CLM_ID,
       cl.LAST_UPDT_RUN_CYC_EXCTN_SK,
       'NA' AS PCA_TYP_CD
FROM {IDSOwner}.CLM cl,
     {IDSOwner}.CD_MPPNG cd
WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'BCBSSC'
  AND cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {BCBSSCRunCycle}
""")
    .load()
)

df_db2_BCA_Clm_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"""
SELECT cl.SRC_SYS_CD_SK,
       COALESCE(cd.TRGT_CD,'UNK') AS SRC_SYS_CD,
       cl.CLM_ID,
       cl.LAST_UPDT_RUN_CYC_EXCTN_SK,
       'NA' AS PCA_TYP_CD
FROM {IDSOwner}.CLM cl,
     {IDSOwner}.CD_MPPNG cd
WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'BCA'
  AND cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {BCARunCycle}
""")
    .load()
)

df_b2_BCBSA_Clm_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"""
SELECT cl.SRC_SYS_CD_SK,
       COALESCE(cd.TRGT_CD,'UNK') AS SRC_SYS_CD,
       cl.CLM_ID,
       cl.LAST_UPDT_RUN_CYC_EXCTN_SK,
       'NA' AS PCA_TYP_CD
FROM {IDSOwner}.CLM cl,
     {IDSOwner}.CD_MPPNG cd
WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'BCBSA'
  AND cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {BCBSARunCycle}
""")
    .load()
)

df_db2_Examone_Clm_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"""
SELECT cl.SRC_SYS_CD_SK,
       COALESCE(cd.TRGT_CD,'UNK') AS SRC_SYS_CD,
       cl.CLM_ID,
       cl.LAST_UPDT_RUN_CYC_EXCTN_SK,
       'NA' AS PCA_TYP_CD
FROM {IDSOwner}.CLM cl,
     {IDSOwner}.CD_MPPNG cd
WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'EXAMONE'
  AND cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExamoneRunCycle}
""")
    .load()
)

df_db2_SAVRX_Clm_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"""
SELECT cl.SRC_SYS_CD_SK,
       COALESCE(cd.TRGT_CD,'UNK') AS SRC_SYS_CD,
       cl.CLM_ID,
       cl.LAST_UPDT_RUN_CYC_EXCTN_SK,
       'NA' AS PCA_TYP_CD
FROM {IDSOwner}.CLM cl,
     {IDSOwner}.CD_MPPNG cd
WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'SAVRX'
  AND cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {SAVRXRunCycle}
""")
    .load()
)

df_db2_LDI_Clm_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"""
SELECT cl.SRC_SYS_CD_SK,
       COALESCE(cd.TRGT_CD,'UNK') AS SRC_SYS_CD,
       cl.CLM_ID,
       cl.LAST_UPDT_RUN_CYC_EXCTN_SK,
       'NA' AS PCA_TYP_CD
FROM {IDSOwner}.CLM cl,
     {IDSOwner}.CD_MPPNG cd
WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'LDI'
  AND cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {LDIRunCycle}
""")
    .load()
)

df_db2_EyeMed_Clm_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"""
SELECT cl.SRC_SYS_CD_SK,
       COALESCE(cd.TRGT_CD,'UNK') AS SRC_SYS_CD,
       cl.CLM_ID,
       cl.LAST_UPDT_RUN_CYC_EXCTN_SK,
       'NA' AS PCA_TYP_CD
FROM {IDSOwner}.CLM cl,
     {IDSOwner}.CD_MPPNG cd
WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'EYEMED'
  AND cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {EyeMedRunCycle}
""")
    .load()
)

df_db2_CVS_Clm_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"""
SELECT cl.SRC_SYS_CD_SK,
       COALESCE(cd.TRGT_CD,'UNK') AS SRC_SYS_CD,
       cl.CLM_ID,
       cl.LAST_UPDT_RUN_CYC_EXCTN_SK,
       'NA' AS PCA_TYP_CD
FROM {IDSOwner}.CLM cl,
     {IDSOwner}.CD_MPPNG cd
WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'CVS'
  AND cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {CVSRunCycle}
""")
    .load()
)

df_db2_OPTUMRX_Clm_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"""
SELECT cl.SRC_SYS_CD_SK,
       COALESCE(cd.TRGT_CD,'UNK') AS SRC_SYS_CD,
       cl.CLM_ID,
       cl.LAST_UPDT_RUN_CYC_EXCTN_SK,
       'NA' AS PCA_TYP_CD
FROM {IDSOwner}.CLM cl,
     {IDSOwner}.CD_MPPNG cd
WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'OPTUMRX'
  AND cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {OPTUMRXRunCycle}
""")
    .load()
)

df_db2_Lumeris_Clm_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"""
SELECT cl.SRC_SYS_CD_SK,
       COALESCE(cd.TRGT_CD,'UNK') AS SRC_SYS_CD,
       cl.CLM_ID,
       cl.LAST_UPDT_RUN_CYC_EXCTN_SK,
       COALESCE(cd2.TRGT_CD,'UNK') AS PCA_TYP_CD
FROM {IDSOwner}.CLM cl,
     {IDSOwner}.CD_MPPNG cd,
     {IDSOwner}.CD_MPPNG cd2
WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'LUMERIS'
  AND cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {LumerisRunCycle}
  AND cl.PCA_TYP_CD_SK = cd2.CD_MPPNG_SK
""")
    .load()
)

df_db2_MedImpact_Clm_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"""
SELECT cl.SRC_SYS_CD_SK,
       COALESCE(cd.TRGT_CD,'UNK') AS SRC_SYS_CD,
       cl.CLM_ID,
       cl.LAST_UPDT_RUN_CYC_EXCTN_SK,
       COALESCE(cd2.TRGT_CD,'UNK') AS PCA_TYP_CD
FROM {IDSOwner}.CLM cl,
     {IDSOwner}.CD_MPPNG cd,
     {IDSOwner}.CD_MPPNG cd2
WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'MEDIMPACT'
  AND cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {MedImpactRunCycle}
  AND cl.PCA_TYP_CD_SK = cd2.CD_MPPNG_SK
""")
    .load()
)

df_db2_LhoHistProc_Clm_Extr = (
    spark.read.format("jdbc")
    .options(**jdbc_props)
    .option("url", jdbc_url)
    .option("query", f"""
SELECT cl.SRC_SYS_CD_SK,
       COALESCE(cd.TRGT_CD,'UNK') AS SRC_SYS_CD,
       cl.CLM_ID,
       cl.LAST_UPDT_RUN_CYC_EXCTN_SK,
       'NA' AS PCA_TYP_CD
FROM {IDSOwner}.CLM cl,
     {IDSOwner}.CD_MPPNG cd
WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'CLAYPLATTE'
  AND cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ClayPlatteRunCycle}
UNION ALL
SELECT cl.SRC_SYS_CD_SK,
       COALESCE(cd.TRGT_CD,'UNK') AS SRC_SYS_CD,
       cl.CLM_ID,
       cl.LAST_UPDT_RUN_CYC_EXCTN_SK,
       'NA' AS PCA_TYP_CD
FROM {IDSOwner}.CLM cl,
     {IDSOwner}.CD_MPPNG cd
WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'ENCOMPASS'
  AND cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {EncompassRunCycle}
UNION ALL
SELECT cl.SRC_SYS_CD_SK,
       COALESCE(cd.TRGT_CD,'UNK') AS SRC_SYS_CD,
       cl.CLM_ID,
       cl.LAST_UPDT_RUN_CYC_EXCTN_SK,
       'NA' AS PCA_TYP_CD
FROM {IDSOwner}.CLM cl,
     {IDSOwner}.CD_MPPNG cd
WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'HEDIS'
  AND cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {HedisRunCycle}
UNION ALL
SELECT cl.SRC_SYS_CD_SK,
       COALESCE(cd.TRGT_CD,'UNK') AS SRC_SYS_CD,
       cl.CLM_ID,
       cl.LAST_UPDT_RUN_CYC_EXCTN_SK,
       'NA' AS PCA_TYP_CD
FROM {IDSOwner}.CLM cl,
     {IDSOwner}.CD_MPPNG cd
WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'JAYHAWK'
  AND cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {JayHawkRunCycle}
UNION ALL
SELECT cl.SRC_SYS_CD_SK,
       COALESCE(cd.TRGT_CD,'UNK') AS SRC_SYS_CD,
       cl.CLM_ID,
       cl.LAST_UPDT_RUN_CYC_EXCTN_SK,
       'NA' AS PCA_TYP_CD
FROM {IDSOwner}.CLM cl,
     {IDSOwner}.CD_MPPNG cd
WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'KCINTRNLMEDS'
  AND cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {KcintrnlmedsRunCycle}
UNION ALL
SELECT cl.SRC_SYS_CD_SK,
       COALESCE(cd.TRGT_CD,'UNK') AS SRC_SYS_CD,
       cl.CLM_ID,
       cl.LAST_UPDT_RUN_CYC_EXCTN_SK,
       'NA' AS PCA_TYP_CD
FROM {IDSOwner}.CLM cl,
     {IDSOwner}.CD_MPPNG cd
WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'LIBERTYHOSP'
  AND cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {LibertyHospRunCycle}
""")
    .load()
)

df_db2_LhoHistProc1_Clm_Extr = (
    spark.read.format("jdbc")
    .options(**jdbc_props)
    .option("url", jdbc_url)
    .option("query", f"""
SELECT cl.SRC_SYS_CD_SK,
       COALESCE(cd.TRGT_CD,'UNK') AS SRC_SYS_CD,
       cl.CLM_ID,
       cl.LAST_UPDT_RUN_CYC_EXCTN_SK,
       'NA' AS PCA_TYP_CD
FROM {IDSOwner}.CLM cl,
     {IDSOwner}.CD_MPPNG cd
WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'MERITAS'
  AND cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {MeritasRunCycle}
UNION ALL
SELECT cl.SRC_SYS_CD_SK,
       COALESCE(cd.TRGT_CD,'UNK') AS SRC_SYS_CD,
       cl.CLM_ID,
       cl.LAST_UPDT_RUN_CYC_EXCTN_SK,
       'NA' AS PCA_TYP_CD
FROM {IDSOwner}.CLM cl,
     {IDSOwner}.CD_MPPNG cd
WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'NORTHLAND'
  AND cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {NorthLandRunCycle}
UNION ALL
SELECT cl.SRC_SYS_CD_SK,
       COALESCE(cd.TRGT_CD,'UNK') AS SRC_SYS_CD,
       cl.CLM_ID,
       cl.LAST_UPDT_RUN_CYC_EXCTN_SK,
       'NA' AS PCA_TYP_CD
FROM {IDSOwner}.CLM cl,
     {IDSOwner}.CD_MPPNG cd
WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'OLATHEMED'
  AND cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {OlathemedRunCycle}
UNION ALL
SELECT cl.SRC_SYS_CD_SK,
       COALESCE(cd.TRGT_CD,'UNK') AS SRC_SYS_CD,
       cl.CLM_ID,
       cl.LAST_UPDT_RUN_CYC_EXCTN_SK,
       'NA' AS PCA_TYP_CD
FROM {IDSOwner}.CLM cl,
     {IDSOwner}.CD_MPPNG cd
WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'PROVIDENCE'
  AND cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ProvidenceRunCycle}
UNION ALL
SELECT cl.SRC_SYS_CD_SK,
       COALESCE(cd.TRGT_CD,'UNK') AS SRC_SYS_CD,
       cl.CLM_ID,
       cl.LAST_UPDT_RUN_CYC_EXCTN_SK,
       'NA' AS PCA_TYP_CD
FROM {IDSOwner}.CLM cl,
     {IDSOwner}.CD_MPPNG cd
WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'PROVSTLUKES'
  AND cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ProvstlukesRunCycle}
UNION ALL
SELECT cl.SRC_SYS_CD_SK,
       COALESCE(cd.TRGT_CD,'UNK') AS SRC_SYS_CD,
       cl.CLM_ID,
       cl.LAST_UPDT_RUN_CYC_EXCTN_SK,
       'NA' AS PCA_TYP_CD
FROM {IDSOwner}.CLM cl,
     {IDSOwner}.CD_MPPNG cd
WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'SUNFLOWER'
  AND cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {SunflowerRunCycle}
UNION ALL
SELECT cl.SRC_SYS_CD_SK,
       COALESCE(cd.TRGT_CD,'UNK') AS SRC_SYS_CD,
       cl.CLM_ID,
       cl.LAST_UPDT_RUN_CYC_EXCTN_SK,
       'NA' AS PCA_TYP_CD
FROM {IDSOwner}.CLM cl,
     {IDSOwner}.CD_MPPNG cd
WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'UNTDMEDGRP'
  AND cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {UntdmedgrpRunCycle}
""")
    .load()
)

df_db2_LhoHistEnc_Clm_Extr = (
    spark.read.format("jdbc")
    .options(**jdbc_props)
    .option("url", jdbc_url)
    .option("query", f"""
SELECT cl.SRC_SYS_CD_SK,
       COALESCE(cd.TRGT_CD,'UNK') AS SRC_SYS_CD,
       cl.CLM_ID,
       cl.LAST_UPDT_RUN_CYC_EXCTN_SK,
       'NA' AS PCA_TYP_CD
FROM {IDSOwner}.CLM cl,
     {IDSOwner}.CD_MPPNG cd
WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'DENTAQUEST'
  AND cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {DentaquestRunCycle}
UNION ALL
SELECT cl.SRC_SYS_CD_SK,
       COALESCE(cd.TRGT_CD,'UNK') AS SRC_SYS_CD,
       cl.CLM_ID,
       cl.LAST_UPDT_RUN_CYC_EXCTN_SK,
       'NA' AS PCA_TYP_CD
FROM {IDSOwner}.CLM cl,
     {IDSOwner}.CD_MPPNG cd
WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'ASH'
  AND cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {AshRunCycle}
""")
    .load()
)

df_db2_LivongoEncClm_Extr = (
    spark.read.format("jdbc")
    .options(**jdbc_props)
    .option("url", jdbc_url)
    .option("query", f"""
SELECT cl.SRC_SYS_CD_SK,
       COALESCE(cd.TRGT_CD,'UNK') AS SRC_SYS_CD,
       cl.CLM_ID,
       cl.LAST_UPDT_RUN_CYC_EXCTN_SK,
       'NA' AS PCA_TYP_CD
FROM {IDSOwner}.CLM cl,
     {IDSOwner}.CD_MPPNG cd
WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'LVNGHLTH'
  AND cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {LivongoRunCycle}
""")
    .load()
)

df_db2_SolutranEncClm_Extr = (
    spark.read.format("jdbc")
    .options(**jdbc_props)
    .option("url", jdbc_url)
    .option("query", f"""
SELECT cl.SRC_SYS_CD_SK,
       COALESCE(cd.TRGT_CD,'UNK') AS SRC_SYS_CD,
       cl.CLM_ID,
       cl.LAST_UPDT_RUN_CYC_EXCTN_SK,
       'NA' AS PCA_TYP_CD
FROM {IDSOwner}.CLM cl,
     {IDSOwner}.CD_MPPNG cd
WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'SOLUTRAN'
  AND cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {SolutranRunCycle}
""")
    .load()
)

df_db2_EMRProcedureClm_Extr = (
    spark.read.format("jdbc")
    .options(**jdbc_props)
    .option("url", jdbc_url)
    .option("query", f"""
SELECT cl.SRC_SYS_CD_SK,
       COALESCE(cd.TRGT_CD,'UNK') AS SRC_SYS_CD,
       cl.CLM_ID,
       cl.LAST_UPDT_RUN_CYC_EXCTN_SK,
       'NA' AS PCA_TYP_CD
FROM {IDSOwner}.CLM cl,
     {IDSOwner}.CD_MPPNG cd
WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'BARRYPOINTE'
  AND cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {BarryPointeRunCycle}
UNION ALL
SELECT cl.SRC_SYS_CD_SK,
       COALESCE(cd.TRGT_CD,'UNK') AS SRC_SYS_CD,
       cl.CLM_ID,
       cl.LAST_UPDT_RUN_CYC_EXCTN_SK,
       'NA' AS PCA_TYP_CD
FROM {IDSOwner}.CLM cl,
     {IDSOwner}.CD_MPPNG cd
WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'LEAWOODFMLYCARE'
  AND cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {LeawoodRunCycle}
UNION ALL
SELECT cl.SRC_SYS_CD_SK,
       COALESCE(cd.TRGT_CD,'UNK') AS SRC_SYS_CD,
       cl.CLM_ID,
       cl.LAST_UPDT_RUN_CYC_EXCTN_SK,
       'NA' AS PCA_TYP_CD
FROM {IDSOwner}.CLM cl,
     {IDSOwner}.CD_MPPNG cd
WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'MOSAICLIFECARE'
  AND cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {MosaicRunCycle}
UNION ALL
SELECT cl.SRC_SYS_CD_SK,
       COALESCE(cd.TRGT_CD,'UNK') AS SRC_SYS_CD,
       cl.CLM_ID,
       cl.LAST_UPDT_RUN_CYC_EXCTN_SK,
       'NA' AS PCA_TYP_CD
FROM {IDSOwner}.CLM cl,
     {IDSOwner}.CD_MPPNG cd
WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'PRIMEMO'
  AND cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {PrimeMoRunCycle}
UNION ALL
SELECT cl.SRC_SYS_CD_SK,
       COALESCE(cd.TRGT_CD,'UNK') AS SRC_SYS_CD,
       cl.CLM_ID,
       cl.LAST_UPDT_RUN_CYC_EXCTN_SK,
       'NA' AS PCA_TYP_CD
FROM {IDSOwner}.CLM cl,
     {IDSOwner}.CD_MPPNG cd
WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'SPIRA'
  AND cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {SpiraRunCycle}
UNION ALL
SELECT cl.SRC_SYS_CD_SK,
       COALESCE(cd.TRGT_CD,'UNK') AS SRC_SYS_CD,
       cl.CLM_ID,
       cl.LAST_UPDT_RUN_CYC_EXCTN_SK,
       'NA' AS PCA_TYP_CD
FROM {IDSOwner}.CLM cl,
     {IDSOwner}.CD_MPPNG cd
WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'TRUMAN'
  AND cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {TrumanRunCycle}
UNION ALL
SELECT cl.SRC_SYS_CD_SK,
       COALESCE(cd.TRGT_CD,'UNK') AS SRC_SYS_CD,
       cl.CLM_ID,
       cl.LAST_UPDT_RUN_CYC_EXCTN_SK,
       'NA' AS PCA_TYP_CD
FROM {IDSOwner}.CLM cl,
     {IDSOwner}.CD_MPPNG cd
WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'CENTRUSHEALTH'
  AND cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {CENTRUSHEALTHRunCycle}
UNION ALL
SELECT cl.SRC_SYS_CD_SK,
       COALESCE(cd.TRGT_CD,'UNK') AS SRC_SYS_CD,
       cl.CLM_ID,
       cl.LAST_UPDT_RUN_CYC_EXCTN_SK,
       'NA' AS PCA_TYP_CD
FROM {IDSOwner}.CLM cl,
     {IDSOwner}.CD_MPPNG cd
WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'MOHEALTH'
  AND cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {MOHEALTHRunCycle}
UNION ALL
SELECT cl.SRC_SYS_CD_SK,
       COALESCE(cd.TRGT_CD,'UNK') AS SRC_SYS_CD,
       cl.CLM_ID,
       cl.LAST_UPDT_RUN_CYC_EXCTN_SK,
       'NA' AS PCA_TYP_CD
FROM {IDSOwner}.CLM cl,
     {IDSOwner}.CD_MPPNG cd
WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'GOLDENVALLEY'
  AND cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {GOLDENVALLEYRunCycle}
UNION ALL
SELECT cl.SRC_SYS_CD_SK,
       COALESCE(cd.TRGT_CD,'UNK') AS SRC_SYS_CD,
       cl.CLM_ID,
       cl.LAST_UPDT_RUN_CYC_EXCTN_SK,
       'NA' AS PCA_TYP_CD
FROM {IDSOwner}.CLM cl,
     {IDSOwner}.CD_MPPNG cd
WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'JEFFERSONRunCycle'
  AND cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {JEFFERSONRunCycle}
UNION ALL
SELECT cl.SRC_SYS_CD_SK,
       COALESCE(cd.TRGT_CD,'UNK') AS SRC_SYS_CD,
       cl.CLM_ID,
       cl.LAST_UPDT_RUN_CYC_EXCTN_SK,
       'NA' AS PCA_TYP_CD
FROM {IDSOwner}.CLM cl,
     {IDSOwner}.CD_MPPNG cd
WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'WESTMOMEDCNTR'
  AND cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {WESTMOMEDCNTRRunCycle}
UNION ALL
SELECT cl.SRC_SYS_CD_SK,
       COALESCE(cd.TRGT_CD,'UNK') AS SRC_SYS_CD,
       cl.CLM_ID,
       cl.LAST_UPDT_RUN_CYC_EXCTN_SK,
       'NA' AS PCA_TYP_CD
FROM {IDSOwner}.CLM cl,
     {IDSOwner}.CD_MPPNG cd
WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'BLUESPRINGS'
  AND cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {BLUESPRINGSRunCycle}
UNION ALL
SELECT cl.SRC_SYS_CD_SK,
       COALESCE(cd.TRGT_CD,'UNK') AS SRC_SYS_CD,
       cl.CLM_ID,
       cl.LAST_UPDT_RUN_CYC_EXCTN_SK,
       'NA' AS PCA_TYP_CD
FROM {IDSOwner}.CLM cl,
     {IDSOwner}.CD_MPPNG cd
WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'EXCELSIOR'
  AND cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {EXCELSIORRunCycle}
UNION ALL
SELECT cl.SRC_SYS_CD_SK,
       COALESCE(cd.TRGT_CD,'UNK') AS SRC_SYS_CD,
       cl.CLM_ID,
       cl.LAST_UPDT_RUN_CYC_EXCTN_SK,
       'NA' AS PCA_TYP_CD
FROM {IDSOwner}.CLM cl,
     {IDSOwner}.CD_MPPNG cd
WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'HARRISONVILLE'
  AND cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {HARRISONVILLERunCycle}
UNION ALL
SELECT cl.SRC_SYS_CD_SK,
       COALESCE(cd.TRGT_CD,'UNK') AS SRC_SYS_CD,
       cl.CLM_ID,
       cl.LAST_UPDT_RUN_CYC_EXCTN_SK,
       'NA' AS PCA_TYP_CD
FROM {IDSOwner}.CLM cl,
     {IDSOwner}.CD_MPPNG cd
WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'ASCENTIST'
  AND cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ASCENTISTRunCycle}
""")
    .load()
)

df_db2_EMRProcedureClmhca_Extr = (
    spark.read.format("jdbc")
    .options(**jdbc_props)
    .option("url", jdbc_url)
    .option("query", f"""
SELECT cl.SRC_SYS_CD_SK,
       COALESCE(cd.TRGT_CD,'UNK') AS SRC_SYS_CD,
       cl.CLM_ID,
       cl.LAST_UPDT_RUN_CYC_EXCTN_SK,
       'NA' AS PCA_TYP_CD
FROM {IDSOwner}.CLM cl,
     {IDSOwner}.CD_MPPNG cd
WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'HCA'
  AND cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {HCARunCycle}
""")
    .load()
)

df_db2_EMRProcedureClmNONSTDSUPLMTDATA_Extr = (
    spark.read.format("jdbc")
    .options(**jdbc_props)
    .option("url", jdbc_url)
    .option("query", f"""
SELECT cl.SRC_SYS_CD_SK,
       COALESCE(cd.TRGT_CD,'UNK') AS SRC_SYS_CD,
       cl.CLM_ID,
       cl.LAST_UPDT_RUN_CYC_EXCTN_SK,
       'NA' AS PCA_TYP_CD
FROM {IDSOwner}.CLM cl,
     {IDSOwner}.CD_MPPNG cd
WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'NONSTDSUPLMTDATA'
  AND cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {NONSTDSUPLMTDATARunCycle}
""")
    .load()
)

df_EMRProcedureClmCHILDRENSMERCY_Extr = (
    spark.read.format("jdbc")
    .options(**jdbc_props)
    .option("url", jdbc_url)
    .option("query", f"""
SELECT cl.SRC_SYS_CD_SK,
       COALESCE(cd.TRGT_CD,'UNK') AS SRC_SYS_CD,
       cl.CLM_ID,
       cl.LAST_UPDT_RUN_CYC_EXCTN_SK,
       'NA' AS PCA_TYP_CD
FROM {IDSOwner}.CLM cl,
     {IDSOwner}.CD_MPPNG cd
WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'CHILDRENSMERCY'
  AND cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {CHILDRENSMERCYRunCycle}
""")
    .load()
)

df_db2_NationBenefitsClm_Extr = (
    spark.read.format("jdbc")
    .options(**jdbc_props)
    .option("url", jdbc_url)
    .option("query", f"""
SELECT cl.SRC_SYS_CD_SK,
       COALESCE(cd.TRGT_CD,'UNK') AS SRC_SYS_CD,
       cl.CLM_ID,
       cl.LAST_UPDT_RUN_CYC_EXCTN_SK,
       'NA' AS PCA_TYP_CD
FROM {IDSOwner}.CLM cl,
     {IDSOwner}.CD_MPPNG cd
WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'NATIONBFTSBBB'
  AND cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {NationBftsBBBRunCycle}
UNION ALL
SELECT cl.SRC_SYS_CD_SK,
       COALESCE(cd.TRGT_CD,'UNK') AS SRC_SYS_CD,
       cl.CLM_ID,
       cl.LAST_UPDT_RUN_CYC_EXCTN_SK,
       'NA' AS PCA_TYP_CD
FROM {IDSOwner}.CLM cl,
     {IDSOwner}.CD_MPPNG cd
WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'NATIONBFTSOTC'
  AND cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {NationBftsOTCRunCycle}
UNION ALL
SELECT cl.SRC_SYS_CD_SK,
       COALESCE(cd.TRGT_CD,'UNK') AS SRC_SYS_CD,
       cl.CLM_ID,
       cl.LAST_UPDT_RUN_CYC_EXCTN_SK,
       'NA' AS PCA_TYP_CD
FROM {IDSOwner}.CLM cl,
     {IDSOwner}.CD_MPPNG cd
WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'NATIONBFTSRWD'
  AND cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {NationBftsRWDRunCycle}
UNION ALL
SELECT cl.SRC_SYS_CD_SK,
       COALESCE(cd.TRGT_CD,'UNK') AS SRC_SYS_CD,
       cl.CLM_ID,
       cl.LAST_UPDT_RUN_CYC_EXCTN_SK,
       'NA' AS PCA_TYP_CD
FROM {IDSOwner}.CLM cl,
     {IDSOwner}.CD_MPPNG cd
WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'NATIONBFTSHRNG'
  AND cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {NationBftsHRNGRunCycle}
UNION ALL
SELECT cl.SRC_SYS_CD_SK,
       COALESCE(cd.TRGT_CD,'UNK') AS SRC_SYS_CD,
       cl.CLM_ID,
       cl.LAST_UPDT_RUN_CYC_EXCTN_SK,
       'NA' AS PCA_TYP_CD
FROM {IDSOwner}.CLM cl,
     {IDSOwner}.CD_MPPNG cd
WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'NATIONBFTSFLEX'
  AND cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {NationBftsFLEXRunCycle}
""")
    .load()
)

df_db2_MarcClm_Extr = (
    spark.read.format("jdbc")
    .options(**jdbc_props)
    .option("url", jdbc_url)
    .option("query", f"""
SELECT cl.SRC_SYS_CD_SK,
       COALESCE(cd.TRGT_CD,'UNK') AS SRC_SYS_CD,
       cl.CLM_ID,
       cl.LAST_UPDT_RUN_CYC_EXCTN_SK,
       'NA' AS PCA_TYP_CD
FROM {IDSOwner}.CLM cl,
     {IDSOwner}.CD_MPPNG cd
WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'MARC'
  AND cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {MarcRunCycle}
""")
    .load()
)

df_db2_DominionClm_Extr = (
    spark.read.format("jdbc")
    .options(**jdbc_props)
    .option("url", jdbc_url)
    .option("query", f"""
SELECT cl.SRC_SYS_CD_SK,
       COALESCE(cd.TRGT_CD,'UNK') AS SRC_SYS_CD,
       cl.CLM_ID,
       cl.LAST_UPDT_RUN_CYC_EXCTN_SK,
       'NA' AS PCA_TYP_CD
FROM {IDSOwner}.CLM cl,
     {IDSOwner}.CD_MPPNG cd
WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'DOMINION'
  AND cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {DominionRunCycle}
""")
    .load()
)

# Funnel (union all)
df_fnl_Data = (
    df_db2_Facets_Clm_Extr
    .unionByName(df_db2_ESI_Clm_Extr)
    .unionByName(df_db2_WellDyne_Clm_Extr)
    .unionByName(df_db2_Nasco_Clm_Extr)
    .unionByName(df_db2_PSEUDO_claim_Extr)
    .unionByName(df_db2_PHP_Clm_Extr)
    .unionByName(df_db2_MCSource_Clm_Extr)
    .unionByName(df_db2_PCS_Clm_Extr)
    .unionByName(df_db2_Medtrak_Clm_Extr)
    .unionByName(df_db2_Medicaid_Clm_Extr)
    .unionByName(df_db2_Caremark_Clm_Extr)
    .unionByName(df_db2_BCBSSC_Clm_Extr)
    .unionByName(df_db2_BCA_Clm_Extr)
    .unionByName(df_b2_BCBSA_Clm_Extr)
    .unionByName(df_db2_Examone_Clm_Extr)
    .unionByName(df_db2_SAVRX_Clm_Extr)
    .unionByName(df_db2_LDI_Clm_Extr)
    .unionByName(df_db2_EyeMed_Clm_Extr)
    .unionByName(df_db2_CVS_Clm_Extr)
    .unionByName(df_db2_OPTUMRX_Clm_Extr)
    .unionByName(df_db2_Lumeris_Clm_Extr)
    .unionByName(df_db2_MedImpact_Clm_Extr)
    .unionByName(df_db2_LhoHistProc_Clm_Extr)
    .unionByName(df_db2_LhoHistProc1_Clm_Extr)
    .unionByName(df_db2_LhoHistEnc_Clm_Extr)
    .unionByName(df_db2_LivongoEncClm_Extr)
    .unionByName(df_db2_SolutranEncClm_Extr)
    .unionByName(df_db2_EMRProcedureClm_Extr)
    .unionByName(df_db2_EMRProcedureClmhca_Extr)
    .unionByName(df_db2_EMRProcedureClmNONSTDSUPLMTDATA_Extr)
    .unionByName(df_EMRProcedureClmCHILDRENSMERCY_Extr)
    .unionByName(df_db2_NationBenefitsClm_Extr)
    .unionByName(df_db2_MarcClm_Extr)
    .unionByName(df_db2_DominionClm_Extr)
)

# Transformer logic
df_fnl_Data = df_fnl_Data.select(
    "SRC_SYS_CD_SK",
    "SRC_SYS_CD",
    "CLM_ID",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "PCA_TYP_CD"
)

# Stage variables in logic:
# svPcaRow => True if PCA_TYP_CD in ['MEDPCA','RUNOUT','PCA','EMPWBNF','EXCPT','RUNINMED','RUNINRX','PCARXONLY','PCAMEDONLY']
# svNormRow => True if PCA_TYP_CD in ['MEDPCA','NA']

conditions_pca = [
    "MEDPCA","RUNOUT","PCA","EMPWBNF","EXCPT","RUNINMED","RUNINRX","PCARXONLY","PCAMEDONLY"
]
conditions_norm = ["MEDPCA","NA"]

df_norm = df_fnl_Data.filter(F.col("PCA_TYP_CD").isin(conditions_norm))
df_pca = df_fnl_Data.filter(F.col("PCA_TYP_CD").isin(conditions_pca))
df_pseudos = df_fnl_Data.filter(
    F.col("SRC_SYS_CD").isin(["OT@2","ADOL","EDC","CAREADVANCE","MOHSAIC","PCT"])
)

# Outputs from Transformer
df_norm_wedwetldrvr = df_norm.select(
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLAIM_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)
df_pca_wedwpcaetl = df_pca.select(
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLAIM_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)
df_norm_wclmdel = df_norm.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CLM_ID").alias("CLAIM_ID"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK")
)
df_pca_wpca = df_pca.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CLM_ID").alias("CLAIM_ID"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK")
)
df_pseudos_out = df_pseudos.select(
    F.col("CLM_ID").alias("CLM_ID")
)

# Before writing, apply rpad to string columns (assume 50 as a placeholder length for unknown char/varchar). 
# For the numeric columns, do not modify.

df_norm_wedwetldrvr = df_norm_wedwetldrvr.withColumn("CLAIM_ID", F.rpad(F.col("CLAIM_ID"), 50, " ")) \
                                         .withColumn("SRC_SYS_CD", F.rpad(F.col("SRC_SYS_CD"), 50, " "))
df_pca_wedwpcaetl = df_pca_wedwpcaetl.withColumn("CLAIM_ID", F.rpad(F.col("CLAIM_ID"), 50, " ")) \
                                     .withColumn("SRC_SYS_CD", F.rpad(F.col("SRC_SYS_CD"), 50, " "))
df_norm_wclmdel = df_norm_wclmdel.withColumn("CLAIM_ID", F.rpad(F.col("CLAIM_ID"), 50, " ")) \
                                 .withColumn("SRC_SYS_CD", F.rpad(F.col("SRC_SYS_CD"), 50, " "))
df_pca_wpca = df_pca_wpca.withColumn("CLAIM_ID", F.rpad(F.col("CLAIM_ID"), 50, " ")) \
                         .withColumn("SRC_SYS_CD", F.rpad(F.col("SRC_SYS_CD"), 50, " "))
df_pseudos_out = df_pseudos_out.withColumn("CLM_ID", F.rpad(F.col("CLM_ID"), 50, " "))

# Write out using write_files
# 1) seq_W_EDW_ETL_DRVR_csv_load => "file#$IDSFilePath#/load/W_EDW_ETL_DRVR.dat"
write_files(
    df_norm_wedwetldrvr,
    f"{adls_path}/{IDSFilePath}/load/W_EDW_ETL_DRVR.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

# 2) seq_W_EDW_PCA_ETL_DRVR_csv_load => "file#$IDSFilePath#/load/W_EDW_PCA_ETL_DRVR.dat"
write_files(
    df_pca_wedwpcaetl,
    f"{adls_path}/{IDSFilePath}/load/W_EDW_PCA_ETL_DRVR.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

# 3) seq_W_CLM_DEL_csv_load => "load/W_CLM_DEL.dat"
#    (no "landing" or "external" in path => adls_path)
write_files(
    df_norm_wclmdel,
    f"{adls_path}/load/W_CLM_DEL.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

# 4) seq_W_PCA_CLM_DEL_csv_load => "load/W_PCA_CLM_DEL.dat"
write_files(
    df_pca_wpca,
    f"{adls_path}/load/W_PCA_CLM_DEL.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

# 5) eq_PSEUDOS_Exist => "file/dev/null"
write_files(
    df_pseudos_out,
    f"{adls_path}/dev/null",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)