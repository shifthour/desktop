# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Job Name:  FacetsEAMExtr
# MAGIC Calling Job:EAMFctsReconRptCntl
# MAGIC 
# MAGIC Description:This Job is created to run part of the stored proc [dbo].[TMGSP_FACETS_EAM_RECON_RPT] that is running slow when going to from SqlServer to Facets database using Linked Server. The transformations are copied from the stored proc as is to keep it close to how the code is in the stored procedure.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC ======================================================================================================================================
# MAGIC Developer                          Date                           Project/Altiris #                              Change Description                                                    Development Project               
# MAGIC ======================================================================================================================================
# MAGIC ReddySanam               2021-01-05                  US329820                                         Original Programming                                                    IntegrateDev2   Kalyan Neelam    2021-01-14
# MAGIC Reddy                          2021-01-25                  US329820                                 Added remove duplicates stage                                                                          Kalyan Neelam    2021-01-25
# MAGIC                                                                                                                           based on FACETS_HIC, FACETS_MemberID,
# MAGIC                                                                                                                           FACETS_GroupID, MEME_CK      
# MAGIC Reddy Sanam             2021-02-03                   US329820                                 Changed Penalty Amount extraction                                   IntegrateDev2      Jeyaprasanna   2021-02-08 
# MAGIC                                                                                                                           pulled from LATE event with 
# MAGIC                                                                                                                           current date between HCFA effective
# MAGIC                                                                                                                           and term date
# MAGIC                                                                                                                          Changed PrimayRXID extraction
# MAGIC                                                                                                                           pulled from PRTD event with
# MAGIC                                                                                                                           current date between HCFA effective
# MAGIC                                                                                                                           and term date
# MAGIC Prabhu ES                    2022-03-21                  S2S                                                   MSSQL ODBC conn params added                             IntegrateDev5	Ken Bradmon	2022-05-20
# MAGIC 
# MAGIC Arpitha V                      2024-04-18                 US 616210                               Added GRP_ID parameter                                                      IntegrateDev2       Jeyaprasanna     2024-06-21

# MAGIC This job is developed to move lon running part of sql in the traceability reporting stored procedure [dbo].[TMGSP_FACETS_EAM_RECON_RPT].
# MAGIC Extracting data from BLBL table. To Narrow down the members, only required group IDs are used
# MAGIC Facets table cannot have more than one entry for this key combination.
# MAGIC 
# MAGIC MBI,MemberID,GroupID and MEME_CK. For PCPID we are getting duplicates and there is no criteria to pick one. So removing the duplicates based on this key combination
# MAGIC Extracting data from MEMD table. From each Event Code Type table, record corresponding to maximum effective_date is extracted
# MAGIC Extracting data from MEMD table. From each Event Code Type table, record corresponding to mimimum effective_date is extracted
# MAGIC Extracting data from MEMD table. From each Event Code Type table, record corresponding to current date between effective and term dates is extracted
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
# COMMAND ----------
# MAGIC %run ../../../Utility_Integrate
# COMMAND ----------


FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
RUNID = get_widget_value('RUNID','')
CurrentDate = get_widget_value('CurrentDate','')
GRP_ID = get_widget_value('GRP_ID','')

jdbc_url, jdbc_props = get_db_config(facets_secret_name)

# MedicarePartDt (ODBCConnectorPX)
sql_MedicarePartDt = f"""select MEMD.MEME_CK, 
LTRIM(RTRIM(ISNULL(MEMD.MEMD_EVENT_CD,''))) AS MEMD_EVENT_CD,
LTRIM(RTRIM(ISNULL(CONVERT(VARCHAR(10), MEMD.MEMD_HCFA_EFF_DT,101),''))) AS FACETS_MedicarePartA,
LTRIM(RTRIM(ISNULL(CONVERT(VARCHAR(10), MEMD.MEMD_HCFA_EFF_DT,101),''))) as FACETS_MedicarePartB,
LTRIM(RTRIM(ISNULL(CONVERT(VARCHAR(10), MEMD.MEMD_HCFA_EFF_DT,101),''))) as FACETS_MedicarePartD,
CASE WHEN MEMD.MEMD_HCFA_EFF_DT < '01/01/2020' THEN '01/01/2020' ELSE '' END as FACETS_PRTDT,
CASE WHEN MEMD.MEMD_ENRL_SOURCE='B' THEN 'Beneficiary election'
     WHEN MEMD.MEMD_ENRL_SOURCE='A' THEN 'Auto enrolled by CMS'
     WHEN MEMD.MEMD_ENRL_SOURCE='C' THEN 'Facilitated enrollment by CMS'
     WHEN MEMD.MEMD_ENRL_SOURCE='D' THEN 'Systematic enollment by CMS (rollover)'
     WHEN MEMD.MEMD_ENRL_SOURCE='E' THEN 'Plan initiated auto-enrollment'
     WHEN MEMD.MEMD_ENRL_SOURCE='F' THEN 'Plan initiated faciliated enrollment'
     WHEN MEMD.MEMD_ENRL_SOURCE='G' THEN 'Point-of-sale enrollment'
     WHEN MEMD.MEMD_ENRL_SOURCE='H' THEN 'CMS or Plan reassignment'
     WHEN MEMD.MEMD_ENRL_SOURCE='I' THEN 'Plan enrollment sources other than B,E,F,G,H'
     WHEN MEMD.MEMD_ENRL_SOURCE='L' THEN 'Beneficiary Election in Financial Alignment Demonstration'
     ELSE ''
END as FACETS_EnrollSource
from {FacetsOwner}.CMC_MEMD_MECR_DETL AS MEMD
WHERE  MEMD.MEMD_HCFA_EFF_DT = (
   SELECT MIN(MEMD_HCFA_EFF_DT) 
   FROM {FacetsOwner}.CMC_MEMD_MECR_DETL AS MEMD1
   WHERE MEMD.MEME_CK = MEMD1.MEME_CK 
     AND MEMD1.MEMD_EVENT_CD = MEMD.MEMD_EVENT_CD
)
AND MEMD.MEMD_EVENT_CD IN ('PRTA','PRTB','PRTD')
"""
df_MedicarePartDt = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", sql_MedicarePartDt)
    .load()
)

# CRCO_TERM (ODBCConnectorPX)
sql_Att_CRCOTerm = f"""SELECT 
 CRCO_TERM.MEME_CK,            
 CASE              
        WHEN CRCO_TERM.MEMD_EVENT_CD = 'CRCO'              
        THEN '0'              
        ELSE NULL              
    END as FACETS_CreditCover_TERM
FROM  {FacetsOwner}.CMC_MEMD_MECR_DETL  AS CRCO_TERM
where CRCO_TERM.MEMD_EVENT_CD = 'CRCO'
  AND CRCO_TERM.MEMD_HCFA_EFF_DT = (
    SELECT MAX(MEMD_HCFA_EFF_DT) 
    FROM {FacetsOwner}.CMC_MEMD_MECR_DETL  AS CRCO_TERM1
    WHERE CRCO_TERM.MEME_CK = CRCO_TERM1.MEME_CK 
      AND CRCO_TERM1.MEMD_EVENT_CD = 'CRCO'
      and CRCO_TERM1.MEMD_HCFA_TERM_DT < '{CurrentDate}'
  )
GROUP BY CRCO_TERM.MEME_CK,CRCO_TERM.MEMD_EVENT_CD
"""
df_Att_CRCOTerm = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", sql_Att_CRCOTerm)
    .load()
)

# ESRD_PERM (ODBCConnectorPX)
sql_Att_ESRD_PREM = f"""select MEMD.MEME_CK,
MEMD.MEMD_EVENT_CD,
CASE              
       WHEN MEMD.MEMD_EVENT_CD = 'ESRD'              
        THEN 'YES'              
        ELSE 'NO'              
    END as FACETS_ESRD,
CASE
WHEN MEMD.MEMD_EVENT_CD = 'PREM'   
THEN  LTRIM(RTRIM(ISNULL(MEMD.MEMD_PREM_WH_OPT ,'')))
ELSE ''
END as FACETS_PremWithholdOption,
CONVERT(VARCHAR(10),MEMD.MEMD_HCFA_EFF_DT,101) AS FACETS_LateEffectiveDate
from  {FacetsOwner}.CMC_MEMD_MECR_DETL  MEMD
where MEMD.MEMD_EVENT_CD in ('ESRD','PREM','LATE')
  AND '{CurrentDate}' BETWEEN MEMD.MEMD_HCFA_EFF_DT AND MEMD.MEMD_HCFA_TERM_DT
"""
df_Att_ESRD_PREM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", sql_Att_ESRD_PREM)
    .load()
)

# MEMD_MAX (ODBCConnectorPX)
sql_MEMD_MAX = f"""SELECT MEME_CK,
LTRIM(RTRIM(MEMD.MEMD_EVENT_CD)) AS MEMD_EVENT_CD,
MEMD.MEME_HICN as FACETS_HIC,
LTRIM(RTRIM(ISNULL(CONVERT(VARCHAR(10),MEMD.MEMD_SIG_DT,101),''))) AS FACETS_Signature_Date,
LTRIM(RTRIM(ISNULL(MEMD.MEMD_MCTR_PBP ,''))) as FACETS_PBPID,
CASE              
  WHEN MEMD.MEMD_ELECT_TYPE='A' THEN 'AEP'
  WHEN MEMD.MEMD_ELECT_TYPE='C' THEN 'SEPC'
  WHEN MEMD.MEMD_ELECT_TYPE='D' THEN 'MAPD'
  WHEN MEMD.MEMD_ELECT_TYPE='E' THEN 'IEP'
  WHEN MEMD.MEMD_ELECT_TYPE='F' THEN 'IEP2'
  WHEN MEMD.MEMD_ELECT_TYPE='I' THEN 'ICEP'
  WHEN MEMD.MEMD_ELECT_TYPE='J' THEN 'SEPJ'
  WHEN MEMD.MEMD_ELECT_TYPE='L' THEN 'SEPL'
  WHEN MEMD.MEMD_ELECT_TYPE='M' THEN 'MAOEP'
  WHEN MEMD.MEMD_ELECT_TYPE='N' THEN 'OEPNEW'
  WHEN MEMD.MEMD_ELECT_TYPE='R' THEN 'SEPR'
  WHEN MEMD.MEMD_ELECT_TYPE='S' THEN 'SEPS'
  WHEN MEMD.MEMD_ELECT_TYPE='T' THEN 'OEPI'
  WHEN MEMD.MEMD_ELECT_TYPE='U' THEN 'SEPU'
  WHEN MEMD.MEMD_ELECT_TYPE='V' THEN 'SEPV'
  WHEN MEMD.MEMD_ELECT_TYPE='W' THEN 'SEPW'
  WHEN MEMD.MEMD_ELECT_TYPE='X' THEN 'SEPX'
  WHEN MEMD.MEMD_ELECT_TYPE='Y' THEN 'SEPY'
  WHEN MEMD.MEMD_ELECT_TYPE='Z' THEN 'CEP'
  ELSE ''
END as FACETS_Election_Type,
CASE WHEN LTRIM(RTRIM(ISNULL(MEMD.MEMD_EVENT_CD,'')))='MDCD' THEN 'YES' ELSE 'NO' END AS FACETS_MemberMCaid,
LTRIM(RTRIM(ISNULL(MEMD.MEMD_EVENT_CD,''))) as FACETS_MemberMCaid2,
CASE              
        WHEN MEMD.MEMD_EVENT_CD = 'EGHP'
        THEN '1'
        ELSE NULL
    END as FACETS_EGHP,
LTRIM(RTRIM(ISNULL(MEMD.MEMD_RX_ID ,''))) as FACETS_PrimaryRXID,
CASE WHEN MEMD.MEMD_COB_IND = 'Y' THEN '1' ELSE '0' END AS  FACETS_SecondaryRxInsurFlag,
LTRIM(RTRIM(ISNULL(MEMD.MEMD_COB_RX_GROUP ,''))) as FACETS_SecondaryRxGroup,
LTRIM(RTRIM(ISNULL(MEMD.MEMD_COB_RX_ID ,''))) as FACETS_SecondaryRxID,
CASE WHEN MEMD.MEMD_COB_RXBIN IS NULL OR MEMD.MEMD_COB_RXBIN = '' OR LTRIM(RTRIM(MEMD.MEMD_COB_RXBIN)) = '0' THEN '0' ELSE LTRIM(RTRIM(MEMD.MEMD_COB_RXBIN)) END as FACETS_SecondaryRXBIN,              
LTRIM(RTRIM(ISNULL(MEMD.MEMD_COB_RXPCN ,''))) as FACETS_SecondaryRXPCN,
CONVERT(VARCHAR(10),MEMD.MEMD_HCFA_EFF_DT,101) AS FACETS_DOD,
CONVERT(VARCHAR(10),MEMD.MEMD_EVENT_EFF_DT,101) AS FACETS_Employer_Subsidy_Enrollment_Override,
CONVERT(VARCHAR(10),MEMD.MEMD_HCFA_EFF_DT,101) AS FACETS_Employer_Subsidy_Enrollment_Override2,
CONVERT(VARCHAR(10),MEMD.MEMD_HCFA_EFF_DT,101) AS FACETS_HospStartDate,
CONVERT(VARCHAR(10),MEMD.MEMD_HCFA_TERM_DT,101) AS FACETS_HospEndDate,
CASE
  WHEN ISNULL(MEMD.MEMD_UNCOV_MOS,0) = 0  THEN ''
  WHEN CONVERT(VARCHAR(10), MEMD.MEMD_HCFA_EFF_DT, 101) = CONVERT(VARCHAR(10), MEMD.MEMD_HCFA_TERM_DT, 101) THEN ''
  ELSE LTRIM(RTRIM(ISNULL(RIGHT('000' + CAST(MEMD.MEMD_UNCOV_MOS AS VARCHAR(3)), 3),'000'))) END AS FACETS_UncoveredMonths,
LTRIM(RTRIM(ISNULL(MEMD.MEMD_LATE_PENALTY,0))) AS FACETS_PenaltyAmount,
CASE              
        WHEN MEMD.MEMD_EVENT_CD = 'CRCO'
        THEN '1'
        ELSE NULL
    END as FACETS_CreditCover,
CONVERT(VARCHAR(10),MEMD.MEMD_HCFA_EFF_DT,101) AS FACETS_MSPStartDate,              
CONVERT(VARCHAR(10),MEMD.MEMD_HCFA_TERM_DT,101) AS FACETS_MSPEndDate,
CONVERT(VARCHAR(10),MEMD.MEMD_HCFA_EFF_DT,101) AS FACETS_WorkingAgedStartDate,              
CONVERT(VARCHAR(10),MEMD.MEMD_HCFA_TERM_DT,101) AS FACETS_WorkingAgedEndDate
FROM {FacetsOwner}.CMC_MEMD_MECR_DETL  AS MEMD
where MEMD.MEMD_EVENT_CD in ( 'HICN','PBP','MDCD','EGHP','PRTD','DETH','ESEO','HSPC','LATE','CRCO','MSP','WKAG' )
  and MEMD.MEMD_HCFA_EFF_DT  = (
    SELECT MAX(MEMD_HCFA_EFF_DT) 
    FROM {FacetsOwner}.CMC_MEMD_MECR_DETL  AS MEMD1
    WHERE MEMD.MEME_CK = MEMD1.MEME_CK  
      and MEMD.MEMD_EVENT_CD = MEMD1.MEMD_EVENT_CD
)
"""
df_MEMDMax = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", sql_MEMD_MAX)
    .load()
)

# LICS (ODBCConnectorPX)
sql_Att_LICS = f"""SELECT 
MEMD.MEME_CK,
MEMD.MEMD_EVENT_CD,
LTRIM(RTRIM(ISNULL(MEMD.MEMD_PARTD_SBSDY,'000'))) AS FACETS_SubsidyLevel,              
CONVERT(VARCHAR(10),MEMD.MEMD_HCFA_EFF_DT,101) AS FACETS_Subsidy_Level_Start_Date,              
CONVERT(VARCHAR(10),MEMD.MEMD_HCFA_TERM_DT,101) AS FACETS_Subsidy_Level_End_Date,              
LTRIM(RTRIM(ISNULL(MEMD.MEMD_COPAY_CAT,''))) AS FACETS_CoPayCategory,              
LTRIM(RTRIM(ISNULL(MEMD.MEMD_LICS_SBSDY,0))) AS FACETS_SubsidyAmount,
LTRIM(RTRIM(ISNULL(MEMD.MEMD_LATE_PENALTY,0))) AS FACETS_PenaltyAmount,
LTRIM(RTRIM(ISNULL(MEMD.MEMD_RX_ID ,''))) as FACETS_PrimaryRXID
FROM  {FacetsOwner}.CMC_MEMD_MECR_DETL AS  MEMD
WHERE MEMD.MEMD_EVENT_CD in ( 'LICS','LATE','PRTD') 
  AND MEMD.MEMD_HCFA_EFF_DT = (
    SELECT MAX(MEMD_HCFA_EFF_DT) 
    FROM {FacetsOwner}.CMC_MEMD_MECR_DETL AS  MEMD1
    WHERE MEMD.MEME_CK = MEMD1.MEME_CK 
      AND MEMD1.MEMD_EVENT_CD = MEMD.MEMD_EVENT_CD 
      AND '{CurrentDate}' BETWEEN MEMD1.MEMD_HCFA_EFF_DT and MEMD1.MEMD_HCFA_TERM_DT
)
"""
df_Att_LICS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", sql_Att_LICS)
    .load()
)

# BINC (ODBCConnectorPX)
sql_Ref_AttBincCp = f"""SELECT DISTINCT BLBL.SBSB_CK AS FACETS_MemberID, 
CASE WHEN LTRIM(RTRIM(ISNULL(BINC.BLDF_MCTR_STMT,''))) = 'MPB' THEN 'MDIRECT'
ELSE LTRIM(RTRIM(ISNULL(BINC.BLDF_MCTR_STMT,'')))
END AS FACETS_BillingEntityID,
CASE WHEN LTRIM(RTRIM(ISNULL(BINC.BLDF_STOCK_ID,''))) IN ('RRB','OPM') THEN 'SSA'
ELSE LTRIM(RTRIM(ISNULL(BINC.BLDF_STOCK_ID,''))) END AS FACETS_StockID
FROM 
{FacetsOwner}.CDS_BEIN_BL_INDIC AS BLBL
LEFT OUTER JOIN {FacetsOwner}.CMC_BLDF_BL_DEFINE AS BINC
   ON BLBL.BLEI_CK = BINC.BLEI_CK
   AND BINC.BLDF_EFF_DT = (
     SELECT MAX(BLDF_EFF_DT) 
     FROM {FacetsOwner}.CMC_BLDF_BL_DEFINE AS BINC1
     WHERE BINC1.BLEI_CK = BINC.BLEI_CK
   )
   AND BLDF_TERM_DT = (
     SELECT MAX(BLDF_TERM_DT) 
     FROM {FacetsOwner}.CMC_BLDF_BL_DEFINE AS BINC2
     WHERE BINC.BLDF_EFF_DT = BINC2.BLDF_EFF_DT 
       AND BINC.BLEI_CK = BINC2.BLEI_CK
   )
INNER JOIN {FacetsOwner}.CMC_GRGR_GROUP GRGR on BLBL.GRGR_CK = GRGR.GRGR_CK
AND GRGR.GRGR_ID IN ({GRP_ID})
and BLBL.SBSB_CK <>0
"""
df_Ref_AttBincCp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", sql_Ref_AttBincCp)
    .load()
)

# MemberAttr (ODBCConnectorPX)
sql_MemAttr = f"""select 
DISTINCT
 MEME.MEME_CK,
 MEME.SBSB_CK,
 LTRIM(RTRIM(ISNULL(SBSB.SBSB_ID,''))) AS FACETS_MemberID,
 LTRIM(RTRIM(ISNULL(MEME.MEME_FIRST_NAME,''))) AS FACETS_FirstName,  
 LTRIM(RTRIM(ISNULL(MEME.MEME_LAST_NAME,''))) AS FACETS_LastName,
 LTRIM(RTRIM(ISNULL(BGBG.BGBG_ID,''))) AS FACETS_PlanID,
 LTRIM(RTRIM(ISNULL(MEME.MEME_MID_INIT,''))) AS FACETS_MI,
 SUBSTRING(LTRIM(RTRIM(ISNULL(SBAD.SBAD_ADDR1,''))),1,35) AS FACETS_Address1,
 SUBSTRING(LTRIM(RTRIM(ISNULL(SBAD.SBAD_ADDR2,''))),1,35) AS FACETS_Address2,
 SUBSTRING(LTRIM(RTRIM(ISNULL(SBAD.SBAD_CITY ,''))),1,19) as FACETS_City,
 SUBSTRING(LTRIM(RTRIM(ISNULL(SBAD.SBAD_STATE ,''))),1,19) as FACETS_State,
 SUBSTRING(REPLACE(LTRIM(RTRIM(ISNULL(SBAD.SBAD_ZIP ,''))),'-',''),1,5) as FACETS_Zip,
 LTRIM(RTRIM(ISNULL(SCCC.MEMD_MCTR_MCST ,'')))+ LTRIM(RTRIM(ISNULL(SCCC.MEMD_MCTR_MCCT ,''))) as FACETS_SCCCode,
 LTRIM(RTRIM(ISNULL(REPLACE(
  REPLACE(
  REPLACE(
   REPLACE
   (
    REPLACE(
    SBAD.SBAD_PHONE,'(','' 
     ),')',''
   ),'-',''
   ),'',''
  ),'__________',''),''))) as FACETS_MemberPhone,
 LTRIM(RTRIM(ISNULL(SBAD.SBAD_EMAIL ,''))) as FACETS_MemberEmailAddress,
 ISNULL(CONVERT(VARCHAR(10),MEPE_MED.MEPE_EFF_DT,101),'') AS FACETS_Effective_Date,
 LTRIM(RTRIM(ISNULL(CONVERT(VARCHAR(10),MEME.MEME_BIRTH_DT,101),''))) AS FACETS_DOB,
 LTRIM(RTRIM(ISNULL(MEME.MEME_SEX,''))) AS FACETS_SEX,
 LTRIM(RTRIM(ISNULL(MEME.MEME_SSN ,''))) as FACETS_SSN,
 LTRIM(RTRIM(ISNULL(MEME.MEME_HICN,''))) as FACETS_Medicare_Number,
 LTRIM(RTRIM(ISNULL(MEME.MEME_MEDCD_NO,''))) AS FACETS_MedicaidNumber,
 LTRIM(RTRIM(ISNULL(MCTRL.MCTR_VALUE,''))) as FACETS_Language,
 LTRIM(RTRIM(ISNULL(MEPR.PRPR_ID ,''))) as FACETS_PCPID,
CASE 
  WHEN CHARINDEX('.',PRPRPCP.PRPR_NAME)=0 THEN LTRIM(RTRIM(ISNULL(PRPRPCP.PRPR_NAME,'')))
  ELSE LTRIM(RTRIM(ISNULL(SUBSTRING(PRPRPCP.PRPR_NAME,0,CHARINDEX('.',PRPRPCP.PRPR_NAME)-2),'')))
END as FACETS_PrimaryCarePhysician,
CASE
  WHEN LTRIM(RTRIM(MERE.MCRE_ID)) = 'NOAGENT' THEN ''
  ELSE LTRIM(RTRIM(ISNULL(MERE.MCRE_ID,'')))
END as FACETS_SalesRepID,
LTRIM(RTRIM(ISNULL(MCRE.MCRE_NAME,''))) AS FACETS_SalesRepName,
SUBSTRING(LTRIM(RTRIM(ISNULL(SBAD2.SBAD_ADDR1,''))),1,35) AS FACETS_MailingAddress1,
SUBSTRING(LTRIM(RTRIM(ISNULL(SBAD2.SBAD_ADDR2,''))),1,35) AS FACETS_MailingAddress2,
SUBSTRING(LTRIM(RTRIM(ISNULL(SBAD2.SBAD_CITY ,''))),1,19) as FACETS_MailingCity,
SUBSTRING(LTRIM(RTRIM(ISNULL(SBAD2.SBAD_STATE ,''))),1,19) as FACETS_MailingState,
SUBSTRING(REPLACE(LTRIM(RTRIM(ISNULL(SBAD2.SBAD_ZIP ,''))),'-',''),1,5) as FACETS_MailingZip,
LTRIM(RTRIM(ISNULL(GRGR.GRGR_ID ,''))) as FACETS_GroupID,
LTRIM(RTRIM(ISNULL(SGSG.SGSG_ID ,''))) as FACETS_SubGroupID,
LTRIM(RTRIM(ISNULL(MEPE_MED.CSCS_ID ,''))) as FACETS_ClassID,
LTRIM(RTRIM(ISNULL(MEPE_MX.CSPI_ID ,''))) as FACETS_MedicalProductID,
LTRIM(RTRIM(ISNULL(MEPE_RX.CSPI_ID ,''))) as FACETS_PharmacyProductID,
LTRIM(RTRIM(ISNULL(MEPE_D.CSPI_ID ,''))) as FACETS_DentalProductID
FROM {FacetsOwner}.CMC_MEME_MEMBER AS MEME
INNER JOIN {FacetsOwner}.CMC_GRGR_GROUP  AS GRGR
   ON MEME.GRGR_CK = GRGR.GRGR_CK
   AND GRGR.GRGR_ID IN ({GRP_ID})
LEFT OUTER JOIN {FacetsOwner}.CMC_MEPE_PRCS_ELIG AS MEPE_MED
   ON MEME.MEME_CK = MEPE_MED.MEME_CK
   and MEPE_MED.MEPE_EFF_DT = (
     select MIN(MEPE_EFF_DT) 
     from {FacetsOwner}.CMC_MEPE_PRCS_ELIG AS MEPE_MED1
     where MEPE_MED.MEME_CK = MEPE_MED1.MEME_CK 
       AND MEPE_MED1.MEPE_ELIG_IND ='Y'
       AND '{CurrentDate}' BETWEEN MEPE_MED1.MEPE_EFF_DT AND MEPE_MED1.MEPE_TERM_DT
   )
   AND MEPE_MED.MEPE_ELIG_IND ='Y'
   AND '{CurrentDate}' BETWEEN MEPE_MED.MEPE_EFF_DT AND MEPE_MED.MEPE_TERM_DT
LEFT OUTER JOIN {FacetsOwner}.CMC_MEPE_PRCS_ELIG AS MEPE_RX
   ON MEME.MEME_CK = MEPE_RX.MEME_CK
   and MEPE_RX.MEPE_EFF_DT = (
     select MIN(MEPE_EFF_DT) 
     from {FacetsOwner}.CMC_MEPE_PRCS_ELIG AS MEPE_RX1
     where MEPE_RX.MEME_CK = MEPE_RX1.MEME_CK 
       AND MEPE_RX1.MEPE_ELIG_IND ='Y' 
       AND MEPE_RX1.CSPD_CAT ='R'
       AND '{CurrentDate}' BETWEEN MEPE_RX1.MEPE_EFF_DT AND MEPE_RX1.MEPE_TERM_DT
   )
   AND MEPE_RX.MEPE_ELIG_IND ='Y' 
   AND MEPE_RX.CSPD_CAT ='R'
   AND '{CurrentDate}' BETWEEN MEPE_RX.MEPE_EFF_DT AND MEPE_RX.MEPE_TERM_DT
LEFT OUTER JOIN {FacetsOwner}.CMC_SBSB_SUBSC  AS SBSB
   ON MEME.SBSB_CK=SBSB.SBSB_CK
LEFT OUTER JOIN {FacetsOwner}.CMC_SBAD_ADDR  AS SBAD
   ON SBSB.SBSB_CK=SBAD.SBSB_CK 
   AND SBAD.SBAD_TYPE = SBSB.SBAD_TYPE_HOME
LEFT OUTER JOIN  {FacetsOwner}.CMC_SBAD_ADDR SBAD2
   ON SBSB.SBSB_CK = SBAD2.SBSB_CK 
   AND SBAD2.SBAD_TYPE = SBSB.SBAD_TYPE_MAIL
LEFT OUTER JOIN {FacetsOwner}.CMC_MCTR_CD_TRANS AS MCTRL
   ON MEME.MEME_MCTR_LANG=MCTRL.MCTR_VALUE 
   AND MCTRL.MCTR_TYPE='LANG' 
   AND MCTRL.MCTR_ENTITY='ALL'
LEFT OUTER JOIN {FacetsOwner}.CMC_MEPE_PRCS_ELIG AS MEPE_MX
   ON MEME.MEME_CK = MEPE_MX.MEME_CK
   and MEPE_MX.MEPE_EFF_DT = (
     select MIN(MEPE_EFF_DT) 
     from {FacetsOwner}.CMC_MEPE_PRCS_ELIG AS MEPE_MX1
     where MEPE_MX.MEME_CK = MEPE_MX1.MEME_CK 
       AND MEPE_MX1.MEPE_ELIG_IND ='Y' 
       AND MEPE_MX1.CSPD_CAT ='M'
       AND '{CurrentDate}' BETWEEN MEPE_MX1.MEPE_EFF_DT AND MEPE_MX1.MEPE_TERM_DT
   )
   AND MEPE_MX.MEPE_ELIG_IND ='Y'
   AND MEPE_MX.CSPD_CAT ='M'
   AND '{CurrentDate}' BETWEEN MEPE_MX.MEPE_EFF_DT AND MEPE_MX.MEPE_TERM_DT
LEFT OUTER JOIN {FacetsOwner}.CMC_MEPE_PRCS_ELIG AS MEPE_D
   ON MEME.MEME_CK = MEPE_D.MEME_CK
   and MEPE_D.MEPE_ELIG_IND ='Y' 
   AND MEPE_D.CSPD_CAT ='D' 
   AND '{CurrentDate}' BETWEEN MEPE_D.MEPE_EFF_DT AND MEPE_D.MEPE_TERM_DT
LEFT OUTER JOIN {FacetsOwner}.CMC_MEMD_MECR_DETL  AS SCCC
   ON MEME.MEME_CK = SCCC.MEME_CK 
   AND SCCC.MEMD_EVENT_CD = 'SCCC'
   AND SCCC.MEMD_HCFA_EFF_DT  = (
     SELECT MAX(MEMD_HCFA_EFF_DT) 
     FROM {FacetsOwner}.CMC_MEMD_MECR_DETL  AS SCCC1
     WHERE SCCC.MEME_CK = SCCC1.MEME_CK 
       AND SCCC1.MEMD_EVENT_CD = 'SCCC'
   )
LEFT OUTER JOIN {FacetsOwner}.CMC_BGBG_BIL_GROUP BGBG
   ON SCCC.BGBG_CK = BGBG.BGBG_CK
LEFT OUTER JOIN  {FacetsOwner}.CMC_MEPR_PRIM_PROV AS MEPR
   ON MEPR.MEME_CK = MEME.MEME_CK 
   AND MEPR_PCP_TYPE='MP'
   AND '{CurrentDate}' between MEPR.MEPR_EFF_DT and MEPR.MEPR_TERM_DT
LEFT OUTER JOIN {FacetsOwner}.CMC_PRPR_PROV AS PRPRPCP
   ON PRPRPCP.PRPR_ID=MEPR.PRPR_ID
LEFT OUTER JOIN {FacetsOwner}.CMC_SGSG_SUB_GROUP AS SGSG
   ON MEPE_MED.GRGR_CK = SGSG.GRGR_CK 
   AND MEPE_MED.SGSG_CK = SGSG.SGSG_CK
LEFT OUTER JOIN {FacetsOwner}.CMC_MERE_RELATION AS MERE
   ON MERE.MEME_CK = MEME.MEME_CK 
   AND MERE.MERE_TYPE='MA'
   AND MERE.MERE_EFF_DT = (
     select MAX(MERE_EFF_DT) 
     from {FacetsOwner}.CMC_MERE_RELATION AS MERE1
     where MERE.MEME_CK = MERE1.MEME_CK 
       AND MERE1.MERE_TYPE='MA'
   )
   AND MERE.MERE_TERM_DT = (
     select MAX(MERE_TERM_DT) 
     from {FacetsOwner}.CMC_MERE_RELATION AS MERE2
     where MERE.MEME_CK = MERE2.MEME_CK 
       AND MERE.MERE_EFF_DT = MERE2.MERE_EFF_DT 
       AND MERE2.MERE_TYPE='MA'
   )
LEFT OUTER JOIN {FacetsOwner}.CMC_MCRE_RELAT_ENT AS MCRE
   ON MERE.MCRE_ID = MCRE.MCRE_ID
"""
df_MemAttr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", sql_MemAttr)
    .load()
)

# FLTR (PxFilter) splitting df_MedicarePartDt into 3 outputs
df_Ref_PRTA = df_MedicarePartDt.select(
    F.col("MEME_CK").alias("MEME_CK"),
    F.col("FACETS_MedicarePartA").alias("FACETS_MedicarePartA")
)

df_Ref_PRTB = df_MedicarePartDt.select(
    F.col("MEME_CK").alias("MEME_CK"),
    F.col("FACETS_MedicarePartB").alias("FACETS_MedicarePartB")
)

df_Ref_PRTD_FLTR = df_MedicarePartDt.select(
    F.col("MEME_CK").alias("MEME_CK"),
    F.col("FACETS_MedicarePartD").alias("FACETS_MedicarePartD"),
    F.col("FACETS_PRTDT").alias("FACETS_PRTDT"),
    F.col("FACETS_EnrollSource").alias("FACETS_EnrollSource")
)

# ESRD_PREM (PxFilter) splitting df_Att_ESRD_PREM into 3
df_Att_ESRD = df_Att_ESRD_PREM.filter(F.col("MEMD_EVENT_CD")=="ESRD").select(
    F.col("MEME_CK").alias("MEME_CK"),
    F.col("FACETS_ESRD").alias("FACETS_ESRD")
)
df_Att_PREM = df_Att_ESRD_PREM.filter(F.col("MEMD_EVENT_CD")=="PREM").select(
    F.col("MEME_CK").alias("MEME_CK"),
    F.col("FACETS_PremWithholdOption").alias("FACETS_PremWithholdOption")
)
df_Late_ESRD = df_Att_ESRD_PREM.filter(F.col("MEMD_EVENT_CD")=="LATE").select(
    F.col("MEME_CK").alias("MEME_CK"),
    F.col("FACETS_LateEffectiveDate").alias("FACETS_LateEffectiveDate")
)

# MEMD_MAX -> Xmr3 (CTransformerStage) => multiple outputs
df_Ref_LATE = df_MEMDMax.filter(F.col("MEMD_EVENT_CD")=="LATE").select(
    F.col("MEME_CK").alias("MEME_CK"),
    F.col("FACETS_UncoveredMonths").alias("FACETS_UncoveredMonths")
)
df_Ref_HSPC = df_MEMDMax.filter(F.col("MEMD_EVENT_CD")=="HSPC").select(
    F.col("MEME_CK").alias("MEME_CK"),
    F.col("FACETS_HospStartDate").alias("FACETS_HospStartDate"),
    F.col("FACETS_HospEndDate").alias("FACETS_HospEndDate")
)
df_Ref_MCAID = df_MEMDMax.filter(F.col("MEMD_EVENT_CD")=="MDCD").select(
    F.col("MEME_CK").alias("MEME_CK"),
    F.col("FACETS_MemberMCaid").alias("FACETS_MemberMCaid"),
    F.col("FACETS_MemberMCaid2").alias("FACETS_MemberMCaid2")
)
df_Ref_EGHP = df_MEMDMax.filter(F.col("MEMD_EVENT_CD")=="EGHP").select(
    F.col("MEME_CK").alias("MEME_CK"),
    F.col("FACETS_EGHP").alias("FACETS_EGHP")
)
df_Ref_CRCO = df_MEMDMax.filter(F.col("MEMD_EVENT_CD")=="CRCO").select(
    F.col("MEME_CK").alias("MEME_CK"),
    F.col("FACETS_CreditCover").alias("FACETS_CreditCover")
)
df_Ref_DETH = df_MEMDMax.filter(F.col("MEMD_EVENT_CD")=="DETH").select(
    F.col("MEME_CK").alias("MEME_CK"),
    F.col("FACETS_DOD").alias("FACETS_DOD")
)
df_Ref_ESEO = df_MEMDMax.filter(F.col("MEMD_EVENT_CD")=="ESEO").select(
    F.col("MEME_CK").alias("MEME_CK"),
    F.col("FACETS_Employer_Subsidy_Enrollment_Override").alias("FACETS_Employer_Subsidy_Enrollment_Override"),
    F.col("FACETS_Employer_Subsidy_Enrollment_Override2").alias("FACETS_Employer_Subsidy_Enrollment_Override2")
)
df_Ref_PBP = df_MEMDMax.filter(F.col("MEMD_EVENT_CD")=="PBP").select(
    F.col("MEME_CK").alias("MEME_CK"),
    F.col("FACETS_Signature_Date").alias("FACETS_Signature_Date"),
    F.col("FACETS_PBPID").alias("FACETS_PBPID"),
    F.col("FACETS_Election_Type").alias("FACETS_Election_Type")
)
df_Ref_WKAG = df_MEMDMax.filter(F.col("MEMD_EVENT_CD")=="WKAG").select(
    F.col("MEME_CK").alias("MEME_CK"),
    F.col("FACETS_WorkingAgedStartDate").alias("FACETS_WorkingAgedStartDate"),
    F.col("FACETS_WorkingAgedEndDate").alias("FACETS_WorkingAgedEndDate")
)
df_Ref_MSP = df_MEMDMax.filter(F.col("MEMD_EVENT_CD")=="MSP").select(
    F.col("MEME_CK").alias("MEME_CK"),
    F.col("FACETS_MSPStartDate").alias("FACETS_MSPStartDate"),
    F.col("FACETS_MSPEndDate").alias("FACETS_MSPEndDate")
)
df_Ref_HICN = df_MEMDMax.filter(F.col("MEMD_EVENT_CD")=="HICN").select(
    F.col("MEME_CK").alias("MEME_CK"),
    F.col("FACETS_HIC").alias("FACETS_HIC")
)

# LICS -> PxFilter
df_Ref_LATE_Penlty = df_Att_LICS.select(
    F.col("MEME_CK").alias("MEME_CK"),
    F.col("FACETS_PenaltyAmount").alias("FACETS_PenaltyAmount")
)
df_Ref_LICS = df_Att_LICS.select(
    F.col("MEME_CK").alias("MEME_CK"),
    F.col("FACETS_SubsidyLevel").alias("FACETS_SubsidyLevel"),
    F.col("FACETS_Subsidy_Level_Start_Date").alias("FACETS_Subsidy_Level_Start_Date"),
    F.col("FACETS_Subsidy_Level_End_Date").alias("FACETS_Subsidy_Level_End_Date"),
    F.col("FACETS_CoPayCategory").alias("FACETS_CoPayCategory"),
    F.col("FACETS_SubsidyAmount").alias("FACETS_SubsidyAmount")
)
df_Ref_PRTD_LICS = df_Att_LICS.select(
    F.col("MEME_CK").alias("MEME_CK"),
    F.col("FACETS_PrimaryRXID").alias("FACETS_PrimaryRXID")
)

# RMDUP_BINC (PxCopy) -> remove duplicates by FACETS_MemberID
df_Ref_AttBincCp_dedup = dedup_sort(
    df_Ref_AttBincCp,
    ["FACETS_MemberID"],
    []
).select(
    F.col("FACETS_MemberID").alias("FACETS_MemberID"),
    F.col("FACETS_BillingEntityID").alias("FACETS_BillingEntityID"),
    F.col("FACETS_StockID").alias("FACETS_StockID")
)

# Xmr (CTransformerStage) => single output df_Enrich
df_Enrich = df_MemAttr.select(
    F.col("MEME_CK").alias("MEME_CK"),
    F.col("SBSB_CK").alias("SBSB_CK"),
    F.col("FACETS_MemberID").alias("FACETS_MemberID"),
    F.col("FACETS_FirstName").alias("FACETS_FirstName"),
    F.col("FACETS_LastName").alias("FACETS_LastName"),
    F.col("FACETS_PlanID").alias("FACETS_PlanID"),
    F.col("FACETS_MI").alias("FACETS_MI"),
    F.col("FACETS_Address1").alias("FACETS_Address1"),
    F.col("FACETS_Address2").alias("FACETS_Address2"),
    F.col("FACETS_City").alias("FACETS_City"),
    F.col("FACETS_State").alias("FACETS_State"),
    F.col("FACETS_Zip").alias("FACETS_Zip"),
    F.col("FACETS_SCCCode").alias("FACETS_SCCCode"),
    F.col("FACETS_MemberPhone").alias("FACETS_MemberPhone"),
    F.col("FACETS_MemberEmailAddress").alias("FACETS_MemberEmailAddress"),
    F.col("FACETS_Effective_Date").alias("FACETS_Effective_Date"),
    F.col("FACETS_DOB").alias("FACETS_DOB"),
    F.col("FACETS_SEX").alias("FACETS_SEX"),
    F.col("FACETS_SSN").alias("FACETS_SSN"),
    F.col("FACETS_Medicare_Number").alias("FACETS_Medicare_Number"),
    F.col("FACETS_MedicaidNumber").alias("FACETS_MedicaidNumber"),
    F.col("FACETS_Language").alias("FACETS_Language"),
    F.col("FACETS_PCPID").alias("FACETS_PCPID"),
    F.col("FACETS_PrimaryCarePhysician").alias("FACETS_PrimaryCarePhysician"),
    F.col("FACETS_SalesRepID").alias("FACETS_SalesRepID"),
    F.col("FACETS_SalesRepName").alias("FACETS_SalesRepName"),
    F.col("FACETS_MailingAddress1").alias("FACETS_MailingAddress1"),
    F.col("FACETS_MailingAddress2").alias("FACETS_MailingAddress2"),
    F.col("FACETS_MailingCity").alias("FACETS_MailingCity"),
    F.col("FACETS_MailingState").alias("FACETS_MailingState"),
    F.col("FACETS_MailingZip").alias("FACETS_MailingZip"),
    F.col("FACETS_GroupID").alias("FACETS_GroupID"),
    F.col("FACETS_SubGroupID").alias("FACETS_SubGroupID"),
    F.col("FACETS_ClassID").alias("FACETS_ClassID"),
    F.col("FACETS_MedicalProductID").alias("FACETS_MedicalProductID"),
    F.col("FACETS_PharmacyProductID").alias("FACETS_PharmacyProductID"),
    F.col("FACETS_DentalProductID").alias("FACETS_DentalProductID")
)

# Att_BINCAttr (PxLookup) -> produce df_Att_CRCO_Term
df_Att_CRCO_Term = (
    df_Enrich.alias("Enrich")
    .join(df_Ref_AttBincCp_dedup.alias("Ref_AttBinc"), F.col("Enrich.FACETS_MemberID")==F.col("Ref_AttBinc.FACETS_MemberID"), how="left")
    .join(df_Ref_HICN.alias("Ref_HICN"), F.col("Enrich.MEME_CK")==F.col("Ref_HICN.MEME_CK"), how="left")
    .join(df_Ref_MSP.alias("Ref_MSP"), F.col("Enrich.MEME_CK")==F.col("Ref_MSP.MEME_CK"), how="left")
    .join(df_Ref_WKAG.alias("Ref_WKAG"), F.col("Enrich.MEME_CK")==F.col("Ref_WKAG.MEME_CK"), how="left")
    .join(df_Ref_PBP.alias("Ref_PBP"), F.col("Enrich.MEME_CK")==F.col("Ref_PBP.MEME_CK"), how="left")
    .join(df_Ref_ESEO.alias("Ref_ESEO"), F.col("Enrich.MEME_CK")==F.col("Ref_ESEO.MEME_CK"), how="left")
    .join(df_Ref_DETH.alias("Ref_DETH"), F.col("Enrich.MEME_CK")==F.col("Ref_DETH.MEME_CK"), how="left")
    .join(df_Ref_CRCO.alias("Ref_CRCO"), F.col("Enrich.MEME_CK")==F.col("Ref_CRCO.MEME_CK"), how="left")
    .join(df_Ref_EGHP.alias("Ref_EGHP"), F.col("Enrich.MEME_CK")==F.col("Ref_EGHP.MEME_CK"), how="left")
    .join(df_Ref_MCAID.alias("Ref_MCAID"), F.col("Enrich.MEME_CK")==F.col("Ref_MCAID.MEME_CK"), how="left")
    .join(df_Ref_HSPC.alias("Ref_HSPC"), F.col("Enrich.MEME_CK")==F.col("Ref_HSPC.MEME_CK"), how="left")
    .join(df_Ref_LATE.alias("Ref_LATE"), F.col("Enrich.MEME_CK")==F.col("Ref_LATE.MEME_CK"), how="left")
    .join(df_Ref_LATE_Penlty.alias("Ref_LATE_Penlty"), F.col("Enrich.MEME_CK")==F.col("Ref_LATE_Penlty.MEME_CK"), how="left")
    .join(df_Ref_LICS.alias("Ref_LICS"), F.col("Enrich.MEME_CK")==F.col("Ref_LICS.MEME_CK"), how="left")
    .join(df_Ref_PRTD_LICS.alias("Ref_PRTD"), (F.col("Enrich.MEME_CK")==F.col("Ref_PRTD.MEME_CK")), how="left")
    .select(
        F.col("Enrich.MEME_CK").alias("MEME_CK"),
        F.col("Enrich.FACETS_MemberID").alias("FACETS_MemberID"),
        F.col("Enrich.FACETS_FirstName").alias("FACETS_FirstName"),
        F.col("Enrich.FACETS_LastName").alias("FACETS_LastName"),
        F.col("Enrich.FACETS_PlanID").alias("FACETS_PlanID"),
        F.col("Enrich.FACETS_MI").alias("FACETS_MI"),
        F.col("Enrich.FACETS_Address1").alias("FACETS_Address1"),
        F.col("Enrich.FACETS_Address2").alias("FACETS_Address2"),
        F.col("Enrich.FACETS_City").alias("FACETS_City"),
        F.col("Enrich.FACETS_State").alias("FACETS_State"),
        F.col("Enrich.FACETS_Zip").alias("FACETS_Zip"),
        F.col("Enrich.FACETS_SCCCode").alias("FACETS_SCCCode"),
        F.col("Enrich.FACETS_MemberPhone").alias("FACETS_MemberPhone"),
        F.col("Enrich.FACETS_MemberEmailAddress").alias("FACETS_MemberEmailAddress"),
        F.col("Enrich.FACETS_Effective_Date").alias("FACETS_Effective_Date"),
        F.col("Enrich.FACETS_DOB").alias("FACETS_DOB"),
        F.col("Enrich.FACETS_SEX").alias("FACETS_SEX"),
        F.col("Enrich.FACETS_SSN").alias("FACETS_SSN"),
        F.col("Enrich.FACETS_Medicare_Number").alias("FACETS_Medicare_Number"),
        F.col("Enrich.FACETS_MedicaidNumber").alias("FACETS_MedicaidNumber"),
        F.col("Enrich.FACETS_Language").alias("FACETS_Language"),
        F.col("Enrich.FACETS_PCPID").alias("FACETS_PCPID"),
        F.col("Enrich.FACETS_PrimaryCarePhysician").alias("FACETS_PrimaryCarePhysician"),
        F.col("Enrich.FACETS_SalesRepID").alias("FACETS_SalesRepID"),
        F.col("Enrich.FACETS_SalesRepName").alias("FACETS_SalesRepName"),
        F.col("Enrich.FACETS_MailingAddress1").alias("FACETS_MailingAddress1"),
        F.col("Enrich.FACETS_MailingAddress2").alias("FACETS_MailingAddress2"),
        F.col("Enrich.FACETS_MailingCity").alias("FACETS_MailingCity"),
        F.col("Enrich.FACETS_MailingState").alias("FACETS_MailingState"),
        F.col("Enrich.FACETS_MailingZip").alias("FACETS_MailingZip"),
        F.col("Enrich.FACETS_GroupID").alias("FACETS_GroupID"),
        F.col("Enrich.FACETS_SubGroupID").alias("FACETS_SubGroupID"),
        F.col("Enrich.FACETS_ClassID").alias("FACETS_ClassID"),
        F.col("Enrich.FACETS_MedicalProductID").alias("FACETS_MedicalProductID"),
        F.col("Enrich.FACETS_PharmacyProductID").alias("FACETS_PharmacyProductID"),
        F.col("Ref_AttBinc.FACETS_BillingEntityID").alias("FACETS_BillingEntityID"),
        F.col("Ref_AttBinc.FACETS_StockID").alias("FACETS_StockID"),
        F.col("Ref_HICN.FACETS_HIC").alias("FACETS_HIC"),
        F.col("Ref_MSP.FACETS_MSPStartDate").alias("FACETS_MSPStartDate"),
        F.col("Ref_MSP.FACETS_MSPEndDate").alias("FACETS_MSPEndDate"),
        F.col("Ref_WKAG.FACETS_WorkingAgedStartDate").alias("FACETS_WorkingAgedStartDate"),
        F.col("Ref_WKAG.FACETS_WorkingAgedEndDate").alias("FACETS_WorkingAgedEndDate"),
        F.col("Ref_PBP.FACETS_Signature_Date").alias("FACETS_Signature_Date"),
        F.col("Ref_PBP.FACETS_PBPID").alias("FACETS_PBPID"),
        F.col("Ref_PBP.FACETS_Election_Type").alias("FACETS_Election_Type"),
        F.col("Ref_ESEO.FACETS_Employer_Subsidy_Enrollment_Override").alias("FACETS_Employer_Subsidy_Enrollment_Override"),
        F.col("Ref_ESEO.FACETS_Employer_Subsidy_Enrollment_Override2").alias("FACETS_Employer_Subsidy_Enrollment_Override2"),
        F.col("Ref_DETH.FACETS_DOD").alias("FACETS_DOD"),
        F.col("Ref_CRCO.FACETS_CreditCover").alias("FACETS_CreditCover"),
        F.col("Ref_PRTD.FACETS_PrimaryRXID").alias("FACETS_PrimaryRXID"),
        F.col("Ref_EGHP.FACETS_EGHP").alias("FACETS_EGHP"),
        F.col("Ref_MCAID.FACETS_MemberMCaid").alias("FACETS_MemberMCaid"),
        F.col("Ref_MCAID.FACETS_MemberMCaid2").alias("FACETS_MemberMCaid2"),
        F.col("Ref_HSPC.FACETS_HospStartDate").alias("FACETS_HospStartDate"),
        F.col("Ref_HSPC.FACETS_HospEndDate").alias("FACETS_HospEndDate"),
        F.col("Ref_LATE.FACETS_UncoveredMonths").alias("FACETS_UncoveredMonths"),
        F.col("Ref_LATE_Penlty.FACETS_PenaltyAmount").alias("FACETS_PenaltyAmount"),
        F.col("Ref_LICS.FACETS_SubsidyLevel").alias("FACETS_SubsidyLevel"),
        F.col("Ref_LICS.FACETS_Subsidy_Level_Start_Date").alias("FACETS_Subsidy_Level_Start_Date"),
        F.col("Ref_LICS.FACETS_Subsidy_Level_End_Date").alias("FACETS_Subsidy_Level_End_Date"),
        F.col("Ref_LICS.FACETS_CoPayCategory").alias("FACETS_CoPayCategory"),
        F.col("Ref_LICS.FACETS_SubsidyAmount").alias("FACETS_SubsidyAmount"),
        F.col("Enrich.FACETS_DentalProductID").alias("FACETS_DentalProductID")
    )
)

# Att_CrcoTerm (PxLookup) -> df_Att_MediCarepart
df_Att_MediCarepart = (
    df_Att_CRCO_Term.alias("Att_CRCO_Term")
    .join(df_Att_CRCOTerm.alias("Att_CRCOTerm"), F.lit(True), how="left")  # No join condition specified in JSON
    .join(df_Att_PREM.alias("Att_PREM"), F.col("Att_CRCO_Term.MEME_CK")==F.col("Att_PREM.MEME_CK"), how="left")
    .join(df_Att_ESRD.alias("Att_ESRD"), F.col("Att_CRCO_Term.MEME_CK")==F.col("Att_ESRD.MEME_CK"), how="left")
    .join(df_Late_ESRD.alias("Late"), F.col("Att_CRCO_Term.MEME_CK")==F.col("Late.MEME_CK"), how="left")
    .select(
        F.col("Att_CRCO_Term.MEME_CK").alias("MEME_CK"),
        F.col("Att_CRCO_Term.FACETS_MemberID").alias("FACETS_MemberID"),
        F.col("Att_CRCO_Term.FACETS_FirstName").alias("FACETS_FirstName"),
        F.col("Att_CRCO_Term.FACETS_LastName").alias("FACETS_LastName"),
        F.col("Att_CRCO_Term.FACETS_PlanID").alias("FACETS_PlanID"),
        F.col("Att_CRCO_Term.FACETS_MI").alias("FACETS_MI"),
        F.col("Att_CRCO_Term.FACETS_Address1").alias("FACETS_Address1"),
        F.col("Att_CRCO_Term.FACETS_Address2").alias("FACETS_Address2"),
        F.col("Att_CRCO_Term.FACETS_City").alias("FACETS_City"),
        F.col("Att_CRCO_Term.FACETS_State").alias("FACETS_State"),
        F.col("Att_CRCO_Term.FACETS_Zip").alias("FACETS_Zip"),
        F.col("Att_CRCO_Term.FACETS_SCCCode").alias("FACETS_SCCCode"),
        F.col("Att_CRCO_Term.FACETS_MemberPhone").alias("FACETS_MemberPhone"),
        F.col("Att_CRCO_Term.FACETS_MemberEmailAddress").alias("FACETS_MemberEmailAddress"),
        F.col("Att_CRCO_Term.FACETS_Effective_Date").alias("FACETS_Effective_Date"),
        F.col("Att_CRCO_Term.FACETS_DOB").alias("FACETS_DOB"),
        F.col("Att_CRCO_Term.FACETS_SEX").alias("FACETS_SEX"),
        F.col("Att_CRCO_Term.FACETS_SSN").alias("FACETS_SSN"),
        F.col("Att_CRCO_Term.FACETS_Medicare_Number").alias("FACETS_Medicare_Number"),
        F.col("Att_CRCO_Term.FACETS_MedicaidNumber").alias("FACETS_MedicaidNumber"),
        F.col("Att_CRCO_Term.FACETS_Language").alias("FACETS_Language"),
        F.col("Att_CRCO_Term.FACETS_PCPID").alias("FACETS_PCPID"),
        F.col("Att_CRCO_Term.FACETS_PrimaryCarePhysician").alias("FACETS_PrimaryCarePhysician"),
        F.col("Att_CRCO_Term.FACETS_SalesRepID").alias("FACETS_SalesRepID"),
        F.col("Att_CRCO_Term.FACETS_SalesRepName").alias("FACETS_SalesRepName"),
        F.col("Att_CRCO_Term.FACETS_MailingAddress1").alias("FACETS_MailingAddress1"),
        F.col("Att_CRCO_Term.FACETS_MailingAddress2").alias("FACETS_MailingAddress2"),
        F.col("Att_CRCO_Term.FACETS_MailingCity").alias("FACETS_MailingCity"),
        F.col("Att_CRCO_Term.FACETS_MailingState").alias("FACETS_MailingState"),
        F.col("Att_CRCO_Term.FACETS_MailingZip").alias("FACETS_MailingZip"),
        F.col("Att_CRCO_Term.FACETS_GroupID").alias("FACETS_GroupID"),
        F.col("Att_CRCO_Term.FACETS_SubGroupID").alias("FACETS_SubGroupID"),
        F.col("Att_CRCO_Term.FACETS_ClassID").alias("FACETS_ClassID"),
        F.col("Att_CRCO_Term.FACETS_MedicalProductID").alias("FACETS_MedicalProductID"),
        F.col("Att_CRCO_Term.FACETS_PharmacyProductID").alias("FACETS_PharmacyProductID"),
        F.col("Att_CRCO_Term.FACETS_BillingEntityID").alias("FACETS_BillingEntityID"),
        F.col("Att_CRCO_Term.FACETS_StockID").alias("FACETS_StockID"),
        F.col("Att_CRCO_Term.FACETS_HIC").alias("FACETS_HIC"),
        F.col("Att_CRCO_Term.FACETS_MSPStartDate").alias("FACETS_MSPStartDate"),
        F.col("Att_CRCO_Term.FACETS_MSPEndDate").alias("FACETS_MSPEndDate"),
        F.col("Att_CRCO_Term.FACETS_WorkingAgedStartDate").alias("FACETS_WorkingAgedStartDate"),
        F.col("Att_CRCO_Term.FACETS_WorkingAgedEndDate").alias("FACETS_WorkingAgedEndDate"),
        F.col("Att_CRCO_Term.FACETS_Signature_Date").alias("FACETS_Signature_Date"),
        F.col("Att_CRCO_Term.FACETS_PBPID").alias("FACETS_PBPID"),
        F.col("Att_CRCO_Term.FACETS_Election_Type").alias("FACETS_Election_Type"),
        F.col("Att_CRCO_Term.FACETS_Employer_Subsidy_Enrollment_Override").alias("FACETS_Employer_Subsidy_Enrollment_Override"),
        F.col("Att_CRCO_Term.FACETS_Employer_Subsidy_Enrollment_Override2").alias("FACETS_Employer_Subsidy_Enrollment_Override2"),
        F.col("Att_CRCO_Term.FACETS_DOD").alias("FACETS_DOD"),
        F.col("Att_CRCO_Term.FACETS_CreditCover").alias("FACETS_CreditCover"),
        F.col("Att_CRCO_Term.FACETS_PrimaryRXID").alias("FACETS_PrimaryRXID"),
        F.col("Att_CRCO_Term.FACETS_EGHP").alias("FACETS_EGHP"),
        F.col("Att_CRCO_Term.FACETS_MemberMCaid").alias("FACETS_MemberMCaid"),
        F.col("Att_CRCO_Term.FACETS_MemberMCaid2").alias("FACETS_MemberMCaid2"),
        F.col("Att_CRCO_Term.FACETS_HospStartDate").alias("FACETS_HospStartDate"),
        F.col("Att_CRCO_Term.FACETS_HospEndDate").alias("FACETS_HospEndDate"),
        F.col("Att_CRCO_Term.FACETS_UncoveredMonths").alias("FACETS_UncoveredMonths"),
        F.col("Att_CRCO_Term.FACETS_PenaltyAmount").alias("FACETS_PenaltyAmount"),
        F.col("Att_CRCO_Term.FACETS_SubsidyLevel").alias("FACETS_SubsidyLevel"),
        F.col("Att_CRCO_Term.FACETS_Subsidy_Level_Start_Date").alias("FACETS_Subsidy_Level_Start_Date"),
        F.col("Att_CRCO_Term.FACETS_Subsidy_Level_End_Date").alias("FACETS_Subsidy_Level_End_Date"),
        F.col("Att_CRCO_Term.FACETS_CoPayCategory").alias("FACETS_CoPayCategory"),
        F.col("Att_CRCO_Term.FACETS_SubsidyAmount").alias("FACETS_SubsidyAmount"),
        F.col("Att_CRCOTerm.FACETS_CreditCover_TERM").alias("FACETS_CreditCover_TERM"),
        F.col("Att_ESRD.FACETS_ESRD").alias("FACETS_ESRD"),
        F.col("Att_PREM.FACETS_PremWithholdOption").alias("FACETS_PremWithholdOption"),
        F.col("Late.FACETS_LateEffectiveDate").alias("FACETS_LateEffectiveDate"),
        F.col("Att_CRCO_Term.FACETS_DentalProductID").alias("FACETS_DentalProductID")
    )
)

# Att_MediCarePart (PxLookup) -> finalXfm
df_FinalXfm = (
    df_Att_MediCarepart.alias("Att_MediCarepart")
    .join(df_Ref_PRTA.alias("Ref_PRTA"), F.col("Att_MediCarepart.MEME_CK")==F.col("Ref_PRTA.MEME_CK"), how="left")
    .join(df_Ref_PRTB.alias("Ref_PRTB"), F.col("Att_MediCarepart.MEME_CK")==F.col("Ref_PRTB.MEME_CK"), how="left")
    .join(df_Ref_PRTD_FLTR.alias("Ref_PRTD"), (F.col("Att_MediCarepart.MEME_CK")==F.col("Ref_PRTD.MEME_CK")), how="left")
    .select(
        F.col("Att_MediCarepart.MEME_CK").alias("MEME_CK"),
        F.col("Att_MediCarepart.FACETS_MemberID").alias("FACETS_MemberID"),
        F.col("Att_MediCarepart.FACETS_FirstName").alias("FACETS_FirstName"),
        F.col("Att_MediCarepart.FACETS_LastName").alias("FACETS_LastName"),
        F.col("Att_MediCarepart.FACETS_PlanID").alias("FACETS_PlanID"),
        F.col("Att_MediCarepart.FACETS_MI").alias("FACETS_MI"),
        F.col("Att_MediCarepart.FACETS_Address1").alias("FACETS_Address1"),
        F.col("Att_MediCarepart.FACETS_Address2").alias("FACETS_Address2"),
        F.col("Att_MediCarepart.FACETS_City").alias("FACETS_City"),
        F.col("Att_MediCarepart.FACETS_State").alias("FACETS_State"),
        F.col("Att_MediCarepart.FACETS_Zip").alias("FACETS_Zip"),
        F.col("Att_MediCarepart.FACETS_SCCCode").alias("FACETS_SCCCode"),
        F.col("Att_MediCarepart.FACETS_MemberPhone").alias("FACETS_MemberPhone"),
        F.col("Att_MediCarepart.FACETS_MemberEmailAddress").alias("FACETS_MemberEmailAddress"),
        F.col("Att_MediCarepart.FACETS_Effective_Date").alias("FACETS_Effective_Date"),
        F.col("Att_MediCarepart.FACETS_DOB").alias("FACETS_DOB"),
        F.col("Att_MediCarepart.FACETS_SEX").alias("FACETS_SEX"),
        F.col("Att_MediCarepart.FACETS_SSN").alias("FACETS_SSN"),
        F.col("Att_MediCarepart.FACETS_Medicare_Number").alias("FACETS_Medicare_Number"),
        F.col("Att_MediCarepart.FACETS_MedicaidNumber").alias("FACETS_MedicaidNumber"),
        F.col("Att_MediCarepart.FACETS_Language").alias("FACETS_Language"),
        F.col("Att_MediCarepart.FACETS_PCPID").alias("FACETS_PCPID"),
        F.col("Att_MediCarepart.FACETS_PrimaryCarePhysician").alias("FACETS_PrimaryCarePhysician"),
        F.col("Att_MediCarepart.FACETS_SalesRepID").alias("FACETS_SalesRepID"),
        F.col("Att_MediCarepart.FACETS_SalesRepName").alias("FACETS_SalesRepName"),
        F.col("Att_MediCarepart.FACETS_MailingAddress1").alias("FACETS_MailingAddress1"),
        F.col("Att_MediCarepart.FACETS_MailingAddress2").alias("FACETS_MailingAddress2"),
        F.col("Att_MediCarepart.FACETS_MailingCity").alias("FACETS_MailingCity"),
        F.col("Att_MediCarepart.FACETS_MailingState").alias("FACETS_MailingState"),
        F.col("Att_MediCarepart.FACETS_MailingZip").alias("FACETS_MailingZip"),
        F.col("Att_MediCarepart.FACETS_GroupID").alias("FACETS_GroupID"),
        F.col("Att_MediCarepart.FACETS_SubGroupID").alias("FACETS_SubGroupID"),
        F.col("Att_MediCarepart.FACETS_ClassID").alias("FACETS_ClassID"),
        F.col("Att_MediCarepart.FACETS_MedicalProductID").alias("FACETS_MedicalProductID"),
        F.col("Att_MediCarepart.FACETS_PharmacyProductID").alias("FACETS_PharmacyProductID"),
        F.col("Att_MediCarepart.FACETS_BillingEntityID").alias("FACETS_BillingEntityID"),
        F.col("Att_MediCarepart.FACETS_StockID").alias("FACETS_StockID"),
        F.col("Att_MediCarepart.FACETS_HIC").alias("FACETS_HIC"),
        F.col("Att_MediCarepart.FACETS_MSPStartDate").alias("FACETS_MSPStartDate"),
        F.col("Att_MediCarepart.FACETS_MSPEndDate").alias("FACETS_MSPEndDate"),
        F.col("Att_MediCarepart.FACETS_WorkingAgedStartDate").alias("FACETS_WorkingAgedStartDate"),
        F.col("Att_MediCarepart.FACETS_WorkingAgedEndDate").alias("FACETS_WorkingAgedEndDate"),
        F.col("Att_MediCarepart.FACETS_Signature_Date").alias("FACETS_Signature_Date"),
        F.col("Att_MediCarepart.FACETS_PBPID").alias("FACETS_PBPID"),
        F.col("Att_MediCarepart.FACETS_Election_Type").alias("FACETS_Election_Type"),
        F.col("Att_MediCarepart.FACETS_Employer_Subsidy_Enrollment_Override").alias("FACETS_Employer_Subsidy_Enrollment_Override"),
        F.col("Att_MediCarepart.FACETS_Employer_Subsidy_Enrollment_Override2").alias("FACETS_Employer_Subsidy_Enrollment_Override2"),
        F.col("Att_MediCarepart.FACETS_DOD").alias("FACETS_DOD"),
        F.col("Att_MediCarepart.FACETS_CreditCover").alias("FACETS_CreditCover"),
        F.col("Att_MediCarepart.FACETS_PrimaryRXID").alias("FACETS_PrimaryRXID"),
        F.col("Att_MediCarepart.FACETS_EGHP").alias("FACETS_EGHP"),
        F.col("Att_MediCarepart.FACETS_MemberMCaid").alias("FACETS_MemberMCaid"),
        F.col("Att_MediCarepart.FACETS_MemberMCaid2").alias("FACETS_MemberMCaid2"),
        F.col("Att_MediCarepart.FACETS_HospStartDate").alias("FACETS_HospStartDate"),
        F.col("Att_MediCarepart.FACETS_HospEndDate").alias("FACETS_HospEndDate"),
        F.col("Att_MediCarepart.FACETS_UncoveredMonths").alias("FACETS_UncoveredMonths"),
        F.col("Att_MediCarepart.FACETS_PenaltyAmount").alias("FACETS_PenaltyAmount"),
        F.col("Att_MediCarepart.FACETS_SubsidyLevel").alias("FACETS_SubsidyLevel"),
        F.col("Att_MediCarepart.FACETS_Subsidy_Level_Start_Date").alias("FACETS_Subsidy_Level_Start_Date"),
        F.col("Att_MediCarepart.FACETS_Subsidy_Level_End_Date").alias("FACETS_Subsidy_Level_End_Date"),
        F.col("Att_MediCarepart.FACETS_CoPayCategory").alias("FACETS_CoPayCategory"),
        F.col("Att_MediCarepart.FACETS_SubsidyAmount").alias("FACETS_SubsidyAmount"),
        F.col("Att_MediCarepart.FACETS_CreditCover_TERM").alias("FACETS_CreditCover_TERM"),
        F.col("Att_MediCarepart.FACETS_ESRD").alias("FACETS_ESRD"),
        F.col("Att_MediCarepart.FACETS_PremWithholdOption").alias("FACETS_PremWithholdOption"),
        F.col("Att_MediCarepart.FACETS_LateEffectiveDate").alias("FACETS_LateEffectiveDate"),
        F.col("Ref_PRTA.FACETS_MedicarePartA").alias("FACETS_MedicarePartA"),
        F.col("Ref_PRTB.FACETS_MedicarePartB").alias("FACETS_MedicarePartB"),
        F.col("Ref_PRTD.FACETS_MedicarePartD").alias("FACETS_MedicarePartD"),
        F.col("Ref_PRTD.FACETS_PRTDT").alias("FACETS_PRTDT"),
        F.col("Ref_PRTD.FACETS_EnrollSource").alias("FACETS_EnrollSource"),
        F.col("Att_MediCarepart.FACETS_DentalProductID").alias("FACETS_DentalProductID")
    )
)

# Xfm2 (CTransformerStage) => df_XmrO_RmDup
df_XmrO_RmDup = df_FinalXfm.select(
    F.col("MEME_CK").alias("MEME_CK"),
    F.col("FACETS_MemberID").alias("FACETS_MemberID"),
    F.col("FACETS_FirstName").alias("FACETS_FirstName"),
    F.col("FACETS_LastName").alias("FACETS_LastName"),
    F.col("FACETS_PlanID").alias("FACETS_PlanID"),
    F.col("FACETS_MI").alias("FACETS_MI"),
    F.col("FACETS_Address1").alias("FACETS_Address1"),
    F.col("FACETS_Address2").alias("FACETS_Address2"),
    F.col("FACETS_City").alias("FACETS_City"),
    F.col("FACETS_State").alias("FACETS_State"),
    F.col("FACETS_Zip").alias("FACETS_Zip"),
    F.col("FACETS_SCCCode").alias("FACETS_SCCCode"),
    F.col("FACETS_MemberPhone").alias("FACETS_MemberPhone"),
    F.col("FACETS_MemberEmailAddress").alias("FACETS_MemberEmailAddress"),
    F.col("FACETS_Effective_Date").alias("FACETS_Effective_Date"),
    F.col("FACETS_DOB").alias("FACETS_DOB"),
    F.col("FACETS_SEX").alias("FACETS_SEX"),
    F.col("FACETS_SSN").alias("FACETS_SSN"),
    F.col("FACETS_Medicare_Number").alias("FACETS_Medicare_Number"),
    F.col("FACETS_MedicaidNumber").alias("FACETS_MedicaidNumber"),
    F.col("FACETS_Language").alias("FACETS_Language"),
    F.col("FACETS_PCPID").alias("FACETS_PCPID"),
    F.col("FACETS_PrimaryCarePhysician").alias("FACETS_PrimaryCarePhysician"),
    F.col("FACETS_SalesRepID").alias("FACETS_SalesRepID"),
    F.col("FACETS_SalesRepName").alias("FACETS_SalesRepName"),
    F.col("FACETS_MailingAddress1").alias("FACETS_MailingAddress1"),
    F.col("FACETS_MailingAddress2").alias("FACETS_MailingAddress2"),
    F.col("FACETS_MailingCity").alias("FACETS_MailingCity"),
    F.col("FACETS_MailingState").alias("FACETS_MailingState"),
    F.col("FACETS_MailingZip").alias("FACETS_MailingZip"),
    F.col("FACETS_GroupID").alias("FACETS_GroupID"),
    F.col("FACETS_SubGroupID").alias("FACETS_SubGroupID"),
    F.col("FACETS_ClassID").alias("FACETS_ClassID"),
    F.col("FACETS_MedicalProductID").alias("FACETS_MedicalProductID"),
    F.col("FACETS_PharmacyProductID").alias("FACETS_PharmacyProductID"),
    F.col("FACETS_BillingEntityID").alias("FACETS_BillingEntityID"),
    F.col("FACETS_StockID").alias("FACETS_StockID"),
    F.col("FACETS_HIC").alias("FACETS_HIC"),
    F.col("FACETS_MSPStartDate").alias("FACETS_MSPStartDate"),
    F.col("FACETS_MSPEndDate").alias("FACETS_MSPEndDate"),
    F.col("FACETS_WorkingAgedStartDate").alias("FACETS_WorkingAgedStartDate"),
    F.col("FACETS_WorkingAgedEndDate").alias("FACETS_WorkingAgedEndDate"),
    F.col("FACETS_Signature_Date").alias("FACETS_Signature_Date"),
    F.col("FACETS_PBPID").alias("FACETS_PBPID"),
    F.col("FACETS_Election_Type").alias("FACETS_Election_Type"),
    F.col("FACETS_Employer_Subsidy_Enrollment_Override").alias("FACETS_Employer_Subsidy_Enrollment_Override"),
    F.col("FACETS_Employer_Subsidy_Enrollment_Override2").alias("FACETS_Employer_Subsidy_Enrollment_Override2"),
    F.col("FACETS_DOD").alias("FACETS_DOD"),
    F.col("FACETS_CreditCover").alias("FACETS_CreditCover"),
    F.col("FACETS_PrimaryRXID").alias("FACETS_PrimaryRXID"),
    F.lit("").alias("FACETS_SecondaryRxInsurFlag"),
    F.lit("").alias("FACETS_SecondaryRxGroup"),
    F.lit("").alias("FACETS_SecondaryRxID"),
    F.lit("").alias("FACETS_SecondaryRXBIN"),
    F.lit("").alias("FACETS_SecondaryRXPCN"),
    F.col("FACETS_EGHP").alias("FACETS_EGHP"),
    F.col("FACETS_MemberMCaid").alias("FACETS_MemberMCaid"),
    F.col("FACETS_MemberMCaid2").alias("FACETS_MemberMCaid2"),
    F.col("FACETS_HospStartDate").alias("FACETS_HospStartDate"),
    F.col("FACETS_HospEndDate").alias("FACETS_HospEndDate"),
    F.col("FACETS_UncoveredMonths").alias("FACETS_UncoveredMonths"),
    F.when(
       (F.col("FACETS_PenaltyAmount").isNull()) | (F.col("FACETS_PenaltyAmount")==""), 
       "0.00"
    ).otherwise(F.col("FACETS_PenaltyAmount")).alias("FACETS_PenaltyAmount"),
    F.col("FACETS_SubsidyLevel").alias("FACETS_SubsidyLevel"),
    F.col("FACETS_Subsidy_Level_Start_Date").alias("FACETS_Subsidy_Level_Start_Date"),
    F.col("FACETS_Subsidy_Level_End_Date").alias("FACETS_Subsidy_Level_End_Date"),
    F.col("FACETS_CoPayCategory").alias("FACETS_CoPayCategory"),
    F.when(
       (F.col("FACETS_SubsidyAmount").isNull()) | (F.col("FACETS_SubsidyAmount")==""), 
       "0.00"
    ).otherwise(F.col("FACETS_SubsidyAmount")).alias("FACETS_SubsidyAmount"),
    F.col("FACETS_CreditCover_TERM").alias("FACETS_CreditCover_TERM"),
    F.col("FACETS_ESRD").alias("FACETS_ESRD"),
    F.col("FACETS_PremWithholdOption").alias("FACETS_PremWithholdOption"),
    F.when(
       (F.col("FACETS_MedicarePartA").isNull()) | (F.col("FACETS_MedicarePartA")==""), 
       F.col("FACETS_PRTDT")
    ).otherwise(F.col("FACETS_MedicarePartA")).alias("FACETS_MedicarePartA"),
    F.when(
       (F.col("FACETS_MedicarePartB").isNull()) | (F.col("FACETS_MedicarePartB")==""), 
       F.col("FACETS_PRTDT")
    ).otherwise(F.col("FACETS_MedicarePartB")).alias("FACETS_MedicarePartB"),
    F.col("FACETS_MedicarePartD").alias("FACETS_MedicarePartD"),
    F.col("FACETS_EnrollSource").alias("FACETS_EnrollSource"),
    F.col("FACETS_LateEffectiveDate").alias("FACETS_LateEffectiveDate"),
    F.col("FACETS_DentalProductID").alias("FACETS_DentalProductID")
)

# RmDup (PxRemDup) => dedup by [FACETS_HIC, FACETS_MemberID, FACETS_GroupID, MEME_CK]
df_File = dedup_sort(
    df_XmrO_RmDup,
    ["FACETS_HIC","FACETS_MemberID","FACETS_GroupID","MEME_CK"],
    []
)

# Final column order for writing
final_cols = [
    "MEME_CK", "FACETS_MemberID", "FACETS_FirstName", "FACETS_LastName", "FACETS_PlanID",
    "FACETS_MI", "FACETS_Address1", "FACETS_Address2", "FACETS_City", "FACETS_State",
    "FACETS_Zip", "FACETS_SCCCode", "FACETS_MemberPhone", "FACETS_MemberEmailAddress",
    "FACETS_Effective_Date", "FACETS_DOB", "FACETS_SEX", "FACETS_SSN", "FACETS_Medicare_Number",
    "FACETS_MedicaidNumber", "FACETS_Language", "FACETS_PCPID", "FACETS_PrimaryCarePhysician",
    "FACETS_SalesRepID", "FACETS_SalesRepName", "FACETS_MailingAddress1", "FACETS_MailingAddress2",
    "FACETS_MailingCity", "FACETS_MailingState", "FACETS_MailingZip", "FACETS_GroupID",
    "FACETS_SubGroupID", "FACETS_ClassID", "FACETS_MedicalProductID", "FACETS_PharmacyProductID",
    "FACETS_BillingEntityID", "FACETS_StockID", "FACETS_HIC", "FACETS_MSPStartDate",
    "FACETS_MSPEndDate", "FACETS_WorkingAgedStartDate", "FACETS_WorkingAgedEndDate",
    "FACETS_Signature_Date", "FACETS_PBPID", "FACETS_Election_Type", "FACETS_Employer_Subsidy_Enrollment_Override",
    "FACETS_Employer_Subsidy_Enrollment_Override2", "FACETS_DOD", "FACETS_CreditCover",
    "FACETS_PrimaryRXID", "FACETS_SecondaryRxInsurFlag", "FACETS_SecondaryRxGroup",
    "FACETS_SecondaryRxID", "FACETS_SecondaryRXBIN", "FACETS_SecondaryRXPCN", "FACETS_EGHP",
    "FACETS_MemberMCaid", "FACETS_MemberMCaid2", "FACETS_HospStartDate", "FACETS_HospEndDate",
    "FACETS_UncoveredMonths", "FACETS_PenaltyAmount", "FACETS_SubsidyLevel", "FACETS_Subsidy_Level_Start_Date",
    "FACETS_Subsidy_Level_End_Date", "FACETS_CoPayCategory", "FACETS_SubsidyAmount",
    "FACETS_CreditCover_TERM", "FACETS_ESRD", "FACETS_PremWithholdOption", "FACETS_MedicarePartA",
    "FACETS_MedicarePartB", "FACETS_MedicarePartD", "FACETS_EnrollSource", "FACETS_LateEffectiveDate",
    "FACETS_DentalProductID"
]

df_final = df_File.select([F.col(c) for c in final_cols])

# Apply rpad for char/varchar columns as per lengths in the final job metadata
# Below lengths are gathered from the specification where "SqlType": "char","Length": ...
df_final_padded = df_final \
.withColumn("FACETS_MemberID", F.rpad(F.col("FACETS_MemberID"), 9, " ")) \
.withColumn("FACETS_FirstName", F.rpad(F.col("FACETS_FirstName"), 15, " ")) \
.withColumn("FACETS_LastName", F.rpad(F.col("FACETS_LastName"), 35, " ")) \
.withColumn("FACETS_PlanID", F.rpad(F.col("FACETS_PlanID"), 8, " ")) \
.withColumn("FACETS_MI", F.rpad(F.col("FACETS_MI"), 1, " ")) \
.withColumn("FACETS_Address1", F.rpad(F.col("FACETS_Address1"), 40, " ")) \
.withColumn("FACETS_Address2", F.rpad(F.col("FACETS_Address2"), 40, " ")) \
.withColumn("FACETS_City", F.rpad(F.col("FACETS_City"), 19, " ")) \
.withColumn("FACETS_State", F.rpad(F.col("FACETS_State"), 2, " ")) \
.withColumn("FACETS_Zip", F.rpad(F.col("FACETS_Zip"), 11, " ")) \
.withColumn("FACETS_SCCCode", F.rpad(F.col("FACETS_SCCCode"), 8, " ")) \
.withColumn("FACETS_MemberPhone", F.rpad(F.col("FACETS_MemberPhone"), 20, " ")) \
.withColumn("FACETS_MemberEmailAddress", F.rpad(F.col("FACETS_MemberEmailAddress"), 40, " ")) \
.withColumn("FACETS_SEX", F.rpad(F.col("FACETS_SEX"), 1, " ")) \
.withColumn("FACETS_SSN", F.rpad(F.col("FACETS_SSN"), 9, " ")) \
.withColumn("FACETS_Medicare_Number", F.rpad(F.col("FACETS_Medicare_Number"), 12, " ")) \
.withColumn("FACETS_MedicaidNumber", F.rpad(F.col("FACETS_MedicaidNumber"), 20, " ")) \
.withColumn("FACETS_Language", F.rpad(F.col("FACETS_Language"), 4, " ")) \
.withColumn("FACETS_PCPID", F.rpad(F.col("FACETS_PCPID"), 12, " ")) \
.withColumn("FACETS_PrimaryCarePhysician", F.rpad(F.col("FACETS_PrimaryCarePhysician"), 55, " ")) \
.withColumn("FACETS_SalesRepID", F.rpad(F.col("FACETS_SalesRepID"), 9, " ")) \
.withColumn("FACETS_SalesRepName", F.rpad(F.col("FACETS_SalesRepName"), 50, " ")) \
.withColumn("FACETS_MailingAddress1", F.rpad(F.col("FACETS_MailingAddress1"), 40, " ")) \
.withColumn("FACETS_MailingAddress2", F.rpad(F.col("FACETS_MailingAddress2"), 40, " ")) \
.withColumn("FACETS_MailingCity", F.rpad(F.col("FACETS_MailingCity"), 19, " ")) \
.withColumn("FACETS_MailingState", F.rpad(F.col("FACETS_MailingState"), 2, " ")) \
.withColumn("FACETS_MailingZip", F.rpad(F.col("FACETS_MailingZip"), 11, " ")) \
.withColumn("FACETS_GroupID", F.rpad(F.col("FACETS_GroupID"), 8, " ")) \
.withColumn("FACETS_SubGroupID", F.rpad(F.col("FACETS_SubGroupID"), 4, " ")) \
.withColumn("FACETS_ClassID", F.rpad(F.col("FACETS_ClassID"), 4, " ")) \
.withColumn("FACETS_MedicalProductID", F.rpad(F.col("FACETS_MedicalProductID"), 8, " ")) \
.withColumn("FACETS_PharmacyProductID", F.rpad(F.col("FACETS_PharmacyProductID"), 8, " ")) \
.withColumn("FACETS_SubsidyLevel", F.rpad(F.col("FACETS_SubsidyLevel"), 3, " ")) \
.withColumn("FACETS_CoPayCategory", F.rpad(F.col("FACETS_CoPayCategory"), 1, " ")) \
.withColumn("FACETS_CreditCover_TERM", F.rpad(F.col("FACETS_CreditCover_TERM"), 4, " ")) \
.withColumn("FACETS_ESRD", F.rpad(F.col("FACETS_ESRD"), 4, " ")) \
.withColumn("FACETS_PremWithholdOption", F.rpad(F.col("FACETS_PremWithholdOption"), 1, " ")) \
.withColumn("FACETS_MedicarePartA", F.rpad(F.col("FACETS_MedicarePartA"), 10, " ")) \
.withColumn("FACETS_MedicarePartB", F.rpad(F.col("FACETS_MedicarePartB"), 10, " ")) \
.withColumn("FACETS_MedicarePartD", F.rpad(F.col("FACETS_MedicarePartD"), 10, " "))

# Load_File (PxSequentialFile)
write_files(
    df_final_padded,
    f"{adls_path}/load/FACETS_EAM_LOAD.{RUNID}.dat",
    delimiter="|",
    mode="overwrite",
    is_parquet=False,
    header=True,
    quote="\"",
    nullValue=None
)