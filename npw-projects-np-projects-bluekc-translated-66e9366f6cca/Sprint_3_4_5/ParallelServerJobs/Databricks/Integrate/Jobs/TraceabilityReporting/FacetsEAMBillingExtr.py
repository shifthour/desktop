# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Job Name: FacetsEAMBillingExtr
# MAGIC Calling Job:EAMFctsReconRptCntl
# MAGIC 
# MAGIC Description:This Job extracts as attributes for Billing Date from Facets and EAM, compares the attributes and write the mismatches to a file.
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC ====================================================================================================================================================================================
# MAGIC Developer                          Date                           Project/Altiris #                              Change Description                                                    Development Project                 Reviewer                            Reviewed On
# MAGIC ====================================================================================================================================================================================
# MAGIC ReddySanam              2021-02-02                  US329820                                         Original Programming                                                    IntegrateDev2                      
# MAGIC ReddySanam              2021-02-10                   US329820                                         Added logic to add                                                      IntegrateDev2                      Jeyaprasanna                        2/10/2021
# MAGIC                                                                                                                                   Missing Eligibility in 
# MAGIC                                                                                                                                   EAM/FACETS by
# MAGIC                                                                                                                                   changing join to Full Outer
# MAGIC Prabhu ES                    2022-03-21                  S2S                                                   MSSQL ODBC conn params added                             IntegrateDev5	Ken Bradmon	2022-05-20
# MAGIC                                                                            
# MAGIC Vamsi Aripaka              2024-04-18                 US 616210                                         Added GRP_ID parameter                                            IntegrateDev2                    Jeyaprasanna                         2024-06-20
# MAGIC 
# MAGIC Deepika C                   2024-05-23                  US 612400                                       Updated query to extract PBPID, PBP EffectiveDate     IntegrateDev2                    Jeyaprasanna                      2024-08-02
# MAGIC                                                                                                                                  from tbTransactions table in EAM_BILL_Attr stage
# MAGIC 
# MAGIC Arpitha V                     2024-09-24                  US 629784                                  Added DateCreated column to retrieve unique and recent    IntegrateDev2                   Jeyaprasanna                     2024-09-24
# MAGIC                                                                                                                            PBPID from tbTransactions table in EAM_BILL_Attr stage
# MAGIC 
# MAGIC Arpitha V                     2024-10-19                  US 631376                                 Added TransStatus = 5 filter condition to retrieve data which   IntegrateDev2                 Jeyaprasanna                    2024-10-23
# MAGIC                                                                                                                             has 'Accepted by CMS' status from tbTransactions table in 
# MAGIC                                                                                                                                                     EAM_BILL_Attr stage

# MAGIC This Job extracts as attributes for Billing Date from Facets and EAM, compares the attributes and write the mismatches to a file.
# MAGIC In this transformer, each record is checked for all the attributes that need to be compared with 1 loop for each attribute
# MAGIC 
# MAGIC If a MBI does not have eligibility in EAM/FACETS only one record corresponding to MBI mismatch is sent formard
# MAGIC This part extracts the translation for PrimaryRXGrp, BIN and PCN values from MCTR
# MAGIC Extracting data from MEMD table. From each Event Code Type table, record corresponding to BillDt falling in between effective and Term dates is used
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, StructType, StructField
# COMMAND ----------
# MAGIC %run ../../../Utility_Integrate
# COMMAND ----------


FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
EAMOwner = get_widget_value('EAMOwner','')
eam_secret_name = get_widget_value('eam_secret_name','')
RUNID = get_widget_value('RUNID','')
CurrentDate = get_widget_value('CurrentDate','')
BillDt = get_widget_value('BillDt','')
GRP_ID = get_widget_value('GRP_ID','')

jdbc_url_eam, jdbc_props_eam = get_db_config(eam_secret_name)
query_EAM_BILL_Attr = f"""
select LTRIM(RTRIM(ISNULL(Groups.GRGR_ID ,''))) as EAM_GroupID
  , LTRIM(RTRIM(ISNULL(mbrs.MemberID,'')))  AS EAM_MemberID
  , LTRIM(RTRIM(ISNULL(mbrs.HIC,''))) AS MBI
  , LTRIM(RTRIM(ISNULL(eff.PlanID,''))) AS EAM_PlanID
  , LTRIM(RTRIM(ISNULL(info.MedicalProductID,''))) AS EAM_MedicalProductID
  , LTRIM(RTRIM(ISNULL(scc.Value,'')))  as EAM_SCCCode
  , LTRIM(RTRIM(ISNULL(info.DentalProductID,''))) as EAM_DentalProductID
  ,pbp.PBPID AS EAM_PBPID
  ,pbp.EffectiveDate as EAM_PBP_EFF_DT
  , LTRIM(RTRIM(ISNULL(info.RxID,''))) AS EAM_PrimaryRXID
  , LTRIM(RTRIM(ISNULL(info.RxGroupID,''))) AS EAM_PrimaryRXGroup
  , LTRIM(RTRIM(ISNULL(info.RxBIN,''))) AS EAM_PrimaryRXBin
  , LTRIM(RTRIM(ISNULL(info.RxPCN,''))) AS EAM_PrimaryRXPCN
  , ISNULL(CONVERT(VARCHAR(10),info.PartDEff,101),'') as EAM_PrimaryRX_EFF_DT
  ,LTRIM(RTRIM(ISNULL(INFO.PartDLateEnrollmentPenaltyAmount,''))) AS EAM_PenaltyAmount
  ,LTRIM(RTRIM(ISNULL(LIS.SubsidyLevel,'000'))) AS EAM_SubsidyLevel
  ,LIS.[Subsidy Level Start Date] AS EAM_Subsidy_Level_Start_Date
  , LTRIM(RTRIM(ISNULL(PW.PWOPTION,''))) AS EAM_PremWithholdOption
  from {EAMOwner}.tbEENRLMembers mbrs
  INNER JOIN {EAMOwner}.tbMemberInfo info ON info.MemCodNum = mbrs.MemCodNum
  INNER JOIN {EAMOwner}.Groups ON Groups.GroupID = info.GroupID
  INNER JOIN {EAMOwner}.tbENRLSpans eff on eff.MemCodNum= mbrs.MemCodNum AND eff.SpanType='EFF'
 LEFT OUTER JOIN
 ( 
   select 
     MemCodNum,
     TransStatus,
     CONVERT(VARCHAR(10), EffectiveDate, 101) AS EffectiveDate,
     CONVERT(VARCHAR(10), DateCreated, 101) AS DateCreated,
     PBPID,
     dense_rank() over (partition by MemCodNum order by EffectiveDate desc,DateCreated desc) rnk
   from {EAMOwner}.tbTransactions 
   where TransCode = '61' or (Transcode = '80' and ReplyCodes = '287')
   group by MemCodNum,TransStatus,EffectiveDate,DateCreated,PBPID
 ) pbp
 on pbp.MemCodNum = mbrs.MemCodNum and pbp.rnk = 1
 LEFT OUTER JOIN {EAMOwner}.tbENRLSpans prtd on prtd.MemCodNum= mbrs.MemCodNum AND prtd.SpanType='EFFD' 
   AND '{BillDt}' BETWEEN prtd.StartDate AND prtd.EndDate
 LEFT OUTER JOIN {EAMOwner}.tbENRLSpans scc on scc.MemCodNum= mbrs.MemCodNum AND scc.SpanType='SCC' 
   AND '{BillDt}' BETWEEN scc.StartDate AND scc.EndDate
 LEFT OUTER JOIN {EAMOwner}.TBPREMIUMWITHOLDOPTIONS PW   ON Info.PWOPTION = PW.PWOID
 LEFT OUTER JOIN {EAMOwner}.TBENRLSPANS AS LEP
   ON LEP.MEMCODNUM = info.MemCodNum
   AND LEP.SPANTYPE = 'LEP'
   AND '{BillDt}' BETWEEN LEP.StartDate and LEP.EndDate 
 OUTER APPLY
 ( 
   SELECT
     LIS.MemCodNum
     , PartDSubsLevel AS SubsidyLevel
     , LowIncCoPayCat AS CopayCat
     , Valid
     , LowIncomePartDPremiumSubsidyAmount
     , CONVERT(VARCHAR(10), LowIncomeCoPayEffectiveDate, 101) AS [Subsidy Level Start Date]
     , CONVERT(VARCHAR(10), LowIncomeCoPayEndDate, 101) AS [Subsidy Level End Date]
     , ROW_NUMBER () OVER (PARTITION BY LIS.MemCodNum ORDER BY [LowIncomeCoPayEffectiveDate] DESC) AS RN
   FROM {EAMOwner}.tbMemberInfoLISLog AS LIS
   WHERE LIS.MemCodNum = info.MemCodNum
     AND Valid=1
     AND '{BillDt}' BETWEEN  LowIncomeCoPayEffectiveDate and LowIncomeCoPayEndDate
 ) LIS
  WHERE '{BillDt}' BETWEEN eff.StartDate AND eff.EndDate
    and LTRIM(RTRIM(ISNULL(Groups.GRGR_ID ,''))) in ({GRP_ID})
    and pbp.TransStatus = 5
"""
df_EAM_BILL_Attr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_eam)
    .options(**jdbc_props_eam)
    .option("query", query_EAM_BILL_Attr)
    .load()
)

df_MAP_EAM = df_EAM_BILL_Attr.select(
    F.col("EAM_GroupID").alias("EAM_GroupID"),
    F.col("EAM_MemberID").alias("EAM_MemberID"),
    F.col("MBI").alias("MBI"),
    F.col("EAM_PlanID").alias("EAM_PlanID"),
    F.col("EAM_MedicalProductID").alias("EAM_MedicalProductID"),
    F.col("EAM_SCCCode").alias("EAM_SCCCode"),
    F.col("EAM_DentalProductID").alias("EAM_DentalProductID"),
    F.col("EAM_PBPID").alias("EAM_PBPID"),
    F.col("EAM_PBP_EFF_DT").alias("EAM_PBP_EFF_DT"),
    F.col("EAM_PrimaryRXID").alias("EAM_PrimaryRXID"),
    F.col("EAM_PrimaryRXGroup").alias("EAM_PrimaryRxGroup"),
    F.col("EAM_PrimaryRXBin").alias("EAM_PrimaryRXBIN"),
    F.col("EAM_PrimaryRXPCN").alias("EAM_PrimaryRXPCN"),
    F.col("EAM_PrimaryRX_EFF_DT").alias("EAM_PrimaryRX_EFF_DT"),
    F.col("EAM_PenaltyAmount").alias("EAM_PenaltyAmount"),
    F.col("EAM_SubsidyLevel").alias("EAM_SubsidyLevel"),
    F.col("EAM_Subsidy_Level_Start_Date").alias("EAM_Subsidy_Level_Start_Date"),
    F.col("EAM_PremWithholdOption").alias("EAM_PremWithholdOption")
)

jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
query_ESRD_PERM = f"""
select DISTINCT 
MEMD.MEME_CK,
LTRIM(RTRIM(ISNULL(MEMD.MEMD_PREM_WH_OPT ,''))) as FACETS_PremWithholdOption
 from  {FacetsOwner}.CMC_MEMD_MECR_DETL  MEMD   where MEMD.MEMD_EVENT_CD = 'PREM'               
    AND MEMD.MEMD_HCFA_EFF_DT = (Select MAX(MEMD_HCFA_EFF_DT) FROM {FacetsOwner}.CMC_MEMD_MECR_DETL MEMD1
                                    WHERE MEMD1.MEME_CK=MEMD.MEME_CK and MEMD1.MEMD_EVENT_CD=MEMD.MEMD_EVENT_CD)
"""
df_ESRD_PERM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", query_ESRD_PERM)
    .load()
)

query_MemberAttr = f"""
select 
   DISTINCT
    MEME.MEME_CK
   ,LTRIM(RTRIM(ISNULL(GRGR.GRGR_ID ,''))) as FACETS_GroupID
  , LTRIM(RTRIM(ISNULL(SBSB.SBSB_ID,''))) AS FACETS_MemberID
  , LTRIM(RTRIM(ISNULL(MEME.MEME_HICN,''))) AS FACETS_HIC
  , LTRIM(RTRIM(ISNULL(BGBG.BGBG_ID,''))) AS FACETS_PlanID
  , ISNULL(CONVERT(VARCHAR(10),MEPE_MED.MEPE_EFF_DT,101),'') AS FACETS_Effective_Date
  , LTRIM(RTRIM(ISNULL(MEPE_MED.CSPI_ID ,''))) as [FACETS_MedicalProductID]
  , LTRIM(RTRIM(ISNULL(SCCC.MEMD_MCTR_MCST ,'')))+ LTRIM(RTRIM(ISNULL(SCCC.MEMD_MCTR_MCCT ,''))) as FACETS_SCCCode
  , ISNULL(CONVERT(VARCHAR(10),DENT.MEPE_EFF_DT,101),'') AS Facets_DentalEffectiveDate
  , LTRIM(RTRIM(ISNULL(DENT.CSPI_ID,''))) as FACETS_DentalProductID
FROM {FacetsOwner}.CMC_SBSB_SUBSC SBSB
INNER JOIN {FacetsOwner}.CMC_GRGR_GROUP GRGR ON GRGR.GRGR_CK=SBSB.GRGR_CK
and GRGR.GRGR_ID IN ({GRP_ID})
INNER JOIN {FacetsOwner}.CMC_MEME_MEMBER MEME ON MEME.SBSB_CK=SBSB.SBSB_CK
INNER JOIN {FacetsOwner}.CMC_MEPE_PRCS_ELIG MEPE_MED ON MEPE_MED.MEME_CK=MEME.MEME_CK AND MEPE_MED.CSPD_CAT='M' AND MEPE_MED.MEPE_ELIG_IND='Y'
AND '{BillDt}'  BETWEEN MEPE_MED.MEPE_EFF_DT AND MEPE_MED.MEPE_TERM_DT
LEFT OUTER JOIN {FacetsOwner}.CMC_MEPE_PRCS_ELIG DENT ON DENT.MEME_CK=MEME.MEME_CK AND DENT.CSPD_CAT='D' AND DENT.MEPE_ELIG_IND='Y'
 AND '{BillDt}'  BETWEEN DENT.MEPE_EFF_DT AND DENT.MEPE_TERM_DT
LEFT OUTER JOIN {FacetsOwner}.CMC_MEMD_MECR_DETL SCCC ON SCCC.MEME_CK=MEME.MEME_CK
            and SCCC.MEMD_EVENT_CD='SCCC'
            and '{BillDt}' BETWEEN SCCC.MEMD_HCFA_EFF_DT AND  SCCC.MEMD_HCFA_TERM_DT
LEFT OUTER JOIN {FacetsOwner}.CMC_BGBG_BIL_GROUP BGBG ON BGBG.BGBG_CK = SCCC.BGBG_CK
"""
df_MemberAttr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", query_MemberAttr)
    .load()
)

df_Xmr = df_MemberAttr.select(
    F.col("MEME_CK").alias("MEME_CK"),
    F.col("FACETS_GroupID").alias("FACETS_GroupID"),
    F.col("FACETS_MemberID").alias("FACETS_MemberID"),
    F.col("FACETS_HIC").alias("MBI"),
    F.col("FACETS_PlanID").alias("FACETS_PlanID"),
    F.col("FACETS_Effective_Date").alias("FACETS_Effective_Date"),
    F.col("FACETS_MedicalProductID").alias("FACETS_MedicalProductID"),
    F.col("FACETS_SCCCode").alias("FACETS_SCCCode"),
    F.col("Facets_DentalEffectiveDate").alias("Facets_DentalEffectiveDate"),
    F.col("FACETS_DentalProductID").alias("FACETS_DentalProductID")
)

query_MEMD_BillDt = f"""
select 
  DISTINCT
    MEMD.MEME_CK
  , MEMD_EVENT_CD
  , LTRIM(RTRIM(ISNULL(MEMD.MEMD_MCTR_PBP ,''))) as FACETS_PBPID
  , CASE WHEN MEMD_EVENT_CD = 'PBP' then ISNULL(CONVERT(VARCHAR(10),MEMD.MEMD_HCFA_EFF_DT,101),'')  ELSE '' END AS FACETS_PBP_EFF_DT
  , LTRIM(RTRIM(ISNULL(MEMD.MEMD_RX_ID ,''))) as FACETS_PrimaryRXID
  , LTRIM(RTRIM(ISNULL(MEMD.MEMD_MCTR_RX_GROUP ,''))) as FACETS_PrimaryRXGroup
  , LTRIM(RTRIM(ISNULL(MEMD.MEMD_MCTR_RXBIN ,''))) as FACETS_PrimaryRXBin
  , LTRIM(RTRIM(ISNULL(MEMD.MEMD_MCTR_RXPCN ,''))) as FACETS_PrimaryRXPCN
  , LTRIM(RTRIM(ISNULL(MEMD.MEMD_LATE_PENALTY,0))) AS FACETS_PenaltyAmount
  , LTRIM(RTRIM(ISNULL(MEMD.MEMD_PARTD_SBSDY,'000'))) AS FACETS_SubsidyLevel
  , CASE WHEN MEMD_EVENT_CD = 'LICS' then ISNULL(CONVERT(VARCHAR(10),MEMD.MEMD_HCFA_EFF_DT,101),'')  ELSE '' END AS FACETS_Subsidy_Level_Start_Date
FROM 
{FacetsOwner}.CMC_MEMD_MECR_DETL  AS MEMD
     where    MEMD.MEMD_EVENT_CD in ( 'PBP','LATE','LICS','PRTD')  
     AND '{BillDt}' BETWEEN MEMD.MEMD_HCFA_EFF_DT AND MEMD.MEMD_HCFA_TERM_DT
"""
df_MEMD_BillDt = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", query_MEMD_BillDt)
    .load()
)

df_Xmr3_PRTD = df_MEMD_BillDt.filter(F.col("MEMD_EVENT_CD") == "PRTD").select(
    F.col("MEME_CK").alias("MEME_CK"),
    F.col("FACETS_PrimaryRXID").alias("FACETS_PrimaryRXID"),
    F.col("FACETS_PrimaryRXGroup").alias("FACETS_PrimaryRxGroup"),
    F.col("FACETS_PrimaryRXBin").alias("FACETS_PrimaryRXBIN"),
    F.col("FACETS_PrimaryRXPCN").alias("FACETS_PrimaryRXPCN"),
    F.lit("").alias("FACETS_PrimaryRX_EFF_DT")
)

df_Xmr3_Ref_LATE = df_MEMD_BillDt.filter(F.col("MEMD_EVENT_CD") == "LATE").select(
    F.col("MEME_CK").alias("MEME_CK"),
    F.col("FACETS_PenaltyAmount").alias("FACETS_PenaltyAmount")
)

df_Xmr3_Ref_PBP = df_MEMD_BillDt.filter(F.trim(F.col("MEMD_EVENT_CD")) == "PBP").select(
    F.col("MEME_CK").alias("MEME_CK"),
    F.col("FACETS_PBPID").alias("FACETS_PBPID"),
    F.col("FACETS_PBP_EFF_DT").alias("FACETS_PBP_EFF_DT")
)

df_Xmr3_Ref_LICS = df_MEMD_BillDt.filter(F.col("MEMD_EVENT_CD") == "LICS").select(
    F.col("MEME_CK").alias("MEME_CK"),
    F.col("FACETS_SubsidyLevel").alias("FACETS_SubsidyLevel"),
    F.col("FACETS_Subsidy_Level_Start_Date").alias("FACETS_Subsidy_Level_Start_Date")
)

query_MCTR = f"""
select DISTINCT MCTR_TYPE,MCTR_VALUE,MCTR_DESC from {FacetsOwner}.CMC_MCTR_CD_TRANS
WHERE MCTR_TYPE IN ( 'PCN','GRP','BIN')
"""
df_MCTR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", query_MCTR)
    .load()
)

df_Xmr_MCTR_Ref_BIN = df_MCTR.filter(
    (F.trim(F.col("MCTR_TYPE")) == "BIN") & (F.trim(F.col("MCTR_VALUE")) != "")
).select(
    F.trim(F.col("MCTR_VALUE")).alias("MCTR_VALUE"),
    F.col("MCTR_DESC").alias("MCTR_DESC")
)

df_Xmr_MCTR_Ref_RXGrp = df_MCTR.filter(
    (F.trim(F.col("MCTR_TYPE")) == "GRP") & (F.trim(F.col("MCTR_VALUE")) != "")
).select(
    F.trim(F.col("MCTR_VALUE")).alias("MCTR_VALUE"),
    F.col("MCTR_DESC").alias("MCTR_DESC")
)

df_Xmr_MCTR_Ref_PCN = df_MCTR.filter(
    (F.trim(F.col("MCTR_TYPE")) == "PCN") & (F.trim(F.col("MCTR_VALUE")) != "")
).select(
    F.trim(F.col("MCTR_VALUE")).alias("MCTR_VALUE"),
    F.col("MCTR_DESC").alias("MCTR_DESC")
)

df_Att_PRTD_DESC = (
    df_Xmr3_PRTD.alias("PRTD")
    .join(df_Xmr_MCTR_Ref_RXGrp.alias("Ref_RXGrp"),
          F.col("PRTD.FACETS_PrimaryRxGroup") == F.col("Ref_RXGrp.MCTR_VALUE"), "left")
    .join(df_Xmr_MCTR_Ref_BIN.alias("Ref_BIN"),
          F.col("PRTD.FACETS_PrimaryRXBIN") == F.col("Ref_BIN.MCTR_VALUE"), "left")
    .join(df_Xmr_MCTR_Ref_PCN.alias("Ref_PCN"),
          F.col("PRTD.FACETS_PrimaryRXPCN") == F.col("Ref_PCN.MCTR_VALUE"), "left")
    .select(
        F.col("PRTD.MEME_CK").alias("MEME_CK"),
        F.col("PRTD.FACETS_PrimaryRXID").alias("FACETS_PrimaryRXID"),
        F.col("PRTD.FACETS_PrimaryRxGroup").alias("FACETS_PrimaryRxGroup"),
        F.col("PRTD.FACETS_PrimaryRXBIN").alias("FACETS_PrimaryRXBIN"),
        F.col("PRTD.FACETS_PrimaryRXPCN").alias("FACETS_PrimaryRXPCN"),
        F.col("PRTD.FACETS_PrimaryRX_EFF_DT").alias("FACETS_PrimaryRX_EFF_DT"),
        F.col("Ref_RXGrp.MCTR_DESC").alias("MCTR_DESC_Grp"),
        F.col("Ref_BIN.MCTR_DESC").alias("MCTR_DESC_Bin"),
        F.col("Ref_PCN.MCTR_DESC").alias("MCTR_DESC_Pcn")
    )
)

df_Att_MEMDAttr = (
    df_Xmr.alias("Enrich")
    .join(df_Xmr3_Ref_LATE.alias("Ref_LATE"),
          F.col("Enrich.MEME_CK") == F.col("Ref_LATE.MEME_CK"), "left")
    .join(df_Xmr3_Ref_LICS.alias("Ref_LICS"),
          F.col("Enrich.MEME_CK") == F.col("Ref_LICS.MEME_CK"), "left")
    .join(df_ESRD_PERM.alias("Ref_PREM"), F.lit(True), "left")
    .join(df_Xmr3_Ref_PBP.alias("Ref_PBP"),
          F.col("Enrich.MEME_CK") == F.col("Ref_PBP.MEME_CK"), "left")
    .join(df_Att_PRTD_DESC.alias("Ref_PRTD"),
          F.col("Enrich.MEME_CK") == F.col("Ref_PRTD.MEME_CK"), "left")
    .select(
        F.col("Enrich.MEME_CK").alias("MEME_CK"),
        F.col("Enrich.FACETS_GroupID").alias("FACETS_GroupID"),
        F.col("Enrich.FACETS_MemberID").alias("FACETS_MemberID"),
        F.col("Enrich.MBI").alias("MBI"),
        F.col("Enrich.FACETS_PlanID").alias("FACETS_PlanID"),
        F.col("Enrich.FACETS_Effective_Date").alias("FACETS_Effective_Date"),
        F.col("Enrich.FACETS_MedicalProductID").alias("FACETS_MedicalProductID"),
        F.col("Enrich.FACETS_SCCCode").alias("FACETS_SCCCode"),
        F.col("Enrich.Facets_DentalEffectiveDate").alias("Facets_DentalEffectiveDate"),
        F.col("Enrich.FACETS_DentalProductID").alias("FACETS_DentalProductID"),
        F.col("Ref_PBP.FACETS_PBPID").alias("FACETS_PBPID"),
        F.col("Ref_PBP.FACETS_PBP_EFF_DT").alias("FACETS_PBP_EFF_DT"),
        F.col("Ref_PRTD.FACETS_PrimaryRXID").alias("FACETS_PrimaryRXID"),
        F.col("Ref_PRTD.MCTR_DESC_Grp").alias("FACETS_PrimaryRxGroup"),
        F.col("Ref_PRTD.MCTR_DESC_Bin").alias("FACETS_PrimaryRXBIN"),
        F.col("Ref_PRTD.MCTR_DESC_Pcn").alias("FACETS_PrimaryRXPCN"),
        F.col("Ref_PRTD.FACETS_PrimaryRX_EFF_DT").alias("FACETS_PrimaryRX_EFF_DT"),
        F.col("Ref_LATE.FACETS_PenaltyAmount").alias("FACETS_PenaltyAmount"),
        F.col("Ref_LICS.FACETS_SubsidyLevel").alias("FACETS_SubsidyLevel"),
        F.col("Ref_LICS.FACETS_Subsidy_Level_Start_Date").alias("FACETS_Subsidy_Level_Start_Date"),
        F.col("Ref_PREM.FACETS_PremWithholdOption").alias("FACETS_PremWithholdOption")
    )
)

df_Jn = (
    df_Att_MEMDAttr.alias("FACETS")
    .join(df_MAP_EAM.alias("EAMdata"),
          F.col("FACETS.MBI") == F.col("EAMdata.MBI"), "fullouter")
    .select(
        F.col("FACETS.MEME_CK").alias("MEME_CK"),
        F.col("FACETS.FACETS_GroupID").alias("FACETS_GroupID"),
        F.col("FACETS.FACETS_MemberID").alias("FACETS_MemberID"),
        F.col("FACETS.MBI").alias("FACETS_MBI"),
        F.col("FACETS.FACETS_PlanID").alias("FACETS_PlanID"),
        F.col("FACETS.FACETS_MedicalProductID").alias("FACETS_MedicalProductID"),
        F.col("FACETS.FACETS_SCCCode").alias("FACETS_SCCCode"),
        F.col("FACETS.FACETS_DentalProductID").alias("FACETS_DentalProductID"),
        F.col("FACETS.FACETS_PBPID").alias("FACETS_PBPID"),
        F.col("FACETS.FACETS_PBP_EFF_DT").alias("FACETS_PBP_EFF_DT"),
        F.col("FACETS.FACETS_PrimaryRXID").alias("FACETS_PrimaryRXID"),
        F.col("FACETS.FACETS_PrimaryRxGroup").alias("FACETS_PrimaryRxGroup"),
        F.col("FACETS.FACETS_PrimaryRXBIN").alias("FACETS_PrimaryRXBIN"),
        F.col("FACETS.FACETS_PrimaryRXPCN").alias("FACETS_PrimaryRXPCN"),
        F.col("FACETS.FACETS_PrimaryRX_EFF_DT").alias("FACETS_PrimaryRX_EFF_DT"),
        F.col("FACETS.FACETS_PenaltyAmount").alias("FACETS_PenaltyAmount"),
        F.col("FACETS.FACETS_SubsidyLevel").alias("FACETS_SubsidyLevel"),
        F.col("FACETS.FACETS_Subsidy_Level_Start_Date").alias("FACETS_Subsidy_Level_Start_Date"),
        F.col("FACETS.FACETS_PremWithholdOption").alias("FACETS_PremWithholdOption"),
        F.col("EAMdata.EAM_GroupID").alias("EAM_GroupID"),
        F.col("EAMdata.EAM_MemberID").alias("EAM_MemberID"),
        F.col("EAMdata.MBI").alias("EAM_MBI"),
        F.col("EAMdata.EAM_PlanID").alias("EAM_PlanID"),
        F.col("EAMdata.EAM_MedicalProductID").alias("EAM_MedicalProductID"),
        F.col("EAMdata.EAM_SCCCode").alias("EAM_SCCCode"),
        F.col("EAMdata.EAM_DentalProductID").alias("EAM_DentalProductID"),
        F.col("EAMdata.EAM_PBPID").alias("EAM_PBPID"),
        F.col("EAMdata.EAM_PBP_EFF_DT").alias("EAM_PBP_EFF_DT"),
        F.col("EAMdata.EAM_PrimaryRXID").alias("EAM_PrimaryRXID"),
        F.col("EAMdata.EAM_PrimaryRxGroup").alias("EAM_PrimaryRxGroup"),
        F.col("EAMdata.EAM_PrimaryRXBIN").alias("EAM_PrimaryRXBIN"),
        F.col("EAMdata.EAM_PrimaryRXPCN").alias("EAM_PrimaryRXPCN"),
        F.col("EAMdata.EAM_PrimaryRX_EFF_DT").alias("EAM_PrimaryRX_EFF_DT"),
        F.col("EAMdata.EAM_PenaltyAmount").alias("EAM_PenaltyAmount"),
        F.col("EAMdata.EAM_SubsidyLevel").alias("EAM_SubsidyLevel"),
        F.col("EAMdata.EAM_Subsidy_Level_Start_Date").alias("EAM_Subsidy_Level_Start_Date"),
        F.col("EAMdata.EAM_PremWithholdOption").alias("EAM_PremWithholdOption")
    )
)

df_iter = spark.range(1, 19).withColumnRenamed("id", "iter")

df_Xmr2_temp = df_Jn.alias("JnO_Xmr").crossJoin(df_iter)

loopVarFieldExpr = F.when(F.col("iter")==1, F.lit("PlanID")) \
    .when(F.col("iter")==2, F.lit("MedicalProductID")) \
    .when(F.col("iter")==3, F.lit("SCCCode")) \
    .when(F.col("iter")==4, F.lit("DentalProductID")) \
    .when(F.col("iter")==5, F.lit("PBPID")) \
    .when(F.col("iter")==6, F.lit("PBP_EFF_DT")) \
    .when(F.col("iter")==7, F.lit("PrimaryRXID")) \
    .when(F.col("iter")==8, F.lit("PrimaryRXGroup")) \
    .when(F.col("iter")==9, F.lit("PrimaryRXBin")) \
    .when(F.col("iter")==10, F.lit("PrimaryRXPCN")) \
    .when(F.col("iter")==11, F.lit("NONE")) \
    .when(F.col("iter")==12, F.lit("PenaltyAmount")) \
    .when(F.col("iter")==13, F.lit("Subsidy Level")) \
    .when(F.col("iter")==14, F.lit("LIS Effective Date")) \
    .when(F.col("iter")==15, F.lit("PremWithholdOption")) \
    .when(F.col("iter")==16, F.lit("MemberID")) \
    .when(F.col("iter")==17, F.lit("GroupID")) \
    .when(F.col("iter")==18, F.lit("MBI")) \
    .otherwise(F.lit("NONE"))

loopVarFacetsAttrExpr = (
    F.when(F.col("iter")==1, F.col("JnO_Xmr.FACETS_PlanID"))
    .when(F.col("iter")==2, F.col("JnO_Xmr.FACETS_MedicalProductID"))
    .when(F.col("iter")==3, F.trim(F.when(F.col("JnO_Xmr.FACETS_SCCCode").isNull(), F.lit("")).otherwise(F.col("JnO_Xmr.FACETS_SCCCode"))))
    .when(F.col("iter")==4, F.col("JnO_Xmr.FACETS_DentalProductID"))
    .when(F.col("iter")==5, F.col("JnO_Xmr.FACETS_PBPID"))
    .when(F.col("iter")==6, F.col("JnO_Xmr.FACETS_PBP_EFF_DT"))
    .when(F.col("iter")==7, F.col("JnO_Xmr.FACETS_PrimaryRXID"))
    .when(F.col("iter")==8, F.col("JnO_Xmr.FACETS_PrimaryRxGroup"))
    .when(F.col("iter")==9, F.col("JnO_Xmr.FACETS_PrimaryRXBIN"))
    .when(F.col("iter")==10, F.col("JnO_Xmr.FACETS_PrimaryRXPCN"))
    .when(F.col("iter")==11, F.lit("NONE"))
    .when(F.col("iter")==12, F.trim(F.when(F.col("JnO_Xmr.FACETS_PenaltyAmount").isNull(), F.lit("0.00")).otherwise(F.col("JnO_Xmr.FACETS_PenaltyAmount"))))
    .when(F.col("iter")==13, F.trim(F.when(F.col("JnO_Xmr.FACETS_SubsidyLevel").isNull(), F.lit("000")).otherwise(F.col("JnO_Xmr.FACETS_SubsidyLevel"))))
    .when(F.col("iter")==14, F.col("JnO_Xmr.FACETS_Subsidy_Level_Start_Date"))
    .when(F.col("iter")==15, F.col("JnO_Xmr.FACETS_PremWithholdOption"))
    .when(F.col("iter")==16, F.col("JnO_Xmr.FACETS_MemberID"))
    .when(F.col("iter")==17, F.col("JnO_Xmr.FACETS_GroupID"))
    .when(F.col("iter")==18, F.trim(F.when(F.col("JnO_Xmr.FACETS_MBI").isNull(), F.lit("")).otherwise(F.col("JnO_Xmr.FACETS_MBI"))))
    .otherwise(F.lit("NONE"))
)

loopVarEAMAttrExpr = (
    F.when(F.col("iter")==1, F.col("JnO_Xmr.EAM_PlanID"))
    .when(F.col("iter")==2, F.col("JnO_Xmr.EAM_MedicalProductID"))
    .when(F.col("iter")==3, F.trim(F.when(F.col("JnO_Xmr.EAM_SCCCode").isNull(), F.lit("")).otherwise(F.col("JnO_Xmr.EAM_SCCCode"))))
    .when(F.col("iter")==4, F.col("JnO_Xmr.EAM_DentalProductID"))
    .when(F.col("iter")==5, F.col("JnO_Xmr.EAM_PBPID"))
    .when(F.col("iter")==6, F.col("JnO_Xmr.EAM_PBP_EFF_DT"))
    .when(F.col("iter")==7, F.col("JnO_Xmr.EAM_PrimaryRXID"))
    .when(F.col("iter")==8, F.col("JnO_Xmr.EAM_PrimaryRxGroup"))
    .when(F.col("iter")==9, F.col("JnO_Xmr.EAM_PrimaryRXBIN"))
    .when(F.col("iter")==10, F.col("JnO_Xmr.EAM_PrimaryRXPCN"))
    .when(F.col("iter")==11, F.lit("NONE"))
    .when(F.col("iter")==12, F.col("JnO_Xmr.EAM_PenaltyAmount"))
    .when(F.col("iter")==13, F.col("JnO_Xmr.EAM_SubsidyLevel"))
    .when(F.col("iter")==14, F.col("JnO_Xmr.EAM_Subsidy_Level_Start_Date"))
    .when(F.col("iter")==15, F.col("JnO_Xmr.EAM_PremWithholdOption"))
    .when(F.col("iter")==16, F.col("JnO_Xmr.EAM_MemberID"))
    .when(F.col("iter")==17, F.col("JnO_Xmr.EAM_GroupID"))
    .when(F.col("iter")==18, F.trim(F.when(F.col("JnO_Xmr.EAM_MBI").isNull(), F.lit("")).otherwise(F.col("JnO_Xmr.EAM_MBI"))))
    .otherwise(F.lit("NONE"))
)

df_Xmr2_sub = df_Xmr2_temp.select(
    F.col("JnO_Xmr.*"),
    loopVarFieldExpr.alias("LoopVarField"),
    loopVarFacetsAttrExpr.alias("LoopVarFacetsAttr"),
    loopVarEAMAttrExpr.alias("LoopVarEAMAttr")
)

trimEAMMBI = F.trim(F.when(F.col("EAM_MBI").isNull(), F.lit("")).otherwise(F.col("EAM_MBI")))
trimFACETSMBI = F.trim(F.when(F.col("FACETS_MBI").isNull(), F.lit("")).otherwise(F.col("FACETS_MBI")))
trimEAMAttr = F.trim(F.when(F.col("LoopVarEAMAttr").isNull(), F.lit("")).otherwise(F.col("LoopVarEAMAttr")))
trimFACETSAttr = F.trim(F.when(F.col("LoopVarFacetsAttr").isNull(), F.lit("")).otherwise(F.col("LoopVarFacetsAttr")))

conditionConstraint = F.when(
    (trimEAMMBI == "") | (trimFACETSMBI == ""),
    (trimEAMAttr != trimFACETSAttr) & (F.col("LoopVarField") == F.lit("MBI"))
).otherwise(trimEAMAttr != trimFACETSAttr)

fieldColumn = F.when(
    F.col("LoopVarField")=="MBI",
    F.when(trimEAMMBI == "", F.lit("NO_ELIG_EAM")).otherwise(F.lit("NO_ELIG_FACETS"))
).otherwise(F.col("LoopVarField"))

df_Xmr2 = df_Xmr2_sub.filter(conditionConstraint).select(
    fieldColumn.alias("Field"),
    F.col("EAM_MemberID").alias("EAM_MemberID"),
    F.col("FACETS_MemberID").alias("FACETS_MemberID"),
    F.col("LoopVarEAMAttr").alias("EAM"),
    F.col("LoopVarFacetsAttr").alias("FACETS"),
    F.col("EAM_MBI").alias("EAM_MBI"),
    F.col("FACETS_MBI").alias("FACETS_MBI"),
    F.col("EAM_GroupID").alias("EAM_GROUP_ID"),
    F.col("FACETS_GroupID").alias("FACETS_GROUP_ID")
)

file_path = f"{adls_path_publish}/external/FACETS_EAM_BillingAttr.{RUNID}.dat"
write_files(
    df_Xmr2,
    file_path,
    delimiter='|',
    mode='overwrite',
    is_pqruet=False,
    header=True,
    quote='"',
    nullValue=None
)