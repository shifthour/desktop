# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC JOB NAME:  FacetsEAMLISCoPayLeveCatExtr
# MAGIC Calling Job:EAMFctsReconRptCntl
# MAGIC 
# MAGIC This job is retrieves the MEMD_COPAY_CAT  from Facets and compares the LISCoPayLevelCat for each MBI  from EAM and write the results to a file where the CoPay_Cat do not match and not present on either
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC ========================================================================================================================================================================
# MAGIC Developer                          Date                           Project/Altiris #                              Change Description                                                    Development Project                 Reviewed By                          Reviewed On
# MAGIC ========================================================================================================================================================================                       
# MAGIC Arpitha V                      2024-03-20                        US 611997                                Original Programming                                                        IntegrateDev2                       Jeyaprasanna                        2024-06-21
# MAGIC 
# MAGIC Arpitha V                      2024-09-11                        US 627862                   Modified the source query in CMC_MEMD_MECR_DETL            IntegrateDev2                       Jeyaprasanna                        2024-09-12
# MAGIC                                                                                                                    stage to retrieve recent data based on MEMD_HCFA_EFF_DT 
# MAGIC                                                                                                                    and replaced Att_EAM_tbMemberInfoLISLog lookup stage with
# MAGIC                                                                                                                    Source_FullOuter join stage to retrieve missing data
# MAGIC 
# MAGIC Arpitha V                     2024-10-11                         US 630533                  Modified the source query in CMC_MEMD_MECR_DETL              IntegrateDev2                      Jeyaprasanna                      2024-10-23
# MAGIC                                                                                                                   and EAM_tbMemberInfoLISLog stage to check if the member
# MAGIC                                                                                                                   is active and to check the term date and LowIncomeCoPayEndDate
# MAGIC 
# MAGIC Arpitha V                    2024-11-27                          US 635075                        Removed Group Number filter condition from                            IntegrateDev2                     Jeyaprasanna                      2024-12-02
# MAGIC                                                                                                                                EAM_tbMemberInfoLISLog  stage
# MAGIC 
# MAGIC Arpitha V                    2024-12-10                          US 635075                  Added conditions in Facets and EAM                                            IntegrateDev2                     Jeyaprasanna                      2024-12-10
# MAGIC                                                                                                               FACETS - and MEMD.MEMD_HCFA_TERM_DT >= '2024-01-01'
# MAGIC                                                                                                               EAM - and Mbrinfo.LowIncomeCoPayEffectiveDate < '2025-01-01'

# MAGIC This job retrieves the MEMD_COPAY_CAT from Facets and compares the LISCoPayLevelCat for each MBI  from  EAM and write the results to a file where the CATs do not match
# MAGIC This file will have discrepancies that are still open
# MAGIC This file will have resolved discrepancies between last run and current run. These records need to be deleted
# MAGIC This file will have newly found discrepancies
# MAGIC Check if a discrepancy already exists for an MBI and take appropriate action if exits/does not exists/existed and fixes by currne Run.
# MAGIC This is to remove duplicates if an MBI has more than one SBSB_ID.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, DateType
# COMMAND ----------
# MAGIC %run ../../../Utility_Integrate
# COMMAND ----------


# Parameter definitions (including any required secret names)
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
EAMOwner = get_widget_value('EAMOwner','')
eam_secret_name = get_widget_value('eam_secret_name','')
EAMRPTOwner = get_widget_value('EAMRPTOwner','')
eamrpt_secret_name = get_widget_value('eamrpt_secret_name','')
RUNID = get_widget_value('RUNID','')
CurrDt = get_widget_value('CurrDt','')
GRP_ID = get_widget_value('GRP_ID','')

# Obtain JDBC configuration for each database secret
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
jdbc_url_eam, jdbc_props_eam = get_db_config(eam_secret_name)
jdbc_url_eamrpt, jdbc_props_eamrpt = get_db_config(eamrpt_secret_name)

# -------------------------------------------------------------------------------------------------
# Stage: EAM_DISC_ANALYSIS (ODBCConnectorPX)
query_EAM_DISC_ANALYSIS = """SELECT  DISC.ID AS ID_SK,DISC.MBI,DISC.IDENTIFIEDBY,DISC.IDENTIFIEDDATE,DISC.DISCREPANCY as DISCREPANCY_ID
FROM #$EAMRPTOwner#.DISCREPANCY_ANALYSIS DISC
INNER JOIN #$EAMRPTOwner#.DISCREPANCY_CONFIG CONFIG
ON DISC.DISCREPANCY = CONFIG.ID
WHERE STATUS = 'OPEN' 
and DISC.IDENTIFIEDBY = 'CURR'
and CONFIG.DISCREPANCY_NAME = 'LIS Level Copay Category'
"""
df_EAM_DISC_ANALYSIS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_eamrpt)
    .options(**jdbc_props_eamrpt)
    .option("query", query_EAM_DISC_ANALYSIS)
    .load()
)

# -------------------------------------------------------------------------------------------------
# Stage: DISC_CONFIG (ODBCConnectorPX)
query_DISC_CONFIG = """select ID AS DISCREPANCY_ID,    DISCREPANCY_NAME,    DISCREPANCY_DESC,    IDENTIFIEDBY,    PRIORITY,    ACTIVE,    SOURCE1,    SOURCE2
from #$EAMRPTOwner#.DISCREPANCY_CONFIG
WHERE ACTIVE = 'Y'
AND IDENTIFIEDBY = 'CURR'
AND DISCREPANCY_NAME = 'LIS Level Copay Category'
"""
df_DISC_CONFIG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_eamrpt)
    .options(**jdbc_props_eamrpt)
    .option("query", query_DISC_CONFIG)
    .load()
)

# -------------------------------------------------------------------------------------------------
# Stage: EAM_tbMemberInfoLISLog (ODBCConnectorPX)
query_EAM_tbMemberInfoLISLog = """select HIC as MBI, LowIncCoPayCat, LowIncomeCoPayEffectiveDate as LICS_EFF_DT, GroupNumber, MemberID
from (
select  mi.HIC , CAST(Mbrinfo.LowIncCoPayCat as Varchar) as LowIncCoPayCat, 
Mbrinfo.LowIncomeCoPayEffectiveDate,
Mbrinfo.LowIncomeCoPayEndDate,
info.GroupNumber, mi.MemberID
,dense_rank() over (partition by mi.HIC order by Mbrinfo.LowIncomeCoPayEffectiveDate desc) rnk

from #$EAMOwner#.tbMemberInfoLISLog Mbrinfo
inner join #$EAMOwner#.tbEENRLMembers mi on Mbrinfo.MemCodNum=mi.MemCodNum
inner join #$EAMOwner#.tbMemberInfo info ON info.MemCodNum = mi.MemCodNum

where Mbrinfo.Valid in ('1')
and Mbrinfo.LowIncomeCoPayEndDate >= '2024-01-01'
and Mbrinfo.LowIncomeCoPayEffectiveDate < '2025-01-01'
Group by mi.HIC, Mbrinfo.LowIncCoPayCat,info.GroupNumber,mi.MemberID,Mbrinfo.LowIncomeCoPayEffectiveDate,Mbrinfo.LowIncomeCoPayEndDate
) LIS
where LIS.rnk = 1
"""
df_EAM_tbMemberInfoLISLog = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_eam)
    .options(**jdbc_props_eam)
    .option("query", query_EAM_tbMemberInfoLISLog)
    .load()
)

# -------------------------------------------------------------------------------------------------
# Stage: CMC_MEMD_MECR_DETL (ODBCConnectorPX)
query_CMC_MEMD_MECR_DETL = f"""Select GRGR_ID,
SBSB_ID,
MBI,
MEMD_COPAY_CAT,
MEMD_HCFA_EFF_DT as LICS_EFF_DT
from (
select distinct
GRGR.GRGR_ID,
SBSB.SBSB_ID,
LTRIM(RTRIM(ISNULL(MBR.MEME_HICN,''))) AS MBI,
MEMD.MEMD_COPAY_CAT,
MEMD.MEMD_HCFA_EFF_DT,
dense_rank() over (partition by MBR.MEME_HICN order by MEMD.MEMD_HCFA_EFF_DT desc) rnk

FROM #$FacetsOwner#.CMC_MEMD_MECR_DETL MEMD 
inner join #$FacetsOwner#.CMC_MEME_MEMBER MBR on MBR.MEME_CK = MEMD.MEME_CK
inner join #$FacetsOwner#.CMC_SBSB_SUBSC SBSB on SBSB.SBSB_CK = MBR.SBSB_CK
INNER JOIN #$FacetsOwner#.CMC_GRGR_GROUP GRGR ON GRGR.GRGR_CK=SBSB.GRGR_CK
INNER JOIN #$FacetsOwner#.CMC_MEPE_PRCS_ELIG ELIG ON MEMD.MEME_CK = ELIG.MEME_CK

where GRGR.GRGR_ID IN ({GRP_ID})
AND MEMD.MEMD_EVENT_CD in ('LICS') 
AND ELIG.MEPE_TERM_DT >= '2024-01-01' 
and ELIG.MEPE_ELIG_IND = 'Y'
and ELIG.CSPD_CAT = 'M'
and MEMD.MEMD_HCFA_TERM_DT >= '2024-01-01'
Group by GRGR.GRGR_ID,SBSB.SBSB_ID,MBR.MEME_HICN,MEMD.MEMD_HCFA_EFF_DT,MEMD.MEMD_COPAY_CAT
) as LIS
where LIS.rnk = 1
"""
df_CMC_MEMD_MECR_DETL = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", query_CMC_MEMD_MECR_DETL)
    .load()
)

# -------------------------------------------------------------------------------------------------
# Stage: Source_FullOuter (PxJoin fullouter on MBI)
df_Source_FullOuter = (
    df_EAM_tbMemberInfoLISLog.alias("Ref_EAM_tbMemberInfoLISLog")
    .join(
        df_CMC_MEMD_MECR_DETL.alias("MemAttr"),
        on=["MBI"],
        how="fullouter"
    )
    .select(
        F.col("MemAttr.GRGR_ID").alias("GRGR_ID"),
        F.col("MemAttr.SBSB_ID").alias("SBSB_ID"),
        F.col("Ref_EAM_tbMemberInfoLISLog.MBI").alias("MBI"),
        F.col("MemAttr.MEMD_COPAY_CAT").alias("MEMD_COPAY_CAT"),
        F.col("MemAttr.MBI").alias("rightRec_MBI"),
        F.col("Ref_EAM_tbMemberInfoLISLog.LowIncCoPayCat").alias("LowIncCoPayCat"),
        F.col("Ref_EAM_tbMemberInfoLISLog.GroupNumber").alias("GroupNumber"),
        F.col("Ref_EAM_tbMemberInfoLISLog.MemberID").alias("MemberID")
    )
)

# -------------------------------------------------------------------------------------------------
# Stage: Xmr (CTransformerStage)
df_Xmr = df_Source_FullOuter.withColumn(
    "svIsNull",
    F.when(F.col("MBI").isNull(), F.col("rightRec_MBI")).otherwise(F.col("MBI"))
)

# Output pin "LISCopay_Lkp", constraint:
# (LowIncCoPayCat <> MEMD_COPAY_CAT) or IsNull(LowIncCoPayCat) or IsNull(MEMD_COPAY_CAT)
df_Xmr_LISCopay_Lkp = (
    df_Xmr.filter(
        (F.col("LowIncCoPayCat") != F.col("MEMD_COPAY_CAT"))
        | (F.col("LowIncCoPayCat").isNull())
        | (F.col("MEMD_COPAY_CAT").isNull())
    )
    .select(
        F.lit("LIS Level Copay Category").alias("Field"),
        F.when(F.col("GRGR_ID").isNull(), F.col("GroupNumber")).otherwise(F.col("GRGR_ID")).alias("GRGR_ID"),
        F.when(F.col("SBSB_ID").isNull(), F.col("MemberID")).otherwise(F.col("SBSB_ID")).alias("SBSB_ID"),
        F.col("svIsNull").alias("MBI"),
        F.when(F.col("MEMD_COPAY_CAT").isNull(), F.lit("")).otherwise(F.col("MEMD_COPAY_CAT")).alias("MEMD_COPAY_CAT"),
        F.when(F.col("LowIncCoPayCat").isNull(), F.lit("")).otherwise(F.col("LowIncCoPayCat")).alias("LowIncCoPayCat"),
    )
)

# -------------------------------------------------------------------------------------------------
# Stage: StrtDt_Config (PxLookup). The link "Ref_Disc_Config" is a lookup link, no explicit condition => cross join (inner).
df_StrtDt_Config = (
    df_Xmr_LISCopay_Lkp.alias("LISCopay_Lkp")
    .join(
        df_DISC_CONFIG.alias("Ref_Disc_Config"),
        how="inner"
    )
    .select(
        F.col("LISCopay_Lkp.Field").alias("Field"),
        F.col("LISCopay_Lkp.GRGR_ID").alias("GRGR_ID"),
        F.col("LISCopay_Lkp.SBSB_ID").alias("SBSB_ID"),
        F.col("LISCopay_Lkp.MBI").alias("MBI"),
        F.col("Ref_Disc_Config.DISCREPANCY_ID").alias("DISCREPANCY_ID"),
        F.col("Ref_Disc_Config.DISCREPANCY_NAME").alias("DISCREPANCY_NAME"),
        F.col("Ref_Disc_Config.DISCREPANCY_DESC").alias("DISCREPANCY_DESC"),
        F.col("Ref_Disc_Config.IDENTIFIEDBY").alias("IDENTIFIEDBY"),
        F.col("Ref_Disc_Config.PRIORITY").alias("PRIORITY"),
        F.col("Ref_Disc_Config.ACTIVE").alias("ACTIVE"),
        F.col("LISCopay_Lkp.LowIncCoPayCat").alias("SOURCE1"),
        F.col("LISCopay_Lkp.MEMD_COPAY_CAT").alias("SOURCE2"),
    )
)

# -------------------------------------------------------------------------------------------------
# Stage: FullOuter (PxJoin), keys = MBI, DISCREPANCY_ID, IDENTIFIEDBY
df_FullOuter = (
    df_StrtDt_Config.alias("RuleFailures")
    .join(
        df_EAM_DISC_ANALYSIS.alias("DISC_Analysis"),
        on=[
            F.col("RuleFailures.MBI") == F.col("DISC_Analysis.MBI"),
            F.col("RuleFailures.DISCREPANCY_ID") == F.col("DISC_Analysis.DISCREPANCY_ID"),
            F.col("RuleFailures.IDENTIFIEDBY") == F.col("DISC_Analysis.IDENTIFIEDBY"),
        ],
        how="fullouter"
    )
    .select(
        F.col("RuleFailures.Field").alias("Field"),
        F.col("RuleFailures.SBSB_ID").alias("FACETS_MemberID"),
        F.col("RuleFailures.MBI").alias("leftRec_MBI"),
        F.col("RuleFailures.GRGR_ID").alias("FACETS_GROUP_ID"),
        F.col("RuleFailures.DISCREPANCY_ID").alias("leftRec_DISCREPANCY_ID"),
        F.col("RuleFailures.DISCREPANCY_DESC").alias("DISCREPANCY_DESC"),
        F.col("RuleFailures.IDENTIFIEDBY").alias("leftRec_IDENTIFIEDBY"),
        F.col("RuleFailures.PRIORITY").alias("PRIORITY"),
        F.col("RuleFailures.SOURCE1").alias("SOURCE1"),
        F.col("RuleFailures.SOURCE2").alias("SOURCE2"),
        F.col("DISC_Analysis.ID_SK").alias("ID_SK"),
        F.col("DISC_Analysis.MBI").alias("rightRec_MBI"),
        F.col("DISC_Analysis.IDENTIFIEDBY").alias("rightRec_IDENTIFIEDBY"),
        F.col("DISC_Analysis.IDENTIFIEDDATE").alias("IDENTIFIEDDATE"),
        F.col("DISC_Analysis.DISCREPANCY").alias("rightRec_DISCREPANCY_ID"),
    )
)

# -------------------------------------------------------------------------------------------------
# Stage: Xmr1 (CTransformerStage)
# Output links: NewAdds, StillOpen, Resolved

# 1) NewAdds
df_Xmr1_NewAdds = (
    df_FullOuter.filter(
        (F.col("rightRec_MBI") == "")
        & (
            F.when(
                F.col("leftRec_MBI").isNotNull(), F.col("leftRec_MBI")
            ).otherwise(F.lit("")) != ""
        )
    )
    .select(
        F.when(F.col("FACETS_GROUP_ID").isNull(), F.lit("NULL"))
         .otherwise(F.col("FACETS_GROUP_ID")).alias("GRGR_ID"),
        F.col("FACETS_MemberID").alias("SBSB_ID"),
        F.col("leftRec_MBI").alias("MBI"),
        F.col("SOURCE1").alias("SOURCE1"),
        F.col("SOURCE2").alias("SOURCE2"),
        F.col("leftRec_IDENTIFIEDBY").alias("IDENTIFIEDBY"),
        # StringToDate(CurrDt, "%yyyy-%mm-%dd") => user-defined function; assume direct call
        # We'll do a withColumn in a moment.
        F.lit("OPEN").alias("STATUS"),
        F.col("leftRec_DISCREPANCY_ID").alias("DISCREPANCY"),
        F.col("DISCREPANCY_DESC").alias("NOTE"),
        F.col("PRIORITY").alias("PRIORITY"),
        F.lit(0).alias("AGE"),
        F.when(
            F.col("FACETS_MemberID") == "", F.lit("N")
        ).otherwise(
            F.when(F.col("FACETS_MemberID").substr(1, 3) == "000", F.lit("Y"))
            .otherwise(F.lit("N"))
        ).alias("LEGACY"),
    )
)

df_Xmr1_NewAdds = (
    df_Xmr1_NewAdds
    .withColumn("IDENTIFIEDDATE", StringToDate(CurrDt, "%yyyy-%mm-%dd"))
    .withColumn("RESOLVEDDATE", F.lit(None).cast(StringType()))
    .withColumn("LAST_UPDATED_DATE", StringToDate(CurrDt, "%yyyy-%mm-%dd"))
)

# 2) StillOpen
df_Xmr1_StillOpen = (
    df_FullOuter.filter(
        (
            F.when(
                F.col("leftRec_MBI").isNotNull(), F.col("leftRec_MBI")
            ).otherwise(F.lit("")) != ""
        )
        & (F.col("rightRec_MBI") != "")
    )
    .withColumn("ID_SK", F.col("ID_SK"))
    .withColumn("AGE", DaysSinceFromDate2(StringToDate(CurrDt, "%yyyy-%mm-%dd"), F.col("IDENTIFIEDDATE")))
    .withColumn("LAST_UPDATED_DATE", StringToDate(CurrDt, "%yyyy-%mm-%dd"))
    .select("ID_SK", "AGE", "LAST_UPDATED_DATE")
)

# 3) Resolved
df_Xmr1_Resolved = (
    df_FullOuter.filter(
        (
            F.when(
                F.col("leftRec_MBI").isNotNull(), F.col("leftRec_MBI")
            ).otherwise(F.lit("")) == ""
        )
        & (F.col("rightRec_MBI") != "")
    )
    .select(
        F.col("ID_SK").alias("ID_SK")
    )
)

# -------------------------------------------------------------------------------------------------
# Stage: Aged (PxSequentialFile) => write "df_Xmr1_StillOpen"
aged_output_path = f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_LISCOPAYLEVELCAT_AGED.{RUNID}.dat"
write_files(
    df_Xmr1_StillOpen.select("ID_SK","AGE","LAST_UPDATED_DATE"),
    aged_output_path,
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)

# -------------------------------------------------------------------------------------------------
# Stage: Resolved (PxSequentialFile) => write "df_Xmr1_Resolved"
resolved_output_path = f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_LISCOPAYLEVELCAT_RESOLVED.{RUNID}.dat"
write_files(
    df_Xmr1_Resolved.select("ID_SK"),
    resolved_output_path,
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)

# -------------------------------------------------------------------------------------------------
# Stage: RmDup (PxRemDup) => keys: MBI, IDENTIFIEDBY, IDENTIFIEDDATE, DISCREPANCY. RetainRecord=first
df_RmDup = dedup_sort(
    df_Xmr1_NewAdds,
    ["MBI", "IDENTIFIEDBY", "IDENTIFIEDDATE", "DISCREPANCY"],
    []
)

# Output => NewAddsFile
df_RmDup_NewAddsFile = df_RmDup.select(
    F.col("GRGR_ID").alias("GRGR_ID"),
    F.col("SBSB_ID").alias("SBSB_ID"),
    F.col("MBI").alias("MBI"),
    F.col("SOURCE1").alias("SOURCE1"),
    F.col("SOURCE2").alias("SOURCE2"),
    F.col("IDENTIFIEDBY").alias("IDENTIFIEDBY"),
    F.col("IDENTIFIEDDATE").alias("IDENTIFIEDDATE"),
    F.col("RESOLVEDDATE").alias("RESOLVEDDATE"),
    F.col("STATUS").alias("STATUS"),
    F.col("DISCREPANCY").alias("DISCREPANCY"),
    F.col("NOTE").alias("NOTE"),
    F.col("PRIORITY").alias("PRIORITY"),
    F.col("AGE").alias("AGE"),
    F.col("LEGACY").alias("LEGACY"),
    F.col("LAST_UPDATED_DATE").alias("LAST_UPDATED_DATE")
)

# -------------------------------------------------------------------------------------------------
# Stage: New (PxSequentialFile) => write final
# LEGACY is char(1), so apply rpad on that column
df_RmDup_NewAddsFile_final = df_RmDup_NewAddsFile.withColumn(
    "LEGACY", F.rpad(F.col("LEGACY"), 1, " ")
).select(
    "GRGR_ID",
    "SBSB_ID",
    "MBI",
    "SOURCE1",
    "SOURCE2",
    "IDENTIFIEDBY",
    "IDENTIFIEDDATE",
    "RESOLVEDDATE",
    "STATUS",
    "DISCREPANCY",
    "NOTE",
    "PRIORITY",
    "AGE",
    "LEGACY",
    "LAST_UPDATED_DATE"
)
new_output_path = f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_LISCOPAYLEVELCAT_APPEND.{RUNID}.dat"
write_files(
    df_RmDup_NewAddsFile_final,
    new_output_path,
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)