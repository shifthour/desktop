# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC JOB NAME:  FacetsEAMEffectiveDateExtr
# MAGIC Calling Job:EAMFctsReconRptCntl
# MAGIC 
# MAGIC This job  retrieves the MEPE_EFF_DT from Facets and compares the EffectiveDate for each MBI  from  EAM and write the results to a file where the Dates do not match.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC ========================================================================================================================================================================
# MAGIC Developer                          Date                           Project/Altiris #                              Change Description                                                    Development Project                 Reviewed By                          Reviewed On
# MAGIC ========================================================================================================================================================================
# MAGIC Kshema H K                     2024-03-20                    US 612321                                  Original Programming                                                        IntegrateDev2                     Jeyaprasanna                          2024-08-02    
# MAGIC 
# MAGIC Arpitha V                          2024-08-13                    US 612321                        Added parenthesis for Transcode filter condition                        IntegrateDev2                     Jeyaprasanna                          2024-08-14
# MAGIC                                                                                                                          in EAM_tbTransactions stage

# MAGIC This job is retrieves the MEPE_EFF_DT from Facets and compares the EffectiveDate for each MBI  from  EAM and write the results to a file where the Dates do not match
# MAGIC Attach EffectiveDate for each MBI from EAM. After the EffectiveDate is attached, it is compared between FACETS and EAM and call out if they don't match.
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
from pyspark.sql.functions import col, lit, when, substring, coalesce, rpad
from pyspark.sql.types import StringType
# COMMAND ----------
# MAGIC %run ../../../Utility_Integrate
# COMMAND ----------

# Obtain parameter values
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
EAMOwner = get_widget_value('EAMOwner','')
eam_secret_name = get_widget_value('eam_secret_name','')
EAMRPTOwner = get_widget_value('EAMRPTOwner','')
eamrpt_secret_name = get_widget_value('eamrpt_secret_name','')
RUNID = get_widget_value('RUNID','')
CurrDt = get_widget_value('CurrDt','')
GRP_ID = get_widget_value('GRP_ID','')

# EAM_DISC_ANALYSIS (ODBCConnectorPX)
jdbc_url_eamrpt, jdbc_props_eamrpt = get_db_config(eamrpt_secret_name)
query_EAM_DISC_ANALYSIS = f"""
SELECT DISC.ID AS ID_SK,
       DISC.MBI,
       DISC.IDENTIFIEDBY,
       DISC.IDENTIFIEDDATE,
       DISC.DISCREPANCY as DISCREPANCY_ID
FROM {EAMRPTOwner}.DISCREPANCY_ANALYSIS DISC
INNER JOIN {EAMRPTOwner}.DISCREPANCY_CONFIG CONFIG
  ON DISC.DISCREPANCY = CONFIG.ID
WHERE STATUS = 'OPEN'
  and DISC.IDENTIFIEDBY = 'CURR'
  and CONFIG.DISCREPANCY_NAME = 'Effective Date'
"""
df_EAM_DISC_ANALYSIS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_eamrpt)
    .options(**jdbc_props_eamrpt)
    .option("query", query_EAM_DISC_ANALYSIS)
    .load()
)

# DISC_CONFIG (ODBCConnectorPX)
query_DISC_CONFIG = f"""
select ID AS DISCREPANCY_ID,
       DISCREPANCY_NAME,
       DISCREPANCY_DESC,
       IDENTIFIEDBY,
       PRIORITY,
       ACTIVE,
       SOURCE1,
       SOURCE2
from {EAMRPTOwner}.DISCREPANCY_CONFIG
WHERE ACTIVE = 'Y'
  AND IDENTIFIEDBY = 'CURR'
  and DISCREPANCY_NAME = 'Effective Date'
"""
df_DISC_CONFIG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_eamrpt)
    .options(**jdbc_props_eamrpt)
    .option("query", query_DISC_CONFIG)
    .load()
)

# EAM_tbTransactions (ODBCConnectorPX)
jdbc_url_eam, jdbc_props_eam = get_db_config(eam_secret_name)
query_EAM_tbTransactions = f"""
select DISTINCT
       trans.HIC as MBI,
       max(trans.EffectiveDate) as EffectiveDate
from {EAMOwner}.tbTransactions trans
inner join {EAMOwner}.tbmemberinfo mi
  on trans.memcodnum = mi.memcodnum
where (trans.TransCode = '61'
       or (trans.TransCode = '80' and trans.ReplyCodes = '287'))
  and mi.GroupNumber in ({GRP_ID})
group by HIC
"""
df_EAM_tbTransactions = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_eam)
    .options(**jdbc_props_eam)
    .option("query", query_EAM_tbTransactions)
    .load()
)

# CMC_MEPE_PRCS_ELIG (ODBCConnectorPX)
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
query_CMC_MEPE_PRCS_ELIG = f"""
Select distinct
       GRGR.GRGR_ID,
       SBSB.SBSB_ID,
       LTRIM(RTRIM(ISNULL(MBR.MEME_HICN,''))) AS MBI,
       ISNULL(MEPE.MEPE_EFF_DT,'') as MEPE_EFF_DT
from {FacetsOwner}.CMC_MEPE_PRCS_ELIG MEPE
inner join {FacetsOwner}.CMC_MEME_MEMBER MBR
  on MBR.MEME_CK = MEPE.MEME_CK
inner join {FacetsOwner}.CMC_SBSB_SUBSC SBSB
  on SBSB.SBSB_CK = MBR.SBSB_CK
inner join {FacetsOwner}.CMC_GRGR_GROUP GRGR
  on GRGR.GRGR_CK = SBSB.GRGR_CK
where (MEPE.PDPD_ID like 'MPM%'
       or MEPE.PDPD_ID like 'MPK%'
       or MEPE.PDPD_ID like 'MHM%'
       or MEPE.PDPD_ID like 'MHK%')
  and MEPE.MEPE_TERM_DT >= '{CurrDt}'
  and GRGR.GRGR_ID IN ({GRP_ID})
  and MEPE.MEPE_ELIG_IND = 'Y'
"""
df_CMC_MEPE_PRCS_ELIG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", query_CMC_MEPE_PRCS_ELIG)
    .load()
)

# Att_EAM_tbTransactions (PxLookup) - primary link: MemAttr (df_CMC_MEPE_PRCS_ELIG), lookup link: df_EAM_tbTransactions (inner join on MBI)
df_Att_EAM_tbTransactions = (
    df_CMC_MEPE_PRCS_ELIG.alias("MemAttr")
    .join(
        df_EAM_tbTransactions.alias("Ref_EAM_tbTransactions"),
        col("MemAttr.MBI") == col("Ref_EAM_tbTransactions.MBI"),
        "inner"
    )
    .select(
        col("MemAttr.GRGR_ID").alias("GRGR_ID"),
        col("MemAttr.SBSB_ID").alias("SBSB_ID"),
        col("MemAttr.MBI").alias("MBI"),
        col("MemAttr.MEPE_EFF_DT").alias("MEPE_EFF_DT"),
        col("Ref_EAM_tbTransactions.EffectiveDate").alias("EffectiveDate")
    )
)

# Xmr (CTransformerStage)
# Create intermediate columns svEffDt, svMepeEffDt by assuming FORMAT_DATE_EE is a user-defined function
df_Xmr = (
    df_Att_EAM_tbTransactions
    .withColumn("svEffDt", FORMAT_DATE_EE(col("EffectiveDate"), "SYBASE", "TIMESTAMP", "CCYY-MM-DD"))
    .withColumn("svMepeEffDt", FORMAT_DATE_EE(col("MEPE_EFF_DT"), "SYBASE", "TIMESTAMP", "CCYY-MM-DD"))
)

# EffDt_Lkp output (constraint: EffectiveDate <> MEPE_EFF_DT)
df_EffDt_Lkp = (
    df_Xmr
    .filter(col("EffectiveDate") != col("MEPE_EFF_DT"))
    .select(
        F.lit("Effective Date").alias("Field"),
        col("GRGR_ID").alias("GRGR_ID"),
        col("SBSB_ID").alias("SBSB_ID"),
        col("MBI").alias("MBI"),
        (
            substring(col("svEffDt"), 6, 2)
            + F.lit("/")
            + substring(col("svEffDt"), 9, 2)
            + F.lit("/")
            + substring(col("svEffDt"), 1, 4)
        ).alias("EffectiveDate"),
        (
            substring(col("svMepeEffDt"), 6, 2)
            + F.lit("/")
            + substring(col("svMepeEffDt"), 9, 2)
            + F.lit("/")
            + substring(col("svMepeEffDt"), 1, 4)
        ).alias("MEPE_EFF_DT")
    )
)

# EffDt_Config (PxLookup) - primary link: df_EffDt_Lkp, lookup link: df_DISC_CONFIG (inner join, no join condition => cross join)
df_EffDt_Config = (
    df_EffDt_Lkp.alias("EffDt_Lkp")
    .crossJoin(df_DISC_CONFIG.alias("Ref_Disc_Config"))
    .select(
        col("EffDt_Lkp.Field").alias("Field"),
        col("EffDt_Lkp.GRGR_ID").alias("GRGR_ID"),
        col("EffDt_Lkp.SBSB_ID").alias("SBSB_ID"),
        col("EffDt_Lkp.MBI").alias("MBI"),
        col("Ref_Disc_Config.DISCREPANCY_ID").alias("DISCREPANCY_ID"),
        col("Ref_Disc_Config.DISCREPANCY_NAME").alias("DISCREPANCY_NAME"),
        col("Ref_Disc_Config.DISCREPANCY_DESC").alias("DISCREPANCY_DESC"),
        col("Ref_Disc_Config.IDENTIFIEDBY").alias("IDENTIFIEDBY"),
        col("Ref_Disc_Config.PRIORITY").alias("PRIORITY"),
        col("Ref_Disc_Config.ACTIVE").alias("ACTIVE"),
        col("EffDt_Lkp.EffectiveDate").alias("SOURCE1"),
        col("EffDt_Lkp.MEPE_EFF_DT").alias("SOURCE2")
    )
)

# FullOuter (PxJoin) - full outer join
df_FullOuter = (
    df_EffDt_Config.alias("RuleFailures")
    .join(
        df_EAM_DISC_ANALYSIS.alias("DISC_Analysis"),
        (
            (col("RuleFailures.MBI") == col("DISC_Analysis.MBI"))
            & (col("RuleFailures.DISCREPANCY_ID") == col("DISC_Analysis.DISCREPANCY_ID"))
            & (col("RuleFailures.IDENTIFIEDBY") == col("DISC_Analysis.IDENTIFIEDBY"))
        ),
        "full"
    )
    .select(
        col("RuleFailures.Field").alias("Field"),
        col("RuleFailures.SBSB_ID").alias("FACETS_MemberID"),
        col("RuleFailures.MBI").alias("leftRec_MBI"),
        col("RuleFailures.GRGR_ID").alias("FACETS_GROUP_ID"),
        col("RuleFailures.DISCREPANCY_ID").alias("leftRec_DISCREPANCY_ID"),
        col("RuleFailures.DISCREPANCY_DESC").alias("DISCREPANCY_DESC"),
        col("RuleFailures.IDENTIFIEDBY").alias("leftRec_IDENTIFIEDBY"),
        col("RuleFailures.PRIORITY").alias("PRIORITY"),
        col("RuleFailures.SOURCE1").alias("SOURCE1"),
        col("RuleFailures.SOURCE2").alias("SOURCE2"),
        col("DISC_Analysis.ID_SK").alias("ID_SK"),
        col("DISC_Analysis.MBI").alias("rightRec_MBI"),
        col("DISC_Analysis.IDENTIFIEDBY").alias("rightRec_IDENTIFIEDBY"),
        col("DISC_Analysis.IDENTIFIEDDATE").alias("IDENTIFIEDDATE"),
        col("DISC_Analysis.DISCREPANCY_ID").alias("rightRec_DISCREPANCY_ID")
    )
)

# Xmr1 (CTransformerStage) - three output links

# Output link "NewAdds"
df_Xmr1_NewAdds = (
    df_FullOuter
    .filter(
        (col("rightRec_MBI") == "")
        & (col("leftRec_MBI").isNotNull())
        & (col("leftRec_MBI") != "")
    )
    .select(
        col("FACETS_GROUP_ID").alias("GRGR_ID"),
        col("FACETS_MemberID").alias("SBSB_ID"),
        col("leftRec_MBI").alias("MBI"),
        col("SOURCE1").alias("SOURCE1"),
        col("SOURCE2").alias("SOURCE2"),
        col("leftRec_IDENTIFIEDBY").alias("IDENTIFIEDBY"),
        StringToDate(lit(CurrDt), "%yyyy-%mm-%dd").alias("IDENTIFIEDDATE"),
        F.lit(None).cast(StringType()).alias("RESOLVEDDATE"),
        F.lit("OPEN").alias("STATUS"),
        col("leftRec_DISCREPANCY_ID").alias("DISCREPANCY"),
        col("DISCREPANCY_DESC").alias("NOTE"),
        col("PRIORITY").alias("PRIORITY"),
        F.lit(0).alias("AGE"),
        when(col("FACETS_MemberID") == "", "N")
        .when(substring(col("FACETS_MemberID"), 1, 3) == "000", "Y")
        .otherwise("N")
        .alias("LEGACY"),
        StringToDate(lit(CurrDt), "%yyyy-%mm-%dd").alias("LAST_UPDATED_DATE")
    )
)

# Output link "Resolved"
df_Xmr1_Resolved = (
    df_FullOuter
    .filter(
        (coalesce(col("leftRec_MBI"), lit("")) == "")
        & (col("rightRec_MBI") != "")
    )
    .select(
        col("ID_SK").alias("ID_SK")
    )
)

# Output link "StillOpen"
df_Xmr1_StillOpen = (
    df_FullOuter
    .filter(
        (coalesce(col("leftRec_MBI"), lit("")) != "")
        & (col("rightRec_MBI") != "")
    )
    .select(
        col("ID_SK").alias("ID_SK"),
        DaysSinceFromDate2(
            StringToDate(lit(CurrDt), "%yyyy-%mm-%dd"),
            col("IDENTIFIEDDATE")
        ).alias("AGE"),
        StringToDate(lit(CurrDt), "%yyyy-%mm-%dd").alias("LAST_UPDATED_DATE")
    )
)

# Aged (PxSequentialFile)
df_Aged = df_Xmr1_StillOpen.select(
    "ID_SK",
    "AGE",
    "LAST_UPDATED_DATE"
)
write_files(
    df_Aged,
    f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_EFFDT_AGED.{RUNID}.dat",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)

# Resolved (PxSequentialFile)
df_Resolved = df_Xmr1_Resolved.select(
    "ID_SK"
)
write_files(
    df_Resolved,
    f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_EFFDT_RESOLVED.{RUNID}.dat",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)

# RmDup (PxRemDup) on NewAdds
df_NewAdds_deduped = dedup_sort(
    df_Xmr1_NewAdds,
    partition_cols=["MBI","IDENTIFIEDBY","IDENTIFIEDDATE","DISCREPANCY"],
    sort_cols=[("MBI","A"),("IDENTIFIEDBY","A"),("IDENTIFIEDDATE","A"),("DISCREPANCY","A")]
)

df_RmDup = df_NewAdds_deduped.select(
    col("GRGR_ID").alias("GRGR_ID"),
    col("SBSB_ID").alias("SBSB_ID"),
    col("MBI").alias("MBI"),
    col("SOURCE1").alias("SOURCE1"),
    col("SOURCE2").alias("SOURCE2"),
    col("IDENTIFIEDBY").alias("IDENTIFIEDBY"),
    col("IDENTIFIEDDATE").alias("IDENTIFIEDDATE"),
    col("RESOLVEDDATE").alias("RESOLVEDDATE"),
    col("STATUS").alias("STATUS"),
    col("DISCREPANCY").alias("DISCREPANCY"),
    col("NOTE").alias("NOTE"),
    col("PRIORITY").alias("PRIORITY"),
    col("AGE").alias("AGE"),
    # LEGACY is char(1) -> rpad
    rpad(col("LEGACY"), 1, " ").alias("LEGACY"),
    col("LAST_UPDATED_DATE").alias("LAST_UPDATED_DATE")
)

# New (PxSequentialFile)
write_files(
    df_RmDup,
    f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_EFFDT_APPEND.{RUNID}.dat",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)