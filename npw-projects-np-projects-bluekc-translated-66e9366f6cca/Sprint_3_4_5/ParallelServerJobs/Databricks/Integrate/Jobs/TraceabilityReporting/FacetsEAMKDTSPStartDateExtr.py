# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC JOB NAME:  FacetsEAMKDTSPStartDateExtr
# MAGIC Calling Job:EAMFctsReconRptCntl
# MAGIC 
# MAGIC This job is retrieves the MEWM_EFF_DT  from Facets and compares the STARTDATE for each MBI  from  EAM and write the results to a file where the Dates do not match.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC ========================================================================================================================================================================
# MAGIC Developer                          Date                           Project/Altiris #                              Change Description                                                    Development Project                 Reviewed By                          Reviewed On
# MAGIC ========================================================================================================================================================================
# MAGIC Arpitha V                       2024-03-20                        US 611749                                Original Programming                                                        IntegrateDev2                       Jeyaprasanna                         2024-08-02

# MAGIC This job retrieves the MEWM_EFF_DT from Facets and compares the StartDate for each MBI  from  EAM and write the results to a file where the Dates do not match
# MAGIC Attach StartDate for each MBI from EAM. After the StartDate is attached, it is compared between FACETS and EAM and call out if they don't match.
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
from pyspark.sql.functions import col, lit, when, coalesce, to_date, date_format, rpad
from pyspark.sql.types import IntegerType
# COMMAND ----------
# MAGIC %run ../../../Utility_Integrate
# COMMAND ----------

# Parameter retrieval
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
EAMOwner = get_widget_value('EAMOwner','')
eam_secret_name = get_widget_value('eam_secret_name','')
EAMRPTOwner = get_widget_value('EAMRPTOwner','')
eamrpt_secret_name = get_widget_value('eamrpt_secret_name','')
RUNID = get_widget_value('RUNID','')
CurrDt = get_widget_value('CurrDt','')
GRP_ID = get_widget_value('GRP_ID','')

# EAM_DISC_ANALYSIS
jdbc_url, jdbc_props = get_db_config(eamrpt_secret_name)
extract_query = f"""
SELECT DISC.ID AS ID_SK,
       DISC.MBI,
       DISC.IDENTIFIEDBY,
       DISC.IDENTIFIEDDATE,
       DISC.DISCREPANCY as DISCREPANCY_ID
FROM {EAMRPTOwner}.DISCREPANCY_ANALYSIS DISC
INNER JOIN {EAMRPTOwner}.DISCREPANCY_CONFIG CONFIG
    ON DISC.DISCREPANCY = CONFIG.ID
WHERE STATUS = 'OPEN'
  AND DISC.IDENTIFIEDBY = 'CURR'
  AND CONFIG.DISCREPANCY_NAME = 'Kidney Transplant Start Date'
"""
df_EAM_DISC_ANALYSIS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# DISC_CONFIG
extract_query = f"""
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
  AND DISCREPANCY_NAME = 'Kidney Transplant Start Date'
"""
df_DISC_CONFIG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# EAM_tbENRLSpans
jdbc_url, jdbc_props = get_db_config(eam_secret_name)
extract_query = f"""
select DISTINCT
       spans.memberid,
       spans.HIC as MBI,
       spans.STARTDATE
from {EAMOwner}.tbENRLSpans spans
inner join {EAMOwner}.tbmemberinfo mi
       on spans.memcodnum=mi.memcodnum
where spans.spantype in ('KDTSP')
  and '{CurrDt}' BETWEEN SPANS.STARTDATE AND SPANS.ENDDATE
  and mi.GroupNumber in ({GRP_ID})
"""
df_EAM_tbENRLSpans = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# CMC_MEWM_ME_MSG
jdbc_url, jdbc_props = get_db_config(facets_secret_name)
extract_query = f"""
Select distinct
       GRGR.GRGR_ID,
       SBSB.SBSB_ID,
       LTRIM(RTRIM(ISNULL(MBR.MEME_HICN,''))) AS MBI,
       ISNULL(MSG.MEWM_EFF_DT,'') as MEWM_EFF_DT
from {FacetsOwner}.CMC_MEWM_ME_MSG  MSG
inner join {FacetsOwner}.CMC_MEME_MEMBER MBR
       on MBR.MEME_CK = MSG.MEME_CK
inner join {FacetsOwner}.CMC_SBSB_SUBSC SBSB
       on SBSB.SBSB_CK = MBR.SBSB_CK
inner join {FacetsOwner}.CMC_GRGR_GROUP GRGR
       on GRGR.GRGR_CK = SBSB.GRGR_CK
where GRGR.GRGR_ID IN ({GRP_ID})
  and MSG.WMDS_SEQ_NO in ('29')
  AND '{CurrDt}' BETWEEN MSG.MEWM_EFF_DT AND MSG.MEWM_TERM_DT
"""
df_CMC_MEWM_ME_MSG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# Att_EAM_tbENRLSpans (PxLookup: primary = MemAttr, lookup = Ref_EAM_tbENRLSpans)
df_Att_EAM_tbENRLSpans = (
    df_CMC_MEWM_ME_MSG.alias("MemAttr")
    .join(
        df_EAM_tbENRLSpans.alias("Ref_EAM_tbENRLSpans"),
        col("MemAttr.MBI") == col("Ref_EAM_tbENRLSpans.MBI"),
        "inner"
    )
    .select(
        col("MemAttr.GRGR_ID").alias("GRGR_ID"),
        col("MemAttr.SBSB_ID").alias("SBSB_ID"),
        col("MemAttr.MBI").alias("MBI"),
        col("MemAttr.MEWM_EFF_DT").alias("MEWM_EFF_DT"),
        col("Ref_EAM_tbENRLSpans.STARTDATE").alias("STARTDATE")
    )
)

# Xmr (CTransformerStage) => single output with constraint: STARTDATE <> MEWM_EFF_DT
df_Xmr = (
    df_Att_EAM_tbENRLSpans
    .filter(col("STARTDATE") != col("MEWM_EFF_DT"))
    .select(
        lit("Kidney Transplant Start Date").alias("Field"),
        col("GRGR_ID"),
        col("SBSB_ID"),
        col("MBI"),
        date_format(col("STARTDATE"), "MM/dd/yyyy").alias("STARTDATE"),
        date_format(col("MEWM_EFF_DT"), "MM/dd/yyyy").alias("MEWM_EFF_DT")
    )
)

# StrtDt_Config (PxLookup: primary = KDTSPStrtDt_Lkp, lookup = Ref_Disc_Config)
df_StrtDt_Config = (
    df_Xmr.alias("KDTSPStrtDt_Lkp")
    .join(
        df_DISC_CONFIG.alias("Ref_Disc_Config"),
        how="inner"  # no join condition => cross join
    )
    .select(
        col("KDTSPStrtDt_Lkp.Field").alias("Field"),
        col("KDTSPStrtDt_Lkp.GRGR_ID").alias("GRGR_ID"),
        col("KDTSPStrtDt_Lkp.SBSB_ID").alias("SBSB_ID"),
        col("KDTSPStrtDt_Lkp.MBI").alias("MBI"),
        col("Ref_Disc_Config.DISCREPANCY_ID").alias("DISCREPANCY_ID"),
        col("Ref_Disc_Config.DISCREPANCY_NAME").alias("DISCREPANCY_NAME"),
        col("Ref_Disc_Config.DISCREPANCY_DESC").alias("DISCREPANCY_DESC"),
        col("Ref_Disc_Config.IDENTIFIEDBY").alias("IDENTIFIEDBY"),
        col("Ref_Disc_Config.PRIORITY").alias("PRIORITY"),
        col("Ref_Disc_Config.ACTIVE").alias("ACTIVE"),
        col("KDTSPStrtDt_Lkp.STARTDATE").alias("SOURCE1"),
        col("KDTSPStrtDt_Lkp.MEWM_EFF_DT").alias("SOURCE2")
    )
)

# FullOuter (PxJoin: RuleFailures full outer join with DISC_Analysis on MBI, DISCREPANCY_ID, IDENTIFIEDBY)
df_FullOuter = (
    df_StrtDt_Config.alias("RuleFailures")
    .join(
        df_EAM_DISC_ANALYSIS.alias("DISC_Analysis"),
        (
            (col("RuleFailures.MBI") == col("DISC_Analysis.MBI")) &
            (col("RuleFailures.DISCREPANCY_ID") == col("DISC_Analysis.DISCREPANCY_ID")) &
            (col("RuleFailures.IDENTIFIEDBY") == col("DISC_Analysis.IDENTIFIEDBY"))
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

# Xmr1 (CTransformerStage) => three output links

# NewAdds constraint:
# rightRec_MBI = '' and coalesce(leftRec_MBI,'') <> ''
df_NewAdds = (
    df_FullOuter
    .filter(
        (col("rightRec_MBI") == "") &
        (coalesce(col("leftRec_MBI"), lit("")) != "")
    )
    .select(
        col("FACETS_GROUP_ID").alias("GRGR_ID"),
        col("FACETS_MemberID").alias("SBSB_ID"),
        col("leftRec_MBI").alias("MBI"),
        col("SOURCE1").alias("SOURCE1"),
        col("SOURCE2").alias("SOURCE2"),
        col("leftRec_IDENTIFIEDBY").alias("IDENTIFIEDBY"),
        to_date(lit(CurrDt), "yyyy-MM-dd").alias("IDENTIFIEDDATE"),
        lit(None).alias("RESOLVEDDATE"),
        lit("OPEN").alias("STATUS"),
        col("leftRec_DISCREPANCY_ID").alias("DISCREPANCY"),
        col("DISCREPANCY_DESC").alias("NOTE"),
        col("PRIORITY").alias("PRIORITY"),
        lit(0).alias("AGE"),
        when(
            (col("FACETS_MemberID") == ""),
            lit("N")
        ).otherwise(
            when(col("FACETS_MemberID").substr(1, 3) == lit("000"), lit("Y")).otherwise(lit("N"))
        ).alias("LEGACY"),
        to_date(lit(CurrDt), "yyyy-MM-dd").alias("LAST_UPDATED_DATE")
    )
)

# Resolved constraint:
# coalesce(leftRec_MBI,'') = '' and rightRec_MBI <> ''
df_Resolved = (
    df_FullOuter
    .filter(
        (coalesce(col("leftRec_MBI"), lit("")) == "") &
        (col("rightRec_MBI") != "")
    )
    .select(
        col("ID_SK").alias("ID_SK")
    )
)

# StillOpen constraint:
# coalesce(leftRec_MBI,'') <> '' and rightRec_MBI <> ''
df_StillOpen = (
    df_FullOuter
    .filter(
        (coalesce(col("leftRec_MBI"), lit("")) != "") &
        (col("rightRec_MBI") != "")
    )
    .select(
        col("ID_SK").alias("ID_SK"),
        DaysSinceFromDate2(to_date(lit(CurrDt), "yyyy-MM-dd"), col("IDENTIFIEDDATE")).alias("AGE"),
        to_date(lit(CurrDt), "yyyy-MM-dd").alias("LAST_UPDATED_DATE")
    )
)

# Aged (PxSequentialFile)
df_StillOpen_final = df_StillOpen.select("ID_SK", "AGE", "LAST_UPDATED_DATE")
write_files(
    df_StillOpen_final,
    f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_KDTSPSTRTDT_AGED.{RUNID}.dat",
    delimiter="|",
    mode="overwrite",
    is_parquet=False,
    header=True,
    quote="\"",
    nullValue=None
)

# Resolved (PxSequentialFile)
df_Resolved_final = df_Resolved.select("ID_SK")
write_files(
    df_Resolved_final,
    f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_KDTSPSTRTDT_RESOLVED.{RUNID}.dat",
    delimiter="|",
    mode="overwrite",
    is_parquet=False,
    header=True,
    quote="\"",
    nullValue=None
)

# RmDup (PxRemDup) => dedup on MBI, IDENTIFIEDBY, IDENTIFIEDDATE, DISCREPANCY
df_RmDup = dedup_sort(
    df_NewAdds,
    partition_cols=["MBI", "IDENTIFIEDBY", "IDENTIFIEDDATE", "DISCREPANCY"],
    sort_cols=[]
)

df_RmDup = df_RmDup.select(
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

# New (PxSequentialFile)
# Apply rpad for LEGACY (char(1))
df_New_final = df_RmDup.withColumn("LEGACY", rpad(col("LEGACY"), 1, " ")).select(
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

write_files(
    df_New_final,
    f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_KDTSPSTRTDT_APPEND.{RUNID}.dat",
    delimiter="|",
    mode="overwrite",
    is_parquet=False,
    header=True,
    quote="\"",
    nullValue=None
)