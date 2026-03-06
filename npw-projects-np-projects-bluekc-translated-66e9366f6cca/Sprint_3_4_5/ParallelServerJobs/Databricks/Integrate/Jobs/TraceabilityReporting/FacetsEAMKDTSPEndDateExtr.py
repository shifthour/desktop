# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC JOB NAME:  FacetsEAMKDTSPEndDateExtr
# MAGIC Calling Job:EAMFctsReconRptCntl
# MAGIC 
# MAGIC This job is retrieves the MEWM_TERM_DT  from Facets and compares the ENDDATE  for each MBI  from  EAM and write the results to a file where the Dates do not match.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC ========================================================================================================================================================================
# MAGIC Developer                          Date                           Project/Altiris #                              Change Description                                                    Development Project                 Reviewed By                          Reviewed On
# MAGIC ========================================================================================================================================================================
# MAGIC Arpitha V                      2024-03-20                          US 611749                                Original Programming                                                        IntegrateDev2                      Jeyaprasanna                         2024-08-02

# MAGIC This job is retrieves the MEWM_TERM_DT from Facets and compares the EndDate for each MBI  from  EAM and write the results to a file where the Dates do not match
# MAGIC Attach EndDate for each MBI from EAM. After the EndDate is attached, it is compared between FACETS and EAM and call out if they don't match.
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
from pyspark.sql.functions import col, lit, when, substring, concat, rpad
# COMMAND ----------
# MAGIC %run ../../../Utility_Integrate
# COMMAND ----------

jdbc_url_eamrpt, jdbc_props_eamrpt = get_db_config(get_widget_value('eamrpt_secret_name',''))
jdbc_url_eam, jdbc_props_eam = get_db_config(get_widget_value('eam_secret_name',''))
jdbc_url_facets, jdbc_props_facets = get_db_config(get_widget_value('facets_secret_name',''))

FacetsOwner = get_widget_value('FacetsOwner','')
EAMOwner = get_widget_value('EAMOwner','')
EAMRPTOwner = get_widget_value('EAMRPTOwner','')
RUNID = get_widget_value('RUNID','')
CurrDt = get_widget_value('CurrDt','')
GRP_ID = get_widget_value('GRP_ID','')

# Read EAM_DISC_ANALYSIS
query_EAM_DISC_ANALYSIS = (
    f"SELECT DISC.ID AS ID_SK, DISC.MBI, DISC.IDENTIFIEDBY, DISC.IDENTIFIEDDATE, DISC.DISCREPANCY as DISCREPANCY_ID "
    f"FROM {EAMRPTOwner}.DISCREPANCY_ANALYSIS DISC "
    f"INNER JOIN {EAMRPTOwner}.DISCREPANCY_CONFIG CONFIG "
    f"ON DISC.DISCREPANCY = CONFIG.ID "
    f"WHERE STATUS = 'OPEN' "
    f"AND DISC.IDENTIFIEDBY = 'CURR' "
    f"AND CONFIG.DISCREPANCY_NAME = 'Kidney Transplant End Date'"
)
df_EAM_DISC_ANALYSIS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_eamrpt)
    .options(**jdbc_props_eamrpt)
    .option("query", query_EAM_DISC_ANALYSIS)
    .load()
)

# Read DISC_CONFIG
query_DISC_CONFIG = (
    f"select ID AS DISCREPANCY_ID, DISCREPANCY_NAME, DISCREPANCY_DESC, IDENTIFIEDBY, PRIORITY, ACTIVE, SOURCE1, SOURCE2 "
    f"from {EAMRPTOwner}.DISCREPANCY_CONFIG "
    f"WHERE ACTIVE = 'Y' "
    f"AND IDENTIFIEDBY = 'CURR' "
    f"AND DISCREPANCY_NAME = 'Kidney Transplant End Date'"
)
df_DISC_CONFIG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_eamrpt)
    .options(**jdbc_props_eamrpt)
    .option("query", query_DISC_CONFIG)
    .load()
)

# Read EAM_tbENRLSpans
query_EAM_tbENRLSpans = (
    f"select DISTINCT spans.memberid, spans.HIC as MBI, spans.ENDDATE "
    f"from {EAMOwner}.tbENRLSpans spans "
    f"inner join {EAMOwner}.tbmemberinfo mi on spans.memcodnum=mi.memcodnum "
    f"where spans.spantype in ('KDTSP') "
    f"and '#CurrDt#' BETWEEN SPANS.STARTDATE AND SPANS.ENDDATE "
    f"and mi.GroupNumber in ({GRP_ID})"
)
df_EAM_tbENRLSpans = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_eam)
    .options(**jdbc_props_eam)
    .option("query", query_EAM_tbENRLSpans)
    .load()
)

# Read CMC_MEWM_ME_MSG
query_CMC_MEWM_ME_MSG = (
    f"Select distinct "
    f"GRGR.GRGR_ID, "
    f"SBSB.SBSB_ID, "
    f"LTRIM(RTRIM(ISNULL(MBR.MEME_HICN,''))) AS MBI, "
    f"ISNULL(MSG.MEWM_TERM_DT,'') as MEWM_TERM_DT "
    f"from {FacetsOwner}.CMC_MEWM_ME_MSG MSG "
    f"inner join {FacetsOwner}.CMC_MEME_MEMBER MBR on MBR.MEME_CK = MSG.MEME_CK "
    f"inner join {FacetsOwner}.CMC_SBSB_SUBSC SBSB on SBSB.SBSB_CK = MBR.SBSB_CK "
    f"inner join {FacetsOwner}.CMC_GRGR_GROUP GRGR on GRGR.GRGR_CK = SBSB.GRGR_CK "
    f"where GRGR.GRGR_ID IN ({GRP_ID}) "
    f"and MSG.WMDS_SEQ_NO in ('29') "
    f"AND '#CurrDt#' BETWEEN MSG.MEWM_EFF_DT AND MSG.MEWM_TERM_DT"
)
df_CMC_MEWM_ME_MSG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", query_CMC_MEWM_ME_MSG)
    .load()
)

# Att_EAM_tbENRLSpans (PxLookup: primary => CMC_MEWM_ME_MSG, lookup => EAM_tbENRLSpans, join on MBI)
df_Validate = (
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
        col("Ref_EAM_tbENRLSpans.ENDDATE").alias("ENDDATE"),
        col("MemAttr.MEWM_TERM_DT").alias("MEWM_TERM_DT")
    )
)

# Xmr (CTransformerStage)
# Constraint => ENDDATE <> MEWM_TERM_DT
df_Xmr_pre = df_Validate.filter(col("ENDDATE") != col("MEWM_TERM_DT"))
df_Xmr = df_Xmr_pre.select(
    col("GRGR_ID").alias("GRGR_ID"),
    col("SBSB_ID").alias("SBSB_ID"),
    lit("Kidney Transplant End Date").alias("Field"),
    col("MBI").alias("MBI"),
    concat(
        substring(col("ENDDATE"), 6, 2),
        lit("/"),
        substring(col("ENDDATE"), 9, 2),
        lit("/"),
        substring(col("ENDDATE"), 1, 4)
    ).alias("ENDDATE"),
    concat(
        substring(col("MEWM_TERM_DT"), 6, 2),
        lit("/"),
        substring(col("MEWM_TERM_DT"), 9, 2),
        lit("/"),
        substring(col("MEWM_TERM_DT"), 1, 4)
    ).alias("MEWM_TERM_DT")
)

# EndDt_Config (PxLookup: primary => Xmr, lookup => DISC_CONFIG, presumably join on Field = DISCREPANCY_NAME)
df_EndDt_Config = (
    df_Xmr.alias("KDTSPEndDt_Lkp")
    .join(
        df_DISC_CONFIG.alias("Ref_Disc_Config"),
        col("KDTSPEndDt_Lkp.Field") == col("Ref_Disc_Config.DISCREPANCY_NAME"),
        "inner"
    )
    .select(
        col("KDTSPEndDt_Lkp.Field").alias("Field"),
        col("KDTSPEndDt_Lkp.GRGR_ID").alias("GRGR_ID"),
        col("KDTSPEndDt_Lkp.SBSB_ID").alias("SBSB_ID"),
        col("KDTSPEndDt_Lkp.MBI").alias("MBI"),
        col("Ref_Disc_Config.DISCREPANCY_ID").alias("DISCREPANCY_ID"),
        col("Ref_Disc_Config.DISCREPANCY_NAME").alias("DISCREPANCY_NAME"),
        col("Ref_Disc_Config.DISCREPANCY_DESC").alias("DISCREPANCY_DESC"),
        col("Ref_Disc_Config.IDENTIFIEDBY").alias("IDENTIFIEDBY"),
        col("Ref_Disc_Config.PRIORITY").alias("PRIORITY"),
        col("Ref_Disc_Config.ACTIVE").alias("ACTIVE"),
        col("KDTSPEndDt_Lkp.ENDDATE").alias("SOURCE1"),
        col("KDTSPEndDt_Lkp.MEWM_TERM_DT").alias("SOURCE2")
    )
)

# FullOuter (PxJoin: fullouterjoin on MBI, DISCREPANCY_ID, IDENTIFIEDBY)
df_FullOuter = (
    df_EndDt_Config.alias("RuleFailures")
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

# Xmr1 (CTransformerStage)
#   NewAdds constraint => rightRec_MBI = '' AND leftRec_MBI != ''
df_NewAdds = df_FullOuter.filter(
    (col("rightRec_MBI") == "")
    & (col("leftRec_MBI").isNotNull())
    & (col("leftRec_MBI") != "")
).select(
    col("FACETS_GROUP_ID").alias("GRGR_ID"),
    col("FACETS_MemberID").alias("SBSB_ID"),
    col("leftRec_MBI").alias("MBI"),
    col("SOURCE1").alias("SOURCE1"),
    col("SOURCE2").alias("SOURCE2"),
    col("leftRec_IDENTIFIEDBY").alias("IDENTIFIEDBY"),
    # StringToDate(CurrDt, "%yyyy-%mm-%dd") -> user-defined
    # We preserve as literal call
    expr("StringToDate(CurrDt, \"%yyyy-%mm-%dd\")").alias("IDENTIFIEDDATE"),
    expr("SetNull()").alias("RESOLVEDDATE"),
    lit("OPEN").alias("STATUS"),
    col("leftRec_DISCREPANCY_ID").alias("DISCREPANCY"),
    col("DISCREPANCY_DESC").alias("NOTE"),
    col("PRIORITY").alias("PRIORITY"),
    lit(0).alias("AGE"),
    when(col("FACETS_MemberID") == "", "N")
    .otherwise(
        when(substring(col("FACETS_MemberID"), 1, 3) == "000", "Y").otherwise("N")
    ).alias("LEGACY"),
    expr(f"StringToDate(CurrDt, \"%yyyy-%mm-%dd\")").alias("LAST_UPDATED_DATE")
)

#   Resolved constraint => leftRec_MBI = '' AND rightRec_MBI != ''
df_Resolved = df_FullOuter.filter(
    (
        ((col("leftRec_MBI").isNull()) | (col("leftRec_MBI") == ""))
        & (col("rightRec_MBI") != "")
    )
).select(
    col("ID_SK").alias("ID_SK")
)

#   StillOpen constraint => leftRec_MBI != '' AND rightRec_MBI != ''
df_StillOpen = df_FullOuter.filter(
    (col("leftRec_MBI").isNotNull())
    & (col("leftRec_MBI") != "")
    & (col("rightRec_MBI") != "")
).select(
    col("ID_SK").alias("ID_SK"),
    # DaysSinceFromDate2(StringToDate(CurrDt, "%yyyy-%mm-%dd"), FinalXmr.IDENTIFIEDDATE)
    expr(f"DaysSinceFromDate2(StringToDate(CurrDt, \"%yyyy-%mm-%dd\"), IDENTIFIEDDATE)").alias("AGE"),
    expr(f"StringToDate(CurrDt, \"%yyyy-%mm-%dd\")").alias("LAST_UPDATED_DATE")
)

# Aged (PxSequentialFile) => write
# Column order: ID_SK, AGE, LAST_UPDATED_DATE
df_aged = df_StillOpen.select(
    col("ID_SK"),
    col("AGE"),
    col("LAST_UPDATED_DATE")
)
write_files(
    df_aged,
    f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_KDTSPENDDT_AGED.{RUNID}.dat",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)

# Resolved (PxSequentialFile) => write
# Column order: ID_SK
df_resolved = df_Resolved.select(
    col("ID_SK")
)
write_files(
    df_resolved,
    f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_KDTSPENDDT_RESOLVED.{RUNID}.dat",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)

# RmDup (PxRemDup: "RetainRecord=first", keys => MBI, IDENTIFIEDBY, IDENTIFIEDDATE, DISCREPANCY)
# Use dedup_sort
df_dedup = dedup_sort(
    df_NewAdds,
    ["MBI", "IDENTIFIEDBY", "IDENTIFIEDDATE", "DISCREPANCY"],
    []
)

# Select columns in correct order for "NewAddsFile"
df_NewAddsFile = df_dedup.select(
    col("GRGR_ID"),
    col("SBSB_ID"),
    col("MBI"),
    col("SOURCE1"),
    col("SOURCE2"),
    col("IDENTIFIEDBY"),
    col("IDENTIFIEDDATE"),
    col("RESOLVEDDATE"),
    col("STATUS"),
    col("DISCREPANCY"),
    col("NOTE"),
    col("PRIORITY"),
    col("AGE"),
    # LEGACY is char(1)
    rpad(col("LEGACY"), 1, " ").alias("LEGACY"),
    col("LAST_UPDATED_DATE")
)

# New (PxSequentialFile) => write
write_files(
    df_NewAddsFile,
    f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_KDTSPENDDT_APPEND.{RUNID}.dat",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)