# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC JOB NAME:  FacetsEAMESRDStartDateExtr
# MAGIC Calling Job:EAMFctsReconRptCntl
# MAGIC 
# MAGIC This job is retrieves the MEMD_HCFA_EFF_DT  from Facets and compares the STARTDATE for each MBI  from  EAM and write the results to a file where the Dates do not match.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC ========================================================================================================================================================================
# MAGIC Developer                          Date                           Project/Altiris #                              Change Description                                                    Development Project                 Reviewed By                          Reviewed On
# MAGIC ========================================================================================================================================================================
# MAGIC Arpitha V                       2024-03-20                        US 611748                                Original Programming                                                        IntegrateDev2                      Jeyaprasanna                          2024-06-21

# MAGIC This job is retrieves the MEMD_HCFA_EFF_DT from Facets and compares the StartDate for each MBI  from  EAM and write the results to a file where the Dates do not match
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
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
# COMMAND ----------
# MAGIC %run ../../../Utility_Integrate
# COMMAND ----------


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
extract_query_eam_disc_analysis = f"""
SELECT DISC.ID AS ID_SK,
       DISC.MBI,
       DISC.IDENTIFIEDBY,
       DISC.IDENTIFIEDDATE,
       DISC.DISCREPANCY AS DISCREPANCY_ID
FROM {EAMRPTOwner}.DISCREPANCY_ANALYSIS DISC
INNER JOIN {EAMRPTOwner}.DISCREPANCY_CONFIG CONFIG
    ON DISC.DISCREPANCY = CONFIG.ID
WHERE STATUS = 'OPEN'
  AND DISC.IDENTIFIEDBY = 'CURR'
  AND CONFIG.DISCREPANCY_NAME = 'ESRD Start Date'
"""
df_EAM_DISC_ANALYSIS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_eamrpt)
    .options(**jdbc_props_eamrpt)
    .option("query", extract_query_eam_disc_analysis)
    .load()
)

# DISC_CONFIG (ODBCConnectorPX)
extract_query_disc_config = f"""
SELECT ID AS DISCREPANCY_ID,
       DISCREPANCY_NAME,
       DISCREPANCY_DESC,
       IDENTIFIEDBY,
       PRIORITY,
       ACTIVE,
       SOURCE1,
       SOURCE2
FROM {EAMRPTOwner}.DISCREPANCY_CONFIG
WHERE ACTIVE = 'Y'
  AND IDENTIFIEDBY = 'CURR'
  AND DISCREPANCY_NAME = 'ESRD Start Date'
"""
df_DISC_CONFIG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_eamrpt)
    .options(**jdbc_props_eamrpt)
    .option("query", extract_query_disc_config)
    .load()
)

# EAM_tbENRLSpans (ODBCConnectorPX)
jdbc_url_eam, jdbc_props_eam = get_db_config(eam_secret_name)
extract_query_eam_tbENRLSpans = f"""
SELECT DISTINCT spans.memberid,
       spans.HIC AS MBI,
       spans.STARTDATE
FROM {EAMOwner}.tbENRLSpans spans
INNER JOIN {EAMOwner}.tbmemberinfo mi
    ON spans.memcodnum = mi.memcodnum
WHERE spans.spantype IN ('ESRD')
  AND '{CurrDt}' BETWEEN spans.STARTDATE AND spans.ENDDATE
  AND mi.GroupNumber IN ({GRP_ID})
"""
df_EAM_tbENRLSpans = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_eam)
    .options(**jdbc_props_eam)
    .option("query", extract_query_eam_tbENRLSpans)
    .load()
)

# CMC_MEMD_MECR_DETL (ODBCConnectorPX)
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
extract_query_cmc_memd_mecr_detl = f"""
SELECT DISTINCT
       GRGR.GRGR_ID,
       SBSB.SBSB_ID,
       LTRIM(RTRIM(ISNULL(MBR.MEME_HICN,''))) AS MBI,
       ISNULL(MEMD.MEMD_HCFA_EFF_DT,'') AS MEMD_HCFA_EFF_DT
FROM {FacetsOwner}.CMC_MEMD_MECR_DETL MEMD
INNER JOIN {FacetsOwner}.CMC_MEME_MEMBER MBR
    ON MBR.MEME_CK = MEMD.MEME_CK
INNER JOIN {FacetsOwner}.CMC_SBSB_SUBSC SBSB
    ON SBSB.SBSB_CK = MBR.SBSB_CK
INNER JOIN {FacetsOwner}.CMC_GRGR_GROUP GRGR
    ON GRGR.GRGR_CK = SBSB.GRGR_CK
WHERE GRGR.GRGR_ID IN ({GRP_ID})
  AND MEMD.MEMD_EVENT_CD IN ('ESRD')
  AND '{CurrDt}' BETWEEN MEMD.MEMD_HCFA_EFF_DT AND MEMD.MEMD_HCFA_TERM_DT
"""
df_CMC_MEMD_MECR_DETL = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_cmc_memd_mecr_detl)
    .load()
)

# Att_EAM_tbENRLSpans (PxLookup) - cross join with join type "inner"
df_Validate = (
    df_CMC_MEMD_MECR_DETL.alias("MemAttr")
    .join(df_EAM_tbENRLSpans.alias("Ref_EAM_tbENRLSpans"), how="inner")
    .select(
        F.col("MemAttr.GRGR_ID").alias("GRGR_ID"),
        F.col("MemAttr.SBSB_ID").alias("SBSB_ID"),
        F.col("MemAttr.MBI").alias("MBI"),
        F.col("MemAttr.MEMD_HCFA_EFF_DT").alias("MEMD_HCFA_EFF_DT"),
        F.col("Ref_EAM_tbENRLSpans.STARTDATE").alias("STARTDATE")
    )
)

# Xmr (CTransformerStage)
df_xmr = (
    df_Validate
    .withColumn("svStartDt", F.call_udf("FORMAT_DATE_EE", F.col("STARTDATE"), F.lit("SYBASE"), F.lit("TIMESTAMP"), F.lit("CCYY-MM-DD")))
    .withColumn("svEffDt", F.call_udf("FORMAT_DATE_EE", F.col("MEMD_HCFA_EFF_DT"), F.lit("SYBASE"), F.lit("TIMESTAMP"), F.lit("CCYY-MM-DD")))
)
df_xmr_filtered = df_xmr.filter(F.col("STARTDATE") != F.col("MEMD_HCFA_EFF_DT"))
df_EsrdStrtDt_Lkp = df_xmr_filtered.select(
    F.lit("ESRD Start Date").alias("Field"),
    F.col("GRGR_ID").alias("GRGR_ID"),
    F.col("SBSB_ID").alias("SBSB_ID"),
    F.col("MBI").alias("MBI"),
    F.concat(
        F.substring("svStartDt", 6, 2),
        F.lit("/"),
        F.substring("svStartDt", 9, 2),
        F.lit("/"),
        F.substring("svStartDt", 1, 4)
    ).alias("STARTDATE"),
    F.concat(
        F.substring("svEffDt", 6, 2),
        F.lit("/"),
        F.substring("svEffDt", 9, 2),
        F.lit("/"),
        F.substring("svEffDt", 1, 4)
    ).alias("MEMD_HCFA_EFF_DT")
)

# StrtDt_Config (PxLookup) - cross join with join type "inner"
df_StrtDt_Config = (
    df_EsrdStrtDt_Lkp.alias("EsrdStrtDt_Lkp")
    .join(df_DISC_CONFIG.alias("Ref_Disc_Config"), how="inner")
    .select(
        F.col("EsrdStrtDt_Lkp.Field").alias("Field"),
        F.col("EsrdStrtDt_Lkp.GRGR_ID").alias("GRGR_ID"),
        F.col("EsrdStrtDt_Lkp.SBSB_ID").alias("SBSB_ID"),
        F.col("EsrdStrtDt_Lkp.MBI").alias("MBI"),
        F.col("Ref_Disc_Config.DISCREPANCY_ID").alias("DISCREPANCY_ID"),
        F.col("Ref_Disc_Config.DISCREPANCY_NAME").alias("DISCREPANCY_NAME"),
        F.col("Ref_Disc_Config.DISCREPANCY_DESC").alias("DISCREPANCY_DESC"),
        F.col("Ref_Disc_Config.IDENTIFIEDBY").alias("IDENTIFIEDBY"),
        F.col("Ref_Disc_Config.PRIORITY").alias("PRIORITY"),
        F.col("Ref_Disc_Config.ACTIVE").alias("ACTIVE"),
        F.col("EsrdStrtDt_Lkp.STARTDATE").alias("SOURCE1"),
        F.col("EsrdStrtDt_Lkp.MEMD_HCFA_EFF_DT").alias("SOURCE2")
    )
)
df_RuleFailures = df_StrtDt_Config

# FullOuter (PxJoin)
df_FullOuter = df_RuleFailures.alias("RuleFailures").join(
    df_EAM_DISC_ANALYSIS.alias("DISC_Analysis"),
    (F.col("RuleFailures.MBI") == F.col("DISC_Analysis.MBI")) &
    (F.col("RuleFailures.DISCREPANCY_ID") == F.col("DISC_Analysis.DISCREPANCY_ID")) &
    (F.col("RuleFailures.IDENTIFIEDBY") == F.col("DISC_Analysis.IDENTIFIEDBY")),
    how="fullouter"
)
df_FinalXmr = df_FullOuter.select(
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
    F.col("DISC_Analysis.DISCREPANCY_ID").alias("rightRec_DISCREPANCY_ID")
)

# Xmr1 (CTransformerStage) - Three output links
df_NewAdds = df_FinalXmr.filter(
    (F.col("rightRec_MBI") == "") &
    (F.when(F.col("leftRec_MBI").isNotNull(), F.col("leftRec_MBI")).otherwise("") != "")
).select(
    F.col("FACETS_GROUP_ID").alias("GRGR_ID"),
    F.col("FACETS_MemberID").alias("SBSB_ID"),
    F.col("leftRec_MBI").alias("MBI"),
    F.col("SOURCE1").alias("SOURCE1"),
    F.col("SOURCE2").alias("SOURCE2"),
    F.col("leftRec_IDENTIFIEDBY").alias("IDENTIFIEDBY"),
    F.call_udf("StringToDate", F.lit(CurrDt), F.lit("%yyyy-%mm-%dd")).alias("IDENTIFIEDDATE"),
    F.lit(None).alias("RESOLVEDDATE"),
    F.lit("OPEN").alias("STATUS"),
    F.col("leftRec_DISCREPANCY_ID").alias("DISCREPANCY"),
    F.col("DISCREPANCY_DESC").alias("NOTE"),
    F.col("PRIORITY").alias("PRIORITY"),
    F.lit(0).alias("AGE"),
    F.when(F.col("FACETS_MemberID") == "", "N").otherwise(
        F.when(F.substring("FACETS_MemberID", 1, 3) == "000", "Y").otherwise("N")
    ).alias("LEGACY"),
    F.call_udf("StringToDate", F.lit(CurrDt), F.lit("%yyyy-%mm-%dd")).alias("LAST_UPDATED_DATE")
)

df_Resolved = df_FinalXmr.filter(
    (F.when(F.col("leftRec_MBI").isNotNull(), F.col("leftRec_MBI")).otherwise("")) == "" &
    (F.col("rightRec_MBI") != "")
).select(
    F.col("ID_SK").alias("ID_SK")
)

df_StillOpen = df_FinalXmr.filter(
    (F.when(F.col("leftRec_MBI").isNotNull(), F.col("leftRec_MBI")).otherwise("")) != "" &
    (F.col("rightRec_MBI") != "")
).select(
    F.col("ID_SK").alias("ID_SK"),
    F.call_udf(
        "DaysSinceFromDate2",
        F.call_udf("StringToDate", F.lit(CurrDt), F.lit("%yyyy-%mm-%dd")),
        F.col("IDENTIFIEDDATE")
    ).alias("AGE"),
    F.call_udf("StringToDate", F.lit(CurrDt), F.lit("%yyyy-%mm-%dd")).alias("LAST_UPDATED_DATE")
)

# Aged (PxSequentialFile)
df_StillOpen_write = df_StillOpen.select("ID_SK", "AGE", "LAST_UPDATED_DATE")
write_files(
    df_StillOpen_write,
    f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_ESRDSTRTDT_AGED.{RUNID}.dat",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote='"',
    nullValue=None
)

# Resolved (PxSequentialFile)
df_Resolved_write = df_Resolved.select("ID_SK")
write_files(
    df_Resolved_write,
    f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_ESRDSTRTDT_RESOLVED.{RUNID}.dat",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote='"',
    nullValue=None
)

# RmDup (PxRemDup)
df_RmDup = dedup_sort(
    df_NewAdds,
    partition_cols=["MBI","IDENTIFIEDBY","IDENTIFIEDDATE","DISCREPANCY"],
    sort_cols=[("MBI","A"),("IDENTIFIEDBY","A"),("IDENTIFIEDDATE","A"),("DISCREPANCY","A")]
)
df_NewAddsFile = df_RmDup.select(
    F.col("GRGR_ID"),
    F.col("SBSB_ID"),
    F.col("MBI"),
    F.col("SOURCE1"),
    F.col("SOURCE2"),
    F.col("IDENTIFIEDBY"),
    F.col("IDENTIFIEDDATE"),
    F.col("RESOLVEDDATE"),
    F.col("STATUS"),
    F.col("DISCREPANCY"),
    F.col("NOTE"),
    F.col("PRIORITY"),
    F.col("AGE"),
    F.rpad(F.col("LEGACY"), 1, " ").alias("LEGACY"),
    F.col("LAST_UPDATED_DATE")
)

# New (PxSequentialFile)
write_files(
    df_NewAddsFile,
    f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_ESRDSTRTDT_APPEND.{RUNID}.dat",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote='"',
    nullValue=None
)