# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC JOB NAME:  FacetsEAMESRDEndDateExtr
# MAGIC Calling Job:EAMFctsReconRptCntl
# MAGIC 
# MAGIC This job is retrieves the MEMD_HCFA_TERM_DT  from Facets and compares the ENDDATE  for each MBI  from  EAM and write the results to a file where the Dates do not match.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC ========================================================================================================================================================================
# MAGIC Developer                          Date                           Project/Altiris #                              Change Description                                                    Development Project                 Reviewed By                          Reviewed On
# MAGIC ========================================================================================================================================================================
# MAGIC Arpitha V                      2024-03-20                          US 611748                                Original Programming                                                        IntegrateDev2                      Jeyaprasanna                          2024-08-02

# MAGIC This job is retrieves the MEMD_HCFA_TERM_DT from Facets and compares the EndDate for each MBI  from  EAM and write the results to a file where the Dates do not match
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
from pyspark.sql.functions import col, lit, when, substring, concat_ws, to_date, date_format, rpad
from pyspark.sql.types import StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../Utility_Integrate
# COMMAND ----------

# MAGIC %run ../../../shared_containers/PrimaryKey/<shared_container_name>  # Example if shared containers existed; none used here.
# COMMAND ----------

# Parameters
FacetsOwner = get_widget_value('$FacetsOwner','')
EAMOwner = get_widget_value('$EAMOwner','')
EAMRPTOwner = get_widget_value('$EAMRPTOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
eam_secret_name = get_widget_value('eam_secret_name','')
eamrpt_secret_name = get_widget_value('eamrpt_secret_name','')
RUNID = get_widget_value('RUNID','')
CurrDt = get_widget_value('CurrDt','')
GRP_ID = get_widget_value('GRP_ID','')

# Database reads
jdbc_url_EAMRPT, jdbc_props_EAMRPT = get_db_config(eamrpt_secret_name)
extract_query_EAM_DISC_ANALYSIS = f"""SELECT  DISC.ID AS ID_SK,DISC.MBI,DISC.IDENTIFIEDBY,DISC.IDENTIFIEDDATE,DISC.DISCREPANCY as DISCREPANCY_ID
FROM {EAMRPTOwner}.DISCREPANCY_ANALYSIS DISC
INNER JOIN {EAMRPTOwner}.DISCREPANCY_CONFIG CONFIG
ON DISC.DISCREPANCY = CONFIG.ID
WHERE STATUS = 'OPEN' 
and DISC.IDENTIFIEDBY = 'CURR'
and CONFIG.DISCREPANCY_NAME = 'ESRD End Date'"""
df_EAM_DISC_ANALYSIS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_EAMRPT)
    .options(**jdbc_props_EAMRPT)
    .option("query", extract_query_EAM_DISC_ANALYSIS)
    .load()
)

extract_query_DISC_CONFIG = f"""select ID AS DISCREPANCY_ID,
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
AND DISCREPANCY_NAME = 'ESRD End Date'"""
df_DISC_CONFIG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_EAMRPT)
    .options(**jdbc_props_EAMRPT)
    .option("query", extract_query_DISC_CONFIG)
    .load()
)

jdbc_url_EAM, jdbc_props_EAM = get_db_config(eam_secret_name)
extract_query_EAM_tbENRLSpans = f"""select  DISTINCT spans.memberid , spans.HIC as MBI, spans.ENDDATE
from {EAMOwner}.tbENRLSpans spans
inner join {EAMOwner}.tbmemberinfo mi on spans.memcodnum=mi.memcodnum
where spans.spantype in ('ESRD')
and '{CurrDt}' BETWEEN spans.STARTDATE AND spans.ENDDATE
and mi.GroupNumber in ({GRP_ID})"""
df_EAM_tbENRLSpans = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_EAM)
    .options(**jdbc_props_EAM)
    .option("query", extract_query_EAM_tbENRLSpans)
    .load()
)

jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
extract_query_CMC_MEMD_MECR_DETL = f"""Select distinct 
GRGR.GRGR_ID,
SBSB.SBSB_ID,
LTRIM(RTRIM(ISNULL(MBR.MEME_HICN,''))) AS MBI,
MEMD.MEMD_HCFA_TERM_DT 
from  {FacetsOwner}.CMC_MEMD_MECR_DETL  MEMD 
inner join {FacetsOwner}.CMC_MEME_MEMBER MBR on MBR.MEME_CK = MEMD.MEME_CK
inner join {FacetsOwner}.CMC_SBSB_SUBSC SBSB on SBSB.SBSB_CK = MBR.SBSB_CK
inner join {FacetsOwner}.CMC_GRGR_GROUP GRGR on GRGR.GRGR_CK = SBSB.GRGR_CK
where GRGR.GRGR_ID IN ({GRP_ID}) 
and MEMD.MEMD_EVENT_CD in ('ESRD')                
AND '{CurrDt}' BETWEEN MEMD.MEMD_HCFA_EFF_DT AND MEMD.MEMD_HCFA_TERM_DT"""
df_CMC_MEMD_MECR_DETL = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_CMC_MEMD_MECR_DETL)
    .load()
)

# Att_EAM_tbENRLSpans (PxLookup with cross join, primary=MemAttr, lookup=Ref_EAM_tbENRLSpans)
df_Att_EAM_tbENRLSpans = df_CMC_MEMD_MECR_DETL.alias("MemAttr").crossJoin(
    df_EAM_tbENRLSpans.alias("Ref_EAM_tbENRLSpans")
).select(
    col("MemAttr.GRGR_ID").alias("GRGR_ID"),
    col("MemAttr.SBSB_ID").alias("SBSB_ID"),
    col("MemAttr.MBI").alias("MBI"),
    col("Ref_EAM_tbENRLSpans.ENDDATE").alias("ENDDATE"),
    col("MemAttr.MEMD_HCFA_TERM_DT").alias("MEMD_HCFA_TERM_DT")
)

# Xmr (CTransformerStage)
df_Xmr = df_Att_EAM_tbENRLSpans.withColumn(
    "svEndDt", date_format(col("ENDDATE"), "yyyy-MM-dd")
).withColumn(
    "svTermDt", date_format(col("MEMD_HCFA_TERM_DT"), "yyyy-MM-dd")
)

df_EsrdEndDt_Lkp = df_Xmr.filter(
    col("ENDDATE") != col("MEMD_HCFA_TERM_DT")
).select(
    lit("ESRD End Date").alias("Field"),
    col("GRGR_ID").alias("GRGR_ID"),
    col("SBSB_ID").alias("SBSB_ID"),
    col("MBI").alias("MBI"),
    concat_ws("/", substring(col("svEndDt"),6,2), substring(col("svEndDt"),9,2), substring(col("svEndDt"),1,4)).alias("ENDDATE"),
    concat_ws("/", substring(col("svTermDt"),6,2), substring(col("svTermDt"),9,2), substring(col("svTermDt"),1,4)).alias("MEMD_HCFA_TERM_DT")
)

# EndDt_Config (PxLookup cross join)
df_RuleFailures = df_EsrdEndDt_Lkp.alias("EsrdEndDt_Lkp").crossJoin(
    df_DISC_CONFIG.alias("Ref_Disc_Config")
).select(
    col("EsrdEndDt_Lkp.Field").alias("Field"),
    col("EsrdEndDt_Lkp.GRGR_ID").alias("GRGR_ID"),
    col("EsrdEndDt_Lkp.SBSB_ID").alias("SBSB_ID"),
    col("EsrdEndDt_Lkp.MBI").alias("MBI"),
    col("Ref_Disc_Config.DISCREPANCY_ID").alias("DISCREPANCY_ID"),
    col("Ref_Disc_Config.DISCREPANCY_NAME").alias("DISCREPANCY_NAME"),
    col("Ref_Disc_Config.DISCREPANCY_DESC").alias("DISCREPANCY_DESC"),
    col("Ref_Disc_Config.IDENTIFIEDBY").alias("IDENTIFIEDBY"),
    col("Ref_Disc_Config.PRIORITY").alias("PRIORITY"),
    col("Ref_Disc_Config.ACTIVE").alias("ACTIVE"),
    col("EsrdEndDt_Lkp.ENDDATE").alias("SOURCE1"),
    col("EsrdEndDt_Lkp.MEMD_HCFA_TERM_DT").alias("SOURCE2")
)

# FullOuter (PxJoin on MBI, DISCREPANCY_ID, IDENTIFIEDBY)
df_FullOuter = df_RuleFailures.alias("RuleFailures").join(
    df_EAM_DISC_ANALYSIS.alias("DISC_Analysis"),
    [
        col("RuleFailures.MBI") == col("DISC_Analysis.MBI"),
        col("RuleFailures.DISCREPANCY_ID") == col("DISC_Analysis.DISCREPANCY_ID"),
        col("RuleFailures.IDENTIFIEDBY") == col("DISC_Analysis.IDENTIFIEDBY")
    ],
    how="fullouter"
).select(
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

# Xmr1 (CTransformerStage) - 3 output links
df_NewAdds = df_FullOuter.filter(
    (col("rightRec_MBI") == "") & (col("leftRec_MBI") != "")
).select(
    col("FACETS_GROUP_ID").alias("GRGR_ID"),
    col("FACETS_MemberID").alias("SBSB_ID"),
    col("leftRec_MBI").alias("MBI"),
    col("SOURCE1").alias("SOURCE1"),
    col("SOURCE2").alias("SOURCE2"),
    col("leftRec_IDENTIFIEDBY").alias("IDENTIFIEDBY"),
    to_date(lit(CurrDt),"yyyy-MM-dd").alias("IDENTIFIEDDATE"),
    lit(None).alias("RESOLVEDDATE"),
    lit("OPEN").alias("STATUS"),
    col("leftRec_DISCREPANCY_ID").alias("DISCREPANCY"),
    col("DISCREPANCY_DESC").alias("NOTE"),
    col("PRIORITY").alias("PRIORITY"),
    lit(0).alias("AGE"),
    when(col("FACETS_MemberID") == "", lit("N")).otherwise(
        when(substring(col("FACETS_MemberID"),1,3) == "000", lit("Y")).otherwise(lit("N"))
    ).alias("LEGACY"),
    to_date(lit(CurrDt),"yyyy-MM-dd").alias("LAST_UPDATED_DATE")
)

df_Resolved = df_FullOuter.filter(
    (col("leftRec_MBI") == "") & (col("rightRec_MBI") != "")
).select(
    col("ID_SK").alias("ID_SK")
)

df_StillOpen = df_FullOuter.filter(
    (col("leftRec_MBI") != "") & (col("rightRec_MBI") != "")
).select(
    col("ID_SK").alias("ID_SK"),
    DaysSinceFromDate2(to_date(lit(CurrDt),"yyyy-MM-dd"), col("IDENTIFIEDDATE")).alias("AGE"),
    to_date(lit(CurrDt),"yyyy-MM-dd").alias("LAST_UPDATED_DATE")
)

# Aged (PxSequentialFile)
df_StillOpen_final = df_StillOpen.select("ID_SK","AGE","LAST_UPDATED_DATE")
write_files(
    df_StillOpen_final,
    f"FACETS_EAM_DISC_ANALYSIS_ESRDENDDT_AGED.{RUNID}.dat",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)

# Resolved (PxSequentialFile)
df_Resolved_final = df_Resolved.select("ID_SK")
write_files(
    df_Resolved_final,
    f"FACETS_EAM_DISC_ANALYSIS_ESRDENDDT_RESOLVED.{RUNID}.dat",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)

# RmDup (PxRemDup) - deduplicate new adds
df_RmDup = dedup_sort(
    df_NewAdds,
    partition_cols=["MBI","IDENTIFIEDBY","IDENTIFIEDDATE","DISCREPANCY"],
    sort_cols=[]
)

df_RmDupOut = df_RmDup.select(
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
    col("LEGACY").alias("LEGACY"),
    col("LAST_UPDATED_DATE").alias("LAST_UPDATED_DATE")
)

# New (PxSequentialFile)
df_New_final = df_RmDupOut.select(
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
    rpad(col("LEGACY"), 1, " ").alias("LEGACY"),
    col("LAST_UPDATED_DATE")
)
write_files(
    df_New_final,
    f"FACETS_EAM_DISC_ANALYSIS_ESRDENDDT_APPEND.{RUNID}.dat",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)