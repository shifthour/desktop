# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC JOB NAME:  FacetsEAMRxIDPBPExtr
# MAGIC Calling Job:EAMFctsReconRptCntl
# MAGIC 
# MAGIC This job validates the first 3 digits of Primary RXID and writes a disrepancy  to discrepancy_analysis table. Compares the data qualified for current run with already existing data and creates load files accordingly.
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC ================================================================================================================================================================================================
# MAGIC Developer                          Date                           Project/Altiris #                              Change Description                                                                                      Development Project                         Reviewer                     Review Date
# MAGIC ================================================================================================================================================================================================
# MAGIC ReddySanam               2021-02-16                  US329820                                         Original Programming                                                                                       IntegrateDev2                            Jeyaprasanna                    2021-02-17
# MAGIC JohnAbraham               2021-08-11                  US391328                 Added EAMRPT Env variables and updated appropriate tables   with these parameters        IntegrateDev1
# MAGIC Prabhu ES                    2022-03-21                  S2S                                                 Unused Parameters removed                                                                             IntegrateDev5	Ken Bradmon	2022-06-04

# MAGIC This job validates the first 3 digits of Primary RXID and writes a disrepancy  to discrepancy_analysis table. Compares the data qualified for current run with already existing data and creates load files accordingly.
# MAGIC If a Specific MBI is already reported on RXIDMismatch. We should add that as RXID TC 72 required Mismatch. so this path will have the RXID Mismatched under BLMM category so that  they will be sent down to Aged/Resolved path and will be dropepd in the transformer Xmr1.
# MAGIC Since we need to drop if RXID Mismatch is already avaliable for an MBI, both RXID TC 72 and RXID Mismatch are read with same discrepancy code 119. The ID column is treated as 0 for RXIDMismatch so that those rows will only used of join and will be dropped in Xmr1
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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import col, lit, when, substring, to_date, rpad
# COMMAND ----------
# MAGIC %run ../../../Utility_Integrate
# COMMAND ----------


EAMRPTOwner = get_widget_value('EAMRPTOwner','')
eamrpt_secret_name = get_widget_value('eamrpt_secret_name','')
EAMOwner = get_widget_value('EAMOwner','')
eam_secret_name = get_widget_value('eam_secret_name','')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
RUNID = get_widget_value('RUNID','')
BillDt = get_widget_value('BillDt','')
CurrDt = get_widget_value('CurrDt','')

# DISC_CONFIG (ODBCConnectorPX)
jdbc_url_DISC_CONFIG, jdbc_props_DISC_CONFIG = get_db_config(eamrpt_secret_name)
extract_query_DISC_CONFIG = f"""
select
  ID AS DISCREPANCY_ID,
  DISCREPANCY_NAME,
  DISCREPANCY_DESC,
  IDENTIFIEDBY,
  PRIORITY,
  ACTIVE,
  SOURCE1,
  SOURCE2
from {EAMRPTOwner}.DISCREPANCY_CONFIG
WHERE ACTIVE = 'Y'
AND IDENTIFIEDBY = 'BLMM'
AND DISCREPANCY_NAME = 'RXID TC 72'
"""
df_DISC_CONFIG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_DISC_CONFIG)
    .options(**jdbc_props_DISC_CONFIG)
    .option("query", extract_query_DISC_CONFIG)
    .load()
)

# EAM_DISC_ANALYSIS (ODBCConnectorPX)
jdbc_url_EAM_DISC_ANALYSIS, jdbc_props_EAM_DISC_ANALYSIS = get_db_config(eamrpt_secret_name)
extract_query_EAM_DISC_ANALYSIS = f"""
SELECT
  CASE WHEN CONFIG.DISCREPANCY_NAME = 'RXID TC 72' then DISC.ID ELSE 0 END  AS ID_SK,
  DISC.MBI,
  DISC.IDENTIFIEDBY,
  DISC.IDENTIFIEDDATE,
  119 as DISCREPANCY_ID
FROM {EAMRPTOwner}.DISCREPANCY_ANALYSIS DISC
INNER JOIN {EAMRPTOwner}.DISCREPANCY_CONFIG CONFIG
  ON DISC.DISCREPANCY = CONFIG.ID
WHERE STATUS = 'OPEN'
  and DISC.IDENTIFIEDBY = 'BLMM'
  and CONFIG.DISCREPANCY_NAME in ('RXID TC 72','PrimaryRXID')
"""
df_EAM_DISC_ANALYSIS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_EAM_DISC_ANALYSIS)
    .options(**jdbc_props_EAM_DISC_ANALYSIS)
    .option("query", extract_query_EAM_DISC_ANALYSIS)
    .load()
)

# EAM_RXIDTC72 (ODBCConnectorPX)
jdbc_url_EAM_RXIDTC72, jdbc_props_EAM_RXIDTC72 = get_db_config(eam_secret_name)
extract_query_EAM_RXIDTC72 = f"""
SELECT DISTINCT
  groups.GRGR_ID AS EAM_GroupID,
  LTRIM(RTRIM(ISNULL(mbrs.MemberID,''))) AS EAM_MemberID,
  LTRIM(RTRIM(ISNULL(mbrs.HIC,''))) AS MBI,
  LTRIM(RTRIM(ISNULL(info.RxID,''))) AS EAM_PrimaryRXID,
  case
    WHEN mbrs.PlanID='H1352' and substring(LTRIM(RTRIM(ISNULL(info.RxID,''))),1,3) <> 'RRK' then 'RRK'
    WHEN mbrs.PlanID= 'H6502' and mbrs.PBP <> '003' and substring(LTRIM(RTRIM(ISNULL(info.RxID,''))),1,3) <> 'RKN' THEN 'RKN'
    ELSE ''
  end AS prefix
FROM {EAMOwner}.tbMemberInfo info
INNER JOIN {EAMOwner}.tbEENRLMembers mbrs ON info.MemCodNum = mbrs.MemCodNum
INNER JOIN {EAMOwner}.tbENRLSpans eff on eff.MemCodNum= mbrs.MemCodNum AND eff.SpanType='EFF'
INNER JOIN {EAMOwner}.GROUPS ON Groups.GroupID = info.GroupID
WHERE
  (
    (mbrs.PlanID='H1352' and substring(LTRIM(RTRIM(ISNULL(info.RxID,''))),1,3) <> 'RRK')
    or (mbrs.PlanID= 'H6502' and mbrs.PBP <> '003' and substring(LTRIM(RTRIM(ISNULL(info.RxID,''))),1,3) <> 'RKN')
  )
  and LTRIM(RTRIM(ISNULL(info.RxID,''))) <> ''
  and '{BillDt}' BETWEEN eff.StartDate AND eff.EndDate
"""
df_EAM_RXIDTC72 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_EAM_RXIDTC72)
    .options(**jdbc_props_EAM_RXIDTC72)
    .option("query", extract_query_EAM_RXIDTC72)
    .load()
)

# Xmr (Transformer)
df_Xmr = df_EAM_RXIDTC72.select(
    lit("RXID TC 72").alias("Field"),
    col("EAM_GroupID").alias("GRGR_ID"),
    col("EAM_MemberID").alias("SBSB_ID"),
    col("MBI").alias("MBI"),
    when(trim(col("EAM_PrimaryRXID")) == "", lit("")).otherwise(col("EAM_PrimaryRXID")).alias("EAM_PrimaryRXID"),
    when(trim(col("prefix")) == "", lit("")).otherwise(col("prefix")).alias("Prefix")
)

# Att_Config (PxLookup) - Join/cross with DISC_CONFIG
df_Att_Config = (
    df_Xmr.alias("XmrO_Lkp")
    .join(df_DISC_CONFIG.alias("Ref_Disc_Config"), how="inner")
    .select(
        col("XmrO_Lkp.Field").alias("Field"),
        col("XmrO_Lkp.SBSB_ID").alias("FACETS_MemberID"),
        col("XmrO_Lkp.GRGR_ID").alias("GRGR_ID"),
        col("XmrO_Lkp.MBI").alias("MBI"),
        col("Ref_Disc_Config.DISCREPANCY_ID").alias("DISCREPANCY_ID"),
        col("Ref_Disc_Config.DISCREPANCY_DESC").alias("DISCREPANCY_DESC"),
        col("Ref_Disc_Config.IDENTIFIEDBY").alias("IDENTIFIEDBY"),
        col("Ref_Disc_Config.PRIORITY").alias("PRIORITY"),
        col("XmrO_Lkp.EAM_PrimaryRXID").alias("SOURCE1"),
        col("XmrO_Lkp.Prefix").alias("SOURCE2")
    )
)

# BLMM_Mismatches (PxSequentialFile) - Reading file
schema_BLMM_Mismatches = StructType([
    StructField("GRGR_ID", StringType(), False),
    StructField("SBSB_ID", StringType(), False),
    StructField("MBI", StringType(), False),
    StructField("SOURCE1", StringType(), False),
    StructField("SOURCE2", StringType(), False),
    StructField("IDENTIFIEDBY", StringType(), False),
    StructField("IDENTIFIEDDATE", DateType(), False),
    StructField("RESOLVEDDATE", DateType(), True),
    StructField("STATUS", StringType(), False),
    StructField("DISCREPANCY", IntegerType(), False),
    StructField("NOTE", StringType(), True),
    StructField("PRIORITY", IntegerType(), False),
    StructField("AGE", IntegerType(), False),
    StructField("LEGACY", StringType(), True),
    StructField("LAST_UPDATED_DATE", DateType(), False)
])
df_BLMM_Mismatches = (
    spark.read.option("header", True)
    .option("delimiter", "|")
    .option("quote", "\"")
    .option("nullValue", "\"\"")
    .schema(schema_BLMM_Mismatches)
    .csv(f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_BLMM_APPEND.{RUNID}.dat")
)

# Add_Col (Transformer) - produce RXIDMisMatches
df_Add_Col = (
    df_BLMM_Mismatches.filter(col("NOTE") == "PrimaryRXID Mismatch")
    .select(
        lit(0).alias("ID_SK"),
        col("MBI").alias("MBI"),
        col("IDENTIFIEDBY").alias("IDENTIFIEDBY"),
        col("IDENTIFIEDDATE").alias("IDENTIFIEDDATE"),
        lit(119).alias("DISCREPANCY_ID")
    )
)

# Fnl (PxFunnel) - funnel RXIDMisMatches + EAM_DISC_ANALYSIS
df_Add_Col_funnel = df_Add_Col.select(
    col("ID_SK"),
    col("MBI"),
    col("IDENTIFIEDBY"),
    col("IDENTIFIEDDATE"),
    col("DISCREPANCY_ID")
)
df_EAM_DISC_ANALYSIS_funnel = df_EAM_DISC_ANALYSIS.select(
    col("ID_SK"),
    col("MBI"),
    col("IDENTIFIEDBY"),
    col("IDENTIFIEDDATE"),
    col("DISCREPANCY_ID")
)
df_Fnl = df_Add_Col_funnel.unionByName(df_EAM_DISC_ANALYSIS_funnel)

# FullOuter (PxJoin) - RuleFailures + DISC_Analysis
df_FullOuter = (
    df_Att_Config.alias("RuleFailures")
    .join(
        df_Fnl.alias("DISC_Analysis"),
        on=[
            (col("RuleFailures.MBI") == col("DISC_Analysis.MBI")),
            (col("RuleFailures.DISCREPANCY_ID") == col("DISC_Analysis.DISCREPANCY_ID")),
            (col("RuleFailures.IDENTIFIEDBY") == col("DISC_Analysis.IDENTIFIEDBY"))
        ],
        how="full"
    )
    .select(
        col("RuleFailures.Field").alias("Field"),
        col("RuleFailures.FACETS_MemberID").alias("FACETS_MemberID"),
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

# Xmr1 (Transformer) => NewAdds, Resolved, StillOpen
df_Xmr1_NewAdds = (
    df_FullOuter.filter(
        (col("rightRec_MBI") == "") &
        (col("leftRec_MBI").isNotNull()) &
        (col("leftRec_MBI") != "")
    )
    .select(
        col("FACETS_GROUP_ID").alias("GRGR_ID"),
        col("FACETS_MemberID").alias("SBSB_ID"),
        col("leftRec_MBI").alias("MBI"),
        col("SOURCE1").alias("SOURCE1"),
        col("SOURCE2").alias("SOURCE2"),
        col("leftRec_IDENTIFIEDBY").alias("IDENTIFIEDBY"),
        to_date(lit(CurrDt), "yyyy-MM-dd").alias("IDENTIFIEDDATE"),
        lit(None).cast(DateType()).alias("RESOLVEDDATE"),
        lit("OPEN").alias("STATUS"),
        col("leftRec_DISCREPANCY_ID").alias("DISCREPANCY"),
        col("DISCREPANCY_DESC").alias("NOTE"),
        col("PRIORITY").alias("PRIORITY"),
        lit(0).alias("AGE"),
        when(col("FACETS_MemberID") == "", lit('N'))
         .otherwise(
             when(substring(col("FACETS_MemberID"), 1, 3) == "000", lit('Y'))
             .otherwise(lit('N'))
         ).alias("LEGACY"),
        to_date(lit(CurrDt), "yyyy-MM-dd").alias("LAST_UPDATED_DATE")
    )
)

df_Xmr1_Resolved = (
    df_FullOuter.filter(
        ((col("leftRec_MBI").isNull()) | (col("leftRec_MBI") == "")) &
        (col("rightRec_MBI") != "") &
        (col("ID_SK") != 0)
    )
    .select(
        col("ID_SK").alias("ID_SK")
    )
)

df_Xmr1_StillOpen = (
    df_FullOuter.filter(
        (col("leftRec_MBI").isNotNull()) & (col("leftRec_MBI") != "") &
        (col("rightRec_MBI") != "") &
        (col("ID_SK") != 0)
    )
    .select(
        col("ID_SK").alias("ID_SK"),
        col("IDENTIFIEDDATE")
    )
)
df_Xmr1_StillOpen = df_Xmr1_StillOpen.withColumn(
    "AGE",
    DaysSinceFromDate2(
        to_date(lit(CurrDt), "yyyy-MM-dd"),
        col("IDENTIFIEDDATE")
    )
).withColumn(
    "LAST_UPDATED_DATE",
    to_date(lit(CurrDt), "yyyy-MM-dd")
)

# Aged (PxSequentialFile) => write StillOpen
df_Xmr1_StillOpen_out = df_Xmr1_StillOpen.select(
    col("ID_SK"),
    col("AGE"),
    col("LAST_UPDATED_DATE")
)
write_files(
    df_Xmr1_StillOpen_out,
    f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_RXIDTC72_AGED.{RUNID}.dat",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)

# Resolved (PxSequentialFile) => write Resolved
df_Xmr1_Resolved_out = df_Xmr1_Resolved.select("ID_SK")
write_files(
    df_Xmr1_Resolved_out,
    f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_RXIDTC72_RESOLVED.{RUNID}.dat",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)

# RmDup (PxRemDup) => deduplicate on MBI, IDENTIFIEDBY, IDENTIFIEDDATE, DISCREPANCY
df_RmDup = dedup_sort(
    df_Xmr1_NewAdds,
    partition_cols=["MBI","IDENTIFIEDBY","IDENTIFIEDDATE","DISCREPANCY"],
    sort_cols=[]
)

df_RmDup = df_RmDup.select(
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
    col("LEGACY"),
    col("LAST_UPDATED_DATE")
)

# New (PxSequentialFile) => final write
df_RmDup_out = df_RmDup.select(
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
    df_RmDup_out,
    f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_RXIDTC72_APPEND.{RUNID}.dat",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)