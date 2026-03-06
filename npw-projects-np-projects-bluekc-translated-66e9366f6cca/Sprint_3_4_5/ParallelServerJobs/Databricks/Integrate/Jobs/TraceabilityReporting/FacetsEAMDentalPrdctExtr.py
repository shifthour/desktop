# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Calling Job:EAMFctsReconRptCntl
# MAGIC 
# MAGIC This job is retrieves the Dental product id from Facets and compares with the plan configuration from EAM and write the results to a file where the Medical Product ids don't match.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC =================================================================================================================================================================
# MAGIC Developer                          Date                           Project/Altiris #                              Change Description                                                    Development Project          Reviewer                 Review Date     
# MAGIC =================================================================================================================================================================
# MAGIC ReddySanam               2021-02-06                  US329820                                         Original Programming                                                    IntegrateDev2                    Jeyaprasanna         2021-02-08
# MAGIC JohnAbraham               2021-08-11                  US391328                  Added EAMRPT Env variables and updated appropriate tables         IntegrateDev1
# MAGIC                                                                                                                with these parameters
# MAGIC Prabhu ES                    2022-03-21                  S2S                                                   MSSQL ODBC conn params added                             IntegrateDev5	Ken Bradmon	2022-06-04

# MAGIC This job is retrieves the Dental product id from Facets, Attaches OSBFlag from EAM and compares with the plan configuration from EAM and write the results to a file where the Dental Product ids don't match.
# MAGIC Attach the plan id for each MBI from EAM. After the plan id is attached, it is compared between FACETS and PLAN CONFIG and call out if they don't match.
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
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
# COMMAND ----------
# MAGIC %run ../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

# Retrieve parameter values
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
EAMRPTOwner = get_widget_value('EAMRPTOwner','')
eamrpt_secret_name = get_widget_value('eamrpt_secret_name','')
EAMOwner = get_widget_value('EAMOwner','')
eam_secret_name = get_widget_value('eam_secret_name','')
RUNID = get_widget_value('RUNID','')
BillDt = get_widget_value('BillDt','')
CurrDt = get_widget_value('CurrDt','')

# EAM_DISC_ANALYSIS (ODBCConnectorPX)
jdbc_url, jdbc_props = get_db_config(eamrpt_secret_name)
extract_query_EAM_DISC_ANALYSIS = f"""
SELECT
  DISC.ID AS ID_SK,
  DISC.MBI,
  DISC.IDENTIFIEDBY,
  DISC.IDENTIFIEDDATE,
  DISC.DISCREPANCY as DISCREPANCY_ID
FROM {EAMRPTOwner}.DISCREPANCY_ANALYSIS DISC
INNER JOIN {EAMRPTOwner}.DISCREPANCY_CONFIG CONFIG
  ON DISC.DISCREPANCY = CONFIG.ID
WHERE STATUS = 'OPEN'
  AND DISC.IDENTIFIEDBY = 'BILL'
  AND CONFIG.DISCREPANCY_NAME in ('INC_Dent','INC_OSB_Dent')
"""
df_EAM_DISC_ANALYSIS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_EAM_DISC_ANALYSIS)
    .load()
)

# DISC_CONFIG (ODBCConnectorPX)
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
  AND IDENTIFIEDBY = 'BILL'
  AND DISCREPANCY_NAME in ('INC_Dent','INC_OSB_Dent')
"""
df_DISC_CONFIG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_DISC_CONFIG)
    .load()
)

# EAM_Product (ODBCConnectorPX)
jdbc_url, jdbc_props = get_db_config(eam_secret_name)
extract_query_EAM_Product = f"""
SELECT Distinct
  PlanID.FieldValue as PlanID,
  PBP.FieldValue as PBP,
  SUBSTRING(SCC.FieldValue, 1, 2) as StateCode,
  Groups.GRGR_ID as GRGR_ID,
  OSB.FieldValue as OSB,
  MedProd.FieldValue as MedicalProduct,
  DentProd.FieldValue as DentalProduct
FROM {EAMOwner}.CoreSystemConfigManagerRules RULES
INNER JOIN {EAMOwner}.CoreSystemConfigManagerFieldMap PlanID
  on PlanID.RuleID = RULES.RuleID AND PlanID.FieldId =1
INNER JOIN {EAMOwner}.CoreSystemConfigManagerFieldMap PBP
  on PBP.RuleID = RULES.RuleID AND PBP.FieldId =2
INNER JOIN {EAMOwner}.CoreSystemConfigManagerFieldMap SCC
  on SCC.RuleID = RULES.RuleID AND SCC.FieldId =4
LEFT OUTER JOIN {EAMOwner}.CoreSystemConfigManagerFieldMap OSB
  on OSB.RuleID = RULES.RuleID AND OSB.FieldId =8
LEFT OUTER JOIN {EAMOwner}.CoreSystemConfigManagerFieldMap GRP
  on GRP.RuleID = RULES.RuleID AND GRP.FieldId =11
LEFT OUTER JOIN {EAMOwner}.Groups
  ON Groups.GroupID = GRP.FieldValue
LEFT OUTER JOIN {EAMOwner}.CoreSystemConfigManagerFieldMap SBG
  on SBG.RuleID = RULES.RuleID AND SBG.FieldId =12
LEFT OUTER JOIN {EAMOwner}.CoreSystemConfigManagerFieldMap CLS
  on CLS.RuleID = RULES.RuleID AND CLS.FieldId =13
LEFT OUTER JOIN {EAMOwner}.CoreSystemConfigManagerFieldMap MedProd
  on MedProd.RuleID = RULES.RuleID AND MedProd.FieldId =14
LEFT OUTER JOIN {EAMOwner}.CoreSystemConfigManagerFieldMap DentProd
  on DentProd.RuleID = RULES.RuleID AND DentProd.FieldId =15
WHERE '{CurrDt}' BETWEEN RULES.MinEffectiveDate AND RULES.MaxEffectiveDate
  AND RULES.IsActive = 1
  AND RULES.IsValid=1
"""
df_EAM_Product = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_EAM_Product)
    .load()
)

# EAM_OSB (ODBCConnectorPX)
extract_query_EAM_OSB = f"""
select distinct
  LTRIM(RTRIM(ISNULL(ENRL.HIC,''))) AS EAM_HIC,
  LTRIM(RTRIM(ISNULL(MEMINFO.OSBFlag ,''))) as EAM_OSBFLAG
from {EAMOwner}.TBMEMBERINFO  AS MEMINFO
INNER JOIN {EAMOwner}.TBEENRLMEMBERS AS ENRL
  ON ENRL.MemCodNum = MEMINFO.MemCodNum
  and ENRL.TERMDATE >= '{BillDt}'
INNER JOIN {EAMOwner}.TBENRLSPANS AS EFF
  ON EFF.MEMCODNUM = MEMINFO.MEMCODNUM
  AND EFF.SPANTYPE = 'EFF'
  and ('{BillDt}' between EFF.StartDate and EFF.EndDate)
"""
df_EAM_OSB = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_EAM_OSB)
    .load()
)

# MemberAttr (ODBCConnectorPX)
jdbc_url, jdbc_props = get_db_config(facets_secret_name)
extract_query_MemberAttr = f"""
SELECT DISTINCT
  LTRIM(RTRIM(GRGR.GRGR_ID)) GRGR_ID,
  SBSB.SBSB_ID,
  LTRIM(RTRIM(MEME.MEME_HICN)) AS MBI,
  LTRIM(RTRIM(BGBG.BGBG_ID)) BGBG_ID,
  LTRIM(RTRIM(PBP.MEMD_MCTR_PBP)) MEMD_MCTR_PBP,
  LTRIM(RTRIM(SCCC.MEMD_MCTR_MCST)) MEMD_MCTR_MCST,
  LTRIM(RTRIM(MEPE.CSPI_ID)) CSPI_ID
FROM {FacetsOwner}.CMC_SBSB_SUBSC SBSB
INNER JOIN {FacetsOwner}.CMC_GRGR_GROUP GRGR
  ON GRGR.GRGR_CK=SBSB.GRGR_CK
INNER JOIN {FacetsOwner}.CMC_MEME_MEMBER MEME
  ON MEME.SBSB_CK=SBSB.SBSB_CK
INNER JOIN {FacetsOwner}.CMC_MEPE_PRCS_ELIG MEPE
  ON MEPE.MEME_CK=MEME.MEME_CK
  AND MEPE.CSPD_CAT='D'
  AND MEPE.MEPE_ELIG_IND='Y'
  AND '{BillDt}' BETWEEN MEPE.MEPE_EFF_DT AND MEPE.MEPE_TERM_DT
INNER JOIN {FacetsOwner}.CMC_MEMD_MECR_DETL PBP
  ON PBP.MEME_CK=MEME.MEME_CK
  AND PBP.MEMD_EVENT_CD='PBP'
  AND '{BillDt}'  BETWEEN PBP.MEMD_HCFA_EFF_DT AND  PBP.MEMD_HCFA_TERM_DT
LEFT OUTER JOIN {FacetsOwner}.CMC_MEMD_MECR_DETL SCCC
  ON SCCC.MEME_CK=MEME.MEME_CK
  and SCCC.MEMD_EVENT_CD='SCCC'
  and '{BillDt}' BETWEEN SCCC.MEMD_HCFA_EFF_DT AND  SCCC.MEMD_HCFA_TERM_DT
LEFT OUTER JOIN {FacetsOwner}.CMC_BGBG_BIL_GROUP BGBG
  ON BGBG.BGBG_CK = SCCC.BGBG_CK
where (LTRIM(RTRIM(SCCC.MEMD_MCTR_MCST)) <> '' )
"""
df_MemberAttr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_MemberAttr)
    .load()
)

# Att_Osb (PxLookup: primary=MemAttr, lookup=EAM_osb, no join conditions => cross join)
df_Att_Osb = (
    df_MemberAttr.alias("MemAttr")
    .join(df_EAM_OSB.alias("Ref_EAM_osb"), how="inner")
    .select(
        F.col("MemAttr.GRGR_ID").alias("GRGR_ID"),
        F.col("MemAttr.SBSB_ID").alias("SBSB_ID"),
        F.col("MemAttr.MBI").alias("MBI"),
        F.col("MemAttr.BGBG_ID").alias("BGBG_ID"),
        F.col("MemAttr.MEMD_MCTR_PBP").alias("MEMD_MCTR_PBP"),
        F.col("MemAttr.MEMD_MCTR_MCST").alias("MEMD_MCTR_MCST"),
        F.col("MemAttr.CSPI_ID").alias("CSPI_ID"),
        F.col("Ref_EAM_osb.EAM_OSBFLAG").alias("EAM_OSBFLAG"),
    )
)

# Att_MedicalEAM (PxLookup: primary=Att_Osb, lookup=EAM_Product => cross join again)
df_Att_MedicalEAM = (
    df_Att_Osb.alias("PrdtMapCheck")
    .join(df_EAM_Product.alias("Ref_EAM_Prdt"), how="inner")
    .select(
        F.col("PrdtMapCheck.GRGR_ID").alias("GRGR_ID"),
        F.col("PrdtMapCheck.SBSB_ID").alias("SBSB_ID"),
        F.col("PrdtMapCheck.MBI").alias("MBI"),
        F.col("PrdtMapCheck.BGBG_ID").alias("BGBG_ID"),
        F.col("PrdtMapCheck.MEMD_MCTR_PBP").alias("MEMD_MCTR_PBP"),
        F.col("PrdtMapCheck.MEMD_MCTR_MCST").alias("MEMD_MCTR_MCST"),
        F.col("PrdtMapCheck.CSPI_ID").alias("CSPI_ID"),
        F.col("PrdtMapCheck.EAM_OSBFLAG").alias("EAM_OSBFLAG"),
        F.col("Ref_EAM_Prdt.MedicalProduct").alias("MedicalProduct"),
        F.col("Ref_EAM_Prdt.DentalProduct").alias("DentalProduct"),
    )
)

# Xmr (CTransformerStage => output link XmrO_Lkp with constraint)
df_Xmr_filtered = df_Att_MedicalEAM.filter(
    F.trim(
        F.when(F.col("CSPI_ID").isNotNull(), F.col("CSPI_ID")).otherwise("")
    )
    != F.trim(
        F.when(F.col("DentalProduct").isNotNull(), F.col("DentalProduct")).otherwise("")
    )
)

df_XmrO_Lkp = (
    df_Xmr_filtered
    .withColumn(
        "Field",
        F.when(F.trim(F.col("EAM_OSBFLAG")) == F.lit("0"), F.lit("INC_Dent")).otherwise(F.lit("INC_OSB_Dent"))
    )
    .select(
        F.col("Field").alias("Field"),
        F.col("GRGR_ID").alias("GRGR_ID"),
        F.col("SBSB_ID").alias("SBSB_ID"),
        F.col("MBI").alias("MBI"),
        F.col("BGBG_ID").alias("BGBG_ID"),
        F.col("MEMD_MCTR_PBP").alias("MEMD_MCTR_PBP"),
        F.col("MEMD_MCTR_MCST").alias("MEMD_MCTR_MCST"),
        F.col("CSPI_ID").alias("CSPI_ID"),
        F.col("DentalProduct").alias("DentalProduct"),
    )
)

# Att_Config (PxLookup: primary=XmrO_Lkp, lookup=DISC_CONFIG => cross join)
df_Att_Config = (
    df_XmrO_Lkp.alias("XmrO_Lkp")
    .join(df_DISC_CONFIG.alias("Ref_Disc_Config"), how="inner")
    .select(
        F.col("XmrO_Lkp.Field").alias("Field"),
        F.col("XmrO_Lkp.SBSB_ID").alias("FACETS_MemberID"),
        F.col("XmrO_Lkp.GRGR_ID").alias("GRGR_ID"),
        F.col("XmrO_Lkp.MBI").alias("MBI"),
        F.col("Ref_Disc_Config.DISCREPANCY_ID").alias("DISCREPANCY_ID"),
        F.col("Ref_Disc_Config.DISCREPANCY_DESC").alias("DISCREPANCY_DESC"),
        F.col("Ref_Disc_Config.IDENTIFIEDBY").alias("IDENTIFIEDBY"),
        F.col("Ref_Disc_Config.PRIORITY").alias("PRIORITY"),
        F.col("XmrO_Lkp.DentalProduct").alias("SOURCE1"),
        F.col("XmrO_Lkp.CSPI_ID").alias("SOURCE2"),
    )
)

# FullOuter (PxJoin: full outer join on MBI, DISCREPANCY_ID, IDENTIFIEDBY)
df_FullOuter = (
    df_Att_Config.alias("RuleFailures")
    .join(
        df_EAM_DISC_ANALYSIS.alias("DISC_Analysis"),
        (
            (F.col("RuleFailures.MBI") == F.col("DISC_Analysis.MBI")) &
            (F.col("RuleFailures.DISCREPANCY_ID") == F.col("DISC_Analysis.DISCREPANCY_ID")) &
            (F.col("RuleFailures.IDENTIFIEDBY") == F.col("DISC_Analysis.IDENTIFIEDBY"))
        ),
        "fullouter"
    )
    .select(
        F.col("RuleFailures.Field").alias("Field"),
        F.col("RuleFailures.FACETS_MemberID").alias("FACETS_MemberID"),
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
)
df_FinalXmr = df_FullOuter

# Xmr1 (CTransformerStage) => multiple output links

# NewAdds
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
    F.to_date(F.lit(CurrDt), "yyyy-MM-dd").alias("IDENTIFIEDDATE"),
    F.lit(None).alias("RESOLVEDDATE"),
    F.lit("OPEN").alias("STATUS"),
    F.col("leftRec_DISCREPANCY_ID").alias("DISCREPANCY"),
    F.col("DISCREPANCY_DESC").alias("NOTE"),
    F.col("PRIORITY").alias("PRIORITY"),
    F.lit(0).alias("AGE"),
    F.when(
        F.col("FACETS_MemberID") == "",
        F.lit("N")
    ).otherwise(
        F.when(F.col("FACETS_MemberID").substr(1, 3) == "000", F.lit("Y")).otherwise(F.lit("N"))
    ).alias("LEGACY"),
    F.to_date(F.lit(CurrDt), "yyyy-MM-dd").alias("LAST_UPDATED_DATE"),
)

# Resolved
df_Resolved = df_FinalXmr.filter(
    (F.when(F.col("leftRec_MBI").isNotNull(), F.col("leftRec_MBI")).otherwise("") == "") &
    (F.col("rightRec_MBI") != "")
).select(
    F.col("ID_SK").alias("ID_SK")
)

# StillOpen
df_StillOpen = df_FinalXmr.filter(
    (F.when(F.col("leftRec_MBI").isNotNull(), F.col("leftRec_MBI")).otherwise("") != "") &
    (F.col("rightRec_MBI") != "")
).select(
    F.col("ID_SK").alias("ID_SK"),
    F.expr("DaysSinceFromDate2(to_date('" + CurrDt + "', 'yyyy-MM-dd'), IDENTIFIEDDATE)").alias("AGE"),
    F.to_date(F.lit(CurrDt), "yyyy-MM-dd").alias("LAST_UPDATED_DATE"),
)

# Aged (PxSequentialFile)
schema_Aged = StructType([
    StructField("ID_SK", StringType(), True),
    StructField("AGE", StringType(), True),
    StructField("LAST_UPDATED_DATE", StringType(), True),
])
df_Aged = df_StillOpen.select("ID_SK", "AGE", "LAST_UPDATED_DATE")
write_files(
    df_Aged,
    f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_DNTLPLN_AGED.{RUNID}.dat",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)

# Resolved (PxSequentialFile)
schema_Resolved = StructType([
    StructField("ID_SK", StringType(), True),
])
df_Resolved_final = df_Resolved.select("ID_SK")
write_files(
    df_Resolved_final,
    f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_DNTLPLN_RESOLVED.{RUNID}.dat",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)

# RmDup (PxRemDup) => deduplicate NewAdds on MBI, IDENTIFIEDBY, IDENTIFIEDDATE, DISCREPANCY
df_RmDup = dedup_sort(
    df_NewAdds,
    partition_cols=["MBI", "IDENTIFIEDBY", "IDENTIFIEDDATE", "DISCREPANCY"],
    sort_cols=[]
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
    F.col("LEGACY"),
    F.col("LAST_UPDATED_DATE"),
)

# New (PxSequentialFile)
# LEGACY is char(1), so apply rpad on final output
df_NewAddsFile_s = (
    df_NewAddsFile
    .withColumn("LEGACY", F.rpad(F.col("LEGACY"), 1, " "))
    .select(
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
        "LAST_UPDATED_DATE",
    )
)

write_files(
    df_NewAddsFile_s,
    f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_DNTLPLN_APPEND.{RUNID}.dat",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)