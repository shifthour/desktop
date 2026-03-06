# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC JOB NAME:  FacetsEAMLICSRateExtr
# MAGIC Calling Job:EAMFctsReconRptCntl
# MAGIC 
# MAGIC Description:This job extracts Medicare Event Rate and validats it
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC ==================================================================================================================================================================
# MAGIC Developer                          Date                           Project/Altiris #                              Change Description                                                    Development Project                  Reviewer              Review Date       
# MAGIC ==================================================================================================================================================================
# MAGIC ReddySanam               2021-02-22                  US329820                                         Original Programming                                                    IntegrateDev2                        Jeyaprasanna           2021-02-23
# MAGIC JohnAbraham               2021-08-11                  US391328                  Added EAMRPT Env variables and updated appropriate tables         IntegrateDev1
# MAGIC                                                                                                                with these parameters
# MAGIC Prabhu ES                    2022-03-21                  S2S                                                   MSSQL ODBC conn params added                             IntegrateDev5	Ken Bradmon	2022-06-04
# MAGIC 
# MAGIC Vamsi Aripaka              2024-04-18                 US 616210                               Added GRP_ID parameter                                                      IntegrateDev2                       Jeyaprasanna           2024-06-20

# MAGIC This job is developed to move lon running part of sql in the traceability reporting stored procedure [dbo].[TMGSP_FACETS_EAM_RECON_RPT].
# MAGIC Description:This job extracts Medicare Event Rate and validats it from FACETS
# MAGIC This file will have newly found discrepancies
# MAGIC This file will have resolved discrepancies between last run and current run
# MAGIC This file will have discrepancies that are still open
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, when, rpad
# COMMAND ----------
# MAGIC %run ../../../Utility_Integrate
# COMMAND ----------

# Obtain parameter values
FacetsOwner = get_widget_value("FacetsOwner", "")
facets_secret_name = get_widget_value("facets_secret_name", "")
EAMRPTOwner = get_widget_value("EAMRPTOwner", "")
eamrpt_secret_name = get_widget_value("eamrpt_secret_name", "")
RUNID = get_widget_value("RUNID", "")
CurrDt = get_widget_value("CurrDt", "")
BillDt = get_widget_value("BillDt", "")
GRP_ID = get_widget_value("GRP_ID", "")

# EAM_DISC_ANALYSIS
jdbc_url_eamrpt, jdbc_props_eamrpt = get_db_config(eamrpt_secret_name)
extract_query_EAM_DISC_ANALYSIS = f"""
SELECT  DISC.ID AS ID_SK,
        DISC.MBI,
        DISC.IDENTIFIEDBY,
        DISC.IDENTIFIEDDATE,
        DISC.DISCREPANCY as DISCREPANCY_ID
FROM {EAMRPTOwner}.DISCREPANCY_ANALYSIS DISC
INNER JOIN {EAMRPTOwner}.DISCREPANCY_CONFIG CONFIG
ON DISC.DISCREPANCY = CONFIG.ID
WHERE STATUS = 'OPEN'
AND DISC.IDENTIFIEDBY = 'BLMM'
AND CONFIG.DISCREPANCY_NAME = 'LICS_RATE'
"""
df_EAM_DISC_ANALYSIS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_eamrpt)
    .options(**jdbc_props_eamrpt)
    .option("query", extract_query_EAM_DISC_ANALYSIS)
    .load()
)

# DISC_CONFIG_MIDM
extract_query_DISC_CONFIG_MIDM = f"""
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
and IDENTIFIEDBY = 'BLMM'
and DISCREPANCY_NAME = 'LICS_RATE'
"""
df_DISC_CONFIG_MIDM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_eamrpt)
    .options(**jdbc_props_eamrpt)
    .option("query", extract_query_DISC_CONFIG_MIDM)
    .load()
)

# LICS_RATE
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
extract_query_LICS_RATE = f"""
select GRGR.GRGR_ID
     , SBSB.SBSB_ID
     , MEME.MEME_HICN AS MBI
     , LICS.MEMD_PARTD_SBSDY
     , SBRT.SBRT_RT_AREA
FROM {FacetsOwner}.CMC_SBSB_SUBSC SBSB
INNER JOIN {FacetsOwner}.CMC_GRGR_GROUP GRGR
    ON GRGR.GRGR_CK=SBSB.GRGR_CK
INNER JOIN {FacetsOwner}.CMC_MEME_MEMBER MEME
    ON MEME.SBSB_CK=SBSB.SBSB_CK
INNER JOIN {FacetsOwner}.CMC_MEPE_PRCS_ELIG MEPE
    ON MEPE.MEME_CK=MEME.MEME_CK
   AND MEPE.CSPD_CAT='M'
   AND MEPE.MEPE_ELIG_IND='Y'
LEFT OUTER JOIN {FacetsOwner}.CMC_MEMD_MECR_DETL LICS
    ON LICS.MEME_CK=MEME.MEME_CK
   AND LICS.MEMD_EVENT_CD='LICS'
   AND '{BillDt}' BETWEEN LICS.MEMD_HCFA_EFF_DT AND LICS.MEMD_HCFA_TERM_DT
LEFT OUTER JOIN {FacetsOwner}.CMC_SBRT_RATE_DATA SBRT
    ON SBRT.SBSB_CK = SBSB.SBSB_CK
   AND '{BillDt}' BETWEEN SBRT.SBRT_EFF_DT AND SBRT.SBRT_TERM_DT
WHERE GRGR_ID IN ({GRP_ID})
  AND (
         '{BillDt}' BETWEEN MEPE.MEPE_EFF_DT AND MEPE.MEPE_TERM_DT
      OR (MEPE.MEPE_EFF_DT < '{BillDt}' and  MEPE.MEPE_TERM_DT is null)
  )
  and LTRIM(RTRIM(ISNULL(MEME.MEME_HICN,''))) <> ''
"""
df_LICS_RATE = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_LICS_RATE)
    .load()
)

# BRule (Transformer)
df_BRule = (
    df_LICS_RATE
    .withColumn(
        "svLICS",
        when(
            col("MEMD_PARTD_SBSDY").isNull() | (trim(col("MEMD_PARTD_SBSDY")) == ""),
            ""
        ).otherwise(trim(col("MEMD_PARTD_SBSDY")))
    )
    .withColumn(
        "svRATE",
        when(
            col("SBRT_RT_AREA").isNull() | (trim(col("SBRT_RT_AREA")) == ""),
            ""
        ).otherwise(trim(col("SBRT_RT_AREA")))
    )
    .withColumn(
        "svCompareFlag",
        when(
            ((col("svLICS") == "") | (col("svLICS") == "000"))
            & ((col("svRATE") == "NOLIPS") | (col("svRATE") == "DIRECT")),
            "Y"
        )
        .when((col("svLICS") == "025") & (col("svRATE") == "LICS025"), "Y")
        .when((col("svLICS") == "050") & (col("svRATE") == "LICS050"), "Y")
        .when((col("svLICS") == "075") & (col("svRATE") == "LICS075"), "Y")
        .when((col("svLICS") == "100") & (col("svRATE") == "LICS100"), "Y")
        .otherwise("N")
    )
)

df_Mismatches = (
    df_BRule
    .filter(col("svCompareFlag") == "N")
    .select(
        lit("LICS_RATE").alias("Field"),
        when(col("GRGR_ID").isNull(), "").otherwise(trim(col("GRGR_ID"))).alias("GRGR_ID"),
        when(col("SBSB_ID").isNull(), "").otherwise(trim(col("SBSB_ID"))).alias("SBSB_ID"),
        when(col("MBI").isNull(), "").otherwise(trim(col("MBI"))).alias("MBI"),
        when(col("MEMD_PARTD_SBSDY").isNull(), "").otherwise(trim(col("MEMD_PARTD_SBSDY"))).alias("MEMD_PARTD_SBSDY"),
        when(col("SBRT_RT_AREA").isNull(), "").otherwise(trim(col("SBRT_RT_AREA"))).alias("SBRT_RT_AREA"),
    )
)

# Att_Disc (Lookup-like cross join with Ref_Disc_Config)
df_Att_Disc = df_Mismatches.crossJoin(df_DISC_CONFIG_MIDM).select(
    col("Mismatches.GRGR_ID").alias("GRGR_ID"),
    col("Mismatches.SBSB_ID").alias("SBSB_ID"),
    col("Mismatches.MBI").alias("MBI"),
    col("Ref_Disc_Config.DISCREPANCY_ID").alias("DISCREPANCY_ID"),
    col("Ref_Disc_Config.DISCREPANCY_DESC").alias("DISCREPANCY_DESC"),
    col("Ref_Disc_Config.IDENTIFIEDBY").alias("IDENTIFIEDBY"),
    col("Ref_Disc_Config.PRIORITY").alias("PRIORITY"),
    col("Mismatches.MEMD_PARTD_SBSDY").alias("SOURCE1"),
    col("Mismatches.SBRT_RT_AREA").alias("SOURCE2"),
)

# FullOuter (Join)
df_FullOuter = (
    df_Att_Disc.alias("Xfm")
    .join(
        df_EAM_DISC_ANALYSIS.alias("DISC_Analysis"),
        (
            (col("Xfm.MBI") == col("DISC_Analysis.MBI"))
            & (col("Xfm.DISCREPANCY_ID") == col("DISC_Analysis.DISCREPANCY_ID"))
            & (col("Xfm.IDENTIFIEDBY") == col("DISC_Analysis.IDENTIFIEDBY"))
        ),
        "full"
    )
    .select(
        col("Xfm.SBSB_ID").alias("MemberID"),
        col("Xfm.MBI").alias("leftRec_MBI"),
        col("Xfm.GRGR_ID").alias("EAM_GROUP_ID"),
        col("Xfm.DISCREPANCY_ID").alias("leftRec_DISCREPANCY_ID"),
        col("Xfm.DISCREPANCY_DESC").alias("DISCREPANCY_DESC"),
        col("Xfm.IDENTIFIEDBY").alias("leftRec_IDENTIFIEDBY"),
        col("Xfm.PRIORITY").alias("PRIORITY"),
        col("Xfm.SOURCE1").alias("SOURCE1"),
        col("Xfm.SOURCE2").alias("SOURCE2"),
        col("DISC_Analysis.ID_SK").alias("ID_SK"),
        col("DISC_Analysis.MBI").alias("rightRec_MBI"),
        col("DISC_Analysis.IDENTIFIEDBY").alias("rightRec_IDENTIFIEDBY"),
        col("DISC_Analysis.IDENTIFIEDDATE").alias("IDENTIFIEDDATE"),
        col("DISC_Analysis.DISCREPANCY_ID").alias("rightRec_DISCREPANCY_ID"),
    )
)

# Map (Transformer) => produce three outputs: NewAdds, Resolved, StillOpen

df_NewAdds = (
    df_FullOuter
    .filter((col("rightRec_MBI") == "") & (col("leftRec_MBI") != ""))
    .select(
        col("EAM_GROUP_ID").alias("GRGR_ID"),
        col("MemberID").alias("SBSB_ID"),
        col("leftRec_MBI").alias("MBI"),
        col("SOURCE1").alias("SOURCE1"),
        col("SOURCE2").alias("SOURCE2"),
        col("leftRec_IDENTIFIEDBY").alias("IDENTIFIEDBY"),
        # "StringToDate(CurrDt, \"%yyyy-%mm-%dd\")"
        # Assume user-defined function StringToDate is available
        StringToDate(lit(CurrDt), "%yyyy-%mm-%dd").alias("IDENTIFIEDDATE"),
        lit(None).alias("RESOLVEDDATE"),
        lit("OPEN").alias("STATUS"),
        col("leftRec_DISCREPANCY_ID").alias("DISCREPANCY"),
        col("DISCREPANCY_DESC").alias("NOTE"),
        col("PRIORITY").alias("PRIORITY"),
        lit(0).alias("AGE"),
        when(col("MemberID") == "", "N").otherwise(
            when(col("MemberID").substr(1, 3) == "000", "Y").otherwise("N")
        ).alias("LEGACY"),
        StringToDate(lit(CurrDt), "%yyyy-%mm-%dd").alias("LAST_UPDATED_DATE"),
    )
)

df_Resolved = (
    df_FullOuter
    .filter((col("leftRec_MBI") == "") & (col("rightRec_MBI") != ""))
    .select(
        col("ID_SK").alias("ID_SK"),
    )
)

df_StillOpen = (
    df_FullOuter
    .filter((col("leftRec_MBI") != "") & (col("rightRec_MBI") != ""))
    .select(
        col("ID_SK").alias("ID_SK"),
        # "DaysSinceFromDate2(StringToDate(CurrDt, \"%yyyy-%mm-%dd\"), IDENTIFIEDDATE)"
        DaysSinceFromDate2(
            StringToDate(lit(CurrDt), "%yyyy-%mm-%dd"),
            col("IDENTIFIEDDATE")
        ).alias("AGE"),
        StringToDate(lit(CurrDt), "%yyyy-%mm-%dd").alias("LAST_UPDATED_DATE"),
    )
)

# Resolved => PxSequentialFile
df_Resolved_for_write = df_Resolved.select("ID_SK")
write_files(
    df_Resolved_for_write,
    f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_LICS_RESOLVED.{RUNID}.dat",
    delimiter="|",
    mode="overwrite",
    is_parquet=False,
    header=True,
    quote='"',
    nullValue=None
)

# Aged => PxSequentialFile
df_Aged_for_write = df_StillOpen.select("ID_SK", "AGE", "LAST_UPDATED_DATE")
write_files(
    df_Aged_for_write,
    f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_LICS_AGED.{RUNID}.dat",
    delimiter="|",
    mode="overwrite",
    is_parquet=False,
    header=True,
    quote='"',
    nullValue=None
)

# RmDup => dedup_sort
df_NewAdds_dedup = dedup_sort(
    df_NewAdds,
    partition_cols=["MBI", "IDENTIFIEDBY", "DISCREPANCY", "IDENTIFIEDDATE"],
    sort_cols=[("MBI","A"), ("IDENTIFIEDBY","A"), ("DISCREPANCY","A"), ("IDENTIFIEDDATE","A")]
)
df_NewAdds_dedup_sel = df_NewAdds_dedup.select(
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

# New => PxSequentialFile
# Apply rpad to char columns: GRGR_ID length=8, SBSB_ID length=9, LEGACY length=1
df_New_for_write = (
    df_NewAdds_dedup_sel
    .withColumn("GRGR_ID", rpad(col("GRGR_ID"), 8, " "))
    .withColumn("SBSB_ID", rpad(col("SBSB_ID"), 9, " "))
    .withColumn("LEGACY", rpad(col("LEGACY"), 1, " "))
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
        "LAST_UPDATED_DATE"
    )
)
write_files(
    df_New_for_write,
    f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_LICS_APPEND.{RUNID}.dat",
    delimiter="|",
    mode="overwrite",
    is_parquet=False,
    header=True,
    quote='"',
    nullValue=None
)