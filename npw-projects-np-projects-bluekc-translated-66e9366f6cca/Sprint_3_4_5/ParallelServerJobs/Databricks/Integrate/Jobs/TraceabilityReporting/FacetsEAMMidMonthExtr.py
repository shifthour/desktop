# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC JOB NAME:  FacetsEAMMidMonthExtr
# MAGIC Calling Job:EAMFctsReconRptCntl
# MAGIC 
# MAGIC Description:This job extract the data from Facets that have mid month effective dates.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC ==================================================================================================================================================================
# MAGIC Developer                          Date                           Project/Altiris #                              Change Description                                                    Development Project                  Reviewer              Review Date       
# MAGIC ==================================================================================================================================================================
# MAGIC ReddySanam               2021-01-30                  US329820                                         Original Programming                                                    IntegrateDev2                         Jeyaprasanna          2021-02-02
# MAGIC JohnAbraham               2021-08-11                  US391328                  Added EAMRPT Env variables and updated appropriate tables         IntegrateDev1
# MAGIC                                                                                                                with these parameters 
# MAGIC 
# MAGIC Prabhu ES                    2022-03-21                  S2S                                                   MSSQL ODBC conn params added                             IntegrateDev5	Ken Bradmon	2022-06-04
# MAGIC 
# MAGIC Vamsi Aripaka              2024-04-18                 US 616210                               Added GRP_ID parameter                                                      IntegrateDev2                         Jeyaprasanna           2024-06-20

# MAGIC This job is developed to move lon running part of sql in the traceability reporting stored procedure [dbo].[TMGSP_FACETS_EAM_RECON_RPT].
# MAGIC This job extracts member information from FACETS that have midmonth effective for Billing Date(Next month First)
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
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../Utility_Integrate
# COMMAND ----------


EAMRPTOwner = get_widget_value('EAMRPTOwner','')
facetsOwner = get_widget_value('FacetsOwner','')
eamrpt_secret_name = get_widget_value('eamrpt_secret_name','')
facets_secret_name = get_widget_value('facets_secret_name','')
RUNID = get_widget_value('RUNID','')
CurrDt = get_widget_value('CurrDt','')
GRP_ID = get_widget_value('GRP_ID','')

jdbc_url_eamrpt, jdbc_props_eamrpt = get_db_config(eamrpt_secret_name)
df_EAM_DISC_ANALYSIS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_eamrpt)
    .options(**jdbc_props_eamrpt)
    .option(
        "query",
        f"""
SELECT DISC.ID AS ID_SK,
       DISC.MBI,
       DISC.IDENTIFIEDBY,
       DISC.IDENTIFIEDDATE,
       DISC.DISCREPANCY as DISCREPANCY_ID
  FROM {EAMRPTOwner}.DISCREPANCY_ANALYSIS DISC
       INNER JOIN {EAMRPTOwner}.DISCREPANCY_CONFIG CONFIG
          ON DISC.DISCREPANCY = CONFIG.ID
 WHERE STATUS = 'OPEN'
   AND DISC.IDENTIFIEDBY = 'BILL'
   AND CONFIG.DISCREPANCY_NAME IN 
       ('MM_EFF_DT','MM_PBP_DT','MM_CRCO_DT','MM_ESRD_DT','MM_MDCD_DT','MM_LATE_DT','MM_LIS_DT')
"""
    )
    .load()
)

df_DISC_CONFIG_MIDM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_eamrpt)
    .options(**jdbc_props_eamrpt)
    .option(
        "query",
        f"""
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
   AND IDENTIFIEDBY = 'BILL'
   AND DISCREPANCY_NAME IN
       ('MM_EFF_DT','MM_PBP_DT','MM_CRCO_DT','MM_ESRD_DT','MM_MDCD_DT','MM_LATE_DT','MM_LIS_DT')
"""
    )
    .load()
)

jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
df_MemberAttr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option(
        "query",
        f"""
SELECT DISTINCT GRGR.GRGR_ID,
                SBSB.SBSB_ID,
                MEME.MEME_HICN AS MBI,
                MEMD.MEMD_EVENT_CD,
                LTRIM(RTRIM(ISNULL(CONVERT(VARCHAR(10), MEMD.MEMD_HCFA_EFF_DT,101),''))) as EFF_DT,
                CASE
                   WHEN MEMD.MEMD_EVENT_CD = 'LATE' THEN 'MM_LATE_DT'
                   WHEN MEMD.MEMD_EVENT_CD = 'LICS' THEN 'MM_LIS_DT'
                   WHEN MEMD.MEMD_EVENT_CD = 'MDCD' THEN 'MM_MDCD_DT'
                   WHEN MEMD.MEMD_EVENT_CD = 'ESRD' THEN 'MM_ESRD_DT'
                   WHEN MEMD.MEMD_EVENT_CD = 'CRCO' THEN 'MM_CRCO_DT'
                   WHEN MEMD.MEMD_EVENT_CD = 'PBP'  THEN 'MM_PBP_DT'
                END AS DiscCodE
  FROM {facetsOwner}.CMC_SBSB_SUBSC SBSB
       INNER JOIN {facetsOwner}.CMC_GRGR_GROUP GRGR
          ON GRGR.GRGR_CK = SBSB.GRGR_CK
       INNER JOIN {facetsOwner}.CMC_MEME_MEMBER MEME
          ON MEME.SBSB_CK = SBSB.SBSB_CK
       INNER JOIN {facetsOwner}.CMC_MEMD_MECR_DETL MEMD
          ON MEMD.MEME_CK = MEME.MEME_CK
         AND MEMD.MEMD_EVENT_CD IN('MDCD', 'ESRD', 'LATE', 'CRCO', 'LICS', 'PBP')
 WHERE GRGR_ID IN ({GRP_ID})
   AND '{CurrDt}' BETWEEN MEMD.MEMD_HCFA_EFF_DT AND MEMD.MEMD_HCFA_TERM_DT
   AND DAY(MEMD.MEMD_HCFA_EFF_DT) <> 1

UNION ALL

SELECT DISTINCT GRGR.GRGR_ID,
                SBSB.SBSB_ID,
                MEME.MEME_HICN as MBI,
                'MEPE' AS MEMD_EVENT_CD,
                LTRIM(RTRIM(ISNULL(CONVERT(VARCHAR(10), MEPE.MEPE_EFF_DT,101),''))) as EFF_DT,
                'MM_EFF_DT' AS DiscCodE
  FROM {facetsOwner}.CMC_SBSB_SUBSC SBSB
       INNER JOIN {facetsOwner}.CMC_GRGR_GROUP GRGR
          ON GRGR.GRGR_CK = SBSB.GRGR_CK
       INNER JOIN {facetsOwner}.CMC_MEME_MEMBER MEME
          ON MEME.SBSB_CK = SBSB.SBSB_CK
       INNER JOIN {facetsOwner}.CMC_MEPE_PRCS_ELIG MEPE
          ON MEPE.MEME_CK = MEME.MEME_CK
         AND MEPE.MEPE_ELIG_IND = 'Y'
         AND '{CurrDt}' BETWEEN MEPE.MEPE_EFF_DT AND MEPE.MEPE_TERM_DT
 WHERE GRGR_ID IN ({GRP_ID})
   AND DAY(MEPE.MEPE_EFF_DT) <> 1
   AND LTRIM(RTRIM(ISNULL(MEME.MEME_HICN,''))) <> ''
"""
    )
    .load()
)

df_Att_Disc = (
    df_MemberAttr.alias("MemAttr")
    .join(
        df_DISC_CONFIG_MIDM.alias("Ref_Disc_Config"),
        F.col("MemAttr.DiscCodE") == F.col("Ref_Disc_Config.DISCREPANCY_NAME"),
        "inner",
    )
    .select(
        F.col("MemAttr.GRGR_ID").alias("GRGR_ID"),
        F.col("MemAttr.SBSB_ID").alias("SBSB_ID"),
        F.col("MemAttr.MBI").alias("MBI"),
        F.col("MemAttr.MEMD_EVENT_CD").alias("MEMD_EVENT_CD"),
        F.col("MemAttr.EFF_DT").alias("EFF_DT"),
        F.col("MemAttr.DiscCodE").alias("DiscCodE"),
        F.col("Ref_Disc_Config.DISCREPANCY_ID").alias("DISCREPANCY_ID"),
        F.col("Ref_Disc_Config.DISCREPANCY_DESC").alias("DISCREPANCY_DESC"),
        F.col("Ref_Disc_Config.IDENTIFIEDBY").alias("IDENTIFIEDBY"),
        F.col("Ref_Disc_Config.PRIORITY").alias("PRIORITY"),
        F.col("Ref_Disc_Config.SOURCE1").alias("SOURCE1"),
        F.col("Ref_Disc_Config.SOURCE2").alias("SOURCE2"),
    )
)

df_FullOuter = (
    df_Att_Disc.alias("Xfm")
    .join(
        df_EAM_DISC_ANALYSIS.alias("DISC_Analysis"),
        on=[
            F.col("Xfm.MBI") == F.col("DISC_Analysis.MBI"),
            F.col("Xfm.DISCREPANCY_ID") == F.col("DISC_Analysis.DISCREPANCY_ID"),
            F.col("Xfm.IDENTIFIEDBY") == F.col("DISC_Analysis.IDENTIFIEDBY"),
        ],
        how="full",
    )
    .select(
        F.col("Xfm.SBSB_ID").alias("MemberID"),
        F.col("Xfm.MBI").alias("leftRec_MBI"),
        F.col("Xfm.GRGR_ID").alias("EAM_GROUP_ID"),
        F.col("Xfm.MEMD_EVENT_CD").alias("MEMD_EVENT_CD"),
        F.col("Xfm.EFF_DT").alias("EFF_DT"),
        F.col("Xfm.DiscCodE").alias("DiscCodE"),
        F.col("Xfm.DISCREPANCY_ID").alias("leftRec_DISCREPANCY_ID"),
        F.col("Xfm.DISCREPANCY_DESC").alias("DISCREPANCY_DESC"),
        F.col("Xfm.IDENTIFIEDBY").alias("leftRec_IDENTIFIEDBY"),
        F.col("Xfm.PRIORITY").alias("PRIORITY"),
        F.col("Xfm.SOURCE1").alias("SOURCE1"),
        F.col("Xfm.SOURCE2").alias("SOURCE2"),
        F.col("DISC_Analysis.ID_SK").alias("ID_SK"),
        F.col("DISC_Analysis.MBI").alias("rightRec_MBI"),
        F.col("DISC_Analysis.IDENTIFIEDBY").alias("rightRec_IDENTIFIEDBY"),
        F.col("DISC_Analysis.IDENTIFIEDDATE").alias("IDENTIFIEDDATE"),
        F.col("DISC_Analysis.DISCREPANCY_ID").alias("rightRec_DISCREPANCY_ID"),
    )
)

df_NewAdds = df_FullOuter.filter(
    (F.col("rightRec_MBI") == "") & (F.coalesce(F.col("leftRec_MBI"), F.lit('')) != "")
).select(
    F.col("EAM_GROUP_ID").alias("GRGR_ID"),
    F.col("MemberID").alias("SBSB_ID"),
    F.col("leftRec_MBI").alias("MBI"),
    F.col("MEMD_EVENT_CD").alias("SOURCE1"),
    F.col("EFF_DT").alias("SOURCE2"),
    F.col("leftRec_IDENTIFIEDBY").alias("IDENTIFIEDBY"),
    StringToDate(CurrDt, "%yyyy-%mm-%dd").alias("IDENTIFIEDDATE"),
    F.lit(None).alias("RESOLVEDDATE"),
    F.lit("OPEN").alias("STATUS"),
    F.col("leftRec_DISCREPANCY_ID").alias("DISCREPANCY"),
    F.col("DISCREPANCY_DESC").alias("NOTE"),
    F.col("PRIORITY").alias("PRIORITY"),
    F.lit(0).alias("AGE"),
    F.when(F.col("MemberID") == "", F.lit("N"))
     .otherwise(
         F.when(F.substring("MemberID", 1, 3) == "000", F.lit("Y")).otherwise(F.lit("N"))
     )
    .alias("LEGACY"),
    StringToDate(CurrDt, "%yyyy-%mm-%dd").alias("LAST_UPDATED_DATE"),
)

df_Resolved = df_FullOuter.filter(
    (F.coalesce(F.col("leftRec_MBI"), F.lit('')) == "") & (F.col("rightRec_MBI") != "")
).select(
    F.col("ID_SK").alias("ID_SK"),
)

df_StillOpen = df_FullOuter.filter(
    (F.coalesce(F.col("leftRec_MBI"), F.lit('')) != "") & (F.col("rightRec_MBI") != "")
).select(
    F.col("ID_SK").alias("ID_SK"),
    DaysSinceFromDate2(StringToDate(CurrDt, "%yyyy-%mm-%dd"), F.col("IDENTIFIEDDATE")).alias("AGE"),
    StringToDate(CurrDt, "%yyyy-%mm-%dd").alias("LAST_UPDATED_DATE"),
)

write_files(
    df_Resolved.select("ID_SK"),
    f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_MIDM_RESOLVED.{RUNID}.dat",
    delimiter="|",
    mode="overwrite",
    is_parquet=False,
    header=True,
    quote="\"",
    nullValue=None
)

write_files(
    df_StillOpen.select("ID_SK", "AGE", "LAST_UPDATED_DATE"),
    f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_MIDM_AGED.{RUNID}.dat",
    delimiter="|",
    mode="overwrite",
    is_parquet=False,
    header=True,
    quote="\"",
    nullValue=None
)

df_RmDup = dedup_sort(
    df_NewAdds,
    ["MBI","IDENTIFIEDBY","DISCREPANCY","IDENTIFIEDDATE"],
    []
)

df_RmDup_final = df_RmDup.select(
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

df_RmDup_final_write = (
    df_RmDup_final
    .withColumn("GRGR_ID", F.rpad(F.col("GRGR_ID"), 8, " "))
    .withColumn("SBSB_ID", F.rpad(F.col("SBSB_ID"), 9, " "))
    .withColumn("LEGACY", F.rpad(F.col("LEGACY"), 1, " "))
)

write_files(
    df_RmDup_final_write.select(
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
    ),
    f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_MIDM_APPEND.{RUNID}.dat",
    delimiter="|",
    mode="overwrite",
    is_parquet=False,
    header=True,
    quote="\"",
    nullValue=None
)