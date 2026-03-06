# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC JOB NAME:  FacetsEAMLBILLExtr
# MAGIC Calling Job:EAMFctsReconRptCntl
# MAGIC 
# MAGIC Description:This job extracts Lisa Billing query result and compares if a discrepancy already exists and writes to appropriate file for load/delete
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC ======================================================================================================================================
# MAGIC Developer                          Date                           Project/Altiris #                              Change Description                                                    Development Project               
# MAGIC ======================================================================================================================================
# MAGIC ReddySanam               2021-01-25                  US329820                                         Original Programming                                                         IntegrateDev2               Kalyan Neelam       2021-01-27
# MAGIC ReddySanam               2021-02-03                  US329820                                         Changed case statement                                                   IntegrateDev2               Jeyaprasanna        2021-02-04
# MAGIC                                                                                                                                    for 'INBL' to BLDF_MCTR_STMT not in ('INBL','RECR')
# MAGIC JohnAbraham               2021-08-11                  US391328                              Added EAMRPT Env variables and updated appropriate tables   IntegrateDev1
# MAGIC                                                                                                                           with these parameters 
# MAGIC Prabhu ES                   2022-03-21                   S2S                                         MSSQL ODBC conn params added                                             IntegrateDev5	Ken Bradmon	2022-06-04
# MAGIC 
# MAGIC Vamsi Aripaka              2024-04-18                 US 616210                               Added GRP_ID parameter                                                      IntegrateDev2                  Jeyaprasanna         2024-06-21

# MAGIC This job extracts Lisa Billing query result and compares if a discrepancy already exists and writes to appropriate file for load/delete
# MAGIC This file will have discrepancies that are still open
# MAGIC This file will have resolved discrepancies between last run and current run
# MAGIC This file will have newly found discrepancies
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../Utility_Integrate
# COMMAND ----------


EAMRPTOwner = get_widget_value('EAMRPTOwner','')
eamrpt_secret_name = get_widget_value('eamrpt_secret_name','')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
RUNID = get_widget_value('RUNID','')
CurrDt = get_widget_value('CurrDt','')
BillDt = get_widget_value('BillDt','')
GRP_ID = get_widget_value('GRP_ID','')

jdbc_url_eamrpt, jdbc_props_eamrpt = get_db_config(eamrpt_secret_name)
extract_query_eam_disc_analysis = f"""
SELECT
  ID AS ID_SK,
  MBI,
  IDENTIFIEDBY,
  IDENTIFIEDDATE,
  DISCREPANCY AS DISCREPANCY_ID
FROM {EAMRPTOwner}.DISCREPANCY_ANALYSIS
WHERE STATUS = 'OPEN'
  AND IDENTIFIEDBY = 'LBILL'
"""
df_EAM_DISC_ANALYSIS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_eamrpt)
    .options(**jdbc_props_eamrpt)
    .option("query", extract_query_eam_disc_analysis)
    .load()
)

extract_query_disc_config_lbill = f"""
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
  and IDENTIFIEDBY = 'LBILL'
"""
df_DISC_CONFIG_LBILL = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_eamrpt)
    .options(**jdbc_props_eamrpt)
    .option("query", extract_query_disc_config_lbill)
    .load()
)

jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
extract_query_memberattr = f"""
SELECT
  GRGR_ID,
  SBSB_ID,
  LTRIM(RTRIM(ISNULL(MBI,''))) as MBI,
  BLEI_CK,
  BLEI_SUSPND_BL_IND,
  BLDF_MCTR_STMT,
  BLDF_STOCK_ID,
  BLDF_BL_GEN_METH,
  BLDF_BL_GEN_VAL,
  BLAD_ADDR1,
  MEMD_PREM_WH_OPT,
  CASE
    WHEN BLEI_CK IS NULL THEN 'MS_BLEI'
    WHEN BLAD_ADDR1 IS NOT NULL THEN 'GRP25'
    WHEN MEMD_PREM_WH_OPT IN ('D','N') AND BLDF_MCTR_STMT not in ('INBL','RECR') AND GRGR_ID='10005000' THEN 'INBL'
    WHEN MEMD_PREM_WH_OPT IN ('R','S') AND BLDF_MCTR_STMT<>'RESS' AND GRGR_ID='10005000' THEN 'RESS'
    WHEN GRGR_ID<>'10005000' AND BLDF_MCTR_STMT<>'L800' THEN 'L800'
    WHEN BLDF_STOCK_ID <>'MABCBS' THEN 'STOCKID'
    WHEN MEMD_PREM_WH_OPT IS NULL THEN 'MS_PWO'
    WHEN (BLDF_BL_GEN_METH<>'D' AND GRGR_ID='10005000' AND BLAD_ADDR1 IS NULL)
         OR (BLDF_BL_GEN_METH<>'A' AND GRGR_ID='10005000' AND BLAD_ADDR1 IS NOT NULL)
         OR (BLDF_BL_GEN_METH<>'A' AND GRGR_ID<>'10005000')
         THEN 'BLDF_METH'
    WHEN BLDF_BL_GEN_VAL<>7 THEN 'BLDF_VAL'
  END AS DiscCode
FROM (
  SELECT DISTINCT
    GRGR.GRGR_ID,
    SBSB.SBSB_ID,
    MEME.MEME_HICN as MBI,
    BLEI.BLEI_CK,
    BLEI.BLEI_SUSPND_BL_IND,
    BLDF.BLDF_MCTR_STMT,
    BLDF.BLDF_STOCK_ID,
    BLDF.BLDF_BL_GEN_METH,
    BLDF.BLDF_BL_GEN_VAL,
    BLAD.BLAD_ADDR1,
    MEMD_PREM_WH_OPT
  FROM {FacetsOwner}.CMC_MEPE_PRCS_ELIG MEPE
  INNER JOIN {FacetsOwner}.CMC_GRGR_GROUP GRGR
    ON MEPE.GRGR_CK=GRGR.GRGR_CK
    AND GRGR.GRGR_ID IN ({GRP_ID})
  INNER JOIN {FacetsOwner}.CMC_MEME_MEMBER MEME
    ON MEPE.MEME_CK=MEME.MEME_CK
  INNER JOIN {FacetsOwner}.CMC_SBSB_SUBSC SBSB
    ON MEME.SBSB_CK=SBSB.SBSB_CK
  LEFT OUTER JOIN {FacetsOwner}.CMC_BLEI_ENTY_INFO BLEI
    ON BLEI.BLEI_BILL_LEVEL='I' AND BLEI.BLEI_BILL_LEVEL_CK=SBSB.SBSB_CK
  LEFT OUTER JOIN {FacetsOwner}.CMC_BLDF_BL_DEFINE BLDF
    ON BLEI.BLEI_CK=BLDF.BLEI_CK
    AND BLDF.BLDF_TERM_DT>= '{BillDt}'
  LEFT OUTER JOIN {FacetsOwner}.CMC_BLAD_ADDR BLAD
    ON BLEI.BLEI_CK=BLAD.BLEI_CK
    AND BLAD_ADDR1 LIKE 'BLUE%'
  LEFT OUTER JOIN {FacetsOwner}.CMC_MEMD_MECR_DETL MEMD
    on MEME.MEME_CK = MEMD.MEME_CK
    AND MEMD.MEMD_EVENT_CD='PREM'
    AND MEMD.MEMD_HCFA_TERM_DT >= '{BillDt}'
  WHERE MEPE.MEPE_ELIG_IND='Y'
    AND MEPE.CSPD_CAT = 'M'
    AND MEPE.MEPE_TERM_DT>='{BillDt}'
    AND (
      BLEI.BLEI_CK IS NULL
      OR BLAD_ADDR1 IS NOT NULL
      OR (MEMD_PREM_WH_OPT IN ('D','N') AND BLDF_MCTR_STMT<>'INBL' AND GRGR_ID='10005000')
      OR (MEMD_PREM_WH_OPT IN ('R','S') AND BLDF_MCTR_STMT<>'RESS' AND GRGR_ID='10005000')
      OR (GRGR_ID<>'10005000' AND BLDF_MCTR_STMT<>'L800')
      OR BLDF_STOCK_ID <> 'MABCBS'
      OR MEMD_PREM_WH_OPT IS NULL
      OR (BLDF_BL_GEN_METH<>'D' AND GRGR_ID='10005000' AND BLAD_ADDR1 IS NULL)
      OR (BLDF_BL_GEN_METH<>'A' AND GRGR_ID='10005000' AND BLAD_ADDR1 IS NOT NULL)
      OR (BLDF_BL_GEN_METH<>'A' AND GRGR_ID<>'10005000')
      OR BLDF_BL_GEN_VAL<>7
    )
) T
"""
df_MemberAttr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_memberattr)
    .load()
)

df_Att_Disc = (
    df_MemberAttr.alias("MemAttr")
    .join(df_DISC_CONFIG_LBILL.alias("Ref_Disc_Config"), F.lit(True), "inner")
)

df_Xfm = df_Att_Disc.select(
    F.col("MemAttr.GRGR_ID").alias("GRGR_ID"),
    F.col("MemAttr.SBSB_ID").alias("SBSB_ID"),
    F.col("MemAttr.MBI").alias("MBI"),
    F.col("MemAttr.BLEI_CK").alias("BLEI_CK"),
    F.col("MemAttr.BLEI_SUSPND_BL_IND").alias("BLEI_SUSPND_BL_IND"),
    F.col("MemAttr.BLDF_MCTR_STMT").alias("BLDF_MCTR_STMT"),
    F.col("MemAttr.BLDF_STOCK_ID").alias("BLDF_STOCK_ID"),
    F.col("MemAttr.BLDF_BL_GEN_METH").alias("BLDF_BL_GEN_METH"),
    F.col("MemAttr.BLDF_BL_GEN_VAL").alias("BLDF_BL_GEN_VAL"),
    F.col("MemAttr.BLAD_ADDR1").alias("BLAD_ADDR1"),
    F.col("MemAttr.MEMD_PREM_WH_OPT").alias("MEMD_PREM_WH_OPT"),
    F.col("MemAttr.DiscCode").alias("DiscCodE"),
    F.col("Ref_Disc_Config.DISCREPANCY_ID").alias("DISCREPANCY_ID"),
    F.col("Ref_Disc_Config.DISCREPANCY_DESC").alias("DISCREPANCY_DESC"),
    F.col("Ref_Disc_Config.IDENTIFIEDBY").alias("IDENTIFIEDBY"),
    F.col("Ref_Disc_Config.PRIORITY").alias("PRIORITY"),
    F.col("Ref_Disc_Config.SOURCE1").alias("SOURCE1"),
    F.col("Ref_Disc_Config.SOURCE2").alias("SOURCE2")
)

df_FullOuter = (
    df_Xfm.alias("Xfm")
    .join(
        df_EAM_DISC_ANALYSIS.alias("DISC_Analysis"),
        (
            (F.col("Xfm.MBI") == F.col("DISC_Analysis.MBI"))
            & (F.col("Xfm.DISCREPANCY_ID") == F.col("DISC_Analysis.DISCREPANCY_ID"))
            & (F.col("Xfm.IDENTIFIEDBY") == F.col("DISC_Analysis.IDENTIFIEDBY"))
        ),
        "full"
    )
)

df_FinalXmr = df_FullOuter.select(
    F.col("Xfm.SBSB_ID").alias("MemberID"),
    F.col("Xfm.MBI").alias("leftRec_MBI"),
    F.col("Xfm.GRGR_ID").alias("EAM_GROUP_ID"),
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
    F.col("Xfm.MEMD_PREM_WH_OPT").alias("MEMD_PREM_WH_OPT"),
    F.col("Xfm.BLDF_MCTR_STMT").alias("BLDF_MCTR_STMT")
)

df_NewAdds = df_FinalXmr.filter(
    (F.col("rightRec_MBI") == "")
    & (F.when(F.col("leftRec_MBI").isNotNull(), F.col("leftRec_MBI")).otherwise("") != "")
)

df_NewAdds_select = df_NewAdds.select(
    F.col("EAM_GROUP_ID").alias("GRGR_ID"),
    F.col("MemberID").alias("SBSB_ID"),
    F.col("leftRec_MBI").alias("MBI"),
    F.when(
        F.col("DiscCodE").isin("INBL","RESS"),
        trim(F.when(F.col("MEMD_PREM_WH_OPT").isNotNull(), F.col("MEMD_PREM_WH_OPT")).otherwise(""))
    ).otherwise("N/A").alias("SOURCE1"),
    F.when(
        F.col("DiscCodE").isin("INBL","RESS"),
        trim(F.when(F.col("BLDF_MCTR_STMT").isNotNull(), F.col("BLDF_MCTR_STMT")).otherwise(""))
    )
    .when(F.col("DiscCodE") == "MS_BLEI", "BLEI_CK")
    .when(F.col("DiscCodE") == "GRP25", "BLAD_ADDR1")
    .when(F.col("DiscCodE") == "L800", "BLDF_MCTR_STMT")
    .when(F.col("DiscCodE") == "STOCKID", "BLDF_STOCK_ID")
    .when(F.col("DiscCodE") == "MS_PWO", "MEMD_PREM_WH_OPT")
    .when(F.col("DiscCodE") == "BLDF_METH", "BLDF_BL_GEN_METH")
    .otherwise("").alias("SOURCE2"),
    F.col("leftRec_IDENTIFIEDBY").alias("IDENTIFIEDBY"),
    F.to_date(F.lit(CurrDt), "yyyy-MM-dd").alias("IDENTIFIEDDATE"),
    F.lit(None).alias("RESOLVEDDATE"),
    F.lit("OPEN").alias("STATUS"),
    F.col("leftRec_DISCREPANCY_ID").alias("DISCREPANCY"),
    F.col("DISCREPANCY_DESC").alias("NOTE"),
    F.col("PRIORITY").alias("PRIORITY"),
    F.lit(0).alias("AGE"),
    F.when(
        F.col("MemberID") == "",
        "N"
    ).otherwise(
        F.when(
            F.col("MemberID").substr(1, 3) == "000",
            "Y"
        ).otherwise("N")
    ).alias("LEGACY"),
    F.to_date(F.lit(CurrDt), "yyyy-MM-dd").alias("LAST_UPDATED_DATE")
)

df_Resolved = df_FinalXmr.filter(
    (
        F.when(F.col("leftRec_MBI").isNotNull(), F.col("leftRec_MBI")).otherwise("") == ""
    ) & (F.col("rightRec_MBI") != "")
)

df_Resolved_select = df_Resolved.select(
    F.col("ID_SK").alias("ID_SK")
)

df_StillOpen = df_FinalXmr.filter(
    (
        F.when(F.col("leftRec_MBI").isNotNull(), F.col("leftRec_MBI")).otherwise("") != ""
    ) & (F.col("rightRec_MBI") != "")
)

df_StillOpen_select = df_StillOpen.select(
    F.col("ID_SK").alias("ID_SK"),
    DaysSinceFromDate2(
        F.to_date(F.lit(CurrDt), "yyyy-MM-dd"),
        F.col("IDENTIFIEDDATE")
    ).alias("AGE"),
    F.to_date(F.lit(CurrDt), "yyyy-MM-dd").alias("LAST_UPDATED_DATE")
)

write_files(
    df_StillOpen_select,
    f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_LBILL_AGED.{RUNID}.dat",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)

write_files(
    df_Resolved_select,
    f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_LBILL_RESOLVED.{RUNID}.dat",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)

df_RmDup = dedup_sort(
    df_NewAdds_select,
    ["MBI","IDENTIFIEDBY","DISCREPANCY","IDENTIFIEDDATE"],
    []
)

df_RmDup_select = df_RmDup.select(
    F.rpad(F.col("GRGR_ID"), 8, " ").alias("GRGR_ID"),
    F.rpad(F.col("SBSB_ID"), 9, " ").alias("SBSB_ID"),
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
    F.rpad(F.col("LEGACY"), 1, " ").alias("LEGACY"),
    F.col("LAST_UPDATED_DATE").alias("LAST_UPDATED_DATE")
)

write_files(
    df_RmDup_select,
    f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_LBILL_APPEND.{RUNID}.dat",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)