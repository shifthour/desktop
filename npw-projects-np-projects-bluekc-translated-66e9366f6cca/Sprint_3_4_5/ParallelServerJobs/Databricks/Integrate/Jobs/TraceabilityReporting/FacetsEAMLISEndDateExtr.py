# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC JOB NAME:  FacetsEAMLISEndDateExtr
# MAGIC Calling Job:EAMFctsReconRptCntl
# MAGIC 
# MAGIC This job is retrieves the MEMD_EVENT_TERM_DT  from Facets and compares the LISEndDate for each MBI  from  EAM and write the results to a file where the Dates do not match.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC ========================================================================================================================================================================
# MAGIC Developer                          Date                           Project/Altiris #                              Change Description                                                    Development Project                 Reviewed By                          Reviewed On
# MAGIC ========================================================================================================================================================================                    
# MAGIC Arpitha V                     2024-03-20                        US 611997                                Original Programming                                                        IntegrateDev2                       Jeyaprasanna                      2024-06-21

# MAGIC This job is retrieves the MEMD_EVENT_TERM_DT from Facets and compares the LISEndDate for each MBI  from  EAM and write the results to a file where the Dates do not match
# MAGIC Attach LISEndDate for each MBI from EAM. After the LISEndDate is attached, it is compared between FACETS and EAM and call out if they don't match.
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
from pyspark.sql.functions import col, lit, when, substring, concat
from pyspark.sql.functions import isnull
from pyspark.sql.functions import rpad
# COMMAND ----------
# MAGIC %run ../../../Utility_Integrate
# COMMAND ----------

jdbc_url_eamrpt, jdbc_props_eamrpt = None, None
jdbc_url_eam, jdbc_props_eam = None, None
jdbc_url_facets, jdbc_props_facets = None, None

FacetsOwner = get_widget_value("FacetsOwner","")
facets_secret_name = get_widget_value("facets_secret_name","")
EAMOwner = get_widget_value("EAMOwner","")
eam_secret_name = get_widget_value("eam_secret_name","")
EAMRPTOwner = get_widget_value("EAMRPTOwner","")
eamrpt_secret_name = get_widget_value("eamrpt_secret_name","")
RUNID = get_widget_value("RUNID","")
CurrDt = get_widget_value("CurrDt","")
GRP_ID = get_widget_value("GRP_ID","")

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
  AND DISC.IDENTIFIEDBY = 'CURR'
  AND CONFIG.DISCREPANCY_NAME = 'LIS End Date'
        """
    )
    .load()
)

df_DISC_CONFIG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_eamrpt)
    .options(**jdbc_props_eamrpt)
    .option(
        "query",
        f"""
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
  AND DISCREPANCY_NAME = 'LIS End Date'
        """
    )
    .load()
)

jdbc_url_eam, jdbc_props_eam = get_db_config(eam_secret_name)
df_EAM_tbMemberInfoLISLog = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_eam)
    .options(**jdbc_props_eam)
    .option(
        "query",
        f"""
select DISTINCT mi.HIC as MBI,
                Mbrinfo.LowIncomeCoPayEndDate
from {EAMOwner}.tbMemberInfoLISLog Mbrinfo
inner join {EAMOwner}.tbEENRLMembers mi on Mbrinfo.MemCodNum=mi.MemCodNum
inner join {EAMOwner}.tbMemberInfo info ON info.MemCodNum = mi.MemCodNum
where Mbrinfo.Valid in ('1')
  and Mbrinfo.[Current] in ('1')
  and info.GroupNumber in ({GRP_ID})
        """
    )
    .load()
)

jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
df_CMC_MEMD_MECR_DETL = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option(
        "query",
        f"""
select distinct
       GRGR.GRGR_ID,
       SBSB.SBSB_ID,
       LTRIM(RTRIM(ISNULL(MBR.MEME_HICN,''))) AS MBI,
       ISNULL(MEMD.MEMD_HCFA_TERM_DT,'') as MEMD_HCFA_TERM_DT
from {FacetsOwner}.CMC_MEMD_MECR_DETL MEMD
inner join {FacetsOwner}.CMC_MEME_MEMBER MBR on MBR.MEME_CK = MEMD.MEME_CK
inner join {FacetsOwner}.CMC_SBSB_SUBSC SBSB on SBSB.SBSB_CK = MBR.SBSB_CK
inner join {FacetsOwner}.CMC_GRGR_GROUP GRGR on GRGR.GRGR_CK = SBSB.GRGR_CK
where GRGR.GRGR_ID IN ({GRP_ID})
  AND MEMD.MEMD_EVENT_CD in ('LICS')
  AND '{CurrDt}' BETWEEN MEMD.MEMD_HCFA_EFF_DT AND MEMD.MEMD_HCFA_TERM_DT
        """
    )
    .load()
)

df_Att_EAM_tbMemberInfoLISLog = (
    df_CMC_MEMD_MECR_DETL.crossJoin(df_EAM_tbMemberInfoLISLog)
    .select(
        df_CMC_MEMD_MECR_DETL["GRGR_ID"].alias("GRGR_ID"),
        df_CMC_MEMD_MECR_DETL["SBSB_ID"].alias("SBSB_ID"),
        df_CMC_MEMD_MECR_DETL["MBI"].alias("MBI"),
        df_EAM_tbMemberInfoLISLog["LowIncomeCoPayEndDate"].alias("LowIncomeCoPayEndDate"),
        df_CMC_MEMD_MECR_DETL["MEMD_HCFA_TERM_DT"].alias("MEMD_HCFA_TERM_DT"),
    )
)

df_Xmr_interim = (
    df_Att_EAM_tbMemberInfoLISLog
    .withColumn(
        "svLISEndDt",
        FORMAT.DATE.EE(col("LowIncomeCoPayEndDate"), lit("SYBASE"), lit("TIMESTAMP"), lit("CCYY-MM-DD"))
    )
    .withColumn(
        "svTermDt",
        FORMAT.DATE.EE(col("MEMD_HCFA_TERM_DT"), lit("SYBASE"), lit("TIMESTAMP"), lit("CCYY-MM-DD"))
    )
)

df_Xmr = (
    df_Xmr_interim
    .filter(col("LowIncomeCoPayEndDate") != col("MEMD_HCFA_TERM_DT"))
    .select(
        lit("LIS End Date").alias("Field"),
        col("GRGR_ID").alias("GRGR_ID"),
        col("SBSB_ID").alias("SBSB_ID"),
        col("MBI").alias("MBI"),
        concat(
            substring(col("svLISEndDt"), 6, 2),
            lit("/"),
            substring(col("svLISEndDt"), 9, 2),
            lit("/"),
            substring(col("svLISEndDt"), 1, 4)
        ).alias("LowIncomeCoPayEndDate"),
        concat(
            substring(col("svTermDt"), 6, 2),
            lit("/"),
            substring(col("svTermDt"), 9, 2),
            lit("/"),
            substring(col("svTermDt"), 1, 4)
        ).alias("MEMD_HCFA_TERM_DT"),
    )
)

df_StrtDt_Config = (
    df_Xmr.crossJoin(df_DISC_CONFIG)
    .select(
        df_Xmr["Field"].alias("Field"),
        df_Xmr["GRGR_ID"].alias("GRGR_ID"),
        df_Xmr["SBSB_ID"].alias("SBSB_ID"),
        df_Xmr["MBI"].alias("MBI"),
        df_DISC_CONFIG["DISCREPANCY_ID"].alias("DISCREPANCY_ID"),
        df_DISC_CONFIG["DISCREPANCY_NAME"].alias("DISCREPANCY_NAME"),
        df_DISC_CONFIG["DISCREPANCY_DESC"].alias("DISCREPANCY_DESC"),
        df_DISC_CONFIG["IDENTIFIEDBY"].alias("IDENTIFIEDBY"),
        df_DISC_CONFIG["PRIORITY"].alias("PRIORITY"),
        rpad(df_DISC_CONFIG["ACTIVE"], 1, " ").alias("ACTIVE"),
        df_Xmr["LowIncomeCoPayEndDate"].alias("SOURCE1"),
        df_Xmr["MEMD_HCFA_TERM_DT"].alias("SOURCE2"),
    )
)

df_FullOuter = (
    df_StrtDt_Config.join(
        df_EAM_DISC_ANALYSIS,
        (
            (df_StrtDt_Config["MBI"] == df_EAM_DISC_ANALYSIS["MBI"])
            & (df_StrtDt_Config["DISCREPANCY_ID"] == df_EAM_DISC_ANALYSIS["DISCREPANCY_ID"])
            & (df_StrtDt_Config["IDENTIFIEDBY"] == df_EAM_DISC_ANALYSIS["IDENTIFIEDBY"])
        ),
        "full"
    )
    .select(
        df_StrtDt_Config["Field"].alias("Field"),
        df_StrtDt_Config["SBSB_ID"].alias("FACETS_MemberID"),
        df_StrtDt_Config["MBI"].alias("leftRec_MBI"),
        df_StrtDt_Config["GRGR_ID"].alias("FACETS_GROUP_ID"),
        df_StrtDt_Config["DISCREPANCY_ID"].alias("leftRec_DISCREPANCY_ID"),
        df_StrtDt_Config["DISCREPANCY_DESC"].alias("DISCREPANCY_DESC"),
        df_StrtDt_Config["IDENTIFIEDBY"].alias("leftRec_IDENTIFIEDBY"),
        df_StrtDt_Config["PRIORITY"].alias("PRIORITY"),
        df_StrtDt_Config["SOURCE1"].alias("SOURCE1"),
        df_StrtDt_Config["SOURCE2"].alias("SOURCE2"),
        df_EAM_DISC_ANALYSIS["ID_SK"].alias("ID_SK"),
        df_EAM_DISC_ANALYSIS["MBI"].alias("rightRec_MBI"),
        df_EAM_DISC_ANALYSIS["IDENTIFIEDBY"].alias("rightRec_IDENTIFIEDBY"),
        df_EAM_DISC_ANALYSIS["IDENTIFIEDDATE"].alias("IDENTIFIEDDATE"),
        df_EAM_DISC_ANALYSIS["DISCREPANCY_ID"].alias("rightRec_DISCREPANCY_ID"),
    )
)

df_NewAdds = (
    df_FullOuter
    .filter(
        (col("rightRec_MBI") == "")
        & (
            when(col("leftRec_MBI").isNotNull(), col("leftRec_MBI"))
            .otherwise(lit(""))
            != lit("")
        )
    )
    .select(
        col("FACETS_GROUP_ID").alias("GRGR_ID"),
        col("FACETS_MemberID").alias("SBSB_ID"),
        col("leftRec_MBI").alias("MBI"),
        col("SOURCE1").alias("SOURCE1"),
        col("SOURCE2").alias("SOURCE2"),
        col("leftRec_IDENTIFIEDBY").alias("IDENTIFIEDBY"),
        StringToDate(lit(CurrDt), lit("%yyyy-%mm-%dd")).alias("IDENTIFIEDDATE"),
        when(lit(True), None).alias("RESOLVEDDATE"),
        lit("OPEN").alias("STATUS"),
        col("leftRec_DISCREPANCY_ID").alias("DISCREPANCY"),
        col("DISCREPANCY_DESC").alias("NOTE"),
        col("PRIORITY").alias("PRIORITY"),
        lit(0).alias("AGE"),
        when(col("FACETS_MemberID") == "", lit("N"))
        .otherwise(
            when(substring(col("FACETS_MemberID"), 1, 3) == "000", lit("Y"))
            .otherwise(lit("N"))
        ).alias("LEGACY"),
        StringToDate(lit(CurrDt), lit("%yyyy-%mm-%dd")).alias("LAST_UPDATED_DATE"),
    )
)

df_StillOpen = (
    df_FullOuter
    .filter(
        (
            when(col("leftRec_MBI").isNotNull(), col("leftRec_MBI"))
            .otherwise(lit(""))
            != lit("")
        )
        & (col("rightRec_MBI") != "")
    )
    .select(
        col("ID_SK").alias("ID_SK"),
        DaysSinceFromDate2(
            StringToDate(lit(CurrDt), lit("%yyyy-%mm-%dd")),
            col("IDENTIFIEDDATE")
        ).alias("AGE"),
        StringToDate(lit(CurrDt), lit("%yyyy-%mm-%dd")).alias("LAST_UPDATED_DATE"),
    )
)

df_Resolved = (
    df_FullOuter
    .filter(
        (
            when(col("leftRec_MBI").isNotNull(), col("leftRec_MBI"))
            .otherwise(lit(""))
            == lit("")
        )
        & (col("rightRec_MBI") != "")
    )
    .select(
        col("ID_SK").alias("ID_SK"),
    )
)

df_StillOpen_final = df_StillOpen.select("ID_SK","AGE","LAST_UPDATED_DATE")
write_files(
    df_StillOpen_final,
    f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_LISENDDATE_AGED.{RUNID}.dat",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)

df_Resolved_final = df_Resolved.select("ID_SK")
write_files(
    df_Resolved_final,
    f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_LISENDDATE_RESOLVED.{RUNID}.dat",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)

df_RmDup_interim = dedup_sort(
    df_NewAdds,
    ["MBI","IDENTIFIEDBY","IDENTIFIEDDATE","DISCREPANCY"],
    []
)

df_RmDup = df_RmDup_interim.select(
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
    rpad(col("LEGACY"), 1, " ").alias("LEGACY"),
    col("LAST_UPDATED_DATE").alias("LAST_UPDATED_DATE"),
)

write_files(
    df_RmDup,
    f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_LISENDDATE_APPEND.{RUNID}.dat",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)