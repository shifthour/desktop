# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC JOB NAME:  FacetsEAMAgentIDExtr
# MAGIC Calling Job:EAMFctsReconRptCntl
# MAGIC 
# MAGIC This job is retrieves the AgentID from Facets and compares the AgentID for each MBI  from  EAM and write the results to a file where the AgentIDs do not match.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC ========================================================================================================================================================================
# MAGIC Developer                          Date                           Project/Altiris #                              Change Description                                                    Development Project                 Reviewed By                          Reviewed On
# MAGIC ========================================================================================================================================================================
# MAGIC ReddySanam               2021-01-26                  US329820                                         Original Programming                                                    IntegrateDev2                          Jeyaprasanna                        2021-02-23
# MAGIC JohnAbraham               2021-08-11                  US391328                  Added EAMRPT Env variables and updated appropriate tables         IntegrateDev1
# MAGIC                                                                                                                with these parameters
# MAGIC Prabhu ES                    2022-03-21                  S2S                                                   MSSQL ODBC conn params added                             IntegrateDev5	Ken Bradmon	2022-06-04
# MAGIC 
# MAGIC Vamsi Aripaka              2024-04-18                 US 616210                               Added GRP_ID parameter                                                      IntegrateDev2                           Jeyaprasanna                       2024-06-20

# MAGIC This job is retrieves the AgentID from Facets and compares the AgentID for each MBI  from  EAM and write the results to a file where the AgentIDs do not match
# MAGIC Attach repid for each MBI from EAM. After the plan id is attached, it is compared between FACETS and EAM and call out if they don't match.
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
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, StructType, StructField, DateType
# COMMAND ----------
# MAGIC %run ../../../Utility_Integrate
# COMMAND ----------


FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
EAMRPTOwner = get_widget_value('EAMRPTOwner','')
eamrpt_secret_name = get_widget_value('eamrpt_secret_name','')
EAMOwner = get_widget_value('EAMOwner','')
eam_secret_name = get_widget_value('eam_secret_name','')
RUNID = get_widget_value('RUNID','')
BillDt = get_widget_value('BillDt','')
CurrDt = get_widget_value('CurrDt','')
GRP_ID = get_widget_value('GRP_ID','')

# EAM_DISC_ANALYSIS
jdbc_url_eamrpt, jdbc_props_eamrpt = get_db_config(eamrpt_secret_name)
query_EAM_DISC_ANALYSIS = (
    "SELECT DISC.ID AS ID_SK,DISC.MBI,DISC.IDENTIFIEDBY,DISC.IDENTIFIEDDATE,DISC.DISCREPANCY as DISCREPANCY_ID "
    f"FROM {EAMRPTOwner}.DISCREPANCY_ANALYSIS DISC "
    f"INNER JOIN {EAMRPTOwner}.DISCREPANCY_CONFIG CONFIG "
    "ON DISC.DISCREPANCY = CONFIG.ID "
    "WHERE STATUS = 'OPEN' "
    "and DISC.IDENTIFIEDBY = 'CURR' "
    "and CONFIG.DISCREPANCY_NAME = 'AgentID'"
)
df_EAM_DISC_ANALYSIS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_eamrpt)
    .options(**jdbc_props_eamrpt)
    .option("query", query_EAM_DISC_ANALYSIS)
    .load()
)

# DISC_CONFIG
query_DISC_CONFIG = (
    "select ID AS DISCREPANCY_ID, DISCREPANCY_NAME, DISCREPANCY_DESC, IDENTIFIEDBY, PRIORITY, ACTIVE, SOURCE1, SOURCE2 "
    f"from {EAMRPTOwner}.DISCREPANCY_CONFIG "
    "WHERE ACTIVE = 'Y' "
    "AND IDENTIFIEDBY = 'CURR' "
    "AND DISCREPANCY_NAME = 'AgentID'"
)
df_DISC_CONFIG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_eamrpt)
    .options(**jdbc_props_eamrpt)
    .option("query", query_DISC_CONFIG)
    .load()
)

# EAM_BRKR
jdbc_url_eam, jdbc_props_eam = get_db_config(eam_secret_name)
query_EAM_BRKR = (
    "select DISTINCT mi.groupnumber as GRGR_ID,spans.memberid , spans.HIC as MBI, "
    "case when spans.DateCreated <'2020-10-01' then case "
    "                                              when LTRIM(RTRIM(ISNULL(crep.repid,''))) = ''  THEN '99992GRZ' ELSE crep.repid END  "
    "                                         else case "
    "                                              when LTRIM(RTRIM(ISNULL(reps.repid,''))) = '' THEN '99992GRZ' ELSE reps.repid  END end  AS repid "
    f"from {EAMOwner}.tbENRLSpans spans "
    f"inner join {EAMOwner}.tbmemberinfo mi on spans.memcodnum=mi.memcodnum "
    f"left outer join  {EAMOwner}.salesreps crep on mi.salesrepid=crep.salesrepid "
    f"inner join {EAMOwner}.tbPlan_PBP pbp on spans.planid=pbp.planid and spans.value=pbp.pbpid "
    "left join ( "
    "SELECT MemCodNum, transid, EffectiveDate, PlanID, PBPID, receiptdate, applicationdate, salesrepid, "
    "       row_number() over(partition by memcodnum, EffectiveDate, PlanID, PBPID order by receiptdate) row "
    f"FROM {EAMOwner}.TBTRANSACTIONS (nolock) "
    "WHERE transcode in (61,71,78) and TransStatus='5' and "
    "(replycodes in ('011','100','179','023') or datecreated<'2020-10-01') "
    ") enrollments on spans.memcodnum=enrollments.memcodnum "
    "and spans.startdate=enrollments.EffectiveDate "
    "and spans.planid=enrollments.planid "
    "and spans.value=enrollments.pbpid and enrollments.row='1' "
    f"left outer join {EAMOwner}.salesreps reps on enrollments.salesrepid=reps.salesrepid "
    f"left outer join {EAMOwner}.elecappfile on enrollments.transid=elecappfile.transid "
    "where spans.spantype in ('PBP') "
    f"and '{CurrDt}' BETWEEN SPANS.STARTDATE AND SPANS.ENDDATE "
    f"and mi.GroupNumber in ({GRP_ID})"
)
df_EAM_BRKR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_eam)
    .options(**jdbc_props_eam)
    .option("query", query_EAM_BRKR)
    .load()
)

# MemberAgentInfo
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
query_MemberAgentInfo = (
    "Select distinct "
    "GRGR.GRGR_ID, "
    "SBSB.SBSB_ID, "
    "LTRIM(RTRIM(ISNULL(MEME.MEME_HICN,''))) AS MBI, "
    "SUBSTRING(LTRIM(RTRIM(ISNULL(BLCO.COAR_ID,''))),1,8) as COAR_ID "
    f"from {FacetsOwner}.CMC_GRGR_GROUP GRGR "
    f"inner join {FacetsOwner}.CMC_SBSB_SUBSC SBSB on SBSB.GRGR_CK = GRGR.GRGR_CK "
    f"inner join {FacetsOwner}.CMC_MEME_MEMBER MEME on SBSB.SBSB_CK = MEME.SBSB_CK "
    f"inner join {FacetsOwner}.CMC_MEPE_PRCS_ELIG MEPE on MEPE.MEME_CK = MEME.MEME_CK "
    f"inner join {FacetsOwner}.CMC_BLEI_ENTY_INFO BLEI on BLEI.BLEI_BILL_LEVEL_CK = SBSB.SBSB_CK "
    f"left outer join {FacetsOwner}.CMC_BLCO_COMM_ITEM BLCO on BLCO.BLEI_CK = BLEI.BLEI_CK AND BLCO.CSPI_ID = MEPE.CSPI_ID "
    "and BLCO.BLCO_EFF_DT<>BLCO.BLCO_TERM_DT "
    f"AND '{CurrDt}' BETWEEN BLCO.BLCO_EFF_DT AND BLCO.BLCO_TERM_DT "
    f"left outer join {FacetsOwner}.CMC_BLCI_CONT_ITEM BLCI on BLCI.BLEI_CK = BLEI.BLEI_CK "
    f"where GRGR.GRGR_ID IN ({GRP_ID}) and MEPE.CSPD_CAT = 'M' AND MEPE.MEPE_ELIG_IND = 'Y' "
    f"AND '{CurrDt}' BETWEEN MEPE.MEPE_EFF_DT AND MEPE.MEPE_TERM_DT"
)
df_MemberAgentInfo = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", query_MemberAgentInfo)
    .load()
)

df_Att_EAM_Brkr = df_MemberAgentInfo.crossJoin(df_EAM_BRKR).select(
    df_MemberAgentInfo["GRGR_ID"].alias("GRGR_ID"),
    df_MemberAgentInfo["SBSB_ID"].alias("SBSB_ID"),
    df_MemberAgentInfo["MBI"].alias("MBI"),
    df_MemberAgentInfo["COAR_ID"].alias("COAR_ID"),
    df_EAM_BRKR["repid"].alias("repid")
)

df_xmr = (
    df_Att_EAM_Brkr.filter(
        trim(F.coalesce(df_Att_EAM_Brkr["COAR_ID"], F.lit("")))
        != trim(F.coalesce(df_Att_EAM_Brkr["repid"], F.lit("")))
    )
    .select(
        F.lit("AgentID").alias("Field"),
        df_Att_EAM_Brkr["GRGR_ID"].alias("GRGR_ID"),
        df_Att_EAM_Brkr["SBSB_ID"].alias("SBSB_ID"),
        df_Att_EAM_Brkr["MBI"].alias("MBI"),
        F.when(
            trim(F.coalesce(df_Att_EAM_Brkr["repid"], F.lit(""))) == F.lit(""),
            F.lit("")
        ).otherwise(df_Att_EAM_Brkr["repid"]).alias("repid"),
        F.when(
            trim(F.coalesce(df_Att_EAM_Brkr["COAR_ID"], F.lit(""))) == F.lit(""),
            F.lit("")
        ).otherwise(df_Att_EAM_Brkr["COAR_ID"]).alias("COAR_ID")
    )
)

df_Att_Config = df_xmr.crossJoin(df_DISC_CONFIG).select(
    df_xmr["Field"].alias("Field"),
    df_xmr["SBSB_ID"].alias("FACETS_MemberID"),
    df_xmr["GRGR_ID"].alias("GRGR_ID"),
    df_xmr["MBI"].alias("MBI"),
    df_DISC_CONFIG["DISCREPANCY_ID"].alias("DISCREPANCY_ID"),
    df_DISC_CONFIG["DISCREPANCY_DESC"].alias("DISCREPANCY_DESC"),
    df_DISC_CONFIG["IDENTIFIEDBY"].alias("IDENTIFIEDBY"),
    df_DISC_CONFIG["PRIORITY"].alias("PRIORITY"),
    df_xmr["repid"].alias("SOURCE1"),
    df_xmr["COAR_ID"].alias("SOURCE2")
)

df_FullOuter = df_Att_Config.join(
    df_EAM_DISC_ANALYSIS,
    (df_Att_Config["MBI"] == df_EAM_DISC_ANALYSIS["MBI"])
    & (df_Att_Config["DISCREPANCY_ID"] == df_EAM_DISC_ANALYSIS["DISCREPANCY_ID"])
    & (df_Att_Config["IDENTIFIEDBY"] == df_EAM_DISC_ANALYSIS["IDENTIFIEDBY"]),
    "full"
).select(
    df_Att_Config["Field"].alias("Field"),
    df_Att_Config["FACETS_MemberID"].alias("FACETS_MemberID"),
    df_Att_Config["MBI"].alias("leftRec_MBI"),
    df_Att_Config["GRGR_ID"].alias("FACETS_GROUP_ID"),
    df_Att_Config["DISCREPANCY_ID"].alias("leftRec_DISCREPANCY_ID"),
    df_Att_Config["DISCREPANCY_DESC"].alias("DISCREPANCY_DESC"),
    df_Att_Config["IDENTIFIEDBY"].alias("leftRec_IDENTIFIEDBY"),
    df_Att_Config["PRIORITY"].alias("PRIORITY"),
    df_Att_Config["SOURCE1"].alias("SOURCE1"),
    df_Att_Config["SOURCE2"].alias("SOURCE2"),
    df_EAM_DISC_ANALYSIS["ID_SK"].alias("ID_SK"),
    df_EAM_DISC_ANALYSIS["MBI"].alias("rightRec_MBI"),
    df_EAM_DISC_ANALYSIS["IDENTIFIEDBY"].alias("rightRec_IDENTIFIEDBY"),
    df_EAM_DISC_ANALYSIS["IDENTIFIEDDATE"].alias("IDENTIFIEDDATE"),
    df_EAM_DISC_ANALYSIS["DISCREPANCY_ID"].alias("rightRec_DISCREPANCY_ID")
)

df_NewAdds = df_FullOuter.filter(
    (F.coalesce(df_FullOuter["rightRec_MBI"], F.lit("")) == "") &
    (F.coalesce(df_FullOuter["leftRec_MBI"], F.lit("")) != "")
).select(
    df_FullOuter["FACETS_GROUP_ID"].alias("GRGR_ID"),
    df_FullOuter["FACETS_MemberID"].alias("SBSB_ID"),
    df_FullOuter["leftRec_MBI"].alias("MBI"),
    df_FullOuter["SOURCE1"].alias("SOURCE1"),
    df_FullOuter["SOURCE2"].alias("SOURCE2"),
    df_FullOuter["leftRec_IDENTIFIEDBY"].alias("IDENTIFIEDBY"),
    F.to_date(F.lit(CurrDt), "yyyy-MM-dd").alias("IDENTIFIEDDATE"),
    F.lit(None).alias("RESOLVEDDATE"),
    F.lit("OPEN").alias("STATUS"),
    df_FullOuter["leftRec_DISCREPANCY_ID"].alias("DISCREPANCY"),
    df_FullOuter["DISCREPANCY_DESC"].alias("NOTE"),
    df_FullOuter["PRIORITY"].alias("PRIORITY"),
    F.lit(0).alias("AGE"),
    F.when(
        df_FullOuter["FACETS_MemberID"] == F.lit(""),
        F.lit("N")
    ).otherwise(
        F.when(
            F.substring(df_FullOuter["FACETS_MemberID"], 1, 3) == F.lit("000"),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    ).alias("LEGACY"),
    F.to_date(F.lit(CurrDt), "yyyy-MM-dd").alias("LAST_UPDATED_DATE")
)

df_Resolved = df_FullOuter.filter(
    (F.coalesce(df_FullOuter["leftRec_MBI"], F.lit("")) == "") &
    (F.coalesce(df_FullOuter["rightRec_MBI"], F.lit("")) != "")
).select(
    df_FullOuter["ID_SK"].alias("ID_SK")
)

df_StillOpen = df_FullOuter.filter(
    (F.coalesce(df_FullOuter["leftRec_MBI"], F.lit("")) != "") &
    (F.coalesce(df_FullOuter["rightRec_MBI"], F.lit("")) != "")
).select(
    df_FullOuter["ID_SK"].alias("ID_SK"),
    DaysSinceFromDate2(F.to_date(F.lit(CurrDt), "yyyy-MM-dd"), df_FullOuter["IDENTIFIEDDATE"]).alias("AGE"),
    F.to_date(F.lit(CurrDt), "yyyy-MM-dd").alias("LAST_UPDATED_DATE")
)

df_StillOpen_write = df_StillOpen.select("ID_SK", "AGE", "LAST_UPDATED_DATE")
write_files(
    df_StillOpen_write,
    f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_AGNT_AGED.{RUNID}.dat",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)

df_Resolved_write = df_Resolved.select("ID_SK")
write_files(
    df_Resolved_write,
    f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_AGNT_RESOLVED.{RUNID}.dat",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)

df_dedup = dedup_sort(
    df_NewAdds,
    partition_cols=["MBI","IDENTIFIEDBY","IDENTIFIEDDATE","DISCREPANCY"],
    sort_cols=[("MBI","A"),("IDENTIFIEDBY","A"),("IDENTIFIEDDATE","A"),("DISCREPANCY","A")]
)

df_NewAddsFile = df_dedup.select(
    df_dedup["GRGR_ID"],
    df_dedup["SBSB_ID"],
    df_dedup["MBI"],
    df_dedup["SOURCE1"],
    df_dedup["SOURCE2"],
    df_dedup["IDENTIFIEDBY"],
    df_dedup["IDENTIFIEDDATE"],
    df_dedup["RESOLVEDDATE"],
    df_dedup["STATUS"],
    df_dedup["DISCREPANCY"],
    df_dedup["NOTE"],
    df_dedup["PRIORITY"],
    df_dedup["AGE"],
    F.rpad(df_dedup["LEGACY"], 1, " ").alias("LEGACY"),
    df_dedup["LAST_UPDATED_DATE"]
)

write_files(
    df_NewAddsFile,
    f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_AGNT_APPEND.{RUNID}.dat",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)