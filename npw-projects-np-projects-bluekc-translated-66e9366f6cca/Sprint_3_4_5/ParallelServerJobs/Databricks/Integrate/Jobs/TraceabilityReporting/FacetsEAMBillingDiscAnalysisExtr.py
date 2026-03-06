# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Calling Job:EAMFctsReconRptCntl
# MAGIC 
# MAGIC Description:This Job Compares Details table with discrepancy analysis table with a full outer join and identifies whether a specific discrepancy needs to be closed/added/aged
# MAGIC 
# MAGIC Aged means number of days the discrepancy has been open
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC ===================================================================================================================================================================
# MAGIC Developer                          Date                           Project/Altiris #                              Change Description                                                    Development Project               Code Reviewer                Review Date
# MAGIC ==================================================================================================================================================================
# MAGIC ReddySanam               2021-01-13                  US329820                                         Original Programming                                                    IntegrateDev2                     Jeyaprasanna                  2021-02-04
# MAGIC ReddySanam               2021-02-10                  US329820                                        Updated logic to process MBIs                                      IntegrateDev2                     Jeyaprasanna                  2021-02-10
# MAGIC                                                                                                                                   missing eligibility in EAM/FACETS
# MAGIC ReddySanam              2021-02-17                   US329820                                        Added list of mismatch config                                         IntegrateDev2                     Jeyaprasanna                  2021-02-17
# MAGIC                                                                                                                                  entries to where clause in EAM_DIS_ANALYSIS
# MAGIC                                                                                                                                  and DISC_CONFIG odbc connector stages
# MAGIC                                                                                                                                  Added annotation that the APPEND file gets 
# MAGIC                                                                                                                                  used in RXID TC72 job
# MAGIC JohnAbraham               2021-08-11                  US391328                   Added EAMRPT Env variables and updated appropriate tables         IntegrateDev1
# MAGIC                                                                                                                with these parameters

# MAGIC This Part Identifies if an MBI has multuple entries in EAM or FACETS under any Mismatch Category.
# MAGIC This part will remove MBI with multiple entries and retain one entry with a single Mismatch error
# MAGIC This file is also used in  EAM RXID TC 72 Rule job -FacetsEAMRxIDPBPExtr. Please address that job if the file name/metadata gets changed here
# MAGIC This Job Compares Details table with discrepancy analysis table with a full outer join and identifies whether a specific discrepancy needs to be closed/added/aged
# MAGIC 
# MAGIC aged means number of days the discrepancy has been open
# MAGIC This file will have newly found discrepancies
# MAGIC This file will have resolved discrepancies between last run and current run
# MAGIC This file will have discrepancies that are still open
# MAGIC This is to remove duplicates if an MBI has more than one entry
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, when, lit, coalesce, rpad, count
# COMMAND ----------
# MAGIC %run ../../../Utility_Integrate
# COMMAND ----------


EAMRPTOwner = get_widget_value('EAMRPTOwner','')
eamrpt_secret_name = get_widget_value('eamrpt_secret_name','')
runid = get_widget_value('RUNID','')
currdt = get_widget_value('CurrDt','')

jdbc_url_eam, jdbc_props_eam = get_db_config(eamrpt_secret_name)

extract_query_EAM_DISC_ANALYSIS = (
    f"SELECT DISC.ID AS ID_SK,DISC.MBI,DISC.IDENTIFIEDBY,DISC.IDENTIFIEDDATE,DISC.DISCREPANCY as DISCREPANCY_ID "
    f"FROM {EAMRPTOwner}.DISCREPANCY_ANALYSIS DISC "
    f"INNER JOIN {EAMRPTOwner}.DISCREPANCY_CONFIG CONFIG "
    f"ON DISC.DISCREPANCY = CONFIG.ID "
    f"WHERE STATUS = 'OPEN' "
    f"and DISC.IDENTIFIEDBY = 'BLMM' "
    f"and CONFIG.DISCREPANCY_NAME  in "
    f"('NO_ELIG_EAM','NO_ELIG_FACETS','MBI_DUPE_FACETS','MBI_DUPE_EAM','MemberID','PlanID','PBP_EFF_DT','PBPID','PremWithholdOption','PrimaryRXID','SCCCode','GroupID','MedicalProductID','PenaltyAmount','MBI','Subsidy Level','DentalProductID','LIS Effective Date','PrimaryRXGroup','PrimaryRXBin','PrimaryRXPCN')"
)

df_EAM_DISC_ANALYSIS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_eam)
    .options(**jdbc_props_eam)
    .option("query", extract_query_EAM_DISC_ANALYSIS)
    .load()
)

extract_query_DISC_CONFIG = (
    f"select ID AS DISCREPANCY_ID,DISCREPANCY_NAME,DISCREPANCY_DESC,IDENTIFIEDBY,PRIORITY,ACTIVE,SOURCE1,SOURCE2 "
    f"from {EAMRPTOwner}.DISCREPANCY_CONFIG "
    f"WHERE ACTIVE = 'Y' "
    f"AND IDENTIFIEDBY = 'BLMM' "
    f"AND DISCREPANCY_NAME in "
    f"('NO_ELIG_EAM','NO_ELIG_FACETS','MBI_DUPE_FACETS','MBI_DUPE_EAM','MemberID','PlanID','PBP_EFF_DT','PBPID','PremWithholdOption','PrimaryRXID','SCCCode','GroupID','MedicalProductID','PenaltyAmount','MBI','Subsidy Level','DentalProductID','LIS Effective Date','PrimaryRXGroup','PrimaryRXBin','PrimaryRXPCN')"
)

df_DISC_CONFIG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_eam)
    .options(**jdbc_props_eam)
    .option("query", extract_query_DISC_CONFIG)
    .load()
)

schema_BillingDisc = StructType([
    StructField("Field", StringType(), True),
    StructField("EAM_MemberID", StringType(), True),
    StructField("FACETS_MemberID", StringType(), True),
    StructField("EAM", StringType(), True),
    StructField("FACETS", StringType(), True),
    StructField("EAM_MBI", StringType(), True),
    StructField("FACETS_MBI", StringType(), True),
    StructField("EAM_GROUP_ID", StringType(), True),
    StructField("FACETS_GROUP_ID", StringType(), True)
])

df_BillingDisc = (
    spark.read.format("csv")
    .option("header", True)
    .option("delimiter", "|")
    .option("quote", "\"")
    .option("nullValue", None)
    .schema(schema_BillingDisc)
    .load(f"{adls_path_publish}/external/FACETS_EAM_BillingAttr.{runid}.dat")
)

df_Map_Main = df_BillingDisc.filter(
    ~(
        (coalesce(trim(col("EAM_MBI")), lit("")) == "") &
        (coalesce(trim(col("FACETS_MBI")), lit("")) == "")
    )
).select(
    col("Field").alias("Field"),
    when(
        coalesce(trim(col("EAM_MBI")), lit("")) == "",
        coalesce(trim(col("FACETS_MBI")), lit(""))
    ).otherwise(coalesce(trim(col("EAM_MBI")), lit(""))).alias("MBI"),
    col("EAM_MemberID").alias("EAM_MemberID"),
    col("FACETS_MemberID").alias("FACETS_MemberID"),
    col("EAM").alias("EAM"),
    col("FACETS").alias("FACETS"),
    col("EAM_MBI").alias("EAM_MBI"),
    col("FACETS_MBI").alias("FACETS_MBI"),
    col("EAM_GROUP_ID").alias("EAM_GROUP_ID"),
    col("FACETS_GROUP_ID").alias("FACETS_GROUP_ID")
)

df_Map_Aggr_EAM = df_BillingDisc.filter(
    (coalesce(trim(col("EAM_MBI")), lit("")) != "") &
    (coalesce(trim(col("FACETS_MBI")), lit("")) != "")
).select(
    lit("EAM").alias("Source"),
    col("Field").alias("Field"),
    col("EAM_MBI").alias("MBI"),
    col("EAM_MemberID").alias("MemberID")
)

df_Map_Aggr_Facets = df_BillingDisc.filter(
    (coalesce(trim(col("EAM_MBI")), lit("")) != "") &
    (coalesce(trim(col("FACETS_MBI")), lit("")) != "")
).select(
    lit("FACETS").alias("Source"),
    col("Field").alias("Field"),
    col("FACETS_MBI").alias("MBI"),
    col("FACETS_MemberID").alias("MemberID")
)

df_Fnl_Srcs = df_Map_Aggr_EAM.union(df_Map_Aggr_Facets)

df_RmDup_EAM = dedup_sort(df_Fnl_Srcs, ["Source","Field","MBI","MemberID"], [])

df_Cnt_Member = (
    df_RmDup_EAM.groupBy("Source","Field","MBI")
    .agg(count("*").alias("MemberCount"))
)

df_Ref_MemCnt = df_Cnt_Member.filter(col("MemberCount") > 1).select(
    col("Source").alias("Source"),
    col("MBI").alias("MBI"),
    col("MemberCount").alias("MemberCount")
)

df_RmDupMBI = dedup_sort(df_Ref_MemCnt, ["MBI"], [])

df_Map_Main_alias = df_Map_Main.alias("Main")
df_Ref_DupMBI_alias = df_RmDupMBI.alias("Ref_DupMBI")
df_join_LkpDupes = df_Map_Main_alias.join(
    df_Ref_DupMBI_alias,
    df_Map_Main_alias["EAM_MBI"] == df_Ref_DupMBI_alias["MBI"],
    "left"
)

df_MBI_Dupes = df_join_LkpDupes.filter(
    df_Ref_DupMBI_alias["MBI"].isNotNull()
).select(
    col("Main.Field").alias("Field"),
    col("Main.MBI").alias("MBI"),
    col("Main.EAM_MemberID").alias("EAM_MemberID"),
    col("Main.FACETS_MemberID").alias("FACETS_MemberID"),
    col("Main.EAM").alias("EAM"),
    col("Main.FACETS").alias("FACETS"),
    col("Main.EAM_MBI").alias("EAM_MBI"),
    col("Main.FACETS_MBI").alias("FACETS_MBI"),
    col("Main.EAM_GROUP_ID").alias("EAM_GROUP_ID"),
    col("Main.FACETS_GROUP_ID").alias("FACETS_GROUP_ID"),
    col("Ref_DupMBI.Source").alias("Source"),
    col("Ref_DupMBI.MemberCount").alias("MemberCount")
)

df_NoMBI_Dupes = df_join_LkpDupes.filter(
    df_Ref_DupMBI_alias["MBI"].isNull()
).select(
    col("Main.Field").alias("Field"),
    col("Main.MBI").alias("MBI"),
    col("Main.EAM_MemberID").alias("EAM_MemberID"),
    col("Main.FACETS_MemberID").alias("FACETS_MemberID"),
    col("Main.EAM").alias("EAM"),
    col("Main.FACETS").alias("FACETS"),
    col("Main.EAM_MBI").alias("EAM_MBI"),
    col("Main.FACETS_MBI").alias("FACETS_MBI"),
    col("Main.EAM_GROUP_ID").alias("EAM_GROUP_ID"),
    col("Main.FACETS_GROUP_ID").alias("FACETS_GROUP_ID")
)

df_Dupes = df_MBI_Dupes.select(
    lit(100).alias("ID"),
    col("MBI"),
    when(col("Source") == lit("EAM"), lit("MBI_DUPE_EAM")).otherwise(lit("MBI_DUPE_FACETS")).alias("Field"),
    col("EAM_MemberID"),
    col("FACETS_MemberID"),
    col("EAM"),
    col("FACETS"),
    col("EAM_MBI"),
    col("FACETS_MBI"),
    col("EAM_GROUP_ID"),
    col("FACETS_GROUP_ID")
)

df_RmDup_MBI = dedup_sort(df_Dupes, ["Field", "EAM_MBI"], [])

df_MultipleMem = df_RmDup_MBI.select(
    col("MBI").alias("MBI"),
    col("Field").alias("Field"),
    col("EAM_MemberID").alias("EAM_MemberID"),
    col("FACETS_MemberID").alias("FACETS_MemberID"),
    col("EAM").alias("EAM"),
    col("FACETS").alias("FACETS"),
    col("EAM_MBI").alias("EAM_MBI"),
    col("FACETS_MBI").alias("FACETS_MBI"),
    col("EAM_GROUP_ID").alias("EAM_GROUP_ID"),
    col("FACETS_GROUP_ID").alias("FACETS_GROUP_ID")
)

df_Fnl = (
    df_MultipleMem.union(
        df_NoMBI_Dupes.select(
            col("Field"),
            col("MBI"),
            col("EAM_MemberID"),
            col("FACETS_MemberID"),
            col("EAM"),
            col("FACETS"),
            col("EAM_MBI"),
            col("FACETS_MBI"),
            col("EAM_GROUP_ID"),
            col("FACETS_GROUP_ID")
        )
    )
)

df_Fnl_alias = df_Fnl.alias("Att_config")
df_DISC_CONFIG_alias = df_DISC_CONFIG.alias("Ref_Disc_Config")
df_Lkp_Config = df_Fnl_alias.crossJoin(df_DISC_CONFIG_alias).select(
    col("Att_config.Field").alias("Field"),
    col("Att_config.MBI").alias("MBI"),
    col("Att_config.EAM_MemberID").alias("EAM_MemberID"),
    col("Att_config.FACETS_MemberID").alias("FACETS_MemberID"),
    col("Att_config.EAM").alias("EAM"),
    col("Att_config.FACETS").alias("FACETS"),
    col("Att_config.EAM_MBI").alias("EAM_MBI"),
    col("Att_config.FACETS_MBI").alias("FACETS_MBI"),
    col("Att_config.EAM_GROUP_ID").alias("EAM_GROUP_ID"),
    col("Att_config.FACETS_GROUP_ID").alias("FACETS_GROUP_ID"),
    col("Ref_Disc_Config.DISCREPANCY_ID").alias("DISCREPANCY_ID"),
    col("Ref_Disc_Config.DISCREPANCY_DESC").alias("DISCREPANCY_DESC"),
    col("Ref_Disc_Config.IDENTIFIEDBY").alias("IDENTIFIEDBY"),
    col("Ref_Disc_Config.PRIORITY").alias("PRIORITY"),
    col("Ref_Disc_Config.SOURCE1").alias("SOURCE1"),
    col("Ref_Disc_Config.SOURCE2").alias("SOURCE2")
)

df_Details = df_Lkp_Config

df_FullOuter_alias_left = df_Details.alias("Details")
df_FullOuter_alias_right = df_EAM_DISC_ANALYSIS.alias("DISC_Analysis")
df_FullOuter = df_FullOuter_alias_left.join(
    df_FullOuter_alias_right,
    [
        df_FullOuter_alias_left["MBI"] == df_FullOuter_alias_right["MBI"],
        df_FullOuter_alias_left["DISCREPANCY_ID"] == df_FullOuter_alias_right["DISCREPANCY_ID"],
        df_FullOuter_alias_left["IDENTIFIEDBY"] == df_FullOuter_alias_right["IDENTIFIEDBY"]
    ],
    "fullouter"
)

df_FinalXmr = df_FullOuter.select(
    col("Details.Field").alias("Field"),
    col("Details.MBI").alias("leftRec_MBI"),
    col("Details.EAM_MemberID").alias("EAM_MemberID"),
    col("Details.FACETS_MemberID").alias("FACETS_MemberID"),
    col("Details.EAM").alias("EAM"),
    col("Details.FACETS").alias("FACETS"),
    col("Details.EAM_MBI").alias("EAM_MBI"),
    col("Details.FACETS_MBI").alias("FACETS_MBI"),
    col("Details.EAM_GROUP_ID").alias("EAM_GROUP_ID"),
    col("Details.FACETS_GROUP_ID").alias("FACETS_GROUP_ID"),
    col("Details.DISCREPANCY_ID").alias("leftRec_DISCREPANCY_ID"),
    col("Details.DISCREPANCY_DESC").alias("DISCREPANCY_DESC"),
    col("Details.IDENTIFIEDBY").alias("leftRec_IDENTIFIEDBY"),
    col("Details.PRIORITY").alias("PRIORITY"),
    col("Details.SOURCE1").alias("SOURCE1"),
    col("Details.SOURCE2").alias("SOURCE2"),
    col("DISC_Analysis.ID_SK").alias("ID_SK"),
    col("DISC_Analysis.MBI").alias("rightRec_MBI"),
    col("DISC_Analysis.IDENTIFIEDBY").alias("rightRec_IDENTIFIEDBY"),
    col("DISC_Analysis.IDENTIFIEDDATE").alias("IDENTIFIEDDATE"),
    col("DISC_Analysis.DISCREPANCY_ID").alias("rightRec_DISCREPANCY_ID")
)

df_Adds = df_FinalXmr.filter(
    (coalesce(col("rightRec_MBI"), lit("")) == "") &
    (coalesce(col("leftRec_MBI"), lit("")) != "")
).select(
    when(
        coalesce(trim(col("EAM_GROUP_ID")), lit("")) == "",
        col("FACETS_GROUP_ID")
    ).otherwise(col("EAM_GROUP_ID")).alias("GRGR_ID"),
    when(
        coalesce(trim(col("EAM_MemberID")), lit("")) == "",
        col("FACETS_MemberID")
    ).otherwise(col("EAM_MemberID")).alias("SBSB_ID"),
    col("leftRec_MBI").alias("MBI"),
    coalesce(trim(col("EAM")), lit("")).alias("SOURCE1"),
    coalesce(trim(col("FACETS")), lit("")).alias("SOURCE2"),
    col("leftRec_IDENTIFIEDBY").alias("IDENTIFIEDBY"),
    StringToDate(currdt, "%yyyy-%mm-%dd").alias("IDENTIFIEDDATE"),
    SetNull().alias("RESOLVEDDATE"),
    lit("OPEN").alias("STATUS"),
    col("leftRec_DISCREPANCY_ID").alias("DISCREPANCY"),
    col("DISCREPANCY_DESC").alias("NOTE"),
    col("PRIORITY").alias("PRIORITY"),
    lit(0).alias("AGE"),
    when(
        (col("EAM_MemberID") == lit("")) & (col("FACETS_MemberID") != lit("")),
        when(col("FACETS_MemberID").substr(1,3) == lit("000"), lit("Y")).otherwise(lit("N"))
    ).otherwise(
        when(col("EAM_MemberID").substr(1,3) == lit("000"), lit("Y")).otherwise(lit("N"))
    ).alias("LEGACY"),
    StringToDate(currdt, "%yyyy-%mm-%dd").alias("LAST_UPDATED_DATE")
)

df_Resolved = df_FinalXmr.filter(
    (coalesce(col("leftRec_MBI"), lit("")) == "") &
    (coalesce(col("rightRec_MBI"), lit("")) != "")
).select(
    col("ID_SK").alias("ID_SK")
)

df_StillOpen = df_FinalXmr.filter(
    (coalesce(col("leftRec_MBI"), lit("")) != "") &
    (coalesce(col("rightRec_MBI"), lit("")) != "")
).select(
    col("ID_SK").alias("ID_SK"),
    DaysSinceFromDate2(StringToDate(currdt, "%yyyy-%mm-%dd"), col("IDENTIFIEDDATE")).alias("AGE"),
    StringToDate(currdt, "%yyyy-%mm-%dd").alias("LAST_UPDATED_DATE")
)

write_files(
    df_Resolved.select(rpad("ID_SK",255," ").alias("ID_SK")),
    f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_BLMM_RESOLVED.{runid}.dat",
    delimiter="|",
    mode="overwrite",
    is_parquet=False,
    header=True,
    quote="\"",
    nullValue=None
)

write_files(
    df_StillOpen.select(
        rpad("ID_SK",255," ").alias("ID_SK"),
        rpad("AGE",255," ").alias("AGE"),
        rpad("LAST_UPDATED_DATE",255," ").alias("LAST_UPDATED_DATE")
    ),
    f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_BLMM_AGED.{runid}.dat",
    delimiter="|",
    mode="overwrite",
    is_parquet=False,
    header=True,
    quote="\"",
    nullValue=None
)

df_RmDup_Key = dedup_sort(
    df_Adds,
    ["MBI","IDENTIFIEDBY","IDENTIFIEDDATE","DISCREPANCY"],
    []
)

df_NewAdds = df_RmDup_Key.select(
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

write_files(
    df_NewAdds.select(
        rpad("GRGR_ID",255," ").alias("GRGR_ID"),
        rpad("SBSB_ID",255," ").alias("SBSB_ID"),
        rpad("MBI",255," ").alias("MBI"),
        rpad("SOURCE1",255," ").alias("SOURCE1"),
        rpad("SOURCE2",255," ").alias("SOURCE2"),
        rpad("IDENTIFIEDBY",255," ").alias("IDENTIFIEDBY"),
        rpad("IDENTIFIEDDATE",255," ").alias("IDENTIFIEDDATE"),
        rpad("RESOLVEDDATE",255," ").alias("RESOLVEDDATE"),
        rpad("STATUS",255," ").alias("STATUS"),
        rpad("DISCREPANCY",255," ").alias("DISCREPANCY"),
        rpad("NOTE",255," ").alias("NOTE"),
        rpad("PRIORITY",255," ").alias("PRIORITY"),
        rpad("AGE",255," ").alias("AGE"),
        rpad("LEGACY",1," ").alias("LEGACY"),
        rpad("LAST_UPDATED_DATE",255," ").alias("LAST_UPDATED_DATE")
    ),
    f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_BLMM_APPEND.{runid}.dat",
    delimiter="|",
    mode="overwrite",
    is_parquet=False,
    header=True,
    quote="\"",
    nullValue=None
)