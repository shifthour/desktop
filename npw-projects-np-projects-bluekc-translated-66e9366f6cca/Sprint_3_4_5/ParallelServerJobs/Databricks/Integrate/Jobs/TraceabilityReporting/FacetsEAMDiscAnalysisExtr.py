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
# MAGIC ====================================================================================================================================================
# MAGIC Developer                          Date                           Project/Altiris #                              Change Description                                            Development Project           Reviewer    
# MAGIC ====================================================================================================================================================
# MAGIC ReddySanam               2021-01-13                  US329820                                         Original Programming                                                 IntegrateDev2          Kalyan Neelam    2021-01-14
# MAGIC 
# MAGIC ReddySanam              2021-01-21                   US329820                                      Map Source1 and Source2 fields
# MAGIC                                                                                                                                from EAM and FACETS respectively                              IntegrateDev2         Kalyan Neelam    2021-01-21
# MAGIC 
# MAGIC ReddySanam              2021-01-21                   US329820                                      Change MBI column length to VARCHAR(12)                IntegrateDev2         Jeyaprasanna     2021-01-28
# MAGIC Reddy Sanam             2021-02-10                   US329820                                     Tuned the Duplicates processing logic                            IntegrateDev2         Jeyaprasanna     2021-02-10
# MAGIC                                                                                                                                Added logic to handle Missing Eligibility                          
# MAGIC                                                                                                                                in EAM/FACETS
# MAGIC Reddy Sanam             2021-02-23                   US329820                                     Explicit config names added to select criteria                   IntegrateDev2          Jeyaprasanna     2021-02-23
# MAGIC                                                                                                                                in DISC_CONFIG and EAM_DISC_ANALYSIS
# MAGIC                                                                                                                                stages
# MAGIC JohnAbraham              2021-08-11                   US391328                    Added EAMRPT Env variables and updated appropriate tables        IntegrateDev1
# MAGIC                                                                                                                with these parameters
# MAGIC      
# MAGIC Arpitha V                     2024-06-14                   US 612321                    Removed Effective Date value from where condition in                    IntegrateDev2        Jeyaprasanna    2024-06-20
# MAGIC                                                                                                                   stage DISC_CONFIG and EAM_DISC_ANALYSIS

# MAGIC This Part Identifies if an MBI has multuple entries in EAM or FACETS under any Mismatch Category.
# MAGIC This part will remove MBI with multiple entries and retain one entry with a single Mismatch error
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
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, DateType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../Utility_Integrate
# COMMAND ----------


# Parameters
EAMRPTOwner = get_widget_value('EAMRPTOwner','')
eamrpt_secret_name = get_widget_value('eamrpt_secret_name','')
RUNID = get_widget_value('RUNID','')
CurrDt = get_widget_value('CurrDt','')

# Read from EAM_DISC_ANALYSIS (ODBCConnectorPX)
jdbc_url_eam_disc_analysis, jdbc_props_eam_disc_analysis = get_db_config(eamrpt_secret_name)
query_eam_disc_analysis = f"""
SELECT  DISC.ID AS ID_SK,
        DISC.MBI,
        DISC.IDENTIFIEDBY,
        DISC.IDENTIFIEDDATE,
        DISC.DISCREPANCY as DISCREPANCY_ID
FROM {EAMRPTOwner}.DISCREPANCY_ANALYSIS DISC
INNER JOIN {EAMRPTOwner}.DISCREPANCY_CONFIG CONFIG
  ON DISC.DISCREPANCY = CONFIG.ID
WHERE STATUS = 'OPEN'
  and DISC.IDENTIFIEDBY = 'CURR'
  and CONFIG.DISCREPANCY_NAME in
('MemberID',
'NO_ELIG_EAM',
'NO_ELIG_FACETS',
'PlanID',
'PBPID',
'PremWithholdOption',
'PrimaryRXID',
'SCCCode',
'GroupID',
'MedicalProductID',
'PenaltyAmount',
'SubsidyAmount',
'HIC',
'Subsidy Level',
'DentalProductID',
'LIS Effective Date',
'LATE Effective Date',
'Member Duplicates in FACETS',
'Member Duplicates in EAM')
"""
df_eam_disc_analysis = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_eam_disc_analysis)
    .options(**jdbc_props_eam_disc_analysis)
    .option("query", query_eam_disc_analysis)
    .load()
)

# Read from DISC_CONFIG (ODBCConnectorPX)
jdbc_url_disc_config, jdbc_props_disc_config = get_db_config(eamrpt_secret_name)
query_disc_config = f"""
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
  and IDENTIFIEDBY = 'CURR'
  AND DISCREPANCY_NAME IN
('MemberID',
'NO_ELIG_EAM',
'NO_ELIG_FACETS',
'PlanID',
'PBPID',
'PremWithholdOption',
'PrimaryRXID',
'SCCCode',
'GroupID',
'MedicalProductID',
'PenaltyAmount',
'SubsidyAmount',
'HIC',
'Subsidy Level',
'DentalProductID',
'LIS Effective Date',
'LATE Effective Date',
'Member Duplicates in FACETS',
'Member Duplicates in EAM')
"""
df_disc_config = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_disc_config)
    .options(**jdbc_props_disc_config)
    .option("query", query_disc_config)
    .load()
)

# Read from EAM_Details (ODBCConnectorPX)
jdbc_url_eam_details, jdbc_props_eam_details = get_db_config(eamrpt_secret_name)
query_eam_details = f"""
select  Details_SK,
        ID,
        CASE WHEN Field = 'HIC' AND LTRIM(RTRIM(ISNULL(EAM_MBI,''))) = '' THEN 'NO_ELIG_EAM'
             WHEN Field = 'HIC' AND LTRIM(RTRIM(ISNULL(FACETS_MBI,''))) = '' THEN 'NO_ELIG_FACETS'
             ELSE Field
        END as Field,
        EAM_MemberID,
        FACETS_MemberID,
        EAM,
        FACETS,
        EAM_MBI,
        FACETS_MBI,
        EAM_GROUP_ID,
        FACETS_GROUP_ID
from {EAMRPTOwner}.Details
"""
df_eam_details = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_eam_details)
    .options(**jdbc_props_eam_details)
    .option("query", query_eam_details)
    .load()
)

# SplitData (CTransformerStage)
tmp_eam_mbi = F.trim(F.coalesce(F.col("EAM_MBI"), F.lit("")))
tmp_facets_mbi = F.trim(F.coalesce(F.col("FACETS_MBI"), F.lit("")))
tmp_field = F.trim(F.coalesce(F.col("Field"), F.lit("")))

cond_main = (
    (
        (
            (tmp_eam_mbi == "") | (tmp_facets_mbi == "")
        )
        & (
            (tmp_field == "NO_ELIG_EAM") | (tmp_field == "NO_ELIG_FACETS")
        )
    )
    | ~(
        (tmp_eam_mbi == "") & (tmp_facets_mbi == "")
    )
)

df_splitdata_main = (
    df_eam_details
    .filter(cond_main)
    .select(
        F.col("Details_SK").alias("Details_SK"),
        F.col("ID").alias("ID"),
        F.when(tmp_eam_mbi == "", tmp_facets_mbi).otherwise(tmp_eam_mbi).alias("MBI"),
        F.col("Field").alias("Field"),
        F.col("EAM_MemberID").alias("EAM_MemberID"),
        F.col("FACETS_MemberID").alias("FACETS_MemberID"),
        F.col("EAM").alias("EAM"),
        F.col("FACETS").alias("FACETS"),
        F.col("EAM_MBI").alias("EAM_MBI"),
        F.col("FACETS_MBI").alias("FACETS_MBI"),
        F.col("EAM_GROUP_ID").alias("EAM_GROUP_ID"),
        F.col("FACETS_GROUP_ID").alias("FACETS_GROUP_ID")
    )
)

df_splitdata_aggr_eam = (
    df_eam_details
    .filter((tmp_eam_mbi != "") & (tmp_facets_mbi != ""))
    .select(
        F.lit("EAM").alias("source"),
        F.col("ID").alias("ID"),
        F.col("Field").alias("Field"),
        F.col("EAM_MBI").alias("MBI"),
        F.col("EAM_MemberID").alias("MemberID")
    )
)

df_splitdata_aggr_facets = (
    df_eam_details
    .filter((tmp_eam_mbi != "") & (tmp_facets_mbi != ""))
    .select(
        F.lit("FACETS").alias("source"),
        F.col("ID").alias("ID"),
        F.col("Field").alias("Field"),
        F.col("FACETS_MBI").alias("MBI"),
        F.col("FACETS_MemberID").alias("MemberID")
    )
)

# Fnl (PxFunnel) for Aggr_EAM + Aggr_Facets -> single dataframe for next stage
df_fnl_from_splitdata = df_splitdata_aggr_eam.unionByName(df_splitdata_aggr_facets)

# RmDup_EAM (PxRemDup)
df_rmdup_eam = dedup_sort(
    df_fnl_from_splitdata,
    partition_cols=["source", "ID", "Field", "MBI", "MemberID"],
    sort_cols=[]
)

# Cnt_EAM (PxAggregator)
df_cnt_eam = (
    df_rmdup_eam
    .groupBy("source", "ID", "Field", "MBI")
    .agg(F.count("*").alias("MemberCount"))
    .select(
        F.col("source").alias("source"),
        F.col("ID").alias("ID"),
        F.col("Field").alias("Field"),
        F.col("MBI").alias("MBI"),
        F.col("MemberCount").alias("MemberCount")
    )
)

# CheckCnt (CTransformerStage)
df_checkcnt = (
    df_cnt_eam
    .filter(F.col("MemberCount") > 1)
    .select(
        F.col("source").alias("source"),
        F.col("MBI").alias("MBI"),
        F.col("MemberCount").alias("MemberCount")
    )
)

# RmDupMBI (PxRemDup)
df_rmdup_mbi = dedup_sort(
    df_checkcnt,
    partition_cols=["MBI"],
    sort_cols=[]
).select(
    F.col("source").alias("source"),
    F.col("MBI").alias("MBI"),
    F.col("MemberCount").alias("MemberCount")
)

# Lkp_Dupes (PxLookup) - left join df_splitdata_main with df_rmdup_mbi
df_lkp_dupes_joined = (
    df_splitdata_main.alias("Main")
    .join(df_rmdup_mbi.alias("Ref_DupMBI"), F.col("Main.MBI") == F.col("Ref_DupMBI.MBI"), "left")
)

df_mbi_dupes = df_lkp_dupes_joined.filter(F.col("Ref_DupMBI.MBI").isNotNull()).select(
    F.col("Main.Details_SK").alias("Details_SK"),
    F.col("Main.ID").alias("ID"),
    F.col("Main.MBI").alias("MBI"),
    F.col("Main.Field").alias("Field"),
    F.col("Main.EAM_MemberID").alias("EAM_MemberID"),
    F.col("Main.FACETS_MemberID").alias("FACETS_MemberID"),
    F.col("Main.EAM").alias("EAM"),
    F.col("Main.FACETS").alias("FACETS"),
    F.col("Main.EAM_MBI").alias("EAM_MBI"),
    F.col("Main.FACETS_MBI").alias("FACETS_MBI"),
    F.col("Main.EAM_GROUP_ID").alias("EAM_GROUP_ID"),
    F.col("Main.FACETS_GROUP_ID").alias("FACETS_GROUP_ID"),
    F.col("Ref_DupMBI.source").alias("source"),
    F.col("Ref_DupMBI.MemberCount").alias("MemberCount")
)

df_no_mbi_dupes = df_lkp_dupes_joined.filter(F.col("Ref_DupMBI.MBI").isNull()).select(
    F.col("Main.Details_SK").alias("Details_SK"),
    F.col("Main.ID").alias("ID"),
    F.col("Main.MBI").alias("MBI"),
    F.col("Main.Field").alias("Field"),
    F.col("Main.EAM_MemberID").alias("EAM_MemberID"),
    F.col("Main.FACETS_MemberID").alias("FACETS_MemberID"),
    F.col("Main.EAM").alias("EAM"),
    F.col("Main.FACETS").alias("FACETS"),
    F.col("Main.EAM_MBI").alias("EAM_MBI"),
    F.col("Main.FACETS_MBI").alias("FACETS_MBI"),
    F.col("Main.EAM_GROUP_ID").alias("EAM_GROUP_ID"),
    F.col("Main.FACETS_GROUP_ID").alias("FACETS_GROUP_ID"),
)

# Assign_Mismatch (CTransformerStage)
df_assign_mismatch = df_mbi_dupes.select(
    F.col("Details_SK").alias("Details_SK"),
    F.lit(100).alias("ID"),
    F.col("MBI").alias("MBI"),
    F.when(F.col("source") == "EAM", F.lit("Member Duplicates in EAM"))
     .otherwise(F.lit("Member Duplicates in FACETS")).alias("Field"),
    F.col("EAM_MemberID").alias("EAM_MemberID"),
    F.col("FACETS_MemberID").alias("FACETS_MemberID"),
    F.col("EAM").alias("EAM"),
    F.col("FACETS").alias("FACETS"),
    F.col("EAM_MBI").alias("EAM_MBI"),
    F.col("FACETS_MBI").alias("FACETS_MBI"),
    F.col("EAM_GROUP_ID").alias("EAM_GROUP_ID"),
    F.col("FACETS_GROUP_ID").alias("FACETS_GROUP_ID")
)

# RmDup_MBI (PxRemDup)
df_rmdup_mbi_2 = dedup_sort(
    df_assign_mismatch,
    partition_cols=["Field", "EAM_MBI"],
    sort_cols=[]
).select(
    F.col("Details_SK").alias("Details_SK"),
    F.col("ID").alias("ID"),
    F.col("MBI").alias("MBI"),
    F.col("Field").alias("Field"),
    F.col("EAM_MemberID").alias("EAM_MemberID"),
    F.col("FACETS_MemberID").alias("FACETS_MemberID"),
    F.col("EAM").alias("EAM"),
    F.col("FACETS").alias("FACETS"),
    F.col("EAM_MBI").alias("EAM_MBI"),
    F.col("FACETS_MBI").alias("FACETS_MBI"),
    F.col("EAM_GROUP_ID").alias("EAM_GROUP_ID"),
    F.col("FACETS_GROUP_ID").alias("FACETS_GROUP_ID")
)

# Fnl (PxFunnel) for df_rmdup_mbi_2 + df_no_mbi_dupes
df_fnl_funnel = (
    df_rmdup_mbi_2
    .unionByName(
        df_no_mbi_dupes.select(
            "Details_SK",
            "ID",
            "MBI",
            "Field",
            "EAM_MemberID",
            "FACETS_MemberID",
            "EAM",
            "FACETS",
            "EAM_MBI",
            "FACETS_MBI",
            "EAM_GROUP_ID",
            "FACETS_GROUP_ID"
        )
    )
)

# Lkp_Config (PxLookup) - primary link df_fnl_funnel, lookup link df_disc_config
# There's no join condition specified in "JoinConditions" for Ref_Disc_Config, so it's effectively cross or an always-true join. But the stage type is "inner" in the JSON.
# In DataStage if no conditions are specified but "inner" is selected, that normally returns no rows unless it's a cross. This is ambiguous. Typically, no matching columns means no match. 
# However, to preserve all columns, we can do an inner with no condition => it results in an empty set. This may or may not be what's intended. DataStage can treat an empty join condition as a cross join. 
# The job references columns from Ref_Disc_Config. That implies it must be a cross join in DataStage. We'll implement an inner cross join in Spark carefully.
df_lkp_config_joined = (
    df_fnl_funnel.alias("Att_config")
    .join(df_disc_config.alias("Ref_Disc_Config"), how="inner")
)

df_details = df_lkp_config_joined.select(
    F.col("Att_config.Details_SK").alias("Details_SK"),
    F.col("Att_config.ID").alias("ID"),
    F.col("Att_config.MBI").alias("MBI"),
    F.col("Att_config.Field").alias("Field"),
    F.col("Att_config.EAM_MemberID").alias("EAM_MemberID"),
    F.col("Att_config.FACETS_MemberID").alias("FACETS_MemberID"),
    F.col("Att_config.EAM").alias("EAM"),
    F.col("Att_config.FACETS").alias("FACETS"),
    F.col("Att_config.EAM_MBI").alias("EAM_MBI"),
    F.col("Att_config.FACETS_MBI").alias("FACETS_MBI"),
    F.col("Att_config.EAM_GROUP_ID").alias("EAM_GROUP_ID"),
    F.col("Att_config.FACETS_GROUP_ID").alias("FACETS_GROUP_ID"),
    F.col("Ref_Disc_Config.DISCREPANCY_ID").alias("DISCREPANCY_ID"),
    F.col("Ref_Disc_Config.DISCREPANCY_DESC").alias("DISCREPANCY_DESC"),
    F.col("Ref_Disc_Config.IDENTIFIEDBY").alias("IDENTIFIEDBY"),
    F.col("Ref_Disc_Config.PRIORITY").alias("PRIORITY"),
    F.col("Ref_Disc_Config.SOURCE1").alias("SOURCE1"),
    F.col("Ref_Disc_Config.SOURCE2").alias("SOURCE2")
)

# FullOuter (PxJoin) - join on [MBI, DISCREPANCY_ID, IDENTIFIEDBY] with df_eam_disc_analysis
df_fullouter = (
    df_details.alias("Details")
    .join(
        df_eam_disc_analysis.alias("DISC_Analysis"),
        [
            F.col("Details.MBI") == F.col("DISC_Analysis.MBI"),
            F.col("Details.DISCREPANCY_ID") == F.col("DISC_Analysis.DISCREPANCY_ID"),
            F.col("Details.IDENTIFIEDBY") == F.col("DISC_Analysis.IDENTIFIEDBY"),
        ],
        how="full"
    )
    .select(
        F.col("Details.Details_SK").alias("Details_SK"),
        F.col("Details.ID").alias("ID"),
        F.col("Details.MBI").alias("leftRec_MBI"),
        F.col("Details.Field").alias("Field"),
        F.col("Details.EAM_MemberID").alias("EAM_MemberID"),
        F.col("Details.FACETS_MemberID").alias("FACETS_MemberID"),
        F.col("Details.EAM").alias("EAM"),
        F.col("Details.FACETS").alias("FACETS"),
        F.col("Details.FACETS_MBI").alias("FACETS_MBI"),
        F.col("Details.EAM_GROUP_ID").alias("EAM_GROUP_ID"),
        F.col("Details.FACETS_GROUP_ID").alias("FACETS_GROUP_ID"),
        F.col("Details.DISCREPANCY_ID").alias("leftRec_DISCREPANCY_ID"),
        F.col("Details.DISCREPANCY_DESC").alias("DISCREPANCY_DESC"),
        F.col("Details.IDENTIFIEDBY").alias("leftRec_IDENTIFIEDBY"),
        F.col("Details.PRIORITY").alias("PRIORITY"),
        F.col("Details.SOURCE1").alias("SOURCE1"),
        F.col("Details.SOURCE2").alias("SOURCE2"),
        F.col("DISC_Analysis.ID_SK").alias("ID_SK"),
        F.col("DISC_Analysis.MBI").alias("rightRec_MBI"),
        F.col("DISC_Analysis.IDENTIFIEDBY").alias("rightRec_IDENTIFIEDBY"),
        F.col("DISC_Analysis.IDENTIFIEDDATE").alias("IDENTIFIEDDATE"),
        F.col("DISC_Analysis.DISCREPANCY_ID").alias("rightRec_DISCREPANCY_ID")
    )
)

# Xmr (CTransformerStage): produces three outputs via filters
df_xmr_append = df_fullouter.filter(
    (F.col("rightRec_MBI") == "") &
    (F.trim(F.coalesce(F.col("leftRec_MBI"), F.lit(""))) != "")
).select(
    F.when(
        F.trim(F.coalesce(F.col("EAM_GROUP_ID"), F.lit(""))) == "",
        F.col("FACETS_GROUP_ID")
    ).otherwise(F.col("EAM_GROUP_ID")).alias("GRGR_ID"),
    F.when(
        F.trim(F.coalesce(F.col("EAM_MemberID"), F.lit(""))) == "",
        F.col("FACETS_MemberID")
    ).otherwise(F.col("EAM_MemberID")).alias("SBSB_ID"),
    F.col("leftRec_MBI").alias("MBI"),
    F.trim(F.coalesce(F.col("EAM"), F.lit(""))).alias("SOURCE1"),
    F.trim(F.coalesce(F.col("FACETS"), F.lit(""))).alias("SOURCE2"),
    F.col("leftRec_IDENTIFIEDBY").alias("IDENTIFIEDBY"),
    F.expr("StringToDate(CurrDt, \"%yyyy-%mm-%dd\")").alias("IDENTIFIEDDATE"),
    F.lit(None).alias("RESOLVEDDATE"),
    F.lit("OPEN").alias("STATUS"),
    F.col("leftRec_DISCREPANCY_ID").alias("DISCREPANCY"),
    F.col("DISCREPANCY_DESC").alias("NOTE"),
    F.col("PRIORITY").alias("PRIORITY"),
    F.lit(0).alias("AGE"),
    F.when(
        (F.col("EAM_MemberID") == "") &
        (F.col("FACETS_MemberID") != "") &
        (F.substring(F.col("FACETS_MemberID"), 1, 3) == "000"),
        F.lit("Y")
    ).when(
        (F.col("EAM_MemberID") == "") &
        (F.col("FACETS_MemberID") != "") &
        (F.substring(F.col("FACETS_MemberID"), 1, 3) != "000"),
        F.lit("N")
    ).when(
        (F.col("EAM_MemberID") != "") &
        (F.substring(F.col("EAM_MemberID"), 1, 3) == "000"),
        F.lit("Y")
    ).otherwise(F.lit("N")).alias("LEGACY"),
    F.expr("StringToDate(CurrDt, \"%yyyy-%mm-%dd\")").alias("LAST_UPDATED_DATE")
)

df_xmr_resolved = df_fullouter.filter(
    (F.trim(F.coalesce(F.col("leftRec_MBI"), F.lit(""))) == "") &
    (F.col("rightRec_MBI") != "")
).select(
    F.col("ID_SK").alias("ID_SK")
)

df_xmr_stillopen = df_fullouter.filter(
    (F.trim(F.coalesce(F.col("leftRec_MBI"), F.lit(""))) != "") &
    (F.col("rightRec_MBI") != "")
).select(
    F.col("ID_SK").alias("ID_SK"),
    F.expr("DaysSinceFromDate2(StringToDate(CurrDt, \"%yyyy-%mm-%dd\"), IDENTIFIEDDATE)").alias("AGE"),
    F.expr("StringToDate(CurrDt, \"%yyyy-%mm-%dd\")").alias("LAST_UPDATED_DATE")
)

# Resolved (PxSequentialFile)
df_resolved = df_xmr_resolved.select("ID_SK")
write_files(
    df_resolved,
    f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_RESOLVED.{RUNID}.dat",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)

# Aged (PxSequentialFile)
df_aged = df_xmr_stillopen.select("ID_SK", "AGE", "LAST_UPDATED_DATE")
write_files(
    df_aged,
    f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_AGED.{RUNID}.dat",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)

# RmDupDisc (PxRemDup) on df_xmr_append
df_rmdupdisc = dedup_sort(
    df_xmr_append,
    partition_cols=["MBI", "IDENTIFIEDBY", "IDENTIFIEDDATE", "DISCREPANCY"],
    sort_cols=[]
).select(
    F.col("GRGR_ID").alias("GRGR_ID"),
    F.col("SBSB_ID").alias("SBSB_ID"),
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
    F.col("LEGACY").alias("LEGACY"),
    F.col("LAST_UPDATED_DATE").alias("LAST_UPDATED_DATE")
)

# New (PxSequentialFile)
df_new = df_rmdupdisc.select(
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
    F.rpad(F.col("LEGACY"), 1, " ").alias("LEGACY"),
    "LAST_UPDATED_DATE"
)
write_files(
    df_new,
    f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_APPEND.{RUNID}.dat",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)