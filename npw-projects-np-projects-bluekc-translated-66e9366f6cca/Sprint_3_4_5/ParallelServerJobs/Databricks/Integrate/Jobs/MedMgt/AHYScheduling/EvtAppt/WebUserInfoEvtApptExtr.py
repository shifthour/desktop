# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2010 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY :  IdsEvtExtrSeq
# MAGIC  
# MAGIC PROCESSING:   Processes the EVENT APPOINTMENT data from WEB User Info and writes out a key file to load into the IDS EVT_APPT table.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Kalyan Neelam        2010-11-02        4529                      Initial Programming                                                                            IntegrateNewDevl       SAndrew                           12/07/2010
# MAGIC Kalyan Neelam        2011-01-26        4529                      Added Balancing Snapshot                                                              IntegrateNewDevl       Steph Goddard         02/11/2011

# MAGIC IDS EVENT APPOINTMENT Extract
# MAGIC Looking to see if any appointments were deleted in the past 90 days. The field EVT_APPT_DELD_IN on the IDS table is updated with 'Y' for the deleted appointements
# MAGIC Look Up to the EVT APPT RSN table on RSN ID to get the EVT_TYP_CD, EVT_APPT_RSN_NM and EFF_DT for Foreign Key look up for EVT_APPT_RSN_SK
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, TimestampType, DecimalType, StructType, StructField
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

# Parameters
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
WebUserInformationOwner = get_widget_value("WebUserInformationOwner","")
webuserinformation_secret_name = get_widget_value("webuserinformation_secret_name","")
CurrRunCycle = get_widget_value("CurrRunCycle","100")
RunID = get_widget_value("RunID","100")
SrcSysCd = get_widget_value("SrcSysCd","WEBUSERINFO")
CurrDate = get_widget_value("CurrDate","2010-11-04")
CurrDateMinus90 = get_widget_value("CurrDateMinus90","2010-08-06")
LastRunDateTime = get_widget_value("LastRunDateTime","2010-09-01")
CurrDateTimestamp = get_widget_value("CurrDateTimestamp","")
SrcSysCdSk = get_widget_value("SrcSysCdSk","")

# Database configs
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
jdbc_url_webuserinformation, jdbc_props_webuserinformation = get_db_config(webuserinformation_secret_name)

# ---------------------------------------------------------------------------------------
# Read from GRP_SCHD_APPT (CODBCStage) - multiple output pins

# 1) Extract
extract_query_Extract = f"""
SELECT
  GRP_SCHD_APPT.EVT_TYP_CD,
  GRP_SCHD_APPT.GRP_ID,
  GRP_SCHD_APPT.EVT_DT,
  GRP_SCHD_APPT.SEQ_NO,
  GRP_SCHD_APPT.APPT_STRT_TM_TX,
  GRP_SCHD_APPT.APPT_END_TM_TX,
  GRP_SCHD_APPT.STAFF_ID,
  GRP_SCHD_APPT.MBR_UNIQ_KEY,
  GRP_SCHD_APPT.MBR_CNTCT_EMAIL_ADDR,
  GRP_SCHD_APPT.MBR_CNTCT_PHN_NO,
  GRP_SCHD_APPT.LAST_UPDT_USER_ID,
  GRP_SCHD_APPT.LAST_UPDT_DTM,
  GRP_SCHD_APPT.EVT_APPT_RSN_ID
FROM {WebUserInformationOwner}.GRP_SCHD_APPT GRP_SCHD_APPT
WHERE GRP_SCHD_APPT.LAST_UPDT_DTM > '{LastRunDateTime}'
"""
df_Extract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_webuserinformation)
    .options(**jdbc_props_webuserinformation)
    .option("query", extract_query_Extract)
    .load()
)

# 2) GrpSchd
extract_query_GrpSchd = f"""
SELECT
  GRP_SCHD.GRP_ID,
  GRP_SCHD.EVT_TYP_CD,
  GRP_SCHD.EVT_DT,
  GRP_SCHD.SEQ_NO,
  GRP_SCHD.LOC_ID
FROM {WebUserInformationOwner}.GRP_SCHD GRP_SCHD
"""
df_GrpSchd = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_webuserinformation)
    .options(**jdbc_props_webuserinformation)
    .option("query", extract_query_GrpSchd)
    .load()
)

# 3) DeldIn_Extract
extract_query_DeldIn_Extract = f"""
SELECT
  GRP_SCHD_APPT.EVT_TYP_CD,
  GRP_SCHD_APPT.GRP_ID,
  GRP_SCHD_APPT.EVT_DT,
  GRP_SCHD_APPT.SEQ_NO,
  GRP_SCHD_APPT.APPT_STRT_TM_TX
FROM {WebUserInformationOwner}.GRP_SCHD_APPT GRP_SCHD_APPT
"""
df_DeldIn_Extract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_webuserinformation)
    .options(**jdbc_props_webuserinformation)
    .option("query", extract_query_DeldIn_Extract)
    .load()
)

# 4) EvtApptRsn
extract_query_EvtApptRsn = f"""
SELECT
  EVT_APPT_RSN.EVT_APPT_RSN_ID,
  EVT_APPT_RSN.EVT_TYP_CD,
  EVT_APPT_RSN.EVT_APPT_RSN_NM,
  EVT_APPT_RSN.EFF_DT
FROM {WebUserInformationOwner}.EVT_APPT_RSN EVT_APPT_RSN
"""
df_EvtApptRsn = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_webuserinformation)
    .options(**jdbc_props_webuserinformation)
    .option("query", extract_query_EvtApptRsn)
    .load()
)

# ---------------------------------------------------------------------------------------
# Read from IDS_EVT_APPT (DB2Connector)
# "SELECT ... FROM #$IDSOwner#.EVT_APPT ... WHERE EVT_APPT.LAST_UPDT_DTM >= '#CurrDateMinus90#'"
extract_query_Ids_Extract = f"""
SELECT
  EVT_APPT.GRP_ID as GRP_ID,
  EVT_APPT.EVT_DT_SK as EVT_DT_SK,
  EVT_APPT.EVT_TYP_ID as EVT_TYP_ID,
  EVT_APPT.SEQ_NO as SEQ_NO,
  EVT_APPT.EVT_APPT_STRT_TM_TX as EVT_APPT_STRT_TM_TX,
  EVT_APPT.SRC_SYS_CD_SK as SRC_SYS_CD_SK,
  EVT_APPT.CRT_RUN_CYC_EXCTN_SK as CRT_RUN_CYC_EXCTN_SK,
  EVT_APPT.LAST_UPDT_RUN_CYC_EXCTN_SK as LAST_UPDT_RUN_CYC_EXCTN_SK,
  EVT_APPT.EVT_APPT_RSN_SK as EVT_APPT_RSN_SK,
  EVT_APPT.EVT_LOC_SK as EVT_LOC_SK,
  EVT_APPT.EVT_STAFF_SK as EVT_STAFF_SK,
  EVT_APPT.EVT_TYP_SK as EVT_TYP_SK,
  EVT_APPT.GRP_SK as GRP_SK,
  EVT_APPT.MBR_SK as MBR_SK,
  EVT_APPT.EVT_APPT_DELD_IN as EVT_APPT_DELD_IN,
  EVT_APPT.EVT_APPT_END_TM_TX as EVT_APPT_END_TM_TX,
  EVT_APPT.LAST_UPDT_DTM as LAST_UPDT_DTM,
  EVT_APPT.LAST_UPDT_USER_ID as LAST_UPDT_USER_ID
FROM {IDSOwner}.EVT_APPT EVT_APPT
WHERE EVT_APPT.LAST_UPDT_DTM >= '{CurrDateMinus90}'
"""
df_Ids_Extract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_Ids_Extract)
    .load()
)

# ---------------------------------------------------------------------------------------
# hf_evt_appt_lkup -> scenario B: read from dummy table in IDS
df_hf_evt_appt_lkup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", """
SELECT
  GRP_ID,
  EVT_DT_SK,
  EVT_TYP_ID,
  SEQ_NO,
  EVT_APPT_STRT_TM_TX,
  SRC_SYS_CD,
  CRT_RUN_CYC_EXCTN_SK,
  EVT_APPT_SK
FROM IDS.dummy_hf_evt_appt
""")
    .load()
)

# ---------------------------------------------------------------------------------------
# StageName=IDS: We have 4 output pins (Staff, Loc, Mbr, ApptRsn)
# 1) Staff
query_Staff = f"""
SELECT
  EVT_STAFF.EVT_STAFF_SK as EVT_STAFF_SK,
  EVT_STAFF.EVT_STAFF_ID as EVT_STAFF_ID
FROM {IDSOwner}.EVT_STAFF EVT_STAFF
"""
df_Staff = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_Staff)
    .load()
)

# 2) Loc
query_Loc = f"""
SELECT
  EVT_LOC.EVT_LOC_SK as EVT_LOC_SK,
  EVT_LOC.EVT_LOC_ID as EVT_LOC_ID
FROM {IDSOwner}.EVT_LOC EVT_LOC
"""
df_Loc = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_Loc)
    .load()
)

# 3) Mbr
query_Mbr = f"""
SELECT
  MBR.MBR_SK as MBR_SK,
  MBR.MBR_UNIQ_KEY as MBR_UNIQ_KEY
FROM {IDSOwner}.MBR MBR
"""
df_Mbr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_Mbr)
    .load()
)

# 4) ApptRsn
query_ApptRsn = f"""
SELECT
  EVT_APPT_RSN.EVT_APPT_RSN_SK,
  EVT_APPT_RSN.EVT_APPT_RSN_ID,
  EVT_APPT_RSN.EVT_TYP_ID,
  EVT_APPT_RSN.EVT_APPT_RSN_NM,
  EVT_APPT_RSN.EFF_DT_SK
FROM {IDSOwner}.EVT_APPT_RSN EVT_APPT_RSN,
     {IDSOwner}.CD_MPPNG CD
WHERE
  EVT_APPT_RSN.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
  AND CD.TRGT_CD = 'WEBUSERINFO'
"""
df_ApptRsn2 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_ApptRsn)
    .load()
)

# ---------------------------------------------------------------------------------------
# Scenario A for these hashed files:
#  hf_evtappt_grpschdappt_delin
#  hf_evtappt_staffid_loc_mbr
#  hf_evtappt_idsdata_land
#  hf_evtappt_evtapptrsn
#  hf_evtappt_grpschd
#  hf_evtappt_webdata_land
# Each of these is: Stage1 -> CHashedFileStage -> Stage2, with no re-writing. We remove the hashed file stage and deduplicate on the PK columns.

# 1) hf_evtappt_grpschdappt_delin: from df_DeldIn_Extract -> dedup -> used as "web_lkup" in Transformer_48
df_web_lkup = dedup_sort(
    df_DeldIn_Extract,
    partition_cols=["EVT_TYP_CD","GRP_ID","EVT_DT","SEQ_NO","APPT_STRT_TM_TX"],
    sort_cols=[]
)

# 2) hf_evtappt_staffid_loc_mbr:
#    It's actually reading from Staff, Loc, Mbr, ApptRsn2 -> and producing 4 outputs for lookup links
#    We remove that hashed file stage. We'll deduplicate each individually:
df_Staff_lkup = dedup_sort(
    df_Staff,
    partition_cols=["EVT_STAFF_SK"],
    sort_cols=[]
)
df_Loc_lkup = dedup_sort(
    df_Loc,
    partition_cols=["EVT_LOC_SK"],
    sort_cols=[]
)
df_Mbr_lkup = dedup_sort(
    df_Mbr,
    partition_cols=["MBR_SK"],
    sort_cols=[]
)
df_ApptRsn_lkup = dedup_sort(
    df_ApptRsn2,
    partition_cols=["EVT_APPT_RSN_SK"],
    sort_cols=[]
)

# 3) hf_evtappt_idsdata_land: from Transformer_48 -> dedup -> eventually goes to Link_Collector
#    We will do this dedup step after we create the corresponding DataFrame from Transformer_48 that meets the "Ids_Land" link.

# 4) hf_evtappt_evtapptrsn: from TrimEvtApptRsn -> dedup -> used as "AppRsnId_lkup" in BusinessRules
#    We'll do that dedup after we produce that in TrimEvtApptRsn.

# 5) hf_evtappt_grpschd: from TrimGrpSchd -> dedup -> used as "LocId_lkup" in BusinessRules

# 6) hf_evtappt_webdata_land: from BusinessRules -> dedup -> goes to Link_Collector

# ---------------------------------------------------------------------------------------
# TrimEvtApptRsn => input: df_EvtApptRsn => output: "ApptRsn"
# Columns:
#   EVT_APPT_RSN_ID = Trim(EvtApptRsn.EVT_APPT_RSN_ID)
#   EVT_TYP_CD = Trim(EvtApptRsn.EVT_TYP_CD)
#   EVT_APPT_RSN_NM = UpCase(Trim(EvtApptRsn.EVT_APPT_RSN_NM))
#   EFF_DT = If (IsNull(EvtApptRsn.EFF_DT) or Len(Trim(EvtApptRsn.EFF_DT))=0)
#            then '1753-01-01'
#            else FORMAT.DATE(Trim(EvtApptRsn.EFF_DT), "SYBASE", "TIMESTAMP", "CCYY-MM-DD")
df_TrimEvtApptRsn = df_EvtApptRsn.select(
    F.trim(F.col("EVT_APPT_RSN_ID")).alias("EVT_APPT_RSN_ID"),
    F.trim(F.col("EVT_TYP_CD")).alias("EVT_TYP_CD"),
    F.upper(F.trim(F.col("EVT_APPT_RSN_NM"))).alias("EVT_APPT_RSN_NM"),
    F.when(
        F.col("EFF_DT").isNull() | (F.length(F.trim(F.col("EFF_DT")))==0),
        F.lit("1753-01-01")
    ).otherwise(
        # using a helper "FORMAT.DATE(...)" was a DataStage directive. We treat it as a python operation:
        F.trim(F.col("EFF_DT"))
    ).alias("EFF_DT")
)

# Now scenario A hashed file we remove, so deduplicate -> "AppRsnId_lkup"
df_AppRsnId_lkup_pre = df_TrimEvtApptRsn
df_AppRsnId_lkup = dedup_sort(
    df_AppRsnId_lkup_pre,
    partition_cols=["EVT_APPT_RSN_ID","EVT_TYP_CD","EVT_APPT_RSN_NM","EFF_DT"],
    sort_cols=[]
)

# ---------------------------------------------------------------------------------------
# TrimGrpSchd => input: df_GrpSchd => output: "Schd"
df_TrimGrpSchd = df_GrpSchd.select(
    F.trim(F.col("GRP_ID")).alias("GRP_ID"),
    F.trim(F.col("EVT_TYP_CD")).alias("EVT_TYP_CD"),
    F.trim(F.col("EVT_DT")).alias("EVT_DT"),
    F.trim(F.col("SEQ_NO")).alias("SEQ_NO"),
    F.col("LOC_ID").alias("LOC_ID")
)

# scenario A hashed file => deduplicate -> "LocId_lkup"
df_LocId_lkup_pre = df_TrimGrpSchd.select(
    F.col("GRP_ID"),
    F.col("EVT_TYP_CD"),
    F.col("EVT_DT"),
    F.col("SEQ_NO"),
    F.col("LOC_ID")
)
df_LocId_lkup = dedup_sort(
    df_LocId_lkup_pre,
    partition_cols=["GRP_ID","EVT_TYP_CD","EVT_DT","SEQ_NO","LOC_ID"],
    sort_cols=[]
)

# ---------------------------------------------------------------------------------------
# BusinessRules => input pins:
#   PrimaryLink = df_Extract
#   LocId_lkup => left join on (GRP_ID, EVT_TYP_CD, EVT_DT, SEQ_NO)
#   AppRsnId_lkup => left join on (Extract.EVT_APPT_RSN_ID == AppRsnId_lkup.EVT_APPT_RSN_ID)
# OutputPin => "Web_Land"

df_BusinessRules_pre = df_Extract.alias("Extract") \
    .join(
        df_LocId_lkup.alias("LocId_lkup"),
        (F.col("Extract.GRP_ID") == F.col("LocId_lkup.GRP_ID")) &
        (F.col("Extract.EVT_TYP_CD") == F.col("LocId_lkup.EVT_TYP_CD")) &
        (F.col("Extract.EVT_DT") == F.col("LocId_lkup.EVT_DT")) &
        (F.col("Extract.SEQ_NO") == F.col("LocId_lkup.SEQ_NO")),
        how="left"
    ) \
    .join(
        df_AppRsnId_lkup.alias("AppRsnId_lkup"),
        (F.col("Extract.EVT_APPT_RSN_ID") == F.col("AppRsnId_lkup.EVT_APPT_RSN_ID")),
        how="left"
    )

# Now build "Web_Land" columns:
df_Web_Land_pre = df_BusinessRules_pre.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lpad(F.lit("I"),10,"I")[0:10].alias("INSRT_UPDT_CD"),  # forcibly ensure length 10 only. 
    F.lpad(F.lit("N"),1,"N")[0:1].alias("DISCARD_IN"),
    F.lpad(F.lit("Y"),1,"Y")[0:1].alias("PASS_THRU_IN"),
    F.lit(CurrDate).alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.concat(
        F.trim(F.col("Extract.GRP_ID")), F.lit(";"),
        F.trim(F.col("Extract.EVT_DT")), F.lit(";"),
        F.upper(F.trim(F.col("Extract.EVT_TYP_CD"))), F.lit(";"),
        F.trim(F.col("Extract.SEQ_NO")), F.lit(";"),
        F.trim(F.col("Extract.APPT_STRT_TM_TX")), F.lit(";"),
        F.lit(SrcSysCd)
    ).alias("PRI_KEY_STRING"),
    F.lit(0).alias("EVT_APPT_SK"),
    F.trim(F.col("Extract.GRP_ID")).alias("GRP_ID"),
    # EVT_DT_SK => FORMAT.DATE(Trim(Extract.EVT_DT)) => we store the trimmed string
    F.trim(F.col("Extract.EVT_DT")).alias("EVT_DT_SK"),
    F.upper(F.trim(F.col("Extract.EVT_TYP_CD"))).alias("EVT_TYP_ID"),
    F.trim(F.col("Extract.SEQ_NO")).alias("SEQ_NO"),
    F.trim(F.col("Extract.APPT_STRT_TM_TX")).alias("EVT_APPT_STRT_TM_TX"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.trim(F.col("Extract.STAFF_ID")).alias("STAFF_ID"),
    F.when(
      (F.trim(F.col("Extract.MBR_UNIQ_KEY")).isNull()) | (F.length(F.trim(F.col("Extract.MBR_UNIQ_KEY")))==0),
      F.lit("NA")
    ).otherwise(F.trim(F.col("Extract.MBR_UNIQ_KEY"))).alias("MBR_UNIQ_KEY"),
    F.trim(F.col("Extract.EVT_APPT_RSN_ID")).alias("EVT_APPT_RSN_ID"),
    F.when(
      (F.col("LocId_lkup.LOC_ID").isNull()) | (F.length(F.trim(F.col("LocId_lkup.LOC_ID")))==0),
      F.lit("UNK")
    ).otherwise(F.trim(F.col("LocId_lkup.LOC_ID"))).alias("LOC_ID"),
    F.lpad(F.lit("N"),1,"N")[0:1].alias("EVT_APPT_DELD_IN"),
    F.when(
      (F.col("Extract.APPT_END_TM_TX").isNull()) | (F.length(F.trim(F.col("Extract.APPT_END_TM_TX")))==0),
      F.lit("UNK")
    ).otherwise(F.trim(F.col("Extract.APPT_END_TM_TX"))).alias("EVT_APPT_END_TM_TX"),
    F.when(
      (F.col("Extract.LAST_UPDT_DTM").isNull()) | (F.length(F.trim(F.col("Extract.LAST_UPDT_DTM")))==0),
      F.lit(CurrDateTimestamp)
    ).otherwise(F.col("Extract.LAST_UPDT_DTM")).alias("LAST_UPDT_DTM"),
    F.when(
      (F.length(F.trim(F.col("Extract.LAST_UPDT_USER_ID")))==0) | (F.col("Extract.LAST_UPDT_USER_ID").isNull()),
      F.lit("UNK")
    ).otherwise(F.upper(F.trim(F.col("Extract.LAST_UPDT_USER_ID")))).alias("LAST_UPDT_USER_ID"),
    F.when(
      (F.col("AppRsnId_lkup.EVT_TYP_CD").isNull()) | (F.length(F.trim(F.col("AppRsnId_lkup.EVT_TYP_CD")))==0),
      F.lit("UNK")
    ).otherwise(F.upper(F.col("AppRsnId_lkup.EVT_TYP_CD"))).alias("EVT_TYP_CD"),
    F.when(
      (F.col("AppRsnId_lkup.EVT_APPT_RSN_NM").isNull()) | (F.length(F.trim(F.col("AppRsnId_lkup.EVT_APPT_RSN_NM")))==0),
      F.lit("UNK")
    ).otherwise(F.upper(F.col("AppRsnId_lkup.EVT_APPT_RSN_NM"))).alias("EVT_APPT_RSN_NM"),
    F.when(
      (F.col("AppRsnId_lkup.EFF_DT").isNull()) | (F.length(F.trim(F.col("AppRsnId_lkup.EFF_DT"))) == 0),
      F.lit("1753-01-01")
    ).otherwise(F.trim(F.col("AppRsnId_lkup.EFF_DT"))).alias("EFF_DT")
)

# scenario A => hashed file "hf_evtappt_webdata_land" => deduplicate
df_Web_Data = dedup_sort(
    df_Web_Land_pre,
    partition_cols=[
      "JOB_EXCTN_RCRD_ERR_SK",
      "INSRT_UPDT_CD",
      "DISCARD_IN",
      "PASS_THRU_IN",
      "FIRST_RECYC_DT",
      "ERR_CT",
      "RECYCLE_CT",
      "SRC_SYS_CD",
      "PRI_KEY_STRING",
      "EVT_APPT_SK",
      "GRP_ID",
      "EVT_DT_SK",
      "EVT_TYP_ID",
      "SEQ_NO",
      "EVT_APPT_STRT_TM_TX",
      "CRT_RUN_CYC_EXCTN_SK",
      "LAST_UPDT_RUN_CYC_EXCTN_SK",
      "STAFF_ID",
      "MBR_UNIQ_KEY",
      "EVT_APPT_RSN_ID",
      "LOC_ID",
      "EVT_APPT_DELD_IN",
      "EVT_APPT_END_TM_TX",
      "LAST_UPDT_DTM",
      "LAST_UPDT_USER_ID",
      "EVT_TYP_CD",
      "EVT_APPT_RSN_NM",
      "EFF_DT"
    ],
    sort_cols=[]
)

# ---------------------------------------------------------------------------------------
# Transformer_48 => primary link = df_Ids_Extract => 5 lookup links:
#   web_lkup (df_web_lkup) => left join on (EVT_TYP_ID->EVT_TYP_CD, GRP_ID->GRP_ID, date->date, SEQ_NO->SEQ_NO, start_tm->start_tm, plus "FORMAT.DATE(EVT_DT_SK..)" => that was effectively the same as checking equality of strings or date. We replicate the logic with left join on these columns but the job code checks for web_lkup.EVT_DT. We'll interpret EVT_DT_SK as string => we do not set a separate transformation because the job used a function "FORMAT.DATE(Ids_Extract.EVT_DT_SK...)". We'll assume they're the same string if the DS code tried to do a match. 
#   Staff_lkup => left join (Ids_Extract.EVT_STAFF_SK = Staff_lkup.EVT_STAFF_SK)
#   Loc_lkup => left join (Ids_Extract.EVT_LOC_SK = Loc_lkup.EVT_LOC_SK)
#   Mbr_lkup => left join (Ids_Extract.MBR_SK = Mbr_lkup.MBR_SK)
#   ApptRsn_lkup => left join (Ids_Extract.EVT_APPT_RSN_SK = ApptRsn_lkup.EVT_APPT_RSN_SK)
# Then output pin "Ids_Land" with constraint: IsNull(web_lkup.EVT_TYP_CD) => produce df_Ids_Land. 
#   The columns are enumerated with if-then logic. We'll build that DataFrame.

df_Transformer_48_pre = (
    df_Ids_Extract.alias("Ids_Extract")
    .join(
        df_web_lkup.alias("web_lkup"),
        (F.col("Ids_Extract.EVT_TYP_ID") == F.col("web_lkup.EVT_TYP_CD")) &
        (F.col("Ids_Extract.GRP_ID") == F.col("web_lkup.GRP_ID")) &
        (F.col("Ids_Extract.EVT_DT_SK") == F.col("web_lkup.EVT_DT")) &
        (F.col("Ids_Extract.SEQ_NO") == F.col("web_lkup.SEQ_NO")) &
        (F.col("Ids_Extract.EVT_APPT_STRT_TM_TX") == F.col("web_lkup.APPT_STRT_TM_TX")),
        how="left"
    )
    .join(
        df_Staff_lkup.alias("Staff_lkup"),
        (F.col("Ids_Extract.EVT_STAFF_SK") == F.col("Staff_lkup.EVT_STAFF_SK")),
        how="left"
    )
    .join(
        df_Loc_lkup.alias("Loc_lkup"),
        (F.col("Ids_Extract.EVT_LOC_SK") == F.col("Loc_lkup.EVT_LOC_SK")),
        how="left"
    )
    .join(
        df_Mbr_lkup.alias("Mbr_lkup"),
        (F.col("Ids_Extract.MBR_SK") == F.col("Mbr_lkup.MBR_SK")),
        how="left"
    )
    .join(
        df_ApptRsn_lkup.alias("ApptRsn_lkup"),
        (F.col("Ids_Extract.EVT_APPT_RSN_SK") == F.col("ApptRsn_lkup.EVT_APPT_RSN_SK")),
        how="left"
    )
)

df_Ids_Land_pre = df_Transformer_48_pre.filter(F.col("web_lkup.EVT_TYP_CD").isNull())  # Constraint: IsNull(web_lkup.EVT_TYP_CD) = true
df_Ids_Land_select = df_Ids_Land_pre.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lpad(F.lit("I"),10,"I")[0:10].alias("INSRT_UPDT_CD"),
    F.lpad(F.lit("N"),1,"N")[0:1].alias("DISCARD_IN"),
    F.lpad(F.lit("Y"),1,"Y")[0:1].alias("PASS_THRU_IN"),
    F.lit(CurrDate).alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.concat(
        F.col("Ids_Extract.GRP_ID"), F.lit(";"),
        F.col("Ids_Extract.EVT_DT_SK"), F.lit(";"),
        F.col("Ids_Extract.EVT_TYP_ID"), F.lit(";"),
        F.col("Ids_Extract.SEQ_NO"), F.lit(";"),
        F.col("Ids_Extract.EVT_APPT_STRT_TM_TX"), F.lit(";"),
        F.lit(SrcSysCd)
    ).alias("PRI_KEY_STRING"),
    F.lit(0).alias("EVT_APPT_SK"),
    F.col("Ids_Extract.GRP_ID").alias("GRP_ID"),
    F.col("Ids_Extract.EVT_DT_SK").alias("EVT_DT_SK"),
    F.col("Ids_Extract.EVT_TYP_ID").alias("EVT_TYP_ID"),
    F.col("Ids_Extract.SEQ_NO").alias("SEQ_NO"),
    F.col("Ids_Extract.EVT_APPT_STRT_TM_TX").alias("EVT_APPT_STRT_TM_TX"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(F.col("Staff_lkup.EVT_STAFF_ID").isNull(), F.lit("UNK")).otherwise(F.col("Staff_lkup.EVT_STAFF_ID")).alias("STAFF_ID"),
    F.when(F.col("Mbr_lkup.MBR_UNIQ_KEY").isNull(), F.lit("NA")).otherwise(F.col("Mbr_lkup.MBR_UNIQ_KEY")).alias("MBR_UNIQ_KEY"),
    F.when(F.col("ApptRsn_lkup.EVT_APPT_RSN_ID").isNull(), F.lit("UNK")).otherwise(F.col("ApptRsn_lkup.EVT_APPT_RSN_ID")).alias("EVT_APPT_RSN_ID"),
    F.when(F.col("Loc_lkup.EVT_LOC_ID").isNull(), F.lit("UNK")).otherwise(F.col("Loc_lkup.EVT_LOC_ID")).alias("LOC_ID"),
    F.lpad(F.lit("Y"),1,"Y")[0:1].alias("EVT_APPT_DELD_IN"),
    F.col("Ids_Extract.EVT_APPT_END_TM_TX").alias("EVT_APPT_END_TM_TX"),
    F.col("Ids_Extract.LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
    F.col("Ids_Extract.LAST_UPDT_USER_ID").alias("LAST_UPDT_USER_ID"),
    F.when(F.col("ApptRsn_lkup.EVT_TYP_ID").isNull(), F.lit("UNK")).otherwise(F.col("ApptRsn_lkup.EVT_TYP_ID")).alias("EVT_TYP_CD"),
    F.when(F.col("ApptRsn_lkup.EVT_APPT_RSN_NM").isNull(), F.lit("UNK")).otherwise(F.col("ApptRsn_lkup.EVT_APPT_RSN_NM")).alias("EVT_APPT_RSN_NM"),
    F.when(F.col("ApptRsn_lkup.EFF_DT_SK").isNull(), F.lit("1753-01-01")).otherwise(F.col("ApptRsn_lkup.EFF_DT_SK")).alias("EFF_DT")
)

# scenario A => hashed file hf_evtappt_idsdata_land => deduplicate
df_Ids_Data = dedup_sort(
    df_Ids_Land_select,
    partition_cols=[
      "JOB_EXCTN_RCRD_ERR_SK",
      "INSRT_UPDT_CD",
      "DISCARD_IN",
      "PASS_THRU_IN",
      "FIRST_RECYC_DT",
      "ERR_CT",
      "RECYCLE_CT",
      "SRC_SYS_CD",
      "PRI_KEY_STRING",
      "EVT_APPT_SK",
      "GRP_ID",
      "EVT_DT_SK",
      "EVT_TYP_ID",
      "SEQ_NO",
      "EVT_APPT_STRT_TM_TX",
      "CRT_RUN_CYC_EXCTN_SK",
      "LAST_UPDT_RUN_CYC_EXCTN_SK",
      "STAFF_ID",
      "MBR_UNIQ_KEY",
      "EVT_APPT_RSN_ID",
      "LOC_ID",
      "EVT_APPT_DELD_IN",
      "EVT_APPT_END_TM_TX",
      "LAST_UPDT_DTM",
      "LAST_UPDT_USER_ID",
      "EVT_TYP_CD",
      "EVT_APPT_RSN_NM",
      "EFF_DT"
    ],
    sort_cols=[]
)

# ---------------------------------------------------------------------------------------
# Link_Collector => collects df_Web_Data and df_Ids_Data => union
common_cols_for_union = [
  "JOB_EXCTN_RCRD_ERR_SK",
  "INSRT_UPDT_CD",
  "DISCARD_IN",
  "PASS_THRU_IN",
  "FIRST_RECYC_DT",
  "ERR_CT",
  "RECYCLE_CT",
  "SRC_SYS_CD",
  "PRI_KEY_STRING",
  "EVT_APPT_SK",
  "GRP_ID",
  "EVT_DT_SK",
  "EVT_TYP_ID",
  "SEQ_NO",
  "EVT_APPT_STRT_TM_TX",
  "CRT_RUN_CYC_EXCTN_SK",
  "LAST_UPDT_RUN_CYC_EXCTN_SK",
  "STAFF_ID",
  "MBR_UNIQ_KEY",
  "EVT_APPT_RSN_ID",
  "LOC_ID",
  "EVT_APPT_DELD_IN",
  "EVT_APPT_END_TM_TX",
  "LAST_UPDT_DTM",
  "LAST_UPDT_USER_ID",
  "EVT_TYP_CD",
  "EVT_APPT_RSN_NM",
  "EFF_DT"
]

df_Web_Data_sel = df_Web_Data.select(common_cols_for_union)
df_Ids_Data_sel = df_Ids_Data.select(common_cols_for_union)

df_Link_Collector = df_Web_Data_sel.unionByName(df_Ids_Data_sel)

# ---------------------------------------------------------------------------------------
# Next: Pkey transformer
# We also have a lookup link "lkup" from df_hf_evt_appt_lkup => left join on
#   (Transform.GRP_ID -> lkup.GRP_ID,
#    Transform.EVT_DT_SK -> lkup.EVT_DT_SK,
#    Transform.EVT_TYP_ID -> lkup.EVT_TYP_ID,
#    Transform.SEQ_NO -> lkup.SEQ_NO,
#    Transform.EVT_APPT_STRT_TM_TX -> lkup.EVT_APPT_STRT_TM_TX,
#    Transform.SRC_SYS_CD -> lkup.SRC_SYS_CD )
# Then stage variables:
#   SK = if IsNull(lkup.EVT_APPT_SK) then KeyMgtGetNextValueConcurrent("EVT_APPT_SK") else lkup.EVT_APPT_SK
#   NewCrtRunCycExtcnSk = if IsNull(lkup.EVT_APPT_SK) then CurrRunCycle else lkup.CRT_RUN_CYC_EXCTN_SK
#
# The output link "Key" => we push the transformed columns, substituting stage variables SK and NewCrtRunCycExtcnSk. 
# We'll implement as a single left join and then expressions. Then we finalize. 
# Also we must call SurrogateKeyGen after we define df_enriched to handle the SK logic.

df_Pkey_pre = (
    df_Link_Collector.alias("Transform")
    .join(
        df_hf_evt_appt_lkup.alias("lkup"),
        (F.col("Transform.GRP_ID")==F.col("lkup.GRP_ID")) &
        (F.col("Transform.EVT_DT_SK")==F.col("lkup.EVT_DT_SK")) &
        (F.col("Transform.EVT_TYP_ID")==F.col("lkup.EVT_TYP_ID")) &
        (F.col("Transform.SEQ_NO")==F.col("lkup.SEQ_NO")) &
        (F.col("Transform.EVT_APPT_STRT_TM_TX")==F.col("lkup.EVT_APPT_STRT_TM_TX")) &
        (F.col("Transform.SRC_SYS_CD")==F.col("lkup.SRC_SYS_CD")),
        how="left"
    )
)

df_Pkey_vars = df_Pkey_pre.withColumn(
    "__SK__",
    F.when(F.col("lkup.EVT_APPT_SK").isNull(), F.lit(None)).otherwise(F.col("lkup.EVT_APPT_SK"))
).withColumn(
    "__NewCrtRunCycExtcnSk__",
    F.when(F.col("lkup.EVT_APPT_SK").isNull(), F.lit(CurrRunCycle)).otherwise(F.col("lkup.CRT_RUN_CYC_EXCTN_SK"))
)

# We will handle the SurrogateKeyGen for column "EVT_APPT_SK". The logic says:
#  if lkup.EVT_APPT_SK is null => we call KeyMgtGetNextValueConcurrent => that means we call SurrogateKeyGen
#  otherwise use lkup.EVT_APPT_SK.
# In this pipeline approach, we'll define the column as "EVT_APPT_SK" = "__SK__"
# Then we will pass the DataFrame to SurrogateKeyGen to fill in missing values.

df_pkey_interim = df_Pkey_vars.select(
    F.col("Transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("Transform.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("Transform.DISCARD_IN").alias("DISCARD_IN"),
    F.col("Transform.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("Transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("Transform.ERR_CT").alias("ERR_CT"),
    F.col("Transform.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("__SK__").alias("EVT_APPT_SK"),
    F.col("Transform.GRP_ID").alias("GRP_ID"),
    F.col("Transform.EVT_DT_SK").alias("EVT_DT_SK"),
    F.col("Transform.EVT_TYP_ID").alias("EVT_TYP_ID"),
    F.col("Transform.SEQ_NO").alias("SEQ_NO"),
    F.col("Transform.EVT_APPT_STRT_TM_TX").alias("EVT_APPT_STRT_TM_TX"),
    # STAFF_ID from Transform
    F.col("Transform.STAFF_ID").alias("STAFF_ID"),
    # CRT_RUN_CYC_EXCTN_SK = NewCrtRunCycExtcnSk
    F.col("__NewCrtRunCycExtcnSk__").alias("CRT_RUN_CYC_EXCTN_SK"),
    # LAST_UPDT_RUN_CYC_EXCTN_SK = CurrRunCycle
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Transform.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("Transform.EVT_APPT_RSN_ID").alias("EVT_APPT_RSN_ID"),
    F.col("Transform.LOC_ID").alias("LOC_ID"),
    F.col("Transform.EVT_APPT_DELD_IN").alias("EVT_APPT_DELD_IN"),
    F.col("Transform.EVT_APPT_END_TM_TX").alias("EVT_APPT_END_TM_TX"),
    F.col("Transform.LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
    F.col("Transform.LAST_UPDT_USER_ID").alias("LAST_UPDT_USER_ID"),
    F.col("Transform.EVT_TYP_CD").alias("EVT_TYP_CD"),
    F.col("Transform.EVT_APPT_RSN_NM").alias("EVT_APPT_RSN_NM"),
    F.col("Transform.EFF_DT").alias("EFF_DT"),
    # also need lkup.EVT_APPT_SK to check constraint, but the next logic merges. We'll do it with the final DF.
)

# Now call SurrogateKeyGen for "EVT_APPT_SK"
df_enriched = df_pkey_interim
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"EVT_APPT_SK",<schema>,<secret_name>)

# That yields the final df for the "Key" link from Pkey transform.

# ---------------------------------------------------------------------------------------
# Also from Pkey: there's an output link "updt" with constraint "IsNull(lkup.EVT_APPT_SK)=@TRUE".
# In scenario B, we do a MERGE to "dummy_hf_evt_appt".
# The PK columns are: (GRP_ID, EVT_DT_SK, EVT_TYP_ID, SEQ_NO, EVT_APPT_STRT_TM_TX, SRC_SYS_CD).
# We must upsert CRT_RUN_CYC_EXCTN_SK, EVT_APPT_SK. 
# So we do a filter for those rows where lkup.EVT_APPT_SK isNull:

df_updt_pre = df_Pkey_vars.filter(F.col("lkup.EVT_APPT_SK").isNull())

df_updt = df_updt_pre.select(
    F.col("Transform.GRP_ID").alias("GRP_ID"),
    F.col("Transform.EVT_DT_SK").alias("EVT_DT_SK"),
    F.col("Transform.EVT_TYP_ID").alias("EVT_TYP_ID"),
    F.col("Transform.SEQ_NO").alias("SEQ_NO"),
    F.col("Transform.EVT_APPT_STRT_TM_TX").alias("EVT_APPT_STRT_TM_TX"),
    F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(None).alias("EVT_APPT_SK")  # We'll rely on SurrogateKeyGen results from df_enriched for actual SK. 
)

# We must merge these updates into IDS.dummy_hf_evt_appt. The MERGE condition: match on PK, update CRT_RUN_CYC_EXCTN_SK + EVT_APPT_SK, else insert. 
# We'll put them into a staging table "STAGING.WebUserInfoEvtAppt_hf_evt_appt_updt_temp" (job name: WebUserInfoEvtAppt, stage name: hf_evt_appt_updt).
temp_table_updt = "STAGING.WebUserInfoEvtAppt_hf_evt_appt_updt_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_updt}", jdbc_url_ids, jdbc_props_ids)

# Write df_updt to the staging table
df_updt.write \
    .format("jdbc") \
    .option("url", jdbc_url_ids) \
    .options(**jdbc_props_ids) \
    .option("dbtable", temp_table_updt) \
    .mode("overwrite") \
    .save()

# Now MERGE:
merge_sql_updt = f"""
MERGE INTO IDS.dummy_hf_evt_appt AS T
USING {temp_table_updt} AS S
ON
  T.GRP_ID = S.GRP_ID AND
  T.EVT_DT_SK = S.EVT_DT_SK AND
  T.EVT_TYP_ID = S.EVT_TYP_ID AND
  T.SEQ_NO = S.SEQ_NO AND
  T.EVT_APPT_STRT_TM_TX = S.EVT_APPT_STRT_TM_TX AND
  T.SRC_SYS_CD = S.SRC_SYS_CD
WHEN MATCHED THEN
  UPDATE SET
    T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
    T.EVT_APPT_SK = S.EVT_APPT_SK
WHEN NOT MATCHED THEN
  INSERT (GRP_ID, EVT_DT_SK, EVT_TYP_ID, SEQ_NO, EVT_APPT_STRT_TM_TX, SRC_SYS_CD, CRT_RUN_CYC_EXCTN_SK, EVT_APPT_SK)
  VALUES (S.GRP_ID, S.EVT_DT_SK, S.EVT_TYP_ID, S.SEQ_NO, S.EVT_APPT_STRT_TM_TX, S.SRC_SYS_CD, S.CRT_RUN_CYC_EXCTN_SK, S.EVT_APPT_SK);
"""
execute_dml(merge_sql_updt, jdbc_url_ids, jdbc_props_ids)

# ---------------------------------------------------------------------------------------
# Another Pkey output link "Snapshot" => B_EVT_APPT => columns:
#   GRP_ID, EVT_DT_SK, EVT_TYP_ID, SEQ_NO, EVT_APPT_STRT_TM_TX, SRC_SYS_CD_SK
df_Snapshot = df_Pkey_vars.select(
    F.col("Transform.GRP_ID").alias("GRP_ID"),
    F.col("Transform.EVT_DT_SK").alias("EVT_DT_SK"),
    F.col("Transform.EVT_TYP_ID").alias("EVT_TYP_ID"),
    F.col("Transform.SEQ_NO").alias("SEQ_NO"),
    F.col("Transform.EVT_APPT_STRT_TM_TX").alias("EVT_APPT_STRT_TM_TX"),
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK")
)

# Write to a .dat file (CSeqFileStage) in the "load" directory
#   => "B_EVT_APPT.dat"
# The instructions for reading/writing delimited: use "write_files(df, file_path, delimiter, mode, is_parquet, header, quote, nullValue)"
#   with "header=False"? Also no mention of quote? We'll keep default quote="\"".
final_cols_snapshot = ["GRP_ID","EVT_DT_SK","EVT_TYP_ID","SEQ_NO","EVT_APPT_STRT_TM_TX","SRC_SYS_CD_SK"]
# For char/varchar we do rpad if needed. Checking the JSON, they look like varchars. If length was not specified, we do not have a forced length. We only do the ones that had "char" with a length. Here they didn't define them. We'll leave them as is.
df_Snapshot_sel = df_Snapshot.select(*final_cols_snapshot)
write_files(
    df_Snapshot_sel,
    f"{adls_path}/load/B_EVT_APPT.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

# ---------------------------------------------------------------------------------------
# Another Pkey output link "Key" => WebUserInfoEvtAppt => final .dat
# The columns from that link in the JSON:
final_cols_webuserinfo = [
  "JOB_EXCTN_RCRD_ERR_SK",
  "INSRT_UPDT_CD",
  "DISCARD_IN",
  "PASS_THRU_IN",
  "FIRST_RECYC_DT",
  "ERR_CT",
  "RECYCLE_CT",
  "SRC_SYS_CD",
  "PRI_KEY_STRING",
  "EVT_APPT_SK",
  "GRP_ID",
  "EVT_DT_SK",
  "EVT_TYP_ID",
  "SEQ_NO",
  "EVT_APPT_STRT_TM_TX",
  "STAFF_ID",
  "CRT_RUN_CYC_EXCTN_SK",
  "LAST_UPDT_RUN_CYC_EXCTN_SK",
  "MBR_UNIQ_KEY",
  "EVT_APPT_RSN_ID",
  "LOC_ID",
  "EVT_APPT_DELD_IN",
  "EVT_APPT_END_TM_TX",
  "LAST_UPDT_DTM",
  "LAST_UPDT_USER_ID",
  "EVT_TYP_CD",
  "EVT_APPT_RSN_NM",
  "EFF_DT"
]

# Now df_enriched has these columns with the same names. We select them in order.
# For char/varchar columns with specified length in the JSON, we rpad. Example "INSRT_UPDT_CD"(char(10)), "DISCARD_IN"(char(1)), etc.
# We'll do a sequence of withColumn for those explicitly typed as char(x) or varchar with a length in the final link definition:

df_final_pre = df_enriched.select(*final_cols_webuserinfo)
df_final = (
    df_final_pre
    .withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " "))
    .withColumn("EVT_DT_SK", F.rpad(F.col("EVT_DT_SK"), 10, " "))
    .withColumn("EVT_APPT_DELD_IN", F.rpad(F.col("EVT_APPT_DELD_IN"), 1, " "))
)

write_files(
    df_final,
    f"{adls_path}/key/WebUserInfoEvtApptExtr.EvtAppt.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

# ---------------------------------------------------------------------------------------
# Lastly, "hf_evt_appt_updt" in scenario B is already handled by the merge. We do not write a separate parquet because scenario B hashed file is replaced by merges to the dummy table.

# ---------------------------------------------------------------------------------------
# Done.