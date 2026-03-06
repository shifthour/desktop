# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2012 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by:
# MAGIC                     MpiBcbsFctsTmpDrvrTblSeq
# MAGIC 
# MAGIC Processing:
# MAGIC                     Extract MPI_BCBS.MBR_OUTP into IDS Bekey load files and FCTS_BE_INPT file
# MAGIC 
# MAGIC Control Job Rerun Information: 
# MAGIC                     Previous Run Successful:    What needs to happen before a special run can occur?
# MAGIC                     Previous Run Aborted:         Restart, no other steps necessary
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                                  Project/                                                                                                                                    Code                   Date
# MAGIC Developer           Date              Altiris #        Change Description                                                                                              Reviewer            Reviewed
# MAGIC -------------------------  -------------------   ----------------   --------------------------------------------------------------------------------------------------------------------------   -------------------------  -------------------
# MAGIC Rick Henry         2012-07-11    4426            Original programming                                                                                            Bhoomi Dasari   08/17/2012
# MAGIC SAndrew            2012-10-14    4426            Changed the source field for the bekey from MbrOutp.PREV_INDV_BE_KEY     Bhoomi Dasari   10/16/2012
# MAGIC                                                                     to  MbrOutp.ASG_INDV_BE_KEY.  Removed all of the lookups for                     Kalyan Neelam   2012-11-15
# MAGIC                                                                     source system and just included it in the join changed the source of the 
# MAGIC                                                                     balancing data to be from the main transformer and not a seperate lookup
# MAGIC SAndrew            2012-10-14    4426            Removed the lookup to facets to get the members bekey.   this should be          Kalyan Neelam   2012-11-15      
# MAGIC                                                                     known on the mpi files / table.  This defeats the purpose of wanting to update 
# MAGIC                                                                     the sub bekey for all members if the only change is the subscribers bekey 
# MAGIC                                                                     changed.   facets will not yet know this.
# MAGIC SAndrew            2012-10-18    4426            Added 3 hash files, making a total of 5.  Added additional extract criteria -          Kalyan Neelam   2012-11-15    
# MAGIC                                                                     pull all members that did not change but the subscriber did .
# MAGIC Hugh Sisson      2012-10-19    4426            Added lookup transform stage to prevent 2 rows for same member being            Kalyan Neelam   2012-11-15        
# MAGIC                                                                     created - 1 from member being changed and 1 for Bub BE Key changing.
# MAGIC                                                                     Fixed column order in 2 hashed files.  Checked Create File and 
# MAGIC                                                                     Clear File Before Writing in one hashed file 
# MAGIC Hugh Sisson      2012-10-24    4426            Moved in logic for creating FCTS_BE_INPT file from IdsIndvBekeyFkey  job      Kalyan Neelam   2012-11-15
# MAGIC                                                                     Changed logic for building lookup hashed file used for building INDV_BE
# MAGIC                                                                     Changed grouping criteria in the two Sort stages
# MAGIC Hugh Sisson      2012-10-27    4426            Added constraint of ASG_INDV_BE_KEY <> 0 to P_MBR_BE_KEY_XREF      Kalyan Neelam   2012-11-15
# MAGIC 
# MAGIC Abhiram D         2012-11-26     4426           Changed the SQL on the ChangedMbrs and ChangedSubs in the                      Bhoomi Dasari    2012-11-29
# MAGIC \(9)\(9)\(9)\(9)    MPI BCBSEXT table, added steps to lookup the eligibility on MEME_CK\(9)
# MAGIC Abhiram D         2012-12-7\(9)4426\(9)    Changed the SQL on the ChangedMbrs and ChangedSubs in the                     Bhoomi Dasari    2012-12-10 
# MAGIC \(9)\(9)\(9)\(9)    MPI BCBSEXT table, added  "AND 
# MAGIC                                                                     tblSRC_SYS_XWALK.ENVRN_ID = '#ProcessOwner#'"
# MAGIC SAndrew            2013-01-17    4426            Changed rules for the records to write to the facets_bekey_upd and                 Bhoomi Dasari    2013-01-18 
# MAGIC                                                                     to the INDV_BE / B tables.    IntegrateNewDevl
# MAGIC Dan Long          2013-07-30   TFS-2434      Added new stage IDS_INDV_BE_KEY_TEXT to be used as a look                  Bhoomi Dasari   8/18/2013  
# MAGIC                                                                     in the MainLogic transformer stage to push MPI changes to be loaded into
# MAGIC                                                                     Facets. Modified the Facets_File constraint.                IntegrateNewDevl
# MAGIC Dan Long          2013-10-10   TFS-2594      Removed the following code from stage CMC_MEME_MEMBER                          IntegrateNewDevl                     
# MAGIC                                                                     AND\(9)ELIG.MEPE_ELIG_IND = 'Y'
# MAGIC                                                                     AND\(9)ELIG.MEPE_EFF_DT <= '#CurrDtm#'
# MAGIC                                                                     AND\(9)ELIG.MEPE_TERM_DT >= '#CurrDtm#'        
# MAGIC 
# MAGIC                                                                     Used IntegratedCurrDevl version of the progran as base to pull in 
# MAGIC                                                                     constraint TRIM(Lookup.INDV_BE_KEY_TX) 
# MAGIC 
# MAGIC SAndrew             2015-08-17      5318        changed the update mode to the output file facets_bekey_upd.dat                   Kalyan Neelam       2015-09-22
# MAGIC                                                                     from an overwrite to an append
# MAGIC                                                                    removed ProcessOwner parameter and how it was used in the 2 main extracts.   No longer will the Environment ID be used to pull from MBR_OUTP
# MAGIC 
# MAGIC Prabhu ES          2022-03-30      S2S          MSSQL ODBC conn params added                             IntegrateDev5
# MAGIC 
# MAGIC Goutham Kalidindi             2023-07027       US-590099\(9)\(9)  Updated Extract SQL to EXCLUDE data WHERE                                      IntegrateDev2   Reddy Sanam    07/28/2023
# MAGIC                                                                                                    PRCS_OWNER_SRC_SYS_ENVRN_ID = 'BLUESC' 
# MAGIC                                                                                                     added new parameter to pass the EXLUDE Value

# MAGIC No PKey is required
# MAGIC Facets File Exclusion
# MAGIC 
# MAGIC IsNull( AllMbrOutpChangesSubMbrs.SUB_ID) = @FALSE               AND
# MAGIC IsNull( AllMbrOutpChangesSubMbrs.GRP_ID) = @FALSE               AND 
# MAGIC IsNull( maxPrcsDtmForFacetsBeInput.MPI_MBR_ID ) = @FALSE   AND 
# MAGIC AllMbrOutpChangesSubMbrs.ASG_INDV_BE_KEY  =  maxPrcsDtmForFacetsBeInput.ASG_INDV_BE_KEY   AND 
# MAGIC ( AllMbrOutpChangesSubMbrs.PREV_INDV_BE_KEY <> AllMbrOutpChangesSubMbrs.ASG_INDV_BE_KEY OR svFacetsBeKeyValue <> AllMbrOutpChangesSubMbrs.ASG_INDV_BE_KEY ) AND 
# MAGIC AllMbrOutpChangesSubMbrs.PRCS_RUN_DTM >= PrevPrcRunDtm
# MAGIC Removed
# MAGIC Facets File
# MAGIC  AND MbrOutpChangesSubMbrs.PRCS_RUN_DTM >= PrevPrcRunDtm
# MAGIC 
# MAGIC INDV_BE_KEY
# MAGIC AND  AllMbrOutpChangesSubMbrs.PRCS_RUN_DTM >= PrevPrcRunDtm
# MAGIC Balancing File for P_MBR_BE_KEY_XREF
# MAGIC Balancing load file for INDV_BE
# MAGIC This exact code is in the MPI_BCBS.MBR_OUTP extract into the facets temp table /MPI/Driver/MpiBcbsFctsTmpMPIMbrDrvrBld program.  These have to be in synch with each other.
# MAGIC Extract from IDS INDV_BE the CreateRunCycle
# MAGIC Update File for Facets to assign BEKEY in CMC_MEME_MEMBER 
# MAGIC Fixed Record Format
# MAGIC Bekey is right justified
# MAGIC 
# MAGIC .../external/facets_bekey_upd.dat
# MAGIC 
# MAGIC Picked up by FAMBD030
# MAGIC Primary key file used in IdsIndvBekeyFkey job
# MAGIC DataStage Extract from RepConnect Staging Table
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType, CharType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


CurrDtm = get_widget_value('CurrDtm','2015-05-08 00:00:00.000')
CurrDate = get_widget_value('CurrDate','2015-09-07')
RunCycle = get_widget_value('RunCycle','100')
PrevPrcRunDtm = get_widget_value('PrevPrcRunDtm','2012-12-13 09:56:50.000')
RunID = get_widget_value('RunID','1234')
TmpTblRunID = get_widget_value('TmpTblRunID','123455')
FacetsOwnerParam = get_widget_value('$FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
MPI_BCBSOwnerParam = get_widget_value('$MPI_BCBSOwner','')
mpibcbs_secret_name = get_widget_value('mpibcbs_secret_name','')
IDSOwnerParam = get_widget_value('$IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
SrcSysExclude = get_widget_value('SrcSysExclude','')

# MAGIC %run ../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

# ---------------------------------------------
# STAGE: IDS (DB2Connector) -> hf_mpibcbs_indv_be_createcyc (Scenario A Hashed File)
# ---------------------------------------------
jdbc_url_IDS, jdbc_props_IDS = get_db_config(ids_secret_name)
df_IDS_raw = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS)
    .options(**jdbc_props_IDS)
    .option("query", f"SELECT INDV_BE_KEY, CRT_RUN_CYC_EXCTN_SK FROM {IDSOwnerParam}.INDV_BE")
    .load()
)
df_IDS = dedup_sort(
    df_IDS_raw,
    partition_cols=["INDV_BE_KEY"],
    sort_cols=[]
)

# ---------------------------------------------
# STAGE: hf_mpibcbs_indv_be_createcyc -> MainLogic (link "getCreateRunCyle")
# We will hold df_IDS as df_getCreateRunCyle for the downstream lookup link.
# ---------------------------------------------
df_getCreateRunCyle = df_IDS

# ---------------------------------------------
# STAGE: CMC_MEME_MEMBER1 (ODBCConnector) -> hf_mpibcbs_indv_be_extr_meme_eff_dt (Scenario A Hashed File)
# ---------------------------------------------
# There is no explicit secret name provided for the ODBC connection. Use <...> for manual remediation.
jdbc_url_CMC_MEME_MEMBER1, jdbc_props_CMC_MEME_MEMBER1 = get_db_config(<...>)
df_CMC_MEME_MEMBER1_raw = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_CMC_MEME_MEMBER1)
    .options(**jdbc_props_CMC_MEME_MEMBER1)
    .option("query", f"""
SELECT MEME.MEME_CK,
       MEME.MEME_ORIG_EFF_DT
FROM tempdb..TMP_MPI_MBR_DRVR DRVR,
     {FacetsOwnerParam}.CMC_MEME_MEMBER MEME
WHERE DRVR.MEME_CK = MEME.MEME_CK
""")
    .load()
)
df_CMC_MEME_MEMBER1 = dedup_sort(
    df_CMC_MEME_MEMBER1_raw,
    partition_cols=["MEME_CK"],
    sort_cols=[]
)
df_hf_mpibcbs_indv_be_extr_meme_eff_dt = df_CMC_MEME_MEMBER1

# ---------------------------------------------
# STAGE: MPI_BCBSEXT (CODBCStage) -> Two outputs => hf_mpibcbs_indv_be_chngd_sub3 & hf_mpibcbs_indv_be_chngd_mbrs3 (Both scenario A)
# ---------------------------------------------
# Again, no explicit secret name, so use <...>.
jdbc_url_MPI_BCBSEXT, jdbc_props_MPI_BCBSEXT = get_db_config(<...>)
# For the "ChangedSubNotMbrs" link:
df_MPI_BCBSEXT_ChangedSubNotMbrs_raw = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_MPI_BCBSEXT)
    .options(**jdbc_props_MPI_BCBSEXT)
    .option("query", f"""
SELECT 
 tblMBR_OUTP.MPI_MBR_ID, 
 tblMBR_OUTP.PRCS_OWNER_SRC_SYS_ENVRN_CK, 
 tblSRC_SYS_XWALK.SRC_SYS_ID,
 tblSRC_SYS_XWALK.PRCS_OWNER_ID,
 tblMBR_OUTP.PRCS_RUN_DTM, 
 tblMBR_OUTP.SSN,
 tblMBR_OUTP.BRTH_DT, 
 tblMBR_OUTP.FIRST_NM, 
 tblMBR_OUTP.MIDINIT, 
 tblMBR_OUTP.LAST_NM, 
 tblMBR_OUTP.NM_PFX,
 tblMBR_OUTP.NM_SFX,
 tblMBR_OUTP.MBR_GNDR_CD, 
 tblMBR_OUTP.SUB_ID, 
 tblMBR_OUTP.MBR_SFX_NO, 
 tblMBR_OUTP.ADDR_LN_1, 
 tblMBR_OUTP.ADDR_LN_2, 
 tblMBR_OUTP.ADDR_LN_3, 
 tblMBR_OUTP.CITY_NM, 
 tblMBR_OUTP.ST_CD, 
 tblMBR_OUTP.POSTAL_CD, 
 tblMBR_OUTP.CTRY_CD, 
 tblMBR_OUTP.MBR_HOME_PHN_NO, 
 tblMBR_OUTP.MBR_MOBL_PHN_NO, 
 tblMBR_OUTP.MBR_WORK_PHN_NO, 
 tblMBR_OUTP.EMAIL_ADDR_TX_1, 
 tblMBR_OUTP.EMAIL_ADDR_TX_2,
 tblMBR_OUTP.EMAIL_ADDR_TX_3, 
 tblMBR_OUTP.MBR_RELSHP_CD,
 tblMBR_OUTP.GRP_ID, 
 tblMBR_OUTP.GRP_NM, 
 isnull(tblSUB_OUTP.ASG_INDV_BE_KEY, isnull(tblMBR_OUTP.SUB_INDV_BE_KEY, 0)),
 tblMBR_OUTP.MPI_SUB_ID, 
 tblMBR_OUTP.GRP_EDI_VNDR_NM,
 tblMBR_OUTP.CLNT_ID, 
 tblMBR_OUTP.MBR_DCSD_DT, 
 tblMBR_OUTP.ASG_INDV_BE_KEY
FROM {MPI_BCBSOwnerParam}.MBR_OUTP  tblMBR_OUTP ,
     {MPI_BCBSOwnerParam}.MBR_OUTP  tblSUB_OUTP ,
     {MPI_BCBSOwnerParam}.PRCS_OWNER_SRC_SYS_ENVRN_CRSWALK  tblSRC_SYS_XWALK
WHERE   tblMBR_OUTP.MPI_SUB_ID   = tblSUB_OUTP.MPI_SUB_ID
AND     tblSUB_OUTP.MBR_RELSHP_CD= 'SUBSCRIBER'
AND     tblSUB_OUTP.PRCS_RUN_DTM > '{PrevPrcRunDtm}'
AND     tblMBR_OUTP.PRCS_OWNER_SRC_SYS_ENVRN_CK = tblSUB_OUTP.PRCS_OWNER_SRC_SYS_ENVRN_CK
AND     tblMBR_OUTP.PRCS_OWNER_SRC_SYS_ENVRN_CK = tblSRC_SYS_XWALK.PRCS_OWNER_SRC_SYS_ENVRN_CK
AND     tblSRC_SYS_XWALK.PRCS_OWNER_SRC_SYS_ENVRN_ID NOT IN ({SrcSysExclude})
AND     tblMBR_OUTP.PRCS_RUN_DTM < '{CurrDtm}'
AND     tblMBR_OUTP.MBR_RELSHP_CD <> 'SUBSCRIBER'
AND     tblMBR_OUTP.MPI_MBR_ID NOT IN
   (SELECT MPI_MBR_ID 
    FROM {MPI_BCBSOwnerParam}.MBR_OUTP  AS OUTP 
    WHERE OUTP.MPI_NTFCTN_IN = 'Y'
      AND OUTP.SSN = '000000000'
      AND OUTP.BRTH_DT = '1753-01-01'
      AND OUTP.MPI_SUB_ID = 0)
""")
    .load()
)
df_chngd_sub_not_mbrs = dedup_sort(
    df_MPI_BCBSEXT_ChangedSubNotMbrs_raw,
    partition_cols=[
        "MPI_MBR_ID",
        "PRCS_OWNER_SRC_SYS_ENVRN_CK",
        "SRC_SYS_ID",
        "PRCS_OWNER_ID",
        "PRCS_RUN_DTM"
    ],
    sort_cols=[]
)

# For the "ChngdPeople" link:
df_MPI_BCBSEXT_ChngdPeople_raw = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_MPI_BCBSEXT)
    .options(**jdbc_props_MPI_BCBSEXT)
    .option("query", f"""
SELECT 
 tblMBR_OUTP.MPI_MBR_ID, 
 tblMBR_OUTP.PRCS_OWNER_SRC_SYS_ENVRN_CK, 
 tblSRC_SYS_XWALK.SRC_SYS_ID,
 tblSRC_SYS_XWALK.PRCS_OWNER_ID,
 tblMBR_OUTP.PRCS_RUN_DTM, 
 tblMBR_OUTP.SSN,
 tblMBR_OUTP.BRTH_DT, 
 tblMBR_OUTP.FIRST_NM, 
 tblMBR_OUTP.MIDINIT, 
 tblMBR_OUTP.LAST_NM, 
 tblMBR_OUTP.NM_PFX,
 tblMBR_OUTP.NM_SFX,
 tblMBR_OUTP.MBR_GNDR_CD, 
 tblMBR_OUTP.SUB_ID, 
 tblMBR_OUTP.MBR_SFX_NO, 
 tblMBR_OUTP.ADDR_LN_1, 
 tblMBR_OUTP.ADDR_LN_2, 
 tblMBR_OUTP.ADDR_LN_3, 
 tblMBR_OUTP.CITY_NM, 
 tblMBR_OUTP.ST_CD, 
 tblMBR_OUTP.POSTAL_CD, 
 tblMBR_OUTP.CTRY_CD, 
 tblMBR_OUTP.MBR_HOME_PHN_NO, 
 tblMBR_OUTP.MBR_MOBL_PHN_NO, 
 tblMBR_OUTP.MBR_WORK_PHN_NO, 
 tblMBR_OUTP.EMAIL_ADDR_TX_1, 
 tblMBR_OUTP.EMAIL_ADDR_TX_2,
 tblMBR_OUTP.EMAIL_ADDR_TX_3, 
 tblMBR_OUTP.MBR_RELSHP_CD,
 tblMBR_OUTP.GRP_ID, 
 tblMBR_OUTP.GRP_NM, 
 isnull(tblSUB_OUTP.ASG_INDV_BE_KEY, isnull(tblMBR_OUTP.SUB_INDV_BE_KEY, 0)),
 tblMBR_OUTP.MPI_SUB_ID, 
 tblMBR_OUTP.GRP_EDI_VNDR_NM,
 tblMBR_OUTP.CLNT_ID, 
 tblMBR_OUTP.MBR_DCSD_DT, 
 tblMBR_OUTP.ASG_INDV_BE_KEY
FROM  {MPI_BCBSOwnerParam}.MBR_OUTP  tblMBR_OUTP 
      Left Outer Join {MPI_BCBSOwnerParam}.MBR_OUTP tblSUB_OUTP
      on (tblMBR_OUTP.MPI_SUB_ID = tblSUB_OUTP.MPI_SUB_ID
          AND tblMBR_OUTP.PRCS_OWNER_SRC_SYS_ENVRN_CK = tblSUB_OUTP.PRCS_OWNER_SRC_SYS_ENVRN_CK
          AND tblSUB_OUTP.MBR_RELSHP_CD = 'SUBSCRIBER'),
      {MPI_BCBSOwnerParam}.PRCS_OWNER_SRC_SYS_ENVRN_CRSWALK  tblSRC_SYS_XWALK
WHERE tblMBR_OUTP.PRCS_OWNER_SRC_SYS_ENVRN_CK = tblSRC_SYS_XWALK.PRCS_OWNER_SRC_SYS_ENVRN_CK
AND   tblSRC_SYS_XWALK.PRCS_OWNER_SRC_SYS_ENVRN_ID NOT IN ({SrcSysExclude})
AND   tblMBR_OUTP.PRCS_RUN_DTM > '{PrevPrcRunDtm}'
AND   tblMBR_OUTP.PRCS_OWNER_SRC_SYS_ENVRN_CK = tblSRC_SYS_XWALK.PRCS_OWNER_SRC_SYS_ENVRN_CK
AND   tblMBR_OUTP.PRCS_RUN_DTM < '{CurrDtm}'
AND   tblMBR_OUTP.MPI_MBR_ID NOT IN
   (SELECT MPI_MBR_ID
    FROM {MPI_BCBSOwnerParam}.MBR_OUTP AS OUTP
    WHERE OUTP.MPI_NTFCTN_IN = 'Y'
    AND OUTP.SSN = '000000000'
    AND OUTP.BRTH_DT = '1753-01-01'
    AND OUTP.MPI_SUB_ID = 0)
""")
    .load()
)
df_chngd_people = dedup_sort(
    df_MPI_BCBSEXT_ChngdPeople_raw,
    partition_cols=[
        "MPI_MBR_ID",
        "PRCS_OWNER_SRC_SYS_ENVRN_CK",
        "SRC_SYS_ID",
        "PRCS_OWNER_ID",
        "PRCS_RUN_DTM"
    ],
    sort_cols=[]
)

# ---------------------------------------------
# STAGE: hf_mpibcbs_indv_be_chngd_mbrs3 -> Link_Collector_499 (mbr_sub_chnged)
# ---------------------------------------------
df_hf_mpibcbs_indv_be_chngd_mbrs3 = df_chngd_people

# Deduplicate on the primary keys:
df_hf_mpibcbs_indv_be_chngd_mbrs3 = dedup_sort(
    df_hf_mpibcbs_indv_be_chngd_mbrs3,
    partition_cols=[
        "MPI_MBR_ID",
        "PRCS_OWNER_SRC_SYS_ENVRN_CK",
        "SRC_SYS_ID",
        "PRCS_OWNER_ID",
        "PRCS_RUN_DTM"
    ],
    sort_cols=[]
)

# ---------------------------------------------
# STAGE: hf_mpibcbs_indv_be_chngd_sub3 -> Link_Collector_499 (sub_only_chngd)
# ---------------------------------------------
df_hf_mpibcbs_indv_be_chngd_sub3 = df_chngd_sub_not_mbrs

df_hf_mpibcbs_indv_be_chngd_sub3 = dedup_sort(
    df_hf_mpibcbs_indv_be_chngd_sub3,
    partition_cols=[
        "MPI_MBR_ID",
        "PRCS_OWNER_SRC_SYS_ENVRN_CK",
        "SRC_SYS_ID",
        "PRCS_OWNER_ID",
        "PRCS_RUN_DTM"
    ],
    sort_cols=[]
)

# ---------------------------------------------
# STAGE: Link_Collector_499 -> hf_mpibcbs_indv_be_changed_all
# Round-Robin collector (just union).
# ---------------------------------------------
common_cols_lc = [
    "MPI_MBR_ID",
    "PRCS_OWNER_SRC_SYS_ENVRN_CK",
    "SRC_SYS_ID",
    "PRCS_OWNER_ID",
    "PRCS_RUN_DTM",
    "SSN",
    "BRTH_DT",
    "FIRST_NM",
    "MIDINIT",
    "LAST_NM",
    "NM_PFX",
    "NM_SFX",
    "MBR_GNDR_CD",
    "SUB_ID",
    "MBR_SFX_NO",
    "ADDR_LN_1",
    "ADDR_LN_2",
    "ADDR_LN_3",
    "CITY_NM",
    "ST_CD",
    "POSTAL_CD",
    "CTRY_CD",
    "MBR_HOME_PHN_NO",
    "MBR_MOBL_PHN_NO",
    "MBR_WORK_PHN_NO",
    "EMAIL_ADDR_TX_1",
    "EMAIL_ADDR_TX_2",
    "EMAIL_ADDR_TX_3",
    "MBR_RELSHP_CD",
    "GRP_ID",
    "GRP_NM",
    "PREV_INDV_BE_KEY",
    "SUB_INDV_BE_KEY",
    "MPI_SUB_ID",
    "GRP_EDI_VNDR_NM",
    "CLNT_ID",
    "MBR_DCSD_DT",
    "ASG_INDV_BE_KEY"
]
df_lc_499 = df_hf_mpibcbs_indv_be_chngd_mbrs3.select(common_cols_lc).unionByName(
    df_hf_mpibcbs_indv_be_chngd_sub3.select(common_cols_lc),
    allowMissingColumns=True
)

# ---------------------------------------------
# STAGE: hf_mpibcbs_indv_be_changed_all -> 
# two outputs: "AllMbrOutpChangesSubMbrs" (to MainLogic) and "DSLink512" (to ReOrderTheKeys)
# Scenario A dedup
# ---------------------------------------------
df_hf_mpibcbs_indv_be_changed_all = dedup_sort(
    df_lc_499,
    partition_cols=[
        "MPI_MBR_ID",
        "PRCS_OWNER_SRC_SYS_ENVRN_CK",
        "SRC_SYS_ID",
        "PRCS_OWNER_ID",
        "PRCS_RUN_DTM"
    ],
    sort_cols=[]
)
df_AllMbrOutpChangesSubMbrs = df_hf_mpibcbs_indv_be_changed_all
df_DSLink512 = df_hf_mpibcbs_indv_be_changed_all

# ---------------------------------------------
# STAGE: CMC_MEME_MEMBER (ODBCConnector) -> hf_mpibcbs_indv_be_extr_meme (Scenario A)
# ---------------------------------------------
jdbc_url_CMC_MEME_MEMBER, jdbc_props_CMC_MEME_MEMBER = get_db_config(<...>)
df_CMC_MEME_MEMBER_raw = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_CMC_MEME_MEMBER)
    .options(**jdbc_props_CMC_MEME_MEMBER)
    .option("query", f"""
SELECT MEME.MEME_CK,
       MEME.MEME_ORIG_EFF_DT,
       ELIG.MEPE_ELIG_IND
FROM tempdb..TMP_MPI_MBR_DRVR DRVR,
     {FacetsOwnerParam}.CMC_MEME_MEMBER MEME
     LEFT OUTER JOIN {FacetsOwnerParam}.CMC_MEPE_PRCS_ELIG ELIG
       ON MEME.MEME_CK = ELIG.MEME_CK
WHERE DRVR.MEME_CK = MEME.MEME_CK
""")
    .load()
)
df_CMC_MEME_MEMBER = dedup_sort(
    df_CMC_MEME_MEMBER_raw,
    partition_cols=["MEME_CK"],
    sort_cols=[]
)
df_hf_mpibcbs_indv_be_extr_meme = df_CMC_MEME_MEMBER

# ---------------------------------------------
# STAGE: hf_mpibcbs_indv_be_extr_meme -> "chk_meme" (to ReOrderTheKeys)
# Scenario A
# ---------------------------------------------
df_hf_mpibcbs_indv_be_extr_meme = dedup_sort(
    df_hf_mpibcbs_indv_be_extr_meme,
    partition_cols=["MEME_CK"],
    sort_cols=[]
)
df_chk_meme = df_hf_mpibcbs_indv_be_extr_meme

# ---------------------------------------------
# STAGE: ReOrderTheKeys (Transformer)
# InputPins:
#   - Primary: DSLink512 -> df_DSLink512
#   - Lookup:  chk_meme -> df_chk_meme (left join on DSLink512.MPI_MBR_ID = chk_meme.MEME_CK)
# OutputPins:
#   - IndvBeOrder -> to SortIndvBeKey
#   - FacetsBeInptOrder -> to SortMbrId
#
# Stage Variable:
#   svCoverageStatusCd = If Len(Trim(chk_meme.MEPE_ELIG_IND))=0 or IsNULL(chk_meme.MEPE_ELIG_IND)=@TRUE then 'C' else 'A'
# ---------------------------------------------
df_reorder_base = df_DSLink512.alias("DSLink512").join(
    df_chk_meme.alias("chk_meme"),
    on=(F.col("DSLink512.MPI_MBR_ID") == F.col("chk_meme.MEME_CK")),
    how="left"
)

df_reorder = df_reorder_base.withColumn(
    "svCoverageStatusCd",
    F.when(
        (F.length(trim(F.col("chk_meme.MEPE_ELIG_IND"))) == 0) | (F.col("chk_meme.MEPE_ELIG_IND").isNull()),
        F.lit("C")
    ).otherwise(F.lit("A"))
)

# Output Pin "IndvBeOrder"
df_IndvBeOrder = df_reorder.select(
    F.col("DSLink512.ASG_INDV_BE_KEY").alias("ASG_INDV_BE_KEY"),
    F.col("DSLink512.PRCS_OWNER_ID").alias("PRCS_OWNER_ID"),
    F.col("DSLink512.SRC_SYS_ID").alias("SRC_SYS_ID"),
    F.col("DSLink512.PRCS_RUN_DTM").alias("PRCS_RUN_DTM"),
    F.col("DSLink512.MPI_MBR_ID").alias("MPI_MBR_ID"),
    F.col("svCoverageStatusCd").alias("COV_STTUS")
)

# Output Pin "FacetsBeInptOrder"
df_FacetsBeInptOrder = df_reorder.select(
    F.col("DSLink512.MPI_MBR_ID").alias("MPI_MBR_ID"),
    F.col("DSLink512.PRCS_OWNER_ID").alias("PRCS_OWNER_ID"),
    F.col("DSLink512.SRC_SYS_ID").alias("SRC_SYS_ID"),
    F.col("DSLink512.PRCS_RUN_DTM").alias("PRCS_RUN_DTM"),
    F.col("DSLink512.ASG_INDV_BE_KEY").alias("ASG_INDV_BE_KEY")
)

# ---------------------------------------------
# STAGE: SortIndvBeKey -> hf_mpibcbs_indv_max_dtm_max_mbr
# sort spec: ASG_INDV_BE_KEY asc, PRCS_RUN_DTM asc, COV_STTUS dsc, MPI_MBR_ID asc
# ---------------------------------------------
df_sort_IndvBeKey = df_IndvBeOrder.sort(
    F.col("ASG_INDV_BE_KEY").asc(),
    F.col("PRCS_RUN_DTM").asc(),
    F.col("COV_STTUS").desc(),
    F.col("MPI_MBR_ID").asc()
)
df_hf_mpibcbs_indv_max_dtm_max_mbr = dedup_sort(
    df_sort_IndvBeKey,
    partition_cols=["ASG_INDV_BE_KEY", "PRCS_OWNER_ID", "SRC_SYS_ID", "PRCS_RUN_DTM", "MPI_MBR_ID"],
    sort_cols=[]
)

# ---------------------------------------------
# STAGE: SortMbrId -> hf_mpibcbs_indv_max_dtm_max_bekey
# sort spec: MPI_MBR_ID asc, PRCS_OWNER_ID asc, SRC_SYS_ID asc, PRCS_RUN_DTM asc, ASG_INDV_BE_KEY asc
# ---------------------------------------------
df_sort_MbrId = df_FacetsBeInptOrder.sort(
    F.col("MPI_MBR_ID").asc(),
    F.col("PRCS_OWNER_ID").asc(),
    F.col("SRC_SYS_ID").asc(),
    F.col("PRCS_RUN_DTM").asc(),
    F.col("ASG_INDV_BE_KEY").asc()
)
df_hf_mpibcbs_indv_max_dtm_max_bekey = dedup_sort(
    df_sort_MbrId,
    partition_cols=["MPI_MBR_ID","PRCS_OWNER_ID","SRC_SYS_ID","PRCS_RUN_DTM","ASG_INDV_BE_KEY"],
    sort_cols=[]
)

# ---------------------------------------------
# STAGE: MainLogic (Transformer)
#   Primary: hf_mpibcbs_indv_be_changed_all -> df_AllMbrOutpChangesSubMbrs
#   Lookups: getCreateRunCyle -> df_getCreateRunCyle
#            maxPrcsDtmMaxMbrUniqKeyForIndvBe -> df_hf_mpibcbs_indv_max_dtm_max_mbr
#            maxPrcsDtmForFacetsBeInput -> df_hf_mpibcbs_indv_max_dtm_max_bekey
#            chk_org_eff_dt -> df_hf_mpibcbs_indv_be_extr_meme_eff_dt (already df_CMC_MEME_MEMBER1 in effect, not used again directly, but was done above as "df_chk_org_eff_dt"? Actually from stage "hf_mpibcbs_indv_be_extr_meme_eff_dt".
#            Lookup -> This is the IDS_INDV_BE_KEY_TX stage with "sub.SUB_ID=? and mbr.MBR_SFX_NO=?"
#                → We must do a join. We'll do a temporary staging approach for df_AllMbrOutpChangesSubMbrs to link sub.SUB_ID, mbr.MBR_SFX_NO.
# Outputs:   Facets_File, INDV_BEKEY_pkey, B_INDV_BE, hf_mpibcbs_indv_pmbrbekey_outp, hf_mpibcbs_indv_pmbrbekey_chng
# ---------------------------------------------

# 1) For the "IDS_INDV_BE_KEY_TX" lookup, we must create a temp table from df_AllMbrOutpChangesSubMbrs, then join #$IDSOwner#.MBR and #$IDSOwner#.SUB on sub.SUB_SK=mbr.SUB_SK. 
#    The original condition was sub.SUB_ID=?, mbr.MBR_SFX_NO=? => means sub.SUB_ID=AllMbrOutpChangesSubMbrs.SUB_ID, mbr.MBR_SFX_NO=AllMbrOutpChangesSubMbrs.MBR_SFX_NO.
#    We will replicate that in PySpark:
df_tempAllMbrOutpChangesSubMbrs = df_AllMbrOutpChangesSubMbrs.select(
    "SUB_ID", "MBR_SFX_NO"
)
temp_table_name = "STAGING.MpiBcbsIndvBeExtr_IDS_INDV_BE_KEY_TX_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_name}", jdbc_url_IDS, jdbc_props_IDS)
(
    df_tempAllMbrOutpChangesSubMbrs.write.format("jdbc")
    .option("url", jdbc_url_IDS)
    .options(**jdbc_props_IDS)
    .option("dbtable", temp_table_name)
    .mode("overwrite")
    .save()
)
df_ids_indv_be_key_tx_raw = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS)
    .options(**jdbc_props_IDS)
    .option("query", f"""
SELECT mbr.SUB_SK,
       sub.SUB_SK as SUB_SK_SUB,
       sub.SUB_ID,
       mbr.MBR_SFX_NO,
       mbr.INDV_BE_KEY_TX
FROM {IDSOwnerParam}.MBR mbr
     JOIN {IDSOwnerParam}.SUB sub
       ON mbr.SUB_SK = sub.SUB_SK
     JOIN {temp_table_name} tmp
       ON sub.SUB_ID = tmp.SUB_ID
       AND mbr.MBR_SFX_NO = tmp.MBR_SFX_NO
""")
    .load()
)
df_ids_indv_be_key_tx = df_ids_indv_be_key_tx_raw.select(
    "SUB_ID","MBR_SFX_NO","INDV_BE_KEY_TX"
)

# 2) getCreateRunCyle => df_getCreateRunCyle
# 3) maxPrcsDtmMaxMbrUniqKeyForIndvBe => df_hf_mpibcbs_indv_max_dtm_max_mbr
# 4) maxPrcsDtmForFacetsBeInput => df_hf_mpibcbs_indv_max_dtm_max_bekey
# 5) chk_org_eff_dt => from hf_mpibcbs_indv_be_extr_meme_eff_dt => df_CMC_MEME_MEMBER1 => we had df_hf_mpibcbs_indv_be_extr_meme_eff_dt earlier, alias df_chk_org_eff_dt
df_chk_org_eff_dt = df_hf_mpibcbs_indv_be_extr_meme_eff_dt

# Now we do a multi-join in PySpark for the MainLogic lookups:
df_mainlogic_base = df_AllMbrOutpChangesSubMbrs.alias("AllMbrOutpChangesSubMbrs") \
    .join(
       df_getCreateRunCyle.alias("getCreateRunCyle"),
       on=(F.col("AllMbrOutpChangesSubMbrs.ASG_INDV_BE_KEY") == F.col("getCreateRunCyle.INDV_BE_KEY")),
       how="left"
    ).join(
       df_hf_mpibcbs_indv_max_dtm_max_mbr.alias("maxPrcsDtmMaxMbrUniqKeyForIndvBe"),
       on=[
         F.col("AllMbrOutpChangesSubMbrs.ASG_INDV_BE_KEY") == F.col("maxPrcsDtmMaxMbrUniqKeyForIndvBe.ASG_INDV_BE_KEY"),
         F.col("AllMbrOutpChangesSubMbrs.PRCS_OWNER_ID") == F.col("maxPrcsDtmMaxMbrUniqKeyForIndvBe.PRCS_OWNER_ID"),
         F.col("AllMbrOutpChangesSubMbrs.SRC_SYS_ID") == F.col("maxPrcsDtmMaxMbrUniqKeyForIndvBe.SRC_SYS_ID"),
         F.col("AllMbrOutpChangesSubMbrs.PRCS_RUN_DTM") == F.col("maxPrcsDtmMaxMbrUniqKeyForIndvBe.PRCS_RUN_DTM"),
         F.col("AllMbrOutpChangesSubMbrs.MPI_MBR_ID") == F.col("maxPrcsDtmMaxMbrUniqKeyForIndvBe.MPI_MBR_ID")
       ],
       how="left"
    ).join(
       df_hf_mpibcbs_indv_max_dtm_max_bekey.alias("maxPrcsDtmForFacetsBeInput"),
       on=[
         F.col("AllMbrOutpChangesSubMbrs.MPI_MBR_ID") == F.col("maxPrcsDtmForFacetsBeInput.MPI_MBR_ID"),
         F.col("AllMbrOutpChangesSubMbrs.PRCS_OWNER_ID") == F.col("maxPrcsDtmForFacetsBeInput.PRCS_OWNER_ID"),
         F.col("AllMbrOutpChangesSubMbrs.SRC_SYS_ID") == F.col("maxPrcsDtmForFacetsBeInput.SRC_SYS_ID"),
         F.col("AllMbrOutpChangesSubMbrs.PRCS_RUN_DTM") == F.col("maxPrcsDtmForFacetsBeInput.PRCS_RUN_DTM"),
         F.col("AllMbrOutpChangesSubMbrs.ASG_INDV_BE_KEY") == F.col("maxPrcsDtmForFacetsBeInput.ASG_INDV_BE_KEY")
       ],
       how="left"
    ).join(
       df_chk_org_eff_dt.alias("chk_org_eff_dt"),
       on=(F.col("AllMbrOutpChangesSubMbrs.MPI_MBR_ID") == F.col("chk_org_eff_dt.MEME_CK")),
       how="left"
    ).join(
       df_ids_indv_be_key_tx.alias("Lookup"),
       on=[
         F.col("AllMbrOutpChangesSubMbrs.SUB_ID") == F.col("Lookup.SUB_ID"),
         F.col("AllMbrOutpChangesSubMbrs.MBR_SFX_NO") == F.col("Lookup.MBR_SFX_NO")
       ],
       how="left"
    )

df_mainlogic_vars = df_mainlogic_base \
    .withColumn(
       "svSubBekey",
       F.when(F.col("AllMbrOutpChangesSubMbrs.SUB_INDV_BE_KEY").isNull(), F.lit(0))
        .otherwise(F.col("AllMbrOutpChangesSubMbrs.SUB_INDV_BE_KEY"))
    ).withColumn(
       "svCreateRunCycle",
       F.when(F.col("getCreateRunCyle.INDV_BE_KEY").isNull(), F.col("RunCycle").cast(IntegerType()))
        .otherwise(F.col("getCreateRunCyle.CRT_RUN_CYC_EXCTN_SK"))
    ).withColumn(
       "svTestIfMbrOutPtoWriteToIndvBe",
       F.when(
         (F.col("maxPrcsDtmMaxMbrUniqKeyForIndvBe.ASG_INDV_BE_KEY").isNotNull()) &
         (F.col("AllMbrOutpChangesSubMbrs.ASG_INDV_BE_KEY") != F.lit(0)) &
         (F.col("AllMbrOutpChangesSubMbrs.MPI_MBR_ID") == F.col("maxPrcsDtmMaxMbrUniqKeyForIndvBe.MPI_MBR_ID")),
         F.lit("Y")
       ).otherwise(F.lit("N"))
    ).withColumn(
       "svFacetsBeKeyValue",
       trim(F.col("Lookup.INDV_BE_KEY_TX"))
    )

# -- The outputs from MainLogic:
#  1) Facets_File (constraint: IsNull(AllMbrOutpChangesSubMbrs.SUB_ID)=false 
#                                    AND IsNull(AllMbrOutpChangesSubMbrs.GRP_ID)=false 
#                                    AND IsNull(maxPrcsDtmForFacetsBeInput.MPI_MBR_ID)=true/false?? Actually the job says "IsNull(...) = @FALSE AND AllMbrOutpChangesSubMbrs.ASG_INDV_BE_KEY = maxPrcsDtmForFacetsBeInput.ASG_INDV_BE_KEY"
#     The constraint is: "IsNull(AllMbrOutpChangesSubMbrs.SUB_ID) = @FALSE 
#                        AND IsNull(AllMbrOutpChangesSubMbrs.GRP_ID) = @FALSE 
#                        AND IsNull(maxPrcsDtmForFacetsBeInput.MPI_MBR_ID) = @FALSE 
#                        AND AllMbrOutpChangesSubMbrs.ASG_INDV_BE_KEY = maxPrcsDtmForFacetsBeInput.ASG_INDV_BE_KEY"
df_Facets_File = df_mainlogic_vars.filter(
    (F.col("AllMbrOutpChangesSubMbrs.SUB_ID").isNotNull()) &
    (F.col("AllMbrOutpChangesSubMbrs.GRP_ID").isNotNull()) &
    (F.col("maxPrcsDtmForFacetsBeInput.MPI_MBR_ID").isNotNull()) &
    (F.col("AllMbrOutpChangesSubMbrs.ASG_INDV_BE_KEY") == F.col("maxPrcsDtmForFacetsBeInput.ASG_INDV_BE_KEY"))
).select(
    rpad(F.col("AllMbrOutpChangesSubMbrs.GRP_ID"),8," ").alias("GRGR_ID"),
    rpad(F.col("AllMbrOutpChangesSubMbrs.SUB_ID"),9," ").alias("SBSB_ID"),
    rpad(
        F.when(
            F.length(trim(F.col("AllMbrOutpChangesSubMbrs.MBR_SFX_NO"))) == 1,
            F.lit("0") + trim(F.col("AllMbrOutpChangesSubMbrs.MBR_SFX_NO"))
        ).otherwise(F.col("AllMbrOutpChangesSubMbrs.MBR_SFX_NO")),
        2," "
    ).alias("MEME_SFX"),
    rpad(F.expr("right(lpad(cast(AllMbrOutpChangesSubMbrs.ASG_INDV_BE_KEY as string),11,' '),11)"),11," ").alias("INDV_BE_KEY"),
    rpad(F.col("AllMbrOutpChangesSubMbrs.FIRST_NM"),15," ").alias("MEME_FIRST_NAME"),
    rpad(F.col("AllMbrOutpChangesSubMbrs.MIDINIT"),1," ").alias("MEME_MID_INIT"),
    rpad(F.col("AllMbrOutpChangesSubMbrs.LAST_NM"),35," ").alias("MEME_LAST_NAME"),
    rpad(F.lit(""),19," ").alias("FILLER")
)

write_files(
    df_Facets_File,
    f"{adls_path_publish}/external/facets_bekey_upd.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

#  2) INDV_BEKEY_pkey (constraint: svTestIfMbrOutPtoWriteToIndvBe='Y')
df_INDV_BEKEY_pkey = df_mainlogic_vars.filter(
    F.col("svTestIfMbrOutPtoWriteToIndvBe") == F.lit("Y")
).select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(F.lit("U"),10," ").alias("INSRT_UPDT_CD"),
    rpad(F.lit("N"),1," ").alias("DISCARD_IN"),
    rpad(F.lit("Y"),1," ").alias("PASS_THRU_IN"),
    # FIRST_RECYC_DT -> "CurrDate"
    rpad(F.lit(CurrDate),10," ").alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.concat(
      F.col("AllMbrOutpChangesSubMbrs.MPI_MBR_ID"), F.lit(";"),
      F.col("AllMbrOutpChangesSubMbrs.SRC_SYS_ID"), F.lit(";"),
      F.col("AllMbrOutpChangesSubMbrs.PRCS_OWNER_SRC_SYS_ENVRN_CK").cast(StringType()), F.lit(";"),
      F.col("AllMbrOutpChangesSubMbrs.PRCS_RUN_DTM").cast(StringType())
    ).alias("PRI_KEY_STRING"),
    F.col("AllMbrOutpChangesSubMbrs.ASG_INDV_BE_KEY").alias("INDV_BE_KEY"),
    F.col("svCreateRunCycle").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(F.col("maxPrcsDtmMaxMbrUniqKeyForIndvBe.COV_STTUS"),1," ").alias("COV_STTUS"),
    rpad(F.col("AllMbrOutpChangesSubMbrs.MBR_GNDR_CD"),1," ").alias("INDV_BE_GNDR_CD"),
    F.col("AllMbrOutpChangesSubMbrs.SRC_SYS_ID").alias("SRC_SYS_CD"),
    rpad(F.lit("Y"),1," ").alias("MBR_IN"),
    rpad(F.substring(F.col("AllMbrOutpChangesSubMbrs.BRTH_DT"),1,10),10," ").alias("BRTH_DT_SK"),
    rpad(F.substring(F.col("AllMbrOutpChangesSubMbrs.MBR_DCSD_DT"),1,10),10," ").alias("DCSD_DT"),
    rpad(
      F.when(
        F.col("chk_org_eff_dt.MEME_ORIG_EFF_DT").isNull(),
        F.lit("1753-01-01")
      ).otherwise(
        # We will treat FORMAT.DATE(...) as a user-defined function or just take substring of the timestamp
        F.substring(F.col("chk_org_eff_dt.MEME_ORIG_EFF_DT").cast(StringType()),1,10)
      ),
      10, " "
    ).alias("ORIG_EFF_DT_SK"),
    F.col("AllMbrOutpChangesSubMbrs.FIRST_NM").alias("FIRST_NM"),
    F.col("AllMbrOutpChangesSubMbrs.MIDINIT").alias("MIDINIT"),
    F.col("AllMbrOutpChangesSubMbrs.LAST_NM").alias("LAST_NM"),
    F.col("AllMbrOutpChangesSubMbrs.SSN").alias("SSN")
)

write_files(
    df_INDV_BEKEY_pkey,
    f"{adls_path}/key/MpiBcbsIndvBeKey.MpiIndvBeKey.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

#  3) B_INDV_BE (constraint: svTestIfMbrOutPtoWriteToIndvBe='Y'), only 1 column: INDV_BE_KEY
df_B_INDV_BE = df_mainlogic_vars.filter(
    F.col("svTestIfMbrOutPtoWriteToIndvBe") == F.lit("Y")
).select(
    F.col("AllMbrOutpChangesSubMbrs.ASG_INDV_BE_KEY").alias("INDV_BE_KEY")
)
write_files(
    df_B_INDV_BE,
    f"{adls_path}/load/B_INDV_BE.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

#  4) P_MBR_BE_KEY_XREF_load (constraint: AllMbrOutpChangesSubMbrs.ASG_INDV_BE_KEY <> 0)
df_P_MBR_BE_KEY_XREF_load = df_mainlogic_vars.filter(
    (F.col("AllMbrOutpChangesSubMbrs.ASG_INDV_BE_KEY") != 0)
).select(
    F.col("AllMbrOutpChangesSubMbrs.SRC_SYS_ID").alias("SRC_SYS_CD"),
    F.col("AllMbrOutpChangesSubMbrs.MPI_MBR_ID").cast(IntegerType()).alias("MBR_UNIQ_KEY"),
    F.col("AllMbrOutpChangesSubMbrs.ASG_INDV_BE_KEY").alias("INDV_BE_KEY"),
    F.col("svSubBekey").alias("SUB_INDV_BE_KEY"),
    F.col("AllMbrOutpChangesSubMbrs.MPI_SUB_ID").alias("SUB_UNIQ_KEY"),
    rpad(F.lit(CurrDate),10," ").alias("LAST_UPDT_DT_SK")
)

# ---------------------------------------------
# STAGE: hf_mpibcbs_indv_pmbrbekey_outp (Scenario A)
# We deduplicate on "SRC_SYS_CD, MBR_UNIQ_KEY" etc.
# Then we have 2 outputs: "MainChange" -> Collect, "OtherChange_lkup" -> OtherChngLkup
# ---------------------------------------------
df_hf_mpibcbs_indv_pmbrbekey_outp = dedup_sort(
    df_P_MBR_BE_KEY_XREF_load,
    partition_cols=["SRC_SYS_CD","MBR_UNIQ_KEY"],
    sort_cols=[]
)

df_MainChange = df_hf_mpibcbs_indv_pmbrbekey_outp
df_OtherChange_lkup = df_hf_mpibcbs_indv_pmbrbekey_outp

# ---------------------------------------------
# STAGE: hf_mpibcbs_indv_pmbrbekey_chng (Scenario A)
# constraint: "AllMbrOutpChangesSubMbrs.MBR_RELSHP_CD = 'SUBSCRIBER'"
# output => "AsgIndvBeKey_lkup"
# ---------------------------------------------
df_hf_mpibcbs_indv_pmbrbekey_chng = df_mainlogic_vars.filter(
    F.col("AllMbrOutpChangesSubMbrs.MBR_RELSHP_CD") == F.lit("SUBSCRIBER")
).select(
    F.col("AllMbrOutpChangesSubMbrs.MPI_SUB_ID").alias("MPI_SUB_ID"),
    F.col("AllMbrOutpChangesSubMbrs.MPI_MBR_ID").alias("MPI_MBR_ID"),
    F.col("AllMbrOutpChangesSubMbrs.MBR_RELSHP_CD").alias("MBR_RELSHP_CD"),
    F.col("AllMbrOutpChangesSubMbrs.ASG_INDV_BE_KEY").alias("ASG_INDV_BE_KEY")
)

df_hf_mpibcbs_indv_pmbrbekey_chng = dedup_sort(
    df_hf_mpibcbs_indv_pmbrbekey_chng,
    partition_cols=["MPI_SUB_ID"],
    sort_cols=[]
)

df_AsgIndvBeKey_lkup = df_hf_mpibcbs_indv_pmbrbekey_chng

# ---------------------------------------------
# STAGE: IDS_PMbrBekeyRecsToUpdt (DB2Connector, read) -> AsgIndvBeKeyLkup
# ---------------------------------------------
# We'll do another <...> for the JDBC config because it's IDS again:
jdbc_url_IDS2, jdbc_props_IDS2 = get_db_config(ids_secret_name)
df_IDS_PMbrBekeyRecsToUpdt_raw = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS2)
    .options(**jdbc_props_IDS2)
    .option("query", f"""
SELECT xref.SRC_SYS_CD as SRC_SYS_CD,
       xref.SUB_UNIQ_KEY as SUB_UNIQ_KEY,
       xref.MBR_UNIQ_KEY as MBR_UNIQ_KEY,
       xref.SUB_INDV_BE_KEY as SUB_INDV_BE_KEY,
       xref.INDV_BE_KEY as INDV_BE_KEY,
       map.TRGT_CD as RELATIONSHIP_CD
FROM {IDSOwnerParam}.P_MBR_BE_KEY_XREF xref,
     {IDSOwnerParam}.MBR mbr,
     {IDSOwnerParam}.CD_MPPNG map
WHERE xref.MBR_UNIQ_KEY = mbr.MBR_UNIQ_KEY
  AND mbr.MBR_RELSHP_CD_SK = map.CD_MPPNG_SK
  AND map.TRGT_CD <> 'SUB'
""")
    .load()
)
df_IDS_PMbrBekeyRecsToUpdt = df_IDS_PMbrBekeyRecsToUpdt_raw

# ---------------------------------------------
# STAGE: hf_mpibcbs_indv_pmbrbekey_chng -> AsgIndvBeKey_lkup (lookup in "AsgIndvBeKeyLkup")
# ---------------------------------------------
# "AsgIndvBeKeyLkup" has PrimaryLink "IDS_PMbrBekeyRecsToUpdt" => df_IDS_PMbrBekeyRecsToUpdt
# and LookupLink => df_AsgIndvBeKey_lkup on SUB_UNIQ_KEY = MPI_SUB_ID
df_AsgIndvBeKeyLkup_base = df_IDS_PMbrBekeyRecsToUpdt.alias("Extract").join(
    df_AsgIndvBeKey_lkup.alias("AsgIndvBeKey_lkup"),
    on=(F.col("Extract.SUB_UNIQ_KEY") == F.col("AsgIndvBeKey_lkup.MPI_SUB_ID")),
    how="left"
)

df_AsgIndvBeKeyLkup_out = df_AsgIndvBeKeyLkup_base.filter(
    F.col("AsgIndvBeKey_lkup.MPI_SUB_ID").isNotNull()
).select(
    F.col("Extract.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Extract.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("Extract.INDV_BE_KEY").alias("INDV_BE_KEY"),
    F.col("AsgIndvBeKey_lkup.ASG_INDV_BE_KEY").alias("SUB_INDV_BE_KEY"),
    F.col("Extract.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    rpad(F.lit(CurrDate),10," ").alias("LAST_UPDT_DT_SK")
)

# ---------------------------------------------
# STAGE: hf_mpibcbs_indv_pmbrbekey_sub (Scenario A)
# Output => "out" -> OtherChngLkup
# ---------------------------------------------
df_hf_mpibcbs_indv_pmbrbekey_sub = dedup_sort(
    df_AsgIndvBeKeyLkup_out,
    partition_cols=["SRC_SYS_CD","MBR_UNIQ_KEY"],
    sort_cols=[]
)

df_out = df_hf_mpibcbs_indv_pmbrbekey_sub

# ---------------------------------------------
# STAGE: OtherChngLkup (Transformer)
# PrimaryLink => df_out
# LookupLink => df_OtherChange_lkup on (out.SRC_SYS_CD=OtherChange_lkup.SRC_SYS_CD and out.MBR_UNIQ_KEY=OtherChange_lkup.MBR_UNIQ_KEY)
# Output => "Change" => hf_mpibcbs_indv_pmbrbekey_sub_2
# Constraint => IsNull(OtherChange_lkup.INDV_BE_KEY) = @TRUE
# ---------------------------------------------
df_other_chng_lkup_join = df_out.alias("out").join(
    df_OtherChange_lkup.alias("OtherChange_lkup"),
    on=[
      F.col("out.SRC_SYS_CD") == F.col("OtherChange_lkup.SRC_SYS_CD"),
      F.col("out.MBR_UNIQ_KEY") == F.col("OtherChange_lkup.MBR_UNIQ_KEY")
    ],
    how="left"
)
df_change = df_other_chng_lkup_join.filter(
    F.col("OtherChange_lkup.INDV_BE_KEY").isNull()
).select(
    F.col("out.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("out.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("out.INDV_BE_KEY").alias("INDV_BE_KEY"),
    F.col("out.SUB_INDV_BE_KEY").alias("SUB_INDV_BE_KEY"),
    F.col("out.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.col("out.LAST_UPDT_DT_SK").alias("LAST_UPDT_DT_SK")
)

# ---------------------------------------------
# STAGE: hf_mpibcbs_indv_pmbrbekey_sub_2 (Scenario A)
# Output => "OtherChange" => Collect
# ---------------------------------------------
df_hf_mpibcbs_indv_pmbrbekey_sub_2 = dedup_sort(
    df_change,
    partition_cols=["SRC_SYS_CD","MBR_UNIQ_KEY"],
    sort_cols=[]
)

df_OtherChange = df_hf_mpibcbs_indv_pmbrbekey_sub_2

# ---------------------------------------------
# STAGE: Collect (CCollector)
# InputPins:
#   - MainChange => df_MainChange
#   - OtherChange => df_OtherChange
# Output => "Combined" => Balancing_Split
# Round-Robin union
# ---------------------------------------------
common_cols_collect = [
   "SRC_SYS_CD",
   "MBR_UNIQ_KEY",
   "INDV_BE_KEY",
   "SUB_INDV_BE_KEY",
   "SUB_UNIQ_KEY",
   "LAST_UPDT_DT_SK"
]
df_collected = df_MainChange.select(common_cols_collect).unionByName(
    df_OtherChange.select(common_cols_collect),
    allowMissingColumns=True
)

# ---------------------------------------------
# STAGE: Balancing_Split (Transformer)
# Input => df_collected (alias "Combined")
# Outputs => 
#   P_MBR_BE_KEY_XREF_load => P_MBR_BE_KEY_XREF
#   P_MBR_BE_KEY_XREF_bal  => ...
# ---------------------------------------------
df_bal_splt = df_collected.alias("Combined")

df_P_MBR_BE_KEY_XREF_load_2 = df_bal_splt.select(
    F.col("Combined.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Combined.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("Combined.INDV_BE_KEY").alias("INDV_BE_KEY"),
    F.col("Combined.SUB_INDV_BE_KEY").alias("SUB_INDV_BE_KEY"),
    F.col("Combined.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.col("Combined.LAST_UPDT_DT_SK").alias("LAST_UPDT_DT_SK")
)
df_P_MBR_BE_KEY_XREF_bal = df_bal_splt.select(
    F.col("Combined.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Combined.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("Combined.INDV_BE_KEY").alias("INDV_BE_KEY"),
    F.col("Combined.SUB_INDV_BE_KEY").alias("SUB_INDV_BE_KEY"),
    F.col("Combined.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.col("Combined.LAST_UPDT_DT_SK").alias("LAST_UPDT_DT_SK")
)

# ---------------------------------------------
# STAGE: P_MBR_BE_KEY_XREF_bal (CSeqFileStage write)
# ---------------------------------------------
write_files(
    df_P_MBR_BE_KEY_XREF_bal,
    f"{adls_path}/load/B_P_MBR_BE_KEY_XREF.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# ---------------------------------------------
# STAGE: P_MBR_BE_KEY_XREF (CSeqFileStage write)
# ---------------------------------------------
write_files(
    df_P_MBR_BE_KEY_XREF_load_2,
    f"{adls_path}/load/P_MBR_BE_KEY_XREF.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# ---------------------------------------------
# Final Writes already done for other SeqFiles.
# ---------------------------------------------