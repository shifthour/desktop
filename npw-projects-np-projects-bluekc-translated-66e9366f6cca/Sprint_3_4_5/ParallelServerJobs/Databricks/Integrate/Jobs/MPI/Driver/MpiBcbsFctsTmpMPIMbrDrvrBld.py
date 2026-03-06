# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2012 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC      
# MAGIC                       
# MAGIC PROCESSING:  Member Unique IDs in MBR_OUTP are used loaded to the facets temp table tmp...TMP_MPI_IDS_MBR_DRVR for efficiency 
# MAGIC                  
# MAGIC   
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                                                                                   Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                                                                                           --------------------------------       -------------------------------   ----------------------------       
# MAGIC Sharon Andrew      2012-10-15        4426 mpi                  created                                                                                                                                                     Integrate New Devl       Bhoomi Dasari            10/16/2012
# MAGIC Abhiram D              2012-12-10        4426 MPI	      Changed the SQL on the ChangedMbrs and ChangedSubs in the                      		          IntegrateNewDevl         Bhoomi Dasari            12/10/2012
# MAGIC 				                      MPI BCBSEXT table, added  "AND tblSRC_SYS_XWALK.ENVRN_ID = '#ProcessOwner#'"	
# MAGIC SAmdrew               2015-08-15        5318                        Removed 		 AND tblSRC_SYS_XWALK.ENVRN_ID = '#ProcessOwner#'                         IntegrateDev2                 Kalyan Neelam           2015-10-16
# MAGIC 				                     removed parameter ProcessOwner       
# MAGIC 
# MAGIC 
# MAGIC Prabhu ES             2022-03-30        S2S                       MSSQL ODBC conn params added                                                                                                             IntegrateDev5

# MAGIC backup of what is loaded to the facets temp table TMP_MPI_MBR_DRVR
# MAGIC used in  MpiBcbsIndvBeExtr.
# MAGIC pull where changed and MBR_OUTP.ASG_INDV_BE_KEY <>0
# MAGIC This exact code is in the MPI_BCBS.MBR_OUTP extract into the IDS.INDV_BE program.
# MAGIC 
# MAGIC /MPI/IndvBe/MpiBcbsIndvBeExtr
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


TmpTblRunID = get_widget_value("TmpTblRunID", "")
PrevPrcRunDtm = get_widget_value("PrevPrcRunDtm", "")
FacetsOwner = get_widget_value("FacetsOwner", "")
MPI_BCBSOwner = get_widget_value("MPI_BCBSOwner", "")
mpi_bcbsover_secret_name = get_widget_value("mpi_bcbsover_secret_name", "")
MPI_BCBSEnvrnId = get_widget_value("MPI_BCBSEnvrnId", "")
CurrDtm = get_widget_value("CurrDtm", "")
tempdb_secret_name = get_widget_value("tempdb_secret_name", "")

jdbc_url_mpi_bcbsover, jdbc_props_mpi_bcbsover = get_db_config(mpi_bcbsover_secret_name)

query_changedsubnotmbrs = f"""
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
tblMBR_OUTP.PREV_INDV_BE_KEY, 
ISNULL(tblSUB_OUTP.ASG_INDV_BE_KEY, ISNULL(tblMBR_OUTP.SUB_INDV_BE_KEY, 0)), 
tblMBR_OUTP.MPI_SUB_ID, 
tblMBR_OUTP.GRP_EDI_VNDR_NM,
tblMBR_OUTP.CLNT_ID, 
tblMBR_OUTP.MBR_DCSD_DT, 
tblMBR_OUTP.ASG_INDV_BE_KEY
FROM {MPI_BCBSOwner}.MBR_OUTP tblMBR_OUTP,
     {MPI_BCBSOwner}.MBR_OUTP tblSUB_OUTP,
     {MPI_BCBSOwner}.PRCS_OWNER_SRC_SYS_ENVRN_CRSWALK tblSRC_SYS_XWALK
WHERE tblMBR_OUTP.MPI_SUB_ID = tblSUB_OUTP.MPI_SUB_ID
  AND tblSUB_OUTP.MBR_RELSHP_CD = 'SUBSCRIBER'
  AND tblSUB_OUTP.PRCS_RUN_DTM > '{PrevPrcRunDtm}'
  AND tblMBR_OUTP.PRCS_OWNER_SRC_SYS_ENVRN_CK = tblSUB_OUTP.PRCS_OWNER_SRC_SYS_ENVRN_CK
  AND tblMBR_OUTP.PRCS_OWNER_SRC_SYS_ENVRN_CK = tblSRC_SYS_XWALK.PRCS_OWNER_SRC_SYS_ENVRN_CK
  AND tblMBR_OUTP.PRCS_RUN_DTM < '{CurrDtm}'
  AND tblMBR_OUTP.MBR_RELSHP_CD <> 'SUBSCRIBER'
  AND tblMBR_OUTP.MPI_MBR_ID NOT IN
    (SELECT MPI_MBR_ID 
     FROM {MPI_BCBSOwner}.MBR_OUTP AS OUTP
     WHERE OUTP.MPI_NTFCTN_IN = 'Y'
       AND OUTP.SSN = '000000000'
       AND OUTP.BRTH_DT = '1753-01-01'
       AND OUTP.MPI_SUB_ID = 0)
"""

df_ChangedSubNotMbrs_raw = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_mpi_bcbsover)
    .options(**jdbc_props_mpi_bcbsover)
    .option("query", query_changedsubnotmbrs)
    .load()
)

df_ChangedSubNotMbrs = df_ChangedSubNotMbrs_raw.select(
    F.col("MPI_MBR_ID"),
    F.col("PRCS_OWNER_SRC_SYS_ENVRN_CK"),
    F.col("SRC_SYS_ID"),
    F.col("PRCS_OWNER_ID"),
    F.col("PRCS_RUN_DTM"),
    F.col("SSN"),
    F.col("BRTH_DT"),
    F.col("FIRST_NM"),
    F.col("MIDINIT"),
    F.col("LAST_NM"),
    F.col("NM_PFX"),
    F.col("NM_SFX"),
    F.col("MBR_GNDR_CD"),
    F.col("SUB_ID"),
    F.col("MBR_SFX_NO"),
    F.col("ADDR_LN_1"),
    F.col("ADDR_LN_2"),
    F.col("ADDR_LN_3"),
    F.col("CITY_NM"),
    F.col("ST_CD"),
    F.col("POSTAL_CD"),
    F.col("CTRY_CD"),
    F.col("MBR_HOME_PHN_NO"),
    F.col("MBR_MOBL_PHN_NO"),
    F.col("MBR_WORK_PHN_NO"),
    F.col("EMAIL_ADDR_TX_1"),
    F.col("EMAIL_ADDR_TX_2"),
    F.col("EMAIL_ADDR_TX_3"),
    F.col("MBR_RELSHP_CD"),
    F.col("GRP_ID"),
    F.col("GRP_NM"),
    F.col("PREV_INDV_BE_KEY"),
    F.col("(ISNULL(tblSUB_OUTP.ASG_INDV_BE_KEY, ISNULL(tblMBR_OUTP.SUB_INDV_BE_KEY, 0)))").alias("SUB_INDV_BE_KEY"),
    F.col("MPI_SUB_ID"),
    F.col("GRP_EDI_VNDR_NM"),
    F.col("CLNT_ID"),
    F.col("MBR_DCSD_DT"),
    F.col("ASG_INDV_BE_KEY"),
)

query_chngdpeople = f"""
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
tblMBR_OUTP.PREV_INDV_BE_KEY, 
ISNULL(tblSUB_OUTP.ASG_INDV_BE_KEY, ISNULL(tblMBR_OUTP.SUB_INDV_BE_KEY, 0)), 
tblMBR_OUTP.MPI_SUB_ID, 
tblMBR_OUTP.GRP_EDI_VNDR_NM,
tblMBR_OUTP.CLNT_ID, 
tblMBR_OUTP.MBR_DCSD_DT, 
tblMBR_OUTP.ASG_INDV_BE_KEY
FROM {MPI_BCBSOwner}.MBR_OUTP tblMBR_OUTP 
     LEFT OUTER JOIN {MPI_BCBSOwner}.MBR_OUTP tblSUB_OUTP
       ON (tblMBR_OUTP.MPI_SUB_ID = tblSUB_OUTP.MPI_SUB_ID
           AND tblMBR_OUTP.PRCS_OWNER_SRC_SYS_ENVRN_CK = tblSUB_OUTP.PRCS_OWNER_SRC_SYS_ENVRN_CK
           AND tblSUB_OUTP.MBR_RELSHP_CD = 'SUBSCRIBER'),
     {MPI_BCBSOwner}.PRCS_OWNER_SRC_SYS_ENVRN_CRSWALK tblSRC_SYS_XWALK
WHERE tblMBR_OUTP.PRCS_OWNER_SRC_SYS_ENVRN_CK = tblSRC_SYS_XWALK.PRCS_OWNER_SRC_SYS_ENVRN_CK
  AND tblMBR_OUTP.PRCS_RUN_DTM > '{PrevPrcRunDtm}'
  AND tblMBR_OUTP.PRCS_OWNER_SRC_SYS_ENVRN_CK = tblSRC_SYS_XWALK.PRCS_OWNER_SRC_SYS_ENVRN_CK
  AND tblMBR_OUTP.PRCS_RUN_DTM < '{CurrDtm}'
  AND tblMBR_OUTP.MPI_MBR_ID NOT IN
    (SELECT MPI_MBR_ID
     FROM {MPI_BCBSOwner}.MBR_OUTP AS OUTP
     WHERE OUTP.MPI_NTFCTN_IN = 'Y'
       AND OUTP.SSN = '000000000'
       AND OUTP.BRTH_DT = '1753-01-01'
       AND OUTP.MPI_SUB_ID = 0)
"""

df_ChngdPeople_raw = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_mpi_bcbsover)
    .options(**jdbc_props_mpi_bcbsover)
    .option("query", query_chngdpeople)
    .load()
)

df_ChngdPeople = df_ChngdPeople_raw.select(
    F.col("MPI_MBR_ID"),
    F.col("PRCS_OWNER_SRC_SYS_ENVRN_CK"),
    F.col("SRC_SYS_ID"),
    F.col("PRCS_OWNER_ID"),
    F.col("PRCS_RUN_DTM"),
    F.col("SSN"),
    F.col("BRTH_DT"),
    F.col("FIRST_NM"),
    F.col("MIDINIT"),
    F.col("LAST_NM"),
    F.col("NM_PFX"),
    F.col("NM_SFX"),
    F.col("MBR_GNDR_CD"),
    F.col("SUB_ID"),
    F.col("MBR_SFX_NO"),
    F.col("ADDR_LN_1"),
    F.col("ADDR_LN_2"),
    F.col("ADDR_LN_3"),
    F.col("CITY_NM"),
    F.col("ST_CD"),
    F.col("POSTAL_CD"),
    F.col("CTRY_CD"),
    F.col("MBR_HOME_PHN_NO"),
    F.col("MBR_MOBL_PHN_NO"),
    F.col("MBR_WORK_PHN_NO"),
    F.col("EMAIL_ADDR_TX_1"),
    F.col("EMAIL_ADDR_TX_2"),
    F.col("EMAIL_ADDR_TX_3"),
    F.col("MBR_RELSHP_CD"),
    F.col("GRP_ID"),
    F.col("GRP_NM"),
    F.col("PREV_INDV_BE_KEY"),
    F.col("(ISNULL(tblSUB_OUTP.ASG_INDV_BE_KEY, ISNULL(tblMBR_OUTP.SUB_INDV_BE_KEY, 0)))").alias("SUB_INDV_BE_KEY"),
    F.col("MPI_SUB_ID"),
    F.col("GRP_EDI_VNDR_NM"),
    F.col("CLNT_ID"),
    F.col("MBR_DCSD_DT"),
    F.col("ASG_INDV_BE_KEY"),
)

df_hf_mpibcbs_indv_be_changed_people = df_ChangedSubNotMbrs.dropDuplicates(
    ["MPI_MBR_ID", "PRCS_OWNER_SRC_SYS_ENVRN_CK", "SRC_SYS_ID", "PRCS_OWNER_ID", "PRCS_RUN_DTM"]
)
df_mbrs_only = df_hf_mpibcbs_indv_be_changed_people

df_hf_mpibcbs_indv_be_changed_sub = df_ChngdPeople.dropDuplicates(
    ["MPI_MBR_ID", "PRCS_OWNER_SRC_SYS_ENVRN_CK", "SRC_SYS_ID", "PRCS_OWNER_ID", "PRCS_RUN_DTM"]
)
df_sub_mbrs = df_hf_mpibcbs_indv_be_changed_sub

df_merged = df_sub_mbrs.unionByName(df_mbrs_only)

df_hf_mpibcbs_indv_be_changed_all = df_merged.dropDuplicates(
    ["MPI_MBR_ID", "PRCS_OWNER_SRC_SYS_ENVRN_CK", "SRC_SYS_ID", "PRCS_OWNER_ID", "PRCS_RUN_DTM"]
)
df_MbrOutp = df_hf_mpibcbs_indv_be_changed_all

df_backup = df_MbrOutp.select(
    F.col("MPI_MBR_ID").alias("MPI_MBR_ID"),
    F.col("PRCS_OWNER_SRC_SYS_ENVRN_CK").alias("PRCS_OWNER_SRC_SYS_ENVRN_CK"),
    F.col("SRC_SYS_ID").alias("SRC_SYS_ID"),
    F.col("CLNT_ID").alias("CLNT_ID"),
    F.col("PRCS_RUN_DTM").alias("PRCS_RUN_DTM"),
    F.rpad(F.col("MBR_SFX_NO"), 2, " ").alias("MBR_SFX_NO"),
    F.col("SUB_INDV_BE_KEY").alias("SUB_INDV_BE_KEY"),
    F.col("MPI_SUB_ID").alias("MPI_SUB_ID"),
    F.col("ASG_INDV_BE_KEY").alias("ASG_INDV_BE_KEY"),
)

df_distinct_mbr_to_tmp_mbr_drvr = df_MbrOutp.select(
    F.col("MPI_MBR_ID").alias("MEME_CK")
)

write_files(
    df_backup,
    f"{adls_path_publish}/external/FctsTmpMPIMbrDrvrTable.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

jdbc_url_tempdb, jdbc_props_tempdb = get_db_config(tempdb_secret_name)

temp_table_name = "tempdb.MpiBcbsFctsTmpMPIMbrDrvrBld_TMP_MPI_MBR_DRVR_temp"
target_table_name = "tempdb.TMP_MPI_MBR_DRVR"

execute_dml(f"DROP TABLE IF EXISTS {temp_table_name}", jdbc_url_tempdb, jdbc_props_tempdb)

(
    df_distinct_mbr_to_tmp_mbr_drvr
    .write
    .format("jdbc")
    .option("url", jdbc_url_tempdb)
    .options(**jdbc_props_tempdb)
    .option("dbtable", temp_table_name)
    .mode("overwrite")
    .save()
)

merge_sql = f"""
MERGE {target_table_name} AS T
USING {temp_table_name} AS S
ON T.MEME_CK = S.MEME_CK
WHEN MATCHED THEN
  UPDATE SET T.MEME_CK = S.MEME_CK
WHEN NOT MATCHED THEN
  INSERT (MEME_CK)
  VALUES (S.MEME_CK);
"""

execute_dml(merge_sql, jdbc_url_tempdb, jdbc_props_tempdb)