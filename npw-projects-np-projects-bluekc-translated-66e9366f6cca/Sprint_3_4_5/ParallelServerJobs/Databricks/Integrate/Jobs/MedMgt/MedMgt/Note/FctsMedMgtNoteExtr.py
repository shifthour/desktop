# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2006, 2007, 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:     FctsMedMgtExtrSeq
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:  Pulls data from CMC_NTNB_NOTE_BASE to a landing file for the IDS
# MAGIC       
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                    Project/Altiris #                     Change Description                                                       Development Project       Code Reviewer              Date Reviewed
# MAGIC ----------------------------------      -------------------      -----------------------------------           ---------------------------------------------------------                                 ----------------------------------      ----------------------------           -------------------------
# MAGIC Suzanne Saylor               2006-02-14                                                      Original Programming.
# MAGIC Suzanne Saylor               2006-05-23                                                      Added fix for care guide note issue and orphan record issue. 
# MAGIC Parik                               04/09/2007              3264                            Added Balancing process to the overall job that takes           devlIDS30                      Steph Goddard               9/14/07       
# MAGIC                                                                                                               a snapshot of the source data         
# MAGIC Ralph Tucker                 2008-06-23        3657 Primary Key                  Changed primary key from hash file to DB2 table                devlIDS                            Steph Goddard               07/03/2008
# MAGIC Prabhu ES                     2022-03-07         S2S Remediation                  MSSQL ODBC conn params added                                   IntegrateDev5               Manasa Andru                 2022-06-14

# MAGIC Container hash file cleared by after-job subroutine
# MAGIC Strip Carriage Return, Line Feed, and  Tab
# MAGIC 
# MAGIC Use STRIP.FIELD function on each character column coming in
# MAGIC Extract Facets Data
# MAGIC Writing Sequential File to ../key
# MAGIC Apply business logic.
# MAGIC Balancing snapshot of source table
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
TmpOutFile = get_widget_value('TmpOutFile','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
BeginDate = get_widget_value('BeginDate','')
EndDate = get_widget_value('EndDate','')
RunID = get_widget_value('RunID','')
CurrDate = get_widget_value('CurrDate','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
SrcSysCd = get_widget_value('SrcSysCd','')

jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)

extract_query_facets = f"""
SELECT 
NOTE.NTNB_ID,
NOTE.NTNB_INPUT_DTM,
NOTE.NTNB_INPUT_USID,
NOTE.NTNB_UPD_USID,
NOTE.NTNB_CAT,
NOTE.NTNB_MCTR_SUBJ,
NOTE.NTNB_UPD_DTM,
CNTCT.NTCT_NAME,
CNTCT.NTCT_PHONE,
CNTCT.NTCT_PHONE_EXT,
CNTCT.NTCT_FAX,
CNTCT.NTCT_FAX_EXT,
NOTE.NTNB_SUMMARY

FROM {FacetsOwner}.CMC_UMUM_UTIL_MGT UMUM , 
{FacetsOwner}.CMC_NTNB_NOTE_BASE NOTE 
LEFT OUTER JOIN {FacetsOwner}.CMC_NTCT_CONTACT CNTCT
    ON NOTE.NTNB_ID = CNTCT.NTNB_ID
    AND NOTE.NTNB_INPUT_DTM = CNTCT.NTNB_INPUT_DTM

WHERE 
NOTE.NTNB_ID = UMUM.NTNB_ID 
AND NOTE.NTNB_INPUT_DTM >= '{BeginDate}' 
AND NOTE.NTNB_INPUT_DTM < '{EndDate}'
AND NOT( NOTE.NTNB_ID = '1753-01-01' AND NOTE.NTNB_INPUT_DTM <> '1753-01-01')

UNION

SELECT 
NOTE.NTNB_ID,
NOTE.NTNB_INPUT_DTM,
NOTE.NTNB_INPUT_USID,
NOTE.NTNB_UPD_USID,
NOTE.NTNB_CAT,
NOTE.NTNB_MCTR_SUBJ,
NOTE.NTNB_UPD_DTM,
CNTCT.NTCT_NAME,
CNTCT.NTCT_PHONE,
CNTCT.NTCT_PHONE_EXT,
CNTCT.NTCT_FAX,
CNTCT.NTCT_FAX_EXT,
NOTE.NTNB_SUMMARY

FROM {FacetsOwner}.CMC_CMCM_CASE_MGT CMCM, 
{FacetsOwner}.CMC_NTNB_NOTE_BASE NOTE 
LEFT OUTER JOIN {FacetsOwner}.CMC_NTCT_CONTACT CNTCT
    ON NOTE.NTNB_ID = CNTCT.NTNB_ID
    AND NOTE.NTNB_INPUT_DTM = CNTCT.NTNB_INPUT_DTM

WHERE 
NOTE.NTNB_ID = CMCM.NTNB_ID
AND NOTE.NTNB_INPUT_DTM >= '{BeginDate}'
AND NOTE.NTNB_INPUT_DTM < '{EndDate}'
AND NOT( NOTE.NTNB_ID = '1753-01-01' AND NOTE.NTNB_INPUT_DTM <> '1753-01-01')
"""

df_FACETS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_facets)
    .load()
)

df_StripField = (
    df_FACETS
    .withColumn("NTNB_ID", F.col("NTNB_ID"))
    .withColumn("NTNB_INPUT_DTM", F.col("NTNB_INPUT_DTM"))
    .withColumn("NTNB_INPUT_USID", trim(strip_field(F.col("NTNB_INPUT_USID"))))
    .withColumn("NTNB_UPD_USID", trim(strip_field(F.col("NTNB_UPD_USID"))))
    .withColumn("NTNB_CAT", trim(strip_field(F.col("NTNB_CAT"))))
    .withColumn("NTNB_MCTR_SUBJ", trim(strip_field(F.col("NTNB_MCTR_SUBJ"))))
    .withColumn("NTNB_UPD_DTM", F.col("NTNB_UPD_DTM"))
    .withColumn("NTCT_NAME", trim(strip_field(F.col("NTCT_NAME"))))
    .withColumn("NTCT_PHONE", trim(strip_field(F.col("NTCT_PHONE"))))
    .withColumn("NTCT_PHONE_EXT", trim(strip_field(F.col("NTCT_PHONE_EXT"))))
    .withColumn("NTCT_FAX", trim(strip_field(F.col("NTCT_FAX"))))
    .withColumn("NTCT_FAX_EXT", trim(strip_field(F.col("NTCT_FAX_EXT"))))
    .withColumn("NTNB_SUMMARY", trim(strip_field(F.col("NTNB_SUMMARY"))))
)

df_BusinessRules_stagevars = (
    df_StripField
    .withColumn("RowPassThru", F.lit("Y"))
    .withColumn(
        "svMedMgtNoteDtm",
        F.concat(
            F.substring(F.col("NTNB_ID"), 1, 10), F.lit(" "),
            F.substring(F.col("NTNB_ID"), 12, 2), F.lit(":"),
            F.substring(F.col("NTNB_ID"), 15, 2), F.lit(":"),
            F.substring(F.col("NTNB_ID"), 18, 2), F.lit("."),
            F.substring(F.col("NTNB_ID"), 21, 6)
        )
    )
    .withColumn(
        "svMedMgtNoteInptDtm",
        F.concat(
            F.substring(F.col("NTNB_INPUT_DTM"), 1, 10), F.lit(" "),
            F.substring(F.col("NTNB_INPUT_DTM"), 12, 2), F.lit(":"),
            F.substring(F.col("NTNB_INPUT_DTM"), 15, 2), F.lit(":"),
            F.substring(F.col("NTNB_INPUT_DTM"), 18, 2), F.lit("."),
            F.substring(F.col("NTNB_INPUT_DTM"), 21, 6)
        )
    )
)

df_BusinessRules = (
    df_BusinessRules_stagevars
    .withColumn("SRC_SYS_CD_SK", F.lit(SrcSysCdSk))
    .withColumn("MED_MGT_NOTE_INPT_DTM", F.col("svMedMgtNoteDtm"))
    .withColumn("MED_MGT_NOTE_DTM", F.col("svMedMgtNoteInptDtm"))
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
    .withColumn("INSRT_UPDT_CD", F.lit("I"))
    .withColumn("DISCARD_IN", F.lit("N"))
    .withColumn("PASS_THRU_IN", F.col("RowPassThru"))
    .withColumn("FIRST_RECYC_DT", F.lit(CurrDate))
    .withColumn("ERR_CT", F.lit(0))
    .withColumn("RECYCLE_CT", F.lit(0))
    .withColumn("SRC_SYS_CD", F.lit("FACETS"))
    .withColumn(
        "PRI_KEY_STRING",
        F.concat(F.lit("FACETS;"), F.col("NTNB_ID"), F.lit(";"), F.col("NTNB_INPUT_DTM"))
    )
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("INPT_USER_SK", F.col("NTNB_INPUT_USID"))
    .withColumn(
        "UPDT_USER_SK",
        F.when(F.col("NTNB_UPD_USID") == "", F.lit("NA")).otherwise(F.col("NTNB_UPD_USID"))
    )
    .withColumn(
        "MED_MGT_NOTE_CAT_CD_SK",
        F.when(F.col("NTNB_CAT") == "", F.lit("NA")).otherwise(F.col("NTNB_CAT"))
    )
    .withColumn(
        "MED_MGT_NOTE_SUBJ_CD_SK",
        F.when(F.col("NTNB_MCTR_SUBJ") == "", F.lit("NA")).otherwise(F.col("NTNB_MCTR_SUBJ"))
    )
    .withColumn(
        "UPDT_DTM",
        F.col("NTNB_UPD_DTM")
    )
    .withColumn("CNTCT_NM", F.col("NTCT_NAME"))
    .withColumn("CNTCT_PHN_NO", F.col("NTCT_PHONE"))
    .withColumn("CNTCT_PHN_NO_EXT", F.col("NTCT_PHONE_EXT"))
    .withColumn("CNTCT_FAX_NO", F.col("NTCT_FAX"))
    .withColumn("CNTCT_FAX_NO_EXT", F.col("NTCT_FAX_EXT"))
    .withColumn("SUM_DESC", F.col("NTNB_SUMMARY"))
)

df_allcol = df_BusinessRules.select(
    "SRC_SYS_CD_SK",
    "MED_MGT_NOTE_INPT_DTM",
    "MED_MGT_NOTE_DTM",
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "INPT_USER_SK",
    "UPDT_USER_SK",
    "MED_MGT_NOTE_CAT_CD_SK",
    "MED_MGT_NOTE_SUBJ_CD_SK",
    "UPDT_DTM",
    "CNTCT_NM",
    "CNTCT_PHN_NO",
    "CNTCT_PHN_NO_EXT",
    "CNTCT_FAX_NO",
    "CNTCT_FAX_NO_EXT",
    "SUM_DESC"
)

df_transform_out = df_BusinessRules.select(
    "SRC_SYS_CD_SK",
    F.col("svMedMgtNoteDtm").alias("MED_MGT_NOTE_DTM"),
    F.col("svMedMgtNoteInptDtm").alias("MED_MGT_NOTE_INPT_DTM")
)

# MAGIC %run ../../../../../shared_containers/PrimaryKey/MedMgtNotePK
# COMMAND ----------

params_MedMgtNotePK = {}
df_IdsMedMgtNoteExtr = MedMgtNotePK(df_transform_out, df_allcol, params_MedMgtNotePK)

df_IdsMedMgtNoteExtr_final = (
    df_IdsMedMgtNoteExtr
    .withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " "))
    .withColumn("CNTCT_PHN_NO_EXT", F.rpad(F.col("CNTCT_PHN_NO_EXT"), 5, " "))
    .withColumn("CNTCT_FAX_NO_EXT", F.rpad(F.col("CNTCT_FAX_NO_EXT"), 5, " "))
    .select(
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "PASS_THRU_IN",
        "FIRST_RECYC_DT",
        "ERR_CT",
        "RECYCLE_CT",
        "SRC_SYS_CD",
        "PRI_KEY_STRING",
        "MED_MGT_NOTE_SK",
        "MED_MGT_NOTE_DTM",
        "MED_MGT_NOTE_INPT_DTM",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "INPT_USER_SK",
        "UPDT_USER_SK",
        "MED_MGT_NOTE_CAT_CD_SK",
        "MED_MGT_NOTE_SUBJ_CD_SK",
        "UPDT_DTM",
        "CNTCT_NM",
        "CNTCT_PHN_NO",
        "CNTCT_PHN_NO_EXT",
        "CNTCT_FAX_NO",
        "CNTCT_FAX_NO_EXT",
        "SUM_DESC"
    )
)

write_files(
    df_IdsMedMgtNoteExtr_final,
    f"{adls_path}/key/FctsMedMgtNoteExtr.MedMgtNote.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

jdbc_url_facets_source, jdbc_props_facets_source = get_db_config(facets_secret_name)

extract_query_facets_source = f"""
SELECT 
NOTE.NTNB_ID,
NOTE.NTNB_INPUT_DTM

FROM {FacetsOwner}.CMC_UMUM_UTIL_MGT UMUM, 
{FacetsOwner}.CMC_NTNB_NOTE_BASE NOTE 
LEFT OUTER JOIN {FacetsOwner}.CMC_NTCT_CONTACT CNTCT
    ON NOTE.NTNB_ID = CNTCT.NTNB_ID
    AND NOTE.NTNB_INPUT_DTM = CNTCT.NTNB_INPUT_DTM

WHERE 
NOTE.NTNB_ID = UMUM.NTNB_ID 
AND NOTE.NTNB_INPUT_DTM >= '{BeginDate}'
AND NOTE.NTNB_INPUT_DTM < '{EndDate}'
AND NOT( NOTE.NTNB_ID = '1753-01-01' AND NOTE.NTNB_INPUT_DTM <> '1753-01-01')

UNION

SELECT 
NOTE.NTNB_ID,
NOTE.NTNB_INPUT_DTM

FROM {FacetsOwner}.CMC_CMCM_CASE_MGT CMCM, 
{FacetsOwner}.CMC_NTNB_NOTE_BASE NOTE 
LEFT OUTER JOIN {FacetsOwner}.CMC_NTCT_CONTACT CNTCT
    ON NOTE.NTNB_ID = CNTCT.NTNB_ID
    AND NOTE.NTNB_INPUT_DTM = CNTCT.NTNB_INPUT_DTM

WHERE 
NOTE.NTNB_ID = CMCM.NTNB_ID
AND NOTE.NTNB_INPUT_DTM >= '{BeginDate}'
AND NOTE.NTNB_INPUT_DTM < '{EndDate}'
AND NOT( NOTE.NTNB_ID = '1753-01-01' AND NOTE.NTNB_INPUT_DTM <> '1753-01-01')
"""

df_Facets_Source = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets_source)
    .options(**jdbc_props_facets_source)
    .option("query", extract_query_facets_source)
    .load()
)

df_Transform_snapshot = (
    df_Facets_Source
    .withColumn("SRC_SYS_CD_SK", F.lit(SrcSysCdSk))
    .withColumn(
        "MED_MGT_NOTE_DTM",
        F.col("NTNB_ID")
    )
    .withColumn(
        "MED_MGT_NOTE_INPT_DTM",
        F.col("NTNB_INPUT_DTM")
    )
    .withColumn(
        "MED_MGT_NOTE_DTM",
        F.concat(
            F.substring(F.col("MED_MGT_NOTE_DTM"), 1, 10), F.lit(" "),
            F.substring(F.col("MED_MGT_NOTE_DTM"), 12, 2), F.lit(":"),
            F.substring(F.col("MED_MGT_NOTE_DTM"), 15, 2), F.lit(":"),
            F.substring(F.col("MED_MGT_NOTE_DTM"), 18, 2), F.lit("."),
            F.substring(F.col("MED_MGT_NOTE_DTM"), 21, 6)
        )
    )
    .withColumn(
        "MED_MGT_NOTE_INPT_DTM",
        F.concat(
            F.substring(F.col("MED_MGT_NOTE_INPT_DTM"), 1, 10), F.lit(" "),
            F.substring(F.col("MED_MGT_NOTE_INPT_DTM"), 12, 2), F.lit(":"),
            F.substring(F.col("MED_MGT_NOTE_INPT_DTM"), 15, 2), F.lit(":"),
            F.substring(F.col("MED_MGT_NOTE_INPT_DTM"), 18, 2), F.lit("."),
            F.substring(F.col("MED_MGT_NOTE_INPT_DTM"), 21, 6)
        )
    )
)

df_Snapshot_File_final = df_Transform_snapshot.select(
    "SRC_SYS_CD_SK",
    "MED_MGT_NOTE_DTM",
    "MED_MGT_NOTE_INPT_DTM"
)

write_files(
    df_Snapshot_File_final,
    f"{adls_path}/load/B_MED_MGT_NOTE.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)