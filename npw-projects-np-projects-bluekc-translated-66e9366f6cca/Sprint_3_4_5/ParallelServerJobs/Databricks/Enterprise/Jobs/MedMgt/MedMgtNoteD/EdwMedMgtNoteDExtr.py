# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     EdwMedMgtNoteDExtr
# MAGIC 
# MAGIC DESCRIPTION:  Extracts from IDS MED_MGT_NOTE to flatfile MED_MGT_NOTE_D.dat
# MAGIC       
# MAGIC 
# MAGIC INPUTS:
# MAGIC The following IDS tables are used:
# MAGIC MED_MGT_NOTE
# MAGIC APP_USER
# MAGIC 
# MAGIC 
# MAGIC   
# MAGIC HASH FILES:
# MAGIC                 hf_cdma
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                   TRIM all the fields that are extracted or used in lookups
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                     Flatfile: MED_MGT_NOTE_D.dat
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC               
# MAGIC Developer                          Date               Project/Altiris #               Change Description                                                                            Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------                                                      ---------------------------------   ---------------------------------    -------------------------   
# MAGIC Suzanne Saylor              3/03/2006          MedMgmt/                  Original Programming.                                                                               devlEDW10
# MAGIC Bhoomi Dasari                01/17/2008        MedMgmt/                  Added LAST_UPDT_RUN_CYC_EXCTN_SK  >= ExtractRunCycle        devlEDW                   Steph Goddard             01/23/2008
# MAGIC                                                                                                      rule based upon which we extract records now.  
# MAGIC                                                                                                       Added Hash.Clear and checked null's for lkups
# MAGIC Ralph Tucker                 09/15/2009     TTR-583                       Added EdwRunCycle to last updt run cyc                                                 devlEDW                    Steph Goddard            09/16/2009
# MAGIC Sravya Gorla                   10/02/2019     US# 140167                 Updated the datatype of MED_MGT_NOTE_DTM                                EnterpriseDev2             Jaideep Mankala        10/07/2019
# MAGIC                                                                                                       from Timestamp(26.3) to timestamp(26.6)

# MAGIC Extract IDS  Data
# MAGIC Apply business logic.
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
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


# Parameter retrieval
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrentDate = get_widget_value('CurrentDate','')
EdwRunCycle = get_widget_value('EdwRunCycle','')
ExtractRunCycle = get_widget_value('ExtractRunCycle','')

# Obtain JDBC configuration
jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# Read from IDS - Extract
extract_query = f"""
SELECT note.MED_MGT_NOTE_SK as MED_MGT_NOTE_SK,
       note.SRC_SYS_CD_SK as SRC_SYS_CD_SK,
       note.MED_MGT_NOTE_DTM as MED_MGT_NOTE_DTM,
       note.MED_MGT_NOTE_INPT_DTM as MED_MGT_NOTE_INPT_DTM,
       note.MED_MGT_NOTE_CAT_CD_SK as MED_MGT_NOTE_CAT_CD_SK,
       note.MED_MGT_NOTE_SUBJ_CD_SK as MED_MGT_NOTE_SUBJ_CD_SK,
       note.UPDT_DTM as UPDT_DTM,
       note.CNTCT_NM as CNTCT_NM,
       note.CNTCT_PHN_NO as CNTCT_PHN_NO,
       note.CNTCT_PHN_NO_EXT as CNTCT_PHN_NO_EXT,
       note.CNTCT_FAX_NO as CNTCT_FAX_NO,
       note.CNTCT_FAX_NO_EXT as CNTCT_FAX_NO_EXT,
       note.INPT_USER_SK as INPT_USER_SK,
       note.SUM_DESC as SUM_DESC,
       note.UPDT_USER_SK as UPDT_USER_SK,
       note.CRT_RUN_CYC_EXCTN_SK as CRT_RUN_CYC_EXCTN_SK,
       note.LAST_UPDT_RUN_CYC_EXCTN_SK as LAST_UPDT_RUN_CYC_EXCTN_SK,
       app.USER_ID as USER_ID
  FROM {IDSOwner}.MED_MGT_NOTE note,
       {IDSOwner}.APP_USER app
 WHERE note.INPT_USER_SK = app.USER_SK
   AND note.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtractRunCycle}
"""
df_IDS_Extract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# Read from IDS - MedMgtNoteUpdtUserId
medmgtuser_query = f"""
SELECT app.USER_SK as USER_SK,
       app.USER_ID as USER_ID
  FROM {IDSOwner}.MED_MGT_NOTE note,
       {IDSOwner}.APP_USER app
 WHERE note.UPDT_USER_SK = app.USER_SK
"""
df_IDS_MedMgtNoteUpdtUserId = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", medmgtuser_query)
    .load()
)

# CHashedFileStage: hf_cdma_codes -> scenario C (read parquet)
df_hf_cdma_codes = spark.read.parquet("hf_cdma_codes.parquet")

# CHashedFileStage: hf_note_updt_user_id -> scenario A (deduplicate on USER_SK)
df_note_updt_user_id = dedup_sort(df_IDS_MedMgtNoteUpdtUserId, ["USER_SK"], [])

# BusinessRules: chain of joins
df_businessRules = (
    df_IDS_Extract.alias("Extract")
    .join(
        df_hf_cdma_codes.alias("refSrcSysCd"),
        F.col("Extract.SRC_SYS_CD_SK") == F.col("refSrcSysCd.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_hf_cdma_codes.alias("refMedMgtNoteCatCd"),
        F.col("Extract.MED_MGT_NOTE_CAT_CD_SK") == F.col("refMedMgtNoteCatCd.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_hf_cdma_codes.alias("refMedMgtNoteSubjCd"),
        F.col("Extract.MED_MGT_NOTE_SUBJ_CD_SK") == F.col("refMedMgtNoteSubjCd.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_note_updt_user_id.alias("lkup"),
        F.col("Extract.UPDT_USER_SK") == F.col("lkup.USER_SK"),
        "left"
    )
)

df_businessRules = (
    df_businessRules
    .withColumn("MED_MGT_NOTE_SK", F.col("Extract.MED_MGT_NOTE_SK"))
    .withColumn(
        "SRC_SYS_CD",
        F.when(
            F.col("refSrcSysCd.TRGT_CD").isNull() |
            (F.length(trim(F.col("refSrcSysCd.TRGT_CD"))) == 0),
            "NA"
        ).otherwise(F.col("refSrcSysCd.TRGT_CD"))
    )
    .withColumn("MED_MGT_NOTE_DTM", F.col("Extract.MED_MGT_NOTE_DTM"))
    .withColumn("MED_MGT_NOTE_INPT_DTM", F.col("Extract.MED_MGT_NOTE_INPT_DTM"))
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.lit(CurrentDate))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.lit(CurrentDate))
    .withColumn(
        "MED_MGT_NOTE_CAT_CD",
        F.when(
            F.col("refMedMgtNoteCatCd.TRGT_CD").isNull() |
            (F.length(trim(F.col("refMedMgtNoteCatCd.TRGT_CD"))) == 0),
            "NA"
        ).otherwise(F.col("refMedMgtNoteCatCd.TRGT_CD"))
    )
    .withColumn(
        "MED_MGT_NOTE_CAT_NM",
        F.when(
            F.col("refMedMgtNoteCatCd.TRGT_CD_NM").isNull() |
            (F.length(trim(F.col("refMedMgtNoteCatCd.TRGT_CD_NM"))) == 0),
            "NA"
        ).otherwise(F.col("refMedMgtNoteCatCd.TRGT_CD_NM"))
    )
    .withColumn(
        "MED_MGT_NOTE_SUBJ_CD",
        F.when(
            F.col("refMedMgtNoteSubjCd.TRGT_CD").isNull() |
            (F.length(trim(F.col("refMedMgtNoteSubjCd.TRGT_CD"))) == 0),
            "NA"
        ).otherwise(F.col("refMedMgtNoteSubjCd.TRGT_CD"))
    )
    .withColumn(
        "MED_MGT_NOTE_SUBJ_NM",
        F.when(
            F.col("refMedMgtNoteSubjCd.TRGT_CD_NM").isNull() |
            (F.length(trim(F.col("refMedMgtNoteSubjCd.TRGT_CD_NM"))) == 0),
            "NA"
        ).otherwise(F.col("refMedMgtNoteSubjCd.TRGT_CD_NM"))
    )
    .withColumn("MED_MGT_NOTE_CAT_CD_SK", F.col("Extract.MED_MGT_NOTE_CAT_CD_SK"))
    .withColumn("MED_MGT_NOTE_SUBJ_CD_SK", F.col("Extract.MED_MGT_NOTE_SUBJ_CD_SK"))
    .withColumn("MED_MGT_NOTE_UPDT_DTM", F.col("Extract.UPDT_DTM"))
    .withColumn("MED_MGT_NOTE_CNTCT_NM", F.col("Extract.CNTCT_NM"))
    .withColumn("MED_MGT_NOTE_CNTCT_PHN_NO", F.col("Extract.CNTCT_PHN_NO"))
    .withColumn("MED_MGT_NOTE_CNTCT_PHN_NO_EXT", F.col("Extract.CNTCT_PHN_NO_EXT"))
    .withColumn("MED_MGT_NOTE_CNTCT_FAX_NO", F.col("Extract.CNTCT_FAX_NO"))
    .withColumn("MED_MGT_NOTE_CNTCT_FAX_NO_EXT", F.col("Extract.CNTCT_FAX_NO_EXT"))
    .withColumn("MED_MGT_NOTE_INPT_USER_ID", F.col("Extract.USER_ID"))
    .withColumn("MED_MGT_NOTE_SUM_DESC", F.col("Extract.SUM_DESC"))
    .withColumn(
        "MED_MGT_NOTE_UPDT_USER_ID",
        F.when(
            F.col("lkup.USER_ID").isNull() |
            (F.length(trim(F.col("lkup.USER_ID"))) == 0),
            "NA"
        ).otherwise(F.col("lkup.USER_ID"))
    )
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.col("Extract.CRT_RUN_CYC_EXCTN_SK"))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(EdwRunCycle))
)

df_final = df_businessRules.select(
    "MED_MGT_NOTE_SK",
    "SRC_SYS_CD",
    "MED_MGT_NOTE_DTM",
    "MED_MGT_NOTE_INPT_DTM",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "MED_MGT_NOTE_CAT_CD",
    "MED_MGT_NOTE_CAT_NM",
    "MED_MGT_NOTE_SUBJ_CD",
    "MED_MGT_NOTE_SUBJ_NM",
    "MED_MGT_NOTE_CAT_CD_SK",
    "MED_MGT_NOTE_SUBJ_CD_SK",
    "MED_MGT_NOTE_UPDT_DTM",
    "MED_MGT_NOTE_CNTCT_NM",
    "MED_MGT_NOTE_CNTCT_PHN_NO",
    "MED_MGT_NOTE_CNTCT_PHN_NO_EXT",
    "MED_MGT_NOTE_CNTCT_FAX_NO",
    "MED_MGT_NOTE_CNTCT_FAX_NO_EXT",
    "MED_MGT_NOTE_INPT_USER_ID",
    "MED_MGT_NOTE_SUM_DESC",
    "MED_MGT_NOTE_UPDT_USER_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK"
).withColumn(
    "CRT_RUN_CYC_EXCTN_DT_SK",
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
).withColumn(
    "MED_MGT_NOTE_CNTCT_PHN_NO_EXT",
    F.rpad(F.col("MED_MGT_NOTE_CNTCT_PHN_NO_EXT"), 5, " ")
).withColumn(
    "MED_MGT_NOTE_CNTCT_FAX_NO_EXT",
    F.rpad(F.col("MED_MGT_NOTE_CNTCT_FAX_NO_EXT"), 5, " ")
).withColumn(
    "MED_MGT_NOTE_INPT_USER_ID",
    F.rpad(F.col("MED_MGT_NOTE_INPT_USER_ID"), 10, " ")
).withColumn(
    "MED_MGT_NOTE_UPDT_USER_ID",
    F.rpad(F.col("MED_MGT_NOTE_UPDT_USER_ID"), 10, " ")
)

write_files(
    df_final,
    f"{adls_path}/load/MED_MGT_NOTE_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)