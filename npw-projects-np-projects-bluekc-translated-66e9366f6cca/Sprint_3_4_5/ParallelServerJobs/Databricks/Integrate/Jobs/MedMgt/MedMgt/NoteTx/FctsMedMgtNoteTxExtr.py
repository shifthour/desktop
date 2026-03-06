# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2006, 2007, 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:    FctsMedMgtExtrSeq 
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:  Pulls data from CMC_NTTX_NOTE_TEXT to a landing file for the IDS
# MAGIC   
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                    Project/Altiris #                     Change Description                                                       Development Project        Code Reviewer               Date Reviewed
# MAGIC ----------------------------------      -------------------      -----------------------------------           ---------------------------------------------------------                                 ----------------------------------       ---------------------------------        -------------------------
# MAGIC Suzanne Saylor               2006-02-15                                                     Original Programming.
# MAGIC Suzanne Saylor               2006-05-23                                                     Added fix for care guide note issue and orphan record issue.
# MAGIC Parik                               04/10/2007              3264                            Added Balancing process to the overall job that takes           devlIDS30                       Steph Goddard               9/14/07
# MAGIC                                                                                                               a snapshot of the source data         
# MAGIC Bhoomi Dasari                2008-06-24                3657(Primary Key)       Changed primary key process from hash file to DB2 table       devlIDS                          Brent Leland                    2008-06-26
# MAGIC 
# MAGIC Sravya Gorla                  2019-09-03                 US140167                  Update the datatype of  NOTE_TX_SEQ_NO   
# MAGIC                                                                                                                    fron Integer to Smallint                                              IntegrateDev2		 Jaideep Mankala	         09/24/2019
# MAGIC Prabhu ES                     2022-03-07               S2S Remediation          MSSQL ODBC conn params added                                IntegrateDev5
# MAGIC Vikas Abbu                     2022-03-07               S2S Remediation          Fixed SQl Error                                                                IntegrateDev5                   Manasa Andru                  2022-06-14

# MAGIC Load IDS temp. table
# MAGIC Container hash file cleared by after-job subroutine
# MAGIC Extract Facets Data
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
from pyspark.sql.functions import col, lit, rpad, concat, substring
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------

# MAGIC %run ../../../../../shared_containers/PrimaryKey/MedMgtNoteTxPK
# COMMAND ----------

# Parameters
CurrRunCycle = get_widget_value('CurrRunCycle','')
BeginDate = get_widget_value('BeginDate','')
EndDate = get_widget_value('EndDate','')
RunID = get_widget_value('RunID','')
CurrDate = get_widget_value('CurrDate','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
SrcSysCd = get_widget_value('SrcSysCd','')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
FacetsDB = get_widget_value('FacetsDB','')

# ------------------------------------------------------------------------------
# Stage: CMC_NTTX_NOTE_TEXT (ODBCConnector)
jdbc_url, jdbc_props = get_db_config(facets_secret_name)
extract_query_CMC_NTTX_NOTE_TEXT = f"""
SELECT 
TXT.NTNB_ID,
TXT.NTNB_INPUT_DTM,
TXT.NTTX_SEQ_NO,
TXT.NTTX_TEXT
FROM {FacetsOwner}.CMC_NTTX_NOTE_TEXT TXT,
     {FacetsOwner}.CMC_UMUM_UTIL_MGT UM,
     {FacetsOwner}.CMC_NTNB_NOTE_BASE NOTE
WHERE UM.NTNB_ID = NOTE.NTNB_ID
  AND NOTE.NTNB_ID = TXT.NTNB_ID
  AND NOTE.NTNB_INPUT_DTM = TXT.NTNB_INPUT_DTM
  AND TXT.NTNB_INPUT_DTM >= '{BeginDate}'
  AND TXT.NTNB_INPUT_DTM < '{EndDate}'
  AND NOT( TXT.NTNB_ID = '1753-01-01' AND TXT.NTNB_INPUT_DTM <> '1753-01-01')

UNION

SELECT
TXT.NTNB_ID,
TXT.NTNB_INPUT_DTM,
TXT.NTTX_SEQ_NO,
TXT.NTTX_TEXT
FROM {FacetsOwner}.CMC_NTTX_NOTE_TEXT TXT,
     {FacetsOwner}.CMC_CMCM_CASE_MGT CM,
     {FacetsOwner}.CMC_NTNB_NOTE_BASE NOTE
WHERE CM.NTNB_ID = NOTE.NTNB_ID
  AND NOTE.NTNB_ID = TXT.NTNB_ID
  AND NOTE.NTNB_INPUT_DTM = TXT.NTNB_INPUT_DTM
  AND TXT.NTNB_INPUT_DTM >= '{BeginDate}'
  AND TXT.NTNB_INPUT_DTM < '{EndDate}'
  AND NOT( TXT.NTNB_ID = '1753-01-01' AND TXT.NTNB_INPUT_DTM <> '1753-01-01')
"""
df_CMC_NTTX_NOTE_TEXT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_CMC_NTTX_NOTE_TEXT)
    .load()
)

# ------------------------------------------------------------------------------
# Stage: StripField (CTransformerStage)
# Apply expressions
df_StripField = df_CMC_NTTX_NOTE_TEXT
df_StripField = df_StripField.withColumn(
    "NTNB_ID",
    FORMAT_DATE(col("NTNB_ID"), lit("SYBASE"), lit("TIMESTAMP"), lit("DB2TIMESTAMP"))
)
df_StripField = df_StripField.withColumn(
    "NTNB_INPUT_DTM",
    FORMAT_DATE(col("NTNB_INPUT_DTM"), lit("SYBASE"), lit("TIMESTAMP"), lit("DB2TIMESTAMP"))
)
df_StripField = df_StripField.withColumn(
    "NTTX_SEQ_NO",
    col("NTTX_SEQ_NO")
)
df_StripField = df_StripField.withColumn(
    "NTTX_TEXT",
    trim(Convert(lit("CHAR(10):CHAR(13):CHAR(9)"), lit(""), col("NTTX_TEXT")))
)

# ------------------------------------------------------------------------------
# Stage: BusinessRules (CTransformerStage)
df_businessRules = (
    df_StripField
    .withColumn("RowPassThru", lit("Y"))
    .withColumn("svSrcSysCd", lit("FACETS"))
    .withColumn(
        "svMedMgtNoteDtm",
        concat(
            substring(col("NTNB_ID"), 1, 10),
            lit(" "),
            substring(col("NTNB_ID"), 12, 2),
            lit(":"),
            substring(col("NTNB_ID"), 15, 2),
            lit(":"),
            substring(col("NTNB_ID"), 18, 2),
            lit("."),
            substring(col("NTNB_ID"), 21, 6)
        )
    )
    .withColumn(
        "svMedMgtNoteInptDtm",
        concat(
            substring(col("NTNB_INPUT_DTM"), 1, 10),
            lit(" "),
            substring(col("NTNB_INPUT_DTM"), 12, 2),
            lit(":"),
            substring(col("NTNB_INPUT_DTM"), 15, 2),
            lit(":"),
            substring(col("NTNB_INPUT_DTM"), 18, 2),
            lit("."),
            substring(col("NTNB_INPUT_DTM"), 21, 6)
        )
    )
)

df_allcoll = df_businessRules.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("svMedMgtNoteDtm").alias("MED_MGT_NOTE_DTM"),
    col("svMedMgtNoteInptDtm").alias("MED_MGT_NOTE_INPT_DTM"),
    col("NTTX_SEQ_NO").alias("NOTE_TX_SEQ_NO"),
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    lit("I").alias("INSRT_UPDT_CD"),
    lit("N").alias("DISCARD_IN"),
    col("RowPassThru").alias("PASS_THRU_IN"),
    lit(CurrDate).alias("FIRST_RECYC_DT"),
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    lit("FACETS").alias("SRC_SYS_CD"),
    concat(lit("FACETS"), lit(";"), col("NTNB_ID"), lit(";"), col("NTNB_INPUT_DTM"), lit(";"), col("NTTX_SEQ_NO")).alias("PRI_KEY_STRING"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("MED_MGT_NOTE_SK"),
    col("NTTX_TEXT").alias("NOTE_TX")
)

df_transform = df_businessRules.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("svMedMgtNoteDtm").alias("MED_MGT_NOTE_DTM"),
    col("svMedMgtNoteInptDtm").alias("MED_MGT_NOTE_INPT_DTM"),
    col("NTTX_SEQ_NO").alias("NOTE_TX_SEQ_NO")
)

# ------------------------------------------------------------------------------
# Stage: MedMgtNoteTxPK (CContainerStage)
params_MedMgtNoteTxPK = {
    "CurrRunCycle": CurrRunCycle,
    "CurrDate": CurrDate,
    "SrcSysCdSk": SrcSysCdSk,
    "SrcSysCd": SrcSysCd,
    "RunID": RunID,
    "IDSOwner": IDSOwner,
    "FacetsDB": FacetsDB,
    "FacetsOwner": FacetsOwner
}
df_key = MedMgtNoteTxPK(df_allcoll, df_transform, params_MedMgtNoteTxPK)

# ------------------------------------------------------------------------------
# Stage: IdsMedMgtNoteTxExtr (CSeqFileStage)
df_IdsMedMgtNoteTxExtr = df_key.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "MED_MGT_NOTE_TX_SK",
    "MED_MGT_NOTE_DTM",
    "MED_MGT_NOTE_INPT_DTM",
    "NOTE_TX_SEQ_NO",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "MED_MGT_NOTE_SK",
    "NOTE_TX"
)

df_IdsMedMgtNoteTxExtr = df_IdsMedMgtNoteTxExtr.withColumn(
    "INSRT_UPDT_CD", rpad(col("INSRT_UPDT_CD"), 10, " ")
).withColumn(
    "DISCARD_IN", rpad(col("DISCARD_IN"), 1, " ")
).withColumn(
    "PASS_THRU_IN", rpad(col("PASS_THRU_IN"), 1, " ")
).withColumn(
    "NOTE_TX", rpad(col("NOTE_TX"), 70, " ")
)

write_files(
    df_IdsMedMgtNoteTxExtr,
    f"{adls_path}/key/FctsMedMgtNoteTxExtr.MedMgtNoteTx.dat.{RunID}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)

# ------------------------------------------------------------------------------
# Stage: Facets_Source (ODBCConnector)
jdbc_url_2, jdbc_props_2 = get_db_config(facets_secret_name)
extract_query_Facets_Source = f"""
SELECT 
TXT.NTNB_ID,
TXT.NTNB_INPUT_DTM,
TXT.NTTX_SEQ_NO
FROM {FacetsOwner}.CMC_NTTX_NOTE_TEXT TXT,
     {FacetsOwner}.CMC_UMUM_UTIL_MGT UM,
     {FacetsOwner}.CMC_NTNB_NOTE_BASE NOTE
WHERE UM.NTNB_ID = NOTE.NTNB_ID
  AND NOTE.NTNB_ID = TXT.NTNB_ID
  AND NOTE.NTNB_INPUT_DTM = TXT.NTNB_INPUT_DTM
  AND TXT.NTNB_INPUT_DTM >= '{BeginDate}'
  AND TXT.NTNB_INPUT_DTM < '{EndDate}'
  AND NOT( TXT.NTNB_ID = '1753-01-01' AND TXT.NTNB_INPUT_DTM <> '1753-01-01')

UNION

SELECT
TXT.NTNB_ID,
TXT.NTNB_INPUT_DTM,
TXT.NTTX_SEQ_NO
FROM {FacetsOwner}.CMC_NTTX_NOTE_TEXT TXT,
     {FacetsOwner}.CMC_CMCM_CASE_MGT CM,
     {FacetsOwner}.CMC_NTNB_NOTE_BASE NOTE
WHERE CM.NTNB_ID = NOTE.NTNB_ID
  AND NOTE.NTNB_ID = TXT.NTNB_ID
  AND NOTE.NTNB_INPUT_DTM = TXT.NTNB_INPUT_DTM
  AND TXT.NTNB_INPUT_DTM >= '{BeginDate}'
  AND TXT.NTNB_INPUT_DTM < '{EndDate}'
  AND NOT( TXT.NTNB_ID = '1753-01-01' AND TXT.NTNB_INPUT_DTM <> '1753-01-01')
"""
df_Facets_Source = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_2)
    .options(**jdbc_props_2)
    .option("query", extract_query_Facets_Source)
    .load()
)

# ------------------------------------------------------------------------------
# Stage: Transform (CTransformerStage) for Facets_Source
df_snapshot_transform = df_Facets_Source.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    FORMAT_DATE(col("NTNB_ID"), lit("SYBASE"), lit("TIMESTAMP"), lit("DB2TIMESTAMP")).alias("MED_MGT_NOTE_DTM"),
    FORMAT_DATE(col("NTNB_INPUT_DTM"), lit("SYBASE"), lit("TIMESTAMP"), lit("DB2TIMESTAMP")).alias("MED_MGT_NOTE_INPT_DTM"),
    col("NTTX_SEQ_NO").alias("NOTE_TX_SEQ_NO")
)

# ------------------------------------------------------------------------------
# Stage: Snapshot_File (CSeqFileStage)
df_Snapshot_File = df_snapshot_transform.select(
    "SRC_SYS_CD_SK",
    "MED_MGT_NOTE_DTM",
    "MED_MGT_NOTE_INPT_DTM",
    "NOTE_TX_SEQ_NO"
)

write_files(
    df_Snapshot_File,
    f"{adls_path}/load/B_MED_MGT_NOTE_TX.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)