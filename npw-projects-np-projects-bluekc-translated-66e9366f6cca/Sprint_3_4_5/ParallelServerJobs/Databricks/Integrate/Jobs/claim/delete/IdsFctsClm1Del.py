# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2006 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:  IdsClmDelete
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  Using the  X_RCRD_DEL table as a driver table delete claims no longer in IDS that are no longer in the source system.
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:   xxxSeq
# MAGIC 
# MAGIC 
# MAGIC INPUTS: See "Parameters" tab
# MAGIC               UV LoadDates Entry:  None
# MAGIC               UV RunDate Entry:     None
# MAGIC                
# MAGIC 
# MAGIC EMAIL FILES:  None
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:   After data is loaded delete claim records matching the driver table where the run cycle update is older than the current run.
# MAGIC 
# MAGIC 
# MAGIC UNIX SCRIPTS USED:  none
# MAGIC 
# MAGIC 
# MAGIC DATABASE TABLES USED:
# MAGIC                  Database                    Table Name
# MAGIC                  ----------------------------       ---------------------------------------------------------------------
# MAGIC                  IDS_PROD                CLM_LN
# MAGIC                  IDS_PROD                CLM_DIAG
# MAGIC                  IDS_PROD                CLM_PROV
# MAGIC                  IDS_PROD                CLM_COB
# MAGIC                  IDS_PROD                FCLTY_CLM_PROC
# MAGIC                  IDS_PROD                CLM_LN_PROC_CD_MOD
# MAGIC                  IDS_PROD                CLM_OVRD
# MAGIC                  IDS_PROD                CLM_ALT_PAYE
# MAGIC                  IDS_PROD                CLM_ATCHMT
# MAGIC                  IDS_PROD                PAYMT_RDUCTN
# MAGIC                  IDS_PROD                PAYMT_RDUCTN_ACTVTY
# MAGIC                  IDS_PROD                CLM_OVER_PAYMT
# MAGIC                  IDS_PROD                CLM_LTR
# MAGIC                  IDS_PROD                CLM_NOTE
# MAGIC                  IDS_PROD                CLM_LN_CLNCL_EDIT
# MAGIC                  IDS_PROD                CLM_LN_OVRD
# MAGIC                  IDS_PROD                CLM_LN_DSALW
# MAGIC                  IDS_PROD                DNTL_CLM_LN
# MAGIC                  IDS_PROD                CLM_LN_COB
# MAGIC                  IDS_PROD                CLM_LN_DIAG
# MAGIC 
# MAGIC BCBSKC TRANSFORMS/ROUTINES USED:    None
# MAGIC 
# MAGIC 
# MAGIC HASH FILES:  None
# MAGIC 
# MAGIC 
# MAGIC OUTPUTS:   None
# MAGIC 
# MAGIC 
# MAGIC RERUN INFORMATION: 
# MAGIC              PREVIOUS RUN SUCCESSFUL:   Nothing to reset just rerun. 
# MAGIC              PREVIOUS RUN ABORTED:         Nothing to reset just rerun.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Date                 Developer                Change Description
# MAGIC ------------------      ----------------------------     --------------------------------------------------------------------------------------------------------
# MAGIC 04-06-2005      Brent Leland             Initial programming.
# MAGIC 08/15/2006     Brent Leland             Change to select records then delete.
# MAGIC 08/30/2006     Brent Leland             Changed claim line disallow sql delete from not equal to equal SK.
# MAGIC 02-14-2009      Brent Leland             Added capture of deleted records for clm_ln_dsalw and clm_diag to eliminate
# MAGIC                                                          SK load failures in the EDW.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC ==================================================================================================================================================================
# MAGIC Developer                    Date                       Change Description                                                                                     Project #             Development Project          Code Reviewer               Date Reviewed  
# MAGIC ==================================================================================================================================================================
# MAGIC Manasa Andru       07-14-2013       Deleting "hf_cmlndsalw_del" file which is not used in the job                                   TTR - 495           IntegrateCurDevl               Bhoomi Dasari                    8/7/2013

# MAGIC Delete claim records that are no longer in the source system.
# MAGIC Claim delete for Facets records
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
SourceSK = get_widget_value('SourceSK','35122')
RunCycle = get_widget_value('RunCycle','130')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# Select_PAYMT_RDUCTN -> PAYMT_RDUCTN
extract_query = f"""
SELECT c.PAYMT_RDUCTN_SK
FROM {IDSOwner}.PAYMT_RDUCTN c,
     {IDSOwner}.W_FCTS_RCRD_DEL d,
     {IDSOwner}.CLM_OVER_PAYMT x
WHERE d.SUBJ_NM = 'CLM_ID'
  AND x.SRC_SYS_CD_SK = {SourceSK}
  AND d.key_val = x.clm_id
  AND x.PAYMT_RDUCTN_REF_ID = c.PAYMT_RDUCTN_REF_ID
  AND c.LAST_UPDT_RUN_CYC_EXCTN_SK <> {RunCycle}
"""
df_Select_PAYMT_RDUCTN = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)
execute_dml("DROP TABLE IF EXISTS STAGING.IdsFctsClm1Del_PAYMT_RDUCTN_temp", jdbc_url, jdbc_props)
df_Select_PAYMT_RDUCTN.write.jdbc(url=jdbc_url, table="STAGING.IdsFctsClm1Del_PAYMT_RDUCTN_temp", mode="overwrite", properties=jdbc_props)
merge_sql = f"""
MERGE INTO {IDSOwner}.PAYMT_RDUCTN AS T
USING STAGING.IdsFctsClm1Del_PAYMT_RDUCTN_temp AS S
ON T.PAYMT_RDUCTN_SK = S.PAYMT_RDUCTN_SK
WHEN MATCHED THEN
  DELETE;
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

# Select_CLM_LN_CLNCL_EDIT -> CLM_LN_CLNCL_EDIT
extract_query = f"""
SELECT c.CLM_LN_CLNCL_EDIT_SK
FROM {IDSOwner}.CLM_LN_CLNCL_EDIT c,
     {IDSOwner}.W_FCTS_RCRD_DEL d
WHERE d.SUBJ_NM = 'CLM_ID'
  AND c.SRC_SYS_CD_SK = {SourceSK}
  AND d.KEY_VAL = c.CLM_ID
  AND c.LAST_UPDT_RUN_CYC_EXCTN_SK <> {RunCycle}
"""
df_Select_CLM_LN_CLNCL_EDIT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)
execute_dml("DROP TABLE IF EXISTS STAGING.IdsFctsClm1Del_CLM_LN_CLNCL_EDIT_temp", jdbc_url, jdbc_props)
df_Select_CLM_LN_CLNCL_EDIT.write.jdbc(url=jdbc_url, table="STAGING.IdsFctsClm1Del_CLM_LN_CLNCL_EDIT_temp", mode="overwrite", properties=jdbc_props)
merge_sql = f"""
MERGE INTO {IDSOwner}.CLM_LN_CLNCL_EDIT AS T
USING STAGING.IdsFctsClm1Del_CLM_LN_CLNCL_EDIT_temp AS S
ON T.CLM_LN_CLNCL_EDIT_SK = S.CLM_LN_CLNCL_EDIT_SK
WHEN MATCHED THEN
  DELETE;
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

# Select_CLM_OVER_PAYMT -> CLM_OVER_PAYMT
extract_query = f"""
SELECT c.CLM_OVER_PAYMT_SK
FROM {IDSOwner}.CLM_OVER_PAYMT c,
     {IDSOwner}.W_FCTS_RCRD_DEL d
WHERE d.SUBJ_NM = 'CLM_ID'
  AND c.SRC_SYS_CD_SK = {SourceSK}
  AND d.KEY_VAL = c.CLM_ID
  AND c.LAST_UPDT_RUN_CYC_EXCTN_SK <> {RunCycle}
"""
df_Select_CLM_OVER_PAYMT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)
execute_dml("DROP TABLE IF EXISTS STAGING.IdsFctsClm1Del_CLM_OVER_PAYMT_temp", jdbc_url, jdbc_props)
df_Select_CLM_OVER_PAYMT.write.jdbc(url=jdbc_url, table="STAGING.IdsFctsClm1Del_CLM_OVER_PAYMT_temp", mode="overwrite", properties=jdbc_props)
merge_sql = f"""
MERGE INTO {IDSOwner}.CLM_OVER_PAYMT AS T
USING STAGING.IdsFctsClm1Del_CLM_OVER_PAYMT_temp AS S
ON T.CLM_OVER_PAYMT_SK = S.CLM_OVER_PAYMT_SK
WHEN MATCHED THEN
  DELETE;
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

# Select_CLM_LTR -> CLM_LTR
extract_query = f"""
SELECT c.CLM_LTR_SK
FROM {IDSOwner}.CLM_LTR c,
     {IDSOwner}.W_FCTS_RCRD_DEL d
WHERE d.SUBJ_NM = 'CLM_ID'
  AND c.SRC_SYS_CD_SK = {SourceSK}
  AND d.KEY_VAL = c.CLM_ID
  AND c.LAST_UPDT_RUN_CYC_EXCTN_SK <> {RunCycle}
"""
df_Select_CLM_LTR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)
execute_dml("DROP TABLE IF EXISTS STAGING.IdsFctsClm1Del_CLM_LTR_temp", jdbc_url, jdbc_props)
df_Select_CLM_LTR.write.jdbc(url=jdbc_url, table="STAGING.IdsFctsClm1Del_CLM_LTR_temp", mode="overwrite", properties=jdbc_props)
merge_sql = f"""
MERGE INTO {IDSOwner}.CLM_LTR AS T
USING STAGING.IdsFctsClm1Del_CLM_LTR_temp AS S
ON T.CLM_LTR_SK = S.CLM_LTR_SK
WHEN MATCHED THEN
  DELETE;
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

# Select_CLM_NOTE -> CLM_NOTE
extract_query = f"""
SELECT c.CLM_NOTE_SK
FROM {IDSOwner}.CLM_NOTE c,
     {IDSOwner}.W_FCTS_RCRD_DEL d
WHERE d.SUBJ_NM = 'CLM_ID'
  AND c.SRC_SYS_CD_SK = {SourceSK}
  AND d.KEY_VAL = c.CLM_ID
  AND c.LAST_UPDT_RUN_CYC_EXCTN_SK <> {RunCycle}
"""
df_Select_CLM_NOTE = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)
execute_dml("DROP TABLE IF EXISTS STAGING.IdsFctsClm1Del_CLM_NOTE_temp", jdbc_url, jdbc_props)
df_Select_CLM_NOTE.write.jdbc(url=jdbc_url, table="STAGING.IdsFctsClm1Del_CLM_NOTE_temp", mode="overwrite", properties=jdbc_props)
merge_sql = f"""
MERGE INTO {IDSOwner}.CLM_NOTE AS T
USING STAGING.IdsFctsClm1Del_CLM_NOTE_temp AS S
ON T.CLM_NOTE_SK = S.CLM_NOTE_SK
WHEN MATCHED THEN
  DELETE;
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

# Select_CLM_LN_COB -> CLM_LN_COB
extract_query = f"""
SELECT c.CLM_COB_LN_SK
FROM {IDSOwner}.CLM_LN_COB c,
     {IDSOwner}.W_FCTS_RCRD_DEL d
WHERE d.SUBJ_NM = 'CLM_ID'
  AND c.SRC_SYS_CD_SK = {SourceSK}
  AND d.KEY_VAL = c.CLM_ID
  AND c.LAST_UPDT_RUN_CYC_EXCTN_SK <> {RunCycle}
"""
df_Select_CLM_LN_COB = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)
execute_dml("DROP TABLE IF EXISTS STAGING.IdsFctsClm1Del_CLM_LN_COB_temp", jdbc_url, jdbc_props)
df_Select_CLM_LN_COB.write.jdbc(url=jdbc_url, table="STAGING.IdsFctsClm1Del_CLM_LN_COB_temp", mode="overwrite", properties=jdbc_props)
merge_sql = f"""
MERGE INTO {IDSOwner}.CLM_LN_COB AS T
USING STAGING.IdsFctsClm1Del_CLM_LN_COB_temp AS S
ON T.CLM_COB_LN_SK = S.CLM_COB_LN_SK
WHEN MATCHED THEN
  DELETE;
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

# Select_DNTL_CLM_LN -> DNTL_CLM_LN
extract_query = f"""
SELECT c.DNTL_CLM_LN_SK
FROM {IDSOwner}.DNTL_CLM_LN c,
     {IDSOwner}.W_FCTS_RCRD_DEL d
WHERE d.SUBJ_NM = 'CLM_ID'
  AND c.SRC_SYS_CD_SK = {SourceSK}
  AND d.KEY_VAL = c.CLM_ID
  AND c.LAST_UPDT_RUN_CYC_EXCTN_SK <> {RunCycle}
"""
df_Select_DNTL_CLM_LN = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)
execute_dml("DROP TABLE IF EXISTS STAGING.IdsFctsClm1Del_DNTL_CLM_LN_temp", jdbc_url, jdbc_props)
df_Select_DNTL_CLM_LN.write.jdbc(url=jdbc_url, table="STAGING.IdsFctsClm1Del_DNTL_CLM_LN_temp", mode="overwrite", properties=jdbc_props)
merge_sql = f"""
MERGE INTO {IDSOwner}.DNTL_CLM_LN AS T
USING STAGING.IdsFctsClm1Del_DNTL_CLM_LN_temp AS S
ON T.DNTL_CLM_LN_SK = S.DNTL_CLM_LN_SK
WHEN MATCHED THEN
  DELETE;
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

# Select_CLM_LN_OVRD -> CLM_LN_OVRD
extract_query = f"""
SELECT c.CLM_LN_OVRD_SK
FROM {IDSOwner}.CLM_LN_OVRD c,
     {IDSOwner}.W_FCTS_RCRD_DEL d
WHERE d.SUBJ_NM = 'CLM_ID'
  AND c.SRC_SYS_CD_SK = {SourceSK}
  AND d.KEY_VAL = c.CLM_ID
  AND c.LAST_UPDT_RUN_CYC_EXCTN_SK <> {RunCycle}
"""
df_Select_CLM_LN_OVRD = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)
execute_dml("DROP TABLE IF EXISTS STAGING.IdsFctsClm1Del_CLM_LN_OVRD_temp", jdbc_url, jdbc_props)
df_Select_CLM_LN_OVRD.write.jdbc(url=jdbc_url, table="STAGING.IdsFctsClm1Del_CLM_LN_OVRD_temp", mode="overwrite", properties=jdbc_props)
merge_sql = f"""
MERGE INTO {IDSOwner}.CLM_LN_OVRD AS T
USING STAGING.IdsFctsClm1Del_CLM_LN_OVRD_temp AS S
ON T.CLM_LN_OVRD_SK = S.CLM_LN_OVRD_SK
WHEN MATCHED THEN
  DELETE;
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

# Select_CLM_LN_DIAG -> CLM_LN_DIAG
extract_query = f"""
SELECT c.CLM_LN_DIAG_SK
FROM {IDSOwner}.CLM_LN_DIAG c,
     {IDSOwner}.W_FCTS_RCRD_DEL d
WHERE d.SUBJ_NM = 'CLM_ID'
  AND c.SRC_SYS_CD_SK = {SourceSK}
  AND d.KEY_VAL = c.CLM_ID
  AND c.LAST_UPDT_RUN_CYC_EXCTN_SK <> {RunCycle}
"""
df_Select_CLM_LN_DIAG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)
execute_dml("DROP TABLE IF EXISTS STAGING.IdsFctsClm1Del_CLM_LN_DIAG_temp", jdbc_url, jdbc_props)
df_Select_CLM_LN_DIAG.write.jdbc(url=jdbc_url, table="STAGING.IdsFctsClm1Del_CLM_LN_DIAG_temp", mode="overwrite", properties=jdbc_props)
merge_sql = f"""
MERGE INTO {IDSOwner}.CLM_LN_DIAG AS T
USING STAGING.IdsFctsClm1Del_CLM_LN_DIAG_temp AS S
ON T.CLM_LN_DIAG_SK = S.CLM_LN_DIAG_SK
WHEN MATCHED THEN
  DELETE;
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

# Select_PAYMT_RDUCTN_ACTVTY -> PAYMT_RDUCTN_ACTVTY
extract_query = f"""
SELECT c.PAYMT_RDUCTN_ACTVTY_SK
FROM {IDSOwner}.PAYMT_RDUCTN_ACTVTY c,
     {IDSOwner}.W_FCTS_RCRD_DEL d,
     {IDSOwner}.CLM_OVER_PAYMT x
WHERE d.SUBJ_NM = 'CLM_ID'
  AND x.SRC_SYS_CD_SK = {SourceSK}
  AND d.key_val = x.clm_id
  AND x.PAYMT_RDUCTN_REF_ID = c.PAYMT_RDUCTN_REF_ID
  AND c.LAST_UPDT_RUN_CYC_EXCTN_SK <> {RunCycle}
"""
df_Select_PAYMT_RDUCTN_ACTVTY = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)
execute_dml("DROP TABLE IF EXISTS STAGING.IdsFctsClm1Del_PAYMT_RDUCTN_ACTVTY_temp", jdbc_url, jdbc_props)
df_Select_PAYMT_RDUCTN_ACTVTY.write.jdbc(url=jdbc_url, table="STAGING.IdsFctsClm1Del_PAYMT_RDUCTN_ACTVTY_temp", mode="overwrite", properties=jdbc_props)
merge_sql = f"""
MERGE INTO {IDSOwner}.PAYMT_RDUCTN_ACTVTY AS T
USING STAGING.IdsFctsClm1Del_PAYMT_RDUCTN_ACTVTY_temp AS S
ON T.PAYMT_RDUCTN_ACTVTY_SK = S.PAYMT_RDUCTN_ACTVTY_SK
WHEN MATCHED THEN
  DELETE;
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

# Select_ITS_CLM_MSG -> ITS_CLM_MSG
extract_query = f"""
SELECT c.ITS_CLM_MSG_SK
FROM {IDSOwner}.ITS_CLM_MSG c,
     {IDSOwner}.W_FCTS_RCRD_DEL d
WHERE d.SUBJ_NM = 'CLM_ID'
  AND c.SRC_SYS_CD_SK = {SourceSK}
  AND d.KEY_VAL = c.CLM_ID
  AND c.LAST_UPDT_RUN_CYC_EXCTN_SK <> {RunCycle}
"""
df_Select_ITS_CLM_MSG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)
execute_dml("DROP TABLE IF EXISTS STAGING.IdsFctsClm1Del_ITS_CLM_MSG_temp", jdbc_url, jdbc_props)
df_Select_ITS_CLM_MSG.write.jdbc(url=jdbc_url, table="STAGING.IdsFctsClm1Del_ITS_CLM_MSG_temp", mode="overwrite", properties=jdbc_props)
merge_sql = f"""
MERGE INTO {IDSOwner}.ITS_CLM_MSG AS T
USING STAGING.IdsFctsClm1Del_ITS_CLM_MSG_temp AS S
ON T.ITS_CLM_MSG_SK = S.ITS_CLM_MSG_SK
WHEN MATCHED THEN
  DELETE;
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)