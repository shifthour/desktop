# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 01/28/08 13:39:53 Batch  14638_49197 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 01/10/08 10:01:22 Batch  14620_36086 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 12/26/07 15:33:58 Batch  14605_56043 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/23/07 15:42:37 Batch  14572_56566 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 09/25/07 16:07:19 Batch  14513_58043 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_2 09/18/07 09:05:03 Batch  14506_32892 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 08/23/07 12:38:45 Batch  14480_45530 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_3 03/23/07 14:23:20 Batch  14327_51805 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 03/07/07 16:30:43 Batch  14311_59451 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_6 11/03/06 15:53:42 Batch  14187_57225 PROMOTE bckcett ids20 u10157 sa
# MAGIC ^1_6 11/03/06 15:52:10 Batch  14187_57132 PROMOTE bckcett devlIDS30 u10157 sa
# MAGIC ^1_6 11/03/06 15:50:55 Batch  14187_57057 INIT bckcett testIDS30 u10157 sa
# MAGIC ^1_2 10/09/06 14:14:37 Batch  14162_51294 INIT bckcett testIDS30 dsadm Keith for Sharon
# MAGIC ^1_5 09/28/06 18:22:34 Batch  14151_66266 PROMOTE bckcett testIDS30 u10157 SA
# MAGIC ^1_5 09/28/06 18:10:23 Batch  14151_65432 PROMOTE bckcett devlIDS30 u10157 sa
# MAGIC ^1_5 09/28/06 18:00:51 Batch  14151_64855 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_4 09/27/06 11:08:05 Batch  14150_40087 PROMOTE bckcett devlIDS30 u10157 sa
# MAGIC ^1_4 09/27/06 10:56:54 Batch  14150_39416 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_3 09/26/06 10:33:56 Batch  14149_38040 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_2 09/22/06 12:01:15 Batch  14145_43279 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_1 07/21/06 17:43:36 Batch  14082_63819 INIT bckcett devlIDS30 u10157 sa
# MAGIC 
# MAGIC Â© Copyright 2006 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:  IdsMbrRDSDeleteFromIdsNotOnFile
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
# MAGIC                  IDS_PROD                MBR_RDS
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
# MAGIC 07-06-2006      Sharon Andrew             Initial programming.

# MAGIC IdsMbrDeleteFromIdsNotOnFile
# MAGIC Updates to the MBR_RDS table are by spreadsheet.   Mistakes can happen  and memebers may be removed from the spreadsheet.  have the capability to clear out members that were submitted for the group, submit date, plan year but were not on the last update run cycle.
# MAGIC hf_mbr_rds_extr_delete_keys is created in FctsMbrRetireeRDSExtr and is cleared in this job.
# MAGIC End of IdsMbrDeleteFromIdsNotOnFile
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner', '')
ids_secret_name = get_widget_value('ids_secret_name', '')

# Read from hashed file (Scenario C -> read parquet)
df_hf_mbr_rds_extr_delete_keys = spark.read.parquet(f"{adls_path}/hf_mbr_rds_extr_delete_keys.parquet")

# Transformer: delete_from_ids_mbr_rds_not_on_source
df_delete_from_ids_mbr_rds_not_on_source = df_hf_mbr_rds_extr_delete_keys.select(
    col("GRP_SK").alias("GRP_SK"),
    rpad(col("SUBMT_DT"), 10, " ").alias("SUBMT_DT_SK"),
    rpad(col("PLN_YR_STRT_DT"), 10, " ").alias("PLN_YR_STRT_DT_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

# MBR_RDS (CODBCStage) - Delete logic translated into merge with WHEN MATCHED THEN DELETE
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
execute_dml("DROP TABLE IF EXISTS STAGING.IdsMbrRDSDeleteFromIdsNotOnFile_MBR_RDS_temp", jdbc_url, jdbc_props)
df_delete_from_ids_mbr_rds_not_on_source.write.jdbc(
    url=jdbc_url,
    table="STAGING.IdsMbrRDSDeleteFromIdsNotOnFile_MBR_RDS_temp",
    mode="overwrite",
    properties=jdbc_props
)

merge_sql = f"""
MERGE INTO {IDSOwner}.MBR_RDS AS T
USING STAGING.IdsMbrRDSDeleteFromIdsNotOnFile_MBR_RDS_temp AS S
ON (
    T.GRP_SK = S.GRP_SK
    AND T.SUBMT_DT_SK = S.SUBMT_DT_SK
    AND T.PLN_YR_STRT_DT_SK = S.PLN_YR_STRT_DT_SK
)
WHEN MATCHED AND T.LAST_UPDT_RUN_CYC_EXCTN_SK <> S.LAST_UPDT_RUN_CYC_EXCTN_SK THEN DELETE;
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)