# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_2 03/10/09 09:25:21 Batch  15045_33943 INIT bckcetl ids20 dcg01 sa Bringing ALL Claim code down to devlIDS
# MAGIC ^1_1 03/02/09 10:47:54 Batch  15037_38896 PROMOTE bckcetl ids20 dsadm rc for brent 
# MAGIC ^1_1 03/02/09 10:41:44 Batch  15037_38516 INIT bckcett testIDS dsadm rc for brent 
# MAGIC ^1_1 02/13/09 15:51:39 Batch  15020_57105 PROMOTE bckcett testIDS u08717 Brent
# MAGIC ^1_1 02/13/09 15:37:03 Batch  15020_56227 INIT bckcett devlIDS u08717 Brent
# MAGIC 
# MAGIC 
# MAGIC COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY: IDSPurgeClaimCntl
# MAGIC 
# MAGIC 
# MAGIC PROCESSING: Select claim ID and source code from CLM table based on status date to build driver table for delete process. 
# MAGIC  
# MAGIC                           Source systems excluded from the purge are CAREADVANCE, MOHSAIC, EDC, OT@2, ADOL
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Brent Leland            2008-08-26       3574 IDS Purge    Original Programming.                                                                   devlIDS                          Steph Goddard          01/09/2009

# MAGIC Build list of claims to purge from Claim Datamart
# MAGIC Select claims based on status date older then purge date.
# MAGIC Link count is retrieved, don't change names
# MAGIC Backup of claims deleted
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


PurgeDate = get_widget_value('PurgeDate','')
SrcExclSK = get_widget_value('SrcExclSK','')
RunID = get_widget_value('RunID','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = f"SELECT SRC_SYS_CD_SK as SRC_SYS_CD_SK, CLM_ID as CLM_ID FROM {IDSOwner}.CLM WHERE STTUS_DT_SK < '{PurgeDate}' AND SRC_SYS_CD_SK not in ({SrcExclSK})"
df_CLM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)
df_ClaimID = df_CLM

df_Trans1_ClmID = df_ClaimID.select(
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID")
)
df_Trans1_Backup = df_ClaimID.select(
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID")
)

execute_dml("DROP TABLE IF EXISTS STAGING.IDSPurgeClaimDriverExtr_W_CLM_PURGE_temp", jdbc_url, jdbc_props)
df_Trans1_ClmID.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.IDSPurgeClaimDriverExtr_W_CLM_PURGE_temp") \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO {IDSOwner}.W_CLM_PURGE AS T
USING STAGING.IDSPurgeClaimDriverExtr_W_CLM_PURGE_temp AS S
ON T.SRC_SYS_CD_SK = S.SRC_SYS_CD_SK AND T.CLM_ID = S.CLM_ID
WHEN MATCHED THEN UPDATE SET
    T.SRC_SYS_CD_SK = S.SRC_SYS_CD_SK,
    T.CLM_ID = S.CLM_ID
WHEN NOT MATCHED THEN
    INSERT (SRC_SYS_CD_SK, CLM_ID)
    VALUES (S.SRC_SYS_CD_SK, S.CLM_ID);
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

execute_dml(f"CALL SYSPROC.ADMIN_CMD('runstats on table {IDSOwner}.W_CLM_PURGE on key columns with distribution on key columns and detailed indexes all allow write access')", jdbc_url, jdbc_props)

df_for_purge_clm = df_Trans1_Backup.withColumn("CLM_ID", F.rpad(F.col("CLM_ID"), 20, " "))
df_for_purge_clm = df_for_purge_clm.select("SRC_SYS_CD_SK", "CLM_ID")

write_files(
    df_for_purge_clm,
    f"{adls_path}/load/processed/W_CLM_PURGE.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote='"',
    nullValue=None
)