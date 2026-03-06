# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC JOB NAME:  FctsPrvcyExtrnlMbrPmedDrvrExtr
# MAGIC CALLED BY:  FctsPrvcyExtrSeq
# MAGIC             
# MAGIC 
# MAGIC PROCESSING:   Extract IDS Privacy Extrnl Mbr Change Data Capture keys.  Selects marked record removing duplicates and segragating deletes.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 	Project/Altiris #	                 Change Description	         Development Project	Code Reviewer	Date Reviewed       
# MAGIC ------------------              --------------------     	------------------------	                 ---------------------------------------------------     --------------------------------	-------------------------------	----------------------------       
# MAGIC Steph Goddard        11/24/09 	3556 Change Data Capture 	 Original Programming                       devlIDSnew                    Brent/Steph             12/29/2009
# MAGIC Ralph Tucker          2010-04-06              3556 CDS                                 Ouput to flat file for merge with        IntegrateCurDevl             Brent Leland             04/07/2010
# MAGIC                                                                                                                  the ENEN flat driver file
# MAGIC 
# MAGIC Manasa Andru         02/06/2012             TTR - 1272                       Added 'FctsCdcOriginID' parameter          IntegrateCurDevl           Brent Leland             02-15-2012
# MAGIC 
# MAGIC Anoop Nair                2022-03-11         S2S Remediation      Added FACETS DSN Connection parameters      IntegrateDev5		Ken Bradmon	2022-06-03

# MAGIC Initiate the Sybase update
# MAGIC Set new rows in staging table to define batch to process
# MAGIC Select all rows ordered by time (cdc_id)
# MAGIC Keep last event record for key value
# MAGIC This link is referenced by multiple sequencers for the row count
# MAGIC Load driver table flat file with CDC keys
# MAGIC Delete records to remove from IDS and EDW
# MAGIC DataStage Extract from RepConnect Staging Table (CDC_FHP_PMED_MEMBER_D_28)
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


# Parameters
TableTimestamp = get_widget_value('TableTimestamp','')
SrcSycCdSk = get_widget_value('SrcSycCdSk','')
FacetsOwner = get_widget_value('FacetsOwner','')
FctsCdcOwner = get_widget_value('FctsCdcOwner','')
FctsCdcID = get_widget_value('FctsCdcID','')
FctsCdcOriginID = get_widget_value('FctsCdcOriginID','')

# Because we see $FctsCdcOwner, add corresponding secret parameter
fctscdc_secret_name = get_widget_value('fctscdc_secret_name','')

# Because the first stage references IDS, add its secret parameter
ids_secret_name = get_widget_value('ids_secret_name','')

# Stage: SYSDUMMY1 (DB2Connector) reading from IDS
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
df_SYSDUMMY1 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", "SELECT IBMREQD  FROM sysibm.sysdummy1")
    .load()
)

# Stage: Trans1 (CTransformerStage)
df_Trans1 = df_SYSDUMMY1.select(
    F.col("IBMREQD").alias("APP1")
)

# Stage: Repl_Extract (ODBCConnector) - Update then Select from #$FctsCdcOwner#.CDC_FHP_PMED_MEMBER_D_#$FctsCdcID#
jdbc_url_fctscdc, jdbc_props_fctscdc = get_db_config(fctscdc_secret_name)

update_sql = (
    f"UPDATE {FctsCdcOwner}.CDC_FHP_PMED_MEMBER_D_{FctsCdcID}\n"
    "SET APP1 = 'E'\n"
    "WHERE APP1 IS NULL"
)
execute_dml(update_sql, jdbc_url_fctscdc, jdbc_props_fctscdc)

extract_query = (
    f"SELECT ORIGINTS, OP, APP1, PMED_CKE, CDC_ID "
    f"FROM {FctsCdcOwner}.CDC_FHP_PMED_MEMBER_D_{FctsCdcID} "
    "WHERE APP1 = 'E' ORDER BY CDC_ID"
)
df_Repl_Extract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_fctscdc)
    .options(**jdbc_props_fctscdc)
    .option("query", extract_query)
    .load()
)

# Stage: Trans2 (CTransformerStage)
df_Trans2 = df_Repl_Extract.select(
    F.col("PMED_CKE").alias("PMED_CKE"),
    F.col("OP").alias("OP")
)

# Stage: hf_cdc_pmed_dedup (CHashedFileStage), Scenario A => deduplicate on key columns [PMED_CKE]
df_hf_cdc_pmed_dedup = df_Trans2.dropDuplicates(["PMED_CKE"])

# Stage: xfmDrvrUpd (CTransformerStage) => Two output links
df_xfmDrvrUpd_Upd = df_hf_cdc_pmed_dedup.filter(trim(F.col("OP")) != 'D').select(
    F.col("PMED_CKE").alias("PMED_CKE"),
    F.col("OP").alias("OP")
)

df_xfmDrvrUpd_DeleteRec = df_hf_cdc_pmed_dedup.filter(trim(F.col("OP")) == 'D').select(
    F.lit(SrcSycCdSk).alias("SRC_SYS_CD_SK"),
    F.col("PMED_CKE").alias("PMED_CKE"),
    F.col("OP").alias("OP")
)

# Stage: PRVCY_EXTRNL_MBR_del (CSeqFileStage)
df_final_del = (
    df_xfmDrvrUpd_DeleteRec
    .withColumn("OP", F.rpad(F.col("OP"), 1, " "))
    .select("SRC_SYS_CD_SK", "PMED_CKE", "OP")
)
write_files(
    df_final_del,
    f"{adls_path}/load/IdsPrvcyExtrnlMbr.del",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)

# Stage: TMP_PRVCY_CDC_PMED (CSeqFileStage)
df_final_upd = (
    df_xfmDrvrUpd_Upd
    .withColumn("OP", F.rpad(F.col("OP"), 1, " "))
    .select("PMED_CKE", "OP")
)
write_files(
    df_final_upd,
    f"{adls_path_raw}/landing/TMP_PRVCY_CDC_PMED.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)