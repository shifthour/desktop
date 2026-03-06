# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC JOB NAME:  FctsPrvcyExtrnlMbrEnenDrvrExtr
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
# MAGIC                                                                                                                  the Pmed flat driver file
# MAGIC 
# MAGIC Manasa Andru        02/06/2012              TTR - 1272                               Added 'FctsCdcOriginID' parameter      IntegrateCurDevl       Brent Leland            02-15-2012        
# MAGIC 
# MAGIC Anoop Nair                2022-03-11         S2S Remediation      Added FACETS DSN Connection parameters      IntegrateDev5           	Ken Bradmon	2022-06-03

# MAGIC Initiate the Sybase update
# MAGIC Set new rows in staging table to define batch to process
# MAGIC Select all rows ordered by time (cdc_id)
# MAGIC Keep last event record for key value
# MAGIC This link is referenced by multiple sequencers for the row count
# MAGIC Load driver table flat file with CDC keys
# MAGIC Delete records to remove from IDS and EDW
# MAGIC DataStage Extract from RepConnect Staging Table (CDC_FHP_ENEN_MEMBER_D_28)
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')  # Not explicitly in parameters, included per guideline if needed
ids_secret_name = get_widget_value('ids_secret_name','')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
FctsCdcOwner = get_widget_value('FctsCdcOwner','')
fctscdc_secret_name = get_widget_value('fctscdc_secret_name','')
FctsCdcID = get_widget_value('FctsCdcID','')
FctsCdcOriginID = get_widget_value('FctsCdcOriginID','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
TableTimestamp = get_widget_value('TableTimestamp','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

df_SYSDUMMY1 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", "SELECT IBMREQD FROM sysibm.sysdummy1")
    .load()
)

df_Trans1 = df_SYSDUMMY1.select(col("IBMREQD").alias("APP1"))

update_sql_Repl_Extract = f"UPDATE {FctsCdcOwner}.CDC_FHD_ENEN_ENTITY_D_{FctsCdcID}\n\nSET APP1 = 'E'\n\nWHERE APP1 IS NULL"
execute_dml(update_sql_Repl_Extract, *get_db_config(fctscdc_secret_name))

select_sql_Repl_Extract = f"SELECT ORIGINTS, OP, APP1, ENEN_CKE AS PMED_CKE, CDC_ID FROM {FctsCdcOwner}.CDC_FHD_ENEN_ENTITY_D_{FctsCdcID} WHERE APP1 = 'E' ORDER BY CDC_ID"
df_Repl_Extract = (
    spark.read.format("jdbc")
    .option("url", get_db_config(fctscdc_secret_name)[0])
    .options(**get_db_config(fctscdc_secret_name)[1])
    .option("query", select_sql_Repl_Extract)
    .load()
)

df_Trans2 = df_Repl_Extract.select("PMED_CKE", "OP")

df_hf_cdc_enen_dedup = df_Trans2.dropDuplicates(["PMED_CKE"])

df_upd = df_hf_cdc_enen_dedup.filter(trim(col("OP")) != lit("D")).select(
    col("PMED_CKE"),
    col("OP")
)

df_del = df_hf_cdc_enen_dedup.filter(trim(col("OP")) == lit("D")).withColumn(
    "SRC_SYS_CD_SK", lit(SrcSysCdSk)
).select(
    "SRC_SYS_CD_SK",
    "PMED_CKE",
    "OP"
)

df_del_final = df_del.withColumn("OP", rpad(col("OP"), 1, " "))
df_del_final = df_del_final.select("SRC_SYS_CD_SK", "PMED_CKE", "OP")
write_files(
    df_del_final,
    f"{adls_path}/load/IdsPrvcyExtrnlMbr.del",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_upd_final = df_upd.withColumn("OP", rpad(col("OP"), 1, " "))
df_upd_final = df_upd_final.select("PMED_CKE", "OP")
write_files(
    df_upd_final,
    f"{adls_path_raw}/landing/TMP_PRVCY_CDC_ENEN.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)