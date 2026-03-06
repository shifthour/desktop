# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC JOB NAME:  FctsPrvcyExtrnlEntyDrvrExtr
# MAGIC CALLED BY:  FctsPrvcyExtrSeq
# MAGIC             
# MAGIC 
# MAGIC PROCESSING:   Extract IDS Privacy Extrnl Enty Change Data Capture keys.  Selects marked record removing duplicates and segragating deletes.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 	Project/Altiris #	                 Change Description	                      Development Project	Code Reviewer	Date Reviewed       
# MAGIC ------------------              --------------------     	------------------------	                 ---------------------------------------------------                   --------------------------------	-------------------------------	----------------------------       
# MAGIC Steph Goddard        11/24/2009	3556 Change Data Capture 	 Original Programming                                           devlIDSnew                    Brent Leland           12/29/2009
# MAGIC                         
# MAGIC Manasa Andru         02/06/2012              TTR - 1272                             Added "FctsCdcOriginID" parameter                IntegrateCurDevl             Brent Leland           02-15-2012
# MAGIC 
# MAGIC Anoop Nair                2022-03-11                  S2S Remediation      Added FACETS DSN Connection parameters            IntegrateDev5	Ken Bradmon	2022-06-09
# MAGIC Vikas Abbu                2022-05-25                  S2S Remediation      removed Dummy src stage and Updated Tgt to
# MAGIC                                                                                                                                               Truncate and Delete            IntegrateDev5

# MAGIC Initiate the Sybase update
# MAGIC Select all rows ordered by time (cdc_id)
# MAGIC Keep last event record for key value
# MAGIC This link is referenced by multiple sequencers for the row count
# MAGIC Load driver table with CDC keys
# MAGIC (TMP_PRVCY_CDC_EE_DRVR$TIMESTAMP)
# MAGIC Delete records to remove from IDS and EDW
# MAGIC DataStage Extract from RepConnect Staging Table (CDC_FHD_ENEN_ENTITY_D_28)
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


# Parameters
TableTimestamp = get_widget_value("TableTimestamp","")
RunID = get_widget_value("RunID","")
SrcSycCdSk = get_widget_value("SrcSycCdSk","")
FacetsOwner = get_widget_value("FacetsOwner","")
facets_secret_name = get_widget_value("facets_secret_name","")
FctsCdcOwner = get_widget_value("FctsCdcOwner","")
fctscdc_secret_name = get_widget_value("fctscdc_secret_name","")
FctsCdcID = get_widget_value("FctsCdcID","")
FctsCdcOriginID = get_widget_value("FctsCdcOriginID","")

# Repl_Extract (ODBCConnector)
jdbc_url, jdbc_props = get_db_config(fctscdc_secret_name)
before_sql_Repl_Extract = f"UPDATE {FctsCdcOwner}.CDC_FHD_ENEN_ENTITY_D_{FctsCdcID}\nSET APP1 = 'E'\nWHERE APP1 IS NULL"
execute_dml(before_sql_Repl_Extract, jdbc_url, jdbc_props)
extract_query_Repl_Extract = f"SELECT ORIGINTS, OP, APP1, ENEN_CKE, CDC_ID FROM {FctsCdcOwner}.CDC_FHD_ENEN_ENTITY_D_{FctsCdcID} WHERE APP1='E' ORDER BY CDC_ID"
df_Repl_Extract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_Repl_Extract)
    .load()
)

# Trans2 (CTransformerStage)
df_Trans2 = df_Repl_Extract.select(
    F.col("ENEN_CKE"),
    F.col("OP")
)

# hf_cdc_enen_dedup (CHashedFileStage) - Scenario A
df_hf_cdc_enen_dedup = df_Trans2.dropDuplicates(["ENEN_CKE"])

# xfmDrvrUpd (CTransformerStage)
df_xfmDrvrUpd_Upd = (
    df_hf_cdc_enen_dedup
    .filter(trim(F.col("OP")) != "D")
    .select(
        F.col("ENEN_CKE"),
        F.col("OP")
    )
)

df_xfmDrvrUpd_DeleteRec = (
    df_hf_cdc_enen_dedup
    .filter(trim(F.col("OP")) == "D")
    .select(
        F.lit(SrcSycCdSk).alias("SRC_SYS_CD_SK"),
        F.col("ENEN_CKE"),
        F.col("OP")
    )
)

# PRVCY_EXTRNL_ENTY_del (CSeqFileStage)
df_PRVCY_EXTRNL_ENTY_del = df_xfmDrvrUpd_DeleteRec.select(
    F.col("SRC_SYS_CD_SK"),
    F.col("ENEN_CKE"),
    F.rpad(F.col("OP"), 1, " ").alias("OP")
)
write_files(
    df_PRVCY_EXTRNL_ENTY_del,
    f"{adls_path}/load/IdsPrvcyExtrnlEnty.del",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

# TMP_PRVCY_CDC_EE_DRVR (ODBCConnector)
df_TMP_PRVCY_CDC_EE_DRVR = df_xfmDrvrUpd_Upd.select(
    F.col("ENEN_CKE"),
    F.rpad(F.col("OP"), 1, " ").alias("OP")
)
execute_dml("DROP TABLE IF EXISTS tempdb.FctsPrvcyExtrnlEntyDrvrExtr_TMP_PRVCY_CDC_EE_DRVR_temp", jdbc_url, jdbc_props)
df_TMP_PRVCY_CDC_EE_DRVR.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "tempdb.FctsPrvcyExtrnlEntyDrvrExtr_TMP_PRVCY_CDC_EE_DRVR_temp") \
    .mode("overwrite") \
    .save()

merge_sql_TMP_PRVCY_CDC_EE_DRVR = """
MERGE tempdb.TMP_PRVCY_CDC_EE_DRVR AS T
USING tempdb.FctsPrvcyExtrnlEntyDrvrExtr_TMP_PRVCY_CDC_EE_DRVR_temp AS S
ON T.ENEN_CKE = S.ENEN_CKE
WHEN MATCHED THEN
    UPDATE SET 
        T.OP = S.OP
WHEN NOT MATCHED THEN
    INSERT (ENEN_CKE, OP) VALUES (S.ENEN_CKE, S.OP);
"""
execute_dml(merge_sql_TMP_PRVCY_CDC_EE_DRVR, jdbc_url, jdbc_props)