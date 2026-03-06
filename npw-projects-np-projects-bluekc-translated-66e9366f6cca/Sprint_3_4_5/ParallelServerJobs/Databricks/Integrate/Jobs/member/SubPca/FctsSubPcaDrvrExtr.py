# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC 
# MAGIC JOB NAME:  FctsSubPcaDrvrExtr
# MAGIC CALLED BY:  FctsSubPcaExtrSeq  
# MAGIC 
# MAGIC 1;hf_cdc_sub_pca_dedup            
# MAGIC 
# MAGIC PROCESSING:   Extract IDS Sub Pca Change Data Capture keys.  Selects marked record removing duplicates and segragating deletes.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 	Project/Altiris #	                 Change Description	                                                           Development Project     	Code Reviewer	Date Reviewed       
# MAGIC ------------------              --------------------     	------------------------	                 ---------------------------------------------------                                                       --------------------------------                  -------------------------------	----------------------------       
# MAGIC Ralph Tucker          2010-01-28	3556 Change Data Capture 	 Original Programming                                                                         IntegrateCurDevl                
# MAGIC Ralph Tucker          2010-03-23              3556 Change Data Capture 	 Added dedup Hash Files                                                                   IntegrateCurDevl            
# MAGIC 
# MAGIC Manasa Andru        02/06/2012               TTR - 1272                           Added 'FctsCdcOriginID' parameter                                                     IntegrateCurDevl                              Brent Leland            02-15-2012
# MAGIC Prabhu ES             2022-03-03                S2S Remediation                  MSSQL ODBC conn params added                                                      IntegrateDev5
# MAGIC Vikas Abbu             2022-05-03                S2S Remediation                  Fixed job errors in FACETS stage                                                         IntegrateDev5	                    Ken Bradmon     	   2022-06-02
# MAGIC 
# MAGIC Vamsi Aripaka         2023-06-05                US584904                          Added FctsCdcDSN, FctsCdcAcct, FctsCdcPW                                    IntegrateDevB                               Reddy Sanam           2023-06-16
# MAGIC                                                                                                               and FctsCdcDB  parameters.

# MAGIC Initiate the Sybase update
# MAGIC Set new rows in staging table to define batch to process
# MAGIC Select all rows ordered by time (cdc_id)
# MAGIC Keep last event record for key value
# MAGIC This link is referenced by multiple sequencers for the row count
# MAGIC Load driver table with CDC keys (TMP_PROD_CDC_SP_DRVR$TIMESTAMP)
# MAGIC Delete records to remove from IDS and EDW
# MAGIC Read hit list in /ids/prod/update/FctsMbrAccumMbrHitList.dat
# MAGIC Lookup member  in Facets for MBR_CK and SBSB_CK
# MAGIC A backup of the daily DRVR table is loaded into ../load/processed directory for troubleshooting
# MAGIC DataStage Extract from RepConnect Staging Table (CMC_SBHS_HSA_ACCUM_28)
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from pyspark.sql.functions import col, lit, length, rpad, trim
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# PARAMETERS
TableTimestamp = get_widget_value("TableTimestamp", "")
RunID = get_widget_value("RunID", "")
SrcSysCd = get_widget_value("SrcSysCd", "FACETS")
SrcSycCdSk = get_widget_value("SrcSycCdSk", "")
FctsCdcID = get_widget_value("$FctsCdcID", "")
FctsCdcOwner = get_widget_value("$FctsCdcOwner", "")
fctscdc_secret_name = get_widget_value("fctscdc_secret_name", "")
FacetsOwner = get_widget_value("$FacetsOwner", "")
facets_secret_name = get_widget_value("facets_secret_name", "")
IDSOwner = get_widget_value("$IDSOwner", "")
ids_secret_name = get_widget_value("ids_secret_name", "")
CurrYear = get_widget_value("CurrYear", "")
FctsCdcOriginID = get_widget_value("$FctsCdcOriginID", "")

# STAGE: MbrCkHitList (CSeqFileStage) - Read
MbrCkHitList_schema = StructType([
    StructField("MEME_CK", IntegerType(), False)
])
df_MbrCkHitList = (
    spark.read.format("csv")
    .option("header", "false")
    .option("quote", "\"")
    .schema(MbrCkHitList_schema)
    .option("mode", "permissive")
    .load(f"{adls_path}/update/FctsMbrAccumMbrHitList.dat")
)

# STAGE: FACETS (ODBCConnector) - Insert input, then SELECT output
# Write df_MbrCkHitList into tempdb.TMP_PROD_CDC_SP_HITDRV
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
execute_dml("DROP TABLE IF EXISTS tempdb.FctsSubPcaDrvrExtr_FACETS_temp", jdbc_url_facets, jdbc_props_facets)
df_MbrCkHitList.write.jdbc(
    url=jdbc_url_facets,
    table="tempdb.FctsSubPcaDrvrExtr_FACETS_temp",
    mode="overwrite",
    properties=jdbc_props_facets
)
merge_sql_facets = """
MERGE tempdb.TMP_PROD_CDC_SP_HITDRV as T
USING tempdb.FctsSubPcaDrvrExtr_FACETS_temp as S
ON T.MEME_CK = S.MEME_CK
WHEN MATCHED THEN
  UPDATE SET T.MEME_CK = S.MEME_CK
WHEN NOT MATCHED THEN
  INSERT (MEME_CK) VALUES (S.MEME_CK);
"""
execute_dml(merge_sql_facets, jdbc_url_facets, jdbc_props_facets)
# Read
df_FACETS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option(
        "query",
        f"SELECT MEME.MEME_CK, SBHS.SBSB_CK, HSAI_ACC_SFX, SBHS_PLAN_YR_DT, SBHS_EFF_DT "
        f"FROM {FacetsOwner}.CMC_MEME_MEMBER MEME, {FacetsOwner}.CMC_SBHS_HSA_ACCUM SBHS, tempdb.TMP_PROD_CDC_SP_HITDRV drvr "
        f"WHERE MEME.SBSB_CK = SBHS.SBSB_CK AND drvr.MEME_CK = MEME.MEME_CK"
    )
    .load()
)

# STAGE: getFacetsUniqKeys (CTransformerStage)
df_getFacetsUniqKeys = (
    df_FACETS.filter(length(trim(col("HSAI_ACC_SFX"))) > 0)
    .withColumn("SBSB_CK", col("SBSB_CK"))
    .withColumn("HSAI_ACC_SFX", col("HSAI_ACC_SFX"))
    .withColumn("SBHS_PLAN_YR_DT", col("SBHS_PLAN_YR_DT"))
    .withColumn("SBHS_EFF_DT", col("SBHS_EFF_DT"))
    .withColumn("OP", lit("U"))
)

# STAGE: hf_sub_pca_dedup2 (CHashedFileStage) - Scenario A: Drop duplicates
df_hf_sub_pca_dedup2 = dedup_sort(
    df_getFacetsUniqKeys,
    ["SBSB_CK", "HSAI_ACC_SFX", "SBHS_PLAN_YR_DT", "SBHS_EFF_DT"],
    [("SBSB_CK", "A")]
)

# STAGE: SYSDUMMY1 (DB2Connector) - Read
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
df_SYSDUMMY1 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", "SELECT IBMREQD FROM sysibm.sysdummy1")
    .load()
)

# STAGE: Trans1 (CTransformerStage)
df_Trans1 = df_SYSDUMMY1.select(col("IBMREQD").alias("APP1"))

# STAGE: Repl_Extract (ODBCConnector) - Update SQL then SELECT
jdbc_url_repl_extract, jdbc_props_repl_extract = get_db_config(fctscdc_secret_name)
update_sql_repl_extract = f"""
UPDATE {FctsCdcOwner}.CDC_CMC_SBHS_HSA_ACCUM_{FctsCdcID}
SET APP1 = 'E'
FROM {FctsCdcOwner}.CDC_CMC_SBHS_HSA_ACCUM_{FctsCdcID} cdc,
     {FacetsOwner}.rs_lastcommit lc
WHERE cdc.APP1 IS NULL
      AND cdc.ORIGINTS < lc.origin_time
      AND lc.origin = {FctsCdcOriginID}
"""
execute_dml(update_sql_repl_extract, jdbc_url_repl_extract, jdbc_props_repl_extract)
df_Repl_Extract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_repl_extract)
    .options(**jdbc_props_repl_extract)
    .option(
        "query",
        f"SELECT ORIGINTS, OP, APP1, SBSB_CK, HSAI_ACC_SFX, SBHS_PLAN_YR_DT, SBHS_EFF_DT, CDC_ID "
        f"FROM {FctsCdcOwner}.CDC_CMC_SBHS_HSA_ACCUM_{FctsCdcID} "
        f"WHERE APP1 = 'E' "
        f"ORDER BY CDC_ID"
    )
    .load()
)

# STAGE: Trans2 (CTransformerStage)
df_Trans2 = df_Repl_Extract.select(
    col("SBSB_CK").alias("SBSB_CK"),
    col("HSAI_ACC_SFX").alias("HSAI_ACC_SFX"),
    col("SBHS_PLAN_YR_DT").alias("SBHS_PLAN_YR_DT"),
    col("SBHS_EFF_DT").alias("SBHS_EFF_DT"),
    col("OP").alias("OP")
)

# STAGE: hf_sub_pca_dedup1 (CHashedFileStage) - Scenario A: Drop duplicates
df_hf_sub_pca_dedup1 = dedup_sort(
    df_Trans2,
    ["SBSB_CK", "HSAI_ACC_SFX", "SBHS_PLAN_YR_DT", "SBHS_EFF_DT"],
    [("SBSB_CK", "A")]
)

# STAGE: Link_Collector_467 (CCollector)
df_Link_Collector_467 = df_hf_sub_pca_dedup1.select(
    "SBSB_CK", "HSAI_ACC_SFX", "SBHS_PLAN_YR_DT", "SBHS_EFF_DT", "OP"
).union(
    df_hf_sub_pca_dedup2.select("SBSB_CK", "HSAI_ACC_SFX", "SBHS_PLAN_YR_DT", "SBHS_EFF_DT", "OP")
)

# STAGE: hf_sub_pca_dedup3 (CHashedFileStage) - Scenario A: Drop duplicates
df_hf_sub_pca_dedup3 = dedup_sort(
    df_Link_Collector_467,
    ["SBSB_CK", "HSAI_ACC_SFX", "SBHS_PLAN_YR_DT", "SBHS_EFF_DT"],
    [("SBSB_CK", "A")]
)

# STAGE: xfmDrvrUpd (CTransformerStage)
df_filter = df_hf_sub_pca_dedup3

df_Upd = (
    df_filter.filter(trim(col("OP")) != "D")
    .select(
        col("SBSB_CK").alias("SBSB_CK"),
        col("HSAI_ACC_SFX").alias("HSAI_ACC_SFX"),
        col("SBHS_PLAN_YR_DT").alias("SBHS_PLAN_YR_DT"),
        col("SBHS_EFF_DT").alias("SBHS_EFF_DT"),
        col("OP").alias("OP")
    )
)

df_DeleteRec = (
    df_filter.filter(trim(col("OP")) == "D")
    .select(
        lit(SrcSycCdSk).alias("SRC_SYS_CD_SK"),
        col("SBSB_CK").alias("SBSB_CK"),
        col("HSAI_ACC_SFX").alias("HSAI_ACC_SFX"),
        col("SBHS_PLAN_YR_DT").alias("SBHS_PLAN_YR_DT"),
        col("SBHS_EFF_DT").alias("SBHS_EFF_DT"),
        col("OP").alias("OP")
    )
)

df_DrvrBkup = (
    df_filter.filter(trim(col("OP")) != "D")
    .select(
        col("SBSB_CK").alias("SBSB_CK"),
        col("HSAI_ACC_SFX").alias("HSAI_ACC_SFX"),
        col("SBHS_PLAN_YR_DT").alias("SBHS_PLAN_YR_DT"),
        col("SBHS_EFF_DT").alias("SBHS_EFF_DT"),
        col("OP").alias("OP")
    )
)

# STAGE: SUB_PCA_del (CSeqFileStage) - Write
df_DeleteRec_final = df_DeleteRec.select(
    "SRC_SYS_CD_SK",
    "SBSB_CK",
    "HSAI_ACC_SFX",
    "SBHS_PLAN_YR_DT",
    "SBHS_EFF_DT",
    "OP"
).withColumn("HSAI_ACC_SFX", rpad("HSAI_ACC_SFX", 4, " ")).withColumn("OP", rpad("OP", 1, " "))
write_files(
    df_DeleteRec_final,
    f"{adls_path}/load/IdsSubPca.del",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# STAGE: DrvrSeq (CSeqFileStage) - Write
df_DrvrBkup_final = df_DrvrBkup.select(
    "SBSB_CK",
    "HSAI_ACC_SFX",
    "SBHS_PLAN_YR_DT",
    "SBHS_EFF_DT",
    "OP"
).withColumn("HSAI_ACC_SFX", rpad("HSAI_ACC_SFX", 4, " ")).withColumn("OP", rpad("OP", 1, " "))
write_files(
    df_DrvrBkup_final,
    f"{adls_path}/load/processed/IdsSubPcaAccumDrvrBkup.seq.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# STAGE: TMP_PROD_CDC_SP_DRVR (ODBCConnector) - Truncate + Insert
jdbc_url_tmp_prod_cdc_sp_drvr, jdbc_props_tmp_prod_cdc_sp_drvr = get_db_config(fctscdc_secret_name)
execute_dml("TRUNCATE TABLE tempdb.TMP_PROD_CDC_SP_DRVR", jdbc_url_tmp_prod_cdc_sp_drvr, jdbc_props_tmp_prod_cdc_sp_drvr)
df_Upd_final = df_Upd.select(
    "SBSB_CK",
    "HSAI_ACC_SFX",
    "SBHS_PLAN_YR_DT",
    "SBHS_EFF_DT",
    "OP"
).withColumn("HSAI_ACC_SFX", rpad("HSAI_ACC_SFX", 4, " ")).withColumn("OP", rpad("OP", 1, " "))
df_Upd_final.write.jdbc(
    url=jdbc_url_tmp_prod_cdc_sp_drvr,
    table="tempdb.TMP_PROD_CDC_SP_DRVR",
    mode="append",
    properties=jdbc_props_tmp_prod_cdc_sp_drvr
)