# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_2 03/10/09 09:25:21 Batch  15045_33943 INIT bckcetl ids20 dcg01 sa Bringing ALL Claim code down to devlIDS
# MAGIC ^1_4 02/10/09 11:16:45 Batch  15017_40611 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_3 02/02/09 09:52:49 Batch  15009_35578 PROMOTE bckcetl ids20 dsadm bls for brent
# MAGIC ^1_3 01/26/09 13:03:15 Batch  15002_46999 INIT bckcett testIDS dsadm bls for bl
# MAGIC ^1_1 12/16/08 10:00:18 Batch  14961_36031 PROMOTE bckcett testIDS u03651 steph for Brent - claim primary keying
# MAGIC ^1_1 12/16/08 09:15:37 Batch  14961_33343 INIT bckcett devlIDS u03651 steffy
# MAGIC ^1_1 01/29/08 08:55:59 Batch  14639_32164 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 12/28/07 10:13:19 Batch  14607_36806 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/26/07 09:37:10 Batch  14575_34636 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_2 08/30/07 07:19:35 Batch  14487_26382 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 08/27/07 10:06:05 Batch  14484_36371 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_2 07/18/07 15:56:28 Batch  14444_57395 PROMOTE bckcetl ids20 dsadm bls for hs
# MAGIC ^1_2 07/18/07 15:52:38 Batch  14444_57163 INIT bckcett testIDSnew dsadm bls for hs
# MAGIC ^1_3 03/26/07 13:01:55 Batch  14330_46919 PROMOTE bckcett testIDSnew dsadm dsadm
# MAGIC ^1_3 03/26/07 12:34:37 Batch  14330_45286 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 03/07/07 16:30:43 Batch  14311_59451 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_2 03/07/07 15:24:05 Batch  14311_55447 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_1 12/31/06 18:07:08 Batch  14245_65258 PROMOTE bckcetl ids20 dsadm julien for ralph
# MAGIC ^1_1 12/31/06 17:54:18 Batch  14245_64556 INIT bckcett testIDS30 dsadm julien for ralph
# MAGIC ^1_1 12/30/06 10:35:44 Batch  14244_38150 PROMOTE bckcett testIDS30 u06640 Ralph
# MAGIC ^1_1 12/30/06 10:33:21 Batch  14244_38008 PROMOTE bckcett testIDS30 u06640 Ralph
# MAGIC ^1_1 12/30/06 10:30:50 Batch  14244_37857 INIT bckcett devlIDS30 u06640 Ralph
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsClmPcaBaseUpdt
# MAGIC 
# MAGIC DESCRIPTION:     Update the IDS claim table with the greates CLM_ID for the REL_BASE_CLM_SK.
# MAGIC       
# MAGIC 
# MAGIC INPUTS:  #$FilePath#/load/CLM_PCA_UPDT.dat - output from IdsClmFkey
# MAGIC 	
# MAGIC   
# MAGIC HASH FILES:     hf_pca_clm_updt
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                            
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC             12/30/2006   Ralph Tucker  -   Originally Programmed
# MAGIC             07/17/2007   Ralh Tucker    -   Added (Appended) hit lists for Argus PCA ClaimsMart & EDW.               code review Steph Goddard    07/18/07

# MAGIC Update W_PCA_CLM_DRVR with CLM_SK to be updated in IDS
# MAGIC File Create in IdsClmFkey
# MAGIC Keep only last (Greatest CLM_ID)
# MAGIC Update the Rel_Pca_Clm_Sk with the CLM_SK for the greatest Clm_Id
# MAGIC Argus PCA Drug Hit File
# MAGIC Argus PCA Drug EDW Hit File
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import pyspark.sql.functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

# ----------------------------------------------------------------------------
# ClmPcaUpdts (StageType=CSeqFileStage) - Read the file CLM_PCA_UPDT.dat
# ----------------------------------------------------------------------------
schema_ClmPcaUpdts = StructType([
    StructField("REL_BASE_CLM_SK", IntegerType(), nullable=False),
    StructField("CLM_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False)
])
df_ClmPcaUpdts = (
    spark.read
    .option("delimiter", ",")
    .option("quote", "\"")
    .schema(schema_ClmPcaUpdts)
    .csv(f"{adls_path}/load/CLM_PCA_UPDT.dat")
)

# ----------------------------------------------------------------------------
# IDSClm_for_Pca_Update (StageType=DB2Connector, with "WHERE CLM.CLM_SK=?")
# Implemented by writing primary link (df_ClmPcaUpdts) to a staging table,
# then joining with {IDSOwner}.CLM on CLM_SK = REL_BASE_CLM_SK
# ----------------------------------------------------------------------------
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
execute_dml("DROP TABLE IF EXISTS STAGING.IdsClmPcaBaseUpdt_IDSClm_for_Pca_Update_temp", jdbc_url, jdbc_props)
df_ClmPcaUpdts.write.jdbc(
    url=jdbc_url,
    table="STAGING.IdsClmPcaBaseUpdt_IDSClm_for_Pca_Update_temp",
    mode="append",
    properties=jdbc_props
)
extract_query_IdsClm_for_Pca_Update = (
    f"SELECT c.CLM_SK, c.SRC_SYS_CD_SK, c.CLM_ID "
    f"FROM {IDSOwner}.CLM c "
    f"JOIN STAGING.IdsClmPcaBaseUpdt_IDSClm_for_Pca_Update_temp p "
    f"ON c.CLM_SK = p.REL_BASE_CLM_SK"
)
df_Clm2 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_IdsClm_for_Pca_Update)
    .load()
)

# ----------------------------------------------------------------------------
# Trans1 (StageType=CTransformerStage, left-join logic "IsNull(Clm2.CLM_SK)=false")
# The output link "updtCLAIM" selects columns from df_Clm2
# ----------------------------------------------------------------------------
df_updtCLAIM = df_Clm2.select(
    F.col("CLM_SK"),
    F.col("SRC_SYS_CD_SK"),
    F.col("CLM_ID")
)

# ----------------------------------------------------------------------------
# hf_clm_elim_dups (StageType=CHashedFileStage) - Scenario A
# Deduplicate on key column CLM_SK
# ----------------------------------------------------------------------------
df_updtCLAIM_deduped = dedup_sort(df_updtCLAIM, ["CLM_SK"], [])

# ----------------------------------------------------------------------------
# Transformer_163 (CTransformerStage)
# Outputs:
#   1) DeDupedClms (CLM_SK)
#   2) DSLink166 (CLM_SK, SRC_SYS_CD_SK, CLM_ID)
#   3) ArgusPCADrugEDWHitList (CLM_SK, SRC_SYS_CD_SK, CLM_ID)
# ----------------------------------------------------------------------------
df_DeDupedClms = df_updtCLAIM_deduped.select(
    F.col("CLM_SK").alias("CLM_SK")
)
df_DSLink166 = df_updtCLAIM_deduped.select(
    F.col("CLM_SK").alias("CLM_SK"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID")
)
df_ArgusPCADrugEDWHitList = df_updtCLAIM_deduped.select(
    F.col("CLM_SK").alias("CLM_SK"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID")
)

# ----------------------------------------------------------------------------
# ArgusPCADrugHitList (StageType=CSeqFileStage) - Write to update/IdsClmMartHitList.dat
# ----------------------------------------------------------------------------
df_DSLink166_enriched = df_DSLink166.withColumn(
    "CLM_ID", F.rpad(F.col("CLM_ID"), <...>, " ")
).select("CLM_SK", "SRC_SYS_CD_SK", "CLM_ID")
write_files(
    df_DSLink166_enriched,
    f"{adls_path}/update/IdsClmMartHitList.dat",
    ",",
    "append",
    False,
    False,
    "\"",
    None
)

# ----------------------------------------------------------------------------
# ArgusPCADrugEDWHitList (StageType=CSeqFileStage) - Write to edw/update/IdsClmHitList.dat
# ----------------------------------------------------------------------------
df_ArgusPCADrugEDWHitList_enriched = df_ArgusPCADrugEDWHitList.withColumn(
    "CLM_ID", F.rpad(F.col("CLM_ID"), <...>, " ")
).select("CLM_SK", "SRC_SYS_CD_SK", "CLM_ID")
write_files(
    df_ArgusPCADrugEDWHitList_enriched,
    f"{adls_path}/edw/update/IdsClmHitList.dat",
    ",",
    "append",
    False,
    False,
    "\"",
    None
)

# ----------------------------------------------------------------------------
# PcaClms (StageType=DB2Connector, WriteMode=Insert)
# Merges df_DeDupedClms into {IDSOwner}.w_pca_clm_drvr and reads AllClmsWithBaseSk
# ----------------------------------------------------------------------------
execute_dml(
    "DROP TABLE IF EXISTS STAGING.IdsClmPcaBaseUpdt_PcaClms_temp",
    jdbc_url,
    jdbc_props
)
df_DeDupedClms.write.jdbc(
    url=jdbc_url,
    table="STAGING.IdsClmPcaBaseUpdt_PcaClms_temp",
    mode="append",
    properties=jdbc_props
)
merge_sql_pcaClms = f"""
MERGE INTO {IDSOwner}.w_pca_clm_drvr as target
USING STAGING.IdsClmPcaBaseUpdt_PcaClms_temp as source
ON target.CLM_SK = source.CLM_SK
WHEN MATCHED THEN
  UPDATE SET target.CLM_SK = source.CLM_SK
WHEN NOT MATCHED THEN
  INSERT (CLM_SK) VALUES (source.CLM_SK);
"""
execute_dml(merge_sql_pcaClms, jdbc_url, jdbc_props)
extract_query_pcaClms = f"""
SELECT b.rel_base_clm_sk, b.clm_id, b.clm_sk
FROM {IDSOwner}.clm b
JOIN {IDSOwner}.w_pca_clm_drvr drvr
ON b.rel_base_clm_sk = drvr.clm_sk
ORDER BY b.clm_id asc
"""
df_AllClmsWithBaseSk = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_pcaClms)
    .load()
)

# ----------------------------------------------------------------------------
# hf_pca_clm_updt (StageType=CHashedFileStage) - Scenario A
# Deduplicate on key column REL_BASE_CLM_SK
# ----------------------------------------------------------------------------
df_AllClmsWithBaseSk_deduped = dedup_sort(df_AllClmsWithBaseSk, ["REL_BASE_CLM_SK"], [])

# ----------------------------------------------------------------------------
# Trans2 (CTransformerStage)
# Columns: CLM_SK = GreatestClmIds.REL_BASE_CLM_SK, REL_PCA_CLM_SK = GreatestClmIds.CLM_SK
# ----------------------------------------------------------------------------
df_updtCLAIM2 = df_AllClmsWithBaseSk_deduped.select(
    F.col("REL_BASE_CLM_SK").alias("CLM_SK"),
    F.col("CLM_SK").alias("REL_PCA_CLM_SK")
)

# ----------------------------------------------------------------------------
# PcaClmUpdate (StageType=DB2Connector, WriteMode=Update)
# Merge into {IDSOwner}.CLM, updating REL_PCA_CLM_SK
# ----------------------------------------------------------------------------
execute_dml(
    "DROP TABLE IF EXISTS STAGING.IdsClmPcaBaseUpdt_PcaClmUpdate_temp",
    jdbc_url,
    jdbc_props
)
df_updtCLAIM2.write.jdbc(
    url=jdbc_url,
    table="STAGING.IdsClmPcaBaseUpdt_PcaClmUpdate_temp",
    mode="append",
    properties=jdbc_props
)
merge_sql_PcaClmUpdate = f"""
MERGE INTO {IDSOwner}.CLM as target
USING STAGING.IdsClmPcaBaseUpdt_PcaClmUpdate_temp as source
ON target.CLM_SK = source.CLM_SK
WHEN MATCHED THEN
  UPDATE SET target.REL_PCA_CLM_SK = source.REL_PCA_CLM_SK
WHEN NOT MATCHED THEN
  INSERT (CLM_SK, REL_PCA_CLM_SK) VALUES (source.CLM_SK, source.REL_PCA_CLM_SK);
"""
execute_dml(merge_sql_PcaClmUpdate, jdbc_url, jdbc_props)