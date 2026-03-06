# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC Â© Copyright 2020  Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC ZENA JOBNAME:  OPTUMRX_IDS_EDW_DRUG_CLM_CMPND_INGR_DAILY_000
# MAGIC 
# MAGIC CALLED BY:  OptumIDSCompoundsSeq
# MAGIC 
# MAGIC JOB NAME:  OptumIDSCompoundsPkey
# MAGIC 
# MAGIC Description:  Job checks for the existing primary key in the K table K_DRUG_CLM_CMPND_INGR and creates a surrogate key for new rows.
# MAGIC 
# MAGIC Modifications:
# MAGIC 
# MAGIC Developer            Date            Project/User Story             Change Description                                                                Development Project    Code Reviewer            Date Reviewed
# MAGIC -------------------------   -----------------   ----------------------------------------  --------------------------------------------------------------------------------------------   ----------------------------------   ---------------------------------   -------------------------
# MAGIC Bill Schroeder      2020-09-30  F-209426/US217375         Copied from PdxRbtQtrlySumPkey project EnterpriseDev2.    IntegrateDev2 
# MAGIC Bill Schroeder      2021-09-22  US334499\(9)          Removed DOSE_FORM_SK column for new table layout.     IntegrateDev2            Jaideep Mankala         12/07/2021

# MAGIC Job checks for the existing primary key in the K_DRUG_CLM_CMPND_INGR table and creates a surrogate key for new rows. Job called from OptumIDSCompoundsSeq.
# MAGIC Remove the duplicates based on the natural key fields
# MAGIC New Pkey is Generated in the Transformer stage, A DB sequencer is used to generate the PKey.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame, functions as F, types as T, Column
from pyspark.sql.types import StructType, StructField, StringType
# COMMAND ----------
# MAGIC %run ../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
RunID = get_widget_value('RunID','')
ids_secret_name = get_widget_value('ids_secret_name','')

# --------------------------------------------------------------------------------
# Read from DB2ConnectorPX: db_K_DRUG_CLM_CMPND_INGR_read
# --------------------------------------------------------------------------------
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query_db_K_DRUG_CLM_CMPND_INGR_read = f"SELECT CLM_ID, DRUG_CLM_CMPND_INGR_SEQ_NO, SRC_SYS_CD_SK, CRT_RUN_CYC_EXCTN_SK, DRUG_CLM_CMPND_INGR_SK FROM {IDSOwner}.K_DRUG_CLM_CMPND_INGR"
df_db_K_DRUG_CLM_CMPND_INGR_read = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db_K_DRUG_CLM_CMPND_INGR_read)
    .load()
)

# --------------------------------------------------------------------------------
# Read from PxDataSet: Drug_Clm_Cmpnd_Ingr_PKey (DRUG_CLM_CMPND_INGR_PKey.#RunID#.ds -> Parquet)
# --------------------------------------------------------------------------------
schema_Drug_Clm_Cmpnd_Ingr_PKey = StructType([
    StructField("DRUG_CLM_CMPND_INGR_SK", StringType(), True),
    StructField("CLM_ID", StringType(), True),
    StructField("DRUG_CLM_CMPND_INGR_SEQ_NO", StringType(), True),
    StructField("SRC_SYS_CD_SK", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", StringType(), True),
    StructField("DRUG_CLM_SK", StringType(), True),
    StructField("NDC_SK", StringType(), True),
    StructField("AHFS_TCC_SK", StringType(), True),
    StructField("DRUG_3RD_PARTY_EXCPT_CD_SK", StringType(), True),
    StructField("FDA_THRPTC_EQVLNT_CD_SK", StringType(), True),
    StructField("GNRC_OVRD_CD_SK", StringType(), True),
    StructField("INGR_STTUS_CD_SK", StringType(), True),
    StructField("KNW_BASED_DRUG_CD_SK", StringType(), True),
    StructField("NDC_DRUG_ABUSE_CTL_CD_SK", StringType(), True),
    StructField("NDC_DRUG_CLS_CD_SK", StringType(), True),
    StructField("INGR_UNIT_OF_MESR_TX", StringType(), True),
    StructField("NDC_GNRC_NMD_DRUG_CD_SK", StringType(), True),
    StructField("NDC_RTE_TYP_CD_SK", StringType(), True),
    StructField("SUBMT_PROD_ID_QLFR_CD_SK", StringType(), True),
    StructField("DRUG_MNTN_IN", StringType(), True),
    StructField("MED_SUPL_IN", StringType(), True),
    StructField("PRMT_EXCL_IN", StringType(), True),
    StructField("PROD_RMBRMT_IN", StringType(), True),
    StructField("RBT_MNFCTR_IN", StringType(), True),
    StructField("SNGL_SRC_IN", StringType(), True),
    StructField("UNIT_DOSE_IN", StringType(), True),
    StructField("UNIT_OF_USE_IN", StringType(), True),
    StructField("AWP_UNIT_CST_AMT", StringType(), True),
    StructField("APRV_CST_SRC_CD_SK", StringType(), True),
    StructField("APRV_CST_TYP_ID", StringType(), True),
    StructField("APRV_CST_TYP_UNIT_CST_AMT", StringType(), True),
    StructField("APRV_INGR_CST_AMT", StringType(), True),
    StructField("APRV_PROF_SVC_FEE_PD_AMT", StringType(), True),
    StructField("CALC_INGR_CST_AMT", StringType(), True),
    StructField("CALC_PROF_SVC_FEE_PD_AMT", StringType(), True),
    StructField("CLNT_CST_SRC_CD_SK", StringType(), True),
    StructField("CLNT_CST_TYP_ID", StringType(), True),
    StructField("CLNT_CST_TYP_UNIT_CST_AMT", StringType(), True),
    StructField("CLNT_INGR_CST_AMT", StringType(), True),
    StructField("CLNT_PROF_SVC_FEE_PD_AMT", StringType(), True),
    StructField("CLNT_RATE_PCT", StringType(), True),
    StructField("SUBMT_CMPND_CST_TYP_ID", StringType(), True),
    StructField("SUBMT_CMPND_INGR_CST_AMT", StringType(), True),
    StructField("SUBMT_CMPND_INGR_QTY", StringType(), True),
    StructField("PDX_RATE_PCT", StringType(), True),
    StructField("DRUG_METRIC_STRG_NO", StringType(), True),
    StructField("INGR_MOD_CD_CT", StringType(), True),
    StructField("CMPND_PROD_BRND_NM", StringType(), True),
    StructField("CMPND_PROD_ID", StringType(), True),
    StructField("DRUG_DSCRPTR_ID", StringType(), True),
    StructField("DRUG_LABEL_NM", StringType(), True),
    StructField("GNRC_NM_SH_DESC", StringType(), True),
    StructField("GNRC_PROD_ID", StringType(), True),
    StructField("LBLR_NM", StringType(), True),
    StructField("LBLR_NO", StringType(), True),
    StructField("NDC_GCN_CD_TX", StringType(), True),
    StructField("GCN_SEQ_NO", StringType(), True),
])
df_Drug_Clm_Cmpnd_Ingr_PKey = spark.read.schema(schema_Drug_Clm_Cmpnd_Ingr_PKey).parquet(
    f"{adls_path}/ds/DRUG_CLM_CMPND_INGR_PKey.{RunID}.parquet"
)

# --------------------------------------------------------------------------------
# Copy_input_file (PxCopy): Splitting into two outputs
# --------------------------------------------------------------------------------
df_copy_input_file_lnk_jn_left = df_Drug_Clm_Cmpnd_Ingr_PKey.select(
    F.col("CLM_ID"),
    F.col("DRUG_CLM_CMPND_INGR_SEQ_NO"),
    F.col("SRC_SYS_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("DRUG_CLM_SK"),
    F.col("NDC_SK"),
    F.col("AHFS_TCC_SK"),
    F.col("DRUG_3RD_PARTY_EXCPT_CD_SK"),
    F.col("FDA_THRPTC_EQVLNT_CD_SK"),
    F.col("GNRC_OVRD_CD_SK"),
    F.col("INGR_STTUS_CD_SK"),
    F.col("KNW_BASED_DRUG_CD_SK"),
    F.col("NDC_DRUG_ABUSE_CTL_CD_SK"),
    F.col("NDC_DRUG_CLS_CD_SK"),
    F.col("INGR_UNIT_OF_MESR_TX"),
    F.col("NDC_GNRC_NMD_DRUG_CD_SK"),
    F.col("NDC_RTE_TYP_CD_SK"),
    F.col("SUBMT_PROD_ID_QLFR_CD_SK"),
    F.col("DRUG_MNTN_IN"),
    F.col("MED_SUPL_IN"),
    F.col("PRMT_EXCL_IN"),
    F.col("PROD_RMBRMT_IN"),
    F.col("RBT_MNFCTR_IN"),
    F.col("SNGL_SRC_IN"),
    F.col("UNIT_DOSE_IN"),
    F.col("UNIT_OF_USE_IN"),
    F.col("AWP_UNIT_CST_AMT"),
    F.col("APRV_CST_SRC_CD_SK"),
    F.col("APRV_CST_TYP_ID"),
    F.col("APRV_CST_TYP_UNIT_CST_AMT"),
    F.col("APRV_INGR_CST_AMT"),
    F.col("APRV_PROF_SVC_FEE_PD_AMT"),
    F.col("CALC_INGR_CST_AMT"),
    F.col("CALC_PROF_SVC_FEE_PD_AMT"),
    F.col("CLNT_CST_SRC_CD_SK"),
    F.col("CLNT_CST_TYP_ID"),
    F.col("CLNT_CST_TYP_UNIT_CST_AMT"),
    F.col("CLNT_INGR_CST_AMT"),
    F.col("CLNT_PROF_SVC_FEE_PD_AMT"),
    F.col("CLNT_RATE_PCT"),
    F.col("SUBMT_CMPND_CST_TYP_ID"),
    F.col("SUBMT_CMPND_INGR_CST_AMT"),
    F.col("SUBMT_CMPND_INGR_QTY"),
    F.col("PDX_RATE_PCT"),
    F.col("DRUG_METRIC_STRG_NO"),
    F.col("INGR_MOD_CD_CT"),
    F.col("CMPND_PROD_BRND_NM"),
    F.col("CMPND_PROD_ID"),
    F.col("DRUG_DSCRPTR_ID"),
    F.col("DRUG_LABEL_NM"),
    F.col("GNRC_NM_SH_DESC"),
    F.col("GNRC_PROD_ID"),
    F.col("LBLR_NM"),
    F.col("LBLR_NO"),
    F.col("NDC_GCN_CD_TX"),
    F.col("GCN_SEQ_NO")
)

df_lnkRemDupDataIn = df_Drug_Clm_Cmpnd_Ingr_PKey.select(
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("DRUG_CLM_CMPND_INGR_SEQ_NO").alias("DRUG_CLM_CMPND_INGR_SEQ_NO"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK")
)

# --------------------------------------------------------------------------------
# removeDuNatKeys (PxRemDup)
# --------------------------------------------------------------------------------
df_removeDuNatKeys = dedup_sort(
    df_lnkRemDupDataIn,
    ["CLM_ID", "DRUG_CLM_CMPND_INGR_SEQ_NO", "SRC_SYS_CD_SK"],
    [("CLM_ID", "A"), ("DRUG_CLM_CMPND_INGR_SEQ_NO", "A"), ("SRC_SYS_CD_SK", "A")]
)

# --------------------------------------------------------------------------------
# jn_left_NaturalKeys (PxJoin) - LEFT JOIN
# --------------------------------------------------------------------------------
df_jn_left_NaturalKeys = df_removeDuNatKeys.alias("lnkRemDupDataOut").join(
    df_db_K_DRUG_CLM_CMPND_INGR_read.alias("lnk_to_jn"),
    (F.col("lnkRemDupDataOut.CLM_ID") == F.col("lnk_to_jn.CLM_ID")) &
    (F.col("lnkRemDupDataOut.DRUG_CLM_CMPND_INGR_SEQ_NO") == F.col("lnk_to_jn.DRUG_CLM_CMPND_INGR_SEQ_NO")) &
    (F.col("lnkRemDupDataOut.SRC_SYS_CD_SK") == F.col("lnk_to_jn.SRC_SYS_CD_SK")),
    how="left"
).select(
    F.col("lnkRemDupDataOut.CLM_ID").alias("CLM_ID"),
    F.col("lnkRemDupDataOut.DRUG_CLM_CMPND_INGR_SEQ_NO").alias("DRUG_CLM_CMPND_INGR_SEQ_NO"),
    F.col("lnkRemDupDataOut.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("lnk_to_jn.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_to_jn.DRUG_CLM_CMPND_INGR_SK").alias("DRUG_CLM_CMPND_INGR_SK")
)

# --------------------------------------------------------------------------------
# xfm_PKEYgen (CTransformerStage)
# --------------------------------------------------------------------------------
df_base_xfm_PKEYgen = df_jn_left_NaturalKeys.withColumn("originalSK", F.col("DRUG_CLM_CMPND_INGR_SK"))

df_enriched = df_base_xfm_PKEYgen
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"DRUG_CLM_CMPND_INGR_SK",<schema>,<secret_name>)
df_enriched = df_enriched.withColumn(
    "CRT_RUN_CYC_EXCTN_SK_right",
    F.when(
        (F.col("originalSK").isNull()) | (F.col("originalSK") == F.lit("0")),
        IDSRunCycle
    ).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_SK_right",
    F.lit(IDSRunCycle)
)

df_lnk_jn_right = df_enriched.select(
    F.col("DRUG_CLM_CMPND_INGR_SK").alias("DRUG_CLM_CMPND_INGR_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("DRUG_CLM_CMPND_INGR_SEQ_NO").alias("DRUG_CLM_CMPND_INGR_SEQ_NO"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK_right").alias("CRT_RUN_CYC_EXCTN_SK_right"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK_right").alias("LAST_UPDT_RUN_CYC_EXCTN_SK_right"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),  # Keep for potential reference
    F.col("originalSK").alias("originalSK")
)

df_lnk_to_db_write = df_enriched.filter(
    (F.col("originalSK").isNull()) | (F.col("originalSK") == F.lit("0"))
).select(
    F.col("DRUG_CLM_CMPND_INGR_SK").alias("DRUG_CLM_CMPND_INGR_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("DRUG_CLM_CMPND_INGR_SEQ_NO").alias("DRUG_CLM_CMPND_INGR_SEQ_NO"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK")
).withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(IDSRunCycle))

# --------------------------------------------------------------------------------
# db_K_DRUG_CLM_CMPND_INGR_write (DB2ConnectorPX) -> MERGE into {IDSOwner}.K_DRUG_CLM_CMPND_INGR
# --------------------------------------------------------------------------------
tmp_table_db_K_DRUG_CLM_CMPND_INGR_write = "STAGING.OptumIDSCompoundsPkey_db_K_DRUG_CLM_CMPND_INGR_write_temp"
execute_dml(f"DROP TABLE IF EXISTS {tmp_table_db_K_DRUG_CLM_CMPND_INGR_write}", jdbc_url, jdbc_props)
df_lnk_to_db_write.write.jdbc(
    url=jdbc_url,
    table=tmp_table_db_K_DRUG_CLM_CMPND_INGR_write,
    mode="overwrite",
    properties=jdbc_props
)
merge_sql_db_K_DRUG_CLM_CMPND_INGR_write = f"""
MERGE INTO {IDSOwner}.K_DRUG_CLM_CMPND_INGR AS T
USING {tmp_table_db_K_DRUG_CLM_CMPND_INGR_write} AS S
ON T.DRUG_CLM_CMPND_INGR_SK = S.DRUG_CLM_CMPND_INGR_SK
WHEN MATCHED THEN
  UPDATE SET
    T.CLM_ID = S.CLM_ID,
    T.DRUG_CLM_CMPND_INGR_SEQ_NO = S.DRUG_CLM_CMPND_INGR_SEQ_NO,
    T.SRC_SYS_CD_SK = S.SRC_SYS_CD_SK,
    T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK
WHEN NOT MATCHED THEN
  INSERT (
    DRUG_CLM_CMPND_INGR_SK,
    CLM_ID,
    DRUG_CLM_CMPND_INGR_SEQ_NO,
    SRC_SYS_CD_SK,
    CRT_RUN_CYC_EXCTN_SK
  )
  VALUES (
    S.DRUG_CLM_CMPND_INGR_SK,
    S.CLM_ID,
    S.DRUG_CLM_CMPND_INGR_SEQ_NO,
    S.SRC_SYS_CD_SK,
    S.CRT_RUN_CYC_EXCTN_SK
  );
"""
execute_dml(merge_sql_db_K_DRUG_CLM_CMPND_INGR_write, jdbc_url, jdbc_props)

# --------------------------------------------------------------------------------
# jn_left (PxJoin) - INNER JOIN
# --------------------------------------------------------------------------------
# df_copy_input_file_lnk_jn_left is "lnk_jn_left" input
# df_lnk_jn_right is "lnk_jn_right" input
df_jn_left = df_lnk_jn_right.alias("lnk_jn_right").join(
    df_copy_input_file_lnk_jn_left.alias("lnk_jn_left"),
    (F.col("lnk_jn_right.CLM_ID") == F.col("lnk_jn_left.CLM_ID")) &
    (F.col("lnk_jn_right.DRUG_CLM_CMPND_INGR_SEQ_NO") == F.col("lnk_jn_left.DRUG_CLM_CMPND_INGR_SEQ_NO")) &
    (F.col("lnk_jn_right.SRC_SYS_CD_SK") == F.col("lnk_jn_left.SRC_SYS_CD_SK")),
    how="inner"
).select(
    F.col("lnk_jn_right.DRUG_CLM_CMPND_INGR_SK").alias("DRUG_CLM_CMPND_INGR_SK"),
    F.col("lnk_jn_right.CLM_ID").alias("CLM_ID"),
    F.col("lnk_jn_right.DRUG_CLM_CMPND_INGR_SEQ_NO").alias("DRUG_CLM_CMPND_INGR_SEQ_NO"),
    F.col("lnk_jn_right.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("lnk_jn_right.CRT_RUN_CYC_EXCTN_SK_right").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_jn_right.LAST_UPDT_RUN_CYC_EXCTN_SK_right").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_jn_left.DRUG_CLM_SK").alias("DRUG_CLM_SK"),
    F.col("lnk_jn_left.NDC_SK").alias("NDC_SK"),
    F.col("lnk_jn_left.AHFS_TCC_SK").alias("AHFS_TCC_SK"),
    F.col("lnk_jn_left.DRUG_3RD_PARTY_EXCPT_CD_SK").alias("DRUG_3RD_PARTY_EXCPT_CD_SK"),
    F.col("lnk_jn_left.FDA_THRPTC_EQVLNT_CD_SK").alias("FDA_THRPTC_EQVLNT_CD_SK"),
    F.col("lnk_jn_left.GNRC_OVRD_CD_SK").alias("GNRC_OVRD_CD_SK"),
    F.col("lnk_jn_left.INGR_STTUS_CD_SK").alias("INGR_STTUS_CD_SK"),
    F.col("lnk_jn_left.KNW_BASED_DRUG_CD_SK").alias("KNW_BASED_DRUG_CD_SK"),
    F.col("lnk_jn_left.NDC_DRUG_ABUSE_CTL_CD_SK").alias("NDC_DRUG_ABUSE_CTL_CD_SK"),
    F.col("lnk_jn_left.NDC_DRUG_CLS_CD_SK").alias("NDC_DRUG_CLS_CD_SK"),
    F.col("lnk_jn_left.INGR_UNIT_OF_MESR_TX").alias("INGR_UNIT_OF_MESR_TX"),
    F.col("lnk_jn_left.NDC_GNRC_NMD_DRUG_CD_SK").alias("NDC_GNRC_NMD_DRUG_CD_SK"),
    F.col("lnk_jn_left.NDC_RTE_TYP_CD_SK").alias("NDC_RTE_TYP_CD_SK"),
    F.col("lnk_jn_left.SUBMT_PROD_ID_QLFR_CD_SK").alias("SUBMT_PROD_ID_QLFR_CD_SK"),
    F.col("lnk_jn_left.DRUG_MNTN_IN").alias("DRUG_MNTN_IN"),
    F.col("lnk_jn_left.MED_SUPL_IN").alias("MED_SUPL_IN"),
    F.col("lnk_jn_left.PRMT_EXCL_IN").alias("PRMT_EXCL_IN"),
    F.col("lnk_jn_left.PROD_RMBRMT_IN").alias("PROD_RMBRMT_IN"),
    F.col("lnk_jn_left.RBT_MNFCTR_IN").alias("RBT_MNFCTR_IN"),
    F.col("lnk_jn_left.SNGL_SRC_IN").alias("SNGL_SRC_IN"),
    F.col("lnk_jn_left.UNIT_DOSE_IN").alias("UNIT_DOSE_IN"),
    F.col("lnk_jn_left.UNIT_OF_USE_IN").alias("UNIT_OF_USE_IN"),
    F.col("lnk_jn_left.AWP_UNIT_CST_AMT").alias("AWP_UNIT_CST_AMT"),
    F.col("lnk_jn_left.APRV_CST_SRC_CD_SK").alias("APRV_CST_SRC_CD_SK"),
    F.col("lnk_jn_left.APRV_CST_TYP_ID").alias("APRV_CST_TYP_ID"),
    F.col("lnk_jn_left.APRV_CST_TYP_UNIT_CST_AMT").alias("APRV_CST_TYP_UNIT_CST_AMT"),
    F.col("lnk_jn_left.APRV_INGR_CST_AMT").alias("APRV_INGR_CST_AMT"),
    F.col("lnk_jn_left.APRV_PROF_SVC_FEE_PD_AMT").alias("APRV_PROF_SVC_FEE_PD_AMT"),
    F.col("lnk_jn_left.CALC_INGR_CST_AMT").alias("CALC_INGR_CST_AMT"),
    F.col("lnk_jn_left.CALC_PROF_SVC_FEE_PD_AMT").alias("CALC_PROF_SVC_FEE_PD_AMT"),
    F.col("lnk_jn_left.CLNT_CST_SRC_CD_SK").alias("CLNT_CST_SRC_CD_SK"),
    F.col("lnk_jn_left.CLNT_CST_TYP_ID").alias("CLNT_CST_TYP_ID"),
    F.col("lnk_jn_left.CLNT_CST_TYP_UNIT_CST_AMT").alias("CLNT_CST_TYP_UNIT_CST_AMT"),
    F.col("lnk_jn_left.CLNT_INGR_CST_AMT").alias("CLNT_INGR_CST_AMT"),
    F.col("lnk_jn_left.CLNT_PROF_SVC_FEE_PD_AMT").alias("CLNT_PROF_SVC_FEE_PD_AMT"),
    F.col("lnk_jn_left.CLNT_RATE_PCT").alias("CLNT_RATE_PCT"),
    F.col("lnk_jn_left.SUBMT_CMPND_CST_TYP_ID").alias("SUBMT_CMPND_CST_TYP_ID"),
    F.col("lnk_jn_left.SUBMT_CMPND_INGR_CST_AMT").alias("SUBMT_CMPND_INGR_CST_AMT"),
    F.col("lnk_jn_left.SUBMT_CMPND_INGR_QTY").alias("SUBMT_CMPND_INGR_QTY"),
    F.col("lnk_jn_left.PDX_RATE_PCT").alias("PDX_RATE_PCT"),
    F.col("lnk_jn_left.DRUG_METRIC_STRG_NO").alias("DRUG_METRIC_STRG_NO"),
    F.col("lnk_jn_left.INGR_MOD_CD_CT").alias("INGR_MOD_CD_CT"),
    F.col("lnk_jn_left.CMPND_PROD_BRND_NM").alias("CMPND_PROD_BRND_NM"),
    F.col("lnk_jn_left.CMPND_PROD_ID").alias("CMPND_PROD_ID"),
    F.col("lnk_jn_left.DRUG_DSCRPTR_ID").alias("DRUG_DSCRPTR_ID"),
    F.col("lnk_jn_left.DRUG_LABEL_NM").alias("DRUG_LABEL_NM"),
    F.col("lnk_jn_left.GNRC_NM_SH_DESC").alias("GNRC_NM_SH_DESC"),
    F.col("lnk_jn_left.GNRC_PROD_ID").alias("GNRC_PROD_ID"),
    F.col("lnk_jn_left.LBLR_NM").alias("LBLR_NM"),
    F.col("lnk_jn_left.LBLR_NO").alias("LBLR_NO"),
    F.col("lnk_jn_left.NDC_GCN_CD_TX").alias("NDC_GCN_CD_TX"),
    F.col("lnk_jn_left.GCN_SEQ_NO").alias("GCN_SEQ_NO")
)

# --------------------------------------------------------------------------------
# Copy (PxCopy) -> Two output links
# --------------------------------------------------------------------------------
df_lnk_to_seq_file_Bal = df_jn_left.select(
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("DRUG_CLM_CMPND_INGR_SEQ_NO").alias("DRUG_CLM_CMPND_INGR_SEQ_NO"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK")
)
df_lnk_to_seq_file_load1 = df_jn_left.select(
    F.col("DRUG_CLM_CMPND_INGR_SK").alias("DRUG_CLM_CMPND_INGR_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("DRUG_CLM_CMPND_INGR_SEQ_NO").alias("DRUG_CLM_CMPND_INGR_SEQ_NO"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("DRUG_CLM_SK").alias("DRUG_CLM_SK"),
    F.col("NDC_SK").alias("NDC_SK"),
    F.col("AHFS_TCC_SK").alias("AHFS_TCC_SK"),
    F.col("DRUG_3RD_PARTY_EXCPT_CD_SK").alias("DRUG_3RD_PARTY_EXCPT_CD_SK"),
    F.col("FDA_THRPTC_EQVLNT_CD_SK").alias("FDA_THRPTC_EQVLNT_CD_SK"),
    F.col("GNRC_OVRD_CD_SK").alias("GNRC_OVRD_CD_SK"),
    F.col("INGR_STTUS_CD_SK").alias("INGR_STTUS_CD_SK"),
    F.col("NDC_DRUG_ABUSE_CTL_CD_SK").alias("NDC_DRUG_ABUSE_CTL_CD_SK"),
    F.col("NDC_DRUG_CLS_CD_SK").alias("NDC_DRUG_CLS_CD_SK"),
    F.col("INGR_UNIT_OF_MESR_TX").alias("INGR_UNIT_OF_MESR_TX"),
    F.col("NDC_GNRC_NMD_DRUG_CD_SK").alias("NDC_GNRC_NMD_DRUG_CD_SK"),
    F.col("NDC_RTE_TYP_CD_SK").alias("NDC_RTE_TYP_CD_SK"),
    F.col("SUBMT_PROD_ID_QLFR_CD_SK").alias("SUBMT_PROD_ID_QLFR_CD_SK"),
    F.col("DRUG_MNTN_IN").alias("DRUG_MNTN_IN"),
    F.col("MED_SUPL_IN").alias("MED_SUPL_IN"),
    F.col("PRMT_EXCL_IN").alias("PRMT_EXCL_IN"),
    F.col("PROD_RMBRMT_IN").alias("PROD_RMBRMT_IN"),
    F.col("RBT_MNFCTR_IN").alias("RBT_MNFCTR_IN"),
    F.col("SNGL_SRC_IN").alias("SNGL_SRC_IN"),
    F.col("UNIT_DOSE_IN").alias("UNIT_DOSE_IN"),
    F.col("UNIT_OF_USE_IN").alias("UNIT_OF_USE_IN"),
    F.col("AWP_UNIT_CST_AMT").alias("AWP_UNIT_CST_AMT"),
    F.col("APRV_CST_SRC_CD_SK").alias("APRV_CST_SRC_CD_SK"),
    F.col("APRV_CST_TYP_ID").alias("APRV_CST_TYP_ID"),
    F.col("APRV_CST_TYP_UNIT_CST_AMT").alias("APRV_CST_TYP_UNIT_CST_AMT"),
    F.col("APRV_INGR_CST_AMT").alias("APRV_INGR_CST_AMT"),
    F.col("APRV_PROF_SVC_FEE_PD_AMT").alias("APRV_PROF_SVC_FEE_PD_AMT"),
    F.col("CALC_INGR_CST_AMT").alias("CALC_INGR_CST_AMT"),
    F.col("CALC_PROF_SVC_FEE_PD_AMT").alias("CALC_PROF_SVC_FEE_PD_AMT"),
    F.col("CLNT_CST_SRC_CD_SK").alias("CLNT_CST_SRC_CD_SK"),
    F.col("CLNT_CST_TYP_ID").alias("CLNT_CST_TYP_ID"),
    F.col("CLNT_CST_TYP_UNIT_CST_AMT").alias("CLNT_CST_TYP_UNIT_CST_AMT"),
    F.col("CLNT_INGR_CST_AMT").alias("CLNT_INGR_CST_AMT"),
    F.col("CLNT_PROF_SVC_FEE_PD_AMT").alias("CLNT_PROF_SVC_FEE_PD_AMT"),
    F.col("CLNT_RATE_PCT").alias("CLNT_RATE_PCT"),
    F.col("SUBMT_CMPND_CST_TYP_ID").alias("SUBMT_CMPND_CST_TYP_ID"),
    F.col("SUBMT_CMPND_INGR_CST_AMT").alias("SUBMT_CMPND_INGR_CST_AMT"),
    F.col("SUBMT_CMPND_INGR_QTY").alias("SUBMT_CMPND_INGR_QTY"),
    F.col("PDX_RATE_PCT").alias("PDX_RATE_PCT"),
    F.col("DRUG_METRIC_STRG_NO").alias("DRUG_METRIC_STRG_NO"),
    F.col("INGR_MOD_CD_CT").alias("INGR_MOD_CD_CT"),
    F.col("CMPND_PROD_BRND_NM").alias("CMPND_PROD_BRND_NM"),
    F.col("CMPND_PROD_ID").alias("CMPND_PROD_ID"),
    F.col("DRUG_DSCRPTR_ID").alias("DRUG_DSCRPTR_ID"),
    F.col("DRUG_LABEL_NM").alias("DRUG_LABEL_NM"),
    F.col("GNRC_NM_SH_DESC").alias("GNRC_NM_SH_DESC"),
    F.col("GNRC_PROD_ID").alias("GNRC_PROD_ID"),
    F.col("LBLR_NM").alias("LBLR_NM"),
    F.col("LBLR_NO").alias("LBLR_NO"),
    F.col("NDC_GCN_CD_TX").alias("NDC_GCN_CD_TX"),
    F.col("GCN_SEQ_NO").alias("GCN_SEQ_NO")
)

# --------------------------------------------------------------------------------
# seq_DRUG_CLM_CMPND_INGR_load (PxSequentialFile)
# --------------------------------------------------------------------------------
df_seq_DRUG_CLM_CMPND_INGR_load = df_lnk_to_seq_file_load1.withColumn(
    "DRUG_MNTN_IN", F.rpad("DRUG_MNTN_IN", 1, " ")
).withColumn(
    "MED_SUPL_IN", F.rpad("MED_SUPL_IN", 1, " ")
).withColumn(
    "PRMT_EXCL_IN", F.rpad("PRMT_EXCL_IN", 1, " ")
).withColumn(
    "PROD_RMBRMT_IN", F.rpad("PROD_RMBRMT_IN", 1, " ")
).withColumn(
    "RBT_MNFCTR_IN", F.rpad("RBT_MNFCTR_IN", 1, " ")
).withColumn(
    "SNGL_SRC_IN", F.rpad("SNGL_SRC_IN", 1, " ")
).withColumn(
    "UNIT_DOSE_IN", F.rpad("UNIT_DOSE_IN", 1, " ")
).withColumn(
    "UNIT_OF_USE_IN", F.rpad("UNIT_OF_USE_IN", 1, " ")
).select(
    "DRUG_CLM_CMPND_INGR_SK",
    "CLM_ID",
    "DRUG_CLM_CMPND_INGR_SEQ_NO",
    "SRC_SYS_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "DRUG_CLM_SK",
    "NDC_SK",
    "AHFS_TCC_SK",
    "DRUG_3RD_PARTY_EXCPT_CD_SK",
    "FDA_THRPTC_EQVLNT_CD_SK",
    "GNRC_OVRD_CD_SK",
    "INGR_STTUS_CD_SK",
    "NDC_DRUG_ABUSE_CTL_CD_SK",
    "NDC_DRUG_CLS_CD_SK",
    "INGR_UNIT_OF_MESR_TX",
    "NDC_GNRC_NMD_DRUG_CD_SK",
    "NDC_RTE_TYP_CD_SK",
    "SUBMT_PROD_ID_QLFR_CD_SK",
    "DRUG_MNTN_IN",
    "MED_SUPL_IN",
    "PRMT_EXCL_IN",
    "PROD_RMBRMT_IN",
    "RBT_MNFCTR_IN",
    "SNGL_SRC_IN",
    "UNIT_DOSE_IN",
    "UNIT_OF_USE_IN",
    "AWP_UNIT_CST_AMT",
    "APRV_CST_SRC_CD_SK",
    "APRV_CST_TYP_ID",
    "APRV_CST_TYP_UNIT_CST_AMT",
    "APRV_INGR_CST_AMT",
    "APRV_PROF_SVC_FEE_PD_AMT",
    "CALC_INGR_CST_AMT",
    "CALC_PROF_SVC_FEE_PD_AMT",
    "CLNT_CST_SRC_CD_SK",
    "CLNT_CST_TYP_ID",
    "CLNT_CST_TYP_UNIT_CST_AMT",
    "CLNT_INGR_CST_AMT",
    "CLNT_PROF_SVC_FEE_PD_AMT",
    "CLNT_RATE_PCT",
    "SUBMT_CMPND_CST_TYP_ID",
    "SUBMT_CMPND_INGR_CST_AMT",
    "SUBMT_CMPND_INGR_QTY",
    "PDX_RATE_PCT",
    "DRUG_METRIC_STRG_NO",
    "INGR_MOD_CD_CT",
    "CMPND_PROD_BRND_NM",
    "CMPND_PROD_ID",
    "DRUG_DSCRPTR_ID",
    "DRUG_LABEL_NM",
    "GNRC_NM_SH_DESC",
    "GNRC_PROD_ID",
    "LBLR_NM",
    "LBLR_NO",
    "NDC_GCN_CD_TX",
    "GCN_SEQ_NO"
)
write_files(
    df_seq_DRUG_CLM_CMPND_INGR_load,
    f"{adls_path}/load/DRUG_CLM_CMPND_INGR.dat",
    delimiter="^",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote=None,
    nullValue="\"\""
)

# --------------------------------------------------------------------------------
# Seq_B_DRUG_CLM_CMPND_INGR (PxSequentialFile)
# --------------------------------------------------------------------------------
df_Seq_B_DRUG_CLM_CMPND_INGR = df_lnk_to_seq_file_Bal.select(
    "CLM_ID",
    "DRUG_CLM_CMPND_INGR_SEQ_NO",
    "SRC_SYS_CD_SK"
)
write_files(
    df_Seq_B_DRUG_CLM_CMPND_INGR,
    f"{adls_path}/load/B_DRUG_CLM_CMPND_INGR.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote=None,
    nullValue="\"\""
)