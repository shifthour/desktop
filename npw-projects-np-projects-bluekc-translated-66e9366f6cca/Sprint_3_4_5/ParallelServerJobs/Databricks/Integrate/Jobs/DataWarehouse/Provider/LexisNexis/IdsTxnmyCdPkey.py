# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC 
# MAGIC Ravi Abburi              2017-11-07          5781                                Original Programming                                                                           IntegrateDev2          Kalyan Neelam              2018-01-30

# MAGIC Remove Duplicates stage takes care of getting rid of duplicate rows with the same Natural key get into the PKEYing step.
# MAGIC A concious effort need to be made to evaluate which one of the duplicate set row to drop is a case by case basis decision.
# MAGIC Table K_TXNMY_CD.Insert new SKEY rows generated as part of this run. This is a database insert operation.
# MAGIC Land into Seq File for the FKEY job
# MAGIC New PKEYs are generated in this transformer. A database sequence will be used to create next PKEY value.
# MAGIC Left Outer Join Here
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# -- PARAMETERS --
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
IDSRunCycle = get_widget_value('IDSRunCycle','100')
SrcSysCd = get_widget_value('SrcSysCd','LEXISNEXIS')
RunID = get_widget_value('RunID','100')

# -- STAGE: ds_ProvTxnmy_Xfrm (PxDataSet) --
df_ds_ProvTxnmy_Xfrm = spark.read.parquet(f"{adls_path}/ds/TXNMY_CD.{SrcSysCd}.extr.{RunID}.parquet")

# -- STAGE: cpy_MultiStreams (PxCopy) --
df_lnk_IdsTxnmyCdPkey_All = df_ds_ProvTxnmy_Xfrm.select(
    F.col("TXNMY_CD").alias("TXNMY_CD"),
    F.col("EFF_DT").alias("EFF_DT"),
    F.col("DCTVTN_DT").alias("DCTVTN_DT"),
    F.col("LAST_UPDT_DT").alias("LAST_UPDT_DT"),
    F.col("TXNMY_DESC").alias("TXNMY_DESC"),
    F.col("TXNMY_PROV_TYP_CD").alias("TXNMY_PROV_TYP_CD"),
    F.col("TXNMY_PROV_TYP_DESC").alias("TXNMY_PROV_TYP_DESC"),
    F.col("TXNMY_CLS_CD").alias("TXNMY_CLS_CD"),
    F.col("TXNMY_CLS_DESC").alias("TXNMY_CLS_DESC"),
    F.col("TXNMY_SPCLIZATION_CD").alias("TXNMY_SPCLIZATION_CD"),
    F.col("TXNMY_SPCLIZATION_DESC").alias("TXNMY_SPCLIZATION_DESC"),
    F.col("CRT_SRC_SYS_CD").alias("CRT_SRC_SYS_CD"),
    F.col("LAST_UPDT_SRC_SYS_CD").alias("LAST_UPDT_SRC_SYS_CD"),
    F.col("PROV_SPEC_CD").alias("PROV_SPEC_CD"),
    F.col("PROV_FCLTY_TYP_CD").alias("PROV_FCLTY_TYP_CD")
)

df_lnk_TxnmyCdPkey_Dedup = df_ds_ProvTxnmy_Xfrm.select(
    F.col("TXNMY_CD").alias("TXNMY_CD")
)

# -- STAGE: rdp_NaturalKeys (PxRemDup) --
df_lnkRemDupDataOut = dedup_sort(df_lnk_TxnmyCdPkey_Dedup, ["TXNMY_CD"], [])

# -- STAGE: db2_K_TXNMY_CD_In (DB2ConnectorPX) --
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_ids = f"SELECT TXNMY_CD, TXNMY_CD_SK, CRT_RUN_CYC_EXCTN_SK FROM {IDSOwner}.K_TXNMY_CD"
df_db2_K_TXNMY_CD_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_ids)
    .load()
)

# -- STAGE: jn_Prov (PxJoin - left outer join on TXNMY_CD) --
dfA = df_lnkRemDupDataOut.alias("A")
dfB = df_db2_K_TXNMY_CD_In.alias("B")
df_jn_Prov = dfA.join(dfB, F.col("A.TXNMY_CD") == F.col("B.TXNMY_CD"), how="left").select(
    F.col("A.TXNMY_CD").alias("TXNMY_CD"),
    F.col("B.TXNMY_CD_SK").alias("TXNMY_CD_SK_in"),
    F.col("B.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK_in")
)

# -- STAGE: xfm_PKEYgen (CTransformerStage) --
# Rename columns to prepare for surrogate key generation
df_enriched = df_jn_Prov.withColumn("TXNMY_CD_SK", F.col("TXNMY_CD_SK_in"))

# Apply SurrogateKeyGen for "TXNMY_CD_SK"
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"TXNMY_CD_SK",<schema>,<secret_name>)

# Split outputs based on constraint "IsNull(TXNMY_CD_SK_in)"
df_lnk_K_TXNMY_CD_New = df_enriched.filter(
    F.col("TXNMY_CD_SK_in").isNull()
).select(
    F.col("TXNMY_CD").alias("TXNMY_CD"),
    F.lit(IDSRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("TXNMY_CD_SK").alias("TXNMY_CD_SK")
)

df_lnkPKEYxfmOut = df_enriched.select(
    F.col("TXNMY_CD").alias("TXNMY_CD"),
    F.when(F.col("TXNMY_CD_SK_in").isNull(), F.lit(IDSRunCycle))
     .otherwise(F.col("CRT_RUN_CYC_EXCTN_SK_in")).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(IDSRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("TXNMY_CD_SK").alias("TXNMY_CD_SK")
)

# -- STAGE: db2_K_TXNMY_CD_Load (DB2ConnectorPX) --
# Merge logic
df_k_txnmy_cd_load = df_lnk_K_TXNMY_CD_New

temp_table_name = "STAGING.IdsTxnmyCdPkey_db2_K_TXNMY_CD_Load_temp"
drop_sql = f"DROP TABLE IF EXISTS {temp_table_name}"
create_and_insert_df = df_k_txnmy_cd_load

execute_dml(drop_sql, jdbc_url_ids, jdbc_props_ids)

create_and_insert_df.write \
    .format("jdbc") \
    .option("url", jdbc_url_ids) \
    .options(**jdbc_props_ids) \
    .option("dbtable", temp_table_name) \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE {IDSOwner}.K_TXNMY_CD AS T
USING {temp_table_name} AS S
ON T.TXNMY_CD = S.TXNMY_CD
WHEN MATCHED THEN
  UPDATE SET 
    T.TXNMY_CD_SK = S.TXNMY_CD_SK,
    T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK
WHEN NOT MATCHED THEN
  INSERT (TXNMY_CD, TXNMY_CD_SK, CRT_RUN_CYC_EXCTN_SK)
  VALUES (S.TXNMY_CD, S.TXNMY_CD_SK, S.CRT_RUN_CYC_EXCTN_SK);
"""
execute_dml(merge_sql, jdbc_url_ids, jdbc_props_ids)

# -- STAGE: jn_PKEYs (PxJoin - inner join on TXNMY_CD) --
df_IdsTxnmyCdPkey_All = df_lnk_IdsTxnmyCdPkey_All.alias("A")
df_PKEYxfmOut = df_lnkPKEYxfmOut.alias("B")
df_jn_PKEYs = df_IdsTxnmyCdPkey_All.join(
    df_PKEYxfmOut,
    F.col("A.TXNMY_CD") == F.col("B.TXNMY_CD"),
    how="inner"
).select(
    F.col("B.TXNMY_CD_SK").alias("TXNMY_CD_SK"),
    F.col("A.TXNMY_CD").alias("TXNMY_CD"),
    F.col("B.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("B.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("A.EFF_DT").alias("EFF_DT"),
    F.col("A.DCTVTN_DT").alias("DCTVTN_DT"),
    F.col("A.LAST_UPDT_DT").alias("LAST_UPDT_DT"),
    F.col("A.TXNMY_DESC").alias("TXNMY_DESC"),
    F.col("A.TXNMY_PROV_TYP_CD").alias("TXNMY_PROV_TYP_CD"),
    F.col("A.TXNMY_PROV_TYP_DESC").alias("TXNMY_PROV_TYP_DESC"),
    F.col("A.TXNMY_CLS_CD").alias("TXNMY_CLS_CD"),
    F.col("A.TXNMY_CLS_DESC").alias("TXNMY_CLS_DESC"),
    F.col("A.TXNMY_SPCLIZATION_CD").alias("TXNMY_SPCLIZATION_CD"),
    F.col("A.TXNMY_SPCLIZATION_DESC").alias("TXNMY_SPCLIZATION_DESC"),
    F.col("A.CRT_SRC_SYS_CD").alias("CRT_SRC_SYS_CD"),
    F.col("A.LAST_UPDT_SRC_SYS_CD").alias("LAST_UPDT_SRC_SYS_CD"),
    F.col("A.PROV_SPEC_CD").alias("PROV_SPEC_CD"),
    F.col("A.PROV_FCLTY_TYP_CD").alias("PROV_FCLTY_TYP_CD")
)

# -- STAGE: seq_TXNMY_CD_Pkey (PxSequentialFile) --
df_final = df_jn_PKEYs.select(
    "TXNMY_CD_SK",
    "TXNMY_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "EFF_DT",
    "DCTVTN_DT",
    "LAST_UPDT_DT",
    "TXNMY_DESC",
    "TXNMY_PROV_TYP_CD",
    "TXNMY_PROV_TYP_DESC",
    "TXNMY_CLS_CD",
    "TXNMY_CLS_DESC",
    "TXNMY_SPCLIZATION_CD",
    "TXNMY_SPCLIZATION_DESC",
    "CRT_SRC_SYS_CD",
    "LAST_UPDT_SRC_SYS_CD",
    "PROV_SPEC_CD",
    "PROV_FCLTY_TYP_CD"
)

write_files(
    df_final,
    f"{adls_path}/key/TXNMY_CD.{SrcSysCd}.pkey.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)