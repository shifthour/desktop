# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC 
# MAGIC JOb Name : FNfmIdsMdlPolAmndmntPkey
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# MAGIC DinakarS            2018-07-02               5205                  Original Programming                                                                             IntegrateWrhsDevl           Kalyan Neelam             2018-07-03

# MAGIC Job Name: IdsMdlPolAmndmntPkey
# MAGIC Remove Duplicates stage takes care of getting rid of duplicate rows with the same Natural key get into the PKEYing step.
# MAGIC A concious effort need to be made to evaluate which one of the duplicate set row to drop is a case by case basis decision.
# MAGIC Table K_MDL_POL_AMNDMNT               Insert new SKEY rows generated as part of this run. This is a database insert operation.
# MAGIC Audit fields are added into this File
# MAGIC New PKEYs are generated in this transformer. A database sequence will be used to create next PKEY value.
# MAGIC Left Outer Join Here
# MAGIC Audit fields are added into this File
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, when, lit
from pyspark.sql import DataFrame
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


# Retrieve parameter values
IDSOwner = get_widget_value("IDSOwner", "")
ids_secret_name = get_widget_value("ids_secret_name", "")
IDSRunCycle = get_widget_value("IDSRunCycle", "")
SrcSysCd = get_widget_value("SrcSysCd", "")
RunID = get_widget_value("RunID", "")

# DB2 Connector Stage: db2_KMdlPolAmndmntExt
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query_db2_KMdlPolAmndmntExt = f"""
SELECT 
  MDL_DOC_ID,
  POL_NO,
  AMNDMNT_ID,
  AMNDMNT_EFF_DT,
  SRC_SYS_CD,
  MDL_POL_AMNDMNT_SK,
  CRT_RUN_CYC_EXCTN_SK
FROM {IDSOwner}.K_MDL_POL_AMNDMNT
"""
df_db2_KMdlPolAmndmntExt = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_KMdlPolAmndmntExt)
    .load()
)

# PxDataSet Stage: ds_MDL_AMNDMNT_DS_1 (Reading .ds as .parquet)
df_ds_MDL_AMNDMNT_DS_1 = spark.read.parquet(
    f"{adls_path}/ds/MDLPOLAMNDMNT.{SrcSysCd}.xfrm.{RunID}.parquet"
)

# PxCopy Stage: cpy_MultiStreams
df_lnkRemDupDataIn = df_ds_MDL_AMNDMNT_DS_1.select(
    col("MDL_DOC_ID"),
    col("POL_NO"),
    col("AMNDMNT_ID"),
    col("AMNDMNT_EFF_DT"),
    col("SRC_SYS_CD")
)
df_lnkFullDataJnIn = df_ds_MDL_AMNDMNT_DS_1.select(
    col("MDL_DOC_ID"),
    col("POL_NO"),
    col("POLICY_FORM_ID"),
    col("POLICY_FORM_DT"),
    col("AMNDMNT_ID"),
    col("MDL_DOC_EFF_DT"),
    col("AMNDMNT_EFF_DT"),
    col("MDL_DOC_TERM_DT"),
    col("SRC_SYS_CD"),
    col("AMNDMNT_EFF_ON_GRP_RNWL_IN"),
    col("AMNDMNT_STATUS_ID"),
    col("AMNDMNT_TERM_DT")
)

# PxRemDup Stage: rdp_NaturalKeys
df_rdp_NaturalKeys = dedup_sort(
    df_lnkRemDupDataIn,
    ["MDL_DOC_ID", "POL_NO", "AMNDMNT_ID", "AMNDMNT_EFF_DT", "SRC_SYS_CD"],
    []
)

# PxJoin Stage: jn_MDL_POL_AMNDMNT (left outer join)
df_jn_MDL_POL_AMNDMNT = df_rdp_NaturalKeys.alias("lnkRemDupDataOut").join(
    df_db2_KMdlPolAmndmntExt.alias("lnkKMdllAmndmntExt"),
    on=[
        col("lnkRemDupDataOut.MDL_DOC_ID") == col("lnkKMdllAmndmntExt.MDL_DOC_ID"),
        col("lnkRemDupDataOut.POL_NO") == col("lnkKMdllAmndmntExt.POL_NO"),
        col("lnkRemDupDataOut.AMNDMNT_ID") == col("lnkKMdllAmndmntExt.AMNDMNT_ID"),
        col("lnkRemDupDataOut.AMNDMNT_EFF_DT") == col("lnkKMdllAmndmntExt.AMNDMNT_EFF_DT"),
        col("lnkRemDupDataOut.SRC_SYS_CD") == col("lnkKMdllAmndmntExt.SRC_SYS_CD")
    ],
    how="left"
).select(
    col("lnkRemDupDataOut.MDL_DOC_ID").alias("MDL_DOC_ID"),
    col("lnkRemDupDataOut.POL_NO").alias("POL_NO"),
    col("lnkRemDupDataOut.AMNDMNT_ID").alias("AMNDMNT_ID"),
    col("lnkRemDupDataOut.AMNDMNT_EFF_DT").alias("AMNDMNT_EFF_DT"),
    col("lnkRemDupDataOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("lnkKMdllAmndmntExt.MDL_POL_AMNDMNT_SK").alias("MDL_POL_AMNDMNT_SK"),
    col("lnkKMdllAmndmntExt.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK")
)

# CTransformerStage: xfm_PKEYgen
df_enriched = df_jn_MDL_POL_AMNDMNT

# Apply SurrogateKeyGen to MDL_POL_AMNDMNT_SK where needed
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"MDL_POL_AMNDMNT_SK",<schema>,<secret_name>)

# lnk_KMdlPolAmndmnt_New: constraint => MDL_POL_AMNDMNT_SK is null or 0
df_lnk_KMdlPolAmndmnt_New = df_enriched.filter(
    (col("MDL_POL_AMNDMNT_SK").isNull()) | (col("MDL_POL_AMNDMNT_SK") == 0)
).select(
    col("MDL_DOC_ID"),
    col("POL_NO"),
    col("AMNDMNT_ID"),
    col("AMNDMNT_EFF_DT"),
    col("SRC_SYS_CD"),
    col("MDL_POL_AMNDMNT_SK"),
    lit(IDSRunCycle).alias("CRT_RUN_CYC_EXCTN_SK")
)

# lnkPKEYxfmOut
df_lnkPKEYxfmOut = (
    df_enriched
    .withColumn(
        "CRT_RUN_CYC_EXCTN_SK",
        when(
            (col("MDL_POL_AMNDMNT_SK").isNull()) | (col("MDL_POL_AMNDMNT_SK") == 0),
            lit(IDSRunCycle)
        ).otherwise(col("CRT_RUN_CYC_EXCTN_SK"))
    )
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(IDSRunCycle))
    .select(
        col("MDL_POL_AMNDMNT_SK"),
        col("MDL_DOC_ID"),
        col("POL_NO"),
        col("AMNDMNT_ID"),
        col("AMNDMNT_EFF_DT"),
        col("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("SRC_SYS_CD")
    )
)

# DB2ConnectorPX: db2_K_MdlPolAmndmntLoad (upsert logic via merge)
# 1) Drop temp table if exists
execute_dml(
    "DROP TABLE IF EXISTS STAGING.NfmIdsMdlPolAmndmntPkey_db2_K_MdlPolAmndmntLoad_temp",
    jdbc_url,
    jdbc_props
)

# 2) Write df_lnk_KMdlPolAmndmnt_New to the temp table
df_temp_db2_K_MdlPolAmndmntLoad = df_lnk_KMdlPolAmndmnt_New.select(
    "MDL_DOC_ID",
    "POL_NO",
    "AMNDMNT_ID",
    "AMNDMNT_EFF_DT",
    "SRC_SYS_CD",
    "MDL_POL_AMNDMNT_SK",
    "CRT_RUN_CYC_EXCTN_SK"
)
(
    df_temp_db2_K_MdlPolAmndmntLoad.write.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", "STAGING.NfmIdsMdlPolAmndmntPkey_db2_K_MdlPolAmndmntLoad_temp")
    .mode("overwrite")
    .save()
)

# 3) Merge statement
merge_sql_db2_K_MdlPolAmndmntLoad = f"""
MERGE INTO {IDSOwner}.K_MDL_POL_AMNDMNT AS T
USING STAGING.NfmIdsMdlPolAmndmntPkey_db2_K_MdlPolAmndmntLoad_temp AS S
ON
  T.MDL_DOC_ID=S.MDL_DOC_ID
  AND T.POL_NO=S.POL_NO
  AND T.AMNDMNT_ID=S.AMNDMNT_ID
  AND T.AMNDMNT_EFF_DT=S.AMNDMNT_EFF_DT
  AND T.SRC_SYS_CD=S.SRC_SYS_CD
WHEN MATCHED THEN UPDATE SET
  T.MDL_POL_AMNDMNT_SK = S.MDL_POL_AMNDMNT_SK,
  T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK
WHEN NOT MATCHED THEN INSERT (
  MDL_DOC_ID,
  POL_NO,
  AMNDMNT_ID,
  AMNDMNT_EFF_DT,
  SRC_SYS_CD,
  MDL_POL_AMNDMNT_SK,
  CRT_RUN_CYC_EXCTN_SK
)
VALUES (
  S.MDL_DOC_ID,
  S.POL_NO,
  S.AMNDMNT_ID,
  S.AMNDMNT_EFF_DT,
  S.SRC_SYS_CD,
  S.MDL_POL_AMNDMNT_SK,
  S.CRT_RUN_CYC_EXCTN_SK
);
"""
execute_dml(merge_sql_db2_K_MdlPolAmndmntLoad, jdbc_url, jdbc_props)

# PxJoin Stage: jn_PKEYs (inner join)
df_jn_PKEYs = df_lnkFullDataJnIn.alias("lnkFullDataJnIn").join(
    df_lnkPKEYxfmOut.alias("lnkPKEYxfmOut"),
    on=[
        col("lnkFullDataJnIn.MDL_DOC_ID") == col("lnkPKEYxfmOut.MDL_DOC_ID"),
        col("lnkFullDataJnIn.POL_NO") == col("lnkPKEYxfmOut.POL_NO"),
        col("lnkFullDataJnIn.AMNDMNT_ID") == col("lnkPKEYxfmOut.AMNDMNT_ID"),
        col("lnkFullDataJnIn.AMNDMNT_EFF_DT") == col("lnkPKEYxfmOut.AMNDMNT_EFF_DT"),
        col("lnkFullDataJnIn.SRC_SYS_CD") == col("lnkPKEYxfmOut.SRC_SYS_CD")
    ],
    how="inner"
).select(
    col("lnkPKEYxfmOut.MDL_POL_AMNDMNT_SK").alias("MDL_POL_AMNDMNT_SK"),
    col("lnkFullDataJnIn.MDL_DOC_ID").alias("MDL_DOC_ID"),
    col("lnkFullDataJnIn.POL_NO").alias("POL_NO"),
    col("lnkFullDataJnIn.AMNDMNT_ID").alias("AMNDMNT_ID"),
    col("lnkFullDataJnIn.AMNDMNT_EFF_DT").alias("AMNDMNT_EFF_DT"),
    col("lnkFullDataJnIn.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("lnkPKEYxfmOut.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("lnkPKEYxfmOut.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("lnkFullDataJnIn.AMNDMNT_EFF_ON_GRP_RNWL_IN").alias("AMNDMNT_EFF_ON_GRP_RNWL_IN"),
    col("lnkFullDataJnIn.AMNDMNT_STATUS_ID").alias("AMNDMNT_STTUS_ID"),
    col("lnkFullDataJnIn.AMNDMNT_TERM_DT").alias("AMNDMNT_TERM_DT"),
    col("lnkFullDataJnIn.MDL_DOC_EFF_DT").alias("MDL_DOC_EFF_DT")
)

# PxSequentialFile Stage: seq_MDL_POL_AMNDMNT_PKEY
df_final_seq_MDL_POL_AMNDMNT_PKEY = df_jn_PKEYs.select(
    "MDL_POL_AMNDMNT_SK",
    "MDL_DOC_ID",
    "POL_NO",
    "AMNDMNT_ID",
    "AMNDMNT_EFF_DT",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "AMNDMNT_EFF_ON_GRP_RNWL_IN",
    "AMNDMNT_STTUS_ID",
    "AMNDMNT_TERM_DT",
    "MDL_DOC_EFF_DT"
)

write_files(
    df_final_seq_MDL_POL_AMNDMNT_PKEY,
    f"{adls_path}/key/MDLPOLAMNDMNT.{SrcSysCd}.pkey.{RunID}.dat",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)