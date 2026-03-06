# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC 
# MAGIC Bhoomi Dasari         2014-04-18               5345                             Original Programming                                                                             IntegrateWrhsDevl      Sharon Andrew       2014-10-21

# MAGIC Left Outer Join Here
# MAGIC New PKEYs are generated in this transformer. A database sequence will be used to create next PKEY value.
# MAGIC Land into Seq File for the FKEY job
# MAGIC Table PRM_RATE Insert new SKEY rows generated as part of this run. This is a database insert operation.
# MAGIC Remove Duplicates stage takes care of getting rid of duplicate rows with the same Natural key get into the PKEYing step.
# MAGIC A concious effort need to be made to evaluate which one of the duplicate set row to drop is a case by case basis decision.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, when, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------

ids_secret_name = get_widget_value('ids_secret_name','')
IdsOwner = get_widget_value('IDSOwner','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')

df_ds_PRM_RATE_Xfrm = spark.read.parquet(
    f"{adls_path}/ds/PRM_RATE.{SrcSysCd}.xfrm.{RunID}.parquet"
)

df_cpy_MultiStreams_lnkRemDupDataIn = df_ds_PRM_RATE_Xfrm.select(
    "SRC_SYS_CD",
    "PRM_RATE_UNIQ_KEY"
)

df_cpy_MultiStreams_lnkFullDataJnIn = df_ds_PRM_RATE_Xfrm.select(
    "PRI_NAT_KEY_STRING",
    "FIRST_RECYC_TS",
    "SRC_SYS_CD",
    "PRM_RATE_UNIQ_KEY",
    "SRC_SYS_CD_SK",
    "PRM_RATE_AGE_ENTY_CD",
    "PRM_RATE_PRM_TYP_CD",
    "EFF_DT",
    "TERM_DT",
    "PRM_RATE_PFX",
    "PRM_RATE_DESC",
    "PRM_RATE_MOD_ID",
    "MAX_CHLDRN_RATED_NO",
    "PRM_RATE_BSS_NO",
    "PRM_RATE_KEY_STRCT_NO",
    "PRM_RATE_METH",
    "PRM_RATE_COL_STRUCT",
    "PRM_RATE_MOD_TYPE",
    "TIER_AGE_BAND_FROM_NO",
    "TIER_AGE_BAND_THRU_NO"
)

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = (
    f"SELECT PRM_RATE_UNIQ_KEY, SRC_SYS_CD, CRT_RUN_CYC_EXCTN_SK, PRM_RATE_SK "
    f"FROM {IdsOwner}.K_PRM_RATE"
)
df_db2_KPrmRateExt = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_rdp_NaturalKeys = dedup_sort(
    df_cpy_MultiStreams_lnkRemDupDataIn,
    ["SRC_SYS_CD", "PRM_RATE_UNIQ_KEY"],
    []
)

df_jn_PrmRate_join = df_rdp_NaturalKeys.alias("lnkRemDupDataOut").join(
    df_db2_KPrmRateExt.alias("lnkKPrmRateExt"),
    on=[
        col("lnkRemDupDataOut.SRC_SYS_CD") == col("lnkKPrmRateExt.SRC_SYS_CD"),
        col("lnkRemDupDataOut.PRM_RATE_UNIQ_KEY") == col("lnkKPrmRateExt.PRM_RATE_UNIQ_KEY"),
    ],
    how="left"
)

df_jn_PrmRate = df_jn_PrmRate_join.select(
    col("lnkRemDupDataOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("lnkRemDupDataOut.PRM_RATE_UNIQ_KEY").alias("PRM_RATE_UNIQ_KEY"),
    col("lnkKPrmRateExt.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("lnkKPrmRateExt.PRM_RATE_SK").alias("PRM_RATE_SK")
)

df_enriched = df_jn_PrmRate.withColumn(
    "svPrmRateSK",
    when(col("PRM_RATE_SK").isNull(), lit(None)).otherwise(col("PRM_RATE_SK"))
)

df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"svPrmRateSK",<schema>,<secret_name>)

df_lnk_KPrmRate_new = df_enriched.filter(col("PRM_RATE_SK").isNull()).select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("PRM_RATE_UNIQ_KEY").alias("PRM_RATE_UNIQ_KEY"),
    lit(IDSRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    col("svPrmRateSK").alias("PRM_RATE_SK")
)

df_lnkPKEYxfmOut = df_enriched.select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("PRM_RATE_UNIQ_KEY").alias("PRM_RATE_UNIQ_KEY"),
    when(col("PRM_RATE_SK").isNull(), IDSRunCycle).otherwise(col("CRT_RUN_CYC_EXCTN_SK")).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(IDSRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("svPrmRateSK").alias("PRM_RATE_SK")
)

temp_table = "STAGING.IdsPrmRatePKey_db2_K_PrmRateLoad_temp"
drop_sql = f"DROP TABLE IF EXISTS {temp_table}"
execute_dml(drop_sql, jdbc_url, jdbc_props)

df_lnk_KPrmRate_new.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", temp_table) \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE {IdsOwner}.K_PRM_RATE AS T
USING {temp_table} AS S
ON T.SRC_SYS_CD = S.SRC_SYS_CD AND T.PRM_RATE_UNIQ_KEY = S.PRM_RATE_UNIQ_KEY
WHEN MATCHED THEN
  UPDATE SET T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
             T.PRM_RATE_SK = S.PRM_RATE_SK
WHEN NOT MATCHED THEN
  INSERT (SRC_SYS_CD, PRM_RATE_UNIQ_KEY, CRT_RUN_CYC_EXCTN_SK, PRM_RATE_SK)
  VALUES (S.SRC_SYS_CD, S.PRM_RATE_UNIQ_KEY, S.CRT_RUN_CYC_EXCTN_SK, S.PRM_RATE_SK);
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

df_jn_PKEYs_join = df_cpy_MultiStreams_lnkFullDataJnIn.alias("lnkFullDataJnIn").join(
    df_lnkPKEYxfmOut.alias("lnkPKEYxfmOut"),
    on=[
        col("lnkFullDataJnIn.SRC_SYS_CD") == col("lnkPKEYxfmOut.SRC_SYS_CD"),
        col("lnkFullDataJnIn.PRM_RATE_UNIQ_KEY") == col("lnkPKEYxfmOut.PRM_RATE_UNIQ_KEY"),
    ],
    how="inner"
)

df_jn_PKEYs = df_jn_PKEYs_join.select(
    col("lnkPKEYxfmOut.PRM_RATE_SK").alias("PRM_RATE_SK"),
    col("lnkFullDataJnIn.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    col("lnkFullDataJnIn.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    col("lnkFullDataJnIn.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("lnkFullDataJnIn.PRM_RATE_UNIQ_KEY").alias("PRM_RATE_UNIQ_KEY"),
    col("lnkPKEYxfmOut.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("lnkPKEYxfmOut.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("lnkFullDataJnIn.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("lnkFullDataJnIn.PRM_RATE_AGE_ENTY_CD").alias("PRM_RATE_AGE_ENTY_CD"),
    col("lnkFullDataJnIn.PRM_RATE_PRM_TYP_CD").alias("PRM_RATE_PRM_TYP_CD"),
    col("lnkFullDataJnIn.EFF_DT").alias("EFF_DT"),
    col("lnkFullDataJnIn.TERM_DT").alias("TERM_DT"),
    col("lnkFullDataJnIn.PRM_RATE_PFX").alias("PRM_RATE_PFX"),
    col("lnkFullDataJnIn.PRM_RATE_DESC").alias("PRM_RATE_DESC"),
    col("lnkFullDataJnIn.PRM_RATE_MOD_ID").alias("PRM_RATE_MOD_ID"),
    col("lnkFullDataJnIn.MAX_CHLDRN_RATED_NO").alias("MAX_CHLDRN_RATED_NO"),
    col("lnkFullDataJnIn.PRM_RATE_BSS_NO").alias("PRM_RATE_BSS_NO"),
    col("lnkFullDataJnIn.PRM_RATE_KEY_STRCT_NO").alias("PRM_RATE_KEY_STRCT_NO"),
    col("lnkFullDataJnIn.PRM_RATE_METH").alias("PRM_RATE_METH"),
    col("lnkFullDataJnIn.PRM_RATE_COL_STRUCT").alias("PRM_RATE_COL_STRUCT"),
    col("lnkFullDataJnIn.PRM_RATE_MOD_TYPE").alias("PRM_RATE_MOD_TYPE"),
    col("lnkFullDataJnIn.TIER_AGE_BAND_FROM_NO").alias("TIER_AGE_BAND_FROM_NO"),
    col("lnkFullDataJnIn.TIER_AGE_BAND_THRU_NO").alias("TIER_AGE_BAND_THRU_NO")
)

df_seq_PRM_RATE_PKey = df_jn_PKEYs.withColumn(
    "EFF_DT", rpad(col("EFF_DT"), 10, " ")
).withColumn(
    "TERM_DT", rpad(col("TERM_DT"), 10, " ")
).withColumn(
    "PRM_RATE_METH", rpad(col("PRM_RATE_METH"), 1, " ")
).withColumn(
    "PRM_RATE_COL_STRUCT", rpad(col("PRM_RATE_COL_STRUCT"), 1, " ")
).withColumn(
    "PRM_RATE_MOD_TYPE", rpad(col("PRM_RATE_MOD_TYPE"), 1, " ")
)

df_seq_PRM_RATE_PKey = df_seq_PRM_RATE_PKey.select(
    "PRM_RATE_SK",
    "PRI_NAT_KEY_STRING",
    "FIRST_RECYC_TS",
    "SRC_SYS_CD",
    "PRM_RATE_UNIQ_KEY",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "SRC_SYS_CD_SK",
    "PRM_RATE_AGE_ENTY_CD",
    "PRM_RATE_PRM_TYP_CD",
    "EFF_DT",
    "TERM_DT",
    "PRM_RATE_PFX",
    "PRM_RATE_DESC",
    "PRM_RATE_MOD_ID",
    "MAX_CHLDRN_RATED_NO",
    "PRM_RATE_BSS_NO",
    "PRM_RATE_KEY_STRCT_NO",
    "PRM_RATE_METH",
    "PRM_RATE_COL_STRUCT",
    "PRM_RATE_MOD_TYPE",
    "TIER_AGE_BAND_FROM_NO",
    "TIER_AGE_BAND_THRU_NO"
)

write_files(
    df_seq_PRM_RATE_PKey,
    f"{adls_path}/key/PRM_RATE.{SrcSysCd}.pkey.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)