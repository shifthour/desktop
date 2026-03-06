# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC 
# MAGIC Bhoomi Dasari         2014-04-18               5345                             Original Programming                                                                             IntegrateWrhsDevl    Sharon Andrew       2014-10-21

# MAGIC Left Outer Join Here
# MAGIC New PKEYs are generated in this transformer. A database sequence will be used to create next PKEY value.
# MAGIC Land into Seq File for the FKEY job
# MAGIC Table PRM_RATE_AGE_CAT            Insert new SKEY rows generated as part of this run. This is a database insert operation.
# MAGIC Remove Duplicates stage takes care of getting rid of duplicate rows with the same Natural key get into the PKEYing step.
# MAGIC A concious effort need to be made to evaluate which one of the duplicate set row to drop
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


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')
IDSRunCycle = get_widget_value('IDSRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

df_ds_PRM_RATE_AGE_CAT_Xfm = spark.read.parquet(f"{adls_path}/ds/PRM_RATE_AGE_CAT.{SrcSysCd}.xfrm.{RunID}.parquet")

df_cpy_MultiStreams_output1 = df_ds_PRM_RATE_AGE_CAT_Xfm.select(
    F.col("AGE_BAND_REF_ID").alias("AGE_BAND_REF_ID"),
    F.col("AGE_FROM_NO").alias("AGE_FROM_NO"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD")
)

df_cpy_MultiStreams_output2 = df_ds_PRM_RATE_AGE_CAT_Xfm.select(
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("AGE_BAND_REF_ID").alias("AGE_BAND_REF_ID"),
    F.col("AGE_FROM_NO").alias("AGE_FROM_NO"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("PRM_1_AMT").alias("PRM_1_AMT"),
    F.col("PRM_2_AMT").alias("PRM_2_AMT"),
    F.col("PRM_3_AMT").alias("PRM_3_AMT"),
    F.col("PRM_4_AMT").alias("PRM_4_AMT"),
    F.col("PRM_5_AMT").alias("PRM_5_AMT"),
    F.col("PRM_6_AMT").alias("PRM_6_AMT"),
    F.col("AGE_THRU_NO").alias("AGE_THRU_NO")
)

extract_query = (
    "SELECT AGE_BAND_REF_ID, AGE_FROM_NO, SRC_SYS_CD, CRT_RUN_CYC_EXCTN_SK, PRM_RATE_AGE_CAT_SK "
    "FROM " + IDOwner + ".K_PRM_RATE_AGE_CAT"
).replace("IDOwner", IDSOwner)
df_db2_KPrmRateAgeCatExt = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_rdp_NaturalKeys = dedup_sort(
    df_cpy_MultiStreams_output1,
    partition_cols=["AGE_BAND_REF_ID","AGE_FROM_NO","SRC_SYS_CD"],
    sort_cols=[]
)

df_jn_PrmRateAgeCat = df_rdp_NaturalKeys.alias("lnkRemDupDataOut").join(
    df_db2_KPrmRateAgeCatExt.alias("lnkKPrmRateExt"),
    [
        F.col("lnkRemDupDataOut.AGE_BAND_REF_ID") == F.col("lnkKPrmRateExt.AGE_BAND_REF_ID"),
        F.col("lnkRemDupDataOut.AGE_FROM_NO") == F.col("lnkKPrmRateExt.AGE_FROM_NO"),
        F.col("lnkRemDupDataOut.SRC_SYS_CD") == F.col("lnkKPrmRateExt.SRC_SYS_CD")
    ],
    "left"
).select(
    F.col("lnkRemDupDataOut.AGE_BAND_REF_ID").alias("AGE_BAND_REF_ID"),
    F.col("lnkRemDupDataOut.AGE_FROM_NO").alias("AGE_FROM_NO"),
    F.col("lnkRemDupDataOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnkKPrmRateExt.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("lnkKPrmRateExt.PRM_RATE_AGE_CAT_SK").alias("PRM_RATE_AGE_CAT_SK")
)

df_enriched = df_jn_PrmRateAgeCat.select(
    F.col("AGE_BAND_REF_ID"),
    F.col("AGE_FROM_NO"),
    F.col("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("PRM_RATE_AGE_CAT_SK").alias("PRM_RATE_AGE_CAT_SK_original")
)

df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"PRM_RATE_AGE_CAT_SK_original",<schema>,<secret_name>)

df_xfm_pkeygen_new = df_enriched.where(
    F.col("PRM_RATE_AGE_CAT_SK_original").isNotNull() & 
    F.col("PRM_RATE_AGE_CAT_SK_original").eqNullSafe(None)
).select(
    F.col("AGE_BAND_REF_ID").alias("AGE_BAND_REF_ID"),
    F.col("AGE_FROM_NO").alias("AGE_FROM_NO"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.lit(IDSRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("PRM_RATE_AGE_CAT_SK_original").alias("PRM_RATE_AGE_CAT_SK")
)

df_xfm_pkeygen_out = df_enriched.select(
    F.col("AGE_BAND_REF_ID").alias("AGE_BAND_REF_ID"),
    F.col("AGE_FROM_NO").alias("AGE_FROM_NO"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.when(
        F.col("PRM_RATE_AGE_CAT_SK_original").isNull(), 
        F.lit(IDSRunCycle)
    ).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK")).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(IDSRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("PRM_RATE_AGE_CAT_SK_original").alias("PRM_RATE_AGE_CAT_SK")
)

staging_table_load = "STAGING.IdsPrmRateAgeCatPKey_db2_K_PrmRateAgeCatLoad_temp"
drop_sql_load = f"DROP TABLE IF EXISTS {staging_table_load}"
execute_dml(drop_sql_load, jdbc_url, jdbc_props)
df_xfm_pkeygen_new.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", staging_table_load) \
    .mode("overwrite") \
    .save()

merge_sql_load = (
    f"MERGE INTO {IDSOwner}.K_PRM_RATE_AGE_CAT AS T "
    f"USING {staging_table_load} AS S "
    f"ON "
    f"(T.AGE_BAND_REF_ID=S.AGE_BAND_REF_ID "
    f"AND T.AGE_FROM_NO=S.AGE_FROM_NO "
    f"AND T.SRC_SYS_CD=S.SRC_SYS_CD) "
    f"WHEN MATCHED THEN UPDATE SET "
    f"T.CRT_RUN_CYC_EXCTN_SK=S.CRT_RUN_CYC_EXCTN_SK,"
    f"T.PRM_RATE_AGE_CAT_SK=S.PRM_RATE_AGE_CAT_SK "
    f"WHEN NOT MATCHED THEN INSERT "
    f"(AGE_BAND_REF_ID,AGE_FROM_NO,SRC_SYS_CD,CRT_RUN_CYC_EXCTN_SK,PRM_RATE_AGE_CAT_SK) VALUES "
    f"(S.AGE_BAND_REF_ID,S.AGE_FROM_NO,S.SRC_SYS_CD,S.CRT_RUN_CYC_EXCTN_SK,S.PRM_RATE_AGE_CAT_SK);"
)
execute_dml(merge_sql_load, jdbc_url, jdbc_props)

df_jn_PKEYs = df_cpy_MultiStreams_output2.alias("lnkFullDataJnIn").join(
    df_xfm_pkeygen_out.alias("lnkPKEYxfmOut"),
    [
        F.col("lnkFullDataJnIn.AGE_BAND_REF_ID") == F.col("lnkPKEYxfmOut.AGE_BAND_REF_ID"),
        F.col("lnkFullDataJnIn.AGE_FROM_NO") == F.col("lnkPKEYxfmOut.AGE_FROM_NO"),
        F.col("lnkFullDataJnIn.SRC_SYS_CD") == F.col("lnkPKEYxfmOut.SRC_SYS_CD")
    ],
    "inner"
).select(
    F.col("lnkFullDataJnIn.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("lnkFullDataJnIn.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("lnkPKEYxfmOut.PRM_RATE_AGE_CAT_SK").alias("PRM_RATE_AGE_CAT_SK"),
    F.col("lnkFullDataJnIn.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnkFullDataJnIn.AGE_BAND_REF_ID").alias("AGE_BAND_REF_ID"),
    F.col("lnkFullDataJnIn.AGE_FROM_NO").alias("AGE_FROM_NO"),
    F.col("lnkPKEYxfmOut.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("lnkPKEYxfmOut.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("lnkFullDataJnIn.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("lnkFullDataJnIn.PRM_1_AMT").alias("PRM_1_AMT"),
    F.col("lnkFullDataJnIn.PRM_2_AMT").alias("PRM_2_AMT"),
    F.col("lnkFullDataJnIn.PRM_3_AMT").alias("PRM_3_AMT"),
    F.col("lnkFullDataJnIn.PRM_4_AMT").alias("PRM_4_AMT"),
    F.col("lnkFullDataJnIn.PRM_5_AMT").alias("PRM_5_AMT"),
    F.col("lnkFullDataJnIn.PRM_6_AMT").alias("PRM_6_AMT"),
    F.col("lnkFullDataJnIn.AGE_THRU_NO").alias("AGE_THRU_NO")
)

df_seq_PRM_RATE_AGE_CAT_PKEY = df_jn_PKEYs.select(
    F.rpad(F.col("PRI_NAT_KEY_STRING"), <...>, " ").alias("PRI_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("PRM_RATE_AGE_CAT_SK").alias("PRM_RATE_AGE_CAT_SK"),
    F.rpad(F.col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    F.rpad(F.col("AGE_BAND_REF_ID"), <...>, " ").alias("AGE_BAND_REF_ID"),
    F.col("AGE_FROM_NO").alias("AGE_FROM_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("PRM_1_AMT").alias("PRM_1_AMT"),
    F.col("PRM_2_AMT").alias("PRM_2_AMT"),
    F.col("PRM_3_AMT").alias("PRM_3_AMT"),
    F.col("PRM_4_AMT").alias("PRM_4_AMT"),
    F.col("PRM_5_AMT").alias("PRM_5_AMT"),
    F.col("PRM_6_AMT").alias("PRM_6_AMT"),
    F.col("AGE_THRU_NO").alias("AGE_THRU_NO")
)

write_files(
    df_seq_PRM_RATE_AGE_CAT_PKEY,
    f"{adls_path}/key/PRM_RATE_AGE_CAT.{SrcSysCd}.pkey.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)