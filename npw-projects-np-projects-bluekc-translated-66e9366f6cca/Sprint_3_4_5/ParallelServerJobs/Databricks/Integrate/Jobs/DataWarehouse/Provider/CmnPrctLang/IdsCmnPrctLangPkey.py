# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# MAGIC Archana Palivela       2014-07-17              5345                             Original Programming                                                                        IntegrateWrhsDevl       Kalyan Neelam             2014-12-31

# MAGIC Job Name: IdsCmnPrctLangPkey
# MAGIC Remove Duplicates stage takes care of getting rid of duplicate rows with the same Natural key get into the PKEYing step.
# MAGIC A concious effort need to be made to evaluate which one of the duplicate set row to drop is a case by case basis decision.
# MAGIC Table K_CMN_PRCT_LANG      Insert new SKEY rows generated as part of this run. This is a database insert operation.
# MAGIC Audit fields are added into this File
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


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')

df_ds_CMN_PRCT_LANG_Xfm = spark.read.parquet(
    f"{adls_path}/ds/CMN_PRCT_LANG.{SrcSysCd}.xfrm.{RunID}.parquet"
)

df_cpy_MultiStreams_lnkRemDupDataIn = df_ds_CMN_PRCT_LANG_Xfm.select(
    F.col("CMN_PRCT_ID").alias("CMN_PRCT_ID"),
    F.col("LANG_SEQ_NO").alias("LANG_SEQ_NO"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD")
)

df_cpy_MultiStreams_lnkFullDataJnIn = df_ds_CMN_PRCT_LANG_Xfm.select(
    F.col("PRI_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS"),
    F.col("CMN_PRCT_ID").alias("CMN_PRCT_ID"),
    F.col("LANG_SEQ_NO").alias("LANG_SEQ_NO"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("SRC_SYS_CD_SK"),
    F.col("LANG_DESC"),
    F.col("CMN_PRCT_LKP_SRC_SYS_CD_FACETS"),
    F.col("CMN_PRCT_LKP_SRC_SYS_CD_VCAC")
)

df_rdp_NaturalKeys_temp = dedup_sort(
    df_cpy_MultiStreams_lnkRemDupDataIn,
    ["CMN_PRCT_ID", "LANG_SEQ_NO", "SRC_SYS_CD"],
    []
)
df_rdp_NaturalKeys_lnkRemDupDataOut = df_rdp_NaturalKeys_temp.select(
    F.col("CMN_PRCT_ID").alias("CMN_PRCT_ID"),
    F.col("LANG_SEQ_NO").alias("LANG_SEQ_NO"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD")
)

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"SELECT CMN_PRCT_ID, LANG_SEQ_NO, SRC_SYS_CD, CRT_RUN_CYC_EXCTN_SK, CMN_PRCT_LANG_SK FROM {IDSOwner}.K_CMN_PRCT_LANG"
df_db2_KCmnPrctLangExt = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_jn_CmnPrctLang = df_rdp_NaturalKeys_lnkRemDupDataOut.alias("lnkRemDupDataOut").join(
    df_db2_KCmnPrctLangExt.alias("lnkKCmnPrctLangExt"),
    [
        F.col("lnkRemDupDataOut.CMN_PRCT_ID") == F.col("lnkKCmnPrctLangExt.CMN_PRCT_ID"),
        F.col("lnkRemDupDataOut.LANG_SEQ_NO") == F.col("lnkKCmnPrctLangExt.LANG_SEQ_NO"),
        F.col("lnkRemDupDataOut.SRC_SYS_CD") == F.col("lnkKCmnPrctLangExt.SRC_SYS_CD")
    ],
    "left"
)
df_jn_CmnPrctLang_lnk_CmnPrctLang_JoinOut = df_jn_CmnPrctLang.select(
    F.col("lnkRemDupDataOut.CMN_PRCT_ID").alias("CMN_PRCT_ID"),
    F.col("lnkRemDupDataOut.LANG_SEQ_NO").alias("LANG_SEQ_NO"),
    F.col("lnkRemDupDataOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnkKCmnPrctLangExt.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("lnkKCmnPrctLangExt.CMN_PRCT_LANG_SK").alias("CMN_PRCT_LANG_SK")
)

df_xfm_base = df_jn_CmnPrctLang_lnk_CmnPrctLang_JoinOut.withColumn(
    "original_sk_is_null",
    F.col("CMN_PRCT_LANG_SK").isNull()
)
df_enriched = SurrogateKeyGen(df_xfm_base,<DB sequence name>,"CMN_PRCT_LANG_SK",<schema>,<secret_name>)

df_xfm_PKEYgen_lnk_KCmnPrctLang_New = df_enriched.filter(
    F.col("original_sk_is_null") == True
).select(
    F.col("CMN_PRCT_ID"),
    F.col("LANG_SEQ_NO"),
    F.col("SRC_SYS_CD"),
    F.lit(IDSRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("CMN_PRCT_LANG_SK").alias("CMN_PRCT_LANG_SK")
)

df_xfm_PKEYgen_lnkPKEYxfmOut = df_enriched.select(
    F.col("CMN_PRCT_ID"),
    F.col("LANG_SEQ_NO"),
    F.col("SRC_SYS_CD"),
    F.col("CMN_PRCT_LANG_SK"),
    F.when(
        F.col("original_sk_is_null") == True,
        F.lit(IDSRunCycle)
    ).otherwise(
        F.col("CRT_RUN_CYC_EXCTN_SK")
    ).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(IDSRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

df_db2_K_CmnPrctLangLoad_temp = df_xfm_PKEYgen_lnk_KCmnPrctLang_New
execute_dml(
    "DROP TABLE IF EXISTS STAGING.IdsCmnPrctLangPkey_db2_K_CmnPrctLangLoad_temp",
    jdbc_url,
    jdbc_props
)
df_db2_K_CmnPrctLangLoad_temp.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.IdsCmnPrctLangPkey_db2_K_CmnPrctLangLoad_temp") \
    .mode("append") \
    .save()

merge_sql = f"""
MERGE INTO {IDSOwner}.K_CMN_PRCT_LANG AS T
USING STAGING.IdsCmnPrctLangPkey_db2_K_CmnPrctLangLoad_temp AS S
ON
    T.CMN_PRCT_ID = S.CMN_PRCT_ID
    AND T.LANG_SEQ_NO = S.LANG_SEQ_NO
    AND T.SRC_SYS_CD = S.SRC_SYS_CD
WHEN MATCHED THEN
    UPDATE SET
        T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
        T.CMN_PRCT_LANG_SK = S.CMN_PRCT_LANG_SK
WHEN NOT MATCHED THEN
    INSERT (
        CMN_PRCT_ID,
        LANG_SEQ_NO,
        SRC_SYS_CD,
        CRT_RUN_CYC_EXCTN_SK,
        CMN_PRCT_LANG_SK
    )
    VALUES (
        S.CMN_PRCT_ID,
        S.LANG_SEQ_NO,
        S.SRC_SYS_CD,
        S.CRT_RUN_CYC_EXCTN_SK,
        S.CMN_PRCT_LANG_SK
    );
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

df_jn_PKEYs = df_cpy_MultiStreams_lnkFullDataJnIn.alias("lnkFullDataJnIn").join(
    df_xfm_PKEYgen_lnkPKEYxfmOut.alias("lnkPKEYxfmOut"),
    [
        F.col("lnkFullDataJnIn.CMN_PRCT_ID") == F.col("lnkPKEYxfmOut.CMN_PRCT_ID"),
        F.col("lnkFullDataJnIn.LANG_SEQ_NO") == F.col("lnkPKEYxfmOut.LANG_SEQ_NO"),
        F.col("lnkFullDataJnIn.SRC_SYS_CD") == F.col("lnkPKEYxfmOut.SRC_SYS_CD")
    ],
    "inner"
)
df_jn_PKEYs_Lnk_dsCmnPrctLangPkey_OutAbc = df_jn_PKEYs.select(
    F.col("lnkFullDataJnIn.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("lnkFullDataJnIn.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("lnkPKEYxfmOut.CMN_PRCT_LANG_SK").alias("CMN_PRCT_LANG_SK"),
    F.col("lnkFullDataJnIn.CMN_PRCT_ID").alias("CMN_PRCT_ID"),
    F.col("lnkFullDataJnIn.LANG_SEQ_NO").alias("LANG_SEQ_NO"),
    F.col("lnkFullDataJnIn.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnkPKEYxfmOut.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("lnkPKEYxfmOut.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("lnkFullDataJnIn.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("lnkFullDataJnIn.LANG_DESC").alias("LANG_DESC"),
    F.col("lnkFullDataJnIn.CMN_PRCT_LKP_SRC_SYS_CD_FACETS").alias("CMN_PRCT_LKP_SRC_SYS_CD_FACETS"),
    F.col("lnkFullDataJnIn.CMN_PRCT_LKP_SRC_SYS_CD_VCAC").alias("CMN_PRCT_LKP_SRC_SYS_CD_VCAC")
)

df_seq_CMN_PRCT_LANG_PKEY = df_jn_PKEYs_Lnk_dsCmnPrctLangPkey_OutAbc.select(
    F.rpad(F.col("PRI_NAT_KEY_STRING"), <...>, " ").alias("PRI_NAT_KEY_STRING"),
    F.rpad(F.col("FIRST_RECYC_TS"), <...>, " ").alias("FIRST_RECYC_TS"),
    F.rpad(F.col("CMN_PRCT_LANG_SK").cast("string"), <...>, " ").alias("CMN_PRCT_LANG_SK"),
    F.rpad(F.col("CMN_PRCT_ID"), <...>, " ").alias("CMN_PRCT_ID"),
    F.rpad(F.col("LANG_SEQ_NO"), <...>, " ").alias("LANG_SEQ_NO"),
    F.rpad(F.col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_SK").cast("string"), <...>, " ").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").cast("string"), <...>, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.rpad(F.col("SRC_SYS_CD_SK").cast("string"), <...>, " ").alias("SRC_SYS_CD_SK"),
    F.rpad(F.col("LANG_DESC"), <...>, " ").alias("LANG_DESC"),
    F.rpad(F.col("CMN_PRCT_LKP_SRC_SYS_CD_FACETS"), <...>, " ").alias("CMN_PRCT_LKP_SRC_SYS_CD_FACETS"),
    F.rpad(F.col("CMN_PRCT_LKP_SRC_SYS_CD_VCAC"), <...>, " ").alias("CMN_PRCT_LKP_SRC_SYS_CD_VCAC")
)

write_files(
    df_seq_CMN_PRCT_LANG_PKEY,
    f"{adls_path}/key/CMN_PRCT_LANG.{SrcSysCd}.pkey.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)