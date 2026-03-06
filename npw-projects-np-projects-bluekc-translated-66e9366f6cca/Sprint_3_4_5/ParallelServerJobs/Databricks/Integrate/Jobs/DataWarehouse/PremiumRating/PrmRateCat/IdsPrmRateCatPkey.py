# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC 
# MAGIC Jag Yelavarthi      2014-04-18               5345                             Original Programming                                                                             IntegrateWrhsDevl    Sharon Andrew       2014-10-21

# MAGIC Remove Duplicates stage takes care of getting rid of duplicate rows with the same Natural key get into the PKEYing step.
# MAGIC A concious effort need to be made to evaluate which one of the duplicate set row to drop is a case by case basis decision.
# MAGIC Table K_PRM_RATE_CAT.               Insert new SKEY rows generated as part of this run. This is a database insert operation.
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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')

# Read ds_PrmRateCatExtr (.ds -> .parquet)
df_ds_PrmRateCatExtr = spark.read.parquet(f"{adls_path}/ds/PRM_RATE_CAT.{SrcSysCd}.xfrm.{RunID}.parquet")

# cpy_MultiStreams - output lnkRemDupDataIn
df_cpy_MultiStreams_lnkRemDupDataIn = df_ds_PrmRateCatExtr.select(
    F.col("PRM_RATE_UNIQ_KEY").alias("PRM_RATE_UNIQ_KEY"),
    F.col("TIER_MOD_ID").alias("TIER_MOD_ID"),
    F.col("PRM_RATE_CAT_GNDR_CD").alias("PRM_RATE_CAT_GNDR_CD"),
    F.col("PRM_RATE_CAT_SMKR_CD").alias("PRM_RATE_CAT_SMKR_CD"),
    F.col("EFF_DT").alias("EFF_DT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD")
)

# cpy_MultiStreams - output lnkFullDataJnIn
df_cpy_MultiStreams_lnkFullDataJnIn = df_ds_PrmRateCatExtr.select(
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("PRM_RATE_UNIQ_KEY").alias("PRM_RATE_UNIQ_KEY"),
    F.col("TIER_MOD_ID").alias("TIER_MOD_ID"),
    F.col("PRM_RATE_CAT_GNDR_CD").alias("PRM_RATE_CAT_GNDR_CD"),
    F.col("PRM_RATE_CAT_SMKR_CD").alias("PRM_RATE_CAT_SMKR_CD"),
    F.col("EFF_DT").alias("EFF_DT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("PRM_RATE_SK").alias("PRM_RATE_SK"),
    F.col("TERM_DT").alias("TERM_DT"),
    F.col("AGE_BAND_REF_ID").alias("AGE_BAND_REF_ID")
)

# rdp_NaturalKeys (PxRemDup)
df_rdp_NaturalKeys_temp = dedup_sort(
    df_cpy_MultiStreams_lnkRemDupDataIn,
    partition_cols=["PRM_RATE_UNIQ_KEY", "TIER_MOD_ID", "PRM_RATE_CAT_GNDR_CD", "PRM_RATE_CAT_SMKR_CD", "EFF_DT", "SRC_SYS_CD"],
    sort_cols=[]
)
df_rdp_NaturalKeys_lnkRemDupDataOut = df_rdp_NaturalKeys_temp.select(
    F.col("PRM_RATE_UNIQ_KEY").alias("PRM_RATE_UNIQ_KEY"),
    F.col("TIER_MOD_ID").alias("TIER_MOD_ID"),
    F.col("PRM_RATE_CAT_GNDR_CD").alias("PRM_RATE_CAT_GNDR_CD"),
    F.col("PRM_RATE_CAT_SMKR_CD").alias("PRM_RATE_CAT_SMKR_CD"),
    F.col("EFF_DT").alias("EFF_DT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD")
)

# db2_KPrmRateCatExt
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_db2_KPrmRateCatExt = f"""
SELECT 
 PRM_RATE_UNIQ_KEY,
 TIER_MOD_ID,
 PRM_RATE_CAT_GNDR_CD,
 PRM_RATE_CAT_SMKR_CD,
 EFF_DT_SK AS EFF_DT,
 SRC_SYS_CD,
 CRT_RUN_CYC_EXCTN_SK,
 PRM_RATE_CAT_SK
FROM {IDSOwner}.K_PRM_RATE_CAT
"""
df_db2_KPrmRateCatExt = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_KPrmRateCatExt)
    .load()
)

# jn_PrmRateCat (left outer join)
df_jn_PrmRateCat = (
    df_rdp_NaturalKeys_lnkRemDupDataOut.alias("ld")
    .join(
        df_db2_KPrmRateCatExt.alias("rd"),
        [
            F.col("ld.PRM_RATE_UNIQ_KEY") == F.col("rd.PRM_RATE_UNIQ_KEY"),
            F.col("ld.TIER_MOD_ID") == F.col("rd.TIER_MOD_ID"),
            F.col("ld.PRM_RATE_CAT_GNDR_CD") == F.col("rd.PRM_RATE_CAT_GNDR_CD"),
            F.col("ld.PRM_RATE_CAT_SMKR_CD") == F.col("rd.PRM_RATE_CAT_SMKR_CD"),
            F.col("ld.EFF_DT") == F.col("rd.EFF_DT"),
            F.col("ld.SRC_SYS_CD") == F.col("rd.SRC_SYS_CD")
        ],
        "left"
    )
    .select(
        F.col("ld.PRM_RATE_UNIQ_KEY").alias("PRM_RATE_UNIQ_KEY"),
        F.col("ld.TIER_MOD_ID").alias("TIER_MOD_ID"),
        F.col("ld.PRM_RATE_CAT_GNDR_CD").alias("PRM_RATE_CAT_GNDR_CD"),
        F.col("ld.PRM_RATE_CAT_SMKR_CD").alias("PRM_RATE_CAT_SMKR_CD"),
        F.col("ld.EFF_DT").alias("EFF_DT"),
        F.col("ld.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("rd.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("rd.PRM_RATE_CAT_SK").alias("PRM_RATE_CAT_SK")
    )
)

# xfm_PKEYgen
df_enriched_temp = df_jn_PrmRateCat.withColumn("original_null_flag", F.col("PRM_RATE_CAT_SK").isNull())
df_enriched = SurrogateKeyGen(df_enriched_temp,<DB sequence name>,"PRM_RATE_CAT_SK",<schema>,<secret_name>)

df_xfm_PKEYgen_link_new = (
    df_enriched.filter(F.col("original_null_flag"))
    .select(
        F.col("PRM_RATE_UNIQ_KEY").alias("PRM_RATE_UNIQ_KEY"),
        F.col("TIER_MOD_ID").alias("TIER_MOD_ID"),
        F.col("PRM_RATE_CAT_GNDR_CD").alias("PRM_RATE_CAT_GNDR_CD"),
        F.col("PRM_RATE_CAT_SMKR_CD").alias("PRM_RATE_CAT_SMKR_CD"),
        F.col("EFF_DT").alias("EFF_DT_SK"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.lit(IDSRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("PRM_RATE_CAT_SK").alias("PRM_RATE_CAT_SK")
    )
)

df_xfm_PKEYgen_link_out = (
    df_enriched.select(
        F.col("PRM_RATE_UNIQ_KEY").alias("PRM_RATE_UNIQ_KEY"),
        F.col("TIER_MOD_ID").alias("TIER_MOD_ID"),
        F.col("PRM_RATE_CAT_GNDR_CD").alias("PRM_RATE_CAT_GNDR_CD"),
        F.col("PRM_RATE_CAT_SMKR_CD").alias("PRM_RATE_CAT_SMKR_CD"),
        F.col("EFF_DT").alias("EFF_DT"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.when(F.col("original_null_flag"), F.lit(IDSRunCycle))
         .otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
         .alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(IDSRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("PRM_RATE_CAT_SK").alias("PRM_RATE_CAT_SK")
    )
)

# db2_K_PrmRateCatLoad: Merge into {IDSOwner}.K_PRM_RATE_CAT
df_db2_K_PrmRateCatLoad = df_xfm_PKEYgen_link_new
spark.sql(f"DROP TABLE IF EXISTS STAGING.{'IdsPrmRateCatPkey'}_{'db2_K_PrmRateCatLoad'}_temp")
(
    df_db2_K_PrmRateCatLoad.write.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("dbtable", f"STAGING.{'IdsPrmRateCatPkey'}_{'db2_K_PrmRateCatLoad'}_temp")
    .mode("overwrite")
    .save()
)
merge_sql_db2_K_PrmRateCatLoad = f"""
MERGE INTO {IDSOwner}.K_PRM_RATE_CAT AS target
USING STAGING.IdsPrmRateCatPkey_db2_K_PrmRateCatLoad_temp AS source
ON
 target.PRM_RATE_UNIQ_KEY = source.PRM_RATE_UNIQ_KEY AND
 target.TIER_MOD_ID = source.TIER_MOD_ID AND
 target.PRM_RATE_CAT_GNDR_CD = source.PRM_RATE_CAT_GNDR_CD AND
 target.PRM_RATE_CAT_SMKR_CD = source.PRM_RATE_CAT_SMKR_CD AND
 target.EFF_DT_SK = source.EFF_DT_SK AND
 target.SRC_SYS_CD = source.SRC_SYS_CD
WHEN MATCHED THEN
 UPDATE SET
  target.CRT_RUN_CYC_EXCTN_SK = source.CRT_RUN_CYC_EXCTN_SK,
  target.PRM_RATE_CAT_SK = source.PRM_RATE_CAT_SK
WHEN NOT MATCHED THEN
 INSERT
  (PRM_RATE_UNIQ_KEY,TIER_MOD_ID,PRM_RATE_CAT_GNDR_CD,PRM_RATE_CAT_SMKR_CD,EFF_DT_SK,SRC_SYS_CD,CRT_RUN_CYC_EXCTN_SK,PRM_RATE_CAT_SK)
 VALUES
  (source.PRM_RATE_UNIQ_KEY,source.TIER_MOD_ID,source.PRM_RATE_CAT_GNDR_CD,source.PRM_RATE_CAT_SMKR_CD,source.EFF_DT_SK,source.SRC_SYS_CD,source.CRT_RUN_CYC_EXCTN_SK,source.PRM_RATE_CAT_SK);
"""
execute_dml(merge_sql_db2_K_PrmRateCatLoad, jdbc_url_ids, jdbc_props_ids)

# jn_PKEYs (inner join)
df_jn_PKEYs = (
    df_cpy_MultiStreams_lnkFullDataJnIn.alias("ld")
    .join(
        df_xfm_PKEYgen_link_out.alias("rd"),
        [
            F.col("ld.PRM_RATE_UNIQ_KEY") == F.col("rd.PRM_RATE_UNIQ_KEY"),
            F.col("ld.TIER_MOD_ID") == F.col("rd.TIER_MOD_ID"),
            F.col("ld.PRM_RATE_CAT_GNDR_CD") == F.col("rd.PRM_RATE_CAT_GNDR_CD"),
            F.col("ld.PRM_RATE_CAT_SMKR_CD") == F.col("rd.PRM_RATE_CAT_SMKR_CD"),
            F.col("ld.EFF_DT") == F.col("rd.EFF_DT"),
            F.col("ld.SRC_SYS_CD") == F.col("rd.SRC_SYS_CD")
        ],
        "inner"
    )
    .select(
        F.col("ld.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
        F.col("ld.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
        F.col("rd.PRM_RATE_CAT_SK").alias("PRM_RATE_CAT_SK"),
        F.col("ld.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("ld.PRM_RATE_UNIQ_KEY").alias("PRM_RATE_UNIQ_KEY"),
        F.col("ld.TIER_MOD_ID").alias("TIER_MOD_ID"),
        F.col("ld.PRM_RATE_CAT_GNDR_CD").alias("PRM_RATE_CAT_GNDR_CD"),
        F.col("ld.PRM_RATE_CAT_SMKR_CD").alias("PRM_RATE_CAT_SMKR_CD"),
        F.col("ld.EFF_DT").alias("EFF_DT"),
        F.col("rd.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("rd.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("ld.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("ld.PRM_RATE_SK").alias("PRM_RATE_SK"),
        F.col("ld.TERM_DT").alias("TERM_DT"),
        F.col("ld.AGE_BAND_REF_ID").alias("AGE_BAND_REF_ID")
    )
)

df_seq_PRM_RATE_CAT_PKEY = (
    df_jn_PKEYs
    .withColumn("EFF_DT", F.rpad(F.col("EFF_DT"), 10, " "))
    .withColumn("TERM_DT", F.rpad(F.col("TERM_DT"), 10, " "))
    .select(
        "PRI_NAT_KEY_STRING",
        "FIRST_RECYC_TS",
        "PRM_RATE_CAT_SK",
        "SRC_SYS_CD",
        "PRM_RATE_UNIQ_KEY",
        "TIER_MOD_ID",
        "PRM_RATE_CAT_GNDR_CD",
        "PRM_RATE_CAT_SMKR_CD",
        "EFF_DT",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "SRC_SYS_CD_SK",
        "PRM_RATE_SK",
        "TERM_DT",
        "AGE_BAND_REF_ID"
    )
)

write_files(
    df_seq_PRM_RATE_CAT_PKEY,
    f"{adls_path}/key/PRM_RATE_CAT.{SrcSysCd}.pkey.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)