# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Job Name: FctsCodeCmpr7Extr
# MAGIC 
# MAGIC Read Facets code tables and compare to CDMA code sets.  Capture differences for emailing metadata analyst.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer	Date		Project/Altiris #		Change Description					Development Project		Reviewer		Review Date
# MAGIC =========================================================================================================================================================================================
# MAGIC Saikiran Subbagari	2018-10-11	Production Support		Original Programming				IntegrateDev2		Abhiram Dasarathy	2018-11-06
# MAGIC Saikiran Subbagari	2018-12-19             	Production Support  		Join on SRC_DMN_NM				IntegrateDev2		Abhiram Dasarathy	2018-12-26
# MAGIC Prabhu ES               	2022-02-24              	S2S Remediation     		MSSQL connection parameters added  			IntegrateDev5		Ken Bradmon	2022-05-02

# MAGIC Code Domains
# MAGIC 
# MAGIC ENROLLMENT CHANNEL
# MAGIC ENROLLMENT SOURCE
# MAGIC APPEAL CATEGORY
# MAGIC DENTAL CATEGORY RULE
# MAGIC CLAIM LINE TYPE OF SERVICE
# MAGIC Compare Facets codes to CDMA and log extra codes in Facets.
# MAGIC 
# MAGIC This process runs in development weekdays and emails the output to Metadata Support
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../Utility_Integrate
# COMMAND ----------


RunID = get_widget_value('RunID','')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
TempTable = get_widget_value('TempTable','')

jdbc_url, jdbc_props = get_db_config(facets_secret_name)

schema_CD_MPPNG_Ext = StructType([
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("SRC_DMN_NM", StringType(), False),
    StructField("SRC_LKUP_VAL", StringType(), False),
    StructField("Dummy", IntegerType(), False)
])
df_CD_MPPNG_Ext = (
    spark.read.format("csv")
    .schema(schema_CD_MPPNG_Ext)
    .option("sep", "|")
    .option("header", False)
    .load(f"{adls_path}/load/CDMA_CD_MPPNG.txt")
)

df_lnk_Enroll_Channel_in = df_CD_MPPNG_Ext.select(
    F.col("SRC_LKUP_VAL"),
    F.col("SRC_SYS_CD"),
    F.col("SRC_DMN_NM"),
    F.col("Dummy")
)
df_lnk_Enroll_Source_in = df_CD_MPPNG_Ext.select(
    F.col("SRC_LKUP_VAL"),
    F.col("SRC_SYS_CD"),
    F.col("SRC_DMN_NM"),
    F.col("Dummy")
)
df_lnk_Appeal_cat_in = df_CD_MPPNG_Ext.select(
    F.col("SRC_LKUP_VAL"),
    F.col("SRC_SYS_CD"),
    F.col("SRC_DMN_NM"),
    F.col("Dummy")
)
df_lnk_DentalCatRule_in = df_CD_MPPNG_Ext.select(
    F.col("SRC_LKUP_VAL"),
    F.col("SRC_SYS_CD"),
    F.col("SRC_DMN_NM"),
    F.col("Dummy")
)
df_lnk_ClmLn_TOS_in = df_CD_MPPNG_Ext.select(
    F.col("SRC_LKUP_VAL"),
    F.col("SRC_SYS_CD"),
    F.col("SRC_DMN_NM"),
    F.col("Dummy")
)

df_CMC_ENCH_DESC = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"SELECT DISTINCT ENCH_CHANNEL_ID, ENCH_CHANNEL_DESC FROM {FacetsOwner}.CMC_ENCH_DESC")
    .load()
)
df_xfm_trm_ENCH_CHANNEL_ID_out = df_CMC_ENCH_DESC.filter(
    F.length(trim(F.col("ENCH_CHANNEL_ID"))) > 0
).select(
    F.upper(trim(F.col("ENCH_CHANNEL_ID"))).substr(1, 20).alias("SRC_LKUP_VAL"),
    F.col("ENCH_CHANNEL_DESC").alias("ENCH_CHANNEL_DESC"),
    F.lit("ENROLLMENT CHANNEL").alias("SRC_DMN_NM")
)
df_join_Enroll_Channel = df_xfm_trm_ENCH_CHANNEL_ID_out.alias("a").join(
    df_lnk_Enroll_Channel_in.alias("b"),
    on=[
        F.col("a.SRC_LKUP_VAL") == F.col("b.SRC_LKUP_VAL"),
        F.col("a.SRC_DMN_NM") == F.col("b.SRC_DMN_NM")
    ],
    how="left"
).select(
    F.col("a.SRC_LKUP_VAL").alias("ENCH_CHANNEL_ID"),
    F.col("b.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("a.ENCH_CHANNEL_DESC").alias("ENCH_CHANNEL_DESC"),
    F.col("b.Dummy").alias("Dummy")
)
df_xfm_Enroll_Channel_out = df_join_Enroll_Channel.filter(
    F.col("Dummy") != 1
).select(
    F.concat(F.lit("|"), F.col("ENCH_CHANNEL_ID"), F.lit("|")).alias("Code"),
    F.upper(F.col("ENCH_CHANNEL_DESC")).alias("Description"),
    F.lit("FACETS").alias("Source_System"),
    F.lit("ENROLLMENT CHANNEL").alias("Domain")
)

df_CMC_ENEX_DESC = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"SELECT DISTINCT ENEX_EXCHANGE_ID, ENEX_EXCHANGE_DESC FROM {FacetsOwner}.CMC_ENEX_DESC")
    .load()
)
df_xfm_trim_ENEX_EXCHANGE_ID_out = df_CMC_ENEX_DESC.filter(
    F.length(trim(F.col("ENEX_EXCHANGE_ID"))) > 0
).select(
    F.upper(trim(F.col("ENEX_EXCHANGE_ID"))).substr(1, 20).alias("SRC_LKUP_VAL"),
    F.col("ENEX_EXCHANGE_DESC").alias("PDBC_DESC"),
    F.lit("ENROLLMENT SOURCE").alias("SRC_DMN_NM")
)
df_join_Enroll_Source = df_xfm_trim_ENEX_EXCHANGE_ID_out.alias("a").join(
    df_lnk_Enroll_Source_in.alias("b"),
    on=[
        F.col("a.SRC_LKUP_VAL") == F.col("b.SRC_LKUP_VAL"),
        F.col("a.SRC_DMN_NM") == F.col("b.SRC_DMN_NM")
    ],
    how="left"
).select(
    F.col("a.SRC_LKUP_VAL").alias("ENEX_EXCHANGE_ID"),
    F.col("b.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("a.PDBC_DESC").alias("ENEX_EXCHANGE_DESC"),
    F.col("b.Dummy").alias("Dummy")
)
df_xfm_Enroll_Source_out = df_join_Enroll_Source.filter(
    F.col("Dummy") != 1
).select(
    F.concat(F.lit("|"), F.col("ENEX_EXCHANGE_ID"), F.lit("|")).alias("Code"),
    F.upper(F.col("ENEX_EXCHANGE_DESC")).alias("Description"),
    F.lit("FACETS").alias("Source_System"),
    F.lit("ENROLLMENT SOURCE").alias("Domain")
)

df_CMC_APAP_APPEALS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"SELECT DISTINCT APAP_MCTR_CAT FROM {FacetsOwner}.CMC_APAP_APPEALS WHERE APAP_MCTR_CAT <> ' '")
    .load()
)
df_xfm_trim_APAP_MCTR_CAT_out = df_CMC_APAP_APPEALS.filter(
    F.length(trim(F.col("APAP_MCTR_CAT"))) > 0
).select(
    F.upper(trim(F.col("APAP_MCTR_CAT"))).alias("SRC_LKUP_VAL"),
    F.lit("APPEAL CATEGORY").alias("SRC_DMN_NM")
)
df_join_Appeal_cat = df_xfm_trim_APAP_MCTR_CAT_out.alias("a").join(
    df_lnk_Appeal_cat_in.alias("b"),
    on=[
        F.col("a.SRC_LKUP_VAL") == F.col("b.SRC_LKUP_VAL"),
        F.col("a.SRC_DMN_NM") == F.col("b.SRC_DMN_NM")
    ],
    how="left"
).select(
    F.col("a.SRC_LKUP_VAL").alias("APAP_MCTR_CAT"),
    F.col("b.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("b.Dummy").alias("Dummy")
)
df_xfm_Appeal_cat_out = df_join_Appeal_cat.filter(
    F.col("Dummy") != 1
).select(
    F.concat(F.lit("|"), F.col("APAP_MCTR_CAT"), F.lit("|")).alias("Code"),
    F.upper(F.col("APAP_MCTR_CAT")).alias("Description"),
    F.lit("FACETS").alias("Source_System"),
    F.lit("APPEAL CATEGORY").alias("Domain")
)

df_CMC_CGCG_CAT_RULE = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"SELECT DISTINCT CGCG_RULE, CGCG_DESC FROM {FacetsOwner}.CMC_CGCG_CAT_RULE GROUP BY CGCG_RULE, CGCG_DESC")
    .load()
)
df_xfm_trm_CGCG_RULE_out = df_CMC_CGCG_CAT_RULE.filter(
    F.length(trim(F.col("CGCG_RULE"))) > 0
).select(
    F.upper(trim(F.col("CGCG_RULE"))).alias("SRC_LKUP_VAL"),
    F.col("CGCG_DESC").alias("CGCG_DESC"),
    F.lit("DENTAL CATEGORY RULE").alias("SRC_DMN_NM")
)
df_join_DentalCatRule = df_xfm_trm_CGCG_RULE_out.alias("a").join(
    df_lnk_DentalCatRule_in.alias("b"),
    on=[
        F.col("a.SRC_LKUP_VAL") == F.col("b.SRC_LKUP_VAL"),
        F.col("a.SRC_DMN_NM") == F.col("b.SRC_DMN_NM")
    ],
    how="left"
).select(
    F.col("a.SRC_LKUP_VAL").alias("CGCG_RULE"),
    F.col("b.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("a.CGCG_DESC").alias("CGCG_DESC"),
    F.col("b.Dummy").alias("Dummy")
)
df_xfm_DentalCatRule_out = df_join_DentalCatRule.filter(
    F.col("Dummy") != 1
).select(
    F.concat(F.lit("|"), F.col("CGCG_RULE"), F.lit("|")).alias("Code"),
    F.upper(F.col("CGCG_DESC")).alias("Description"),
    F.lit("FACETS").alias("Source_System"),
    F.lit("DENTAL CATEGORY RULE").alias("Domain")
)

df_CMC_SEDS_SE_DESC = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"SELECT DISTINCT SESE_ID, SEDS_DESC FROM {FacetsOwner}.CMC_SEDS_SE_DESC")
    .load()
)
df_xfm_trim_SESE_ID_out = df_CMC_SEDS_SE_DESC.filter(
    F.length(trim(F.col("SESE_ID"))) > 0
).select(
    F.upper(trim(F.col("SESE_ID"))).alias("SRC_LKUP_VAL"),
    F.col("SEDS_DESC").alias("SESE_DESC"),
    F.lit("CLAIM LINE TYPE OF SERVICE").alias("SRC_DMN_NM")
)
df_join_ClmLn_TOS = df_xfm_trim_SESE_ID_out.alias("a").join(
    df_lnk_ClmLn_TOS_in.alias("b"),
    on=[
        F.col("a.SRC_LKUP_VAL") == F.col("b.SRC_LKUP_VAL"),
        F.col("a.SRC_DMN_NM") == F.col("b.SRC_DMN_NM")
    ],
    how="left"
).select(
    F.col("a.SRC_LKUP_VAL").alias("SESE_ID"),
    F.col("b.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("a.SESE_DESC").alias("SESE_DESC"),
    F.col("b.Dummy").alias("Dummy")
)
df_xfm_ClmLn_TOS_out = df_join_ClmLn_TOS.filter(
    F.col("Dummy") != 1
).select(
    F.concat(F.lit("|"), F.col("SESE_ID"), F.lit("|")).alias("Code"),
    F.upper(F.col("SESE_DESC")).alias("Description"),
    F.lit("FACETS").alias("Source_System"),
    F.lit("CLAIM LINE TYPE OF SERVICE").alias("Domain")
)

df_fl_New_Codes = (
    df_xfm_Enroll_Channel_out
    .unionByName(df_xfm_Enroll_Source_out)
    .unionByName(df_xfm_Appeal_cat_out)
    .unionByName(df_xfm_DentalCatRule_out)
    .unionByName(df_xfm_ClmLn_TOS_out)
)
df_fl_New_Codes_sorted = df_fl_New_Codes.sort(
    F.col("Domain").asc(),
    F.col("Code").asc()
)

df_CDMA_Code_Diff = df_fl_New_Codes_sorted.select(
    F.col("Code"),
    F.rpad(F.col("Description"), 80, " ").alias("Description"),
    F.col("Source_System"),
    F.rpad(F.col("Domain"), 80, " ").alias("Domain")
)
write_files(
    df_CDMA_Code_Diff,
    f"{adls_path}/load/processed/CDMA_Code_Diff.{RunID}.txt",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)