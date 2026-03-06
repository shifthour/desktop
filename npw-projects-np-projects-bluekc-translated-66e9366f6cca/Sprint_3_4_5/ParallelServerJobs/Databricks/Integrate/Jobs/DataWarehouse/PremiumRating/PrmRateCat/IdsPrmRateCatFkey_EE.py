# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC 
# MAGIC Jag Yelavarthi      2014-04-18               5345                             Original Programming                                                                             IntegrateWrhsDevl      Sharon Andrew       2014-10-21

# MAGIC FKEY failures are written into this flat file.
# MAGIC Code lookups operation to get the needed FKEY values
# MAGIC This is a load ready file that will go into Load job
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType
)
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')
PrefixFkeyFailedFileName = get_widget_value('PrefixFkeyFailedFileName','')
ids_secret_name = get_widget_value('ids_secret_name','')

DSJobName = "IdsPrmRateCatFkey_EE"

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

schema_seq_PRM_RATE_CAT_PKEY = StructType([
    StructField("PRI_NAT_KEY_STRING", StringType(), False),
    StructField("FIRST_RECYC_TS", TimestampType(), False),
    StructField("PRM_RATE_CAT_SK", IntegerType(), True),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRM_RATE_UNIQ_KEY", IntegerType(), False),
    StructField("TIER_MOD_ID", StringType(), False),
    StructField("PRM_RATE_CAT_GNDR_CD", StringType(), False),
    StructField("PRM_RATE_CAT_SMKR_CD", StringType(), False),
    StructField("EFF_DT", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("SRC_SYS_CD_SK", IntegerType(), False),
    StructField("PRM_RATE_SK", IntegerType(), True),
    StructField("TERM_DT", StringType(), False),
    StructField("AGE_BAND_REF_ID", TimestampType(), False)
])

df_seq_PRM_RATE_CAT_PKEY = (
    spark.read
    .option("delimiter", ",")
    .option("quote", "^")
    .option("header", "false")
    .option("nullValue", None)
    .schema(schema_seq_PRM_RATE_CAT_PKEY)
    .csv(f"{adls_path}/key/PRM_RATE_CAT.{SrcSysCd}.pkey.{RunID}.dat")
)

extract_query_db2_K_Prm_rate_Lkp = (
    f"SELECT SRC_SYS_CD, PRM_RATE_UNIQ_KEY, PRM_RATE_SK "
    f"FROM {IDSOwner}.K_PRM_RATE"
)
df_db2_K_Prm_rate_Lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_K_Prm_rate_Lkp)
    .load()
)

extract_query_db2_Clndr_Dt_Term_Dt_Lkp = (
    f"SELECT DISTINCT CLNDR_DT_SK, CLNDR_DT "
    f"FROM {IDSOwner}.CLNDR_DT "
    f"WHERE CLNDR_DT_SK NOT IN ('INVALID','NA','NULL','UNK')"
)
df_db2_Clndr_Dt_Term_Dt_Lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_Clndr_Dt_Term_Dt_Lkp)
    .load()
)

extract_query_db2_Clndr_Dt_Eff_Dt_Lkp = (
    f"SELECT DISTINCT CLNDR_DT_SK, CLNDR_DT "
    f"FROM {IDSOwner}.CLNDR_DT "
    f"WHERE CLNDR_DT_SK NOT IN ('INVALID','NA','NULL','UNK')"
)
df_db2_Clndr_Dt_Eff_Dt_Lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_Clndr_Dt_Eff_Dt_Lkp)
    .load()
)

df_ds_CD_MPPNG_LkpData = spark.read.parquet(f"{adls_path}/ds/CD_MPPNG.parquet")

df_lnkGenderLkpData = (
    df_ds_CD_MPPNG_LkpData
    .filter(
        (F.col("SRC_SYS_CD") == "FACETS") &
        (F.col("SRC_CLCTN_CD") == "FACETS DBO") &
        (F.col("TRGT_CLCTN_CD") == "IDS") &
        (F.col("SRC_DOMAIN_NM") == "GENDER") &
        (F.col("TRGT_DOMAIN_NM") == "GENDER")
    )
    .select(
        F.col("SRC_CD").alias("SRC_CD"),
        F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
        F.col("TRGT_CD").alias("TRGT_CD")
    )
)

df_lnkSmokerLkpData = (
    df_ds_CD_MPPNG_LkpData
    .filter(
        (F.col("SRC_SYS_CD") == "FACETS") &
        (F.col("SRC_CLCTN_CD") == "FACETS DBO") &
        (F.col("TRGT_CLCTN_CD") == "IDS") &
        (F.col("SRC_DOMAIN_NM") == "PREMIUM RATE CATEGORY SMOKER") &
        (F.col("TRGT_DOMAIN_NM") == "PREMIUM RATE CATEGORY SMOKER")
    )
    .select(
        F.col("SRC_CD").alias("SRC_CD"),
        F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
        F.col("TRGT_CD").alias("TRGT_CD")
    )
)

df_lkp_Code_SKs = (
    df_seq_PRM_RATE_CAT_PKEY.alias("p")
    .join(
        df_db2_K_Prm_rate_Lkp.alias("Ref_PrmRate"),
        (
            (F.col("p.SRC_SYS_CD") == F.col("Ref_PrmRate.SRC_SYS_CD"))
            & (F.col("p.PRM_RATE_UNIQ_KEY") == F.col("Ref_PrmRate.PRM_RATE_UNIQ_KEY"))
        ),
        "left"
    )
    .join(
        df_db2_Clndr_Dt_Term_Dt_Lkp.alias("Ref_Term_Dt"),
        F.col("p.TERM_DT") == F.col("Ref_Term_Dt.CLNDR_DT_SK"),
        "left"
    )
    .join(
        df_lnkSmokerLkpData.alias("lnkSmokerLkpData"),
        F.col("p.PRM_RATE_CAT_SMKR_CD") == F.col("lnkSmokerLkpData.SRC_CD"),
        "left"
    )
    .join(
        df_lnkGenderLkpData.alias("lnkGenderLkpData"),
        F.col("p.PRM_RATE_CAT_GNDR_CD") == F.col("lnkGenderLkpData.SRC_CD"),
        "left"
    )
    .join(
        df_db2_Clndr_Dt_Eff_Dt_Lkp.alias("Ref_Eff_Dt"),
        F.col("p.EFF_DT") == F.col("Ref_Eff_Dt.CLNDR_DT_SK"),
        "left"
    )
    .select(
        F.col("p.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
        F.col("p.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
        F.col("p.PRM_RATE_CAT_SK").alias("PRM_RATE_CAT_SK"),
        F.col("p.PRM_RATE_UNIQ_KEY").alias("PRM_RATE_UNIQ_KEY"),
        F.col("p.TIER_MOD_ID").alias("TIER_MOD_ID"),
        F.col("lnkGenderLkpData.TRGT_CD").alias("PRM_RATE_CAT_GNDR_CD"),
        F.col("lnkSmokerLkpData.TRGT_CD").alias("PRM_RATE_CAT_SMKR_CD"),
        F.col("Ref_Eff_Dt.CLNDR_DT_SK").alias("EFF_DT_SK"),
        F.col("p.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("p.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("p.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("Ref_PrmRate.PRM_RATE_SK").alias("PRM_RATE_SK"),
        F.col("Ref_Term_Dt.CLNDR_DT_SK").alias("TERM_DT_SK"),
        F.col("p.TERM_DT").alias("TERM_DT_IN"),
        F.col("p.AGE_BAND_REF_ID").alias("AGE_BAND_REF_ID"),
        F.col("lnkGenderLkpData.CD_MPPNG_SK").alias("PRM_RATE_CAT_GNDR_SK"),
        F.col("lnkSmokerLkpData.CD_MPPNG_SK").alias("PRM_RATE_CAT_SMKR_SK"),
        F.col("p.EFF_DT").alias("EFF_DT_IN")
    )
)

df_xfm_CheckLkpResults_temp = (
    df_lkp_Code_SKs
    .withColumn("svPrmRateLkpCheck", F.when(F.isnull(F.col("PRM_RATE_SK")), F.lit("Y")).otherwise(F.lit("N")))
    .withColumn("svlnkGenderLkpCheck", F.when(F.isnull(F.col("PRM_RATE_CAT_GNDR_SK")), F.lit("Y")).otherwise(F.lit("N")))
    .withColumn("svEffDtLkpCheck", F.when(F.isnull(F.col("EFF_DT_SK")), F.lit("Y")).otherwise(F.lit("N")))
    .withColumn("svTermDtLkpCheck", F.when(F.isnull(F.col("TERM_DT_SK")), F.lit("Y")).otherwise(F.lit("N")))
    .withColumn("svSmokerCdLkpCheck", F.when(F.isnull(F.col("PRM_RATE_CAT_SMKR_SK")), F.lit("Y")).otherwise(F.lit("N")))
)

df_Lnk_PrmRateCat_Main = df_xfm_CheckLkpResults_temp.select(
    F.col("PRM_RATE_CAT_SK").alias("PRM_RATE_CAT_SK"),
    F.col("PRM_RATE_UNIQ_KEY").alias("PRM_RATE_UNIQ_KEY"),
    F.col("TIER_MOD_ID").alias("TIER_MOD_ID"),
    F.when(F.col("PRM_RATE_CAT_GNDR_CD").isNull(), F.lit("UNK")).otherwise(F.col("PRM_RATE_CAT_GNDR_CD")).alias("PRM_RATE_CAT_GNDR_CD"),
    F.when(F.col("PRM_RATE_CAT_SMKR_CD").isNull(), F.lit("UNK")).otherwise(F.col("PRM_RATE_CAT_SMKR_CD")).alias("PRM_RATE_CAT_SMKR_CD"),
    F.when(F.col("EFF_DT_SK").isNull(), F.lit("1753-01-01")).otherwise(F.col("EFF_DT_SK")).alias("EFF_DT_SK"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(F.col("PRM_RATE_SK").isNull(), F.lit(0)).otherwise(F.col("PRM_RATE_SK")).alias("PRM_RATE_SK"),
    F.when(F.col("TERM_DT_SK").isNull(), F.lit("2199-12-31")).otherwise(F.col("TERM_DT_SK")).alias("TERM_DT_SK"),
    F.col("AGE_BAND_REF_ID").alias("AGE_BAND_REF_ID"),
    F.when(F.trim(F.col("PRM_RATE_CAT_GNDR_CD")) == F.lit("UNK"), F.lit(0)).otherwise(
        F.when(F.trim(F.col("PRM_RATE_CAT_GNDR_CD")) == F.lit("NA"), F.lit(1)).otherwise(
            F.when(F.col("PRM_RATE_CAT_GNDR_SK").isNull(), F.lit(1)).otherwise(F.col("PRM_RATE_CAT_GNDR_SK"))
        )
    ).alias("PRM_RATE_CAT_GNDR_CD_SK"),
    F.when(F.trim(F.col("PRM_RATE_CAT_SMKR_CD")) == F.lit("UNK"), F.lit(0)).otherwise(
        F.when(F.trim(F.col("PRM_RATE_CAT_SMKR_CD")) == F.lit("NA"), F.lit(1)).otherwise(
            F.when(F.col("PRM_RATE_CAT_SMKR_SK").isNull(), F.lit(1)).otherwise(F.col("PRM_RATE_CAT_SMKR_SK"))
        )
    ).alias("PRM_RATE_CAT_SMKR_CD_SK")
)

df_Lnk_PrmRateCatUNK = (
    df_xfm_CheckLkpResults_temp
    .withColumn("rownumU", F.row_number().over(Window.orderBy(F.lit(1))))
    .filter(F.col("rownumU") == 1)
    .select(
        F.lit(0).alias("PRM_RATE_CAT_SK"),
        F.lit(0).alias("PRM_RATE_UNIQ_KEY"),
        F.lit("UNK").alias("TIER_MOD_ID"),
        F.lit("UNK").alias("PRM_RATE_CAT_GNDR_CD"),
        F.lit("UNK").alias("PRM_RATE_CAT_SMKR_CD"),
        F.lit("1753-01-01").alias("EFF_DT_SK"),
        F.lit(0).alias("SRC_SYS_CD_SK"),
        F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("PRM_RATE_SK"),
        F.lit("1753-01-01").alias("TERM_DT_SK"),
        F.lit("1753-01-01 00:00:00").alias("AGE_BAND_REF_ID"),
        F.lit(0).alias("PRM_RATE_CAT_GNDR_CD_SK"),
        F.lit(0).alias("PRM_RATE_CAT_SMKR_CD_SK")
    )
)

df_Lnk_PrmRateCatNA = (
    df_xfm_CheckLkpResults_temp
    .withColumn("rownumN", F.row_number().over(Window.orderBy(F.lit(1))))
    .filter(F.col("rownumN") == 1)
    .select(
        F.lit(1).alias("PRM_RATE_CAT_SK"),
        F.lit(1).alias("PRM_RATE_UNIQ_KEY"),
        F.lit("NA").alias("TIER_MOD_ID"),
        F.lit("NA").alias("PRM_RATE_CAT_GNDR_CD"),
        F.lit("NA").alias("PRM_RATE_CAT_SMKR_CD"),
        F.lit("1753-01-01").alias("EFF_DT_SK"),
        F.lit(1).alias("SRC_SYS_CD_SK"),
        F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("PRM_RATE_SK"),
        F.lit("1753-01-01").alias("TERM_DT_SK"),
        F.lit("1753-01-01 00:00:00").alias("AGE_BAND_REF_ID"),
        F.lit(1).alias("PRM_RATE_CAT_GNDR_CD_SK"),
        F.lit(1).alias("PRM_RATE_CAT_SMKR_CD_SK")
    )
)

df_LnkTermDtFail = (
    df_xfm_CheckLkpResults_temp
    .filter(F.col("svTermDtLkpCheck") == "Y")
    .select(
        F.col("PRM_RATE_CAT_SK").alias("PRI_SK"),
        F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
        F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.lit(DSJobName).alias("JOB_NM"),
        F.lit("FKLOOKUP").alias("ERROR_TYP"),
        F.lit("CLNDR_DT").alias("PHYSCL_FILE_NM"),
        F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("TERM_DT_IN")).alias("FRGN_NAT_KEY_STRING"),
        F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
    )
)

df_LnkPrmRateFail = (
    df_xfm_CheckLkpResults_temp
    .filter(F.col("svPrmRateLkpCheck") == "Y")
    .select(
        F.col("PRM_RATE_CAT_SK").alias("PRI_SK"),
        F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
        F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.lit(DSJobName).alias("JOB_NM"),
        F.lit("FKLOOKUP").alias("ERROR_TYP"),
        F.lit("PRMRATE").alias("PHYSCL_FILE_NM"),
        F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("PRM_RATE_UNIQ_KEY").cast(StringType())).alias("FRGN_NAT_KEY_STRING"),
        F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
    )
)

df_lnkSmokerLkpFail = (
    df_xfm_CheckLkpResults_temp
    .filter(F.col("svSmokerCdLkpCheck") == "Y")
    .select(
        F.col("PRM_RATE_CAT_SK").alias("PRI_SK"),
        F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
        F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.lit(DSJobName).alias("JOB_NM"),
        F.lit("CDLOOKUP").alias("ERROR_TYP"),
        F.lit("CD_MPPNG").alias("PHYSCL_FILE_NM"),
        F.concat(
            F.lit(SrcSysCd),
            F.lit(";"),
            F.lit("FACETS DB"),
            F.lit(";"),
            F.lit("IDS"),
            F.lit(";"),
            F.lit("PREMIUM RATE CATEGORY SMOKER"),
            F.lit(";"),
            F.lit("PREMIUM RATE CATEGORY SMOKER"),
            F.lit(";"),
            F.col("PRM_RATE_CAT_SMKR_CD")
        ).alias("FRGN_NAT_KEY_STRING"),
        F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
    )
)

df_lnkGenderLkpFail = (
    df_xfm_CheckLkpResults_temp
    .filter(F.col("svlnkGenderLkpCheck") == "Y")
    .select(
        F.col("PRM_RATE_CAT_SK").alias("PRI_SK"),
        F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
        F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.lit(DSJobName).alias("JOB_NM"),
        F.lit("CDLOOKUP").alias("ERROR_TYP"),
        F.lit("CD_MPPNG").alias("PHYSCL_FILE_NM"),
        F.concat(
            F.lit(SrcSysCd),
            F.lit(";"),
            F.lit("FACETS DB"),
            F.lit(";"),
            F.lit("IDS"),
            F.lit(";"),
            F.lit("GENDER"),
            F.lit(";"),
            F.lit("GENDER"),
            F.lit(";"),
            F.col("PRM_RATE_CAT_GNDR_CD")
        ).alias("FRGN_NAT_KEY_STRING"),
        F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
    )
)

df_lnkEffDtLkpFail = (
    df_xfm_CheckLkpResults_temp
    .filter(F.col("svEffDtLkpCheck") == "Y")
    .select(
        F.col("PRM_RATE_CAT_SK").alias("PRI_SK"),
        F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
        F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.lit(DSJobName).alias("JOB_NM"),
        F.lit("FKLOOKUP").alias("ERROR_TYP"),
        F.lit("CLNDR_DT").alias("PHYSCL_FILE_NM"),
        F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("EFF_DT_IN")).alias("FRGN_NAT_KEY_STRING"),
        F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
    )
)

df_fnl_NA_UNK_Streams = (
    df_Lnk_PrmRateCat_Main
    .unionByName(df_Lnk_PrmRateCatUNK.select(df_Lnk_PrmRateCat_Main.columns))
    .unionByName(df_Lnk_PrmRateCatNA.select(df_Lnk_PrmRateCat_Main.columns))
)

df_fnl_NA_UNK_Streams_rpad = (
    df_fnl_NA_UNK_Streams
    .withColumn("EFF_DT_SK", F.rpad(F.col("EFF_DT_SK"), 10, " "))
    .withColumn("TERM_DT_SK", F.rpad(F.col("TERM_DT_SK"), 10, " "))
)

df_Fnl_LkpFail = (
    df_LnkPrmRateFail
    .unionByName(df_LnkTermDtFail.select(df_LnkPrmRateFail.columns))
    .unionByName(df_lnkSmokerLkpFail.select(df_LnkPrmRateFail.columns))
    .unionByName(df_lnkGenderLkpFail.select(df_LnkPrmRateFail.columns))
    .unionByName(df_lnkEffDtLkpFail.select(df_LnkPrmRateFail.columns))
)

write_files(
    df_fnl_NA_UNK_Streams_rpad.select(
        "PRM_RATE_CAT_SK",
        "PRM_RATE_UNIQ_KEY",
        "TIER_MOD_ID",
        "PRM_RATE_CAT_GNDR_CD",
        "PRM_RATE_CAT_SMKR_CD",
        "EFF_DT_SK",
        "SRC_SYS_CD_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "PRM_RATE_SK",
        "TERM_DT_SK",
        "AGE_BAND_REF_ID",
        "PRM_RATE_CAT_GNDR_CD_SK",
        "PRM_RATE_CAT_SMKR_CD_SK"
    ),
    f"{adls_path}/load/PRM_RATE_CAT.{SrcSysCd}.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

write_files(
    df_Fnl_LkpFail.select(
        "PRI_SK",
        "PRI_NAT_KEY_STRING",
        "SRC_SYS_CD_SK",
        "JOB_NM",
        "ERROR_TYP",
        "PHYSCL_FILE_NM",
        "FRGN_NAT_KEY_STRING",
        "FIRST_RECYC_TS",
        "JOB_EXCTN_SK"
    ),
    f"{adls_path}/fkey_failures/{PrefixFkeyFailedFileName}.{DSJobName}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)