# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Called By: SbcIdsProdQhpExtCntl
# MAGIC 
# MAGIC Process Description: Perform Foreign Key Lookups to populate PROD_QHP table.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Jaideep Mankala      2016-07-22          5605                             Original Programming                                                                             IntegrateDev2           Kalyan Neelam             2016-08-10
# MAGIC Jaideep Mankala      2016-08-16          5605                            Modified Lookup logic on db2_K_Qhp_Lkup stage                                IntegrateDev2           Kalyan Neelam             2016-09-21

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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')
PrefixFkeyFailedFileName = get_widget_value('PrefixFkeyFailedFileName','')
RunDateTime = get_widget_value('RunDateTime','')
DSJobName = "SbcIdsProdQhpFkey"

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

schema_seq_PROD_QHP = StructType([
    StructField("PRI_NAT_KEY_STRING", StringType(), False),
    StructField("FIRST_RECYC_TS", TimestampType(), False),
    StructField("PROD_ID", StringType(), False),
    StructField("PROD_QHP_EFF_DT_SK", StringType(), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("PROD_QHP_SK", IntegerType(), False),
    StructField("TERM_DT", StringType(), False),
    StructField("QHP_ID", StringType(), False),
    StructField("LAST_UPDT_DT", TimestampType(), False),
    StructField("QHP_EFF_DT_SK", StringType(), True)
])

path_seq_PROD_QHP = f"{adls_path}/key/PROD_QHP.{SrcSysCd}.pkey.{RunID}.dat"
df_seq_PROD_QHP = (
    spark.read
    .option("header", False)
    .option("delimiter", ",")
    .option("quote", "^")
    .option("escape", "^")
    .schema(schema_seq_PROD_QHP)
    .csv(path_seq_PROD_QHP)
)

extract_query_db2_K_Prod = f"SELECT DISTINCT PROD_ID, PROD_SK FROM {IDSOwner}.K_PROD"
df_db2_K_Prod_Lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_K_Prod)
    .load()
)

extract_query_db2_K_Qhp = f"SELECT DISTINCT QHP_ID, EFF_DT_SK, QHP_SK FROM {IDSOwner}.QHP WHERE EFF_DT_SK <> TERM_DT_SK"
df_db2_K_Qhp_Lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_K_Qhp)
    .load()
)

df_ds_CD_MPPNG_LkpData = spark.read.parquet(f"{adls_path}/ds/CD_MPPNG.parquet")

df_fltr_FilterData_cond0 = df_ds_CD_MPPNG_LkpData.filter(
    (F.col("SRC_SYS_CD") == F.lit("IDS")) &
    (F.col("SRC_CLCTN_CD") == F.lit("IDS")) &
    (F.col("TRGT_CLCTN_CD") == F.lit("IDS")) &
    (F.col("SRC_DOMAIN_NM") == F.lit("SOURCE SYSTEM")) &
    (F.col("TRGT_DOMAIN_NM") == F.lit("SOURCE SYSTEM")) &
    (F.col("SRC_DRVD_LKUP_VAL") == F.lit("SBC"))
)
df_LnkSBCCdData = df_fltr_FilterData_cond0.select(
    F.col("SRC_CD").alias("SRC_CD"),
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
)

df_fltr_FilterData_cond1 = df_ds_CD_MPPNG_LkpData.filter(
    (F.col("SRC_SYS_CD") == F.lit("FACETS")) &
    (F.col("SRC_CLCTN_CD") == F.lit("FACETS BCBS-EXTENSIO")) &
    (F.col("SRC_DOMAIN_NM") == F.lit("SOURCE SYSTEM")) &
    (F.col("TRGT_CLCTN_CD") == F.lit("IDS")) &
    (F.col("TRGT_DOMAIN_NM") == F.lit("SOURCE SYSTEM")) &
    (F.col("SRC_DRVD_LKUP_VAL") == F.lit("FACETS"))
)
df_msgbrkr_srccd = df_fltr_FilterData_cond1.select(
    F.col("SRC_CD").alias("SRC_CD"),
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
)

df_lkp_Code_SKs = (
    df_seq_PROD_QHP.alias("Lnk_IdsProdQhpFkey_LKp")
    .join(
        df_db2_K_Qhp_Lkp.alias("Ref_K_Qhp_In"),
        (
            (F.col("Lnk_IdsProdQhpFkey_LKp.QHP_ID") == F.col("Ref_K_Qhp_In.QHP_ID")) &
            (F.col("Lnk_IdsProdQhpFkey_LKp.QHP_EFF_DT_SK") == F.col("Ref_K_Qhp_In.EFF_DT_SK"))
        ),
        "left"
    )
    .join(
        df_LnkSBCCdData.alias("LnkSBCCdData"),
        F.col("Lnk_IdsProdQhpFkey_LKp.SRC_SYS_CD") == F.col("LnkSBCCdData.SRC_CD"),
        "left"
    )
    .join(
        df_db2_K_Prod_Lkp.alias("Ref_K_Prod_In"),
        F.col("Lnk_IdsProdQhpFkey_LKp.PROD_ID") == F.col("Ref_K_Prod_In.PROD_ID"),
        "left"
    )
    .join(
        df_msgbrkr_srccd.alias("msgbrkr_srccd"),
        F.col("Lnk_IdsProdQhpFkey_LKp.SRC_SYS_CD") == F.col("msgbrkr_srccd.SRC_CD"),
        "left"
    )
    .select(
        F.col("Lnk_IdsProdQhpFkey_LKp.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
        F.col("Lnk_IdsProdQhpFkey_LKp.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
        F.col("Lnk_IdsProdQhpFkey_LKp.PROD_ID").alias("PROD_ID"),
        F.col("Lnk_IdsProdQhpFkey_LKp.PROD_QHP_EFF_DT_SK").alias("PROD_QHP_EFF_DT_SK"),
        F.col("Lnk_IdsProdQhpFkey_LKp.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Lnk_IdsProdQhpFkey_LKp.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("Lnk_IdsProdQhpFkey_LKp.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("Lnk_IdsProdQhpFkey_LKp.PROD_QHP_SK").alias("PROD_QHP_SK"),
        F.col("Lnk_IdsProdQhpFkey_LKp.TERM_DT").alias("TERM_DT"),
        F.col("Lnk_IdsProdQhpFkey_LKp.QHP_ID").alias("QHP_ID"),
        F.col("Ref_K_Qhp_In.QHP_SK").alias("QHP_SK"),
        F.col("Ref_K_Prod_In.PROD_SK").alias("PROD_SK"),
        F.col("LnkSBCCdData.CD_MPPNG_SK").alias("CD_MPPNG_SK_SBC"),
        F.col("msgbrkr_srccd.CD_MPPNG_SK").alias("CD_MPPNG_SK_FACETS"),
        F.col("Lnk_IdsProdQhpFkey_LKp.LAST_UPDT_DT").alias("LAST_UPDT_DT")
    )
)

df_xfm_CheckLkpResults = (
    df_lkp_Code_SKs
    .withColumn(
        "svKProdLkpFailCheck",
        F.when(
            F.isnull(F.col("PROD_SK")) & (trim(F.col("PROD_ID")) != F.lit("NA")),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "svSrcCodeFailCheck",
        F.when(
            F.isnull(F.col("CD_MPPNG_SK_SBC")) &
            F.isnull(F.col("CD_MPPNG_SK_FACETS")) &
            (F.col("SRC_SYS_CD") != F.lit("NA")),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "svKQhpLkpFailCheck",
        F.when(
            F.isnull(F.col("QHP_SK")) & (F.col("QHP_ID") != F.lit("NA")),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
)

# Main link
df_Lnk_ProdQhpFkey_Main = df_xfm_CheckLkpResults.select(
    F.col("PROD_QHP_SK").alias("PROD_QHP_SK"),
    F.col("PROD_ID").alias("PROD_ID"),
    F.col("PROD_QHP_EFF_DT_SK").alias("PROD_QHP_EFF_DT_SK"),
    F.when(
        F.col("svSrcCodeFailCheck") == F.lit("Y"),
        F.lit(0)
    ).otherwise(
        F.when(F.isnull(F.col("CD_MPPNG_SK_SBC")), F.col("CD_MPPNG_SK_FACETS")).otherwise(F.col("CD_MPPNG_SK_SBC"))
    ).alias("SRC_SYS_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(F.col("svKQhpLkpFailCheck") == F.lit("Y"), F.lit(0)).otherwise(F.col("QHP_SK")).alias("QHP_SK"),
    F.when(F.col("svKProdLkpFailCheck") == F.lit("Y"), F.lit(0)).otherwise(F.col("PROD_SK")).alias("PROD_SK"),
    F.col("TERM_DT").alias("PROD_QHP_TERM_DT_SK"),
    F.col("LAST_UPDT_DT").alias("SRC_SYS_LAST_PRCS_DTM")
)

# UNK link (one row)
df_Lnk_ProdQhpUNK = df_xfm_CheckLkpResults.limit(1).select(
    F.lit(0).alias("PROD_QHP_SK"),
    F.lit("UNK").alias("PROD_ID"),
    F.lit("1753-01-01").alias("PROD_QHP_EFF_DT_SK"),
    F.lit(0).alias("SRC_SYS_CD_SK"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("QHP_SK"),
    F.lit(0).alias("PROD_SK"),
    F.lit("1753-01-01").alias("PROD_QHP_TERM_DT_SK"),
    F.lit(RunDateTime).alias("SRC_SYS_LAST_PRCS_DTM")
)

# NA link (one row)
df_Lnk_ProdQhp_NA = df_xfm_CheckLkpResults.limit(1).select(
    F.lit(1).alias("PROD_QHP_SK"),
    F.lit("NA").alias("PROD_ID"),
    F.lit("1753-01-01").alias("PROD_QHP_EFF_DT_SK"),
    F.lit(1).alias("SRC_SYS_CD_SK"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("QHP_SK"),
    F.lit(1).alias("PROD_SK"),
    F.lit("1753-01-01").alias("PROD_QHP_TERM_DT_SK"),
    F.lit(RunDateTime).alias("SRC_SYS_LAST_PRCS_DTM")
)

# SourceCd_Fail
df_LNk_K_SourceCd_Fail = (
    df_xfm_CheckLkpResults
    .filter(F.col("svSrcCodeFailCheck") == F.lit("Y"))
    .select(
        F.col("PROD_QHP_SK").alias("PRI_SK"),
        F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
        F.when(F.isnull(F.col("CD_MPPNG_SK_SBC")), F.col("CD_MPPNG_SK_FACETS")).otherwise(F.col("CD_MPPNG_SK_SBC")).alias("SRC_SYS_CD_SK"),
        F.lit(DSJobName).alias("JOB_NM"),
        F.lit("FKLOOKUP").alias("ERROR_TYP"),
        F.lit("CD_MPPNG").alias("PHYSCL_FILE_NM"),
        F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("SRC_SYS_CD")).alias("FRGN_NAT_KEY_STRING"),
        F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
    )
)

# Prod_Fail
df_Lnk_K_Prod_Fail = (
    df_xfm_CheckLkpResults
    .filter(F.col("svKProdLkpFailCheck") == F.lit("Y"))
    .select(
        F.col("PROD_QHP_SK").alias("PRI_SK"),
        F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
        F.when(F.isnull(F.col("CD_MPPNG_SK_SBC")), F.col("CD_MPPNG_SK_FACETS")).otherwise(F.col("CD_MPPNG_SK_SBC")).alias("SRC_SYS_CD_SK"),
        F.lit(DSJobName).alias("JOB_NM"),
        F.lit("FKLOOKUP").alias("ERROR_TYP"),
        F.lit("K_PROD").alias("PHYSCL_FILE_NM"),
        F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("PROD_ID")).alias("FRGN_NAT_KEY_STRING"),
        F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
    )
)

# Qhp_Fail
df_Lnk_K_Qhp_Fail = (
    df_xfm_CheckLkpResults
    .filter(F.col("svKQhpLkpFailCheck") == F.lit("Y"))
    .select(
        F.col("PROD_QHP_SK").alias("PRI_SK"),
        F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
        F.when(F.isnull(F.col("CD_MPPNG_SK_SBC")), F.col("CD_MPPNG_SK_FACETS")).otherwise(F.col("CD_MPPNG_SK_SBC")).alias("SRC_SYS_CD_SK"),
        F.lit(DSJobName).alias("JOB_NM"),
        F.lit("FKLOOKUP").alias("ERROR_TYP"),
        F.lit("K_QHP").alias("PHYSCL_FILE_NM"),
        F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("QHP_ID")).alias("FRGN_NAT_KEY_STRING"),
        F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
    )
)

df_fnl_NA_UNK_Streams = (
    df_Lnk_ProdQhpFkey_Main
    .unionByName(df_Lnk_ProdQhpUNK)
    .unionByName(df_Lnk_ProdQhp_NA)
)

df_fnl_NA_UNK_Streams_out = df_fnl_NA_UNK_Streams.select(
    F.col("PROD_QHP_SK"),
    F.rpad(F.col("PROD_ID"), 8, " ").alias("PROD_ID"),
    F.rpad(F.col("PROD_QHP_EFF_DT_SK"), 10, " ").alias("PROD_QHP_EFF_DT_SK"),
    F.col("SRC_SYS_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("QHP_SK"),
    F.col("PROD_SK"),
    F.rpad(F.col("PROD_QHP_TERM_DT_SK"), 10, " ").alias("PROD_QHP_TERM_DT_SK"),
    F.col("SRC_SYS_LAST_PRCS_DTM")
)

path_seq_PROD_QHP_FKEY = f"{adls_path}/load/PROD_QHP.{SrcSysCd}.{RunID}.dat"
write_files(
    df_fnl_NA_UNK_Streams_out,
    path_seq_PROD_QHP_FKEY,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

df_fnlFkeyFailures = (
    df_Lnk_K_Qhp_Fail
    .unionByName(df_LNk_K_SourceCd_Fail)
    .unionByName(df_Lnk_K_Prod_Fail)
)

df_fnlFkeyFailures_out = df_fnlFkeyFailures.select(
    F.col("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD_SK"),
    F.col("JOB_NM"),
    F.col("ERROR_TYP"),
    F.col("PHYSCL_FILE_NM"),
    F.col("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS"),
    F.col("JOB_EXCTN_SK")
)

path_seq_FkeyFailedFile = f"{adls_path}/fkey_failures/{PrefixFkeyFailedFileName}.{DSJobName}.dat"
write_files(
    df_fnlFkeyFailures_out,
    path_seq_FkeyFailedFile,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)