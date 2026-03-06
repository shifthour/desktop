# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Balkarn Gill               10/16/2013        5114                              Create Load File for EDW Table MRKR_CAT_D                             EnterpriseWhseDevl      Bhoomi Dasari              12/18/2013

# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Job Name: IdsEdwMrkrCatDExtr
# MAGIC Read from source table MRKR_CAT .  Apply Run Cycle filters
# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC MAJ_PRCTC_CAT_CD_SK
# MAGIC Write MRKR_CAT_D Data into a Sequential file for Load Job IdsEdwMrkrCatDLoad.
# MAGIC Add Defaults and Null Handling.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, when, trim, rpad, length
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


# Parameters
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
CurrRunCycleDate = get_widget_value('CurrRunCycleDate','')
CurrRunCycle = get_widget_value('CurrRunCycle','')

# Obtain JDBC configuration for IDS
jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# Read from db2_MRKR_CAT_in
query_db2_MRKR_CAT_in = f"""
SELECT 
MRKR_CAT.MRKR_CAT_SK,
COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
MRKR_CAT.MRKR_CAT_CD,
MRKR_CAT.CRT_RUN_CYC_EXCTN_SK,
MRKR_CAT.LAST_UPDT_RUN_CYC_EXCTN_SK,
MRKR_CAT.MAJ_PRCTC_CAT_CD_SK,
MRKR_CAT.MRKR_CAT_DESC
FROM
{IDSOwner}.MRKR_CAT MRKR_CAT
LEFT JOIN {IDSOwner}.CD_MPPNG CD
ON MRKR_CAT.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
WHERE
MRKR_CAT.LAST_UPDT_RUN_CYC_EXCTN_SK >= {IDSRunCycle}
"""

df_db2_MRKR_CAT_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_db2_MRKR_CAT_in)
    .load()
)

# Read from db2_CD_MPPNG_Extr
query_db2_CD_MPPNG_Extr = f"""
SELECT 
CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') TRGT_CD,
COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG
"""

df_db2_CD_MPPNG_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_db2_CD_MPPNG_Extr)
    .load()
)

# lkp_Codes (PxLookup)
df_lkp_Codes = (
    df_db2_MRKR_CAT_in.alias("lnk_IdsEdwMrkrCatDExtr_InAbc")
    .join(
        df_db2_CD_MPPNG_Extr.alias("lkp_MajPrctcCatCd_ref"),
        col("lnk_IdsEdwMrkrCatDExtr_InAbc.MAJ_PRCTC_CAT_CD_SK") == col("lkp_MajPrctcCatCd_ref.CD_MPPNG_SK"),
        "left"
    )
    .select(
        col("lnk_IdsEdwMrkrCatDExtr_InAbc.MRKR_CAT_SK").alias("MRKR_CAT_SK"),
        col("lnk_IdsEdwMrkrCatDExtr_InAbc.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("lnk_IdsEdwMrkrCatDExtr_InAbc.MRKR_CAT_CD").alias("MRKR_CAT_CD"),
        col("lkp_MajPrctcCatCd_ref.TRGT_CD").alias("TRGT_CD"),
        col("lkp_MajPrctcCatCd_ref.TRGT_CD_NM").alias("TRGT_CD_NM"),
        col("lnk_IdsEdwMrkrCatDExtr_InAbc.MRKR_CAT_DESC").alias("MRKR_CAT_DESC")
    )
)

# xfm_BusinessLogic - lnk_Main
df_xfm_BusinessLogic_lnk_Main = (
    df_lkp_Codes
    .filter((col("MRKR_CAT_SK") != 0) & (col("MRKR_CAT_SK") != 1))
    .select(
        col("MRKR_CAT_SK").alias("MRKR_CAT_SK"),
        when(trim(col("SRC_SYS_CD")) == "", lit("NA")).otherwise(col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
        col("MRKR_CAT_CD").alias("MRKR_CAT_CD"),
        lit(CurrRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        lit(CurrRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        when(
            col("TRGT_CD").isNull() | (length(trim(col("TRGT_CD"))) == 0),
            lit("NA")
        ).otherwise(col("TRGT_CD")).alias("MAJ_PRCTC_CAT_CD"),
        when(
            col("TRGT_CD_NM").isNull() | (length(trim(col("TRGT_CD_NM"))) == 0),
            lit("NA")
        ).otherwise(col("TRGT_CD_NM")).alias("MAJ_PRCTC_CAT_DESC"),
        col("MRKR_CAT_DESC").alias("MRKR_CAT_DESC"),
        lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
    )
)

# xfm_BusinessLogic - NA link (single row)
df_xfm_BusinessLogic_NA = (
    df_lkp_Codes
    .limit(1)
    .select(
        lit(1).alias("MRKR_CAT_SK"),
        lit("NA").alias("SRC_SYS_CD"),
        lit("NA").alias("MRKR_CAT_CD"),
        lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        lit("NA").alias("MAJ_PRCTC_CAT_CD"),
        lit("").alias("MAJ_PRCTC_CAT_DESC"),
        lit("").alias("MRKR_CAT_DESC"),
        lit("100").alias("CRT_RUN_CYC_EXCTN_SK"),
        lit("100").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
    )
)

# xfm_BusinessLogic - UNK link (single row)
df_xfm_BusinessLogic_UNK = (
    df_lkp_Codes
    .limit(1)
    .select(
        lit(0).alias("MRKR_CAT_SK"),
        lit("UNK").alias("SRC_SYS_CD"),
        lit("UNK").alias("MRKR_CAT_CD"),
        lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        lit("UNK").alias("MAJ_PRCTC_CAT_CD"),
        lit("").alias("MAJ_PRCTC_CAT_DESC"),
        lit("").alias("MRKR_CAT_DESC"),
        lit("100").alias("CRT_RUN_CYC_EXCTN_SK"),
        lit("100").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
    )
)

# fnl_dataLinks (PxFunnel)
df_fnl_dataLinks = df_xfm_BusinessLogic_NA.unionByName(df_xfm_BusinessLogic_lnk_Main).unionByName(df_xfm_BusinessLogic_UNK)

# seq_MRKR_CAT_D_Load (PxSequentialFile) - final select with rpad for char columns
df_final = df_fnl_dataLinks.select(
    col("MRKR_CAT_SK"),
    col("SRC_SYS_CD"),
    col("MRKR_CAT_CD"),
    rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("MAJ_PRCTC_CAT_CD"),
    col("MAJ_PRCTC_CAT_DESC"),
    col("MRKR_CAT_DESC"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

write_files(
    df_final,
    f"{adls_path}/load/MRKR_CAT_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)