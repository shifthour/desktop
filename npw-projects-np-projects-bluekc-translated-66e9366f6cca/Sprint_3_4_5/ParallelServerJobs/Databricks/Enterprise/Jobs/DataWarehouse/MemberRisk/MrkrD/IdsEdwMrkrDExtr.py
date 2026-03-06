# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Balkarn Gill               10/16/2013        5114                              Create Load File for EDW Table MRKR_CAT_D                             EnterpriseWhseDevl       Bhoomi Dasari              12/18/2013

# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Job Name: IdsEdwMrkrCatDExtr
# MAGIC Read from source table MRKR .  Apply Run Cycle filters
# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC MAJ_PRCTC_CAT_CD_SK,
# MAGIC MRKR_TYP_CD_SK
# MAGIC Write MRKR_D Data into a Sequential file for Load Job IdsEdwMrkrDLoad.
# MAGIC Add Defaults and Null Handling.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, lit, when, length, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner', '')
ids_secret_name = get_widget_value('ids_secret_name', '')
IDSRunCycle = get_widget_value('IDSRunCycle', '')
CurrRunCycleDate = get_widget_value('CurrRunCycleDate', '')
CurrRunCycle = get_widget_value('CurrRunCycle', '')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
df_db2_CD_MPPNG_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"SELECT CD_MPPNG_SK, COALESCE(TRGT_CD,'UNK') TRGT_CD, COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM from {IDSOwner}.CD_MPPNG")
    .load()
)

df_Ref_MrkrTypCd_Lkp = df_db2_CD_MPPNG_Extr.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Ref_MajPrctcCatCd_Lkp = df_db2_CD_MPPNG_Extr.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
df_db2_Mrkr_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"SELECT MRKR.MRKR_SK, COALESCE(CD.TRGT_CD,'UNK') AS SRC_SYS_CD, MRKR.MRKR_ID, MRKR.CRT_RUN_CYC_EXCTN_SK, MRKR.MRKR_CAT_SK, MRKR.MRKR_TYP_CD_SK, MRKR.MRKR_DESC, MRKR.MRKR_LABEL FROM {IDSOwner}.MRKR MRKR LEFT JOIN {IDSOwner}.CD_MPPNG CD ON MRKR.SRC_SYS_CD_SK = CD.CD_MPPNG_SK WHERE MRKR.LAST_UPDT_RUN_CYC_EXCTN_SK >= {IDSRunCycle}")
    .load()
)

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
df_db2_MRKR_CAT_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"SELECT MRKR_CAT.MRKR_CAT_SK, MRKR_CAT.MRKR_CAT_CD, MRKR_CAT.MAJ_PRCTC_CAT_CD_SK, MRKR_CAT.MRKR_CAT_DESC FROM {IDSOwner}.MRKR_CAT MRKR_CAT")
    .load()
)

df_lkp_MrkrCat = (
    df_db2_Mrkr_in.alias("lnk_IdsEdwMrkrDExtr_InAbc")
    .join(
        df_db2_MRKR_CAT_in.alias("lkp_IdsEdwMrkrCatSk_ref"),
        col("lnk_IdsEdwMrkrDExtr_InAbc.MRKR_CAT_SK") == col("lkp_IdsEdwMrkrCatSk_ref.MRKR_CAT_SK"),
        "left"
    )
    .select(
        col("lnk_IdsEdwMrkrDExtr_InAbc.MRKR_SK").alias("MRKR_SK"),
        col("lnk_IdsEdwMrkrDExtr_InAbc.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("lnk_IdsEdwMrkrDExtr_InAbc.MRKR_ID").alias("MRKR_ID"),
        col("lnk_IdsEdwMrkrDExtr_InAbc.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("lnk_IdsEdwMrkrDExtr_InAbc.MRKR_CAT_SK").alias("MRKR_CAT_SK"),
        col("lnk_IdsEdwMrkrDExtr_InAbc.MRKR_TYP_CD_SK").alias("MRKR_TYP_CD_SK"),
        col("lnk_IdsEdwMrkrDExtr_InAbc.MRKR_DESC").alias("MRKR_DESC"),
        col("lnk_IdsEdwMrkrDExtr_InAbc.MRKR_LABEL").alias("MRKR_LABEL"),
        col("lkp_IdsEdwMrkrCatSk_ref.MRKR_CAT_CD").alias("MRKR_CAT_CD"),
        col("lkp_IdsEdwMrkrCatSk_ref.MAJ_PRCTC_CAT_CD_SK").alias("MAJ_PRCTC_CAT_CD_SK"),
        col("lkp_IdsEdwMrkrCatSk_ref.MRKR_CAT_DESC").alias("MRKR_CAT_DESC")
    )
)

df_Mrkr_Pca = (
    df_lkp_MrkrCat
    .withColumn(
        "SRC_SYS_CD",
        when(trim(col("SRC_SYS_CD")) == "", lit("NA")).otherwise(col("SRC_SYS_CD"))
    )
    .withColumn(
        "MRKR_CAT_CD",
        when(col("MRKR_CAT_CD").isNull() | (length(col("MRKR_CAT_CD")) == 0), lit("0")).otherwise(col("MRKR_CAT_CD"))
    )
    .withColumn(
        "MAJ_PRCTC_CAT_CD_SK",
        when(col("MAJ_PRCTC_CAT_CD_SK").isNull() | (length(col("MAJ_PRCTC_CAT_CD_SK")) == 0), lit(0)).otherwise(col("MAJ_PRCTC_CAT_CD_SK"))
    )
    .withColumn(
        "MRKR_CAT_DESC",
        when(col("MRKR_CAT_DESC").isNull() | (length(col("MRKR_CAT_DESC")) == 0), lit("0")).otherwise(col("MRKR_CAT_DESC"))
    )
)

df_lkp_Codes = (
    df_Mrkr_Pca.alias("xfm_Pca_Out")
    .join(
        df_Ref_MajPrctcCatCd_Lkp.alias("Ref_MajPrctcCatCd_Lkp"),
        col("xfm_Pca_Out.MAJ_PRCTC_CAT_CD_SK") == col("Ref_MajPrctcCatCd_Lkp.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_Ref_MrkrTypCd_Lkp.alias("Ref_MrkrTypCd_Lkp"),
        col("xfm_Pca_Out.MRKR_TYP_CD_SK") == col("Ref_MrkrTypCd_Lkp.CD_MPPNG_SK"),
        "left"
    )
    .select(
        col("xfm_Pca_Out.MRKR_SK").alias("MRKR_SK"),
        col("xfm_Pca_Out.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("xfm_Pca_Out.MRKR_CAT_CD").alias("MRKR_CAT_CD"),
        col("xfm_Pca_Out.MRKR_CAT_DESC").alias("MRKR_CAT_DESC"),
        col("xfm_Pca_Out.MRKR_DESC").alias("MRKR_DESC"),
        col("xfm_Pca_Out.MRKR_LABEL").alias("MRKR_LABEL"),
        col("Ref_MajPrctcCatCd_Lkp.TRGT_CD").alias("MAJ_PRCTC_CAT_CD"),
        col("Ref_MajPrctcCatCd_Lkp.TRGT_CD_NM").alias("MAJ_PRCTC_CAT_DESC"),
        col("Ref_MrkrTypCd_Lkp.TRGT_CD").alias("MRKR_TYP_CD"),
        col("Ref_MrkrTypCd_Lkp.TRGT_CD_NM").alias("MRKR_TYP_DESC"),
        col("xfm_Pca_Out.MRKR_ID").alias("MRKR_ID")
    )
)

df_xfm_BusinessLogic = (
    df_lkp_Codes
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", lit(CurrRunCycleDate))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", lit(CurrRunCycleDate))
    .withColumn(
        "MAJ_PRCTC_CAT_CD",
        when(col("MAJ_PRCTC_CAT_CD").isNull() | (length(trim(col("MAJ_PRCTC_CAT_CD"))) == 0), lit("NA")).otherwise(col("MAJ_PRCTC_CAT_CD"))
    )
    .withColumn(
        "MAJ_PRCTC_CAT_DESC",
        when(col("MAJ_PRCTC_CAT_DESC").isNull() | (length(trim(col("MAJ_PRCTC_CAT_DESC"))) == 0), lit("NA")).otherwise(col("MAJ_PRCTC_CAT_DESC"))
    )
    .withColumn(
        "MRKR_TYP_CD",
        when(col("MRKR_TYP_CD").isNull() | (length(trim(col("MRKR_TYP_CD"))) == 0), lit("NA")).otherwise(col("MRKR_TYP_CD"))
    )
    .withColumn(
        "MRKR_TYP_DESC",
        when(col("MRKR_TYP_DESC").isNull() | (length(trim(col("MRKR_TYP_DESC"))) == 0), lit("NA")).otherwise(col("MRKR_TYP_DESC"))
    )
    .withColumn("CRT_RUN_CYC_EXCTN_SK", lit(CurrRunCycle))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(CurrRunCycle))
)

df_xfm_BusinessLogic_lnk_Main = df_xfm_BusinessLogic.filter(
    (col("MRKR_SK") != 0) & (col("MRKR_SK") != 1)
)

schema_NA = StructType([
    StructField("MRKR_SK", IntegerType()),
    StructField("SRC_SYS_CD", StringType()),
    StructField("MRKR_ID", StringType()),
    StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType()),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType()),
    StructField("MRKR_CAT_CD", StringType()),
    StructField("MRKR_CAT_DESC", StringType()),
    StructField("MRKR_DESC", StringType()),
    StructField("MRKR_LABEL", StringType()),
    StructField("MAJ_PRCTC_CAT_CD", StringType()),
    StructField("MAJ_PRCTC_CAT_DESC", StringType()),
    StructField("MRKR_TYP_CD", StringType()),
    StructField("MRKR_TYP_DESC", StringType()),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType()),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType())
])

df_xfm_BusinessLogic_NA = spark.createDataFrame(
    [
        (
            1, 'NA', 'NA', '1753-01-01', '1753-01-01',
            'NA', '', '', '', 'NA', '', 'NA', '', 100, 100
        )
    ],
    schema_NA
)

df_xfm_BusinessLogic_UNK = spark.createDataFrame(
    [
        (
            0, 'UNK', 'UNK', '1753-01-01', '1753-01-01',
            'UNK', '', '', '', 'UNK', '', 'UNK', '', 100, 100
        )
    ],
    schema_NA
)

df_fnl_dataLinks = (
    df_xfm_BusinessLogic_NA
    .unionByName(df_xfm_BusinessLogic_lnk_Main)
    .unionByName(df_xfm_BusinessLogic_UNK)
)

final_df = df_fnl_dataLinks.select(
    col("MRKR_SK"),
    col("SRC_SYS_CD"),
    col("MRKR_ID"),
    rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("MRKR_CAT_CD"),
    col("MRKR_CAT_DESC"),
    col("MRKR_DESC"),
    col("MRKR_LABEL"),
    col("MAJ_PRCTC_CAT_CD"),
    col("MAJ_PRCTC_CAT_DESC"),
    col("MRKR_TYP_CD"),
    col("MRKR_TYP_DESC"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

write_files(
    final_df,
    f"{adls_path}/load/MRKR_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)