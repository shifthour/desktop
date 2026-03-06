# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:  Extract data from Case_Defn
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date               Project/Altiris #               Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------   ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Ralph Tucker                  4/2/2008           3036                               Originally Programmed                         devlEDW                         Steph Goddard            04/15/2008
# MAGIC 
# MAGIC Syed Husseini                 10/15/2013       5114                               Newly Programmed                             EnterpriseWhrsDevl         Jag Yelavarthi              2013-12-09

# MAGIC Code SK lookups for Denormalization
# MAGIC Write CASE_DEFN_D Data into a Sequential file for Load Job IdsEdwCaseDefnDLoad.
# MAGIC Read all the Data from IDS CASE_DEFN Table;
# MAGIC Add Defaults and Null Handling.
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Job Name: IdsEdwCaseDefnDExtr
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
ExtractRunCycle = get_widget_value('ExtractRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query_db2_CASE_DEFN_in = (
    f"SELECT CASE_DEFN_SK, SRC_SYS_CD_SK, CASE_DEFN_ID, BCBSKC_CLNCL_PGM_TYP_CD_SK, "
    f"CRT_RUN_CYC_EXCTN_SK, LAST_UPDT_RUN_CYC_EXCTN_SK, CASE_DEFN_CAT_CD_SK, SH_DESC "
    f"FROM {IDSOwner}.CASE_DEFN "
    f"WHERE LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtractRunCycle}"
)

df_db2_CASE_DEFN_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CASE_DEFN_in)
    .load()
)

extract_query_db2_CD_MPPNG_in = (
    f"SELECT CD_MPPNG_SK, COALESCE(TRGT_CD,'UNK') TRGT_CD, COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM "
    f"FROM {IDSOwner}.CD_MPPNG"
)

df_db2_CD_MPPNG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CD_MPPNG_in)
    .load()
)

df_cpy_cd_mppng = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_SrcSysCdLkup = df_cpy_cd_mppng
df_ClnclPgmTypCd = df_cpy_cd_mppng
df_CaseDefnCatCd = df_cpy_cd_mppng

df_lkp_Codes = (
    df_db2_CASE_DEFN_in.alias("lnk_IdsEdwCaseDefnExtr_InABC")
    .join(
        df_SrcSysCdLkup.alias("SrcSysCdLkup"),
        F.col("lnk_IdsEdwCaseDefnExtr_InABC.SRC_SYS_CD_SK") == F.col("SrcSysCdLkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_ClnclPgmTypCd.alias("ClnclPgmTypCd"),
        F.col("lnk_IdsEdwCaseDefnExtr_InABC.BCBSKC_CLNCL_PGM_TYP_CD_SK") == F.col("ClnclPgmTypCd.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_CaseDefnCatCd.alias("CaseDefnCatCd"),
        F.col("lnk_IdsEdwCaseDefnExtr_InABC.CASE_DEFN_CAT_CD_SK") == F.col("CaseDefnCatCd.CD_MPPNG_SK"),
        "left"
    )
    .select(
        F.col("lnk_IdsEdwCaseDefnExtr_InABC.CASE_DEFN_SK").alias("CASE_DEFN_SK"),
        F.col("lnk_IdsEdwCaseDefnExtr_InABC.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("lnk_IdsEdwCaseDefnExtr_InABC.CASE_DEFN_ID").alias("CASE_DEFN_ID"),
        F.col("lnk_IdsEdwCaseDefnExtr_InABC.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("lnk_IdsEdwCaseDefnExtr_InABC.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("lnk_IdsEdwCaseDefnExtr_InABC.BCBSKC_CLNCL_PGM_TYP_CD_SK").alias("BCBSKC_CLNCL_PGM_TYP_CD_SK"),
        F.col("lnk_IdsEdwCaseDefnExtr_InABC.CASE_DEFN_CAT_CD_SK").alias("CASE_DEFN_CAT_CD_SK"),
        F.col("lnk_IdsEdwCaseDefnExtr_InABC.SH_DESC").alias("SH_DESC"),
        F.col("SrcSysCdLkup.TRGT_CD").alias("SRC_SYS_CD"),
        F.col("ClnclPgmTypCd.TRGT_CD").alias("BCBSKC_CLNCL_PGM_TYP_CD"),
        F.col("ClnclPgmTypCd.TRGT_CD_NM").alias("BCBSKC_CLNCL_PGM_TYP_NM"),
        F.col("CaseDefnCatCd.TRGT_CD").alias("CASE_DEFN_CAT_CD"),
        F.col("CaseDefnCatCd.TRGT_CD_NM").alias("CASE_DEFN_CAT_NM")
    )
)

windowSpec = Window.orderBy(F.monotonically_increasing_id())
df_with_rn = df_lkp_Codes.withColumn("row_id", F.row_number().over(windowSpec))

df_lnk_Detail_filtered = df_with_rn.filter(
    (F.col("CASE_DEFN_SK") != 0) & (F.col("CASE_DEFN_SK") != 1)
)
df_lnk_Detail = df_lnk_Detail_filtered.select(
    F.col("CASE_DEFN_SK").alias("CASE_DEFN_SK"),
    F.when(F.col("SRC_SYS_CD").isNull(), F.lit("UNK")).otherwise(F.col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
    F.col("CASE_DEFN_ID").alias("CASE_DEFN_ID"),
    F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.when(F.col("BCBSKC_CLNCL_PGM_TYP_CD").isNull(), F.lit("UNK")).otherwise(F.col("BCBSKC_CLNCL_PGM_TYP_CD")).alias("BCBSKC_CLNCL_PGM_TYP_CD"),
    F.when(F.col("BCBSKC_CLNCL_PGM_TYP_NM").isNull(), F.lit("UNK")).otherwise(F.col("BCBSKC_CLNCL_PGM_TYP_NM")).alias("BCBSKC_CLNCL_PGM_TYP_NM"),
    F.when(F.col("CASE_DEFN_CAT_CD").isNull(), F.lit("UNK")).otherwise(F.col("CASE_DEFN_CAT_CD")).alias("CASE_DEFN_CAT_CD"),
    F.when(F.col("CASE_DEFN_CAT_NM").isNull(), F.lit("UNK")).otherwise(F.col("CASE_DEFN_CAT_NM")).alias("CASE_DEFN_CAT_NM"),
    F.col("SH_DESC").alias("CASE_DEFN_SH_DESC"),
    F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("BCBSKC_CLNCL_PGM_TYP_CD_SK").alias("BCBSKC_CLNCL_PGM_TYP_CD_SK"),
    F.col("CASE_DEFN_CAT_CD_SK").alias("CASE_DEFN_CAT_CD_SK")
)

df_lnk_Na_filtered = df_with_rn.filter(F.col("row_id") == 1)
df_lnk_Na = df_lnk_Na_filtered.select(
    F.lit(1).alias("CASE_DEFN_SK"),
    F.lit("NA").alias("SRC_SYS_CD"),
    F.lit("NA").alias("CASE_DEFN_ID"),
    F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("NA").alias("BCBSKC_CLNCL_PGM_TYP_CD"),
    F.lit("NA").alias("BCBSKC_CLNCL_PGM_TYP_NM"),
    F.lit("NA").alias("CASE_DEFN_CAT_CD"),
    F.lit("NA").alias("CASE_DEFN_CAT_NM"),
    F.lit("").alias("CASE_DEFN_SH_DESC"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("BCBSKC_CLNCL_PGM_TYP_CD_SK"),
    F.lit(1).alias("CASE_DEFN_CAT_CD_SK")
)

df_lnk_Unk_filtered = df_with_rn.filter(F.col("row_id") == 1)
df_lnk_Unk = df_lnk_Unk_filtered.select(
    F.lit(0).alias("CASE_DEFN_SK"),
    F.lit("UNK").alias("SRC_SYS_CD"),
    F.lit("UNK").alias("CASE_DEFN_ID"),
    F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("UNK").alias("BCBSKC_CLNCL_PGM_TYP_CD"),
    F.lit("UNK").alias("BCBSKC_CLNCL_PGM_TYP_NM"),
    F.lit("UNK").alias("CASE_DEFN_CAT_CD"),
    F.lit("UNK").alias("CASE_DEFN_CAT_NM"),
    F.lit("").alias("CASE_DEFN_SH_DESC"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("BCBSKC_CLNCL_PGM_TYP_CD_SK"),
    F.lit(0).alias("CASE_DEFN_CAT_CD_SK")
)

df_Fnl_Case_Defn_D = df_lnk_Detail.unionByName(df_lnk_Na).unionByName(df_lnk_Unk)

df_final = df_Fnl_Case_Defn_D.select(
    F.col("CASE_DEFN_SK"),
    F.col("SRC_SYS_CD"),
    F.col("CASE_DEFN_ID"),
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("BCBSKC_CLNCL_PGM_TYP_CD"),
    F.col("BCBSKC_CLNCL_PGM_TYP_NM"),
    F.col("CASE_DEFN_CAT_CD"),
    F.col("CASE_DEFN_CAT_NM"),
    F.col("CASE_DEFN_SH_DESC"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("BCBSKC_CLNCL_PGM_TYP_CD_SK"),
    F.col("CASE_DEFN_CAT_CD_SK")
)

write_files(
    df_final,
    f"{adls_path}/load/CASE_DEFN_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)