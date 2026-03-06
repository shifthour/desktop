# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date               Project/Altiris #               Change Description                                                                     Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------                                              ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Ralph Tucker                  4/2/2008       3036                               Originally Programmed                                                                    devlEDW                         Steph Goddard             04/15/2008
# MAGIC 
# MAGIC Bhupinder Kaur                10/14/2013    EDWefficiencies(5114)     rewrite in parallel ( creating load file to load the edw table)          EnterpriseWrhsDevl      Peter Marshall              12/11/2013

# MAGIC This file will be used to load into CLNCL_IN_D table.
# MAGIC JobName: IdsEdwClnclInDExtr
# MAGIC Data extracted from IDS table CLINICAL_IN
# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC CD_SK
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
ExtractRunCycle = get_widget_value('ExtractRunCycle','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

select_query_IdsClnclIn_in = f"""SELECT
CLNIN.CLNCL_IN_SK,
COALESCE(CM.TRGT_CD,'UNK') as SRC_SYS_CD,
CLNIN.CLNCL_IN_ID,
CLNIN.CRT_RUN_CYC_EXCTN_SK,
CLNIN.LAST_UPDT_RUN_CYC_EXCTN_SK,
CLNIN.BCBSKC_CLNCL_PGM_TYP_CD_SK,
CLNIN.CLNCL_IN_GNRL_CAT_CD_SK,
CLNIN.CLNCL_IN_PRI_DSS_CD_SK,
CLNIN.SH_DESC
FROM {IDSOwner}.CLNCL_IN CLNIN
LEFT OUTER JOIN {IDSOwner}.CD_MPPNG CM
ON CLNIN.SRC_SYS_CD_SK=CM.CD_MPPNG_SK
WHERE CLNIN.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtractRunCycle}
"""

df_IdsClnclIn_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", select_query_IdsClnclIn_in)
    .load()
)

select_query_db2_CD_MPPNG_in = f"""SELECT
CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') as TRGT_CD,
COALESCE(TRGT_CD_NM,'UNK') as TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG
"""

df_db2_CD_MPPNG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", select_query_db2_CD_MPPNG_in)
    .load()
)

df_ClnclnGnrlCatCd = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_ClnclPgmTypCd = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_ClnclInPriDssCd = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_lkp_CdMppng = (
    df_IdsClnclIn_in.alias("lnk_IdsEdwClnclInDExtr_in_ABC")
    .join(
        df_ClnclnGnrlCatCd.alias("ClnclnGnrlCatCd"),
        F.col("lnk_IdsEdwClnclInDExtr_in_ABC.CLNCL_IN_GNRL_CAT_CD_SK") == F.col("ClnclnGnrlCatCd.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_ClnclPgmTypCd.alias("ClnclPgmTypCd"),
        F.col("lnk_IdsEdwClnclInDExtr_in_ABC.BCBSKC_CLNCL_PGM_TYP_CD_SK") == F.col("ClnclPgmTypCd.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_ClnclInPriDssCd.alias("ClnclInPriDssCd"),
        F.col("lnk_IdsEdwClnclInDExtr_in_ABC.CLNCL_IN_PRI_DSS_CD_SK") == F.col("ClnclInPriDssCd.CD_MPPNG_SK"),
        "left"
    )
    .select(
        F.col("lnk_IdsEdwClnclInDExtr_in_ABC.CLNCL_IN_SK").alias("CLNCL_IN_SK"),
        F.col("lnk_IdsEdwClnclInDExtr_in_ABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnk_IdsEdwClnclInDExtr_in_ABC.CLNCL_IN_ID").alias("CLNCL_IN_ID"),
        F.col("ClnclPgmTypCd.TRGT_CD").alias("BCBSKC_CLNCL_PGM_TYP_CD"),
        F.col("ClnclPgmTypCd.TRGT_CD_NM").alias("BCBSKC_CLNCL_PGM_TYP_NM"),
        F.col("ClnclnGnrlCatCd.TRGT_CD").alias("CLNCL_IN_GNRL_CAT_CD"),
        F.col("ClnclnGnrlCatCd.TRGT_CD_NM").alias("CLNCL_IN_GNRL_CAT_NM"),
        F.col("ClnclInPriDssCd.TRGT_CD").alias("CLNCL_IN_PRI_DSS_CD"),
        F.col("ClnclInPriDssCd.TRGT_CD_NM").alias("CLNCL_IN_PRI_DSS_NM"),
        F.col("lnk_IdsEdwClnclInDExtr_in_ABC.SH_DESC").alias("SH_DESC"),
        F.col("lnk_IdsEdwClnclInDExtr_in_ABC.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("lnk_IdsEdwClnclInDExtr_in_ABC.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("lnk_IdsEdwClnclInDExtr_in_ABC.BCBSKC_CLNCL_PGM_TYP_CD_SK").alias("BCBSKC_CLNCL_PGM_TYP_CD_SK"),
        F.col("lnk_IdsEdwClnclInDExtr_in_ABC.CLNCL_IN_GNRL_CAT_CD_SK").alias("CLNCL_IN_GNRL_CAT_CD_SK"),
        F.col("lnk_IdsEdwClnclInDExtr_in_ABC.CLNCL_IN_PRI_DSS_CD_SK").alias("CLNCL_IN_PRI_DSS_CD_SK")
    )
)

df_lnk_Full_Data_Out = df_lkp_CdMppng.filter(
    (F.col("CLNCL_IN_SK") != 0) & (F.col("CLNCL_IN_SK") != 1)
).select(
    F.col("CLNCL_IN_SK").alias("CLNCL_IN_SK"),
    F.when(F.col("SRC_SYS_CD").isNull() | (F.col("SRC_SYS_CD") == ""), F.lit("UNK")).otherwise(F.col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
    F.col("CLNCL_IN_ID").alias("CLNCL_IN_ID"),
    F.rpad(F.lit(EDWRunCycleDate), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.lit(EDWRunCycleDate), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.when(F.col("BCBSKC_CLNCL_PGM_TYP_CD").isNull() | (F.col("BCBSKC_CLNCL_PGM_TYP_CD") == ""), F.lit("UNK")).otherwise(F.col("BCBSKC_CLNCL_PGM_TYP_CD")).alias("BCBSKC_CLNCL_PGM_TYP_CD"),
    F.when(F.col("BCBSKC_CLNCL_PGM_TYP_NM").isNull() | (F.col("BCBSKC_CLNCL_PGM_TYP_NM") == ""), F.lit("UNK")).otherwise(F.col("BCBSKC_CLNCL_PGM_TYP_NM")).alias("BCBSKC_CLNCL_PGM_TYP_NM"),
    F.when(F.col("CLNCL_IN_GNRL_CAT_CD").isNull() | (F.col("CLNCL_IN_GNRL_CAT_CD") == ""), F.lit("UNK")).otherwise(F.col("CLNCL_IN_GNRL_CAT_CD")).alias("CLNCL_IN_GNRL_CAT_CD"),
    F.when(F.col("CLNCL_IN_GNRL_CAT_NM").isNull() | (F.col("CLNCL_IN_GNRL_CAT_NM") == ""), F.lit("UNK")).otherwise(F.col("CLNCL_IN_GNRL_CAT_NM")).alias("CLNCL_IN_GNRL_CAT_NM"),
    F.when(F.col("CLNCL_IN_PRI_DSS_CD").isNull() | (F.col("CLNCL_IN_PRI_DSS_CD") == ""), F.lit("UNK")).otherwise(F.col("CLNCL_IN_PRI_DSS_CD")).alias("CLNCL_IN_PRI_DSS_CD"),
    F.when(F.col("CLNCL_IN_PRI_DSS_NM").isNull() | (F.col("CLNCL_IN_PRI_DSS_NM") == ""), F.lit("UNK")).otherwise(F.col("CLNCL_IN_PRI_DSS_NM")).alias("CLNCL_IN_PRI_DSS_NM"),
    F.col("SH_DESC").alias("SH_DESC"),
    F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("BCBSKC_CLNCL_PGM_TYP_CD_SK").alias("BCBSKC_CLNCL_PGM_TYP_CD_SK"),
    F.col("CLNCL_IN_GNRL_CAT_CD_SK").alias("CLNCL_IN_GNRL_CAT_CD_SK"),
    F.col("CLNCL_IN_PRI_DSS_CD_SK").alias("CLNCL_IN_PRI_DSS_CD_SK")
)

col_order = [
    "CLNCL_IN_SK",
    "SRC_SYS_CD",
    "CLNCL_IN_ID",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "BCBSKC_CLNCL_PGM_TYP_CD",
    "BCBSKC_CLNCL_PGM_TYP_NM",
    "CLNCL_IN_GNRL_CAT_CD",
    "CLNCL_IN_GNRL_CAT_NM",
    "CLNCL_IN_PRI_DSS_CD",
    "CLNCL_IN_PRI_DSS_NM",
    "SH_DESC",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "BCBSKC_CLNCL_PGM_TYP_CD_SK",
    "CLNCL_IN_GNRL_CAT_CD_SK",
    "CLNCL_IN_PRI_DSS_CD_SK"
]

data_unk = [
    (0, "UNK", "UNK", "1753-01-01", "1753-01-01", "UNK", "UNK", "UNK", "UNK", "UNK", "UNK", None, 100, 100, 0, 0, 0)
]
df_lnk_UNK_Out_temp = spark.createDataFrame(data_unk, col_order)
df_lnk_UNK_Out = df_lnk_UNK_Out_temp.select(
    F.col("CLNCL_IN_SK"),
    F.col("SRC_SYS_CD"),
    F.col("CLNCL_IN_ID"),
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("BCBSKC_CLNCL_PGM_TYP_CD"),
    F.col("BCBSKC_CLNCL_PGM_TYP_NM"),
    F.col("CLNCL_IN_GNRL_CAT_CD"),
    F.col("CLNCL_IN_GNRL_CAT_NM"),
    F.col("CLNCL_IN_PRI_DSS_CD"),
    F.col("CLNCL_IN_PRI_DSS_NM"),
    F.col("SH_DESC"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("BCBSKC_CLNCL_PGM_TYP_CD_SK"),
    F.col("CLNCL_IN_GNRL_CAT_CD_SK"),
    F.col("CLNCL_IN_PRI_DSS_CD_SK")
)

data_na = [
    (1, "NA", "NA", "1753-01-01", "1753-01-01", "NA", "NA", "NA", "NA", "NA", "NA", None, 100, 100, 1, 1, 1)
]
df_lnk_NA_Out_temp = spark.createDataFrame(data_na, col_order)
df_lnk_NA_Out = df_lnk_NA_Out_temp.select(
    F.col("CLNCL_IN_SK"),
    F.col("SRC_SYS_CD"),
    F.col("CLNCL_IN_ID"),
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("BCBSKC_CLNCL_PGM_TYP_CD"),
    F.col("BCBSKC_CLNCL_PGM_TYP_NM"),
    F.col("CLNCL_IN_GNRL_CAT_CD"),
    F.col("CLNCL_IN_GNRL_CAT_NM"),
    F.col("CLNCL_IN_PRI_DSS_CD"),
    F.col("CLNCL_IN_PRI_DSS_NM"),
    F.col("SH_DESC"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("BCBSKC_CLNCL_PGM_TYP_CD_SK"),
    F.col("CLNCL_IN_GNRL_CAT_CD_SK"),
    F.col("CLNCL_IN_PRI_DSS_CD_SK")
)

df_Fnl_UNK_NA_data_temp = df_lnk_NA_Out.unionByName(df_lnk_UNK_Out).unionByName(df_lnk_Full_Data_Out)
df_Fnl_UNK_NA_data = df_Fnl_UNK_NA_data_temp.select(
    F.col("CLNCL_IN_SK").alias("CLNCL_IN_SK"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CLNCL_IN_ID").alias("CLNCL_IN_ID"),
    F.col("CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("BCBSKC_CLNCL_PGM_TYP_CD").alias("BCBSKC_CLNCL_PGM_TYP_CD"),
    F.col("BCBSKC_CLNCL_PGM_TYP_NM").alias("BCBSKC_CLNCL_PGM_TYP_NM"),
    F.col("CLNCL_IN_GNRL_CAT_CD").alias("CLNCL_IN_GNRL_CAT_CD"),
    F.col("CLNCL_IN_GNRL_CAT_NM").alias("CLNCL_IN_GNRL_CAT_NM"),
    F.col("CLNCL_IN_PRI_DSS_CD").alias("CLNCL_IN_PRI_DSS_CD"),
    F.col("CLNCL_IN_PRI_DSS_NM").alias("CLNCL_IN_PRI_DSS_NM"),
    F.col("SH_DESC").alias("CLNCL_IN_SH_DESC"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("BCBSKC_CLNCL_PGM_TYP_CD_SK").alias("BCBSKC_CLNCL_PGM_TYP_CD_SK"),
    F.col("CLNCL_IN_GNRL_CAT_CD_SK").alias("CLNCL_IN_GNRL_CAT_CD_SK"),
    F.col("CLNCL_IN_PRI_DSS_CD_SK").alias("CLNCL_IN_PRI_DSS_CD_SK")
)

write_files(
    df_Fnl_UNK_NA_data,
    f"{adls_path}/load/CLNCL_IN_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)