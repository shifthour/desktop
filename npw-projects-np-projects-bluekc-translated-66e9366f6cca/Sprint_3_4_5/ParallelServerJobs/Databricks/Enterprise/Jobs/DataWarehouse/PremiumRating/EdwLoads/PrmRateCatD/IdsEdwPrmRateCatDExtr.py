# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC Â© Copyright 2009 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by:
# MAGIC                     *
# MAGIC 
# MAGIC Processing:
# MAGIC                     *
# MAGIC 
# MAGIC Control Job Rerun Information: 
# MAGIC                     Previous Run Successful:    What needs to happen before a special run can occur?
# MAGIC                     Previous Run Aborted:         Restart, no other steps necessary
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                                  Project/                                                                                                                       Code                   Date
# MAGIC Developer           Date              Altiris #     Change Description                                                                                     Reviewer            Reviewed
# MAGIC -------------------------  -------------------   -------------   -----------------------------------------------------------------------------------------------------------------   -------------------------  -------------------
# MAGIC Landon Hall        01/05/2009   Q3 TTR   Original program                                                                                           Steph Goddard   02/10/2009
# MAGIC 
# MAGIC Pooja Sunkara    08/29/2013    5114       Converted from Server to Parallel                                                               Peter Marshall      11/20/2013

# MAGIC Read data from source table PRM_RATE_CAT
# MAGIC Extracts all data from IDS reference table CD_MPPNG.
# MAGIC 
# MAGIC Code SK lookups for Denormalization
# MAGIC Lookup Keys:
# MAGIC GRP_REL_ENTY_TERM_RSN_CD_SK
# MAGIC GRP_REL_ENTY_TYP_CD_SK
# MAGIC GRP_REL_ENTY_CAT_CD_SK
# MAGIC SRC_SYS_CD_SK
# MAGIC Add CRT_RUN_CYC_EXCTN_DT_SK and LAST_UPDT_RUN_CYC_EXCTN_DT_SK
# MAGIC Write PRM_RATE_CAT_D Data into a Sequential file for Load Ready Job.
# MAGIC Extracts Premium Rating for loading into EDW System - PRM_RATE_CAT_D
# MAGIC Job name:
# MAGIC IdsEdwPrmRateCatDExtr
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame, functions as F, Window
from pyspark.sql.functions import col, lit, row_number, rpad
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# Stage: db2_PRM_RATE_CAT_in
extract_query_db2_PRM_RATE_CAT_in = f"""
SELECT
  PRIM_RATE_CAT.PRM_RATE_CAT_SK,
  COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
  PRIM_RATE_CAT.PRM_RATE_UNIQ_KEY,
  PRIM_RATE_CAT.TIER_MOD_ID,
  PRIM_RATE_CAT.PRM_RATE_CAT_GNDR_CD_SK,
  PRIM_RATE_CAT.PRM_RATE_CAT_SMKR_CD_SK,
  PRIM_RATE_CAT.EFF_DT_SK,
  PRIM_RATE_CAT.CRT_RUN_CYC_EXCTN_SK,
  PRIM_RATE_CAT.LAST_UPDT_RUN_CYC_EXCTN_SK,
  PRIM_RATE_CAT.PRM_RATE_SK,
  PRIM_RATE_CAT.TERM_DT_SK,
  PRIM_RATE_CAT.AGE_BAND_REF_ID
FROM {IDSOwner}.PRM_RATE_CAT PRIM_RATE_CAT
LEFT JOIN {IDSOwner}.CD_MPPNG CD
  ON PRIM_RATE_CAT.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
"""
df_db2_PRM_RATE_CAT_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_PRM_RATE_CAT_in)
    .load()
)

# Stage: db2_CD_MPPNG1_in
extract_query_db2_CD_MPPNG1_in = f"""
SELECT
  CD_MPPNG_SK,
  COALESCE(TRGT_CD,'UNK') TRGT_CD,
  COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG
"""
df_db2_CD_MPPNG1_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CD_MPPNG1_in)
    .load()
)

# Stage: Copy_CdMppng
df_Copy_CdMppng_ref_PrmRatesMkr = df_db2_CD_MPPNG1_in.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_Copy_CdMppng_ref_PrmRateGender = df_db2_CD_MPPNG1_in.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

# Stage: lkp_CdmaCodes (PxLookup)
df_left_join_1 = (
    df_db2_PRM_RATE_CAT_in.alias("Ink_IdsEdwPrmRateCatDExtr_InABC")
    .join(
        df_Copy_CdMppng_ref_PrmRatesMkr.alias("ref_PrmRatesMkr"),
        col("Ink_IdsEdwPrmRateCatDExtr_InABC.PRM_RATE_CAT_SMKR_CD_SK") == col("ref_PrmRatesMkr.CD_MPPNG_SK"),
        "left"
    )
)
df_lkp_CdmaCodes = (
    df_left_join_1.join(
        df_Copy_CdMppng_ref_PrmRateGender.alias("ref_PrmRateGender"),
        col("Ink_IdsEdwPrmRateCatDExtr_InABC.PRM_RATE_CAT_GNDR_CD_SK") == col("ref_PrmRateGender.CD_MPPNG_SK"),
        "left"
    )
)

df_lkp_CdmaCodes = df_lkp_CdmaCodes.select(
    col("Ink_IdsEdwPrmRateCatDExtr_InABC.PRM_RATE_CAT_SK").alias("PRM_RATE_CAT_SK"),
    col("Ink_IdsEdwPrmRateCatDExtr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("Ink_IdsEdwPrmRateCatDExtr_InABC.PRM_RATE_UNIQ_KEY").alias("PRM_RATE_UNIQ_KEY"),
    col("Ink_IdsEdwPrmRateCatDExtr_InABC.TIER_MOD_ID").alias("TIER_MOD_ID"),
    col("Ink_IdsEdwPrmRateCatDExtr_InABC.PRM_RATE_CAT_GNDR_CD_SK").alias("PRM_RATE_CAT_GNDR_CD_SK"),
    col("Ink_IdsEdwPrmRateCatDExtr_InABC.PRM_RATE_CAT_SMKR_CD_SK").alias("PRM_RATE_CAT_SMOKER_CD_SK"),
    col("Ink_IdsEdwPrmRateCatDExtr_InABC.EFF_DT_SK").alias("EFF_DT_SK"),
    col("Ink_IdsEdwPrmRateCatDExtr_InABC.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("Ink_IdsEdwPrmRateCatDExtr_InABC.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("Ink_IdsEdwPrmRateCatDExtr_InABC.PRM_RATE_SK").alias("PRM_RATE_SK"),
    col("Ink_IdsEdwPrmRateCatDExtr_InABC.TERM_DT_SK").alias("TERM_DT_SK"),
    col("Ink_IdsEdwPrmRateCatDExtr_InABC.AGE_BAND_REF_ID").alias("AGE_BAND_REF_ID"),
    col("ref_PrmRatesMkr.TRGT_CD").alias("PRM_RATE_CAT_SMKR_CD"),
    col("ref_PrmRatesMkr.TRGT_CD_NM").alias("PRM_RATE_CAT_SMKR_NM"),
    col("ref_PrmRateGender.TRGT_CD").alias("PRM_RATE_CAT_GNDR_CD"),
    col("ref_PrmRateGender.TRGT_CD_NM").alias("PRM_RATE_CAT_GNDR_NM")
)

# Stage: xfrm_businessLogic (CTransformerStage)
df_lkp_CdmaCodes_withIndex = df_lkp_CdmaCodes.withColumn(
    "row_id", row_number().over(Window.orderBy(F.lit(1)))
)

df_xfrm_businessLogic_lnk_IdsEdwPrmRateCatDExtr_OutMain = (
    df_lkp_CdmaCodes_withIndex
    .filter(
        (col("PRM_RATE_CAT_SK") != 0) & (col("PRM_RATE_CAT_SK") != 1)
    )
    .select(
        col("PRM_RATE_CAT_SK").alias("PRM_RATE_CAT_SK"),
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("PRM_RATE_UNIQ_KEY").alias("PRM_RATE_UNIQ_KEY"),
        col("TIER_MOD_ID").alias("PRM_RATE_CAT_TIER_MOD_ID"),
        col("PRM_RATE_CAT_GNDR_CD").alias("PRM_RATE_CAT_GNDR_CD"),
        col("PRM_RATE_CAT_SMKR_CD").alias("PRM_RATE_CAT_SMKR_CD"),
        col("EFF_DT_SK").alias("PRM_RATE_CAT_EFF_DT_SK"),
        lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        col("PRM_RATE_SK").alias("PRM_RATE_SK"),
        col("AGE_BAND_REF_ID").alias("PRM_RATE_AGE_CAT_BAND_REF_ID"),
        col("PRM_RATE_CAT_GNDR_NM").alias("PRM_RATE_CAT_GNDR_NM"),
        col("PRM_RATE_CAT_SMKR_NM").alias("PRM_RATE_CAT_SMKR_NM"),
        col("TERM_DT_SK").alias("PRM_RATE_CAT_TERM_DT_SK"),
        lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("PRM_RATE_CAT_GNDR_CD_SK").alias("PRM_RATE_CAT_GNDR_CD_SK"),
        col("PRM_RATE_CAT_SMOKER_CD_SK").alias("PRM_RATE_CAT_SMKR_CD_SK")
    )
)

df_xfrm_businessLogic_NALink = (
    df_lkp_CdmaCodes_withIndex
    .filter(col("row_id") == 1)
    .select(
        lit(1).alias("PRM_RATE_CAT_SK"),
        lit("NA").alias("SRC_SYS_CD"),
        lit(1).alias("PRM_RATE_UNIQ_KEY"),
        lit("NA").alias("PRM_RATE_CAT_TIER_MOD_ID"),
        lit("NA").alias("PRM_RATE_CAT_GNDR_CD"),
        lit("NA").alias("PRM_RATE_CAT_SMKR_CD"),
        lit("1753-01-01").alias("PRM_RATE_CAT_EFF_DT_SK"),
        lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        lit(1).alias("PRM_RATE_SK"),
        lit("1753-01-01 00:00:00").alias("PRM_RATE_AGE_CAT_BAND_REF_ID"),
        lit("NA").alias("PRM_RATE_CAT_GNDR_NM"),
        lit("NA").alias("PRM_RATE_CAT_SMKR_NM"),
        lit("1753-01-01").alias("PRM_RATE_CAT_TERM_DT_SK"),
        lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(1).alias("PRM_RATE_CAT_GNDR_CD_SK"),
        lit(1).alias("PRM_RATE_CAT_SMKR_CD_SK")
    )
)

df_xfrm_businessLogic_UNKLink = (
    df_lkp_CdmaCodes_withIndex
    .filter(col("row_id") == 1)
    .select(
        lit(0).alias("PRM_RATE_CAT_SK"),
        lit("UNK").alias("SRC_SYS_CD"),
        lit(0).alias("PRM_RATE_UNIQ_KEY"),
        lit("UNK").alias("PRM_RATE_CAT_TIER_MOD_ID"),
        lit("UNK").alias("PRM_RATE_CAT_GNDR_CD"),
        lit("UNK").alias("PRM_RATE_CAT_SMKR_CD"),
        lit("1753-01-01").alias("PRM_RATE_CAT_EFF_DT_SK"),
        lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        lit(0).alias("PRM_RATE_SK"),
        lit("1753-01-01 00:00:00").alias("PRM_RATE_AGE_CAT_BAND_REF_ID"),
        lit("UNK").alias("PRM_RATE_CAT_GNDR_NM"),
        lit("UNK").alias("PRM_RATE_CAT_SMKR_NM"),
        lit("1753-01-01").alias("PRM_RATE_CAT_TERM_DT_SK"),
        lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(0).alias("PRM_RATE_CAT_GNDR_CD_SK"),
        lit(0).alias("PRM_RATE_CAT_SMKR_CD_SK")
    )
)

# Stage: Funnel_37 (PxFunnel)
# Ensure column order is the same for all three inputs
common_cols = [
    "PRM_RATE_CAT_SK",
    "SRC_SYS_CD",
    "PRM_RATE_UNIQ_KEY",
    "PRM_RATE_CAT_TIER_MOD_ID",
    "PRM_RATE_CAT_GNDR_CD",
    "PRM_RATE_CAT_SMKR_CD",
    "PRM_RATE_CAT_EFF_DT_SK",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "PRM_RATE_SK",
    "PRM_RATE_AGE_CAT_BAND_REF_ID",
    "PRM_RATE_CAT_GNDR_NM",
    "PRM_RATE_CAT_SMKR_NM",
    "PRM_RATE_CAT_TERM_DT_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "PRM_RATE_CAT_GNDR_CD_SK",
    "PRM_RATE_CAT_SMKR_CD_SK"
]

df_Funnel_37 = (
    df_xfrm_businessLogic_lnk_IdsEdwPrmRateCatDExtr_OutMain.select(common_cols)
    .unionByName(df_xfrm_businessLogic_NALink.select(common_cols))
    .unionByName(df_xfrm_businessLogic_UNKLink.select(common_cols))
)

# Final select with rpad for char(10) columns
df_Funnel_37_final = df_Funnel_37.select(
    col("PRM_RATE_CAT_SK"),
    col("SRC_SYS_CD"),
    col("PRM_RATE_UNIQ_KEY"),
    col("PRM_RATE_CAT_TIER_MOD_ID"),
    col("PRM_RATE_CAT_GNDR_CD"),
    col("PRM_RATE_CAT_SMKR_CD"),
    rpad(col("PRM_RATE_CAT_EFF_DT_SK"), 10, " ").alias("PRM_RATE_CAT_EFF_DT_SK"),
    rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("PRM_RATE_SK"),
    col("PRM_RATE_AGE_CAT_BAND_REF_ID"),
    col("PRM_RATE_CAT_GNDR_NM"),
    col("PRM_RATE_CAT_SMKR_NM"),
    rpad(col("PRM_RATE_CAT_TERM_DT_SK"), 10, " ").alias("PRM_RATE_CAT_TERM_DT_SK"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("PRM_RATE_CAT_GNDR_CD_SK"),
    col("PRM_RATE_CAT_SMKR_CD_SK")
)

# Stage: seq_PRM_RATE_CAT_D_csv_load (PxSequentialFile)
write_files(
    df_Funnel_37_final,
    f"{adls_path}/load/PRM_RATE_CAT_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)