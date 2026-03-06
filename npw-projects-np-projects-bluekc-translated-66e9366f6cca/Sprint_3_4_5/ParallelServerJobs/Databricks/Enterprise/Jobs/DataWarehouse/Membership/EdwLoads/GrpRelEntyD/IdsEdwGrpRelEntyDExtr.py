# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2012 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by: EdwMbrNoDriverSeq
# MAGIC 
# MAGIC                     
# MAGIC Processing:     Extract IDS data and populate EDW table GRP_REL_ENTY_D
# MAGIC 
# MAGIC    
# MAGIC 
# MAGIC MODIFICATIONS LOG:
# MAGIC ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# MAGIC Developer                 Date                           Project/TTR                                        Description                                                            Environment                          Code Reviewer        Review Date
# MAGIC -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# MAGIC 
# MAGIC Manasa Andru          2012-02-28               4830 - AHY 3.0                               Original programming                                                    EnterpriseCurDevl                  Brent Leland            03-02-2012
# MAGIC 
# MAGIC Pooja Sunkara          07/12/2013                  5114                                           Converted job from server to parallel version.               EnterpriseWrhsDevl

# MAGIC Read data from IDS source table GRP_REL_ENTY.
# MAGIC Extracts all data from IDS reference table CD_MPPNG.
# MAGIC 
# MAGIC Code SK lookups for Denormalization
# MAGIC Lookup Keys:
# MAGIC GRP_REL_ENTY_TERM_RSN_CD_SK
# MAGIC GRP_REL_ENTY_TYP_CD_SK
# MAGIC GRP_REL_ENTY_CAT_CD_SK
# MAGIC SRC_SYS_CD_SK
# MAGIC Add CRT_RUN_CYC_EXCTN_DT_SK and LAST_UPDT_RUN_CYC_EXCTN_DT_SK
# MAGIC Write GRP_REL_ENTY Data into a Sequential file for Load Ready Job.
# MAGIC Job name:
# MAGIC IdsEdwGrpRelEntyDExtr
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, when, length, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

extract_query_db2_GRP_REL_ENTY_in = """SELECT
 GRP_REL_ENTY.GRP_REL_ENTY_SK,
 GRP_REL_ENTY.SRC_SYS_CD_SK,
 GRP_REL_ENTY.GRP_ID,
 GRP_REL_ENTY.GRP_REL_ENTY_CAT_CD,
 GRP_REL_ENTY.GRP_REL_ENTY_TYP_CD,
 GRP_REL_ENTY.EFF_DT_SK,
 GRP_REL_ENTY.CRT_RUN_CYC_EXCTN_SK,
 GRP_REL_ENTY.LAST_UPDT_RUN_CYC_EXCTN_SK,
 GRP_REL_ENTY.GRP_SK,
 GRP_REL_ENTY.GRP_REL_ENTY_CAT_CD_SK,
 GRP_REL_ENTY.GRP_REL_ENTY_TERM_RSN_CD_SK,
 GRP_REL_ENTY.GRP_REL_ENTY_TYP_CD_SK,
 GRP_REL_ENTY.TERM_DT_SK,
 GRP_REL_ENTY.REL_ENTY_ID,
 GRP_REL_ENTY.REL_ENTY_NM
FROM {IDSOwner}.GRP_REL_ENTY GRP_REL_ENTY"""
df_db2_GRP_REL_ENTY_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_GRP_REL_ENTY_in)
    .load()
)

extract_query_db2_CD_MPPNG1_in = """SELECT 
 CD_MPPNG_SK,
 COALESCE(TRGT_CD,'UNK') TRGT_CD,
 COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM
FROM
 {IDSOwner}.CD_MPPNG"""
df_db2_CD_MPPNG1_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_CD_MPPNG1_in)
    .load()
)

df_Copy_CdMppng = df_db2_CD_MPPNG1_in

df_ref_GrpRelEntyTypCd = df_Copy_CdMppng.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_ref_GrpRelEntyTermRsnCd = df_Copy_CdMppng.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_ref_GrpRelEntyCatCd = df_Copy_CdMppng.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_ref_SrcSysCd = df_Copy_CdMppng.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_lkp_CdmaCodes = df_db2_GRP_REL_ENTY_in.alias("Ink_IdsEdwGrpRelEntyDExtr_InABC") \
    .join(
        df_ref_GrpRelEntyTermRsnCd.alias("ref_GrpRelEntyTermRsnCd"),
        col("Ink_IdsEdwGrpRelEntyDExtr_InABC.GRP_REL_ENTY_TERM_RSN_CD_SK") == col("ref_GrpRelEntyTermRsnCd.CD_MPPNG_SK"),
        "left"
    ).join(
        df_ref_GrpRelEntyCatCd.alias("ref_GrpRelEntyCatCd"),
        col("Ink_IdsEdwGrpRelEntyDExtr_InABC.GRP_REL_ENTY_CAT_CD_SK") == col("ref_GrpRelEntyCatCd.CD_MPPNG_SK"),
        "left"
    ).join(
        df_ref_GrpRelEntyTypCd.alias("ref_GrpRelEntyTypCd"),
        col("Ink_IdsEdwGrpRelEntyDExtr_InABC.GRP_REL_ENTY_TYP_CD_SK") == col("ref_GrpRelEntyTypCd.CD_MPPNG_SK"),
        "left"
    ).join(
        df_ref_SrcSysCd.alias("ref_SrcSysCd"),
        col("Ink_IdsEdwGrpRelEntyDExtr_InABC.SRC_SYS_CD_SK") == col("ref_SrcSysCd.CD_MPPNG_SK"),
        "left"
    )

df_lkp_CdmaCodes = df_lkp_CdmaCodes.select(
    col("Ink_IdsEdwGrpRelEntyDExtr_InABC.GRP_REL_ENTY_SK").alias("GRP_REL_ENTY_SK"),
    col("ref_SrcSysCd.TRGT_CD").alias("SRC_SYS_CD"),
    col("Ink_IdsEdwGrpRelEntyDExtr_InABC.GRP_ID").alias("GRP_ID"),
    col("Ink_IdsEdwGrpRelEntyDExtr_InABC.EFF_DT_SK").alias("EFF_DT_SK"),
    col("Ink_IdsEdwGrpRelEntyDExtr_InABC.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("Ink_IdsEdwGrpRelEntyDExtr_InABC.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("Ink_IdsEdwGrpRelEntyDExtr_InABC.GRP_SK").alias("GRP_SK"),
    col("Ink_IdsEdwGrpRelEntyDExtr_InABC.GRP_REL_ENTY_CAT_CD_SK").alias("GRP_REL_ENTY_CAT_CD_SK"),
    col("Ink_IdsEdwGrpRelEntyDExtr_InABC.GRP_REL_ENTY_TERM_RSN_CD_SK").alias("GRP_REL_ENTY_TERM_RSN_CD_SK"),
    col("Ink_IdsEdwGrpRelEntyDExtr_InABC.GRP_REL_ENTY_TYP_CD_SK").alias("GRP_REL_ENTY_TYP_CD_SK"),
    col("Ink_IdsEdwGrpRelEntyDExtr_InABC.TERM_DT_SK").alias("TERM_DT_SK"),
    col("Ink_IdsEdwGrpRelEntyDExtr_InABC.REL_ENTY_ID").alias("REL_ENTY_ID"),
    col("Ink_IdsEdwGrpRelEntyDExtr_InABC.REL_ENTY_NM").alias("REL_ENTY_NM"),
    col("ref_GrpRelEntyTermRsnCd.TRGT_CD").alias("GRP_REL_ENTY_TERM_RSN_CD"),
    col("ref_GrpRelEntyTermRsnCd.TRGT_CD_NM").alias("GRP_REL_ENTY_TERM_RSN_NM"),
    col("ref_GrpRelEntyCatCd.TRGT_CD").alias("GRP_REL_ENTY_CAT_CD"),
    col("ref_GrpRelEntyCatCd.TRGT_CD_NM").alias("GRP_REL_ENTY_CAT_NM"),
    col("ref_GrpRelEntyTypCd.TRGT_CD").alias("GRP_REL_ENTY_TYP_CD"),
    col("ref_GrpRelEntyTypCd.TRGT_CD_NM").alias("GRP_REL_ENTY_TYP_NM")
)

df_temp = df_lkp_CdmaCodes.withColumnRenamed("LAST_UPDT_RUN_CYC_EXCTN_SK", "TEMP_IDS_LAST_UPDT_RUN_CYC_EXCTN_SK")

df_xfrm_businessLogic = df_temp.select(
    col("GRP_REL_ENTY_SK"),
    when((col("SRC_SYS_CD").isNull()) | (length(col("SRC_SYS_CD")) == 0), "UNK").otherwise(col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
    col("GRP_ID"),
    when((col("GRP_REL_ENTY_CAT_CD").isNull()) | (length(col("GRP_REL_ENTY_CAT_CD")) == 0), "UNK").otherwise(col("GRP_REL_ENTY_CAT_CD")).alias("GRP_REL_ENTY_CAT_CD"),
    when((col("GRP_REL_ENTY_TYP_CD").isNull()) | (length(col("GRP_REL_ENTY_TYP_CD")) == 0), "UNK").otherwise(col("GRP_REL_ENTY_TYP_CD")).alias("GRP_REL_ENTY_TYP_CD"),
    col("EFF_DT_SK").alias("GRP_REL_ENTY_EFF_DT_SK"),
    lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("GRP_SK"),
    when((col("GRP_REL_ENTY_CAT_NM").isNull()) | (length(col("GRP_REL_ENTY_CAT_NM")) == 0), "UNK").otherwise(col("GRP_REL_ENTY_CAT_NM")).alias("GRP_REL_ENTY_CAT_NM"),
    when((col("GRP_REL_ENTY_TERM_RSN_CD").isNull()) | (length(col("GRP_REL_ENTY_TERM_RSN_CD")) == 0), "UNK").otherwise(col("GRP_REL_ENTY_TERM_RSN_CD")).alias("GRP_REL_ENTY_TERM_RSN_CD"),
    when((col("GRP_REL_ENTY_TERM_RSN_NM").isNull()) | (length(col("GRP_REL_ENTY_TERM_RSN_NM")) == 0), "UNK").otherwise(col("GRP_REL_ENTY_TERM_RSN_NM")).alias("GRP_REL_ENTY_TERM_RSN_NM"),
    when((col("GRP_REL_ENTY_TYP_NM").isNull()) | (length(col("GRP_REL_ENTY_TYP_NM")) == 0), "UNK").otherwise(col("GRP_REL_ENTY_TYP_NM")).alias("GRP_REL_ENTY_TYP_NM"),
    col("TERM_DT_SK").alias("GRP_REL_ENTY_TERM_DT_SK"),
    col("REL_ENTY_ID"),
    col("REL_ENTY_NM"),
    lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("TEMP_IDS_LAST_UPDT_RUN_CYC_EXCTN_SK").alias("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("GRP_REL_ENTY_CAT_CD_SK"),
    col("GRP_REL_ENTY_TERM_RSN_CD_SK"),
    col("GRP_REL_ENTY_TYP_CD_SK")
)

df_final = df_xfrm_businessLogic
df_final = df_final.withColumn("GRP_REL_ENTY_EFF_DT_SK", rpad(col("GRP_REL_ENTY_EFF_DT_SK"), 10, " "))
df_final = df_final.withColumn("CRT_RUN_CYC_EXCTN_DT_SK", rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
df_final = df_final.withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
df_final = df_final.withColumn("GRP_REL_ENTY_TERM_DT_SK", rpad(col("GRP_REL_ENTY_TERM_DT_SK"), 10, " "))

write_files(
    df_final.select(
        "GRP_REL_ENTY_SK",
        "SRC_SYS_CD",
        "GRP_ID",
        "GRP_REL_ENTY_CAT_CD",
        "GRP_REL_ENTY_TYP_CD",
        "GRP_REL_ENTY_EFF_DT_SK",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "GRP_SK",
        "GRP_REL_ENTY_CAT_NM",
        "GRP_REL_ENTY_TERM_RSN_CD",
        "GRP_REL_ENTY_TERM_RSN_NM",
        "GRP_REL_ENTY_TYP_NM",
        "GRP_REL_ENTY_TERM_DT_SK",
        "REL_ENTY_ID",
        "REL_ENTY_NM",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "IDS_LAST_UPDT_RUN_CYC_EXCTN_SK",
        "GRP_REL_ENTY_CAT_CD_SK",
        "GRP_REL_ENTY_TERM_RSN_CD_SK",
        "GRP_REL_ENTY_TYP_CD_SK"
    ),
    f"{adls_path}/load/GRP_REL_ENTY_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)