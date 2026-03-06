# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2012 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY: EdwGrpAhyPgmCntl
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION: Extracts from IDS GRP_AHY_PGM and loads into the EDW table
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      \(9)Change Description                                                                          Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      \(9)-------------------------------------------------                                                       --------------------------------       -------------------------------   ----------------------------   
# MAGIC Kalyan Neelam          2012-11-13       4830                               Initial Programming                                                                         EnterpriseNewDevl       Bhoomi Dasari            11/29/2012
# MAGIC 
# MAGIC Manasa Andru         2014-04-03         TFS - 8459              Added 9 new fields - GRP_AHY_PGM_AHY_BUYUP_CD,                  EnterpriseNewDevl          Kalyan Neelam          2014-06-24
# MAGIC                                                                                   GRP_AHY_PGM_AHY_BUYUP_NM, GRP_AHY_PGM_AHY_BUYUP_CD_SK,                  
# MAGIC                                                                                   GRP_AHY_PGM_SPOUSE_BUYUP_CD, GRP_AHY_PGM_SPOUSE_BUYUP_NM,
# MAGIC                                                                                   GRP_AHY_PGM_SPOUSE_BUYUP_CD_SK, GRP_AHY_PGM_SUB_BUYUP_CD,
# MAGIC                                                                                     GRP_AHY_PGM_SUB_BUYUP_NM,  GRP_AHY_PGM_SUB_BUYUP_CD_SK
# MAGIC 
# MAGIC Raja Gummadi       2016-06-21           5414                       Added new column GRP_AHY_PGM_OPT_OUT_IN                           EnterpriseDev1               Jag Yelavarthi              2016-06-24

# MAGIC EDW GRP AHY PGM Extract job
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, when, length, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


EDWOwner = get_widget_value("EDWOwner","")
edw_secret_name = get_widget_value("edw_secret_name","")
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
CurrDate = get_widget_value("CurrDate","")
CurrRunCycle = get_widget_value("CurrRunCycle","")
IDSRunCycle = get_widget_value("IDSRunCycle","")

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"""
SELECT GRP_AHY_PGM.GRP_AHY_PGM_SK,GRP_AHY_PGM.GRP_ID,GRP_AHY_PGM.GRP_AHY_PGM_STRT_DT_SK,GRP_AHY_PGM.SRC_SYS_CD_SK,GRP_AHY_PGM.CRT_RUN_CYC_EXCTN_SK,GRP_AHY_PGM.LAST_UPDT_RUN_CYC_EXCTN_SK,GRP_AHY_PGM.GRP_SK,GRP_AHY_PGM.GRP_AHY_PGM_END_DT_SK,GRP_AHY_PGM.GRP_AHY_PGM_INCNTV_STRT_DT_SK,GRP_AHY_PGM.GRP_AHY_PGM_INCNTV_END_DT_SK,GRP_AHY_PGM.GRP_SEL_SPOUSE_AHY_PGM_ID,GRP_AHY_PGM.GRP_SEL_SUB_AHY_PGM_ID,GRP_AHY_PGM.GRP_SEL_SUB_AHY_ONLY_PGM_ID,GRP_AHY_PGM_AHY_BUYUP_CD_SK, GRP_AHY_PGM_SPOUSE_BUYUP_CD_SK, GRP_AHY_PGM_SUB_BUYUP_CD_SK,GRP_AHY_PGM.GRP_AHY_PGM_OPT_OUT_IN
FROM {IDSOwner}.GRP_AHY_PGM GRP_AHY_PGM

WHERE
GRP_AHY_PGM.LAST_UPDT_RUN_CYC_EXCTN_SK >= {IDSRunCycle}

UNION

SELECT GRP_AHY_PGM.GRP_AHY_PGM_SK,GRP_AHY_PGM.GRP_ID,GRP_AHY_PGM.GRP_AHY_PGM_STRT_DT_SK,GRP_AHY_PGM.SRC_SYS_CD_SK,GRP_AHY_PGM.CRT_RUN_CYC_EXCTN_SK,GRP_AHY_PGM.LAST_UPDT_RUN_CYC_EXCTN_SK,GRP_AHY_PGM.GRP_SK,GRP_AHY_PGM.GRP_AHY_PGM_END_DT_SK,GRP_AHY_PGM.GRP_AHY_PGM_INCNTV_STRT_DT_SK,GRP_AHY_PGM.GRP_AHY_PGM_INCNTV_END_DT_SK,GRP_AHY_PGM.GRP_SEL_SPOUSE_AHY_PGM_ID,GRP_AHY_PGM.GRP_SEL_SUB_AHY_PGM_ID,GRP_AHY_PGM.GRP_SEL_SUB_AHY_ONLY_PGM_ID,GRP_AHY_PGM_AHY_BUYUP_CD_SK, GRP_AHY_PGM_SPOUSE_BUYUP_CD_SK, GRP_AHY_PGM_SUB_BUYUP_CD_SK,GRP_AHY_PGM_OPT_OUT_IN
FROM {IDSOwner}.GRP_AHY_PGM GRP_AHY_PGM , {IDSOwner}.CD_MPPNG CD

WHERE
GRP_AHY_PGM.SRC_SYS_CD_SK = CD.CD_MPPNG_SK AND CD.TRGT_CD = 'CRM'
"""

df_GRP_AHY_PGM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_hf_cdma_codes = spark.read.parquet(f"{adls_path}/hf_cdma_codes.parquet")
df_hf_ahy_pgm_new_codes = spark.read.parquet(f"{adls_path}/hf_cdma_codes.parquet")

df_Extract_alias = df_GRP_AHY_PGM.alias("Extract")
df_SrcSysCdlkup_alias = df_hf_cdma_codes.alias("SrcSysCdlkup")
df_AhyPgmBuyUpCd_alias = df_hf_ahy_pgm_new_codes.alias("AhyPgmBuyUpCd")
df_AhyPgmSpBuyUpCd_alias = df_hf_ahy_pgm_new_codes.alias("AhyPgmSpBuyUpCd")
df_AhyPgmSubBuyUpCd_alias = df_hf_ahy_pgm_new_codes.alias("AhyPgmSubBuyUpCd")

df_joined = (
    df_Extract_alias
    .join(df_SrcSysCdlkup_alias, col("Extract.SRC_SYS_CD_SK") == col("SrcSysCdlkup.CD_MPPNG_SK"), "left")
    .join(df_AhyPgmBuyUpCd_alias, col("Extract.GRP_AHY_PGM_AHY_BUYUP_CD_SK") == col("AhyPgmBuyUpCd.CD_MPPNG_SK"), "left")
    .join(df_AhyPgmSpBuyUpCd_alias, col("Extract.GRP_AHY_PGM_SPOUSE_BUYUP_CD_SK") == col("AhyPgmSpBuyUpCd.CD_MPPNG_SK"), "left")
    .join(df_AhyPgmSubBuyUpCd_alias, col("Extract.GRP_AHY_PGM_SUB_BUYUP_CD_SK") == col("AhyPgmSubBuyUpCd.CD_MPPNG_SK"), "left")
)

df_enriched = df_joined.select(
    col("Extract.GRP_AHY_PGM_SK").alias("GRP_AHY_PGM_SK"),
    col("Extract.GRP_ID").alias("GRP_ID"),
    rpad(col("Extract.GRP_AHY_PGM_STRT_DT_SK"), 10, " ").alias("GRP_AHY_PGM_STRT_DT_SK"),
    rpad(
        when(col("SrcSysCdlkup.TRGT_CD").isNull(), lit("UNK"))
        .otherwise(col("SrcSysCdlkup.TRGT_CD")),
        255, " "
    ).alias("SRC_SYS_CD"),
    rpad(lit(CurrDate), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad(lit(CurrDate), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("Extract.GRP_SK").alias("GRP_SK"),
    rpad(col("Extract.GRP_AHY_PGM_END_DT_SK"), 10, " ").alias("GRP_AHY_PGM_END_DT_SK"),
    rpad(col("Extract.GRP_AHY_PGM_INCNTV_STRT_DT_SK"), 10, " ").alias("GRP_AHY_PGM_INCNTV_STRT_DT_SK"),
    rpad(col("Extract.GRP_AHY_PGM_INCNTV_END_DT_SK"), 10, " ").alias("GRP_AHY_PGM_INCNTV_END_DT_SK"),
    col("Extract.GRP_SEL_SPOUSE_AHY_PGM_ID").alias("GRP_SEL_SPOUSE_AHY_PGM_ID"),
    col("Extract.GRP_SEL_SUB_AHY_PGM_ID").alias("GRP_SEL_SUB_AHY_PGM_ID"),
    col("Extract.GRP_SEL_SUB_AHY_ONLY_PGM_ID").alias("GRP_SEL_SUB_AHY_ONLY_PGM_ID"),
    lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("Extract.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(
        when(
            col("AhyPgmBuyUpCd.TRGT_CD").isNull() | (length(trim(col("AhyPgmBuyUpCd.TRGT_CD"))) == 0),
            lit("NA")
        ).otherwise(col("AhyPgmBuyUpCd.TRGT_CD")),
        255, " "
    ).alias("GRP_AHY_PGM_AHY_BUYUP_CD"),
    rpad(
        when(
            col("AhyPgmBuyUpCd.TRGT_CD_NM").isNull() | (length(trim(col("AhyPgmBuyUpCd.TRGT_CD_NM"))) == 0),
            lit("NA")
        ).otherwise(col("AhyPgmBuyUpCd.TRGT_CD_NM")),
        255, " "
    ).alias("GRP_AHY_PGM_AHY_BUYUP_NM"),
    col("Extract.GRP_AHY_PGM_AHY_BUYUP_CD_SK").alias("GRP_AHY_PGM_AHY_BUYUP_CD_SK"),
    rpad(
        when(
            col("AhyPgmSpBuyUpCd.TRGT_CD").isNull() | (length(trim(col("AhyPgmSpBuyUpCd.TRGT_CD"))) == 0),
            lit("NA")
        ).otherwise(col("AhyPgmSpBuyUpCd.TRGT_CD")),
        255, " "
    ).alias("GRP_AHY_PGM_SPOUSE_BUYUP_CD"),
    rpad(
        when(
            col("AhyPgmSpBuyUpCd.TRGT_CD_NM").isNull() | (length(trim(col("AhyPgmSpBuyUpCd.TRGT_CD_NM"))) == 0),
            lit("NA")
        ).otherwise(col("AhyPgmSpBuyUpCd.TRGT_CD_NM")),
        255, " "
    ).alias("GRP_AHY_PGM_SPOUSE_BUYUP_NM"),
    col("Extract.GRP_AHY_PGM_SPOUSE_BUYUP_CD_SK").alias("GRP_AHY_PGM_SPOUSE_BUYUP_CD_SK"),
    rpad(
        when(
            col("AhyPgmSubBuyUpCd.TRGT_CD").isNull() | (length(trim(col("AhyPgmSubBuyUpCd.TRGT_CD"))) == 0),
            lit("NA")
        ).otherwise(col("AhyPgmSubBuyUpCd.TRGT_CD")),
        255, " "
    ).alias("GRP_AHY_PGM_SUB_BUYUP_CD"),
    rpad(
        when(
            col("AhyPgmSubBuyUpCd.TRGT_CD_NM").isNull() | (length(trim(col("AhyPgmSubBuyUpCd.TRGT_CD_NM"))) == 0),
            lit("NA")
        ).otherwise(col("AhyPgmSubBuyUpCd.TRGT_CD_NM")),
        255, " "
    ).alias("GRP_AHY_PGM_SUB_BUYUP_NM"),
    col("Extract.GRP_AHY_PGM_SUB_BUYUP_CD_SK").alias("GRP_AHY_PGM_SUB_BUYUP_CD_SK"),
    rpad(col("Extract.GRP_AHY_PGM_OPT_OUT_IN"), 1, " ").alias("GRP_AHY_PGM_OPT_OUT_IN")
)

write_files(
    df_enriched,
    f"{adls_path}/load/GRP_AHY_PGM_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)