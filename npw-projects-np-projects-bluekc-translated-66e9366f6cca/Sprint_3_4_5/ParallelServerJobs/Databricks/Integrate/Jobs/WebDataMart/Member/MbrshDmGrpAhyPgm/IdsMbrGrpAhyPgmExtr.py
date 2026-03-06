# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2012 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC CALLED BY:  HlthFtnsGrpAhyPgmCntl
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                     Extracts data from IDS GRP_AHY_PGM table and loads the DataMart table MBRSH_DM_GRP_AHY_PGM.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #                                   Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------                                   -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Kalyan Neelam         2012-11-28       4830                                                   Original Programming                                                                   IntegrateNewDevl          Bhoomi Dasari            11/29/2012

# MAGIC If there are multiple Previous rows then only retain the most previous one
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, when, lit, rpad
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
ClmMartOwner = get_widget_value('ClmMartOwner','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')
CurrDate = get_widget_value('CurrDate','')

# ---------------------
# Stage: CD_MPPNG (DB2Connector)
# ---------------------
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_CD_MPPNG = (
    f"SELECT SRC_CD, TRGT_CD "
    f"FROM {IDSOwner}.CD_MPPNG "
    f"WHERE TRGT_DOMAIN_NM = 'PERIOD' "
    f"  AND TRGT_CLCTN_CD = 'WEB ACCESS DATA MART' "
    f"  AND SRC_DOMAIN_NM = 'CUSTOM DOMAIN MAPPING'"
)
df_CD_MPPNG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_CD_MPPNG)
    .load()
)

# ---------------------
# Stage: hf_mbrdm_grpahypgm_prdcdlkup (CHashedFileStage) - Scenario A
#   (Intermediate hashed file: CD_MPPNG -> hf_mbrdm_grpahypgm_prdcdlkup -> Period_Lkup)
#   Key column for dedup is SRC_CD
# ---------------------
df_hf_mbrdm_grpahypgm_prdcdlkup = dedup_sort(df_CD_MPPNG, ["SRC_CD"], [])

# ---------------------
# Stage: hf_etrnl_cd_mppng (CHashedFileStage) - Scenario C (source-only hashed file)
#   Translate as a read from parquet
#   Apply expression for CD_MPPNG_SK = trim(SRC_SYS_CD_SK)
# ---------------------
df_hf_etrnl_cd_mppng_raw = spark.read.parquet(f"{adls_path}/hf_etrnl_cd_mppng.parquet")
df_hf_etrnl_cd_mppng = df_hf_etrnl_cd_mppng_raw.select(
    trim(col("SRC_SYS_CD_SK")).alias("CD_MPPNG_SK"),
    col("TRGT_CD"),
    col("TRGT_CD_NM"),
    col("SRC_CD"),
    col("SRC_CD_NM")
)

# ---------------------
# Stage: IDS_GRP_AHY_PGM (DB2Connector)
# ---------------------
extract_query_IDS_GRP_AHY_PGM = (
    f"SELECT GRP_AHY_PGM.GRP_ID, "
    f"       GRP_AHY_PGM.GRP_AHY_PGM_STRT_DT_SK, "
    f"       GRP_AHY_PGM.SRC_SYS_CD_SK, "
    f"       GRP_AHY_PGM.LAST_UPDT_RUN_CYC_EXCTN_SK, "
    f"       GRP_AHY_PGM.GRP_AHY_PGM_END_DT_SK, "
    f"       GRP_AHY_PGM.GRP_AHY_PGM_INCNTV_STRT_DT_SK, "
    f"       GRP_AHY_PGM.GRP_AHY_PGM_INCNTV_END_DT_SK, "
    f"       GRP_AHY_PGM.GRP_SEL_SPOUSE_AHY_PGM_ID, "
    f"       GRP_AHY_PGM.GRP_SEL_SUB_AHY_PGM_ID, "
    f"       GRP_AHY_PGM.GRP_SEL_SUB_AHY_ONLY_PGM_ID "
    f"FROM {IDSOwner}.GRP_AHY_PGM GRP_AHY_PGM "
    f"WHERE GRP_AHY_PGM_SK NOT IN (0,1)"
)
df_IDS_GRP_AHY_PGM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_IDS_GRP_AHY_PGM)
    .load()
)

# ---------------------
# Stage: Transformer
#   Primary link: df_IDS_GRP_AHY_PGM (alias Extract)
#   Lookup link: df_hf_etrnl_cd_mppng (alias Src_Sys_Cd_Lookup) left join
#   Output link: "Sort"
# ---------------------
df_Transformer = (
    df_IDS_GRP_AHY_PGM.alias("Extract")
    .join(
        df_hf_etrnl_cd_mppng.alias("Src_Sys_Cd_Lookup"),
        col("Extract.SRC_SYS_CD_SK") == col("Src_Sys_Cd_Lookup.CD_MPPNG_SK"),
        "left"
    )
    .select(
        col("Extract.GRP_ID").alias("GRP_ID"),
        when(
            (col("Extract.GRP_AHY_PGM_STRT_DT_SK") < lit(CurrDate))
            & (col("Extract.GRP_AHY_PGM_END_DT_SK") < lit(CurrDate)),
            lit("PREV")
        )
        .when(
            (col("Extract.GRP_AHY_PGM_STRT_DT_SK") <= lit(CurrDate))
            & (col("Extract.GRP_AHY_PGM_END_DT_SK") >= lit(CurrDate)),
            lit("CUR")
        )
        .when(
            (col("Extract.GRP_AHY_PGM_STRT_DT_SK") > lit(CurrDate))
            & (col("Extract.GRP_AHY_PGM_END_DT_SK") > lit(CurrDate)),
            lit("FTR")
        )
        .otherwise(lit("NA"))
        .alias("AHY_PERD_CD"),
        col("Extract.GRP_AHY_PGM_STRT_DT_SK").alias("GRP_AHY_PGM_STRT_DT"),
        col("Extract.GRP_AHY_PGM_END_DT_SK").alias("GRP_AHY_PGM_END_DT"),
        when(col("Src_Sys_Cd_Lookup.TRGT_CD").isNull(), lit("UNK"))
        .otherwise(col("Src_Sys_Cd_Lookup.TRGT_CD"))
        .alias("SRC_SYS_CD"),
        col("Extract.GRP_AHY_PGM_INCNTV_STRT_DT_SK").alias("GRP_AHY_PGM_INCNTV_STRT_DT"),
        col("Extract.GRP_AHY_PGM_INCNTV_END_DT_SK").alias("GRP_AHY_PGM_INCNTV_END_DT"),
        col("Extract.GRP_SEL_SPOUSE_AHY_PGM_ID").alias("GRP_SEL_SPOUSE_AHY_PGM_ID"),
        col("Extract.GRP_SEL_SUB_AHY_PGM_ID").alias("GRP_SEL_SUB_AHY_PGM_ID"),
        col("Extract.GRP_SEL_SUB_AHY_ONLY_PGM_ID").alias("GRP_SEL_SUB_AHY_ONLY_PGM_ID"),
        col("Extract.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_NO")
    )
)

# ---------------------
# Stage: Sort
#   SORTSPEC = GRP_ID asc, GRP_AHY_PGM_STRT_DT asc, GRP_AHY_PGM_END_DT asc
# ---------------------
df_Sort = df_Transformer.sort(
    col("GRP_ID").asc(),
    col("GRP_AHY_PGM_STRT_DT").asc(),
    col("GRP_AHY_PGM_END_DT").asc()
)

# ---------------------
# Stage: hf_mbrdm_grpahypgm_dedupe (CHashedFileStage) - Scenario A
#   Input: df_Sort -> Output: Next -> Period_Lkup
#   Key columns: GRP_ID, AHY_PERD_CD
# ---------------------
df_hf_mbrdm_grpahypgm_dedupe = dedup_sort(df_Sort, ["GRP_ID", "AHY_PERD_CD"], [])

# ---------------------
# Stage: Period_Lkup (CTransformerStage)
#   Primary link: df_hf_mbrdm_grpahypgm_dedupe (alias Next)
#   Lookup link: df_hf_mbrdm_grpahypgm_prdcdlkup (alias cdlkup) left join
#   Output link: "Load" -> MBRSH_DM_GRP_AHY_PGM
# ---------------------
df_Period_Lkup = (
    df_hf_mbrdm_grpahypgm_dedupe.alias("Next")
    .join(
        df_hf_mbrdm_grpahypgm_prdcdlkup.alias("cdlkup"),
        col("Next.AHY_PERD_CD") == col("cdlkup.SRC_CD"),
        "left"
    )
    .select(
        col("Next.GRP_ID").alias("GRP_ID"),
        col("Next.GRP_AHY_PGM_STRT_DT").alias("GRP_AHY_PGM_STRT_DT"),
        col("Next.SRC_SYS_CD").alias("SRC_SYS_CD"),
        when(col("cdlkup.TRGT_CD").isNull(), lit("UNK")).otherwise(col("cdlkup.TRGT_CD")).alias("AHY_PERD_CD"),
        col("Next.GRP_AHY_PGM_END_DT").alias("GRP_AHY_PGM_END_DT"),
        col("Next.GRP_AHY_PGM_INCNTV_STRT_DT").alias("GRP_AHY_PGM_INCNTV_STRT_DT"),
        col("Next.GRP_AHY_PGM_INCNTV_END_DT").alias("GRP_AHY_PGM_INCNTV_END_DT"),
        col("Next.GRP_SEL_SPOUSE_AHY_PGM_ID").alias("GRP_SEL_SPOUSE_AHY_PGM_ID"),
        col("Next.GRP_SEL_SUB_AHY_PGM_ID").alias("GRP_SEL_SUB_AHY_PGM_ID"),
        col("Next.GRP_SEL_SUB_AHY_ONLY_PGM_ID").alias("GRP_SEL_SUB_AHY_ONLY_PGM_ID"),
        col("Next.LAST_UPDT_RUN_CYC_NO").alias("LAST_UPDT_RUN_CYC_NO")
    )
)

# Apply rpad for char/varchar columns before final DB load
df_MBRSH_DM_GRP_AHY_PGM = df_Period_Lkup.select(
    rpad(col("GRP_ID"), <...>, " ").alias("GRP_ID"),
    rpad(col("GRP_AHY_PGM_STRT_DT"), 10, " ").alias("GRP_AHY_PGM_STRT_DT"),
    rpad(col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    rpad(col("AHY_PERD_CD"), <...>, " ").alias("AHY_PERD_CD"),
    rpad(col("GRP_AHY_PGM_END_DT"), 10, " ").alias("GRP_AHY_PGM_END_DT"),
    rpad(col("GRP_AHY_PGM_INCNTV_STRT_DT"), 10, " ").alias("GRP_AHY_PGM_INCNTV_STRT_DT"),
    rpad(col("GRP_AHY_PGM_INCNTV_END_DT"), 10, " ").alias("GRP_AHY_PGM_INCNTV_END_DT"),
    rpad(col("GRP_SEL_SPOUSE_AHY_PGM_ID"), <...>, " ").alias("GRP_SEL_SPOUSE_AHY_PGM_ID"),
    rpad(col("GRP_SEL_SUB_AHY_PGM_ID"), <...>, " ").alias("GRP_SEL_SUB_AHY_PGM_ID"),
    rpad(col("GRP_SEL_SUB_AHY_ONLY_PGM_ID"), <...>, " ").alias("GRP_SEL_SUB_AHY_ONLY_PGM_ID"),
    col("LAST_UPDT_RUN_CYC_NO").alias("LAST_UPDT_RUN_CYC_NO")
)

# ---------------------
# Stage: MBRSH_DM_GRP_AHY_PGM (CODBCStage)
#   Merge/Upsert into #$ClmMartOwner#.MBRSH_DM_GRP_AHY_PGM
# ---------------------
jdbc_url_clm, jdbc_props_clm = get_db_config(clmmart_secret_name)
execute_dml(
    f"DROP TABLE IF EXISTS STAGING.IdsMbrGrpAhyPgmExtr_MBRSH_DM_GRP_AHY_PGM_temp",
    jdbc_url_clm,
    jdbc_props_clm
)

(
    df_MBRSH_DM_GRP_AHY_PGM.write
    .format("jdbc")
    .option("url", jdbc_url_clm)
    .options(**jdbc_props_clm)
    .option("dbtable", "STAGING.IdsMbrGrpAhyPgmExtr_MBRSH_DM_GRP_AHY_PGM_temp")
    .mode("overwrite")
    .save()
)

merge_sql = f"""
MERGE INTO {ClmMartOwner}.MBRSH_DM_GRP_AHY_PGM AS Target
USING STAGING.IdsMbrGrpAhyPgmExtr_MBRSH_DM_GRP_AHY_PGM_temp AS Source
ON  Target.GRP_ID = Source.GRP_ID
    AND Target.GRP_AHY_PGM_STRT_DT = Source.GRP_AHY_PGM_STRT_DT
    AND Target.SRC_SYS_CD = Source.SRC_SYS_CD
WHEN MATCHED THEN
    UPDATE SET
        Target.AHY_PERD_CD = Source.AHY_PERD_CD,
        Target.GRP_AHY_PGM_END_DT = Source.GRP_AHY_PGM_END_DT,
        Target.GRP_AHY_PGM_INCNTV_STRT_DT = Source.GRP_AHY_PGM_INCNTV_STRT_DT,
        Target.GRP_AHY_PGM_INCNTV_END_DT = Source.GRP_AHY_PGM_INCNTV_END_DT,
        Target.GRP_SEL_SPOUSE_AHY_PGM_ID = Source.GRP_SEL_SPOUSE_AHY_PGM_ID,
        Target.GRP_SEL_SUB_AHY_PGM_ID = Source.GRP_SEL_SUB_AHY_PGM_ID,
        Target.GRP_SEL_SUB_AHY_ONLY_PGM_ID = Source.GRP_SEL_SUB_AHY_ONLY_PGM_ID,
        Target.LAST_UPDT_RUN_CYC_NO = Source.LAST_UPDT_RUN_CYC_NO
WHEN NOT MATCHED THEN
    INSERT (
        GRP_ID,
        GRP_AHY_PGM_STRT_DT,
        SRC_SYS_CD,
        AHY_PERD_CD,
        GRP_AHY_PGM_END_DT,
        GRP_AHY_PGM_INCNTV_STRT_DT,
        GRP_AHY_PGM_INCNTV_END_DT,
        GRP_SEL_SPOUSE_AHY_PGM_ID,
        GRP_SEL_SUB_AHY_PGM_ID,
        GRP_SEL_SUB_AHY_ONLY_PGM_ID,
        LAST_UPDT_RUN_CYC_NO
    )
    VALUES (
        Source.GRP_ID,
        Source.GRP_AHY_PGM_STRT_DT,
        Source.SRC_SYS_CD,
        Source.AHY_PERD_CD,
        Source.GRP_AHY_PGM_END_DT,
        Source.GRP_AHY_PGM_INCNTV_STRT_DT,
        Source.GRP_AHY_PGM_INCNTV_END_DT,
        Source.GRP_SEL_SPOUSE_AHY_PGM_ID,
        Source.GRP_SEL_SUB_AHY_PGM_ID,
        Source.GRP_SEL_SUB_AHY_ONLY_PGM_ID,
        Source.LAST_UPDT_RUN_CYC_NO
    );
"""
execute_dml(merge_sql, jdbc_url_clm, jdbc_props_clm)