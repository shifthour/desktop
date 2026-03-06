# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """


# MAGIC JobName: IdsEdwGrpCtDExtr
# MAGIC 
# MAGIC This job Extracts Group Count data from IDS and  Creates file for GRP_CT_D table in EDW
# MAGIC Extracts data from GRP_CT table in IDS
# MAGIC Get target Cd and Name details from Cd_Mapping table from IDS
# MAGIC creates load file for GRP_CT_D
# MAGIC Null Handling
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
EDWRunDtCycle = get_widget_value('EDWRunDtCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

df_db2_GRP_CT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        """SELECT 
GRP_CT.GRP_CT_SK,
GRP_CT.GRP_ID,
GRP_CT.GRP_CT_TYP_CD,
GRP_CT.EFF_DT_SK,
COALESCE(CD.TRGT_CD, 'UNK') SRC_SYS_CD,
GRP_CT.CRT_RUN_CYC_EXCTN_SK,
GRP_CT.LAST_UPDT_RUN_CYC_EXCTN_SK,
GRP_CT.GRP_SK,
GRP_CT.GRP_CT_TYP_CD_SK,
GRP_CT.GRP_SIZE_CD_SK,
GRP_CT.TERM_DT_SK,
GRP_CT.GRP_CT
FROM """ + f"{IDSOwner}" + """.GRP_CT GRP_CT
  LEFT JOIN """ + f"{IDSOwner}" + """.CD_MPPNG CD ON GRP_CT.SRC_SYS_CD_SK = CD.CD_MPPNG_SK""",
    )
    .load()
)

df_db2_CD_MPPNG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        """SELECT
CD.CD_MPPNG_SK,
COALESCE(CD.TRGT_CD, 'UNK') TRGT_CD,
CD.TRGT_CD_NM
FROM """ + f"{IDSOwner}" + """.CD_MPPNG CD"""
    )
    .load()
)

df_lnk_GrpSizeSk_In = df_db2_CD_MPPNG.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD_GrpSizeCd"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM_GrpSizeNm"),
)

df_lnk_GrpCtTypCdSk_In = df_db2_CD_MPPNG.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD_GrpCtTypeCd"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM_GrpCtTypeCdNm"),
)

df_lkp_Cd_Mppng = (
    df_db2_GRP_CT.alias("lnk_GRP_CT_In")
    .join(
        df_lnk_GrpSizeSk_In.alias("lnk_GrpSizeSk_In"),
        F.col("lnk_GRP_CT_In.GRP_SIZE_CD_SK") == F.col("lnk_GrpSizeSk_In.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_lnk_GrpCtTypCdSk_In.alias("lnk_GrpCtTypCdSk_In"),
        F.col("lnk_GRP_CT_In.GRP_CT_TYP_CD_SK")
        == F.col("lnk_GrpCtTypCdSk_In.CD_MPPNG_SK"),
        "left",
    )
    .select(
        F.col("lnk_GRP_CT_In.GRP_CT_SK").alias("GRP_CT_SK"),
        F.col("lnk_GRP_CT_In.GRP_ID").alias("GRP_ID"),
        F.col("lnk_GRP_CT_In.GRP_CT_TYP_CD").alias("GRP_CT_TYP_CD"),
        F.col("lnk_GRP_CT_In.EFF_DT_SK").alias("EFF_DT_SK"),
        F.col("lnk_GRP_CT_In.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnk_GRP_CT_In.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("lnk_GRP_CT_In.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("lnk_GRP_CT_In.GRP_SK").alias("GRP_SK"),
        F.col("lnk_GRP_CT_In.GRP_CT_TYP_CD_SK").alias("GRP_CT_TYP_CD_SK"),
        F.col("lnk_GRP_CT_In.GRP_SIZE_CD_SK").alias("GRP_SIZE_CD_SK"),
        F.col("lnk_GRP_CT_In.TERM_DT_SK").alias("TERM_DT_SK"),
        F.col("lnk_GRP_CT_In.GRP_CT").alias("GRP_CT"),
        F.col("lnk_GrpSizeSk_In.TRGT_CD_NM_GrpSizeNm").alias("TRGT_CD_NM_GrpSizeNm"),
        F.col("lnk_GrpSizeSk_In.TRGT_CD_GrpSizeCd").alias("TRGT_CD_GrpSizeCd"),
        F.col("lnk_GrpCtTypCdSk_In.TRGT_CD_GrpCtTypeCd").alias("TRGT_CD_GrpCtTypeCd"),
        F.col("lnk_GrpCtTypCdSk_In.TRGT_CD_NM_GrpCtTypeCdNm").alias("TRGT_CD_NM_GrpCtTypeCdNm"),
    )
)

df_xmf_BusinessLogic = (
    df_lkp_Cd_Mppng
    .withColumn("ORIG_LAST_UPDT_RUN_CYC_EXCTN_SK", F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"))
    .withColumn(
        "SRC_SYS_CD",
        F.when(
            F.col("SRC_SYS_CD").isNull() | (F.col("SRC_SYS_CD") == ""),
            F.lit("UNK"),
        ).otherwise(F.col("SRC_SYS_CD")),
    )
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.lit(EDWRunDtCycle))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.lit(EDWRunDtCycle))
    .withColumn(
        "GRP_CT_TYP_NM",
        F.when(
            F.col("TRGT_CD_NM_GrpCtTypeCdNm").isNull()
            | (F.col("TRGT_CD_NM_GrpCtTypeCdNm") == ""),
            F.lit("UNK"),
        ).otherwise(F.col("TRGT_CD_NM_GrpCtTypeCdNm")),
    )
    .withColumn(
        "GRP_SIZE_CD",
        F.when(
            F.col("TRGT_CD_GrpSizeCd").isNull()
            | (F.col("TRGT_CD_GrpSizeCd") == ""),
            F.lit("UNK"),
        ).otherwise(F.col("TRGT_CD_GrpSizeCd")),
    )
    .withColumn(
        "GRP_SIZE_NM",
        F.when(
            F.col("TRGT_CD_NM_GrpSizeNm").isNull()
            | (F.col("TRGT_CD_NM_GrpSizeNm") == ""),
            F.lit("UNK"),
        ).otherwise(F.col("TRGT_CD_NM_GrpSizeNm")),
    )
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(EDWRunCycle))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(EDWRunCycle))
)

df_seq_GRP_CT_D = df_xmf_BusinessLogic.select(
    F.col("GRP_CT_SK"),
    F.col("GRP_ID"),
    F.col("GRP_CT_TYP_CD"),
    F.rpad(F.col("EFF_DT_SK"), 10, " ").alias("EFF_DT_SK"),
    F.col("SRC_SYS_CD"),
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("GRP_SK"),
    F.col("GRP_CT_TYP_NM"),
    F.col("GRP_SIZE_CD"),
    F.col("GRP_SIZE_NM"),
    F.rpad(F.col("TERM_DT_SK"), 10, " ").alias("TERM_DT_SK"),
    F.col("GRP_CT"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("ORIG_LAST_UPDT_RUN_CYC_EXCTN_SK").alias("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("GRP_CT_TYP_CD_SK"),
    F.col("GRP_SIZE_CD_SK"),
)

write_files(
    df_seq_GRP_CT_D,
    f"{adls_path}/load/GRP_CT_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)