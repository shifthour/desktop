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
# MAGIC                                                  Project/                                                                                                                                                                   Code                   Date
# MAGIC Developer           Date              Altiris #                             Change Description                                                                         Environment            Reviewer            Reviewed
# MAGIC -------------------------  -------------------   -------------                           ----------------------------------------------------------------------------------------------------    --------------------------   -------------------------  -----------------
# MAGIC Landon Hall        01/05/2009   Q3 TTR                          Original program                                                                                                              Steph Goddard   02/10/2009
# MAGIC 
# MAGIC Kimberly Doty      04-30-2010    4044 Blue Renew v2      Added 14 new fields after PRM_RATE_PRM_TYP_CD_SK;          EnterpriseCurDevl  Steph Goddard   05/19/2010
# MAGIC                                                                                                                                          Brought up to current standards 
# MAGIC Lee Moore       8/30/2013       5114                             Rewrite in parallel                                                                                EnterpriseWrhsDevl  Peter Marshall  11/26/2013

# MAGIC Code SK lookups for Denormalization
# MAGIC Write PRM_RATE   Data into a Sequential file for Load Job IdsEdwPrmRateDLoad.
# MAGIC Read all the Data from IDS PRM_RATE Table;
# MAGIC Add Defaults and Null Handling.
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Job Name: IdsEdwPrmRateDExtr
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query_db2_PRM_RATE_in = """SELECT 
PRM_RATE_SK,
SRC_SYS_CD_SK,
PRM_RATE_UNIQ_KEY,
PRM_RATE_AGE_ENTY_CD_SK,
PRM_RATE_DESC,
EFF_DT_SK,
PRM_RATE_MOD_ID,
PRM_RATE_PFX,
PRM_RATE_PRM_TYP_CD_SK,
TERM_DT_SK,
CRT_RUN_CYC_EXCTN_SK,
MAX_CHLDRN_RATED_NO,
PRM_RATE_BSS_NO,
PRM_RATE_METH_CD_SK,
PRM_RATE_KEY_STRCT_NO,
PRM_RATE_TIER_CLMN_STRCT_CD_SK,
PRM_RATE_TIER_MOD_TYP_CD_SK,
TIER_AGE_BAND_FROM_NO,
TIER_AGE_BAND_THRU_NO
FROM {}.PRM_RATE
""".format(IDSOwner)

df_db2_PRM_RATE_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_PRM_RATE_in)
    .load()
)

extract_query_db2_CD_MPPNG_in = """SELECT 
CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') AS TRGT_CD,
COALESCE(TRGT_CD_NM,'UNK') AS TRGT_CD_NM
FROM {}.CD_MPPNG
""".format(IDSOwner)

df_db2_CD_MPPNG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CD_MPPNG_in)
    .load()
)

df_cpy_cd_mppng = df_db2_CD_MPPNG_in

df_lkp_Codes = (
    df_db2_PRM_RATE_in.alias("lnk_IdsEdwPrmRateDExtr_InABC")
    .join(
        df_cpy_cd_mppng.alias("lnk_PrmRateTypLkup"),
        F.col("lnk_IdsEdwPrmRateDExtr_InABC.SRC_SYS_CD_SK") == F.col("lnk_PrmRateTypLkup.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_cpy_cd_mppng.alias("lnk_PrmRateTierClmnStrctCdLkup"),
        F.col("lnk_IdsEdwPrmRateDExtr_InABC.PRM_RATE_AGE_ENTY_CD_SK") == F.col("lnk_PrmRateTierClmnStrctCdLkup.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_cpy_cd_mppng.alias("lnk_PrmRateTierModTypCdLkup"),
        F.col("lnk_IdsEdwPrmRateDExtr_InABC.PRM_RATE_PRM_TYP_CD_SK") == F.col("lnk_PrmRateTierModTypCdLkup.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_cpy_cd_mppng.alias("lnk_SrcSysLkup"),
        F.col("lnk_IdsEdwPrmRateDExtr_InABC.PRM_RATE_METH_CD_SK") == F.col("lnk_SrcSysLkup.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_cpy_cd_mppng.alias("lnk_PrmRateMethCdLkup"),
        F.col("lnk_IdsEdwPrmRateDExtr_InABC.PRM_RATE_TIER_CLMN_STRCT_CD_SK") == F.col("lnk_PrmRateMethCdLkup.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_cpy_cd_mppng.alias("lnk_PrmRateAgeLkup"),
        F.col("lnk_IdsEdwPrmRateDExtr_InABC.PRM_RATE_TIER_MOD_TYP_CD_SK") == F.col("lnk_PrmRateAgeLkup.CD_MPPNG_SK"),
        "left",
    )
    .select(
        F.col("lnk_IdsEdwPrmRateDExtr_InABC.PRM_RATE_SK").alias("PRM_RATE_SK"),
        F.col("lnk_IdsEdwPrmRateDExtr_InABC.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("lnk_IdsEdwPrmRateDExtr_InABC.PRM_RATE_UNIQ_KEY").alias("PRM_RATE_UNIQ_KEY"),
        F.col("lnk_IdsEdwPrmRateDExtr_InABC.PRM_RATE_AGE_ENTY_CD_SK").alias("PRM_RATE_AGE_ENTY_CD_SK"),
        F.col("lnk_IdsEdwPrmRateDExtr_InABC.PRM_RATE_DESC").alias("PRM_RATE_DESC"),
        F.col("lnk_IdsEdwPrmRateDExtr_InABC.EFF_DT_SK").alias("EFF_DT_SK"),
        F.col("lnk_IdsEdwPrmRateDExtr_InABC.PRM_RATE_MOD_ID").alias("PRM_RATE_MOD_ID"),
        F.col("lnk_IdsEdwPrmRateDExtr_InABC.PRM_RATE_PFX").alias("PRM_RATE_PFX"),
        F.col("lnk_IdsEdwPrmRateDExtr_InABC.PRM_RATE_PRM_TYP_CD_SK").alias("PRM_RATE_PRM_TYP_CD_SK"),
        F.col("lnk_IdsEdwPrmRateDExtr_InABC.TERM_DT_SK").alias("TERM_DT_SK"),
        F.col("lnk_IdsEdwPrmRateDExtr_InABC.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("lnk_IdsEdwPrmRateDExtr_InABC.MAX_CHLDRN_RATED_NO").alias("MAX_CHLDRN_RATED_NO"),
        F.col("lnk_IdsEdwPrmRateDExtr_InABC.PRM_RATE_BSS_NO").alias("PRM_RATE_BSS_NO"),
        F.col("lnk_IdsEdwPrmRateDExtr_InABC.PRM_RATE_METH_CD_SK").alias("PRM_RATE_METH_CD_SK"),
        F.col("lnk_IdsEdwPrmRateDExtr_InABC.PRM_RATE_KEY_STRCT_NO").alias("PRM_RATE_KEY_STRCT_NO"),
        F.col("lnk_IdsEdwPrmRateDExtr_InABC.PRM_RATE_TIER_CLMN_STRCT_CD_SK").alias("PRM_RATE_TIER_CLMN_STRCT_CD_SK"),
        F.col("lnk_IdsEdwPrmRateDExtr_InABC.PRM_RATE_TIER_MOD_TYP_CD_SK").alias("PRM_RATE_TIER_MOD_TYP_CD_SK"),
        F.col("lnk_IdsEdwPrmRateDExtr_InABC.TIER_AGE_BAND_FROM_NO").alias("TIER_AGE_BAND_FROM_NO"),
        F.col("lnk_IdsEdwPrmRateDExtr_InABC.TIER_AGE_BAND_THRU_NO").alias("TIER_AGE_BAND_THRU_NO"),
        F.col("lnk_PrmRateTypLkup.TRGT_CD").alias("SRC_SYS_CD"),
        F.col("lnk_PrmRateTierClmnStrctCdLkup.TRGT_CD").alias("RATE_AGE_ENTY_CD"),
        F.col("lnk_PrmRateTierClmnStrctCdLkup.TRGT_CD_NM").alias("RATE_AGE_ENTY_NM"),
        F.col("lnk_PrmRateTierModTypCdLkup.TRGT_CD").alias("RATE_PRM_TYP_CD"),
        F.col("lnk_PrmRateTierModTypCdLkup.TRGT_CD_NM").alias("RATE_PRM_TYP_NM"),
        F.col("lnk_SrcSysLkup.TRGT_CD").alias("RATE_METH_CD"),
        F.col("lnk_SrcSysLkup.TRGT_CD_NM").alias("RATE_METH_NM"),
        F.col("lnk_PrmRateMethCdLkup.TRGT_CD").alias("TEIR_CLMN_CD"),
        F.col("lnk_PrmRateMethCdLkup.TRGT_CD_NM").alias("TIER_CLMN_NM"),
        F.col("lnk_PrmRateAgeLkup.TRGT_CD").alias("TIER_MOD_CD"),
        F.col("lnk_PrmRateAgeLkup.TRGT_CD_NM").alias("TIER_MOD_NM"),
    )
)

df_xfrm_BusinessLogic_main = (
    df_lkp_Codes.filter((F.col("PRM_RATE_SK") != 0) & (F.col("PRM_RATE_SK") != 1))
    .select(
        F.col("PRM_RATE_SK").alias("PRM_RATE_SK"),
        F.when(
            (F.length(trim(F.col("SRC_SYS_CD"))) == 0),
            F.lit("UNK"),
        ).otherwise(
            F.when(F.col("SRC_SYS_CD_SK").isNull(), F.lit("UNK")).otherwise(F.col("SRC_SYS_CD"))
        ).alias("SRC_SYS_CD"),
        F.col("PRM_RATE_UNIQ_KEY").alias("PRM_RATE_UNIQ_KEY"),
        F.rpad(F.lit(EDWRunCycleDate), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.rpad(F.lit(EDWRunCycleDate), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.when(
            (F.length(trim(F.col("RATE_AGE_ENTY_CD"))) == 0),
            F.lit("UNK"),
        ).otherwise(
            F.when(F.col("RATE_AGE_ENTY_CD").isNull(), F.lit("UNK")).otherwise(F.col("RATE_AGE_ENTY_CD"))
        ).alias("PRM_RATE_AGE_ENTY_CD"),
        F.when(
            (F.length(trim(F.col("RATE_AGE_ENTY_NM"))) == 0),
            F.lit("UNK"),
        ).otherwise(
            F.when(F.col("RATE_AGE_ENTY_NM").isNull(), F.lit("UNK")).otherwise(F.col("RATE_AGE_ENTY_NM"))
        ).alias("PRM_RATE_AGE_ENTY_NM"),
        F.col("PRM_RATE_DESC").alias("PRM_RATE_DESC"),
        F.rpad(F.col("EFF_DT_SK"), 10, " ").alias("PRM_RATE_EFF_DT_SK"),
        F.col("PRM_RATE_MOD_ID").alias("PRM_RATE_MOD_ID"),
        F.col("PRM_RATE_PFX").alias("PRM_RATE_PFX"),
        F.when(
            (F.length(trim(F.col("RATE_PRM_TYP_CD"))) == 0),
            F.lit("UNK"),
        ).otherwise(
            F.when(F.col("RATE_PRM_TYP_CD").isNull(), F.lit("UNK")).otherwise(F.col("RATE_PRM_TYP_CD"))
        ).alias("PRM_RATE_PRM_TYP_CD"),
        F.when(
            (F.length(trim(F.col("RATE_PRM_TYP_NM"))) == 0),
            F.lit("UNK"),
        ).otherwise(
            F.when(F.col("RATE_PRM_TYP_NM").isNull(), F.lit("UNK")).otherwise(F.col("RATE_PRM_TYP_NM"))
        ).alias("PRM_RATE_PRM_TYP_NM"),
        F.rpad(F.col("TERM_DT_SK"), 10, " ").alias("PRM_RATE_TERM_DT_SK"),
        F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("PRM_RATE_AGE_ENTY_CD_SK").alias("PRM_RATE_AGE_ENTY_CD_SK"),
        F.col("PRM_RATE_PRM_TYP_CD_SK").alias("PRM_RATE_PRM_TYP_CD_SK"),
        F.col("MAX_CHLDRN_RATED_NO").alias("PRM_RATE_MAX_CHLDRN_RATED_NO"),
        F.col("PRM_RATE_BSS_NO").alias("PRM_RATE_BSS_NO"),
        F.when(
            (F.length(trim(F.col("RATE_METH_CD"))) == 0),
            F.lit("UNK"),
        ).otherwise(
            F.when(F.col("RATE_METH_CD").isNull(), F.lit("UNK")).otherwise(F.col("RATE_METH_CD"))
        ).alias("PRM_RATE_METH_CD"),
        F.when(
            (F.length(trim(F.col("RATE_METH_NM"))) == 0),
            F.lit("UNK"),
        ).otherwise(
            F.when(F.col("RATE_METH_NM").isNull(), F.lit("UNK")).otherwise(F.col("RATE_METH_NM"))
        ).alias("PRM_RATE_METH_NM"),
        F.col("PRM_RATE_KEY_STRCT_NO").alias("PRM_RATE_KEY_STRCT_NO"),
        F.when(
            (F.length(trim(F.col("TEIR_CLMN_CD"))) == 0),
            F.lit("UNK"),
        ).otherwise(
            F.when(F.col("TEIR_CLMN_CD").isNull(), F.lit("UNK")).otherwise(F.col("TEIR_CLMN_CD"))
        ).alias("PRM_RATE_TIER_CLMN_STRCT_CD"),
        F.when(
            (F.length(trim(F.col("TIER_CLMN_NM"))) == 0),
            F.lit("UNK"),
        ).otherwise(
            F.when(F.col("TIER_CLMN_NM").isNull(), F.lit("UNK")).otherwise(F.col("TIER_CLMN_NM"))
        ).alias("PRM_RATE_TIER_CLMN_STRCT_NM"),
        F.when(
            (F.length(trim(F.col("TIER_MOD_CD"))) == 0),
            F.lit("UNK"),
        ).otherwise(
            F.when(F.col("TIER_MOD_CD").isNull(), F.lit("UNK")).otherwise(F.col("TIER_MOD_CD"))
        ).alias("PRM_RATE_TIER_MOD_TYP_CD"),
        F.when(
            (F.length(trim(F.col("TIER_MOD_NM"))) == 0),
            F.lit("UNK"),
        ).otherwise(
            F.when(F.col("TIER_MOD_NM").isNull(), F.lit("UNK")).otherwise(F.col("TIER_MOD_NM"))
        ).alias("PRM_RATE_TIER_MOD_TYP_NM"),
        F.col("TIER_AGE_BAND_FROM_NO").alias("PRM_RATE_TIER_AGE_BAND_FROM_NO"),
        F.col("TIER_AGE_BAND_THRU_NO").alias("PRM_RATE_TIER_AGE_BAND_THRU_NO"),
        F.col("PRM_RATE_METH_CD_SK").alias("PRM_RATE_METH_CD_SK"),
        F.col("PRM_RATE_TIER_CLMN_STRCT_CD_SK").alias("PRM_RATE_TIER_CLMN_STRCT_CD_SK"),
        F.col("PRM_RATE_TIER_MOD_TYP_CD_SK").alias("PRM_RATE_TIER_MOD_TYP_CD_SK"),
    )
)

df_xfrm_BusinessLogic_unk = spark.createDataFrame(
    [
        (
            0, 'UNK', 0, '1753-01-01', '1753-01-01', 'UNK', 'UNK', ' ', '1753-01-01',
            'UNK', ' ', 'UNK', 'UNK', '1753-01-01', 100, 100, 0, 0, 0, 0, 'UNK',
            'UNK', 0, 'UNK', 'UNK', 'UNK', 'UNK', 0, 0, 0, 0, 0
        )
    ],
    [
        "PRM_RATE_SK","SRC_SYS_CD","PRM_RATE_UNIQ_KEY","CRT_RUN_CYC_EXCTN_DT_SK","LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "PRM_RATE_AGE_ENTY_CD","PRM_RATE_AGE_ENTY_NM","PRM_RATE_DESC","PRM_RATE_EFF_DT_SK","PRM_RATE_MOD_ID",
        "PRM_RATE_PFX","PRM_RATE_PRM_TYP_CD","PRM_RATE_PRM_TYP_NM","PRM_RATE_TERM_DT_SK","CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK","PRM_RATE_AGE_ENTY_CD_SK","PRM_RATE_PRM_TYP_CD_SK","PRM_RATE_MAX_CHLDRN_RATED_NO",
        "PRM_RATE_BSS_NO","PRM_RATE_METH_CD","PRM_RATE_METH_NM","PRM_RATE_KEY_STRCT_NO","PRM_RATE_TIER_CLMN_STRCT_CD",
        "PRM_RATE_TIER_CLMN_STRCT_NM","PRM_RATE_TIER_MOD_TYP_CD","PRM_RATE_TIER_MOD_TYP_NM","PRM_RATE_TIER_AGE_BAND_FROM_NO",
        "PRM_RATE_TIER_AGE_BAND_THRU_NO","PRM_RATE_METH_CD_SK","PRM_RATE_TIER_CLMN_STRCT_CD_SK","PRM_RATE_TIER_MOD_TYP_CD_SK"
    ]
)

df_xfrm_BusinessLogic_na = spark.createDataFrame(
    [
        (
            1, 'NA', 1, '1753-01-01', '1753-01-01', 'NA', 'NA', ' ', '1753-01-01',
            'NA', ' ', 'NA', 'NA', '1753-01-01', 100, 100, 1, 1, 0, 0, 'NA',
            'NA', 0, 'NA', 'NA', 'NA', 'NA', 0, 0, 1, 1, 1
        )
    ],
    [
        "PRM_RATE_SK","SRC_SYS_CD","PRM_RATE_UNIQ_KEY","CRT_RUN_CYC_EXCTN_DT_SK","LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "PRM_RATE_AGE_ENTY_CD","PRM_RATE_AGE_ENTY_NM","PRM_RATE_DESC","PRM_RATE_EFF_DT_SK","PRM_RATE_MOD_ID",
        "PRM_RATE_PFX","PRM_RATE_PRM_TYP_CD","PRM_RATE_PRM_TYP_NM","PRM_RATE_TERM_DT_SK","CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK","PRM_RATE_AGE_ENTY_CD_SK","PRM_RATE_PRM_TYP_CD_SK","PRM_RATE_MAX_CHLDRN_RATED_NO",
        "PRM_RATE_BSS_NO","PRM_RATE_METH_CD","PRM_RATE_METH_NM","PRM_RATE_KEY_STRCT_NO","PRM_RATE_TIER_CLMN_STRCT_CD",
        "PRM_RATE_TIER_CLMN_STRCT_NM","PRM_RATE_TIER_MOD_TYP_CD","PRM_RATE_TIER_MOD_TYP_NM","PRM_RATE_TIER_AGE_BAND_FROM_NO",
        "PRM_RATE_TIER_AGE_BAND_THRU_NO","PRM_RATE_METH_CD_SK","PRM_RATE_TIER_CLMN_STRCT_CD_SK","PRM_RATE_TIER_MOD_TYP_CD_SK"
    ]
)

df_fnl_NA_UNK = df_xfrm_BusinessLogic_main.union(df_xfrm_BusinessLogic_unk).union(df_xfrm_BusinessLogic_na)

df_final = df_fnl_NA_UNK.select(
    F.col("PRM_RATE_SK"),
    F.col("SRC_SYS_CD"),
    F.col("PRM_RATE_UNIQ_KEY"),
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("PRM_RATE_AGE_ENTY_CD"),
    F.col("PRM_RATE_AGE_ENTY_NM"),
    F.col("PRM_RATE_DESC"),
    F.rpad(F.col("PRM_RATE_EFF_DT_SK"), 10, " ").alias("PRM_RATE_EFF_DT_SK"),
    F.col("PRM_RATE_MOD_ID"),
    F.col("PRM_RATE_PFX"),
    F.col("PRM_RATE_PRM_TYP_CD"),
    F.col("PRM_RATE_PRM_TYP_NM"),
    F.rpad(F.col("PRM_RATE_TERM_DT_SK"), 10, " ").alias("PRM_RATE_TERM_DT_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("PRM_RATE_AGE_ENTY_CD_SK"),
    F.col("PRM_RATE_PRM_TYP_CD_SK"),
    F.col("PRM_RATE_MAX_CHLDRN_RATED_NO"),
    F.col("PRM_RATE_BSS_NO"),
    F.col("PRM_RATE_METH_CD"),
    F.col("PRM_RATE_METH_NM"),
    F.col("PRM_RATE_KEY_STRCT_NO"),
    F.col("PRM_RATE_TIER_CLMN_STRCT_CD"),
    F.col("PRM_RATE_TIER_CLMN_STRCT_NM"),
    F.col("PRM_RATE_TIER_MOD_TYP_CD"),
    F.col("PRM_RATE_TIER_MOD_TYP_NM"),
    F.col("PRM_RATE_TIER_AGE_BAND_FROM_NO"),
    F.col("PRM_RATE_TIER_AGE_BAND_THRU_NO"),
    F.col("PRM_RATE_METH_CD_SK"),
    F.col("PRM_RATE_TIER_CLMN_STRCT_CD_SK"),
    F.col("PRM_RATE_TIER_MOD_TYP_CD_SK")
)

write_files(
    df_final,
    f"{adls_path}/load/PRM_RATE_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)