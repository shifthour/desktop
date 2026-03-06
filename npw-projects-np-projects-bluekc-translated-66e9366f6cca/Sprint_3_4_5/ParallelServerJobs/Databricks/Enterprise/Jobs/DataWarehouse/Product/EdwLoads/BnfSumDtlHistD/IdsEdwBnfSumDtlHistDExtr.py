# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC CALLED BY: IdsEdwProductExtr1Seq
# MAGIC 
# MAGIC PROCESSING: This Job extracts Data from IDS and creates a Load file for BNF_SUM_DTL_HIST_D
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                               Date               Project/Altiris #               Change Description                     Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------          -------------------     -------------------------------    -------------------------------------------------         ----------------------------------   ---------------------------------    -------------------------   
# MAGIC 
# MAGIC HariKrishnaRao Yadav   09/30/2021            US 433516               Original Programming                           EnterpriseSITF            Jeyaprasanna              2021-11-05

# MAGIC This is Source extract data from an IDS table and apply Code denormalization, create a load ready file.
# MAGIC 
# MAGIC Job Name:
# MAGIC IdsEdwBnfSumDtlHistDExtr
# MAGIC 
# MAGIC Table:
# MAGIC BNF_SUM_DTL_HIST_D
# MAGIC Read from source table BNF_SUM_DTL_HIST from IDS ;
# MAGIC Pull records based on LAST_UPDT_RUN_CYC_EXCTN_SK
# MAGIC This is a load ready file conforms to the target table structure.
# MAGIC 
# MAGIC Write BNF_SUM_DTL_HIST_D Data into a Sequential file for Load Job.
# MAGIC Add Defaults and Null Handling
# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC 
# MAGIC BNF_SUM_DTL_ACCUM_LVL_CD_SK
# MAGIC BNF_SUM_DTL_NTWK_TYP_CD_SK
# MAGIC BNF_SUM_DTL_COV_CD_SK
# MAGIC BNF_SUM_DTL_LMT_CTR_TYP_CD_SK
# MAGIC BNF_SUM_DTL_LMT_PERD_TYP_CD_SK
# MAGIC BNF_SUM_DTL_LMT_TYP_CD_SK
# MAGIC BNF_SUM_DTL_TYP_CD_SK
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query_db2_BNF_SUM_DTL_HIST_in = """SELECT
BNF_SUM_DTL_HIST_SK,
PROD_CMPNT_PFX_ID,
BNF_SUM_DTL_TYP_CD,
BNF_SUM_DTL_NTWK_TYP_CD,
BNF_SUM_DTL_EFF_DT_SK,
SRC_SYS_CD,
CRT_RUN_CYC_EXCTN_SK,
LAST_UPDT_RUN_CYC_EXCTN_SK,
BNF_SUM_DTL_ACCUM_LVL_CD_SK,
BNF_SUM_DTL_COV_CD_SK,
BNF_SUM_DTL_NTWK_TYP_CD_SK,
BNF_SUM_DTL_LMT_CTR_TYP_CD_SK,
BNF_SUM_DTL_LMT_PERD_TYP_CD_SK,
BNF_SUM_DTL_LMT_TYP_CD_SK,
BNF_SUM_DTL_TYP_CD_SK,
BNF_SUM_DTL_TERM_DT_SK,
BNF_SUM_DTL_COINS_PCT_AMT,
BNF_SUM_DTL_COPAY_AMT,
BNF_SUM_DTL_DEDCT_AMT,
BNF_SUM_DTL_LMT_AMT,
BNF_SUM_DTL_STOPLOSS_AMT,
BNF_SUM_DTL_TIER_NO,
BNF_SUM_DTL_PLN_YR_BEG_MO_DAY,
BNF_SUM_DTL_USER_FLD_LABEL_1_TX,
BNF_SUM_DTL_USER_FLD_DATA_1_TX,
BNF_SUM_DTL_USER_FLD_LABEL_2_TX,
BNF_SUM_DTL_USER_FLD_DATA_2_TX,
BNF_SUM_DTL_USER_FLD_LABEL_3_TX,
BNF_SUM_DTL_USER_FLD_DATA_3_TX,
BNF_SUM_DTL_USER_FLD_LABEL_4_TX,
BNF_SUM_DTL_USER_FLD_DATA_4_TX,
BNF_SUM_DTL_USER_FLD_LABEL_5_TX,
BNF_SUM_DTL_USER_FLD_DATA_5_TX,
BNF_SUM_DTL_USER_FLD_LABEL_6_TX,
BNF_SUM_DTL_USER_FLD_DATA_6_TX,
SRC_SYS_LAST_UPDT_USER_ID,
SRC_SYS_LAST_UPDT_DTM
FROM {}.BNF_SUM_DTL_HIST
""".format(IDSOwner)

df_db2_BNF_SUM_DTL_HIST_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_BNF_SUM_DTL_HIST_in)
    .load()
)

extract_query_db2_CD_MPPNG_Extr = """SELECT
CD_MPPNG_SK,
COALESCE(SRC_CD,'UNK') SRC_CD,
COALESCE(SRC_CD_NM,'UNK') SRC_CD_NM
FROM {}.CD_MPPNG
""".format(IDSOwner)

df_db2_CD_MPPNG_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CD_MPPNG_Extr)
    .load()
)

df_Cpy_CD_SK = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK"),
    F.col("SRC_CD"),
    F.col("SRC_CD_NM")
)

df_lkp_Codes_primary = df_db2_BNF_SUM_DTL_HIST_in.alias("lnk_IdsEdwBnfSumDtlHistDExtr_InABC")

df_Cpy_CD_SK_accum_lvl = df_Cpy_CD_SK.alias("lnk_BNF_SUM_DTL_ACCUM_LVL_CD_SK").select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK_BNF_SUM_DTL_ACCUM_LVL_CD_SK"),
    F.col("SRC_CD").alias("SRC_CD_BNF_SUM_DTL_ACCUM_LVL_CD_SK"),
    F.col("SRC_CD_NM").alias("SRC_CD_NM_BNF_SUM_DTL_ACCUM_LVL_CD_SK")
)
df_Cpy_CD_SK_cov_cd = df_Cpy_CD_SK.alias("lnk_BNF_SUM_DTL_COV_CD_SK").select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK_BNF_SUM_DTL_COV_CD_SK"),
    F.col("SRC_CD").alias("SRC_CD_BNF_SUM_DTL_COV_CD_SK"),
    F.col("SRC_CD_NM").alias("SRC_CD_NM_BNF_SUM_DTL_COV_CD_SK")
)
df_Cpy_CD_SK_lmt_ctr = df_Cpy_CD_SK.alias("lnk_BNF_SUM_DTL_LMT_CTR_TYP_CD_SK").select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK_BNF_SUM_DTL_LMT_CTR_TYP_CD_SK"),
    F.col("SRC_CD").alias("SRC_CD_BNF_SUM_DTL_LMT_CTR_TYP_CD_SK"),
    F.col("SRC_CD_NM").alias("SRC_CD_NM_BNF_SUM_DTL_LMT_CTR_TYP_CD_SK")
)
df_Cpy_CD_SK_lmt_perd = df_Cpy_CD_SK.alias("lnk_BNF_SUM_DTL_LMT_PERD_TYP_CD_SK").select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK_BNF_SUM_DTL_LMT_PERD_TYP_CD_SK"),
    F.col("SRC_CD").alias("SRC_CD_BNF_SUM_DTL_LMT_PERD_TYP_CD_SK"),
    F.col("SRC_CD_NM").alias("SRC_CD_NM_BNF_SUM_DTL_LMT_PERD_TYP_CD_SK")
)
df_Cpy_CD_SK_lmt_typ = df_Cpy_CD_SK.alias("lnk_BNF_SUM_DTL_LMT_TYP_CD_SK").select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK_BNF_SUM_DTL_LMT_TYP_CD_SK"),
    F.col("SRC_CD").alias("SRC_CD_BNF_SUM_DTL_LMT_TYP_CD_SK"),
    F.col("SRC_CD_NM").alias("SRC_CD_NM_BNF_SUM_DTL_LMT_TYP_CD_SK")
)
df_Cpy_CD_SK_ntwk_typ = df_Cpy_CD_SK.alias("lnk_BNF_SUM_DTL_NTWK_TYP_CD_SK").select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK_BNF_SUM_DTL_NTWK_TYP_CD_SK"),
    F.col("SRC_CD_NM").alias("SRC_CD_NM_BNF_SUM_DTL_NTWK_TYP_CD_SK")
)
df_Cpy_CD_SK_typ_cd = df_Cpy_CD_SK.alias("lnk_BNF_SUM_DTL_TYP_CD_SK").select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK_BNF_SUM_DTL_TYP_CD_SK"),
    F.col("SRC_CD_NM").alias("SRC_CD_NM_BNF_SUM_DTL_TYP_CD_SK")
)

df_lkp_Codes = (
    df_lkp_Codes_primary
    .join(
        df_Cpy_CD_SK_accum_lvl,
        F.col("lnk_IdsEdwBnfSumDtlHistDExtr_InABC.BNF_SUM_DTL_ACCUM_LVL_CD_SK")
        == F.col("lnk_BNF_SUM_DTL_ACCUM_LVL_CD_SK.CD_MPPNG_SK_BNF_SUM_DTL_ACCUM_LVL_CD_SK"),
        "left"
    )
    .join(
        df_Cpy_CD_SK_cov_cd,
        F.col("lnk_IdsEdwBnfSumDtlHistDExtr_InABC.BNF_SUM_DTL_COV_CD_SK")
        == F.col("lnk_BNF_SUM_DTL_COV_CD_SK.CD_MPPNG_SK_BNF_SUM_DTL_COV_CD_SK"),
        "left"
    )
    .join(
        df_Cpy_CD_SK_lmt_ctr,
        F.col("lnk_IdsEdwBnfSumDtlHistDExtr_InABC.BNF_SUM_DTL_LMT_CTR_TYP_CD_SK")
        == F.col("lnk_BNF_SUM_DTL_LMT_CTR_TYP_CD_SK.CD_MPPNG_SK_BNF_SUM_DTL_LMT_CTR_TYP_CD_SK"),
        "left"
    )
    .join(
        df_Cpy_CD_SK_lmt_perd,
        F.col("lnk_IdsEdwBnfSumDtlHistDExtr_InABC.BNF_SUM_DTL_LMT_PERD_TYP_CD_SK")
        == F.col("lnk_BNF_SUM_DTL_LMT_PERD_TYP_CD_SK.CD_MPPNG_SK_BNF_SUM_DTL_LMT_PERD_TYP_CD_SK"),
        "left"
    )
    .join(
        df_Cpy_CD_SK_lmt_typ,
        F.col("lnk_IdsEdwBnfSumDtlHistDExtr_InABC.BNF_SUM_DTL_LMT_TYP_CD_SK")
        == F.col("lnk_BNF_SUM_DTL_LMT_TYP_CD_SK.CD_MPPNG_SK_BNF_SUM_DTL_LMT_TYP_CD_SK"),
        "left"
    )
    .join(
        df_Cpy_CD_SK_ntwk_typ,
        F.col("lnk_IdsEdwBnfSumDtlHistDExtr_InABC.BNF_SUM_DTL_NTWK_TYP_CD_SK")
        == F.col("lnk_BNF_SUM_DTL_NTWK_TYP_CD_SK.CD_MPPNG_SK_BNF_SUM_DTL_NTWK_TYP_CD_SK"),
        "left"
    )
    .join(
        df_Cpy_CD_SK_typ_cd,
        F.col("lnk_IdsEdwBnfSumDtlHistDExtr_InABC.BNF_SUM_DTL_TYP_CD_SK")
        == F.col("lnk_BNF_SUM_DTL_TYP_CD_SK.CD_MPPNG_SK_BNF_SUM_DTL_TYP_CD_SK"),
        "left"
    )
    .select(
        F.col("lnk_IdsEdwBnfSumDtlHistDExtr_InABC.BNF_SUM_DTL_HIST_SK").alias("BNF_SUM_DTL_HIST_SK"),
        F.col("lnk_IdsEdwBnfSumDtlHistDExtr_InABC.PROD_CMPNT_PFX_ID").alias("PROD_CMPNT_PFX_ID"),
        F.col("lnk_IdsEdwBnfSumDtlHistDExtr_InABC.BNF_SUM_DTL_TYP_CD").alias("BNF_SUM_DTL_TYP_CD"),
        F.col("lnk_IdsEdwBnfSumDtlHistDExtr_InABC.BNF_SUM_DTL_NTWK_TYP_CD").alias("BNF_SUM_DTL_NTWK_TYP_CD"),
        F.col("lnk_IdsEdwBnfSumDtlHistDExtr_InABC.BNF_SUM_DTL_EFF_DT_SK").alias("BNF_SUM_DTL_EFF_DT_SK"),
        F.col("lnk_IdsEdwBnfSumDtlHistDExtr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnk_BNF_SUM_DTL_ACCUM_LVL_CD_SK.SRC_CD_BNF_SUM_DTL_ACCUM_LVL_CD_SK").alias("BNF_SUM_DTL_ACCUM_LVL_CD"),
        F.col("lnk_BNF_SUM_DTL_ACCUM_LVL_CD_SK.SRC_CD_NM_BNF_SUM_DTL_ACCUM_LVL_CD_SK").alias("BNF_SUM_DTL_ACCUM_LVL_NM"),
        F.col("lnk_BNF_SUM_DTL_COV_CD_SK.SRC_CD_BNF_SUM_DTL_COV_CD_SK").alias("BNF_SUM_DTL_COV_CD"),
        F.col("lnk_BNF_SUM_DTL_COV_CD_SK.SRC_CD_NM_BNF_SUM_DTL_COV_CD_SK").alias("BNF_SUM_DTL_COV_NM"),
        F.col("lnk_BNF_SUM_DTL_LMT_CTR_TYP_CD_SK.SRC_CD_BNF_SUM_DTL_LMT_CTR_TYP_CD_SK").alias("BNF_SUM_DTL_LMT_CTR_TYP_CD"),
        F.col("lnk_BNF_SUM_DTL_LMT_CTR_TYP_CD_SK.SRC_CD_NM_BNF_SUM_DTL_LMT_CTR_TYP_CD_SK").alias("BNF_SUM_DTL_LMT_CTR_TYP_NM"),
        F.col("lnk_BNF_SUM_DTL_LMT_PERD_TYP_CD_SK.SRC_CD_BNF_SUM_DTL_LMT_PERD_TYP_CD_SK").alias("BNF_SUM_DTL_LMT_PERD_TYP_CD"),
        F.col("lnk_BNF_SUM_DTL_LMT_PERD_TYP_CD_SK.SRC_CD_NM_BNF_SUM_DTL_LMT_PERD_TYP_CD_SK").alias("BNF_SUM_DTL_LMT_PERD_TYP_NM"),
        F.col("lnk_BNF_SUM_DTL_LMT_TYP_CD_SK.SRC_CD_BNF_SUM_DTL_LMT_TYP_CD_SK").alias("BNF_SUM_DTL_LMT_TYP_CD"),
        F.col("lnk_BNF_SUM_DTL_LMT_TYP_CD_SK.SRC_CD_NM_BNF_SUM_DTL_LMT_TYP_CD_SK").alias("BNF_SUM_DTL_LMT_TYP_NM"),
        F.col("lnk_BNF_SUM_DTL_NTWK_TYP_CD_SK.SRC_CD_NM_BNF_SUM_DTL_NTWK_TYP_CD_SK").alias("BNF_SUM_DTL_NTWK_TYP_NM"),
        F.col("lnk_BNF_SUM_DTL_TYP_CD_SK.SRC_CD_NM_BNF_SUM_DTL_TYP_CD_SK").alias("BNF_SUM_DTL_TYP_NM"),
        F.col("lnk_IdsEdwBnfSumDtlHistDExtr_InABC.BNF_SUM_DTL_TERM_DT_SK").alias("BNF_SUM_DTL_TERM_DT_SK"),
        F.col("lnk_IdsEdwBnfSumDtlHistDExtr_InABC.BNF_SUM_DTL_COINS_PCT_AMT").alias("BNF_SUM_DTL_COINS_PCT_AMT"),
        F.col("lnk_IdsEdwBnfSumDtlHistDExtr_InABC.BNF_SUM_DTL_COPAY_AMT").alias("BNF_SUM_DTL_COPAY_AMT"),
        F.col("lnk_IdsEdwBnfSumDtlHistDExtr_InABC.BNF_SUM_DTL_DEDCT_AMT").alias("BNF_SUM_DTL_DEDCT_AMT"),
        F.col("lnk_IdsEdwBnfSumDtlHistDExtr_InABC.BNF_SUM_DTL_LMT_AMT").alias("BNF_SUM_DTL_LMT_AMT"),
        F.col("lnk_IdsEdwBnfSumDtlHistDExtr_InABC.BNF_SUM_DTL_STOPLOSS_AMT").alias("BNF_SUM_DTL_STOPLOSS_AMT"),
        F.col("lnk_IdsEdwBnfSumDtlHistDExtr_InABC.BNF_SUM_DTL_PLN_YR_BEG_MO_DAY").alias("BNF_SUM_DTL_PLN_YR_BEG_MO_DAY"),
        F.col("lnk_IdsEdwBnfSumDtlHistDExtr_InABC.BNF_SUM_DTL_TIER_NO").alias("BNF_SUM_DTL_TIER_NO"),
        F.col("lnk_IdsEdwBnfSumDtlHistDExtr_InABC.BNF_SUM_DTL_USER_FLD_LABEL_1_TX").alias("BNF_SUM_DTL_USER_FLD_LABEL_1_TX"),
        F.col("lnk_IdsEdwBnfSumDtlHistDExtr_InABC.BNF_SUM_DTL_USER_FLD_DATA_1_TX").alias("BNF_SUM_DTL_USER_FLD_DATA_1_TX"),
        F.col("lnk_IdsEdwBnfSumDtlHistDExtr_InABC.BNF_SUM_DTL_USER_FLD_LABEL_2_TX").alias("BNF_SUM_DTL_USER_FLD_LABEL_2_TX"),
        F.col("lnk_IdsEdwBnfSumDtlHistDExtr_InABC.BNF_SUM_DTL_USER_FLD_DATA_2_TX").alias("BNF_SUM_DTL_USER_FLD_DATA_2_TX"),
        F.col("lnk_IdsEdwBnfSumDtlHistDExtr_InABC.BNF_SUM_DTL_USER_FLD_LABEL_3_TX").alias("BNF_SUM_DTL_USER_FLD_LABEL_3_TX"),
        F.col("lnk_IdsEdwBnfSumDtlHistDExtr_InABC.BNF_SUM_DTL_USER_FLD_DATA_3_TX").alias("BNF_SUM_DTL_USER_FLD_DATA_3_TX"),
        F.col("lnk_IdsEdwBnfSumDtlHistDExtr_InABC.BNF_SUM_DTL_USER_FLD_LABEL_4_TX").alias("BNF_SUM_DTL_USER_FLD_LABEL_4_TX"),
        F.col("lnk_IdsEdwBnfSumDtlHistDExtr_InABC.BNF_SUM_DTL_USER_FLD_DATA_4_TX").alias("BNF_SUM_DTL_USER_FLD_DATA_4_TX"),
        F.col("lnk_IdsEdwBnfSumDtlHistDExtr_InABC.BNF_SUM_DTL_USER_FLD_LABEL_5_TX").alias("BNF_SUM_DTL_USER_FLD_LABEL_5_TX"),
        F.col("lnk_IdsEdwBnfSumDtlHistDExtr_InABC.BNF_SUM_DTL_USER_FLD_DATA_5_TX").alias("BNF_SUM_DTL_USER_FLD_DATA_5_TX"),
        F.col("lnk_IdsEdwBnfSumDtlHistDExtr_InABC.BNF_SUM_DTL_USER_FLD_LABEL_6_TX").alias("BNF_SUM_DTL_USER_FLD_LABEL_6_TX"),
        F.col("lnk_IdsEdwBnfSumDtlHistDExtr_InABC.BNF_SUM_DTL_USER_FLD_DATA_6_TX").alias("BNF_SUM_DTL_USER_FLD_DATA_6_TX"),
        F.col("lnk_IdsEdwBnfSumDtlHistDExtr_InABC.SRC_SYS_LAST_UPDT_USER_ID").alias("SRC_SYS_LAST_UPDT_USER_ID"),
        F.col("lnk_IdsEdwBnfSumDtlHistDExtr_InABC.SRC_SYS_LAST_UPDT_DTM").alias("SRC_SYS_LAST_UPDT_DTM"),
        F.col("lnk_IdsEdwBnfSumDtlHistDExtr_InABC.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("lnk_IdsEdwBnfSumDtlHistDExtr_InABC.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("lnk_IdsEdwBnfSumDtlHistDExtr_InABC.BNF_SUM_DTL_ACCUM_LVL_CD_SK").alias("BNF_SUM_DTL_ACCUM_LVL_CD_SK"),
        F.col("lnk_IdsEdwBnfSumDtlHistDExtr_InABC.BNF_SUM_DTL_COV_CD_SK").alias("BNF_SUM_DTL_COV_CD_SK"),
        F.col("lnk_IdsEdwBnfSumDtlHistDExtr_InABC.BNF_SUM_DTL_LMT_CTR_TYP_CD_SK").alias("BNF_SUM_DTL_LMT_CTR_TYP_CD_SK"),
        F.col("lnk_IdsEdwBnfSumDtlHistDExtr_InABC.BNF_SUM_DTL_LMT_PERD_TYP_CD_SK").alias("BNF_SUM_DTL_LMT_PERD_TYP_CD_SK"),
        F.col("lnk_IdsEdwBnfSumDtlHistDExtr_InABC.BNF_SUM_DTL_LMT_TYP_CD_SK").alias("BNF_SUM_DTL_LMT_TYP_CD_SK"),
        F.col("lnk_IdsEdwBnfSumDtlHistDExtr_InABC.BNF_SUM_DTL_NTWK_TYP_CD_SK").alias("BNF_SUM_DTL_NTWK_TYP_CD_SK"),
        F.col("lnk_IdsEdwBnfSumDtlHistDExtr_InABC.BNF_SUM_DTL_TYP_CD_SK").alias("BNF_SUM_DTL_TYP_CD_SK")
    )
)

df_xfm_BusinessLogic = df_lkp_Codes.select(
    F.col("BNF_SUM_DTL_HIST_SK").alias("BNF_SUM_DTL_HIST_SK"),
    F.col("PROD_CMPNT_PFX_ID").alias("PROD_CMPNT_PFX_ID"),
    F.col("BNF_SUM_DTL_TYP_CD").alias("BNF_SUM_DTL_TYP_CD"),
    F.col("BNF_SUM_DTL_NTWK_TYP_CD").alias("BNF_SUM_DTL_NTWK_TYP_CD"),
    rpad(F.col("BNF_SUM_DTL_EFF_DT_SK"), 10, " ").alias("BNF_SUM_DTL_EFF_DT_SK"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    rpad(F.lit(EDWRunCycleDate), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad(F.lit(EDWRunCycleDate), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.when(
        (trim(F.col("BNF_SUM_DTL_ACCUM_LVL_CD")) == "") | (F.col("BNF_SUM_DTL_ACCUM_LVL_CD").isNull()),
        "UNK"
    ).otherwise(F.col("BNF_SUM_DTL_ACCUM_LVL_CD")).alias("BNF_SUM_DTL_ACCUM_LVL_CD"),
    F.when(
        (trim(F.col("BNF_SUM_DTL_ACCUM_LVL_NM")) == "") | (F.col("BNF_SUM_DTL_ACCUM_LVL_NM").isNull()),
        "UNK"
    ).otherwise(F.col("BNF_SUM_DTL_ACCUM_LVL_NM")).alias("BNF_SUM_DTL_ACCUM_LVL_NM"),
    F.when(
        (trim(F.col("BNF_SUM_DTL_COV_CD")) == "") | (F.col("BNF_SUM_DTL_COV_CD").isNull()),
        "UNK"
    ).otherwise(F.col("BNF_SUM_DTL_COV_CD")).alias("BNF_SUM_DTL_COV_CD"),
    F.when(
        (trim(F.col("BNF_SUM_DTL_COV_NM")) == "") | (F.col("BNF_SUM_DTL_COV_NM").isNull()),
        "UNK"
    ).otherwise(F.col("BNF_SUM_DTL_COV_NM")).alias("BNF_SUM_DTL_COV_NM"),
    F.when(
        (trim(F.col("BNF_SUM_DTL_LMT_CTR_TYP_CD")) == "") | (F.col("BNF_SUM_DTL_LMT_CTR_TYP_CD").isNull()),
        "UNK"
    ).otherwise(F.col("BNF_SUM_DTL_LMT_CTR_TYP_CD")).alias("BNF_SUM_DTL_LMT_CTR_TYP_CD"),
    F.when(
        (trim(F.col("BNF_SUM_DTL_LMT_CTR_TYP_NM")) == "") | (F.col("BNF_SUM_DTL_LMT_CTR_TYP_NM").isNull()),
        "UNK"
    ).otherwise(F.col("BNF_SUM_DTL_LMT_CTR_TYP_NM")).alias("BNF_SUM_DTL_LMT_CTR_TYP_NM"),
    F.when(
        (trim(F.col("BNF_SUM_DTL_LMT_PERD_TYP_CD")) == "") | (F.col("BNF_SUM_DTL_LMT_PERD_TYP_CD").isNull()),
        "UNK"
    ).otherwise(F.col("BNF_SUM_DTL_LMT_PERD_TYP_CD")).alias("BNF_SUM_DTL_LMT_PERD_TYP_CD"),
    F.when(
        (trim(F.col("BNF_SUM_DTL_LMT_PERD_TYP_NM")) == "") | (F.col("BNF_SUM_DTL_LMT_PERD_TYP_NM").isNull()),
        "UNK"
    ).otherwise(F.col("BNF_SUM_DTL_LMT_PERD_TYP_NM")).alias("BNF_SUM_DTL_LMT_PERD_TYP_NM"),
    F.when(
        (trim(F.col("BNF_SUM_DTL_LMT_TYP_CD")) == "") | (F.col("BNF_SUM_DTL_LMT_TYP_CD").isNull()),
        "UNK"
    ).otherwise(F.col("BNF_SUM_DTL_LMT_TYP_CD")).alias("BNF_SUM_DTL_LMT_TYP_CD"),
    F.when(
        (trim(F.col("BNF_SUM_DTL_LMT_TYP_NM")) == "") | (F.col("BNF_SUM_DTL_LMT_TYP_NM").isNull()),
        "UNK"
    ).otherwise(F.col("BNF_SUM_DTL_LMT_TYP_NM")).alias("BNF_SUM_DTL_LMT_TYP_NM"),
    F.when(
        (trim(F.col("BNF_SUM_DTL_NTWK_TYP_NM")) == "") | (F.col("BNF_SUM_DTL_NTWK_TYP_NM").isNull()),
        "UNK"
    ).otherwise(F.col("BNF_SUM_DTL_NTWK_TYP_NM")).alias("BNF_SUM_DTL_NTWK_TYP_NM"),
    F.when(
        (trim(F.col("BNF_SUM_DTL_TYP_NM")) == "") | (F.col("BNF_SUM_DTL_TYP_NM").isNull()),
        "UNK"
    ).otherwise(F.col("BNF_SUM_DTL_TYP_NM")).alias("BNF_SUM_DTL_TYP_NM"),
    rpad(F.col("BNF_SUM_DTL_TERM_DT_SK"), 10, " ").alias("BNF_SUM_DTL_TERM_DT_SK"),
    F.col("BNF_SUM_DTL_COINS_PCT_AMT").alias("BNF_SUM_DTL_COINS_PCT_AMT"),
    F.col("BNF_SUM_DTL_COPAY_AMT").alias("BNF_SUM_DTL_COPAY_AMT"),
    F.col("BNF_SUM_DTL_DEDCT_AMT").alias("BNF_SUM_DTL_DEDCT_AMT"),
    F.col("BNF_SUM_DTL_LMT_AMT").alias("BNF_SUM_DTL_LMT_AMT"),
    F.col("BNF_SUM_DTL_STOPLOSS_AMT").alias("BNF_SUM_DTL_STOPLOSS_AMT"),
    F.col("BNF_SUM_DTL_PLN_YR_BEG_MO_DAY").alias("BNF_SUM_DTL_PLN_YR_BEG_MO_DAY"),
    F.col("BNF_SUM_DTL_TIER_NO").alias("BNF_SUM_DTL_TIER_NO"),
    F.col("BNF_SUM_DTL_USER_FLD_LABEL_1_TX").alias("BNF_SUM_DTL_USER_FLD_LABEL_1_TX"),
    F.col("BNF_SUM_DTL_USER_FLD_DATA_1_TX").alias("BNF_SUM_DTL_USER_FLD_DATA_1_TX"),
    F.col("BNF_SUM_DTL_USER_FLD_LABEL_2_TX").alias("BNF_SUM_DTL_USER_FLD_LABEL_2_TX"),
    F.col("BNF_SUM_DTL_USER_FLD_DATA_2_TX").alias("BNF_SUM_DTL_USER_FLD_DATA_2_TX"),
    F.col("BNF_SUM_DTL_USER_FLD_LABEL_3_TX").alias("BNF_SUM_DTL_USER_FLD_LABEL_3_TX"),
    F.col("BNF_SUM_DTL_USER_FLD_DATA_3_TX").alias("BNF_SUM_DTL_USER_FLD_DATA_3_TX"),
    F.col("BNF_SUM_DTL_USER_FLD_LABEL_4_TX").alias("BNF_SUM_DTL_USER_FLD_LABEL_4_TX"),
    F.col("BNF_SUM_DTL_USER_FLD_DATA_4_TX").alias("BNF_SUM_DTL_USER_FLD_DATA_4_TX"),
    F.col("BNF_SUM_DTL_USER_FLD_LABEL_5_TX").alias("BNF_SUM_DTL_USER_FLD_LABEL_5_TX"),
    F.col("BNF_SUM_DTL_USER_FLD_DATA_5_TX").alias("BNF_SUM_DTL_USER_FLD_DATA_5_TX"),
    F.col("BNF_SUM_DTL_USER_FLD_LABEL_6_TX").alias("BNF_SUM_DTL_USER_FLD_LABEL_6_TX"),
    F.col("BNF_SUM_DTL_USER_FLD_DATA_6_TX").alias("BNF_SUM_DTL_USER_FLD_DATA_6_TX"),
    F.col("SRC_SYS_LAST_UPDT_USER_ID").alias("SRC_SYS_LAST_UPDT_USER_ID"),
    F.col("SRC_SYS_LAST_UPDT_DTM").alias("SRC_SYS_LAST_UPDT_DTM"),
    F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("BNF_SUM_DTL_ACCUM_LVL_CD_SK").alias("BNF_SUM_DTL_ACCUM_LVL_CD_SK"),
    F.col("BNF_SUM_DTL_COV_CD_SK").alias("BNF_SUM_DTL_COV_CD_SK"),
    F.col("BNF_SUM_DTL_LMT_CTR_TYP_CD_SK").alias("BNF_SUM_DTL_LMT_CTR_TYP_CD_SK"),
    F.col("BNF_SUM_DTL_LMT_PERD_TYP_CD_SK").alias("BNF_SUM_DTL_LMT_PERD_TYP_CD_SK"),
    F.col("BNF_SUM_DTL_LMT_TYP_CD_SK").alias("BNF_SUM_DTL_LMT_TYP_CD_SK"),
    F.col("BNF_SUM_DTL_NTWK_TYP_CD_SK").alias("BNF_SUM_DTL_NTWK_TYP_CD_SK"),
    F.col("BNF_SUM_DTL_TYP_CD_SK").alias("BNF_SUM_DTL_TYP_CD_SK")
)

write_files(
    df_xfm_BusinessLogic,
    f"{adls_path}/load/BNF_SUM_DTL_HIST_D.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)