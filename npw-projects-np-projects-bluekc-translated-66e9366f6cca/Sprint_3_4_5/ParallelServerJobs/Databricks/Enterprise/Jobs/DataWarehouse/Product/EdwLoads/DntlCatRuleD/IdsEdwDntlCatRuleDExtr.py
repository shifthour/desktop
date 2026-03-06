# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date               Project/Altiris #               Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------        -------------------   -----------------------------------    ---------------------------------------------------------   ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Kimberly Doty                    04-29-2010     4044 Blue Renew v2      Original Programming                          EnterpriseCurDevl 
# MAGIC 
# MAGIC Srikanth Mettpalli              05/23/2013        5114                           Original Programming                          EnterpriseWrhsDevl        Jag Yelavarthi              2013-08-11

# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC CD_MPPNG_SK   CALC_ORDER_CD_SK
# MAGIC RULE_BSS_CD_SK
# MAGIC Write DNTL_CAT_RULE Data into a Sequential file for Load Job IdsEdwDntlCatRuleDLoad.
# MAGIC Read all the Data from IDS DNTL_CAT_RULE Table;
# MAGIC Add Defaults and Null Handling.
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Job Name: IdsEdwDntlCatRuleDExtr
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
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query_db2_DNTL_CAT_RULE_in = (
    "SELECT \n"
    "D.DNTL_CAT_RULE_SK,\n"
    "COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,\n"
    "D.DNTL_CAT_ID,\n"
    "D.DNTL_CAT_RULE_ID,\n"
    "D.DNTL_CAT_SK,\n"
    "D.CALC_ORDER_CD_SK,\n"
    "D.DNTL_CAT_RULE_DESC,\n"
    "D.FSA_RMBRMT_IN,\n"
    "D.RULE_BSS_CD_SK,\n"
    "D.WARN_MSG_SEQ_NO\n"
    "FROM " + IDSOwner + ".DNTL_CAT_RULE D\n"
    "LEFT JOIN " + IDSOwner + ".CD_MPPNG CD ON D.SRC_SYS_CD_SK = CD.CD_MPPNG_SK"
)
df_db2_DNTL_CAT_RULE_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_DNTL_CAT_RULE_in)
    .load()
)

extract_query_db2_CD_MPPNG_in = (
    "SELECT CD_MPPNG_SK,COALESCE(TRGT_CD,'UNK') TRGT_CD,COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM "
    "FROM " + IDSOwner + ".CD_MPPNG"
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
df_cpy_cd_mppng_Ref_CalcOrderCdLkup = df_cpy_cd_mppng
df_cpy_cd_mppng_Ref_RuleBssCdLkup = df_cpy_cd_mppng

df_lkp_Codes_temp = df_db2_DNTL_CAT_RULE_in.alias("lnk_IdsEdwDntlCatRuleDExtr_InABC") \
    .join(
        df_cpy_cd_mppng_Ref_CalcOrderCdLkup.alias("Ref_CalcOrderCdLkup"),
        F.col("lnk_IdsEdwDntlCatRuleDExtr_InABC.CALC_ORDER_CD_SK") == F.col("Ref_CalcOrderCdLkup.CD_MPPNG_SK"),
        "left"
    ) \
    .join(
        df_cpy_cd_mppng_Ref_RuleBssCdLkup.alias("Ref_RuleBssCdLkup"),
        F.col("lnk_IdsEdwDntlCatRuleDExtr_InABC.RULE_BSS_CD_SK") == F.col("Ref_RuleBssCdLkup.CD_MPPNG_SK"),
        "left"
    )

df_lkp_Codes = df_lkp_Codes_temp.select(
    F.col("lnk_IdsEdwDntlCatRuleDExtr_InABC.DNTL_CAT_RULE_SK").alias("DNTL_CAT_RULE_SK"),
    F.col("lnk_IdsEdwDntlCatRuleDExtr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_IdsEdwDntlCatRuleDExtr_InABC.DNTL_CAT_ID").alias("DNTL_CAT_ID"),
    F.col("lnk_IdsEdwDntlCatRuleDExtr_InABC.DNTL_CAT_RULE_ID").alias("DNTL_CAT_RULE_ID"),
    F.col("lnk_IdsEdwDntlCatRuleDExtr_InABC.DNTL_CAT_SK").alias("DNTL_CAT_SK"),
    F.col("Ref_CalcOrderCdLkup.TRGT_CD").alias("DNTL_CAT_RULE_CALC_ORDER_CD"),
    F.col("Ref_CalcOrderCdLkup.TRGT_CD_NM").alias("DNTL_CAT_RULE_CALC_ORDER_NM"),
    F.col("lnk_IdsEdwDntlCatRuleDExtr_InABC.DNTL_CAT_RULE_DESC").alias("DNTL_CAT_RULE_DESC"),
    F.col("lnk_IdsEdwDntlCatRuleDExtr_InABC.FSA_RMBRMT_IN").alias("DNTL_CAT_RULE_FSA_RMBRMT_IN"),
    F.col("Ref_RuleBssCdLkup.TRGT_CD").alias("DNTL_CAT_RULE_BSS_CD"),
    F.col("Ref_RuleBssCdLkup.TRGT_CD_NM").alias("DNTL_CAT_RULE_BSS_NM"),
    F.col("lnk_IdsEdwDntlCatRuleDExtr_InABC.WARN_MSG_SEQ_NO").alias("DNTL_CAT_RULE_WARN_MSG_SEQ_NO"),
    F.col("lnk_IdsEdwDntlCatRuleDExtr_InABC.CALC_ORDER_CD_SK").alias("DNTL_CAT_RULE_CALC_ORDER_CD_SK"),
    F.col("lnk_IdsEdwDntlCatRuleDExtr_InABC.RULE_BSS_CD_SK").alias("DNTL_CAT_RULE_RULE_BSS_CD_SK")
)

df_xfrm_BusinessLogic = df_lkp_Codes.select(
    F.col("DNTL_CAT_RULE_SK").alias("DNTL_CAT_RULE_SK"),
    F.when(F.trim(F.col("SRC_SYS_CD")) == '', 'UNK').otherwise(F.col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
    F.col("DNTL_CAT_ID").alias("DNTL_CAT_ID"),
    F.col("DNTL_CAT_RULE_ID").alias("DNTL_CAT_RULE_ID"),
    F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("DNTL_CAT_SK").alias("DNTL_CAT_SK"),
    F.when(F.trim(F.col("DNTL_CAT_RULE_CALC_ORDER_CD")) == '', 'UNK')
     .otherwise(F.col("DNTL_CAT_RULE_CALC_ORDER_CD")).alias("DNTL_CAT_RULE_CALC_ORDER_CD"),
    F.when(F.trim(F.col("DNTL_CAT_RULE_CALC_ORDER_NM")) == '', 'UNK')
     .otherwise(F.col("DNTL_CAT_RULE_CALC_ORDER_NM")).alias("DNTL_CAT_RULE_CALC_ORDER_NM"),
    F.col("DNTL_CAT_RULE_DESC").alias("DNTL_CAT_RULE_DESC"),
    F.col("DNTL_CAT_RULE_FSA_RMBRMT_IN").alias("DNTL_CAT_RULE_FSA_RMBRMT_IN"),
    F.when(F.trim(F.col("DNTL_CAT_RULE_BSS_CD")) == '', 'UNK')
     .otherwise(F.col("DNTL_CAT_RULE_BSS_CD")).alias("DNTL_CAT_RULE_BSS_CD"),
    F.when(F.trim(F.col("DNTL_CAT_RULE_BSS_NM")) == '', 'UNK')
     .otherwise(F.col("DNTL_CAT_RULE_BSS_NM")).alias("DNTL_CAT_RULE_BSS_NM"),
    F.col("DNTL_CAT_RULE_WARN_MSG_SEQ_NO").alias("DNTL_CAT_RULE_WARN_MSG_SEQ_NO"),
    F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("DNTL_CAT_RULE_CALC_ORDER_CD_SK").alias("DNTL_CAT_RULE_CALC_ORDER_CD_SK"),
    F.col("DNTL_CAT_RULE_RULE_BSS_CD_SK").alias("DNTL_CAT_RULE_RULE_BSS_CD_SK")
)

df_final = df_xfrm_BusinessLogic.select(
    F.col("DNTL_CAT_RULE_SK"),
    F.col("SRC_SYS_CD"),
    F.col("DNTL_CAT_ID"),
    F.col("DNTL_CAT_RULE_ID"),
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("DNTL_CAT_SK"),
    F.col("DNTL_CAT_RULE_CALC_ORDER_CD"),
    F.col("DNTL_CAT_RULE_CALC_ORDER_NM"),
    F.col("DNTL_CAT_RULE_DESC"),
    F.rpad(F.col("DNTL_CAT_RULE_FSA_RMBRMT_IN"), 1, " ").alias("DNTL_CAT_RULE_FSA_RMBRMT_IN"),
    F.col("DNTL_CAT_RULE_BSS_CD"),
    F.col("DNTL_CAT_RULE_BSS_NM"),
    F.col("DNTL_CAT_RULE_WARN_MSG_SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("DNTL_CAT_RULE_CALC_ORDER_CD_SK"),
    F.col("DNTL_CAT_RULE_RULE_BSS_CD_SK")
)

write_files(
    df_final,
    f"{adls_path}/load/DNTL_CAT_RULE_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)