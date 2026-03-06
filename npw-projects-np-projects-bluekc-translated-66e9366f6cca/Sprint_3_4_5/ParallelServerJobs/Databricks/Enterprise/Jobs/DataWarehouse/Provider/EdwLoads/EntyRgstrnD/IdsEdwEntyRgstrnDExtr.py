# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Ralph Tucker           09/14/2005                                             Originally Programmed
# MAGIC 
# MAGIC Archana Palivela     06/19/2013        5114                              Originally Programmed(In Parallel)                                                               EnterpriseWhseDevl
# MAGIC 
# MAGIC PadmajaBandi         31-03-2022     US505557                       Added the 11 columns  to the  ENTY_RGSTRND  table                               EnterPriseDevB              Goutham Kalidindi         2022-05-02

# MAGIC Job name: IdsEdwEntyRgstrnDExtr
# MAGIC 
# MAGIC 
# MAGIC Job Extract data from IDS table and apply Fkey and Code denormalization, create a load ready file.
# MAGIC Write ENTY_RGSTRN_D Data into a Sequential file for Load Ready Job.
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC CD_MPPNG_SK
# MAGIC ENTY_RGSTRN_ST_CD_SK
# MAGIC ENTY_RGSTRN_TYP_CD_SK
# MAGIC Read data from source table ENTY_RGSTRN_D
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when, rpad
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

extract_query_db2_ENTY_RGSTRN_D_Extr = f"""
SELECT 
ENTY.ENTY_RGSTRN_SK,
COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
ENTY.ENTY_ID,
ENTY.ENTY_RGSTRN_TYP_CD_SK,
ENTY.RGSTRN_SEQ_NO,
ENTY.CRT_RUN_CYC_EXCTN_SK,
ENTY.LAST_UPDT_RUN_CYC_EXCTN_SK,
ENTY.CMN_PRCT_SK,
ENTY.PROV_SK,
ENTY.ENTY_RGSTRN_ST_CD_SK,
ENTY.CMN_PRCT_IN,
ENTY.EFF_DT_SK,
ENTY.TERM_DT_SK,
ENTY.RGSTRN_ID,
ENTY.RGSTRN_REL_TX,
ENTY.NTNL_PROV_SK,
ENTY.PROV_RGSTRN_SPEC_CD_SK,
ENTY.PROV_RGSTRN_STTUS_CD_SK,
ENTY.PROV_RGSTRN_VER_METH_CD_SK,
ENTY.PROV_RGSTRN_VER_RSLT_CD_SK,
ENTY.PROV_RGSTRN_VER_SRC_CD_SK,
ENTY.PROV_RGSTRN_CERT_IN,
ENTY.PROV_RGSTRN_PRI_VER_IN,
ENTY.PROV_RGSTRN_INIT_VER_DT_SK,
ENTY.PROV_RGSTRN_LAST_VER_DT_SK,
ENTY.PROV_RGSTRN_NEXT_VER_DT_SK,
ENTY.PROV_RGSTRN_RCVD_VER_DT_SK
FROM {IDSOwner}.ENTY_RGSTRN ENTY
LEFT JOIN {IDSOwner}.CD_MPPNG CD
ON ENTY.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
"""
df_db2_ENTY_RGSTRN_D_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_ENTY_RGSTRN_D_Extr)
    .load()
)

df_db2_ENTY_RGSTRN_D_Extr = df_db2_ENTY_RGSTRN_D_Extr.withColumn("PRRG_MCTR_SPEC", lit(None))

extract_query_DB2_K_PROV_SPEC_CD = f"""
SELECT
PROV_SPEC_CD_SK,
PROV_SPEC_CD,
PROV_SPEC_NM
FROM {IDSOwner}.PROV_SPEC_CD
"""
df_DB2_K_PROV_SPEC_CD = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_DB2_K_PROV_SPEC_CD)
    .load()
)

extract_query_db2_CD_MPPNG_Extr = f"""
SELECT 
CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') TRGT_CD,
COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM
from {IDSOwner}.CD_MPPNG
"""
df_db2_CD_MPPNG_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_CD_MPPNG_Extr)
    .load()
)

df_RefEntyRgstrnTyp = df_db2_CD_MPPNG_Extr.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_RefEntyRgstrnSt = df_db2_CD_MPPNG_Extr.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_Ref_Prov_Rgstrn_Sttus = df_db2_CD_MPPNG_Extr.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_Ref_Prov_Rgstrn_Ver_Meth = df_db2_CD_MPPNG_Extr.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_Ref_Prov_Rgstrn_Ver_Rslt = df_db2_CD_MPPNG_Extr.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_Ref_Prov_Rgstrn_Ver_Src = df_db2_CD_MPPNG_Extr.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_lkp_Codes_temp = df_db2_ENTY_RGSTRN_D_Extr.alias("Lnk_IdsEdwEntyRgstrnDExtr_InABC") \
    .join(
        df_RefEntyRgstrnTyp.alias("RefEntyRgstrnTyp"),
        col("Lnk_IdsEdwEntyRgstrnDExtr_InABC.ENTY_RGSTRN_TYP_CD_SK") == col("RefEntyRgstrnTyp.CD_MPPNG_SK"),
        "left"
    ).join(
        df_RefEntyRgstrnSt.alias("RefEntyRgstrnSt"),
        col("Lnk_IdsEdwEntyRgstrnDExtr_InABC.ENTY_RGSTRN_ST_CD_SK") == col("RefEntyRgstrnSt.CD_MPPNG_SK"),
        "left"
    ).join(
        df_DB2_K_PROV_SPEC_CD.alias("ref_Prov_spec_cd_Sk"),
        (
            (col("Lnk_IdsEdwEntyRgstrnDExtr_InABC.PROV_RGSTRN_SPEC_CD_SK") == col("ref_Prov_spec_cd_Sk.PROV_SPEC_CD_SK"))
            &
            (col("Lnk_IdsEdwEntyRgstrnDExtr_InABC.PRRG_MCTR_SPEC") == col("ref_Prov_spec_cd_Sk.PROV_SPEC_CD"))
        ),
        "left"
    ).join(
        df_Ref_Prov_Rgstrn_Sttus.alias("Ref_Prov_Rgstrn_Sttus"),
        col("Lnk_IdsEdwEntyRgstrnDExtr_InABC.PROV_RGSTRN_STTUS_CD_SK") == col("Ref_Prov_Rgstrn_Sttus.CD_MPPNG_SK"),
        "left"
    ).join(
        df_Ref_Prov_Rgstrn_Ver_Meth.alias("Ref_Prov_Rgstrn_Ver_Meth"),
        col("Lnk_IdsEdwEntyRgstrnDExtr_InABC.PROV_RGSTRN_VER_METH_CD_SK") == col("Ref_Prov_Rgstrn_Ver_Meth.CD_MPPNG_SK"),
        "left"
    ).join(
        df_Ref_Prov_Rgstrn_Ver_Rslt.alias("Ref_Prov_Rgstrn_Ver_Rslt"),
        col("Lnk_IdsEdwEntyRgstrnDExtr_InABC.PROV_RGSTRN_VER_RSLT_CD_SK") == col("Ref_Prov_Rgstrn_Ver_Rslt.CD_MPPNG_SK"),
        "left"
    ).join(
        df_Ref_Prov_Rgstrn_Ver_Src.alias("Ref_Prov_Rgstrn_Ver_Src"),
        col("Lnk_IdsEdwEntyRgstrnDExtr_InABC.PROV_RGSTRN_VER_SRC_CD_SK") == col("Ref_Prov_Rgstrn_Ver_Src.CD_MPPNG_SK"),
        "left"
    )

df_lkp_Codes = df_lkp_Codes_temp.select(
    col("Lnk_IdsEdwEntyRgstrnDExtr_InABC.ENTY_RGSTRN_SK").alias("ENTY_RGSTRN_SK"),
    col("Lnk_IdsEdwEntyRgstrnDExtr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("Lnk_IdsEdwEntyRgstrnDExtr_InABC.ENTY_ID").alias("ENTY_ID"),
    col("RefEntyRgstrnTyp.TRGT_CD").alias("ENTY_RGSTRN_TYP_CD"),
    col("Lnk_IdsEdwEntyRgstrnDExtr_InABC.RGSTRN_SEQ_NO").alias("RGSTRN_SEQ_NO"),
    col("Lnk_IdsEdwEntyRgstrnDExtr_InABC.CMN_PRCT_SK").alias("CMN_PRCT_SK"),
    col("Lnk_IdsEdwEntyRgstrnDExtr_InABC.PROV_SK").alias("PROV_SK"),
    col("Lnk_IdsEdwEntyRgstrnDExtr_InABC.CMN_PRCT_IN").alias("CMN_PRCT_IN"),
    col("Lnk_IdsEdwEntyRgstrnDExtr_InABC.EFF_DT_SK").alias("ENTY_RGSTRN_EFF_DT_SK"),
    col("Lnk_IdsEdwEntyRgstrnDExtr_InABC.RGSTRN_ID").alias("ENTY_RGSTRN_ID"),
    col("Lnk_IdsEdwEntyRgstrnDExtr_InABC.RGSTRN_REL_TX").alias("ENTY_RGSTRN_REL_TX"),
    col("RefEntyRgstrnSt.TRGT_CD").alias("ENTY_RGSTRN_ST_CD"),
    col("RefEntyRgstrnSt.TRGT_CD_NM").alias("ENTY_RGSTRN_ST_NM"),
    col("Lnk_IdsEdwEntyRgstrnDExtr_InABC.TERM_DT_SK").alias("ENTY_RGSTRN_TERM_DT_SK"),
    col("RefEntyRgstrnTyp.TRGT_CD_NM").alias("ENTY_RGSTRN_TYP_NM"),
    col("Lnk_IdsEdwEntyRgstrnDExtr_InABC.ENTY_RGSTRN_TYP_CD_SK").alias("ENTY_RGSTRN_TYP_CD_SK"),
    col("Lnk_IdsEdwEntyRgstrnDExtr_InABC.ENTY_RGSTRN_ST_CD_SK").alias("ENTY_RGSTRN_ST_CD_SK"),
    col("Lnk_IdsEdwEntyRgstrnDExtr_InABC.NTNL_PROV_SK").alias("NTNL_PROV_SK"),
    col("ref_Prov_spec_cd_Sk.PROV_SPEC_CD").alias("PROV_RGSTRN_SPEC_CD"),
    col("ref_Prov_spec_cd_Sk.PROV_SPEC_NM").alias("PROV_RGSTRN_SPEC_NM"),
    col("Ref_Prov_Rgstrn_Sttus.TRGT_CD").alias("PROV_RGSTRN_STTUS_CD"),
    col("Ref_Prov_Rgstrn_Sttus.TRGT_CD_NM").alias("PROV_RGSTRN_STTUS_NM"),
    col("Ref_Prov_Rgstrn_Ver_Meth.TRGT_CD").alias("PROV_RGSTRN_VER_METH_CD"),
    col("Ref_Prov_Rgstrn_Ver_Meth.TRGT_CD_NM").alias("PROV_RGSTRN_VER_METH_NM"),
    col("Ref_Prov_Rgstrn_Ver_Rslt.TRGT_CD").alias("PROV_RGSTRN_VER_RSLT_CD"),
    col("Ref_Prov_Rgstrn_Ver_Rslt.TRGT_CD_NM").alias("PROV_RGSTRN_VER_RSLT_NM"),
    col("Ref_Prov_Rgstrn_Ver_Src.TRGT_CD").alias("PROV_RGSTRN_VER_SRC_CD"),
    col("Ref_Prov_Rgstrn_Ver_Src.TRGT_CD_NM").alias("PROV_RGSTRN_VER_SRC_NM"),
    col("Lnk_IdsEdwEntyRgstrnDExtr_InABC.PROV_RGSTRN_CERT_IN").alias("PROV_RGSTRN_CERT_IN"),
    col("Lnk_IdsEdwEntyRgstrnDExtr_InABC.PROV_RGSTRN_PRI_VER_IN").alias("PROV_RGSTRN_PRI_VER_IN"),
    col("Lnk_IdsEdwEntyRgstrnDExtr_InABC.PROV_RGSTRN_INIT_VER_DT_SK").alias("PROV_RGSTRN_INIT_VER_DT_SK"),
    col("Lnk_IdsEdwEntyRgstrnDExtr_InABC.PROV_RGSTRN_LAST_VER_DT_SK").alias("PROV_RGSTRN_LAST_VER_DT_SK"),
    col("Lnk_IdsEdwEntyRgstrnDExtr_InABC.PROV_RGSTRN_NEXT_VER_DT_SK").alias("PROV_RGSTRN_NEXT_VER_DT_SK"),
    col("Lnk_IdsEdwEntyRgstrnDExtr_InABC.PROV_RGSTRN_RCVD_VER_DT_SK").alias("PROV_RGSTRN_RCVD_VER_DT_SK"),
    col("Lnk_IdsEdwEntyRgstrnDExtr_InABC.PROV_RGSTRN_SPEC_CD_SK").alias("PROV_RGSTRN_SPEC_CD_SK"),
    col("Lnk_IdsEdwEntyRgstrnDExtr_InABC.PROV_RGSTRN_STTUS_CD_SK").alias("PROV_RGSTRN_STTUS_CD_SK"),
    col("Lnk_IdsEdwEntyRgstrnDExtr_InABC.PROV_RGSTRN_VER_METH_CD_SK").alias("PROV_RGSTRN_VER_METH_CD_SK"),
    col("Lnk_IdsEdwEntyRgstrnDExtr_InABC.PROV_RGSTRN_VER_RSLT_CD_SK").alias("PROV_RGSTRN_VER_RSLT_CD_SK"),
    col("Lnk_IdsEdwEntyRgstrnDExtr_InABC.PROV_RGSTRN_VER_SRC_CD_SK").alias("PROV_RGSTRN_VER_SRC_CD_SK")
)

df_xmf_businessLogic = df_lkp_Codes.select(
    col("ENTY_RGSTRN_SK").alias("ENTY_RGSTRN_SK"),
    when(trim(col("SRC_SYS_CD")) == '', 'UNK').otherwise(col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
    col("ENTY_ID").alias("ENTY_ID"),
    when(trim(col("ENTY_RGSTRN_TYP_CD")) == '', 'UNK').otherwise(col("ENTY_RGSTRN_TYP_CD")).alias("ENTY_RGSTRN_TYP_CD"),
    col("RGSTRN_SEQ_NO").alias("ENTY_RGSTRN_SEQ_NO"),
    lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("CMN_PRCT_SK").alias("CMN_PRCT_SK"),
    col("PROV_SK").alias("PROV_SK"),
    col("CMN_PRCT_IN").alias("CMN_PRCT_IN"),
    col("ENTY_RGSTRN_EFF_DT_SK").alias("ENTY_RGSTRN_EFF_DT_SK"),
    col("ENTY_RGSTRN_ID").alias("ENTY_RGSTRN_ID"),
    col("ENTY_RGSTRN_REL_TX").alias("ENTY_RGSTRN_REL_TX"),
    when(trim(col("ENTY_RGSTRN_ST_CD")) == '', 'UNK').otherwise(col("ENTY_RGSTRN_ST_CD")).alias("ENTY_RGSTRN_ST_CD"),
    when(trim(col("ENTY_RGSTRN_ST_NM")) == '', 'UNK').otherwise(col("ENTY_RGSTRN_ST_NM")).alias("ENTY_RGSTRN_ST_NM"),
    col("ENTY_RGSTRN_TERM_DT_SK").alias("ENTY_RGSTRN_TERM_DT_SK"),
    when(trim(col("ENTY_RGSTRN_TYP_NM")) == '', 'UNK').otherwise(col("ENTY_RGSTRN_TYP_NM")).alias("ENTY_RGSTRN_TYP_NM"),
    lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("ENTY_RGSTRN_ST_CD_SK").alias("ENTY_RGSTRN_ST_CD_SK"),
    col("ENTY_RGSTRN_TYP_CD_SK").alias("ENTY_RGSTRN_TYP_CD_SK"),
    col("NTNL_PROV_SK").alias("NTNL_PROV_SK"),
    when(trim(col("PROV_RGSTRN_SPEC_CD")) == '', 'UNK').otherwise(col("PROV_RGSTRN_SPEC_CD")).alias("PROV_RGSTRN_SPEC_CD"),
    when(trim(col("PROV_RGSTRN_SPEC_NM")) == '', 'UNK').otherwise(col("PROV_RGSTRN_SPEC_NM")).alias("PROV_RGSTRN_SPEC_NM"),
    when(trim(col("PROV_RGSTRN_STTUS_CD")) == '', 'UNK').otherwise(col("PROV_RGSTRN_STTUS_CD")).alias("PROV_RGSTRN_STTUS_CD"),
    when(trim(col("PROV_RGSTRN_STTUS_NM")) == '', 'UNK').otherwise(col("PROV_RGSTRN_STTUS_NM")).alias("PROV_RGSTRN_STTUS_NM"),
    when(trim(col("PROV_RGSTRN_VER_METH_CD")) == '', 'UNK').otherwise(col("PROV_RGSTRN_VER_METH_CD")).alias("PROV_RGSTRN_VER_METH_CD"),
    when(trim(col("PROV_RGSTRN_VER_METH_NM")) == '', 'UNK').otherwise(col("PROV_RGSTRN_VER_METH_NM")).alias("PROV_RGSTRN_VER_METH_NM"),
    when(trim(col("PROV_RGSTRN_VER_RSLT_CD")) == '', 'UNK').otherwise(col("PROV_RGSTRN_VER_RSLT_CD")).alias("PROV_RGSTRN_VER_RSLT_CD"),
    when(trim(col("PROV_RGSTRN_VER_RSLT_NM")) == '', 'UNK').otherwise(col("PROV_RGSTRN_VER_RSLT_NM")).alias("PROV_RGSTRN_VER_RSLT_NM"),
    when(trim(col("PROV_RGSTRN_VER_SRC_CD")) == '', 'UNK').otherwise(col("PROV_RGSTRN_VER_SRC_CD")).alias("PROV_RGSTRN_VER_SRC_CD"),
    when(trim(col("PROV_RGSTRN_VER_SRC_NM")) == '', 'UNK').otherwise(col("PROV_RGSTRN_VER_SRC_NM")).alias("PROV_RGSTRN_VER_SRC_NM"),
    col("PROV_RGSTRN_CERT_IN").alias("PROV_RGSTRN_CERT_IN"),
    col("PROV_RGSTRN_PRI_VER_IN").alias("PROV_RGSTRN_PRI_VER_IN"),
    col("PROV_RGSTRN_INIT_VER_DT_SK").alias("PROV_RGSTRN_INIT_VER_DT_SK"),
    col("PROV_RGSTRN_LAST_VER_DT_SK").alias("PROV_RGSTRN_LAST_VER_DT_SK"),
    col("PROV_RGSTRN_NEXT_VER_DT_SK").alias("PROV_RGSTRN_NEXT_VER_DT_SK"),
    col("PROV_RGSTRN_RCVD_VER_DT_SK").alias("PROV_RGSTRN_RCVD_VER_DT_SK"),
    col("PROV_RGSTRN_SPEC_CD_SK").alias("PROV_RGSTRN_SPEC_CD_SK"),
    col("PROV_RGSTRN_STTUS_CD_SK").alias("PROV_RGSTRN_STTUS_CD_SK"),
    col("PROV_RGSTRN_VER_METH_CD_SK").alias("PROV_RGSTRN_VER_METH_CD_SK"),
    col("PROV_RGSTRN_VER_RSLT_CD_SK").alias("PROV_RGSTRN_VER_RSLT_CD_SK"),
    col("PROV_RGSTRN_VER_SRC_CD_SK").alias("PROV_RGSTRN_VER_SRC_CD_SK")
)

df_final = df_xmf_businessLogic.select(
    col("ENTY_RGSTRN_SK"),
    col("SRC_SYS_CD"),
    col("ENTY_ID"),
    col("ENTY_RGSTRN_TYP_CD"),
    col("ENTY_RGSTRN_SEQ_NO"),
    rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("CMN_PRCT_SK"),
    col("PROV_SK"),
    rpad(col("CMN_PRCT_IN"), 1, " ").alias("CMN_PRCT_IN"),
    rpad(col("ENTY_RGSTRN_EFF_DT_SK"), 10, " ").alias("ENTY_RGSTRN_EFF_DT_SK"),
    col("ENTY_RGSTRN_ID"),
    col("ENTY_RGSTRN_REL_TX"),
    col("ENTY_RGSTRN_ST_CD"),
    col("ENTY_RGSTRN_ST_NM"),
    rpad(col("ENTY_RGSTRN_TERM_DT_SK"), 10, " ").alias("ENTY_RGSTRN_TERM_DT_SK"),
    col("ENTY_RGSTRN_TYP_NM"),
    rpad(col("CRT_RUN_CYC_EXCTN_SK"), 0, " ").alias("CRT_RUN_CYC_EXCTN_SK"),  # If length=10 was not specified, leave as is or 0 if it was "char" length=0
    rpad(col("LAST_UPDT_RUN_CYC_EXCTN_SK"), 0, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("ENTY_RGSTRN_ST_CD_SK"),
    col("ENTY_RGSTRN_TYP_CD_SK"),
    rpad(col("NTNL_PROV_SK"), 0, " ").alias("NTNL_PROV_SK"),
    col("PROV_RGSTRN_SPEC_CD"),
    col("PROV_RGSTRN_SPEC_NM"),
    col("PROV_RGSTRN_STTUS_CD"),
    col("PROV_RGSTRN_STTUS_NM"),
    col("PROV_RGSTRN_VER_METH_CD"),
    col("PROV_RGSTRN_VER_METH_NM"),
    col("PROV_RGSTRN_VER_RSLT_CD"),
    col("PROV_RGSTRN_VER_RSLT_NM"),
    col("PROV_RGSTRN_VER_SRC_CD"),
    col("PROV_RGSTRN_VER_SRC_NM"),
    rpad(col("PROV_RGSTRN_CERT_IN"), 1, " ").alias("PROV_RGSTRN_CERT_IN"),
    rpad(col("PROV_RGSTRN_PRI_VER_IN"), 1, " ").alias("PROV_RGSTRN_PRI_VER_IN"),
    rpad(col("PROV_RGSTRN_INIT_VER_DT_SK"), 10, " ").alias("PROV_RGSTRN_INIT_VER_DT_SK"),
    rpad(col("PROV_RGSTRN_LAST_VER_DT_SK"), 10, " ").alias("PROV_RGSTRN_LAST_VER_DT_SK"),
    rpad(col("PROV_RGSTRN_NEXT_VER_DT_SK"), 10, " ").alias("PROV_RGSTRN_NEXT_VER_DT_SK"),
    rpad(col("PROV_RGSTRN_RCVD_VER_DT_SK"), 10, " ").alias("PROV_RGSTRN_RCVD_VER_DT_SK"),
    col("PROV_RGSTRN_SPEC_CD_SK"),
    col("PROV_RGSTRN_STTUS_CD_SK"),
    col("PROV_RGSTRN_VER_METH_CD_SK"),
    col("PROV_RGSTRN_VER_RSLT_CD_SK"),
    col("PROV_RGSTRN_VER_SRC_CD_SK")
)

write_files(
    df_final,
    f"{adls_path}/load/ENTY_RGSTRN_D.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)