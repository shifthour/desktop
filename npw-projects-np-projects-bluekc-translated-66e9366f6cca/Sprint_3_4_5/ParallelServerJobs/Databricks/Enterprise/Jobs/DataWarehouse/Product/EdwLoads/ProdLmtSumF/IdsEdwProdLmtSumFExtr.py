# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               12/07/2007          3044                              Originally Programmed                           devlEDW10                                
# MAGIC                                                                                                                                       
# MAGIC Balkarn Gill            05/28/2013      5114                                                                                                     EnterpriseWhseDevl        Pete Marshall                2013-08-08

# MAGIC Write PROD_LMT_SUM_F Data into a Sequential file for Load Job IdsEdwProdLmtSumFLoad.
# MAGIC Code SK lookups for Denormalization
# MAGIC Read from source table PROD_CMPNT and CD_MPPNG.
# MAGIC Job:
# MAGIC ProdLmtSumFExtr
# MAGIC Table:
# MAGIC  PROD_LMT_SUM_F         
# MAGIC Pull the matched set of products fron IDS and move to EDW.
# MAGIC Add Defaults and Null Handling.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value("IDSOwner", "")
ids_secret_name = get_widget_value("ids_secret_name", "")
EDWRunCycleDate = get_widget_value("EDWRunCycleDate", "")
EDWRunCycle = get_widget_value("EDWRunCycle", "")

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query_db2_IdsProdCmpnt_in = f"""SELECT 
COALESCE(CD2.TRGT_CD,'UNK') AS SRC_SYS_CD,
PROD_CMPNT.PROD_ID,
PROD_CMPNT.PROD_CMPNT_EFF_DT_SK,
PROD_CMPNT.PROD_SK,
PROD_CMPNT.PROD_CMPNT_TERM_DT_SK,
PROD_CMPNT.PROD_CMPNT_PFX_ID
FROM {IDSOwner}.PROD_CMPNT PROD_CMPNT
JOIN {IDSOwner}.CD_MPPNG CD1
  ON PROD_CMPNT.PROD_CMPNT_TYP_CD_SK = CD1.CD_MPPNG_SK
LEFT OUTER JOIN {IDSOwner}.CD_MPPNG CD2
  ON PROD_CMPNT.SRC_SYS_CD_SK= CD2.CD_MPPNG_SK
WHERE CD1.TRGT_CD = 'BSBS'
ORDER BY
COALESCE(CD2.TRGT_CD,'UNK'),
PROD_CMPNT.PROD_ID,
PROD_CMPNT.PROD_CMPNT_EFF_DT_SK
"""
df_db2_IdsProdCmpnt_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_IdsProdCmpnt_in)
    .load()
)

extract_query_db2_Saip_in = f"""SELECT BNF_SUM_DTL.PROD_CMPNT_PFX_ID,BNF_SUM_DTL.LMT_AMT 
FROM {IDSOwner}.BNF_SUM_DTL BNF_SUM_DTL,{IDSOwner}.CD_MPPNG CD_MPPNG 
WHERE BNF_SUM_DTL.BNF_SUM_DTL_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK 
  AND CD_MPPNG.TRGT_CD = 'SAIP'
"""
df_db2_Saip_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_Saip_in)
    .load()
)

extract_query_db2_Lftm_in = f"""SELECT BNF_SUM_DTL.PROD_CMPNT_PFX_ID,BNF_SUM_DTL.LMT_AMT 
FROM {IDSOwner}.BNF_SUM_DTL BNF_SUM_DTL,{IDSOwner}.CD_MPPNG CD_MPPNG 
WHERE BNF_SUM_DTL.BNF_SUM_DTL_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK 
  AND CD_MPPNG.TRGT_CD = 'LFTM'
"""
df_db2_Lftm_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_Lftm_in)
    .load()
)

extract_query_db2_Dme_in = f"""SELECT BNF_SUM_DTL.PROD_CMPNT_PFX_ID,BNF_SUM_DTL.LMT_AMT 
FROM {IDSOwner}.BNF_SUM_DTL BNF_SUM_DTL,{IDSOwner}.CD_MPPNG CD_MPPNG
WHERE BNF_SUM_DTL.BNF_SUM_DTL_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD = 'DME'
"""
df_db2_Dme_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_Dme_in)
    .load()
)

extract_query_db2_Ifs_in = f"""SELECT BNF_SUM_DTL.PROD_CMPNT_PFX_ID,BNF_SUM_DTL.LMT_AMT 
FROM {IDSOwner}.BNF_SUM_DTL BNF_SUM_DTL,{IDSOwner}.CD_MPPNG CD_MPPNG
WHERE BNF_SUM_DTL.BNF_SUM_DTL_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD = 'IFS'
"""
df_db2_Ifs_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_Ifs_in)
    .load()
)

extract_query_db2_Mhik_in = f"""SELECT BNF_SUM_DTL.PROD_CMPNT_PFX_ID,BNF_SUM_DTL.LMT_AMT 
FROM {IDSOwner}.BNF_SUM_DTL BNF_SUM_DTL,{IDSOwner}.CD_MPPNG CD_MPPNG
WHERE BNF_SUM_DTL.BNF_SUM_DTL_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD = 'MHIK'
"""
df_db2_Mhik_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_Mhik_in)
    .load()
)

extract_query_db2_Sto_in = f"""SELECT BNF_SUM_DTL.PROD_CMPNT_PFX_ID,BNF_SUM_DTL.LMT_AMT 
FROM {IDSOwner}.BNF_SUM_DTL BNF_SUM_DTL,{IDSOwner}.CD_MPPNG CD_MPPNG
WHERE BNF_SUM_DTL.BNF_SUM_DTL_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD = 'STO'
"""
df_db2_Sto_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_Sto_in)
    .load()
)

extract_query_db2_Rtnm_in = f"""SELECT BNF_SUM_DTL.PROD_CMPNT_PFX_ID,BNF_SUM_DTL.LMT_AMT
FROM {IDSOwner}.BNF_SUM_DTL BNF_SUM_DTL,{IDSOwner}.CD_MPPNG CD_MPPNG
WHERE BNF_SUM_DTL.BNF_SUM_DTL_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD = 'RTNM'
"""
df_db2_Rtnm_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_Rtnm_in)
    .load()
)

extract_query_db2_Mhdk_in = f"""SELECT BNF_SUM_DTL.PROD_CMPNT_PFX_ID,BNF_SUM_DTL.LMT_AMT
FROM {IDSOwner}.BNF_SUM_DTL BNF_SUM_DTL,{IDSOwner}.CD_MPPNG CD_MPPNG
WHERE BNF_SUM_DTL.BNF_SUM_DTL_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD = 'MHDK'
"""
df_db2_Mhdk_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_Mhdk_in)
    .load()
)

extract_query_db2_Chir_in = f"""SELECT BNF_SUM_DTL.PROD_CMPNT_PFX_ID,BNF_SUM_DTL.LMT_AMT
FROM {IDSOwner}.BNF_SUM_DTL BNF_SUM_DTL,{IDSOwner}.CD_MPPNG CD_MPPNG
WHERE BNF_SUM_DTL.BNF_SUM_DTL_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD = 'CHIR'
"""
df_db2_Chir_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_Chir_in)
    .load()
)

extract_query_db2_Ptot_in = f"""SELECT BNF_SUM_DTL.PROD_CMPNT_PFX_ID,BNF_SUM_DTL.LMT_AMT
FROM {IDSOwner}.BNF_SUM_DTL BNF_SUM_DTL,{IDSOwner}.CD_MPPNG CD_MPPNG
WHERE BNF_SUM_DTL.BNF_SUM_DTL_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD = 'PTOT'
"""
df_db2_Ptot_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_Ptot_in)
    .load()
)

extract_query_db2_Snf_in = f"""SELECT BNF_SUM_DTL.PROD_CMPNT_PFX_ID,BNF_SUM_DTL.LMT_AMT
FROM {IDSOwner}.BNF_SUM_DTL BNF_SUM_DTL,{IDSOwner}.CD_MPPNG CD_MPPNG
WHERE BNF_SUM_DTL.BNF_SUM_DTL_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD = 'SNF'
"""
df_db2_Snf_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_Snf_in)
    .load()
)

extract_query_db2_Mii_in = f"""SELECT BNF_SUM_DTL.PROD_CMPNT_PFX_ID,BNF_SUM_DTL.LMT_AMT
FROM {IDSOwner}.BNF_SUM_DTL BNF_SUM_DTL,{IDSOwner}.CD_MPPNG CD_MPPNG
WHERE BNF_SUM_DTL.BNF_SUM_DTL_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD = 'MII'
"""
df_db2_Mii_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_Mii_in)
    .load()
)

extract_query_db2_Detx_in = f"""SELECT BNF_SUM_DTL.PROD_CMPNT_PFX_ID,BNF_SUM_DTL.LMT_AMT
FROM {IDSOwner}.BNF_SUM_DTL BNF_SUM_DTL,{IDSOwner}.CD_MPPNG CD_MPPNG
WHERE BNF_SUM_DTL.BNF_SUM_DTL_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD = 'DETX'
"""
df_db2_Detx_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_Detx_in)
    .load()
)

extract_query_db2_Oph_in = f"""SELECT BNF_SUM_DTL.PROD_CMPNT_PFX_ID,BNF_SUM_DTL.LMT_AMT
FROM {IDSOwner}.BNF_SUM_DTL BNF_SUM_DTL,{IDSOwner}.CD_MPPNG CD_MPPNG
WHERE BNF_SUM_DTL.BNF_SUM_DTL_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD = 'OPH'
"""
df_db2_Oph_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_Oph_in)
    .load()
)

extract_query_db2_Rehb_in = f"""SELECT BNF_SUM_DTL.PROD_CMPNT_PFX_ID,BNF_SUM_DTL.LMT_AMT
FROM {IDSOwner}.BNF_SUM_DTL BNF_SUM_DTL,{IDSOwner}.CD_MPPNG CD_MPPNG
WHERE BNF_SUM_DTL.BNF_SUM_DTL_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD = 'REHB'
"""
df_db2_Rehb_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_Rehb_in)
    .load()
)

extract_query_db2_Hosi_in = f"""SELECT BNF_SUM_DTL.PROD_CMPNT_PFX_ID,BNF_SUM_DTL.LMT_AMT
FROM {IDSOwner}.BNF_SUM_DTL BNF_SUM_DTL,{IDSOwner}.CD_MPPNG CD_MPPNG
WHERE BNF_SUM_DTL.BNF_SUM_DTL_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD = 'HOSI'
"""
df_db2_Hosi_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_Hosi_in)
    .load()
)

extract_query_db2_Hhcv_in = f"""SELECT BNF_SUM_DTL.PROD_CMPNT_PFX_ID,BNF_SUM_DTL.LMT_AMT
FROM {IDSOwner}.BNF_SUM_DTL BNF_SUM_DTL,{IDSOwner}.CD_MPPNG CD_MPPNG
WHERE BNF_SUM_DTL.BNF_SUM_DTL_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD = 'HHCV'
"""
df_db2_Hhcv_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_Hhcv_in)
    .load()
)

extract_query_db2_Mhkk_in = f"""SELECT BNF_SUM_DTL.PROD_CMPNT_PFX_ID,BNF_SUM_DTL.LMT_AMT
FROM {IDSOwner}.BNF_SUM_DTL BNF_SUM_DTL,{IDSOwner}.CD_MPPNG CD_MPPNG
WHERE BNF_SUM_DTL.BNF_SUM_DTL_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD = 'MHKK'
"""
df_db2_Mhkk_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_Mhkk_in)
    .load()
)

extract_query_db2_Hoi_in = f"""SELECT BNF_SUM_DTL.PROD_CMPNT_PFX_ID,BNF_SUM_DTL.LMT_AMT
FROM {IDSOwner}.BNF_SUM_DTL BNF_SUM_DTL,{IDSOwner}.CD_MPPNG CD_MPPNG
WHERE BNF_SUM_DTL.BNF_SUM_DTL_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD = 'HOI'
"""
df_db2_Hoi_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_Hoi_in)
    .load()
)

df_lkp_FKeys_in = df_db2_IdsProdCmpnt_in.alias("lnk_IdsEdwProdLmtSumFExtr_InAbc") \
    .join(df_db2_Dme_in.alias("Ref_Dme"), F.col("lnk_IdsEdwProdLmtSumFExtr_InAbc.PROD_CMPNT_PFX_ID") == F.col("Ref_Dme.PROD_CMPNT_PFX_ID"), "left") \
    .join(df_db2_Lftm_in.alias("Ref_Lftm"), F.col("lnk_IdsEdwProdLmtSumFExtr_InAbc.PROD_CMPNT_PFX_ID") == F.col("Ref_Lftm.PROD_CMPNT_PFX_ID"), "left") \
    .join(df_db2_Hoi_in.alias("Ref_Hoi"), F.col("lnk_IdsEdwProdLmtSumFExtr_InAbc.PROD_CMPNT_PFX_ID") == F.col("Ref_Hoi.PROD_CMPNT_PFX_ID"), "left") \
    .join(df_db2_Ifs_in.alias("Ref_Ifs"), F.col("lnk_IdsEdwProdLmtSumFExtr_InAbc.PROD_CMPNT_PFX_ID") == F.col("Ref_Ifs.PROD_CMPNT_PFX_ID"), "left") \
    .join(df_db2_Mhik_in.alias("Ref_Mhik"), F.col("lnk_IdsEdwProdLmtSumFExtr_InAbc.PROD_CMPNT_PFX_ID") == F.col("Ref_Mhik.PROD_CMPNT_PFX_ID"), "left") \
    .join(df_db2_Mhkk_in.alias("Ref_Mhkk"), F.col("lnk_IdsEdwProdLmtSumFExtr_InAbc.PROD_CMPNT_PFX_ID") == F.col("Ref_Mhkk.PROD_CMPNT_PFX_ID"), "left") \
    .join(df_db2_Mii_in.alias("Ref_Mii"), F.col("lnk_IdsEdwProdLmtSumFExtr_InAbc.PROD_CMPNT_PFX_ID") == F.col("Ref_Mii.PROD_CMPNT_PFX_ID"), "left") \
    .join(df_db2_Oph_in.alias("Ref_Oph"), F.col("lnk_IdsEdwProdLmtSumFExtr_InAbc.PROD_CMPNT_PFX_ID") == F.col("Ref_Oph.PROD_CMPNT_PFX_ID"), "left") \
    .join(df_db2_Sto_in.alias("Ref_Sto"), F.col("lnk_IdsEdwProdLmtSumFExtr_InAbc.PROD_CMPNT_PFX_ID") == F.col("Ref_Sto.PROD_CMPNT_PFX_ID"), "left") \
    .join(df_db2_Mhdk_in.alias("Ref_Mhdk"), F.col("lnk_IdsEdwProdLmtSumFExtr_InAbc.PROD_CMPNT_PFX_ID") == F.col("Ref_Mhdk.PROD_CMPNT_PFX_ID"), "left") \
    .join(df_db2_Rtnm_in.alias("Ref_Rtnm"), F.col("lnk_IdsEdwProdLmtSumFExtr_InAbc.PROD_CMPNT_PFX_ID") == F.col("Ref_Rtnm.PROD_CMPNT_PFX_ID"), "left") \
    .join(df_db2_Saip_in.alias("Ref_Saip"), F.col("lnk_IdsEdwProdLmtSumFExtr_InAbc.PROD_CMPNT_PFX_ID") == F.col("Ref_Saip.PROD_CMPNT_PFX_ID"), "left") \
    .join(df_db2_Snf_in.alias("Ref_Snf"), F.col("lnk_IdsEdwProdLmtSumFExtr_InAbc.PROD_CMPNT_PFX_ID") == F.col("Ref_Snf.PROD_CMPNT_PFX_ID"), "left") \
    .join(df_db2_Chir_in.alias("Ref_Chir"), F.col("lnk_IdsEdwProdLmtSumFExtr_InAbc.PROD_CMPNT_PFX_ID") == F.col("Ref_Chir.PROD_CMPNT_PFX_ID"), "left") \
    .join(df_db2_Ptot_in.alias("Ref_Ptot"), F.col("lnk_IdsEdwProdLmtSumFExtr_InAbc.PROD_CMPNT_PFX_ID") == F.col("Ref_Ptot.PROD_CMPNT_PFX_ID"), "left") \
    .join(df_db2_Detx_in.alias("Ref_Detx"), F.col("lnk_IdsEdwProdLmtSumFExtr_InAbc.PROD_CMPNT_PFX_ID") == F.col("Ref_Detx.PROD_CMPNT_PFX_ID"), "left") \
    .join(df_db2_Hhcv_in.alias("Ref_Hhcv"), F.col("lnk_IdsEdwProdLmtSumFExtr_InAbc.PROD_CMPNT_PFX_ID") == F.col("Ref_Hhcv.PROD_CMPNT_PFX_ID"), "left") \
    .join(df_db2_Hosi_in.alias("Ref_Hosi"), F.col("lnk_IdsEdwProdLmtSumFExtr_InAbc.PROD_CMPNT_PFX_ID") == F.col("Ref_Hosi.PROD_CMPNT_PFX_ID"), "left") \
    .join(df_db2_Rehb_in.alias("Ref_Rehb"), F.col("lnk_IdsEdwProdLmtSumFExtr_InAbc.PROD_CMPNT_PFX_ID") == F.col("Ref_Rehb.PROD_CMPNT_PFX_ID"), "left")

df_lkp_FKeys = df_lkp_FKeys_in.select(
    F.col("lnk_IdsEdwProdLmtSumFExtr_InAbc.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_IdsEdwProdLmtSumFExtr_InAbc.PROD_ID").alias("PROD_ID"),
    F.col("lnk_IdsEdwProdLmtSumFExtr_InAbc.PROD_CMPNT_EFF_DT_SK").alias("PROD_CMPNT_EFF_DT_SK"),
    F.col("lnk_IdsEdwProdLmtSumFExtr_InAbc.PROD_SK").alias("PROD_SK"),
    F.col("lnk_IdsEdwProdLmtSumFExtr_InAbc.PROD_CMPNT_TERM_DT_SK").alias("PROD_CMPNT_TERM_DT_SK"),
    F.col("Ref_Dme.LMT_AMT").alias("DME_MAX_AMT"),
    F.col("Ref_Lftm.LMT_AMT").alias("IN_OUT_NTWK_LFTM_MAX_AMT"),
    F.col("Ref_Hoi.LMT_AMT").alias("IP_HOSP_CYM_COPAY_AMT"),
    F.col("Ref_Ifs.LMT_AMT").alias("INFRTL_LFTM_MAX_AMT"),
    F.col("Ref_Mhik.LMT_AMT").alias("KS_KS_MNTL_HLTH_IP_MAX_VST_AMT"),
    F.col("Ref_Mhkk.LMT_AMT").alias("KS_KS_MNTL_HLTH_MAX_VST_AMT"),
    F.col("Ref_Mii.LMT_AMT").alias("MHLTH_IP_IN_NTWK_CYM_COPAY_AMT"),
    F.col("Ref_Oph.LMT_AMT").alias("OP_HOSP_CYM_COPAY_AMT"),
    F.col("Ref_Sto.LMT_AMT").alias("OP_SPCH_THER_SVC_MAX_AMT"),
    F.col("Ref_Mhdk.LMT_AMT").alias("PPO_KS_DIR_ENR_MHLTH_MAX_AMT"),
    F.col("Ref_Rtnm.LMT_AMT").alias("RTN_MAX_AMT"),
    F.col("Ref_Saip.LMT_AMT").alias("SBSTNC_ABUSE_IP_MAX_AMT"),
    F.col("Ref_Snf.LMT_AMT").alias("SKILL_NURSE_CYM_AMT"),
    F.col("Ref_Chir.LMT_AMT").alias("CHIRO_CYM_VST_CT"),
    F.col("Ref_Ptot.LMT_AMT").alias("COMBND_PT_OT_CYM_VST_CT"),
    F.col("Ref_Detx.LMT_AMT").alias("DETOX_DAYS_MAX_VST_CT"),
    F.col("Ref_Hhcv.LMT_AMT").alias("HOME_HLTH_CYM_VST_CT"),
    F.col("Ref_Hosi.LMT_AMT").alias("IP_HSPC_LFTM_MAX_VST_CT"),
    F.col("Ref_Rehb.LMT_AMT").alias("OP_REHAB_MAX_VST_CT"),
    F.col("Ref_Dme.PROD_CMPNT_PFX_ID").alias("DME_PROD_CMPNT_PFX_ID"),
    F.col("Ref_Lftm.PROD_CMPNT_PFX_ID").alias("LFTM_PROD_CMPNT_PFX_ID"),
    F.col("Ref_Hoi.PROD_CMPNT_PFX_ID").alias("HOI_PROD_CMPNT_PFX_ID"),
    F.col("Ref_Ifs.PROD_CMPNT_PFX_ID").alias("IFS_PROD_CMPNT_PFX_ID"),
    F.col("Ref_Mhik.PROD_CMPNT_PFX_ID").alias("MHIK_PROD_CMPNT_PFX_ID"),
    F.col("Ref_Mhkk.PROD_CMPNT_PFX_ID").alias("MHKK_PROD_CMPNT_PFX_ID"),
    F.col("Ref_Mii.PROD_CMPNT_PFX_ID").alias("MII_PROD_CMPNT_PFX_ID"),
    F.col("Ref_Oph.PROD_CMPNT_PFX_ID").alias("OPH_PROD_CMPNT_PFX_ID"),
    F.col("Ref_Sto.PROD_CMPNT_PFX_ID").alias("STO_PROD_CMPNT_PFX_ID"),
    F.col("Ref_Mhdk.PROD_CMPNT_PFX_ID").alias("MHDK_PROD_CMPNT_PFX_ID"),
    F.col("Ref_Rtnm.PROD_CMPNT_PFX_ID").alias("RTNM_PROD_CMPNT_PFX_ID"),
    F.col("Ref_Saip.PROD_CMPNT_PFX_ID").alias("SAIP_PROD_CMPNT_PFX_ID"),
    F.col("Ref_Snf.PROD_CMPNT_PFX_ID").alias("SNF_PROD_CMPNT_PFX_ID"),
    F.col("Ref_Chir.PROD_CMPNT_PFX_ID").alias("CHIR_PROD_CMPNT_PFX_ID"),
    F.col("Ref_Ptot.PROD_CMPNT_PFX_ID").alias("PTOT_PROD_CMPNT_PFX_ID"),
    F.col("Ref_Detx.PROD_CMPNT_PFX_ID").alias("DEXT_PROD_CMPNT_PFX_ID"),
    F.col("Ref_Hhcv.PROD_CMPNT_PFX_ID").alias("HHCV_PROD_CMPNT_PFX_ID"),
    F.col("Ref_Hosi.PROD_CMPNT_PFX_ID").alias("HOSI_PROD_CMPNT_PFX_ID"),
    F.col("Ref_Rehb.PROD_CMPNT_PFX_ID").alias("REHB_PROD_CMPNT_PFX_ID")
)

w = Window.orderBy(F.lit(1))
df_xfm_BusinessLogic_in = df_lkp_FKeys

df_xfm_BusinessLogic_temp = df_xfm_BusinessLogic_in.withColumn("rowNo", F.row_number().over(w))

df_xfm_BusinessLogic_main_temp = df_xfm_BusinessLogic_temp.filter(
    "(SRC_SYS_CD <> 'UNK' AND PROD_ID <> 'UNK' AND PROD_CMPNT_EFF_DT_SK <> '1753-01-01') AND (SRC_SYS_CD <> 'NA' AND PROD_ID <> 'NA' AND PROD_CMPNT_EFF_DT_SK <> '1753-01-01')"
)

df_xfm_BusinessLogic_na_temp = df_xfm_BusinessLogic_temp.filter("rowNo = 1")
df_xfm_BusinessLogic_unk_temp = df_xfm_BusinessLogic_temp.filter("rowNo = 1")

df_xfm_BusinessLogic_main = df_xfm_BusinessLogic_main_temp.select(
    F.lit(0).alias("PROD_LMT_SUM_SK"),
    F.when(F.length(trim(F.col("SRC_SYS_CD"))) == 0, F.lit("UNK")).otherwise(F.col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
    F.col("PROD_ID").alias("PROD_ID"),
    F.col("PROD_CMPNT_EFF_DT_SK").alias("PROD_CMPNT_EFF_DT_SK"),
    F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("EDWRunCycleDate").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("PROD_SK").alias("PROD_SK"),
    F.col("PROD_CMPNT_TERM_DT_SK").alias("PROD_CMPNT_TERM_DT_SK"),
    F.when(F.length(trim(F.col("DME_PROD_CMPNT_PFX_ID"))) == 0, F.lit(None)).otherwise(F.col("DME_MAX_AMT")).alias("DME_MAX_AMT"),
    F.when(F.length(trim(F.col("LFTM_PROD_CMPNT_PFX_ID"))) == 0, F.lit(None)).otherwise(F.col("IN_OUT_NTWK_LFTM_MAX_AMT")).alias("IN_OUT_NTWK_LFTM_MAX_AMT"),
    F.when(F.length(trim(F.col("HOI_PROD_CMPNT_PFX_ID"))) == 0, F.lit(None)).otherwise(F.col("IP_HOSP_CYM_COPAY_AMT")).alias("IP_HOSP_CYM_COPAY_AMT"),
    F.when(F.length(trim(F.col("IFS_PROD_CMPNT_PFX_ID"))) == 0, F.lit(None)).otherwise(F.col("INFRTL_LFTM_MAX_AMT")).alias("INFRTL_LFTM_MAX_AMT"),
    F.when(F.length(trim(F.col("MHIK_PROD_CMPNT_PFX_ID"))) == 0, F.lit(None)).otherwise(F.col("KS_KS_MNTL_HLTH_IP_MAX_VST_AMT")).alias("KS_KS_MNTL_HLTH_IP_MAX_VST_AMT"),
    F.when(F.length(trim(F.col("MHKK_PROD_CMPNT_PFX_ID"))) == 0, F.lit(None)).otherwise(F.col("KS_KS_MNTL_HLTH_MAX_VST_AMT")).alias("KS_KS_MNTL_HLTH_MAX_VST_AMT"),
    F.when(F.length(trim(F.col("MII_PROD_CMPNT_PFX_ID"))) == 0, F.lit(None)).otherwise(F.col("MHLTH_IP_IN_NTWK_CYM_COPAY_AMT")).alias("MHLTH_IP_IN_NTWK_CYM_COPAY_AMT"),
    F.when(F.length(trim(F.col("OPH_PROD_CMPNT_PFX_ID"))) == 0, F.lit(None)).otherwise(F.col("OP_HOSP_CYM_COPAY_AMT")).alias("OP_HOSP_CYM_COPAY_AMT"),
    F.when(F.length(trim(F.col("STO_PROD_CMPNT_PFX_ID"))) == 0, F.lit(None)).otherwise(F.col("OP_SPCH_THER_SVC_MAX_AMT")).alias("OP_SPCH_THER_SVC_MAX_AMT"),
    F.when(F.length(trim(F.col("MHDK_PROD_CMPNT_PFX_ID"))) == 0, F.lit(None)).otherwise(F.col("PPO_KS_DIR_ENR_MHLTH_MAX_AMT")).alias("PPO_KS_DIR_ENR_MHLTH_MAX_AMT"),
    F.when(F.length(trim(F.col("RTNM_PROD_CMPNT_PFX_ID"))) == 0, F.lit(None)).otherwise(F.col("RTN_MAX_AMT")).alias("RTN_MAX_AMT"),
    F.when(F.length(trim(F.col("SAIP_PROD_CMPNT_PFX_ID"))) == 0, F.lit(None)).otherwise(F.col("SBSTNC_ABUSE_IP_MAX_AMT")).alias("SBSTNC_ABUSE_IP_MAX_AMT"),
    F.when(F.length(trim(F.col("SNF_PROD_CMPNT_PFX_ID"))) == 0, F.lit(None)).otherwise(F.col("SKILL_NURSE_CYM_AMT")).alias("SKILL_NURSE_CYM_AMT"),
    F.when(F.length(trim(F.col("CHIR_PROD_CMPNT_PFX_ID"))) == 0, F.lit(None)).otherwise(F.col("CHIRO_CYM_VST_CT").cast(IntegerType())).alias("CHIRO_CYM_VST_CT"),
    F.when(F.length(trim(F.col("PTOT_PROD_CMPNT_PFX_ID"))) == 0, F.lit(None)).otherwise(F.col("COMBND_PT_OT_CYM_VST_CT").cast(IntegerType())).alias("COMBND_PT_OT_CYM_VST_CT"),
    F.when(F.length(trim(F.col("DEXT_PROD_CMPNT_PFX_ID"))) == 0, F.lit(None)).otherwise(F.col("DETOX_DAYS_MAX_VST_CT").cast(IntegerType())).alias("DETOX_DAYS_MAX_VST_CT"),
    F.when(F.length(trim(F.col("HHCV_PROD_CMPNT_PFX_ID"))) == 0, F.lit(None)).otherwise(F.col("HOME_HLTH_CYM_VST_CT").cast(IntegerType())).alias("HOME_HLTH_CYM_VST_CT"),
    F.when(F.length(trim(F.col("HOSI_PROD_CMPNT_PFX_ID"))) == 0, F.lit(None)).otherwise(F.col("IP_HSPC_LFTM_MAX_VST_CT")).alias("IP_HSPC_LFTM_MAX_VST_CT"),
    F.when(F.length(trim(F.col("REHB_PROD_CMPNT_PFX_ID"))) == 0, F.lit(None)).otherwise(F.col("OP_REHAB_MAX_VST_CT").cast(IntegerType())).alias("OP_REHAB_MAX_VST_CT"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("EDWRunCycle").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

df_xfm_BusinessLogic_na = df_xfm_BusinessLogic_na_temp.select(
    F.lit(1).alias("PROD_LMT_SUM_SK"),
    F.lit("NA").alias("SRC_SYS_CD"),
    F.lit("NA").alias("PROD_ID"),
    F.lit("1753-01-01").alias("PROD_CMPNT_EFF_DT_SK"),
    F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("EDWRunCycleDate").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(1).alias("PROD_SK"),
    F.lit("1753-01-01").alias("PROD_CMPNT_TERM_DT_SK"),
    F.lit(None).alias("DME_MAX_AMT"),
    F.lit(None).alias("IN_OUT_NTWK_LFTM_MAX_AMT"),
    F.lit(None).alias("IP_HOSP_CYM_COPAY_AMT"),
    F.lit(None).alias("INFRTL_LFTM_MAX_AMT"),
    F.lit(None).alias("KS_KS_MNTL_HLTH_IP_MAX_VST_AMT"),
    F.lit(None).alias("KS_KS_MNTL_HLTH_MAX_VST_AMT"),
    F.lit(None).alias("MHLTH_IP_IN_NTWK_CYM_COPAY_AMT"),
    F.lit(None).alias("OP_HOSP_CYM_COPAY_AMT"),
    F.lit(None).alias("OP_SPCH_THER_SVC_MAX_AMT"),
    F.lit(None).alias("PPO_KS_DIR_ENR_MHLTH_MAX_AMT"),
    F.lit(None).alias("RTN_MAX_AMT"),
    F.lit(None).alias("SBSTNC_ABUSE_IP_MAX_AMT"),
    F.lit(None).alias("SKILL_NURSE_CYM_AMT"),
    F.lit(None).alias("CHIRO_CYM_VST_CT"),
    F.lit(None).alias("COMBND_PT_OT_CYM_VST_CT"),
    F.lit(None).alias("DETOX_DAYS_MAX_VST_CT"),
    F.lit(None).alias("HOME_HLTH_CYM_VST_CT"),
    F.lit(None).alias("IP_HSPC_LFTM_MAX_VST_CT"),
    F.lit(None).alias("OP_REHAB_MAX_VST_CT"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("EDWRunCycle").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

df_xfm_BusinessLogic_unk = df_xfm_BusinessLogic_unk_temp.select(
    F.lit(0).alias("PROD_LMT_SUM_SK"),
    F.lit("UNK").alias("SRC_SYS_CD"),
    F.lit("UNK").alias("PROD_ID"),
    F.lit("1753-01-01").alias("PROD_CMPNT_EFF_DT_SK"),
    F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("EDWRunCycleDate").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(0).alias("PROD_SK"),
    F.lit("1753-01-01").alias("PROD_CMPNT_TERM_DT_SK"),
    F.lit(None).alias("DME_MAX_AMT"),
    F.lit(None).alias("IN_OUT_NTWK_LFTM_MAX_AMT"),
    F.lit(None).alias("IP_HOSP_CYM_COPAY_AMT"),
    F.lit(None).alias("INFRTL_LFTM_MAX_AMT"),
    F.lit(None).alias("KS_KS_MNTL_HLTH_IP_MAX_VST_AMT"),
    F.lit(None).alias("KS_KS_MNTL_HLTH_MAX_VST_AMT"),
    F.lit(None).alias("MHLTH_IP_IN_NTWK_CYM_COPAY_AMT"),
    F.lit(None).alias("OP_HOSP_CYM_COPAY_AMT"),
    F.lit(None).alias("OP_SPCH_THER_SVC_MAX_AMT"),
    F.lit(None).alias("PPO_KS_DIR_ENR_MHLTH_MAX_AMT"),
    F.lit(None).alias("RTN_MAX_AMT"),
    F.lit(None).alias("SBSTNC_ABUSE_IP_MAX_AMT"),
    F.lit(None).alias("SKILL_NURSE_CYM_AMT"),
    F.lit(None).alias("CHIRO_CYM_VST_CT"),
    F.lit(None).alias("COMBND_PT_OT_CYM_VST_CT"),
    F.lit(None).alias("DETOX_DAYS_MAX_VST_CT"),
    F.lit(None).alias("HOME_HLTH_CYM_VST_CT"),
    F.lit(None).alias("IP_HSPC_LFTM_MAX_VST_CT"),
    F.lit(None).alias("OP_REHAB_MAX_VST_CT"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("EDWRunCycle").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

df_fnl_dataLinks = df_xfm_BusinessLogic_na.unionByName(df_xfm_BusinessLogic_unk).unionByName(df_xfm_BusinessLogic_main)

df_final = df_fnl_dataLinks.select(
    F.col("PROD_LMT_SUM_SK"),
    F.col("SRC_SYS_CD"),
    F.col("PROD_ID"),
    F.rpad(F.col("PROD_CMPNT_EFF_DT_SK"), 10, " ").alias("PROD_CMPNT_EFF_DT_SK"),
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("PROD_SK"),
    F.rpad(F.col("PROD_CMPNT_TERM_DT_SK"), 10, " ").alias("PROD_CMPNT_TERM_DT_SK"),
    F.col("DME_MAX_AMT"),
    F.col("IN_OUT_NTWK_LFTM_MAX_AMT"),
    F.col("IP_HOSP_CYM_COPAY_AMT"),
    F.col("INFRTL_LFTM_MAX_AMT"),
    F.col("KS_KS_MNTL_HLTH_IP_MAX_VST_AMT"),
    F.col("KS_KS_MNTL_HLTH_MAX_VST_AMT"),
    F.col("MHLTH_IP_IN_NTWK_CYM_COPAY_AMT"),
    F.col("OP_HOSP_CYM_COPAY_AMT"),
    F.col("OP_SPCH_THER_SVC_MAX_AMT"),
    F.col("PPO_KS_DIR_ENR_MHLTH_MAX_AMT"),
    F.col("RTN_MAX_AMT"),
    F.col("SBSTNC_ABUSE_IP_MAX_AMT"),
    F.col("SKILL_NURSE_CYM_AMT"),
    F.col("CHIRO_CYM_VST_CT"),
    F.col("COMBND_PT_OT_CYM_VST_CT"),
    F.col("DETOX_DAYS_MAX_VST_CT"),
    F.col("HOME_HLTH_CYM_VST_CT"),
    F.col("IP_HSPC_LFTM_MAX_VST_CT"),
    F.col("OP_REHAB_MAX_VST_CT"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

write_files(
    df_final,
    "PROD_LMT_SUM_F.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)