# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:    IdsMbrAlphaPfxExtr
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC                    IDS Membership Data Mart Alpha Prefix extract from IDS to Data Mart.  
# MAGIC 
# MAGIC INPUTS:
# MAGIC                    IDS:  ALPHA_PFX
# MAGIC 
# MAGIC 
# MAGIC HASH FILES:         
# MAGIC                     hf_etrnl_cd_mppng
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                     Lookups to extract fields
# MAGIC              
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                     Extracts from IDS and pulls to the Data Marts.  Perform no changes with the data.
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                     Data Mart:  MBRSH_DM_ALPHA_PFX
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                    Suzanne Saylor:  Original Programming - 02/23/2006
# MAGIC 
# MAGIC 
# MAGIC      
# MAGIC Developer                          Date                    Project/Altiris #                     Change Description                                                       Development Project               Code Reviewer                      Date Reviewed
# MAGIC ---------------------------------      -------------------      -----------------------------------           ---------------------------------------------------------                                 ----------------------------------              ---------------------------------               -------------------------
# MAGIC 
# MAGIC Archana Palivela              03/26/14              5114                                 Original Programming(Server to Parallel)                             IntegrateWrhsDevl                 Bhoomi Dasari                        4/8/2014

# MAGIC Write REF_DM_ALPHA_PFX Data into a Sequential file for Load Job IdsDmClmMartPaymtSumLoad.
# MAGIC Read all the Data from IDS ALPHA_PFX Table;
# MAGIC Add Defaults and Null Handling.
# MAGIC Job Name: 
# MAGIC 
# MAGIC IdsDmRefAlphaPfxExtr
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType, BooleanType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query_db2_ALPHA_PFX_Extr = f"""SELECT
A.ALPHA_PFX_SK,
A.ALPHA_PFX_CD,
A.ALPHA_PFX_INSTUT_CLM_PLN_CD_SK,
A.ALPHA_PFX_PROF_CLM_PLN_CD_SK,
A.ALPHA_PFX_PGM_CD_SK,
A.PLN_PROFL_STD_RULE_IN,
A.ALPHA_PFX_NM
FROM {IDSOwner}.ALPHA_PFX A"""

df_db2_ALPHA_PFX_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_ALPHA_PFX_Extr)
    .load()
)

extract_query_db2_CD_MPPNG_in = f"""SELECT
CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') SRC_CD,
COALESCE(TRGT_CD_NM,'UNK') SRC_CD_NM,
COALESCE(TRGT_CD,'UNK') TRGT_CD,
COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG
"""

df_db2_CD_MPPNG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CD_MPPNG_in)
    .load()
)

df_RefAlphaPfxPgmCd = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM"),
    F.col("SRC_CD").alias("SRC_CD"),
    F.col("SRC_CD_NM").alias("SRC_CD_NM")
)

df_RefAlphaPfxProfClmPlnCd = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM"),
    F.col("SRC_CD").alias("SRC_CD"),
    F.col("SRC_CD_NM").alias("SRC_CD_NM")
)

df_RefAlphaPfxInstutClmPlnCd = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM"),
    F.col("SRC_CD").alias("SRC_CD"),
    F.col("SRC_CD_NM").alias("SRC_CD_NM")
)

df_lkp_Codes = (
    df_db2_ALPHA_PFX_Extr.alias("lnk_IdsDmRefAlphaPfxExtr_InABC")
    .join(
        df_RefAlphaPfxPgmCd.alias("RefAlphaPfxPgmCd"),
        F.col("lnk_IdsDmRefAlphaPfxExtr_InABC.ALPHA_PFX_INSTUT_CLM_PLN_CD_SK")
        == F.col("RefAlphaPfxPgmCd.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_RefAlphaPfxProfClmPlnCd.alias("RefAlphaPfxProfClmPlnCd"),
        F.col("lnk_IdsDmRefAlphaPfxExtr_InABC.ALPHA_PFX_PROF_CLM_PLN_CD_SK")
        == F.col("RefAlphaPfxProfClmPlnCd.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_RefAlphaPfxInstutClmPlnCd.alias("RefAlphaPfxInstutClmPlnCd"),
        F.col("lnk_IdsDmRefAlphaPfxExtr_InABC.ALPHA_PFX_PGM_CD_SK")
        == F.col("RefAlphaPfxInstutClmPlnCd.CD_MPPNG_SK"),
        "left"
    )
)

df_lkp_Codes_DataOut = df_lkp_Codes.select(
    F.col("lnk_IdsDmRefAlphaPfxExtr_InABC.ALPHA_PFX_SK").alias("ALPHA_PFX_SK"),
    F.col("RefAlphaPfxPgmCd.TRGT_CD").alias("ALPHA_PFX_INSTUT_CLM_PLN_CD"),
    F.col("RefAlphaPfxProfClmPlnCd.TRGT_CD").alias("ALPHA_PFX_PROF_CLM_PLN_CD"),
    F.col("RefAlphaPfxInstutClmPlnCd.TRGT_CD").alias("ALPHA_PFX_PGM_CD"),
    F.col("lnk_IdsDmRefAlphaPfxExtr_InABC.PLN_PROFL_STD_RULE_IN").alias("PLN_PROFL_STD_RULE_IN"),
    F.col("lnk_IdsDmRefAlphaPfxExtr_InABC.ALPHA_PFX_NM").alias("ALPHA_PFX_NM"),
    F.col("lnk_IdsDmRefAlphaPfxExtr_InABC.ALPHA_PFX_CD").alias("ALPHA_PFX_CD")
)

df_xfrm_BusinessLogic = df_lkp_Codes_DataOut.select(
    F.col("ALPHA_PFX_CD").alias("ALPHA_PFX_CD"),
    F.col("ALPHA_PFX_SK").alias("ALPHA_PFX_SK"),
    F.when(F.col("ALPHA_PFX_INSTUT_CLM_PLN_CD").isNull(), F.lit(" "))
    .otherwise(F.col("ALPHA_PFX_INSTUT_CLM_PLN_CD"))
    .alias("ALPHA_PFX_INSTUT_CLM_PLN_CD"),
    F.when(F.col("ALPHA_PFX_PROF_CLM_PLN_CD").isNull(), F.lit(" "))
    .otherwise(F.col("ALPHA_PFX_PROF_CLM_PLN_CD"))
    .alias("ALPHA_PFX_PROF_CLM_PLN_CD"),
    F.when(F.col("ALPHA_PFX_PGM_CD").isNull(), F.lit(" "))
    .otherwise(F.col("ALPHA_PFX_PGM_CD"))
    .alias("ALPHA_PFX_PGM_CD"),
    F.col("PLN_PROFL_STD_RULE_IN").alias("PLN_PROFL_STD_RULE_IN"),
    F.col("ALPHA_PFX_NM").alias("ALPHA_PFX_NM"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_NO")
)

df_xfrm_BusinessLogic = df_xfrm_BusinessLogic.withColumn(
    "PLN_PROFL_STD_RULE_IN",
    F.rpad(F.col("PLN_PROFL_STD_RULE_IN"), 1, " ")
)

final_df = df_xfrm_BusinessLogic.select(
    "ALPHA_PFX_CD",
    "ALPHA_PFX_SK",
    "ALPHA_PFX_INSTUT_CLM_PLN_CD",
    "ALPHA_PFX_PROF_CLM_PLN_CD",
    "ALPHA_PFX_PGM_CD",
    "PLN_PROFL_STD_RULE_IN",
    "ALPHA_PFX_NM",
    "LAST_UPDT_RUN_CYC_NO"
)

write_files(
    final_df,
    f"{adls_path}/load/REF_DM_ALPHA_PFX.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)