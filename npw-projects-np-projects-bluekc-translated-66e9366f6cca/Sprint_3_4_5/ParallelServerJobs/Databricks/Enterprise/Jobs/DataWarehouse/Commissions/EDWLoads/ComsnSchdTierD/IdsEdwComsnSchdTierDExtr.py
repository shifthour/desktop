# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC JOB NAME:     EdwComsnSchdTierDExtr
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   Pulls data from ids fee discount table COMSN_SCHD and loads to EDW
# MAGIC   Does not keep history.
# MAGIC       
# MAGIC 
# MAGIC INPUTS:
# MAGIC 	IDS:     COMSN_SCHD_TIER
# MAGIC                 EDW:  COMSN_SCHD_TIER_D                        
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                   TRIM all the fields that are extracted or used in lookups
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                   IDS - read from records from the source that have a run cycle greater than the BeginCycle, lookup all code SK values and get the natural codes
# MAGIC  
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                   An EDW load file.   Since no history, then the output is a load file to update the EDW table.
# MAGIC 
# MAGIC MODIFICATIONS:                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        
# MAGIC                                                                                                                                                                                                                            
# MAGIC DEVELOPER                          DATE                  PROJECT                                                    DESCRIPTION                                                   DATASTAGE                                        CODE                                      DATE 
# MAGIC                                                                                                                                                                                                                            ENVIRONMENT                                   REVIEWER                             REVIEW
# MAGIC ----------------------------------------       --------------------        -------------------------------------------------                   -----------------------------------------------------------                 -----------------------------------------------               ------------------------------                   --------------------
# MAGIC               Sharon Andrew 11/07/2005  -                                                                                 Originally Programmed
# MAGIC 
# MAGIC               Rama Kamjula    12/10/2013                                                                                Rewritten from server to parallel version                  EnterpriseWrhsDevl                               Jag Yelavarthi                         2013-12-22

# MAGIC JobName: IdsEdwComsnSchdTierDExtr
# MAGIC 
# MAGIC This Job creates load file for ComsnSchdTierD  to load into EDW
# MAGIC Extract data from COMSN_SCHD_TIER from IDS table
# MAGIC Null Handling and business logic
# MAGIC creates load file for COMSN_SCHD_TIER_D
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DoubleType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
CurrRunCycleDate = get_widget_value('CurrRunCycleDate','')
RunID = get_widget_value('RunID','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query_db2_COMSN_SCHD_TIER = f"""
SELECT
 TIER.COMSN_SCHD_TIER_SK,
 COALESCE(CD.TRGT_CD, 'UNK') AS SRC_SYS_CD,
 TIER.COMSN_SCHD_ID,
 TIER.DURATN_EFF_DT_SK,
 TIER.DURATN_STRT_PERD_NO,
 TIER.PRM_FROM_THRSHLD_AMT,
 TIER.CRT_RUN_CYC_EXCTN_SK,
 TIER.LAST_UPDT_RUN_CYC_EXCTN_SK,
 TIER.COMSN_SCHD_SK,
 TIER.COMSN_SCHD_TIER_CALCMETH_CD_SK,
 TIER.PRM_THRU_THRESOLD_AMT,
 TIER.TIER_AMT,
 TIER.TIER_PCT,
 TIER.TIER_UNIQ_KEY
FROM {IDSOwner}.COMSN_SCHD_TIER TIER
LEFT JOIN {IDSOwner}.CD_MPPNG CD ON CD.CD_MPPNG_SK = TIER.SRC_SYS_CD_SK
"""

df_db2_COMSN_SCHD_TIER = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_COMSN_SCHD_TIER)
    .load()
)

extract_query_db2_CD_MPPNG = f"""
SELECT
 CD_MPPNG_SK,
 TRGT_CD,
 TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG
"""

df_db2_CD_MPPNG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CD_MPPNG)
    .load()
)

df_lkp_Codes = df_db2_COMSN_SCHD_TIER.alias("lnk_ComsnSchdTier").join(
    df_db2_CD_MPPNG.alias("lnk_CalcMethod"),
    F.col("lnk_ComsnSchdTier.COMSN_SCHD_TIER_CALCMETH_CD_SK") == F.col("lnk_CalcMethod.CD_MPPNG_SK"),
    how="left"
)

df_lkp_Codes_out = df_lkp_Codes.select(
    F.col("lnk_ComsnSchdTier.COMSN_SCHD_TIER_SK").alias("COMSN_SCHD_TIER_SK"),
    F.col("lnk_ComsnSchdTier.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_ComsnSchdTier.COMSN_SCHD_ID").alias("COMSN_SCHD_ID"),
    F.col("lnk_ComsnSchdTier.DURATN_EFF_DT_SK").alias("DURATN_EFF_DT_SK"),
    F.col("lnk_ComsnSchdTier.DURATN_STRT_PERD_NO").alias("DURATN_STRT_PERD_NO"),
    F.col("lnk_ComsnSchdTier.PRM_FROM_THRSHLD_AMT").alias("PRM_FROM_THRSHLD_AMT"),
    F.col("lnk_ComsnSchdTier.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_ComsnSchdTier.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_ComsnSchdTier.COMSN_SCHD_SK").alias("COMSN_SCHD_SK"),
    F.col("lnk_ComsnSchdTier.COMSN_SCHD_TIER_CALCMETH_CD_SK").alias("COMSN_SCHD_TIER_CALCMETH_CD_SK"),
    F.col("lnk_ComsnSchdTier.PRM_THRU_THRESOLD_AMT").alias("PRM_THRU_THRESOLD_AMT"),
    F.col("lnk_ComsnSchdTier.TIER_AMT").alias("TIER_AMT"),
    F.col("lnk_ComsnSchdTier.TIER_PCT").alias("TIER_PCT"),
    F.col("lnk_ComsnSchdTier.TIER_UNIQ_KEY").alias("TIER_UNIQ_KEY"),
    F.col("lnk_CalcMethod.TRGT_CD").alias("TRGT_CD"),
    F.col("lnk_CalcMethod.TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_ComsnSchdTierD = df_lkp_Codes_out.filter(
    (F.col("COMSN_SCHD_TIER_SK") != 0) & (F.col("COMSN_SCHD_TIER_SK") != 1)
).select(
    F.col("COMSN_SCHD_TIER_SK"),
    F.col("SRC_SYS_CD"),
    F.col("COMSN_SCHD_ID"),
    F.col("DURATN_EFF_DT_SK").alias("COMSN_SCHD_TIER_DUR_EFF_DT_SK"),
    F.col("DURATN_STRT_PERD_NO").alias("COMSN_SCHD_TIER_DURSTRTPERD_NO"),
    F.col("PRM_FROM_THRSHLD_AMT"),
    F.lit(CurrRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(CurrRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("COMSN_SCHD_SK"),
    F.col("TIER_AMT").alias("COMSN_SCHD_TIER_AMT"),
    F.when(
        F.col("TRGT_CD").isNull() | (trim(F.col("TRGT_CD")) == F.lit("")),
        F.lit("UNK")
    ).otherwise(F.col("TRGT_CD")).alias("COMSN_SCHD_TIER_CALC_METH_CD"),
    F.when(
        F.col("TRGT_CD_NM").isNull() | (trim(F.col("TRGT_CD_NM")) == F.lit("")),
        F.lit("UNK")
    ).otherwise(F.col("TRGT_CD_NM")).alias("COMSN_SCHD_TIER_CALC_METH_NM"),
    F.col("TIER_PCT").alias("COMSN_SCHD_TIER_PCT"),
    F.col("PRM_THRU_THRESOLD_AMT").alias("COMSN_SCHD_TIER_PRMTHRUTHR_AMT"),
    F.col("TIER_UNIQ_KEY").alias("COMSN_SCHD_TIER_UNIQ_KEY"),
    F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("COMSN_SCHD_TIER_CALCMETH_CD_SK")
)

df_NA_1 = df_lkp_Codes_out.limit(1)
df_NA = df_NA_1.select(
    F.lit(1).alias("COMSN_SCHD_TIER_SK"),
    F.lit("NA").alias("SRC_SYS_CD"),
    F.lit("NA").alias("COMSN_SCHD_ID"),
    F.lit("1753-01-01").alias("COMSN_SCHD_TIER_DUR_EFF_DT_SK"),
    F.lit(0).alias("COMSN_SCHD_TIER_DURSTRTPERD_NO"),
    F.lit(0).alias("PRM_FROM_THRSHLD_AMT"),
    F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(1).alias("COMSN_SCHD_SK"),
    F.lit(0).alias("COMSN_SCHD_TIER_AMT"),
    F.lit("NA").alias("COMSN_SCHD_TIER_CALC_METH_CD"),
    F.lit("NA").alias("COMSN_SCHD_TIER_CALC_METH_NM"),
    F.lit(0).alias("COMSN_SCHD_TIER_PCT"),
    F.lit(0).alias("COMSN_SCHD_TIER_PRMTHRUTHR_AMT"),
    F.lit(1).alias("COMSN_SCHD_TIER_UNIQ_KEY"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("COMSN_SCHD_TIER_CALCMETH_CD_SK")
)

df_UNK_1 = df_lkp_Codes_out.limit(1)
df_UNK = df_UNK_1.select(
    F.lit(0).alias("COMSN_SCHD_TIER_SK"),
    F.lit("UNK").alias("SRC_SYS_CD"),
    F.lit("UNK").alias("COMSN_SCHD_ID"),
    F.lit("1753-01-01").alias("COMSN_SCHD_TIER_DUR_EFF_DT_SK"),
    F.lit(0).alias("COMSN_SCHD_TIER_DURSTRTPERD_NO"),
    F.lit(0).alias("PRM_FROM_THRSHLD_AMT"),
    F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(0).alias("COMSN_SCHD_SK"),
    F.lit(0).alias("COMSN_SCHD_TIER_AMT"),
    F.lit("UNK").alias("COMSN_SCHD_TIER_CALC_METH_CD"),
    F.lit("UNK").alias("COMSN_SCHD_TIER_CALC_METH_NM"),
    F.lit(0).alias("COMSN_SCHD_TIER_PCT"),
    F.lit(0).alias("COMSN_SCHD_TIER_PRMTHRUTHR_AMT"),
    F.lit(0).alias("COMSN_SCHD_TIER_UNIQ_KEY"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("COMSN_SCHD_TIER_CALCMETH_CD_SK")
)

common_cols = [
    "COMSN_SCHD_TIER_SK",
    "SRC_SYS_CD",
    "COMSN_SCHD_ID",
    "COMSN_SCHD_TIER_DUR_EFF_DT_SK",
    "COMSN_SCHD_TIER_DURSTRTPERD_NO",
    "PRM_FROM_THRSHLD_AMT",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "COMSN_SCHD_SK",
    "COMSN_SCHD_TIER_AMT",
    "COMSN_SCHD_TIER_CALC_METH_CD",
    "COMSN_SCHD_TIER_CALC_METH_NM",
    "COMSN_SCHD_TIER_PCT",
    "COMSN_SCHD_TIER_PRMTHRUTHR_AMT",
    "COMSN_SCHD_TIER_UNIQ_KEY",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "COMSN_SCHD_TIER_CALCMETH_CD_SK"
]

df_fnl_Combine = (
    df_ComsnSchdTierD.select(common_cols)
    .unionByName(df_NA.select(common_cols))
    .unionByName(df_UNK.select(common_cols))
)

df_final = df_fnl_Combine.select(
    F.col("COMSN_SCHD_TIER_SK"),
    F.col("SRC_SYS_CD"),
    F.col("COMSN_SCHD_ID"),
    F.rpad(F.col("COMSN_SCHD_TIER_DUR_EFF_DT_SK"), 10, " ").alias("COMSN_SCHD_TIER_DUR_EFF_DT_SK"),
    F.col("COMSN_SCHD_TIER_DURSTRTPERD_NO"),
    F.col("PRM_FROM_THRSHLD_AMT"),
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("COMSN_SCHD_SK"),
    F.col("COMSN_SCHD_TIER_AMT"),
    F.col("COMSN_SCHD_TIER_CALC_METH_CD"),
    F.col("COMSN_SCHD_TIER_CALC_METH_NM"),
    F.col("COMSN_SCHD_TIER_PCT"),
    F.col("COMSN_SCHD_TIER_PRMTHRUTHR_AMT"),
    F.col("COMSN_SCHD_TIER_UNIQ_KEY"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("COMSN_SCHD_TIER_CALCMETH_CD_SK")
)

write_files(
    df_final,
    f"{adls_path}/load/COMSN_SCHD_TIER_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)