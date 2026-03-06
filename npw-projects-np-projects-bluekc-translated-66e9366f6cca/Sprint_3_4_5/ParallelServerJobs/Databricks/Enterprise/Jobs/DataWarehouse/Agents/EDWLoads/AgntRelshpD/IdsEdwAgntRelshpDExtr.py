# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC JOB NAME:     EdwAgntRelExtr
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   Pulls data from ids and edw  
# MAGIC       
# MAGIC 
# MAGIC INPUTS:
# MAGIC 	IDS - AGNT_RELSHP
# MAGIC   
# MAGIC HASH FILES:
# MAGIC                 hf_cdma_codes         - CDMA code table - resolve code lookups
# MAGIC                
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                   TRIM all the fields that are extracted or used in lookups
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                   IDS - read from source, lookup all code SK values and get the natural codes
# MAGIC                 
# MAGIC   
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                     AGNT_RELSHP__D.dat
# MAGIC 
# MAGIC MODIFICATIONS:                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        
# MAGIC                                                                                                                                                                                                                            
# MAGIC DEVELOPER                          DATE                  PROJECT                                                    DESCRIPTION                                                   DATASTAGE                                        CODE                                      DATE 
# MAGIC                                                                                                                                                                                                                            ENVIRONMENT                                   REVIEWER                             REVIEW
# MAGIC ----------------------------------------       --------------------        -------------------------------------------------                   -----------------------------------------------------------                 -----------------------------------------------               ------------------------------                   --------------------
# MAGIC               Ralph Tucker  12/28/2005    Originally Programmed
# MAGIC 
# MAGIC               Rama Kamjula   12/10/2013                    #5114                                                    Rewritten from Server to Parallel version                EnterpriseWrhsDevl                              Jag Yelavarthi                           2013-12-22
# MAGIC 
# MAGIC               Krishnakanth          2017-08-10                 30001                                                    Added the columns AGNT_SK, REL_AGNT_SK   EnterpriseDev3                                    Jag Yelavarthi                           2017-08-14                     
# MAGIC                    Manivannan                                                                                                       to the table AGNT_RELSHP_D in EDW

# MAGIC JobName: IdsEdwAgntRelshpDExtr
# MAGIC 
# MAGIC This Job creates load file for AGENT RELSHP to load into EDW
# MAGIC IDS Extract for AGNT_RELSHP
# MAGIC Load file for AGNT_RELSHP_D
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
CurrRunCycle = get_widget_value('CurrRunCycle','')
CurrRunCycleDate = get_widget_value('CurrRunCycleDate','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

query_db2_AGNT_RELSHP = f"""
SELECT 
 AGNT_RELSHP.AGNT_RELSHP_SK,
 COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
 AGNT_RELSHP.AGNT_ID,
 AGNT_RELSHP.REL_AGNT_ID,
 AGNT_RELSHP.EFF_DT_SK,
 AGNT_RELSHP.CRT_RUN_CYC_EXCTN_SK,
 AGNT_RELSHP.LAST_UPDT_RUN_CYC_EXCTN_SK,
 AGNT_RELSHP.AGNT_RELSHP_TERM_RSN_CD_SK,
 AGNT_RELSHP.TERM_DT_SK,
 AGNT_RELSHP.AGNT_SK,
 AGNT_RELSHP.REL_AGNT_SK
FROM {IDSOwner}.AGNT_RELSHP AGNT_RELSHP
LEFT JOIN {IDSOwner}.CD_MPPNG CD
   ON AGNT_RELSHP.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
"""

df_db2_AGNT_RELSHP = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_db2_AGNT_RELSHP)
    .load()
)

query_db2_CD_MPPNG = f"""
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
    .option("query", query_db2_CD_MPPNG)
    .load()
)

df_lkp_Codes = df_db2_AGNT_RELSHP.alias("lnk_AgntRelshp_In").join(
    df_db2_CD_MPPNG.alias("lnk_TermRel"),
    (F.col("lnk_AgntRelshp_In.AGNT_RELSHP_TERM_RSN_CD_SK") == F.col("lnk_TermRel.CD_MPPNG_SK")),
    how="left"
)

df_lnk_AgntRelshp = df_lkp_Codes.select(
    F.col("lnk_AgntRelshp_In.AGNT_RELSHP_SK").alias("AGNT_RELSHP_SK"),
    F.col("lnk_AgntRelshp_In.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_AgntRelshp_In.AGNT_ID").alias("AGNT_ID"),
    F.col("lnk_AgntRelshp_In.REL_AGNT_ID").alias("REL_AGNT_ID"),
    F.col("lnk_AgntRelshp_In.EFF_DT_SK").alias("EFF_DT_SK"),
    F.col("lnk_AgntRelshp_In.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_AgntRelshp_In.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_AgntRelshp_In.AGNT_RELSHP_TERM_RSN_CD_SK").alias("AGNT_RELSHP_TERM_RSN_CD_SK"),
    F.col("lnk_AgntRelshp_In.TERM_DT_SK").alias("TERM_DT_SK"),
    F.col("lnk_TermRel.TRGT_CD").alias("TRGT_CD_TermRel"),
    F.col("lnk_TermRel.TRGT_CD_NM").alias("TRGT_CD_NM_TermRel"),
    F.col("lnk_AgntRelshp_In.AGNT_SK").alias("AGNT_SK"),
    F.col("lnk_AgntRelshp_In.REL_AGNT_SK").alias("REL_AGNT_SK")
)

df_lnk_AgntRelshp_Out = df_lnk_AgntRelshp.filter(
    (F.col("AGNT_RELSHP_SK") != 0) & (F.col("AGNT_RELSHP_SK") != 1)
).select(
    F.col("AGNT_RELSHP_SK").alias("AGNT_RELSHP_SK"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("AGNT_ID").alias("AGNT_ID"),
    F.col("REL_AGNT_ID").alias("REL_AGNT_ID"),
    F.col("EFF_DT_SK").alias("AGNT_RELSHP_EFF_DT_SK"),
    F.lit(CurrRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(CurrRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("TERM_DT_SK").alias("AGNT_RELSHP_TERM_DT_SK"),
    F.when(
        (F.col("TRGT_CD_TermRel").isNull()) | (trim(F.col("TRGT_CD_TermRel")) == ""),
        F.lit("")
    ).otherwise(F.col("TRGT_CD_TermRel")).alias("AGNT_RELSHP_TERM_RSN_CD"),
    F.when(
        (F.col("TRGT_CD_NM_TermRel").isNull()) | (trim(F.col("TRGT_CD_NM_TermRel")) == ""),
        F.lit("")
    ).otherwise(F.col("TRGT_CD_NM_TermRel")).alias("AGNT_RELSHP_TERM_RSN_NM"),
    F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("AGNT_RELSHP_TERM_RSN_CD_SK").alias("AGNT_RELSHP_TERM_RSN_CD_SK"),
    F.col("AGNT_SK").alias("AGNT_SK"),
    F.col("REL_AGNT_SK").alias("REL_AGNT_SK")
)

df_lnk_NA = df_lnk_AgntRelshp.limit(1).select(
    F.lit(1).alias("AGNT_RELSHP_SK"),
    F.lit("NA").alias("SRC_SYS_CD"),
    F.lit("NA").alias("AGNT_ID"),
    F.lit("NA").alias("REL_AGNT_ID"),
    F.lit("1753-01-01").alias("AGNT_RELSHP_EFF_DT_SK"),
    F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("1753-01-01").alias("AGNT_RELSHP_TERM_DT_SK"),
    F.lit("NA").alias("AGNT_RELSHP_TERM_RSN_CD"),
    F.lit("NA").alias("AGNT_RELSHP_TERM_RSN_NM"),
    F.lit("100").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit("100").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit("1").alias("AGNT_RELSHP_TERM_RSN_CD_SK"),
    F.lit("1").alias("AGNT_SK"),
    F.lit("1").alias("REL_AGNT_SK")
)

df_lnk_UNK = df_lnk_AgntRelshp.limit(1).select(
    F.lit(0).alias("AGNT_RELSHP_SK"),
    F.lit("UNK").alias("SRC_SYS_CD"),
    F.lit("UNK").alias("AGNT_ID"),
    F.lit("UNK").alias("REL_AGNT_ID"),
    F.lit("1753-01-01").alias("AGNT_RELSHP_EFF_DT_SK"),
    F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("1753-01-01").alias("AGNT_RELSHP_TERM_DT_SK"),
    F.lit("UNK").alias("AGNT_RELSHP_TERM_RSN_CD"),
    F.lit("UNK").alias("AGNT_RELSHP_TERM_RSN_NM"),
    F.lit("100").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit("100").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit("0").alias("AGNT_RELSHP_TERM_RSN_CD_SK"),
    F.lit("0").alias("AGNT_SK"),
    F.lit("0").alias("REL_AGNT_SK")
)

df_fnl_Combine = df_lnk_AgntRelshp_Out.unionByName(df_lnk_NA).unionByName(df_lnk_UNK)

df_final = df_fnl_Combine \
    .withColumn("AGNT_RELSHP_EFF_DT_SK", F.rpad(F.col("AGNT_RELSHP_EFF_DT_SK"), 10, " ")) \
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ")) \
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ")) \
    .withColumn("AGNT_RELSHP_TERM_DT_SK", F.rpad(F.col("AGNT_RELSHP_TERM_DT_SK"), 10, " ")) \
    .select(
        "AGNT_RELSHP_SK",
        "SRC_SYS_CD",
        "AGNT_ID",
        "REL_AGNT_ID",
        "AGNT_RELSHP_EFF_DT_SK",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "AGNT_RELSHP_TERM_DT_SK",
        "AGNT_RELSHP_TERM_RSN_CD",
        "AGNT_RELSHP_TERM_RSN_NM",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "AGNT_RELSHP_TERM_RSN_CD_SK",
        "AGNT_SK",
        "REL_AGNT_SK"
    )

write_files(
    df_final,
    f"{adls_path}/load/AGNT_RELSHP_D.dat",
    ",",
    "overwrite",
    False,
    False,
    "^",
    None
)