# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2009 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY:  
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #                             Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------                             -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Pooja Sunkara          2014-09-16         5345                                               Original Programming.                                                                IntegrateWrhsDevl       Kalyan Neelam          2015-01-16
# MAGIC 
# MAGIC Ravi Abburi            2018-01-04            5781                                       Added the NTNL_PROV_SK lookup Hashed file                              IntegrateDev2              Kalyan Neelam          2018-04-19

# MAGIC Do not clear hash files
# MAGIC This Job runs daily after IDS Parallel Provider Controller run for creating a hash file that will be used in other jobs / other subject areas.
# MAGIC hf_ntnl_prov File is used in GetFkeyNtnlProvId routine which is used in the 
# MAGIC IdsProvFkey,
# MAGIC IdsProvDEAFkey,
# MAGIC IdsClmProvFkey 
# MAGIC to get the NTNL_PROV_SK value to load into the PROV table.
# MAGIC IDS_PROVIDER K Table to Hash File Load.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

# Stage: IDS_K_CMN_PRCT
extract_query_IDS_K_CMN_PRCT = f"""
SELECT
  SRC_SYS_CD,
  CMN_PRCT_ID,
  CRT_RUN_CYC_EXCTN_SK,
  CMN_PRCT_SK
FROM {IDSOwner}.K_CMN_PRCT
WHERE CMN_PRCT_SK NOT IN (0,1)
"""
df_IDS_K_CMN_PRCT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_IDS_K_CMN_PRCT)
    .load()
)

# Stage: Xfrm
df_xfrm = df_IDS_K_CMN_PRCT.select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("CMN_PRCT_ID").alias("CMN_PRCT_ID"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("CMN_PRCT_SK").alias("CMN_PRCT_SK")
)

# Stage: hf_cmn_prct (CHashedFileStage, Scenario C)
df_hf_cmn_prct = df_xfrm.select(
    col("SRC_SYS_CD"),
    rpad(col("CMN_PRCT_ID"), 20, " ").alias("CMN_PRCT_ID"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("CMN_PRCT_SK")
)
write_files(
    df_hf_cmn_prct,
    "hf_cmn_prct.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

# Stage: IDS_K_FCLTY_TYP_CD
extract_query_IDS_K_FCLTY_TYP_CD = f"""
SELECT
  FCLTY_TYP_CD,
  CRT_RUN_CYC_EXCTN_SK,
  FCLTY_TYP_CD_SK
FROM {IDSOwner}.K_FCLTY_TYP_CD
WHERE FCLTY_TYP_CD_SK NOT IN (0,1)
"""
df_IDS_K_FCLTY_TYP_CD = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_IDS_K_FCLTY_TYP_CD)
    .load()
)

# Stage: hf_fclty_typ_cd (CHashedFileStage, Scenario C)
df_hf_fclty_typ_cd = df_IDS_K_FCLTY_TYP_CD.select(
    col("FCLTY_TYP_CD"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("FCLTY_TYP_CD_SK")
)
write_files(
    df_hf_fclty_typ_cd,
    "hf_fclty_typ_cd.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

# Stage: IDS_K_NTWK
extract_query_IDS_K_NTWK = f"""
SELECT
  SRC_SYS_CD,
  NTWK_ID,
  CRT_RUN_CYC_EXCTN_SK,
  NTWK_SK
FROM {IDSOwner}.K_NTWK
WHERE NTWK_SK NOT IN (0,1)
"""
df_IDS_K_NTWK = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_IDS_K_NTWK)
    .load()
)

# Stage: hf_ntwk (CHashedFileStage, Scenario C)
df_hf_ntwk = df_IDS_K_NTWK.select(
    col("SRC_SYS_CD"),
    col("NTWK_ID"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("NTWK_SK")
)
write_files(
    df_hf_ntwk,
    "hf_ntwk.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

# Stage: IDS_K_PROV
extract_query_IDS_K_PROV = f"""
SELECT
  SRC_SYS_CD,
  PROV_ID,
  CRT_RUN_CYC_EXCTN_SK,
  PROV_SK
FROM {IDSOwner}.K_PROV
WHERE PROV_SK NOT IN (0,1)
"""
df_IDS_K_PROV = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_IDS_K_PROV)
    .load()
)

# Stage: hf_prov (CHashedFileStage, Scenario C)
df_hf_prov = df_IDS_K_PROV.select(
    col("SRC_SYS_CD"),
    col("PROV_ID"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("PROV_SK")
)
write_files(
    df_hf_prov,
    "hf_prov.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

# Stage: IDS_K_PROV_LOC
extract_query_IDS_K_PROV_LOC = f"""
SELECT
  SRC_SYS_CD,
  PROV_ID,
  PROV_ADDR_ID,
  PROV_ADDR_TYP_CD,
  PROV_ADDR_EFF_DT_SK AS PROV_ADDR_EFF_DT,
  CRT_RUN_CYC_EXCTN_SK,
  PROV_LOC_SK
FROM {IDSOwner}.K_PROV_LOC
WHERE PROV_LOC_SK NOT IN (0,1)
"""
df_IDS_K_PROV_LOC = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_IDS_K_PROV_LOC)
    .load()
)

# Stage: hf_prov_loc (CHashedFileStage, Scenario C)
df_hf_prov_loc = df_IDS_K_PROV_LOC.select(
    col("SRC_SYS_CD"),
    col("PROV_ID"),
    col("PROV_ADDR_ID"),
    col("PROV_ADDR_TYP_CD"),
    col("PROV_ADDR_EFF_DT"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("PROV_LOC_SK")
)
write_files(
    df_hf_prov_loc,
    "hf_prov_loc.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

# Stage: IDS_K_PROV_TYP_CD
extract_query_IDS_K_PROV_TYP_CD = f"""
SELECT
  PROV_TYP_CD,
  CRT_RUN_CYC_EXCTN_SK,
  PROV_TYP_CD_SK
FROM {IDSOwner}.K_PROV_TYP_CD
WHERE PROV_TYP_CD_SK NOT IN (0,1)
"""
df_IDS_K_PROV_TYP_CD = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_IDS_K_PROV_TYP_CD)
    .load()
)

# Stage: hf_prov_typ_cd (CHashedFileStage, Scenario C)
df_hf_prov_typ_cd = df_IDS_K_PROV_TYP_CD.select(
    col("PROV_TYP_CD"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("PROV_TYP_CD_SK")
)
write_files(
    df_hf_prov_typ_cd,
    "hf_prov_typ_cd.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

# Stage: IDS_K_PROV_SPEC_CD
extract_query_IDS_K_PROV_SPEC_CD = f"""
SELECT
  PROV_SPEC_CD,
  CRT_RUN_CYC_EXCTN_SK,
  PROV_SPEC_CD_SK
FROM {IDSOwner}.K_PROV_SPEC_CD
WHERE PROV_SPEC_CD_SK NOT IN (0,1)
"""
df_IDS_K_PROV_SPEC_CD = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_IDS_K_PROV_SPEC_CD)
    .load()
)

# Stage: hf_prov_spec_cd (CHashedFileStage, Scenario C)
df_hf_prov_spec_cd = df_IDS_K_PROV_SPEC_CD.select(
    col("PROV_SPEC_CD"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("PROV_SPEC_CD_SK")
)
write_files(
    df_hf_prov_spec_cd,
    "hf_prov_spec_cd.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

# Stage: IDS_K_PROV_ADDR
extract_query_IDS_K_PROV_ADDR = f"""
SELECT
  SRC_SYS_CD,
  PROV_ADDR_ID,
  PROV_ADDR_TYP_CD,
  PROV_ADDR_EFF_DT_SK,
  CRT_RUN_CYC_EXCTN_SK,
  PROV_ADDR_SK
FROM {IDSOwner}.K_PROV_ADDR
WHERE PROV_ADDR_SK NOT IN (0,1)
"""
df_IDS_K_PROV_ADDR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_IDS_K_PROV_ADDR)
    .load()
)

# Stage: hf_prov_addr (CHashedFileStage, Scenario C)
df_hf_prov_addr = df_IDS_K_PROV_ADDR.select(
    col("SRC_SYS_CD"),
    col("PROV_ADDR_ID"),
    col("PROV_ADDR_TYP_CD"),
    col("PROV_ADDR_EFF_DT_SK"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("PROV_ADDR_SK")
)
write_files(
    df_hf_prov_addr,
    "hf_prov_addr.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

# Stage: IDS_K_TAX_DMGRPHC
extract_query_IDS_K_TAX_DMGRPHC = f"""
SELECT
  SRC_SYS_CD,
  TAX_ID,
  CRT_RUN_CYC_EXCTN_SK,
  TAX_DMGRPHC_SK
FROM {IDSOwner}.K_TAX_DMGRPHC
WHERE TAX_DMGRPHC_SK NOT IN (0,1)
"""
df_IDS_K_TAX_DMGRPHC = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_IDS_K_TAX_DMGRPHC)
    .load()
)

# Stage: hf_tax_dmgrphc (CHashedFileStage, Scenario C)
df_hf_tax_dmgrphc = df_IDS_K_TAX_DMGRPHC.select(
    col("SRC_SYS_CD"),
    col("TAX_ID"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("TAX_DMGRPHC_SK")
)
write_files(
    df_hf_tax_dmgrphc,
    "hf_tax_dmgrphc.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

# Stage: IDS_K_NTNL_PROV
extract_query_IDS_K_NTNL_PROV = f"""
SELECT
  NTNL_PROV_ID,
  SRC_SYS_CD,
  CRT_RUN_CYC_EXCTN_SK,
  NTNL_PROV_SK
FROM {IDSOwner}.K_NTNL_PROV
WHERE NTNL_PROV_SK NOT IN (0,1)
"""
df_IDS_K_NTNL_PROV = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_IDS_K_NTNL_PROV)
    .load()
)

# Stage: hf_ntnl_prov (CHashedFileStage, Scenario C)
df_hf_ntnl_prov = df_IDS_K_NTNL_PROV.select(
    col("NTNL_PROV_ID"),
    col("SRC_SYS_CD"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("NTNL_PROV_SK")
)
write_files(
    df_hf_ntnl_prov,
    "hf_ntnl_prov.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)