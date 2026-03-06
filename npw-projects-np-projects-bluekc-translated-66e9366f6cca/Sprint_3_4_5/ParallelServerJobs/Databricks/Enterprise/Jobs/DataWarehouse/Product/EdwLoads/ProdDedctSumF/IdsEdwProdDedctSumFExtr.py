# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:          
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is an extract from Product component table into the PROD_DEDCT_SUM_F table with summation logic added.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               12/05/2007          3044                              Originally Programmed                           devlEDW10                      
# MAGIC             
# MAGIC Leandrew Moore               06/03/2013           5114                             rewrite in parallel                                   Enterprisewarehouse       Pete Marshall       2013-08-08

# MAGIC This is a load ready file conforms to the target table structure.
# MAGIC 
# MAGIC Write this into a Dataset if the data goes into a PKEY job otherwise write this info into a Sequential file
# MAGIC 
# MAGIC Please use Metadata available in Table definitions to support data Lineage.
# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Once the SK is used for denormalization, do not carry oevr that information if it is not needed in the target table.
# MAGIC Read from source table;PROD_CMPNT and CODES 
# MAGIC Apply Run Cycle filters when applicable to get just the needed rows forward
# MAGIC Job:
# MAGIC ProdDedctSumFExtr
# MAGIC Table:
# MAGIC  PROD_DEDCT_SUM_F         
# MAGIC Pull the matched set of products fron IDS and move to EDW.
# MAGIC Partition on src_sys_cd,prod_id,prod_cmpnt_eff_dt_sk.                                                Remove duplicates stage is added in all the Extract jobs to address duplicates situation.
# MAGIC 
# MAGIC Data will be Hash Partitioned and sorted on Natural Keys. Need to choose Keep First record inside RDP stage.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, length, when, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------

# Parameter parsing
IDSOwner = get_widget_value("IDSOwner", "")
ids_secret_name = get_widget_value("ids_secret_name", "")
EDWRunCycleDate = get_widget_value("EDWRunCycleDate", "")
EDWRunCycle = get_widget_value("EDWRunCycle", "")

# Database connection
jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# Stages: db2_ProdCmpnt_Extr
extract_query_db2_ProdCmpnt_Extr = f"""
SELECT
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
df_db2_ProdCmpnt_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_ProdCmpnt_Extr)
    .load()
)

# Stages: db2_RX1GExtr
extract_query_db2_RX1GExtr = f"""
SELECT
BNF_SUM_DTL.PROD_CMPNT_PFX_ID,
BNF_SUM_DTL.DEDCT_AMT
FROM
{IDSOwner}.BNF_SUM_DTL BNF_SUM_DTL,
{IDSOwner}.CD_MPPNG CD_MPPNG
WHERE BNF_SUM_DTL.BNF_SUM_DTL_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
AND CD_MPPNG.TRGT_CD = 'RX1G'
"""
df_db2_RX1GExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_RX1GExtr)
    .load()
)

# Stages: db2_Rx1pExtr
extract_query_db2_Rx1pExtr = f"""
SELECT
BNF_SUM_DTL.PROD_CMPNT_PFX_ID,
BNF_SUM_DTL.DEDCT_AMT
FROM
{IDSOwner}.BNF_SUM_DTL BNF_SUM_DTL,
{IDSOwner}.CD_MPPNG CD_MPPNG
WHERE BNF_SUM_DTL.BNF_SUM_DTL_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
AND CD_MPPNG.TRGT_CD = 'RX1P'
"""
df_db2_Rx1pExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_Rx1pExtr)
    .load()
)

# Stages: db2_Rx1xExtr
extract_query_db2_Rx1xExtr = f"""
SELECT
BNF_SUM_DTL.PROD_CMPNT_PFX_ID,
BNF_SUM_DTL.DEDCT_AMT
FROM
{IDSOwner}.BNF_SUM_DTL BNF_SUM_DTL,
{IDSOwner}.CD_MPPNG CD_MPPNG
WHERE BNF_SUM_DTL.BNF_SUM_DTL_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
AND CD_MPPNG.TRGT_CD = 'RX1X'
"""
df_db2_Rx1xExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_Rx1xExtr)
    .load()
)

# Stages: db2_RX1FExtr
extract_query_db2_RX1FExtr = f"""
SELECT
BNF_SUM_DTL.PROD_CMPNT_PFX_ID,
BNF_SUM_DTL.DEDCT_AMT
FROM
{IDSOwner}.BNF_SUM_DTL BNF_SUM_DTL,
{IDSOwner}.CD_MPPNG CD_MPPNG
WHERE BNF_SUM_DTL.BNF_SUM_DTL_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
AND CD_MPPNG.TRGT_CD = 'RX1F'
"""
df_db2_RX1FExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_RX1FExtr)
    .load()
)

# Stages: db2_DFOExtr
extract_query_db2_DFOExtr = f"""
SELECT
BNF_SUM_DTL.PROD_CMPNT_PFX_ID,
BNF_SUM_DTL.DEDCT_AMT
FROM
{IDSOwner}.BNF_SUM_DTL BNF_SUM_DTL,
{IDSOwner}.CD_MPPNG CD_MPPNG
WHERE BNF_SUM_DTL.BNF_SUM_DTL_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
AND CD_MPPNG.TRGT_CD = 'DFO'
"""
df_db2_DFOExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_DFOExtr)
    .load()
)

# Stages: db2_DEDFExtr
extract_query_db2_DEDFExtr = f"""
SELECT
BNF_SUM_DTL.PROD_CMPNT_PFX_ID,
BNF_SUM_DTL.DEDCT_AMT
FROM
{IDSOwner}.BNF_SUM_DTL BNF_SUM_DTL,
{IDSOwner}.CD_MPPNG CD_MPPNG
WHERE BNF_SUM_DTL.BNF_SUM_DTL_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
AND CD_MPPNG.TRGT_CD = 'DEDF'
"""
df_db2_DEDFExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_DEDFExtr)
    .load()
)

# Stages: db2_RX1DExtr
extract_query_db2_RX1DExtr = f"""
SELECT
BNF_SUM_DTL.PROD_CMPNT_PFX_ID,
BNF_SUM_DTL.DEDCT_AMT
FROM
{IDSOwner}.BNF_SUM_DTL BNF_SUM_DTL,
{IDSOwner}.CD_MPPNG CD_MPPNG
WHERE BNF_SUM_DTL.BNF_SUM_DTL_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
AND CD_MPPNG.TRGT_CD = 'RX1D'
"""
df_db2_RX1DExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_RX1DExtr)
    .load()
)

# Stages: db2_DEDIExtr
extract_query_db2_DEDIExtr = f"""
SELECT
BNF_SUM_DTL.PROD_CMPNT_PFX_ID,
BNF_SUM_DTL.DEDCT_AMT
FROM
{IDSOwner}.BNF_SUM_DTL BNF_SUM_DTL,
{IDSOwner}.CD_MPPNG CD_MPPNG
WHERE BNF_SUM_DTL.BNF_SUM_DTL_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
AND CD_MPPNG.TRGT_CD = 'DEDI'
"""
df_db2_DEDIExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_DEDIExtr)
    .load()
)

# Stages: db2_DIOExtr
extract_query_db2_DIOExtr = f"""
SELECT
BNF_SUM_DTL.PROD_CMPNT_PFX_ID,
BNF_SUM_DTL.DEDCT_AMT
FROM
{IDSOwner}.BNF_SUM_DTL BNF_SUM_DTL,
{IDSOwner}.CD_MPPNG CD_MPPNG
WHERE BNF_SUM_DTL.BNF_SUM_DTL_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
AND CD_MPPNG.TRGT_CD = 'DIO'
"""
df_db2_DIOExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_DIOExtr)
    .load()
)

# Stages: Rx1nExtr
extract_query_Rx1nExtr = f"""
SELECT
BNF_SUM_DTL.PROD_CMPNT_PFX_ID,
BNF_SUM_DTL.DEDCT_AMT
FROM
{IDSOwner}.BNF_SUM_DTL BNF_SUM_DTL,
{IDSOwner}.CD_MPPNG CD_MPPNG
WHERE BNF_SUM_DTL.BNF_SUM_DTL_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
AND CD_MPPNG.TRGT_CD = 'RX1N'
"""
df_Rx1nExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_Rx1nExtr)
    .load()
)

# Stage: lkp_prod_dedct_sum_f (PxLookup)
df_lkp_prod_dedct_sum_f = (
    df_db2_ProdCmpnt_Extr.alias("lnkProdDedctSumFExtrInAbc")
    .join(df_db2_RX1GExtr.alias("lnkRx1gExtrOut"),
          col("lnkProdDedctSumFExtrInAbc.PROD_CMPNT_PFX_ID") == col("lnkRx1gExtrOut.PROD_CMPNT_PFX_ID"),
          "left")
    .join(df_Rx1nExtr.alias("lnkRx1nExtr"),
          col("lnkProdDedctSumFExtrInAbc.PROD_CMPNT_PFX_ID") == col("lnkRx1nExtr.PROD_CMPNT_PFX_ID"),
          "left")
    .join(df_db2_Rx1pExtr.alias("lnkRx1pExtrOut"),
          col("lnkProdDedctSumFExtrInAbc.PROD_CMPNT_PFX_ID") == col("lnkRx1pExtrOut.PROD_CMPNT_PFX_ID"),
          "left")
    .join(df_db2_Rx1xExtr.alias("lnkRx1xExtrOut"),
          col("lnkProdDedctSumFExtrInAbc.PROD_CMPNT_PFX_ID") == col("lnkRx1xExtrOut.PROD_CMPNT_PFX_ID"),
          "left")
    .join(df_db2_RX1FExtr.alias("lnkRX1FExtrOut"),
          col("lnkProdDedctSumFExtrInAbc.PROD_CMPNT_PFX_ID") == col("lnkRX1FExtrOut.PROD_CMPNT_PFX_ID"),
          "left")
    .join(df_db2_DEDFExtr.alias("lnkDEDFExtrOut"),
          col("lnkProdDedctSumFExtrInAbc.PROD_CMPNT_PFX_ID") == col("lnkDEDFExtrOut.PROD_CMPNT_PFX_ID"),
          "left")
    .join(df_db2_DFOExtr.alias("lnkDFOExtrOut"),
          col("lnkProdDedctSumFExtrInAbc.PROD_CMPNT_PFX_ID") == col("lnkDFOExtrOut.PROD_CMPNT_PFX_ID"),
          "left")
    .join(df_db2_RX1DExtr.alias("lnkRX1DExtrOut"),
          col("lnkProdDedctSumFExtrInAbc.PROD_CMPNT_PFX_ID") == col("lnkRX1DExtrOut.PROD_CMPNT_PFX_ID"),
          "left")
    .join(df_db2_DEDIExtr.alias("lnkDEDIExtrOut"),
          col("lnkProdDedctSumFExtrInAbc.PROD_CMPNT_PFX_ID") == col("lnkDEDIExtrOut.PROD_CMPNT_PFX_ID"),
          "left")
    .join(df_db2_DIOExtr.alias("lnkDIOExtrOut"),
          col("lnkProdDedctSumFExtrInAbc.PROD_CMPNT_PFX_ID") == col("lnkDIOExtrOut.PROD_CMPNT_PFX_ID"),
          "left")
    .select(
        col("lnkProdDedctSumFExtrInAbc.PROD_CMPNT_PFX_ID").alias("PROD_CMPNT_PFX_ID"),
        col("lnkProdDedctSumFExtrInAbc.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("lnkProdDedctSumFExtrInAbc.PROD_ID").alias("PROD_ID"),
        col("lnkProdDedctSumFExtrInAbc.PROD_CMPNT_EFF_DT_SK").alias("PROD_CMPNT_EFF_DT_SK"),
        col("lnkProdDedctSumFExtrInAbc.PROD_SK").alias("PROD_SK"),
        col("lnkProdDedctSumFExtrInAbc.PROD_CMPNT_TERM_DT_SK").alias("PROD_CMPNT_TERM_DT_SK"),
        col("lnkRx1gExtrOut.DEDCT_AMT").alias("DRUG_GNRC_DEDCT_AMT"),
        col("lnkRx1nExtr.DEDCT_AMT").alias("DRUG_NM_BRND_DEDCT_AMT"),
        col("lnkRx1pExtrOut.DEDCT_AMT").alias("DRUG_PRFRD_DEDCT_AMT"),
        col("lnkRx1xExtrOut.DEDCT_AMT").alias("DRUG_NPRFR_DEDCT_AMT"),
        col("lnkRX1FExtrOut.DEDCT_AMT").alias("FMLY_DRUG_DEDCT_AMT"),
        col("lnkDEDFExtrOut.DEDCT_AMT").alias("FMLY_IN_NTWK_DEDCT_AMT"),
        col("lnkDFOExtrOut.DEDCT_AMT").alias("FMLY_OUT_NTWK_DEDCT_AMT"),
        col("lnkRX1DExtrOut.DEDCT_AMT").alias("INDV_DRUG_DEDCT_AMT"),
        col("lnkDEDIExtrOut.DEDCT_AMT").alias("INDV_IN_NTWK_DEDCT_AMT"),
        col("lnkDIOExtrOut.DEDCT_AMT").alias("INDV_OUT_NTWK_DEDCT_AMT"),
        col("PROD_CMPNT_PFX_ID")  # Retained for Transformer expressions
    )
)

# Stage: xfm_BusinessLogic
df_xfm_BusinessLogic_in = df_lkp_prod_dedct_sum_f.alias("lnk_rdpNaturalKeys")

df_lnk_Main_Data_in = df_xfm_BusinessLogic_in.filter(
    (col("SRC_SYS_CD") != "UNK")
    & (col("PROD_ID") != "UNK")
    & (col("PROD_CMPNT_EFF_DT_SK") != "1753-01-01")
    & (col("SRC_SYS_CD") != "NA")
    & (col("PROD_ID") != "NA")
    & (col("PROD_CMPNT_EFF_DT_SK") != "1753-01-01")
).select(
    lit(0).alias("PROD_DEDCT_SUM_SK"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("PROD_ID").alias("PROD_ID"),
    col("PROD_CMPNT_EFF_DT_SK").alias("PROD_CMPNT_EFF_DT_SK"),
    col("PROD_SK").alias("PROD_SK"),
    col("PROD_CMPNT_TERM_DT_SK").alias("PROD_CMPNT_TERM_DT_SK"),
    when(length(trim(col("PROD_CMPNT_PFX_ID"))) == 0, None).otherwise(col("DRUG_GNRC_DEDCT_AMT")).alias("DRUG_GNRC_DEDCT_AMT"),
    when(length(trim(col("PROD_CMPNT_PFX_ID"))) == 0, None).otherwise(col("DRUG_NM_BRND_DEDCT_AMT")).alias("DRUG_NM_BRND_DEDCT_AMT"),
    when(length(trim(col("PROD_CMPNT_PFX_ID"))) == 0, None).otherwise(col("DRUG_PRFRD_DEDCT_AMT")).alias("DRUG_PRFRD_DEDCT_AMT"),
    when(length(trim(col("PROD_CMPNT_PFX_ID"))) == 0, None).otherwise(col("DRUG_NPRFR_DEDCT_AMT")).alias("DRUG_NPRFR_DEDCT_AMT"),
    when(length(trim(col("PROD_CMPNT_PFX_ID"))) == 0, None).otherwise(col("FMLY_DRUG_DEDCT_AMT")).alias("FMLY_DRUG_DEDCT_AMT"),
    when(length(trim(col("PROD_CMPNT_PFX_ID"))) == 0, None).otherwise(col("FMLY_IN_NTWK_DEDCT_AMT")).alias("FMLY_IN_NTWK_DEDCT_AMT"),
    when(length(trim(col("PROD_CMPNT_PFX_ID"))) == 0, None).otherwise(col("FMLY_OUT_NTWK_DEDCT_AMT")).alias("FMLY_OUT_NTWK_DEDCT_AMT"),
    when(length(trim(col("PROD_CMPNT_PFX_ID"))) == 0, None).otherwise(col("INDV_DRUG_DEDCT_AMT")).alias("INDV_DRUG_DEDCT_AMT"),
    when(length(trim(col("PROD_CMPNT_PFX_ID"))) == 0, None).otherwise(col("INDV_IN_NTWK_DEDCT_AMT")).alias("INDV_IN_NTWK_DEDCT_AMT"),
    when(length(trim(col("PROD_CMPNT_PFX_ID"))) == 0, None).otherwise(col("INDV_OUT_NTWK_DEDCT_AMT")).alias("INDV_OUT_NTWK_DEDCT_AMT"),
    lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

df_lnk_UNK = spark.createDataFrame(
    [
        (
            0,
            "UNK",
            "UNK",
            "1753-01-01",
            0,
            "1753-01-01",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            "1753-01-01",
            EDWRunCycleDate,
            100,
            EDWRunCycle
        )
    ],
    [
        "PROD_DEDCT_SUM_SK",
        "SRC_SYS_CD",
        "PROD_ID",
        "PROD_CMPNT_EFF_DT_SK",
        "PROD_SK",
        "PROD_CMPNT_TERM_DT_SK",
        "DRUG_GNRC_DEDCT_AMT",
        "DRUG_NM_BRND_DEDCT_AMT",
        "DRUG_PRFRD_DEDCT_AMT",
        "DRUG_NPRFR_DEDCT_AMT",
        "FMLY_DRUG_DEDCT_AMT",
        "FMLY_IN_NTWK_DEDCT_AMT",
        "FMLY_OUT_NTWK_DEDCT_AMT",
        "INDV_DRUG_DEDCT_AMT",
        "INDV_IN_NTWK_DEDCT_AMT",
        "INDV_OUT_NTWK_DEDCT_AMT",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK"
    ]
)

df_lnk_NA = spark.createDataFrame(
    [
        (
            1,
            "NA",
            "NA",
            "1753-01-01",
            1,
            "1753-01-01",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            "1753-01-01",
            EDWRunCycleDate,
            100,
            EDWRunCycle
        )
    ],
    [
        "PROD_DEDCT_SUM_SK",
        "SRC_SYS_CD",
        "PROD_ID",
        "PROD_CMPNT_EFF_DT_SK",
        "PROD_SK",
        "PROD_CMPNT_TERM_DT_SK",
        "DRUG_GNRC_DEDCT_AMT",
        "DRUG_NM_BRND_DEDCT_AMT",
        "DRUG_PRFRD_DEDCT_AMT",
        "DRUG_NPRFR_DEDCT_AMT",
        "FMLY_DRUG_DEDCT_AMT",
        "FMLY_IN_NTWK_DEDCT_AMT",
        "FMLY_OUT_NTWK_DEDCT_AMT",
        "INDV_DRUG_DEDCT_AMT",
        "INDV_IN_NTWK_DEDCT_AMT",
        "INDV_OUT_NTWK_DEDCT_AMT",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK"
    ]
)

# Stage: fnl (PxFunnel)
common_cols = [
    "PROD_DEDCT_SUM_SK",
    "SRC_SYS_CD",
    "PROD_ID",
    "PROD_CMPNT_EFF_DT_SK",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "PROD_SK",
    "PROD_CMPNT_TERM_DT_SK",
    "DRUG_GNRC_DEDCT_AMT",
    "DRUG_NM_BRND_DEDCT_AMT",
    "DRUG_PRFRD_DEDCT_AMT",
    "DRUG_NPRFR_DEDCT_AMT",
    "FMLY_DRUG_DEDCT_AMT",
    "FMLY_IN_NTWK_DEDCT_AMT",
    "FMLY_OUT_NTWK_DEDCT_AMT",
    "INDV_DRUG_DEDCT_AMT",
    "INDV_IN_NTWK_DEDCT_AMT",
    "INDV_OUT_NTWK_DEDCT_AMT",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK"
]

df_fnl = (
    df_lnk_UNK.select(common_cols)
    .unionByName(df_lnk_NA.select(common_cols))
    .unionByName(df_lnk_Main_Data_in.select(common_cols))
)

# Stage: ds_PROD_DEDCT_SUM_F_EXT (PxDataSet -> Parquet)
# Apply rpad for char columns of length 10
df_final = df_fnl.select(
    col("PROD_DEDCT_SUM_SK"),
    col("SRC_SYS_CD"),
    col("PROD_ID"),
    rpad(col("PROD_CMPNT_EFF_DT_SK"), 10, " ").alias("PROD_CMPNT_EFF_DT_SK"),
    rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("PROD_SK"),
    rpad(col("PROD_CMPNT_TERM_DT_SK"), 10, " ").alias("PROD_CMPNT_TERM_DT_SK"),
    col("DRUG_GNRC_DEDCT_AMT"),
    col("DRUG_NM_BRND_DEDCT_AMT"),
    col("DRUG_PRFRD_DEDCT_AMT"),
    col("DRUG_NPRFR_DEDCT_AMT"),
    col("FMLY_DRUG_DEDCT_AMT"),
    col("FMLY_IN_NTWK_DEDCT_AMT"),
    col("FMLY_OUT_NTWK_DEDCT_AMT"),
    col("INDV_DRUG_DEDCT_AMT"),
    col("INDV_IN_NTWK_DEDCT_AMT"),
    col("INDV_OUT_NTWK_DEDCT_AMT"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

write_files(
    df_final,
    f"{adls_path}/ds/PROD_DEDCT_SUM_F.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)