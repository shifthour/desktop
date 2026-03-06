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
# MAGIC                   This is an extract from Product component table into the PROD_COPAY_SUM_F table with summation logic added.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               12/07/2007          3044                              Originally Programmed                           devlEDW10                  
# MAGIC Leandrew Moore                05/22/2013          5114                             rewrite in  parallel                                  Enterprisewarehouse  Peter Marshall            8/8/2013
# MAGIC 
# MAGIC Aishwarya                           03/15/2016          5600                            DRUG_NPRFR_SPEC_COPAY_AMT EnterpriseDev1            Jag Yelavarthi            2016-04-06     
# MAGIC                                                                                                              column added to the target

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
# MAGIC ProdCopaySumFExtr
# MAGIC Table:
# MAGIC  PROD_COPAY_SUM_F         
# MAGIC Pull the matched set of products fron IDS and move to EDW.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

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
  ON PROD_CMPNT.SRC_SYS_CD_SK = CD2.CD_MPPNG_SK
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
    .alias("prodcmp")
)

extract_query_db2_HosiExtr = f"""
SELECT BNF_SUM_DTL.PROD_CMPNT_PFX_ID,
BNF_SUM_DTL.COPAY_AMT
FROM {IDSOwner}.BNF_SUM_DTL BNF_SUM_DTL,
     {IDSOwner}.CD_MPPNG CD_MPPNG
WHERE BNF_SUM_DTL.BNF_SUM_DTL_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD = 'HOSI'
"""
df_db2_HosiExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_HosiExtr)
    .load()
    .alias("hosi")
)

extract_query_db2_Rx1nExtr = f"""
SELECT BNF_SUM_DTL.PROD_CMPNT_PFX_ID,
BNF_SUM_DTL.COPAY_AMT
FROM {IDSOwner}.BNF_SUM_DTL BNF_SUM_DTL,
     {IDSOwner}.CD_MPPNG CD_MPPNG
WHERE BNF_SUM_DTL.BNF_SUM_DTL_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD = 'RX1N'
"""
df_db2_Rx1nExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_Rx1nExtr)
    .load()
    .alias("rx1n")
)

extract_query_db2_Rx1gExtr = f"""
SELECT BNF_SUM_DTL.PROD_CMPNT_PFX_ID,
BNF_SUM_DTL.COPAY_AMT
FROM {IDSOwner}.BNF_SUM_DTL BNF_SUM_DTL,
     {IDSOwner}.CD_MPPNG CD_MPPNG
WHERE BNF_SUM_DTL.BNF_SUM_DTL_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD = 'RX1G'
"""
df_db2_Rx1gExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_Rx1gExtr)
    .load()
    .alias("rx1g")
)

extract_query_db2_Rx1pExtr = f"""
SELECT BNF_SUM_DTL.PROD_CMPNT_PFX_ID,
BNF_SUM_DTL.COPAY_AMT
FROM {IDSOwner}.BNF_SUM_DTL BNF_SUM_DTL,
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
    .alias("rx1p")
)

extract_query_db2_Rx1xExtr = f"""
SELECT BNF_SUM_DTL.PROD_CMPNT_PFX_ID,
BNF_SUM_DTL.COPAY_AMT
FROM {IDSOwner}.BNF_SUM_DTL BNF_SUM_DTL,
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
    .alias("rx1x")
)

extract_query_db2_ErExtr = f"""
SELECT BNF_SUM_DTL.PROD_CMPNT_PFX_ID,
BNF_SUM_DTL.COPAY_AMT
FROM {IDSOwner}.BNF_SUM_DTL BNF_SUM_DTL,
     {IDSOwner}.CD_MPPNG CD_MPPNG
WHERE BNF_SUM_DTL.BNF_SUM_DTL_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD = 'ER'
"""
df_db2_ErExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_ErExtr)
    .load()
    .alias("er")
)

extract_query_db2_EroExtr = f"""
SELECT BNF_SUM_DTL.PROD_CMPNT_PFX_ID,
BNF_SUM_DTL.COPAY_AMT
FROM {IDSOwner}.BNF_SUM_DTL BNF_SUM_DTL,
     {IDSOwner}.CD_MPPNG CD_MPPNG
WHERE BNF_SUM_DTL.BNF_SUM_DTL_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD = 'ERO'
"""
df_db2_EroExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_EroExtr)
    .load()
    .alias("ero")
)

extract_query_db2_HhcvExtr = f"""
SELECT BNF_SUM_DTL.PROD_CMPNT_PFX_ID,
BNF_SUM_DTL.COPAY_AMT
FROM {IDSOwner}.BNF_SUM_DTL BNF_SUM_DTL,
     {IDSOwner}.CD_MPPNG CD_MPPNG
WHERE BNF_SUM_DTL.BNF_SUM_DTL_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD = 'HHCV'
"""
df_db2_HhcvExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_HhcvExtr)
    .load()
    .alias("hhcv")
)

extract_query_db2_ViiExtr = f"""
SELECT BNF_SUM_DTL.PROD_CMPNT_PFX_ID,
BNF_SUM_DTL.COPAY_AMT
FROM {IDSOwner}.BNF_SUM_DTL BNF_SUM_DTL,
     {IDSOwner}.CD_MPPNG CD_MPPNG
WHERE BNF_SUM_DTL.BNF_SUM_DTL_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD = 'VII'
"""
df_db2_ViiExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_ViiExtr)
    .load()
    .alias("vii")
)

extract_query_db2_UcExtr = f"""
SELECT BNF_SUM_DTL.PROD_CMPNT_PFX_ID,
BNF_SUM_DTL.COPAY_AMT
FROM {IDSOwner}.BNF_SUM_DTL BNF_SUM_DTL,
     {IDSOwner}.CD_MPPNG CD_MPPNG
WHERE BNF_SUM_DTL.BNF_SUM_DTL_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD = 'UC'
"""
df_db2_UcExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_UcExtr)
    .load()
    .alias("uc")
)

extract_query_db2_SnfExtr = f"""
SELECT BNF_SUM_DTL.PROD_CMPNT_PFX_ID,
BNF_SUM_DTL.COPAY_AMT
FROM {IDSOwner}.BNF_SUM_DTL BNF_SUM_DTL,
     {IDSOwner}.CD_MPPNG CD_MPPNG
WHERE BNF_SUM_DTL.BNF_SUM_DTL_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD = 'SNF'
"""
df_db2_SnfExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_SnfExtr)
    .load()
    .alias("snf")
)

extract_query_db2_MoiExtr = f"""
SELECT BNF_SUM_DTL.PROD_CMPNT_PFX_ID,
BNF_SUM_DTL.COPAY_AMT
FROM {IDSOwner}.BNF_SUM_DTL BNF_SUM_DTL,
     {IDSOwner}.CD_MPPNG CD_MPPNG
WHERE BNF_SUM_DTL.BNF_SUM_DTL_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD = 'MOI'
"""
df_db2_MoiExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_MoiExtr)
    .load()
    .alias("moi")
)

extract_query_db2_MiiExtr = f"""
SELECT BNF_SUM_DTL.PROD_CMPNT_PFX_ID,
BNF_SUM_DTL.COPAY_AMT
FROM {IDSOwner}.BNF_SUM_DTL BNF_SUM_DTL,
     {IDSOwner}.CD_MPPNG CD_MPPNG
WHERE BNF_SUM_DTL.BNF_SUM_DTL_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD = 'MII'
"""
df_db2_MiiExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_MiiExtr)
    .load()
    .alias("mii")
)

extract_query_db2_OvsExtr = f"""
SELECT BNF_SUM_DTL.PROD_CMPNT_PFX_ID,
BNF_SUM_DTL.COPAY_AMT
FROM {IDSOwner}.BNF_SUM_DTL BNF_SUM_DTL,
     {IDSOwner}.CD_MPPNG CD_MPPNG
WHERE BNF_SUM_DTL.BNF_SUM_DTL_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD = 'OVS'
"""
df_db2_OvsExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_OvsExtr)
    .load()
    .alias("ovs")
)

extract_query_db2_OphExtr = f"""
SELECT BNF_SUM_DTL.PROD_CMPNT_PFX_ID,
BNF_SUM_DTL.COPAY_AMT
FROM {IDSOwner}.BNF_SUM_DTL BNF_SUM_DTL,
     {IDSOwner}.CD_MPPNG CD_MPPNG
WHERE BNF_SUM_DTL.BNF_SUM_DTL_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD = 'OPH'
"""
df_db2_OphExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_OphExtr)
    .load()
    .alias("oph")
)

extract_query_db2_McpcExtr = f"""
SELECT BNF_SUM_DTL.PROD_CMPNT_PFX_ID,
BNF_SUM_DTL.COPAY_AMT
FROM {IDSOwner}.BNF_SUM_DTL BNF_SUM_DTL,
     {IDSOwner}.CD_MPPNG CD_MPPNG
WHERE BNF_SUM_DTL.BNF_SUM_DTL_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD = 'MCPC'
"""
df_db2_McpcExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_McpcExtr)
    .load()
    .alias("mcpc")
)

extract_query_db2_OvExtr = f"""
SELECT BNF_SUM_DTL.PROD_CMPNT_PFX_ID,
BNF_SUM_DTL.COPAY_AMT
FROM {IDSOwner}.BNF_SUM_DTL BNF_SUM_DTL,
     {IDSOwner}.CD_MPPNG CD_MPPNG
WHERE BNF_SUM_DTL.BNF_SUM_DTL_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD = 'OV'
"""
df_db2_OvExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_OvExtr)
    .load()
    .alias("ov")
)

extract_query_db2_OvpExtr = f"""
SELECT BNF_SUM_DTL.PROD_CMPNT_PFX_ID,
BNF_SUM_DTL.COPAY_AMT
FROM {IDSOwner}.BNF_SUM_DTL BNF_SUM_DTL,
     {IDSOwner}.CD_MPPNG CD_MPPNG
WHERE BNF_SUM_DTL.BNF_SUM_DTL_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD = 'OVP'
"""
df_db2_OvpExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_OvpExtr)
    .load()
    .alias("ovp")
)

extract_query_db2_EpvExtr = f"""
SELECT BNF_SUM_DTL.PROD_CMPNT_PFX_ID,
BNF_SUM_DTL.COPAY_AMT
FROM {IDSOwner}.BNF_SUM_DTL BNF_SUM_DTL,
     {IDSOwner}.CD_MPPNG CD_MPPNG
WHERE BNF_SUM_DTL.BNF_SUM_DTL_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD = 'EPV'
"""
df_db2_EpvExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_EpvExtr)
    .load()
    .alias("epv")
)

extract_query_db2_HOIExtr = f"""
SELECT BNF_SUM_DTL.PROD_CMPNT_PFX_ID,
BNF_SUM_DTL.COPAY_AMT
FROM {IDSOwner}.BNF_SUM_DTL BNF_SUM_DTL,
     {IDSOwner}.CD_MPPNG CD_MPPNG
WHERE BNF_SUM_DTL.BNF_SUM_DTL_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD = 'HOI'
"""
df_db2_HOIExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_HOIExtr)
    .load()
    .alias("hoi")
)

extract_query_db2_RxSpExtr = f"""
SELECT
BNF_SUM_DTL.PROD_CMPNT_PFX_ID,
BNF_SUM_DTL.COPAY_AMT
FROM {IDSOwner}.BNF_SUM_DTL BNF_SUM_DTL,
     {IDSOwner}.CD_MPPNG CD_MPPNG
WHERE BNF_SUM_DTL.BNF_SUM_DTL_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD = 'RXSP'
  AND CD_MPPNG.SRC_SYS_CD = 'FACETS'
  AND CD_MPPNG.SRC_DOMAIN_NM = 'CAPITATION COPAYMENT TYPE'
  AND CD_MPPNG.SRC_CLCTN_CD = 'FACETS DBO'
  AND CD_MPPNG.TRGT_DOMAIN_NM = 'CAPITATION COPAYMENT TYPE'
  AND CD_MPPNG.TRGT_CLCTN_CD = 'IDS'
"""
df_db2_RxSpExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_RxSpExtr)
    .load()
    .alias("rxsp")
)

df_lkp = (
    df_db2_ProdCmpnt_Extr
    .join(df_db2_Rx1nExtr, F.col("prodcmp.PROD_CMPNT_PFX_ID") == F.col("rx1n.PROD_CMPNT_PFX_ID"), "left")
    .join(df_db2_Rx1gExtr, F.col("prodcmp.PROD_CMPNT_PFX_ID") == F.col("rx1g.PROD_CMPNT_PFX_ID"), "left")
    .join(df_db2_Rx1pExtr, F.col("prodcmp.PROD_CMPNT_PFX_ID") == F.col("rx1p.PROD_CMPNT_PFX_ID"), "left")
    .join(df_db2_Rx1xExtr, F.col("prodcmp.PROD_CMPNT_PFX_ID") == F.col("rx1x.PROD_CMPNT_PFX_ID"), "left")
    .join(df_db2_EpvExtr, F.col("prodcmp.PROD_CMPNT_PFX_ID") == F.col("epv.PROD_CMPNT_PFX_ID"), "left")
    .join(df_db2_ErExtr, F.col("prodcmp.PROD_CMPNT_PFX_ID") == F.col("er.PROD_CMPNT_PFX_ID"), "left")
    .join(df_db2_EroExtr, F.col("prodcmp.PROD_CMPNT_PFX_ID") == F.col("ero.PROD_CMPNT_PFX_ID"), "left")
    .join(df_db2_HhcvExtr, F.col("prodcmp.PROD_CMPNT_PFX_ID") == F.col("hhcv.PROD_CMPNT_PFX_ID"), "left")
    .join(df_db2_HOIExtr, F.col("prodcmp.PROD_CMPNT_PFX_ID") == F.col("hoi.PROD_CMPNT_PFX_ID"), "left")
    .join(df_db2_HosiExtr, F.col("prodcmp.PROD_CMPNT_PFX_ID") == F.col("hosi.PROD_CMPNT_PFX_ID"), "left")
    .join(df_db2_OphExtr, F.col("prodcmp.PROD_CMPNT_PFX_ID") == F.col("oph.PROD_CMPNT_PFX_ID"), "left")
    .join(df_db2_OvsExtr, F.col("prodcmp.PROD_CMPNT_PFX_ID") == F.col("ovs.PROD_CMPNT_PFX_ID"), "left")
    .join(df_db2_ViiExtr, F.col("prodcmp.PROD_CMPNT_PFX_ID") == F.col("vii.PROD_CMPNT_PFX_ID"), "left")
    .join(df_db2_UcExtr, F.col("prodcmp.PROD_CMPNT_PFX_ID") == F.col("uc.PROD_CMPNT_PFX_ID"), "left")
    .join(df_db2_OvpExtr, F.col("prodcmp.PROD_CMPNT_PFX_ID") == F.col("ovp.PROD_CMPNT_PFX_ID"), "left")
    .join(df_db2_OvExtr, F.col("prodcmp.PROD_CMPNT_PFX_ID") == F.col("ov.PROD_CMPNT_PFX_ID"), "left")
    .join(df_db2_SnfExtr, F.col("prodcmp.PROD_CMPNT_PFX_ID") == F.col("snf.PROD_CMPNT_PFX_ID"), "left")
    .join(df_db2_MiiExtr, F.col("prodcmp.PROD_CMPNT_PFX_ID") == F.col("mii.PROD_CMPNT_PFX_ID"), "left")
    .join(df_db2_MoiExtr, F.col("prodcmp.PROD_CMPNT_PFX_ID") == F.col("moi.PROD_CMPNT_PFX_ID"), "left")
    .join(df_db2_McpcExtr, F.col("prodcmp.PROD_CMPNT_PFX_ID") == F.col("mcpc.PROD_CMPNT_PFX_ID"), "left")
    .join(df_db2_RxSpExtr, F.col("prodcmp.PROD_CMPNT_PFX_ID") == F.col("rxsp.PROD_CMPNT_PFX_ID"), "left")
    .select(
        F.col("prodcmp.PROD_CMPNT_PFX_ID").alias("PROD_CMPNT_PFX_ID"),
        F.col("prodcmp.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("prodcmp.PROD_ID").alias("PROD_ID"),
        F.col("prodcmp.PROD_CMPNT_EFF_DT_SK").alias("PROD_CMPNT_EFF_DT_SK"),
        F.col("prodcmp.PROD_SK").alias("PROD_SK"),
        F.col("prodcmp.PROD_CMPNT_TERM_DT_SK").alias("PROD_CMPNT_TERM_DT_SK"),
        F.col("rx1g.COPAY_AMT").alias("DRUG_GNRC_COPAY_AMT"),
        F.col("rx1n.COPAY_AMT").alias("DRUG_NM_BRND_COPAY_AMT"),
        F.col("rx1p.COPAY_AMT").alias("DRUG_PRFRD_COPAY_AMT"),
        F.col("rx1x.COPAY_AMT").alias("DRUG_NPRFR_COPAY_AMT"),
        F.col("epv.COPAY_AMT").alias("ELTRNC_PHYS_VST_COPAY_AMT"),
        F.col("er.COPAY_AMT").alias("ER_IN_NTWK_COPAY_AMT"),
        F.col("ero.COPAY_AMT").alias("ER_OUT_NTWK_COPAY_AMT"),
        F.col("hhcv.COPAY_AMT").alias("HOME_HLTH_VST_COPAY_AMT"),
        F.col("hoi.COPAY_AMT").alias("IN_HOSP_IN_OUT_NTWK_COPAY_AMT"),
        F.col("hosi.COPAY_AMT").alias("IP_HSPC_COPAY_AMT"),
        F.col("mii.COPAY_AMT").alias("MNTL_HLTH_IP_IN_NTWK_COPAY_AMT"),
        F.col("moi.COPAY_AMT").alias("MNTL_HLTH_OP_IN_NTWK_COPAY_AMT"),
        F.col("snf.COPAY_AMT").alias("SKILL_NURSE_COPAY_AMT"),
        F.col("mcpc.COPAY_AMT").alias("MRI_CT_PET_MRA_COPAY_AMT"),
        F.col("ov.COPAY_AMT").alias("OV_IN_OUT_NTWK_COPAY_AMT"),
        F.col("ovp.COPAY_AMT").alias("OV_PCP_IN_OUT_NTWK_COPAY_AMT"),
        F.col("ovs.COPAY_AMT").alias("OV_SPLST_IN_OUT_NTWK_COPAY_AMT"),
        F.col("oph.COPAY_AMT").alias("OP_HOSP_IN_OUT_NTWK_COPAY_AMT"),
        F.col("uc.COPAY_AMT").alias("UC_IN_NTWK_COPAY_AMT"),
        F.col("vii.COPAY_AMT").alias("VSN_IN_NTWK_COPAY_AMT"),
        F.col("rxsp.COPAY_AMT").alias("DRUG_NPRFR_SPEC_COPAY_AMT"),
    )
)

# xfm_BusinessLogic (Transformer) - build main data link
df_main_transform = (
    df_lkp
    .withColumn("PROD_COPAY_SUM_SK", F.lit(0))
    .withColumn("SRC_SYS_CD", F.col("SRC_SYS_CD"))
    .withColumn("PROD_ID", F.col("PROD_ID"))
    .withColumn("PROD_CMPNT_EFF_DT_SK", F.col("PROD_CMPNT_EFF_DT_SK"))
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.lit("1753-01-01"))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.lit(EDWRunCycleDate))
    .withColumn("PROD_SK", F.col("PROD_SK"))
    .withColumn("PROD_CMPNT_TERM_DT_SK", F.col("PROD_CMPNT_TERM_DT_SK"))
    .withColumn(
        "DRUG_GNRC_COPAY_AMT",
        F.when(F.length(trim(F.col("PROD_CMPNT_PFX_ID"))) == 0, F.lit(None))
         .otherwise(F.col("DRUG_GNRC_COPAY_AMT")),
    )
    .withColumn(
        "DRUG_NM_BRND_COPAY_AMT",
        F.when(F.length(trim(F.col("PROD_CMPNT_PFX_ID"))) == 0, F.lit(None))
         .otherwise(F.col("DRUG_NM_BRND_COPAY_AMT")),
    )
    .withColumn(
        "DRUG_PRFRD_COPAY_AMT",
        F.when(F.length(trim(F.col("PROD_CMPNT_PFX_ID"))) == 0, F.lit(None))
         .otherwise(F.col("DRUG_PRFRD_COPAY_AMT")),
    )
    .withColumn(
        "DRUG_NPRFR_COPAY_AMT",
        F.when(F.length(trim(F.col("PROD_CMPNT_PFX_ID"))) == 0, F.lit(None))
         .otherwise(F.col("DRUG_NPRFR_COPAY_AMT")),
    )
    .withColumn(
        "ELTRNC_PHYS_VST_COPAY_AMT",
        F.when(F.length(trim(F.col("PROD_CMPNT_PFX_ID"))) == 0, F.lit(None))
         .otherwise(F.col("ELTRNC_PHYS_VST_COPAY_AMT")),
    )
    .withColumn(
        "ER_IN_NTWK_COPAY_AMT",
        F.when(F.length(trim(F.col("PROD_CMPNT_PFX_ID"))) == 0, F.lit(None))
         .otherwise(F.col("ER_IN_NTWK_COPAY_AMT")),
    )
    .withColumn(
        "ER_OUT_NTWK_COPAY_AMT",
        F.when(F.length(trim(F.col("PROD_CMPNT_PFX_ID"))) == 0, F.lit(None))
         .otherwise(F.col("ER_OUT_NTWK_COPAY_AMT")),
    )
    .withColumn(
        "HOME_HLTH_VST_COPAY_AMT",
        F.when(F.length(trim(F.col("PROD_CMPNT_PFX_ID"))) == 0, F.lit(None))
         .otherwise(F.col("HOME_HLTH_VST_COPAY_AMT")),
    )
    .withColumn(
        "IN_HOSP_IN_OUT_NTWK_COPAY_AMT",
        F.when(F.length(trim(F.col("PROD_CMPNT_PFX_ID"))) == 0, F.lit(None))
         .otherwise(F.col("IN_HOSP_IN_OUT_NTWK_COPAY_AMT")),
    )
    .withColumn(
        "IP_HSPC_COPAY_AMT",
        F.when(F.length(trim(F.col("PROD_CMPNT_PFX_ID"))) == 0, F.lit(None))
         .otherwise(F.col("IP_HSPC_COPAY_AMT")),
    )
    .withColumn(
        "MNTL_HLTH_IP_IN_NTWK_COPAY_AMT",
        F.when(F.length(trim(F.col("PROD_CMPNT_PFX_ID"))) == 0, F.lit(None))
         .otherwise(F.col("MNTL_HLTH_IP_IN_NTWK_COPAY_AMT")),
    )
    .withColumn(
        "MNTL_HLTH_OP_IN_NTWK_COPAY_AMT",
        F.when(F.length(trim(F.col("PROD_CMPNT_PFX_ID"))) == 0, F.lit(None))
         .otherwise(F.col("MNTL_HLTH_OP_IN_NTWK_COPAY_AMT")),
    )
    .withColumn(
        "MRI_CT_PET_MRA_COPAY_AMT",
        F.when(F.length(trim(F.col("PROD_CMPNT_PFX_ID"))) == 0, F.lit(None))
         .otherwise(F.col("MRI_CT_PET_MRA_COPAY_AMT")),
    )
    .withColumn(
        "OV_IN_OUT_NTWK_COPAY_AMT",
        F.when(F.length(trim(F.col("PROD_CMPNT_PFX_ID"))) == 0, F.lit(None))
         .otherwise(F.col("OV_IN_OUT_NTWK_COPAY_AMT")),
    )
    .withColumn(
        "OV_PCP_IN_OUT_NTWK_COPAY_AMT",
        F.when(F.length(trim(F.col("PROD_CMPNT_PFX_ID"))) == 0, F.lit(None))
         .otherwise(F.col("OV_PCP_IN_OUT_NTWK_COPAY_AMT")),
    )
    .withColumn(
        "OV_SPLST_IN_OUT_NTWK_COPAY_AMT",
        F.when(F.length(trim(F.col("PROD_CMPNT_PFX_ID"))) == 0, F.lit(None))
         .otherwise(F.col("OV_SPLST_IN_OUT_NTWK_COPAY_AMT")),
    )
    .withColumn(
        "OP_HOSP_IN_OUT_NTWK_COPAY_AMT",
        F.when(F.length(trim(F.col("PROD_CMPNT_PFX_ID"))) == 0, F.lit(None))
         .otherwise(F.col("OP_HOSP_IN_OUT_NTWK_COPAY_AMT")),
    )
    .withColumn(
        "SKILL_NURSE_COPAY_AMT",
        F.when(F.length(trim(F.col("PROD_CMPNT_PFX_ID"))) == 0, F.lit(None))
         .otherwise(F.col("SKILL_NURSE_COPAY_AMT")),
    )
    .withColumn(
        "UC_IN_NTWK_COPAY_AMT",
        F.when(F.length(trim(F.col("PROD_CMPNT_PFX_ID"))) == 0, F.lit(None))
         .otherwise(F.col("UC_IN_NTWK_COPAY_AMT")),
    )
    .withColumn(
        "VSN_IN_NTWK_COPAY_AMT",
        F.when(F.length(trim(F.col("PROD_CMPNT_PFX_ID"))) == 0, F.lit(None))
         .otherwise(F.col("VSN_IN_NTWK_COPAY_AMT")),
    )
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(100))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(EDWRunCycle))
    .withColumn(
        "DRUG_NPRFR_SPEC_COPAY_AMT",
        F.col("DRUG_NPRFR_SPEC_COPAY_AMT"),
    )
)

df_main = df_main_transform.filter(
    (F.col("SRC_SYS_CD") != "UNK")
    & (F.col("PROD_ID") != "UNK")
    & (F.col("PROD_CMPNT_EFF_DT_SK") != "1753-01-01")
    & (F.col("SRC_SYS_CD") != "NA")
    & (F.col("PROD_ID") != "NA")
    & (F.col("PROD_CMPNT_EFF_DT_SK") != "1753-01-01")
)

# Single-row UNK link
schema_unk = StructType([
    StructField("PROD_COPAY_SUM_SK", IntegerType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("PROD_ID", StringType(), True),
    StructField("PROD_CMPNT_EFF_DT_SK", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("PROD_SK", IntegerType(), True),
    StructField("PROD_CMPNT_TERM_DT_SK", StringType(), True),
    StructField("DRUG_GNRC_COPAY_AMT", DecimalType(38,10), True),
    StructField("DRUG_NM_BRND_COPAY_AMT", DecimalType(38,10), True),
    StructField("DRUG_PRFRD_COPAY_AMT", DecimalType(38,10), True),
    StructField("DRUG_NPRFR_COPAY_AMT", DecimalType(38,10), True),
    StructField("ELTRNC_PHYS_VST_COPAY_AMT", DecimalType(38,10), True),
    StructField("ER_IN_NTWK_COPAY_AMT", DecimalType(38,10), True),
    StructField("ER_OUT_NTWK_COPAY_AMT", DecimalType(38,10), True),
    StructField("HOME_HLTH_VST_COPAY_AMT", DecimalType(38,10), True),
    StructField("IN_HOSP_IN_OUT_NTWK_COPAY_AMT", DecimalType(38,10), True),
    StructField("IP_HSPC_COPAY_AMT", DecimalType(38,10), True),
    StructField("MNTL_HLTH_IP_IN_NTWK_COPAY_AMT", DecimalType(38,10), True),
    StructField("MNTL_HLTH_OP_IN_NTWK_COPAY_AMT", DecimalType(38,10), True),
    StructField("MRI_CT_PET_MRA_COPAY_AMT", DecimalType(38,10), True),
    StructField("OV_IN_OUT_NTWK_COPAY_AMT", DecimalType(38,10), True),
    StructField("OV_PCP_IN_OUT_NTWK_COPAY_AMT", DecimalType(38,10), True),
    StructField("OV_SPLST_IN_OUT_NTWK_COPAY_AMT", DecimalType(38,10), True),
    StructField("OP_HOSP_IN_OUT_NTWK_COPAY_AMT", DecimalType(38,10), True),
    StructField("SKILL_NURSE_COPAY_AMT", DecimalType(38,10), True),
    StructField("UC_IN_NTWK_COPAY_AMT", DecimalType(38,10), True),
    StructField("VSN_IN_NTWK_COPAY_AMT", DecimalType(38,10), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(),