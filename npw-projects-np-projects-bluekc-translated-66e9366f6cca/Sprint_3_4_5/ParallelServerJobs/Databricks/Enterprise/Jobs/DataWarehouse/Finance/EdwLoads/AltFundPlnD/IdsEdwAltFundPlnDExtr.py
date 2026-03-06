# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC                                                                                                                                                                                                                 
# MAGIC CALLED BY:
# MAGIC 
# MAGIC PROCESSING:
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                             Date                  Project/Altiris #                                   Change Description                                                    Development Project     Code Reviewer            Date Reviewed
# MAGIC ----------------------------------        -------------------      -----------------------------------    ---------------------------------------------------------------------------------------------             ----------------------------------    ---------------------------------    -------------------------   
# MAGIC Naren                                9/20/2007                         3259                        Originally Programmed                                                                        devlEDW10                 
# MAGIC 
# MAGIC 
# MAGIC Raj Mangalampally            08/16/2013                       5114                        Original Programming                                                               EnterpriseWrhsDevl       Peter Marshall              12/10/2013
# MAGIC                                                                                                                    (Server to Parallel Conversion)

# MAGIC This is Source extract data from an IDS table and apply Code denormalization, create a load ready file.
# MAGIC 
# MAGIC Job Name:
# MAGIC IdsEdwAltFundPlnDExtr
# MAGIC 
# MAGIC Table:
# MAGIC ALT_FUND_PLN_D
# MAGIC Read from source table ALT_FUND_PLN AND ALT_FUND;
# MAGIC Full extract from source
# MAGIC This is a load ready file conforms to the target table structure.
# MAGIC 
# MAGIC Write ALT_FUND_PLN_D Data into a Sequential file for Load Job.
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Add Defaults and Null Handling
# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC 
# MAGIC 1) ALT_FUND_CLS_PROD_CAT_CD_SK
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query_db2_ALT_FUND_PLN_D_in = f"""
SELECT 
ALT_FUND_PLN.ALT_FUND_PLN_SK,
COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
ALT_FUND_PLN.SRC_SYS_CD_SK,
ALT_FUND_PLN.ALT_FUND_PLN_ROW_ID,
ALT_FUND_PLN.ALT_FUND_SK,
ALT_FUND_PLN.CLS_PLN_SK,
ALT_FUND_PLN.GRP_SK,
ALT_FUND_PLN.SUBGRP_SK,
ALT_FUND_PLN.ALT_FUND_CLS_PROD_CAT_CD_SK,
ALT_FUND_PLN.ALT_FUND_CNTR_PERD_NO,
ALT_FUND_PLN.ALT_FUND_UNIQ_KEY,
ALT_FUND.ALT_FUND_ID,
ALT_FUND.ALT_FUND_NM,
GRP.GRP_ID,
SUBGRP.SUBGRP_ID,
CLS_PLN.CLS_PLN_ID
FROM
{IDSOwner}.ALT_FUND_PLN ALT_FUND_PLN
LEFT OUTER JOIN {IDSOwner}.CD_MPPNG CD ON ALT_FUND_PLN.SRC_SYS_CD_SK = CD.CD_MPPNG_SK,
{IDSOwner}.GRP GRP,
{IDSOwner}.SUBGRP SUBGRP,
{IDSOwner}.ALT_FUND ALT_FUND,
{IDSOwner}.CLS_PLN CLS_PLN
WHERE
ALT_FUND_PLN.GRP_SK=GRP.GRP_SK AND
ALT_FUND_PLN.SUBGRP_SK=SUBGRP.SUBGRP_SK AND
ALT_FUND_PLN.CLS_PLN_SK=CLS_PLN.CLS_PLN_SK AND
ALT_FUND_PLN.ALT_FUND_SK=ALT_FUND.ALT_FUND_SK
"""

df_db2_ALT_FUND_PLN_D_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_ALT_FUND_PLN_D_in)
    .load()
)

extract_query_db2_CD_MPPNG_Extr = f"""
SELECT 
CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') TRGT_CD,
COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG
"""

df_db2_CD_MPPNG_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CD_MPPNG_Extr)
    .load()
)

df_lkp_Codes = (
    df_db2_ALT_FUND_PLN_D_in.alias("lnk_IdsEdwAltFundPlnDExtr_InABC")
    .join(
        df_db2_CD_MPPNG_Extr.alias("lnk_RefAltFundClsProdCatCdSk_Out"),
        F.col("lnk_IdsEdwAltFundPlnDExtr_InABC.ALT_FUND_CLS_PROD_CAT_CD_SK")
        == F.col("lnk_RefAltFundClsProdCatCdSk_Out.CD_MPPNG_SK"),
        "left"
    )
    .select(
        F.col("lnk_IdsEdwAltFundPlnDExtr_InABC.ALT_FUND_PLN_SK").alias("ALT_FUND_PLN_SK"),
        F.col("lnk_IdsEdwAltFundPlnDExtr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnk_IdsEdwAltFundPlnDExtr_InABC.ALT_FUND_PLN_ROW_ID").alias("ALT_FUND_PLN_ROW_ID"),
        F.col("lnk_IdsEdwAltFundPlnDExtr_InABC.ALT_FUND_SK").alias("ALT_FUND_SK"),
        F.col("lnk_IdsEdwAltFundPlnDExtr_InABC.CLS_PLN_SK").alias("CLS_PLN_SK"),
        F.col("lnk_IdsEdwAltFundPlnDExtr_InABC.GRP_SK").alias("GRP_SK"),
        F.col("lnk_IdsEdwAltFundPlnDExtr_InABC.SUBGRP_SK").alias("SUBGRP_SK"),
        F.col("lnk_RefAltFundClsProdCatCdSk_Out.TRGT_CD").alias("ALT_FUND_CLS_PROD_CAT_CD"),
        F.col("lnk_RefAltFundClsProdCatCdSk_Out.TRGT_CD_NM").alias("ALT_FUND_CLS_PROD_CAT_NM"),
        F.col("lnk_IdsEdwAltFundPlnDExtr_InABC.ALT_FUND_CNTR_PERD_NO").alias("ALT_FUND_CNTR_PERD_NO"),
        F.col("lnk_IdsEdwAltFundPlnDExtr_InABC.ALT_FUND_ID").alias("ALT_FUND_ID"),
        F.col("lnk_IdsEdwAltFundPlnDExtr_InABC.ALT_FUND_NM").alias("ALT_FUND_NM"),
        F.col("lnk_IdsEdwAltFundPlnDExtr_InABC.ALT_FUND_UNIQ_KEY").alias("ALT_FUND_UNIQ_KEY"),
        F.col("lnk_IdsEdwAltFundPlnDExtr_InABC.CLS_PLN_ID").alias("CLS_PLN_ID"),
        F.col("lnk_IdsEdwAltFundPlnDExtr_InABC.GRP_ID").alias("GRP_ID"),
        F.col("lnk_IdsEdwAltFundPlnDExtr_InABC.SUBGRP_ID").alias("SUBGRP_ID"),
        F.col("lnk_IdsEdwAltFundPlnDExtr_InABC.ALT_FUND_CLS_PROD_CAT_CD_SK").alias("ALT_FUND_CLS_PROD_CAT_CD_SK")
    )
)

df_main = (
    df_lkp_Codes
    .filter((F.col("ALT_FUND_PLN_SK") != 0) & (F.col("ALT_FUND_PLN_SK") != 1))
    .withColumn("SRC_SYS_CD", F.when(F.trim(F.col("SRC_SYS_CD")) == "", F.lit("UNK")).otherwise(F.col("SRC_SYS_CD")))
    .withColumn("ALT_FUND_PLN_ROW_ID", F.col("ALT_FUND_PLN_ROW_ID"))
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.lit(EDWRunCycleDate))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.lit(EDWRunCycleDate))
    .withColumn("ALT_FUND_SK", F.col("ALT_FUND_SK"))
    .withColumn("CLS_PLN_SK", F.col("CLS_PLN_SK"))
    .withColumn("GRP_SK", F.col("GRP_SK"))
    .withColumn("SUBGRP_SK", F.col("SUBGRP_SK"))
    .withColumn("ALT_FUND_CLS_PROD_CAT_CD", F.col("ALT_FUND_CLS_PROD_CAT_CD"))
    .withColumn("ALT_FUND_CLS_PROD_CAT_NM", F.col("ALT_FUND_CLS_PROD_CAT_NM"))
    .withColumn("ALT_FUND_CNTR_PERD_NO", F.col("ALT_FUND_CNTR_PERD_NO"))
    .withColumn("ALT_FUND_ID", F.col("ALT_FUND_ID"))
    .withColumn("ALT_FUND_NM", F.col("ALT_FUND_NM"))
    .withColumn("ALT_FUND_UNIQ_KEY", F.col("ALT_FUND_UNIQ_KEY"))
    .withColumn("CLS_PLN_ID", F.col("CLS_PLN_ID"))
    .withColumn("GRP_ID", F.col("GRP_ID"))
    .withColumn("SUBGRP_ID", F.col("SUBGRP_ID"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(EDWRunCycle))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(EDWRunCycle))
    .withColumn("ALT_FUND_CLS_PROD_CAT_CD_SK", F.col("ALT_FUND_CLS_PROD_CAT_CD_SK"))
)

schema_transform = [
    StructField("ALT_FUND_PLN_SK", IntegerType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("ALT_FUND_PLN_ROW_ID", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("ALT_FUND_SK", IntegerType(), True),
    StructField("CLS_PLN_SK", IntegerType(), True),
    StructField("GRP_SK", IntegerType(), True),
    StructField("SUBGRP_SK", IntegerType(), True),
    StructField("ALT_FUND_CLS_PROD_CAT_CD", StringType(), True),
    StructField("ALT_FUND_CLS_PROD_CAT_NM", StringType(), True),
    StructField("ALT_FUND_CNTR_PERD_NO", StringType(), True),
    StructField("ALT_FUND_ID", StringType(), True),
    StructField("ALT_FUND_NM", StringType(), True),
    StructField("ALT_FUND_UNIQ_KEY", IntegerType(), True),
    StructField("CLS_PLN_ID", StringType(), True),
    StructField("GRP_ID", StringType(), True),
    StructField("SUBGRP_ID", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", StringType(), True),
    StructField("ALT_FUND_CLS_PROD_CAT_CD_SK", IntegerType(), True),
]

df_UNK = spark.createDataFrame([
    (
        0,
        "UNK",
        "UNK",
        "1753-01-01",
        "1753-01-01",
        0,
        0,
        0,
        0,
        "UNK",
        "UNK",
        "UNK",
        "UNK",
        "UNK",
        0,
        "UNK",
        "UNK",
        "UNK",
        "100",
        "100",
        0
    )
], schema=StructType(schema_transform))

df_NA = spark.createDataFrame([
    (
        1,
        "NA",
        "NA",
        "1753-01-01",
        "1753-01-01",
        1,
        1,
        1,
        1,
        "NA",
        "NA",
        "NA",
        "NA",
        "NA",
        1,
        "NA",
        "NA",
        "NA",
        "100",
        "100",
        1
    )
], schema=StructType(schema_transform))

common_cols = [
    "ALT_FUND_PLN_SK",
    "SRC_SYS_CD",
    "ALT_FUND_PLN_ROW_ID",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "ALT_FUND_SK",
    "CLS_PLN_SK",
    "GRP_SK",
    "SUBGRP_SK",
    "ALT_FUND_CLS_PROD_CAT_CD",
    "ALT_FUND_CLS_PROD_CAT_NM",
    "ALT_FUND_CNTR_PERD_NO",
    "ALT_FUND_ID",
    "ALT_FUND_NM",
    "ALT_FUND_UNIQ_KEY",
    "CLS_PLN_ID",
    "GRP_ID",
    "SUBGRP_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "ALT_FUND_CLS_PROD_CAT_CD_SK"
]

df_fnl_UNK_NA = (
    df_NA.select(common_cols)
    .unionByName(df_main.select(common_cols))
    .unionByName(df_UNK.select(common_cols))
)

df_fnl_UNK_NA_final = df_fnl_UNK_NA.select(
    F.col("ALT_FUND_PLN_SK"),
    F.col("SRC_SYS_CD"),
    F.col("ALT_FUND_PLN_ROW_ID"),
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("ALT_FUND_SK"),
    F.col("CLS_PLN_SK"),
    F.col("GRP_SK"),
    F.col("SUBGRP_SK"),
    F.col("ALT_FUND_CLS_PROD_CAT_CD"),
    F.col("ALT_FUND_CLS_PROD_CAT_NM"),
    F.col("ALT_FUND_CNTR_PERD_NO"),
    F.col("ALT_FUND_ID"),
    F.col("ALT_FUND_NM"),
    F.col("ALT_FUND_UNIQ_KEY"),
    F.col("CLS_PLN_ID"),
    F.col("GRP_ID"),
    F.col("SUBGRP_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("ALT_FUND_CLS_PROD_CAT_CD_SK")
)

write_files(
    df_fnl_UNK_NA_final,
    f"{adls_path}/load/ALT_FUND_PLN_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)