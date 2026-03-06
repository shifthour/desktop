# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC DESCRIPTION:   Pulls data from AHY_ELIG and creates AHY_ELIG_D table   
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer               Date                         Change Description                                                                           Project #          Development Project          Code Reviewer          Date Reviewed  
# MAGIC ------------------             ----------------------------     -----------------------------------------------------------------------                                   ----------------         ------------------------------------       ----------------------------      ----------------
# MAGIC Bhoomi Dasari       2009-02-06                Initial programming                                                                               3863                devlEDWcur                      Steph Goddard          02/12/2009
# MAGIC 
# MAGIC Pooja Sunkara       2013-06-19               Server to parallel job convertion                                                           5114                EnterpriseWrhsDevl

# MAGIC Read data from source table AHY_ELIG. Pull TRGT_CD(SRC_SYS_CD) column from CD_MPPNG table by joining key columns SRC_SYS_CD_SK and CD_MPPNG_SK and on LAST_UPDT_RUN_CYC_EXCTN_SK.
# MAGIC Code SK lookups for Denormalization
# MAGIC Lookup Keys:
# MAGIC AHY_ELIG_TYP_SK
# MAGIC 
# MAGIC Pull Ref cols 
# MAGIC AHY_ELIG_TYP_ID
# MAGIC AHY_ELIG_TYP_DESC
# MAGIC Add CRT_RUN_CYC_EXCTN_DT_SK ,LAST_UPDT_RUN_CYC_EXCTN_DT_SK, NA and UNK
# MAGIC Write AHY_ELIG_D Data into a Sequential file for Load Ready Job.
# MAGIC Code SK lookups for Denormalization
# MAGIC Lookup Keys:
# MAGIC CLS_PLN_SK
# MAGIC 
# MAGIC Pull Ref cols 
# MAGIC CLS_PLN_ID
# MAGIC Code SK lookups for Denormalization
# MAGIC Lookup Keys:
# MAGIC GRP_SK
# MAGIC Pull Ref cols 
# MAGIC GRP_ID
# MAGIC Code SK lookups for Denormalization
# MAGIC Lookup Keys:
# MAGIC SUBGRP_SK
# MAGIC Pull Ref cols 
# MAGIC SUBGRP_ID
# MAGIC Job name:
# MAGIC IdsEdwAhyEligDExtr
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------

ids_secret_name = get_widget_value('ids_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query_db2_AHY_ELIG_TYP_in = """SELECT 
AHY_ELIG_TYP.AHY_ELIG_TYP_SK,
AHY_ELIG_TYP.AHY_ELIG_TYP_ID,
AHY_ELIG_TYP.AHY_ELIG_TYP_DESC 
FROM #$IDSOwner#.AHY_ELIG_TYP AHY_ELIG_TYP"""
df_db2_AHY_ELIG_TYP_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_AHY_ELIG_TYP_in)
    .load()
)
extract_query_db2_AHY_ELIG_in = """SELECT
AHY_ELIG.AHY_ELIG_SK,
COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
AHY_ELIG.CLNT_SRC_ID,
AHY_ELIG.CLNT_GRP_NO,
AHY_ELIG.CLNT_SUBGRP_NO,
AHY_ELIG.EFF_DT_SK,
AHY_ELIG.CLS_ID,
AHY_ELIG.CRT_RUN_CYC_EXCTN_SK,
AHY_ELIG.LAST_UPDT_RUN_CYC_EXCTN_SK,
AHY_ELIG.AHY_ELIG_TYP_SK,
AHY_ELIG.CLS_SK,
AHY_ELIG.CLS_PLN_SK,
AHY_ELIG.GRP_SK,
AHY_ELIG.SUBGRP_SK,
AHY_ELIG.INCLD_NON_MBR_IN,
AHY_ELIG.TERM_DT_SK,
AHY_ELIG.SRC_SYS_LAST_UPDT_DT_SK,
AHY_ELIG.SRC_SYS_LAST_UPDT_USER_SK
FROM #$IDSOwner#.AHY_ELIG AHY_ELIG
LEFT JOIN #$IDSOwner#.CD_MPPNG CD
ON AHY_ELIG.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
WHERE AHY_ELIG.LAST_UPDT_RUN_CYC_EXCTN_SK >= #IDSRunCycle#"""
df_db2_AHY_ELIG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_AHY_ELIG_in)
    .load()
)
extract_query_db2_CLS_PLN_in = """SELECT
CLS_PLN.CLS_PLN_SK,
CLS_PLN.CLS_PLN_ID
FROM #$IDSOwner#.CLS_PLN CLS_PLN"""
df_db2_CLS_PLN_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CLS_PLN_in)
    .load()
)
extract_query_db2_GRP_in = """SELECT
GRP.GRP_SK,
GRP.GRP_ID
FROM #$IDSOwner#.GRP GRP"""
df_db2_GRP_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_GRP_in)
    .load()
)
extract_query_db2_SUBGRP_in = """SELECT
SUBGRP.SUBGRP_SK,
SUBGRP.SUBGRP_ID
FROM #$IDSOwner#.SUBGRP SUBGRP"""
df_db2_SUBGRP_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_SUBGRP_in)
    .load()
)
df_lkp_Codes = (
    df_db2_AHY_ELIG_in.alias("Ink_IdsEdwAhyEligDExtr_InABC")
    .join(
        df_db2_AHY_ELIG_TYP_in.alias("ref_AHY_ELIG_TYP_SK"),
        F.col("Ink_IdsEdwAhyEligDExtr_InABC.AHY_ELIG_TYP_SK") == F.col("ref_AHY_ELIG_TYP_SK.AHY_ELIG_TYP_SK"),
        "left"
    )
    .join(
        df_db2_CLS_PLN_in.alias("ref_CLS_PLN_SK"),
        F.col("Ink_IdsEdwAhyEligDExtr_InABC.CLS_PLN_SK") == F.col("ref_CLS_PLN_SK.CLS_PLN_SK"),
        "left"
    )
    .join(
        df_db2_GRP_in.alias("ref_GRP_SK"),
        F.col("Ink_IdsEdwAhyEligDExtr_InABC.GRP_SK") == F.col("ref_GRP_SK.GRP_SK"),
        "left"
    )
    .join(
        df_db2_SUBGRP_in.alias("ref_SUBGRP_SK"),
        F.col("Ink_IdsEdwAhyEligDExtr_InABC.SUBGRP_SK") == F.col("ref_SUBGRP_SK.SUBGRP_SK"),
        "left"
    )
    .select(
        F.col("Ink_IdsEdwAhyEligDExtr_InABC.AHY_ELIG_SK").alias("AHY_ELIG_SK"),
        F.col("Ink_IdsEdwAhyEligDExtr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Ink_IdsEdwAhyEligDExtr_InABC.CLNT_SRC_ID").alias("CLNT_SRC_ID"),
        F.col("Ink_IdsEdwAhyEligDExtr_InABC.CLNT_GRP_NO").alias("CLNT_GRP_NO"),
        F.col("Ink_IdsEdwAhyEligDExtr_InABC.CLNT_SUBGRP_NO").alias("CLNT_SUBGRP_NO"),
        F.col("Ink_IdsEdwAhyEligDExtr_InABC.EFF_DT_SK").alias("EFF_DT_SK"),
        F.col("Ink_IdsEdwAhyEligDExtr_InABC.CLS_ID").alias("CLS_ID"),
        F.col("Ink_IdsEdwAhyEligDExtr_InABC.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("Ink_IdsEdwAhyEligDExtr_InABC.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("Ink_IdsEdwAhyEligDExtr_InABC.AHY_ELIG_TYP_SK").alias("AHY_ELIG_TYP_SK"),
        F.col("Ink_IdsEdwAhyEligDExtr_InABC.CLS_SK").alias("CLS_SK"),
        F.col("Ink_IdsEdwAhyEligDExtr_InABC.CLS_PLN_SK").alias("CLS_PLN_SK"),
        F.col("Ink_IdsEdwAhyEligDExtr_InABC.GRP_SK").alias("GRP_SK"),
        F.col("Ink_IdsEdwAhyEligDExtr_InABC.SUBGRP_SK").alias("SUBGRP_SK"),
        F.col("Ink_IdsEdwAhyEligDExtr_InABC.INCLD_NON_MBR_IN").alias("INCLD_NON_MBR_IN"),
        F.col("Ink_IdsEdwAhyEligDExtr_InABC.TERM_DT_SK").alias("TERM_DT_SK"),
        F.col("Ink_IdsEdwAhyEligDExtr_InABC.SRC_SYS_LAST_UPDT_DT_SK").alias("SRC_SYS_LAST_UPDT_DT_SK"),
        F.col("Ink_IdsEdwAhyEligDExtr_InABC.SRC_SYS_LAST_UPDT_USER_SK").alias("SRC_SYS_LAST_UPDT_USER_SK"),
        F.col("ref_AHY_ELIG_TYP_SK.AHY_ELIG_TYP_ID").alias("AHY_ELIG_TYP_ID"),
        F.col("ref_AHY_ELIG_TYP_SK.AHY_ELIG_TYP_DESC").alias("AHY_ELIG_TYP_DESC"),
        F.col("ref_CLS_PLN_SK.CLS_PLN_ID").alias("CLS_PLN_ID"),
        F.col("ref_GRP_SK.GRP_ID").alias("GRP_ID"),
        F.col("ref_SUBGRP_SK.SUBGRP_ID").alias("SUBGRP_ID")
    )
)
df_main = (
    df_lkp_Codes
    .filter((F.col("AHY_ELIG_SK") != 0) & (F.col("AHY_ELIG_SK") != 1))
    .withColumn(
        "SRC_SYS_CD",
        F.when(
            (F.col("SRC_SYS_CD").isNull()) | (F.trim(F.col("SRC_SYS_CD")) == ""),
            "UNK"
        ).otherwise(F.col("SRC_SYS_CD"))
    )
    .withColumn("AHY_ELIG_CLNT_SRC_ID", F.col("CLNT_SRC_ID"))
    .withColumn("AHY_ELIG_CLNT_GRP_NO", F.col("CLNT_GRP_NO"))
    .withColumn("AHY_ELIG_CLNT_SUBGRP_NO", F.col("CLNT_SUBGRP_NO"))
    .withColumn("AHY_ELIG_EFF_DT_SK", F.col("EFF_DT_SK"))
    .withColumn("CLS_ID", F.col("CLS_ID"))
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.lit(EDWRunCycleDate))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.lit(EDWRunCycleDate))
    .withColumn("CLS_SK", F.col("CLS_SK"))
    .withColumn("CLS_PLN_SK", F.col("CLS_PLN_SK"))
    .withColumn("GRP_SK", F.col("GRP_SK"))
    .withColumn("SUBGRP_SK", F.col("SUBGRP_SK"))
    .withColumn("AHY_ELIG_INCLD_NON_MBR_IN", F.col("INCLD_NON_MBR_IN"))
    .withColumn("AHY_ELIG_TERM_DT_SK", F.col("TERM_DT_SK"))
    .withColumn(
        "AHY_ELIG_TYP_ID",
        F.when(
            (F.col("AHY_ELIG_TYP_ID").isNull()) | (F.trim(F.col("AHY_ELIG_TYP_ID")) == ""),
            "NA"
        ).otherwise(F.col("AHY_ELIG_TYP_ID"))
    )
    .withColumn(
        "AHY_ELIG_TYP_DESC",
        F.when(
            (F.col("AHY_ELIG_TYP_DESC").isNull()) | (F.trim(F.col("AHY_ELIG_TYP_DESC")) == ""),
            "NA"
        ).otherwise(F.col("AHY_ELIG_TYP_DESC"))
    )
    .withColumn(
        "CLS_PLN_ID",
        F.when(
            (F.col("CLS_PLN_ID").isNull()) | (F.trim(F.col("CLS_PLN_ID")) == ""),
            "NA"
        ).otherwise(F.col("CLS_PLN_ID"))
    )
    .withColumn(
        "GRP_ID",
        F.when(
            (F.col("GRP_ID").isNull()) | (F.trim(F.col("GRP_ID")) == ""),
            "NA"
        ).otherwise(F.col("GRP_ID"))
    )
    .withColumn(
        "SUBGRP_ID",
        F.when(
            (F.col("SUBGRP_ID").isNull()) | (F.trim(F.col("SUBGRP_ID")) == ""),
            "NA"
        ).otherwise(F.col("SUBGRP_ID"))
    )
    .withColumn("SRC_SYS_LAST_UPDT_DT_SK", F.col("SRC_SYS_LAST_UPDT_DT_SK"))
    .withColumn("SRC_SYS_LAST_UPDT_USER_SK", F.col("SRC_SYS_LAST_UPDT_USER_SK"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(EDWRunCycle))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(EDWRunCycle))
    .withColumn("AHY_ELIG_TYP_SK", F.col("AHY_ELIG_TYP_SK"))
    .select(
        "AHY_ELIG_SK",
        "SRC_SYS_CD",
        "AHY_ELIG_CLNT_SRC_ID",
        "AHY_ELIG_CLNT_GRP_NO",
        "AHY_ELIG_CLNT_SUBGRP_NO",
        "AHY_ELIG_EFF_DT_SK",
        "CLS_ID",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "CLS_SK",
        "CLS_PLN_SK",
        "GRP_SK",
        "SUBGRP_SK",
        "AHY_ELIG_INCLD_NON_MBR_IN",
        "AHY_ELIG_TERM_DT_SK",
        "AHY_ELIG_TYP_ID",
        "AHY_ELIG_TYP_DESC",
        "CLS_PLN_ID",
        "GRP_ID",
        "SUBGRP_ID",
        "SRC_SYS_LAST_UPDT_DT_SK",
        "SRC_SYS_LAST_UPDT_USER_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "AHY_ELIG_TYP_SK"
    )
)
schema_unk = StructType([
    StructField("AHY_ELIG_SK", StringType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("AHY_ELIG_CLNT_SRC_ID", StringType(), True),
    StructField("AHY_ELIG_CLNT_GRP_NO", StringType(), True),
    StructField("AHY_ELIG_CLNT_SUBGRP_NO", StringType(), True),
    StructField("AHY_ELIG_EFF_DT_SK", StringType(), True),
    StructField("CLS_ID", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("CLS_SK", StringType(), True),
    StructField("CLS_PLN_SK", StringType(), True),
    StructField("GRP_SK", StringType(), True),
    StructField("SUBGRP_SK", StringType(), True),
    StructField("AHY_ELIG_INCLD_NON_MBR_IN", StringType(), True),
    StructField("AHY_ELIG_TERM_DT_SK", StringType(), True),
    StructField("AHY_ELIG_TYP_ID", StringType(), True),
    StructField("AHY_ELIG_TYP_DESC", StringType(), True),
    StructField("CLS_PLN_ID", StringType(), True),
    StructField("GRP_ID", StringType(), True),
    StructField("SUBGRP_ID", StringType(), True),
    StructField("SRC_SYS_LAST_UPDT_DT_SK", StringType(), True),
    StructField("SRC_SYS_LAST_UPDT_USER_SK", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", StringType(), True),
    StructField("AHY_ELIG_TYP_SK", StringType(), True)
])
df_UNKRow = spark.createDataFrame(
    [
        {
            "AHY_ELIG_SK": "0",
            "SRC_SYS_CD": "UNK",
            "AHY_ELIG_CLNT_SRC_ID": "UNK",
            "AHY_ELIG_CLNT_GRP_NO": "UNK",
            "AHY_ELIG_CLNT_SUBGRP_NO": "UNK",
            "AHY_ELIG_EFF_DT_SK": "1753-01-01",
            "CLS_ID": "UNK",
            "CRT_RUN_CYC_EXCTN_DT_SK": "1753-01-01",
            "LAST_UPDT_RUN_CYC_EXCTN_DT_SK": EDWRunCycleDate,
            "CLS_SK": "0",
            "CLS_PLN_SK": "0",
            "GRP_SK": "0",
            "SUBGRP_SK": "0",
            "AHY_ELIG_INCLD_NON_MBR_IN": "N",
            "AHY_ELIG_TERM_DT_SK": "1753-01-01",
            "AHY_ELIG_TYP_ID": "UNK",
            "AHY_ELIG_TYP_DESC": "UNK",
            "CLS_PLN_ID": "UNK",
            "GRP_ID": "UNK",
            "SUBGRP_ID": "UNK",
            "SRC_SYS_LAST_UPDT_DT_SK": "1753-01-01",
            "SRC_SYS_LAST_UPDT_USER_SK": "0",
            "CRT_RUN_CYC_EXCTN_SK": "100",
            "LAST_UPDT_RUN_CYC_EXCTN_SK": EDWRunCycle,
            "AHY_ELIG_TYP_SK": "0"
        }
    ],
    schema_unk
).select(df_main.columns)
schema_na = StructType([
    StructField("AHY_ELIG_SK", StringType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("AHY_ELIG_CLNT_SRC_ID", StringType(), True),
    StructField("AHY_ELIG_CLNT_GRP_NO", StringType(), True),
    StructField("AHY_ELIG_CLNT_SUBGRP_NO", StringType(), True),
    StructField("AHY_ELIG_EFF_DT_SK", StringType(), True),
    StructField("CLS_ID", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("CLS_SK", StringType(), True),
    StructField("CLS_PLN_SK", StringType(), True),
    StructField("GRP_SK", StringType(), True),
    StructField("SUBGRP_SK", StringType(), True),
    StructField("AHY_ELIG_INCLD_NON_MBR_IN", StringType(), True),
    StructField("AHY_ELIG_TERM_DT_SK", StringType(), True),
    StructField("AHY_ELIG_TYP_ID", StringType(), True),
    StructField("AHY_ELIG_TYP_DESC", StringType(), True),
    StructField("CLS_PLN_ID", StringType(), True),
    StructField("GRP_ID", StringType(), True),
    StructField("SUBGRP_ID", StringType(), True),
    StructField("SRC_SYS_LAST_UPDT_DT_SK", StringType(), True),
    StructField("SRC_SYS_LAST_UPDT_USER_SK", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", StringType(), True),
    StructField("AHY_ELIG_TYP_SK", StringType(), True)
])
df_NARow = spark.createDataFrame(
    [
        {
            "AHY_ELIG_SK": "1",
            "SRC_SYS_CD": "NA",
            "AHY_ELIG_CLNT_SRC_ID": "NA",
            "AHY_ELIG_CLNT_GRP_NO": "NA",
            "AHY_ELIG_CLNT_SUBGRP_NO": "NA",
            "AHY_ELIG_EFF_DT_SK": "1753-01-01",
            "CLS_ID": "NA",
            "CRT_RUN_CYC_EXCTN_DT_SK": "1753-01-01",
            "LAST_UPDT_RUN_CYC_EXCTN_DT_SK": EDWRunCycleDate,
            "CLS_SK": "1",
            "CLS_PLN_SK": "1",
            "GRP_SK": "1",
            "SUBGRP_SK": "1",
            "AHY_ELIG_INCLD_NON_MBR_IN": "N",
            "AHY_ELIG_TERM_DT_SK": "1753-01-01",
            "AHY_ELIG_TYP_ID": "NA",
            "AHY_ELIG_TYP_DESC": "NA",
            "CLS_PLN_ID": "NA",
            "GRP_ID": "NA",
            "SUBGRP_ID": "NA",
            "SRC_SYS_LAST_UPDT_DT_SK": "1753-01-01",
            "SRC_SYS_LAST_UPDT_USER_SK": "1",
            "CRT_RUN_CYC_EXCTN_SK": "100",
            "LAST_UPDT_RUN_CYC_EXCTN_SK": EDWRunCycle,
            "AHY_ELIG_TYP_SK": "1"
        }
    ],
    schema_na
).select(df_main.columns)
df_fnl_dataLinks = df_main.unionByName(df_UNKRow).unionByName(df_NARow)
df_fnl_dataLinks = df_fnl_dataLinks.withColumn(
    "AHY_ELIG_EFF_DT_SK", F.rpad(F.col("AHY_ELIG_EFF_DT_SK"), 10, " ")
).withColumn(
    "CRT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
).withColumn(
    "AHY_ELIG_INCLD_NON_MBR_IN", F.rpad(F.col("AHY_ELIG_INCLD_NON_MBR_IN"), 1, " ")
).withColumn(
    "AHY_ELIG_TERM_DT_SK", F.rpad(F.col("AHY_ELIG_TERM_DT_SK"), 10, " ")
).withColumn(
    "SRC_SYS_LAST_UPDT_DT_SK", F.rpad(F.col("SRC_SYS_LAST_UPDT_DT_SK"), 10, " ")
)
df_final = df_fnl_dataLinks.select(
    "AHY_ELIG_SK",
    "SRC_SYS_CD",
    "AHY_ELIG_CLNT_SRC_ID",
    "AHY_ELIG_CLNT_GRP_NO",
    "AHY_ELIG_CLNT_SUBGRP_NO",
    "AHY_ELIG_EFF_DT_SK",
    "CLS_ID",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "CLS_SK",
    "CLS_PLN_SK",
    "GRP_SK",
    "SUBGRP_SK",
    "AHY_ELIG_INCLD_NON_MBR_IN",
    "AHY_ELIG_TERM_DT_SK",
    "AHY_ELIG_TYP_ID",
    "AHY_ELIG_TYP_DESC",
    "CLS_PLN_ID",
    "GRP_ID",
    "SUBGRP_ID",
    "SRC_SYS_LAST_UPDT_DT_SK",
    "SRC_SYS_LAST_UPDT_USER_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "AHY_ELIG_TYP_SK"
)
write_files(
    df_final,
    f"{adls_path}/load/AHY_ELIG_D.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)