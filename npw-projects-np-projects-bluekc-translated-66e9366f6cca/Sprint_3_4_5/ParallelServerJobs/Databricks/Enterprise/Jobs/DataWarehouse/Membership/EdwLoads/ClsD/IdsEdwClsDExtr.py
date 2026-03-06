# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC               Tom Harrocks 08/02/2004-   Originally Programmed
# MAGIC               Steph Goddard 08/16/2005   Rewritten for EDW 2.0
# MAGIC               Brent Leland     11/08/2005   Move GRP_SK column to standard order.
# MAGIC               Suzanne Saylor  04/12/2006 - changed > #BeginCycle# to >=, added RunCycle parameter, set LAST_RUN_CYC_EXCTN_SK to RunCycle
# MAGIC               Sharon Andrew   07/07/2006    Balancing.   
# MAGIC 				Changed the value moved to the EDW Last Activity Run Cycle to be the EDW Run Cyle No from the P_RUN_CYC table and not the IDS' Last Activity Run Cycle.   
# MAGIC 			               Renamed from EdwClsExtr to IdsClsExtr
# MAGIC 				Removed inner join within the main extraction of GRP with CLS.  Each will be sperate extraction
# MAGIC              Rama Kamjula     06/25/2013    Converted from server to parallel version

# MAGIC Extract Class data from CLS table in IDS
# MAGIC Load file for class data to load into EDW.
# MAGIC Extract GRP_NM from GRP table in IDS.
# MAGIC Extract TRGT_CD and TRGT_NM from CD_MPPNG in IDS
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


IDSOwner = get_widget_value("IDSOwner", "")
ids_secret_name = get_widget_value("ids_secret_name", "")
EDWRunCycle = get_widget_value("EDWRunCycle", "")
EDWRunCycleDate = get_widget_value("EDWRunCycleDate", "")

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query_db2_CLS_In = f"""
SELECT
CLS.CLS_SK,
COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
CLS.GRP_ID,
CLS.CLS_ID,
CLS.GRP_SK,
CLS.CLS_DESC,
CLS.CLS_MCAID_CAT_OF_AID_CD_SK,
CLS.MCAID_ELIG_TTL
FROM {IDSOwner}.CLS CLS
LEFT JOIN {IDSOwner}.CD_MPPNG CD
ON CLS.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
"""

df_db2_CLS_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CLS_In)
    .load()
)

extract_query_db2_CD_MPPNG_In = f"""
SELECT
CD.CD_MPPNG_SK,
CD.TRGT_CD,
CD.TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG CD
"""

df_db2_CD_MPPNG_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CD_MPPNG_In)
    .load()
)

extract_query_db2_GRP_In = f"""
SELECT
GRP.GRP_SK,
GRP.GRP_ID,
GRP.GRP_NM
FROM {IDSOwner}.GRP GRP
"""

df_db2_GRP_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_GRP_In)
    .load()
)

df_lkp_codes_joined = (
    df_db2_CLS_In.alias("lnk_Cls_In")
    .join(
        df_db2_CD_MPPNG_In.alias("lnk_Cd_Mppng_In"),
        F.col("lnk_Cls_In.CLS_MCAID_CAT_OF_AID_CD_SK") == F.col("lnk_Cd_Mppng_In.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_db2_GRP_In.alias("lnk_Grp_In"),
        F.col("lnk_Cls_In.GRP_SK") == F.col("lnk_Grp_In.GRP_SK"),
        "left"
    )
)

df_lkp_out = df_lkp_codes_joined.select(
    F.col("lnk_Grp_In.GRP_NM").alias("GRP_NM"),
    F.col("lnk_Cls_In.CLS_SK").alias("CLS_SK"),
    F.col("lnk_Cls_In.GRP_ID").alias("GRP_ID"),
    F.col("lnk_Cls_In.CLS_ID").alias("CLS_ID"),
    F.col("lnk_Cls_In.GRP_SK").alias("GRP_SK"),
    F.col("lnk_Cls_In.CLS_DESC").alias("CLS_DESC"),
    F.col("lnk_Cls_In.CLS_MCAID_CAT_OF_AID_CD_SK").alias("CLS_MCAID_CAT_OF_AID_CD_SK"),
    F.col("lnk_Cls_In.MCAID_ELIG_TTL").alias("MCAID_ELIG_TTL"),
    F.col("lnk_Cd_Mppng_In.TRGT_CD").alias("TRGT_CD"),
    F.col("lnk_Cd_Mppng_In.TRGT_CD_NM").alias("TRGT_CD_NM"),
    F.col("lnk_Cls_In.SRC_SYS_CD").alias("SRC_SYS_CD")
)

df_enriched = (
    df_lkp_out
    .withColumn("CLS_SK", F.col("CLS_SK"))
    .withColumn("SRC_SYS_CD", trim(F.col("SRC_SYS_CD")))
    .withColumn("GRP_ID", trim(F.col("GRP_ID")))
    .withColumn("CLS_ID", trim(F.col("CLS_ID")))
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", rpad(F.lit(EDWRunCycleDate), 10, " "))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", rpad(F.lit(EDWRunCycleDate), 10, " "))
    .withColumn("GRP_SK", F.col("GRP_SK"))
    .withColumn("CLS_DESC", trim(F.col("CLS_DESC")))
    .withColumn(
        "CLS_MCAID_CAT_OF_AID_CD",
        F.when(F.col("TRGT_CD").isNull(), F.lit("UNK")).otherwise(F.col("TRGT_CD"))
    )
    .withColumn(
        "CLS_MCAID_CAT_OF_AID_NM",
        F.when(trim(F.col("TRGT_CD_NM")) == "", F.lit("UNK")).otherwise(F.col("TRGT_CD_NM"))
    )
    .withColumn("CLS_MCAID_ELIG_TTL", trim(F.col("MCAID_ELIG_TTL")))
    .withColumn(
        "GRP_NM",
        F.when(trim(F.col("GRP_NM")) == "", F.lit("UNK")).otherwise(F.col("GRP_NM"))
    )
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(EDWRunCycle))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(EDWRunCycle))
    .withColumn("CLS_MCAID_CAT_OF_AID_CD_SK", F.col("CLS_MCAID_CAT_OF_AID_CD_SK"))
)

df_final = df_enriched.select(
    "CLS_SK",
    "SRC_SYS_CD",
    "GRP_ID",
    "CLS_ID",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "GRP_SK",
    "CLS_DESC",
    "CLS_MCAID_CAT_OF_AID_CD",
    "CLS_MCAID_CAT_OF_AID_NM",
    "CLS_MCAID_ELIG_TTL",
    "GRP_NM",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CLS_MCAID_CAT_OF_AID_CD_SK"
)

write_files(
    df_final,
    f"{adls_path}/load/CLS_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)