# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC PROCESSING:
# MAGIC                   This pulls Facets & Nasco Customer Service information to load into the IDS driver table 'W_CUST_SVC_DRVR'.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                       Date               Project/Altiris #               Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    -------------------------------------                      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Ralph Tucker                  3/16/2007           3028                          Originally Programmed                          devlIDS30                   
# MAGIC              
# MAGIC Bhupinder Kaur               12/10/2013          5114                         Rewrite in Parallel                                 EnterpriseWhseDevl     Jag Yelavarthi              2014-01-29

# MAGIC This load file drive the IDS extracts from each CustSvc table.
# MAGIC Job: IdsEdwCustSvcDriverExtr
# MAGIC Data extracted from IDS table                CUST_SVC  and CD_MPPNG
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as psf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
FctsRunCycle = get_widget_value('FctsRunCycle','')
NascoRunCycle = get_widget_value('NascoRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query_facets = f"""
SELECT cs.SRC_SYS_CD_SK,
       cd.TRGT_CD,
       cs.CUST_SVC_ID,
       cs.LAST_UPDT_RUN_CYC_EXCTN_SK
FROM {IDSOwner}.CUST_SVC cs,
     {IDSOwner}.CD_MPPNG cd
WHERE cs.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'FACETS'
  AND cs.LAST_UPDT_RUN_CYC_EXCTN_SK >= {FctsRunCycle}
  AND cs.SRC_SYS_CD_SK NOT IN (0,1)

UNION

SELECT cs.SRC_SYS_CD_SK,
       'UNK',
       cs.CUST_SVC_ID,
       cs.LAST_UPDT_RUN_CYC_EXCTN_SK
FROM {IDSOwner}.CUST_SVC cs
WHERE cs.CUST_SVC_SK = 0
  AND cs.SRC_SYS_CD_SK NOT IN (0,1)

UNION

SELECT cs.SRC_SYS_CD_SK,
       'NA',
       cs.CUST_SVC_ID,
       cs.LAST_UPDT_RUN_CYC_EXCTN_SK
FROM {IDSOwner}.CUST_SVC cs
WHERE cs.CUST_SVC_SK = 1
  AND cs.SRC_SYS_CD_SK NOT IN (0,1)

UNION

SELECT cst.SRC_SYS_CD_SK,
       cd.TRGT_CD,
       cst.CUST_SVC_ID,
       cst.LAST_UPDT_RUN_CYC_EXCTN_SK
FROM {IDSOwner}.CUST_SVC_TASK cst,
     {IDSOwner}.CD_MPPNG cd
WHERE cst.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'FACETS'
  AND cst.CLSD_DT_SK = '1753-01-01'
  AND cst.SRC_SYS_CD_SK NOT IN (0,1)
"""

df_db2_CUST_SVC_Facets_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_facets)
    .load()
)

extract_query_nasco = f"""
SELECT cs.SRC_SYS_CD_SK,
       cd.TRGT_CD,
       cs.CUST_SVC_ID,
       cs.LAST_UPDT_RUN_CYC_EXCTN_SK
FROM {IDSOwner}.CUST_SVC cs,
     {IDSOwner}.CD_MPPNG cd
WHERE cs.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'NPS'
  AND cs.LAST_UPDT_RUN_CYC_EXCTN_SK >= {NascoRunCycle}
  AND cs.SRC_SYS_CD_SK NOT IN (0,1)
"""

df_db2_CUST_SVC_Nasco_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_nasco)
    .load()
)

df_Fnl_CustSvc = df_db2_CUST_SVC_Nasco_in.select(
    psf.col("SRC_SYS_CD_SK"),
    psf.col("TRGT_CD"),
    psf.col("CUST_SVC_ID"),
    psf.col("LAST_UPDT_RUN_CYC_EXCTN_SK")
).union(
    df_db2_CUST_SVC_Facets_in.select(
        psf.col("SRC_SYS_CD_SK"),
        psf.col("TRGT_CD"),
        psf.col("CUST_SVC_ID"),
        psf.col("LAST_UPDT_RUN_CYC_EXCTN_SK")
    )
)

df_Rm_Dup_CustSvcId = dedup_sort(
    df_Fnl_CustSvc,
    ["SRC_SYS_CD_SK", "CUST_SVC_ID"],
    [("SRC_SYS_CD_SK", "A"), ("CUST_SVC_ID", "A")]
)

df_enriched = df_Rm_Dup_CustSvcId.withColumn(
    "SRC_SYS_CD_SK",
    psf.col("SRC_SYS_CD_SK")
).withColumn(
    "CUST_SVC_ID",
    psf.col("CUST_SVC_ID")
).withColumn(
    "SRC_SYS_CD",
    psf.when(
        psf.col("TRGT_CD").isNull() |
        (psf.length(trim(psf.col("TRGT_CD"))) == 0),
        psf.lit("NA")
    ).otherwise(psf.col("TRGT_CD"))
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    psf.col("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

write_files(
    df_enriched.select(
        "SRC_SYS_CD_SK",
        "CUST_SVC_ID",
        "SRC_SYS_CD",
        "LAST_UPDT_RUN_CYC_EXCTN_SK"
    ),
    f"{adls_path}/load/W_CUST_SVC_DRVR.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)