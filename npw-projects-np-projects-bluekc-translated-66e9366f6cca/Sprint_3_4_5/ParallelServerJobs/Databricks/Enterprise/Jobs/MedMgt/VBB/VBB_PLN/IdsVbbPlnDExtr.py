# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2013 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  EdwVbbExtrSeq
# MAGIC 
# MAGIC PROCESSING: Extracts data from IDS VBB_PLN table and creates a load file for EDW VBB_PLN_D
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date               Project/Altiris #                  Change Description                               Development Project      Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------      ---------------------------------------------------------       ----------------------------------     ---------------------------------    -------------------------   
# MAGIC Kalyan Neelam                2013-05-11    4963 VBB Phase III              Initial Programming                                EnterpriseNewDevl        Bhoomi Dasari              5/17/2013

# MAGIC EDW VBB_PLN extract from IDS
# MAGIC Business Rules that determine Edw Output
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner', '')
ids_secret_name = get_widget_value('ids_secret_name', '')
ExtractRunCycle = get_widget_value('ExtractRunCycle', '')
CurrRunCycleDate = get_widget_value('CurrRunCycleDate', '2007-12-12')
EDWRunCycle = get_widget_value('EDWRunCycle', '0')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = f"""SELECT
             VBB_PLN.VBB_PLN_SK,
             VBB_PLN.VBB_PLN_UNIQ_KEY,
             VBB_PLN.SRC_SYS_CD_SK,
             VBB_PLN.CRT_RUN_CYC_EXCTN_SK,
             VBB_PLN.LAST_UPDT_RUN_CYC_EXCTN_SK,
             VBB_PLN.VBB_MDL_CD_SK,
             VBB_PLN.VBB_PLN_CAT_CD_SK,
             VBB_PLN.VBB_PLN_PROD_CAT_CD_SK,
             VBB_PLN.VBB_PLN_TERM_RSN_CD_SK,
             VBB_PLN.VBB_PLN_STRT_YR_IN,
             VBB_PLN.VBB_PLN_STRT_YR_NO,
             VBB_PLN.VBB_PLN_STRT_DT_SK,
             VBB_PLN.VBB_PLN_END_DT_SK,
             VBB_PLN.VBB_PLN_TERM_DT_SK,
             VBB_PLN.SRC_SYS_CRT_DTM,
             VBB_PLN.SRC_SYS_UPDT_DTM,
             VBB_PLN.VBB_PLN_NM

FROM {IDSOwner}.VBB_PLN VBB_PLN

WHERE
    VBB_PLN.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtractRunCycle};

{IDSOwner}.WELNS_CLS
"""

df_VBB_PLN = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_hf_cdma_codes = spark.read.parquet(f"{adls_path}/hf_cdma_codes.parquet")

df_Ext = df_VBB_PLN.alias("Extract")

df_hf_cdma_codes_1 = df_hf_cdma_codes.select("CD_MPPNG_SK", "TRGT_CD", "TRGT_CD_NM").alias("SrcSysCdLkup")
df_hf_cdma_codes_2 = df_hf_cdma_codes.select("CD_MPPNG_SK", "TRGT_CD", "TRGT_CD_NM").alias("Pln_Prod_Cat_cd")
df_hf_cdma_codes_3 = df_hf_cdma_codes.select("CD_MPPNG_SK", "TRGT_CD", "TRGT_CD_NM").alias("Pln_Cat_cd")
df_hf_cdma_codes_4 = df_hf_cdma_codes.select("CD_MPPNG_SK", "TRGT_CD", "TRGT_CD_NM").alias("Mdl_cd")
df_hf_cdma_codes_5 = df_hf_cdma_codes.select("CD_MPPNG_SK", "TRGT_CD", "TRGT_CD_NM").alias("Pln_Term_Rsn_Cd")

df_bl = (
    df_Ext
    .join(df_hf_cdma_codes_1.alias("SrcSysCdLkup"),
          F.col("Extract.SRC_SYS_CD_SK") == F.col("SrcSysCdLkup.CD_MPPNG_SK"), "left")
    .join(df_hf_cdma_codes_2.alias("Pln_Prod_Cat_cd"),
          F.col("Extract.VBB_PLN_PROD_CAT_CD_SK") == F.col("Pln_Prod_Cat_cd.CD_MPPNG_SK"), "left")
    .join(df_hf_cdma_codes_3.alias("Pln_Cat_cd"),
          F.col("Extract.VBB_PLN_CAT_CD_SK") == F.col("Pln_Cat_cd.CD_MPPNG_SK"), "left")
    .join(df_hf_cdma_codes_4.alias("Mdl_cd"),
          F.col("Extract.VBB_MDL_CD_SK") == F.col("Mdl_cd.CD_MPPNG_SK"), "left")
    .join(df_hf_cdma_codes_5.alias("Pln_Term_Rsn_Cd"),
          F.col("Extract.VBB_PLN_TERM_RSN_CD_SK") == F.col("Pln_Term_Rsn_Cd.CD_MPPNG_SK"), "left")
)

df_enriched = df_bl.select(
    F.col("Extract.VBB_PLN_SK").alias("VBB_PLN_SK"),
    F.col("Extract.VBB_PLN_UNIQ_KEY").alias("VBB_PLN_UNIQ_KEY"),
    F.when(F.col("SrcSysCdLkup.TRGT_CD").isNull(), "UNK").otherwise(F.col("SrcSysCdLkup.TRGT_CD")).alias("SRC_SYS_CD"),
    F.lit(CurrRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(CurrRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.when(F.col("Mdl_cd.TRGT_CD").isNull(), "UNK").otherwise(F.col("Mdl_cd.TRGT_CD")).alias("VBB_MDL_CD"),
    F.when(F.col("Mdl_cd.TRGT_CD_NM").isNull(), "UNK").otherwise(F.col("Mdl_cd.TRGT_CD_NM")).alias("VBB_MDL_NM"),
    F.when(F.col("Pln_Cat_cd.TRGT_CD").isNull(), "UNK").otherwise(F.col("Pln_Cat_cd.TRGT_CD")).alias("VBB_PLN_CAT_CD"),
    F.when(F.col("Pln_Cat_cd.TRGT_CD_NM").isNull(), "UNK").otherwise(F.col("Pln_Cat_cd.TRGT_CD_NM")).alias("VBB_PLN_CAT_NM"),
    F.when(F.col("Pln_Prod_Cat_cd.TRGT_CD").isNull(), "UNK").otherwise(F.col("Pln_Prod_Cat_cd.TRGT_CD")).alias("VBB_PLN_PROD_CAT_CD"),
    F.when(F.col("Pln_Prod_Cat_cd.TRGT_CD_NM").isNull(), "UNK").otherwise(F.col("Pln_Prod_Cat_cd.TRGT_CD_NM")).alias("VBB_PLN_PROD_CAT_NM"),
    F.when(F.col("Pln_Term_Rsn_Cd.TRGT_CD").isNull(), "UNK").otherwise(F.col("Pln_Term_Rsn_Cd.TRGT_CD")).alias("VBB_PLN_TERM_RSN_CD"),
    F.when(F.col("Pln_Term_Rsn_Cd.TRGT_CD_NM").isNull(), "UNK").otherwise(F.col("Pln_Term_Rsn_Cd.TRGT_CD_NM")).alias("VBB_PLN_TERM_RSN_NM"),
    F.col("Extract.VBB_PLN_STRT_YR_IN").alias("VBB_PLN_STRT_YR_IN"),
    F.col("Extract.VBB_PLN_STRT_YR_NO").alias("VBB_PLN_STRT_YR_NO"),
    F.col("Extract.VBB_PLN_STRT_DT_SK").alias("VBB_PLN_STRT_DT_SK"),
    F.col("Extract.VBB_PLN_END_DT_SK").alias("VBB_PLN_END_DT_SK"),
    F.col("Extract.VBB_PLN_TERM_DT_SK").alias("VBB_PLN_TERM_DT_SK"),
    F.col("Extract.SRC_SYS_CRT_DTM").alias("SRC_SYS_CRT_DTM"),
    F.col("Extract.SRC_SYS_UPDT_DTM").alias("SRC_SYS_UPDT_DTM"),
    F.col("Extract.VBB_PLN_NM").alias("VBB_PLN_NM"),
    F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Extract.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Extract.VBB_MDL_CD_SK").alias("VBB_MDL_CD_SK"),
    F.col("Extract.VBB_PLN_CAT_CD_SK").alias("VBB_PLN_CAT_CD_SK"),
    F.col("Extract.VBB_PLN_PROD_CAT_CD_SK").alias("VBB_PLN_PROD_CAT_CD_SK"),
    F.col("Extract.VBB_PLN_TERM_RSN_CD_SK").alias("VBB_PLN_TERM_RSN_CD_SK")
)

df_final = (
    df_enriched
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("VBB_PLN_STRT_YR_IN", rpad(F.col("VBB_PLN_STRT_YR_IN"), 1, " "))
    .withColumn("VBB_PLN_STRT_DT_SK", rpad(F.col("VBB_PLN_STRT_DT_SK"), 10, " "))
    .withColumn("VBB_PLN_END_DT_SK", rpad(F.col("VBB_PLN_END_DT_SK"), 10, " "))
    .withColumn("VBB_PLN_TERM_DT_SK", rpad(F.col("VBB_PLN_TERM_DT_SK"), 10, " "))
    .withColumn("SRC_SYS_CD", rpad(F.col("SRC_SYS_CD"), <...>, " "))
    .withColumn("VBB_MDL_CD", rpad(F.col("VBB_MDL_CD"), <...>, " "))
    .withColumn("VBB_MDL_NM", rpad(F.col("VBB_MDL_NM"), <...>, " "))
    .withColumn("VBB_PLN_CAT_CD", rpad(F.col("VBB_PLN_CAT_CD"), <...>, " "))
    .withColumn("VBB_PLN_CAT_NM", rpad(F.col("VBB_PLN_CAT_NM"), <...>, " "))
    .withColumn("VBB_PLN_PROD_CAT_CD", rpad(F.col("VBB_PLN_PROD_CAT_CD"), <...>, " "))
    .withColumn("VBB_PLN_PROD_CAT_NM", rpad(F.col("VBB_PLN_PROD_CAT_NM"), <...>, " "))
    .withColumn("VBB_PLN_TERM_RSN_CD", rpad(F.col("VBB_PLN_TERM_RSN_CD"), <...>, " "))
    .withColumn("VBB_PLN_TERM_RSN_NM", rpad(F.col("VBB_PLN_TERM_RSN_NM"), <...>, " "))
)

write_files(
    df_final,
    f"{adls_path}/load/VBB_PLN_D.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)