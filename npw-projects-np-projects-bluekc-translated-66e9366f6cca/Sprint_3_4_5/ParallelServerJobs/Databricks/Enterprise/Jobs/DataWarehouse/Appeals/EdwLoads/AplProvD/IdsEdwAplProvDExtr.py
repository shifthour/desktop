# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC ******************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:
# MAGIC 
# MAGIC PROCESSING:
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date               Project/Altiris #               Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------   ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Naren Garapaty                08/02/2007                                              Originally Programmed                             devlEDW10              
# MAGIC 
# MAGIC 
# MAGIC Pooja Sunkara                  11/05/2013      5114                                Rewrite in Parallel                               EnterpriseWrhsDevl  Peter Marshall              1/14/2014

# MAGIC CD_MPPNG Lookup
# MAGIC Add CRT_RUN_CYC_EXCTN_DT_SK and LAST_UPDT_RUN_CYC_EXCTN_DT_SK
# MAGIC Job name:
# MAGIC IdsEdwAplProvDExtr
# MAGIC EDW APL_PROV_D Extract
# MAGIC Write APL_PROV_D Data into a Sequential file for Load Ready Job.
# MAGIC Read data from source table APL_PROV
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
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


# Parameter definitions
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
ExtractRunCycle = get_widget_value('ExtractRunCycle','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')

# Database config
jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# --------------------------------------------------------------------------------
# db2_APL_PROV_in
# --------------------------------------------------------------------------------
query_db2_APL_PROV_in = f"""
SELECT
APL_PROV.APL_PROV_SK,
APL_PROV.SRC_SYS_CD_SK,
APL_PROV.APL_ID,
APL_PROV.SEQ_NO,
APL_PROV.CRT_RUN_CYC_EXCTN_SK,
APL_PROV.LAST_UPDT_RUN_CYC_EXCTN_SK,
APL_PROV.APL_SK,
APL_PROV.PROV_SK,
APL_PROV.APL_PROV_RELSHP_RSN_CD_SK,
APL_PROV.PROV_GRP_PROV_SK
FROM {IDSOwner}.APL_PROV APL_PROV
WHERE
APL_PROV.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtractRunCycle}
"""
df_db2_APL_PROV_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_db2_APL_PROV_in)
    .load()
)

# --------------------------------------------------------------------------------
# db2_FCLTY_TYP_CD_in
# --------------------------------------------------------------------------------
query_db2_FCLTY_TYP_CD_in = f"""
SELECT
APL_PROV.APL_PROV_SK,
FCLTY_TYP_CD.FCLTY_TYP_CD,
FCLTY_TYP_CD.FCLTY_TYP_NM
FROM {IDSOwner}.APL_PROV APL_PROV,
     {IDSOwner}.PROV PROV,
     {IDSOwner}.FCLTY_TYP_CD FCLTY_TYP_CD
WHERE
APL_PROV.PROV_SK=PROV.PROV_SK AND
PROV.PROV_FCLTY_TYP_CD_SK=FCLTY_TYP_CD.FCLTY_TYP_CD_SK
"""
df_db2_FCLTY_TYP_CD_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_db2_FCLTY_TYP_CD_in)
    .load()
)

# --------------------------------------------------------------------------------
# db2_PROV_TYP_CD_in
# --------------------------------------------------------------------------------
query_db2_PROV_TYP_CD_in = f"""
SELECT
APL_PROV.APL_PROV_SK,
PROV_TYP_CD.PROV_TYP_CD,
PROV_TYP_CD.PROV_TYP_NM
FROM {IDSOwner}.APL_PROV APL_PROV,
     {IDSOwner}.PROV PROV,
     {IDSOwner}.PROV_TYP_CD PROV_TYP_CD
WHERE
APL_PROV.PROV_SK=PROV.PROV_SK AND
PROV.PROV_TYP_CD_SK=PROV_TYP_CD.PROV_TYP_CD_SK
ORDER BY
APL_PROV.APL_PROV_SK
"""
df_db2_PROV_TYP_CD_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_db2_PROV_TYP_CD_in)
    .load()
)

# --------------------------------------------------------------------------------
# db2_PROV_SPEC_CD_in
# --------------------------------------------------------------------------------
query_db2_PROV_SPEC_CD_in = f"""
SELECT
APL_PROV.APL_PROV_SK,
PROV_SPEC_CD.PROV_SPEC_CD,
PROV_SPEC_CD.PROV_SPEC_NM
FROM {IDSOwner}.APL_PROV APL_PROV,
     {IDSOwner}.PROV PROV,
     {IDSOwner}.PROV_SPEC_CD PROV_SPEC_CD
WHERE
APL_PROV.PROV_SK=PROV.PROV_SK AND
PROV.PROV_SPEC_CD_SK=PROV_SPEC_CD.PROV_SPEC_CD_SK
"""
df_db2_PROV_SPEC_CD_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_db2_PROV_SPEC_CD_in)
    .load()
)

# --------------------------------------------------------------------------------
# db2_PROVGrp_in
# --------------------------------------------------------------------------------
query_db2_PROVGrp_in = f"""
SELECT
APL_PROV.APL_PROV_SK,
PROV.PROV_ID,
PROV.PROV_NM
FROM {IDSOwner}.APL_PROV APL_PROV,
     {IDSOwner}.PROV PROV
WHERE
APL_PROV.PROV_GRP_PROV_SK=PROV.PROV_SK
"""
df_db2_PROVGrp_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_db2_PROVGrp_in)
    .load()
)

# --------------------------------------------------------------------------------
# db2_PROV_in
# --------------------------------------------------------------------------------
query_db2_PROV_in = f"""
SELECT
APL_PROV.APL_PROV_SK,
PROV.PROV_ID,
PROV.PROV_ENTY_CD_SK,
PROV.PROV_FCLTY_TYP_CD_SK,
PROV.PROV_SPEC_CD_SK,
PROV.PROV_TYP_CD_SK,
PROV.PROV_NM
FROM {IDSOwner}.APL_PROV APL_PROV,
     {IDSOwner}.PROV PROV
WHERE
APL_PROV.PROV_SK=PROV.PROV_SK
"""
df_db2_PROV_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_db2_PROV_in)
    .load()
)

# --------------------------------------------------------------------------------
# db2_CDMPNG_in
# --------------------------------------------------------------------------------
query_db2_CDMPNG_in = f"""
SELECT
APL_PROV.APL_PROV_SK,
CD_MPPNG.TRGT_CD,
CD_MPPNG.TRGT_CD_NM
FROM {IDSOwner}.APL_PROV APL_PROV,
     {IDSOwner}.PROV PROV,
     {IDSOwner}.CD_MPPNG CD_MPPNG
WHERE
APL_PROV.PROV_SK=PROV.PROV_SK AND
PROV.PROV_ENTY_CD_SK=CD_MPPNG.CD_MPPNG_SK
"""
df_db2_CDMPNG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_db2_CDMPNG_in)
    .load()
)

# --------------------------------------------------------------------------------
# db2_CD_MPPNG_in
# --------------------------------------------------------------------------------
query_db2_CD_MPPNG_in = f"""
SELECT
CD_MPPNG_SK,
TRGT_CD,
TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG
"""
df_db2_CD_MPPNG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_db2_CD_MPPNG_in)
    .load()
)

# --------------------------------------------------------------------------------
# Cpy_CdMppng (PxCopy)
# --------------------------------------------------------------------------------
df_Cpy_CdMppng_in = df_db2_CD_MPPNG_in

df_ref_AplLvlSttusCd = df_Cpy_CdMppng_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_ref_SrcSysCd = df_Cpy_CdMppng_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

# --------------------------------------------------------------------------------
# Lkp_Codes (PxLookup)
# --------------------------------------------------------------------------------
df_Lkp_Codes = (
    df_db2_APL_PROV_in.alias("Ink_IdsEdwAplProvDExtr_inABC")
    .join(
        df_ref_SrcSysCd.alias("ref_SrcSysCd"),
        F.col("Ink_IdsEdwAplProvDExtr_inABC.SRC_SYS_CD_SK") == F.col("ref_SrcSysCd.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_ref_AplLvlSttusCd.alias("ref_AplLvlSttusCd"),
        F.col("Ink_IdsEdwAplProvDExtr_inABC.APL_PROV_RELSHP_RSN_CD_SK") == F.col("ref_AplLvlSttusCd.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_db2_FCLTY_TYP_CD_in.alias("ref_FcltyTypCd"),
        F.col("Ink_IdsEdwAplProvDExtr_inABC.APL_PROV_SK") == F.col("ref_FcltyTypCd.APL_PROV_SK"),
        "left"
    )
    .join(
        df_db2_PROV_TYP_CD_in.alias("ref_ProvTypCd"),
        F.col("Ink_IdsEdwAplProvDExtr_inABC.APL_PROV_SK") == F.col("ref_ProvTypCd.APL_PROV_SK"),
        "left"
    )
    .join(
        df_db2_PROV_SPEC_CD_in.alias("ref_ProvSpecCd"),
        F.col("Ink_IdsEdwAplProvDExtr_inABC.APL_PROV_SK") == F.col("ref_ProvSpecCd.APL_PROV_SK"),
        "left"
    )
    .join(
        df_db2_PROVGrp_in.alias("ref_Provgrp"),
        F.col("Ink_IdsEdwAplProvDExtr_inABC.APL_PROV_SK") == F.col("ref_Provgrp.APL_PROV_SK"),
        "left"
    )
    .join(
        df_db2_PROV_in.alias("ref_Prov"),
        F.col("Ink_IdsEdwAplProvDExtr_inABC.APL_PROV_SK") == F.col("ref_Prov.APL_PROV_SK"),
        "left"
    )
    .join(
        df_db2_CDMPNG_in.alias("ref_Enty"),
        F.col("Ink_IdsEdwAplProvDExtr_inABC.APL_PROV_SK") == F.col("ref_Enty.APL_PROV_SK"),
        "left"
    )
    .select(
        F.col("Ink_IdsEdwAplProvDExtr_inABC.APL_PROV_SK").alias("APL_PROV_SK"),
        F.col("Ink_IdsEdwAplProvDExtr_inABC.APL_ID").alias("APL_ID"),
        F.col("Ink_IdsEdwAplProvDExtr_inABC.SEQ_NO").alias("SEQ_NO"),
        F.col("Ink_IdsEdwAplProvDExtr_inABC.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("Ink_IdsEdwAplProvDExtr_inABC.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("Ink_IdsEdwAplProvDExtr_inABC.APL_SK").alias("APL_SK"),
        F.col("Ink_IdsEdwAplProvDExtr_inABC.PROV_SK").alias("PROV_SK"),
        F.col("Ink_IdsEdwAplProvDExtr_inABC.APL_PROV_RELSHP_RSN_CD_SK").alias("APL_PROV_RELSHP_RSN_CD_SK"),
        F.col("Ink_IdsEdwAplProvDExtr_inABC.PROV_GRP_PROV_SK").alias("PROV_GRP_PROV_SK"),
        F.col("ref_SrcSysCd.TRGT_CD").alias("SRC_SYS_CD"),
        F.col("ref_AplLvlSttusCd.TRGT_CD").alias("APL_PROV_RELSHP_RSN_CD"),
        F.col("ref_AplLvlSttusCd.TRGT_CD_NM").alias("APL_PROV_RELSHP_RSN_NM"),
        F.col("ref_FcltyTypCd.FCLTY_TYP_CD").alias("FCLTY_TYP_CD"),
        F.col("ref_FcltyTypCd.FCLTY_TYP_NM").alias("FCLTY_TYP_NM"),
        F.col("ref_ProvTypCd.PROV_TYP_CD").alias("PROV_TYP_CD"),
        F.col("ref_ProvTypCd.PROV_TYP_NM").alias("PROV_TYP_NM"),
        F.col("ref_ProvSpecCd.PROV_SPEC_CD").alias("PROV_SPEC_CD"),
        F.col("ref_ProvSpecCd.PROV_SPEC_NM").alias("PROV_SPEC_NM"),
        F.col("ref_Provgrp.PROV_ID").alias("PROV_GRP_PROV_ID"),
        F.col("ref_Provgrp.PROV_NM").alias("PROV_GRP_PROV_NM"),
        F.col("ref_Prov.PROV_ID").alias("PROV_ID"),
        F.col("ref_Prov.PROV_ENTY_CD_SK").alias("PROV_ENTY_CD_SK"),
        F.col("ref_Prov.PROV_FCLTY_TYP_CD_SK").alias("FCLTY_TYP_SK"),
        F.col("ref_Prov.PROV_SPEC_CD_SK").alias("PROV_SPEC_SK"),
        F.col("ref_Prov.PROV_TYP_CD_SK").alias("PROV_TYP_SK"),
        F.col("ref_Prov.PROV_NM").alias("PROV_NM"),
        F.col("ref_Enty.TRGT_CD").alias("PROV_ENTY_TYP_CD"),
        F.col("ref_Enty.TRGT_CD_NM").alias("PROV_ENTY_TYP_NM"),
    )
)

# --------------------------------------------------------------------------------
# xfrm_BusinessLogic (CTransformerStage)
# We need to produce three outputs from the same input:
#   1) outMain (filter by APL_PROV_SK <> 0 and <> 1)
#   2) NALink (only first row overall)
#   3) UNKLink (only first row overall)
# --------------------------------------------------------------------------------
df_temp = df_Lkp_Codes.withColumn(
    "row_no", F.row_number().over(Window.orderBy(F.lit(1)))
)

# outMain
df_xfrm_BusinessLogic_outMain = (
    df_temp.filter((F.col("APL_PROV_SK") != 0) & (F.col("APL_PROV_SK") != 1))
    .withColumn(
        "CRT_RUN_CYC_EXCTN_DT_SK", F.lit(EDWRunCycleDate)
    )
    .withColumn(
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.lit(EDWRunCycleDate)
    )
    .withColumn(
        "FCLTY_TYP_SK",
        F.when(F.col("FCLTY_TYP_SK").isNull(), F.lit(0)).otherwise(F.col("FCLTY_TYP_SK"))
    )
    .withColumn(
        "PROV_TYP_SK",
        F.when(F.col("PROV_TYP_SK").isNull(), F.lit(0)).otherwise(F.col("PROV_TYP_SK"))
    )
    .withColumn(
        "PROV_SPEC_SK",
        F.when(F.col("PROV_SPEC_SK").isNull(), F.lit(0)).otherwise(F.col("PROV_SPEC_SK"))
    )
    .withColumn(
        "APL_PROV_RELSHP_RSN_CD",
        F.when(F.col("APL_PROV_RELSHP_RSN_CD").isNull(), F.lit("NA")).otherwise(F.col("APL_PROV_RELSHP_RSN_CD"))
    )
    .withColumn(
        "APL_PROV_RELSHP_RSN_NM",
        F.when(F.col("APL_PROV_RELSHP_RSN_NM").isNull(), F.lit("NA")).otherwise(F.col("APL_PROV_RELSHP_RSN_NM"))
    )
    .withColumn(
        "FCLTY_TYP_CD",
        F.when(F.col("FCLTY_TYP_CD").isNull(), F.lit("NA")).otherwise(F.col("FCLTY_TYP_CD"))
    )
    .withColumn(
        "FCLTY_TYP_NM",
        F.when(F.col("FCLTY_TYP_NM").isNull(), F.lit("NA")).otherwise(F.col("FCLTY_TYP_NM"))
    )
    .withColumn(
        "PROV_ENTY_CD",
        F.when(F.col("PROV_ENTY_TYP_CD").isNull(), F.lit("NA")).otherwise(F.col("PROV_ENTY_TYP_CD"))
    )
    .withColumn(
        "PROV_ENTY_NM",
        F.when(F.col("PROV_ENTY_TYP_NM").isNull(), F.lit("NA")).otherwise(F.col("PROV_ENTY_TYP_NM"))
    )
    .withColumn(
        "PROV_GRP_PROV_ID",
        F.when(F.col("PROV_GRP_PROV_ID").isNull(), F.lit("NA")).otherwise(F.col("PROV_GRP_PROV_ID"))
    )
    .withColumn(
        "PROV_GRP_PROV_NM",
        F.when(F.col("PROV_GRP_PROV_NM").isNull(), F.lit("NA")).otherwise(F.col("PROV_GRP_PROV_NM"))
    )
    .withColumn(
        "PROV_ID",
        F.when(F.col("PROV_ID").isNull(), F.lit("NA")).otherwise(F.col("PROV_ID"))
    )
    .withColumn(
        "PROV_NM",
        F.when(F.col("PROV_NM").isNull(), F.lit("NA")).otherwise(F.col("PROV_NM"))
    )
    .withColumn(
        "PROV_TYP_CD",
        F.when((F.col("PROV_TYP_CD").isNull()) | (F.length(F.col("PROV_TYP_CD")) == 0), F.lit("NA")).otherwise(F.col("PROV_TYP_CD"))
    )
    .withColumn(
        "PROV_TYP_NM",
        F.when((F.col("PROV_TYP_NM").isNull()) | (F.length(F.col("PROV_TYP_NM")) == 0), F.lit("NA ")).otherwise(F.col("PROV_TYP_NM"))
    )
    .withColumn(
        "PROV_SPEC_CD",
        F.when(F.col("PROV_SPEC_CD").isNull(), F.lit("NA")).otherwise(F.col("PROV_SPEC_CD"))
    )
    .withColumn(
        "PROV_SPEC_NM",
        F.when(F.col("PROV_SPEC_NM").isNull(), F.lit("NA")).otherwise(F.col("PROV_SPEC_NM"))
    )
    .withColumn(
        "CRT_RUN_CYC_EXCTN_SK", F.lit(EDWRunCycle)
    )
    .withColumn(
        "LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(EDWRunCycle)
    )
    .withColumn(
        "PROV_ENTY_CD_SK",
        F.when(F.col("PROV_ENTY_CD_SK").isNull(), F.lit("NA")).otherwise(F.col("PROV_ENTY_CD_SK"))
    )
    .select(
        F.col("APL_PROV_SK").alias("APL_PROV_SK"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("APL_ID").alias("APL_ID"),
        F.col("SEQ_NO").alias("APL_PROV_SEQ_NO"),
        F.col("CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.col("APL_SK").alias("APL_SK"),
        F.col("FCLTY_TYP_SK").alias("FCLTY_TYP_SK"),
        F.col("PROV_SK").alias("PROV_SK"),
        F.col("PROV_GRP_PROV_SK").alias("PROV_GRP_PROV_SK"),
        F.col("PROV_TYP_SK").alias("PROV_TYP_SK"),
        F.col("PROV_SPEC_SK").alias("PROV_SPEC_SK"),
        F.col("APL_PROV_RELSHP_RSN_CD").alias("APL_PROV_RELSHP_RSN_CD"),
        F.col("APL_PROV_RELSHP_RSN_NM").alias("APL_PROV_RELSHP_RSN_NM"),
        F.col("FCLTY_TYP_CD").alias("FCLTY_TYP_CD"),
        F.col("FCLTY_TYP_NM").alias("FCLTY_TYP_NM"),
        F.col("PROV_ENTY_CD").alias("PROV_ENTY_CD"),
        F.col("PROV_ENTY_NM").alias("PROV_ENTY_NM"),
        F.col("PROV_GRP_PROV_ID").alias("PROV_GRP_PROV_ID"),
        F.col("PROV_GRP_PROV_NM").alias("PROV_GRP_PROV_NM"),
        F.col("PROV_ID").alias("PROV_ID"),
        F.col("PROV_NM").alias("PROV_NM"),
        F.col("PROV_TYP_CD").alias("PROV_TYP_CD"),
        F.col("PROV_TYP_NM").alias("PROV_TYP_NM"),
        F.col("PROV_SPEC_CD").alias("PROV_SPEC_CD"),
        F.col("PROV_SPEC_NM").alias("PROV_SPEC_NM"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("APL_PROV_RELSHP_RSN_CD_SK").alias("APL_PROV_RELSHP_RSN_CD_SK"),
        F.col("PROV_ENTY_CD_SK").alias("PROV_ENTY_CD_SK"),
    )
)

# NALink (single row with constants)
df_xfrm_BusinessLogic_NALink = (
    df_temp.filter(F.col("row_no") == 1)
    .select(
        F.lit(1).alias("APL_PROV_SK"),
        F.lit("NA").alias("SRC_SYS_CD"),
        F.lit("NA").alias("APL_ID"),
        F.lit(0).alias("APL_PROV_SEQ_NO"),
        F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.lit(1).alias("APL_SK"),
        F.lit(1).alias("FCLTY_TYP_SK"),
        F.lit(1).alias("PROV_SK"),
        F.lit(1).alias("PROV_GRP_PROV_SK"),
        F.lit(1).alias("PROV_TYP_SK"),
        F.lit(1).alias("PROV_SPEC_SK"),
        F.lit("NA").alias("APL_PROV_RELSHP_RSN_CD"),
        F.lit("NA").alias("APL_PROV_RELSHP_RSN_NM"),
        F.lit("NA").alias("FCLTY_TYP_CD"),
        F.lit("NA").alias("FCLTY_TYP_NM"),
        F.lit("NA").alias("PROV_ENTY_CD"),
        F.lit("NA").alias("PROV_ENTY_NM"),
        F.lit("NA").alias("PROV_GRP_PROV_ID"),
        F.lit("NA").alias("PROV_GRP_PROV_NM"),
        F.lit("NA").alias("PROV_ID"),
        F.lit("NA").alias("PROV_NM"),
        F.lit("NA").alias("PROV_TYP_CD"),
        F.lit("NA").alias("PROV_TYP_NM"),
        F.lit("NA").alias("PROV_SPEC_CD"),
        F.lit("NA").alias("PROV_SPEC_NM"),
        F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("APL_PROV_RELSHP_RSN_CD_SK"),
        F.lit(1).alias("PROV_ENTY_CD_SK"),
    )
)

# UNKLink (single row with constants)
df_xfrm_BusinessLogic_UNKLink = (
    df_temp.filter(F.col("row_no") == 1)
    .select(
        F.lit(0).alias("APL_PROV_SK"),
        F.lit("UNK").alias("SRC_SYS_CD"),
        F.lit("UNK").alias("APL_ID"),
        F.lit(0).alias("APL_PROV_SEQ_NO"),
        F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.lit(0).alias("APL_SK"),
        F.lit(0).alias("FCLTY_TYP_SK"),
        F.lit(0).alias("PROV_SK"),
        F.lit(0).alias("PROV_GRP_PROV_SK"),
        F.lit(0).alias("PROV_TYP_SK"),
        F.lit(0).alias("PROV_SPEC_SK"),
        F.lit("UNK").alias("APL_PROV_RELSHP_RSN_CD"),
        F.lit("UNK").alias("APL_PROV_RELSHP_RSN_NM"),
        F.lit("UNK").alias("FCLTY_TYP_CD"),
        F.lit("UNK").alias("FCLTY_TYP_NM"),
        F.lit("UNK").alias("PROV_ENTY_CD"),
        F.lit("UNK").alias("PROV_ENTY_NM"),
        F.lit("UNK").alias("PROV_GRP_PROV_ID"),
        F.lit("UNK").alias("PROV_GRP_PROV_NM"),
        F.lit("UNK").alias("PROV_ID"),
        F.lit("UNK").alias("PROV_NM"),
        F.lit("UNK").alias("PROV_TYP_CD"),
        F.lit("UNK").alias("PROV_TYP_NM"),
        F.lit("UNK").alias("PROV_SPEC_CD"),
        F.lit("UNK").alias("PROV_SPEC_NM"),
        F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("APL_PROV_RELSHP_RSN_CD_SK"),
        F.lit(0).alias("PROV_ENTY_CD_SK"),
    )
)

# --------------------------------------------------------------------------------
# FnlData (PxFunnel)
# Funnel is basically union the three dataframes
# --------------------------------------------------------------------------------
df_FnlData = df_xfrm_BusinessLogic_outMain.unionByName(df_xfrm_BusinessLogic_NALink).unionByName(df_xfrm_BusinessLogic_UNKLink)

# Apply rpad for columns with SqlType=char(10)
df_FnlData = df_FnlData.withColumn(
    "CRT_RUN_CYC_EXCTN_DT_SK",
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
)

# Final column order as in the funnel output
df_FnlData = df_FnlData.select(
    "APL_PROV_SK",
    "SRC_SYS_CD",
    "APL_ID",
    "APL_PROV_SEQ_NO",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "APL_SK",
    "FCLTY_TYP_SK",
    "PROV_SK",
    "PROV_GRP_PROV_SK",
    "PROV_TYP_SK",
    "PROV_SPEC_SK",
    "APL_PROV_RELSHP_RSN_CD",
    "APL_PROV_RELSHP_RSN_NM",
    "FCLTY_TYP_CD",
    "FCLTY_TYP_NM",
    "PROV_ENTY_CD",
    "PROV_ENTY_NM",
    "PROV_GRP_PROV_ID",
    "PROV_GRP_PROV_NM",
    "PROV_ID",
    "PROV_NM",
    "PROV_TYP_CD",
    "PROV_TYP_NM",
    "PROV_SPEC_CD",
    "PROV_SPEC_NM",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "APL_PROV_RELSHP_RSN_CD_SK",
    "PROV_ENTY_CD_SK",
)

# --------------------------------------------------------------------------------
# seq_APL_PROV_D_csv_load (PxSequentialFile - Write)
# Write to CSV with delimiter = ',', quoteChar='^', no header
# --------------------------------------------------------------------------------
write_files(
    df_FnlData,
    f"{adls_path}/load/APL_PROV_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)