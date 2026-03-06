# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2011 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by:
# MAGIC                     IdsClsPlnCustSvcIdCardPhnDExtrSeq
# MAGIC 
# MAGIC Processing:
# MAGIC                     Extract data from the IDS to be loaded into the EDW
# MAGIC 
# MAGIC Control Job Rerun Information: 
# MAGIC                     Previous Run Successful:    Restart, no other steps necessary
# MAGIC                     Previous Run Aborted:         Restart, no other steps necessary
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                                  Project/                                                                                                                       Code                   Date
# MAGIC Developer           Date              Altiris #     Change Description                                                                                     Reviewer            Reviewed
# MAGIC -------------------------  -------------------   -------------   -----------------------------------------------------------------------------------------------------------------   -------------------------  -------------------
# MAGIC Hugh Sisson       10/25/2011   4795        Original program                                                                                           SAndrew             2011-11-09
# MAGIC 
# MAGIC Pooja Sunkara    07/09/2013   5114       Converted job form server to parallel version                                                 Peter Marshall     9/4/2013

# MAGIC Extracts all data from IDS reference table CD_MPPNG.
# MAGIC 
# MAGIC Code SK lookups for Denormalization
# MAGIC Lookup Keys:
# MAGIC SRC_SYS_CD_SK
# MAGIC CLS_PLN_DTL_PROD_CAT_CD_SK
# MAGIC CLS_PLN_DTL_PROD_CAT_NM_SK
# MAGIC CUST_SVC_ID_CARD_PHN_TYP_CD_SK
# MAGIC CUST_SVC_ID_CARD_PHN_TYP_NM_SK
# MAGIC CUST_SVC_ID_CARD_PHN_USE_NM_SK
# MAGIC Add CRT_RUN_CYC_EXCTN_DT_SK ,LAST_UPDT_RUN_CYC_EXCTN_DT_SK,NA and UNK rows
# MAGIC Job name:
# MAGIC IdsEdwClsPlnCustSvcIdCardPhnDExtr
# MAGIC EDW Class Plan Customer Service ID Card Phone Number extract from IDS
# MAGIC Write CLS_PLN_CUST_SVC_ID_CARD_PHN_D Data into a Sequential file for Load Ready Job.
# MAGIC Read data from source table CLS_PLN_CUST_SVC_ID_CARD_PHN  CPCSICP,   
# MAGIC CUST_SVC_ID_CARD_PHN CSICP
# MAGIC ON
# MAGIC CUST_SVC_ID_CARD_PHN_SK      AND
# MAGIC LAST_UPDT_RUN_CYC_EXCTN_SK
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


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query_db2_CLS_PLN_CUST_SVC_ID_CARD_PHN_in = """SELECT
     CPCSICP.CLS_PLN_CUST_SVC_IDCARD_PHN_SK,
     CPCSICP.SRC_SYS_CD_SK,
     CPCSICP.GRP_ID,
     CPCSICP.SUBGRP_ID,
     CPCSICP.CLS_ID,
     CPCSICP.CLS_PLN_ID,
     CPCSICP.PROD_ID,
     CPCSICP.EFF_DT_SK,
     CPCSICP.CUST_SVC_ID_CARD_PHN_KEY,
     CPCSICP.CUST_SVC_ID_CARD_PHN_USE_CD,
     CPCSICP.CRT_RUN_CYC_EXCTN_SK,
     CPCSICP.LAST_UPDT_RUN_CYC_EXCTN_SK,
     CPCSICP.CLS_SK,
     CPCSICP.CLS_PLN_SK,
     CPCSICP.GRP_SK,
     CPCSICP.PROD_SK,
     CPCSICP.SUBGRP_SK,
     CPCSICP.CLS_PLN_DTL_PROD_CAT_CD_SK,
     CPCSICP.CUST_SVC_ID_CARD_PHN_USE_CD_SK,
     CPCSICP.TERM_DT_SK,
     CSICP.CUST_SVC_ID_CARD_PHN_TYP_CD_SK,
     CSICP.CUST_SVC_ID_CARD_PHN_AREA_CD,
     CSICP.CUST_SVC_ID_CARD_PHN_EXCH_NO,
     CSICP.CUST_SVC_ID_CARD_PHN_LN_NO
FROM
     """ + f"{IDSOwner}" + """.CLS_PLN_CUST_SVC_ID_CARD_PHN  CPCSICP,
     """ + f"{IDSOwner}" + """.CUST_SVC_ID_CARD_PHN CSICP
WHERE
     CPCSICP.CUST_SVC_ID_CARD_PHN_SK =  CSICP.CUST_SVC_ID_CARD_PHN_SK
     AND CPCSICP.LAST_UPDT_RUN_CYC_EXCTN_SK >=  """ + f"{IDSRunCycle}"

df_db2_CLS_PLN_CUST_SVC_ID_CARD_PHN_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CLS_PLN_CUST_SVC_ID_CARD_PHN_in)
    .load()
)

extract_query_db2_CD_MPPNG2_in = """SELECT 
CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') TRGT_CD,
COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM 
FROM
""" + f"{IDSOwner}" + """.CD_MPPNG"""

df_db2_CD_MPPNG2_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CD_MPPNG2_in)
    .load()
)

df_ref_CustSvcIdCardPhnTyp = df_db2_CD_MPPNG2_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_ref_CustSvcIdCardUse = df_db2_CD_MPPNG2_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_ClsPlnDtlProdCat = df_db2_CD_MPPNG2_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_ref_SrcSysCd = df_db2_CD_MPPNG2_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_inabc = df_db2_CLS_PLN_CUST_SVC_ID_CARD_PHN_in.alias("inabc")
df_phnTyp = df_ref_CustSvcIdCardPhnTyp.alias("refCustSvcPhnTyp")
df_phnUse = df_ref_CustSvcIdCardUse.alias("refCustSvcPhnUse")
df_cat = df_ClsPlnDtlProdCat.alias("clsPlnCat")
df_srcSys = df_ref_SrcSysCd.alias("refSys")

df_lkp_CdmaCodes = (
    df_inabc
    .join(df_phnTyp, df_inabc["CUST_SVC_ID_CARD_PHN_TYP_CD_SK"] == df_phnTyp["CD_MPPNG_SK"], "left")
    .join(df_phnUse, df_inabc["CUST_SVC_ID_CARD_PHN_USE_CD_SK"] == df_phnUse["CD_MPPNG_SK"], "left")
    .join(df_cat, df_inabc["CLS_PLN_DTL_PROD_CAT_CD_SK"] == df_cat["CD_MPPNG_SK"], "left")
    .join(df_srcSys, df_inabc["SRC_SYS_CD_SK"] == df_srcSys["CD_MPPNG_SK"], "left")
)

df_lkp_CdmaCodes_out = df_lkp_CdmaCodes.select(
    F.col("inabc.CLS_PLN_CUST_SVC_IDCARD_PHN_SK").alias("CLS_PLN_CUST_SVC_IDCARD_PHN_SK"),
    F.col("inabc.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("inabc.GRP_ID").alias("GRP_ID"),
    F.col("inabc.SUBGRP_ID").alias("SUBGRP_ID"),
    F.col("inabc.CLS_ID").alias("CLS_ID"),
    F.col("inabc.CLS_PLN_ID").alias("CLS_PLN_ID"),
    F.col("inabc.PROD_ID").alias("PROD_ID"),
    F.col("inabc.EFF_DT_SK").alias("EFF_DT_SK"),
    F.col("inabc.CUST_SVC_ID_CARD_PHN_KEY").alias("CUST_SVC_ID_CARD_PHN_KEY"),
    F.col("inabc.CUST_SVC_ID_CARD_PHN_USE_CD").alias("CUST_SVC_ID_CARD_PHN_USE_CD"),
    F.col("inabc.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("inabc.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("inabc.CLS_SK").alias("CLS_SK"),
    F.col("inabc.CLS_PLN_SK").alias("CLS_PLN_SK"),
    F.col("inabc.GRP_SK").alias("GRP_SK"),
    F.col("inabc.PROD_SK").alias("PROD_SK"),
    F.col("inabc.SUBGRP_SK").alias("SUBGRP_SK"),
    F.col("inabc.CLS_PLN_DTL_PROD_CAT_CD_SK").alias("CLS_PLN_DTL_PROD_CAT_CD_SK"),
    F.col("inabc.CUST_SVC_ID_CARD_PHN_USE_CD_SK").alias("CUST_SVC_ID_CARD_PHN_USE_CD_SK"),
    F.col("inabc.TERM_DT_SK").alias("TERM_DT_SK"),
    F.col("inabc.CUST_SVC_ID_CARD_PHN_TYP_CD_SK").alias("CUST_SVC_ID_CARD_PHN_TYP_CD_SK"),
    F.col("inabc.CUST_SVC_ID_CARD_PHN_AREA_CD").alias("CUST_SVC_ID_CARD_PHN_AREA_CD"),
    F.col("inabc.CUST_SVC_ID_CARD_PHN_EXCH_NO").alias("CUST_SVC_ID_CARD_PHN_EXCH_NO"),
    F.col("inabc.CUST_SVC_ID_CARD_PHN_LN_NO").alias("CUST_SVC_ID_CARD_PHN_LN_NO"),
    F.col("refCustSvcPhnTyp.TRGT_CD").alias("CUST_SVC_ID_CARD_PHN_TYP_CD"),
    F.col("refCustSvcPhnTyp.TRGT_CD_NM").alias("CUST_SVC_ID_CARD_PHN_TYP_NM"),
    F.col("refCustSvcPhnUse.TRGT_CD_NM").alias("CUST_SVC_ID_CARD_PHN_USE_NM"),
    F.col("clsPlnCat.TRGT_CD").alias("CLS_PLN_DTL_PROD_CAT_CD"),
    F.col("clsPlnCat.TRGT_CD_NM").alias("CLS_PLN_DTL_PROD_CAT_NM"),
    F.col("refSys.TRGT_CD").alias("SRC_SYS_CD")
)

cond_phn = (
    (F.col("CUST_SVC_ID_CARD_PHN_AREA_CD").isNull()) |
    (trim(F.col("CUST_SVC_ID_CARD_PHN_AREA_CD")) == "") |
    (F.col("CUST_SVC_ID_CARD_PHN_EXCH_NO").isNull()) |
    (trim(F.col("CUST_SVC_ID_CARD_PHN_EXCH_NO")) == "") |
    (F.col("CUST_SVC_ID_CARD_PHN_LN_NO").isNull()) |
    (trim(F.col("CUST_SVC_ID_CARD_PHN_LN_NO")) == "")
)

df_xmf_businessLogic_stageVar = df_lkp_CdmaCodes_out.withColumn(
    "svPHNNO",
    F.when(cond_phn, F.lit("0000000000")).otherwise(
        F.concat(
            F.col("CUST_SVC_ID_CARD_PHN_AREA_CD"),
            F.col("CUST_SVC_ID_CARD_PHN_EXCH_NO"),
            F.col("CUST_SVC_ID_CARD_PHN_LN_NO")
        )
    )
)

df_fnl_main = df_xmf_businessLogic_stageVar.filter(
    (F.col("CLS_PLN_CUST_SVC_IDCARD_PHN_SK") != 0) &
    (F.col("CLS_PLN_CUST_SVC_IDCARD_PHN_SK") != 1)
)

df_fnl_main_sel = df_fnl_main.select(
    F.col("CLS_PLN_CUST_SVC_IDCARD_PHN_SK").alias("CLS_PLN_CUST_SVC_IDCARD_PHN_SK"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("SUBGRP_ID").alias("SUBGRP_ID"),
    F.col("CLS_ID").alias("CLS_ID"),
    F.col("CLS_PLN_ID").alias("CLS_PLN_ID"),
    F.col("PROD_ID").alias("PROD_ID"),
    F.col("EFF_DT_SK").alias("CUST_SVC_ID_CARD_EFF_DT_SK"),
    F.col("CUST_SVC_ID_CARD_PHN_KEY").alias("CUST_SVC_ID_CARD_PHN_KEY"),
    F.col("CUST_SVC_ID_CARD_PHN_USE_CD").alias("CUST_SVC_ID_CARD_PHN_USE_CD"),
    F.when(F.col("SRC_SYS_CD").isNull() | (trim(F.col("SRC_SYS_CD")) == ""), F.lit("UNK"))
     .otherwise(F.col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
    F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("CLS_SK").alias("CLS_SK"),
    F.col("CLS_PLN_SK").alias("CLS_PLN_SK"),
    F.col("GRP_SK").alias("GRP_SK"),
    F.col("PROD_SK").alias("PROD_SK"),
    F.col("SUBGRP_SK").alias("SUBGRP_SK"),
    F.when(F.col("CLS_PLN_DTL_PROD_CAT_CD").isNull() | (trim(F.col("CLS_PLN_DTL_PROD_CAT_CD")) == ""), F.lit("UNK"))
     .otherwise(F.col("CLS_PLN_DTL_PROD_CAT_CD")).alias("CLS_PLN_DTL_PROD_CAT_CD"),
    F.when(F.col("CLS_PLN_DTL_PROD_CAT_NM").isNull() | (trim(F.col("CLS_PLN_DTL_PROD_CAT_NM")) == ""), F.lit("UNK"))
     .otherwise(F.col("CLS_PLN_DTL_PROD_CAT_NM")).alias("CLS_PLN_DTL_PROD_CAT_NM"),
    F.when(F.col("CUST_SVC_ID_CARD_PHN_TYP_CD").isNull() | (trim(F.col("CUST_SVC_ID_CARD_PHN_TYP_CD")) == ""), F.lit("UNK"))
     .otherwise(F.col("CUST_SVC_ID_CARD_PHN_TYP_CD")).alias("CUST_SVC_ID_CARD_PHN_TYP_CD"),
    F.when(F.col("CUST_SVC_ID_CARD_PHN_TYP_NM").isNull() | (trim(F.col("CUST_SVC_ID_CARD_PHN_TYP_NM")) == ""), F.lit("UNK"))
     .otherwise(F.col("CUST_SVC_ID_CARD_PHN_TYP_NM")).alias("CUST_SVC_ID_CARD_PHN_TYP_NM"),
    F.when(F.col("CUST_SVC_ID_CARD_PHN_USE_NM").isNull() | (trim(F.col("CUST_SVC_ID_CARD_PHN_USE_NM")) == ""), F.lit("UNK"))
     .otherwise(F.col("CUST_SVC_ID_CARD_PHN_USE_NM")).alias("CUST_SVC_ID_CARD_PHN_USE_NM"),
    F.col("TERM_DT_SK").alias("CUST_SVC_ID_CARD_TERM_DT_SK"),
    F.col("svPHNNO").alias("CUST_SVC_ID_CARD_PHN_NO"),
    F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLS_PLN_DTL_PROD_CAT_CD_SK").alias("CLS_PLN_DTL_PROD_CAT_CD_SK"),
    F.col("CUST_SVC_ID_CARD_PHN_TYP_CD_SK").alias("CUST_SVC_ID_CARD_PHN_TYP_CD_SK"),
    F.col("CUST_SVC_ID_CARD_PHN_USE_CD_SK").alias("CUST_SVC_ID_CARD_PHN_USE_CD_SK")
)

unk_schema = StructType([
    StructField("CLS_PLN_CUST_SVC_IDCARD_PHN_SK", StringType(), True),
    StructField("GRP_ID", StringType(), True),
    StructField("SUBGRP_ID", StringType(), True),
    StructField("CLS_ID", StringType(), True),
    StructField("CLS_PLN_ID", StringType(), True),
    StructField("PROD_ID", StringType(), True),
    StructField("CUST_SVC_ID_CARD_EFF_DT_SK", StringType(), True),
    StructField("CUST_SVC_ID_CARD_PHN_KEY", StringType(), True),
    StructField("CUST_SVC_ID_CARD_PHN_USE_CD", StringType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("CLS_SK", StringType(), True),
    StructField("CLS_PLN_SK", StringType(), True),
    StructField("GRP_SK", StringType(), True),
    StructField("PROD_SK", StringType(), True),
    StructField("SUBGRP_SK", StringType(), True),
    StructField("CLS_PLN_DTL_PROD_CAT_CD", StringType(), True),
    StructField("CLS_PLN_DTL_PROD_CAT_NM", StringType(), True),
    StructField("CUST_SVC_ID_CARD_PHN_TYP_CD", StringType(), True),
    StructField("CUST_SVC_ID_CARD_PHN_TYP_NM", StringType(), True),
    StructField("CUST_SVC_ID_CARD_PHN_USE_NM", StringType(), True),
    StructField("CUST_SVC_ID_CARD_TERM_DT_SK", StringType(), True),
    StructField("CUST_SVC_ID_CARD_PHN_NO", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", StringType(), True),
    StructField("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK", StringType(), True),
    StructField("CLS_PLN_DTL_PROD_CAT_CD_SK", StringType(), True),
    StructField("CUST_SVC_ID_CARD_PHN_TYP_CD_SK", StringType(), True),
    StructField("CUST_SVC_ID_CARD_PHN_USE_CD_SK", StringType(), True)
])

df_unk_row = spark.createDataFrame([
    (
        "0",
        "UNK",
        "UNK",
        "UNK",
        "UNK",
        "UNK",
        "1753-01-01",
        "0",
        "UNK",
        "UNK",
        "1753-01-01",
        EDWRunCycleDate,
        "0",
        "0",
        "0",
        "0",
        "0",
        "UNK",
        "UNK",
        "UNK",
        "UNK",
        "UNK",
        "1753-01-01",
        "UNK",
        "100",
        EDWRunCycle,
        "0",
        "0",
        "0",
        "0"
    )
], unk_schema)

na_schema = StructType([
    StructField("CLS_PLN_CUST_SVC_IDCARD_PHN_SK", StringType(), True),
    StructField("GRP_ID", StringType(), True),
    StructField("SUBGRP_ID", StringType(), True),
    StructField("CLS_ID", StringType(), True),
    StructField("CLS_PLN_ID", StringType(), True),
    StructField("PROD_ID", StringType(), True),
    StructField("CUST_SVC_ID_CARD_EFF_DT_SK", StringType(), True),
    StructField("CUST_SVC_ID_CARD_PHN_KEY", StringType(), True),
    StructField("CUST_SVC_ID_CARD_PHN_USE_CD", StringType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("CLS_SK", StringType(), True),
    StructField("CLS_PLN_SK", StringType(), True),
    StructField("GRP_SK", StringType(), True),
    StructField("PROD_SK", StringType(), True),
    StructField("SUBGRP_SK", StringType(), True),
    StructField("CLS_PLN_DTL_PROD_CAT_CD", StringType(), True),
    StructField("CLS_PLN_DTL_PROD_CAT_NM", StringType(), True),
    StructField("CUST_SVC_ID_CARD_PHN_TYP_CD", StringType(), True),
    StructField("CUST_SVC_ID_CARD_PHN_TYP_NM", StringType(), True),
    StructField("CUST_SVC_ID_CARD_PHN_USE_NM", StringType(), True),
    StructField("CUST_SVC_ID_CARD_TERM_DT_SK", StringType(), True),
    StructField("CUST_SVC_ID_CARD_PHN_NO", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", StringType(), True),
    StructField("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK", StringType(), True),
    StructField("CLS_PLN_DTL_PROD_CAT_CD_SK", StringType(), True),
    StructField("CUST_SVC_ID_CARD_PHN_TYP_CD_SK", StringType(), True),
    StructField("CUST_SVC_ID_CARD_PHN_USE_CD_SK", StringType(), True)
])

df_na_row = spark.createDataFrame([
    (
        "1",
        "NA",
        "NA",
        "NA",
        "NA",
        "NA",
        "1753-01-01",
        "0",
        "NA",
        "NA",
        "1753-01-01",
        EDWRunCycleDate,
        "1",
        "1",
        "1",
        "1",
        "1",
        "NA",
        "NA",
        "NA",
        "NA",
        "NA",
        "1753-01-01",
        "NA",
        "100",
        EDWRunCycle,
        "0",
        "1",
        "1",
        "1"
    )
], na_schema)

df_fnl_dataLinks = df_fnl_main_sel.unionByName(df_unk_row).unionByName(df_na_row)

df_final = df_fnl_dataLinks.select(
    F.col("CLS_PLN_CUST_SVC_IDCARD_PHN_SK").alias("CLS_PLN_CUST_SVC_IDCARD_PHN_SK"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("SUBGRP_ID").alias("SUBGRP_ID"),
    F.col("CLS_ID").alias("CLS_ID"),
    F.col("CLS_PLN_ID").alias("CLS_PLN_ID"),
    F.col("PROD_ID").alias("PROD_ID"),
    F.rpad(F.col("CUST_SVC_ID_CARD_EFF_DT_SK"), 10, " ").alias("CUST_SVC_ID_CARD_EFF_DT_SK"),
    F.col("CUST_SVC_ID_CARD_PHN_KEY").alias("CUST_SVC_ID_CARD_PHN_KEY"),
    F.col("CUST_SVC_ID_CARD_PHN_USE_CD").alias("CUST_SVC_ID_CARD_PHN_USE_CD"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("CLS_SK").alias("CLS_SK"),
    F.col("CLS_PLN_SK").alias("CLS_PLN_SK"),
    F.col("GRP_SK").alias("GRP_SK"),
    F.col("PROD_SK").alias("PROD_SK"),
    F.col("SUBGRP_SK").alias("SUBGRP_SK"),
    F.col("CLS_PLN_DTL_PROD_CAT_CD").alias("CLS_PLN_DTL_PROD_CAT_CD"),
    F.col("CLS_PLN_DTL_PROD_CAT_NM").alias("CLS_PLN_DTL_PROD_CAT_NM"),
    F.col("CUST_SVC_ID_CARD_PHN_TYP_CD").alias("CUST_SVC_ID_CARD_PHN_TYP_CD"),
    F.col("CUST_SVC_ID_CARD_PHN_TYP_NM").alias("CUST_SVC_ID_CARD_PHN_TYP_NM"),
    F.col("CUST_SVC_ID_CARD_PHN_USE_NM").alias("CUST_SVC_ID_CARD_PHN_USE_NM"),
    F.rpad(F.col("CUST_SVC_ID_CARD_TERM_DT_SK"), 10, " ").alias("CUST_SVC_ID_CARD_TERM_DT_SK"),
    F.col("CUST_SVC_ID_CARD_PHN_NO").alias("CUST_SVC_ID_CARD_PHN_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK").alias("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLS_PLN_DTL_PROD_CAT_CD_SK").alias("CLS_PLN_DTL_PROD_CAT_CD_SK"),
    F.col("CUST_SVC_ID_CARD_PHN_TYP_CD_SK").alias("CUST_SVC_ID_CARD_PHN_TYP_CD_SK"),
    F.col("CUST_SVC_ID_CARD_PHN_USE_CD_SK").alias("CUST_SVC_ID_CARD_PHN_USE_CD_SK")
)

write_files(
    df_final,
    f"{adls_path}/load/CLS_PLN_CUST_SVC_ID_CARD_PHN_D.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)