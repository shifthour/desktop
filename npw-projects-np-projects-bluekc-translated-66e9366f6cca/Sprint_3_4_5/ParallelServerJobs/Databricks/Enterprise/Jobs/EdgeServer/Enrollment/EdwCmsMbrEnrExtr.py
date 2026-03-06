# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2021 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:    EdwCmsMbrEnrExtr
# MAGIC DESCRIPTION:  Extract member enrollment data for CMS
# MAGIC CALLED BY: EdgeServerEdwCmsEnrSeq
# MAGIC 
# MAGIC RERUN INFORMATION: 
# MAGIC              PREVIOUS RUN SUCCESSFUL:   Nothing to reset. 
# MAGIC              PREVIOUS RUN ABORTED:         Nothing to reset, just restart.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC ======================================================================================================================================================================================
# MAGIC Developer                   Date\(9)\(9)Project/Ticket #\(9)      \(9)Change Description\(9)\(9)\(9)\(9)\(9)Development Project\(9)\(9)Code Reviewer\(9)       Date Reviewed
# MAGIC ======================================================================================================================================================================================
# MAGIC Rajitha Vadlamudi       2013\(9)\(9)5125 Risk Adjustment\(9)\(9)Original Programming\(9)\(9)\(9)\(9)EnterpriseCurDevl 
# MAGIC 
# MAGIC Kimberly Doty              2014-03-24\(9)\(9)5125 Risk Adjustment\(9)\(9)Brought up to standards\(9)\(9)\(9)\(9)EnterpriseNewDevl              \(9)Kalyan Neelam           2015-03-26
# MAGIC 
# MAGIC Raja Gummadi            2015-07-30\(9)\(9)5125\(9)\(9)\(9)Added MBR_INDV_BE_KEY\(9)\(9)\(9)\(9)EnterpriseDev2                     \(9)Bhoomi Dasari            07/30/2015
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)and SUB_INDV_BE_KEY columns
# MAGIC Jaideep Mankala        2016-10-12\(9)5605\(9)\(9)\(9)Modified Query on MBR to replace PROD_D table\(9)\(9)\(9)\(9)\(9)Kalyan Neelam           \(9)2016-10-12
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)with MBR_ENR_QHP_D table
# MAGIC Harsha Ravuri\(9)2019-08-13\(9)Risk Adjustment\(9)\(9)updated source extract MBR and refrence\(9)\(9)\(9)EnterpriseDev2                \(9)Kalyan Neelam           2019-08-19
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)MBR_ENR_D SQL's filters to PROD_SN_NM =
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)"BLUESELECT+". Changed logic to remove 
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)SUB_RATE_RATE_CLS_ID, SUBGRP_RATE_RATE_CLS_ID
# MAGIC Harsha Ravuri\(9)2021-08-19\(9)US#273281\(9)\(9)Added enrollment effective and term dates to\(9)\(9)EnterpriseDev2\(9)\(9)Ken Bradmon\(9)2021-09-08
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)the target file.

# MAGIC CMS Mbr Enrollment
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


EDWOwner = get_widget_value("EDWOwner","")
edw_secret_name = get_widget_value("edw_secret_name","")
BeginExtrDate = get_widget_value("BeginExtrDate","")
EndExtrDate = get_widget_value("EndExtrDate","")
ProdIn = get_widget_value("ProdIn","")
CmsVersion = get_widget_value("CmsVersion","")
FileIdVer = get_widget_value("FileIdVer","")
State = get_widget_value("State","")
QHPID = get_widget_value("QHPID","")

jdbc_url, jdbc_props = get_db_config(edw_secret_name)
extract_query_MBR = f"""
SELECT
   mbr.MBR_SK, 
   mbr.SUB_SK,
   mbr.MBR_UNIQ_KEY, 
   mbr.MBR_BRTH_DT_SK, 
   mbr.MBR_GNDR_CD, 
   mbr.MBR_INDV_BE_KEY, 
   mbr.SUB_IN, 
   grp.GRP_DP_IN, 
   mbrEnr.MBR_ENR_EFF_DT_SK, 
   mbrEnr.MBR_ENR_TERM_DT_SK,
   mbrEnr.PROD_SK,
   MBR_QHP.QHP_ID,
   mbr.SUB_UNIQ_KEY,
   mbr.MBR_RELSHP_CD,
   mbr.MBR_HOME_ADDR_ST_CD,
   mbr.MBR_HOME_ADDR_CNTY_NM,
   sub.SUB_INDV_BE_KEY
FROM {EDWOwner}.MBR_D mbr, 
     {EDWOwner}.MBR_ENR_D mbrEnr,
     {EDWOwner}.GRP_D grp,
     {EDWOwner}.MBR_ENR_QHP_D MBR_QHP,
     {EDWOwner}.SUB_D sub
WHERE 
      mbr.MBR_SK = mbrEnr.MBR_SK
  AND grp.GRP_SK = mbr.GRP_SK
  AND MBR_QHP.MBR_ENR_SK = mbrEnr.MBR_ENR_SK
  AND MBR_QHP.MBR_UNIQ_KEY = mbrEnr.MBR_UNIQ_KEY
  AND MBR_QHP.MBR_ENR_EFF_DT_SK = mbrEnr.MBR_ENR_EFF_DT_SK
  AND mbrEnr.MBR_ENR_ELIG_IN = 'Y'
  AND mbrEnr.MBR_ENR_CLS_PLN_PROD_CAT_CD = 'MED'
  AND MBR_QHP.MBR_ENR_CLS_PLN_PROD_CAT_CD = 'MED'
  AND mbrEnr.MBR_ENR_EFF_DT_SK <= '{EndExtrDate}'
  AND mbrEnr.MBR_ENR_TERM_DT_SK >= '{BeginExtrDate}'
  AND mbrEnr.PROD_SH_NM IN ('PCB', 'PC', 'BCARE', 'BLUE-ACCESS', 'BLUE-SELECT','BLUESELECT+')
  AND mbrEnr.CLS_ID <> 'MHIP'
  AND MBR_QHP.QHP_ID <> 'NA'
  AND MBR_QHP.QHP_ID LIKE '{QHPID}'
  AND mbr.SUB_UNIQ_KEY = sub.SUB_UNIQ_KEY
"""
df_MBR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_MBR)
    .load()
)

extract_query_MBR_ENR_D = f"""
SELECT
    mbr.MBR_SK,
    mbr1.MBR_UNIQ_KEY
FROM {EDWOwner}.MBR_D mbr, 
     {EDWOwner}.MBR_ENR_D mbrEnr,
     {EDWOwner}.MBR_D mbr1
WHERE 
      mbrEnr.MBR_SK = mbr.MBR_SK
  AND mbr.SUB_MBR_SK = mbr1.MBR_SK
  AND mbrEnr.MBR_ENR_EFF_DT_SK <= '{EndExtrDate}'
  AND mbrEnr.MBR_ENR_TERM_DT_SK >= '{BeginExtrDate}'
  AND mbrEnr.MBR_ENR_ELIG_IN = 'Y'
  AND mbrEnr.MBR_ENR_CLS_PLN_PROD_CAT_CD = 'MED'
  AND mbrEnr.PROD_SH_NM IN ('PCB', 'PC', 'BCARE', 'BLUE-ACCESS', 'BLUE-SELECT','BLUESELECT+')
  AND mbrEnr.CLS_ID <> 'MHIP'
"""
df_MBR_ENR_D = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_MBR_ENR_D)
    .load()
)

df_lkupRates = (
    df_MBR.alias("Mbr")
    .join(
        df_MBR_ENR_D.alias("UniqKey"),
        F.col("Mbr.MBR_SK") == F.col("UniqKey.MBR_SK"),
        "left"
    )
    .select(
        F.col("Mbr.MBR_SK").alias("MBR_SK"),
        F.col("Mbr.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.col("Mbr.MBR_BRTH_DT_SK").alias("MBR_BRTH_DT_SK"),
        F.col("Mbr.MBR_GNDR_CD").alias("MBR_GNDR_CD"),
        F.col("Mbr.SUB_IN").alias("SUB_IN"),
        F.col("Mbr.MBR_ENR_EFF_DT_SK").alias("MBR_ENR_EFF_DT_SK"),
        F.col("Mbr.MBR_ENR_TERM_DT_SK").alias("MBR_ENR_TERM_DT_SK"),
        F.col("Mbr.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"),
        F.col("Mbr.GRP_DP_IN").alias("GRP_DP_IN"),
        F.col("Mbr.QHP_ID").alias("QHP_ID"),
        F.col("UniqKey.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY_SUBID"),
        F.col("Mbr.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
        F.col("Mbr.MBR_RELSHP_CD").alias("MBR_RELSHP_CD"),
        F.col("Mbr.MBR_HOME_ADDR_ST_CD").alias("MBR_HOME_ADDR_ST_CD"),
        F.col("Mbr.MBR_HOME_ADDR_CNTY_NM").alias("MBR_HOME_ADDR_CNTY_NM"),
        F.col("Mbr.SUB_INDV_BE_KEY").alias("SUB_INDV_BE_KEY"),
    )
)

df_sortMember = (
    df_lkupRates.orderBy(F.col("QHP_ID"))
    .select(
        F.col("MBR_SK").alias("MBR_SK"),
        F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.col("MBR_BRTH_DT_SK").alias("MBR_BRTH_DT_SK"),
        F.col("MBR_GNDR_CD").alias("MBR_GNDR_CD"),
        F.col("SUB_IN").alias("SUB_IN"),
        F.col("MBR_ENR_EFF_DT_SK").alias("MBR_ENR_EFF_DT_SK"),
        F.col("MBR_ENR_TERM_DT_SK").alias("MBR_ENR_TERM_DT_SK"),
        F.col("MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"),
        F.col("GRP_DP_IN").alias("GRP_DP_IN"),
        F.col("QHP_ID").alias("QHP_ID"),
        F.col("MBR_UNIQ_KEY_SUBID").alias("MBR_UNIQ_KEY_SUBID"),
        F.col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
        F.col("MBR_RELSHP_CD").alias("MBR_RELSHP_CD"),
        F.col("MBR_HOME_ADDR_ST_CD").alias("MBR_HOME_ADDR_ST_CD"),
        F.col("MBR_HOME_ADDR_CNTY_NM").alias("MBR_HOME_ADDR_CNTY_NM"),
        F.col("SUB_INDV_BE_KEY").alias("SUB_INDV_BE_KEY"),
    )
)

df_MemberInd = df_sortMember.withColumn(
    "MBR_UNIQ_KEY_SUBID",
    F.when(
        (F.length(F.col("MBR_UNIQ_KEY_SUBID")) == 0) | (F.col("MBR_UNIQ_KEY_SUBID").isNull()),
        F.lit("NA")
    ).otherwise(F.col("MBR_UNIQ_KEY_SUBID"))
).select(
    F.col("MBR_SK"),
    F.col("MBR_UNIQ_KEY"),
    F.col("MBR_BRTH_DT_SK"),
    F.col("MBR_GNDR_CD"),
    F.col("SUB_IN"),
    F.col("MBR_ENR_EFF_DT_SK"),
    F.col("MBR_ENR_TERM_DT_SK"),
    F.col("MBR_INDV_BE_KEY"),
    F.col("GRP_DP_IN"),
    F.col("MBR_UNIQ_KEY_SUBID"),
    F.col("SUB_UNIQ_KEY"),
    F.col("MBR_RELSHP_CD"),
    F.col("MBR_HOME_ADDR_ST_CD"),
    F.col("MBR_HOME_ADDR_CNTY_NM"),
    F.col("QHP_ID"),
    F.col("SUB_INDV_BE_KEY")
)

df_final = df_MemberInd.select(
    F.col("MBR_SK"),
    F.col("MBR_UNIQ_KEY"),
    F.rpad(F.col("MBR_BRTH_DT_SK"), 10, " ").alias("MBR_BRTH_DT_SK"),
    F.col("MBR_GNDR_CD"),
    F.rpad(F.col("SUB_IN"), 1, " ").alias("SUB_IN"),
    F.col("MBR_ENR_EFF_DT_SK"),
    F.col("MBR_ENR_TERM_DT_SK"),
    F.col("MBR_INDV_BE_KEY"),
    F.rpad(F.col("GRP_DP_IN"), 1, " ").alias("GRP_DP_IN"),
    F.col("MBR_UNIQ_KEY_SUBID"),
    F.col("SUB_UNIQ_KEY"),
    F.col("MBR_RELSHP_CD"),
    F.col("MBR_HOME_ADDR_ST_CD"),
    F.col("MBR_HOME_ADDR_CNTY_NM"),
    F.col("QHP_ID"),
    F.col("SUB_INDV_BE_KEY")
)

write_files(
    df_final,
    f"{adls_path_publish}/external/EDGE_RA_Enrollment_PreProcessing_{State}.txt",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=True,
    quote="\"",
    nullValue=None
)