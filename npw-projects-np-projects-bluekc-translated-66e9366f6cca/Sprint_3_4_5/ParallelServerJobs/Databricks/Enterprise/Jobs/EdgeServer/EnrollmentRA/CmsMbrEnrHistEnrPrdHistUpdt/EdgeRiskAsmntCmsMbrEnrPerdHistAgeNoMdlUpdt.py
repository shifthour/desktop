# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2019 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  EdgeRiskAsmntCmsMbrEnrPerdHistAgeNoMdlUpdt
# MAGIC 
# MAGIC DESCRIPTION:  This job loads  Member Enrollment Perd History data to CMS_MBR_ENR_PERD_HIST table 
# MAGIC 
# MAGIC CALLED BY:  EdgeRiskAsmntEnrSbmsnRAExtrSeq
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                   Date                     Project/Ticket #\(9)      Change Description\(9)\(9)\(9)          Development Project\(9)     Code Reviewer\(9)       Date Reviewed       
# MAGIC -------------------------          -------------------          --------------------------            ----------------------------------------------------------------------              ---------------------------------          ------------------------          -------------------------  
# MAGIC Harsha Ravuri           2019-03-05            5873 Risk Adjustment     Original Programming                                                  EnterpriseDev2\(9)     Abhiram Dasarathy     2019-03-06

# MAGIC This jobs updates PERD_AGE_YR_NO, PERD_AGE_MDL_TYP to CMS_MBR_ENR_PERD_HIST table
# MAGIC To get Age Modle Type
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
from pyspark.sql import types as T
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


EDGERiskAsmntOwner = get_widget_value('EDGERiskAsmntOwner','')
edgeriskasmnt_secret_name = get_widget_value('edgeriskasmnt_secret_name','')
AdjYear = get_widget_value('AdjYear','')

jdbc_url, jdbc_props = get_db_config(edgeriskasmnt_secret_name)

extract_query_1 = f"""
SELECT
  MEPH.ISSUER_ID,
  MEPH.FILE_ID,
  MEPH.MBR_RCRD_ID,
  MEPH.MBR_ENR_PERD_RCRD_ID,
  MEH.MBR_INDV_BE_KEY,
  MEH.MBR_UNIQ_KEY,
  MEPH.SUB_MBR_UNIQ_KEY,
  MEPH.SUB_INDV_BE_KEY,
  MEH.MBR_GNDR_CD,
  MEPH.MBR_ENR_TERM_DT_SK,
  MEPH.MBR_ENR_EFF_DT_SK,
  MEH.MBR_BRTH_DT,
  MEPH.GNRTN_DTM,
  MEPH.QHP_ID,
  MEPH.MBR_RELSHP_CD
FROM {EDGERiskAsmntOwner}.CMS_MBR_ENR_PERD_HIST MEPH,
     {EDGERiskAsmntOwner}.CMS_MBR_ENR_HIST MEH
WHERE MEPH.LAST_UPDT_DTM = (
        SELECT MAX(LAST_UPDT_DTM)
        FROM {EDGERiskAsmntOwner}.CMS_MBR_ENR_PERD_HIST
      )
  AND MEPH.FILE_ID = MEH.FILE_ID
  AND MEPH.ISSUER_ID = MEH.ISSUER_ID
  AND MEPH.MBR_RCRD_ID = MEH.MBR_RCRD_ID
  AND DATEPART(yy, MBR_ENR_TERM_DT_SK ) = {AdjYear}
"""

df_CMS_MBR_ENR_PERD_HIST = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_1)
    .load()
)

df_Age_logic_stage_vars = (
    df_CMS_MBR_ENR_PERD_HIST
    .withColumn("svTmYr", F.substring(F.col("MBR_ENR_TERM_DT_SK"), 1, 4))
    .withColumn("svTrmMnDy", F.concat(F.substring(F.col("MBR_ENR_TERM_DT_SK"), 6, 2), F.substring(F.col("MBR_ENR_TERM_DT_SK"), 9, 2)))
    .withColumn("svBirthYr", F.substring(F.col("MBR_BRTH_DT"), 1, 4))
    .withColumn("svBirthMonDay", F.concat(F.substring(F.col("MBR_BRTH_DT"), 6, 2), F.substring(F.col("MBR_BRTH_DT"), 9, 2)))
)

df_Age = df_Age_logic_stage_vars.select(
    (
        F.col("svTmYr").cast(T.IntegerType())
        - F.col("svBirthYr").cast(T.IntegerType())
        - F.when(F.col("svTrmMnDy") >= F.col("svBirthMonDay"), 0).otherwise(1)
    ).alias("PERD_AGE_YR_NO"),
    F.col("ISSUER_ID"),
    F.col("FILE_ID"),
    F.col("MBR_RCRD_ID"),
    F.col("MBR_ENR_PERD_RCRD_ID"),
    F.col("SUB_MBR_UNIQ_KEY"),
    F.col("MBR_INDV_BE_KEY"),
    F.col("MBR_UNIQ_KEY"),
    F.col("SUB_INDV_BE_KEY"),
    F.col("MBR_GNDR_CD"),
    F.col("QHP_ID"),
    F.col("MBR_BRTH_DT"),
    F.col("GNRTN_DTM"),
    F.col("MBR_ENR_TERM_DT_SK"),
    F.col("MBR_ENR_EFF_DT_SK"),
    F.col("MBR_RELSHP_CD")
)

extract_query_2 = f"""
SELECT DISTINCT
  LTRIM(RTRIM(AGE_MDL_TYP)) AGE_MDL_TYP,
  AGE_RNG_MIN_YR_NO,
  AGE_RNG_MAX_YR_NO
FROM {EDGERiskAsmntOwner}.CMS_DMGRPHC_FCTR
"""

df_ODBC_CMS_DMGRPHC_FCTR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_2)
    .load()
)

df_Copy = df_ODBC_CMS_DMGRPHC_FCTR.select(
    F.col("AGE_MDL_TYP").alias("AGE_MDL_TYP"),
    F.col("AGE_RNG_MIN_YR_NO").alias("AGE_RNG_MIN_YR_NO"),
    F.col("AGE_RNG_MAX_YR_NO").alias("AGE_RNG_MAX_YR_NO")
)

df_Lkp_Age_Mdl_Typ = (
    df_Age.alias("Age")
    .crossJoin(df_Copy.alias("Max_Min"))
    .select(
        F.col("Age.ISSUER_ID").alias("ISSUER_ID"),
        F.col("Age.FILE_ID").alias("FILE_ID"),
        F.col("Age.MBR_RCRD_ID").alias("MBR_RCRD_ID"),
        F.col("Age.MBR_ENR_PERD_RCRD_ID").alias("MBR_ENR_PERD_RCRD_ID"),
        F.col("Age.SUB_MBR_UNIQ_KEY").alias("SUB_MBR_UNIQ_KEY"),
        F.col("Age.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"),
        F.col("Age.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.col("Age.SUB_INDV_BE_KEY").alias("SUB_INDV_BE_KEY"),
        F.col("Age.MBR_GNDR_CD").alias("MBR_GNDR_CD"),
        F.col("Age.QHP_ID").alias("QHP_ID"),
        F.col("Age.GNRTN_DTM").alias("GNRTN_DTM"),
        F.col("Age.MBR_ENR_TERM_DT_SK").alias("MBR_ENR_TERM_DT_SK"),
        F.col("Age.MBR_ENR_EFF_DT_SK").alias("MBR_ENR_EFF_DT_SK"),
        F.col("Age.MBR_BRTH_DT").alias("MBR_BRTH_DT"),
        F.col("Age.PERD_AGE_YR_NO").alias("PERD_AGE_YR_NO"),
        F.col("Max_Min.AGE_MDL_TYP").alias("PERD_AGE_MDL_TYP"),
        F.col("Age.MBR_RELSHP_CD").alias("MBR_RELSHP_CD")
    )
)

df_Tfm_Age_Mdl_Typ = df_Lkp_Age_Mdl_Typ.select(
    F.col("ISSUER_ID").alias("ISSUER_ID"),
    F.col("FILE_ID").alias("FILE_ID"),
    F.col("MBR_RCRD_ID").alias("MBR_RCRD_ID"),
    F.col("MBR_ENR_PERD_RCRD_ID").alias("MBR_ENR_PERD_RCRD_ID"),
    F.col("SUB_MBR_UNIQ_KEY").alias("SUB_MBR_UNIQ_KEY"),
    F.col("SUB_INDV_BE_KEY").alias("SUB_INDV_BE_KEY"),
    F.col("MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("MBR_GNDR_CD").alias("MBR_GNDR_CD"),
    F.col("QHP_ID").alias("QHP_ID"),
    F.col("GNRTN_DTM").alias("GNRTN_DTM"),
    F.col("MBR_ENR_TERM_DT_SK").alias("MBR_ENR_TERM_DT_SK"),
    F.col("MBR_ENR_EFF_DT_SK").alias("MBR_ENR_EFF_DT_SK"),
    F.col("MBR_BRTH_DT").alias("MBR_BRTH_DT"),
    F.col("PERD_AGE_YR_NO").alias("PERD_AGE_YR_NO"),
    F.col("PERD_AGE_MDL_TYP").alias("PERD_AGE_MDL_TYP"),
    F.col("MBR_RELSHP_CD").alias("MBR_RELSHP_CD")
)

df_CMS_MBR_ENR_PERD_HIST_updt = df_Tfm_Age_Mdl_Typ.select(
    F.rpad(F.col("ISSUER_ID"), 5, " ").alias("ISSUER_ID"),
    F.rpad(F.col("FILE_ID"), 12, " ").alias("FILE_ID"),
    F.col("MBR_RCRD_ID"),
    F.col("MBR_ENR_PERD_RCRD_ID"),
    F.col("SUB_MBR_UNIQ_KEY"),
    F.col("SUB_INDV_BE_KEY"),
    F.col("MBR_INDV_BE_KEY"),
    F.col("MBR_UNIQ_KEY"),
    F.col("MBR_GNDR_CD"),
    F.col("QHP_ID"),
    F.col("GNRTN_DTM"),
    F.col("MBR_ENR_TERM_DT_SK"),
    F.col("MBR_ENR_EFF_DT_SK"),
    F.col("MBR_BRTH_DT"),
    F.col("PERD_AGE_YR_NO"),
    F.col("PERD_AGE_MDL_TYP"),
    F.col("MBR_RELSHP_CD")
)

write_files(
    df_CMS_MBR_ENR_PERD_HIST_updt,
    f"{adls_path}/load/CMS_MBR_ENR_PERD_HIST_updt.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)