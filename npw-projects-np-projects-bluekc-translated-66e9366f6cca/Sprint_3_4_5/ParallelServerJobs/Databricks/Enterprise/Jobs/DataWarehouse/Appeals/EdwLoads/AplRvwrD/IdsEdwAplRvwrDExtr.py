# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date               Project/Altiris #               Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------   ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Bhoomi Dasari                2/08/2007           Appeals/3028              Originally Programmed                        devlEDW10                  
# MAGIC 
# MAGIC 
# MAGIC Rama Kamjula                11/05/2013          5114                         Converted from Server to  Parallel        EnterpriseWrhsDevl        Jag Yelavarthi             2014-01-17

# MAGIC JobName: IdsEdwMbrMrkrMesrIExtr
# MAGIC 
# MAGIC Job creates loadfile for MBR_Mrkr_Mesr_I  in EDW
# MAGIC JobName: IDsEdwAplRvwrDExtr
# MAGIC 
# MAGIC EDW  Appeal Reviewer Extract from IDS
# MAGIC Appeal Reviewer Extract from IDS
# MAGIC Null handling and Business Logic
# MAGIC Creates Load file for APL_RVWR_D
# MAGIC Appeal  User Extract from IDS
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner', '')
ids_secret_name = get_widget_value('ids_secret_name', '')
ExtractRunCycle = get_widget_value('ExtractRunCycle', '')
CurrRunCycleDate = get_widget_value('CurrRunCycleDate', '')
CurrRunCycle = get_widget_value('CurrRunCycle', '')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# db2_AplRvwr
extract_query_db2_AplRvwr = f"""
SELECT
  APL_RVWR.APL_RVWR_SK,
  COALESCE(CD.TRGT_CD, 'NA') SRC_SYS_CD,
  APL_RVWR.APL_RVWR_ID,
  APL_RVWR.LAST_UPDT_USER_SK,
  APL_RVWR.APL_RVWR_CAT_CD_SK,
  APL_RVWR.APL_RVWR_SUBCAT_CD_SK,
  APL_RVWR.EFF_DT_SK,
  APL_RVWR.LAST_UPDT_DTM,
  APL_RVWR.CRDTL_SUM_DESC,
  APL_RVWR.CRT_RUN_CYC_EXCTN_SK,
  APL_RVWR.LAST_UPDT_RUN_CYC_EXCTN_SK,
  APL_RVWR.APL_RVWR_NM
FROM {IDSOwner}.APL_RVWR APL_RVWR
LEFT JOIN {IDSOwner}.CD_MPPNG CD
  ON CD.CD_MPPNG_SK = APL_RVWR.SRC_SYS_CD_SK
WHERE APL_RVWR.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtractRunCycle}
"""
df_db2_AplRvwr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_AplRvwr)
    .load()
)

# db2_AplRvwrUsr
extract_query_db2_AplRvwrUsr = f"""
SELECT
  APP_USER.USER_SK,
  APP_USER.USER_ID
FROM {IDSOwner}.APP_USER APP_USER
"""
df_db2_AplRvwrUsr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_AplRvwrUsr)
    .load()
)

# db2_CD_MPPNG
extract_query_db2_CD_MPPNG = f"""
SELECT
  CD.CD_MPPNG_SK,
  CD.TRGT_CD,
  CD.TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG CD
"""
df_db2_CD_MPPNG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CD_MPPNG)
    .load()
)

# cp_CdMppng (PxCopy)
df_cp_CdMppng = df_db2_CD_MPPNG.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

# For lookup usage, replicate for subcat/cat
df_cp_CdMppngSub = df_cp_CdMppng.select("CD_MPPNG_SK", "TRGT_CD", "TRGT_CD_NM")
df_cp_CdMppngCat = df_cp_CdMppng.select("CD_MPPNG_SK", "TRGT_CD", "TRGT_CD_NM")

# lkp_Codes (PxLookup)
df_lkp_Codes = (
    df_db2_AplRvwr.alias("lnk_AplRvwr")
    .join(
        df_cp_CdMppngSub.alias("lnk_AplRvwrSubctCdLkup"),
        F.col("lnk_AplRvwr.APL_RVWR_SUBCAT_CD_SK") == F.col("lnk_AplRvwrSubctCdLkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_cp_CdMppngCat.alias("lnk_AplRvwrCatCdLkup"),
        F.col("lnk_AplRvwr.APL_RVWR_CAT_CD_SK") == F.col("lnk_AplRvwrCatCdLkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_db2_AplRvwrUsr.alias("lnk_AplRvwrUsr"),
        F.col("lnk_AplRvwr.LAST_UPDT_USER_SK") == F.col("lnk_AplRvwrUsr.USER_SK"),
        "left"
    )
)

df_lkp_Codes_out = df_lkp_Codes.select(
    F.col("lnk_AplRvwr.APL_RVWR_SK").alias("APL_RVWR_SK"),
    F.col("lnk_AplRvwr.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_AplRvwr.APL_RVWR_ID").alias("APL_RVWR_ID"),
    F.col("lnk_AplRvwr.LAST_UPDT_USER_SK").alias("LAST_UPDT_USER_SK"),
    F.col("lnk_AplRvwr.APL_RVWR_CAT_CD_SK").alias("APL_RVWR_CAT_CD_SK"),
    F.col("lnk_AplRvwr.APL_RVWR_SUBCAT_CD_SK").alias("APL_RVWR_SUBCAT_CD_SK"),
    F.col("lnk_AplRvwr.EFF_DT_SK").alias("EFF_DT_SK"),
    F.col("lnk_AplRvwr.LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
    F.col("lnk_AplRvwr.APL_RVWR_NM").alias("APL_RVWR_NM"),
    F.col("lnk_AplRvwr.CRDTL_SUM_DESC").alias("CRDTL_SUM_DESC"),
    F.col("lnk_AplRvwr.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_AplRvwr.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_AplRvwrSubctCdLkup.TRGT_CD").alias("TRGT_CD_AplRvwrSubCatCd"),
    F.col("lnk_AplRvwrSubctCdLkup.TRGT_CD_NM").alias("TRGT_CD_NM_AplRvwrSubCatCd"),
    F.col("lnk_AplRvwrCatCdLkup.TRGT_CD").alias("TRGT_CD_AplRvwrCatCd"),
    F.col("lnk_AplRvwrCatCdLkup.TRGT_CD_NM").alias("TRGT_CD_NM_AplRvwrCatCd"),
    F.col("lnk_AplRvwrUsr.USER_ID").alias("USER_ID")
)

# xfm_BusinessLogic (CTransformerStage)
# Output link lnk_AplRvwrOut
df_AplRvwrOut = (
    df_lkp_Codes_out
    .filter(
        (F.col("APL_RVWR_SK") != 0) & (F.col("APL_RVWR_SK") != 1)
    )
    .select(
        F.col("APL_RVWR_SK").alias("APL_RVWR_SK"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("APL_RVWR_ID").alias("APL_RVWR_ID"),
        F.lit(CurrRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.lit(CurrRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.col("LAST_UPDT_USER_SK").alias("LAST_UPDT_USER_SK"),
        F.col("TRGT_CD_AplRvwrCatCd").alias("APL_RVWR_CAT_CD"),
        F.col("TRGT_CD_NM_AplRvwrCatCd").alias("APL_RVWR_CAT_NM"),
        F.col("TRGT_CD_AplRvwrSubCatCd").alias("APL_RVWR_SUBCAT_CD"),
        F.col("TRGT_CD_NM_AplRvwrSubCatCd").alias("APL_RVWR_SUBCAT_NM"),
        F.col("EFF_DT_SK").alias("APL_RVWR_EFF_DT_SK"),
        F.date_format(F.col("LAST_UPDT_DTM"), "yyyy-MM-dd").alias("LAST_UPDT_DT_SK"),
        F.col("CRDTL_SUM_DESC").alias("APL_RVWR_CRDTL_SUM_DESC"),
        F.col("APL_RVWR_NM").alias("APL_RVWR_NM"),
        F.when(
            F.col("USER_ID").isNull() | (trim(F.col("USER_ID")) == ""), 
            "NA"
        ).otherwise(F.col("USER_ID")).alias("LAST_UPDT_USER_ID"),
        F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("APL_RVWR_CAT_CD_SK").alias("APL_RVWR_CAT_CD_SK"),
        F.col("APL_RVWR_SUBCAT_CD_SK").alias("APL_RVWR_SUBCAT_CD_SK")
    )
)

# Output link lnk_NA (single row)
df_NA = spark.range(1).select(
    F.lit(1).alias("APL_RVWR_SK"),
    F.lit("NA").alias("SRC_SYS_CD"),
    F.lit("NA").alias("APL_RVWR_ID"),
    F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(1).alias("LAST_UPDT_USER_SK"),
    F.lit("NA").alias("APL_RVWR_CAT_CD"),
    F.lit("NA").alias("APL_RVWR_CAT_NM"),
    F.lit("NA").alias("APL_RVWR_SUBCAT_CD"),
    F.lit("NA").alias("APL_RVWR_SUBCAT_NM"),
    F.lit("1753-01-01").alias("APL_RVWR_EFF_DT_SK"),
    F.lit("1753-01-01").alias("LAST_UPDT_DT_SK"),
    F.lit(None).alias("APL_RVWR_CRDTL_SUM_DESC"),
    F.lit(None).alias("APL_RVWR_NM"),
    F.lit("NA").alias("LAST_UPDT_USER_ID"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("APL_RVWR_CAT_CD_SK"),
    F.lit(1).alias("APL_RVWR_SUBCAT_CD_SK")
)

# Output link lnk_UNK (single row)
df_UNK = spark.range(1).select(
    F.lit(0).alias("APL_RVWR_SK"),
    F.lit("UNK").alias("SRC_SYS_CD"),
    F.lit("UNK").alias("APL_RVWR_ID"),
    F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(0).alias("LAST_UPDT_USER_SK"),
    F.lit("UNK").alias("APL_RVWR_CAT_CD"),
    F.lit("UNK").alias("APL_RVWR_CAT_NM"),
    F.lit("UNK").alias("APL_RVWR_SUBCAT_CD"),
    F.lit("UNK").alias("APL_RVWR_SUBCAT_NM"),
    F.lit("1753-01-01").alias("APL_RVWR_EFF_DT_SK"),
    F.lit("1753-01-01").alias("LAST_UPDT_DT_SK"),
    F.lit(None).alias("APL_RVWR_CRDTL_SUM_DESC"),
    F.lit(None).alias("APL_RVWR_NM"),
    F.lit("UNK").alias("LAST_UPDT_USER_ID"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("APL_RVWR_CAT_CD_SK"),
    F.lit(0).alias("APL_RVWR_SUBCAT_CD_SK")
)

# Funnel_90 (PxFunnel)
funnel_columns = [
    "APL_RVWR_SK","SRC_SYS_CD","APL_RVWR_ID","CRT_RUN_CYC_EXCTN_DT_SK","LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_USER_SK","APL_RVWR_CAT_CD","APL_RVWR_CAT_NM","APL_RVWR_SUBCAT_CD","APL_RVWR_SUBCAT_NM",
    "APL_RVWR_EFF_DT_SK","LAST_UPDT_DT_SK","APL_RVWR_CRDTL_SUM_DESC","APL_RVWR_NM","LAST_UPDT_USER_ID",
    "CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK","APL_RVWR_CAT_CD_SK","APL_RVWR_SUBCAT_CD_SK"
]

df_funnel_90 = (
    df_AplRvwrOut.select(funnel_columns)
    .unionByName(df_NA.select(funnel_columns))
    .unionByName(df_UNK.select(funnel_columns))
)

# Final rpad for columns with char(10) in the final layout
df_final = (
    df_funnel_90
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("APL_RVWR_EFF_DT_SK", F.rpad(F.col("APL_RVWR_EFF_DT_SK"), 10, " "))
    .withColumn("LAST_UPDT_DT_SK", F.rpad(F.col("LAST_UPDT_DT_SK"), 10, " "))
)

# seq_APL_RVWR_D (PxSequentialFile) - write to .dat
write_files(
    df_final.select(funnel_columns),
    f"{adls_path}/load/APL_RVWR_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)