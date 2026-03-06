# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC COPYRIGHT 2023 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC CALLED BY :  OptumDrugCntl
# MAGIC  
# MAGIC DESCRIPTION:  Extract from IDS table DRUG_CLM_ACCUM_IMPCT to create input file forDataMart table CLM_DM_DRUG_CLM_ACCUM_IMPCT.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                     Date               Project                 Change Description                                                                                                   Development Project    Code Reviewer               Date Reviewed
# MAGIC --------------------------          --------------------    ----------------------      ------------------------------------------------------------------------------------------------------------------------------    ---------------------------------    ------------------------------------    ----------------------------              
# MAGIC Bill Schroeder              07/27/2023     US-586570          Original Programming                                                                                                 IntegrateDev2                Jeyaprasanna                2023-08-15

# MAGIC Lookup in mapping table to get the name (NCP_COUPON_TYP_CD_NM) of the coupon type code.
# MAGIC Transform integer NCP_COUPON_TYP_CD_SK back to character for Lookup against TRGT_CD.
# MAGIC Extract from IDS table DRUG_CLM_ACCUM_IMPCT to create input file for DataMart table CLM_DM_DRUG_CLM_ACCUM_IMPCT.
# MAGIC Lookup in mapping table to get the target code for SRC_SYS_CD.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, when, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value("IDSOwner", "")
ids_secret_name = get_widget_value("ids_secret_name", "")
RunCycle = get_widget_value("RunCycle", "")

# Stage: DB2_CD_MPPNG_Coupon_Nm
extract_query_DB2_CD_MPPNG_Coupon_Nm = f"""SELECT CD_MPPNG_SK as SRC_CD,TRGT_CD_NM,TRGT_CD
  FROM {IDSOwner}.CD_MPPNG
 WHERE SRC_CLCTN_CD   = 'OPTUMRX'
   AND SRC_SYS_CD     = 'OPTUMRX'
   AND SRC_DOMAIN_NM  = 'NCP COUPON TYPE'
   AND TRGT_CLCTN_CD  = 'IDS'
   AND TRGT_DOMAIN_NM = 'NCP COUPON TYPE'
 union
 SELECT CD_MPPNG_SK as SRC_CD,TRGT_CD_NM,TRGT_CD
   FROM {IDSOwner}.CD_MPPNG
  WHERE CD_MPPNG_SK='1'"""
jdbc_url_DB2_CD_MPPNG_Coupon_Nm, jdbc_props_DB2_CD_MPPNG_Coupon_Nm = get_db_config(ids_secret_name)
df_DB2_CD_MPPNG_Coupon_Nm = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_DB2_CD_MPPNG_Coupon_Nm)
    .options(**jdbc_props_DB2_CD_MPPNG_Coupon_Nm)
    .option("query", extract_query_DB2_CD_MPPNG_Coupon_Nm)
    .load()
)

# Stage: DB2_CD_MPPNG_Src_Cd
extract_query_DB2_CD_MPPNG_Src_Cd = f"""SELECT CD_MPPNG_SK, TRGT_CD
FROM {IDSOwner}.CD_MPPNG
WHERE TRGT_DOMAIN_NM = 'SOURCE SYSTEM'"""
jdbc_url_DB2_CD_MPPNG_Src_Cd, jdbc_props_DB2_CD_MPPNG_Src_Cd = get_db_config(ids_secret_name)
df_DB2_CD_MPPNG_Src_Cd = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_DB2_CD_MPPNG_Src_Cd)
    .options(**jdbc_props_DB2_CD_MPPNG_Src_Cd)
    .option("query", extract_query_DB2_CD_MPPNG_Src_Cd)
    .load()
)

# Stage: DRUG_CLM_ACCUM_IMPCT
extract_query_DRUG_CLM_ACCUM_IMPCT = f"""SELECT
 CLM.DRUG_CLM_SK,
 CLM.SRC_SYS_CD_SK,
 CLM.CLM_ID,
 CLM.CRT_RUN_CYC_EXCTN_SK,
 CLM.LAST_UPDT_RUN_CYC_EXCTN_SK,
 CLM.NCP_COUPON_TYP_CD_SK,
 CLM.CCAA_APLD_IN,
 CLM.CCAA_COUPON_AMT,
 CLM.FMLY_ACCUM_DEDCT_AMT,
 CLM.FMLY_ACCUM_OOP_AMT,
 CLM.INDV_ACCUM_DEDCT_AMT,
 CLM.INDV_ACCUM_OOP_AMT,
 CLM.INDV_APLD_DEDCT_AMT,
 CLM.INDV_APLD_OOP_AMT
FROM {IDSOwner}.DRUG_CLM_ACCUM_IMPCT CLM,
     {IDSOwner}.W_WEBDM_ETL_DRVR DRVR
WHERE
  DRVR.SRC_SYS_CD_SK = CLM.SRC_SYS_CD_SK
  AND DRVR.CLM_ID = CLM.CLM_ID"""
jdbc_url_DRUG_CLM_ACCUM_IMPCT, jdbc_props_DRUG_CLM_ACCUM_IMPCT = get_db_config(ids_secret_name)
df_DRUG_CLM_ACCUM_IMPCT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_DRUG_CLM_ACCUM_IMPCT)
    .options(**jdbc_props_DRUG_CLM_ACCUM_IMPCT)
    .option("query", extract_query_DRUG_CLM_ACCUM_IMPCT)
    .load()
)

# Stage: Trans_Coupon (CTransformerStage)
df_Trans_Coupon = df_DRUG_CLM_ACCUM_IMPCT.select(
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("CLM_ID").alias("CLM_ID"),
    col("CCAA_APLD_IN").alias("CCAA_APLD_IN"),
    col("CCAA_COUPON_AMT").alias("CCAA_COUPON_AMT"),
    col("FMLY_ACCUM_DEDCT_AMT").alias("FMLY_ACCUM_DEDCT_AMT"),
    col("FMLY_ACCUM_OOP_AMT").alias("FMLY_ACCUM_OOP_AMT"),
    col("INDV_ACCUM_DEDCT_AMT").alias("INDV_ACCUM_DEDCT_AMT"),
    col("INDV_ACCUM_OOP_AMT").alias("INDV_ACCUM_OOP_AMT"),
    col("INDV_APLD_DEDCT_AMT").alias("INDV_APLD_DEDCT_AMT"),
    col("INDV_APLD_OOP_AMT").alias("INDV_APLD_OOP_AMT"),
    col("NCP_COUPON_TYP_CD_SK").alias("NCP_COUPON_TYP_CD")
)

# Stage: Lkup_Codes (PxLookup)
df_lookup_temp = df_Trans_Coupon.alias("Lnk_Trans_Extr").join(
    df_DB2_CD_MPPNG_Coupon_Nm.alias("Lnk_Lkup_Coupon_Nm"),
    col("Lnk_Trans_Extr.NCP_COUPON_TYP_CD") == col("Lnk_Lkup_Coupon_Nm.SRC_CD"),
    "left"
)
df_Lkup_Codes = df_lookup_temp.join(
    df_DB2_CD_MPPNG_Src_Cd.alias("Lnk_Lkup_Src_Cd"),
    col("Lnk_Trans_Extr.SRC_SYS_CD_SK") == col("Lnk_Lkup_Src_Cd.CD_MPPNG_SK"),
    "left"
)
df_Lkup_Codes = df_Lkup_Codes.select(
    col("Lnk_Trans_Extr.CLM_ID").alias("CLM_ID"),
    col("Lnk_Trans_Extr.CCAA_APLD_IN").alias("CCAA_APLD_IN"),
    col("Lnk_Trans_Extr.CCAA_COUPON_AMT").alias("CCAA_COUPON_AMT"),
    col("Lnk_Trans_Extr.FMLY_ACCUM_DEDCT_AMT").alias("FMLY_ACCUM_DEDCT_AMT"),
    col("Lnk_Trans_Extr.FMLY_ACCUM_OOP_AMT").alias("FMLY_ACCUM_OOP_AMT"),
    col("Lnk_Trans_Extr.INDV_ACCUM_DEDCT_AMT").alias("INDV_ACCUM_DEDCT_AMT"),
    col("Lnk_Trans_Extr.INDV_ACCUM_OOP_AMT").alias("INDV_ACCUM_OOP_AMT"),
    col("Lnk_Trans_Extr.INDV_APLD_DEDCT_AMT").alias("INDV_APLD_DEDCT_AMT"),
    col("Lnk_Trans_Extr.INDV_APLD_OOP_AMT").alias("INDV_APLD_OOP_AMT"),
    col("Lnk_Lkup_Src_Cd.CD_MPPNG_SK").alias("CD_MPPNG_SK_Src_Sys"),
    col("Lnk_Lkup_Src_Cd.TRGT_CD").alias("TRGT_CD_Src_Sys"),
    col("Lnk_Lkup_Coupon_Nm.TRGT_CD").alias("NCP_COUPON_TYP_CD"),
    col("Lnk_Lkup_Coupon_Nm.TRGT_CD_NM").alias("NCP_COUPON_TYP_NM")
)

# Stage: Trans_Src_Sys (CTransformerStage)
df_Trans_Src_Sys = df_Lkup_Codes.select(
    when(col("CD_MPPNG_SK_Src_Sys").isNull(), lit(" ")).otherwise(col("TRGT_CD_Src_Sys")).alias("SRC_SYS_CD"),
    col("CLM_ID").alias("CLM_ID"),
    when(col("NCP_COUPON_TYP_CD").isNull(), lit(" ")).otherwise(col("NCP_COUPON_TYP_CD")).alias("NCP_COUPON_TYP_CD"),
    when(col("NCP_COUPON_TYP_NM").isNull(), lit(" ")).otherwise(col("NCP_COUPON_TYP_NM")).alias("NCP_COUPON_TYP_NM"),
    col("CCAA_APLD_IN").alias("CCAA_APLD_IN"),
    col("CCAA_COUPON_AMT").alias("CCAA_COUPON_AMT"),
    col("FMLY_ACCUM_DEDCT_AMT").alias("FMLY_ACCUM_DEDCT_AMT"),
    col("FMLY_ACCUM_OOP_AMT").alias("FMLY_ACCUM_OOP_AMT"),
    col("INDV_ACCUM_DEDCT_AMT").alias("INDV_ACCUM_DEDCT_AMT"),
    col("INDV_ACCUM_OOP_AMT").alias("INDV_ACCUM_OOP_AMT"),
    col("INDV_APLD_DEDCT_AMT").alias("INDV_APLD_DEDCT_AMT"),
    col("INDV_APLD_OOP_AMT").alias("INDV_APLD_OOP_AMT"),
    lit(RunCycle).alias("LAST_UPDT_RUN_CYC_NO")
)

# Stage: CLM_DM_DRUG_CLM_ACCUM_IMPCT_extract (PxSequentialFile)
df_final = df_Trans_Src_Sys.select(
    col("SRC_SYS_CD"),
    col("CLM_ID"),
    col("NCP_COUPON_TYP_CD"),
    col("NCP_COUPON_TYP_NM"),
    rpad(col("CCAA_APLD_IN"), 1, " ").alias("CCAA_APLD_IN"),
    col("CCAA_COUPON_AMT"),
    col("FMLY_ACCUM_DEDCT_AMT"),
    col("FMLY_ACCUM_OOP_AMT"),
    col("INDV_ACCUM_DEDCT_AMT"),
    col("INDV_ACCUM_OOP_AMT"),
    col("INDV_APLD_DEDCT_AMT"),
    col("INDV_APLD_OOP_AMT"),
    col("LAST_UPDT_RUN_CYC_NO")
)

write_files(
    df_final,
    f"{adls_path}/load/CLM_DM_DRUG_CLM_ACCUM_IMPCT.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)