# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2023 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC CALLED BY :  IdsClaimExtrSeq
# MAGIC  
# MAGIC DESCRIPTION:  Extract from IDS table DRUG_CLM_ACCUM_IMPCT to create input file for EDW table DRUG_CLM_ACCUM_IMPCT_F.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                     Date               Project                 Change Description                                                                                                   Development Project    Code Reviewer               Date Reviewed
# MAGIC --------------------------          --------------------    ----------------------      ------------------------------------------------------------------------------------------------------------------------------    ---------------------------------    ------------------------------------    ----------------------------              
# MAGIC Bill Schroeder              07/18/2023     US-586570          Original Programming                                                                                                 EnterpriseDev2              Jeyaprasanna                  2023-08-14

# MAGIC Transform integer NCP_COUPON_TYP_CD_SK back to character for Lookup against TRGT_CD.
# MAGIC Lookup in table K_CLM (already created for table CLM) to get the surrogate key values for column CLM_SK.
# MAGIC Extract from IDS table DRUG_CLM_ACCUM_IMPCT to create input file for EDW table DRUG_CLM_ACCUM_IMPCT_F.
# MAGIC Lookup in mapping table to get the target code for SRC_SYS_CD.
# MAGIC Lookup in mapping table to get the name (NCP_COUPON_TYP_CD_NM) of the coupon type code.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, coalesce, rpad
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------

EDWRunCycle = get_widget_value('EDWRunCycle','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
IDSOwner = get_widget_value('$IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# Stage: db2_CD_MPPNG
extract_query = f"""SELECT  CD_MPPNG_SK as SRC_CD, TRGT_CD_NM,TRGT_CD
  FROM {IDSOwner}.CD_MPPNG 
 WHERE SRC_CLCTN_CD   = 'OPTUMRX' 
   AND SRC_SYS_CD     = 'OPTUMRX' 
   AND SRC_DOMAIN_NM  = 'NCP COUPON TYPE' 
   AND TRGT_CLCTN_CD  = 'IDS' 
   AND TRGT_DOMAIN_NM = 'NCP COUPON TYPE' 
union
SELECT  CD_MPPNG_SK as SRC_CD, TRGT_CD_NM,TRGT_CD
  FROM {IDSOwner}.CD_MPPNG 
 where CD_MPPNG_SK='1'"""
df_db2_CD_MPPNG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# Stage: db2_K_CLM
extract_query = f"""select C.SRC_SYS_CD_SK, C.CLM_ID, C.CRT_RUN_CYC_EXCTN_SK, C.CLM_SK 
from {IDSOwner}.K_CLM C,
     {IDSOwner}.CD_MPPNG CD
where C.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
  and cd.TRGT_CD = 'OPTUMRX'
  and cd.TRGT_DOMAIN_NM = 'SOURCE SYSTEM'"""
df_db2_K_CLM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# Stage: DB2_CD_MPPNG_Src_Cd
extract_query = f"""SELECT CD_MPPNG_SK, TRGT_CD_NM 
FROM {IDSOwner}.CD_MPPNG 
WHERE TRGT_DOMAIN_NM = 'SOURCE SYSTEM'"""
df_DB2_CD_MPPNG_Src_Cd = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# Stage: db2_DRUG_CLM_ACCUM_IMPCT
extract_query = f"""SELECT 
    C.DRUG_CLM_SK, 
    C.SRC_SYS_CD_SK, 
    C.CLM_ID, 
    C.CRT_RUN_CYC_EXCTN_SK, 
    C.LAST_UPDT_RUN_CYC_EXCTN_SK, 
    C.NCP_COUPON_TYP_CD_SK, 
    C.CCAA_APLD_IN, 
    C.CCAA_COUPON_AMT, 
    C.FMLY_ACCUM_DEDCT_AMT, 
    C.FMLY_ACCUM_OOP_AMT, 
    C.INDV_ACCUM_DEDCT_AMT, 
    C.INDV_ACCUM_OOP_AMT, 
    C.INDV_APLD_DEDCT_AMT, 
    C.INDV_APLD_OOP_AMT
FROM 
    {IDSOwner}.DRUG_CLM_ACCUM_IMPCT C,
    {IDSOwner}.W_EDW_ETL_DRVR DRVR
WHERE 
    C.CLM_ID = DRVR.CLM_ID AND 
    C.SRC_SYS_CD_SK = DRVR.SRC_SYS_CD_SK"""
df_db2_DRUG_CLM_ACCUM_IMPCT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# Stage: Trans_Coupon
df_Trans_Coupon = df_db2_DRUG_CLM_ACCUM_IMPCT.select(
    col("DRUG_CLM_SK").alias("DRUG_CLM_SK"),
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("CLM_ID").alias("CLM_ID"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("NCP_COUPON_TYP_CD_SK").alias("NCP_COUPON_TYP_CD"),
    col("NCP_COUPON_TYP_CD_SK").alias("NCP_COUPON_TYP_CD_SK"),
    col("CCAA_APLD_IN").alias("CCAA_APLD_IN"),
    col("CCAA_COUPON_AMT").alias("CCAA_COUPON_AMT"),
    col("FMLY_ACCUM_DEDCT_AMT").alias("FMLY_ACCUM_DEDCT_AMT"),
    col("FMLY_ACCUM_OOP_AMT").alias("FMLY_ACCUM_OOP_AMT"),
    col("INDV_ACCUM_DEDCT_AMT").alias("INDV_ACCUM_DEDCT_AMT"),
    col("INDV_ACCUM_OOP_AMT").alias("INDV_ACCUM_OOP_AMT"),
    col("INDV_APLD_DEDCT_AMT").alias("INDV_APLD_DEDCT_AMT"),
    col("INDV_APLD_OOP_AMT").alias("INDV_APLD_OOP_AMT")
).withColumn("CCAA_APLD_IN", rpad(col("CCAA_APLD_IN"), 1, " "))

# Stage: Lkup_for_TRGT_CD_NM (PxLookup)
df_Lkup_for_TRGT_CD_NM = (
    df_Trans_Coupon.alias("Lnk_Trans_Extr")
    .join(
        df_db2_CD_MPPNG.alias("Ref_db2_CD_MPPNG"),
        col("Lnk_Trans_Extr.NCP_COUPON_TYP_CD") == col("Ref_db2_CD_MPPNG.SRC_CD"),
        "left"
    )
    .join(
        df_db2_K_CLM.alias("Ref_db2_K_CLM"),
        [
            col("Lnk_Trans_Extr.SRC_SYS_CD_SK") == col("Ref_db2_K_CLM.SRC_SYS_CD_SK"),
            col("Lnk_Trans_Extr.CLM_ID") == col("Ref_db2_K_CLM.CLM_ID")
        ],
        "left"
    )
    .join(
        df_DB2_CD_MPPNG_Src_Cd.alias("Lnk_Lkup_Src_Cd"),
        col("Lnk_Trans_Extr.SRC_SYS_CD_SK") == col("Lnk_Lkup_Src_Cd.CD_MPPNG_SK"),
        "left"
    )
)

df_Lkup_for_TRGT_CD_NM = df_Lkup_for_TRGT_CD_NM.select(
    col("Ref_db2_K_CLM.CLM_SK").alias("CLM_SK"),
    col("Lnk_Trans_Extr.DRUG_CLM_SK").alias("DRUG_CLM_SK"),
    col("Lnk_Trans_Extr.CLM_ID").alias("CLM_ID"),
    col("Lnk_Trans_Extr.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("Lnk_Trans_Extr.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("Ref_db2_CD_MPPNG.TRGT_CD_NM").alias("TRGT_CD_NM"),
    col("Lnk_Trans_Extr.CCAA_APLD_IN").alias("CCAA_APLD_IN"),
    col("Lnk_Trans_Extr.CCAA_COUPON_AMT").alias("CCAA_COUPON_AMT"),
    col("Lnk_Trans_Extr.FMLY_ACCUM_DEDCT_AMT").alias("FMLY_ACCUM_DEDCT_AMT"),
    col("Lnk_Trans_Extr.FMLY_ACCUM_OOP_AMT").alias("FMLY_ACCUM_OOP_AMT"),
    col("Lnk_Trans_Extr.INDV_ACCUM_DEDCT_AMT").alias("INDV_ACCUM_DEDCT_AMT"),
    col("Lnk_Trans_Extr.INDV_ACCUM_OOP_AMT").alias("INDV_ACCUM_OOP_AMT"),
    col("Lnk_Trans_Extr.INDV_APLD_DEDCT_AMT").alias("INDV_APLD_DEDCT_AMT"),
    col("Lnk_Trans_Extr.INDV_APLD_OOP_AMT").alias("INDV_APLD_OOP_AMT"),
    col("Lnk_Trans_Extr.NCP_COUPON_TYP_CD_SK").alias("NCP_COUPON_TYP_CD_SK"),
    col("Lnk_Lkup_Src_Cd.TRGT_CD_NM").alias("TRGT_CD_Src_Sys"),
    col("Ref_db2_CD_MPPNG.TRGT_CD").alias("TRGT_CD")
).withColumn("CCAA_APLD_IN", rpad(col("CCAA_APLD_IN"), 1, " "))

# Stage: Trans_Accum
df_Trans_Accum = df_Lkup_for_TRGT_CD_NM.select(
    col("DRUG_CLM_SK").alias("CLM_SK"),
    col("CLM_ID").alias("CLM_ID"),
    coalesce(col("TRGT_CD_Src_Sys"), lit(" ")).alias("SRC_SYS_CD"),
    lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("CLM_SK").alias("DRUG_CLM_SK"),
    coalesce(col("TRGT_CD"), lit(" ")).alias("NCP_COUPON_TYP_CD"),
    coalesce(col("TRGT_CD_NM"), lit(" ")).alias("NCP_COUPON_TYP_CD_NM"),
    col("CCAA_APLD_IN").alias("CCAA_APLD_IN"),
    col("CCAA_COUPON_AMT").alias("CCAA_COUPON_AMT"),
    col("FMLY_ACCUM_DEDCT_AMT").alias("FMLY_ACCUM_DEDCT_AMT"),
    col("FMLY_ACCUM_OOP_AMT").alias("FMLY_ACCUM_OOP_AMT"),
    col("INDV_ACCUM_DEDCT_AMT").alias("INDV_ACCUM_DEDCT_AMT"),
    col("INDV_ACCUM_OOP_AMT").alias("INDV_ACCUM_OOP_AMT"),
    col("INDV_APLD_DEDCT_AMT").alias("INDV_APLD_DEDCT_AMT"),
    col("INDV_APLD_OOP_AMT").alias("INDV_APLD_OOP_AMT"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("NCP_COUPON_TYP_CD_SK").alias("NCP_COUPON_TYP_CD_SK")
)

df_Trans_Accum = df_Trans_Accum.withColumn("CCAA_APLD_IN", rpad(col("CCAA_APLD_IN"), 1, " "))
df_Trans_Accum = df_Trans_Accum.withColumn("CRT_RUN_CYC_EXCTN_DT_SK", rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
df_Trans_Accum = df_Trans_Accum.withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " "))

# Stage: DRUG_CLM_ACCUM_IMPCT_F_extract (PxSequentialFile)
write_files(
    df_Trans_Accum,
    f"{adls_path}/load/DRUG_CLM_ACCUM_IMPCT_F.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)