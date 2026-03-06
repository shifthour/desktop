# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:    IdsClmLnPcaExtr
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC 
# MAGIC   Pulls data from IDS table CLM_LN_PCA, CLM_LN, CLM, EXCD,W_EDW_PCA_ETL_DRVR and CD_MPPNG  into EDW table PCA_CLM_LN_F
# MAGIC       
# MAGIC INPUTS:
# MAGIC               CLM_LN_PCA
# MAGIC               CLM_LN
# MAGIC               CLM
# MAGIC               EXCD
# MAGIC               CD_MPPNG
# MAGIC               W_EDW_PCA_ETL_DRVR
# MAGIC   
# MAGIC HASH FILES:   
# MAGIC                   hf_pca_clm_ln_f_edw 
# MAGIC                   hf_pca_codes
# MAGIC 
# MAGIC TRANSFORMS:  NONE
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC 
# MAGIC                JOINING OF ALL THESE TABLES FOR DIFFERENT SOURCES
# MAGIC                CD MPPNG LOOKUP into Hash File
# MAGIC   
# MAGIC OUTPUTS: 
# MAGIC                   Loading into a sequential file
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                Parikshith Chada    11/09/2006  ---    Originally Programmed
# MAGIC                Ralph Tucker         11/21/2006 --     Added W_EDW_PCA_ETL_DRVR table to pull only PCA claims into EDW from IDS.
# MAGIC                Bhoomi Dasari        05/03/2007 - Made change to sql in ClmChk lookup
# MAGIC                Bhoomi Dasari        07/01/2007 - Made some code chages to source2.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                              Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------     -----------------------------------    ---------------------------------------------------------        ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               04/01/2008          TTR-284                  Changed the HASH.CLEAR part, wrong   devlEDWcur                Steph Goddard             04/08/2008
# MAGIC                                                                                                        hash files were being cleared
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Balkarn Gill               09/30/2013        5114                              Create Load File for EDW Table PCA_CLM_LN_F                             EnterpriseWhseDevl   Peter Marshall               12/20/2013

# MAGIC Job Name: IdsEdwClmSttusAuditFExtr
# MAGIC Read from source table CLM_LN .
# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC CLM_STTUS_CD_SK,
# MAGIC TRNSMSN_SRC_CD_SK,
# MAGIC CLM_STTUS_CHG_RSN_CD_SK.
# MAGIC Write PCA_CLM_LN_F Data into a Sequential file for Load Job IdsEdwPcaClmLnFLoad.
# MAGIC Add Defaults and Null Handling.
# MAGIC Job Name: IdsEdwPcaClmLnFExtr
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql.types import IntegerType, StringType, StructType, StructField
from pyspark.sql.functions import col, lit, when, rpad
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrRunCycleDate = get_widget_value('CurrRunCycleDate','')
CurrRunCycle = get_widget_value('CurrRunCycle','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

# ------------------------------------------
# Stage: db2_CLM_LN_PCA_in
# ------------------------------------------
extract_query_db2_CLM_LN_PCA_in = f"""
SELECT 
CLM_LN_PCA.CLM_LN_SK,
CD_MPPNG.TRGT_CD SRC_SYS_CD,
CLM_LN_PCA.CLM_ID,
CLM_LN_PCA.CLM_LN_SEQ_NO,
CLM_LN_PCA.CRT_RUN_CYC_EXCTN_SK,
CLM_LN_PCA.CLM_SK,
CLM_LN_PCA.DSALW_EXCD_SK,
CLM_LN_PCA.CLM_LN_PCA_LOB_CD_SK,
CLM_LN_PCA.CLM_LN_PCA_PRCS_CD_SK,
CLM_LN_PCA.CNSD_AMT,
CLM_LN_PCA.DSALW_AMT,
CLM_LN_PCA.NONCNSD_AMT,
CLM_LN_PCA.PROV_PD_AMT,
CLM_LN_PCA.SUB_PD_AMT,
CLM_LN_PCA.PD_AMT,
CD_MPPNG2.TRGT_CD CLM_LN_PCA_LOB_CD,
CD_MPPNG2.TRGT_CD_NM CLM_LN_PCA_LOB_NM,
CD_MPPNG3.TRGT_CD CLM_LN_PCA_PRCS_CD,
CD_MPPNG3.TRGT_CD_NM CLM_LN_PCA_PRCS_NM,
EXCD.EXCD_ID
FROM {IDSOwner}.CLM_LN_PCA CLM_LN_PCA,
     {IDSOwner}.CD_MPPNG CD_MPPNG,
     {IDSOwner}.CD_MPPNG CD_MPPNG2,
     {IDSOwner}.CD_MPPNG CD_MPPNG3,
     {IDSOwner}.W_EDW_PCA_ETL_DRVR DRVR,
     {IDSOwner}.EXCD EXCD
WHERE CLM_LN_PCA.SRC_SYS_CD_SK=CD_MPPNG.CD_MPPNG_SK
  AND CLM_LN_PCA.CLM_LN_PCA_LOB_CD_SK=CD_MPPNG2.CD_MPPNG_SK
  AND CLM_LN_PCA.CLM_LN_PCA_PRCS_CD_SK=CD_MPPNG3.CD_MPPNG_SK
  AND CLM_LN_PCA.DSALW_EXCD_SK=EXCD.EXCD_SK
  AND CLM_LN_PCA.CLM_ID = DRVR.CLM_ID
  AND CLM_LN_PCA.SRC_SYS_CD_SK = DRVR.SRC_SYS_CD_SK
"""
df_db2_CLM_LN_PCA_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_CLM_LN_PCA_in)
    .load()
)

# ------------------------------------------
# Stage: db2_CLM_CHK_in
# ------------------------------------------
extract_query_db2_CLM_CHK_in = f"""
SELECT 
CLM_CHK.CLM_SK,
CLM_CHK.CLM_CHK_LOB_CD_SK,
CLM_CHK.PCA_CHK_IN,
CD_MPPNG.TRGT_CD,
CD_MPPNG.TRGT_CD_NM 
FROM 
{IDSOwner}.CLM_CHK CLM_CHK,
{IDSOwner}.CD_MPPNG CD_MPPNG 
WHERE 
CLM_CHK.CLM_CHK_LOB_CD_SK = CD_MPPNG.CD_MPPNG_SK
AND CLM_CHK.PCA_CHK_IN = 'Y'
"""
df_db2_CLM_CHK_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_CLM_CHK_in)
    .load()
)

# ------------------------------------------
# Stage: db2_CD_MPPNGClmLnLob_Extr
# ------------------------------------------
extract_query_db2_CD_MPPNGClmLnLob_Extr = f"""
SELECT 
CD_MPPNG.CD_MPPNG_SK,
CD_MPPNG.TRGT_CD,
CD_MPPNG.TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG CD_MPPNG
WHERE CD_MPPNG.TRGT_DOMAIN_NM='CLAIM LINE LOB'
  AND CD_MPPNG.TRGT_CD='PCA'
"""
df_db2_CD_MPPNGClmLnLob_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_CD_MPPNGClmLnLob_Extr)
    .load()
)

# ------------------------------------------
# Stage: db2_CD_MPPNGPcaProc_Extr
# ------------------------------------------
extract_query_db2_CD_MPPNGPcaProc_Extr = f"""
SELECT 
CD_MPPNG.CD_MPPNG_SK,
CD_MPPNG.TRGT_CD,
CD_MPPNG.TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG CD_MPPNG
WHERE CD_MPPNG.TRGT_DOMAIN_NM='PERSONAL CARE ACCOUNT PROCESSING'
  AND CD_MPPNG.SRC_SYS_CD='FACETS'
  AND CD_MPPNG.SRC_CD='H'
"""
df_db2_CD_MPPNGPcaProc_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_CD_MPPNGPcaProc_Extr)
    .load()
)

# ------------------------------------------
# Stage: db2_CLM_LN_in
# ------------------------------------------
extract_query_db2_CLM_LN_in = f"""
SELECT 
CLM_LN.CLM_LN_SK,
CD_MPPNG2.TRGT_CD SRC_SYS_CD,
CLM.CLM_ID,
CLM_LN.CLM_LN_SEQ_NO,
CLM_LN.CRT_RUN_CYC_EXCTN_SK,
CLM.CLM_SK,
CLM_LN.CLM_LN_DSALW_EXCD_SK,
CD_MPPNG.CD_MPPNG_SK CLM_LN_PCA_PRCS_CD_SK,
CLM_LN.CHRG_AMT,
CLM.DSALW_AMT,
CLM.CNSD_CHRG_AMT,
CLM.COPAY_AMT,
CLM_LN.PAYBL_AMT SUB_PD_AMT,
CLM_LN.PAYBL_AMT,
CD_MPPNG2.TRGT_CD EXCD_ID
FROM
{IDSOwner}.CLM_LN CLM_LN,
{IDSOwner}.CLM CLM,
{IDSOwner}.CD_MPPNG CD_MPPNG,
{IDSOwner}.W_EDW_PCA_ETL_DRVR DRVR,
{IDSOwner}.CD_MPPNG CD_MPPNG2
WHERE CLM.PCA_TYP_CD_SK=CD_MPPNG.CD_MPPNG_SK
  AND CLM.CLM_SK=CLM_LN.CLM_SK
  AND CLM.CLM_ID = DRVR.CLM_ID
  AND CLM.SRC_SYS_CD_SK=DRVR.SRC_SYS_CD_SK
  AND CLM_LN.SRC_SYS_CD_SK=CD_MPPNG2.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD IN ('RUNOUT', 'EMPWBNF')
"""
df_db2_CLM_LN_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_CLM_LN_in)
    .load()
)

# ------------------------------------------
# Stage: Xfm_Pca (CTransformerStage)
# ------------------------------------------
df_xfm_Pca = (
    df_db2_CLM_LN_in
    .withColumn("CLM_LN_PCA_PRCS_CD", lit("PCA"))
    .withColumn("CLM_LN_PCA_PRCS_NM", lit("PERSONAL CARE ACCOUNT"))
    .select(
        col("CLM_LN_SK"),
        col("SRC_SYS_CD"),
        col("CLM_ID"),
        col("CLM_LN_SEQ_NO"),
        col("CRT_RUN_CYC_EXCTN_SK"),
        col("CLM_SK"),
        col("CLM_LN_DSALW_EXCD_SK").alias("CLM_LN_DSALW_EXCD_SK"),
        col("CLM_LN_PCA_PRCS_CD_SK"),
        col("CHRG_AMT"),
        col("DSALW_AMT"),
        col("CNSD_CHRG_AMT"),
        col("COPAY_AMT"),
        col("SUB_PD_AMT"),
        col("PAYBL_AMT"),
        col("CLM_LN_PCA_PRCS_CD"),
        col("CLM_LN_PCA_PRCS_NM"),
        col("EXCD_ID")
    )
)

# ------------------------------------------
# Stage: lkp_Codes (PxLookup)
# ------------------------------------------
df_lkp_Codes = (
    df_xfm_Pca.alias("xfm_Pca_Out")
    .join(
        df_db2_CLM_CHK_in.alias("lnk_ClmChk_Lkp"),
        col("xfm_Pca_Out.CLM_SK") == col("lnk_ClmChk_Lkp.CLM_SK"),
        "left"
    )
    .join(
        df_db2_CD_MPPNGClmLnLob_Extr.alias("lnk_ClmLnLob_Lkp"),
        (
            (col("xfm_Pca_Out.CLM_LN_PCA_PRCS_CD") == col("lnk_ClmLnLob_Lkp.TRGT_CD"))
            & (col("xfm_Pca_Out.CLM_LN_PCA_PRCS_NM") == col("lnk_ClmLnLob_Lkp.TRGT_CD_NM"))
        ),
        "left"
    )
    .join(
        df_db2_CD_MPPNGPcaProc_Extr.alias("lnk_PcaProc_lkp"),
        (
            (col("xfm_Pca_Out.CLM_LN_PCA_PRCS_CD") == col("lnk_PcaProc_lkp.TRGT_CD"))
            & (col("xfm_Pca_Out.CLM_LN_PCA_PRCS_NM") == col("lnk_PcaProc_lkp.TRGT_CD_NM"))
        ),
        "left"
    )
    .select(
        col("xfm_Pca_Out.CLM_LN_SK").alias("CLM_LN_SK"),
        col("xfm_Pca_Out.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("xfm_Pca_Out.CLM_ID").alias("CLM_ID"),
        col("xfm_Pca_Out.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        col("xfm_Pca_Out.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("xfm_Pca_Out.CLM_SK").alias("CLM_SK"),
        col("xfm_Pca_Out.CLM_LN_DSALW_EXCD_SK").alias("CLM_LN_DSALW_EXCD_SK"),
        col("xfm_Pca_Out.CLM_LN_PCA_PRCS_CD_SK").alias("CLM_LN_PCA_PRCS_CD_SK"),
        col("xfm_Pca_Out.CHRG_AMT").alias("CHRG_AMT"),
        col("xfm_Pca_Out.DSALW_AMT").alias("DSALW_AMT"),
        col("xfm_Pca_Out.CNSD_CHRG_AMT").alias("CNSD_CHRG_AMT"),
        col("xfm_Pca_Out.COPAY_AMT").alias("COPAY_AMT"),
        col("xfm_Pca_Out.SUB_PD_AMT").alias("SUB_PD_AMT"),
        col("xfm_Pca_Out.PAYBL_AMT").alias("PAYBL_AMT"),
        col("xfm_Pca_Out.CLM_LN_PCA_PRCS_CD").alias("CLM_LN_PCA_PRCS_CD"),
        col("xfm_Pca_Out.CLM_LN_PCA_PRCS_NM").alias("CLM_LN_PCA_PRCS_NM"),
        col("xfm_Pca_Out.EXCD_ID").alias("EXCD_ID"),
        col("lnk_PcaProc_lkp.CD_MPPNG_SK").alias("PCA_CD_MPPNG_SK_1"),
        col("lnk_PcaProc_lkp.TRGT_CD").alias("PCA_TRGT_CD"),
        col("lnk_PcaProc_lkp.TRGT_CD_NM").alias("PCA_TRGT_CD_NM"),
        col("lnk_ClmChk_Lkp.CLM_SK").alias("CLM_CHK_SK"),
        col("lnk_ClmChk_Lkp.CLM_CHK_LOB_CD_SK").alias("CLM_CHK_LOB_CD_SK"),
        col("lnk_ClmChk_Lkp.PCA_CHK_IN").alias("PCA_CHK_IN"),
        col("lnk_ClmChk_Lkp.TRGT_CD").alias("CLM_CD_TRGT_CD_1"),
        col("lnk_ClmChk_Lkp.TRGT_CD_NM").alias("CLM_CD_TRGT_CD_NM_1"),
        col("lnk_ClmLnLob_Lkp.CD_MPPNG_SK").alias("CD_LKP_CD_MPPNG_SK_1"),
        col("lnk_ClmLnLob_Lkp.TRGT_CD").alias("CD_LKP_TRGT_CD"),
        col("lnk_ClmLnLob_Lkp.TRGT_CD_NM").alias("CD_LKP_TRGT_CD_NM")
    )
)

# ------------------------------------------
# Stage: Xfm_ClmLnPca (CTransformerStage)
# ------------------------------------------
df_Xfm_ClmLnPca = (
    df_lkp_Codes
    .withColumn("DSALW_EXCD_SK", lit(1))
    .withColumn(
        "CLM_LN_PCA_LOB_CD_SK",
        when(
            (col("PCA_CHK_IN") == lit("Y")) & (col("CLM_CHK_SK").isNotNull()),
            col("CLM_CHK_LOB_CD_SK")
        ).otherwise(col("CD_LKP_CD_MPPNG_SK_1"))
    )
    .withColumn("CNSD_AMT", col("CHRG_AMT"))
    .withColumn("DSALW_AMT", lit(0))
    .withColumn("NONCNSD_AMT", lit(0))
    .withColumn("PROV_PD_AMT", lit(0))
    .withColumn("SUB_PD_AMT", col("SUB_PD_AMT"))
    .withColumn("PD_AMT", col("PAYBL_AMT"))
    .withColumn(
        "CLM_LN_PCA_LOB_CD",
        when(
            (col("PCA_CHK_IN") == lit("Y")) & (col("CLM_CHK_SK").isNotNull()),
            col("CLM_CD_TRGT_CD_1")
        ).otherwise(col("CD_LKP_TRGT_CD"))
    )
    .withColumn(
        "CLM_LN_PCA_LOB_NM",
        when(
            (col("PCA_CHK_IN") == lit("Y")) & (col("CLM_CHK_SK").isNotNull()),
            col("CLM_CD_TRGT_CD_NM_1")
        ).otherwise(col("CD_LKP_TRGT_CD_NM"))
    )
    .withColumn("CLM_LN_PCA_PRCS_CD", col("CLM_LN_PCA_PRCS_CD"))
    .withColumn("CLM_LN_PCA_PRCS_NM", col("CLM_LN_PCA_PRCS_NM"))
    .withColumn("EXCD_ID", lit("NA"))
    .select(
        col("CLM_LN_SK"),
        col("SRC_SYS_CD"),
        col("CLM_ID"),
        col("CLM_LN_SEQ_NO"),
        col("CRT_RUN_CYC_EXCTN_SK"),
        col("CLM_SK"),
        col("DSALW_EXCD_SK"),
        col("CLM_LN_PCA_LOB_CD_SK"),
        col("CLM_LN_PCA_PRCS_CD_SK"),
        col("CNSD_AMT"),
        col("DSALW_AMT"),
        col("NONCNSD_AMT"),
        col("PROV_PD_AMT"),
        col("SUB_PD_AMT"),
        col("PD_AMT"),
        col("CLM_LN_PCA_LOB_CD"),
        col("CLM_LN_PCA_LOB_NM"),
        col("CLM_LN_PCA_PRCS_CD"),
        col("CLM_LN_PCA_PRCS_NM"),
        col("EXCD_ID")
    )
)

# ------------------------------------------
# Stage: Fnl (PxFunnel)
# ------------------------------------------
# We funnel df_db2_CLM_LN_PCA_in and df_Xfm_ClmLnPca together based on the same column order.
# df_db2_CLM_LN_PCA_in must be selected in the same 20-column order as funnel output.
df_fnl_in1 = df_db2_CLM_LN_PCA_in.select(
    col("CLM_LN_SK"),
    col("SRC_SYS_CD"),
    col("CLM_ID"),
    col("CLM_LN_SEQ_NO"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("CLM_SK"),
    col("DSALW_EXCD_SK"),
    col("CLM_LN_PCA_LOB_CD_SK"),
    col("CLM_LN_PCA_PRCS_CD_SK"),
    col("CNSD_AMT"),
    col("DSALW_AMT"),
    col("NONCNSD_AMT"),
    col("PROV_PD_AMT"),
    col("SUB_PD_AMT"),
    col("PD_AMT"),
    col("CLM_LN_PCA_LOB_CD"),
    col("CLM_LN_PCA_LOB_NM"),
    col("CLM_LN_PCA_PRCS_CD"),
    col("CLM_LN_PCA_PRCS_NM"),
    col("EXCD_ID")
)

df_fnl_in2 = df_Xfm_ClmLnPca.select(
    col("CLM_LN_SK"),
    col("SRC_SYS_CD"),
    col("CLM_ID"),
    col("CLM_LN_SEQ_NO"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("CLM_SK"),
    col("DSALW_EXCD_SK"),
    col("CLM_LN_PCA_LOB_CD_SK"),
    col("CLM_LN_PCA_PRCS_CD_SK"),
    col("CNSD_AMT"),
    col("DSALW_AMT"),
    col("NONCNSD_AMT"),
    col("PROV_PD_AMT"),
    col("SUB_PD_AMT"),
    col("PD_AMT"),
    col("CLM_LN_PCA_LOB_CD"),
    col("CLM_LN_PCA_LOB_NM"),
    col("CLM_LN_PCA_PRCS_CD"),
    col("CLM_LN_PCA_PRCS_NM"),
    col("EXCD_ID")
)

df_fnl = df_fnl_in1.unionByName(df_fnl_in2)

# ------------------------------------------
# Stage: xfm_BusinessLogic
#   Creates 3 outputs:
#   lnk_MainData_in => condition CLM_LN_SK != 0 and != 1
#   lnk_NA_out => single row with "NA" or "1" => per the DataStage constraint
#   lnk_UNK_out => single row with "UNK" or "0" => per the DataStage constraint
# ------------------------------------------
df_businessLogic_main = (
    df_fnl
    .filter((col("CLM_LN_SK") != 0) & (col("CLM_LN_SK") != 1))
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", lit(CurrRunCycleDate))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", lit(CurrRunCycleDate))
    .withColumn("DSALW_EXCD_ID", col("EXCD_ID"))
    .withColumn("CLM_LN_PCA_CNSD_AMT", col("CNSD_AMT"))
    .withColumn("CLM_LN_PCA_DSALW_AMT", col("DSALW_AMT"))
    .withColumn("CLM_LN_PCA_NONCNSD_AMT", col("NONCNSD_AMT"))
    .withColumn("CLM_LN_PCA_PROV_PD_AMT", col("PROV_PD_AMT"))
    .withColumn("CLM_LN_PCA_SUB_PD_AMT", col("SUB_PD_AMT"))
    .withColumn("CLM_LN_PCA_PD_AMT", col("PD_AMT"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", lit(CurrRunCycle))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(CurrRunCycle))
    .select(
        col("CLM_LN_SK"),
        col("SRC_SYS_CD"),
        col("CLM_ID"),
        col("CLM_LN_SEQ_NO"),
        col("CRT_RUN_CYC_EXCTN_DT_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        col("CLM_SK"),
        col("DSALW_EXCD_SK"),
        col("DSALW_EXCD_ID"),
        col("CLM_LN_PCA_LOB_CD"),
        col("CLM_LN_PCA_LOB_NM"),
        col("CLM_LN_PCA_PRCS_CD"),
        col("CLM_LN_PCA_PRCS_NM"),
        col("CLM_LN_PCA_CNSD_AMT"),
        col("CLM_LN_PCA_DSALW_AMT"),
        col("CLM_LN_PCA_NONCNSD_AMT"),
        col("CLM_LN_PCA_PROV_PD_AMT"),
        col("CLM_LN_PCA_SUB_PD_AMT"),
        col("CLM_LN_PCA_PD_AMT"),
        col("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("CLM_LN_PCA_LOB_CD_SK"),
        col("CLM_LN_PCA_PRCS_CD_SK"),
    )
)

df_businessLogic_na = spark.createDataFrame(
    [
        (
            1,          # CLM_LN_SK
            "NA",       # SRC_SYS_CD
            "NA",       # CLM_ID
            0,          # CLM_LN_SEQ_NO
            "1753-01-01",  # CRT_RUN_CYC_EXCTN_DT_SK (char 10)
            "1753-01-01",  # LAST_UPDT_RUN_CYC_EXCTN_DT_SK (char 10)
            1,          # CLM_SK
            1,          # DSALW_EXCD_SK
            "NA",       # DSALW_EXCD_ID
            "NA",       # CLM_LN_PCA_LOB_CD
            "NA",       # CLM_LN_PCA_LOB_NM
            "NA",       # CLM_LN_PCA_PRCS_CD
            "NA",       # CLM_LN_PCA_PRCS_NM
            0,          # CLM_LN_PCA_CNSD_AMT
            0,          # CLM_LN_PCA_DSALW_AMT
            0,          # CLM_LN_PCA_NONCNSD_AMT
            0,          # CLM_LN_PCA_PROV_PD_AMT
            0,          # CLM_LN_PCA_SUB_PD_AMT
            0,          # CLM_LN_PCA_PD_AMT
            100,        # CRT_RUN_CYC_EXCTN_SK
            100,        # LAST_UPDT_RUN_CYC_EXCTN_SK
            1,          # CLM_LN_PCA_LOB_CD_SK
            1,          # CLM_LN_PCA_PRCS_CD_SK
        )
    ],
    [
        "CLM_LN_SK",
        "SRC_SYS_CD",
        "CLM_ID",
        "CLM_LN_SEQ_NO",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "CLM_SK",
        "DSALW_EXCD_SK",
        "DSALW_EXCD_ID",
        "CLM_LN_PCA_LOB_CD",
        "CLM_LN_PCA_LOB_NM",
        "CLM_LN_PCA_PRCS_CD",
        "CLM_LN_PCA_PRCS_NM",
        "CLM_LN_PCA_CNSD_AMT",
        "CLM_LN_PCA_DSALW_AMT",
        "CLM_LN_PCA_NONCNSD_AMT",
        "CLM_LN_PCA_PROV_PD_AMT",
        "CLM_LN_PCA_SUB_PD_AMT",
        "CLM_LN_PCA_PD_AMT",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CLM_LN_PCA_LOB_CD_SK",
        "CLM_LN_PCA_PRCS_CD_SK"
    ]
)

df_businessLogic_unk = spark.createDataFrame(
    [
        (
            0,           # CLM_LN_SK
            "UNK",       # SRC_SYS_CD
            "UNK",       # CLM_ID
            0,           # CLM_LN_SEQ_NO
            "1753-01-01",  # CRT_RUN_CYC_EXCTN_DT_SK (char 10)
            "1753-01-01",  # LAST_UPDT_RUN_CYC_EXCTN_DT_SK (char 10)
            0,           # CLM_SK
            0,           # DSALW_EXCD_SK
            "UNK",       # DSALW_EXCD_ID
            "UNK",       # CLM_LN_PCA_LOB_CD
            "UNK",       # CLM_LN_PCA_LOB_NM
            "UNK",       # CLM_LN_PCA_PRCS_CD
            "UNK",       # CLM_LN_PCA_PRCS_NM
            0,           # CLM_LN_PCA_CNSD_AMT
            0,           # CLM_LN_PCA_DSALW_AMT
            0,           # CLM_LN_PCA_NONCNSD_AMT
            0,           # CLM_LN_PCA_PROV_PD_AMT
            0,           # CLM_LN_PCA_SUB_PD_AMT
            0,           # CLM_LN_PCA_PD_AMT
            100,         # CRT_RUN_CYC_EXCTN_SK
            100,         # LAST_UPDT_RUN_CYC_EXCTN_SK
            0,           # CLM_LN_PCA_LOB_CD_SK
            0            # CLM_LN_PCA_PRCS_CD_SK
        )
    ],
    [
        "CLM_LN_SK",
        "SRC_SYS_CD",
        "CLM_ID",
        "CLM_LN_SEQ_NO",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "CLM_SK",
        "DSALW_EXCD_SK",
        "DSALW_EXCD_ID",
        "CLM_LN_PCA_LOB_CD",
        "CLM_LN_PCA_LOB_NM",
        "CLM_LN_PCA_PRCS_CD",
        "CLM_LN_PCA_PRCS_NM",
        "CLM_LN_PCA_CNSD_AMT",
        "CLM_LN_PCA_DSALW_AMT",
        "CLM_LN_PCA_NONCNSD_AMT",
        "CLM_LN_PCA_PROV_PD_AMT",
        "CLM_LN_PCA_SUB_PD_AMT",
        "CLM_LN_PCA_PD_AMT",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CLM_LN_PCA_LOB_CD_SK",
        "CLM_LN_PCA_PRCS_CD_SK"
    ]
)

# ------------------------------------------
# Stage: Fnl_Cocc_F (PxFunnel)
#   Input: lnk_UNK_out, lnk_NA_out, lnk_MainData_in
# ------------------------------------------
df_fnl_cocc_f = df_businessLogic_unk.unionByName(
    df_businessLogic_na
).unionByName(
    df_businessLogic_main
)

# ------------------------------------------
# Final columns for the output of Fnl_Cocc_F => lnk_IdsEdwPcaClmLnFExtrOutAbc
# We apply rpad on columns that are char(n): 
#   CRT_RUN_CYC_EXCTN_DT_SK (char(10)), LAST_UPDT_RUN_CYC_EXCTN_DT_SK (char(10))
# ------------------------------------------

df_final = (
    df_fnl_cocc_f
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .select(
        "CLM_LN_SK",
        "SRC_SYS_CD",
        "CLM_ID",
        "CLM_LN_SEQ_NO",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "CLM_SK",
        "DSALW_EXCD_SK",
        "DSALW_EXCD_ID",
        "CLM_LN_PCA_LOB_CD",
        "CLM_LN_PCA_LOB_NM",
        "CLM_LN_PCA_PRCS_CD",
        "CLM_LN_PCA_PRCS_NM",
        "CLM_LN_PCA_CNSD_AMT",
        "CLM_LN_PCA_DSALW_AMT",
        "CLM_LN_PCA_NONCNSD_AMT",
        "CLM_LN_PCA_PROV_PD_AMT",
        "CLM_LN_PCA_SUB_PD_AMT",
        "CLM_LN_PCA_PD_AMT",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CLM_LN_PCA_LOB_CD_SK",
        "CLM_LN_PCA_PRCS_CD_SK"
    )
)

# ------------------------------------------
# Stage: seq_PCA_CLM_LN_F_Load (PxSequentialFile)
#   Write a delimited file with no header, mode=overwrite, delimiter=",", quote="^"
# ------------------------------------------
write_files(
    df_final,
    f"{adls_path}/load/PCA_CLM_LN_F.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)