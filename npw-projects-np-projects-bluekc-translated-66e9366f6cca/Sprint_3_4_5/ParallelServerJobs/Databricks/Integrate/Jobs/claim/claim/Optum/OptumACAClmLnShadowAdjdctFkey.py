# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Called By : OptumACAClmLnShadowAdjctCntl
# MAGIC                                                                                                                                                                                                                                                                                             DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                                   DESCRIPTION                                                                                                                                                    ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# MAGIC Sri Nannapaneni        08-19-2020        shadow adjuducation                  This process runs to consume a new file (EX-9 Supplemental file) and 
# MAGIC                                                                                                       Perform Foreign Key / code mapping Lookups to populate CLM_LN_SHADOW_ADJDCT File                                      IntegrateDev2            Reddy Sanam             2020-12-16


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
InFile = get_widget_value('InFile','')
ExclusionList = get_widget_value('ExclusionList','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

extract_query = (
    f"SELECT DISTINCT CD_MPPNG_SK, SRC_CD "
    f"FROM {IDSOwner}.CD_MPPNG "
    f"WHERE SRC_CD = '{SrcSysCd}' "
    f"AND SRC_SYS_CD = 'IDS' "
    f"AND SRC_DOMAIN_NM = 'SOURCE SYSTEM' "
    f"AND SRC_CLCTN_CD = 'IDS' "
    f"AND TRGT_DOMAIN_NM = 'SOURCE SYSTEM' "
    f"AND TRGT_CLCTN_CD = 'IDS'"
)
df_db2_CD_MPPNG_SK = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query)
    .load()
)

extract_query = (
    f"Select CLM_LN_SK, CLM_ID "
    f"from {IDSOwner}.CLM_LN "
    f"WHERE SRC_SYS_CD_SK IN (Select DISTINCT SRC_CD_SK from {IDSOwner}.CD_MPPNG WHERE SRC_SYS_CD = '{SrcSysCd}')"
)
df_db2_ClmLn = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query)
    .load()
)

col_details_SeqFile_CLM_ACA_PDX_SUPLMT = {
    "PDX_CLM_NO": {"len": 15, "type": StringType()},
    "CLM_SEQ_NO": {"len": 3, "type": StringType()},
    "CLAIMSTS": {"len": 1, "type": StringType()},
    "EX9_RCRD_SEQ_NO": {"len": 5, "type": StringType()},
    "PD7_REPRCS_SEQ_NO": {"len": 3, "type": StringType()},
    "EX8_DT_SUBMT": {"len": 8, "type": StringType()},
    "EX8_TM_SUBMT": {"len": 6, "type": StringType()},
    "TCD_CAR_ID": {"len": 9, "type": StringType()},
    "TCD_ACCT_ID": {"len": 15, "type": StringType()},
    "TCD_GRP_ID": {"len": 15, "type": StringType()},
    "POV_GRP_PLN_CD": {"len": 10, "type": StringType()},
    "POV_GRP_PLN_EFF_DT": {"len": 7, "type": StringType()},
    "TCD_MBR_ID": {"len": 20, "type": StringType()},
    "TCD_SBM_SVC_DT": {"len": 8, "type": StringType()},
    "EX8_HIM_QHP_ID": {"len": 14, "type": StringType()},
    "HIM_ISSUER_ID": {"len": 5, "type": StringType()},
    "HIM_ST_ID": {"len": 2, "type": StringType()},
    "HIM_PROD_NO": {"len": 3, "type": StringType()},
    "HIM_STD_CMPNT": {"len": 4, "type": StringType()},
    "HIM_SEQ_NO": {"len": 5, "type": StringType()},
    "HIM_FROM_DT": {"len": 7, "type": StringType()},
    "HIM_THRU_DT": {"len": 7, "type": StringType()},
    "HIM_STTUS": {"len": 1, "type": StringType()},
    "HIM_CSR_LVL": {"len": 2, "type": StringType()},
    "HIM_PLN_METAL_IN": {"len": 1, "type": StringType()},
    "HIM_APTC_IN": {"len": 1, "type": StringType()},
    "HIM_ETHNITICITY": {"len": 2, "type": StringType()},
    "TC2_HIM_GRC_PERD_IN": {"len": 2, "type": StringType()},
    "HIM_GRC_PERD_STRT_DT": {"len": 7, "type": StringType()},
    "HIM_GRC_PERD_END_DT": {"len": 7, "type": StringType()},
    "GPH_PRBTN_PERD_DAYS": {"len": 3, "type": StringType()},
    "GPH_AFTR_PRBTN_PERD": {"len": 1, "type": StringType()},
    "GHP_MSG_CD": {"len": 10, "type": StringType()},
    "GHP_MSG_TYP": {"len": 1, "type": StringType()},
    "MBR_ALCO_CD": {"len": 1, "type": StringType()},
    "MBR_SMKNG_CD": {"len": 1, "type": StringType()},
    "MBR_PRGNCY_CD": {"len": 1, "type": StringType()},
    "EX8_VOID_IN": {"len": 1, "type": StringType()},
    "HIM_SBSDY_AMT": {"len": 11, "type": StringType()},
    "TCD_RBL_INGR_CST": {"len": 11, "type": StringType()},
    "TCD_RBL_DISPNS_FEE": {"len": 11, "type": StringType()},
    "PDT_RBL_PCT_SLS_TAX_PD": {"len": 11, "type": StringType()},
    "TCD_RBL_FLAT_SLS_TAX_AMT": {"len": 11, "type": StringType()},
    "TCD_RBL_PATN_PAY_AMT": {"len": 11, "type": StringType()},
    "TCD_RBL_TOT_AMT": {"len": 11, "type": StringType()},
    "TCD_APP_INGR_CST": {"len": 11, "type": StringType()},
    "TCD_APP_DISPNS_FEE": {"len": 11, "type": StringType()},
    "TCD_APP_PCT_SLS_TAX_PD": {"len": 11, "type": StringType()},
    "TCD_APP_FLAT_SLS_TAX_AMT": {"len": 11, "type": StringType()},
    "TCD_APP_PATN_PAY_AMT": {"len": 11, "type": StringType()},
    "TCD_APP_TOT_AMT": {"len": 11, "type": StringType()},
    "TCD_PROD_KEY": {"len": 9, "type": StringType()},
    "TCD_SBM_PROD_ID": {"len": 20, "type": StringType()},
    "TCD_GPI_NO": {"len": 14, "type": StringType()},
    "TCD_SBM_QTY_DISPNS": {"len": 11, "type": StringType()},
    "TCD_SBM_DAYS_SUPL": {"len": 3, "type": StringType()},
    "TCD_SBM_PDX_NO": {"len": 12, "type": StringType()},
    "TCD_SBM_FILL_NO": {"len": 2, "type": StringType()},
    "TAP_SBM_DISPNS_STTUS": {"len": 1, "type": StringType()},
    "TAP_SBM_CMPND_CD": {"len": 1, "type": StringType()},
    "TCD_COB_CLM_IN": {"len": 2, "type": StringType()},
    "TCD_COB_PRICE_TYP": {"len": 2, "type": StringType()},
    "TC3_CLM_ORIG_FLAG": {"len": 1, "type": StringType()},
    "TCD_GNRC_IN": {"len": 1, "type": StringType()},
    "TCD_PDX_NTWK_ID": {"len": 6, "type": StringType()},
    "TCD_SBM_SVC_PROV_ID_QLFR": {"len": 2, "type": StringType()},
    "TCD_SBM_SVC_PROV_ID": {"len": 15, "type": StringType()},
    "TCD_SBM_PRSCRBR_ID_QLFR": {"len": 2, "type": StringType()},
    "TCD_SBM_PRSCRBR_ID": {"len": 15, "type": StringType()},
    "TCD_SBM_OTHR_COV_CD": {"len": 2, "type": StringType()},
    "TCD_RBL_OTHR_PAYMT_AMT_RECOG": {"len": 11, "type": StringType()},
    "PDT_FINL_PLN_CD": {"len": 10, "type": StringType()},
    "TCD_FINL_PLN_EFF_DT": {"len": 7, "type": StringType()},
    "TCD_DT_ORIG_CLM_RCVD": {"len": 8, "type": StringType()},
    "EX8_PRAUTH_RSN_CD": {"len": 2, "type": StringType()},
    "TCD_MBR_PRAUTH_NO": {"len": 11, "type": StringType()},
    "TCD_PATN_RESDNC": {"len": 2, "type": StringType()},
    "TC3_SBM_POS": {"len": 2, "type": StringType()},
    "TC3_CONT_THER_FLAG": {"len": 1, "type": StringType()},
    "TC3_CONT_THER_SCHED_ID": {"len": 20, "type": StringType()},
    "TC3_APP_AMT_APLD_PER_DEDCT": {"len": 11, "type": StringType()},
    "PDH_APP_AMT_ATRBD_PRCSR_FEE": {"len": 11, "type": StringType()},
    "PDH_APP_AMT_ATRBD_SLS_TAX": {"len": 11, "type": StringType()},
    "PDH_APP_ATRBD_PROD_SEL_AMT": {"len": 11, "type": StringType()},
    "PDH_APP_COPAY_AMT": {"len": 11, "type": StringType()},
    "PDH_APP_COPAY_FLAT_AMT": {"len": 11, "type": StringType()},
    "PDH_APP_COPAY_PCT_AMT": {"len": 11, "type": StringType()},
    "PDH_APP_DISPNS_FEE": {"len": 11, "type": StringType()},
    "PDH_APP_DUE_AMT": {"len": 11, "type": StringType()},
    "PDH_APP_EXCEEDING_PER_BNF_AMT": {"len": 11, "type": StringType()},
    "PDH_APP_FLAT_SLS_TAX_AMT": {"len": 11, "type": StringType()},
    "PDH_APP_PCT_SLS_TAX_AMT_PD": {"len": 11, "type": StringType()},
    "PDH_APP_INCNTV_AMT": {"len": 11, "type": StringType()},
    "PDH_APP_INGR_CST": {"len": 11, "type": StringType()},
    "PDH_APP_OTHR_PAYER_AMT_RECOG": {"len": 11, "type": StringType()},
    "PDH_APP_PATN_PAY_AMT": {"len": 11, "type": StringType()},
    "PDH_APP_TOT_OTHR_AMT": {"len": 11, "type": StringType()},
    "PDH_APP_WTHLD_AMT": {"len": 11, "type": StringType()},
    "PDH_APP_AMT_ATRBD_PROV_NTWK_SEL": {"len": 11, "type": StringType()},
    "PDH_APP_HLTH_PLN_FUND_ASSTANCE_AMT": {"len": 11, "type": StringType()},
    "PDH_CAL_PCT_SLS_TAX_AMT_PD": {"len": 11, "type": StringType()},
    "PDH_CAL_AMT_APLD_PER_DEDCT": {"len": 11, "type": StringType()},
    "PDH_CAL_AMT_ATRBD_PRCSR_FEE": {"len": 11, "type": StringType()},
    "PDH_CAL_AMT_ATRBD_SLS_TAX": {"len": 11, "type": StringType()},
    "PDH_CAL_ATRBD_PROD_SEL_AMT": {"len": 11, "type": StringType()},
    "PDH_CAL_COPAY_AMT": {"len": 11, "type": StringType()},
    "PDH_CAL_COPAY_FLAT_AMT": {"len": 11, "type": StringType()},
    "PDH_CAL_COPAY_PCT_AMT": {"len": 11, "type": StringType()},
    "PDH_CAL_DISPNS_FEE": {"len": 11, "type": StringType()},
    "PDH_CAL_EXCEEDING_PER_BNF_AMT": {"len": 11, "type": StringType()},
    "PDH_CAL_FLAT_SLSTAX_AMT": {"len": 11, "type": StringType()},
    "PDH_CAL_INCNTV_FEE_AMT": {"len": 11, "type": StringType()},
    "PDH_CAL_INGR_CST": {"len": 11, "type": StringType()},
    "PDH_CAL_OTHR_PAYER_AMT_RECOG": {"len": 11, "type": StringType()},
    "PDH_CAL_PATN_PAY_AMT": {"len": 11, "type": StringType()},
    "PDH_CAL_TOT_AMT": {"len": 11, "type": StringType()},
    "PDH_CAL_TOT_OTHR_AMT": {"len": 11, "type": StringType()},
    "PDH_CAL_WTHLD_AMT": {"len": 11, "type": StringType()},
    "PDH_CAL_AMT_ATRBD_PROV_NTWK_SEL": {"len": 11, "type": StringType()},
    "PDH_CAL_HLTH_PLN_FUND_ASSTNC_AMT": {"len": 11, "type": StringType()},
    "PDH_RBL_PCT_SLS_TAX_AMT_PD": {"len": 11, "type": StringType()},
    "PDH_RBL_AMT_APLD_PER_DEDCT": {"len": 11, "type": StringType()},
    "PDH_RBL_AMT_ATRBD_PRCSR_FEE": {"len": 11, "type": StringType()},
    "PDH_RBL_AMT_ATRBD_SLS_TAX": {"len": 11, "type": StringType()},
    "PDH_RBL_ATRBD_PROD_SEL_AMT": {"len": 11, "type": StringType()},
    "PDH_RBL_COPAY_AMT": {"len": 11, "type": StringType()},
    "PDH_RBL_COPAY_FLAT_AMT": {"len": 11, "type": StringType()},
    "PDH_RBL_COPAY_PCT_AMT": {"len": 11, "type": StringType()},
    "PDH_RBL_DISPNS_FEE": {"len": 11, "type": StringType()},
    "PDH_RBL_DUE_AMT": {"len": 11, "type": StringType()},
    "PDH_RBL_EXCEEDING_PER_BNF_AMT": {"len": 11, "type": StringType()},
    "PDH_RBL_FLAT_SLS_TAX_AMT": {"len": 11, "type": StringType()},
    "PDH_RBL_INCNTV_AMT": {"len": 11, "type": StringType()},
    "PDH_RBL_INGR_CST": {"len": 11, "type": StringType()},
    "PDH_RBL_OTHR_PAYOR_AMT_RECOG": {"len": 11, "type": StringType()},
    "PDH_RBL_PATN_PAY_AMT": {"len": 11, "type": StringType()},
    "PDH_RBL_TOT_OTHR_AMT": {"len": 11, "type": StringType()},
    "PDH_RBL_WTHLD_AMT": {"len": 11, "type": StringType()},
    "PDH_RBL_AMT_ATRBD_PROV_NTWK_SEL": {"len": 11, "type": StringType()},
    "PDH_RBL_HLTH_PLN_FUND_ASSTNC_AMT": {"len": 11, "type": StringType()},
    "ADD_USER_NM": {"len": 10, "type": StringType()},
    "ADD_DT": {"len": 7, "type": StringType()},
    "ADD_TM": {"len": 6, "type": StringType()},
    "ADD_PGM_NM": {"len": 10, "type": StringType()},
    "REPRCS_DT": {"len": 8, "type": StringType()},
    "PD7_REPRCS_TM": {"len": 6, "type": StringType()},
    "IN_NTWK_IN": {"len": 1, "type": StringType()},
    "PROD_ID_QLFR": {"len": 2, "type": StringType()},
    "RXC_UNUSED_COUPON_AMT": {"len": 11, "type": StringType()},
    "FLR": {"len": 127, "type": StringType()}
}

df_seqFile_CLM_ACA_PDX_SUPLMT = fixed_file_read_write(
    f"{adls_path_raw}/landing/{InFile}",
    col_details_SeqFile_CLM_ACA_PDX_SUPLMT,
    "read"
)

df_seqFile_CLM_ACA_PDX_SUPLMT_vars = (
    df_seqFile_CLM_ACA_PDX_SUPLMT
    .withColumn("SVClmInd", F.when(trim(F.col("CLAIMSTS")) == 'X', 'R').otherwise(''))
    .withColumn("SVCoinsAmt", F.col("PDH_RBL_COPAY_PCT_AMT"))
    .withColumn("SVCopayAmt", F.col("PDH_RBL_COPAY_FLAT_AMT"))
    .withColumn("SVDectAmt", F.col("PDH_RBL_AMT_APLD_PER_DEDCT"))
    .withColumn("SVPayblAmt", F.col("PDH_RBL_DUE_AMT"))
    .withColumn("SVCsrSbsdyAmt", F.col("HIM_SBSDY_AMT"))
)

df_xfm_CLM_ACA_PDX_SUPLMT = df_seqFile_CLM_ACA_PDX_SUPLMT_vars.select(
    F.concat(trim(F.col("PDX_CLM_NO")), trim(F.col("CLM_SEQ_NO")), F.col("SVClmInd")).alias("CLM_ID"),
    F.lit(1).alias("CLM_LN_SEQ_NO"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.lit(IDSRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(IDSRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("CLM_LN_SHADOW_ADJDCT_DSALW_EXCD_SK"),
    F.lit(1).alias("CLM_LN_SHADOW_ADJDCT_STTUS_CD_SK"),
    F.lit("N").alias("SHADOW_MED_UTIL_EDIT_IN"),
    F.lit("0.00").alias("CNSD_CHRG_AMT"),
    F.lit("0.00").alias("INIT_ADJDCT_ALW_AMT"),
    F.lit("0.00").alias("SHADOW_ADJDCT_ALW_AMT"),
    F.when(
        F.col("SVCoinsAmt").isNull() | (F.length(trim(F.col("SVCoinsAmt"))) == 0),
        F.lit("0.00")
    ).otherwise(F.col("SVCoinsAmt")).alias("SHADOW_ADJDCT_COINS_AMT"),
    F.when(
        F.col("SVCopayAmt").isNull() | (F.length(trim(F.col("SVCopayAmt"))) == 0),
        F.lit("0.00")
    ).otherwise(F.col("SVCopayAmt")).alias("SHADOW_ADJDCT_COPAY_AMT"),
    F.when(
        F.col("SVDectAmt").isNull() | (F.length(trim(F.col("SVDectAmt"))) == 0),
        F.lit("0.00")
    ).otherwise(F.col("SVDectAmt")).alias("SHADOW_ADJDCT_DEDCT_AMT"),
    F.lit("0.00").alias("SHADOW_ADJDCT_DSALW_AMT"),
    F.when(
        F.col("SVPayblAmt").isNull() | (F.length(trim(F.col("SVPayblAmt"))) == 0),
        F.lit("0.00")
    ).otherwise(F.col("SVPayblAmt")).alias("SHADOW_ADJDCT_PAYBL_AMT"),
    F.lit(0).alias("INIT_ADJDCT_ALW_PRICE_UNIT_CT"),
    F.lit(0).alias("SHADOW_ADJDCT_ALW_PRICE_UNIT_CT"),
    F.lit("NA").alias("SHADOW_DEDCT_AMT_ACCUM_ID"),
    F.lit("NA").alias("SHADOW_LMT_PFX_ID"),
    F.lit("NA").alias("SHADOW_PROD_CMPNT_DEDCT_PFX_ID"),
    F.lit("NA").alias("SHADOW_PROD_CMPNT_SVC_PAYMT_ID"),
    F.lit("NULL").alias("SHADOW_SVC_RULE_TYP_TX"),
    (
        F.when(
            F.col("SVCsrSbsdyAmt").isNull() | (F.length(trim(F.col("SVCsrSbsdyAmt"))) == 0),
            F.lit("0.00")
        )
        .otherwise(
            F.format_number(F.col("SVCsrSbsdyAmt").cast("double") * -1, 2)
        )
    ).alias("PBM_CALC_CSR_SBSDY_AMT")
)

df_lkp_codes_and_ids = df_xfm_CLM_ACA_PDX_SUPLMT.alias("Lnk_Xfm_logic") \
    .join(
        df_db2_ClmLn.alias("Lnk_db2_ClmLn"),
        F.col("Lnk_Xfm_logic.CLM_ID") == F.col("Lnk_db2_ClmLn.CLM_ID"),
        "left"
    ) \
    .join(
        df_db2_CD_MPPNG_SK.alias("lnk_Codes"),
        F.col("Lnk_Xfm_logic.SRC_SYS_CD") == F.col("lnk_Codes.SRC_CD"),
        "left"
    )

df_lkp_main = df_lkp_codes_and_ids.filter(
    F.col("Lnk_db2_ClmLn.CLM_LN_SK").isNotNull() & F.col("lnk_Codes.CD_MPPNG_SK").isNotNull()
)

df_lkp_reject = df_lkp_codes_and_ids.filter(
    F.col("Lnk_db2_ClmLn.CLM_LN_SK").isNull() | F.col("lnk_Codes.CD_MPPNG_SK").isNull()
)

df_LnkDrugClmPrice_Out = df_lkp_main.select(
    F.col("Lnk_db2_ClmLn.CLM_LN_SK").alias("CLM_LN_SK"),
    F.col("Lnk_Xfm_logic.CLM_ID").alias("CLM_ID"),
    F.col("Lnk_Xfm_logic.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("lnk_Codes.CD_MPPNG_SK").alias("SRC_SYS_CD_SK"),
    F.col("Lnk_Xfm_logic.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Lnk_Xfm_logic.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Lnk_Xfm_logic.CLM_LN_SHADOW_ADJDCT_DSALW_EXCD_SK").alias("CLM_LN_SHADOW_ADJDCT_DSALW_EXCD_SK"),
    F.col("Lnk_Xfm_logic.CLM_LN_SHADOW_ADJDCT_STTUS_CD_SK").alias("CLM_LN_SHADOW_ADJDCT_STTUS_CD_SK"),
    F.col("Lnk_Xfm_logic.SHADOW_MED_UTIL_EDIT_IN").alias("SHADOW_MED_UTIL_EDIT_IN"),
    F.col("Lnk_Xfm_logic.CNSD_CHRG_AMT").alias("CNSD_CHRG_AMT"),
    F.col("Lnk_Xfm_logic.INIT_ADJDCT_ALW_AMT").alias("INIT_ADJDCT_ALW_AMT"),
    F.col("Lnk_Xfm_logic.SHADOW_ADJDCT_ALW_AMT").alias("SHADOW_ADJDCT_ALW_AMT"),
    F.col("Lnk_Xfm_logic.SHADOW_ADJDCT_COINS_AMT").alias("SHADOW_ADJDCT_COINS_AMT"),
    F.col("Lnk_Xfm_logic.SHADOW_ADJDCT_COPAY_AMT").alias("SHADOW_ADJDCT_COPAY_AMT"),
    F.col("Lnk_Xfm_logic.SHADOW_ADJDCT_DEDCT_AMT").alias("SHADOW_ADJDCT_DEDCT_AMT"),
    F.col("Lnk_Xfm_logic.SHADOW_ADJDCT_DSALW_AMT").alias("SHADOW_ADJDCT_DSALW_AMT"),
    F.col("Lnk_Xfm_logic.SHADOW_ADJDCT_PAYBL_AMT").alias("SHADOW_ADJDCT_PAYBL_AMT"),
    F.col("Lnk_Xfm_logic.INIT_ADJDCT_ALW_PRICE_UNIT_CT").alias("INIT_ADJDCT_ALW_PRICE_UNIT_CT"),
    F.col("Lnk_Xfm_logic.SHADOW_ADJDCT_ALW_PRICE_UNIT_CT").alias("SHADOW_ADJDCT_ALW_PRICE_UNIT_CT"),
    F.col("Lnk_Xfm_logic.SHADOW_DEDCT_AMT_ACCUM_ID").alias("SHADOW_DEDCT_AMT_ACCUM_ID"),
    F.col("Lnk_Xfm_logic.SHADOW_LMT_PFX_ID").alias("SHADOW_LMT_PFX_ID"),
    F.col("Lnk_Xfm_logic.SHADOW_PROD_CMPNT_DEDCT_PFX_ID").alias("SHADOW_PROD_CMPNT_DEDCT_PFX_ID"),
    F.col("Lnk_Xfm_logic.SHADOW_PROD_CMPNT_SVC_PAYMT_ID").alias("SHADOW_PROD_CMPNT_SVC_PAYMT_ID"),
    F.col("Lnk_Xfm_logic.SHADOW_SVC_RULE_TYP_TX").alias("SHADOW_SVC_RULE_TYP_TX"),
    F.col("Lnk_Xfm_logic.PBM_CALC_CSR_SBSDY_AMT").alias("PBM_CALC_CSR_SBSDY_AMT")
)

df_Reject_CLM_LN_SHADOW_ADJDCT = df_lkp_reject.select(
    F.col("Lnk_db2_ClmLn.CLM_LN_SK").alias("CLM_LN_SK"),
    F.col("Lnk_Xfm_logic.CLM_ID").alias("CLM_ID"),
    F.col("Lnk_Xfm_logic.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("lnk_Codes.CD_MPPNG_SK").alias("SRC_SYS_CD_SK"),
    F.col("Lnk_Xfm_logic.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Lnk_Xfm_logic.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Lnk_Xfm_logic.CLM_LN_SHADOW_ADJDCT_DSALW_EXCD_SK").alias("CLM_LN_SHADOW_ADJDCT_DSALW_EXCD_SK"),
    F.col("Lnk_Xfm_logic.CLM_LN_SHADOW_ADJDCT_STTUS_CD_SK").alias("CLM_LN_SHADOW_ADJDCT_STTUS_CD_SK"),
    F.col("Lnk_Xfm_logic.SHADOW_MED_UTIL_EDIT_IN").alias("SHADOW_MED_UTIL_EDIT_IN"),
    F.col("Lnk_Xfm_logic.CNSD_CHRG_AMT").alias("CNSD_CHRG_AMT"),
    F.col("Lnk_Xfm_logic.INIT_ADJDCT_ALW_AMT").alias("INIT_ADJDCT_ALW_AMT"),
    F.col("Lnk_Xfm_logic.SHADOW_ADJDCT_ALW_AMT").alias("SHADOW_ADJDCT_ALW_AMT"),
    F.col("Lnk_Xfm_logic.SHADOW_ADJDCT_COINS_AMT").alias("SHADOW_ADJDCT_COINS_AMT"),
    F.col("Lnk_Xfm_logic.SHADOW_ADJDCT_COPAY_AMT").alias("SHADOW_ADJDCT_COPAY_AMT"),
    F.col("Lnk_Xfm_logic.SHADOW_ADJDCT_DEDCT_AMT").alias("SHADOW_ADJDCT_DEDCT_AMT"),
    F.col("Lnk_Xfm_logic.SHADOW_ADJDCT_DSALW_AMT").alias("SHADOW_ADJDCT_DSALW_AMT"),
    F.col("Lnk_Xfm_logic.SHADOW_ADJDCT_PAYBL_AMT").alias("SHADOW_ADJDCT_PAYBL_AMT"),
    F.col("Lnk_Xfm_logic.INIT_ADJDCT_ALW_PRICE_UNIT_CT").alias("INIT_ADJDCT_ALW_PRICE_UNIT_CT"),
    F.col("Lnk_Xfm_logic.SHADOW_ADJDCT_ALW_PRICE_UNIT_CT").alias("SHADOW_ADJDCT_ALW_PRICE_UNIT_CT"),
    F.col("Lnk_Xfm_logic.SHADOW_DEDCT_AMT_ACCUM_ID").alias("SHADOW_DEDCT_AMT_ACCUM_ID"),
    F.col("Lnk_Xfm_logic.SHADOW_LMT_PFX_ID").alias("SHADOW_LMT_PFX_ID"),
    F.col("Lnk_Xfm_logic.SHADOW_PROD_CMPNT_DEDCT_PFX_ID").alias("SHADOW_PROD_CMPNT_DEDCT_PFX_ID"),
    F.col("Lnk_Xfm_logic.SHADOW_PROD_CMPNT_SVC_PAYMT_ID").alias("SHADOW_PROD_CMPNT_SVC_PAYMT_ID"),
    F.col("Lnk_Xfm_logic.SHADOW_SVC_RULE_TYP_TX").alias("SHADOW_SVC_RULE_TYP_TX"),
    F.col("Lnk_Xfm_logic.PBM_CALC_CSR_SBSDY_AMT").alias("PBM_CALC_CSR_SBSDY_AMT")
)

write_files(
    df_Reject_CLM_LN_SHADOW_ADJDCT,
    f"{adls_path}/load/REJECT_CLM_LN_SHADOW_ADJDCT_{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote='"',
    nullValue=None
)

df_xfm_filter_clmsk = df_LnkDrugClmPrice_Out.filter(F.length(F.col("CLM_LN_SK")) > 1)

df_xfm_filter_clmsk_rpad = df_xfm_filter_clmsk.withColumn(
    "SHADOW_MED_UTIL_EDIT_IN",
    F.rpad(F.col("SHADOW_MED_UTIL_EDIT_IN"), 1, " ")
)

write_files(
    df_xfm_filter_clmsk_rpad.select(
        "CLM_LN_SK",
        "CLM_ID",
        "CLM_LN_SEQ_NO",
        "SRC_SYS_CD_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CLM_LN_SHADOW_ADJDCT_DSALW_EXCD_SK",
        "CLM_LN_SHADOW_ADJDCT_STTUS_CD_SK",
        "SHADOW_MED_UTIL_EDIT_IN",
        "CNSD_CHRG_AMT",
        "INIT_ADJDCT_ALW_AMT",
        "SHADOW_ADJDCT_ALW_AMT",
        "SHADOW_ADJDCT_COINS_AMT",
        "SHADOW_ADJDCT_COPAY_AMT",
        "SHADOW_ADJDCT_DEDCT_AMT",
        "SHADOW_ADJDCT_DSALW_AMT",
        "SHADOW_ADJDCT_PAYBL_AMT",
        "INIT_ADJDCT_ALW_PRICE_UNIT_CT",
        "SHADOW_ADJDCT_ALW_PRICE_UNIT_CT",
        "SHADOW_DEDCT_AMT_ACCUM_ID",
        "SHADOW_LMT_PFX_ID",
        "SHADOW_PROD_CMPNT_DEDCT_PFX_ID",
        "SHADOW_PROD_CMPNT_SVC_PAYMT_ID",
        "SHADOW_SVC_RULE_TYP_TX",
        "PBM_CALC_CSR_SBSDY_AMT"
    ),
    f"{adls_path}/load/CLM_LN_SHADOW_ADJDCT.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)