# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC Â© Copyright 2020 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY :   OptumACAClmLnShadowAdjctCntl
# MAGIC  
# MAGIC DESCRIPTION:  This process runs to consume the ACA Reprocessing file (EX-9 file).
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                         Date                 Project/Altiris #                                                                               Change Description        Development Project      Code Reviewer          Date Reviewed       
# MAGIC                                                                                                                                                                                                                                                                                      
# MAGIC Velmani Kondappan      10-30-2020          6264 - PBM Phase II - Government Programs                                     IntegrateDev5                                                    Reddy Sanam           12-16-2020

# MAGIC Contraint applied to filter out Claims Status is Paid and Reprocess Sequence Number not equal to 000
# MAGIC Pkey DRUG_CLM_ACA_REPRCS_RSLT_SK Is  Generated uskng K table methodology.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DecimalType
)
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

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query_db_K_DRUG_CLM_ACA_REPRCS_RSLT_read = f"SELECT DRUG_CLM_ACA_REPRCS_RSLT_SK, CLM_ID, CLM_REPRCS_SEQ_NO, SRC_SYS_CD, CRT_RUN_CYC_EXCTN_SK FROM {IDSOwner}.K_DRUG_CLM_ACA_REPRCS_RSLT"
df_db_K_DRUG_CLM_ACA_REPRCS_RSLT_read = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db_K_DRUG_CLM_ACA_REPRCS_RSLT_read)
    .load()
)

schema_SeqFile_CLM_ACA_PDX_SUPLMT = StructType([
    StructField("PDX_CLM_NO", StringType(), False),
    StructField("CLM_SEQ_NO", StringType(), False),
    StructField("CLAIMSTS", StringType(), False),
    StructField("EX9_RCRD_SEQ_NO", StringType(), False),
    StructField("PD7_REPRCS_SEQ_NO", StringType(), False),
    StructField("EX8_DT_SUBMT", StringType(), False),
    StructField("EX8_TM_SUBMT", StringType(), False),
    StructField("TCD_CAR_ID", StringType(), False),
    StructField("TCD_ACCT_ID", StringType(), False),
    StructField("TCD_GRP_ID", StringType(), False),
    StructField("POV_GRP_PLN_CD", StringType(), False),
    StructField("POV_GRP_PLN_EFF_DT", StringType(), False),
    StructField("TCD_MBR_ID", StringType(), False),
    StructField("TCD_SBM_SVC_DT", StringType(), False),
    StructField("EX8_HIM_QHP_ID", StringType(), False),
    StructField("HIM_ISSUER_ID", StringType(), False),
    StructField("HIM_ST_ID", StringType(), False),
    StructField("HIM_PROD_NO", StringType(), False),
    StructField("HIM_STD_CMPNT", StringType(), False),
    StructField("HIM_SEQ_NO", StringType(), False),
    StructField("HIM_FROM_DT", StringType(), False),
    StructField("HIM_THRU_DT", StringType(), False),
    StructField("HIM_STTUS", StringType(), False),
    StructField("HIM_CSR_LVL", StringType(), False),
    StructField("HIM_PLN_METAL_IN", StringType(), False),
    StructField("HIM_APTC_IN", StringType(), False),
    StructField("HIM_ETHNITICITY", StringType(), False),
    StructField("TC2_HIM_GRC_PERD_IN", StringType(), False),
    StructField("HIM_GRC_PERD_STRT_DT", StringType(), False),
    StructField("HIM_GRC_PERD_END_DT", StringType(), False),
    StructField("GPH_PRBTN_PERD_DAYS", StringType(), False),
    StructField("GPH_AFTR_PRBTN_PERD", StringType(), False),
    StructField("GHP_MSG_CD", StringType(), False),
    StructField("GHP_MSG_TYP", StringType(), False),
    StructField("MBR_ALCO_CD", StringType(), False),
    StructField("MBR_SMKNG_CD", StringType(), False),
    StructField("MBR_PRGNCY_CD", StringType(), False),
    StructField("EX8_VOID_IN", StringType(), False),
    StructField("HIM_SBSDY_AMT", StringType(), False),
    StructField("TCD_RBL_INGR_CST", StringType(), False),
    StructField("TCD_RBL_DISPNS_FEE", StringType(), False),
    StructField("PDT_RBL_PCT_SLS_TAX_PD", StringType(), False),
    StructField("TCD_RBL_FLAT_SLS_TAX_AMT", StringType(), False),
    StructField("TCD_RBL_PATN_PAY_AMT", StringType(), False),
    StructField("TCD_RBL_TOT_AMT", StringType(), False),
    StructField("TCD_APP_INGR_CST", StringType(), False),
    StructField("TCD_APP_DISPNS_FEE", StringType(), False),
    StructField("TCD_APP_PCT_SLS_TAX_PD", StringType(), False),
    StructField("TCD_APP_FLAT_SLS_TAX_AMT", StringType(), False),
    StructField("TCD_APP_PATN_PAY_AMT", StringType(), False),
    StructField("TCD_APP_TOT_AMT", StringType(), False),
    StructField("TCD_PROD_KEY", StringType(), False),
    StructField("TCD_SBM_PROD_ID", StringType(), False),
    StructField("TCD_GPI_NO", StringType(), False),
    StructField("TCD_SBM_QTY_DISPNS", StringType(), False),
    StructField("TCD_SBM_DAYS_SUPL", StringType(), False),
    StructField("TCD_SBM_PDX_NO", StringType(), False),
    StructField("TCD_SBM_FILL_NO", StringType(), False),
    StructField("TAP_SBM_DISPNS_STTUS", StringType(), False),
    StructField("TAP_SBM_CMPND_CD", StringType(), False),
    StructField("TCD_COB_CLM_IN", StringType(), False),
    StructField("TCD_COB_PRICE_TYP", StringType(), False),
    StructField("TC3_CLM_ORIG_FLAG", StringType(), False),
    StructField("TCD_GNRC_IN", StringType(), False),
    StructField("TCD_PDX_NTWK_ID", StringType(), False),
    StructField("TCD_SBM_SVC_PROV_ID_QLFR", StringType(), False),
    StructField("TCD_SBM_SVC_PROV_ID", StringType(), False),
    StructField("TCD_SBM_PRSCRBR_ID_QLFR", StringType(), False),
    StructField("TCD_SBM_PRSCRBR_ID", StringType(), False),
    StructField("TCD_SBM_OTHR_COV_CD", StringType(), False),
    StructField("TCD_RBL_OTHR_PAYMT_AMT_RECOG", StringType(), False),
    StructField("PDT_FINL_PLN_CD", StringType(), False),
    StructField("TCD_FINL_PLN_EFF_DT", StringType(), False),
    StructField("TCD_DT_ORIG_CLM_RCVD", StringType(), False),
    StructField("EX8_PRAUTH_RSN_CD", StringType(), False),
    StructField("TCD_MBR_PRAUTH_NO", StringType(), False),
    StructField("TCD_PATN_RESDNC", StringType(), False),
    StructField("TC3_SBM_POS", StringType(), False),
    StructField("TC3_CONT_THER_FLAG", StringType(), False),
    StructField("TC3_CONT_THER_SCHED_ID", StringType(), False),
    StructField("TC3_APP_AMT_APLD_PER_DEDCT", StringType(), False),
    StructField("PDH_APP_AMT_ATRBD_PRCSR_FEE", StringType(), False),
    StructField("PDH_APP_AMT_ATRBD_SLS_TAX", StringType(), False),
    StructField("PDH_APP_ATRBD_PROD_SEL_AMT", StringType(), False),
    StructField("PDH_APP_COPAY_AMT", StringType(), False),
    StructField("PDH_APP_COPAY_FLAT_AMT", StringType(), False),
    StructField("PDH_APP_COPAY_PCT_AMT", StringType(), False),
    StructField("PDH_APP_DISPNS_FEE", StringType(), False),
    StructField("PDH_APP_DUE_AMT", StringType(), False),
    StructField("PDH_APP_EXCEEDING_PER_BNF_AMT", StringType(), False),
    StructField("PDH_APP_FLAT_SLS_TAX_AMT", StringType(), False),
    StructField("PDH_APP_PCT_SLS_TAX_AMT_PD", StringType(), False),
    StructField("PDH_APP_INCNTV_AMT", StringType(), False),
    StructField("PDH_APP_INGR_CST", StringType(), False),
    StructField("PDH_APP_OTHR_PAYER_AMT_RECOG", StringType(), False),
    StructField("PDH_APP_PATN_PAY_AMT", StringType(), False),
    StructField("PDH_APP_TOT_OTHR_AMT", StringType(), False),
    StructField("PDH_APP_WTHLD_AMT", StringType(), False),
    StructField("PDH_APP_AMT_ATRBD_PROV_NTWK_SEL", StringType(), False),
    StructField("PDH_APP_HLTH_PLN_FUND_ASSTANCE_AMT", StringType(), False),
    StructField("PDH_CAL_PCT_SLS_TAX_AMT_PD", StringType(), False),
    StructField("PDH_CAL_AMT_APLD_PER_DEDCT", StringType(), False),
    StructField("PDH_CAL_AMT_ATRBD_PRCSR_FEE", StringType(), False),
    StructField("PDH_CAL_AMT_ATRBD_SLS_TAX", StringType(), False),
    StructField("PDH_CAL_ATRBD_PROD_SEL_AMT", StringType(), False),
    StructField("PDH_CAL_COPAY_AMT", StringType(), False),
    StructField("PDH_CAL_COPAY_FLAT_AMT", StringType(), False),
    StructField("PDH_CAL_COPAY_PCT_AMT", StringType(), False),
    StructField("PDH_CAL_DISPNS_FEE", StringType(), False),
    StructField("PDH_CAL_EXCEEDING_PER_BNF_AMT", StringType(), False),
    StructField("PDH_CAL_FLAT_SLSTAX_AMT", StringType(), False),
    StructField("PDH_CAL_INCNTV_FEE_AMT", StringType(), False),
    StructField("PDH_CAL_INGR_CST", StringType(), False),
    StructField("PDH_CAL_OTHR_PAYER_AMT_RECOG", StringType(), False),
    StructField("PDH_CAL_PATN_PAY_AMT", StringType(), False),
    StructField("PDH_CAL_TOT_AMT", StringType(), False),
    StructField("PDH_CAL_TOT_OTHR_AMT", StringType(), False),
    StructField("PDH_CAL_WTHLD_AMT", StringType(), False),
    StructField("PDH_CAL_AMT_ATRBD_PROV_NTWK_SEL", StringType(), False),
    StructField("PDH_CAL_HLTH_PLN_FUND_ASSTNC_AMT", StringType(), False),
    StructField("PDH_RBL_PCT_SLS_TAX_AMT_PD", StringType(), False),
    StructField("PDH_RBL_AMT_APLD_PER_DEDCT", StringType(), False),
    StructField("PDH_RBL_AMT_ATRBD_PRCSR_FEE", StringType(), False),
    StructField("PDH_RBL_AMT_ATRBD_SLS_TAX", StringType(), False),
    StructField("PDH_RBL_ATRBD_PROD_SEL_AMT", StringType(), False),
    StructField("PDH_RBL_COPAY_AMT", StringType(), False),
    StructField("PDH_RBL_COPAY_FLAT_AMT", StringType(), False),
    StructField("PDH_RBL_COPAY_PCT_AMT", StringType(), False),
    StructField("PDH_RBL_DISPNS_FEE", StringType(), False),
    StructField("PDH_RBL_DUE_AMT", StringType(), False),
    StructField("PDH_RBL_EXCEEDING_PER_BNF_AMT", StringType(), False),
    StructField("PDH_RBL_FLAT_SLS_TAX_AMT", StringType(), False),
    StructField("PDH_RBL_INCNTV_AMT", StringType(), False),
    StructField("PDH_RBL_INGR_CST", StringType(), False),
    StructField("PDH_RBL_OTHR_PAYOR_AMT_RECOG", StringType(), False),
    StructField("PDH_RBL_PATN_PAY_AMT", StringType(), False),
    StructField("PDH_RBL_TOT_OTHR_AMT", StringType(), False),
    StructField("PDH_RBL_WTHLD_AMT", StringType(), False),
    StructField("PDH_RBL_AMT_ATRBD_PROV_NTWK_SEL", StringType(), False),
    StructField("PDH_RBL_HLTH_PLN_FUND_ASSTNC_AMT", StringType(), False),
    StructField("ADD_USER_NM", StringType(), False),
    StructField("ADD_DT", StringType(), False),
    StructField("ADD_TM", StringType(), False),
    StructField("ADD_PGM_NM", StringType(), False),
    StructField("REPRCS_DT", StringType(), False),
    StructField("PD7_REPRCS_TM", StringType(), False),
    StructField("IN_NTWK_IN", StringType(), False),
    StructField("PROD_ID_QLFR", StringType(), False),
    StructField("RXC_UNUSED_COUPON_AMT", StringType(), False),
    StructField("FLR", StringType(), False)
])

df_SeqFile_CLM_ACA_PDX_SUPLMT_raw = (
    spark.read.format("csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .option("quote", "")
    .schema(schema_SeqFile_CLM_ACA_PDX_SUPLMT)
    .load(f"{adls_path_raw}/landing/{InFile}")
)

df_SeqFile_CLM_ACA_PDX_SUPLMT = df_SeqFile_CLM_ACA_PDX_SUPLMT_raw.alias("Lnk_CLM_ACA_PDX_SUPLMT")

df_Xfm_CLM_ACA_PDX_SUPLMT_filtered = df_SeqFile_CLM_ACA_PDX_SUPLMT.filter(
    (F.upper(F.col("CLAIMSTS")) == "P") & (F.col("PD7_REPRCS_SEQ_NO") != "000")
)

df_Xfm_CLM_ACA_PDX_SUPLMT_svars = (
    df_Xfm_CLM_ACA_PDX_SUPLMT_filtered
    .withColumn(
        "SVClmInd",
        F.when(trim(F.col("CLAIMSTS")) == "X", F.lit("R")).otherwise(F.lit(""))
    )
    .withColumn("CLM_ID", F.concat(trim(F.col("PDX_CLM_NO")), trim(F.col("CLM_SEQ_NO")), F.col("SVClmInd")))
    .withColumn("CLM_REPRCS_SEQ_NO", F.col("PD7_REPRCS_SEQ_NO").cast(DecimalType(10,0)))
    .withColumn("SRC_SYS_CD", F.lit(SrcSysCd))
    .withColumn("DRUG_CLM_SK", F.lit(0))
    .withColumn("QHP_CSR_VRNT_CD", F.col("HIM_CSR_LVL"))
    .withColumn(
        "CLM_REPRCS_DT_SK",
        F.when(
            (F.col("REPRCS_DT").isNull())
            | (trim(F.col("REPRCS_DT")) == "")
            | (F.col("REPRCS_DT") == "00000000"),
            F.lit("1753-01-01")
        ).otherwise(
            F.concat(
                F.col("REPRCS_DT").substr(F.lit(1), F.lit(4)), F.lit("-"),
                F.col("REPRCS_DT").substr(F.lit(5), F.lit(2)), F.lit("-"),
                F.col("REPRCS_DT").substr(F.lit(7), F.lit(2))
            )
        )
    )
    .withColumn(
        "CLM_SUBMT_DT_SK",
        F.when(
            (F.col("EX8_DT_SUBMT").isNull())
            | (trim(F.col("EX8_DT_SUBMT")) == "")
            | (F.col("EX8_DT_SUBMT") == "00000000"),
            F.lit("1753-01-01")
        ).otherwise(
            F.concat(
                F.col("EX8_DT_SUBMT").substr(F.lit(1), F.lit(4)), F.lit("-"),
                F.col("EX8_DT_SUBMT").substr(F.lit(5), F.lit(2)), F.lit("-"),
                F.col("EX8_DT_SUBMT").substr(F.lit(7), F.lit(2))
            )
        )
    )
    .withColumn(
        "CLM_SUBMT_TM",
        F.when(
            (F.col("EX8_TM_SUBMT").isNull())
            | (trim(F.col("EX8_TM_SUBMT")) == ""),
            F.lit("00:00:00")
        ).otherwise(
            F.concat(
                F.col("EX8_TM_SUBMT").substr(F.lit(1), F.lit(2)), F.lit(":"),
                F.col("EX8_TM_SUBMT").substr(F.lit(3), F.lit(2)), F.lit(":"),
                F.col("EX8_TM_SUBMT").substr(F.lit(5), F.lit(2))
            )
        )
    )
    .withColumn(
        "ORIG_CLM_RCVD_DT_SK",
        F.when(
            (F.col("TCD_DT_ORIG_CLM_RCVD").isNull())
            | (trim(F.col("TCD_DT_ORIG_CLM_RCVD")) == "")
            | (F.col("TCD_DT_ORIG_CLM_RCVD") == "00000000"),
            F.lit("1753-01-01")
        ).otherwise(
            F.concat(
                F.col("TCD_DT_ORIG_CLM_RCVD").substr(F.lit(1), F.lit(4)), F.lit("-"),
                F.col("TCD_DT_ORIG_CLM_RCVD").substr(F.lit(5), F.lit(2)), F.lit("-"),
                F.col("TCD_DT_ORIG_CLM_RCVD").substr(F.lit(7), F.lit(2))
            )
        )
    )
    .withColumn(
        "CLM_REPRCS_PAYBL_AMT",
        F.when(
            (F.col("TCD_RBL_TOT_AMT").isNull()) | (trim(F.col("TCD_RBL_TOT_AMT")) == ""),
            F.lit("0.00")
        ).otherwise(trim(F.col("TCD_RBL_TOT_AMT")))
    )
    .withColumn(
        "CLM_REPRCS_PATN_RESP_AMT",
        F.when(
            (F.col("TCD_RBL_PATN_PAY_AMT").isNull()) | (trim(F.col("TCD_RBL_PATN_PAY_AMT")) == ""),
            F.lit("0.00")
        ).otherwise(trim(F.col("TCD_RBL_PATN_PAY_AMT")))
    )
    .withColumn(
        "SHADOW_ADJDCT_PAYBL_AMT",
        F.when(
            (F.col("PDH_RBL_DUE_AMT").isNull()) | (trim(F.col("PDH_RBL_DUE_AMT")) == ""),
            F.lit("0.00")
        ).otherwise(trim(F.col("PDH_RBL_DUE_AMT")))
    )
    .withColumn(
        "SHADOW_ADJDCT_PATN_RESP_AMT",
        F.when(
            (F.col("PDH_RBL_PATN_PAY_AMT").isNull()) | (trim(F.col("PDH_RBL_PATN_PAY_AMT")) == ""),
            F.lit("0.00")
        ).otherwise(trim(F.col("PDH_RBL_PATN_PAY_AMT")))
    )
    .withColumn(
        "SHADOW_ADJDCT_COINS_AMT",
        F.when(
            (F.col("PDH_RBL_COPAY_PCT_AMT").isNull()) | (trim(F.col("PDH_RBL_COPAY_PCT_AMT")) == ""),
            F.lit("0.00")
        ).otherwise(trim(F.col("PDH_RBL_COPAY_PCT_AMT")))
    )
    .withColumn(
        "SHADOW_ADJDCT_COPAY_AMT",
        F.when(
            (F.col("PDH_RBL_COPAY_FLAT_AMT").isNull()) | (trim(F.col("PDH_RBL_COPAY_FLAT_AMT")) == ""),
            F.lit("0.00")
        ).otherwise(trim(F.col("PDH_RBL_COPAY_FLAT_AMT")))
    )
    .withColumn(
        "SHADOW_ADJDCT_DEDCT_AMT",
        F.when(
            (F.col("PDH_RBL_AMT_APLD_PER_DEDCT").isNull()) | (trim(F.col("PDH_RBL_AMT_APLD_PER_DEDCT")) == ""),
            F.lit("0.00")
        ).otherwise(trim(F.col("PDH_RBL_AMT_APLD_PER_DEDCT")))
    )
    .withColumn(
        "PBM_CALC_CSR_SBSDY_AMT",
        F.expr("CAST(HIM_SBSDY_AMT AS DECIMAL(38,2)) * -1")
    )
)

df_Lnk_Xfm_logic = df_Xfm_CLM_ACA_PDX_SUPLMT_svars.select(
    F.col("CLM_ID"),
    F.col("CLM_REPRCS_SEQ_NO"),
    F.col("SRC_SYS_CD"),
    F.col("DRUG_CLM_SK"),
    F.col("QHP_CSR_VRNT_CD"),
    F.col("CLM_REPRCS_DT_SK"),
    F.col("CLM_SUBMT_DT_SK"),
    F.col("CLM_SUBMT_TM"),
    F.col("ORIG_CLM_RCVD_DT_SK"),
    F.col("CLM_REPRCS_PAYBL_AMT"),
    F.col("CLM_REPRCS_PATN_RESP_AMT"),
    F.col("SHADOW_ADJDCT_PAYBL_AMT"),
    F.col("SHADOW_ADJDCT_PATN_RESP_AMT"),
    F.col("SHADOW_ADJDCT_COINS_AMT"),
    F.col("SHADOW_ADJDCT_COPAY_AMT"),
    F.col("SHADOW_ADJDCT_DEDCT_AMT"),
    F.col("PBM_CALC_CSR_SBSDY_AMT")
)

df_Copy = df_Lnk_Xfm_logic

df_LnkLeftJn = df_Copy.select(
    F.col("CLM_ID"),
    F.col("CLM_REPRCS_SEQ_NO"),
    F.col("SRC_SYS_CD"),
    F.col("DRUG_CLM_SK"),
    F.col("QHP_CSR_VRNT_CD"),
    F.col("CLM_REPRCS_DT_SK"),
    F.col("CLM_SUBMT_DT_SK"),
    F.col("CLM_SUBMT_TM"),
    F.col("ORIG_CLM_RCVD_DT_SK"),
    F.col("CLM_REPRCS_PAYBL_AMT"),
    F.col("CLM_REPRCS_PATN_RESP_AMT"),
    F.col("SHADOW_ADJDCT_PAYBL_AMT"),
    F.col("SHADOW_ADJDCT_PATN_RESP_AMT"),
    F.col("SHADOW_ADJDCT_COINS_AMT"),
    F.col("SHADOW_ADJDCT_COPAY_AMT"),
    F.col("SHADOW_ADJDCT_DEDCT_AMT"),
    F.col("PBM_CALC_CSR_SBSDY_AMT")
)

df_LnkRemovDup = df_Copy.select(
    F.col("CLM_ID"),
    F.col("CLM_REPRCS_SEQ_NO"),
    F.col("SRC_SYS_CD")
)

df_Remove_Duplicates_164 = dedup_sort(
    df_LnkRemovDup,
    ["CLM_ID","CLM_REPRCS_SEQ_NO","SRC_SYS_CD"],
    []
)

df_lnkRemDupDataOut = df_Remove_Duplicates_164.select(
    F.col("CLM_ID"),
    F.col("CLM_REPRCS_SEQ_NO"),
    F.col("SRC_SYS_CD")
)

df_jn_left_NaturalKeys = df_lnkRemDupDataOut.alias("lnkRemDupDataOut").join(
    df_db_K_DRUG_CLM_ACA_REPRCS_RSLT_read.alias("lnk_to_jn"),
    on=[
        F.col("lnkRemDupDataOut.CLM_ID") == F.col("lnk_to_jn.CLM_ID"),
        F.col("lnkRemDupDataOut.CLM_REPRCS_SEQ_NO") == F.col("lnk_to_jn.CLM_REPRCS_SEQ_NO"),
        F.col("lnkRemDupDataOut.SRC_SYS_CD") == F.col("lnk_to_jn.SRC_SYS_CD")
    ],
    how="left"
).select(
    F.col("lnkRemDupDataOut.CLM_ID").alias("CLM_ID"),
    F.col("lnkRemDupDataOut.CLM_REPRCS_SEQ_NO").alias("CLM_REPRCS_SEQ_NO"),
    F.col("lnkRemDupDataOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_to_jn.DRUG_CLM_ACA_REPRCS_RSLT_SK").alias("DRUG_CLM_ACA_REPRCS_RSLT_SK"),
    F.col("lnk_to_jn.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK")
)

df_to_pKeyGen = df_jn_left_NaturalKeys

df_enriched = df_to_pKeyGen.withColumnRenamed("DRUG_CLM_ACA_REPRCS_RSLT_SK","orig_DRUG_CLM_ACA_REPRCS_RSLT_SK")
df_enriched = df_enriched.withColumn(
    "DRUG_CLM_ACA_REPRCS_RSLT_SK",
    F.when(
        (F.col("orig_DRUG_CLM_ACA_REPRCS_RSLT_SK").isNull()) | (F.col("orig_DRUG_CLM_ACA_REPRCS_RSLT_SK") == 0),
        F.lit(0)
    ).otherwise(
        trim(F.col("orig_DRUG_CLM_ACA_REPRCS_RSLT_SK"))
    )
)

df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,'DRUG_CLM_ACA_REPRCS_RSLT_SK',<schema>,<secret_name>)

df_xfm_PKEYgen = (
    df_enriched
    .withColumn(
        "CRT_RUN_CYC_EXCTN_SK_right",
        F.when(
            (F.col("orig_DRUG_CLM_ACA_REPRCS_RSLT_SK").isNull()) | (F.col("orig_DRUG_CLM_ACA_REPRCS_RSLT_SK") == 0),
            F.lit(IDSRunCycle)
        ).otherwise(
            F.col("CRT_RUN_CYC_EXCTN_SK")
        )
    )
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK_right", F.lit(IDSRunCycle))
)

df_lnk_jn_right = df_xfm_PKEYgen.select(
    F.col("DRUG_CLM_ACA_REPRCS_RSLT_SK"),
    F.col("CLM_ID"),
    F.col("CLM_REPRCS_SEQ_NO"),
    F.col("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK_right"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK_right")
)

df_lnk_to_db_write = df_xfm_PKEYgen.filter(
    ((F.col("orig_DRUG_CLM_ACA_REPRCS_RSLT_SK").isNull()) | (F.col("orig_DRUG_CLM_ACA_REPRCS_RSLT_SK") == 0))
).select(
    F.col("DRUG_CLM_ACA_REPRCS_RSLT_SK").alias("DRUG_CLM_ACA_REPRCS_RSLT_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_REPRCS_SEQ_NO").alias("CLM_REPRCS_SEQ_NO"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.lit(IDSRunCycle).alias("CRT_RUN_CYC_EXCTN_SK")
)

temp_table_db_K_DRUG_CLM_ACA_REPRCS_RSLT_write = "STAGING.OptumACADrugClmReprcsRsltPK_db_K_DRUG_CLM_ACA_REPRCS_RSLT_write_temp"

execute_dml(f"DROP TABLE IF EXISTS {temp_table_db_K_DRUG_CLM_ACA_REPRCS_RSLT_write}", jdbc_url, jdbc_props)
df_lnk_to_db_write.write.jdbc(
    url=jdbc_url,
    table=temp_table_db_K_DRUG_CLM_ACA_REPRCS_RSLT_write,
    mode="overwrite",
    properties=jdbc_props
)

merge_sql_db_K_DRUG_CLM_ACA_REPRCS_RSLT_write = f"""
MERGE INTO {IDSOwner}.K_DRUG_CLM_ACA_REPRCS_RSLT AS tgt
USING {temp_table_db_K_DRUG_CLM_ACA_REPRCS_RSLT_write} AS stg
ON tgt.DRUG_CLM_ACA_REPRCS_RSLT_SK = stg.DRUG_CLM_ACA_REPRCS_RSLT_SK
WHEN MATCHED THEN
  UPDATE SET
    tgt.CLM_ID = stg.CLM_ID,
    tgt.CLM_REPRCS_SEQ_NO = stg.CLM_REPRCS_SEQ_NO,
    tgt.SRC_SYS_CD = stg.SRC_SYS_CD,
    tgt.CRT_RUN_CYC_EXCTN_SK = stg.CRT_RUN_CYC_EXCTN_SK
WHEN NOT MATCHED THEN
  INSERT (
    DRUG_CLM_ACA_REPRCS_RSLT_SK,
    CLM_ID,
    CLM_REPRCS_SEQ_NO,
    SRC_SYS_CD,
    CRT_RUN_CYC_EXCTN_SK
  )
  VALUES (
    stg.DRUG_CLM_ACA_REPRCS_RSLT_SK,
    stg.CLM_ID,
    stg.CLM_REPRCS_SEQ_NO,
    stg.SRC_SYS_CD,
    stg.CRT_RUN_CYC_EXCTN_SK
  );
"""
execute_dml(merge_sql_db_K_DRUG_CLM_ACA_REPRCS_RSLT_write, jdbc_url, jdbc_props)

df_jn_left = df_lnk_jn_right.alias("lnk_jn_right").join(
    df_LnkLeftJn.alias("LnkLeftJn"),
    on=[
        F.col("lnk_jn_right.CLM_ID") == F.col("LnkLeftJn.CLM_ID"),
        F.col("lnk_jn_right.CLM_REPRCS_SEQ_NO") == F.col("LnkLeftJn.CLM_REPRCS_SEQ_NO"),
        F.col("lnk_jn_right.SRC_SYS_CD") == F.col("LnkLeftJn.SRC_SYS_CD")
    ],
    how="inner"
).select(
    F.col("lnk_jn_right.DRUG_CLM_ACA_REPRCS_RSLT_SK").alias("DRUG_CLM_ACA_REPRCS_RSLT_SK"),
    F.col("lnk_jn_right.CLM_ID").alias("CLM_ID"),
    F.col("lnk_jn_right.CLM_REPRCS_SEQ_NO").alias("CLM_REPRCS_SEQ_NO"),
    F.col("lnk_jn_right.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_jn_right.CRT_RUN_CYC_EXCTN_SK_right").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_jn_right.LAST_UPDT_RUN_CYC_EXCTN_SK_right").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("LnkLeftJn.DRUG_CLM_SK").alias("DRUG_CLM_SK"),
    F.col("LnkLeftJn.QHP_CSR_VRNT_CD").alias("QHP_CSR_VRNT_CD"),
    F.col("LnkLeftJn.CLM_REPRcs_DT_SK").alias("CLM_REPRCS_DT_SK"),
    F.col("LnkLeftJn.CLM_SUBMT_DT_SK").alias("CLM_SUBMT_DT_SK"),
    F.col("LnkLeftJn.CLM_SUBMT_TM").alias("CLM_SUBMT_TM"),
    F.col("LnkLeftJn.ORIG_CLM_RCVD_DT_SK").alias("ORIG_CLM_RCVD_DT_SK"),
    F.col("LnkLeftJn.CLM_REPRCS_PAYBL_AMT").alias("CLM_REPRCS_PAYBL_AMT"),
    F.col("LnkLeftJn.CLM_REPRCS_PATN_RESP_AMT").alias("CLM_REPRCS_PATN_RESP_AMT"),
    F.col("LnkLeftJn.SHADOW_ADJDCT_PAYBL_AMT").alias("SHADOW_ADJDCT_PAYBL_AMT"),
    F.col("LnkLeftJn.SHADOW_ADJDCT_PATN_RESP_AMT").alias("SHADOW_ADJDCT_PATN_RESP_AMT"),
    F.col("LnkLeftJn.SHADOW_ADJDCT_COINS_AMT").alias("SHADOW_ADJDCT_COINS_AMT"),
    F.col("LnkLeftJn.SHADOW_ADJDCT_COPAY_AMT").alias("SHADOW_ADJDCT_COPAY_AMT"),
    F.col("LnkLeftJn.SHADOW_ADJDCT_DEDCT_AMT").alias("SHADOW_ADJDCT_DEDCT_AMT"),
    F.col("LnkLeftJn.PBM_CALC_CSR_SBSDY_AMT").alias("PBM_CALC_CSR_SBSDY_AMT")
)

df_Xfm1 = df_jn_left

df_lnk_to_seq_file_load = df_Xfm1.select(
    F.col("DRUG_CLM_ACA_REPRCS_RSLT_SK"),
    F.col("CLM_ID"),
    F.col("CLM_REPRCS_SEQ_NO"),
    F.col("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("DRUG_CLM_SK"),
    F.col("QHP_CSR_VRNT_CD"),
    F.col("CLM_REPRCS_DT_SK"),
    F.col("CLM_SUBMT_DT_SK"),
    F.col("CLM_SUBMT_TM"),
    F.col("ORIG_CLM_RCVD_DT_SK"),
    F.col("CLM_REPRCS_PAYBL_AMT"),
    F.col("CLM_REPRCS_PATN_RESP_AMT"),
    F.col("SHADOW_ADJDCT_PAYBL_AMT"),
    F.col("SHADOW_ADJDCT_PATN_RESP_AMT"),
    F.col("SHADOW_ADJDCT_COINS_AMT"),
    F.col("SHADOW_ADJDCT_COPAY_AMT"),
    F.col("SHADOW_ADJDCT_DEDCT_AMT"),
    F.col("PBM_CALC_CSR_SBSDY_AMT")
)

df_Lnk_BDruGClmAcaReprcsRslt = df_Xfm1.select(
    F.col("CLM_ID"),
    F.col("CLM_REPRCS_SEQ_NO"),
    F.col("SRC_SYS_CD")
)

df_final_Seq_DRUG_CLM_ACA_REPRCS_RSLT = df_lnk_to_seq_file_load
rpad_columns_seq1 = [
    ("QHP_CSR_VRNT_CD", 2),
    ("CLM_REPRCS_DT_SK", 10),
    ("CLM_SUBMT_DT_SK", 10),
    ("ORIG_CLM_RCVD_DT_SK", 10)
]
for c, ln in rpad_columns_seq1:
    df_final_Seq_DRUG_CLM_ACA_REPRCS_RSLT = df_final_Seq_DRUG_CLM_ACA_REPRCS_RSLT.withColumn(
        c, F.rpad(F.col(c), ln, " ")
    )

df_final_Seq_B_DRUG_CLM_ACA_REPRCS_RSLT = df_Lnk_BDruGClmAcaReprcsRslt

write_files(
    df_final_Seq_DRUG_CLM_ACA_REPRCS_RSLT.select(
        "DRUG_CLM_ACA_REPRCS_RSLT_SK",
        "CLM_ID",
        "CLM_REPRCS_SEQ_NO",
        "SRC_SYS_CD",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "DRUG_CLM_SK",
        "QHP_CSR_VRNT_CD",
        "CLM_REPRCS_DT_SK",
        "CLM_SUBMT_DT_SK",
        "CLM_SUBMT_TM",
        "ORIG_CLM_RCVD_DT_SK",
        "CLM_REPRCS_PAYBL_AMT",
        "CLM_REPRCS_PATN_RESP_AMT",
        "SHADOW_ADJDCT_PAYBL_AMT",
        "SHADOW_ADJDCT_PATN_RESP_AMT",
        "SHADOW_ADJDCT_COINS_AMT",
        "SHADOW_ADJDCT_COPAY_AMT",
        "SHADOW_ADJDCT_DEDCT_AMT",
        "PBM_CALC_CSR_SBSDY_AMT"
    ),
    f"{adls_path}/key/DRUG_CLM_ACA_REPRCS_RSLT.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

write_files(
    df_final_Seq_B_DRUG_CLM_ACA_REPRCS_RSLT.select(
        "CLM_ID",
        "CLM_REPRCS_SEQ_NO",
        "SRC_SYS_CD"
    ),
    f"{adls_path}/load/B_DRUG_CLM_ACA_REPRCS_RSLT.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="",
    nullValue=None
)