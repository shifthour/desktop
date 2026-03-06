# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC Â© Copyright 2020  Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC ZENA JOBNAME:  OPTUMRX_IDS_EDW_DRUG_CLM_CMPND_INGR_DAILY_000
# MAGIC 
# MAGIC CALLED BY:  OptumIDSCompoundsSeq
# MAGIC 
# MAGIC JOB NAME:  OptumIDSCompoundsExtr
# MAGIC 
# MAGIC Description:  Job transforms the ACA and MA Compound data files from OptumRx before loading into table DRUG_CLM_CMPND_INGR in database IDS.
# MAGIC 
# MAGIC Modifications:
# MAGIC 
# MAGIC Developer            Date            Project/User Story             Change Description                                                                Development Project    Code Reviewer            Date Reviewed
# MAGIC -------------------------   -----------------   ----------------------------------------  --------------------------------------------------------------------------------------------   ----------------------------------   ---------------------------------   -------------------------
# MAGIC Bill Schroeder      2020-09-25  F-209426/US217375         Initial Programming.                                                                 IntegrateDev2 \(9)Jaideep Mankala      12/18/2020
# MAGIC Bill Schroeder      2021-09-22  US334499\(9)          Removed DOSE_FORM_SK column for new table layout.     IntegrateDev2               Jaideep Mankala        12/07/2021

# MAGIC Filter for Paid and Reversal Claims
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
# COMMAND ----------
# MAGIC %run ../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
FileName = get_widget_value('FileName','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
RunID = get_widget_value('RunID','')

# Read "Compound_Supplemental_Claims"
schema_Compound_Supplemental_Claims = StructType([
    StructField("RCXCLM_NO", StringType(), False),
    StructField("CLM_SEQ_NO", StringType(), False),
    StructField("CLM_STTUS", StringType(), False),
    StructField("OCD_CMPND_SEQ_NO", StringType(), False),
    StructField("OCD_SUBMT_CMPND_PROD_ID_QLFR", StringType(), False),
    StructField("OCD_CMPND_PROD_ID", StringType(), False),
    StructField("OCD_CMPND_PROD_KEY", StringType(), False),
    StructField("PROD_DESC_ABBR", StringType(), False),
    StructField("PROD_BRND_NM", StringType(), False),
    StructField("DRUG_MNFCTR_ID", StringType(), False),
    StructField("DRUG_MNFCTR", StringType(), False),
    StructField("CMD_GNRC_PROD_IN_NO", StringType(), False),
    StructField("DRUG_GNRC_NM", StringType(), False),
    StructField("DDID_DRUG_DSCRPTR_ID", StringType(), False),
    StructField("GCN_GNRC_CD_NO", StringType(), False),
    StructField("GCN_GNRC_CD_SEQ_NO", StringType(), False),
    StructField("PRD_KNW_BASED_DRUG_CD", StringType(), False),
    StructField("AMRCN_HOSP_FRMLRY_SVC_CLS_CD", StringType(), False),
    StructField("DEA_CD", StringType(), False),
    StructField("DRUG_RX_OVER_THE_CTR_IN", StringType(), False),
    StructField("MULTI_SRC_CD", StringType(), False),
    StructField("GNRC_IN_OVRD", StringType(), False),
    StructField("PROD_RMBRMT_IN", StringType(), False),
    StructField("BRND_NM_CD", StringType(), False),
    StructField("DRUG_FDA_THRPTC_EQVLNT", StringType(), False),
    StructField("DM_METRIC_STRG", StringType(), False),
    StructField("DRUG_STRG_UNIT_OF_MESR", StringType(), False),
    StructField("DRUG_ADM_RTE", StringType(), False),
    StructField("DRUG_DOSE_FORM", StringType(), False),
    StructField("MNTN_DRUG_CD", StringType(), False),
    StructField("DRUG_3RD_PARTY_EXCPT_CD", StringType(), False),
    StructField("DRUG_UNIT_DOSE", StringType(), False),
    StructField("OCD_SUBMT_CMPND_INGR_QTY", StringType(), False),
    StructField("OCD_SUBMT_CMPND_INGR_CST", StringType(), False),
    StructField("OCD_SUBMT_CMPND_BSS_OF_CST", StringType(), False),
    StructField("OCD_INGR_STTUS", StringType(), False),
    StructField("CMD_CALC_INGR_CST", StringType(), False),
    StructField("CMD_APRV_INGR_CST", StringType(), False),
    StructField("CMD_CLNT_INGR_CST", StringType(), False),
    StructField("CMD_APRV_CST_SRC", StringType(), False),
    StructField("CMD_APRV_CST_TYP_CD", StringType(), False),
    StructField("CMD_APRV_CST_TYP_UNIT_CST", StringType(), False),
    StructField("CMD_CLNT_CST_SRC", StringType(), False),
    StructField("CMD_CLNT_CST_TYP_CD", StringType(), False),
    StructField("CMD_CLNT_CST_TYP_UNIT_CST", StringType(), False),
    StructField("CMD_AVG_WHLSL_PRICE_UNIT_CST", StringType(), False),
    StructField("CMD_CALC_PROF_SVC_FEE_PD", StringType(), False),
    StructField("CMD_APRV_PROF_SVC_FEE_PD", StringType(), False),
    StructField("CMD_CLNT_PROF_SVC_FEE_PD", StringType(), False),
    StructField("RBT_MNFCTR_IN", StringType(), False),
    StructField("PRMT_EXCL_IN", StringType(), False),
    StructField("MED_SUPL_IN", StringType(), False),
    StructField("PDX_RATE", StringType(), False),
    StructField("CLNT_RATE_PCT", StringType(), False),
    StructField("INGR_MOD_CD_CT", StringType(), False),
    StructField("FILLER", StringType(), False)
])
df_Compound_Supplemental_Claims = (
    spark.read
    .option("header", False)
    .option("quote", "\"")
    .schema(schema_Compound_Supplemental_Claims)
    .csv(f"{adls_path_raw}/landing/{FileName}")
)

# Transformer: Filter_Clm_Status => 2 outputs

df_Filter_Out_temp = df_Compound_Supplemental_Claims.filter(
    (
        (F.col("CLM_STTUS") == "P") | (F.col("CLM_STTUS") == "X")
    )
    & (F.col("OCD_INGR_STTUS") != "R")
)

# Build df_Filter_Out columns:
df_Filter_Out = df_Filter_Out_temp.select(
    F.trim(F.col("RCXCLM_NO")).alias("RCXCLM_NO"),
    F.trim(F.col("CLM_SEQ_NO")).alias("CLM_SEQ_NO"),
    F.trim(F.col("CLM_STTUS")).alias("CLM_STTUS"),
    F.when(F.trim(F.col("CLM_STTUS")) == "X",
           F.concat(F.trim(F.col("RCXCLM_NO")), F.trim(F.col("CLM_SEQ_NO")), F.lit("R")))
     .otherwise(F.concat(F.trim(F.col("RCXCLM_NO")), F.trim(F.col("CLM_SEQ_NO"))))
      .alias("CLM_ID"),
    F.trim(F.col("OCD_CMPND_SEQ_NO")).alias("OCD_CMPND_SEQ_NO"),
    F.trim(F.col("OCD_SUBMT_CMPND_PROD_ID_QLFR")).alias("OCD_SUBMT_CMPND_PROD_ID_QLFR"),
    F.trim(F.col("OCD_CMPND_PROD_ID")).alias("OCD_CMPND_PROD_ID"),
    F.trim(F.col("OCD_CMPND_PROD_KEY")).alias("OCD_CMPND_PROD_KEY"),
    F.trim(F.col("PROD_DESC_ABBR")).alias("PROD_DESC_ABBR"),
    F.trim(F.col("PROD_BRND_NM")).alias("PROD_BRND_NM"),
    F.trim(F.col("DRUG_MNFCTR_ID")).alias("DRUG_MNFCTR_ID"),
    F.trim(F.col("DRUG_MNFCTR")).alias("DRUG_MNFCTR"),
    F.trim(F.col("CMD_GNRC_PROD_IN_NO")).alias("CMD_GNRC_PROD_IN_NO"),
    F.trim(F.col("DRUG_GNRC_NM")).alias("DRUG_GNRC_NM"),
    F.trim(F.col("DDID_DRUG_DSCRPTR_ID")).alias("DDID_DRUG_DSCRPTR_ID"),
    F.trim(F.col("GCN_GNRC_CD_NO")).alias("GCN_GNRC_CD_NO"),
    F.trim(F.col("GCN_GNRC_CD_SEQ_NO")).alias("GCN_GNRC_CD_SEQ_NO"),
    F.trim(F.col("PRD_KNW_BASED_DRUG_CD")).alias("PRD_KNW_BASED_DRUG_CD"),
    F.trim(F.col("AMRCN_HOSP_FRMLRY_SVC_CLS_CD")).alias("AMRCN_HOSP_FRMLRY_SVC_CLS_CD"),
    F.trim(F.col("DEA_CD")).alias("DEA_CD"),
    F.trim(F.col("DRUG_RX_OVER_THE_CTR_IN")).alias("DRUG_RX_OVER_THE_CTR_IN"),
    F.trim(F.col("MULTI_SRC_CD")).alias("MULTI_SRC_CD"),
    F.trim(F.col("GNRC_IN_OVRD")).alias("GNRC_IN_OVRD"),
    F.trim(F.col("PROD_RMBRMT_IN")).alias("PROD_RMBRMT_IN"),
    F.trim(F.col("BRND_NM_CD")).alias("BRND_NM_CD"),
    F.trim(F.col("DRUG_FDA_THRPTC_EQVLNT")).alias("DRUG_FDA_THRPTC_EQVLNT"),
    F.trim(F.col("DM_METRIC_STRG")).alias("DM_METRIC_STRG"),
    F.trim(F.col("DRUG_STRG_UNIT_OF_MESR")).alias("DRUG_STRG_UNIT_OF_MESR"),
    F.trim(F.col("DRUG_ADM_RTE")).alias("DRUG_ADM_RTE"),
    F.trim(F.col("DRUG_DOSE_FORM")).alias("DRUG_DOSE_FORM"),
    F.trim(F.col("MNTN_DRUG_CD")).alias("MNTN_DRUG_CD"),
    F.trim(F.col("DRUG_3RD_PARTY_EXCPT_CD")).alias("DRUG_3RD_PARTY_EXCPT_CD"),
    F.trim(F.col("DRUG_UNIT_DOSE")).alias("DRUG_UNIT_DOSE"),
    F.when(
        (F.col("OCD_SUBMT_CMPND_INGR_QTY").isNull())
        | (F.col("OCD_SUBmt_CMPND_INGR_QTY") == "")
        | (F.col("OCD_SUBMT_CMPND_INGR_QTY") == " "),
        F.lit(-999999)
    ).otherwise(F.trim(F.col("OCD_SUBMT_CMPND_INGR_QTY"))).alias("OCD_SUBMT_CMPND_INGR_QTY"),
    F.when(
        (F.col("OCD_SUBMT_CMPND_INGR_CST").isNull())
        | (F.col("OCD_SUBMT_CMPND_INGR_CST") == "")
        | (F.col("OCD_SUBMT_CMPND_INGR_CST") == " "),
        F.lit(0)
    ).otherwise(F.regexp_replace(F.col("OCD_SUBMT_CMPND_INGR_CST"), " ", "")).alias("OCD_SUBMT_CMPND_INGR_CST"),
    F.trim(F.col("OCD_SUBMT_CMPND_BSS_OF_CST")).alias("OCD_SUBMT_CMPND_BSS_OF_CST"),
    F.trim(F.col("OCD_INGR_STTUS")).alias("OCD_INGR_STTUS"),
    F.when(
        (F.col("CMD_CALC_INGR_CST").isNull())
        | (F.col("CMD_CALC_INGR_CST") == "")
        | (F.col("CMD_CALC_INGR_CST") == " "),
        F.lit(0)
    ).otherwise(F.regexp_replace(F.col("CMD_CALC_INGR_CST"), " ", "")).alias("CMD_CALC_INGR_CST"),
    F.when(
        (F.col("CMD_APRV_INGR_CST").isNull())
        | (F.col("CMD_APRV_INGR_CST") == "")
        | (F.col("CMD_APRV_INGR_CST") == " "),
        F.lit(0)
    ).otherwise(F.regexp_replace(F.col("CMD_APRV_INGR_CST"), " ", "")).alias("CMD_APRV_INGR_CST"),
    F.when(
        (F.col("CMD_CLNT_INGR_CST").isNull())
        | (F.col("CMD_CLNT_INGR_CST") == "")
        | (F.col("CMD_CLNT_INGR_CST") == " "),
        F.lit(0)
    ).otherwise(F.regexp_replace(F.col("CMD_CLNT_INGR_CST"), " ", "")).alias("CMD_CLNT_INGR_CST"),
    F.trim(F.col("CMD_APRV_CST_SRC")).alias("CMD_APRV_CST_SRC"),
    F.trim(F.col("CMD_APRV_CST_TYP_CD")).alias("CMD_APRV_CST_TYP_CD"),
    F.when(
        (F.col("CMD_APRV_CST_TYP_UNIT_CST").isNull())
        | (F.col("CMD_APRV_CST_TYP_UNIT_CST") == "")
        | (F.col("CMD_APRV_CST_TYP_UNIT_CST") == " "),
        F.lit(0)
    ).otherwise(F.regexp_replace(F.col("CMD_APRV_CST_TYP_UNIT_CST"), " ", "")).alias("CMD_APRV_CST_TYP_UNIT_CST"),
    F.trim(F.col("CMD_CLNT_CST_SRC")).alias("CMD_CLNT_CST_SRC"),
    F.trim(F.col("CMD_CLNT_CST_TYP_CD")).alias("CMD_CLNT_CST_TYP_CD"),
    F.when(
        (F.col("CMD_CLNT_CST_TYP_UNIT_CST").isNull())
        | (F.col("CMD_CLNT_CST_TYP_UNIT_CST") == "")
        | (F.col("CMD_CLNT_CST_TYP_UNIT_CST") == " "),
        F.lit(0)
    ).otherwise(F.regexp_replace(F.col("CMD_CLNT_CST_TYP_UNIT_CST"), " ", "")).alias("CMD_CLNT_CST_TYP_UNIT_CST"),
    F.when(
        (F.col("CMD_AVG_WHLSL_PRICE_UNIT_CST").isNull())
        | (F.col("CMD_AVG_WHLSL_PRICE_UNIT_CST") == "")
        | (F.col("CMD_AVG_WHLSL_PRICE_UNIT_CST") == " "),
        F.lit(0)
    ).otherwise(F.regexp_replace(F.col("CMD_AVG_WHLSL_PRICE_UNIT_CST"), " ", "")).alias("CMD_AVG_WHLSL_PRICE_UNIT_CST"),
    F.when(
        (F.col("CMD_CALC_PROF_SVC_FEE_PD").isNull())
        | (F.col("CMD_CALC_PROF_SVC_FEE_PD") == "")
        | (F.col("CMD_CALC_PROF_SVC_FEE_PD") == " "),
        F.lit(0)
    ).otherwise(F.regexp_replace(F.col("CMD_CALC_PROF_SVC_FEE_PD"), " ", "")).alias("CMD_CALC_PROF_SVC_FEE_PD"),
    F.when(
        (F.col("CMD_APRV_PROF_SVC_FEE_PD").isNull())
        | (F.col("CMD_APRV_PROF_SVC_FEE_PD") == "")
        | (F.col("CMD_APRV_PROF_SVC_FEE_PD") == " "),
        F.lit(0)
    ).otherwise(F.regexp_replace(F.col("CMD_APRV_PROF_SVC_FEE_PD"), " ", "")).alias("CMD_APRV_PROF_SVC_FEE_PD"),
    F.when(
        (F.col("CMD_CLNT_PROF_SVC_FEE_PD").isNull())
        | (F.col("CMD_CLNT_PROF_SVC_FEE_PD") == "")
        | (F.col("CMD_CLNT_PROF_SVC_FEE_PD") == " "),
        F.lit(0)
    ).otherwise(F.regexp_replace(F.col("CMD_CLNT_PROF_SVC_FEE_PD"), " ", "")).alias("CMD_CLNT_PROF_SVC_FEE_PD"),
    F.trim(F.col("RBT_MNFCTR_IN")).alias("RBT_MNFCTR_IN"),
    F.trim(F.col("PRMT_EXCL_IN")).alias("PRMT_EXCL_IN"),
    F.trim(F.col("MED_SUPL_IN")).alias("MED_SUPL_IN"),
    F.trim(F.col("PDX_RATE")).alias("PDX_RATE"),
    F.trim(F.col("CLNT_RATE_PCT")).alias("CLNT_RATE_PCT"),
    F.trim(F.col("INGR_MOD_CD_CT")).alias("INGR_MOD_CD_CT"),
    F.lit(9999).alias("CONSTANT")
)

df_Claim_Reject_Status_temp = df_Compound_Supplemental_Claims.filter(
    (
        (F.col("CLM_STTUS") != "P")
        & (F.col("CLM_STTUS") != "X")
    )
    | (F.col("OCD_INGR_STTUS") == "R")
)

df_Claim_Reject_Status = df_Claim_Reject_Status_temp.select(
    F.col("RCXCLM_NO"),
    F.col("CLM_SEQ_NO"),
    F.col("CLM_STTUS"),
    F.col("OCD_CMPND_SEQ_NO"),
    F.col("OCD_SUBMT_CMPND_PROD_ID_QLFR"),
    F.col("OCD_CMPND_PROD_ID"),
    F.col("OCD_CMPND_PROD_KEY"),
    F.col("PROD_DESC_ABBR"),
    F.col("PROD_BRND_NM"),
    F.col("DRUG_MNFCTR_ID"),
    F.col("DRUG_MNFCTR"),
    F.col("CMD_GNRC_PROD_IN_NO"),
    F.col("DRUG_GNRC_NM"),
    F.col("DDID_DRUG_DSCRPTR_ID"),
    F.col("GCN_GNRC_CD_NO"),
    F.col("GCN_GNRC_CD_SEQ_NO"),
    F.col("PRD_KNW_BASED_DRUG_CD"),
    F.col("AMRCN_HOSP_FRMLRY_SVC_CLS_CD"),
    F.col("DEA_CD"),
    F.col("DRUG_RX_OVER_THE_CTR_IN"),
    F.col("MULTI_SRC_CD"),
    F.col("GNRC_IN_OVRD"),
    F.col("PROD_RMBRMT_IN"),
    F.col("BRND_NM_CD"),
    F.col("DRUG_FDA_THRPTC_EQVLNT"),
    F.col("DM_METRIC_STRG"),
    F.col("DRUG_STRG_UNIT_OF_MESR"),
    F.col("DRUG_ADM_RTE"),
    F.col("DRUG_DOSE_FORM"),
    F.col("MNTN_DRUG_CD"),
    F.col("DRUG_3RD_PARTY_EXCPT_CD"),
    F.col("DRUG_UNIT_DOSE"),
    F.col("OCD_SUBMT_CMPND_INGR_QTY"),
    F.col("OCD_SUBMT_CMPND_INGR_CST"),
    F.col("OCD_SUBMT_CMPND_BSS_OF_CST"),
    F.col("OCD_INGR_STTUS"),
    F.col("CMD_CALC_INGR_CST"),
    F.col("CMD_APRV_INGR_CST"),
    F.col("CMD_CLNT_INGR_CST"),
    F.col("CMD_APRV_CST_SRC"),
    F.col("CMD_APRV_CST_TYP_CD"),
    F.col("CMD_APRV_CST_TYP_UNIT_CST"),
    F.col("CMD_CLNT_CST_SRC"),
    F.col("CMD_CLNT_CST_TYP_CD"),
    F.col("CMD_CLNT_CST_TYP_UNIT_CST"),
    F.col("CMD_AVG_WHLSL_PRICE_UNIT_CST"),
    F.col("CMD_CALC_PROF_SVC_FEE_PD"),
    F.col("CMD_APRV_PROF_SVC_FEE_PD"),
    F.col("CMD_CLNT_PROF_SVC_FEE_PD"),
    F.col("RBT_MNFCTR_IN"),
    F.col("PRMT_EXCL_IN"),
    F.col("MED_SUPL_IN"),
    F.col("PDX_RATE"),
    F.col("CLNT_RATE_PCT"),
    F.col("INGR_MOD_CD_CT"),
    F.col("FILLER")
)

# Write "Drug_Clm_Cmpnd_Rejects" (PxSequentialFile)
# All columns are char except for no explicit mention of types? The job says all are "char" in metadata.
# We apply rpad for each with the length from the input metadata. We'll follow the column definitions:

df_Claim_Reject_Status_for_write = df_Claim_Reject_Status.select(
    rpad("RCXCLM_NO", 15, " ").alias("RCXCLM_NO"),
    rpad("CLM_SEQ_NO", 3, " ").alias("CLM_SEQ_NO"),
    rpad("CLM_STTUS", 1, " ").alias("CLM_STTUS"),
    rpad("OCD_CMPND_SEQ_NO", 3, " ").alias("OCD_CMPND_SEQ_NO"),
    rpad("OCD_SUBMT_CMPND_PROD_ID_QLFR", 2, " ").alias("OCD_SUBMT_CMPND_PROD_ID_QLFR"),
    rpad("OCD_CMPND_PROD_ID", 20, " ").alias("OCD_CMPND_PROD_ID"),
    rpad("OCD_CMPND_PROD_KEY", 9, " ").alias("OCD_CMPND_PROD_KEY"),
    rpad("PROD_DESC_ABBR", 30, " ").alias("PROD_DESC_ABBR"),
    rpad("PROD_BRND_NM", 70, " ").alias("PROD_BRND_NM"),
    rpad("DRUG_MNFCTR_ID", 5, " ").alias("DRUG_MNFCTR_ID"),
    rpad("DRUG_MNFCTR", 10, " ").alias("DRUG_MNFCTR"),
    rpad("CMD_GNRC_PROD_IN_NO", 14, " ").alias("CMD_GNRC_PROD_IN_NO"),
    rpad("DRUG_GNRC_NM", 60, " ").alias("DRUG_GNRC_NM"),
    rpad("DDID_DRUG_DSCRPTR_ID", 6, " ").alias("DDID_DRUG_DSCRPTR_ID"),
    rpad("GCN_GNRC_CD_NO", 5, " ").alias("GCN_GNRC_CD_NO"),
    rpad("GCN_GNRC_CD_SEQ_NO", 6, " ").alias("GCN_GNRC_CD_SEQ_NO"),
    rpad("PRD_KNW_BASED_DRUG_CD", 10, " ").alias("PRD_KNW_BASED_DRUG_CD"),
    rpad("AMRCN_HOSP_FRMLRY_SVC_CLS_CD", 8, " ").alias("AMRCN_HOSP_FRMLRY_SVC_CLS_CD"),
    rpad("DEA_CD", 1, " ").alias("DEA_CD"),
    rpad("DRUG_RX_OVER_THE_CTR_IN", 1, " ").alias("DRUG_RX_OVER_THE_CTR_IN"),
    rpad("MULTI_SRC_CD", 1, " ").alias("MULTI_SRC_CD"),
    rpad("GNRC_IN_OVRD", 1, " ").alias("GNRC_IN_OVRD"),
    rpad("PROD_RMBRMT_IN", 1, " ").alias("PROD_RMBRMT_IN"),
    rpad("BRND_NM_CD", 1, " ").alias("BRND_NM_CD"),
    rpad("DRUG_FDA_THRPTC_EQVLNT", 2, " ").alias("DRUG_FDA_THRPTC_EQVLNT"),
    rpad("DM_METRIC_STRG", 12, " ").alias("DM_METRIC_STRG"),
    rpad("DRUG_STRG_UNIT_OF_MESR", 10, " ").alias("DRUG_STRG_UNIT_OF_MESR"),
    rpad("DRUG_ADM_RTE", 2, " ").alias("DRUG_ADM_RTE"),
    rpad("DRUG_DOSE_FORM", 4, " ").alias("DRUG_DOSE_FORM"),
    rpad("MNTN_DRUG_CD", 1, " ").alias("MNTN_DRUG_CD"),
    rpad("DRUG_3RD_PARTY_EXCPT_CD", 1, " ").alias("DRUG_3RD_PARTY_EXCPT_CD"),
    rpad("DRUG_UNIT_DOSE", 1, " ").alias("DRUG_UNIT_DOSE"),
    rpad("OCD_SUBMT_CMPND_INGR_QTY", 12, " ").alias("OCD_SUBMT_CMPND_INGR_QTY"),
    rpad("OCD_SUBMT_CMPND_INGR_CST", 11, " ").alias("OCD_SUBMT_CMPND_INGR_CST"),
    rpad("OCD_SUBMT_CMPND_BSS_OF_CST", 2, " ").alias("OCD_SUBMT_CMPND_BSS_OF_CST"),
    rpad("OCD_INGR_STTUS", 1, " ").alias("OCD_INGR_STTUS"),
    rpad("CMD_CALC_INGR_CST", 11, " ").alias("CMD_CALC_INGR_CST"),
    rpad("CMD_APRV_INGR_CST", 11, " ").alias("CMD_APRV_INGR_CST"),
    rpad("CMD_CLNT_INGR_CST", 11, " ").alias("CMD_CLNT_INGR_CST"),
    rpad("CMD_APRV_CST_SRC", 1, " ").alias("CMD_APRV_CST_SRC"),
    rpad("CMD_APRV_CST_TYP_CD", 10, " ").alias("CMD_APRV_CST_TYP_CD"),
    rpad("CMD_APRV_CST_TYP_UNIT_CST", 14, " ").alias("CMD_APRV_CST_TYP_UNIT_CST"),
    rpad("CMD_CLNT_CST_SRC", 1, " ").alias("CMD_CLNT_CST_SRC"),
    rpad("CMD_CLNT_CST_TYP_CD", 10, " ").alias("CMD_CLNT_CST_TYP_CD"),
    rpad("CMD_CLNT_CST_TYP_UNIT_CST", 14, " ").alias("CMD_CLNT_CST_TYP_UNIT_CST"),
    rpad("CMD_AVG_WHLSL_PRICE_UNIT_CST", 14, " ").alias("CMD_AVG_WHLSL_PRICE_UNIT_CST"),
    rpad("CMD_CALC_PROF_SVC_FEE_PD", 11, " ").alias("CMD_CALC_PROF_SVC_FEE_PD"),
    rpad("CMD_APRV_PROF_SVC_FEE_PD", 11, " ").alias("CMD_APRV_PROF_SVC_FEE_PD"),
    rpad("CMD_CLNT_PROF_SVC_FEE_PD", 11, " ").alias("CMD_CLNT_PROF_SVC_FEE_PD"),
    rpad("RBT_MNFCTR_IN", 1, " ").alias("RBT_MNFCTR_IN"),
    rpad("PRMT_EXCL_IN", 1, " ").alias("PRMT_EXCL_IN"),
    rpad("MED_SUPL_IN", 1, " ").alias("MED_SUPL_IN"),
    rpad("PDX_RATE", 6, " ").alias("PDX_RATE"),
    rpad("CLNT_RATE_PCT", 6, " ").alias("CLNT_RATE_PCT"),
    rpad("INGR_MOD_CD_CT", 2, " ").alias("INGR_MOD_CD_CT"),
    rpad("FILLER", 20, " ").alias("FILLER")
)

write_files(
    df_Claim_Reject_Status_for_write,
    f"{adls_path}/load/DRUG_CLM_CMPND_REJECTS.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote=None,
    nullValue=None
)

# Read DB2ConnectorPX stages from IDS
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

extract_query_NDC = f"SELECT NDC_SK, NDC FROM {IDSOwner}.NDC WITH UR"
df_NDC = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_NDC)
    .load()
)

extract_query_Cd_Mppng = (
    f"SELECT CD_MPPNG_SK, 9999 as CONSTANT FROM {IDSOwner}.CD_MPPNG "
    "WHERE SRC_CD = 'OPTUMRX' "
    "AND SRC_DOMAIN_NM = 'SOURCE SYSTEM' "
    "AND SRC_CLCTN_CD = 'IDS' "
    "AND TRGT_DOMAIN_NM = 'SOURCE SYSTEM' "
    "AND TRGT_CLCTN_CD = 'IDS' WITH UR"
)
df_Cd_Mppng = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_Cd_Mppng)
    .load()
)

extract_query_FDA_Mppng = (
    f"SELECT CD_MPPNG_SK, SRC_CD FROM {IDSOwner}.CD_MPPNG "
    "WHERE SRC_CLCTN_CD   = 'OPTUMRX' "
    "AND SRC_SYS_CD     = 'OPTUMRX' "
    "AND SRC_DOMAIN_NM  = 'THERAPEUTIC EQUIVILENT' "
    "AND TRGT_CLCTN_CD  = 'IDS' "
    "AND TRGT_DOMAIN_NM = 'THERAPEUTIC EQUIVILENT' WITH UR"
)
df_FDA_Mppng = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_FDA_Mppng)
    .load()
)

extract_query_GO_Mppng = (
    f"SELECT CD_MPPNG_SK, SRC_CD FROM {IDSOwner}.CD_MPPNG "
    "WHERE SRC_CLCTN_CD   = 'OPTUMRX' "
    "AND SRC_SYS_CD     = 'OPTUMRX' "
    "AND SRC_DOMAIN_NM  = 'DRUG GENERIC OVERRIDE' "
    "AND TRGT_CLCTN_CD  = 'IDS' "
    "AND TRGT_DOMAIN_NM = 'DRUG GENERIC OVERRIDE' WITH UR"
)
df_GO_Mppng = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_GO_Mppng)
    .load()
)

extract_query_IS_Mppng = (
    f"SELECT CD_MPPNG_SK, SRC_CD FROM {IDSOwner}.CD_MPPNG "
    "WHERE SRC_CLCTN_CD   = 'OPTUMRX' "
    "AND SRC_SYS_CD     = 'OPTUMRX' "
    "AND SRC_DOMAIN_NM  = 'INGREDIENT STATUS' "
    "AND TRGT_CLCTN_CD  = 'IDS' "
    "AND TRGT_DOMAIN_NM = 'INGREDIENT STATUS' WITH UR"
)
df_IS_Mppng = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_IS_Mppng)
    .load()
)

extract_query_DA_Mppng = (
    f"SELECT CD_MPPNG_SK, SRC_CD FROM {IDSOwner}.CD_MPPNG "
    "WHERE SRC_CLCTN_CD   = 'OPTUMRX' "
    "AND SRC_SYS_CD     = 'OPTUMRX' "
    "AND SRC_DOMAIN_NM  = 'NDC DRUG ABUSE CONTROL' "
    "AND TRGT_CLCTN_CD  = 'IDS' "
    "AND TRGT_DOMAIN_NM = 'NDC DRUG ABUSE CONTROL' WITH UR"
)
df_DA_Mppng = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_DA_Mppng)
    .load()
)

extract_query_DC_Mppng = (
    f"SELECT CD_MPPNG_SK, SRC_CD FROM {IDSOwner}.CD_MPPNG "
    "WHERE SRC_CLCTN_CD   = 'OPTUMRX' "
    "AND SRC_SYS_CD     = 'OPTUMRX' "
    "AND SRC_DOMAIN_NM  = 'NDC DRUG CLASS' "
    "AND TRGT_CLCTN_CD  = 'IDS' "
    "AND TRGT_DOMAIN_NM = 'NDC DRUG CLASS' WITH UR"
)
df_DC_Mppng = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_DC_Mppng)
    .load()
)

extract_query_BN_Mppng = (
    f"SELECT CD_MPPNG_SK, SRC_CD FROM {IDSOwner}.CD_MPPNG "
    "WHERE SRC_CLCTN_CD   = 'OPTUMRX' "
    "AND SRC_SYS_CD     = 'OPTUMRX' "
    "AND SRC_DOMAIN_NM  = 'NDC GENERIC NAME' "
    "AND TRGT_CLCTN_CD  = 'IDS' "
    "AND TRGT_DOMAIN_NM = 'NDC GENERIC NAME' WITH UR"
)
df_BN_Mppng = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_BN_Mppng)
    .load()
)

extract_query_DAR_Mppng = (
    f"SELECT CD_MPPNG_SK, SRC_CD FROM {IDSOwner}.CD_MPPNG "
    "WHERE SRC_CLCTN_CD   = 'OPTUMRX' "
    "AND SRC_SYS_CD     = 'OPTUMRX' "
    "AND SRC_DOMAIN_NM  = 'NDC ROUTE TYPE' "
    "AND TRGT_CLCTN_CD  = 'IDS' "
    "AND TRGT_DOMAIN_NM = 'NDC ROUTE TYPE' WITH UR"
)
df_DAR_Mppng = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_DAR_Mppng)
    .load()
)

extract_query_CAC_Mppng = (
    f"SELECT CD_MPPNG_SK, SRC_CD FROM {IDSOwner}.CD_MPPNG "
    "WHERE SRC_CLCTN_CD   = 'OPTUMRX' "
    "AND SRC_SYS_CD     = 'OPTUMRX' "
    "AND SRC_DOMAIN_NM  = 'BUY COST SOURCE' "
    "AND TRGT_CLCTN_CD  = 'IDS' "
    "AND TRGT_DOMAIN_NM = 'BUY COST SOURCE' WITH UR"
)
df_CAC_Mppng = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_CAC_Mppng)
    .load()
)

extract_query_CCC_Mppng = (
    f"SELECT CD_MPPNG_SK, SRC_CD FROM {IDSOwner}.CD_MPPNG "
    "WHERE SRC_CLCTN_CD   = 'OPTUMRX' "
    "AND SRC_SYS_CD     = 'OPTUMRX' "
    "AND SRC_DOMAIN_NM  = 'BUY COST SOURCE' "
    "AND TRGT_CLCTN_CD  = 'IDS' "
    "AND TRGT_DOMAIN_NM = 'BUY COST SOURCE' WITH UR"
)
df_CCC_Mppng = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_CCC_Mppng)
    .load()
)

extract_query_D3P_Mppng = (
    f"SELECT CD_MPPNG_SK, SRC_CD FROM {IDSOwner}.CD_MPPNG "
    "WHERE SRC_CLCTN_CD   = 'OPTUMRX' "
    "AND SRC_SYS_CD     = 'OPTUMRX' "
    "AND SRC_DOMAIN_NM  = '3RD PARTY EXCEPTION' "
    "AND TRGT_CLCTN_CD  = 'IDS' "
    "AND TRGT_DOMAIN_NM = '3RD PARTY EXCEPTION' WITH UR"
)
df_D3P_Mppng = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_D3P_Mppng)
    .load()
)

extract_query_AHFS = (
    f"SELECT AHFS_TCC_SK, AHFS_TCC FROM {IDSOwner}.AHFS_TCC WITH UR"
)
df_AHFS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_AHFS)
    .load()
)

extract_query_PIQ_Mppng = (
    f"SELECT CD_MPPNG_SK, SRC_CD FROM {IDSOwner}.CD_MPPNG "
    "WHERE SRC_CLCTN_CD   = 'OPTUMRX' "
    "AND SRC_SYS_CD     = 'OPTUMRX' "
    "AND SRC_DOMAIN_NM  = 'SUBMITTED PRODUCT IDENTIFIER QUALIFIER' "
    "AND TRGT_CLCTN_CD  = 'IDS' "
    "AND TRGT_DOMAIN_NM = 'SUBMITTED PRODUCT IDENTIFIER QUALIFIER' WITH UR"
)
df_PIQ_Mppng = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_PIQ_Mppng)
    .load()
)

extract_query_DCLM_Mppng = (
    f"SELECT DRUG_CLM_SK, CLM_ID FROM {IDSOwner}.DRUG_CLM WITH UR"
)
df_DCLM_Mppng = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_DCLM_Mppng)
    .load()
)

# CCC_Lookup => multiple left joins
df_CCC_Lookup_joined = (
    df_Filter_Out
    .join(df_CCC_Mppng.alias("CCC_Mppng_Out"),
          F.col("Filter_Out.CMD_CLNT_CST_SRC") == F.col("CCC_Mppng_Out.SRC_CD"), "left")
    .join(df_CAC_Mppng.alias("CAC_Mppng_Out"),
          F.col("Filter_Out.CMD_APRV_CST_SRC") == F.col("CAC_Mppng_Out.SRC_CD"), "left")
    .join(df_DAR_Mppng.alias("DAR_Mppng_Out"),
          F.col("Filter_Out.DRUG_ADM_RTE") == F.col("DAR_Mppng_Out.SRC_CD"), "left")
    .join(df_BN_Mppng.alias("BN_Mppng_Out"),
          F.col("Filter_Out.BRND_NM_CD") == F.col("BN_Mppng_Out.SRC_CD"), "left")
    .join(df_DC_Mppng.alias("DC_Mppng_Out"),
          F.col("Filter_Out.DRUG_RX_OVER_THE_CTR_IN") == F.col("DC_Mppng_Out.SRC_CD"), "left")
    .join(df_DA_Mppng.alias("DA_Mppng_Out"),
          F.col("Filter_Out.DEA_CD") == F.col("DA_Mppng_Out.SRC_CD"), "left")
    .join(df_IS_Mppng.alias("IS_Mppng_Out"),
          F.col("Filter_Out.OCD_INGR_STTUS") == F.col("IS_Mppng_Out.SRC_CD"), "left")
    .join(df_GO_Mppng.alias("GO_Mppng_Out"),
          F.col("Filter_Out.GNRC_IN_OVRD") == F.col("GO_Mppng_Out.SRC_CD"), "left")
    .join(df_FDA_Mppng.alias("FDA_Mppng_Out"),
          F.col("Filter_Out.DRUG_FDA_THRPTC_EQVLNT") == F.col("FDA_Mppng_Out.SRC_CD"), "left")
    .join(df_D3P_Mppng.alias("D3P_Mppng_Out"),
          F.col("Filter_Out.DRUG_3RD_PARTY_EXCPT_CD") == F.col("D3P_Mppng_Out.SRC_CD"), "left")
    .join(df_AHFS.alias("AHFS_Out"),
          F.col("Filter_Out.AMRCN_HOSP_FRMLRY_SVC_CLS_CD") == F.col("AHFS_Out.AHFS_TCC"), "left")
    .join(df_NDC.alias("NDC_Out"),
          ((F.col("Filter_Out.OCD_CMPND_PROD_KEY") == F.col("NDC_Out.NDC_SK")) &
           (F.col("Filter_Out.OCD_CMPND_PROD_ID") == F.col("NDC_Out.NDC"))),
          "left")
    .join(df_Cd_Mppng.alias("Cd_Mppng_Out"),
          F.col("Filter_Out.CONSTANT") == F.col("Cd_Mppng_Out.CONSTANT"), "left")
    .join(df_PIQ_Mppng.alias("PIQ_Mppng"),
          F.col("Filter_Out.OCD_SUBMT_CMPND_PROD_ID_QLFR") == F.col("PIQ_Mppng.SRC_CD"), "left")
    .join(df_DCLM_Mppng.alias("DCLM_Mppng"),
          F.col("Filter_Out.CLM_ID") == F.col("DCLM_Mppng.CLM_ID"), "left")
)

df_CSC_Link = df_CCC_Lookup_joined.select(
    F.col("Filter_Out.RCXCLM_NO").alias("RCXCLM_NO"),
    F.col("Filter_Out.CLM_SEQ_NO").alias("CLM_SEQ_NO"),
    F.col("Filter_Out.CLM_STTUS").alias("CLM_STTUS"),
    F.col("Filter_Out.CLM_ID").alias("CLM_ID"),
    F.col("Filter_Out.OCD_CMPND_SEQ_NO").alias("OCD_CMPND_SEQ_NO"),
    F.col("Filter_Out.OCD_SUBMT_CMPND_PROD_ID_QLFR").alias("OCD_SUBMT_CMPND_PROD_ID_QLFR"),
    F.col("PIQ_Mppng.CD_MPPNG_SK").alias("PIQ_CD_MPPNG_SK"),
    F.col("Filter_Out.OCD_CMPND_PROD_ID").alias("OCD_CMPND_PROD_ID"),
    F.col("Filter_Out.OCD_CMPND_PROD_KEY").alias("OCD_CMPND_PROD_KEY"),
    F.col("Filter_Out.PROD_DESC_ABBR").alias("PROD_DESC_ABBR"),
    F.col("Filter_Out.PROD_BRND_NM").alias("PROD_BRND_NM"),
    F.col("Filter_Out.DRUG_MNFCTR_ID").alias("DRUG_MNFCTR_ID"),
    F.col("Filter_Out.DRUG_MNFCTR").alias("DRUG_MNFCTR"),
    F.col("Filter_Out.CMD_GNRC_PROD_IN_NO").alias("CMD_GNRC_PROD_IN_NO"),
    F.col("Filter_Out.DRUG_GNRC_NM").alias("DRUG_GNRC_NM"),
    F.col("Filter_Out.DDID_DRUG_DSCRPTR_ID").alias("DDID_DRUG_DSCRPTR_ID"),
    F.col("Filter_Out.GCN_GNRC_CD_NO").alias("GCN_GNRC_CD_NO"),
    F.col("Filter_Out.GCN_GNRC_CD_SEQ_NO").alias("GCN_GNRC_CD_SEQ_NO"),
    F.col("Filter_Out.PRD_KNW_BASED_DRUG_CD").alias("PRD_KNW_BASED_DRUG_CD"),
    F.col("Filter_Out.AMRCN_HOSP_FRMLRY_SVC_CLS_CD").alias("AMRCN_HOSP_FRMLRY_SVC_CLS_CD"),
    F.col("Filter_Out.DEA_CD").alias("DEA_CD"),
    F.col("DA_Mppng_Out.CD_MPPNG_SK").alias("DA_CD_MPPNG_SK"),
    F.col("Filter_Out.DRUG_RX_OVER_THE_CTR_IN").alias("DRUG_RX_OVER_THE_CTR_IN"),
    F.col("DC_Mppng_Out.CD_MPPNG_SK").alias("DC_CD_MPPNG_SK"),
    F.col("Filter_Out.MULTI_SRC_CD").alias("MULTI_SRC_CD"),
    F.col("Filter_Out.GNRC_IN_OVRD").alias("GNRC_IN_OVRD"),
    F.col("GO_Mppng_Out.CD_MPPNG_SK").alias("GO_CD_MPPNG_SK"),
    F.col("Filter_Out.PROD_RMBRMT_IN").alias("PROD_RMBRMT_IN"),
    F.col("Filter_Out.BRND_NM_CD").alias("BRND_NM_CD"),
    F.col("BN_Mppng_Out.CD_MPPNG_SK").alias("BN_CD_MPPNG_SK"),
    F.col("Filter_Out.DRUG_FDA_THRPTC_EQVLNT").alias("DRUG_FDA_THRPTC_EQVLNT"),
    F.col("FDA_Mppng_Out.CD_MPPNG_SK").alias("FDA_CD_MPPNG_SK"),
    F.col("Filter_Out.DM_METRIC_STRG").alias("DM_METRIC_STRG"),
    F.col("Filter_Out.DRUG_STRG_UNIT_OF_MESR").alias("DRUG_STRG_UNIT_OF_MESR"),
    F.col("Filter_Out.DRUG_ADM_RTE").alias("DRUG_ADM_RTE"),
    F.col("DAR_Mppng_Out.CD_MPPNG_SK").alias("DAR_CD_MPPNG_SK"),
    F.col("Filter_Out.DRUG_DOSE_FORM").alias("DRUG_DOSE_FORM"),
    F.col("Filter_Out.MNTN_DRUG_CD").alias("MNTN_DRUG_CD"),
    F.col("Filter_Out.DRUG_3RD_PARTY_EXCPT_CD").alias("DRUG_3RD_PARTY_EXCPT_CD"),
    F.col("D3P_Mppng_Out.CD_MPPNG_SK").alias("D3P_CD_MPPNG_SK"),
    F.col("Filter_Out.DRUG_UNIT_DOSE").alias("DRUG_UNIT_DOSE"),
    F.col("Filter_Out.OCD_SUBMT_CMPND_INGR_QTY").alias("OCD_SUBMT_CMPND_INGR_QTY"),
    F.col("Filter_Out.OCD_SUBMT_CMPND_INGR_CST").alias("OCD_SUBMT_CMPND_INGR_CST"),
    F.col("Filter_Out.OCD_SUBMT_CMPND_BSS_OF_CST").alias("OCD_SUBMT_CMPND_BSS_OF_CST"),
    F.col("Filter_Out.OCD_INGR_STTUS").alias("OCD_INGR_STTUS"),
    F.col("IS_Mppng_Out.CD_MPPNG_SK").alias("IS_CD_MPPNG_SK"),
    F.col("Filter_Out.CMD_CALC_INGR_CST").alias("CMD_CALC_INGR_CST"),
    F.col("Filter_Out.CMD_APRV_INGR_CST").alias("CMD_APRV_INGR_CST"),
    F.col("Filter_Out.CMD_CLNT_INGR_CST").alias("CMD_CLNT_INGR_CST"),
    F.col("Filter_Out.CMD_APRV_CST_SRC").alias("CMD_APRV_CST_SRC"),
    F.col("CAC_Mppng_Out.CD_MPPNG_SK").alias("CAC_CD_MPPNG_SK"),
    F.col("Filter_Out.CMD_APRV_CST_TYP_CD").alias("CMD_APRV_CST_TYP_CD"),
    F.col("Filter_Out.CMD_APRV_CST_TYP_UNIT_CST").alias("CMD_APRV_CST_TYP_UNIT_CST"),
    F.col("Filter_Out.CMD_CLNT_CST_SRC").alias("CMD_CLNT_CST_SRC"),
    F.col("CCC_Mppng_Out.CD_MPPNG_SK").alias("CCC_CD_MPPNG_SK"),
    F.col("Filter_Out.CMD_CLNT_CST_TYP_CD").alias("CMD_CLNT_CST_TYP_CD"),
    F.col("Filter_Out.CMD_CLNT_CST_TYP_UNIT_CST").alias("CMD_CLNT_CST_TYP_UNIT_CST"),
    F.col("Filter_Out.CMD_AVG_WHLSL_PRICE_UNIT_CST").alias("CMD_AVG_WHLSL_PRICE_UNIT_CST"),
    F.col("Filter_Out.CMD_CALC_PROF_SVC_FEE_PD").alias("CMD_CALC_PROF_SVC_FEE_PD"),
    F.col("Filter_Out.CMD_APRV_PROF_SVC_FEE_PD").alias("CMD_APRV_PROF_SVC_FEE_PD"),
    F.col("Filter_Out.CMD_CLNT_PROF_SVC_FEE_PD").alias("CMD_CLNT_PROF_SVC_FEE_PD"),
    F.col("Filter_Out.RBT_MNFCTR_IN").alias("RBT_MNFCTR_IN"),
    F.col("Filter_Out.PRMT_EXCL_IN").alias("PRMT_EXCL_IN"),
    F.col("Filter_Out.MED_SUPL_IN").alias("MED_SUPL_IN"),
    F.col("Filter_Out.PDX_RATE").alias("PDX_RATE"),
    F.col("Filter_Out.CLNT_RATE_PCT").alias("CLNT_RATE_PCT"),
    F.col("Filter_Out.INGR_MOD_CD_CT").alias("INGR_MOD_CD_CT"),
    F.col("Cd_Mppng_Out.CD_MPPNG_SK").alias("SRC_SYS_CD_SK_CONSTANT"),
    F.col("NDC_Out.NDC_SK").alias("NDC_SK"),
    F.col("AHFS_Out.AHFS_TCC_SK").alias("AHFS_TCC_SK"),
    F.col("DCLM_Mppng.DRUG_CLM_SK").alias("DRUG_CLM_SK")
)

# Next stage: clm_cmpnd_ingr => transform => "Drug_Clm_Cmpnd_Ingr_Link"
df_Drug_Clm_Cmpnd_Ingr_Link = df_CSC_Link.select(
    F.lit(0).alias("DRUG_CLM_CMPND_INGR_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("OCD_CMPND_SEQ_NO").alias("DRUG_CLM_CMPND_INGR_SEQ_NO"),
    F.col("SRC_SYS_CD_SK_CONSTANT").alias("SRC_SYS_CD_SK"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("DRUG_CLM_SK").alias("DRUG_CLM_SK"),
    F.when(
        (F.col("OCD_SUBMT_CMPND_PROD_ID_QLFR") == "03"),
        F.when(
            (F.col("NDC_SK") != 0),
            F.col("NDC_SK")
        ).otherwise(F.lit(0))
    ).otherwise(F.lit(1)).alias("NDC_SK"),
    F.when(
        (F.col("AHFS_TCC_SK") != 0),
        F.col("AHFS_TCC_SK")
    ).otherwise(F.lit(1)).alias("AHFS_TCC_SK"),
    F.when(
        (F.col("DRUG_3RD_PARTY_EXCPT_CD").isNull())
        | (F.col("DRUG_3RD_PARTY_EXCPT_CD") == "")
        | (F.col("DRUG_3RD_PARTY_EXCPT_CD") == " "),
        F.lit(1)
    ).otherwise(
        F.when(
            (F.col("D3P_CD_MPPNG_SK") != 0),
            F.col("D3P_CD_MPPNG_SK")
        ).otherwise(F.lit(0))
    ).alias("DRUG_3RD_PARTY_EXCPT_CD_SK"),
    F.when(
        (F.col("DRUG_FDA_THRPTC_EQVLNT").isNull())
        | (F.col("DRUG_FDA_THRPTC_EQVLNT") == "")
        | (F.col("DRUG_FDA_THRPTC_EQVLNT") == " ")
        | (F.col("DRUG_FDA_THRPTC_EQVLNT") == "  "),
        F.lit(1)
    ).otherwise(
        F.when(
            (F.col("FDA_CD_MPPNG_SK") != 0),
            F.col("FDA_CD_MPPNG_SK")
        ).otherwise(F.lit(0))
    ).alias("FDA_THRPTC_EQVLNT_CD_SK"),
    F.when(
        (F.col("GNRC_IN_OVRD").isNull())
        | (F.col("GNRC_IN_OVRD") == "")
        | (F.col("GNRC_IN_OVRD") == " "),
        F.lit(1)
    ).otherwise(
        F.when(
            (F.col("GO_CD_MPPNG_SK") != 0),
            F.col("GO_CD_MPPNG_SK")
        ).otherwise(F.lit(0))
    ).alias("GNRC_OVRD_CD_SK"),
    F.when(
        (F.col("OCD_INGR_STTUS").isNull())
        | (F.col("OCD_INGR_STTUS") == "")
        | (F.col("OCD_INGR_STTUS") == " "),
        F.lit(1)
    ).otherwise(
        F.when(
            (F.col("IS_CD_MPPNG_SK") != 0),
            F.col("IS_CD_MPPNG_SK")
        ).otherwise(F.lit(0))
    ).alias("INGR_STTUS_CD_SK"),
    F.lit(1).alias("KNW_BASED_DRUG_CD_SK"),
    F.when(
        (F.col("DEA_CD").isNull())
        | (F.col("DEA_CD") == "")
        | (F.col("DEA_CD") == " "),
        F.lit(1)
    ).otherwise(
        F.when(
            (F.col("DA_CD_MPPNG_SK") != 0),
            F.col("DA_CD_MPPNG_SK")
        ).otherwise(F.lit(0))
    ).alias("NDC_DRUG_ABUSE_CTL_CD_SK"),
    F.when(
        (F.col("DRUG_RX_OVER_THE_CTR_IN").isNull())
        | (F.col("DRUG_RX_OVER_THE_CTR_IN") == "")
        | (F.col("DRUG_RX_OVER_THE_CTR_IN") == " "),
        F.lit(1)
    ).otherwise(
        F.when(
            (F.col("DC_CD_MPPNG_SK") != 0),
            F.col("DC_CD_MPPNG_SK")
        ).otherwise(F.lit(0))
    ).alias("NDC_DRUG_CLS_CD_SK"),
    F.when(
        (F.col("DRUG_STRG_UNIT_OF_MESR").isNull())
        | (F.col("DRUG_STRG_UNIT_OF_MESR") == "")
        | (F.col("DRUG_STRG_UNIT_OF_MESR") == " ")
        | (F.col("DRUG_STRG_UNIT_OF_MESR") == "          "),
        F.lit(1)
    ).otherwise(F.col("DRUG_STRG_UNIT_OF_MESR")).alias("INGR_UNIT_OF_MESR_TX"),
    F.when(
        (F.col("BRND_NM_CD").isNull())
        | (F.col("BRND_NM_CD") == "")
        | (F.col("BRND_NM_CD") == " "),
        F.lit(1)
    ).otherwise(
        F.when(
            (F.col("BN_CD_MPPNG_SK") != 0),
            F.col("BN_CD_MPPNG_SK")
        ).otherwise(F.lit(0))
    ).alias("NDC_GNRC_NMD_DRUG_CD_SK"),
    F.when(
        (F.col("DRUG_ADM_RTE").isNull())
        | (F.col("DRUG_ADM_RTE") == "")
        | (F.col("DRUG_ADM_RTE") == " ")
        | (F.col("DRUG_ADM_RTE") == "  "),
        F.lit(1)
    ).otherwise(
        F.when(
            (F.col("DAR_CD_MPPNG_SK") != 0),
            F.col("DAR_CD_MPPNG_SK")
        ).otherwise(F.lit(0))
    ).alias("NDC_RTE_TYP_CD_SK"),
    F.col("PIQ_CD_MPPNG_SK").alias("SUBMT_PROD_ID_QLFR_CD_SK"),
    F.when(F.col("MNTN_DRUG_CD") == "X", F.lit("Y")).otherwise(F.lit("N")).alias("DRUG_MNTN_IN"),
    F.when(
        (F.col("MED_SUPL_IN").isNull())
        | (F.col("MED_SUPL_IN") == "")
        | (F.col("MED_SUPL_IN") == " ")
        | (F.col("MED_SUPL_IN") == "0"),
        F.lit("N")
    ).otherwise(F.lit("Y")).alias("MED_SUPL_IN"),
    F.when(
        (F.col("PRMT_EXCL_IN").isNull())
        | (F.col("PRMT_EXCL_IN") == "")
        | (F.col("PRMT_EXCL_IN") == " ")
        | (F.col("PRMT_EXCL_IN") == "0"),
        F.lit("N")
    ).otherwise(F.lit("Y")).alias("PRMT_EXCL_IN"),
    F.when(
        (F.col("PROD_RMBRMT_IN") == "1")
        | (F.col("PROD_RMBRMT_IN") == "2")
        | (F.col("PROD_RMBRMT_IN") == "3"),
        F.lit("Y")
    ).otherwise(F.lit("N")).alias("PROD_RMBRMT_IN"),
    F.when(F.col("RBT_MNFCTR_IN") == "1", F.lit("Y")).otherwise(F.lit("N")).alias("RBT_MNFCTR_IN"),
    F.when(
        (F.col("MULTI_SRC_CD") == "M") | (F.col("MULTI_SRC_CD") == "N"),
        F.lit("1")
    ).otherwise(
        F.when(F.col("MULTI_SRC_CD") == "Y", F.lit("3"))
        .otherwise(
            F.when(F.col("MULTI_SRC_CD") == "O", F.lit("5"))
            .otherwise(F.lit("0"))
        )
    ).alias("SNGL_SRC_IN"),
    F.when(F.col("DRUG_UNIT_DOSE") == "X", F.lit("Y")).otherwise(F.lit("N")).alias("UNIT_DOSE_IN"),
    F.when(F.col("DRUG_UNIT_DOSE") == "U", F.lit("Y")).otherwise(F.lit("N")).alias("UNIT_OF_USE_IN"),
    F.when(
        (F.col("CLM_STTUS") == "X") & (F.col("CMD_AVG_WHLSL_PRICE_UNIT_CST") > 0),
        F.col("CMD_AVG_WHLSL_PRICE_UNIT_CST") * -1
    ).otherwise(F.col("CMD_AVG_WHLSL_PRICE_UNIT_CST")).alias("AWP_UNIT_CST_AMT"),
    F.when(
        (F.col("CMD_APRV_CST_SRC").isNull())
        | (F.col("CMD_APRV_CST_SRC") == "")
        | (F.col("CMD_APRV_CST_SRC") == " "),
        F.lit(1)
    ).otherwise(
        F.when(F.col("CAC_CD_MPPNG_SK").isNotNull(), F.col("CAC_CD_MPPNG_SK")).otherwise(F.lit(0))
    ).alias("APRV_CST_SRC_CD_SK"),
    F.when(
        (F.col("CMD_APRV_CST_TYP_CD").isNull())
        | (F.col("CMD_APRV_CST_TYP_CD") == "")
        | (F.col("CMD_APRV_CST_TYP_CD") == " ")
        | (F.col("CMD_APRV_CST_TYP_CD") == "          "),
        F.lit("NA")
    ).otherwise(F.col("CMD_APRV_CST_TYP_CD")).alias("APRV_CST_TYP_ID"),
    F.when(
        (F.col("CLM_STTUS") == "X") & (F.col("CMD_APRV_CST_TYP_UNIT_CST") > 0),
        F.col("CMD_APRV_CST_TYP_UNIT_CST") * -1
    ).otherwise(F.col("CMD_APRV_CST_TYP_UNIT_CST")).alias("APRV_CST_TYP_UNIT_CST_AMT"),
    F.when(
        (F.col("CLM_STTUS") == "X") & (F.col("CMD_APRV_INGR_CST") > 0),
        F.col("CMD_APRV_INGR_CST") * -1
    ).otherwise(F.col("CMD_APRV_INGR_CST")).alias("APRV_INGR_CST_AMT"),
    F.when(
        (F.col("CLM_STTUS") == "X") & (F.col("CMD_APRV_PROF_SVC_FEE_PD") > 0),
        F.col("CMD_APRV_PROF_SVC_FEE_PD") * -1
    ).otherwise(F.col("CMD_APRV_PROF_SVC_FEE_PD")).alias("APRV_PROF_SVC_FEE_PD_AMT"),
    F.when(
        (F.col("CLM_STTUS") == "X") & (F.col("CMD_CALC_INGR_CST") > 0),
        F.col("CMD_CALC_INGR_CST") * -1
    ).otherwise(F.col("CMD_CALC_INGR_CST")).alias("CALC_INGR_CST_AMT"),
    F.when(
        (F.col("CLM_STTUS") == "X") & (F.col("CMD_CALC_PROF_SVC_FEE_PD") > 0),
        F.col("CMD_CALC_PROF_SVC_FEE_PD") * -1
    ).otherwise(F.col("CMD_CALC_PROF_SVC_FEE_PD")).alias("CALC_PROF_SVC_FEE_PD_AMT"),
    F.when(
        (F.col("CMD_CLNT_CST_SRC").isNull())
        | (F.col("CMD_CLNT_CST_SRC") == "")
        | (F.col("CMD_CLNT_CST_SRC") == " "),
        F.lit(1)
    ).otherwise(
        F.when(F.col("CCC_CD_MPPNG_SK").isNotNull(), F.col("CCC_CD_MPPNG_SK")).otherwise(F.lit(0))
    ).alias("CLNT_CST_SRC_CD_SK"),
    F.when(
        (F.col("CMD_CLNT_CST_TYP_CD").isNull())
        | (F.col("CMD_CLNT_CST_TYP_CD") == "")
        | (F.col("CMD_CLNT_CST_TYP_CD") == " ")
        | (F.col("CMD_CLNT_CST_TYP_CD") == "          "),
        F.lit("NA")
    ).otherwise(F.col("CMD_CLNT_CST_TYP_CD")).alias("CLNT_CST_TYP_ID"),
    F.when(
        (F.col("CLM_STTUS") == "X") & (F.col("CMD_CLNT_CST_TYP_UNIT_CST") > 0),
        F.col("CMD_CLNT_CST_TYP_UNIT_CST") * -1
    ).otherwise(F.col("CMD_CLNT_CST_TYP_UNIT_CST")).alias("CLNT_CST_TYP_UNIT_CST_AMT"),
    F.when(
        (F.col("CLM_STTUS") == "X") & (F.col("CMD_CLNT_INGR_CST") > 0),
        F.col("CMD_CLNT_INGR_CST") * -1
    ).otherwise(F.col("CMD_CLNT_INGR_CST")).alias("CLNT_INGR_CST_AMT"),
    F.when(
        (F.col("CLM_STTUS") == "X") & (F.col("CMD_CLNT_PROF_SVC_FEE_PD") > 0),
        F.col("CMD_CLNT_PROF_SVC_FEE_PD") * -1
    ).otherwise(F.col("CMD_CLNT_PROF_SVC_FEE_PD")).alias("CLNT_PROF_SVC_FEE_PD_AMT"),
    F.when(
        (F.col("CLNT_RATE_PCT").isNull())
        | (F.col("CLNT_RATE_PCT") == "")
        | (F.col("CLNT_RATE_PCT") == " ")
        | (F.col("CLNT_RATE_PCT") == "      "),
        F.lit(0)
    ).otherwise(F.col("CLNT_RATE_PCT")).alias("CLNT_RATE_PCT"),
    F.when(
        (F.col("OCD_SUBMT_CMPND_BSS_OF_CST").isNull())
        | (F.col("OCD_SUBMT_CMPND_BSS_OF_CST") == "")
        | (F.col("OCD_SUBMT_CMPND_BSS_OF_CST") == " ")
        | (F.col("OCD_SUBMT_CMPND_BSS_OF_CST") == "  "),
        F.lit("NA")
    ).otherwise(F.col("OCD_SUBMT_CMPND_BSS_OF_CST")).alias("SUBMT_CMPND_CST_TYP_ID"),
    F.when(
        (F.col("CLM_STTUS") == "X") & (F.col("OCD_SUBMT_CMPND_INGR_CST") > 0),
        F.col("OCD_SUBMT_CMPND_INGR_CST") * -1
    ).otherwise(F.col("OCD_SUBMT_CMPND_INGR_CST")).alias("SUBMT_CMPND_INGR_CST_AMT"),
    F.when(
        (F.col("CLM_STTUS") == "X") & (F.col("OCD_SUBMT_CMPND_INGR_QTY") > 0),
        F.col("OCD_SUBMT_CMPND_INGR_QTY") * -1
    ).otherwise(F.col("OCD_SUBMT_CMPND_INGR_QTY")).alias("SUBMT_CMPND_INGR_QTY"),
    F.when(
        (F.col("PDX_RATE").isNull())
        | (F.col("PDX_RATE") == "")
        | (F.col("PDX_RATE") == " ")
        | (F.col("PDX_RATE") == "      "),
        F.lit(0)
    ).otherwise(F.col("PDX_RATE")).alias("PDX_RATE_PCT"),
    F.when(
        (F.col("DM_METRIC_STRG").isNull())
        | (F.col("DM_METRIC_STRG") == "")
        | (F.col("DM_METRIC_STRG") == " ")
        | (F.col("DM_METRIC_STRG") == "            "),
        F.lit(0)
    ).otherwise(F.col("DM_METRIC_STRG")).alias("DRUG_METRIC_STRG_NO"),
    F.lit(1).alias("INGR_MOD_CD_CT"),
    F.when(
        (F.col("PROD_BRND_NM").isNull()) | (F.col("PROD_BRND_NM") == ""),
        F.lit(" ")
    ).otherwise(F.col("PROD_BRND_NM")).alias("CMPND_PROD_BRND_NM"),
    F.when(
        (F.col("OCD_CMPND_PROD_ID").isNull())
        | (F.col("OCD_CMPND_PROD_ID") == "")
        | (F.col("OCD_CMPND_PROD_ID") == " ")
        | (F.col("OCD_CMPND_PROD_ID") == "                    "),
        F.lit("NA")
    ).otherwise(F.col("OCD_CMPND_PROD_ID")).alias("CMPND_PROD_ID"),
    F.when(
        (F.col("DDID_DRUG_DSCRPTR_ID").isNull())
        | (F.col("DDID_DRUG_DSCRPTR_ID") == "")
        | (F.col("DDID_DRUG_DSCRPTR_ID") == " ")
        | (F.col("DDID_DRUG_DSCRPTR_ID") == "      "),
        F.lit("NA")
    ).otherwise(F.col("DDID_DRUG_DSCRPTR_ID")).alias("DRUG_DSCRPTR_ID"),
    F.when(
        (F.col("PROD_DESC_ABBR").isNull()) | (F.col("PROD_DESC_ABBR") == ""),
        F.lit(" ")
    ).otherwise(F.col("PROD_DESC_ABBR")).alias("DRUG_LABEL_NM"),
    F.when(
        (F.col("DRUG_GNRC_NM").isNull()) | (F.col("DRUG_GNRC_NM") == ""),
        F.lit(" ")
    ).otherwise(F.col("DRUG_GNRC_NM")).alias("GNRC_NM_SH_DESC"),
    F.when(
        (F.col("CMD_GNRC_PROD_IN_NO").isNull())
        | (F.col("CMD_GNRC_PROD_IN_NO") == "")
        | (F.col("CMD_GNRC_PROD_IN_NO") == " ")
        | (F.col("CMD_GNRC_PROD_IN_NO") == "              "),
        F.lit("NA")
    ).otherwise(F.col("CMD_GNRC_PROD_IN_NO")).alias("GNRC_PROD_ID"),
    F.when(
        (F.col("DRUG_MNFCTR").isNull()) | (F.col("DRUG_MNFCTR") == ""),
        F.lit(" ")
    ).otherwise(F.col("DRUG_MNFCTR")).alias("LBLR_NM"),
    F.when(
        (F.col("DRUG_MNFCTR_ID").isNull()) | (F.col("DRUG_MNFCTR_ID") == ""),
        F.lit(" ")
    ).otherwise(F.col("DRUG_MNFCTR_ID")).alias("LBLR_NO"),
    F.when(
        (F.col("GCN_GNRC_CD_NO").isNull())
        | (F.col("GCN_GNRC_CD_NO") == "")
        | (F.col("GCN_GNRC_CD_NO") == " ")
        | (F.col("GCN_GNRC_CD_NO") == "     ")
        | (F.col("GCN_GNRC_CD_NO") == "00000"),
        F.lit("UNK")
    ).otherwise(F.col("GCN_GNRC_CD_NO")).alias("NDC_GCN_CD_TX"),
    F.when(
        (F.col("GCN_GNRC_CD_SEQ_NO").isNull())
        | (F.col("GCN_GNRC_CD_SEQ_NO") == "")
        | (F.col("GCN_GNRC_CD_SEQ_NO") == " ")
        | (F.col("GCN_GNRC_CD_SEQ_NO") == "      ")
        | (F.col("GCN_GNRC_CD_SEQ_NO") == "000000"),
        F.lit(0)
    ).otherwise(F.col("GCN_GNRC_CD_SEQ_NO")).alias("GCN_SEQ_NO")
)

# Copy_Compounds => 2 outputs

df_Compound_Link = df_Drug_Clm_Cmpnd_Ingr_Link.select(
    F.col("DRUG_CLM_CMPND_INGR_SK"),
    F.col("CLM_ID"),
    F.col("DRUG_CLM_CMPND_INGR_SEQ_NO"),
    F.col("SRC_SYS_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("DRUG_CLM_SK"),
    F.col("NDC_SK"),
    F.col("AHFS_TCC_SK"),
    F.col("DRUG_3RD_PARTY_EXCPT_CD_SK"),
    F.col("FDA_THRPTC_EQVLNT_CD_SK"),
    F.col("GNRC_OVRD_CD_SK"),
    F.col("INGR_STTUS_CD_SK"),
    F.col("KNW_BASED_DRUG_CD_SK"),
    F.col("NDC_DRUG_ABUSE_CTL_CD_SK"),
    F.col("NDC_DRUG_CLS_CD_SK"),
    F.col("INGR_UNIT_OF_MESR_TX"),
    F.col("NDC_GNRC_NMD_DRUG_CD_SK"),
    F.col("NDC_RTE_TYP_CD_SK"),
    F.col("SUBMT_PROD_ID_QLFR_CD_SK"),
    F.col("DRUG_MNTN_IN"),
    F.col("MED_SUPL_IN"),
    F.col("PRMT_EXCL_IN"),
    F.col("PROD_RMBRMT_IN"),
    F.col("RBT_MNFCTR_IN"),
    F.col("SNGL_SRC_IN"),
    F.col("UNIT_DOSE_IN"),
    F.col("UNIT_OF_USE_IN"),
    F.col("AWP_UNIT_CST_AMT"),
    F.col("APRV_CST_SRC_CD_SK"),
    F.col("APRV_CST_TYP_ID"),
    F.col("APRV_CST_TYP_UNIT_CST_AMT"),
    F.col("APRV_INGR_CST_AMT"),
    F.col("APRV_PROF_SVC_FEE_PD_AMT"),
    F.col("CALC_INGR_CST_AMT"),
    F.col("CALC_PROF_SVC_FEE_PD_AMT"),
    F.col("CLNT_CST_SRC_CD_SK"),
    F.col("CLNT_CST_TYP_ID"),
    F.col("CLNT_CST_TYP_UNIT_CST_AMT"),
    F.col("CLNT_INGR_CST_AMT"),
    F.col("CLNT_PROF_SVC_FEE_PD_AMT"),
    F.col("CLNT_RATE_PCT"),
    F.col("SUBMT_CMPND_CST_TYP_ID"),
    F.col("SUBMT_CMPND_INGR_CST_AMT"),
    F.col("SUBMT_CMPND_INGR_QTY"),
    F.col("PDX_RATE_PCT"),
    F.col("DRUG_METRIC_STRG_NO"),
    F.col("INGR_MOD_CD_CT"),
    F.col("CMPND_PROD_BRND_NM"),
    F.col("CMPND_PROD_ID"),
    F.col("DRUG_DSCRPTR_ID"),
    F.col("DRUG_LABEL_NM"),
    F.col("GNRC_NM_SH_DESC"),
    F.col("GNRC_PROD_ID"),
    F.col("LBLR_NM"),
    F.col("LBLR_NO"),
    F.col("NDC_GCN_CD_TX"),
    F.col("GCN_SEQ_NO")
)

df_Compound_Debug_Link = df_Drug_Clm_Cmpnd_Ingr_Link.select(
    F.col("DRUG_CLM_CMPND_INGR_SK"),
    F.col("CLM_ID"),
    F.col("DRUG_CLM_CMPND_INGR_SEQ_NO"),
    F.col("SRC_SYS_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("DRUG_CLM_SK"),
    F.col("NDC_SK"),
    F.col("AHFS_TCC_SK"),
    F.col("DRUG_3RD_PARTY_EXCPT_CD_SK"),
    F.col("FDA_THRPTC_EQVLNT_CD_SK"),
    F.col("GNRC_OVRD_CD_SK"),
    F.col("INGR_STTUS_CD_SK"),
    F.col("KNW_BASED_DRUG_CD_SK"),
    F.col("NDC_DRUG_ABUSE_CTL_CD_SK"),
    F.col("NDC_DRUG_CLS_CD_SK"),
    F.col("INGR_UNIT_OF_MESR_TX"),
    F.col("NDC_GNRC_NMD_DRUG_CD_SK"),
    F.col("NDC_RTE_TYP_CD_SK"),
    F.col("SUBMT_PROD_ID_QLFR_CD_SK"),
    F.col("DRUG_MNTN_IN"),
    F.col("MED_SUPL_IN"),
    F.col("PRMT_EXCL_IN"),
    F.col("PROD_RMBRMT_IN"),
    F.col("RBT_MNFCTR_IN"),
    F.col("SNGL_SRC_IN"),
    F.col("UNIT_DOSE_IN"),
    F.col("UNIT_OF_USE_IN"),
    F.col("AWP_UNIT_CST_AMT"),
    F.col("APRV_CST_SRC_CD_SK"),
    F.col("APRV_CST_TYP_ID"),
    F.col("APRV_CST_TYP_UNIT_CST_AMT"),
    F.col("APRV_INGR_CST_AMT"),
    F.col("APRV_PROF_SVC_FEE_PD_AMT"),
    F.col("CALC_INGR_CST_AMT"),
    F.col("CALC_PROF_SVC_FEE_PD_AMT"),
    F.col("CLNT_CST_SRC_CD_SK"),
    F.col("CLNT_CST_TYP_ID"),
    F.col("CLNT_CST_TYP_UNIT_CST_AMT"),
    F.col("CLNT_INGR_CST_AMT"),
    F.col("CLNT_PROF_SVC_FEE_PD_AMT"),
    F.col("CLNT_RATE_PCT"),
    F.col("SUBMT_CMPND_CST_TYP_ID"),
    F.col("SUBMT_CMPND_INGR_CST_AMT"),
    F.col("SUBMT_CMPND_INGR_QTY"),
    F.col("PDX_RATE_PCT"),
    F.col("DRUG_METRIC_STRG_NO"),
    F.col("INGR_MOD_CD_CT"),
    F.col("CMPND_PROD_BRND_NM"),
    F.col("CMPND_PROD_ID"),
    F.col("DRUG_DSCRPTR_ID"),
    F.col("DRUG_LABEL_NM"),
    F.col("GNRC_NM_SH_DESC"),
    F.col("GNRC_PROD_ID"),
    F.col("LBLR_NM"),
    F.col("LBLR_NO"),
    F.col("NDC_GCN_CD_TX"),
    F.col("GCN_SEQ_NO")
)

# Drug_Clm_Cmpnd_Ingr_PKey_Debug => write file
df_Compound_Debug_Link_for_write = df_Compound_Debug_Link  # no char length specs given except "header=true"
write_files(
    df_Compound_Debug_Link_for_write,
    f"{adls_path}/key/DRUG_CLM_CMPND_INGR_PKey_DEBUG.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=True,
    quote=None,
    nullValue=None
)

# Drug_Clm_Cmpnd_Ingr_PKey => dataset => must be parquet
df_Compound_Link_for_write = df_Compound_Link
write_files(
    df_Compound_Link_for_write,
    f"{adls_path}/ds/DRUG_CLM_CMPND_INGR_PKey.{RunID}.parquet",
    delimiter=",",
    mode="overwrite",
    is_parquet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# Copy_NDC => 2 outputs

df_NDC_Table_Link = df_CSC_Link.select(
    F.lit(0).alias("NDC_SK"),
    F.col("OCD_CMPND_PROD_ID").alias("NDC"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("AHFS_TCC_SK").alias("AHFS_TCC_SK"),
    F.lit("1").alias("DOSE_FORM_SK"),
    F.lit(0).alias("TCC_SK"),
    F.lit(0).alias("NDC_DSM_DRUG_TYP_CD_SK"),
    F.when(
        (F.col("DEA_CD").isNull())
        | (F.col("DEA_CD") == "")
        | (F.col("DEA_CD") == " "),
        F.lit(1)
    ).otherwise(
        F.when(F.col("DA_CD_MPPNG_SK").isNotNull(), F.col("DA_CD_MPPNG_SK")).otherwise(F.lit(0))
    ).alias("NDC_DRUG_ABUSE_CTL_CD_SK"),
    F.when(
        (F.col("DRUG_RX_OVER_THE_CTR_IN").isNull())
        | (F.col("DRUG_RX_OVER_THE_CTR_IN") == "")
        | (F.col("DRUG_RX_OVER_THE_CTR_IN") == " "),
        F.lit(1)
    ).otherwise(
        F.when(F.col("DC_CD_MPPNG_SK").isNotNull(), F.col("DC_CD_MPPNG_SK")).otherwise(F.lit(0))
    ).alias("NDC_DRUG_CLS_CD_SK"),
    F.lit(1).alias("NDC_DRUG_FORM_CD_SK"),
    F.lit(0).alias("NDC_FMT_CD_SK"),
    F.lit(0).alias("NDC_GNRC_MNFCTR_CD_SK"),
    F.when(
        (F.col("BRND_NM_CD").isNull())
        | (F.col("BRND_NM_CD") == "")
        | (F.col("BRND_NM_CD") == " "),
        F.lit(1)
    ).otherwise(
        F.when(F.col("BN_CD_MPPNG_SK").isNotNull(), F.col("BN_CD_MPPNG_SK")).otherwise(F.lit(0))
    ).alias("NDC_GNRC_NMD_DRUG_CD_SK"),
    F.lit(0).alias("NDC_GNRC_PRICE_CD_SK"),
    F.lit(0).alias("NDC_GNRC_PRICE_SPREAD_CD_SK"),
    F.lit(0).alias("NDC_ORANGE_BOOK_CD_SK"),
    F.when(
        (F.col("DRUG_ADM_RTE").isNull())
        | (F.col("DRUG_ADM_RTE") == "")
        | (F.col("DRUG_ADM_RTE") == " ")
        | (F.col("DRUG_ADM_RTE") == "  "),
        F.lit(1)
    ).otherwise(
        F.when(F.col("DAR_CD_MPPNG_SK").isNotNull(), F.col("DAR_CD_MPPNG_SK")).otherwise(F.lit(0))
    ).alias("NDC_RTE_TYP_CD_SK"),
    F.lit("N").alias("CLM_TRANS_ADD_IN"),
    F.lit("N").alias("DESI_DRUG_IN"),
    F.when(F.col("MNTN_DRUG_CD") == "X", F.lit("Y")).otherwise(F.lit("N")).alias("DRUG_MNTN_IN"),
    F.lit("N").alias("INNVTR_IN"),
    F.lit("N").alias("INSTUT_PROD_IN"),
    F.lit("N").alias("PRIV_LBLR_IN"),
    F.when(
        (F.col("MULTI_SRC_CD") == "M") | (F.col("MULTI_SRC_CD") == "N"),
        F.lit("1")
    ).otherwise(
        F.when(F.col("MULTI_SRC_CD") == "Y", F.lit("3"))
        .otherwise(
            F.when(F.col("MULTI_SRC_CD") == "O", F.lit("5"))
            .otherwise(F.lit("0"))
        )
    ).alias("SNGL_SRC_IN"),
    F.when(F.col("DRUG_UNIT_DOSE") == "X", F.lit("Y")).otherwise(F.lit("N")).alias("UNIT_DOSE_IN"),
    F.when(F.col("DRUG_UNIT_DOSE") == "U", F.lit("Y")).otherwise(F.lit("N")).alias("UNIT_OF_USE_IN"),
    F.lit("1753-01-01").alias("AVG_WHLSL_PRICE_CHG_DT_SK"),
    F.lit("1753-01-01").alias("GNRC_PRICE_IN_CHG_DT_SK"),
    F.lit("1753-01-01").alias("OBSLT_DT_SK"),
    F.lit("1753-01-01").alias("SRC_NDC_CRT_DT_SK"),
    F.lit("1753-01-01").alias("SRC_NDC_UPDT_DT_SK")
).filter(
    (F.col("OCD_SUBMT_CMPND_PROD_ID_QLFR") == "03")
    & (
        (F.col("NDC_SK").isNull()) | (F.col("NDC_SK") == 0)
    )
)

df_NDC_Debug_Link = df_NDC_Table_Link.select(
    F.col("NDC_SK"),
    F.col("NDC"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("AHFS_TCC_SK"),
    F.col("DOSE_FORM_SK"),
    F.col("TCC_SK"),
    F.col("NDC_DSM_DRUG_TYP_CD_SK"),
    F.col("NDC_DRUG_ABUSE_CTL_CD_SK"),
    F.col("NDC_DRUG_CLS_CD_SK"),
    F.col("NDC_DRUG_FORM_CD_SK"),
    F.col("NDC_FMT_CD_SK"),
    F.col("NDC_GNRC_MNFCTR_CD_SK"),
    F.col("NDC_GNRC_NMD_DRUG_CD_SK"),
    F.col("NDC_GNRC_PRICE_CD_SK"),
    F.col("NDC_GNRC_PRICE_SPREAD_CD_SK"),
    F.col("NDC_ORANGE_BOOK_CD_SK"),
    F.col("NDC_RTE_TYP_CD_SK"),
    F.col("CLM_TRANS_ADD_IN"),
    F.col("DESI_DRUG_IN"),
    F.col("DRUG_MNTN_IN"),
    F.col("INNVTR_IN"),
    F.col("INSTUT_PROD_IN"),
    F.col("PRIV_LBLR_IN"),
    F.col("SNGL_SRC_IN"),
    F.col("UNIT_DOSE_IN"),
    F.col("UNIT_OF_USE_IN"),
    F.col("AVG_WHLSL_PRICE_CHG_DT_SK"),
    F.col("GNRC_PRICE_IN_CHG_DT_SK"),
    F.col("OBSLT_DT_SK"),
    F.col("SRC_NDC_CRT_DT_SK"),
    F.col("SRC_NDC_UPDT_DT_SK")
)

df_NDC_Link = df_NDC_Table_Link.select(
    F.col("NDC_SK"),
    F.col("NDC"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("AHFS_TCC_SK"),
    F.col("DOSE_FORM_SK"),
    F.col("TCC_SK"),
    F.col("NDC_DSM_DRUG_TYP_CD_SK"),
    F.col("NDC_DRUG_ABUSE_CTL_CD_SK"),
    F.col("NDC_DRUG_CLS_CD_SK"),
    F.col("NDC_DRUG_FORM_CD_SK"),
    F.col("NDC_FMT_CD_SK"),
    F.col("NDC_GNRC_MNFCTR_CD_SK"),
    F.col("NDC_GNRC_NMD_DRUG_CD_SK"),
    F.col("NDC_GNRC_PRICE_CD_SK"),
    F.col("NDC_GNRC_PRICE_SPREAD_CD_SK"),
    F.col("NDC_ORANGE_BOOK_CD_SK"),
    F.col("NDC_RTE_TYP_CD_SK"),
    F.col("CLM_TRANS_ADD_IN"),
    F.col("DESI_DRUG_IN"),
    F.col("DRUG_MNTN_IN"),
    F.col("INNVTR_IN"),
    F.col("INSTUT_PROD_IN"),
    F.col("PRIV_LBLR_IN"),
    F.col("SNGL_SRC_IN"),
    F.col("UNIT_DOSE_IN"),
    F.col("UNIT_OF_USE_IN"),
    F.col("AVG_WHLSL_PRICE_CHG_DT_SK"),
    F.col("GNRC_PRICE_IN_CHG_DT_SK"),
    F.col("OBSLT_DT_SK"),
    F.col("SRC_NDC_CRT_DT_SK"),
    F.col("SRC_NDC_UPDT_DT_SK")
)

# Drug_Clm_Cmpnd_NDC_PKey_Debug => write
write_files(
    df_NDC_Debug_Link,
    f"{adls_path}/key/DRUG_CLM_CMPND_NDC_Pkey_DEBUG.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=True,
    quote=None,
    nullValue=None
)

# Drug_Clm_Cmpnd_NDC_PKey => .dat
df_NDC_Link_for_write = df_NDC_Link.select(
    rpad("NDC_SK", 0, " ").alias("NDC_SK"),  # The job metadata shows NDC_SK primaryKey= true but not char length. Keep as is or rpad(??).
    "NDC",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "AHFS_TCC_SK",
    "DOSE_FORM_SK",
    "TCC_SK",
    "NDC_DSM_DRUG_TYP_CD_SK",
    "NDC_DRUG_ABUSE_CTL_CD_SK",
    "NDC_DRUG_CLS_CD_SK",
    "NDC_DRUG_FORM_CD_SK",
    "NDC_FMT_CD_SK",
    "NDC_GNRC_MNFCTR_CD_SK",
    "NDC_GNRC_NMD_DRUG_CD_SK",
    "NDC_GNRC_PRICE_CD_SK",
    "NDC_GNRC_PRICE_SPREAD_CD_SK",
    "NDC_ORANGE_BOOK_CD_SK",
    "NDC_RTE_TYP_CD_SK",
    "CLM_TRANS_ADD_IN",
    "DESI_DRUG_IN",
    "DRUG_MNTN_IN",
    "INNVTR_IN",
    "INSTUT_PROD_IN",
    "PRIV_LBLR_IN",
    "SNGL_SRC_IN",
    "UNIT_DOSE_IN",
    "UNIT_OF_USE_IN",
    "AVG_WHLSL_PRICE_CHG_DT_SK",
    "GNRC_PRICE_IN_CHG_DT_SK",
    "OBSLT_DT_SK",
    "SRC_NDC_CRT_DT_SK",
    "SRC_NDC_UPDT_DT_SK"
)

write_files(
    df_NDC_Link_for_write,
    f"{adls_path}/key/DRUG_CLM_CMPND_NDC_PKey.dat",
    delimiter="^",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote=None,
    nullValue=None
)