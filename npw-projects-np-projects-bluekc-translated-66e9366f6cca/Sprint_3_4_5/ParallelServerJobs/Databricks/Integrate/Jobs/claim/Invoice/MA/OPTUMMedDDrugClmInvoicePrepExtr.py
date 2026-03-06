# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC ********************************************************************************************
# MAGIC COPYRIGHT 2020 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC CALLED BY: OPTUMMedDDrugInvoiceUpdateSeq
# MAGIC 
# MAGIC PROCESSING:  Takes drug file from OPTUMRX and strips out single quotes if any and move the file to the /verifid directory 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS
# MAGIC Developer                Date                 Prjoect / TTR                             Change Description                                                                 Development Project                    Code Reviewer          Date Reviewed          
# MAGIC ------------------              --------------------    -----------------------                             -------------------------------------------------------------------------------                --------------------------------                   -------------------------------     ----------------------------       
# MAGIC Velmani Kondappan    2020-10-20      6264 - PBM PHASE II - Government Programs                      Initial Development                    IntegrateDev2    	Abhiram Dasarathy	2020-12-10

# MAGIC This job reads the Bi Monthly Invoice File Claim_Detail_BKCMEDD_*****_ARXLD.csv, clean up the file and create the Invoice input File in the path ../../verified/
# MAGIC Called from OPTUMMedDDrugInvoiceUpdateSeq
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col, upper, when, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


RunID = get_widget_value('RunID','')
CurrentDate = get_widget_value('CurrentDate','')
FileName = get_widget_value('FileName','')

# 1) Read from OPTUMRX_InPutFile (CSeqFileStage)
schema_OPTUMRX_InPutFile = StructType([
    StructField("INVC_NO", DoubleType(), True),
    StructField("INVC_DT", DoubleType(), True),
    StructField("BILL_ENTY_ID", StringType(), True),
    StructField("BILL_ENTY_NM", StringType(), True),
    StructField("CAR_ID", StringType(), True),
    StructField("CAR_NM", StringType(), True),
    StructField("ACCT_ID", StringType(), True),
    StructField("ACCT_NM", StringType(), True),
    StructField("GRP_ID", StringType(), True),
    StructField("GRP_NM", StringType(), True),
    StructField("RXCLM_NO", DoubleType(), True),
    StructField("CLM_SEQ_NO", DoubleType(), True),
    StructField("CLM_STTUS", StringType(), True),
    StructField("CLM_CLS_SUB_CD", StringType(), True),
    StructField("CLM_SUBMSN_TYP", StringType(), True),
    StructField("TAX_CD", StringType(), True),
    StructField("NET_CLM_CT", DoubleType(), True),
    StructField("PDX_NCPDP_ID", DoubleType(), True),
    StructField("PDX_NPI_ID", DoubleType(), True),
    StructField("SUBMT_PDX_ID", DoubleType(), True),
    StructField("RX_NO", StringType(), True),
    StructField("DT_RX_WRTN", DoubleType(), True),
    StructField("MNL_DT_SUBMSN", DoubleType(), True),
    StructField("FILL_DT", DoubleType(), True),
    StructField("SUBMT_DT", DoubleType(), True),
    StructField("RFL_CD", StringType(), True),
    StructField("RFL_STTUS", StringType(), True),
    StructField("PRAUTH_NO", StringType(), True),
    StructField("CARDHLDR_ID", StringType(), True),
    StructField("MBR_ID", StringType(), True),
    StructField("MBR_FIRST_NM", StringType(), True),
    StructField("MBR_LAST_NM", StringType(), True),
    StructField("PATN_FIRST_NM", StringType(), True),
    StructField("PATN_LAST_NM", StringType(), True),
    StructField("RELSHP_CD", StringType(), True),
    StructField("PRSN_CD", StringType(), True),
    StructField("SEX", StringType(), True),
    StructField("BRTH_DT", DoubleType(), True),
    StructField("MBR_CLNT_RIDER_CD", StringType(), True),
    StructField("MBR_DUR_KEY", StringType(), True),
    StructField("CARE_FCLTY_ID", StringType(), True),
    StructField("CARE_NTWK_ID", StringType(), True),
    StructField("CARE_QLFR_ID", StringType(), True),
    StructField("NDC", StringType(), True),
    StructField("REP_NDC", StringType(), True),
    StructField("GNRC_PROD_IN_NO", StringType(), True),
    StructField("DRUG_NM", StringType(), True),
    StructField("DRUG_STRG", StringType(), True),
    StructField("CMPND_IN", StringType(), True),
    StructField("GNRC_OVRD_IN", StringType(), True),
    StructField("PROD_MULTI_SRC_IN", StringType(), True),
    StructField("GNRC_IN", StringType(), True),
    StructField("MAIL_ORDER_IN", StringType(), True),
    StructField("FRMLRY_IN", StringType(), True),
    StructField("DRUG_MNFCTR_ID", StringType(), True),
    StructField("PRSCRBR_ID", StringType(), True),
    StructField("PRSCRBR_FIRST_NM", StringType(), True),
    StructField("PRSCRBR_LAST_NM", StringType(), True),
    StructField("QTY_DISPNS", DoubleType(), True),
    StructField("DAYS_SUPL", DoubleType(), True),
    StructField("INGRS_CST", DoubleType(), True),
    StructField("DISPENSE_FEE", DoubleType(), True),
    StructField("SLS_TAX", DoubleType(), True),
    StructField("PATN_CST", DoubleType(), True),
    StructField("PLN_CST", DoubleType(), True),
    StructField("TOT_CST", DoubleType(), True),
    StructField("BILL_CLM_CST", DoubleType(), True),
    StructField("CLM_ADM_FEE", DoubleType(), True),
    StructField("BILL_CLSIFIER_CD_DESC", StringType(), True),
    StructField("REJ_CD", StringType(), True),
    StructField("PDX_NM", StringType(), True),
    StructField("PDX_CITY", StringType(), True),
    StructField("PDX_ST", StringType(), True),
    StructField("PDX_ZIP_CD", StringType(), True),
    StructField("PDX_NTWK_PRTY", StringType(), True),
    StructField("PDX_NTWK_ID", StringType(), True),
    StructField("SUPER_NTWK_ID", StringType(), True),
    StructField("NO_BILL_IN", StringType(), True),
    StructField("CLM_SK", DoubleType(), True),
    StructField("AWP_AMT", DoubleType(), True),
    StructField("LICS_AMT", DoubleType(), True),
    StructField("CGAP_AMT", DoubleType(), True),
    StructField("MI_HICA_TAX_IN", StringType(), True),
    StructField("MI_HICA_TAX_AMT", DoubleType(), True),
    StructField("HLTH_PLN_AMT", DoubleType(), True),
    StructField("MCARE_PLN_TYP", StringType(), True),
    StructField("DUAL_EGWP_AMT", DoubleType(), True)
])

input_file_path = f"{adls_path}/verified/{FileName}"
df_OPTUMRX_InPutFile = (
    spark.read
         .option("header", True)
         .option("sep", ",")
         .option("quote", "'")
         .schema(schema_OPTUMRX_InPutFile)
         .csv(input_file_path)
)

# 2) Transformer (File_xfm)
dfFile_xfm = df_OPTUMRX_InPutFile.select(
    col("INVC_NO").alias("INVC_NO"),
    col("INVC_DT").alias("INVC_DT"),
    col("BILL_ENTY_ID").alias("BILL_ENTY_ID"),
    col("BILL_ENTY_NM").alias("BILL_ENTY_NM"),
    col("CAR_ID").alias("CAR_ID"),
    col("CAR_NM").alias("CAR_NM"),
    col("ACCT_ID").alias("ACCT_ID"),
    col("ACCT_NM").alias("ACCT_NM"),
    col("GRP_ID").alias("GRP_ID"),
    col("GRP_NM").alias("GRP_NM"),
    col("RXCLM_NO").alias("RXCLM_NO"),
    col("CLM_SEQ_NO").alias("CLM_SEQ_NO"),
    when(upper(trim(col("CLM_STTUS"))) == "REVERSED", "X")
      .when(upper(trim(col("CLM_STTUS"))) == "PAID", "P")
      .when(upper(trim(col("CLM_STTUS"))) == "REJECTED", "R")
      .otherwise(trim(col("CLM_STTUS")))
      .alias("CLM_STTUS"),
    col("CLM_CLS_SUB_CD").alias("CLM_CLS_SUB_CD"),
    col("CLM_SUBMSN_TYP").alias("CLM_SUBMSN_TYP"),
    col("TAX_CD").alias("TAX_CD"),
    col("NET_CLM_CT").alias("NET_CLM_CT"),
    col("PDX_NCPDP_ID").alias("PDX_NCPDP_ID"),
    col("PDX_NPI_ID").alias("PDX_NPI_ID"),
    col("SUBMT_PDX_ID").alias("SUBMT_PDX_ID"),
    col("RX_NO").alias("RX_NO"),
    col("DT_RX_WRTN").alias("DT_RX_WRTN"),
    col("MNL_DT_SUBMSN").alias("MNL_DT_SUBMSN"),
    col("FILL_DT").alias("FILL_DT"),
    col("SUBMT_DT").alias("SUBMT_DT"),
    col("RFL_CD").alias("RFL_CD"),
    col("RFL_STTUS").alias("RFL_STTUS"),
    col("PRAUTH_NO").alias("PRAUTH_NO"),
    col("CARDHLDR_ID").alias("CARDHLDR_ID"),
    col("MBR_ID").alias("MBR_ID"),
    col("MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    col("MBR_LAST_NM").alias("MBR_LAST_NM"),
    col("PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    col("PATN_LAST_NM").alias("PATN_LAST_NM"),
    col("RELSHP_CD").alias("RELSHP_CD"),
    col("PRSN_CD").alias("PRSN_CD"),
    col("SEX").alias("SEX"),
    col("BRTH_DT").alias("BRTH_DT"),
    col("MBR_CLNT_RIDER_CD").alias("MBR_CLNT_RIDER_CD"),
    col("MBR_DUR_KEY").alias("MBR_DUR_KEY"),
    col("CARE_FCLTY_ID").alias("CARE_FCLTY_ID"),
    col("CARE_NTWK_ID").alias("CARE_NTWK_ID"),
    col("CARE_QLFR_ID").alias("CARE_QLFR_ID"),
    col("NDC").alias("NDC"),
    col("REP_NDC").alias("REP_NDC"),
    col("GNRC_PROD_IN_NO").alias("GNRC_PROD_IN_NO"),
    col("DRUG_NM").alias("DRUG_NM"),
    col("DRUG_STRG").alias("DRUG_STRG"),
    col("CMPND_IN").alias("CMPND_IN"),
    col("GNRC_OVRD_IN").alias("GNRC_OVRD_IN"),
    col("PROD_MULTI_SRC_IN").alias("PROD_MULTI_SRC_IN"),
    col("GNRC_IN").alias("GNRC_IN"),
    col("MAIL_ORDER_IN").alias("MAIL_ORDER_IN"),
    col("FRMLRY_IN").alias("FRMLRY_IN"),
    col("DRUG_MNFCTR_ID").alias("DRUG_MNFCTR_ID"),
    col("PRSCRBR_ID").alias("PRSCRBR_ID"),
    col("PRSCRBR_FIRST_NM").alias("PRSCRBR_FIRST_NM"),
    col("PRSCRBR_LAST_NM").alias("PRSCRBR_LAST_NM"),
    col("QTY_DISPNS").alias("QTY_DISPNS"),
    col("DAYS_SUPL").alias("DAYS_SUPL"),
    col("INGRS_CST").alias("INGRS_CST"),
    col("DISPENSE_FEE").alias("DISPENSE_FEE"),
    col("SLS_TAX").alias("SLS_TAX"),
    col("PATN_CST").alias("PATN_CST"),
    col("PLN_CST").alias("PLN_CST"),
    col("TOT_CST").alias("TOT_CST"),
    col("BILL_CLM_CST").alias("BILL_CLM_CST"),
    col("CLM_ADM_FEE").alias("CLM_ADM_FEE"),
    col("BILL_CLSIFIER_CD_DESC").alias("BILL_CLSIFIER_CD_DESC"),
    col("REJ_CD").alias("REJ_CD"),
    col("PDX_NM").alias("PDX_NM"),
    col("PDX_CITY").alias("PDX_CITY"),
    col("PDX_ST").alias("PDX_ST"),
    col("PDX_ZIP_CD").alias("PDX_ZIP_CD"),
    col("PDX_NTWK_PRTY").alias("PDX_NTWK_PRTY"),
    col("PDX_NTWK_ID").alias("PDX_NTWK_ID"),
    col("SUPER_NTWK_ID").alias("SUPER_NTWK_ID"),
    col("NO_BILL_IN").alias("NO_BILL_IN"),
    col("CLM_SK").alias("CLM_SK"),
    col("AWP_AMT").alias("AWP_AMT"),
    col("LICS_AMT").alias("LICS_AMT"),
    col("CGAP_AMT").alias("CGAP_AMT"),
    col("MI_HICA_TAX_IN").alias("MI_HICA_TAX_IN"),
    col("MI_HICA_TAX_AMT").alias("MI_HICA_TAX_AMT"),
    col("HLTH_PLN_AMT").alias("HLTH_PLN_AMT"),
    col("MCARE_PLN_TYP").alias("MCARE_PLN_TYP"),
    col("DUAL_EGWP_AMT").alias("DUAL_EGWP_AMT")
)

# 3) Write to OPTUM_DrugInvoiceFile (CSeqFileStage) with padding for char columns
df_final = dfFile_xfm.select(
    col("INVC_NO"),
    col("INVC_DT"),
    rpad(col("BILL_ENTY_ID"), 20, " ").alias("BILL_ENTY_ID"),
    rpad(col("BILL_ENTY_NM"), 30, " ").alias("BILL_ENTY_NM"),
    rpad(col("CAR_ID"), 15, " ").alias("CAR_ID"),
    rpad(col("CAR_NM"), 30, " ").alias("CAR_NM"),
    rpad(col("ACCT_ID"), 15, " ").alias("ACCT_ID"),
    rpad(col("ACCT_NM"), 30, " ").alias("ACCT_NM"),
    rpad(col("GRP_ID"), 15, " ").alias("GRP_ID"),
    rpad(col("GRP_NM"), 30, " ").alias("GRP_NM"),
    col("RXCLM_NO"),
    col("CLM_SEQ_NO"),
    rpad(col("CLM_STTUS"), 8, " ").alias("CLM_STTUS"),
    rpad(col("CLM_CLS_SUB_CD"), 1, " ").alias("CLM_CLS_SUB_CD"),
    rpad(col("CLM_SUBMSN_TYP"), 1, " ").alias("CLM_SUBMSN_TYP"),
    rpad(col("TAX_CD"), 2, " ").alias("TAX_CD"),
    col("NET_CLM_CT"),
    col("PDX_NCPDP_ID"),
    col("PDX_NPI_ID"),
    col("SUBMT_PDX_ID"),
    rpad(col("RX_NO"), 12, " ").alias("RX_NO"),
    col("DT_RX_WRTN"),
    col("MNL_DT_SUBMSN"),
    col("FILL_DT"),
    col("SUBMT_DT"),
    rpad(col("RFL_CD"), 2, " ").alias("RFL_CD"),
    rpad(col("RFL_STTUS"), 6, " ").alias("RFL_STTUS"),
    rpad(col("PRAUTH_NO"), 11, " ").alias("PRAUTH_NO"),
    rpad(col("CARDHLDR_ID"), 20, " ").alias("CARDHLDR_ID"),
    rpad(col("MBR_ID"), 20, " ").alias("MBR_ID"),
    rpad(col("MBR_FIRST_NM"), 35, " ").alias("MBR_FIRST_NM"),
    rpad(col("MBR_LAST_NM"), 35, " ").alias("MBR_LAST_NM"),
    rpad(col("PATN_FIRST_NM"), 35, " ").alias("PATN_FIRST_NM"),
    rpad(col("PATN_LAST_NM"), 35, " ").alias("PATN_LAST_NM"),
    rpad(col("RELSHP_CD"), 3, " ").alias("RELSHP_CD"),
    rpad(col("PRSN_CD"), 3, " ").alias("PRSN_CD"),
    rpad(col("SEX"), 1, " ").alias("SEX"),
    col("BRTH_DT"),
    rpad(col("MBR_CLNT_RIDER_CD"), 6, " ").alias("MBR_CLNT_RIDER_CD"),
    rpad(col("MBR_DUR_KEY"), 18, " ").alias("MBR_DUR_KEY"),
    rpad(col("CARE_FCLTY_ID"), 10, " ").alias("CARE_FCLTY_ID"),
    rpad(col("CARE_NTWK_ID"), 10, " ").alias("CARE_NTWK_ID"),
    rpad(col("CARE_QLFR_ID"), 10, " ").alias("CARE_QLFR_ID"),
    rpad(col("NDC"), 12, " ").alias("NDC"),
    rpad(col("REP_NDC"), 12, " ").alias("REP_NDC"),
    rpad(col("GNRC_PROD_IN_NO"), 14, " ").alias("GNRC_PROD_IN_NO"),
    rpad(col("DRUG_NM"), 50, " ").alias("DRUG_NM"),
    rpad(col("DRUG_STRG"), 10, " ").alias("DRUG_STRG"),
    rpad(col("CMPND_IN"), 1, " ").alias("CMPND_IN"),
    rpad(col("GNRC_OVRD_IN"), 1, " ").alias("GNRC_OVRD_IN"),
    rpad(col("PROD_MULTI_SRC_IN"), 1, " ").alias("PROD_MULTI_SRC_IN"),
    rpad(col("GNRC_IN"), 1, " ").alias("GNRC_IN"),
    rpad(col("MAIL_ORDER_IN"), 1, " ").alias("MAIL_ORDER_IN"),
    rpad(col("FRMLRY_IN"), 1, " ").alias("FRMLRY_IN"),
    rpad(col("DRUG_MNFCTR_ID"), 10, " ").alias("DRUG_MNFCTR_ID"),
    rpad(col("PRSCRBR_ID"), 15, " ").alias("PRSCRBR_ID"),
    rpad(col("PRSCRBR_FIRST_NM"), 15, " ").alias("PRSCRBR_FIRST_NM"),
    rpad(col("PRSCRBR_LAST_NM"), 25, " ").alias("PRSCRBR_LAST_NM"),
    col("QTY_DISPNS"),
    col("DAYS_SUPL"),
    col("INGRS_CST"),
    col("DISPENSE_FEE"),
    col("SLS_TAX"),
    col("PATN_CST"),
    col("PLN_CST"),
    col("TOT_CST"),
    col("BILL_CLM_CST"),
    col("CLM_ADM_FEE"),
    rpad(col("BILL_CLSIFIER_CD_DESC"), 50, " ").alias("BILL_CLSIFIER_CD_DESC"),
    rpad(col("REJ_CD"), 3, " ").alias("REJ_CD"),
    rpad(col("PDX_NM"), 35, " ").alias("PDX_NM"),
    rpad(col("PDX_CITY"), 30, " ").alias("PDX_CITY"),
    rpad(col("PDX_ST"), 2, " ").alias("PDX_ST"),
    rpad(col("PDX_ZIP_CD"), 10, " ").alias("PDX_ZIP_CD"),
    rpad(col("PDX_NTWK_PRTY"), 3, " ").alias("PDX_NTWK_PRTY"),
    rpad(col("PDX_NTWK_ID"), 6, " ").alias("PDX_NTWK_ID"),
    rpad(col("SUPER_NTWK_ID"), 6, " ").alias("SUPER_NTWK_ID"),
    rpad(col("NO_BILL_IN"), 1, " ").alias("NO_BILL_IN"),
    col("CLM_SK"),
    col("AWP_AMT"),
    col("LICS_AMT"),
    col("CGAP_AMT"),
    rpad(col("MI_HICA_TAX_IN"), 2, " ").alias("MI_HICA_TAX_IN"),
    col("MI_HICA_TAX_AMT"),
    col("HLTH_PLN_AMT"),
    rpad(col("MCARE_PLN_TYP"), 1, " ").alias("MCARE_PLN_TYP"),
    col("DUAL_EGWP_AMT")
)

output_file_path = f"{adls_path}/verified/OPTUMRX_DrugInvoice.dat.{RunID}"
write_files(
    df_final,
    output_file_path,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)