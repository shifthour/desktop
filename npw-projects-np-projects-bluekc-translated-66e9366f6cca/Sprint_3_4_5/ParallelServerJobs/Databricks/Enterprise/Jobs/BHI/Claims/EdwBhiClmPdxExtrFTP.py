# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2013 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  This Job FTP's std_pharm_claims file
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  
# MAGIC 
# MAGIC HASH FILES:  
# MAGIC 
# MAGIC 
# MAGIC RERUN INFORMATION: 
# MAGIC 
# MAGIC              PREVIOUS RUN SUCCESSFUL:   Nothing to reset. 
# MAGIC              PREVIOUS RUN ABORTED:         Nothing to reset, just restart.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                          Date                    Project/Ticket #\(9)           Change Description\(9)\(9)\(9)              Development Project\(9)      Code Reviewer\(9)        Date Reviewed       
# MAGIC ------------------                    -------------------            ---------------------------\(9)           -----------------------------------------------------------------------            --------------------------------\(9)      -------------------------         -------------------------    
# MAGIC Praveen  Annam             2014-07-14              5115 BHI                                       Original programming                                                  EnterpriseNewDevl        Bhoomi Dasari           7/15/2014  
# MAGIC Bhoomi Dasari                10/7/2014               5115-BHI                                   Changes to FTP stage to remove extra bytes                 EnterpriseNewDevl         Kalyan Neelam          2014-10-08
# MAGIC Madhu Sudhan Thera    2019-08-13            99156                                             Added substance use flag in source file                                EnterpriseDev2        Kalyan Neelam          2019-08-14

# MAGIC Job Name: std_pharm_claims File FTP
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StringType
from pyspark.sql.functions import rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------

ProdIn = get_widget_value('ProdIn','')

col_details_std_pharm_claims = {
    "BHI_HOME_PLN_ID": {"len": 3, "type": StringType()},
    "CLM_ID": {"len": 25, "type": StringType()},
    "CLM_LN_NO": {"len": 3, "type": StringType()},
    "TRACEABILITY_FLD": {"len": 5, "type": StringType()},
    "ADJ_SEQ_NO": {"len": 8, "type": StringType()},
    "HOST_PLN_ID": {"len": 3, "type": StringType()},
    "HOME_PLN_PROD_ID": {"len": 15, "type": StringType()},
    "ACCT": {"len": 14, "type": StringType()},
    "GRP_ID": {"len": 14, "type": StringType()},
    "SUBGRP_ID": {"len": 10, "type": StringType()},
    "MBR_ID": {"len": 22, "type": StringType()},
    "MBR_ZIP_CD_ON_CLM": {"len": 5, "type": StringType()},
    "MBR_CTRY_ON_CLM": {"len": 2, "type": StringType()},
    "BILL_PROV_ID": {"len": 27, "type": StringType()},
    "NPI_BILL_PROV_ID": {"len": 10, "type": StringType()},
    "BILL_PROV_SPEC": {"len": 2, "type": StringType()},
    "PCP_PROV_ID": {"len": 27, "type": StringType()},
    "NPI_PCP_PROV_ID": {"len": 10, "type": StringType()},
    "REND_PROV_ID": {"len": 27, "type": StringType()},
    "NPI_REND_PROV_ID": {"len": 10, "type": StringType()},
    "REND_PROV_SPEC": {"len": 2, "type": StringType()},
    "REND_PROV_TYP": {"len": 2, "type": StringType()},
    "PRSCRB_PROV_ID": {"len": 27, "type": StringType()},
    "NPI_PRSCRB_PROV_ID": {"len": 10, "type": StringType()},
    "BSS_OF_RMBRMT_DTRM": {"len": 2, "type": StringType()},
    "BNF_PAYMT_STTUS_CD": {"len": 1, "type": StringType()},
    "PDX_CARVE_OUT_SUBMSN_IN": {"len": 1, "type": StringType()},
    "CAT_OF_SVC": {"len": 14, "type": StringType()},
    "CLM_PAYMT_STTUS": {"len": 1, "type": StringType()},
    "CMPND_CD": {"len": 1, "type": StringType()},
    "DAW_CD": {"len": 2, "type": StringType()},
    "DAYS_SUPL": {"len": 3, "type": StringType()},
    "DISPNS_STTUS": {"len": 1, "type": StringType()},
    "FRMLRY_IN": {"len": 1, "type": StringType()},
    "PLN_SPEC_DRUG_IN": {"len": 1, "type": StringType()},
    "NON_COV_RSN_CD_PRI": {"len": 2, "type": StringType()},
    "OTHR_COV_CD": {"len": 2, "type": StringType()},
    "PBM_CD": {"len": 2, "type": StringType()},
    "POS": {"len": 2, "type": StringType()},
    "PROD_SVC_ID": {"len": 11, "type": StringType()},
    "QTY_DISPNS": {"len": 11, "type": StringType()},
    "SUBMSN_TYP": {"len": 2, "type": StringType()},
    "ADJDCT_DT": {"len": 8, "type": StringType()},
    "CLM_PD_DT": {"len": 8, "type": StringType()},
    "DT_OF_SVC": {"len": 8, "type": StringType()},
    "ALW_AMT": {"len": 9, "type": StringType()},
    "NON_COV_AMT": {"len": 9, "type": StringType()},
    "PAYMT_AMT": {"len": 9, "type": StringType()},
    "SUBMT_AMT": {"len": 9, "type": StringType()},
    "AMT_APLD_TO_PRDC_DEDCT": {"len": 9, "type": StringType()},
    "AMT_ATRBD_TO_PROD_SEL": {"len": 9, "type": StringType()},
    "AMT_ATRBD_TO_SLS_TAX": {"len": 9, "type": StringType()},
    "AMT_OF_COPAY_CO_INSUR": {"len": 9, "type": StringType()},
    "AVG_WHLSL_PRICE_AMT": {"len": 9, "type": StringType()},
    "DISPNS_FEE_PD": {"len": 9, "type": StringType()},
    "FLAT_SLS_TAX_AMT_PD": {"len": 9, "type": StringType()},
    "INCNTV_AMT_PD": {"len": 9, "type": StringType()},
    "INGR_CST_PD": {"len": 9, "type": StringType()},
    "OTHR_AMT_PD": {"len": 9, "type": StringType()},
    "OTHR_PAYER_AMT_RECOGNIZED": {"len": 9, "type": StringType()},
    "PATN_PAY_AMT": {"len": 9, "type": StringType()},
    "PCT_SLS_TAX_AMT_PD": {"len": 9, "type": StringType()},
    "TOT_AMT_PD": {"len": 9, "type": StringType()},
    "SBSTNC_USE_RCRD_IN": {"len": 1, "type": StringType()}
}

df_std_pharm_claims = fixed_file_read_write(
    f"{adls_path_publish}/external/std_pharm_claims",
    col_details_std_pharm_claims,
    "read"
)

df_std_pharm_claims = df_std_pharm_claims.select(
    rpad(df_std_pharm_claims["BHI_HOME_PLN_ID"], 3, " ").alias("BHI_HOME_PLN_ID"),
    rpad(df_std_pharm_claims["CLM_ID"], 25, " ").alias("CLM_ID"),
    rpad(df_std_pharm_claims["CLM_LN_NO"], 3, " ").alias("CLM_LN_NO"),
    rpad(df_std_pharm_claims["TRACEABILITY_FLD"], 5, " ").alias("TRACEABILITY_FLD"),
    rpad(df_std_pharm_claims["ADJ_SEQ_NO"], 8, " ").alias("ADJ_SEQ_NO"),
    rpad(df_std_pharm_claims["HOST_PLN_ID"], 3, " ").alias("HOST_PLN_ID"),
    rpad(df_std_pharm_claims["HOME_PLN_PROD_ID"], 15, " ").alias("HOME_PLN_PROD_ID"),
    rpad(df_std_pharm_claims["ACCT"], 14, " ").alias("ACCT"),
    rpad(df_std_pharm_claims["GRP_ID"], 14, " ").alias("GRP_ID"),
    rpad(df_std_pharm_claims["SUBGRP_ID"], 10, " ").alias("SUBGRP_ID"),
    rpad(df_std_pharm_claims["MBR_ID"], 22, " ").alias("MBR_ID"),
    rpad(df_std_pharm_claims["MBR_ZIP_CD_ON_CLM"], 5, " ").alias("MBR_ZIP_CD_ON_CLM"),
    rpad(df_std_pharm_claims["MBR_CTRY_ON_CLM"], 2, " ").alias("MBR_CTRY_ON_CLM"),
    rpad(df_std_pharm_claims["BILL_PROV_ID"], 27, " ").alias("BILL_PROV_ID"),
    rpad(df_std_pharm_claims["NPI_BILL_PROV_ID"], 10, " ").alias("NPI_BILL_PROV_ID"),
    rpad(df_std_pharm_claims["BILL_PROV_SPEC"], 2, " ").alias("BILL_PROV_SPEC"),
    rpad(df_std_pharm_claims["PCP_PROV_ID"], 27, " ").alias("PCP_PROV_ID"),
    rpad(df_std_pharm_claims["NPI_PCP_PROV_ID"], 10, " ").alias("NPI_PCP_PROV_ID"),
    rpad(df_std_pharm_claims["REND_PROV_ID"], 27, " ").alias("REND_PROV_ID"),
    rpad(df_std_pharm_claims["NPI_REND_PROV_ID"], 10, " ").alias("NPI_REND_PROV_ID"),
    rpad(df_std_pharm_claims["REND_PROV_SPEC"], 2, " ").alias("REND_PROV_SPEC"),
    rpad(df_std_pharm_claims["REND_PROV_TYP"], 2, " ").alias("REND_PROV_TYP"),
    rpad(df_std_pharm_claims["PRSCRB_PROV_ID"], 27, " ").alias("PRSCRB_PROV_ID"),
    rpad(df_std_pharm_claims["NPI_PRSCRB_PROV_ID"], 10, " ").alias("NPI_PRSCRB_PROV_ID"),
    rpad(df_std_pharm_claims["BSS_OF_RMBRMT_DTRM"], 2, " ").alias("BSS_OF_RMBRMT_DTRM"),
    rpad(df_std_pharm_claims["BNF_PAYMT_STTUS_CD"], 1, " ").alias("BNF_PAYMT_STTUS_CD"),
    rpad(df_std_pharm_claims["PDX_CARVE_OUT_SUBMSN_IN"], 1, " ").alias("PDX_CARVE_OUT_SUBMSN_IN"),
    rpad(df_std_pharm_claims["CAT_OF_SVC"], 14, " ").alias("CAT_OF_SVC"),
    rpad(df_std_pharm_claims["CLM_PAYMT_STTUS"], 1, " ").alias("CLM_PAYMT_STTUS"),
    rpad(df_std_pharm_claims["CMPND_CD"], 1, " ").alias("CMPND_CD"),
    rpad(df_std_pharm_claims["DAW_CD"], 2, " ").alias("DAW_CD"),
    rpad(df_std_pharm_claims["DAYS_SUPL"], 3, " ").alias("DAYS_SUPL"),
    rpad(df_std_pharm_claims["DISPNS_STTUS"], 1, " ").alias("DISPNS_STTUS"),
    rpad(df_std_pharm_claims["FRMLRY_IN"], 1, " ").alias("FRMLRY_IN"),
    rpad(df_std_pharm_claims["PLN_SPEC_DRUG_IN"], 1, " ").alias("PLN_SPEC_DRUG_IN"),
    rpad(df_std_pharm_claims["NON_COV_RSN_CD_PRI"], 2, " ").alias("NON_COV_RSN_CD_PRI"),
    rpad(df_std_pharm_claims["OTHR_COV_CD"], 2, " ").alias("OTHR_COV_CD"),
    rpad(df_std_pharm_claims["PBM_CD"], 2, " ").alias("PBM_CD"),
    rpad(df_std_pharm_claims["POS"], 2, " ").alias("POS"),
    rpad(df_std_pharm_claims["PROD_SVC_ID"], 11, " ").alias("PROD_SVC_ID"),
    rpad(df_std_pharm_claims["QTY_DISPNS"], 11, " ").alias("QTY_DISPNS"),
    rpad(df_std_pharm_claims["SUBMSN_TYP"], 2, " ").alias("SUBMSN_TYP"),
    rpad(df_std_pharm_claims["ADJDCT_DT"], 8, " ").alias("ADJDCT_DT"),
    rpad(df_std_pharm_claims["CLM_PD_DT"], 8, " ").alias("CLM_PD_DT"),
    rpad(df_std_pharm_claims["DT_OF_SVC"], 8, " ").alias("DT_OF_SVC"),
    rpad(df_std_pharm_claims["ALW_AMT"], 9, " ").alias("ALW_AMT"),
    rpad(df_std_pharm_claims["NON_COV_AMT"], 9, " ").alias("NON_COV_AMT"),
    rpad(df_std_pharm_claims["PAYMT_AMT"], 9, " ").alias("PAYMT_AMT"),
    rpad(df_std_pharm_claims["SUBMT_AMT"], 9, " ").alias("SUBMT_AMT"),
    rpad(df_std_pharm_claims["AMT_APLD_TO_PRDC_DEDCT"], 9, " ").alias("AMT_APLD_TO_PRDC_DEDCT"),
    rpad(df_std_pharm_claims["AMT_ATRBD_TO_PROD_SEL"], 9, " ").alias("AMT_ATRBD_TO_PROD_SEL"),
    rpad(df_std_pharm_claims["AMT_ATRBD_TO_SLS_TAX"], 9, " ").alias("AMT_ATRBD_TO_SLS_TAX"),
    rpad(df_std_pharm_claims["AMT_OF_COPAY_CO_INSUR"], 9, " ").alias("AMT_OF_COPAY_CO_INSUR"),
    rpad(df_std_pharm_claims["AVG_WHLSL_PRICE_AMT"], 9, " ").alias("AVG_WHLSL_PRICE_AMT"),
    rpad(df_std_pharm_claims["DISPNS_FEE_PD"], 9, " ").alias("DISPNS_FEE_PD"),
    rpad(df_std_pharm_claims["FLAT_SLS_TAX_AMT_PD"], 9, " ").alias("FLAT_SLS_TAX_AMT_PD"),
    rpad(df_std_pharm_claims["INCNTV_AMT_PD"], 9, " ").alias("INCNTV_AMT_PD"),
    rpad(df_std_pharm_claims["INGR_CST_PD"], 9, " ").alias("INGR_CST_PD"),
    rpad(df_std_pharm_claims["OTHR_AMT_PD"], 9, " ").alias("OTHR_AMT_PD"),
    rpad(df_std_pharm_claims["OTHR_PAYER_AMT_RECOGNIZED"], 9, " ").alias("OTHR_PAYER_AMT_RECOGNIZED"),
    rpad(df_std_pharm_claims["PATN_PAY_AMT"], 9, " ").alias("PATN_PAY_AMT"),
    rpad(df_std_pharm_claims["PCT_SLS_TAX_AMT_PD"], 9, " ").alias("PCT_SLS_TAX_AMT_PD"),
    rpad(df_std_pharm_claims["TOT_AMT_PD"], 9, " ").alias("TOT_AMT_PD"),
    rpad(df_std_pharm_claims["SBSTNC_USE_RCRD_IN"], 1, " ").alias("SBSTNC_USE_RCRD_IN")
)

df_ftp_std_pharm_claims = df_std_pharm_claims