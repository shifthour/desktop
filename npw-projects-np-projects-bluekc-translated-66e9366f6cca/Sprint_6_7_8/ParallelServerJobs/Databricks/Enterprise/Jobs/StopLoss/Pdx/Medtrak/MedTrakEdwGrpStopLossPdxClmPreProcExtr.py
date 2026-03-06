# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-26
# MAGIC """
# MAGIC Â© Copyright 2018 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by:
# MAGIC                     
# MAGIC 
# MAGIC PROCESSING : Reads data from the Medtrak Claim file and creats a common output
# MAGIC 
# MAGIC Modifications:                        
# MAGIC  Developer                        Date               Project/Altiris #     Change Description                                                            Development Project       Code Reviewer            Date Reviewed
# MAGIC --------------------------------------   -------------------    -------------------------    ----------------------------------------------------------------------------------------   ----------------------------------      -------------------------           -------------------------   
# MAGIC Kaushik Kapoor                2018-03-30     5828                     Initial Programming                                                              EnterpriseDev2                Kalyan Neelam            2018-04-20
# MAGIC Kaushik Kapoor                2018-08-13     5828                     Getting Min and Max Prcs_Dt in a file                                 EnterpriseDev2                Hugh Sisson                2018-08-17

# MAGIC This job extracts data from Medtrak claims file and load file date and other required details in to a common layout for further use.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, DecimalType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

InFileName = get_widget_value("InFileName","")
RunId = get_widget_value("RunId","")
RunDate = get_widget_value("RunDate","")

if "landing" in InFileName:
    input_file_MedtrakClmFile = f"{adls_path_raw}/{InFileName}"
elif "external" in InFileName:
    input_file_MedtrakClmFile = f"{adls_path_publish}/{InFileName}"
else:
    input_file_MedtrakClmFile = f"{adls_path}/{InFileName}"

col_details_MedtrakClmFile = {
    "RCRD_ID": {"len": 1, "type": StringType()},
    "PRCSR_NO": {"len": 10, "type": StringType()},
    "BTCH_NO": {"len": 5, "type": StringType()},
    "PDX_NO": {"len": 15, "type": StringType()},
    "RX_NO": {"len": 9, "type": StringType()},
    "DT_FILLED": {"len": 8, "type": StringType()},
    "NDC_NO": {"len": 11, "type": StringType()},
    "DRUG_DESC": {"len": 30, "type": StringType()},
    "NEW_RFL_CD": {"len": 2, "type": StringType()},
    "METRIC_QTY": {"len": 10, "type": StringType()},
    "DAYS_SUPL": {"len": 3, "type": StringType()},
    "BSS_OF_CST_DTRM": {"len": 2, "type": StringType()},
    "INGR_CST": {"len": 9, "type": StringType()},
    "DISPNS_FEE": {"len": 9, "type": StringType()},
    "COPAY_AMT": {"len": 9, "type": StringType()},
    "SLS_TAX": {"len": 8, "type": StringType()},
    "AMT_BILL": {"len": 9, "type": StringType()},
    "PATN_FIRST_NM": {"len": 12, "type": StringType()},
    "PATN_LAST_NM": {"len": 15, "type": StringType()},
    "DOB": {"len": 8, "type": StringType()},
    "SEX_CD": {"len": 1, "type": StringType()},
    "CARDHLDR_ID_NO": {"len": 18, "type": StringType()},
    "RELSHP_CD": {"len": 1, "type": StringType()},
    "GRP_NO": {"len": 18, "type": StringType()},
    "HOME_PLN": {"len": 3, "type": StringType()},
    "HOST_PLN": {"len": 3, "type": StringType()},
    "PRSCRBR_ID": {"len": 15, "type": StringType()},
    "DIAG_CD": {"len": 6, "type": StringType()},
    "CARDHLDR_FIRST_NM": {"len": 12, "type": StringType()},
    "CARDHLDR_LAST_NM": {"len": 15, "type": StringType()},
    "PRAUTH_NO": {"len": 12, "type": StringType()},
    "PA_MC_SC_NO": {"len": 7, "type": StringType()},
    "CUST_LOC": {"len": 2, "type": StringType()},
    "RESUB_CYC_CT": {"len": 2, "type": StringType()},
    "DT_RX_WRTN": {"len": 8, "type": StringType()},
    "DISPENSE_AS_WRTN_PROD_SEL_CD": {"len": 1, "type": StringType()},
    "PRSN_CD": {"len": 3, "type": StringType()},
    "OTHR_COV_CD": {"len": 2, "type": StringType()},
    "ELIG_CLRFCTN_CD": {"len": 1, "type": StringType()},
    "CMPND_CD": {"len": 1, "type": StringType()},
    "NO_OF_RFLS_AUTH": {"len": 2, "type": StringType()},
    "LVL_OF_SVC": {"len": 2, "type": StringType()},
    "RX_ORIG_CD": {"len": 1, "type": StringType()},
    "RX_DENIAL_CLRFCTN": {"len": 2, "type": StringType()},
    "PRI_PRSCRBR": {"len": 10, "type": StringType()},
    "CLNC_ID_NO": {"len": 5, "type": StringType()},
    "DRUG_TYP": {"len": 1, "type": StringType()},
    "PRSCRBR_LAST_NM": {"len": 15, "type": StringType()},
    "POSTAGE_AMT_CLMED": {"len": 4, "type": StringType()},
    "UNIT_DOSE_IN": {"len": 1, "type": StringType()},
    "OTHR_PAYOR_AMT": {"len": 9, "type": StringType()},
    "BSS_OF_DAYS_SUPL_DTRM": {"len": 1, "type": StringType()},
    "FULL_AWP": {"len": 9, "type": StringType()},
    "EXPNSN_AREA": {"len": 1, "type": StringType()},
    "MSTR_CAR": {"len": 4, "type": StringType()},
    "SUB_CAR": {"len": 4, "type": StringType()},
    "CLM_TYP": {"len": 1, "type": StringType()},
    "ESI_SUB_GRP": {"len": 20, "type": StringType()},
    "PLN_DSGNR": {"len": 1, "type": StringType()},
    "ADJDCT_DT": {"len": 8, "type": StringType()},
    "ADMIN_FEE": {"len": 9, "type": StringType()},
    "CAP_AMT": {"len": 9, "type": StringType()},
    "INGR_CST_SUB": {"len": 9, "type": StringType()},
    "MBR_NON_COPAY_AMT": {"len": 9, "type": StringType()},
    "MBR_PAY_CD": {"len": 2, "type": StringType()},
    "INCNTV_FEE": {"len": 8, "type": StringType()},
    "CLM_ADJ_AMT": {"len": 6, "type": StringType()},
    "CLM_ADJ_CD": {"len": 2, "type": StringType()},
    "FRMLRY_FLAG": {"len": 1, "type": StringType()},
    "GNRC_CLS_NO": {"len": 14, "type": StringType()},
    "THRPTC_CLS_AHFS": {"len": 6, "type": StringType()},
    "PDX_TYP": {"len": 1, "type": StringType()},
    "BILL_BSS_CD": {"len": 2, "type": StringType()},
    "USL_AND_CUST_CHRG": {"len": 9, "type": StringType()},
    "PD_DT": {"len": 8, "type": StringType()},
    "BNF_CD": {"len": 10, "type": StringType()},
    "DRUG_STRG": {"len": 10, "type": StringType()},
    "ORIG_MBR": {"len": 2, "type": StringType()},
    "DT_OF_INJRY": {"len": 8, "type": StringType()},
    "FEE_AMT": {"len": 9, "type": StringType()},
    "ESI_REF_NO": {"len": 14, "type": StringType()},
    "CLNT_CUST_ID": {"len": 20, "type": StringType()},
    "PLN_TYP": {"len": 10, "type": StringType()},
    "ESI_ADJDCT_REF_NO": {"len": 9, "type": StringType()},
    "ESI_ANCLRY_AMT": {"len": 9, "type": StringType()},
    "ESI_CLNT_GNRL_PRPS_AREA": {"len": 40, "type": StringType()},
    "PRTL_FILL_STTUS_CD": {"len": 1, "type": StringType()},
    "ESI_BILL_DT": {"len": 8, "type": StringType()},
    "FSA_VNDR_CD": {"len": 2, "type": StringType()},
    "PICA_DRUG_CD": {"len": 1, "type": StringType()},
    "AMT_CLMED": {"len": 9, "type": StringType()},
    "AMT_DSALW": {"len": 9, "type": StringType()},
    "FED_DRUG_CLS_CD": {"len": 1, "type": StringType()},
    "DEDCT_AMT": {"len": 9, "type": StringType()},
    "BNF_COPAY_100": {"len": 1, "type": StringType()},
    "CLM_PRCS_TYP": {"len": 1, "type": StringType()},
    "INDEM_HIER_TIER_NO": {"len": 4, "type": StringType()},
    "FLR": {"len": 1, "type": StringType()},
    "MCARE_D_COV_DRUG": {"len": 1, "type": StringType()},
    "RETRO_LICS_CD": {"len": 1, "type": StringType()},
    "RETRO_LICS_AMT": {"len": 9, "type": StringType()},
    "LICS_SBSDY_AMT": {"len": 9, "type": StringType()},
    "MED_B_DRUG": {"len": 1, "type": StringType()},
    "MED_B_CLM": {"len": 1, "type": StringType()},
    "PRSCRBR_QLFR": {"len": 2, "type": StringType()},
    "PRSCRBR_ID_NPI": {"len": 10, "type": StringType()},
    "PDX_QLFR": {"len": 2, "type": StringType()},
    "PDX_ID_NPI": {"len": 10, "type": StringType()},
    "HRA_APLD_AMT": {"len": 11, "type": StringType()},
    "ESI_THER_CLS": {"len": 6, "type": StringType()},
    "HIC_NO": {"len": 12, "type": StringType()},
    "HRA_FLAG": {"len": 1, "type": StringType()},
    "DOSE_CD": {"len": 4, "type": StringType()},
    "LOW_INCM": {"len": 1, "type": StringType()},
    "RTE_OF_ADMIN": {"len": 2, "type": StringType()},
    "DEA_SCHD": {"len": 1, "type": StringType()},
    "COPAY_BNF_OPT": {"len": 10, "type": StringType()},
    "GNRC_PROD_IN_GPI": {"len": 14, "type": StringType()},
    "PRSCRBR_SPEC": {"len": 10, "type": StringType()},
    "VAL_CD": {"len": 18, "type": StringType()},
    "PRI_CARE_PDX": {"len": 18, "type": StringType()},
    "OFC_OF_INSPECTOR_GNRL_OIG": {"len": 1, "type": StringType()},
    "PATN_SSN": {"len": 11, "type": StringType()},
    "CARDHLDR_SSN": {"len": 11, "type": StringType()},
    "CARDHLDR_DOB": {"len": 8, "type": StringType()},
    "CARDHLDR_ADDR": {"len": 25, "type": StringType()},
    "CARDHLDR_CITY": {"len": 18, "type": StringType()},
    "CHADHLDR_ST": {"len": 2, "type": StringType()},
    "CARDHLDR_ZIP_CD": {"len": 5, "type": StringType()},
    "FLR3": {"len": 65, "type": StringType()},
    "PSL_FMLY_MET_AMT": {"len": 9, "type": StringType()},
    "PSL_MBR_MET_AMT": {"len": 9, "type": StringType()},
    "PSL_FMLY_AMT": {"len": 9, "type": StringType()},
    "DED_FMLY_MET_AMT": {"len": 9, "type": StringType()},
    "DED_FMLY_AMT": {"len": 9, "type": StringType()},
    "MOPS_FMLY_AMT": {"len": 9, "type": StringType()},
    "MOPS_FMLY_MET_AMT": {"len": 9, "type": StringType()},
    "MOPS_MBR_MET_AMT": {"len": 9, "type": StringType()},
    "DED_MBR_MET_AMT": {"len": 9, "type": StringType()},
    "PSL_APLD_AMT": {"len": 9, "type": StringType()},
    "MOPS_APLD_AMT": {"len": 9, "type": StringType()},
    "PAR_PDX_IND": {"len": 1, "type": StringType()},
    "COPAY_PCT_AMT": {"len": 8, "type": StringType()},
    "COPAY_FLAT_AMT": {"len": 9, "type": StringType()},
    "CLM_TRANSMITTAL_METH": {"len": 1, "type": StringType()},
    "FLR4": {"len": 82, "type": StringType()},
    "RX_NO_2012": {"len": 12, "type": StringType()}
}

df_MedtrakClmFile = fixed_file_read_write(input_file_MedtrakClmFile, col_details_MedtrakClmFile, "read")

df_MedtrakClmFile_filtered = df_MedtrakClmFile.filter(F.col("RCRD_ID") == "4")

df_Common_OP = df_MedtrakClmFile_filtered.select(
    F.lit("MEDTRAK").alias("SRC_SYS_CD"),
    F.concat(Upcase(trim(F.col("MSTR_CAR"))), Upcase(trim(F.col("SUB_CAR")))).alias("GRP_BASE"),
    F.lit(RunDate[:7]).alias("PRCS_DT")
)

df_MinMaxDt = df_MedtrakClmFile_filtered.select(
    F.lit(RunDate[:7]).alias("RUNDATE"),
    F.col("PD_DT").cast(DecimalType(8,0)).alias("PD_DT")
)

df_agg_Min_Max_Dt = df_MinMaxDt.groupBy("RUNDATE").agg(
    F.min("PD_DT").alias("MIN_PD_DT"),
    F.max("PD_DT").alias("MAX_PD_DT")
)

df_ConverttoChar = df_agg_Min_Max_Dt.select(
    F.date_format(F.to_date(F.col("MIN_PD_DT").cast("string"), "yyyyMMdd"), "yyyy-MM-dd").alias("MIN_PD_DT"),
    F.date_format(F.to_date(F.col("MAX_PD_DT").cast("string"), "yyyyMMdd"), "yyyy-MM-dd").alias("MAX_PD_DT")
)

out_file_PDX_COMM_OP = f"verified/PDX_EDW_CLM_STD_INPT_MEDTRAK.dat.{RunId}"
output_path_PDX_COMM_OP = ""
if "landing" in out_file_PDX_COMM_OP:
    output_path_PDX_COMM_OP = f"{adls_path_raw}/{out_file_PDX_COMM_OP}"
elif "external" in out_file_PDX_COMM_OP:
    output_path_PDX_COMM_OP = f"{adls_path_publish}/{out_file_PDX_COMM_OP}"
else:
    output_path_PDX_COMM_OP = f"{adls_path}/{out_file_PDX_COMM_OP}"

write_files(
    df_Common_OP.select("SRC_SYS_CD","GRP_BASE","PRCS_DT"),
    output_path_PDX_COMM_OP,
    delimiter="|",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote='"',
    nullValue=None
)

out_file_Min_Max_Dt = "external/MedTrak_Min_Max_Pcrs_Dt.dat"
output_path_Min_Max_Dt = ""
if "landing" in out_file_Min_Max_Dt:
    output_path_Min_Max_Dt = f"{adls_path_raw}/{out_file_Min_Max_Dt}"
elif "external" in out_file_Min_Max_Dt:
    output_path_Min_Max_Dt = f"{adls_path_publish}/{out_file_Min_Max_Dt}"
else:
    output_path_Min_Max_Dt = f"{adls_path}/{out_file_Min_Max_Dt}"

write_files(
    df_ConverttoChar.select("MIN_PD_DT","MAX_PD_DT"),
    output_path_Min_Max_Dt,
    delimiter="|",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote=None,
    nullValue=None
)