# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-26
# MAGIC """
# MAGIC Â© Copyright 2018 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by:
# MAGIC                     
# MAGIC 
# MAGIC PROCESSING : Reads data from the SavRx Claim file and creats a common output
# MAGIC 
# MAGIC Modifications:                        
# MAGIC  Developer                            Date                Project/Altiris #    Change Description                                                  Development Project    Code Reviewer          Date Reviewed
# MAGIC ---------------------------------------      -------------------    -------------------------    -----------------------------------------------------------------------------    ----------------------------------   -------------------------------   -------------------------   
# MAGIC Kaushik Kapoor                    2018-09-05     5828                     Initial Programming                                                    EnterpriseDev2            Hugh Sisson               2018-09-17

# MAGIC This job extracts data from BCBSSC claims file and load file date and other required details in to a common layout for further use.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, lit, substring, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------

InFileName = get_widget_value('InFileName','')
RunId = get_widget_value('RunId','')

col_details_BcbsScSrcFile = {
    "GRP_PFX": {"len": 2, "type": StringType()},
    "GRP_BASE": {"len": 5, "type": StringType()},
    "GRP_SFX": {"len": 2, "type": StringType()},
    "SUB_ID": {"len": 12, "type": StringType()},
    "PATN_ID": {"len": 3, "type": StringType()},
    "RELSHP": {"len": 1, "type": StringType()},
    "PATN_SEX": {"len": 1, "type": StringType()},
    "FLR1": {"len": 2, "type": StringType()},
    "PATN_DOB": {"len": 10, "type": StringType()},
    "SUB_LAST_NM": {"len": 25, "type": StringType()},
    "SUB_FIRST_NM": {"len": 18, "type": StringType()},
    "PATN_LAST_NM": {"len": 25, "type": StringType()},
    "PATN_FIRST_NM": {"len": 20, "type": StringType()},
    "SUB_ADDR_1": {"len": 25, "type": StringType()},
    "SUB_ADDR_2": {"len": 25, "type": StringType()},
    "SUB_CITY": {"len": 18, "type": StringType()},
    "SUB_ST": {"len": 5, "type": StringType()},
    "SUB_ZIP": {"len": 5, "type": StringType()},
    "CLM_TYP": {"len": 4, "type": StringType()},
    "PDX_ID": {"len": 12, "type": StringType()},
    "PDX_ID_NO_TYP": {"len": 2, "type": StringType()},
    "PDX_NM": {"len": 30, "type": StringType()},
    "RTL_OR_MAIL": {"len": 3, "type": StringType()},
    "PRSCRBR_ID": {"len": 15, "type": StringType()},
    "PHYS_LAST_NM": {"len": 30, "type": StringType()},
    "VNDR_BTCH_NO": {"len": 18, "type": StringType()},
    "SVC_DT": {"len": 10, "type": StringType()},
    "RX_NO": {"len": 7, "type": StringType()},
    "FLR2": {"len": 2, "type": StringType()},
    "NDC": {"len": 11, "type": StringType()},
    "DRUG_NM": {"len": 15, "type": StringType()},
    "FLR3": {"len": 2, "type": StringType()},
    "QTY": {"len": 13, "type": StringType()},
    "QTY_SIGN": {"len": 1, "type": StringType()},
    "DAYS_SUPL": {"len": 5, "type": StringType()},
    "DAW_NDC": {"len": 3, "type": StringType()},
    "INGR_CST": {"len": 13, "type": StringType()},
    "INGR_CST_SIGN": {"len": 1, "type": StringType()},
    "DISPNS_FEE": {"len": 13, "type": StringType()},
    "DISPNS_FEE_SIGN": {"len": 1, "type": StringType()},
    "SLS_TAX": {"len": 13, "type": StringType()},
    "SLS_TAX_SIGN": {"len": 1, "type": StringType()},
    "GROS_CST": {"len": 13, "type": StringType()},
    "GROS_CST_SIGN": {"len": 1, "type": StringType()},
    "COPAY": {"len": 15, "type": StringType()},
    "COPAY_SIGN": {"len": 1, "type": StringType()},
    "PLN_PD_AMT": {"len": 15, "type": StringType()},
    "PLN_PD_AMT_SIGN": {"len": 1, "type": StringType()},
    "FLR3A": {"len": 2, "type": StringType()},
    "FRMLRY_IN": {"len": 1, "type": StringType()},
    "RFL_NO": {"len": 2, "type": StringType()},
    "ALT_ID": {"len": 15, "type": StringType()},
    "FLR4": {"len": 5, "type": StringType()},
    "DRUG_CAT_CD": {"len": 1, "type": StringType()},
    "MNTN_DRUG_IN": {"len": 1, "type": StringType()},
    "BSS_OF_CST": {"len": 2, "type": StringType()},
    "SUBMT_INGR_AMT": {"len": 15, "type": StringType()},
    "SUBMT_INGR_AMT_SIGN": {"len": 1, "type": StringType()},
    "DRUG_IN": {"len": 1, "type": StringType()},
    "PDX_TYP": {"len": 3, "type": StringType()},
    "PROV_IN": {"len": 2, "type": StringType()},
    "RCVD_DT": {"len": 10, "type": StringType()},
    "PDX_NTWK": {"len": 1, "type": StringType()},
    "FLR5": {"len": 9, "type": StringType()},
    "PDX_ADDR": {"len": 20, "type": StringType()},
    "PDX_CITY": {"len": 20, "type": StringType()},
    "PDX_ST": {"len": 2, "type": StringType()},
    "PDX_ZIPCD": {"len": 5, "type": StringType()},
    "RUN_DT": {"len": 10, "type": StringType()},
    "FLR6": {"len": 47, "type": StringType()}
}

df_BcbsScSrcFile = fixed_file_read_write(InFileName, col_details_BcbsScSrcFile, "read")

df_Trim_ReqrdField = df_BcbsScSrcFile.select(
    lit("BCBSSC").alias("SRC_SYS_CD"),
    trim(col("GRP_BASE")).alias("GRP_BASE"),
    substring(trim(col("RUN_DT")), 1, 7).alias("PRCS_DT")
)

df_final = df_Trim_ReqrdField.select(
    rpad(col("SRC_SYS_CD"), 6, " ").alias("SRC_SYS_CD"),
    rpad(col("GRP_BASE"), 5, " ").alias("GRP_BASE"),
    rpad(col("PRCS_DT"), 7, " ").alias("PRCS_DT")
)

output_path = f"{adls_path}/verified/PDX_EDW_CLM_STD_INPT_BCBSSC.dat.{RunId}"
write_files(
    df_final,
    output_path,
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)