# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2010 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY : 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION: The job writes the Claim Id and ESI_GNRL_PRPS_AREA from the landing file to a hashed file which is later read in the IdsDrugClmFkey job to be loaded into the W_DRUG_CLM table. The ESI_GNRL_PRPS_AREA for an adjusted claim contains the value of the ESI_REF_NO of its original claim.
# MAGIC                                
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                        Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                --------------------------------       -------------------------------   ----------------------------      
# MAGIC Kalyan Neelam        2011-01-13        4616                      Original Programming                                                       IntegrateNewDevl         Steph Goddard          01/18/2011
# MAGIC Raja Gummadi        2012-07-23         TTR 1330            Changed RX_NO field size from 9 to 20 in input file         IntegrateWrhsDevl         Bhoomi Dasari            08/08/2012

# MAGIC This Hashed File is used and cleared in IdsDrugClmFkey
# MAGIC Medtrak Drug Reversals Hashed File create job
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType
from pyspark.sql.functions import col, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


RunID = get_widget_value('RunID','')

schema_MedtrakClmLand = StructType([
    StructField("RCRD_ID", DecimalType(38,10), True),
    StructField("CLAIM_ID", StringType(), True),
    StructField("PRCSR_NO", DecimalType(38,10), True),
    StructField("MEM_CK_KEY", IntegerType(), True),
    StructField("BTCH_NO", DecimalType(38,10), True),
    StructField("PDX_NO", StringType(), True),
    StructField("RX_NO", DecimalType(38,10), True),
    StructField("DT_FILLED", StringType(), True),
    StructField("NDC_NO", DecimalType(38,10), True),
    StructField("DRUG_DESC", StringType(), True),
    StructField("NEW_RFL_CD", DecimalType(38,10), True),
    StructField("METRIC_QTY", DecimalType(38,10), True),
    StructField("DAYS_SUPL", DecimalType(38,10), True),
    StructField("BSS_OF_CST_DTRM", StringType(), True),
    StructField("INGR_CST", DecimalType(38,10), True),
    StructField("DISPNS_FEE", DecimalType(38,10), True),
    StructField("COPAY_AMT", DecimalType(38,10), True),
    StructField("SLS_TAX", DecimalType(38,10), True),
    StructField("AMT_BILL", DecimalType(38,10), True),
    StructField("PATN_FIRST_NM", StringType(), True),
    StructField("PATN_LAST_NM", StringType(), True),
    StructField("DOB", DecimalType(38,10), True),
    StructField("SEX_CD", DecimalType(38,10), True),
    StructField("CARDHLDR_ID_NO", StringType(), True),
    StructField("RELSHP_CD", DecimalType(38,10), True),
    StructField("GRP_NO", StringType(), True),
    StructField("HOME_PLN", StringType(), True),
    StructField("HOST_PLN", DecimalType(38,10), True),
    StructField("PRESCRIBER_ID", StringType(), True),
    StructField("DIAG_CD", StringType(), True),
    StructField("CARDHLDR_FIRST_NM", StringType(), True),
    StructField("CARDHLDR_LAST_NM", StringType(), True),
    StructField("PRAUTH_NO", DecimalType(38,10), True),
    StructField("PA_MC_SC_NO", StringType(), True),
    StructField("CUST_LOC", DecimalType(38,10), True),
    StructField("RESUB_CYC_CT", DecimalType(38,10), True),
    StructField("DT_RX_WRTN", DecimalType(38,10), True),
    StructField("DISPENSE_AS_WRTN_PROD_SEL_CD", StringType(), True),
    StructField("PRSN_CD", StringType(), True),
    StructField("OTHR_COV_CD", DecimalType(38,10), True),
    StructField("ELIG_CLRFCTN_CD", DecimalType(38,10), True),
    StructField("CMPND_CD", DecimalType(38,10), True),
    StructField("NO_OF_RFLS_AUTH", DecimalType(38,10), True),
    StructField("LVL_OF_SVC", DecimalType(38,10), True),
    StructField("RX_ORIG_CD", DecimalType(38,10), True),
    StructField("RX_DENIAL_CLRFCTN", DecimalType(38,10), True),
    StructField("PRI_PRESCRIBER", StringType(), True),
    StructField("CLNC_ID_NO", DecimalType(38,10), True),
    StructField("DRUG_TYP", DecimalType(38,10), True),
    StructField("PRESCRIBER_LAST_NM", StringType(), True),
    StructField("POSTAGE_AMT_CLMED", DecimalType(38,10), True),
    StructField("UNIT_DOSE_IN", DecimalType(38,10), True),
    StructField("OTHR_PAYOR_AMT", DecimalType(38,10), True),
    StructField("BSS_OF_DAYS_SUPL_DTRM", DecimalType(38,10), True),
    StructField("FULL_AWP", DecimalType(38,10), True),
    StructField("EXPNSN_AREA", StringType(), True),
    StructField("MSTR_CAR", StringType(), True),
    StructField("SUB_CAR", StringType(), True),
    StructField("CLM_TYP", StringType(), True),
    StructField("ESI_SUB_GRP", StringType(), True),
    StructField("PLN_DSGNR", StringType(), True),
    StructField("ADJDCT_DT", StringType(), True),
    StructField("ADMIN_FEE", DecimalType(38,10), True),
    StructField("CAP_AMT", DecimalType(38,10), True),
    StructField("INGR_CST_SUB", DecimalType(38,10), True),
    StructField("MBR_NON_COPAY_AMT", DecimalType(38,10), True),
    StructField("MBR_PAY_CD", StringType(), True),
    StructField("INCNTV_FEE", DecimalType(38,10), True),
    StructField("CLM_ADJ_AMT", DecimalType(38,10), True),
    StructField("CLM_ADJ_CD", StringType(), True),
    StructField("FRMLRY_FLAG", StringType(), True),
    StructField("GNRC_CLS_NO", StringType(), True),
    StructField("THRPTC_CLS_AHFS", StringType(), True),
    StructField("PDX_TYP", StringType(), True),
    StructField("BILL_BSS_CD", StringType(), True),
    StructField("USL_AND_CUST_CHRG", DecimalType(38,10), True),
    StructField("PD_DT", StringType(), True),
    StructField("BNF_CD", StringType(), True),
    StructField("DRUG_STRG", StringType(), True),
    StructField("ORIG_MBR", StringType(), True),
    StructField("DT_OF_INJURY", DecimalType(38,10), True),
    StructField("FEE_AMT", DecimalType(38,10), True),
    StructField("ESI_REF_NO", StringType(), True),
    StructField("CLNT_CUST_ID", StringType(), True),
    StructField("PLN_TYP", StringType(), True),
    StructField("ESI_ADJDCT_REF_NO", DecimalType(38,10), True),
    StructField("ESI_ANCLRY_AMT", DecimalType(38,10), True),
    StructField("ESI_CLNT_GNRL_PRPS_AREA", StringType(), True),
    StructField("GRP_ID", StringType(), True),
    StructField("SUBGRP_ID", StringType(), True),
    StructField("CLS_PLN_ID", StringType(), True),
    StructField("PAID_DATE", StringType(), True),
    StructField("PRTL_FILL_STTUS_CD", StringType(), True),
    StructField("ESI_BILL_DT", DecimalType(38,10), True),
    StructField("FSA_VNDR_CD", StringType(), True),
    StructField("PICA_DRUG_CD", StringType(), True),
    StructField("AMT_CLMED", DecimalType(38,10), True),
    StructField("AMT_DSALW", DecimalType(38,10), True),
    StructField("FED_DRUG_CLS_CD", StringType(), True),
    StructField("DEDCT_AMT", DecimalType(38,10), True),
    StructField("BNF_COPAY_100", StringType(), True),
    StructField("CLM_PRCS_TYP", StringType(), True),
    StructField("INDEM_HIER_TIER_NO", DecimalType(38,10), True),
    StructField("FLR", StringType(), True),
    StructField("MCARE_D_COV_DRUG", StringType(), True),
    StructField("RETRO_LICS_CD", StringType(), True),
    StructField("RETRO_LICS_AMT", DecimalType(38,10), True),
    StructField("LICS_SBSDY_AMT", DecimalType(38,10), True),
    StructField("MED_B_DRUG", StringType(), True),
    StructField("MED_B_CLM", StringType(), True),
    StructField("PRESCRIBER_QLFR", StringType(), True),
    StructField("PRESCRIBER_ID_NPI", StringType(), True),
    StructField("PDX_QLFR", StringType(), True),
    StructField("PDX_ID_NPI", StringType(), True),
    StructField("HRA_APLD_AMT", DecimalType(38,10), True),
    StructField("ESI_THER_CLS", DecimalType(38,10), True),
    StructField("HIC_NO", StringType(), True),
    StructField("HRA_FLAG", StringType(), True),
    StructField("DOSE_CD", DecimalType(38,10), True),
    StructField("LOW_INCM", StringType(), True),
    StructField("RTE_OF_ADMIN", StringType(), True),
    StructField("DEA_SCHD", DecimalType(38,10), True),
    StructField("COPAY_BNF_OPT", DecimalType(38,10), True),
    StructField("GNRC_PROD_IN_GPI", DecimalType(38,10), True),
    StructField("PRESCRIBER_SPEC", StringType(), True),
    StructField("VAL_CD", StringType(), True),
    StructField("PRI_CARE_PDX", StringType(), True),
    StructField("OFC_OF_INSPECTOR_GNRL_OIG", StringType(), True),
    StructField("FLR3", StringType(), True),
    StructField("PSL_FMLY_MET_AMT", DecimalType(38,10), True),
    StructField("PSL_MBR_MET_AMT", DecimalType(38,10), True),
    StructField("PSL_FMLY_AMT", DecimalType(38,10), True),
    StructField("DED_FMLY_MET_AMT", DecimalType(38,10), True),
    StructField("DED_FMLY_AMT", DecimalType(38,10), True),
    StructField("MOPS_FMLY_AMT", DecimalType(38,10), True),
    StructField("MOPS_FMLY_MET_AMT", DecimalType(38,10), True),
    StructField("MOPS_MBR_MET_AMT", DecimalType(38,10), True),
    StructField("DED_MBR_MET_AMT", DecimalType(38,10), True),
    StructField("PSL_APLD_AMT", DecimalType(38,10), True),
    StructField("MOPS_APLD_AMT", DecimalType(38,10), True),
    StructField("PAR_PDX_IND", StringType(), True),
    StructField("COPAY_PCT_AMT", DecimalType(38,10), True),
    StructField("COPAY_FLAT_AMT", DecimalType(38,10), True),
    StructField("CLM_TRANSMITTAL_METH", StringType(), True),
    StructField("FLR4", StringType(), True)
])

df_MedtrakClmLand = (
    spark.read.format("csv")
    .option("delimiter", ",")
    .option("quote", "\"")
    .schema(schema_MedtrakClmLand)
    .load(f"{adls_path}/verified/MedtrakDrugClm_Land.dat.{RunID}")
)

df_DropFields = (
    df_MedtrakClmLand
    .filter(trim(col("CLM_TYP")) == 'R')
    .select(
        trim(col("CLAIM_ID")).alias("CLAIM_ID"),
        trim(col("ESI_CLNT_GNRL_PRPS_AREA")).alias("ESI_CLNT_GNRL_PRPS_AREA")
    )
)

df_DropFields = df_DropFields.withColumn(
    "ESI_CLNT_GNRL_PRPS_AREA",
    rpad(col("ESI_CLNT_GNRL_PRPS_AREA"), 40, " ")
)

write_files(
    df_DropFields.select("CLAIM_ID","ESI_CLNT_GNRL_PRPS_AREA"),
    "hf_medtrak_drgclm_rvrsl_hf.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)