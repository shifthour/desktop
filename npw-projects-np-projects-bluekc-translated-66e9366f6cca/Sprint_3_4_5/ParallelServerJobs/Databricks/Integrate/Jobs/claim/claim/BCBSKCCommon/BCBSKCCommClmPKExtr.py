# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC Copyright 2010 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  
# MAGIC                
# MAGIC 
# MAGIC PROCESSING:   
# MAGIC          *  Assigns / creates claim primary key surrogate keys for all records processed in current run.  Adjusted to/from claims don't need to be in hash file. 
# MAGIC          *  The primary key hash file hf_clm is the output of this job and is used by the following tables for keying
# MAGIC              CLM
# MAGIC              DRUG_CLM
# MAGIC              CLM_REMIT_HIST
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                    Date                 \(9)Project/Altiris #\(9)Change Description\(9)\(9)\(9)\(9)   Development Project\(9)Code Reviewer\(9)Date Reviewed       
# MAGIC ------------------                  --------------------     \(9)------------------------\(9)-----------------------------------------------------------------------\(9)    --------------------------------\(9)-------------------------------\(9)----------------------------       
# MAGIC Kaushik Kapoor             2018-01-08              5828                 Original Programming                                                    IntegrateDev2                      Kalyan Neelam         2018-02-26 
# MAGIC Rekha Radhakrishna    2020-08-13              6131                 Modified common file layout to include new 8 fields      IntegrateDev2                      Sravya Gorla             2020-09-12
# MAGIC                                                                                                ( field 151 - 158)

# MAGIC BCBSKSCommon Claim Primary Key Process
# MAGIC This job loads primary key hash file with all keys used by tables with natural keys of source system code and claim ID
# MAGIC Output required by container but not used here
# MAGIC Get SK for records with out keys
# MAGIC This container is used in:
# MAGIC ESIClmInvoicePKExtr
# MAGIC ESIClmPKExtr
# MAGIC FctsClmPKExtr
# MAGIC FctsPcaClmPKExtr
# MAGIC MCSourceClmPKExtr
# MAGIC MedicaidClmPKExtr
# MAGIC NascoClmExtr
# MAGIC NascoClmPKExtr
# MAGIC PcsClmPKExtr
# MAGIC WellDyneClmPKExtr
# MAGIC MedtrakClmPKExtr
# MAGIC BCBSKCCommClmPKExtr
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType
)
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


SrcSysCd = get_widget_value('SrcSysCd','SAVRX')
SrcSysCdSK = get_widget_value('SrcSysCdSK','')
CurrRunCycle = get_widget_value('CurrRunCycle','100')
RunID = get_widget_value('RunID','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

schema_BCBSCommClmLand = StructType([
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("FILE_RCVD_DT", StringType(), True),
    StructField("RCRD_ID", DecimalType(38,10), True),
    StructField("PRCSR_NO", StringType(), True),
    StructField("BTCH_NO", DecimalType(38,10), True),
    StructField("PDX_NO", DecimalType(38,10), True),
    StructField("RX_NO", DecimalType(38,10), True),
    StructField("FILL_DT", StringType(), True),
    StructField("NDC", DecimalType(38,10), True),
    StructField("DRUG_DESC", StringType(), True),
    StructField("NEW_OR_RFL_CD", IntegerType(), True),
    StructField("METRIC_QTY", DecimalType(38,10), True),
    StructField("DAYS_SUPL", DecimalType(38,10), True),
    StructField("BSS_OF_CST_DTRM", StringType(), True),
    StructField("INGR_CST_AMT", DecimalType(38,10), True),
    StructField("DISPNS_FEE_AMT", DecimalType(38,10), True),
    StructField("COPAY_AMT", DecimalType(38,10), True),
    StructField("SLS_TAX_AMT", DecimalType(38,10), True),
    StructField("BILL_AMT", DecimalType(38,10), True),
    StructField("PATN_FIRST_NM", StringType(), True),
    StructField("PATN_LAST_NM", StringType(), True),
    StructField("BRTH_DT", StringType(), True),
    StructField("SEX_CD", DecimalType(38,10), True),
    StructField("CARDHLDR_ID_NO", StringType(), True),
    StructField("RELSHP_CD", DecimalType(38,10), True),
    StructField("GRP_NO", StringType(), True),
    StructField("HOME_PLN", StringType(), True),
    StructField("HOST_PLN", StringType(), True),
    StructField("PRSCRBR_ID", StringType(), True),
    StructField("DIAG_CD", StringType(), True),
    StructField("CARDHLDR_FIRST_NM", StringType(), True),
    StructField("CARDHLDR_LAST_NM", StringType(), True),
    StructField("PRAUTH_NO", StringType(), True),
    StructField("PA_MC_SC_NO", StringType(), True),
    StructField("CUST_LOC", DecimalType(38,10), True),
    StructField("RESUB_CYC_CT", DecimalType(38,10), True),
    StructField("RX_DT", StringType(), True),
    StructField("DISPENSE_AS_WRTN_PROD_SEL_CD", StringType(), True),
    StructField("PRSN_CD", StringType(), True),
    StructField("OTHR_COV_CD", DecimalType(38,10), True),
    StructField("ELIG_CLRFCTN_CD", DecimalType(38,10), True),
    StructField("CMPND_CD", DecimalType(38,10), True),
    StructField("NO_OF_RFLS_AUTH", DecimalType(38,10), True),
    StructField("LVL_OF_SVC", DecimalType(38,10), True),
    StructField("RX_ORIG_CD", DecimalType(38,10), True),
    StructField("RX_DENIAL_CLRFCTN", DecimalType(38,10), True),
    StructField("PRI_PRSCRBR", StringType(), True),
    StructField("CLNC_ID_NO", DecimalType(38,10), True),
    StructField("DRUG_TYP", DecimalType(38,10), True),
    StructField("PRSCRBR_LAST_NM", StringType(), True),
    StructField("POSTAGE_AMT", DecimalType(38,10), True),
    StructField("UNIT_DOSE_IN", DecimalType(38,10), True),
    StructField("OTHR_PAYOR_AMT", DecimalType(38,10), True),
    StructField("BSS_OF_DAYS_SUPL_DTRM", DecimalType(38,10), True),
    StructField("FULL_AVG_WHLSL_PRICE", DecimalType(38,10), True),
    StructField("EXPNSN_AREA", StringType(), True),
    StructField("MSTR_CAR", StringType(), True),
    StructField("SUBCAR", StringType(), True),
    StructField("CLM_TYP", StringType(), True),
    StructField("SUBGRP", StringType(), True),
    StructField("PLN_DSGNR", StringType(), True),
    StructField("ADJDCT_DT", StringType(), True),
    StructField("ADMIN_FEE_AMT", DecimalType(38,10), True),
    StructField("CAP_AMT", DecimalType(38,10), True),
    StructField("INGR_CST_SUB_AMT", DecimalType(38,10), True),
    StructField("MBR_NON_COPAY_AMT", DecimalType(38,10), True),
    StructField("MBR_PAY_CD", StringType(), True),
    StructField("INCNTV_FEE_AMT", DecimalType(38,10), True),
    StructField("CLM_ADJ_AMT", DecimalType(38,10), True),
    StructField("CLM_ADJ_CD", StringType(), True),
    StructField("FRMLRY_FLAG", StringType(), True),
    StructField("GNRC_CLS_NO", StringType(), True),
    StructField("THRPTC_CLS_AHFS", StringType(), True),
    StructField("PDX_TYP", StringType(), True),
    StructField("BILL_BSS_CD", StringType(), True),
    StructField("USL_AND_CUST_CHRG_AMT", DecimalType(38,10), True),
    StructField("PD_DT", StringType(), True),
    StructField("BNF_CD", StringType(), True),
    StructField("DRUG_STRG", StringType(), True),
    StructField("ORIG_MBR", StringType(), True),
    StructField("INJRY_DT", StringType(), True),
    StructField("FEE_AMT", DecimalType(38,10), True),
    StructField("REF_NO", StringType(), True),
    StructField("CLNT_CUST_ID", StringType(), True),
    StructField("PLN_TYP", StringType(), True),
    StructField("ADJDCT_REF_NO", DecimalType(38,10), True),
    StructField("ANCLRY_AMT", DecimalType(38,10), True),
    StructField("CLNT_GNRL_PRPS_AREA", StringType(), True),
    StructField("PRTL_FILL_STTUS_CD", StringType(), True),
    StructField("BILL_DT", StringType(), True),
    StructField("FSA_VNDR_CD", StringType(), True),
    StructField("PICA_DRUG_CD", StringType(), True),
    StructField("CLM_AMT", DecimalType(38,10), True),
    StructField("DSALW_AMT", DecimalType(38,10), True),
    StructField("FED_DRUG_CLS_CD", StringType(), True),
    StructField("DEDCT_AMT", DecimalType(38,10), True),
    StructField("BNF_COPAY_100", StringType(), True),
    StructField("CLM_PRCS_TYP", StringType(), True),
    StructField("INDEM_HIER_TIER_NO", DecimalType(38,10), True),
    StructField("MCARE_D_COV_DRUG", StringType(), True),
    StructField("RETRO_LICS_CD", StringType(), True),
    StructField("RETRO_LICS_AMT", DecimalType(38,10), True),
    StructField("LICS_SBSDY_AMT", DecimalType(38,10), True),
    StructField("MCARE_B_DRUG", StringType(), True),
    StructField("MCARE_B_CLM", StringType(), True),
    StructField("PRSCRBR_QLFR", StringType(), True),
    StructField("PRSCRBR_NTNL_PROV_ID", StringType(), True),
    StructField("PDX_QLFR", StringType(), True),
    StructField("PDX_NTNL_PROV_ID", StringType(), True),
    StructField("HLTH_RMBRMT_ARGMT_APLD_AMT", DecimalType(38,10), True),
    StructField("THER_CLS", DecimalType(38,10), True),
    StructField("HIC_NO", StringType(), True),
    StructField("HLTH_RMBRMT_ARGMT_FLAG", StringType(), True),
    StructField("DOSE_CD", DecimalType(38,10), True),
    StructField("LOW_INCM", StringType(), True),
    StructField("RTE_OF_ADMIN", StringType(), True),
    StructField("DEA_SCHD", DecimalType(38,10), True),
    StructField("COPAY_BNF_OPT", DecimalType(38,10), True),
    StructField("GNRC_PROD_IN", DecimalType(38,10), True),
    StructField("PRSCRBR_SPEC", StringType(), True),
    StructField("VAL_CD", StringType(), True),
    StructField("PRI_CARE_PDX", StringType(), True),
    StructField("OFC_OF_INSPECTOR_GNRL", StringType(), True),
    StructField("PATN_SSN", StringType(), True),
    StructField("CARDHLDR_SSN", StringType(), True),
    StructField("CARDHLDR_BRTH_DT", StringType(), True),
    StructField("CARDHLDR_ADDR", StringType(), True),
    StructField("CARDHLDR_CITY", StringType(), True),
    StructField("CHADHLDR_ST", StringType(), True),
    StructField("CARDHLDR_ZIP_CD", StringType(), True),
    StructField("PSL_FMLY_MET_AMT", DecimalType(38,10), True),
    StructField("PSL_MBR_MET_AMT", DecimalType(38,10), True),
    StructField("PSL_FMLY_AMT", DecimalType(38,10), True),
    StructField("DEDCT_FMLY_MET_AMT", StringType(), True),
    StructField("DEDCT_FMLY_AMT", DecimalType(38,10), True),
    StructField("MOPS_FMLY_AMT", DecimalType(38,10), True),
    StructField("MOPS_FMLY_MET_AMT", DecimalType(38,10), True),
    StructField("MOPS_MBR_MET_AMT", DecimalType(38,10), True),
    StructField("DEDCT_MBR_MET_AMT", DecimalType(38,10), True),
    StructField("PSL_APLD_AMT", DecimalType(38,10), True),
    StructField("MOPS_APLD_AMT", DecimalType(38,10), True),
    StructField("PAR_PDX_IN", StringType(), True),
    StructField("COPAY_PCT_AMT", DecimalType(38,10), True),
    StructField("COPAY_FLAT_AMT", DecimalType(38,10), True),
    StructField("CLM_TRNSMSN_METH", StringType(), True),
    StructField("RX_NO_2012", DecimalType(38,10), True),
    StructField("CLM_ID", StringType(), True),
    StructField("CLM_STTUS_CD", StringType(), True),
    StructField("ADJ_FROM_CLM_ID", StringType(), True),
    StructField("ADJ_TO_CLM_ID", StringType(), True),
    StructField("SUBMT_PROD_ID_QLFR", StringType(), True),
    StructField("CNTNGNT_THER_FLAG", StringType(), True),
    StructField("CNTNGNT_THER_SCHD", StringType(), True),
    StructField("CLNT_PATN_PAY_ATRBD_PROD_AMT", DecimalType(38,10), True),
    StructField("CLNT_PATN_PAY_ATRBD_SLS_TAX_AMT", DecimalType(38,10), True),
    StructField("CLNT_PATN_PAY_ATRBD_PRCSR_FEE_AMT", DecimalType(38,10), True),
    StructField("CLNT_PATN_PAY_ATRBD_NTWK_AMT", DecimalType(38,10), True),
    StructField("LOB_IN", StringType(), True)
])

df_BCBSCommClmLand = (
    spark.read.format("csv")
    .option("header", "false")
    .option("quote", "\"")
    .schema(schema_BCBSCommClmLand)
    .load(f"{adls_path}/verified/PDX_CLM_STD_INPT_Land_{SrcSysCd}.dat.{RunID}")
)

df_bcbskccomm = df_BCBSCommClmLand

df_adjustments = df_bcbskccomm.filter(
    (F.substring(F.col("CLM_TYP"), 1, 1) == "X") |
    (F.substring(F.col("CLM_TYP"), 1, 1) == "R")
).select(
    F.lit(SrcSysCdSK).alias("SRC_SYS_CD_SK"),
    F.substring(F.trim(F.col("CLM_ID")), 1, F.length(F.trim(F.col("CLM_ID"))) - 1).alias("CLM_ID")
)

df_regular = df_bcbskccomm.select(
    F.lit(SrcSysCdSK).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID")
)

df_collector = df_adjustments.select("SRC_SYS_CD_SK","CLM_ID").union(
    df_regular.select("SRC_SYS_CD_SK","CLM_ID")
)

df_dedup = df_collector.dropDuplicates(["SRC_SYS_CD_SK","CLM_ID"])

# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmLoadPK
# COMMAND ----------

params_ClmLoadPK = {
    "SrcSysCd": SrcSysCd,
    "SrcSysCdSK": SrcSysCdSK,
    "CurrRunCycle": CurrRunCycle,
    "IDSOwner": IDSOwner
}

df_clmLoadPK = ClmLoadPK(df_dedup, params_ClmLoadPK)

df_final = df_clmLoadPK.select(
    F.rpad(F.col("SRC_SYS_CD"), F.lit(<...>), F.lit(" ")).alias("SRC_SYS_CD"),
    F.rpad(F.col("CLM_ID"), F.lit(<...>), F.lit(" ")).alias("CLM_ID"),
    "CRT_RUN_CYC_EXCTN_SK",
    "CLM_SK"
)

write_files(
    df_final,
    "hf_clm_pk_lkup.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)