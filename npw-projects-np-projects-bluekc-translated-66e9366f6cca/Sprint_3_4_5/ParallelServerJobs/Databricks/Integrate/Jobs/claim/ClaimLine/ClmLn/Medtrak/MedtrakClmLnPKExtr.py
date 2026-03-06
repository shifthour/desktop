# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
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
# MAGIC Developer                Date                 \(9)Project/Altiris #\(9)Change Description\(9)\(9)\(9)\(9)Development Project\(9)Code Reviewer\(9)Date Reviewed       
# MAGIC ------------------              --------------------     \(9)------------------------\(9)-----------------------------------------------------------------------\(9)--------------------------------\(9)-------------------------------\(9)----------------------------       
# MAGIC Kalyan Neelam         2010-12-22              4616                         Initial Programming                                                  IntegrateNewDevl                   Steph Goddard       12/23/2010
# MAGIC Raja Gummadi          2012-07-23             TTR 1330               Changed RX_NO field size from 9 to 20 in input file    IntegrateWrhsDevl                Bhoomi Dasari         08/08/2012

# MAGIC Get SK for primary key on input record
# MAGIC MEDTRAK Claim Primary Key Process
# MAGIC This job loads primary key hash file with all keys used by tables with natural keys of source system code claim ID and seq_no
# MAGIC Output required by container but not used here
# MAGIC This container is used in:
# MAGIC ESIClmLnPKExtr
# MAGIC FctsClmLnPKExtr
# MAGIC FctsClmLnRemitPKExtr
# MAGIC MCSourceClmLnPKExtr
# MAGIC MedicaidClmLnPKExtr
# MAGIC NascoClmLnPKExtr
# MAGIC PcsClmLnPKExtr
# MAGIC WellDyneClmLnPKExtr
# MAGIC MedtrakClmLnPKExtr
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DecimalType,
    IntegerType
)
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../../shared_containers/PrimaryKey/ClmLnLoadPK
# COMMAND ----------

SrcSysCd = get_widget_value('SrcSysCd','MEDTRAK')
SrcSysCdSK = get_widget_value('SrcSysCdSK','')
CurrRunCycle = get_widget_value('CurrRunCycle','100')
RunID = get_widget_value('RunID','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

schema_MedtrakClmLand = StructType([
    StructField("RCRD_ID", DecimalType(38,10), False),
    StructField("CLAIM_ID", StringType(), False),
    StructField("PRCSR_NO", DecimalType(38,10), False),
    StructField("MEM_CK_KEY", IntegerType(), False),
    StructField("BTCH_NO", DecimalType(38,10), False),
    StructField("PDX_NO", StringType(), False),
    StructField("RX_NO", DecimalType(38,10), False),
    StructField("DT_FILLED", StringType(), False),
    StructField("NDC_NO", DecimalType(38,10), False),
    StructField("DRUG_DESC", StringType(), False),
    StructField("NEW_RFL_CD", DecimalType(38,10), False),
    StructField("METRIC_QTY", DecimalType(38,10), False),
    StructField("DAYS_SUPL", DecimalType(38,10), False),
    StructField("BSS_OF_CST_DTRM", StringType(), False),
    StructField("INGR_CST", DecimalType(38,10), False),
    StructField("DISPNS_FEE", DecimalType(38,10), False),
    StructField("COPAY_AMT", DecimalType(38,10), False),
    StructField("SLS_TAX", DecimalType(38,10), False),
    StructField("AMT_BILL", DecimalType(38,10), False),
    StructField("PATN_FIRST_NM", StringType(), False),
    StructField("PATN_LAST_NM", StringType(), False),
    StructField("DOB", DecimalType(38,10), False),
    StructField("SEX_CD", DecimalType(38,10), False),
    StructField("CARDHLDR_ID_NO", StringType(), False),
    StructField("RELSHP_CD", DecimalType(38,10), False),
    StructField("GRP_NO", StringType(), False),
    StructField("HOME_PLN", StringType(), False),
    StructField("HOST_PLN", DecimalType(38,10), False),
    StructField("PRESCRIBER_ID", StringType(), False),
    StructField("DIAG_CD", StringType(), False),
    StructField("CARDHLDR_FIRST_NM", StringType(), False),
    StructField("CARDHLDR_LAST_NM", StringType(), False),
    StructField("PRAUTH_NO", DecimalType(38,10), False),
    StructField("PA_MC_SC_NO", StringType(), False),
    StructField("CUST_LOC", DecimalType(38,10), False),
    StructField("RESUB_CYC_CT", DecimalType(38,10), False),
    StructField("DT_RX_WRTN", DecimalType(38,10), False),
    StructField("DISPENSE_AS_WRTN_PROD_SEL_CD", StringType(), False),
    StructField("PRSN_CD", StringType(), False),
    StructField("OTHR_COV_CD", DecimalType(38,10), False),
    StructField("ELIG_CLRFCTN_CD", DecimalType(38,10), False),
    StructField("CMPND_CD", DecimalType(38,10), False),
    StructField("NO_OF_RFLS_AUTH", DecimalType(38,10), False),
    StructField("LVL_OF_SVC", DecimalType(38,10), False),
    StructField("RX_ORIG_CD", DecimalType(38,10), False),
    StructField("RX_DENIAL_CLRFCTN", DecimalType(38,10), False),
    StructField("PRI_PRESCRIBER", StringType(), False),
    StructField("CLNC_ID_NO", DecimalType(38,10), False),
    StructField("DRUG_TYP", DecimalType(38,10), False),
    StructField("PRESCRIBER_LAST_NM", StringType(), False),
    StructField("POSTAGE_AMT_CLMED", DecimalType(38,10), False),
    StructField("UNIT_DOSE_IN", DecimalType(38,10), False),
    StructField("OTHR_PAYOR_AMT", DecimalType(38,10), False),
    StructField("BSS_OF_DAYS_SUPL_DTRM", DecimalType(38,10), False),
    StructField("FULL_AWP", DecimalType(38,10), False),
    StructField("EXPNSN_AREA", StringType(), False),
    StructField("MSTR_CAR", StringType(), False),
    StructField("SUB_CAR", StringType(), False),
    StructField("CLM_TYP", StringType(), False),
    StructField("ESI_SUB_GRP", StringType(), False),
    StructField("PLN_DSGNR", StringType(), False),
    StructField("ADJDCT_DT", StringType(), False),
    StructField("ADMIN_FEE", DecimalType(38,10), False),
    StructField("CAP_AMT", DecimalType(38,10), False),
    StructField("INGR_CST_SUB", DecimalType(38,10), False),
    StructField("MBR_NON_COPAY_AMT", DecimalType(38,10), False),
    StructField("MBR_PAY_CD", StringType(), False),
    StructField("INCNTV_FEE", DecimalType(38,10), False),
    StructField("CLM_ADJ_AMT", DecimalType(38,10), False),
    StructField("CLM_ADJ_CD", StringType(), False),
    StructField("FRMLRY_FLAG", StringType(), False),
    StructField("GNRC_CLS_NO", StringType(), False),
    StructField("THRPTC_CLS_AHFS", StringType(), False),
    StructField("PDX_TYP", StringType(), False),
    StructField("BILL_BSS_CD", StringType(), False),
    StructField("USL_AND_CUST_CHRG", DecimalType(38,10), False),
    StructField("PD_DT", StringType(), False),
    StructField("BNF_CD", StringType(), False),
    StructField("DRUG_STRG", StringType(), False),
    StructField("ORIG_MBR", StringType(), False),
    StructField("DT_OF_INJURY", DecimalType(38,10), False),
    StructField("FEE_AMT", DecimalType(38,10), False),
    StructField("ESI_REF_NO", StringType(), False),
    StructField("CLNT_CUST_ID", StringType(), False),
    StructField("PLN_TYP", StringType(), False),
    StructField("ESI_ADJDCT_REF_NO", DecimalType(38,10), False),
    StructField("ESI_ANCLRY_AMT", DecimalType(38,10), False),
    StructField("ESI_CLNT_GNRL_PRPS_AREA", StringType(), True),
    StructField("GRP_ID", StringType(), False),
    StructField("SUBGRP_ID", StringType(), False),
    StructField("CLS_PLN_ID", StringType(), False),
    StructField("PAID_DATE", StringType(), False),
    StructField("PRTL_FILL_STTUS_CD", StringType(), False),
    StructField("ESI_BILL_DT", DecimalType(38,10), False),
    StructField("FSA_VNDR_CD", StringType(), False),
    StructField("PICA_DRUG_CD", StringType(), False),
    StructField("AMT_CLMED", DecimalType(38,10), False),
    StructField("AMT_DSALW", DecimalType(38,10), False),
    StructField("FED_DRUG_CLS_CD", StringType(), False),
    StructField("DEDCT_AMT", DecimalType(38,10), False),
    StructField("BNF_COPAY_100", StringType(), False),
    StructField("CLM_PRCS_TYP", StringType(), False),
    StructField("INDEM_HIER_TIER_NO", DecimalType(38,10), False),
    StructField("FLR", StringType(), False),
    StructField("MCARE_D_COV_DRUG", StringType(), False),
    StructField("RETRO_LICS_CD", StringType(), False),
    StructField("RETRO_LICS_AMT", DecimalType(38,10), False),
    StructField("LICS_SBSDY_AMT", DecimalType(38,10), False),
    StructField("MED_B_DRUG", StringType(), False),
    StructField("MED_B_CLM", StringType(), False),
    StructField("PRESCRIBER_QLFR", StringType(), False),
    StructField("PRESCRIBER_ID_NPI", StringType(), False),
    StructField("PDX_QLFR", StringType(), False),
    StructField("PDX_ID_NPI", StringType(), False),
    StructField("HRA_APLD_AMT", DecimalType(38,10), False),
    StructField("ESI_THER_CLS", DecimalType(38,10), False),
    StructField("HIC_NO", StringType(), False),
    StructField("HRA_FLAG", StringType(), False),
    StructField("DOSE_CD", DecimalType(38,10), False),
    StructField("LOW_INCM", StringType(), False),
    StructField("RTE_OF_ADMIN", StringType(), False),
    StructField("DEA_SCHD", DecimalType(38,10), False),
    StructField("COPAY_BNF_OPT", DecimalType(38,10), False),
    StructField("GNRC_PROD_IN_GPI", DecimalType(38,10), False),
    StructField("PRESCRIBER_SPEC", StringType(), False),
    StructField("VAL_CD", StringType(), False),
    StructField("PRI_CARE_PDX", StringType(), False),
    StructField("OFC_OF_INSPECTOR_GNRL_OIG", StringType(), False),
    StructField("FLR3", StringType(), False),
    StructField("PSL_FMLY_MET_AMT", DecimalType(38,10), False),
    StructField("PSL_MBR_MET_AMT", DecimalType(38,10), False),
    StructField("PSL_FMLY_AMT", DecimalType(38,10), False),
    StructField("DED_FMLY_MET_AMT", DecimalType(38,10), False),
    StructField("DED_FMLY_AMT", DecimalType(38,10), False),
    StructField("MOPS_FMLY_AMT", DecimalType(38,10), False),
    StructField("MOPS_FMLY_MET_AMT", DecimalType(38,10), False),
    StructField("MOPS_MBR_MET_AMT", DecimalType(38,10), False),
    StructField("DED_MBR_MET_AMT", DecimalType(38,10), False),
    StructField("PSL_APLD_AMT", DecimalType(38,10), False),
    StructField("MOPS_APLD_AMT", DecimalType(38,10), False),
    StructField("PAR_PDX_IND", StringType(), False),
    StructField("COPAY_PCT_AMT", DecimalType(38,10), False),
    StructField("COPAY_FLAT_AMT", DecimalType(38,10), False),
    StructField("CLM_TRANSMITTAL_METH", StringType(), False),
    StructField("FLR4", StringType(), False)
])

df_MedtrakClmLand = (
    spark.read.format("csv")
    .option("header", "false")
    .option("quote", "\"")
    .option("delimiter", ",")
    .schema(schema_MedtrakClmLand)
    .load(f"{adls_path}/verified/MedtrakDrugClm_Land.dat.{RunID}")
)

df_adjustments = (
    df_MedtrakClmLand
    .filter(F.substring(F.col("CLM_TYP"), 1, 1) == 'R')
    .select(
        F.lit(SrcSysCdSK).alias("SRC_SYS_CD_SK"),
        F.substring(F.trim(F.col("CLAIM_ID")), 1, F.length(F.trim(F.col("CLAIM_ID"))) - 1).alias("CLM_ID"),
        F.lit(1).alias("CLM_LN_SEQ_NO")
    )
)

df_regular = (
    df_MedtrakClmLand
    .select(
        F.lit(SrcSysCdSK).alias("SRC_SYS_CD_SK"),
        F.col("CLAIM_ID").alias("CLM_ID"),
        F.lit(1).alias("CLM_LN_SEQ_NO")
    )
)

df_collector = df_adjustments.unionByName(df_regular)

params_ClmLnLoadPK = {
    "SrcSysCd": SrcSysCd,
    "SrcSysCdSK": SrcSysCdSK,
    "CurrRunCycle": CurrRunCycle,
    "IDSOwner": IDSOwner
}

df_ClmLnLoadPK_output = ClmLnLoadPK(df_collector, params_ClmLnLoadPK)

df_hf_clm_ln_pk_lkup = df_ClmLnLoadPK_output.select(
    F.rpad(F.col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    F.rpad(F.col("CLM_ID"), <...>, " ").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_LN_SK").alias("CLM_LN_SK")
)

write_files(
    df_hf_clm_ln_pk_lkup,
    f"{adls_path}/hf_clm_ln_pk_lkup.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)