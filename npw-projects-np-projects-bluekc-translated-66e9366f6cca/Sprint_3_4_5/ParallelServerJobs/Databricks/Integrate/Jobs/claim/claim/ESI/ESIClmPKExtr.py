# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Copyright 2008, 2019 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  ESIDrugExtrSeq
# MAGIC                
# MAGIC 
# MAGIC PROCESSING:   
# MAGIC          *  This job must be run after ESIClmLand
# MAGIC          *  UNIX file K_CLM.dat is removed in Before-job to remove previous keys incase of earlier abend.  File writting is set to append.  
# MAGIC          *  Assigns / creates claim primary key surrogate keys for all records processed in current run.  Adjusted to/from claims don't need to be in hash file. 
# MAGIC          *  The primary key hash file hf_clm is the output of this job and is used by the following tables for keying
# MAGIC              CLM
# MAGIC              DRUG_CLM
# MAGIC              CLM_REMIT_HIST
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                   Date             Project/Altiris #        Change Description\(9)\(9)\(9)\(9)Development Project     Code Reviewer\(9)       Date Reviewed       
# MAGIC ------------------                 ------------------   -----------------------------   --------------------------------------------------------------------------------------\(9)----------------------------------    ------------------------------   ----------------------------       
# MAGIC Brent Leland\(9)   2008-11-18   3567 Primary Key     Original Programming                                                       devlIDS                         Steph Goddard         10/03/2008
# MAGIC Tracy Davis                2009-04-09   TTR 477                  New field CML_TRANSMITTAL_METH                         devlIDS                         Steph Goddard         04/14/2009
# MAGIC SAndrew                     2014-01-30                                   added hf_esi_clm_pkextr_dedup                                    devlnew                         Bhoomi Dasari          4/16/2014   
# MAGIC                                                                                         changed the source of the original claim id to be
# MAGIC                                                                                         from the field CROSS_REFERENCE_ID.  
# MAGIC                                                                                         And only if it does not equal 18 zeros.
# MAGIC Shashank Akinapalli   2019-04-10   97615                      Adding CLM_LN_VBB_IN & adjusting the Filler length.    IntegrateDev2                Hugh Sisson            2019-05-02

# MAGIC ESI Claim Primary Key Process
# MAGIC This job loads primary key hash file with all keys used by tables with natural keys of source system code and claim ID
# MAGIC Output required by container but not used here
# MAGIC Get SK for records with out keys
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmLoadPK

SrcSysCd = get_widget_value("SrcSysCd","")
SrcSysCdSK = get_widget_value("SrcSysCdSK","")
CurrRunCycle = get_widget_value("CurrRunCycle","")
RunID = get_widget_value("RunID","")
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")

schema_ESIClmLand = StructType([
    StructField("RCRD_ID", DoubleType(), False),
    StructField("CLAIM_ID", StringType(), False),
    StructField("PRCSR_NO", DoubleType(), False),
    StructField("MEM_CK_KEY", IntegerType(), False),
    StructField("BTCH_NO", DoubleType(), False),
    StructField("PDX_NO", StringType(), False),
    StructField("RX_NO", DoubleType(), False),
    StructField("DT_FILLED", StringType(), False),
    StructField("NDC_NO", DoubleType(), False),
    StructField("DRUG_DESC", StringType(), False),
    StructField("NEW_RFL_CD", DoubleType(), False),
    StructField("METRIC_QTY", DoubleType(), False),
    StructField("DAYS_SUPL", DoubleType(), False),
    StructField("BSS_OF_CST_DTRM", StringType(), False),
    StructField("INGR_CST", DoubleType(), False),
    StructField("DISPNS_FEE", DoubleType(), False),
    StructField("COPAY_AMT", DoubleType(), False),
    StructField("SLS_TAX", DoubleType(), False),
    StructField("AMT_BILL", DoubleType(), False),
    StructField("PATN_FIRST_NM", StringType(), False),
    StructField("PATN_LAST_NM", StringType(), False),
    StructField("DOB", StringType(), False),
    StructField("SEX_CD", DoubleType(), False),
    StructField("CARDHLDR_ID_NO", StringType(), False),
    StructField("RELSHP_CD", DoubleType(), False),
    StructField("GRP_NO", StringType(), False),
    StructField("HOME_PLN", StringType(), False),
    StructField("HOST_PLN", DoubleType(), False),
    StructField("PRESCRIBER_ID", StringType(), False),
    StructField("DIAG_CD", StringType(), False),
    StructField("CARDHLDR_FIRST_NM", StringType(), False),
    StructField("CARDHLDR_LAST_NM", StringType(), False),
    StructField("PRAUTH_NO", DoubleType(), False),
    StructField("PA_MC_SC_NO", StringType(), False),
    StructField("CUST_LOC", DoubleType(), False),
    StructField("RESUB_CYC_CT", DoubleType(), False),
    StructField("DT_RX_WRTN", StringType(), False),
    StructField("DISPENSE_AS_WRTN_PROD_SEL_CD", StringType(), False),
    StructField("PRSN_CD", StringType(), False),
    StructField("OTHR_COV_CD", DoubleType(), False),
    StructField("ELIG_CLRFCTN_CD", DoubleType(), False),
    StructField("CMPND_CD", DoubleType(), False),
    StructField("NO_OF_RFLS_AUTH", DoubleType(), False),
    StructField("LVL_OF_SVC", DoubleType(), False),
    StructField("RX_ORIG_CD", DoubleType(), False),
    StructField("RX_DENIAL_CLRFCTN", DoubleType(), False),
    StructField("PRI_PRESCRIBER", StringType(), False),
    StructField("CLNC_ID_NO", DoubleType(), False),
    StructField("DRUG_TYP", DoubleType(), False),
    StructField("PRESCRIBER_LAST_NM", StringType(), False),
    StructField("POSTAGE_AMT_CLMED", DoubleType(), False),
    StructField("UNIT_DOSE_IN", DoubleType(), False),
    StructField("OTHR_PAYOR_AMT", DoubleType(), False),
    StructField("BSS_OF_DAYS_SUPL_DTRM", DoubleType(), False),
    StructField("FULL_AWP", DoubleType(), False),
    StructField("EXPNSN_AREA", StringType(), False),
    StructField("MSTR_CAR", StringType(), False),
    StructField("SUB_CAR", StringType(), False),
    StructField("CLM_TYP", StringType(), False),
    StructField("ESI_SUB_GRP", StringType(), False),
    StructField("PLN_DSGNR", StringType(), False),
    StructField("ADJDCT_DT", StringType(), False),
    StructField("ADMIN_FEE", DoubleType(), False),
    StructField("CAP_AMT", DoubleType(), False),
    StructField("INGR_CST_SUB", DoubleType(), False),
    StructField("MBR_NON_COPAY_AMT", DoubleType(), False),
    StructField("MBR_PAY_CD", StringType(), False),
    StructField("INCNTV_FEE", DoubleType(), False),
    StructField("CLM_ADJ_AMT", DoubleType(), False),
    StructField("CLM_ADJ_CD", StringType(), False),
    StructField("FRMLRY_FLAG", StringType(), False),
    StructField("GNRC_CLS_NO", StringType(), False),
    StructField("THRPTC_CLS_AHFS", StringType(), False),
    StructField("PDX_TYP", StringType(), False),
    StructField("BILL_BSS_CD", StringType(), False),
    StructField("USL_AND_CUST_CHRG", DoubleType(), False),
    StructField("PD_DT", StringType(), False),
    StructField("BNF_CD", StringType(), False),
    StructField("DRUG_STRG", StringType(), False),
    StructField("ORIG_MBR", StringType(), False),
    StructField("DT_OF_INJURY", StringType(), False),
    StructField("FEE_AMT", DoubleType(), False),
    StructField("ESI_REF_NO", StringType(), False),
    StructField("CLNT_CUST_ID", StringType(), False),
    StructField("PLN_TYP", StringType(), False),
    StructField("ESI_ADJDCT_REF_NO", DoubleType(), False),
    StructField("ESI_ANCLRY_AMT", DoubleType(), False),
    StructField("GRP_ID", StringType(), False),
    StructField("SUBGRP_ID", StringType(), False),
    StructField("CLS_PLN_ID", StringType(), False),
    StructField("PAID_DATE", StringType(), False),
    StructField("PRTL_FILL_STTUS_CD", StringType(), False),
    StructField("ESI_BILL_DT", StringType(), False),
    StructField("FSA_VNDR_CD", StringType(), False),
    StructField("PICA_DRUG_CD", StringType(), False),
    StructField("AMT_CLMED", DoubleType(), False),
    StructField("AMT_DSALW", DoubleType(), False),
    StructField("FED_DRUG_CLS_CD", StringType(), False),
    StructField("DEDCT_AMT", DoubleType(), False),
    StructField("BNF_COPAY_100", StringType(), False),
    StructField("CLM_PRCS_TYP", StringType(), False),
    StructField("INDEM_HIER_TIER_NO", DoubleType(), False),
    StructField("FLR", StringType(), False),
    StructField("MCARE_D_COV_DRUG", StringType(), False),
    StructField("RETRO_LICS_CD", StringType(), False),
    StructField("RETRO_LICS_AMT", DoubleType(), False),
    StructField("LICS_SBSDY_AMT", DoubleType(), False),
    StructField("MED_B_DRUG", StringType(), False),
    StructField("MED_B_CLM", StringType(), False),
    StructField("PRESCRIBER_QLFR", StringType(), False),
    StructField("PRESCRIBER_ID_NPI", StringType(), False),
    StructField("PDX_QLFR", StringType(), False),
    StructField("PDX_ID_NPI", StringType(), False),
    StructField("HRA_APLD_AMT", DoubleType(), False),
    StructField("ESI_THER_CLS", DoubleType(), False),
    StructField("HIC_NO", StringType(), False),
    StructField("HRA_FLAG", StringType(), False),
    StructField("DOSE_CD", DoubleType(), False),
    StructField("LOW_INCM", StringType(), False),
    StructField("RTE_OF_ADMIN", StringType(), False),
    StructField("DEA_SCHD", DoubleType(), False),
    StructField("COPAY_BNF_OPT", DoubleType(), False),
    StructField("GNRC_PROD_IN_GPI", DoubleType(), False),
    StructField("PRESCRIBER_SPEC", StringType(), False),
    StructField("VAL_CD", StringType(), False),
    StructField("PRI_CARE_PDX", StringType(), False),
    StructField("OFC_OF_INSPECTOR_GNRL_OIG", StringType(), False),
    StructField("FLR3", StringType(), False),
    StructField("PSL_FMLY_MET_AMT", DoubleType(), False),
    StructField("PSL_MBR_MET_AMT", DoubleType(), False),
    StructField("PSL_FMLY_AMT", DoubleType(), False),
    StructField("DED_FMLY_MET_AMT", DoubleType(), False),
    StructField("DED_FMLY_AMT", DoubleType(), False),
    StructField("MOPS_FMLY_AMT", DoubleType(), False),
    StructField("MOPS_FMLY_MET_AMT", DoubleType(), False),
    StructField("MOPS_MBR_MET_AMT", DoubleType(), False),
    StructField("DED_MBR_MET_AMT", DoubleType(), False),
    StructField("PSL_APLD_AMT", DoubleType(), False),
    StructField("MOPS_APLD_AMT", DoubleType(), False),
    StructField("PAR_PDX_IND", StringType(), False),
    StructField("COPAY_PCT_AMT", DoubleType(), False),
    StructField("COPAY_FLAT_AMT", DoubleType(), False),
    StructField("CLM_TRANSMITTAL_METH", StringType(), False),
    StructField("PRESCRIPTION_NBR_2", StringType(), False),
    StructField("TRANSACTION_ID", StringType(), False),
    StructField("CROSS_REF_ID", StringType(), False),
    StructField("ADJDCT_TIMESTAMP", StringType(), False),
    StructField("CLM_LN_VBB_IN", StringType(), False),
    StructField("FLR4", StringType(), False)
])

df_ESIClmLand = (
    spark.read
    .option("header", False)
    .option("delimiter", ",")
    .option("quote", "\"")
    .schema(schema_ESIClmLand)
    .csv(f"{adls_path}/verified/ESIDrugClmDaily_Land.dat." + RunID)
)

df_Trans2_Regular = df_ESIClmLand.select(
    lit(SrcSysCdSK).cast(IntegerType()).alias("SRC_SYS_CD_SK"),
    col("CLAIM_ID").alias("CLM_ID")
)

df_Trans2_Adjusting_From = df_ESIClmLand.filter(
    col("CROSS_REF_ID") != "000000000000000000"
).select(
    lit(SrcSysCdSK).cast(IntegerType()).alias("SRC_SYS_CD_SK"),
    col("CROSS_REF_ID").alias("CLM_ID")
)

df_Collector_merged_clmids = df_Trans2_Adjusting_From.unionByName(df_Trans2_Regular)

df_dedup_esi_clm_pkextr = df_Collector_merged_clmids.dropDuplicates(["SRC_SYS_CD_SK", "CLM_ID"])

params_ClmLoadPK = {
    "SrcSysCd": SrcSysCd,
    "SrcSysCdSK": SrcSysCdSK,
    "CurrRunCycle": CurrRunCycle,
    "$IDSOwner": IDSOwner
}

df_ClmLoadPK_out = ClmLoadPK(df_dedup_esi_clm_pkextr, params_ClmLoadPK)

df_hf_clm_pk_lkup_out = df_ClmLoadPK_out.select(
    col("SRC_SYS_CD"),
    rpad(col("CLM_ID"), <...>, " ").alias("CLM_ID"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("CLM_SK")
)

write_files(
    df_hf_clm_pk_lkup_out,
    "hf_clm_pk_lkup.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)