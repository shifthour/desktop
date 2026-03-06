# Databricks notebook source
# MAGIC %md
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
# MAGIC
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

# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmLoadPK

# COMMAND ----------

from pyspark.sql.functions import col, lit, length, substring, rpad, when

# COMMAND ----------

# MAGIC %run ../../../../../Utility_Integrate

# COMMAND ----------

# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)

# COMMAND ----------

SRC_SYS_CD = get_widget_value('SrcSysCd','SAVRX')
SrcSysCdSK = get_widget_value('SrcSysCdSK','')
CurrRunCycle = get_widget_value('CurrRunCycle','100')
RunID = get_widget_value('RunID','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

SRC_SYS_CD = 'SAVRX'
SrcSysCdSK = get_widget_value('SrcSysCdSK','')
CurrRunCycle = get_widget_value('CurrRunCycle','100')
RunID = get_widget_value('RunID','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')


# COMMAND ----------



df_BCBSCommClmLand = (
    spark.read
    .option("header", "false")
    .option("sep", ",")
    .option("quote", "\"")
    .csv(f"{adls_path}/verified/PDX_CLM_STD_INPT_Land_{SRC_SYS_CD}.dat.{RunID}")
    .select(
        col("_c0").alias("SRC_SYS_CD"),
        rpad(col("_c1"),10," ").alias("FILE_RCVD_DT"),
        col("_c2").alias("RCRD_ID"),
        col("_c3").alias("PRCSR_NO"),
        col("_c4").alias("BTCH_NO"),
        col("_c5").alias("PDX_NO"),
        col("_c6").alias("RX_NO"),
        rpad(col("_c7"),10," ").alias("FILL_DT"),
        col("_c8").alias("NDC"),
        col("_c9").alias("DRUG_DESC"),
        col("_c10").alias("NEW_OR_RFL_CD"),
        col("_c11").alias("METRIC_QTY"),
        col("_c12").alias("DAYS_SUPL"),
        rpad(col("_c13"),2," ").alias("BSS_OF_CST_DTRM"),
        col("_c14").alias("INGR_CST_AMT"),
        col("_c15").alias("DISPNS_FEE_AMT"),
        col("_c16").alias("COPAY_AMT"),
        col("_c17").alias("SLS_TAX_AMT"),
        col("_c18").alias("BILL_AMT"),
        col("_c19").alias("PATN_FIRST_NM"),
        col("_c20").alias("PATN_LAST_NM"),
        rpad(col("_c21"),10," ").alias("BRTH_DT"),
        col("_c22").alias("SEX_CD"),
        col("_c23").alias("CARDHLDR_ID_NO"),
        col("_c24").alias("RELSHP_CD"),
        col("_c25").alias("GRP_NO"),
        col("_c26").alias("HOME_PLN"),
        col("_c27").alias("HOST_PLN"),
        col("_c28").alias("PRSCRBR_ID"),
        col("_c29").alias("DIAG_CD"),
        col("_c30").alias("CARDHLDR_FIRST_NM"),
        col("_c31").alias("CARDHLDR_LAST_NM"),
        col("_c32").alias("PRAUTH_NO"),
        col("_c33").alias("PA_MC_SC_NO"),
        col("_c34").alias("CUST_LOC"),
        col("_c35").alias("RESUB_CYC_CT"),
        rpad(col("_c36"),10," ").alias("RX_DT"),
        rpad(col("_c37"),1," ").alias("DISPENSE_AS_WRTN_PROD_SEL_CD"),
        col("_c38").alias("PRSN_CD"),
        col("_c39").alias("OTHR_COV_CD"),
        col("_c40").alias("ELIG_CLRFCTN_CD"),
        col("_c41").alias("CMPND_CD"),
        col("_c42").alias("NO_OF_RFLS_AUTH"),
        col("_c43").alias("LVL_OF_SVC"),
        col("_c44").alias("RX_ORIG_CD"),
        col("_c45").alias("RX_DENIAL_CLRFCTN"),
        col("_c46").alias("PRI_PRSCRBR"),
        col("_c47").alias("CLNC_ID_NO"),
        col("_c48").alias("DRUG_TYP"),
        col("_c49").alias("PRSCRBR_LAST_NM"),
        col("_c50").alias("POSTAGE_AMT"),
        col("_c51").alias("UNIT_DOSE_IN"),
        col("_c52").alias("OTHR_PAYOR_AMT"),
        col("_c53").alias("BSS_OF_DAYS_SUPL_DTRM"),
        col("_c54").alias("FULL_AVG_WHLSL_PRICE"),
        rpad(col("_c55"),1," ").alias("EXPNSN_AREA"),
        col("_c56").alias("MSTR_CAR"),
        col("_c57").alias("SUBCAR"),
        rpad(col("_c58"),1," ").alias("CLM_TYP"),
        col("_c59").alias("SUBGRP"),
        rpad(col("_c60"),1," ").alias("PLN_DSGNR"),
        rpad(col("_c61"),10," ").alias("ADJDCT_DT"),
        col("_c62").alias("ADMIN_FEE_AMT"),
        col("_c63").alias("CAP_AMT"),
        col("_c64").alias("INGR_CST_SUB_AMT"),
        col("_c65").alias("MBR_NON_COPAY_AMT"),
        rpad(col("_c66"),2," ").alias("MBR_PAY_CD"),
        col("_c67").alias("INCNTV_FEE_AMT"),
        col("_c68").alias("CLM_ADJ_AMT"),
        rpad(col("_c69"),2," ").alias("CLM_ADJ_CD"),
        rpad(col("_c70"),1," ").alias("FRMLRY_FLAG"),
        col("_c71").alias("GNRC_CLS_NO"),
        col("_c72").alias("THRPTC_CLS_AHFS"),
        rpad(col("_c73"),1," ").alias("PDX_TYP"),
        rpad(col("_c74"),2," ").alias("BILL_BSS_CD"),
        col("_c75").alias("USL_AND_CUST_CHRG_AMT"),
        rpad(col("_c76"),10," ").alias("PD_DT"),
        col("_c77").alias("BNF_CD"),
        col("_c78").alias("DRUG_STRG"),
        col("_c79").alias("ORIG_MBR"),
        rpad(col("_c80"),10," ").alias("INJRY_DT"),
        col("_c81").alias("FEE_AMT"),
        col("_c82").alias("REF_NO"),
        col("_c83").alias("CLNT_CUST_ID"),
        col("_c84").alias("PLN_TYP"),
        col("_c85").alias("ADJDCT_REF_NO"),
        col("_c86").alias("ANCLRY_AMT"),
        col("_c87").alias("CLNT_GNRL_PRPS_AREA"),
        rpad(col("_c88"),1," ").alias("PRTL_FILL_STTUS_CD"),
        rpad(col("_c89"),10," ").alias("BILL_DT"),
        col("_c90").alias("FSA_VNDR_CD"),
        rpad(col("_c91"),1," ").alias("PICA_DRUG_CD"),
        col("_c92").alias("CLM_AMT"),
        col("_c93").alias("DSALW_AMT"),
        rpad(col("_c94"),1," ").alias("FED_DRUG_CLS_CD"),
        col("_c95").alias("DEDCT_AMT"),
        rpad(col("_c96"),1," ").alias("BNF_COPAY_100"),
        rpad(col("_c97"),1," ").alias("CLM_PRCS_TYP"),
        col("_c98").alias("INDEM_HIER_TIER_NO"),
        rpad(col("_c99"),1," ").alias("MCARE_D_COV_DRUG"),
        rpad(col("_c100"),1," ").alias("RETRO_LICS_CD"),
        col("_c101").alias("RETRO_LICS_AMT"),
        col("_c102").alias("LICS_SBSDY_AMT"),
        rpad(col("_c103"),1," ").alias("MCARE_B_DRUG"),
        rpad(col("_c104"),1," ").alias("MCARE_B_CLM"),
        rpad(col("_c105"),2," ").alias("PRSCRBR_QLFR"),
        col("_c106").alias("PRSCRBR_NTNL_PROV_ID"),
        rpad(col("_c107"),2," ").alias("PDX_QLFR"),
        col("_c108").alias("PDX_NTNL_PROV_ID"),
        col("_c109").alias("HLTH_RMBRMT_ARGMT_APLD_AMT"),
        col("_c110").alias("THER_CLS"),
        col("_c111").alias("HIC_NO"),
        rpad(col("_c112"),1," ").alias("HLTH_RMBRMT_ARGMT_FLAG"),
        col("_c113").alias("DOSE_CD"),
        rpad(col("_c114"),1," ").alias("LOW_INCM"),
        rpad(col("_c115"),2," ").alias("RTE_OF_ADMIN"),
        col("_c116").alias("DEA_SCHD"),
        col("_c117").alias("COPAY_BNF_OPT"),
        col("_c118").alias("GNRC_PROD_IN"),
        col("_c119").alias("PRSCRBR_SPEC"),
        col("_c120").alias("VAL_CD"),
        col("_c121").alias("PRI_CARE_PDX"),
        rpad(col("_c122"),1," ").alias("OFC_OF_INSPECTOR_GNRL"),
        col("_c123").alias("PATN_SSN"),
        col("_c124").alias("CARDHLDR_SSN"),
        col("_c125").alias("CARDHLDR_BRTH_DT"),
        col("_c126").alias("CARDHLDR_ADDR"),
        col("_c127").alias("CARDHLDR_CITY"),
        col("_c128").alias("CHADHLDR_ST"),
        col("_c129").alias("CARDHLDR_ZIP_CD"),
        col("_c130").alias("PSL_FMLY_MET_AMT"),
        col("_c131").alias("PSL_MBR_MET_AMT"),
        col("_c132").alias("PSL_FMLY_AMT"),
        col("_c133").alias("DEDCT_FMLY_MET_AMT"),
        col("_c134").alias("DEDCT_FMLY_AMT"),
        col("_c135").alias("MOPS_FMLY_AMT"),
        col("_c136").alias("MOPS_FMLY_MET_AMT"),
        col("_c137").alias("MOPS_MBR_MET_AMT"),
        col("_c138").alias("DEDCT_MBR_MET_AMT"),
        col("_c139").alias("PSL_APLD_AMT"),
        col("_c140").alias("MOPS_APLD_AMT"),
        rpad(col("_c141"),1," ").alias("PAR_PDX_IN"),
        col("_c142").alias("COPAY_PCT_AMT"),
        col("_c143").alias("COPAY_FLAT_AMT"),
        rpad(col("_c144"),1," ").alias("CLM_TRNSMSN_METH"),
        col("_c145").alias("RX_NO_2012"),
        col("_c146").alias("CLM_ID"),
        col("_c147").alias("CLM_STTUS_CD"),
        col("_c148").alias("ADJ_FROM_CLM_ID"),
        col("_c149").alias("ADJ_TO_CLM_ID"),
        col("_c150").alias("SUBMT_PROD_ID_QLFR"),
        col("_c151").alias("CNTNGNT_THER_FLAG"),
        col("_c152").alias("CNTNGNT_THER_SCHD"),
        col("_c153").alias("CLNT_PATN_PAY_ATRBD_PROD_AMT"),
        col("_c154").alias("CLNT_PATN_PAY_ATRBD_SLS_TAX_AMT"),
        col("_c155").alias("CLNT_PATN_PAY_ATRBD_PRCSR_FEE_AMT"),
        col("_c156").alias("CLNT_PATN_PAY_ATRBD_NTWK_AMT"),
        col("_c157").alias("LOB_IN")
    )
)

df_Trans2_Regular = (
    df_BCBSCommClmLand
    .filter(
        ~(
            substring(col("CLM_TYP"),1,1).isin("X","R")
        )
    )
    .select(
        lit(SrcSysCdSK).alias("SRC_SYS_CD_SK"),
        col("CLM_ID").alias("CLM_ID")
    )
)

df_Trans2_Adjustments = (
    df_BCBSCommClmLand
    .filter(
        substring(col("CLM_TYP"),1,1).isin("X","R")
    )
    .select(
        lit(SrcSysCdSK).alias("SRC_SYS_CD_SK"),
        substring(trim(col("CLM_ID")), 1, length(trim(col("CLM_ID")))-1).alias("CLM_ID")
    )
)

df_Collector = df_Trans2_Regular.unionByName(df_Trans2_Adjustments)

df_Collector_dedup = dedup_sort(
    df_Collector,
    ["SRC_SYS_CD_SK","CLM_ID"],
    []
)

params_clmloadpk = {
    "SrcSysCd": SRC_SYS_CD,
    "SrcSysCdSK": SrcSysCdSK,
    "CurrRunCycle": CurrRunCycle,
    "IDSOwner": IDSOwner,
    "ids_secret_name" :ids_secret_name
}

df_clmloadpk_output = run_ClmLoadPK(df_Collector_dedup, params_clmloadpk)

final_df = df_clmloadpk_output.select(
    "SRC_SYS_CD",
    "CLM_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "CLM_SK"
)

write_files(
    df_hf_clm_ln_pk_lkup,
    f"{adls_path}/hf_clm_ln_pk_lkup.parquet",
    delimiter=',',
    mode='overwrite',
    is_pqruet=True,
    header=True,
    quote='"',
    nullValue=None
)