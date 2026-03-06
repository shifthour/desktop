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
# MAGIC DESCRIPTION:      Medtrak Drug Claim Landing Extract. Looks up against the MBR_ENR table to get the right member unique key for a Member for a Claim
# MAGIC                                
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                        Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                --------------------------------       -------------------------------   ----------------------------      
# MAGIC Kalyan Neelam        2011-01-02        4616                      Original Programming                                                     IntegrateNewDevl           Steph Goddard          01/05/2011
# MAGIC Kalyan Neelam        2011-12-12        TTR 1216              Added one new field ERR_RSN_STRING                   IntegrateWrhsDevl           Sandrew                    2012-01-03
# MAGIC                                                                                         on end to the error file.
# MAGIC Raja Gummadi        2012-07-23         TTR 1330            Changed RX_NO field size from 9 to 20 in input file       IntegrateWrhsDevl           Bhoomi Dasari           08/08/2012
# MAGIC 
# MAGIC Manasa Andru        2017-05-12         TFS - 18089        Relaxing the member match rules by making changes    IntegrateDev2                 Jag Yelavarthi             2017-05-23
# MAGIC                                                                                      in the extract SQL in the IDS_MBR stage so that we can 
# MAGIC                                                                                     upate Claims table for members even when the member has the  
# MAGIC                                                                               Eligibility indicator of 'N' along with writing the record to the error table.
# MAGIC 
# MAGIC Manasa Andru       2017-06-19       TFS - 19397      Added Group Number field to the error file so that the group          IntegrateDev1     Kalyan Neelam          2017-06-20
# MAGIC                                                                            information for the unmatched member on the file could be available.                 
# MAGIC 
# MAGIC Jaideep Mankala     2018-02-26          5828               Removed constraint from Transformer to process member not       IntegrateDev2
# MAGIC \(9)\(9)\(9)\(9)\(9)found records 
# MAGIC 
# MAGIC Kaushik Kapoor       2018-04-13           5828         adding missing logic to add missing group from P_PBM_GRP_XREF    IntegrateDev2 Kalyan Neelam        2018-04-13

# MAGIC Medtrak Drug Claim Landing Extract
# MAGIC The Enr unmatched records are appended to the Mbr unmatched records file from MedtrakDrgClmPreProcExtr.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, DecimalType
)
from pyspark.sql.functions import (
    col, lit, concat, upper, when, length, rpad, coalesce, lpad, expr, asc, desc, trim as pyspark_trim
)
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
RunID = get_widget_value('RunID','')
RunDate = get_widget_value('RunDate','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

###############################################################################
# Stage: P_PBM_GRP_XREF (DB2Connector, reading from IDS)
###############################################################################
extract_query_P_PBM_GRP_XREF = f"""
SELECT DISTINCT
PBM_GRP_ID,
GRP_ID
FROM {IDSOwner}.P_PBM_GRP_XREF XREF
WHERE UPPER(XREF.SRC_SYS_CD) = 'MEDTRAK'
AND '{RunDate}' BETWEEN DATE(XREF.EFF_DT) AND DATE(XREF.TERM_DT)
"""
df_P_PBM_GRP_XREF = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_P_PBM_GRP_XREF)
    .load()
)

# hf_medtrak_drgclm_land_grpxref -> Scenario A => deduplicate on key columns (PBM_GRP_ID)
df_hf_medtrak_drgclm_land_grpxref = dedup_sort(
    df_P_PBM_GRP_XREF,
    partition_cols=["PBM_GRP_ID"],
    sort_cols=[]
)

###############################################################################
# Stage: MedtrakClmPreProc (CSeqFileStage) reading: verified/MedtrakDrugClm_PreProc.dat.#RunID#
###############################################################################
schema_MedtrakClmPreProc = StructType([
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
    StructField("FLR4", StringType(), False),
    StructField("CARDHLDR_ID_NO_1", StringType(), True),
    StructField("PATN_SSN", StringType(), True),
    StructField("CARDHLDR_SSN", StringType(), True),
    StructField("CARDHLDR_DOB", StringType(), True)
])

df_MedtrakClmPreProc = (
    spark.read
    .option("header", "false")
    .option("sep", ",")
    .option("quote", "\"")
    .schema(schema_MedtrakClmPreProc)
    .csv(f"{adls_path}/verified/MedtrakDrugClm_PreProc.dat.{RunID}")
)

###############################################################################
# Stage: Transformer_292
###############################################################################
df_Transformer_292 = df_MedtrakClmPreProc.select(
    col("RCRD_ID").alias("RCRD_ID"),
    col("CLAIM_ID").alias("CLAIM_ID"),
    col("PRCSR_NO").alias("PRCSR_NO"),
    col("MEM_CK_KEY").alias("MEM_CK_KEY"),
    col("BTCH_NO").alias("BTCH_NO"),
    col("PDX_NO").alias("PDX_NO"),
    col("RX_NO").alias("RX_NO"),
    col("DT_FILLED").alias("DT_FILLED"),
    col("NDC_NO").alias("NDC_NO"),
    col("DRUG_DESC").alias("DRUG_DESC"),
    col("NEW_RFL_CD").alias("NEW_RFL_CD"),
    col("METRIC_QTY").alias("METRIC_QTY"),
    col("DAYS_SUPL").alias("DAYS_SUPL"),
    col("BSS_OF_CST_DTRM").alias("BSS_OF_CST_DTRM"),
    col("INGR_CST").alias("INGR_CST"),
    col("DISPNS_FEE").alias("DISPNS_FEE"),
    col("COPAY_AMT").alias("COPAY_AMT"),
    col("SLS_TAX").alias("SLS_TAX"),
    col("AMT_BILL").alias("AMT_BILL"),
    col("PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    col("PATN_LAST_NM").alias("PATN_LAST_NM"),
    col("DOB").alias("DOB"),
    col("SEX_CD").alias("SEX_CD"),
    col("CARDHLDR_ID_NO").alias("CARDHLDR_ID_NO"),
    col("RELSHP_CD").alias("RELSHP_CD"),
    col("GRP_NO").alias("GRP_NO"),
    col("HOME_PLN").alias("HOME_PLN"),
    col("HOST_PLN").alias("HOST_PLN"),
    col("PRESCRIBER_ID").alias("PRESCRIBER_ID"),
    col("DIAG_CD").alias("DIAG_CD"),
    col("CARDHLDR_FIRST_NM").alias("CARDHLDR_FIRST_NM"),
    col("CARDHLDR_LAST_NM").alias("CARDHLDR_LAST_NM"),
    col("PRAUTH_NO").alias("PRAUTH_NO"),
    col("PA_MC_SC_NO").alias("PA_MC_SC_NO"),
    col("CUST_LOC").alias("CUST_LOC"),
    col("RESUB_CYC_CT").alias("RESUB_CYC_CT"),
    col("DT_RX_WRTN").alias("DT_RX_WRTN"),
    col("DISPENSE_AS_WRTN_PROD_SEL_CD").alias("DISPENSE_AS_WRTN_PROD_SEL_CD"),
    col("PRSN_CD").alias("PRSN_CD"),
    col("OTHR_COV_CD").alias("OTHR_COV_CD"),
    col("ELIG_CLRFCTN_CD").alias("ELIG_CLRFCTN_CD"),
    col("CMPND_CD").alias("CMPND_CD"),
    col("NO_OF_RFLS_AUTH").alias("NO_OF_RFLS_AUTH"),
    col("LVL_OF_SVC").alias("LVL_OF_SVC"),
    col("RX_ORIG_CD").alias("RX_ORIG_CD"),
    col("RX_DENIAL_CLRFCTN").alias("RX_DENIAL_CLRFCTN"),
    col("PRI_PRESCRIBER").alias("PRI_PRESCRIBER"),
    col("CLNC_ID_NO").alias("CLNC_ID_NO"),
    col("DRUG_TYP").alias("DRUG_TYP"),
    col("PRESCRIBER_LAST_NM").alias("PRESCRIBER_LAST_NM"),
    col("POSTAGE_AMT_CLMED").alias("POSTAGE_AMT_CLMED"),
    col("UNIT_DOSE_IN").alias("UNIT_DOSE_IN"),
    col("OTHR_PAYOR_AMT").alias("OTHR_PAYOR_AMT"),
    col("BSS_OF_DAYS_SUPL_DTRM").alias("BSS_OF_DAYS_SUPL_DTRM"),
    col("FULL_AWP").alias("FULL_AWP"),
    col("EXPNSN_AREA").alias("EXPNSN_AREA"),
    col("MSTR_CAR").alias("MSTR_CAR"),
    col("SUB_CAR").alias("SUB_CAR"),
    col("CLM_TYP").alias("CLM_TYP"),
    col("ESI_SUB_GRP").alias("ESI_SUB_GRP"),
    col("PLN_DSGNR").alias("PLN_DSGNR"),
    col("ADJDCT_DT").alias("ADJDCT_DT"),
    col("ADMIN_FEE").alias("ADMIN_FEE"),
    col("CAP_AMT").alias("CAP_AMT"),
    col("INGR_CST_SUB").alias("INGR_CST_SUB"),
    col("MBR_NON_COPAY_AMT").alias("MBR_NON_COPAY_AMT"),
    col("MBR_PAY_CD").alias("MBR_PAY_CD"),
    col("INCNTV_FEE").alias("INCNTV_FEE"),
    col("CLM_ADJ_AMT").alias("CLM_ADJ_AMT"),
    col("CLM_ADJ_CD").alias("CLM_ADJ_CD"),
    col("FRMLRY_FLAG").alias("FRMLRY_FLAG"),
    col("GNRC_CLS_NO").alias("GNRC_CLS_NO"),
    col("THRPTC_CLS_AHFS").alias("THRPTC_CLS_AHFS"),
    col("PDX_TYP").alias("PDX_TYP"),
    col("BILL_BSS_CD").alias("BILL_BSS_CD"),
    col("USL_AND_CUST_CHRG").alias("USL_AND_CUST_CHRG"),
    col("PD_DT").alias("PD_DT"),
    col("BNF_CD").alias("BNF_CD"),
    col("DRUG_STRG").alias("DRUG_STRG"),
    col("ORIG_MBR").alias("ORIG_MBR"),
    col("DT_OF_INJURY").alias("DT_OF_INJURY"),
    col("FEE_AMT").alias("FEE_AMT"),
    col("ESI_REF_NO").alias("ESI_REF_NO"),
    col("CLNT_CUST_ID").alias("CLNT_CUST_ID"),
    col("PLN_TYP").alias("PLN_TYP"),
    col("ESI_ADJDCT_REF_NO").alias("ESI_ADJDCT_REF_NO"),
    col("ESI_ANCLRY_AMT").alias("ESI_ANCLRY_AMT"),
    col("ESI_CLNT_GNRL_PRPS_AREA").alias("ESI_CLNT_GNRL_PRPS_AREA"),
    col("GRP_ID").alias("GRP_ID"),
    col("SUBGRP_ID").alias("SUBGRP_ID"),
    col("CLS_PLN_ID").alias("CLS_PLN_ID"),
    col("PAID_DATE").alias("PAID_DATE"),
    col("PRTL_FILL_STTUS_CD").alias("PRTL_FILL_STTUS_CD"),
    col("ESI_BILL_DT").alias("ESI_BILL_DT"),
    col("FSA_VNDR_CD").alias("FSA_VNDR_CD"),
    col("PICA_DRUG_CD").alias("PICA_DRUG_CD"),
    col("AMT_CLMED").alias("AMT_CLMED"),
    col("AMT_DSALW").alias("AMT_DSALW"),
    col("FED_DRUG_CLS_CD").alias("FED_DRUG_CLS_CD"),
    col("DEDCT_AMT").alias("DEDCT_AMT"),
    col("BNF_COPAY_100").alias("BNF_COPAY_100"),
    col("CLM_PRCS_TYP").alias("CLM_PRCS_TYP"),
    col("INDEM_HIER_TIER_NO").alias("INDEM_HIER_TIER_NO"),
    col("FLR").alias("FLR"),
    col("MCARE_D_COV_DRUG").alias("MCARE_D_COV_DRUG"),
    col("RETRO_LICS_CD").alias("RETRO_LICS_CD"),
    col("RETRO_LICS_AMT").alias("RETRO_LICS_AMT"),
    col("LICS_SBSDY_AMT").alias("LICS_SBSDY_AMT"),
    col("MED_B_DRUG").alias("MED_B_DRUG"),
    col("MED_B_CLM").alias("MED_B_CLM"),
    col("PRESCRIBER_QLFR").alias("PRESCRIBER_QLFR"),
    col("PRESCRIBER_ID_NPI").alias("PRESCRIBER_ID_NPI"),
    col("PDX_QLFR").alias("PDX_QLFR"),
    col("PDX_ID_NPI").alias("PDX_ID_NPI"),
    col("HRA_APLD_AMT").alias("HRA_APLD_AMT"),
    col("ESI_THER_CLS").alias("ESI_THER_CLS"),
    col("HIC_NO").alias("HIC_NO"),
    col("HRA_FLAG").alias("HRA_FLAG"),
    col("DOSE_CD").alias("DOSE_CD"),
    col("LOW_INCM").alias("LOW_INCM"),
    col("RTE_OF_ADMIN").alias("RTE_OF_ADMIN"),
    col("DEA_SCHD").alias("DEA_SCHD"),
    col("COPAY_BNF_OPT").alias("COPAY_BNF_OPT"),
    col("GNRC_PROD_IN_GPI").alias("GNRC_PROD_IN_GPI"),
    col("PRESCRIBER_SPEC").alias("PRESCRIBER_SPEC"),
    col("VAL_CD").alias("VAL_CD"),
    col("PRI_CARE_PDX").alias("PRI_CARE_PDX"),
    col("OFC_OF_INSPECTOR_GNRL_OIG").alias("OFC_OF_INSPECTOR_GNRL_OIG"),
    col("FLR3").alias("FLR3"),
    col("PSL_FMLY_MET_AMT").alias("PSL_FMLY_MET_AMT"),
    col("PSL_MBR_MET_AMT").alias("PSL_MBR_MET_AMT"),
    col("PSL_FMLY_AMT").alias("PSL_FMLY_AMT"),
    col("DED_FMLY_MET_AMT").alias("DED_FMLY_MET_AMT"),
    col("DED_FMLY_AMT").alias("DED_FMLY_AMT"),
    col("MOPS_FMLY_AMT").alias("MOPS_FMLY_AMT"),
    col("MOPS_FMLY_MET_AMT").alias("MOPS_FMLY_MET_AMT"),
    col("MOPS_MBR_MET_AMT").alias("MOPS_MBR_MET_AMT"),
    col("DED_MBR_MET_AMT").alias("DED_MBR_MET_AMT"),
    col("PSL_APLD_AMT").alias("PSL_APLD_AMT"),
    col("MOPS_APLD_AMT").alias("MOPS_APLD_AMT"),
    col("PAR_PDX_IND").alias("PAR_PDX_IND"),
    col("COPAY_PCT_AMT").alias("COPAY_PCT_AMT"),
    col("COPAY_FLAT_AMT").alias("COPAY_FLAT_AMT"),
    col("CLM_TRANSMITTAL_METH").alias("CLM_TRANSMITTAL_METH"),
    col("FLR4").alias("FLR4"),
    col("CARDHLDR_ID_NO_1").alias("CARDHLDR_ID_NO_1"),
    col("PATN_SSN").alias("PATN_SSN"),
    col("CARDHLDR_SSN").alias("CARDHLDR_SSN"),
    col("CARDHLDR_DOB").alias("CARDHLDR_DOB")
)

# hf_medtrak_drgclm_land_filededupe -> Scenario A => deduplicate on (CLM_ID, MBR_UNIQ_KEY, FILLED_DT)
df_hf_medtrak_drgclm_land_filededupe_pre = df_Transformer_292.select(
    col("CLAIM_ID").alias("CLM_ID"),
    col("MEM_CK_KEY").alias("MBR_UNIQ_KEY"),
    col("DT_FILLED").alias("FILLED_DT"),
    *[col(x) for x in df_Transformer_292.columns]
)
df_hf_medtrak_drgclm_land_filededupe = dedup_sort(
    df_hf_medtrak_drgclm_land_filededupe_pre,
    partition_cols=["CLM_ID","MBR_UNIQ_KEY","FILLED_DT"],
    sort_cols=[]
)

###############################################################################
# Stage: IDS_MBR (DB2Connector, reading from IDS)
###############################################################################
extract_query_IDS_MBR = f"""
SELECT 
       CLM_ID,
       MBR_UNIQ_KEY,
       "Order"
FROM 
(
 (
   SELECT 
      DRUG.CLM_ID,
      ENR.MBR_UNIQ_KEY,
      MAX(ENR.TERM_DT_SK) TERM_DT_SK,
      MAX(ENR.EFF_DT_SK) EFF_DT_SK,
      3 as "Order"
   FROM
     {IDSOwner}.MBR_ENR ENR,
     {IDSOwner}.CD_MPPNG CD1,
     {IDSOwner}.W_DRUG_ENR_MATCH DRUG
   WHERE
     ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = CD1.CD_MPPNG_SK AND
     CD1.TRGT_CD IN ('MED', 'MED1') AND
     ENR.ELIG_IN = 'Y' AND
     ENR.MBR_UNIQ_KEY = DRUG.MBR_UNIQ_KEY AND
     ENR.EFF_DT_SK <= DRUG.FILL_DT_SK AND
     ENR.TERM_DT_SK >= DRUG.FILL_DT_SK
   GROUP BY
     DRUG.CLM_ID,
     ENR.MBR_UNIQ_KEY
 )
 UNION
 (
   SELECT 
      DRUG.CLM_ID,
      ENR.MBR_UNIQ_KEY,
      MAX(ENR.TERM_DT_SK) TERM_DT_SK,
      MAX(ENR.EFF_DT_SK) EFF_DT_SK,
      2 as "Order"
   FROM
     {IDSOwner}.MBR_ENR ENR,
     {IDSOwner}.CD_MPPNG CD1,
     {IDSOwner}.W_DRUG_ENR_MATCH DRUG
   WHERE
     ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = CD1.CD_MPPNG_SK AND
     CD1.TRGT_CD IN ('MED', 'MED1') AND
     ENR.ELIG_IN = 'Y' AND
     ENR.MBR_UNIQ_KEY = DRUG.MBR_UNIQ_KEY
   GROUP BY
     DRUG.CLM_ID,
     ENR.MBR_UNIQ_KEY
 )
 UNION
 (
   SELECT 
      DRUG.CLM_ID,
      ENR.MBR_UNIQ_KEY,
      MAX(ENR.TERM_DT_SK) TERM_DT_SK,
      MAX(ENR.EFF_DT_SK) EFF_DT_SK,
      1 as "Order"
   FROM
     {IDSOwner}.MBR_ENR ENR,
     {IDSOwner}.CD_MPPNG CD1,
     {IDSOwner}.W_DRUG_ENR_MATCH DRUG
   WHERE
     ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = CD1.CD_MPPNG_SK AND
     CD1.TRGT_CD IN ('MED', 'MED1') AND
     ENR.ELIG_IN = 'N' AND
     ENR.MBR_UNIQ_KEY = DRUG.MBR_UNIQ_KEY
   GROUP BY
     DRUG.CLM_ID,
     ENR.MBR_UNIQ_KEY
 )
)
ORDER BY
CLM_ID,
"Order",
TERM_DT_SK,
EFF_DT_SK,
MBR_UNIQ_KEY
"""
df_IDS_MBR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_IDS_MBR)
    .load()
)

# hf_medtrak_drgclm_land_clmid_dedupe -> Scenario A => deduplicate on [CLM_ID]
df_hf_medtrak_drgclm_land_clmid_dedupe = dedup_sort(
    df_IDS_MBR,
    partition_cols=["CLM_ID"],
    sort_cols=[]
)

###############################################################################
# Stage: ReKey (CTransformerStage)
###############################################################################
df_ReKey_Clm_Deduped = df_hf_medtrak_drgclm_land_clmid_dedupe

df_ReKey_DSLink282 = df_ReKey_Clm_Deduped.select(
    col("CLM_ID").alias("CLM_ID"),
    col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY")
)

df_ReKey_ClmIdsForErr = df_ReKey_Clm_Deduped.select(
    col("CLM_ID").alias("CLM_ID")
)

###############################################################################
# Stage: hf_medtrak_drgclm_land_clmid_lkup
# Scenario A => deduplicate on [CLM_ID, MBR_UNIQ_KEY]
###############################################################################
df_hf_medtrak_drgclm_land_clmid_lkup_pre = df_ReKey_DSLink282
df_hf_medtrak_drgclm_land_clmid_lkup = dedup_sort(
    df_hf_medtrak_drgclm_land_clmid_lkup_pre,
    partition_cols=["CLM_ID","MBR_UNIQ_KEY"],
    sort_cols=[]
)

###############################################################################
# Stage: hf_medtrak_drgclm_land_clmidlkup_err
# Scenario A => deduplicate on [CLM_ID]
###############################################################################
df_hf_medtrak_drgclm_land_clmidlkup_err_pre = df_ReKey_ClmIdsForErr
df_hf_medtrak_drgclm_land_clmidlkup_err = dedup_sort(
    df_hf_medtrak_drgclm_land_clmidlkup_err_pre,
    partition_cols=["CLM_ID"],
    sort_cols=[]
)

###############################################################################
# Join all needed in MemberLookUp (CTransformerStage)
###############################################################################
df_Medtrak_primary = df_hf_medtrak_drgclm_land_filededupe.alias("Medtrak")
df_clm_mbr_lkup = df_hf_medtrak_drgclm_land_clmid_lkup.alias("clm_mbr_lkup")
df_ErrProc_lkup = df_hf_medtrak_drgclm_land_clmidlkup_err.alias("ErrProc_lkup")
df_Grp_Xref = df_hf_medtrak_drgclm_land_grpxref.alias("Grp_Xref")

# First join: clm_mbr_lkup on (Medtrak.CLAIM_ID = clm_mbr_lkup.CLM_ID) & (Medtrak.MEM_CK_KEY = clm_mbr_lkup.MBR_UNIQ_KEY) 
df_join_clm_mbr = df_Medtrak_primary.join(
    df_clm_mbr_lkup,
    (col("Medtrak.CLM_ID") == col("clm_mbr_lkup.CLM_ID")) &
    (col("Medtrak.MBR_UNIQ_KEY") == col("clm_mbr_lkup.MBR_UNIQ_KEY")),
    "left"
)

# Second join: ErrProc_lkup on (Medtrak.CLM_ID = ErrProc_lkup.CLM_ID) 
df_join_err = df_join_clm_mbr.join(
    df_ErrProc_lkup,
    (col("Medtrak.CLM_ID") == col("ErrProc_lkup.CLM_ID")),
    "left"
)

# Third join: Grp_Xref on ( trim(upper(Medtrak.MSTR_CAR)) + trim(upper(Medtrak.SUB_CAR)) = Grp_Xref.PBM_GRP_ID )
df_join_grp = df_join_err.withColumn(
    "_join_key_grp",
    concat(
        upper(pyspark_trim(col("Medtrak.MSTR_CAR"))),
        upper(pyspark_trim(col("Medtrak.SUB_CAR")))
    )
).join(
    df_Grp_Xref,
    col("_join_key_grp") == col("Grp_Xref.PBM_GRP_ID"),
    "left"
)

df_MemberLookUp_all = df_join_grp.withColumn(
    "svXrefGrp",
    when(
        (col("Grp_Xref.GRP_ID").isNull()) | (pyspark_trim(col("Grp_Xref.GRP_ID")) == ""),
        lit("UNK")
    ).otherwise(pyspark_trim(col("Grp_Xref.GRP_ID")))
)

# Output pins from MemberLookUp:

# 1) Sort (no filter, just columns)
df_Sort = df_MemberLookUp_all.select(
    col("Medtrak.RCRD_ID").alias("RCRD_ID"),
    col("Medtrak.CLAIM_ID").alias("CLAIM_ID"),
    col("Medtrak.PRCSR_NO").alias("PRCSR_NO"),
    col("Medtrak.MEM_CK_KEY").alias("MEM_CK_KEY"),
    col("Medtrak.BTCH_NO").alias("BTCH_NO"),
    col("Medtrak.PDX_NO").alias("PDX_NO"),
    col("Medtrak.RX_NO").alias("RX_NO"),
    col("Medtrak.DT_FILLED").alias("DT_FILLED"),
    col("Medtrak.NDC_NO").alias("NDC_NO"),
    col("Medtrak.DRUG_DESC").alias("DRUG_DESC"),
    col("Medtrak.NEW_RFL_CD").alias("NEW_RFL_CD"),
    col("Medtrak.METRIC_QTY").alias("METRIC_QTY"),
    col("Medtrak.DAYS_SUPL").alias("DAYS_SUPL"),
    col("Medtrak.BSS_OF_CST_DTRM").alias("BSS_OF_CST_DTRM"),
    col("Medtrak.INGR_CST").alias("INGR_CST"),
    col("Medtrak.DISPNS_FEE").alias("DISPNS_FEE"),
    col("Medtrak.COPAY_AMT").alias("COPAY_AMT"),
    col("Medtrak.SLS_TAX").alias("SLS_TAX"),
    col("Medtrak.AMT_BILL").alias("AMT_BILL"),
    col("Medtrak.PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    col("Medtrak.PATN_LAST_NM").alias("PATN_LAST_NM"),
    col("Medtrak.DOB").alias("DOB"),
    col("Medtrak.SEX_CD").alias("SEX_CD"),
    col("Medtrak.CARDHLDR_ID_NO").alias("CARDHLDR_ID_NO"),
    col("Medtrak.RELSHP_CD").alias("RELSHP_CD"),
    col("Medtrak.GRP_NO").alias("GRP_NO"),
    col("Medtrak.HOME_PLN").alias("HOME_PLN"),
    col("Medtrak.HOST_PLN").alias("HOST_PLN"),
    col("Medtrak.PRESCRIBER_ID").alias("PRESCRIBER_ID"),
    col("Medtrak.DIAG_CD").alias("DIAG_CD"),
    col("Medtrak.CARDHLDR_FIRST_NM").alias("CARDHLDR_FIRST_NM"),
    col("Medtrak.CARDHLDR_LAST_NM").alias("CARDHLDR_LAST_NM"),
    col("Medtrak.PRAUTH_NO").alias("PRAUTH_NO"),
    col("Medtrak.PA_MC_SC_NO").alias("PA_MC_SC_NO"),
    col("Medtrak.CUST_LOC").alias("CUST_LOC"),
    col("Medtrak.RESUB_CYC_CT").alias("RESUB_CYC_CT"),
    col("Medtrak.DT_RX_WRTN").alias("DT_RX_WRTN"),
    col("Medtrak.DISPENSE_AS_WRTN_PROD_SEL_CD").alias("DISPENSE_AS_WRTN_PROD_SEL_CD"),
    col("Medtrak.PRSN_CD").alias("PRSN_CD"),
    col("Medtrak.OTHR_COV_CD").alias("OTHR_COV_CD"),
    col("Medtrak.ELIG_CLRFCTN_CD").alias("ELIG_CLRFCTN_CD"),
    col("Medtrak.CMPND_CD").alias("CMPND_CD"),
    col("Medtrak.NO_OF_RFLS_AUTH").alias("NO_OF_RFLS_AUTH"),
    col("Medtrak.LVL_OF_SVC").alias("LVL_OF_SVC"),
    col("Medtrak.RX_ORIG_CD").alias("RX_ORIG_CD"),
    col("Medtrak.RX_DENIAL_CLRFCTN").alias("RX_DENIAL_CLRFCTN"),
    col("Medtrak.PRI_PRESCRIBER").alias("PRI_PRESCRIBER"),
    col("Medtrak.CLNC_ID_NO").alias("CLNC_ID_NO"),
    col("Medtrak.DRUG_TYP").alias("DRUG_TYP"),
    col("Medtrak.PRESCRIBER_LAST_NM").alias("PRESCRIBER_LAST_NM"),
    col("Medtrak.POSTAGE_AMT_CLMED").alias("POSTAGE_AMT_CLMED"),
    col("Medtrak.UNIT_DOSE_IN").alias("UNIT_DOSE_IN"),
    col("Medtrak.OTHR_PAYOR_AMT").alias("OTHR_PAYOR_AMT"),
    col("Medtrak.BSS_OF_DAYS_SUPL_DTRM").alias("BSS_OF_DAYS_SUPL_DTRM"),
    col("Medtrak.FULL_AWP").alias("FULL_AWP"),
    col("Medtrak.EXPNSN_AREA").alias("EXPNSN_AREA"),
    col("Medtrak.MSTR_CAR").alias("MSTR_CAR"),
    col("Medtrak.SUB_CAR").alias("SUB_CAR"),
    col("Medtrak.CLM_TYP").alias("CLM_TYP"),
    col("Medtrak.ESI_SUB_GRP").alias("ESI_SUB_GRP"),
    col("Medtrak.PLN_DSGNR").alias("PLN_DSGNR"),
    col("Medtrak.ADJDCT_DT").alias("ADJDCT_DT"),
    col("Medtrak.ADMIN_FEE").alias("ADMIN_FEE"),
    col("Medtrak.CAP_AMT").alias("CAP_AMT"),
    col("Medtrak.INGR_CST_SUB").alias("INGR_CST_SUB"),
    col("Medtrak.MBR_NON_COPAY_AMT").alias("MBR_NON_COPAY_AMT"),
    col("Medtrak.MBR_PAY_CD").alias("MBR_PAY_CD"),
    col("Medtrak.INCNTV_FEE").alias("INCNTV_FEE"),
    col("Medtrak.CLM_ADJ_AMT").alias("CLM_ADJ_AMT"),
    col("Medtrak.CLM_ADJ_CD").alias("CLM_ADJ_CD"),
    col("Medtrak.FRMLRY_FLAG").alias("FRMLRY_FLAG"),
    col("Medtrak.GNRC_CLS_NO").alias("GNRC_CLS_NO"),
    col("Medtrak.THRPTC_CLS_AHFS").alias("THRPTC_CLS_AHFS"),
    col("Medtrak.PDX_TYP").alias("PDX_TYP"),
    col("Medtrak.BILL_BSS_CD").alias("BILL_BSS_CD"),
    col("Medtrak.USL_AND_CUST_CHRG").alias("USL_AND_CUST_CHRG"),
    col("Medtrak.PD_DT").alias("PD_DT"),
    col("Medtrak.BNF_CD").alias("BNF_CD"),
    col("Medtrak.DRUG_STRG").alias("DRUG_STRG"),
    col("Medtrak.ORIG_MBR").alias("ORIG_MBR"),
    col("Medtrak.DT_OF_INJURY").alias("DT_OF_INJURY"),
    col("Medtrak.FEE_AMT").alias("FEE_AMT"),
    col("Medtrak.ESI_REF_NO").alias("ESI_REF_NO"),
    col("Medtrak.CLNT_CUST_ID").alias("CLNT_CUST_ID"),
    col("Medtrak.PLN_TYP").alias("PLN_TYP"),
    col("Medtrak.ESI_ADJDCT_REF_NO").alias("ESI_ADJDCT_REF_NO"),
    col("Medtrak.ESI_ANCLRY_AMT").alias("ESI_ANCLRY_AMT"),
    col("Medtrak.ESI_CLNT_GNRL_PRPS_AREA").alias("ESI_CLNT_GNRL_PRPS_AREA"),
    col("svXrefGrp").alias("GRP_ID"),
    col("Medtrak.SUBGRP_ID").alias("SUBGRP_ID"),
    col("Medtrak.CLS_PLN_ID").alias("CLS_PLN_ID"),
    col("Medtrak.PAID_DATE").alias("PAID_DATE"),
    col("Medtrak.PRTL_FILL_STTUS_CD").alias("PRTL_FILL_STTUS_CD"),
    col("Medtrak.ESI_BILL_DT").alias("ESI_BILL_DT"),
    col("Medtrak.FSA_VNDR_CD").alias("FSA_VNDR_CD"),
    col("Medtrak.PICA_DRUG_CD").alias("PICA_DRUG_CD"),
    col("Medtrak.AMT_CLMED").alias("AMT_CLMED"),
    col("Medtrak.AMT_DSALW").alias("AMT_DSALW"),
    col("Medtrak.FED_DRUG_CLS_CD").alias("FED_DRUG_CLS_CD"),
    col("Medtrak.DEDCT_AMT").alias("DEDCT_AMT"),
    col("Medtrak.BNF_COPAY_100").alias("BNF_COPAY_100"),
    col("Medtrak.CLM_PRCS_TYP").alias("CLM_PRCS_TYP"),
    col("Medtrak.INDEM_HIER_TIER_NO").alias("INDEM_HIER_TIER_NO"),
    col("Medtrak.FLR").alias("FLR"),
    col("Medtrak.MCARE_D_COV_DRUG").alias("MCARE_D_COV_DRUG"),
    col("Medtrak.RETRO_LICS_CD").alias("RETRO_LICS_CD"),
    col("Medtrak.RETRO_LICS_AMT").alias("RETRO_LICS_AMT"),
    col("Medtrak.LICS_SBSDY_AMT").alias("LICS_SBSDY_AMT"),
    col("Medtrak.MED_B_DRUG").alias("MED_B_DRUG"),
    col("Medtrak.MED_B_CLM").alias("MED_B_CLM"),
    col("Medtrak.PRESCRIBER_QLFR").alias("PRESCRIBER_QLFR"),
    col("Medtrak.PRESCRIBER_ID_NPI").alias("PRESCRIBER_ID_NPI"),
    col("Medtrak.PDX_QLFR").alias("PDX_QLFR"),
    col("Medtrak.PDX_ID_NPI").alias("PDX_ID_NPI"),
    col("Medtrak.HRA_APLD_AMT").alias("HRA_APLD_AMT"),
    col("Medtrak.ESI_THER_CLS").alias("ESI_THER_CLS"),
    col("Medtrak.HIC_NO").alias("HIC_NO"),
    col("Medtrak.HRA_FLAG").alias("HRA_FLAG"),
    col("Medtrak.DOSE_CD").alias("DOSE_CD"),
    col("Medtrak.LOW_INCM").alias("LOW_INCM"),
    col("Medtrak.RTE_OF_ADMIN").alias("RTE_OF_ADMIN"),
    col("Medtrak.DEA_SCHD").alias("DEA_SCHD"),
    col("Medtrak.COPAY_BNF_OPT").alias("COPAY_BNF_OPT"),
    col("Medtrak.GNRC_PROD_IN_GPI").alias("GNRC_PROD_IN_GPI"),
    col("Medtrak.PRESCRIBER_SPEC").alias("PRESCRIBER_SPEC"),
    col("Medtrak.VAL_CD").alias("VAL_CD"),
    col("Medtrak.PRI_CARE_PDX").alias("PRI_CARE_PDX"),
    col("Medtrak.OFC_OF_INSPECTOR_GNRL_OIG").alias("OFC_OF_INSPECTOR_GNRL_OIG"),
    col("Medtrak.FLR3").alias("FLR3"),
    col("Medtrak.PSL_FMLY_MET_AMT").alias("PSL_FMLY_MET_AMT"),
    col("Medtrak.PSL_MBR_MET_AMT").alias("PSL_MBR_MET_AMT"),
    col("Medtrak.PSL_FMLY_AMT").alias("PSL_FMLY_AMT"),
    col("Medtrak.DED_FMLY_MET_AMT").alias("DED_FMLY_MET_AMT"),
    col("Medtrak.DED_FMLY_AMT").alias("DED_FMLY_AMT"),
    col("Medtrak.MOPS_FMLY_AMT").alias("MOPS_FMLY_AMT"),
    col("Medtrak.MOPS_FMLY_MET_AMT").alias("MOPS_FMLY_MET_AMT"),
    col("Medtrak.MOPS_MBR_MET_AMT").alias("MOPS_MBR_MET_AMT"),
    col("Medtrak.DED_MBR_MET_AMT").alias("DED_MBR_MET_AMT"),
    col("Medtrak.PSL_APLD_AMT").alias("PSL_APLD_AMT"),
    col("Medtrak.MOPS_APLD_AMT").alias("MOPS_APLD_AMT"),
    col("Medtrak.PAR_PDX_IND").alias("PAR_PDX_IND"),
    col("Medtrak.COPAY_PCT_AMT").alias("COPAY_PCT_AMT"),
    col("Medtrak.COPAY_FLAT_AMT").alias("COPAY_FLAT_AMT"),
    col("Medtrak.CLM_TRANSMITTAL_METH").alias("CLM_TRANSMITTAL_METH"),
    col("Medtrak.FLR4").alias("FLR4")
)

# 2) MbrFillDt
df_MbrFillDt = df_MemberLookUp_all.select(
    col("Medtrak.MEM_CK_KEY").alias("MBR_UNIQ_KEY"),
    col("Medtrak.DT_FILLED").alias("FILL_DT_SK")
)

# 3) Load
df_Load = df_MemberLookUp_all.select(
    col("Medtrak.CLAIM_ID").alias("CLM_ID"),
    col("Medtrak.DT_FILLED").alias("FILL_DT_SK"),
    col("Medtrak.MEM_CK_KEY").alias("MBR_UNIQ_KEY")
)

# 4) Reject (constraint IsNull(ErrProc_lkup.CLM_ID) = TRUE)
df_Reject = df_MemberLookUp_all.filter(col("ErrProc_lkup.CLM_ID").isNull()).select(
    col("Medtrak.CLM_ID").alias("CLM_ID"),
    col("Medtrak.GRP_NO").alias("GRP_NO"),
    col("Medtrak.ESI_REF_NO").alias("ESI_REF_NO"),
    col("Medtrak.RX_NO").alias("RX_NO"),
    when(col("Medtrak.FILLED_DT") == lit("1753-01-01"), lit("")).otherwise(col("Medtrak.FILLED_DT")).alias("DT_FILLED"),
    col("Medtrak.PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    col("Medtrak.PATN_LAST_NM").alias("PATN_LAST_NM"),
    when(col("Medtrak.DOB") == lit("1753-01-01"), lit("")).otherwise(col("Medtrak.DOB")).alias("DOB"),
    col("Medtrak.PATN_SSN").alias("PATN_SSN"),
    col("Medtrak.CARDHLDR_ID_NO_1").alias("CARDHLDR_ID_NO"),
    col("Medtrak.CARDHLDR_FIRST_NM").alias("CARDHLDR_FIRST_NM"),
    col("Medtrak.CARDHLDR_LAST_NM").alias("CARDHLDR_LAST_NM"),
    col("Medtrak.CARDHLDR_DOB").alias("CARDHLDR_DOB"),
    col("Medtrak.CARDHLDR_SSN").alias("CARDHLDR_SSN"),
    when(col("Medtrak.SEX_CD") == lit("F"), lit("2")).otherwise(
        when(col("Medtrak.SEX_CD") == lit("M"), lit("1")).otherwise(lit("0"))
    ).alias("SEX_CD"),
    col("Medtrak.RELSHP_CD").alias("RELSHP_CD"),
)

###############################################################################
# Stage: W_DRUG_ENR (CSeqFileStage) writing from df_Load
###############################################################################
# Final select with char rpad if needed:
df_W_DRUG_ENR_write = df_Load.select(
    col("CLM_ID"),  # no char
    rpad(col("FILL_DT_SK"),10," ").alias("FILL_DT_SK"),  # char(10)
    col("MBR_UNIQ_KEY")  # int
)
write_files(
    df_W_DRUG_ENR_write,
    f"{adls_path}/load/W_DRUG_ENR.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

###############################################################################
# Stage: hf_medtrak_clm_land_mbr_fill_dt (Scenario A => deduplicate on [MBR_UNIQ_KEY, FILL_DT_SK])
###############################################################################
df_hf_medtrak_clm_land_mbr_fill_dt_pre = df_MbrFillDt
df_hf_medtrak_clm_land_mbr_fill_dt = dedup_sort(
    df_hf_medtrak_clm_land_mbr_fill_dt_pre,
    partition_cols=["MBR_UNIQ_KEY","FILL_DT_SK"],
    sort_cols=[]
)

###############################################################################
# Stage: W_DRUG_CLM_PCP (CSeqFileStage) from df_hf_medtrak_clm_land_mbr_fill_dt
###############################################################################
df_W_DRUG_CLM_PCP_write = df_hf_medtrak_clm_land_mbr_fill_dt.select(
    col("MBR_UNIQ_KEY"),
    rpad(col("FILL_DT_SK"),10," ").alias("FILL_DT_SK")
)
write_files(
    df_W_DRUG_CLM_PCP_write,
    f"{adls_path}/load/W_DRUG_CLM_PCP.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

###############################################################################
# Stage: hf_medtrak_drgclm_land_reject (deduplicate on [CLM_ID])
###############################################################################
df_hf_medtrak_drgclm_land_reject_pre = df_Reject
df_hf_medtrak_drgclm_land_reject = dedup_sort(
    df_hf_medtrak_drgclm_land_reject_pre,
    partition_cols=["CLM_ID"],
    sort_cols=[]
)

###############################################################################
# Stage: DropField => from df_hf_medtrak_drgclm_land_reject => output LoadRejects
###############################################################################
df_DropField_LoadRejects = df_hf_medtrak_drgclm_land_reject.select(
    col("ESI_REF_NO").alias("ESI_REF_NO"),
    col("GRP_NO").alias("GRP_NO"),
    col("RX_NO").alias("RX_NO"),
    col("DT_FILLED").alias("DT_FILLED"),
    col("PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    col("PATN_LAST_NM").alias("PATN_LAST_NM"),
    col("DOB").alias("DOB"),
    col("PATN_SSN").alias("PATN_SSN"),
    col("CARDHLDR_ID_NO").alias("CARDHLDR_ID_NO"),
    col("CARDHLDR_FIRST_NM").alias("CARDHLDR_FIRST_NM"),
    col("CARDHLDR_LAST_NM").alias("CARDHLDR_LAST_NM"),
    col("CARDHLDR_DOB").alias("CARDHLDR_DOB"),
    col("CARDHLDR_SSN").alias("CARDHLDR_SSN"),
    col("SEX_CD").alias("SEX_CD"),
    col("RELSHP_CD").alias("RELSHP_CD"),
    lit("FAILED ELIGIBILITY").alias("ERR_RSN_STRING")
)

###############################################################################
# Stage: MbrEnrNotFound (CSeqFileStage), appending to external csv
###############################################################################
df_MbrEnrNotFound_write = df_DropField_LoadRejects.select(
    rpad(col("ESI_REF_NO"),14," ").alias("ESI_REF_NO"),
    rpad(col("GRP_NO"),18," ").alias("GRP_NO"),
    col("RX_NO"),
    col("DT_FILLED"),
    rpad(col("PATN_FIRST_NM"),12," ").alias("PATN_FIRST_NM"),
    rpad(col("PATN_LAST_NM"),15," ").alias("PATN_LAST_NM"),
    col("DOB"),
    rpad(col("PATN_SSN"),11," ").alias("PATN_SSN"),
    rpad(col("CARDHLDR_ID_NO"),18," ").alias("CARDHLDR_ID_NO"),
    rpad(col("CARDHLDR_FIRST_NM"),12," ").alias("CARDHLDR_FIRST_NM"),
    rpad(col("CARDHLDR_LAST_NM"),15," ").alias("CARDHLDR_LAST_NM"),
    rpad(col("CARDHLDR_DOB"),8," ").alias("CARDHLDR_DOB"),
    rpad(col("CARDHLDR_SSN"),11," ").alias("CARDHLDR_SSN"),
    col("SEX_CD"),
    col("RELSHP_CD"),
    rpad(col("ERR_RSN_STRING"),100," ").alias("ERR_RSN_STRING")
)
write_files(
    df_MbrEnrNotFound_write,
    f"{adls_path_publish}/external/MedtrakDrugClm_NoMbrMatchRecs.csv",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

###############################################################################
# Stage: Sort => from df_Sort => (sort order: CLAIM_ID asc, MEM_CK_KEY asc)
###############################################################################
df_Sort_sorted = df_Sort.orderBy(col("CLAIM_ID").asc(), col("MEM_CK_KEY").asc())

###############################################################################
# Stage: hf_medtrak_dedupe_clm_mbr_uniq_key => scenario A => deduplicate on [RCRD_ID, CLAIM_ID]
###############################################################################
df_hf_medtrak_dedupe_clm_mbr_uniq_key_pre = df_Sort_sorted
df_hf_medtrak_dedupe_clm_mbr_uniq_key = dedup_sort(
    df_hf_medtrak_dedupe_clm_mbr_uniq_key_pre,
    partition_cols=["RCRD_ID","CLAIM_ID"],
    sort_cols=[]
)

###############################################################################
# Stage: Medtrak_DrgClmLand (CSeqFileStage) => writes verified/MedtrakDrugClm_Land.dat.#RunID#
###############################################################################
df_Medtrak_DrgClmLand_write = df_hf_medtrak_dedupe_clm_mbr_uniq_key.select(
    rpad(col("RCRD_ID").cast(StringType()), 38, " ").alias("RCRD_ID"),
    rpad(col("CLAIM_ID"),         len("CLAIM_ID"), " ").alias("CLAIM_ID"),
    col("PRCSR_NO"),
    col("MEM_CK_KEY"),
    col("BTCH_NO"),
    rpad(col("PDX_NO"),15," ").alias("PDX_NO"),
    col("RX_NO"),
    rpad(col("DT_FILLED"),10," ").alias("DT_FILLED"),
    col("NDC_NO"),
    rpad(col("DRUG_DESC"),30," ").alias("DRUG_DESC"),
    col("NEW_RFL_CD"),
    col("METRIC_QTY"),
    col("DAYS_SUPL"),
    rpad(col("BSS_OF_CST_DTRM"),2," ").alias("BSS_OF_CST_DTRM"),
    col("INGR_CST"),
    col("DISPNS_FEE"),
    col("COPAY_AMT"),
    col("SLS_TAX"),
    col("AMT_BILL"),
    rpad(col("PATN_FIRST_NM"),12," ").alias("PATN_FIRST_NM"),
    rpad(col("PATN_LAST_NM"),15," ").alias("PATN_LAST_NM"),
    col("DOB"),
    col("SEX_CD"),
    rpad(col("CARDHLDR_ID_NO"),18," ").alias("CARDHLDR_ID_NO"),
    col("RELSHP_CD"),
    rpad(col("GRP_NO"),18," ").alias("GRP_NO"),
    rpad(col("HOME_PLN"),3," ").alias("HOME_PLN"),
    col("HOST_PLN"),
    rpad(col("PRESCRIBER_ID"),15," ").alias("PRESCRIBER_ID"),
    rpad(col("DIAG_CD"),6," ").alias("DIAG_CD"),
    rpad(col("CARDHLDR_FIRST_NM"),12," ").alias("CARDHLDR_FIRST_NM"),
    rpad(col("CARDHLDR_LAST_NM"),15," ").alias("CARDHLDR_LAST_NM"),
    col("PRAUTH_NO"),
    rpad(col("PA_MC_SC_NO"),7," ").alias("PA_MC_SC_NO"),
    col("CUST_LOC"),
    col("RESUB_CYC_CT"),
    col("DT_RX_WRTN"),
    rpad(col("DISPENSE_AS_WRTN_PROD_SEL_CD"),1," ").alias("DISPENSE_AS_WRTN_PROD_SEL_CD"),
    rpad(col("PRSN_CD"),2," ").alias("PRSN_CD"),
    col("OTHR_COV_CD"),
    col("ELIG_CLRFCTN_CD"),
    col("CMPND_CD"),
    col("NO_OF_RFLS_AUTH"),
    col("LVL_OF_SVC"),
    col("RX_ORIG_CD"),
    col("RX_DENIAL_CLRFCTN"),
    rpad(col("PRI_PRESCRIBER"),10," ").alias("PRI_PRESCRIBER"),
    col("CLNC_ID_NO"),
    col("DRUG_TYP"),
    rpad(col("PRESCRIBER_LAST_NM"),15," ").alias("PRESCRIBER_LAST_NM"),
    col("POSTAGE_AMT_CLMED"),
    col("UNIT_DOSE_IN"),
    col("OTHR_PAYOR_AMT"),
    col("BSS_OF_DAYS_SUPL_DTRM"),
    col("FULL_AWP"),
    rpad(col("EXPNSN_AREA"),1," ").alias("EXPNSN_AREA"),
    rpad(col("MSTR_CAR"),4," ").alias("MSTR_CAR"),
    rpad(col("SUB_CAR"),4," ").alias("SUB_CAR"),
    rpad(col("CLM_TYP"),1," ").alias("CLM_TYP"),
    rpad(col("ESI_SUB_GRP"),20," ").alias("ESI_SUB_GRP"),
    rpad(col("PLN_DSGNR"),1," ").alias("PLN_DSGNR"),
    rpad(col("ADJDCT_DT"),10," ").alias("ADJDCT_DT"),
    col("ADMIN_FEE"),
    col("CAP_AMT"),
    col("INGR_CST_SUB"),
    col("MBR_NON_COPAY_AMT"),
    rpad(col("MBR_PAY_CD"),2," ").alias("MBR_PAY_CD"),
    col("INCNTV_FEE"),
    col("CLM_ADJ_AMT"),
    rpad(col("CLM_ADJ_CD"),2," ").alias("CLM_ADJ_CD"),
    rpad(col("FRMLRY_FLAG"),1," ").alias("FRMLRY_FLAG"),
    rpad(col("GNRC_CLS_NO"),14," ").alias("GNRC_CLS_NO"),
    rpad(col("THRPTC_CLS_AHFS"),6," ").alias("THRPTC_CLS_AHFS"),
    rpad(col("PDX_TYP"),1," ").alias("PDX_TYP"),
    rpad(col("BILL_BSS_CD"),2," ").alias("BILL_BSS_CD"),
    col("USL_AND_CUST_CHRG"),
    rpad(col("PD_DT"),10," ").alias("PD_DT"),
    rpad(col("BNF_CD"),10," ").alias("BNF_CD"),
    rpad(col("DRUG_STRG"),10," ").alias("DRUG_STRG"),
    rpad(col("ORIG_MBR"),2," ").alias("ORIG_MBR"),
    col("DT_OF_INJURY"),
    col("FEE_AMT"),
    rpad(col("ESI_REF_NO"),14," ").alias("ESI_REF_NO"),
    rpad(col("CLNT_CUST_ID"),20," ").alias("CLNT_CUST_ID"),
    rpad(col("PLN_TYP"),10," ").alias("PLN_TYP"),
    col("ESI_ADJDCT_REF_NO"),
    col("ESI_ANCLRY_AMT"),
    rpad(col("ESI_CLNT_GNRL_PRPS_AREA"),40," ").alias("ESI_CLNT_GNRL_PRPS_AREA"),
    rpad(col("GRP_ID"),8," ").alias("GRP_ID"),
    rpad(col("SUBGRP_ID"),4," ").alias("SUBGRP_ID"),
    rpad(col("CLS_PLN_ID"),4," ").alias("CLS_PLN_ID"),
    rpad(col("PAID_DATE"),10," ").alias("PAID_DATE"),
    rpad(col("PRTL_FILL_STTUS_CD"),1," ").alias("PRTL_FILL_STTUS_CD"),
    col("ESI_BILL_DT"),
    rpad(col("FSA_VNDR_CD"),2," ").alias("FSA_VNDR_CD"),
    rpad(col("PICA_DRUG_CD"),1," ").alias("PICA_DRUG_CD"),
    col("AMT_CLMED"),
    col("AMT_DSALW"),
    rpad(col("FED_DRUG_CLS_CD"),1," ").alias("FED_DRUG_CLS_CD"),
    col("DEDCT_AMT"),
    rpad(col("BNF_COPAY_100"),1," ").alias("BNF_COPAY_100"),
    rpad(col("CLM_PRCS_TYP"),1," ").alias("CLM_PRCS_TYP"),
    col("INDEM_HIER_TIER_NO"),
    rpad(col("FLR"),1," ").alias("FLR"),
    rpad(col("MCARE_D_COV_DRUG"),1," ").alias("MCARE_D_COV_DRUG"),
    rpad(col("RETRO_LICS_CD"),1," ").alias("RETRO_LICS_CD"),
    col("RETRO_LICS_AMT"),
    col("LICS_SBSDY_AMT"),
    rpad(col("MED_B_DRUG"),1," ").alias("MED_B_DRUG"),
    rpad(col("MED_B_CLM"),1," ").alias("MED_B_CLM"),
    rpad(col("PRESCRIBER_QLFR"),2," ").alias("PRESCRIBER_QLFR"),
    rpad(col("PRESCRIBER_ID_NPI"),10," ").alias("PRESCRIBER_ID_NPI"),
    rpad(col("PDX_QLFR"),2," ").alias("PDX_QLFR"),
    rpad(col("PDX_ID_NPI"),10," ").alias("PDX_ID_NPI"),
    col("HRA_APLD_AMT"),
    col("ESI_THER_CLS"),
    rpad(col("HIC_NO"),12," ").alias("HIC_NO"),
    rpad(col("HRA_FLAG"),1," ").alias("HRA_FLAG"),
    col("DOSE_CD"),
    rpad(col("LOW_INCM"),1," ").alias("LOW_INCM"),
    rpad(col("RTE_OF_ADMIN"),2," ").alias("RTE_OF_ADMIN"),
    col("DEA_SCHD"),
    col("COPAY_BNF_OPT"),
    col("GNRC_PROD_IN_GPI"),
    rpad(col("PRESCRIBER_SPEC"),10," ").alias("PRESCRIBER_SPEC"),
    rpad(col("VAL_CD"),18," ").alias("VAL_CD"),
    rpad(col("PRI_CARE_PDX"),18," ").alias("PRI_CARE_PDX"),
    rpad(col("OFC_OF_INSPECTOR_GNRL_OIG"),1," ").alias("OFC_OF_INSPECTOR_GNRL_OIG"),
    rpad(col("FLR3"),145," ").alias("FLR3"),
    col("PSL_FMLY_MET_AMT"),
    col("PSL_MBR_MET_AMT"),
    col("PSL_FMLY_AMT"),
    col("DED_FMLY_MET_AMT"),
    col("DED_FMLY_AMT"),
    col("MOPS_FMLY_AMT"),
    col("MOPS_FMLY_MET_AMT"),
    col("MOPS_MBR_MET_AMT"),
    col("DED_MBR_MET_AMT"),
    col("PSL_APLD_AMT"),
    col("MOPS_APLD_AMT"),
    rpad(col("PAR_PDX_IND"),1," ").alias("PAR_PDX_IND"),
    col("COPAY_PCT_AMT"),
    col("COPAY_FLAT_AMT"),
    rpad(col("CLM_TRANSMITTAL_METH"),1," ").alias("CLM_TRANSMITTAL_METH"),
    rpad(col("FLR4"),82," ").alias("FLR4")
)
write_files(
    df_Medtrak_DrgClmLand_write,
    f"{adls_path}/verified/MedtrakDrugClm_Land.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)