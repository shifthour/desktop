# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC Â© Copyright 2020 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY :   OptumMedDSupplementLoadSeq
# MAGIC  
# MAGIC DESCRIPTION:  This process runs to consume the OPTUMRX MedD Supplemental file.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                         Date                 Project/Altiris #                                                                               Change Description                                                                                          Development Project      Code Reviewer          Date Reviewed       
# MAGIC                                                                                                                                                                                                                                                                                      
# MAGIC Rekha Radhakrishna     11-20-2020          6264 - PBM Phase II - Government Programs                          Initial Programming                                                                                                       IntegrateDev2      Abhiram Dasarathy\(9)2020-12-10
# MAGIC 
# MAGIC Rekha Radhakrishna       01/18/2021     PBM Phase 2 - Day 2                                                             Changed mapping for  EGWP_MBR_AMT_TOWARD_CATO_AMT                              IntegrateDev2          Jaideep Mankala      01/26/2021

# MAGIC Pkey DRUG_CLM_MCARE_BNF_PHS_DTL_SK Is  Generated uskng K table methodology.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import DataFrame
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')
InFile = get_widget_value('InFile','')
IDSRunCycle = get_widget_value('IDSRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query_db_K_DRUG_CLM_MCARE_BNF_PHS_DTL_read = f"SELECT DRUG_CLM_MCARE_BNF_PHS_DTL_SK, CLM_ID, CLM_REPRCS_SEQ_NO, SRC_SYS_CD, CRT_RUN_CYC_EXCTN_SK FROM {IDSOwner}.K_DRUG_CLM_MCARE_BNF_PHS_DTL"
df_db_K_DRUG_CLM_MCARE_BNF_PHS_DTL_read = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db_K_DRUG_CLM_MCARE_BNF_PHS_DTL_read)
    .load()
)

schema_SeqFile_CLM_MedD_PDX_SUPLMT = StructType([
    StructField("PDX_CLM_NO", StringType(), False),
    StructField("CLM_SEQ_NO", StringType(), False),
    StructField("CLM_STTUS", StringType(), False),
    StructField("EX7_RCRD_SEQ_NO", StringType(), False),
    StructField("TCD_CAR_ID", StringType(), False),
    StructField("EX8_DT_SUBMT", StringType(), False),
    StructField("TCD_ACCT_ID", StringType(), False),
    StructField("TCD_GRP_ID", StringType(), False),
    StructField("TCD_MBR_ID", StringType(), False),
    StructField("TCD_SBM_SVC_DT", StringType(), False),
    StructField("TC4_CNTR_NO", StringType(), False),
    StructField("TC4_PLN_BNF_PCKG", StringType(), False),
    StructField("TC4_TROOP_SCHD", StringType(), False),
    StructField("TC4_DRUG_COV_STS_SCHED", StringType(), False),
    StructField("TC4_DRUG_COV_STS_CD", StringType(), False),
    StructField("TC4_CAR_ID", StringType(), False),
    StructField("TC4_ACCT_ID", StringType(), False),
    StructField("TC4_GRP_ID", StringType(), False),
    StructField("TC4_PLN_CD", StringType(), False),
    StructField("TC4_PLN_EFF_DT", StringType(), False),
    StructField("TC4_PDP_SEQ_NO", StringType(), False),
    StructField("TC4_CPP_AMT", StringType(), False),
    StructField("TC4_PRIOR_TROOP_AMT", StringType(), False),
    StructField("CBP_SBM_OTHR_PAYOR_PD_AMT", StringType(), False),
    StructField("EX4_CLT_INGR_CST", StringType(), False),
    StructField("EX4_CLT_DISPNS_FEE", StringType(), False),
    StructField("EX4_CLT_SLS_TAX", StringType(), False),
    StructField("EX4_CLT_PATN_PAY", StringType(), False),
    StructField("EX4_PHR_PATN_PAY", StringType(), False),
    StructField("TAP_SBM_DISPNS_STTUS", StringType(), False),
    StructField("EX4_LICS_AMT", StringType(), False),
    StructField("EX4_LICS_TROOP_SCHD", StringType(), False),
    StructField("EX4_LICS_SCHD_STEP", StringType(), False),
    StructField("EX4_LICS_TROOP_NOR_STEPS", StringType(), False),
    StructField("EX4_DEDCT_LVL_DRUG_SPEND", StringType(), False),
    StructField("EX4_AMT_TOWARD_PLN_DEDCT", StringType(), False),
    StructField("EX4_DEDCT_LVL_TROOP_AMT", StringType(), False),
    StructField("EX4_PLN_GAP_AMT", StringType(), False),
    StructField("EX4_AMT_TOWARD_PLN_GAP", StringType(), False),
    StructField("EX4_PLN_CATO_AMT", StringType(), False),
    StructField("EX4_AMT_TOWARD_PLN_CATO", StringType(), False),
    StructField("EX4_PLN_INIT_COV_AMT", StringType(), False),
    StructField("EX4_PLNPY_AMT_INIT_COV_LVL", StringType(), False),
    StructField("EX4_MBR_AMT_TOWARD_DEDCT", StringType(), False),
    StructField("EX4_DRUG_SPEND_TOWARD_DEDCT", StringType(), False),
    StructField("EX4_MBR_AMT_TOWARD_COV_GAP", StringType(), False),
    StructField("EX4_DRUG_SPEND_TOWARD_COV_GAP", StringType(), False),
    StructField("EX4_MBR_AMT_TOWARD_CATO", StringType(), False),
    StructField("EX4_DRUG_SPEND_TOWARD_CATO", StringType(), False),
    StructField("EX4_PLN_TROOP_SCHD", StringType(), False),
    StructField("EX4_MBR_AMT_TOWARD_INIT_COV", StringType(), False),
    StructField("EX4_DRUG_SPEND_TOWARD_INIT_COV", StringType(), False),
    StructField("EX4_CATO_COV_CD", StringType(), False),
    StructField("EX4_GROST_DRUG_CST_ABOVE_AMT", StringType(), False),
    StructField("EX4_GROS_DRUG_CST_BELOW_AMT", StringType(), False),
    StructField("EX4_NONCOV_PLN_PD_AMT", StringType(), False),
    StructField("EX4_DRUG_SPEND_AMT", StringType(), False),
    StructField("EX4_EST_RBT_AT_POS", StringType(), False),
    StructField("EX4_CLT_VCCN_ADM_FEE", StringType(), False),
    StructField("EX4_REPRCS_CLM_SEQ", StringType(), False),
    StructField("EX4_UPDT_BY_CD", StringType(), False),
    StructField("TCD_PROD_KEY", StringType(), False),
    StructField("TCD_SBM_PROD_ID", StringType(), False),
    StructField("TCD_GNRC_PROD_NO", StringType(), False),
    StructField("TCD_SBM_QTY_DISPNS", StringType(), False),
    StructField("TCD_SBM_DAYS_SUPL", StringType(), False),
    StructField("TCD_SBM_RX_NO", StringType(), False),
    StructField("TCD_SBM_FILL_NO", StringType(), False),
    StructField("TCD_SBM_CMPND_CD", StringType(), False),
    StructField("TCD_SBM_PROD_SEL_CD", StringType(), False),
    StructField("TCD_COB_CLM_IN", StringType(), False),
    StructField("TCD_CLM_ORIG_FLAG", StringType(), False),
    StructField("TCD_GNRC_IN", StringType(), False),
    StructField("TCD_RMBRMT_FLAG", StringType(), False),
    StructField("EX4_SVC_CLM", StringType(), False),
    StructField("TCD_PDX_NTWK_ID", StringType(), False),
    StructField("CPQ_SBM_SVC_PROV_ID_QLFR", StringType(), False),
    StructField("CPQ_SBM_SVC_PROV_ID", StringType(), False),
    StructField("TCD_SBM_SVC_PROV_ID_QLFR", StringType(), False),
    StructField("TCD_SBM_SVC_PROV_ID", StringType(), False),
    StructField("TCD_SBM_PRSCRBR_ID_QLFR", StringType(), False),
    StructField("TCD_SBM_PRSCRBR_ID", StringType(), False),
    StructField("TCD_SBM_OTHR_COV_CD", StringType(), False),
    StructField("PDT_RBL_OTHR_PAYOR_AMT_RECOGNIZED", StringType(), False),
    StructField("TC4_LICS_DRUG_SPEND_DEDCT_BAL", StringType(), False),
    StructField("TC4_LICS_DRUG_SPEND_DEDCT_AMT", StringType(), False),
    StructField("TC4_CLNT_PATN_PAY", StringType(), False),
    StructField("EX4_SUBRO_AMT", StringType(), False),
    StructField("EX4_PHR_INGR_CST", StringType(), False),
    StructField("EX4_PHR_DISPNS_FEE", StringType(), False),
    StructField("EX4_PHR_SLS_TAX", StringType(), False),
    StructField("EX4_PHR_VCCN_ADM_FEE", StringType(), False),
    StructField("EX4_PHR_OTHR_PAYOR_AMT_RECOGNIZED", StringType(), False),
    StructField("TC3_SBM_RX_ORIG_CD", StringType(), False),
    StructField("EX4_OUT_OF_NTWK_CD", StringType(), False),
    StructField("TC4_ACCUM_BSS", StringType(), False),
    StructField("TCD_FINL_PLN_CD", StringType(), False),
    StructField("TCD_FINL_PLN_EFF_DT", StringType(), False),
    StructField("EX4_DT_ORIG_CLM_RCVD", StringType(), False),
    StructField("EX4_ADJDCT_BEG_TS", StringType(), False),
    StructField("TC4_TOT_GROS_DURG_CST_ACCUM", StringType(), False),
    StructField("TC4_TRUE_OOP_ACCUM", StringType(), False),
    StructField("TC4_BRND_GNRC_CD", StringType(), False),
    StructField("EX4_BEG_BNF_PHS", StringType(), False),
    StructField("EX4_END_BNF_PHS", StringType(), False),
    StructField("EX4_RPTD_GAP_DSCNT", StringType(), False),
    StructField("EX4_TIER", StringType(), False),
    StructField("EX4_FRMLRY_CD", StringType(), False),
    StructField("EX4_GAP_DSCNT_PLN_OVRD_CD", StringType(), False),
    StructField("MMD_COPAY_CAT", StringType(), False),
    StructField("MMD_SBSDY_LVL", StringType(), False),
    StructField("TC4_TRUE_DSCNT_ELIG_AMT", StringType(), False),
    StructField("TBR_REJ_RSN_1", StringType(), False),
    StructField("TBR_REJ_RSN_2", StringType(), False),
    StructField("TBR_REJ_RSN_3", StringType(), False),
    StructField("TC4_GAP_BRND_DSCNT", StringType(), False),
    StructField("TC2_MCAID_SUBRO", StringType(), False),
    StructField("TC2_SH_CYC_PROV_QLFD", StringType(), False),
    StructField("TC2_SH_CYC_PROD_QLFD", StringType(), False),
    StructField("TC2_SH_CYC_CLM_QLFD", StringType(), False),
    StructField("TCS_PROD_CLM_NO", StringType(), False),
    StructField("TCS_PROD_CLM_SEQ_NO", StringType(), False),
    StructField("EX4_PROD_CLM_PROD_ID", StringType(), False),
    StructField("EX4_PROD_CLM_PROD_KEY", StringType(), False),
    StructField("EX4_PROD_CLM_GPI", StringType(), False),
    StructField("EX4_PRSCRBR_NPI", StringType(), False),
    StructField("TC3_SBM_PDX_SVC_TYP", StringType(), False),
    StructField("TC3_PATN_RESDNC", StringType(), False),
    StructField("TCD_SBM_CLRFCTN_CD", StringType(), False),
    StructField("TC2_DCS_DLY_PRORTN", StringType(), False),
    StructField("AMN_AMNDMNT_IN", StringType(), False),
    StructField("AMN_PDX_SVC_TYP", StringType(), False),
    StructField("AMN_PRSCRBR_ID", StringType(), False),
    StructField("AMN_PRSCRBR_QLFR", StringType(), False),
    StructField("AMN_RX_ORIG", StringType(), False),
    StructField("AMN_PATN_RESDNC", StringType(), True),
    StructField("AMN_CNTR", StringType(), False),
    StructField("AMN_PBP", StringType(), False),
    StructField("EX4_CRT_BY_VRSN", StringType(), False),
    StructField("PD7_ADJ_SEQ", StringType(), False),
    StructField("EX7_DIAG_LIST_ID", StringType(), False),
    StructField("EX7_DIAG_LIST_QLFR", StringType(), False),
    StructField("PDT_RSP_PATN_PAY_AMT", StringType(), False),
    StructField("PDT_RSP_INGR_CST_PD", StringType(), False),
    StructField("PDT_RSP_DISPNS_FEE_PD", StringType(), False),
    StructField("PDT_RSP_FLAT_SLS_TAX_PD", StringType(), False),
    StructField("PDT_RSP_COPAY_AMT_PD", StringType(), False),
    StructField("PDT_RSP_COPAY_FLAT_AMT", StringType(), False),
    StructField("PDT_RSP_COPAY_PCT_AMT", StringType(), False),
    StructField("PDT_RSP_TOT_AMT_PD", StringType(), False),
    StructField("PDT_RSP_ATRBD_SLS_TAX_AMT", StringType(), False),
    StructField("PDT_RSP_INCNTV_FEE_PD", StringType(), False),
    StructField("PDT_RSP_PROF_SVC_FEE_PD", StringType(), False),
    StructField("PDT_RSP_OTHR_PAYER_AMT_RECOGNIZED", StringType(), False),
    StructField("PDT_RSP_TOT_OTHR_AMT", StringType(), False),
    StructField("PDT_RSP_AMT_ATRBD_PRCSR_FEE", StringType(), False),
    StructField("PDT_RSP_PATN_SLS_TAX_AMT", StringType(), False),
    StructField("PDT_RSP_PLN_SLS_TAX_AMT", StringType(), False),
    StructField("PDT_RSP_SPEND_ACCT_AMT_REMAIN", StringType(), False),
    StructField("PDT_RSP_HLTH_PLN_FUND_ASSTNC_AMT", StringType(), False),
    StructField("PDT_RSP_AMT_ATRBD_PROV_NTWK_SEL", StringType(), False),
    StructField("PDT_RSP_AMT_ATRBD_PROD_BRND_DRUG", StringType(), False),
    StructField("PDT_RSP_AMT_ATRBD_PROD_NPRFR_FRMLRY", StringType(), False),
    StructField("PDT_RSP_AMT_ATRBD_PROD_BRND_NPRFR", StringType(), False),
    StructField("PDT_RSP_AMT_ATRBD_COV_GAP", StringType(), False),
    StructField("PDT_RSP_ING_CST_CNTR_AMT", StringType(), False),
    StructField("PDT_RSP_DISPNS_FEE_CNTR_AMT", StringType(), False),
    StructField("EX4_MCARE_PLN_TYP", StringType(), False),
    StructField("EX4_EGWP_PLN_TROOP_SCHD", StringType(), False),
    StructField("EGWP_PLN_TROOP_SCHD_STEP", StringType(), False),
    StructField("EX4_EGWP_DEDCT_LVL_DRUG_SPEND", StringType(), False),
    StructField("EX4_EGWP_AMT_TOWARD_PLN_DEDCT", StringType(), False),
    StructField("EX4_EGWP_DEDCT_LVL_TROOP_AMT", StringType(), False),
    StructField("EX4_EGWP_PLN_GAP_AMT", StringType(), False),
    StructField("EX4_EGWP_AMT_TOWARD_PLN_GAP_AMT", StringType(), False),
    StructField("EX4_EGWP_PLN_CATO_AMT", StringType(), False),
    StructField("EX4_EGWP_AMT_TOWARD_PLN_CATO", StringType(), False),
    StructField("EX4_EGWP_PLN_INIT_COV_AMT", StringType(), False),
    StructField("EX4_EGWP_PLN_PAY_AMT_INIT_COV_LVL", StringType(), False),
    StructField("EX4_EGWP_MBR_AMT_TOWARD_DEDCT", StringType(), False),
    StructField("EX4_EGWP_DRUG_SPEND_TOWARD_DEDCT", StringType(), False),
    StructField("EX4_EGWP_MBR_AMT_TOWARD_GAP", StringType(), False),
    StructField("EX4_EGWP_DRUG_SPEND_TOWARD_GAP", StringType(), False),
    StructField("EX4_EGWP_MBR_AMT_TOWARD_CATO", StringType(), False),
    StructField("EX4_EGWP_DRUG_SPEND_TOWARD_CATO", StringType(), False),
    StructField("EX4_EGWP_MBR_AMT_TOWARD_INIT_COV", StringType(), False),
    StructField("EX4_EGWP_DRUG_SPEND_TOWARD_INIT_COV", StringType(), False),
    StructField("EX4_DUAL_AMT", StringType(), False),
    StructField("EX4_PART_B_DRUG_SPEND_TOWARD_OOP", StringType(), False),
    StructField("EX4_PART_B_MBR_AMT_TOWARD_OOP", StringType(), False),
])

df_SeqFile_CLM_MedD_PDX_SUPLMT = (
    spark.read
    .option("header","false")
    .option("delimiter",",")
    .schema(schema_SeqFile_CLM_MedD_PDX_SUPLMT)
    .csv(f"{adls_path_raw}/landing/{InFile}")
)

df_Business_Rules_1 = df_SeqFile_CLM_MedD_PDX_SUPLMT.withColumn(
    "svClmId",
    F.when(
        trim(F.col("CLM_STTUS")) == F.lit("X"),
        trim(F.col("PDX_CLM_NO")) + trim(F.col("CLM_SEQ_NO")) + F.lit("R")
    ).otherwise(
        trim(F.col("PDX_CLM_NO")) + trim(F.col("CLM_SEQ_NO"))
    )
)

df_Business_Rules_2 = (
    df_Business_Rules_1
    .withColumn("PDX_CLM_NO_out", trim(F.col("PDX_CLM_NO")))
    .withColumn("CLM_REPRCS_SEQ_NO_out", trim(F.col("EX4_REPRCS_CLM_SEQ")))
    .withColumn("CLM_STTUS_out", F.col("CLM_STTUS"))
    .withColumn("TC4_CNTR_NO_out", trim(F.col("TC4_CNTR_NO")))
    .withColumn("TC4_PLN_BNF_PCKG_out", trim(F.col("TC4_PLN_BNF_PCKG")))
    .withColumn("TC4_DRUG_COV_STS_SCHED_out", trim(F.col("TC4_DRUG_COV_STS_SCHED")))
    .withColumn("TC4_DRUG_COV_STS_CD_out", trim(F.col("TC4_DRUG_COV_STS_CD")))
    .withColumn("EX4_LICS_AMT_out", trim(F.col("EX4_LICS_AMT")))
    .withColumn("EX4_MBR_AMT_TOWARD_DEDCT_out", trim(F.col("EX4_MBR_AMT_TOWARD_DEDCT")))
    .withColumn("EX4_MBR_AMT_TOWARD_COV_GAP_out", trim(F.col("EX4_MBR_AMT_TOWARD_COV_GAP")))
    .withColumn("EX4_MBR_AMT_TOWARD_CATO_out", trim(F.col("EX4_MBR_AMT_TOWARD_CATO")))
    .withColumn("EX4_MBR_AMT_TOWARD_INIT_COV_out", trim(F.col("EX4_MBR_AMT_TOWARD_INIT_COV")))
    .withColumn("EX4_RPTD_GAP_DSCNT_out", trim(F.col("EX4_RPTD_GAP_DSCNT")))
    .withColumn("TC4_GAP_BRND_DSCNT_out", trim(F.col("TC4_GAP_BRND_DSCNT")))
    .withColumn("EX4_EGWP_PLN_CATO_AMT_out", trim(F.col("EX4_EGWP_PLN_CATO_AMT")))
    .withColumn("EX4_EGWP_PLN_INIT_COV_AMT_out", trim(F.col("EX4_EGWP_PLN_INIT_COV_AMT")))
    .withColumn("EX4_EGWP_MBR_AMT_TOWARD_DEDCT_out", trim(F.col("EX4_EGWP_MBR_AMT_TOWARD_DEDCT")))
    .withColumn("EX4_EGWP_MBR_AMT_TOWARD_GAP_out", trim(F.col("EX4_EGWP_MBR_AMT_TOWARD_GAP")))
    .withColumn("EX4_EGWP_DRUG_SPEND_TOWARD_GAP_out", trim(F.col("EX4_EGWP_DRUG_SPEND_TOWARD_GAP")))
    .withColumn("EX4_DUAL_AMT_out", trim(F.col("EX4_DUAL_AMT")))
    .withColumn("EX4_PART_B_DRUG_SPEND_TOWARD_OOP_out", trim(F.col("EX4_PART_B_DRUG_SPEND_TOWARD_OOP")))
    .withColumn("EX4_PART_B_MBR_AMT_TOWARD_OOP_out", trim(F.col("EX4_PART_B_MBR_AMT_TOWARD_OOP")))
    .withColumn("TCD_GNRC_IN_out", trim(F.col("TCD_GNRC_IN")))
    .withColumn("EX8_DT_SUBMT_out", trim(F.col("EX8_DT_SUBMT")))
    .withColumn("EX4_DT_ORIG_CLM_RCVD_out", trim(F.col("EX4_DT_ORIG_CLM_RCVD")))
    .withColumn("PDT_RSP_TOT_AMT_PD_out", trim(F.col("PDT_RSP_TOT_AMT_PD")))
    .withColumn("EX4_PHR_PATN_PAY_out", trim(F.col("EX4_PHR_PATN_PAY")))
    .withColumn("TC4_CPP_AMT_out", trim(F.col("TC4_CPP_AMT")))
    .withColumn("EX4_DEDCT_LVL_DRUG_SPEND_out", trim(F.col("EX4_DEDCT_LVL_DRUG_SPEND")))
    .withColumn("EX4_DEDCT_LVL_TROOP_AMT_out", trim(F.col("EX4_DEDCT_LVL_TROOP_AMT")))
    .withColumn("EX4_DRUG_SPEND_AMT_out", trim(F.col("EX4_DRUG_SPEND_AMT")))
    .withColumn("EX4_DRUG_SPEND_TOWARD_DEDCT_out", trim(F.col("EX4_DRUG_SPEND_TOWARD_DEDCT")))
    .withColumn("EX4_DRUG_SPEND_TOWARD_INIT_COV_out", trim(F.col("EX4_DRUG_SPEND_TOWARD_INIT_COV")))
    .withColumn("EX4_EGWP_DEDCT_LVL_TROOP_AMT_out", trim(F.col("EX4_EGWP_DEDCT_LVL_TROOP_AMT")))
    .withColumn("EX4_EGWP_MBR_AMT_TOWARD_CATO_out", trim(F.col("EX4_EGWP_MBR_AMT_TOWARD_CATO")))
    .withColumn("EX4_EGWP_DRUG_SPEND_TOWARD_CATO_out", trim(F.col("EX4_EGWP_DRUG_SPEND_TOWARD_CATO")))
    .withColumn("EX4_EGWP_DRUG_SPEND_TOWARD_DEDCT_out", trim(F.col("EX4_EGWP_DRUG_SPEND_TOWARD_DEDCT")))
    .withColumn("EX4_EGWP_MBR_AMT_TOWARD_INIT_COV_out", trim(F.col("EX4_EGWP_MBR_AMT_TOWARD_INIT_COV")))
    .withColumn("EX4_EGWP_PLN_GAP_AMT_out", trim(F.col("EX4_EGWP_PLN_GAP_AMT")))
    .withColumn("EX4_EGWP_AMT_TOWARD_PLN_CATO_out", trim(F.col("EX4_EGWP_AMT_TOWARD_PLN_CATO")))
    .withColumn("EX4_EGWP_AMT_TOWARD_PLN_DEDCT_out", trim(F.col("EX4_EGWP_AMT_TOWARD_PLN_DEDCT")))
    .withColumn("EX4_EGWP_AMT_TOWARD_PLN_GAP_AMT_out", trim(F.col("EX4_EGWP_AMT_TOWARD_PLN_GAP_AMT")))
    .withColumn("EX4_GROST_DRUG_CST_ABOVE_AMT_out", trim(F.col("EX4_GROST_DRUG_CST_ABOVE_AMT")))
    .withColumn("EX4_GROS_DRUG_CST_BELOW_AMT_out", trim(F.col("EX4_GROS_DRUG_CST_BELOW_AMT")))
    .withColumn("TC4_LICS_DRUG_SPEND_DEDCT_AMT_out", trim(F.col("TC4_LICS_DRUG_SPEND_DEDCT_AMT")))
    .withColumn("TC4_LICS_DRUG_SPEND_DEDCT_BAL_out", trim(F.col("TC4_LICS_DRUG_SPEND_DEDCT_BAL")))
    .withColumn("EX4_NONCOV_PLN_PD_AMT_out", trim(F.col("EX4_NONCOV_PLN_PD_AMT")))
    .withColumn("TC4_PRIOR_TROOP_AMT_out", trim(F.col("TC4_PRIOR_TROOP_AMT")))
    .withColumn("EX4_SUBRO_AMT_out", trim(F.col("EX4_SUBRO_AMT")))
    .withColumn("TC4_TRUE_OOP_ACCUM_out", trim(F.col("TC4_TRUE_OOP_ACCUM")))
    .withColumn("TC4_TOT_GROS_DURG_CST_ACCUM_out", trim(F.col("TC4_TOT_GROS_DURG_CST_ACCUM")))
    .withColumn("EX4_DRUG_SPEND_TOWARD_CATO_out", trim(F.col("EX4_DRUG_SPEND_TOWARD_CATO")))
    .withColumn("EX4_DRUG_SPEND_TOWARD_COV_GAP_out", trim(F.col("EX4_DRUG_SPEND_TOWARD_COV_GAP")))
    .withColumn("EX4_AMT_TOWARD_PLN_CATO_out", trim(F.col("EX4_AMT_TOWARD_PLN_CATO")))
    .withColumn("EX4_AMT_TOWARD_PLN_DEDCT_out", trim(F.col("EX4_AMT_TOWARD_PLN_DEDCT")))
    .withColumn("EX4_AMT_TOWARD_PLN_GAP_out", trim(F.col("EX4_AMT_TOWARD_PLN_GAP")))
    .withColumn("EX4_BEG_BNF_PHS_out", trim(F.col("EX4_BEG_BNF_PHS")))
    .withColumn("EX4_END_BNF_PHS_out", trim(F.col("EX4_END_BNF_PHS")))
    .withColumn("TC4_TROOP_SCHD_out", F.col("TC4_TROOP_SCHD"))
    .withColumn("CLM_ID_out", F.col("svClmId"))
    .withColumn("SRC_SYS_CD_out", F.lit(SrcSysCd))
)

df_lnk_MedDSuplmnt = df_Business_Rules_2.select(
    F.col("PDX_CLM_NO_out").alias("PDX_CLM_NO"),
    F.col("CLM_REPRCS_SEQ_NO_out").alias("CLM_REPRCS_SEQ_NO"),
    F.col("CLM_STTUS_out").alias("CLM_STTUS"),
    F.col("TC4_CNTR_NO_out").alias("TC4_CNTR_NO"),
    F.col("TC4_PLN_BNF_PCKG_out").alias("TC4_PLN_BNF_PCKG"),
    F.col("TC4_DRUG_COV_STS_SCHED_out").alias("TC4_DRUG_COV_STS_SCHED"),
    F.col("TC4_DRUG_COV_STS_CD_out").alias("TC4_DRUG_COV_STS_CD"),
    F.col("EX4_LICS_AMT_out").alias("EX4_LICS_AMT"),
    F.col("EX4_MBR_AMT_TOWARD_DEDCT_out").alias("EX4_MBR_AMT_TOWARD_DEDCT"),
    F.col("EX4_MBR_AMT_TOWARD_COV_GAP_out").alias("EX4_MBR_AMT_TOWARD_COV_GAP"),
    F.col("EX4_MBR_AMT_TOWARD_CATO_out").alias("EX4_MBR_AMT_TOWARD_CATO"),
    F.col("EX4_MBR_AMT_TOWARD_INIT_COV_out").alias("EX4_MBR_AMT_TOWARD_INIT_COV"),
    F.col("EX4_RPTD_GAP_DSCNT_out").alias("EX4_RPTD_GAP_DSCNT"),
    F.col("TC4_GAP_BRND_DSCNT_out").alias("TC4_GAP_BRND_DSCNT"),
    F.col("EX4_EGWP_PLN_CATO_AMT_out").alias("EX4_EGWP_PLN_CATO_AMT"),
    F.col("EX4_EGWP_PLN_INIT_COV_AMT_out").alias("EX4_EGWP_PLN_INIT_COV_AMT"),
    F.col("EX4_EGWP_MBR_AMT_TOWARD_DEDCT_out").alias("EX4_EGWP_MBR_AMT_TOWARD_DEDCT"),
    F.col("EX4_EGWP_MBR_AMT_TOWARD_GAP_out").alias("EX4_EGWP_MBR_AMT_TOWARD_GAP"),
    F.col("EX4_EGWP_DRUG_SPEND_TOWARD_GAP_out").alias("EX4_EGWP_DRUG_SPEND_TOWARD_GAP"),
    F.col("EX4_DUAL_AMT_out").alias("EX4_DUAL_AMT"),
    F.col("EX4_PART_B_DRUG_SPEND_TOWARD_OOP_out").alias("EX4_PART_B_DRUG_SPEND_TOWARD_OOP"),
    F.col("EX4_PART_B_MBR_AMT_TOWARD_OOP_out").alias("EX4_PART_B_MBR_AMT_TOWARD_OOP"),
    F.col("TCD_GNRC_IN_out").alias("TCD_GNRC_IN"),
    F.col("EX8_DT_SUBMT_out").alias("EX8_DT_SUBMT"),
    F.col("EX4_DT_ORIG_CLM_RCVD_out").alias("EX4_DT_ORIG_CLM_RCVD"),
    F.col("PDT_RSP_TOT_AMT_PD_out").alias("PDT_RSP_TOT_AMT_PD"),
    F.col("EX4_PHR_PATN_PAY_out").alias("EX4_PHR_PATN_PAY"),
    F.col("TC4_CPP_AMT_out").alias("TC4_CPP_AMT"),
    F.col("EX4_DEDCT_LVL_DRUG_SPEND_out").alias("EX4_DEDCT_LVL_DRUG_SPEND"),
    F.col("EX4_DEDCT_LVL_TROOP_AMT_out").alias("EX4_DEDCT_LVL_TROOP_AMT"),
    F.col("EX4_DRUG_SPEND_AMT_out").alias("EX4_DRUG_SPEND_AMT"),
    F.col("EX4_DRUG_SPEND_TOWARD_DEDCT_out").alias("EX4_DRUG_SPEND_TOWARD_DEDCT"),
    F.col("EX4_DRUG_SPEND_TOWARD_INIT_COV_out").alias("EX4_DRUG_SPEND_TOWARD_INIT_COV"),
    F.col("EX4_EGWP_DEDCT_LVL_TROOP_AMT_out").alias("EX4_EGWP_DEDCT_LVL_TROOP_AMT"),
    F.col("EX4_EGWP_MBR_AMT_TOWARD_CATO_out").alias("EX4_EGWP_MBR_AMT_TOWARD_CATO"),
    F.col("EX4_EGWP_DRUG_SPEND_TOWARD_CATO_out").alias("EX4_EGWP_DRUG_SPEND_TOWARD_CATO"),
    F.col("EX4_EGWP_DRUG_SPEND_TOWARD_DEDCT_out").alias("EX4_EGWP_DRUG_SPEND_TOWARD_DEDCT"),
    F.col("EX4_EGWP_MBR_AMT_TOWARD_INIT_COV_out").alias("EX4_EGWP_MBR_AMT_TOWARD_INIT_COV"),
    F.col("EX4_EGWP_PLN_GAP_AMT_out").alias("EX4_EGWP_PLN_GAP_AMT"),
    F.col("EX4_EGWP_AMT_TOWARD_PLN_CATO_out").alias("EX4_EGWP_AMT_TOWARD_PLN_CATO"),
    F.col("EX4_EGWP_AMT_TOWARD_PLN_DEDCT_out").alias("EX4_EGWP_AMT_TOWARD_PLN_DEDCT"),
    F.col("EX4_EGWP_AMT_TOWARD_PLN_GAP_AMT_out").alias("EX4_EGWP_AMT_TOWARD_PLN_GAP_AMT"),
    F.col("EX4_GROST_DRUG_CST_ABOVE_AMT_out").alias("EX4_GROST_DRUG_CST_ABOVE_AMT"),
    F.col("EX4_GROS_DRUG_CST_BELOW_AMT_out").alias("EX4_GROS_DRUG_CST_BELOW_AMT"),
    F.col("TC4_LICS_DRUG_SPEND_DEDCT_AMT_out").alias("TC4_LICS_DRUG_SPEND_DEDCT_AMT"),
    F.col("TC4_LICS_DRUG_SPEND_DEDCT_BAL_out").alias("TC4_LICS_DRUG_SPEND_DEDCT_BAL"),
    F.col("EX4_NONCOV_PLN_PD_AMT_out").alias("EX4_NONCOV_PLN_PD_AMT"),
    F.col("TC4_PRIOR_TROOP_AMT_out").alias("TC4_PRIOR_TROOP_AMT"),
    F.col("EX4_SUBRO_AMT_out").alias("EX4_SUBRO_AMT"),
    F.col("TC4_TRUE_OOP_ACCUM_out").alias("TC4_TRUE_OOP_ACCUM"),
    F.col("TC4_TOT_GROS_DURG_CST_ACCUM_out").alias("TC4_TOT_GROS_DURG_CST_ACCUM"),
    F.col("EX4_DRUG_SPEND_TOWARD_CATO_out").alias("EX4_DRUG_SPEND_TOWARD_CATO"),
    F.col("EX4_DRUG_SPEND_TOWARD_COV_GAP_out").alias("EX4_DRUG_SPEND_TOWARD_COV_GAP"),
    F.col("EX4_AMT_TOWARD_PLN_CATO_out").alias("EX4_AMT_TOWARD_PLN_CATO"),
    F.col("EX4_AMT_TOWARD_PLN_DEDCT_out").alias("EX4_AMT_TOWARD_PLN_DEDCT"),
    F.col("EX4_AMT_TOWARD_PLN_GAP_out").alias("EX4_AMT_TOWARD_PLN_GAP"),
    F.col("EX4_BEG_BNF_PHS_out").alias("EX4_BEG_BNF_PHS"),
    F.col("EX4_END_BNF_PHS_out").alias("EX4_END_BNF_PHS"),
    F.col("TC4_TROOP_SCHD_out").alias("TC4_TROOP_SCHD"),
    F.col("CLM_ID_out").alias("CLM_ID"),
    F.col("SRC_SYS_CD_out").alias("SRC_SYS_CD")
)

df_Copy_LnkLeftJn = df_lnk_MedDSuplmnt.select(
    "PDX_CLM_NO",
    "CLM_REPRCS_SEQ_NO",
    "CLM_STTUS",
    "TC4_CNTR_NO",
    "TC4_PLN_BNF_PCKG",
    "TC4_DRUG_COV_STS_SCHED",
    "TC4_DRUG_COV_STS_CD",
    "EX4_LICS_AMT",
    "EX4_MBR_AMT_TOWARD_DEDCT",
    "EX4_MBR_AMT_TOWARD_COV_GAP",
    "EX4_MBR_AMT_TOWARD_CATO",
    "EX4_MBR_AMT_TOWARD_INIT_COV",
    "EX4_RPTD_GAP_DSCNT",
    "TC4_GAP_BRND_DSCNT",
    "EX4_EGWP_PLN_CATO_AMT",
    "EX4_EGWP_PLN_INIT_COV_AMT",
    "EX4_EGWP_MBR_AMT_TOWARD_DEDCT",
    "EX4_EGWP_MBR_AMT_TOWARD_GAP",
    "EX4_EGWP_DRUG_SPEND_TOWARD_GAP",
    "EX4_DUAL_AMT",
    "EX4_PART_B_DRUG_SPEND_TOWARD_OOP",
    "EX4_PART_B_MBR_AMT_TOWARD_OOP",
    "TCD_GNRC_IN",
    "EX8_DT_SUBMT",
    "EX4_DT_ORIG_CLM_RCVD",
    "PDT_RSP_TOT_AMT_PD",
    "EX4_PHR_PATN_PAY",
    "TC4_CPP_AMT",
    "EX4_DEDCT_LVL_DRUG_SPEND",
    "EX4_DEDCT_LVL_TROOP_AMT",
    "EX4_DRUG_SPEND_AMT",
    "EX4_DRUG_SPEND_TOWARD_DEDCT",
    "EX4_DRUG_SPEND_TOWARD_INIT_COV",
    "EX4_EGWP_DEDCT_LVL_TROOP_AMT",
    "EX4_EGWP_MBR_AMT_TOWARD_CATO",
    "EX4_EGWP_DRUG_SPEND_TOWARD_CATO",
    "EX4_EGWP_DRUG_SPEND_TOWARD_DEDCT",
    "EX4_EGWP_MBR_AMT_TOWARD_INIT_COV",
    "EX4_EGWP_PLN_GAP_AMT",
    "EX4_EGWP_AMT_TOWARD_PLN_CATO",
    "EX4_EGWP_AMT_TOWARD_PLN_DEDCT",
    "EX4_EGWP_AMT_TOWARD_PLN_GAP_AMT",
    "EX4_GROST_DRUG_CST_ABOVE_AMT",
    "EX4_GROS_DRUG_CST_BELOW_AMT",
    "TC4_LICS_DRUG_SPEND_DEDCT_AMT",
    "TC4_LICS_DRUG_SPEND_DEDCT_BAL",
    "EX4_NONCOV_PLN_PD_AMT",
    "TC4_PRIOR_TROOP_AMT",
    "EX4_SUBRO_AMT",
    "TC4_TRUE_OOP_ACCUM",
    "TC4_TOT_GROS_DURG_CST_ACCUM",
    "EX4_DRUG_SPEND_TOWARD_CATO",
    "EX4_DRUG_SPEND_TOWARD_COV_GAP",
    "EX4_AMT_TOWARD_PLN_CATO",
    "EX4_AMT_TOWARD_PLN_DEDCT",
    "EX4_AMT_TOWARD_PLN_GAP",
    "EX4_BEG_BNF_PHS",
    "EX4_END_BNF_PHS",
    "TC4_TROOP_SCHD",
    "CLM_ID",
    "SRC_SYS_CD"
)

df_Copy_LnkRemovDup = df_lnk_MedDSuplmnt.select(
    F.col("CLM_ID"),
    F.col("CLM_REPRCS_SEQ_NO"),
    F.col("SRC_SYS_CD")
)

df_RemDup = dedup_sort(
    df_Copy_LnkRemovDup,
    partition_cols=["CLM_ID","CLM_REPRCS_SEQ_NO","SRC_SYS_CD"],
    sort_cols=[]
)

df_jn_left_NaturalKeys = (
    df_RemDup.alias("lnkRemDupDataOut")
    .join(
        df_db_K_DRUG_CLM_MCARE_BNF_PHS_DTL_read.alias("lnk_to_jn"),
        (F.col("lnkRemDupDataOut.CLM_ID") == F.col("lnk_to_jn.CLM_ID")) &
        (F.col("lnkRemDupDataOut.CLM_REPRCS_SEQ_NO") == F.col("lnk_to_jn.CLM_REPRCS_SEQ_NO")) &
        (F.col("lnkRemDupDataOut.SRC_SYS_CD") == F.col("lnk_to_jn.SRC_SYS_CD")),
        how="left"
    )
    .select(
        F.col("lnkRemDupDataOut.CLM_ID").alias("CLM_ID"),
        F.col("lnkRemDupDataOut.CLM_REPRCS_SEQ_NO").alias("CLM_REPRCS_SEQ_NO"),
        F.col("lnkRemDupDataOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnk_to_jn.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("lnk_to_jn.DRUG_CLM_MCARE_BNF_PHS_DTL_SK").alias("DRUG_CLM_MCARE_BNF_PHS_DTL_SK")
    )
)

df_xfm_PKEYgen_intermediate = df_jn_left_NaturalKeys.withColumn("orig_sk", F.col("DRUG_CLM_MCARE_BNF_PHS_DTL_SK"))

df_enriched = df_xfm_PKEYgen_intermediate
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"DRUG_CLM_MCARE_BNF_PHS_DTL_SK",<schema>,<secret_name>)

df_enriched = df_enriched.withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(IDSRunCycle))

df_lnk_jn_right = df_enriched.select(
    F.col("CLM_ID"),
    F.col("CLM_REPRCS_SEQ_NO"),
    F.col("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("DRUG_CLM_MCARE_BNF_PHS_DTL_SK")
)

df_lnk_to_db_write = (
    df_enriched
    .filter(
        (F.col("orig_sk").isNull()) | (F.col("orig_sk") == F.lit(0))
    )
    .select(
        F.col("CLM_ID"),
        F.col("CLM_REPRCS_SEQ_NO"),
        F.col("SRC_SYS_CD"),
        F.lit(IDSRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("DRUG_CLM_MCARE_BNF_PHS_DTL_SK")
    )
)

df_lnk_to_db_write.createOrReplaceTempView("TEMP_lnk_to_db_write_for_stage_DB2")
# We do not use createOrReplaceTempView according to instructions. Instead, we must write to a physical STAGING table.
spark.sql("DROP TABLE IF EXISTS STAGING.OptumMedDSupplementExtr_db_K_DRUG_CLM_MCARE_BNF_PHS_DTL_write_temp")
(
    df_lnk_to_db_write.write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", "STAGING.OptumMedDSupplementExtr_db_K_DRUG_CLM_MCARE_BNF_PHS_DTL_write_temp")
    .mode("append")
    .save()
)

merge_sql_db_K_DRUG_CLM_MCARE_BNF_PHS_DTL_write = f"""
MERGE INTO {IDSOwner}.K_DRUG_CLM_MCARE_BNF_PHS_DTL AS T
USING STAGING.OptumMedDSupplementExtr_db_K_DRUG_CLM_MCARE_BNF_PHS_DTL_write_temp AS S
ON
  T.CLM_ID = S.CLM_ID AND 
  T.CLM_REPRCS_SEQ_NO = S.CLM_REPRCS_SEQ_NO AND
  T.SRC_SYS_CD = S.SRC_SYS_CD
WHEN MATCHED THEN
  UPDATE SET
    T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
    T.DRUG_CLM_MCARE_BNF_PHS_DTL_SK = S.DRUG_CLM_MCARE_BNF_PHS_DTL_SK
WHEN NOT MATCHED THEN
  INSERT (
    CLM_ID,
    CLM_REPRCS_SEQ_NO,
    SRC_SYS_CD,
    CRT_RUN_CYC_EXCTN_SK,
    DRUG_CLM_MCARE_BNF_PHS_DTL_SK
  )
  VALUES (
    S.CLM_ID,
    S.CLM_REPRCS_SEQ_NO,
    S.SRC_SYS_CD,
    S.CRT_RUN_CYC_EXCTN_SK,
    S.DRUG_CLM_MCARE_BNF_PHS_DTL_SK
  );
"""

execute_dml(merge_sql_db_K_DRUG_CLM_MCARE_BNF_PHS_DTL_write, jdbc_url, jdbc_props)

df_jn_left = (
    df_lnk_jn_right.alias("lnk_jn_right")
    .join(
        df_Copy_LnkLeftJn.alias("LnkLeftJn"),
        (F.col("lnk_jn_right.CLM_ID") == F.col("LnkLeftJn.CLM_ID")) &
        (F.col("lnk_jn_right.CLM_REPRCS_SEQ_NO") == F.col("LnkLeftJn.CLM_REPRCS_SEQ_NO")) &
        (F.col("lnk_jn_right.SRC_SYS_CD") == F.col("LnkLeftJn.SRC_SYS_CD")),
        how="inner"
    )
    .select(
        F.col("lnk_jn_right.CLM_ID").alias("CLM_ID"),
        F.col("lnk_jn_right.CLM_REPRCS_SEQ_NO").alias("CLM_REPRCS_SEQ_NO"),
        F.col("lnk_jn_right.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnk_jn_right.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("lnk_jn_right.DRUG_CLM_MCARE_BNF_PHS_DTL_SK").alias("DRUG_CLM_MCARE_BNF_PHS_DTL_SK"),
        F.col("LnkLeftJn.PDX_CLM_NO").alias("PDX_CLM_NO"),
        F.col("LnkLeftJn.CLM_STTUS").alias("CLM_STTUS"),
        F.col("LnkLeftJn.TC4_CNTR_NO").alias("TC4_CNTR_NO"),
        F.col("LnkLeftJn.TC4_PLN_BNF_PCKG").alias("TC4_PLN_BNF_PCKG"),
        F.col("LnkLeftJn.TC4_DRUG_COV_STS_SCHED").alias("TC4_DRUG_COV_STS_SCHED"),
        F.col("LnkLeftJn.TC4_DRUG_COV_STS_CD").alias("TC4_DRUG_COV_STS_CD"),
        F.col("LnkLeftJn.EX4_LICS_AMT").alias("EX4_LICS_AMT"),
        F.col("LnkLeftJn.EX4_MBR_AMT_TOWARD_DEDCT").alias("EX4_MBR_AMT_TOWARD_DEDCT"),
        F.col("LnkLeftJn.EX4_MBR_AMT_TOWARD_COV_GAP").alias("EX4_MBR_AMT_TOWARD_COV_GAP"),
        F.col("LnkLeftJn.EX4_MBR_AMT_TOWARD_CATO").alias("EX4_MBR_AMT_TOWARD_CATO"),
        F.col("LnkLeftJn.EX4_MBR_AMT_TOWARD_INIT_COV").alias("EX4_MBR_AMT_TOWARD_INIT_COV"),
        F.col("LnkLeftJn.EX4_RPTD_GAP_DSCNT").alias("EX4_RPTD_GAP_DSCNT"),
        F.col("LnkLeftJn.TC4_GAP_BRND_DSCNT").alias("TC4_GAP_BRND_DSCNT"),
        F.col("LnkLeftJn.EX4_EGWP_PLN_CATO_AMT").alias("EX4_EGWP_PLN_CATO_AMT"),
        F.col("LnkLeftJn.EX4_EGWP_PLN_INIT_COV_AMT").alias("EX4_EGWP_PLN_INIT_COV_AMT"),
        F.col("LnkLeftJn.EX4_EGWP_MBR_AMT_TOWARD_DEDCT").alias("EX4_EGWP_MBR_AMT_TOWARD_DEDCT"),
        F.col("LnkLeftJn.EX4_EGWP_MBR_AMT_TOWARD_GAP").alias("EX4_EGWP_MBR_AMT_TOWARD_GAP"),
        F.col("LnkLeftJn.EX4_EGWP_DRUG_SPEND_TOWARD_GAP").alias("EX4_EGWP_DRUG_SPEND_TOWARD_GAP"),
        F.col("LnkLeftJn.EX4_DUAL_AMT").alias("EX4_DUAL_AMT"),
        F.col("LnkLeftJn.EX4_PART_B_DRUG_SPEND_TOWARD_OOP").alias("EX4_PART_B_DRUG_SPEND_TOWARD_OOP"),
        F.col("LnkLeftJn.EX4_PART_B_MBR_AMT_TOWARD_OOP").alias("EX4_PART_B_MBR_AMT_TOWARD_OOP"),
        F.col("LnkLeftJn.TCD_GNRC_IN").alias("TCD_GNRC_IN"),
        F.col("LnkLeftJn.EX8_DT_SUBMT").alias("EX8_DT_SUBMT"),
        F.col("LnkLeftJn.EX4_DT_ORIG_CLM_RCVD").alias("EX4_DT_ORIG_CLM_RCVD"),
        F.col("LnkLeftJn.PDT_RSP_TOT_AMT_PD").alias("PDT_RSP_TOT_AMT_PD"),
        F.col("LnkLeftJn.EX4_PHR_PATN_PAY").alias("EX4_PHR_PATN_PAY"),
        F.col("LnkLeftJn.TC4_CPP_AMT").alias("TC4_CPP_AMT"),
        F.col("LnkLeftJn.EX4_DEDCT_LVL_DRUG_SPEND").alias("EX4_DEDCT_LVL_DRUG_SPEND"),
        F.col("LnkLeftJn.EX4_DEDCT_LVL_TROOP_AMT").alias("EX4_DEDCT_LVL_TROOP_AMT"),
        F.col("LnkLeftJn.EX4_DRUG_SPEND_AMT").alias("EX4_DRUG_SPEND_AMT"),
        F.col("LnkLeftJn.EX4_DRUG_SPEND_TOWARD_DEDCT").alias("EX4_DRUG_SPEND_TOWARD_DEDCT"),
        F.col("LnkLeftJn.EX4_DRUG_SPEND_TOWARD_INIT_COV").alias("EX4_DRUG_SPEND_TOWARD_INIT_COV"),
        F.col("LnkLeftJn.EX4_EGWP_DEDCT_LVL_TROOP_AMT").alias("EX4_EGWP_DEDCT_LVL_TROOP_AMT"),
        F.col("LnkLeftJn.EX4_EGWP_MBR_AMT_TOWARD_CATO").alias("EX4_EGWP_MBR_AMT_TOWARD_CATO"),
        F.col("LnkLeftJn.EX4_EGWP_DRUG_SPEND_TOWARD_CATO").alias("EX4_EGWP_DRUG_SPEND_TOWARD_CATO"),
        F.col("LnkLeftJn.EX4_EGWP_DRUG_SPEND_TOWARD_DEDCT").alias("EX4_EGWP_DRUG_SPEND_TOWARD_DEDCT"),
        F.col("LnkLeftJn.EX4_EGWP_MBR_AMT_TOWARD_INIT_COV").alias("EX4_EGWP_MBR_AMT_TOWARD_INIT_COV"),
        F.col("LnkLeftJn.EX4_EGWP_PLN_GAP_AMT").alias("EX4_EGWP_PLN_GAP_AMT"),
        F.col("LnkLeftJn.EX4_EGWP_AMT_TOWARD_PLN_CATO").alias("EX4_EGWP_AMT_TOWARD_PLN_CATO"),
        F.col("LnkLeftJn.EX4_EGWP_AMT_TOWARD_PLN_DEDCT").alias("EX4_EGWP_AMT_TOWARD_PLN_DEDCT"),
        F.col("LnkLeftJn.EX4_EGWP_AMT_TOWARD_PLN_GAP_AMT").alias("EX4_EGWP_AMT_TOWARD_PLN_GAP_AMT"),
        F.col("LnkLeftJn.EX4_GROST_DRUG_CST_ABOVE_AMT").alias("EX4_GROST_DRUG_CST_ABOVE_AMT"),
        F.col("LnkLeftJn.EX4_GROS_DRUG_CST_BELOW_AMT").alias("EX4_GROS_DRUG_CST_BELOW_AMT"),
        F.col("LnkLeftJn.TC4_LICS_DRUG_SPEND_DEDCT_AMT").alias("TC4_LICS_DRUG_SPEND_DEDCT_AMT"),
        F.col("LnkLeftJn.TC4_LICS_DRUG_SPEND_DEDCT_BAL").alias("TC4_LICS_DRUG_SPEND_DEDCT_BAL"),
        F.col("LnkLeftJn.EX4_NONCOV_PLN_PD_AMT").alias("EX4_NONCOV_PLN_PD_AMT"),
        F.col("LnkLeftJn.TC4_PRIOR_TROOP_AMT").alias("TC4_PRIOR_TROOP_AMT"),
        F.col("LnkLeftJn.EX4_SUBRO_AMT").alias("EX4_SUBRO_AMT"),
        F.col("LnkLeftJn.TC4_TRUE_OOP_ACCUM").alias("TC4_TRUE_OOP_ACCUM"),
        F.col("LnkLeftJn.TC4_TOT_GROS_DURG_CST_ACCUM").alias("TC4_TOT_GROS_DURG_CST_ACCUM"),
        F.col("LnkLeftJn.EX4_DRUG_SPEND_TOWARD_CATO").alias("EX4_DRUG_SPEND_TOWARD_CATO"),
        F.col("LnkLeftJn.EX4_DRUG_SPEND_TOWARD_COV_GAP").alias("EX4_DRUG_SPEND_TOWARD_COV_GAP"),
        F.col("LnkLeftJn.EX4_AMT_TOWARD_PLN_CATO").alias("EX4_AMT_TOWARD_PLN_CATO"),
        F.col("LnkLeftJn.EX4_AMT_TOWARD_PLN_DEDCT").alias("EX4_AMT_TOWARD_PLN_DEDCT"),
        F.col("LnkLeftJn.EX4_AMT_TOWARD_PLN_GAP").alias("EX4_AMT_TOWARD_PLN_GAP"),
        F.col("LnkLeftJn.EX4_BEG_BNF_PHS").alias("EX4_BEG_BNF_PHS"),
        F.col("LnkLeftJn.EX4_END_BNF_PHS").alias("EX4_END_BNF_PHS"),
        F.col("LnkLeftJn.TC4_TROOP_SCHD").alias("TC4_TROOP_SCHD")
    )
)

df_Xfm1 = df_jn_left

df_lnk_to_seq_file_load = df_Xfm1.select(
    F.col("DRUG_CLM_MCARE_BNF_PHS_DTL_SK").alias("DRUG_CLM_MCARE_BNF_PHS_DTL_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_REPRCS_SEQ_NO").alias("CLM_REPRCS_SEQ_NO"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(IDSRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit("").alias("DRUG_CLM_SK"),
    F.col("TC4_DRUG_COV_STS_CD").alias("DRUG_COV_STTUS_CD"),
    F.col("TCD_GNRC_IN").alias("MEDSPAN_GNRC_DRUG_IN"),
    F.when(
        (F.col("EX8_DT_SUBMT") != "") & (F.col("EX8_DT_SUBMT").isNotNull()),
        F.concat_ws("-", F.col("EX8_DT_SUBMT").substr(F.lit(1),F.lit(4)),
                         F.col("EX8_DT_SUBMT").substr(F.lit(5),F.lit(2)),
                         F.col("EX8_DT_SUBMT").substr(F.lit(7),F.lit(2)) )
    ).otherwise(F.col("EX8_DT_SUBMT")).alias("CLM_SUBMT_DT_SK"),
    F.when(
        (F.col("EX4_DT_ORIG_CLM_RCVD") != "") & (F.col("EX4_DT_ORIG_CLM_RCVD").isNotNull()),
        F.concat_ws("-", F.col("EX4_DT_ORIG_CLM_RCVD").substr(F.lit(1),F.lit(4)),
                         F.col("EX4_DT_ORIG_CLM_RCVD").substr(F.lit(5),F.lit(2)),
                         F.col("EX4_DT_ORIG_CLM_RCVD").substr(F.lit(7),F.lit(2)) )
    ).otherwise(F.col("EX4_DT_ORIG_CLM_RCVD")).alias("ORIG_CLM_RCVD_DT_SK"),
    F.col("PDT_RSP_TOT_AMT_PD").alias("CLM_REPRCS_PAYBL_AMT"),
    F.col("EX4_PHR_PATN_PAY").alias("CLM_REPRCS_PATN_RESP_AMT"),
    F.col("TC4_CPP_AMT").alias("COV_PLN_PD_AMT"),
    F.col("EX4_DEDCT_LVL_DRUG_SPEND").alias("DEDCT_LVL_DRUG_SPEND_THRSHLD_AMT"),
    F.col("EX4_DEDCT_LVL_TROOP_AMT").alias("DEDCT_LVL_TRUE_OOP_THRSHLD_AMT"),
    F.col("EX4_DRUG_SPEND_AMT").alias("DRUG_SPEND_AMT"),
    F.col("EX4_DRUG_SPEND_TOWARD_DEDCT").alias("DRUG_SPEND_TOWARD_DEDCT_AMT"),
    F.col("EX4_DRUG_SPEND_TOWARD_INIT_COV").alias("DRUG_SPEND_TOWARD_INIT_COV_AMT"),
    F.col("EX4_DUAL_AMT").alias("DUAL_AMT"),
    F.col("EX4_EGWP_DEDCT_LVL_TROOP_AMT").alias("EGWP_DEDCT_LVL_TROOP_THRSHLD_AMT"),
    F.col("EX4_EGWP_DRUG_SPEND_TOWARD_CATO").alias("EGWP_DRUG_SPEND_TOWARD_CATO_AMT"),
    F.col("EX4_EGWP_DRUG_SPEND_TOWARD_DEDCT").alias("EGWP_DRUG_SPEND_TOWARD_DEDCT_AMT"),
    F.col("EX4_EGWP_DRUG_SPEND_TOWARD_GAP").alias("EGWP_DRUG_SPEND_TOWARD_GAP_AMT"),
    F.col("EX4_EGWP_MBR_AMT_TOWARD_INIT_COV").alias("EGWP_DRUG_SPEND_TOWARD_INIT_COV_AMT"),
    F.col("EX4_EGWP_MBR_AMT_TOWARD_CATO").alias("EGWP_MBR_TOWARD_CATO_AMT"),
    F.col("EX4_EGWP_MBR_AMT_TOWARD_DEDCT").alias("EGWP_MBR_TOWARD_DEDCT_AMT"),
    F.col("EX4_EGWP_MBR_AMT_TOWARD_GAP").alias("EGWP_MBR_TOWARD_GAP_AMT"),
    F.col("EX4_EGWP_PLN_INIT_COV_AMT").alias("EGWP_MBR_TOWARD_INIT_COV_AMT"),
    F.col("EX4_EGWP_PLN_CATO_AMT").alias("EGWP_PLN_CATO_AMT"),
    F.col("EX4_EGWP_PLN_GAP_AMT").alias("EGWP_PLN_GAP_THRSHLD_AMT"),
    F.col("EX4_EGWP_AMT_TOWARD_PLN_CATO").alias("EGWP_TOWARD_PLN_CATO_AMT"),
    F.col("EX4_EGWP_AMT_TOWARD_PLN_DEDCT").alias("EGWP_TOWARD_PLN_DEDCT_AMT"),
    F.col("EX4_EGWP_AMT_TOWARD_PLN_GAP_AMT").alias("EGWP_TOWARD_PLN_GAP_AMT"),
    F.col("TC4_GAP_BRND_DSCNT").alias("GAP_BRND_DSCNT_AMT"),
    F.col("EX4_GROST_DRUG_CST_ABOVE_AMT").alias("GROS_DRUG_CST_AFTR_AMT"),
    F.col("EX4_GROS_DRUG_CST_BELOW_AMT").alias("GROS_DRUG_CST_BFR_AMT"),
    F.col("TC4_LICS_DRUG_SPEND_DEDCT_AMT").alias("LICS_DRUG_SPEND_DEDCT_AMT"),
    F.col("TC4_LICS_DRUG_SPEND_DEDCT_BAL").alias("LICS_DRUG_SPEND_DEDCT_BAL_AMT"),
    F.col("EX4_LICS_AMT").alias("LOW_INCM_CST_SHR_SBSDY_AMT"),
    F.col("EX4_PART_B_DRUG_SPEND_TOWARD_OOP").alias("MCAREB_DRUG_SPEND_TOWARD_OOP_AMT"),
    F.col("EX4_PART_B_MBR_AMT_TOWARD_OOP").alias("MCAREB_MBR_TOWARD_OOP_AMT"),
    F.col("EX4_MBR_AMT_TOWARD_CATO").alias("MBR_TOWARD_CATO_AMT"),
    F.col("EX4_MBR_AMT_TOWARD_DEDCT").alias("MBR_TOWARD_DEDCT_AMT"),
    F.col("EX4_MBR_AMT_TOWARD_COV_GAP").alias("MBR_TOWARD_GAP_AMT"),
    F.col("EX4_MBR_AMT_TOWARD_INIT_COV").alias("MBR_TOWARD_INIT_COV_AMT"),
    F.col("EX4_NONCOV_PLN_PD_AMT").alias("NCOV_PLN_PD_AMT"),
    F.col("TC4_PRIOR_TROOP_AMT").alias("PRIOR_TRUE_OOP_AMT"),
    F.col("EX4_RPTD_GAP_DSCNT").alias("RPTD_GAP_DSCNT_AMT"),
    F.col("EX4_SUBRO_AMT").alias("SUBRO_AMT"),
    F.col("TC4_TRUE_OOP_ACCUM").alias("TRUE_OOP_ACCUM_AMT"),
    F.col("TC4_TOT_GROS_DURG_CST_ACCUM").alias("TOT_GROS_DRUG_CST_ACCUM_AMT"),
    F.col("EX4_DRUG_SPEND_TOWARD_CATO").alias("TOWARD_CATO_PHS_DRUG_SPEND_AMT"),
    F.col("EX4_DRUG_SPEND_TOWARD_COV_GAP").alias("TOWARD_GAP_PHS_DRUG_SPEND_AMT"),
    F.col("EX4_AMT_TOWARD_PLN_CATO").alias("TOWARD_PLN_CATO_AMT"),
    F.col("EX4_AMT_TOWARD_PLN_DEDCT").alias("TOWARD_PLN_DEDCT_AMT"),
    F.col("EX4_AMT_TOWARD_PLN_GAP").alias("TOWARD_PLN_GAP_AMT"),
    F.col("EX4_BEG_BNF_PHS").alias("BEG_BNF_PHS_ID"),
    F.col("EX4_END_BNF_PHS").alias("END_BNF_PHS_ID"),
    F.col("TC4_CNTR_NO").alias("MCARE_CNTR_ID"),
    F.col("TC4_PLN_BNF_PCKG").alias("PLN_BNF_PCKG_ID"),
    F.col("TC4_TROOP_SCHD").alias("TRUE_OOP_SCHD_ID")
)

# For the final write, all columns with "char"/"varchar" in the job metadata must be rpad() to the specified length.
# Below we apply rpad for the columns that are identified as char with a known length from the final stage definition:
# (Lengths derived from the DataStage output columns for "Seq_DRUG_CLM_MCARE_BNF_PHS_DTL".)
# Some columns do not have explicit length in the final stage—rpad only if known from previous metadata:
df_seq_drug_clm_mcare_bnf_phs_dtl = df_lnk_to_seq_file_load
df_seq_drug_clm_mcare_bnf_phs_dtl_out = df_seq_drug_clm_mcare_bnf_phs_dtl
# (No explicit lengths provided in the final stage excerpt for each column. If we had them, we would use rpad. 
#  Here we proceed without further lengths unless specified.)

write_files(
    df_seq_drug_clm_mcare_bnf_phs_dtl_out,
    f"{adls_path}/key/DRUG_CLM_MCARE_BNF_PHS_DTL.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_lnk_BDrugClmMcareBnfPhsDtl = df_Xfm1.select(
    F.col("CLM_ID"),
    F.col("CLM_REPRCS_SEQ_NO"),
    F.col("SRC_SYS_CD")
)

# Again, if any column lengths were specifically declared as char in final, we would apply rpad. 
df_seq_b_drug_clm_mcare_bnf_phs_dtl = df_lnk_BDrugClmMcareBnfPhsDtl

write_files(
    df_seq_b_drug_clm_mcare_bnf_phs_dtl,
    f"{adls_path}/load/B_DRUG_CLM_MCARE_BNF_PHS_DTL.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)