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
# MAGIC DESCRIPTION:      CVS Drug Claim Landing Extract. Looks up against the MBR_ENR table to get the right member unique key for a Member for a Claim
# MAGIC                                
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                Date                 Project/Altiris #                                                       Change Description                                                                                          Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------                                                       -----------------------------------------------------------------------                                                   --------------------------------       -------------------------------   ----------------------------      
# MAGIC Kaushik Kapoor                                5828                                                                        Initial Programming                                                                                             IntegrateDev2                 Kalyan Neelam          2018-09-28
# MAGIC Ramu Avula            2020-08-28        6264-PBM Phase II - Government Programs           Added SUBMT_PROD_ID_QLFR,
# MAGIC                                                                                                                                          CNTNGNT_THER_FLAG,CNTNGNT_THER_SCHD,                                      IntegrateDev5                 Kalyan Neelam          2020-12-10
# MAGIC                                                                                                                                          CLNT_PATN_PAY_ATRBD_PROD_AMT,
# MAGIC                                                                                                                                          CLNT_PATN_PAY_ATRBD_SLS_TAX_AMT,CLNT_PATN_PAY_ATRBD_SLS_TAX_AMT
# MAGIC                                                                                                                                          CLNT_PATN_PAY_ATRBD_PRCSR_FEE_AMT,CLNT_PATN_PAY_ATRBD_NTWK_AMT,LOB_IN  fields

# MAGIC CVS Claim Landing Extract
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType,
    LongType,
    ShortType,
    DateType,
    BooleanType
)
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
RunID = get_widget_value('RunID','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunDate = get_widget_value('RunDate','')

# ----------------------------------------------------------------------------
# STAGE: P_PBM_GRP_XREF (DB2Connector) => scenario: read from DB, direct to next
# ----------------------------------------------------------------------------
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query_P_PBM_GRP_XREF = (
    f"SELECT DISTINCT PBM_GRP_ID, GRP_ID "
    f"FROM {IDSOwner}.P_PBM_GRP_XREF XREF "
    f"WHERE UPPER(XREF.SRC_SYS_CD) = '{SrcSysCd}' "
    f"AND '{RunDate}' BETWEEN CONVERT(DATE, XREF.EFF_DT) AND CONVERT(DATE, XREF.TERM_DT)"
)
df_P_PBM_GRP_XREF = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_P_PBM_GRP_XREF)
    .load()
)

# ----------------------------------------------------------------------------
# STAGE: hf_CVS_clm_land_grpxref (CHashedFileStage) => scenario A
# Deduplicate on key column PBM_GRP_ID
# ----------------------------------------------------------------------------
df_Grp_Xref = df_P_PBM_GRP_XREF.dropDuplicates(["PBM_GRP_ID"])

# ----------------------------------------------------------------------------
# STAGE: CVSPreProcExtr (CSeqFileStage) => read from "verified/#SrcSysCd#_Clm_PreProc.dat.#RunID#"
# ----------------------------------------------------------------------------
schema_CVSPreProcExtr = StructType([
    StructField("CLAIM_ID", StringType(), nullable=False),
    StructField("TRANS_ID", StringType(), nullable=True),
    StructField("CLM_TYP", StringType(), nullable=True),
    StructField("MBR_UNIQ_KEY", IntegerType(), nullable=False),
    StructField("RCRD_TYP", StringType(), nullable=True),
    StructField("RCRD_IN", StringType(), nullable=True),
    StructField("FINL_PLN_QLFR", StringType(), nullable=True),
    StructField("CARDHLDR_ID", StringType(), nullable=True),
    StructField("CARDHLDR_LAST_NM", StringType(), nullable=True),
    StructField("CARDHLDR_FIRST_NM", StringType(), nullable=True),
    StructField("CARDHLDR_MIDINIT", StringType(), nullable=True),
    StructField("CARDHLDR_DOB", StringType(), nullable=True),
    StructField("PATN_LAST_NM", StringType(), nullable=True),
    StructField("PATN_FIRST_NM", StringType(), nullable=True),
    StructField("PATN_MIDINIT", StringType(), nullable=True),
    StructField("PATN_DOB", StringType(), nullable=True),
    StructField("PATN_GNDR_CD", StringType(), nullable=True),
    StructField("PATN_RELSHP_CD", StringType(), nullable=True),
    StructField("PATN_AGE", StringType(), nullable=True),
    StructField("SRC_GRP_ID", StringType(), nullable=True),
    StructField("CAR_NO", StringType(), nullable=True),
    StructField("OTHR_COV_CD", StringType(), nullable=True),
    StructField("SVC_PROV_ID_QLFR", StringType(), nullable=True),
    StructField("SVC_PROV_ID", StringType(), nullable=True),
    StructField("PDX_NM", StringType(), nullable=True),
    StructField("PDX_ADDR_LN_1", StringType(), nullable=True),
    StructField("PDX_ADDR_LN_2", StringType(), nullable=True),
    StructField("PDX_CITY", StringType(), nullable=True),
    StructField("PDX_ST", StringType(), nullable=True),
    StructField("PDX_ZIP_CD", StringType(), nullable=True),
    StructField("PDX_TEL_NO", StringType(), nullable=True),
    StructField("PDX_DISPENSER_TYP", StringType(), nullable=True),
    StructField("NTWK_RMBRMT_ID", StringType(), nullable=True),
    StructField("PRSCRBR_ID_QLFR", StringType(), nullable=True),
    StructField("PRSCRBR_ID", StringType(), nullable=True),
    StructField("PRSCRBR_LAST_NM", StringType(), nullable=True),
    StructField("PRSCRBR_FIRST_NM", StringType(), nullable=True),
    StructField("RCRD_STTUS_CD", StringType(), nullable=True),
    StructField("CLM_MEDIA_TYP", StringType(), nullable=True),
    StructField("PROD_ID_QLFR", StringType(), nullable=True),
    StructField("PROD_ID", StringType(), nullable=True),
    StructField("DT_OF_SVC", StringType(), nullable=True),
    StructField("ADJDCT_DT", StringType(), nullable=True),
    StructField("CYC_END_DT", StringType(), nullable=True),
    StructField("D_0_RX_NO", StringType(), nullable=True),
    StructField("RX_NO_QLFR", StringType(), nullable=True),
    StructField("QTY_DISPNS", StringType(), nullable=True),
    StructField("FILL_NO", StringType(), nullable=True),
    StructField("DAYS_SUPL", StringType(), nullable=True),
    StructField("DT_RX_WRTN", StringType(), nullable=True),
    StructField("DISPNS_AS_WRTN_PROD_SEL_CD", StringType(), nullable=True),
    StructField("NO_OF_RFLS_AUTH", StringType(), nullable=True),
    StructField("UNIT_OF_MESR", StringType(), nullable=True),
    StructField("ORIG_QTY", StringType(), nullable=True),
    StructField("ORIG_DAY_SUPL", StringType(), nullable=True),
    StructField("CMPND_CD", StringType(), nullable=True),
    StructField("DIAG_CD_QLFR", StringType(), nullable=True),
    StructField("DIAG_CD", StringType(), nullable=True),
    StructField("REJ_CD_1", StringType(), nullable=True),
    StructField("REJ_CD_2", StringType(), nullable=True),
    StructField("REJ_CD_3", StringType(), nullable=True),
    StructField("DB_IN", StringType(), nullable=True),
    StructField("PROD_NM", StringType(), nullable=True),
    StructField("GNRC_NM", StringType(), nullable=True),
    StructField("PROD_STRG", StringType(), nullable=True),
    StructField("DOSE_FORM_CD", StringType(), nullable=True),
    StructField("DRUG_TYP", StringType(), nullable=True),
    StructField("MNTN_DRUG_IN", StringType(), nullable=True),
    StructField("DRUG_CAT_CD", StringType(), nullable=True),
    StructField("DENIAL_CLRFCTN_CD", StringType(), nullable=True),
    StructField("GCN_NO", StringType(), nullable=True),
    StructField("GNRC_PROD_ID", StringType(), nullable=True),
    StructField("MED_B_OR_D_IN", StringType(), nullable=True),
    StructField("TCC", StringType(), nullable=True),
    StructField("FRMLRY_STTUS", StringType(), nullable=True),
    StructField("INGR_CST_PD", StringType(), nullable=True),
    StructField("DISPNS_FEE_PD", StringType(), nullable=True),
    StructField("TOT_AMT_PD_ALL_SRCS", StringType(), nullable=True),
    StructField("AMT_ATRBD_TO_SLS_TAX", StringType(), nullable=True),
    StructField("PATN_PAY_AMT", StringType(), nullable=True),
    StructField("AMT_OF_COPAY", StringType(), nullable=True),
    StructField("AMT_OF_COINS", StringType(), nullable=True),
    StructField("AMT_ATRBD_TO_PROD_SEL", StringType(), nullable=True),
    StructField("AMT_APLD_TO_PRDC_DEDCT", StringType(), nullable=True),
    StructField("MAC_REDC_IN", StringType(), nullable=True),
    StructField("CLNT_PRICE_BSS_OF_CST", StringType(), nullable=True),
    StructField("GNRC_IN", StringType(), nullable=True),
    StructField("OOP_APPLY_AMT", StringType(), nullable=True),
    StructField("AWP_TYP_IN", StringType(), nullable=True),
    StructField("AVG_WHLSL_UNIT_PRICE", StringType(), nullable=True),
    StructField("AVG_WHLSL_UNIT_FULL_PRICE", StringType(), nullable=True),
    StructField("INGR_CST_SUBMT", StringType(), nullable=True),
    StructField("USL_AND_CUST_CHRG", StringType(), nullable=True),
    StructField("FLAT_SLS_TAX_AMT_PD", StringType(), nullable=True),
    StructField("PCT_SLS_TAX_AMT_PD", StringType(), nullable=True),
    StructField("NET_AMT_PD", StringType(), nullable=True),
    StructField("BSS_OF_RMBRMT_DTRM", StringType(), nullable=True),
    StructField("ACCUM_DEDCT_AMT", StringType(), nullable=True),
    StructField("AMT_EXCEEDING_PRDC_BNF_MAX", StringType(), nullable=True),
    StructField("BSS_OF_COPAY_CALC", StringType(), nullable=True),
    StructField("ADJ_ISSUE_ID", StringType(), nullable=True),
    StructField("PRCSR_DEFN_PRAUTH_CRTF_CD", StringType(), nullable=True),
    StructField("ADJ_RSN_CD", StringType(), nullable=True),
    StructField("ELIG_COB_IN", StringType(), nullable=True),
    StructField("COB_PRI_PAYER_AMT_PD", StringType(), nullable=True),
    StructField("OPAR_AMT", StringType(), nullable=True),
    StructField("COB_PRI_PAYER_COPAY", StringType(), nullable=True),
    StructField("ACCT_ID", StringType(), nullable=True),
    StructField("CARE_FCLTY", StringType(), nullable=True),
    StructField("SPEC_RX_CLM", StringType(), nullable=True),
    StructField("ALT_ID", StringType(), nullable=True),
    StructField("VCCN_ADM_FEE_PD", StringType(), nullable=True),
    StructField("DRUG_ADMIN_FEE_TYP_CD", StringType(), nullable=True),
    StructField("MNTN_CHOICE_IN", StringType(), nullable=True),
    StructField("APLD_HRA_AMT", StringType(), nullable=True),
    StructField("CB5_QLFR_1", StringType(), nullable=True),
    StructField("CB5_OTH_AMT_1", StringType(), nullable=True),
    StructField("CB5_QLFR_2", StringType(), nullable=True),
    StructField("CB5_OTH_AMT_2", StringType(), nullable=True),
    StructField("CB5_QLFR_3", StringType(), nullable=True),
    StructField("CB5_OTH_AMT_3", StringType(), nullable=True),
    StructField("PD6_CLNT_TOT_OTHR_AMT", StringType(), nullable=True),
    StructField("PD6_BUY_TOT_OTHR_AMT", StringType(), nullable=True),
    StructField("GRP_ID", StringType(), nullable=False)
])

df_CVSPreProcExtr = (
    spark.read.format("csv")
    .option("header", "false")
    .option("quote", "\"")
    .option("delimiter", ",")
    .schema(schema_CVSPreProcExtr)
    .load(f"{adls_path}/verified/{SrcSysCd}_Clm_PreProc.dat.{RunID}")
)

# ----------------------------------------------------------------------------
# STAGE: KeyCol (CTransformerStage) => output link "Dedup"
# ----------------------------------------------------------------------------
df_KeyCol = df_CVSPreProcExtr.select(
    F.col("CLAIM_ID").alias("CLAIM_ID"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("DT_OF_SVC").alias("DT_OF_SVC"),
    F.col("TRANS_ID").alias("TRANS_ID"),
    F.lit("CVS").alias("SRC_SYS_CD"),
    F.col("CLM_TYP").alias("CLM_TYP"),
    F.col("RCRD_TYP").alias("RCRD_TYP"),
    F.col("RCRD_IN").alias("RCRD_IN"),
    F.col("FINL_PLN_QLFR").alias("FINL_PLN_QLFR"),
    F.col("CARDHLDR_ID").alias("CARDHLDR_ID"),
    F.col("CARDHLDR_LAST_NM").alias("CARDHLDR_LAST_NM"),
    F.col("CARDHLDR_FIRST_NM").alias("CARDHLDR_FIRST_NM"),
    F.col("CARDHLDR_MIDINIT").alias("CARDHLDR_MIDINIT"),
    F.col("CARDHLDR_DOB").alias("CARDHLDR_DOB"),
    F.col("PATN_LAST_NM").alias("PATN_LAST_NM"),
    F.col("PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("PATN_MIDINIT").alias("PATN_MIDINIT"),
    F.col("PATN_DOB").alias("PATN_DOB"),
    F.col("PATN_GNDR_CD").alias("PATN_GNDR_CD"),
    F.col("PATN_RELSHP_CD").alias("PATN_RELSHP_CD"),
    F.col("PATN_AGE").alias("PATN_AGE"),
    F.col("SRC_GRP_ID").alias("SRC_GRP_ID"),
    F.col("CAR_NO").alias("CAR_NO"),
    F.col("OTHR_COV_CD").alias("OTHR_COV_CD"),
    F.col("SVC_PROV_ID_QLFR").alias("SVC_PROV_ID_QLFR"),
    F.col("SVC_PROV_ID").alias("SVC_PROV_ID"),
    F.col("PDX_NM").alias("PDX_NM"),
    F.col("PDX_ADDR_LN_1").alias("PDX_ADDR_LN_1"),
    F.col("PDX_ADDR_LN_2").alias("PDX_ADDR_LN_2"),
    F.col("PDX_CITY").alias("PDX_CITY"),
    F.col("PDX_ST").alias("PDX_ST"),
    F.col("PDX_ZIP_CD").alias("PDX_ZIP_CD"),
    F.col("PDX_TEL_NO").alias("PDX_TEL_NO"),
    F.col("PDX_DISPENSER_TYP").alias("PDX_DISPENSER_TYP"),
    F.col("NTWK_RMBRMT_ID").alias("NTWK_RMBRMT_ID"),
    F.col("PRSCRBR_ID_QLFR").alias("PRSCRBR_ID_QLFR"),
    F.col("PRSCRBR_ID").alias("PRSCRBR_ID"),
    F.col("PRSCRBR_LAST_NM").alias("PRSCRBR_LAST_NM"),
    F.col("PRSCRBR_FIRST_NM").alias("PRSCRBR_FIRST_NM"),
    F.col("RCRD_STTUS_CD").alias("RCRD_STTUS_CD"),
    F.col("CLM_MEDIA_TYP").alias("CLM_MEDIA_TYP"),
    F.col("PROD_ID_QLFR").alias("PROD_ID_QLFR"),
    F.col("PROD_ID").alias("PROD_ID"),
    F.col("ADJDCT_DT").alias("ADJDCT_DT"),
    F.col("CYC_END_DT").alias("CYC_END_DT"),
    F.col("D_0_RX_NO").alias("D_0_RX_NO"),
    F.col("RX_NO_QLFR").alias("RX_NO_QLFR"),
    F.col("QTY_DISPNS").alias("QTY_DISPNS"),
    F.col("FILL_NO").alias("FILL_NO"),
    F.col("DAYS_SUPL").alias("DAYS_SUPL"),
    F.col("DT_RX_WRTN").alias("DT_RX_WRTN"),
    F.col("DISPNS_AS_WRTN_PROD_SEL_CD").alias("DISPNS_AS_WRTN_PROD_SEL_CD"),
    F.col("NO_OF_RFLS_AUTH").alias("NO_OF_RFLS_AUTH"),
    F.col("UNIT_OF_MESR").alias("UNIT_OF_MESR"),
    F.col("ORIG_QTY").alias("ORIG_QTY"),
    F.col("ORIG_DAY_SUPL").alias("ORIG_DAY_SUPL"),
    F.col("CMPND_CD").alias("CMPND_CD"),
    F.col("DIAG_CD_QLFR").alias("DIAG_CD_QLFR"),
    F.col("DIAG_CD").alias("DIAG_CD"),
    F.col("REJ_CD_1").alias("REJ_CD_1"),
    F.col("REJ_CD_2").alias("REJ_CD_2"),
    F.col("REJ_CD_3").alias("REJ_CD_3"),
    F.col("DB_IN").alias("DB_IN"),
    F.col("PROD_NM").alias("PROD_NM"),
    F.col("GNRC_NM").alias("GNRC_NM"),
    F.col("PROD_STRG").alias("PROD_STRG"),
    F.col("DOSE_FORM_CD").alias("DOSE_FORM_CD"),
    F.col("DRUG_TYP").alias("DRUG_TYP"),
    F.col("MNTN_DRUG_IN").alias("MNTN_DRUG_IN"),
    F.col("DRUG_CAT_CD").alias("DRUG_CAT_CD"),
    F.col("DENIAL_CLRFCTN_CD").alias("DENIAL_CLRFCTN_CD"),
    F.col("GCN_NO").alias("GCN_NO"),
    F.col("GNRC_PROD_ID").alias("GNRC_PROD_ID"),
    F.col("MED_B_OR_D_IN").alias("MED_B_OR_D_IN"),
    F.col("TCC").alias("TCC"),
    F.col("FRMLRY_STTUS").alias("FRMLRY_STTUS"),
    F.col("INGR_CST_PD").alias("INGR_CST_PD"),
    F.col("DISPNS_FEE_PD").alias("DISPNS_FEE_PD"),
    F.col("TOT_AMT_PD_ALL_SRCS").alias("TOT_AMT_PD_ALL_SRCS"),
    F.col("AMT_ATRBD_TO_SLS_TAX").alias("AMT_ATRBD_TO_SLS_TAX"),
    F.col("PATN_PAY_AMT").alias("PATN_PAY_AMT"),
    F.col("AMT_OF_COPAY").alias("AMT_OF_COPAY"),
    F.col("AMT_OF_COINS").alias("AMT_OF_COINS"),
    F.col("AMT_ATRBD_TO_PROD_SEL").alias("AMT_ATRBD_TO_PROD_SEL"),
    F.col("AMT_APLD_TO_PRDC_DEDCT").alias("AMT_APLD_TO_PRDC_DEDCT"),
    F.col("MAC_REDC_IN").alias("MAC_REDC_IN"),
    F.col("CLNT_PRICE_BSS_OF_CST").alias("CLNT_PRICE_BSS_OF_CST"),
    F.col("GNRC_IN").alias("GNRC_IN"),
    F.col("OOP_APPLY_AMT").alias("OOP_APPLY_AMT"),
    F.col("AWP_TYP_IN").alias("AWP_TYP_IN"),
    F.col("AVG_WHLSL_UNIT_PRICE").alias("AVG_WHLSL_UNIT_PRICE"),
    F.col("AVG_WHLSL_UNIT_FULL_PRICE").alias("AVG_WHLSL_UNIT_FULL_PRICE"),
    F.col("INGR_CST_SUBMT").alias("INGR_CST_SUBMT"),
    F.col("USL_AND_CUST_CHRG").alias("USL_AND_CUST_CHRG"),
    F.col("FLAT_SLS_TAX_AMT_PD").alias("FLAT_SLS_TAX_AMT_PD"),
    F.col("PCT_SLS_TAX_AMT_PD").alias("PCT_SLS_TAX_AMT_PD"),
    F.col("NET_AMT_PD").alias("NET_AMT_PD"),
    F.col("BSS_OF_RMBRMT_DTRM").alias("BSS_OF_RMBRMT_DTRM"),
    F.col("ACCUM_DEDCT_AMT").alias("ACCUM_DEDCT_AMT"),
    F.col("AMT_EXCEEDING_PRDC_BNF_MAX").alias("AMT_EXCEEDING_PRDC_BNF_MAX"),
    F.col("BSS_OF_COPAY_CALC").alias("BSS_OF_COPAY_CALC"),
    F.col("ADJ_ISSUE_ID").alias("ADJ_ISSUE_ID"),
    F.col("PRCSR_DEFN_PRAUTH_CRTF_CD").alias("PRCSR_DEFN_PRAUTH_CRTF_CD"),
    F.col("ADJ_RSN_CD").alias("ADJ_RSN_CD"),
    F.col("ELIG_COB_IN").alias("ELIG_COB_IN"),
    F.col("COB_PRI_PAYER_AMT_PD").alias("COB_PRI_PAYER_AMT_PD"),
    F.col("OPAR_AMT").alias("OPAR_AMT"),
    F.col("COB_PRI_PAYER_COPAY").alias("COB_PRI_PAYER_COPAY"),
    F.col("ACCT_ID").alias("ACCT_ID"),
    F.col("CARE_FCLTY").alias("CARE_FCLTY"),
    F.col("SPEC_RX_CLM").alias("SPEC_RX_CLM"),
    F.col("ALT_ID").alias("ALT_ID"),
    F.col("VCCN_ADM_FEE_PD").alias("VCCN_ADM_FEE_PD"),
    F.col("DRUG_ADMIN_FEE_TYP_CD").alias("DRUG_ADMIN_FEE_TYP_CD"),
    F.col("MNTN_CHOICE_IN").alias("MNTN_CHOICE_IN"),
    F.col("APLD_HRA_AMT").alias("APLD_HRA_AMT"),
    F.col("CB5_QLFR_1").alias("CB5_QLFR_1"),
    F.col("CB5_OTH_AMT_1").alias("CB5_OTH_AMT_1"),
    F.col("CB5_QLFR_2").alias("CB5_QLFR_2"),
    F.col("CB5_OTH_AMT_2").alias("CB5_OTH_AMT_2"),
    F.col("CB5_QLFR_3").alias("CB5_QLFR_3"),
    F.col("CB5_OTH_AMT_3").alias("CB5_OTH_AMT_3"),
    F.col("PD6_CLNT_TOT_OTHR_AMT").alias("PD6_CLNT_TOT_OTHR_AMT"),
    F.col("PD6_BUY_TOT_OTHR_AMT").alias("PD6_BUY_TOT_OTHR_AMT"),
    F.col("GRP_ID").alias("GRP_ID")
)

# ----------------------------------------------------------------------------
# STAGE: hf_CVS_clm_land_filededupe => scenario A, deduplicate by keys ["CLAIM_ID","MBR_UNIQ_KEY","DT_OF_SVC"]
# ----------------------------------------------------------------------------
df_CVS = df_KeyCol.dropDuplicates(["CLAIM_ID","MBR_UNIQ_KEY","DT_OF_SVC"])

# ----------------------------------------------------------------------------
# STAGE: IDS_MBR (DB2Connector)
# ----------------------------------------------------------------------------
extract_query_IDS_MBR = (
    f"SELECT CLM_ID, MBR_UNIQ_KEY, [Order] "
    f"FROM ("
    f"(SELECT DRUG.CLM_ID, ENR.MBR_UNIQ_KEY, MAX(ENR.TERM_DT_SK) TERM_DT_SK, MAX(ENR.EFF_DT_SK) EFF_DT_SK, 4 as [Order] "
    f"  FROM {IDSOwner}.MBR_ENR ENR, {IDSOwner}.CD_MPPNG CD1, {IDSOwner}.W_DRUG_ENR_MATCH DRUG "
    f"  WHERE ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = CD1.CD_MPPNG_SK "
    f"    AND CD1.TRGT_CD = 'MED' AND ENR.ELIG_IN = 'Y' "
    f"    AND ENR.MBR_UNIQ_KEY = DRUG.MBR_UNIQ_KEY "
    f"    AND ENR.EFF_DT_SK <= DRUG.FILL_DT_SK "
    f"    AND ENR.TERM_DT_SK >= DRUG.FILL_DT_SK "
    f"  GROUP BY DRUG.CLM_ID, ENR.MBR_UNIQ_KEY) "
    f" UNION "
    f"(SELECT DRUG.CLM_ID, ENR.MBR_UNIQ_KEY, MAX(ENR.TERM_DT_SK) TERM_DT_SK, MAX(ENR.EFF_DT_SK) EFF_DT_SK, 3 as [Order] "
    f"  FROM {IDSOwner}.MBR_ENR ENR, {IDSOwner}.CD_MPPNG CD1, {IDSOwner}.W_DRUG_ENR_MATCH DRUG "
    f"  WHERE ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = CD1.CD_MPPNG_SK "
    f"    AND CD1.TRGT_CD = 'MED' AND ENR.ELIG_IN = 'Y' "
    f"    AND ENR.MBR_UNIQ_KEY = DRUG.MBR_UNIQ_KEY "
    f"  GROUP BY DRUG.CLM_ID, ENR.MBR_UNIQ_KEY) "
    f" UNION "
    f"(SELECT DRUG.CLM_ID, ENR.MBR_UNIQ_KEY, MAX(ENR.TERM_DT_SK) TERM_DT_SK, MAX(ENR.EFF_DT_SK) EFF_DT_SK, 2 as [Order] "
    f"  FROM {IDSOwner}.MBR_ENR ENR, {IDSOwner}.CD_MPPNG CD1, {IDSOwner}.W_DRUG_ENR_MATCH DRUG "
    f"  WHERE ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = CD1.CD_MPPNG_SK "
    f"    AND CD1.TRGT_CD = 'MED' AND ENR.ELIG_IN = 'N' "
    f"    AND ENR.MBR_UNIQ_KEY = DRUG.MBR_UNIQ_KEY "
    f"    AND ENR.EFF_DT_SK <= DRUG.FILL_DT_SK "
    f"    AND ENR.TERM_DT_SK >= DRUG.FILL_DT_SK "
    f"  GROUP BY DRUG.CLM_ID, ENR.MBR_UNIQ_KEY) "
    f" UNION "
    f"(SELECT DRUG.CLM_ID, ENR.MBR_UNIQ_KEY, MAX(ENR.TERM_DT_SK) TERM_DT_SK, MAX(ENR.EFF_DT_SK) EFF_DT_SK, 1 as [Order] "
    f"  FROM {IDSOwner}.MBR_ENR ENR, {IDSOwner}.CD_MPPNG CD1, {IDSOwner}.W_DRUG_ENR_MATCH DRUG "
    f"  WHERE ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = CD1.CD_MPPNG_SK "
    f"    AND CD1.TRGT_CD = 'MED' "
    f"    AND ENR.MBR_UNIQ_KEY = DRUG.MBR_UNIQ_KEY "
    f"  GROUP BY DRUG.CLM_ID, ENR.MBR_UNIQ_KEY) "
    f") "
    f"ORDER BY CLM_ID, [Order], TERM_DT_SK, EFF_DT_SK, MBR_UNIQ_KEY"
)
df_IDS_MBR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_IDS_MBR)
    .load()
)

# ----------------------------------------------------------------------------
# STAGE: hf_CVS_clm_land_clmid_dedupe => scenario A
# PK = ["CLM_ID"]
# ----------------------------------------------------------------------------
df_Clm_Dedup = df_IDS_MBR.dropDuplicates(["CLM_ID"])

# ----------------------------------------------------------------------------
# STAGE: ReKey (CTransformerStage)
# ----------------------------------------------------------------------------
df_ReKey_Clm_Mbr = df_Clm_Dedup.select(
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY")
)
df_ReKey_ClmIdforErr = df_Clm_Dedup.select(
    F.col("CLM_ID").alias("CLM_ID")
)

# ----------------------------------------------------------------------------
# STAGE: hf_CVS_clm_land_clmid_lkup => scenario A
# PK = ["CLM_ID","MBR_UNIQ_KEY"]
# ----------------------------------------------------------------------------
df_Clm_Mbr = df_ReKey_Clm_Mbr.dropDuplicates(["CLM_ID","MBR_UNIQ_KEY"])

# ----------------------------------------------------------------------------
# STAGE: hf_CVS_clm_land_clmidlkup_err => scenario A
# PK = ["CLM_ID"]
# ----------------------------------------------------------------------------
df_ClmIdforErr = df_ReKey_ClmIdforErr.dropDuplicates(["CLM_ID"])

# ----------------------------------------------------------------------------
# Now we have df_CVS (primary data), df_Clm_Mbr (lookup), df_ClmIdforErr (lookup), df_Grp_Xref (lookup)
# STAGE: MemberLkp (CTransformerStage)
# ----------------------------------------------------------------------------
# Perform left joins:
df_joined_MemberLkp = (
    df_CVS.alias("CVS")
    .join(df_Clm_Mbr.alias("Clm_Mbr_Lkp"),
          [F.col("CVS.CLAIM_ID")==F.col("Clm_Mbr_Lkp.CLM_ID"), 
           F.col("CVS.MBR_UNIQ_KEY")==F.col("Clm_Mbr_Lkp.MBR_UNIQ_KEY")],
          how="left")
    .join(df_ClmIdforErr.alias("ErrProc_Lkp"),
          F.col("CVS.CLAIM_ID")==F.col("ErrProc_Lkp.CLM_ID"),
          how="left")
    .join(df_Grp_Xref.alias("Grp_Xref"),
          trim(F.col("CVS.SRC_GRP_ID"))==F.col("Grp_Xref.PBM_GRP_ID"),
          how="left")
)

# Stage variable: svXrefGrp = If IsNull(Grp_Xref.GRP_ID) or length=0 => 'UNK' else Trim(Grp_Xref.GRP_ID)
df_joined_MemberLkp = df_joined_MemberLkp.withColumn(
    "svXrefGrp",
    F.when(
        F.col("Grp_Xref.GRP_ID").isNull() | (F.length(trim(F.col("Grp_Xref.GRP_ID")))==0),
        F.lit("UNK")
    ).otherwise(trim(F.col("Grp_Xref.GRP_ID")))
)

# Split outputs: "Sort" link = rows where ErrProc_Lkp.CLM_ID isNotNull()
# "Reject" link = rows where ErrProc_Lkp.CLM_ID isNull()
df_MemberLkp_Sort = df_joined_MemberLkp.filter(F.col("ErrProc_Lkp.CLM_ID").isNotNull())
df_MemberLkp_Reject = df_joined_MemberLkp.filter(F.col("ErrProc_Lkp.CLM_ID").isNull())

# Build the "Sort" link columns:
df_MemberLkp_Sort_out = df_MemberLkp_Sort.select(
    trim(F.col("CVS.CLAIM_ID")).alias("CLM_ID"),
    F.lit("CVS").alias("SRC_SYS_CD"),
    F.when(
        F.col("CVS.CYC_END_DT").isNull() | (F.length(trim(F.col("CVS.CYC_END_DT")))==0),
        F.lit("1753-01-01")
    ).otherwise(F.col("CVS.CYC_END_DT")).alias("FILE_RCVD_DT"),
    F.lit(None).alias("RCRD_ID"),
    F.when(
        F.col("CVS.SVC_PROV_ID").isNull() | (F.length(trim(F.col("CVS.SVC_PROV_ID")))==0),
        F.lit("UNK")
    ).otherwise(F.col("CVS.SVC_PROV_ID")).alias("PRCSR_NO"),
    F.lit(None).alias("BTCH_NO"),
    F.when(
        F.col("CVS.SVC_PROV_ID").isNull() | (F.length(trim(F.col("CVS.SVC_PROV_ID")))==0),
        F.lit("UNK")
    ).otherwise(F.col("CVS.SVC_PROV_ID")).alias("PDX_NO"),
    F.when(
        F.col("CVS.D_0_RX_NO").isNull() | (F.length(trim(F.col("CVS.D_0_RX_NO")))==0),
        F.lit("UNK")
    ).otherwise(trim(F.col("CVS.D_0_RX_NO"))).alias("RX_NO"),
    F.when(
        F.col("CVS.DT_OF_SVC").isNull() | (F.length(trim(F.col("CVS.DT_OF_SVC")))==0),
        F.lit("1753-01-01")
    ).otherwise(F.col("CVS.DT_OF_SVC")).alias("FILL_DT"),
    F.when(
        F.col("CVS.PROD_ID").isNull() | (F.length(trim(F.col("CVS.PROD_ID")))==0),
        F.lit("UNK")
    ).otherwise(trim(F.col("CVS.PROD_ID"))).alias("NDC"),
    F.when(
        F.col("CVS.PROD_NM").isNull() | (F.length(trim(F.col("CVS.PROD_NM")))==0),
        F.lit(None)
    ).otherwise(trim(F.col("CVS.PROD_NM"))).alias("DRUG_DESC"),
    F.when(
        F.col("CVS.FILL_NO").isNull() | (F.length(trim(F.col("CVS.FILL_NO")))==0),
        F.lit("0")
    ).otherwise(trim(F.col("CVS.FILL_NO"))).alias("NEW_OR_RFL_CD"),
    F.when(
        F.col("CVS.QTY_DISPNS").isNull() | (F.length(trim(F.col("CVS.QTY_DISPNS")))==0),
        F.lit(0)
    ).otherwise(trim(F.col("CVS.QTY_DISPNS"))).alias("METRIC_QTY"),
    F.when(
        F.col("CVS.DAYS_SUPL").isNull() | (F.length(trim(F.col("CVS.DAYS_SUPL")))==0),
        F.lit(0)
    ).otherwise(trim(F.col("CVS.DAYS_SUPL"))).alias("DAYS_SUPL"),
    F.lit(None).alias("BSS_OF_CST_DTRM"),
    F.lit(None).alias("INGR_CST_AMT"),
    F.lit(None).alias("DISPNS_FEE_AMT"),
    F.lit(None).alias("COPAY_AMT"),
    F.lit(None).alias("SLS_TAX_AMT"),
    F.when(F.col("CVS.TOT_AMT_PD_ALL_SRCS").isNull(), F.lit(0)).otherwise(F.col("CVS.TOT_AMT_PD_ALL_SRCS")).alias("BILL_AMT"),
    F.col("CVS.PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("CVS.PATN_LAST_NM").alias("PATN_LAST_NM"),
    F.when(
        F.col("CVS.PATN_DOB").isNull() | (F.length(trim(F.col("CVS.PATN_DOB")))==0),
        F.lit("1753-01-01")
    ).otherwise(F.col("CVS.PATN_DOB")).alias("BRTH_DT"),
    F.col("CVS.PATN_GNDR_CD").alias("SEX_CD"),
    F.when(
        F.col("CVS.CARDHLDR_ID").isNull() | (F.length(trim(F.col("CVS.CARDHLDR_ID")))==0),
        F.lit("UNK")
    ).otherwise(trim(F.col("CVS.CARDHLDR_ID"))).alias("CARDHLDR_ID_NO"),
    F.lit(None).alias("RELSHP_CD"),
    F.when(
        (F.col("CVS.GRP_ID").isNull()) |
        (F.length(trim(F.col("CVS.GRP_ID")))==0) |
        (trim(F.col("CVS.GRP_ID"))=="UNK") |
        (F.col("CVS.GRP_ID")!=F.col("Grp_Xref.GRP_ID"))
      , F.col("svXrefGrp")).otherwise(trim(F.col("CVS.GRP_ID"))).alias("GRP_NO"),
    F.lit(None).alias("HOME_PLN"),
    F.lit(None).alias("HOST_PLN"),
    F.when(
        F.col("CVS.PRSCRBR_ID").isNull() | (F.length(trim(F.col("CVS.PRSCRBR_ID")))==0),
        F.lit("UNK")
    ).otherwise(trim(F.col("CVS.PRSCRBR_ID"))).alias("PRSCRBR_ID"),
    F.when(
        F.col("CVS.DIAG_CD").isNull() | (F.length(trim(F.col("CVS.DIAG_CD")))==0),
        F.lit("UNK")
    ).otherwise(trim(F.col("CVS.DIAG_CD"))).alias("DIAG_CD"),
    F.when(
        F.col("CVS.CARDHLDR_FIRST_NM").isNull() | (F.length(trim(F.col("CVS.CARDHLDR_FIRST_NM")))==0),
        F.lit(None)
    ).otherwise(trim(F.col("CVS.CARDHLDR_FIRST_NM"))).alias("CARDHLDR_FIRST_NM"),
    F.when(
        F.col("CVS.CARDHLDR_LAST_NM").isNull() | (F.length(trim(F.col("CVS.CARDHLDR_LAST_NM")))==0),
        F.lit(None)
    ).otherwise(trim(F.col("CVS.CARDHLDR_LAST_NM"))).alias("CARDHLDR_LAST_NM"),
    F.lit(None).alias("PRAUTH_NO"),
    F.lit(None).alias("PA_MC_SC_NO"),
    F.lit(None).alias("CUST_LOC"),
    F.lit(None).alias("RESUB_CYC_CT"),
    F.when(
        F.col("CVS.DT_RX_WRTN").isNull() | (F.length(trim(F.col("CVS.DT_RX_WRTN")))==0),
        F.lit("1753-01-01")
    ).otherwise(trim(F.col("CVS.DT_RX_WRTN"))).alias("RX_DT"),
    F.when(
        F.col("CVS.DISPNS_AS_WRTN_PROD_SEL_CD").isNull() | (F.length(trim(F.col("CVS.DISPNS_AS_WRTN_PROD_SEL_CD")))==0),
        F.lit("UNK")
    ).otherwise(trim(F.col("CVS.DISPNS_AS_WRTN_PROD_SEL_CD"))).alias("DISPENSE_AS_WRTN_PROD_SEL_CD"),
    F.lit(None).alias("PRSN_CD"),
    F.when(
        F.col("CVS.OTHR_COV_CD").isNull() | (F.length(trim(F.col("CVS.OTHR_COV_CD")))==0),
        F.lit(0)
    ).otherwise(trim(F.col("CVS.OTHR_COV_CD"))).alias("OTHR_COV_CD"),
    F.lit(None).alias("ELIG_CLRFCTN_CD"),
    F.when(
        F.col("CVS.CMPND_CD").isNull() | (F.length(trim(F.col("CVS.CMPND_CD")))==0),
        F.lit(0)
    ).otherwise(trim(F.col("CVS.CMPND_CD"))).alias("CMPND_CD"),
    F.when(
        F.col("CVS.NO_OF_RFLS_AUTH").isNull() | (F.length(trim(F.col("CVS.NO_OF_RFLS_AUTH")))==0),
        F.lit(0)
    ).otherwise(trim(F.col("CVS.NO_OF_RFLS_AUTH"))).alias("NO_OF_RFLS_AUTH"),
    F.lit(None).alias("LVL_OF_SVC"),
    F.lit(None).alias("RX_ORIG_CD"),
    F.lit(None).alias("RX_DENIAL_CLRFCTN"),
    F.lit(None).alias("PRI_PRSCRBR"),
    F.lit(None).alias("CLNC_ID_NO"),
    F.col("CVS.DRUG_TYP").alias("DRUG_TYP"),
    F.col("CVS.PRSCRBR_LAST_NM").alias("PRSCRBR_LAST_NM"),
    F.lit(None).alias("POSTAGE_AMT"),
    F.lit(None).alias("UNIT_DOSE_IN"),
    F.when(F.col("CVS.OPAR_AMT").isNull(), F.lit(0)).otherwise(F.col("CVS.OPAR_AMT")).alias("OTHR_PAYOR_AMT"),
    F.lit(None).alias("BSS_OF_DAYS_SUPL_DTRM"),
    F.lit(None).alias("FULL_AVG_WHLSL_PRICE"),
    F.lit(None).alias("EXPNSN_AREA"),
    F.lit(None).alias("MSTR_CAR"),
    F.lit(None).alias("SUBCAR"),
    F.col("CVS.CLM_TYP").alias("CLM_TYP"),
    F.lit(None).alias("SUBGRP"),
    F.lit(None).alias("PLN_DSGNR"),
    F.when(
        F.col("CVS.ADJDCT_DT").isNull() | (F.length(trim(F.col("CVS.ADJDCT_DT")))==0),
        F.lit("1753-01-01")
    ).otherwise(F.col("CVS.ADJDCT_DT")).alias("ADJDCT_DT"),
    F.lit(None).alias("ADMIN_FEE_AMT"),
    F.lit(None).alias("CAP_AMT"),
    F.lit(None).alias("INGR_CST_SUB_AMT"),
    F.lit(None).alias("MBR_NON_COPAY_AMT"),
    F.lit(None).alias("MBR_PAY_CD"),
    F.lit(None).alias("INCNTV_FEE_AMT"),
    F.lit(None).alias("CLM_ADJ_AMT"),
    F.lit(None).alias("CLM_ADJ_CD"),
    F.when(F.col("CVS.FRMLRY_STTUS")==F.lit("P"), F.lit("Y")).otherwise(F.lit("N")).alias("FRMLRY_FLAG"),
    F.lit(None).alias("GNRC_CLS_NO"),
    F.lit(None).alias("THRPTC_CLS_AHFS"),
    F.lit(None).alias("PDX_TYP"),
    F.lit(None).alias("BILL_BSS_CD"),
    F.lit(None).alias("USL_AND_CUST_CHRG_AMT"),
    F.when(
        F.col("CVS.DT_OF_SVC").isNull() | (F.length(trim(F.col("CVS.DT_OF_SVC")))==0),
        F.lit("1753-01-01")
    ).otherwise(F.col("CVS.DT_OF_SVC")).alias("PD_DT"),
    F.lit(None).alias("BNF_CD"),
    F.lit(None).alias("DRUG_STRG"),
    F.when(
        F.col("CVS.MBR_UNIQ_KEY")==0,
        F.col("CVS.MBR_UNIQ_KEY")
    ).otherwise(F.col("Clm_Mbr_Lkp.MBR_UNIQ_KEY")).alias("ORIG_MBR"),
    F.lit("1753-01-01").alias("INJRY_DT"),
    F.lit("0.00").alias("FEE_AMT"),
    F.when(
        F.col("CVS.TRANS_ID").isNull() | (F.length(trim(F.col("CVS.TRANS_ID")))==0),
        F.lit("UNK")
    ).otherwise(trim(F.col("CVS.TRANS_ID"))).alias("REF_NO"),
    F.lit(None).alias("CLNT_CUST_ID"),
    F.lit(None).alias("PLN_TYP"),
    F.lit(None).alias("ADJDCT_REF_NO"),
    F.lit(None).alias("ANCLRY_AMT"),
    F.lit(None).alias("CLNT_GNRL_PRPS_AREA"),
    F.lit(None).alias("PRTL_FILL_STTUS_CD"),
    F.when(
        F.col("CVS.ADJDCT_DT").isNull() | (F.length(trim(F.col("CVS.ADJDCT_DT")))==0),
        F.lit("1753-01-01")
    ).otherwise(F.col("CVS.ADJDCT_DT")).alias("BILL_DT"),
    F.lit(None).alias("FSA_VNDR_CD"),
    F.lit(None).alias("PICA_DRUG_CD"),
    F.when(F.col("CVS.TOT_AMT_PD_ALL_SRCS").isNull(), F.lit(0)).otherwise(F.col("CVS.TOT_AMT_PD_ALL_SRCS")).alias("CLM_AMT"),
    F.lit(None).alias("DSALW_AMT"),
    F.lit(None).alias("FED_DRUG_CLS_CD"),
    F.lit(None).alias("DEDCT_AMT"),
    F.lit(None).alias("BNF_COPAY_100"),
    F.lit(None).alias("CLM_PRCS_TYP"),
    F.lit(None).alias("INDEM_HIER_TIER_NO"),
    F.lit(None).alias("MCARE_D_COV_DRUG"),
    F.lit(None).alias("RETRO_LICS_CD"),
    F.lit(None).alias("RETRO_LICS_AMT"),
    F.lit(None).alias("LICS_SBSDY_AMT"),
    F.lit(None).alias("MCARE_B_DRUG"),
    F.lit(None).alias("MCARE_B_CLM"),
    F.lit(None).alias("PRSCRBR_QLFR"),
    F.when(
        F.col("CVS.PRSCRBR_ID").isNull() | (F.length(trim(F.col("CVS.PRSCRBR_ID")))==0),
        F.lit("UNK")
    ).otherwise(trim(F.col("CVS.PRSCRBR_ID"))).alias("PRSCRBR_NTNL_PROV_ID"),
    F.lit(None).alias("PDX_QLFR"),
    F.when(
        F.col("CVS.SVC_PROV_ID").isNull() | (F.length(trim(F.col("CVS.SVC_PROV_ID")))==0),
        F.lit("UNK")
    ).otherwise(trim(F.col("CVS.SVC_PROV_ID"))).alias("PDX_NTNL_PROV_ID"),
    F.lit(None).alias("HLTH_RMBRMT_ARGMT_APLD_AMT"),
    F.lit(None).alias("THER_CLS"),
    F.lit(None).alias("HIC_NO"),
    F.lit(None).alias("HLTH_RMBRMT_ARGMT_FLAG"),
    F.lit(None).alias("DOSE_CD"),
    F.lit(None).alias("LOW_INCM"),
    F.lit(None).alias("RTE_OF_ADMIN"),
    F.lit(None).alias("DEA_SCHD"),
    F.lit(None).alias("COPAY_BNF_OPT"),
    F.when(
        F.col("CVS.GNRC_PROD_ID").isNull() | (F.length(trim(F.col("CVS.GNRC_PROD_ID")))==0),
        F.lit("1")
    ).otherwise(trim(F.col("CVS.GNRC_PROD_ID"))).alias("GNRC_PROD_IN"),
    F.lit(None).alias("PRSCRBR_SPEC"),
    F.lit(None).alias("VAL_CD"),
    F.lit(None).alias("PRI_CARE_PDX"),
    F.lit(None).alias("OFC_OF_INSPECTOR_GNRL"),
    F.lit("000000000").alias("PATN_SSN"),
    F.when(
        F.col("CVS.CARDHLDR_ID").isNull() | (F.length(trim(F.col("CVS.CARDHLDR_ID")))==0),
        F.lit("000000000")
    ).otherwise(trim(F.col("CVS.CARDHLDR_ID"))).alias("CARDHLDR_SSN"),
    F.when(
        F.col("CVS.CARDHLDR_DOB").isNull() | (F.length(trim(F.col("CVS.CARDHLDR_DOB")))==0),
        F.lit("1753-01-01")
    ).otherwise(trim(F.col("CVS.CARDHLDR_DOB"))).alias("CARDHLDR_BRTH_DT"),
    F.lit(None).alias("CARDHLDR_ADDR"),
    F.lit(None).alias("CARDHLDR_CITY"),
    F.lit(None).alias("CHADHLDR_ST"),
    F.lit(None).alias("CARDHLDR_ZIP_CD"),
    F.lit(None).alias("PSL_FMLY_MET_AMT"),
    F.lit(None).alias("PSL_MBR_MET_AMT"),
    F.lit(None).alias("PSL_FMLY_AMT"),
    F.lit(None).alias("DEDCT_FMLY_MET_AMT"),
    F.lit(None).alias("DEDCT_FMLY_AMT"),
    F.lit(None).alias("MOPS_FMLY_AMT"),
    F.lit(None).alias("MOPS_FMLY_MET_AMT"),
    F.lit(None).alias("MOPS_MBR_MET_AMT"),
    F.lit(None).alias("DEDCT_MBR_MET_AMT"),
    F.lit(None).alias("PSL_APLD_AMT"),
    F.lit(None).alias("MOPS_APLD_AMT"),
    F.lit("Y").alias("PAR_PDX_IN"),
    F.lit(None).alias("COPAY_PCT_AMT"),
    F.lit(None).alias("COPAY_FLAT_AMT"),
    F.lit(None).alias("CLM_TRNSMSN_METH"),
    F.when(
        F.col("CVS.D_0_RX_NO").isNull() | (F.length(trim(F.col("CVS.D_0_RX_NO")))==0),
        F.lit(0)
    ).otherwise(trim(F.col("CVS.D_0_RX_NO"))).alias("RX_NO_2012"),
    F.col("CVS.RCRD_STTUS_CD").alias("CLM_STTUS_CD"),
    F.when(
        F.col("CVS.CLM_TYP")==F.lit("R"),
        F.expr("substring(trim(CVS.CLAIM_ID), 1, length(trim(CVS.CLAIM_ID))-1)")
    ).otherwise(F.lit("NA")).alias("ADJ_FROM_CLM_ID"),
    F.lit("NA").alias("ADJ_TO_CLM_ID")
)

# Build the "Reject" link columns:
df_MemberLkp_Reject_out = df_MemberLkp_Reject.select(
    F.col("CVS.CLAIM_ID").alias("CLM_ID"),
    F.col("CVS.GRP_ID").alias("GRP_NO"),
    F.col("CVS.TRANS_ID").alias("REF_NO"),
    F.col("CVS.D_0_RX_NO").alias("RX_NO"),
    F.when(
        F.col("CVS.DT_OF_SVC").isNull() | (F.length(trim(F.col("CVS.DT_OF_SVC")))==0),
        F.lit("        ")
    ).otherwise(F.col("CVS.DT_OF_SVC")).alias("FILL_DT"),
    F.col("CVS.PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("CVS.PATN_LAST_NM").alias("PATN_LAST_NM"),
    F.when(
        F.col("CVS.PATN_DOB").isNull() | (F.length(trim(F.col("CVS.PATN_DOB")))==0),
        F.lit("        ")
    ).otherwise(F.col("CVS.PATN_DOB")).alias("BRTH_DT"),
    F.lit("000000000").alias("PATN_SSN"),
    F.col("CVS.CARDHLDR_ID").alias("CARDHLDR_ID_NO"),
    F.col("CVS.CARDHLDR_FIRST_NM").alias("CARDHLDR_FIRST_NM"),
    F.col("CVS.CARDHLDR_LAST_NM").alias("CARDHLDR_LAST_NM"),
    F.when(
        F.col("CVS.CARDHLDR_DOB").isNull() | (F.length(trim(F.col("CVS.CARDHLDR_DOB")))==0),
        F.lit("        ")
    ).otherwise(F.col("CVS.CARDHLDR_DOB")).alias("CARDHLDR_BRTH_DT"),
    F.col("CVS.CARDHLDR_ID").alias("CARDHLDR_SSN"),
    F.col("CVS.PATN_GNDR_CD").alias("GNDR_CD")
)

# ----------------------------------------------------------------------------
# STAGE: hf_CVS_clm_land_reject => scenario A
# PK = ["CLM_ID"]
# ----------------------------------------------------------------------------
df_Reject = df_MemberLkp_Reject_out.dropDuplicates(["CLM_ID"])

# ----------------------------------------------------------------------------
# STAGE: DropField (CTransformerStage)
# ----------------------------------------------------------------------------
df_DropField = df_Reject.select(
    F.col("REF_NO").alias("REF_NO"),
    F.col("GRP_NO").alias("GRP_NO"),
    F.col("FILL_DT").alias("FILL_DT"),
    F.col("PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("PATN_LAST_NM").alias("PATN_LAST_NM"),
    F.col("BRTH_DT").alias("BRTH_DT"),
    F.col("PATN_SSN").alias("PATN_SSN"),
    F.col("CARDHLDR_ID_NO").alias("CARDHLDR_ID_NO"),
    F.col("CARDHLDR_FIRST_NM").alias("CARDHLDR_FIRST_NM"),
    F.col("CARDHLDR_LAST_NM").alias("CARDHLDR_LAST_NM"),
    F.col("CARDHLDR_BRTH_DT").alias("CARDHLDR_BRTH_DT"),
    F.col("CARDHLDR_SSN").alias("CARDHLDR_SSN"),
    F.col("GNDR_CD").alias("GNDR_CD"),
    F.lit("FAILED ELIGIBILITY").alias("ERR_RSN_STRING")
)

# ----------------------------------------------------------------------------
# STAGE: MbrEnrNotFound (CSeqFileStage) => write to "external/CVSDrugClm_NoMbrMatchRecs.csv" (append)
# ----------------------------------------------------------------------------
# Rpad columns that are char or varchar:
df_MbrEnrNotFound_write = df_DropField.select(
    F.rpad(F.col("REF_NO"), 0, " ").alias("REF_NO"),
    F.rpad(F.col("GRP_NO"), 0, " ").alias("GRP_NO"),
    F.rpad(F.col("FILL_DT"), 10, " ").alias("FILL_DT"),
    F.rpad(F.col("PATN_FIRST_NM"), 0, " ").alias("PATN_FIRST_NM"),
    F.rpad(F.col("PATN_LAST_NM"), 0, " ").alias("PATN_LAST_NM"),
    F.rpad(F.col("BRTH_DT"), 10, " ").alias("BRTH_DT"),
    F.rpad(F.col("PATN_SSN"), 11, " ").alias("PATN_SSN"),
    F.rpad(F.col("CARDHLDR_ID_NO"), 0, " ").alias("CARDHLDR_ID_NO"),
    F.rpad(F.col("CARDHLDR_FIRST_NM"), 0, " ").alias("CARDHLDR_FIRST_NM"),
    F.rpad(F.col("CARDHLDR_LAST_NM"), 0, " ").alias("CARDHLDR_LAST_NM"),
    F.rpad(F.col("CARDHLDR_BRTH_DT"), 10, " ").alias("CARDHLDR_BRTH_DT"),
    F.rpad(F.col("CARDHLDR_SSN"), 11, " ").alias("CARDHLDR_SSN"),
    F.rpad(F.col("GNDR_CD"), 1, " ").alias("GNDR_CD"),
    F.rpad(F.col("ERR_RSN_STRING"), 100, " ").alias("ERR_RSN_STRING")
)
write_files(
    df_MbrEnrNotFound_write,
    f"{adls_path_publish}/external/CVSDrugClm_NoMbrMatchRecs.csv",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# ----------------------------------------------------------------------------
# STAGE: Sort => sorted by "CLM_ID asc, ORIG_MBR asc"
# ----------------------------------------------------------------------------
df_Sort = df_MemberLkp_Sort_out.orderBy(F.col("CLM_ID").asc(), F.col("ORIG_MBR").asc())

# ----------------------------------------------------------------------------
# STAGE: hf_CVS_dedupe_clm_mbr_uniq_key => scenario A
# PK = ["CLM_ID"]
# ----------------------------------------------------------------------------
df_WritetoFile = df_Sort.dropDuplicates(["CLM_ID"])

# ----------------------------------------------------------------------------
# STAGE: Enr_PCP (CTransformerStage) => outputs "Landing_File", "Load_ENR", "Load_PCP"
# ----------------------------------------------------------------------------
df_Enr_PCP_Landing_File = df_WritetoFile.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("FILE_RCVD_DT").alias("FILE_RCVD_DT"),
    F.col("RCRD_ID").alias("RCRD_ID"),
    F.col("PRCSR_NO").alias("PRCSR_NO"),
    F.col("BTCH_NO").alias("BTCH_NO"),
    F.col("PDX_NO").alias("PDX_NO"),
    F.col("RX_NO").alias("RX_NO"),
    F.col("FILL_DT").alias("FILL_DT"),
    F.col("NDC").alias("NDC"),
    F.col("DRUG_DESC").alias("DRUG_DESC"),
    F.col("NEW_OR_RFL_CD").alias("NEW_OR_RFL_CD"),
    F.col("METRIC_QTY").alias("METRIC_QTY"),
    F.col("DAYS_SUPL").alias("DAYS_SUPL"),
    F.col("BSS_OF_CST_DTRM").alias("BSS_OF_CST_DTRM"),
    F.col("INGR_CST_AMT").alias("INGR_CST_AMT"),
    F.col("DISPNS_FEE_AMT").alias("DISPNS_FEE_AMT"),
    F.col("COPAY_AMT").alias("COPAY_AMT"),
    F.col("SLS_TAX_AMT").alias("SLS_TAX_AMT"),
    F.col("BILL_AMT").alias("BILL_AMT"),
    F.col("PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("PATN_LAST_NM").alias("PATN_LAST_NM"),
    F.col("BRTH_DT").alias("BRTH_DT"),
    F.col("SEX_CD").alias("SEX_CD"),
    F.col("CARDHLDR_ID_NO").alias("CARDHLDR_ID_NO"),
    F.col("RELSHP_CD").alias("RELSHP_CD"),
    F.col("GRP_NO").alias("GRP_NO"),
    F.col("HOME_PLN").alias("HOME_PLN"),
    F.col("HOST_PLN").alias("HOST_PLN"),
    F.col("PRSCRBR_ID").alias("PRSCRBR_ID"),
    F.col("DIAG_CD").alias("DIAG_CD"),
    F.col("CARDHLDR_FIRST_NM").alias("CARDHLDR_FIRST_NM"),
    F.col("CARDHLDR_LAST_NM").alias("CARDHLDR_LAST_NM"),
    F.col("PRAUTH_NO").alias("PRAUTH_NO"),
    F.col("PA_MC_SC_NO").alias("PA_MC_SC_NO"),
    F.col("CUST_LOC").alias("CUST_LOC"),
    F.col("RESUB_CYC_CT").alias("RESUB_CYC_CT"),
    F.col("RX_DT").alias("RX_DT"),
    F.col("DISPENSE_AS_WRTN_PROD_SEL_CD").alias("DISPENSE_AS_WRTN_PROD_SEL_CD"),
    F.col("PRSN_CD").alias("PRSN_CD"),
    F.col("OTHR_COV_CD").alias("OTHR_COV_CD"),
    F.col("ELIG_CLRFCTN_CD").alias("ELIG_CLRFCTN_CD"),
    F.col("CMPND_CD").alias("CMPND_CD"),
    F.col("NO_OF_RFLS_AUTH").alias("NO_OF_RFLS_AUTH"),
    F.col("LVL_OF_SVC").alias("LVL_OF_SVC"),
    F.col("RX_ORIG_CD").alias("RX_ORIG_CD"),
    F.col("RX_DENIAL_CLRFCTN").alias("RX_DENIAL_CLRFCTN"),
    F.col("PRI_PRSCRBR").alias("PRI_PRSCRBR"),
    F.col("CLNC_ID_NO").alias("CLNC_ID_NO"),
    F.col("DRUG_TYP").alias("DRUG_TYP"),
    F.col("PRSCRBR_LAST_NM").alias("PRSCRBR_LAST_NM"),
    F.col("POSTAGE_AMT").alias("POSTAGE_AMT"),
    F.col("UNIT_DOSE_IN").alias("UNIT_DOSE_IN"),
    F.col("OTHR_PAYOR_AMT").alias("OTHR_PAYOR_AMT"),
    F.col("BSS_OF_DAYS_SUPL_DTRM").alias("BSS_OF_DAYS_SUPL_DTRM"),
    F.col("FULL_AVG_WHLSL_PRICE").alias("FULL_AVG_WHLSL_PRICE"),
    F.col("EXPNSN_AREA").alias("EXPNSN_AREA"),
    F.col("MSTR_CAR").alias("MSTR_CAR"),
    F.col("SUBCAR").alias("SUBCAR"),
    F.col("CLM_TYP").alias("CLM_TYP"),
    F.col("SUBGRP").alias("SUBGRP"),
    F.col("PLN_DSGNR").alias("PLN_DSGNR"),
    F.col("ADJDCT_DT").alias("ADJDCT_DT"),
    F.col("ADMIN_FEE_AMT").alias("ADMIN_FEE_AMT"),
    F.col("CAP_AMT").alias("CAP_AMT"),
    F.col("INGR_CST_SUB_AMT").alias("INGR_CST_SUB_AMT"),
    F.col("MBR_NON_COPAY_AMT").alias("MBR_NON_COPAY_AMT"),
    F.col("MBR_PAY_CD").alias("MBR_PAY_CD"),
    F.col("INCNTV_FEE_AMT").alias("INCNTV_FEE_AMT"),
    F.col("CLM_ADJ_AMT").alias("CLM_ADJ_AMT"),
    F.col("CLM_ADJ_CD").alias("CLM_ADJ_CD"),
    F.col("FRMLRY_FLAG").alias("FRMLRY_FLAG"),
    F.col("GNRC_CLS_NO").alias("GNRC_CLS_NO"),
    F.col("THRPTC_CLS_AHFS").alias("THRPTC_CLS_AHFS"),
    F.col("PDX_TYP").alias("PDX_TYP"),
    F.col("BILL_BSS_CD").alias("BILL_BSS_CD"),
    F.col("USL_AND_CUST_CHRG_AMT").alias("USL_AND_CUST_CHRG_AMT"),
    F.col("PD_DT").alias("PD_DT"),
    F.col("BNF_CD").alias("BNF_CD"),
    F.col("DRUG_STRG").alias("DRUG_STRG"),
    F.col("ORIG_MBR").alias("ORIG_MBR"),
    F.col("INJRY_DT").alias("INJRY_DT"),
    F.col("FEE_AMT").alias("FEE_AMT"),
    F.col("REF_NO").alias("REF_NO"),
    F.col("CLNT_CUST_ID").alias("CLNT_CUST_ID"),
    F.col("PLN_TYP").alias("PLN_TYP"),
    F.col("ADJDCT_REF_NO").alias("ADJDCT_REF_NO"),
    F.col("ANCLRY_AMT").alias("ANCLRY_AMT"),
    F.col("CLNT_GNRL_PRPS_AREA").alias("CLNT_GNRL_PRPS_AREA"),
    F.col("PRTL_FILL_STTUS_CD").alias("PRTL_FILL_STTUS_CD"),
    F.col("BILL_DT").alias("BILL_DT"),
    F.col("FSA_VNDR_CD").alias("FSA_VNDR_CD"),
    F.col("PICA_DRUG_CD").alias("PICA_DRUG_CD"),
    F.col("CLM_AMT").alias("CLM_AMT"),
    F.col("DSALW_AMT").alias("DSALW_AMT"),
    F.col("FED_DRUG_CLS_CD").alias("FED_DRUG_CLS_CD"),
    F.col("DEDCT_AMT").alias("DEDCT_AMT"),
    F.col("BNF_COPAY_100").alias("BNF_COPAY_100"),
    F.col("CLM_PRCS_TYP").alias("CLM_PRCS_TYP"),
    F.col("INDEM_HIER_TIER_NO").alias("INDEM_HIER_TIER_NO"),
    F.col("MCARE_D_COV_DRUG").alias("MCARE_D_COV_DRUG"),
    F.col("RETRO_LICS_CD").alias("RETRO_LICS_CD"),
    F.col("RETRO_LICS_AMT").alias("RETRO_LICS_AMT"),
    F.col("LICS_SBSDY_AMT").alias("LICS_SBSDY_AMT"),
    F.col("MCARE_B_DRUG").alias("MCARE_B_DRUG"),
    F.col("MCARE_B_CLM").alias("MCARE_B_CLM"),
    F.col("PRSCRBR_QLFR").alias("PRSCRBR_QLFR"),
    F.col("PRSCRBR_NTNL_PROV_ID").alias("PRSCRBR_NTNL_PROV_ID"),
    F.col("PDX_QLFR").alias("PDX_QLFR"),
    F.col("PDX_NTNL_PROV_ID").alias("PDX_NTNL_PROV_ID"),
    F.col("HLTH_RMBRMT_ARGMT_APLD_AMT").alias("HLTH_RMBRMT_ARGMT_APLD_AMT"),
    F.col("THER_CLS").alias("THER_CLS"),
    F.col("HIC_NO").alias("HIC_NO"),
    F.col("HLTH_RMBRMT_ARGMT_FLAG").alias("HLTH_RMBRMT_ARGMT_FLAG"),
    F.col("DOSE_CD").alias("DOSE_CD"),
    F.col("LOW_INCM").alias("LOW_INCM"),
    F.col("RTE_OF_ADMIN").alias("RTE_OF_ADMIN"),
    F.col("DEA_SCHD").alias("DEA_SCHD"),
    F.col("COPAY_BNF_OPT").alias("COPAY_BNF_OPT"),
    F.col("GNRC_PROD_IN").alias("GNRC_PROD_IN"),
    F.col("PRSCRBR_SPEC").alias("PRSCRBR_SPEC"),
    F.col("VAL_CD").alias("VAL_CD"),
    F.col("PRI_CARE_PDX").alias("PRI_CARE_PDX"),
    F.col("OFC_OF_INSPECTOR_GNRL").alias("OFC_OF_INSPECTOR_GNRL"),
    F.col("PATN_SSN").alias("PATN_SSN"),
    F.col("CARDHLDR_SSN").alias("CARDHLDR_SSN"),
    F.col("CARDHLDR_BRTH_DT").alias("CARDHLDR_BRTH_DT"),
    F.col("CARDHLDR_ADDR").alias("CARDHLDR_ADDR"),
    F.col("CARDHLDR_CITY").alias("CARDHLDR_CITY"),
    F.col("CHADHLDR_ST").alias("CHADHLDR_ST"),
    F.col("CARDHLDR_ZIP_CD").alias("CARDHLDR_ZIP_CD"),
    F.col("PSL_FMLY_MET_AMT").alias("PSL_FMLY_MET_AMT"),
    F.col("PSL_MBR_MET_AMT").alias("PSL_MBR_MET_AMT"),
    F.col("PSL_FMLY_AMT").alias("PSL_FMLY_AMT"),
    F.col("DEDCT_FMLY_MET_AMT").alias("DEDCT_FMLY_MET_AMT"),
    F.col("DEDCT_FMLY_AMT").alias("DEDCT_FMLY_AMT"),
    F.col("MOPS_FMLY_AMT").alias("MOPS_FMLY_AMT"),
    F.col("MOPS_FMLY_MET_AMT").alias("MOPS_FMLY_MET_AMT"),
    F.col("MOPS_MBR_MET_AMT").alias("MOPS_MBR_MET_AMT"),
    F.col("DEDCT_MBR_MET_AMT").alias("DEDCT_MBR_MET_AMT"),
    F.col("PSL_APLD_AMT").alias("PSL_APLD_AMT"),
    F.col("MOPS_APLD_AMT").alias("MOPS_APLD_AMT"),
    F.col("PAR_PDX_IN").alias("PAR_PDX_IN"),
    F.col("COPAY_PCT_AMT").alias("COPAY_PCT_AMT"),
    F.col("COPAY_FLAT_AMT").alias("COPAY_FLAT_AMT"),
    F.col("CLM_TRNSMSN_METH").alias("CLM_TRNSMSN_METH"),
    F.col("RX_NO_2012").alias("RX_NO_2012"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_STTUS_CD").alias("CLM_STTUS_CD"),
    F.col("ADJ_FROM_CLM_ID").alias("ADJ_FROM_CLM_ID"),
    F.col("ADJ_TO_CLM_ID").alias("ADJ_TO_CLM_ID")
)

df_Enr_PCP_Load_ENR = df_WritetoFile.select(
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("FILL_DT").alias("FILL_DT_SK"),
    F.col("ORIG_MBR").alias("MBR_UNIQ_KEY")
)

df_Enr_PCP_Load_PCP = df_WritetoFile.select(
    F.col("ORIG_MBR").alias("MBR_UNIQ_KEY"),
    F.col("FILL_DT").alias("FILL_DT_SK")
)

# ----------------------------------------------------------------------------
# STAGE: PDX_CLM_STD_INPT_Land (CSeqFileStage)
# Write to "verified/PDX_CLM_STD_INPT_Land_#SrcSysCd#.dat.#RunID#"
# ----------------------------------------------------------------------------
df_Landing_File_write = df_Enr_PCP_Landing_File.select(
    *[F.rpad(F.col(c), 0, " ") if c not in ["FILE_RCVD_DT","FILL_DT","BRTH_DT","RX_DT","DISPENSE_AS_WRTN_PROD_SEL_CD",
                                            "CLM_TYP","ADJDCT_DT","PAR_PDX_IN","PD_DT","INJRY_DT","BILL_DT","PICA_DRUG_CD",
                                            "CLM_PRCS_TYP","BNF_COPAY_100"] else F.rpad(F.col(c), 
                                                10 if c in ["FILE_RCVD_DT","FILL_DT","BRTH_DT","RX_DT","PD_DT","INJRY_DT","BILL_DT"] 
                                                else (1 if c in ["DISPENSE_AS_WRTN_PROD_SEL_CD","CLM_TYP","PAR_PDX_IN","PICA_DRUG_CD","CLM_PRCS_TYP","BNF_COPAY_100"] 
                                                else 2), " ").alias(c)
    for c in df_Enr_PCP_Landing_File.columns
)
write_files(
    df_Landing_File_write,
    f"{adls_path}/verified/PDX_CLM_STD_INPT_Land_{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# ----------------------------------------------------------------------------
# STAGE: W_DRUG_ENR (CSeqFileStage) => "load/W_DRUG_ENR.dat"
# ----------------------------------------------------------------------------
df_Load_ENR_write = df_Enr_PCP_Load_ENR.select(
    F.rpad(F.col("CLM_ID"), 0, " ").alias("CLM_ID"),
    F.rpad(F.col("FILL_DT_SK"), 10, " ").alias("FILL_DT_SK"),
    F.rpad(F.col("MBR_UNIQ_KEY"), 0, " ").alias("MBR_UNIQ_KEY")
)
write_files(
    df_Load_ENR_write,
    f"{adls_path}/load/W_DRUG_ENR.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# ----------------------------------------------------------------------------
# STAGE: W_DRUG_CLM_PCP (CSeqFileStage) => "load/W_DRUG_CLM_PCP.dat"
# ----------------------------------------------------------------------------
df_Load_PCP_write = df_Enr_PCP_Load_PCP.select(
    F.rpad(F.col("MBR_UNIQ_KEY"), 0, " ").alias("MBR_UNIQ_KEY"),
    F.rpad(F.col("FILL_DT_SK"), 10, " ").alias("FILL_DT_SK")
)
write_files(
    df_Load_PCP_write,
    f"{adls_path}/load/W_DRUG_CLM_PCP.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)