# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2010 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:   Pulls data from MBR_MCARE_EVT_AUDIT and loads in to MBR_MCARE_EVT_AUDIT_D table.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer               Date                         Change Description                                                                           Project #          Development Project          Code Reviewer          Date Reviewed  
# MAGIC ------------------             ----------------------------     -----------------------------------------------------------------------                                   ----------------         ------------------------------------       ----------------------------      ----------------
# MAGIC Kalyan Neelam         2020-10-05               Original Prgramming                                                                                                    EnterpriseDev2                  Reddy Sanam            2020-10-05

# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Write MBR_MCARE_EVT_AUDIT_D Data into a Sequential file for Load Ready Job.
# MAGIC Job name: IdsEdwMbrMcareEvtAuditDExtr
# MAGIC 
# MAGIC 
# MAGIC Job Extract data from IDS table and apply Fkey and Code denormalization, create a load ready file.
# MAGIC Read data from source table 
# MAGIC MBR_MCARE_EVT_AUDIT
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
IdsMbrMcareEvtAuditRunCycle = get_widget_value('IdsMbrMcareEvtAuditRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = f"""SELECT 
MBR_MCARE_EVT_AUDIT.MBR_MCARE_EVT_AUDIT_SK,
COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
MBR_MCARE_EVT_AUDIT.MBR_MCARE_EVT_ROW_AUDIT_ID,
MBR_MCARE_EVT_AUDIT.CRT_RUN_CYC_EXCTN_SK,
MBR_MCARE_EVT_AUDIT.LAST_UPDT_RUN_CYC_EXCTN_SK,
MBR_MCARE_EVT_AUDIT.SRC_SYS_CRT_DT_SK,
MBR_MCARE_EVT_AUDIT.MBR_MCARE_EVT_ACTN_CD_SK,
MBR_MCARE_EVT_AUDIT.MBR_SK,
MBR_MCARE_EVT_AUDIT.SRC_SYS_CRT_USER_SK,
MBR_MCARE_EVT_AUDIT.MBR_UNIQ_KEY,
MBR_MCARE_EVT_AUDIT.MBR_MCARE_EVT_CD_SK,
MBR_MCARE_EVT_AUDIT.EFF_DT_SK,
MBR_MCARE_EVT_AUDIT.TERM_DT_SK,
MBR_MCARE_EVT_AUDIT.SRC_SYS_CRT_DT,
MBR_MCARE_EVT_AUDIT.MCARE_EVT_ADTNL_EFF_DT,
MBR_MCARE_EVT_AUDIT.MCARE_EVT_ADTNL_TERM_DT,
MBR_MCARE_EVT_AUDIT.MBR_RESDNC_ST_NM,
MBR_MCARE_EVT_AUDIT.MBR_RESDNC_CNTY_NM,
MBR_MCARE_EVT_AUDIT.MBR_MCARE_ID,
MBR_MCARE_EVT_AUDIT.BILL_GRP_UNIQ_KEY,
MBR_MCARE_EVT_AUDIT.MCARE_CNTR_ID,
MBR_MCARE_EVT_AUDIT.PART_A_RISK_ADJ_FCTR,
MBR_MCARE_EVT_AUDIT.PART_B_RISK_ADJ_FCTR,
MBR_MCARE_EVT_AUDIT.SGNTR_DT,
MBR_MCARE_EVT_AUDIT.MCARE_ELECTN_TYP_ID,
MBR_MCARE_EVT_AUDIT.PLN_BNF_PCKG_ID,
MBR_MCARE_EVT_AUDIT.SEG_ID,
MBR_MCARE_EVT_AUDIT.PART_D_RISK_ADJ_FCTR,
MBR_MCARE_EVT_AUDIT.RISK_ADJ_FCTR_TYP_ID,
MBR_MCARE_EVT_AUDIT.PRM_WTHLD_OPT_ID,
MBR_MCARE_EVT_AUDIT.PART_C_PRM_AMT,
MBR_MCARE_EVT_AUDIT.PART_D_PRM_AMT,
MBR_MCARE_EVT_AUDIT.PRIOR_COM_OVRD_ID,
MBR_MCARE_EVT_AUDIT.ENR_SRC_ID,
MBR_MCARE_EVT_AUDIT.UNCOV_MO_CT,
MBR_MCARE_EVT_AUDIT.PART_D_ID,
MBR_MCARE_EVT_AUDIT.PART_D_GRP_ID,
MBR_MCARE_EVT_AUDIT.PART_D_RX_BIN_ID,
MBR_MCARE_EVT_AUDIT.PART_D_RX_PCN_ID,
MBR_MCARE_EVT_AUDIT.SEC_DRUG_INSUR_IN,
MBR_MCARE_EVT_AUDIT.SEC_DRUG_INSUR_ID,
MBR_MCARE_EVT_AUDIT.SEC_DRUG_INSUR_GRP_ID,
MBR_MCARE_EVT_AUDIT.SEC_DRUG_INSUR_BIN_ID,
MBR_MCARE_EVT_AUDIT.SEC_DRUG_INSUR_PCN_ID,
MBR_MCARE_EVT_AUDIT.PART_D_SBSDY_ID,
MBR_MCARE_EVT_AUDIT.COPAY_CAT_ID,
MBR_MCARE_EVT_AUDIT.PART_D_LOW_INCM_PRM_SBSDY_AMT,
MBR_MCARE_EVT_AUDIT.PART_D_LATE_ENR_PNLTY_AMT,
MBR_MCARE_EVT_AUDIT.PART_D_LATE_ENR_PNLTY_WAIVED_AMT,
MBR_MCARE_EVT_AUDIT.PART_D_LATE_ENR_PNLTY_SBSDY_AMT,
MBR_MCARE_EVT_AUDIT.AGED_DSBLD_MCARE_SEC_PAYER_STTUS_ID,
MBR_MCARE_EVT_AUDIT.PART_D_RISK_ADJ_FCTR_TYP_ID,
MBR_MCARE_EVT_AUDIT.IC_MDL_IN,
MBR_MCARE_EVT_AUDIT.IC_MDL_BNF_STTUS_TX,
MBR_MCARE_EVT_AUDIT.IC_MDL_END_DT_RSN_TX,
MBR_MCARE_EVT_AUDIT.MBR_MCARE_EVT_ATCHMT_DTM
FROM {IDSOwner}.MBR_MCARE_EVT_AUDIT MBR_MCARE_EVT_AUDIT
LEFT JOIN {IDSOwner}.CD_MPPNG CD
ON  MBR_MCARE_EVT_AUDIT.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
WHERE 
MBR_MCARE_EVT_AUDIT.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
AND (CD.TRGT_CD = 'FACETS' AND MBR_MCARE_EVT_AUDIT.LAST_UPDT_RUN_CYC_EXCTN_SK >= {IdsMbrMcareEvtAuditRunCycle})
"""
df_DB2_MBR_MCARE_EVT_AUDIT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

extract_query = f"""SELECT
CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') AS TRGT_CD,
COALESCE(TRGT_CD_NM,'UNK') AS TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG
"""
df_db2_CD_MPPNG_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_ActnCd = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_EvtCd = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_lkp_FKeys = (
    df_DB2_MBR_MCARE_EVT_AUDIT.alias("Lnk_IdsEdwMbrMcareEvtAuditDExtr_InABC")
    .join(
        df_ActnCd.alias("ActnCd"),
        F.col("Lnk_IdsEdwMbrMcareEvtAuditDExtr_InABC.MBR_MCARE_EVT_ACTN_CD_SK") == F.col("ActnCd.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_EvtCd.alias("EvtCd"),
        F.col("Lnk_IdsEdwMbrMcareEvtAuditDExtr_InABC.MBR_MCARE_EVT_CD_SK") == F.col("EvtCd.CD_MPPNG_SK"),
        "left"
    )
)

df_lkp_FKeys = df_lkp_FKeys.select(
    F.col("Lnk_IdsEdwMbrMcareEvtAuditDExtr_InABC.MBR_MCARE_EVT_AUDIT_SK").alias("MBR_MCARE_EVT_AUDIT_SK"),
    F.col("Lnk_IdsEdwMbrMcareEvtAuditDExtr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Lnk_IdsEdwMbrMcareEvtAuditDExtr_InABC.MBR_MCARE_EVT_ROW_AUDIT_ID").alias("MBR_MCARE_EVT_ROW_AUDIT_ID"),
    F.col("Lnk_IdsEdwMbrMcareEvtAuditDExtr_InABC.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Lnk_IdsEdwMbrMcareEvtAuditDExtr_InABC.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Lnk_IdsEdwMbrMcareEvtAuditDExtr_InABC.SRC_SYS_CRT_DT_SK").alias("SRC_SYS_CRT_DT_SK"),
    F.col("Lnk_IdsEdwMbrMcareEvtAuditDExtr_InABC.MBR_MCARE_EVT_ACTN_CD_SK").alias("MBR_MCARE_EVT_ACTN_CD_SK"),
    F.col("Lnk_IdsEdwMbrMcareEvtAuditDExtr_InABC.MBR_SK").alias("MBR_SK"),
    F.col("Lnk_IdsEdwMbrMcareEvtAuditDExtr_InABC.SRC_SYS_CRT_USER_SK").alias("SRC_SYS_CRT_USER_SK"),
    F.col("Lnk_IdsEdwMbrMcareEvtAuditDExtr_InABC.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("Lnk_IdsEdwMbrMcareEvtAuditDExtr_InABC.MBR_MCARE_EVT_CD_SK").alias("MBR_MCARE_EVT_CD_SK"),
    F.col("Lnk_IdsEdwMbrMcareEvtAuditDExtr_InABC.EFF_DT_SK").alias("EFF_DT_SK"),
    F.col("Lnk_IdsEdwMbrMcareEvtAuditDExtr_InABC.TERM_DT_SK").alias("TERM_DT_SK"),
    F.col("Lnk_IdsEdwMbrMcareEvtAuditDExtr_InABC.SRC_SYS_CRT_DT").alias("SRC_SYS_CRT_DT"),
    F.col("Lnk_IdsEdwMbrMcareEvtAuditDExtr_InABC.MCARE_EVT_ADTNL_EFF_DT").alias("MCARE_EVT_ADTNL_EFF_DT"),
    F.col("Lnk_IdsEdwMbrMcareEvtAuditDExtr_InABC.MCARE_EVT_ADTNL_TERM_DT").alias("MCARE_EVT_ADTNL_TERM_DT"),
    F.col("Lnk_IdsEdwMbrMcareEvtAuditDExtr_InABC.MBR_RESDNC_ST_NM").alias("MBR_RESDNC_ST_NM"),
    F.col("Lnk_IdsEdwMbrMcareEvtAuditDExtr_InABC.MBR_RESDNC_CNTY_NM").alias("MBR_RESDNC_CNTY_NM"),
    F.col("Lnk_IdsEdwMbrMcareEvtAuditDExtr_InABC.MBR_MCARE_ID").alias("MBR_MCARE_ID"),
    F.col("Lnk_IdsEdwMbrMcareEvtAuditDExtr_InABC.BILL_GRP_UNIQ_KEY").alias("BILL_GRP_UNIQ_KEY"),
    F.col("Lnk_IdsEdwMbrMcareEvtAuditDExtr_InABC.MCARE_CNTR_ID").alias("MCARE_CNTR_ID"),
    F.col("Lnk_IdsEdwMbrMcareEvtAuditDExtr_InABC.PART_A_RISK_ADJ_FCTR").alias("PART_A_RISK_ADJ_FCTR"),
    F.col("Lnk_IdsEdwMbrMcareEvtAuditDExtr_InABC.PART_B_RISK_ADJ_FCTR").alias("PART_B_RISK_ADJ_FCTR"),
    F.col("Lnk_IdsEdwMbrMcareEvtAuditDExtr_InABC.SGNTR_DT").alias("SGNTR_DT"),
    F.col("Lnk_IdsEdwMbrMcareEvtAuditDExtr_InABC.MCARE_ELECTN_TYP_ID").alias("MCARE_ELECTN_TYP_ID"),
    F.col("Lnk_IdsEdwMbrMcareEvtAuditDExtr_InABC.PLN_BNF_PCKG_ID").alias("PLN_BNF_PCKG_ID"),
    F.col("Lnk_IdsEdwMbrMcareEvtAuditDExtr_InABC.SEG_ID").alias("SEG_ID"),
    F.col("Lnk_IdsEdwMbrMcareEvtAuditDExtr_InABC.PART_D_RISK_ADJ_FCTR").alias("PART_D_RISK_ADJ_FCTR"),
    F.col("Lnk_IdsEdwMbrMcareEvtAuditDExtr_InABC.RISK_ADJ_FCTR_TYP_ID").alias("RISK_ADJ_FCTR_TYP_ID"),
    F.col("Lnk_IdsEdwMbrMcareEvtAuditDExtr_InABC.PRM_WTHLD_OPT_ID").alias("PRM_WTHLD_OPT_ID"),
    F.col("Lnk_IdsEdwMbrMcareEvtAuditDExtr_InABC.PART_C_PRM_AMT").alias("PART_C_PRM_AMT"),
    F.col("Lnk_IdsEdwMbrMcareEvtAuditDExtr_InABC.PART_D_PRM_AMT").alias("PART_D_PRM_AMT"),
    F.col("Lnk_IdsEdwMbrMcareEvtAuditDExtr_InABC.PRIOR_COM_OVRD_ID").alias("PRIOR_COM_OVRD_ID"),
    F.col("Lnk_IdsEdwMbrMcareEvtAuditDExtr_InABC.ENR_SRC_ID").alias("ENR_SRC_ID"),
    F.col("Lnk_IdsEdwMbrMcareEvtAuditDExtr_InABC.UNCOV_MO_CT").alias("UNCOV_MO_CT"),
    F.col("Lnk_IdsEdwMbrMcareEvtAuditDExtr_InABC.PART_D_ID").alias("PART_D_ID"),
    F.col("Lnk_IdsEdwMbrMcareEvtAuditDExtr_InABC.PART_D_GRP_ID").alias("PART_D_GRP_ID"),
    F.col("Lnk_IdsEdwMbrMcareEvtAuditDExtr_InABC.PART_D_RX_BIN_ID").alias("PART_D_RX_BIN_ID"),
    F.col("Lnk_IdsEdwMbrMcareEvtAuditDExtr_InABC.PART_D_RX_PCN_ID").alias("PART_D_RX_PCN_ID"),
    F.col("Lnk_IdsEdwMbrMcareEvtAuditDExtr_InABC.SEC_DRUG_INSUR_IN").alias("SEC_DRUG_INSUR_IN"),
    F.col("Lnk_IdsEdwMbrMcareEvtAuditDExtr_InABC.SEC_DRUG_INSUR_ID").alias("SEC_DRUG_INSUR_ID"),
    F.col("Lnk_IdsEdwMbrMcareEvtAuditDExtr_InABC.SEC_DRUG_INSUR_GRP_ID").alias("SEC_DRUG_INSUR_GRP_ID"),
    F.col("Lnk_IdsEdwMbrMcareEvtAuditDExtr_InABC.SEC_DRUG_INSUR_BIN_ID").alias("SEC_DRUG_INSUR_BIN_ID"),
    F.col("Lnk_IdsEdwMbrMcareEvtAuditDExtr_InABC.SEC_DRUG_INSUR_PCN_ID").alias("SEC_DRUG_INSUR_PCN_ID"),
    F.col("Lnk_IdsEdwMbrMcareEvtAuditDExtr_InABC.PART_D_SBSDY_ID").alias("PART_D_SBSDY_ID"),
    F.col("Lnk_IdsEdwMbrMcareEvtAuditDExtr_InABC.COPAY_CAT_ID").alias("COPAY_CAT_ID"),
    F.col("Lnk_IdsEdwMbrMcareEvtAuditDExtr_InABC.PART_D_LOW_INCM_PRM_SBSDY_AMT").alias("PART_D_LOW_INCM_PRM_SBSDY_AMT"),
    F.col("Lnk_IdsEdwMbrMcareEvtAuditDExtr_InABC.PART_D_LATE_ENR_PNLTY_AMT").alias("PART_D_LATE_ENR_PNLTY_AMT"),
    F.col("Lnk_IdsEdwMbrMcareEvtAuditDExtr_InABC.PART_D_LATE_ENR_PNLTY_WAIVED_AMT").alias("PART_D_LATE_ENR_PNLTY_WAIVED_AMT"),
    F.col("Lnk_IdsEdwMbrMcareEvtAuditDExtr_InABC.PART_D_LATE_ENR_PNLTY_SBSDY_AMT").alias("PART_D_LATE_ENR_PNLTY_SBSDY_AMT"),
    F.col("Lnk_IdsEdwMbrMcareEvtAuditDExtr_InABC.AGED_DSBLD_MCARE_SEC_PAYER_STTUS_ID").alias("AGED_DSBLD_MCARE_SEC_PAYER_STTUS_ID"),
    F.col("Lnk_IdsEdwMbrMcareEvtAuditDExtr_InABC.PART_D_RISK_ADJ_FCTR_TYP_ID").alias("PART_D_RISK_ADJ_FCTR_TYP_ID"),
    F.col("Lnk_IdsEdwMbrMcareEvtAuditDExtr_InABC.IC_MDL_IN").alias("IC_MDL_IN"),
    F.col("Lnk_IdsEdwMbrMcareEvtAuditDExtr_InABC.IC_MDL_BNF_STTUS_TX").alias("IC_MDL_BNF_STTUS_TX"),
    F.col("Lnk_IdsEdwMbrMcareEvtAuditDExtr_InABC.IC_MDL_END_DT_RSN_TX").alias("IC_MDL_END_DT_RSN_TX"),
    F.col("Lnk_IdsEdwMbrMcareEvtAuditDExtr_InABC.MBR_MCARE_EVT_ATCHMT_DTM").alias("MBR_MCARE_EVT_ATCHMT_DTM"),
    F.col("ActnCd.TRGT_CD").alias("MBR_MCARE_EVT_ACTN_CD"),
    F.col("ActnCd.TRGT_CD_NM").alias("MBR_MCARE_EVT_ACTN_CD_NM"),
    F.col("EvtCd.TRGT_CD").alias("MBR_MCARE_EVT_CD"),
    F.col("EvtCd.TRGT_CD_NM").alias("MBR_MCARE_EVT_CD_NM")
)

df_xmf_businessLogic = df_lkp_FKeys.select(
    F.col("MBR_MCARE_EVT_AUDIT_SK").alias("MBR_MCARE_EVT_AUDIT_SK"),
    F.col("MBR_MCARE_EVT_ROW_AUDIT_ID").alias("MBR_MCARE_EVT_AUDIT_ROW_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("SRC_SYS_CRT_DT_SK").alias("SRC_SYS_CRT_DT_SK"),
    F.when(trim(F.col("MBR_MCARE_EVT_ACTN_CD")) == "", F.lit("UNK"))
     .otherwise(F.col("MBR_MCARE_EVT_ACTN_CD"))
     .alias("MBR_MCARE_EVT_ACTN_CD"),
    F.when(trim(F.col("MBR_MCARE_EVT_ACTN_CD_NM")) == "", F.lit("UNK"))
     .otherwise(F.col("MBR_MCARE_EVT_ACTN_CD_NM"))
     .alias("MBR_MCARE_EVT_ACTN_NM"),
    F.col("MBR_SK").alias("MBR_SK"),
    F.col("SRC_SYS_CRT_USER_SK").alias("SRC_SYS_CRT_USER_SK"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.when(trim(F.col("MBR_MCARE_EVT_CD")) == "", F.lit("UNK"))
     .otherwise(F.col("MBR_MCARE_EVT_CD"))
     .alias("MBR_MCARE_EVT_CD"),
    F.when(trim(F.col("MBR_MCARE_EVT_CD_NM")) == "", F.lit("UNK"))
     .otherwise(F.col("MBR_MCARE_EVT_CD_NM"))
     .alias("MBR_MCARE_EVT_NM"),
    F.col("EFF_DT_SK").alias("MBR_MCARE_EVT_EFF_DT_SK"),
    F.col("TERM_DT_SK").alias("MBR_MCARE_EVT_TERM_DT_SK"),
    TimestampToDate(F.col("SRC_SYS_CRT_DT")).alias("SRC_SYS_CRT_DT"),
    TimestampToDate(F.col("MCARE_EVT_ADTNL_EFF_DT")).alias("MCARE_EVT_ADTNL_EFF_DT"),
    TimestampToDate(F.col("MCARE_EVT_ADTNL_TERM_DT")).alias("MCARE_EVT_ADTNL_TERM_DT"),
    F.col("MBR_RESDNC_ST_NM").alias("MBR_RESDNC_ST_NM"),
    F.col("MBR_RESDNC_CNTY_NM").alias("MBR_RESDNC_CNTY_NM"),
    F.col("MBR_MCARE_ID").alias("MBR_MCARE_ID"),
    F.col("BILL_GRP_UNIQ_KEY").alias("BILL_GRP_UNIQ_KEY"),
    F.col("MCARE_CNTR_ID").alias("MCARE_CNTR_ID"),
    F.col("PART_A_RISK_ADJ_FCTR").alias("PART_A_RISK_ADJ_FCTR"),
    F.col("PART_B_RISK_ADJ_FCTR").alias("PART_B_RISK_ADJ_FCTR"),
    TimestampToDate(F.col("SGNTR_DT")).alias("SGNTR_DT"),
    F.col("MCARE_ELECTN_TYP_ID").alias("MCARE_ELECTN_TYP_ID"),
    F.col("PLN_BNF_PCKG_ID").alias("PLN_BNF_PCKG_ID"),
    F.col("SEG_ID").alias("SEG_ID"),
    F.col("PART_D_RISK_ADJ_FCTR").alias("PART_D_RISK_ADJ_FCTR"),
    F.col("RISK_ADJ_FCTR_TYP_ID").alias("RISK_ADJ_FCTR_TYP_ID"),
    F.col("PRM_WTHLD_OPT_ID").alias("PRM_WTHLD_OPT_ID"),
    F.col("PART_C_PRM_AMT").alias("PART_C_PRM_AMT"),
    F.col("PART_D_PRM_AMT").alias("PART_D_PRM_AMT"),
    F.col("PRIOR_COM_OVRD_ID").alias("PRIOR_COM_OVRD_ID"),
    F.col("ENR_SRC_ID").alias("ENR_SRC_ID"),
    F.col("UNCOV_MO_CT").alias("UNCOV_MO_CT"),
    F.col("PART_D_ID").alias("PART_D_ID"),
    F.col("PART_D_GRP_ID").alias("PART_D_GRP_ID"),
    F.col("PART_D_RX_BIN_ID").alias("PART_D_RX_BIN_ID"),
    F.col("PART_D_RX_PCN_ID").alias("PART_D_RX_PCN_ID"),
    F.col("SEC_DRUG_INSUR_IN").alias("SEC_DRUG_INSUR_IN"),
    F.col("SEC_DRUG_INSUR_ID").alias("SEC_DRUG_INSUR_ID"),
    F.col("SEC_DRUG_INSUR_GRP_ID").alias("SEC_DRUG_INSUR_GRP_ID"),
    F.col("SEC_DRUG_INSUR_BIN_ID").alias("SEC_DRUG_INSUR_BIN_ID"),
    F.col("SEC_DRUG_INSUR_PCN_ID").alias("SEC_DRUG_INSUR_PCN_ID"),
    F.col("PART_D_SBSDY_ID").alias("PART_D_SBSDY_ID"),
    F.col("COPAY_CAT_ID").alias("COPAY_CAT_ID"),
    F.col("PART_D_LOW_INCM_PRM_SBSDY_AMT").alias("PART_D_LOW_INCM_PRM_SBSDY_AMT"),
    F.col("PART_D_LATE_ENR_PNLTY_AMT").alias("PART_D_LATE_ENR_PNLTY_AMT"),
    F.col("PART_D_LATE_ENR_PNLTY_WAIVED_AMT").alias("PART_D_LATE_ENR_PNLTY_WAIVED_AMT"),
    F.col("PART_D_LATE_ENR_PNLTY_SBSDY_AMT").alias("PART_D_LATE_ENR_PNLTY_SBSDY_AMT"),
    F.col("AGED_DSBLD_MCARE_SEC_PAYER_STTUS_ID").alias("AGED_DSBLD_MCARE_SEC_PAYER_STTUS_ID"),
    F.col("PART_D_RISK_ADJ_FCTR_TYP_ID").alias("PART_D_RISK_ADJ_FCTR_TYP_ID"),
    F.col("IC_MDL_IN").alias("IC_MDL_IN"),
    F.col("IC_MDL_BNF_STTUS_TX").alias("IC_MDL_BNF_STTUS_TX"),
    F.col("IC_MDL_END_DT_RSN_TX").alias("IC_MDL_END_DT_RSN_TX"),
    F.col("MBR_MCARE_EVT_ATCHMT_DTM").alias("MBR_MCARE_EVT_ATCHMT_DTM"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("MBR_MCARE_EVT_ACTN_CD_SK").alias("MBR_MCARE_EVT_ACTN_CD_SK"),
    F.col("MBR_MCARE_EVT_CD_SK").alias("MBR_MCARE_EVT_CD_SK")
)

df_final = df_xmf_businessLogic.select(
    F.col("MBR_MCARE_EVT_AUDIT_SK").alias("MBR_MCARE_EVT_AUDIT_SK"),
    F.col("MBR_MCARE_EVT_AUDIT_ROW_ID").alias("MBR_MCARE_EVT_AUDIT_ROW_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.col("SRC_SYS_CRT_DT_SK"), 10, " ").alias("SRC_SYS_CRT_DT_SK"),
    F.col("MBR_MCARE_EVT_ACTN_CD").alias("MBR_MCARE_EVT_ACTN_CD"),
    F.col("MBR_MCARE_EVT_ACTN_NM").alias("MBR_MCARE_EVT_ACTN_NM"),
    F.col("MBR_SK").alias("MBR_SK"),
    F.col("SRC_SYS_CRT_USER_SK").alias("SRC_SYS_CRT_USER_SK"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("MBR_MCARE_EVT_CD").alias("MBR_MCARE_EVT_CD"),
    F.col("MBR_MCARE_EVT_NM").alias("MBR_MCARE_EVT_NM"),
    F.rpad(F.col("MBR_MCARE_EVT_EFF_DT_SK"), 10, " ").alias("MBR_MCARE_EVT_EFF_DT_SK"),
    F.rpad(F.col("MBR_MCARE_EVT_TERM_DT_SK"), 10, " ").alias("MBR_MCARE_EVT_TERM_DT_SK"),
    F.col("SRC_SYS_CRT_DT").alias("SRC_SYS_CRT_DT"),
    F.col("MCARE_EVT_ADTNL_EFF_DT").alias("MCARE_EVT_ADTNL_EFF_DT"),
    F.col("MCARE_EVT_ADTNL_TERM_DT").alias("MCARE_EVT_ADTNL_TERM_DT"),
    F.col("MBR_RESDNC_ST_NM").alias("MBR_RESDNC_ST_NM"),
    F.col("MBR_RESDNC_CNTY_NM").alias("MBR_RESDNC_CNTY_NM"),
    F.col("MBR_MCARE_ID").alias("MBR_MCARE_ID"),
    F.col("BILL_GRP_UNIQ_KEY").alias("BILL_GRP_UNIQ_KEY"),
    F.col("MCARE_CNTR_ID").alias("MCARE_CNTR_ID"),
    F.col("PART_A_RISK_ADJ_FCTR").alias("PART_A_RISK_ADJ_FCTR"),
    F.col("PART_B_RISK_ADJ_FCTR").alias("PART_B_RISK_ADJ_FCTR"),
    F.col("SGNTR_DT").alias("SGNTR_DT"),
    F.col("MCARE_ELECTN_TYP_ID").alias("MCARE_ELECTN_TYP_ID"),
    F.col("PLN_BNF_PCKG_ID").alias("PLN_BNF_PCKG_ID"),
    F.col("SEG_ID").alias("SEG_ID"),
    F.col("PART_D_RISK_ADJ_FCTR").alias("PART_D_RISK_ADJ_FCTR"),
    F.col("RISK_ADJ_FCTR_TYP_ID").alias("RISK_ADJ_FCTR_TYP_ID"),
    F.col("PRM_WTHLD_OPT_ID").alias("PRM_WTHLD_OPT_ID"),
    F.col("PART_C_PRM_AMT").alias("PART_C_PRM_AMT"),
    F.col("PART_D_PRM_AMT").alias("PART_D_PRM_AMT"),
    F.col("PRIOR_COM_OVRD_ID").alias("PRIOR_COM_OVRD_ID"),
    F.col("ENR_SRC_ID").alias("ENR_SRC_ID"),
    F.col("UNCOV_MO_CT").alias("UNCOV_MO_CT"),
    F.col("PART_D_ID").alias("PART_D_ID"),
    F.col("PART_D_GRP_ID").alias("PART_D_GRP_ID"),
    F.col("PART_D_RX_BIN_ID").alias("PART_D_RX_BIN_ID"),
    F.col("PART_D_RX_PCN_ID").alias("PART_D_RX_PCN_ID"),
    F.rpad(F.col("SEC_DRUG_INSUR_IN"), 1, " ").alias("SEC_DRUG_INSUR_IN"),
    F.col("SEC_DRUG_INSUR_ID").alias("SEC_DRUG_INSUR_ID"),
    F.col("SEC_DRUG_INSUR_GRP_ID").alias("SEC_DRUG_INSUR_GRP_ID"),
    F.col("SEC_DRUG_INSUR_BIN_ID").alias("SEC_DRUG_INSUR_BIN_ID"),
    F.col("SEC_DRUG_INSUR_PCN_ID").alias("SEC_DRUG_INSUR_PCN_ID"),
    F.col("PART_D_SBSDY_ID").alias("PART_D_SBSDY_ID"),
    F.col("COPAY_CAT_ID").alias("COPAY_CAT_ID"),
    F.col("PART_D_LOW_INCM_PRM_SBSDY_AMT").alias("PART_D_LOW_INCM_PRM_SBSDY_AMT"),
    F.col("PART_D_LATE_ENR_PNLTY_AMT").alias("PART_D_LATE_ENR_PNLTY_AMT"),
    F.col("PART_D_LATE_ENR_PNLTY_WAIVED_AMT").alias("PART_D_LATE_ENR_PNLTY_WAIVED_AMT"),
    F.col("PART_D_LATE_ENR_PNLTY_SBSDY_AMT").alias("PART_D_LATE_ENR_PNLTY_SBSDY_AMT"),
    F.col("AGED_DSBLD_MCARE_SEC_PAYER_STTUS_ID").alias("AGED_DSBLD_MCARE_SEC_PAYER_STTUS_ID"),
    F.col("PART_D_RISK_ADJ_FCTR_TYP_ID").alias("PART_D_RISK_ADJ_FCTR_TYP_ID"),
    F.rpad(F.col("IC_MDL_IN"), 1, " ").alias("IC_MDL_IN"),
    F.col("IC_MDL_BNF_STTUS_TX").alias("IC_MDL_BNF_STTUS_TX"),
    F.col("IC_MDL_END_DT_RSN_TX").alias("IC_MDL_END_DT_RSN_TX"),
    F.col("MBR_MCARE_EVT_ATCHMT_DTM").alias("MBR_MCARE_EVT_ATCHMT_DTM"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("MBR_MCARE_EVT_ACTN_CD_SK").alias("MBR_MCARE_EVT_ACTN_CD_SK"),
    F.col("MBR_MCARE_EVT_CD_SK").alias("MBR_MCARE_EVT_CD_SK")
)

write_files(
    df_final,
    f"{adls_path}/load/MBR_MCARE_EVT_AUDIT_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)