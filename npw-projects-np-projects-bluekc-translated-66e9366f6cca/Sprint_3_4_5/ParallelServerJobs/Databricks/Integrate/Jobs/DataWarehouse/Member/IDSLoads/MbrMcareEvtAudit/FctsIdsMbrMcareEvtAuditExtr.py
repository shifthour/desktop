# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2016 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC         
# MAGIC PROCESSING: Extract job for MBR_MCARE_EVT_AUDIT table
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer               Date                         Change Description                                                                           Project #          Development Project          Code Reviewer          Date Reviewed  
# MAGIC ------------------             ----------------------------     -----------------------------------------------------------------------                                   ----------------         ------------------------------------       ----------------------------      ----------------
# MAGIC Kalyan Neelam         2020-10-05               Original Prgramming                                                                                                    IntegrateDev2                  Reddy Sanam            2020-10-05
# MAGIC 
# MAGIC Prabhu ES               2022-04-11                S2S MSSQL ODBC conn params added                                                                    IntegrateDev5                   Kalyan Neelam          2022-06-10


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import *
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

RunID = get_widget_value('RunID','100')
LastRunDt = get_widget_value('LastRunDt','2020-10-01')
SrcSysCdSk = get_widget_value('SrcSysCdSk','1581')
RunIDTimeStamp = get_widget_value('RunIDTimeStamp','')
SrcSysCd = get_widget_value('SrcSysCd','FACETS')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
FacetsAuditOwner = get_widget_value('FacetsAuditOwner','')
facetsaudit_secret_name = get_widget_value('facetsaudit_secret_name','')

jdbc_url, jdbc_props = get_db_config(facets_secret_name)
df_CMC_MCTR_CD_TRANS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        "SELECT MCTR_ENTITY, MCTR_TYPE, MCTR_VALUE, MCTR_DESC, MCTR_FILTER, MCTR_VALUE_SIZE, MCTR_SORT, MCTR_LOCK_TOKEN "
        + "FROM " + FacetsOwner + ".CMC_MCTR_CD_TRANS"
    )
    .load()
)

df_filter_16_Resdnc_St_Nm = (
    df_CMC_MCTR_CD_TRANS
    .filter((F.col("MCTR_ENTITY") == "MCST") & (F.col("MCTR_TYPE") == "ST"))
    .select(
        F.col("MCTR_VALUE").alias("MCTR_VALUE"),
        F.col("MCTR_DESC").alias("MCTR_DESC")
    )
)
df_filter_16_Resdnc_Cnty_Nm = (
    df_CMC_MCTR_CD_TRANS
    .filter((F.col("MCTR_ENTITY") == "MCCT") & (F.col("MCTR_TYPE") == "CTY"))
    .select(
        F.col("MCTR_VALUE").alias("MCTR_VALUE"),
        F.col("MCTR_DESC").alias("MCTR_DESC")
    )
)
df_filter_16_Pckg_Id = (
    df_CMC_MCTR_CD_TRANS
    .filter((F.col("MCTR_ENTITY") == "MEMD") & (F.col("MCTR_TYPE") == "PBP"))
    .select(
        F.col("MCTR_VALUE").alias("MCTR_VALUE"),
        F.col("MCTR_DESC").alias("MCTR_DESC")
    )
)
df_filter_16_PartD_GrpId = (
    df_CMC_MCTR_CD_TRANS
    .filter((F.col("MCTR_ENTITY") == "MEMD") & (F.col("MCTR_TYPE") == "GRP"))
    .select(
        F.col("MCTR_VALUE").alias("MCTR_VALUE"),
        F.col("MCTR_DESC").alias("MCTR_DESC")
    )
)
df_filter_16_PartD_Rx_Bnf_Id = (
    df_CMC_MCTR_CD_TRANS
    .filter((F.col("MCTR_ENTITY") == "MEMD") & (F.col("MCTR_TYPE") == "BIN"))
    .select(
        F.col("MCTR_VALUE").alias("MCTR_VALUE"),
        F.col("MCTR_DESC").alias("MCTR_DESC")
    )
)
df_filter_16_PartD_Rx_Pcn_Id = (
    df_CMC_MCTR_CD_TRANS
    .filter((F.col("MCTR_ENTITY") == "MEMD") & (F.col("MCTR_TYPE") == "PCN"))
    .select(
        F.col("MCTR_VALUE").alias("MCTR_VALUE"),
        F.col("MCTR_DESC").alias("MCTR_DESC")
    )
)

jdbc_url_audit, jdbc_props_audit = get_db_config(facetsaudit_secret_name)
df_CMC_MEMD_MECR_DTL = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_audit)
    .options(**jdbc_props_audit)
    .option(
        "query",
        "SELECT MECR.MEME_CK, MECR.MEMD_EVENT_CD, MECR.MEMD_HCFA_EFF_DT, CAST(MECR.HIST_ROW_ID AS VARCHAR(20)) AS HIST_ROW_ID, "
        "MECR.HIST_CREATE_DTM, MECR.HIST_USUS_ID, MECR.HIST_LOG_ACT_CD, MECR.HIST_PHYS_ACT_CD, MECR.HIST_IMAGE_CD, MECR.HIST_TERM_ID, "
        "MECR.TXN1_ROW_ID, MECR.GRGR_CK, MECR.MEMD_HCFA_TERM_DT, MECR.MEMD_INPUT_DT, MECR.MEMD_EVENT_EFF_DT, MECR.MEMD_EVENT_TERM_DT, "
        "MECR.MEMD_MCTR_MCST, MECR.MEMD_MCTR_MCCT, MECR.MEME_HICN, MECR.BGBG_CK, MECR.MRAC_CAT, MECR.MEMD_RA_PRTA_FCTR, "
        "MECR.MEMD_RA_PRTB_FCTR, MECR.MEMD_SIG_DT, MECR.MEMD_ELECT_TYPE, MECR.MEMD_MCTR_PBP, MECR.MEMD_SEGMENT_ID, MECR.MEMD_RA_PRTD_FCTR, "
        "MECR.MEMD_RA_FCTR_TYPE, MECR.MEMD_PREM_WH_OPT, MECR.MEMD_PRTC_PREM, MECR.MEMD_PRTD_PREM, MECR.MEMD_PRIOR_COM_OVR, MECR.MEMD_ENRL_SOURCE, "
        "MECR.MEMD_UNCOV_MOS, MECR.MEMD_RX_ID, MECR.MEMD_MCTR_RX_GROUP, MECR.MEMD_MCTR_RXBIN, MECR.MEMD_MCTR_RXPCN, MECR.MEMD_COB_IND, "
        "MECR.MEMD_COB_RX_ID, MECR.MEMD_COB_RX_GROUP, MECR.MEMD_COB_RXBIN, MECR.MEMD_COB_RXPCN, MECR.MEMD_PARTD_SBSDY, MECR.MEMD_COPAY_CAT, "
        "MECR.MEMD_LICS_SBSDY, MECR.MEMD_LATE_PENALTY, MECR.MEMD_LATE_WAIV_AMT, MECR.MEMD_LATE_SBSDY, MECR.MEMD_MSP_CD, MECR.MEMD_RAD_FCTR_TYPE, "
        "MECR.MEMD_LOCK_TOKEN, MECR.ATXR_SOURCE_ID, MECR.MEMD_IC_FLAG_NVL, MECR.MEMD_IC_STS_NVL, MECR.MEMD_IC_TRSN_NVL, BIL.BGBG_ID "
        "FROM " + FacetsAuditOwner + ".CMC_MEMD_MECR_DETL MECR "
        "LEFT OUTER JOIN " + FacetsOwner + ".CMC_BGBG_BIL_GROUP BIL ON MECR.BGBG_CK = BIL.BGBG_CK "
        "WHERE MECR.HIST_CREATE_DTM > '" + LastRunDt + "'"
    )
    .load()
)

df_transform = df_CMC_MEMD_MECR_DTL.select(
    F.lit(0).alias("MBR_MCARE_EVT_AUDIT_SK"),
    trim(F.col("HIST_ROW_ID")).alias("MBR_MCARE_EVT_ROW_AUDIT_ID"),
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    TimestampToDate(F.col("HIST_CREATE_DTM")).alias("SRC_SYS_CRT_DT_SK"),
    trim(F.col("HIST_PHYS_ACT_CD")).alias("MBR_MCARE_EVT_ACTN_CD"),
    F.lit(0).alias("MBR_SK"),
    trim(F.col("HIST_USUS_ID")).alias("SRC_SYS_CRT_USER"),
    trim(F.col("MEME_CK")).alias("MBR_UNIQ_KEY"),
    trim(F.col("MEMD_EVENT_CD")).alias("MBR_MCARE_EVT_CD"),
    TimestampToDate(F.col("MEMD_HCFA_EFF_DT")).alias("EFF_DT_SK"),
    TimestampToDate(F.col("MEMD_HCFA_TERM_DT")).alias("TERM_DT_SK"),
    TimestampToDate(F.col("HIST_CREATE_DTM")).alias("SRC_SYS_CRT_DT"),
    TimestampToDate(F.col("MEMD_EVENT_EFF_DT")).alias("MCARE_EVT_ADTNL_EFF_DT"),
    TimestampToDate(F.col("MEMD_EVENT_TERM_DT")).alias("MCARE_EVT_ADTNL_TERM_DT"),
    trim(F.col("MEMD_MCTR_MCST")).alias("MBR_RESDNC_ST_NM"),
    trim(F.col("MEMD_MCTR_MCCT")).alias("MBR_RESDNC_CNTY_NM"),
    trim(F.col("MEME_HICN")).alias("MBR_MCARE_ID"),
    trim(F.col("BGBG_CK")).alias("BILL_GRP_UNIQ_KEY"),
    F.when(trim(F.col("BGBG_ID")) == "", F.lit("UNK")).otherwise(F.col("BGBG_ID")).alias("MCARE_CNTR_ID"),
    trim(F.col("MEMD_RA_PRTA_FCTR")).alias("PART_A_RISK_ADJ_FCTR"),
    trim(F.col("MEMD_RA_PRTB_FCTR")).alias("PART_B_RISK_ADJ_FCTR"),
    TimestampToDate(F.col("MEMD_SIG_DT")).alias("SGNTR_DT"),
    trim(F.col("MEMD_ELECT_TYPE")).alias("MCARE_ELECTN_TYP_ID"),
    trim(F.col("MEMD_MCTR_PBP")).alias("PLN_BNF_PCKG_ID"),
    trim(F.col("MEMD_SEGMENT_ID")).alias("SEG_ID"),
    trim(F.col("MEMD_RA_PRTD_FCTR")).alias("PART_D_RISK_ADJ_FCTR"),
    trim(F.col("MEMD_RA_FCTR_TYPE")).alias("RISK_ADJ_FCTR_TYP_ID"),
    trim(F.col("MEMD_PREM_WH_OPT")).alias("PRM_WTHLD_OPT_ID"),
    trim(F.col("MEMD_PRTC_PREM")).alias("PART_C_PRM_AMT"),
    trim(F.col("MEMD_PRTD_PREM")).alias("PART_D_PRM_AMT"),
    trim(F.col("MEMD_PRIOR_COM_OVR")).alias("PRIOR_COM_OVRD_ID"),
    trim(F.col("MEMD_ENRL_SOURCE")).alias("ENR_SRC_ID"),
    trim(F.col("MEMD_UNCOV_MOS")).alias("UNCOV_MO_CT"),
    trim(F.col("MEMD_RX_ID")).alias("PART_D_ID"),
    trim(F.col("MEMD_MCTR_RX_GROUP")).alias("PART_D_GRP_ID"),
    trim(F.col("MEMD_MCTR_RXBIN")).alias("PART_D_RX_BIN_ID"),
    trim(F.col("MEMD_MCTR_RXPCN")).alias("PART_D_RX_PCN_ID"),
    trim(F.col("MEMD_COB_IND")).alias("SEC_DRUG_INSUR_IN"),
    trim(F.col("MEMD_COB_RX_ID")).alias("SEC_DRUG_INSUR_ID"),
    trim(F.col("MEMD_COB_RX_GROUP")).alias("SEC_DRUG_INSUR_GRP_ID"),
    trim(F.col("MEMD_COB_RXBIN")).alias("SEC_DRUG_INSUR_BIN_ID"),
    trim(F.col("MEMD_COB_RXPCN")).alias("SEC_DRUG_INSUR_PCN_ID"),
    trim(F.col("MEMD_PARTD_SBSDY")).alias("PART_D_SBSDY_ID"),
    trim(F.col("MEMD_COPAY_CAT")).alias("COPAY_CAT_ID"),
    trim(F.col("MEMD_LICS_SBSDY")).alias("PART_D_LOW_INCM_PRM_SBSDY_AMT"),
    trim(F.col("MEMD_LATE_PENALTY")).alias("PART_D_LATE_ENR_PNLTY_AMT"),
    trim(F.col("MEMD_LATE_WAIV_AMT")).alias("PART_D_LATE_ENR_PNLTY_WAIVED_AMT"),
    trim(F.col("MEMD_LATE_SBSDY")).alias("PART_D_LATE_ENR_PNLTY_SBSDY_AMT"),
    trim(F.col("MEMD_MSP_CD")).alias("AGED_DSBLD_MCARE_SEC_PAYER_STTUS_ID"),
    trim(F.col("MEMD_RAD_FCTR_TYPE")).alias("PART_D_RISK_ADJ_FCTR_TYP_ID"),
    trim(F.col("MEMD_IC_FLAG_NVL")).alias("IC_MDL_IN"),
    trim(F.col("MEMD_IC_STS_NVL")).alias("IC_MDL_BNF_STTUS_TX"),
    trim(F.col("MEMD_IC_TRSN_NVL")).alias("IC_MDL_END_DT_RSN_TX"),
    F.expr('FORMAT.DATE.EE(ATXR_SOURCE_ID, "SYBASE", "TIMESTAMP", "DB2TIMESTAMP")').alias("MBR_MCARE_EVT_ATCHMT_DTM")
)

df_lookup_29 = (
    df_transform.alias("Dataset")
    .join(df_filter_16_Resdnc_St_Nm.alias("Resdnc_St_Nm"),
          F.col("Dataset.MBR_RESDNC_ST_NM") == F.col("Resdnc_St_Nm.MCTR_VALUE"),
          "left")
    .join(df_filter_16_Resdnc_Cnty_Nm.alias("Resdnc_Cnty_Nm"),
          F.col("Dataset.MBR_RESDNC_CNTY_NM") == F.col("Resdnc_Cnty_Nm.MCTR_VALUE"),
          "left")
    .join(df_filter_16_Pckg_Id.alias("Pckg_Id"),
          F.col("Dataset.PLN_BNF_PCKG_ID") == F.col("Pckg_Id.MCTR_VALUE"),
          "left")
    .join(df_filter_16_PartD_GrpId.alias("PartD_GrpId"),
          F.col("Dataset.PART_D_GRP_ID") == F.col("PartD_GrpId.MCTR_VALUE"),
          "left")
    .join(df_filter_16_PartD_Rx_Bnf_Id.alias("PartD_Rx_Bnf_Id"),
          F.col("Dataset.PART_D_RX_BIN_ID") == F.col("PartD_Rx_Bnf_Id.MCTR_VALUE"),
          "left")
    .join(df_filter_16_PartD_Rx_Pcn_Id.alias("PartD_Rx_Pcn_Id"),
          F.col("Dataset.PART_D_RX_PCN_ID") == F.col("PartD_Rx_Pcn_Id.MCTR_VALUE"),
          "left")
    .select(
        F.col("Dataset.MBR_MCARE_EVT_AUDIT_SK").alias("MBR_MCARE_EVT_AUDIT_SK"),
        F.col("Dataset.MBR_MCARE_EVT_ROW_AUDIT_ID").alias("MBR_MCARE_EVT_ROW_AUDIT_ID"),
        F.col("Dataset.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("Dataset.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("Dataset.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("Dataset.SRC_SYS_CRT_DT_SK").alias("SRC_SYS_CRT_DT_SK"),
        F.col("Dataset.MBR_MCARE_EVT_ACTN_CD").alias("MBR_MCARE_EVT_ACTN_CD"),
        F.col("Dataset.MBR_SK").alias("MBR_SK"),
        F.col("Dataset.SRC_SYS_CRT_USER").alias("SRC_SYS_CRT_USER"),
        F.col("Dataset.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.col("Dataset.MBR_MCARE_EVT_CD").alias("MBR_MCARE_EVT_CD"),
        F.col("Dataset.EFF_DT_SK").alias("EFF_DT_SK"),
        F.col("Dataset.TERM_DT_SK").alias("TERM_DT_SK"),
        F.col("Dataset.SRC_SYS_CRT_DT").alias("SRC_SYS_CRT_DT"),
        F.col("Dataset.MCARE_EVT_ADTNL_EFF_DT").alias("MCARE_EVT_ADTNL_EFF_DT"),
        F.col("Dataset.MCARE_EVT_ADTNL_TERM_DT").alias("MCARE_EVT_ADTNL_TERM_DT"),
        F.col("Resdnc_St_Nm.MCTR_DESC").alias("MBR_RESDNC_ST_NM"),
        F.col("Resdnc_Cnty_Nm.MCTR_DESC").alias("MBR_RESDNC_CNTY_NM"),
        F.col("Dataset.MBR_MCARE_ID").alias("MBR_MCARE_ID"),
        F.col("Dataset.BILL_GRP_UNIQ_KEY").alias("BILL_GRP_UNIQ_KEY"),
        F.col("Dataset.MCARE_CNTR_ID").alias("MCARE_CNTR_ID"),
        F.col("Dataset.PART_A_RISK_ADJ_FCTR").alias("PART_A_RISK_ADJ_FCTR"),
        F.col("Dataset.PART_B_RISK_ADJ_FCTR").alias("PART_B_RISK_ADJ_FCTR"),
        F.col("Dataset.SGNTR_DT").alias("SGNTR_DT"),
        F.col("Dataset.MCARE_ELECTN_TYP_ID").alias("MCARE_ELECTN_TYP_ID"),
        F.col("Pckg_Id.MCTR_DESC").alias("PLN_BNF_PCKG_ID"),
        F.col("Dataset.SEG_ID").alias("SEG_ID"),
        F.col("Dataset.PART_D_RISK_ADJ_FCTR").alias("PART_D_RISK_ADJ_FCTR"),
        F.col("Dataset.RISK_ADJ_FCTR_TYP_ID").alias("RISK_ADJ_FCTR_TYP_ID"),
        F.col("Dataset.PRM_WTHLD_OPT_ID").alias("PRM_WTHLD_OPT_ID"),
        F.col("Dataset.PART_C_PRM_AMT").alias("PART_C_PRM_AMT"),
        F.col("Dataset.PART_D_PRM_AMT").alias("PART_D_PRM_AMT"),
        F.col("Dataset.PRIOR_COM_OVRD_ID").alias("PRIOR_COM_OVRD_ID"),
        F.col("Dataset.ENR_SRC_ID").alias("ENR_SRC_ID"),
        F.col("Dataset.UNCOV_MO_CT").alias("UNCOV_MO_CT"),
        F.col("Dataset.PART_D_ID").alias("PART_D_ID"),
        F.col("PartD_GrpId.MCTR_DESC").alias("PART_D_GRP_ID"),
        F.col("PartD_Rx_Bnf_Id.MCTR_DESC").alias("PART_D_RX_BIN_ID"),
        F.col("PartD_Rx_Pcn_Id.MCTR_DESC").alias("PART_D_RX_PCN_ID"),
        F.col("Dataset.SEC_DRUG_INSUR_IN").alias("SEC_DRUG_INSUR_IN"),
        F.col("Dataset.SEC_DRUG_INSUR_ID").alias("SEC_DRUG_INSUR_ID"),
        F.col("Dataset.SEC_DRUG_INSUR_GRP_ID").alias("SEC_DRUG_INSUR_GRP_ID"),
        F.col("Dataset.SEC_DRUG_INSUR_BIN_ID").alias("SEC_DRUG_INSUR_BIN_ID"),
        F.col("Dataset.SEC_DRUG_INSUR_PCN_ID").alias("SEC_DRUG_INSUR_PCN_ID"),
        F.col("Dataset.PART_D_SBSDY_ID").alias("PART_D_SBSDY_ID"),
        F.col("Dataset.COPAY_CAT_ID").alias("COPAY_CAT_ID"),
        F.col("Dataset.PART_D_LOW_INCM_PRM_SBSDY_AMT").alias("PART_D_LOW_INCM_PRM_SBSDY_AMT"),
        F.col("Dataset.PART_D_LATE_ENR_PNLTY_AMT").alias("PART_D_LATE_ENR_PNLTY_AMT"),
        F.col("Dataset.PART_D_LATE_ENR_PNLTY_WAIVED_AMT").alias("PART_D_LATE_ENR_PNLTY_WAIVED_AMT"),
        F.col("Dataset.PART_D_LATE_ENR_PNLTY_SBSDY_AMT").alias("PART_D_LATE_ENR_PNLTY_SBSDY_AMT"),
        F.col("Dataset.AGED_DSBLD_MCARE_SEC_PAYER_STTUS_ID").alias("AGED_DSBLD_MCARE_SEC_PAYER_STTUS_ID"),
        F.col("Dataset.PART_D_RISK_ADJ_FCTR_TYP_ID").alias("PART_D_RISK_ADJ_FCTR_TYP_ID"),
        F.col("Dataset.IC_MDL_IN").alias("IC_MDL_IN"),
        F.col("Dataset.IC_MDL_BNF_STTUS_TX").alias("IC_MDL_BNF_STTUS_TX"),
        F.col("Dataset.IC_MDL_END_DT_RSN_TX").alias("IC_MDL_END_DT_RSN_TX"),
        F.col("Dataset.MBR_MCARE_EVT_ATCHMT_DTM").alias("MBR_MCARE_EVT_ATCHMT_DTM")
    )
)

df_transformer_13 = df_lookup_29.select(
    F.concat(F.col("MBR_MCARE_EVT_ROW_AUDIT_ID"), F.lit(";"), F.lit(SrcSysCd)).alias("PRI_NAT_KEY_STRING"),
    F.lit(RunIDTimeStamp).alias("FIRST_RECYCLE_TS"),
    F.col("MBR_MCARE_EVT_AUDIT_SK").alias("MBR_MCARE_EVT_AUDIT_SK"),
    F.col("MBR_MCARE_EVT_ROW_AUDIT_ID").alias("MBR_MCARE_EVT_ROW_AUDIT_ID"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("SRC_SYS_CRT_DT_SK").alias("SRC_SYS_CRT_DT_SK"),
    F.col("MBR_MCARE_EVT_ACTN_CD").alias("MBR_MCARE_EVT_ACTN_CD"),
    F.col("MBR_SK").alias("MBR_SK"),
    F.col("SRC_SYS_CRT_USER").alias("SRC_SYS_CRT_USER"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("MBR_MCARE_EVT_CD").alias("MBR_MCARE_EVT_CD"),
    F.col("EFF_DT_SK").alias("EFF_DT_SK"),
    F.col("TERM_DT_SK").alias("TERM_DT_SK"),
    F.col("SRC_SYS_CRT_DT").alias("SRC_SYS_CRT_DT"),
    F.col("MCARE_EVT_ADTNL_EFF_DT").alias("MCARE_EVT_ADTNL_EFF_DT"),
    F.col("MCARE_EVT_ADTNL_TERM_DT").alias("MCARE_EVT_ADTNL_TERM_DT"),
    F.when(F.col("MBR_RESDNC_ST_NM").isNull(), F.lit("UNK"))
     .otherwise(F.col("MBR_RESDNC_ST_NM").substr(1,35)).alias("MBR_RESDNC_ST_NM"),
    F.when(F.col("MBR_RESDNC_CNTY_NM").isNull(), F.lit("UNK"))
     .otherwise(F.col("MBR_RESDNC_CNTY_NM").substr(1,35)).alias("MBR_RESDNC_CNTY_NM"),
    F.col("MBR_MCARE_ID").alias("MBR_MCARE_ID"),
    F.col("BILL_GRP_UNIQ_KEY").alias("BILL_GRP_UNIQ_KEY"),
    F.col("MCARE_CNTR_ID").alias("MCARE_CNTR_ID"),
    F.col("PART_A_RISK_ADJ_FCTR").alias("PART_A_RISK_ADJ_FCTR"),
    F.col("PART_B_RISK_ADJ_FCTR").alias("PART_B_RISK_ADJ_FCTR"),
    F.col("SGNTR_DT").alias("SGNTR_DT"),
    F.col("MCARE_ELECTN_TYP_ID").alias("MCARE_ELECTN_TYP_ID"),
    F.when(F.col("PLN_BNF_PCKG_ID").isNull(), F.lit("UNK"))
     .otherwise(F.col("PLN_BNF_PCKG_ID").substr(1,20)).alias("PLN_BNF_PCKG_ID"),
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
    F.when(F.col("PART_D_GRP_ID").isNull(), F.lit("UNK"))
     .otherwise(F.col("PART_D_GRP_ID").substr(1,20)).alias("PART_D_GRP_ID"),
    F.when(F.col("PART_D_RX_BIN_ID").isNull(), F.lit("UNK"))
     .otherwise(F.col("PART_D_RX_BIN_ID").substr(1,20)).alias("PART_D_RX_BIN_ID"),
    F.when(F.col("PART_D_RX_PCN_ID").isNull(), F.lit("UNK"))
     .otherwise(F.col("PART_D_RX_PCN_ID").substr(1,20)).alias("PART_D_RX_PCN_ID"),
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
    F.col("MBR_MCARE_EVT_ATCHMT_DTM").alias("MBR_MCARE_EVT_ATCHMT_DTM")
)

df_ds_MBR_MCARE_EVT_AUDIT = df_transformer_13.select(
    F.col("PRI_NAT_KEY_STRING"),
    F.col("FIRST_RECYCLE_TS"),
    F.col("MBR_MCARE_EVT_AUDIT_SK"),
    F.col("MBR_MCARE_EVT_ROW_AUDIT_ID"),
    F.col("SRC_SYS_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("SRC_SYS_CRT_DT_SK"),
    F.col("MBR_MCARE_EVT_ACTN_CD"),
    F.col("MBR_SK"),
    F.col("SRC_SYS_CRT_USER"),
    F.col("MBR_UNIQ_KEY"),
    F.col("MBR_MCARE_EVT_CD"),
    F.col("EFF_DT_SK"),
    F.col("TERM_DT_SK"),
    F.col("SRC_SYS_CRT_DT"),
    F.col("MCARE_EVT_ADTNL_EFF_DT"),
    F.col("MCARE_EVT_ADTNL_TERM_DT"),
    F.rpad(F.col("MBR_RESDNC_ST_NM"), 35, " ").alias("MBR_RESDNC_ST_NM"),
    F.rpad(F.col("MBR_RESDNC_CNTY_NM"), 35, " ").alias("MBR_RESDNC_CNTY_NM"),
    F.col("MBR_MCARE_ID"),
    F.col("BILL_GRP_UNIQ_KEY"),
    F.col("MCARE_CNTR_ID"),
    F.col("PART_A_RISK_ADJ_FCTR"),
    F.col("PART_B_RISK_ADJ_FCTR"),
    F.col("SGNTR_DT"),
    F.col("MCARE_ELECTN_TYP_ID"),
    F.rpad(F.col("PLN_BNF_PCKG_ID"), 20, " ").alias("PLN_BNF_PCKG_ID"),
    F.col("SEG_ID"),
    F.col("PART_D_RISK_ADJ_FCTR"),
    F.col("RISK_ADJ_FCTR_TYP_ID"),
    F.col("PRM_WTHLD_OPT_ID"),
    F.col("PART_C_PRM_AMT"),
    F.col("PART_D_PRM_AMT"),
    F.col("PRIOR_COM_OVRD_ID"),
    F.col("ENR_SRC_ID"),
    F.col("UNCOV_MO_CT"),
    F.col("PART_D_ID"),
    F.rpad(F.col("PART_D_GRP_ID"), 20, " ").alias("PART_D_GRP_ID"),
    F.rpad(F.col("PART_D_RX_BIN_ID"), 20, " ").alias("PART_D_RX_BIN_ID"),
    F.rpad(F.col("PART_D_RX_PCN_ID"), 20, " ").alias("PART_D_RX_PCN_ID"),
    F.rpad(F.col("SEC_DRUG_INSUR_IN"), 1, " ").alias("SEC_DRUG_INSUR_IN"),
    F.col("SEC_DRUG_INSUR_ID"),
    F.col("SEC_DRUG_INSUR_GRP_ID"),
    F.col("SEC_DRUG_INSUR_BIN_ID"),
    F.col("SEC_DRUG_INSUR_PCN_ID"),
    F.col("PART_D_SBSDY_ID"),
    F.col("COPAY_CAT_ID"),
    F.col("PART_D_LOW_INCM_PRM_SBSDY_AMT"),
    F.col("PART_D_LATE_ENR_PNLTY_AMT"),
    F.col("PART_D_LATE_ENR_PNLTY_WAIVED_AMT"),
    F.col("PART_D_LATE_ENR_PNLTY_SBSDY_AMT"),
    F.col("AGED_DSBLD_MCARE_SEC_PAYER_STTUS_ID"),
    F.col("PART_D_RISK_ADJ_FCTR_TYP_ID"),
    F.rpad(F.col("IC_MDL_IN"), 1, " ").alias("IC_MDL_IN"),
    F.col("IC_MDL_BNF_STTUS_TX"),
    F.col("IC_MDL_END_DT_RSN_TX"),
    F.col("MBR_MCARE_EVT_ATCHMT_DTM")
)

write_files(
    df_ds_MBR_MCARE_EVT_AUDIT,
    f"{adls_path}/ds/MBR_MCARE_EVT_AUDIT.xfrm.{RunID}.parquet",
    delimiter=",",
    mode="overwrite",
    is_parquet=True,
    header=True,
    quote="\"",
    nullValue=None
)