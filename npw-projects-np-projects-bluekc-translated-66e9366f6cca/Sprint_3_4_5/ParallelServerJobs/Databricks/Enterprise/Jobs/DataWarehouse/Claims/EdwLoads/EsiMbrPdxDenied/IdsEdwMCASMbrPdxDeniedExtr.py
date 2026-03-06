# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                               DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT          DESCRIPTION                                                               ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     -----------------------    ------------------------------------------------- -                                   -----------------------------       ------------------------------       --------------------
# MAGIC Ravi Singh                08/02/2018           5796               Initital development                                                        EnterpriseDev1             Hugh Sisson                 2018-09-20
# MAGIC Peter Gichiri              11/14/2019           6131               PBM Replacement added the
# MAGIC                                                                                       IDSOPTUMRXRunCycle & IDSESIRunCycle params    EnterpriseDev1              Kalyan Neelam             2019-11-21

# MAGIC Write MBR_PDX_D_TRANS_F Data into a Sequential file for Load Job MBR_PDX_DENIED_TRANS_F Load.
# MAGIC Read all the Data from IDS MBR_PDX_DENIED_TRANS Table; Pull records based on LAST_UPDT_RUN_CYC_EXCTN_SK.
# MAGIC Add Defaults and Null Handling.
# MAGIC Lookup with ref MEDIA_TYP_CD,GNDR_CD, RELSHP_CD, RSPN_RSN_CD, RSPN_TYP_CD
# MAGIC Job Name: IdsEdwMCASMbrPdxDeniedExtr
# MAGIC 
# MAGIC Description: Claims denied data is extracted from IDS and loaded into the  EDW MBR_PDX_DENIED_TRANS_F Table
# MAGIC Called from: IdsEdwMbrPdxDeniedClaimCntl
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType, DateType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
IDSOPTUMRXRunCycle = get_widget_value('IDSOPTUMRXRunCycle','')
IDSESIRunCycle = get_widget_value('IDSESIRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# Stage: MBR_PDX_DENIED_TRANS
query_MBR_PDX_DENIED_TRANS = f"""
SELECT
  TRANS.MBR_PDX_DENIED_TRANS_SK,
  TRANS.MBR_UNIQ_KEY,
  TRANS.PDX_NTNL_PROV_ID,
  TRANS.TRANS_TYP_CD,
  TRANS.TRANS_DENIED_DT,
  TRANS.RX_NO,
  TRANS.PRCS_DT,
  TRANS.SRC_SYS_CD,
  TRANS.CRT_RUN_CYC_EXCTN_SK,
  TRANS.LAST_UPDT_RUN_CYC_EXCTN_SK,
  TRANS.GRP_SK,
  TRANS.MBR_SK,
  TRANS.NDC_SK,
  TRANS.PRSCRB_PROV_SK,
  TRANS.PROV_SPEC_CD_SK,
  TRANS.SRV_PROV_SK,
  TRANS.SUB_SK,
  TRANS.MEDIA_TYP_CD_SK,
  TRANS.MBR_GNDR_CD_SK,
  TRANS.MBR_RELSHP_CD_SK,
  TRANS.PDX_RSPN_RSN_CD_SK,
  TRANS.PDX_RSPN_TYP_CD_SK,
  TRANS.TRANS_TYP_CD_SK,
  TRANS.MAIL_ORDER_IN,
  TRANS.MBR_BRTH_DT,
  TRANS.SRC_SYS_CLM_RCVD_DT,
  TRANS.SRC_SYS_CLM_RCVD_TM,
  TRANS.SUB_BRTH_DT,
  TRANS.BILL_RX_DISPENSE_FEE_AMT,
  TRANS.BILL_RX_GROS_APRV_AMT,
  TRANS.BILL_RX_NET_CHK_AMT,
  TRANS.BILL_RX_PATN_PAY_AMT,
  TRANS.INGR_CST_ALW_AMT,
  TRANS.TRANS_MO_NO,
  TRANS.TRANS_YR_NO,
  TRANS.MBR_ID,
  TRANS.MBR_SFX_NO,
  TRANS.MBR_FIRST_NM,
  TRANS.MBR_LAST_NM,
  TRANS.MBR_SSN,
  TRANS.PDX_NM,
  TRANS.PDX_PHN_NO,
  TRANS.PHYS_DEA_NO,
  TRANS.PHYS_NTNL_PROV_ID,
  TRANS.PHYS_FIRST_NM,
  TRANS.PHYS_LAST_NM,
  TRANS.PHYS_ST_ADDR_LN,
  TRANS.PHYS_CITY_NM,
  TRANS.PHYS_ST_CD,
  TRANS.PHYS_POSTAL_CD,
  TRANS.RX_LABEL_TX,
  TRANS.SVC_PROV_NABP_NM,
  TRANS.SRC_SYS_CAR_ID,
  TRANS.SRC_SYS_CLNT_ORG_ID,
  TRANS.SRC_SYS_CLNT_ORG_NM,
  TRANS.SRC_SYS_CNTR_ID,
  TRANS.SRC_SYS_GRP_ID,
  TRANS.SUB_ID,
  TRANS.SUB_FIRST_NM,
  TRANS.SUB_LAST_NM
FROM {IDSOwner}.MBR_PDX_DENIED_TRANS TRANS,
     {IDSOwner}.CD_MPPNG CD
WHERE CD.TRGT_CD = TRANS.SRC_SYS_CD
  AND (
       (CD.TRGT_CD = 'OPTUMRX' AND TRANS.LAST_UPDT_RUN_CYC_EXCTN_SK >= {IDSOPTUMRXRunCycle})
       OR (CD.TRGT_CD = 'ESI' AND TRANS.LAST_UPDT_RUN_CYC_EXCTN_SK >= {IDSESIRunCycle})
      )
"""
df_MBR_PDX_DENIED_TRANS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_MBR_PDX_DENIED_TRANS)
    .load()
)

# Stage: db2_CD_MPPNG_Extr
query_db2_CD_MPPNG_Extr = f"""
SELECT
  CD_MPPNG_SK,
  COALESCE(TRGT_CD,'UNK') TRGT_CD,
  COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG
"""
df_db2_CD_MPPNG_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_db2_CD_MPPNG_Extr)
    .load()
)

# Stage: Cpy_cd_mpping -> multiple outputs
df_refMediaTypCd = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_refMbrGndrCd = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_refMbrRelshp = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_refPdxRSPNRsn = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_refPDXRSPNTypCd = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

# Stage: Lookup
df_Lookup = (
    df_MBR_PDX_DENIED_TRANS.alias("lnk_MbrPdxDeniedTrans")
    .join(
        df_refMbrGndrCd.alias("refMbrGndrCd"),
        F.expr("lnk_MbrPdxDeniedTrans.MBR_GNDR_CD_SK = refMbrGndrCd.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_refMbrRelshp.alias("refMbrRelshp"),
        F.expr("lnk_MbrPdxDeniedTrans.MBR_RELSHP_CD_SK = refMbrRelshp.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_refPdxRSPNRsn.alias("refPdxRSPNRsn"),
        F.expr("lnk_MbrPdxDeniedTrans.PDX_RSPN_RSN_CD_SK = refPdxRSPNRsn.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_refMediaTypCd.alias("refMediaTypCd"),
        F.expr("lnk_MbrPdxDeniedTrans.MEDIA_TYP_CD_SK = refMediaTypCd.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_refPDXRSPNTypCd.alias("refPDXRSPNTypCd"),
        F.expr("lnk_MbrPdxDeniedTrans.PDX_RSPN_TYP_CD_SK = refPDXRSPNTypCd.CD_MPPNG_SK"),
        "left"
    )
)

df_Lookup = df_Lookup.select(
    F.col("lnk_MbrPdxDeniedTrans.MBR_PDX_DENIED_TRANS_SK").alias("MBR_PDX_DENIED_TRANS_SK"),
    F.col("lnk_MbrPdxDeniedTrans.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("lnk_MbrPdxDeniedTrans.PDX_NTNL_PROV_ID").alias("PDX_NTNL_PROV_ID"),
    F.col("lnk_MbrPdxDeniedTrans.TRANS_TYP_CD").alias("TRANS_TYP_CD"),
    F.col("lnk_MbrPdxDeniedTrans.TRANS_DENIED_DT").alias("TRANS_DENIED_DT"),
    F.col("lnk_MbrPdxDeniedTrans.RX_NO").alias("RX_NO"),
    F.col("lnk_MbrPdxDeniedTrans.PRCS_DT").alias("PRCS_DT"),
    F.col("lnk_MbrPdxDeniedTrans.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_MbrPdxDeniedTrans.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_MbrPdxDeniedTrans.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_MbrPdxDeniedTrans.GRP_SK").alias("GRP_SK"),
    F.col("lnk_MbrPdxDeniedTrans.MBR_SK").alias("MBR_SK"),
    F.col("lnk_MbrPdxDeniedTrans.NDC_SK").alias("NDC_SK"),
    F.col("lnk_MbrPdxDeniedTrans.PRSCRB_PROV_SK").alias("PRSCRB_PROV_SK"),
    F.col("lnk_MbrPdxDeniedTrans.PROV_SPEC_CD_SK").alias("PROV_SPEC_CD_SK"),
    F.col("lnk_MbrPdxDeniedTrans.SRV_PROV_SK").alias("SRV_PROV_SK"),
    F.col("lnk_MbrPdxDeniedTrans.SUB_SK").alias("SUB_SK"),
    F.col("lnk_MbrPdxDeniedTrans.MEDIA_TYP_CD_SK").alias("MEDIA_TYP_CD_SK"),
    F.col("lnk_MbrPdxDeniedTrans.MBR_GNDR_CD_SK").alias("MBR_GNDR_CD_SK"),
    F.col("lnk_MbrPdxDeniedTrans.MBR_RELSHP_CD_SK").alias("MBR_RELSHP_CD_SK"),
    F.col("lnk_MbrPdxDeniedTrans.PDX_RSPN_RSN_CD_SK").alias("PDX_RSPN_RSN_CD_SK"),
    F.col("lnk_MbrPdxDeniedTrans.PDX_RSPN_TYP_CD_SK").alias("PDX_RSPN_TYP_CD_SK"),
    F.col("lnk_MbrPdxDeniedTrans.TRANS_TYP_CD_SK").alias("TRANS_TYP_CD_SK"),
    F.col("lnk_MbrPdxDeniedTrans.MAIL_ORDER_IN").alias("MAIL_ORDER_IN"),
    F.col("lnk_MbrPdxDeniedTrans.MBR_BRTH_DT").alias("MBR_BRTH_DT"),
    F.col("lnk_MbrPdxDeniedTrans.SRC_SYS_CLM_RCVD_DT").alias("SRC_SYS_CLM_RCVD_DT"),
    F.col("lnk_MbrPdxDeniedTrans.SRC_SYS_CLM_RCVD_TM").alias("SRC_SYS_CLM_RCVD_TM"),
    F.col("lnk_MbrPdxDeniedTrans.SUB_BRTH_DT").alias("SUB_BRTH_DT"),
    F.col("lnk_MbrPdxDeniedTrans.BILL_RX_DISPENSE_FEE_AMT").alias("BILL_RX_DISPENSE_FEE_AMT"),
    F.col("lnk_MbrPdxDeniedTrans.BILL_RX_GROS_APRV_AMT").alias("BILL_RX_GROS_APRV_AMT"),
    F.col("lnk_MbrPdxDeniedTrans.BILL_RX_NET_CHK_AMT").alias("BILL_RX_NET_CHK_AMT"),
    F.col("lnk_MbrPdxDeniedTrans.BILL_RX_PATN_PAY_AMT").alias("BILL_RX_PATN_PAY_AMT"),
    F.col("lnk_MbrPdxDeniedTrans.INGR_CST_ALW_AMT").alias("INGR_CST_ALW_AMT"),
    F.col("lnk_MbrPdxDeniedTrans.TRANS_MO_NO").alias("TRANS_MO_NO"),
    F.col("lnk_MbrPdxDeniedTrans.TRANS_YR_NO").alias("TRANS_YR_NO"),
    F.col("lnk_MbrPdxDeniedTrans.MBR_ID").alias("MBR_ID"),
    F.col("lnk_MbrPdxDeniedTrans.MBR_SFX_NO").alias("MBR_SFX_NO"),
    F.col("lnk_MbrPdxDeniedTrans.MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    F.col("lnk_MbrPdxDeniedTrans.MBR_LAST_NM").alias("MBR_LAST_NM"),
    F.col("lnk_MbrPdxDeniedTrans.MBR_SSN").alias("MBR_SSN"),
    F.col("lnk_MbrPdxDeniedTrans.PDX_NM").alias("PDX_NM"),
    F.col("lnk_MbrPdxDeniedTrans.PDX_PHN_NO").alias("PDX_PHN_NO"),
    F.col("lnk_MbrPdxDeniedTrans.PHYS_DEA_NO").alias("PHYS_DEA_NO"),
    F.col("lnk_MbrPdxDeniedTrans.PHYS_NTNL_PROV_ID").alias("PHYS_NTNL_PROV_ID"),
    F.col("lnk_MbrPdxDeniedTrans.PHYS_FIRST_NM").alias("PHYS_FIRST_NM"),
    F.col("lnk_MbrPdxDeniedTrans.PHYS_LAST_NM").alias("PHYS_LAST_NM"),
    F.col("lnk_MbrPdxDeniedTrans.PHYS_ST_ADDR_LN").alias("PHYS_ST_ADDR_LN"),
    F.col("lnk_MbrPdxDeniedTrans.PHYS_CITY_NM").alias("PHYS_CITY_NM"),
    F.col("lnk_MbrPdxDeniedTrans.PHYS_ST_CD").alias("PHYS_ST_CD"),
    F.col("lnk_MbrPdxDeniedTrans.PHYS_POSTAL_CD").alias("PHYS_POSTAL_CD"),
    F.col("lnk_MbrPdxDeniedTrans.RX_LABEL_TX").alias("RX_LABEL_TX"),
    F.col("lnk_MbrPdxDeniedTrans.SVC_PROV_NABP_NM").alias("SVC_PROV_NABP_NM"),
    F.col("lnk_MbrPdxDeniedTrans.SRC_SYS_CAR_ID").alias("SRC_SYS_CAR_ID"),
    F.col("lnk_MbrPdxDeniedTrans.SRC_SYS_CLNT_ORG_ID").alias("SRC_SYS_CLNT_ORG_ID"),
    F.col("lnk_MbrPdxDeniedTrans.SRC_SYS_CLNT_ORG_NM").alias("SRC_SYS_CLNT_ORG_NM"),
    F.col("lnk_MbrPdxDeniedTrans.SRC_SYS_CNTR_ID").alias("SRC_SYS_CNTR_ID"),
    F.col("lnk_MbrPdxDeniedTrans.SRC_SYS_GRP_ID").alias("SRC_SYS_GRP_ID"),
    F.col("lnk_MbrPdxDeniedTrans.SUB_ID").alias("SUB_ID"),
    F.col("lnk_MbrPdxDeniedTrans.SUB_FIRST_NM").alias("SUB_FIRST_NM"),
    F.col("lnk_MbrPdxDeniedTrans.SUB_LAST_NM").alias("SUB_LAST_NM"),
    F.col("refMediaTypCd.TRGT_CD").alias("MED_TYP_TRGT_CD"),
    F.col("refMediaTypCd.TRGT_CD_NM").alias("MED_TYP_TRGT_CD_NM"),
    F.col("refMbrGndrCd.TRGT_CD").alias("MBR_GNDR_TRGT_CD"),
    F.col("refMbrGndrCd.TRGT_CD_NM").alias("MBR_GNDR_TRGT_CD_NM"),
    F.col("refMbrRelshp.TRGT_CD").alias("MBR_RELSHP_TRGT_CD"),
    F.col("refMbrRelshp.TRGT_CD_NM").alias("MBR_RELSHP_TRGT_CD_NM"),
    F.col("refPdxRSPNRsn.TRGT_CD").alias("PDX_RSPN_RSN_TRGT_CD"),
    F.col("refPdxRSPNRsn.TRGT_CD_NM").alias("PDX_RSPN_RSN_TRGT_CD_NM"),
    F.col("refPDXRSPNTypCd.TRGT_CD").alias("PDX_RSPN_TYP_TRGT_CD"),
    F.col("refPDXRSPNTypCd.TRGT_CD_NM").alias("PDX_RSPN_TYP_TRGT_CD_NM")
)

# Stage: xfrm_BusinessLogic
df_enriched = df_Lookup.select(
    F.col("MBR_PDX_DENIED_TRANS_SK").alias("MBR_PDX_DENIED_TRANS_SK"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("PDX_NTNL_PROV_ID").alias("PDX_NTNL_PROV_ID"),
    F.col("TRANS_TYP_CD").alias("TRANS_TYP_CD"),
    F.col("TRANS_DENIED_DT").alias("TRANS_DENIED_DT"),
    F.col("RX_NO").alias("RX_NO"),
    F.col("PRCS_DT").alias("PRCS_DT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("GRP_SK").alias("GRP_SK"),
    F.col("MBR_SK").alias("MBR_SK"),
    F.col("NDC_SK").alias("NDC_SK"),
    F.col("PRSCRB_PROV_SK").alias("PRESCRIBING_PROVIDER_SK"),
    F.col("PROV_SPEC_CD_SK").alias("PROV_SPEC_SK"),
    F.col("SRV_PROV_SK").alias("SERVICING_PROVIDER_SK"),
    F.col("SUB_SK").alias("SUB_SK"),
    F.col("MED_TYP_TRGT_CD").alias("MEDIA_TYP_CD"),
    F.col("MBR_GNDR_TRGT_CD").alias("MBR_GNDR_CD"),
    F.col("MBR_RELSHP_TRGT_CD").alias("MBR_RELSHP_CD"),
    F.col("PDX_RSPN_RSN_TRGT_CD").alias("PDX_RSPN_RSN_CD"),
    F.col("PDX_RSPN_TYP_TRGT_CD").alias("PDX_RSPN_TYP_CD"),
    F.col("MAIL_ORDER_IN").alias("MAIL_ORDER_IN"),
    F.col("MBR_BRTH_DT").alias("MBR_BRTH_DT"),
    F.col("SRC_SYS_CLM_RCVD_DT").alias("SRC_SYS_CLM_RCVD_DT"),
    F.col("SRC_SYS_CLM_RCVD_TM").alias("SRC_SYS_CLM_RCVD_TM"),
    F.col("SUB_BRTH_DT").alias("SUB_BRTH_DT"),
    F.col("BILL_RX_DISPENSE_FEE_AMT").alias("BILL_RX_DISPENSE_FEE_AMT"),
    F.col("BILL_RX_GROS_APRV_AMT").alias("BILL_RX_GROS_APRV_AMT"),
    F.col("BILL_RX_NET_CHK_AMT").alias("BILL_RX_NET_CHK_AMT"),
    F.col("BILL_RX_PATN_PAY_AMT").alias("BILL_RX_PATN_PAY_AMT"),
    F.col("INGR_CST_ALW_AMT").alias("INGR_CST_ALW_AMT"),
    F.col("TRANS_MO_NO").alias("TRANS_MO_NO"),
    F.col("TRANS_YR_NO").alias("TRANS_YR_NO"),
    F.col("MBR_ID").alias("MBR_ID"),
    F.col("MBR_SFX_NO").alias("MBR_SFX_NO"),
    F.col("MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    F.col("MBR_LAST_NM").alias("MBR_LAST_NM"),
    F.col("MBR_SSN").alias("MBR_SSN"),
    F.col("PDX_NM").alias("PDX_NM"),
    F.col("PDX_PHN_NO").alias("PDX_PHN_NO"),
    F.col("PHYS_DEA_NO").alias("PHYS_DEA_NO"),
    F.col("PHYS_NTNL_PROV_ID").alias("PHYS_NTNL_PROV_ID"),
    F.col("PHYS_FIRST_NM").alias("PHYS_FIRST_NM"),
    F.col("PHYS_LAST_NM").alias("PHYS_LAST_NM"),
    F.col("PHYS_ST_ADDR_LN").alias("PHYS_ST_ADDR_LN"),
    F.col("PHYS_CITY_NM").alias("PHYS_CITY_NM"),
    F.col("PHYS_ST_CD").alias("PHYS_ST_CD"),
    F.col("PHYS_POSTAL_CD").alias("PHYS_POSTAL_CD"),
    F.col("RX_LABEL_TX").alias("RX_LABEL_TX"),
    F.col("SVC_PROV_NABP_NM").alias("SVC_PROV_NABP_NM"),
    F.col("SRC_SYS_CAR_ID").alias("SRC_SYS_CAR_ID"),
    F.col("SRC_SYS_CLNT_ORG_ID").alias("SRC_SYS_CLNT_ORG_ID"),
    F.col("SRC_SYS_CLNT_ORG_NM").alias("SRC_SYS_CLNT_ORG_NM"),
    F.col("SRC_SYS_CNTR_ID").alias("SRC_SYS_CNTR_ID"),
    F.col("SRC_SYS_GRP_ID").alias("SRC_SYS_GRP_ID"),
    F.col("SUB_ID").alias("SUB_ID"),
    F.col("SUB_FIRST_NM").alias("SUB_FIRST_NM"),
    F.col("SUB_LAST_NM").alias("SUB_LAST_NM"),
    F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("MEDIA_TYP_CD_SK").alias("MEDIA_TYP_CD_SK"),
    F.col("MBR_GNDR_CD_SK").alias("MBR_GNDR_CD_SK"),
    F.col("MBR_RELSHP_CD_SK").alias("MBR_RELSHP_CD_SK"),
    F.col("PDX_RSPN_RSN_CD_SK").alias("PDX_RSPN_RSN_CD_SK"),
    F.col("PDX_RSPN_TYP_CD_SK").alias("PDX_RSPN_TYP_CD_SK"),
    F.col("TRANS_TYP_CD_SK").alias("TRANS_TYP_CD_SK")
)

df_enriched = df_enriched.withColumn(
    "CRT_RUN_CYC_EXCTN_DT_SK",
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
).withColumn(
    "MAIL_ORDER_IN",
    F.rpad(F.col("MAIL_ORDER_IN"), 1, " ")
).withColumn(
    "MBR_SFX_NO",
    F.rpad(F.col("MBR_SFX_NO"), 2, " ")
)

# Stage: seq_MbrPdxDeniedTrans_csv_load
write_files(
    df_enriched,
    f"{adls_path}/load/MBR_PDX_D_TRANS_F.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)