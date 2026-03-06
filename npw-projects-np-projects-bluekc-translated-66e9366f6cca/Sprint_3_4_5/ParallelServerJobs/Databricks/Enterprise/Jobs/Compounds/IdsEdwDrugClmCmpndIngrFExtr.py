# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC Â© Copyright 2020  Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC ZENA JOBNAME:  OPTUMRX_IDS_EDW_DRUG_CLM_CMPND_INGR_DAILY_000
# MAGIC 
# MAGIC CALLED BY:  IdsEdwDrugClmCmpndIngrFSeqCntl
# MAGIC 
# MAGIC JOB NAME:  IdsEdwDrugClmCmpndIngrFExtr
# MAGIC 
# MAGIC Description:  Job extracts from the DRUG_CLM_CMPND_INGR compound table from the IDS database for loading to EDW database.
# MAGIC 
# MAGIC Modifications:
# MAGIC 
# MAGIC Developer                         Date            Project/User Story             Change Description                                                             Development Project    Code Reviewer            Date Reviewed
# MAGIC -------------------------             -----------------   ----------------------------------------  --------------------------------------------------------------------------------------------   ----------------------------------   ---------------------------------   -------------------------
# MAGIC Velmani Kondappan      2020-10-23  F-209426/US217375         Initial Programming.                                                                 EnterpriseDev2     Jaideep Mankala              12/19/2020
# MAGIC Bill Schroeder\(9)     2021-09-22  US334499\(9) Removed the DOSE_* columns for new table layouts.\(9)    EnterpriseDev2           Jaideep Mankala              12/07/2021

# MAGIC This job extracts from the DRUG_CLM_CMPND_INGR compound table from the IDS database. Job is called from IdsEdwDrugClmCmpndIngrFSeqCntl Seq.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
OptumIdsRunCycle = get_widget_value('OptumIdsRunCycle','')
CurrRunCycleDate = get_widget_value('CurrRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

df_IDS_AHFS_TCC = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"SELECT COALESCE(AHFS_TCC.AHFS_TCC,'') as AHFS_TCC, AHFS_TCC.AHFS_TCC_SK, COALESCE(AHFS_TCC.AHFS_TCC_DESC,'') as AHFS_TCC_DESC FROM {IDSOwner}.AHFS_TCC AHFS_TCC"
    )
    .load()
)

df_IDS_Drug_Clm_Cmpnd_Ingr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"SELECT DRUG_CLM_CMPND_INGR_SK, CLM_ID, DRUG_CLM_CMPND_INGR_SEQ_NO, SRC_SYS_CD_SK, CRT_RUN_CYC_EXCTN_SK, LAST_UPDT_RUN_CYC_EXCTN_SK, DRUG_CLM_SK, NDC_SK, AHFS_TCC_SK, DRUG_3RD_PARTY_EXCPT_CD_SK, FDA_THRPTC_EQVLNT_CD_SK, GNRC_OVRD_CD_SK, INGR_STTUS_CD_SK, NDC_DRUG_ABUSE_CTL_CD_SK, NDC_DRUG_CLS_CD_SK, NDC_GNRC_NMD_DRUG_CD_SK, NDC_RTE_TYP_CD_SK, SUBMT_PROD_ID_QLFR_CD_SK, DRUG_MNTN_IN, MED_SUPL_IN, PRMT_EXCL_IN, PROD_RMBRMT_IN, RBT_MNFCTR_IN, SNGL_SRC_IN, UNIT_DOSE_IN, UNIT_OF_USE_IN, AWP_UNIT_CST_AMT, APRV_CST_SRC_CD_SK, APRV_CST_TYP_ID, APRV_CST_TYP_UNIT_CST_AMT, APRV_INGR_CST_AMT, APRV_PROF_SVC_FEE_PD_AMT, CALC_INGR_CST_AMT, CALC_PROF_SVC_FEE_PD_AMT, CLNT_CST_SRC_CD_SK, CLNT_CST_TYP_ID, CLNT_CST_TYP_UNIT_CST_AMT, CLNT_INGR_CST_AMT, CLNT_PROF_SVC_FEE_PD_AMT, CLNT_RATE_PCT, SUBMT_CMPND_CST_TYP_ID, SUBMT_CMPND_INGR_CST_AMT, SUBMT_CMPND_INGR_QTY, PDX_RATE_PCT, DRUG_METRIC_STRG_NO, INGR_MOD_CD_CT, CMPND_PROD_BRND_NM, CMPND_PROD_ID, DRUG_DSCRPTR_ID, DRUG_LABEL_NM, GNRC_NM_SH_DESC, GNRC_PROD_ID, INGR_UNIT_OF_MESR_TX, LBLR_NM, LBLR_NO, NDC_GCN_CD_TX, GCN_SEQ_NO FROM {IDSOwner}.DRUG_CLM_CMPND_INGR WHERE LAST_UPDT_RUN_CYC_EXCTN_SK >= {OptumIdsRunCycle}"
    )
    .load()
)

df_IDS_CD_MPPNG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"SELECT CD_MPPNG_SK, COALESCE(SRC_CD,'') as TRGT_CD, COALESCE(SRC_CD_NM,'') as TRGT_CD_NM FROM {IDSOwner}.CD_MPPNG"
    )
    .load()
)

df_Trans1_base = df_IDS_CD_MPPNG.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Trans1_SrcSysCdSk = df_Trans1_base.alias("SrcSysCdSk")
df_Trans1_Drug3rdPartyExcptCdSk = df_Trans1_base.alias("Drug3rdPartyExcptCdSk")
df_Trans1_FdaThrptcEqvlntCd = df_Trans1_base.alias("FdaThrptcEqvlntCd")
df_Trans1_GnrcOvrdCdSk = df_Trans1_base.alias("GnrcOvrdCdSk")
df_Trans1_IngrSttusCdSk = df_Trans1_base.alias("IngrSttusCdSk")
df_Trans1_NdcDrugAbuseCtlCdSk = df_Trans1_base.alias("NdcDrugAbuseCtlCdSk")
df_Trans1_NdcDrugClsCdSk = df_Trans1_base.alias("NdcDrugClsCdSk")
df_Trans1_NdcGnrcNmdDrugCdSk = df_Trans1_base.alias("NdcGnrcNmdDrugCdSk")
df_Trans1_NdcRteTypCdSk = df_Trans1_base.alias("NdcRteTypCdSk")
df_Trans1_SubmtProdIdQlfrCdSk = df_Trans1_base.alias("SubmtProdIdQlfrCdSk")
df_Trans1_AprvCstSrcCdSk = df_Trans1_base.alias("AprvCstSrcCdSk")
df_Trans1_ClntCstSrcCdSk = df_Trans1_base.alias("ClntCstSrcCdSk")

df_CdMppngLkp_intermediate = (
    df_IDS_Drug_Clm_Cmpnd_Ingr.alias("IDS_Out_Link")
    .join(
        df_Trans1_SrcSysCdSk,
        F.col("IDS_Out_Link.SRC_SYS_CD_SK") == F.col("SrcSysCdSk.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_Trans1_Drug3rdPartyExcptCdSk,
        F.col("IDS_Out_Link.DRUG_3RD_PARTY_EXCPT_CD_SK") == F.col("Drug3rdPartyExcptCdSk.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_Trans1_FdaThrptcEqvlntCd,
        F.col("IDS_Out_Link.FDA_THRPTC_EQVLNT_CD_SK") == F.col("FdaThrptcEqvlntCd.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_Trans1_GnrcOvrdCdSk,
        F.col("IDS_Out_Link.GNRC_OVRD_CD_SK") == F.col("GnrcOvrdCdSk.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_Trans1_IngrSttusCdSk,
        F.col("IDS_Out_Link.INGR_STTUS_CD_SK") == F.col("IngrSttusCdSk.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_Trans1_NdcDrugAbuseCtlCdSk,
        F.col("IDS_Out_Link.NDC_DRUG_ABUSE_CTL_CD_SK") == F.col("NdcDrugAbuseCtlCdSk.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_Trans1_NdcDrugClsCdSk,
        F.col("IDS_Out_Link.NDC_DRUG_CLS_CD_SK") == F.col("NdcDrugClsCdSk.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_Trans1_NdcGnrcNmdDrugCdSk,
        F.col("IDS_Out_Link.NDC_GNRC_NMD_DRUG_CD_SK") == F.col("NdcGnrcNmdDrugCdSk.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_Trans1_NdcRteTypCdSk,
        F.col("IDS_Out_Link.NDC_RTE_TYP_CD_SK") == F.col("NdcRteTypCdSk.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_Trans1_SubmtProdIdQlfrCdSk,
        F.col("IDS_Out_Link.SUBMT_PROD_ID_QLFR_CD_SK") == F.col("SubmtProdIdQlfrCdSk.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_Trans1_AprvCstSrcCdSk,
        F.col("IDS_Out_Link.APRV_CST_SRC_CD_SK") == F.col("AprvCstSrcCdSk.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_Trans1_ClntCstSrcCdSk,
        F.col("IDS_Out_Link.CLNT_CST_SRC_CD_SK") == F.col("ClntCstSrcCdSk.CD_MPPNG_SK"),
        "left",
    )
)

df_CdMppngLkp = df_CdMppngLkp_intermediate.select(
    F.col("IDS_Out_Link.DRUG_CLM_CMPND_INGR_SK").alias("DRUG_CLM_CMPND_INGR_SK"),
    F.col("IDS_Out_Link.CLM_ID").alias("CLM_ID"),
    F.col("IDS_Out_Link.DRUG_CLM_CMPND_INGR_SEQ_NO").alias("DRUG_CLM_CMPND_INGR_SEQ_NO"),
    F.col("SrcSysCdSk.TRGT_CD").alias("SRC_SYS_CD"),
    F.col("IDS_Out_Link.DRUG_CLM_SK").alias("CLM_SK"),
    F.col("IDS_Out_Link.NDC_SK").alias("NDC_SK"),
    F.col("Drug3rdPartyExcptCdSk.TRGT_CD").alias("DRUG_3RD_PARTY_EXCPT_CD"),
    F.col("Drug3rdPartyExcptCdSk.TRGT_CD_NM").alias("DRUG_3RD_PARTY_EXCPT_NM"),
    F.col("FdaThrptcEqvlntCd.TRGT_CD").alias("FDA_THRPTC_EQVLNT_CD"),
    F.col("FdaThrptcEqvlntCd.TRGT_CD_NM").alias("FDA_THRPTC_EQVLNT_NM"),
    F.col("GnrcOvrdCdSk.TRGT_CD").alias("GNRC_OVRD_CD"),
    F.col("GnrcOvrdCdSk.TRGT_CD_NM").alias("GNRC_OVRD_NM"),
    F.col("IngrSttusCdSk.TRGT_CD").alias("INGR_STTUS_CD"),
    F.col("IngrSttusCdSk.TRGT_CD_NM").alias("INGR_STTUS_NM"),
    F.col("NdcDrugAbuseCtlCdSk.TRGT_CD").alias("NDC_DRUG_ABUSE_CTL_CD"),
    F.col("NdcDrugAbuseCtlCdSk.TRGT_CD_NM").alias("NDC_DRUG_ABUSE_CTL_NM"),
    F.col("NdcDrugClsCdSk.TRGT_CD").alias("NDC_DRUG_CLS_CD"),
    F.col("NdcDrugClsCdSk.TRGT_CD_NM").alias("NDC_DRUG_CLS_NM"),
    F.col("NdcGnrcNmdDrugCdSk.TRGT_CD").alias("NDC_GNRC_NMD_DRUG_CD"),
    F.col("NdcGnrcNmdDrugCdSk.TRGT_CD_NM").alias("NDC_GNRC_NMD_DRUG_NM"),
    F.col("NdcRteTypCdSk.TRGT_CD").alias("NDC_RTE_TYP_CD"),
    F.col("NdcRteTypCdSk.TRGT_CD_NM").alias("NDC_RTE_TYP_NM"),
    F.col("SubmtProdIdQlfrCdSk.TRGT_CD").alias("SUBMT_PROD_ID_QLFR_CD"),
    F.col("SubmtProdIdQlfrCdSk.TRGT_CD_NM").alias("SUBMT_PROD_ID_QLFR_NM"),
    F.col("IDS_Out_Link.DRUG_MNTN_IN").alias("DRUG_MNTN_IN"),
    F.col("IDS_Out_Link.MED_SUPL_IN").alias("MED_SUPL_IN"),
    F.col("IDS_Out_Link.PRMT_EXCL_IN").alias("PRMT_EXCL_IN"),
    F.col("IDS_Out_Link.PROD_RMBRMT_IN").alias("PROD_RMBRMT_IN"),
    F.col("IDS_Out_Link.RBT_MNFCTR_IN").alias("RBT_MNFCTR_IN"),
    F.col("IDS_Out_Link.SNGL_SRC_IN").alias("SNGL_SRC_IN"),
    F.col("IDS_Out_Link.UNIT_DOSE_IN").alias("UNIT_DOSE_IN"),
    F.col("IDS_Out_Link.UNIT_OF_USE_IN").alias("UNIT_OF_USE_IN"),
    F.col("IDS_Out_Link.AWP_UNIT_CST_AMT").alias("AWP_UNIT_CST_AMT"),
    F.col("AprvCstSrcCdSk.TRGT_CD").alias("APRV_CST_SRC_CD"),
    F.col("AprvCstSrcCdSk.TRGT_CD_NM").alias("APRV_CST_SRC_NM"),
    F.col("IDS_Out_Link.APRV_CST_TYP_ID").alias("APRV_CST_TYP_ID"),
    F.col("IDS_Out_Link.APRV_CST_TYP_UNIT_CST_AMT").alias("APRV_CST_TYP_UNIT_CST_AMT"),
    F.col("IDS_Out_Link.APRV_INGR_CST_AMT").alias("APRV_INGR_CST_AMT"),
    F.col("IDS_Out_Link.APRV_PROF_SVC_FEE_PD_AMT").alias("APRV_PROF_SVC_FEE_PD_AMT"),
    F.col("IDS_Out_Link.CALC_INGR_CST_AMT").alias("CALC_INGR_CST_AMT"),
    F.col("IDS_Out_Link.CALC_PROF_SVC_FEE_PD_AMT").alias("CALC_PROF_SVC_FEE_PD_AMT"),
    F.col("ClntCstSrcCdSk.TRGT_CD").alias("CLNT_CST_SRC_CD"),
    F.col("ClntCstSrcCdSk.TRGT_CD_NM").alias("CLNT_CST_SRC_NM"),
    F.col("IDS_Out_Link.CLNT_CST_TYP_ID").alias("CLNT_CST_TYP_ID"),
    F.col("IDS_Out_Link.CLNT_CST_TYP_UNIT_CST_AMT").alias("CLNT_CST_TYP_UNIT_CST_AMT"),
    F.col("IDS_Out_Link.CLNT_INGR_CST_AMT").alias("CLNT_INGR_CST_AMT"),
    F.col("IDS_Out_Link.CLNT_PROF_SVC_FEE_PD_AMT").alias("CLNT_PROF_SVC_FEE_PD_AMT"),
    F.col("IDS_Out_Link.CLNT_RATE_PCT").alias("CLNT_RATE_PCT"),
    F.col("IDS_Out_Link.SUBMT_CMPND_CST_TYP_ID").alias("SUBMT_CMPND_CST_TYP_ID"),
    F.col("IDS_Out_Link.SUBMT_CMPND_INGR_CST_AMT").alias("SUBMT_CMPND_INGR_CST_AMT"),
    F.col("IDS_Out_Link.SUBMT_CMPND_INGR_QTY").alias("SUBMT_CMPND_INGR_QTY"),
    F.col("IDS_Out_Link.PDX_RATE_PCT").alias("PDX_RATE_PCT"),
    F.col("IDS_Out_Link.DRUG_METRIC_STRG_NO").alias("DRUG_METRIC_STRG_NO"),
    F.col("IDS_Out_Link.INGR_MOD_CD_CT").alias("INGR_MOD_CD_CT"),
    F.col("IDS_Out_Link.CMPND_PROD_BRND_NM").alias("CMPND_PROD_BRND_NM"),
    F.col("IDS_Out_Link.CMPND_PROD_ID").alias("CMPND_PROD_ID"),
    F.col("IDS_Out_Link.DRUG_DSCRPTR_ID").alias("DRUG_DSCRPTR_ID"),
    F.col("IDS_Out_Link.DRUG_LABEL_NM").alias("DRUG_LABEL_NM"),
    F.col("IDS_Out_Link.GNRC_NM_SH_DESC").alias("GNRC_NM_SH_DESC"),
    F.col("IDS_Out_Link.GNRC_PROD_ID").alias("GNRC_PROD_ID"),
    F.col("IDS_Out_Link.LBLR_NM").alias("LBLR_NM"),
    F.col("IDS_Out_Link.LBLR_NO").alias("LBLR_NO"),
    F.col("IDS_Out_Link.NDC_GCN_CD_TX").alias("NDC_GCN_CD_TX"),
    F.col("IDS_Out_Link.GCN_SEQ_NO").alias("GCN_SEQ_NO"),
    F.col("IDS_Out_Link.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("IDS_Out_Link.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("IDS_Out_Link.AHFS_TCC_SK").alias("AHFS_TCC_SK"),
    F.col("IDS_Out_Link.APRV_CST_SRC_CD_SK").alias("APRV_CST_SRC_CD_SK"),
    F.col("IDS_Out_Link.CLNT_CST_SRC_CD_SK").alias("CLNT_CST_SRC_CD_SK"),
    F.col("IDS_Out_Link.DRUG_3RD_PARTY_EXCPT_CD_SK").alias("DRUG_3RD_PARTY_EXCPT_CD_SK"),
    F.col("IDS_Out_Link.GNRC_OVRD_CD_SK").alias("GNRC_OVRD_CD_SK"),
    F.col("IDS_Out_Link.INGR_STTUS_CD_SK").alias("INGR_STTUS_CD_SK"),
    F.col("IDS_Out_Link.NDC_DRUG_ABUSE_CTL_CD_SK").alias("NDC_DRUG_ABUSE_CTL_CD_SK"),
    F.col("IDS_Out_Link.NDC_DRUG_CLS_CD_SK").alias("NDC_DRUG_CLS_CD_SK"),
    F.col("IDS_Out_Link.NDC_GNRC_NMD_DRUG_CD_SK").alias("NDC_GNRC_NMD_DRUG_CD_SK"),
    F.col("IDS_Out_Link.NDC_RTE_TYP_CD_SK").alias("NDC_RTE_TYP_CD_SK"),
    F.col("IDS_Out_Link.SUBMT_PROD_ID_QLFR_CD_SK").alias("SUBMT_PROD_ID_QLFR_CD_SK"),
    F.col("IDS_Out_Link.INGR_UNIT_OF_MESR_TX").alias("INGR_UNIT_OF_MESR_TX"),
    F.col("IDS_Out_Link.FDA_THRPTC_EQVLNT_CD_SK").alias("FDA_THRPTC_EQVLNT_CD_SK")
)

df_AhfsTccLkp_intermediate = (
    df_CdMppngLkp.alias("LnkToLkp1")
    .join(
        df_IDS_AHFS_TCC.alias("LnkAhfsTcc"),
        F.col("LnkToLkp1.AHFS_TCC_SK") == F.col("LnkAhfsTcc.AHFS_TCC_SK"),
        "left",
    )
)

df_AhfsTccLkp = df_AhfsTccLkp_intermediate.select(
    F.col("LnkToLkp1.DRUG_CLM_CMPND_INGR_SK").alias("DRUG_CLM_CMPND_INGR_SK"),
    F.col("LnkToLkp1.CLM_ID").alias("CLM_ID"),
    F.col("LnkToLkp1.DRUG_CLM_CMPND_INGR_SEQ_NO").alias("DRUG_CLM_CMPND_INGR_SEQ_NO"),
    F.col("LnkToLkp1.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("LnkToLkp1.CLM_SK").alias("CLM_SK"),
    F.col("LnkToLkp1.NDC_SK").alias("NDC_SK"),
    F.col("LnkAhfsTcc.AHFS_TCC").alias("AHFS_TCC"),
    F.col("LnkAhfsTcc.AHFS_TCC_DESC").alias("AHFS_TCC_DESC"),
    F.col("LnkToLkp1.DRUG_3RD_PARTY_EXCPT_CD").alias("DRUG_3RD_PARTY_EXCPT_CD"),
    F.col("LnkToLkp1.DRUG_3RD_PARTY_EXCPT_NM").alias("DRUG_3RD_PARTY_EXCPT_NM"),
    F.col("LnkToLkp1.FDA_THRPTC_EQVLNT_CD").alias("FDA_THRPTC_EQVLNT_CD"),
    F.col("LnkToLkp1.FDA_THRPTC_EQVLNT_NM").alias("FDA_THRPTC_EQVLNT_NM"),
    F.col("LnkToLkp1.GNRC_OVRD_CD").alias("GNRC_OVRD_CD"),
    F.col("LnkToLkp1.GNRC_OVRD_NM").alias("GNRC_OVRD_NM"),
    F.col("LnkToLkp1.INGR_STTUS_CD").alias("INGR_STTUS_CD"),
    F.col("LnkToLkp1.INGR_STTUS_NM").alias("INGR_STTUS_NM"),
    F.col("LnkToLkp1.NDC_DRUG_ABUSE_CTL_CD").alias("NDC_DRUG_ABUSE_CTL_CD"),
    F.col("LnkToLkp1.NDC_DRUG_ABUSE_CTL_NM").alias("NDC_DRUG_ABUSE_CTL_NM"),
    F.col("LnkToLkp1.NDC_DRUG_CLS_CD").alias("NDC_DRUG_CLS_CD"),
    F.col("LnkToLkp1.NDC_DRUG_CLS_NM").alias("NDC_DRUG_CLS_NM"),
    F.col("LnkToLkp1.NDC_GNRC_NMD_DRUG_CD").alias("NDC_GNRC_NMD_DRUG_CD"),
    F.col("LnkToLkp1.NDC_GNRC_NMD_DRUG_NM").alias("NDC_GNRC_NMD_DRUG_NM"),
    F.col("LnkToLkp1.NDC_RTE_TYP_CD").alias("NDC_RTE_TYP_CD"),
    F.col("LnkToLkp1.NDC_RTE_TYP_NM").alias("NDC_RTE_TYP_NM"),
    F.col("LnkToLkp1.SUBMT_PROD_ID_QLFR_CD").alias("SUBMT_PROD_ID_QLFR_CD"),
    F.col("LnkToLkp1.SUBMT_PROD_ID_QLFR_NM").alias("SUBMT_PROD_ID_QLFR_NM"),
    F.col("LnkToLkp1.DRUG_MNTN_IN").alias("DRUG_MNTN_IN"),
    F.col("LnkToLkp1.MED_SUPL_IN").alias("MED_SUPL_IN"),
    F.col("LnkToLkp1.PRMT_EXCL_IN").alias("PRMT_EXCL_IN"),
    F.col("LnkToLkp1.PROD_RMBRMT_IN").alias("PROD_RMBRMT_IN"),
    F.col("LnkToLkp1.RBT_MNFCTR_IN").alias("RBT_MNFCTR_IN"),
    F.col("LnkToLkp1.SNGL_SRC_IN").alias("SNGL_SRC_IN"),
    F.col("LnkToLkp1.UNIT_DOSE_IN").alias("UNIT_DOSE_IN"),
    F.col("LnkToLkp1.UNIT_OF_USE_IN").alias("UNIT_OF_USE_IN"),
    F.col("LnkToLkp1.AWP_UNIT_CST_AMT").alias("AWP_UNIT_CST_AMT"),
    F.col("LnkToLkp1.APRV_CST_SRC_CD").alias("APRV_CST_SRC_CD"),
    F.col("LnkToLkp1.APRV_CST_SRC_NM").alias("APRV_CST_SRC_NM"),
    F.col("LnkToLkp1.APRV_CST_TYP_ID").alias("APRV_CST_TYP_ID"),
    F.col("LnkToLkp1.APRV_CST_TYP_UNIT_CST_AMT").alias("APRV_CST_TYP_UNIT_CST_AMT"),
    F.col("LnkToLkp1.APRV_INGR_CST_AMT").alias("APRV_INGR_CST_AMT"),
    F.col("LnkToLkp1.APRV_PROF_SVC_FEE_PD_AMT").alias("APRV_PROF_SVC_FEE_PD_AMT"),
    F.col("LnkToLkp1.CALC_INGR_CST_AMT").alias("CALC_INGR_CST_AMT"),
    F.col("LnkToLkp1.CALC_PROF_SVC_FEE_PD_AMT").alias("CALC_PROF_SVC_FEE_PD_AMT"),
    F.col("LnkToLkp1.CLNT_CST_SRC_CD").alias("CLNT_CST_SRC_CD"),
    F.col("LnkToLkp1.CLNT_CST_SRC_NM").alias("CLNT_CST_SRC_NM"),
    F.col("LnkToLkp1.CLNT_CST_TYP_ID").alias("CLNT_CST_TYP_ID"),
    F.col("LnkToLkp1.CLNT_CST_TYP_UNIT_CST_AMT").alias("CLNT_CST_TYP_UNIT_CST_AMT"),
    F.col("LnkToLkp1.CLNT_INGR_CST_AMT").alias("CLNT_INGR_CST_AMT"),
    F.col("LnkToLkp1.CLNT_PROF_SVC_FEE_PD_AMT").alias("CLNT_PROF_SVC_FEE_PD_AMT"),
    F.col("LnkToLkp1.CLNT_RATE_PCT").alias("CLNT_RATE_PCT"),
    F.col("LnkToLkp1.SUBMT_CMPND_CST_TYP_ID").alias("SUBMT_CMPND_CST_TYP_ID"),
    F.col("LnkToLkp1.SUBMT_CMPND_INGR_CST_AMT").alias("SUBMT_CMPND_INGR_CST_AMT"),
    F.col("LnkToLkp1.SUBMT_CMPND_INGR_QTY").alias("SUBMT_CMPND_INGR_QTY"),
    F.col("LnkToLkp1.PDX_RATE_PCT").alias("PDX_RATE_PCT"),
    F.col("LnkToLkp1.DRUG_METRIC_STRG_NO").alias("DRUG_METRIC_STRG_NO"),
    F.col("LnkToLkp1.INGR_MOD_CD_CT").alias("INGR_MOD_CD_CT"),
    F.col("LnkToLkp1.CMPND_PROD_BRND_NM").alias("CMPND_PROD_BRND_NM"),
    F.col("LnkToLkp1.CMPND_PROD_ID").alias("CMPND_PROD_ID"),
    F.col("LnkToLkp1.DRUG_DSCRPTR_ID").alias("DRUG_DSCRPTR_ID"),
    F.col("LnkToLkp1.DRUG_LABEL_NM").alias("DRUG_LABEL_NM"),
    F.col("LnkToLkp1.GNRC_NM_SH_DESC").alias("GNRC_NM_SH_DESC"),
    F.col("LnkToLkp1.GNRC_PROD_ID").alias("GNRC_PROD_ID"),
    F.col("LnkToLkp1.LBLR_NM").alias("LBLR_NM"),
    F.col("LnkToLkp1.LBLR_NO").alias("LBLR_NO"),
    F.col("LnkToLkp1.NDC_GCN_CD_TX").alias("NDC_GCN_CD_TX"),
    F.col("LnkToLkp1.GCN_SEQ_NO").alias("GCN_SEQ_NO"),
    F.col("LnkToLkp1.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LnkToLkp1.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("LnkToLkp1.AHFS_TCC_SK").alias("AHFS_TCC_SK"),
    F.col("LnkToLkp1.APRV_CST_SRC_CD_SK").alias("APRV_CST_SRC_CD_SK"),
    F.col("LnkToLkp1.CLNT_CST_SRC_CD_SK").alias("CLNT_CST_SRC_CD_SK"),
    F.col("LnkToLkp1.DRUG_3RD_PARTY_EXCPT_CD_SK").alias("DRUG_3RD_PARTY_EXCPT_CD_SK"),
    F.col("LnkToLkp1.FDA_THRPTC_EQVLNT_CD_SK").alias("FDA_THRPTC_EQVLNT_CD_SK"),
    F.col("LnkToLkp1.GNRC_OVRD_CD_SK").alias("GNRC_OVRD_CD_SK"),
    F.col("LnkToLkp1.INGR_STTUS_CD_SK").alias("INGR_STTUS_CD_SK"),
    F.col("LnkToLkp1.NDC_DRUG_ABUSE_CTL_CD_SK").alias("NDC_DRUG_ABUSE_CTL_CD_SK"),
    F.col("LnkToLkp1.NDC_DRUG_CLS_CD_SK").alias("NDC_DRUG_CLS_CD_SK"),
    F.col("LnkToLkp1.NDC_GNRC_NMD_DRUG_CD_SK").alias("NDC_GNRC_NMD_DRUG_CD_SK"),
    F.col("LnkToLkp1.NDC_RTE_TYP_CD_SK").alias("NDC_RTE_TYP_CD_SK"),
    F.col("LnkToLkp1.SUBMT_PROD_ID_QLFR_CD_SK").alias("SUBMT_PROD_ID_QLFR_CD_SK"),
    F.col("LnkToLkp1.INGR_UNIT_OF_MESR_TX").alias("INGR_UNIT_OF_MESR_TX"),
)

df_Trans2 = df_AhfsTccLkp.select(
    F.col("DRUG_CLM_CMPND_INGR_SK").alias("DRUG_CLM_CMPND_INGR_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("DRUG_CLM_CMPND_INGR_SEQ_NO").alias("DRUG_CLM_CMPND_INGR_SEQ_NO"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.lit(CurrRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(CurrRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("CLM_SK").alias("CLM_SK"),
    F.col("NDC_SK").alias("NDC_SK"),
    F.col("AHFS_TCC_SK").alias("AHFS_TCC_SK"),
    F.col("AHFS_TCC").alias("AHFS_TCC"),
    F.col("AHFS_TCC_DESC").alias("AHFS_TCC_DESC"),
    F.col("DRUG_3RD_PARTY_EXCPT_CD").alias("DRUG_3RD_PARTY_EXCPT_CD"),
    F.col("DRUG_3RD_PARTY_EXCPT_NM").alias("DRUG_3RD_PARTY_EXCPT_NM"),
    F.col("FDA_THRPTC_EQVLNT_CD").alias("FDA_THRPTC_EQVLNT_CD"),
    F.col("FDA_THRPTC_EQVLNT_NM").alias("FDA_THRPTC_EQVLNT_NM"),
    F.col("GNRC_OVRD_CD").alias("GNRC_OVRD_CD"),
    F.col("GNRC_OVRD_NM").alias("GNRC_OVRD_NM"),
    F.col("INGR_STTUS_CD").alias("INGR_STTUS_CD"),
    F.col("INGR_STTUS_NM").alias("INGR_STTUS_NM"),
    F.col("NDC_DRUG_ABUSE_CTL_CD").alias("NDC_DRUG_ABUSE_CTL_CD"),
    F.col("NDC_DRUG_ABUSE_CTL_NM").alias("NDC_DRUG_ABUSE_CTL_NM"),
    F.col("NDC_DRUG_CLS_CD").alias("NDC_DRUG_CLS_CD"),
    F.col("NDC_DRUG_CLS_NM").alias("NDC_DRUG_CLS_NM"),
    F.col("NDC_GNRC_NMD_DRUG_CD").alias("NDC_GNRC_NMD_DRUG_CD"),
    F.col("NDC_GNRC_NMD_DRUG_NM").alias("NDC_GNRC_NMD_DRUG_NM"),
    F.col("NDC_RTE_TYP_CD").alias("NDC_RTE_TYP_CD"),
    F.col("NDC_RTE_TYP_NM").alias("NDC_RTE_TYP_NM"),
    F.col("SUBMT_PROD_ID_QLFR_CD").alias("SUBMT_PROD_ID_QLFR_CD"),
    F.col("SUBMT_PROD_ID_QLFR_NM").alias("SUBMT_PROD_ID_QLFR_NM"),
    F.col("DRUG_MNTN_IN").alias("DRUG_MNTN_IN"),
    F.col("MED_SUPL_IN").alias("MED_SUPL_IN"),
    F.col("PRMT_EXCL_IN").alias("PRMT_EXCL_IN"),
    F.col("PROD_RMBRMT_IN").alias("PROD_RMBRMT_IN"),
    F.col("RBT_MNFCTR_IN").alias("RBT_MNFCTR_IN"),
    F.col("SNGL_SRC_IN").alias("SNGL_SRC_IN"),
    F.col("UNIT_DOSE_IN").alias("UNIT_DOSE_IN"),
    F.col("UNIT_OF_USE_IN").alias("UNIT_OF_USE_IN"),
    F.col("AWP_UNIT_CST_AMT").alias("AWP_UNIT_CST_AMT"),
    F.col("APRV_CST_SRC_CD").alias("APRV_CST_SRC_CD"),
    F.col("APRV_CST_SRC_NM").alias("APRV_CST_SRC_NM"),
    F.col("APRV_CST_TYP_ID").alias("APRV_CST_TYP_ID"),
    F.col("APRV_CST_TYP_UNIT_CST_AMT").alias("APRV_CST_TYP_UNIT_CST_AMT"),
    F.col("APRV_INGR_CST_AMT").alias("APRV_INGR_CST_AMT"),
    F.col("APRV_PROF_SVC_FEE_PD_AMT").alias("APRV_PROF_SVC_FEE_PD_AMT"),
    F.col("CALC_INGR_CST_AMT").alias("CALC_INGR_CST_AMT"),
    F.col("CALC_PROF_SVC_FEE_PD_AMT").alias("CALC_PROF_SVC_FEE_PD_AMT"),
    F.col("CLNT_CST_SRC_CD").alias("CLNT_CST_SRC_CD"),
    F.col("CLNT_CST_SRC_NM").alias("CLNT_CST_SRC_NM"),
    F.col("CLNT_CST_TYP_ID").alias("CLNT_CST_TYP_ID"),
    F.col("CLNT_CST_TYP_UNIT_CST_AMT").alias("CLNT_CST_TYP_UNIT_CST_AMT"),
    F.col("CLNT_INGR_CST_AMT").alias("CLNT_INGR_CST_AMT"),
    F.col("CLNT_PROF_SVC_FEE_PD_AMT").alias("CLNT_PROF_SVC_FEE_PD_AMT"),
    F.col("CLNT_RATE_PCT").alias("CLNT_RATE_PCT"),
    F.col("SUBMT_CMPND_CST_TYP_ID").alias("SUBMT_CMPND_CST_TYP_ID"),
    F.col("SUBMT_CMPND_INGR_CST_AMT").alias("SUBMT_CMPND_INGR_CST_AMT"),
    F.col("SUBMT_CMPND_INGR_QTY").alias("SUBMT_CMPND_INGR_QTY"),
    F.col("PDX_RATE_PCT").alias("PDX_RATE_PCT"),
    F.col("DRUG_METRIC_STRG_NO").alias("DRUG_METRIC_STRG_NO"),
    F.col("INGR_MOD_CD_CT").alias("INGR_MOD_CD_CT"),
    F.col("CMPND_PROD_BRND_NM").alias("CMPND_PROD_BRND_NM"),
    F.col("CMPND_PROD_ID").alias("CMPND_PROD_ID"),
    F.col("DRUG_DSCRPTR_ID").alias("DRUG_DSCRPTR_ID"),
    F.col("DRUG_LABEL_NM").alias("DRUG_LABEL_NM"),
    F.col("GNRC_NM_SH_DESC").alias("GNRC_NM_SH_DESC"),
    F.col("GNRC_PROD_ID").alias("GNRC_PROD_ID"),
    F.col("INGR_UNIT_OF_MESR_TX").alias("INGR_UNIT_OF_MESR_TX"),
    F.col("LBLR_NM").alias("LBLR_NM"),
    F.col("LBLR_NO").alias("LBLR_NO"),
    F.col("NDC_GCN_CD_TX").alias("NDC_GCN_CD_TX"),
    F.col("GCN_SEQ_NO").alias("GCN_SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("APRV_CST_SRC_CD_SK").alias("APRV_CST_SRC_CD_SK"),
    F.col("CLNT_CST_SRC_CD_SK").alias("CLNT_CST_SRC_CD_SK"),
    F.col("DRUG_3RD_PARTY_EXCPT_CD_SK").alias("DRUG_3RD_PARTY_EXCPT_CD_SK"),
    F.col("FDA_THRPTC_EQVLNT_CD_SK").alias("FDA_THRPTC_EQVLNT_CD_SK"),
    F.col("GNRC_OVRD_CD_SK").alias("GNRC_OVRD_CD_SK"),
    F.col("INGR_STTUS_CD_SK").alias("INGR_STTUS_CD_SK"),
    F.col("NDC_DRUG_ABUSE_CTL_CD_SK").alias("NDC_DRUG_ABUSE_CTL_CD_SK"),
    F.col("NDC_DRUG_CLS_CD_SK").alias("NDC_DRUG_CLS_CD_SK"),
    F.col("NDC_GNRC_NMD_DRUG_CD_SK").alias("NDC_GNRC_NMD_DRUG_CD_SK"),
    F.col("NDC_RTE_TYP_CD_SK").alias("NDC_RTE_TYP_CD_SK"),
    F.col("SUBMT_PROD_ID_QLFR_CD_SK").alias("SUBMT_PROD_ID_QLFR_CD_SK"),
)

df_final = df_Trans2

df_final = df_final.withColumn(
    "CRT_RUN_CYC_EXCTN_DT_SK",
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
).withColumn(
    "DRUG_MNTN_IN",
    F.rpad(F.col("DRUG_MNTN_IN"), 1, " ")
).withColumn(
    "MED_SUPL_IN",
    F.rpad(F.col("MED_SUPL_IN"), 1, " ")
).withColumn(
    "PRMT_EXCL_IN",
    F.rpad(F.col("PRMT_EXCL_IN"), 1, " ")
).withColumn(
    "PROD_RMBRMT_IN",
    F.rpad(F.col("PROD_RMBRMT_IN"), 1, " ")
).withColumn(
    "RBT_MNFCTR_IN",
    F.rpad(F.col("RBT_MNFCTR_IN"), 1, " ")
).withColumn(
    "SNGL_SRC_IN",
    F.rpad(F.col("SNGL_SRC_IN"), 1, " ")
).withColumn(
    "UNIT_DOSE_IN",
    F.rpad(F.col("UNIT_DOSE_IN"), 1, " ")
).withColumn(
    "UNIT_OF_USE_IN",
    F.rpad(F.col("UNIT_OF_USE_IN"), 1, " ")
)

write_files(
    df_final,
    f"{adls_path}/load/DRUG_CLM_CMPND_INGR_F.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)