# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:
# MAGIC 
# MAGIC PROCESSING:
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                             Date                  Project/Altiris #                                   Change Description                                                    Development Project     Code Reviewer            Date Reviewed
# MAGIC ----------------------------------        -------------------      -----------------------------------    ---------------------------------------------------------------------------------------------             ----------------------------------    ---------------------------------    -------------------------   
# MAGIC Naren                                 9/20/2007                                                 Originally Programmed                                                                        devlEDW10                 Steph Goddard            10/07/2007
# MAGIC Steph Goddard                   2/19/2008          prod supp                       added hash file before collector                                                         devlEDW                     Brent Leland                 02/19/2008
# MAGIC 
# MAGIC Raj Mangalampally             2013-08-30         5114                                  Original Programming (Server to Parallel Conversion)                       EnterpriseWrhsDevl    Peter Marshall              12/10/2013
# MAGIC 
# MAGIC Manasa Andru                    2016-06-08        TFS - 12538                  Updated the data type of   ALT_FUND_INVC_PAYMT_SEQ_NO       EnterpriseDev1            Jag Yelavarthi              2016-06-08
# MAGIC                                                                                                         field from SMALLINT to INTEGER so that 10 digit values are loaded

# MAGIC INVC_DSCRTN
# MAGIC Funnel NA, UNK Rows
# MAGIC Job Name : IdsEdwAltFundInvcDtlFExtr
# MAGIC FUND_PAYMENT
# MAGIC BILL_RDUCTION
# MAGIC Look for the Matching key in Code mapping for De-Normalization
# MAGIC Adding Defaults and Business Rules
# MAGIC Bill Rduction Cd Lookup
# MAGIC PaymentInvcCd Lookup
# MAGIC DscrtnInvcCd lookup
# MAGIC Adding Defaults and Business Rules
# MAGIC Adding Defaults and Business Rules
# MAGIC Write to a Dataset ALT_FUND_INVC_DTL_F.ds for Primary keying Job
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

# --------------------------------------------------------------------------------
# db2_IDS_BILL_RDUCTION
# --------------------------------------------------------------------------------
query_db2_IDS_BILL_RDUCTION = (
  "SELECT \n"
  "INVC.ALT_FUND_INVC_SK,\n"
  "COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,\n"
  "INVC.ALT_FUND_INVC_ID,\n"
  "INVC.ALT_FUND_SK,\n"
  "INVC.BILL_ENTY_SK,\n"
  "INVC.FUND_FROM_DT_SK,\n"
  "INVC.FUND_THRU_DT_SK,\n"
  "ALT_FUND.ALT_FUND_ID,\n"
  "ALT_FUND.ALT_FUND_NM,\n"
  "HIST.BILL_RDUCTN_HIST_SK,\n"
  "HIST.MBR_UNIQ_KEY,\n"
  "HIST.BILL_ENTY_UNIQ_KEY,\n"
  "HIST.SEQ_NO,\n"
  "HIST.CLS_PLN_SK,\n"
  "HIST.GRP_SK,\n"
  "HIST.MBR_SK,\n"
  "HIST.CLM_SK,\n"
  "HIST.PROD_SK,\n"
  "HIST.SUBGRP_SK,\n"
  "HIST.PCA_AMT,\n"
  "HIST.CLM_ID,\n"
  "HIST.ALT_FUND_CNTR_PERD_NO,\n"
  "CLM.PD_DT_SK,\n"
  "CLM.SVC_STRT_DT_SK,\n"
  "CLS.CLS_SK,\n"
  "CLS.CLS_ID,\n"
  "CLS_PLN.CLS_PLN_ID,\n"
  "GRP.GRP_ID,\n"
  "GRP.GRP_NM,\n"
  "SUBGRP.SUBGRP_ID,\n"
  "SUB.SUB_SK,\n"
  "SUB.SUB_UNIQ_KEY,\n"
  "PROD.PROD_ID\n"
  f"FROM {IDSOwner}.ALT_FUND_INVC INVC\n"
  f"LEFT JOIN {IDSOwner}.CD_MPPNG CD\n"
  "ON INVC.SRC_SYS_CD_SK = CD.CD_MPPNG_SK,\n"
  f"{IDSOwner}.BILL_RDUCTN_HIST HIST,\n"
  f"{IDSOwner}.ALT_FUND ALT_FUND,\n"
  f"{IDSOwner}.CLM CLM,\n"
  f"{IDSOwner}.CLS CLS,\n"
  f"{IDSOwner}.CLS_PLN CLS_PLN,\n"
  f"{IDSOwner}.GRP GRP,\n"
  f"{IDSOwner}.SUBGRP SUBGRP,\n"
  f"{IDSOwner}.SUB SUB,\n"
  f"{IDSOwner}.PROD PROD\n"
  "WHERE\n"
  "ALT_FUND.ALT_FUND_SK=INVC.ALT_FUND_SK AND\n"
  "INVC.ALT_FUND_INVC_SK=HIST.ALT_FUND_INVC_SK AND\n"
  "(INVC.ALT_FUND_INVC_SK<>0 AND INVC.ALT_FUND_INVC_SK<>1) AND\n"
  "HIST.CLM_SK=CLM.CLM_SK AND\n"
  "CLM.SUB_SK=SUB.SUB_SK AND\n"
  "CLM.CLS_SK=CLS.CLS_SK AND\n"
  "HIST.CLS_PLN_SK=CLS_PLN.CLS_PLN_SK AND\n"
  "HIST.GRP_SK=GRP.GRP_SK AND\n"
  "HIST.SUBGRP_SK=SUBGRP.SUBGRP_SK AND\n"
  "HIST.PROD_SK=PROD.PROD_SK\n"
  "AND CLM.SRC_SYS_CD_SK = 1581 AND CLM.PCA_TYP_CD_SK <>1"
)

df_db2_IDS_BILL_RDUCTION = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_db2_IDS_BILL_RDUCTION)
    .load()
)

# --------------------------------------------------------------------------------
# xfm_businesslogic4
# --------------------------------------------------------------------------------
df_xfm_businesslogic4 = (
    df_db2_IDS_BILL_RDUCTION.alias("lnk_IdsBillRductionExtr_InABC")
    .select(
        F.when(F.trim(F.col("SRC_SYS_CD")) == "", F.lit("UNK")).otherwise(F.col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
        F.col("ALT_FUND_INVC_ID").alias("ALT_FUND_INVC_ID"),
        F.lit("RDUCTN").alias("TRGT_CD"),
        F.col("ALT_FUND_SK").alias("ALT_FUND_SK"),
        F.col("ALT_FUND_INVC_SK").alias("ALT_FUND_INVC_SK"),
        F.col("BILL_ENTY_SK").alias("BILL_ENTY_SK"),
        F.col("BILL_RDUCTN_HIST_SK").alias("BILL_RDUCTN_HIST_SK"),
        F.col("CLM_SK").alias("CLM_SK"),
        F.col("CLS_SK").alias("CLS_SK"),
        F.col("CLS_PLN_SK").alias("CLS_PLN_SK"),
        F.col("GRP_SK").alias("GRP_SK"),
        F.col("SUBGRP_SK").alias("SUBGRP_SK"),
        F.col("SUB_SK").alias("SUB_SK"),
        F.col("MBR_SK").alias("MBR_SK"),
        F.col("PROD_SK").alias("PROD_SK"),
        F.col("FUND_THRU_DT_SK").alias("FUND_THRU_DT_SK"),
        F.col("FUND_FROM_DT_SK").alias("FUND_FROM_DT_SK"),
        F.col("PD_DT_SK").alias("PD_DT_SK"),
        F.col("SVC_STRT_DT_SK").alias("SVC_STRT_DT_SK"),
        F.col("PCA_AMT").alias("PCA_AMT"),
        F.col("ALT_FUND_CNTR_PERD_NO").alias("ALT_FUND_CNTR_PERD_NO"),
        F.col("SEQ_NO").alias("SEQ_NO"),
        F.col("BILL_ENTY_UNIQ_KEY").alias("BILL_ENTY_UNIQ_KEY"),
        F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
        F.col("ALT_FUND_ID").alias("ALT_FUND_ID"),
        F.col("ALT_FUND_NM").alias("ALT_FUND_NM"),
        F.col("CLM_ID").alias("CLM_ID"),
        F.col("CLS_ID").alias("CLS_ID"),
        F.col("CLS_PLN_ID").alias("CLS_PLN_ID"),
        F.col("GRP_ID").alias("GRP_ID"),
        F.col("GRP_NM").alias("GRP_NM"),
        F.col("PROD_ID").alias("PROD_ID"),
        F.col("SUBGRP_ID").alias("SUBGRP_ID"),
    )
)

# --------------------------------------------------------------------------------
# db2_IDS_FUND_PAYMENT_in
# --------------------------------------------------------------------------------
query_db2_IDS_FUND_PAYMENT_in = (
  "SELECT \n"
  "INVC.ALT_FUND_INVC_SK,\n"
  "COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,\n"
  "INVC.ALT_FUND_INVC_ID,\n"
  "INVC.ALT_FUND_SK,\n"
  "INVC.FUND_FROM_DT_SK,\n"
  "INVC.FUND_THRU_DT_SK,\n"
  "ALT_FUND.ALT_FUND_ID,\n"
  "ALT_FUND.ALT_FUND_NM,\n"
  "PAYMT.ALT_FUND_INVC_PAYMT_SK,\n"
  "PAYMT.SEQ_NO,\n"
  "PAYMT.MBR_UNIQ_KEY,\n"
  "PAYMT.SUB_UNIQ_KEY,\n"
  "PAYMT.CLM_SK,\n"
  "PAYMT.GRP_SK,\n"
  "PAYMT.CLS_PLN_SK,\n"
  "PAYMT.MBR_SK,\n"
  "PAYMT.PROD_SK,\n"
  "PAYMT.SUBGRP_SK,\n"
  "PAYMT.SUB_SK,\n"
  "PAYMT.CLS_PLN_ID,\n"
  "PAYMT.CLM_ID,\n"
  "PAYMT.PROD_ID,\n"
  "PAYMT.ALT_FUND_CNTR_PERD_NO,\n"
  "PAYMT.PCA_AMT,\n"
  "CLS.CLS_SK,\n"
  "CLS.CLS_ID,\n"
  "GRP.GRP_ID,\n"
  "SUBGRP.SUBGRP_ID,\n"
  "GRP.GRP_NM,\n"
  "CLM.PD_DT_SK,\n"
  "CLM.SVC_STRT_DT_SK,\n"
  "BILL_ENTY.BILL_ENTY_SK,\n"
  "BILL_ENTY.BILL_ENTY_UNIQ_KEY\n"
  f"FROM {IDSOwner}.ALT_FUND_INVC INVC \n"
  f"LEFT JOIN {IDSOwner}.CD_MPPNG CD\n"
  "ON INVC.SRC_SYS_CD_SK = CD.CD_MPPNG_SK,\n"
  f"{IDSOwner}.ALT_FUND ALT_FUND,\n"
  f"{IDSOwner}.ALT_FUND_INVC_PAYMT PAYMT,\n"
  f"{IDSOwner}.CLM CLM,\n"
  f"{IDSOwner}.CLS CLS,\n"
  f"{IDSOwner}.GRP GRP,\n"
  f"{IDSOwner}.SUBGRP SUBGRP,\n"
  f"{IDSOwner}.BILL_ENTY BILL_ENTY\n"
  "WHERE \n"
  "INVC.ALT_FUND_SK=ALT_FUND.ALT_FUND_SK AND\n"
  "INVC.ALT_FUND_INVC_SK=PAYMT.ALT_FUND_INVC_SK AND\n"
  "INVC.ALT_FUND_INVC_SK<>0 AND INVC.ALT_FUND_INVC_SK<>1 AND\n"
  "PAYMT.CLM_SK=CLM.CLM_SK AND\n"
  "CLM.CLS_SK=CLS.CLS_SK AND\n"
  "CLM.GRP_SK=GRP.GRP_SK AND\n"
  "CLM.SUBGRP_SK=SUBGRP.SUBGRP_SK AND\n"
  "INVC.BILL_ENTY_SK=BILL_ENTY.BILL_ENTY_SK\n"
  "AND CLM.SRC_SYS_CD_SK = 1581 AND CLM.PCA_TYP_CD_SK <>1"
)

df_db2_IDS_FUND_PAYMENT_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_db2_IDS_FUND_PAYMENT_in)
    .load()
)

# --------------------------------------------------------------------------------
# xfm_businesslogic3
# --------------------------------------------------------------------------------
df_xfm_businesslogic3 = (
    df_db2_IDS_FUND_PAYMENT_in.alias("lnk_IdsFundPaymentExtr_InABC")
    .select(
        F.when(F.trim(F.col("SRC_SYS_CD")) == "", F.lit("UNK")).otherwise(F.col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
        F.col("ALT_FUND_INVC_ID").alias("ALT_FUND_INVC_ID"),
        F.lit("PAYMT").alias("TRGT_CD"),
        F.col("ALT_FUND_SK").alias("ALT_FUND_SK"),
        F.col("ALT_FUND_INVC_SK").alias("ALT_FUND_INVC_SK"),
        F.col("ALT_FUND_INVC_PAYMT_SK").alias("ALT_FUND_INVC_PAYMT_SK"),
        F.col("BILL_ENTY_SK").alias("BILL_ENTY_SK"),
        F.col("CLM_SK").alias("CLM_SK"),
        F.col("CLS_SK").alias("CLS_SK"),
        F.col("CLS_PLN_SK").alias("CLS_PLN_SK"),
        F.col("GRP_SK").alias("GRP_SK"),
        F.col("SUBGRP_SK").alias("SUBGRP_SK"),
        F.col("SUB_SK").alias("SUB_SK"),
        F.col("MBR_SK").alias("MBR_SK"),
        F.col("PROD_SK").alias("PROD_SK"),
        F.col("FUND_THRU_DT_SK").alias("FUND_THRU_DT_SK"),
        F.col("FUND_FROM_DT_SK").alias("FUND_FROM_DT_SK"),
        F.col("PD_DT_SK").alias("PD_DT_SK"),
        F.col("SVC_STRT_DT_SK").alias("SVC_STRT_DT_SK"),
        F.col("PCA_AMT").alias("PCA_AMT"),
        F.col("ALT_FUND_CNTR_PERD_NO").alias("ALT_FUND_CNTR_PERD_NO"),
        F.col("SEQ_NO").alias("SEQ_NO"),
        F.col("BILL_ENTY_UNIQ_KEY").alias("BILL_ENTY_UNIQ_KEY"),
        F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
        F.col("ALT_FUND_ID").alias("ALT_FUND_ID"),
        F.col("ALT_FUND_NM").alias("ALT_FUND_NM"),
        F.col("CLM_ID").alias("CLM_ID"),
        F.col("CLS_ID").alias("CLS_ID"),
        F.col("CLS_PLN_ID").alias("CLS_PLN_ID"),
        F.col("GRP_ID").alias("GRP_ID"),
        F.col("GRP_NM").alias("GRP_NM"),
        F.col("PROD_ID").alias("PROD_ID"),
        F.col("SUBGRP_ID").alias("SUBGRP_ID"),
    )
)

# --------------------------------------------------------------------------------
# db2_IDS_INVC_DSCRTN_in
# --------------------------------------------------------------------------------
query_db2_IDS_INVC_DSCRTN_in = (
  "SELECT \n"
  "INVC.ALT_FUND_INVC_SK,\n"
  "COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,\n"
  "INVC.ALT_FUND_INVC_ID,\n"
  "INVC.ALT_FUND_SK,\n"
  "INVC.BILL_ENTY_SK,\n"
  "INVC.FUND_FROM_DT_SK,\n"
  "INVC.FUND_THRU_DT_SK,\n"
  "ALT_FUND.ALT_FUND_ID,\n"
  "ALT_FUND.ALT_FUND_NM,\n"
  "DSCRTN.ALT_FUND_INVC_DSCRTN_SK,\n"
  "DSCRTN.SEQ_NO,\n"
  "DSCRTN.GRP_SK,\n"
  "DSCRTN.SUBGRP_SK,\n"
  "DSCRTN.INVC_DSCRTN_BEG_DT_SK,\n"
  "DSCRTN.INVC_DSCRTN_END_DT_SK,\n"
  "DSCRTN.EXTRA_CNTR_AMT,\n"
  "DSCRTN.DSCRTN_MO_QTY,\n"
  "DSCRTN.DSCRTN_DESC,\n"
  "DSCRTN.DSCRTN_PRSN_ID_TX,\n"
  "DSCRTN.DSCRTN_SH_DESC,\n"
  "GRP.GRP_ID,\n"
  "GRP.GRP_NM,\n"
  "SUBGRP.SUBGRP_ID,\n"
  "BILL_ENTY.BILL_ENTY_UNIQ_KEY\n"
  f"FROM {IDSOwner}.ALT_FUND_INVC INVC\n"
  f"LEFT JOIN {IDSOwner}.CD_MPPNG CD\n"
  "ON INVC.SRC_SYS_CD_SK = CD.CD_MPPNG_SK,\n"
  f"{IDSOwner}.ALT_FUND ALT_FUND,\n"
  f"{IDSOwner}.ALT_FUND_INVC_DSCRTN DSCRTN,\n"
  f"{IDSOwner}.GRP GRP,\n"
  f"{IDSOwner}.SUBGRP SUBGRP,\n"
  f"{IDSOwner}.BILL_ENTY BILL_ENTY\n"
  "WHERE\n"
  "INVC.ALT_FUND_SK=ALT_FUND.ALT_FUND_SK AND\n"
  "INVC.ALT_FUND_INVC_SK=DSCRTN.ALT_FUND_INVC_SK AND\n"
  "DSCRTN.GRP_SK=GRP.GRP_SK AND\n"
  "DSCRTN.SUBGRP_SK=SUBGRP.SUBGRP_SK AND\n"
  "INVC.BILL_ENTY_SK=BILL_ENTY.BILL_ENTY_SK AND\n"
  "INVC.ALT_FUND_INVC_SK<>0 AND INVC.ALT_FUND_INVC_SK<>1;"
)

df_db2_IDS_INVC_DSCRTN_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_db2_IDS_INVC_DSCRTN_in)
    .load()
)

# --------------------------------------------------------------------------------
# xfm_businesslogic2
# --------------------------------------------------------------------------------
df_xfm_businesslogic2 = (
    df_db2_IDS_INVC_DSCRTN_in.alias("lnk_IdsInvcDscrtnExtr_InABC")
    .select(
        F.when(F.trim(F.col("SRC_SYS_CD")) == "", F.lit("UNK")).otherwise(F.col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
        F.col("ALT_FUND_INVC_ID").alias("ALT_FUND_INVC_ID"),
        F.lit("DSCRTN").alias("TRGT_CD"),
        F.col("ALT_FUND_SK").alias("ALT_FUND_SK"),
        F.col("ALT_FUND_INVC_SK").alias("ALT_FUND_INVC_SK"),
        F.col("BILL_ENTY_SK").alias("BILL_ENTY_SK"),
        F.col("FUND_FROM_DT_SK").alias("FUND_FROM_DT_SK"),
        F.col("FUND_THRU_DT_SK").alias("FUND_THRU_DT_SK"),
        F.col("ALT_FUND_ID").alias("ALT_FUND_ID"),
        F.col("ALT_FUND_NM").alias("ALT_FUND_NM"),
        F.col("ALT_FUND_INVC_DSCRTN_SK").alias("ALT_FUND_INVC_DSCRTN_SK"),
        F.col("SEQ_NO").alias("SEQ_NO"),
        F.col("GRP_SK").alias("GRP_SK"),
        F.col("SUBGRP_SK").alias("SUBGRP_SK"),
        F.col("INVC_DSCRTN_BEG_DT_SK").alias("INVC_DSCRTN_BEG_DT_SK"),
        F.col("INVC_DSCRTN_END_DT_SK").alias("INVC_DSCRTN_END_DT_SK"),
        F.col("EXTRA_CNTR_AMT").alias("EXTRA_CNTR_AMT"),
        F.col("DSCRTN_MO_QTY").alias("DSCRTN_MO_QTY"),
        F.col("DSCRTN_DESC").alias("DSCRTN_DESC"),
        F.col("DSCRTN_PRSN_ID_TX").alias("DSCRTN_PRSN_ID_TX"),
        F.col("DSCRTN_SH_DESC").alias("DSCRTN_SH_DESC"),
        F.col("GRP_ID").alias("GRP_ID"),
        F.col("GRP_NM").alias("GRP_NM"),
        F.col("SUBGRP_ID").alias("SUBGRP_ID"),
        F.col("BILL_ENTY_UNIQ_KEY").alias("BILL_ENTY_UNIQ_KEY"),
    )
)

# --------------------------------------------------------------------------------
# db2_CD_MPPNG_in
# --------------------------------------------------------------------------------
query_db2_CD_MPPNG_in = (
  "SELECT \n"
  "COALESCE(CD_MPPNG.TRGT_CD ,'UNK')  TRGT_CD ,\n"
  "CD_MPPNG.TRGT_CD_NM,\n"
  "CD_MPPNG.CD_MPPNG_SK\n"
  f"FROM {IDSOwner}.CD_MPPNG CD_MPPNG\n"
  "WHERE\n"
  "CD_MPPNG.TRGT_DOMAIN_NM='ALTERNATE FUNDING INVOICE DETAIL'"
)

df_db2_CD_MPPNG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_db2_CD_MPPNG_in)
    .load()
)

# --------------------------------------------------------------------------------
# xfm_businesslogic1
# Three output links with different constraints: RDUCTN, PAYMT, DSCRTN
# --------------------------------------------------------------------------------
df_xfm_businesslogic1_all = df_db2_CD_MPPNG_in.alias("lnk_CdMppng_out")

df_xfm_businesslogic1_Ref_RductionCdLkup = (
    df_xfm_businesslogic1_all
    .filter(F.col("TRGT_CD") == F.lit("RDUCTN"))
    .select(
        F.col("TRGT_CD").alias("TRGT_CD"),
        F.col("TRGT_CD_NM").alias("TRGT_CD_NM"),
        F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
    )
)

df_xfm_businesslogic1_Ref_PaymentInvcCdLkup = (
    df_xfm_businesslogic1_all
    .filter(F.col("TRGT_CD") == F.lit("PAYMT"))
    .select(
        F.col("TRGT_CD").alias("TRGT_CD"),
        F.col("TRGT_CD_NM").alias("TRGT_CD_NM"),
        F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
    )
)

df_xfm_businesslogic1_Ref_DscrtnInvCdLkp = (
    df_xfm_businesslogic1_all
    .filter(F.col("TRGT_CD") == F.lit("DSCRTN"))
    .select(
        F.col("TRGT_CD").alias("TRGT_CD"),
        F.col("TRGT_CD_NM").alias("TRGT_CD_NM"),
        F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
    )
)

# --------------------------------------------------------------------------------
# lkp_BillCodes (PxLookup)
# --------------------------------------------------------------------------------
# Primary link: df_xfm_businesslogic4 (lnk_IdsBillRduction_out)
# Lookup link: df_xfm_businesslogic1_Ref_RductionCdLkup (Ref_RductionCdLkup)
# Join condition: lnk_IdsBillRduction_out.TRGT_CD = Ref_RductionCdLkup.TRGT_CD
df_lkp_BillCodes = (
    df_xfm_businesslogic4.alias("lnk_IdsBillRduction_out")
    .join(
        df_xfm_businesslogic1_Ref_RductionCdLkup.alias("Ref_RductionCdLkup"),
        F.col("lnk_IdsBillRduction_out.TRGT_CD") == F.col("Ref_RductionCdLkup.TRGT_CD"),
        how="left"
    )
)

df_lkp_BillCodesLkpData_out = df_lkp_BillCodes.select(
    F.col("lnk_IdsBillRduction_out.ALT_FUND_INVC_SK").alias("ALT_FUND_INVC_SK"),
    F.col("lnk_IdsBillRduction_out.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_IdsBillRduction_out.ALT_FUND_INVC_ID").alias("ALT_FUND_INVC_ID"),
    F.col("Ref_RductionCdLkup.TRGT_CD").alias("ALT_FUND_INVC_DTL_CD"),
    F.col("lnk_IdsBillRduction_out.ALT_FUND_SK").alias("ALT_FUND_SK"),
    F.col("lnk_IdsBillRduction_out.BILL_ENTY_SK").alias("BILL_ENTY_SK"),
    F.col("lnk_IdsBillRduction_out.BILL_RDUCTN_HIST_SK").alias("BILL_RDUCTN_HIST_SK"),
    F.col("lnk_IdsBillRduction_out.CLM_SK").alias("CLM_SK"),
    F.col("lnk_IdsBillRduction_out.CLS_SK").alias("CLS_SK"),
    F.col("lnk_IdsBillRduction_out.CLS_PLN_SK").alias("CLS_PLN_SK"),
    F.col("lnk_IdsBillRduction_out.GRP_SK").alias("GRP_SK"),
    F.col("lnk_IdsBillRduction_out.SUBGRP_SK").alias("SUBGRP_SK"),
    F.col("lnk_IdsBillRduction_out.SUB_SK").alias("SUB_SK"),
    F.col("lnk_IdsBillRduction_out.MBR_SK").alias("MBR_SK"),
    F.col("lnk_IdsBillRduction_out.PROD_SK").alias("PROD_SK"),
    F.col("lnk_IdsBillRduction_out.FUND_THRU_DT_SK").alias("FUND_THRU_DT_SK"),
    F.col("lnk_IdsBillRduction_out.FUND_FROM_DT_SK").alias("FUND_FROM_DT_SK"),
    F.col("lnk_IdsBillRduction_out.PD_DT_SK").alias("PD_DT_SK"),
    F.col("lnk_IdsBillRduction_out.SVC_STRT_DT_SK").alias("SVC_STRT_DT_SK"),
    F.col("lnk_IdsBillRduction_out.PCA_AMT").alias("PCA_AMT"),
    F.col("lnk_IdsBillRduction_out.ALT_FUND_CNTR_PERD_NO").alias("ALT_FUND_CNTR_PERD_NO"),
    F.col("lnk_IdsBillRduction_out.SEQ_NO").alias("SEQ_NO"),
    F.col("lnk_IdsBillRduction_out.BILL_ENTY_UNIQ_KEY").alias("BILL_ENTY_UNIQ_KEY"),
    F.col("lnk_IdsBillRduction_out.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("lnk_IdsBillRduction_out.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.col("lnk_IdsBillRduction_out.ALT_FUND_ID").alias("ALT_FUND_ID"),
    F.col("Ref_RductionCdLkup.TRGT_CD_NM").alias("TRGT_CD_NM"),
    F.col("lnk_IdsBillRduction_out.ALT_FUND_NM").alias("ALT_FUND_NM"),
    F.col("lnk_IdsBillRduction_out.CLM_ID").alias("CLM_ID"),
    F.col("lnk_IdsBillRduction_out.CLS_ID").alias("CLS_ID"),
    F.col("lnk_IdsBillRduction_out.CLS_PLN_ID").alias("CLS_PLN_ID"),
    F.col("lnk_IdsBillRduction_out.GRP_ID").alias("GRP_ID"),
    F.col("lnk_IdsBillRduction_out.GRP_NM").alias("GRP_NM"),
    F.col("lnk_IdsBillRduction_out.PROD_ID").alias("PROD_ID"),
    F.col("lnk_IdsBillRduction_out.SUBGRP_ID").alias("SUBGRP_ID"),
    F.col("Ref_RductionCdLkup.CD_MPPNG_SK").alias("CD_MPPNG_SK")
)

# --------------------------------------------------------------------------------
# xfm_BillSeqNo (CTransformerStage)
# Uses row-by-row logic: partition by SRC_SYS_CD, ALT_FUND_INVC_ID with sort order
# --------------------------------------------------------------------------------
windowSpec_BillSeqNo = Window.partitionBy("SRC_SYS_CD","ALT_FUND_INVC_ID").orderBy("ALT_FUND_INVC_SK","SRC_SYS_CD","ALT_FUND_INVC_ID")

df_xfm_BillSeqNo = (
    df_lkp_BillCodesLkpData_out
    .withColumn("ALT_FUND_INVC_DTL_SEQ_NO", F.row_number().over(windowSpec_BillSeqNo))
    .select(
        F.lit(0).alias("ALT_FUND_INVC_DTL_SK"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("ALT_FUND_INVC_ID").alias("ALT_FUND_INVC_ID"),
        F.col("ALT_FUND_INVC_DTL_CD").alias("ALT_FUND_INVC_DTL_CD"),
        F.col("ALT_FUND_INVC_DTL_SEQ_NO").alias("ALT_FUND_INVC_DTL_SEQ_NO"),
        F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.col("ALT_FUND_SK").alias("ALT_FUND_SK"),
        F.col("ALT_FUND_INVC_SK").alias("ALT_FUND_INVC_SK"),
        F.lit(1).alias("ALT_FUND_INVC_DSCRTN_SK"),
        F.lit(1).alias("ALT_FUND_INVC_PAYMT_SK"),
        F.col("BILL_ENTY_SK").alias("BILL_ENTY_SK"),
        F.col("BILL_RDUCTN_HIST_SK").alias("BILL_RDUCTN_HIST_SK"),
        F.col("CLM_SK").alias("CLM_SK"),
        F.col("CLS_SK").alias("CLS_SK"),
        F.col("CLS_PLN_SK").alias("CLS_PLN_SK"),
        F.col("GRP_SK").alias("GRP_SK"),
        F.col("SUBGRP_SK").alias("SUBGRP_SK"),
        F.col("SUB_SK").alias("SUB_SK"),
        F.col("MBR_SK").alias("MBR_SK"),
        F.col("PROD_SK").alias("PROD_SK"),
        F.lit("1753-01-01").alias("ALT_FUND_INVC_DSCRTN_BEG_DT_SK"),
        F.lit("2199-12-31").alias("ALT_FUND_INVC_DSCRTN_END_DT_SK"),
        F.col("FUND_THRU_DT_SK").alias("ALT_FUND_INVC_FUND_THRU_DT_SK"),
        F.col("FUND_FROM_DT_SK").alias("ALT_FUND_INVC_FUND_FROM_DT_SK"),
        F.col("PD_DT_SK").alias("CLM_PD_DT_SK"),
        F.concat(F.col("PD_DT_SK").substr(F.lit(1),F.lit(4)),F.col("PD_DT_SK").substr(F.lit(6),F.lit(2))).alias("CLM_PD_YR_MO_SK"),
        F.col("SVC_STRT_DT_SK").alias("CLM_SVC_STRT_DT_SK"),
        F.concat(F.col("SVC_STRT_DT_SK").substr(F.lit(1),F.lit(4)),F.col("SVC_STRT_DT_SK").substr(F.lit(6),F.lit(2))).alias("CLM_SVC_STRT_YR_MO_SK"),
        F.col("PCA_AMT").alias("ALT_FUND_INVC_DTL_BILL_AMT"),
        F.col("ALT_FUND_CNTR_PERD_NO").alias("ALT_FUND_CNTR_PERD_NO"),
        F.lit(0).alias("ALT_FUND_INVC_DSCRTN_MO_QTY"),
        F.lit(0).alias("ALT_FUND_INVC_DSCRTN_SEQ_NO"),
        F.lit(0).alias("ALT_FUND_INVC_PAYMT_SEQ_NO"),
        F.col("SEQ_NO").alias("BILL_RDUCTN_HIST_SEQ_NO"),
        F.col("BILL_ENTY_UNIQ_KEY").alias("BILL_ENTY_UNIQ_KEY"),
        F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
        F.col("ALT_FUND_ID").alias("ALT_FUND_ID"),
        F.col("TRGT_CD_NM").alias("ALT_FUND_INVC_DTL_NM"),
        F.lit(None).alias("ALT_FUND_INVC_DSCRTN_DESC"),
        F.lit(None).alias("AF_INVC_DSCRTN_PRSN_ID_TX"),
        F.lit(None).alias("ALT_FUND_INVC_DSCRTN_SH_DESC"),
        F.col("ALT_FUND_NM").alias("ALT_FUND_NM"),
        F.col("CLM_ID").alias("CLM_ID"),
        F.col("CLS_ID").alias("CLS_ID"),
        F.col("CLS_PLN_ID").alias("CLS_PLN_ID"),
        F.col("GRP_ID").alias("GRP_ID"),
        F.col("GRP_NM").alias("GRP_NM"),
        F.col("PROD_ID").alias("PROD_ID"),
        F.col("SUBGRP_ID").alias("SUBGRP_ID"),
        F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("CD_MPPNG_SK").alias("ALT_FUND_INVC_DTL_CD_SK")
    )
)

# --------------------------------------------------------------------------------
# lkp_FundCodes (PxLookup)
# --------------------------------------------------------------------------------
# Primary link: df_xfm_businesslogic3 (lnk_IdsFundPayment_out)
# Lookup link: df_xfm_businesslogic1_Ref_PaymentInvcCdLkup (Ref_PaymentInvcCdLkup)
df_lkp_FundCodes = (
    df_xfm_businesslogic3.alias("lnk_IdsFundPayment_out")
    .join(
        df_xfm_businesslogic1_Ref_PaymentInvcCdLkup.alias("Ref_PaymentInvcCdLkup"),
        F.col("lnk_IdsFundPayment_out.TRGT_CD") == F.col("Ref_PaymentInvcCdLkup.TRGT_CD"),
        how="left"
    )
)

df_lkp_FundCodesLkpData_out = df_lkp_FundCodes.select(
    F.col("lnk_IdsFundPayment_out.ALT_FUND_INVC_SK").alias("ALT_FUND_INVC_SK"),
    F.col("lnk_IdsFundPayment_out.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_IdsFundPayment_out.ALT_FUND_INVC_ID").alias("ALT_FUND_INVC_ID"),
    F.col("Ref_PaymentInvcCdLkup.TRGT_CD").alias("ALT_FUND_INVC_DTL_CD"),
    F.col("lnk_IdsFundPayment_out.ALT_FUND_SK").alias("ALT_FUND_SK"),
    F.col("lnk_IdsFundPayment_out.ALT_FUND_INVC_PAYMT_SK").alias("ALT_FUND_INVC_PAYMT_SK"),
    F.col("lnk_IdsFundPayment_out.BILL_ENTY_SK").alias("BILL_ENTY_SK"),
    F.col("lnk_IdsFundPayment_out.CLM_SK").alias("CLM_SK"),
    F.col("lnk_IdsFundPayment_out.CLS_SK").alias("CLS_SK"),
    F.col("lnk_IdsFundPayment_out.CLS_PLN_SK").alias("CLS_PLN_SK"),
    F.col("lnk_IdsFundPayment_out.GRP_SK").alias("GRP_SK"),
    F.col("lnk_IdsFundPayment_out.SUBGRP_SK").alias("SUBGRP_SK"),
    F.col("lnk_IdsFundPayment_out.SUB_SK").alias("SUB_SK"),
    F.col("lnk_IdsFundPayment_out.MBR_SK").alias("MBR_SK"),
    F.col("lnk_IdsFundPayment_out.PROD_SK").alias("PROD_SK"),
    F.col("lnk_IdsFundPayment_out.FUND_THRU_DT_SK").alias("FUND_THRU_DT_SK"),
    F.col("lnk_IdsFundPayment_out.FUND_FROM_DT_SK").alias("FUND_FROM_DT_SK"),
    F.col("lnk_IdsFundPayment_out.PD_DT_SK").alias("PD_DT_SK"),
    F.col("lnk_IdsFundPayment_out.SVC_STRT_DT_SK").alias("SVC_STRT_DT_SK"),
    F.col("lnk_IdsFundPayment_out.PCA_AMT").alias("PCA_AMT"),
    F.col("lnk_IdsFundPayment_out.ALT_FUND_CNTR_PERD_NO").alias("ALT_FUND_CNTR_PERD_NO"),
    F.col("lnk_IdsFundPayment_out.SEQ_NO").alias("SEQ_NO"),
    F.col("lnk_IdsFundPayment_out.BILL_ENTY_UNIQ_KEY").alias("BILL_ENTY_UNIQ_KEY"),
    F.col("lnk_IdsFundPayment_out.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("lnk_IdsFundPayment_out.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.col("lnk_IdsFundPayment_out.ALT_FUND_ID").alias("ALT_FUND_ID"),
    F.col("Ref_PaymentInvcCdLkup.TRGT_CD_NM").alias("TRGT_CD_NM"),
    F.col("lnk_IdsFundPayment_out.ALT_FUND_NM").alias("ALT_FUND_NM"),
    F.col("lnk_IdsFundPayment_out.CLM_ID").alias("CLM_ID"),
    F.col("lnk_IdsFundPayment_out.CLS_ID").alias("CLS_ID"),
    F.col("lnk_IdsFundPayment_out.CLS_PLN_ID").alias("CLS_PLN_ID"),
    F.col("lnk_IdsFundPayment_out.GRP_ID").alias("GRP_ID"),
    F.col("lnk_IdsFundPayment_out.GRP_NM").alias("GRP_NM"),
    F.col("lnk_IdsFundPayment_out.PROD_ID").alias("PROD_ID"),
    F.col("lnk_IdsFundPayment_out.SUBGRP_ID").alias("SUBGRP_ID"),
    F.col("Ref_PaymentInvcCdLkup.CD_MPPNG_SK").alias("CD_MPPNG_SK")
)

# --------------------------------------------------------------------------------
# xfm_FundSeqNo
# Has three outputs: main, UNK, NA. 
# We'll produce them by row_number plus union with single rows for UNK and NA.
# --------------------------------------------------------------------------------
windowSpec_FundSeqNo = Window.partitionBy("SRC_SYS_CD","ALT_FUND_INVC_ID").orderBy("ALT_FUND_INVC_SK","SRC_SYS_CD","ALT_FUND_INVC_ID")

df_FundSeqNo_main = (
    df_lkp_FundCodesLkpData_out
    .withColumn("ALT_FUND_INVC_DTL_SEQ_NO", F.row_number().over(windowSpec_FundSeqNo))
    .select(
        F.lit(0).alias("ALT_FUND_INVC_DTL_SK"),
        F.col("SRC_SYS_CD"),
        F.col("ALT_FUND_INVC_ID"),
        F.col("ALT_FUND_INVC_DTL_CD"),
        F.col("ALT_FUND_INVC_DTL_SEQ_NO"),
        F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.col("ALT_FUND_SK"),
        F.col("ALT_FUND_INVC_SK"),
        F.lit(1).alias("ALT_FUND_INVC_DSCRTN_SK"),
        F.col("ALT_FUND_INVC_PAYMT_SK"),
        F.col("BILL_ENTY_SK"),
        F.lit(1).alias("BILL_RDUCTN_HIST_SK"),
        F.col("CLM_SK"),
        F.col("CLS_SK"),
        F.col("CLS_PLN_SK"),
        F.col("GRP_SK"),
        F.col("SUBGRP_SK"),
        F.col("SUB_SK"),
        F.col("MBR_SK"),
        F.col("PROD_SK"),
        F.lit("1753-01-01").alias("ALT_FUND_INVC_DSCRTN_BEG_DT_SK"),
        F.lit("2199-12-31").alias("ALT_FUND_INVC_DSCRTN_END_DT_SK"),
        F.col("FUND_THRU_DT_SK").alias("ALT_FUND_INVC_FUND_THRU_DT_SK"),
        F.col("FUND_FROM_DT_SK").alias("ALT_FUND_INVC_FUND_FROM_DT_SK"),
        F.col("PD_DT_SK").alias("CLM_PD_DT_SK"),
        F.concat(F.col("PD_DT_SK").substr(F.lit(1),F.lit(4)),F.col("PD_DT_SK").substr(F.lit(6),F.lit(2))).alias("CLM_PD_YR_MO_SK"),
        F.col("SVC_STRT_DT_SK").alias("CLM_SVC_STRT_DT_SK"),
        F.concat(F.col("SVC_STRT_DT_SK").substr(F.lit(1),F.lit(4)),F.col("SVC_STRT_DT_SK").substr(F.lit(6),F.lit(2))).alias("CLM_SVC_STRT_YR_MO_SK"),
        F.col("PCA_AMT").alias("ALT_FUND_INVC_DTL_BILL_AMT"),
        F.col("ALT_FUND_CNTR_PERD_NO").alias("ALT_FUND_CNTR_PERD_NO"),
        F.lit(0).alias("ALT_FUND_INVC_DSCRTN_MO_QTY"),
        F.lit(0).alias("ALT_FUND_INVC_DSCRTN_SEQ_NO"),
        F.col("SEQ_NO").alias("ALT_FUND_INVC_PAYMT_SEQ_NO"),
        F.lit(0).alias("BILL_RDUCTN_HIST_SEQ_NO"),
        F.col("BILL_ENTY_UNIQ_KEY"),
        F.col("MBR_UNIQ_KEY"),
        F.col("SUB_UNIQ_KEY"),
        F.col("ALT_FUND_ID"),
        F.col("TRGT_CD_NM").alias("ALT_FUND_INVC_DTL_NM"),
        F.lit(None).alias("ALT_FUND_INVC_DSCRTN_DESC"),
        F.lit(None).alias("AF_INVC_DSCRTN_PRSN_ID_TX"),
        F.lit(None).alias("ALT_FUND_INVC_DSCRTN_SH_DESC"),
        F.col("ALT_FUND_NM"),
        F.col("CLM_ID"),
        F.col("CLS_ID"),
        F.col("CLS_PLN_ID"),
        F.col("GRP_ID"),
        F.col("GRP_NM"),
        F.col("PROD_ID"),
        F.col("SUBGRP_ID"),
        F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("CD_MPPNG_SK").alias("ALT_FUND_INVC_DTL_CD_SK")
    )
)

schema_fundSeqNo = df_FundSeqNo_main.schema

df_FundSeqNo_UNK = spark.createDataFrame([
    (
        0,"UNK","UNK","UNK",0,
        "1753-01-01","1753-01-01",
        0,0,0,0,0,
        0,0,0,0,0,0,0,0,
        "1753-01-01","1753-01-01",
        "1753-01-01","1753-01-01",
        "1753-01-01","175301",
        "1753-01-01","175301",
        0,None,None,None,None,None,
        0,0,0,"UNK","UNK","", "", "",
        "UNK","UNK","UNK","UNK","UNK","UNK","UNK","UNK",100,100,0
    )
], schema_fundSeqNo)

df_FundSeqNo_NA = spark.createDataFrame([
    (
        1,"NA","NA","NA",0,
        "1753-01-01","1753-01-01",
        1,1,1,1,1,
        1,1,1,1,1,1,1,1,
        "1753-01-01","1753-01-01",
        "1753-01-01","1753-01-01",
        "1753-01-01","175301",
        "1753-01-01","175301",
        0,None,None,None,None,None,
        1,1,1,"NA","NA","", "", "",
        "NA","NA","NA","NA","NA","NA","NA","NA",100,100,1
    )
], schema_fundSeqNo)

df_xfm_FundSeqNo = df_FundSeqNo_main.unionByName(df_FundSeqNo_UNK).unionByName(df_FundSeqNo_NA)

# --------------------------------------------------------------------------------
# lkp_DscrtnCodes (PxLookup)
# --------------------------------------------------------------------------------
# Primary link: df_xfm_businesslogic2 (lnk_IdsInvDscrtn_out)
# Lookup link: df_xfm_businesslogic1_Ref_DscrtnInvCdLkp (Ref_DscrtnInvCdLkp)
df_lkp_DscrtnCodes = (
    df_xfm_businesslogic2.alias("lnk_IdsInvDscrtn_out")
    .join(
        df_xfm_businesslogic1_Ref_DscrtnInvCdLkp.alias("Ref_DscrtnInvCdLkp"),
        F.col("lnk_IdsInvDscrtn_out.TRGT_CD") == F.col("Ref_DscrtnInvCdLkp.TRGT_CD"),
        how="left"
    )
)

df_lkp_DscrtnCodesLkpData_out = df_lkp_DscrtnCodes.select(
    F.col("lnk_IdsInvDscrtn_out.ALT_FUND_INVC_SK").alias("ALT_FUND_INVC_SK"),
    F.col("lnk_IdsInvDscrtn_out.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_IdsInvDscrtn_out.ALT_FUND_INVC_ID").alias("ALT_FUND_INVC_ID"),
    F.col("Ref_DscrtnInvCdLkp.TRGT_CD").alias("ALT_FUND_INVC_DTL_CD"),
    F.col("lnk_IdsInvDscrtn_out.ALT_FUND_SK").alias("ALT_FUND_SK"),
    F.col("lnk_IdsInvDscrtn_out.ALT_FUND_INVC_DSCRTN_SK").alias("ALT_FUND_INVC_DSCRTN_SK"),
    F.col("lnk_IdsInvDscrtn_out.BILL_ENTY_SK").alias("BILL_ENTY_SK"),
    F.col("lnk_IdsInvDscrtn_out.GRP_SK").alias("GRP_SK"),
    F.col("lnk_IdsInvDscrtn_out.SUBGRP_SK").alias("SUBGRP_SK"),
    F.col("lnk_IdsInvDscrtn_out.INVC_DSCRTN_BEG_DT_SK").alias("INVC_DSCRTN_BEG_DT_SK"),
    F.col("lnk_IdsInvDscrtn_out.INVC_DSCRTN_END_DT_SK").alias("INVC_DSCRTN_END_DT_SK"),
    F.col("lnk_IdsInvDscrtn_out.FUND_THRU_DT_SK").alias("FUND_THRU_DT_SK"),
    F.col("lnk_IdsInvDscrtn_out.FUND_FROM_DT_SK").alias("FUND_FROM_DT_SK"),
    F.col("lnk_IdsInvDscrtn_out.EXTRA_CNTR_AMT").alias("EXTRA_CNTR_AMT"),
    F.col("lnk_IdsInvDscrtn_out.DSCRTN_MO_QTY").alias("DSCRTN_MO_QTY"),
    F.col("lnk_IdsInvDscrtn_out.SEQ_NO").alias("SEQ_NO"),
    F.col("lnk_IdsInvDscrtn_out.BILL_ENTY_UNIQ_KEY").alias("BILL_ENTY_UNIQ_KEY"),
    F.col("lnk_IdsInvDscrtn_out.ALT_FUND_ID").alias("ALT_FUND_ID"),
    F.col("Ref_DscrtnInvCdLkp.TRGT_CD_NM").alias("TRGT_CD_NM"),
    F.col("lnk_IdsInvDscrtn_out.DSCRTN_DESC").alias("DSCRTN_DESC"),
    F.col("lnk_IdsInvDscrtn_out.DSCRTN_PRSN_ID_TX").alias("DSCRTN_PRSN_ID_TX"),
    F.col("lnk_IdsInvDscrtn_out.DSCRTN_SH_DESC").alias("DSCRTN_SH_DESC"),
    F.col("lnk_IdsInvDscrtn_out.ALT_FUND_NM").alias("ALT_FUND_NM"),
    F.col("lnk_IdsInvDscrtn_out.GRP_ID").alias("GRP_ID"),
    F.col("lnk_IdsInvDscrtn_out.GRP_NM").alias("GRP_NM"),
    F.col("lnk_IdsInvDscrtn_out.SUBGRP_ID").alias("SUBGRP_ID"),
    F.col("Ref_DscrtnInvCdLkp.CD_MPPNG_SK").alias("CD_MPPNG_SK")
)

# --------------------------------------------------------------------------------
# xfm_DscrthSeqNo
# Uses row-by-row logic again
# --------------------------------------------------------------------------------
windowSpec_DscrthSeqNo = Window.partitionBy("SRC_SYS_CD","ALT_FUND_INVC_ID").orderBy("ALT_FUND_INVC_SK","SRC_SYS_CD","ALT_FUND_INVC_ID")

df_xfm_DscrthSeqNo = (
    df_lkp_DscrtnCodesLkpData_out
    .withColumn("ALT_FUND_INVC_DTL_SEQ_NO", F.row_number().over(windowSpec_DscrthSeqNo))
    .select(
        F.lit(0).alias("ALT_FUND_INVC_DTL_SK"),
        F.col("SRC_SYS_CD"),
        F.col("ALT_FUND_INVC_ID"),
        F.col("ALT_FUND_INVC_DTL_CD"),
        F.col("ALT_FUND_INVC_DTL_SEQ_NO"),
        F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.col("ALT_FUND_SK"),
        F.col("ALT_FUND_INVC_SK"),
        F.col("ALT_FUND_INVC_DSCRTN_SK"),
        F.lit(1).alias("ALT_FUND_INVC_PAYMT_SK"),
        F.col("BILL_ENTY_SK"),
        F.lit(1).alias("BILL_RDUCTN_HIST_SK"),
        F.lit(1).alias("CLM_SK"),
        F.lit(1).alias("CLS_SK"),
        F.lit(1).alias("CLS_PLN_SK"),
        F.col("GRP_SK"),
        F.col("SUBGRP_SK"),
        F.lit(1).alias("SUB_SK"),
        F.lit(1).alias("MBR_SK"),
        F.lit(1).alias("PROD_SK"),
        F.col("INVC_DSCRTN_BEG_DT_SK").alias("ALT_FUND_INVC_DSCRTN_BEG_DT_SK"),
        F.col("INVC_DSCRTN_END_DT_SK").alias("ALT_FUND_INVC_DSCRTN_END_DT_SK"),
        F.col("FUND_THRU_DT_SK").alias("ALT_FUND_INVC_FUND_THRU_DT_SK"),
        F.col("FUND_FROM_DT_SK").alias("ALT_FUND_INVC_FUND_FROM_DT_SK"),
        F.lit("1753-01-01").alias("CLM_PD_DT_SK"),
        F.lit("175301").alias("CLM_PD_YR_MO_SK"),
        F.lit("1753-01-01").alias("CLM_SVC_STRT_DT_SK"),
        F.lit("175301").alias("CLM_SVC_STRT_YR_MO_SK"),
        F.col("EXTRA_CNTR_AMT").alias("ALT_FUND_INVC_DTL_BILL_AMT"),
        F.lit(0).alias("ALT_FUND_CNTR_PERD_NO"),
        F.col("DSCRTN_MO_QTY").alias("ALT_FUND_INVC_DSCRTN_MO_QTY"),
        F.col("SEQ_NO").alias("ALT_FUND_INVC_DSCRTN_SEQ_NO"),
        F.lit(0).alias("ALT_FUND_INVC_PAYMT_SEQ_NO"),
        F.lit(0).alias("BILL_RDUCTN_HIST_SEQ_NO"),
        F.col("BILL_ENTY_UNIQ_KEY"),
        F.lit(None).alias("MBR_UNIQ_KEY"),
        F.lit(None).alias("SUB_UNIQ_KEY"),
        F.col("ALT_FUND_ID"),
        F.col("TRGT_CD_NM").alias("ALT_FUND_INVC_DTL_NM"),
        F.when(F.trim(F.col("DSCRTN_DESC"))=="", None).otherwise(F.col("DSCRTN_DESC")).alias("ALT_FUND_INVC_DSCRTN_DESC"),
        F.when(F.trim(F.col("DSCRTN_PRSN_ID_TX"))=="", None).otherwise(F.col("DSCRTN_PRSN_ID_TX")).alias("AF_INVC_DSCRTN_PRSN_ID_TX"),
        F.when(F.trim(F.col("DSCRTN_SH_DESC"))=="", None).otherwise(F.col("DSCRTN_SH_DESC")).alias("ALT_FUND_INVC_DSCRTN_SH_DESC"),
        F.col("ALT_FUND_NM"),
        F.lit("NA").alias("CLM_ID"),
        F.lit("NA").alias("CLS_ID"),
        F.lit("NA").alias("CLS_PLN_ID"),
        F.col("GRP_ID"),
        F.col("GRP_NM"),
        F.lit("NA").alias("PROD_ID"),
        F.col("SUBGRP_ID"),
        F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("CD_MPPNG_SK").alias("ALT_FUND_INVC_DTL_CD_SK")
    )
)

# --------------------------------------------------------------------------------
# fnl_Data (PxFunnel): merges 5 inputs
# 1) df_xfm_DscrthSeqNo
# 2) df_xfm_BillSeqNo
# 3) df_xfm_FundSeqNo main+UNK+NA (already unioned as df_xfm_FundSeqNo)
# --------------------------------------------------------------------------------
df_fnl_Data = (
    df_xfm_DscrthSeqNo
    .unionByName(df_xfm_BillSeqNo)
    .unionByName(df_xfm_FundSeqNo)
)

# --------------------------------------------------------------------------------
# ds_ALT_FUND_INVC_DTL_PKey_out (PxDataSet) => write to parquet
# Maintain column order, apply rpad for char columns
# --------------------------------------------------------------------------------
final_columns_in_order = [
    "ALT_FUND_INVC_DTL_SK",
    "SRC_SYS_CD",
    "ALT_FUND_INVC_ID",
    "ALT_FUND_INVC_DTL_CD",
    "ALT_FUND_INVC_DTL_SEQ_NO",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "ALT_FUND_SK",
    "ALT_FUND_INVC_SK",
    "ALT_FUND_INVC_DSCRTN_SK",
    "ALT_FUND_INVC_PAYMT_SK",
    "BILL_ENTY_SK",
    "BILL_RDUCTN_HIST_SK",
    "CLM_SK",
    "CLS_SK",
    "CLS_PLN_SK",
    "GRP_SK",
    "SUBGRP_SK",
    "SUB_SK",
    "MBR_SK",
    "PROD_SK",
    "ALT_FUND_INVC_DSCRTN_BEG_DT_SK",
    "ALT_FUND_INVC_DSCRTN_END_DT_SK",
    "ALT_FUND_INVC_FUND_THRU_DT_SK",
    "ALT_FUND_INVC_FUND_FROM_DT_SK",
    "CLM_PD_DT_SK",
    "CLM_PD_YR_MO_SK",
    "CLM_SVC_STRT_DT_SK",
    "CLM_SVC_STRT_YR_MO_SK",
    "ALT_FUND_INVC_DTL_BILL_AMT",
    "ALT_FUND_CNTR_PERD_NO",
    "ALT_FUND_INVC_DSCRTN_MO_QTY",
    "ALT_FUND_INVC_DSCRTN_SEQ_NO",
    "ALT_FUND_INVC_PAYMT_SEQ_NO",
    "BILL_RDUCTN_HIST_SEQ_NO",
    "BILL_ENTY_UNIQ_KEY",
    "MBR_UNIQ_KEY",
    "SUB_UNIQ_KEY",
    "ALT_FUND_ID",
    "ALT_FUND_INVC_DTL_NM",
    "ALT_FUND_INVC_DSCRTN_DESC",
    "AF_INVC_DSCRTN_PRSN_ID_TX",
    "ALT_FUND_INVC_DSCRTN_SH_DESC",
    "ALT_FUND_NM",
    "CLM_ID",
    "CLS_ID",
    "CLS_PLN_ID",
    "GRP_ID",
    "GRP_NM",
    "PROD_ID",
    "SUBGRP_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "ALT_FUND_INVC_DTL_CD_SK"
]

# Identify which are char columns and their lengths (from the job metadata):
char_lengths = {
    "CRT_RUN_CYC_EXCTN_DT_SK": 10,
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK": 10,
    "ALT_FUND_INVC_DSCRTN_BEG_DT_SK": 10,
    "ALT_FUND_INVC_DSCRTN_END_DT_SK": 10,
    "ALT_FUND_INVC_FUND_THRU_DT_SK": 10,
    "ALT_FUND_INVC_FUND_FROM_DT_SK": 10,
    "CLM_PD_DT_SK": 10,
    "CLM_PD_YR_MO_SK": 6,
    "CLM_SVC_STRT_DT_SK": 10,
    "CLM_SVC_STRT_YR_MO_SK": 6,
    "PROD_ID": 8
}

df_final_select = df_fnl_Data.select(
    *[
        F.rpad(F.col(c), char_lengths[c], " ").alias(c)
        if c in char_lengths else F.col(c)
        for c in final_columns_in_order
    ]
)

write_files(
    df_final_select,
    "ALT_FUND_INVC_DTL_F.parquet",
    delimiter=",",
    mode="overwrite",
    is_parqruet=True,
    header=True,
    quote='"',
    nullValue=None
)