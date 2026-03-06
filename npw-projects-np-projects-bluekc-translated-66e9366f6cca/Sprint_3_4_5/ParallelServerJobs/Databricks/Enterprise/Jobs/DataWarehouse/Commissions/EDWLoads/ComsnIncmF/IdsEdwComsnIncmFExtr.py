# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2012  Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  EdwCommissionMonthlyExtrSeq
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                                    Development Project     Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    --------------------------------------    ---------------------------------------------------------                           ----------------------------------    ---------------------------------    -------------------------   
# MAGIC Bhoomi Dasari                 2012-11-01                   4873                      Original Programming.                                                 EnterpriseNewDevl         Kalyan Neelam             2012-11-06  
# MAGIC 
# MAGIC Rama Kamjula                  2013-12-10                 5114                   Rewritten from Server to Parallel version                            EnterpriseWrhsDevl    Jag Yelavarthi               2013-12-22
# MAGIC 
# MAGIC T.Sieg                             2021-03-02             335378                           Adding 16 new columns to source and target            EnterpriseDev2             Raja Gummadi             2021-05-03
# MAGIC                                                                                                              for ACA and MA reporting

# MAGIC JobName: IdsEdwAgntRelshpDExtr
# MAGIC 
# MAGIC Pull From IDS Commission Schedule  and Updates  EDW  COMSN_INCM_F
# MAGIC IDS Extract for COMSN_INCM  Based on Last_Updt_Run_Cyc_Exctn_Sk
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
from pyspark.sql.types import StructType,StructField,StringType,DecimalType,IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
CurrRunCycleDate = get_widget_value('CurrRunCycleDate','')
IDSRunCycle = get_widget_value('IDSRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

df_db2_CD_MPPNG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"SELECT CD_MPPNG_SK, TRGT_CD, TRGT_CD_NM, 'Y' as IND FROM {IDSOwner}.CD_MPPNG"
    )
    .load()
)

df_lnk_comsnbsssrccd = df_db2_CD_MPPNG.select("CD_MPPNG_SK","TRGT_CD","TRGT_CD_NM")
df_lnk_agntsttuscd = df_db2_CD_MPPNG.select("CD_MPPNG_SK","TRGT_CD","TRGT_CD_NM")
df_lnk_comsncovcatcd = df_db2_CD_MPPNG.select("CD_MPPNG_SK","TRGT_CD","TRGT_CD_NM")
df_lnk_Comtypcd = df_db2_CD_MPPNG.select("CD_MPPNG_SK","TRGT_CD","TRGT_CD_NM")
df_lnk_comsnmtdtypcd = df_db2_CD_MPPNG.select("CD_MPPNG_SK","TRGT_CD","TRGT_CD_NM")
df_lnk_Cttypcd = df_db2_CD_MPPNG.select("CD_MPPNG_SK","TRGT_CD","TRGT_CD_NM")
df_lnk_funcatcd = df_db2_CD_MPPNG.select("CD_MPPNG_SK","TRGT_CD","TRGT_CD_NM")
df_lnk_agnttierlvlcd = df_db2_CD_MPPNG.select("CD_MPPNG_SK","TRGT_CD","TRGT_CD_NM")
df_lnk_billentylvlcd = df_db2_CD_MPPNG.select("CD_MPPNG_SK","TRGT_CD","TRGT_CD_NM","IND")

df_db2_COMSN_INCM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
        SELECT
        COMSN_INCM.COMSN_INCM_SK,
        COMSN_INCM.PD_AGNT_ID,
        COMSN_INCM.ERN_AGNT_ID,
        COMSN_INCM.COMSN_PD_DT_SK,
        COMSN_INCM.BILL_ENTY_UNIQ_KEY,
        COMSN_INCM.BILL_INCM_RCPT_BILL_DUE_DT_SK,
        COMSN_INCM.COMSN_INCM_SEQ_NO,
        COMSN_INCM.CLS_PLN_ID,
        COMSN_INCM.PROD_ID,
        COMSN_INCM.COMSN_DTL_INCM_DISP_CD,
        COMSN_INCM.COMSN_TYP_CD,
        COALESCE(CD.TRGT_CD, 'UNK') SRC_SYS_CD,
        COMSN_INCM.CRT_RUN_CYC_EXCTN_SK,
        COMSN_INCM.LAST_UPDT_RUN_CYC_EXCTN_SK,
        COMSN_INCM.AGNY_AGNT_SK,
        COMSN_INCM.BILL_INCM_RCPT_SK,
        COMSN_INCM.BILL_ENTY_SK,
        COMSN_INCM.BILL_SUM_SK,
        COMSN_INCM.CLS_PLN_SK,
        COMSN_INCM.COMSN_AGMNT_SK,
        COMSN_INCM.ERN_AGNT_SK,
        COMSN_INCM.EXPRNC_CAT_SK,
        COMSN_INCM.FNCL_LOB_SK,
        COMSN_INCM.PD_AGNT_SK,
        COMSN_INCM.PROD_SK,
        COMSN_INCM.PROD_SH_NM_SK,
        COMSN_INCM.AGNT_STTUS_CD_SK,
        COMSN_INCM.AGNT_TIER_LVL_CD_SK,
        COMSN_INCM.COMSN_BSS_SRC_CD_SK,
        COMSN_INCM.COMSN_COV_CAT_CD_SK,
        COMSN_INCM.COMSN_DTL_INCM_DISP_CD_SK,
        COMSN_INCM.COMSN_MTHDLGY_TYP_CD_SK,
        COMSN_INCM.COMSN_TYP_CD_SK,
        COMSN_INCM.CT_TYP_CD_SK,
        COMSN_INCM.FUND_CAT_CD_SK,
        COMSN_INCM.COMSN_PD_YR_MO,
        COMSN_INCM.CONT_COV_STRT_DT_SK,
        COMSN_INCM.COV_END_DT_SK,
        COMSN_INCM.COV_PLN_YR,
        COMSN_INCM.DP_CANC_YR_MO,
        COMSN_INCM.PLN_YR_COV_EFF_DT_SK,
        COMSN_INCM.PRM_PD_TO_DT_SK,
        COMSN_INCM.COMSN_BSS_AMT,
        COMSN_INCM.COMSN_RATE_AMT,
        COMSN_INCM.PD_COMSN_AMT,
        COMSN_INCM.BILL_COMSN_PRM_PCT,
        COMSN_INCM.COMSN_AGMNT_SCHD_FCTR,
        COMSN_INCM.COMSN_RATE_PCT,
        COMSN_INCM.CNTR_CT,
        COMSN_INCM.MBR_CT,
        COMSN_INCM.SRC_TRANS_TYP_TX,
        COMSN_INCM.ACA_BUS_SBSDY_TYP_ID,
        COMSN_INCM.ACA_PRM_SBSDY_AMT,
        COMSN_INCM.AGNY_HIER_LVL_ID,
        COMSN_INCM.CMS_CUST_POL_ID,
        COMSN_INCM.CMS_PLN_BNF_PCKG_ID,
        COMSN_INCM.COMSN_BLUE_KC_BRKR_PAYMT_IN,
        COMSN_INCM.COMSN_PAYOUT_TYP_ID,
        COMSN_INCM.COMSN_RETROACTV_ADJ_IN,
        COMSN_INCM.CUST_BUS_TYP_ID,
        COMSN_INCM.CUST_BUS_SUBTYP_ID,
        COMSN_INCM.ICM_COMSN_PAYE_ID,
        COMSN_INCM.MCARE_ADVNTG_ENR_CYC_YR_ID,
        COMSN_INCM.MCARE_BNFCRY_ID,
        COMSN_INCM.MCARE_ENR_TYP_ID,
        COMSN_INCM.TRANS_CNTR_CT,
        COMSN_INCM.TRNSMSN_SRC_SYS_CD
        FROM {IDSOwner}.COMSN_INCM COMSN_INCM
        LEFT JOIN {IDSOwner}.CD_MPPNG CD ON CD.CD_MPPNG_SK = COMSN_INCM.SRC_SYS_CD_SK
        WHERE COMSN_INCM.LAST_UPDT_RUN_CYC_EXCTN_SK >= {IDSRunCycle}
        """
    )
    .load()
)

df_db2_BILL_ENTY = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
        SELECT 
        ENTY.BILL_ENTY_SK,
        ENTY.GRP_SK,
        ENTY.SUBGRP_SK,
        ENTY.SUB_SK,
        ENTY.BILL_ENTY_LVL_CD_SK
        FROM {IDSOwner}.BILL_ENTY ENTY
        """
    )
    .load()
)

df_db2_AGNT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
        SELECT 
        AGNT.AGNT_SK,
        AGNT.AGNT_ID
        FROM {IDSOwner}.AGNT AGNT
        """
    )
    .load()
)

df_db2_GRP = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
        SELECT 
        ENTY.BILL_ENTY_SK,
        GRP.GRP_ID,
        GRP.DP_IN
        FROM {IDSOwner}.BILL_ENTY ENTY,
             {IDSOwner}.GRP GRP
        WHERE GRP.GRP_SK = ENTY.GRP_SK
        """
    )
    .load()
)

df_db2_COMSN_AGMNT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
        SELECT 
        AGMNT.COMSN_AGMNT_SK,
        AGMNT.COMSN_ARGMT_SK,
        AGMNT.COMSN_SCHD_SK,
        AGMNT.COMSN_ARGMT_ID
        FROM {IDSOwner}.COMSN_AGMNT AGMNT
        """
    )
    .load()
)

df_db2_SCHD = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
        SELECT
        AGMNT.COMSN_AGMNT_SK,
        SCHD.COMSN_SCHD_ID
        FROM {IDSOwner}.COMSN_AGMNT AGMNT,
             {IDSOwner}.COMSN_SCHD SCHD
        WHERE AGMNT.COMSN_SCHD_SK = SCHD.COMSN_SCHD_SK
        """
    )
    .load()
)

df_db2_SUB = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
        SELECT
        ENTY.BILL_ENTY_SK,
        SUB.SUB_ID
        FROM {IDSOwner}.BILL_ENTY ENTY,
             {IDSOwner}.SUB SUB
        WHERE SUB.SUB_SK = ENTY.SUB_SK
        """
    )
    .load()
)

df_db2_EXPRNC = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
        SELECT
        EXPRNC_CAT.EXPRNC_CAT_SK,
        EXPRNC_CAT.EXPRNC_CAT_CD
        FROM {IDSOwner}.EXPRNC_CAT EXPRNC_CAT
        """
    )
    .load()
)

df_db2_SUBGRP = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
        SELECT
        ENTY.BILL_ENTY_SK,
        SUBGRP.SUBGRP_ID
        FROM {IDSOwner}.BILL_ENTY ENTY,
             {IDSOwner}.SUBGRP SUBGRP
        WHERE SUBGRP.SUBGRP_SK = ENTY.SUBGRP_SK
        """
    )
    .load()
)

df_db2_FNCL_LOB = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
        SELECT
        FNCL_LOB.FNCL_LOB_SK,
        FNCL_LOB.FNCL_LOB_CD
        FROM {IDSOwner}.FNCL_LOB FNCL_LOB
        """
    )
    .load()
)

df_db2_PROD_SH_NM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
        SELECT
        PROD.PROD_SH_NM_SK,
        PROD.PROD_SH_NM
        FROM {IDSOwner}.PROD_SH_NM PROD
        """
    )
    .load()
)

df_lkp_Codes1_pre = (
    df_db2_COMSN_INCM.alias("lnk_ComsnIncm_In")
    .join(
        df_db2_COMSN_AGMNT.alias("lnk_ComsnAgmnt"),
        F.col("lnk_ComsnIncm_In.COMSN_AGMNT_SK")==F.col("lnk_ComsnAgmnt.COMSN_AGMNT_SK"),
        "left"
    )
    .join(
        df_db2_BILL_ENTY.alias("lnk_BillEnty"),
        F.col("lnk_ComsnIncm_In.BILL_ENTY_SK")==F.col("lnk_BillEnty.BILL_ENTY_SK"),
        "left"
    )
    .join(
        df_db2_AGNT.alias("lnk_Agnt"),
        F.col("lnk_ComsnIncm_In.AGNY_AGNT_SK")==F.col("lnk_Agnt.AGNT_SK"),
        "left"
    )
    .join(
        df_db2_GRP.alias("lnk_Grp"),
        F.col("lnk_ComsnIncm_In.BILL_ENTY_SK")==F.col("lnk_Grp.BILL_ENTY_SK"),
        "left"
    )
    .join(
        df_lnk_comsnbsssrccd.alias("lnk_comsnbsssrccd"),
        F.col("lnk_ComsnIncm_In.COMSN_BSS_SRC_CD_SK")==F.col("lnk_comsnbsssrccd.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_lnk_agntsttuscd.alias("lnk_agntsttuscd"),
        F.col("lnk_ComsnIncm_In.AGNT_STTUS_CD_SK")==F.col("lnk_agntsttuscd.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_lnk_comsncovcatcd.alias("lnk_comsncovcatcd"),
        F.col("lnk_ComsnIncm_In.COMSN_COV_CAT_CD_SK")==F.col("lnk_comsncovcatcd.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_lnk_Comtypcd.alias("lnk_Comtypcd"),
        F.col("lnk_ComsnIncm_In.COMSN_TYP_CD_SK")==F.col("lnk_Comtypcd.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_lnk_comsnmtdtypcd.alias("lnk_comsnmtdtypcd"),
        F.col("lnk_ComsnIncm_In.COMSN_MTHDLGY_TYP_CD_SK")==F.col("lnk_comsnmtdtypcd.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_lnk_Cttypcd.alias("lnk_Cttypcd"),
        F.col("lnk_ComsnIncm_In.CT_TYP_CD_SK")==F.col("lnk_Cttypcd.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_lnk_funcatcd.alias("lnk_funcatcd"),
        F.col("lnk_ComsnIncm_In.FUND_CAT_CD_SK")==F.col("lnk_funcatcd.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_lnk_agnttierlvlcd.alias("lnk_agnttierlvlcd"),
        F.col("lnk_ComsnIncm_In.AGNT_TIER_LVL_CD_SK")==F.col("lnk_agnttierlvlcd.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_db2_SUBGRP.alias("lnk_SubGrp"),
        F.col("lnk_ComsnIncm_In.BILL_ENTY_SK")==F.col("lnk_SubGrp.BILL_ENTY_SK"),
        "left"
    )
    .join(
        df_db2_SUB.alias("lnk_Sub"),
        F.col("lnk_ComsnIncm_In.BILL_ENTY_SK")==F.col("lnk_Sub.BILL_ENTY_SK"),
        "left"
    )
    .join(
        df_db2_SCHD.alias("lnk_Schd"),
        F.col("lnk_ComsnIncm_In.COMSN_AGMNT_SK")==F.col("lnk_Schd.COMSN_AGMNT_SK"),
        "left"
    )
    .join(
        df_db2_EXPRNC.alias("lnk_Exprnc"),
        F.col("lnk_ComsnIncm_In.EXPRNC_CAT_SK")==F.col("lnk_Exprnc.EXPRNC_CAT_SK"),
        "left"
    )
    .join(
        df_db2_FNCL_LOB.alias("lnk_FnclLob"),
        F.col("lnk_ComsnIncm_In.FNCL_LOB_SK")==F.col("lnk_FnclLob.FNCL_LOB_SK"),
        "left"
    )
    .join(
        df_db2_PROD_SH_NM.alias("lnk_ProdShNm"),
        F.col("lnk_ComsnIncm_In.PROD_SH_NM_SK")==F.col("lnk_ProdShNm.PROD_SH_NM_SK"),
        "left"
    )
)

df_lnk_ComsnIncm = df_lkp_Codes1_pre.select(
    F.col("lnk_ComsnIncm_In.COMSN_INCM_SK").alias("COMSN_INCM_SK"),
    F.col("lnk_ComsnIncm_In.PD_AGNT_ID").alias("PD_AGNT_ID"),
    F.col("lnk_ComsnIncm_In.ERN_AGNT_ID").alias("ERN_AGNT_ID"),
    F.col("lnk_ComsnIncm_In.COMSN_PD_DT_SK").alias("COMSN_PD_DT_SK"),
    F.col("lnk_ComsnIncm_In.BILL_ENTY_UNIQ_KEY").alias("BILL_ENTY_UNIQ_KEY"),
    F.col("lnk_ComsnIncm_In.BILL_INCM_RCPT_BILL_DUE_DT_SK").alias("BILL_INCM_RCPT_BILL_DUE_DT_SK"),
    F.col("lnk_ComsnIncm_In.COMSN_INCM_SEQ_NO").alias("COMSN_INCM_SEQ_NO"),
    F.col("lnk_ComsnIncm_In.CLS_PLN_ID").alias("CLS_PLN_ID"),
    F.col("lnk_ComsnIncm_In.PROD_ID").alias("PROD_ID"),
    F.col("lnk_ComsnIncm_In.COMSN_DTL_INCM_DISP_CD").alias("COMSN_DTL_INCM_DISP_CD"),
    F.col("lnk_ComsnIncm_In.COMSN_TYP_CD").alias("COMSN_TYP_CD"),
    F.col("lnk_ComsnIncm_In.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_ComsnIncm_In.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_ComsnIncm_In.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_ComsnIncm_In.BILL_INCM_RCPT_SK").alias("BILL_INCM_RCPT_SK"),
    F.col("lnk_ComsnIncm_In.BILL_ENTY_SK").alias("BILL_ENTY_SK"),
    F.col("lnk_ComsnIncm_In.BILL_SUM_SK").alias("BILL_SUM_SK"),
    F.col("lnk_ComsnIncm_In.CLS_PLN_SK").alias("CLS_PLN_SK"),
    F.col("lnk_ComsnIncm_In.COMSN_AGMNT_SK").alias("COMSN_AGMNT_SK"),
    F.col("lnk_ComsnIncm_In.AGNY_AGNT_SK").alias("AGNY_AGNT_SK"),
    F.col("lnk_ComsnIncm_In.ERN_AGNT_SK").alias("ERN_AGNT_SK"),
    F.col("lnk_ComsnIncm_In.EXPRNC_CAT_SK").alias("EXPRNC_CAT_SK"),
    F.col("lnk_ComsnIncm_In.FNCL_LOB_SK").alias("FNCL_LOB_SK"),
    F.col("lnk_ComsnIncm_In.PROD_SK").alias("PROD_SK"),
    F.col("lnk_ComsnIncm_In.PD_AGNT_SK").alias("PD_AGNT_SK"),
    F.col("lnk_ComsnIncm_In.PROD_SH_NM_SK").alias("PROD_SH_NM_SK"),
    F.col("lnk_ComsnIncm_In.AGNT_STTUS_CD_SK").alias("AGNT_STTUS_CD_SK"),
    F.col("lnk_ComsnIncm_In.AGNT_TIER_LVL_CD_SK").alias("AGNT_TIER_LVL_CD_SK"),
    F.col("lnk_ComsnIncm_In.COMSN_BSS_SRC_CD_SK").alias("COMSN_BSS_SRC_CD_SK"),
    F.col("lnk_ComsnIncm_In.COMSN_COV_CAT_CD_SK").alias("COMSN_COV_CAT_CD_SK"),
    F.col("lnk_ComsnIncm_In.COMSN_DTL_INCM_DISP_CD_SK").alias("COMSN_DTL_INCM_DISP_CD_SK"),
    F.col("lnk_ComsnIncm_In.COMSN_MTHDLGY_TYP_CD_SK").alias("COMSN_MTHDLGY_TYP_CD_SK"),
    F.col("lnk_ComsnIncm_In.COMSN_TYP_CD_SK").alias("COMSN_TYP_CD_SK"),
    F.col("lnk_ComsnIncm_In.CT_TYP_CD_SK").alias("CT_TYP_CD_SK"),
    F.col("lnk_ComsnIncm_In.FUND_CAT_CD_SK").alias("FUND_CAT_CD_SK"),
    F.col("lnk_ComsnIncm_In.COMSN_PD_YR_MO").alias("COMSN_PD_YR_MO"),
    F.col("lnk_ComsnIncm_In.CONT_COV_STRT_DT_SK").alias("CONT_COV_STRT_DT_SK"),
    F.col("lnk_ComsnIncm_In.COV_END_DT_SK").alias("COV_END_DT_SK"),
    F.col("lnk_ComsnIncm_In.COV_PLN_YR").alias("COV_PLN_YR"),
    F.col("lnk_ComsnIncm_In.DP_CANC_YR_MO").alias("DP_CANC_YR_MO"),
    F.col("lnk_ComsnIncm_In.PLN_YR_COV_EFF_DT_SK").alias("PLN_YR_COV_EFF_DT_SK"),
    F.col("lnk_ComsnIncm_In.PRM_PD_TO_DT_SK").alias("PRM_PD_TO_DT_SK"),
    F.col("lnk_ComsnIncm_In.COMSN_BSS_AMT").alias("COMSN_BSS_AMT"),
    F.col("lnk_ComsnIncm_In.COMSN_RATE_AMT").alias("COMSN_RATE_AMT"),
    F.col("lnk_ComsnIncm_In.PD_COMSN_AMT").alias("PD_COMSN_AMT"),
    F.col("lnk_ComsnIncm_In.BILL_COMSN_PRM_PCT").alias("BILL_COMSN_PRM_PCT"),
    F.col("lnk_ComsnIncm_In.COMSN_AGMNT_SCHD_FCTR").alias("COMSN_AGMNT_SCHD_FCTR"),
    F.col("lnk_ComsnIncm_In.COMSN_RATE_PCT").alias("COMSN_RATE_PCT"),
    F.col("lnk_ComsnIncm_In.CNTR_CT").alias("CNTR_CT"),
    F.col("lnk_ComsnIncm_In.MBR_CT").alias("MBR_CT"),
    F.col("lnk_ComsnIncm_In.SRC_TRANS_TYP_TX").alias("SRC_TRANS_TYP_TX"),
    F.col("lnk_Comtypcd.TRGT_CD").alias("TRGT_CD_ComTypCd"),
    F.col("lnk_Comtypcd.TRGT_CD_NM").alias("TRGT_CD_NM_ComTypCd"),
    F.col("lnk_agntsttuscd.TRGT_CD").alias("TRGT_CD_AgntSttusCd"),
    F.col("lnk_agntsttuscd.TRGT_CD_NM").alias("TRGT_CD_NM_AgntSttusCd"),
    F.col("lnk_agnttierlvlcd.TRGT_CD").alias("TRGT_CD_AgntTierLvlCd"),
    F.col("lnk_agnttierlvlcd.TRGT_CD_NM").alias("TRGT_CD_NM_AgntTierLvlCd"),
    F.col("lnk_comsnbsssrccd.TRGT_CD").alias("TRGT_CD_ComsnBssSrcCd"),
    F.col("lnk_comsnbsssrccd.TRGT_CD_NM").alias("TRGT_CD_NM_ComsnBssSrcCd"),
    F.col("lnk_comsncovcatcd.TRGT_CD").alias("TRGT_CD_ComsnCovCatCd"),
    F.col("lnk_comsncovcatcd.TRGT_CD_NM").alias("TRGT_CD_NM_ComsnCovCatCd"),
    F.col("lnk_comsnmtdtypcd.TRGT_CD").alias("TRGT_CD_ComsnMtdTypCd"),
    F.col("lnk_comsnmtdtypcd.TRGT_CD_NM").alias("TRGT_CD_NM_ComsnMtdTypCd"),
    F.col("lnk_Cttypcd.TRGT_CD").alias("TRGT_CD_CtTypCd"),
    F.col("lnk_Cttypcd.TRGT_CD_NM").alias("TRGT_CD_NM_CtTypCd"),
    F.col("lnk_funcatcd.TRGT_CD").alias("TRGT_CD_FundCatCd"),
    F.col("lnk_funcatcd.TRGT_CD_NM").alias("TRGT_CD_NM_FundCatCd"),
    F.col("lnk_ComsnAgmnt.COMSN_ARGMT_ID").alias("COMSN_ARGMT_ID_Agmnt"),
    F.col("lnk_ComsnAgmnt.COMSN_AGMNT_SK").alias("COMSN_ARGMT_SK_Agmnt"),
    F.col("lnk_ComsnAgmnt.COMSN_SCHD_SK").alias("COMSN_SCHD_SK_Agmnt"),
    F.col("lnk_BillEnty.GRP_SK").alias("GRP_SK_BillEnty"),
    F.col("lnk_BillEnty.SUBGRP_SK").alias("SUBGRP_SK_BillEnty"),
    F.col("lnk_BillEnty.SUB_SK").alias("SUB_SK_BillEnty"),
    F.col("lnk_BillEnty.BILL_ENTY_LVL_CD_SK").alias("BILL_ENTY_LVL_CD_SK_BillEnty"),
    F.col("lnk_Agnt.AGNT_ID").alias("AGNT_ID_Agnt"),
    F.col("lnk_Grp.GRP_ID").alias("GRP_ID_grp"),
    F.col("lnk_Grp.DP_IN").alias("DP_IN_grp"),
    F.col("lnk_SubGrp.SUBGRP_ID").alias("SUBGRP_ID_subgrp"),
    F.col("lnk_Sub.SUB_ID").alias("SUB_ID_sub"),
    F.col("lnk_Schd.COMSN_SCHD_ID").alias("COMSN_SCHD_ID_schd"),
    F.col("lnk_Exprnc.EXPRNC_CAT_CD").alias("EXPRNC_CAT_CD_exprnc"),
    F.col("lnk_FnclLob.FNCL_LOB_CD").alias("FNCL_LOB_CD"),
    F.col("lnk_ProdShNm.PROD_SH_NM").alias("PROD_SH_NM"),
    F.col("lnk_ComsnIncm_In.ACA_BUS_SBSDY_TYP_ID").alias("ACA_BUS_SBSDY_TYP_ID"),
    F.col("lnk_ComsnIncm_In.ACA_PRM_SBSDY_AMT").alias("ACA_PRM_SBSDY_AMT"),
    F.col("lnk_ComsnIncm_In.AGNY_HIER_LVL_ID").alias("AGNY_HIER_LVL_ID"),
    F.col("lnk_ComsnIncm_In.CMS_CUST_POL_ID").alias("CMS_CUST_POL_ID"),
    F.col("lnk_ComsnIncm_In.CMS_PLN_BNF_PCKG_ID").alias("CMS_PLN_BNF_PCKG_ID"),
    F.col("lnk_ComsnIncm_In.COMSN_BLUE_KC_BRKR_PAYMT_IN").alias("COMSN_BLUE_KC_BRKR_PAYMT_IN"),
    F.col("lnk_ComsnIncm_In.COMSN_PAYOUT_TYP_ID").alias("COMSN_PAYOUT_TYP_ID"),
    F.col("lnk_ComsnIncm_In.COMSN_RETROACTV_ADJ_IN").alias("COMSN_RETROACTV_ADJ_IN"),
    F.col("lnk_ComsnIncm_In.CUST_BUS_TYP_ID").alias("CUST_BUS_TYP_ID"),
    F.col("lnk_ComsnIncm_In.CUST_BUS_SUBTYP_ID").alias("CUST_BUS_SUBTYP_ID"),
    F.col("lnk_ComsnIncm_In.ICM_COMSN_PAYE_ID").alias("ICM_COMSN_PAYE_ID"),
    F.col("lnk_ComsnIncm_In.MCARE_ADVNTG_ENR_CYC_YR_ID").alias("MCARE_ADVNTG_ENR_CYC_YR_ID"),
    F.col("lnk_ComsnIncm_In.MCARE_BNFCRY_ID").alias("MCARE_BNFCRY_ID"),
    F.col("lnk_ComsnIncm_In.MCARE_ENR_TYP_ID").alias("MCARE_ENR_TYP_ID"),
    F.col("lnk_ComsnIncm_In.TRANS_CNTR_CT").alias("TRANS_CNTR_CT"),
    F.col("lnk_ComsnIncm_In.TRNSMSN_SRC_SYS_CD").alias("TRNSMSN_SRC_SYS_CD")
)

df_xfm_BusinessLogic1 = df_lnk_ComsnIncm.select(
    F.col("COMSN_INCM_SK"),
    F.col("PD_AGNT_ID"),
    F.col("ERN_AGNT_ID"),
    F.col("COMSN_PD_DT_SK"),
    F.col("BILL_ENTY_UNIQ_KEY"),
    F.col("BILL_INCM_RCPT_BILL_DUE_DT_SK"),
    F.col("COMSN_INCM_SEQ_NO"),
    F.col("CLS_PLN_ID"),
    F.col("PROD_ID"),
    F.col("COMSN_DTL_INCM_DISP_CD"),
    F.when(
        F.col("TRGT_CD_ComTypCd").isNull() | (trim(F.col("TRGT_CD_ComTypCd")) == ""),
        F.lit("UNK")
    ).otherwise(F.col("TRGT_CD_ComTypCd")).alias("COMSN_TYP_CD"),
    F.col("SRC_SYS_CD"),
    F.lit(CurrRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(CurrRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("AGNY_AGNT_SK"),
    F.col("BILL_ENTY_SK"),
    F.col("BILL_SUM_SK"),
    F.col("CLS_PLN_SK"),
    F.col("COMSN_AGMNT_SK"),
    F.when(
        F.col("COMSN_ARGMT_SK_Agmnt").isNull(),
        F.lit("1")
    ).otherwise(F.col("COMSN_ARGMT_SK_Agmnt")).alias("COMSN_ARGMT_SK"),
    F.when(
        F.col("COMSN_SCHD_SK_Agmnt").isNull(),
        F.lit("1")
    ).otherwise(F.col("COMSN_SCHD_SK_Agmnt")).alias("COMSN_SCHD_SK"),
    F.col("ERN_AGNT_SK"),
    F.col("EXPRNC_CAT_SK"),
    F.col("FNCL_LOB_SK"),
    F.when(F.col("GRP_SK_BillEnty").isNull(), F.lit("1")).otherwise(F.col("GRP_SK_BillEnty")).alias("GRP_SK"),
    F.col("PD_AGNT_SK"),
    F.col("PROD_SK"),
    F.col("PROD_SH_NM_SK"),
    F.col("BILL_INCM_RCPT_SK").alias("RCVD_INCM_SK"),
    F.when(F.col("SUBGRP_SK_BillEnty").isNull(), F.lit("1")).otherwise(F.col("SUBGRP_SK_BillEnty")).alias("SUBGRP_SK"),
    F.when(F.col("SUB_SK_BillEnty").isNull(), F.lit("1")).otherwise(F.col("SUB_SK_BillEnty")).alias("SUB_SK"),
    F.when(
        F.col("TRGT_CD_AgntSttusCd").isNull() | (trim(F.col("TRGT_CD_AgntSttusCd")) == ""),
        F.lit("UNK")
    ).otherwise(F.col("TRGT_CD_AgntSttusCd")).alias("AGNT_STTUS_CD"),
    F.when(
        F.col("TRGT_CD_NM_AgntSttusCd").isNull() | (trim(F.col("TRGT_CD_NM_AgntSttusCd")) == ""),
        F.lit("UNKNOWN")
    ).otherwise(F.col("TRGT_CD_NM_AgntSttusCd")).alias("AGNT_STTUS_NM"),
    F.when(
        F.col("TRGT_CD_AgntTierLvlCd").isNull() | (trim(F.col("TRGT_CD_AgntTierLvlCd")) == ""),
        F.lit("UNK")
    ).otherwise(F.col("TRGT_CD_AgntTierLvlCd")).alias("AGNT_TIER_LVL_CD"),
    F.when(
        F.col("TRGT_CD_NM_AgntTierLvlCd").isNull() | (trim(F.col("TRGT_CD_NM_AgntTierLvlCd")) == ""),
        F.lit("UNKNOWN")
    ).otherwise(F.col("TRGT_CD_NM_AgntTierLvlCd")).alias("AGNT_TIER_LVL_NM"),
    F.lit("UNK").alias("BILL_ENTY_LVL_CD"),
    F.lit("UNKNOWN").alias("BILL_ENTY_LVL_NM"),
    F.when(
        F.col("TRGT_CD_ComsnBssSrcCd").isNull() | (trim(F.col("TRGT_CD_ComsnBssSrcCd")) == ""),
        F.lit("UNK")
    ).otherwise(F.col("TRGT_CD_ComsnBssSrcCd")).alias("COMSN_BSS_SRC_CD"),
    F.when(
        F.col("TRGT_CD_NM_ComsnBssSrcCd").isNull() | (trim(F.col("TRGT_CD_NM_ComsnBssSrcCd")) == ""),
        F.lit("UNKNOWN")
    ).otherwise(F.col("TRGT_CD_NM_ComsnBssSrcCd")).alias("COMSN_BSS_SRC_NM"),
    F.when(
        F.col("TRGT_CD_ComsnCovCatCd").isNull() | (trim(F.col("TRGT_CD_ComsnCovCatCd")) == ""),
        F.lit("UNK")
    ).otherwise(F.col("TRGT_CD_ComsnCovCatCd")).alias("COMSN_COV_CAT_CD"),
    F.when(
        F.col("TRGT_CD_NM_ComsnCovCatCd").isNull() | (trim(F.col("TRGT_CD_NM_ComsnCovCatCd")) == ""),
        F.lit("UNKNOWN")
    ).otherwise(F.col("TRGT_CD_NM_ComsnCovCatCd")).alias("COMSN_COV_CAT_NM"),
    F.when(
        F.col("TRGT_CD_ComsnMtdTypCd").isNull() | (trim(F.col("TRGT_CD_ComsnMtdTypCd")) == ""),
        F.lit("UNK")
    ).otherwise(F.col("TRGT_CD_ComsnMtdTypCd")).alias("COMSN_MTHDLGY_TYP_CD"),
    F.when(
        F.col("TRGT_CD_NM_ComsnMtdTypCd").isNull() | (trim(F.col("TRGT_CD_NM_ComsnMtdTypCd")) == ""),
        F.lit("UNKNOWN")
    ).otherwise(F.col("TRGT_CD_NM_ComsnMtdTypCd")).alias("COMSN_MTHDLGY_TYP_NM"),
    F.when(
        F.col("TRGT_CD_NM_ComTypCd").isNull() | (trim(F.col("TRGT_CD_NM_ComTypCd")) == ""),
        F.lit("UNKNOWN")
    ).otherwise(F.col("TRGT_CD_NM_ComTypCd")).alias("COMSN_TYP_NM"),
    F.when(
        F.col("TRGT_CD_CtTypCd").isNull() | (trim(F.col("TRGT_CD_CtTypCd")) == ""),
        F.lit("UNK")
    ).otherwise(F.col("TRGT_CD_CtTypCd")).alias("CT_TYP_CD"),
    F.when(
        F.col("TRGT_CD_NM_CtTypCd").isNull() | (trim(F.col("TRGT_CD_NM_CtTypCd")) == ""),
        F.lit("UNKNOWN")
    ).otherwise(F.col("TRGT_CD_NM_CtTypCd")).alias("CT_TYP_NM"),
    F.when(
        F.col("EXPRNC_CAT_CD_exprnc").isNull() | (trim(F.col("EXPRNC_CAT_CD_exprnc")) == ""),
        F.lit("UNK")
    ).otherwise(F.col("EXPRNC_CAT_CD_exprnc")).alias("EXPRNC_CAT_CD"),
    F.when(
        F.col("TRGT_CD_FundCatCd").isNull() | (trim(F.col("TRGT_CD_FundCatCd")) == ""),
        F.lit("UNK")
    ).otherwise(F.col("TRGT_CD_FundCatCd")).alias("FUND_CAT_CD"),
    F.when(
        F.col("TRGT_CD_NM_FundCatCd").isNull() | (trim(F.col("TRGT_CD_NM_FundCatCd")) == ""),
        F.lit("UNKNOWN")
    ).otherwise(F.col("TRGT_CD_NM_FundCatCd")).alias("FUND_CAT_NM"),
    F.when(
        F.col("DP_IN_grp").isNull() | (trim(F.col("DP_IN_grp")) == ""),
        F.lit("NA")
    ).otherwise(F.col("DP_IN_grp")).alias("GRP_DP_IN"),
    F.col("COMSN_PD_YR_MO"),
    F.col("CONT_COV_STRT_DT_SK"),
    F.col("COV_END_DT_SK"),
    F.col("COV_PLN_YR"),
    F.col("DP_CANC_YR_MO"),
    F.col("PLN_YR_COV_EFF_DT_SK"),
    F.col("PRM_PD_TO_DT_SK"),
    F.col("COMSN_BSS_AMT"),
    F.col("COMSN_RATE_AMT"),
    F.col("PD_COMSN_AMT"),
    F.col("BILL_COMSN_PRM_PCT"),
    F.col("COMSN_AGMNT_SCHD_FCTR"),
    F.col("COMSN_RATE_PCT"),
    F.col("CNTR_CT"),
    F.col("MBR_CT"),
    F.when(
        F.col("AGNT_ID_Agnt").isNull() | (trim(F.col("AGNT_ID_Agnt")) == ""),
        F.lit("1")
    ).otherwise(F.col("AGNT_ID_Agnt")).alias("AGNY_AGNT_ID"),
    F.when(
        F.col("COMSN_ARGMT_ID_Agmnt").isNull() | (trim(F.col("COMSN_ARGMT_ID_Agmnt")) == ""),
        F.lit("1")
    ).otherwise(F.col("COMSN_ARGMT_ID_Agmnt")).alias("COMSN_ARGMT_ID"),
    F.when(
        F.col("COMSN_SCHD_ID_schd").isNull() | (trim(F.col("COMSN_SCHD_ID_schd")) == ""),
        F.lit("1")
    ).otherwise(F.col("COMSN_SCHD_ID_schd")).alias("COMSN_SCHD_ID"),
    F.when(
        F.col("FNCL_LOB_CD").isNull() | (trim(F.col("FNCL_LOB_CD")) == ""),
        F.lit("UNK")
    ).otherwise(F.col("FNCL_LOB_CD")).alias("FNCL_LOB_CD"),
    F.when(
        F.col("GRP_ID_grp").isNull() | (trim(F.col("GRP_ID_grp")) == ""),
        F.lit("1")
    ).otherwise(F.col("GRP_ID_grp")).alias("GRP_ID"),
    F.when(
        F.col("PROD_SH_NM").isNull() | (trim(F.col("PROD_SH_NM")) == ""),
        F.lit("UNK")
    ).otherwise(F.col("PROD_SH_NM")).alias("PROD_SH_NM"),
    F.when(
        F.col("SUBGRP_ID_subgrp").isNull() | (trim(F.col("SUBGRP_ID_subgrp")) == ""),
        F.lit("NA")
    ).otherwise(F.col("SUBGRP_ID_subgrp")).alias("SUBGRP_ID"),
    F.when(
        F.col("SUB_ID_sub").isNull() | (trim(F.col("SUB_ID_sub")) == ""),
        F.lit("1")
    ).otherwise(F.col("SUB_ID_sub")).alias("SUB_ID"),
    F.col("SRC_TRANS_TYP_TX"),
    F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("AGNT_STTUS_CD_SK"),
    F.col("AGNT_TIER_LVL_CD_SK"),
    F.when(F.col("BILL_ENTY_LVL_CD_SK_BillEnty").isNull(), F.lit("1")).otherwise(F.col("BILL_ENTY_LVL_CD_SK_BillEnty")).alias("BILL_ENTY_LVL_CD_SK"),
    F.col("COMSN_BSS_SRC_CD_SK"),
    F.col("COMSN_COV_CAT_CD_SK"),
    F.col("COMSN_DTL_INCM_DISP_CD_SK"),
    F.col("COMSN_MTHDLGY_TYP_CD_SK"),
    F.col("COMSN_TYP_CD_SK"),
    F.col("CT_TYP_CD_SK"),
    F.col("FUND_CAT_CD_SK"),
    F.col("ACA_BUS_SBSDY_TYP_ID"),
    F.col("ACA_PRM_SBSDY_AMT"),
    F.col("AGNY_HIER_LVL_ID"),
    F.col("CMS_CUST_POL_ID"),
    F.col("CMS_PLN_BNF_PCKG_ID"),
    F.col("COMSN_BLUE_KC_BRKR_PAYMT_IN"),
    F.col("COMSN_PAYOUT_TYP_ID"),
    F.col("COMSN_RETROACTV_ADJ_IN"),
    F.col("CUST_BUS_TYP_ID"),
    F.col("CUST_BUS_SUBTYP_ID"),
    F.col("ICM_COMSN_PAYE_ID"),
    F.col("MCARE_ADVNTG_ENR_CYC_YR_ID"),
    F.col("MCARE_BNFCRY_ID"),
    F.col("MCARE_ENR_TYP_ID"),
    F.col("TRANS_CNTR_CT"),
    F.col("TRNSMSN_SRC_SYS_CD")
)

df_lkp_codes2_pre = (
    df_xfm_BusinessLogic1.alias("lnk_ComsnIncm_F")
    .join(
        df_lnk_billentylvlcd.alias("lnk_billentylvlcd"),
        F.col("lnk_ComsnIncm_F.BILL_ENTY_LVL_CD_SK")==F.col("lnk_billentylvlcd.CD_MPPNG_SK"),
        "left"
    )
)

df_lnk_ComsnIncm_Extr = df_lkp_codes2_pre.select(
    F.col("lnk_ComsnIncm_F.COMSN_INCM_SK"),
    F.col("lnk_ComsnIncm_F.PD_AGNT_ID"),
    F.col("lnk_ComsnIncm_F.ERN_AGNT_ID"),
    F.col("lnk_ComsnIncm_F.COMSN_PD_DT_SK"),
    F.col("lnk_ComsnIncm_F.BILL_ENTY_UNIQ_KEY"),
    F.col("lnk_ComsnIncm_F.BILL_INCM_RCPT_BILL_DUE_DT_SK"),
    F.col("lnk_ComsnIncm_F.COMSN_INCM_SEQ_NO"),
    F.col("lnk_ComsnIncm_F.CLS_PLN_ID"),
    F.col("lnk_ComsnIncm_F.PROD_ID"),
    F.col("lnk_ComsnIncm_F.COMSN_DTL_INCM_DISP_CD"),
    F.col("lnk_ComsnIncm_F.COMSN_TYP_CD"),
    F.col("lnk_ComsnIncm_F.CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("lnk_ComsnIncm_F.LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("lnk_ComsnIncm_F.SRC_SYS_CD"),
    F.col("lnk_ComsnIncm_F.AGNY_AGNT_SK"),
    F.col("lnk_ComsnIncm_F.BILL_ENTY_SK"),
    F.col("lnk_ComsnIncm_F.BILL_SUM_SK"),
    F.col("lnk_ComsnIncm_F.CLS_PLN_SK"),
    F.col("lnk_ComsnIncm_F.COMSN_AGMNT_SK"),
    F.col("lnk_ComsnIncm_F.COMSN_ARGMT_SK"),
    F.col("lnk_ComsnIncm_F.COMSN_SCHD_SK"),
    F.col("lnk_ComsnIncm_F.ERN_AGNT_SK"),
    F.col("lnk_ComsnIncm_F.EXPRNC_CAT_SK"),
    F.col("lnk_ComsnIncm_F.FNCL_LOB_SK"),
    F.col("lnk_ComsnIncm_F.GRP_SK"),
    F.col("lnk_ComsnIncm_F.PD_AGNT_SK"),
    F.col("lnk_ComsnIncm_F.PROD_SK"),
    F.col("lnk_ComsnIncm_F.PROD_SH_NM_SK"),
    F.col("lnk_ComsnIncm_F.RCVD_INCM_SK"),
    F.col("lnk_ComsnIncm_F.SUBGRP_SK"),
    F.col("lnk_ComsnIncm_F.SUB_SK"),
    F.col("lnk_ComsnIncm_F.AGNT_STTUS_CD"),
    F.col("lnk_ComsnIncm_F.AGNT_STTUS_NM"),
    F.col("lnk_ComsnIncm_F.AGNT_TIER_LVL_CD"),
    F.col("lnk_ComsnIncm_F.AGNT_TIER_LVL_NM"),
    F.col("lnk_ComsnIncm_F.BILL_ENTY_LVL_CD"),
    F.col("lnk_ComsnIncm_F.BILL_ENTY_LVL_NM"),
    F.col("lnk_ComsnIncm_F.COMSN_BSS_SRC_CD"),
    F.col("lnk_ComsnIncm_F.COMSN_BSS_SRC_NM"),
    F.col("lnk_ComsnIncm_F.COMSN_COV_CAT_CD"),
    F.col("lnk_ComsnIncm_F.COMSN_COV_CAT_NM"),
    F.col("lnk_ComsnIncm_F.COMSN_MTHDLGY_TYP_CD"),
    F.col("lnk_ComsnIncm_F.COMSN_MTHDLGY_TYP_NM"),
    F.col("lnk_ComsnIncm_F.COMSN_TYP_NM"),
    F.col("lnk_ComsnIncm_F.CT_TYP_CD"),
    F.col("lnk_ComsnIncm_F.CT_TYP_NM"),
    F.col("lnk_ComsnIncm_F.EXPRNC_CAT_CD"),
    F.col("lnk_ComsnIncm_F.FUND_CAT_CD"),
    F.col("lnk_ComsnIncm_F.FUND_CAT_NM"),
    F.col("lnk_ComsnIncm_F.GRP_DP_IN"),
    F.col("lnk_ComsnIncm_F.COMSN_PD_YR_MO"),
    F.col("lnk_ComsnIncm_F.CONT_COV_STRT_DT_SK"),
    F.col("lnk_ComsnIncm_F.COV_END_DT_SK"),
    F.col("lnk_ComsnIncm_F.COV_PLN_YR"),
    F.col("lnk_ComsnIncm_F.DP_CANC_YR_MO"),
    F.col("lnk_ComsnIncm_F.PLN_YR_COV_EFF_DT_SK"),
    F.col("lnk_ComsnIncm_F.PRM_PD_TO_DT_SK"),
    F.col("lnk_ComsnIncm_F.COMSN_BSS_AMT"),
    F.col("lnk_ComsnIncm_F.COMSN_RATE_AMT"),
    F.col("lnk_ComsnIncm_F.PD_COMSN_AMT"),
    F.col("lnk_ComsnIncm_F.BILL_COMSN_PRM_PCT"),
    F.col("lnk_ComsnIncm_F.COMSN_AGMNT_SCHD_FCTR"),
    F.col("lnk_ComsnIncm_F.COMSN_RATE_PCT"),
    F.col("lnk_ComsnIncm_F.CNTR_CT"),
    F.col("lnk_ComsnIncm_F.MBR_CT"),
    F.col("lnk_ComsnIncm_F.AGNY_AGNT_ID"),
    F.col("lnk_ComsnIncm_F.COMSN_ARGMT_ID"),
    F.col("lnk_ComsnIncm_F.COMSN_SCHD_ID"),
    F.col("lnk_ComsnIncm_F.FNCL_LOB_CD"),
    F.col("lnk_ComsnIncm_F.GRP_ID"),
    F.col("lnk_ComsnIncm_F.PROD_SH_NM"),
    F.col("lnk_ComsnIncm_F.SUBGRP_ID"),
    F.col("lnk_ComsnIncm_F.SUB_ID"),
    F.col("lnk_ComsnIncm_F.SRC_TRANS_TYP_TX"),
    F.col("lnk_ComsnIncm_F.CRT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_ComsnIncm_F.LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_ComsnIncm_F.AGNT_STTUS_CD_SK"),
    F.col("lnk_ComsnIncm_F.AGNT_TIER_LVL_CD_SK"),
    F.col("lnk_ComsnIncm_F.BILL_ENTY_LVL_CD_SK"),
    F.col("lnk_ComsnIncm_F.COMSN_BSS_SRC_CD_SK"),
    F.col("lnk_ComsnIncm_F.COMSN_COV_CAT_CD_SK"),
    F.col("lnk_ComsnIncm_F.COMSN_DTL_INCM_DISP_CD_SK"),
    F.col("lnk_ComsnIncm_F.COMSN_MTHDLGY_TYP_CD_SK"),
    F.col("lnk_ComsnIncm_F.COMSN_TYP_CD_SK"),
    F.col("lnk_ComsnIncm_F.CT_TYP_CD_SK"),
    F.col("lnk_ComsnIncm_F.FUND_CAT_CD_SK"),
    F.col("lnk_billentylvlcd.TRGT_CD").alias("TRGT_CD"),
    F.col("lnk_billentylvlcd.TRGT_CD_NM").alias("TRGT_CD_NM"),
    F.col("lnk_billentylvlcd.IND").alias("IND"),
    F.col("lnk_ComsnIncm_F.ACA_BUS_SBSDY_TYP_ID"),
    F.col("lnk_ComsnIncm_F.ACA_PRM_SBSDY_AMT"),
    F.col("lnk_ComsnIncm_F.AGNY_HIER_LVL_ID"),
    F.col("lnk_ComsnIncm_F.CMS_CUST_POL_ID"),
    F.col("lnk_ComsnIncm_F.CMS_PLN_BNF_PCKG_ID"),
    F.col("lnk_ComsnIncm_F.COMSN_BLUE_KC_BRKR_PAYMT_IN"),
    F.col("lnk_ComsnIncm_F.COMSN_PAYOUT_TYP_ID"),
    F.col("lnk_ComsnIncm_F.COMSN_RETROACTV_ADJ_IN"),
    F.col("lnk_ComsnIncm_F.CUST_BUS_TYP_ID"),
    F.col("lnk_ComsnIncm_F.CUST_BUS_SUBTYP_ID"),
    F.col("lnk_ComsnIncm_F.ICM_COMSN_PAYE_ID"),
    F.col("lnk_ComsnIncm_F.MCARE_ADVNTG_ENR_CYC_YR_ID"),
    F.col("lnk_ComsnIncm_F.MCARE_BNFCRY_ID"),
    F.col("lnk_ComsnIncm_F.MCARE_ENR_TYP_ID"),
    F.col("lnk_ComsnIncm_F.TRANS_CNTR_CT"),
    F.col("lnk_ComsnIncm_F.TRNSMSN_SRC_SYS_CD")
)

df_lnk_NA_schema = df_lnk_ComsnIncm_Extr.schema
df_lnk_UNK_schema = df_lnk_ComsnIncm_Extr.schema

# Build single-row DataFrame for "lnk_NA" with columns set to the specified WhereExpression
row_lnk_NA = []
for field in df_lnk_NA_schema.fields:
    colName = field.name
    if colName == "COMSN_INCM_SK":
        row_lnk_NA.append(0 if field.dataType not in [StringType()] else "1")
    elif colName in ["BILL_COMSN_PRM_PCT","COMSN_RATE_PCT","CNTR_CT","MBR_CT"]:
        row_lnk_NA.append(None)
    elif colName == "COV_PLN_YR":
        row_lnk_NA.append("")
    elif colName == "IND":
        row_lnk_NA.append("0")
    else:
        # match the stage's definitions for "lnk_NA"
        # columns that appear as numeric "1" get 1, "0" get 0, strings get those literal
        # check the job JSON
        # We'll apply the exact "WhereExpression" from the outputPins for lnk_NA
        # This is mechanical. For brevity, do a direct match approach:
        if colName == "PD_AGNT_ID": row_lnk_NA.append("NA")
        elif colName == "ERN_AGNT_ID": row_lnk_NA.append("NA")
        elif colName == "COMSN_PD_DT_SK": row_lnk_NA.append("1753-01-01")
        elif colName == "BILL_ENTY_UNIQ_KEY": row_lnk_NA.append(1)
        elif colName == "BILL_INCM_RCPT_BILL_DUE_DT_SK": row_lnk_NA.append("1753-01-01")
        elif colName == "COMSN_INCM_SEQ_NO": row_lnk_NA.append(0)
        elif colName == "CLS_PLN_ID": row_lnk_NA.append("NA")
        elif colName == "PROD_ID": row_lnk_NA.append("NA")
        elif colName == "COMSN_DTL_INCM_DISP_CD": row_lnk_NA.append("NA")
        elif colName == "COMSN_TYP_CD": row_lnk_NA.append("NA")
        elif colName == "SRC_SYS_CD": row_lnk_NA.append("NA")
        elif colName == "CRT_RUN_CYC_EXCTN_DT_SK": row_lnk_NA.append("1753-01-01")
        elif colName == "LAST_UPDT_RUN_CYC_EXCTN_DT_SK": row_lnk_NA.append("1753-01-01")
        elif colName == "AGNY_AGNT_SK": row_lnk_NA.append(1)
        elif colName == "BILL_ENTY_SK": row_lnk_NA.append(1)
        elif colName == "BILL_SUM_SK": row_lnk_NA.append(1)
        elif colName == "CLS_PLN_SK": row_lnk_NA.append(1)
        elif colName == "COMSN_AGMNT_SK": row_lnk_NA.append(1)
        elif colName == "COMSN_ARGMT_SK": row_lnk_NA.append(1)
        elif colName == "COMSN_SCHD_SK": row_lnk_NA.append(1)
        elif colName == "ERN_AGNT_SK": row_lnk_NA.append(1)
        elif colName == "EXPRNC_CAT_SK": row_lnk_NA.append(1)
        elif colName == "FNCL_LOB_SK": row_lnk_NA.append(1)
        elif colName == "GRP_SK": row_lnk_NA.append(1)
        elif colName == "PD_AGNT_SK": row_lnk_NA.append(1)
        elif colName == "PROD_SK": row_lnk_NA.append(1)
        elif colName == "PROD_SH_NM_SK": row_lnk_NA.append(1)
        elif colName == "RCVD_INCM_SK": row_lnk_NA.append(1)
        elif colName == "SUBGRP_SK": row_lnk_NA.append(1)
        elif colName == "SUB_SK": row_lnk_NA.append(1)
        elif colName == "AGNT_STTUS_CD": row_lnk_NA.append("NA")
        elif colName == "AGNT_STTUS_NM": row_lnk_NA.append("NA")
        elif colName == "AGNT_TIER_LVL_CD": row_lnk_NA.append("NA")
        elif colName == "AGNT_TIER_LVL_NM": row_lnk_NA.append("NA")
        elif colName == "BILL_ENTY_LVL_CD": row_lnk_NA.append("NA")
        elif colName == "BILL_ENTY_LVL_NM": row_lnk_NA.append("NA")
        elif colName == "COMSN_BSS_SRC_CD": row_lnk_NA.append("NA")
        elif colName == "COMSN_BSS_SRC_NM": row_lnk_NA.append("NA")
        elif colName == "COMSN_COV_CAT_CD": row_lnk_NA.append("NA")
        elif colName == "COMSN_COV_CAT_NM": row_lnk_NA.append("NA")
        elif colName == "COMSN_MTHDLGY_TYP_CD": row_lnk_NA.append("NA")
        elif colName == "COMSN_MTHDLGY_TYP_NM": row_lnk_NA.append("NA")
        elif colName == "COMSN_TYP_NM": row_lnk_NA.append("NA")
        elif colName == "CT_TYP_CD": row_lnk_NA.append("NA")
        elif colName == "CT_TYP_NM": row_lnk_NA.append("NA")
        elif colName == "EXPRNC_CAT_CD": row_lnk_NA.append("NA")
        elif colName == "FUND_CAT_CD": row_lnk_NA.append("NA")
        elif colName == "FUND_CAT_NM": row_lnk_NA.append("NA")
        elif colName == "GRP_DP_IN": row_lnk_NA.append("N")
        elif colName == "COMSN_PD_YR_MO": row_lnk_NA.append("175301")
        elif colName == "CONT_COV_STRT_DT_SK": row_lnk_NA.append("1753-01-01")
        elif colName == "COV_END_DT_SK": row_lnk_NA.append("1753-01-01")
        elif colName == "DP_CANC_YR_MO": row_lnk_NA.append("175301")
        elif colName == "PLN_YR_COV_EFF_DT_SK": row_lnk_NA.append("1753-01-01")
        elif colName == "PRM_PD_TO_DT_SK": row_lnk_NA.append("1753-01-01")
        elif colName == "COMSN_BSS_AMT": row_lnk_NA.append(0)
        elif colName == "COMSN_RATE_AMT": row_lnk_NA.append(0)
        elif colName == "PD_COMSN_AMT": row_lnk_NA.append(0)
        elif colName == "COMSN_AGMNT_SCHD_FCTR": row_lnk_NA.append(0)
        elif colName == "AGNY_AGNT_ID": row_lnk_NA.append("NA")
        elif colName == "COMSN_ARGMT_ID": row_lnk_NA.append("NA")
        elif colName == "COMSN_SCHD_ID": row_lnk_NA.append("NA")
        elif colName == "FNCL_LOB_CD": row_lnk_NA.append("NA")
        elif colName == "GRP_ID": row_lnk_NA.append("NA")
        elif colName == "PROD_SH_NM": row_lnk_NA.append("NA")
        elif colName == "SUBGRP_ID": row_lnk_NA.append("NA")
        elif colName == "SUB_ID": row_lnk_NA.append("NA")
        elif colName == "SRC_TRANS_TYP_TX": row_lnk_NA.append("")
        elif colName == "CRT_RUN_CYC_EXCTN_SK": row_lnk_NA.append(100)
        elif colName == "LAST_UPDT_RUN_CYC_EXCTN_SK": row_lnk_NA.append(100)
        elif colName == "AGNT_STTUS_CD_SK": row_lnk_NA.append(1)
        elif colName == "AGNT_TIER_LVL_CD_SK": row_lnk_NA.append(1)
        elif colName == "BILL_ENTY_LVL_CD_SK": row_lnk_NA.append(1)
        elif colName == "COMSN_BSS_SRC_CD_SK": row_lnk_NA.append(1)
        elif colName == "COMSN_COV_CAT_CD_SK": row_lnk_NA.append(1)
        elif colName == "COMSN_DTL_INCM_DISP_CD_SK": row_lnk_NA.append(1)
        elif colName == "COMSN_MTHDLGY_TYP_CD_SK": row_lnk_NA.append(1)
        elif colName == "COMSN_TYP_CD_SK": row_lnk_NA.append(1)
        elif colName == "CT_TYP_CD_SK": row_lnk_NA.append(1)
        elif colName == "FUND_CAT_CD_SK": row_lnk_NA.append(1)
        elif colName == "ACA_BUS_SBSDY_TYP_ID": row_lnk_NA.append("UNK")
        elif colName == "ACA_PRM_SBSDY_AMT": row_lnk_NA.append(0)
        elif colName == "AGNY_HIER_LVL_ID": row_lnk_NA.append("UNK")
        elif colName == "CMS_CUST_POL_ID": row_lnk_NA.append("UNK")
        elif colName == "CMS_PLN_BNF_PCKG_ID": row_lnk_NA.append("UNK")
        elif colName == "COMSN_BLUE_KC_BRKR_PAYMT_IN": row_lnk_NA.append("0")
        elif colName == "COMSN_PAYOUT_TYP_ID": row_lnk_NA.append("UNK")
        elif colName == "COMSN_RETROACTV_ADJ_IN": row_lnk_NA.append("0")
        elif colName == "CUST_BUS_TYP_ID": row_lnk_NA.append("UNK")
        elif colName == "CUST_BUS_SUBTYP_ID": row_lnk_NA.append("UNK")
        elif colName == "ICM_COMSN_PAYE_ID": row_lnk_NA.append("UNK")
        elif colName == "MCARE_ADVNTG_ENR_CYC_YR_ID": row_lnk_NA.append("UNK")
        elif colName == "MCARE_BNFCRY_ID": row_lnk_NA.append("UNK")
        elif colName == "MCARE_ENR_TYP_ID": row_lnk_NA.append("UNK")
        elif colName == "TRANS_CNTR_CT": row_lnk_NA.append(0)
        elif colName == "TRNSMSN_SRC_SYS_CD": row_lnk_NA.append("UNK")
        else:
            row_lnk_NA.append(None)

df_lnk_NA = spark.createDataFrame([tuple(row_lnk_NA)], df_lnk_NA_schema)

df_lnk_ComsnIncmF = df_lnk_ComsnIncm_Extr.filter(
    (F.col("COMSN_INCM_SK") != 0) & (F.col("COMSN_INCM_SK") != 1)
)

row_lnk_UNK = []
for field in df_lnk_UNK_schema.fields:
    colName = field.name
    # Similarly replicate "lnk_UNK" columns from the job JSON
    if colName == "IND":
        row_lnk_UNK.append("0")
    elif colName in ["BILL_COMSN_PRM_PCT","COMSN_RATE_PCT","CNTR_CT","MBR_CT"]:
        row_lnk_UNK.append(None)
    elif colName == "COV_PLN_YR":
        row_lnk_UNK.append("")
    else:
        if colName == "COMSN_INCM_SK": row_lnk_UNK.append(0)
        elif colName in ["PD_AGNT_ID","ERN_AGNT_ID","CLS_PLN_ID","PROD_ID","COMSN_DTL_INCM_DISP_CD","COMSN_TYP_CD","SRC_SYS_CD","AGNT_STTUS_CD","AGNT_STTUS_NM","AGNT_TIER_LVL_CD","AGNT_TIER_LVL_NM","BILL_ENTY_LVL_CD","BILL_ENTY_LVL_NM","COMSN_BSS_SRC_CD","COMSN_BSS_SRC_NM","COMSN_COV_CAT_CD","COMSN_COV_CAT_NM","COMSN_MTHDLGY_TYP_CD","COMSN_MTHDLGY_TYP_NM","COMSN_TYP_NM","CT_TYP_CD","CT_TYP_NM","EXPRNC_CAT_CD","FUND_CAT_CD","FUND_CAT_NM","AGNY_AGNT_ID","COMSN_ARGMT_ID","COMSN_SCHD_ID","FNCL_LOB_CD","GRP_ID","PROD_SH_NM","SUBGRP_ID","SUB_ID"]:
            row_lnk_UNK.append("UNK")
        elif colName in ["BILL_ENTY_UNIQ_KEY","COMSN_INCM_SEQ_NO","BILL_ENTY_SK","BILL_SUM_SK","CLS_PLN_SK","COMSN_AGMNT_SK","COMSN_ARGMT_SK","COMSN_SCHD_SK","ERN_AGNT_SK","EXPRNC_CAT_SK","FNCL_LOB_SK","GRP_SK","PD_AGNT_SK","PROD_SK","PROD_SH_NM_SK","RCVD_INCM_SK","SUBGRP_SK","SUB_SK","AGNT_STTUS_CD_SK","AGNT_TIER_LVL_CD_SK","BILL_ENTY_LVL_CD_SK","COMSN_BSS_SRC_CD_SK","COMSN_COV_CAT_CD_SK","COMSN_DTL_INCM_DISP_CD_SK","COMSN_MTHDLGY_TYP_CD_SK","COMSN_TYP_CD_SK","CT_TYP_CD_SK","FUND_CAT_CD_SK","TRANS_CNTR_CT"]:
            row_lnk_UNK.append(0)
        elif colName in ["COMSN_BSS_AMT","COMSN_RATE_AMT","PD_COMSN_AMT","COMSN_AGMNT_SCHD_FCTR"]:
            row_lnk_UNK.append(0)
        elif colName == "COMSN_BLUE_KC_BRKR_PAYMT_IN":
            row_lnk_UNK.append("0")
        elif colName == "COMSN_RETROACTV_ADJ_IN":
            row_lnk_UNK.append("0")
        elif colName == "ACA_PRM_SBSDY_AMT":
            row_lnk_UNK.append(0)
        elif colName in ["BILL_COMSN_PRM_PCT","COMSN_RATE_PCT","CNTR_CT","MBR_CT"]:
            row_lnk_UNK.append(None)
        elif colName == "COMSN_PD_DT_SK": row_lnk_UNK.append("1753-01-01")
        elif colName == "BILL_INCM_RCPT_BILL_DUE_DT_SK": row_lnk_UNK.append("1753-01-01")
        elif colName == "CRT_RUN_CYC_EXCTN_DT_SK": row_lnk_UNK.append("1753-01-01")
        elif colName == "LAST_UPDT_RUN_CYC_EXCTN_DT_SK": row_lnk_UNK.append("1753-01-01")
        elif colName == "GRP_DP_IN": row_lnk_UNK.append("N")
        elif colName == "COMSN_PD_YR_MO": row_lnk_UNK.append("175301")
        elif colName == "CONT_COV_STRT_DT_SK": row_lnk_UNK.append("1753-01-01")
        elif colName == "COV_END_DT_SK": row_lnk_UNK.append("1753-01-01")
        elif colName == "DP_CANC_YR_MO": row_lnk_UNK.append("175301")
        elif colName == "PLN_YR_COV_EFF_DT_SK": row_lnk_UNK.append("1753-01-01")
        elif colName == "PRM_PD_TO_DT_SK": row_lnk_UNK.append("1753-01-01")
        elif colName == "SRC_TRANS_TYP_TX": row_lnk_UNK.append("")
        elif colName == "CRT_RUN_CYC_EXCTN_SK": row_lnk_UNK.append(100)
        elif colName == "LAST_UPDT_RUN_CYC_EXCTN_SK": row_lnk_UNK.append(100)
        elif colName == "ACA_BUS_SBSDY_TYP_ID": row_lnk_UNK.append("UNK")
        elif colName == "AGNY_HIER_LVL_ID": row_lnk_UNK.append("UNK")
        elif colName == "CMS_CUST_POL_ID": row_lnk_UNK.append("UNK")
        elif colName == "CMS_PLN_BNF_PCKG_ID": row_lnk_UNK.append("UNK")
        elif colName == "COMSN_PAYOUT_TYP_ID": row_lnk_UNK.append("UNK")
        elif colName == "CUST_BUS_TYP_ID": row_lnk_UNK.append("UNK")
        elif colName == "CUST_BUS_SUBTYP_ID": row_lnk_UNK.append("UNK")
        elif colName == "ICM_COMSN_PAYE_ID": row_lnk_UNK.append("UNK")
        elif colName == "MCARE_ADVNTG_ENR_CYC_YR_ID": row_lnk_UNK.append("UNK")
        elif colName == "MCARE_BNFCRY_ID": row_lnk_UNK.append("UNK")
        elif colName == "MCARE_ENR_TYP_ID": row_lnk_UNK.append("UNK")
        elif colName == "TRNSMSN_SRC_SYS_CD": row_lnk_UNK.append("UNK")
        else:
            row_lnk_UNK.append(None)

df_lnk_UNK = spark.createDataFrame([tuple(row_lnk_UNK)], df_lnk_UNK_schema)

df_fnl_Combine_pre = df_lnk_NA.unionByName(df_lnk_ComsnIncmF).unionByName(df_lnk_UNK)

df_fnl_Combine = df_fnl_Combine_pre.select([F.col(c) for c in df_lnk_ComsnIncm_Extr.columns])

# Apply rpad for all columns that are char in final definition
# Job final: "seq_COMSN_INCM_F" has columns with SqlType=char => we rpad accordingly
char_columns_lengths = {
    "COMSN_PD_DT_SK": 10,
    "BILL_INCM_RCPT_BILL_DUE_DT_SK": 10,
    "CRT_RUN_CYC_EXCTN_DT_SK": 10,
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK": 10,
    "COMSN_BLUE_KC_BRKR_PAYMT_IN": 1,
    "COMSN_RETROACTV_ADJ_IN": 1,
    "GRP_DP_IN": 1,
    "COMSN_PD_YR_MO": 6,
    "CONT_COV_STRT_DT_SK": 10,
    "COV_END_DT_SK": 10,
    "COV_PLN_YR": 4,
    "DP_CANC_YR_MO": 6,
    "PLN_YR_COV_EFF_DT_SK": 10,
    "PRM_PD_TO_DT_SK": 10
}

df_final = df_fnl_Combine
for col_name, length_val in char_columns_lengths.items():
    df_final = df_final.withColumn(col_name, F.rpad(F.col(col_name), length_val, " "))

write_files(
    df_final,
    f"{adls_path}/load/COMSN_INCM_F.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)