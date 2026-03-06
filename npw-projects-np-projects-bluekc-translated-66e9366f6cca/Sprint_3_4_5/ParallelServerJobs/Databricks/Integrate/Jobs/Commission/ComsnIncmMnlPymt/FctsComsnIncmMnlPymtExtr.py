# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2012 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     FctsComsnIncmMnlPymtExtr
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:   Pulls data from Commission Manual payment file for loading into IDS.
# MAGIC 
# MAGIC INPUTS:     ComsnIncmMnlPymt.csv
# MAGIC 
# MAGIC PROCESSING:  Extract, transform, and primary keying for Comission Income subject area.
# MAGIC 
# MAGIC OUTPUTS:   Sequential file
# MAGIC 
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                                   ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Tim Sieg                           2020-05-07        us216436                              Originally Programmed                                                    IntegrateDev2         Kalyan Neelam             2020-05-08
# MAGIC 
# MAGIC T.Sieg                              2021-05-03        us335378                      Adding 16 columns to process to include                               IntegrateDev2              Raja Gummadi              2021-05-03
# MAGIC                                                                                                         new ACA and MA commission data
# MAGIC Prabhu ES                       2022-03-14        S2S                               Sybase Params removed                                                        IntegrateDev5            Goutham Kalidindi         2022-06-12
# MAGIC 
# MAGIC T.Sieg                             2025-04-07        US645059                     Change the order of columns in Seq_ComsnIncmMnlPaymt   IntegrateDev2            Jeyaprasanna               2025-04-15
# MAGIC                                                                                                        Update source used in Trns_SplitFile stage for PD_DT,
# MAGIC                                                                                                        BILL_DUE_DT,ENR_DT,EFF_STRT_DT,COV_END_DT
# MAGIC                                                                                                        COV_EFF_DT
# MAGIC                                                                                                        Update logic in Dp_Extract for date format for ENR_DT
# MAGIC                                                                                                        COV_END_DT, and COV_EFF_DT
# MAGIC                                                                                                        Update logic in BusinessRules stage to include new
# MAGIC                                                                                                        TRANS_TYP of PE for svRateAmt,COMSN_RATE_PCT and
# MAGIC                                                                                                        COMSN_BSS_SRC_CD

# MAGIC Read Commission Income Manual Payment Data
# MAGIC Apply business logic
# MAGIC Hash file hf_comsn_incm_allcol cleared
# MAGIC Writing Sequential File to /pkey
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
)
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


TmpOutFile = get_widget_value("TmpOutFile","")
CurrRunCycle = get_widget_value("CurrRunCycle","")
RunID = get_widget_value("RunID","")
CurrDate = get_widget_value("CurrDate","")
SrcSysCdSk = get_widget_value("SrcSysCdSk","")
SrcSysCd = get_widget_value("SrcSysCd","")
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
TransYrMo = get_widget_value("TransYrMo","")
CommMnlPymtFile = get_widget_value("CommMnlPymtFile","")

# MAGIC %run ../../../../shared_containers/PrimaryKey/ComsnIncmPK
# COMMAND ----------

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

df_intrctn_raw = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""
SELECT
 AGNT.AGNT_ID,
 COMSN_AGMNT.COMSN_ARGMT_ID,
 COMSN_AGMNT.EFF_DT_SK,
 AGNT.TERM_DT_SK
FROM {IDSOwner}.AGNT AGNT,
     {IDSOwner}.COMSN_AGMNT COMSN_AGMNT
WHERE AGNT.AGNT_ID = COMSN_AGMNT.AGNT_ID
"""
    )
    .load()
)
df_intrctn = dedup_sort(
    df_intrctn_raw,
    partition_cols=["AGNT_ID","COMSN_ARGMT_ID","EFF_DT_SK"],
    sort_cols=[]
)

df_exprnc_raw = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""
SELECT
 PROD_ID,
 EXPRNC_CAT_SK,
 FNCL_LOB_SK,
 PROD_SH_NM_SK
FROM {IDSOwner}.PROD
"""
    )
    .load()
)
df_exprnc = dedup_sort(
    df_exprnc_raw,
    partition_cols=["PROD_ID"],
    sort_cols=[]
)

df_agmnt_raw = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""
SELECT
 AGNT_ID,
 COMSN_ARGMT_ID,
 EFF_DT_SK,
 SCHD_FCTR,
 COMSN_AGMNT_SK,
 AGNT_SK
FROM {IDSOwner}.COMSN_AGMNT
"""
    )
    .load()
)
df_agmnt = dedup_sort(
    df_agmnt_raw,
    partition_cols=["AGNT_ID","COMSN_ARGMT_ID","EFF_DT_SK"],
    sort_cols=[]
)

df_billsum_raw = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""
SELECT
 BILL_SUM.BILL_ENTY_UNIQ_KEY,
 BILL_SUM.BILL_DUE_DT_SK,
 BILL_SUM.BILL_SUM_SK
FROM {IDSOwner}.BILL_SUM BILL_SUM
"""
    )
    .load()
)
df_billsum = dedup_sort(
    df_billsum_raw,
    partition_cols=["BILL_ENTY_UNIQ_KEY","BILL_DUE_DT_SK"],
    sort_cols=[]
)

df_rcpt_lkup_raw = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""
SELECT
 BILL_INCM_RCPT_SK
FROM {IDSOwner}.BILL_INCM_RCPT
"""
    )
    .load()
)
df_rcpt_lkup = dedup_sort(
    df_rcpt_lkup_raw,
    partition_cols=["BILL_INCM_RCPT_SK"],
    sort_cols=[]
)

df_dpyrmo_raw = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""
SELECT
 BILL_SUM.BILL_ENTY_UNIQ_KEY,
 BILL_SUM.BILL_DUE_DT_SK
FROM {IDSOwner}.BILL_SUM BILL_SUM,
     {IDSOwner}.CD_MPPNG MPPNG
WHERE BILL_SUM.BILL_SUM_SPCL_BILL_CD_SK = MPPNG.CD_MPPNG_SK
  AND MPPNG.TRGT_CD = 'FINLBILL'
"""
    )
    .load()
)
df_dpyrmo = dedup_sort(
    df_dpyrmo_raw,
    partition_cols=["BILL_ENTY_UNIQ_KEY","BILL_DUE_DT_SK"],
    sort_cols=[]
)

df_ern_agnt_raw = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""
SELECT
 AGNT_ID,
 COMSN_ARGMT_ID,
 EFF_DT_SK,
 AGNT_SK
FROM {IDSOwner}.COMSN_AGMNT
;
"""
    )
    .load()
)
df_ern_agnt = dedup_sort(
    df_ern_agnt_raw,
    partition_cols=["AGNT_ID","COMSN_ARGMT_ID","EFF_DT_SK"],
    sort_cols=[]
)

df_agny_agnt_raw = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""
SELECT
 AGNT_ID,
 AGNT_SK
FROM {IDSOwner}.AGNT
;
"""
    )
    .load()
)
df_agny_agnt = dedup_sort(
    df_agny_agnt_raw,
    partition_cols=["AGNT_ID"],
    sort_cols=[]
)

schema_Seq_ComsnIncmMnlPymt = StructType([
    StructField("PERD_NM",StringType(),True),
    StructField("PAYE_ID",StringType(),True),
    StructField("ORDER_ID",StringType(),True),
    StructField("TRNSMSN_YR_MO",StringType(),True),
    StructField("TRANS_ID",StringType(),True),
    StructField("TRANS_TYP",StringType(),True),
    StructField("PD_DT",StringType(),True),
    StructField("PROD_ID",StringType(),True),
    StructField("BILL_ENTY_UNIQ_KEY",StringType(),True),
    StructField("BILL_DUE_DT",StringType(),True),
    StructField("CLS_PLN_ID",StringType(),True),
    StructField("PD_AMT",StringType(),True),
    StructField("DISP_CD",StringType(),True),
    StructField("AGNT_FULL_NM",StringType(),True),
    StructField("AGNT_ID",StringType(),True),
    StructField("AGNY_NM",StringType(),True),
    StructField("AGNY_ID",StringType(),True),
    StructField("AGNT_MAIL_ADDR_LN_1",StringType(),True),
    StructField("AGNT_MAIL_ADDR_LN_2",StringType(),True),
    StructField("AGNT_MAIL_CITY",StringType(),True),
    StructField("AGNT_MAIL_ST_CD",StringType(),True),
    StructField("AGNT_MAIL_ZIP_CD",StringType(),True),
    StructField("SPLT_PCT",StringType(),True),
    StructField("AGNT_TIER_LVL_CD",StringType(),True),
    StructField("EFF_STRT_DT",StringType(),True),
    StructField("EFF_END_DT",StringType(),True),
    StructField("RCV_PAYMT_IN",StringType(),True),
    StructField("GRP_NM",StringType(),True),
    StructField("GRP_ID",StringType(),True),
    StructField("SUBGRP_NM",StringType(),True),
    StructField("SUBGRP_ID",StringType(),True),
    StructField("COV_PLN_YR",StringType(),True),
    StructField("COV_EFF_DT",StringType(),True),
    StructField("COV_END_DT",StringType(),True),
    StructField("MBR_FULL_NM",StringType(),True),
    StructField("MBR_ID",StringType(),True),
    StructField("MBR_TERM_DT",StringType(),True),
    StructField("PROD_BILL_CMPNT_ID",StringType(),True),
    StructField("PROD_BILL_CMPNT_DESC",StringType(),True),
    StructField("FUND_TYP",StringType(),True),
    StructField("COMSN_MTHDLGY_TYP",StringType(),True),
    StructField("ASF_PCT_AMT",StringType(),True),
    StructField("ASF_PCPM_AMT",StringType(),True),
    StructField("GRP_ADDR_ST_CD",StringType(),True),
    StructField("COMSN_CAT_CD",StringType(),True),
    StructField("CNTR_CT",StringType(),True),
    StructField("MBR_CT",StringType(),True),
    StructField("ASF_MBR_CT",StringType(),True),
    StructField("ASF_CNTR_CT",StringType(),True),
    StructField("ASO_MBR_CT",StringType(),True),
    StructField("ASO_CNTR_CT",StringType(),True),
    StructField("ASF_RATE",StringType(),True),
    StructField("ACES_FEE_AMT",StringType(),True),
    StructField("PRM_EQVLNT_AMT",StringType(),True),
    StructField("MNTHLY_CLTD_PRM_AMT",StringType(),True),
    StructField("YTD_CLTD_PRM_AMT",StringType(),True),
    StructField("PTD_CLTD_PRM_AMT",StringType(),True),
    StructField("YTD_PD_AMT",StringType(),True),
    StructField("ANUL_BNS_PT",StringType(),True),
    StructField("CNTR_TYP",StringType(),True),
    StructField("BM_THRSHLD_ACCUM_IN",StringType(),True),
    StructField("PRM_RCPT_DT",StringType(),True),
    StructField("COMSN_ARGMT_ID",StringType(),True),
    StructField("ENR_DT",StringType(),True),
    StructField("ACA_BUS_SBSDY_TYP_ID",StringType(),True),
    StructField("ACA_PRM_SBSDY_AMT",StringType(),True),
    StructField("AGNY_HIER_LVL_ID",StringType(),True),
    StructField("CMS_CUST_POL_ID",StringType(),True),
    StructField("CMS_PLN_BNF_PCKG_ID",StringType(),True),
    StructField("COMSN_BLUE_KC_BRKR_PAYMT_IN",StringType(),True),
    StructField("COMSN_PAYOUT_TYP_ID",StringType(),True),
    StructField("COMSN_RETROACTV_ADJ_IN",StringType(),True),
    StructField("CUST_BUS_TYP_ID",StringType(),True),
    StructField("CUST_BUS_SUBTYP_ID",StringType(),True),
    StructField("ICM_COMSN_PAYE_ID",StringType(),True),
    StructField("MCARE_ADVNTG_ENR_CYC_YR_ID",StringType(),True),
    StructField("MCARE_BNFCRY_ID",StringType(),True),
    StructField("MCARE_ENR_TYP_ID",StringType(),True),
    StructField("TRANS_CNTR_CT",StringType(),True),
    StructField("TRNSMSN_SRC_SYS_CD",StringType(),True)
])

df_Seq_ComsnIncmMnlPymt_raw = (
    spark.read.format("csv")
    .option("header",True)
    .option("quote","\"")
    .schema(schema_Seq_ComsnIncmMnlPymt)
    .load(f"{adls_path_raw}/landing/{CommMnlPymtFile}")
)

df_Extract_Dp_Yr_Mo = df_Seq_ComsnIncmMnlPymt_raw.select(
    F.trim(F.regexp_replace(F.col("PERD_NM"), "[\\x0A\\x0D\\x09]", "")).alias("PERD_NM"),
    F.trim(F.regexp_replace(F.col("PAYE_ID"), "[\\x0A\\x0D\\x09]", "")).alias("PAYE_ID"),
    F.col("BILL_DUE_DT").alias("BILL_DUE_DT")
)
df_Strip1 = df_Seq_ComsnIncmMnlPymt_raw.select(
    F.trim(F.regexp_replace(F.col("PAYE_ID"), "[\\x0A\\x0D\\x09]", "")).alias("PAYE_ID"),
    F.trim(F.regexp_replace(F.col("AGNT_ID"), "[\\x0A\\x0D\\x09]", "")).alias("AGNT_ID"),
    F.col("PD_DT").alias("PD_DT"),
    F.col("BILL_ENTY_UNIQ_KEY").alias("BILL_ENTY_UNIQ_KEY"),
    F.col("BILL_DUE_DT").alias("BILL_DUE_DT"),
    F.col("TRANS_ID").alias("TRANS_ID"),
    F.trim(F.regexp_replace(F.col("CLS_PLN_ID"), "[\\x0A\\x0D\\x09]", "")).alias("CLS_PLN_ID"),
    F.trim(F.regexp_replace(F.col("PROD_ID"), "[\\x0A\\x0D\\x09]", "")).alias("PROD_ID"),
    F.upper(F.trim(F.regexp_replace(F.col("TRANS_TYP"), "[\\x0A\\x0D\\x09]", ""))).alias("TRANS_TYP"),
    F.trim(F.regexp_replace(F.col("DISP_CD"), "[\\x0A\\x0D\\x09]", "")).alias("DISP_CD"),
    F.upper(F.trim(F.regexp_replace(F.col("AGNT_TIER_LVL_CD"), "[\\x0A\\x0D\\x09]", ""))).alias("AGNT_TIER_LVL_CD"),
    F.upper(F.trim(F.regexp_replace(F.col("COMSN_CAT_CD"), "[\\x0A\\x0D\\x09]", ""))).alias("COMSN_CAT_CD"),
    F.upper(F.trim(F.regexp_replace(F.col("COMSN_MTHDLGY_TYP"), "[\\x0A\\x0D\\x09]", ""))).alias("COMSN_MTHDLGY_TYP"),
    F.trim(F.regexp_replace(F.col("FUND_TYP"), "[\\x0A\\x0D\\x09]", "")).alias("FUND_TYP"),
    F.trim(F.regexp_replace(F.col("TRNSMSN_YR_MO"), "[\\x0A\\x0D\\x09]", "")).alias("TRNSMSN_YR_MO"),
    F.col("EFF_STRT_DT").alias("ENR_DT"),
    F.col("COV_EFF_DT").alias("COV_END_DT"),
    F.trim(F.regexp_replace(F.col("COV_PLN_YR"), "[\\x0A\\x0D\\x09]", "")).alias("COV_PLN_YR"),
    F.col("MNTHLY_CLTD_PRM_AMT").alias("MNTHLY_CLTD_PRM_AMT"),
    F.col("ACES_FEE_AMT").alias("ACES_FEE_AMT"),
    F.col("PD_AMT").alias("PD_AMT"),
    F.col("SPLT_PCT").alias("SPLT_PCT"),
    F.col("ASF_PCT_AMT").alias("ASF_PCT_AMT"),
    F.col("EFF_STRT_DT").alias("EFF_STRT_DT"),
    F.col("ASF_PCPM_AMT").alias("ASF_PCPM_AMT"),
    F.expr("""
IF(ASF_MBR_CT IS NULL OR length(trim(ASF_MBR_CT))=0,0,ASF_MBR_CT)
""").alias("ASF_MBR_CT"),
    F.expr("""
IF(ASF_CNTR_CT IS NULL OR length(trim(ASF_CNTR_CT))=0,0,ASF_CNTR_CT)
""").alias("ASF_CNTR_CT"),
    F.expr("""
IF(CNTR_CT IS NULL OR length(trim(CNTR_CT))=0,0,CNTR_CT)
""").alias("CNTR_CT"),
    F.trim(F.regexp_replace(F.col("COMSN_ARGMT_ID"), "[\\x0A\\x0D\\x09]", "")).alias("COMSN_ARGMT_ID"),
    F.upper(F.trim(F.regexp_replace(F.col("PERD_NM"), "[\\x0A\\x0D\\x09]", ""))).alias("PERD_NM"),
    F.lit("0.00").alias("BASE_COMSN_PCT"),
    F.trim(F.regexp_replace(F.col("AGNY_ID"), "[\\x0A\\x0D\\x09]", "" )).alias("AGNY_ID"),
    F.col("COV_EFF_DT").alias("COV_EFF_DT"),
    F.col("ACA_BUS_SBSDY_TYP_ID").alias("ACA_BUS_SBSDY_TYP_ID"),
    F.col("ACA_PRM_SBSDY_AMT").alias("ACA_PRM_SBSDY_AMT"),
    F.col("AGNY_HIER_LVL_ID").alias("AGNY_HIER_LVL_ID"),
    F.col("CMS_CUST_POL_ID").alias("CMS_CUST_POL_ID"),
    F.col("CMS_PLN_BNF_PCKG_ID").alias("CMS_PLN_BNF_PCKG_ID"),
    F.trim(F.col("COMSN_BLUE_KC_BRKR_PAYMT_IN")).alias("COMSN_BLUE_KC_BRKR_PAYMT_IN"),
    F.col("COMSN_PAYOUT_TYP_ID").alias("COMSN_PAYOUT_TYP_ID"),
    F.col("COMSN_RETROACTV_ADJ_IN").alias("COMSN_RETROACTV_ADJ_IN"),
    F.col("CUST_BUS_TYP_ID").alias("CUST_BUS_TYP_ID"),
    F.col("CUST_BUS_SUBTYP_ID").alias("CUST_BUS_SUBTYP_ID"),
    F.col("ICM_COMSN_PAYE_ID").alias("ICM_COMSN_PAYE_ID"),
    F.col("MCARE_ADVNTG_ENR_CYC_YR_ID").alias("MCARE_ADVNTG_ENR_CYC_YR_ID"),
    F.col("MCARE_BNFCRY_ID").alias("MCARE_BNFCRY_ID"),
    F.col("MCARE_ENR_TYP_ID").alias("MCARE_ENR_TYP_ID"),
    F.expr("""
IF(TRANS_CNTR_CT IS NULL OR length(trim(TRANS_CNTR_CT))=0,0,TRANS_CNTR_CT)
""").alias("TRANS_CNTR_CT"),
    F.col("TRNSMSN_SRC_SYS_CD").alias("TRNSMSN_SRC_SYS_CD")
)

df_DpCancYrMo = df_Seq_ComsnIncmMnlPymt_raw.filter(
    (
       (F.col("CNTR_TYP")==F.lit("DP")) &
       (
         (F.col("GRP_ID")==F.lit("10001000"))|
         (F.col("GRP_ID")==F.lit("10003000"))|
         (F.col("GRP_ID")==F.lit("10004000"))|
         (F.col("GRP_ID")==F.lit("10000000"))
       )
    )
).select(
    F.trim(F.regexp_replace(F.col("PAYE_ID"), "[\\x0A\\x0D\\x09]", "")).alias("PAYE_ID"),
    F.trim(F.regexp_replace(F.col("AGNT_ID"), "[\\x0A\\x0D\\x09]", "")).alias("AGNT_ID"),
    F.col("BILL_DUE_DT").alias("PD_DT"),
    F.col("TRANS_ID").alias("TRANS_ID"),
    F.trim(F.regexp_replace(F.col("CLS_PLN_ID"), "[\\x0A\\x0D\\x09]", "")).alias("CLS_PLN_ID"),
    F.trim(F.regexp_replace(F.col("PROD_ID"), "[\\x0A\\x0D\\x09]", "")).alias("PROD_ID"),
    F.trim(F.regexp_replace(F.col("DISP_CD"), "[\\x0A\\x0D\\x09]", "")).alias("DISP_CD"),
    F.upper(F.trim(F.regexp_replace(F.col("TRANS_TYP"), "[\\x0A\\x0D\\x09]", ""))).alias("TRANS_TYP"),
    F.col("BILL_ENTY_UNIQ_KEY").alias("BILL_ENTY_UNIQ_KEY"),
    F.col("BILL_DUE_DT").alias("BILL_DUE_DT")
)

df_dpcancyrmo_tmp = dedup_sort(
    df_DpCancYrMo,
    partition_cols=[
      "PAYE_ID","AGNT_ID","PD_DT","TRANS_ID","CLS_PLN_ID","PROD_ID","DISP_CD","TRANS_TYP","BILL_ENTY_UNIQ_KEY","BILL_DUE_DT"
    ],
    sort_cols=[]
)

df_Strip_tmp = df_Strip1.select(
    "PAYE_ID","AGNT_ID","PD_DT","BILL_ENTY_UNIQ_KEY","BILL_DUE_DT","TRANS_ID","CLS_PLN_ID","PROD_ID","TRANS_TYP",
    "DISP_CD","COMSN_CAT_CD","AGNT_TIER_LVL_CD","COMSN_MTHDLGY_TYP","FUND_TYP","TRNSMSN_YR_MO",
    F.expr('FORMAT.DATE(ENR_DT,"SYBASE","Timestamp","CCYY-MM-DD")').alias("ENR_DT"),
    F.expr('FORMAT.DATE(COV_END_DT,"SYBASE","Timestamp","CCYY-MM-DD")').alias("COV_END_DT"),
    "COV_PLN_YR",
    F.expr("""
IF(MNTHLY_CLTD_PRM_AMT IS NULL OR length(MNTHLY_CLTD_PRM_AMT)=0,0,MNTHLY_CLTD_PRM_AMT)
""").alias("MNTHLY_CLTD_PRM_AMT"),
    F.expr("""
IF(ACES_FEE_AMT IS NULL OR length(ACES_FEE_AMT)=0,0,ACES_FEE_AMT)
""").alias("ACES_FEE_AMT"),
    F.expr("""
IF(PD_AMT IS NULL OR length(PD_AMT)=0,0,PD_AMT)
""").alias("PD_AMT"),
    F.expr("""
IF(SPLT_PCT IS NULL OR length(SPLT_PCT)=0,0,SPLT_PCT)
""").alias("SPLT_PCT"),
    F.expr("""
IF(ASF_PCT_AMT IS NULL OR length(ASF_PCT_AMT)=0,0,ASF_PCT_AMT)
""").alias("ASF_PCT_AMT"),
    "EFF_STRT_DT",
    F.expr("""
IF(ASF_PCPM_AMT IS NULL OR length(ASF_PCPM_AMT)=0,0,ASF_PCPM_AMT)
""").alias("ASF_PCPM_AMT"),
    "ASF_MBR_CT","ASF_CNTR_CT","CNTR_CT","COMSN_ARGMT_ID","PERD_NM",
    F.col("BILL_DUE_DT").alias("BILL_DUE_DT_1"),
    F.expr("""
IF(BASE_COMSN_PCT IS NULL OR length(BASE_COMSN_PCT)=0,0,BASE_COMSN_PCT)
""").alias("BASE_COMSN_PCT"),
    "AGNY_ID",
    F.expr('FORMAT.DATE(COV_EFF_DT,"SYBASE","Timestamp","CCYY-MM-DD")').alias("COV_EFF_DT"),
    "ACA_BUS_SBSDY_TYP_ID","ACA_PRM_SBSDY_AMT","AGNY_HIER_LVL_ID","CMS_CUST_POL_ID","CMS_PLN_BNF_PCKG_ID",
    F.col("COMSN_BLUE_KC_BRKR_PAYMT_IN").alias("COMSN_BLUE_KC_BRKR_PAYMT_IN"),
    "COMSN_PAYOUT_TYP_ID",
    F.col("COMSN_RETROACTV_ADJ_IN").alias("COMSN_RETROACTV_ADJ_IN"),
    "CUST_BUS_TYP_ID","CUST_BUS_SUBTYP_ID","ICM_COMSN_PAYE_ID","MCARE_ADVNTG_ENR_CYC_YR_ID","MCARE_BNFCRY_ID","MCARE_ENR_TYP_ID","TRANS_CNTR_CT","TRNSMSN_SRC_SYS_CD"
)

df_hf_comsnincm_dups = dedup_sort(
    df_Strip_tmp,
    partition_cols=[
      "PAYE_ID","AGNT_ID","PD_DT","BILL_ENTY_UNIQ_KEY","BILL_DUE_DT","TRANS_ID","CLS_PLN_ID","PROD_ID","TRANS_TYP","DISP_CD"
    ],
    sort_cols=[]
)

df_Extract_Dp_Yr_Mo_grp = df_Extract_Dp_Yr_Mo.groupBy("PERD_NM","PAYE_ID").agg(F.max("BILL_DUE_DT").alias("BILL_DUE_DT"))
df_hf_comsnincm_billduedt = dedup_sort(
    df_Extract_Dp_Yr_Mo_grp,
    partition_cols=["PERD_NM","PAYE_ID"],
    sort_cols=[]
)

df_BusinessRules_joined = (
    df_hf_comsnincm_dups.alias("Strip2")
    .join(
        df_hf_comsnincm_billduedt.alias("billduedt_lkup"),
        [
          F.col("Strip2.PERD_NM")==F.col("billduedt_lkup.PERD_NM"),
          F.col("Strip2.PAYE_ID")==F.col("billduedt_lkup.PAYE_ID")
        ],
        "left"
    )
    .join(
        df_intrctn.alias("agnt_lkup"),
        [
          F.col("Strip2.PAYE_ID")==F.col("agnt_lkup.AGNT_ID"),
          F.col("Strip2.COMSN_ARGMT_ID")==F.col("agnt_lkup.COMSN_ARGMT_ID"),
          F.col("Strip2.EFF_STRT_DT")==F.col("agnt_lkup.EFF_DT_SK")
        ],
        "left"
    )
    .join(
        df_exprnc.alias("exprnc_lkup"),
        F.col("Strip2.PROD_ID")==F.col("exprnc_lkup.PROD_ID"),
        "left"
    )
    .join(
        df_agmnt.alias("agmnt_lkup"),
        [
          F.col("Strip2.PAYE_ID")==F.col("agmnt_lkup.AGNT_ID"),
          F.col("Strip2.COMSN_ARGMT_ID")==F.col("agmnt_lkup.COMSN_ARGMT_ID"),
          F.col("Strip2.EFF_STRT_DT")==F.col("agmnt_lkup.EFF_DT_SK")
        ],
        "left"
    )
    .join(
        df_billsum.alias("billsum_lkup"),
        [
          F.col("Strip2.BILL_ENTY_UNIQ_KEY")==F.col("billsum_lkup.BILL_ENTY_UNIQ_KEY"),
          F.col("Strip2.BILL_DUE_DT")==F.col("billsum_lkup.BILL_DUE_DT_SK")
        ],
        "left"
    )
    .join(
        df_dpyrmo.alias("dpyrmo_lkup"),
        [
          F.col("Strip2.BILL_ENTY_UNIQ_KEY")==F.col("dpyrmo_lkup.BILL_ENTY_UNIQ_KEY"),
          F.col("Strip2.BILL_DUE_DT_1")==F.col("dpyrmo_lkup.BILL_DUE_DT_SK")
        ],
        "left"
    )
    .join(
        df_ern_agnt.alias("ernagnt_lkup"),
        [
          F.col("Strip2.AGNT_ID")==F.col("ernagnt_lkup.AGNT_ID"),
          F.col("Strip2.COMSN_ARGMT_ID")==F.col("ernagnt_lkup.COMSN_ARGMT_ID"),
          F.col("Strip2.EFF_STRT_DT")==F.col("ernagnt_lkup.EFF_DT_SK")
        ],
        "left"
    )
    .join(
        df_rcpt_lkup.alias("rcpt_lkup"),
        F.col("Strip2.TRANS_ID")==F.col("rcpt_lkup.BILL_INCM_RCPT_SK"),
        "left"
    )
    .join(
        df_agny_agnt.alias("agnyagnt_lkup"),
        F.col("Strip2.AGNY_ID").substr(F.lit(1),F.lit(8))==F.col("agnyagnt_lkup.AGNT_ID"),
        "left"
    )
    .join(
        df_dpcancyrmo_tmp.alias("dpcncyrmo_lkup"),
        [
          F.col("Strip2.PAYE_ID")==F.col("dpcncyrmo_lkup.PAYE_ID"),
          F.col("Strip2.AGNT_ID")==F.col("dpcncyrmo_lkup.AGNT_ID"),
          F.col("Strip2.PD_DT")==F.col("dpcncyrmo_lkup.PD_DT"),
          F.col("Strip2.TRANS_ID")==F.col("dpcncyrmo_lkup.TRANS_ID"),
          F.col("Strip2.CLS_PLN_ID")==F.col("dpcncyrmo_lkup.CLS_PLN_ID"),
          F.col("Strip2.PROD_ID")==F.col("dpcncyrmo_lkup.PROD_ID"),
          F.col("Strip2.DISP_CD")==F.col("dpcncyrmo_lkup.DISP_CD"),
          F.col("Strip2.TRANS_TYP")==F.col("dpcncyrmo_lkup.TRANS_TYP"),
          F.col("Strip2.BILL_ENTY_UNIQ_KEY")==F.col("dpcncyrmo_lkup.BILL_ENTY_UNIQ_KEY")
        ],
        "left"
    )
)

df_BusinessRules_withVars = df_BusinessRules_joined.withColumn(
    "RowPassThru", F.lit("Y")
).withColumn(
    "svRateAmt",
    F.expr("""
CASE
 WHEN Strip2.TRANS_TYP in ('ADMINFEEGROUPPE','BASECOMPMPE','PE') THEN Strip2.ACES_FEE_AMT
 ELSE Strip2.ASF_PCPM_AMT
END
""")
).withColumn(
    "svCovPlnYr",
    F.expr("""
CASE
 WHEN (Strip2.COV_PLN_YR is null or length(Strip2.COV_PLN_YR)=0) THEN '1753-01-01'
 ELSE Strip2.COV_PLN_YR||'-00-00'
END
""")
)

df_AllCol = df_BusinessRules_withVars.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("Strip2.PAYE_ID").alias("PD_AGNT_ID"),
    F.col("Strip2.AGNT_ID").alias("ERN_AGNT_ID"),
    F.col("Strip2.PD_DT").alias("COMSN_PD_DT_SK"),
    F.col("Strip2.BILL_ENTY_UNIQ_KEY").alias("BILL_ENTY_UNIQ_KEY"),
    F.col("Strip2.BILL_DUE_DT").alias("BILL_INCM_RCPT_BILL_DUE_DT_SK"),
    F.col("Strip2.TRANS_ID").alias("COMSN_INCM_SEQ_NO"),
    F.col("Strip2.CLS_PLN_ID").alias("CLS_PLN_ID"),
    F.col("Strip2.PROD_ID").alias("PROD_ID"),
    F.upper(F.col("Strip2.DISP_CD")).alias("COMSN_DTL_INCM_DISP_CD"),
    F.expr("""
CASE WHEN Strip2.TRANS_TYP is null or length(Strip2.TRANS_TYP)=0 THEN 'NA'
     ELSE Strip2.TRANS_TYP
END
""").alias("COMSN_TYP_CD"),
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.col("RowPassThru").alias("PASS_THRU_IN"),
    F.col("CurrDate").alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("FACETS").alias("SRC_SYS_CD"),
    F.expr("""
"FACETS;"
||Strip2.PAYE_ID||";"
||Strip2.AGNT_ID||";"
||Strip2.PD_DT||";"
||Strip2.BILL_ENTY_UNIQ_KEY||";"
||Strip2.BILL_DUE_DT||";"
||Strip2.TRANS_ID||";"
||Strip2.CLS_PLN_ID||";"
||Strip2.PROD_ID||";"
||Strip2.DISP_CD||";"
||Strip2.TRANS_TYP
""").alias("PRI_KEY_STRING"),
    F.lit(0).alias("COMSN_INCM_SK"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.expr("Left(Strip2.AGNT_ID,8)").alias("AGNT_ID"),
    F.expr("CASE WHEN agnyagnt_lkup.AGNT_SK IS NULL THEN 0 ELSE agnyagnt_lkup.AGNT_SK END").alias("AGNY_AGNT_SK"),
    F.expr("""
CASE WHEN rcpt_lkup.BILL_INCM_RCPT_SK IS NULL OR length(rcpt_lkup.BILL_INCM_RCPT_SK)=0 THEN 0
     ELSE rcpt_lkup.BILL_INCM_RCPT_SK
END
""").alias("BILL_INCM_RCPT_SK"),
    F.lit(0).alias("BILL_ENTY_SK"),
    F.expr("""
CASE WHEN billsum_lkup.BILL_SUM_SK IS NULL OR length(billsum_lkup.BILL_SUM_SK)=0 THEN 0
     ELSE billsum_lkup.BILL_SUM_SK
END
""").alias("BILL_SUM_SK"),
    F.lit(0).alias("CLS_PLN_SK"),
    F.expr("""
CASE WHEN agmnt_lkup.COMSN_AGMNT_SK IS NULL OR length(agmnt_lkup.COMSN_AGMNT_SK)=0 THEN 0
     ELSE agmnt_lkup.COMSN_AGMNT_SK
END
""").alias("COMSN_AGMNT_SK"),
    F.expr("CASE WHEN ernagnt_lkup.AGNT_SK IS NULL THEN 0 ELSE ernagnt_lkup.AGNT_SK END").alias("ERN_AGNT_SK"),
    F.expr("""
CASE WHEN exprnc_lkup.PROD_ID IS NULL OR length(exprnc_lkup.PROD_ID)=0 THEN 0
     ELSE exprnc_lkup.EXPRNC_CAT_SK
END
""").alias("EXPRNC_CAT_SK"),
    F.expr("""
CASE WHEN exprnc_lkup.PROD_ID IS NULL OR length(exprnc_lkup.PROD_ID)=0 THEN 0
     ELSE exprnc_lkup.FNCL_LOB_SK
END
""").alias("FNCL_LOB_SK"),
    F.expr("""
CASE WHEN agmnt_lkup.AGNT_SK IS NULL THEN 0 ELSE agmnt_lkup.AGNT_SK
END
""").alias("PD_AGNT_SK"),
    F.lit(0).alias("PROD_SK"),
    F.expr("""
CASE WHEN exprnc_lkup.PROD_ID IS NULL OR length(exprnc_lkup.PROD_ID)=0 THEN 0
     ELSE exprnc_lkup.PROD_SH_NM_SK
END
""").alias("PROD_SH_NM_SK"),
    F.expr("""
CASE WHEN Strip2.AGNT_TIER_LVL_CD IS NULL OR length(Strip2.AGNT_TIER_LVL_CD)=0 THEN 'NA'
     ELSE Strip2.AGNT_TIER_LVL_CD
END
""").alias("AGNT_TIER_LVL_CD"),
    F.expr("""
CASE WHEN Strip2.COMSN_CAT_CD IS NULL OR length(Strip2.COMSN_CAT_CD)=0 THEN 'NA'
     ELSE Strip2.COMSN_CAT_CD
END
""").alias("COMSN_CAT_CD"),
    F.expr("""
CASE WHEN Strip2.FUND_TYP IS NULL OR length(Strip2.FUND_TYP)=0 THEN 'NA'
     ELSE Strip2.FUND_TYP
END
""").alias("FUND_TYP"),
    F.expr("""
CASE
 WHEN Strip2.TRANS_TYP='BASECOMM' THEN 'ACT'
 WHEN substring(Strip2.TRANS_TYP,1,8)='ADMINFEE' THEN 'DRVD'
 WHEN Strip2.TRANS_TYP in ('ADVANCERECONCILE','BLUEMAX') THEN 'NA'
 ELSE 'NA'
END
""").alias("CT_TYP_CD"),
    F.expr("""
CASE
 WHEN Strip2.COMSN_MTHDLGY_TYP='FLAT PCT' THEN 'FLATPCT'
 WHEN Strip2.COMSN_MTHDLGY_TYP='SLIDING' THEN 'SLIDESCALE'
 WHEN Strip2.COMSN_MTHDLGY_TYP='PCPM' THEN 'PCPM'
 WHEN Strip2.COMSN_MTHDLGY_TYP='FLAT DOLLAR' THEN 'FLATDLR'
 WHEN Strip2.COMSN_MTHDLGY_TYP='PCT' THEN 'PCT'
 ELSE 'SCHD'
END
""").alias("COMSN_MTHDLGY_TYP"),
    F.expr("""
CASE WHEN agnt_lkup.TERM_DT_SK='9999-12-31' THEN 'ACTV'
     ELSE 'INACTV'
END
""").alias("AGNT_STTUS_CD"),
    F.expr("""
CASE
 WHEN Strip2.TRANS_TYP in ('BASECOMMPE','ADMINFEEGROUPPE','PE') THEN 'PRMEQULNT'
 WHEN Strip2.TRANS_TYP in ('ADMINFEEDIRECT','ADMINFEEGROUP') THEN 'ASO'
 WHEN Strip2.TRANS_TYP='ADVANCERECONCILE' THEN 'NA'
 ELSE 'FACETSBILL'
END
""").alias("COMSN_BSS_SRC_CD"),
    F.lit(0).alias("AGNT_STTUS_CD_SK"),
    F.lit(0).alias("AGNT_TIER_LVL_CD_SK"),
    F.lit(0).alias("COMSN_BSS_SRC_CD_SK"),
    F.lit(0).alias("COMSN_COV_CAT_CD_SK"),
    F.lit(0).alias("COMSN_DTL_INCM_DISP_CD_SK"),
    F.lit(0).alias("COMSN_MTHDLGY_TYP_CD_SK"),
    F.lit(0).alias("COMSN_TYP_CD_SK"),
    F.lit(0).alias("CT_TYP_CD_SK"),
    F.lit(0).alias("FUND_CAT_CD_SK"),
    F.expr("""
CASE WHEN Strip2.PD_DT IS NULL OR length(Strip2.PD_DT)=0 THEN '201101'
     ELSE FORMAT.DATE(Strip2.PD_DT,'DATE','DATE','CCYYMM')
END
""").alias("COMSN_PD_YR_MO"),
    F.expr("""
CASE WHEN Strip2.ENR_DT IS NULL OR length(Strip2.ENR_DT)=0 THEN '1753-01-01'
     ELSE Strip2.ENR_DT
END
""").alias("CONT_COV_STRT_DT_SK"),
    F.expr("""
CASE WHEN Strip2.COV_END_DT IS NULL OR length(Strip2.COV_END_DT)=0 THEN '2199-12-31'
     WHEN Strip2.COV_END_DT='2200-01-01' THEN '2199-12-31'
     ELSE Strip2.COV_END_DT
END
""").alias("COV_END_DT_SK"),
    F.expr("""
CASE WHEN Strip2.COV_PLN_YR IS NULL OR length(Strip2.COV_PLN_YR)=0 THEN '2199'
     ELSE Strip2.COV_PLN_YR
END
""").alias("COV_PLN_YR"),
    F.expr("""
CASE WHEN dpyrmo_lkup.BILL_DUE_DT_SK IS NULL THEN '219901'
     WHEN dpcncyrmo_lkup.BILL_DUE_DT IS NOT NULL
          AND dpyrmo_lkup.BILL_DUE_DT_SK=dpcncyrmo_lkup.BILL_DUE_DT
     THEN regexp_replace(dpyrmo_lkup.BILL_DUE_DT_SK.substr(1,7),'-','')
     ELSE '219901'
END
""").alias("DP_CANC_YR_MO"),
    F.expr("""
CASE WHEN Strip2.COV_EFF_DT IS NULL OR length(Strip2.COV_EFF_DT)=0 THEN svCovPlnYr
     ELSE Strip2.COV_EFF_DT
END
""").alias("PLN_YR_COV_EFF_DT_SK"),
    F.expr("""
CASE WHEN billduedt_lkup.BILL_DUE_DT IS NULL OR length(billduedt_lkup.BILL_DUE_DT)=0 THEN '2011-01-01'
     ELSE billduedt_lkup.BILL_DUE_DT
END
""").alias("PRM_PD_TO_DT_SK"),
    F.col("Strip2.MNTHLY_CLTD_PRM_AMT").alias("COMSN_BSS_AMT"),
    F.expr("""
CASE WHEN svRateAmt IS NULL OR length(svRateAmt)=0 THEN 0 ELSE svRateAmt
END
""").alias("COMSN_RATE_AMT"),
    F.col("Strip2.PD_AMT").alias("PD_COMSN_AMT"),
    F.col("Strip2.SPLT_PCT").alias("BILL_COMSN_PRM_PCT"),
    F.expr("""
CASE WHEN agmnt_lkup.SCHD_FCTR IS NULL OR length(agmnt_lkup.SCHD_FCTR)=0 THEN 0
     ELSE agmnt_lkup.SCHD_FCTR
END
""").alias("COMSN_AGMNT_SCHD_FCTR"),
    F.expr("""
CASE
 WHEN Strip2.TRANS_TYP in ('BASECOMMPE','BASECOMM') THEN Strip2.BASE_COMSN_PCT
 WHEN Strip2.TRANS_TYP in ('ADMINFEEDIRECT','ADMINFEEGROUP','ADMINFEEGROUPPE','BLUEMAX','PE')
 THEN Strip2.ASF_PCT_AMT
 ELSE 0
END
""").alias("COMSN_RATE_PCT"),
    F.expr("""
CASE
 WHEN Strip2.TRANS_TYP='BASECOMM' THEN Strip2.CNTR_CT
 WHEN substring(Strip2.TRANS_TYP,1,8)='ADMINFEE' THEN Strip2.ASF_CNTR_CT
 ELSE 0
END
""").alias("CNTR_CT"),
    F.expr("""
CASE
 WHEN substring(Strip2.TRANS_TYP,1,8)='ADMINFEE' THEN Strip2.ASF_MBR_CT
 ELSE 0
END
""").alias("MBR_CT"),
    F.expr("""
CASE WHEN Strip2.TRANS_TYP IS NULL OR length(Strip2.TRANS_TYP)=0 THEN 'NA'
     ELSE Strip2.TRANS_TYP
END
""").alias("SRC_TRANS_TYP_TX"),
    F.col("Strip2.COMSN_ARGMT_ID").alias("COMSN_ARGMT_ID"),
    F.col("Strip2.EFF_STRT_DT").alias("EFF_STRT_DT"),
    F.col("Strip2.ACA_BUS_SBSDY_TYP_ID").alias("ACA_BUS_SBSDY_TYP_ID"),
    F.col("Strip2.ACA_PRM_SBSDY_AMT").alias("ACA_PRM_SBSDY_AMT"),
    F.col("Strip2.AGNY_HIER_LVL_ID").alias("AGNY_HIER_LVL_ID"),
    F.col("Strip2.CMS_CUST_POL_ID").alias("CMS_CUST_POL_ID"),
    F.col("Strip2.CMS_PLN_BNF_PCKG_ID").alias("CMS_PLN_BNF_PCKG_ID"),
    F.col("Strip2.COMSN_BLUE_KC_BRKR_PAYMT_IN").alias("COMSN_BLUE_KC_BRKR_PAYMT_IN"),
    F.col("Strip2.COMSN_PAYOUT_TYP_ID").alias("COMSN_PAYOUT_TYP_ID"),
    F.col("Strip2.COMSN_RETROACTV_ADJ_IN").alias("COMSN_RETROACTV_ADJ_IN"),
    F.col("Strip2.CUST_BUS_TYP_ID").alias("CUST_BUS_TYP_ID"),
    F.col("Strip2.CUST_BUS_SUBTYP_ID").alias("CUST_BUS_SUBTYP_ID"),
    F.col("Strip2.ICM_COMSN_PAYE_ID").alias("ICM_COMSN_PAYE_ID"),
    F.col("Strip2.MCARE_ADVNTG_ENR_CYC_YR_ID").alias("MCARE_ADVNTG_ENR_CYC_YR_ID"),
    F.col("Strip2.MCARE_BNFCRY_ID").alias("MCARE_BNFCRY_ID"),
    F.col("Strip2.MCARE_ENR_TYP_ID").alias("MCARE_ENR_TYP_ID"),
    F.col("Strip2.TRANS_CNTR_CT").alias("TRANS_CNTR_CT"),
    F.col("Strip2.TRNSMSN_SRC_SYS_CD").alias("TRNSMSN_SRC_SYS_CD")
)

df_Transform = df_BusinessRules_withVars.select(
    F.col("Strip2.PAYE_ID").alias("PD_AGNT_ID"),
    F.col("Strip2.AGNT_ID").alias("ERN_AGNT_ID"),
    F.col("Strip2.PD_DT").alias("COMSN_PD_DT_SK"),
    F.col("Strip2.BILL_ENTY_UNIQ_KEY").alias("BILL_ENTY_UNIQ_KEY"),
    F.col("Strip2.BILL_DUE_DT").alias("BILL_INCM_RCPT_BILL_DUE_DT_SK"),
    F.col("Strip2.TRANS_ID").alias("COMSN_INCM_SEQ_NO"),
    F.col("Strip2.CLS_PLN_ID").alias("CLS_PLN_ID"),
    F.col("Strip2.PROD_ID").alias("PROD_ID"),
    F.upper(F.col("Strip2.DISP_CD")).alias("COMSN_DTL_INCM_DISP_CD"),
    F.expr("""
CASE WHEN Strip2.TRANS_TYP is null or length(Strip2.TRANS_TYP)=0 THEN 'NA'
     ELSE Strip2.TRANS_TYP
END
""").alias("COMSN_TYP_CD"),
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK")
)

df_ComsnIncmPK_Key = ComsnIncmPK(
    df_AllCol,
    df_Transform,
    {
        "CurrRunCycle": CurrRunCycle,
        "SrcSysCd": SrcSysCd,
        "RunID": RunID,
        "CurrentDate": CurrDate,
        "IDSOwner": IDSOwner
    }
)

final_cols = [
    ("JOB_EXCTN_RCRD_ERR_SK","varchar",None),
    ("INSRT_UPDT_CD","char",10),
    ("DISCARD_IN","char",1),
    ("PASS_THRU_IN","char",1),
    ("FIRST_RECYC_DT","varchar",None),
    ("ERR_CT","varchar",None),
    ("RECYCLE_CT","varchar",None),
    ("SRC_SYS_CD","varchar",None),
    ("PRI_KEY_STRING","varchar",None),
    ("COMSN_INCM_SK","varchar",None),
    ("PD_AGNT_ID","varchar",None),
    ("ERN_AGNT_ID","varchar",None),
    ("COMSN_PD_DT_SK","char",10),
    ("BILL_ENTY_UNIQ_KEY","varchar",None),
    ("BILL_INCM_RCPT_BILL_DUE_DT_SK","char",10),
    ("COMSN_INCM_SEQ_NO","varchar",None),
    ("CLS_PLN_ID","varchar",None),
    ("PROD_ID","varchar",None),
    ("COMSN_DTL_INCM_DISP_CD","varchar",None),
    ("COMSN_TYP_CD","varchar",None),
    ("CRT_RUN_CYC_EXTCN_SK","varchar",None),
    ("LAST_UPDT_RUN_CYC_EXTCN_SK","varchar",None),
    ("AGNT_ID","varchar",None),
    ("AGNY_AGNT_SK","varchar",None),
    ("BILL_INCM_RCPT_SK","varchar",None),
    ("BILL_ENTY_SK","varchar",None),
    ("BILL_SUM_SK","varchar",None),
    ("CLS_PLN_SK","varchar",None),
    ("COMSN_AGMNT_SK","varchar",None),
    ("ERN_AGNT_SK","varchar",None),
    ("EXPRNC_CAT_SK","varchar",None),
    ("FNCL_LOB_SK","varchar",None),
    ("PD_AGNT_SK","varchar",None),
    ("PROD_SK","varchar",None),
    ("PROD_SH_NM_SK","varchar",None),
    ("AGNT_TIER_LVL_CD","varchar",None),
    ("COMSN_CAT_CD","varchar",None),
    ("FUND_TYP","varchar",None),
    ("CT_TYP_CD","varchar",None),
    ("COMSN_MTHDLGY_TYP","varchar",None),
    ("AGNT_STTUS_CD","varchar",None),
    ("COMSN_BSS_SRC_CD","varchar",None),
    ("AGNT_STTUS_CD_SK","varchar",None),
    ("AGNT_TIER_LVL_CD_SK","varchar",None),
    ("COMSN_BSS_SRC_CD_SK","varchar",None),
    ("COMSN_COV_CAT_CD_SK","varchar",None),
    ("COMSN_DTL_INCM_DISP_CD_SK","varchar",None),
    ("COMSN_MTHDLGY_TYP_CD_SK","varchar",None),
    ("COMSN_TYP_CD_SK","varchar",None),
    ("CT_TYP_CD_SK","varchar",None),
    ("FUND_CAT_CD_SK","varchar",None),
    ("COMSN_PD_YR_MO","char",6),
    ("CONT_COV_STRT_DT_SK","char",10),
    ("COV_END_DT_SK","char",10),
    ("COV_PLN_YR","char",4),
    ("DP_CANC_YR_MO","char",6),
    ("PLN_YR_COV_EFF_DT_SK","char",10),
    ("PRM_PD_TO_DT_SK","char",10),
    ("COMSN_BSS_AMT","varchar",None),
    ("COMSN_RATE_AMT","varchar",None),
    ("PD_COMSN_AMT","varchar",None),
    ("BILL_COMSN_PRM_PCT","varchar",None),
    ("COMSN_AGMNT_SCHD_FCTR","varchar",None),
    ("COMSN_RATE_PCT","varchar",None),
    ("CNTR_CT","varchar",None),
    ("MBR_CT","varchar",None),
    ("SRC_TRANS_TYP_TX","varchar",None),
    ("COMSN_ARGMT_ID","varchar",None),
    ("EFF_STRT_DT","char",10),
    ("ACA_BUS_SBSDY_TYP_ID","varchar",None),
    ("ACA_PRM_SBSDY_AMT","varchar",None),
    ("AGNY_HIER_LVL_ID","varchar",None),
    ("CMS_CUST_POL_ID","varchar",None),
    ("CMS_PLN_BNF_PCKG_ID","varchar",None),
    ("COMSN_BLUE_KC_BRKR_PAYMT_IN","char",1),
    ("COMSN_PAYOUT_TYP_ID","varchar",None),
    ("COMSN_RETROACTV_ADJ_IN","char",1),
    ("CUST_BUS_TYP_ID","varchar",None),
    ("CUST_BUS_SUBTYP_ID","varchar",None),
    ("ICM_COMSN_PAYE_ID","varchar",None),
    ("MCARE_ADVNTG_ENR_CYC_YR_ID","varchar",None),
    ("MCARE_BNFCRY_ID","varchar",None),
    ("MCARE_ENR_TYP_ID","varchar",None),
    ("TRANS_CNTR_CT","varchar",None),
    ("TRNSMSN_SRC_SYS_CD","varchar",None)
]

df_final = df_ComsnIncmPK_Key.select([
    F.rpad(F.col(c[0]).cast(StringType()), c[2] if c[2] else 255, " ").alias(c[0]) if (c[1] in ["char","varchar"]) else F.col(c[0]).alias(c[0])
    for c in final_cols
])

write_files(
    df_final,
    f"{adls_path}/key/{TmpOutFile}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)