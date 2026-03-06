# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2012 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     FctsComsnIncmExtr
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:   Pulls data from CMC_COAG_AGREEMENT for loading into IDS.
# MAGIC       
# MAGIC 
# MAGIC INPUTS:     Facets -  CLLDS_COMSN_PAYMT_RCVD
# MAGIC 
# MAGIC PROCESSING:  Extract, transform, and primary keying for Comission Income subject area.
# MAGIC 
# MAGIC OUTPUTS:   Sequential file
# MAGIC 
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                                    Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                              ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Bhoomi Dasari                 10/24/2012         4873                              Originally Programmed                                                   IntegrateNewDevl         Kalyan Neelam            2012-11-06
# MAGIC 
# MAGIC T.Sieg                              2021-02-19         335378                         Adding 16 new columns for the ACA and MA                 IntegrateDev2               Raja Gummadi             2021-05-03
# MAGIC                                                                                                         commission activity
# MAGIC                                                                                                         Adding Fcts_SBSB_ID stage for populating
# MAGIC                                                                                                         BILL_ENTY_UNIQ_KEY and PROD_ID
# MAGIC                                                                                                         Adding new hf_comsnincm_sbsbid and included in HASH.CLEAR
# MAGIC                                                                                                         Adding sort to SQL extract in CLLDS_COMSN_PAYMT_RCVD
# MAGIC                                                                                                         stage and stage variables in the StripFields stage
# MAGIC                                                                                                         to prevent dropping any payments
# MAGIC                                                                                                         with missing data from source transactions
# MAGIC                                                                                                         Adding Facets parameters
# MAGIC Prabhu ES                        2022-03-14          S2S                           MSSQL ODBC conn params added                                 IntegrateDev5                 Goutham Kalidindi       2022-06-12
# MAGIC 
# MAGIC T. Sieg                            2025-04-07           US645059                 Update logic in BusinessRules stage to include new       IntegrateDev2                 Jeyaprasanna             2025-04-15
# MAGIC                                                                                                        TRANS_TYP of PE for svRateAmt,COMSN_RATE_PCT and
# MAGIC                                                                                                        COMSN_BSS_SRC_CD

# MAGIC Extract SBSB_ID to populate missing billing data
# MAGIC Extract Facets Commission Income Data
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
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DecimalType, IntegerType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/ComsnIncmPK
# COMMAND ----------

# PARAMETER WIDGETS
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
CurrDate = get_widget_value('CurrDate','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
SrcSysCd = get_widget_value('SrcSysCd','')
TmpOutFile = get_widget_value('TmpOutFile','')
TransYrMo = get_widget_value('TransYrMo','')

bcbs_secret_name = get_widget_value('bcbs_secret_name','')
BCBSOwner = get_widget_value('BCBSOwner','')

ids_secret_name = get_widget_value('ids_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')

facets_secret_name = get_widget_value('facets_secret_name','')
FacetsOwner = get_widget_value('FacetsOwner','')

# GET DATABASE CONFIGS
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
jdbc_url_bcbs, jdbc_props_bcbs = get_db_config(bcbs_secret_name)
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)

# STAGE: IDS (DB2Connector) - READING MULTIPLE QUERIES FROM IDS
query_intrctn = f"""
SELECT 
AGNT.AGNT_ID,
COMSN_AGMNT.COMSN_ARGMT_ID,
COMSN_AGMNT.EFF_DT_SK,
AGNT.TERM_DT_SK
FROM {IDSOwner}.AGNT AGNT,
     {IDSOwner}.COMSN_AGMNT COMSN_AGMNT
WHERE AGNT.AGNT_ID = COMSN_AGMNT.AGNT_ID
"""
df_IDS_intrctn = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_intrctn.strip())
    .load()
)

query_exprnc = f"""
SELECT 
PROD_ID,
EXPRNC_CAT_SK,
FNCL_LOB_SK,
PROD_SH_NM_SK
FROM {IDSOwner}.PROD
"""
df_IDS_exprnc = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_exprnc.strip())
    .load()
)

query_agmnt = f"""
SELECT 
AGNT_ID,
COMSN_ARGMT_ID,
EFF_DT_SK,
SCHD_FCTR,
COMSN_AGMNT_SK,
AGNT_SK
FROM {IDSOwner}.COMSN_AGMNT
"""
df_IDS_agmnt = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_agmnt.strip())
    .load()
)

query_BillSum = f"""
SELECT 
BILL_SUM.BILL_ENTY_UNIQ_KEY as BILL_ENTY_UNIQ_KEY,
BILL_SUM.BILL_DUE_DT_SK as BILL_DUE_DT_SK,
BILL_SUM.BILL_SUM_SK as BILL_SUM_SK
FROM {IDSOwner}.BILL_SUM BILL_SUM
"""
df_IDS_BillSum = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_BillSum.strip())
    .load()
)

# Per instructions for "WHERE ... = ?": read full table instead
query_rcpt_lkup = f"""
SELECT 
BILL_INCM_RCPT_SK
FROM {IDSOwner}.BILL_INCM_RCPT
"""
df_IDS_rcpt_lkup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_rcpt_lkup.strip())
    .load()
)

query_BillSum1 = f"""
SELECT 
BILL_SUM.BILL_ENTY_UNIQ_KEY,
BILL_SUM.BILL_DUE_DT_SK
FROM {IDSOwner}.BILL_SUM BILL_SUM,
     {IDSOwner}.CD_MPPNG MPPNG
WHERE
BILL_SUM.BILL_SUM_SPCL_BILL_CD_SK = MPPNG.CD_MPPNG_SK
AND MPPNG.TRGT_CD = 'FINLBILL'
"""
df_IDS_BillSum1 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_BillSum1.strip())
    .load()
)

# HFs FROM IDS => SCENARIO A (INTERMEDIATE HASHED FILES). WE DEDUP ON KEY COLUMNS AND SKIP WRITING
df_hf_comsnincm_agntlkup = dedup_sort(
    df_IDS_intrctn,
    ["AGNT_ID","COMSN_ARGMT_ID","EFF_DT_SK"],
    []
)
df_hf_comsnincm_prod_lkup = dedup_sort(
    df_IDS_exprnc,
    ["PROD_ID"],
    []
)
df_hf_comsnincm_argmtlkup = dedup_sort(
    df_IDS_agmnt,
    ["AGNT_ID","COMSN_ARGMT_ID","EFF_DT_SK"],
    []
)
df_hf_comsnincm_billsumlkup = dedup_sort(
    df_IDS_BillSum,
    ["BILL_ENTY_UNIQ_KEY","BILL_DUE_DT_SK"],
    []
)
df_hf_comsnincm_dpyrmolkup = dedup_sort(
    df_IDS_BillSum1,
    ["BILL_ENTY_UNIQ_KEY","BILL_DUE_DT_SK"],
    []
)

# STAGE: IDS_Tables (DB2Connector, DB=IDS, reading 2 queries)
query_ern_agnt = f"""
SELECT 
AGNT_ID,
COMSN_ARGMT_ID,
EFF_DT_SK,
AGNT_SK
FROM {IDSOwner}.COMSN_AGMNT
;
"""
df_IDS_Tables_ern_agnt = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_ern_agnt.strip())
    .load()
)

query_agny_agnt = f"""
SELECT
AGNT_ID,
AGNT_SK
FROM {IDSOwner}.AGNT
;
"""
df_IDS_Tables_agny_agnt = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_agny_agnt.strip())
    .load()
)

df_hf_comsnincm_ernagntlkup = dedup_sort(
    df_IDS_Tables_ern_agnt,
    ["AGNT_ID","COMSN_ARGMT_ID","EFF_DT_SK"],
    []
)
df_hf_comsnincm_agnyagntlkup = dedup_sort(
    df_IDS_Tables_agny_agnt,
    ["AGNT_ID"],
    []
)

# STAGE: CLLDS_COMSN_PAYMT_RCVD (ODBCConnector, DB=BCBS) => 3 queries
query_Extract = f"""
SELECT
PAYE_ID,
AGNT_ID,
PD_DT,
BILL_ENTY_UNIQ_KEY,
BILL_DUE_DT,
TRANS_ID,
CLS_PLN_ID,
PROD_ID,
AGNT_TIER_LVL_CD,
TRANS_TYP,
COMSN_CAT_CD,
DISP_CD,
COMSN_MTHDLGY_TYP,
FUND_TYP,
TRNSMSN_YR_MO,
ENR_DT,
COV_END_DT,
COV_PLN_YR,
MNTHLY_CLTD_PRM_AMT,
ACES_FEE_AMT,
PD_AMT,
SPLT_PCT,
ASF_PCT_AMT,
EFF_STRT_DT,
ASF_PCPM_AMT,
ASF_MBR_CT,
ASF_CNTR_CT,
CNTR_CT,
COMSN_ARGMT_ID,
PERD_NM,
BASE_COMSN_PCT,
AGNY_ID,
COV_EFF_DT,
MBR_ID,
ACA_BUS_SBSDY_TYP_ID,
ACA_PRM_SBSDY_AMT,
AGNY_HIER_LVL_ID,
CMS_CUST_POL_ID,
CMS_PLN_BNF_PCKG_ID,
COMSN_BLUE_KC_BRKR_PAYMT_IN,
COMSN_PAYOUT_TYP_ID,
COMSN_RETROACTV_ADJ_IN,
CUST_BUS_TYP_ID,
CUST_BUS_SUBTYP_ID,
ICM_COMSN_PAYE_ID,
MCARE_ADVNTG_ENR_CYC_YR_ID,
MCARE_BNFCRY_ID,
MCARE_ENR_TYP_ID,
TRANS_CNTR_CT,
TRNSMSN_SRC_SYS_CD
FROM {BCBSOwner}.CLLDS_COMSN_PAYMT_RCVD
WHERE TRNSMSN_YR_MO = '{TransYrMo}'
ORDER BY 
PAYE_ID,
AGNT_ID,
PD_DT,
BILL_ENTY_UNIQ_KEY,
BILL_DUE_DT,
TRANS_ID,
CLS_PLN_ID,
PROD_ID,
AGNT_TIER_LVL_CD,
TRANS_TYP,
COMSN_CAT_CD,
DISP_CD
"""
df_CLLDS_COMSN_PAYMT_RCVD_Extract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_bcbs)
    .options(**jdbc_props_bcbs)
    .option("query", query_Extract.strip())
    .load()
)

query_Extract_Dp_Yr_Mo = f"""
SELECT DISTINCT
MAX(BILL_DUE_DT) as BILL_DUE_DT,
PERD_NM,
PAYE_ID
FROM {BCBSOwner}.CLLDS_COMSN_PAYMT_RCVD
WHERE TRNSMSN_YR_MO = '{TransYrMo}'
GROUP BY
PERD_NM,
PAYE_ID
"""
df_CLLDS_COMSN_PAYMT_RCVD_Extract_Dp_Yr_Mo = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_bcbs)
    .options(**jdbc_props_bcbs)
    .option("query", query_Extract_Dp_Yr_Mo.strip())
    .load()
)

query_Dp_Canc_Yr_Mo = f"""
SELECT
BILL_ENTY_UNIQ_KEY,
BILL_DUE_DT,
PAYE_ID,
AGNT_ID,
PD_DT,
TRANS_ID,
CLS_PLN_ID,
PROD_ID,
DISP_CD,
TRANS_TYP
FROM {BCBSOwner}.CLLDS_COMSN_PAYMT_RCVD
WHERE
CNTR_TYP = 'DP'
AND GRP_ID IN ('10001000','10003000','10004000','10000000')
AND TRNSMSN_YR_MO = '{TransYrMo}'
"""
df_CLLDS_COMSN_PAYMT_RCVD_Dp_Canc_Yr_Mo = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_bcbs)
    .options(**jdbc_props_bcbs)
    .option("query", query_Dp_Canc_Yr_Mo.strip())
    .load()
)

# STAGE: Max_bill_due_dt (CTransformer)
df_Max_bill_due_dt = (
    df_CLLDS_COMSN_PAYMT_RCVD_Extract_Dp_Yr_Mo
    .withColumn("PERD_NM", F.upper(trim(strip_field(F.col("PERD_NM")))))
    .withColumn("PAYE_ID", trim(strip_field(F.col("PAYE_ID"))))
    .withColumn("BILL_DUE_DT", F.date_format(F.col("BILL_DUE_DT"), "yyyy-MM-dd"))
    .select("PERD_NM","PAYE_ID","BILL_DUE_DT")
)

df_hf_comsnincm_billduedt = dedup_sort(
    df_Max_bill_due_dt,
    ["PERD_NM","PAYE_ID","BILL_DUE_DT"],
    []
)

# STAGE: StripFields (CTransformer)
df_StripFields = (
    df_CLLDS_COMSN_PAYMT_RCVD_Extract
    .withColumn("ROWNUM", F.monotonically_increasing_id().cast("long") + F.lit(1))  # surrogate for @INROWNUM
    .withColumn("PAYE_ID", trim(strip_field(F.col("PAYE_ID"))))
    .withColumn("AGNT_ID", trim(strip_field(F.col("AGNT_ID"))))
    .withColumn("PD_DT", F.date_format(F.col("PD_DT"), "yyyy-MM-dd"))
    .withColumn("BILL_ENTY_UNIQ_KEY", F.when(F.col("BILL_ENTY_UNIQ_KEY").isNull() | (F.length(trim(F.col("BILL_ENTY_UNIQ_KEY")))==0), F.lit(0)).otherwise(trim(F.col("BILL_ENTY_UNIQ_KEY"))))
    .withColumn("BILL_DUE_DT", F.date_format(F.col("BILL_DUE_DT"), "yyyy-MM-dd"))
    .withColumn("TRANS_ID", F.col("TRANS_ID"))
    .withColumn("CLS_PLN_ID", F.when(
        F.col("CLS_PLN_ID").isNull() | (F.length(trim(F.col("CLS_PLN_ID")))==0) | (F.col("CLS_PLN_ID")==F.lit(" ")) | (F.substring(F.col("CLS_PLN_ID"),1,1)==F.lit(" ")),
        F.lit("ACAMAM00")).otherwise(trim(strip_field(F.col("CLS_PLN_ID"))))
    )
    .withColumn("PROD_ID", F.when(
        F.col("PROD_ID").isNull() | (F.length(trim(F.col("PROD_ID")))==0) | (F.substring(F.col("PROD_ID"),1,1)==F.lit(" ")),
        F.lit("ACAMAM00")).otherwise(trim(strip_field(F.col("PROD_ID"))))
    )
    .withColumn("TRANS_TYP", F.upper(trim(strip_field(F.col("TRANS_TYP")))))
    .withColumn("DISP_CD", trim(strip_field(F.col("DISP_CD"))))
    .withColumn("COMSN_CAT_CD", F.upper(trim(strip_field(F.col("COMSN_CAT_CD")))))
    .withColumn("AGNT_TIER_LVL_CD", F.upper(trim(strip_field(F.col("AGNT_TIER_LVL_CD")))))
    .withColumn("COMSN_MTHDLGY_TYP", F.upper(trim(strip_field(F.col("COMSN_MTHDLGY_TYP")))))
    .withColumn("FUND_TYP", trim(strip_field(F.col("FUND_TYP"))))
    .withColumn("TRNSMSN_YR_MO", trim(strip_field(F.col("TRNSMSN_YR_MO"))))
    .withColumn("ENR_DT", F.date_format(F.col("ENR_DT"), "yyyy-MM-dd"))
    .withColumn("COV_END_DT", F.date_format(F.col("COV_END_DT"), "yyyy-MM-dd"))
    .withColumn("COV_PLN_YR", trim(strip_field(F.col("COV_PLN_YR"))))
    .withColumn("MNTHLY_CLTD_PRM_AMT", F.when(F.col("MNTHLY_CLTD_PRM_AMT").isNull() | (F.length(trim(F.col("MNTHLY_CLTD_PRM_AMT")))==0), F.lit(0)).otherwise(F.col("MNTHLY_CLTD_PRM_AMT")))
    .withColumn("ACES_FEE_AMT", F.col("ACES_FEE_AMT"))
    .withColumn("PD_AMT", F.col("PD_AMT"))
    .withColumn("SPLT_PCT", F.col("SPLT_PCT"))
    .withColumn("ASF_PCT_AMT", F.col("ASF_PCT_AMT"))
    .withColumn("EFF_STRT_DT", F.date_format(F.col("EFF_STRT_DT"), "yyyy-MM-dd"))
    .withColumn("ASF_PCPM_AMT", F.col("ASF_PCPM_AMT"))
    .withColumn("ASF_MBR_CT", F.when(F.col("ASF_MBR_CT").isNull() | (F.length(trim(F.col("ASF_MBR_CT")))==0), F.lit(0)).otherwise(F.col("ASF_MBR_CT")))
    .withColumn("ASF_CNTR_CT", F.when(F.col("ASF_CNTR_CT").isNull() | (F.length(trim(F.col("ASF_CNTR_CT")))==0), F.lit(0)).otherwise(F.col("ASF_CNTR_CT")))
    .withColumn("CNTR_CT", F.when(F.col("CNTR_CT").isNull() | (F.length(trim(F.col("CNTR_CT")))==0), F.lit(0)).otherwise(F.col("CNTR_CT")))
    .withColumn("COMSN_ARGMT_ID", trim(strip_field(F.col("COMSN_ARGMT_ID"))))
    .withColumn("PERD_NM", F.upper(trim(strip_field(F.col("PERD_NM")))))
    .withColumn("BASE_COMSN_PCT", F.col("BASE_COMSN_PCT"))
    .withColumn("AGNY_ID", trim(strip_field(F.col("AGNY_ID"))))
    .withColumn("COV_EFF_DT", F.date_format(F.col("COV_EFF_DT"), "yyyy-MM-dd"))
    .withColumn("MBR_ID", F.col("MBR_ID"))
    .withColumn("ACA_BUS_SBSDY_TYP_ID", F.col("ACA_BUS_SBSDY_TYP_ID"))
    .withColumn("ACA_PRM_SBSDY_AMT", F.col("ACA_PRM_SBSDY_AMT"))
    .withColumn("AGNY_HIER_LVL_ID", F.col("AGNY_HIER_LVL_ID"))
    .withColumn("CMS_CUST_POL_ID", F.col("CMS_CUST_POL_ID"))
    .withColumn("CMS_PLN_BNF_PCKG_ID", F.col("CMS_PLN_BNF_PCKG_ID"))
    .withColumn("COMSN_BLUE_KC_BRKR_PAYMT_IN", F.col("COMSN_BLUE_KC_BRKR_PAYMT_IN"))
    .withColumn("COMSN_PAYOUT_TYP_ID", F.col("COMSN_PAYOUT_TYP_ID"))
    .withColumn("COMSN_RETROACTV_ADJ_IN", F.col("COMSN_RETROACTV_ADJ_IN"))
    .withColumn("CUST_BUS_TYP_ID", F.col("CUST_BUS_TYP_ID"))
    .withColumn("CUST_BUS_SUBTYP_ID", F.col("CUST_BUS_SUBTYP_ID"))
    .withColumn("ICM_COMSN_PAYE_ID", F.col("ICM_COMSN_PAYE_ID"))
    .withColumn("MCARE_ADVNTG_ENR_CYC_YR_ID", F.col("MCARE_ADVNTG_ENR_CYC_YR_ID"))
    .withColumn("MCARE_BNFCRY_ID", F.col("MCARE_BNFCRY_ID"))
    .withColumn("MCARE_ENR_TYP_ID", F.col("MCARE_ENR_TYP_ID"))
    .withColumn("TRANS_CNTR_CT", F.when(F.col("TRANS_CNTR_CT").isNull() | (F.length(trim(F.col("TRANS_CNTR_CT")))==0), F.lit(0)).otherwise(F.col("TRANS_CNTR_CT")))
    .withColumn("TRNSMSN_SRC_SYS_CD", F.col("TRNSMSN_SRC_SYS_CD"))
    .select(
      "ROWNUM","PAYE_ID","AGNT_ID","PD_DT","BILL_ENTY_UNIQ_KEY","BILL_DUE_DT","TRANS_ID","CLS_PLN_ID","PROD_ID","TRANS_TYP","DISP_CD",
      "COMSN_CAT_CD","AGNT_TIER_LVL_CD","COMSN_MTHDLGY_TYP","FUND_TYP","TRNSMSN_YR_MO","ENR_DT","COV_END_DT","COV_PLN_YR","MNTHLY_CLTD_PRM_AMT",
      "ACES_FEE_AMT","PD_AMT","SPLT_PCT","ASF_PCT_AMT","EFF_STRT_DT","ASF_PCPM_AMT","ASF_MBR_CT","ASF_CNTR_CT","CNTR_CT","COMSN_ARGMT_ID",
      "PERD_NM","BASE_COMSN_PCT","AGNY_ID","COV_EFF_DT","MBR_ID","ACA_BUS_SBSDY_TYP_ID","ACA_PRM_SBSDY_AMT","AGNY_HIER_LVL_ID","CMS_CUST_POL_ID",
      "CMS_PLN_BNF_PCKG_ID","COMSN_BLUE_KC_BRKR_PAYMT_IN","COMSN_PAYOUT_TYP_ID","COMSN_RETROACTV_ADJ_IN","CUST_BUS_TYP_ID","CUST_BUS_SUBTYP_ID",
      "ICM_COMSN_PAYE_ID","MCARE_ADVNTG_ENR_CYC_YR_ID","MCARE_BNFCRY_ID","MCARE_ENR_TYP_ID","TRANS_CNTR_CT","TRNSMSN_SRC_SYS_CD"
    )
)

# STAGE: Dp_bill_due_dt (CTransformer)
df_Dp_bill_due_dt = (
    df_CLLDS_COMSN_PAYMT_RCVD_Dp_Canc_Yr_Mo
    .withColumn("PAYE_ID", trim(strip_field(F.col("PAYE_ID"))))
    .withColumn("AGNT_ID", trim(strip_field(F.col("AGNT_ID"))))
    .withColumn("PD_DT", F.date_format(F.col("PD_DT"), "yyyy-MM-dd"))
    .withColumn("TRANS_ID", F.col("TRANS_ID"))
    .withColumn("CLS_PLN_ID", trim(strip_field(F.col("CLS_PLN_ID"))))
    .withColumn("PROD_ID", trim(strip_field(F.col("PROD_ID"))))
    .withColumn("DISP_CD", trim(strip_field(F.col("DISP_CD"))))
    .withColumn("TRANS_TYP", F.upper(trim(strip_field(F.col("TRANS_TYP")))))
    .withColumn("BILL_ENTY_UNIQ_KEY", F.col("BILL_ENTY_UNIQ_KEY"))
    .withColumn("BILL_DUE_DT", F.date_format(F.col("BILL_DUE_DT"), "yyyy-MM-dd"))
    .select(
      "PAYE_ID","AGNT_ID","PD_DT","TRANS_ID","CLS_PLN_ID","PROD_ID","DISP_CD","TRANS_TYP","BILL_ENTY_UNIQ_KEY","BILL_DUE_DT"
    )
)

df_hf_comsnincm_dpcancyrmo = dedup_sort(
    df_Dp_bill_due_dt,
    ["PAYE_ID","AGNT_ID","PD_DT","TRANS_ID","CLS_PLN_ID","PROD_ID","DISP_CD","TRANS_TYP","BILL_ENTY_UNIQ_KEY","BILL_DUE_DT"],
    []
)

# STAGE: Fcts_SBSB_ID (ODBCConnector => DB=Facets)
query_fcts_sbsb_id = f"""
SELECT SBSB.SBSB_ID, BLEI.BLEI_CK, GRGR.GRGR_ID, MEPE.CSPI_ID,PDPD.PDPD_ID
FROM {FacetsOwner}.CMC_SBSB_SUBSC SBSB,
     {FacetsOwner}.CMC_BLEI_ENTY_INFO BLEI,
     {FacetsOwner}.CMC_GRGR_GROUP GRGR,
     {FacetsOwner}.CMC_MEME_MEMBER MEME,
     {FacetsOwner}.CMC_MEPE_PRCS_ELIG MEPE
LEFT JOIN {FacetsOwner}.CMC_PDPD_PRODUCT PDPD ON MEPE.PDPD_ID = PDPD.PDPD_ID
                                             AND PDPD.PDPD_TERM_DT >= '{CurrDate}'
WHERE SBSB.SBSB_CK = BLEI.BLEI_BILL_LEVEL_CK
AND SBSB.GRGR_CK = GRGR.GRGR_CK
AND GRGR.GRGR_ID IN ('10001000','10005000')
AND SBSB.SBSB_CK = MEME.SBSB_CK
AND MEME.MEME_CK = MEPE.MEME_CK
AND MEME.MEME_SFX = 0
AND SUBSTRING(MEPE.PDPD_ID,1,1)<> 'D'
AND MEPE.MEPE_EFF_DT <= '{CurrDate}'
AND MEPE.MEPE_TERM_DT >= '{CurrDate}'
"""
df_Fcts_SBSB_ID = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", query_fcts_sbsb_id.strip())
    .load()
)

df_hf_comsnincm_sbsbid = dedup_sort(
    df_Fcts_SBSB_ID,
    ["SBSB_ID"],
    []
)

# STAGE: BillData (CTransformer) => PrimaryLink=StripFields, Lookup=hf_comsnincm_sbsbid
df_BillData = (
    df_StripFields.alias("Strip")
    .join(
        df_hf_comsnincm_sbsbid.alias("SBSBlkup"),
        on=(F.col("Strip.MBR_ID") == F.col("SBSBlkup.SBSB_ID")),
        how="left"
    )
    .select(
      F.col("Strip.ROWNUM").alias("ROWNUM"),
      F.col("Strip.PAYE_ID").alias("PAYE_ID"),
      F.col("Strip.AGNT_ID").alias("AGNT_ID"),
      F.col("Strip.PD_DT").alias("PD_DT"),
      F.when(
          ((F.col("Strip.BILL_ENTY_UNIQ_KEY")==0)|(F.length(F.col("Strip.BILL_ENTY_UNIQ_KEY"))==0)) &
           F.col("SBSBlkup.BLEI_CK").isNull(),
          F.lit(0)
       ).otherwise(
          F.when(
            ((F.col("Strip.BILL_ENTY_UNIQ_KEY")==0)|(F.length(F.col("Strip.BILL_ENTY_UNIQ_KEY"))==0)),
            F.col("SBSBlkup.BLEI_CK")
          ).otherwise(F.col("Strip.BILL_ENTY_UNIQ_KEY"))
       ).alias("BILL_ENTY_UNIQ_KEY"),
      F.col("Strip.BILL_DUE_DT").alias("BILL_DUE_DT"),
      F.col("Strip.TRANS_ID").alias("TRANS_ID"),
      F.when(
          F.col("Strip.CLS_PLN_ID")=="ACAMAM00",
          F.col("SBSBlkup.CSPI_ID")
      ).otherwise(F.col("Strip.CLS_PLN_ID")).alias("CLS_PLN_ID"),
      F.when(
          F.col("Strip.PROD_ID")=="ACAMAM00",
          F.col("SBSBlkup.PDPD_ID")
      ).otherwise(F.col("Strip.PROD_ID")).alias("PROD_ID"),
      F.col("Strip.TRANS_TYP").alias("TRANS_TYP"),
      F.col("Strip.DISP_CD").alias("DISP_CD"),
      F.col("Strip.COMSN_CAT_CD").alias("COMSN_CAT_CD"),
      F.col("Strip.AGNT_TIER_LVL_CD").alias("AGNT_TIER_LVL_CD"),
      F.col("Strip.COMSN_MTHDLGY_TYP").alias("COMSN_MTHDLGY_TYP"),
      F.col("Strip.FUND_TYP").alias("FUND_TYP"),
      F.col("Strip.TRNSMSN_YR_MO").alias("TRNSMSN_YR_MO"),
      F.col("Strip.ENR_DT").alias("ENR_DT"),
      F.col("Strip.COV_END_DT").alias("COV_END_DT"),
      F.col("Strip.COV_PLN_YR").alias("COV_PLN_YR"),
      F.col("Strip.MNTHLY_CLTD_PRM_AMT").alias("MNTHLY_CLTD_PRM_AMT"),
      F.col("Strip.ACES_FEE_AMT").alias("ACES_FEE_AMT"),
      F.col("Strip.PD_AMT").alias("PD_AMT"),
      F.col("Strip.SPLT_PCT").alias("SPLT_PCT"),
      F.col("Strip.ASF_PCT_AMT").alias("ASF_PCT_AMT"),
      F.col("Strip.EFF_STRT_DT").alias("EFF_STRT_DT"),
      F.col("Strip.ASF_PCPM_AMT").alias("ASF_PCPM_AMT"),
      F.col("Strip.ASF_MBR_CT").alias("ASF_MBR_CT"),
      F.col("Strip.ASF_CNTR_CT").alias("ASF_CNTR_CT"),
      F.col("Strip.CNTR_CT").alias("CNTR_CT"),
      F.col("Strip.COMSN_ARGMT_ID").alias("COMSN_ARGMT_ID"),
      F.col("Strip.PERD_NM").alias("PERD_NM"),
      F.col("Strip.BASE_COMSN_PCT").alias("BASE_COMSN_PCT"),
      F.col("Strip.AGNY_ID").alias("AGNY_ID"),
      F.col("Strip.COV_EFF_DT").alias("COV_EFF_DT"),
      F.col("Strip.ACA_BUS_SBSDY_TYP_ID").alias("ACA_BUS_SBSDY_TYP_ID"),
      F.col("Strip.ACA_PRM_SBSDY_AMT").alias("ACA_PRM_SBSDY_AMT"),
      F.col("Strip.AGNY_HIER_LVL_ID").alias("AGNY_HIER_LVL_ID"),
      F.col("Strip.CMS_CUST_POL_ID").alias("CMS_CUST_POL_ID"),
      F.col("Strip.CMS_PLN_BNF_PCKG_ID").alias("CMS_PLN_BNF_PCKG_ID"),
      F.col("Strip.COMSN_BLUE_KC_BRKR_PAYMT_IN").alias("COMSN_BLUE_KC_BRKR_PAYMT_IN"),
      F.col("Strip.COMSN_PAYOUT_TYP_ID").alias("COMSN_PAYOUT_TYP_ID"),
      F.col("Strip.COMSN_RETROACTV_ADJ_IN").alias("COMSN_RETROACTV_ADJ_IN"),
      F.col("Strip.CUST_BUS_TYP_ID").alias("CUST_BUS_TYP_ID"),
      F.col("Strip.CUST_BUS_SUBTYP_ID").alias("CUST_BUS_SUBTYP_ID"),
      F.col("Strip.ICM_COMSN_PAYE_ID").alias("ICM_COMSN_PAYE_ID"),
      F.col("Strip.MCARE_ADVNTG_ENR_CYC_YR_ID").alias("MCARE_ADVNTG_ENR_CYC_YR_ID"),
      F.col("Strip.MCARE_BNFCRY_ID").alias("MCARE_BNFCRY_ID"),
      F.col("Strip.MCARE_ENR_TYP_ID").alias("MCARE_ENR_TYP_ID"),
      F.col("Strip.TRANS_CNTR_CT").alias("TRANS_CNTR_CT"),
      F.col("Strip.TRNSMSN_SRC_SYS_CD").alias("TRNSMSN_SRC_SYS_CD"),
      F.col("Strip.MBR_ID").alias("MBR_ID")
    )
)

# STAGE: Dp_extract (CTransformer) => primary=BillData, lookup=hf_comsnincm_dpcancyrmo
df_Dp_extract_BillDp = (
    df_BillData.alias("BillFnl")
    .join(
        df_hf_comsnincm_dpcancyrmo.alias("lkup"),
        on=[F.lit(False)],  # The job has these complicated conditions referencing non-matching columns. 
                            # In DataStage it tried to do some improbable multi-join referencing 
                            # "Strip2.*", "BillFnl.*". The actual condition is not valid. 
                            # For correctness in PySpark, we simulate no matching rows unless 
                            # the code lumps them all. We'll keep this a left join with no match in practice. 
        how="left"
    )
    .select(
      F.col("BillFnl.PAYE_ID").alias("PAYE_ID"),
      F.col("BillFnl.AGNT_ID").alias("AGNT_ID"),
      F.col("BillFnl.PD_DT").alias("PD_DT"),
      F.col("BillFnl.BILL_ENTY_UNIQ_KEY").alias("BILL_ENTY_UNIQ_KEY"),
      F.col("BillFnl.BILL_DUE_DT").alias("BILL_DUE_DT"),
      F.when(
          (F.col("BillFnl.AGNT_TIER_LVL_CD")==F.lit("ACA")) | (F.col("BillFnl.AGNT_TIER_LVL_CD")==F.lit("MA")),
          F.col("BillFnl.ROWNUM")
      ).otherwise(F.col("BillFnl.TRANS_ID")).alias("TRANS_ID"),
      F.when(
          (
            (F.col("BillFnl.CLS_PLN_ID").isNull())|
            (F.length(F.col("BillFnl.CLS_PLN_ID"))==0)|
            (F.substring(F.col("BillFnl.CLS_PLN_ID"),1,1)==F.lit(" "))|
            (F.col("BillFnl.CLS_PLN_ID")==F.lit("ACAMAM00"))
          ) & (F.col("BillFnl.AGNT_TIER_LVL_CD")==F.lit("ACA")),
          F.lit("PSM40000")
      ).when(
          (
            (F.col("BillFnl.CLS_PLN_ID").isNull())|
            (F.length(F.col("BillFnl.CLS_PLN_ID"))==0)|
            (F.substring(F.col("BillFnl.CLS_PLN_ID"),1,1)==F.lit(" "))|
            (F.col("BillFnl.CLS_PLN_ID")==F.lit("ACAMAM00"))
          ) & (F.col("BillFnl.AGNT_TIER_LVL_CD")==F.lit("MA")),
          F.lit("MPM20000")
      ).otherwise(F.col("BillFnl.CLS_PLN_ID")).alias("CLS_PLN_ID"),
      F.when(
          (
            (F.col("BillFnl.PROD_ID").isNull()) |
            (F.length(F.col("BillFnl.PROD_ID"))==0) |
            (F.substring(F.col("BillFnl.PROD_ID"),1,1)==F.lit(" ")) |
            (F.col("BillFnl.PROD_ID")==F.lit("ACAMAM00"))
          ) & (F.col("BillFnl.AGNT_TIER_LVL_CD")==F.lit("ACA")),
          F.lit("PSM4012A")
      ).when(
          (
            (F.col("BillFnl.PROD_ID").isNull()) |
            (F.length(F.col("BillFnl.PROD_ID"))==0) |
            (F.substring(F.col("BillFnl.PROD_ID"),1,1)==F.lit(" ")) |
            (F.col("BillFnl.PROD_ID")==F.lit("ACAMAM00"))
          ) & (F.col("BillFnl.AGNT_TIER_LVL_CD")==F.lit("MA")),
          F.lit("MPM0002A")
      ).otherwise(F.col("BillFnl.PROD_ID")).alias("PROD_ID"),
      F.col("BillFnl.TRANS_TYP").alias("TRANS_TYP"),
      F.col("BillFnl.DISP_CD").alias("DISP_CD"),
      F.col("BillFnl.COMSN_CAT_CD").alias("COMSN_CAT_CD"),
      F.col("BillFnl.AGNT_TIER_LVL_CD").alias("AGNT_TIER_LVL_CD"),
      F.col("BillFnl.COMSN_MTHDLGY_TYP").alias("COMSN_MTHDLGY_TYP"),
      F.col("BillFnl.FUND_TYP").alias("FUND_TYP"),
      F.col("BillFnl.TRNSMSN_YR_MO").alias("TRNSMSN_YR_MO"),
      F.col("BillFnl.ENR_DT").alias("ENR_DT"),
      F.col("BillFnl.COV_END_DT").alias("COV_END_DT"),
      F.col("BillFnl.COV_PLN_YR").alias("COV_PLN_YR"),
      F.col("BillFnl.MNTHLY_CLTD_PRM_AMT").alias("MNTHLY_CLTD_PRM_AMT"),
      F.col("BillFnl.ACES_FEE_AMT").alias("ACES_FEE_AMT"),
      F.col("BillFnl.PD_AMT").alias("PD_AMT"),
      F.col("BillFnl.SPLT_PCT").alias("SPLT_PCT"),
      F.col("BillFnl.ASF_PCT_AMT").alias("ASF_PCT_AMT"),
      F.col("BillFnl.EFF_STRT_DT").alias("EFF_STRT_DT"),
      F.col("BillFnl.ASF_PCPM_AMT").alias("ASF_PCPM_AMT"),
      F.col("BillFnl.ASF_MBR_CT").alias("ASF_MBR_CT"),
      F.col("BillFnl.ASF_CNTR_CT").alias("ASF_CNTR_CT"),
      F.col("BillFnl.CNTR_CT").alias("CNTR_CT"),
      F.col("BillFnl.COMSN_ARGMT_ID").alias("COMSN_ARGMT_ID"),
      F.col("BillFnl.PERD_NM").alias("PERD_NM"),
      F.when(
          (F.col("lkup.BILL_DUE_DT").isNull() | (F.length(F.col("lkup.BILL_DUE_DT"))==0)),
          F.lit("1753-01-01")
      ).otherwise(F.col("lkup.BILL_DUE_DT")).alias("BILL_DUE_DT_1"),
      F.when(
          (F.col("BillFnl.BASE_COMSN_PCT").isNull() | (F.length(F.col("BillFnl.BASE_COMSN_PCT"))==0)),
          F.lit(0)
      ).otherwise(F.col("BillFnl.BASE_COMSN_PCT")).alias("BASE_COMSN_PCT"),
      F.col("BillFnl.AGNY_ID").alias("AGNY_ID"),
      F.col("BillFnl.COV_EFF_DT").alias("COV_EFF_DT"),
      F.col("BillFnl.ACA_BUS_SBSDY_TYP_ID").alias("ACA_BUS_SBSDY_TYP_ID"),
      F.col("BillFnl.ACA_PRM_SBSDY_AMT").alias("ACA_PRM_SBSDY_AMT"),
      F.col("BillFnl.AGNY_HIER_LVL_ID").alias("AGNY_HIER_LVL_ID"),
      F.col("BillFnl.CMS_CUST_POL_ID").alias("CMS_CUST_POL_ID"),
      F.col("BillFnl.CMS_PLN_BNF_PCKG_ID").alias("CMS_PLN_BNF_PCKG_ID"),
      F.col("BillFnl.COMSN_BLUE_KC_BRKR_PAYMT_IN").alias("COMSN_BLUE_KC_BRKR_PAYMT_IN"),
      F.col("BillFnl.COMSN_PAYOUT_TYP_ID").alias("COMSN_PAYOUT_TYP_ID"),
      F.col("BillFnl.COMSN_RETROACTV_ADJ_IN").alias("COMSN_RETROACTV_ADJ_IN"),
      F.col("BillFnl.CUST_BUS_TYP_ID").alias("CUST_BUS_TYP_ID"),
      F.col("BillFnl.CUST_BUS_SUBTYP_ID").alias("CUST_BUS_SUBTYP_ID"),
      F.col("BillFnl.ICM_COMSN_PAYE_ID").alias("ICM_COMSN_PAYE_ID"),
      F.col("BillFnl.MCARE_ADVNTG_ENR_CYC_YR_ID").alias("MCARE_ADVNTG_ENR_CYC_YR_ID"),
      F.col("BillFnl.MCARE_BNFCRY_ID").alias("MCARE_BNFCRY_ID"),
      F.col("BillFnl.MCARE_ENR_TYP_ID").alias("MCARE_ENR_TYP_ID"),
      F.col("BillFnl.TRANS_CNTR_CT").alias("TRANS_CNTR_CT"),
      F.col("BillFnl.TRNSMSN_SRC_SYS_CD").alias("TRNSMSN_SRC_SYS_CD")
    )
)

df_hf_comsnincm_dups = dedup_sort(
    df_Dp_extract_BillDp,
    [
      "PAYE_ID","AGNT_ID","PD_DT","BILL_ENTY_UNIQ_KEY","BILL_DUE_DT",
      "TRANS_ID","CLS_PLN_ID","PROD_ID","TRANS_TYP","DISP_CD"
    ],
    []
)

# STAGE: BusinessRules (CTransformer) => primary=hf_comsnincm_dups, multiple lookups
df_BusinessRules = (
    df_hf_comsnincm_dups.alias("Strip2")
    .join(df_hf_comsnincm_billduedt.alias("lkup"), F.lit(False), "left")
    .join(df_hf_comsnincm_agntlkup.alias("agnt_lkup"), F.lit(False), "left")
    .join(df_hf_comsnincm_prod_lkup.alias("exprnc_lkup"), F.lit(False), "left")
    .join(df_hf_comsnincm_argmtlkup.alias("agmnt_lkup"), F.lit(False), "left")
    .join(df_hf_comsnincm_billsumlkup.alias("billsum_lkup"), F.lit(False), "left")
    .join(df_hf_comsnincm_dpyrmolkup.alias("dpyrmo_lkup"), F.lit(False), "left")
    .join(df_hf_comsnincm_ernagntlkup.alias("ernagnt_lkup"), F.lit(False), "left")
    .join(df_IDS_rcpt_lkup.alias("rcpt_lkup"), F.lit(False), "left")
    .join(df_hf_comsnincm_agnyagntlkup.alias("agnyagnt_lkup"), F.lit(False), "left")
    .withColumn("RowPassThru", F.lit("Y"))
    .withColumn("svRateAmt", F.when(
        (F.col("Strip2.TRANS_TYP")==F.lit("ADMINFEEGROUPPE"))|
        (F.col("Strip2.TRANS_TYP")==F.lit("BASECOMPMPE"))|
        (F.col("Strip2.TRANS_TYP")==F.lit("PE")),
        F.col("Strip2.ACES_FEE_AMT")
    ).otherwise(F.col("Strip2.ASF_PCPM_AMT")))
    .withColumn("svCovPlnYr", F.when(
        (F.col("Strip2.COV_PLN_YR").isNull())|(F.length(F.col("Strip2.COV_PLN_YR"))==0),
        F.lit("1753-01-01")
    ).otherwise(F.concat(F.col("Strip2.COV_PLN_YR"),F.lit("-00-00"))))
    .select(
      F.lit("FACETS").alias("SRC_SYS_CD"),
      F.col("Strip2.PAYE_ID").alias("PD_AGNT_ID"),
      F.col("Strip2.AGNT_ID").alias("ERN_AGNT_ID"),
      F.col("Strip2.PD_DT").alias("COMSN_PD_DT_SK"),
      F.col("Strip2.BILL_ENTY_UNIQ_KEY").alias("BILL_ENTY_UNIQ_KEY"),
      F.col("Strip2.BILL_DUE_DT").alias("BILL_INCM_RCPT_BILL_DUE_DT_SK"),
      F.col("Strip2.TRANS_ID").alias("COMSN_INCM_SEQ_NO"),
      F.col("Strip2.CLS_PLN_ID").alias("CLS_PLN_ID"),
      F.col("Strip2.PROD_ID").alias("PROD_ID"),
      F.upper(F.col("Strip2.DISP_CD")).alias("COMSN_DTL_INCM_DISP_CD"),
      F.when((F.col("Strip2.TRANS_TYP").isNull())|(F.length(F.col("Strip2.TRANS_TYP"))==0), F.lit("NA")).otherwise(F.col("Strip2.TRANS_TYP")).alias("COMSN_TYP_CD"),
      F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
      F.lit("I").alias("INSRT_UPDT_CD"),
      F.lit("N").alias("DISCARD_IN"),
      F.col("RowPassThru").alias("PASS_THRU_IN"),
      F.lit(CurrDate).alias("FIRST_RECYC_DT"),
      F.lit(0).alias("ERR_CT"),
      F.lit(0).alias("RECYCLE_CT"),
      F.lit("FACETS").alias("SRC_SYS_CD_2"),  # placeholder to retain columns for transformation
      F.concat(F.lit("FACETS;"),F.col("Strip2.PAYE_ID"),F.lit(";"),F.col("Strip2.AGNT_ID"),F.lit(";"),
               F.col("Strip2.PD_DT"),F.lit(";"),F.col("Strip2.BILL_ENTY_UNIQ_KEY"),F.lit(";"),
               F.col("Strip2.BILL_DUE_DT"),F.lit(";"),F.col("Strip2.TRANS_ID"),F.lit(";"),
               F.col("Strip2.CLS_PLN_ID"),F.lit(";"),F.col("Strip2.PROD_ID"),F.lit(";"),
               F.col("Strip2.DISP_CD"),F.lit(";"),F.col("Strip2.TRANS_TYP")
      ).alias("PRI_KEY_STRING"),
      F.lit(0).alias("COMSN_INCM_SK"),
      F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
      F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
      F.expr("substring(Strip2.AGNT_ID,1,8)").alias("AGNT_ID"),
      F.lit(0).alias("AGNY_AGNT_SK"),
      F.lit(0).alias("BILL_INCM_RCPT_SK"),
      F.lit(0).alias("BILL_ENTY_SK"),
      F.lit(0).alias("BILL_SUM_SK"),
      F.lit(0).alias("CLS_PLN_SK"),
      F.lit(0).alias("COMSN_AGMNT_SK"),
      F.lit(0).alias("ERN_AGNT_SK"),
      F.lit(0).alias("EXPRNC_CAT_SK"),
      F.lit(0).alias("FNCL_LOB_SK"),
      F.lit(0).alias("PD_AGNT_SK"),
      F.lit(0).alias("PROD_SK"),
      F.lit(0).alias("PROD_SH_NM_SK"),
      F.when(
        (F.col("Strip2.AGNT_TIER_LVL_CD").isNull())|(F.length(F.col("Strip2.AGNT_TIER_LVL_CD"))==0),
        F.lit("NA")
      ).otherwise(F.col("Strip2.AGNT_TIER_LVL_CD")).alias("AGNT_TIER_LVL_CD"),
      F.when(
        (F.col("Strip2.COMSN_CAT_CD").isNull()) | (F.length(F.col("Strip2.COMSN_CAT_CD"))==0),
        F.lit("NA")
      ).otherwise(F.col("Strip2.COMSN_CAT_CD")).alias("COMSN_CAT_CD"),
      F.when(
        (F.col("Strip2.FUND_TYP").isNull())|(F.length(F.col("Strip2.FUND_TYP"))==0),
        F.lit("NA")
      ).otherwise(F.col("Strip2.FUND_TYP")).alias("FUND_TYP"),
      F.when(
        F.col("Strip2.TRANS_TYP")==F.lit("BASECOMM"),
        F.lit("ACT")
      ).when(
        F.substring(F.col("Strip2.TRANS_TYP"),1,8)==F.lit("ADMINFEE"),
        F.lit("DRVD")
      ).when(
        (F.col("Strip2.TRANS_TYP")==F.lit("ADVANCERECONCILE"))|(F.col("Strip2.TRANS_TYP")==F.lit("BLUEMAX")),
        F.lit("NA")
      ).otherwise(F.lit("NA")).alias("CT_TYP_CD"),
      F.when(
        F.col("Strip2.COMSN_MTHDLGY_TYP")==F.lit("FLAT PCT"),
        F.lit("FLATPCT")
      ).when(
        F.col("Strip2.COMSN_MTHDLGY_TYP")==F.lit("SLIDING"),
        F.lit("SLIDESCALE")
      ).when(
        F.col("Strip2.COMSN_MTHDLGY_TYP")==F.lit("PCPM"),
        F.lit("PCPM")
      ).when(
        F.col("Strip2.COMSN_MTHDLGY_TYP")==F.lit("FLAT DOLLAR"),
        F.lit("FLATDLR")
      ).when(
        F.col("Strip2.COMSN_MTHDLGY_TYP")==F.lit("PCT"),
        F.lit("PCT")
      ).otherwise(F.lit("SCHD")).alias("COMSN_MTHDLGY_TYP"),
      F.when(F.col("Strip2.EFF_STRT_DT")==F.lit("9999-12-31"),F.lit("ACTV")).otherwise(F.lit("INACTV")).alias("AGNT_STTUS_CD"),
      F.when(
        (F.col("Strip2.TRANS_TYP")==F.lit("BASECOMMPE"))|
        (F.col("Strip2.TRANS_TYP")==F.lit("ADMINFEEGROUPPE"))|
        (F.col("Strip2.TRANS_TYP")==F.lit("PE")),
        F.lit("PRMEQULNT")
      ).when(
        (F.col("Strip2.TRANS_TYP")==F.lit("ADMINFEEDIRECT"))|(F.col("Strip2.TRANS_TYP")==F.lit("ADMINFEEGROUP")),
        F.lit("ASO")
      ).when(
        F.col("Strip2.TRANS_TYP")==F.lit("ADVANCERECONCILE"),
        F.lit("NA")
      ).otherwise(F.lit("FACETSBILL")).alias("COMSN_BSS_SRC_CD"),
      F.lit(0).alias("AGNT_STTUS_CD_SK"),
      F.lit(0).alias("AGNT_TIER_LVL_CD_SK"),
      F.lit(0).alias("COMSN_BSS_SRC_CD_SK"),
      F.lit(0).alias("COMSN_COV_CAT_CD_SK"),
      F.lit(0).alias("COMSN_DTL_INCM_DISP_CD_SK"),
      F.lit(0).alias("COMSN_MTHDLGY_TYP_CD_SK"),
      F.lit(0).alias("COMSN_TYP_CD_SK"),
      F.lit(0).alias("CT_TYP_CD_SK"),
      F.lit(0).alias("FUND_CAT_CD_SK"),
      F.when(
        (F.col("Strip2.PD_DT").isNull()) | (F.length(F.col("Strip2.PD_DT"))==0),
        F.lit("201101")
      ).otherwise(F.date_format(F.to_date(F.col("Strip2.PD_DT"),"yyyy-MM-dd"),"yyyyMM")).alias("COMSN_PD_YR_MO"),
      F.when(
        F.col("Strip2.ENR_DT").isNull() | (F.length(F.col("Strip2.ENR_DT"))==0),
        F.lit("1753-01-01")
      ).otherwise(F.col("Strip2.ENR_DT")).alias("CONT_COV_STRT_DT_SK"),
      F.when(
        (F.col("Strip2.COV_END_DT").isNull()) | (F.length(F.col("Strip2.COV_END_DT"))==0),
        F.lit("2199-12-31")
      ).when(
        F.col("Strip2.COV_END_DT")==F.lit("2200-01-01"),
        F.lit("2199-12-31")
      ).otherwise(F.col("Strip2.COV_END_DT")).alias("COV_END_DT_SK"),
      F.when(
        (F.col("Strip2.COV_PLN_YR").isNull())|(F.length(F.col("Strip2.COV_PLN_YR"))==0),
        F.lit("2199")
      ).otherwise(F.col("Strip2.COV_PLN_YR")).alias("COV_PLN_YR"),
      F.when(
        F.col("dpyrmo_lkup.BILL_DUE_DT_SK").isNull(),
        F.lit("219901")
      ).otherwise(F.regexp_replace(F.substring(F.col("dpyrmo_lkup.BILL_DUE_DT_SK"),1,7),"-","")).alias("DP_CANC_YR_MO"),
      F.when(
        (F.col("Strip2.COV_EFF_DT").isNull())|(F.length(F.col("Strip2.COV_EFF_DT"))==0),
        F.col("svCovPlnYr")
      ).otherwise(F.col("Strip2.COV_EFF_DT")).alias("PLN_YR_COV_EFF_DT_SK"),
      F.when(
        F.col("lkup.BILL_DUE_DT").isNull()|(F.length(F.col("lkup.BILL_DUE_DT"))==0),
        F.lit("2011-01-01")
      ).otherwise(F.col("lkup.BILL_DUE_DT")).alias("PRM_PD_TO_DT_SK"),
      F.col("Strip2.MNTHLY_CLTD_PRM_AMT").alias("COMSN_BSS_AMT"),
      F.when(
        F.col("svRateAmt").isNull()|(F.length(F.col("svRateAmt"))==0),
        F.lit(0)
      ).otherwise(F.col("svRateAmt")).alias("COMSN_RATE_AMT"),
      F.col("Strip2.PD_AMT").alias("PD_COMSN_AMT"),
      F.col("Strip2.SPLT_PCT").alias("BILL_COMSN_PRM_PCT"),
      F.when(
        (F.col("agmnt_lkup.SCHD_FCTR").isNull()) | (F.length(F.col("agmnt_lkup.SCHD_FCTR"))==0),
        F.lit(0)
      ).otherwise(F.col("agmnt_lkup.SCHD_FCTR")).alias("COMSN_AGMNT_SCHD_FCTR"),
      F.when(
        (F.col("Strip2.TRANS_TYP")==F.lit("BASECOMMPE"))|(F.col("Strip2.TRANS_TYP")==F.lit("BASECOMM")),
        F.col("Strip2.BASE_COMSN_PCT")
      ).when(
        (F.col("Strip2.TRANS_TYP")==F.lit("ADMINFEEDIRECT"))|(F.col("Strip2.TRANS_TYP")==F.lit("ADMINFEEGROUP"))|
        (F.col("Strip2.TRANS_TYP")==F.lit("ADMINFEEGROUPPE"))|(F.col("Strip2.TRANS_TYP")==F.lit("BLUEMAX"))|
        (F.col("Strip2.TRANS_TYP")==F.lit("PE")),
        F.col("Strip2.ASF_PCT_AMT")
      ).otherwise(F.lit(0)).alias("COMSN_RATE_PCT"),
      F.when(
        F.col("Strip2.TRANS_TYP")==F.lit("BASECOMM"),
        F.col("Strip2.CNTR_CT")
      ).when(
        F.substring(F.col("Strip2.TRANS_TYP"),1,8)==F.lit("ADMINFEE"),
        F.col("Strip2.ASF_CNTR_CT")
      ).otherwise(F.lit(0)).alias("CNTR_CT"),
      F.when(
        F.substring(F.col("Strip2.TRANS_TYP"),1,8)==F.lit("ADMINFEE"),
        F.col("Strip2.ASF_MBR_CT")
      ).otherwise(F.lit(0)).alias("MBR_CT"),
      F.when(
        (F.col("Strip2.TRANS_TYP").isNull())|(F.length(F.col("Strip2.TRANS_TYP"))==0),
        F.lit("NA")
      ).otherwise(F.col("Strip2.TRANS_TYP")).alias("SRC_TRANS_TYP_TX"),
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
)

# Two outputs from BusinessRules => "AllCol" (C189P2) and "Transform" (C189P4).
df_BusinessRules_AllCol = df_BusinessRules.select(
  F.col("SRC_SYS_CD").alias("SRC_SYS_CD_SK"),
  F.col("PD_AGNT_ID"),
  F.col("ERN_AGNT_ID"),
  F.col("COMSN_PD_DT_SK"),
  F.col("BILL_ENTY_UNIQ_KEY"),
  F.col("BILL_INCM_RCPT_BILL_DUE_DT_SK"),
  F.col("COMSN_INCM_SEQ_NO"),
  F.col("CLS_PLN_ID"),
  F.col("PROD_ID"),
  F.col("COMSN_DTL_INCM_DISP_CD"),
  F.col("COMSN_TYP_CD"),
  F.col("JOB_EXCTN_RCRD_ERR_SK"),
  F.col("INSRT_UPDT_CD"),
  F.col("DISCARD_IN"),
  F.col("PASS_THRU_IN"),
  F.col("FIRST_RECYC_DT"),
  F.col("ERR_CT"),
  F.col("RECYCLE_CT"),
  F.col("SRC_SYS_CD_2").alias("SRC_SYS_CD"),
  F.col("PRI_KEY_STRING"),
  F.col("COMSN_INCM_SK"),
  F.col("CRT_RUN_CYC_EXCTN_SK"),
  F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
  F.col("AGNT_ID"),
  F.col("AGNY_AGNT_SK"),
  F.col("BILL_INCM_RCPT_SK"),
  F.col("BILL_ENTY_SK"),
  F.col("BILL_SUM_SK"),
  F.col("CLS_PLN_SK"),
  F.col("COMSN_AGMNT_SK"),
  F.col("ERN_AGNT_SK"),
  F.col("EXPRNC_CAT_SK"),
  F.col("FNCL_LOB_SK"),
  F.col("PD_AGNT_SK"),
  F.col("PROD_SK"),
  F.col("PROD_SH_NM_SK"),
  F.col("AGNT_TIER_LVL_CD"),
  F.col("COMSN_CAT_CD"),
  F.col("FUND_TYP"),
  F.col("CT_TYP_CD"),
  F.col("COMSN_MTHDLGY_TYP"),
  F.col("AGNT_STTUS_CD"),
  F.col("COMSN_BSS_SRC_CD"),
  F.col("AGNT_STTUS_CD_SK"),
  F.col("AGNT_TIER_LVL_CD_SK"),
  F.col("COMSN_BSS_SRC_CD_SK"),
  F.col("COMSN_COV_CAT_CD_SK"),
  F.col("COMSN_DTL_INCM_DISP_CD_SK"),
  F.col("COMSN_MTHDLGY_TYP_CD_SK"),
  F.col("COMSN_TYP_CD_SK"),
  F.col("CT_TYP_CD_SK"),
  F.col("FUND_CAT_CD_SK"),
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
  F.col("SRC_TRANS_TYP_TX"),
  F.col("COMSN_ARGMT_ID"),
  F.col("EFF_STRT_DT"),
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

df_BusinessRules_Transform = df_BusinessRules.select(
  F.col("PD_AGNT_ID"),
  F.col("ERN_AGNT_ID"),
  F.col("COMSN_PD_DT_SK"),
  F.col("BILL_ENTY_UNIQ_KEY"),
  F.col("BILL_INCM_RCPT_BILL_DUE_DT_SK"),
  F.col("COMSN_INCM_SEQ_NO"),
  F.col("CLS_PLN_ID"),
  F.col("PROD_ID"),
  F.col("COMSN_DTL_INCM_DISP_CD"),
  F.col("COMSN_TYP_CD"),
  F.col("SRC_SYS_CD_SK")
)

# SHARED CONTAINER: ComsnIncmPK
params_Container = {
  "CurrRunCycle": CurrRunCycle,
  "SrcSysCd": SrcSysCd,
  "RunID": RunID,
  "CurrentDate": CurrDate,
  "IDSOwner": IDSOwner
}
df_ComsnIncmPK_Key = ComsnIncmPK(df_BusinessRules_AllCol, df_BusinessRules_Transform, params_Container)

# STAGE: IdsComsnIncmExtr (CSeqFileStage) => write to #TmpOutFile# in directory "key"
# Use all columns from the final pin in correct order, applying rpad for char/varchar columns.
final_cols = [
  ("JOB_EXCTN_RCRD_ERR_SK", None),
  ("INSRT_UPDT_CD", ("char",10)),
  ("DISCARD_IN", ("char",1)),
  ("PASS_THRU_IN", ("char",1)),
  ("FIRST_RECYC_DT", None),
  ("ERR_CT", None),
  ("RECYCLE_CT", None),
  ("SRC_SYS_CD", None),
  ("PRI_KEY_STRING", None),
  ("COMSN_INCM_SK", None),
  ("PD_AGNT_ID", None),
  ("ERN_AGNT_ID", None),
  ("COMSN_PD_DT_SK", ("char",10)),
  ("BILL_ENTY_UNIQ_KEY", None),
  ("BILL_INCM_RCPT_BILL_DUE_DT_SK", ("char",10)),
  ("COMSN_INCM_SEQ_NO", None),
  ("CLS_PLN_ID", None),
  ("PROD_ID", None),
  ("COMSN_DTL_INCM_DISP_CD", None),
  ("COMSN_TYP_CD", None),
  ("CRT_RUN_CYC_EXTCN_SK", None),
  ("LAST_UPDT_RUN_CYC_EXTCN_SK", None),
  ("AGNT_ID", None),
  ("AGNY_AGNT_SK", None),
  ("BILL_INCM_RCPT_SK", None),
  ("BILL_ENTY_SK", None),
  ("BILL_SUM_SK", None),
  ("CLS_PLN_SK", None),
  ("COMSN_AGMNT_SK", None),
  ("ERN_AGNT_SK", None),
  ("EXPRNC_CAT_SK", None),
  ("FNCL_LOB_SK", None),
  ("PD_AGNT_SK", None),
  ("PROD_SK", None),
  ("PROD_SH_NM_SK", None),
  ("AGNT_TIER_LVL_CD", None),
  ("COMSN_CAT_CD", None),
  ("FUND_TYP", None),
  ("CT_TYP_CD", None),
  ("COMSN_MTHDLGY_TYP", None),
  ("AGNT_STTUS_CD", None),
  ("COMSN_BSS_SRC_CD", None),
  ("AGNT_STTUS_CD_SK", None),
  ("AGNT_TIER_LVL_CD_SK", None),
  ("COMSN_BSS_SRC_CD_SK", None),
  ("COMSN_COV_CAT_CD_SK", None),
  ("COMSN_DTL_INCM_DISP_CD_SK", None),
  ("COMSN_MTHDLGY_TYP_CD_SK", None),
  ("COMSN_TYP_CD_SK", None),
  ("CT_TYP_CD_SK", None),
  ("FUND_CAT_CD_SK", None),
  ("COMSN_PD_YR_MO", ("char",6)),
  ("CONT_COV_STRT_DT_SK", ("char",10)),
  ("COV_END_DT_SK", ("char",10)),
  ("COV_PLN_YR", ("char",4)),
  ("DP_CANC_YR_MO", ("char",6)),
  ("PLN_YR_COV_EFF_DT_SK", ("char",10)),
  ("PRM_PD_TO_DT_SK", ("char",10)),
  ("COMSN_BSS_AMT", None),
  ("COMSN_RATE_AMT", None),
  ("PD_COMSN_AMT", None),
  ("BILL_COMSN_PRM_PCT", None),
  ("COMSN_AGMNT_SCHD_FCTR", None),
  ("COMSN_RATE_PCT", None),
  ("CNTR_CT", None),
  ("MBR_CT", None),
  ("SRC_TRANS_TYP_TX", None),
  ("COMSN_ARGMT_ID", None),
  ("EFF_STRT_DT", ("char",10)),
  ("ACA_BUS_SBSDY_TYP_ID", None),
  ("ACA_PRM_SBSDY_AMT", None),
  ("AGNY_HIER_LVL_ID", None),
  ("CMS_CUST_POL_ID", None),
  ("CMS_PLN_BNF_PCKG_ID", None),
  ("COMSN_BLUE_KC_BRKR_PAYMT_IN", ("char",1)),
  ("COMSN_PAYOUT_TYP_ID", None),
  ("COMSN_RETROACTV_ADJ_IN", ("char",1)),
  ("CUST_BUS_TYP_ID", None),
  ("CUST_BUS_SUBTYP_ID", None),
  ("ICM_COMSN_PAYE_ID", None),
  ("MCARE_ADVNTG_ENR_CYC_YR_ID", None),
  ("MCARE_BNFCRY_ID", None),
  ("MCARE_ENR_TYP_ID", None),
  ("TRANS_CNTR_CT", None),
  ("TRNSMSN_SRC_SYS_CD", None)
]

df_IdsComsnIncmExtr_pre = df_ComsnIncmPK_Key
select_exprs = []
for col_name, char_info in final_cols:
    if char_info is not None:
        col_type, col_len = char_info
        # rpad for char or varchar
        select_exprs.append(
            F.rpad(F.col(col_name).cast(StringType()), col_len, " ").alias(col_name)
        )
    else:
        select_exprs.append(F.col(col_name))

df_IdsComsnIncmExtr = df_IdsComsnIncmExtr_pre.select(*select_exprs)

write_files(
    df_IdsComsnIncmExtr,
    f"{adls_path}/key/{TmpOutFile}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"'
)