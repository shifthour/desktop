# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:   Job called from IdsClaimCntl
# MAGIC 
# MAGIC Developer                                     Date                 Project/Altiris #                Change Description                                                                                                                     Development Project      Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                                                                                                           ---------------------------------      ---------------------------------    -------------------------   
# MAGIC Sri Nannpaneni               2019-11-25            6131                      Initial Programming                                                                                                                                          EnterpriseDev2              Kalyan Neelam             2019-11-26
# MAGIC Rekha Radhakrishna      2020-02-05            6131                      Mapped new fields SELL_INCNTV_FEE_AMT,BUY_INCNTV_FEE_AMT                                                    EnterpriseDev2              Kalyan Neelam             2020-02-06
# MAGIC                                                                                                    ,SELL_OTHR_PAYOR_AMT,SELL_OTHR_AMT and SPREAD_INCNTV_ FEE_AMT
# MAGIC Peter Gichiri                      2020-02-25           6131                     Using the field DRUG_CLM_RX_SUBMT_QTY from  CLM_F table in  the fields 
# MAGIC                                                                                                    CALC_BUY_INGR_CST_AMT and CALC_SELL_INGR_CST_AMT calculation                                             EnterpriseDev2              Jaideep Mankala         02/27/2020
# MAGIC Rekha Radhakrishna      2020-02-29           6131                      Included join to DRUG_CLM_PRICE_F table to map exisitng value of RECON_IN                                         EnterpriseDev2              Jaideep Mankala         03/04/2020
# MAGIC                                                                                                   during an Update.
# MAGIC Geetanjali Rajendran    2021-06-03        PBM PhaseII          Perform code mapping lookup to populate DRUG_TYPE_CD, DRUG_TYP_NM  and FRMLRY_D lookup         EnterpriseDev2	  Abhiram Dasarathy      2021-06-24
# MAGIC                                                                                                                 to populate FRMLRY_ID.Mapped new fields FRMLRY_SK, DRUG_TYP_CD_SK and TIER_ID
# MAGIC 
# MAGIC Rekha Radhakrishna      2021-07-04    PBM Phase II  Carryover      Set the Nullability on FRMLRY_SK fields correctly during data read and data flow                                    EnterpriseDev2            Hugh Sisson                 2021-07-04
# MAGIC 
# MAGIC Rekha Radhakrishna    2022-02-10    PBM Phase II Carryover     Added FRMLRY_D join                                                                                                                                 EnterpriseDev2              Raja Gummadi            2021-02-17
# MAGIC 
# MAGIC Vamsi Aripaka               2023-11-08       US 600308                      Added PLN_DRUG_STTUS_CD_SK to the source query, joined with CD_MPPNG and retrived                 EnterpriseDevB            Jeyaprasanna              2024-01-03
# MAGIC                                                                                                       PLN_DRUG_STTUS_CD, PLN_DRUG_STTUS_NM and PLN_DRUG_STTUS_CD_SK columns. 
# MAGIC                                                                                                       mapped till target.
# MAGIC                                                                                                        
# MAGIC Ashok kumar B               2024-02-08       US 608682                     Added DRUG_PLN_TYP_ID to the source query,                                                                                       EnterpriseDev2              Jeyaprasanna             2024-03-14
# MAGIC 
# MAGIC Kshema H K                  2024-11-12        US 625960                Updated  Existing Derivation for Amount fields i.e CALC_SELL_INGR_CST_AMT,                                           EnterpriseDev2              Jeyaprasanna             2024-11-13
# MAGIC                                                                                                   CALC_BUY_INGR_CST_AMT,CALC_GUAR_RBT_AMT,CALC_SELL_DISPNS_FEE_AMT,
# MAGIC                                                                                                   CALC_BUY_DISPNS_FEE_AMT to Stage Variable and Added logic to have a negative value
# MAGIC                                                                                                    for Reversal claims in IdsEdwDrugClmPriceFExtr.xfm_BusinessLogic Stage.

# MAGIC Job Name: IdsEdwDrugClmPriceFExtr
# MAGIC 
# MAGIC Description: Drug Claims Price data is extracted from IDS and loaded into the  DRUG_CLM_PRICE_F Table
# MAGIC Called from: IdsEdwDrugClmPriceFCntl
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


# Obtain all parameter values (including secret names for IDS and EDW)
idsowner = get_widget_value('IDSOwner','')
edwowner = get_widget_value('EDWOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
edw_secret_name = get_widget_value('edw_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')

# Read from db2_DRUG_CLM_PRICE_F (EDW)
jdbc_url, jdbc_props = get_db_config(edw_secret_name)
extract_query_db2_DRUG_CLM_PRICE_F = f"SELECT CLM_SK, RECON_IN FROM {edwowner}.DRUG_CLM_PRICE_F WHERE SRC_SYS_CD = 'OPTUMRX'"
df_db2_DRUG_CLM_PRICE_F = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_DRUG_CLM_PRICE_F)
    .load()
)

# Read from db2_FRMLRY_D (EDW)
jdbc_url, jdbc_props = get_db_config(edw_secret_name)
extract_query_db2_FRMLRY_D = f"SELECT FRMLRY_SK, FRMLRY_ID FROM {edwowner}.FRMLRY_D"
df_db2_FRMLRY_D = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_FRMLRY_D)
    .load()
)

# Read from db2_DRUG_CLM_PRICE (IDS)
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query_db2_DRUG_CLM_PRICE = f"""SELECT 
DRUG_CLM_SK
,DCP.SRC_SYS_CD_SK
,DCP.CLM_ID
,CRT_RUN_CYC_EXCTN_SK
,DCP.LAST_UPDT_RUN_CYC_EXCTN_SK
,BUY_CST_SRC_CD_SK
,GNRC_OVRD_CD_SK
,PDX_NTWK_QLFR_CD_SK
,FRMLRY_PROTOCOL_IN
,RECON_IN
,SPEC_PGM_IN
,FINL_PLN_EFF_DT_SK
,ORIG_PD_TRANS_SUBMT_DT_SK
,PRORTD_DISPNS_QTY
,SUBMT_DISPNS_METRIC_QTY
,AVG_WHLSL_PRICE_UNIT_CST_AMT
,WHLSL_ACQSTN_CST_UNIT_CST_AMT
,BUY_DISPNS_FEE_AMT
,BUY_INGR_CST_AMT
,BUY_RATE_PCT
,CST_TYP_UNIT_CST_AMT
,SELL_CST_TYP_UNIT_CST_AMT
,SELL_RATE_PCT
,SPREAD_DISPNS_FEE_AMT
,SPREAD_INGR_CST_AMT
,BUY_CST_TYP_ID
,BUY_PRICE_TYP_ID
,CST_TYP_ID
,DRUG_TYP_ID
,FINL_PLN_ID
,GNRC_PROD_ID
,SELL_PRICE_TYP_ID
,SPEC_PGM_ID
,BUY_SLS_TAX_AMT
,BUY_TOT_DUE_AMT
,INVC_TOT_DUE_AMT
,SPREAD_SLS_TAX_AMT
,BUY_TOT_OTHR_AMT
,BUY_INCNTV_FEE_AMT
,SELL_INCNTV_FEE_AMT
,SPREAD_INCNTV_FEE_AMT
,SELL_OTHR_PAYER_AMT
,SELL_OTHR_AMT
,FRMLRY_SK
,TIER_ID
,DRUG_TYP_CD_SK
,PLN_DRUG_STTUS_CD_SK
,DRUG_PLN_TYP_ID
FROM {idsowner}.DRUG_CLM_PRICE DCP,
     {idsowner}.W_EDW_ETL_DRVR DRVR
WHERE DCP.CLM_ID = DRVR.CLM_ID
AND DCP.SRC_SYS_CD_SK = DRVR.SRC_SYS_CD_SK
"""
df_db2_DRUG_CLM_PRICE = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_DRUG_CLM_PRICE)
    .load()
)

# Read from db2_PBM_CNTR_D (EDW)
jdbc_url, jdbc_props = get_db_config(edw_secret_name)
extract_query_db2_PBM_CNTR_D = f"""SELECT DISTINCT
CF.CLM_SK,
CF.DRUG_CLM_RX_SUBMT_QTY,
PC.DISPNS_FEE_AMT,
PC.PLN_GUAR_DSCNT_PCT,
coalesce(PCG.GRP_GUAR_DSCNT_PCT  ,PCGN.GRP_GUAR_DSCNT_PCT , 1) as GRP_GUAR_DSCNT_PCT,
coalesce(PCG.GRP_GUAR_DISPNS_FEE_AMT,PCGN.GRP_GUAR_DISPNS_FEE_AMT) as GRP_GUAR_DISPNS_FEE_AMT,
RBT.PLN_GUAR_RBT_AMT,
DC.DRUG_TYP_ID as DRUG_TYP_CD,
PC.PDX_NTWK_QLFR_CD
FROM {edwowner}.CLM_F2 CF2
INNER JOIN {edwowner}.CLM_F CF
  ON CF.CLM_SK = CF2.CLM_SK
INNER JOIN {edwowner}.DRUG_CLM_PRICE_F DC
  ON CF.CLM_SK = DC.CLM_SK
LEFT JOIN {edwowner}.FRMLRY_D F
  ON DC.FRMLRY_SK = F.FRMLRY_SK
LEFT JOIN {edwowner}.PBM_CNTR_D PC
  ON CF2.DRUG_CLM_PDX_NTWK_ID = PC.PDX_NTWK_ID
  AND COALESCE(CASE WHEN DC.PDX_NTWK_QLFR_NM = 'RETAIL' AND ABS(CF.DRUG_CLM_RX_ALW_DAYS_SUPL_QTY) > 34 THEN 'RETAIL90'
                    WHEN DC.PDX_NTWK_QLFR_NM = 'RETAIL' AND ABS(CF.DRUG_CLM_RX_ALW_DAYS_SUPL_QTY) <= 34 THEN 'RETAIL30'
                    ELSE DC.PDX_NTWK_QLFR_NM END, 'UNK') = PC.RX_CHAN_NM
  and ABS(CF.DRUG_CLM_RX_ALW_DAYS_SUPL_QTY) BETWEEN PC.MIN_DAYS_SUPL_QTY and PC.MAX_DAYS_SUPL_QTY
  and CF2.DRUG_CLM_FILL_DT_SK BETWEEN PC.PBM_CNTR_EFF_DT and PC.PBM_CNTR_TERM_DT
  AND DC.DRUG_TYP_ID = PC.DRUG_TYP_CD
  AND PC.SRC_SYS_CD = 'OPTUMRX'
LEFT JOIN {edwowner}.PBM_CNTR_GRP_GUAR_D PCG
  ON CF.GRP_ID = PCG.GRP_ID
  AND PCG.SRC_SYS_CD = 'OPTUMRX'
  AND PC.PDX_NTWK_ID = PCG.PDX_NTWK_ID
  AND PC.PBM_CNTR_EFF_DT  = PCG.PBM_CNTR_EFF_DT
  AND ABS(CF.DRUG_CLM_RX_ALW_DAYS_SUPL_QTY) BETWEEN PCG.MIN_DAYS_SUPL_QTY and PCG.MAX_DAYS_SUPL_QTY
  AND PC.DRUG_TYP_CD = PCG.DRUG_TYP_CD
LEFT JOIN {edwowner}.PBM_CNTR_GRP_GUAR_D PCGN
  ON PC.PDX_NTWK_ID =PCGN.PDX_NTWK_ID
  AND PCGN.SRC_SYS_CD = 'OPTUMRX'
  AND PC.PBM_CNTR_EFF_DT  = PCGN.PBM_CNTR_EFF_DT
  AND ABS(CF.DRUG_CLM_RX_ALW_DAYS_SUPL_QTY) BETWEEN PCGN.MIN_DAYS_SUPL_QTY and PCGN.MAX_DAYS_SUPL_QTY
  AND PC.DRUG_TYP_CD = PCGN.DRUG_TYP_CD
  AND PCGN.GRP_ID = 'CUSTOM'
LEFT JOIN {edwowner}.PBM_RBT_D RBT
  ON DC.DRUG_TYP_ID = RBT.DRUG_TYP_CD
  AND COALESCE(CASE WHEN DC.PDX_NTWK_QLFR_NM = 'RETAIL' AND ABS(CF.DRUG_CLM_RX_ALW_DAYS_SUPL_QTY) > 34 THEN '90 DAY AT RETAIL'
                    WHEN DC.PDX_NTWK_QLFR_NM = 'RETAIL' AND ABS(CF.DRUG_CLM_RX_ALW_DAYS_SUPL_QTY) <= 34 THEN 'RETAIL'
                    ELSE DC.PDX_NTWK_QLFR_NM END, 'UNK') = RBT.RX_CHAN_NM
  AND F.FRMLRY_RBT_GUAR_CAT_TX = RBT.FRMLRY_RBT_GUAR_CAT_TX
  AND CF2.DRUG_CLM_FILL_DT_SK BETWEEN RBT.PBM_RBT_EFF_DT and RBT.PBM_RBT_TERM_DT
WHERE CF.SRC_SYS_CD = 'OPTUMRX' AND DC.PDX_NTWK_QLFR_NM != 'UNK'
"""
df_db2_PBM_CNTR_D = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_PBM_CNTR_D)
    .load()
)

# Remove_Duplicates_288 (PxRemDup) on df_db2_PBM_CNTR_D
df_remove_duplicates_288_raw = dedup_sort(
    df_db2_PBM_CNTR_D,
    partition_cols=["CLM_SK","DRUG_TYP_CD"],
    sort_cols=[("CLM_SK","A"),("DRUG_TYP_CD","A")]
)
df_remove_duplicates_288 = df_remove_duplicates_288_raw.select(
    F.col("CLM_SK").alias("CLM_SK"),
    F.col("DRUG_CLM_RX_SUBMT_QTY").alias("DRUG_CLM_RX_SUBMT_QTY"),
    F.col("DISPNS_FEE_AMT").alias("DISPNS_FEE_AMT"),
    F.col("PLN_GUAR_DSCNT_PCT").alias("PLN_GUAR_DSCNT_PCT"),
    F.col("GRP_GUAR_DSCNT_PCT").alias("GRP_GUAR_DSCNT_PCT"),
    F.col("GRP_GUAR_DISPNS_FEE_AMT").alias("GRP_GUAR_DISPNS_FEE_AMT"),
    F.col("PLN_GUAR_RBT_AMT").alias("PLN_GUAR_RBT_AMT"),
    F.col("DRUG_TYP_CD").alias("DRUG_TYP_CD"),
    F.col("PDX_NTWK_QLFR_CD").alias("PDX_NTWK_QLFR_CD")
)

# Read from db2_CD_MPPNG (IDS)
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query_db2_CD_MPPNG = f"SELECT CD.CD_MPPNG_SK, COALESCE(CD.TRGT_CD, 'UNK') TRGT_CD, CD.TRGT_CD_NM FROM {idsowner}.CD_MPPNG CD"
df_db2_CD_MPPNG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CD_MPPNG)
    .load()
)

# cpy_MultiStreams
df_cpy_MultiStreams = df_db2_CD_MPPNG.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_cpy_MultiStreams_lnk_SrcSysCdSk_In = df_cpy_MultiStreams
df_cpy_MultiStreams_Lnk_BuyCstSrcCdSk_In = df_cpy_MultiStreams
df_cpy_MultiStreams_Lnk_PdxNtwkQlfrSk_In = df_cpy_MultiStreams
df_cpy_MultiStreams_Lnk_GnrcOvrdCd_In = df_cpy_MultiStreams
df_cpy_MultiStreams_Lnk_Drugtypcdsk = df_cpy_MultiStreams
df_cpy_MultiStreams_Lnk_PlnDrugSttusCdSk = df_cpy_MultiStreams

# lkp_Codes (PxLookup) - multiple left joins
df_lkp_Codes_joined = (
    df_db2_DRUG_CLM_PRICE.alias("lnk_Grp_In")
    .join(
        df_cpy_MultiStreams_lnk_SrcSysCdSk_In.alias("lnk_SrcSysCdSk_In"),
        F.col("lnk_Grp_In.SRC_SYS_CD_SK") == F.col("lnk_SrcSysCdSk_In.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_cpy_MultiStreams_Lnk_BuyCstSrcCdSk_In.alias("Lnk_BuyCstSrcCdSk_In"),
        F.col("lnk_Grp_In.BUY_CST_SRC_CD_SK") == F.col("Lnk_BuyCstSrcCdSk_In.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_cpy_MultiStreams_Lnk_PdxNtwkQlfrSk_In.alias("Lnk_PdxNtwkQlfrSk_In"),
        F.col("lnk_Grp_In.PDX_NTWK_QLFR_CD_SK") == F.col("Lnk_PdxNtwkQlfrSk_In.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_cpy_MultiStreams_Lnk_GnrcOvrdCd_In.alias("Lnk_GnrcOvrdCd_In"),
        F.col("lnk_Grp_In.GNRC_OVRD_CD_SK") == F.col("Lnk_GnrcOvrdCd_In.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_cpy_MultiStreams_Lnk_Drugtypcdsk.alias("Lnk_Drugtypcdsk"),
        F.col("lnk_Grp_In.DRUG_TYP_CD_SK") == F.col("Lnk_Drugtypcdsk.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_cpy_MultiStreams_Lnk_PlnDrugSttusCdSk.alias("Lnk_PlnDrugSttusCdSk"),
        F.col("lnk_Grp_In.PLN_DRUG_STTUS_CD_SK") == F.col("Lnk_PlnDrugSttusCdSk.CD_MPPNG_SK"),
        "left"
    )
)

df_lkp_Codes = df_lkp_Codes_joined.select(
    F.col("lnk_Grp_In.CLM_ID").alias("CLM_ID"),
    F.col("lnk_SrcSysCdSk_In.TRGT_CD").alias("SRC_SYS_CD"),
    F.col("lnk_Grp_In.DRUG_CLM_SK").alias("DRUG_CLM_SK"),
    F.col("Lnk_BuyCstSrcCdSk_In.TRGT_CD").alias("BUY_CST_SRC_CD"),
    F.col("Lnk_BuyCstSrcCdSk_In.TRGT_CD_NM").alias("BUY_CST_SRC_NM"),
    F.col("Lnk_GnrcOvrdCd_In.TRGT_CD").alias("GNRC_OVRD_CD"),
    F.col("Lnk_GnrcOvrdCd_In.TRGT_CD_NM").alias("GNRC_OVRD_NM"),
    F.col("Lnk_PdxNtwkQlfrSk_In.TRGT_CD").alias("PDX_NTWK_QLFR_CD"),
    F.col("Lnk_PdxNtwkQlfrSk_In.TRGT_CD_NM").alias("PDX_NTWK_QLFR_NM"),
    F.col("lnk_Grp_In.FRMLRY_PROTOCOL_IN").alias("FRMLRY_PROTOCOL_IN"),
    F.col("lnk_Grp_In.RECON_IN").alias("RECON_IN"),
    F.col("lnk_Grp_In.SPEC_PGM_IN").alias("SPEC_PGM_IN"),
    F.col("lnk_Grp_In.FINL_PLN_EFF_DT_SK").alias("FINL_PLN_EFF_DT_SK"),
    F.col("lnk_Grp_In.ORIG_PD_TRANS_SUBMT_DT_SK").alias("ORIG_PD_TRANS_SUBMT_DT_SK"),
    F.col("lnk_Grp_In.PRORTD_DISPNS_QTY").alias("PRORTD_DISPNS_QTY"),
    F.col("lnk_Grp_In.SUBMT_DISPNS_METRIC_QTY").alias("SUBMT_DISPNS_METRIC_QTY"),
    F.col("lnk_Grp_In.AVG_WHLSL_PRICE_UNIT_CST_AMT").alias("AVG_WHLSL_PRICE_UNIT_CST_AMT"),
    F.col("lnk_Grp_In.WHLSL_ACQSTN_CST_UNIT_CST_AMT").alias("WHLSL_ACQSTN_CST_UNIT_CST_AMT"),
    F.col("lnk_Grp_In.CST_TYP_UNIT_CST_AMT").alias("CST_TYP_UNIT_CST_AMT"),
    F.col("lnk_Grp_In.BUY_DISPNS_FEE_AMT").alias("BUY_DISPNS_FEE_AMT"),
    F.col("lnk_Grp_In.BUY_INGR_CST_AMT").alias("BUY_INGR_CST_AMT"),
    F.col("lnk_Grp_In.BUY_RATE_PCT").alias("BUY_RATE_PCT"),
    F.col("lnk_Grp_In.SELL_CST_TYP_UNIT_CST_AMT").alias("SELL_CST_TYP_UNIT_CST_AMT"),
    F.col("lnk_Grp_In.SELL_RATE_PCT").alias("SELL_RATE_PCT"),
    F.col("lnk_Grp_In.SPREAD_DISPNS_FEE_AMT").alias("SPREAD_DISPNS_FEE_AMT"),
    F.col("lnk_Grp_In.SPREAD_INGR_CST_AMT").alias("SPREAD_INGR_CST_AMT"),
    F.col("lnk_Grp_In.BUY_CST_TYP_ID").alias("BUY_CST_TYP_ID"),
    F.col("lnk_Grp_In.BUY_PRICE_TYP_ID").alias("BUY_PRICE_TYP_ID"),
    F.col("lnk_Grp_In.CST_TYP_ID").alias("CST_TYP_ID"),
    F.col("lnk_Grp_In.DRUG_TYP_ID").alias("DRUG_TYP_ID"),
    F.col("lnk_Grp_In.FINL_PLN_ID").alias("FINL_PLN_ID"),
    F.col("lnk_Grp_In.GNRC_PROD_ID").alias("GNRC_PROD_ID"),
    F.col("lnk_Grp_In.SELL_PRICE_TYP_ID").alias("SELL_PRICE_TYP_ID"),
    F.col("lnk_Grp_In.SPEC_PGM_ID").alias("SPEC_PGM_ID"),
    F.col("lnk_Grp_In.BUY_SLS_TAX_AMT").alias("BUY_SLS_TAX_AMT"),
    F.col("lnk_Grp_In.BUY_TOT_DUE_AMT").alias("BUY_TOT_DUE_AMT"),
    F.col("lnk_Grp_In.BUY_TOT_OTHR_AMT").alias("BUY_TOT_OTHR_AMT"),
    F.col("lnk_Grp_In.INVC_TOT_DUE_AMT").alias("INVC_TOT_DUE_AMT"),
    F.col("lnk_Grp_In.SPREAD_SLS_TAX_AMT").alias("SPREAD_SLS_TAX_AMT"),
    F.col("lnk_Grp_In.BUY_INCNTV_FEE_AMT").alias("BUY_INCNTV_FEE_AMT"),
    F.col("lnk_Grp_In.SELL_INCNTV_FEE_AMT").alias("SELL_INCNTV_FEE_AMT"),
    F.col("lnk_Grp_In.SPREAD_INCNTV_FEE_AMT").alias("SPREAD_INCNTV_FEE_AMT"),
    F.col("lnk_Grp_In.SELL_OTHR_PAYER_AMT").alias("SELL_OTHR_PAYER_AMT"),
    F.col("lnk_Grp_In.SELL_OTHR_AMT").alias("SELL_OTHR_AMT"),
    F.col("lnk_Grp_In.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_Grp_In.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_Grp_In.BUY_CST_SRC_CD_SK").alias("BUY_CST_SRC_CD_SK"),
    F.col("lnk_Grp_In.GNRC_OVRD_CD_SK").alias("GNRC_OVRD_CD_SK"),
    F.col("lnk_Grp_In.PDX_NTWK_QLFR_CD_SK").alias("PDX_NTWK_QLFR_CD_SK"),
    F.col("lnk_Grp_In.FRMLRY_SK").alias("FRMLRY_SK"),
    F.col("lnk_Grp_In.TIER_ID").alias("TIER_ID"),
    F.col("Lnk_Drugtypcdsk.TRGT_CD").alias("DRUG_TYP_CD"),
    F.col("Lnk_Drugtypcdsk.TRGT_CD_NM").alias("DRUG_TYP_NM"),
    F.col("lnk_Grp_In.DRUG_TYP_CD_SK").alias("DRUG_TYP_CD_SK"),
    F.col("Lnk_PlnDrugSttusCdSk.TRGT_CD").alias("PLN_DRUG_STTUS_CD"),
    F.col("Lnk_PlnDrugSttusCdSk.TRGT_CD_NM").alias("PLN_DRUG_STTUS_NM"),
    F.col("lnk_Grp_In.PLN_DRUG_STTUS_CD_SK").alias("PLN_DRUG_STTUS_CD_SK"),
    F.col("lnk_Grp_In.DRUG_PLN_TYP_ID").alias("DRUG_PLN_TYP_ID")
)

# Lkp_config (PxLookup) - left join with df_remove_duplicates_288
df_lkp_config_joined = (
    df_lkp_Codes.alias("lnk_Codes_Extract_In")
    .join(
        df_remove_duplicates_288.alias("lnk_ConfigData_InLkp"),
        [
            F.col("lnk_Codes_Extract_In.DRUG_CLM_SK") == F.col("lnk_ConfigData_InLkp.CLM_SK"),
            F.col("lnk_Codes_Extract_In.DRUG_TYP_ID") == F.col("lnk_ConfigData_InLkp.DRUG_TYP_CD")
        ],
        "left"
    )
)

df_lkp_config = df_lkp_config_joined.select(
    F.col("lnk_Codes_Extract_In.CLM_ID").alias("CLM_ID"),
    F.col("lnk_Codes_Extract_In.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_Codes_Extract_In.DRUG_CLM_SK").alias("DRUG_CLM_SK"),
    F.col("lnk_Codes_Extract_In.BUY_CST_SRC_CD").alias("BUY_CST_SRC_CD"),
    F.col("lnk_Codes_Extract_In.BUY_CST_SRC_NM").alias("BUY_CST_SRC_NM"),
    F.col("lnk_Codes_Extract_In.GNRC_OVRD_CD").alias("GNRC_OVRD_CD"),
    F.col("lnk_Codes_Extract_In.GNRC_OVRD_NM").alias("GNRC_OVRD_NM"),
    F.col("lnk_Codes_Extract_In.PDX_NTWK_QLFR_CD").alias("PDX_NTWK_QLFR_CD"),
    F.col("lnk_Codes_Extract_In.PDX_NTWK_QLFR_NM").alias("PDX_NTWK_QLFR_NM"),
    F.col("lnk_Codes_Extract_In.FRMLRY_PROTOCOL_IN").alias("FRMLRY_PROTOCOL_IN"),
    F.col("lnk_Codes_Extract_In.RECON_IN").alias("RECON_IN"),
    F.col("lnk_Codes_Extract_In.SPEC_PGM_IN").alias("SPEC_PGM_IN"),
    F.col("lnk_Codes_Extract_In.FINL_PLN_EFF_DT_SK").alias("FINL_PLN_EFF_DT_SK"),
    F.col("lnk_Codes_Extract_In.ORIG_PD_TRANS_SUBMT_DT_SK").alias("ORIG_PD_TRANS_SUBMT_DT_SK"),
    F.col("lnk_Codes_Extract_In.PRORTD_DISPNS_QTY").alias("PRORTD_DISPNS_QTY"),
    F.col("lnk_Codes_Extract_In.SUBMT_DISPNS_METRIC_QTY").alias("SUBMT_DISPNS_METRIC_QTY"),
    F.col("lnk_Codes_Extract_In.AVG_WHLSL_PRICE_UNIT_CST_AMT").alias("AVG_WHLSL_PRICE_UNIT_CST_AMT"),
    F.col("lnk_Codes_Extract_In.WHLSL_ACQSTN_CST_UNIT_CST_AMT").alias("WHLSL_ACQSTN_CST_UNIT_CST_AMT"),
    F.col("lnk_Codes_Extract_In.CST_TYP_UNIT_CST_AMT").alias("CST_TYP_UNIT_CST_AMT"),
    F.col("lnk_Codes_Extract_In.BUY_DISPNS_FEE_AMT").alias("BUY_DISPNS_FEE_AMT"),
    F.col("lnk_Codes_Extract_In.BUY_INGR_CST_AMT").alias("BUY_INGR_CST_AMT"),
    F.col("lnk_Codes_Extract_In.BUY_RATE_PCT").alias("BUY_RATE_PCT"),
    F.col("lnk_Codes_Extract_In.SELL_CST_TYP_UNIT_CST_AMT").alias("SELL_CST_TYP_UNIT_CST_AMT"),
    F.col("lnk_Codes_Extract_In.SELL_RATE_PCT").alias("SELL_RATE_PCT"),
    F.col("lnk_Codes_Extract_In.SPREAD_DISPNS_FEE_AMT").alias("SPREAD_DISPNS_FEE_AMT"),
    F.col("lnk_Codes_Extract_In.SPREAD_INGR_CST_AMT").alias("SPREAD_INGR_CST_AMT"),
    F.col("lnk_Codes_Extract_In.BUY_CST_TYP_ID").alias("BUY_CST_TYP_ID"),
    F.col("lnk_Codes_Extract_In.BUY_PRICE_TYP_ID").alias("BUY_PRICE_TYP_ID"),
    F.col("lnk_Codes_Extract_In.CST_TYP_ID").alias("CST_TYP_ID"),
    F.col("lnk_Codes_Extract_In.DRUG_TYP_ID").alias("DRUG_TYP_ID"),
    F.col("lnk_Codes_Extract_In.FINL_PLN_ID").alias("FINL_PLN_ID"),
    F.col("lnk_Codes_Extract_In.GNRC_PROD_ID").alias("GNRC_PROD_ID"),
    F.col("lnk_Codes_Extract_In.SELL_PRICE_TYP_ID").alias("SELL_PRICE_TYP_ID"),
    F.col("lnk_Codes_Extract_In.SPEC_PGM_ID").alias("SPEC_PGM_ID"),
    F.col("lnk_Codes_Extract_In.BUY_SLS_TAX_AMT").alias("BUY_SLS_TAX_AMT"),
    F.col("lnk_Codes_Extract_In.BUY_TOT_DUE_AMT").alias("BUY_TOT_DUE_AMT"),
    F.col("lnk_Codes_Extract_In.BUY_TOT_OTHR_AMT").alias("BUY_TOT_OTHR_AMT"),
    F.col("lnk_Codes_Extract_In.INVC_TOT_DUE_AMT").alias("INVC_TOT_DUE_AMT"),
    F.col("lnk_Codes_Extract_In.SPREAD_SLS_TAX_AMT").alias("SPREAD_SLS_TAX_AMT"),
    F.col("lnk_Codes_Extract_In.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_Codes_Extract_In.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_Codes_Extract_In.BUY_CST_SRC_CD_SK").alias("BUY_CST_SRC_CD_SK"),
    F.col("lnk_Codes_Extract_In.GNRC_OVRD_CD_SK").alias("GNRC_OVRD_CD_SK"),
    F.col("lnk_Codes_Extract_In.PDX_NTWK_QLFR_CD_SK").alias("PDX_NTWK_QLFR_CD_SK"),
    F.col("lnk_ConfigData_InLkp.DISPNS_FEE_AMT").alias("DISPNS_FEE_AMT"),
    F.col("lnk_ConfigData_InLkp.PLN_GUAR_DSCNT_PCT").alias("PLN_GUAR_DSCNT_PCT"),
    F.col("lnk_ConfigData_InLkp.GRP_GUAR_DSCNT_PCT").alias("GRP_GUAR_DSCNT_PCT"),
    F.col("lnk_ConfigData_InLkp.GRP_GUAR_DISPNS_FEE_AMT").alias("GRP_GUAR_DISPNS_FEE_AMT"),
    F.col("lnk_ConfigData_InLkp.PLN_GUAR_RBT_AMT").alias("PLN_GUAR_RBT_AMT"),
    F.col("lnk_Codes_Extract_In.BUY_INCNTV_FEE_AMT").alias("BUY_INCNTV_FEE_AMT"),
    F.col("lnk_Codes_Extract_In.SELL_INCNTV_FEE_AMT").alias("SELL_INCNTV_FEE_AMT"),
    F.col("lnk_Codes_Extract_In.SPREAD_INCNTV_FEE_AMT").alias("SPREAD_INCNTV_FEE_AMT"),
    F.col("lnk_Codes_Extract_In.SELL_OTHR_PAYER_AMT").alias("SELL_OTHR_PAYER_AMT"),
    F.col("lnk_Codes_Extract_In.SELL_OTHR_AMT").alias("SELL_OTHR_AMT"),
    F.col("lnk_ConfigData_InLkp.DRUG_CLM_RX_SUBMT_QTY").alias("DRUG_CLM_RX_SUBMT_QTY"),
    F.col("lnk_Codes_Extract_In.FRMLRY_SK").alias("FRMLRY_SK"),
    F.col("lnk_Codes_Extract_In.TIER_ID").alias("TIER_ID"),
    F.col("lnk_Codes_Extract_In.DRUG_TYP_CD").alias("DRUG_TYP_CD"),
    F.col("lnk_Codes_Extract_In.DRUG_TYP_NM").alias("DRUG_TYP_NM"),
    F.col("lnk_Codes_Extract_In.DRUG_TYP_CD_SK").alias("DRUG_TYP_CD_SK"),
    F.col("lnk_Codes_Extract_In.PLN_DRUG_STTUS_CD").alias("PLN_DRUG_STTUS_CD"),
    F.col("lnk_Codes_Extract_In.PLN_DRUG_STTUS_NM").alias("PLN_DRUG_STTUS_NM"),
    F.col("lnk_Codes_Extract_In.PLN_DRUG_STTUS_CD_SK").alias("PLN_DRUG_STTUS_CD_SK"),
    F.col("lnk_Codes_Extract_In.DRUG_PLN_TYP_ID").alias("DRUG_PLN_TYP_ID")
)

# Lkp_frmlrysk (PxLookup) - left join with db2_FRMLRY_D
df_Lkp_frmlrysk_joined = (
    df_lkp_config.alias("lnk_Extract_In1")
    .join(
        df_db2_FRMLRY_D.alias("lnk_FRMLRY_D_In"),
        F.col("lnk_Extract_In1.FRMLRY_SK") == F.col("lnk_FRMLRY_D_In.FRMLRY_SK"),
        "left"
    )
)

df_Lkp_frmlrysk = df_Lkp_frmlrysk_joined.select(
    F.col("lnk_Extract_In1.CLM_ID").alias("CLM_ID"),
    F.col("lnk_Extract_In1.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_Extract_In1.DRUG_CLM_SK").alias("DRUG_CLM_SK"),
    F.col("lnk_Extract_In1.BUY_CST_SRC_CD").alias("BUY_CST_SRC_CD"),
    F.col("lnk_Extract_In1.BUY_CST_SRC_NM").alias("BUY_CST_SRC_NM"),
    F.col("lnk_Extract_In1.GNRC_OVRD_CD").alias("GNRC_OVRD_CD"),
    F.col("lnk_Extract_In1.GNRC_OVRD_NM").alias("GNRC_OVRD_NM"),
    F.col("lnk_Extract_In1.PDX_NTWK_QLFR_CD").alias("PDX_NTWK_QLFR_CD"),
    F.col("lnk_Extract_In1.PDX_NTWK_QLFR_NM").alias("PDX_NTWK_QLFR_NM"),
    F.col("lnk_Extract_In1.FRMLRY_PROTOCOL_IN").alias("FRMLRY_PROTOCOL_IN"),
    F.col("lnk_Extract_In1.RECON_IN").alias("RECON_IN"),
    F.col("lnk_Extract_In1.SPEC_PGM_IN").alias("SPEC_PGM_IN"),
    F.col("lnk_Extract_In1.FINL_PLN_EFF_DT_SK").alias("FINL_PLN_EFF_DT_SK"),
    F.col("lnk_Extract_In1.ORIG_PD_TRANS_SUBMT_DT_SK").alias("ORIG_PD_TRANS_SUBMT_DT_SK"),
    F.col("lnk_Extract_In1.PRORTD_DISPNS_QTY").alias("PRORTD_DISPNS_QTY"),
    F.col("lnk_Extract_In1.SUBMT_DISPNS_METRIC_QTY").alias("SUBMT_DISPNS_METRIC_QTY"),
    F.col("lnk_Extract_In1.AVG_WHLSL_PRICE_UNIT_CST_AMT").alias("AVG_WHLSL_PRICE_UNIT_CST_AMT"),
    F.col("lnk_Extract_In1.WHLSL_ACQSTN_CST_UNIT_CST_AMT").alias("WHLSL_ACQSTN_CST_UNIT_CST_AMT"),
    F.col("lnk_Extract_In1.CST_TYP_UNIT_CST_AMT").alias("CST_TYP_UNIT_CST_AMT"),
    F.col("lnk_Extract_In1.BUY_DISPNS_FEE_AMT").alias("BUY_DISPNS_FEE_AMT"),
    F.col("lnk_Extract_In1.BUY_INGR_CST_AMT").alias("BUY_INGR_CST_AMT"),
    F.col("lnk_Extract_In1.BUY_RATE_PCT").alias("BUY_RATE_PCT"),
    F.col("lnk_Extract_In1.SELL_CST_TYP_UNIT_CST_AMT").alias("SELL_CST_TYP_UNIT_CST_AMT"),
    F.col("lnk_Extract_In1.SELL_RATE_PCT").alias("SELL_RATE_PCT"),
    F.col("lnk_Extract_In1.SPREAD_DISPNS_FEE_AMT").alias("SPREAD_DISPNS_FEE_AMT"),
    F.col("lnk_Extract_In1.SPREAD_INGR_CST_AMT").alias("SPREAD_INGR_CST_AMT"),
    F.col("lnk_Extract_In1.BUY_CST_TYP_ID").alias("BUY_CST_TYP_ID"),
    F.col("lnk_Extract_In1.BUY_PRICE_TYP_ID").alias("BUY_PRICE_TYP_ID"),
    F.col("lnk_Extract_In1.CST_TYP_ID").alias("CST_TYP_ID"),
    F.col("lnk_Extract_In1.DRUG_TYP_ID").alias("DRUG_TYP_ID"),
    F.col("lnk_Extract_In1.FINL_PLN_ID").alias("FINL_PLN_ID"),
    F.col("lnk_Extract_In1.GNRC_PROD_ID").alias("GNRC_PROD_ID"),
    F.col("lnk_Extract_In1.SELL_PRICE_TYP_ID").alias("SELL_PRICE_TYP_ID"),
    F.col("lnk_Extract_In1.SPEC_PGM_ID").alias("SPEC_PGM_ID"),
    F.col("lnk_Extract_In1.BUY_SLS_TAX_AMT").alias("BUY_SLS_TAX_AMT"),
    F.col("lnk_Extract_In1.BUY_TOT_DUE_AMT").alias("BUY_TOT_DUE_AMT"),
    F.col("lnk_Extract_In1.BUY_TOT_OTHR_AMT").alias("BUY_TOT_OTHR_AMT"),
    F.col("lnk_Extract_In1.INVC_TOT_DUE_AMT").alias("INVC_TOT_DUE_AMT"),
    F.col("lnk_Extract_In1.SPREAD_SLS_TAX_AMT").alias("SPREAD_SLS_TAX_AMT"),
    F.col("lnk_Extract_In1.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_Extract_In1.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_Extract_In1.BUY_CST_SRC_CD_SK").alias("BUY_CST_SRC_CD_SK"),
    F.col("lnk_Extract_In1.GNRC_OVRD_CD_SK").alias("GNRC_OVRD_CD_SK"),
    F.col("lnk_Extract_In1.PDX_NTWK_QLFR_CD_SK").alias("PDX_NTWK_QLFR_CD_SK"),
    F.col("lnk_Extract_In1.DISPNS_FEE_AMT").alias("DISPNS_FEE_AMT"),
    F.col("lnk_Extract_In1.PLN_GUAR_DSCNT_PCT").alias("PLN_GUAR_DSCNT_PCT"),
    F.col("lnk_Extract_In1.GRP_GUAR_DSCNT_PCT").alias("GRP_GUAR_DSCNT_PCT"),
    F.col("lnk_Extract_In1.GRP_GUAR_DISPNS_FEE_AMT").alias("GRP_GUAR_DISPNS_FEE_AMT"),
    F.col("lnk_Extract_In1.PLN_GUAR_RBT_AMT").alias("PLN_GUAR_RBT_AMT"),
    F.col("lnk_Extract_In1.BUY_INCNTV_FEE_AMT").alias("BUY_INCNTV_FEE_AMT"),
    F.col("lnk_Extract_In1.SELL_INCNTV_FEE_AMT").alias("SELL_INCNTV_FEE_AMT"),
    F.col("lnk_Extract_In1.SPREAD_INCNTV_FEE_AMT").alias("SPREAD_INCNTV_FEE_AMT"),
    F.col("lnk_Extract_In1.SELL_OTHR_PAYER_AMT").alias("SELL_OTHR_PAYER_AMT"),
    F.col("lnk_Extract_In1.SELL_OTHR_AMT").alias("SELL_OTHR_AMT"),
    F.col("lnk_Extract_In1.DRUG_CLM_RX_SUBMT_QTY").alias("DRUG_CLM_RX_SUBMT_QTY"),
    F.col("lnk_Extract_In1.FRMLRY_SK").alias("FRMLRY_SK"),
    F.col("lnk_FRMLRY_D_In.FRMLRY_ID").alias("FRMLRY_ID"),
    F.col("lnk_Extract_In1.TIER_ID").alias("TIER_ID"),
    F.col("lnk_Extract_In1.DRUG_TYP_CD").alias("DRUG_TYP_CD"),
    F.col("lnk_Extract_In1.DRUG_TYP_NM").alias("DRUG_TYP_NM"),
    F.col("lnk_Extract_In1.DRUG_TYP_CD_SK").alias("DRUG_TYP_CD_SK"),
    F.col("lnk_Extract_In1.PLN_DRUG_STTUS_CD").alias("PLN_DRUG_STTUS_CD"),
    F.col("lnk_Extract_In1.PLN_DRUG_STTUS_NM").alias("PLN_DRUG_STTUS_NM"),
    F.col("lnk_Extract_In1.PLN_DRUG_STTUS_CD_SK").alias("PLN_DRUG_STTUS_CD_SK"),
    F.col("lnk_Extract_In1.DRUG_PLN_TYP_ID").alias("DRUG_PLN_TYP_ID")
)

# xfm_BusinessLogic (CTransformerStage) - define stage variables and final derived columns
df_xfm_BusinessLogic_vars = (
    df_Lkp_frmlrysk
    .withColumn("svDisc", F.lit(1.0) - F.when(F.col("GRP_GUAR_DSCNT_PCT").isNotNull(), F.col("GRP_GUAR_DSCNT_PCT")).otherwise(F.lit(0.0)))
    .withColumn("svCalcBuyDispnsFeeAmt", F.when(F.col("DISPNS_FEE_AMT").isNotNull(), F.col("DISPNS_FEE_AMT")).otherwise(F.lit(0.0)))
    .withColumn("svCalcBuyIngrCstAmt",
        (F.when(F.col("AVG_WHLSL_PRICE_UNIT_CST_AMT").isNotNull(), F.col("AVG_WHLSL_PRICE_UNIT_CST_AMT")).otherwise(F.lit(0.0)))
        * (F.lit(1.0) - F.when(F.col("PLN_GUAR_DSCNT_PCT").isNotNull(), F.col("PLN_GUAR_DSCNT_PCT")).otherwise(F.lit(0.0)))
        * F.when(F.col("DRUG_CLM_RX_SUBMT_QTY").isNotNull(), F.col("DRUG_CLM_RX_SUBMT_QTY")).otherwise(F.lit(0.0)))
    .withColumn("svCalcGuarRbtAmt", F.when(F.col("PLN_GUAR_RBT_AMT").isNotNull(), F.col("PLN_GUAR_RBT_AMT")).otherwise(F.lit(0.0)))
    .withColumn("svCalcSellDispnsFeeAmt", F.when(F.col("GRP_GUAR_DISPNS_FEE_AMT").isNotNull(), F.col("GRP_GUAR_DISPNS_FEE_AMT")).otherwise(F.lit(0.0)))
    .withColumn("svCalcSellIngrCstAmt",
        (F.when(F.col("AVG_WHLSL_PRICE_UNIT_CST_AMT").isNotNull(), F.col("AVG_WHLSL_PRICE_UNIT_CST_AMT")).otherwise(F.lit(0.0)))
        * F.col("svDisc")
        * F.when(F.col("DRUG_CLM_RX_SUBMT_QTY").isNotNull(), F.col("DRUG_CLM_RX_SUBMT_QTY")).otherwise(F.lit(0.0)))
)

df_xfm_BusinessLogic = df_xfm_BusinessLogic_vars.select(
    F.col("DRUG_CLM_SK").alias("CLM_SK"),
    F.col("CLM_ID"),
    F.when(
        F.col("SRC_SYS_CD").isNotNull(),
        F.col("SRC_SYS_CD")
    ).otherwise(F.lit("")).alias("SRC_SYS_CD"),
    F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("DRUG_CLM_SK").alias("DRUG_CLM_SK"),
    F.when(
        F.col("BUY_CST_SRC_CD").isNotNull(),
        F.col("BUY_CST_SRC_CD")
    ).otherwise(F.lit("")).alias("BUY_CST_SRC_CD"),
    F.when(
        F.col("BUY_CST_SRC_NM").isNotNull(),
        F.col("BUY_CST_SRC_NM")
    ).otherwise(F.lit("")).alias("BUY_CST_SRC_NM"),
    F.when(
        F.col("GNRC_OVRD_CD").isNotNull(),
        F.col("GNRC_OVRD_CD")
    ).otherwise(F.lit("")).alias("GNRC_OVRD_CD"),
    F.when(
        F.col("GNRC_OVRD_NM").isNotNull(),
        F.col("GNRC_OVRD_NM")
    ).otherwise(F.lit("")).alias("GNRC_OVRD_NM"),
    F.when(
        F.col("PDX_NTWK_QLFR_CD").isNotNull(),
        F.col("PDX_NTWK_QLFR_CD")
    ).otherwise(F.lit("")).alias("PDX_NTWK_QLFR_CD"),
    F.when(
        F.col("PDX_NTWK_QLFR_NM").isNotNull(),
        F.col("PDX_NTWK_QLFR_NM")
    ).otherwise(F.lit("")).alias("PDX_NTWK_QLFR_NM"),
    F.col("FRMLRY_PROTOCOL_IN").alias("FRMLRY_PROTOCOL_IN"),
    F.col("RECON_IN").alias("RECON_IN"),
    F.col("SPEC_PGM_IN").alias("SPEC_PGM_IN"),
    F.col("FINL_PLN_EFF_DT_SK").alias("FINL_PLN_EFF_DT_SK"),
    F.col("ORIG_PD_TRANS_SUBMT_DT_SK").alias("ORIG_PD_TRANS_SUBMT_DT_SK"),
    F.col("PRORTD_DISPNS_QTY").alias("PRORTD_DISPNS_QTY"),
    F.col("SUBMT_DISPNS_METRIC_QTY").alias("SUBMT_DISPNS_METRIC_QTY"),
    F.col("AVG_WHLSL_PRICE_UNIT_CST_AMT").alias("AVG_WHLSL_PRICE_UNIT_CST_AMT"),
    F.col("WHLSL_ACQSTN_CST_UNIT_CST_AMT").alias("WHLSL_ACQSTN_CST_UNIT_CST_AMT"),
    F.col("CST_TYP_UNIT_CST_AMT").alias("CST_TYP_UNIT_CST_AMT"),
    F.col("BUY_DISPNS_FEE_AMT").alias("BUY_DISPNS_FEE_AMT"),
    F.col("BUY_INGR_CST_AMT").alias("BUY_INGR_CST_AMT"),
    F.col("BUY_RATE_PCT").alias("BUY_RATE_PCT"),
    F.col("BUY_SLS_TAX_AMT").alias("BUY_SLS_TAX_AMT"),
    F.col("BUY_TOT_DUE_AMT").alias("BUY_TOT_DUE_AMT"),
    F.col("BUY_TOT_OTHR_AMT").alias("BUY_TOT_OTHR_AMT"),
    F.col("INVC_TOT_DUE_AMT").alias("INVC_TOT_DUE_AMT"),
    F.col("SELL_CST_TYP_UNIT_CST_AMT").alias("SELL_CST_TYP_UNIT_CST_AMT"),
    F.col("SELL_RATE_PCT").alias("SELL_RATE_PCT"),
    F.when(
        F.substring(trim(F.col("CLM_ID")), F.length(trim(F.col("CLM_ID"))), 1).eqNullSafe("R")
        & (F.col("svCalcBuyDispnsFeeAmt") > 0.0),
        F.col("svCalcBuyDispnsFeeAmt") * -1
    ).otherwise(F.col("svCalcBuyDispnsFeeAmt")).alias("CALC_BUY_DISPNS_FEE_AMT"),
    F.when(
        F.substring(trim(F.col("CLM_ID")), F.length(trim(F.col("CLM_ID"))), 1).eqNullSafe("R")
        & (F.col("svCalcBuyIngrCstAmt") > 0.0),
        F.col("svCalcBuyIngrCstAmt") * -1
    ).otherwise(F.col("svCalcBuyIngrCstAmt")).alias("CALC_BUY_INGR_CST_AMT"),
    F.when(
        F.substring(trim(F.col("CLM_ID")), F.length(trim(F.col("CLM_ID"))), 1).eqNullSafe("R")
        & (F.col("svCalcGuarRbtAmt") > 0.0),
        F.col("svCalcGuarRbtAmt") * -1
    ).otherwise(F.col("svCalcGuarRbtAmt")).alias("CALC_GUAR_RBT_AMT"),
    F.when(
        F.substring(trim(F.col("CLM_ID")), F.length(trim(F.col("CLM_ID"))), 1).eqNullSafe("R")
        & (F.col("svCalcSellDispnsFeeAmt") > 0.0),
        F.col("svCalcSellDispnsFeeAmt") * -1
    ).otherwise(F.col("svCalcSellDispnsFeeAmt")).alias("CALC_SELL_DISPNS_FEE_AMT"),
    F.when(
        F.substring(trim(F.col("CLM_ID")), F.length(trim(F.col("CLM_ID"))), 1).eqNullSafe("R")
        & (F.col("svCalcSellIngrCstAmt") > 0.0),
        F.col("svCalcSellIngrCstAmt") * -1
    ).otherwise(F.col("svCalcSellIngrCstAmt")).alias("CALC_SELL_INGR_CST_AMT"),
    F.col("SPREAD_DISPNS_FEE_AMT").alias("SPREAD_DISPNS_FEE_AMT"),
    F.col("SPREAD_INGR_CST_AMT").alias("SPREAD_INGR_CST_AMT"),
    F.col("SPREAD_SLS_TAX_AMT").alias("SPREAD_SLS_TAX_AMT"),
    F.when(
        F.col("BUY_CST_TYP_ID").isNotNull(),
        F.col("BUY_CST_TYP_ID")
    ).otherwise(F.lit("")).alias("BUY_CST_TYP_ID"),
    F.when(
        F.col("BUY_PRICE_TYP_ID").isNotNull(),
        F.col("BUY_PRICE_TYP_ID")
    ).otherwise(F.lit("")).alias("BUY_PRICE_TYP_ID"),
    F.when(
        F.col("CST_TYP_ID").isNotNull(),
        F.col("CST_TYP_ID")
    ).otherwise(F.lit("")).alias("CST_TYP_ID"),
    F.when(
        F.col("DRUG_TYP_ID").isNotNull(),
        F.col("DRUG_TYP_ID")
    ).otherwise(F.lit("")).alias("DRUG_TYP_ID"),
    F.when(
        F.col("FINL_PLN_ID").isNotNull(),
        F.col("FINL_PLN_ID")
    ).otherwise(F.lit("")).alias("FINL_PLN_ID"),
    F.when(
        F.col("GNRC_PROD_ID").isNotNull(),
        F.col("GNRC_PROD_ID")
    ).otherwise(F.lit("")).alias("GNRC_PROD_ID"),
    F.when(
        F.col("SELL_PRICE_TYP_ID").isNotNull(),
        F.col("SELL_PRICE_TYP_ID")
    ).otherwise(F.lit("")).alias("SELL_PRICE_TYP_ID"),
    F.when(
        F.col("SPEC_PGM_ID").isNotNull(),
        F.col("SPEC_PGM_ID")
    ).otherwise(F.lit("")).alias("SPEC_PGM_ID"),
    F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("BUY_CST_SRC_CD_SK").alias("BUY_CST_SRC_CD_SK"),
    F.col("GNRC_OVRD_CD_SK").alias("GNRC_OVRD_CD_SK"),
    F.col("PDX_NTWK_QLFR_CD_SK").alias("PDX_NTWK_QLFR_CD_SK"),
    F.col("BUY_INCNTV_FEE_AMT").alias("BUY_INCNTV_FEE_AMT"),
    F.col("SELL_INCNTV_FEE_AMT").alias("SELL_INCNTV_FEE_AMT"),
    F.col("SPREAD_INCNTV_FEE_AMT").alias("SPREAD_INCNTV_FEE_AMT"),
    F.col("SELL_OTHR_PAYER_AMT").alias("SELL_OTHR_PAYER_AMT"),
    F.col("SELL_OTHR_AMT").alias("SELL_OTHR_AMT"),
    trim(F.col("FRMLRY_SK")).alias("FRMLRY_SK"),
    trim(F.col("FRMLRY_ID")).alias("FRMLRY_ID"),
    trim(F.col("TIER_ID")).alias("TIER_ID"),
    trim(F.col("DRUG_TYP_CD")).alias("DRUG_TYP_CD"),
    trim(F.col("DRUG_TYP_NM")).alias("DRUG_TYP_NM"),
    trim(F.col("DRUG_TYP_CD_SK")).alias("DRUG_TYP_CD_SK"),
    trim(F.col("PLN_DRUG_STTUS_CD")).alias("PLN_DRUG_STTUS_CD"),
    trim(F.col("PLN_DRUG_STTUS_NM")).alias("PLN_DRUG_STTUS_NM"),
    trim(F.col("PLN_DRUG_STTUS_CD_SK")).alias("PLN_DRUG_STTUS_CD_SK"),
    F.col("DRUG_PLN_TYP_ID").alias("DRUG_PLN_TYP_ID")
)

# lkupDrugClmPriceF (PxLookup) - left join with df_db2_DRUG_CLM_PRICE_F
df_lkupDrugClmPriceF_joined = (
    df_xfm_BusinessLogic.alias("lnk_DRUG_CLM_PRICE_F")
    .join(
        df_db2_DRUG_CLM_PRICE_F.alias("lnk_IdsEdwDrugclmPriceFLoad_Out"),
        F.col("lnk_DRUG_CLM_PRICE_F.CLM_SK") == F.col("lnk_IdsEdwDrugclmPriceFLoad_Out.CLM_SK"),
        "left"
    )
)

df_lkupDrugClmPriceF = df_lkupDrugClmPriceF_joined.select(
    F.col("lnk_DRUG_CLM_PRICE_F.CLM_SK").alias("CLM_SK"),
    F.col("lnk_DRUG_CLM_PRICE_F.CLM_ID").alias("CLM_ID"),
    F.col("lnk_DRUG_CLM_PRICE_F.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_DRUG_CLM_PRICE_F.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("lnk_DRUG_CLM_PRICE_F.LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("lnk_DRUG_CLM_PRICE_F.DRUG_CLM_SK").alias("DRUG_CLM_SK"),
    F.col("lnk_DRUG_CLM_PRICE_F.BUY_CST_SRC_CD").alias("BUY_CST_SRC_CD"),
    F.col("lnk_DRUG_CLM_PRICE_F.BUY_CST_SRC_NM").alias("BUY_CST_SRC_NM"),
    F.col("lnk_DRUG_CLM_PRICE_F.GNRC_OVRD_CD").alias("GNRC_OVRD_CD"),
    F.col("lnk_DRUG_CLM_PRICE_F.GNRC_OVRD_NM").alias("GNRC_OVRD_NM"),
    F.col("lnk_DRUG_CLM_PRICE_F.PDX_NTWK_QLFR_CD").alias("PDX_NTWK_QLFR_CD"),
    F.col("lnk_DRUG_CLM_PRICE_F.PDX_NTWK_QLFR_NM").alias("PDX_NTWK_QLFR_NM"),
    F.col("lnk_DRUG_CLM_PRICE_F.FRMLRY_PROTOCOL_IN").alias("FRMLRY_PROTOCOL_IN"),
    F.col("lnk_DRUG_CLM_PRICE_F.RECON_IN").alias("RECON_IN"),
    F.col("lnk_DRUG_CLM_PRICE_F.SPEC_PGM_IN").alias("SPEC_PGM_IN"),
    F.col("lnk_DRUG_CLM_PRICE_F.FINL_PLN_EFF_DT_SK").alias("FINL_PLN_EFF_DT_SK"),
    F.col("lnk_DRUG_CLM_PRICE_F.ORIG_PD_TRANS_SUBMT_DT_SK").alias("ORIG_PD_TRANS_SUBMT_DT_SK"),
    F.col("lnk_DRUG_CLM_PRICE_F.PRORTD_DISPNS_QTY").alias("PRORTD_DISPNS_QTY"),
    F.col("lnk_DRUG_CLM_PRICE_F.SUBMT_DISPNS_METRIC_QTY").alias("SUBMT_DISPNS_METRIC_QTY"),
    F.col("lnk_DRUG_CLM_PRICE_F.AVG_WHLSL_PRICE_UNIT_CST_AMT").alias("AVG_WHLSL_PRICE_UNIT_CST_AMT"),
    F.col("lnk_DRUG_CLM_PRICE_F.WHLSL_ACQSTN_CST_UNIT_CST_AMT").alias("WHLSL_ACQSTN_CST_UNIT_CST_AMT"),
    F.col("lnk_DRUG_CLM_PRICE_F.CST_TYP_UNIT_CST_AMT").alias("CST_TYP_UNIT_CST_AMT"),
    F.col("lnk_DRUG_CLM_PRICE_F.BUY_DISPNS_FEE_AMT").alias("BUY_DISPNS_FEE_AMT"),
    F.col("lnk_DRUG_CLM_PRICE_F.BUY_INGR_CST_AMT").alias("BUY_INGR_CST_AMT"),
    F.col("lnk_DRUG_CLM_PRICE_F.BUY_RATE_PCT").alias("BUY_RATE_PCT"),
    F.col("lnk_DRUG_CLM_PRICE_F.BUY_SLS_TAX_AMT").alias("BUY_SLS_TAX_AMT"),
    F.col("lnk_DRUG_CLM_PRICE_F.BUY_TOT_DUE_AMT").alias("BUY_TOT_DUE_AMT"),
    F.col("lnk_DRUG_CLM_PRICE_F.BUY_TOT_OTHR_AMT").alias("BUY_TOT_OTHR_AMT"),
    F.col("lnk_DRUG_CLM_PRICE_F.INVC_TOT_DUE_AMT").alias("INVC_TOT_DUE_AMT"),
    F.col("lnk_DRUG_CLM_PRICE_F.SELL_CST_TYP_UNIT_CST_AMT").alias("SELL_CST_TYP_UNIT_CST_AMT"),
    F.col("lnk_DRUG_CLM_PRICE_F.SELL_RATE_PCT").alias("SELL_RATE_PCT"),
    F.col("lnk_DRUG_CLM_PRICE_F.CALC_BUY_DISPNS_FEE_AMT").alias("CALC_BUY_DISPNS_FEE_AMT"),
    F.col("lnk_DRUG_CLM_PRICE_F.CALC_BUY_INGR_CST_AMT").alias("CALC_BUY_INGR_CST_AMT"),
    F.col("lnk_DRUG_CLM_PRICE_F.CALC_GUAR_RBT_AMT").alias("CALC_GUAR_RBT_AMT"),
    F.col("lnk_DRUG_CLM_PRICE_F.CALC_SELL_DISPNS_FEE_AMT").alias("CALC_SELL_DISPNS_FEE_AMT"),
    F.col("lnk_DRUG_CLM_PRICE_F.CALC_SELL_INGR_CST_AMT").alias("CALC_SELL_INGR_CST_AMT"),
    F.col("lnk_DRUG_CLM_PRICE_F.SPREAD_DISPNS_FEE_AMT").alias("SPREAD_DISPNS_FEE_AMT"),
    F.col("lnk_DRUG_CLM_PRICE_F.SPREAD_INGR_CST_AMT").alias("SPREAD_INGR_CST_AMT"),
    F.col("lnk_DRUG_CLM_PRICE_F.SPREAD_SLS_TAX_AMT").alias("SPREAD_SLS_TAX_AMT"),
    F.col("lnk_DRUG_CLM_PRICE_F.BUY_CST_TYP_ID").alias("BUY_CST_TYP_ID"),
    F.col("lnk_DRUG_CLM_PRICE_F.BUY_PRICE_TYP_ID").alias("BUY_PRICE_TYP_ID"),
    F.col("lnk_DRUG_CLM_PRICE_F.CST_TYP_ID").alias("CST_TYP_ID"),
    F.col("lnk_DRUG_CLM_PRICE_F.DRUG_TYP_ID").alias("DRUG_TYP_ID"),
    F.col("lnk_DRUG_CLM_PRICE_F.FINL_PLN_ID").alias("FINL_PLN_ID"),
    F.col("lnk_DRUG_CLM_PRICE_F.GNRC_PROD_ID").alias("GNRC_PROD_ID"),
    F.col("lnk_DRUG_CLM_PRICE_F.SELL_PRICE_TYP_ID").alias("SELL_PRICE_TYP_ID"),
    F.col("lnk_DRUG_CLM_PRICE_F.SPEC_PGM_ID").alias("SPEC_PGM_ID"),
    F.col("lnk_DRUG_CLM_PRICE_F.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_DRUG_CLM_PRICE_F.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_DRUG_CLM_PRICE_F.IDS_LAST_UPDT_RUN_CYC_EXCTN_SK").alias("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_DRUG_CLM_PRICE_F.BUY_CST_SRC_CD_SK").alias("BUY_CST_SRC_CD_SK"),
    F.col("lnk_DRUG_CLM_PRICE_F.GNRC_OVRD_CD_SK").alias("GNRC_OVRD_CD_SK"),
    F.col("lnk_DRUG_CLM_PRICE_F.PDX_NTWK_QLFR_CD_SK").alias("PDX_NTWK_QLFR_CD_SK"),
    F.col("lnk_DRUG_CLM_PRICE_F.BUY_INCNTV_FEE_AMT").alias("BUY_INCNTV_FEE_AMT"),
    F.col("lnk_DRUG_CLM_PRICE_F.SELL_INCNTV_FEE_AMT").alias("SELL_INCNTV_FEE_AMT"),
    F.col("lnk_DRUG_CLM_PRICE_F.SPREAD_INCNTV_FEE_AMT").alias("SPREAD_INCNTV_FEE_AMT"),
    F.col("lnk_DRUG_CLM_PRICE_F.SELL_OTHR_PAYER_AMT").alias("SELL_OTHR_PAYER_AMT"),
    F.col("lnk_DRUG_CLM_PRICE_F.SELL_OTHR_AMT").alias("SELL_OTHR_AMT"),
    F.col("lnk_IdsEdwDrugclmPriceFLoad_Out.CLM_SK").alias("TRG_CLM_SK"),
    F.col("lnk_IdsEdwDrugclmPriceFLoad_Out.RECON_IN").alias("TRG_RECON_IN"),
    F.col("lnk_DRUG_CLM_PRICE_F.FRMLRY_SK").alias("FRMLRY_SK"),
    F.col("lnk_DRUG_CLM_PRICE_F.FRMLRY_ID").alias("FRMLRY_ID"),
    F.col("lnk_DRUG_CLM_PRICE_F.TIER_ID").alias("TIER_ID"),
    F.col("lnk_DRUG_CLM_PRICE_F.DRUG_TYP_CD").alias("DRUG_TYP_CD"),
    F.col("lnk_DRUG_CLM_PRICE_F.DRUG_TYP_NM").alias("DRUG_TYP_NM"),
    F.col("lnk_DRUG_CLM_PRICE_F.DRUG_TYP_CD_SK").alias("DRUG_TYP_CD_SK"),
    F.col("lnk_DRUG_CLM_PRICE_F.PLN_DRUG_STTUS_CD").alias("PLN_DRUG_STTUS_CD"),
    F.col("lnk_DRUG_CLM_PRICE_F.PLN_DRUG_STTUS_NM").alias("PLN_DRUG_STTUS_NM"),
    F.col("lnk_DRUG_CLM_PRICE_F.PLN_DRUG_STTUS_CD_SK").alias("PLN_DRUG_STTUS_CD_SK"),
    F.col("lnk_DRUG_CLM_PRICE_F.DRUG_PLN_TYP_ID").alias("DRUG_PLN_TYP_ID")
)

# xfm_existence (CTransformerStage)
df_xfm_existence = df_lkupDrugClmPriceF.select(
    F.col("CLM_SK").alias("CLM_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("DRUG_CLM_SK").alias("DRUG_CLM_SK"),
    F.col("BUY_CST_SRC_CD").alias("BUY_CST_SRC_CD"),
    F.col("BUY_CST_SRC_NM").alias("BUY_CST_SRC_NM"),
    F.col("GNRC_OVRD_CD").alias("GNRC_OVRD_CD"),
    F.col("GNRC_OVRD_NM").alias("GNRC_OVRD_NM"),
    F.col("PDX_NTWK_QLFR_CD").alias("PDX_NTWK_QLFR_CD"),
    F.col("PDX_NTWK_QLFR_NM").alias("PDX_NTWK_QLFR_NM"),
    F.col("FRMLRY_PROTOCOL_IN").alias("FRMLRY_PROTOCOL_IN"),
    F.when(
        F.col("TRG_CLM_SK").isNull(),
        F.col("RECON_IN")
    ).otherwise(
        F.col("TRG_RECON_IN")
    ).alias("RECON_IN"),
    F.col("SPEC_PGM_IN").alias("SPEC_PGM_IN"),
    F.col("FINL_PLN_EFF_DT_SK").alias("FINL_PLN_EFF_DT_SK"),
    F.col("ORIG_PD_TRANS_SUBMT_DT_SK").alias("ORIG_PD_TRANS_SUBMT_DT_SK"),
    F.col("PRORTD_DISPNS_QTY").alias("PRORTD_DISPNS_QTY"),
    F.col("SUBMT_DISPNS_METRIC_QTY").alias("SUBMT_DISPNS_METRIC_QTY"),
    F.col("AVG_WHLSL_PRICE_UNIT_CST_AMT").alias("AVG_WHLSL_PRICE_UNIT_CST_AMT"),
    F.col("WHLSL_ACQSTN_CST_UNIT_CST_AMT").alias("WHLSL_ACQSTN_CST_UNIT_CST_AMT"),
    F.col("CST_TYP_UNIT_CST_AMT").alias("CST_TYP_UNIT_CST_AMT"),
    F.col("BUY_DISPNS_FEE_AMT").alias("BUY_DISPNS_FEE_AMT"),
    F.col("BUY_INGR_CST_AMT").alias("BUY_INGR_CST_AMT"),
    F.col("BUY_RATE_PCT").alias("BUY_RATE_PCT"),
    F.col("BUY_SLS_TAX_AMT").alias("BUY_SLS_TAX_AMT"),
    F.col("BUY_TOT_DUE_AMT").alias("BUY_TOT_DUE_AMT"),
    F.col("BUY_TOT_OTHR_AMT").alias("BUY_TOT_OTHR_AMT"),
    F.col("INVC_TOT_DUE_AMT").alias("INVC_TOT_DUE_AMT"),
    F.col("SELL_CST_TYP_UNIT_CST_AMT").alias("SELL_CST_TYP_UNIT_CST_AMT"),
    F.col("SELL_RATE_PCT").alias("SELL_RATE_PCT"),
    F.col("CALC_BUY_DISPNS_FEE_AMT").alias("CALC_BUY_DISPNS_FEE_AMT"),
    F.col("CALC_BUY_INGR_CST_AMT").alias("CALC_BUY_INGR_CST_AMT"),
    F.col("CALC_GUAR_RBT_AMT").alias("CALC_GUAR_RBT_AMT"),
    F.col("CALC_SELL_DISPNS_FEE_AMT").alias("CALC_SELL_DISPNS_FEE_AMT"),
    F.col("CALC_SELL_INGR_CST_AMT").alias("CALC_SELL_INGR_CST_AMT"),
    F.col("SPREAD_DISPNS_FEE_AMT").alias("SPREAD_DISPNS_FEE_AMT"),
    F.col("SPREAD_INGR_CST_AMT").alias("SPREAD_INGR_CST_AMT"),
    F.col("SPREAD_SLS_TAX_AMT").alias("SPREAD_SLS_TAX_AMT"),
    F.col("BUY_CST_TYP_ID").alias("BUY_CST_TYP_ID"),
    F.col("BUY_PRICE_TYP_ID").alias("BUY_PRICE_TYP_ID"),
    F.col("CST_TYP_ID").alias("CST_TYP_ID"),
    F.col("DRUG_TYP_ID").alias("DRUG_TYP_ID"),
    F.col("FINL_PLN_ID").alias("FINL_PLN_ID"),
    F.col("GNRC_PROD_ID").alias("GNRC_PROD_ID"),
    F.col("SELL_PRICE_TYP_ID").alias("SELL_PRICE_TYP_ID"),
    F.col("SPEC_PGM_ID").alias("SPEC_PGM_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK").alias("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("BUY_CST_SRC_CD_SK").alias("BUY_CST_SRC_CD_SK"),
    F.col("GNRC_OVRD_CD_SK").alias("GNRC_OVRD_CD_SK"),
    F.col("PDX_NTWK_QLFR_CD_SK").alias("PDX_NTWK_QLFR_CD_SK"),
    F.col("BUY_INCNTV_FEE_AMT").alias("BUY_INCNTV_FEE_AMT"),
    F.col("SELL_INCNTV_FEE_AMT").alias("SELL_INCNTV_FEE_AMT"),
    F.col("SPREAD_INCNTV_FEE_AMT").alias("SPREAD_INCNTV_FEE_AMT"),
    F.col("SELL_OTHR_PAYER_AMT").alias("SELL_OTHR_PAYER_AMT"),
    F.col("SELL_OTHR_AMT").alias("SELL_OTHR_AMT"),
    F.col("FRMLRY_SK").alias("FRMLRY_SK"),
    F.col("FRMLRY_ID").alias("FRMLRY_ID"),
    F.col("TIER_ID").alias("TIER_ID"),
    F.col("DRUG_TYP_CD").alias("DRUG_TYP_CD"),
    F.col("DRUG_TYP_NM").alias("DRUG_TYP_NM"),
    F.col("DRUG_TYP_CD_SK").alias("DRUG_TYP_CD_SK"),
    F.col("PLN_DRUG_STTUS_CD").alias("PLN_DRUG_STTUS_CD"),
    F.col("PLN_DRUG_STTUS_NM").alias("PLN_DRUG_STTUS_NM"),
    F.col("PLN_DRUG_STTUS_CD_SK").alias("PLN_DRUG_STTUS_CD_SK"),
    F.col("DRUG_PLN_TYP_ID").alias("DRUG_PLN_TYP_ID")
)

# seq_DRUG_CLM_PRICE_F (PxSequentialFile) => Write to .dat
# Apply rpad for char columns (length=1 or length=10) in final select
df_final = df_xfm_existence.select(
    "CLM_SK",
    "CLM_ID",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "DRUG_CLM_SK",
    "BUY_CST_SRC_CD",
    "BUY_CST_SRC_NM",
    "GNRC_OVRD_CD",
    "GNRC_OVRD_NM",
    "PDX_NTWK_QLFR_CD",
    "PDX_NTWK_QLFR_NM",
    "FRMLRY_PROTOCOL_IN",
    "RECON_IN",
    "SPEC_PGM_IN",
    "FINL_PLN_EFF_DT_SK",
    "ORIG_PD_TRANS_SUBMT_DT_SK",
    "PRORTD_DISPNS_QTY",
    "SUBMT_DISPNS_METRIC_QTY",
    "AVG_WHLSL_PRICE_UNIT_CST_AMT",
    "WHLSL_ACQSTN_CST_UNIT_CST_AMT",
    "CST_TYP_UNIT_CST_AMT",
    "BUY_DISPNS_FEE_AMT",
    "BUY_INGR_CST_AMT",
    "BUY_RATE_PCT",
    "BUY_SLS_TAX_AMT",
    "BUY_TOT_DUE_AMT",
    "BUY_TOT_OTHR_AMT",
    "INVC_TOT_DUE_AMT",
    "SELL_CST_TYP_UNIT_CST_AMT",
    "SELL_RATE_PCT",
    "CALC_BUY_DISPNS_FEE_AMT",
    "CALC_BUY_INGR_CST_AMT",
    "CALC_GUAR_RBT_AMT",
    "CALC_SELL_DISPNS_FEE_AMT",
    "CALC_SELL_INGR_CST_AMT",
    "SPREAD_DISPNS_FEE_AMT",
    "SPREAD_INGR_CST_AMT",
    "SPREAD_SLS_TAX_AMT",
    "BUY_CST_TYP_ID",
    "BUY_PRICE_TYP_ID",
    "CST_TYP_ID",
    "DRUG_TYP_ID",
    "FINL_PLN_ID",
    "GNRC_PROD_ID",
    "SELL_PRICE_TYP_ID",
    "SPEC_PGM_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "IDS_LAST_UPDT_RUN_CYC_EXCTN_SK",
    "BUY_CST_SRC_CD_SK",
    "GNRC_OVRD_CD_SK",
    "PDX_NTWK_QLFR_CD_SK",
    "BUY_INCNTV_FEE_AMT",
    "SELL_INCNTV_FEE_AMT",
    "SPREAD_INCNTV_FEE_AMT",
    "SELL_OTHR_PAYER_AMT",
    "SELL_OTHR_AMT",
    "FRMLRY_SK",
    "FRMLRY_ID",
    "TIER_ID",
    "DRUG_TYP_CD",
    "DRUG_TYP_NM",
    "DRUG_TYP_CD_SK",
    "PLN_DRUG_STTUS_CD",
    "PLN_DRUG_STTUS_NM",
    "PLN_DRUG_STTUS_CD_SK",
    "DRUG_PLN_TYP_ID"
).withColumn(
    "FRMLRY_PROTOCOL_IN", F.rpad(F.col("FRMLRY_PROTOCOL_IN"), 1, " ")
).withColumn(
    "RECON_IN", F.rpad(F.col("RECON_IN"), 1, " ")
).withColumn(
    "SPEC_PGM_IN", F.rpad(F.col("SPEC_PGM_IN"), 1, " ")
).withColumn(
    "CRT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
).withColumn(
    "FINL_PLN_EFF_DT_SK", F.rpad(F.col("FINL_PLN_EFF_DT_SK"), 10, " ")
).withColumn(
    "ORIG_PD_TRANS_SUBMT_DT_SK", F.rpad(F.col("ORIG_PD_TRANS_SUBMT_DT_SK"), 10, " ")
)

write_files(
    df_final,
    f"{adls_path}/load/DRUG_CLM_PRICE_F.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)