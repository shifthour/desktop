# Databricks notebook source
# MAGIC %md
# MAGIC **Translation Date:** 2025-09-02
# MAGIC """
# MAGIC Â© Copyright 2008 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by:
# MAGIC                     PBMEdwEdwGrpStopLossPdxClmMnthlyCntl
# MAGIC 
# MAGIC PROCESSING : Update Calculated field Value and Generate load records from SC source File to GRP_STOP_LOSS_PDX_CLM_RCVD_MNTHLY_SUM for Medtrak
# MAGIC 
# MAGIC Modifications:                        
# MAGIC  Developer                   Date              Project/Altiris #                        Change Description                                                                                  Development Project       Code Reviewer            Date Reviewed
# MAGIC --------------------------------    -------------------   -------------------------------------------     --------------------------------------------------------------------------------------------------------------   ----------------------------------      ---------------------------------   -------------------------   
# MAGIC Kaushik Kapoor           2018-08-13    5828-Stop Loss-Calc Fields      Original Programming                                                                               EnterpriseDev2                Hugh Sisson                 2018-08-17

# MAGIC Remove Dups on File_Dt_SK and retain first
# MAGIC Edw to Edw Stop Loss PDX Claim Extr
# MAGIC 
# MAGIC 1) Missing GrpBase  in PBM's Control File from PBM's Source file
# MAGIC 2) Update Calculated field for "GROUP TOTAL" Record Type Name
# MAGIC Lkp SC source file key fields with SC control file key fields
# MAGIC Retain Last based on PRCS_DT and GRP_ID
# MAGIC The load  file is used by the EdwEdwStopLossPdxLoad job to load EDW DB GRP_STOP_LOSS_PDX_CLM_RCVD_MNTHLY_SUM Table
# MAGIC CLM PDX Agg Amt from CLM_F
# MAGIC GROUP TOTAL records from 
# MAGIC EDW GRP_STOP_LOSS_PDX_ CLM_RCVD_MNTHLY_SUM Table
# MAGIC GROUP RX records from 
# MAGIC EDW GRP_STOP_LOSS_PDX_ CLM_RCVD_MNTHLY_SUM Table
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F, types as T
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


# 1) Retrieve all parameter values, including database-owner parameters and their corresponding secret names
IDSOwner = get_widget_value('IDSOwner', '')
ids_secret_name = get_widget_value('ids_secret_name', '')
EDWOwner = get_widget_value('EDWOwner', '')
edw_secret_name = get_widget_value('edw_secret_name', '')
RunCycleDt = get_widget_value('RunCycleDt', '')
InFileName = get_widget_value('InFileName', '')
FILE_DATE = get_widget_value('FILE_DATE', '')
SrcSysCd = get_widget_value('SrcSysCd', '')
RunID = get_widget_value('RunID', '')
MinPrcsDt = get_widget_value('MinPrcsDt', '')
MaxPrcsDt = get_widget_value('MaxPrcsDt', '')

# 2) Read from EDW: GrndTotPdx
jdbc_url_edw, jdbc_props_edw = get_db_config(edw_secret_name)
sql_GrndTotPdx = f"""
SELECT
  LEFT(FILE_DT_SK,7) AS FILE_DT_SK_YR_MN,
  FILE_DT_SK AS FILE_DT_SK,
  SRC_SYS_GRP_ID,
  RCRD_TYP_NM,
  SRC_SYS_CD,
  CRT_RUN_CYC_EXCTN_DT_SK,
  LAST_UPDT_RUN_CYC_EXCTN_DT_SK,
  GRP_ID,
  SRC_SYS_COV_NO,
  RCVD_TOT_RCRD_CT,
  RCVD_TOT_INGR_CST_AMT,
  RCVD_TOT_DRUG_ADM_FEE_AMT,
  RCVD_TOT_COPAY_AMT,
  RCVD_TOT_PD_AMT,
  RCVD_TOT_QTY_CT,
  CALC_TOT_RCRD_CT,
  CALC_TOT_INGR_CST_AMT,
  CALC_TOT_DRUG_ADM_FEE_AMT,
  CALC_TOT_COPAY_AMT,
  CALC_TOT_PD_AMT,
  CALC_TOT_QTY_CT
FROM {EDWOwner}.GRP_STOP_LOSS_PDX_CLM_RCVD_MNTHLY_SUM
WHERE
  RCRD_TYP_NM = 'GRAND TOTAL'
  AND SRC_SYS_CD = '{SrcSysCd}'
  AND LEFT(FILE_DT_SK,7) = CASE WHEN '{FILE_DATE}' = '2199-12'
    THEN (
      SELECT MAX(LEFT(FILE_DT_SK,7))
      FROM {EDWOwner}.GRP_STOP_LOSS_PDX_CLM_RCVD_MNTHLY_SUM
      WHERE SRC_SYS_CD = '{SrcSysCd}'
    )
    ELSE '{FILE_DATE}' END
  AND FILE_DT_SK = (
    SELECT MAX(FILE_DT_SK)
    FROM {EDWOwner}.GRP_STOP_LOSS_PDX_CLM_RCVD_MNTHLY_SUM
    WHERE SRC_SYS_CD = '{SrcSysCd}'
      AND RCRD_TYP_NM = 'GRAND TOTAL'
  )
"""
df_GrndTotPdx = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", sql_GrndTotPdx)
    .load()
)

# 3) Read from EDW: GrpTot_Med
sql_GrpTot_Med = f"""
SELECT
  LEFT(FILE_DT_SK,7) AS FILE_DT_SK_YR_MN,
  MAX(FILE_DT_SK) AS FILE_DT_SK,
  CAST(MAX(SRC_SYS_GRP_ID) AS VARCHAR(20)) AS SRC_SYS_GRP_ID,
  RCRD_TYP_NM,
  SRC_SYS_CD,
  MAX(CRT_RUN_CYC_EXCTN_DT_SK) AS CRT_RUN_CYC_EXCTN_DT_SK,
  GRP_ID,
  CAST(MAX(SRC_SYS_COV_NO) AS VARCHAR(20)) AS SRC_SYS_COV_NO,
  SUM(RCVD_TOT_RCRD_CT) AS RCVD_TOT_RCRD_CT,
  CAST(SUM(RCVD_TOT_INGR_CST_AMT) AS DECIMAL(13,2)) AS RCVD_TOT_INGR_CST_AMT,
  CAST(SUM(RCVD_TOT_DRUG_ADM_FEE_AMT) AS DECIMAL(13,2)) AS RCVD_TOT_DRUG_ADM_FEE_AMT,
  CAST(SUM(RCVD_TOT_COPAY_AMT) AS DECIMAL(13,2)) AS RCVD_TOT_COPAY_AMT,
  CAST(SUM(RCVD_TOT_PD_AMT) AS DECIMAL(13,2)) AS RCVD_TOT_PD_AMT,
  CAST(SUM(RCVD_TOT_QTY_CT) AS DECIMAL(13,3)) AS RCVD_TOT_QTY_CT
FROM {EDWOwner}.GRP_STOP_LOSS_PDX_CLM_RCVD_MNTHLY_SUM
WHERE
  RCRD_TYP_NM = 'GROUP TOTAL'
  AND SRC_SYS_CD = '{SrcSysCd}'
  AND LEFT(FILE_DT_SK,7) = CASE WHEN '{FILE_DATE}' = '2199-12'
    THEN (
      SELECT MAX(LEFT(FILE_DT_SK,7))
      FROM {EDWOwner}.GRP_STOP_LOSS_PDX_CLM_RCVD_MNTHLY_SUM
      WHERE SRC_SYS_CD = '{SrcSysCd}'
        AND RCRD_TYP_NM <> 'GRAND TOTAL'
    )
    ELSE '{FILE_DATE}' END
  AND FILE_DT_SK = (
    SELECT MAX(FILE_DT_SK)
    FROM {EDWOwner}.GRP_STOP_LOSS_PDX_CLM_RCVD_MNTHLY_SUM
    WHERE SRC_SYS_CD = '{SrcSysCd}'
      AND RCRD_TYP_NM = 'GROUP TOTAL'
  )
GROUP BY
  LEFT(FILE_DT_SK,7),
  RCRD_TYP_NM,
  SRC_SYS_CD,
  GRP_ID
"""
df_GrpTot_Med = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", sql_GrpTot_Med)
    .load()
)

# 4) Read from EDW: CLM_AggAmt
sql_CLM_AggAmt = f"""
SELECT
  clm.GRP_SK,
  LEFT('{MaxPrcsDt}',7) AS CLM_PRCS_DT_SK,
  clm.SRC_SYS_CD,
  clm.GRP_ID,
  clm.CLM_SUBTYP_CD,
  CAST(SUM(clm.CLM_ACTL_PD_AMT) AS DECIMAL(13,2)) AS CLM_ACTL_PD_AMT,
  CAST(SUM(clm.CLM_LN_TOT_COPAY_AMT) AS DECIMAL(13,2)) AS CLM_LN_TOT_COPAY_AMT,
  CAST(SUM(clm.DRUG_CLM_INGR_CST_ALW_AMT) AS DECIMAL(13,2)) AS DRUG_CLM_INGR_CST_ALW_AMT,
  CAST(SUM(clm.DRUG_CLM_RX_ALW_QTY) AS DECIMAL(13,3)) AS DRUG_CLM_RX_ALW_QTY,
  COUNT(*) AS REC_CNT,
  1 AS GRND_TOT_REC_CNT
FROM {EDWOwner}.CLM_F clm
INNER JOIN {EDWOwner}.CLM_F2 clm2
  ON clm.CLM_SK = clm2.CLM_SK
WHERE
  clm.SRC_SYS_CD = '{SrcSysCd}'
  AND clm.CLM_SUBTYP_CD = 'RX'
  AND clm2.CLM_PRCS_DT_SK >= '{MinPrcsDt}'
  AND clm2.CLM_PRCS_DT_SK <= '{MaxPrcsDt}'
GROUP BY
  clm.GRP_SK,
  clm.SRC_SYS_CD,
  clm.GRP_ID,
  clm.CLM_SUBTYP_CD
"""
df_CLM_AggAmt = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", sql_CLM_AggAmt)
    .load()
)

# 5) cpy_clmAggAmt (PxCopy) -> create two outputs from df_CLM_AggAmt
df_cpy_clmAggAmt_GrndTot = df_CLM_AggAmt.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CLM_SUBTYP_CD").alias("CLM_SUBTYP_CD"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("CLM_PRCS_DT_SK").alias("CLM_PRCS_DT_SK"),
    F.col("CLM_ACTL_PD_AMT").alias("CLM_ACTL_PD_AMT"),
    F.col("CLM_LN_TOT_COPAY_AMT").alias("CLM_LN_TOT_COPAY_AMT"),
    F.col("DRUG_CLM_INGR_CST_ALW_AMT").alias("DRUG_CLM_INGR_CST_ALW_AMT"),
    F.col("DRUG_CLM_RX_ALW_QTY").alias("DRUG_CLM_RX_ALW_QTY"),
    F.col("REC_CNT").alias("REC_CNT")
)
df_cpy_clmAggAmt_Lnk_AggAmt = df_CLM_AggAmt.select(
    F.col("GRP_SK").alias("GRP_SK"),
    F.col("CLM_PRCS_DT_SK").alias("CLM_PRCS_DT_SK"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("CLM_SUBTYP_CD").alias("CLM_SUBTYP_CD"),
    F.col("CLM_ACTL_PD_AMT").alias("CLM_ACTL_PD_AMT"),
    F.col("CLM_LN_TOT_COPAY_AMT").alias("CLM_LN_TOT_COPAY_AMT"),
    F.col("DRUG_CLM_INGR_CST_ALW_AMT").alias("DRUG_CLM_INGR_CST_ALW_AMT"),
    F.col("DRUG_CLM_RX_ALW_QTY").alias("DRUG_CLM_RX_ALW_QTY"),
    F.col("REC_CNT").alias("REC_CNT"),
)

# 6) Lkp_SCgrpTot_ClmAggAmt (PxLookup) -> primary: df_cpy_clmAggAmt_Lnk_AggAmt, lookup: df_GrpTot_Med (left join)
# Join columns: (CLM_PRCS_DT_SK=FILE_DT_SK_YR_MN, SRC_SYS_CD=SRC_SYS_CD, GRP_ID=GRP_ID)
df_cpy_clmAggAmt_Lnk_AggAmt_alias = df_cpy_clmAggAmt_Lnk_AggAmt.alias("Lnk_AggAmt")
df_GrpTot_Med_alias = df_GrpTot_Med.alias("SCgrpTot")
cond_Lkp_SCgrpTot = [
    F.col("Lnk_AggAmt.CLM_PRCS_DT_SK") == F.col("SCgrpTot.FILE_DT_SK_YR_MN"),
    F.col("Lnk_AggAmt.SRC_SYS_CD") == F.col("SCgrpTot.SRC_SYS_CD"),
    F.col("Lnk_AggAmt.GRP_ID") == F.col("SCgrpTot.GRP_ID")
]
df_Lkp_SCgrpTot_ClmAggAmt = (
    df_cpy_clmAggAmt_Lnk_AggAmt_alias.join(df_GrpTot_Med_alias, cond_Lkp_SCgrpTot, how="left")
    .select(
        F.col("Lnk_AggAmt.GRP_SK").alias("GRP_SK"),
        F.col("Lnk_AggAmt.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Lnk_AggAmt.CLM_ACTL_PD_AMT").alias("CLM_ACTL_PD_AMT"),
        F.col("Lnk_AggAmt.CLM_LN_TOT_COPAY_AMT").alias("CLM_LN_TOT_COPAY_AMT"),
        F.col("Lnk_AggAmt.DRUG_CLM_INGR_CST_ALW_AMT").alias("DRUG_CLM_INGR_CST_ALW_AMT"),
        F.col("Lnk_AggAmt.DRUG_CLM_RX_ALW_QTY").alias("DRUG_CLM_RX_ALW_QTY"),
        F.col("Lnk_AggAmt.REC_CNT").alias("REC_CNT"),
        F.col("Lnk_AggAmt.GRP_ID").alias("GRP_ID"),
        F.col("Lnk_AggAmt.CLM_PRCS_DT_SK").alias("CLM_PRCS_DT_SK"),
        F.col("SCgrpTot.FILE_DT_SK_YR_MN").alias("SC_FILE_DT_SK"),
        F.col("SCgrpTot.FILE_DT_SK").alias("FILE_DT_SK"),
        F.col("SCgrpTot.SRC_SYS_GRP_ID").alias("SRC_SYS_GRP_ID"),
        F.col("SCgrpTot.RCRD_TYP_NM").alias("RCRD_TYP_NM"),
        F.col("SCgrpTot.SRC_SYS_CD").alias("SC_SRC_SYS_CD"),
        F.col("SCgrpTot.GRP_ID").alias("SC_GRP_ID"),
        F.col("SCgrpTot.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.col("SCgrpTot.SRC_SYS_COV_NO").alias("SRC_SYS_COV_NO"),
        F.col("SCgrpTot.RCVD_TOT_RCRD_CT").alias("RCVD_TOT_RCRD_CT"),
        F.col("SCgrpTot.RCVD_TOT_INGR_CST_AMT").alias("RCVD_TOT_INGR_CST_AMT"),
        F.col("SCgrpTot.RCVD_TOT_DRUG_ADM_FEE_AMT").alias("RCVD_TOT_DRUG_ADM_FEE_AMT"),
        F.col("SCgrpTot.RCVD_TOT_COPAY_AMT").alias("RCVD_TOT_COPAY_AMT"),
        F.col("SCgrpTot.RCVD_TOT_PD_AMT").alias("RCVD_TOT_PD_AMT"),
        F.col("SCgrpTot.RCVD_TOT_QTY_CT").alias("RCVD_TOT_QTY_CT")
    )
)

# 7) CLM_SC_MatchUnmatch (Transformer) -> output links: Lnk_updateGrpTot, Lnk_All
df_CLM_SC_in = df_Lkp_SCgrpTot_ClmAggAmt.alias("Lnk_GrpTot")
# Build columns for Lnk_updateGrpTot
df_CLM_SC_Lnk_updateGrpTot = df_CLM_SC_in.select(
    F.when(
        F.trim(F.when(F.col("Lnk_GrpTot.FILE_DT_SK").isNotNull(), F.col("Lnk_GrpTot.FILE_DT_SK")).otherwise(F.lit(""))) == F.lit(""),
        F.lit("1753-01-01")
    ).otherwise(F.col("Lnk_GrpTot.FILE_DT_SK")).alias("FILE_DT_SK"),
    F.when(
        F.trim(F.when(F.col("Lnk_GrpTot.SRC_SYS_GRP_ID").isNotNull(), F.col("Lnk_GrpTot.SRC_SYS_GRP_ID")).otherwise(F.lit(""))) == F.lit(""),
        F.lit("999999")
    ).otherwise(F.col("Lnk_GrpTot.SRC_SYS_GRP_ID")).alias("SRC_SYS_GRP_ID"),
    F.when(
        F.trim(F.when(F.col("Lnk_GrpTot.RCRD_TYP_NM").isNotNull(), F.col("Lnk_GrpTot.RCRD_TYP_NM")).otherwise(F.lit(""))) == F.lit(""),
        F.lit("GROUP TOTAL")
    ).otherwise(F.col("Lnk_GrpTot.RCRD_TYP_NM")).alias("RCRD_TYP_NM"),
    F.when(
        F.trim(F.when(F.col("Lnk_GrpTot.SC_SRC_SYS_CD").isNotNull(), F.col("Lnk_GrpTot.SC_SRC_SYS_CD")).otherwise(F.lit(""))) == F.lit(""),
        F.lit(SrcSysCd)
    ).otherwise(F.col("Lnk_GrpTot.SC_SRC_SYS_CD")).alias("SRC_SYS_CD"),
    F.when(
        F.trim(F.when(F.col("Lnk_GrpTot.CRT_RUN_CYC_EXCTN_DT_SK").isNotNull(), F.col("Lnk_GrpTot.CRT_RUN_CYC_EXCTN_DT_SK")).otherwise(F.lit(""))) == F.lit(""),
        F.lit(RunCycleDt)
    ).otherwise(F.col("Lnk_GrpTot.CRT_RUN_CYC_EXCTN_DT_SK")).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.when(
        F.trim(F.when(F.lit(RunCycleDt).isNotNull(), F.lit(RunCycleDt)).otherwise(F.lit(""))) == F.lit(""),
        F.lit("")
    ).otherwise(F.lit(RunCycleDt)).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.when(
        F.col("Lnk_GrpTot.GRP_SK") == F.lit(0),
        F.lit("UNK")
    ).otherwise(
        F.when(
            F.trim(F.when(F.col("Lnk_GrpTot.GRP_ID").isNotNull(), F.col("Lnk_GrpTot.GRP_ID")).otherwise(F.lit(""))) == F.lit(""),
            F.lit("999999")
        ).otherwise(F.col("Lnk_GrpTot.GRP_ID"))
    ).alias("GRP_ID"),
    F.when(
        F.trim(F.when(F.col("Lnk_GrpTot.SRC_SYS_COV_NO").isNotNull(), F.col("Lnk_GrpTot.SRC_SYS_COV_NO")).otherwise(F.lit(""))) == F.lit(""),
        F.lit("999999")
    ).otherwise(F.col("Lnk_GrpTot.SRC_SYS_COV_NO")).alias("SRC_SYS_COV_NO"),
    F.when(F.coalesce(F.col("Lnk_GrpTot.RCVD_TOT_RCRD_CT"), F.lit(0)) == 0, F.lit(0)).otherwise(F.col("Lnk_GrpTot.RCVD_TOT_RCRD_CT")).alias("RCVD_TOT_RCRD_CT"),
    F.when(F.coalesce(F.col("Lnk_GrpTot.RCVD_TOT_INGR_CST_AMT"), F.lit(0)) == 0, F.lit(0)).otherwise(F.col("Lnk_GrpTot.RCVD_TOT_INGR_CST_AMT")).alias("RCVD_TOT_INGR_CST_AMT"),
    F.when(F.coalesce(F.col("Lnk_GrpTot.RCVD_TOT_DRUG_ADM_FEE_AMT"), F.lit(0)) == 0, F.lit(0)).otherwise(F.col("Lnk_GrpTot.RCVD_TOT_DRUG_ADM_FEE_AMT")).alias("RCVD_TOT_DRUG_ADM_FEE_AMT"),
    F.when(F.coalesce(F.col("Lnk_GrpTot.RCVD_TOT_COPAY_AMT"), F.lit(0)) == 0, F.lit(0)).otherwise(F.col("Lnk_GrpTot.RCVD_TOT_COPAY_AMT")).alias("RCVD_TOT_COPAY_AMT"),
    F.when(F.coalesce(F.col("Lnk_GrpTot.RCVD_TOT_PD_AMT"), F.lit(0)) == 0, F.lit(0)).otherwise(F.col("Lnk_GrpTot.RCVD_TOT_PD_AMT")).alias("RCVD_TOT_PD_AMT"),
    F.when(F.coalesce(F.col("Lnk_GrpTot.RCVD_TOT_QTY_CT"), F.lit(0)) == 0, F.lit(0)).otherwise(F.col("Lnk_GrpTot.RCVD_TOT_QTY_CT")).alias("RCVD_TOT_QTY_CT"),
    F.when(F.coalesce(F.col("Lnk_GrpTot.REC_CNT"), F.lit(0)) == 0, F.lit(0)).otherwise(F.col("Lnk_GrpTot.REC_CNT")).alias("CALC_TOT_RCRD_CT"),
    F.when(F.coalesce(F.col("Lnk_GrpTot.DRUG_CLM_INGR_CST_ALW_AMT"), F.lit(0)) == 0, F.lit(0)).otherwise(F.col("Lnk_GrpTot.DRUG_CLM_INGR_CST_ALW_AMT")).alias("CALC_TOT_INGR_CST_AMT"),
    F.lit(None).alias("CALC_TOT_DRUG_ADM_FEE_AMT"),
    F.when(F.coalesce(F.col("Lnk_GrpTot.CLM_LN_TOT_COPAY_AMT"), F.lit(0)) == 0, F.lit(0)).otherwise(F.col("Lnk_GrpTot.CLM_LN_TOT_COPAY_AMT")).alias("CALC_TOT_COPAY_AMT"),
    F.when(F.coalesce(F.col("Lnk_GrpTot.CLM_ACTL_PD_AMT"), F.lit(0)) == 0, F.lit(0)).otherwise(F.col("Lnk_GrpTot.CLM_ACTL_PD_AMT")).alias("CALC_TOT_PD_AMT"),
    F.when(F.coalesce(F.col("Lnk_GrpTot.DRUG_CLM_RX_ALW_QTY"), F.lit(0)) == 0, F.lit(0)).otherwise(F.col("Lnk_GrpTot.DRUG_CLM_RX_ALW_QTY")).alias("CALC_TOT_QTY_CT")
)

# Build columns for Lnk_All
df_CLM_SC_Lnk_All = df_CLM_SC_in.select(
    F.when(
        F.trim(F.when(F.col("Lnk_GrpTot.CLM_PRCS_DT_SK").isNotNull(), F.col("Lnk_GrpTot.CLM_PRCS_DT_SK")).otherwise(F.lit(""))) == F.lit(""),
        F.lit("")
    ).otherwise(F.col("Lnk_GrpTot.CLM_PRCS_DT_SK")).alias("FILE_DT_SK_YRMN"),
    F.when(
        F.trim(F.when(F.col("Lnk_GrpTot.FILE_DT_SK").isNotNull(), F.col("Lnk_GrpTot.FILE_DT_SK")).otherwise(F.lit(""))) == F.lit(""),
        F.lit("")
    ).otherwise(F.col("Lnk_GrpTot.FILE_DT_SK")).alias("FILE_DT_SK"),
    F.when(
        F.col("Lnk_GrpTot.GRP_SK") == 0,
        F.lit("UNK")
    ).otherwise(
        F.when(
            F.trim(F.when(F.col("Lnk_GrpTot.GRP_ID").isNotNull(), F.col("Lnk_GrpTot.GRP_ID")).otherwise(F.lit(""))) == F.lit(""),
            F.lit("")
        ).otherwise(F.col("Lnk_GrpTot.GRP_ID"))
    ).alias("GRP_ID"),
    F.lit("GROUP TOTAL").alias("RCRD_TYP_NM"),
    F.col("Lnk_GrpTot.REC_CNT").alias("CALC_TOT_RCRD_CT"),
    F.col("Lnk_GrpTot.DRUG_CLM_INGR_CST_ALW_AMT").alias("CALC_TOT_INGR_CST_AMT"),
    F.lit(None).alias("CALC_TOT_DRUG_ADM_FEE_AMT"),
    F.col("Lnk_GrpTot.CLM_LN_TOT_COPAY_AMT").alias("CALC_TOT_COPAY_AMT"),
    F.col("Lnk_GrpTot.CLM_ACTL_PD_AMT").alias("CALC_TOT_PD_AMT"),
    F.col("Lnk_GrpTot.DRUG_CLM_RX_ALW_QTY").alias("CALC_TOT_QTY_CT")
)

# 8) Agg_GrpTot_Amt (PxAggregator) on df_cpy_clmAggAmt_GrndTot
df_Agg_GrpTot_Amt = (
    df_cpy_clmAggAmt_GrndTot.groupBy("CLM_PRCS_DT_SK", "SRC_SYS_CD")
    .agg(
        F.sum("CLM_ACTL_PD_AMT").alias("CLM_ACTL_PD_AMT"),
        F.sum("CLM_LN_TOT_COPAY_AMT").alias("CLM_LN_TOT_COPAY_AMT"),
        F.sum("DRUG_CLM_INGR_CST_ALW_AMT").alias("DRUG_CLM_INGR_CST_ALW_AMT"),
        F.sum("DRUG_CLM_RX_ALW_QTY").alias("DRUG_CLM_RX_ALW_QTY"),
        F.sum("REC_CNT").alias("GRND_REC_CNT")
    )
)

# 9) xfm_Prcs_dt (Transformer) => output "Gnd_Lkp"
df_xfm_Prcs_dt = df_Agg_GrpTot_Amt.alias("Lnk_AggGrpTot").select(
    F.when(
        F.col("Lnk_AggGrpTot.SRC_SYS_CD") == F.lit("MEDTRAK"),
        F.col("Lnk_AggGrpTot.CLM_PRCS_DT_SK")
    ).otherwise(F.substring(F.lit(RunCycleDt), 1, 7)).alias("CLM_PRCS_DT_SK"),
    F.col("Lnk_AggGrpTot.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.lit("GRAND TOTAL").alias("RCRD_TYP_NM"),
    F.col("Lnk_AggGrpTot.CLM_ACTL_PD_AMT").alias("CLM_ACTL_PD_AMT"),
    F.col("Lnk_AggGrpTot.CLM_LN_TOT_COPAY_AMT").alias("CLM_LN_TOT_COPAY_AMT"),
    F.col("Lnk_AggGrpTot.DRUG_CLM_INGR_CST_ALW_AMT").alias("DRUG_CLM_INGR_CST_ALW_AMT"),
    F.col("Lnk_AggGrpTot.DRUG_CLM_RX_ALW_QTY").alias("DRUG_CLM_RX_ALW_QTY"),
    F.col("Lnk_AggGrpTot.GRND_REC_CNT").alias("GRND_REC_CNT")
)

# 10) Lkp_SCgrndTot_CLM: primary is xfm_Prcs_dt => "Gnd_Lkp", lookup is df_GrndTotPdx => left join
df_xfm_Prcs_dt_alias = df_xfm_Prcs_dt.alias("Gnd_Lkp")
df_GrndTotPdx_alias = df_GrndTotPdx.alias("Lnk_GrndTotSC")
cond_Lkp_SCgrndTot_CLM = [
    F.col("Gnd_Lkp.CLM_PRCS_DT_SK") == F.col("Lnk_GrndTotSC.FILE_DT_SK_YR_MN"),
    F.col("Gnd_Lkp.RCRD_TYP_NM") == F.col("Lnk_GrndTotSC.RCRD_TYP_NM"),
    F.col("Gnd_Lkp.SRC_SYS_CD") == F.col("Lnk_GrndTotSC.SRC_SYS_CD")
]
df_Lkp_SCgrndTot_CLM = (
    df_xfm_Prcs_dt_alias.join(df_GrndTotPdx_alias, cond_Lkp_SCgrndTot_CLM, how="left")
    .select(
        F.col("Lnk_GrndTotSC.FILE_DT_SK").alias("FILE_DT_SK"),
        F.col("Lnk_GrndTotSC.SRC_SYS_GRP_ID").alias("SRC_SYS_GRP_ID"),
        F.col("Lnk_GrndTotSC.RCRD_TYP_NM").alias("RCRD_TYP_NM"),
        F.col("Lnk_GrndTotSC.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Lnk_GrndTotSC.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.col("Lnk_GrndTotSC.GRP_ID").alias("GRP_ID"),
        F.col("Lnk_GrndTotSC.SRC_SYS_COV_NO").alias("SRC_SYS_COV_NO"),
        F.col("Lnk_GrndTotSC.RCVD_TOT_RCRD_CT").alias("RCVD_TOT_RCRD_CT"),
        F.col("Lnk_GrndTotSC.RCVD_TOT_INGR_CST_AMT").alias("RCVD_TOT_INGR_CST_AMT"),
        F.col("Lnk_GrndTotSC.RCVD_TOT_DRUG_ADM_FEE_AMT").alias("RCVD_TOT_DRUG_ADM_FEE_AMT"),
        F.col("Lnk_GrndTotSC.RCVD_TOT_COPAY_AMT").alias("RCVD_TOT_COPAY_AMT"),
        F.col("Lnk_GrndTotSC.RCVD_TOT_PD_AMT").alias("RCVD_TOT_PD_AMT"),
        F.col("Lnk_GrndTotSC.RCVD_TOT_QTY_CT").alias("RCVD_TOT_QTY_CT"),
        F.col("Lnk_GrndTotSC.CALC_TOT_RCRD_CT").alias("CALC_TOT_RCRD_CT"),
        F.col("Lnk_GrndTotSC.CALC_TOT_INGR_CST_AMT").alias("CALC_TOT_INGR_CST_AMT"),
        F.col("Lnk_GrndTotSC.CALC_TOT_DRUG_ADM_FEE_AMT").alias("CALC_TOT_DRUG_ADM_FEE_AMT"),
        F.col("Lnk_GrndTotSC.CALC_TOT_COPAY_AMT").alias("CALC_TOT_COPAY_AMT"),
        F.col("Lnk_GrndTotSC.CALC_TOT_PD_AMT").alias("CALC_TOT_PD_AMT"),
        F.col("Lnk_GrndTotSC.CALC_TOT_QTY_CT").alias("CALC_TOT_QTY_CT"),
        F.col("Gnd_Lkp.CLM_ACTL_PD_AMT").alias("CLM_ACTL_PD_AMT"),
        F.col("Gnd_Lkp.CLM_LN_TOT_COPAY_AMT").alias("CLM_LN_TOT_COPAY_AMT"),
        F.col("Gnd_Lkp.DRUG_CLM_INGR_CST_ALW_AMT").alias("DRUG_CLM_INGR_CST_ALW_AMT"),
        F.col("Gnd_Lkp.DRUG_CLM_RX_ALW_QTY").alias("DRUG_CLM_RX_ALW_QTY"),
        F.col("Gnd_Lkp.GRND_REC_CNT").alias("GRND_REC_CNT")
    )
)

# 11) Xfm_BusinessLogic => output link Lnk_GrndTotOut
df_Xfm_BusinessLogic = df_Lkp_SCgrndTot_CLM.alias("Lnk_GrndTot").select(
    F.when(
        F.trim(F.when(F.col("Lnk_GrndTot.FILE_DT_SK").isNotNull(), F.col("Lnk_GrndTot.FILE_DT_SK")).otherwise(F.lit(""))) == F.lit(""),
        F.lit("UNK")
    ).otherwise(F.col("Lnk_GrndTot.FILE_DT_SK")).alias("FILE_DT_SK"),
    F.when(
        F.trim(F.when(F.col("Lnk_GrndTot.SRC_SYS_GRP_ID").isNotNull(), F.col("Lnk_GrndTot.SRC_SYS_GRP_ID")).otherwise(F.lit(""))) == F.lit(""),
        F.lit("")
    ).otherwise(F.col("Lnk_GrndTot.SRC_SYS_GRP_ID")).alias("SRC_SYS_GRP_ID"),
    F.when(
        F.trim(F.when(F.col("Lnk_GrndTot.RCRD_TYP_NM").isNotNull(), F.col("Lnk_GrndTot.RCRD_TYP_NM")).otherwise(F.lit(""))) == F.lit(""),
        F.lit("")
    ).otherwise(F.col("Lnk_GrndTot.RCRD_TYP_NM")).alias("RCRD_TYP_NM"),
    F.when(
        F.trim(F.when(F.col("Lnk_GrndTot.SRC_SYS_CD").isNotNull(), F.col("Lnk_GrndTot.SRC_SYS_CD")).otherwise(F.lit(""))) == F.lit(""),
        F.lit("")
    ).otherwise(F.col("Lnk_GrndTot.SRC_SYS_CD")).alias("SRC_SYS_CD"),
    F.when(
        F.trim(F.when(F.col("Lnk_GrndTot.CRT_RUN_CYC_EXCTN_DT_SK").isNotNull(), F.col("Lnk_GrndTot.CRT_RUN_CYC_EXCTN_DT_SK")).otherwise(F.lit(""))) == F.lit(""),
        F.lit(RunCycleDt)
    ).otherwise(F.col("Lnk_GrndTot.CRT_RUN_CYC_EXCTN_DT_SK")).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.when(
        F.trim(F.when(F.lit(RunCycleDt).isNotNull(), F.lit(RunCycleDt)).otherwise(F.lit(""))) == F.lit(""),
        F.lit("")
    ).otherwise(F.lit(RunCycleDt)).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.when(
        F.trim(F.when(F.col("Lnk_GrndTot.GRP_ID").isNotNull(), F.col("Lnk_GrndTot.GRP_ID")).otherwise(F.lit(""))) == F.lit(""),
        F.lit("")
    ).otherwise(
        F.when(F.col("Lnk_GrndTot.RCRD_TYP_NM") == F.lit("GRAND TOTAL"), F.lit("")).otherwise(F.col("Lnk_GrndTot.GRP_ID"))
    ).alias("GRP_ID"),
    F.when(
        F.trim(F.when(F.col("Lnk_GrndTot.SRC_SYS_COV_NO").isNotNull(), F.col("Lnk_GrndTot.SRC_SYS_COV_NO")).otherwise(F.lit(""))) == F.lit(""),
        F.lit("")
    ).otherwise(F.col("Lnk_GrndTot.SRC_SYS_COV_NO")).alias("SRC_SYS_COV_NO"),
    F.when(F.coalesce(F.col("Lnk_GrndTot.RCVD_TOT_RCRD_CT"), F.lit(0)) == 0, F.lit(0)).otherwise(F.col("Lnk_GrndTot.RCVD_TOT_RCRD_CT")).alias("RCVD_TOT_RCRD_CT"),
    F.when(F.coalesce(F.col("Lnk_GrndTot.RCVD_TOT_INGR_CST_AMT"), F.lit(0)) == 0, F.lit(0)).otherwise(F.col("Lnk_GrndTot.RCVD_TOT_INGR_CST_AMT")).alias("RCVD_TOT_INGR_CST_AMT"),
    F.when(F.coalesce(F.col("Lnk_GrndTot.RCVD_TOT_DRUG_ADM_FEE_AMT"), F.lit(0)) == 0, F.lit(0)).otherwise(F.col("Lnk_GrndTot.RCVD_TOT_DRUG_ADM_FEE_AMT")).alias("RCVD_TOT_DRUG_ADM_FEE_AMT"),
    F.when(F.coalesce(F.col("Lnk_GrndTot.RCVD_TOT_COPAY_AMT"), F.lit(0)) == 0, F.lit(0)).otherwise(F.col("Lnk_GrndTot.RCVD_TOT_COPAY_AMT")).alias("RCVD_TOT_COPAY_AMT"),
    F.when(F.coalesce(F.col("Lnk_GrndTot.RCVD_TOT_PD_AMT"), F.lit(0)) == 0, F.lit(0)).otherwise(F.col("Lnk_GrndTot.RCVD_TOT_PD_AMT")).alias("RCVD_TOT_PD_AMT"),
    F.when(F.coalesce(F.col("Lnk_GrndTot.RCVD_TOT_QTY_CT"), F.lit(0)) == 0, F.lit(0)).otherwise(F.col("Lnk_GrndTot.RCVD_TOT_QTY_CT")).alias("RCVD_TOT_QTY_CT"),
    F.when(F.coalesce(F.col("Lnk_GrndTot.GRND_REC_CNT"), F.lit(0)) == 0, F.lit(0)).otherwise(F.col("Lnk_GrndTot.GRND_REC_CNT")).alias("CALC_TOT_RCRD_CT"),
    F.when(F.coalesce(F.col("Lnk_GrndTot.DRUG_CLM_INGR_CST_ALW_AMT"), F.lit(0)) == 0, F.lit(0)).otherwise(F.col("Lnk_GrndTot.DRUG_CLM_INGR_CST_ALW_AMT")).alias("CALC_TOT_INGR_CST_AMT"),
    F.lit(None).alias("CALC_TOT_DRUG_ADM_FEE_AMT"),
    F.when(F.coalesce(F.col("Lnk_GrndTot.CLM_LN_TOT_COPAY_AMT"), F.lit(0)) == 0, F.lit(0)).otherwise(F.col("Lnk_GrndTot.CLM_LN_TOT_COPAY_AMT")).alias("CALC_TOT_COPAY_AMT"),
    F.when(F.coalesce(F.col("Lnk_GrndTot.CLM_ACTL_PD_AMT"), F.lit(0)) == 0, F.lit(0)).otherwise(F.col("Lnk_GrndTot.CLM_ACTL_PD_AMT")).alias("CALC_TOT_PD_AMT"),
    F.when(F.coalesce(F.col("Lnk_GrndTot.DRUG_CLM_RX_ALW_QTY"), F.lit(0)) == 0, F.lit(0)).otherwise(F.col("Lnk_GrndTot.DRUG_CLM_RX_ALW_QTY")).alias("CALC_TOT_QTY_CT")
)

# 12) Now read PdxClm from EDW
sql_PdxClm = f"""
SELECT DISTINCT
  LEFT(FILE_DT_SK,7) AS FILE_DT_SK,
  FILE_DT_SK AS FILE_DT_SK_WITH_DT,
  GRP_ID,
  SRC_SYS_GRP_ID
FROM {EDWOwner}.GRP_STOP_LOSS_PDX_CLM_RCVD_MNTHLY_SUM
WHERE
  SRC_SYS_CD = '{SrcSysCd}'
  AND LEFT(FILE_DT_SK,7) = CASE WHEN '{FILE_DATE}' = '2199-12'
    THEN (
      SELECT MAX(LEFT(FILE_DT_SK,7))
      FROM {EDWOwner}.GRP_STOP_LOSS_PDX_CLM_RCVD_MNTHLY_SUM
      WHERE SRC_SYS_CD = '{SrcSysCd}'
        AND RCRD_TYP_NM <> 'GRAND TOTAL'
    )
    ELSE '{FILE_DATE}' END
  AND FILE_DT_SK = (
    SELECT MAX(FILE_DT_SK)
    FROM {EDWOwner}.GRP_STOP_LOSS_PDX_CLM_RCVD_MNTHLY_SUM
    WHERE SRC_SYS_CD = '{SrcSysCd}'
      AND RCRD_TYP_NM = 'GROUP TOTAL'
  )
  AND RCRD_TYP_NM NOT IN ('GRAND TOTAL', 'GROUP TOTAL')
ORDER BY FILE_DT_SK, GRP_ID, SRC_SYS_GRP_ID DESC
"""
df_PdxClm = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", sql_PdxClm)
    .load()
)

# 13) Cpy_PBM_GrpTot => two outputs
df_Cpy_PBM_GrpTot_Lnk_PdxClm = df_PdxClm.select(
    F.col("FILE_DT_SK").alias("FILE_DT_SK"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("SRC_SYS_GRP_ID").alias("SRC_SYS_GRP_ID")
)
df_Cpy_PBM_GrpTot_PdxClmDt1 = df_PdxClm.select(
    F.col("FILE_DT_SK").alias("FILE_DT_SK"),
    F.col("FILE_DT_SK_WITH_DT").alias("FILE_DT_SK_WITH_DT")
)

# 14) RmvDup_ClmDt (PxRemDup) => dedup FILE_DT_SK => keep first
df_dedup_ClmDt = dedup_sort(
    df_Cpy_PBM_GrpTot_PdxClmDt1,
    partition_cols=["FILE_DT_SK"],
    sort_cols=[("FILE_DT_SK", "A")]
)

# 15) Read from IDS: PbmGrpXref
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
sql_PbmGrpXref = f"""
SELECT
  SRC_SYS_CD,
  PBM_GRP_ID,
  GRP_ID
FROM {IDSOwner}.P_PBM_GRP_XREF XREF
WHERE
  UPPER(XREF.SRC_SYS_CD) = '{SrcSysCd.upper()}'
  AND '{RunCycleDt}' BETWEEN DATE(XREF.EFF_DT) AND DATE(XREF.TERM_DT)
"""
df_PbmGrpXref = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", sql_PbmGrpXref)
    .load()
)

# 16) CommonPDXSrcFile => read from a file (PxSequentialFile) => we define its schema
schema_CommonPDXSrcFile = T.StructType([
    T.StructField("SRC_SYS_CD", T.StringType(), nullable=False),
    T.StructField("GRP_BASE", T.StringType(), nullable=False),
    T.StructField("PRCS_DT", T.StringType(), nullable=False)
])
# For lack of explicit path in JSON, treat as "other path" => f"{adls_path}/CommonPDXSrcFile.dat"
file_path_CommonPDXSrcFile = f"{adls_path}/CommonPDXSrcFile.dat"
df_CommonPDXSrcFile = (
    spark.read
    .option("header", "false")
    .option("delimiter", "|")
    .option("quote", "\"")
    .schema(schema_CommonPDXSrcFile)
    .csv(file_path_CommonPDXSrcFile)
)

# 17) Agg_GrpBy => group by (SRC_SYS_CD, GRP_BASE, PRCS_DT) => count
df_Agg_GrpBy = (
    df_CommonPDXSrcFile.groupBy("SRC_SYS_CD", "GRP_BASE", "PRCS_DT")
    .agg(F.count(F.lit(1)).alias("Dummy"))
)

# 18) Lkp_GrpRelEnty => primary df_Agg_GrpBy => left join with df_PbmGrpXref on (SRC_SYS_CD -> SRC_SYS_CD, GRP_BASE -> PBM_GRP_ID)
df_Agg_GrpBy_alias = df_Agg_GrpBy.alias("Lnk_GrpBy")
df_PbmGrpXref_alias = df_PbmGrpXref.alias("Xref")
cond_GrpRelEnty = [
    F.col("Lnk_GrpBy.SRC_SYS_CD") == F.col("Xref.SRC_SYS_CD"),
    F.col("Lnk_GrpBy.GRP_BASE") == F.col("Xref.PBM_GRP_ID")
]
df_Lkp_GrpRelEnty = (
    df_Agg_GrpBy_alias.join(df_PbmGrpXref_alias, cond_GrpRelEnty, how="left")
    .select(
        F.col("Lnk_GrpBy.GRP_BASE").alias("GRP_BASE"),
        F.col("Lnk_GrpBy.PRCS_DT").alias("PRCS_DT"),
        F.col("Xref.GRP_ID").alias("GRP_XREF_GRP_ID")
    )
)

# 19) Xfm_GrpId => transform with stage var => output link "All" and "GrpBase"
df_Xfm_GrpId_in = df_Lkp_GrpRelEnty.alias("Lnk_EnrichGrpID")
df_Xfm_GrpId = df_Xfm_GrpId_in.select(
    F.col("Lnk_EnrichGrpID.GRP_BASE").alias("GRP_BASE"),
    F.col("Lnk_EnrichGrpID.PRCS_DT").alias("PRCS_DT"),
    F.when(
        (F.col("Lnk_EnrichGrpID.GRP_XREF_GRP_ID").isNull()) | (F.length(F.trim(F.col("Lnk_EnrichGrpID.GRP_XREF_GRP_ID"))) == 0),
        F.lit("UNK")
    ).otherwise(F.trim(F.col("Lnk_EnrichGrpID.GRP_XREF_GRP_ID"))).alias("GRP_ID")
)
df_Xfm_GrpId_All = df_Xfm_GrpId.select(
    F.col("GRP_BASE").alias("GRP_BASE"),
    F.col("PRCS_DT").alias("PRCS_DT"),
    F.col("GRP_ID").alias("GRP_ID")
)
df_Xfm_GrpId_GrpBase = df_Xfm_GrpId.select(
    F.col("GRP_BASE").alias("GRP_BASE"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("PRCS_DT").alias("PRCS_DT")
)

# 20) RmvDup_GrpID => dedup on (PRCS_DT, GRP_ID) => keep last
df_dedup_GrpID = dedup_sort(
    df_Xfm_GrpId_GrpBase,
    partition_cols=["PRCS_DT", "GRP_ID"],
    sort_cols=[("PRCS_DT", "A"), ("GRP_ID", "A"), ("GRP_BASE", "A")]
)
# 21) Lkp_StopLossPdx => primary: df_Xfm_GrpId_All => lookups: df_dedup_ClmDt, df_Cpy_PBM_GrpTot_Lnk_PdxClm, df_dedup_GrpID
# We do stepwise left joins.
df_1 = df_Xfm_GrpId_All.alias("All")
df_2 = df_dedup_ClmDt.alias("PdxClmDt")
cond_1 = [F.col("All.PRCS_DT") == F.col("PdxClmDt.FILE_DT_SK")]
joined_1 = df_1.join(df_2, cond_1, how="left").select(
    F.col("All.GRP_BASE").alias("GRP_BASE"),
    F.col("All.PRCS_DT").alias("PRCS_DT"),
    F.col("All.GRP_ID").alias("GRP_ID"),
    F.col("PdxClmDt.FILE_DT_SK_WITH_DT").alias("FILE_DT_SK_WITH_DT")
)
df_3 = df_Cpy_PBM_GrpTot_Lnk_PdxClm.alias("Lnk_PdxClm")
cond_2 = [
    F.col("joined_1.PRCS_DT") == F.col("Lnk_PdxClm.FILE_DT_SK"),
    F.col("joined_1.GRP_BASE") == F.col("Lnk_PdxClm.SRC_SYS_GRP_ID")
]
joined_2 = joined_1.alias("joined_1").join(df_3, cond_2, how="left").select(
    F.col("joined_1.GRP_BASE").alias("GRP_BASE"),
    F.col("joined_1.PRCS_DT").alias("PRCS_DT"),
    F.col("joined_1.GRP_ID").alias("GRP_ID"),
    F.col("joined_1.FILE_DT_SK_WITH_DT").alias("FILE_DT_SK_WITH_DT"),
    F.col("Lnk_PdxClm.FILE_DT_SK").alias("FILE_DT_SK_CNTL"),
    F.col("Lnk_PdxClm.GRP_ID").alias("GRP_ID_CNTL"),
    F.col("Lnk_PdxClm.SRC_SYS_GRP_ID").alias("SRC_SYS_GRP_ID_CNTL")
)
df_4 = df_dedup_GrpID.alias("Lnk_MaxGrpBase")
cond_3 = [
    F.col("joined_2.GRP_ID") == F.col("Lnk_MaxGrpBase.GRP_ID"),
    F.col("joined_2.PRCS_DT") == F.col("Lnk_MaxGrpBase.PRCS_DT")
]
df_Lkp_StopLossPdx = joined_2.alias("joined_2").join(df_4, cond_3, how="left").select(
    F.col("joined_2.GRP_BASE").alias("GRP_BASE"),
    F.col("joined_2.PRCS_DT").alias("PRCS_DT"),
    F.col("joined_2.GRP_ID").alias("GRP_ID"),
    F.col("Lnk_MaxGrpBase.GRP_BASE").alias("GRP_BASE_MAX"),
    F.col("joined_2.FILE_DT_SK_CNTL").alias("FILE_DT_SK_CNTL"),
    F.col("joined_2.FILE_DT_SK_WITH_DT").alias("FILE_DT_SK_WITH_DT"),
    F.col("joined_2.GRP_ID_CNTL").alias("GRP_ID_CNTL"),
    F.col("joined_2.SRC_SYS_GRP_ID_CNTL").alias("SRC_SYS_GRP_ID_CNTL")
)

# 22) Set_GrpBase => transform => output "AddGrpBase" with constraint
df_Set_GrpBase_in = df_Lkp_StopLossPdx.alias("Chk_GrpBase")
df_Set_GrpBase = df_Set_GrpBase_in.select(
    F.when(
        F.trim(F.when(F.col("Chk_GrpBase.GRP_BASE").isNotNull(), F.col("Chk_GrpBase.GRP_BASE")).otherwise(F.lit(""))) == F.lit(""),
        F.lit("")
    ).otherwise(F.trim(F.col("Chk_GrpBase.GRP_BASE"))).alias("GRP_BASE"),
    F.when(
        F.trim(F.when(F.col("Chk_GrpBase.PRCS_DT").isNotNull(), F.col("Chk_GrpBase.PRCS_DT")).otherwise(F.lit(""))) == F.lit(""),
        F.lit("")
    ).otherwise(F.trim(F.col("Chk_GrpBase.PRCS_DT"))).alias("PRCS_DT"),
    F.when(
        F.trim(F.when(F.col("Chk_GrpBase.FILE_DT_SK_WITH_DT").isNotNull(), F.col("Chk_GrpBase.FILE_DT_SK_WITH_DT")).otherwise(F.lit(""))) == F.lit(""),
        F.lit("")
    ).otherwise(F.trim(F.col("Chk_GrpBase.FILE_DT_SK_WITH_DT"))).alias("FILE_DT_SK_WITH_DT"),
    F.when(
        F.trim(F.when(F.col("Chk_GrpBase.GRP_ID").isNotNull(), F.col("Chk_GrpBase.GRP_ID")).otherwise(F.lit(""))) == F.lit(""),
        F.lit("")
    ).otherwise(F.col("Chk_GrpBase.GRP_ID")).alias("GRP_ID"),
    F.when(
        F.trim(F.col("Chk_GrpBase.GRP_BASE_MAX")) == F.lit(""),
        F.lit("")
    ).otherwise(F.concat(F.expr("split(trim(Chk_GrpBase.GRP_BASE_MAX), '\\.')[0]"), F.lit(" NICF"))).alias("GRP_BASE_MAX"),
    F.lit("GROUP RX").alias("RCRD_TYP_NM")
).filter(
    (F.trim(F.col("Chk_GrpBase.PRCS_DT")) != F.trim(F.col("Chk_GrpBase.FILE_DT_SK_CNTL"))) |
    (F.trim(F.col("Chk_GrpBase.GRP_ID")) != F.trim(F.col("Chk_GrpBase.GRP_ID_CNTL"))) |
    (F.trim(F.col("Chk_GrpBase.GRP_BASE")) != F.trim(F.col("Chk_GrpBase.SRC_SYS_GRP_ID_CNTL")))
)

# 23) Cpy_GrpTot => two outputs
df_Cpy_GrpTot_in = df_Set_GrpBase.alias("AddGrpBase")
df_Cpy_GrpTot_Lnk_NonGrpTot = df_Cpy_GrpTot_in.select(
    F.col("AddGrpBase.PRCS_DT").alias("PRCS_DT"),
    F.col("AddGrpBase.GRP_ID").alias("GRP_ID"),
    F.col("AddGrpBase.GRP_BASE").alias("GRP_BASE"),
    F.col("AddGrpBase.FILE_DT_SK_WITH_DT").alias("FILE_DT_SK_WITH_DT"),
    F.col("AddGrpBase.RCRD_TYP_NM").alias("RCRD_TYP_NM")
)
df_Cpy_GrpTot_Lnk_GenGrpTot = df_Cpy_GrpTot_in.select(
    F.col("AddGrpBase.PRCS_DT").alias("PRCS_DT"),
    F.col("AddGrpBase.GRP_ID").alias("GRP_ID"),
    F.col("AddGrpBase.GRP_BASE_MAX").alias("GRP_BASE"),
    F.col("AddGrpBase.FILE_DT_SK_WITH_DT").alias("FILE_DT_SK_WITH_DT")
)

# 24) Sort_MaxGrpBase => sort by (PRCS_DT, GRP_ID, GRP_BASE)
df_Sort_MaxGrpBase = df_Cpy_GrpTot_Lnk_GenGrpTot.sort(
    F.col("PRCS_DT").asc(),
    F.col("GRP_ID").asc(),
    F.col("GRP_BASE").asc()
)

# 25) Xfm_AddRcrdTypNm => output => Lnk_FilledRcrdTypNm
df_Xfm_AddRcrdTypNm = df_Sort_MaxGrpBase.select(
    F.col("PRCS_DT").alias("PRCS_DT"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("GRP_BASE").alias("GRP_BASE"),
    F.col("FILE_DT_SK_WITH_DT").alias("FILE_DT_SK_WITH_DT"),
    F.lit("GROUP TOTAL").alias("RCRD_TYP_NM")
)

# 26) All_Missing funnel => union of Lnk_NonGrpTot, Lnk_FilledRcrdTypNm
commonCols_all_missing = ["PRCS_DT","GRP_ID","GRP_BASE","FILE_DT_SK_WITH_DT","RCRD_TYP_NM"]
df_All_missing = df_Cpy_GrpTot_Lnk_NonGrpTot.select(commonCols_all_missing).unionByName(
    df_Xfm_AddRcrdTypNm.select(commonCols_all_missing)
)

# 27) Lkp_SCmissingGrpBase => primary df_All_missing => lookup df_CLM_SC_Lnk_All => left join on (PRCS_DT=FILE_DT_SK_YRMN, GRP_ID=GRP_ID)
df_CLM_SC_Lnk_All_alias = df_CLM_SC_Lnk_All.alias("Lnk_All")
df_All_missing_alias = df_All_missing.alias("Lnk_NoMatch")
cond_SCmissingGrpBase = [
    F.col("Lnk_NoMatch.PRCS_DT") == F.col("Lnk_All.FILE_DT_SK_YRMN"),
    F.col("Lnk_NoMatch.GRP_ID") == F.col("Lnk_All.GRP_ID")
]
df_Lkp_SCmissingGrpBase = (
    df_All_missing_alias.join(df_CLM_SC_Lnk_All_alias, cond_SCmissingGrpBase, how="left")
    .select(
        F.col("Lnk_NoMatch.FILE_DT_SK_WITH_DT").alias("FILE_DT_SK"),
        F.col("Lnk_NoMatch.GRP_ID").alias("GRP_ID"),
        F.col("Lnk_NoMatch.GRP_BASE").alias("SRC_SYS_GRP_ID"),
        F.col("Lnk_NoMatch.RCRD_TYP_NM").alias("RCRD_TYP_NM"),
        F.col("Lnk_All.CALC_TOT_RCRD_CT").alias("CALC_TOT_RCRD_CT"),
        F.col("Lnk_All.CALC_TOT_INGR_CST_AMT").alias("CALC_TOT_INGR_CST_AMT"),
        F.col("Lnk_All.CALC_TOT_DRUG_ADM_FEE_AMT").alias("CALC_TOT_DRUG_ADM_FEE_AMT"),
        F.col("Lnk_All.CALC_TOT_COPAY_AMT").alias("CALC_TOT_COPAY_AMT"),
        F.col("Lnk_All.CALC_TOT_PD_AMT").alias("CALC_TOT_PD_AMT"),
        F.col("Lnk_All.CALC_TOT_QTY_CT").alias("CALC_TOT_QTY_CT")
    )
)

# 28) Xfm_InsertGrpTot => transform => two outputs: Lnk_InsertMissingGrpTot, Lnk_InsertMissingNonGrpTot
df_ins_in = df_Lkp_SCmissingGrpBase.alias("Lnk_GrpTot")
col_FILE_DT_SK = F.when(
    F.trim(F.when(F.col("Lnk_GrpTot.FILE_DT_SK").isNotNull(), F.col("Lnk_GrpTot.FILE_DT_SK")).otherwise(F.lit(""))) == F.lit(""),
    F.lit("UNK")
).otherwise(F.col("Lnk_GrpTot.FILE_DT_SK")).alias("FILE_DT_SK")
col_SRC_SYS_GRP_ID = F.when(
    F.trim(F.when(F.col("Lnk_GrpTot.SRC_SYS_GRP_ID").isNotNull(), F.col("Lnk_GrpTot.SRC_SYS_GRP_ID")).otherwise(F.lit(""))) == F.lit(""),
    F.lit("")
).otherwise(F.col("Lnk_GrpTot.SRC_SYS_GRP_ID")).alias("SRC_SYS_GRP_ID")
col_RCRD_TYP_NM = F.when(
    F.trim(F.when(F.col("Lnk_GrpTot.RCRD_TYP_NM").isNotNull(), F.col("Lnk_GrpTot.RCRD_TYP_NM")).otherwise(F.lit(""))) == F.lit(""),
    F.lit("")
).otherwise(F.col("Lnk_GrpTot.RCRD_TYP_NM")).alias("RCRD_TYP_NM")
col_SRC_SYS_CD = F.lit(SrcSysCd).alias("SRC_SYS_CD")
col_CRT_RUN = F.when(
    F.trim(F.when(F.lit(RunCycleDt).isNotNull(), F.lit(RunCycleDt)).otherwise(F.lit(""))) == F.lit(""),
    F.lit("")
).otherwise(F.lit(RunCycleDt)).alias("CRT_RUN_CYC_EXCTN_DT_SK")
col_LAST_UPDT = F.when(
    F.trim(F.when(F.lit(RunCycleDt).isNotNull(), F.lit(RunCycleDt)).otherwise(F.lit(""))) == F.lit(""),
    F.lit("")
).otherwise(F.lit(RunCycleDt)).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK")
col_GRP_ID = F.when(
    F.trim(F.when(F.col("Lnk_GrpTot.GRP_ID").isNotNull(), F.col("Lnk_GrpTot.GRP_ID")).otherwise(F.lit(""))) == F.lit(""),
    F.lit("")
).otherwise(F.col("Lnk_GrpTot.GRP_ID")).alias("GRP_ID")
col_SRC_SYS_COV_NO = F.lit("UNK").alias("SRC_SYS_COV_NO")
col_RCVD_TOT_RCRD_CT = F.lit(0).alias("RCVD_TOT_RCRD_CT")
col_RCVD_TOT_INGR_CST_AMT = F.lit(0.00).alias("RCVD_TOT_INGR_CST_AMT")
col_RCVD_TOT_DRUG_ADM_FEE_AMT = F.lit(0.00).alias("RCVD_TOT_DRUG_ADM_FEE_AMT")
col_RCVD_TOT_COPAY_AMT = F.lit(0.00).alias("RCVD_TOT_COPAY_AMT")
col_RCVD_TOT_PD_AMT = F.lit(0.00).alias("RCVD_TOT_PD_AMT")
col_RCVD_TOT_QTY_CT = F.lit(0.00).alias("RCVD_TOT_QTY_CT")
col_CALC_TOT_RCRD_CT = F.when(F.coalesce(F.col("Lnk_GrpTot.CALC_TOT_RCRD_CT"), F.lit(0))==0,F.lit(0)).otherwise(F.col("Lnk_GrpTot.CALC_TOT_RCRD_CT")).alias("CALC_TOT_RCRD_CT")
col_CALC_TOT_INGR_CST_AMT = F.when(F.coalesce(F.col("Lnk_GrpTot.CALC_TOT_INGR_CST_AMT"), F.lit(0))==0,F.lit(0)).otherwise(F.col("Lnk_GrpTot.CALC_TOT_INGR_CST_AMT")).alias("CALC_TOT_INGR_CST_AMT")
col_CALC_TOT_DRUG_ADM_FEE_AMT = F.when(F.coalesce(F.col("Lnk_GrpTot.CALC_TOT_DRUG_ADM_FEE_AMT"), F.lit(0))==0,F.lit(0)).otherwise(F.col("Lnk_GrpTot.CALC_TOT_DRUG_ADM_FEE_AMT")).alias("CALC_TOT_DRUG_ADM_FEE_AMT")
col_CALC_TOT_COPAY_AMT = F.when(F.coalesce(F.col("Lnk_GrpTot.CALC_TOT_COPAY_AMT"), F.lit(0))==0,F.lit(0)).otherwise(F.col("Lnk_GrpTot.CALC_TOT_COPAY_AMT")).alias("CALC_TOT_COPAY_AMT")
col_CALC_TOT_PD_AMT = F.when(F.coalesce(F.col("Lnk_GrpTot.CALC_TOT_PD_AMT"), F.lit(0))==0,F.lit(0)).otherwise(F.col("Lnk_GrpTot.CALC_TOT_PD_AMT")).alias("CALC_TOT_PD_AMT")
col_CALC_TOT_QTY_CT = F.when(F.coalesce(F.col("Lnk_GrpTot.CALC_TOT_QTY_CT"), F.lit(0))==0,F.lit(0)).otherwise(F.col("Lnk_GrpTot.CALC_TOT_QTY_CT")).alias("CALC_TOT_QTY_CT")

svGrpTot = F.when(F.trim(F.col("Lnk_GrpTot.RCRD_TYP_NM")) == F.lit("GROUP TOTAL"), F.lit("Y")).otherwise(F.lit("N")).alias("svGrpTot")

df_xfm_ins_temp = df_ins_in.select(
    svGrpTot,
    col_FILE_DT_SK, col_SRC_SYS_GRP_ID, col_RCRD_TYP_NM, col_SRC_SYS_CD,
    col_CRT_RUN, col_LAST_UPDT, col_GRP_ID, col_SRC_SYS_COV_NO,
    col_RCVD_TOT_RCRD_CT, col_RCVD_TOT_INGR_CST_AMT, col_RCVD_TOT_DRUG_ADM_FEE_AMT,
    col_RCVD_TOT_COPAY_AMT, col_RCVD_TOT_PD_AMT, col_RCVD_TOT_QTY_CT,
    col_CALC_TOT_RCRD_CT, col_CALC_TOT_INGR_CST_AMT, col_CALC_TOT_DRUG_ADM_FEE_AMT,
    col_CALC_TOT_COPAY_AMT, col_CALC_TOT_PD_AMT, col_CALC_TOT_QTY_CT
).alias("transformInsert")

df_Xfm_InsertGrpTot_Grp = df_xfm_ins_temp.filter(F.col("svGrpTot") == "Y").drop("svGrpTot")
df_Xfm_InsertGrpTot_NonGrp = df_xfm_ins_temp.filter(F.col("svGrpTot") == "N").drop("svGrpTot")

# 29) Funnel_All => union of Xfm_BusinessLogic(Lnk_GrndTotOut), CLM_SC_MatchUnmatch(Lnk_updateGrpTot), Xfm_InsertGrpTot(Lnk_InsertMissingGrpTot, Lnk_InsertMissingNonGrpTot)
commonCols_funnel = [
    "FILE_DT_SK","SRC_SYS_GRP_ID","RCRD_TYP_NM","SRC_SYS_CD","CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK","GRP_ID","SRC_SYS_COV_NO","RCVD_TOT_RCRD_CT","RCVD_TOT_INGR_CST_AMT",
    "RCVD_TOT_DRUG_ADM_FEE_AMT","RCVD_TOT_COPAY_AMT","RCVD_TOT_PD_AMT","RCVD_TOT_QTY_CT",
    "CALC_TOT_RCRD_CT","CALC_TOT_INGR_CST_AMT","CALC_TOT_DRUG_ADM_FEE_AMT","CALC_TOT_COPAY_AMT","CALC_TOT_PD_AMT","CALC_TOT_QTY_CT"
]

df_Funnel_link1 = df_Xfm_BusinessLogic.select(commonCols_funnel)
df_Funnel_link2 = df_CLM_SC_Lnk_updateGrpTot.select(commonCols_funnel)
df_Funnel_link3 = df_Xfm_InsertGrpTot_Grp.select(commonCols_funnel)
df_Funnel_link4 = df_Xfm_InsertGrpTot_NonGrp.select(commonCols_funnel)

df_Funnel_All = df_Funnel_link1.unionByName(df_Funnel_link2) \
    .unionByName(df_Funnel_link3) \
    .unionByName(df_Funnel_link4)

# 30) Final output => CALC_FIELDS_GRP_STOP_LOSS_PDX_CLM_RCVD_MNTHLY_SUM (PxSequentialFile)
#     We must preserve column order and apply rpad for any char/varchar columns with known length=10
final_cols = [
    "FILE_DT_SK","SRC_SYS_GRP_ID","RCRD_TYP_NM","SRC_SYS_CD","CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK","GRP_ID","SRC_SYS_COV_NO","RCVD_TOT_RCRD_CT","RCVD_TOT_INGR_CST_AMT",
    "RCVD_TOT_DRUG_ADM_FEE_AMT","RCVD_TOT_COPAY_AMT","RCVD_TOT_PD_AMT","RCVD_TOT_QTY_CT",
    "CALC_TOT_RCRD_CT","CALC_TOT_INGR_CST_AMT","CALC_TOT_DRUG_ADM_FEE_AMT","CALC_TOT_COPAY_AMT","CALC_TOT_PD_AMT","CALC_TOT_QTY_CT"
]
df_final = df_Funnel_All.select(final_cols)

# rpad for FILE_DT_SK and any other char(10) columns
df_final = df_final.withColumn(
    "FILE_DT_SK", F.rpad(F.col("FILE_DT_SK"), 10, " ")
).withColumn(
    "CRT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
)

# Write the final file as .dat, overwrite mode, no header, comma-delimited
final_file_path = f"{adls_path}/load/{SrcSysCd}_CALCAMT_GRP_STOP_LOSS_PDX_CLM_RCVD_MNTHLY_SUM.dat"

write_files(
    df_final,
    final_file_path,
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)