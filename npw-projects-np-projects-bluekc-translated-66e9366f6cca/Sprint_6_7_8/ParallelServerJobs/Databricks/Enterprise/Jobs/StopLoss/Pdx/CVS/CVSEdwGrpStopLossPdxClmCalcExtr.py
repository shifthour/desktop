# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-26
# MAGIC """
# MAGIC Â© Copyright 2008 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by:
# MAGIC                     EdwEdwGrpStopLossPdxClmMnthlyCntl
# MAGIC 
# MAGIC PROCESSING : Update Calculated field Value and Generate load records from CVSsource File to GRP_STOP_LOSS_PDX_CLM_RCVD_MNTHLY_SUM where SRC_SYS_GRP_ID = ' ' 
# MAGIC 
# MAGIC Modifications:                        
# MAGIC  Developer                                                Date                   Project/Altiris #                                 Change Description                                       Development Project       Code Reviewer            Date Reviewed
# MAGIC ---------------------------------------                          -------------------        ---------------------------                              -----------------------------------                                  ----------------------------------      -------------------------           -------------------------   
# MAGIC Kalyan Neelam                                               2018-11-09       5828- Stop Loss-Calc Fields              Original Development                                   EnterpriseDev2                          Abhiram Dasarathy        2018-11-12

# MAGIC CLM PDX Agg Amt from CLM_F
# MAGIC The load  file is used by the EdwEdwStopLossPdxLoad job to load EDW DB GRP_STOP_LOSS_PDX_CLM_RCVD_MNTHLY_SUM Table
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DoubleType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
RunCycleDt = get_widget_value('RunCycleDt','')
InFileName = get_widget_value('InFileName','')
FILE_DATE = get_widget_value('FILE_DATE','')
SrcSysCd = get_widget_value('SrcSysCd','')

jdbc_url, jdbc_props = get_db_config(edw_secret_name)
extract_query_GrndTotPdx = f"""
SELECT
  SUBSTRING(FILE_DT_SK,1,7) AS FILE_DT_SK_YR_MN,
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
  SRC_SYS_CD = '{SrcSysCd}'
  AND SUBSTRING(FILE_DT_SK,1,7) = CASE
    WHEN '{FILE_DATE}' = '2199-12' THEN (
      SELECT MAX(SUBSTRING(FILE_DT_SK,1,7))
      FROM {EDWOwner}.GRP_STOP_LOSS_PDX_CLM_RCVD_MNTHLY_SUM
      WHERE SRC_SYS_CD = '{SrcSysCd}'
    )
    ELSE '{FILE_DATE}'
  END
"""
df_GrndTotPdx = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_GrndTotPdx)
    .load()
)

extract_query_CLM_AggAmt = f"""
SELECT
  clm.GRP_SK,
  SUBSTRING(CLM_RCVD_YR_MO_SK,1,4)+'-'+SUBSTRING(CLM_RCVD_YR_MO_SK,5,2) AS CLM_PRCS_DT_SK,
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
WHERE
  clm.SRC_SYS_CD = '{SrcSysCd}'
  AND clm.CLM_SUBTYP_CD = 'RX'
  AND SUBSTRING(CLM_RCVD_YR_MO_SK,1,4)+'-'+SUBSTRING(CLM_RCVD_YR_MO_SK,5,2) = CASE
    WHEN '{FILE_DATE}' = '2199-12' THEN (
      SELECT MAX(SUBSTRING(FILE_DT_SK,1,7))
      FROM {EDWOwner}.GRP_STOP_LOSS_PDX_CLM_RCVD_MNTHLY_SUM
      WHERE SRC_SYS_CD = '{SrcSysCd}' AND RCRD_TYP_NM = 'GRAND TOTAL'
    )
    ELSE '{FILE_DATE}'
  END
GROUP BY
  clm.GRP_SK,
  clm.SRC_SYS_CD,
  clm.GRP_ID,
  clm.CLM_SUBTYP_CD,
  CLM_RCVD_YR_MO_SK
"""
df_CLM_AggAmt = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_CLM_AggAmt)
    .load()
)

df_cpy_clmAggAmt = df_CLM_AggAmt.select(
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

df_Agg_GrpTot_Amt = (
    df_cpy_clmAggAmt
    .groupBy("CLM_PRCS_DT_SK", "SRC_SYS_CD")
    .agg(
        F.sum("CLM_ACTL_PD_AMT").alias("CLM_ACTL_PD_AMT"),
        F.sum("CLM_LN_TOT_COPAY_AMT").alias("CLM_LN_TOT_COPAY_AMT"),
        F.sum("DRUG_CLM_INGR_CST_ALW_AMT").alias("DRUG_CLM_INGR_CST_ALW_AMT"),
        F.sum("DRUG_CLM_RX_ALW_QTY").alias("DRUG_CLM_RX_ALW_QTY"),
        F.sum("REC_CNT").alias("GRND_REC_CNT")
    )
)

df_xfm_Prcs_dt = df_Agg_GrpTot_Amt.select(
    F.col("CLM_PRCS_DT_SK").alias("CLM_PRCS_DT_SK"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.lit("GRAND TOTAL").alias("RCRD_TYP_NM"),
    F.col("CLM_ACTL_PD_AMT").alias("CLM_ACTL_PD_AMT"),
    F.col("CLM_LN_TOT_COPAY_AMT").alias("CLM_LN_TOT_COPAY_AMT"),
    F.col("DRUG_CLM_INGR_CST_ALW_AMT").alias("DRUG_CLM_INGR_CST_ALW_AMT"),
    F.col("DRUG_CLM_RX_ALW_QTY").alias("DRUG_CLM_RX_ALW_QTY"),
    F.col("GRND_REC_CNT").alias("GRND_REC_CNT")
)

df_Lookup_392 = (
    df_GrndTotPdx.alias("Lnk_GrndTotSC")
    .join(
        df_xfm_Prcs_dt.alias("Gnd_Lkp"),
        (
            (F.col("Lnk_GrndTotSC.FILE_DT_SK_YR_MN") == F.col("Gnd_Lkp.CLM_PRCS_DT_SK"))
            & (F.col("Lnk_GrndTotSC.SRC_SYS_CD") == F.col("Gnd_Lkp.SRC_SYS_CD"))
            & (F.col("Lnk_GrndTotSC.RCRD_TYP_NM") == F.col("Gnd_Lkp.RCRD_TYP_NM"))
        ),
        "left"
    )
    .select(
        F.col("Lnk_GrndTotSC.FILE_DT_SK_YR_MN").alias("FILE_DT_SK_YR_MN"),
        F.col("Lnk_GrndTotSC.FILE_DT_SK").alias("FILE_DT_SK"),
        F.col("Lnk_GrndTotSC.SRC_SYS_GRP_ID").alias("SRC_SYS_GRP_ID"),
        F.col("Lnk_GrndTotSC.RCRD_TYP_NM").alias("RCRD_TYP_NM"),
        F.col("Lnk_GrndTotSC.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Lnk_GrndTotSC.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.col("Lnk_GrndTotSC.LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
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

df_Xfm_BusinessLogic = (
    df_Lookup_392
    .withColumn(
        "FILE_DT_SK",
        F.col("FILE_DT_SK")
    )
    .withColumn(
        "SRC_SYS_GRP_ID",
        F.col("SRC_SYS_GRP_ID")
    )
    .withColumn(
        "RCRD_TYP_NM",
        F.col("RCRD_TYP_NM")
    )
    .withColumn(
        "SRC_SYS_CD",
        F.col("SRC_SYS_CD")
    )
    .withColumn(
        "CRT_RUN_CYC_EXCTN_DT_SK",
        F.when(
            (F.col("CRT_RUN_CYC_EXCTN_DT_SK").isNull()) | (F.col("CRT_RUN_CYC_EXCTN_DT_SK") == ""),
            F.lit(RunCycleDt)
        ).otherwise(F.col("CRT_RUN_CYC_EXCTN_DT_SK"))
    )
    .withColumn(
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        F.when(
            (F.lit(RunCycleDt).isNull()) | (F.lit(RunCycleDt) == ""),
            F.lit("")
        ).otherwise(F.lit(RunCycleDt))
    )
    .withColumn(
        "GRP_ID",
        F.col("GRP_ID")
    )
    .withColumn(
        "SRC_SYS_COV_NO",
        F.col("SRC_SYS_COV_NO")
    )
    .withColumn(
        "RCVD_TOT_RCRD_CT",
        F.col("RCVD_TOT_RCRD_CT")
    )
    .withColumn(
        "RCVD_TOT_INGR_CST_AMT",
        F.col("RCVD_TOT_INGR_CST_AMT")
    )
    .withColumn(
        "RCVD_TOT_DRUG_ADM_FEE_AMT",
        F.col("RCVD_TOT_DRUG_ADM_FEE_AMT")
    )
    .withColumn(
        "RCVD_TOT_COPAY_AMT",
        F.col("RCVD_TOT_COPAY_AMT")
    )
    .withColumn(
        "RCVD_TOT_PD_AMT",
        F.col("RCVD_TOT_PD_AMT")
    )
    .withColumn(
        "RCVD_TOT_QTY_CT",
        F.col("RCVD_TOT_QTY_CT")
    )
    .withColumn(
        "CALC_TOT_RCRD_CT",
        F.when(
            (F.col("GRND_REC_CNT").isNull()) | (F.col("GRND_REC_CNT") == 0),
            F.lit(0)
        ).otherwise(F.col("GRND_REC_CNT"))
    )
    .withColumn(
        "CALC_TOT_INGR_CST_AMT",
        F.when(
            (F.col("DRUG_CLM_INGR_CST_ALW_AMT").isNull()) | (F.col("DRUG_CLM_INGR_CST_ALW_AMT") == 0),
            F.lit(0)
        ).otherwise(F.col("DRUG_CLM_INGR_CST_ALW_AMT"))
    )
    .withColumn(
        "CALC_TOT_DRUG_ADM_FEE_AMT",
        F.lit(None)
    )
    .withColumn(
        "CALC_TOT_COPAY_AMT",
        F.when(
            (F.col("CLM_LN_TOT_COPAY_AMT").isNull()) | (F.col("CLM_LN_TOT_COPAY_AMT") == 0),
            F.lit(0)
        ).otherwise(F.col("CLM_LN_TOT_COPAY_AMT"))
    )
    .withColumn(
        "CALC_TOT_PD_AMT",
        F.when(
            (F.col("CLM_ACTL_PD_AMT").isNull()) | (F.col("CLM_ACTL_PD_AMT") == 0),
            F.lit(0)
        ).otherwise(F.col("CLM_ACTL_PD_AMT"))
    )
    .withColumn(
        "CALC_TOT_QTY_CT",
        F.when(
            (F.col("DRUG_CLM_RX_ALW_QTY").isNull()) | (F.col("DRUG_CLM_RX_ALW_QTY") == 0),
            F.lit(0)
        ).otherwise(F.col("DRUG_CLM_RX_ALW_QTY"))
    )
)

df_Xfm_BusinessLogic = (
    df_Xfm_BusinessLogic
    .withColumn(
        "FILE_DT_SK",
        F.rpad(F.col("FILE_DT_SK"), 10, " ")
    )
    .withColumn(
        "CRT_RUN_CYC_EXCTN_DT_SK",
        F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
    )
    .withColumn(
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
    )
)

df_final = df_Xfm_BusinessLogic.select(
    "FILE_DT_SK",
    "SRC_SYS_GRP_ID",
    "RCRD_TYP_NM",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "GRP_ID",
    "SRC_SYS_COV_NO",
    "RCVD_TOT_RCRD_CT",
    "RCVD_TOT_INGR_CST_AMT",
    "RCVD_TOT_DRUG_ADM_FEE_AMT",
    "RCVD_TOT_COPAY_AMT",
    "RCVD_TOT_PD_AMT",
    "RCVD_TOT_QTY_CT",
    "CALC_TOT_RCRD_CT",
    "CALC_TOT_INGR_CST_AMT",
    "CALC_TOT_DRUG_ADM_FEE_AMT",
    "CALC_TOT_COPAY_AMT",
    "CALC_TOT_PD_AMT",
    "CALC_TOT_QTY_CT"
)

write_files(
    df_final,
    f"{adls_path}/load/{SrcSysCd}_CALCAMT_GRP_STOP_LOSS_PDX_CLM_RCVD_MNTHLY_SUM.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)