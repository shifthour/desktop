# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC CALLED BY: IdsEdwDrugClmMCAREBnfPhsDtlFSeq
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:     Reads the IDS CLM_COB data for Optum claims just loaded and creates a EDW DRUG_CLM_MCARE_BNF_PHS_DTL_F
# MAGIC 
# MAGIC Developer           Date                        Project/Altius #                                                            Change Description                                                     Development Project          Code Reviewer          Date Reviewed  
# MAGIC -----------------------    ----------------------------    ---------------------------                                                       ----------------------------------------------------------------------------        ------------------------------------       ----------------------------      -------------------------
# MAGIC Sagar Sayam     2020-11-05              6264 - PBM Phase II - Government Programs                      Initial Programming		EnterpriceDev2	Abhiram Dasarathy	2020-12-10

# MAGIC OPTUMRX EDW Claim DRUG_CLM_MCARE_BNF_PHS_DTL F
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, rpad
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, LongType, IntegerType, FloatType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

df_ids_cd_mppng_tbl = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"""
SELECT 
  CD_MPPNG.CD_MPPNG_SK as CD_MPPNG_SK,
  COALESCE(CD_MPPNG.TRGT_CD,'') as TRGT_CD,
  COALESCE(CD_MPPNG.TRGT_CD_NM,'') as TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG CD_MPPNG
""")
    .load()
)

df_DRUG_CLM_MCARE_BNF_PHS_DTL = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"""
SELECT 
DRUG_CLM_MCARE_BNF_PHS_DTL_SK,
CLM_ID,
CLM_REPRCS_SEQ_NO,
SRC_SYS_CD,
CRT_RUN_CYC_EXCTN_SK,
LAST_UPDT_RUN_CYC_EXCTN_SK,
DRUG_CLM_SK,
DRUG_COV_STTUS_CD_SK,
MEDSPAN_GNRC_DRUG_IN,
CLM_SUBMT_DT_SK,
ORIG_CLM_RCVD_DT_SK,
CLM_REPRCS_PAYBL_AMT,
CLM_REPRCS_PATN_RESP_AMT,
COV_PLN_PD_AMT,
DEDCT_LVL_DRUG_SPEND_THRSHLD_AMT,
DEDCT_LVL_TRUE_OOP_THRSHLD_AMT,
DRUG_SPEND_AMT,
DRUG_SPEND_TOWARD_DEDCT_AMT,
DRUG_SPEND_TOWARD_INIT_COV_AMT,
DUAL_AMT,
EGWP_DEDCT_LVL_TROOP_THRSHLD_AMT,
EGWP_DRUG_SPEND_TOWARD_CATO_AMT,
EGWP_DRUG_SPEND_TOWARD_DEDCT_AMT,
EGWP_DRUG_SPEND_TOWARD_GAP_AMT,
EGWP_DRUG_SPEND_TOWARD_INIT_COV_AMT,
EGWP_MBR_TOWARD_CATO_AMT,
EGWP_MBR_TOWARD_DEDCT_AMT,
EGWP_MBR_TOWARD_GAP_AMT,
EGWP_MBR_TOWARD_INIT_COV_AMT,
EGWP_PLN_CATO_AMT,
EGWP_PLN_GAP_THRSHLD_AMT,
EGWP_TOWARD_PLN_CATO_AMT,
EGWP_TOWARD_PLN_DEDCT_AMT,
EGWP_TOWARD_PLN_GAP_AMT,
GAP_BRND_DSCNT_AMT,
GROS_DRUG_CST_AFTR_AMT,
GROS_DRUG_CST_BFR_AMT,
LICS_DRUG_SPEND_DEDCT_AMT,
LICS_DRUG_SPEND_DEDCT_BAL_AMT,
LOW_INCM_CST_SHR_SBSDY_AMT,
MCAREB_DRUG_SPEND_TOWARD_OOP_AMT,
MCAREB_MBR_TOWARD_OOP_AMT,
MBR_TOWARD_CATO_AMT,
MBR_TOWARD_DEDCT_AMT,
MBR_TOWARD_GAP_AMT,
MBR_TOWARD_INIT_COV_AMT,
NCOV_PLN_PD_AMT,
PRIOR_TRUE_OOP_AMT,
RPTD_GAP_DSCNT_AMT,
SUBRO_AMT,
TRUE_OOP_ACCUM_AMT,
TOT_GROS_DRUG_CST_ACCUM_AMT,
TOWARD_CATO_PHS_DRUG_SPEND_AMT,
TOWARD_GAP_PHS_DRUG_SPEND_AMT,
TOWARD_PLN_CATO_AMT,
TOWARD_PLN_DEDCT_AMT,
TOWARD_PLN_GAP_AMT,
BEG_BNF_PHS_ID,
END_BNF_PHS_ID,
MCARE_CNTR_ID,
PLN_BNF_PCKG_ID,
TRUE_OOP_SCHD_ID
FROM {IDSOwner}.DRUG_CLM_MCARE_BNF_PHS_DTL MCARE
WHERE  MCARE.LAST_UPDT_RUN_CYC_EXCTN_SK >= {IDSRunCycle}
""")
    .load()
)

df_Lkp_CdMppng_base = df_DRUG_CLM_MCARE_BNF_PHS_DTL.alias("Extract").join(
    df_ids_cd_mppng_tbl.alias("refDrugCovSts"),
    on=[col("Extract.DRUG_COV_STTUS_CD_SK") == col("refDrugCovSts.CD_MPPNG_SK")],
    how="left"
)

df_Lkp_CdMppng = df_Lkp_CdMppng_base.select(
    col("Extract.DRUG_CLM_MCARE_BNF_PHS_DTL_SK").alias("DRUG_CLM_MCARE_BNF_PHS_DTL_SK"),
    col("Extract.CLM_ID").alias("CLM_ID"),
    col("Extract.CLM_REPRCS_SEQ_NO").alias("CLM_REPRCS_SEQ_NO"),
    col("Extract.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("Extract.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("Extract.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("Extract.DRUG_CLM_SK").alias("CLM_SK"),
    col("Extract.DRUG_COV_STTUS_CD_SK").alias("DRUG_COV_STTUS_CD_SK"),
    col("refDrugCovSts.TRGT_CD_NM").alias("DRUG_COV_STTUS_NM"),
    col("Extract.MEDSPAN_GNRC_DRUG_IN").alias("MEDSPAN_GNRC_DRUG_IN"),
    col("Extract.CLM_SUBMT_DT_SK").alias("CLM_SUBMT_DT_SK"),
    col("Extract.ORIG_CLM_RCVD_DT_SK").alias("ORIG_CLM_RCVD_DT_SK"),
    col("Extract.CLM_REPRCS_PAYBL_AMT").alias("CLM_REPRCS_PAYBL_AMT"),
    col("Extract.CLM_REPRCS_PATN_RESP_AMT").alias("CLM_REPRCS_PATN_RESP_AMT"),
    col("Extract.COV_PLN_PD_AMT").alias("COV_PLN_PD_AMT"),
    col("Extract.DEDCT_LVL_DRUG_SPEND_THRSHLD_AMT").alias("DEDCT_LVL_DRUG_SPEND_THRSHLD_AMT"),
    col("Extract.DEDCT_LVL_TRUE_OOP_THRSHLD_AMT").alias("DEDCT_LVL_TRUE_OOP_THRSHLD_AMT"),
    col("Extract.DRUG_SPEND_AMT").alias("DRUG_SPEND_AMT"),
    col("Extract.DRUG_SPEND_TOWARD_DEDCT_AMT").alias("DRUG_SPEND_TOWARD_DEDCT_AMT"),
    col("Extract.DRUG_SPEND_TOWARD_INIT_COV_AMT").alias("DRUG_SPEND_TOWARD_INIT_COV_AMT"),
    col("Extract.DUAL_AMT").alias("DUAL_AMT"),
    col("Extract.EGWP_DEDCT_LVL_TROOP_THRSHLD_AMT").alias("EGWP_DEDCT_LVL_TROOP_THRSHLD_AMT"),
    col("Extract.EGWP_DRUG_SPEND_TOWARD_CATO_AMT").alias("EGWP_DRUG_SPEND_TOWARD_CATO_AMT"),
    col("Extract.EGWP_DRUG_SPEND_TOWARD_DEDCT_AMT").alias("EGWP_DRUG_SPEND_TOWARD_DEDCT_AMT"),
    col("Extract.EGWP_DRUG_SPEND_TOWARD_GAP_AMT").alias("EGWP_DRUG_SPEND_TOWARD_GAP_AMT"),
    col("Extract.EGWP_DRUG_SPEND_TOWARD_INIT_COV_AMT").alias("EGWP_DRUG_SPEND_TOWARD_INIT_COV_AMT"),
    col("Extract.EGWP_MBR_TOWARD_CATO_AMT").alias("EGWP_MBR_TOWARD_CATO_AMT"),
    col("Extract.EGWP_MBR_TOWARD_DEDCT_AMT").alias("EGWP_MBR_TOWARD_DEDCT_AMT"),
    col("Extract.EGWP_MBR_TOWARD_GAP_AMT").alias("EGWP_MBR_TOWARD_GAP_AMT"),
    col("Extract.EGWP_MBR_TOWARD_INIT_COV_AMT").alias("EGWP_MBR_TOWARD_INIT_COV_AMT"),
    col("Extract.EGWP_PLN_CATO_AMT").alias("EGWP_PLN_CATO_AMT"),
    col("Extract.EGWP_PLN_GAP_THRSHLD_AMT").alias("EGWP_PLN_GAP_THRSHLD_AMT"),
    col("Extract.EGWP_TOWARD_PLN_CATO_AMT").alias("EGWP_TOWARD_PLN_CATO_AMT"),
    col("Extract.EGWP_TOWARD_PLN_DEDCT_AMT").alias("EGWP_TOWARD_PLN_DEDCT_AMT"),
    col("Extract.EGWP_TOWARD_PLN_GAP_AMT").alias("EGWP_TOWARD_PLN_GAP_AMT"),
    col("Extract.GAP_BRND_DSCNT_AMT").alias("GAP_BRND_DSCNT_AMT"),
    col("Extract.GROS_DRUG_CST_AFTR_AMT").alias("GROS_DRUG_CST_AFTR_AMT"),
    col("Extract.GROS_DRUG_CST_BFR_AMT").alias("GROS_DRUG_CST_BFR_AMT"),
    col("Extract.LICS_DRUG_SPEND_DEDCT_AMT").alias("LICS_DRUG_SPEND_DEDCT_AMT"),
    col("Extract.LICS_DRUG_SPEND_DEDCT_BAL_AMT").alias("LICS_DRUG_SPEND_DEDCT_BAL_AMT"),
    col("Extract.LOW_INCM_CST_SHR_SBSDY_AMT").alias("LOW_INCM_CST_SHR_SBSDY_AMT"),
    col("Extract.MCAREB_DRUG_SPEND_TOWARD_OOP_AMT").alias("MCAREB_DRUG_SPEND_TOWARD_OOP_AMT"),
    col("Extract.MCAREB_MBR_TOWARD_OOP_AMT").alias("MCAREB_MBR_TOWARD_OOP_AMT"),
    col("Extract.MBR_TOWARD_CATO_AMT").alias("MBR_TOWARD_CATO_AMT"),
    col("Extract.MBR_TOWARD_DEDCT_AMT").alias("MBR_TOWARD_DEDCT_AMT"),
    col("Extract.MBR_TOWARD_GAP_AMT").alias("MBR_TOWARD_GAP_AMT"),
    col("Extract.MBR_TOWARD_INIT_COV_AMT").alias("MBR_TOWARD_INIT_COV_AMT"),
    col("Extract.NCOV_PLN_PD_AMT").alias("NCOV_PLN_PD_AMT"),
    col("Extract.PRIOR_TRUE_OOP_AMT").alias("PRIOR_TRUE_OOP_AMT"),
    col("Extract.RPTD_GAP_DSCNT_AMT").alias("RPTD_GAP_DSCNT_AMT"),
    col("Extract.SUBRO_AMT").alias("SUBRO_AMT"),
    col("Extract.TRUE_OOP_ACCUM_AMT").alias("TRUE_OOP_ACCUM_AMT"),
    col("Extract.TOT_GROS_DRUG_CST_ACCUM_AMT").alias("TOT_GROS_DRUG_CST_ACCUM_AMT"),
    col("Extract.TOWARD_CATO_PHS_DRUG_SPEND_AMT").alias("TOWARD_CATO_PHS_DRUG_SPEND_AMT"),
    col("Extract.TOWARD_GAP_PHS_DRUG_SPEND_AMT").alias("TOWARD_GAP_PHS_DRUG_SPEND_AMT"),
    col("Extract.TOWARD_PLN_CATO_AMT").alias("TOWARD_PLN_CATO_AMT"),
    col("Extract.TOWARD_PLN_DEDCT_AMT").alias("TOWARD_PLN_DEDCT_AMT"),
    col("Extract.TOWARD_PLN_GAP_AMT").alias("TOWARD_PLN_GAP_AMT"),
    col("Extract.BEG_BNF_PHS_ID").alias("BEG_BNF_PHS_ID"),
    col("Extract.END_BNF_PHS_ID").alias("END_BNF_PHS_ID"),
    col("Extract.MCARE_CNTR_ID").alias("MCARE_CNTR_ID"),
    col("Extract.PLN_BNF_PCKG_ID").alias("PLN_BNF_PCKG_ID"),
    col("Extract.TRUE_OOP_SCHD_ID").alias("TRUE_OOP_SCHD_ID"),
    col("refDrugCovSts.TRGT_CD").alias("TRGT_CD")
)

df_Trans_Codes_Lnk_All = df_Lkp_CdMppng.select(
    col("DRUG_CLM_MCARE_BNF_PHS_DTL_SK").alias("DRUG_CLM_MCARE_BNF_PHS_DTL_SK"),
    col("CLM_ID").alias("CLM_ID"),
    col("CLM_REPRCS_SEQ_NO").alias("CLM_REPRCS_SEQ_NO"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("CLM_SK").alias("CLM_SK"),
    col("TRGT_CD").alias("DRUG_COV_STTUS_CD"),
    col("DRUG_COV_STTUS_NM").alias("DRUG_COV_STTUS_NM"),
    col("MEDSPAN_GNRC_DRUG_IN").alias("MEDSPAN_GNRC_DRUG_IN"),
    col("CLM_SUBMT_DT_SK").alias("CLM_SUBMT_DT_SK"),
    col("ORIG_CLM_RCVD_DT_SK").alias("ORIG_CLM_RCVD_DT_SK"),
    col("CLM_REPRCS_PAYBL_AMT").alias("CLM_REPRCS_PAYBL_AMT"),
    col("CLM_REPRCS_PATN_RESP_AMT").alias("CLM_REPRCS_PATN_RESP_AMT"),
    col("COV_PLN_PD_AMT").alias("COV_PLN_PD_AMT"),
    col("DEDCT_LVL_DRUG_SPEND_THRSHLD_AMT").alias("DEDCT_LVL_DRUG_SPEND_THRSHLD_AMT"),
    col("DEDCT_LVL_TRUE_OOP_THRSHLD_AMT").alias("DEDCT_LVL_TRUE_OOP_THRSHLD_AMT"),
    col("DRUG_SPEND_AMT").alias("DRUG_SPEND_AMT"),
    col("DRUG_SPEND_TOWARD_DEDCT_AMT").alias("DRUG_SPEND_TOWARD_DEDCT_AMT"),
    col("DRUG_SPEND_TOWARD_INIT_COV_AMT").alias("DRUG_SPEND_TOWARD_INIT_COV_AMT"),
    col("DUAL_AMT").alias("DUAL_AMT"),
    col("EGWP_DEDCT_LVL_TROOP_THRSHLD_AMT").alias("EGWP_DEDCT_LVL_TROOP_THRSHLD_AMT"),
    col("EGWP_DRUG_SPEND_TOWARD_CATO_AMT").alias("EGWP_DRUG_SPEND_TOWARD_CATO_AMT"),
    col("EGWP_DRUG_SPEND_TOWARD_DEDCT_AMT").alias("EGWP_DRUG_SPEND_TOWARD_DEDCT_AMT"),
    col("EGWP_DRUG_SPEND_TOWARD_GAP_AMT").alias("EGWP_DRUG_SPEND_TOWARD_GAP_AMT"),
    col("EGWP_DRUG_SPEND_TOWARD_INIT_COV_AMT").alias("EGWP_DRUG_SPEND_TOWARD_INIT_COV_AMT"),
    col("EGWP_MBR_TOWARD_CATO_AMT").alias("EGWP_MBR_TOWARD_CATO_AMT"),
    col("EGWP_MBR_TOWARD_DEDCT_AMT").alias("EGWP_MBR_TOWARD_DEDCT_AMT"),
    col("EGWP_MBR_TOWARD_GAP_AMT").alias("EGWP_MBR_TOWARD_GAP_AMT"),
    col("EGWP_MBR_TOWARD_INIT_COV_AMT").alias("EGWP_MBR_TOWARD_INIT_COV_AMT"),
    col("EGWP_PLN_CATO_AMT").alias("EGWP_PLN_CATO_AMT"),
    col("EGWP_PLN_GAP_THRSHLD_AMT").alias("EGWP_PLN_GAP_THRSHLD_AMT"),
    col("EGWP_TOWARD_PLN_CATO_AMT").alias("EGWP_TOWARD_PLN_CATO_AMT"),
    col("EGWP_TOWARD_PLN_DEDCT_AMT").alias("EGWP_TOWARD_PLN_DEDCT_AMT"),
    col("EGWP_TOWARD_PLN_GAP_AMT").alias("EGWP_TOWARD_PLN_GAP_AMT"),
    col("GAP_BRND_DSCNT_AMT").alias("GAP_BRND_DSCNT_AMT"),
    col("GROS_DRUG_CST_AFTR_AMT").alias("GROS_DRUG_CST_AFTR_AMT"),
    col("GROS_DRUG_CST_BFR_AMT").alias("GROS_DRUG_CST_BFR_AMT"),
    col("LICS_DRUG_SPEND_DEDCT_AMT").alias("LICS_DRUG_SPEND_DEDCT_AMT"),
    col("LICS_DRUG_SPEND_DEDCT_BAL_AMT").alias("LICS_DRUG_SPEND_DEDCT_BAL_AMT"),
    col("LOW_INCM_CST_SHR_SBSDY_AMT").alias("LOW_INCM_CST_SHR_SBSDY_AMT"),
    col("MCAREB_DRUG_SPEND_TOWARD_OOP_AMT").alias("MCAREB_DRUG_SPEND_TOWARD_OOP_AMT"),
    col("MCAREB_MBR_TOWARD_OOP_AMT").alias("MCAREB_MBR_TOWARD_OOP_AMT"),
    col("MBR_TOWARD_CATO_AMT").alias("MBR_TOWARD_CATO_AMT"),
    col("MBR_TOWARD_DEDCT_AMT").alias("MBR_TOWARD_DEDCT_AMT"),
    col("MBR_TOWARD_GAP_AMT").alias("MBR_TOWARD_GAP_AMT"),
    col("MBR_TOWARD_INIT_COV_AMT").alias("MBR_TOWARD_INIT_COV_AMT"),
    col("NCOV_PLN_PD_AMT").alias("NCOV_PLN_PD_AMT"),
    col("PRIOR_TRUE_OOP_AMT").alias("PRIOR_TRUE_OOP_AMT"),
    col("RPTD_GAP_DSCNT_AMT").alias("RPTD_GAP_DSCNT_AMT"),
    col("SUBRO_AMT").alias("SUBRO_AMT"),
    col("TRUE_OOP_ACCUM_AMT").alias("TRUE_OOP_ACCUM_AMT"),
    col("TOT_GROS_DRUG_CST_ACCUM_AMT").alias("TOT_GROS_DRUG_CST_ACCUM_AMT"),
    col("TOWARD_CATO_PHS_DRUG_SPEND_AMT").alias("TOWARD_CATO_PHS_DRUG_SPEND_AMT"),
    col("TOWARD_GAP_PHS_DRUG_SPEND_AMT").alias("TOWARD_GAP_PHS_DRUG_SPEND_AMT"),
    col("TOWARD_PLN_CATO_AMT").alias("TOWARD_PLN_CATO_AMT"),
    col("TOWARD_PLN_DEDCT_AMT").alias("TOWARD_PLN_DEDCT_AMT"),
    col("TOWARD_PLN_GAP_AMT").alias("TOWARD_PLN_GAP_AMT"),
    col("BEG_BNF_PHS_ID").alias("BEG_BNF_PHS_ID"),
    col("END_BNF_PHS_ID").alias("END_BNF_PHS_ID"),
    col("MCARE_CNTR_ID").alias("MCARE_CNTR_ID"),
    col("PLN_BNF_PCKG_ID").alias("PLN_BNF_PCKG_ID"),
    col("TRUE_OOP_SCHD_ID").alias("TRUE_OOP_SCHD_ID"),
    lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("DRUG_COV_STTUS_CD_SK").alias("DRUG_COV_STTUS_CD_SK")
)

df_Trans_Codes_Lnk_NA = df_Lkp_CdMppng.limit(1).select(
    lit(1).alias("DRUG_CLM_MCARE_BNF_PHS_DTL_SK"),
    lit("NA").alias("CLM_ID"),
    lit(1).alias("CLM_REPRCS_SEQ_NO"),
    lit("NA").alias("SRC_SYS_CD"),
    lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    lit(1).alias("CLM_SK"),
    lit("NA").alias("DRUG_COV_STTUS_CD"),
    lit("NA").alias("DRUG_COV_STTUS_NM"),
    lit("N").alias("MEDSPAN_GNRC_DRUG_IN"),
    lit("1753-01-01").alias("CLM_SUBMT_DT_SK"),
    lit("1753-01-01").alias("ORIG_CLM_RCVD_DT_SK"),
    lit(0.00).alias("CLM_REPRCS_PAYBL_AMT"),
    lit(0.00).alias("CLM_REPRCS_PATN_RESP_AMT"),
    lit(0.00).alias("COV_PLN_PD_AMT"),
    lit(0.00).alias("DEDCT_LVL_DRUG_SPEND_THRSHLD_AMT"),
    lit(0.00).alias("DEDCT_LVL_TRUE_OOP_THRSHLD_AMT"),
    lit(0.00).alias("DRUG_SPEND_AMT"),
    lit(0.00).alias("DRUG_SPEND_TOWARD_DEDCT_AMT"),
    lit(0.00).alias("DRUG_SPEND_TOWARD_INIT_COV_AMT"),
    lit(0.00).alias("DUAL_AMT"),
    lit(0.00).alias("EGWP_DEDCT_LVL_TROOP_THRSHLD_AMT"),
    lit(0.00).alias("EGWP_DRUG_SPEND_TOWARD_CATO_AMT"),
    lit(0.00).alias("EGWP_DRUG_SPEND_TOWARD_DEDCT_AMT"),
    lit(0.00).alias("EGWP_DRUG_SPEND_TOWARD_GAP_AMT"),
    lit(0.00).alias("EGWP_DRUG_SPEND_TOWARD_INIT_COV_AMT"),
    lit(0.00).alias("EGWP_MBR_TOWARD_CATO_AMT"),
    lit(0.00).alias("EGWP_MBR_TOWARD_DEDCT_AMT"),
    lit(0.00).alias("EGWP_MBR_TOWARD_GAP_AMT"),
    lit(0.00).alias("EGWP_MBR_TOWARD_INIT_COV_AMT"),
    lit(0.00).alias("EGWP_PLN_CATO_AMT"),
    lit(0.00).alias("EGWP_PLN_GAP_THRSHLD_AMT"),
    lit(0.00).alias("EGWP_TOWARD_PLN_CATO_AMT"),
    lit(0.00).alias("EGWP_TOWARD_PLN_DEDCT_AMT"),
    lit(0.00).alias("EGWP_TOWARD_PLN_GAP_AMT"),
    lit(0.00).alias("GAP_BRND_DSCNT_AMT"),
    lit(0.00).alias("GROS_DRUG_CST_AFTR_AMT"),
    lit(0.00).alias("GROS_DRUG_CST_BFR_AMT"),
    lit(0.00).alias("LICS_DRUG_SPEND_DEDCT_AMT"),
    lit(0.00).alias("LICS_DRUG_SPEND_DEDCT_BAL_AMT"),
    lit(0.00).alias("LOW_INCM_CST_SHR_SBSDY_AMT"),
    lit(0.00).alias("MCAREB_DRUG_SPEND_TOWARD_OOP_AMT"),
    lit(0.00).alias("MCAREB_MBR_TOWARD_OOP_AMT"),
    lit(0.00).alias("MBR_TOWARD_CATO_AMT"),
    lit(0.00).alias("MBR_TOWARD_DEDCT_AMT"),
    lit(0.00).alias("MBR_TOWARD_GAP_AMT"),
    lit(0.00).alias("MBR_TOWARD_INIT_COV_AMT"),
    lit(0.00).alias("NCOV_PLN_PD_AMT"),
    lit(0.00).alias("PRIOR_TRUE_OOP_AMT"),
    lit(0.00).alias("RPTD_GAP_DSCNT_AMT"),
    lit(0.00).alias("SUBRO_AMT"),
    lit(0.00).alias("TRUE_OOP_ACCUM_AMT"),
    lit(0.00).alias("TOT_GROS_DRUG_CST_ACCUM_AMT"),
    lit(0.00).alias("TOWARD_CATO_PHS_DRUG_SPEND_AMT"),
    lit(0.00).alias("TOWARD_GAP_PHS_DRUG_SPEND_AMT"),
    lit(0.00).alias("TOWARD_PLN_CATO_AMT"),
    lit(0.00).alias("TOWARD_PLN_DEDCT_AMT"),
    lit(0.00).alias("TOWARD_PLN_GAP_AMT"),
    lit("NA").alias("BEG_BNF_PHS_ID"),
    lit("NA").alias("END_BNF_PHS_ID"),
    lit("NA").alias("MCARE_CNTR_ID"),
    lit("NA").alias("PLN_BNF_PCKG_ID"),
    lit("NA").alias("TRUE_OOP_SCHD_ID"),
    lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(1).alias("DRUG_COV_STTUS_CD_SK")
)

df_Trans_Codes_Lnk_UNK = df_Lkp_CdMppng.limit(1).select(
    lit(0).alias("DRUG_CLM_MCARE_BNF_PHS_DTL_SK"),
    lit("UNK").alias("CLM_ID"),
    lit(0).alias("CLM_REPRCS_SEQ_NO"),
    lit("UNK").alias("SRC_SYS_CD"),
    lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    lit(0).alias("CLM_SK"),
    lit("UNK").alias("DRUG_COV_STTUS_CD"),
    lit("UNK").alias("DRUG_COV_STTUS_NM"),
    lit("U").alias("MEDSPAN_GNRC_DRUG_IN"),
    lit("1753-01-01").alias("CLM_SUBMT_DT_SK"),
    lit("1753-01-01").alias("ORIG_CLM_RCVD_DT_SK"),
    lit(0.00).alias("CLM_REPRCS_PAYBL_AMT"),
    lit(0.00).alias("CLM_REPRCS_PATN_RESP_AMT"),
    lit(0.00).alias("COV_PLN_PD_AMT"),
    lit(0.00).alias("DEDCT_LVL_DRUG_SPEND_THRSHLD_AMT"),
    lit(0.00).alias("DEDCT_LVL_TRUE_OOP_THRSHLD_AMT"),
    lit(0.00).alias("DRUG_SPEND_AMT"),
    lit(0.00).alias("DRUG_SPEND_TOWARD_DEDCT_AMT"),
    lit(0.00).alias("DRUG_SPEND_TOWARD_INIT_COV_AMT"),
    lit(0.00).alias("DUAL_AMT"),
    lit(0.00).alias("EGWP_DEDCT_LVL_TROOP_THRSHLD_AMT"),
    lit(0.00).alias("EGWP_DRUG_SPEND_TOWARD_CATO_AMT"),
    lit(0.00).alias("EGWP_DRUG_SPEND_TOWARD_DEDCT_AMT"),
    lit(0.00).alias("EGWP_DRUG_SPEND_TOWARD_GAP_AMT"),
    lit(0.00).alias("EGWP_DRUG_SPEND_TOWARD_INIT_COV_AMT"),
    lit(0.00).alias("EGWP_MBR_TOWARD_CATO_AMT"),
    lit(0.00).alias("EGWP_MBR_TOWARD_DEDCT_AMT"),
    lit(0.00).alias("EGWP_MBR_TOWARD_GAP_AMT"),
    lit(0.00).alias("EGWP_MBR_TOWARD_INIT_COV_AMT"),
    lit(0.00).alias("EGWP_PLN_CATO_AMT"),
    lit(0.00).alias("EGWP_PLN_GAP_THRSHLD_AMT"),
    lit(0.00).alias("EGWP_TOWARD_PLN_CATO_AMT"),
    lit(0.00).alias("EGWP_TOWARD_PLN_DEDCT_AMT"),
    lit(0.00).alias("EGWP_TOWARD_PLN_GAP_AMT"),
    lit(0.00).alias("GAP_BRND_DSCNT_AMT"),
    lit(0.00).alias("GROS_DRUG_CST_AFTR_AMT"),
    lit(0.00).alias("GROS_DRUG_CST_BFR_AMT"),
    lit(0.00).alias("LICS_DRUG_SPEND_DEDCT_AMT"),
    lit(0.00).alias("LICS_DRUG_SPEND_DEDCT_BAL_AMT"),
    lit(0.00).alias("LOW_INCM_CST_SHR_SBSDY_AMT"),
    lit(0.00).alias("MCAREB_DRUG_SPEND_TOWARD_OOP_AMT"),
    lit(0.00).alias("MCAREB_MBR_TOWARD_OOP_AMT"),
    lit(0.00).alias("MBR_TOWARD_CATO_AMT"),
    lit(0.00).alias("MBR_TOWARD_DEDCT_AMT"),
    lit(0.00).alias("MBR_TOWARD_GAP_AMT"),
    lit(0.00).alias("MBR_TOWARD_INIT_COV_AMT"),
    lit(0.00).alias("NCOV_PLN_PD_AMT"),
    lit(0.00).alias("PRIOR_TRUE_OOP_AMT"),
    lit(0.00).alias("RPTD_GAP_DSCNT_AMT"),
    lit(0.00).alias("SUBRO_AMT"),
    lit(0.00).alias("TRUE_OOP_ACCUM_AMT"),
    lit(0.00).alias("TOT_GROS_DRUG_CST_ACCUM_AMT"),
    lit(0.00).alias("TOWARD_CATO_PHS_DRUG_SPEND_AMT"),
    lit(0.00).alias("TOWARD_GAP_PHS_DRUG_SPEND_AMT"),
    lit(0.00).alias("TOWARD_PLN_CATO_AMT"),
    lit(0.00).alias("TOWARD_PLN_DEDCT_AMT"),
    lit(0.00).alias("TOWARD_PLN_GAP_AMT"),
    lit("UNK").alias("BEG_BNF_PHS_ID"),
    lit("UNK").alias("END_BNF_PHS_ID"),
    lit("UNK").alias("MCARE_CNTR_ID"),
    lit("UNK").alias("PLN_BNF_PCKG_ID"),
    lit("UNK").alias("TRUE_OOP_SCHD_ID"),
    lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("DRUG_COV_STTUS_CD_SK")
)

df_Funnel_28 = (
    df_Trans_Codes_Lnk_All
    .unionByName(df_Trans_Codes_Lnk_NA)
    .unionByName(df_Trans_Codes_Lnk_UNK)
)

df_final = df_Funnel_28.select(
    "DRUG_CLM_MCARE_BNF_PHS_DTL_SK",
    "CLM_ID",
    "CLM_REPRCS_SEQ_NO",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "CLM_SK",
    "DRUG_COV_STTUS_CD",
    "DRUG_COV_STTUS_NM",
    "MEDSPAN_GNRC_DRUG_IN",
    "CLM_SUBMT_DT_SK",
    "ORIG_CLM_RCVD_DT_SK",
    "CLM_REPRCS_PAYBL_AMT",
    "CLM_REPRCS_PATN_RESP_AMT",
    "COV_PLN_PD_AMT",
    "DEDCT_LVL_DRUG_SPEND_THRSHLD_AMT",
    "DEDCT_LVL_TRUE_OOP_THRSHLD_AMT",
    "DRUG_SPEND_AMT",
    "DRUG_SPEND_TOWARD_DEDCT_AMT",
    "DRUG_SPEND_TOWARD_INIT_COV_AMT",
    "DUAL_AMT",
    "EGWP_DEDCT_LVL_TROOP_THRSHLD_AMT",
    "EGWP_DRUG_SPEND_TOWARD_CATO_AMT",
    "EGWP_DRUG_SPEND_TOWARD_DEDCT_AMT",
    "EGWP_DRUG_SPEND_TOWARD_GAP_AMT",
    "EGWP_DRUG_SPEND_TOWARD_INIT_COV_AMT",
    "EGWP_MBR_TOWARD_CATO_AMT",
    "EGWP_MBR_TOWARD_DEDCT_AMT",
    "EGWP_MBR_TOWARD_GAP_AMT",
    "EGWP_MBR_TOWARD_INIT_COV_AMT",
    "EGWP_PLN_CATO_AMT",
    "EGWP_PLN_GAP_THRSHLD_AMT",
    "EGWP_TOWARD_PLN_CATO_AMT",
    "EGWP_TOWARD_PLN_DEDCT_AMT",
    "EGWP_TOWARD_PLN_GAP_AMT",
    "GAP_BRND_DSCNT_AMT",
    "GROS_DRUG_CST_AFTR_AMT",
    "GROS_DRUG_CST_BFR_AMT",
    "LICS_DRUG_SPEND_DEDCT_AMT",
    "LICS_DRUG_SPEND_DEDCT_BAL_AMT",
    "LOW_INCM_CST_SHR_SBSDY_AMT",
    "MCAREB_DRUG_SPEND_TOWARD_OOP_AMT",
    "MCAREB_MBR_TOWARD_OOP_AMT",
    "MBR_TOWARD_CATO_AMT",
    "MBR_TOWARD_DEDCT_AMT",
    "MBR_TOWARD_GAP_AMT",
    "MBR_TOWARD_INIT_COV_AMT",
    "NCOV_PLN_PD_AMT",
    "PRIOR_TRUE_OOP_AMT",
    "RPTD_GAP_DSCNT_AMT",
    "SUBRO_AMT",
    "TRUE_OOP_ACCUM_AMT",
    "TOT_GROS_DRUG_CST_ACCUM_AMT",
    "TOWARD_CATO_PHS_DRUG_SPEND_AMT",
    "TOWARD_GAP_PHS_DRUG_SPEND_AMT",
    "TOWARD_PLN_CATO_AMT",
    "TOWARD_PLN_DEDCT_AMT",
    "TOWARD_PLN_GAP_AMT",
    "BEG_BNF_PHS_ID",
    "END_BNF_PHS_ID",
    "MCARE_CNTR_ID",
    "PLN_BNF_PCKG_ID",
    "TRUE_OOP_SCHD_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "DRUG_COV_STTUS_CD_SK"
)

df_final = df_final.withColumn("CRT_RUN_CYC_EXCTN_DT_SK", rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ")) \
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ")) \
    .withColumn("MEDSPAN_GNRC_DRUG_IN", rpad(col("MEDSPAN_GNRC_DRUG_IN"), 1, " ")) \
    .withColumn("CLM_SUBMT_DT_SK", rpad(col("CLM_SUBMT_DT_SK"), 10, " ")) \
    .withColumn("ORIG_CLM_RCVD_DT_SK", rpad(col("ORIG_CLM_RCVD_DT_SK"), 10, " "))

write_files(
    df_final,
    f"{adls_path}/load/DRUG_CLM_MCARE_BNF_PHS_DTL_F.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)