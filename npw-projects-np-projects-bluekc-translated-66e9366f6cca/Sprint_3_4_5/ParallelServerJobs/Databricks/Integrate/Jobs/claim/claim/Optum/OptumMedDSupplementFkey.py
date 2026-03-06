# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC **************************************************************************************
# MAGIC COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC CALLED BY:  
# MAGIC 
# MAGIC DESCRIPTION:    Pulls data from Optum Supplemental Claims file to load DRUG_CLM_MCARE_BNF_PHS_DT Table
# MAGIC 
# MAGIC       
# MAGIC MODIFICATIONS:
# MAGIC Developer                                           Date                      Project/Altiris #                                                 Change Description                             Development Project                                Code Reviewer                 Date Reviewed
# MAGIC ---------------------                                  ------------------      -------------------------------------------                                ---------------------------------------------------------      -------------------------------------------------------           -----------------------------------------      -------------------------   
# MAGIC Velmani Kondappan                       2020-10-19      6264 - PBM Phase II - Government Programs                  Initial Programming                               IntegrateDev2		Abhiram Dasarathy	2020-12-10


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
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
RunID = get_widget_value("RunID","")
RunCycle = get_widget_value("RunCycle","")
SrcSysCd = get_widget_value("SrcSysCd","")
RunDate = get_widget_value("RunDate","")

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

# Read from Seq_DRUG_CLM_MCARE_BNF_PHS_DTL
schema_Seq_DRUG_CLM_MCARE_BNF_PHS_DTL = StructType([
    StructField("DRUG_CLM_MCARE_BNF_PHS_DTL_SK", IntegerType(), False),
    StructField("CLM_ID", StringType(), False),
    StructField("CLM_REPRCS_SEQ_NO", DecimalType(38,10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("DRUG_CLM_SK", IntegerType(), False),
    StructField("DRUG_COV_STTUS_CD", StringType(), False),
    StructField("MEDSPAN_GNRC_DRUG_IN", StringType(), False),
    StructField("CLM_SUBMT_DT_SK", StringType(), False),
    StructField("ORIG_CLM_RCVD_DT_SK", StringType(), False),
    StructField("CLM_REPRCS_PAYBL_AMT", DecimalType(38,10), False),
    StructField("CLM_REPRCS_PATN_RESP_AMT", DecimalType(38,10), False),
    StructField("COV_PLN_PD_AMT", DecimalType(38,10), False),
    StructField("DEDCT_LVL_DRUG_SPEND_THRSHLD_AMT", DecimalType(38,10), False),
    StructField("DEDCT_LVL_TRUE_OOP_THRSHLD_AMT", DecimalType(38,10), False),
    StructField("DRUG_SPEND_AMT", DecimalType(38,10), False),
    StructField("DRUG_SPEND_TOWARD_DEDCT_AMT", DecimalType(38,10), False),
    StructField("DRUG_SPEND_TOWARD_INIT_COV_AMT", DecimalType(38,10), False),
    StructField("DUAL_AMT", DecimalType(38,10), False),
    StructField("EGWP_DEDCT_LVL_TROOP_THRSHLD_AMT", DecimalType(38,10), False),
    StructField("EGWP_DRUG_SPEND_TOWARD_CATO_AMT", DecimalType(38,10), False),
    StructField("EGWP_DRUG_SPEND_TOWARD_DEDCT_AMT", DecimalType(38,10), False),
    StructField("EGWP_DRUG_SPEND_TOWARD_GAP_AMT", DecimalType(38,10), False),
    StructField("EGWP_DRUG_SPEND_TOWARD_INIT_COV_AMT", DecimalType(38,10), False),
    StructField("EGWP_MBR_TOWARD_CATO_AMT", DecimalType(38,10), False),
    StructField("EGWP_MBR_TOWARD_DEDCT_AMT", DecimalType(38,10), False),
    StructField("EGWP_MBR_TOWARD_GAP_AMT", DecimalType(38,10), False),
    StructField("EGWP_MBR_TOWARD_INIT_COV_AMT", DecimalType(38,10), False),
    StructField("EGWP_PLN_CATO_AMT", DecimalType(38,10), False),
    StructField("EGWP_PLN_GAP_THRSHLD_AMT", DecimalType(38,10), False),
    StructField("EGWP_TOWARD_PLN_CATO_AMT", DecimalType(38,10), False),
    StructField("EGWP_TOWARD_PLN_DEDCT_AMT", DecimalType(38,10), False),
    StructField("EGWP_TOWARD_PLN_GAP_AMT", DecimalType(38,10), False),
    StructField("GAP_BRND_DSCNT_AMT", DecimalType(38,10), False),
    StructField("GROS_DRUG_CST_AFTR_AMT", DecimalType(38,10), False),
    StructField("GROS_DRUG_CST_BFR_AMT", DecimalType(38,10), False),
    StructField("LICS_DRUG_SPEND_DEDCT_AMT", DecimalType(38,10), False),
    StructField("LICS_DRUG_SPEND_DEDCT_BAL_AMT", DecimalType(38,10), False),
    StructField("LOW_INCM_CST_SHR_SBSDY_AMT", DecimalType(38,10), False),
    StructField("MCAREB_DRUG_SPEND_TOWARD_OOP_AMT", DecimalType(38,10), False),
    StructField("MCAREB_MBR_TOWARD_OOP_AMT", DecimalType(38,10), False),
    StructField("MBR_TOWARD_CATO_AMT", DecimalType(38,10), False),
    StructField("MBR_TOWARD_DEDCT_AMT", DecimalType(38,10), False),
    StructField("MBR_TOWARD_GAP_AMT", DecimalType(38,10), False),
    StructField("MBR_TOWARD_INIT_COV_AMT", DecimalType(38,10), False),
    StructField("NCOV_PLN_PD_AMT", DecimalType(38,10), False),
    StructField("PRIOR_TRUE_OOP_AMT", DecimalType(38,10), False),
    StructField("RPTD_GAP_DSCNT_AMT", DecimalType(38,10), False),
    StructField("SUBRO_AMT", DecimalType(38,10), False),
    StructField("TRUE_OOP_ACCUM_AMT", DecimalType(38,10), False),
    StructField("TOT_GROS_DRUG_CST_ACCUM_AMT", DecimalType(38,10), False),
    StructField("TOWARD_CATO_PHS_DRUG_SPEND_AMT", DecimalType(38,10), False),
    StructField("TOWARD_GAP_PHS_DRUG_SPEND_AMT", DecimalType(38,10), False),
    StructField("TOWARD_PLN_CATO_AMT", DecimalType(38,10), False),
    StructField("TOWARD_PLN_DEDCT_AMT", DecimalType(38,10), False),
    StructField("TOWARD_PLN_GAP_AMT", DecimalType(38,10), False),
    StructField("BEG_BNF_PHS_ID", StringType(), False),
    StructField("END_BNF_PHS_ID", StringType(), False),
    StructField("MCARE_CNTR_ID", StringType(), False),
    StructField("PLN_BNF_PCKG_ID", StringType(), False),
    StructField("TRUE_OOP_SCHD_ID", StringType(), False)
])

file_path_Seq_DRUG_CLM_MCARE_BNF_PHS_DTL = f"{adls_path}/key/DRUG_CLM_MCARE_BNF_PHS_DTL.{RunID}.dat"
df_Seq_DRUG_CLM_MCARE_BNF_PHS_DTL = (
    spark.read.format("csv")
    .option("header", "false")
    .option("sep", ",")
    .option("quote", "\"")
    .option("escape", "\"")
    .option("nullValue", None)
    .schema(schema_Seq_DRUG_CLM_MCARE_BNF_PHS_DTL)
    .load(file_path_Seq_DRUG_CLM_MCARE_BNF_PHS_DTL)
)

# Transformer Trans1
df_Trans1 = df_Seq_DRUG_CLM_MCARE_BNF_PHS_DTL.select(
    F.col("DRUG_CLM_MCARE_BNF_PHS_DTL_SK").alias("DRUG_CLM_MCARE_BNF_PHS_DTL_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_REPRCS_SEQ_NO").alias("CLM_REPRCS_SEQ_NO"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("DRUG_CLM_SK").alias("DRUG_CLM_SK"),
    F.col("DRUG_COV_STTUS_CD").alias("DRUG_COV_STTUS_CD"),
    F.col("MEDSPAN_GNRC_DRUG_IN").alias("MEDSPAN_GNRC_DRUG_IN"),
    F.col("CLM_SUBMT_DT_SK").alias("CLM_SUBMT_DT_SK"),
    F.col("ORIG_CLM_RCVD_DT_SK").alias("ORIG_CLM_RCVD_DT_SK"),
    F.col("CLM_REPRCS_PAYBL_AMT").alias("CLM_REPRCS_PAYBL_AMT"),
    F.col("CLM_REPRCS_PATN_RESP_AMT").alias("CLM_REPRCS_PATN_RESP_AMT"),
    F.col("COV_PLN_PD_AMT").alias("COV_PLN_PD_AMT"),
    F.col("DEDCT_LVL_DRUG_SPEND_THRSHLD_AMT").alias("DEDCT_LVL_DRUG_SPEND_THRSHLD_AMT"),
    F.col("DEDCT_LVL_TRUE_OOP_THRSHLD_AMT").alias("DEDCT_LVL_TRUE_OOP_THRSHLD_AMT"),
    F.col("DRUG_SPEND_AMT").alias("DRUG_SPEND_AMT"),
    F.col("DRUG_SPEND_TOWARD_DEDCT_AMT").alias("DRUG_SPEND_TOWARD_DEDCT_AMT"),
    F.col("DRUG_SPEND_TOWARD_INIT_COV_AMT").alias("DRUG_SPEND_TOWARD_INIT_COV_AMT"),
    F.col("DUAL_AMT").alias("DUAL_AMT"),
    F.col("EGWP_DEDCT_LVL_TROOP_THRSHLD_AMT").alias("EGWP_DEDCT_LVL_TROOP_THRSHLD_AMT"),
    F.col("EGWP_DRUG_SPEND_TOWARD_CATO_AMT").alias("EGWP_DRUG_SPEND_TOWARD_CATO_AMT"),
    F.col("EGWP_DRUG_SPEND_TOWARD_DEDCT_AMT").alias("EGWP_DRUG_SPEND_TOWARD_DEDCT_AMT"),
    F.col("EGWP_DRUG_SPEND_TOWARD_GAP_AMT").alias("EGWP_DRUG_SPEND_TOWARD_GAP_AMT"),
    F.col("EGWP_DRUG_SPEND_TOWARD_INIT_COV_AMT").alias("EGWP_DRUG_SPEND_TOWARD_INIT_COV_AMT"),
    F.col("EGWP_MBR_TOWARD_CATO_AMT").alias("EGWP_MBR_TOWARD_CATO_AMT"),
    F.col("EGWP_MBR_TOWARD_DEDCT_AMT").alias("EGWP_MBR_TOWARD_DEDCT_AMT"),
    F.col("EGWP_MBR_TOWARD_GAP_AMT").alias("EGWP_MBR_TOWARD_GAP_AMT"),
    F.col("EGWP_MBR_TOWARD_INIT_COV_AMT").alias("EGWP_MBR_TOWARD_INIT_COV_AMT"),
    F.col("EGWP_PLN_CATO_AMT").alias("EGWP_PLN_CATO_AMT"),
    F.col("EGWP_PLN_GAP_THRSHLD_AMT").alias("EGWP_PLN_GAP_THRSHLD_AMT"),
    F.col("EGWP_TOWARD_PLN_CATO_AMT").alias("EGWP_TOWARD_PLN_CATO_AMT"),
    F.col("EGWP_TOWARD_PLN_DEDCT_AMT").alias("EGWP_TOWARD_PLN_DEDCT_AMT"),
    F.col("EGWP_TOWARD_PLN_GAP_AMT").alias("EGWP_TOWARD_PLN_GAP_AMT"),
    F.col("GAP_BRND_DSCNT_AMT").alias("GAP_BRND_DSCNT_AMT"),
    F.col("GROS_DRUG_CST_AFTR_AMT").alias("GROS_DRUG_CST_AFTR_AMT"),
    F.col("GROS_DRUG_CST_BFR_AMT").alias("GROS_DRUG_CST_BFR_AMT"),
    F.col("LICS_DRUG_SPEND_DEDCT_AMT").alias("LICS_DRUG_SPEND_DEDCT_AMT"),
    F.col("LICS_DRUG_SPEND_DEDCT_BAL_AMT").alias("LICS_DRUG_SPEND_DEDCT_BAL_AMT"),
    F.col("LOW_INCM_CST_SHR_SBSDY_AMT").alias("LOW_INCM_CST_SHR_SBSDY_AMT"),
    F.col("MCAREB_DRUG_SPEND_TOWARD_OOP_AMT").alias("MCAREB_DRUG_SPEND_TOWARD_OOP_AMT"),
    F.col("MCAREB_MBR_TOWARD_OOP_AMT").alias("MCAREB_MBR_TOWARD_OOP_AMT"),
    F.col("MBR_TOWARD_CATO_AMT").alias("MBR_TOWARD_CATO_AMT"),
    F.col("MBR_TOWARD_DEDCT_AMT").alias("MBR_TOWARD_DEDCT_AMT"),
    F.col("MBR_TOWARD_GAP_AMT").alias("MBR_TOWARD_GAP_AMT"),
    F.col("MBR_TOWARD_INIT_COV_AMT").alias("MBR_TOWARD_INIT_COV_AMT"),
    F.col("NCOV_PLN_PD_AMT").alias("NCOV_PLN_PD_AMT"),
    F.col("PRIOR_TRUE_OOP_AMT").alias("PRIOR_TRUE_OOP_AMT"),
    F.col("RPTD_GAP_DSCNT_AMT").alias("RPTD_GAP_DSCNT_AMT"),
    F.col("SUBRO_AMT").alias("SUBRO_AMT"),
    F.col("TRUE_OOP_ACCUM_AMT").alias("TRUE_OOP_ACCUM_AMT"),
    F.col("TOT_GROS_DRUG_CST_ACCUM_AMT").alias("TOT_GROS_DRUG_CST_ACCUM_AMT"),
    F.col("TOWARD_CATO_PHS_DRUG_SPEND_AMT").alias("TOWARD_CATO_PHS_DRUG_SPEND_AMT"),
    F.col("TOWARD_GAP_PHS_DRUG_SPEND_AMT").alias("TOWARD_GAP_PHS_DRUG_SPEND_AMT"),
    F.col("TOWARD_PLN_CATO_AMT").alias("TOWARD_PLN_CATO_AMT"),
    F.col("TOWARD_PLN_DEDCT_AMT").alias("TOWARD_PLN_DEDCT_AMT"),
    F.col("TOWARD_PLN_GAP_AMT").alias("TOWARD_PLN_GAP_AMT"),
    F.col("BEG_BNF_PHS_ID").alias("BEG_BNF_PHS_ID"),
    F.col("END_BNF_PHS_ID").alias("END_BNF_PHS_ID"),
    F.col("MCARE_CNTR_ID").alias("MCARE_CNTR_ID"),
    F.col("PLN_BNF_PCKG_ID").alias("PLN_BNF_PCKG_ID"),
    F.col("TRUE_OOP_SCHD_ID").alias("TRUE_OOP_SCHD_ID")
)

# Read reference DB2 (db2_DrugClm) for the lookup (Lkp_DRUG_CLM)
db2_DrugClm_query = f"""
SELECT
DRUG_CLM.CLM_ID AS CLM_ID,
CD_MPPNG.SRC_CD AS SRC_CD,
DRUG_CLM.DRUG_CLM_SK AS DRUG_CLM_SK
FROM {IDSOwner}.DRUG_CLM DRUG_CLM
JOIN {IDSOwner}.CD_MPPNG CD_MPPNG
  ON DRUG_CLM.SRC_SYS_CD_SK = CD_MPPNG.CD_MPPNG_SK
"""
df_db2_DrugClm = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", db2_DrugClm_query)
    .load()
)

# Perform the left join for Lkp_DRUG_CLM
df_Lkp_DRUG_CLM_ = df_Trans1.alias("Lnk_Lkp").join(
    df_db2_DrugClm.alias("Lnk_db2_DrugClm"),
    [
        F.col("Lnk_Lkp.CLM_ID") == F.col("Lnk_db2_DrugClm.CLM_ID"),
        F.col("Lnk_Lkp.SRC_SYS_CD") == F.col("Lnk_db2_DrugClm.SRC_CD")
    ],
    how="left"
)

# Main output link from Lkp_DRUG_CLM
df_Lkp_DRUG_CLM_main = df_Lkp_DRUG_CLM_.select(
    F.col("Lnk_Lkp.DRUG_CLM_MCARE_BNF_PHS_DTL_SK").alias("DRUG_CLM_MCARE_BNF_PHS_DTL_SK"),
    F.col("Lnk_Lkp.CLM_ID").alias("CLM_ID"),
    F.col("Lnk_Lkp.CLM_REPRCS_SEQ_NO").alias("CLM_REPRCS_SEQ_NO"),
    F.col("Lnk_Lkp.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Lnk_Lkp.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Lnk_Lkp.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Lnk_db2_DrugClm.DRUG_CLM_SK").alias("DRUG_CLM_SK"),
    F.col("Lnk_Lkp.DRUG_COV_STTUS_CD").alias("DRUG_COV_STTUS_CD"),
    F.col("Lnk_Lkp.MEDSPAN_GNRC_DRUG_IN").alias("MEDSPAN_GNRC_DRUG_IN"),
    F.col("Lnk_Lkp.CLM_SUBMT_DT_SK").alias("CLM_SUBMT_DT_SK"),
    F.col("Lnk_Lkp.ORIG_CLM_RCVD_DT_SK").alias("ORIG_CLM_RCVD_DT_SK"),
    F.col("Lnk_Lkp.CLM_REPRCS_PAYBL_AMT").alias("CLM_REPRCS_PAYBL_AMT"),
    F.col("Lnk_Lkp.CLM_REPRCS_PATN_RESP_AMT").alias("CLM_REPRCS_PATN_RESP_AMT"),
    F.col("Lnk_Lkp.COV_PLN_PD_AMT").alias("COV_PLN_PD_AMT"),
    F.col("Lnk_Lkp.DEDCT_LVL_DRUG_SPEND_THRSHLD_AMT").alias("DEDCT_LVL_DRUG_SPEND_THRSHLD_AMT"),
    F.col("Lnk_Lkp.DEDCT_LVL_TRUE_OOP_THRSHLD_AMT").alias("DEDCT_LVL_TRUE_OOP_THRSHLD_AMT"),
    F.col("Lnk_Lkp.DRUG_SPEND_AMT").alias("DRUG_SPEND_AMT"),
    F.col("Lnk_Lkp.DRUG_SPEND_TOWARD_DEDCT_AMT").alias("DRUG_SPEND_TOWARD_DEDCT_AMT"),
    F.col("Lnk_Lkp.DRUG_SPEND_TOWARD_INIT_COV_AMT").alias("DRUG_SPEND_TOWARD_INIT_COV_AMT"),
    F.col("Lnk_Lkp.DUAL_AMT").alias("DUAL_AMT"),
    F.col("Lnk_Lkp.EGWP_DEDCT_LVL_TROOP_THRSHLD_AMT").alias("EGWP_DEDCT_LVL_TROOP_THRSHLD_AMT"),
    F.col("Lnk_Lkp.EGWP_DRUG_SPEND_TOWARD_CATO_AMT").alias("EGWP_DRUG_SPEND_TOWARD_CATO_AMT"),
    F.col("Lnk_Lkp.EGWP_DRUG_SPEND_TOWARD_DEDCT_AMT").alias("EGWP_DRUG_SPEND_TOWARD_DEDCT_AMT"),
    F.col("Lnk_Lkp.EGWP_DRUG_SPEND_TOWARD_GAP_AMT").alias("EGWP_DRUG_SPEND_TOWARD_GAP_AMT"),
    F.col("Lnk_Lkp.EGWP_DRUG_SPEND_TOWARD_INIT_COV_AMT").alias("EGWP_DRUG_SPEND_TOWARD_INIT_COV_AMT"),
    F.col("Lnk_Lkp.EGWP_MBR_TOWARD_CATO_AMT").alias("EGWP_MBR_TOWARD_CATO_AMT"),
    F.col("Lnk_Lkp.EGWP_MBR_TOWARD_DEDCT_AMT").alias("EGWP_MBR_TOWARD_DEDCT_AMT"),
    F.col("Lnk_Lkp.EGWP_MBR_TOWARD_GAP_AMT").alias("EGWP_MBR_TOWARD_GAP_AMT"),
    F.col("Lnk_Lkp.EGWP_MBR_TOWARD_INIT_COV_AMT").alias("EGWP_MBR_TOWARD_INIT_COV_AMT"),
    F.col("Lnk_Lkp.EGWP_PLN_CATO_AMT").alias("EGWP_PLN_CATO_AMT"),
    F.col("Lnk_Lkp.EGWP_PLN_GAP_THRSHLD_AMT").alias("EGWP_PLN_GAP_THRSHLD_AMT"),
    F.col("Lnk_Lkp.EGWP_TOWARD_PLN_CATO_AMT").alias("EGWP_TOWARD_PLN_CATO_AMT"),
    F.col("Lnk_Lkp.EGWP_TOWARD_PLN_DEDCT_AMT").alias("EGWP_TOWARD_PLN_DEDCT_AMT"),
    F.col("Lnk_Lkp.EGWP_TOWARD_PLN_GAP_AMT").alias("EGWP_TOWARD_PLN_GAP_AMT"),
    F.col("Lnk_Lkp.GAP_BRND_DSCNT_AMT").alias("GAP_BRND_DSCNT_AMT"),
    F.col("Lnk_Lkp.GROS_DRUG_CST_AFTR_AMT").alias("GROS_DRUG_CST_AFTR_AMT"),
    F.col("Lnk_Lkp.GROS_DRUG_CST_BFR_AMT").alias("GROS_DRUG_CST_BFR_AMT"),
    F.col("Lnk_Lkp.LICS_DRUG_SPEND_DEDCT_AMT").alias("LICS_DRUG_SPEND_DEDCT_AMT"),
    F.col("Lnk_Lkp.LICS_DRUG_SPEND_DEDCT_BAL_AMT").alias("LICS_DRUG_SPEND_DEDCT_BAL_AMT"),
    F.col("Lnk_Lkp.LOW_INCM_CST_SHR_SBSDY_AMT").alias("LOW_INCM_CST_SHR_SBSDY_AMT"),
    F.col("Lnk_Lkp.MCAREB_DRUG_SPEND_TOWARD_OOP_AMT").alias("MCAREB_DRUG_SPEND_TOWARD_OOP_AMT"),
    F.col("Lnk_Lkp.MCAREB_MBR_TOWARD_OOP_AMT").alias("MCAREB_MBR_TOWARD_OOP_AMT"),
    F.col("Lnk_Lkp.MBR_TOWARD_CATO_AMT").alias("MBR_TOWARD_CATO_AMT"),
    F.col("Lnk_Lkp.MBR_TOWARD_DEDCT_AMT").alias("MBR_TOWARD_DEDCT_AMT"),
    F.col("Lnk_Lkp.MBR_TOWARD_GAP_AMT").alias("MBR_TOWARD_GAP_AMT"),
    F.col("Lnk_Lkp.MBR_TOWARD_INIT_COV_AMT").alias("MBR_TOWARD_INIT_COV_AMT"),
    F.col("Lnk_Lkp.NCOV_PLN_PD_AMT").alias("NCOV_PLN_PD_AMT"),
    F.col("Lnk_Lkp.PRIOR_TRUE_OOP_AMT").alias("PRIOR_TRUE_OOP_AMT"),
    F.col("Lnk_Lkp.RPTD_GAP_DSCNT_AMT").alias("RPTD_GAP_DSCNT_AMT"),
    F.col("Lnk_Lkp.SUBRO_AMT").alias("SUBRO_AMT"),
    F.col("Lnk_Lkp.TRUE_OOP_ACCUM_AMT").alias("TRUE_OOP_ACCUM_AMT"),
    F.col("Lnk_Lkp.TOT_GROS_DRUG_CST_ACCUM_AMT").alias("TOT_GROS_DRUG_CST_ACCUM_AMT"),
    F.col("Lnk_Lkp.TOWARD_CATO_PHS_DRUG_SPEND_AMT").alias("TOWARD_CATO_PHS_DRUG_SPEND_AMT"),
    F.col("Lnk_Lkp.TOWARD_GAP_PHS_DRUG_SPEND_AMT").alias("TOWARD_GAP_PHS_DRUG_SPEND_AMT"),
    F.col("Lnk_Lkp.TOWARD_PLN_CATO_AMT").alias("TOWARD_PLN_CATO_AMT"),
    F.col("Lnk_Lkp.TOWARD_PLN_DEDCT_AMT").alias("TOWARD_PLN_DEDCT_AMT"),
    F.col("Lnk_Lkp.TOWARD_PLN_GAP_AMT").alias("TOWARD_PLN_GAP_AMT"),
    F.col("Lnk_Lkp.BEG_BNF_PHS_ID").alias("BEG_BNF_PHS_ID"),
    F.col("Lnk_Lkp.END_BNF_PHS_ID").alias("END_BNF_PHS_ID"),
    F.col("Lnk_Lkp.MCARE_CNTR_ID").alias("MCARE_CNTR_ID"),
    F.col("Lnk_Lkp.PLN_BNF_PCKG_ID").alias("PLN_BNF_PCKG_ID"),
    F.col("Lnk_Lkp.TRUE_OOP_SCHD_ID").alias("TRUE_OOP_SCHD_ID")
)

# Reject link from Lkp_DRUG_CLM
df_Lkp_DRUG_CLM_reject = df_Lkp_DRUG_CLM_.filter(F.col("Lnk_db2_DrugClm.DRUG_CLM_SK").isNull()).select(
    df_Trans1.columns
)

# Write the reject file
# Apply rpad for char/varchar columns in the same order as input
reject_columns = []
for field in schema_Seq_DRUG_CLM_MCARE_BNF_PHS_DTL.fields:
    col_name = field.name
    col_type = field.dataType
    if isinstance(col_type, StringType):
        # Check if there's a length in the metadata (for char)
        if "Length" in field.metadata and field.metadata["Length"]:
            reject_columns.append(F.rpad(F.col(col_name), field.metadata["Length"], " ").alias(col_name))
        else:
            reject_columns.append(F.rpad(F.col(col_name), F.lit(<...>), " ").alias(col_name))
    else:
        reject_columns.append(F.col(col_name))

df_Lkp_DRUG_CLM_reject_final = df_Lkp_DRUG_CLM_reject.select(*reject_columns)
reject_file_path = f"{adls_path}/load/REJECT_DRUG_CLM_MCARE_BNF_PHS_DTL.{RunID}.dat"
write_files(
    df_Lkp_DRUG_CLM_reject_final,
    reject_file_path,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)

# Read reference DB2 (CD_MPPNG)
cd_mppng_sql = f"""
SELECT
CD_MPPNG_SK,
SRC_DRVD_LKUP_VAL,
SRC_SYS_CD
FROM {IDSOwner}.CD_MPPNG
WHERE SRC_SYS_CD='{SrcSysCd}'
  AND SRC_CLCTN_CD='{SrcSysCd}'
  AND SRC_DOMAIN_NM='DRUG COVERAGE STATUS'
  AND TRGT_CLCTN_CD='IDS'
  AND TRGT_DOMAIN_NM='DRUG COVERAGE STATUS'
"""
df_cd_mppng = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", cd_mppng_sql)
    .load()
)

# Lkp_CdMppng (left join)
df_Lkp_CdMppng_ = df_Lkp_DRUG_CLM_main.alias("Lnk_CdMppngLkp").join(
    df_cd_mppng.alias("Lnk_DrugClm_Lkp"),
    [
        F.col("Lnk_CdMppngLkp.DRUG_COV_STTUS_CD") == F.col("Lnk_DrugClm_Lkp.SRC_DRVD_LKUP_VAL"),
        F.col("Lnk_CdMppngLkp.SRC_SYS_CD") == F.col("Lnk_DrugClm_Lkp.SRC_SYS_CD")
    ],
    how="left"
)

df_Lkp_CdMppng_main = df_Lkp_CdMppng_.select(
    F.col("Lnk_CdMppngLkp.DRUG_CLM_MCARE_BNF_PHS_DTL_SK").alias("DRUG_CLM_MCARE_BNF_PHS_DTL_SK"),
    F.col("Lnk_CdMppngLkp.CLM_ID").alias("CLM_ID"),
    F.col("Lnk_CdMppngLkp.CLM_REPRCS_SEQ_NO").alias("CLM_REPRCS_SEQ_NO"),
    F.col("Lnk_CdMppngLkp.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Lnk_CdMppngLkp.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Lnk_CdMppngLkp.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Lnk_CdMppngLkp.DRUG_CLM_SK").alias("DRUG_CLM_SK"),
    F.col("Lnk_DrugClm_Lkp.CD_MPPNG_SK").alias("DRUG_COV_STTUS_CD_SK"),
    F.col("Lnk_CdMppngLkp.MEDSPAN_GNRC_DRUG_IN").alias("MEDSPAN_GNRC_DRUG_IN"),
    F.col("Lnk_CdMppngLkp.CLM_SUBMT_DT_SK").alias("CLM_SUBMT_DT_SK"),
    F.col("Lnk_CdMppngLkp.ORIG_CLM_RCVD_DT_SK").alias("ORIG_CLM_RCVD_DT_SK"),
    F.col("Lnk_CdMppngLkp.CLM_REPRCS_PAYBL_AMT").alias("CLM_REPRCS_PAYBL_AMT"),
    F.col("Lnk_CdMppngLkp.CLM_REPRCS_PATN_RESP_AMT").alias("CLM_REPRCS_PATN_RESP_AMT"),
    F.col("Lnk_CdMppngLkp.COV_PLN_PD_AMT").alias("COV_PLN_PD_AMT"),
    F.col("Lnk_CdMppngLkp.DEDCT_LVL_DRUG_SPEND_THRSHLD_AMT").alias("DEDCT_LVL_DRUG_SPEND_THRSHLD_AMT"),
    F.col("Lnk_CdMppngLkp.DEDCT_LVL_TRUE_OOP_THRSHLD_AMT").alias("DEDCT_LVL_TRUE_OOP_THRSHLD_AMT"),
    F.col("Lnk_CdMppngLkp.DRUG_SPEND_AMT").alias("DRUG_SPEND_AMT"),
    F.col("Lnk_CdMppngLkp.DRUG_SPEND_TOWARD_DEDCT_AMT").alias("DRUG_SPEND_TOWARD_DEDCT_AMT"),
    F.col("Lnk_CdMppngLkp.DRUG_SPEND_TOWARD_INIT_COV_AMT").alias("DRUG_SPEND_TOWARD_INIT_COV_AMT"),
    F.col("Lnk_CdMppngLkp.DUAL_AMT").alias("DUAL_AMT"),
    F.col("Lnk_CdMppngLkp.EGWP_DEDCT_LVL_TROOP_THRSHLD_AMT").alias("EGWP_DEDCT_LVL_TROOP_THRSHLD_AMT"),
    F.col("Lnk_CdMppngLkp.EGWP_DRUG_SPEND_TOWARD_CATO_AMT").alias("EGWP_DRUG_SPEND_TOWARD_CATO_AMT"),
    F.col("Lnk_CdMppngLkp.EGWP_DRUG_SPEND_TOWARD_DEDCT_AMT").alias("EGWP_DRUG_SPEND_TOWARD_DEDCT_AMT"),
    F.col("Lnk_CdMppngLkp.EGWP_DRUG_SPEND_TOWARD_GAP_AMT").alias("EGWP_DRUG_SPEND_TOWARD_GAP_AMT"),
    F.col("Lnk_CdMppngLkp.EGWP_DRUG_SPEND_TOWARD_INIT_COV_AMT").alias("EGWP_DRUG_SPEND_TOWARD_INIT_COV_AMT"),
    F.col("Lnk_CdMppngLkp.EGWP_MBR_TOWARD_CATO_AMT").alias("EGWP_MBR_TOWARD_CATO_AMT"),
    F.col("Lnk_CdMppngLkp.EGWP_MBR_TOWARD_DEDCT_AMT").alias("EGWP_MBR_TOWARD_DEDCT_AMT"),
    F.col("Lnk_CdMppngLkp.EGWP_MBR_TOWARD_GAP_AMT").alias("EGWP_MBR_TOWARD_GAP_AMT"),
    F.col("Lnk_CdMppngLkp.EGWP_MBR_TOWARD_INIT_COV_AMT").alias("EGWP_MBR_TOWARD_INIT_COV_AMT"),
    F.col("Lnk_CdMppngLkp.EGWP_PLN_CATO_AMT").alias("EGWP_PLN_CATO_AMT"),
    F.col("Lnk_CdMppngLkp.EGWP_PLN_GAP_THRSHLD_AMT").alias("EGWP_PLN_GAP_THRSHLD_AMT"),
    F.col("Lnk_CdMppngLkp.EGWP_TOWARD_PLN_CATO_AMT").alias("EGWP_TOWARD_PLN_CATO_AMT"),
    F.col("Lnk_CdMppngLkp.EGWP_TOWARD_PLN_DEDCT_AMT").alias("EGWP_TOWARD_PLN_DEDCT_AMT"),
    F.col("Lnk_CdMppngLkp.EGWP_TOWARD_PLN_GAP_AMT").alias("EGWP_TOWARD_PLN_GAP_AMT"),
    F.col("Lnk_CdMppngLkp.GAP_BRND_DSCNT_AMT").alias("GAP_BRND_DSCNT_AMT"),
    F.col("Lnk_CdMppngLkp.GROS_DRUG_CST_AFTR_AMT").alias("GROS_DRUG_CST_AFTR_AMT"),
    F.col("Lnk_CdMppngLkp.GROS_DRUG_CST_BFR_AMT").alias("GROS_DRUG_CST_BFR_AMT"),
    F.col("Lnk_CdMppngLkp.LICS_DRUG_SPEND_DEDCT_AMT").alias("LICS_DRUG_SPEND_DEDCT_AMT"),
    F.col("Lnk_CdMppngLkp.LICS_DRUG_SPEND_DEDCT_BAL_AMT").alias("LICS_DRUG_SPEND_DEDCT_BAL_AMT"),
    F.col("Lnk_CdMppngLkp.LOW_INCM_CST_SHR_SBSDY_AMT").alias("LOW_INCM_CST_SHR_SBSDY_AMT"),
    F.col("Lnk_CdMppngLkp.MCAREB_DRUG_SPEND_TOWARD_OOP_AMT").alias("MCAREB_DRUG_SPEND_TOWARD_OOP_AMT"),
    F.col("Lnk_CdMppngLkp.MCAREB_MBR_TOWARD_OOP_AMT").alias("MCAREB_MBR_TOWARD_OOP_AMT"),
    F.col("Lnk_CdMppngLkp.MBR_TOWARD_CATO_AMT").alias("MBR_TOWARD_CATO_AMT"),
    F.col("Lnk_CdMppngLkp.MBR_TOWARD_DEDCT_AMT").alias("MBR_TOWARD_DEDCT_AMT"),
    F.col("Lnk_CdMppngLkp.MBR_TOWARD_GAP_AMT").alias("MBR_TOWARD_GAP_AMT"),
    F.col("Lnk_CdMppngLkp.MBR_TOWARD_INIT_COV_AMT").alias("MBR_TOWARD_INIT_COV_AMT"),
    F.col("Lnk_CdMppngLkp.NCOV_PLN_PD_AMT").alias("NCOV_PLN_PD_AMT"),
    F.col("Lnk_CdMppngLkp.PRIOR_TRUE_OOP_AMT").alias("PRIOR_TRUE_OOP_AMT"),
    F.col("Lnk_CdMppngLkp.RPTD_GAP_DSCNT_AMT").alias("RPTD_GAP_DSCNT_AMT"),
    F.col("Lnk_CdMppngLkp.SUBRO_AMT").alias("SUBRO_AMT"),
    F.col("Lnk_CdMppngLkp.TRUE_OOP_ACCUM_AMT").alias("TRUE_OOP_ACCUM_AMT"),
    F.col("Lnk_CdMppngLkp.TOT_GROS_DRUG_CST_ACCUM_AMT").alias("TOT_GROS_DRUG_CST_ACCUM_AMT"),
    F.col("Lnk_CdMppngLkp.TOWARD_CATO_PHS_DRUG_SPEND_AMT").alias("TOWARD_CATO_PHS_DRUG_SPEND_AMT"),
    F.col("Lnk_CdMppngLkp.TOWARD_GAP_PHS_DRUG_SPEND_AMT").alias("TOWARD_GAP_PHS_DRUG_SPEND_AMT"),
    F.col("Lnk_CdMppngLkp.TOWARD_PLN_CATO_AMT").alias("TOWARD_PLN_CATO_AMT"),
    F.col("Lnk_CdMppngLkp.TOWARD_PLN_DEDCT_AMT").alias("TOWARD_PLN_DEDCT_AMT"),
    F.col("Lnk_CdMppngLkp.TOWARD_PLN_GAP_AMT").alias("TOWARD_PLN_GAP_AMT"),
    F.col("Lnk_CdMppngLkp.BEG_BNF_PHS_ID").alias("BEG_BNF_PHS_ID"),
    F.col("Lnk_CdMppngLkp.END_BNF_PHS_ID").alias("END_BNF_PHS_ID"),
    F.col("Lnk_CdMppngLkp.MCARE_CNTR_ID").alias("MCARE_CNTR_ID"),
    F.col("Lnk_CdMppngLkp.PLN_BNF_PCKG_ID").alias("PLN_BNF_PCKG_ID"),
    F.col("Lnk_CdMppngLkp.TRUE_OOP_SCHD_ID").alias("TRUE_OOP_SCHD_ID")
)

# Trans2 splits into Lnk_FkeyOut (all rows), LnkUNK (constraint row=1 with UNK data), LnkNA (constraint row=1 with NA data)
df_Trans2_in = df_Lkp_CdMppng_main
w = Window.orderBy(F.lit(1))
df_with_rn = df_Trans2_in.withColumn("rn", F.row_number().over(w))

df_Trans2_fkeyout = df_Trans2_in

df_Trans2_unk_selected = df_with_rn.filter(F.col("rn") == 1)
df_Trans2_na_selected = df_with_rn.filter(F.col("rn") == 1)

cols_Trans2 = df_Trans2_in.columns

df_Trans2_unk = df_Trans2_unk_selected.select(
    F.lit(0).alias("DRUG_CLM_MCARE_BNF_PHS_DTL_SK"),
    F.lit("UNK").alias("CLM_ID"),
    F.lit(0).alias("CLM_REPRCS_SEQ_NO"),
    F.lit("UNK").alias("SRC_SYS_CD"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("DRUG_CLM_SK"),
    F.lit(0).alias("DRUG_COV_STTUS_CD_SK"),
    F.lit(0).alias("MEDSPAN_GNRC_DRUG_IN"),
    F.lit(0).alias("CLM_SUBMT_DT_SK"),
    F.lit(0).alias("ORIG_CLM_RCVD_DT_SK"),
    F.lit(0.00).alias("CLM_REPRCS_PAYBL_AMT"),
    F.lit(0.00).alias("CLM_REPRCS_PATN_RESP_AMT"),
    F.lit(0.00).alias("COV_PLN_PD_AMT"),
    F.lit(0.00).alias("DEDCT_LVL_DRUG_SPEND_THRSHLD_AMT"),
    F.lit(0.00).alias("DEDCT_LVL_TRUE_OOP_THRSHLD_AMT"),
    F.lit(0.00).alias("DRUG_SPEND_AMT"),
    F.lit(0.00).alias("DRUG_SPEND_TOWARD_DEDCT_AMT"),
    F.lit(0.00).alias("DRUG_SPEND_TOWARD_INIT_COV_AMT"),
    F.lit(0.00).alias("DUAL_AMT"),
    F.lit(0.00).alias("EGWP_DEDCT_LVL_TROOP_THRSHLD_AMT"),
    F.lit(0.00).alias("EGWP_DRUG_SPEND_TOWARD_CATO_AMT"),
    F.lit(0.00).alias("EGWP_DRUG_SPEND_TOWARD_DEDCT_AMT"),
    F.lit(0.00).alias("EGWP_DRUG_SPEND_TOWARD_GAP_AMT"),
    F.lit(0.00).alias("EGWP_DRUG_SPEND_TOWARD_INIT_COV_AMT"),
    F.lit(0.00).alias("EGWP_MBR_TOWARD_CATO_AMT"),
    F.lit(0.00).alias("EGWP_MBR_TOWARD_DEDCT_AMT"),
    F.lit(0.00).alias("EGWP_MBR_TOWARD_GAP_AMT"),
    F.lit(0.00).alias("EGWP_MBR_TOWARD_INIT_COV_AMT"),
    F.lit(0.00).alias("EGWP_PLN_CATO_AMT"),
    F.lit(0.00).alias("EGWP_PLN_GAP_THRSHLD_AMT"),
    F.lit(0.00).alias("EGWP_TOWARD_PLN_CATO_AMT"),
    F.lit(0.00).alias("EGWP_TOWARD_PLN_DEDCT_AMT"),
    F.lit(0.00).alias("EGWP_TOWARD_PLN_GAP_AMT"),
    F.lit(0.00).alias("GAP_BRND_DSCNT_AMT"),
    F.lit(0.00).alias("GROS_DRUG_CST_AFTR_AMT"),
    F.lit(0.00).alias("GROS_DRUG_CST_BFR_AMT"),
    F.lit(0.00).alias("LICS_DRUG_SPEND_DEDCT_AMT"),
    F.lit(0.00).alias("LICS_DRUG_SPEND_DEDCT_BAL_AMT"),
    F.lit(0.00).alias("LOW_INCM_CST_SHR_SBSDY_AMT"),
    F.lit(0.00).alias("MCAREB_DRUG_SPEND_TOWARD_OOP_AMT"),
    F.lit(0.00).alias("MCAREB_MBR_TOWARD_OOP_AMT"),
    F.lit(0.00).alias("MBR_TOWARD_CATO_AMT"),
    F.lit(0.00).alias("MBR_TOWARD_DEDCT_AMT"),
    F.lit(0.00).alias("MBR_TOWARD_GAP_AMT"),
    F.lit(0.00).alias("MBR_TOWARD_INIT_COV_AMT"),
    F.lit(0.00).alias("NCOV_PLN_PD_AMT"),
    F.lit(0.00).alias("PRIOR_TRUE_OOP_AMT"),
    F.lit(0.00).alias("RPTD_GAP_DSCNT_AMT"),
    F.lit(0.00).alias("SUBRO_AMT"),
    F.lit(0.00).alias("TRUE_OOP_ACCUM_AMT"),
    F.lit(0.00).alias("TOT_GROS_DRUG_CST_ACCUM_AMT"),
    F.lit(0.00).alias("TOWARD_CATO_PHS_DRUG_SPEND_AMT"),
    F.lit(0.00).alias("TOWARD_GAP_PHS_DRUG_SPEND_AMT"),
    F.lit(0.00).alias("TOWARD_PLN_CATO_AMT"),
    F.lit(0.00).alias("TOWARD_PLN_DEDCT_AMT"),
    F.lit(0.00).alias("TOWARD_PLN_GAP_AMT"),
    F.lit(0).alias("BEG_BNF_PHS_ID"),
    F.lit(0).alias("END_BNF_PHS_ID"),
    F.lit(0).alias("MCARE_CNTR_ID"),
    F.lit(0).alias("PLN_BNF_PCKG_ID"),
    F.lit(0).alias("TRUE_OOP_SCHD_ID")
)

df_Trans2_na = df_Trans2_na_selected.select(
    F.lit(1).alias("DRUG_CLM_MCARE_BNF_PHS_DTL_SK"),
    F.lit("NA").alias("CLM_ID"),
    F.lit(1).alias("CLM_REPRCS_SEQ_NO"),
    F.lit("NA").alias("SRC_SYS_CD"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("DRUG_CLM_SK"),
    F.lit(1).alias("DRUG_COV_STTUS_CD_SK"),
    F.lit(1).alias("MEDSPAN_GNRC_DRUG_IN"),
    F.lit(1).alias("CLM_SUBMT_DT_SK"),
    F.lit(1).alias("ORIG_CLM_RCVD_DT_SK"),
    F.lit(0.00).alias("CLM_REPRCS_PAYBL_AMT"),
    F.lit(0.00).alias("CLM_REPRCS_PATN_RESP_AMT"),
    F.lit(0.00).alias("COV_PLN_PD_AMT"),
    F.lit(0.00).alias("DEDCT_LVL_DRUG_SPEND_THRSHLD_AMT"),
    F.lit(0.00).alias("DEDCT_LVL_TRUE_OOP_THRSHLD_AMT"),
    F.lit(0.00).alias("DRUG_SPEND_AMT"),
    F.lit(0.00).alias("DRUG_SPEND_TOWARD_DEDCT_AMT"),
    F.lit(0.00).alias("DRUG_SPEND_TOWARD_INIT_COV_AMT"),
    F.lit(0.00).alias("DUAL_AMT"),
    F.lit(0.00).alias("EGWP_DEDCT_LVL_TROOP_THRSHLD_AMT"),
    F.lit(0.00).alias("EGWP_DRUG_SPEND_TOWARD_CATO_AMT"),
    F.lit(0.00).alias("EGWP_DRUG_SPEND_TOWARD_DEDCT_AMT"),
    F.lit(0.00).alias("EGWP_DRUG_SPEND_TOWARD_GAP_AMT"),
    F.lit(0.00).alias("EGWP_DRUG_SPEND_TOWARD_INIT_COV_AMT"),
    F.lit(0.00).alias("EGWP_MBR_TOWARD_CATO_AMT"),
    F.lit(0.00).alias("EGWP_MBR_TOWARD_DEDCT_AMT"),
    F.lit(0.00).alias("EGWP_MBR_TOWARD_GAP_AMT"),
    F.lit(0.00).alias("EGWP_MBR_TOWARD_INIT_COV_AMT"),
    F.lit(0.00).alias("EGWP_PLN_CATO_AMT"),
    F.lit(0.00).alias("EGWP_PLN_GAP_THRSHLD_AMT"),
    F.lit(0.00).alias("EGWP_TOWARD_PLN_CATO_AMT"),
    F.lit(0.00).alias("EGWP_TOWARD_PLN_DEDCT_AMT"),
    F.lit(0.00).alias("EGWP_TOWARD_PLN_GAP_AMT"),
    F.lit(0.00).alias("GAP_BRND_DSCNT_AMT"),
    F.lit(0.00).alias("GROS_DRUG_CST_AFTR_AMT"),
    F.lit(0.00).alias("GROS_DRUG_CST_BFR_AMT"),
    F.lit(0.00).alias("LICS_DRUG_SPEND_DEDCT_AMT"),
    F.lit(0.00).alias("LICS_DRUG_SPEND_DEDCT_BAL_AMT"),
    F.lit(0.00).alias("LOW_INCM_CST_SHR_SBSDY_AMT"),
    F.lit(0.00).alias("MCAREB_DRUG_SPEND_TOWARD_OOP_AMT"),
    F.lit(0.00).alias("MCAREB_MBR_TOWARD_OOP_AMT"),
    F.lit(0.00).alias("MBR_TOWARD_CATO_AMT"),
    F.lit(0.00).alias("MBR_TOWARD_DEDCT_AMT"),
    F.lit(0.00).alias("MBR_TOWARD_GAP_AMT"),
    F.lit(0.00).alias("MBR_TOWARD_INIT_COV_AMT"),
    F.lit(0.00).alias("NCOV_PLN_PD_AMT"),
    F.lit(0.00).alias("PRIOR_TRUE_OOP_AMT"),
    F.lit(0.00).alias("RPTD_GAP_DSCNT_AMT"),
    F.lit(0.00).alias("SUBRO_AMT"),
    F.lit(0.00).alias("TRUE_OOP_ACCUM_AMT"),
    F.lit(0.00).alias("TOT_GROS_DRUG_CST_ACCUM_AMT"),
    F.lit(0.00).alias("TOWARD_CATO_PHS_DRUG_SPEND_AMT"),
    F.lit(0.00).alias("TOWARD_GAP_PHS_DRUG_SPEND_AMT"),
    F.lit(0.00).alias("TOWARD_PLN_CATO_AMT"),
    F.lit(0.00).alias("TOWARD_PLN_DEDCT_AMT"),
    F.lit(0.00).alias("TOWARD_PLN_GAP_AMT"),
    F.lit(1).alias("BEG_BNF_PHS_ID"),
    F.lit(1).alias("END_BNF_PHS_ID"),
    F.lit(1).alias("MCARE_CNTR_ID"),
    F.lit(1).alias("PLN_BNF_PCKG_ID"),
    F.lit(1).alias("TRUE_OOP_SCHD_ID")
)

# Funnel_170: union in order LnkUNK -> LnkNA -> Lnk_FkeyOut
df_funnel_170 = df_Trans2_unk.unionByName(df_Trans2_na).unionByName(df_Trans2_fkeyout)

# Final write to DrugClmMcareBnfPhsDt
# Apply rpad for any char/varchar columns. We examine the final funnel schema. 
final_columns = []
for field in df_funnel_170.schema.fields:
    fname = field.name
    ftype = field.dataType
    # Attempt to find this in the known metadata (some were char(1) or char(10)) if relevant
    # We match by name in the original input or transformations if found
    # Otherwise, if it's char/varchar and we have no length, we pass <...> 
    original_field = None
    for sf in schema_Seq_DRUG_CLM_MCARE_BNF_PHS_DTL.fields:
        if sf.name == fname:
            original_field = sf
            break
    if isinstance(ftype, StringType):
        length_val = None
        if original_field and "Length" in original_field.metadata:
            length_val = original_field.metadata["Length"]
        if length_val:
            final_columns.append(F.rpad(F.col(fname), length_val, " ").alias(fname))
        else:
            final_columns.append(F.rpad(F.col(fname), F.lit(<...>), " ").alias(fname))
    else:
        final_columns.append(F.col(fname))

df_final = df_funnel_170.select(*final_columns)

output_file_path = f"{adls_path}/load/DRUG_CLM_MCARE_BNF_PHS_DT.{SrcSysCd}.dat"
write_files(
    df_final,
    output_file_path,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue="\"\""
)