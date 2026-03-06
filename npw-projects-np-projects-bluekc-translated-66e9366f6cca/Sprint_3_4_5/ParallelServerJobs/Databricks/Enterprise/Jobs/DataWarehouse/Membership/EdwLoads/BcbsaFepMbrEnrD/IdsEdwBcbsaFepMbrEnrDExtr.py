# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC PROCESSING: Extracts data from IDS BCBSA_FEP_MBR table and creates a load file for EDW
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                                                     Development Project    Code Reviewer            Date Reviewed
# MAGIC ---------------------------            ---------------------     -----------------------------------     -----------------------------------------------------------                                           --------------------------------------- ------------------------------ --------------------------------------
# MAGIC Kalyan Neelam                2015-11-23          5403                                Initial Programming                                                                              EnterpriseDev1             Bhoomi Dasari            11/27/2015
# MAGIC Abhiram Dasarathy          2016-11-07	    5568 - HEDIS for 2015    Added FEP_MBR_ID column  to end of the processing	       EnterpriseDev2             Kalyan Neelam           2016-11-15
# MAGIC Kailash Jadhav                2017-06-27	    5781 - HEDIS                  Added FEP_COV_TYP_TX column to end of the processing	       EnterpriseDev1             Kalyan Neelam           2017-06-29

# MAGIC Get both Cd and NM from Code Mapping table.
# MAGIC Load file for BCBSA_FEP_MBR_ENR_D
# MAGIC Extract from IDS BCBSA_FEP_MBR_ENR table
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType
)
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
EDWRunDtCycle = get_widget_value('EDWRunDtCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# Stage: db2_BCBSA_FEP_MBR_ENR
extract_query = f"""
SELECT 
    BCBSA_FEP_MBR_ENR.BCBSA_FEP_MBR_ENR_SK, 
    BCBSA_FEP_MBR_ENR.MBR_UNIQ_KEY, 
    BCBSA_FEP_MBR_ENR.PROD_SH_NM, 
    BCBSA_FEP_MBR_ENR.FEP_MBR_ENR_EFF_DT_SK, 
    COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
    BCBSA_FEP_MBR_ENR.CRT_RUN_CYC_EXCTN_SK, 
    BCBSA_FEP_MBR_ENR.LAST_UPDT_RUN_CYC_EXCTN_SK, 
    BCBSA_FEP_MBR_ENR.MBR_SK, 
    BCBSA_FEP_MBR_ENR.PROD_SH_NM_SK, 
    BCBSA_FEP_MBR_ENR.BC_PLN_CD_SK, 
    BCBSA_FEP_MBR_ENR.BS_PLN_CD_SK, 
    BCBSA_FEP_MBR_ENR.CHMCL_DPNDC_BNF_IN, 
    BCBSA_FEP_MBR_ENR.CHMCL_DPNDC_IP_BNF_IN, 
    BCBSA_FEP_MBR_ENR.CHMCL_DPNDC_IP_PRTL_DAY_BNF_IN, 
    BCBSA_FEP_MBR_ENR.CHMCL_DPNDC_OP_BNF_IN, 
    BCBSA_FEP_MBR_ENR.CONF_COMM_IN, 
    BCBSA_FEP_MBR_ENR.DNTL_BNF_IN, 
    BCBSA_FEP_MBR_ENR.MNTL_HLTH_BNF_IN, 
    BCBSA_FEP_MBR_ENR.MNTL_HLTH_IP_BNF_IN, 
    BCBSA_FEP_MBR_ENR.MNTL_HLTH_IP_PRTL_DAY_BNF_IN, 
    BCBSA_FEP_MBR_ENR.MNTL_HLTH_OP_BNF_IN, 
    BCBSA_FEP_MBR_ENR.OP_BNF_IN, 
    BCBSA_FEP_MBR_ENR.PDX_BNF_IN, 
    BCBSA_FEP_MBR_ENR.FEP_MBR_ENR_TERM_DT_SK, 
    BCBSA_FEP_MBR_ENR.SRC_SYS_LAST_UPDT_DT_SK, 
    BCBSA_FEP_MBR_ENR.FEP_PLN_PROD_ID, 
    BCBSA_FEP_MBR_ENR.FEP_PLN_RGN_ID, 
    BCBSA_FEP_MBR_ENR.MBR_ID, 
    BCBSA_FEP_MBR_ENR.MBR_EMPLMT_STTUS_NM, 
    BCBSA_FEP_MBR_ENR.PROD_DESC,
    BCBSA_FEP_MBR_ENR.FEP_MBR_ID,
    BCBSA_FEP_MBR_ENR.FEP_COV_TYP_TX
FROM {IDSOwner}.BCBSA_FEP_MBR_ENR BCBSA_FEP_MBR_ENR
LEFT JOIN {IDSOwner}.CD_MPPNG CD  
  ON BCBSA_FEP_MBR_ENR.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
WHERE 
  (CD.TRGT_CD = 'BCBSA' AND BCBSA_FEP_MBR_ENR.LAST_UPDT_RUN_CYC_EXCTN_SK >= {IDSRunCycle})
"""
df_db2_BCBSA_FEP_MBR_ENR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# Stage: db2_CD_MPPNG
extract_query = f"""
SELECT
  CD.CD_MPPNG_SK,
  COALESCE(CD.TRGT_CD, 'UNK') TRGT_CD,
  CD.TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG CD
"""
df_db2_CD_MPPNG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# Stage: Copy (PxCopy) -> produces two outputs
df_lnk_BcPlnCd_In = df_db2_CD_MPPNG.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_lnk_BsPlnCd_In = df_db2_CD_MPPNG.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

# Stage: lkp_Codes (PxLookup)
df_lkp_Codes = (
    df_db2_BCBSA_FEP_MBR_ENR.alias("lnk_Grp_In")
    .join(
        df_lnk_BcPlnCd_In.alias("lnk_BcPlnCd_In"),
        F.col("lnk_Grp_In.BC_PLN_CD_SK") == F.col("lnk_BcPlnCd_In.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_lnk_BsPlnCd_In.alias("lnk_BsPlnCd_In"),
        F.col("lnk_Grp_In.BS_PLN_CD_SK") == F.col("lnk_BsPlnCd_In.CD_MPPNG_SK"),
        "left"
    )
    .select(
        F.col("lnk_Grp_In.BCBSA_FEP_MBR_ENR_SK").alias("BCBSA_FEP_MBR_ENR_SK"),
        F.col("lnk_Grp_In.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.col("lnk_Grp_In.PROD_SH_NM").alias("PROD_SH_NM"),
        F.col("lnk_Grp_In.FEP_MBR_ENR_EFF_DT_SK").alias("FEP_MBR_ENR_EFF_DT_SK"),
        F.col("lnk_Grp_In.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnk_Grp_In.MBR_SK").alias("MBR_SK"),
        F.col("lnk_Grp_In.PROD_SH_NM_SK").alias("PROD_SH_NM_SK"),
        F.col("lnk_BcPlnCd_In.TRGT_CD").alias("BC_PLN_CD"),
        F.col("lnk_BcPlnCd_In.TRGT_CD_NM").alias("BC_PLN_NM"),
        F.col("lnk_BsPlnCd_In.TRGT_CD").alias("BS_PLN_CD"),
        F.col("lnk_BsPlnCd_In.TRGT_CD_NM").alias("BS_PLN_NM"),
        F.col("lnk_Grp_In.CHMCL_DPNDC_BNF_IN").alias("CHMCL_DPNDC_BNF_IN"),
        F.col("lnk_Grp_In.CHMCL_DPNDC_IP_BNF_IN").alias("CHMCL_DPNDC_IP_BNF_IN"),
        F.col("lnk_Grp_In.CHMCL_DPNDC_IP_PRTL_DAY_BNF_IN").alias("CHMCL_DPNDC_IP_PRTL_DAY_BNF_IN"),
        F.col("lnk_Grp_In.CHMCL_DPNDC_OP_BNF_IN").alias("CHMCL_DPNDC_OP_BNF_IN"),
        F.col("lnk_Grp_In.CONF_COMM_IN").alias("CONF_COMM_IN"),
        F.col("lnk_Grp_In.DNTL_BNF_IN").alias("DNTL_BNF_IN"),
        F.col("lnk_Grp_In.MNTL_HLTH_BNF_IN").alias("MNTL_HLTH_BNF_IN"),
        F.col("lnk_Grp_In.MNTL_HLTH_IP_BNF_IN").alias("MNTL_HLTH_IP_BNF_IN"),
        F.col("lnk_Grp_In.MNTL_HLTH_IP_PRTL_DAY_BNF_IN").alias("MNTL_HLTH_IP_PRTL_DAY_BNF_IN"),
        F.col("lnk_Grp_In.MNTL_HLTH_OP_BNF_IN").alias("MNTL_HLTH_OP_BNF_IN"),
        F.col("lnk_Grp_In.OP_BNF_IN").alias("OP_BNF_IN"),
        F.col("lnk_Grp_In.PDX_BNF_IN").alias("PDX_BNF_IN"),
        F.col("lnk_Grp_In.FEP_MBR_ENR_TERM_DT_SK").alias("FEP_MBR_ENR_TERM_DT_SK"),
        F.col("lnk_Grp_In.SRC_SYS_LAST_UPDT_DT_SK").alias("SRC_SYS_LAST_UPDT_DT_SK"),
        F.col("lnk_Grp_In.FEP_PLN_PROD_ID").alias("FEP_PLN_PROD_ID"),
        F.col("lnk_Grp_In.FEP_PLN_RGN_ID").alias("FEP_PLN_RGN_ID"),
        F.col("lnk_Grp_In.MBR_ID").alias("MBR_ID"),
        F.col("lnk_Grp_In.MBR_EMPLMT_STTUS_NM").alias("MBR_EMPLMT_STTUS_NM"),
        F.col("lnk_Grp_In.PROD_DESC").alias("PROD_DESC"),
        F.col("lnk_Grp_In.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("lnk_Grp_In.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("lnk_Grp_In.BC_PLN_CD_SK").alias("BC_PLN_CD_SK"),
        F.col("lnk_Grp_In.BS_PLN_CD_SK").alias("BS_PLN_CD_SK"),
        F.col("lnk_Grp_In.FEP_MBR_ID").alias("FEP_MBR_ID"),
        F.col("lnk_Grp_In.FEP_COV_TYP_TX").alias("FEP_COV_TYP_TX")
    )
)

# Stage: xfm_BusinessLogic1 (CTransformerStage)

# Output link: lnk_Extract_Out
df_lnk_Extract_Out = (
    df_lkp_Codes
    .filter(
        (F.col("BCBSA_FEP_MBR_ENR_SK") != 0) &
        (F.col("BCBSA_FEP_MBR_ENR_SK") != 1)
    )
    .select(
        F.col("BCBSA_FEP_MBR_ENR_SK").alias("BCBSA_FEP_MBR_ENR_SK"),
        F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.col("PROD_SH_NM").alias("PROD_SH_NM"),
        F.col("FEP_MBR_ENR_EFF_DT_SK").alias("FEP_MBR_ENR_EFF_DT_SK"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.lit(EDWRunDtCycle).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.lit(EDWRunDtCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.col("MBR_SK").alias("MBR_SK"),
        F.col("PROD_SH_NM_SK").alias("PROD_SH_NM_SK"),
        F.when(F.col("BC_PLN_CD").isNull(), "UNK").otherwise(F.col("BC_PLN_CD")).alias("BC_PLN_CD"),
        F.when(F.col("BC_PLN_NM").isNull(), "UNK").otherwise(F.col("BC_PLN_NM")).alias("BC_PLN_NM"),
        F.when(F.col("BS_PLN_CD").isNull(), "UNK").otherwise(F.col("BS_PLN_CD")).alias("BS_PLN_CD"),
        F.when(F.col("BS_PLN_NM").isNull(), "UNK").otherwise(F.col("BS_PLN_NM")).alias("BS_PLN_NM"),
        F.col("CHMCL_DPNDC_BNF_IN").alias("CHMCL_DPNDC_BNF_IN"),
        F.col("CHMCL_DPNDC_IP_BNF_IN").alias("CHMCL_DPNDC_IP_BNF_IN"),
        F.col("CHMCL_DPNDC_IP_PRTL_DAY_BNF_IN").alias("CHMCL_DPNDC_IP_PRTL_DAY_BNF_IN"),
        F.col("CHMCL_DPNDC_OP_BNF_IN").alias("CHMCL_DPNDC_OP_BNF_IN"),
        F.col("CONF_COMM_IN").alias("CONF_COMM_IN"),
        F.col("DNTL_BNF_IN").alias("DNTL_BNF_IN"),
        F.col("MNTL_HLTH_BNF_IN").alias("MNTL_HLTH_BNF_IN"),
        F.col("MNTL_HLTH_IP_BNF_IN").alias("MNTL_HLTH_IP_BNF_IN"),
        F.col("MNTL_HLTH_IP_PRTL_DAY_BNF_IN").alias("MNTL_HLTH_IP_PRTL_DAY_BNF_IN"),
        F.col("MNTL_HLTH_OP_BNF_IN").alias("MNTL_HLTH_OP_BNF_IN"),
        F.col("OP_BNF_IN").alias("OP_BNF_IN"),
        F.col("PDX_BNF_IN").alias("PDX_BNF_IN"),
        F.col("FEP_MBR_ENR_TERM_DT_SK").alias("FEP_MBR_ENR_TERM_DT_SK"),
        F.col("SRC_SYS_LAST_UPDT_DT_SK").alias("SRC_SYS_LAST_UPDT_DT_SK"),
        F.col("FEP_PLN_PROD_ID").alias("FEP_PLN_PROD_ID"),
        F.col("FEP_PLN_RGN_ID").alias("FEP_PLN_RGN_ID"),
        F.col("MBR_ID").alias("MBR_ID"),
        F.col("MBR_EMPLMT_STTUS_NM").alias("MBR_EMPLMT_STTUS_NM"),
        F.col("PROD_DESC").alias("PROD_DESC"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK").alias("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("BC_PLN_CD_SK").alias("BC_PLN_CD_SK"),
        F.col("BS_PLN_CD_SK").alias("BS_PLN_CD_SK"),
        F.col("FEP_MBR_ID").alias("FEP_MBR_ID"),
        F.col("FEP_COV_TYP_TX").alias("FEP_COV_TYP_TX")
    )
)

# Output link: lnk_UNK_row (single-row constants)
schema_xfm = StructType([
    StructField("BCBSA_FEP_MBR_ENR_SK", IntegerType(), True),
    StructField("MBR_UNIQ_KEY", IntegerType(), True),
    StructField("PROD_SH_NM", StringType(), True),
    StructField("FEP_MBR_ENR_EFF_DT_SK", StringType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("MBR_SK", IntegerType(), True),
    StructField("PROD_SH_NM_SK", IntegerType(), True),
    StructField("BC_PLN_CD", StringType(), True),
    StructField("BC_PLN_NM", StringType(), True),
    StructField("BS_PLN_CD", StringType(), True),
    StructField("BS_PLN_NM", StringType(), True),
    StructField("CHMCL_DPNDC_BNF_IN", StringType(), True),
    StructField("CHMCL_DPNDC_IP_BNF_IN", StringType(), True),
    StructField("CHMCL_DPNDC_IP_PRTL_DAY_BNF_IN", StringType(), True),
    StructField("CHMCL_DPNDC_OP_BNF_IN", StringType(), True),
    StructField("CONF_COMM_IN", StringType(), True),
    StructField("DNTL_BNF_IN", StringType(), True),
    StructField("MNTL_HLTH_BNF_IN", StringType(), True),
    StructField("MNTL_HLTH_IP_BNF_IN", StringType(), True),
    StructField("MNTL_HLTH_IP_PRTL_DAY_BNF_IN", StringType(), True),
    StructField("MNTL_HLTH_OP_BNF_IN", StringType(), True),
    StructField("OP_BNF_IN", StringType(), True),
    StructField("PDX_BNF_IN", StringType(), True),
    StructField("FEP_MBR_ENR_TERM_DT_SK", StringType(), True),
    StructField("SRC_SYS_LAST_UPDT_DT_SK", StringType(), True),
    StructField("FEP_PLN_PROD_ID", StringType(), True),
    StructField("FEP_PLN_RGN_ID", StringType(), True),
    StructField("MBR_ID", StringType(), True),
    StructField("MBR_EMPLMT_STTUS_NM", StringType(), True),
    StructField("PROD_DESC", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", StringType(), True),
    StructField("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("BC_PLN_CD_SK", IntegerType(), True),
    StructField("BS_PLN_CD_SK", IntegerType(), True),
    StructField("FEP_MBR_ID", StringType(), True),
    StructField("FEP_COV_TYP_TX", StringType(), True),
])

data_unk = [(
    0, 0, 'UNK', '1753-01-01', 'UNK', '1753-01-01', EDWRunDtCycle,
    0, 0, 'UNK', 'UNK', 'UNK', 'UNK',
    'N','N','N','N','N','N','N','N','N','N','N','N','N','2199-12-31','1753-01-01','UNK','UNK','UNK','UNK','UNK',
    100, EDWRunCycle, 100, 0, 0, 'UNK', 'UNK'
)]
df_lnk_UNK_row = spark.createDataFrame(data_unk, schema_xfm)

# Output link: lnk_NA_row (single-row constants)
data_na = [(
    1, 1, 'NA', '1753-01-01', 'NA', '1753-01-01', EDWRunDtCycle,
    1, 1, 'NA', 'NA', 'NA', 'NA',
    'N','N','N','N','N','N','N','N','N','N','N','N','N','2199-12-31','1753-01-01','NA','NA','NA','NA','NA',
    100, EDWRunCycle, 100, 1, 1, 'NA', 'NA'
)]
df_lnk_NA_row = spark.createDataFrame(data_na, schema_xfm)

# Stage: fn_combinedata_out (PxFunnel) -> union in order
df_fn_combinedata_out = df_lnk_UNK_row.unionByName(df_lnk_NA_row).unionByName(df_lnk_Extract_Out)

# Final select with correct column order
final_cols = [
    "BCBSA_FEP_MBR_ENR_SK",
    "MBR_UNIQ_KEY",
    "PROD_SH_NM",
    "FEP_MBR_ENR_EFF_DT_SK",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "MBR_SK",
    "PROD_SH_NM_SK",
    "BC_PLN_CD",
    "BC_PLN_NM",
    "BS_PLN_CD",
    "BS_PLN_NM",
    "CHMCL_DPNDC_BNF_IN",
    "CHMCL_DPNDC_IP_BNF_IN",
    "CHMCL_DPNDC_IP_PRTL_DAY_BNF_IN",
    "CHMCL_DPNDC_OP_BNF_IN",
    "CONF_COMM_IN",
    "DNTL_BNF_IN",
    "MNTL_HLTH_BNF_IN",
    "MNTL_HLTH_IP_BNF_IN",
    "MNTL_HLTH_IP_PRTL_DAY_BNF_IN",
    "MNTL_HLTH_OP_BNF_IN",
    "OP_BNF_IN",
    "PDX_BNF_IN",
    "FEP_MBR_ENR_TERM_DT_SK",
    "SRC_SYS_LAST_UPDT_DT_SK",
    "FEP_PLN_PROD_ID",
    "FEP_PLN_RGN_ID",
    "MBR_ID",
    "MBR_EMPLMT_STTUS_NM",
    "PROD_DESC",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "IDS_LAST_UPDT_RUN_CYC_EXCTN_SK",
    "BC_PLN_CD_SK",
    "BS_PLN_CD_SK",
    "FEP_MBR_ID",
    "FEP_COV_TYP_TX"
]

df_final = df_fn_combinedata_out.select(final_cols)

# rpad for char columns
df_final = df_final \
    .withColumn("FEP_MBR_ENR_EFF_DT_SK", F.rpad(F.col("FEP_MBR_ENR_EFF_DT_SK"), 10, " ")) \
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ")) \
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ")) \
    .withColumn("CHMCL_DPNDC_BNF_IN", F.rpad(F.col("CHMCL_DPNDC_BNF_IN"), 1, " ")) \
    .withColumn("CHMCL_DPNDC_IP_BNF_IN", F.rpad(F.col("CHMCL_DPNDC_IP_BNF_IN"), 1, " ")) \
    .withColumn("CHMCL_DPNDC_IP_PRTL_DAY_BNF_IN", F.rpad(F.col("CHMCL_DPNDC_IP_PRTL_DAY_BNF_IN"), 1, " ")) \
    .withColumn("CHMCL_DPNDC_OP_BNF_IN", F.rpad(F.col("CHMCL_DPNDC_OP_BNF_IN"), 1, " ")) \
    .withColumn("CONF_COMM_IN", F.rpad(F.col("CONF_COMM_IN"), 1, " ")) \
    .withColumn("DNTL_BNF_IN", F.rpad(F.col("DNTL_BNF_IN"), 1, " ")) \
    .withColumn("MNTL_HLTH_BNF_IN", F.rpad(F.col("MNTL_HLTH_BNF_IN"), 1, " ")) \
    .withColumn("MNTL_HLTH_IP_BNF_IN", F.rpad(F.col("MNTL_HLTH_IP_BNF_IN"), 1, " ")) \
    .withColumn("MNTL_HLTH_IP_PRTL_DAY_BNF_IN", F.rpad(F.col("MNTL_HLTH_IP_PRTL_DAY_BNF_IN"), 1, " ")) \
    .withColumn("MNTL_HLTH_OP_BNF_IN", F.rpad(F.col("MNTL_HLTH_OP_BNF_IN"), 1, " ")) \
    .withColumn("OP_BNF_IN", F.rpad(F.col("OP_BNF_IN"), 1, " ")) \
    .withColumn("PDX_BNF_IN", F.rpad(F.col("PDX_BNF_IN"), 1, " ")) \
    .withColumn("FEP_MBR_ENR_TERM_DT_SK", F.rpad(F.col("FEP_MBR_ENR_TERM_DT_SK"), 10, " ")) \
    .withColumn("SRC_SYS_LAST_UPDT_DT_SK", F.rpad(F.col("SRC_SYS_LAST_UPDT_DT_SK"), 10, " "))

# Stage: seq_BCBSA_FEP_MBR_ENR_D (PxSequentialFile) -> write the file
write_files(
    df_final,
    f"{adls_path}/load/BCBSA_FEP_MBR_ENR_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)