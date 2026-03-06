# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2015 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY: IdsEdwAcaPgmPaymtCntl
# MAGIC 
# MAGIC PROCESSING: Extracts data from IDS ACA_PGM_PAYMT to be loaded into EDW
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                             Date                  Project/Altiris #                                   Change Description                                                    Development Project     Code Reviewer            Date Reviewed
# MAGIC ----------------------------------        -------------------      -----------------------------------    ---------------------------------------------------------------------------------------------             ----------------------------------    ---------------------------------    -------------------------   
# MAGIC KalyanNeelam                    2015-10-22            5128                             Initial Programming                                                                          EnterpriseDev1              Bhoomi Dasari              10/23/2015

# MAGIC Read from source table ACA_PGM_PAYMT_F
# MAGIC This is a load ready file conforms to the target table structure.
# MAGIC 
# MAGIC Write ACA_PGM_PAYMT_F Data into a Sequential file for Load Job.
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Add Defaults and Null Handling
# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC 
# MAGIC 1) PAYMT_TYP_CD_SK
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when, rpad

# Retrieve parameter values
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
IdsRunCycle = get_widget_value('IdsRunCycle','')

# Get JDBC configuration
jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# Read from db2_ACA_PGM_PAYMT_F_in
extract_query_db2_ACA_PGM_PAYMT_F_in = f"""
SELECT
    APP.ACA_PGM_PAYMT_SK,
    APP.ACTVTY_YR_MO,
    APP.PAYMT_COV_YR_MO,
    APP.PAYMT_TYP_CD,
    APP.ST_CD,
    APP.QHP_ID,
    APP.ACA_PGM_PAYMT_SEQ_NO,
    COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
    APP.CRT_RUN_CYC_EXCTN_SK,
    APP.LAST_UPDT_RUN_CYC_EXCTN_SK,
    APP.QHP_SK,
    APP.PAYMT_TYP_CD_SK,
    APP.COV_STRT_DT_SK,
    APP.COV_END_DT_SK,
    APP.EFT_EFF_DT_SK,
    APP.ACA_PGM_TRANS_AMT,
    APP.ACA_PGM_PAYMT_UNIQ_KEY,
    APP.EFT_TRACE_ID,
    APP.EXCH_RPT_DOC_CTL_ID,
    APP.EXCH_RPT_NM
FROM {IDSOwner}.ACA_PGM_PAYMT APP
LEFT JOIN {IDSOwner}.CD_MPPNG CD
    ON APP.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
WHERE
    CD.TRGT_CD = 'VALENCIA'
    AND APP.LAST_UPDT_RUN_CYC_EXCTN_SK >= {IdsRunCycle}
"""

df_db2_ACA_PGM_PAYMT_F_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_ACA_PGM_PAYMT_F_in)
    .load()
)

# Read from db2_CD_MPPNG_Extr
extract_query_db2_CD_MPPNG_Extr = f"""
SELECT
    CD_MPPNG_SK,
    COALESCE(TRGT_CD,'UNK') AS TRGT_CD,
    COALESCE(TRGT_CD_NM,'UNK') AS TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG
"""

df_db2_CD_MPPNG_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CD_MPPNG_Extr)
    .load()
)

# lkp_Codes (PxLookup) - left join
df_lkp_Codes_pre = df_db2_ACA_PGM_PAYMT_F_in.alias("lnk_IdsEdwAcaPaymtPgmFExtr_InABC").join(
    df_db2_CD_MPPNG_Extr.alias("lnk_RefPaymtTypCdSkOut"),
    col("lnk_IdsEdwAcaPaymtPgmFExtr_InABC.PAYMT_TYP_CD_SK") == col("lnk_RefPaymtTypCdSkOut.CD_MPPNG_SK"),
    "left"
)

df_lkp_Codes = df_lkp_Codes_pre.select(
    col("lnk_IdsEdwAcaPaymtPgmFExtr_InABC.ACA_PGM_PAYMT_SK").alias("ACA_PGM_PAYMT_SK"),
    col("lnk_IdsEdwAcaPaymtPgmFExtr_InABC.ACTVTY_YR_MO").alias("ACTVTY_YR_MO"),
    col("lnk_IdsEdwAcaPaymtPgmFExtr_InABC.PAYMT_COV_YR_MO").alias("PAYMT_COV_YR_MO"),
    col("lnk_IdsEdwAcaPaymtPgmFExtr_InABC.PAYMT_TYP_CD").alias("PAYMT_TYP_CD"),
    col("lnk_IdsEdwAcaPaymtPgmFExtr_InABC.ST_CD").alias("ST_CD"),
    col("lnk_IdsEdwAcaPaymtPgmFExtr_InABC.QHP_ID").alias("QHP_ID"),
    col("lnk_IdsEdwAcaPaymtPgmFExtr_InABC.ACA_PGM_PAYMT_SEQ_NO").alias("ACA_PGM_PAYMT_SEQ_NO"),
    col("lnk_IdsEdwAcaPaymtPgmFExtr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("lnk_IdsEdwAcaPaymtPgmFExtr_InABC.QHP_SK").alias("QHP_SK"),
    col("lnk_RefPaymtTypCdSkOut.TRGT_CD_NM").alias("PAYMT_TYP_NM"),
    col("lnk_IdsEdwAcaPaymtPgmFExtr_InABC.COV_STRT_DT_SK").alias("COV_STRT_DT_SK"),
    col("lnk_IdsEdwAcaPaymtPgmFExtr_InABC.COV_END_DT_SK").alias("COV_END_DT_SK"),
    col("lnk_IdsEdwAcaPaymtPgmFExtr_InABC.EFT_EFF_DT_SK").alias("EFT_EFF_DT_SK"),
    col("lnk_IdsEdwAcaPaymtPgmFExtr_InABC.ACA_PGM_TRANS_AMT").alias("ACA_PGM_TRANS_AMT"),
    col("lnk_IdsEdwAcaPaymtPgmFExtr_InABC.ACA_PGM_PAYMT_UNIQ_KEY").alias("ACA_PGM_PAYMT_UNIQ_KEY"),
    col("lnk_IdsEdwAcaPaymtPgmFExtr_InABC.EFT_TRACE_ID").alias("EFT_TRACE_ID"),
    col("lnk_IdsEdwAcaPaymtPgmFExtr_InABC.EXCH_RPT_DOC_CTL_ID").alias("EXCH_RPT_DOC_CTL_ID"),
    col("lnk_IdsEdwAcaPaymtPgmFExtr_InABC.EXCH_RPT_NM").alias("EXCH_RPT_NM"),
    col("lnk_IdsEdwAcaPaymtPgmFExtr_InABC.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("lnk_IdsEdwAcaPaymtPgmFExtr_InABC.PAYMT_TYP_CD_SK").alias("PAYMT_TYP_CD_SK")
)

# xfm_BusinessLogic
# 1) AcaPaymtPgmFMainExtr
df_AcaPaymtPgmFMainExtr = df_lkp_Codes.filter(
    (col("ACA_PGM_PAYMT_SK") != 0) & (col("ACA_PGM_PAYMT_SK") != 1)
).select(
    col("ACA_PGM_PAYMT_SK").alias("ACA_PGM_PAYMT_SK"),
    col("ACTVTY_YR_MO").alias("ACTVTY_YR_MO"),
    col("PAYMT_COV_YR_MO").alias("PAYMT_COV_YR_MO"),
    col("PAYMT_TYP_CD").alias("PAYMT_TYP_CD"),
    col("ST_CD").alias("ST_CD"),
    col("QHP_ID").alias("QHP_ID"),
    col("ACA_PGM_PAYMT_SEQ_NO").alias("ACA_PGM_PAYMT_SEQ_NO"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("QHP_SK").alias("QHP_SK"),
    when(col("PAYMT_TYP_NM").isNull(), lit("UNK")).otherwise(col("PAYMT_TYP_NM")).alias("PAYMT_TYP_NM"),
    col("COV_STRT_DT_SK").alias("COV_STRT_DT_SK"),
    col("COV_END_DT_SK").alias("COV_END_DT_SK"),
    col("EFT_EFF_DT_SK").alias("EFT_EFF_DT_SK"),
    col("ACA_PGM_TRANS_AMT").alias("ACA_PGM_TRANS_AMT"),
    col("ACA_PGM_PAYMT_UNIQ_KEY").alias("ACA_PGM_PAYMT_UNIQ_KEY"),
    col("EFT_TRACE_ID").alias("EFT_TRACE_ID"),
    col("EXCH_RPT_DOC_CTL_ID").alias("EXCH_RPT_DOC_CTL_ID"),
    col("EXCH_RPT_NM").alias("EXCH_RPT_NM"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("PAYMT_TYP_CD_SK").alias("PAYMT_TYP_CD_SK")
)

# 2) UNK - single row
# Schema for the 23 columns
# (All strings here except numeric columns set as int; using minimal schema inference)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


schema_23 = StructType([
    StructField("ACA_PGM_PAYMT_SK", IntegerType(), True),
    StructField("ACTVTY_YR_MO", StringType(), True),
    StructField("PAYMT_COV_YR_MO", StringType(), True),
    StructField("PAYMT_TYP_CD", StringType(), True),
    StructField("ST_CD", StringType(), True),
    StructField("QHP_ID", StringType(), True),
    StructField("ACA_PGM_PAYMT_SEQ_NO", IntegerType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("QHP_SK", IntegerType(), True),
    StructField("PAYMT_TYP_NM", StringType(), True),
    StructField("COV_STRT_DT_SK", StringType(), True),
    StructField("COV_END_DT_SK", StringType(), True),
    StructField("EFT_EFF_DT_SK", StringType(), True),
    StructField("ACA_PGM_TRANS_AMT", IntegerType(), True),
    StructField("ACA_PGM_PAYMT_UNIQ_KEY", IntegerType(), True),
    StructField("EFT_TRACE_ID", StringType(), True),
    StructField("EXCH_RPT_DOC_CTL_ID", StringType(), True),
    StructField("EXCH_RPT_NM", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("PAYMT_TYP_CD_SK", IntegerType(), True)
])

df_UNK = spark.createDataFrame(
    [
        (
            0,
            "175301",
            "175301",
            "UNK",
            "UNK",
            "UNK",
            0,
            "UNK",
            "1753-01-01",
            "1753-01-01",
            0,
            "UNK",
            "1753-01-01",
            "2199-12-31",
            "1753-01-01",
            0,
            0,
            "UNK",
            "UNK",
            "UNK",
            0,
            0,
            0
        )
    ],
    schema_23
)

# 3) NA - single row
df_NA = spark.createDataFrame(
    [
        (
            1,
            "175301",
            "175301",
            "NA",
            "NA",
            "NA",
            0,
            "NA",
            "1753-01-01",
            "1753-01-01",
            1,
            "NA",
            "1753-01-01",
            "2199-12-31",
            "1753-01-01",
            0,
            1,
            "NA",
            "NA",
            "NA",
            1,
            1,
            1
        )
    ],
    schema_23
)

# Funnel (fnl_UNK_NA)
df_fnl_UNK_NA = df_AcaPaymtPgmFMainExtr.unionByName(df_UNK).unionByName(df_NA)

# Prepare final select with padding for char columns
df_output = df_fnl_UNK_NA.select(
    col("ACA_PGM_PAYMT_SK").alias("ACA_PGM_PAYMT_SK"),
    rpad(col("ACTVTY_YR_MO"), 6, " ").alias("ACTVTY_YR_MO"),
    rpad(col("PAYMT_COV_YR_MO"), 6, " ").alias("PAYMT_COV_YR_MO"),
    col("PAYMT_TYP_CD").alias("PAYMT_TYP_CD"),
    col("ST_CD").alias("ST_CD"),
    col("QHP_ID").alias("QHP_ID"),
    col("ACA_PGM_PAYMT_SEQ_NO").alias("ACA_PGM_PAYMT_SEQ_NO"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("QHP_SK").alias("QHP_SK"),
    col("PAYMT_TYP_NM").alias("PAYMT_TYP_NM"),
    rpad(col("COV_STRT_DT_SK"), 10, " ").alias("COV_STRT_DT_SK"),
    rpad(col("COV_END_DT_SK"), 10, " ").alias("COV_END_DT_SK"),
    rpad(col("EFT_EFF_DT_SK"), 10, " ").alias("EFT_EFF_DT_SK"),
    col("ACA_PGM_TRANS_AMT").alias("ACA_PGM_TRANS_AMT"),
    col("ACA_PGM_PAYMT_UNIQ_KEY").alias("ACA_PGM_PAYMT_UNIQ_KEY"),
    col("EFT_TRACE_ID").alias("EFT_TRACE_ID"),
    col("EXCH_RPT_DOC_CTL_ID").alias("EXCH_RPT_DOC_CTL_ID"),
    col("EXCH_RPT_NM").alias("EXCH_RPT_NM"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("PAYMT_TYP_CD_SK").alias("PAYMT_TYP_CD_SK")
)

# seq_ACA_PGM_PAYMT_F_csv_load - write to .dat
write_files(
    df_output,
    f"{adls_path}/load/ACA_PGM_PAYMT_F.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)