# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:
# MAGIC 
# MAGIC PROCESSING:
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                             Date                  Project/Altiris #                                   Change Description                                                    Development Project     Code Reviewer            Date Reviewed
# MAGIC ----------------------------------        -------------------      -----------------------------------    ---------------------------------------------------------------------------------------------             ----------------------------------    ---------------------------------    -------------------------   
# MAGIC Naren                                9/20/2007                    3259                   Originally Programmed                                                                        devlEDW10                   Steph Goddard             10/07/2007
# MAGIC 
# MAGIC 
# MAGIC Raj Mangalampally            08/20/2013                5114                     Original Programming                                                                        EnterpriseWrhsDevl        Peter Marshall               12/10/2013
# MAGIC                                                                                                            (Server to Parallel Conversion)

# MAGIC This is Source extract data from an IDS table and apply Code denormalization, create a load ready file.
# MAGIC 
# MAGIC Job Name:
# MAGIC IdsEdwAltFundInvcFExtr
# MAGIC 
# MAGIC Table:
# MAGIC ALT_FUND_INVC_F
# MAGIC Read from source table 
# MAGIC Pull records based on LAST_UPDT_RUN_CYC_EXTN_SK
# MAGIC This is a load ready file conforms to the target table structure.
# MAGIC 
# MAGIC Write ALT_FUND_INVC_F Data into a Sequential file for Load Job.
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Add Defaults and Null Handling
# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC 
# MAGIC 1) ALT_FUND_INVC_STYLE_CD_SK
# MAGIC 2) ALT_FUND_INVC_PAYMT_CD_SK
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query_db2_ALT_FUND_INV_F_in = """SELECT 
ALT_FUND_INVC_SK,
COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
ALT_FUND_INVC_ID,
INVC.ALT_FUND_SK,
BILL_ENTY_SK,
ALT_FUND_INVC_STYLE_CD_SK,
ALT_FUND_INVC_PAYMT_CD_SK,
BILL_DUE_DT_SK,
BILL_END_DT_SK,
CRT_DT_SK,
FUND_FROM_DT_SK,
FUND_THRU_DT_SK,
BILL_AMT,
NET_DUE_AMT,
OUTSTND_BAL_AMT,
BCBS_BILL_DT_SK,
BCBS_DUE_DT_SK,
ALT_FUND_ID,
ALT_FUND_NM 
FROM #$IDSOwner#.ALT_FUND_INVC INVC
LEFT JOIN #$IDSOwner#.CD_MPPNG CD
ON INVC.SRC_SYS_CD_SK = CD.CD_MPPNG_SK,
#$IDSOwner#.ALT_FUND ALT_FUND 
WHERE 
INVC.ALT_FUND_SK=ALT_FUND.ALT_FUND_SK
"""
df_db2_ALT_FUND_INV_F_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_ALT_FUND_INV_F_in)
    .load()
)

extract_query_db2_CD_MPPNG_Extr = """SELECT CD_MPPNG_SK,COALESCE(TRGT_CD,'UNK') TRGT_CD,COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM from #$IDSOwner#.CD_MPPNG"""
df_db2_CD_MPPNG_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CD_MPPNG_Extr)
    .load()
)

df_lnk_Cd_mppng_out = df_db2_CD_MPPNG_Extr

df_Ref_InvcPaymntCd = df_lnk_Cd_mppng_out.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Ref_InvcStylCdLkup = df_lnk_Cd_mppng_out.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_lkp_temp1 = df_db2_ALT_FUND_INV_F_in.alias("lnk_IdsEdwAltFundInvFExtr_InABC").join(
    df_Ref_InvcStylCdLkup.alias("Ref_InvcStylCdLkup"),
    F.col("lnk_IdsEdwAltFundInvFExtr_InABC.ALT_FUND_INVC_STYLE_CD_SK") == F.col("Ref_InvcStylCdLkup.CD_MPPNG_SK"),
    how="left"
)

df_lkp_Codes = df_lkp_temp1.join(
    df_Ref_InvcPaymntCd.alias("Ref_InvcPaymntCd"),
    F.col("lnk_IdsEdwAltFundInvFExtr_InABC.ALT_FUND_INVC_PAYMT_CD_SK") == F.col("Ref_InvcPaymntCd.CD_MPPNG_SK"),
    how="left"
).select(
    F.col("lnk_IdsEdwAltFundInvFExtr_InABC.ALT_FUND_INVC_SK").alias("ALT_FUND_INVC_SK"),
    F.col("lnk_IdsEdwAltFundInvFExtr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_IdsEdwAltFundInvFExtr_InABC.ALT_FUND_INVC_ID").alias("ALT_FUND_INVC_ID"),
    F.col("lnk_IdsEdwAltFundInvFExtr_InABC.ALT_FUND_SK").alias("ALT_FUND_SK"),
    F.col("lnk_IdsEdwAltFundInvFExtr_InABC.BILL_ENTY_SK").alias("BILL_ENTY_SK"),
    F.col("Ref_InvcStylCdLkup.TRGT_CD").alias("ALT_FUND_INVC_STYLE_CD"),
    F.col("Ref_InvcStylCdLkup.TRGT_CD_NM").alias("ALT_FUND_INVC_STYLE_NM"),
    F.col("Ref_InvcPaymntCd.TRGT_CD").alias("ALT_FUND_INVC_PAYMT_CD"),
    F.col("Ref_InvcPaymntCd.TRGT_CD_NM").alias("ALT_FUND_INVC_PAYMT_NM"),
    F.col("lnk_IdsEdwAltFundInvFExtr_InABC.BCBS_BILL_DT_SK").alias("BCBS_BILL_DT_SK"),
    F.col("lnk_IdsEdwAltFundInvFExtr_InABC.BCBS_DUE_DT_SK").alias("ALT_FUND_INVC_BCBS_DUE_DT_SK"),
    F.col("lnk_IdsEdwAltFundInvFExtr_InABC.BILL_DUE_DT_SK").alias("ALT_FUND_INVC_BILL_DUE_DT_SK"),
    F.col("lnk_IdsEdwAltFundInvFExtr_InABC.BILL_END_DT_SK").alias("ALT_FUND_INVC_BILL_END_DT_SK"),
    F.col("lnk_IdsEdwAltFundInvFExtr_InABC.CRT_DT_SK").alias("ALT_FUND_INVC_CRT_DT_SK"),
    F.col("lnk_IdsEdwAltFundInvFExtr_InABC.FUND_FROM_DT_SK").alias("ALT_FUND_INVC_FUND_FROM_DT_SK"),
    F.col("lnk_IdsEdwAltFundInvFExtr_InABC.FUND_THRU_DT_SK").alias("ALT_FUND_INVC_FUND_THRU_DT_SK"),
    F.col("lnk_IdsEdwAltFundInvFExtr_InABC.BILL_AMT").alias("ALT_FUND_INVC_TOT_BILL_AMT"),
    F.col("lnk_IdsEdwAltFundInvFExtr_InABC.NET_DUE_AMT").alias("ALT_FUND_INVC_NET_DUE_AMT"),
    F.col("lnk_IdsEdwAltFundInvFExtr_InABC.OUTSTND_BAL_AMT").alias("ALT_FUND_INVC_OUTSTND_BAL_AMT"),
    F.col("lnk_IdsEdwAltFundInvFExtr_InABC.ALT_FUND_ID").alias("ALT_FUND_ID"),
    F.col("lnk_IdsEdwAltFundInvFExtr_InABC.ALT_FUND_NM").alias("ALT_FUND_NM"),
    F.col("lnk_IdsEdwAltFundInvFExtr_InABC.ALT_FUND_INVC_STYLE_CD_SK").alias("ALT_FUND_INVC_STYLE_CD_SK"),
    F.col("lnk_IdsEdwAltFundInvFExtr_InABC.ALT_FUND_INVC_PAYMT_CD_SK").alias("ALT_FUND_INVC_PAYMT_CD_SK")
)

df_AltFundInvcFMainExtr = df_lkp_Codes.filter(
    (F.col("ALT_FUND_INVC_SK") != 0) & (F.col("ALT_FUND_INVC_SK") != 1)
).select(
    F.col("ALT_FUND_INVC_SK").alias("ALT_FUND_INVC_SK"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("ALT_FUND_INVC_ID").alias("ALT_FUND_INVC_ID"),
    F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("ALT_FUND_SK").alias("ALT_FUND_SK"),
    F.col("BILL_ENTY_SK").alias("BILL_ENTY_SK"),
    F.col("ALT_FUND_INVC_STYLE_CD").alias("ALT_FUND_INVC_STYLE_CD"),
    F.col("ALT_FUND_INVC_STYLE_NM").alias("ALT_FUND_INVC_STYLE_NM"),
    F.col("ALT_FUND_INVC_PAYMT_CD").alias("ALT_FUND_INVC_PAYMT_CD"),
    F.col("ALT_FUND_INVC_PAYMT_NM").alias("ALT_FUND_INVC_PAYMT_NM"),
    F.col("BCBS_BILL_DT_SK").alias("ALT_FUND_INVC_BCBS_BILL_DT_SK"),
    F.concat(
        F.substring(F.col("BCBS_BILL_DT_SK"), 1, 4),
        F.substring(F.col("BCBS_BILL_DT_SK"), 6, 2)
    ).alias("AF_INVC_BCBS_BILL_YR_MO_SK"),
    F.col("ALT_FUND_INVC_BCBS_DUE_DT_SK").alias("ALT_FUND_INVC_BCBS_DUE_DT_SK"),
    F.col("ALT_FUND_INVC_BILL_DUE_DT_SK").alias("ALT_FUND_INVC_BILL_DUE_DT_SK"),
    F.col("ALT_FUND_INVC_BILL_END_DT_SK").alias("ALT_FUND_INVC_BILL_END_DT_SK"),
    F.col("ALT_FUND_INVC_CRT_DT_SK").alias("ALT_FUND_INVC_CRT_DT_SK"),
    F.col("ALT_FUND_INVC_FUND_FROM_DT_SK").alias("ALT_FUND_INVC_FUND_FROM_DT_SK"),
    F.col("ALT_FUND_INVC_FUND_THRU_DT_SK").alias("ALT_FUND_INVC_FUND_THRU_DT_SK"),
    F.col("ALT_FUND_INVC_TOT_BILL_AMT").alias("ALT_FUND_INVC_TOT_BILL_AMT"),
    F.col("ALT_FUND_INVC_NET_DUE_AMT").alias("ALT_FUND_INVC_NET_DUE_AMT"),
    F.col("ALT_FUND_INVC_OUTSTND_BAL_AMT").alias("ALT_FUND_INVC_OUTSTND_BAL_AMT"),
    F.col("ALT_FUND_ID").alias("ALT_FUND_ID"),
    F.col("ALT_FUND_NM").alias("ALT_FUND_NM"),
    F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("ALT_FUND_INVC_STYLE_CD_SK").alias("ALT_FUND_INVC_STYLE_CD_SK"),
    F.col("ALT_FUND_INVC_PAYMT_CD_SK").alias("ALT_FUND_INVC_PAYMT_CD_SK")
)

schema_UNK = [
    StructField("ALT_FUND_INVC_SK", IntegerType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("ALT_FUND_INVC_ID", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("ALT_FUND_SK", IntegerType(), True),
    StructField("BILL_ENTY_SK", IntegerType(), True),
    StructField("ALT_FUND_INVC_STYLE_CD", StringType(), True),
    StructField("ALT_FUND_INVC_STYLE_NM", StringType(), True),
    StructField("ALT_FUND_INVC_PAYMT_CD", StringType(), True),
    StructField("ALT_FUND_INVC_PAYMT_NM", StringType(), True),
    StructField("ALT_FUND_INVC_BCBS_BILL_DT_SK", StringType(), True),
    StructField("AF_INVC_BCBS_BILL_YR_MO_SK", StringType(), True),
    StructField("ALT_FUND_INVC_BCBS_DUE_DT_SK", StringType(), True),
    StructField("ALT_FUND_INVC_BILL_DUE_DT_SK", StringType(), True),
    StructField("ALT_FUND_INVC_BILL_END_DT_SK", StringType(), True),
    StructField("ALT_FUND_INVC_CRT_DT_SK", StringType(), True),
    StructField("ALT_FUND_INVC_FUND_FROM_DT_SK", StringType(), True),
    StructField("ALT_FUND_INVC_FUND_THRU_DT_SK", StringType(), True),
    StructField("ALT_FUND_INVC_TOT_BILL_AMT", IntegerType(), True),
    StructField("ALT_FUND_INVC_NET_DUE_AMT", IntegerType(), True),
    StructField("ALT_FUND_INVC_OUTSTND_BAL_AMT", IntegerType(), True),
    StructField("ALT_FUND_ID", StringType(), True),
    StructField("ALT_FUND_NM", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("ALT_FUND_INVC_STYLE_CD_SK", IntegerType(), True),
    StructField("ALT_FUND_INVC_PAYMT_CD_SK", IntegerType(), True)
]
df_UNK = spark.createDataFrame(
    [
        (
            0, 'UNK', 'UNK', '1753-01-01', '1753-01-01', 0, 0, 'UNK', 'UNK', 'UNK', 'UNK',
            '1753-01-01', '175301', '1753-01-01', '1753-01-01', '1753-01-01',
            '1753-01-01', '1753-01-01', '1753-01-01', 0, 0, 0, 'UNK', 'UNK',
            100, 100, 0, 0
        )
    ],
    schema=StructType(schema_UNK)
)

schema_NA = [
    StructField("ALT_FUND_INVC_SK", IntegerType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("ALT_FUND_INVC_ID", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("ALT_FUND_SK", IntegerType(), True),
    StructField("BILL_ENTY_SK", IntegerType(), True),
    StructField("ALT_FUND_INVC_STYLE_CD", StringType(), True),
    StructField("ALT_FUND_INVC_STYLE_NM", StringType(), True),
    StructField("ALT_FUND_INVC_PAYMT_CD", StringType(), True),
    StructField("ALT_FUND_INVC_PAYMT_NM", StringType(), True),
    StructField("ALT_FUND_INVC_BCBS_BILL_DT_SK", StringType(), True),
    StructField("AF_INVC_BCBS_BILL_YR_MO_SK", StringType(), True),
    StructField("ALT_FUND_INVC_BCBS_DUE_DT_SK", StringType(), True),
    StructField("ALT_FUND_INVC_BILL_DUE_DT_SK", StringType(), True),
    StructField("ALT_FUND_INVC_BILL_END_DT_SK", StringType(), True),
    StructField("ALT_FUND_INVC_CRT_DT_SK", StringType(), True),
    StructField("ALT_FUND_INVC_FUND_FROM_DT_SK", StringType(), True),
    StructField("ALT_FUND_INVC_FUND_THRU_DT_SK", StringType(), True),
    StructField("ALT_FUND_INVC_TOT_BILL_AMT", IntegerType(), True),
    StructField("ALT_FUND_INVC_NET_DUE_AMT", IntegerType(), True),
    StructField("ALT_FUND_INVC_OUTSTND_BAL_AMT", IntegerType(), True),
    StructField("ALT_FUND_ID", StringType(), True),
    StructField("ALT_FUND_NM", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("ALT_FUND_INVC_STYLE_CD_SK", IntegerType(), True),
    StructField("ALT_FUND_INVC_PAYMT_CD_SK", IntegerType(), True)
]
df_NA = spark.createDataFrame(
    [
        (
            1, 'NA', 'NA', '1753-01-01', '1753-01-01', 1, 1, 'NA', 'NA', 'NA', 'NA',
            '1753-01-01', '175301', '1753-01-01', '1753-01-01', '1753-01-01',
            '1753-01-01', '1753-01-01', '1753-01-01', 0, 0, 0, 'NA', 'NA',
            100, 100, 1, 1
        )
    ],
    schema=StructType(schema_NA)
)

df_fnl_UNK_NA = df_AltFundInvcFMainExtr.unionByName(df_UNK).unionByName(df_NA)

df_final = df_fnl_UNK_NA.select(
    F.col("ALT_FUND_INVC_SK"),
    F.col("SRC_SYS_CD"),
    F.col("ALT_FUND_INVC_ID"),
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("ALT_FUND_SK"),
    F.col("BILL_ENTY_SK"),
    F.col("ALT_FUND_INVC_STYLE_CD"),
    F.col("ALT_FUND_INVC_STYLE_NM"),
    F.col("ALT_FUND_INVC_PAYMT_CD"),
    F.col("ALT_FUND_INVC_PAYMT_NM"),
    F.rpad(F.col("ALT_FUND_INVC_BCBS_BILL_DT_SK"), 10, " ").alias("ALT_FUND_INVC_BCBS_BILL_DT_SK"),
    F.rpad(F.col("AF_INVC_BCBS_BILL_YR_MO_SK"), 6, " ").alias("AF_INVC_BCBS_BILL_YR_MO_SK"),
    F.rpad(F.col("ALT_FUND_INVC_BCBS_DUE_DT_SK"), 10, " ").alias("ALT_FUND_INVC_BCBS_DUE_DT_SK"),
    F.rpad(F.col("ALT_FUND_INVC_BILL_DUE_DT_SK"), 10, " ").alias("ALT_FUND_INVC_BILL_DUE_DT_SK"),
    F.rpad(F.col("ALT_FUND_INVC_BILL_END_DT_SK"), 10, " ").alias("ALT_FUND_INVC_BILL_END_DT_SK"),
    F.rpad(F.col("ALT_FUND_INVC_CRT_DT_SK"), 10, " ").alias("ALT_FUND_INVC_CRT_DT_SK"),
    F.rpad(F.col("ALT_FUND_INVC_FUND_FROM_DT_SK"), 10, " ").alias("ALT_FUND_INVC_FUND_FROM_DT_SK"),
    F.rpad(F.col("ALT_FUND_INVC_FUND_THRU_DT_SK"), 10, " ").alias("ALT_FUND_INVC_FUND_THRU_DT_SK"),
    F.col("ALT_FUND_INVC_TOT_BILL_AMT"),
    F.col("ALT_FUND_INVC_NET_DUE_AMT"),
    F.col("ALT_FUND_INVC_OUTSTND_BAL_AMT"),
    F.col("ALT_FUND_ID"),
    F.col("ALT_FUND_NM"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("ALT_FUND_INVC_STYLE_CD_SK"),
    F.col("ALT_FUND_INVC_PAYMT_CD_SK")
)

write_files(
    df_final,
    f"{adls_path}/load/ALT_FUND_INVC_F.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)