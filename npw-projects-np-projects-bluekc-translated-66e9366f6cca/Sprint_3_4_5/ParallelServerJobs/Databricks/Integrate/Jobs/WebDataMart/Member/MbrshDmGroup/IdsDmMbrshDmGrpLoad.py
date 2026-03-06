# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        ---------------------------------------------------------------------------                             ------------------------------    ------------------------------       --------------------
# MAGIC Bhupinder Kaur         2013 -07-29      5114                             Load  DM MBRSH_DM_GRP                                                               IntegrateWrhsDevl       Jag Yelavarthi           2013-10-23

# MAGIC Job Name: IdsDmMbrshDmGrpLoad
# MAGIC Read Load File created in the IdsDmMbrshDmGrpExtr Job
# MAGIC Copy Stage for buffer
# MAGIC Replace MBRSH_DM_GRP Data.
# MAGIC Load rejects are redirected into a Reject file
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Parameter Retrieval
ClmMartOwner = get_widget_value('ClmMartOwner','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')
ClmMartArraySize = get_widget_value('ClmMartArraySize','')
ClmMartRecordCount = get_widget_value('ClmMartRecordCount','')

# Schema Definition for seq_MBRSH_DM_GRP_csv_load
schema_seq_MBRSH_DM_GRP_csv_load = StructType([
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("GRP_ID", StringType(), True),
    StructField("GRP_ORIG_EFF_DT", TimestampType(), True),
    StructField("GRP_TERM_DT", TimestampType(), True),
    StructField("GRP_UNIQ_KEY", IntegerType(), True),
    StructField("GRP_NM", StringType(), True),
    StructField("GRP_ADDR_LN_1", StringType(), True),
    StructField("GRP_ADDR_LN_2", StringType(), True),
    StructField("GRP_ADDR_LN_3", StringType(), True),
    StructField("GRP_CITY_NM", StringType(), True),
    StructField("GRP_ST_CD", StringType(), True),
    StructField("GRP_POSTAL_CD", StringType(), True),
    StructField("GRP_CNTY_NM", StringType(), True),
    StructField("GRP_PHN_NO", StringType(), True),
    StructField("GRP_PHN_EXT_NO", StringType(), True),
    StructField("GRP_FAX_NO", StringType(), True),
    StructField("GRP_FAX_EXT_NO", StringType(), True),
    StructField("GRP_EMAIL_ADDR", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_NO", IntegerType(), True),
    StructField("GRP_EDI_ACCT_VNDR_CD", StringType(), True),
    StructField("GRP_EDI_ACCT_VNDR_NM", StringType(), True),
    StructField("GRP_MKT_SIZE_CAT_CD", StringType(), True),
    StructField("GRP_MKT_SIZE_CAT_NM", StringType(), True),
    StructField("GRP_WEB_RSTRCT_IN", StringType(), True),
    StructField("GRP_CUR_ANNV_DT", TimestampType(), True),
    StructField("GRP_NEXT_ANNV_DT", TimestampType(), True),
    StructField("GRP_REINST_DT", TimestampType(), True),
    StructField("GRP_RNWL_DT", TimestampType(), True),
    StructField("GRP_STTUS_CD", StringType(), True),
    StructField("GRP_STTUS_NM", StringType(), True),
    StructField("GRP_TERM_RSN_CD", StringType(), True),
    StructField("GRP_TERM_RSN_NM", StringType(), True),
    StructField("GRP_MKTNG_TERR_ID", StringType(), True),
    StructField("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True)
])

# Read seq_MBRSH_DM_GRP_csv_load
df_seq_MBRSH_DM_GRP_csv_load = (
    spark.read
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "^")
    .option("inferSchema", "false")
    .schema(schema_seq_MBRSH_DM_GRP_csv_load)
    .csv(f"{adls_path}/load/MBRSH_DM_GRP.dat")
)

df_seq_MBRSH_DM_GRP_csv_load = df_seq_MBRSH_DM_GRP_csv_load.select(
    "SRC_SYS_CD",
    "GRP_ID",
    "GRP_ORIG_EFF_DT",
    "GRP_TERM_DT",
    "GRP_UNIQ_KEY",
    "GRP_NM",
    "GRP_ADDR_LN_1",
    "GRP_ADDR_LN_2",
    "GRP_ADDR_LN_3",
    "GRP_CITY_NM",
    "GRP_ST_CD",
    "GRP_POSTAL_CD",
    "GRP_CNTY_NM",
    "GRP_PHN_NO",
    "GRP_PHN_EXT_NO",
    "GRP_FAX_NO",
    "GRP_FAX_EXT_NO",
    "GRP_EMAIL_ADDR",
    "LAST_UPDT_RUN_CYC_NO",
    "GRP_EDI_ACCT_VNDR_CD",
    "GRP_EDI_ACCT_VNDR_NM",
    "GRP_MKT_SIZE_CAT_CD",
    "GRP_MKT_SIZE_CAT_NM",
    "GRP_WEB_RSTRCT_IN",
    "GRP_CUR_ANNV_DT",
    "GRP_NEXT_ANNV_DT",
    "GRP_REINST_DT",
    "GRP_RNWL_DT",
    "GRP_STTUS_CD",
    "GRP_STTUS_NM",
    "GRP_TERM_RSN_CD",
    "GRP_TERM_RSN_NM",
    "GRP_MKTNG_TERR_ID",
    "IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"
)

# cpy_forBuffer
df_cpy_forBuffer = df_seq_MBRSH_DM_GRP_csv_load.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("GRP_ORIG_EFF_DT").alias("GRP_ORIG_EFF_DT"),
    F.col("GRP_TERM_DT").alias("GRP_TERM_DT"),
    F.col("GRP_UNIQ_KEY").alias("GRP_UNIQ_KEY"),
    F.col("GRP_NM").alias("GRP_NM"),
    F.col("GRP_ADDR_LN_1").alias("GRP_ADDR_LN_1"),
    F.col("GRP_ADDR_LN_2").alias("GRP_ADDR_LN_2"),
    F.col("GRP_ADDR_LN_3").alias("GRP_ADDR_LN_3"),
    F.col("GRP_CITY_NM").alias("GRP_CITY_NM"),
    F.col("GRP_ST_CD").alias("GRP_ST_CD"),
    F.col("GRP_POSTAL_CD").alias("GRP_POSTAL_CD"),
    F.col("GRP_CNTY_NM").alias("GRP_CNTY_NM"),
    F.col("GRP_PHN_NO").alias("GRP_PHN_NO"),
    F.col("GRP_PHN_EXT_NO").alias("GRP_PHN_EXT_NO"),
    F.col("GRP_FAX_NO").alias("GRP_FAX_NO"),
    F.col("GRP_FAX_EXT_NO").alias("GRP_FAX_EXT_NO"),
    F.col("GRP_EMAIL_ADDR").alias("GRP_EMAIL_ADDR"),
    F.col("LAST_UPDT_RUN_CYC_NO").alias("LAST_UPDT_RUN_CYC_NO"),
    F.col("GRP_EDI_ACCT_VNDR_CD").alias("GRP_EDI_ACCT_VNDR_CD"),
    F.col("GRP_EDI_ACCT_VNDR_NM").alias("GRP_EDI_ACCT_VNDR_NM"),
    F.col("GRP_MKT_SIZE_CAT_CD").alias("GRP_MKT_SIZE_CAT_CD"),
    F.col("GRP_MKT_SIZE_CAT_NM").alias("GRP_MKT_SIZE_CAT_NM"),
    F.col("GRP_WEB_RSTRCT_IN").alias("GRP_WEB_RSTRCT_IN"),
    F.col("GRP_CUR_ANNV_DT").alias("GRP_CUR_ANNV_DT"),
    F.col("GRP_NEXT_ANNV_DT").alias("GRP_NEXT_ANNV_DT"),
    F.col("GRP_REINST_DT").alias("GRP_REINST_DT"),
    F.col("GRP_RNWL_DT").alias("GRP_RNWL_DT"),
    F.col("GRP_STTUS_CD").alias("GRP_STTUS_CD"),
    F.col("GRP_STTUS_NM").alias("GRP_STTUS_NM"),
    F.col("GRP_TERM_RSN_CD").alias("GRP_TERM_RSN_CD"),
    F.col("GRP_TERM_RSN_NM").alias("GRP_TERM_RSN_NM"),
    F.col("GRP_MKTNG_TERR_ID").alias("GRP_MKTNG_TERR_ID"),
    F.col("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK").alias("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK")
)

# ODBC_MBRSH_DM_GRP_out (Writing to ClmMartOwner.MBRSH_DM_GRP via Merge)
df_ODBC_MBRSH_DM_GRP_out = df_cpy_forBuffer.select(
    F.rpad(F.col("SRC_SYS_CD"), 255, " ").alias("SRC_SYS_CD"),
    F.rpad(F.col("GRP_ID"), 255, " ").alias("GRP_ID"),
    F.col("GRP_ORIG_EFF_DT").alias("GRP_ORIG_EFF_DT"),
    F.col("GRP_TERM_DT").alias("GRP_TERM_DT"),
    F.col("GRP_UNIQ_KEY").alias("GRP_UNIQ_KEY"),
    F.rpad(F.col("GRP_NM"), 255, " ").alias("GRP_NM"),
    F.rpad(F.col("GRP_ADDR_LN_1"), 255, " ").alias("GRP_ADDR_LN_1"),
    F.rpad(F.col("GRP_ADDR_LN_2"), 255, " ").alias("GRP_ADDR_LN_2"),
    F.rpad(F.col("GRP_ADDR_LN_3"), 255, " ").alias("GRP_ADDR_LN_3"),
    F.rpad(F.col("GRP_CITY_NM"), 255, " ").alias("GRP_CITY_NM"),
    F.rpad(F.col("GRP_ST_CD"), 255, " ").alias("GRP_ST_CD"),
    F.rpad(F.col("GRP_POSTAL_CD"), 255, " ").alias("GRP_POSTAL_CD"),
    F.rpad(F.col("GRP_CNTY_NM"), 255, " ").alias("GRP_CNTY_NM"),
    F.rpad(F.col("GRP_PHN_NO"), 255, " ").alias("GRP_PHN_NO"),
    F.rpad(F.col("GRP_PHN_EXT_NO"), 255, " ").alias("GRP_PHN_EXT_NO"),
    F.rpad(F.col("GRP_FAX_NO"), 255, " ").alias("GRP_FAX_NO"),
    F.rpad(F.col("GRP_FAX_EXT_NO"), 255, " ").alias("GRP_FAX_EXT_NO"),
    F.rpad(F.col("GRP_EMAIL_ADDR"), 255, " ").alias("GRP_EMAIL_ADDR"),
    F.col("LAST_UPDT_RUN_CYC_NO").alias("LAST_UPDT_RUN_CYC_NO"),
    F.rpad(F.col("GRP_EDI_ACCT_VNDR_CD"), 255, " ").alias("GRP_EDI_ACCT_VNDR_CD"),
    F.rpad(F.col("GRP_EDI_ACCT_VNDR_NM"), 255, " ").alias("GRP_EDI_ACCT_VNDR_NM"),
    F.rpad(F.col("GRP_MKT_SIZE_CAT_CD"), 255, " ").alias("GRP_MKT_SIZE_CAT_CD"),
    F.rpad(F.col("GRP_MKT_SIZE_CAT_NM"), 255, " ").alias("GRP_MKT_SIZE_CAT_NM"),
    F.rpad(F.col("GRP_WEB_RSTRCT_IN"), 1, " ").alias("GRP_WEB_RSTRCT_IN"),
    F.col("GRP_CUR_ANNV_DT").alias("GRP_CUR_ANNV_DT"),
    F.col("GRP_NEXT_ANNV_DT").alias("GRP_NEXT_ANNV_DT"),
    F.col("GRP_REINST_DT").alias("GRP_REINST_DT"),
    F.col("GRP_RNWL_DT").alias("GRP_RNWL_DT"),
    F.rpad(F.col("GRP_STTUS_CD"), 255, " ").alias("GRP_STTUS_CD"),
    F.rpad(F.col("GRP_STTUS_NM"), 255, " ").alias("GRP_STTUS_NM"),
    F.rpad(F.col("GRP_TERM_RSN_CD"), 255, " ").alias("GRP_TERM_RSN_CD"),
    F.rpad(F.col("GRP_TERM_RSN_NM"), 255, " ").alias("GRP_TERM_RSN_NM"),
    F.rpad(F.col("GRP_MKTNG_TERR_ID"), 255, " ").alias("GRP_MKTNG_TERR_ID"),
    F.col("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK").alias("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK")
)

jdbc_url, jdbc_props = get_db_config(clmmart_secret_name)

execute_dml(f"DROP TABLE IF EXISTS STAGING.IdsDmMbrshDmGrpLoad_ODBC_MBRSH_DM_GRP_out_temp", jdbc_url, jdbc_props)

df_ODBC_MBRSH_DM_GRP_out.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.IdsDmMbrshDmGrpLoad_ODBC_MBRSH_DM_GRP_out_temp") \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO {ClmMartOwner}.MBRSH_DM_GRP AS T
USING STAGING.IdsDmMbrshDmGrpLoad_ODBC_MBRSH_DM_GRP_out_temp AS S
ON
(
T.SRC_SYS_CD = S.SRC_SYS_CD
AND T.GRP_ID = S.GRP_ID
)
WHEN MATCHED THEN
UPDATE SET
T.GRP_ORIG_EFF_DT = S.GRP_ORIG_EFF_DT,
T.GRP_TERM_DT = S.GRP_TERM_DT,
T.GRP_UNIQ_KEY = S.GRP_UNIQ_KEY,
T.GRP_NM = S.GRP_NM,
T.GRP_ADDR_LN_1 = S.GRP_ADDR_LN_1,
T.GRP_ADDR_LN_2 = S.GRP_ADDR_LN_2,
T.GRP_ADDR_LN_3 = S.GRP_ADDR_LN_3,
T.GRP_CITY_NM = S.GRP_CITY_NM,
T.GRP_ST_CD = S.GRP_ST_CD,
T.GRP_POSTAL_CD = S.GRP_POSTAL_CD,
T.GRP_CNTY_NM = S.GRP_CNTY_NM,
T.GRP_PHN_NO = S.GRP_PHN_NO,
T.GRP_PHN_EXT_NO = S.GRP_PHN_EXT_NO,
T.GRP_FAX_NO = S.GRP_FAX_NO,
T.GRP_FAX_EXT_NO = S.GRP_FAX_EXT_NO,
T.GRP_EMAIL_ADDR = S.GRP_EMAIL_ADDR,
T.LAST_UPDT_RUN_CYC_NO = S.LAST_UPDT_RUN_CYC_NO,
T.GRP_EDI_ACCT_VNDR_CD = S.GRP_EDI_ACCT_VNDR_CD,
T.GRP_EDI_ACCT_VNDR_NM = S.GRP_EDI_ACCT_VNDR_NM,
T.GRP_MKT_SIZE_CAT_CD = S.GRP_MKT_SIZE_CAT_CD,
T.GRP_MKT_SIZE_CAT_NM = S.GRP_MKT_SIZE_CAT_NM,
T.GRP_WEB_RSTRCT_IN = S.GRP_WEB_RSTRCT_IN,
T.GRP_CUR_ANNV_DT = S.GRP_CUR_ANNV_DT,
T.GRP_NEXT_ANNV_DT = S.GRP_NEXT_ANNV_DT,
T.GRP_REINST_DT = S.GRP_REINST_DT,
T.GRP_RNWL_DT = S.GRP_RNWL_DT,
T.GRP_STTUS_CD = S.GRP_STTUS_CD,
T.GRP_STTUS_NM = S.GRP_STTUS_NM,
T.GRP_TERM_RSN_CD = S.GRP_TERM_RSN_CD,
T.GRP_TERM_RSN_NM = S.GRP_TERM_RSN_NM,
T.GRP_MKTNG_TERR_ID = S.GRP_MKTNG_TERR_ID,
T.IDS_LAST_UPDT_RUN_CYC_EXCTN_SK = S.IDS_LAST_UPDT_RUN_CYC_EXCTN_SK
WHEN NOT MATCHED THEN
INSERT
(
SRC_SYS_CD,
GRP_ID,
GRP_ORIG_EFF_DT,
GRP_TERM_DT,
GRP_UNIQ_KEY,
GRP_NM,
GRP_ADDR_LN_1,
GRP_ADDR_LN_2,
GRP_ADDR_LN_3,
GRP_CITY_NM,
GRP_ST_CD,
GRP_POSTAL_CD,
GRP_CNTY_NM,
GRP_PHN_NO,
GRP_PHN_EXT_NO,
GRP_FAX_NO,
GRP_FAX_EXT_NO,
GRP_EMAIL_ADDR,
LAST_UPDT_RUN_CYC_NO,
GRP_EDI_ACCT_VNDR_CD,
GRP_EDI_ACCT_VNDR_NM,
GRP_MKT_SIZE_CAT_CD,
GRP_MKT_SIZE_CAT_NM,
GRP_WEB_RSTRCT_IN,
GRP_CUR_ANNV_DT,
GRP_NEXT_ANNV_DT,
GRP_REINST_DT,
GRP_RNWL_DT,
GRP_STTUS_CD,
GRP_STTUS_NM,
GRP_TERM_RSN_CD,
GRP_TERM_RSN_NM,
GRP_MKTNG_TERR_ID,
IDS_LAST_UPDT_RUN_CYC_EXCTN_SK
)
VALUES
(
S.SRC_SYS_CD,
S.GRP_ID,
S.GRP_ORIG_EFF_DT,
S.GRP_TERM_DT,
S.GRP_UNIQ_KEY,
S.GRP_NM,
S.GRP_ADDR_LN_1,
S.GRP_ADDR_LN_2,
S.GRP_ADDR_LN_3,
S.GRP_CITY_NM,
S.GRP_ST_CD,
S.GRP_POSTAL_CD,
S.GRP_CNTY_NM,
S.GRP_PHN_NO,
S.GRP_PHN_EXT_NO,
S.GRP_FAX_NO,
S.GRP_FAX_EXT_NO,
S.GRP_EMAIL_ADDR,
S.LAST_UPDT_RUN_CYC_NO,
S.GRP_EDI_ACCT_VNDR_CD,
S.GRP_EDI_ACCT_VNDR_NM,
S.GRP_MKT_SIZE_CAT_CD,
S.GRP_MKT_SIZE_CAT_NM,
S.GRP_WEB_RSTRCT_IN,
S.GRP_CUR_ANNV_DT,
S.GRP_NEXT_ANNV_DT,
S.GRP_REINST_DT,
S.GRP_RNWL_DT,
S.GRP_STTUS_CD,
S.GRP_STTUS_NM,
S.GRP_TERM_RSN_CD,
S.GRP_TERM_RSN_NM,
S.GRP_MKTNG_TERR_ID,
S.IDS_LAST_UPDT_RUN_CYC_EXCTN_SK
);
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)

# seq_MBRSH_DM_GRP_csv_rej (Reject Link)
df_ODBC_MBRSH_DM_GRP_out_rej = spark.createDataFrame(
    [],
    StructType([
        StructField("ERRORCODE", StringType(), True),
        StructField("ERRORTEXT", StringType(), True)
    ])
)

df_ODBC_MBRSH_DM_GRP_out_rej = df_ODBC_MBRSH_DM_GRP_out_rej.select("ERRORCODE", "ERRORTEXT")

write_files(
    df_ODBC_MBRSH_DM_GRP_out_rej,
    f"{adls_path}/load/MBRSH_DM_GRP_Rej.dat",
    delimiter=",",
    mode="append",
    is_parqet=False,
    header=False,
    quote="^",
    nullValue=None
)