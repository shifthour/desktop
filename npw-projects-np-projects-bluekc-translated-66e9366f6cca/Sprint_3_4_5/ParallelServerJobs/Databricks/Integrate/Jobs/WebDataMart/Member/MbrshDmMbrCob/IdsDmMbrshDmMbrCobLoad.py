# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        ---------------------------------------------------------------------------                             ------------------------------    ------------------------------       --------------------
# MAGIC Bhupinder Kaur         2013 -08-08      5114                             Load  DM MBRSH_DM_MBR_COB                                                   IntegrateWrhsDevl        Jag Yelavarthi            2013-10-23

# MAGIC Job Name: IdsDmMbrshDmMbrCobLoad
# MAGIC Read Load File created in the IdsDmMbrshDmMbrCobExtr Job
# MAGIC Copy Stage for buffer
# MAGIC Update then insert  MBRSH_DM_MBR_COB Data.
# MAGIC Load rejects are redirected into a Reject file
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
ClmMartOwner = get_widget_value('ClmMartOwner','')
ClmMartArraySize = get_widget_value('ClmMartArraySize','')
ClmMartRecordCount = get_widget_value('ClmMartRecordCount','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


schema_seq_MBRSH_DM_MBR_COB_csv_load = StructType([
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("MBR_UNIQ_KEY", IntegerType(), nullable=False),
    StructField("MBR_COB_TYP_CD", StringType(), nullable=False),
    StructField("MBR_COB_PAYMT_PRTY_CD", StringType(), nullable=False),
    StructField("MBR_COB_EFF_DT", TimestampType(), nullable=False),
    StructField("MBR_COB_TYP_NM", StringType(), nullable=True),
    StructField("MBR_COB_LAST_VER_METH_CD", StringType(), nullable=False),
    StructField("MBR_COB_LAST_VER_METH_NM", StringType(), nullable=True),
    StructField("MBR_COB_OTHR_CAR_ID_CD", StringType(), nullable=False),
    StructField("MBR_COB_OTHR_CAR_ID_NM", StringType(), nullable=True),
    StructField("MBR_COB_PAYMT_PRTY_NM", StringType(), nullable=True),
    StructField("MBR_COB_TERM_RSN_CD", StringType(), nullable=False),
    StructField("MBR_COB_TERM_RSN_NM", StringType(), nullable=True),
    StructField("MBR_COB_LACK_COB_INFO_STRT_DT", TimestampType(), nullable=True),
    StructField("MBR_COB_LTR_TRGR_DT", TimestampType(), nullable=True),
    StructField("MBR_COB_TERM_DT", TimestampType(), nullable=False),
    StructField("GRP_ID", StringType(), nullable=False),
    StructField("GRP_UNIQ_KEY", IntegerType(), nullable=False),
    StructField("MBR_COB_LAST_VERIFIER_TX", StringType(), nullable=True),
    StructField("MBR_COB_OTHR_CAR_POL_ID", StringType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_NO", IntegerType(), nullable=False),
    StructField("MBR_COB_CAT_CD", StringType(), nullable=True),
    StructField("MBR_COB_CAT_NM", StringType(), nullable=True),
    StructField("MBR_COB_ACTV_COB_IN", StringType(), nullable=True)
])

df_seq_MBRSH_DM_MBR_COB_csv_load = (
    spark.read
    .option("header", False)
    .option("sep", ",")
    .option("quote", "^")
    .option("nullValue", None)
    .schema(schema_seq_MBRSH_DM_MBR_COB_csv_load)
    .csv(f"{adls_path}/load/MBRSH_DM_MBR_COB.dat")
)

df_cpy_forBuffer = df_seq_MBRSH_DM_MBR_COB_csv_load.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("MBR_COB_TYP_CD").alias("MBR_COB_TYP_CD"),
    F.col("MBR_COB_PAYMT_PRTY_CD").alias("MBR_COB_PAYMT_PRTY_CD"),
    F.col("MBR_COB_EFF_DT").alias("MBR_COB_EFF_DT"),
    F.col("MBR_COB_TYP_NM").alias("MBR_COB_TYP_NM"),
    F.col("MBR_COB_LAST_VER_METH_CD").alias("MBR_COB_LAST_VER_METH_CD"),
    F.col("MBR_COB_LAST_VER_METH_NM").alias("MBR_COB_LAST_VER_METH_NM"),
    F.col("MBR_COB_OTHR_CAR_ID_CD").alias("MBR_COB_OTHR_CAR_ID_CD"),
    F.col("MBR_COB_OTHR_CAR_ID_NM").alias("MBR_COB_OTHR_CAR_ID_NM"),
    F.col("MBR_COB_PAYMT_PRTY_NM").alias("MBR_COB_PAYMT_PRTY_NM"),
    F.col("MBR_COB_TERM_RSN_CD").alias("MBR_COB_TERM_RSN_CD"),
    F.col("MBR_COB_TERM_RSN_NM").alias("MBR_COB_TERM_RSN_NM"),
    F.col("MBR_COB_LACK_COB_INFO_STRT_DT").alias("MBR_COB_LACK_COB_INFO_STRT_DT"),
    F.col("MBR_COB_LTR_TRGR_DT").alias("MBR_COB_LTR_TRGR_DT"),
    F.col("MBR_COB_TERM_DT").alias("MBR_COB_TERM_DT"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("GRP_UNIQ_KEY").alias("GRP_UNIQ_KEY"),
    F.col("MBR_COB_LAST_VERIFIER_TX").alias("MBR_COB_LAST_VERIFIER_TX"),
    F.col("MBR_COB_OTHR_CAR_POL_ID").alias("MBR_COB_OTHR_CAR_POL_ID"),
    F.col("LAST_UPDT_RUN_CYC_NO").alias("LAST_UPDT_RUN_CYC_NO"),
    F.col("MBR_COB_CAT_CD").alias("MBR_COB_CAT_CD"),
    F.col("MBR_COB_CAT_NM").alias("MBR_COB_CAT_NM"),
    F.col("MBR_COB_ACTV_COB_IN").alias("MBR_COB_ACTV_COB_IN")
)

df_ODBC_MBRSH_DM_MBR_COB_out_input = df_cpy_forBuffer

jdbc_url, jdbc_props = get_db_config(clmmart_secret_name)
temp_table_odbc_mbr = "STAGING.IdsDmMbrshDmMbrCobLoad_ODBC_MBRSH_DM_MBR_COB_out_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_odbc_mbr}", jdbc_url, jdbc_props)

(
    df_ODBC_MBRSH_DM_MBR_COB_out_input
    .write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", temp_table_odbc_mbr)
    .mode("overwrite")
    .save()
)

merge_sql = f"""
MERGE #$ClmMartOwner#.MBRSH_DM_MBR_COB AS T
USING {temp_table_odbc_mbr} AS S
ON
    T.SRC_SYS_CD = S.SRC_SYS_CD
    AND T.MBR_UNIQ_KEY = S.MBR_UNIQ_KEY
    AND T.MBR_COB_TYP_CD = S.MBR_COB_TYP_CD
    AND T.MBR_COB_PAYMT_PRTY_CD = S.MBR_COB_PAYMT_PRTY_CD
    AND T.MBR_COB_EFF_DT = S.MBR_COB_EFF_DT
WHEN MATCHED THEN
  UPDATE SET
    T.MBR_COB_TYP_NM = S.MBR_COB_TYP_NM,
    T.MBR_COB_LAST_VER_METH_CD = S.MBR_COB_LAST_VER_METH_CD,
    T.MBR_COB_LAST_VER_METH_NM = S.MBR_COB_LAST_VER_METH_NM,
    T.MBR_COB_OTHR_CAR_ID_CD = S.MBR_COB_OTHR_CAR_ID_CD,
    T.MBR_COB_OTHR_CAR_ID_NM = S.MBR_COB_OTHR_CAR_ID_NM,
    T.MBR_COB_PAYMT_PRTY_NM = S.MBR_COB_PAYMT_PRTY_NM,
    T.MBR_COB_TERM_RSN_CD = S.MBR_COB_TERM_RSN_CD,
    T.MBR_COB_TERM_RSN_NM = S.MBR_COB_TERM_RSN_NM,
    T.MBR_COB_LACK_COB_INFO_STRT_DT = S.MBR_COB_LACK_COB_INFO_STRT_DT,
    T.MBR_COB_LTR_TRGR_DT = S.MBR_COB_LTR_TRGR_DT,
    T.MBR_COB_TERM_DT = S.MBR_COB_TERM_DT,
    T.GRP_ID = S.GRP_ID,
    T.GRP_UNIQ_KEY = S.GRP_UNIQ_KEY,
    T.MBR_COB_LAST_VERIFIER_TX = S.MBR_COB_LAST_VERIFIER_TX,
    T.MBR_COB_OTHR_CAR_POL_ID = S.MBR_COB_OTHR_CAR_POL_ID,
    T.LAST_UPDT_RUN_CYC_NO = S.LAST_UPDT_RUN_CYC_NO,
    T.MBR_COB_CAT_CD = S.MBR_COB_CAT_CD,
    T.MBR_COB_CAT_NM = S.MBR_COB_CAT_NM,
    T.MBR_COB_ACTV_COB_IN = S.MBR_COB_ACTV_COB_IN
WHEN NOT MATCHED THEN
  INSERT (
    SRC_SYS_CD,
    MBR_UNIQ_KEY,
    MBR_COB_TYP_CD,
    MBR_COB_PAYMT_PRTY_CD,
    MBR_COB_EFF_DT,
    MBR_COB_TYP_NM,
    MBR_COB_LAST_VER_METH_CD,
    MBR_COB_LAST_VER_METH_NM,
    MBR_COB_OTHR_CAR_ID_CD,
    MBR_COB_OTHR_CAR_ID_NM,
    MBR_COB_PAYMT_PRTY_NM,
    MBR_COB_TERM_RSN_CD,
    MBR_COB_TERM_RSN_NM,
    MBR_COB_LACK_COB_INFO_STRT_DT,
    MBR_COB_LTR_TRGR_DT,
    MBR_COB_TERM_DT,
    GRP_ID,
    GRP_UNIQ_KEY,
    MBR_COB_LAST_VERIFIER_TX,
    MBR_COB_OTHR_CAR_POL_ID,
    LAST_UPDT_RUN_CYC_NO,
    MBR_COB_CAT_CD,
    MBR_COB_CAT_NM,
    MBR_COB_ACTV_COB_IN
  )
  VALUES (
    S.SRC_SYS_CD,
    S.MBR_UNIQ_KEY,
    S.MBR_COB_TYP_CD,
    S.MBR_COB_PAYMT_PRTY_CD,
    S.MBR_COB_EFF_DT,
    S.MBR_COB_TYP_NM,
    S.MBR_COB_LAST_VER_METH_CD,
    S.MBR_COB_LAST_VER_METH_NM,
    S.MBR_COB_OTHR_CAR_ID_CD,
    S.MBR_COB_OTHR_CAR_ID_NM,
    S.MBR_COB_PAYMT_PRTY_NM,
    S.MBR_COB_TERM_RSN_CD,
    S.MBR_COB_TERM_RSN_NM,
    S.MBR_COB_LACK_COB_INFO_STRT_DT,
    S.MBR_COB_LTR_TRGR_DT,
    S.MBR_COB_TERM_DT,
    S.GRP_ID,
    S.GRP_UNIQ_KEY,
    S.MBR_COB_LAST_VERIFIER_TX,
    S.MBR_COB_OTHR_CAR_POL_ID,
    S.LAST_UPDT_RUN_CYC_NO,
    S.MBR_COB_CAT_CD,
    S.MBR_COB_CAT_NM,
    S.MBR_COB_ACTV_COB_IN
  );
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

schema_seq_MBRSH_DM_MBR_COB_csv_rej = StructType([
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("MBR_UNIQ_KEY", IntegerType(), True),
    StructField("MBR_COB_TYP_CD", StringType(), True),
    StructField("MBR_COB_PAYMT_PRTY_CD", StringType(), True),
    StructField("MBR_COB_EFF_DT", TimestampType(), True),
    StructField("MBR_COB_TYP_NM", StringType(), True),
    StructField("MBR_COB_LAST_VER_METH_CD", StringType(), True),
    StructField("MBR_COB_LAST_VER_METH_NM", StringType(), True),
    StructField("MBR_COB_OTHR_CAR_ID_CD", StringType(), True),
    StructField("MBR_COB_OTHR_CAR_ID_NM", StringType(), True),
    StructField("MBR_COB_PAYMT_PRTY_NM", StringType(), True),
    StructField("MBR_COB_TERM_RSN_CD", StringType(), True),
    StructField("MBR_COB_TERM_RSN_NM", StringType(), True),
    StructField("MBR_COB_LACK_COB_INFO_STRT_DT", TimestampType(), True),
    StructField("MBR_COB_LTR_TRGR_DT", TimestampType(), True),
    StructField("MBR_COB_TERM_DT", TimestampType(), True),
    StructField("GRP_ID", StringType(), True),
    StructField("GRP_UNIQ_KEY", IntegerType(), True),
    StructField("MBR_COB_LAST_VERIFIER_TX", StringType(), True),
    StructField("MBR_COB_OTHR_CAR_POL_ID", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_NO", IntegerType(), True),
    StructField("MBR_COB_CAT_CD", StringType(), True),
    StructField("MBR_COB_CAT_NM", StringType(), True),
    StructField("MBR_COB_ACTV_COB_IN", StringType(), True),
    StructField("ERRORCODE", StringType(), True),
    StructField("ERRORTEXT", StringType(), True)
])

df_seq_MBRSH_DM_MBR_COB_csv_rej_empty = spark.createDataFrame([], schema_seq_MBRSH_DM_MBR_COB_csv_rej)
df_seq_MBRSH_DM_MBR_COB_csv_rej_empty = df_seq_MBRSH_DM_MBR_COB_csv_rej_empty.withColumn(
    "MBR_COB_ACTV_COB_IN",
    F.rpad(F.col("MBR_COB_ACTV_COB_IN"), 1, " ")
)

df_seq_MBRSH_DM_MBR_COB_csv_rej_empty = df_seq_MBRSH_DM_MBR_COB_csv_rej_empty.select(
    "SRC_SYS_CD",
    "MBR_UNIQ_KEY",
    "MBR_COB_TYP_CD",
    "MBR_COB_PAYMT_PRTY_CD",
    "MBR_COB_EFF_DT",
    "MBR_COB_TYP_NM",
    "MBR_COB_LAST_VER_METH_CD",
    "MBR_COB_LAST_VER_METH_NM",
    "MBR_COB_OTHR_CAR_ID_CD",
    "MBR_COB_OTHR_CAR_ID_NM",
    "MBR_COB_PAYMT_PRTY_NM",
    "MBR_COB_TERM_RSN_CD",
    "MBR_COB_TERM_RSN_NM",
    "MBR_COB_LACK_COB_INFO_STRT_DT",
    "MBR_COB_LTR_TRGR_DT",
    "MBR_COB_TERM_DT",
    "GRP_ID",
    "GRP_UNIQ_KEY",
    "MBR_COB_LAST_VERIFIER_TX",
    "MBR_COB_OTHR_CAR_POL_ID",
    "LAST_UPDT_RUN_CYC_NO",
    "MBR_COB_CAT_CD",
    "MBR_COB_CAT_NM",
    "MBR_COB_ACTV_COB_IN",
    "ERRORCODE",
    "ERRORTEXT"
)

write_files(
    df_seq_MBRSH_DM_MBR_COB_csv_rej_empty,
    f"{adls_path}/load/MBRSH_DM_MBR_COB_Rej.dat",
    delimiter=",",
    mode="append",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)