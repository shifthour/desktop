# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Job: I820CMSAcaPgmPaymtLoad
# MAGIC 
# MAGIC This job Loads Sybase Table CMS_ACA_PGM_PAYMT
# MAGIC 
# MAGIC CALLED BY: GORMANI820BCBSFinanceTablesLoadSeq
# MAGIC 
# MAGIC Developer                                            Date                          Project or Ticket                         Description                                    Environment                   Code Reviewer               Review Date
# MAGIC ------------------------                           -----------------------          ---------------------------------------            ---------------------------------------------------    -----------------------------          -----------------------------          ------------------------------
# MAGIC Raj Kommineni                               2021-12-15              US390105                                        Original programming                     IntegrateDev2         Jeyaprasanna                     2021-12-18       
# MAGIC Prabhu ES                                     2022-04-07               S2S                                         MSSQL ODBC conn params added       IntegrateDev5          Kalyan Neelam                   2022-06-12

# MAGIC This job Loads Sybase Table CMS_ACA_PGM_PAYMT
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from pyspark.sql.functions import col, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


BCBSFINOwner = get_widget_value('BCBSFINOwner','')
bcbsfin_secret_name = get_widget_value('bcbsfin_secret_name','')

# Define schema for SF_CMSAcaPgmPaymtExtr
schema_SF_CMSAcaPgmPaymtExtr = StructType([
    StructField("ACTVTY_YRMO", StringType(), True),
    StructField("PAYMT_COV_YRMO", StringType(), True),
    StructField("PAYMT_TYP_CD", StringType(), True),
    StructField("STATE_CODE", StringType(), True),
    StructField("QHP_ID", StringType(), True),
    StructField("CMS_ACA_PGM_PAYMT_SEQ_NO", DecimalType(38,10), True),
    StructField("COV_START_DT", TimestampType(), True),
    StructField("COV_END_DT", TimestampType(), True),
    StructField("TRANS_AMT", DecimalType(38,10), True),
    StructField("ACA_PGM_PAYMT_UNIQ_KEY", DecimalType(38,10), True),
    StructField("EXCH_RPT_DOC_CTL_NO", StringType(), True),
    StructField("EXCH_RPT_NM", StringType(), True),
    StructField("EFT_TRACE_ID", StringType(), True),
    StructField("CRT_DT", TimestampType(), True),
    StructField("LAST_UPDT_DT", TimestampType(), True)
])

df_SF_CMSAcaPgmPaymtExtr = (
    spark.read.format("csv")
    .option("delimiter", ",")
    .option("quote", "\"")
    .option("header", "false")
    .schema(schema_SF_CMSAcaPgmPaymtExtr)
    .load(f"{adls_path}/load/I820_CMS_ACA_PGM_PAYMT_VALENCIA.DAT")
)

df_Cp_PgmPaymt = df_SF_CMSAcaPgmPaymtExtr.select(
    rpad(col("ACTVTY_YRMO"), 6, " ").alias("ACTVTY_YRMO"),
    rpad(col("PAYMT_COV_YRMO"), 6, " ").alias("PAYMT_COV_YRMO"),
    rpad(col("PAYMT_TYP_CD"), <...>, " ").alias("PAYMT_TYP_CD"),
    rpad(col("STATE_CODE"), <...>, " ").alias("STATE_CODE"),
    rpad(col("QHP_ID"), <...>, " ").alias("QHP_ID"),
    col("CMS_ACA_PGM_PAYMT_SEQ_NO").alias("CMS_ACA_PGM_PAYMT_SEQ_NO"),
    col("COV_START_DT").alias("COV_START_DT"),
    col("COV_END_DT").alias("COV_END_DT"),
    col("TRANS_AMT").alias("TRANS_AMT"),
    col("ACA_PGM_PAYMT_UNIQ_KEY").alias("ACA_PGM_PAYMT_UNIQ_KEY"),
    rpad(col("EXCH_RPT_DOC_CTL_NO"), <...>, " ").alias("EXCH_RPT_DOC_CTL_NO"),
    rpad(col("EXCH_RPT_NM"), <...>, " ").alias("EXCH_RPT_NM"),
    rpad(col("EFT_TRACE_ID"), <...>, " ").alias("EFT_TRACE_ID"),
    col("CRT_DT").alias("CRT_DT"),
    col("LAST_UPDT_DT").alias("LAST_UPDT_DT")
)

jdbc_url, jdbc_props = get_db_config(bcbsfin_secret_name)

temp_table_name = "STAGING.I820CMSAcaPgmPaymtLoad_BCBSFIN_CMS_ACA_PGM_PAYMT_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_name}", jdbc_url, jdbc_props)

df_Cp_PgmPaymt.write \
    .jdbc(url=jdbc_url, table=temp_table_name, mode="overwrite", properties=jdbc_props)

merge_sql = f"""
MERGE {BCBSFINOwner}.CMS_ACA_PGM_PAYMT AS T
USING {temp_table_name} AS S
ON 
    T.ACTVTY_YRMO = S.ACTVTY_YRMO
    AND T.PAYMT_COV_YRMO = S.PAYMT_COV_YRMO
    AND T.PAYMT_TYP_CD = S.PAYMT_TYP_CD
    AND T.STATE_CODE = S.STATE_CODE
    AND T.QHP_ID = S.QHP_ID
    AND T.CMS_ACA_PGM_PAYMT_SEQ_NO = S.CMS_ACA_PGM_PAYMT_SEQ_NO
WHEN MATCHED THEN 
  UPDATE SET
    T.COV_START_DT = S.COV_START_DT,
    T.COV_END_DT = S.COV_END_DT,
    T.TRANS_AMT = S.TRANS_AMT,
    T.ACA_PGM_PAYMT_UNIQ_KEY = S.ACA_PGM_PAYMT_UNIQ_KEY,
    T.EXCH_RPT_DOC_CTL_NO = S.EXCH_RPT_DOC_CTL_NO,
    T.EXCH_RPT_NM = S.EXCH_RPT_NM,
    T.EFT_TRACE_ID = S.EFT_TRACE_ID,
    T.CRT_DT = S.CRT_DT,
    T.LAST_UPDT_DT = S.LAST_UPDT_DT
WHEN NOT MATCHED THEN
  INSERT (
    ACTVTY_YRMO,
    PAYMT_COV_YRMO,
    PAYMT_TYP_CD,
    STATE_CODE,
    QHP_ID,
    CMS_ACA_PGM_PAYMT_SEQ_NO,
    COV_START_DT,
    COV_END_DT,
    TRANS_AMT,
    ACA_PGM_PAYMT_UNIQ_KEY,
    EXCH_RPT_DOC_CTL_NO,
    EXCH_RPT_NM,
    EFT_TRACE_ID,
    CRT_DT,
    LAST_UPDT_DT
  )
  VALUES (
    S.ACTVTY_YRMO,
    S.PAYMT_COV_YRMO,
    S.PAYMT_TYP_CD,
    S.STATE_CODE,
    S.QHP_ID,
    S.CMS_ACA_PGM_PAYMT_SEQ_NO,
    S.COV_START_DT,
    S.COV_END_DT,
    S.TRANS_AMT,
    S.ACA_PGM_PAYMT_UNIQ_KEY,
    S.EXCH_RPT_DOC_CTL_NO,
    S.EXCH_RPT_NM,
    S.EFT_TRACE_ID,
    S.CRT_DT,
    S.LAST_UPDT_DT
  )
;
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)