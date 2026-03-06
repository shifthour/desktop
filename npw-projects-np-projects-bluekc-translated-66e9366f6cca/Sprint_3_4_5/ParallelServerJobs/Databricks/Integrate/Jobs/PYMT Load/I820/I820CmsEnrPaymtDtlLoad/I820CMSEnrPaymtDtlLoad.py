# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Job: I820CMSEnrPaymtDtlLoad
# MAGIC 
# MAGIC This job Loads CMS_ENR_PAYMT_DTL table from I820 file
# MAGIC 
# MAGIC CALLED BY: CMSI820BCBSFinanceTablesLoadSeq
# MAGIC 
# MAGIC Developer                                            Date                          Project or Ticket                         Description                                    Environment                   Code Reviewer               Review Date
# MAGIC ------------------------                           -----------------------          ---------------------------------------            ---------------------------------------------------    -----------------------------          -----------------------------          ------------------------------
# MAGIC Raj Kommineni                               2021-12-15              US390105                                        Original programming                     IntegrateDev2           Jeyaprasanna                 2021-12-18  
# MAGIC Prabhu ES                                     2022-03-30               S2S                                        MSSQL ODBC conn params added       IntegrateDev5		Harsha Ravuri	06-03-2022

# MAGIC This job loads the SybaseTable  CMS_ENR_PAYMT_DTL from I820_CMS_ENR_PAYMT_DTL_VALENCIA.DAT
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType,
    DateType,
    TimestampType
)
from pyspark.sql.functions import col, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


BCBSFINOwner = get_widget_value('BCBSFINOwner','')
bcbsfin_secret_name = get_widget_value('bcbsfin_secret_name','')

schema_seq_cmsenrpaymtdtlextr = StructType([
    StructField("EXCH_MBR_ID", StringType(), False),
    StructField("QHP_ID", StringType(), False),
    StructField("PAYMT_SUBMT_DT", DateType(), False),
    StructField("PAYMT_COV_YRMO", StringType(), False),
    StructField("ST_CD", StringType(), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("TOT_PRM_AMT", DecimalType(38,10), True),
    StructField("COV_STRT_DT", TimestampType(), False),
    StructField("COV_END_DT", TimestampType(), False),
    StructField("ENR_TRANS_TYP_CD", StringType(), False),
    StructField("FCTS_SUB_ID", StringType(), False),
    StructField("FCTS_PROD_ID", StringType(), False),
    StructField("MBR_RELSHP_TYP_CD", StringType(), False),
    StructField("MBR_PRM_AMT", DecimalType(38,10), True),
    StructField("PAYMT_RCVD_DT", DateType(), False),
    StructField("AS_OF_DT", TimestampType(), False),
    StructField("BILL_ENTY_UNIQ_KEY", IntegerType(), False),
    StructField("CLS_PLN_ID", StringType(), False),
    StructField("GRP_LVL_BILL_ENTY_UNIQ_KEY", IntegerType(), True),
    StructField("EFT_TRACE_ID", StringType(), True),
    StructField("ENR_PAYMT_UNIQ_KEY", IntegerType(), True),
    StructField("SUB_FIRST_NM", StringType(), True),
    StructField("SUB_LAST_NM", StringType(), True),
    StructField("EXCH_ASG_POL_ID", StringType(), True),
    StructField("EXCH_ASG_SUB_ID", StringType(), True),
    StructField("EXCH_RPT_DOC_CTL_NO", StringType(), True),
    StructField("EXCH_RPT_NM", StringType(), True),
    StructField("CRT_DT", TimestampType(), False),
    StructField("LAST_UPDT_DT", TimestampType(), False)
])

df_seq_cmsenrpaymtdtlextr = (
    spark.read
    .format("csv")
    .option("header", "false")
    .option("sep", ",")
    .option("quote", "\"")
    .option("nullValue", "")
    .schema(schema_seq_cmsenrpaymtdtlextr)
    .load(f"{adls_path}/load/I820_CMS_ENR_PAYMT_DTL_VALENCIA.DAT")
)

df_cpy_cmsenrpaymntdtlfl = df_seq_cmsenrpaymtdtlextr.select(
    col("EXCH_MBR_ID"),
    col("QHP_ID"),
    col("PAYMT_SUBMT_DT"),
    rpad(col("PAYMT_COV_YRMO"), 6, " ").alias("PAYMT_COV_YRMO"),
    col("ST_CD"),
    col("SRC_SYS_CD"),
    col("TOT_PRM_AMT"),
    col("COV_STRT_DT"),
    col("COV_END_DT"),
    col("ENR_TRANS_TYP_CD"),
    col("FCTS_SUB_ID"),
    col("FCTS_PROD_ID"),
    col("MBR_RELSHP_TYP_CD"),
    col("MBR_PRM_AMT"),
    col("PAYMT_RCVD_DT"),
    col("AS_OF_DT"),
    col("BILL_ENTY_UNIQ_KEY"),
    col("CLS_PLN_ID"),
    col("GRP_LVL_BILL_ENTY_UNIQ_KEY"),
    col("EFT_TRACE_ID"),
    col("ENR_PAYMT_UNIQ_KEY"),
    col("SUB_FIRST_NM"),
    col("SUB_LAST_NM"),
    col("EXCH_ASG_POL_ID"),
    col("EXCH_ASG_SUB_ID"),
    col("EXCH_RPT_DOC_CTL_NO"),
    col("EXCH_RPT_NM"),
    col("CRT_DT"),
    col("LAST_UPDT_DT")
)

jdbc_url, jdbc_props = get_db_config(bcbsfin_secret_name)

execute_dml(
    "DROP TABLE IF EXISTS STAGING.I820CMSEnrPaymtDtlLoad_BCBSFIN_CMS_ENR_PAYMT_DTL_temp",
    jdbc_url,
    jdbc_props
)

df_cpy_cmsenrpaymntdtlfl.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.I820CMSEnrPaymtDtlLoad_BCBSFIN_CMS_ENR_PAYMT_DTL_temp") \
    .mode("errorifexists") \
    .save()

merge_sql = f"""
MERGE {BCBSFINOwner}.CMS_ENR_PAYMT_DTL AS T
USING STAGING.I820CMSEnrPaymtDtlLoad_BCBSFIN_CMS_ENR_PAYMT_DTL_temp AS S
ON T.EXCH_MBR_ID = S.EXCH_MBR_ID
   AND T.QHP_ID = S.QHP_ID
   AND T.PAYMT_SUBMT_DT = S.PAYMT_SUBMT_DT
   AND T.PAYMT_COV_YRMO = S.PAYMT_COV_YRMO
   AND T.ST_CD = S.ST_CD
   AND T.SRC_SYS_CD = S.SRC_SYS_CD
   AND T.ENR_TRANS_TYP_CD = S.ENR_TRANS_TYP_CD
WHEN MATCHED THEN
  UPDATE SET
    T.TOT_PRM_AMT = S.TOT_PRM_AMT,
    T.COV_STRT_DT = S.COV_STRT_DT,
    T.COV_END_DT = S.COV_END_DT,
    T.FCTS_SUB_ID = S.FCTS_SUB_ID,
    T.FCTS_PROD_ID = S.FCTS_PROD_ID,
    T.MBR_RELSHP_TYP_CD = S.MBR_RELSHP_TYP_CD,
    T.MBR_PRM_AMT = S.MBR_PRM_AMT,
    T.PAYMT_RCVD_DT = S.PAYMT_RCVD_DT,
    T.AS_OF_DT = S.AS_OF_DT,
    T.BILL_ENTY_UNIQ_KEY = S.BILL_ENTY_UNIQ_KEY,
    T.CLS_PLN_ID = S.CLS_PLN_ID,
    T.GRP_LVL_BILL_ENTY_UNIQ_KEY = S.GRP_LVL_BILL_ENTY_UNIQ_KEY,
    T.EFT_TRACE_ID = S.EFT_TRACE_ID,
    T.ENR_PAYMT_UNIQ_KEY = S.ENR_PAYMT_UNIQ_KEY,
    T.SUB_FIRST_NM = S.SUB_FIRST_NM,
    T.SUB_LAST_NM = S.SUB_LAST_NM,
    T.EXCH_ASG_POL_ID = S.EXCH_ASG_POL_ID,
    T.EXCH_ASG_SUB_ID = S.EXCH_ASG_SUB_ID,
    T.EXCH_RPT_DOC_CTL_NO = S.EXCH_RPT_DOC_CTL_NO,
    T.EXCH_RPT_NM = S.EXCH_RPT_NM,
    T.CRT_DT = S.CRT_DT,
    T.LAST_UPDT_DT = S.LAST_UPDT_DT
WHEN NOT MATCHED THEN
  INSERT (
    EXCH_MBR_ID, QHP_ID, PAYMT_SUBMT_DT, PAYMT_COV_YRMO, ST_CD, SRC_SYS_CD, TOT_PRM_AMT, COV_STRT_DT,
    COV_END_DT, ENR_TRANS_TYP_CD, FCTS_SUB_ID, FCTS_PROD_ID, MBR_RELSHP_TYP_CD, MBR_PRM_AMT,
    PAYMT_RCVD_DT, AS_OF_DT, BILL_ENTY_UNIQ_KEY, CLS_PLN_ID, GRP_LVL_BILL_ENTY_UNIQ_KEY,
    EFT_TRACE_ID, ENR_PAYMT_UNIQ_KEY, SUB_FIRST_NM, SUB_LAST_NM, EXCH_ASG_POL_ID, EXCH_ASG_SUB_ID,
    EXCH_RPT_DOC_CTL_NO, EXCH_RPT_NM, CRT_DT, LAST_UPDT_DT
  )
  VALUES (
    S.EXCH_MBR_ID, S.QHP_ID, S.PAYMT_SUBMT_DT, S.PAYMT_COV_YRMO, S.ST_CD, S.SRC_SYS_CD, S.TOT_PRM_AMT, S.COV_STRT_DT,
    S.COV_END_DT, S.ENR_TRANS_TYP_CD, S.FCTS_SUB_ID, S.FCTS_PROD_ID, S.MBR_RELSHP_TYP_CD, S.MBR_PRM_AMT,
    S.PAYMT_RCVD_DT, S.AS_OF_DT, S.BILL_ENTY_UNIQ_KEY, S.CLS_PLN_ID, S.GRP_LVL_BILL_ENTY_UNIQ_KEY,
    S.EFT_TRACE_ID, S.ENR_PAYMT_UNIQ_KEY, S.SUB_FIRST_NM, S.SUB_LAST_NM, S.EXCH_ASG_POL_ID, S.EXCH_ASG_SUB_ID,
    S.EXCH_RPT_DOC_CTL_NO, S.EXCH_RPT_NM, S.CRT_DT, S.LAST_UPDT_DT
  );
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)