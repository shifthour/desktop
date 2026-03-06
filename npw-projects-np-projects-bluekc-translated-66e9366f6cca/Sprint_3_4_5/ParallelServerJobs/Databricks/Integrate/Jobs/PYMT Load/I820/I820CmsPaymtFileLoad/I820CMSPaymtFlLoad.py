# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Job: I820CMSPaymtFlLoad
# MAGIC 
# MAGIC This job loads data to CMS_PAYMT_FILE
# MAGIC 
# MAGIC CALLED BY: GORMANI820BCBSFinanceTablesLoadSeq
# MAGIC 
# MAGIC Developer                                            Date                          Project or Ticket                         Description                                    Environment                   Code Reviewer               Review Date
# MAGIC ------------------------                           -----------------------          ---------------------------------------            ---------------------------------------------------    -----------------------------          -----------------------------          ------------------------------
# MAGIC Raj Kommineni                               2021-12-15              US390105                                        Original programming                     IntegrateDev2           Jeyaprasanna                  2021-12-18
# MAGIC Prabhu ES                                     2022-04-27               S2S                                       MSSQL ODBC conn params added         IntegrateDev5 		Harsha Ravuri	06-03-2022

# MAGIC This job loads the SybaseTable  CMS_PAYMT_FILE from intermediate file I820_CMS_PAYMT_FILE_VALENCIA.dat
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from pyspark.sql.functions import rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


BCBSFINOwner = get_widget_value('BCBSFINOwner','')
bcbsfin_secret_name = get_widget_value('bcbsfin_secret_name','')

schemaCmsPaymtFlExtr = StructType([
    StructField("EFT_TRACE_ID", StringType(), True),
    StructField("EFT_EFF_DT", TimestampType(), True),
    StructField("EFT_TOT_PAYMT_AMT", DecimalType(38,10), True),
    StructField("TRDNG_PRTNR_ID", StringType(), True),
    StructField("SRC_SYS_CRT_DT", TimestampType(), True),
    StructField("LOAD_FILE_NM", StringType(), True)
])

df_CmsPaymtFlExtr = (
    spark.read.format("csv")
    .option("sep", ",")
    .option("quote", "\"")
    .option("header", False)
    .schema(schemaCmsPaymtFlExtr)
    .load(f"{adls_path}/load/I820_CMS_PAYMT_FILE_VALENCIA.dat")
)

df_Cpy_CmsEnrPaymntDtlFl = df_CmsPaymtFlExtr.select(
    df_CmsPaymtFlExtr["EFT_TRACE_ID"].alias("EFT_TRACE_ID"),
    df_CmsPaymtFlExtr["EFT_EFF_DT"].alias("EFT_EFF_DT"),
    df_CmsPaymtFlExtr["EFT_TOT_PAYMT_AMT"].alias("EFT_TOT_PAYMT_AMT"),
    df_CmsPaymtFlExtr["TRDNG_PRTNR_ID"].alias("TRDNG_PRTNR_ID"),
    df_CmsPaymtFlExtr["SRC_SYS_CRT_DT"].alias("SRC_SYS_CRT_DT"),
    df_CmsPaymtFlExtr["LOAD_FILE_NM"].alias("LOAD_FILE_NM")
)

df_final = (
    df_Cpy_CmsEnrPaymntDtlFl
    .withColumn("EFT_TRACE_ID", rpad("EFT_TRACE_ID", <...>, " "))
    .withColumn("TRDNG_PRTNR_ID", rpad("TRDNG_PRTNR_ID", <...>, " "))
    .withColumn("LOAD_FILE_NM", rpad("LOAD_FILE_NM", <...>, " "))
    .select(
        "EFT_TRACE_ID",
        "EFT_EFF_DT",
        "EFT_TOT_PAYMT_AMT",
        "TRDNG_PRTNR_ID",
        "SRC_SYS_CRT_DT",
        "LOAD_FILE_NM"
    )
)

jdbc_url, jdbc_props = get_db_config(bcbsfin_secret_name)

execute_dml(
    "DROP TABLE IF EXISTS STAGING.I820CMSPaymtFlLoad_BCBSFIN_CMS_PAYMNT_FILE_temp",
    jdbc_url,
    jdbc_props
)

df_final.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.I820CMSPaymtFlLoad_BCBSFIN_CMS_PAYMNT_FILE_temp") \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO {BCBSFINOwner}.CMS_PAYMT_FILE AS T
USING STAGING.I820CMSPaymtFlLoad_BCBSFIN_CMS_PAYMNT_FILE_temp AS S
ON T.EFT_TRACE_ID = S.EFT_TRACE_ID
WHEN NOT MATCHED THEN
  INSERT (
    EFT_TRACE_ID,
    EFT_EFF_DT,
    EFT_TOT_PAYMT_AMT,
    TRDNG_PRTNR_ID,
    SRC_SYS_CRT_DT,
    LOAD_FILE_NM
  )
  VALUES (
    S.EFT_TRACE_ID,
    S.EFT_EFF_DT,
    S.EFT_TOT_PAYMT_AMT,
    S.TRDNG_PRTNR_ID,
    S.SRC_SYS_CRT_DT,
    S.LOAD_FILE_NM
  )
;
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)