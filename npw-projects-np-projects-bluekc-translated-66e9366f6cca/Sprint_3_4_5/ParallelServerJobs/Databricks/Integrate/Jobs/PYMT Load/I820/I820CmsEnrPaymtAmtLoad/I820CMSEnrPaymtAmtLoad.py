# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Job: I820CMSEnrPaymtAmtLoad
# MAGIC 
# MAGIC This job Loads CMS_ENR_PAYMT_AMT table from I820 file
# MAGIC 
# MAGIC CALLED BY: CMSI820BCBSFinanceTablesLoadSeq
# MAGIC 
# MAGIC Developer                                            Date                          Project or Ticket                         Description                                    Environment                   Code Reviewer               Review Date
# MAGIC ------------------------                           -----------------------          ---------------------------------------            ---------------------------------------------------    -----------------------------          -----------------------------          ------------------------------
# MAGIC Raj Kommineni                               2021-12-15              US390105                                        Original programming                     IntegrateDev2          Jeyaprasanna                 2021-12-18   
# MAGIC Vikas Abbu                                    2022-04-18               S2S                                        Updated Facets to SQL server               IntegrateDev5

# MAGIC This job Loads Sybase Table CMS_ENR_PAYMT_AMT from intermediate file I820_CMS_ENR_PAYMT_VALENCIA.dat
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, DecimalType, StringType, IntegerType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


BCBSFINOwner = get_widget_value('BCBSFINOwner','')
bcbsfin_secret_name = get_widget_value('bcbsfin_secret_name','')

schema_SF_CMSEnrPaymtAmtExtr = StructType([
    StructField("CMS_ENR_PAYMT_DTL_CK", DecimalType(38,10), False),
    StructField("PAYMT_TYP_CD", StringType(), False),
    StructField("CMS_ENR_PAYMT_AMT_SEQ_NO", IntegerType(), False),
    StructField("PAYMT_AMT", DecimalType(38,10), False)
])

df_SF_CMSEnrPaymtAmtExtr = (
    spark.read
    .option("header", "false")
    .option("sep", ",")
    .schema(schema_SF_CMSEnrPaymtAmtExtr)
    .csv(f"{adls_path}/load/I820_CMS_ENR_PAYMT_AMT_VALENCIA.DAT")
)

df_CpPaymtAmt = df_SF_CMSEnrPaymtAmtExtr.select(
    "CMS_ENR_PAYMT_DTL_CK",
    "CMS_ENR_PAYMT_AMT_SEQ_NO",
    "PAYMT_TYP_CD",
    "PAYMT_AMT"
)

df_enriched = df_CpPaymtAmt.select(
    "CMS_ENR_PAYMT_DTL_CK",
    "CMS_ENR_PAYMT_AMT_SEQ_NO",
    "PAYMT_TYP_CD",
    "PAYMT_AMT"
)

df_enriched = df_enriched.withColumn(
    "PAYMT_TYP_CD", rpad("PAYMT_TYP_CD", <...>, " ")
)

jdbc_url, jdbc_props = get_db_config(bcbsfin_secret_name)

execute_dml("DROP TABLE IF EXISTS STAGING.I820CMSEnrPaymtAmtLoad_BCBSFIN_CMS_ENR_PAYMT_AMT_temp", jdbc_url, jdbc_props)

(
    df_enriched
    .write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", "STAGING.I820CMSEnrPaymtAmtLoad_BCBSFIN_CMS_ENR_PAYMT_AMT_temp")
    .mode("overwrite")
    .save()
)

merge_sql = f"""
MERGE {BCBSFINOwner}.CMS_ENR_PAYMT_AMT AS main
USING STAGING.I820CMSEnrPaymtAmtLoad_BCBSFIN_CMS_ENR_PAYMT_AMT_temp AS stage
ON (
  main.CMS_ENR_PAYMT_DTL_CK = stage.CMS_ENR_PAYMT_DTL_CK
  AND main.PAYMT_TYP_CD = stage.PAYMT_TYP_CD
  AND main.CMS_ENR_PAYMT_AMT_SEQ_NO = stage.CMS_ENR_PAYMT_AMT_SEQ_NO
)
WHEN MATCHED THEN
  UPDATE SET
    main.CMS_ENR_PAYMT_DTL_CK = stage.CMS_ENR_PAYMT_DTL_CK,
    main.PAYMT_TYP_CD = stage.PAYMT_TYP_CD,
    main.CMS_ENR_PAYMT_AMT_SEQ_NO = stage.CMS_ENR_PAYMT_AMT_SEQ_NO,
    main.PAYMT_AMT = stage.PAYMT_AMT
WHEN NOT MATCHED THEN
  INSERT (CMS_ENR_PAYMT_DTL_CK, PAYMT_TYP_CD, CMS_ENR_PAYMT_AMT_SEQ_NO, PAYMT_AMT)
  VALUES (
    stage.CMS_ENR_PAYMT_DTL_CK,
    stage.PAYMT_TYP_CD,
    stage.CMS_ENR_PAYMT_AMT_SEQ_NO,
    stage.PAYMT_AMT
  );
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)