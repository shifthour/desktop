# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2008 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by:
# MAGIC                     ScorecardEmplProdItemDailyCntl
# MAGIC 
# MAGIC 
# MAGIC Modifications:
# MAGIC =====================================================================================================================================================================
# MAGIC  Developer\(9)Date\(9)\(9)Project/Altiris #\(9)                    Change Description\(9)\(9)\(9)\(9)Development Project\(9)Code Reviewer\(9)             Date Reviewed
# MAGIC =====================================================================================================================================================================
# MAGIC Sruthi M    \(9)07/09/2018\(9)5236-Indigo Replacement\(9)    Original Development\(9)\(9)\(9)\(9)EnterpriseDev1\(9)                 Kalyan Neelam                     2018-07-16


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import col, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


APT_IMPORT_NOWARN_STRING_FIELD_OVERRUNS = get_widget_value('APT_IMPORT_NOWARN_STRING_FIELD_OVERRUNS','1')
ScoreCardOwner = get_widget_value('ScoreCardOwner','')
scorecard_secret_name = get_widget_value('scorecard_secret_name','')
Source = get_widget_value('Source','')

schema_Seq_Scrcrd_Item_Typ = StructType([
    StructField("SCRCRD_ITEM_TYP_ID", StringType(), False),
    StructField("SCRCRD_ITEM_TYP_DESC", StringType(), False),
    StructField("ACTV_IN", StringType(), False),
    StructField("CRT_DTM", TimestampType(), False),
    StructField("CRT_USER_ID", StringType(), False),
    StructField("LAST_UPDT_DTM", TimestampType(), False),
    StructField("LAST_UPDT_USER_ID", StringType(), False)
])

df_Seq_Scrcrd_Item_Typ = (
    spark.read.format("csv")
    .option("delimiter", ",")
    .option("quote", "\"")
    .option("header", "false")
    .schema(schema_Seq_Scrcrd_Item_Typ)
    .load(f"{adls_path}/Scrcrd_Item_Typ.dat")
)

df_bsns_logic_item_typ = df_Seq_Scrcrd_Item_Typ.select(
    col("SCRCRD_ITEM_TYP_ID").alias("SCRCRD_ITEM_TYP_ID"),
    col("SCRCRD_ITEM_TYP_DESC").alias("SCRCRD_ITEM_TYP_DESC"),
    col("ACTV_IN").alias("ACTV_IN"),
    col("CRT_DTM").alias("CRT_DTM"),
    col("CRT_USER_ID").alias("CRT_USER_ID"),
    col("LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
    col("LAST_UPDT_USER_ID").alias("LAST_UPDT_USER_ID")
)

df_SCRCRD_ITEM_TYP_enriched = (
    df_bsns_logic_item_typ
    .withColumn("SCRCRD_ITEM_TYP_ID", col("SCRCRD_ITEM_TYP_ID"))
    .withColumn("SCRCRD_ITEM_TYP_DESC", col("SCRCRD_ITEM_TYP_DESC"))
    .withColumn("ACTV_IN", rpad("ACTV_IN", 1, " "))
    .withColumn("CRT_DTM", col("CRT_DTM"))
    .withColumn("CRT_USER_ID", col("CRT_USER_ID"))
    .withColumn("LAST_UPDT_DTM", col("LAST_UPDT_DTM"))
    .withColumn("LAST_UPDT_USER_ID", col("LAST_UPDT_USER_ID"))
    .select(
        "SCRCRD_ITEM_TYP_ID",
        "SCRCRD_ITEM_TYP_DESC",
        "ACTV_IN",
        "CRT_DTM",
        "CRT_USER_ID",
        "LAST_UPDT_DTM",
        "LAST_UPDT_USER_ID"
    )
)

jdbc_url, jdbc_props = get_db_config(scorecard_secret_name)

execute_dml(
    "DROP TABLE IF EXISTS STAGING.ScrcrdEmpProdItemRefScrcrdItemTypLoad_SCRCRD_ITEM_TYP_temp",
    jdbc_url,
    jdbc_props
)

df_SCRCRD_ITEM_TYP_enriched.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.ScrcrdEmpProdItemRefScrcrdItemTypLoad_SCRCRD_ITEM_TYP_temp") \
    .mode("append") \
    .save()

merge_sql = f"""
MERGE INTO {ScoreCardOwner}.SCRCRD_ITEM_TYP as target
USING STAGING.ScrcrdEmpProdItemRefScrcrdItemTypLoad_SCRCRD_ITEM_TYP_temp as source
ON target.SCRCRD_ITEM_TYP_ID = source.SCRCRD_ITEM_TYP_ID
WHEN MATCHED THEN
  UPDATE SET
    target.SCRCRD_ITEM_TYP_DESC = source.SCRCRD_ITEM_TYP_DESC,
    target.ACTV_IN = source.ACTV_IN,
    target.CRT_DTM = source.CRT_DTM,
    target.CRT_USER_ID = source.CRT_USER_ID,
    target.LAST_UPDT_DTM = source.LAST_UPDT_DTM,
    target.LAST_UPDT_USER_ID = source.LAST_UPDT_USER_ID
WHEN NOT MATCHED THEN
  INSERT (
    SCRCRD_ITEM_TYP_ID,
    SCRCRD_ITEM_TYP_DESC,
    ACTV_IN,
    CRT_DTM,
    CRT_USER_ID,
    LAST_UPDT_DTM,
    LAST_UPDT_USER_ID
  )
  VALUES (
    source.SCRCRD_ITEM_TYP_ID,
    source.SCRCRD_ITEM_TYP_DESC,
    source.ACTV_IN,
    source.CRT_DTM,
    source.CRT_USER_ID,
    source.LAST_UPDT_DTM,
    source.LAST_UPDT_USER_ID
  );
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)