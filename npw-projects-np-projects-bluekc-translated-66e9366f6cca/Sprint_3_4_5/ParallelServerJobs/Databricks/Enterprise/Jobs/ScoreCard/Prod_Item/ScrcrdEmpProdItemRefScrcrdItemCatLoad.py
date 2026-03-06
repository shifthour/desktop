# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2008 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by:
# MAGIC                    ScorecardEmplProdItemDailyCntl
# MAGIC 
# MAGIC 
# MAGIC Modifications:
# MAGIC =====================================================================================================================================================================
# MAGIC  Developer\(9)Date\(9)\(9)Project/Altiris #\(9)                    Change Description\(9)\(9)\(9)\(9)Development Project\(9)Code Reviewer\(9)             Date Reviewed
# MAGIC =====================================================================================================================================================================
# MAGIC Sruthi M    \(9)07/09/2018\(9)5236-Indigo Replacement\(9)    Original Development\(9)\(9)\(9)\(9)EnterpriseDev1\(9)\(9) Kalyan Neelam                     2018-07-16


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import rpad, col
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


APT_IMPORT_NOWARN_STRING_FIELD_OVERRUNS = get_widget_value('APT_IMPORT_NOWARN_STRING_FIELD_OVERRUNS','1')
ScoreCardOwner = get_widget_value('ScoreCardOwner','')
scorecard_secret_name = get_widget_value('scorecard_secret_name','')
Source = get_widget_value('Source','')

schema_Seq_Scrcrd_Item_Cat = StructType([
    StructField("SCRCRD_ITEM_CAT_ID", StringType(), nullable=False),
    StructField("SCRCRD_ITEM_CAT_DESC", StringType(), nullable=False),
    StructField("ACTV_IN", StringType(), nullable=False),
    StructField("CRT_DTM", TimestampType(), nullable=False),
    StructField("CRT_USER_ID", StringType(), nullable=False),
    StructField("LAST_UPDT_DTM", TimestampType(), nullable=False),
    StructField("LAST_UPDT_USER_ID", StringType(), nullable=False)
])

df_Seq_Scrcrd_Item_Cat = (
    spark.read.format("csv")
    .schema(schema_Seq_Scrcrd_Item_Cat)
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "\"")
    .option("escape", "\"")
    .option("nullValue", None)
    .load(f"{adls_path}/Seq_Scrcrd_Item_Cat.dat")
)

df_bsns_logic_Item_Cat = df_Seq_Scrcrd_Item_Cat.selectExpr(
    "SCRCRD_ITEM_CAT_ID as SCRCRD_ITEM_CAT_ID",
    "SCRCRD_ITEM_CAT_DESC as SCRCRD_ITEM_CAT_DESC",
    "ACTV_IN as ACTV_IN",
    "CRT_DTM as CRT_DTM",
    "CRT_USER_ID as CRT_USER_ID",
    "LAST_UPDT_DTM as LAST_UPDT_DTM",
    "LAST_UPDT_USER_ID as LAST_UPDT_USER_ID"
)

df_SCRCRD_ITEM_CAT = df_bsns_logic_Item_Cat.select(
    rpad(col("SCRCRD_ITEM_CAT_ID"), <...>, " ").alias("SCRCRD_ITEM_CAT_ID"),
    rpad(col("SCRCRD_ITEM_CAT_DESC"), <...>, " ").alias("SCRCRD_ITEM_CAT_DESC"),
    rpad(col("ACTV_IN"), 1, " ").alias("ACTV_IN"),
    col("CRT_DTM").alias("CRT_DTM"),
    rpad(col("CRT_USER_ID"), <...>, " ").alias("CRT_USER_ID"),
    col("LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
    rpad(col("LAST_UPDT_USER_ID"), <...>, " ").alias("LAST_UPDT_USER_ID")
)

jdbc_url, jdbc_props = get_db_config(scorecard_secret_name)

execute_dml(
    f"DROP TABLE IF EXISTS STAGING.ScrcrdEmpProdItemRefScrcrdItemCatLoad_SCRCRD_ITEM_CAT_temp",
    jdbc_url,
    jdbc_props
)

df_SCRCRD_ITEM_CAT.write.jdbc(
    url=jdbc_url,
    table="STAGING.ScrcrdEmpProdItemRefScrcrdItemCatLoad_SCRCRD_ITEM_CAT_temp",
    mode="overwrite",
    properties=jdbc_props
)

merge_sql = f"""
MERGE INTO {ScoreCardOwner}.SCRCRD_ITEM_CAT AS T
USING STAGING.ScrcrdEmpProdItemRefScrcrdItemCatLoad_SCRCRD_ITEM_CAT_temp AS S
    ON T.SCRCRD_ITEM_CAT_ID = S.SCRCRD_ITEM_CAT_ID
WHEN MATCHED THEN
  UPDATE SET
    T.SCRCRD_ITEM_CAT_DESC = S.SCRCRD_ITEM_CAT_DESC,
    T.ACTV_IN = S.ACTV_IN,
    T.CRT_DTM = S.CRT_DTM,
    T.CRT_USER_ID = S.CRT_USER_ID,
    T.LAST_UPDT_DTM = S.LAST_UPDT_DTM,
    T.LAST_UPDT_USER_ID = S.LAST_UPDT_USER_ID
WHEN NOT MATCHED THEN
  INSERT (
    SCRCRD_ITEM_CAT_ID,
    SCRCRD_ITEM_CAT_DESC,
    ACTV_IN,
    CRT_DTM,
    CRT_USER_ID,
    LAST_UPDT_DTM,
    LAST_UPDT_USER_ID
  )
  VALUES (
    S.SCRCRD_ITEM_CAT_ID,
    S.SCRCRD_ITEM_CAT_DESC,
    S.ACTV_IN,
    S.CRT_DTM,
    S.CRT_USER_ID,
    S.LAST_UPDT_DTM,
    S.LAST_UPDT_USER_ID
  );
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)