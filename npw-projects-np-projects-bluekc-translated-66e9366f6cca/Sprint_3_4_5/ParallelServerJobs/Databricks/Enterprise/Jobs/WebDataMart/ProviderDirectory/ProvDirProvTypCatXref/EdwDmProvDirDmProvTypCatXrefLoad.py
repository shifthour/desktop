# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC MODIFICATIONS:                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        
# MAGIC                                                                                                                                                                                                                                                         
# MAGIC DEVELOPER                           DATE                PROJECT                                             DESCRIPTION                                                      DATASTAGE                                     CODE                                     DATE
# MAGIC                                                                                                                                                                                                                       ENVIRONMENT                                REVIEWER                            REVIEW
# MAGIC -------------------------------------------      -------------------      ---------------------------------------------                 -------------------------------------------------------------------           -------------------------------------------------          ------------------------------                  -------------------
# MAGIC Rajasekhar Mangalampally       06/17/2013     5114                                                       Original Programming                                                EnterpriseWrhsDevl                        Jag Yelavarthi                     2013-08-29
# MAGIC                                                                                                                                         (Server to Parallel Conversion)

# MAGIC Read the Load ready file created in EdwDmProvDirDmProvTypCatXrefExtr Job
# MAGIC Load rejects are redirected into a Reject file
# MAGIC 
# MAGIC This file is an append
# MAGIC Load file created in the previous job will be loaded into the target SQL Server PROV_DIR_DM_PROV_TYP_CAT_XREF Table here. 
# MAGIC 
# MAGIC Load Type; "Truncate then Insert" .
# MAGIC Copy For Buffer
# MAGIC Job Name: 
# MAGIC EdwDmProvDirDmProvTypCatXrefLoad
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import col, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


WebProvDirOwner = get_widget_value('WebProvDirOwner','')
webprovdir_secret_name = get_widget_value('webprovdir_secret_name','')
ProvDirArraySize = get_widget_value('ProvDirArraySize','')
ProvDirRecordCount = get_widget_value('ProvDirRecordCount','')

schema_seq_PROV_DIR_DM_PROV_TYP_CAT_XREF_csv_load = StructType([
    StructField("PROV_TYP_CD", StringType(), False),
    StructField("PROV_CAT_CD", StringType(), False),
    StructField("SEQ_NO", IntegerType(), False),
    StructField("WEB_SRCH_IN", StringType(), False),
    StructField("USER_ID", StringType(), False),
    StructField("LAST_UPDT_DT", TimestampType(), False)
])

df_seq_PROV_DIR_DM_PROV_TYP_CAT_XREF_csv_load = (
    spark.read.format("csv")
    .option("header", "false")
    .option("sep", ",")
    .option("quote", "^")
    .schema(schema_seq_PROV_DIR_DM_PROV_TYP_CAT_XREF_csv_load)
    .load(f"{adls_path}/load/PROV_DIR_DM_PROV_TYP_CAT_XREF.dat")
)

df_cpy_forBuffer = df_seq_PROV_DIR_DM_PROV_TYP_CAT_XREF_csv_load.select(
    col("PROV_TYP_CD"),
    col("PROV_CAT_CD"),
    col("SEQ_NO"),
    col("WEB_SRCH_IN"),
    col("USER_ID"),
    col("LAST_UPDT_DT")
)

df_cpy_forBuffer_for_write = df_cpy_forBuffer.select(
    rpad(col("PROV_TYP_CD"), <...>, " ").alias("PROV_TYP_CD"),
    rpad(col("PROV_CAT_CD"), <...>, " ").alias("PROV_CAT_CD"),
    col("SEQ_NO").alias("SEQ_NO"),
    rpad(col("WEB_SRCH_IN"), 1, " ").alias("WEB_SRCH_IN"),
    rpad(col("USER_ID"), <...>, " ").alias("USER_ID"),
    col("LAST_UPDT_DT").alias("LAST_UPDT_DT")
)

jdbc_url, jdbc_props = get_db_config(webprovdir_secret_name)

execute_dml(
    f"DROP TABLE IF EXISTS STAGING.EdwDmProvDirDmProvTypCatXrefLoad_odbc_PROV_DIR_DM_PROV_TYP_CAT_XREF_out_temp",
    jdbc_url,
    jdbc_props
)

df_cpy_forBuffer_for_write.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.EdwDmProvDirDmProvTypCatXrefLoad_odbc_PROV_DIR_DM_PROV_TYP_CAT_XREF_out_temp") \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO {WebProvDirOwner}.PROV_DIR_DM_PROV_TYP_CAT_XREF as T
USING STAGING.EdwDmProvDirDmProvTypCatXrefLoad_odbc_PROV_DIR_DM_PROV_TYP_CAT_XREF_out_temp as S
ON T.PROV_TYP_CD = S.PROV_TYP_CD
AND T.PROV_CAT_CD = S.PROV_CAT_CD
AND T.SEQ_NO = S.SEQ_NO
WHEN MATCHED THEN
  UPDATE SET
    T.WEB_SRCH_IN = S.WEB_SRCH_IN,
    T.USER_ID = S.USER_ID,
    T.LAST_UPDT_DT = S.LAST_UPDT_DT
WHEN NOT MATCHED THEN
  INSERT (PROV_TYP_CD, PROV_CAT_CD, SEQ_NO, WEB_SRCH_IN, USER_ID, LAST_UPDT_DT)
  VALUES (S.PROV_TYP_CD, S.PROV_CAT_CD, S.SEQ_NO, S.WEB_SRCH_IN, S.USER_ID, S.LAST_UPDT_DT)
;
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

df_odbc_PROV_DIR_DM_PROV_TYP_CAT_XREF_out_reject = spark.createDataFrame([], StructType([
    StructField("ERRORCODE", StringType(), True),
    StructField("ERRORTEXT", StringType(), True)
]))

df_odbc_PROV_DIR_DM_PROV_TYP_CAT_XREF_out_reject_rpad = df_odbc_PROV_DIR_DM_PROV_TYP_CAT_XREF_out_reject.select(
    rpad(col("ERRORCODE"), <...>, " ").alias("ERRORCODE"),
    rpad(col("ERRORTEXT"), <...>, " ").alias("ERRORTEXT")
)

write_files(
    df_odbc_PROV_DIR_DM_PROV_TYP_CAT_XREF_out_reject_rpad,
    f"{adls_path}/load/PROV_DIR_DM_PROV_TYP_CAT_XREF_Rej.dat",
    ",",
    "append",
    False,
    False,
    "^",
    None
)