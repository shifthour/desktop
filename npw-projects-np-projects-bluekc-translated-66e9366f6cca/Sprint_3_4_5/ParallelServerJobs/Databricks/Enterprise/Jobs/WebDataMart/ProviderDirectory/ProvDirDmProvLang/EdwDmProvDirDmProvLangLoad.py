# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC DEVELOPER                         DATE                PROJECT                                             DESCRIPTION                                                          DATASTAGE  ENVIRONMENT         CODE REVIEWER                 REVIEW  DATE
# MAGIC -----------------------------------------      ------------------        -----------------------------------------------              -------------------------------------------------------------------------         -----------------------------------------------             ------------------------------                 -------------------
# MAGIC Shiva Devagiri                                06/26/2013               5114                                   Original Programming                                                     EnterpriseWrhsDevl                           Jag Yelavarthi                         2013-09-01
# MAGIC                                                                                                                                          (Server to Parallel Conversion)

# MAGIC Load file created in the previous job will be loaded into the target table here. 
# MAGIC 
# MAGIC Load Type; "Update then Insert"
# MAGIC Read Load File created in the EdwProvDirDmProvLangExtr Job
# MAGIC Load rejects are redirected into a Reject file
# MAGIC Copy For Buffer
# MAGIC Job Name: 
# MAGIC EdwProvDirDmProvLangLoad
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import col, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


WebProvDirOwner = get_widget_value('WebProvDirOwner','')
webprovdir_secret_name = get_widget_value('webprovdir_secret_name','')
ProvDirArraySize = get_widget_value('ProvDirArraySize','')
ProvDirRecordCount = get_widget_value('ProvDirRecordCount','')

schema_seq_PROV_DIR_DM_PROV_LANG_csv_load = StructType([
    StructField("PROV_ID", StringType(), False),
    StructField("LANG_SEQ_NO", IntegerType(), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("LANG_DESC", StringType(), False),
    StructField("LAST_UPDT_DT", TimestampType(), False)
])

df_seq_PROV_DIR_DM_PROV_LANG_csv_load = (
    spark.read
    .schema(schema_seq_PROV_DIR_DM_PROV_LANG_csv_load)
    .option("delimiter", ",")
    .option("quote", "^")
    .option("header", "false")
    .csv(f"{adls_path}/load/PROV_DIR_DM_PROV_LANG.dat")
)

df_cpy_forBuffer = df_seq_PROV_DIR_DM_PROV_LANG_csv_load.select(
    col("PROV_ID").alias("PROV_ID"),
    col("LANG_SEQ_NO").alias("LANG_SEQ_NO"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("LANG_DESC").alias("LANG_DESC"),
    col("LAST_UPDT_DT").alias("LAST_UPDT_DT")
)

jdbc_url, jdbc_props = get_db_config(webprovdir_secret_name)

spark.sql(f"DROP TABLE IF EXISTS STAGING.EdwDmProvDirDmProvLangLoad_odbc_PROV_DIR_DM_PROV_LANG_out_temp")

df_cpy_forBuffer.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.EdwDmProvDirDmProvLangLoad_odbc_PROV_DIR_DM_PROV_LANG_out_temp") \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO {WebProvDirOwner}.PROV_DIR_DM_PROV_LANG AS T
USING STAGING.EdwDmProvDirDmProvLangLoad_odbc_PROV_DIR_DM_PROV_LANG_out_temp AS S
ON T.PROV_ID = S.PROV_ID
AND T.LANG_SEQ_NO = S.LANG_SEQ_NO
AND T.SRC_SYS_CD = S.SRC_SYS_CD
WHEN MATCHED THEN
UPDATE SET
  T.PROV_ID = S.PROV_ID,
  T.LANG_SEQ_NO = S.LANG_SEQ_NO,
  T.SRC_SYS_CD = S.SRC_SYS_CD,
  T.LANG_DESC = S.LANG_DESC,
  T.LAST_UPDT_DT = S.LAST_UPDT_DT
WHEN NOT MATCHED THEN
INSERT (PROV_ID, LANG_SEQ_NO, SRC_SYS_CD, LANG_DESC, LAST_UPDT_DT)
VALUES (S.PROV_ID, S.LANG_SEQ_NO, S.SRC_SYS_CD, S.LANG_DESC, S.LAST_UPDT_DT);
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)

df_odbc_PROV_DIR_DM_PROV_LANG_out_reject = spark.createDataFrame(
    [],
    StructType([
        StructField("ERRORCODE", StringType(), True),
        StructField("ERRORTEXT", StringType(), True)
    ])
)

df_odbc_PROV_DIR_DM_PROV_LANG_out_reject = df_odbc_PROV_DIR_DM_PROV_LANG_out_reject.select(
    rpad(col("ERRORCODE"), <...>, " ").alias("ERRORCODE"),
    rpad(col("ERRORTEXT"), <...>, " ").alias("ERRORTEXT")
)

write_files(
    df_odbc_PROV_DIR_DM_PROV_LANG_out_reject,
    f"{adls_path}/load/PROV_DIR_DM_PROV_LANG_Rej.dat",
    ",",
    "append",
    False,
    False,
    "^",
    "\"\""
)