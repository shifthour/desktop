# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Balkarn Gill               06/11/2013        5114                             Load DM Table PROV_DIR_DM_PROV_SPEC_CT                             EnterpriseWrhsDevl

# MAGIC Job Name: EdwDmProdDirProvSpecCtLoad
# MAGIC Read Load File created in the EdwDmProdDirDmProvSpecCtExtr Job
# MAGIC Copy Stage for buffer
# MAGIC Update then Insert the PROV_DIR_DM_PROV_SPEC_CT Data.
# MAGIC Load rejects are redirected into a Reject file
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


WebProvDirOwner = get_widget_value('WebProvDirOwner','')
webprovdir_secret_name = get_widget_value('webprovdir_secret_name','')
ProvDirRecordCount = get_widget_value('ProvDirRecordCount','')
ProvDirArraySize = get_widget_value('ProvDirArraySize','')

schema_seq_PROV_DIR_DM_PROV_SPEC_CT_csv_load = StructType([
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PROV_SPEC_CD", StringType(), False),
    StructField("NTWK_ID", StringType(), False),
    StructField("PROV_CT", IntegerType(), False)
])

df_seq_PROV_DIR_DM_PROV_SPEC_CT_csv_load = (
    spark.read
    .option("delimiter", ",")
    .option("quote", "^")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(schema_seq_PROV_DIR_DM_PROV_SPEC_CT_csv_load)
    .csv(f"{adls_path}/load/PROV_DIR_DM_PROV_SPEC_CT.dat")
)

df_cpy_forBuffer_out = df_seq_PROV_DIR_DM_PROV_SPEC_CT_csv_load.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PROV_SPEC_CD").alias("PROV_SPEC_CD"),
    F.col("NTWK_ID").alias("NTWK_ID"),
    F.col("PROV_CT").alias("PROV_CT")
)

jdbc_url, jdbc_props = get_db_config(webprovdir_secret_name)

df_cpy_forBuffer_out_rpadded = (
    df_cpy_forBuffer_out
    .withColumn("SRC_SYS_CD", F.rpad(F.col("SRC_SYS_CD"), <...>, " "))
    .withColumn("PROV_SPEC_CD", F.rpad(F.col("PROV_SPEC_CD"), <...>, " "))
    .withColumn("NTWK_ID", F.rpad(F.col("NTWK_ID"), <...>, " "))
)

df_cpy_forBuffer_out_final = df_cpy_forBuffer_out_rpadded.select(
    "SRC_SYS_CD",
    "PROV_SPEC_CD",
    "NTWK_ID",
    "PROV_CT"
)

execute_dml(
    "DROP TABLE IF EXISTS STAGING.EdwDmProvDirDmProvSpecCtLoad_ODBC_PROV_DIR_DM_SPEC_CT_out_temp",
    jdbc_url,
    jdbc_props
)

(
    df_cpy_forBuffer_out_final
    .write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", "STAGING.EdwDmProvDirDmProvSpecCtLoad_ODBC_PROV_DIR_DM_SPEC_CT_out_temp")
    .mode("errorifexists")
    .save()
)

merge_sql = """
MERGE INTO #$WebProvDirOwner#.PROV_DIR_DM_PROV_SPEC_CT AS T
USING STAGING.EdwDmProvDirDmProvSpecCtLoad_ODBC_PROV_DIR_DM_SPEC_CT_out_temp AS S
ON (T.SRC_SYS_CD = S.SRC_SYS_CD AND T.PROV_SPEC_CD = S.PROV_SPEC_CD AND T.NTWK_ID = S.NTWK_ID)
WHEN MATCHED THEN
  UPDATE SET
    T.PROV_CT = S.PROV_CT
WHEN NOT MATCHED THEN
  INSERT (SRC_SYS_CD, PROV_SPEC_CD, NTWK_ID, PROV_CT)
  VALUES (S.SRC_SYS_CD, S.PROV_SPEC_CD, S.NTWK_ID, S.PROV_CT);
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)

schema_reject = StructType([
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("PROV_SPEC_CD", StringType(), True),
    StructField("NTWK_ID", StringType(), True),
    StructField("PROV_CT", IntegerType(), True),
    StructField("ERRORCODE", StringType(), True),
    StructField("ERRORTEXT", StringType(), True)
])

df_ODBC_PROV_DIR_DM_SPEC_CT_out_rej = spark.createDataFrame([], schema_reject)

df_ODBC_PROV_DIR_DM_SPEC_CT_out_rej = (
    df_ODBC_PROV_DIR_DM_SPEC_CT_out_rej
    .withColumn("SRC_SYS_CD", F.rpad(F.col("SRC_SYS_CD"), <...>, " "))
    .withColumn("PROV_SPEC_CD", F.rpad(F.col("PROV_SPEC_CD"), <...>, " "))
    .withColumn("NTWK_ID", F.rpad(F.col("NTWK_ID"), <...>, " "))
    .withColumn("ERRORCODE", F.rpad(F.col("ERRORCODE"), <...>, " "))
    .withColumn("ERRORTEXT", F.rpad(F.col("ERRORTEXT"), <...>, " "))
)

df_ODBC_PROV_DIR_DM_SPEC_CT_out_rej_final = df_ODBC_PROV_DIR_DM_SPEC_CT_out_rej.select(
    "SRC_SYS_CD",
    "PROV_SPEC_CD",
    "NTWK_ID",
    "PROV_CT",
    "ERRORCODE",
    "ERRORTEXT"
)

write_files(
    df_ODBC_PROV_DIR_DM_SPEC_CT_out_rej_final,
    f"{adls_path}/load/PROV_DIR_DM_PROV_SPEC_CT_Rej.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)