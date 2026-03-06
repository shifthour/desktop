# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE                  CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT             REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------           ------------------------------       --------------------
# MAGIC Balkarn Gill               06/12/2013        5114                             Load DM Table PROV_DIR_DM_PROV_BRD_CRTF                                EnterpriseWrhsDevl      Peter Marshall             9/3/2013

# MAGIC Job Name: EdwDmProvDirDmProvBrdCrtfLoad
# MAGIC Read Load File created in the EdwDmProvDirDmProvBrdCrtfExtr Job
# MAGIC Copy Stage for buffer
# MAGIC Update then Insert the PROV_DIR_DM_PROV_BRD_CRTF Data.
# MAGIC Load rejects are redirected into a Reject file
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


WebProvDirOwner = get_widget_value("WebProvDirOwner","")
webprovdir_secret_name = get_widget_value("webprovdir_secret_name","")
ProvDirRecordCount = get_widget_value("ProvDirRecordCount","")
ProvDirArraySize = get_widget_value("ProvDirArraySize","")

schema_seq_PROV_DIR_DM_PROV_BRD_CRTF_csv_load = StructType([
    StructField("PROV_ID", StringType(), True),
    StructField("BRD_CRTF_SEQ_NO", IntegerType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("BRD_CRTF_DIR_DESC", StringType(), True),
    StructField("LAST_UPDT_DT", TimestampType(), True)
])

df_seq_PROV_DIR_DM_PROV_BRD_CRTF_csv_load = (
    spark.read.format("csv")
    .option("header", "false")
    .option("sep", ",")
    .option("quote", "^")
    .option("escape", "^")
    .schema(schema_seq_PROV_DIR_DM_PROV_BRD_CRTF_csv_load)
    .load(f"{adls_path}/load/PROV_DIR_DM_PROV_BRD_CRTF.dat")
)

df_cpy_forBuffer = df_seq_PROV_DIR_DM_PROV_BRD_CRTF_csv_load.select(
    F.col("PROV_ID").alias("PROV_ID"),
    F.col("BRD_CRTF_SEQ_NO").alias("BRD_CRTF_SEQ_NO"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("BRD_CRTF_DIR_DESC").alias("BRD_CRTF_DIR_DESC"),
    F.col("LAST_UPDT_DT").alias("LAST_UPDT_DT")
)

jdbc_url, jdbc_props = get_db_config(webprovdir_secret_name)
execute_dml("DROP TABLE IF EXISTS STAGING.EdwDmProvDirDmProvBrdCrtfLoad_ODBC_PROV_DIR_DM_PROV_BRD_CRTF_out_temp", jdbc_url, jdbc_props)

(
    df_cpy_forBuffer.write.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", "STAGING.EdwDmProvDirDmProvBrdCrtfLoad_ODBC_PROV_DIR_DM_PROV_BRD_CRTF_out_temp")
    .mode("overwrite")
    .save()
)

merge_sql = f"""
MERGE INTO {WebProvDirOwner}.PROV_DIR_DM_PROV_BRD_CRTF AS T
USING STAGING.EdwDmProvDirDmProvBrdCrtfLoad_ODBC_PROV_DIR_DM_PROV_BRD_CRTF_out_temp AS S
ON (T.PROV_ID = S.PROV_ID AND T.BRD_CRTF_SEQ_NO = S.BRD_CRTF_SEQ_NO AND T.SRC_SYS_CD = S.SRC_SYS_CD)
WHEN MATCHED THEN
  UPDATE SET
    T.BRD_CRTF_DIR_DESC = S.BRD_CRTF_DIR_DESC,
    T.LAST_UPDT_DT = S.LAST_UPDT_DT
WHEN NOT MATCHED THEN
  INSERT(PROV_ID, BRD_CRTF_SEQ_NO, SRC_SYS_CD, BRD_CRTF_DIR_DESC, LAST_UPDT_DT)
  VALUES(S.PROV_ID, S.BRD_CRTF_SEQ_NO, S.SRC_SYS_CD, S.BRD_CRTF_DIR_DESC, S.LAST_UPDT_DT)
;
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)

schema_odbc_rej = StructType([
    StructField("PROV_ID", StringType(), True),
    StructField("BRD_CRTF_SEQ_NO", IntegerType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("BRD_CRTF_DIR_DESC", StringType(), True),
    StructField("LAST_UPDT_DT", TimestampType(), True),
    StructField("ERRORCODE", StringType(), True),
    StructField("ERRORTEXT", StringType(), True)
])

df_odbc_rej = spark.createDataFrame([], schema_odbc_rej)

df_odbc_rej_final = df_odbc_rej.select(
    F.rpad(F.col("PROV_ID"), F.lit("<...>"), F.lit(" ")).alias("PROV_ID"),
    F.col("BRD_CRTF_SEQ_NO").alias("BRD_CRTF_SEQ_NO"),
    F.rpad(F.col("SRC_SYS_CD"), F.lit("<...>"), F.lit(" ")).alias("SRC_SYS_CD"),
    F.rpad(F.col("BRD_CRTF_DIR_DESC"), F.lit("<...>"), F.lit(" ")).alias("BRD_CRTF_DIR_DESC"),
    F.col("LAST_UPDT_DT").alias("LAST_UPDT_DT"),
    F.rpad(F.col("ERRORCODE"), F.lit("<...>"), F.lit(" ")).alias("ERRORCODE"),
    F.rpad(F.col("ERRORTEXT"), F.lit("<...>"), F.lit(" ")).alias("ERRORTEXT")
)

write_files(
    df_odbc_rej_final,
    f"{adls_path}/load/PROV_DIR_DM_PROV_BRD_CRTF_Rej.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)