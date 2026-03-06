# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC DEVELOPER                         DATE                PROJECT                                             DESCRIPTION                                                          DATASTAGE  ENVIRONMENT         CODE REVIEWER                 REVIEW  DATE
# MAGIC -----------------------------------------      ------------------        -----------------------------------------------              -------------------------------------------------------------------------         -----------------------------------------------             ------------------------------                 -------------------
# MAGIC Shiva Devagiri                             06/20/2013       5114                                           Original Programming                                                     EnterpriseWrhsDevl                            Jag Yelavarthi                          2013-08-30
# MAGIC                                                                                                                                         (Server to Parallel Conversion)

# MAGIC Load file created in the previous job will be loaded into the target table here. 
# MAGIC 
# MAGIC Load Type; "Update then Insert"
# MAGIC Read Load File created in the EdwProvDirDmProvTypExtr Job
# MAGIC Load rejects are redirected into a Reject file
# MAGIC Copy For Buffer
# MAGIC Job Name: 
# MAGIC EdwProvDirDmProvTypLoad
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


WebProvDirOwner = get_widget_value('WebProvDirOwner','')
webprovdir_secret_name = get_widget_value('webprovdir_secret_name','')
ProvDirArraySize = get_widget_value('ProvDirArraySize','')
ProvDirRecordCount = get_widget_value('ProvDirRecordCount','')

schema_seq_PROV_DIR_DM_PROV_TYP_csv_load = StructType([
    StructField("PROV_TYP_CD", StringType(), False),
    StructField("PROV_TYP_NM", StringType(), False),
    StructField("LAST_UPDT_DT", TimestampType(), False)
])

df_seq_PROV_DIR_DM_PROV_TYP_csv_load = (
    spark.read
    .option("header", False)
    .option("sep", ",")
    .option("quote", "^")
    .schema(schema_seq_PROV_DIR_DM_PROV_TYP_csv_load)
    .csv(f"{adls_path}/load/PROV_DIR_DM_PROV_TYP.dat")
)

df_cpy_forBuffer = df_seq_PROV_DIR_DM_PROV_TYP_csv_load.select(
    F.col("PROV_TYP_CD").alias("PROV_TYP_CD"),
    F.col("PROV_TYP_NM").alias("PROV_TYP_NM"),
    F.col("LAST_UPDT_DT").alias("LAST_UPDT_DT")
)

jdbc_url, jdbc_props = get_db_config(webprovdir_secret_name)

df_odbc_PROV_DIR_DM_PROV_TYP_out_in = df_cpy_forBuffer

drop_table_sql = "DROP TABLE IF EXISTS STAGING.EdwDmProvDirDmProvTypLoad_odbc_PROV_DIR_DM_PROV_TYP_out_temp"
execute_dml(drop_table_sql, jdbc_url, jdbc_props)

(
    df_odbc_PROV_DIR_DM_PROV_TYP_out_in
    .write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", "STAGING.EdwDmProvDirDmProvTypLoad_odbc_PROV_DIR_DM_PROV_TYP_out_temp")
    .mode("overwrite")
    .save()
)

merge_sql = f"""MERGE {WebProvDirOwner}.PROV_DIR_DM_PROV_TYP as T
USING STAGING.EdwDmProvDirDmProvTypLoad_odbc_PROV_DIR_DM_PROV_TYP_out_temp as S
ON (T.PROV_TYP_CD = S.PROV_TYP_CD)
WHEN MATCHED THEN
 UPDATE SET
  T.PROV_TYP_CD = S.PROV_TYP_CD,
  T.PROV_TYP_NM = S.PROV_TYP_NM,
  T.LAST_UPDT_DT = S.LAST_UPDT_DT
WHEN NOT MATCHED THEN
 INSERT (PROV_TYP_CD, PROV_TYP_NM, LAST_UPDT_DT)
 VALUES (S.PROV_TYP_CD, S.PROV_TYP_NM, S.LAST_UPDT_DT);
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

df_seq_PROV_DIR_DM_PROV_TYP_csv_rej_in = spark.createDataFrame(
    [],
    StructType([
        StructField("PROV_TYP_CD", StringType(), True),
        StructField("PROV_TYP_NM", StringType(), True),
        StructField("LAST_UPDT_DT", TimestampType(), True),
        StructField("ERRORCODE", StringType(), True),
        StructField("ERRORTEXT", StringType(), True)
    ])
)

df_seq_PROV_DIR_DM_PROV_TYP_csv_rej_final = df_seq_PROV_DIR_DM_PROV_TYP_csv_rej_in.select(
    F.rpad(F.col("PROV_TYP_CD"), <...>, " ").alias("PROV_TYP_CD"),
    F.rpad(F.col("PROV_TYP_NM"), <...>, " ").alias("PROV_TYP_NM"),
    F.col("LAST_UPDT_DT").alias("LAST_UPDT_DT"),
    F.rpad(F.col("ERRORCODE"), <...>, " ").alias("ERRORCODE"),
    F.rpad(F.col("ERRORTEXT"), <...>, " ").alias("ERRORTEXT")
)

write_files(
    df_seq_PROV_DIR_DM_PROV_TYP_csv_rej_final,
    f"{adls_path}/load/PROV_DIR_DM_PROV_TYP_Rej.dat",
    ",",
    "append",
    False,
    False,
    "^",
    None
)