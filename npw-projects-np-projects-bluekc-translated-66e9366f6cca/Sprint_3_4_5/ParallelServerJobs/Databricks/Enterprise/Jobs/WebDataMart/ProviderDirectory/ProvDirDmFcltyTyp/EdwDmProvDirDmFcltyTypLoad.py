# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Balkarn Gill               06/12/2013        5114                             Load DM Table PROV_DIR_DM_FCLTY_TYP                                EnterpriseWrhsDevl

# MAGIC Job Name: EdwDmProvDirDmFcltyTypLoad
# MAGIC Read Load File created in the EdwDmProvDirDmFcltyTypExtr Job
# MAGIC Copy Stage for buffer
# MAGIC Update then Insert the PROV_DIR_DM_FCLTY_TYP Data.
# MAGIC Load rejects are redirected into a Reject file
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import rpad, lit, col
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


# Parameter retrieval
WebProvDirOwner = get_widget_value('WebProvDirOwner','')
webprovdir_secret_name = get_widget_value('webprovdir_secret_name','')
ProvDirRecordCount = get_widget_value('ProvDirRecordCount','')
ProvDirArraySize = get_widget_value('ProvDirArraySize','')

# --------------------------------------------------------------------------------
# seq_PROV_DIR_DM_FCLTY_TYP_csv_load (PxSequentialFile) - READ
# --------------------------------------------------------------------------------
schema_seq_PROV_DIR_DM_FCLTY_TYP_csv_load = StructType([
    StructField("FCLTY_TYP_CD", StringType(), False),
    StructField("FCLTY_TYP_NM", StringType(), False),
    StructField("LAST_UPDT_DT", TimestampType(), False)
])

df_seq_PROV_DIR_DM_FCLTY_TYP_csv_load = (
    spark.read.format("csv")
    .schema(schema_seq_PROV_DIR_DM_FCLTY_TYP_csv_load)
    .option("delimiter", ",")
    .option("quote", "^")
    .option("header", "false")
    .option("nullValue", None)
    .load(f"{adls_path}/load/PROV_DIR_DM_FCLTY_TYP.dat")
)

# --------------------------------------------------------------------------------
# cpy_forBuffer (PxCopy)
# --------------------------------------------------------------------------------
df_cpy_forBuffer = df_seq_PROV_DIR_DM_FCLTY_TYP_csv_load.select(
    col("FCLTY_TYP_CD"),
    col("FCLTY_TYP_NM"),
    col("LAST_UPDT_DT")
)

# --------------------------------------------------------------------------------
# ODBC_PROV_DIR_DM_FCLTY_TYP_out (ODBCConnectorPX) - MERGE logic
# --------------------------------------------------------------------------------
df_ODBC_PROV_DIR_DM_FCLTY_TYP_out = df_cpy_forBuffer.select(
    col("FCLTY_TYP_CD"),
    col("FCLTY_TYP_NM"),
    col("LAST_UPDT_DT")
)

jdbc_url, jdbc_props = get_db_config(webprovdir_secret_name)

execute_dml(
    "DROP TABLE IF EXISTS STAGING.EdwDmProvDirDmFcltyTypLoad_ODBC_PROV_DIR_DM_FCLTY_TYP_out_temp",
    jdbc_url,
    jdbc_props
)

(
    df_ODBC_PROV_DIR_DM_FCLTY_TYP_out.write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", "STAGING.EdwDmProvDirDmFcltyTypLoad_ODBC_PROV_DIR_DM_FCLTY_TYP_out_temp")
    .mode("overwrite")
    .save()
)

merge_sql = f"""
MERGE {WebProvDirOwner}.PROV_DIR_DM_FCLTY_TYP AS T
USING STAGING.EdwDmProvDirDmFcltyTypLoad_ODBC_PROV_DIR_DM_FCLTY_TYP_out_temp AS S
ON T.FCLTY_TYP_CD = S.FCLTY_TYP_CD
WHEN MATCHED THEN
  UPDATE SET
    T.FCLTY_TYP_NM = S.FCLTY_TYP_NM,
    T.LAST_UPDT_DT = S.LAST_UPDT_DT
WHEN NOT MATCHED THEN
  INSERT
  (
    FCLTY_TYP_CD,
    FCLTY_TYP_NM,
    LAST_UPDT_DT
  )
  VALUES
  (
    S.FCLTY_TYP_CD,
    S.FCLTY_TYP_NM,
    S.LAST_UPDT_DT
  );
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

# --------------------------------------------------------------------------------
# seq_PROV_DIR_DM_FCLTY_TYP_csv_rej (PxSequentialFile) - WRITE REJECTS
# --------------------------------------------------------------------------------
# In DataStage, the reject link captures rows that fail to merge/update, carrying
# the original columns plus ERRORCODE, ERRORTEXT. We simulate it here.
df_seq_PROV_DIR_DM_FCLTY_TYP_csv_rej = df_cpy_forBuffer.select(
    col("FCLTY_TYP_CD"),
    col("FCLTY_TYP_NM"),
    col("LAST_UPDT_DT"),
    lit(None).cast(StringType()).alias("ERRORCODE"),
    lit(None).cast(StringType()).alias("ERRORTEXT")
)

df_seq_PROV_DIR_DM_FCLTY_TYP_csv_rej_final = df_seq_PROV_DIR_DM_FCLTY_TYP_csv_rej.select(
    rpad("FCLTY_TYP_CD", <...>, " ").alias("FCLTY_TYP_CD"),
    rpad("FCLTY_TYP_NM", <...>, " ").alias("FCLTY_TYP_NM"),
    col("LAST_UPDT_DT"),
    rpad("ERRORCODE", <...>, " ").alias("ERRORCODE"),
    rpad("ERRORTEXT", <...>, " ").alias("ERRORTEXT")
)

write_files(
    df_seq_PROV_DIR_DM_FCLTY_TYP_csv_rej_final,
    f"{adls_path}/load/PROV_DIR_DM_FCLTY_TYP_Rej.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)