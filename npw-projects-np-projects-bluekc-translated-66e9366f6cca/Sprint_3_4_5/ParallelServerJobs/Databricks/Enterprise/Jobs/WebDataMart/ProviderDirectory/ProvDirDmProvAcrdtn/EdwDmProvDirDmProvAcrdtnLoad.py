# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC DEVELOPER        DATE                PROJECT                       DESCRIPTION                                                                DATASTAGE  ENVIRONMENT         CODE REVIEWER                 REVIEW  DATE
# MAGIC -------------------------     ------------------    ---------------------------------              ---------------------------------                                                    -----------------------------------------------             ------------------------------                 -------------------
# MAGIC Shiva Devagiri    06/18/2013       5114                             Load DMTable PROV_DIR_DM_PROV_ACRDTN                      EnterpriseWrhsDevl                          Jag Yelavarthi                 2013-08-30

# MAGIC Load file created in the previous job will be loaded into the target table here. 
# MAGIC 
# MAGIC Load Type; "Update then Insert"
# MAGIC Read Load File created in the EdwProvDirDmProvAcrdtnExtr Job
# MAGIC Load rejects are redirected into a Reject file
# MAGIC Copy For Buffer
# MAGIC Job Name: 
# MAGIC EdwProvDirDmProvAcrdtnLoad
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


WebProvDirOwner = get_widget_value('WebProvDirOwner', '')
webprovdir_secret_name = get_widget_value('webprovdir_secret_name', '')
ProvDirArraySize = get_widget_value('ProvDirArraySize', '')
ProvDirRecordCount = get_widget_value('ProvDirRecordCount', '')

schema_seq_PROV_DIR_DM_PROV_ACRDTN_csv_load = StructType([
    StructField("PROV_ID", StringType(), nullable=False),
    StructField("ACRDTN_SEQ_NO", IntegerType(), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("ACRDTN_DESC", StringType(), nullable=False),
    StructField("LAST_UPDT_DT", TimestampType(), nullable=False)
])

df_seq_PROV_DIR_DM_PROV_ACRDTN_csv_load = (
    spark.read
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "^")
    .option("nullValue", None)
    .schema(schema_seq_PROV_DIR_DM_PROV_ACRDTN_csv_load)
    .csv(f"{adls_path}/load/PROV_DIR_DM_PROV_ACRDTN.dat")
)

df_cpy_forBuffer = df_seq_PROV_DIR_DM_PROV_ACRDTN_csv_load.select(
    col("PROV_ID").alias("PROV_ID"),
    col("ACRDTN_SEQ_NO").alias("ACRDTN_SEQ_NO"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("ACRDTN_DESC").alias("ACRDTN_DESC"),
    col("LAST_UPDT_DT").alias("LAST_UPDT_DT")
)

df_odbc_PROV_DIR_DM_PROV_ACRDTN_out = df_cpy_forBuffer.select(
    rpad(col("PROV_ID"), <...>, " ").alias("PROV_ID"),
    col("ACRDTN_SEQ_NO").alias("ACRDTN_SEQ_NO"),
    rpad(col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    rpad(col("ACRDTN_DESC"), <...>, " ").alias("ACRDTN_DESC"),
    col("LAST_UPDT_DT").alias("LAST_UPDT_DT")
)

jdbc_url, jdbc_props = get_db_config(webprovdir_secret_name)

execute_dml(
    "DROP TABLE IF EXISTS STAGING.EdwDmProvDirDmProvAcrdtnLoad_odbc_PROV_DIR_DM_PROV_ACRDTN_out_temp",
    jdbc_url,
    jdbc_props
)

(
    df_odbc_PROV_DIR_DM_PROV_ACRDTN_out
    .write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", "STAGING.EdwDmProvDirDmProvAcrdtnLoad_odbc_PROV_DIR_DM_PROV_ACRDTN_out_temp")
    .mode("append")
    .save()
)

merge_sql = f"""
MERGE {WebProvDirOwner}.PROV_DIR_DM_PROV_ACRDTN AS T
USING STAGING.EdwDmProvDirDmProvAcrdtnLoad_odbc_PROV_DIR_DM_PROV_ACRDTN_out_temp AS S
ON
    T.PROV_ID = S.PROV_ID
    AND T.ACRDTN_SEQ_NO = S.ACRDTN_SEQ_NO
    AND T.SRC_SYS_CD = S.SRC_SYS_CD
WHEN MATCHED THEN
    UPDATE SET
        T.PROV_ID = S.PROV_ID,
        T.ACRDTN_SEQ_NO = S.ACRDTN_SEQ_NO,
        T.SRC_SYS_CD = S.SRC_SYS_CD,
        T.ACRDTN_DESC = S.ACRDTN_DESC,
        T.LAST_UPDT_DT = S.LAST_UPDT_DT
WHEN NOT MATCHED THEN
    INSERT (PROV_ID, ACRDTN_SEQ_NO, SRC_SYS_CD, ACRDTN_DESC, LAST_UPDT_DT)
    VALUES (S.PROV_ID, S.ACRDTN_SEQ_NO, S.SRC_SYS_CD, S.ACRDTN_DESC, S.LAST_UPDT_DT);
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

schema_odbc_PROV_DIR_DM_PROV_ACRDTN_out_rej = StructType([
    StructField("ERRORCODE", StringType(), True),
    StructField("ERRORTEXT", StringType(), True)
])
df_odbc_PROV_DIR_DM_PROV_ACRDTN_out_rej = spark.createDataFrame([], schema_odbc_PROV_DIR_DM_PROV_ACRDTN_out_rej)

df_odbc_PROV_DIR_DM_PROV_ACRDTN_out_rej = df_odbc_PROV_DIR_DM_PROV_ACRDTN_out_rej.select(
    rpad(col("ERRORCODE"), <...>, " ").alias("ERRORCODE"),
    rpad(col("ERRORTEXT"), <...>, " ").alias("ERRORTEXT")
)

write_files(
    df_odbc_PROV_DIR_DM_PROV_ACRDTN_out_rej,
    f"{adls_path}/load/PROV_DIR_DM_WEB_PROV_ACRDTN_Rej.dat",
    ",",
    "append",
    False,
    False,
    "^",
    None
)