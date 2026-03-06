# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Archana Palivela     07/31/2013          5114                             Original Programming                                                                             IntegrateWrhsDevl       Jag Yelavarthi            2013-11-30

# MAGIC Job Name: IdsDmWClmDelLoad
# MAGIC Read Load File created in the IdsDmClmMartDriverExtr Job
# MAGIC Copy Stage for buffer
# MAGIC Truncate then Insert the W_CLM_DEL
# MAGIC  Data.
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
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


ClmMartOwner = get_widget_value('ClmMartOwner','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')
ClmMartArraySize = get_widget_value('ClmMartArraySize','')
ClmMartRecordCount = get_widget_value('ClmMartRecordCount','')

schema_seq_W_CLM_DEL_csv_load = StructType([
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("CLM_ID", StringType(), False),
    StructField("SRC_SYS_CD_SK", IntegerType(), False)
])

df_seq_W_CLM_DEL_csv_load = (
    spark.read
    .option("delimiter", ",")
    .option("quote", "^")
    .option("header", "false")
    .option("nullValue", None)
    .schema(schema_seq_W_CLM_DEL_csv_load)
    .csv(f"{adls_path}/load/W_CLM_DEL.dat")
)

df_cpy_forBuffer = df_seq_W_CLM_DEL_csv_load.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK")
)

df_Odbc_W_CLM_DEL_out = df_cpy_forBuffer.select(
    F.rpad(F.col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    F.rpad(F.col("CLM_ID"), <...>, " ").alias("CLM_ID"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK")
)

jdbc_url_clmmart, jdbc_props_clmmart = get_db_config(clmmart_secret_name)

execute_dml("DROP TABLE IF EXISTS STAGING.IdsDmWClmDelLoad_Odbc_W_CLM_DEL_out_temp", jdbc_url_clmmart, jdbc_props_clmmart)

df_Odbc_W_CLM_DEL_out.write.format("jdbc") \
    .option("url", jdbc_url_clmmart) \
    .options(**jdbc_props_clmmart) \
    .option("dbtable", "STAGING.IdsDmWClmDelLoad_Odbc_W_CLM_DEL_out_temp") \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO {ClmMartOwner}.W_CLM_DEL AS T
USING STAGING.IdsDmWClmDelLoad_Odbc_W_CLM_DEL_out_temp AS S
ON T.SRC_SYS_CD = S.SRC_SYS_CD AND T.CLM_ID = S.CLM_ID
WHEN MATCHED THEN UPDATE SET
    T.SRC_SYS_CD_SK = S.SRC_SYS_CD_SK
WHEN NOT MATCHED THEN INSERT (SRC_SYS_CD, CLM_ID, SRC_SYS_CD_SK)
VALUES (S.SRC_SYS_CD, S.CLM_ID, S.SRC_SYS_CD_SK);
"""

execute_dml(merge_sql, jdbc_url_clmmart, jdbc_props_clmmart)

schema_reject = StructType([
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("CLM_ID", StringType(), True),
    StructField("SRC_SYS_CD_SK", IntegerType(), True),
    StructField("ERRORCODE", StringType(), True),
    StructField("ERRORTEXT", StringType(), True)
])

df_Odbc_W_CLM_DEL_out_reject = spark.createDataFrame([], schema_reject)

df_seq_W_CLM_DEL_csv_rej = df_Odbc_W_CLM_DEL_out_reject.select(
    F.rpad(F.col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    F.rpad(F.col("CLM_ID"), <...>, " ").alias("CLM_ID"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.rpad(F.col("ERRORCODE"), <...>, " ").alias("ERRORCODE"),
    F.rpad(F.col("ERRORTEXT"), <...>, " ").alias("ERRORTEXT")
)

write_files(
    df_seq_W_CLM_DEL_csv_rej,
    f"{adls_path}/load/W_CLM_DEL_Rej.dat",
    ",",
    "append",
    False,
    False,
    "^",
    None
)