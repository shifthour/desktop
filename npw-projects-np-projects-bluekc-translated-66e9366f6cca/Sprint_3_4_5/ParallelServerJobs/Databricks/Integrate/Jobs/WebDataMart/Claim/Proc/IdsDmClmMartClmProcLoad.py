# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                                          DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                                               ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------                        ------------------------------    ------------------------------       --------------------
# MAGIC Nagesh Bandi          09/18/2013          5114                             Movies Data from IDS to  CLM_DM_CLM_PROC                                                         IntegrateWrhsDevl

# MAGIC Job Name: IdsDmClmMartClmProcLoad
# MAGIC Read Load File created in the 
# MAGIC IdsDmClmMartClmProcExtr Job
# MAGIC Copy Stage for buffer
# MAGIC Update and  Insert the CLM_DM_CLM_PROC  Data.
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
from pyspark.sql.functions import col, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


ClmMartOwner = get_widget_value('ClmMartOwner','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')
ClmMartArraySize = get_widget_value('ClmMartArraySize','')
ClmMartRecordCount = get_widget_value('ClmMartRecordCount','')

schema_seq_CLM_DM_CLM_PROC_csv_load = StructType([
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("CLM_ID", StringType(), False),
    StructField("CLM_PROC_ORDNL_CD", StringType(), False),
    StructField("PROC_CD", StringType(), False),
    StructField("PROC_CAT_CD", StringType(), False),
    StructField("PROC_CD_DESC", StringType(), False),
    StructField("LAST_UPDT_RUN_CYC_NO", IntegerType(), False),
])

df_seq_CLM_DM_CLM_PROC_csv_load = (
    spark.read
    .option("sep", ",")
    .option("quote", "^")
    .option("header", "false")
    .option("nullValue", None)
    .schema(schema_seq_CLM_DM_CLM_PROC_csv_load)
    .csv(f"{adls_path}/load/CLM_DM_CLM_PROC.dat")
)

df_cpy_forBuffer = df_seq_CLM_DM_CLM_PROC_csv_load.select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("CLM_ID").alias("CLM_ID"),
    col("CLM_PROC_ORDNL_CD").alias("CLM_PROC_ORDNL_CD"),
    col("PROC_CD").alias("PROC_CD"),
    col("PROC_CAT_CD").alias("PROC_CAT_CD"),
    col("PROC_CD_DESC").alias("PROC_CD_DESC"),
    col("LAST_UPDT_RUN_CYC_NO").alias("LAST_UPDT_RUN_CYC_NO")
)

df_Odbc_CLM_DM_CLM_PROC_out = df_cpy_forBuffer.select(
    rpad(col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    rpad(col("CLM_ID"), <...>, " ").alias("CLM_ID"),
    rpad(col("CLM_PROC_ORDNL_CD"), <...>, " ").alias("CLM_PROC_ORDNL_CD"),
    rpad(col("PROC_CD"), <...>, " ").alias("PROC_CD"),
    rpad(col("PROC_CAT_CD"), <...>, " ").alias("PROC_CAT_CD"),
    rpad(col("PROC_CD_DESC"), <...>, " ").alias("PROC_CD_DESC"),
    col("LAST_UPDT_RUN_CYC_NO").alias("LAST_UPDT_RUN_CYC_NO")
)

jdbc_url, jdbc_props = get_db_config(clmmart_secret_name)

execute_dml("DROP TABLE IF EXISTS STAGING.IdsDmClmMartClmProcLoad_Odbc_CLM_DM_CLM_PROC_out_temp", jdbc_url, jdbc_props)

df_Odbc_CLM_DM_CLM_PROC_out.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.IdsDmClmMartClmProcLoad_Odbc_CLM_DM_CLM_PROC_out_temp") \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO {ClmMartOwner}.CLM_DM_CLM_PROC AS T
USING STAGING.IdsDmClmMartClmProcLoad_Odbc_CLM_DM_CLM_PROC_out_temp AS S
ON T.SRC_SYS_CD = S.SRC_SYS_CD
   AND T.CLM_ID = S.CLM_ID
   AND T.CLM_PROC_ORDNL_CD = S.CLM_PROC_ORDNL_CD
WHEN MATCHED THEN UPDATE SET
  T.PROC_CD = S.PROC_CD,
  T.PROC_CAT_CD = S.PROC_CAT_CD,
  T.PROC_CD_DESC = S.PROC_CD_DESC,
  T.LAST_UPDT_RUN_CYC_NO = S.LAST_UPDT_RUN_CYC_NO
WHEN NOT MATCHED THEN
  INSERT (SRC_SYS_CD, CLM_ID, CLM_PROC_ORDNL_CD, PROC_CD, PROC_CAT_CD, PROC_CD_DESC, LAST_UPDT_RUN_CYC_NO)
  VALUES (S.SRC_SYS_CD, S.CLM_ID, S.CLM_PROC_ORDNL_CD, S.PROC_CD, S.PROC_CAT_CD, S.PROC_CD_DESC, S.LAST_UPDT_RUN_CYC_NO);
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)

reject_schema = StructType([
    StructField("ERRORCODE", StringType(), True),
    StructField("ERRORTEXT", StringType(), True)
])

df_seq_CLM_DM_CLM_PROC_csv_rej = spark.createDataFrame([], reject_schema)

df_seq_CLM_DM_CLM_PROC_csv_rej_final = df_seq_CLM_DM_CLM_PROC_csv_rej.select(
    rpad(col("ERRORCODE"), <...>, " ").alias("ERRORCODE"),
    rpad(col("ERRORTEXT"), <...>, " ").alias("ERRORTEXT")
)

write_files(
    df_seq_CLM_DM_CLM_PROC_csv_rej_final,
    f"{adls_path}/load/CLM_DM_CLM_PROC_Rej.dat",
    ",",
    "append",
    False,
    False,
    "^",
    None
)