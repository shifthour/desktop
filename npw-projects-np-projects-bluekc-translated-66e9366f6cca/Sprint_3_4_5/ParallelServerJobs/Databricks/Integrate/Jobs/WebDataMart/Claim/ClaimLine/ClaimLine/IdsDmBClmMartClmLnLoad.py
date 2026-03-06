# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Archana Palivela     09/15/2013          5114                             Original Programming                                                                             IntegrateWrhsDevl       Jag Yelavarthi            2013-12-11

# MAGIC Job Name: IdsDmBClmMartClmLnLoad
# MAGIC Read Load File created in the IdsDmClmDmClmDiagExtr Job
# MAGIC Copy Stage for buffer
# MAGIC Update then Insert the W_BAL_DM_CLM_LN Data.
# MAGIC Load rejects are redirected into a Reject file
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import pyspark.sql.functions as F
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


ClmMartOwner = get_widget_value('ClmMartOwner','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')
ClmMartArraySize = get_widget_value('ClmMartArraySize','')
ClmMartRecordCount = get_widget_value('ClmMartRecordCount','')

schema_seq_W_BAL_DM_CLM_LN_csv_load = StructType([
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("CLM_ID", StringType(), nullable=False),
    StructField("CLM_LN_SEQ_NO", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False)
])
df_seq_W_BAL_DM_CLM_LN_csv_load = (
    spark.read
    .option("delimiter", ",")
    .option("header", "false")
    .option("quote", "^")
    .schema(schema_seq_W_BAL_DM_CLM_LN_csv_load)
    .csv(f"{adls_path}/load/W_BAL_DM_CLM_LN.dat")
)

df_cpy_forBuffer = df_seq_W_BAL_DM_CLM_LN_csv_load.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

df_Odbc_W_BAL_DM_CLM_LN_out = df_cpy_forBuffer.select(
    F.rpad(F.col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    F.rpad(F.col("CLM_ID"), <...>, " ").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

jdbc_url, jdbc_props = get_db_config(clmmart_secret_name)

spark.sql("DROP TABLE IF EXISTS STAGING.IdsDmBClmMartClmLnLoad_Odbc_W_BAL_DM_CLM_LN_out_temp")
(
    df_Odbc_W_BAL_DM_CLM_LN_out
    .write
    .jdbc(
        url=jdbc_url,
        table="STAGING.IdsDmBClmMartClmLnLoad_Odbc_W_BAL_DM_CLM_LN_out_temp",
        mode="overwrite",
        properties=jdbc_props
    )
)

merge_sql = f"""
MERGE INTO {ClmMartOwner}.W_BAL_DM_CLM_LN AS tgt
USING STAGING.IdsDmBClmMartClmLnLoad_Odbc_W_BAL_DM_CLM_LN_out_temp AS src
ON
    tgt.SRC_SYS_CD = src.SRC_SYS_CD
    AND tgt.CLM_ID = src.CLM_ID
    AND tgt.CLM_LN_SEQ_NO = src.CLM_LN_SEQ_NO
WHEN MATCHED THEN
    UPDATE SET 
        tgt.LAST_UPDT_RUN_CYC_EXCTN_SK = src.LAST_UPDT_RUN_CYC_EXCTN_SK
WHEN NOT MATCHED THEN
    INSERT (SRC_SYS_CD, CLM_ID, CLM_LN_SEQ_NO, LAST_UPDT_RUN_CYC_EXCTN_SK)
    VALUES (src.SRC_SYS_CD, src.CLM_ID, src.CLM_LN_SEQ_NO, src.LAST_UPDT_RUN_CYC_EXCTN_SK);
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)

df_Odbc_W_BAL_DM_CLM_LN_out_reject = spark.createDataFrame([], StructType([
    StructField("ERRORCODE", StringType(), True),
    StructField("ERRORTEXT", StringType(), True)
]))

df_Odbc_W_BAL_DM_CLM_LN_out_reject = df_Odbc_W_BAL_DM_CLM_LN_out_reject.select(
    F.rpad(F.col("ERRORCODE"), <...>, " ").alias("ERRORCODE"),
    F.rpad(F.col("ERRORTEXT"), <...>, " ").alias("ERRORTEXT")
)

write_files(
    df_Odbc_W_BAL_DM_CLM_LN_out_reject,
    f"{adls_path}/load/W_BAL_DM_CLM_LN_Rej.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)