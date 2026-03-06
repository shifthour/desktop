# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC 
# MAGIC Archana Palivela     09/15/2013          5114                             Original Programming                                                                             IntegrateWrhsDevl      Jag Yelavarthi             2013-11-30 
# MAGIC 
# MAGIC Jag Yelavarthi          2015-04-08         5345 - Daptiv#253         Added Sorting in-addition to Partitioning in Copy stage.                           IntegrateNewDevl      Kalyan Neelam            2015-04-14
# MAGIC                                                                                                   Sort will let the job run without hanging in a multi-configuration
# MAGIC                                                                                                   environment.

# MAGIC Job Name: IdsDmClmDmClmDiagLoad
# MAGIC Read Load File created in the IdsDmClmDmClmDiagExtr Job
# MAGIC Copy Stage for buffer
# MAGIC Update then Insert the CLM_DM_CLM_DIAG Data.
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
from pyspark.sql.functions import col, rpad, lit
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------

ClmMartOwner = get_widget_value('ClmMartOwner','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')
ClmMartArraySize = get_widget_value('ClmMartArraySize','')
ClmMartRecordCount = get_widget_value('ClmMartRecordCount','')

schema_seq_CLM_DM_CLM_DIAG_csv_load = StructType([
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("CLM_ID", StringType(), False),
    StructField("CLM_DIAG_ORDNL_CD", StringType(), False),
    StructField("DIAG_CD", StringType(), False),
    StructField("DIAG_CD_DESC", StringType(), False),
    StructField("LAST_UPDT_RUN_CYC_NO", IntegerType(), False)
])

df_seq_CLM_DM_CLM_DIAG_csv_load = (
    spark.read
        .option("header", False)
        .option("delimiter", ",")
        .option("quote", "^")
        .option("inferSchema", False)
        .schema(schema_seq_CLM_DM_CLM_DIAG_csv_load)
        .csv(f"{adls_path}/load/CLM_DM_CLM_DIAG.dat")
)

df_cpy_forBuffer = df_seq_CLM_DM_CLM_DIAG_csv_load.orderBy(
    col("SRC_SYS_CD"),
    col("CLM_ID"),
    col("CLM_DIAG_ORDNL_CD")
)

df_Odbc_CLM_DM_CLM_DIAG_out = df_cpy_forBuffer.select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("CLM_ID").alias("CLM_ID"),
    col("CLM_DIAG_ORDNL_CD").alias("CLM_DIAG_ORDNL_CD"),
    col("DIAG_CD").alias("DIAG_CD"),
    col("DIAG_CD_DESC").alias("DIAG_CD_DESC"),
    col("LAST_UPDT_RUN_CYC_NO").alias("LAST_UPDT_RUN_CYC_NO")
)

jdbc_url, jdbc_props = get_db_config(clmmart_secret_name)

execute_dml(
    "DROP TABLE IF EXISTS STAGING.IdsDmClmDmClmDiagLoad_Odbc_CLM_DM_CLM_DIAG_out_temp",
    jdbc_url,
    jdbc_props
)

df_Odbc_CLM_DM_CLM_DIAG_out.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.IdsDmClmDmClmDiagLoad_Odbc_CLM_DM_CLM_DIAG_out_temp") \
    .mode("append") \
    .save()

merge_sql = f"""
MERGE INTO {ClmMartOwner}.CLM_DM_CLM_DIAG AS T
USING STAGING.IdsDmClmDmClmDiagLoad_Odbc_CLM_DM_CLM_DIAG_out_temp AS S
ON
  T.SRC_SYS_CD = S.SRC_SYS_CD
  AND T.CLM_ID = S.CLM_ID
  AND T.CLM_DIAG_ORDNL_CD = S.CLM_DIAG_ORDNL_CD
WHEN MATCHED THEN
  UPDATE SET
    T.DIAG_CD = S.DIAG_CD,
    T.DIAG_CD_DESC = S.DIAG_CD_DESC,
    T.LAST_UPDT_RUN_CYC_NO = S.LAST_UPDT_RUN_CYC_NO
WHEN NOT MATCHED THEN
  INSERT (
    SRC_SYS_CD,
    CLM_ID,
    CLM_DIAG_ORDNL_CD,
    DIAG_CD,
    DIAG_CD_DESC,
    LAST_UPDT_RUN_CYC_NO
  )
  VALUES (
    S.SRC_SYS_CD,
    S.CLM_ID,
    S.CLM_DIAG_ORDNL_CD,
    S.DIAG_CD,
    S.DIAG_CD_DESC,
    S.LAST_UPDT_RUN_CYC_NO
  );
"""

try:
    execute_dml(merge_sql, jdbc_url, jdbc_props)
except Exception as e:
    df_reject = spark.createDataFrame(
        [(str(e.__class__.__name__), str(e))],
        ["ERRORCODE", "ERRORTEXT"]
    )
    df_reject = df_reject.select(
        rpad(col("ERRORCODE"), <...>, " ").alias("ERRORCODE"),
        rpad(col("ERRORTEXT"), <...>, " ").alias("ERRORTEXT")
    )
    write_files(
        df_reject,
        f"{adls_path}/load/CLM_DM_CLM_DIAG_Rej.dat",
        delimiter=",",
        mode="append",
        is_pqruet=False,
        header=False,
        quote="^",
        nullValue=None
    )