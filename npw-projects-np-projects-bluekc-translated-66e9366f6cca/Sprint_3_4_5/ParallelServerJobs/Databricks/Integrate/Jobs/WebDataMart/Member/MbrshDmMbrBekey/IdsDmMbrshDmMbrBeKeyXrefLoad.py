# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Shiva Devagiri         07/23/2013        5114                             Load DM Table MBRSHP_DM_MBR_BE_KEY_XREF                         IntegrateWrhsDevl    Peter Marshall               10/21/2013

# MAGIC Job Name: IdsDmMbrshDmMbrBeKeyXrefLoad
# MAGIC Read Load File created in the IdsDmMbrshpDmMbrBeKeyXrefExtr Job
# MAGIC Copy Stage for buffer
# MAGIC Update then Insert the MBRSHP_DM_MBR_BE_KEY_XREF Data.
# MAGIC Load rejects are redirected into a Reject file
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, DecimalType, StringType, IntegerType
import pyspark.sql.functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


ClmMartOwner = get_widget_value('ClmMartOwner','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')
ClmMartArraySize = get_widget_value('ClmMartArraySize','')
ClmMartRecordCount = get_widget_value('ClmMartRecordCount','')

schema_seq_MBRSHP_DM_MBR_BE_KEY_XREF_csv_load = StructType([
    StructField("INDV_BE_KEY", DecimalType(38,10), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("MBR_UNIQ_KEY", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_NO", IntegerType(), True)
])

df_seq_MBRSHP_DM_MBR_BE_KEY_XREF_csv_load = (
    spark.read
    .option("sep", ",")
    .option("quote", "^")
    .option("header", "false")
    .schema(schema_seq_MBRSHP_DM_MBR_BE_KEY_XREF_csv_load)
    .csv(f"{adls_path}/load/MBRSHP_DM_MBR_BE_KEY_XREF.dat")
)

df_cpy_forBuffer = df_seq_MBRSHP_DM_MBR_BE_KEY_XREF_csv_load.select(
    F.col("INDV_BE_KEY").alias("INDV_BE_KEY"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("LAST_UPDT_RUN_CYC_NO").alias("LAST_UPDT_RUN_CYC_NO")
)

jdbc_url, jdbc_props = get_db_config(clmmart_secret_name)

execute_dml(
    "DROP TABLE IF EXISTS STAGING.IdsDmMbrshDmMbrBeKeyXrefLoad_ODBC_MBRSHP_DM_MBR_BE_KEY_XREF_out_temp",
    jdbc_url,
    jdbc_props
)

(
    df_cpy_forBuffer
    .write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", "STAGING.IdsDmMbrshDmMbrBeKeyXrefLoad_ODBC_MBRSHP_DM_MBR_BE_KEY_XREF_out_temp")
    .mode("overwrite")
    .save()
)

merge_sql = f"""
MERGE INTO {ClmMartOwner}.MBRSH_DM_MBR_BE_KEY_XREF AS T
USING STAGING.IdsDmMbrshDmMbrBeKeyXrefLoad_ODBC_MBRSHP_DM_MBR_BE_KEY_XREF_out_temp AS S
ON 
    T.INDV_BE_KEY = S.INDV_BE_KEY
    AND T.SRC_SYS_CD = S.SRC_SYS_CD
    AND T.MBR_UNIQ_KEY = S.MBR_UNIQ_KEY
WHEN MATCHED THEN
  UPDATE SET
    T.INDV_BE_KEY = S.INDV_BE_KEY,
    T.SRC_SYS_CD = S.SRC_SYS_CD,
    T.MBR_UNIQ_KEY = S.MBR_UNIQ_KEY,
    T.LAST_UPDT_RUN_CYC_NO = S.LAST_UPDT_RUN_CYC_NO
WHEN NOT MATCHED THEN
  INSERT (
    INDV_BE_KEY,
    SRC_SYS_CD,
    MBR_UNIQ_KEY,
    LAST_UPDT_RUN_CYC_NO
  )
  VALUES (
    S.INDV_BE_KEY,
    S.SRC_SYS_CD,
    S.MBR_UNIQ_KEY,
    S.LAST_UPDT_RUN_CYC_NO
  )
;
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)

df_seq_MBRSHP_DM_MBR_BE_KEY_XREF_csv_rej_schema = StructType([
    StructField("ERRORCODE", StringType(), True),
    StructField("ERRORTEXT", StringType(), True)
])

df_seq_MBRSHP_DM_MBR_BE_KEY_XREF_csv_rej = spark.createDataFrame(
    [],
    df_seq_MBRSHP_DM_MBR_BE_KEY_XREF_csv_rej_schema
)

df_seq_MBRSHP_DM_MBR_BE_KEY_XREF_csv_rej = df_seq_MBRSHP_DM_MBR_BE_KEY_XREF_csv_rej.withColumn(
    "ERRORCODE", F.rpad("ERRORCODE", 100, " ")
).withColumn(
    "ERRORTEXT", F.rpad("ERRORTEXT", 100, " ")
)

df_seq_MBRSHP_DM_MBR_BE_KEY_XREF_csv_rej = df_seq_MBRSHP_DM_MBR_BE_KEY_XREF_csv_rej.select(
    "ERRORCODE",
    "ERRORTEXT"
)

write_files(
    df_seq_MBRSHP_DM_MBR_BE_KEY_XREF_csv_rej,
    f"{adls_path}/load/MBRSHP_DM_MBR_BE_KEY_XREF.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)