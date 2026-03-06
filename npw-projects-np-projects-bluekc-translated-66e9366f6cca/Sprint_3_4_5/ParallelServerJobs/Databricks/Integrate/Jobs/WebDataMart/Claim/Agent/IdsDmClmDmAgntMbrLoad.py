# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Archana Palivela     09/15/2013          5114                             Original Programming                                                                             IntegrateWrhsDevl      Jag Yelavarthi             2013-11-28

# MAGIC Job Name: IdsDmMbrshDmFmlyLmtLoad
# MAGIC Read Load File created in the IdsDmClmDmAgntMbrExtr Job
# MAGIC Copy Stage for buffer
# MAGIC Replace the CLM_DM_AGNT_MBR Data.
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

schema_seq_CLM_DM_AGNT_MBR_csv_load = StructType([
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("AGNT_ID", StringType(), nullable=False),
    StructField("MBR_UNIQ_KEY", IntegerType(), nullable=False),
    StructField("AGNT_NM", StringType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_NO", IntegerType(), nullable=False)
])

df_seq_CLM_DM_AGNT_MBR_csv_load = (
    spark.read
    .option("sep", ",")
    .option("quote", "^")
    .option("header", "false")
    .schema(schema_seq_CLM_DM_AGNT_MBR_csv_load)
    .csv(f"{adls_path}/load/CLM_DM_AGNT_MBR.dat")
)

df_cpy_forBuffer = df_seq_CLM_DM_AGNT_MBR_csv_load.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("AGNT_ID").alias("AGNT_ID"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("AGNT_NM").alias("AGNT_NM"),
    F.col("LAST_UPDT_RUN_CYC_NO").alias("LAST_UPDT_RUN_CYC_NO")
)

df_Odbc_CLM_DM_AGNT_MBR_out_input = df_cpy_forBuffer.select(
    F.col("SRC_SYS_CD"),
    F.col("AGNT_ID"),
    F.col("MBR_UNIQ_KEY"),
    F.col("AGNT_NM"),
    F.col("LAST_UPDT_RUN_CYC_NO")
).withColumn("SRC_SYS_CD", F.rpad(F.col("SRC_SYS_CD"), <...>, " ")) \
 .withColumn("AGNT_ID", F.rpad(F.col("AGNT_ID"), <...>, " ")) \
 .withColumn("AGNT_NM", F.rpad(F.col("AGNT_NM"), <...>, " "))

jdbc_url, jdbc_props = get_db_config(clmmart_secret_name)

execute_dml("DROP TABLE IF EXISTS STAGING.IdsDmClmDmAgntMbrLoad_Odbc_CLM_DM_AGNT_MBR_out_temp", jdbc_url, jdbc_props)

df_Odbc_CLM_DM_AGNT_MBR_out_input.write.format("jdbc") \
  .option("url", jdbc_url) \
  .options(**jdbc_props) \
  .option("dbtable", "STAGING.IdsDmClmDmAgntMbrLoad_Odbc_CLM_DM_AGNT_MBR_out_temp") \
  .mode("overwrite") \
  .save()

execute_dml(f"TRUNCATE TABLE {ClmMartOwner}.CLM_DM_AGNT_MBR", jdbc_url, jdbc_props)

merge_sql = f"""
MERGE INTO {ClmMartOwner}.CLM_DM_AGNT_MBR AS T
USING STAGING.IdsDmClmDmAgntMbrLoad_Odbc_CLM_DM_AGNT_MBR_out_temp AS S
ON (T.SRC_SYS_CD = S.SRC_SYS_CD
    AND T.AGNT_ID = S.AGNT_ID
    AND T.MBR_UNIQ_KEY = S.MBR_UNIQ_KEY)
WHEN MATCHED THEN UPDATE SET
  T.AGNT_NM = S.AGNT_NM,
  T.LAST_UPDT_RUN_CYC_NO = S.LAST_UPDT_RUN_CYC_NO
WHEN NOT MATCHED THEN INSERT (
  SRC_SYS_CD, AGNT_ID, MBR_UNIQ_KEY, AGNT_NM, LAST_UPDT_RUN_CYC_NO
)
VALUES (
  S.SRC_SYS_CD, S.AGNT_ID, S.MBR_UNIQ_KEY, S.AGNT_NM, S.LAST_UPDT_RUN_CYC_NO
);
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

schema_seq_CLM_DM_AGNT_MBR_csv_rej = StructType([
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("AGNT_ID", StringType(), True),
    StructField("MBR_UNIQ_KEY", IntegerType(), True),
    StructField("AGNT_NM", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_NO", IntegerType(), True),
    StructField("ERRORCODE", StringType(), True),
    StructField("ERRORTEXT", StringType(), True)
])

df_seq_CLM_DM_AGNT_MBR_csv_rej = spark.createDataFrame([], schema_seq_CLM_DM_AGNT_MBR_csv_rej).select(
    F.rpad(F.col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    F.rpad(F.col("AGNT_ID"), <...>, " ").alias("AGNT_ID"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.rpad(F.col("AGNT_NM"), <...>, " ").alias("AGNT_NM"),
    F.col("LAST_UPDT_RUN_CYC_NO").alias("LAST_UPDT_RUN_CYC_NO"),
    F.rpad(F.col("ERRORCODE"), <...>, " ").alias("ERRORCODE"),
    F.rpad(F.col("ERRORTEXT"), <...>, " ").alias("ERRORTEXT")
)

write_files(
    df_seq_CLM_DM_AGNT_MBR_csv_rej,
    f"{adls_path}/load/CLM_DM_AGNT_MBR_Rej.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)