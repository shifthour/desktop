# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Balkarn Gill               06/18/2013        5114                             Load DM Table PROV_DIR_DM_PDX_NTWK                               EnterpriseWrhsDevl        Peter Marshall               9/3/2013

# MAGIC Job Name: EdwDmProvDirDmPdxNtwkLoad
# MAGIC Read Load File created in the EdwDmProvDirDmPdxNtwkExtr Job
# MAGIC Copy Stage for buffer
# MAGIC Update then Insert the PROV_DIR_DM_PDX_NTWK Data.
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


WebProvDirOwner = get_widget_value('WebProvDirOwner','')
webprovdir_secret_name = get_widget_value('webprovdir_secret_name','')
ProvDirRecordCount = get_widget_value('ProvDirRecordCount','')
ProvDirArraySize = get_widget_value('ProvDirArraySize','')

schema_seq_PROV_DIR_DM_PDX_NTWK_csv_load = StructType([
    StructField("PROV_ID", StringType(), True),
    StructField("PDX_NTWK_CD", StringType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("PROV_SK", IntegerType(), True),
    StructField("DIR_IN", StringType(), True),
    StructField("EFF_DT", TimestampType(), True),
    StructField("TERM_DT", TimestampType(), True),
    StructField("PDX_NTWK_NM", StringType(), True),
    StructField("LAST_UPDT_DT", TimestampType(), True)
])

df_seq_PROV_DIR_DM_PDX_NTWK_csv_load = (
    spark.read.format("csv")
    .option("header", "false")
    .option("sep", ",")
    .option("quote", "^")
    .option("nullValue", None)
    .schema(schema_seq_PROV_DIR_DM_PDX_NTWK_csv_load)
    .load(f"{adls_path}/load/PROV_DIR_DM_PDX_NTWK.dat")
)

df_cpy_forBuffer = df_seq_PROV_DIR_DM_PDX_NTWK_csv_load.select(
    F.rpad(F.col("PROV_ID"), F.lit(<...>), " ").alias("PROV_ID"),
    F.rpad(F.col("PDX_NTWK_CD"), F.lit(<...>), " ").alias("PDX_NTWK_CD"),
    F.rpad(F.col("SRC_SYS_CD"), F.lit(<...>), " ").alias("SRC_SYS_CD"),
    F.col("PROV_SK").alias("PROV_SK"),
    F.rpad(F.col("DIR_IN"), 1, " ").alias("DIR_IN"),
    F.col("EFF_DT").alias("EFF_DT"),
    F.col("TERM_DT").alias("TERM_DT"),
    F.rpad(F.col("PDX_NTWK_NM"), F.lit(<...>), " ").alias("PDX_NTWK_NM"),
    F.col("LAST_UPDT_DT").alias("LAST_UPDT_DT")
)

jdbc_url, jdbc_props = get_db_config(webprovdir_secret_name)

execute_dml(
    "DROP TABLE IF EXISTS STAGING.EdwDmProvDirDmPdxNtwkLoad_ODBC_PROV_DIR_DM_PDX_NTWK_out_temp",
    jdbc_url,
    jdbc_props
)

(
    df_cpy_forBuffer
    .write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", "STAGING.EdwDmProvDirDmPdxNtwkLoad_ODBC_PROV_DIR_DM_PDX_NTWK_out_temp")
    .mode("overwrite")
    .save()
)

merge_sql = (
    "MERGE INTO " + WebProvDirOwner + ".PROV_DIR_DM_PDX_NTWK AS T "
    "USING STAGING.EdwDmProvDirDmPdxNtwkLoad_ODBC_PROV_DIR_DM_PDX_NTWK_out_temp AS S "
    "ON (T.PROV_ID = S.PROV_ID AND T.PDX_NTWK_CD = S.PDX_NTWK_CD AND T.SRC_SYS_CD = S.SRC_SYS_CD) "
    "WHEN MATCHED THEN UPDATE SET "
    "T.PROV_SK = S.PROV_SK, "
    "T.DIR_IN = S.DIR_IN, "
    "T.EFF_DT = S.EFF_DT, "
    "T.TERM_DT = S.TERM_DT, "
    "T.PDX_NTWK_NM = S.PDX_NTWK_NM, "
    "T.LAST_UPDT_DT = S.LAST_UPDT_DT "
    "WHEN NOT MATCHED THEN INSERT (PROV_ID, PDX_NTWK_CD, SRC_SYS_CD, PROV_SK, DIR_IN, EFF_DT, TERM_DT, PDX_NTWK_NM, LAST_UPDT_DT) "
    "VALUES (S.PROV_ID, S.PDX_NTWK_CD, S.SRC_SYS_CD, S.PROV_SK, S.DIR_IN, S.EFF_DT, S.TERM_DT, S.PDX_NTWK_NM, S.LAST_UPDT_DT);"
)

execute_dml(merge_sql, jdbc_url, jdbc_props)

df_seq_PROV_DIR_DM_PDX_NTWK_csv_rej = df_cpy_forBuffer.select(
    "PROV_ID",
    "PDX_NTWK_CD",
    "SRC_SYS_CD",
    "PROV_SK",
    "DIR_IN",
    "EFF_DT",
    "TERM_DT",
    "PDX_NTWK_NM",
    "LAST_UPDT_DT"
).withColumn("ERRORCODE", F.lit(None).cast(StringType())).withColumn("ERRORTEXT", F.lit(None).cast(StringType()))

write_files(
    df_seq_PROV_DIR_DM_PDX_NTWK_csv_rej,
    f"{adls_path}/load/PROV_DIR_DM_PDX_NTWK_Rej.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)