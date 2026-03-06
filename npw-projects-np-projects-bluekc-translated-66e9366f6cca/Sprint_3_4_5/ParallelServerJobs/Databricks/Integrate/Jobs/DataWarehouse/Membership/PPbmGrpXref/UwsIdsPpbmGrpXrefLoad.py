# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Called By: MedtrakDrugClmCntl and SavRXDrugClmCntl jobs.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                             DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER                       DATE                     PROJECT                     DESCRIPTION                                                                                   ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------             ----------------------     ------------------------------        ---------------------------------------------------------------------------                                      ------------------------------    ------------------------------       --------------------
# MAGIC Shanmugam Annamalai      2017 -11-17             5828                                 Original Program                                                                                IntegrateDev2            Kalyan Neelam           2017-12-05

# MAGIC Copy Stage for buffer
# MAGIC Job to load the Target DB2 table P_PBM_GRP_XREF
# MAGIC Load file created in Extract job will be loaded into the target DB2 table P_PBM_GRP_XREF
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
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

schema_seq_P_PBM_GRP_XREF_csv_load = StructType([
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PBM_GRP_ID", StringType(), nullable=False),
    StructField("EFF_DT", TimestampType(), nullable=False),
    StructField("TERM_DT", TimestampType(), nullable=False),
    StructField("GRP_ID", StringType(), nullable=False),
    StructField("CRT_USER_ID", StringType(), nullable=False),
    StructField("CRT_DTM", TimestampType(), nullable=False),
    StructField("LAST_UPDT_USER_ID", StringType(), nullable=False),
    StructField("LAST_UPDT_DTM", TimestampType(), nullable=False)
])

df_seq_P_PBM_GRP_XREF_csv_load = (
    spark.read.format("csv")
    .option("delimiter", ",")
    .option("quote", "^")
    .option("header", "false")
    .option("nullValue", None)
    .schema(schema_seq_P_PBM_GRP_XREF_csv_load)
    .load(f"{adls_path}/load/P_PBM_GRP_XREF.dat")
)

df_cpy_forBuffer = df_seq_P_PBM_GRP_XREF_csv_load.select(
    F.col("SRC_SYS_CD"),
    F.col("PBM_GRP_ID"),
    F.col("EFF_DT"),
    F.col("TERM_DT"),
    F.col("GRP_ID"),
    F.col("CRT_USER_ID"),
    F.col("CRT_DTM"),
    F.col("LAST_UPDT_USER_ID"),
    F.col("LAST_UPDT_DTM")
)

df_cpy_forBuffer_final = df_cpy_forBuffer.select(
    F.rpad(F.col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    F.rpad(F.col("PBM_GRP_ID"), <...>, " ").alias("PBM_GRP_ID"),
    F.col("EFF_DT"),
    F.col("TERM_DT"),
    F.rpad(F.col("GRP_ID"), <...>, " ").alias("GRP_ID"),
    F.rpad(F.col("CRT_USER_ID"), <...>, " ").alias("CRT_USER_ID"),
    F.col("CRT_DTM"),
    F.rpad(F.col("LAST_UPDT_USER_ID"), <...>, " ").alias("LAST_UPDT_USER_ID"),
    F.col("LAST_UPDT_DTM")
)

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

truncate_sql = f"TRUNCATE TABLE {IDSOwner}.P_PBM_GRP_XREF"
execute_dml(truncate_sql, jdbc_url, jdbc_props)

(
    df_cpy_forBuffer_final
    .write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", f"{IDSOwner}.P_PBM_GRP_XREF")
    .mode("append")
    .save()
)