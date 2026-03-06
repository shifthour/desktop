# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Archana Palivela     08/09/2013          5114                             Original Programming                                                                             IntegrateWrhsDevl       Jag Yelavarthi            2013-11-30
# MAGIC 
# MAGIC Ashok Donavalli      05/18/2020        225386                          Added Run Stats post load for W_WEBDM_ETL_DRVR          IntegrateDev2                 Jaideep Mankala     05/21/2020

# MAGIC Job Name: IdsIdsWCdmEtlDrvrLoad
# MAGIC Read Load File created in the IdsDmClmMartDriverExtr Job
# MAGIC Copy Stage for buffer
# MAGIC Replace and Insert the W_WEBDM_ETL_DRVR Data.
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


# Retrieve parameters
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

# ----------------------------------------------------------------------------
# Stage: seq_W_WEBDM_ETL_DRVR_load (PxSequentialFile)
# ----------------------------------------------------------------------------
schema_seq_W_WEBDM_ETL_DRVR_load = StructType([
    StructField("SRC_SYS_CD_SK", IntegerType(), nullable=False),
    StructField("CLM_ID", StringType(), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False)
])

df_seq_W_WEBDM_ETL_DRVR_load = (
    spark.read
    .options(
        delimiter=",",
        quote="^",
        header=False,
        nullValue=None
    )
    .schema(schema_seq_W_WEBDM_ETL_DRVR_load)
    .csv(f"{adls_path}/load/W_WEBDM_ETL_DRVR.dat")
)

# ----------------------------------------------------------------------------
# Stage: cpy_forBuffer (PxCopy)
# ----------------------------------------------------------------------------
df_cpy_forBuffer = df_seq_W_WEBDM_ETL_DRVR_load.select(
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

# ----------------------------------------------------------------------------
# Stage: DB2_W_CDM_ETL_DRVR (DB2ConnectorPX)
# ----------------------------------------------------------------------------
df_DB2_W_CDM_ETL_DRVR_in = df_cpy_forBuffer.select(
    F.col("SRC_SYS_CD_SK"),
    F.rpad(F.col("CLM_ID"), <...>, " ").alias("CLM_ID"),
    F.rpad(F.col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

execute_dml(
    "DROP TABLE IF EXISTS STAGING.IdsDmClmMartDriverLoad_DB2_W_CDM_ETL_DRVR_temp",
    jdbc_url,
    jdbc_props
)

df_DB2_W_CDM_ETL_DRVR_in.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.IdsDmClmMartDriverLoad_DB2_W_CDM_ETL_DRVR_temp") \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO {IDSOwner}.W_WEBDM_ETL_DRVR AS T
USING STAGING.IdsDmClmMartDriverLoad_DB2_W_CDM_ETL_DRVR_temp AS S
ON (T.SRC_SYS_CD_SK = S.SRC_SYS_CD_SK AND T.CLM_ID = S.CLM_ID)
WHEN MATCHED THEN UPDATE SET
  T.SRC_SYS_CD = S.SRC_SYS_CD,
  T.LAST_UPDT_RUN_CYC_EXCTN_SK = S.LAST_UPDT_RUN_CYC_EXCTN_SK
WHEN NOT MATCHED THEN INSERT
  (SRC_SYS_CD_SK, CLM_ID, SRC_SYS_CD, LAST_UPDT_RUN_CYC_EXCTN_SK)
  VALUES
  (S.SRC_SYS_CD_SK, S.CLM_ID, S.SRC_SYS_CD, S.LAST_UPDT_RUN_CYC_EXCTN_SK);
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)

after_sql = f"""call admin_cmd('runstats on table {IDSOwner}.W_WEBDM_ETL_DRVR AND detailed indexes all ALLOW READ ACCESS')
commit"""
execute_dml(after_sql, jdbc_url, jdbc_props)

# ----------------------------------------------------------------------------
# Stage: seq_W_WEBDM_ETL_DRVR_csv_rej (PxSequentialFile)
# ----------------------------------------------------------------------------
schema_seq_W_WEBDM_ETL_DRVR_csv_rej = StructType([
    StructField("SRC_SYS_CD_SK", IntegerType(), nullable=True),
    StructField("CLM_ID", StringType(), nullable=True),
    StructField("SRC_SYS_CD", StringType(), nullable=True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=True),
    StructField("ERRORCODE", StringType(), nullable=True),
    StructField("ERRORTEXT", StringType(), nullable=True)
])

df_seq_W_WEBDM_ETL_DRVR_csv_rej = spark.createDataFrame([], schema_seq_W_WEBDM_ETL_DRVR_csv_rej)

df_seq_W_WEBDM_ETL_DRVR_csv_rej_out = df_seq_W_WEBDM_ETL_DRVR_csv_rej.select(
    F.col("SRC_SYS_CD_SK"),
    F.rpad(F.col("CLM_ID"), <...>, " ").alias("CLM_ID"),
    F.rpad(F.col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.rpad(F.col("ERRORCODE"), <...>, " ").alias("ERRORCODE"),
    F.rpad(F.col("ERRORTEXT"), <...>, " ").alias("ERRORTEXT")
)

write_files(
    df_seq_W_WEBDM_ETL_DRVR_csv_rej_out,
    f"{adls_path}/load/W_WEBDM_ETL_DRVR_Rej.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)