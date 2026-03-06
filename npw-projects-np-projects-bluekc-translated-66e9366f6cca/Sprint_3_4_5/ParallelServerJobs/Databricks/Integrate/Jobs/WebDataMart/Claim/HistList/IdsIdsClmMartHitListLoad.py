# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Archana Palivela     08/09/2013          5114                             Original Programming                                                                             IntegrateWrhsDevl       Jag Yelavarthi            2013-11-30
# MAGIC Bhoomi Dasari         1/6/2014             Production Fix                Changed field order to match Source file                                              IntegrateWrhsDevl       Kalyan Neelam            1/6/2014

# MAGIC Job Name: IdsIdsWCdmEtlDrvrLoad
# MAGIC Read Load File created in the IdsDmClmMartDriverExtr Job
# MAGIC Copy Stage for buffer
# MAGIC Update and Insert the W_WEBDM_ETL_DRVR Data.
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
from pyspark.sql.functions import rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

schema_seq_W_WEBDM_ETL_DRVR_load = StructType([
    StructField("CLM_ID", StringType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("SRC_SYS_CD_SK", IntegerType(), False),
    StructField("SRC_SYS_CD", StringType(), False)
])

df_seq_W_WEBDM_ETL_DRVR_load = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "^")
    .option("nullValue", None)
    .schema(schema_seq_W_WEBDM_ETL_DRVR_load)
    .load(f"{adls_path}/load/W_WEBDM_ETL_DRVR.dat")
)

df_cpy_forBuffer = df_seq_W_WEBDM_ETL_DRVR_load.repartition("SRC_SYS_CD_SK","CLM_ID").select(
    df_seq_W_WEBDM_ETL_DRVR_load["SRC_SYS_CD_SK"].alias("SRC_SYS_CD_SK"),
    df_seq_W_WEBDM_ETL_DRVR_load["CLM_ID"].alias("CLM_ID"),
    df_seq_W_WEBDM_ETL_DRVR_load["SRC_SYS_CD"].alias("SRC_SYS_CD"),
    df_seq_W_WEBDM_ETL_DRVR_load["LAST_UPDT_RUN_CYC_EXCTN_SK"].alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
execute_dml(f"DROP TABLE IF EXISTS STAGING.IdsIdsClmMartHitListLoad_DB2_W_CDM_ETL_DRVR_temp", jdbc_url, jdbc_props)

df_cpy_forBuffer.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.IdsIdsClmMartHitListLoad_DB2_W_CDM_ETL_DRVR_temp") \
    .mode("overwrite") \
    .save()

merge_sql = f"""MERGE {IDSOwner}.W_WEBDM_ETL_DRVR as T
USING STAGING.IdsIdsClmMartHitListLoad_DB2_W_CDM_ETL_DRVR_temp as S
ON T.SRC_SYS_CD_SK = S.SRC_SYS_CD_SK AND T.CLM_ID = S.CLM_ID
WHEN MATCHED THEN UPDATE
  SET T.SRC_SYS_CD = S.SRC_SYS_CD,
      T.LAST_UPDT_RUN_CYC_EXCTN_SK = S.LAST_UPDT_RUN_CYC_EXCTN_SK
WHEN NOT MATCHED THEN INSERT
  (SRC_SYS_CD_SK, CLM_ID, SRC_SYS_CD, LAST_UPDT_RUN_CYC_EXCTN_SK)
  VALUES
  (S.SRC_SYS_CD_SK, S.CLM_ID, S.SRC_SYS_CD, S.LAST_UPDT_RUN_CYC_EXCTN_SK);
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

schema_seq_W_WEBDM_ETL_DRVR_csv_rej = StructType([
    StructField("SRC_SYS_CD_SK", IntegerType(), True),
    StructField("CLM_ID", StringType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("ERRORCODE", StringType(), True),
    StructField("ERRORTEXT", StringType(), True)
])

df_seq_W_WEBDM_ETL_DRVR_csv_rej = spark.createDataFrame([], schema_seq_W_WEBDM_ETL_DRVR_csv_rej)
df_seq_W_WEBDM_ETL_DRVR_csv_rej = df_seq_W_WEBDM_ETL_DRVR_csv_rej.withColumn("CLM_ID", rpad("CLM_ID", 100, " ")) \
    .withColumn("SRC_SYS_CD", rpad("SRC_SYS_CD", 100, " ")) \
    .withColumn("ERRORCODE", rpad("ERRORCODE", 100, " ")) \
    .withColumn("ERRORTEXT", rpad("ERRORTEXT", 100, " "))

write_files(
    df_seq_W_WEBDM_ETL_DRVR_csv_rej.select(
        "SRC_SYS_CD_SK",
        "CLM_ID",
        "SRC_SYS_CD",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "ERRORCODE",
        "ERRORTEXT"
    ),
    f"{adls_path}/load/W_WEBDM_ETL_DRVR_Rej.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)