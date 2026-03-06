# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2010 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME : IdsEdwCoLoad
# MAGIC 
# MAGIC CALLED BY: IdsEdwCoCntl
# MAGIC 
# MAGIC PROCESSING: Loads data from the extracts of workday company file.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                          Date                      Project/Altiris #                    Change Description                             Development Project              Code Reviewer          Date Reviewed
# MAGIC ----------------------------------      -------------------         -----------------------------------        -----------------------------------------------------         ----------------------------------               ----------------------------      -------------------------   
# MAGIC Raj Kommineni                 2020-05-05                  5879                                Originally Programmed                         EnterpriseDev2                       Kalyan Neelam          2020-05-07 
# MAGIC 
# MAGIC Raj Kommineni                 2020-06-08                  5879                                updated db2 stage properties and keys     EnterpriseDev2          Jaideep Mankala       06/11/2020
# MAGIC 
# MAGIC Raj Kommineni                 2020-06-22                  5879                               added hash partition and sorting            EnterpriseDev2                  Jaideep Mankala       06/23/2020
# MAGIC 
# MAGIC Raj Kommineni                 2020-07-06                  5879                                Updated Db2 properties and added parameters  EnterpriseDev2   Jaideep Mankala   07/09/2020


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import col, rpad
# COMMAND ----------
# MAGIC %run ../../../Utility_Enterprise
# COMMAND ----------


EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')

schema_seq_COLOAD = StructType([
    StructField("CO_SK", IntegerType(), True),
    StructField("CO_ID", StringType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_DT_SK", DateType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", DateType(), True),
    StructField("CO_LGL_NM", StringType(), True),
    StructField("FULL_CO_MAIL_ADDR_TX", StringType(), True),
    StructField("CO_MAIL_ADDR_LN_1", StringType(), True),
    StructField("CO_MAIL_ADDR_LN_2", StringType(), True),
    StructField("CO_MAIL_ADDR_CITY_NM", StringType(), True),
    StructField("CO_MAIL_ADDR_ST_CD", StringType(), True),
    StructField("CO_MAIL_ADDR_ST_NM", StringType(), True),
    StructField("CO_MAIL_ADDR_ZIP_CD_5", StringType(), True),
    StructField("CO_MAIL_ADDR_ZIP_CD_4", StringType(), True),
    StructField("NAIC_ID", StringType(), True),
    StructField("TAX_ID", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("CO_MAIL_ADDR_ST_CD_SK", IntegerType(), True)
])

df_seq_COLOAD = (
    spark.read
    .option("header", True)
    .option("sep", "|")
    .option("quote", '"')
    .schema(schema_seq_COLOAD)
    .csv(f"{adls_path}/load/EDW.CO_D.Load.dat")
)

df_cpy = df_seq_COLOAD.select(
    col("CO_SK").alias("CO_SK"),
    col("CO_ID").alias("CO_ID"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("CO_LGL_NM").alias("CO_LGL_NM"),
    col("FULL_CO_MAIL_ADDR_TX").alias("FULL_CO_MAIL_ADDR_TX"),
    col("CO_MAIL_ADDR_LN_1").alias("CO_MAIL_ADDR_LN_1"),
    col("CO_MAIL_ADDR_LN_2").alias("CO_MAIL_ADDR_LN_2"),
    col("CO_MAIL_ADDR_CITY_NM").alias("CO_MAIL_ADDR_CITY_NM"),
    col("CO_MAIL_ADDR_ST_CD").alias("CO_MAIL_ADDR_ST_CD"),
    col("CO_MAIL_ADDR_ST_NM").alias("CO_MAIL_ADDR_ST_NM"),
    col("CO_MAIL_ADDR_ZIP_CD_5").alias("CO_MAIL_ADDR_ZIP_CD_5"),
    col("CO_MAIL_ADDR_ZIP_CD_4").alias("CO_MAIL_ADDR_ZIP_CD_4"),
    col("NAIC_ID").alias("NAIC_ID"),
    col("TAX_ID").alias("TAX_ID"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("CO_MAIL_ADDR_ST_CD_SK").alias("CO_MAIL_ADDR_ST_CD_SK")
)

df_final = (
    df_cpy
    .withColumn("CO_MAIL_ADDR_ZIP_CD_5", rpad(col("CO_MAIL_ADDR_ZIP_CD_5"), 5, " "))
    .withColumn("CO_MAIL_ADDR_ZIP_CD_4", rpad(col("CO_MAIL_ADDR_ZIP_CD_4"), 4, " "))
    .select(
        "CO_SK",
        "CO_ID",
        "SRC_SYS_CD",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "CO_LGL_NM",
        "FULL_CO_MAIL_ADDR_TX",
        "CO_MAIL_ADDR_LN_1",
        "CO_MAIL_ADDR_LN_2",
        "CO_MAIL_ADDR_CITY_NM",
        "CO_MAIL_ADDR_ST_CD",
        "CO_MAIL_ADDR_ST_NM",
        "CO_MAIL_ADDR_ZIP_CD_5",
        "CO_MAIL_ADDR_ZIP_CD_4",
        "NAIC_ID",
        "TAX_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CO_MAIL_ADDR_ST_CD_SK"
    )
)

jdbc_url, jdbc_props = get_db_config(edw_secret_name)

execute_dml(f"DROP TABLE IF EXISTS STAGING.IdsEdwCodLoad_CO_temp", jdbc_url, jdbc_props)

df_final.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.IdsEdwCodLoad_CO_temp") \
    .mode("append") \
    .save()

merge_sql = f"""
MERGE {EDWOwner}.CO_D AS T
USING STAGING.IdsEdwCodLoad_CO_temp AS S
ON T.CO_SK = S.CO_SK
WHEN MATCHED THEN
  UPDATE SET
    T.CO_SK = S.CO_SK,
    T.CRT_RUN_CYC_EXCTN_DT_SK = S.CRT_RUN_CYC_EXCTN_DT_SK,
    T.LAST_UPDT_RUN_CYC_EXCTN_DT_SK = S.LAST_UPDT_RUN_CYC_EXCTN_DT_SK,
    T.CO_LGL_NM = S.CO_LGL_NM,
    T.FULL_CO_MAIL_ADDR_TX = S.FULL_CO_MAIL_ADDR_TX,
    T.CO_MAIL_ADDR_LN_1 = S.CO_MAIL_ADDR_LN_1,
    T.CO_MAIL_ADDR_LN_2 = S.CO_MAIL_ADDR_LN_2,
    T.CO_MAIL_ADDR_CITY_NM = S.CO_MAIL_ADDR_CITY_NM,
    T.CO_MAIL_ADDR_ST_CD = S.CO_MAIL_ADDR_ST_CD,
    T.CO_MAIL_ADDR_ST_NM = S.CO_MAIL_ADDR_ST_NM,
    T.CO_MAIL_ADDR_ZIP_CD_5 = S.CO_MAIL_ADDR_ZIP_CD_5,
    T.CO_MAIL_ADDR_ZIP_CD_4 = S.CO_MAIL_ADDR_ZIP_CD_4,
    T.NAIC_ID = S.NAIC_ID,
    T.TAX_ID = S.TAX_ID,
    T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
    T.LAST_UPDT_RUN_CYC_EXCTN_SK = S.LAST_UPDT_RUN_CYC_EXCTN_SK,
    T.CO_MAIL_ADDR_ST_CD_SK = S.CO_MAIL_ADDR_ST_CD_SK
WHEN NOT MATCHED THEN
  INSERT (
    CO_SK,
    CO_ID,
    SRC_SYS_CD,
    CRT_RUN_CYC_EXCTN_DT_SK,
    LAST_UPDT_RUN_CYC_EXCTN_DT_SK,
    CO_LGL_NM,
    FULL_CO_MAIL_ADDR_TX,
    CO_MAIL_ADDR_LN_1,
    CO_MAIL_ADDR_LN_2,
    CO_MAIL_ADDR_CITY_NM,
    CO_MAIL_ADDR_ST_CD,
    CO_MAIL_ADDR_ST_NM,
    CO_MAIL_ADDR_ZIP_CD_5,
    CO_MAIL_ADDR_ZIP_CD_4,
    NAIC_ID,
    TAX_ID,
    CRT_RUN_CYC_EXCTN_SK,
    LAST_UPDT_RUN_CYC_EXCTN_SK,
    CO_MAIL_ADDR_ST_CD_SK
  ) VALUES (
    S.CO_SK,
    S.CO_ID,
    S.SRC_SYS_CD,
    S.CRT_RUN_CYC_EXCTN_DT_SK,
    S.LAST_UPDT_RUN_CYC_EXCTN_DT_SK,
    S.CO_LGL_NM,
    S.FULL_CO_MAIL_ADDR_TX,
    S.CO_MAIL_ADDR_LN_1,
    S.CO_MAIL_ADDR_LN_2,
    S.CO_MAIL_ADDR_CITY_NM,
    S.CO_MAIL_ADDR_ST_CD,
    S.CO_MAIL_ADDR_ST_NM,
    S.CO_MAIL_ADDR_ZIP_CD_5,
    S.CO_MAIL_ADDR_ZIP_CD_4,
    S.NAIC_ID,
    S.TAX_ID,
    S.CRT_RUN_CYC_EXCTN_SK,
    S.LAST_UPDT_RUN_CYC_EXCTN_SK,
    S.CO_MAIL_ADDR_ST_CD_SK
  )
;
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)