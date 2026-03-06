# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2010 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME : FncIdsCoLoad
# MAGIC 
# MAGIC CALLED BY: FncIdsCoCntl
# MAGIC 
# MAGIC PROCESSING: Loads data from the extracts of workday company file.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                          Date                      Project/Altiris #                    Change Description                             		Development Project              Code Reviewer          Date Reviewed
# MAGIC ----------------------------------      -------------------         -----------------------------------        -----------------------------------------------------         ----------------------------------               ----------------------------      -------------------------   
# MAGIC Raj Kommineni                 2020-04-29                  5879                                Originally Programmed                         		IntegrateDev2                        Kalyan Neelam          2020-05-05
# MAGIC Raj Kommineni                 2020-06-08                  5879                                Updated db2 stage properties and keys   		IntegrateDev2          	Jaideep Mankala         06/11/2020
# MAGIC Raj Kommineni                 2020-06-22                  5879                                added hash partition and sorting           		IntegrateDev2       	Jaideep Mankala             06/23/2020
# MAGIC Raj Kommineni                 2020-07-06                  5879                                Updated Db2 properties and added parameters  	IntegrateDev2     	Jaideep Mankala   07/09/2020


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, rpad
# COMMAND ----------
# MAGIC %run ../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
RunID = get_widget_value("RunID","")

schema_seq_COLOAD = StructType([
    StructField("CO_SK", IntegerType(), nullable=False),
    StructField("CO_ID", StringType(), nullable=False),
    StructField("SRC_SYS_CD", IntegerType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("CO_LGL_NM", StringType(), nullable=False),
    StructField("FULL_CO_MAIL_ADDR_TX", StringType(), nullable=False),
    StructField("CO_MAIL_ADDR_LN_1", StringType(), nullable=True),
    StructField("CO_MAIL_ADDR_LN_2", StringType(), nullable=True),
    StructField("CO_MAIL_ADDR_CITY_NM", StringType(), nullable=True),
    StructField("CO_MAIL_ADDR_ST_CD_SK", IntegerType(), nullable=True),
    StructField("CO_MAIL_ADDR_ZIP_CD_5", StringType(), nullable=True),
    StructField("CO_MAIL_ADDR_ZIP_CD_4", StringType(), nullable=True),
    StructField("NAIC_ID", StringType(), nullable=False),
    StructField("TAX_ID", StringType(), nullable=False)
])

df_seq_COLOAD = (
    spark.read.format("csv")
    .option("header", True)
    .option("sep", "|")
    .option("quote", "\"")
    .option("nullValue", None)
    .schema(schema_seq_COLOAD)
    .load(f"{adls_path}/load/IDS.CO.Load.{RunID}.dat")
)

df_cpy = df_seq_COLOAD.select(
    df_seq_COLOAD["CO_SK"].alias("CO_SK"),
    df_seq_COLOAD["CO_ID"].alias("CO_ID"),
    df_seq_COLOAD["SRC_SYS_CD"].alias("SRC_SYS_CD"),
    df_seq_COLOAD["CRT_RUN_CYC_EXCTN_SK"].alias("CRT_RUN_CYC_EXCTN_SK"),
    df_seq_COLOAD["LAST_UPDT_RUN_CYC_EXCTN_SK"].alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    df_seq_COLOAD["CO_LGL_NM"].alias("CO_LGL_NM"),
    df_seq_COLOAD["FULL_CO_MAIL_ADDR_TX"].alias("FULL_CO_MAIL_ADDR_TX"),
    df_seq_COLOAD["CO_MAIL_ADDR_LN_1"].alias("CO_MAIL_ADDR_LN_1"),
    df_seq_COLOAD["CO_MAIL_ADDR_LN_2"].alias("CO_MAIL_ADDR_LN_2"),
    df_seq_COLOAD["CO_MAIL_ADDR_CITY_NM"].alias("CO_MAIL_ADDR_CITY_NM"),
    df_seq_COLOAD["CO_MAIL_ADDR_ST_CD_SK"].alias("CO_MAIL_ADDR_ST_CD_SK"),
    df_seq_COLOAD["CO_MAIL_ADDR_ZIP_CD_5"].alias("CO_MAIL_ADDR_ZIP_CD_5"),
    df_seq_COLOAD["CO_MAIL_ADDR_ZIP_CD_4"].alias("CO_MAIL_ADDR_ZIP_CD_4"),
    df_seq_COLOAD["NAIC_ID"].alias("NAIC_ID"),
    df_seq_COLOAD["TAX_ID"].alias("TAX_ID")
)

df_enriched = df_cpy.select(
    col("CO_SK").alias("CO_SK"),
    rpad(col("CO_ID"), <...>, " ").alias("CO_ID"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(col("CO_LGL_NM"), <...>, " ").alias("CO_LGL_NM"),
    rpad(col("FULL_CO_MAIL_ADDR_TX"), <...>, " ").alias("FULL_CO_MAIL_ADDR_TX"),
    rpad(col("CO_MAIL_ADDR_LN_1"), <...>, " ").alias("CO_MAIL_ADDR_LN_1"),
    rpad(col("CO_MAIL_ADDR_LN_2"), <...>, " ").alias("CO_MAIL_ADDR_LN_2"),
    rpad(col("CO_MAIL_ADDR_CITY_NM"), <...>, " ").alias("CO_MAIL_ADDR_CITY_NM"),
    col("CO_MAIL_ADDR_ST_CD_SK").alias("CO_MAIL_ADDR_ST_CD_SK"),
    rpad(col("CO_MAIL_ADDR_ZIP_CD_5"), 5, " ").alias("CO_MAIL_ADDR_ZIP_CD_5"),
    rpad(col("CO_MAIL_ADDR_ZIP_CD_4"), 4, " ").alias("CO_MAIL_ADDR_ZIP_CD_4"),
    rpad(col("NAIC_ID"), <...>, " ").alias("NAIC_ID"),
    rpad(col("TAX_ID"), <...>, " ").alias("TAX_ID")
)

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
execute_dml("DROP TABLE IF EXISTS STAGING.FncIdsCoLoad_CO_temp", jdbc_url, jdbc_props)

df_enriched.write.format("jdbc").option("url", jdbc_url).options(**jdbc_props)\
    .option("dbtable", "STAGING.FncIdsCoLoad_CO_temp").mode("append").save()

merge_sql = f"""
MERGE INTO {IDSOwner}.CO AS T
USING STAGING.FncIdsCoLoad_CO_temp AS S
ON T.CO_SK = S.CO_SK
WHEN MATCHED THEN UPDATE SET
  T.CO_SK = S.CO_SK,
  T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
  T.LAST_UPDT_RUN_CYC_EXCTN_SK = S.LAST_UPDT_RUN_CYC_EXCTN_SK,
  T.CO_LGL_NM = S.CO_LGL_NM,
  T.FULL_CO_MAIL_ADDR_TX = S.FULL_CO_MAIL_ADDR_TX,
  T.CO_MAIL_ADDR_LN_1 = S.CO_MAIL_ADDR_LN_1,
  T.CO_MAIL_ADDR_LN_2 = S.CO_MAIL_ADDR_LN_2,
  T.CO_MAIL_ADDR_CITY_NM = S.CO_MAIL_ADDR_CITY_NM,
  T.CO_MAIL_ADDR_ST_CD_SK = S.CO_MAIL_ADDR_ST_CD_SK,
  T.CO_MAIL_ADDR_ZIP_CD_5 = S.CO_MAIL_ADDR_ZIP_CD_5,
  T.CO_MAIL_ADDR_ZIP_CD_4 = S.CO_MAIL_ADDR_ZIP_CD_4,
  T.NAIC_ID = S.NAIC_ID,
  T.TAX_ID = S.TAX_ID
WHEN NOT MATCHED THEN
  INSERT (
    CO_SK,
    CO_ID,
    SRC_SYS_CD,
    CRT_RUN_CYC_EXCTN_SK,
    LAST_UPDT_RUN_CYC_EXCTN_SK,
    CO_LGL_NM,
    FULL_CO_MAIL_ADDR_TX,
    CO_MAIL_ADDR_LN_1,
    CO_MAIL_ADDR_LN_2,
    CO_MAIL_ADDR_CITY_NM,
    CO_MAIL_ADDR_ST_CD_SK,
    CO_MAIL_ADDR_ZIP_CD_5,
    CO_MAIL_ADDR_ZIP_CD_4,
    NAIC_ID,
    TAX_ID
  )
  VALUES (
    S.CO_SK,
    S.CO_ID,
    S.SRC_SYS_CD,
    S.CRT_RUN_CYC_EXCTN_SK,
    S.LAST_UPDT_RUN_CYC_EXCTN_SK,
    S.CO_LGL_NM,
    S.FULL_CO_MAIL_ADDR_TX,
    S.CO_MAIL_ADDR_LN_1,
    S.CO_MAIL_ADDR_LN_2,
    S.CO_MAIL_ADDR_CITY_NM,
    S.CO_MAIL_ADDR_ST_CD_SK,
    S.CO_MAIL_ADDR_ZIP_CD_5,
    S.CO_MAIL_ADDR_ZIP_CD_4,
    S.NAIC_ID,
    S.TAX_ID
  )
;
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)