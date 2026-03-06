# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Archana Palivela        3/26/14              5114                             Original Programming                                                                            IntegrateWrhsDevl     Bhoomi Dasari              4/8/2014

# MAGIC Job Name: UwsDmWebGrpCfgLoad
# MAGIC Read Load File created in the UwsDmWebGrpCfgExtr Job
# MAGIC Copy Stage for buffer
# MAGIC Update and  Insert the WEB_GRP_CFG Data.
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
from pyspark.sql.functions import col, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


ClmMartOwner = get_widget_value('ClmMartOwner','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')
ClmMartArraySize = get_widget_value('ClmMartArraySize','')
ClmMartRecordCount = get_widget_value('ClmMartRecordCount','')

schema_seq_WEB_GRP_CFG_csv_load = StructType([
    StructField("GRP_ID", StringType(), nullable=False),
    StructField("GRP_UNIQ_KEY", IntegerType(), nullable=False),
    StructField("LOGO_URL_ADDR", StringType(), nullable=False),
    StructField("USER_ID", StringType(), nullable=False),
    StructField("LAST_UPDT_DT_SK", StringType(), nullable=False)
])

df_seq_WEB_GRP_CFG_csv_load = (
    spark.read.format("csv")
    .option("header", "false")
    .option("sep", ",")
    .option("quote", "^")
    .option("nullValue", None)
    .schema(schema_seq_WEB_GRP_CFG_csv_load)
    .load(f"{adls_path}/load/WEB_GRP_CFG.dat")
)

df_cpy_forBuffer = df_seq_WEB_GRP_CFG_csv_load.select(
    col("GRP_ID").alias("GRP_ID"),
    col("GRP_UNIQ_KEY").alias("GRP_UNIQ_KEY"),
    col("LOGO_URL_ADDR").alias("LOGO_URL_ADDR"),
    col("USER_ID").alias("USER_ID"),
    col("LAST_UPDT_DT_SK").alias("LAST_UPDT_DT_SK")
)

df_cpy_forBuffer_final = (
    df_cpy_forBuffer
    .withColumn("GRP_ID", rpad("GRP_ID", <...>, " "))
    .withColumn("LOGO_URL_ADDR", rpad("LOGO_URL_ADDR", <...>, " "))
    .withColumn("USER_ID", rpad("USER_ID", <...>, " "))
    .withColumn("LAST_UPDT_DT_SK", rpad("LAST_UPDT_DT_SK", 10, " "))
)

jdbc_url, jdbc_props = get_db_config(clmmart_secret_name)
drop_sql = "DROP TABLE IF EXISTS STAGING.UwsDmWebGrpCfgLoad_Odbc_WEB_GRP_CFG_out_temp"
execute_dml(drop_sql, jdbc_url, jdbc_props)

(
    df_cpy_forBuffer_final
    .write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", "STAGING.UwsDmWebGrpCfgLoad_Odbc_WEB_GRP_CFG_out_temp")
    .mode("append")
    .save()
)

merge_sql = f"""
MERGE INTO {ClmMartOwner}.WEB_GRP_CFG AS T
USING STAGING.UwsDmWebGrpCfgLoad_Odbc_WEB_GRP_CFG_out_temp AS S
ON T.GRP_ID = S.GRP_ID
WHEN MATCHED THEN UPDATE SET
  T.GRP_UNIQ_KEY = S.GRP_UNIQ_KEY,
  T.LOGO_URL_ADDR = S.LOGO_URL_ADDR,
  T.USER_ID = S.USER_ID,
  T.LAST_UPDT_DT_SK = S.LAST_UPDT_DT_SK
WHEN NOT MATCHED THEN
  INSERT (GRP_ID, GRP_UNIQ_KEY, LOGO_URL_ADDR, USER_ID, LAST_UPDT_DT_SK)
  VALUES (S.GRP_ID, S.GRP_UNIQ_KEY, S.LOGO_URL_ADDR, S.USER_ID, S.LAST_UPDT_DT_SK);
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

df_odbc_WEB_GRP_CFG_out_reject = spark.createDataFrame(
    [],
    StructType([
        StructField("GRP_ID", StringType(), True),
        StructField("GRP_UNIQ_KEY", StringType(), True),
        StructField("LOGO_URL_ADDR", StringType(), True),
        StructField("USER_ID", StringType(), True),
        StructField("LAST_UPDT_DT_SK", StringType(), True),
        StructField("ERRORCODE", StringType(), True),
        StructField("ERRORTEXT", StringType(), True)
    ])
)

df_seq_WEB_GRP_CFG_csv_rej = (
    df_odbc_WEB_GRP_CFG_out_reject
    .withColumn("GRP_ID", rpad("GRP_ID", <...>, " "))
    .withColumn("LOGO_URL_ADDR", rpad("LOGO_URL_ADDR", <...>, " "))
    .withColumn("USER_ID", rpad("USER_ID", <...>, " "))
    .withColumn("LAST_UPDT_DT_SK", rpad("LAST_UPDT_DT_SK", 10, " "))
    .withColumn("ERRORCODE", rpad("ERRORCODE", <...>, " "))
    .withColumn("ERRORTEXT", rpad("ERRORTEXT", <...>, " "))
)

write_files(
    df_seq_WEB_GRP_CFG_csv_rej.select("GRP_ID", "GRP_UNIQ_KEY", "LOGO_URL_ADDR", "USER_ID", "LAST_UPDT_DT_SK", "ERRORCODE", "ERRORTEXT"),
    f"{adls_path}/load/WEB_GRP_CFG_Rej.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)