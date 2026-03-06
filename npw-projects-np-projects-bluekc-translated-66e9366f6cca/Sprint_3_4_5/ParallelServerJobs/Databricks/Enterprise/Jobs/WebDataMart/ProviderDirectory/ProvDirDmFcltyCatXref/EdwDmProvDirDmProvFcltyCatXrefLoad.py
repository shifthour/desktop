# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC DEVELOPER                         DATE                PROJECT                                             DESCRIPTION                                                          DATASTAGE  ENVIRONMENT         CODE REVIEWER                 REVIEW  DATE
# MAGIC -----------------------------------------      ------------------        -----------------------------------------------              -------------------------------------------------------------------------         -----------------------------------------------             ------------------------------                 -------------------
# MAGIC Shiva Devagiri          06-18-2013                                     5114                                    Original Programming                                                              EnterpriseWrhsDevl                 Jag Yelavarthi                         2013-08-30
# MAGIC                                                                                                                                  (Server to Parallel Conversion)

# MAGIC Load file created in the previous job will be loaded into the target DB2 table here. 
# MAGIC 
# MAGIC Load Type; "Truncate then Insert"
# MAGIC Read Load File created in the EdwProvDirDmFcltyCatXrefExtr Job
# MAGIC Load rejects are redirected into a Reject file
# MAGIC Copy For Buffer
# MAGIC Job Name: 
# MAGIC EdwProvDirDmFcltyCatXrefLoad
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import col, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


ProvDirRecordCount = get_widget_value('ProvDirRecordCount','')
ProvDirArraySize = get_widget_value('ProvDirArraySize','')
WebProvDirOwner = get_widget_value('WebProvDirOwner','')
webprovdir_secret_name = get_widget_value('webprovdir_secret_name','')

schema_seq_PROV_DIR_DM_FCLTY_CAT_XREF_csv_load = StructType([
    StructField("FCLTY_TYP_CD", StringType(), False),
    StructField("PROV_CAT_CD", StringType(), False),
    StructField("SEQ_NO", IntegerType(), False),
    StructField("WEB_SRCH_IN", StringType(), False),
    StructField("USER_ID", StringType(), False),
    StructField("LAST_UPDT_DT", TimestampType(), False)
])

df_seq_PROV_DIR_DM_FCLTY_CAT_XREF_csv_load = (
    spark.read.format("csv")
    .option("header", "false")
    .option("sep", ",")
    .option("quote", "^")
    .schema(schema_seq_PROV_DIR_DM_FCLTY_CAT_XREF_csv_load)
    .load(f"{adls_path}/load/PROV_DIR_DM_FCLTY_CAT_XREF.dat")
)

df_cpy_forBuffer = df_seq_PROV_DIR_DM_FCLTY_CAT_XREF_csv_load.select(
    col("FCLTY_TYP_CD").alias("FCLTY_TYP_CD"),
    col("PROV_CAT_CD").alias("PROV_CAT_CD"),
    col("SEQ_NO").alias("SEQ_NO"),
    col("WEB_SRCH_IN").alias("WEB_SRCH_IN"),
    col("USER_ID").alias("USER_ID"),
    col("LAST_UPDT_DT").alias("LAST_UPDT_DT")
)

df_odbc_PROV_DIR_DM_FCLTY_CAT_XREF_out = df_cpy_forBuffer.select(
    rpad(col("FCLTY_TYP_CD"), 255, " ").alias("FCLTY_TYP_CD"),
    rpad(col("PROV_CAT_CD"), 255, " ").alias("PROV_CAT_CD"),
    col("SEQ_NO").alias("SEQ_NO"),
    rpad(col("WEB_SRCH_IN"), 1, " ").alias("WEB_SRCH_IN"),
    rpad(col("USER_ID"), 255, " ").alias("USER_ID"),
    col("LAST_UPDT_DT").alias("LAST_UPDT_DT")
)

jdbc_url_webprovdir, jdbc_props_webprovdir = get_db_config(webprovdir_secret_name)

execute_dml(
    f"DROP TABLE IF EXISTS STAGING.EdwDmProvDirDmProvFcltyCatXrefLoad_odbc_PROV_DIR_DM_FCLTY_CAT_XREF_out_temp",
    jdbc_url_webprovdir,
    jdbc_props_webprovdir
)

df_odbc_PROV_DIR_DM_FCLTY_CAT_XREF_out.write.format("jdbc") \
    .option("url", jdbc_url_webprovdir) \
    .options(**jdbc_props_webprovdir) \
    .option("dbtable", "STAGING.EdwDmProvDirDmProvFcltyCatXrefLoad_odbc_PROV_DIR_DM_FCLTY_CAT_XREF_out_temp") \
    .mode("append") \
    .save()

merge_sql = """
MERGE INTO {owner}.PROV_DIR_DM_FCLTY_CAT_XREF AS T
USING STAGING.EdwDmProvDirDmProvFcltyCatXrefLoad_odbc_PROV_DIR_DM_FCLTY_CAT_XREF_out_temp AS S
ON (
    T.FCLTY_TYP_CD = S.FCLTY_TYP_CD
    AND T.PROV_CAT_CD = S.PROV_CAT_CD
    AND T.SEQ_NO = S.SEQ_NO
)
WHEN MATCHED THEN
  UPDATE SET
    T.WEB_SRCH_IN = S.WEB_SRCH_IN,
    T.USER_ID = S.USER_ID,
    T.LAST_UPDT_DT = S.LAST_UPDT_DT
WHEN NOT MATCHED THEN
  INSERT (
    FCLTY_TYP_CD,
    PROV_CAT_CD,
    SEQ_NO,
    WEB_SRCH_IN,
    USER_ID,
    LAST_UPDT_DT
  )
  VALUES (
    S.FCLTY_TYP_CD,
    S.PROV_CAT_CD,
    S.SEQ_NO,
    S.WEB_SRCH_IN,
    S.USER_ID,
    S.LAST_UPDT_DT
  );
""".format(owner=WebProvDirOwner)

execute_dml(merge_sql, jdbc_url_webprovdir, jdbc_props_webprovdir)

df_seq_PROV_DIR_DM_FCLTY_CAT_XREFcsv_rej = spark.createDataFrame(
    [],
    StructType([
        StructField("ERRORCODE", StringType(), True),
        StructField("ERRORTEXT", StringType(), True)
    ])
)

df_seq_PROV_DIR_DM_FCLTY_CAT_XREFcsv_rej = df_seq_PROV_DIR_DM_FCLTY_CAT_XREFcsv_rej.select(
    rpad(col("ERRORCODE"), 255, " ").alias("ERRORCODE"),
    rpad(col("ERRORTEXT"), 255, " ").alias("ERRORTEXT")
)

write_files(
    df_seq_PROV_DIR_DM_FCLTY_CAT_XREFcsv_rej,
    f"{adls_path}/load/PROV_DIR_DM_FCLTY_CAT_XREF_Rej.dat",
    ",",
    "append",
    False,
    False,
    "^",
    "\"\""
)