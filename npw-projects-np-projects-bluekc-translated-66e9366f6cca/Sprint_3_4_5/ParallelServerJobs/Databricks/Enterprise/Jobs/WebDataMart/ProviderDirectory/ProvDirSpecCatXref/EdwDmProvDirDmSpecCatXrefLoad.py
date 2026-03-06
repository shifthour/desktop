# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC MODIFICATIONS:                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        
# MAGIC                                                                                                                                                                                                                                                         
# MAGIC DEVELOPER                           DATE                PROJECT                                             DESCRIPTION                                                      DATASTAGE                                     CODE                                     DATE
# MAGIC                                                                                                                                                                                                                       ENVIRONMENT                                REVIEWER                            REVIEW
# MAGIC -------------------------------------------      -------------------      ---------------------------------------------                 -------------------------------------------------------------------           -------------------------------------------------          ------------------------------                  -------------------
# MAGIC Rajasekhar Mangalampally       06/17/2013     5114                                                       Original Programming                                                EnterpriseWrhsDevl                        Jag Yelavarthi                          2013-08-29
# MAGIC                                                                                                                                         (Server to Parallel Conversion)

# MAGIC Read the Load ready file created in EdwProvDirDMWebProvCatExtr Job
# MAGIC Load rejects are redirected into a Reject file
# MAGIC 
# MAGIC This file is an append
# MAGIC Load file created in the previous job will be loaded into the target SQL Server PROV_DIR_DM_SPEC_CAT_XREF table here. 
# MAGIC 
# MAGIC Load Type; "Truncate then Insert" .
# MAGIC Copy For Buffer
# MAGIC Job Name: 
# MAGIC EdwDmProvDirDmSpecCatXrefLoad
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


WebProvDirOwner = get_widget_value("WebProvDirOwner","")
webprovdir_secret_name = get_widget_value("webprovdir_secret_name","")
ProvDirArraySize = get_widget_value("ProvDirArraySize","")
ProvDirRecordCount = get_widget_value("ProvDirRecordCount","")

schema_seq_PROV_DIR_DM_SPEC_CAT_XREF_csv_load = StructType([
    StructField("PROV_SPEC_CD", StringType(), True),
    StructField("PROV_CAT_CD", StringType(), True),
    StructField("SEQ_NO", IntegerType(), True),
    StructField("WEB_SRCH_IN", StringType(), True),
    StructField("USER_ID", StringType(), True),
    StructField("LAST_UPDT_DT", TimestampType(), True)
])

df_seq_PROV_DIR_DM_SPEC_CAT_XREF_csv_load = spark.read.csv(
    path=f"{adls_path}/load/PROV_DIR_DM_SPEC_CAT_XREF.dat",
    schema=schema_seq_PROV_DIR_DM_SPEC_CAT_XREF_csv_load,
    sep=",",
    quote="^",
    header=False
)

df_cpy_forBuffer = df_seq_PROV_DIR_DM_SPEC_CAT_XREF_csv_load

df_odbc_PROV_DIR_DM_SPEC_CAT_XREF_out = df_cpy_forBuffer.select(
    F.rpad(F.col("PROV_SPEC_CD"), <...>, " ").alias("PROV_SPEC_CD"),
    F.rpad(F.col("PROV_CAT_CD"), <...>, " ").alias("PROV_CAT_CD"),
    F.col("SEQ_NO").alias("SEQ_NO"),
    F.rpad(F.col("WEB_SRCH_IN"), 1, " ").alias("WEB_SRCH_IN"),
    F.rpad(F.col("USER_ID"), <...>, " ").alias("USER_ID"),
    F.col("LAST_UPDT_DT").alias("LAST_UPDT_DT")
)

jdbc_url, jdbc_props = get_db_config(webprovdir_secret_name)

execute_dml("DROP TABLE IF EXISTS STAGING.EdwDmProvDirDmSpecCatXrefLoad_odbc_PROV_DIR_DM_SPEC_CAT_XREF_out_temp", jdbc_url, jdbc_props)

df_odbc_PROV_DIR_DM_SPEC_CAT_XREF_out.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.EdwDmProvDirDmSpecCatXrefLoad_odbc_PROV_DIR_DM_SPEC_CAT_XREF_out_temp") \
    .mode("overwrite") \
    .save()

execute_dml(f"TRUNCATE TABLE {WebProvDirOwner}.PROV_DIR_DM_SPEC_CAT_XREF", jdbc_url, jdbc_props)

merge_sql = f"""
MERGE {WebProvDirOwner}.PROV_DIR_DM_SPEC_CAT_XREF as T
USING STAGING.EdwDmProvDirDmSpecCatXrefLoad_odbc_PROV_DIR_DM_SPEC_CAT_XREF_out_temp as S
    ON (
        T.PROV_SPEC_CD = S.PROV_SPEC_CD
        AND T.PROV_CAT_CD = S.PROV_CAT_CD
        AND T.SEQ_NO = S.SEQ_NO
    )
WHEN NOT MATCHED THEN
    INSERT (
        PROV_SPEC_CD,
        PROV_CAT_CD,
        SEQ_NO,
        WEB_SRCH_IN,
        USER_ID,
        LAST_UPDT_DT
    )
    VALUES (
        S.PROV_SPEC_CD,
        S.PROV_CAT_CD,
        S.SEQ_NO,
        S.WEB_SRCH_IN,
        S.USER_ID,
        S.LAST_UPDT_DT
    );
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)

schema_odbc_PROV_DIR_DM_SPEC_CAT_XREF_out_rej = StructType([
    StructField("PROV_SPEC_CD", StringType(), True),
    StructField("PROV_CAT_CD", StringType(), True),
    StructField("SEQ_NO", IntegerType(), True),
    StructField("WEB_SRCH_IN", StringType(), True),
    StructField("USER_ID", StringType(), True),
    StructField("LAST_UPDT_DT", TimestampType(), True),
    StructField("ERRORCODE", StringType(), True),
    StructField("ERRORTEXT", StringType(), True)
])

df_odbc_PROV_DIR_DM_SPEC_CAT_XREF_out_rej = spark.createDataFrame([], schema_odbc_PROV_DIR_DM_SPEC_CAT_XREF_out_rej)

df_seq_PROV_DIR_DM_PROV_SPEC_CAT_XREF_csv_rej = df_odbc_PROV_DIR_DM_SPEC_CAT_XREF_out_rej.select(
    F.rpad(F.col("PROV_SPEC_CD"), <...>, " ").alias("PROV_SPEC_CD"),
    F.rpad(F.col("PROV_CAT_CD"), <...>, " ").alias("PROV_CAT_CD"),
    F.col("SEQ_NO").alias("SEQ_NO"),
    F.rpad(F.col("WEB_SRCH_IN"), 1, " ").alias("WEB_SRCH_IN"),
    F.rpad(F.col("USER_ID"), <...>, " ").alias("USER_ID"),
    F.col("LAST_UPDT_DT").alias("LAST_UPDT_DT"),
    F.rpad(F.col("ERRORCODE"), <...>, " ").alias("ERRORCODE"),
    F.rpad(F.col("ERRORTEXT"), <...>, " ").alias("ERRORTEXT")
)

write_files(
    df_seq_PROV_DIR_DM_PROV_SPEC_CAT_XREF_csv_rej,
    f"{adls_path}/load/PROV_DIR_DM_SPEC_CAT_XREF_Rej.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)