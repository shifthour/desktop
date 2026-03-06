# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Balkarn Gill               06/18/2013        5114                             Load DM Table PROV_DIR_DM_NTWK                               EnterpriseWrhsDevl                  Peter Marshall               9/3/2013

# MAGIC Job Name: EdwDmProvDirDmNtwkLoad
# MAGIC Read Load File created in the EdwDmProvDirDmNtwkExtr Job
# MAGIC Copy Stage for buffer
# MAGIC Update then Insert the PROV_DIR_DM_NTWK Data.
# MAGIC Load rejects are redirected into a Reject file
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


# Retrieve parameter values
WebProvDirOwner = get_widget_value('WebProvDirOwner','')
webprovdir_secret_name = get_widget_value('webprovdir_secret_name','')
ProvDirRecordCount = get_widget_value('ProvDirRecordCount','')
ProvDirArraySize = get_widget_value('ProvDirArraySize','')

# --------------------------------------------------------------------------------
# Stage: seq_PROV_DIR_DM_NTWK_csv_load (PxSequentialFile)
# --------------------------------------------------------------------------------
schema_seq_PROV_DIR_DM_NTWK_csv_load = StructType([
    StructField("NTWK_ID", StringType(), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("NTWK_DIR_CD", StringType(), False),
    StructField("NTWK_DIR_NM", StringType(), False),
    StructField("NTWK_NM", StringType(), True),
    StructField("NTWK_SH_NM", StringType(), False),
    StructField("NTWK_TYP_CD", StringType(), False),
    StructField("NTWK_TYP_NM", StringType(), True),
    StructField("PROD_SH_NM", StringType(), False),
    StructField("USER_ID", StringType(), False),
    StructField("LAST_UPDT_DT", TimestampType(), False)
])

df_seq_PROV_DIR_DM_NTWK_csv_load = (
    spark.read.format("csv")
    .option("header", "false")
    .option("sep", ",")
    .option("quote", "^")
    .option("escape", "^")
    .option("nullValue", None)
    .schema(schema_seq_PROV_DIR_DM_NTWK_csv_load)
    .load(f"{adls_path}/load/PROV_DIR_DM_NTWK.dat")
)

# --------------------------------------------------------------------------------
# Stage: cpy_forBuffer (PxCopy)
# --------------------------------------------------------------------------------
df_cpy_forBuffer = df_seq_PROV_DIR_DM_NTWK_csv_load.select(
    F.col("NTWK_ID").alias("NTWK_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("NTWK_DIR_CD").alias("NTWK_DIR_CD"),
    F.col("NTWK_DIR_NM").alias("NTWK_DIR_NM"),
    F.col("NTWK_NM").alias("NTWK_NM"),
    F.col("NTWK_SH_NM").alias("NTWK_SH_NM"),
    F.col("NTWK_TYP_CD").alias("NTWK_TYP_CD"),
    F.col("NTWK_TYP_NM").alias("NTWK_TYP_NM"),
    F.col("PROD_SH_NM").alias("PROD_SH_NM"),
    F.col("USER_ID").alias("USER_ID"),
    F.col("LAST_UPDT_DT").alias("LAST_UPDT_DT")
)

# --------------------------------------------------------------------------------
# Stage: ODBC_PROV_DIR_DM_NTWK_out (ODBCConnectorPX) - Upsert to #$WebProvDirOwner#.PROV_DIR_DM_NTWK
# --------------------------------------------------------------------------------
# Prepare final columns (rpad for varchar/char) before writing to staging table
df_odbc_prep = df_cpy_forBuffer.select(
    F.rpad(F.col("NTWK_ID"), <...>, " ").alias("NTWK_ID"),
    F.rpad(F.col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    F.rpad(F.col("NTWK_DIR_CD"), <...>, " ").alias("NTWK_DIR_CD"),
    F.rpad(F.col("NTWK_DIR_NM"), <...>, " ").alias("NTWK_DIR_NM"),
    F.rpad(F.col("NTWK_NM"), <...>, " ").alias("NTWK_NM"),
    F.rpad(F.col("NTWK_SH_NM"), <...>, " ").alias("NTWK_SH_NM"),
    F.rpad(F.col("NTWK_TYP_CD"), <...>, " ").alias("NTWK_TYP_CD"),
    F.rpad(F.col("NTWK_TYP_NM"), <...>, " ").alias("NTWK_TYP_NM"),
    F.rpad(F.col("PROD_SH_NM"), <...>, " ").alias("PROD_SH_NM"),
    F.rpad(F.col("USER_ID"), <...>, " ").alias("USER_ID"),
    F.col("LAST_UPDT_DT").alias("LAST_UPDT_DT")
)

jdbc_url, jdbc_props = get_db_config(webprovdir_secret_name)
execute_dml(
    "DROP TABLE IF EXISTS STAGING.EdwDmProvDirDmNtwkLoad_ODBC_PROV_DIR_DM_NTWK_out_temp",
    jdbc_url,
    jdbc_props
)

(
    df_odbc_prep
    .write.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", "STAGING.EdwDmProvDirDmNtwkLoad_ODBC_PROV_DIR_DM_NTWK_out_temp")
    .mode("overwrite")
    .save()
)

merge_sql = """
MERGE INTO #$WebProvDirOwner#.PROV_DIR_DM_NTWK AS Target
USING STAGING.EdwDmProvDirDmNtwkLoad_ODBC_PROV_DIR_DM_NTWK_out_temp AS Source
ON Target.NTWK_ID = Source.NTWK_ID
AND Target.SRC_SYS_CD = Source.SRC_SYS_CD
WHEN MATCHED THEN
  UPDATE SET
    Target.NTWK_DIR_CD = Source.NTWK_DIR_CD,
    Target.NTWK_DIR_NM = Source.NTWK_DIR_NM,
    Target.NTWK_NM = Source.NTWK_NM,
    Target.NTWK_SH_NM = Source.NTWK_SH_NM,
    Target.NTWK_TYP_CD = Source.NTWK_TYP_CD,
    Target.NTWK_TYP_NM = Source.NTWK_TYP_NM,
    Target.PROD_SH_NM = Source.PROD_SH_NM,
    Target.USER_ID = Source.USER_ID,
    Target.LAST_UPDT_DT = Source.LAST_UPDT_DT
WHEN NOT MATCHED THEN
  INSERT (NTWK_ID, SRC_SYS_CD, NTWK_DIR_CD, NTWK_DIR_NM, NTWK_NM, NTWK_SH_NM, NTWK_TYP_CD, NTWK_TYP_NM, PROD_SH_NM, USER_ID, LAST_UPDT_DT)
  VALUES (Source.NTWK_ID, Source.SRC_SYS_CD, Source.NTWK_DIR_CD, Source.NTWK_DIR_NM, Source.NTWK_NM, Source.NTWK_SH_NM, Source.NTWK_TYP_CD, Source.NTWK_TYP_NM, Source.PROD_SH_NM, Source.USER_ID, Source.LAST_UPDT_DT)
;
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)

# --------------------------------------------------------------------------------
# Stage: seq_PROV_DIR_DM_NTWK_csv_rej (PxSequentialFile) - DB Reject Link
# --------------------------------------------------------------------------------
# In DataStage, this would capture rejected rows including ERRORCODE, ERRORTEXT, plus all input columns.
# Spark cannot natively capture DB rejects this way; we create an empty DataFrame to illustrate.
schema_seq_PROV_DIR_DM_NTWK_csv_rej = StructType([
    StructField("NTWK_ID", StringType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("NTWK_DIR_CD", StringType(), True),
    StructField("NTWK_DIR_NM", StringType(), True),
    StructField("NTWK_NM", StringType(), True),
    StructField("NTWK_SH_NM", StringType(), True),
    StructField("NTWK_TYP_CD", StringType(), True),
    StructField("NTWK_TYP_NM", StringType(), True),
    StructField("PROD_SH_NM", StringType(), True),
    StructField("USER_ID", StringType(), True),
    StructField("LAST_UPDT_DT", TimestampType(), True),
    StructField("ERRORCODE", StringType(), True),
    StructField("ERRORTEXT", StringType(), True)
])

df_seq_PROV_DIR_DM_NTWK_csv_rej = spark.createDataFrame([], schema_seq_PROV_DIR_DM_NTWK_csv_rej).select(
    F.rpad(F.col("NTWK_ID"), <...>, " ").alias("NTWK_ID"),
    F.rpad(F.col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    F.rpad(F.col("NTWK_DIR_CD"), <...>, " ").alias("NTWK_DIR_CD"),
    F.rpad(F.col("NTWK_DIR_NM"), <...>, " ").alias("NTWK_DIR_NM"),
    F.rpad(F.col("NTWK_NM"), <...>, " ").alias("NTWK_NM"),
    F.rpad(F.col("NTWK_SH_NM"), <...>, " ").alias("NTWK_SH_NM"),
    F.rpad(F.col("NTWK_TYP_CD"), <...>, " ").alias("NTWK_TYP_CD"),
    F.rpad(F.col("NTWK_TYP_NM"), <...>, " ").alias("NTWK_TYP_NM"),
    F.rpad(F.col("PROD_SH_NM"), <...>, " ").alias("PROD_SH_NM"),
    F.rpad(F.col("USER_ID"), <...>, " ").alias("USER_ID"),
    F.col("LAST_UPDT_DT").alias("LAST_UPDT_DT"),
    F.rpad(F.col("ERRORCODE"), <...>, " ").alias("ERRORCODE"),
    F.rpad(F.col("ERRORTEXT"), <...>, " ").alias("ERRORTEXT")
)

write_files(
    df_seq_PROV_DIR_DM_NTWK_csv_rej,
    f"{adls_path}/load/PROV_DIR_DM_NTWK_Rej.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)