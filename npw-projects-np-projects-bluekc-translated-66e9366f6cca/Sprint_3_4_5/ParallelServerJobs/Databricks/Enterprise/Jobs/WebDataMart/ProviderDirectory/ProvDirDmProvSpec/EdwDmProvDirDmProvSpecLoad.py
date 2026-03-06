# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC DEVELOPER                         DATE                PROJECT                                             DESCRIPTION                                                          DATASTAGE  ENVIRONMENT         CODE REVIEWER                 REVIEW  DATE
# MAGIC -----------------------------------------      ------------------        -----------------------------------------------              -------------------------------------------------------------------------         -----------------------------------------------             ------------------------------                 -------------------
# MAGIC Shiva Devagiri                          06/25/2013       5114                                                   Original Programming                                               EterpriseWrhsDevl                                  Jag Yelavarthi                        2013-09-01
# MAGIC                                                                                                                                        (Server to Parallel Conversion)

# MAGIC Load file created in the previous job will be loaded into the target table here. 
# MAGIC 
# MAGIC Load Type; "Update then Insert"
# MAGIC Read Load File created in the EdwProvDirDmProvSpecExtr Job
# MAGIC Load rejects are redirected into a Reject file
# MAGIC Copy For Buffer
# MAGIC Job Name: 
# MAGIC EdwProvDirDmProvSpecLoad
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import col, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


# Parameters
WebProvDirOwner = get_widget_value('WebProvDirOwner','')
ProvDirArraySize = get_widget_value('ProvDirArraySize','')
ProvDirRecordCount = get_widget_value('ProvDirRecordCount','')
webprovdir_secret_name = get_widget_value('webprovdir_secret_name','')

# Read from seq_PROV_DIR_DM_PROV_SPEC_csv_load
schema_seq_PROV_DIR_DM_PROV_SPEC_csv_load = StructType([
    StructField("PROV_SPEC_CD", StringType(), False),
    StructField("PROV_SPEC_NM", StringType(), False),
    StructField("LAST_UPDT_DT", TimestampType(), False)
])
df_seq_PROV_DIR_DM_PROV_SPEC_csv_load = (
    spark.read
    .option("delimiter", ",")
    .option("quote", "^")
    .option("header", False)
    .schema(schema_seq_PROV_DIR_DM_PROV_SPEC_csv_load)
    .csv(f"{adls_path}/load/PROV_DIR_DM_PROV_SPEC.dat")
)

# cpy_forBuffer
df_cpy_forBuffer = df_seq_PROV_DIR_DM_PROV_SPEC_csv_load.repartition("PROV_SPEC_CD")

# Prepare DataFrame for odbc_PROV_DIR_DM_PROV_SPEC_out
df_odbc_PROV_DIR_DM_PROV_SPEC_out = df_cpy_forBuffer.select(
    col("PROV_SPEC_CD").alias("PROV_SPEC_CD"),
    col("PROV_SPEC_NM").alias("PROV_SPEC_NM"),
    col("LAST_UPDT_DT").alias("LAST_UPDT_DT")
)

# Write to database (Update then Insert) via merge
jdbc_url, jdbc_props = get_db_config(webprovdir_secret_name)

execute_dml(
    f"DROP TABLE IF EXISTS STAGING.EdwDmProvDirDmProvSpecLoad_odbc_PROV_DIR_DM_PROV_SPEC_out_temp",
    jdbc_url,
    jdbc_props
)

df_odbc_PROV_DIR_DM_PROV_SPEC_out_to_temp = df_odbc_PROV_DIR_DM_PROV_SPEC_out.select(
    "PROV_SPEC_CD",
    "PROV_SPEC_NM",
    "LAST_UPDT_DT"
)

(
    df_odbc_PROV_DIR_DM_PROV_SPEC_out_to_temp
    .write
    .jdbc(
        url=jdbc_url,
        table="STAGING.EdwDmProvDirDmProvSpecLoad_odbc_PROV_DIR_DM_PROV_SPEC_out_temp",
        mode="overwrite",
        properties=jdbc_props
    )
)

merge_sql = (
    f"MERGE INTO {WebProvDirOwner}.PROV_DIR_DM_PROV_SPEC AS T "
    f"USING STAGING.EdwDmProvDirDmProvSpecLoad_odbc_PROV_DIR_DM_PROV_SPEC_out_temp AS S "
    f"ON T.PROV_SPEC_CD = S.PROV_SPEC_CD "
    f"WHEN MATCHED THEN UPDATE SET "
    f"T.PROV_SPEC_CD = S.PROV_SPEC_CD, "
    f"T.PROV_SPEC_NM = S.PROV_SPEC_NM, "
    f"T.LAST_UPDT_DT = S.LAST_UPDT_DT "
    f"WHEN NOT MATCHED THEN "
    f"INSERT (PROV_SPEC_CD, PROV_SPEC_NM, LAST_UPDT_DT) "
    f"VALUES (S.PROV_SPEC_CD, S.PROV_SPEC_NM, S.LAST_UPDT_DT);"
)
execute_dml(merge_sql, jdbc_url, jdbc_props)

# Simulate reject link as a DataFrame (row-level reject capture is not directly feasible in Spark)
df_odbc_PROV_DIR_DM_PROV_SPEC_rej = (
    df_odbc_PROV_DIR_DM_PROV_SPEC_out
    .withColumn("ERRORCODE", col("PROV_SPEC_CD").substr(-1, 0).cast(StringType()))  # Creating a dummy column
    .withColumn("ERRORTEXT", col("PROV_SPEC_NM").substr(-1, 0).cast(StringType()))  # Creating a dummy column
)

# Write rejects to seq_PROV_DIR_DM_PROV_SPEC_csv_rej
df_rej_final = df_odbc_PROV_DIR_DM_PROV_SPEC_rej.select(
    rpad(col("PROV_SPEC_CD"), 255, ' ').alias("PROV_SPEC_CD"),
    rpad(col("PROV_SPEC_NM"), 255, ' ').alias("PROV_SPEC_NM"),
    col("LAST_UPDT_DT").alias("LAST_UPDT_DT"),
    rpad(col("ERRORCODE"), 255, ' ').alias("ERRORCODE"),
    rpad(col("ERRORTEXT"), 255, ' ').alias("ERRORTEXT")
)

write_files(
    df_rej_final,
    f"{adls_path}/load/PROV_DIR_DM_PROV_SPEC_Rej.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)