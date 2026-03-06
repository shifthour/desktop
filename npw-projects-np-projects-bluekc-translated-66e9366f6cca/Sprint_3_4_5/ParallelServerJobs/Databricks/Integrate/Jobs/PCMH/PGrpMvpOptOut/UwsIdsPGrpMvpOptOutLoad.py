# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Called By: TreoIdsMbrPcpAttrbtnCntl
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                                DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER                       DATE                     PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------          ----------------------     ------------------------------        ---------------------------------------------------------------------------                                           ------------------------------    ------------------------------       --------------------
# MAGIC Karthik Chintalapani       2015 -12-18             5212                                 Original Program                                                                                        IntegrateDev1           Kalyan Neelam            2015-12-22

# MAGIC Copy Stage for buffer
# MAGIC Job to load the Target DB2 table P_GRP_MVP_OPT_OUT
# MAGIC Load rejects are redirected into a Reject file
# MAGIC Load file created in Extract job will be loaded into the target DB2 table P_GRP_MVP_OPT_OUT
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import col, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

schema_seq_P_GRP_MVP_OPT_OUT_csv_load = StructType([
    StructField("GRP_ID", StringType(), False),
    StructField("GRP_MVP_OPT_OUT_EFF_DT_SK", StringType(), False),
    StructField("GRP_MVP_OPT_OUT_TERM_DT_SK", StringType(), False),
    StructField("USER_ID", StringType(), False),
    StructField("LAST_UPDT_DTM", TimestampType(), False)
])

df_seq_P_GRP_MVP_OPT_OUT_csv_load = (
    spark.read
    .option("sep", ",")
    .option("quote", "^")
    .option("header", False)
    .option("nullValue", None)
    .schema(schema_seq_P_GRP_MVP_OPT_OUT_csv_load)
    .csv(f"{adls_path}/load/P_GRP_MVP_OPT_OUT.dat")
)

df_cpy_forBuffer = df_seq_P_GRP_MVP_OPT_OUT_csv_load.select(
    col("GRP_ID").alias("GRP_ID"),
    col("GRP_MVP_OPT_OUT_EFF_DT_SK").alias("GRP_MVP_OPT_OUT_EFF_DT_SK"),
    col("GRP_MVP_OPT_OUT_TERM_DT_SK").alias("GRP_MVP_OPT_OUT_TERM_DT_SK"),
    col("USER_ID").alias("USER_ID"),
    col("LAST_UPDT_DTM").alias("LAST_UPDT_DTM")
)

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

truncate_sql = f"TRUNCATE TABLE {IDSOwner}.P_GRP_MVP_OPT_OUT"
execute_dml(truncate_sql, jdbc_url, jdbc_props)

temp_table_name = "STAGING.UwsIdsPGrpMvpOptOutLoad_DB2_P_GRP_MVP_OPT_OUT_temp"
drop_temp_sql = f"DROP TABLE IF EXISTS {temp_table_name}"
execute_dml(drop_temp_sql, jdbc_url, jdbc_props)

df_cpy_forBuffer.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", temp_table_name) \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO {IDSOwner}.P_GRP_MVP_OPT_OUT AS T
USING {temp_table_name} AS S
ON T.GRP_ID = S.GRP_ID
   AND T.GRP_MVP_OPT_OUT_EFF_DT_SK = S.GRP_MVP_OPT_OUT_EFF_DT_SK
WHEN MATCHED THEN
  UPDATE SET
    T.GRP_MVP_OPT_OUT_TERM_DT_SK = S.GRP_MVP_OPT_OUT_TERM_DT_SK,
    T.USER_ID = S.USER_ID,
    T.LAST_UPDT_DTM = S.LAST_UPDT_DTM
WHEN NOT MATCHED THEN
  INSERT (GRP_ID, GRP_MVP_OPT_OUT_EFF_DT_SK, GRP_MVP_OPT_OUT_TERM_DT_SK, USER_ID, LAST_UPDT_DTM)
  VALUES (S.GRP_ID, S.GRP_MVP_OPT_OUT_EFF_DT_SK, S.GRP_MVP_OPT_OUT_TERM_DT_SK, S.USER_ID, S.LAST_UPDT_DTM);
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

reject_schema = StructType([
    StructField("GRP_ID", StringType(), False),
    StructField("GRP_MVP_OPT_OUT_EFF_DT_SK", StringType(), False),
    StructField("GRP_MVP_OPT_OUT_TERM_DT_SK", StringType(), False),
    StructField("USER_ID", StringType(), False),
    StructField("LAST_UPDT_DTM", TimestampType(), False),
    StructField("ERRORCODE", StringType(), False),
    StructField("ERRORTEXT", StringType(), False)
])

df_DB2_P_GRP_MVP_OPT_OUT_reject = spark.createDataFrame([], reject_schema)

df_DB2_P_GRP_MVP_OPT_OUT_reject_rpad = df_DB2_P_GRP_MVP_OPT_OUT_reject.select(
    rpad(col("GRP_ID"), 255, " ").alias("GRP_ID"),
    rpad(col("GRP_MVP_OPT_OUT_EFF_DT_SK"), 10, " ").alias("GRP_MVP_OPT_OUT_EFF_DT_SK"),
    rpad(col("GRP_MVP_OPT_OUT_TERM_DT_SK"), 10, " ").alias("GRP_MVP_OPT_OUT_TERM_DT_SK"),
    rpad(col("USER_ID"), 255, " ").alias("USER_ID"),
    col("LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
    rpad(col("ERRORCODE"), 255, " ").alias("ERRORCODE"),
    rpad(col("ERRORTEXT"), 255, " ").alias("ERRORTEXT")
)

write_files(
    df_DB2_P_GRP_MVP_OPT_OUT_reject_rpad,
    f"{adls_path}/load/P_GRP_MVP_OPT_OUT_Rej.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)