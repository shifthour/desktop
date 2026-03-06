# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC MODIFICATIONS:                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        
# MAGIC                                                                                                                                                                                                                                                         
# MAGIC DEVELOPER                         DATE                PROJECT                                             DESCRIPTION                                                          DATASTAGE                                       CODE                                    DATE                     
# MAGIC                                                                                                                                                                                                                         ENVIRONMENT                                  REVIEWER                           REVIEW         
# MAGIC -----------------------------------------      ------------------        -----------------------------------------------              -------------------------------------------------------------------------         -----------------------------------------------             ------------------------------                 -------------------
# MAGIC Rajasekhar Mangalampally    06/11/2013       5114                                                      Original Programming                                                EnterpriseWrhsDevl                               Jag Yelavarthi                       2013-08-25
# MAGIC                                                                                                                                       (Server to Parallel Conversion)

# MAGIC Read the Load ready file created in EdwDmProvDirDMWebProvCatExtr Job
# MAGIC Load rejects are redirected into a Reject file
# MAGIC 
# MAGIC This file is an append
# MAGIC Load file created in the previous job will be loaded into the target SQLServer PROV_DIR_DM_WEB_PROV_CAT table here. 
# MAGIC 
# MAGIC Load Type; "Truncate then Insert" .
# MAGIC Copy For Buffer
# MAGIC Job Name: 
# MAGIC EdwDmProvDirDmWebProvCatLoad
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


WebProvDirOwner = get_widget_value('WebProvDirOwner','')
webprovdir_secret_name = get_widget_value('webprovdir_secret_name','')
ProvDirArraySize = get_widget_value('ProvDirArraySize','')
ProvDirRecordCount = get_widget_value('ProvDirRecordCount','')

schema_seq_PROV_DIR_DM_WEB_PROV_CAT_csv_load = StructType([
    StructField("PROV_CAT_CD", StringType(), True),
    StructField("PROV_CAT_NM", StringType(), True),
    StructField("PROV_CAT_TYP_CD", StringType(), True),
    StructField("PROV_CAT_TYP_NM", StringType(), True),
    StructField("WEB_SRCH_IN", StringType(), True),
    StructField("USER_ID", StringType(), True),
    StructField("LAST_UPDT_DT", TimestampType(), True)
])

df_seq_PROV_DIR_DM_WEB_PROV_CAT_csv_load = spark.read \
    .option("delimiter", ",") \
    .option("quote", "^") \
    .schema(schema_seq_PROV_DIR_DM_WEB_PROV_CAT_csv_load) \
    .csv(f"{adls_path}/load/PROV_DIR_DM_WEB_PROV_CAT.dat")

df_cpy_forBuffer = df_seq_PROV_DIR_DM_WEB_PROV_CAT_csv_load.select(
    F.col("PROV_CAT_CD").alias("PROV_CAT_CD"),
    F.col("PROV_CAT_NM").alias("PROV_CAT_NM"),
    F.col("PROV_CAT_TYP_CD").alias("PROV_CAT_TYP_CD"),
    F.col("PROV_CAT_TYP_NM").alias("PROV_CAT_TYP_NM"),
    F.col("WEB_SRCH_IN").alias("WEB_SRCH_IN"),
    F.col("USER_ID").alias("USER_ID"),
    F.col("LAST_UPDT_DT").alias("LAST_UPDT_DT")
)

df_odbc_PROV_DIR_DM_WEB_PROV_CAT_out = df_cpy_forBuffer \
    .withColumn("PROV_CAT_CD", F.rpad(F.col("PROV_CAT_CD"), F.lit(<...>), F.lit(" "))) \
    .withColumn("PROV_CAT_NM", F.rpad(F.col("PROV_CAT_NM"), F.lit(<...>), F.lit(" "))) \
    .withColumn("PROV_CAT_TYP_CD", F.rpad(F.col("PROV_CAT_TYP_CD"), F.lit(<...>), F.lit(" "))) \
    .withColumn("PROV_CAT_TYP_NM", F.rpad(F.col("PROV_CAT_TYP_NM"), F.lit(<...>), F.lit(" "))) \
    .withColumn("WEB_SRCH_IN", F.rpad(F.col("WEB_SRCH_IN"), F.lit(1), F.lit(" "))) \
    .withColumn("USER_ID", F.rpad(F.col("USER_ID"), F.lit(<...>), F.lit(" ")))

jdbc_url, jdbc_props = get_db_config(webprovdir_secret_name)

drop_sql = "DROP TABLE IF EXISTS STAGING.EdwDmProvDirDmWebProvCatLoad_odbc_PROV_DIR_DM_WEB_PROV_CAT_out_temp"
execute_dml(drop_sql, jdbc_url, jdbc_props)

df_odbc_PROV_DIR_DM_WEB_PROV_CAT_out.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.EdwDmProvDirDmWebProvCatLoad_odbc_PROV_DIR_DM_WEB_PROV_CAT_out_temp") \
    .mode("append") \
    .save()

merge_sql = f"""
MERGE INTO {WebProvDirOwner}.PROV_DIR_DM_WEB_PROV_CAT AS T
USING STAGING.EdwDmProvDirDmWebProvCatLoad_odbc_PROV_DIR_DM_WEB_PROV_CAT_out_temp AS S
ON T.PROV_CAT_CD = S.PROV_CAT_CD
WHEN MATCHED THEN
  UPDATE SET
    T.PROV_CAT_NM = S.PROV_CAT_NM,
    T.PROV_CAT_TYP_CD = S.PROV_CAT_TYP_CD,
    T.PROV_CAT_TYP_NM = S.PROV_CAT_TYP_NM,
    T.WEB_SRCH_IN = S.WEB_SRCH_IN,
    T.USER_ID = S.USER_ID,
    T.LAST_UPDT_DT = S.LAST_UPDT_DT
WHEN NOT MATCHED THEN
  INSERT (PROV_CAT_CD, PROV_CAT_NM, PROV_CAT_TYP_CD, PROV_CAT_TYP_NM, WEB_SRCH_IN, USER_ID, LAST_UPDT_DT)
  VALUES (S.PROV_CAT_CD, S.PROV_CAT_NM, S.PROV_CAT_TYP_CD, S.PROV_CAT_TYP_NM, S.WEB_SRCH_IN, S.USER_ID, S.LAST_UPDT_DT);
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

schema_rej = StructType([
    StructField("ERRORCODE", StringType(), True),
    StructField("ERRORTEXT", StringType(), True)
])

df_odbc_PROV_DIR_DM_WEB_PROV_CAT_out_rej = spark.createDataFrame([], schema_rej)

df_odbc_PROV_DIR_DM_WEB_PROV_CAT_out_rej = df_odbc_PROV_DIR_DM_WEB_PROV_CAT_out_rej \
    .withColumn("ERRORCODE", F.rpad(F.col("ERRORCODE"), F.lit(<...>), F.lit(" "))) \
    .withColumn("ERRORTEXT", F.rpad(F.col("ERRORTEXT"), F.lit(<...>), F.lit(" ")))

write_files(
    df_odbc_PROV_DIR_DM_WEB_PROV_CAT_out_rej,
    f"{adls_path}/load/PROV_DIR_DM_WEB_PROV_CAT_Rej.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)