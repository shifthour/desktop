# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC MODIFICATIONS:                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        
# MAGIC                                                                                                                                                                                                                                                         
# MAGIC DEVELOPER                         DATE                PROJECT                                             DESCRIPTION                                                                  DATASTAGE                                      CODE                                     DATE                                          
# MAGIC                                                                                                                                                                                                                                 ENVIRONMENT                                 REVIEWER                            REVIEW 
# MAGIC -----------------------------------------      ------------------        -----------------------------------------------              --------------------------------------------------------------------------------          -----------------------------------------------             ------------------------------                 -------------------
# MAGIC Rajasekhar Mangalampally    06/18/2013       5114-EDW Process Efficiencies             Original Programming                                                        EnterpriseWrhsDevl                            Jag Yelavarthi                        2013-08-29
# MAGIC                                                                                                                                         (Server to Parallel Conversion)

# MAGIC Read the Load ready file created in EdwDmProvDirDmProvTypCtExtr Job
# MAGIC Load rejects are redirected into a Reject file
# MAGIC 
# MAGIC This file is an append
# MAGIC Load file created in the previous job will be loaded into the target SQL Server PROV_DIR_DM_PROV_TYP_CT table here. 
# MAGIC 
# MAGIC Load Type; "Update then Insert" .
# MAGIC Copy For Buffer
# MAGIC Job Name: 
# MAGIC EdwProvDirDmProvTypCtLoad
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, rpad, lit
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


WebProvDirOwner = get_widget_value('WebProvDirOwner','')
webprovdir_secret_name = get_widget_value('webprovdir_secret_name','')
ProvDirArraySize = get_widget_value('ProvDirArraySize','')
ProvDirRecordCount = get_widget_value('ProvDirRecordCount','')

schema_seq_PROV_DIR_DM_PROV_TYP_CT_csv_load = StructType([
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PROV_TYP_CD", StringType(), False),
    StructField("NTWK_ID", StringType(), False),
    StructField("PROV_CT", IntegerType(), False)
])

df_seq_PROV_DIR_DM_PROV_TYP_CT_csv_load = (
    spark.read
    .option("header", False)
    .option("sep", ",")
    .option("quote", "^")
    .schema(schema_seq_PROV_DIR_DM_PROV_TYP_CT_csv_load)
    .csv(f"{adls_path}/load/PROV_DIR_DM_PROV_TYP_CT.dat")
)

df_cpy_forBuffer = df_seq_PROV_DIR_DM_PROV_TYP_CT_csv_load.select(
    col("SRC_SYS_CD"),
    col("PROV_TYP_CD"),
    col("NTWK_ID"),
    col("PROV_CT")
)

df_odbc_PROV_DIR_DM_PROV_TYP_CT_out = df_cpy_forBuffer.select(
    rpad(col("SRC_SYS_CD"), 256, " ").alias("SRC_SYS_CD"),
    rpad(col("PROV_TYP_CD"), 256, " ").alias("PROV_TYP_CD"),
    rpad(col("NTWK_ID"), 256, " ").alias("NTWK_ID"),
    col("PROV_CT")
)

jdbc_url, jdbc_props = get_db_config(webprovdir_secret_name)

drop_temp_sql = "DROP TABLE IF EXISTS STAGING.EdwDmProvDirDmProvTypCtLoad_odbc_PROV_DIR_DM_PROV_TYP_CT_out_temp"
execute_dml(drop_temp_sql, jdbc_url, jdbc_props)

df_odbc_PROV_DIR_DM_PROV_TYP_CT_out.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.EdwDmProvDirDmProvTypCtLoad_odbc_PROV_DIR_DM_PROV_TYP_CT_out_temp") \
    .mode("append") \
    .save()

merge_sql = """
MERGE INTO #$WebProvDirOwner#.PROV_DIR_DM_PROV_TYP_CT AS T
USING STAGING.EdwDmProvDirDmProvTypCtLoad_odbc_PROV_DIR_DM_PROV_TYP_CT_out_temp AS S
ON 
    T.SRC_SYS_CD = S.SRC_SYS_CD
    AND T.PROV_TYP_CD = S.PROV_TYP_CD
    AND T.NTWK_ID = S.NTWK_ID
WHEN MATCHED THEN
    UPDATE SET T.PROV_CT = S.PROV_CT
WHEN NOT MATCHED THEN
    INSERT (SRC_SYS_CD, PROV_TYP_CD, NTWK_ID, PROV_CT)
    VALUES (S.SRC_SYS_CD, S.PROV_TYP_CD, S.NTWK_ID, S.PROV_CT);
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

schema_seq_PROV_DIR_DM_PROV_TYP_CT_csv_rej = StructType([
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("PROV_TYP_CD", StringType(), True),
    StructField("NTWK_ID", StringType(), True),
    StructField("PROV_CT", IntegerType(), True),
    StructField("ERRORCODE", StringType(), True),
    StructField("ERRORTEXT", StringType(), True)
])

df_temp_rej = spark.createDataFrame([], schema_seq_PROV_DIR_DM_PROV_TYP_CT_csv_rej)

df_seq_PROV_DIR_DM_PROV_TYP_CT_csv_rej = df_temp_rej.select(
    rpad(col("SRC_SYS_CD"), 256, " ").alias("SRC_SYS_CD"),
    rpad(col("PROV_TYP_CD"), 256, " ").alias("PROV_TYP_CD"),
    rpad(col("NTWK_ID"), 256, " ").alias("NTWK_ID"),
    col("PROV_CT").alias("PROV_CT"),
    rpad(col("ERRORCODE"), 256, " ").alias("ERRORCODE"),
    rpad(col("ERRORTEXT"), 256, " ").alias("ERRORTEXT")
)

write_files(
    df_seq_PROV_DIR_DM_PROV_TYP_CT_csv_rej,
    f"{adls_path}/load/PROV_DIR_DM_PROV_TYP_CT_Rej.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)