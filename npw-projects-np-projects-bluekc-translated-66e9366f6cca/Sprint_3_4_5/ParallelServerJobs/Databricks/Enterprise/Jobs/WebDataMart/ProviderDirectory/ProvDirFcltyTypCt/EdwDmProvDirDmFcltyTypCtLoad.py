# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Balkarn Gill               06/11/2013        5114                             Load DM Table PROV_DIR_DM_FCLTY_TYP_CT                              EnterpriseWrhsDevl

# MAGIC Job Name: EdwDmProvDirDmFcltyTypCtLoad
# MAGIC Read Load File created in the EdwDmProvDirDmFcltyTypCtExtr Job
# MAGIC Copy Stage for buffer
# MAGIC Update then Insert the PROV_DIR_DM_FCLTY_TYP_CT Data.
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
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


WebProvDirOwner = get_widget_value('WebProvDirOwner','')
ProvDirRecordCount = get_widget_value('ProvDirRecordCount','')
ProvDirArraySize = get_widget_value('ProvDirArraySize','')
webprovdir_secret_name = get_widget_value('webprovdir_secret_name','')

schema_seq_PROV_DIR_DM_FCLTY_TYP_CT = StructType([
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("FCLTY_TYP_CD", StringType(), False),
    StructField("NTWK_ID", StringType(), False),
    StructField("PROV_CT", IntegerType(), False)
])

df_seq_PROV_DIR_DM_FCLTY_TYP_CT_csv_load = (
    spark.read
    .option("delimiter", ",")
    .option("quote", "^")
    .option("header", "false")
    .schema(schema_seq_PROV_DIR_DM_FCLTY_TYP_CT)
    .csv(f"{adls_path}/load/PROV_DIR_DM_FCLTY_TYP_CT.dat")
)

df_cpy_forBuffer = df_seq_PROV_DIR_DM_FCLTY_TYP_CT_csv_load.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("FCLTY_TYP_CD").alias("FCLTY_TYP_CD"),
    F.col("NTWK_ID").alias("NTWK_ID"),
    F.col("PROV_CT").alias("PROV_CT")
)

jdbc_url, jdbc_props = get_db_config(webprovdir_secret_name)

execute_dml(
    "DROP TABLE IF EXISTS STAGING.EdwDmProvDirDmFcltyTypCtLoad_ODBC_PROV_DIR_DM_FCLTY_TYP_CT_out_temp",
    jdbc_url,
    jdbc_props
)

df_merge_input = df_cpy_forBuffer.select(
    F.rpad("SRC_SYS_CD", <...>, " ").alias("SRC_SYS_CD"),
    F.rpad("FCLTY_TYP_CD", <...>, " ").alias("FCLTY_TYP_CD"),
    F.rpad("NTWK_ID", <...>, " ").alias("NTWK_ID"),
    "PROV_CT"
)

(
    df_merge_input
    .write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", "STAGING.EdwDmProvDirDmFcltyTypCtLoad_ODBC_PROV_DIR_DM_FCLTY_TYP_CT_out_temp")
    .mode("overwrite")
    .save()
)

merge_sql = f"""
MERGE INTO {WebProvDirOwner}.PROV_DIR_DM_FCLTY_TYP_CT AS T
USING STAGING.EdwDmProvDirDmFcltyTypCtLoad_ODBC_PROV_DIR_DM_FCLTY_TYP_CT_out_temp AS S
ON
    T.SRC_SYS_CD = S.SRC_SYS_CD
    AND T.FCLTY_TYP_CD = S.FCLTY_TYP_CD
    AND T.NTWK_ID = S.NTWK_ID
WHEN MATCHED THEN
    UPDATE SET T.PROV_CT = S.PROV_CT
WHEN NOT MATCHED THEN
    INSERT (SRC_SYS_CD, FCLTY_TYP_CD, NTWK_ID, PROV_CT)
    VALUES (S.SRC_SYS_CD, S.FCLTY_TYP_CD, S.NTWK_ID, S.PROV_CT);
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)

df_seq_PROV_DIR_DM_FCLTY_TYP_CT_csv_rej = spark.createDataFrame(
    [],
    StructType([
        StructField("ERRORCODE", StringType(), True),
        StructField("ERRORTEXT", StringType(), True)
    ])
)

write_files(
    df_seq_PROV_DIR_DM_FCLTY_TYP_CT_csv_rej,
    f"{adls_path}/load/PROV_DIR_DM_FCLTY_TYP_CT_Rej.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)