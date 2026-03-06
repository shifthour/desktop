# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC MODIFICATIONS:                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        
# MAGIC                                                                                                                                                                                                                                                         
# MAGIC DEVELOPER                           DATE                PROJECT                                             DESCRIPTION                                                      DATASTAGE  ENVIRONMENT         CODE REVIEWER                 DATE REVIEW
# MAGIC -------------------------------------------      -------------------      ---------------------------------------------                 -------------------------------------------------------------------           -------------------------------------------------          ------------------------------                  -------------------
# MAGIC Rajasekhar Mangalampally       06/12/2013     5114                                                       Original Programming                                                EnterpriseWrhsDevl                          Jag Yelavarthi                         2013-08-25
# MAGIC                                                                                                                                         (Server to Parallel Conversion)

# MAGIC Read the Load ready file created in EdwDmProvDirDmProvNtwkExtr Job
# MAGIC Load rejects are redirected into a Reject file
# MAGIC 
# MAGIC This file is an append
# MAGIC Load file created in the previous job will be loaded into the target SQL Server table PROV_DIR_DM_PROV_NTWK here. 
# MAGIC 
# MAGIC Load Type; "Update then Insert"
# MAGIC Copy For Buffer
# MAGIC Job Name: EdwDmProvDirDmProvNtwkLoad
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


WebProvDirOwner = get_widget_value('WebProvDirOwner','')
webprovdir_secret_name = get_widget_value('webprovdir_secret_name','')
ProvDirRecordCount = get_widget_value('ProvDirRecordCount','')
ProvDirArraySize = get_widget_value('ProvDirArraySize','')

schema_seq_PROV_DIR_DM_PROV_NTWK_csv_load = StructType([
    StructField("NTWK_ID", StringType(), True),
    StructField("PROV_ID", StringType(), True),
    StructField("PROV_NTWK_PFX_ID", StringType(), True),
    StructField("PROV_NTWK_EFF_DT", TimestampType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("PROV_NTWK_ACPTNG_MCAID_PATN_IN", StringType(), True),
    StructField("PROV_NTWK_ACPTNG_MCARE_PATN_IN", StringType(), True),
    StructField("PROV_NTWK_AUTO_PCP_ASGMT_IN", StringType(), True),
    StructField("PROV_NTWK_DIR_IN", StringType(), True),
    StructField("PROV_NTWK_ACPTNG_PATN_IN", StringType(), True),
    StructField("PROV_NTWK_GNDR_ACPTD_CD", StringType(), True),
    StructField("PROV_NTWK_GNDR_ACPTD_NM", StringType(), True),
    StructField("PROV_NTWK_MAX_PATN_AGE", IntegerType(), True),
    StructField("PROV_NTWK_MIN_PATN_AGE", IntegerType(), True),
    StructField("PROV_NTWK_PCP_IN", StringType(), True),
    StructField("PROV_NTWK_PROV_DIR_IN", StringType(), True),
    StructField("NTWK_NM", StringType(), True),
    StructField("NTWK_SH_NM", StringType(), True),
    StructField("PROV_NTWK_TERM_DT", TimestampType(), True),
    StructField("PROV_NTWK_CNTR_IN", StringType(), True),
    StructField("LAST_UPDT_DT", TimestampType(), True),
    StructField("PCMH_IN", StringType(), True)
])

df_seq_PROV_DIR_DM_PROV_NTWK_csv_load = (
    spark.read
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "^")
    .option("nullValue", None)
    .schema(schema_seq_PROV_DIR_DM_PROV_NTWK_csv_load)
    .csv(f"{adls_path}/load/PROV_DIR_DM_PROV_NTWK.dat")
)

df_cpy_forBuffer = df_seq_PROV_DIR_DM_PROV_NTWK_csv_load.select(
    col("NTWK_ID").alias("NTWK_ID"),
    col("PROV_ID").alias("PROV_ID"),
    col("PROV_NTWK_PFX_ID").alias("PROV_NTWK_PFX_ID"),
    col("PROV_NTWK_EFF_DT").alias("PROV_NTWK_EFF_DT"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("PROV_NTWK_ACPTNG_MCAID_PATN_IN").alias("PROV_NTWK_ACPTNG_MCAID_PATN_IN"),
    col("PROV_NTWK_ACPTNG_MCARE_PATN_IN").alias("PROV_NTWK_ACPTNG_MCARE_PATN_IN"),
    col("PROV_NTWK_AUTO_PCP_ASGMT_IN").alias("PROV_NTWK_AUTO_PCP_ASGMT_IN"),
    col("PROV_NTWK_DIR_IN").alias("PROV_NTWK_DIR_IN"),
    col("PROV_NTWK_ACPTNG_PATN_IN").alias("PROV_NTWK_ACPTNG_PATN_IN"),
    col("PROV_NTWK_GNDR_ACPTD_CD").alias("PROV_NTWK_GNDR_ACPTD_CD"),
    col("PROV_NTWK_GNDR_ACPTD_NM").alias("PROV_NTWK_GNDR_ACPTD_NM"),
    col("PROV_NTWK_MAX_PATN_AGE").alias("PROV_NTWK_MAX_PATN_AGE"),
    col("PROV_NTWK_MIN_PATN_AGE").alias("PROV_NTWK_MIN_PATN_AGE"),
    col("PROV_NTWK_PCP_IN").alias("PROV_NTWK_PCP_IN"),
    col("PROV_NTWK_PROV_DIR_IN").alias("PROV_NTWK_PROV_DIR_IN"),
    col("NTWK_NM").alias("NTWK_NM"),
    col("NTWK_SH_NM").alias("NTWK_SH_NM"),
    col("PROV_NTWK_TERM_DT").alias("PROV_NTWK_TERM_DT"),
    col("PROV_NTWK_CNTR_IN").alias("PROV_NTWK_CNTR_IN"),
    col("LAST_UPDT_DT").alias("LAST_UPDT_DT"),
    col("PCMH_IN").alias("PCMH_IN")
)

df_odbc_PROV_DIR_DM_PROV_NTWK_out = df_cpy_forBuffer.select(
    rpad(col("NTWK_ID"), 256, " ").alias("NTWK_ID"),
    rpad(col("PROV_ID"), 256, " ").alias("PROV_ID"),
    rpad(col("PROV_NTWK_PFX_ID"), 256, " ").alias("PROV_NTWK_PFX_ID"),
    col("PROV_NTWK_EFF_DT").alias("PROV_NTWK_EFF_DT"),
    rpad(col("SRC_SYS_CD"), 256, " ").alias("SRC_SYS_CD"),
    rpad(col("PROV_NTWK_ACPTNG_MCAID_PATN_IN"), 1, " ").alias("PROV_NTWK_ACPTNG_MCAID_PATN_IN"),
    rpad(col("PROV_NTWK_ACPTNG_MCARE_PATN_IN"), 1, " ").alias("PROV_NTWK_ACPTNG_MCARE_PATN_IN"),
    rpad(col("PROV_NTWK_AUTO_PCP_ASGMT_IN"), 1, " ").alias("PROV_NTWK_AUTO_PCP_ASGMT_IN"),
    rpad(col("PROV_NTWK_DIR_IN"), 1, " ").alias("PROV_NTWK_DIR_IN"),
    rpad(col("PROV_NTWK_ACPTNG_PATN_IN"), 1, " ").alias("PROV_NTWK_ACPTNG_PATN_IN"),
    rpad(col("PROV_NTWK_GNDR_ACPTD_CD"), 256, " ").alias("PROV_NTWK_GNDR_ACPTD_CD"),
    rpad(col("PROV_NTWK_GNDR_ACPTD_NM"), 256, " ").alias("PROV_NTWK_GNDR_ACPTD_NM"),
    col("PROV_NTWK_MAX_PATN_AGE").alias("PROV_NTWK_MAX_PATN_AGE"),
    col("PROV_NTWK_MIN_PATN_AGE").alias("PROV_NTWK_MIN_PATN_AGE"),
    rpad(col("PROV_NTWK_PCP_IN"), 1, " ").alias("PROV_NTWK_PCP_IN"),
    rpad(col("PROV_NTWK_PROV_DIR_IN"), 1, " ").alias("PROV_NTWK_PROV_DIR_IN"),
    rpad(col("NTWK_NM"), 256, " ").alias("NTWK_NM"),
    rpad(col("NTWK_SH_NM"), 256, " ").alias("NTWK_SH_NM"),
    col("PROV_NTWK_TERM_DT").alias("PROV_NTWK_TERM_DT"),
    rpad(col("PROV_NTWK_CNTR_IN"), 1, " ").alias("PROV_NTWK_CNTR_IN"),
    col("LAST_UPDT_DT").alias("LAST_UPDT_DT"),
    rpad(col("PCMH_IN"), 1, " ").alias("PCMH_IN")
)

jdbc_url, jdbc_props = get_db_config(webprovdir_secret_name)
execute_dml(f"DROP TABLE IF EXISTS STAGING.EdwDmProvDirDmProvNtwkLoad_odbc_PROV_DIR_DM_PROV_NTWK_out_temp", jdbc_url, jdbc_props)

(
    df_odbc_PROV_DIR_DM_PROV_NTWK_out
    .write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", "STAGING.EdwDmProvDirDmProvNtwkLoad_odbc_PROV_DIR_DM_PROV_NTWK_out_temp")
    .mode("overwrite")
    .save()
)

merge_sql = f"""
MERGE {WebProvDirOwner}.PROV_DIR_DM_PROV_NTWK AS T
USING STAGING.EdwDmProvDirDmProvNtwkLoad_odbc_PROV_DIR_DM_PROV_NTWK_out_temp AS S
ON T.NTWK_ID = S.NTWK_ID
   AND T.PROV_ID = S.PROV_ID
   AND T.PROV_NTWK_PFX_ID = S.PROV_NTWK_PFX_ID
   AND T.PROV_NTWK_EFF_DT = S.PROV_NTWK_EFF_DT
   AND T.SRC_SYS_CD = S.SRC_SYS_CD
WHEN MATCHED THEN
  UPDATE SET
    T.PROV_NTWK_ACPTNG_MCAID_PATN_IN = S.PROV_NTWK_ACPTNG_MCAID_PATN_IN,
    T.PROV_NTWK_ACPTNG_MCARE_PATN_IN = S.PROV_NTWK_ACPTNG_MCARE_PATN_IN,
    T.PROV_NTWK_AUTO_PCP_ASGMT_IN = S.PROV_NTWK_AUTO_PCP_ASGMT_IN,
    T.PROV_NTWK_DIR_IN = S.PROV_NTWK_DIR_IN,
    T.PROV_NTWK_ACPTNG_PATN_IN = S.PROV_NTWK_ACPTNG_PATN_IN,
    T.PROV_NTWK_GNDR_ACPTD_CD = S.PROV_NTWK_GNDR_ACPTD_CD,
    T.PROV_NTWK_GNDR_ACPTD_NM = S.PROV_NTWK_GNDR_ACPTD_NM,
    T.PROV_NTWK_MAX_PATN_AGE = S.PROV_NTWK_MAX_PATN_AGE,
    T.PROV_NTWK_MIN_PATN_AGE = S.PROV_NTWK_MIN_PATN_AGE,
    T.PROV_NTWK_PCP_IN = S.PROV_NTWK_PCP_IN,
    T.PROV_NTWK_PROV_DIR_IN = S.PROV_NTWK_PROV_DIR_IN,
    T.NTWK_NM = S.NTWK_NM,
    T.NTWK_SH_NM = S.NTWK_SH_NM,
    T.PROV_NTWK_TERM_DT = S.PROV_NTWK_TERM_DT,
    T.PROV_NTWK_CNTR_IN = S.PROV_NTWK_CNTR_IN,
    T.LAST_UPDT_DT = S.LAST_UPDT_DT,
    T.PCMH_IN = S.PCMH_IN
WHEN NOT MATCHED THEN
  INSERT
    (NTWK_ID, PROV_ID, PROV_NTWK_PFX_ID, PROV_NTWK_EFF_DT, SRC_SYS_CD,
     PROV_NTWK_ACPTNG_MCAID_PATN_IN, PROV_NTWK_ACPTNG_MCARE_PATN_IN, PROV_NTWK_AUTO_PCP_ASGMT_IN,
     PROV_NTWK_DIR_IN, PROV_NTWK_ACPTNG_PATN_IN, PROV_NTWK_GNDR_ACPTD_CD, PROV_NTWK_GNDR_ACPTD_NM,
     PROV_NTWK_MAX_PATN_AGE, PROV_NTWK_MIN_PATN_AGE, PROV_NTWK_PCP_IN, PROV_NTWK_PROV_DIR_IN,
     NTWK_NM, NTWK_SH_NM, PROV_NTWK_TERM_DT, PROV_NTWK_CNTR_IN, LAST_UPDT_DT, PCMH_IN)
  VALUES
    (S.NTWK_ID, S.PROV_ID, S.PROV_NTWK_PFX_ID, S.PROV_NTWK_EFF_DT, S.SRC_SYS_CD,
     S.PROV_NTWK_ACPTNG_MCAID_PATN_IN, S.PROV_NTWK_ACPTNG_MCARE_PATN_IN, S.PROV_NTWK_AUTO_PCP_ASGMT_IN,
     S.PROV_NTWK_DIR_IN, S.PROV_NTWK_ACPTNG_PATN_IN, S.PROV_NTWK_GNDR_ACPTD_CD, S.PROV_NTWK_GNDR_ACPTD_NM,
     S.PROV_NTWK_MAX_PATN_AGE, S.PROV_NTWK_MIN_PATN_AGE, S.PROV_NTWK_PCP_IN, S.PROV_NTWK_PROV_DIR_IN,
     S.NTWK_NM, S.NTWK_SH_NM, S.PROV_NTWK_TERM_DT, S.PROV_NTWK_CNTR_IN, S.LAST_UPDT_DT, S.PCMH_IN);
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)

df_odbc_PROV_DIR_DM_PROV_NTWK_out_rej = spark.createDataFrame(
    [],
    StructType([
        StructField("ERRORCODE", StringType(), True),
        StructField("ERRORTEXT", StringType(), True)
    ])
).select(
    rpad(col("ERRORCODE"), 256, " ").alias("ERRORCODE"),
    rpad(col("ERRORTEXT"), 256, " ").alias("ERRORTEXT")
)

write_files(
    df_odbc_PROV_DIR_DM_PROV_NTWK_out_rej.select("ERRORCODE", "ERRORTEXT"),
    f"{adls_path}/load/PROV_DIR_DM_PROV_NTWK_Rej.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)