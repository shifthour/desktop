# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Archana Palivela     09/15/2013          5114                             Original Programming                                                                             IntegrateWrhsDevl       Jag Yelavarthi             2013-12-01

# MAGIC Job Name: IdsDmBClmDmClmLoad
# MAGIC Read Load File created in the IdsDmClmDmClmExtr
# MAGIC  Job
# MAGIC Copy Stage for buffer
# MAGIC Update then Insert the B_CLM_DM_CLMData.
# MAGIC Load rejects are redirected into a Reject file
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DecimalType, IntegerType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


ClmMartOwner = get_widget_value('ClmMartOwner','')
ClmMartArraySize = get_widget_value('ClmMartArraySize','')
ClmMartRecordCount = get_widget_value('ClmMartRecordCount','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')

# ----------------------------------------------------------------------------
# Stage: seq_B_CLM_DM_CLMcsv_load (PxSequentialFile)
# ----------------------------------------------------------------------------
schema_seq_B_CLM_DM_CLMcsv_load = StructType([
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("CLM_ID", StringType(), True),
    StructField("CLM_SVC_STRT_DT", TimestampType(), True),
    StructField("CLM_LN_TOT_CHRG_AMT", DecimalType(10,2), True),
    StructField("CLM_PAYBL_AMT", DecimalType(10,2), True),
    StructField("MBR_UNIQ_KEY", IntegerType(), True),
    StructField("GRP_ID", StringType(), True)
])
df_seq_B_CLM_DM_CLMcsv_load = (
    spark.read.format("csv")
    .option("sep", ",")
    .option("quote", "^")
    .option("header", "false")
    .option("nullValue", None)
    .schema(schema_seq_B_CLM_DM_CLMcsv_load)
    .load(f"{adls_path}/load/B_CLM_DM_CLM.dat")
)

# ----------------------------------------------------------------------------
# Stage: cpy_forBuffer (PxCopy)
# ----------------------------------------------------------------------------
df_cpy_forBuffer = (
    df_seq_B_CLM_DM_CLMcsv_load
    .repartition("SRC_SYS_CD", "CLM_ID")  # Replicating partition by SRC_SYS_CD, CLM_ID; non-stable sort not directly implemented
    .select(
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("CLM_ID").alias("CLM_ID"),
        F.col("CLM_SVC_STRT_DT").alias("CLM_SVC_STRT_DT"),
        F.col("CLM_LN_TOT_CHRG_AMT").alias("CLM_LN_TOT_CHRG_AMT"),
        F.col("CLM_PAYBL_AMT").alias("CLM_PAYBL_AMT"),
        F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.col("GRP_ID").alias("GRP_ID")
    )
)

# ----------------------------------------------------------------------------
# Stage: Odbc_B_CLM_DM_CLMout (ODBCConnectorPX) - Merge logic
# ----------------------------------------------------------------------------
jdbc_url, jdbc_props = get_db_config(clmmart_secret_name)
temp_table_name = "STAGING.IdsDmBClmDmClmLoad_Odbc_B_CLM_DM_CLMout_temp"

execute_dml(f"DROP TABLE IF EXISTS {temp_table_name}", jdbc_url, jdbc_props)

(
    df_cpy_forBuffer
    .write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", temp_table_name)
    .mode("overwrite")
    .save()
)

merge_sql = f"""
MERGE INTO {ClmMartOwner}.B_CLM_DM_CLM AS target
USING {temp_table_name} AS source
ON target.SRC_SYS_CD = source.SRC_SYS_CD
   AND target.CLM_ID = source.CLM_ID
WHEN MATCHED THEN
  UPDATE SET
    target.CLM_SVC_STRT_DT = source.CLM_SVC_STRT_DT,
    target.CLM_LN_TOT_CHRG_AMT = source.CLM_LN_TOT_CHRG_AMT,
    target.CLM_PAYBL_AMT = source.CLM_PAYBL_AMT,
    target.MBR_UNIQ_KEY = source.MBR_UNIQ_KEY,
    target.GRP_ID = source.GRP_ID
WHEN NOT MATCHED THEN
  INSERT
    (SRC_SYS_CD, CLM_ID, CLM_SVC_STRT_DT, CLM_LN_TOT_CHRG_AMT, CLM_PAYBL_AMT, MBR_UNIQ_KEY, GRP_ID)
  VALUES
    (source.SRC_SYS_CD, source.CLM_ID, source.CLM_SVC_STRT_DT, source.CLM_LN_TOT_CHRG_AMT, source.CLM_PAYBL_AMT, source.MBR_UNIQ_KEY, source.GRP_ID);
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

# ----------------------------------------------------------------------------
# Stage: seq_B_CLM_DM_CLM_csv_rej (PxSequentialFile)
# ----------------------------------------------------------------------------
schema_reject = StructType([
    StructField("ERRORCODE", StringType(), True),
    StructField("ERRORTEXT", StringType(), True)
])
df_seq_B_CLM_DM_CLM_csv_rej_temp = spark.createDataFrame([], schema_reject)
df_seq_B_CLM_DM_CLM_csv_rej = df_seq_B_CLM_DM_CLM_csv_rej_temp.select(
    F.rpad(F.col("ERRORCODE"), 100, " ").alias("ERRORCODE"),
    F.rpad(F.col("ERRORTEXT"), 100, " ").alias("ERRORTEXT")
)

write_files(
    df_seq_B_CLM_DM_CLM_csv_rej,
    f"{adls_path}/load/B_CLM_DM_CLM_Rej.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)