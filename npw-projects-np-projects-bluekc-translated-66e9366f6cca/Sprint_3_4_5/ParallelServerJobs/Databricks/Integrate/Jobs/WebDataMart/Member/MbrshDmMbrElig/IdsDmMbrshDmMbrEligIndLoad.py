# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Shiva Devagiri         07/23/2013        5114                             Load DM Table MBRSHIP_DM_MBR_IND_ELIG                                IntegrateWrhsDevl     Peter Marshall               10/21/2013  
# MAGIC 
# MAGIC Archana Palivela     08/05/2014         5345                                 Changed the reject file name acc to standards.                                  IntegrateNewDevl    Jag Yelavarthi                2014-08-07
# MAGIC 
# MAGIC Jag Yelavarthi          2015-04-08         Daptiv#253                   Added Sorting in-addition to Partitioning in Copy stage.                         IntegrateNewDevl        Kalyan Neelam               2015-04-13
# MAGIC                                                                                                   Sort will let the job run without hanging in a multi-configuration
# MAGIC                                                                                                   environment.

# MAGIC Job Name: IdsDmMbrshDmMbrEligIndLoad
# MAGIC Read Load File created in the IdsDmMbrshpDmMbrEligIndExtr Job
# MAGIC Copy Stage for buffer
# MAGIC Update then Insert the MBRSHP_DM_MBR_ELIG Data.
# MAGIC Load rejects are redirected into a Reject file
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import pyspark.sql.functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


ClmMartOwner = get_widget_value('ClmMartOwner','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')
ClmMartArraySize = get_widget_value('ClmMartArraySize','')
ClmMartRecordCount = get_widget_value('ClmMartRecordCount','')

schema_seq_MBRSHP_DM_MBR_ELIG_IND_load = StructType([
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("MBR_UNIQ_KEY", IntegerType(), False),
    StructField("MBR_ENR_CLS_PLN_PROD_CAT_CD", StringType(), False),
    StructField("MBR_ENR_EFF_DT", TimestampType(), False),
    StructField("MBR_ENR_ELIG_IN", StringType(), False),
    StructField("MBR_ENR_TERM_DT", TimestampType(), True),
    StructField("MBR_ENR_ACTV_ROW_IN", StringType(), True)
])

df_seq_MBRSHP_DM_MBR_ELIG_IND_load = (
    spark.read
    .option("delimiter", ",")
    .option("quote", "^")
    .option("header", "false")
    .schema(schema_seq_MBRSHP_DM_MBR_ELIG_IND_load)
    .csv(f"{adls_path}/load/MBRSHIP_DM_MBR_IND_ELIG.dat")
)

df_cpy_forBuffer = df_seq_MBRSHP_DM_MBR_ELIG_IND_load
df_cpy_forBuffer = df_cpy_forBuffer.repartition(
    "SRC_SYS_CD", "MBR_UNIQ_KEY", "MBR_ENR_CLS_PLN_PROD_CAT_CD", "MBR_ENR_EFF_DT"
)
df_cpy_forBuffer = df_cpy_forBuffer.sortWithinPartitions(
    "SRC_SYS_CD", "MBR_UNIQ_KEY", "MBR_ENR_CLS_PLN_PROD_CAT_CD", "MBR_ENR_EFF_DT"
)

df_cpy_forBuffer_output = df_cpy_forBuffer.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("MBR_ENR_CLS_PLN_PROD_CAT_CD").alias("MBR_ENR_CLS_PLN_PROD_CAT_CD"),
    F.col("MBR_ENR_EFF_DT").alias("MBR_ENR_EFF_DT"),
    F.col("MBR_ENR_ELIG_IN").alias("MBR_ENR_ELIG_IN"),
    F.col("MBR_ENR_TERM_DT").alias("MBR_ENR_TERM_DT"),
    F.col("MBR_ENR_ACTV_ROW_IN").alias("MBR_ENR_ACTV_ROW_IN")
)

jdbc_url, jdbc_props = get_db_config(clmmart_secret_name)
execute_dml(
    "DROP TABLE IF EXISTS STAGING.IdsDmMbrshDmMbrEligIndLoad_ODBC_MBRSHP_DM_MBR_ELIG_IND_out_temp",
    jdbc_url,
    jdbc_props
)

df_cpy_forBuffer_output.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.IdsDmMbrshDmMbrEligIndLoad_ODBC_MBRSHP_DM_MBR_ELIG_IND_out_temp") \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE {ClmMartOwner}.MBRSH_DM_MBR_ELIG AS T
USING STAGING.IdsDmMbrshDmMbrEligIndLoad_ODBC_MBRSHP_DM_MBR_ELIG_IND_out_temp AS S
ON
(
  T.SRC_SYS_CD = S.SRC_SYS_CD
  AND T.MBR_UNIQ_KEY = S.MBR_UNIQ_KEY
  AND T.MBR_ENR_CLS_PLN_PROD_CAT_CD = S.MBR_ENR_CLS_PLN_PROD_CAT_CD
  AND T.MBR_ENR_EFF_DT = S.MBR_ENR_EFF_DT
)
WHEN MATCHED THEN
  UPDATE SET
    T.MBR_ENR_ELIG_IN = S.MBR_ENR_ELIG_IN,
    T.MBR_ENR_TERM_DT = S.MBR_ENR_TERM_DT,
    T.MBR_ENR_ACTV_ROW_IN = S.MBR_ENR_ACTV_ROW_IN
WHEN NOT MATCHED THEN
  INSERT
  (
    SRC_SYS_CD,
    MBR_UNIQ_KEY,
    MBR_ENR_CLS_PLN_PROD_CAT_CD,
    MBR_ENR_EFF_DT,
    MBR_ENR_ELIG_IN,
    MBR_ENR_TERM_DT,
    MBR_ENR_ACTV_ROW_IN
  )
  VALUES
  (
    S.SRC_SYS_CD,
    S.MBR_UNIQ_KEY,
    S.MBR_ENR_CLS_PLN_PROD_CAT_CD,
    S.MBR_ENR_EFF_DT,
    S.MBR_ENR_ELIG_IN,
    S.MBR_ENR_TERM_DT,
    S.MBR_ENR_ACTV_ROW_IN
);
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

df_seq_MBRSHP_DM_MBR_ELIG_IND_csv_rej = spark.createDataFrame(
    [],
    "ERRORCODE STRING, ERRORTEXT STRING"
)

write_files(
    df_seq_MBRSHP_DM_MBR_ELIG_IND_csv_rej,
    f"{adls_path}/load/MBRSHIP_DM_MBR_IND_ELIG_Rej.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)