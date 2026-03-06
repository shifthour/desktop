# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Nagesh Bandi          09/27/2013          5114                             Movies Data from IDS to  CLM_DM_PAYMT_SUM                             IntegrateWrhsDevl

# MAGIC Job Name: IdsDmClmDmClmLnAltChrgLoad
# MAGIC Read Load File created in the IdsDmClmDmClmLnAltChrgExtr Job
# MAGIC Copy Stage for buffer
# MAGIC Update and  Insert the CLM_DM_PAYMT_SUM  Data.
# MAGIC Load rejects are redirected into a Reject file
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType, TimestampType
from pyspark.sql.functions import col, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


ClmMartOwner = get_widget_value('ClmMartOwner','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')
ClmMartArraySize = get_widget_value('ClmMartArraySize','')
ClmMartRecordCount = get_widget_value('ClmMartRecordCount','')

schema_seq_CLM_DM_PAYMT_SUM_csv_load = StructType([
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PAYMT_REF_ID", StringType(), False),
    StructField("PAYMT_SUM_LOB_CD", StringType(), False),
    StructField("PROV_PD_PROV_ID", StringType(), False),
    StructField("PAYMT_SUM_PAYE_TYP_CD", StringType(), False),
    StructField("PAYMT_SUM_PAYMT_TYP_CD", StringType(), False),
    StructField("PAYMT_SUM_TYP_CD", StringType(), False),
    StructField("PAYMT_SUM_COMBND_CLM_PAYMT_IN", StringType(), False),
    StructField("PD_DT", TimestampType(), True),
    StructField("PERD_END_DT", TimestampType(), False),
    StructField("DEDCT_AMT", DecimalType(38,10), False),
    StructField("NET_AMT", DecimalType(38,10), False),
    StructField("ORIG_SUM_AMT", DecimalType(38,10), False),
    StructField("CUR_CHK_SEQ_NO", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_NO", IntegerType(), False)
])

df_seq_CLM_DM_PAYMT_SUM_csv_load = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "^")
    .schema(schema_seq_CLM_DM_PAYMT_SUM_csv_load)
    .load(f"{adls_path}/load/CLM_DM_PAYMT_SUM.dat")
)

df_cpy_forBuffer = df_seq_CLM_DM_PAYMT_SUM_csv_load.repartition("SRC_SYS_CD", "PAYMT_REF_ID", "PAYMT_SUM_LOB_CD")

df_Odbc_CLM_DM_PAYMT_SUM_out = df_cpy_forBuffer

jdbc_url_clmmart, jdbc_props_clmmart = get_db_config(clmmart_secret_name)
drop_temp_sql = "DROP TABLE IF EXISTS STAGING.IdsDmClmMartPaymtSumLoad_Odbc_CLM_DM_PAYMT_SUM_out_temp"
execute_dml(drop_temp_sql, jdbc_url_clmmart, jdbc_props_clmmart)

(
    df_Odbc_CLM_DM_PAYMT_SUM_out.write
    .format("jdbc")
    .option("url", jdbc_url_clmmart)
    .options(**jdbc_props_clmmart)
    .option("dbtable", "STAGING.IdsDmClmMartPaymtSumLoad_Odbc_CLM_DM_PAYMT_SUM_out_temp")
    .mode("overwrite")
    .save()
)

merge_sql = f"""
MERGE INTO {ClmMartOwner}.CLM_DM_PAYMT_SUM AS T
USING STAGING.IdsDmClmMartPaymtSumLoad_Odbc_CLM_DM_PAYMT_SUM_out_temp AS S
ON T.SRC_SYS_CD = S.SRC_SYS_CD
AND T.PAYMT_REF_ID = S.PAYMT_REF_ID
AND T.PAYMT_SUM_LOB_CD = S.PAYMT_SUM_LOB_CD
WHEN MATCHED THEN UPDATE SET 
T.PROV_PD_PROV_ID = S.PROV_PD_PROV_ID,
T.PAYMT_SUM_PAYE_TYP_CD = S.PAYMT_SUM_PAYE_TYP_CD,
T.PAYMT_SUM_PAYMT_TYP_CD = S.PAYMT_SUM_PAYMT_TYP_CD,
T.PAYMT_SUM_TYP_CD = S.PAYMT_SUM_TYP_CD,
T.PAYMT_SUM_COMBND_CLM_PAYMT_IN = S.PAYMT_SUM_COMBND_CLM_PAYMT_IN,
T.PD_DT = S.PD_DT,
T.PERD_END_DT = S.PERD_END_DT,
T.DEDCT_AMT = S.DEDCT_AMT,
T.NET_AMT = S.NET_AMT,
T.ORIG_SUM_AMT = S.ORIG_SUM_AMT,
T.CUR_CHK_SEQ_NO = S.CUR_CHK_SEQ_NO,
T.LAST_UPDT_RUN_CYC_NO = S.LAST_UPDT_RUN_CYC_NO
WHEN NOT MATCHED THEN
INSERT (
SRC_SYS_CD,
PAYMT_REF_ID,
PAYMT_SUM_LOB_CD,
PROV_PD_PROV_ID,
PAYMT_SUM_PAYE_TYP_CD,
PAYMT_SUM_PAYMT_TYP_CD,
PAYMT_SUM_TYP_CD,
PAYMT_SUM_COMBND_CLM_PAYMT_IN,
PD_DT,
PERD_END_DT,
DEDCT_AMT,
NET_AMT,
ORIG_SUM_AMT,
CUR_CHK_SEQ_NO,
LAST_UPDT_RUN_CYC_NO
)
VALUES (
S.SRC_SYS_CD,
S.PAYMT_REF_ID,
S.PAYMT_SUM_LOB_CD,
S.PROV_PD_PROV_ID,
S.PAYMT_SUM_PAYE_TYP_CD,
S.PAYMT_SUM_PAYMT_TYP_CD,
S.PAYMT_SUM_TYP_CD,
S.PAYMT_SUM_COMBND_CLM_PAYMT_IN,
S.PD_DT,
S.PERD_END_DT,
S.DEDCT_AMT,
S.NET_AMT,
S.ORIG_SUM_AMT,
S.CUR_CHK_SEQ_NO,
S.LAST_UPDT_RUN_CYC_NO
);
"""

execute_dml(merge_sql, jdbc_url_clmmart, jdbc_props_clmmart)

schema_seq_CLM_DM_PAYMT_SUM_csv_rej = StructType([
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("PAYMT_REF_ID", StringType(), True),
    StructField("PAYMT_SUM_LOB_CD", StringType(), True),
    StructField("PROV_PD_PROV_ID", StringType(), True),
    StructField("PAYMT_SUM_PAYE_TYP_CD", StringType(), True),
    StructField("PAYMT_SUM_PAYMT_TYP_CD", StringType(), True),
    StructField("PAYMT_SUM_TYP_CD", StringType(), True),
    StructField("PAYMT_SUM_COMBND_CLM_PAYMT_IN", StringType(), True),
    StructField("PD_DT", TimestampType(), True),
    StructField("PERD_END_DT", TimestampType(), True),
    StructField("DEDCT_AMT", DecimalType(38,10), True),
    StructField("NET_AMT", DecimalType(38,10), True),
    StructField("ORIG_SUM_AMT", DecimalType(38,10), True),
    StructField("CUR_CHK_SEQ_NO", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_NO", IntegerType(), True),
    StructField("ERRORCODE", StringType(), True),
    StructField("ERRORTEXT", StringType(), True)
])

df_seq_CLM_DM_PAYMT_SUM_csv_rej = spark.createDataFrame([], schema_seq_CLM_DM_PAYMT_SUM_csv_rej)

df_seq_CLM_DM_PAYMT_SUM_csv_rej_final = df_seq_CLM_DM_PAYMT_SUM_csv_rej.select(
    rpad(col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    rpad(col("PAYMT_REF_ID"), <...>, " ").alias("PAYMT_REF_ID"),
    rpad(col("PAYMT_SUM_LOB_CD"), <...>, " ").alias("PAYMT_SUM_LOB_CD"),
    rpad(col("PROV_PD_PROV_ID"), <...>, " ").alias("PROV_PD_PROV_ID"),
    rpad(col("PAYMT_SUM_PAYE_TYP_CD"), <...>, " ").alias("PAYMT_SUM_PAYE_TYP_CD"),
    rpad(col("PAYMT_SUM_PAYMT_TYP_CD"), <...>, " ").alias("PAYMT_SUM_PAYMT_TYP_CD"),
    rpad(col("PAYMT_SUM_TYP_CD"), <...>, " ").alias("PAYMT_SUM_TYP_CD"),
    rpad(col("PAYMT_SUM_COMBND_CLM_PAYMT_IN"), 1, " ").alias("PAYMT_SUM_COMBND_CLM_PAYMT_IN"),
    col("PD_DT"),
    col("PERD_END_DT"),
    col("DEDCT_AMT"),
    col("NET_AMT"),
    col("ORIG_SUM_AMT"),
    col("CUR_CHK_SEQ_NO"),
    col("LAST_UPDT_RUN_CYC_NO"),
    rpad(col("ERRORCODE"), <...>, " ").alias("ERRORCODE"),
    rpad(col("ERRORTEXT"), <...>, " ").alias("ERRORTEXT")
)

write_files(
    df_seq_CLM_DM_PAYMT_SUM_csv_rej_final,
    f"{adls_path}/load/CLM_DM_PAYMT_SUM_Rej.dat",
    ",",
    "append",
    False,
    False,
    "^",
    None
)