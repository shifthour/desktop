# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Nagesh Bandi           07/30/2013          5114                             Movies Data from IDS to  MBRSH_DM_SUB_ID_HIST                       IntegrateWrhsDevl  Peter Marshall               10/23/2013

# MAGIC Job Name: IdsDmMbrshDmSubIdHistLoad
# MAGIC Read Load File created in the IdsDmMbrshDmSubIdHistExtr Job
# MAGIC Copy Stage for buffer
# MAGIC Update then Insert the 
# MAGIC 
# MAGIC MBRSH_DM_SUB_ID_HIST Data.
# MAGIC Load rejects are redirected into a Reject file
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
from pyspark.sql.functions import col, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


ClmMartOwner = get_widget_value('ClmMartOwner','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')
ClmMartArraySize = get_widget_value('ClmMartArraySize','')
ClmMartRecordCount = get_widget_value('ClmMartRecordCount','')

schema_seq_MBRSH_DM_SUB_ID_HIST_csv_load = StructType([
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("SUB_UNIQ_KEY", IntegerType(), False),
    StructField("EDW_RCRD_STRT_DT", TimestampType(), False),
    StructField("EDW_CUR_RCRD_IN", StringType(), False),
    StructField("EDW_RCRD_END_DT", TimestampType(), False),
    StructField("GRP_SK", IntegerType(), False),
    StructField("GRP_ID", StringType(), False),
    StructField("SUB_SK", IntegerType(), False),
    StructField("SCRD_IN", StringType(), False),
    StructField("SUB_INDV_BE_KEY", DecimalType(38,10), False),
    StructField("SUB_ID", StringType(), False),
    StructField("LAST_UPDT_RUN_CYC_NO", IntegerType(), False)
])

df_seq_MBRSH_DM_SUB_ID_HIST_csv_load = (
    spark.read
    .option("header", False)
    .option("sep", ",")
    .option("quote", "^")
    .option("nullValue", None)
    .schema(schema_seq_MBRSH_DM_SUB_ID_HIST_csv_load)
    .csv(f"{adls_path}/load/MBRSH_DM_SUB_ID_HIST.dat")
)

df_cpy_forBuffer = df_seq_MBRSH_DM_SUB_ID_HIST_csv_load.select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    col("EDW_RCRD_STRT_DT").alias("EDW_RCRD_STRT_DT"),
    col("EDW_CUR_RCRD_IN").alias("EDW_CUR_RCRD_IN"),
    col("EDW_RCRD_END_DT").alias("EDW_RCRD_END_DT"),
    col("GRP_SK").alias("GRP_SK"),
    col("GRP_ID").alias("GRP_ID"),
    col("SUB_SK").alias("SUB_SK"),
    col("SCRD_IN").alias("SCRD_IN"),
    col("SUB_INDV_BE_KEY").alias("SUB_INDV_BE_KEY"),
    col("SUB_ID").alias("SUB_ID"),
    col("LAST_UPDT_RUN_CYC_NO").alias("LAST_UPDT_RUN_CYC_NO")
)

jdbc_url, jdbc_props = get_db_config(clmmart_secret_name)

temp_table_name = "STAGING.IdsDmMbrshDmSubIdHistLoad_Odbc_MBRSH_DM_SUB_ID_HIST_out_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_name}", jdbc_url, jdbc_props)
df_cpy_forBuffer.write.jdbc(
    url=jdbc_url,
    table=temp_table_name,
    mode="overwrite",
    properties=jdbc_props
)

merge_sql = f"""
MERGE INTO {ClmMartOwner}.MBRSH_DM_SUB_ID_HIST AS T
USING {temp_table_name} AS S
ON
    T.SRC_SYS_CD = S.SRC_SYS_CD
    AND T.SUB_UNIQ_KEY = S.SUB_UNIQ_KEY
    AND T.EDW_RCRD_STRT_DT = S.EDW_RCRD_STRT_DT
WHEN MATCHED THEN
    UPDATE SET
    T.EDW_CUR_RCRD_IN = S.EDW_CUR_RCRD_IN,
    T.EDW_RCRD_END_DT = S.EDW_RCRD_END_DT,
    T.GRP_SK = S.GRP_SK,
    T.GRP_ID = S.GRP_ID,
    T.SUB_SK = S.SUB_SK,
    T.SCRD_IN = S.SCRD_IN,
    T.SUB_INDV_BE_KEY = S.SUB_INDV_BE_KEY,
    T.SUB_ID = S.SUB_ID,
    T.LAST_UPDT_RUN_CYC_NO = S.LAST_UPDT_RUN_CYC_NO
WHEN NOT MATCHED THEN
    INSERT
    (
        SRC_SYS_CD,
        SUB_UNIQ_KEY,
        EDW_RCRD_STRT_DT,
        EDW_CUR_RCRD_IN,
        EDW_RCRD_END_DT,
        GRP_SK,
        GRP_ID,
        SUB_SK,
        SCRD_IN,
        SUB_INDV_BE_KEY,
        SUB_ID,
        LAST_UPDT_RUN_CYC_NO
    )
    VALUES
    (
        S.SRC_SYS_CD,
        S.SUB_UNIQ_KEY,
        S.EDW_RCRD_STRT_DT,
        S.EDW_CUR_RCRD_IN,
        S.EDW_RCRD_END_DT,
        S.GRP_SK,
        S.GRP_ID,
        S.SUB_SK,
        S.SCRD_IN,
        S.SUB_INDV_BE_KEY,
        S.SUB_ID,
        S.LAST_UPDT_RUN_CYC_NO
    );
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

schema_seq_MBRSH_DM_SUB_ID_HIST_csv_rej = StructType([
    StructField("ERRORCODE", StringType(), True),
    StructField("ERRORTEXT", StringType(), True)
])

df_seq_MBRSH_DM_SUB_ID_HIST_csv_rej = spark.createDataFrame([], schema_seq_MBRSH_DM_SUB_ID_HIST_csv_rej)

df_seq_MBRSH_DM_SUB_ID_HIST_csv_rej = df_seq_MBRSH_DM_SUB_ID_HIST_csv_rej.select(
    rpad(col("ERRORCODE"), <...>, " ").alias("ERRORCODE"),
    rpad(col("ERRORTEXT"), <...>, " ").alias("ERRORTEXT")
)

write_files(
    df_seq_MBRSH_DM_SUB_ID_HIST_csv_rej,
    f"{adls_path}/load/MBRSH_DM_SUB_ID_HIST_Rej.dat",
    ",",
    "append",
    False,
    False,
    "^",
    None
)