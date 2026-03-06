# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                             DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                                  ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------               -----------------------------    ------------------------------       --------------------
# MAGIC Pooja Sunkara          08/13/2013          5114                             Loads Data from IDS to  MBRSH_DM_CLS_PLN                                           IntegrateWrhsDevl   Peter Marshall               9/26/2013

# MAGIC Job Name: IdsDmMbrshDmClsPlnLoad
# MAGIC Read Load File created in the IdsDmMbrshDmClsPlnExtr Job
# MAGIC Copy Stage for buffer
# MAGIC Truncate then Insert the 
# MAGIC 
# MAGIC MBRSH_DM_CLS_PLN table
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
from pyspark.sql.functions import col, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


ClmMartOwner = get_widget_value('ClmMartOwner','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')
ClmMartArraySize = get_widget_value('ClmMartArraySize','')
ClmMartRecordCount = get_widget_value('ClmMartRecordCount','')

schema_seq_MBRSH_DM_CLS_PLN_csv_load = StructType([
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("CLS_PLN_ID", StringType(), False),
    StructField("CLS_PLN_BUY_UP_IN", StringType(), False),
    StructField("CLS_PLN_DESC", StringType(), False),
    StructField("CLS_PLN_METRO_RURAL_CD", StringType(), False),
    StructField("CLS_PLN_METRO_RURAL_NM", StringType(), False),
    StructField("CLS_PLN_PCMH_IN", StringType(), False),
    StructField("LAST_UPDT_RUN_CYC_NO", IntegerType(), False)
])

df_seq_MBRSH_DM_CLS_PLN_csv_load = spark.read \
    .option("header", False) \
    .option("sep", ",") \
    .option("quote", "^") \
    .option("escape", "^") \
    .schema(schema_seq_MBRSH_DM_CLS_PLN_csv_load) \
    .csv(f"{adls_path}/load/MBRSH_DM_CLS_PLN.dat")

df_cpy_forBuffer = df_seq_MBRSH_DM_CLS_PLN_csv_load.select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("CLS_PLN_ID").alias("CLS_PLN_ID"),
    col("CLS_PLN_BUY_UP_IN").alias("CLS_PLN_BUY_UP_IN"),
    col("CLS_PLN_DESC").alias("CLS_PLN_DESC"),
    col("CLS_PLN_METRO_RURAL_CD").alias("CLS_PLN_METRO_RURAL_CD"),
    col("CLS_PLN_METRO_RURAL_NM").alias("CLS_PLN_METRO_RURAL_NM"),
    col("CLS_PLN_PCMH_IN").alias("CLS_PLN_PCMH_IN"),
    col("LAST_UPDT_RUN_CYC_NO").alias("LAST_UPDT_RUN_CYC_NO")
)

df_Odbc_MBRSH_DM_CLS_PLN_out = df_cpy_forBuffer.select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("CLS_PLN_ID").alias("CLS_PLN_ID"),
    rpad(col("CLS_PLN_BUY_UP_IN"), 1, " ").alias("CLS_PLN_BUY_UP_IN"),
    col("CLS_PLN_DESC").alias("CLS_PLN_DESC"),
    col("CLS_PLN_METRO_RURAL_CD").alias("CLS_PLN_METRO_RURAL_CD"),
    col("CLS_PLN_METRO_RURAL_NM").alias("CLS_PLN_METRO_RURAL_NM"),
    rpad(col("CLS_PLN_PCMH_IN"), 1, " ").alias("CLS_PLN_PCMH_IN"),
    col("LAST_UPDT_RUN_CYC_NO").alias("LAST_UPDT_RUN_CYC_NO")
)

jdbc_url, jdbc_props = get_db_config(clmmart_secret_name)

execute_dml(f"DROP TABLE IF EXISTS STAGING.IdsDmMbrshDmClsPlnLoad_Odbc_MBRSH_DM_CLS_PLN_out_temp", jdbc_url, jdbc_props)

df_Odbc_MBRSH_DM_CLS_PLN_out.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.IdsDmMbrshDmClsPlnLoad_Odbc_MBRSH_DM_CLS_PLN_out_temp") \
    .mode("overwrite") \
    .save()

execute_dml(f"TRUNCATE TABLE {ClmMartOwner}.MBRSH_DM_CLS_PLN", jdbc_url, jdbc_props)

merge_sql = f"""
MERGE INTO {ClmMartOwner}.MBRSH_DM_CLS_PLN AS T
USING STAGING.IdsDmMbrshDmClsPlnLoad_Odbc_MBRSH_DM_CLS_PLN_out_temp AS S
ON 
    T.SRC_SYS_CD = S.SRC_SYS_CD
    AND T.CLS_PLN_ID = S.CLS_PLN_ID
WHEN MATCHED THEN 
    UPDATE SET 
        T.CLS_PLN_BUY_UP_IN = S.CLS_PLN_BUY_UP_IN,
        T.CLS_PLN_DESC = S.CLS_PLN_DESC,
        T.CLS_PLN_METRO_RURAL_CD = S.CLS_PLN_METRO_RURAL_CD,
        T.CLS_PLN_METRO_RURAL_NM = S.CLS_PLN_METRO_RURAL_NM,
        T.CLS_PLN_PCMH_IN = S.CLS_PLN_PCMH_IN,
        T.LAST_UPDT_RUN_CYC_NO = S.LAST_UPDT_RUN_CYC_NO
WHEN NOT MATCHED THEN
    INSERT (
        SRC_SYS_CD,
        CLS_PLN_ID,
        CLS_PLN_BUY_UP_IN,
        CLS_PLN_DESC,
        CLS_PLN_METRO_RURAL_CD,
        CLS_PLN_METRO_RURAL_NM,
        CLS_PLN_PCMH_IN,
        LAST_UPDT_RUN_CYC_NO
    )
    VALUES (
        S.SRC_SYS_CD,
        S.CLS_PLN_ID,
        S.CLS_PLN_BUY_UP_IN,
        S.CLS_PLN_DESC,
        S.CLS_PLN_METRO_RURAL_CD,
        S.CLS_PLN_METRO_RURAL_NM,
        S.CLS_PLN_PCMH_IN,
        S.LAST_UPDT_RUN_CYC_NO
    )
;
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)

schema_rej = StructType([
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("CLS_PLN_ID", StringType(), True),
    StructField("CLS_PLN_BUY_UP_IN", StringType(), True),
    StructField("CLS_PLN_DESC", StringType(), True),
    StructField("CLS_PLN_METRO_RURAL_CD", StringType(), True),
    StructField("CLS_PLN_METRO_RURAL_NM", StringType(), True),
    StructField("CLS_PLN_PCMH_IN", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_NO", IntegerType(), True),
    StructField("ERRORCODE", StringType(), True),
    StructField("ERRORTEXT", StringType(), True)
])

df_Odbc_MBRSH_DM_CLS_PLN_out_rej = spark.createDataFrame([], schema_rej)

df_seq_MBRSH_DM_CLS_PLN_csv_rej = df_Odbc_MBRSH_DM_CLS_PLN_out_rej.select(
    "SRC_SYS_CD",
    "CLS_PLN_ID",
    "CLS_PLN_BUY_UP_IN",
    "CLS_PLN_DESC",
    "CLS_PLN_METRO_RURAL_CD",
    "CLS_PLN_METRO_RURAL_NM",
    "CLS_PLN_PCMH_IN",
    "LAST_UPDT_RUN_CYC_NO",
    "ERRORCODE",
    "ERRORTEXT"
)

write_files(
    df_seq_MBRSH_DM_CLS_PLN_csv_rej,
    f"{adls_path}/load/MBRSH_DM_CLS_PLN_Rej.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)