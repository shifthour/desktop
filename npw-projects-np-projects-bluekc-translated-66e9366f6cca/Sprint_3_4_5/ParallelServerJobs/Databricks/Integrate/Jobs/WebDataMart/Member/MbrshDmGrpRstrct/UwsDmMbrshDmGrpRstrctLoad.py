# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        ---------------------------------------------------------------------------                             ------------------------------    ------------------------------       --------------------
# MAGIC Bhupinder Kaur         2013 -08-16      5114                             Load  DM MBRSH_DM_GRP_RSTRCT                                   IntegrateWrhsDevl               Jag Yelavarthi            2013-10-23

# MAGIC Job Name: UwsDmMbrshDmGrpRstrctLoad
# MAGIC Read Load File created in the UwsDmMbrshDmGrpRstrctExtr Job
# MAGIC Copy Stage for buffer
# MAGIC Update then Insert MBRSH_DM_GRP_RSTRCT Data.
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


ClmMartOwner = get_widget_value("ClmMartOwner","")
clmmart_secret_name = get_widget_value("clmmart_secret_name","")
ClmMartArraySize = get_widget_value("ClmMartArraySize","")
ClmMartRecordCount = get_widget_value("ClmMartRecordCount","")

schema_seq_MBRSH_DM_GRP_RSTRCT_csv_load = StructType([
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("GRP_ID", StringType(), False),
    StructField("WEB_RSTRCT_RSN_CD", StringType(), False),
    StructField("WEB_RSTRCT_RSN_NM", StringType(), False),
    StructField("LAST_UPDT_RUN_CYC_NO", IntegerType(), False)
])

df_seq_MBRSH_DM_GRP_RSTRCT_csv_load = (
    spark.read.format("csv")
    .option("sep", ",")
    .option("quote", "^")
    .option("header", "false")
    .option("nullValue", None)
    .schema(schema_seq_MBRSH_DM_GRP_RSTRCT_csv_load)
    .load(f"{adls_path}/load/MBRSH_DM_GRP_RSTRCT.dat")
)

df_cpy_forBuffer = df_seq_MBRSH_DM_GRP_RSTRCT_csv_load.select(
    col("SRC_SYS_CD"),
    col("GRP_ID"),
    col("WEB_RSTRCT_RSN_CD"),
    col("WEB_RSTRCT_RSN_NM"),
    col("LAST_UPDT_RUN_CYC_NO")
)

df_ODBC_MBRSH_DM_GRP_RSTRCT_out = df_cpy_forBuffer.select(
    rpad(col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    rpad(col("GRP_ID"), <...>, " ").alias("GRP_ID"),
    rpad(col("WEB_RSTRCT_RSN_CD"), <...>, " ").alias("WEB_RSTRCT_RSN_CD"),
    rpad(col("WEB_RSTRCT_RSN_NM"), <...>, " ").alias("WEB_RSTRCT_RSN_NM"),
    col("LAST_UPDT_RUN_CYC_NO").alias("LAST_UPDT_RUN_CYC_NO")
)

jdbc_url, jdbc_props = get_db_config(clmmart_secret_name)

execute_dml(
    "DROP TABLE IF EXISTS STAGING.UwsDmMbrshDmGrpRstrctLoad_ODBC_MBRSH_DM_GRP_RSTRCT_out_temp",
    jdbc_url,
    jdbc_props
)

df_ODBC_MBRSH_DM_GRP_RSTRCT_out.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.UwsDmMbrshDmGrpRstrctLoad_ODBC_MBRSH_DM_GRP_RSTRCT_out_temp") \
    .mode("append") \
    .save()

merge_sql = f"""
MERGE {ClmMartOwner}.MBRSH_DM_GRP_RSTRCT AS T
USING STAGING.UwsDmMbrshDmGrpRstrctLoad_ODBC_MBRSH_DM_GRP_RSTRCT_out_temp AS S
ON
    T.SRC_SYS_CD = S.SRC_SYS_CD
    AND T.GRP_ID = S.GRP_ID
    AND T.WEB_RSTRCT_RSN_CD = S.WEB_RSTRCT_RSN_CD
WHEN MATCHED THEN
    UPDATE SET
        T.WEB_RSTRCT_RSN_NM = S.WEB_RSTRCT_RSN_NM,
        T.LAST_UPDT_RUN_CYC_NO = S.LAST_UPDT_RUN_CYC_NO
WHEN NOT MATCHED THEN
    INSERT (SRC_SYS_CD, GRP_ID, WEB_RSTRCT_RSN_CD, WEB_RSTRCT_RSN_NM, LAST_UPDT_RUN_CYC_NO)
    VALUES (S.SRC_SYS_CD, S.GRP_ID, S.WEB_RSTRCT_RSN_CD, S.WEB_RSTRCT_RSN_NM, S.LAST_UPDT_RUN_CYC_NO);
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)

schema_seq_MBRSH_DM_GRP_RSTRCT_csv_rej = StructType([
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("GRP_ID", StringType(), False),
    StructField("WEB_RSTRCT_RSN_CD", StringType(), False),
    StructField("WEB_RSTRCT_RSN_NM", StringType(), False),
    StructField("LAST_UPDT_RUN_CYC_NO", IntegerType(), False),
    StructField("ERRORCODE", StringType(), True),
    StructField("ERRORTEXT", StringType(), True)
])

df_seq_MBRSH_DM_GRP_RSTRCT_csv_rej = spark.createDataFrame([], schema_seq_MBRSH_DM_GRP_RSTRCT_csv_rej).select(
    rpad(col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    rpad(col("GRP_ID"), <...>, " ").alias("GRP_ID"),
    rpad(col("WEB_RSTRCT_RSN_CD"), <...>, " ").alias("WEB_RSTRCT_RSN_CD"),
    rpad(col("WEB_RSTRCT_RSN_NM"), <...>, " ").alias("WEB_RSTRCT_RSN_NM"),
    col("LAST_UPDT_RUN_CYC_NO").alias("LAST_UPDT_RUN_CYC_NO"),
    col("ERRORCODE").alias("ERRORCODE"),
    col("ERRORTEXT").alias("ERRORTEXT")
)

write_files(
    df_seq_MBRSH_DM_GRP_RSTRCT_csv_rej,
    f"{adls_path}/load/MBRSH_DM_GRP_RSTRCT_Rej.dat",
    ",",
    "append",
    False,
    False,
    "^",
    None
)