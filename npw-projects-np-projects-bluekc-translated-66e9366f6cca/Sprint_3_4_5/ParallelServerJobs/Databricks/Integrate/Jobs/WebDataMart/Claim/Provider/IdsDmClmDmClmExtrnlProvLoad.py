# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Archana Palivela     09/21/2013          5114                             Original Programming                                                                             IntegrateWrhsDevl      Jag Yelavarthi             2013-11-30

# MAGIC Job Name: IdsDmClmDmClmExtrnlProvLoad
# MAGIC Read Load File created in the IdsDmClmDmClmExtrnlProvExtr Job
# MAGIC Copy Stage for buffer
# MAGIC Update then Insert the CLM_DM_CLM_EXTRNL_PROV Data.
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
from pyspark.sql.functions import rpad, col
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


ClmMartOwner = get_widget_value('ClmMartOwner','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')
ClmMartArraySize = get_widget_value('ClmMartArraySize','')
ClmMartRecordCount = get_widget_value('ClmMartRecordCount','')

schema_seq_CLM_DM_CLM_EXTRNL_PROV_csv_load = StructType([
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("CLM_ID", StringType(), True),
    StructField("CLM_EXTRNL_PROV_NM", StringType(), True),
    StructField("CLM_EXTRNL_PROV_ADDR_LN_1", StringType(), True),
    StructField("CLM_EXTRNL_PROV_ADDR_LN_2", StringType(), True),
    StructField("CLM_EXTRNL_PROV_ADDR_LN_3", StringType(), True),
    StructField("CLM_EXTRNL_PROV_CITY_NM", StringType(), True),
    StructField("CLM_EXTRNL_PROV_ST_CD", StringType(), True),
    StructField("CLM_EXTRNL_PROV_POSTAL_CD", StringType(), True),
    StructField("CLM_EXTRNL_PROV_CNTY_NM", StringType(), True),
    StructField("CLM_EXTRNL_PROV_CTRY_CD", StringType(), True),
    StructField("CLM_EXTRNL_PROV_PHN_NO", StringType(), True),
    StructField("CLM_EXTRNL_PROV_ID", StringType(), True),
    StructField("CLM_EXTRNL_PROV_NPI", StringType(), True),
    StructField("CLM_EXTRNL_PROV_SVC_PROV_ID", StringType(), True),
    StructField("CLM_EXTRNL_PROV_SVC_PROV_NPI", StringType(), True),
    StructField("CLM_EXTRNL_PROV_TAX_ID", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_NO", IntegerType(), True)
])

df_seq_CLM_DM_CLM_EXTRNL_PROV_csv_load = (
    spark.read.format("csv")
    .option("delimiter", ",")
    .option("quote", "^")
    .option("header", "false")
    .option("nullValue", None)
    .schema(schema_seq_CLM_DM_CLM_EXTRNL_PROV_csv_load)
    .load(f"{adls_path}/load/CLM_DM_CLM_EXTRNL_PROV.dat")
)

df_cpy_forBuffer = df_seq_CLM_DM_CLM_EXTRNL_PROV_csv_load.select(
    col("SRC_SYS_CD"),
    col("CLM_ID"),
    col("CLM_EXTRNL_PROV_NM"),
    col("CLM_EXTRNL_PROV_ADDR_LN_1"),
    col("CLM_EXTRNL_PROV_ADDR_LN_2"),
    col("CLM_EXTRNL_PROV_ADDR_LN_3"),
    col("CLM_EXTRNL_PROV_CITY_NM"),
    col("CLM_EXTRNL_PROV_ST_CD"),
    col("CLM_EXTRNL_PROV_POSTAL_CD"),
    col("CLM_EXTRNL_PROV_CNTY_NM"),
    col("CLM_EXTRNL_PROV_CTRY_CD"),
    col("CLM_EXTRNL_PROV_PHN_NO"),
    col("CLM_EXTRNL_PROV_ID"),
    col("CLM_EXTRNL_PROV_NPI"),
    col("CLM_EXTRNL_PROV_SVC_PROV_ID"),
    col("CLM_EXTRNL_PROV_SVC_PROV_NPI"),
    col("CLM_EXTRNL_PROV_TAX_ID"),
    col("LAST_UPDT_RUN_CYC_NO")
)

df_forDB = (
    df_cpy_forBuffer
    .withColumn("CLM_EXTRNL_PROV_POSTAL_CD", rpad(col("CLM_EXTRNL_PROV_POSTAL_CD"), 11, " "))
    .withColumn("CLM_EXTRNL_PROV_PHN_NO", rpad(col("CLM_EXTRNL_PROV_PHN_NO"), 20, " "))
    .withColumn("CLM_EXTRNL_PROV_SVC_PROV_ID", rpad(col("CLM_EXTRNL_PROV_SVC_PROV_ID"), 13, " "))
    .withColumn("CLM_EXTRNL_PROV_TAX_ID", rpad(col("CLM_EXTRNL_PROV_TAX_ID"), 9, " "))
    .select(
        "SRC_SYS_CD",
        "CLM_ID",
        "CLM_EXTRNL_PROV_NM",
        "CLM_EXTRNL_PROV_ADDR_LN_1",
        "CLM_EXTRNL_PROV_ADDR_LN_2",
        "CLM_EXTRNL_PROV_ADDR_LN_3",
        "CLM_EXTRNL_PROV_CITY_NM",
        "CLM_EXTRNL_PROV_ST_CD",
        "CLM_EXTRNL_PROV_POSTAL_CD",
        "CLM_EXTRNL_PROV_CNTY_NM",
        "CLM_EXTRNL_PROV_CTRY_CD",
        "CLM_EXTRNL_PROV_PHN_NO",
        "CLM_EXTRNL_PROV_ID",
        "CLM_EXTRNL_PROV_NPI",
        "CLM_EXTRNL_PROV_SVC_PROV_ID",
        "CLM_EXTRNL_PROV_SVC_PROV_NPI",
        "CLM_EXTRNL_PROV_TAX_ID",
        "LAST_UPDT_RUN_CYC_NO"
    )
)

jdbc_url, jdbc_props = get_db_config(clmmart_secret_name)

execute_dml(
    "DROP TABLE IF EXISTS STAGING.IdsDmClmDmClmExtrnlProvLoad_Odbc_CLM_DM_CLM_EXTRNL_PROV_out_temp",
    jdbc_url,
    jdbc_props
)

df_forDB.write.jdbc(
    url=jdbc_url,
    table="STAGING.IdsDmClmDmClmExtrnlProvLoad_Odbc_CLM_DM_CLM_EXTRNL_PROV_out_temp",
    mode="overwrite",
    properties=jdbc_props
)

merge_sql = f"""
MERGE {ClmMartOwner}.CLM_DM_CLM_EXTRNL_PROV AS TGT
USING STAGING.IdsDmClmDmClmExtrnlProvLoad_Odbc_CLM_DM_CLM_EXTRNL_PROV_out_temp AS SRC
ON TGT.SRC_SYS_CD = SRC.SRC_SYS_CD
   AND TGT.CLM_ID = SRC.CLM_ID
WHEN MATCHED THEN
  UPDATE SET
    TGT.CLM_EXTRNL_PROV_NM = SRC.CLM_EXTRNL_PROV_NM,
    TGT.CLM_EXTRNL_PROV_ADDR_LN_1 = SRC.CLM_EXTRNL_PROV_ADDR_LN_1,
    TGT.CLM_EXTRNL_PROV_ADDR_LN_2 = SRC.CLM_EXTRNL_PROV_ADDR_LN_2,
    TGT.CLM_EXTRNL_PROV_ADDR_LN_3 = SRC.CLM_EXTRNL_PROV_ADDR_LN_3,
    TGT.CLM_EXTRNL_PROV_CITY_NM = SRC.CLM_EXTRNL_PROV_CITY_NM,
    TGT.CLM_EXTRNL_PROV_ST_CD = SRC.CLM_EXTRNL_PROV_ST_CD,
    TGT.CLM_EXTRNL_PROV_POSTAL_CD = SRC.CLM_EXTRNL_PROV_POSTAL_CD,
    TGT.CLM_EXTRNL_PROV_CNTY_NM = SRC.CLM_EXTRNL_PROV_CNTY_NM,
    TGT.CLM_EXTRNL_PROV_CTRY_CD = SRC.CLM_EXTRNL_PROV_CTRY_CD,
    TGT.CLM_EXTRNL_PROV_PHN_NO = SRC.CLM_EXTRNL_PROV_PHN_NO,
    TGT.CLM_EXTRNL_PROV_ID = SRC.CLM_EXTRNL_PROV_ID,
    TGT.CLM_EXTRNL_PROV_NPI = SRC.CLM_EXTRNL_PROV_NPI,
    TGT.CLM_EXTRNL_PROV_SVC_PROV_ID = SRC.CLM_EXTRNL_PROV_SVC_PROV_ID,
    TGT.CLM_EXTRNL_PROV_SVC_PROV_NPI = SRC.CLM_EXTRNL_PROV_SVC_PROV_NPI,
    TGT.CLM_EXTRNL_PROV_TAX_ID = SRC.CLM_EXTRNL_PROV_TAX_ID,
    TGT.LAST_UPDT_RUN_CYC_NO = SRC.LAST_UPDT_RUN_CYC_NO
WHEN NOT MATCHED THEN
  INSERT (
    SRC_SYS_CD,
    CLM_ID,
    CLM_EXTRNL_PROV_NM,
    CLM_EXTRNL_PROV_ADDR_LN_1,
    CLM_EXTRNL_PROV_ADDR_LN_2,
    CLM_EXTRNL_PROV_ADDR_LN_3,
    CLM_EXTRNL_PROV_CITY_NM,
    CLM_EXTRNL_PROV_ST_CD,
    CLM_EXTRNL_PROV_POSTAL_CD,
    CLM_EXTRNL_PROV_CNTY_NM,
    CLM_EXTRNL_PROV_CTRY_CD,
    CLM_EXTRNL_PROV_PHN_NO,
    CLM_EXTRNL_PROV_ID,
    CLM_EXTRNL_PROV_NPI,
    CLM_EXTRNL_PROV_SVC_PROV_ID,
    CLM_EXTRNL_PROV_SVC_PROV_NPI,
    CLM_EXTRNL_PROV_TAX_ID,
    LAST_UPDT_RUN_CYC_NO
  )
  VALUES (
    SRC.SRC_SYS_CD,
    SRC.CLM_ID,
    SRC.CLM_EXTRNL_PROV_NM,
    SRC.CLM_EXTRNL_PROV_ADDR_LN_1,
    SRC.CLM_EXTRNL_PROV_ADDR_LN_2,
    SRC.CLM_EXTRNL_PROV_ADDR_LN_3,
    SRC.CLM_EXTRNL_PROV_CITY_NM,
    SRC.CLM_EXTRNL_PROV_ST_CD,
    SRC.CLM_EXTRNL_PROV_POSTAL_CD,
    SRC.CLM_EXTRNL_PROV_CNTY_NM,
    SRC.CLM_EXTRNL_PROV_CTRY_CD,
    SRC.CLM_EXTRNL_PROV_PHN_NO,
    SRC.CLM_EXTRNL_PROV_ID,
    SRC.CLM_EXTRNL_PROV_NPI,
    SRC.CLM_EXTRNL_PROV_SVC_PROV_ID,
    SRC.CLM_EXTRNL_PROV_SVC_PROV_NPI,
    SRC.CLM_EXTRNL_PROV_TAX_ID,
    SRC.LAST_UPDT_RUN_CYC_NO
  );
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)

schema_reject = StructType([
    StructField("ERRORCODE", StringType(), True),
    StructField("ERRORTEXT", StringType(), True)
])
df_Odbc_CLM_DM_CLM_EXTRNL_PROV_out = spark.createDataFrame([], schema_reject)

df_seq_CLM_DM_CLM_EXTRNL_PROV_csv_rej = df_Odbc_CLM_DM_CLM_EXTRNL_PROV_out.select("ERRORCODE","ERRORTEXT")

write_files(
    df_seq_CLM_DM_CLM_EXTRNL_PROV_csv_rej,
    f"{adls_path}/load/CLM_DM_CLM_EXTRNL_PROV_Rej.dat",
    delimiter=",",
    mode="append",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)