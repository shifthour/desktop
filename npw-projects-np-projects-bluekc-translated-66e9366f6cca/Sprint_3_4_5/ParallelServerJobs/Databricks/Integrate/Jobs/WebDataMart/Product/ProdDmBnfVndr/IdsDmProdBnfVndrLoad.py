# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Balkarn Gill               06/07/2013        5114                             Load DM Table PROD_DM_BNF_VNDR                                           IntegrateWrhsDevl

# MAGIC Job Name: IdsDmProdDmBnfVndrLoad
# MAGIC Read Load File created in the IdsDmProdDmBnfVndrExtr Job
# MAGIC Copy Stage for buffer
# MAGIC Update then Insert the PROD_DM_BNF_VNDR Data.
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

schema_seq_PROD_DM_BNF_VNDR_csv_load = StructType([
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("BNF_VNDR_ID", StringType(), False),
    StructField("BNF_VNDR_NM", StringType(), False),
    StructField("BNF_VNDR_PHN_NO", StringType(), False),
    StructField("BNF_VNDR_TYP_CD", StringType(), False),
    StructField("BNF_VNDR_TYP_NM", StringType(), False),
    StructField("LAST_UPDT_RUN_CYC_NO", IntegerType(), False),
    StructField("BNF_VNDR_PHN_NO_EXT", StringType(), True)
])

df_seq_PROD_DM_BNF_VNDR_csv_load = (
    spark.read.format("csv")
    .schema(schema_seq_PROD_DM_BNF_VNDR_csv_load)
    .option("header", "false")
    .option("sep", ",")
    .option("quote", "^")
    .option("nullValue", None)
    .load(f"{adls_path}/load/PROD_DM_BNF_VNDR.dat")
)

df_cpy_forBuffer = df_seq_PROD_DM_BNF_VNDR_csv_load.select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("BNF_VNDR_ID").alias("BNF_VNDR_ID"),
    col("BNF_VNDR_NM").alias("BNF_VNDR_NM"),
    col("BNF_VNDR_PHN_NO").alias("BNF_VNDR_PHN_NO"),
    col("BNF_VNDR_TYP_CD").alias("BNF_VNDR_TYP_CD"),
    col("BNF_VNDR_TYP_NM").alias("BNF_VNDR_TYP_NM"),
    col("LAST_UPDT_RUN_CYC_NO").alias("LAST_UPDT_RUN_CYC_NO"),
    col("BNF_VNDR_PHN_NO_EXT").alias("BNF_VNDR_PHN_NO_EXT")
)

jdbc_url, jdbc_props = get_db_config(clmmart_secret_name)
execute_dml("DROP TABLE IF EXISTS STAGING.IdsDmProdBnfVndrLoad_ODBC_PROD_DM_BNF_VNDR_out_temp", jdbc_url, jdbc_props)

df_cpy_forBuffer.write.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "STAGING.IdsDmProdBnfVndrLoad_ODBC_PROD_DM_BNF_VNDR_out_temp") \
    .mode("append") \
    .options(**jdbc_props) \
    .save()

merge_sql = f"""
MERGE INTO {ClmMartOwner}.PROD_DM_BNF_VNDR AS T
USING STAGING.IdsDmProdBnfVndrLoad_ODBC_PROD_DM_BNF_VNDR_out_temp AS S
ON 
    T.SRC_SYS_CD = S.SRC_SYS_CD
    AND T.BNF_VNDR_ID = S.BNF_VNDR_ID
WHEN MATCHED THEN
    UPDATE SET
        T.BNF_VNDR_NM = S.BNF_VNDR_NM,
        T.BNF_VNDR_PHN_NO = S.BNF_VNDR_PHN_NO,
        T.BNF_VNDR_TYP_CD = S.BNF_VNDR_TYP_CD,
        T.BNF_VNDR_TYP_NM = S.BNF_VNDR_TYP_NM,
        T.LAST_UPDT_RUN_CYC_NO = S.LAST_UPDT_RUN_CYC_NO,
        T.BNF_VNDR_PHN_NO_EXT = S.BNF_VNDR_PHN_NO_EXT
WHEN NOT MATCHED THEN
    INSERT
    (
        SRC_SYS_CD,
        BNF_VNDR_ID,
        BNF_VNDR_NM,
        BNF_VNDR_PHN_NO,
        BNF_VNDR_TYP_CD,
        BNF_VNDR_TYP_NM,
        LAST_UPDT_RUN_CYC_NO,
        BNF_VNDR_PHN_NO_EXT
    )
    VALUES
    (
        S.SRC_SYS_CD,
        S.BNF_VNDR_ID,
        S.BNF_VNDR_NM,
        S.BNF_VNDR_PHN_NO,
        S.BNF_VNDR_TYP_CD,
        S.BNF_VNDR_TYP_NM,
        S.LAST_UPDT_RUN_CYC_NO,
        S.BNF_VNDR_PHN_NO_EXT
    );
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

schema_seq_PROD_DM_BNF_VNDR_csv_rej = StructType([
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("BNF_VNDR_ID", StringType(), True),
    StructField("BNF_VNDR_NM", StringType(), True),
    StructField("BNF_VNDR_PHN_NO", StringType(), True),
    StructField("BNF_VNDR_TYP_CD", StringType(), True),
    StructField("BNF_VNDR_TYP_NM", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_NO", IntegerType(), True),
    StructField("BNF_VNDR_PHN_NO_EXT", StringType(), True),
    StructField("ERRORCODE", StringType(), True),
    StructField("ERRORTEXT", StringType(), True)
])

df_seq_PROD_DM_BNF_VNDR_csv_rej = spark.createDataFrame([], schema_seq_PROD_DM_BNF_VNDR_csv_rej)

df_seq_PROD_DM_BNF_VNDR_csv_rej_final = (
    df_seq_PROD_DM_BNF_VNDR_csv_rej
    .withColumn("SRC_SYS_CD", rpad("SRC_SYS_CD", <...>, " "))
    .withColumn("BNF_VNDR_ID", rpad("BNF_VNDR_ID", <...>, " "))
    .withColumn("BNF_VNDR_NM", rpad("BNF_VNDR_NM", <...>, " "))
    .withColumn("BNF_VNDR_PHN_NO", rpad("BNF_VNDR_PHN_NO", <...>, " "))
    .withColumn("BNF_VNDR_TYP_CD", rpad("BNF_VNDR_TYP_CD", <...>, " "))
    .withColumn("BNF_VNDR_TYP_NM", rpad("BNF_VNDR_TYP_NM", <...>, " "))
    .withColumn("BNF_VNDR_PHN_NO_EXT", rpad("BNF_VNDR_PHN_NO_EXT", <...>, " "))
    .withColumn("ERRORCODE", rpad("ERRORCODE", <...>, " "))
    .withColumn("ERRORTEXT", rpad("ERRORTEXT", <...>, " "))
    .select(
        "SRC_SYS_CD",
        "BNF_VNDR_ID",
        "BNF_VNDR_NM",
        "BNF_VNDR_PHN_NO",
        "BNF_VNDR_TYP_CD",
        "BNF_VNDR_TYP_NM",
        "LAST_UPDT_RUN_CYC_NO",
        "BNF_VNDR_PHN_NO_EXT",
        "ERRORCODE",
        "ERRORTEXT"
    )
)

write_files(
    df_seq_PROD_DM_BNF_VNDR_csv_rej_final,
    f"{adls_path}/load/PROD_DM_BNF_VNDR_Rej.dat",
    delimiter=",",
    mode="append",
    is_parqruet=False,
    header=False,
    quote="^",
    nullValue=None
)