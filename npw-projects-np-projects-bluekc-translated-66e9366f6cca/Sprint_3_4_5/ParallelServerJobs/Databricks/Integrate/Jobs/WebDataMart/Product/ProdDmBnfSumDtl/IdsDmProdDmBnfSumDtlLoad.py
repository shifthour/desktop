# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                      DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                          ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Leandrew Moore        06/16/2013             5114                      Initial Programming                                    IntegrateWrhsDevl        Jag Yelavarthi        2013-08-12

# MAGIC Job Name: IdsDmProdDmBnfSumDtlLoad
# MAGIC Read Load File created in the IdsDmProdDmBnfSumDtlExtr Job
# MAGIC Copy Stage for buffer
# MAGIC Update then Insert the PROD_DM_BNF_SUM Data.
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
from pyspark.sql.functions import rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


ClmMartOwner = get_widget_value('ClmMartOwner','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')
ClmMartArraySize = get_widget_value('ClmMartArraySize','')
ClmMartRecordCount = get_widget_value('ClmMartRecordCount','')

schema_seq_PROD_DM_BNF_SUM_DTL_ext = StructType([
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PROD_ID", StringType(), False),
    StructField("BNF_SUM_DTL_TYP_CD", StringType(), False),
    StructField("PROD_CMPNT_EFF_DT", TimestampType(), False),
    StructField("BNF_SUM_DTL_TYP_NM", StringType(), False),
    StructField("PROD_CMPNT_TERM_DT", TimestampType(), False),
    StructField("BNF_SUM_DTL_COINS_PCT_AMT", DecimalType(38, 10), False),
    StructField("BNF_SUM_DTL_COPAY_AMT", DecimalType(38, 10), False),
    StructField("BNF_SUM_DTL_DEDCT_AMT", DecimalType(38, 10), False),
    StructField("BNF_SUM_DTL_LMT_AMT", DecimalType(38, 10), False),
    StructField("PROD_CMPNT_PFX_ID", StringType(), False),
    StructField("LAST_UPDT_RUN_CYC_NO", IntegerType(), False)
])

df_seq_PROD_DM_BNF_SUM_DTL_ext = (
    spark.read
    .option("delimiter", ",")
    .option("quote", "^")
    .option("header", "false")
    .option("nullValue", None)
    .schema(schema_seq_PROD_DM_BNF_SUM_DTL_ext)
    .csv(f"{adls_path}/load/PROD_DM_BNF_SUM_DTL.dat")
)

df_cpy_forBuffer = df_seq_PROD_DM_BNF_SUM_DTL_ext.select(
    "SRC_SYS_CD",
    "PROD_ID",
    "BNF_SUM_DTL_TYP_CD",
    "PROD_CMPNT_EFF_DT",
    "BNF_SUM_DTL_TYP_NM",
    "PROD_CMPNT_TERM_DT",
    "BNF_SUM_DTL_COINS_PCT_AMT",
    "BNF_SUM_DTL_COPAY_AMT",
    "BNF_SUM_DTL_DEDCT_AMT",
    "BNF_SUM_DTL_LMT_AMT",
    "PROD_CMPNT_PFX_ID",
    "LAST_UPDT_RUN_CYC_NO"
)

jdbc_url, jdbc_props = get_db_config(clmmart_secret_name)

execute_dml("DROP TABLE IF EXISTS STAGING.IdsDmProdDmBnfSumDtlLoad_ODBC_PROD_DM_BNF_SUM_DTL_out_temp", jdbc_url, jdbc_props)

df_cpy_forBuffer.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.IdsDmProdDmBnfSumDtlLoad_ODBC_PROD_DM_BNF_SUM_DTL_out_temp") \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE {ClmMartOwner}.PROD_DM_BNF_SUM_DTL AS T
USING STAGING.IdsDmProdDmBnfSumDtlLoad_ODBC_PROD_DM_BNF_SUM_DTL_out_temp AS S
ON
    T.SRC_SYS_CD = S.SRC_SYS_CD
    AND T.PROD_ID = S.PROD_ID
    AND T.BNF_SUM_DTL_TYP_CD = S.BNF_SUM_DTL_TYP_CD
    AND T.PROD_CMPNT_EFF_DT = S.PROD_CMPNT_EFF_DT
WHEN MATCHED THEN
  UPDATE SET
    T.BNF_SUM_DTL_TYP_NM = S.BNF_SUM_DTL_TYP_NM,
    T.PROD_CMPNT_TERM_DT = S.PROD_CMPNT_TERM_DT,
    T.BNF_SUM_DTL_COINS_PCT_AMT = S.BNF_SUM_DTL_COINS_PCT_AMT,
    T.BNF_SUM_DTL_COPAY_AMT = S.BNF_SUM_DTL_COPAY_AMT,
    T.BNF_SUM_DTL_DEDCT_AMT = S.BNF_SUM_DTL_DEDCT_AMT,
    T.BNF_SUM_DTL_LMT_AMT = S.BNF_SUM_DTL_LMT_AMT,
    T.PROD_CMPNT_PFX_ID = S.PROD_CMPNT_PFX_ID,
    T.LAST_UPDT_RUN_CYC_NO = S.LAST_UPDT_RUN_CYC_NO
WHEN NOT MATCHED THEN
  INSERT (
    SRC_SYS_CD,
    PROD_ID,
    BNF_SUM_DTL_TYP_CD,
    PROD_CMPNT_EFF_DT,
    BNF_SUM_DTL_TYP_NM,
    PROD_CMPNT_TERM_DT,
    BNF_SUM_DTL_COINS_PCT_AMT,
    BNF_SUM_DTL_COPAY_AMT,
    BNF_SUM_DTL_DEDCT_AMT,
    BNF_SUM_DTL_LMT_AMT,
    PROD_CMPNT_PFX_ID,
    LAST_UPDT_RUN_CYC_NO
  )
  VALUES (
    S.SRC_SYS_CD,
    S.PROD_ID,
    S.BNF_SUM_DTL_TYP_CD,
    S.PROD_CMPNT_EFF_DT,
    S.BNF_SUM_DTL_TYP_NM,
    S.PROD_CMPNT_TERM_DT,
    S.BNF_SUM_DTL_COINS_PCT_AMT,
    S.BNF_SUM_DTL_COPAY_AMT,
    S.BNF_SUM_DTL_DEDCT_AMT,
    S.BNF_SUM_DTL_LMT_AMT,
    S.PROD_CMPNT_PFX_ID,
    S.LAST_UPDT_RUN_CYC_NO
  )
;
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)

schema_rej = StructType([
    StructField("ERRORCODE", StringType(), True),
    StructField("ERRORTEXT", StringType(), True)
])

df_ODBC_PROD_DM_BNF_SUM_DTL_out_rej = spark.createDataFrame([], schema_rej)
df_ODBC_PROD_DM_BNF_SUM_DTL_out_rej_final = df_ODBC_PROD_DM_BNF_SUM_DTL_out_rej.select(
    rpad("ERRORCODE", 50, " ").alias("ERRORCODE"),
    rpad("ERRORTEXT", 50, " ").alias("ERRORTEXT")
)

write_files(
    df_ODBC_PROD_DM_BNF_SUM_DTL_out_rej_final,
    f"{adls_path}/load/PROD_DM_BNF_SUM_DTL_Rej.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)