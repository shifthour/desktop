# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Srikanth Mettpalli      06/6/2013          5114                             Original Programming                                                                             IntegrateWrhsDevl       Jag Yelavarthi            2013-08-11

# MAGIC Job Name: IdsDmProdDmDedctCmpntLoad
# MAGIC Read Load File created in the IdsDmProdDmDedctCmpntExtr Job
# MAGIC Copy Stage for buffer
# MAGIC Truncate then Insert the PROD_DM_DEDCT_CMPNT Data.
# MAGIC Load rejects are redirected into a Reject file
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
from pyspark.sql.functions import col, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


ClmMartOwner = get_widget_value('ClmMartOwner','')
ClmMartArraySize = get_widget_value('ClmMartArraySize','')
ClmMartRecordCount = get_widget_value('ClmMartRecordCount','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')

schema_seq_PROD_DM_DEDCT_CMPNT_csv_load = StructType([
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("ACCUM_NO", IntegerType(), False),
    StructField("DEDCT_CMPNT_ID", StringType(), False),
    StructField("DEDCT_CMPNT_ACCUM_PERD_CD", StringType(), False),
    StructField("DEDCT_CMPNT_ACCUM_PERD_NM", StringType(), False),
    StructField("DEDCT_CMPNT_OOP_USAGE_CD", StringType(), False),
    StructField("DEDCT_CMPNT_OOP_USAGE_NM", StringType(), False),
    StructField("DEDCT_CMPNT_TYP_CD", StringType(), False),
    StructField("DEDCT_CMPNT_TYP_NM", StringType(), False),
    StructField("STOP_LOSS_IN", StringType(), False),
    StructField("FMLY_DEDCT_AMT", DecimalType(38,10), False),
    StructField("FMLY_DEDCT_CAROVR_AMT", DecimalType(38,10), False),
    StructField("MBR_DEDCT_AMT", DecimalType(38,10), False),
    StructField("MBR_DEDCT_CAROVR_AMT", DecimalType(38,10), False),
    StructField("FMLY_DEDCT_CAROVR_PRSN_CT", IntegerType(), False),
    StructField("FMLY_DEDCT_PRSN_CT", IntegerType(), False),
    StructField("REL_ACCUM_NO", IntegerType(), False),
    StructField("DEDCT_CMPNT_DESC", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_NO", IntegerType(), False),
])

df_seq_PROD_DM_DEDCT_CMPNT_csv_load = (
    spark.read
    .option("header", False)
    .option("sep", ",")
    .option("quote", "^")
    .option("nullValue", None)
    .schema(schema_seq_PROD_DM_DEDCT_CMPNT_csv_load)
    .csv(f"{adls_path}/load/PROD_DM_DEDCT_CMPNT.dat")
)

df_cpy_forBuffer = df_seq_PROD_DM_DEDCT_CMPNT_csv_load.select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("ACCUM_NO").alias("ACCUM_NO"),
    col("DEDCT_CMPNT_ID").alias("DEDCT_CMPNT_ID"),
    col("DEDCT_CMPNT_ACCUM_PERD_CD").alias("DEDCT_CMPNT_ACCUM_PERD_CD"),
    col("DEDCT_CMPNT_ACCUM_PERD_NM").alias("DEDCT_CMPNT_ACCUM_PERD_NM"),
    col("DEDCT_CMPNT_OOP_USAGE_CD").alias("DEDCT_CMPNT_OOP_USAGE_CD"),
    col("DEDCT_CMPNT_OOP_USAGE_NM").alias("DEDCT_CMPNT_OOP_USAGE_NM"),
    col("DEDCT_CMPNT_TYP_CD").alias("DEDCT_CMPNT_TYP_CD"),
    col("DEDCT_CMPNT_TYP_NM").alias("DEDCT_CMPNT_TYP_NM"),
    col("STOP_LOSS_IN").alias("STOP_LOSS_IN"),
    col("FMLY_DEDCT_AMT").alias("FMLY_DEDCT_AMT"),
    col("FMLY_DEDCT_CAROVR_AMT").alias("FMLY_DEDCT_CAROVR_AMT"),
    col("MBR_DEDCT_AMT").alias("MBR_DEDCT_AMT"),
    col("MBR_DEDCT_CAROVR_AMT").alias("MBR_DEDCT_CAROVR_AMT"),
    col("FMLY_DEDCT_CAROVR_PRSN_CT").alias("FMLY_DEDCT_CAROVR_PRSN_CT"),
    col("FMLY_DEDCT_PRSN_CT").alias("FMLY_DEDCT_PRSN_CT"),
    col("REL_ACCUM_NO").alias("REL_ACCUM_NO"),
    col("DEDCT_CMPNT_DESC").alias("DEDCT_CMPNT_DESC"),
    col("LAST_UPDT_RUN_CYC_NO").alias("LAST_UPDT_RUN_CYC_NO"),
)

df_Odbc_PROD_DM_DEDCT_CMPNT_out = df_cpy_forBuffer.withColumn(
    "STOP_LOSS_IN", rpad(col("STOP_LOSS_IN"), 1, " ")
).select(
    col("SRC_SYS_CD"),
    col("ACCUM_NO"),
    col("DEDCT_CMPNT_ID"),
    col("DEDCT_CMPNT_ACCUM_PERD_CD"),
    col("DEDCT_CMPNT_ACCUM_PERD_NM"),
    col("DEDCT_CMPNT_OOP_USAGE_CD"),
    col("DEDCT_CMPNT_OOP_USAGE_NM"),
    col("DEDCT_CMPNT_TYP_CD"),
    col("DEDCT_CMPNT_TYP_NM"),
    col("STOP_LOSS_IN"),
    col("FMLY_DEDCT_AMT"),
    col("FMLY_DEDCT_CAROVR_AMT"),
    col("MBR_DEDCT_AMT"),
    col("MBR_DEDCT_CAROVR_AMT"),
    col("FMLY_DEDCT_CAROVR_PRSN_CT"),
    col("FMLY_DEDCT_PRSN_CT"),
    col("REL_ACCUM_NO"),
    col("DEDCT_CMPNT_DESC"),
    col("LAST_UPDT_RUN_CYC_NO"),
)

jdbc_url, jdbc_props = get_db_config(clmmart_secret_name)
execute_dml(
    "DROP TABLE IF EXISTS STAGING.IdsDmProdDmDedctCmpntLoad_Odbc_PROD_DM_DEDCT_CMPNT_out_temp",
    jdbc_url,
    jdbc_props
)

df_Odbc_PROD_DM_DEDCT_CMPNT_out.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.IdsDmProdDmDedctCmpntLoad_Odbc_PROD_DM_DEDCT_CMPNT_out_temp") \
    .mode("append") \
    .save()

merge_sql = f"""
MERGE INTO {ClmMartOwner}.PROD_DM_DEDCT_CMPNT AS T
USING STAGING.IdsDmProdDmDedctCmpntLoad_Odbc_PROD_DM_DEDCT_CMPNT_out_temp AS S
ON (
  T.SRC_SYS_CD = S.SRC_SYS_CD
  AND T.ACCUM_NO = S.ACCUM_NO
  AND T.DEDCT_CMPNT_ID = S.DEDCT_CMPNT_ID
)
WHEN MATCHED THEN
  UPDATE SET
    T.SRC_SYS_CD = S.SRC_SYS_CD,
    T.ACCUM_NO = S.ACCUM_NO,
    T.DEDCT_CMPNT_ID = S.DEDCT_CMPNT_ID,
    T.DEDCT_CMPNT_ACCUM_PERD_CD = S.DEDCT_CMPNT_ACCUM_PERD_CD,
    T.DEDCT_CMPNT_ACCUM_PERD_NM = S.DEDCT_CMPNT_ACCUM_PERD_NM,
    T.DEDCT_CMPNT_OOP_USAGE_CD = S.DEDCT_CMPNT_OOP_USAGE_CD,
    T.DEDCT_CMPNT_OOP_USAGE_NM = S.DEDCT_CMPNT_OOP_USAGE_NM,
    T.DEDCT_CMPNT_TYP_CD = S.DEDCT_CMPNT_TYP_CD,
    T.DEDCT_CMPNT_TYP_NM = S.DEDCT_CMPNT_TYP_NM,
    T.STOP_LOSS_IN = S.STOP_LOSS_IN,
    T.FMLY_DEDCT_AMT = S.FMLY_DEDCT_AMT,
    T.FMLY_DEDCT_CAROVR_AMT = S.FMLY_DEDCT_CAROVR_AMT,
    T.MBR_DEDCT_AMT = S.MBR_DEDCT_AMT,
    T.MBR_DEDCT_CAROVR_AMT = S.MBR_DEDCT_CAROVR_AMT,
    T.FMLY_DEDCT_CAROVR_PRSN_CT = S.FMLY_DEDCT_CAROVR_PRSN_CT,
    T.FMLY_DEDCT_PRSN_CT = S.FMLY_DEDCT_PRSN_CT,
    T.REL_ACCUM_NO = S.REL_ACCUM_NO,
    T.DEDCT_CMPNT_DESC = S.DEDCT_CMPNT_DESC,
    T.LAST_UPDT_RUN_CYC_NO = S.LAST_UPDT_RUN_CYC_NO
WHEN NOT MATCHED THEN
  INSERT (
    SRC_SYS_CD,
    ACCUM_NO,
    DEDCT_CMPNT_ID,
    DEDCT_CMPNT_ACCUM_PERD_CD,
    DEDCT_CMPNT_ACCUM_PERD_NM,
    DEDCT_CMPNT_OOP_USAGE_CD,
    DEDCT_CMPNT_OOP_USAGE_NM,
    DEDCT_CMPNT_TYP_CD,
    DEDCT_CMPNT_TYP_NM,
    STOP_LOSS_IN,
    FMLY_DEDCT_AMT,
    FMLY_DEDCT_CAROVR_AMT,
    MBR_DEDCT_AMT,
    MBR_DEDCT_CAROVR_AMT,
    FMLY_DEDCT_CAROVR_PRSN_CT,
    FMLY_DEDCT_PRSN_CT,
    REL_ACCUM_NO,
    DEDCT_CMPNT_DESC,
    LAST_UPDT_RUN_CYC_NO
  )
  VALUES (
    S.SRC_SYS_CD,
    S.ACCUM_NO,
    S.DEDCT_CMPNT_ID,
    S.DEDCT_CMPNT_ACCUM_PERD_CD,
    S.DEDCT_CMPNT_ACCUM_PERD_NM,
    S.DEDCT_CMPNT_OOP_USAGE_CD,
    S.DEDCT_CMPNT_OOP_USAGE_NM,
    S.DEDCT_CMPNT_TYP_CD,
    S.DEDCT_CMPNT_TYP_NM,
    S.STOP_LOSS_IN,
    S.FMLY_DEDCT_AMT,
    S.FMLY_DEDCT_CAROVR_AMT,
    S.MBR_DEDCT_AMT,
    S.MBR_DEDCT_CAROVR_AMT,
    S.FMLY_DEDCT_CAROVR_PRSN_CT,
    S.FMLY_DEDCT_PRSN_CT,
    S.REL_ACCUM_NO,
    S.DEDCT_CMPNT_DESC,
    S.LAST_UPDT_RUN_CYC_NO
  );
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)

schema_rej = StructType([
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("ACCUM_NO", IntegerType(), True),
    StructField("DEDCT_CMPNT_ID", StringType(), True),
    StructField("DEDCT_CMPNT_ACCUM_PERD_CD", StringType(), True),
    StructField("DEDCT_CMPNT_ACCUM_PERD_NM", StringType(), True),
    StructField("DEDCT_CMPNT_OOP_USAGE_CD", StringType(), True),
    StructField("DEDCT_CMPNT_OOP_USAGE_NM", StringType(), True),
    StructField("DEDCT_CMPNT_TYP_CD", StringType(), True),
    StructField("DEDCT_CMPNT_TYP_NM", StringType(), True),
    StructField("STOP_LOSS_IN", StringType(), True),
    StructField("FMLY_DEDCT_AMT", DecimalType(38,10), True),
    StructField("FMLY_DEDCT_CAROVR_AMT", DecimalType(38,10), True),
    StructField("MBR_DEDCT_AMT", DecimalType(38,10), True),
    StructField("MBR_DEDCT_CAROVR_AMT", DecimalType(38,10), True),
    StructField("FMLY_DEDCT_CAROVR_PRSN_CT", IntegerType(), True),
    StructField("FMLY_DEDCT_PRSN_CT", IntegerType(), True),
    StructField("REL_ACCUM_NO", IntegerType(), True),
    StructField("DEDCT_CMPNT_DESC", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_NO", IntegerType(), True),
    StructField("ERRORCODE", StringType(), True),
    StructField("ERRORTEXT", StringType(), True),
])

df_seq_DNTL_CAT_RULE_D_csv_rej = spark.createDataFrame([], schema_rej)

write_files(
    df_seq_DNTL_CAT_RULE_D_csv_rej.select(
        "SRC_SYS_CD",
        "ACCUM_NO",
        "DEDCT_CMPNT_ID",
        "DEDCT_CMPNT_ACCUM_PERD_CD",
        "DEDCT_CMPNT_ACCUM_PERD_NM",
        "DEDCT_CMPNT_OOP_USAGE_CD",
        "DEDCT_CMPNT_OOP_USAGE_NM",
        "DEDCT_CMPNT_TYP_CD",
        "DEDCT_CMPNT_TYP_NM",
        "STOP_LOSS_IN",
        "FMLY_DEDCT_AMT",
        "FMLY_DEDCT_CAROVR_AMT",
        "MBR_DEDCT_AMT",
        "MBR_DEDCT_CAROVR_AMT",
        "FMLY_DEDCT_CAROVR_PRSN_CT",
        "FMLY_DEDCT_PRSN_CT",
        "REL_ACCUM_NO",
        "DEDCT_CMPNT_DESC",
        "LAST_UPDT_RUN_CYC_NO",
        "ERRORCODE",
        "ERRORTEXT"
    ),
    f"{adls_path}/load/PROD_DM_DEDCT_CMPNT_Rej.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)