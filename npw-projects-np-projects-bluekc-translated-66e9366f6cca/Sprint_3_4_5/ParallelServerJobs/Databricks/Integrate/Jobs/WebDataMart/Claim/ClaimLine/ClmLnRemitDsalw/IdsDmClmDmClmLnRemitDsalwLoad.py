# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                                                DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                                                    ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------                                 ------------------------------    ------------------------------       --------------------
# MAGIC Nagesh Bandi          09/10/2013          5114                             Movies Data from IDS to  CLM_DM_CLM_LN_REMIT_DSALW                                        IntegrateWrhsDevl

# MAGIC Job Name: IdsDmClmDmClmLnRemitDsalwLoad
# MAGIC Read Load File created in the IdsDmClmDmClmLnAltChrgRemitDsalwExtr Job
# MAGIC Copy Stage for buffer
# MAGIC Update and  Insert the CLM_DM_CLM_LN_REMIT_DSALW  Data.
# MAGIC Load rejects are redirected into a Reject file
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
from pyspark.sql.functions import col, rpad
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------

ClmMartOwner = get_widget_value('ClmMartOwner','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')
ClmMartArraySize = get_widget_value('ClmMartArraySize','')
ClmMartRecordCount = get_widget_value('ClmMartRecordCount','')

schema_seq_CLM_DM_CLM_LN_REMIT_DSALW_csv_load = StructType([
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("CLM_ID", StringType(), True),
    StructField("CLM_LN_SEQ_NO", IntegerType(), True),
    StructField("CLM_LN_DSALW_TYP_CD", StringType(), True),
    StructField("CLM_LN_REMIT_DSALW_AMT", DecimalType(38,10), True),
    StructField("CLM_LN_REMIT_DSALW_EXCD_ID", StringType(), True),
    StructField("CLM_LN_REMIT_DSALW_EXCD_DESC", StringType(), True),
    StructField("CLM_LN_RMT_DSALW_EXCD_RESP_CD", StringType(), True),
    StructField("CLM_LN_RMT_DSALW_EXCD_RESP_NM", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_NO", IntegerType(), True)
])

df_seq_CLM_DM_CLM_LN_REMIT_DSALW_csv_load = (
    spark.read.format("csv")
    .option("header", "false")
    .option("sep", ",")
    .option("quote", "^")
    .option("nullValue", None)
    .schema(schema_seq_CLM_DM_CLM_LN_REMIT_DSALW_csv_load)
    .load(f"{adls_path}/load/CLM_DM_CLM_LN_REMIT_DSALW.dat")
)

df_cpy_forBuffer = df_seq_CLM_DM_CLM_LN_REMIT_DSALW_csv_load.select(
    col("SRC_SYS_CD"),
    col("CLM_ID"),
    col("CLM_LN_SEQ_NO"),
    col("CLM_LN_DSALW_TYP_CD"),
    col("CLM_LN_REMIT_DSALW_AMT"),
    col("CLM_LN_REMIT_DSALW_EXCD_ID"),
    col("CLM_LN_REMIT_DSALW_EXCD_DESC"),
    col("CLM_LN_RMT_DSALW_EXCD_RESP_CD"),
    col("CLM_LN_RMT_DSALW_EXCD_RESP_NM"),
    col("LAST_UPDT_RUN_CYC_NO")
)

jdbc_url, jdbc_props = get_db_config(clmmart_secret_name)

spark.sql("DROP TABLE IF EXISTS STAGING.IdsDmClmDmClmLnRemitDsalwLoad_Odbc_CLM_DM_CLM_LN_REMIT_DSALW_out_temp")

df_cpy_forBuffer.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.IdsDmClmDmClmLnRemitDsalwLoad_Odbc_CLM_DM_CLM_LN_REMIT_DSALW_out_temp") \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO {ClmMartOwner}.CLM_DM_CLM_LN_REMIT_DSALW AS T
USING STAGING.IdsDmClmDmClmLnRemitDsalwLoad_Odbc_CLM_DM_CLM_LN_REMIT_DSALW_out_temp AS S
ON
    T.SRC_SYS_CD = S.SRC_SYS_CD
    AND T.CLM_ID = S.CLM_ID
    AND T.CLM_LN_SEQ_NO = S.CLM_LN_SEQ_NO
    AND T.CLM_LN_DSALW_TYP_CD = S.CLM_LN_DSALW_TYP_CD
WHEN MATCHED THEN
    UPDATE SET
        T.CLM_LN_REMIT_DSALW_AMT = S.CLM_LN_REMIT_DSALW_AMT,
        T.CLM_LN_REMIT_DSALW_EXCD_ID = S.CLM_LN_REMIT_DSALW_EXCD_ID,
        T.CLM_LN_REMIT_DSALW_EXCD_DESC = S.CLM_LN_REMIT_DSALW_EXCD_DESC,
        T.CLM_LN_RMT_DSALW_EXCD_RESP_CD = S.CLM_LN_RMT_DSALW_EXCD_RESP_CD,
        T.CLM_LN_RMT_DSALW_EXCD_RESP_NM = S.CLM_LN_RMT_DSALW_EXCD_RESP_NM,
        T.LAST_UPDT_RUN_CYC_NO = S.LAST_UPDT_RUN_CYC_NO
WHEN NOT MATCHED THEN
    INSERT (
        SRC_SYS_CD,
        CLM_ID,
        CLM_LN_SEQ_NO,
        CLM_LN_DSALW_TYP_CD,
        CLM_LN_REMIT_DSALW_AMT,
        CLM_LN_REMIT_DSALW_EXCD_ID,
        CLM_LN_REMIT_DSALW_EXCD_DESC,
        CLM_LN_RMT_DSALW_EXCD_RESP_CD,
        CLM_LN_RMT_DSALW_EXCD_RESP_NM,
        LAST_UPDT_RUN_CYC_NO
    )
    VALUES (
        S.SRC_SYS_CD,
        S.CLM_ID,
        S.CLM_LN_SEQ_NO,
        S.CLM_LN_DSALW_TYP_CD,
        S.CLM_LN_REMIT_DSALW_AMT,
        S.CLM_LN_REMIT_DSALW_EXCD_ID,
        S.CLM_LN_REMIT_DSALW_EXCD_DESC,
        S.CLM_LN_RMT_DSALW_EXCD_RESP_CD,
        S.CLM_LN_RMT_DSALW_EXCD_RESP_NM,
        S.LAST_UPDT_RUN_CYC_NO
    );
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)

schema_reject = StructType([
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("CLM_ID", StringType(), True),
    StructField("CLM_LN_SEQ_NO", IntegerType(), True),
    StructField("CLM_LN_DSALW_TYP_CD", StringType(), True),
    StructField("CLM_LN_REMIT_DSALW_AMT", DecimalType(38,10), True),
    StructField("CLM_LN_REMIT_DSALW_EXCD_ID", StringType(), True),
    StructField("CLM_LN_REMIT_DSALW_EXCD_DESC", StringType(), True),
    StructField("CLM_LN_RMT_DSALW_EXCD_RESP_CD", StringType(), True),
    StructField("CLM_LN_RMT_DSALW_EXCD_RESP_NM", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_NO", IntegerType(), True),
    StructField("ERRORCODE", StringType(), True),
    StructField("ERRORTEXT", StringType(), True)
])

df_Odbc_CLM_DM_CLM_LN_REMIT_DSALW_out_rej = spark.createDataFrame([], schema_reject)

df_Odbc_CLM_DM_CLM_LN_REMIT_DSALW_out_rej = df_Odbc_CLM_DM_CLM_LN_REMIT_DSALW_out_rej.select(
    rpad(col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    rpad(col("CLM_ID"), <...>, " ").alias("CLM_ID"),
    col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    rpad(col("CLM_LN_DSALW_TYP_CD"), <...>, " ").alias("CLM_LN_DSALW_TYP_CD"),
    col("CLM_LN_REMIT_DSALW_AMT").alias("CLM_LN_REMIT_DSALW_AMT"),
    rpad(col("CLM_LN_REMIT_DSALW_EXCD_ID"), <...>, " ").alias("CLM_LN_REMIT_DSALW_EXCD_ID"),
    rpad(col("CLM_LN_REMIT_DSALW_EXCD_DESC"), <...>, " ").alias("CLM_LN_REMIT_DSALW_EXCD_DESC"),
    rpad(col("CLM_LN_RMT_DSALW_EXCD_RESP_CD"), <...>, " ").alias("CLM_LN_RMT_DSALW_EXCD_RESP_CD"),
    rpad(col("CLM_LN_RMT_DSALW_EXCD_RESP_NM"), <...>, " ").alias("CLM_LN_RMT_DSALW_EXCD_RESP_NM"),
    col("LAST_UPDT_RUN_CYC_NO").alias("LAST_UPDT_RUN_CYC_NO"),
    rpad(col("ERRORCODE"), <...>, " ").alias("ERRORCODE"),
    rpad(col("ERRORTEXT"), <...>, " ").alias("ERRORTEXT")
)

write_files(
    df_Odbc_CLM_DM_CLM_LN_REMIT_DSALW_out_rej,
    f"{adls_path}/load/CLM_DM_CLM_LN_ALT_CHRG_REMIT_DSALW_Rej.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)