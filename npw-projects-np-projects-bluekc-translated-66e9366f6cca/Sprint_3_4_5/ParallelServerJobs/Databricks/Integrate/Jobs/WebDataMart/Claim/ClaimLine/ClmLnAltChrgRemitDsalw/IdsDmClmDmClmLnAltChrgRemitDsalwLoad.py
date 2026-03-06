# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                                                DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                                                    ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------                                 ------------------------------    ------------------------------       --------------------
# MAGIC Nagesh Bandi          09/10/2013          5114                             Movies Data from IDS to  CLM_DM_CLM_LN_ALT_CHRG_REMIT_DSALW                      IntegrateWrhsDevl

# MAGIC Job Name: IdsDmClmDmClmLnAltChrgRemitDsalwLoad
# MAGIC Read Load File created in the IdsDmClmDmClmLnAltChrgRemitSExtr Job
# MAGIC Copy Stage for buffer
# MAGIC Update and  Insert the CLM_DM_CLM_LN_ALT_CHRG_REMIT_DSALW  Data.
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
clmmartowner_secret_name = get_widget_value('clmmartowner_secret_name','')
ClmMartArraySize = get_widget_value('ClmMartArraySize','')
ClmMartRecordCount = get_widget_value('ClmMartRecordCount','')

schema_seq_CLM_DM_CLM_LN_ALT_CHRG_REMIT_DSALW = StructType([
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("CLM_ID", StringType(), False),
    StructField("CLM_LN_SEQ_NO", IntegerType(), False),
    StructField("CLM_LN_DSALW_TYP_CD", StringType(), False),
    StructField("CLM_LN_REMIT_DSALW_AMT", DecimalType(15, 2), False),
    StructField("CLM_LN_REMIT_DSALW_EXCD_ID", StringType(), False),
    StructField("CLM_LN_REMIT_DSALW_EXCD_DESC", StringType(), True),
    StructField("CLM_LN_RMT_DSALW_EXCD_RESP_CD", StringType(), False),
    StructField("CLM_LN_RMT_DSALW_EXCD_RESP_NM", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_NO", IntegerType(), False)
])

df_seq_CLM_DM_CLM_LN_ALT_CHRG_REMIT_DSALW_csv_load = (
    spark.read.format("csv")
    .option("delimiter", ",")
    .option("quote", "^")
    .option("header", "false")
    .schema(schema_seq_CLM_DM_CLM_LN_ALT_CHRG_REMIT_DSALW)
    .load(f"{adls_path}/load/CLM_DM_CLM_LN_ALT_CHRG_REMIT_DSALW.dat")
)

df_cpy_forBuffer = df_seq_CLM_DM_CLM_LN_ALT_CHRG_REMIT_DSALW_csv_load.select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("CLM_ID").alias("CLM_ID"),
    col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    col("CLM_LN_DSALW_TYP_CD").alias("CLM_LN_DSALW_TYP_CD"),
    col("CLM_LN_REMIT_DSALW_AMT").alias("CLM_LN_REMIT_DSALW_AMT"),
    col("CLM_LN_REMIT_DSALW_EXCD_ID").alias("CLM_LN_REMIT_DSALW_EXCD_ID"),
    col("CLM_LN_REMIT_DSALW_EXCD_DESC").alias("CLM_LN_REMIT_DSALW_EXCD_DESC"),
    col("CLM_LN_RMT_DSALW_EXCD_RESP_CD").alias("CLM_LN_RMT_DSALW_EXCD_RESP_CD"),
    col("CLM_LN_RMT_DSALW_EXCD_RESP_NM").alias("CLM_LN_RMT_DSALW_EXCD_RESP_NM"),
    col("LAST_UPDT_RUN_CYC_NO").alias("LAST_UPDT_RUN_CYC_NO")
)

jdbc_url, jdbc_props = get_db_config(clmmartowner_secret_name)

staging_table_name = "STAGING.IdsDmClmDmClmLnAltChrgRemitDsalwLoad_Odbc_CLM_DM_CLM_LN_ALT_CHRG_REMIT_DSALW_out_temp"
execute_dml(f"DROP TABLE IF EXISTS {staging_table_name}", jdbc_url, jdbc_props)

df_cpy_forBuffer.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", staging_table_name) \
    .mode("overwrite") \
    .save()

merge_sql = """
MERGE INTO #$ClmMartOwner#.CLM_DM_CLM_LN_ALT_CHRG_REMIT_DSALW AS T
USING {staging_table} AS S
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
    ) VALUES (
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
""".replace("{staging_table}", staging_table_name)

execute_dml(merge_sql, jdbc_url, jdbc_props)

schema_odbc_reject = StructType([
    StructField("ERRORCODE", StringType(), True),
    StructField("ERRORTEXT", StringType(), True)
])

df_odbc_reject = spark.createDataFrame([], schema_odbc_reject)

df_odbc_reject_final = df_odbc_reject.select(
    rpad(col("ERRORCODE"), 100, " ").alias("ERRORCODE"),
    rpad(col("ERRORTEXT"), 100, " ").alias("ERRORTEXT")
)

write_files(
    df_odbc_reject_final,
    f"{adls_path}/load/CLM_DM_CLM_LN_ALT_CHRG_REMIT_DSALW_Rej.dat",
    ",",
    "append",
    False,
    False,
    "^",
    None
)