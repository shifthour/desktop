# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                                                DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                                                    ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------                                 ------------------------------    ------------------------------       --------------------
# MAGIC Nagesh Bandi          09/12/2013          5114                             Movies Data from IDS to  CLM_DM_CLM_LN_PROC_CD_MOD                                       IntegrateWrhsDevl

# MAGIC Job Name: IdsDmClmDmClmLnProcCdModLoad
# MAGIC Read Load File created in the 
# MAGIC IdsDmClmDmClmLnProcCdModExtr Job
# MAGIC Copy Stage for buffer
# MAGIC Update and  Insert the CLM_DM_CLM_LN_PROC_CD_MOD  Data.
# MAGIC Load rejects are redirected into a Reject file
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


ClmMartOwner = get_widget_value('ClmMartOwner','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')
ClmMartArraySize = get_widget_value('ClmMartArraySize','')
ClmMartRecordCount = get_widget_value('ClmMartRecordCount','')

schema_seq_CLM_DM_CLM_LN_PROC_CD_MOD_csv_load = StructType([
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("CLM_ID", StringType(), False),
    StructField("CLM_LN_SEQ_NO", IntegerType(), False),
    StructField("CLM_LN_PROC_CD_MOD_ORDNL_CD", StringType(), False),
    StructField("PROC_CD_MOD_TX", StringType(), False),
    StructField("LAST_UPDT_RUN_CYC_NO", IntegerType(), False)
])

df_seq_CLM_DM_CLM_LN_PROC_CD_MOD_csv_load = (
    spark.read
    .option("delimiter", ",")
    .option("quote", "^")
    .option("header", "false")
    .schema(schema_seq_CLM_DM_CLM_LN_PROC_CD_MOD_csv_load)
    .csv(f"{adls_path}/load/CLM_DM_CLM_LN_PROC_CD_MOD.dat")
)

df_cpy_forBuffer = (
    df_seq_CLM_DM_CLM_LN_PROC_CD_MOD_csv_load
    .select(
        F.col("SRC_SYS_CD"),
        F.col("CLM_ID"),
        F.col("CLM_LN_SEQ_NO"),
        F.col("CLM_LN_PROC_CD_MOD_ORDNL_CD"),
        F.rpad(F.col("PROC_CD_MOD_TX"), 2, " ").alias("PROC_CD_MOD_TX"),
        F.col("LAST_UPDT_RUN_CYC_NO")
    )
)

jdbc_url, jdbc_props = get_db_config(clmmart_secret_name)
merge_temp_table = "STAGING.IdsDmClmDmClmLnProcCdModLoad_Odbc_CLM_DM_CLM_LN_PROC_CD_MOD_out_temp"

execute_dml(f"DROP TABLE IF EXISTS {merge_temp_table}", jdbc_url, jdbc_props)

df_cpy_forBuffer.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", merge_temp_table) \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO {ClmMartOwner}.CLM_DM_CLM_LN_PROC_CD_MOD AS T
USING {merge_temp_table} AS S
ON
    T.SRC_SYS_CD = S.SRC_SYS_CD
    AND T.CLM_ID = S.CLM_ID
    AND T.CLM_LN_SEQ_NO = S.CLM_LN_SEQ_NO
    AND T.CLM_LN_PROC_CD_MOD_ORDNL_CD = S.CLM_LN_PROC_CD_MOD_ORDNL_CD
WHEN MATCHED THEN
    UPDATE SET
        T.PROC_CD_MOD_TX = S.PROC_CD_MOD_TX,
        T.LAST_UPDT_RUN_CYC_NO = S.LAST_UPDT_RUN_CYC_NO
WHEN NOT MATCHED THEN
    INSERT
        (SRC_SYS_CD,
         CLM_ID,
         CLM_LN_SEQ_NO,
         CLM_LN_PROC_CD_MOD_ORDNL_CD,
         PROC_CD_MOD_TX,
         LAST_UPDT_RUN_CYC_NO)
    VALUES
        (S.SRC_SYS_CD,
         S.CLM_ID,
         S.CLM_LN_SEQ_NO,
         S.CLM_LN_PROC_CD_MOD_ORDNL_CD,
         S.PROC_CD_MOD_TX,
         S.LAST_UPDT_RUN_CYC_NO);
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)

schema_reject = StructType([
    StructField("ERRORCODE", StringType(), True),
    StructField("ERRORTEXT", StringType(), True)
])
df_reject_Odbc_CLM_DM_CLM_LN_PROC_CD_MOD_out = spark.createDataFrame([], schema_reject)
df_reject_Odbc_CLM_DM_CLM_LN_PROC_CD_MOD_out_final = df_reject_Odbc_CLM_DM_CLM_LN_PROC_CD_MOD_out.select("ERRORCODE", "ERRORTEXT")

write_files(
    df_reject_Odbc_CLM_DM_CLM_LN_PROC_CD_MOD_out_final,
    f"{adls_path}/load/CLM_DM_CLM_LN_PROC_CD_MOD_Rej.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)