# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Nagesh Bandi          09/05/2013          5114                             Movies Data from IDS to  CLM_DM_CLM_LN_ALT_CHRG                    IntegrateWrhsDevl

# MAGIC Job Name: IdsDmClmDmClmLnAltChrgLoad
# MAGIC Read Load File created in the IdsDmClmDmClmLnAltChrgExtr Job
# MAGIC Copy Stage for buffer
# MAGIC Update and  Insert the CLM_DM_CLM_LN_ALT_CHRG  Data.
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
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


ClmMartOwner = get_widget_value('ClmMartOwner','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')
ClmMartArraySize = get_widget_value('ClmMartArraySize','')
ClmMartRecordCount = get_widget_value('ClmMartRecordCount','')

schema_seq_CLM_DM_CLM_LN_ALT_CHRG_csv_load = StructType([
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("CLM_ID", StringType(), True),
    StructField("CLM_LN_SEQ_NO", IntegerType(), True),
    StructField("CLM_LN_REMIT_PATN_RESP_AMT", DecimalType(15, 2), True),
    StructField("CLM_LN_REMIT_PROV_WRT_OFF_AMT", DecimalType(15, 2), True),
    StructField("CLM_LN_REMIT_MBR_OTHR_LIAB_AMT", DecimalType(15, 2), True),
    StructField("CLM_LN_REMIT_NO_RESP_AMT", DecimalType(15, 2), True),
    StructField("LAST_UPDT_RUN_CYC_NO", IntegerType(), True)
])

df_seq_CLM_DM_CLM_LN_ALT_CHRG_csv_load = (
    spark.read
    .option("header", False)
    .option("inferSchema", False)
    .option("delimiter", ",")
    .option("quote", "^")
    .option("nullValue", None)
    .schema(schema_seq_CLM_DM_CLM_LN_ALT_CHRG_csv_load)
    .csv(f"{adls_path}/load/CLM_DM_CLM_LN_ALT_CHRG.dat")
)

df_cpy_forBuffer = df_seq_CLM_DM_CLM_LN_ALT_CHRG_csv_load.selectExpr(
    "SRC_SYS_CD as SRC_SYS_CD",
    "CLM_ID as CLM_ID",
    "CLM_LN_SEQ_NO as CLM_LN_SEQ_NO",
    "CLM_LN_REMIT_PATN_RESP_AMT as CLM_LN_REMIT_PATN_RESP_AMT",
    "CLM_LN_REMIT_PROV_WRT_OFF_AMT as CLM_LN_REMIT_PROV_WRT_OFF_AMT",
    "CLM_LN_REMIT_MBR_OTHR_LIAB_AMT as CLM_LN_REMIT_MBR_OTHR_LIAB_AMT",
    "CLM_LN_REMIT_NO_RESP_AMT as CLM_LN_REMIT_NO_RESP_AMT",
    "LAST_UPDT_RUN_CYC_NO as LAST_UPDT_RUN_CYC_NO"
)

jdbc_url, jdbc_props = get_db_config(clmmart_secret_name)

execute_dml(
    f"DROP TABLE IF EXISTS STAGING.IdsDmClmDmClmLnAltChrgLoad_Odbc_CLM_DM_CLM_LN_ALT_CHRG_out_temp",
    jdbc_url,
    jdbc_props
)

df_cpy_forBuffer.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.IdsDmClmDmClmLnAltChrgLoad_Odbc_CLM_DM_CLM_LN_ALT_CHRG_out_temp") \
    .mode("append") \
    .save()

merge_sql = f"""
MERGE {ClmMartOwner}.CLM_DM_CLM_LN_ALT_CHRG AS T
USING STAGING.IdsDmClmDmClmLnAltChrgLoad_Odbc_CLM_DM_CLM_LN_ALT_CHRG_out_temp AS S
ON
    T.SRC_SYS_CD = S.SRC_SYS_CD
    AND T.CLM_ID = S.CLM_ID
    AND T.CLM_LN_SEQ_NO = S.CLM_LN_SEQ_NO
WHEN MATCHED THEN
    UPDATE SET
        T.CLM_LN_REMIT_PATN_RESP_AMT = S.CLM_LN_REMIT_PATN_RESP_AMT,
        T.CLM_LN_REMIT_PROV_WRT_OFF_AMT = S.CLM_LN_REMIT_PROV_WRT_OFF_AMT,
        T.CLM_LN_REMIT_MBR_OTHR_LIAB_AMT = S.CLM_LN_REMIT_MBR_OTHR_LIAB_AMT,
        T.CLM_LN_REMIT_NO_RESP_AMT = S.CLM_LN_REMIT_NO_RESP_AMT,
        T.LAST_UPDT_RUN_CYC_NO = S.LAST_UPDT_RUN_CYC_NO
WHEN NOT MATCHED THEN
    INSERT (
        SRC_SYS_CD,
        CLM_ID,
        CLM_LN_SEQ_NO,
        CLM_LN_REMIT_PATN_RESP_AMT,
        CLM_LN_REMIT_PROV_WRT_OFF_AMT,
        CLM_LN_REMIT_MBR_OTHR_LIAB_AMT,
        CLM_LN_REMIT_NO_RESP_AMT,
        LAST_UPDT_RUN_CYC_NO
    )
    VALUES (
        S.SRC_SYS_CD,
        S.CLM_ID,
        S.CLM_LN_SEQ_NO,
        S.CLM_LN_REMIT_PATN_RESP_AMT,
        S.CLM_LN_REMIT_PROV_WRT_OFF_AMT,
        S.CLM_LN_REMIT_MBR_OTHR_LIAB_AMT,
        S.CLM_LN_REMIT_NO_RESP_AMT,
        S.LAST_UPDT_RUN_CYC_NO
    )
;
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)

schema_seq_CLM_DM_CLM_LN_ALT_CHRG_csv_rej = StructType([
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("CLM_ID", StringType(), True),
    StructField("CLM_LN_SEQ_NO", IntegerType(), True),
    StructField("CLM_LN_REMIT_PATN_RESP_AMT", DecimalType(15, 2), True),
    StructField("CLM_LN_REMIT_PROV_WRT_OFF_AMT", DecimalType(15, 2), True),
    StructField("CLM_LN_REMIT_MBR_OTHR_LIAB_AMT", DecimalType(15, 2), True),
    StructField("CLM_LN_REMIT_NO_RESP_AMT", DecimalType(15, 2), True),
    StructField("LAST_UPDT_RUN_CYC_NO", IntegerType(), True),
    StructField("ERRORCODE", StringType(), True),
    StructField("ERRORTEXT", StringType(), True)
])

df_seq_CLM_DM_CLM_LN_ALT_CHRG_csv_rej = spark.createDataFrame([], schema_seq_CLM_DM_CLM_LN_ALT_CHRG_csv_rej)

final_df_seq_CLM_DM_CLM_LN_ALT_CHRG_csv_rej = df_seq_CLM_DM_CLM_LN_ALT_CHRG_csv_rej.select(
    "SRC_SYS_CD",
    "CLM_ID",
    "CLM_LN_SEQ_NO",
    "CLM_LN_REMIT_PATN_RESP_AMT",
    "CLM_LN_REMIT_PROV_WRT_OFF_AMT",
    "CLM_LN_REMIT_MBR_OTHR_LIAB_AMT",
    "CLM_LN_REMIT_NO_RESP_AMT",
    "LAST_UPDT_RUN_CYC_NO",
    "ERRORCODE",
    "ERRORTEXT"
)

write_files(
    final_df_seq_CLM_DM_CLM_LN_ALT_CHRG_csv_rej,
    f"{adls_path}/load/CLM_DM_CLM_LN_ALT_CHRG_Rej.dat",
    delimiter=",",
    mode="append",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)