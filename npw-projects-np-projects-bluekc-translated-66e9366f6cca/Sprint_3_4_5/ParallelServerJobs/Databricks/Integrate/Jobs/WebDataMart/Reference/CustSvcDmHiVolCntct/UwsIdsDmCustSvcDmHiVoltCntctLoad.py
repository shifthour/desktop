# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        ---------------------------------------------------------------------------                             ------------------------------        ------------------------------       --------------------
# MAGIC Archana Palivela            03/26/2014         5114                                   Original Programming                                                              IntegrateWrhsDevl        Bhoomi Dasari             4/6/2014

# MAGIC Job Name: UwsIdsDmCustSvcDmHiVoltCntctExtr
# MAGIC Read Load File created in the UwsIdsDmCustSvcDmHiVoltCntctExtr Job
# MAGIC Copy Stage for buffer
# MAGIC Clear the Table and Insert New Rows CUST_SVC_DM_HI_VOL_CNTCT Data.
# MAGIC Load rejects are redirected into a Reject file
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
import pyspark.sql.functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


ClmMartOwner = get_widget_value('ClmMartOwner','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')
ClmMartArraySize = get_widget_value('ClmMartArraySize','')
ClmMartRecordCount = get_widget_value('ClmMartRecordCount','')

schema_seq_CUST_SVC_DM_HI_VOL_CNTCT_csv_load = StructType([
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("TM_PERD_CD", StringType(), nullable=False),
    StructField("EFF_DT", TimestampType(), nullable=False),
    StructField("TERM_DT", TimestampType(), nullable=False),
    StructField("CNTCT_THRSHLD_NO", IntegerType(), nullable=False),
    StructField("USER_ID", StringType(), nullable=False),
    StructField("LAST_UPDT_DT", TimestampType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_NO", IntegerType(), nullable=False)
])

df_seq_CUST_SVC_DM_HI_VOL_CNTCT_csv_load = (
    spark.read.format("csv")
    .option("delimiter", ",")
    .option("quote", "^")
    .option("header", "false")
    .option("nullValue", None)
    .schema(schema_seq_CUST_SVC_DM_HI_VOL_CNTCT_csv_load)
    .load(f"{adls_path}/load/CUST_SVC_DM_HI_VOL_CNTCT.dat")
)

df_cpy_forBuffer = df_seq_CUST_SVC_DM_HI_VOL_CNTCT_csv_load.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("TM_PERD_CD").alias("TM_PERD_CD"),
    F.col("EFF_DT").alias("EFF_DT"),
    F.col("TERM_DT").alias("TERM_DT"),
    F.col("CNTCT_THRSHLD_NO").alias("CNTCT_THRSHLD_NO"),
    F.col("USER_ID").alias("USER_ID"),
    F.col("LAST_UPDT_DT").alias("LAST_UPDT_DT"),
    F.col("LAST_UPDT_RUN_CYC_NO").alias("LAST_UPDT_RUN_CYC_NO")
)

df_final_odbc_CUST_SVC_DM_HI_VOL_CNTCT_out = (
    df_cpy_forBuffer
    .withColumn("SRC_SYS_CD", F.rpad(F.col("SRC_SYS_CD"), 1, " "))
    .withColumn("TM_PERD_CD", F.rpad(F.col("TM_PERD_CD"), 1, " "))
    .withColumn("USER_ID", F.rpad(F.col("USER_ID"), 1, " "))
    .select(
        "SRC_SYS_CD",
        "TM_PERD_CD",
        "EFF_DT",
        "TERM_DT",
        "CNTCT_THRSHLD_NO",
        "USER_ID",
        "LAST_UPDT_DT",
        "LAST_UPDT_RUN_CYC_NO"
    )
)

jdbc_url, jdbc_props = get_db_config(clmmart_secret_name)
staging_table = "STAGING.UwsIdsDmCustSvcDmHiVoltCntctLoad_odbc_CUST_SVC_DM_HI_VOL_CNTCT_out_temp"
target_table = f"{ClmMartOwner}.CUST_SVC_DM_HI_VOL_CNTCT"

execute_dml(f"DROP TABLE IF EXISTS {staging_table}", jdbc_url, jdbc_props)

df_final_odbc_CUST_SVC_DM_HI_VOL_CNTCT_out.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", staging_table) \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO {target_table} AS T
USING {staging_table} AS S
    ON T.SRC_SYS_CD = S.SRC_SYS_CD
    AND T.TM_PERD_CD = S.TM_PERD_CD
    AND T.EFF_DT = S.EFF_DT
WHEN MATCHED THEN UPDATE SET
    T.TERM_DT = S.TERM_DT,
    T.CNTCT_THRSHLD_NO = S.CNTCT_THRSHLD_NO,
    T.USER_ID = S.USER_ID,
    T.LAST_UPDT_DT = S.LAST_UPDT_DT,
    T.LAST_UPDT_RUN_CYC_NO = S.LAST_UPDT_RUN_CYC_NO
WHEN NOT MATCHED THEN INSERT
(
    SRC_SYS_CD,
    TM_PERD_CD,
    EFF_DT,
    TERM_DT,
    CNTCT_THRSHLD_NO,
    USER_ID,
    LAST_UPDT_DT,
    LAST_UPDT_RUN_CYC_NO
)
VALUES
(
    S.SRC_SYS_CD,
    S.TM_PERD_CD,
    S.EFF_DT,
    S.TERM_DT,
    S.CNTCT_THRSHLD_NO,
    S.USER_ID,
    S.LAST_UPDT_DT,
    S.LAST_UPDT_RUN_CYC_NO
);
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)

schema_seq_CUST_SVC_DM_HI_VOL_CNTCT_csv_load_Rej = StructType([
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("TM_PERD_CD", StringType(), True),
    StructField("EFF_DT", TimestampType(), True),
    StructField("TERM_DT", TimestampType(), True),
    StructField("CNTCT_THRSHLD_NO", IntegerType(), True),
    StructField("USER_ID", StringType(), True),
    StructField("LAST_UPDT_DT", TimestampType(), True),
    StructField("LAST_UPDT_RUN_CYC_NO", IntegerType(), True),
    StructField("ERRORCODE", StringType(), True),
    StructField("ERRORTEXT", StringType(), True)
])

df_seq_CUST_SVC_DM_HI_VOL_CNTCT_csv_load_Rej = spark.createDataFrame([], schema_seq_CUST_SVC_DM_HI_VOL_CNTCT_csv_load_Rej)

df_final_seq_CUST_SVC_DM_HI_VOL_CNTCT_csv_load_Rej = (
    df_seq_CUST_SVC_DM_HI_VOL_CNTCT_csv_load_Rej
    .withColumn("SRC_SYS_CD", F.rpad(F.col("SRC_SYS_CD"), 1, " "))
    .withColumn("TM_PERD_CD", F.rpad(F.col("TM_PERD_CD"), 1, " "))
    .withColumn("USER_ID", F.rpad(F.col("USER_ID"), 1, " "))
    .select(
        "SRC_SYS_CD",
        "TM_PERD_CD",
        "EFF_DT",
        "TERM_DT",
        "CNTCT_THRSHLD_NO",
        "USER_ID",
        "LAST_UPDT_DT",
        "LAST_UPDT_RUN_CYC_NO",
        "ERRORCODE",
        "ERRORTEXT"
    )
)

write_files(
    df_final_seq_CUST_SVC_DM_HI_VOL_CNTCT_csv_load_Rej,
    f"{adls_path}/load/CUST_SVC_DM_HI_VOL_CNTCT_Rej.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)