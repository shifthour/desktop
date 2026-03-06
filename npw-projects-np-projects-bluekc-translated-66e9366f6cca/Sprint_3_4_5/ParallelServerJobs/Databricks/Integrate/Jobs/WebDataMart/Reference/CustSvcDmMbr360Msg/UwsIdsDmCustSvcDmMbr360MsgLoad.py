# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        ---------------------------------------------------------------------------                             ------------------------------    ------------------------------       --------------------
# MAGIC Archana Palivela            03/28/2014        5114                                   Original Programming                                                              IntegrateWrhsDevl        Bhoomi Dasari         4/6/2014

# MAGIC Job Name: UwsIdsDmCustSvcDmMbr360MsgLoad
# MAGIC Read Load File created in the UwsIdsDmCustSvcDmMbr360MsgExtr Job
# MAGIC Copy Stage for buffer
# MAGIC Clear the Table and Insert New Rows CUST_SVC_DM_MBR_360_MSG Data.
# MAGIC Load rejects are redirected into a Reject file
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from pyspark.sql.functions import rpad, col
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


ClmMartOwner = get_widget_value('ClmMartOwner','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')
ClmMartArraySize = get_widget_value('ClmMartArraySize','')
ClmMartRecordCount = get_widget_value('ClmMartRecordCount','')

schema_seq_CUST_SVC_DM_MBR_360_MSG_csv_load = StructType([
    StructField("MBR_360_MSG_NO", IntegerType(), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("EFF_DT", TimestampType(), False),
    StructField("TERM_DT", TimestampType(), False),
    StructField("MSG_TX", StringType(), False),
    StructField("USER_ID", StringType(), False),
    StructField("LAST_UPDT_DT", StringType(), False)
])

df_seq_CUST_SVC_DM_MBR_360_MSG_csv_load = (
    spark.read.format("csv")
    .option("header", False)
    .option("delimiter", ",")
    .option("quote", "^")
    .option("escape", "^")
    .option("nullValue", None)
    .schema(schema_seq_CUST_SVC_DM_MBR_360_MSG_csv_load)
    .load(f"{adls_path}/load/CUST_SVC_DM_MBR_360_MSG.dat")
)

df_cpy_forBuffer = df_seq_CUST_SVC_DM_MBR_360_MSG_csv_load.select(
    col("MBR_360_MSG_NO").alias("MBR_360_MSG_NO"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("EFF_DT").alias("EFF_DT"),
    col("TERM_DT").alias("TERM_DT"),
    col("MSG_TX").alias("MSG_TX"),
    col("USER_ID").alias("USER_ID"),
    col("LAST_UPDT_DT").alias("LAST_UPDT_DT")
)

jdbc_url, jdbc_props = get_db_config(clmmart_secret_name)

df_cpy_forBuffer.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.UwsIdsDmCustSvcDmMbr360MsgLoad_odbc_CUST_SVC_DM_MBR_360_MSG_out_temp") \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO {ClmMartOwner}.CUST_SVC_DM_MBR_360_MSG AS Target
USING STAGING.UwsIdsDmCustSvcDmMbr360MsgLoad_odbc_CUST_SVC_DM_MBR_360_MSG_out_temp AS Source
ON Target.MBR_360_MSG_NO = Source.MBR_360_MSG_NO
AND Target.SRC_SYS_CD = Source.SRC_SYS_CD
WHEN MATCHED THEN UPDATE SET
  Target.EFF_DT = Source.EFF_DT,
  Target.TERM_DT = Source.TERM_DT,
  Target.MSG_TX = Source.MSG_TX,
  Target.USER_ID = Source.USER_ID,
  Target.LAST_UPDT_DT = Source.LAST_UPDT_DT
WHEN NOT MATCHED THEN INSERT
(
  MBR_360_MSG_NO,
  SRC_SYS_CD,
  EFF_DT,
  TERM_DT,
  MSG_TX,
  USER_ID,
  LAST_UPDT_DT
)
VALUES
(
  Source.MBR_360_MSG_NO,
  Source.SRC_SYS_CD,
  Source.EFF_DT,
  Source.TERM_DT,
  Source.MSG_TX,
  Source.USER_ID,
  Source.LAST_UPDT_DT
);
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)

df_odbc_CUST_SVC_DM_MBR_360_MSG_out_rej = spark.createDataFrame(
    [],
    schema=StructType([
        StructField("MBR_360_MSG_NO", IntegerType(), True),
        StructField("SRC_SYS_CD", StringType(), True),
        StructField("EFF_DT", TimestampType(), True),
        StructField("TERM_DT", TimestampType(), True),
        StructField("MSG_TX", StringType(), True),
        StructField("USER_ID", StringType(), True),
        StructField("LAST_UPDT_DT", StringType(), True),
        StructField("ERRORCODE", StringType(), True),
        StructField("ERRORTEXT", StringType(), True)
    ])
)

df_odbc_CUST_SVC_DM_MBR_360_MSG_out_rej = df_odbc_CUST_SVC_DM_MBR_360_MSG_out_rej.withColumn(
    "SRC_SYS_CD", rpad(col("SRC_SYS_CD"), <...>, " ")
).withColumn(
    "MSG_TX", rpad(col("MSG_TX"), <...>, " ")
).withColumn(
    "USER_ID", rpad(col("USER_ID"), <...>, " ")
).withColumn(
    "LAST_UPDT_DT", rpad(col("LAST_UPDT_DT"), 10, " ")
)

df_odbc_CUST_SVC_DM_MBR_360_MSG_out_rej = df_odbc_CUST_SVC_DM_MBR_360_MSG_out_rej.select(
    "MBR_360_MSG_NO",
    "SRC_SYS_CD",
    "EFF_DT",
    "TERM_DT",
    "MSG_TX",
    "USER_ID",
    "LAST_UPDT_DT",
    "ERRORCODE",
    "ERRORTEXT"
)

write_files(
    df_odbc_CUST_SVC_DM_MBR_360_MSG_out_rej,
    f"{adls_path}/load/CUST_SVC_DM_MBR_360_MSG_Rej.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)