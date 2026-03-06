# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Shiva Devagiri         07/26/2013        5114                             Load DM Table MBRSH_DM_SUBGRP_LVL_AGNT                         IntegrateWrhsDevl     Peter Marshall               10/22/2013  
# MAGIC 
# MAGIC 
# MAGIC Archana Palivela     7/21/2014            5345                          Changed Load file name as per naming standards in After job 
# MAGIC                                                                                                 Subroutine , Load file and Reject file                                                    IntegrateWrhsDevl     Jag Yelavarthi                2014-08-07

# MAGIC Job Name: IdsDmMbrshDmSubGrpLvlAgtLoad
# MAGIC Read Load File created in the IdsDmMbrshDmSubGrpLvlAgtExtr
# MAGIC Copy Stage for buffer
# MAGIC Update then Insert the MBRSH_DM_SUBGRP_LVL_AGNT Data.
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
from pyspark.sql.functions import col, rpad, lit
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Parameters
ClmMartOwner = get_widget_value('ClmMartOwner','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')
ClmMartArraySize = get_widget_value('ClmMartArraySize','')
ClmMartRecordCount = get_widget_value('ClmMartRecordCount','')

# Read Stage: seq_MBRSH_DM_SUBGRP_LVL_AGNT_Extr
schema_seq_MBRSH_DM_SUBGRP_LVL_AGNT_Extr = StructType([
    StructField("GRP_ID", StringType(), True),
    StructField("SUBGRP_ID", StringType(), True),
    StructField("CLS_PLN_ID", StringType(), True),
    StructField("AGNT_ID", StringType(), True),
    StructField("SUBGRP_LVL_AGNT_ROLE_TYP_CD", StringType(), True),
    StructField("EFF_DT", TimestampType(), True),
    StructField("TERM_DT", TimestampType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_NO", IntegerType(), True),
    StructField("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True)
])

df_seq_MBRSH_DM_SUBGRP_LVL_AGNT_Extr = (
    spark.read
    .option("sep", ",")
    .option("quote", "^")
    .option("header", "false")
    .option("nullValue", None)
    .schema(schema_seq_MBRSH_DM_SUBGRP_LVL_AGNT_Extr)
    .csv(f"{adls_path}/load/MBRSH_DM_SUBGRP_LVL_AGNT.dat")
)

# Copy Stage: cpy_forBuffer
df_cpy_forBuffer = df_seq_MBRSH_DM_SUBGRP_LVL_AGNT_Extr.select(
    col("GRP_ID").alias("GRP_ID"),
    col("SUBGRP_ID").alias("SUBGRP_ID"),
    col("CLS_PLN_ID").alias("CLS_PLN_ID"),
    col("AGNT_ID").alias("AGNT_ID"),
    col("SUBGRP_LVL_AGNT_ROLE_TYP_CD").alias("SUBGRP_LVL_AGNT_ROLE_TYP_CD"),
    col("EFF_DT").alias("EFF_DT"),
    col("TERM_DT").alias("TERM_DT"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("LAST_UPDT_RUN_CYC_NO").alias("LAST_UPDT_RUN_CYC_NO"),
    col("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK").alias("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK")
)

# Prepare for final write (rpad for varchar columns)
df_cpy_forBuffer_rpad = df_cpy_forBuffer.select(
    rpad(col("GRP_ID"), 255, " ").alias("GRP_ID"),
    rpad(col("SUBGRP_ID"), 255, " ").alias("SUBGRP_ID"),
    rpad(col("CLS_PLN_ID"), 255, " ").alias("CLS_PLN_ID"),
    rpad(col("AGNT_ID"), 255, " ").alias("AGNT_ID"),
    rpad(col("SUBGRP_LVL_AGNT_ROLE_TYP_CD"), 255, " ").alias("SUBGRP_LVL_AGNT_ROLE_TYP_CD"),
    col("EFF_DT").alias("EFF_DT"),
    col("TERM_DT").alias("TERM_DT"),
    rpad(col("SRC_SYS_CD"), 255, " ").alias("SRC_SYS_CD"),
    col("LAST_UPDT_RUN_CYC_NO").alias("LAST_UPDT_RUN_CYC_NO"),
    col("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK").alias("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK")
)

# Database Write Stage: ODBC_MBRSH_DM_SUBGRP_LVL_AGNT_out (merge logic)
jdbc_url, jdbc_props = get_db_config(clmmart_secret_name)

execute_dml(
    "DROP TABLE IF EXISTS STAGING.IdsDmMbrshDmSubGrpLvlAgtLoad_ODBC_MBRSH_DM_SUBGRP_LVL_AGNT_out_temp",
    jdbc_url,
    jdbc_props
)

df_cpy_forBuffer_rpad.write.jdbc(
    url=jdbc_url,
    table="STAGING.IdsDmMbrshDmSubGrpLvlAgtLoad_ODBC_MBRSH_DM_SUBGRP_LVL_AGNT_out_temp",
    mode="append",
    properties=jdbc_props
)

merge_sql = f"""
MERGE INTO {ClmMartOwner}.MBRSH_DM_SUBGRP_LVL_AGNT AS T
USING STAGING.IdsDmMbrshDmSubGrpLvlAgtLoad_ODBC_MBRSH_DM_SUBGRP_LVL_AGNT_out_temp AS S
ON
    T.GRP_ID = S.GRP_ID AND
    T.SUBGRP_ID = S.SUBGRP_ID AND
    T.CLS_PLN_ID = S.CLS_PLN_ID AND
    T.AGNT_ID = S.AGNT_ID AND
    T.SUBGRP_LVL_AGNT_ROLE_TYP_CD = S.SUBGRP_LVL_AGNT_ROLE_TYP_CD AND
    T.EFF_DT = S.EFF_DT AND
    T.TERM_DT = S.TERM_DT AND
    T.SRC_SYS_CD = S.SRC_SYS_CD
WHEN MATCHED THEN
    UPDATE SET
        T.LAST_UPDT_RUN_CYC_NO = S.LAST_UPDT_RUN_CYC_NO,
        T.IDS_LAST_UPDT_RUN_CYC_EXCTN_SK = S.IDS_LAST_UPDT_RUN_CYC_EXCTN_SK
WHEN NOT MATCHED THEN
    INSERT (
       GRP_ID,
       SUBGRP_ID,
       CLS_PLN_ID,
       AGNT_ID,
       SUBGRP_LVL_AGNT_ROLE_TYP_CD,
       EFF_DT,
       TERM_DT,
       SRC_SYS_CD,
       LAST_UPDT_RUN_CYC_NO,
       IDS_LAST_UPDT_RUN_CYC_EXCTN_SK
    )
    VALUES (
       S.GRP_ID,
       S.SUBGRP_ID,
       S.CLS_PLN_ID,
       S.AGNT_ID,
       S.SUBGRP_LVL_AGNT_ROLE_TYP_CD,
       S.EFF_DT,
       S.TERM_DT,
       S.SRC_SYS_CD,
       S.LAST_UPDT_RUN_CYC_NO,
       S.IDS_LAST_UPDT_RUN_CYC_EXCTN_SK
    )
;
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)

# Reject Link: seq_MBRSH_DM_SUBGRP_LVL_AGNT_csv_rej
reject_schema = StructType([
    StructField("GRP_ID", StringType(), True),
    StructField("SUBGRP_ID", StringType(), True),
    StructField("CLS_PLN_ID", StringType(), True),
    StructField("AGNT_ID", StringType(), True),
    StructField("SUBGRP_LVL_AGNT_ROLE_TYP_CD", StringType(), True),
    StructField("EFF_DT", TimestampType(), True),
    StructField("TERM_DT", TimestampType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_NO", IntegerType(), True),
    StructField("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("ERRORCODE", StringType(), True),
    StructField("ERRORTEXT", StringType(), True)
])

df_seq_MBRSH_DM_SUBGRP_LVL_AGNT_csv_rej = spark.createDataFrame([], reject_schema)

write_files(
    df_seq_MBRSH_DM_SUBGRP_LVL_AGNT_csv_rej,
    f"{adls_path}/load/MBRSH_DM_SUBGRP_LVL_AGNT_Rej.dat",
    delimiter=",",
    mode="append",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)