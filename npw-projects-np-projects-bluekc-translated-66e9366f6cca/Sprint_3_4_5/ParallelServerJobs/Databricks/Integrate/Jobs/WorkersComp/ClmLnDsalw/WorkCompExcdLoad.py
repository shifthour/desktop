# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2017 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY:    FctsIdsWorkCompClmSeq
# MAGIC 
# MAGIC PROCESSING:     To Load the DB2 table EXCD.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer            Date             Project               Change Description                                           Development Project   Code Reviewer          Date Reviewed       
# MAGIC -------------------------   ------------------   -----------------------   -----------------------------------------------------------------------   ---------------------------------   -------------------------------   -------------------------       
# MAGIC Kailashnath J       2017-03-16\(9) 5628  \(9)           Original Programming\(9)\(9)\(9)     Integratedev2              Hugh Sisson              2017-03-21

# MAGIC Job to load the  DB2 table EXCD.
# MAGIC Read the load file created in the IdsExCdExtr job
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

schema_seq_Excd = StructType([
    StructField("EXCD_SK", IntegerType(), nullable=False),
    StructField("SRC_SYS_CD_SK", IntegerType(), nullable=False),
    StructField("EXCD_ID", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("EXCD_HLTHCARE_ADJ_RSN_CD_SK", IntegerType(), nullable=False),
    StructField("EXCD_LIAB_CD_SK", IntegerType(), nullable=False),
    StructField("EXCD_HIPAA_PROV_ADJ_CD_SK", IntegerType(), nullable=False),
    StructField("EXCD_HIPAA_REMIT_REMARK_CD_SK", IntegerType(), nullable=False),
    StructField("EXCD_STTUS_CD_SK", IntegerType(), nullable=False),
    StructField("EXCD_TYP_CD_SK", IntegerType(), nullable=False),
    StructField("EXCD_LONG_TX1", StringType(), nullable=True),
    StructField("EXCD_LONG_TX2", StringType(), nullable=True),
    StructField("EXCD_SH_TX", StringType(), nullable=True)
])

df_seq_Excd = (
    spark.read.format("csv")
    .option("delimiter", ",")
    .option("quote", "\"")
    .schema(schema_seq_Excd)
    .load(f"{adls_path}/load/WorkComp_EXCD.dat")
)

df_cpy_forBuffer = df_seq_Excd.select(
    col("EXCD_SK"),
    col("SRC_SYS_CD_SK"),
    col("EXCD_ID"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("EXCD_HLTHCARE_ADJ_RSN_CD_SK"),
    col("EXCD_LIAB_CD_SK"),
    col("EXCD_HIPAA_PROV_ADJ_CD_SK"),
    col("EXCD_HIPAA_REMIT_REMARK_CD_SK"),
    col("EXCD_STTUS_CD_SK"),
    col("EXCD_TYP_CD_SK"),
    col("EXCD_LONG_TX1"),
    col("EXCD_LONG_TX2"),
    col("EXCD_SH_TX")
)

df_IDS_EXCD = df_cpy_forBuffer.select(
    col("EXCD_SK").alias("EXCD_SK"),
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    rpad(col("EXCD_ID"), 4, " ").alias("EXCD_ID"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("EXCD_HLTHCARE_ADJ_RSN_CD_SK").alias("EXCD_HLTHCARE_ADJ_RSN_CD_SK"),
    col("EXCD_LIAB_CD_SK").alias("EXCD_LIAB_CD_SK"),
    col("EXCD_HIPAA_PROV_ADJ_CD_SK").alias("EXCD_HIPAA_PROV_ADJ_CD_SK"),
    col("EXCD_HIPAA_REMIT_REMARK_CD_SK").alias("EXCD_HIPAA_REMIT_REMARK_CD_SK"),
    col("EXCD_STTUS_CD_SK").alias("EXCD_STTUS_CD_SK"),
    col("EXCD_TYP_CD_SK").alias("EXCD_TYP_CD_SK"),
    rpad(col("EXCD_LONG_TX1"), <...>, " ").alias("EXCD_LONG_TX1"),
    rpad(col("EXCD_LONG_TX2"), <...>, " ").alias("EXCD_LONG_TX2"),
    rpad(col("EXCD_SH_TX"), <...>, " ").alias("EXCD_SH_TX")
)

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
execute_dml(f"DROP TABLE IF EXISTS STAGING.WorkCompExcdLoad_IDS_EXCD_temp", jdbc_url, jdbc_props)

(
    df_IDS_EXCD.write.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", "STAGING.WorkCompExcdLoad_IDS_EXCD_temp")
    .mode("overwrite")
    .save()
)

merge_sql = f"""
MERGE INTO {IDSOwner}.EXCD AS T
USING STAGING.WorkCompExcdLoad_IDS_EXCD_temp AS S
ON T.EXCD_SK = S.EXCD_SK
WHEN MATCHED THEN
  UPDATE SET
    T.EXCD_SK = S.EXCD_SK,
    T.SRC_SYS_CD_SK = S.SRC_SYS_CD_SK,
    T.EXCD_ID = S.EXCD_ID,
    T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
    T.LAST_UPDT_RUN_CYC_EXCTN_SK = S.LAST_UPDT_RUN_CYC_EXCTN_SK,
    T.EXCD_HLTHCARE_ADJ_RSN_CD_SK = S.EXCD_HLTHCARE_ADJ_RSN_CD_SK,
    T.EXCD_LIAB_CD_SK = S.EXCD_LIAB_CD_SK,
    T.EXCD_HIPAA_PROV_ADJ_CD_SK = S.EXCD_HIPAA_PROV_ADJ_CD_SK,
    T.EXCD_HIPAA_REMIT_REMARK_CD_SK = S.EXCD_HIPAA_REMIT_REMARK_CD_SK,
    T.EXCD_STTUS_CD_SK = S.EXCD_STTUS_CD_SK,
    T.EXCD_TYP_CD_SK = S.EXCD_TYP_CD_SK,
    T.EXCD_LONG_TX1 = S.EXCD_LONG_TX1,
    T.EXCD_LONG_TX2 = S.EXCD_LONG_TX2,
    T.EXCD_SH_TX = S.EXCD_SH_TX
WHEN NOT MATCHED THEN
  INSERT (
    EXCD_SK,
    SRC_SYS_CD_SK,
    EXCD_ID,
    CRT_RUN_CYC_EXCTN_SK,
    LAST_UPDT_RUN_CYC_EXCTN_SK,
    EXCD_HLTHCARE_ADJ_RSN_CD_SK,
    EXCD_LIAB_CD_SK,
    EXCD_HIPAA_PROV_ADJ_CD_SK,
    EXCD_HIPAA_REMIT_REMARK_CD_SK,
    EXCD_STTUS_CD_SK,
    EXCD_TYP_CD_SK,
    EXCD_LONG_TX1,
    EXCD_LONG_TX2,
    EXCD_SH_TX
  )
  VALUES (
    S.EXCD_SK,
    S.SRC_SYS_CD_SK,
    S.EXCD_ID,
    S.CRT_RUN_CYC_EXCTN_SK,
    S.LAST_UPDT_RUN_CYC_EXCTN_SK,
    S.EXCD_HLTHCARE_ADJ_RSN_CD_SK,
    S.EXCD_LIAB_CD_SK,
    S.EXCD_HIPAA_PROV_ADJ_CD_SK,
    S.EXCD_HIPAA_REMIT_REMARK_CD_SK,
    S.EXCD_STTUS_CD_SK,
    S.EXCD_TYP_CD_SK,
    S.EXCD_LONG_TX1,
    S.EXCD_LONG_TX2,
    S.EXCD_SH_TX
  );
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)