# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2013 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY:    FctsIdsWorkCompClmSeq
# MAGIC 
# MAGIC PROCESSING:     Workers Compensation claims extract from FacetsDbo and load to IDS WORK_COMPNSTN_CLM_DIAG table
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                    Date                 \(9)Project                                                         Change Description                                                   Development Project\(9)          Code Reviewer          Date Reviewed       
# MAGIC ------------------                --------------------     \(9)--------------------------------------------------------------      -----------------------------------------------------------------------           ------------------------------\(9)         -------------------------------   ----------------------------       
# MAGIC Akhila Manickavelu\(9)  2017-01-05\(9)5628 WORK_COMPNSTN_CLM_DIAG \(9)    Original Programming\(9)\(9)\(9)      Integratedev2                              Kalyan Neelam           2017-02-27
# MAGIC                                                                 (Facets to IDS) ETL Report

# MAGIC Job to load the Target DB2 table WORK_COMPNSTN_CLM_DIAG
# MAGIC Read the load file created in the FctsIdsWorkCompDiagExtr job
# MAGIC Load the file created in FctsIdsWorkCompDiagExtr job into the DB2 table WORK_COMPNSTN_CLM_DIAG
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import pyspark.sql.functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
WorkCompOwner = get_widget_value('WorkCompOwner','')

schema_seq_WorkCompnstnClmDiag = StructType([
    StructField("CLM_ID", StringType(), nullable=False),
    StructField("CLM_DIAG_ORDNL_CD", StringType(), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("DIAG_CD_SK", IntegerType(), nullable=False),
    StructField("CLM_DIAG_ORDNL_CD_SK", IntegerType(), nullable=False),
    StructField("CLM_DIAG_POA_CD_SK", IntegerType(), nullable=False)
])

df_seq_WorkCompnstnClmDiag = (
    spark.read.format("csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .option("delimiter", ",")
    .option("quote", None)
    .option("escape", None)
    .option("nullValue", None)
    .schema(schema_seq_WorkCompnstnClmDiag)
    .load(f"{adls_path}/load/WORK_COMPNSTN_CLM_DIAG.dat")
)

df_cpy_forBuffer = df_seq_WorkCompnstnClmDiag.select(
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_DIAG_ORDNL_CD").alias("CLM_DIAG_ORDNL_CD"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("DIAG_CD_SK").alias("DIAG_CD_SK"),
    F.col("CLM_DIAG_ORDNL_CD_SK").alias("CLM_DIAG_ORDNL_CD_SK"),
    F.col("CLM_DIAG_POA_CD_SK").alias("CLM_DIAG_POA_CD_SK")
)

df_db2_WorkCompnstnClmDiag = df_cpy_forBuffer.select(
    "CLM_ID",
    "CLM_DIAG_ORDNL_CD",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "DIAG_CD_SK",
    "CLM_DIAG_ORDNL_CD_SK",
    "CLM_DIAG_POA_CD_SK"
).withColumn(
    "CLM_ID", F.rpad("CLM_ID", <...>, " ")
).withColumn(
    "CLM_DIAG_ORDNL_CD", F.rpad("CLM_DIAG_ORDNL_CD", <...>, " ")
).withColumn(
    "SRC_SYS_CD", F.rpad("SRC_SYS_CD", <...>, " ")
)

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
execute_dml("DROP TABLE IF EXISTS STAGING.FctsIdsWorkCompnClmDiag_db2_WorkCompnstnClmDiag_temp", jdbc_url, jdbc_props)

df_db2_WorkCompnstnClmDiag.write.jdbc(
    url=jdbc_url,
    table="STAGING.FctsIdsWorkCompnClmDiag_db2_WorkCompnstnClmDiag_temp",
    mode="overwrite",
    properties=jdbc_props
)

merge_sql = f"""
MERGE INTO {WorkCompOwner}.WORK_COMPNSTN_CLM_DIAG AS Tgt
USING STAGING.FctsIdsWorkCompnClmDiag_db2_WorkCompnstnClmDiag_temp AS Src
   ON Tgt.CLM_ID = Src.CLM_ID
   AND Tgt.CLM_DIAG_ORDNL_CD = Src.CLM_DIAG_ORDNL_CD
   AND Tgt.SRC_SYS_CD = Src.SRC_SYS_CD
WHEN MATCHED THEN
   UPDATE SET
       Tgt.CRT_RUN_CYC_EXCTN_SK = Src.CRT_RUN_CYC_EXCTN_SK,
       Tgt.LAST_UPDT_RUN_CYC_EXCTN_SK = Src.LAST_UPDT_RUN_CYC_EXCTN_SK,
       Tgt.DIAG_CD_SK = Src.DIAG_CD_SK,
       Tgt.CLM_DIAG_ORDNL_CD_SK = Src.CLM_DIAG_ORDNL_CD_SK,
       Tgt.CLM_DIAG_POA_CD_SK = Src.CLM_DIAG_POA_CD_SK
WHEN NOT MATCHED THEN
   INSERT (CLM_ID, CLM_DIAG_ORDNL_CD, SRC_SYS_CD, CRT_RUN_CYC_EXCTN_SK, LAST_UPDT_RUN_CYC_EXCTN_SK, DIAG_CD_SK, CLM_DIAG_ORDNL_CD_SK, CLM_DIAG_POA_CD_SK)
   VALUES (Src.CLM_ID, Src.CLM_DIAG_ORDNL_CD, Src.SRC_SYS_CD, Src.CRT_RUN_CYC_EXCTN_SK, Src.LAST_UPDT_RUN_CYC_EXCTN_SK, Src.DIAG_CD_SK, Src.CLM_DIAG_ORDNL_CD_SK, Src.CLM_DIAG_POA_CD_SK)
;
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)