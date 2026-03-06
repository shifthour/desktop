# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2013 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY:        FctsIdsWorkCompClmSeq
# MAGIC PROCESSING:     Workers Compensation claims extract from FacetsDbo and load to IDS WORK_COMPNSTN_CLM_STTUS_AUDIT table
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                    Date                 \(9)Project                                                                                      Change Description                                                   Development Project\(9)       Code Reviewer             Date Reviewed       
# MAGIC ------------------                --------------------     \(9)------------------------------------------------------------------------------               -----------------------------------------------------------------                  ------------------------------\(9)                       -------------------------------         ----------------------------       
# MAGIC Akhila Manickavelu\(9)  2017-01-10\(9)5628 WORK_COMPNSTN_CLM_STTUS_AUDIT \(9) Original Programming\(9)\(9)\(9)   Integratedev2                               Kalyan Neelam                 2017-02-23
# MAGIC                                                                 (Facets to IDS) ETL Report

# MAGIC Job to load the Target DB2 table WORK_COMPNSTN_CLM_STTUS_AUDIT
# MAGIC Read the load file created in the FctsIdsWorkCompClmSttusAuditExtr job
# MAGIC Load the file created in FctsIdsWorkCompClmSttusAuditExtr job into the DB2 table WORK_COMPNSTN_CLM_STTUS_AUDIT
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
Env = get_widget_value('Env','')
RunID = get_widget_value('RunID','')
WorkCompOwner = get_widget_value('WorkCompOwner','')
workcomp_secret_name = get_widget_value('workcomp_secret_name','')

schema_seq_WORK_COMPNSTN_CLM_STTUS_AUDIT = StructType([
    StructField("CLM_ID", StringType(), nullable=False),
    StructField("CLM_STTUS_AUDIT_SEQ_NO", IntegerType(), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("CRT_BY_APP_USER_SK", IntegerType(), nullable=False),
    StructField("ROUT_TO_APP_USER_SK", IntegerType(), nullable=False),
    StructField("CLM_STTUS_CHG_RSN_CD_SK", IntegerType(), nullable=False),
    StructField("CLM_STTUS_CD_SK", IntegerType(), nullable=False),
    StructField("TRNSMSN_SRC_CD_SK", IntegerType(), nullable=False),
    StructField("CLM_STTUS_DTM", TimestampType(), nullable=False)
])

df_seq_WORK_COMPNSTN_CLM_STTUS_AUDIT = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", ",")
    .schema(schema_seq_WORK_COMPNSTN_CLM_STTUS_AUDIT)
    .load(f"{adls_path}/load/WORK_COMPNSTN_CLM_STTUS_AUDIT.dat")
)

df_cpy_forBuffer = df_seq_WORK_COMPNSTN_CLM_STTUS_AUDIT.select(
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_STTUS_AUDIT_SEQ_NO").alias("CLM_STTUS_AUDIT_SEQ_NO"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CRT_BY_APP_USER_SK").alias("CRT_BY_APP_USER_SK"),
    F.col("ROUT_TO_APP_USER_SK").alias("ROUT_TO_APP_USER_SK"),
    F.col("CLM_STTUS_CHG_RSN_CD_SK").alias("CLM_STTUS_CHG_RSN_CD_SK"),
    F.col("CLM_STTUS_CD_SK").alias("CLM_STTUS_CD_SK"),
    F.col("TRNSMSN_SRC_CD_SK").alias("TRNSMSN_SRC_CD_SK"),
    F.col("CLM_STTUS_DTM").alias("CLM_STTUS_DTM")
)

df_cpy_forBuffer_final = df_cpy_forBuffer.select(
    F.rpad(F.col("CLM_ID"), <...>, " ").alias("CLM_ID"),
    F.col("CLM_STTUS_AUDIT_SEQ_NO").alias("CLM_STTUS_AUDIT_SEQ_NO"),
    F.rpad(F.col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CRT_BY_APP_USER_SK").alias("CRT_BY_APP_USER_SK"),
    F.col("ROUT_TO_APP_USER_SK").alias("ROUT_TO_APP_USER_SK"),
    F.col("CLM_STTUS_CHG_RSN_CD_SK").alias("CLM_STTUS_CHG_RSN_CD_SK"),
    F.col("CLM_STTUS_CD_SK").alias("CLM_STTUS_CD_SK"),
    F.col("TRNSMSN_SRC_CD_SK").alias("TRNSMSN_SRC_CD_SK"),
    F.col("CLM_STTUS_DTM").alias("CLM_STTUS_DTM")
)

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

execute_dml(
    f"DROP TABLE IF EXISTS STAGING.FctsIdsWorkCompClmSttusAuditLoad_db2_WorkCompnstnClmSttusAudit_temp",
    jdbc_url_ids,
    jdbc_props_ids
)

df_cpy_forBuffer_final.write \
    .format("jdbc") \
    .option("url", jdbc_url_ids) \
    .options(**jdbc_props_ids) \
    .option("dbtable", "STAGING.FctsIdsWorkCompClmSttusAuditLoad_db2_WorkCompnstnClmSttusAudit_temp") \
    .mode("overwrite") \
    .save()

merge_sql_db2_WorkCompnstnClmSttusAudit = f"""
MERGE {WorkCompOwner}.WORK_COMPNSTN_CLM_STTUS_AUDIT AS T
USING STAGING.FctsIdsWorkCompClmSttusAuditLoad_db2_WorkCompnstnClmSttusAudit_temp AS S
ON 
    T.CLM_ID = S.CLM_ID
    AND T.CLM_STTUS_AUDIT_SEQ_NO = S.CLM_STTUS_AUDIT_SEQ_NO
    AND T.SRC_SYS_CD = S.SRC_SYS_CD
WHEN MATCHED THEN UPDATE SET
    T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
    T.LAST_UPDT_RUN_CYC_EXCTN_SK = S.LAST_UPDT_RUN_CYC_EXCTN_SK,
    T.CRT_BY_APP_USER_SK = S.CRT_BY_APP_USER_SK,
    T.ROUT_TO_APP_USER_SK = S.ROUT_TO_APP_USER_SK,
    T.CLM_STTUS_CHG_RSN_CD_SK = S.CLM_STTUS_CHG_RSN_CD_SK,
    T.CLM_STTUS_CD_SK = S.CLM_STTUS_CD_SK,
    T.TRNSMSN_SRC_CD_SK = S.TRNSMSN_SRC_CD_SK,
    T.CLM_STTUS_DTM = S.CLM_STTUS_DTM
WHEN NOT MATCHED THEN INSERT (
    CLM_ID,
    CLM_STTUS_AUDIT_SEQ_NO,
    SRC_SYS_CD,
    CRT_RUN_CYC_EXCTN_SK,
    LAST_UPDT_RUN_CYC_EXCTN_SK,
    CRT_BY_APP_USER_SK,
    ROUT_TO_APP_USER_SK,
    CLM_STTUS_CHG_RSN_CD_SK,
    CLM_STTUS_CD_SK,
    TRNSMSN_SRC_CD_SK,
    CLM_STTUS_DTM
)
VALUES (
    S.CLM_ID,
    S.CLM_STTUS_AUDIT_SEQ_NO,
    S.SRC_SYS_CD,
    S.CRT_RUN_CYC_EXCTN_SK,
    S.LAST_UPDT_RUN_CYC_EXCTN_SK,
    S.CRT_BY_APP_USER_SK,
    S.ROUT_TO_APP_USER_SK,
    S.CLM_STTUS_CHG_RSN_CD_SK,
    S.CLM_STTUS_CD_SK,
    S.TRNSMSN_SRC_CD_SK,
    S.CLM_STTUS_DTM
);
"""

execute_dml(merge_sql_db2_WorkCompnstnClmSttusAudit, jdbc_url_ids, jdbc_props_ids)