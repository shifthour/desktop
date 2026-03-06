# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2013 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY:    FctsIdsWorkCompClmSeq
# MAGIC 
# MAGIC PROCESSING:     Workers Compensation claims extract from FacetsDbo and load to IDS WORK_COMPNSTN_CLM_LN_DIAG table
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                    Date                 \(9)Project                                                                 Change Description                                                   Development Project\(9)          Code Reviewer          Date Reviewed       
# MAGIC ------------------                --------------------     \(9)---------------------------------------------------------------------      -----------------------------------------------------------------------           ------------------------------\(9)         -------------------------------   ----------------------------       
# MAGIC Akhila Manickavelu\(9)  2017-01-16\(9)5628 WORK_COMPNSTN_CLM_LN_DIAG \(9)    Original Programming\(9)\(9)\(9)      Integratedev2              Kalyan Neelam           2017-02-17        
# MAGIC                                                                 (Facets to IDS) ETL Report

# MAGIC Job to load the Target DB2 table WORK_COMPNSTN_CLM_LN_DIAG
# MAGIC Read the load file created in the FctsIdsWorkCompClmLnDiagExtr job
# MAGIC Load the file created in FctsIdsWorkCompClmLnDiagExtr job into the DB2 table WORK_COMPNSTN_CLM_LN_DIAG
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
WorkCompOwner = get_widget_value('WorkCompOwner','')
workcomp_secret_name = get_widget_value('workcomp_secret_name','')

schema_seq_WorkCompnstnClmLnDiag = StructType([
    StructField("CLM_ID", StringType(), True),
    StructField("CLM_LN_SEQ_NO", IntegerType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("CLM_LN_DIAG_ORDNL_CD", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("DIAG_CD_SK", IntegerType(), True),
    StructField("CLM_LN_DIAG_ORDNL_CD_SK", IntegerType(), True)
])

df_seq_WorkCompnstnClmLnDiag = spark.read.csv(
    path=f"{adls_path}/load/WORK_COMPNSTN_CLM_LN_DIAG.dat",
    schema=schema_seq_WorkCompnstnClmLnDiag,
    sep=",",
    header=False,
    quote=None,
    nullValue=None
)

df_cpy_forBuffer = df_seq_WorkCompnstnClmLnDiag.select(
    col("CLM_ID").alias("CLM_ID"),
    col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("CLM_LN_DIAG_ORDNL_CD").alias("CLM_LN_DIAG_ORDNL_CD"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("DIAG_CD_SK").alias("DIAG_CD_SK"),
    col("CLM_LN_DIAG_ORDNL_CD_SK").alias("CLM_LN_DIAG_ORDNL_CD_SK")
)

df_db2_WorkCompnstnClmLnDiag = (
    df_cpy_forBuffer
    .withColumn("CLM_ID", rpad("CLM_ID", <...>, " "))
    .withColumn("SRC_SYS_CD", rpad("SRC_SYS_CD", <...>, " "))
    .withColumn("CLM_LN_DIAG_ORDNL_CD", rpad("CLM_LN_DIAG_ORDNL_CD", <...>, " "))
    .select(
        "CLM_ID",
        "CLM_LN_SEQ_NO",
        "SRC_SYS_CD",
        "CLM_LN_DIAG_ORDNL_CD",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "DIAG_CD_SK",
        "CLM_LN_DIAG_ORDNL_CD_SK"
    )
)

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

execute_dml("DROP TABLE IF EXISTS STAGING.FctsIdsWorkCompClmLnDiag_db2_WorkCompnstnClmLnDiag_temp", jdbc_url, jdbc_props)

df_db2_WorkCompnstnClmLnDiag.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.FctsIdsWorkCompClmLnDiag_db2_WorkCompnstnClmLnDiag_temp") \
    .mode("overwrite") \
    .save()

merge_sql_db2_WorkCompnstnClmLnDiag = f"""
MERGE {WorkCompOwner}.WORK_COMPNSTN_CLM_LN_DIAG AS T
USING STAGING.FctsIdsWorkCompClmLnDiag_db2_WorkCompnstnClmLnDiag_temp AS S
ON (
    T.CLM_ID = S.CLM_ID
    AND T.CLM_LN_SEQ_NO = S.CLM_LN_SEQ_NO
    AND T.SRC_SYS_CD = S.SRC_SYS_CD
    AND T.CLM_LN_DIAG_ORDNL_CD = S.CLM_LN_DIAG_ORDNL_CD
)
WHEN MATCHED THEN
    UPDATE SET
        T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
        T.LAST_UPDT_RUN_CYC_EXCTN_SK = S.LAST_UPDT_RUN_CYC_EXCTN_SK,
        T.DIAG_CD_SK = S.DIAG_CD_SK,
        T.CLM_LN_DIAG_ORDNL_CD_SK = S.CLM_LN_DIAG_ORDNL_CD_SK
WHEN NOT MATCHED THEN
    INSERT (
        CLM_ID,
        CLM_LN_SEQ_NO,
        SRC_SYS_CD,
        CLM_LN_DIAG_ORDNL_CD,
        CRT_RUN_CYC_EXCTN_SK,
        LAST_UPDT_RUN_CYC_EXCTN_SK,
        DIAG_CD_SK,
        CLM_LN_DIAG_ORDNL_CD_SK
    )
    VALUES (
        S.CLM_ID,
        S.CLM_LN_SEQ_NO,
        S.SRC_SYS_CD,
        S.CLM_LN_DIAG_ORDNL_CD,
        S.CRT_RUN_CYC_EXCTN_SK,
        S.LAST_UPDT_RUN_CYC_EXCTN_SK,
        S.DIAG_CD_SK,
        S.CLM_LN_DIAG_ORDNL_CD_SK
    );
"""

execute_dml(merge_sql_db2_WorkCompnstnClmLnDiag, jdbc_url, jdbc_props)