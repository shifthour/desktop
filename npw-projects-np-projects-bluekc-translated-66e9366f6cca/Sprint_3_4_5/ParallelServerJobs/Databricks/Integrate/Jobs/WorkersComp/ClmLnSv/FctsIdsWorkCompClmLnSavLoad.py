# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2013 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY:    FctsIdsWorkCompClmSeq
# MAGIC 
# MAGIC PROCESSING:     Workers Compensation claims extract from FacetsDbo and load to IDS WORK_COMPNSTN_CLM table
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                    Date                 \(9)Project                                                          Change Description                                                   Development Project\(9)          Code Reviewer          Date Reviewed       
# MAGIC ------------------                --------------------     \(9)------------------------                                        -----------------------------------------------------------------------                  ------------------------------\(9)         -------------------------------   ----------------------------       
# MAGIC Akhila Manickavelu\(9)  2016-12-01\(9)5628 WORK_COMPNSTN_CLM \(9)    Original Programming\(9)\(9)\(9)          Integratedev2                          Kalyan Neelam           2017-03-15
# MAGIC                                                                 (Facets to IDS) ETL Report

# MAGIC Job to load the Target DB2 table WORK_COMPNSTN_CLM_LN_SAV
# MAGIC Read the load file created in the FctsIdsWorkCompClmLnSavExtr job
# MAGIC Load the file created into the DB2 table WORK_COMPNSTN_CLM_LN_SAV
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


WorkCompOwner = get_widget_value('WorkCompOwner', '')
workcomp_secret_name = get_widget_value('workcomp_secret_name', '')

schema_seq_WorkCompnstnClm = StructType([
    StructField("CLM_ID", StringType(), False),
    StructField("CLM_LN_SEQ_NO", IntegerType(), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("CLM_LN_SAV_TYP_CD", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("CLM_LN_SAV_TYP_CD_SK", IntegerType(), False),
    StructField("SAV_AMT", DecimalType(38, 10), False)
])

df_seq_WorkCompnstnClm = (
    spark.read.format("csv")
    .schema(schema_seq_WorkCompnstnClm)
    .option("sep", ",")
    .option("header", "false")
    .option("quote", "\"")
    .option("escape", "\"")
    .option("multiline", "false")
    .option("nullValue", None)
    .load(f"{adls_path}/load/WORK_COMPNSTN_CLM_LN_SAV.dat")
)

df_cpy_forBuffer = df_seq_WorkCompnstnClm.select(
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CLM_LN_SAV_TYP_CD").alias("CLM_LN_SAV_TYP_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_LN_SAV_TYP_CD_SK").alias("CLM_LN_SAV_TYP_CD_SK"),
    F.col("SAV_AMT").alias("SAV_AMT")
)

df_db2_WorkCompnstnClmLnSav = (
    df_cpy_forBuffer
    .withColumn("CLM_ID", F.rpad(F.col("CLM_ID"), <...>, " "))
    .withColumn("SRC_SYS_CD", F.rpad(F.col("SRC_SYS_CD"), <...>, " "))
    .withColumn("CLM_LN_SAV_TYP_CD", F.rpad(F.col("CLM_LN_SAV_TYP_CD"), <...>, " "))
    .select(
        "CLM_ID",
        "CLM_LN_SEQ_NO",
        "SRC_SYS_CD",
        "CLM_LN_SAV_TYP_CD",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CLM_LN_SAV_TYP_CD_SK",
        "SAV_AMT"
    )
)

jdbc_url, jdbc_props = get_db_config(workcomp_secret_name)

execute_dml(
    f"DROP TABLE IF EXISTS STAGING.FctsIdsWorkCompClmLnSav_db2_WorkCompnstnClmLnSav_temp",
    jdbc_url,
    jdbc_props
)

df_db2_WorkCompnstnClmLnSav.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.FctsIdsWorkCompClmLnSav_db2_WorkCompnstnClmLnSav_temp") \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO {WorkCompOwner}.WORK_COMPNSTN_CLM_LN_SAV AS T
USING STAGING.FctsIdsWorkCompClmLnSav_db2_WorkCompnstnClmLnSav_temp AS S
ON 
  T.CLM_ID = S.CLM_ID AND
  T.CLM_LN_SEQ_NO = S.CLM_LN_SEQ_NO AND
  T.SRC_SYS_CD = S.SRC_SYS_CD AND
  T.CLM_LN_SAV_TYP_CD = S.CLM_LN_SAV_TYP_CD
WHEN MATCHED THEN 
  UPDATE SET 
    T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
    T.LAST_UPDT_RUN_CYC_EXCTN_SK = S.LAST_UPDT_RUN_CYC_EXCTN_SK,
    T.CLM_LN_SAV_TYP_CD_SK = S.CLM_LN_SAV_TYP_CD_SK,
    T.SAV_AMT = S.SAV_AMT
WHEN NOT MATCHED THEN 
  INSERT (
    CLM_ID,
    CLM_LN_SEQ_NO,
    SRC_SYS_CD,
    CLM_LN_SAV_TYP_CD,
    CRT_RUN_CYC_EXCTN_SK,
    LAST_UPDT_RUN_CYC_EXCTN_SK,
    CLM_LN_SAV_TYP_CD_SK,
    SAV_AMT
  )
  VALUES 
  (
    S.CLM_ID,
    S.CLM_LN_SEQ_NO,
    S.SRC_SYS_CD,
    S.CLM_LN_SAV_TYP_CD,
    S.CRT_RUN_CYC_EXCTN_SK,
    S.LAST_UPDT_RUN_CYC_EXCTN_SK,
    S.CLM_LN_SAV_TYP_CD_SK,
    S.SAV_AMT
  );
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)