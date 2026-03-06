# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2013 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY:         FctsIdsWorkCompClmSeq
# MAGIC PROCESSING:     Workers Compensation claims extract from FacetsDbo and load to IDS WORK_COMPNSTN_GRP table
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                    Date                 \(9)Project                                                                   Change Description                                                   Development Project\(9)       Code Reviewer             Date Reviewed       
# MAGIC ------------------                --------------------     \(9)---------------------------------------------------------------               -----------------------------------------------------------------                  ------------------------------\(9)                       -------------------------------         ----------------------------       
# MAGIC Akhila Manickavelu\(9)  2017-01-05\(9)5628 WORK_COMPNSTN_CLM_PROV \(9)    Original Programming\(9)\(9)\(9)   Integratedev2                               Kalyan Neelam                2017-02-22
# MAGIC                                                                 (Facets to IDS) ETL Report

# MAGIC Job to load the Target DB2 table WORK_COMPNSTN_CLM_PROV
# MAGIC Read the load file created in the FctsIdsWorkCompProvExtr job
# MAGIC Load the file created in FctsIdsWorkCompProvExtr job into the DB2 table WORK_COMPNSTN_CLM_PROV
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
Env = get_widget_value('Env','')
RunID = get_widget_value('RunID','')
WorkCompOwner = get_widget_value('WorkCompOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
workcomp_secret_name = get_widget_value('workcomp_secret_name','')

schema_seq_WorkCompnstnProv = StructType([
    StructField("CLM_ID", StringType(), True),
    StructField("CLM_PROV_ROLE_TYP_CD", StringType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("PROV_SK", IntegerType(), True),
    StructField("CLM_PROV_ROLE_TYP_CD_SK", IntegerType(), True),
    StructField("PROV_ID", StringType(), True),
    StructField("TAX_ID", StringType(), True)
])

df_seq_WorkCompnstnProv = (
    spark.read.format("csv")
    .option("header", False)
    .option("delimiter", ",")
    .schema(schema_seq_WorkCompnstnProv)
    .load(f"{adls_path}/load/WORK_COMPNSTN_CLM_PROV.dat")
)

df_cpy_forBuffer = df_seq_WorkCompnstnProv.select(
    "CLM_ID",
    "CLM_PROV_ROLE_TYP_CD",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "PROV_SK",
    "CLM_PROV_ROLE_TYP_CD_SK",
    "PROV_ID",
    "TAX_ID"
)

df_db2_WorkCompnstnProv = df_cpy_forBuffer.withColumn(
    "CLM_ID", rpad("CLM_ID", <...>, " ")
).withColumn(
    "CLM_PROV_ROLE_TYP_CD", rpad("CLM_PROV_ROLE_TYP_CD", <...>, " ")
).withColumn(
    "SRC_SYS_CD", rpad("SRC_SYS_CD", <...>, " ")
).withColumn(
    "PROV_ID", rpad("PROV_ID", <...>, " ")
).withColumn(
    "TAX_ID", rpad("TAX_ID", <...>, " ")
).select(
    "CLM_ID",
    "CLM_PROV_ROLE_TYP_CD",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "PROV_SK",
    "CLM_PROV_ROLE_TYP_CD_SK",
    "PROV_ID",
    "TAX_ID"
)

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

spark.sql("DROP TABLE IF EXISTS STAGING.FctsIdsWorkCompClmProvLoad_db2_WorkCompnstnProv_temp")

df_db2_WorkCompnstnProv.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.FctsIdsWorkCompClmProvLoad_db2_WorkCompnstnProv_temp") \
    .mode("append") \
    .save()

merge_sql = f"""
MERGE INTO {WorkCompOwner}.WORK_COMPNSTN_CLM_PROV AS T
USING STAGING.FctsIdsWorkCompClmProvLoad_db2_WorkCompnstnProv_temp AS S
ON T.CLM_ID = S.CLM_ID
   AND T.CLM_PROV_ROLE_TYP_CD = S.CLM_PROV_ROLE_TYP_CD
   AND T.SRC_SYS_CD = S.SRC_SYS_CD
WHEN MATCHED THEN UPDATE SET
   T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
   T.LAST_UPDT_RUN_CYC_EXCTN_SK = S.LAST_UPDT_RUN_CYC_EXCTN_SK,
   T.PROV_SK = S.PROV_SK,
   T.CLM_PROV_ROLE_TYP_CD_SK = S.CLM_PROV_ROLE_TYP_CD_SK,
   T.PROV_ID = S.PROV_ID,
   T.TAX_ID = S.TAX_ID
WHEN NOT MATCHED THEN
   INSERT (CLM_ID, CLM_PROV_ROLE_TYP_CD, SRC_SYS_CD, CRT_RUN_CYC_EXCTN_SK, LAST_UPDT_RUN_CYC_EXCTN_SK, PROV_SK, CLM_PROV_ROLE_TYP_CD_SK, PROV_ID, TAX_ID)
   VALUES (S.CLM_ID, S.CLM_PROV_ROLE_TYP_CD, S.SRC_SYS_CD, S.CRT_RUN_CYC_EXCTN_SK, S.LAST_UPDT_RUN_CYC_EXCTN_SK, S.PROV_SK, S.CLM_PROV_ROLE_TYP_CD_SK, S.PROV_ID, S.TAX_ID);
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)