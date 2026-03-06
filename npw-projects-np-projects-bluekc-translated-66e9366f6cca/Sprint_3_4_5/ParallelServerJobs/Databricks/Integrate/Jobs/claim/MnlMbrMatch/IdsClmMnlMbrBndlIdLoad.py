# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Called By: MedtrakDrugClmCntl and SavRXDrugClmCntl jobs.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                        DATASTAGE         CODE                         DATE
# MAGIC DEVELOPER          DATE                PROJECT                 DESCRIPTION                                                                                    ENVIRONMENT    REVIEWER                REVIEW
# MAGIC -----------------------------   ----------------------   ------------------------------    ---------------------------------------------------------------------------------------------------------   ---------------------------    ------------------------------     --------------------
# MAGIC Kaushik Kapoor       2018 -08-06      5828                          Original Programming                                                                           IntegrateDev2        Hugh Sisson               2018-08-13

# MAGIC Copy Stage for buffer
# MAGIC Job to update BUNDLE_ID in P_CLM_MBRSH_ERR_RECYC table
# MAGIC Load file created in Extract job and data will be updated in P_CLM_MBRSH_ERR_RECYC table
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

schema_seqf_P_CLM_MBRSH_ERR_RECYC = StructType([
    StructField("CLM_SK", IntegerType(), nullable=False),
    StructField("CLM_ID", StringType(), nullable=False),
    StructField("CLM_TYP_CD", StringType(), nullable=False),
    StructField("CLM_SUBTYP_CD", StringType(), nullable=False),
    StructField("CLM_SVC_STRT_DT_SK", StringType(), nullable=False),
    StructField("SRC_SYS_GRP_PFX", StringType(), nullable=False),
    StructField("SRC_SYS_GRP_ID", StringType(), nullable=False),
    StructField("SRC_SYS_GRP_SFX", StringType(), nullable=False),
    StructField("ERR_CD", StringType(), nullable=False),
    StructField("ERR_DESC", StringType(), nullable=False),
    StructField("FEP_MBR_ID", StringType(), nullable=False),
    StructField("SRC_SYS_SUB_ID", StringType(), nullable=True),
    StructField("SRC_SYS_MBR_SFX_NO", StringType(), nullable=True),
    StructField("FILE_DT_SK", StringType(), nullable=True),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("SUB_SSN", StringType(), nullable=False),
    StructField("PATN_LAST_NM", StringType(), nullable=False),
    StructField("PATN_FIRST_NM", StringType(), nullable=False),
    StructField("PATN_GNDR_CD", StringType(), nullable=False),
    StructField("PATN_BRTH_DT_SK", StringType(), nullable=False),
    StructField("SUB_FIRST_NM", StringType(), nullable=True),
    StructField("SUB_LAST_NM", StringType(), nullable=True),
    StructField("GRP_ID", StringType(), nullable=True),
    StructField("PATN_SSN", StringType(), nullable=True),
    StructField("MBR_MNL_MATCH_BUNDLE_ID", IntegerType(), nullable=True)
])

df_seqf_P_CLM_MBRSH_ERR_RECYC = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", "|")
    .option("quote", None)
    .option("escape", None)
    .option("nullValue", None)
    .schema(schema_seqf_P_CLM_MBRSH_ERR_RECYC)
    .load(f"{adls_path}/load/P_CLM_MBRSH_ERR_RECYC_BNDL.dat")
)

df_cpy_forBuffer = df_seqf_P_CLM_MBRSH_ERR_RECYC.select(
    F.col("CLM_SK").alias("CLM_SK"),
    F.col("MBR_MNL_MATCH_BUNDLE_ID").alias("MBR_MNL_MATCH_BUNDLE_ID")
)

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

temp_table_name = "STAGING.IdsClmMnlMbrBndlIdLoad_DB2_P_CLM_MBRSH_ERR_RECYC_temp"

execute_dml(f"DROP TABLE IF EXISTS {temp_table_name}", jdbc_url, jdbc_props)

(
    df_cpy_forBuffer
    .write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", temp_table_name)
    .mode("overwrite")
    .save()
)

merge_sql = f"""
MERGE INTO {IDSOwner}.P_CLM_MBRSH_ERR_RECYC as T
USING {temp_table_name} as S
ON T.CLM_SK = S.CLM_SK
WHEN MATCHED THEN
  UPDATE SET T.MBR_MNL_MATCH_BUNDLE_ID = S.MBR_MNL_MATCH_BUNDLE_ID
WHEN NOT MATCHED THEN
  INSERT (CLM_SK, MBR_MNL_MATCH_BUNDLE_ID)
  VALUES (S.CLM_SK, S.MBR_MNL_MATCH_BUNDLE_ID);
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)