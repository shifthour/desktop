# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY : IdsWebDmStndExtrMbrHedisMesrYrMoFSeq
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                This Job Loads Data from Sequential File created in Extract Job and loads DataMart Table MED_MGT_DM_MBR_HEDIS_MESR_YTD
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                              Project/Altiris #                                        Change Description                                                            Development Project                                              Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------          ---------------------------------------------------------                                                                                             ----------------------------------   ---------------------------------    -------------------------   
# MAGIC 
# MAGIC Goutham Kalidindi           09/26/2024                    US-628702                                              New Development                                                                     DatamartDev3                                                        Reddy Sanam            09/30/2024

# MAGIC Read Load File created in the IdsEdwMbrHedisMesrYrMoFExtr Job
# MAGIC Copy Stage for buffer
# MAGIC Insert data to table MED_MGT_DM_HEDIS_MESR_GAP_DPLY
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../Utility_DataMart3
# COMMAND ----------


EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
ClmMartOwner = get_widget_value('ClmMartOwner','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')
TableName = get_widget_value('TableName','')

schema_seq_MED_MGT_DM_HEDIS_MESR_GAP_load = StructType([
    StructField("HEDIS_MESR_NM", StringType(), False),
    StructField("HEDIS_SUB_MESR_NM", StringType(), False),
    StructField("HEDIS_MBR_BUCKET_ID", StringType(), False),
    StructField("MOD_ID", StringType(), False),
    StructField("HEDIS_MESR_GAP_DPLY_MOD_EFF_DT", TimestampType(), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("HEDIS_MESR_ABBR_ID", StringType(), False),
    StructField("HEDIS_MESR_GAP_DPLY_MOD_TERMDT", TimestampType(), False),
    StructField("HEDIS_MESR_GAP_MOD_CAT_ID", StringType(), False),
    StructField("HEDIS_MESR_GAP_MOD_DPLY_TX", StringType(), False),
    StructField("HEDIS_MESR_GAP_MOD_PRTY_NO", IntegerType(), False),
    StructField("HEDIS_MESR_GAP_MOD_SCRIPT_TX", StringType(), False),
    StructField("HEDIS_MESR_MOD_KEY_ID", StringType(), False),
    StructField("LAST_UPDT_RUN_CYC_NO", IntegerType(), False),
    StructField("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False)
])

df_seq_MED_MGT_DM_HEDIS_MESR_GAP_load = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", "^")
    .option("quote", None)
    .option("escape", None)
    .option("nullValue", None)
    .schema(schema_seq_MED_MGT_DM_HEDIS_MESR_GAP_load)
    .load(f"{adls_path}/load/MED_MGT_DM_HEDIS_MESR_GAP_DPLY.dat")
)

df_cpy_forBuffer = df_seq_MED_MGT_DM_HEDIS_MESR_GAP_load.select(
    F.col("HEDIS_MESR_NM").alias("HEDIS_MESR_NM"),
    F.col("HEDIS_SUB_MESR_NM").alias("HEDIS_SUB_MESR_NM"),
    F.col("HEDIS_MBR_BUCKET_ID").alias("HEDIS_MBR_BUCKET_ID"),
    F.col("MOD_ID").alias("MOD_ID"),
    F.col("HEDIS_MESR_GAP_DPLY_MOD_EFF_DT").alias("HEDIS_MESR_GAP_DPLY_MOD_EFF_DT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("HEDIS_MESR_ABBR_ID").alias("HEDIS_MESR_ABBR_ID"),
    F.col("HEDIS_MESR_GAP_DPLY_MOD_TERMDT").alias("HEDIS_MESR_GAP_DPLY_MOD_TERMDT"),
    F.col("HEDIS_MESR_GAP_MOD_CAT_ID").alias("HEDIS_MESR_GAP_MOD_CAT_ID"),
    F.col("HEDIS_MESR_GAP_MOD_DPLY_TX").alias("HEDIS_MESR_GAP_MOD_DPLY_TX"),
    F.col("HEDIS_MESR_GAP_MOD_PRTY_NO").alias("HEDIS_MESR_GAP_MOD_PRTY_NO"),
    F.col("HEDIS_MESR_GAP_MOD_SCRIPT_TX").alias("HEDIS_MESR_GAP_MOD_SCRIPT_TX"),
    F.col("HEDIS_MESR_MOD_KEY_ID").alias("HEDIS_MESR_MOD_KEY_ID"),
    F.col("LAST_UPDT_RUN_CYC_NO").alias("LAST_UPDT_RUN_CYC_NO"),
    F.col("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK").alias("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK")
)

df_lnk_IdsEdwMbrHedisMesrYrMo_Out = df_cpy_forBuffer.select(
    F.rpad("HEDIS_MESR_NM", <...>, " ").alias("HEDIS_MESR_NM"),
    F.rpad("HEDIS_SUB_MESR_NM", <...>, " ").alias("HEDIS_SUB_MESR_NM"),
    F.rpad("HEDIS_MBR_BUCKET_ID", <...>, " ").alias("HEDIS_MBR_BUCKET_ID"),
    F.rpad("MOD_ID", <...>, " ").alias("MOD_ID"),
    F.col("HEDIS_MESR_GAP_DPLY_MOD_EFF_DT").alias("HEDIS_MESR_GAP_DPLY_MOD_EFF_DT"),
    F.rpad("SRC_SYS_CD", <...>, " ").alias("SRC_SYS_CD"),
    F.rpad("HEDIS_MESR_ABBR_ID", <...>, " ").alias("HEDIS_MESR_ABBR_ID"),
    F.col("HEDIS_MESR_GAP_DPLY_MOD_TERMDT").alias("HEDIS_MESR_GAP_DPLY_MOD_TERMDT"),
    F.rpad("HEDIS_MESR_GAP_MOD_CAT_ID", <...>, " ").alias("HEDIS_MESR_GAP_MOD_CAT_ID"),
    F.rpad("HEDIS_MESR_GAP_MOD_DPLY_TX", <...>, " ").alias("HEDIS_MESR_GAP_MOD_DPLY_TX"),
    F.col("HEDIS_MESR_GAP_MOD_PRTY_NO").alias("HEDIS_MESR_GAP_MOD_PRTY_NO"),
    F.rpad("HEDIS_MESR_GAP_MOD_SCRIPT_TX", <...>, " ").alias("HEDIS_MESR_GAP_MOD_SCRIPT_TX"),
    F.rpad("HEDIS_MESR_MOD_KEY_ID", <...>, " ").alias("HEDIS_MESR_MOD_KEY_ID"),
    F.col("LAST_UPDT_RUN_CYC_NO").alias("LAST_UPDT_RUN_CYC_NO"),
    F.col("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK").alias("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK")
)

jdbc_url, jdbc_props = get_db_config(clmmart_secret_name)

execute_dml(
    f"DROP TABLE IF EXISTS STAGING.IdsWebDmHedisMesrGapDplyLoad_MED_MGT_DM_HEDIS_MESR_GAP_DPLY_temp",
    jdbc_url,
    jdbc_props
)

(
    df_lnk_IdsEdwMbrHedisMesrYrMo_Out
    .write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", "STAGING.IdsWebDmHedisMesrGapDplyLoad_MED_MGT_DM_HEDIS_MESR_GAP_DPLY_temp")
    .mode("overwrite")
    .save()
)

merge_sql = f"""
MERGE INTO {ClmMartOwner}.{TableName} AS T
USING STAGING.IdsWebDmHedisMesrGapDplyLoad_MED_MGT_DM_HEDIS_MESR_GAP_DPLY_temp AS S
ON
    T.HEDIS_MESR_NM = S.HEDIS_MESR_NM AND
    T.HEDIS_SUB_MESR_NM = S.HEDIS_SUB_MESR_NM AND
    T.HEDIS_MBR_BUCKET_ID = S.HEDIS_MBR_BUCKET_ID AND
    T.MOD_ID = S.MOD_ID AND
    T.HEDIS_MESR_GAP_DPLY_MOD_EFF_DT = S.HEDIS_MESR_GAP_DPLY_MOD_EFF_DT AND
    T.SRC_SYS_CD = S.SRC_SYS_CD
WHEN MATCHED THEN
  UPDATE SET
    T.HEDIS_MESR_ABBR_ID = S.HEDIS_MESR_ABBR_ID,
    T.HEDIS_MESR_GAP_DPLY_MOD_TERMDT = S.HEDIS_MESR_GAP_DPLY_MOD_TERMDT,
    T.HEDIS_MESR_GAP_MOD_CAT_ID = S.HEDIS_MESR_GAP_MOD_CAT_ID,
    T.HEDIS_MESR_GAP_MOD_DPLY_TX = S.HEDIS_MESR_GAP_MOD_DPLY_TX,
    T.HEDIS_MESR_GAP_MOD_PRTY_NO = S.HEDIS_MESR_GAP_MOD_PRTY_NO,
    T.HEDIS_MESR_GAP_MOD_SCRIPT_TX = S.HEDIS_MESR_GAP_MOD_SCRIPT_TX,
    T.HEDIS_MESR_MOD_KEY_ID = S.HEDIS_MESR_MOD_KEY_ID,
    T.LAST_UPDT_RUN_CYC_NO = S.LAST_UPDT_RUN_CYC_NO,
    T.IDS_LAST_UPDT_RUN_CYC_EXCTN_SK = S.IDS_LAST_UPDT_RUN_CYC_EXCTN_SK
WHEN NOT MATCHED THEN
  INSERT (
    HEDIS_MESR_NM,
    HEDIS_SUB_MESR_NM,
    HEDIS_MBR_BUCKET_ID,
    MOD_ID,
    HEDIS_MESR_GAP_DPLY_MOD_EFF_DT,
    SRC_SYS_CD,
    HEDIS_MESR_ABBR_ID,
    HEDIS_MESR_GAP_DPLY_MOD_TERMDT,
    HEDIS_MESR_GAP_MOD_CAT_ID,
    HEDIS_MESR_GAP_MOD_DPLY_TX,
    HEDIS_MESR_GAP_MOD_PRTY_NO,
    HEDIS_MESR_GAP_MOD_SCRIPT_TX,
    HEDIS_MESR_MOD_KEY_ID,
    LAST_UPDT_RUN_CYC_NO,
    IDS_LAST_UPDT_RUN_CYC_EXCTN_SK
  )
  VALUES (
    S.HEDIS_MESR_NM,
    S.HEDIS_SUB_MESR_NM,
    S.HEDIS_MBR_BUCKET_ID,
    S.MOD_ID,
    S.HEDIS_MESR_GAP_DPLY_MOD_EFF_DT,
    S.SRC_SYS_CD,
    S.HEDIS_MESR_ABBR_ID,
    S.HEDIS_MESR_GAP_DPLY_MOD_TERMDT,
    S.HEDIS_MESR_GAP_MOD_CAT_ID,
    S.HEDIS_MESR_GAP_MOD_DPLY_TX,
    S.HEDIS_MESR_GAP_MOD_PRTY_NO,
    S.HEDIS_MESR_GAP_MOD_SCRIPT_TX,
    S.HEDIS_MESR_MOD_KEY_ID,
    S.LAST_UPDT_RUN_CYC_NO,
    S.IDS_LAST_UPDT_RUN_CYC_EXCTN_SK
  );
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)