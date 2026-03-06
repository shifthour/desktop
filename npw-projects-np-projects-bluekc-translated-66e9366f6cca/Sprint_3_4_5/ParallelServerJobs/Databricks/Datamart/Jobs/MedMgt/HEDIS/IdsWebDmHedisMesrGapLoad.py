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
# MAGIC CALLED BY : IdsWebDmHedisMesrGapSeq
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                This Job Loads Data from Sequential File created in Extract Job and loads DataMart Table MED_MGT_DM_HEDIS_MESR_GAP
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                              Project/Altiris #                                        Change Description                                                            Development Project                                              Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------          ---------------------------------------------------------                                                                                             ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Goutham Kalidindi           9/16/2024                    US-628702                                            New Development                                                                      DatamartDev3                                                    Reddy Sanam                09/30/2024

# MAGIC Read Load File created in the IdsEdwHedisMesrGapExtr Job
# MAGIC Copy Stage for buffer
# MAGIC Insert data to table MED_MGT_DM_MBR_HEDIS_MESR_GAP
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
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("HEDIS_MESR_ABBR_ID", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_NO", IntegerType(), False),
    StructField("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False)
])

df_seq_MED_MGT_DM_HEDIS_MESR_GAP_load = (
    spark.read
    .option("delimiter", "^")
    .option("quote", None)
    .option("header", "false")
    .option("nullValue", None)
    .schema(schema_seq_MED_MGT_DM_HEDIS_MESR_GAP_load)
    .csv(f"{adls_path}/load/MED_MGT_DM_HEDIS_MESR_GAP.dat")
)

df_cpy_forBuffer = df_seq_MED_MGT_DM_HEDIS_MESR_GAP_load.select(
    col("HEDIS_MESR_NM").alias("HEDIS_MESR_NM"),
    col("HEDIS_SUB_MESR_NM").alias("HEDIS_SUB_MESR_NM"),
    col("HEDIS_MBR_BUCKET_ID").alias("HEDIS_MBR_BUCKET_ID"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("HEDIS_MESR_ABBR_ID").alias("HEDIS_MESR_ABBR_ID"),
    col("LAST_UPDT_RUN_CYC_NO").alias("LAST_UPDT_RUN_CYC_NO"),
    col("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK").alias("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK")
)

df_final = df_cpy_forBuffer.select(
    rpad(col("HEDIS_MESR_NM"), <...>, " ").alias("HEDIS_MESR_NM"),
    rpad(col("HEDIS_SUB_MESR_NM"), <...>, " ").alias("HEDIS_SUB_MESR_NM"),
    rpad(col("HEDIS_MBR_BUCKET_ID"), <...>, " ").alias("HEDIS_MBR_BUCKET_ID"),
    rpad(col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    rpad(col("HEDIS_MESR_ABBR_ID"), <...>, " ").alias("HEDIS_MESR_ABBR_ID"),
    col("LAST_UPDT_RUN_CYC_NO"),
    col("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK")
)

jdbc_url, jdbc_props = get_db_config(clmmart_secret_name)

drop_table_sql = """
DROP TABLE IF EXISTS STAGING.IdsWebDmHedisMesrGapLoad_MED_MGT_DM_MBR_HEDIS_MESR_GAP_temp
"""
execute_dml(drop_table_sql, jdbc_url, jdbc_props)

(
    df_final.write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", "STAGING.IdsWebDmHedisMesrGapLoad_MED_MGT_DM_MBR_HEDIS_MESR_GAP_temp")
    .mode("overwrite")
    .save()
)

merge_sql = f"""
MERGE INTO {ClmMartOwner}.{TableName} AS T
USING STAGING.IdsWebDmHedisMesrGapLoad_MED_MGT_DM_MBR_HEDIS_MESR_GAP_temp AS S
ON
    T.HEDIS_MESR_NM = S.HEDIS_MESR_NM
    AND T.HEDIS_SUB_MESR_NM = S.HEDIS_SUB_MESR_NM
    AND T.HEDIS_MBR_BUCKET_ID = S.HEDIS_MBR_BUCKET_ID
    AND T.SRC_SYS_CD = S.SRC_SYS_CD
WHEN MATCHED THEN
  UPDATE SET T.HEDIS_MESR_ABBR_ID = S.HEDIS_MESR_ABBR_ID,
             T.LAST_UPDT_RUN_CYC_NO = S.LAST_UPDT_RUN_CYC_NO,
             T.IDS_LAST_UPDT_RUN_CYC_EXCTN_SK = S.IDS_LAST_UPDT_RUN_CYC_EXCTN_SK
WHEN NOT MATCHED THEN
  INSERT (
    HEDIS_MESR_NM,
    HEDIS_SUB_MESR_NM,
    HEDIS_MBR_BUCKET_ID,
    SRC_SYS_CD,
    HEDIS_MESR_ABBR_ID,
    LAST_UPDT_RUN_CYC_NO,
    IDS_LAST_UPDT_RUN_CYC_EXCTN_SK
  )
  VALUES (
    S.HEDIS_MESR_NM,
    S.HEDIS_SUB_MESR_NM,
    S.HEDIS_MBR_BUCKET_ID,
    S.SRC_SYS_CD,
    S.HEDIS_MESR_ABBR_ID,
    S.LAST_UPDT_RUN_CYC_NO,
    S.IDS_LAST_UPDT_RUN_CYC_EXCTN_SK
  );
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)