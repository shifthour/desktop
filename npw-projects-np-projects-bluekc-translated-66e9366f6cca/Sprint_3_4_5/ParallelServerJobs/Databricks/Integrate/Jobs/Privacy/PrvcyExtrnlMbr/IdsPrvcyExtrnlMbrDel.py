# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC CALLED BY:  FctsPrvcyLoadSeq
# MAGIC             
# MAGIC 
# MAGIC PROCESSING:   Delete IDS Privacy Extrnl Mbr Change Data Capture rows.   
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 	Project/Altiris #	                 Change Description	         Development Project	Code Reviewer	Date Reviewed       
# MAGIC ------------------              --------------------     	------------------------	                 ---------------------------------------------------     --------------------------------	-------------------------------	----------------------------       
# MAGIC Steph Goddard        11/24/09   	3556 Change Data Capture 	 Original Programming                       devlIDSnew                     Brent/Steph            12/29/2009
# MAGIC                                                                                                                                                                                                                   SAndrew                 2010-04-07

# MAGIC File created in driver job FctsExtrnlMbrDrvrExtr
# MAGIC Apply Deletions to IDS Privacy Extrnl Mbr table,
# MAGIC Copy deletes to EDW file path for EDW jobs to process
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


SrcSysCd = get_widget_value('SrcSysCd','FACETS')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWFilePath = get_widget_value('EDWFilePath','')
RunID = get_widget_value('RunID','')

schema_PrvcyExtrnlMbrDelCrf = StructType([
    StructField("SRC_SYS_CD_SK", IntegerType(), nullable=False),
    StructField("PMED_CKE", IntegerType(), nullable=False),
    StructField("OP", StringType(), nullable=False)
])

df_PrvcyExtrnlMbrDelCrf = (
    spark.read.format("csv")
    .option("header", "false")
    .option("sep", ",")
    .option("quote", "\"")
    .schema(schema_PrvcyExtrnlMbrDelCrf)
    .load(f"{adls_path}/load/IdsPrvcyExtrnlMbr.del")
)

df_IdsDelete = df_PrvcyExtrnlMbrDelCrf.select(
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("PMED_CKE").alias("PRVCY_EXTRNL_MBR_UNIQ_KEY")
)

df_EdwDeleteFile = df_PrvcyExtrnlMbrDelCrf.withColumn("SRC_SYS_CD", F.lit(SrcSysCd)).select(
    F.col("SRC_SYS_CD"),
    F.col("PMED_CKE").alias("PMED_CKE")
)

write_files(
    df_EdwDeleteFile,
    f"{adls_path}/load/FctsPrvcyExtrnlMbrDel.EdwPrvcyExtrnlMbrDel.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
execute_dml(f"DROP TABLE IF EXISTS STAGING.IdsPrvcyExtrnlMbrDel_PRVCY_EXTRNL_MBR_temp", jdbc_url, jdbc_props)

df_IdsDelete.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.IdsPrvcyExtrnlMbrDel_PRVCY_EXTRNL_MBR_temp") \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE {IDSOwner}.PRVCY_EXTRNL_MBR AS T
USING STAGING.IdsPrvcyExtrnlMbrDel_PRVCY_EXTRNL_MBR_temp AS S
ON T.SRC_SYS_CD_SK = S.SRC_SYS_CD_SK
AND T.PRVCY_EXTRNL_MBR_UNIQ_KEY = S.PRVCY_EXTRNL_MBR_UNIQ_KEY
WHEN MATCHED THEN
  DELETE;
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)