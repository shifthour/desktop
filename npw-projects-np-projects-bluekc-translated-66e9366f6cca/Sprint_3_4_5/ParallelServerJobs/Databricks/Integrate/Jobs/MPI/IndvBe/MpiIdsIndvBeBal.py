# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2012 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:       MpiIdsIndvBeBalseq01
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 	Project/Altiris #	                 Change Description	         Development Project	Code Reviewer	Date Reviewed       
# MAGIC ------------------              --------------------     	------------------------	                 ---------------------------------------------------     --------------------------------	-------------------------------	----------------------------       
# MAGIC Rick Henry              2012-07-11              4426 - ICW MPI                        Written                                             IntegrateNewDevl          Bhoomi Dasari          09/19/2012

# MAGIC File checked later for rows and email to on-call
# MAGIC Detail file for on-call to research errors
# MAGIC Rows with Failed Comparison on Specified columns
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
RunID = get_widget_value('RunID','')
ToleranceCd = get_widget_value('ToleranceCd','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"""
SELECT
  INDV_BE.INDV_BE_KEY as SRC_INDV_BE_KEY,
  B_INDV_BE.INDV_BE_KEY as TRGT_INDV_BE_KEY
FROM
  {IDSOwner}.INDV_BE INDV_BE
FULL OUTER JOIN
  {IDSOwner}.B_INDV_BE B_INDV_BE
ON
  INDV_BE.INDV_BE_KEY = B_INDV_BE.INDV_BE_KEY
  AND INDV_BE.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
"""

df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_Missing = df_SrcTrgtComp

df_Research = df_Missing.filter(
    (F.col("SRC_INDV_BE_KEY").isNull()) | (F.col("TRGT_INDV_BE_KEY").isNull())
)

df_notify_candidates = df_Missing.select(
    F.lit("ROW COUNT BALANCING MPI BCBS EXTENSION - IDS MPI INDV BE OUT OF TOLERANCE").alias("NOTIFICATION")
).withColumn(
    "_rownum", F.row_number().over(Window.orderBy(F.lit(1)))
)

df_Notify = df_notify_candidates.filter(
    (F.col("_rownum") == 1) & (F.lit(ToleranceCd) == 'OUT')
).drop("_rownum")

df_Notify = df_Notify.withColumn("NOTIFICATION", rpad(F.col("NOTIFICATION"), 70, " "))

df_Research_final = df_Research.select("TRGT_INDV_BE_KEY", "SRC_INDV_BE_KEY")
write_files(
    df_Research_final,
    f"{adls_path}/balancing/research/MpiBcbsExtIdsIndvBeResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_Notify_final = df_Notify.select("NOTIFICATION")
write_files(
    df_Notify_final,
    f"{adls_path}/balancing/notify/IndvBeBalancingNotification.txt",
    delimiter=",",
    mode="append",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)