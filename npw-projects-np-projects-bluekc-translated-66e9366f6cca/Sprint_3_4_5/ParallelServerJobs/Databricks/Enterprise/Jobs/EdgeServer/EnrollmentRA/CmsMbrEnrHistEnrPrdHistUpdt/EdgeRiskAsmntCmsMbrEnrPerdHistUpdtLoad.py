# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2019 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  EdgeRiskAsmntCmsMbrEnrPerdHistUpdtLoad
# MAGIC 
# MAGIC DESCRIPTION:  This job updates Member enrollment Perd  History data to CMS_MBR_ENR_PERD_HIST table 
# MAGIC 
# MAGIC CALLED BY:  EdgeRiskAsmntEnrSbmsnRAExtrSeq
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                   Date                     Project/Ticket #\(9)      Change Description\(9)\(9)Development Project\(9)Code Reviewer\(9)Date Reviewed       
# MAGIC -------------------------          -------------------          --------------------------            -------------------------------------\(9)\(9)---------------------------------\(9)------------------------        -------------------------  
# MAGIC Harsha Ravuri\(9)   2019-01-11\(9)5873 Risk Adjustment      Original Programming\(9)\(9)EnterpriseDev2\(9)\(9)Kalyan Neelam\(9)2019-01-30

# MAGIC Job Name: EdgeRiskAsmntCmsMbrEnrPerdHistUpdtLoad
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
from pyspark.sql.functions import col, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


EDGERiskAsmntOwner = get_widget_value('EDGERiskAsmntOwner','')
edgeriskasmnt_secret_name = get_widget_value('edgeriskasmnt_secret_name','')
RunID = get_widget_value('RunID','')

schema_Seq_CMS_MBR_ENR_PERD_HIST_Extr = StructType([
    StructField("ISSUER_ID", StringType(), True),
    StructField("FILE_ID", StringType(), True),
    StructField("MBR_RCRD_ID", IntegerType(), True),
    StructField("MBR_ENR_PERD_RCRD_ID", IntegerType(), True),
    StructField("PERD_AGE_YR_NO", IntegerType(), True),
    StructField("PERD_AGE_MDL_TYP", StringType(), True),
    StructField("PERD_MBR_MO_CT", DecimalType(38, 10), True),
    StructField("PERD_BILL_MO_CT", DecimalType(38, 10), True)
])

df_Seq_CMS_MBR_ENR_PERD_HIST_Extr = (
    spark.read
    .option("header", False)
    .option("sep", ",")
    .option("quote", "\"")
    .option("nullValue", None)
    .schema(schema_Seq_CMS_MBR_ENR_PERD_HIST_Extr)
    .csv(f"{adls_path}/load/CMS_MBR_ENR_PERD_HIST.dat")
)

df_cp_buffer = df_Seq_CMS_MBR_ENR_PERD_HIST_Extr.select(
    col("ISSUER_ID").alias("ISSUER_ID"),
    col("FILE_ID").alias("FILE_ID"),
    col("MBR_RCRD_ID").alias("MBR_RCRD_ID"),
    col("MBR_ENR_PERD_RCRD_ID").alias("MBR_ENR_PERD_RCRD_ID"),
    col("PERD_AGE_YR_NO").alias("PERD_AGE_YR_NO"),
    col("PERD_AGE_MDL_TYP").alias("PERD_AGE_MDL_TYP"),
    col("PERD_MBR_MO_CT").alias("PERD_MBR_MO_CT"),
    col("PERD_BILL_MO_CT").alias("PERD_BILL_MO_CT")
)

df_Odbc_CMS_MBR_ENR_PERD_HIST_Out = df_cp_buffer.select(
    rpad(col("ISSUER_ID"), 5, " ").alias("ISSUER_ID"),
    rpad(col("FILE_ID"), 12, " ").alias("FILE_ID"),
    col("MBR_RCRD_ID").alias("MBR_RCRD_ID"),
    col("MBR_ENR_PERD_RCRD_ID").alias("MBR_ENR_PERD_RCRD_ID"),
    col("PERD_AGE_YR_NO").alias("PERD_AGE_YR_NO"),
    rpad(col("PERD_AGE_MDL_TYP"), 25, " ").alias("PERD_AGE_MDL_TYP"),
    col("PERD_MBR_MO_CT").alias("PERD_MBR_MO_CT"),
    col("PERD_BILL_MO_CT").alias("PERD_BILL_MO_CT")
)

jdbc_url, jdbc_props = get_db_config(edgeriskasmnt_secret_name)
temp_table_name = "STAGING.EdgeRiskAsmntCmsMbrEnrPerdHistUpdtLoad_Odbc_CMS_MBR_ENR_PERD_HIST_Out_temp"

execute_dml(f"DROP TABLE IF EXISTS {temp_table_name}", jdbc_url, jdbc_props)

df_Odbc_CMS_MBR_ENR_PERD_HIST_Out.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", temp_table_name) \
    .mode("append") \
    .save()

merge_sql = f"""
MERGE INTO {EDGERiskAsmntOwner}.CMS_MBR_ENR_PERD_HIST AS T
USING {temp_table_name} AS S
ON T.ISSUER_ID = S.ISSUER_ID
   AND T.FILE_ID = S.FILE_ID
   AND T.MBR_RCRD_ID = S.MBR_RCRD_ID
   AND T.MBR_ENR_PERD_RCRD_ID = S.MBR_ENR_PERD_RCRD_ID
WHEN MATCHED THEN
  UPDATE SET
    T.ISSUER_ID = S.ISSUER_ID,
    T.FILE_ID = S.FILE_ID,
    T.MBR_RCRD_ID = S.MBR_RCRD_ID,
    T.MBR_ENR_PERD_RCRD_ID = S.MBR_ENR_PERD_RCRD_ID,
    T.PERD_AGE_YR_NO = S.PERD_AGE_YR_NO,
    T.PERD_AGE_MDL_TYP = S.PERD_AGE_MDL_TYP,
    T.PERD_MBR_MO_CT = S.PERD_MBR_MO_CT,
    T.PERD_BILL_MO_CT = S.PERD_BILL_MO_CT
WHEN NOT MATCHED THEN
  INSERT (ISSUER_ID, FILE_ID, MBR_RCRD_ID, MBR_ENR_PERD_RCRD_ID, PERD_AGE_YR_NO, PERD_AGE_MDL_TYP, PERD_MBR_MO_CT, PERD_BILL_MO_CT)
  VALUES (S.ISSUER_ID, S.FILE_ID, S.MBR_RCRD_ID, S.MBR_ENR_PERD_RCRD_ID, S.PERD_AGE_YR_NO, S.PERD_AGE_MDL_TYP, S.PERD_MBR_MO_CT, S.PERD_BILL_MO_CT);
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)