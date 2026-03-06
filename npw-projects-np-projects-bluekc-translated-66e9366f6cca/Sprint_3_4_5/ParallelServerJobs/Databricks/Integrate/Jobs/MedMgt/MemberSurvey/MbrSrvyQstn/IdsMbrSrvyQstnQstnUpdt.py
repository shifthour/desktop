# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2012 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  IdsMbrSrvyQstnFltrExtrSeq
# MAGIC                
# MAGIC 
# MAGIC PROCESSING: Updates the RSPN_STORED_IN column in the MBR_SRVY_RSPN table for all the Questions that are needed to be exluded or included from the MBR_SRVY_QSTN_FLTR table
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 \(9)Project/Altiris #\(9)Change Description\(9)\(9)\(9)\(9)Development Project\(9)Code Reviewer\(9)Date Reviewed       
# MAGIC ------------------              --------------------     \(9)------------------------\(9)-----------------------------------------------------------------------\(9)--------------------------------\(9)-------------------------------\(9)----------------------------       
# MAGIC Kalyan Neelam        2012-09-19              4830 & 4735              Initial Programming                                                   IntegrateNewDevl                 Bhoomi Dasari          09/25/2012
# MAGIC Kalyan Neelam        2012-11-12              4830                          Added CurrDate contraints to the SQL                     IntegrateNewDevl                 Bhoomi Dasari          11/12/2012
# MAGIC                                                                                                   in MBR_SRVY_QSTN_FLTR

# MAGIC MBR_SRVY_QTSN Update
# MAGIC Updates the RSPN_STORED_RSPN field on the MBR_SRVY_RSPN table for all the Questions that are needed to be excluded or included
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
SrcSysCd = get_widget_value('SrcSysCd','')
CurrDate = get_widget_value('CurrDate','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query_fltr = f"""
SELECT
  FLTR.SRC_SYS_CD_SK,
  FLTR.MBR_SRVY_TYP_CD,
  FLTR.MBR_SRVY_QSTN_CD_TX,
  FLTR.INCLD_RSPN_IN
FROM {IDSOwner}.MBR_SRVY_QSTN_FLTR FLTR,
     {IDSOwner}.CD_MPPNG CD
WHERE FLTR.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
  AND CD.TRGT_CD = '{SrcSysCd}'
  AND FLTR.EFF_DT_SK <= '{CurrDate}'
  AND FLTR.TERM_DT_SK >= '{CurrDate}'
"""

df_MBR_SRVY_QSTN_FLTR = (
    spark.read.format("jdbc")
        .option("url", jdbc_url)
        .options(**jdbc_props)
        .option("query", extract_query_fltr)
        .load()
)

extract_query_qstn = f"""
SELECT
  QSTN.MBR_SRVY_QSTN_SK,
  QSTN.SRC_SYS_CD_SK,
  QSTN.MBR_SRVY_TYP_CD,
  QSTN.QSTN_CD_TX,
  QSTN.LAST_UPDT_RUN_CYC_EXCTN_SK,
  QSTN.MBR_SRVY_QSTN_RSPN_STORED_IN
FROM {IDSOwner}.MBR_SRVY_QSTN QSTN,
     {IDSOwner}.CD_MPPNG CD
WHERE QSTN.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
  AND CD.TRGT_CD = '{SrcSysCd}'
"""

df_MBR_SRVY_QSTN = (
    spark.read.format("jdbc")
        .option("url", jdbc_url)
        .options(**jdbc_props)
        .option("query", extract_query_qstn)
        .load()
)

df_join = df_MBR_SRVY_QSTN.alias("Qstn").join(
    df_MBR_SRVY_QSTN_FLTR.alias("Fltr"),
    on=[
        col("Qstn.SRC_SYS_CD_SK") == col("Fltr.SRC_SYS_CD_SK"),
        col("Qstn.MBR_SRVY_TYP_CD") == col("Fltr.MBR_SRVY_TYP_CD"),
        col("Qstn.QSTN_CD_TX") == col("Fltr.MBR_SRVY_QSTN_CD_TX")
    ],
    how="left"
)

df_updt = df_join.filter(col("Fltr.INCLD_RSPN_IN").isNotNull())

df_updt = df_updt.select(
    col("Qstn.MBR_SRVY_QSTN_SK").alias("MBR_SRVY_QSTN_SK"),
    lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("Fltr.INCLD_RSPN_IN").alias("MBR_SRVY_QSTN_RSPN_STORED_IN")
)

df_updt = df_updt.withColumn(
    "MBR_SRVY_QSTN_RSPN_STORED_IN",
    rpad(col("MBR_SRVY_QSTN_RSPN_STORED_IN"), 1, " ")
)

temp_table_name = "STAGING.IdsMbrSrvyQstnQstnUpdt_MBR_SRVY_QSTN_updt_temp"
drop_sql = f"DROP TABLE IF EXISTS {temp_table_name}"
execute_dml(drop_sql, jdbc_url, jdbc_props)

df_updt.write.jdbc(jdbc_url, temp_table_name, mode="overwrite", properties=jdbc_props)

merge_sql = f"""
MERGE INTO {IDSOwner}.MBR_SRVY_QSTN AS T
USING {temp_table_name} AS S
ON T.MBR_SRVY_QSTN_SK = S.MBR_SRVY_QSTN_SK
WHEN MATCHED THEN
  UPDATE SET
    T.LAST_UPDT_RUN_CYC_EXCTN_SK = S.LAST_UPDT_RUN_CYC_EXCTN_SK,
    T.MBR_SRVY_QSTN_RSPN_STORED_IN = S.MBR_SRVY_QSTN_RSPN_STORED_IN
WHEN NOT MATCHED THEN
  INSERT (
    MBR_SRVY_QSTN_SK,
    LAST_UPDT_RUN_CYC_EXCTN_SK,
    MBR_SRVY_QSTN_RSPN_STORED_IN
  )
  VALUES (
    S.MBR_SRVY_QSTN_SK,
    S.LAST_UPDT_RUN_CYC_EXCTN_SK,
    S.MBR_SRVY_QSTN_RSPN_STORED_IN
  );
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)