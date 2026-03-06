# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC COPYRIGHT 2010 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:  FctsSubPcaDrvrAllExtr
# MAGIC CALLED BY: FctsSubPcaExtrSeq
# MAGIC 
# MAGIC                            
# MAGIC PROCESSING: Extract all keys from Facets CDC_CMC_SBHS_HSA_ACCUM and load to tempdb driver table.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #               Change Description                             Development Project      Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------     ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Ralph Tucker                  01/29/2010    3556 CDC                        Originally Programmed                            IntegrateCurDevl              
# MAGIC 
# MAGIC Madhavan B                  2017-08-22       5630                                Removed Conv06Extr and                     IntegrateDev1                Kalyan Neelam             2017-08-29
# MAGIC                                                                                                        Conv07Extr Source Links
# MAGIC Prabhu ES                     2022-03-03       S2S Remediation             MSSQL ODBC conn/params added      IntegrateDev5	Ken Bradmon	2022-06-02

# MAGIC Facets Sub Pca Driver Extract for All Records
# MAGIC This link is referenced by multiple sequencers for the row count
# MAGIC Load driver table with CDC keys
# MAGIC (TMP_PROD_CDC_SP_DRVR$TIMESTAMP)
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
CurrDate = get_widget_value('CurrDate','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
TableTimestamp = get_widget_value('TableTimestamp','')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
tempdb_secret_name = get_widget_value('tempdb_secret_name','')

jdbc_url, jdbc_props = get_db_config(facets_secret_name)

extract_query_FACETS = f"""
SELECT DISTINCT
       HSA.SBSB_CK,
       HSA.HSAI_ACC_SFX,
       HSA.SBHS_PLAN_YR_DT,
       HSA.SBHS_EFF_DT
FROM {FacetsOwner}.CMC_SBHS_HSA_ACCUM HSA
LEFT JOIN {FacetsOwner}.CMC_GRGR_GROUP GRP
       ON HSA.GRGR_CK = GRP.GRGR_CK
"""

df_FACETS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_FACETS)
    .load()
)

df_StripFieldsCurr = df_FACETS.select(
    F.col("SBSB_CK").alias("SBSB_CK"),
    UpCase(Convert([chr(10), chr(13), chr(9)], "", trim(F.col("HSAI_ACC_SFX")))).alias("HSAI_ACC_SFX"),
    F.col("SBHS_PLAN_YR_DT").alias("SBHS_PLAN_YR_DT"),
    F.col("SBHS_EFF_DT").alias("SBHS_EFF_DT")
)

df_CurrBusinessLogic = df_StripFieldsCurr.select(
    F.col("SBSB_CK").alias("SUB_UNIQ_KEY"),
    F.col("HSAI_ACC_SFX").alias("SUB_PCA_ACCUM_PFX_ID"),
    F.substring(F.col("SBHS_PLAN_YR_DT"), 1, 10).alias("PLN_YR_BEG_DT"),
    F.when(F.substring(F.col("SBHS_EFF_DT"), 1, 10).isNull(), "1753-01-01")
     .otherwise(F.substring(F.col("SBHS_EFF_DT"), 1, 10))
     .alias("SBHS_EFF_DT")
)

df_Combined1Out = df_CurrBusinessLogic.dropDuplicates(
    ["SUB_UNIQ_KEY","SUB_PCA_ACCUM_PFX_ID","PLN_YR_BEG_DT","SBHS_EFF_DT"]
)

df_DeDup = df_Combined1Out.select(
    F.col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.col("SUB_PCA_ACCUM_PFX_ID").alias("SUB_PCA_ACCUM_PFX_ID"),
    F.col("PLN_YR_BEG_DT").alias("SBHS_PLAN_YR_DT"),
    F.col("SBHS_EFF_DT").alias("SBHS_EFF_DT")
)

df_Extract = df_DeDup.dropDuplicates(
    ["SUB_UNIQ_KEY","SUB_PCA_ACCUM_PFX_ID","SBHS_PLAN_YR_DT","SBHS_EFF_DT"]
)

df_Trans2_intermediate = (
    df_Extract
    .withColumn("svPlanDt", F.to_timestamp(F.col("SBHS_PLAN_YR_DT"), "yyyy-MM-dd"))
    .withColumn("svEffDt", F.to_timestamp(F.col("SBHS_EFF_DT"), "yyyy-MM-dd"))
)

df_Trans2 = df_Trans2_intermediate.select(
    F.col("SUB_UNIQ_KEY").alias("SBSB_CK"),
    F.col("SUB_PCA_ACCUM_PFX_ID").alias("HSAI_ACC_SFX"),
    F.col("svPlanDt").alias("SBHS_PLAN_YR_DT"),
    F.col("svEffDt").alias("SBHS_EFF_DT"),
    F.lit("I").alias("OP")
)

df_TMP_PROD_CDC_SP_DRVR = (
    df_Trans2
    .withColumn("HSAI_ACC_SFX", F.rpad(F.col("HSAI_ACC_SFX"), 4, " "))
    .withColumn("SBHS_PLAN_YR_DT", F.rpad(F.col("SBHS_PLAN_YR_DT"), 10, " "))
    .withColumn("SBHS_EFF_DT", F.rpad(F.col("SBHS_EFF_DT"), 10, " "))
    .withColumn("OP", F.rpad(F.col("OP"), 1, " "))
)

jdbc_url_tempdb, jdbc_props_tempdb = get_db_config(tempdb_secret_name)

execute_dml(
    "DROP TABLE IF EXISTS tempdb.FctsSubPcaDrvrAllExtr_TMP_PROD_CDC_SP_DRVR_temp",
    jdbc_url_tempdb,
    jdbc_props_tempdb
)

df_TMP_PROD_CDC_SP_DRVR.write.format("jdbc").option("url", jdbc_url_tempdb)\
    .option("dbtable", "tempdb.FctsSubPcaDrvrAllExtr_TMP_PROD_CDC_SP_DRVR_temp")\
    .mode("overwrite")\
    .options(**jdbc_props_tempdb)\
    .save()

merge_sql = """
MERGE tempdb.TMP_PROD_CDC_SP_DRVR AS T
USING tempdb.FctsSubPcaDrvrAllExtr_TMP_PROD_CDC_SP_DRVR_temp AS S
ON T.SBSB_CK = S.SBSB_CK
AND T.HSAI_ACC_SFX = S.HSAI_ACC_SFX
AND T.SBHS_PLAN_YR_DT = S.SBHS_PLAN_YR_DT
AND T.SBHS_EFF_DT = S.SBHS_EFF_DT
WHEN MATCHED THEN
  UPDATE SET
    T.OP = S.OP
WHEN NOT MATCHED THEN
  INSERT (SBSB_CK, HSAI_ACC_SFX, SBHS_PLAN_YR_DT, SBHS_EFF_DT, OP)
  VALUES (S.SBSB_CK, S.HSAI_ACC_SFX, S.SBHS_PLAN_YR_DT, S.SBHS_EFF_DT, S.OP);
"""

execute_dml(merge_sql, jdbc_url_tempdb, jdbc_props_tempdb)