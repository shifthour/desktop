# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Calling Job:EAMFctsReconRptCntl
# MAGIC 
# MAGIC Description:This Job Loads the Resolved Load file to EAM DISCREPANCY_ANALYSIS table. This Job only deletes existing rows
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC ======================================================================================================================================
# MAGIC Developer                          Date                           Project/Altiris #                              Change Description                                                    Development Project               
# MAGIC ======================================================================================================================================
# MAGIC ReddySanam               2021-01-14                  US329820                                         Original Programming                                                    IntegrateDev2      Kalyan Neelam    2021-01-14
# MAGIC ReddySanam               2021-01-27                  US329820                                         Added  Billing and MedPrdt files                                    IntegrateDev2  Kalyan Neelam    2021-01-27
# MAGIC ReddySanam               2021-02-01                  US329820                                         Added  MidMonth  file                                                   IntegrateDev2   Jeyaprasanna    2021-02-02
# MAGIC ReddySanam               2021-02-03                  US329820                                         Added  BLMM  file                                                        IntegrateDev2    Jeyaprasanna    2021-02-03
# MAGIC ReddySanam               2021-02-03                  US329820                                         Added  Dntl Pln  file                                                      IntegrateDev2    Jeyaprasanna    2021-02-08
# MAGIC ReddySanam               2021-02-16                  US329820                                         Added  RXIDTC72 file                                                  IntegrateDev2    Jeyaprasanna    2021-02-17
# MAGIC ReddySanam               2021-02-22                  US329820                                         Added  AgentID file                                                      IntegrateDev2    Jeyaprasanna     2021-02-23
# MAGIC                                                                                                                                   Added  LICS   file
# MAGIC JohnAbraham               2021-08-11                  US391328                  Added EAMRPT Env variables and updated appropriate tables         IntegrateDev1
# MAGIC                                                                                                                with these parameters
# MAGIC 
# MAGIC Arpitha V                     2024-03-20                    US 611997                          AddedESRDSTRTDT_File,LISCOPAYLEVELCAT_File,          IntegrateDev2     Jeyaprasanna     2024-06-24
# MAGIC                                                                                                                                  LISENDDATE_File
# MAGIC 
# MAGIC Arpitha V                     2024-07-03             US 611748,611749,        Added ESRDENDDT_File,KDTSPSTRTDT_File,                               IntegrateDev2     Jeyaprasanna      2024-08-02
# MAGIC                                                                             612321                               KDTSPSTRTDT_File, EFFDT_File

# MAGIC This Job Deletes the Resolved records from DISCREPANCY_ANALYSIS table
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, IntegerType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../Utility_Integrate
# COMMAND ----------


EAMRPTOwner = get_widget_value('EAMRPTOwner','')
eamrpt_secret_name = get_widget_value('eamrpt_secret_name','')
RUNID = get_widget_value('RUNID','')

schema_single_col_id_sk = StructType([StructField("ID_SK", IntegerType(), False)])

df_Resolved = (
    spark.read
    .schema(schema_single_col_id_sk)
    .option("header", True)
    .option("sep", "|")
    .option("quote", "\"")
    .option("nullValue", None)
    .csv(f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_RESOLVED.{RUNID}.dat")
)

df_LBILLResolved = (
    spark.read
    .schema(schema_single_col_id_sk)
    .option("header", True)
    .option("sep", "|")
    .option("quote", "\"")
    .option("nullValue", None)
    .csv(f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_LBILL_RESOLVED.{RUNID}.dat")
)

df_MEDPLN_Resolved = (
    spark.read
    .schema(schema_single_col_id_sk)
    .option("header", True)
    .option("sep", "|")
    .option("quote", "\"")
    .option("nullValue", None)
    .csv(f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_MEDLPLN_RESOLVED.{RUNID}.dat")
)

df_MidMonth_Resolved = (
    spark.read
    .schema(schema_single_col_id_sk)
    .option("header", True)
    .option("sep", "|")
    .option("quote", "\"")
    .option("nullValue", None)
    .csv(f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_MIDM_RESOLVED.{RUNID}.dat")
)

df_BLMM_Resolved = (
    spark.read
    .schema(schema_single_col_id_sk)
    .option("header", True)
    .option("sep", "|")
    .option("quote", "\"")
    .option("nullValue", None)
    .csv(f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_BLMM_RESOLVED.{RUNID}.dat")
)

df_DntlPLN_Resolved = (
    spark.read
    .schema(schema_single_col_id_sk)
    .option("header", True)
    .option("sep", "|")
    .option("quote", "\"")
    .option("nullValue", None)
    .csv(f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_DNTLPLN_RESOLVED.{RUNID}.dat")
)

df_RXIDTC72_Resolved = (
    spark.read
    .schema(schema_single_col_id_sk)
    .option("header", True)
    .option("sep", "|")
    .option("quote", "\"")
    .option("nullValue", None)
    .csv(f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_RXIDTC72_RESOLVED.{RUNID}.dat")
)

df_AGNT_Resolved = (
    spark.read
    .schema(schema_single_col_id_sk)
    .option("header", True)
    .option("sep", "|")
    .option("quote", "\"")
    .option("nullValue", None)
    .csv(f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_AGNT_RESOLVED.{RUNID}.dat")
)

df_LICS_Resolved = (
    spark.read
    .schema(schema_single_col_id_sk)
    .option("header", True)
    .option("sep", "|")
    .option("quote", "\"")
    .option("nullValue", None)
    .csv(f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_LICS_RESOLVED.{RUNID}.dat")
)

df_KDTSPENDDT_Resolved = (
    spark.read
    .schema(schema_single_col_id_sk)
    .option("header", True)
    .option("sep", "|")
    .option("quote", "\"")
    .option("nullValue", None)
    .csv(f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_KDTSPENDDT_RESOLVED.{RUNID}.dat")
)

df_KDTSPSTRTDT_Resolved = (
    spark.read
    .schema(schema_single_col_id_sk)
    .option("header", True)
    .option("sep", "|")
    .option("quote", "\"")
    .option("nullValue", None)
    .csv(f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_KDTSPSTRTDT_RESOLVED.{RUNID}.dat")
)

df_ESRDENDDT_Resolved = (
    spark.read
    .schema(schema_single_col_id_sk)
    .option("header", True)
    .option("sep", "|")
    .option("quote", "\"")
    .option("nullValue", None)
    .csv(f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_ESRDENDDT_RESOLVED.{RUNID}.dat")
)

df_ESRDSTRTDT_Resolved = (
    spark.read
    .schema(schema_single_col_id_sk)
    .option("header", True)
    .option("sep", "|")
    .option("quote", "\"")
    .option("nullValue", None)
    .csv(f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_ESRDSTRTDT_RESOLVED.{RUNID}.dat")
)

df_EFFDT_Resolved = (
    spark.read
    .schema(schema_single_col_id_sk)
    .option("header", True)
    .option("sep", "|")
    .option("quote", "\"")
    .option("nullValue", None)
    .csv(f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_EFFDT_RESOLVED.{RUNID}.dat")
)

df_LISCOPAYLEVELCAT_Resolved = (
    spark.read
    .schema(schema_single_col_id_sk)
    .option("header", True)
    .option("sep", "|")
    .option("quote", "\"")
    .option("nullValue", None)
    .csv(f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_LISCOPAYLEVELCAT_RESOLVED.{RUNID}.dat")
)

df_LISENDDATE_Resolved = (
    spark.read
    .schema(schema_single_col_id_sk)
    .option("header", True)
    .option("sep", "|")
    .option("quote", "\"")
    .option("nullValue", None)
    .csv(f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_LISENDDATE_RESOLVED.{RUNID}.dat")
)

dfFnl = (
    df_MEDPLN_Resolved.select(F.col("ID_SK").alias("ID"))
    .union(df_LBILLResolved.select(F.col("ID_SK").alias("ID")))
    .union(df_Resolved.select(F.col("ID_SK").alias("ID")))
    .union(df_MidMonth_Resolved.select(F.col("ID_SK").alias("ID")))
    .union(df_BLMM_Resolved.select(F.col("ID_SK").alias("ID")))
    .union(df_DntlPLN_Resolved.select(F.col("ID_SK").alias("ID")))
    .union(df_RXIDTC72_Resolved.select(F.col("ID_SK").alias("ID")))
    .union(df_AGNT_Resolved.select(F.col("ID_SK").alias("ID")))
    .union(df_LICS_Resolved.select(F.col("ID_SK").alias("ID")))
    .union(df_ESRDSTRTDT_Resolved.select(F.col("ID_SK").alias("ID")))
    .union(df_EFFDT_Resolved.select(F.col("ID_SK").alias("ID")))
    .union(df_ESRDENDDT_Resolved.select(F.col("ID_SK").alias("ID")))
    .union(df_KDTSPSTRTDT_Resolved.select(F.col("ID_SK").alias("ID")))
    .union(df_LISCOPAYLEVELCAT_Resolved.select(F.col("ID_SK").alias("ID")))
    .union(df_KDTSPENDDT_Resolved.select(F.col("ID_SK").alias("ID")))
    .union(df_LISENDDATE_Resolved.select(F.col("ID_SK").alias("ID")))
)

df_Update = dfFnl.select("ID")

jdbc_url, jdbc_props = get_db_config(eamrpt_secret_name)

execute_dml(
    "DROP TABLE IF EXISTS STAGING.FacetsEAMDiscAnalysisReslLoad_DISC_ANALYSIS_temp",
    jdbc_url,
    jdbc_props
)

df_Update.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.FacetsEAMDiscAnalysisReslLoad_DISC_ANALYSIS_temp") \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO {EAMRPTOwner}.DISCREPANCY_ANALYSIS AS T
USING STAGING.FacetsEAMDiscAnalysisReslLoad_DISC_ANALYSIS_temp AS S
ON (T.ID = S.ID)
WHEN MATCHED THEN UPDATE SET T.ID = S.ID
WHEN NOT MATCHED THEN INSERT (ID) VALUES (S.ID);
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)