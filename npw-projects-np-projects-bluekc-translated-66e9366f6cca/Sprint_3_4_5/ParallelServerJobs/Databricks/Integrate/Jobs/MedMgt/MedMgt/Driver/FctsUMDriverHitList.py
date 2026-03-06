# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2009, 2023 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC Called by FctsUMPrerReqSeq
# MAGIC 
# MAGIC Developer                   Date                User Story #                          Change Description                                                                  Development Project    Code Reviewer           Date Reviewed
# MAGIC ---------------------------   ----------------     -----------------------------------    ------------------------------------------------------------------------------    ------------------------------   ----------------------------   ---------------------
# MAGIC Ralph Tucker             03/19/2009     3808 - BICC                          Initial development                                                                    devlIDS                           Steph Goddard           04/02/2009
# MAGIC Steph Goddard         03/25/2011     TTR-1055                              changed path name for input file                                            IntegrateCurDevl           SAndrew                      2011-04-26
# MAGIC                                                                                                              removed temp table read and hash file; not needed
# MAGIC Raja Gummadi          03/18/2013      TTR-                                      Changed query to 'Insert without clearing' IN O/P stage     IntegrateNewDevl         Kalyan Neelam            2013-03-19
# MAGIC Prabhu ES                 2022-03-07     S2S Remediation                MSSQL ODBC conn params added                                       IntegrateDev5               Manasa Andru            2022-06-14
# MAGIC Hugh Sisson              2023-12-11     ProdSupp                             Added lookup against rows already in the driver table         IntegrateDev2

# MAGIC Load list of UM Reference IDs from the hit list into the TMP_IDS_UM temp table.
# MAGIC Read Facets UM Reference IDs from file
# MAGIC 
# MAGIC /ids/.../update/FctsMedMgtUMHitList.dat
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


DriverTable = get_widget_value('DriverTable','TMP_IDS_UM')
FacetsOwner = get_widget_value('$FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')

# Stage: DriverTable
DriverTable_jdbc_url, DriverTable_jdbc_props = get_db_config(facets_secret_name)
extract_query = "SELECT * FROM tempdb..#DriverTable#"
df_DriverTable = (
    spark.read.format("jdbc")
    .option("url", DriverTable_jdbc_url)
    .options(**DriverTable_jdbc_props)
    .option("query", extract_query)
    .load()
)

# Stage: FctsUmHitList
schema_FctsUmHitList = StructType([
    StructField("UM_REF_ID", StringType(), True)
])
df_FctsUmHitList = (
    spark.read.format("csv")
    .option("header", "false")
    .option("quote", "\"")
    .schema(schema_FctsUmHitList)
    .load(f"{adls_path}/update/FctsMedMgtUMHitList.dat")
)

# Stage: TrnsSK
df_joined = df_FctsUmHitList.alias("UmHitList").join(
    df_DriverTable.alias("fctsTempTable"),
    F.col("UmHitList.UM_REF_ID") == F.col("fctsTempTable.UM_REF_ID"),
    how="left"
)
df_lnkUmRefIdOut = df_joined.filter(
    F.col("fctsTempTable.UM_REF_ID").isNull()
).select(
    F.col("UmHitList.UM_REF_ID").alias("UM_REF_ID")
)

df_lnkUmRefIdOut_final = df_lnkUmRefIdOut.select(
    F.rpad("UM_REF_ID", 9, " ").alias("UM_REF_ID")
)

# Stage: TMP_IDS_UM
execute_dml(
    "DROP TABLE IF EXISTS tempdb.FctsUMDriverHitList_TMP_IDS_UM_temp",
    DriverTable_jdbc_url,
    DriverTable_jdbc_props
)

df_lnkUmRefIdOut_final.write.format("jdbc") \
    .option("url", DriverTable_jdbc_url) \
    .options(**DriverTable_jdbc_props) \
    .option("dbtable", "tempdb.FctsUMDriverHitList_TMP_IDS_UM_temp") \
    .mode("errorifexists") \
    .save()

merge_sql = """
MERGE tempdb..#DriverTable# AS T
USING tempdb.FctsUMDriverHitList_TMP_IDS_UM_temp AS S
ON T.UM_REF_ID = S.UM_REF_ID
WHEN NOT MATCHED THEN
  INSERT (UM_REF_ID)
  VALUES (S.UM_REF_ID);
"""
execute_dml(merge_sql, DriverTable_jdbc_url, DriverTable_jdbc_props)