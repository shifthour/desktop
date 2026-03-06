# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2007, 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY: FctsCustSvcPrerecSeq
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:  Load hit list data to driver table.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date               Project/Altiris #               Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------   ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Ralph Tucker                  9/5/2007       15                                   Originally Programmed                          devlIDS30                     Steph Goddard            01/09/2008
# MAGIC Ralph Tucker                  1/15/2008     15                                   Changed Driver table name and           devlIDS                         Steph Goddard            01/17/2008
# MAGIC                                                                                                      added CSTK_LAST_UPD_DTM             
# MAGIC Brent Leland                    02/29/2008   3567 Primary Key           Removed source table update             devlIDScur                     Steph Goddard            05/06/2008
# MAGIC 
# MAGIC Manasa Andru                 2013-10-17     TTR - 443                     Added hf_cust_svc_hit_list_dedupe     IdsNewDevl                 Kalyan Neelam                 2013-10-21
# MAGIC                                                                                                               hashed file
# MAGIC Prabhu ES                       2022-03-01    S2S Remediation           MSSQL connection parameters added IntegrateDev5              Kalyan Neelam                 2022-06-08

# MAGIC /ids/prod/update/FctsCustSvcHitList.dat
# MAGIC Load hit list data into Facets driver table
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


BeginDate = get_widget_value("BeginDate","2007-11-20 00:00:00.000")
FacetsOwner = get_widget_value("FacetsOwner","$PROJDEF")
facets_secret_name = get_widget_value("facets_secret_name","")

schema_Cust_Svc_Hit_List = StructType([
    StructField("CSSC_ID", StringType(), False),
    StructField("CSTK_SEQ_NO", IntegerType(), False),
    StructField("CSTK_LAST_UPD_DTM", TimestampType(), False)
])

df_Cust_Svc_Hit_List = (
    spark.read.format("csv")
    .schema(schema_Cust_Svc_Hit_List)
    .option("header", "false")
    .option("quote", '"')
    .load(f"{adls_path}/update/FctsCustSvcHitList.dat")
)

df_hf_cust_svc_hit_list_dedupe = df_Cust_Svc_Hit_List.dropDuplicates(["CSSC_ID","CSTK_SEQ_NO","CSTK_LAST_UPD_DTM"])

df_Trans1 = (
    df_hf_cust_svc_hit_list_dedupe
    .withColumn("CSSC_ID", trim("CSSC_ID"))
    .withColumn("CSTK_SEQ_NO", F.col("CSTK_SEQ_NO"))
    .withColumn("CSTK_LAST_UPD_DTM", F.col("CSTK_LAST_UPD_DTM"))
)

df_final = df_Trans1.select(
    F.rpad("CSSC_ID", 20, " ").alias("CSSC_ID"),
    "CSTK_SEQ_NO",
    "CSTK_LAST_UPD_DTM"
)

jdbc_url, jdbc_props = get_db_config(<...>)
df_final.write.jdbc(
    url=jdbc_url,
    table="tempdb.FctsCustSvcDrvrHitListLoad_TMP_IDS_CUST_SVC_DRVR_temp",
    mode="overwrite",
    properties=jdbc_props
)

merge_sql = """
MERGE tempdb..TMP_IDS_CUST_SVC_DRVR AS T
USING tempdb..FctsCustSvcDrvrHitListLoad_TMP_IDS_CUST_SVC_DRVR_temp AS S
ON 
  T.CSSC_ID = S.CSSC_ID
  AND T.CSTK_SEQ_NO = S.CSTK_SEQ_NO
  AND T.CSTK_LAST_UPD_DTM = S.CSTK_LAST_UPD_DTM
WHEN MATCHED THEN 
  UPDATE SET 
    T.CSSC_ID = S.CSSC_ID,
    T.CSTK_SEQ_NO = S.CSTK_SEQ_NO,
    T.CSTK_LAST_UPD_DTM = S.CSTK_LAST_UPD_DTM
WHEN NOT MATCHED THEN
  INSERT (
    CSSC_ID,
    CSTK_SEQ_NO,
    CSTK_LAST_UPD_DTM
  )
  VALUES (
    S.CSSC_ID,
    S.CSTK_SEQ_NO,
    S.CSTK_LAST_UPD_DTM
  );
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)