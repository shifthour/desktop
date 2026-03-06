# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 07/24/08 10:56:42 Batch  14816_39411 PROMOTE bckcetl ids20 dsadm rc for brent 
# MAGIC ^1_1 07/24/08 10:41:03 Batch  14816_38477 INIT bckcett testIDS dsadm rc fro brent 
# MAGIC ^1_3 07/22/08 08:32:54 Batch  14814_30778 PROMOTE bckcett testIDS u08717 Brent
# MAGIC ^1_3 07/22/08 08:21:54 Batch  14814_30118 INIT bckcett devlIDS u08717 Brent
# MAGIC ^1_2 07/07/08 11:47:11 Batch  14799_42473 INIT bckcett devlIDS u11141 has
# MAGIC ^1_1 06/23/08 16:01:25 Batch  14785_57704 PROMOTE bckcett devlIDS u10913 O. Nielsen move from devlIDScur to devlIDS for B. Leland
# MAGIC ^1_1 06/23/08 15:24:04 Batch  14785_55472 INIT bckcett devlIDScur u10913 O. Nielsen move from devlIDSCUR to devlIDS for B. Leland
# MAGIC ^1_1 01/28/08 10:06:06 Batch  14638_36371 PROMOTE bckcett devlIDScur dsadm dsadm
# MAGIC ^1_1 01/28/08 09:39:22 Batch  14638_34767 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 12/27/07 09:46:56 Batch  14606_35219 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/24/07 09:35:50 Batch  14573_34558 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 08/27/07 08:55:46 Batch  14484_32151 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_3 03/27/07 13:27:29 Batch  14331_48452 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 03/07/07 16:30:43 Batch  14311_59451 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 01/10/07 13:53:12 Batch  14255_50002 PROMOTE bckcetl ids20 dsadm Keith for Ralph
# MAGIC ^1_1 01/10/07 13:47:36 Batch  14255_49665 INIT bckcett testIDS30 dsadm Keith for Ralph
# MAGIC ^1_2 01/04/07 09:49:52 Batch  14249_35398 PROMOTE bckcett testIDS30 u06640 Ralph
# MAGIC ^1_2 01/04/07 09:47:31 Batch  14249_35260 INIT bckcett devlIDS30 u06640 Ralph
# MAGIC ^1_1 12/22/06 14:39:13 Batch  14236_52758 INIT bckcett devlIDS30 u06640 Ralph
# MAGIC ^1_2 07/14/06 13:05:39 Batch  14075_47144 INIT bckcett devlIDS30 u06640 Ralph
# MAGIC ^1_1 07/14/06 13:04:47 Batch  14075_47095 INIT bckcett devlIDS30 u06640 Ralph
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC Â© Copyright 2006, 2007, 2008 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  FctsPaymtSumExtrSeq
# MAGIC                
# MAGIC 
# MAGIC PROCESSING:   Load changed Facets keys into temp. driver table for extracting.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Ralph Tucker           05/01/2006                                   Original Programming - 
# MAGIC Brent Leland            2008-02-15       3567 Primary Key   Modified SQL to only pull changed data                                      devlIDScur                      Steph Goddard          02/22/2008
# MAGIC Prabhu ES               2022-02-24       S2S Remediation  MSSQL connection parameters added                                         IntegrateDev5\(9)Harsha Ravuri\(9)06-10-2022

# MAGIC Load driver table with changed keys
# MAGIC Link count used in FctsPaymtSumExtrSeq
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, when, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


BeginDate = get_widget_value('BeginDate','2008-02-13')
TableTimeStamp = get_widget_value('TableTimeStamp','9999')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')

jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)

extract_query = f"SELECT CKPY_REF_ID, CKPY_PAY_DT FROM {FacetsOwner}.CMC_CKPY_PAYEE_SUM WHERE CKPY_PAY_DT >= Left('{BeginDate}', 23)"
df_CMC_CKPY_PAYEE_SUM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query)
    .load()
)

df_Trans01 = (
    df_CMC_CKPY_PAYEE_SUM
    .withColumn("CKPY_REF_ID", trim(when(col("CKPY_REF_ID").isNull(), lit("")).otherwise(col("CKPY_REF_ID"))))
    .withColumn("CKPY_PAY_DT", trim(col("CKPY_PAY_DT")))
)

df_TMP_IDS_PMTSUM = df_Trans01.select(
    rpad(col("CKPY_REF_ID"), <...>, " ").alias("CKPY_REF_ID"),
    rpad(col("CKPY_PAY_DT"), <...>, " ").alias("CKPY_PAY_DT")
)

jdbc_url_TMP_IDS_PMTSUM, jdbc_props_TMP_IDS_PMTSUM = get_db_config(<...>)

execute_dml("DROP TABLE IF EXISTS tempdb.FctsPaymtSumTempLoad_TMP_IDS_PMTSUM_temp", jdbc_url_TMP_IDS_PMTSUM, jdbc_props_TMP_IDS_PMTSUM)

(
    df_TMP_IDS_PMTSUM.write.format("jdbc")
    .option("url", jdbc_url_TMP_IDS_PMTSUM)
    .options(**jdbc_props_TMP_IDS_PMTSUM)
    .option("dbtable", "tempdb.FctsPaymtSumTempLoad_TMP_IDS_PMTSUM_temp")
    .mode("overwrite")
    .save()
)

merge_sql = """
MERGE tempdb.TMP_IDS_PMTSUM AS T
USING tempdb.FctsPaymtSumTempLoad_TMP_IDS_PMTSUM_temp AS S
ON T.CKPY_REF_ID = S.CKPY_REF_ID
WHEN MATCHED THEN
  UPDATE SET
    T.CKPY_REF_ID = S.CKPY_REF_ID,
    T.CKPY_PAY_DT = S.CKPY_PAY_DT
WHEN NOT MATCHED THEN
  INSERT (CKPY_REF_ID, CKPY_PAY_DT)
  VALUES (S.CKPY_REF_ID, S.CKPY_PAY_DT);
"""

execute_dml(merge_sql, jdbc_url_TMP_IDS_PMTSUM, jdbc_props_TMP_IDS_PMTSUM)

AfterJobRoutine = 1