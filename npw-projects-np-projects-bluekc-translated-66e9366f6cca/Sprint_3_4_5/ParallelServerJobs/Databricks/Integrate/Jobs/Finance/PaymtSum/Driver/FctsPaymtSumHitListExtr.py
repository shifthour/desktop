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
# MAGIC 
# MAGIC Â© Copyright 2008 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  FctsPaymtSumExtrSeq
# MAGIC                
# MAGIC 
# MAGIC PROCESSING:   Read records from /ids/prod/update/FctsPaymtSumHitList.dat and load them to the Facets driver table.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Brent Leland            2008-02-15       Primary Key           Original Programming.                                                                   devlIDScur                     Steph Goddard           02/22/2008
# MAGIC Prabhu ES               2022-02-24       S2S Remediation  MSSQL connection parameters added                                         IntegrateDev5\(9)Harsha Ravuri\(9)06-10-2022

# MAGIC /ids/prod/update/FctsPaymtSumHitList.dat
# MAGIC Load Payment Summary hit list records to Facets driver table
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


TableTimeStamp = get_widget_value('TableTimeStamp','')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
tempdb_secret_name = get_widget_value('tempdb_secret_name','')

schema_FctsPaymtSumHitList = StructType([
    StructField("CKPY_REF_ID", StringType(), False)
])

df_FctsPaymtSumHitList = (
    spark.read.format("csv")
    .option("sep", ",")
    .option("quote", "\"")
    .option("header", "false")
    .schema(schema_FctsPaymtSumHitList)
    .load(f"{adls_path}/update/FctsPaymtSumHitList.dat")
)

df_Trans01 = df_FctsPaymtSumHitList.select("CKPY_REF_ID").withColumn("CKPY_PAY_DT", lit("2008-01-01 00:00:00.000"))

df_Pmt_Sum_Hit_List = df_Trans01.select(
    rpad(col("CKPY_REF_ID"), <...>, " ").alias("CKPY_REF_ID"),
    col("CKPY_PAY_DT")
)

jdbc_url, jdbc_props = get_db_config(tempdb_secret_name)
execute_dml("DROP TABLE IF EXISTS tempdb.FctsPaymtSumHitListExtr_Pmt_Sum_Hit_List_temp", jdbc_url, jdbc_props)

df_Pmt_Sum_Hit_List.write.format("jdbc").options(
    url=jdbc_url,
    **jdbc_props,
    dbtable="tempdb.FctsPaymtSumHitListExtr_Pmt_Sum_Hit_List_temp"
).mode("overwrite").save()

merge_sql = """
MERGE tempdb.TMP_IDS_PMTSUM AS T
USING tempdb.FctsPaymtSumHitListExtr_Pmt_Sum_Hit_List_temp AS S
ON T.CKPY_REF_ID = S.CKPY_REF_ID
WHEN MATCHED THEN UPDATE SET T.CKPY_PAY_DT = S.CKPY_PAY_DT
WHEN NOT MATCHED THEN INSERT (CKPY_REF_ID, CKPY_PAY_DT) VALUES (S.CKPY_REF_ID, S.CKPY_PAY_DT);
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)