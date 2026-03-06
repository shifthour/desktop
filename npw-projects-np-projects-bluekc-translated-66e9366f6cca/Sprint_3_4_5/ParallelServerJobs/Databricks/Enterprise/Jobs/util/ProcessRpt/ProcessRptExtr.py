# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 05/06/09 15:38:02 Batch  15102_56307 PROMOTE bckcetl edw10 dsadm rc for brent 
# MAGIC ^1_1 05/06/09 15:33:33 Batch  15102_56026 INIT bckcett:31540 testEDW dsadm rc for brent 
# MAGIC ^1_1 07/24/08 10:56:42 Batch  14816_39411 PROMOTE bckcetl ids20 dsadm rc for brent 
# MAGIC ^1_1 07/24/08 10:41:03 Batch  14816_38477 INIT bckcett testIDS dsadm rc fro brent 
# MAGIC ^1_2 07/22/08 08:32:54 Batch  14814_30778 PROMOTE bckcett testIDS u08717 Brent
# MAGIC ^1_2 07/22/08 08:21:54 Batch  14814_30118 INIT bckcett devlIDS u08717 Brent
# MAGIC ^1_1 07/07/08 11:47:11 Batch  14799_42473 INIT bckcett devlIDS u11141 has
# MAGIC 
# MAGIC Â© Copyright 2008 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  Various job controls
# MAGIC                
# MAGIC 
# MAGIC PROCESSING:   Select row from P_RUN_CYC based on input source, target, subject, and run cycle.  Caculate run time durartion and append information to file /ids/prod/landing/ids_processing.dat.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Brent Leland            2008-02-14       IAD Prod. Supp     Original Programming.                                                                   devlIDScur                     Steph Goddard          02/22/2008
# MAGIC 
# MAGIC Shanmugam A.        2017-03-08           5321                 Updated Aliase for output column 'DURATION' in                        EnterpriseDev2               Jag Yelavarthi             2017-03-08
# MAGIC                                                                                         IDS stage to match with stage meta data

# MAGIC Select data from P_RUN_CYC table based on job parameters
# MAGIC Records are appended to file
# MAGIC /ids/prod/scripts/log/processing_rpt.dat
# MAGIC Copy of this job exists in other project
# MAGIC Write out processing statistics to file
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, concat, locate, substring, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


IDSFilePath = get_widget_value('IDSFilePath','')
RunCycle = get_widget_value('RunCycle','470')
Subject = get_widget_value('Subject','CLAIM')
SourceSys = get_widget_value('SourceSys','FACETS')
TargetSys = get_widget_value('TargetSys','IDS')
RowCount = get_widget_value('RowCount','10000')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"""
SELECT 
r.SUBJ_CD,
r.SRC_SYS_CD,
r.TRGT_SYS_CD,
r.STRT_DTM,
r.END_DTM,
c.DAY_OF_WK_FULL_NM,
(r.END_DTM - r.STRT_DTM) AS DURATION
FROM {IDSOwner}.P_RUN_CYC r,
     {IDSOwner}.P_BTCH_CYC b,
     {IDSOwner}.CLNDR_DT c
WHERE r.SUBJ_CD = '{Subject}'
  AND r.TRGT_SYS_CD = '{TargetSys}'
  AND r.SRC_SYS_CD = '{SourceSys}'
  AND r.BTCH_CYC_SK = b.BTCH_CYC_SK
  AND b.BTCH_CYC_DESC = c.CLNDR_DT_SK
  AND r.RUN_CYC_NO = {RunCycle}
order by r.STRT_DTM
"""

df_IDS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_Trans1 = (
    df_IDS
    .withColumn("Value", concat(lit("000000"), trim(col("DURATION"))))
    .withColumn("PerPos", locate('.', col("Value")))
    .withColumn("Sec", substring(col("Value"), col("PerPos") - 2, 2))
    .withColumn("Min", substring(col("Value"), col("PerPos") - 4, 2))
    .withColumn("Hrs", substring(col("Value"), col("PerPos") - 6, 2))
)

df_Trans1_out = df_Trans1.select(
    col("SUBJ_CD"),
    col("SRC_SYS_CD"),
    col("TRGT_SYS_CD"),
    col("STRT_DTM"),
    col("END_DTM"),
    rpad(col("DAY_OF_WK_FULL_NM"), 10, " ").alias("DAY_OF_WK_FULL_NM"),
    concat(col("Hrs"), lit(":"), col("Min")).alias("TIME_DURATION"),
    lit(RowCount).alias("ROW_COUNT")
)

write_files(
    df_Trans1_out,
    f"{adls_path}/scripts/log/processing_rpt.dat",
    ",",
    "append",
    False,
    False,
    "\"",
    None
)