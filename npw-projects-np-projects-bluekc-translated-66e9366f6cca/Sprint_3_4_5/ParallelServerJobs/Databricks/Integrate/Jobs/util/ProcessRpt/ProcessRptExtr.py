# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
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
# MAGIC Jaideep Mankala     2017-03-13           5321                 Updated Aliase for output column 'DURATION' in                        IntegrateDev2                Jag Yelavarthi             2017-03-13         
# MAGIC                                                                                         IDS stage to match with stage meta data.
# MAGIC                                                                                       Changed Timestamp precision from 3 to 6

# MAGIC Select data from P_RUN_CYC table based on job parameters
# MAGIC Records are appended to file
# MAGIC /ids/prod/scripts/log/processing_rpt.dat
# MAGIC Write out processing statistics to file
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Parameters
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
IDSFilePath = get_widget_value("IDSFilePath","")
RunCycle = get_widget_value("RunCycle","470")
Subject = get_widget_value("Subject","CLAIM")
SourceSys = get_widget_value("SourceSys","FACETS")
TargetSys = get_widget_value("TargetSys","IDS")
RowCount = get_widget_value("RowCount","10000")

# DB2Connector Stage: IDS
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"""SELECT 
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
ORDER BY r.STRT_DTM
"""
df_IDS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# CTransformerStage: Trans1
df_Trans1 = (
    df_IDS
    .withColumn("_Value", F.concat(F.lit("000000"), trim(F.col("DURATION"))))
    .withColumn("_PerPos", F.instr(F.col("_Value"), "."))
    .withColumn("_Sec", F.expr("substring(_Value, _PerPos - 2, 2)"))
    .withColumn("_Min", F.expr("substring(_Value, _PerPos - 4, 2)"))
    .withColumn("_Hrs", F.expr("substring(_Value, _PerPos - 6, 2)"))
    .withColumn("DAY_OF_WK_FULL_NM", F.rpad(F.col("DAY_OF_WK_FULL_NM"), 10, " "))
    .withColumn("TIME_DURATION", F.concat(F.col("_Hrs"), F.lit(":"), F.col("_Min")))
    .withColumn("ROW_COUNT", F.lit(RowCount))
)

df_Trans1_final = df_Trans1.select(
    "SUBJ_CD",
    "SRC_SYS_CD",
    "TRGT_SYS_CD",
    "STRT_DTM",
    "END_DTM",
    "DAY_OF_WK_FULL_NM",
    "TIME_DURATION",
    "ROW_COUNT"
)

# CSeqFileStage: processing_rpt
write_files(
    df_Trans1_final,
    f"{adls_path}/scripts/log/processing_rpt.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)