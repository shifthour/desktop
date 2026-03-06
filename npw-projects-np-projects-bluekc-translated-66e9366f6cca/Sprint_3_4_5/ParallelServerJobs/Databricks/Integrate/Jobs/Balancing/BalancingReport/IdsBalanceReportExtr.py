# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2011 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:  Extracts Balancing Information from ROW_CT_DTL and writes it to a file which is used to send email.
# MAGIC 
# MAGIC Called by :  IdsBalReportCntl
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                       Date               Project/Altiris #               Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------     -------------------   -----------------------------------    ---------------------------------------------------------   ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Raja Gummadi              2012-02-28      TTR-1184                         Original Programming                           IntegrateCurDevl         Brent Leland                 03-08-2012

# MAGIC Check against last run date
# MAGIC Tables in Balance
# MAGIC Tables Out of Balance
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------

UWSOwner = get_widget_value('UWSOwner','')
uws_secret_name = get_widget_value('uws_secret_name','')
CurrDate = get_widget_value('CurrDate','')

jdbc_url, jdbc_props = get_db_config(uws_secret_name)
extract_query = f"SELECT ROW_CT_DTL.TRGT_SYS_CD, ROW_CT_DTL.SRC_SYS_CD, ROW_CT_DTL.SUBJ_AREA_NM, ROW_CT_DTL.TRGT_TBL_NM, ROW_CT_DTL.DIFF_CT, ROW_CT_DTL.TLRNC_CD, ROW_CT_DTL.CRT_DTM, ROW_CT_DTL.TRGT_CT FROM {UWSOwner}.ROW_CT_DTL ROW_CT_DTL WHERE ROW_CT_DTL.CRT_DTM > '{CurrDate}' ORDER BY ROW_CT_DTL.SUBJ_AREA_NM"
df_ROW_CT_DTL = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_Bal = df_ROW_CT_DTL.filter(col("TLRNC_CD") == "BAL").select(
    col("SUBJ_AREA_NM").alias("SUBJECT"),
    col("SRC_SYS_CD").alias("SOURCE"),
    col("TRGT_SYS_CD").alias("TARGET"),
    col("TRGT_TBL_NM").alias("TABLE"),
    col("TRGT_CT").alias("ROW_CT")
)
df_Bal = df_Bal.withColumn("SUBJECT", rpad(col("SUBJECT"), 35, " ")) \
               .withColumn("SOURCE", rpad(col("SOURCE"), 20, " ")) \
               .withColumn("TARGET", rpad(col("TARGET"), 20, " ")) \
               .withColumn("TABLE", rpad(col("TABLE"), 35, " "))
df_BalReport = df_Bal.select("SUBJECT", "SOURCE", "TARGET", "TABLE", "ROW_CT")
write_files(
    df_BalReport,
    f"{adls_path_publish}/external/InBalance.txt",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)

df_Out = df_ROW_CT_DTL.filter(col("TLRNC_CD") == "OUT").select(
    col("SUBJ_AREA_NM").alias("SUBJECT"),
    col("SRC_SYS_CD").alias("SOURCE"),
    col("TRGT_SYS_CD").alias("TARGET"),
    col("TRGT_TBL_NM").alias("TABLE"),
    col("DIFF_CT").alias("DIFF")
)
df_Out = df_Out.withColumn("SUBJECT", rpad(col("SUBJECT"), 35, " ")) \
               .withColumn("SOURCE", rpad(col("SOURCE"), 20, " ")) \
               .withColumn("TARGET", rpad(col("TARGET"), 20, " ")) \
               .withColumn("TABLE", rpad(col("TABLE"), 35, " "))
df_BalReportOut = df_Out.select("SUBJECT", "SOURCE", "TARGET", "TABLE", "DIFF")
write_files(
    df_BalReportOut,
    f"{adls_path_publish}/external/OutOfBalance.txt",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)