# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2021 - 2023 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  ScrcrdEmpProdItemBSPreExtr
# MAGIC CALLED BY:  ScorecardEmplProdItemDailyCntl
# MAGIC 
# MAGIC Modifications:                        
# MAGIC 
# MAGIC  Developer                             Date                     Project/Altiris #                             Change Description                                                    Development Project       Code Reviewer            Date Reviewed
# MAGIC -------------------------------------        ---------------------        ---------------------------                         ------------------------------------------\(9)                                    ----------------------------------      -------------------------           -------------------------   
# MAGIC Emran.Mohammad               2021-27-21           us387093                                 Added new ScrcrdEmpProdItemBSPreExtr job                  EnterpriseDev2                Jeyaprasanna              2023-03-01
# MAGIC                                                                                                                                         to pull 4 new source files      
# MAGIC 
# MAGIC Venkata Yama                  2023-10-12          US592924                                Funnel all olib daily files in single file to extract dowstream      EnterpriseDev2\(9)\(9)Ken Bradmon\(9)2023-11-10

# MAGIC This job extracts the new source files and concatinate into single file which is used as source file for BlueSquareExtr4 job
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


OutputFileNm = get_widget_value("OutputFileNm","")
HomeRcvd = get_widget_value("HomeRcvd","")
HomeSent = get_widget_value("HomeSent","")
HostRcvd = get_widget_value("HostRcvd","")
HostSent = get_widget_value("HostSent","")

schema_HomeRcvd = StructType([
    StructField("DateClosed", StringType(), True),
    StructField("B2OwnerID", StringType(), True),
    StructField("MsgType", StringType(), True),
    StructField("Reason", StringType(), True),
    StructField("Perspective", StringType(), True),
    StructField("SCCF", StringType(), True),
    StructField("MsgID", StringType(), True),
    StructField("Req_Resp", StringType(), True),
    StructField("MsgStatus", StringType(), True)
])

df_HomeRcvd = (
    spark.read
    .option("header", True)
    .option("delimiter", ",")
    .schema(schema_HomeRcvd)
    .csv(f"{adls_path_raw}/landing/{HomeRcvd}")
)

schema_HomeSent = StructType([
    StructField("DateClosed", StringType(), True),
    StructField("B2OwnerID", StringType(), True),
    StructField("MsgType", StringType(), True),
    StructField("Reason", StringType(), True),
    StructField("Perspective", StringType(), True),
    StructField("SCCF", StringType(), True),
    StructField("MsgID", StringType(), True),
    StructField("Req_Resp", StringType(), True),
    StructField("MsgStatus", StringType(), True)
])

df_HomeSent = (
    spark.read
    .option("header", True)
    .option("delimiter", ",")
    .option("quote", "\"")
    .schema(schema_HomeSent)
    .csv(f"{adls_path_raw}/landing/{HomeSent}")
)

schema_HostRcvd = StructType([
    StructField("DateClosed", StringType(), True),
    StructField("B2OwnerID", StringType(), True),
    StructField("MsgType", StringType(), True),
    StructField("Reason", StringType(), True),
    StructField("Perspective", StringType(), True),
    StructField("SCCF", StringType(), True),
    StructField("MsgID", StringType(), True),
    StructField("Req_Resp", StringType(), True),
    StructField("MsgStatus", StringType(), True)
])

df_HostRcvd = (
    spark.read
    .option("header", True)
    .option("delimiter", ",")
    .option("quote", "\"")
    .schema(schema_HostRcvd)
    .csv(f"{adls_path_raw}/landing/{HostRcvd}")
)

schema_HostSent = StructType([
    StructField("DateClosed", StringType(), True),
    StructField("B2OwnerID", StringType(), True),
    StructField("MsgType", StringType(), True),
    StructField("Reason", StringType(), True),
    StructField("Perspective", StringType(), True),
    StructField("SCCF", StringType(), True),
    StructField("MsgID", StringType(), True),
    StructField("Req_Resp", StringType(), True),
    StructField("MsgStatus", StringType(), True)
])

df_HostSent = (
    spark.read
    .option("header", True)
    .option("delimiter", ",")
    .option("quote", "\"")
    .schema(schema_HostSent)
    .csv(f"{adls_path_raw}/landing/{HostSent}")
)

df_HomeRcvd = df_HomeRcvd.select(
    "DateClosed",
    "B2OwnerID",
    "MsgType",
    "Reason",
    "Perspective",
    "SCCF",
    "MsgID",
    "Req_Resp",
    "MsgStatus"
)

df_HomeSent = df_HomeSent.select(
    "DateClosed",
    "B2OwnerID",
    "MsgType",
    "Reason",
    "Perspective",
    "SCCF",
    "MsgID",
    "Req_Resp",
    "MsgStatus"
)

df_HostRcvd = df_HostRcvd.select(
    "DateClosed",
    "B2OwnerID",
    "MsgType",
    "Reason",
    "Perspective",
    "SCCF",
    "MsgID",
    "Req_Resp",
    "MsgStatus"
)

df_HostSent = df_HostSent.select(
    "DateClosed",
    "B2OwnerID",
    "MsgType",
    "Reason",
    "Perspective",
    "SCCF",
    "MsgID",
    "Req_Resp",
    "MsgStatus"
)

df_funnel = df_HomeRcvd.union(df_HomeSent).union(df_HostRcvd).union(df_HostSent)

df_final = df_funnel.select(
    "DateClosed",
    "B2OwnerID",
    "MsgType",
    "Reason",
    "Perspective",
    "SCCF",
    "MsgID",
    "Req_Resp",
    "MsgStatus"
)

df_final = df_final.withColumn("DateClosed", rpad("DateClosed", <...>, " "))
df_final = df_final.withColumn("B2OwnerID", rpad("B2OwnerID", <...>, " "))
df_final = df_final.withColumn("MsgType", rpad("MsgType", <...>, " "))
df_final = df_final.withColumn("Reason", rpad("Reason", <...>, " "))
df_final = df_final.withColumn("Perspective", rpad("Perspective", <...>, " "))
df_final = df_final.withColumn("SCCF", rpad("SCCF", <...>, " "))
df_final = df_final.withColumn("MsgID", rpad("MsgID", <...>, " "))
df_final = df_final.withColumn("Req_Resp", rpad("Req_Resp", <...>, " "))
df_final = df_final.withColumn("MsgStatus", rpad("MsgStatus", <...>, " "))

write_files(
    df_final,
    f"{adls_path_raw}/landing/{OutputFileNm}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)