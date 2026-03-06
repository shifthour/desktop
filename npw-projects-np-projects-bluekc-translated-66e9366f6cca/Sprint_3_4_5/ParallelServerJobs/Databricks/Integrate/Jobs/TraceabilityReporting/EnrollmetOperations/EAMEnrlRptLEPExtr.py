# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT2020 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC Job Name : EAMEnrlRptLEPExtr
# MAGIC Calling Job: 
# MAGIC                                     
# MAGIC PROCESSING:This Extracts the data from LEP file that business will edit with the desired frequency       
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                                       Date                     Project/Altiris #                       Change Description                   Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                     ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Reddy Sanam                       2021-08-17    414930                      Original Programming                                                IntegrateDev2             Goutham K                   8/25/2021

# MAGIC Job Name: EAMEnrlRptLEPExtr
# MAGIC This Job reads and  processes Excel file for Late Enrollment Penalty that is copied from LAN to ETL Landing Dir.
# MAGIC This Unstructured Stage reads the the tab Newly_Enrolled
# MAGIC This Unstructured Stage reads the the tab BreakInCoverage
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


RunID = get_widget_value('RunID','')
LEPFileName = get_widget_value('LEPFileName','')
CurrDt = get_widget_value('CurrDt','')

schema_BreakInCoverage = StructType([
    StructField("Letter_ID", StringType(), True),
    StructField("MBI", StringType(), True),
    StructField("Column_E", StringType(), True),
    StructField("Column_F", StringType(), True),
    StructField("Member_Name", StringType(), True),
    StructField("Column_H", StringType(), True),
    StructField("Column_I", StringType(), True),
    StructField("Date_Sent", StringType(), True),
    StructField("Column_K", StringType(), True),
    StructField("Column_L", StringType(), True),
    StructField("Column_M", StringType(), True),
    StructField("User_Sent", StringType(), True),
    StructField("Column_O", StringType(), True),
    StructField("Column_P", StringType(), True),
    StructField("Date_73_Submitted", StringType(), True),
    StructField("Column_R", StringType(), True),
    StructField("Column_S", StringType(), True),
    StructField("Column_T", StringType(), True),
    StructField("Date_Letter_Mailed", StringType(), True),
    StructField("Column_V", StringType(), True),
    StructField("Column_W", StringType(), True),
    StructField("Column_X", StringType(), True),
    StructField("FileName", StringType(), True)
])

df_BreakInCoverage_raw = (
    spark.read.csv(
        f"{adls_path_raw}/landing/{LEPFileName}",
        schema=schema_BreakInCoverage,
        sep='|',
        header=False
    )
)

df_BrkInCvg = (
    df_BreakInCoverage_raw
    .filter(
        F.trim(
            F.when(F.col("Letter_ID").isNotNull(), F.col("Letter_ID")).otherwise(F.lit(""))
        ) != ""
    )
    .select(
        F.col("Letter_ID").alias("Letter_ID"),
        F.col("MBI").alias("MBI"),
        F.col("Member_Name").alias("Member_Name"),
        F.col("Date_Sent").alias("Date_Sent"),
        F.col("User_Sent").alias("User_Sent"),
        F.col("Date_73_Submitted").alias("Date_73_Submitted"),
        F.col("Date_Letter_Mailed").alias("Date_Letter_Mailed"),
        F.col("FileName").alias("FileName")
    )
)

schema_NewEnrollments = StructType([
    StructField("Letter_ID", StringType(), True),
    StructField("MBI", StringType(), True),
    StructField("Column_E", StringType(), True),
    StructField("Column_F", StringType(), True),
    StructField("Member_Name", StringType(), True),
    StructField("Column_H", StringType(), True),
    StructField("Column_I", StringType(), True),
    StructField("Date_Sent", StringType(), True),
    StructField("Column_K", StringType(), True),
    StructField("Column_L", StringType(), True),
    StructField("Column_M", StringType(), True),
    StructField("User_Sent", StringType(), True),
    StructField("Column_O", StringType(), True),
    StructField("Column_P", StringType(), True),
    StructField("Date_73_Submitted", StringType(), True),
    StructField("Column_R", StringType(), True),
    StructField("Column_S", StringType(), True),
    StructField("Column_T", StringType(), True),
    StructField("Date_Letter_Mailed", StringType(), True),
    StructField("Column_V", StringType(), True),
    StructField("Column_W", StringType(), True),
    StructField("Column_X", StringType(), True),
    StructField("FileName", StringType(), True)
])

df_NewEnrollments_raw = (
    spark.read.csv(
        f"{adls_path_raw}/landing/{LEPFileName}",
        schema=schema_NewEnrollments,
        sep='|',
        header=False
    )
)

df_NewEnrl = (
    df_NewEnrollments_raw
    .filter(
        F.trim(
            F.when(F.col("Letter_ID").isNotNull(), F.col("Letter_ID")).otherwise(F.lit(""))
        ) != ""
    )
    .select(
        F.col("Letter_ID").alias("Letter_ID"),
        F.col("MBI").alias("MBI"),
        F.col("Member_Name").alias("Member_Name"),
        F.col("Date_Sent").alias("Date_Sent"),
        F.col("User_Sent").alias("User_Sent"),
        F.col("Date_73_Submitted").alias("Date_73_Submitted"),
        F.col("Date_Letter_Mailed").alias("Date_Letter_Mailed"),
        F.col("FileName").alias("FileName")
    )
)

df_Fnl = (
    df_NewEnrl
    .unionByName(df_BrkInCvg)
    .select(
        F.col("Letter_ID"),
        F.col("MBI"),
        F.col("Member_Name"),
        F.col("Date_Sent"),
        F.col("User_Sent"),
        F.col("Date_73_Submitted"),
        F.col("Date_Letter_Mailed"),
        F.col("FileName")
    )
)

df_Xfrm = (
    df_Fnl
    .filter(
        F.trim(
            F.when(F.col("Date_Letter_Mailed").isNotNull(), F.col("Date_Letter_Mailed")).otherwise(F.lit(""))
        ) != ""
    )
    .select(
        F.col("Letter_ID").alias("LTR_ID"),
        F.when(
            F.trim(F.coalesce(F.col("Date_Letter_Mailed"), F.lit(""))) == "",
            F.lit(None)
        ).otherwise(
            F.to_date(F.trim(F.coalesce(F.col("Date_Letter_Mailed"), F.lit(""))), "yyyy-MM-dd")
        ).alias("LTR_MLD_DT"),
        F.col("MBI").alias("MBI"),
        F.col("Member_Name").alias("MBR_NM"),
        F.lit(None).alias("SENT_DT"),
        F.to_date(F.lit(CurrDt), "yyyy-MM-dd").alias("PRCS_RUN_DT")
    )
)

df_final_write = (
    df_Xfrm
    .select(
        F.rpad(F.col("LTR_ID"), <...>, " ").alias("LTR_ID"),
        F.col("LTR_MLD_DT"),
        F.rpad(F.col("MBI"), <...>, " ").alias("MBI"),
        F.rpad(F.col("MBR_NM"), <...>, " ").alias("MBR_NM"),
        F.rpad(F.col("SENT_DT").cast(StringType()), <...>, " ").alias("SENT_DT"),
        F.col("PRCS_RUN_DT")
    )
)

write_files(
    df_final_write,
    f"{adls_path}/load/EAM_ENRL_OPS_LEP_LOAD.{RunID}.dat",
    "|",
    "overwrite",
    False,
    True,
    "\"",
    None
)