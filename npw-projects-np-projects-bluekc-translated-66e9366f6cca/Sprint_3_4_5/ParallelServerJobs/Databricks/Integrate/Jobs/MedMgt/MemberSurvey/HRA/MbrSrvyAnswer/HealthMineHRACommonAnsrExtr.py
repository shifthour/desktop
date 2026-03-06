# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2011 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  HlthMineMbrSrvyQstnAnswrExtrSeq
# MAGIC                
# MAGIC 
# MAGIC PROCESSING: Common Extract job for the MBR_SRVY_ANSWER table in to Common File Format. The job processes landing files generated from the HRA.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC =====================================================================================================================================================
# MAGIC Developer\(9)Date\(9)\(9)Project/Altiris #\(9)Change Description\(9)\(9)\(9)\(9)\(9)Development Project\(9)Code Reviewer\(9)Date Reviewed
# MAGIC =====================================================================================================================================================
# MAGIC Abhiram Dasarathy\(9)2016-03-23\(9)5414 - MEP\(9)Original Programming\(9)\(9)\(9)\(9)IntegrateDev2\(9)\(9) Kalyan Neelam        2016-04-26
# MAGIC 
# MAGIC Reddy Sanam         2022-06-15              US492429               Changed column Length from 100                                                                                            Goutham Kalidindi    2022-07-14
# MAGIC                                                                                                to 255  for "MBR_SRVY_ANSWER_CD_TX"                         IntegrateDev2
# MAGIC                                                                                                field

# MAGIC HRA - MBR_SRVY_ANSWER Common Extract Job
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


SrcSysCd = get_widget_value("SrcSysCd","")
InFile = get_widget_value("InFile","")
OutFile = get_widget_value("OutFile","")

schema_HRA_Answer_tbl = StructType([
    StructField("Question_Code", StringType(), True),
    StructField("Answer_Code", StringType(), True)
])

df_HRA_Answer_tbl = (
    spark.read
    .option("header", True)
    .option("quote", '"')
    .option("sep", ",")
    .schema(schema_HRA_Answer_tbl)
    .csv(f"{adls_path_raw}/landing/{InFile}")
)

df_Transformer_stagevars = (
    df_HRA_Answer_tbl
    .withColumn("svQstnCdTx", strip_field(trim(F.col("Question_Code"))))
    .withColumn("svAnsrCdTx", strip_field(trim(F.col("Answer_Code"))))
)

df_Transformer_stagevars = df_Transformer_stagevars.withColumn(
    "svQstnCdIndicator",
    F.when(F.isnull(F.col("svQstnCdTx")) | (F.length(F.col("svQstnCdTx")) == 0), F.lit("N"))
     .when(F.isnull(F.col("svAnsrCdTx")) | (F.length(F.col("svAnsrCdTx")) == 0), F.lit("N"))
     .otherwise(F.lit("Y"))
)

df_Transformer_filtered = df_Transformer_stagevars.filter(F.col("svQstnCdIndicator") == "Y")

df_Transformer_Key = df_Transformer_filtered.select(
    F.lit(0).alias("MBR_SRVY_ANSWER_SK"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.lit("HRA").alias("MBR_SRVY_TYP_CD"),
    F.col("svQstnCdTx").alias("MBR_SRVY_QSTN_CD_TX"),
    F.col("svAnsrCdTx").alias("MBR_SRVY_ANSWER_CD_TX"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("MBR_SRVY_QSTN_SK")
)

df_final = df_Transformer_Key.select(
    F.col("MBR_SRVY_ANSWER_SK"),
    F.rpad(F.col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    F.rpad(F.col("MBR_SRVY_TYP_CD"), <...>, " ").alias("MBR_SRVY_TYP_CD"),
    F.rpad(F.col("MBR_SRVY_QSTN_CD_TX"), <...>, " ").alias("MBR_SRVY_QSTN_CD_TX"),
    F.rpad(F.col("MBR_SRVY_ANSWER_CD_TX"), <...>, " ").alias("MBR_SRVY_ANSWER_CD_TX"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("MBR_SRVY_QSTN_SK")
)

write_files(
    df_final,
    f"{adls_path_raw}/landing/{OutFile}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)