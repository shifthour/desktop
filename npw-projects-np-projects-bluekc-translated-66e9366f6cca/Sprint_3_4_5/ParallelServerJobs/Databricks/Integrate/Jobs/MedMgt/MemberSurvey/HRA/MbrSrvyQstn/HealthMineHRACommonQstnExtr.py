# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2016 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  HlthMineHRAMbrSrvyQstnAnswrExtrSeq
# MAGIC                
# MAGIC 
# MAGIC PROCESSING: Common Extract job for the MBR_SRVY_QSTN table in to Common File Format. The job processes landing files generated from the HRA.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC =====================================================================================================================================================
# MAGIC Developer\(9)Date\(9)\(9)Project/Altiris #\(9)Change Description\(9)\(9)\(9)\(9)\(9)Development Project\(9)Code Reviewer\(9)Date Reviewed
# MAGIC =====================================================================================================================================================
# MAGIC Abhiram Dasarathy\(9)2016-03-23\(9)5414 - MEP\(9)Original Programming\(9)\(9)\(9)\(9)IntegrateDev2\(9)\(9) Kalyan Neelam        2016-04-26

# MAGIC HRA - MBR_SRVY_QSTN Common Extract Job
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, lit, length, when, rpad, trim
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


SrcSysCd = get_widget_value('SrcSysCd','')
InFile = get_widget_value('InFile','')
OutFile = get_widget_value('OutFile','')

schema_HRA_Question_tbl = StructType([
    StructField("Question_Code", StringType(), True),
    StructField("Question_Text", StringType(), True),
    StructField("Answer_Type", StringType(), True),
    StructField("Answer_Datatype", StringType(), True)
])

df_HRA_Question_tbl = (
    spark.read
    .option("header", True)
    .option("quote", "\"")
    .option("delimiter", ",")
    .schema(schema_HRA_Question_tbl)
    .csv(f"{adls_path_raw}/landing/{InFile}")
)

df_transformer = (
    df_HRA_Question_tbl
    .withColumn("svQstnCd", strip_field(trim(col("Question_Code"))))
    .withColumn("svAnsrDtypTx", strip_field(trim(col("Answer_Datatype"))))
    .withColumn("svMbrSrvyQstnAnsrTypTx", strip_field(trim(col("Answer_Type"))))
    .withColumn("svQstnTx", strip_field(trim(col("Question_Text"))))
    .withColumn(
        "svQstnCodeInd",
        when(
            col("svQstnCd").isNull() | (length(col("svQstnCd")) == 0)
            | col("svAnsrDtypTx").isNull() | (length(col("svAnsrDtypTx")) == 0)
            | col("svMbrSrvyQstnAnsrTypTx").isNull() | (length(col("svMbrSrvyQstnAnsrTypTx")) == 0)
            | col("svQstnTx").isNull() | (length(col("svQstnTx")) == 0),
            'N'
        ).otherwise('Y')
    )
)

df_enriched = df_transformer.filter(col("svQstnCodeInd") == 'Y')

df_enriched = (
    df_enriched
    .withColumn("MBR_SRVY_QSTN_SK", lit(0))
    .withColumn("SRC_SYS_CD", lit(SrcSysCd))
    .withColumn("MBR_SRVY_TYP_CD", lit("HRA"))
    .withColumn("QSTN_CD_TX", col("svQstnCd"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(0))
    .withColumn("MBR_SRVY_SK", lit(0))
    .withColumn("MBR_SRVY_TYP_CD_SK", lit(0))
    .withColumn("EFF_YR_MO_SK", lit("175301"))
    .withColumn("TERM_YR_MO_SK", lit("219912"))
    .withColumn(
        "ANSWER_DTYP_TX",
        when(
            col("svAnsrDtypTx").isNull() | (length(col("svAnsrDtypTx")) == 0),
            "UNK"
        ).otherwise(col("svAnsrDtypTx"))
    )
    .withColumn(
        "MBR_SRVY_QSTN_ANSWER_TYP_TX",
        when(
            col("svMbrSrvyQstnAnsrTypTx").isNull() | (length(col("svMbrSrvyQstnAnsrTypTx")) == 0),
            "UNK"
        ).otherwise(col("svMbrSrvyQstnAnsrTypTx"))
    )
    .withColumn(
        "QSTN_TX",
        when(
            col("svQstnTx").isNull() | (length(col("svQstnTx")) == 0),
            "UNK"
        ).otherwise(col("svQstnTx"))
    )
    .withColumn("MBR_SRVY_TYP_CD_SRC_CD", lit("HRA"))
)

df_final = df_enriched.select(
    col("MBR_SRVY_QSTN_SK").alias("MBR_SRVY_QSTN_SK"),
    rpad(col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    rpad(col("MBR_SRVY_TYP_CD"), <...>, " ").alias("MBR_SRVY_TYP_CD"),
    rpad(col("QSTN_CD_TX"), <...>, " ").alias("QSTN_CD_TX"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("MBR_SRVY_SK").alias("MBR_SRVY_SK"),
    col("MBR_SRVY_TYP_CD_SK").alias("MBR_SRVY_TYP_CD_SK"),
    rpad(col("EFF_YR_MO_SK"), 6, " ").alias("EFF_YR_MO_SK"),
    rpad(col("TERM_YR_MO_SK"), 6, " ").alias("TERM_YR_MO_SK"),
    rpad(col("ANSWER_DTYP_TX"), <...>, " ").alias("ANSWER_DTYP_TX"),
    rpad(col("MBR_SRVY_QSTN_ANSWER_TYP_TX"), <...>, " ").alias("MBR_SRVY_QSTN_ANSWER_TYP_TX"),
    rpad(col("QSTN_TX"), <...>, " ").alias("QSTN_TX"),
    rpad(col("MBR_SRVY_TYP_CD_SRC_CD"), <...>, " ").alias("MBR_SRVY_TYP_CD_SRC_CD")
)

write_files(
    df_final,
    f"{adls_path_raw}/landing/{OutFile}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)