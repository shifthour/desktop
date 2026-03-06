# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2013 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  EdwBhiSubCtrlEmptyExtr
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  Append one default row to submission control file for empty extraction 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  None
# MAGIC 
# MAGIC HASH FILES:  None
# MAGIC 
# MAGIC 
# MAGIC RERUN INFORMATION: 
# MAGIC 
# MAGIC              PREVIOUS RUN SUCCESSFUL:   Nothing to reset. 
# MAGIC              PREVIOUS RUN ABORTED:         Nothing to reset, just restart.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                                                    Date                    Project/Ticket #\(9)           Change Description\(9)\(9)\(9)              Development Project\(9)      Code Reviewer\(9)        Date Reviewed       
# MAGIC ------------------                                                 -------------------         ---------------------------\(9)           -----------------------------------------------------------------------            --------------------------------\(9)      -------------------------         -------------------------    
# MAGIC Aishwarya                                                2016-04-20           5604 BHI                               Original programming                                                  EnterpriseDevl                  Kalyan Neelam          2016-07-06

# MAGIC Append one default row to submission control file for empty extraction
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


CurrDate = get_widget_value('CurrDate','')
ExtrNm = get_widget_value('ExtrNm','')

df_Row_Generator_15 = spark.createDataFrame(
    [
        ("", 0, 0, 0, 0, 0, 0, 0, 0, 0)
    ],
    [
        "BHI_HOME_PLN_ID",
        "COUNT",
        "TOT_SUBMT_AMT",
        "TOT_NON_COV_AMT",
        "TOT_ALW_AMT",
        "TOT_PD_AMT",
        "TOT_COB_TPL_AMT",
        "TOT_COINS_AMT",
        "TOT_COPAY_AMT",
        "TOT_DEDCT_AMT"
    ]
)

df_trns_cntrl_intermediate = (
    df_Row_Generator_15
    .withColumn("BHI_HOME_PLN_ID", F.lit("240"))
    .withColumn("EXTR_NM", PadString(F.lit(ExtrNm), " ", 30))
    .withColumn("MIN_CLM_PROCESSED_DT", trim(F.lit(CurrDate)))
    .withColumn("MAX_CLM_PRCS_DT", trim(F.lit(CurrDate)))
    .withColumn("SUBMSN_DT", trim(F.lit(CurrDate)))
    .withColumn("RCRD_CT", F.lit("0000000000"))
    .withColumn("TOT_SUBMT_AMT", F.lit("+00000000000000"))
    .withColumn("TOT_NONCOV_AMT", F.lit("+00000000000000"))
    .withColumn("TOT_ALW_AMT", F.lit("+00000000000000"))
    .withColumn("TOT_PD_AMT", F.lit("+00000000000000"))
    .withColumn("TOT_COB_TPL_AMT", F.lit("+00000000000000"))
    .withColumn("TOT_COINS_AMT", F.lit("+00000000000000"))
    .withColumn("TOT_COPAY_AMT", F.lit("+00000000000000"))
    .withColumn("TOT_DEDCT_AMT", F.lit("+00000000000000"))
    .withColumn("TOT_FFS_EQVLNT_AMT", F.lit("+00000000000000"))
)

df_trns_cntrl = df_trns_cntrl_intermediate.select(
    F.rpad("BHI_HOME_PLN_ID", 3, " ").alias("BHI_HOME_PLN_ID"),
    F.rpad("EXTR_NM", 30, " ").alias("EXTR_NM"),
    F.rpad("MIN_CLM_PROCESSED_DT", 8, " ").alias("MIN_CLM_PROCESSED_DT"),
    F.rpad("MAX_CLM_PRCS_DT", 8, " ").alias("MAX_CLM_PRCS_DT"),
    F.rpad("SUBMSN_DT", 8, " ").alias("SUBMSN_DT"),
    F.rpad("RCRD_CT", 10, " ").alias("RCRD_CT"),
    F.rpad("TOT_SUBMT_AMT", 15, " ").alias("TOT_SUBMT_AMT"),
    F.rpad("TOT_NONCOV_AMT", 15, " ").alias("TOT_NONCOV_AMT"),
    F.rpad("TOT_ALW_AMT", 15, " ").alias("TOT_ALW_AMT"),
    F.rpad("TOT_PD_AMT", 15, " ").alias("TOT_PD_AMT"),
    F.rpad("TOT_COB_TPL_AMT", 15, " ").alias("TOT_COB_TPL_AMT"),
    F.rpad("TOT_COINS_AMT", 15, " ").alias("TOT_COINS_AMT"),
    F.rpad("TOT_COPAY_AMT", 15, " ").alias("TOT_COPAY_AMT"),
    F.rpad("TOT_DEDCT_AMT", 15, " ").alias("TOT_DEDCT_AMT"),
    F.rpad("TOT_FFS_EQVLNT_AMT", 15, " ").alias("TOT_FFS_EQVLNT_AMT")
)

write_files(
    df_trns_cntrl,
    f"{adls_path_publish}/external/submission_control",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="",
    nullValue=None
)