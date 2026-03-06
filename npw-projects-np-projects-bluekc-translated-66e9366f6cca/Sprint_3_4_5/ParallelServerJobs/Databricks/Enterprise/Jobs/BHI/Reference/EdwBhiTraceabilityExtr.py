# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2013 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  EdwBhiTraceabilityExtr
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  Extract Traceability Ref to be sent to BCBSA for BHI (Blue Health Intelligence)
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
# MAGIC Praveen Annam                                          2013-08-22           5115 BHI                               Original programming                                                  EnterpriseNewDevl        Kalyan Neelam         2013-10-31
# MAGIC Praveen Annam                                          2014-07-14           5115 BHI                               FTP stage added to transfer in binary mode               EnterpriseNewDevl        Bhoomi Dasari          7/15/2014 
# MAGIC Abhiram Dasarathy                                      2019-07-29          US- 136673                           Coding from MA Datamart to BCA                               EnterpriseDev2          Kalyan Neelam         2019-08-09
# MAGIC                                                                                                                                           (Blue Cross Association) for Traceability Extract    
# MAGIC 
# MAGIC Mohan Karnati                                            2020-02-10           US#189058                       Generating a record for OPTUMRX in Trns stage.        EnterpriseDev2          Jaideep Mankala       02/11/2020

# MAGIC Create two rows for Traceability Extract,
# MAGIC Create fixed length data and provide monthly file to BHI
# MAGIC Control File is created with file name and respective counts, write mode is set as append to capture information for all BHI files
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, lit, explode, array, struct, unionByName, expr, count as F_count, length, lpad, concat, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------

# Parameters
EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
MADataMartAsstOwner = get_widget_value('MADataMartAsstOwner','')
ma_datamart_asst_secret_name = get_widget_value('ma_datamart_asst_secret_name','')
StartDate = get_widget_value('StartDate','')
EndDate = get_widget_value('EndDate','')
CurrDate = get_widget_value('CurrDate','')
ProdIn = get_widget_value('ProdIn','')

# Stage: Row_Generator_15
df_row_generator_15 = spark.createDataFrame(
    [(None, None, None)],
    ["BHI_HOME_PLN_ID", "TRACEABILITY_FLD", "TRACEABILITY_DESC"]
)

# Stage: Trns
df_Fun1 = df_row_generator_15.select(
    explode(
        array(
            struct(
                lit('240').alias('BHI_HOME_PLN_ID'),
                lit('FCTS ').alias('TRACEABILITY_FLD'),
                lit('FACETS').alias('TRACEABILITY_DESC')
            ),
            struct(
                lit('240').alias('BHI_HOME_PLN_ID'),
                lit('ESI  ').alias('TRACEABILITY_FLD'),
                lit('EXPRESS SCRIPTS INC').alias('TRACEABILITY_DESC')
            ),
            struct(
                lit('240').alias('BHI_HOME_PLN_ID'),
                lit('OPTUM').alias('TRACEABILITY_FLD'),
                lit('OPTUMRX').alias('TRACEABILITY_DESC')
            )
        )
    ).alias('col')
).select('col.*')

# Stage: MCARE_PLN_V (ODBCConnectorPX)
extract_query = f"SELECT DISTINCT SRC_SYS_CD FROM {MADataMartAsstOwner}.MCARE_PLN_V"
jdbc_url, jdbc_props = get_db_config(ma_datamart_asst_secret_name)
df_MCARE_PLN_V = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# Stage: Pln_Trns
df_Fun2 = df_MCARE_PLN_V.select(
    lit('240').alias('BHI_HOME_PLN_ID'),
    lit('BPO  ').alias('TRACEABILITY_FLD'),
    col('SRC_SYS_CD').alias('TRACEABILITY_DESC')
)

# Stage: Fun (PxFunnel)
df_Fun = df_Fun1.unionByName(df_Fun2)

# Stage: Copy (PxCopy) -> Extract, Count
df_Extract = df_Fun.select("BHI_HOME_PLN_ID", "TRACEABILITY_FLD", "TRACEABILITY_DESC")
df_Count = df_Fun.select("BHI_HOME_PLN_ID")

# Stage: traceability_ref (PxSequentialFile)
df_Extract_out = df_Extract.select(
    rpad(col("BHI_HOME_PLN_ID"), 3, " ").alias("BHI_HOME_PLN_ID"),
    rpad(col("TRACEABILITY_FLD"), 5, " ").alias("TRACEABILITY_FLD"),
    rpad(col("TRACEABILITY_DESC"), 30, " ").alias("TRACEABILITY_DESC")
)

write_files(
    df_Extract_out,
    f"{adls_path_publish}/external/traceability_ref",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote=None,
    nullValue=None
)

# Stage: Aggregator (PxAggregator)
df_Aggregator = df_Count.groupBy("BHI_HOME_PLN_ID").agg(F_count("*").alias("COUNT"))

# Stage: Trns_control
df_Trns_control = df_Aggregator.select(
    col("BHI_HOME_PLN_ID"),
    lit("STD_TRACEABILITY_REF").alias("EXTR_NM"),
    trim(lit(StartDate)).alias("MIN_CLM_PROCESSED_DT"),
    trim(lit(EndDate)).alias("MAX_CLM_PRCS_DT"),
    trim(lit(CurrDate)).alias("SUBMSN_DT"),
    expr("LPAD(CAST(COUNT AS STRING), 10, '0')").alias("RCRD_CT"),
    lit("+00000000000000").alias("TOT_SUBMT_AMT"),
    lit("+00000000000000").alias("TOT_NONCOV_AMT"),
    lit("+00000000000000").alias("TOT_ALW_AMT"),
    lit("+00000000000000").alias("TOT_PD_AMT"),
    lit("+00000000000000").alias("TOT_COB_TPL_AMT"),
    lit("+00000000000000").alias("TOT_COINS_AMT"),
    lit("+00000000000000").alias("TOT_COPAY_AMT"),
    lit("+00000000000000").alias("TOT_DEDCT_AMT"),
    lit("+00000000000000").alias("TOT_FFS_EQVLNT_AMT")
)

# Stage: submission_control (PxSequentialFile)
df_submission_control = df_Trns_control.select(
    rpad(col("BHI_HOME_PLN_ID"), 3, " ").alias("BHI_HOME_PLN_ID"),
    rpad(col("EXTR_NM"), 30, " ").alias("EXTR_NM"),
    rpad(col("MIN_CLM_PROCESSED_DT"), 8, " ").alias("MIN_CLM_PROCESSED_DT"),
    rpad(col("MAX_CLM_PRCS_DT"), 8, " ").alias("MAX_CLM_PRCS_DT"),
    rpad(col("SUBMSN_DT"), 8, " ").alias("SUBMSN_DT"),
    rpad(col("RCRD_CT"), 10, " ").alias("RCRD_CT"),
    rpad(col("TOT_SUBMT_AMT"), 15, " ").alias("TOT_SUBMT_AMT"),
    rpad(col("TOT_NONCOV_AMT"), 15, " ").alias("TOT_NONCOV_AMT"),
    rpad(col("TOT_ALW_AMT"), 15, " ").alias("TOT_ALW_AMT"),
    rpad(col("TOT_PD_AMT"), 15, " ").alias("TOT_PD_AMT"),
    rpad(col("TOT_COB_TPL_AMT"), 15, " ").alias("TOT_COB_TPL_AMT"),
    rpad(col("TOT_COINS_AMT"), 15, " ").alias("TOT_COINS_AMT"),
    rpad(col("TOT_COPAY_AMT"), 15, " ").alias("TOT_COPAY_AMT"),
    rpad(col("TOT_DEDCT_AMT"), 15, " ").alias("TOT_DEDCT_AMT"),
    rpad(col("TOT_FFS_EQVLNT_AMT"), 15, " ").alias("TOT_FFS_EQVLNT_AMT")
)

write_files(
    df_submission_control,
    f"{adls_path_publish}/external/submission_control",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote=None,
    nullValue=None
)