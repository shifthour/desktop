# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2013 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  EdwBhiAcctRptExtr
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  Extract Account Reporting Ref to be sent to BCBSA for BHI (Blue Health Intelligence)
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  EdwBhiRefExtrSeq
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
# MAGIC Praveen Annam                                          2013-08-22           5115 BHI                               Original programming                                                  EnterpriseNewDevl        Kalyan Neelam          2013-10-31
# MAGIC Praveen Annam                                          2014-07-14            5115 BHI                              FTP stage added to transfer in binary mode               EnterpriseNewDevl        Bhoomi Dasari           7/15/2014
# MAGIC Mohan Karnati                                            2020-08-03           US-252543                    Including High Performance n/w  in product short    
# MAGIC                                                                                                                                                 while extracting in the source query                          EnterpriseDev1              Hugh Sisson             2020-08-18
# MAGIC Tamannakumari                                          2024-02-29           US- 612200           Include additional BMADVH and BMADVP in source query
# MAGIC                                                                                                                               (where PROD_SH_NM = 'BMADVH', 'BMADVP')                       EnterpriseDev1              Jeyaprasanna           2024-03-04

# MAGIC Extract Account Reporting data,
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
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
StartDate = get_widget_value('StartDate','')
EndDate = get_widget_value('EndDate','')
CurrDate = get_widget_value('CurrDate','')
ProdIn = get_widget_value('ProdIn','')

jdbc_url, jdbc_props = get_db_config(edw_secret_name)
extract_query = f"""SELECT DISTINCT
   clsPln.PROD_ID,
   grp.GRP_ID,
   grp.GRP_NM,
   grp.PRNT_GRP_ID
FROM {EDWOwner}.CLS_PLN_DTL_I clsPln,
     {EDWOwner}.GRP_D grp,
     {EDWOwner}.PROD_D prd
WHERE clsPln.GRP_SK = grp.GRP_SK
AND clsPln.PROD_SK = prd.PROD_SK
AND clsPln.GRP_ID NOT IN ('10004000','10023000','10024000')
AND clsPln.CLS_PLN_DTL_PROD_CAT_CD = 'MED'
AND grp.PRNT_GRP_ID <> '650600000'
AND prd.PROD_SH_NM IN ('PC','PCB','BLUE-ACCESS','BLUE-SELECT','BCARE','BLUESELECT+','HP','BMADVH','BMADVP')
AND clsPln.CLS_PLN_DTL_TERM_DT_SK >= '2010-01-01'
"""
df_Account_Report = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_Trns = df_Account_Report.select(
    F.lit("240").alias("BHI_HOME_PLN_ID"),
    PadString(F.col("PROD_ID"), ' ', 15).alias("HOME_PLN_PROD_ID"),
    PadString(F.col("PRNT_GRP_ID"), ' ', 14).alias("ACCT"),
    PadString(F.col("GRP_ID"), ' ', 14).alias("RPTNG_ACCT_ID"),
    trim(PadString(F.col("GRP_NM"), ' ', 50), '\"', 'A').alias("RPTNG_ACCT_NM")
)

df_Copy_Extract = df_Trns.select(
    F.col("BHI_HOME_PLN_ID"),
    F.col("HOME_PLN_PROD_ID"),
    F.col("ACCT"),
    F.col("RPTNG_ACCT_ID"),
    F.col("RPTNG_ACCT_NM")
)

df_Copy_Count = df_Trns.select(
    F.col("BHI_HOME_PLN_ID")
)

df_Copy_Extract_Ordered = df_Copy_Extract.select(
    "BHI_HOME_PLN_ID",
    "HOME_PLN_PROD_ID",
    "ACCT",
    "RPTNG_ACCT_ID",
    "RPTNG_ACCT_NM"
)
df_Copy_Extract_Ordered = df_Copy_Extract_Ordered.withColumn(
    "BHI_HOME_PLN_ID", F.rpad(F.col("BHI_HOME_PLN_ID"), 3, " ")
).withColumn(
    "HOME_PLN_PROD_ID", F.rpad(F.col("HOME_PLN_PROD_ID"), 15, " ")
).withColumn(
    "ACCT", F.rpad(F.col("ACCT"), 14, " ")
).withColumn(
    "RPTNG_ACCT_ID", F.rpad(F.col("RPTNG_ACCT_ID"), 14, " ")
).withColumn(
    "RPTNG_ACCT_NM", F.rpad(F.col("RPTNG_ACCT_NM"), 50, " ")
)
write_files(
    df_Copy_Extract_Ordered,
    f"{adls_path_publish}/external/account_reporting_ref",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote=None,
    nullValue=None
)

df_Aggregator = df_Copy_Count.groupBy("BHI_HOME_PLN_ID").agg(F.count("*").alias("COUNT"))

df_Trns_cntrl = df_Aggregator.select(
    F.col("BHI_HOME_PLN_ID").alias("BHI_HOME_PLN_ID"),
    PadString(F.lit("ACCOUNT_REPORTING_REF"), ' ', 30).alias("EXTR_NM"),
    trim(F.lit(StartDate), '-', 'A').alias("MIN_CLM_PROCESSED_DT"),
    trim(F.lit(EndDate), '-', 'A').alias("MAX_CLM_PRCS_DT"),
    trim(F.lit(CurrDate), '-', 'A').alias("SUBMSN_DT"),
    (Str(F.lit('0'), F.lit(10) - LEN(F.col("COUNT"))) + F.col("COUNT")).alias("RCRD_CT"),
    If(Left(F.lit("00000000000000"), F.lit(1)) <> F.lit("-"),
       F.lit("+") + F.lit("00000000000000"),
       F.lit("00000000000000")).alias("TOT_SUBMT_AMT"),
    If(Left(F.lit("00000000000000"), F.lit(1)) <> F.lit("-"),
       F.lit("+") + F.lit("00000000000000"),
       F.lit("00000000000000")).alias("TOT_NONCOV_AMT"),
    If(Left(F.lit("00000000000000"), F.lit(1)) <> F.lit("-"),
       F.lit("+") + F.lit("00000000000000"),
       F.lit("00000000000000")).alias("TOT_ALW_AMT"),
    If(Left(F.lit("00000000000000"), F.lit(1)) <> F.lit("-"),
       F.lit("+") + F.lit("00000000000000"),
       F.lit("00000000000000")).alias("TOT_PD_AMT"),
    If(Left(F.lit("00000000000000"), F.lit(1)) <> F.lit("-"),
       F.lit("+") + F.lit("00000000000000"),
       F.lit("00000000000000")).alias("TOT_COB_TPL_AMT"),
    If(Left(F.lit("00000000000000"), F.lit(1)) <> F.lit("-"),
       F.lit("+") + F.lit("00000000000000"),
       F.lit("00000000000000")).alias("TOT_COINS_AMT"),
    If(Left(F.lit("00000000000000"), F.lit(1)) <> F.lit("-"),
       F.lit("+") + F.lit("00000000000000"),
       F.lit("00000000000000")).alias("TOT_COPAY_AMT"),
    If(Left(F.lit("00000000000000"), F.lit(1)) <> F.lit("-"),
       F.lit("+") + F.lit("00000000000000"),
       F.lit("00000000000000")).alias("TOT_DEDCT_AMT"),
    If(Left(F.lit("00000000000000"), F.lit(1)) <> F.lit("-"),
       F.lit("+") + F.lit("00000000000000"),
       F.lit("00000000000000")).alias("TOT_FFS_EQVLNT_AMT")
)

df_Trns_cntrl_Ordered = df_Trns_cntrl.select(
    "BHI_HOME_PLN_ID",
    "EXTR_NM",
    "MIN_CLM_PROCESSED_DT",
    "MAX_CLM_PRCS_DT",
    "SUBMSN_DT",
    "RCRD_CT",
    "TOT_SUBMT_AMT",
    "TOT_NONCOV_AMT",
    "TOT_ALW_AMT",
    "TOT_PD_AMT",
    "TOT_COB_TPL_AMT",
    "TOT_COINS_AMT",
    "TOT_COPAY_AMT",
    "TOT_DEDCT_AMT",
    "TOT_FFS_EQVLNT_AMT"
)
df_Trns_cntrl_Ordered = df_Trns_cntrl_Ordered.withColumn(
    "BHI_HOME_PLN_ID", F.rpad(F.col("BHI_HOME_PLN_ID"), 3, " ")
).withColumn(
    "EXTR_NM", F.rpad(F.col("EXTR_NM"), 30, " ")
).withColumn(
    "MIN_CLM_PROCESSED_DT", F.rpad(F.col("MIN_CLM_PROCESSED_DT"), 8, " ")
).withColumn(
    "MAX_CLM_PRCS_DT", F.rpad(F.col("MAX_CLM_PRCS_DT"), 8, " ")
).withColumn(
    "SUBMSN_DT", F.rpad(F.col("SUBMSN_DT"), 8, " ")
).withColumn(
    "RCRD_CT", F.rpad(F.col("RCRD_CT"), 10, " ")
).withColumn(
    "TOT_SUBMT_AMT", F.rpad(F.col("TOT_SUBMT_AMT"), 15, " ")
).withColumn(
    "TOT_NONCOV_AMT", F.rpad(F.col("TOT_NONCOV_AMT"), 15, " ")
).withColumn(
    "TOT_ALW_AMT", F.rpad(F.col("TOT_ALW_AMT"), 15, " ")
).withColumn(
    "TOT_PD_AMT", F.rpad(F.col("TOT_PD_AMT"), 15, " ")
).withColumn(
    "TOT_COB_TPL_AMT", F.rpad(F.col("TOT_COB_TPL_AMT"), 15, " ")
).withColumn(
    "TOT_COINS_AMT", F.rpad(F.col("TOT_COINS_AMT"), 15, " ")
).withColumn(
    "TOT_COPAY_AMT", F.rpad(F.col("TOT_COPAY_AMT"), 15, " ")
).withColumn(
    "TOT_DEDCT_AMT", F.rpad(F.col("TOT_DEDCT_AMT"), 15, " ")
).withColumn(
    "TOT_FFS_EQVLNT_AMT", F.rpad(F.col("TOT_FFS_EQVLNT_AMT"), 15, " ")
)

write_files(
    df_Trns_cntrl_Ordered,
    f"{adls_path_publish}/external/submission_control",
    delimiter=",",
    mode="append",
    is_parquet=False,
    header=False,
    quote=None,
    nullValue=None
)