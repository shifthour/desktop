# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 10/21/08 10:40:52 Batch  14905_38463 PROMOTE bckcetl ids20 dsadm rc for brent  
# MAGIC ^1_1 10/21/08 10:17:42 Batch  14905_37069 INIT bckcett testIDS dsadm rc for brent 
# MAGIC ^1_1 10/20/08 12:51:44 Batch  14904_46312 PROMOTE bckcett testIDS u08717 Brent
# MAGIC ^1_1 10/20/08 10:48:46 Batch  14904_38931 INIT bckcett devlIDS u08717 Brent
# MAGIC ^1_1 08/01/08 10:50:09 Batch  14824_39011 INIT bckcett devlIDS u03651 steffy
# MAGIC ^1_2 07/23/08 15:42:49 Batch  14815_56575 PROMOTE bckcett devlIDS u08717 Brent
# MAGIC ^1_2 07/23/08 15:08:26 Batch  14815_54510 INIT bckcett devlIDScur u08717 Brent
# MAGIC ^1_1 05/12/08 08:17:54 Batch  14743_29878 INIT bckcett devlIDScur u08717 Brent
# MAGIC 
# MAGIC Â© Copyright 2008 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  FctsPaymtSumRecycleSeq
# MAGIC                
# MAGIC 
# MAGIC PROCESSING:   Write keys found in error recyle hash file to hit list for next day processing.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Brent Leland            2008-02-29        Primary Key           Original Programming.                                                                  devlIDScur                     Steph Goddard          05/06/2008

# MAGIC Loaded by every Fkey job
# MAGIC Write keys to hit list /ids/prod/update/FctsCustSvcHitList.dat
# MAGIC Write keys from foreign key job errors to hit list
# MAGIC File is overwritten to remove yesterday's recycle records
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Read from hashed file (Scenario C: translate hashed file to Parquet)
df_hf_custsvc_recycle_keys = spark.read.parquet(f"{adls_path}/hf_custsvc_recycle_keys.parquet")

# Transformer: Trans2
df_Trans2 = df_hf_custsvc_recycle_keys.filter(trim(col("SRC_SYS_CD")) == "FACETS").select(
    col("CUST_SVC_ID").alias("CSSC_ID"),
    col("TASK_SEQ_NO").alias("CSTK_SEQ_NO"),
    lit("1753-01-01 00:00:00.000").alias("CSTK_LAST_UPD_DTM")
)

df_Trans2 = df_Trans2.withColumn("CSSC_ID", rpad(col("CSSC_ID"), 12, " "))

df_final = df_Trans2.select("CSSC_ID", "CSTK_SEQ_NO", "CSTK_LAST_UPD_DTM")

# Write to sequential file (CSeqFileStage)
write_files(
    df_final,
    f"{adls_path}/update/FctsCustSvcHitList.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)