# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 07/24/08 10:56:42 Batch  14816_39411 PROMOTE bckcetl ids20 dsadm rc for brent 
# MAGIC ^1_1 07/24/08 10:41:03 Batch  14816_38477 INIT bckcett testIDS dsadm rc fro brent 
# MAGIC ^1_3 07/22/08 08:32:54 Batch  14814_30778 PROMOTE bckcett testIDS u08717 Brent
# MAGIC ^1_3 07/22/08 08:21:54 Batch  14814_30118 INIT bckcett devlIDS u08717 Brent
# MAGIC ^1_2 07/07/08 11:47:11 Batch  14799_42473 INIT bckcett devlIDS u11141 has
# MAGIC ^1_1 06/23/08 16:01:25 Batch  14785_57704 PROMOTE bckcett devlIDS u10913 O. Nielsen move from devlIDScur to devlIDS for B. Leland
# MAGIC ^1_1 06/23/08 15:24:04 Batch  14785_55472 INIT bckcett devlIDScur u10913 O. Nielsen move from devlIDSCUR to devlIDS for B. Leland
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
# MAGIC Brent Leland            2008-08-15        Primary Key           Original Programming.                                                                  devlIDScur                     Steph Goddard          02/22/2008

# MAGIC Loaded by every Fkey job
# MAGIC Write keys to hit list /ids/prod/update/FctsPaymtSumHitList.dat
# MAGIC Write keys from foreign key job errors to hit list
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Stage: hf_paymtsum_recycle_keys (CHashedFileStage, Scenario C: Read from Parquet)
df_hf_paymtsum_recycle_keys = spark.read.parquet(f"{adls_path}/hf_paymtsum_recycle_keys.parquet")

# Stage: Trans2 (CTransformerStage)
df_Trans2_output = df_hf_paymtsum_recycle_keys.filter(trim(col("SRC_SYS_CD")) == 'FACETS').select(
    col("PAYMT_REF_ID")
)

df_Trans2_output = df_Trans2_output.withColumn("PAYMT_REF_ID", rpad(col("PAYMT_REF_ID"), <...>, " "))

# Stage: FctsPaymtSumHitList (CSeqFileStage)
write_files(
    df_Trans2_output.select("PAYMT_REF_ID"),
    f"{adls_path}/update/FctsPaymtSumHitList.dat",
    ",",
    "append",
    False,
    False,
    "\"",
    None
)