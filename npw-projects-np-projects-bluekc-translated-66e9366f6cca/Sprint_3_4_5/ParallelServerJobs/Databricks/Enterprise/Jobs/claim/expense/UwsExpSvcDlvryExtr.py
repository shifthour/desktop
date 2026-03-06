# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_3 03/23/09 15:20:40 Batch  15058_55245 INIT bckcetl edw10 dsadm dsadm
# MAGIC ^1_1 03/12/09 19:44:50 Batch  15047_71107 INIT bckcetl edw10 dcg01 sa - bringing everthing down for test
# MAGIC ^1_2 02/25/09 17:10:06 Batch  15032_61828 INIT bckcetl edw10 dcg01 Bringing production code down to edw_test  claim only
# MAGIC ^1_1 02/01/08 12:08:08 Batch  14642_43698 INIT bckcetl edw10 dsadm dsadm
# MAGIC ^1_1 12/27/07 14:18:21 Batch  14606_51505 INIT bckcetl edw10 dsadm dsadm
# MAGIC ^1_1 08/31/07 14:39:46 Batch  14488_52789 INIT bckcetl edw10 dsadm dsadm
# MAGIC ^1_2 04/17/07 08:21:24 Batch  14352_30089 INIT bckcetl edw10 dsadm dsadm
# MAGIC ^1_1 04/04/07 08:38:24 Batch  14339_31110 INIT bckcetl edw10 dsadm dsadm
# MAGIC ^1_1 12/18/06 12:46:24 Batch  14232_46016 INIT bckcetl edw10 dsadm Backup of Claim For12/18/2006
# MAGIC ^1_1 08/15/06 14:50:29 Batch  14107_53439 PROMOTE bckcetl edw10 dsadm bls
# MAGIC ^1_1 08/15/06 14:45:08 Batch  14107_53117 INIT bckcett devlEDW10 i08185 bls
# MAGIC ^1_2 07/11/06 07:22:48 Batch  14072_26575 INIT bckcett devlEDW10 u08717 Brent
# MAGIC ^1_1 07/07/06 11:49:12 Batch  14068_42557 INIT bckcett devlEDW10 u05779 bj
# MAGIC ^1_1 04/24/06 08:59:47 Batch  13994_32389 INIT bckcett devlEDW10 u03651 steffy
# MAGIC 
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     UwsExpSvcDlvryExtr
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:    Pulls data from EXP_SVC_DLVRY to a landing file for the EDW
# MAGIC       
# MAGIC 
# MAGIC INPUTS:	 UWS EXP_SVC_DLVRY
# MAGIC   
# MAGIC 
# MAGIC HASH FILES: hf_exp_svc_dlvry for primary key lookup
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                             
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:  Create load file for EXP_SVC_DLVRY_D, pulling the uws and adding rows for UNK and NA
# MAGIC   
# MAGIC 
# MAGIC OUTPUTS:    Sequential file 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Date                 Developer                Change Description
# MAGIC ------------------      ----------------------------     --------------------------------------------------------------------------------------------------------
# MAGIC 2005-01-11      Suzanne Saylor         Original Programming.
# MAGIC 2005-07-12      Brent Leland              Added UWSOwner to parameter list and used in SQL statement.

# MAGIC Extract UWS Data
# MAGIC Apply business logic
# MAGIC Assign primary surrogate key
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


UWSOwner = get_widget_value('UWSOwner','')
CurrentDate = get_widget_value('CurrentDate','')
uws_secret_name = get_widget_value('uws_secret_name','')

jdbc_url, jdbc_props = get_db_config(uws_secret_name)

# Read from dummy table replacing hashed file hf_exp_svc_dlvry_lookup (Scenario B read step)
df_hf_exp_svc_dlvry_lookup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", "SELECT SRC_SYS_CD, EXP_SVC_DLVRY_CD, CRT_RUN_CYC_EXCTN_DT_SK, EXP_SVC_DLVRY_SK FROM dummy_hf_exp_svc_dlvry")
    .load()
)

# Read from the ODBC stage (EXP_SVC_DLVRY_D), removing the "WHERE EXP_SVC_DLVRY_CD = ?" condition
df_EXP_SVC_DLVRY_D = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"SELECT EXP_SVC_DLVRY_CD, EXP_SVC_DLVRY_NM, USER_ID, LAST_UPDT_DT_SK FROM {UWSOwner}.EXP_SVC_DLVRY")
    .load()
)

# Transformer Strip
df_Strip = df_EXP_SVC_DLVRY_D.select(
    trim("EXP_SVC_DLVRY_CD").alias("EXP_SVC_DLVRY_CD"),
    trim("EXP_SVC_DLVRY_NM").alias("EXP_SVC_DLVRY_NM"),
    trim("USER_ID").alias("USER_ID"),
    trim("LAST_UPDT_DT_SK").alias("LAST_UPDT_DT_SK")
)

# Transformer BusinessRules
df_BusinessRules = df_Strip.select(
    F.lit("UWS").alias("SRC_SYS_CD"),
    col("EXP_SVC_DLVRY_CD").alias("EXP_SVC_DLVRY_CD"),
    col("EXP_SVC_DLVRY_NM").alias("EXP_SVC_DLVRY_NM"),
    col("USER_ID").alias("USER_ID"),
    col("LAST_UPDT_DT_SK").alias("LAST_UPDT_DT_SK")
)

# Transformer PrimaryKey - left join with hf_exp_svc_dlvry_lookup (dummy table)
df_PrimaryKey_joined = (
    df_BusinessRules.alias("B")
    .join(
        df_hf_exp_svc_dlvry_lookup.alias("L"),
        (col("B.SRC_SYS_CD") == col("L.SRC_SYS_CD"))
        & (col("B.EXP_SVC_DLVRY_CD") == col("L.EXP_SVC_DLVRY_CD")),
        "left"
    )
    .select(
        col("B.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("B.EXP_SVC_DLVRY_CD").alias("EXP_SVC_DLVRY_CD"),
        col("B.EXP_SVC_DLVRY_NM").alias("EXP_SVC_DLVRY_NM"),
        col("B.USER_ID").alias("USER_ID"),
        col("B.LAST_UPDT_DT_SK").alias("LAST_UPDT_DT_SK"),
        when(col("L.SRC_SYS_CD").isNull(), lit(True)).otherwise(lit(False)).alias("NOTFOUND"),
        when(col("L.SRC_SYS_CD").isNull(), current_date()).otherwise(col("L.CRT_RUN_CYC_EXCTN_DT_SK")).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        col("L.EXP_SVC_DLVRY_SK").alias("EXP_SVC_DLVRY_SK")
    )
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", current_date())
)

# Apply SurrogateKeyGen for KeyMgtGetNextValueConcurrent usage
df_enriched = df_PrimaryKey_joined
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"EXP_SVC_DLVRY_SK",<schema>,<secret_name>)

# Link write_exp_svc_dlvry -> filter NOTFOUND rows
df_write_exp_svc_dlvry = df_enriched.filter(col("NOTFOUND") == True).select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("EXP_SVC_DLVRY_CD").alias("EXP_SVC_DLVRY_CD"),
    col("CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    col("EXP_SVC_DLVRY_SK").alias("EXP_SVC_DLVRY_SK")
)

# Replace hf_exp_svc_dlvry_write with a merge to dummy_hf_exp_svc_dlvry (Scenario B write step)
spark.sql("DROP TABLE IF EXISTS STAGING.UwsExpSvcDlvryExtr_hf_exp_svc_dlvry_write_temp")
(
    df_write_exp_svc_dlvry.write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", "STAGING.UwsExpSvcDlvryExtr_hf_exp_svc_dlvry_write_temp")
    .mode("overwrite")
    .save()
)
merge_sql_hf_exp_svc_dlvry_write = """
MERGE dummy_hf_exp_svc_dlvry AS T
USING STAGING.UwsExpSvcDlvryExtr_hf_exp_svc_dlvry_write_temp AS S
ON T.SRC_SYS_CD = S.SRC_SYS_CD
AND T.EXP_SVC_DLVRY_CD = S.EXP_SVC_DLVRY_CD
WHEN MATCHED THEN
  UPDATE SET
    T.CRT_RUN_CYC_EXCTN_DT_SK = S.CRT_RUN_CYC_EXCTN_DT_SK,
    T.EXP_SVC_DLVRY_SK = S.EXP_SVC_DLVRY_SK
WHEN NOT MATCHED THEN
  INSERT (SRC_SYS_CD, EXP_SVC_DLVRY_CD, CRT_RUN_CYC_EXCTN_DT_SK, EXP_SVC_DLVRY_SK)
  VALUES (S.SRC_SYS_CD, S.EXP_SVC_DLVRY_CD, S.CRT_RUN_CYC_EXCTN_DT_SK, S.EXP_SVC_DLVRY_SK)
;
"""
execute_dml(merge_sql_hf_exp_svc_dlvry_write, jdbc_url, jdbc_props)

# DefaultUNK and DefaultNA rows
schema_Default = StructType([
    StructField("EXP_SVC_DLVRY_SK", IntegerType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("EXP_SVC_DLVRY_CD", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("EXP_SVC_DLVRY_NM", StringType(), True),
    StructField("USER_ID", StringType(), True),
    StructField("LAST_UPDT_DT_SK", StringType(), True)
])
df_DefaultUNK = spark.createDataFrame(
    [(0, "UNK", "UNK", "UNK", "UNK", "UNK", "UNK", "UNK")],
    schema_Default
)
df_DefaultNA = spark.createDataFrame(
    [(1, "NA", "NA", "NA", "NA", "NA", "NA", "NA")],
    schema_Default
)

# Link PKey
df_PKey = df_enriched.select(
    col("EXP_SVC_DLVRY_SK").alias("EXP_SVC_DLVRY_SK"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("EXP_SVC_DLVRY_CD").alias("EXP_SVC_DLVRY_CD"),
    col("CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("EXP_SVC_DLVRY_NM").alias("EXP_SVC_DLVRY_NM"),
    col("USER_ID").alias("USER_ID"),
    col("LAST_UPDT_DT_SK").alias("LAST_UPDT_DT_SK")
)

# Collector (round-robin union) of DefaultNA, DefaultUNK, PKey
df_Collector = df_DefaultNA.unionByName(df_DefaultUNK).unionByName(df_PKey)

# Final select with column order and rpad on char columns
df_Collector_final = df_Collector.select(
    col("EXP_SVC_DLVRY_SK"),
    col("SRC_SYS_CD"),
    col("EXP_SVC_DLVRY_CD"),
    rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("EXP_SVC_DLVRY_NM"),
    col("USER_ID"),
    rpad(col("LAST_UPDT_DT_SK"), 10, " ").alias("LAST_UPDT_DT_SK")
)

# Write final file (Exp_Svc_Dlvry_D_dat)
write_files(
    df_Collector_final,
    f"{adls_path}/load/EXP_SVC_DLVRY_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)