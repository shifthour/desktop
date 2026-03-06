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
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     UwsExpSvcCatExtr
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC                   Pulls data from the UserWarehouseSupport to fill the EDW EXP_SVC_CAT_D table
# MAGIC 
# MAGIC INPUTS:
# MAGIC                   EXP_SVC_CAT
# MAGIC                   
# MAGIC HASH FILES:
# MAGIC                   hf_exp_sub_cat
# MAGIC                   hf_exp_svc_cat
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                   Lookup to hf_exp_sub_cat to retrieve EXP_SUB_CAT_SK and CRT_RUN_CYC_EXCTN_DT_SK
# MAGIC                   Lookup to hf_exp_svc_cat to retrieve EXP_SVC_CAT_SK
# MAGIC                   TRIM
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   Do nothing with the data
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                   EXP_SVC_CAT_D.dat
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                   Original Development - 01/13/2006 - Tao Luo


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
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


# Receive job parameters
UWSOwner = get_widget_value('UWSOwner','')
uws_secret_name = get_widget_value('uws_secret_name','')
CurrRunDate = get_widget_value('CurrRunDate','2006-01-17')

# Prepare JDBC connection for "dummy_hf_exp_svc_cat" (scenario B hashed file)
jdbc_url, jdbc_props = get_db_config(uws_secret_name)

# Read dummy table for hf_exp_svc_cat_lookup (scenario B read)
df_hf_exp_svc_cat_lookup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"SELECT SRC_SYS_CD, EXP_SVC_CAT_CD, CRT_RUN_CYC_EXCTN_DT_SK, EXP_SUB_CAT_SK FROM {UWSOwner}.dummy_hf_exp_svc_cat")
    .load()
)

# Read hf_exp_clm_typ_lookup as parquet (scenario C)
df_hf_exp_clm_typ_lookup = spark.read.parquet(f"{adls_path}/hf_exp_clm_typ.parquet")

# Reuse same JDBC for "UWS" stage
# Translate the ODBC query to read full table (since WHERE ... = ? cannot be used directly)
df_UWS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"SELECT EXP_SVC_CAT.EXP_SVC_CAT_CD, EXP_SVC_CAT.EXP_SVC_CAT_NM, EXP_SVC_CAT.EXP_CLM_TYP_CD, EXP_SVC_CAT.USER_ID, EXP_SVC_CAT.LAST_UPDT_DT_SK FROM {UWSOwner}.EXP_SVC_CAT EXP_SVC_CAT")
    .load()
)

# Strip transformer
df_Strip = (
    df_UWS
    .withColumn("EXP_SVC_CAT_CD", trim(F.col("EXP_SVC_CAT_CD")))
    .withColumn("EXP_SVC_CAT_NM", trim(F.col("EXP_SVC_CAT_NM")))
    .withColumn("EXP_CLM_TYP_CD", trim(F.col("EXP_CLM_TYP_CD")))
    .withColumn("USER_ID", trim(F.col("USER_ID")))
    .withColumn("LAST_UPDT_DT_SK", trim(F.col("LAST_UPDT_DT_SK")))
)

# BusinessRules transformer
df_BusinessRules = df_Strip.select(
    F.lit("UWS").alias("SRC_SYS_CD"),
    F.col("EXP_SVC_CAT_CD").alias("EXP_SVC_CAT_CD"),
    F.col("EXP_SVC_CAT_NM").alias("EXP_SVC_CAT_NM"),
    F.col("EXP_CLM_TYP_CD").alias("EXP_CLM_TYP_CD"),
    F.col("USER_ID").alias("USER_ID"),
    F.col("LAST_UPDT_DT_SK").alias("LAST_UPDT_DT_SK")
)

# PrimaryKey transformer: join lookups
df_PrimaryKey = (
    df_BusinessRules.alias("BusinessRulesApplied")
    .join(
        df_hf_exp_svc_cat_lookup.alias("lookup_exp_sub_cat"),
        (
            (F.col("BusinessRulesApplied.SRC_SYS_CD") == F.col("lookup_exp_sub_cat.SRC_SYS_CD"))
            & (F.col("BusinessRulesApplied.EXP_SVC_CAT_CD") == F.col("lookup_exp_sub_cat.EXP_SVC_CAT_CD"))
        ),
        "left"
    )
    .join(
        df_hf_exp_clm_typ_lookup.alias("lookup_exp_clm_typ"),
        (
            (F.col("BusinessRulesApplied.SRC_SYS_CD") == F.col("lookup_exp_clm_typ.SRC_SYS_CD"))
            & (F.col("BusinessRulesApplied.EXP_CLM_TYP_CD") == F.col("lookup_exp_clm_typ.EXP_CLM_TYP_CD"))
        ),
        "left"
    )
)

# Derive columns based on stage variables and expressions
df_PrimaryKey2 = (
    df_PrimaryKey
    .withColumn(
        "CRT_RUN_CYC_EXCTN_DT_SK",
        F.when(F.col("lookup_exp_sub_cat.EXP_SUB_CAT_SK").isNull(), F.lit(CurrRunDate))
         .otherwise(F.col("lookup_exp_sub_cat.CRT_RUN_CYC_EXCTN_DT_SK"))
    )
    .withColumn("EXP_SVC_CAT_SK", F.col("lookup_exp_sub_cat.EXP_SUB_CAT_SK"))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.lit(CurrRunDate))
    .withColumn("EXP_CLM_TYP_SK", F.col("lookup_exp_clm_typ.EXP_CLM_TYP_SK"))
    .withColumn("EXP_SVC_CAT_NM", F.col("BusinessRulesApplied.EXP_SVC_CAT_NM"))
    .withColumn("USER_ID", F.col("BusinessRulesApplied.USER_ID"))
    .withColumn("LAST_UPDT_DT_SK", F.col("BusinessRulesApplied.LAST_UPDT_DT_SK"))
    .withColumn("SRC_SYS_CD", F.col("BusinessRulesApplied.SRC_SYS_CD"))
    .withColumn("EXP_SVC_CAT_CD", F.col("BusinessRulesApplied.EXP_SVC_CAT_CD"))
)

# Apply SurrogateKeyGen to fill EXP_SVC_CAT_SK where null
df_enriched = df_PrimaryKey2
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"EXP_SVC_CAT_SK",<schema>,<secret_name>)

# "Load" link output: final columns in correct order
df_load = df_enriched.select(
    "EXP_SVC_CAT_SK",
    "SRC_SYS_CD",
    "EXP_SVC_CAT_CD",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "EXP_CLM_TYP_SK",
    "EXP_SVC_CAT_NM",
    "USER_ID",
    "LAST_UPDT_DT_SK"
)

# Apply rpad for char columns in the final output
df_load_final = df_load.select(
    F.col("EXP_SVC_CAT_SK").alias("EXP_SVC_CAT_SK"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("EXP_SVC_CAT_CD").alias("EXP_SVC_CAT_CD"),
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("EXP_CLM_TYP_SK").alias("EXP_CLM_TYP_SK"),
    F.col("EXP_SVC_CAT_NM").alias("EXP_SVC_CAT_NM"),
    F.col("USER_ID").alias("USER_ID"),
    F.rpad(F.col("LAST_UPDT_DT_SK"), 10, " ").alias("LAST_UPDT_DT_SK")
)

# Write to EXP_SVC_CAT_D.dat
write_files(
    df_load_final,
    f"{adls_path}/load/EXP_SVC_CAT_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# "write_exp_sub_cat" link => scenario B insert
# Filter rows where lookup_exp_sub_cat was not found
df_write_exp_sub_cat = df_enriched.filter(
    F.col("lookup_exp_sub_cat.EXP_SUB_CAT_SK").isNull()
).select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("EXP_SVC_CAT_CD").alias("EXP_SVC_CAT_CD"),
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("EXP_SVC_CAT_SK").alias("EXP_SUB_CAT_SK")
)

# Create staging table and merge into dummy_hf_exp_svc_cat
spark.sql(f"DROP TABLE IF EXISTS STAGING.UwsExpSvcCatExtr_hf_exp_svc_cat_write_temp")

df_write_exp_sub_cat.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.UwsExpSvcCatExtr_hf_exp_svc_cat_write_temp") \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO {UWSOwner}.dummy_hf_exp_svc_cat AS T
USING STAGING.UwsExpSvcCatExtr_hf_exp_svc_cat_write_temp AS S
ON T.SRC_SYS_CD = S.SRC_SYS_CD AND T.EXP_SVC_CAT_CD = S.EXP_SVC_CAT_CD
WHEN NOT MATCHED THEN
  INSERT (SRC_SYS_CD, EXP_SVC_CAT_CD, CRT_RUN_CYC_EXCTN_DT_SK, EXP_SUB_CAT_SK)
  VALUES (S.SRC_SYS_CD, S.EXP_SVC_CAT_CD, S.CRT_RUN_CYC_EXCTN_DT_SK, S.EXP_SUB_CAT_SK);
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)