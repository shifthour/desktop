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
# MAGIC JOB NAME:     UwsExpSubCatExtr
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC                   Pulls data from the UserWarehouseSupport to fill the EDW EXP_SUB_CAT_D table
# MAGIC 
# MAGIC INPUTS:
# MAGIC                   EXP_SUB_CAT
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
# MAGIC                    Do nothing with the data            
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                   EXP_SUB_CAT_D.dat
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                   Original Development - 01/10/2006 - Tao Luo


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import types as T
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


UWSOwner = get_widget_value('UWSOwner','')
uws_secret_name = get_widget_value('uws_secret_name','')
CurrRunDate = get_widget_value('CurrRunDate','2006-01-17')

jdbc_url_uws, jdbc_props_uws = get_db_config(uws_secret_name)

df_hf_exp_sub_cat_lookup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_uws)
    .options(**jdbc_props_uws)
    .option("query", "SELECT SRC_SYS_CD, EXP_SUB_CAT_CD, CRT_RUN_CYC_EXCTN_DT_SK, EXP_SUB_CAT_SK FROM dummy_hf_exp_sub_cat")
    .load()
)

df_hf_exp_svc_cat_lookup = spark.read.parquet("hf_exp_svc_cat.parquet")

extract_query_UWS = f"SELECT EXP_SUB_CAT.EXP_SUB_CAT_CD, EXP_SUB_CAT.EXP_SUB_CAT_NM, EXP_SUB_CAT.EXP_SVC_CAT_CD, EXP_SUB_CAT.USER_ID, EXP_SUB_CAT.LAST_UPDT_DT_SK FROM {UWSOwner}.EXP_SUB_CAT EXP_SUB_CAT"
df_UWS_Extract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_uws)
    .options(**jdbc_props_uws)
    .option("query", extract_query_UWS)
    .load()
)

df_Strip = df_UWS_Extract.select(
    trim("EXP_SUB_CAT_CD").alias("EXP_SUB_CAT_CD"),
    trim("EXP_SUB_CAT_NM").alias("EXP_SUB_CAT_NM"),
    trim("EXP_SVC_CAT_CD").alias("EXP_SVC_CAT_CD"),
    trim("USER_ID").alias("USER_ID"),
    trim("LAST_UPDT_DT_SK").alias("LAST_UPDT_DT_SK")
)

df_BusinessRules = df_Strip.withColumn("SRC_SYS_CD", F.lit("UWS")).select(
    "SRC_SYS_CD",
    "EXP_SUB_CAT_CD",
    "EXP_SUB_CAT_NM",
    "EXP_SVC_CAT_CD",
    "USER_ID",
    "LAST_UPDT_DT_SK"
)

df_PrimaryKey_temp = df_BusinessRules.alias("BusinessRulesApplied").join(
    df_hf_exp_sub_cat_lookup.alias("lookup_exp_sub_cat"),
    (
        F.col("BusinessRulesApplied.SRC_SYS_CD") == F.col("lookup_exp_sub_cat.SRC_SYS_CD")
    )
    & (
        F.col("BusinessRulesApplied.EXP_SUB_CAT_CD") == F.col("lookup_exp_sub_cat.EXP_SUB_CAT_CD")
    ),
    "left"
).join(
    df_hf_exp_svc_cat_lookup.alias("lookup_exp_svc_cat_sk"),
    (
        F.col("BusinessRulesApplied.SRC_SYS_CD") == F.col("lookup_exp_svc_cat_sk.SRC_SYS_CD")
    )
    & (
        F.col("BusinessRulesApplied.EXP_SVC_CAT_CD") == F.col("lookup_exp_svc_cat_sk.EXP_SVC_CAT_CD")
    ),
    "left"
).withColumn(
    "svNewCrtRunCycExtcnSk",
    F.when(F.col("lookup_exp_sub_cat.CRT_RUN_CYC_EXCTN_DT_SK").isNull(), F.lit(CurrRunDate)).otherwise(F.col("lookup_exp_sub_cat.CRT_RUN_CYC_EXCTN_DT_SK"))
).withColumn(
    "temp_EXP_SUB_CAT_SK",
    F.when(F.col("lookup_exp_sub_cat.EXP_SUB_CAT_SK").isNull(), F.lit(None).cast(T.IntegerType())).otherwise(F.col("lookup_exp_sub_cat.EXP_SUB_CAT_SK"))
).withColumnRenamed(
    "lookup_exp_svc_cat_sk.EXP_SVC_CAT_SK",
    "lkp_EXP_SVC_CAT_SK"
)

df_enriched = df_PrimaryKey_temp
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"temp_EXP_SUB_CAT_SK",<schema>,<secret_name>)

df_PrimaryKey_load = df_enriched.select(
    F.col("temp_EXP_SUB_CAT_SK").alias("EXP_SUB_CAT_SK"),
    F.col("BusinessRulesApplied.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("BusinessRulesApplied.EXP_SUB_CAT_CD").alias("EXP_SUB_CAT_CD"),
    F.col("svNewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(CurrRunDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("lkp_EXP_SVC_CAT_SK").alias("EXP_SVC_CAT_SK"),
    F.col("BusinessRulesApplied.EXP_SUB_CAT_NM").alias("EXP_SUB_CAT_NM"),
    F.col("BusinessRulesApplied.USER_ID").alias("USER_ID"),
    F.col("BusinessRulesApplied.LAST_UPDT_DT_SK").alias("LAST_UPDT_DT_SK")
)

df_PrimaryKey_load_final = df_PrimaryKey_load.select(
    F.col("EXP_SUB_CAT_SK"),
    F.col("SRC_SYS_CD"),
    F.col("EXP_SUB_CAT_CD"),
    F.rpad("CRT_RUN_CYC_EXCTN_DT_SK", 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("EXP_SVC_CAT_SK"),
    F.col("EXP_SUB_CAT_NM"),
    F.col("USER_ID"),
    F.rpad("LAST_UPDT_DT_SK", 10, " ").alias("LAST_UPDT_DT_SK")
)

write_files(
    df_PrimaryKey_load_final,
    f"{adls_path}/load/EXP_SUB_CAT_D.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)

df_hf_exp_sub_cat_write = df_enriched.filter(
    F.col("lookup_exp_sub_cat.EXP_SUB_CAT_SK").isNull()
).select(
    F.col("BusinessRulesApplied.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("BusinessRulesApplied.EXP_SUB_CAT_CD").alias("EXP_SUB_CAT_CD"),
    F.col("svNewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("temp_EXP_SUB_CAT_SK").alias("EXP_SUB_CAT_SK")
)

execute_dml("DROP TABLE IF EXISTS STAGING.UwsExpSubCatExtr_hf_exp_sub_cat_write_temp", jdbc_url_uws, jdbc_props_uws)

df_hf_exp_sub_cat_write.write.format("jdbc") \
    .option("url", jdbc_url_uws) \
    .options(**jdbc_props_uws) \
    .option("dbtable", "STAGING.UwsExpSubCatExtr_hf_exp_sub_cat_write_temp") \
    .mode("append") \
    .save()

merge_sql = """
MERGE dummy_hf_exp_sub_cat AS T
USING STAGING.UwsExpSubCatExtr_hf_exp_sub_cat_write_temp AS S
ON T.SRC_SYS_CD = S.SRC_SYS_CD
AND T.EXP_SUB_CAT_CD = S.EXP_SUB_CAT_CD
WHEN NOT MATCHED THEN
INSERT (SRC_SYS_CD, EXP_SUB_CAT_CD, CRT_RUN_CYC_EXCTN_DT_SK, EXP_SUB_CAT_SK)
VALUES (S.SRC_SYS_CD, S.EXP_SUB_CAT_CD, S.CRT_RUN_CYC_EXCTN_DT_SK, S.EXP_SUB_CAT_SK);
"""

execute_dml(merge_sql, jdbc_url_uws, jdbc_props_uws)