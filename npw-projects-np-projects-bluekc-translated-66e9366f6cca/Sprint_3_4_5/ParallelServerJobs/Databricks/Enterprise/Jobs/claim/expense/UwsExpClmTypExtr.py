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
# MAGIC JOB NAME:     UwsExpClmTypExtr
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC                   Pulls data from the UserWarehouseSupport to fill the EDW EXP_CLM_TYP_D table
# MAGIC 
# MAGIC INPUTS:
# MAGIC                   EXP_CLM_TYP
# MAGIC                   
# MAGIC                  
# MAGIC HASH FILES:
# MAGIC                   hf_exp_clm_typ
# MAGIC 
# MAGIC TRANSFORMS:   
# MAGIC                   Lookup to hf_exp_clm_typ to retrieve EXP_CLM_TYP_SK and CRT_RUN_CYC_EXCTN_DT_SK
# MAGIC                   TRIM
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                    Do nothing with the data                  
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                   EXP_CLM_TYP_D.dat
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
from pyspark.sql.functions import col, lit, when, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------

uws_secret_name = get_widget_value('uws_secret_name','')
UWSOwner = get_widget_value('UWSOwner','')
CurrRunDate = get_widget_value('CurrRunDate','2006-02-16')
jdbc_url, jdbc_props = get_db_config(uws_secret_name)

df_hf_exp_clm_typ_lookup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", "SELECT SRC_SYS_CD, EXP_CLM_TYP_CD, CRT_RUN_CYC_EXCTN_DT_SK, EXP_CLM_TYP_SK FROM dummy_hf_exp_clm_typ")
    .load()
)

extract_query = f"SELECT EXP_CLM_TYP.EXP_CLM_TYP_CD, EXP_CLM_TYP.EXP_CLM_TYP_NM, EXP_CLM_TYP.USER_ID, EXP_CLM_TYP.LAST_UPDT_DT_SK FROM {UWSOwner}.EXP_CLM_TYP EXP_CLM_TYP"
df_UWS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_Strip = (
    df_UWS
    .withColumn("EXP_CLM_TYP_CD", trim("EXP_CLM_TYP_CD"))
    .withColumn("EXP_CLM_TYP_NM", trim("EXP_CLM_TYP_NM"))
    .withColumn("USER_ID", trim("USER_ID"))
    .withColumn("LAST_UPDT_DT_SK", trim("LAST_UPDT_DT_SK"))
)

df_BusinessRules = df_Strip.withColumn("SRC_SYS_CD", lit("UWS"))

df_PrimaryKey = (
    df_BusinessRules.alias("BusinessRulesApplied")
    .join(
        df_hf_exp_clm_typ_lookup.alias("lookup_exp_clm_typ"),
        (
            col("BusinessRulesApplied.SRC_SYS_CD") == col("lookup_exp_clm_typ.SRC_SYS_CD")
        )
        & (
            col("BusinessRulesApplied.EXP_CLM_TYP_CD") == col("lookup_exp_clm_typ.EXP_CLM_TYP_CD")
        ),
        "left"
    )
)

df_enriched = (
    df_PrimaryKey
    .withColumn(
        "EXP_CLM_TYP_SK",
        when(col("lookup_exp_clm_typ.SRC_SYS_CD").isNull(), lit(None)).otherwise(col("lookup_exp_clm_typ.EXP_CLM_TYP_SK"))
    )
    .withColumn(
        "CRT_RUN_CYC_EXCTN_DT_SK",
        when(col("lookup_exp_clm_typ.SRC_SYS_CD").isNull(), lit(CurrRunDate)).otherwise(col("lookup_exp_clm_typ.CRT_RUN_CYC_EXCTN_DT_SK"))
    )
)

df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"EXP_CLM_TYP_SK",<schema>,<secret_name>)

df_load = df_enriched.select(
    col("EXP_CLM_TYP_SK"),
    rpad(col("SRC_SYS_CD"),<...>," ").alias("SRC_SYS_CD"),
    rpad(col("EXP_CLM_TYP_CD"),<...>," ").alias("EXP_CLM_TYP_CD"),
    rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"),10," ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad(lit(CurrRunDate),10," ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    rpad(col("EXP_CLM_TYP_NM"),<...>," ").alias("EXP_CLM_TYP_NM"),
    rpad(col("USER_ID"),<...>," ").alias("USER_ID"),
    rpad(col("LAST_UPDT_DT_SK"),10," ").alias("LAST_UPDT_DT_SK")
)

write_files(
    df_load,
    f"{adls_path}/load/EXP_CLM_TYP_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

df_hf_exp_clm_typ_write = (
    df_enriched
    .filter(col("lookup_exp_clm_typ.SRC_SYS_CD").isNull())
    .select(
        rpad(col("SRC_SYS_CD"),<...>," ").alias("SRC_SYS_CD"),
        rpad(col("EXP_CLM_TYP_CD"),<...>," ").alias("EXP_CLM_TYP_CD"),
        rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"),10," ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        col("EXP_CLM_TYP_SK")
    )
)

drop_temp_sql = "DROP TABLE IF EXISTS STAGING.UwsExpClmTypExtr_hf_exp_clm_typ_write_temp"
execute_dml(drop_temp_sql, jdbc_url, jdbc_props)

df_hf_exp_clm_typ_write.write.format("jdbc")\
    .option("url", jdbc_url)\
    .options(**jdbc_props)\
    .option("dbtable", "STAGING.UwsExpClmTypExtr_hf_exp_clm_typ_write_temp")\
    .mode("overwrite")\
    .save()

merge_sql = """
MERGE INTO dummy_hf_exp_clm_typ T
USING STAGING.UwsExpClmTypExtr_hf_exp_clm_typ_write_temp S
ON (T.SRC_SYS_CD = S.SRC_SYS_CD AND T.EXP_CLM_TYP_CD = S.EXP_CLM_TYP_CD)
WHEN NOT MATCHED THEN
  INSERT (SRC_SYS_CD, EXP_CLM_TYP_CD, CRT_RUN_CYC_EXCTN_DT_SK, EXP_CLM_TYP_SK)
  VALUES (S.SRC_SYS_CD, S.EXP_CLM_TYP_CD, S.CRT_RUN_CYC_EXCTN_DT_SK, S.EXP_CLM_TYP_SK);
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)