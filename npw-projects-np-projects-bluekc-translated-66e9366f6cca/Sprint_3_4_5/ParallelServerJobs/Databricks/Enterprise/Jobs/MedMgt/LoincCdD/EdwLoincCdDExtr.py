# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 02/01/08 12:08:08 Batch  14642_43698 INIT bckcetl edw10 dsadm dsadm
# MAGIC ^1_1 08/31/07 14:39:46 Batch  14488_52789 INIT bckcetl edw10 dsadm dsadm
# MAGIC ^1_1 04/10/07 07:38:17 Batch  14345_27504 INIT bckcetl edw10 dsadm dsadm
# MAGIC ^1_2 06/15/06 11:46:45 Batch  14046_42416 PROMOTE bckcetl edw10 dsadm j. Mahaffey for Steph Goddard
# MAGIC ^1_2 06/15/06 11:44:35 Batch  14046_42285 INIT bckcett testEDW10 dsadm J. Mahaffey for Steph Goddard
# MAGIC ^1_1 06/15/06 11:32:40 Batch  14046_41574 INIT bckcett testEDW10 dsadm j. Mahaffey for Steph Goddard
# MAGIC ^1_2 06/04/06 13:46:09 Batch  14035_49576 PROMOTE bckcett testEDW10 u03651 steffy
# MAGIC ^1_2 06/04/06 13:45:02 Batch  14035_49507 INIT bckcett devlEDW10 u03651 steffy
# MAGIC ^1_1 05/30/06 11:18:28 Batch  14030_40711 INIT bckcett devlEDW10 u03651 steffy
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     EdwLoincCdDExtr
# MAGIC 
# MAGIC DESCRIPTION:  Extracts from IDS LOINC_CD to flatfile LOINC_CD_D.dat
# MAGIC       
# MAGIC 
# MAGIC INPUTS:
# MAGIC The following IDS table is used:
# MAGIC LOINC_CD
# MAGIC   
# MAGIC HASH FILES:  
# MAGIC             none
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                   TRIM all the fields that are extracted or used in lookups
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                     Flatfile: LOINC_CD_D.dat
# MAGIC 
# MAGIC               
# MAGIC MODIFICATIONS:
# MAGIC Date                 Developer                Change Description
# MAGIC ------------------      ----------------------------     --------------------------------------------------------------------------------------------------------
# MAGIC 2006-04-07      Suzanne Saylor          Original Programming.

# MAGIC Extract IDS  Data
# MAGIC Apply business logic.
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
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrentDate = get_widget_value('CurrentDate','')
RunCycle = get_widget_value('RunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
df_IDS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"SELECT loi.LOINC_CD_SK as LOINC_CD_SK,loi.LOINC_CD as LOINC_CD,loi.CRT_RUN_CYC_EXCTN_SK as CRT_RUN_CYC_EXCTN_SK,loi.LAST_UPDT_RUN_CYC_EXCTN_SK as LAST_UPDT_RUN_CYC_EXCTN_SK,loi.LOINC_CD_REL_NM as LOINC_CD_REL_NM,loi.LOINC_CD_SH_NM as LOINC_CD_SH_NM FROM {IDSOwner}.LOINC_CD loi\n\n{IDSOwner}.LOINC_CD loi")
    .load()
)

df_BusinessRules = df_IDS.select(
    col("LOINC_CD_SK").alias("LOINC_SK"),
    col("LOINC_CD").alias("LOINC_CD"),
    lit(CurrentDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    lit(CurrentDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("LOINC_CD_REL_NM").alias("LOINC_CD_REL_NM"),
    col("LOINC_CD_SH_NM").alias("LOINC_CD_SH_NM"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

df_final = (
    df_BusinessRules
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .select(
        "LOINC_SK",
        "LOINC_CD",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "LOINC_CD_REL_NM",
        "LOINC_CD_SH_NM",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK"
    )
)

write_files(
    df_final,
    f"{adls_path}/load/LOINC_CD_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)