# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:    Reads IDS Member Eligibility Audit table checking for fields set to zero and rekeys them
# MAGIC       
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Sharon Andrew       2007-03-23        3265                      Initial program                                                                               devlIDS30                     Brent Leland               2007-04-05
# MAGIC 
# MAGIC Manasa Andru         2015-03-31        TFS - 9741            Modified the extract SQL to extract all the records                       IntegrateNewDevl          Kalyan Neelam            2015-04-02
# MAGIC                                                                                             where the MBR_SK = 0

# MAGIC Set all foreign surragote keys
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


IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
RunID = get_widget_value("RunID","")
RunCycle = get_widget_value("RunCycle","100")

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = f"""
SELECT
  MBR_ELIG_AUDIT.MBR_ELIG_AUDIT_SK,
  MBR_ELIG_AUDIT.SRC_SYS_CD_SK,
  MBR_ELIG_AUDIT.MBR_ELIG_AUDIT_ROW_ID,
  MBR_ELIG_AUDIT.CRT_RUN_CYC_EXCTN_SK,
  MBR_ELIG_AUDIT.LAST_UPDT_RUN_CYC_EXCTN_SK,
  MBR_ELIG_AUDIT.EXCD_SK,
  MBR_ELIG_AUDIT.MBR_SK,
  MBR_ELIG_AUDIT.SRC_SYS_CRT_USER_SK,
  MBR_ELIG_AUDIT.MBR_ELIG_AUDIT_ACTN_CD_SK,
  MBR_ELIG_AUDIT.MBR_ELIG_CLS_PROD_CAT_CD_SK,
  MBR_ELIG_AUDIT.MBR_ELIG_TYP_CD_SK,
  MBR_ELIG_AUDIT.VOID_IN,
  MBR_ELIG_AUDIT.EFF_DT_SK,
  MBR_ELIG_AUDIT.SRC_SYS_CRT_DT_SK,
  MBR_ELIG_AUDIT.MBR_UNIQ_KEY,
  MBR_ELIG_AUDIT.CLS_PLN_SK
FROM {IDSOwner}.MBR_ELIG_AUDIT MBR_ELIG_AUDIT
WHERE MBR_ELIG_AUDIT.MBR_SK = 0
"""

df_MBR_ELIG_AUDIT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_key = df_MBR_ELIG_AUDIT.withColumn(
    "svMbrSk",
    GetFkeyMbr(
        "FACETS",
        col("MBR_ELIG_AUDIT_SK"),
        col("MBR_UNIQ_KEY"),
        lit("X")
    )
)

df_MbrEligAudit = df_key.select(
    col("MBR_ELIG_AUDIT_SK").alias("MBR_ELIG_AUDIT_SK"),
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("MBR_ELIG_AUDIT_ROW_ID").alias("MBR_ELIG_AUDIT_ROW_ID"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("EXCD_SK").alias("EXCD_SK"),
    col("svMbrSk").alias("MBR_SK"),
    col("SRC_SYS_CRT_USER_SK").alias("SRC_SYS_CRT_USER_SK"),
    col("MBR_ELIG_AUDIT_ACTN_CD_SK").alias("MBR_ELIG_AUDIT_ACTN_CD_SK"),
    col("MBR_ELIG_CLS_PROD_CAT_CD_SK").alias("MBR_ELIG_CLS_PROD_CAT_CD_SK"),
    col("MBR_ELIG_TYP_CD_SK").alias("MBR_ELIG_TYP_CD_SK"),
    rpad(col("VOID_IN"), 1, " ").alias("VOID_IN"),
    rpad(col("EFF_DT_SK"), 10, " ").alias("EFF_DT_SK"),
    rpad(col("SRC_SYS_CRT_DT_SK"), 10, " ").alias("SRC_SYS_CRT_DT_SK"),
    col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("CLS_PLN_SK").alias("CLS_PLN_SK")
)

write_files(
    df_MbrEligAudit,
    f"{adls_path}/load/MBR_ELIG_AUDIT.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)