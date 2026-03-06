# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:    Reads IDS Subscriber Address Audit table checking for fields set to zero and rekeys them
# MAGIC       
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Sharon Andrew       2007-03-23        3265                      Initial program                                                                               devlIDS30                      Brent Leland             2007-04-05
# MAGIC 
# MAGIC Manasa Andru         2015-03-31        TFS - 9741            Modified the extract SQL to extract all the records                       IntegrateNewDevl          Kalyan Neelam             2015-04-02
# MAGIC                                                                                             where the SUB_SK = 0

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


RunID = get_widget_value("RunID","")
RunCycle = get_widget_value("RunCycle","100")
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query_SUB_ADDR_AUDIT = f"SELECT SUB_ADDR_AUDIT.SUB_ADDR_AUDIT_SK, SUB_ADDR_AUDIT.SRC_SYS_CD_SK, SUB_ADDR_AUDIT.SUB_ADDR_AUDIT_ROW_ID, SUB_ADDR_AUDIT.CRT_RUN_CYC_EXCTN_SK, SUB_ADDR_AUDIT.LAST_UPDT_RUN_CYC_EXCTN_SK, SUB_ADDR_AUDIT.SRC_SYS_CRT_USER_SK, SUB_ADDR_AUDIT.SUB_SK, SUB_ADDR_AUDIT.SUB_ADDR_AUDIT_ACTN_CD_SK, SUB_ADDR_AUDIT.SUB_ADDR_CD_SK, SUB_ADDR_AUDIT.SRC_SYS_CRT_DT_SK, SUB_ADDR_AUDIT.SUB_UNIQ_KEY, SUB_ADDR_AUDIT.ADDR_LN_1, SUB_ADDR_AUDIT.ADDR_LN_2, SUB_ADDR_AUDIT.ADDR_LN_3, SUB_ADDR_AUDIT.CITY_NM, SUB_ADDR_AUDIT.SUB_ADDR_ST_CD_SK, SUB_ADDR_AUDIT.POSTAL_CD, SUB_ADDR_AUDIT.CNTY_NM, SUB_ADDR_AUDIT.SUB_ADDR_CTRY_CD_SK, SUB_ADDR_AUDIT.PHN_NO FROM {IDSOwner}.SUB_ADDR_AUDIT SUB_ADDR_AUDIT WHERE SUB_ADDR_AUDIT.SUB_SK = 0"
df_SUB_ADDR_AUDIT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_SUB_ADDR_AUDIT)
    .load()
)

df_Key = df_SUB_ADDR_AUDIT.select(
    col("SUB_ADDR_AUDIT_SK").alias("SUB_ADDR_AUDIT_SK"),
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("SUB_ADDR_AUDIT_ROW_ID").alias("SUB_ADDR_AUDIT_ROW_ID"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("SRC_SYS_CRT_USER_SK").alias("SRC_SYS_CRT_USER_SK"),
    GetFkeySub("FACETS", col("SUB_ADDR_AUDIT_SK"), col("SUB_UNIQ_KEY"), lit("X")).alias("SUB_SK"),
    col("SUB_ADDR_AUDIT_ACTN_CD_SK").alias("SUB_ADDR_AUDIT_ACTN_CD_SK"),
    col("SUB_ADDR_CD_SK").alias("SUB_ADDR_CD_SK"),
    col("SRC_SYS_CRT_DT_SK").alias("SRC_SYS_CRT_DT_SK"),
    col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    col("ADDR_LN_1").alias("ADDR_LN_1"),
    col("ADDR_LN_2").alias("ADDR_LN_2"),
    col("ADDR_LN_3").alias("ADDR_LN_3"),
    col("CITY_NM").alias("CITY_NM"),
    col("SUB_ADDR_ST_CD_SK").alias("SUB_ADDR_ST_CD_SK"),
    col("POSTAL_CD").alias("POSTAL_CD"),
    col("CNTY_NM").alias("CNTY_NM"),
    col("SUB_ADDR_CTRY_CD_SK").alias("SUB_ADDR_CTRY_CD_SK"),
    col("PHN_NO").alias("PHN_NO")
)

df_ADDR_AUDIT = df_Key.select(
    col("SUB_ADDR_AUDIT_SK"),
    col("SRC_SYS_CD_SK"),
    col("SUB_ADDR_AUDIT_ROW_ID"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("SRC_SYS_CRT_USER_SK"),
    col("SUB_SK"),
    col("SUB_ADDR_AUDIT_ACTN_CD_SK"),
    col("SUB_ADDR_CD_SK"),
    rpad(col("SRC_SYS_CRT_DT_SK"), 10, " ").alias("SRC_SYS_CRT_DT_SK"),
    col("SUB_UNIQ_KEY"),
    col("ADDR_LN_1"),
    col("ADDR_LN_2"),
    col("ADDR_LN_3"),
    col("CITY_NM"),
    col("SUB_ADDR_ST_CD_SK"),
    col("POSTAL_CD"),
    col("CNTY_NM"),
    col("SUB_ADDR_CTRY_CD_SK"),
    col("PHN_NO")
)

write_files(
    df_ADDR_AUDIT,
    f"{adls_path}/load/SUB_ADDR_AUDIT.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)