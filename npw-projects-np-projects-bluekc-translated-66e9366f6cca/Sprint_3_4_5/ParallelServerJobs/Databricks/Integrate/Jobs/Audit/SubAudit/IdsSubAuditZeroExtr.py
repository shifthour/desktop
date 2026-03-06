# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:    Reads IDS Subscriber Audit table checking for fields set to zero and rekeys them
# MAGIC       
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Sharon Andrew       2007-03-23        3265                      Initial program                                                                               devlIDS30                      Brent Leland              2007-04-05
# MAGIC 
# MAGIC Manasa Andru         2015-03-31        TFS - 9741            Modified the extract SQL to extract all the records                       IntegrateNewDevl           Kalyan Neelam            2015-04-02
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


RunID = get_widget_value('RunID','')
RunCycle = get_widget_value('RunCycle','100')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = (
    "SELECT "
    "SUB_AUDIT.SUB_AUDIT_SK, "
    "SUB_AUDIT.SRC_SYS_CD_SK, "
    "SUB_AUDIT.SUB_AUDIT_ROW_ID, "
    "SUB_AUDIT.CRT_RUN_CYC_EXCTN_SK, "
    "SUB_AUDIT.LAST_UPDT_RUN_CYC_EXCTN_SK, "
    "SUB_AUDIT.SRC_SYS_CRT_USER_SK, "
    "SUB_AUDIT.SUB_SK, "
    "SUB_AUDIT.SUB_AUDIT_ACTN_CD_SK, "
    "SUB_AUDIT.SUB_FMLY_CNTR_CD_SK, "
    "SUB_AUDIT.SCRD_IN, "
    "SUB_AUDIT.ORIG_EFF_DT_SK, "
    "SUB_AUDIT.RETR_DT_SK, "
    "SUB_AUDIT.SRC_SYS_CRT_DT_SK, "
    "SUB_AUDIT.SUB_UNIQ_KEY, "
    "SUB_AUDIT.SUB_FIRST_NM, "
    "SUB_AUDIT.SUB_MIDINIT, "
    "SUB_AUDIT.SUB_LAST_NM, "
    "SUB_AUDIT.SUB_AUDIT_HIRE_DT_TX, "
    "SUB_AUDIT.SUB_AUDIT_RCVD_DT_TX, "
    "SUB_AUDIT.SUB_AUDIT_STTUS_TX "
    f"FROM {IDSOwner}.SUB_AUDIT SUB_AUDIT "
    "WHERE SUB_AUDIT.SUB_SK = 0"
)
df_SUB_AUDIT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_Key = df_SUB_AUDIT.select(
    col("SUB_AUDIT_SK"),
    col("SRC_SYS_CD_SK"),
    col("SUB_AUDIT_ROW_ID"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("SRC_SYS_CRT_USER_SK"),
    GetFkeySub(lit("FACETS"), col("SUB_AUDIT_SK"), col("SUB_UNIQ_KEY"), lit("X")).alias("SUB_SK"),
    col("SUB_AUDIT_ACTN_CD_SK"),
    col("SUB_FMLY_CNTR_CD_SK"),
    col("SCRD_IN"),
    col("ORIG_EFF_DT_SK"),
    col("RETR_DT_SK"),
    col("SRC_SYS_CRT_DT_SK"),
    col("SUB_UNIQ_KEY"),
    col("SUB_FIRST_NM"),
    col("SUB_MIDINIT"),
    col("SUB_LAST_NM"),
    col("SUB_AUDIT_HIRE_DT_TX"),
    col("SUB_AUDIT_RCVD_DT_TX"),
    col("SUB_AUDIT_STTUS_TX")
)

df_SUB = df_Key.select(
    col("SUB_AUDIT_SK"),
    col("SRC_SYS_CD_SK"),
    col("SUB_AUDIT_ROW_ID"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("SRC_SYS_CRT_USER_SK"),
    col("SUB_SK"),
    col("SUB_AUDIT_ACTN_CD_SK"),
    col("SUB_FMLY_CNTR_CD_SK"),
    rpad(col("SCRD_IN"), 1, " ").alias("SCRD_IN"),
    rpad(col("ORIG_EFF_DT_SK"), 10, " ").alias("ORIG_EFF_DT_SK"),
    rpad(col("RETR_DT_SK"), 10, " ").alias("RETR_DT_SK"),
    rpad(col("SRC_SYS_CRT_DT_SK"), 10, " ").alias("SRC_SYS_CRT_DT_SK"),
    col("SUB_UNIQ_KEY"),
    col("SUB_FIRST_NM"),
    rpad(col("SUB_MIDINIT"), 1, " ").alias("SUB_MIDINIT"),
    col("SUB_LAST_NM"),
    col("SUB_AUDIT_HIRE_DT_TX"),
    col("SUB_AUDIT_RCVD_DT_TX"),
    col("SUB_AUDIT_STTUS_TX")
)

write_files(
    df_SUB,
    f"{adls_path}/load/SUB_AUDIT.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)