# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:    Reads IDS Member Audit table checking for fields set to zero and rekeys them
# MAGIC       
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Sharon Andrew       2007-03-23        3265                      Initial program                                                                               devlIDS30                      Brent Leland              2007-03-28
# MAGIC 
# MAGIC Manasa Andru         2015-03-31        TFS - 9741            Modified the extract SQL to extract all the records                       IntegrateNewDevl           Kalyan Neelam           2015-04-02
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


RunID = get_widget_value("RunID","")
RunCycle = get_widget_value("RunCycle","100")
IDSOwner = get_widget_value("IDSOwner","$PROJDEF")
ids_secret_name = get_widget_value("ids_secret_name","")

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = (
    "SELECT MBR_AUDIT.MBR_AUDIT_SK,\n"
    "MBR_AUDIT.SRC_SYS_CD_SK,\n"
    "MBR_AUDIT.MBR_AUDIT_ROW_ID,\n"
    "MBR_AUDIT.CRT_RUN_CYC_EXCTN_SK,\n"
    "MBR_AUDIT.LAST_UPDT_RUN_CYC_EXCTN_SK,\n"
    "MBR_AUDIT.MBR_SK,\n"
    "MBR_AUDIT.SRC_SYS_CRT_USER_SK,\n"
    "MBR_AUDIT.MBR_AUDIT_ACTN_CD_SK,\n"
    "MBR_AUDIT.MBR_GNDR_CD_SK,\n"
    "MBR_AUDIT.SCRD_IN,\n"
    "MBR_AUDIT.BRTH_DT_SK,\n"
    "MBR_AUDIT.PREX_COND_EFF_DT_SK,\n"
    "MBR_AUDIT.SRC_SYS_CRT_DT_SK,\n"
    "MBR_AUDIT.MBR_UNIQ_KEY,\n"
    "MBR_AUDIT.FIRST_NM,\n"
    "MBR_AUDIT.MIDINIT,\n"
    "MBR_AUDIT.LAST_NM,\n"
    "MBR_AUDIT.SSN\n"
    f"FROM {IDSOwner}.MBR_AUDIT MBR_AUDIT\n"
    "WHERE MBR_AUDIT.MBR_SK = 0"
)

df_MBR_AUDIT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_Key = df_MBR_AUDIT.withColumn(
    "svMbrSk",
    GetFkeyMbr("FACETS", col("MBR_AUDIT_SK"), col("MBR_UNIQ_KEY"), lit("X"))
)

df_KeyOut = df_Key.select(
    col("MBR_AUDIT_SK").alias("MBR_AUDIT_SK"),
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("MBR_AUDIT_ROW_ID").alias("MBR_AUDIT_ROW_ID"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("svMbrSk").alias("MBR_SK"),
    col("SRC_SYS_CRT_USER_SK").alias("SRC_SYS_CRT_USER_SK"),
    col("MBR_AUDIT_ACTN_CD_SK").alias("MBR_AUDIT_ACTN_CD_SK"),
    col("MBR_GNDR_CD_SK").alias("MBR_GNDR_CD_SK"),
    col("SCRD_IN").alias("SCRD_IN"),
    col("BRTH_DT_SK").alias("BRTH_DT_SK"),
    col("PREX_COND_EFF_DT_SK").alias("PREX_COND_EFF_DT_SK"),
    col("SRC_SYS_CRT_DT_SK").alias("SRC_SYS_CRT_DT_SK"),
    col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("FIRST_NM").alias("FIRST_NM"),
    col("MIDINIT").alias("MIDINIT"),
    col("LAST_NM").alias("LAST_NM"),
    col("SSN").alias("SSN")
)

df_AUDIT = (
    df_KeyOut
    .withColumn("SCRD_IN", rpad(col("SCRD_IN"), 1, " "))
    .withColumn("BRTH_DT_SK", rpad(col("BRTH_DT_SK"), 10, " "))
    .withColumn("PREX_COND_EFF_DT_SK", rpad(col("PREX_COND_EFF_DT_SK"), 10, " "))
    .withColumn("SRC_SYS_CRT_DT_SK", rpad(col("SRC_SYS_CRT_DT_SK"), 10, " "))
    .withColumn("MIDINIT", rpad(col("MIDINIT"), 1, " "))
)

write_files(
    df_AUDIT.select(
        "MBR_AUDIT_SK",
        "SRC_SYS_CD_SK",
        "MBR_AUDIT_ROW_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "MBR_SK",
        "SRC_SYS_CRT_USER_SK",
        "MBR_AUDIT_ACTN_CD_SK",
        "MBR_GNDR_CD_SK",
        "SCRD_IN",
        "BRTH_DT_SK",
        "PREX_COND_EFF_DT_SK",
        "SRC_SYS_CRT_DT_SK",
        "MBR_UNIQ_KEY",
        "FIRST_NM",
        "MIDINIT",
        "LAST_NM",
        "SSN"
    ),
    f"{adls_path}/load/MBR_AUDIT.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)