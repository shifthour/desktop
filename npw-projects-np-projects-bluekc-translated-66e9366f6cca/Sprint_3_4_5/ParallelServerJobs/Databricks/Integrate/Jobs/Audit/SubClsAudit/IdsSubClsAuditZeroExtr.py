# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:    Reads IDS Subscriber Class Audit table checking for fields set to zero and rekeys them
# MAGIC       
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Sharon Andrew       2007-03-23        3265                      Initial program                                                                               devlIDS30                      Brent Leland              2007-04-05
# MAGIC 
# MAGIC Manasa Andru         2015-03-31        TFS - 9741           Modified the extract SQL to extract all the records                       IntegrateNewDevl            Kalyan Neelam          2015-04-02
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
extract_query = f"""SELECT 
SUB_CLS_AUDIT.SUB_CLS_AUDIT_SK,
SUB_CLS_AUDIT.SRC_SYS_CD_SK,
SUB_CLS_AUDIT.SUB_CLS_AUDIT_ROW_ID,
SUB_CLS_AUDIT.CRT_RUN_CYC_EXCTN_SK,
SUB_CLS_AUDIT.LAST_UPDT_RUN_CYC_EXCTN_SK,
SUB_CLS_AUDIT.CLS_SK,
SUB_CLS_AUDIT.SRC_SYS_CRT_USER_SK,
SUB_CLS_AUDIT.SUB_SK,
SUB_CLS_AUDIT.SUB_CLS_AUDIT_ACTN_CD_SK,
SUB_CLS_AUDIT.SRC_SYS_CRT_DT_SK,
SUB_CLS_AUDIT.SUB_UNIQ_KEY,
SUB_CLS_AUDIT.SUB_CLS_AUDIT_EFF_DT,
SUB_CLS_AUDIT.SUB_CLS_AUDIT_TERM_DT
FROM {IDSOwner}.SUB_CLS_AUDIT SUB_CLS_AUDIT
WHERE SUB_CLS_AUDIT.SUB_SK = 0
"""

df_SUB_CLS_AUDIT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_key = df_SUB_CLS_AUDIT.withColumn(
    "svSubSk",
    GetFkeySub(lit("FACETS"), col("SUB_CLS_AUDIT_SK"), col("SUB_UNIQ_KEY"), lit("X"))
)

df_key2 = df_key.withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(RunCycle))
df_key3 = df_key2.withColumn("SUB_SK", col("svSubSk"))

df_final = (
    df_key3
    .withColumn("SRC_SYS_CRT_DT_SK", rpad(col("SRC_SYS_CRT_DT_SK"), 10, " "))
    .withColumn("SUB_CLS_AUDIT_EFF_DT", rpad(col("SUB_CLS_AUDIT_EFF_DT"), 10, " "))
    .withColumn("SUB_CLS_AUDIT_TERM_DT", rpad(col("SUB_CLS_AUDIT_TERM_DT"), 10, " "))
    .select(
        "SUB_CLS_AUDIT_SK",
        "SRC_SYS_CD_SK",
        "SUB_CLS_AUDIT_ROW_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CLS_SK",
        "SRC_SYS_CRT_USER_SK",
        "SUB_SK",
        "SUB_CLS_AUDIT_ACTN_CD_SK",
        "SRC_SYS_CRT_DT_SK",
        "SUB_UNIQ_KEY",
        "SUB_CLS_AUDIT_EFF_DT",
        "SUB_CLS_AUDIT_TERM_DT"
    )
)

write_files(
    df_final,
    f"{adls_path}/load/SUB_CLS_AUDIT.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)