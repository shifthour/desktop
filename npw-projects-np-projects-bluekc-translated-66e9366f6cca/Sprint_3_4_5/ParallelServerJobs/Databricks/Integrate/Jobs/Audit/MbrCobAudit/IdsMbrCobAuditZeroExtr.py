# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:    Reads IDS Member COB Audit table checking for fields set to zero and rekeys them
# MAGIC       
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Sharon Andrew       2007-03-23        3265                      Initial program                                                                               devlIDS30                      Brent Leland             2007-04-05    
# MAGIC O. Nielsen               2008-08-01        Facets 4.5.1          Change OTHR_CAR_POL_ID to varchar(40)                               devlIDSnew  
# MAGIC 
# MAGIC Manasa Andru         2015-03-31        TFS - 9741            Modified the extract SQL to extract all the records                       IntegrateNewDevl          Kalyan Neelam          2015-04-02
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
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


RunID = get_widget_value("RunID","")
RunCycle = get_widget_value("RunCycle","100")
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"""
SELECT MBR_COB_AUDIT.MBR_COB_AUDIT_SK,
       MBR_COB_AUDIT.SRC_SYS_CD_SK,
       MBR_COB_AUDIT.MBR_COB_AUDIT_ROW_ID,
       MBR_COB_AUDIT.CRT_RUN_CYC_EXCTN_SK,
       MBR_COB_AUDIT.LAST_UPDT_RUN_CYC_EXCTN_SK,
       MBR_COB_AUDIT.MBR_SK,
       MBR_COB_AUDIT.SRC_SYS_CRT_USER_SK,
       MBR_COB_AUDIT.MBR_COB_AUDIT_ACTN_CD_SK,
       MBR_COB_AUDIT.MBR_COB_LAST_VER_METH_CD_SK,
       MBR_COB_AUDIT.MBR_COB_OTHR_CAR_ID_CD_SK,
       MBR_COB_AUDIT.MBR_COB_PAYMT_PRTY_CD_SK,
       MBR_COB_AUDIT.MBR_COB_TERM_RSN_CD_SK,
       MBR_COB_AUDIT.MBR_COB_TYP_CD_SK,
       MBR_COB_AUDIT.MBR_UNIQ_KEY,
       MBR_COB_AUDIT.COB_LTR_TRGR_DT_SK,
       MBR_COB_AUDIT.EFF_DT_SK,
       MBR_COB_AUDIT.LACK_OF_COB_INFO_STRT_DT_SK,
       MBR_COB_AUDIT.SRC_SYS_CRT_DT_SK,
       MBR_COB_AUDIT.TERM_DT_SK,
       MBR_COB_AUDIT.LAST_VERIFIER_TX,
       MBR_COB_AUDIT.OTHR_CAR_POL_ID
FROM {IDSOwner}.MBR_COB_AUDIT MBR_COB_AUDIT
WHERE MBR_COB_AUDIT.MBR_SK = 0
"""

df_MBR_COB_AUDIT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_Key = df_MBR_COB_AUDIT.withColumn(
    "svMbrSk",
    GetFkeyMbr(
        F.lit("FACETS"),
        F.col("MBR_COB_AUDIT_SK"),
        F.col("MBR_UNIQ_KEY"),
        F.lit("X")
    )
)

df_COB_AUDIT = df_Key \
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(RunCycle)) \
    .withColumn("MBR_SK", F.col("svMbrSk")) \
    .withColumn("COB_LTR_TRGR_DT_SK", F.rpad(F.col("COB_LTR_TRGR_DT_SK"), 10, " ")) \
    .withColumn("EFF_DT_SK", F.rpad(F.col("EFF_DT_SK"), 10, " ")) \
    .withColumn("LACK_OF_COB_INFO_STRT_DT_SK", F.rpad(F.col("LACK_OF_COB_INFO_STRT_DT_SK"), 10, " ")) \
    .withColumn("SRC_SYS_CRT_DT_SK", F.rpad(F.col("SRC_SYS_CRT_DT_SK"), 10, " ")) \
    .withColumn("TERM_DT_SK", F.rpad(F.col("TERM_DT_SK"), 10, " ")) \
    .select(
        "MBR_COB_AUDIT_SK",
        "SRC_SYS_CD_SK",
        "MBR_COB_AUDIT_ROW_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "MBR_SK",
        "SRC_SYS_CRT_USER_SK",
        "MBR_COB_AUDIT_ACTN_CD_SK",
        "MBR_COB_LAST_VER_METH_CD_SK",
        "MBR_COB_OTHR_CAR_ID_CD_SK",
        "MBR_COB_PAYMT_PRTY_CD_SK",
        "MBR_COB_TERM_RSN_CD_SK",
        "MBR_COB_TYP_CD_SK",
        "MBR_UNIQ_KEY",
        "COB_LTR_TRGR_DT_SK",
        "EFF_DT_SK",
        "LACK_OF_COB_INFO_STRT_DT_SK",
        "SRC_SYS_CRT_DT_SK",
        "TERM_DT_SK",
        "LAST_VERIFIER_TX",
        "OTHR_CAR_POL_ID"
    )

write_files(
    df_COB_AUDIT,
    f"{adls_path}/load/MBR_COB_AUDIT.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)