# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2014 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:   IdsIhmPgmHierRowCntBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS: 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                       Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                   ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Kalyan Neelam                2014-12-04           TFS 9558                      Initial Programming                                           IntegrateNewDevl       Bhoomi Dasari              12/09/2014

# MAGIC File checked later for rows and email to on-call
# MAGIC Detail file for on-call to research errors
# MAGIC Rows with Failed Comparison on Specified columns
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


RunID = get_widget_value("RunID","")
ToleranceCd = get_widget_value("ToleranceCd","")
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
ExtrRunCycle = get_widget_value("ExtrRunCycle","")
SrcSysCd = get_widget_value("SrcSysCd","")

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"""
SELECT
  IHM_PGM_HIER.PGM_ID as SRC_PGM_ID,
  IHM_PGM_HIER.SBPRG_ID as SRC_SBPRG_ID,
  IHM_PGM_HIER.RISK_SVRTY_CD as SRC_RISK_SVRTY_CD,
  IHM_PGM_HIER.SRC_SYS_CD_SK as SRC_SRC_SYS_CD_SK,
  B_IHM_PGM_HIER.PGM_ID as TRGT_PGM_ID,
  B_IHM_PGM_HIER.SBPRG_ID as TRGT_SBPRG_ID,
  B_IHM_PGM_HIER.RISK_SVRTY_CD as TRGT_RISK_SVRTY_CD,
  B_IHM_PGM_HIER.SRC_SYS_CD_SK as TRGT_SRC_SYS_CD_SK
FROM {IDSOwner}.IHM_PGM_HIER IHM_PGM_HIER
FULL OUTER JOIN {IDSOwner}.B_IHM_PGM_HIER B_IHM_PGM_HIER
  ON IHM_PGM_HIER.PGM_ID = B_IHM_PGM_HIER.PGM_ID
 AND IHM_PGM_HIER.SBPRG_ID = B_IHM_PGM_HIER.SBPRG_ID
 AND IHM_PGM_HIER.RISK_SVRTY_CD = B_IHM_PGM_HIER.RISK_SVRTY_CD
 AND IHM_PGM_HIER.SRC_SYS_CD_SK = B_IHM_PGM_HIER.SRC_SYS_CD_SK
WHERE IHM_PGM_HIER.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
"""

df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_Research = (
    df_SrcTrgtComp.filter(
        (F.col("SRC_PGM_ID").isNull())
        | (F.col("SRC_SBPRG_ID").isNull())
        | (F.col("SRC_RISK_SVRTY_CD").isNull())
        | (F.col("SRC_SRC_SYS_CD_SK").isNull())
        | (F.col("TRGT_PGM_ID").isNull())
        | (F.col("TRGT_SBPRG_ID").isNull())
        | (F.col("TRGT_RISK_SVRTY_CD").isNull())
        | (F.col("TRGT_SRC_SYS_CD_SK").isNull())
    )
    .select(
        "TRGT_PGM_ID",
        "TRGT_SBPRG_ID",
        "TRGT_RISK_SVRTY_CD",
        "TRGT_SRC_SYS_CD_SK",
        "SRC_PGM_ID",
        "SRC_SBPRG_ID",
        "SRC_RISK_SVRTY_CD",
        "SRC_SRC_SYS_CD_SK"
    )
)

if ToleranceCd == "OUT":
    df_Notify = spark.createDataFrame(
        [
            ("ROW COUNT BALANCING " + SrcSysCd + " - IDS IHM_PGM_HIER OUT OF TOLERANCE",)
        ],
        ["NOTIFICATION"]
    )
else:
    schema_notify = StructType([StructField("NOTIFICATION", StringType(), True)])
    df_Notify = spark.createDataFrame([], schema_notify)

df_Notify_final = df_Notify.withColumn("NOTIFICATION", F.rpad("NOTIFICATION", 70, " ")).select("NOTIFICATION")

write_files(
    df_Research,
    f"{adls_path}/balancing/research/{SrcSysCd}IdsIhmPgmHierResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

write_files(
    df_Notify_final,
    f"{adls_path}/balancing/notify/MedMgtBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)