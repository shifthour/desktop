# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:    IdsEdwAplBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This pulls data from the Concatenated Source Sequential file coming from IDS and loads into the B table in EDW
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                                          Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Bhoomi Dasari                 10/10/2007         3028                              Originally Programmed                                                       devlEDW10                      Steph Goddard            10/18/2007
# MAGIC 
# MAGIC Shanmugam A.                03/08/2017         5321                          Updated Aliase for all columns in SrcTrgtRowComp             EnterpriseDev2                 Jag Yelavarthi               2017-03-08
# MAGIC                                                                                                           stage to match with stage meta data

# MAGIC File checked later for rows and email to on-call
# MAGIC Detail files for on-call to research errors
# MAGIC Rows with Failed Comparison on Specified columns
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


# Retrieve all parameter values
EDWOwner = get_widget_value('EDWOwner', '')
edw_secret_name = get_widget_value('edw_secret_name', '')
ExtrRunCycle = get_widget_value('ExtrRunCycle', '')
RunID = get_widget_value('RunID', '')

# Obtain JDBC configuration for EDW
jdbc_url, jdbc_props = get_db_config(edw_secret_name)

# Read from SrcTrgtRowComp - first output pin: key
extract_query_1 = f"""SELECT
APL_F.APL_ID AS APL_F_APL_ID,
APL_LVL_D.APL_ID AS APL_LVL_D_APL_ID
FROM {EDWOwner}.APL_F APL_F FULL OUTER JOIN {EDWOwner}.APL_LVL_D APL_LVL_D
ON APL_F.APL_ID = APL_LVL_D.APL_ID
WHERE APL_F.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
"""
df_SrcTrgtRowComp_key = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_1)
    .load()
)

# Read from SrcTrgtRowComp - second output pin: Match
extract_query_2 = f"""SELECT
APL_F.SRC_SYS_CD AS APL_F_SRC_SYS_CD,
APL_F.APL_ID AS APL_F_APL_ID,
APL_LVL_D.SRC_SYS_CD AS APL_LVL_D_SRC_SYS_CD,
APL_LVL_D.APL_ID AS APL_LVL_D_APL_ID
FROM {EDWOwner}.APL_F APL_F INNER JOIN {EDWOwner}.APL_LVL_D APL_LVL_D
ON APL_F.SRC_SYS_CD = APL_LVL_D.SRC_SYS_CD
AND APL_F.APL_ID = APL_LVL_D.APL_ID
WHERE APL_F.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
"""
df_SrcTrgtRowComp_Match = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_2)
    .load()
)

# Write ParChldMatch (df_SrcTrgtRowComp_Match) to file
write_files(
    df_SrcTrgtRowComp_Match.select(
        "APL_F_SRC_SYS_CD",
        "APL_F_APL_ID",
        "APL_LVL_D_SRC_SYS_CD",
        "APL_LVL_D_APL_ID"
    ),
    f"{adls_path}/balancing/sync/AplFAplLvlDBalancingTotalMatch.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Transformer: Transform1
df_Research1 = (
    df_SrcTrgtRowComp_key
    .filter(F.col("APL_F_APL_ID").isNull())
    .select("APL_F_APL_ID", "APL_LVL_D_APL_ID")
)

df_Research2 = (
    df_SrcTrgtRowComp_key
    .filter(F.col("APL_LVL_D_APL_ID").isNull())
    .select("APL_F_APL_ID", "APL_LVL_D_APL_ID")
)

df_Notify = df_SrcTrgtRowComp_key.limit(1).select()
df_Notify = df_Notify.withColumn(
    "NOTIFICATION",
    F.lit("REFERENTIAL INTEGRITY BALANCING IDS - EDW APL F AND APL LVL D CHECK FOR OUT OF TOLERANCE")
)
df_Notify = df_Notify.withColumn("NOTIFICATION", rpad("NOTIFICATION", 70, " "))

# ResearchFile2 (df_Research2)
write_files(
    df_Research2.select("APL_F_APL_ID", "APL_LVL_D_APL_ID"),
    f"{adls_path}/balancing/research/IdsEdwChldParAplFAplLvlDRIResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# ResearchFile1 (df_Research1)
write_files(
    df_Research1.select("APL_F_APL_ID", "APL_LVL_D_APL_ID"),
    f"{adls_path}/balancing/research/IdsEdwParChldAplFAplLvlDRIResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# NotificationFile (df_Notify)
write_files(
    df_Notify.select("NOTIFICATION"),
    f"{adls_path}/balancing/notify/AppealsBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)