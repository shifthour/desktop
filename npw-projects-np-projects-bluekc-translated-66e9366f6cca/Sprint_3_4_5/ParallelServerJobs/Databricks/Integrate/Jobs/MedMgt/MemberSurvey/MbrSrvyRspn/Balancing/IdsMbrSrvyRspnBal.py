# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2011 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:    IdsMbrSrvyRspnRowCntBalSeq
# MAGIC 
# MAGIC PROCESSING: Row Count errors balancing job for MBR_SRVY_RSPN
# MAGIC                       
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                          Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Kalyan Neelam                2011-06-01             4673                            Initial Programming                                            IntegrateWrhsDevl       Brent Leland                 06-16-2011

# MAGIC File checked later for rows and email to on-call
# MAGIC Detail file for on-call to research errors
# MAGIC Rows with Failed Comparison on Specified columns
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, row_number, rpad
from pyspark.sql import Window
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


# Parameters
RunID = get_widget_value('RunID','')
ToleranceCd = get_widget_value('ToleranceCd','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
IDSOwner = get_widget_value('$IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
SrcSysCd = get_widget_value('SrcSysCd','')

# DB Configuration
jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# Extract Query
extract_query = f"""
SELECT 
MBR_SRVY_RSPN.MBR_SRVY_TYP_CD,
MBR_SRVY_RSPN.MBR_UNIQ_KEY,
MBR_SRVY_RSPN.QSTN_CD_TX,
MBR_SRVY_RSPN.RSPN_DT_SK,
MBR_SRVY_RSPN.SEQ_NO,
MBR_SRVY_RSPN.SRC_SYS_CD_SK,
B_MBR_SRVY_RSPN.MBR_SRVY_TYP_CD,
B_MBR_SRVY_RSPN.MBR_UNIQ_KEY,
B_MBR_SRVY_RSPN.QSTN_CD_TX,
B_MBR_SRVY_RSPN.RSPN_DT_SK,
B_MBR_SRVY_RSPN.SEQ_NO,
B_MBR_SRVY_RSPN.SRC_SYS_CD_SK
FROM {IDSOwner}.MBR_SRVY_RSPN MBR_SRVY_RSPN
FULL OUTER JOIN {IDSOwner}.B_MBR_SRVY_RSPN B_MBR_SRVY_RSPN
    ON MBR_SRVY_RSPN.MBR_SRVY_TYP_CD = B_MBR_SRVY_RSPN.MBR_SRVY_TYP_CD
    AND MBR_SRVY_RSPN.MBR_UNIQ_KEY = B_MBR_SRVY_RSPN.MBR_UNIQ_KEY
    AND MBR_SRVY_RSPN.QSTN_CD_TX = B_MBR_SRVY_RSPN.QSTN_CD_TX
    AND MBR_SRVY_RSPN.RSPN_DT_SK = B_MBR_SRVY_RSPN.RSPN_DT_SK
    AND MBR_SRVY_RSPN.SEQ_NO = B_MBR_SRVY_RSPN.SEQ_NO
    AND MBR_SRVY_RSPN.SRC_SYS_CD_SK = B_MBR_SRVY_RSPN.SRC_SYS_CD_SK
WHERE MBR_SRVY_RSPN.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
"""

df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# Rename columns to match DataStage output
rename_map = {
    "MBR_SRVY_TYP_CD": "SRC_MBR_SRVY_TYP_CD",
    "MBR_UNIQ_KEY": "SRC_MBR_UNIQ_KEY",
    "QSTN_CD_TX": "SRC_QSTN_CD_TX",
    "RSPN_DT_SK": "SRC_RSPN_DT_SK",
    "SEQ_NO": "SRC_SEQ_NO",
    "SRC_SYS_CD_SK": "SRC_SRC_SYS_CD_SK",
    "MBR_SRVY_TYP_CD_1": "TRGT_MBR_SRVY_TYP_CD",
    "MBR_UNIQ_KEY_1": "TRGT_MBR_UNIQ_KEY",
    "QSTN_CD_TX_1": "TRGT_QSTN_CD_TX",
    "RSPN_DT_SK_1": "TRGT_RSPN_DT_SK",
    "SEQ_NO_1": "TRGT_SEQ_NO",
    "SRC_SYS_CD_SK_1": "TRGT_SRC_SYS_CD_SK"
}
for old_col, new_col in rename_map.items():
    if old_col in df_SrcTrgtComp.columns:
        df_SrcTrgtComp = df_SrcTrgtComp.withColumnRenamed(old_col, new_col)

df_Missing = df_SrcTrgtComp

# Transformer Logic - Research Link
df_Research = df_Missing.filter(
    col("SRC_MBR_SRVY_TYP_CD").isNull() |
    col("SRC_MBR_UNIQ_KEY").isNull() |
    col("SRC_QSTN_CD_TX").isNull() |
    col("SRC_RSPN_DT_SK").isNull() |
    col("SRC_SEQ_NO").isNull() |
    col("SRC_SRC_SYS_CD_SK").isNull() |
    col("TRGT_MBR_SRVY_TYP_CD").isNull() |
    col("TRGT_MBR_UNIQ_KEY").isNull() |
    col("TRGT_QSTN_CD_TX").isNull() |
    col("TRGT_RSPN_DT_SK").isNull() |
    col("TRGT_SEQ_NO").isNull() |
    col("TRGT_SRC_SYS_CD_SK").isNull()
)

df_Research_final = df_Research.select(
    col("TRGT_MBR_SRVY_TYP_CD"),
    col("TRGT_MBR_UNIQ_KEY"),
    col("TRGT_QSTN_CD_TX"),
    rpad(col("TRGT_RSPN_DT_SK"), 10, " ").alias("TRGT_RSPN_DT_SK"),
    col("TRGT_SEQ_NO"),
    col("TRGT_SRC_SYS_CD_SK"),
    col("SRC_MBR_SRVY_TYP_CD"),
    col("SRC_MBR_UNIQ_KEY"),
    col("SRC_QSTN_CD_TX"),
    rpad(col("SRC_RSPN_DT_SK"), 10, " ").alias("SRC_RSPN_DT_SK"),
    col("SRC_SEQ_NO"),
    col("SRC_SRC_SYS_CD_SK")
)

# Transformer Logic - Notify Link
windowSpec = Window.orderBy(lit(1))
df_temp = df_Missing.withColumn("__rowNum", row_number().over(windowSpec))
df_NotifyCandidate = df_temp.filter(col("__rowNum") == 1)
if ToleranceCd == "OUT":
    df_Notify = df_NotifyCandidate.select(
        rpad(lit(f"ROW COUNT BALANCING {SrcSysCd} - IDS MBR_SRVY_RSPN OUT OF TOLERANCE"), 70, " ").alias("NOTIFICATION")
    )
else:
    df_Notify = spark.createDataFrame([], "NOTIFICATION STRING")

# Research File
write_files(
    df_Research_final,
    f"{adls_path}/balancing/research/IdsMbrSrvyRspnResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Error Notification File
write_files(
    df_Notify,
    f"{adls_path}/balancing/notify/MemberSurveyBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)