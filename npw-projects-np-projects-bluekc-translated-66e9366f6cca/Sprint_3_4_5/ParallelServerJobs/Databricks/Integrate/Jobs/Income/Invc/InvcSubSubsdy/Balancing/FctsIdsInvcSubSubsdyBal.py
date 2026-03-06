# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2014 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:     FctsIdsInvcSubSbsdyBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Kalyan Neelam                 2014-06-20           5235                         Initial Programming                                                IntegrateNewDevl        Bhoomi Dasari              6/22/2014

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
from pyspark.sql import Window
from pyspark.sql.functions import col, lit, row_number, isnull, rpad
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
RunID = get_widget_value('RunID','')
ToleranceCd = get_widget_value('ToleranceCd','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = f"""
SELECT 
INVC_SUB_SUBSDY.BILL_INVC_ID,
INVC_SUB_SUBSDY.SUB_UNIQ_KEY,
INVC_SUB_SUBSDY.CLS_PLN_ID,
INVC_SUB_SUBSDY.PROD_ID,
INVC_SUB_SUBSDY.PROD_BILL_CMPNT_ID,
INVC_SUB_SUBSDY.COV_DUE_DT_SK,
INVC_SUB_SUBSDY.COV_STRT_DT_SK,
INVC_SUB_SUBSDY.INVC_SUB_SBSDY_PRM_TYP_CD,
INVC_SUB_SUBSDY.CRT_TS,
INVC_SUB_SUBSDY.INVC_SUB_SBSDY_BILL_DISP_CD,
INVC_SUB_SUBSDY.SRC_SYS_CD_SK,
B_INVC_SUB_SUBSDY.BILL_INVC_ID,
B_INVC_SUB_SUBSDY.SUB_UNIQ_KEY,
B_INVC_SUB_SUBSDY.CLS_PLN_ID,
B_INVC_SUB_SUBSDY.PROD_ID,
B_INVC_SUB_SUBSDY.PROD_BILL_CMPNT_ID,
B_INVC_SUB_SUBSDY.COV_DUE_DT_SK,
B_INVC_SUB_SUBSDY.COV_STRT_DT_SK,
B_INVC_SUB_SUBSDY.INVC_SUB_SBSDY_PRM_TYP_CD,
B_INVC_SUB_SUBSDY.CRT_TS,
B_INVC_SUB_SUBSDY.INVC_SUB_SBSDY_BILL_DISP_CD,
B_INVC_SUB_SUBSDY.SRC_SYS_CD_SK 
FROM {IDSOwner}.INVC_SUB_SBSDY INVC_SUB_SUBSDY
FULL OUTER JOIN {IDSOwner}.B_INVC_SUB_SBSDY B_INVC_SUB_SUBSDY
ON INVC_SUB_SUBSDY.BILL_INVC_ID = B_INVC_SUB_SUBSDY.BILL_INVC_ID
AND INVC_SUB_SUBSDY.SUB_UNIQ_KEY = B_INVC_SUB_SUBSDY.SUB_UNIQ_KEY
AND INVC_SUB_SUBSDY.CLS_PLN_ID = B_INVC_SUB_SUBSDY.CLS_PLN_ID
AND INVC_SUB_SUBSDY.PROD_ID = B_INVC_SUB_SUBSDY.PROD_ID
AND INVC_SUB_SUBSDY.PROD_BILL_CMPNT_ID = B_INVC_SUB_SUBSDY.PROD_BILL_CMPNT_ID
AND INVC_SUB_SUBSDY.COV_DUE_DT_SK = B_INVC_SUB_SUBSDY.COV_DUE_DT_SK
AND INVC_SUB_SUBSDY.COV_STRT_DT_SK = B_INVC_SUB_SUBSDY.COV_STRT_DT_SK
AND INVC_SUB_SUBSDY.INVC_SUB_SBSDY_PRM_TYP_CD = B_INVC_SUB_SUBSDY.INVC_SUB_SBSDY_PRM_TYP_CD
AND INVC_SUB_SUBSDY.CRT_TS = B_INVC_SUB_SUBSDY.CRT_TS
AND INVC_SUB_SUBSDY.INVC_SUB_SBSDY_BILL_DISP_CD = B_INVC_SUB_SUBSDY.INVC_SUB_SBSDY_BILL_DISP_CD
AND INVC_SUB_SUBSDY.SRC_SYS_CD_SK = B_INVC_SUB_SUBSDY.SRC_SYS_CD_SK
WHERE INVC_SUB_SUBSDY.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
"""

df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_SrcTrgtComp = (
    df_SrcTrgtComp
    .withColumnRenamed("BILL_INVC_ID", "SRC_BILL_INVC_ID")
    .withColumnRenamed("SUB_UNIQ_KEY", "SRC_SUB_UNIQ_KEY")
    .withColumnRenamed("CLS_PLN_ID", "SRC_CLS_PLN_ID")
    .withColumnRenamed("PROD_ID", "SRC_PROD_ID")
    .withColumnRenamed("PROD_BILL_CMPNT_ID", "SRC_PROD_BILL_CMPNT_ID")
    .withColumnRenamed("COV_DUE_DT_SK", "SRC_COV_DUE_DT_SK")
    .withColumnRenamed("COV_STRT_DT_SK", "SRC_COV_STRT_DT_SK")
    .withColumnRenamed("INVC_SUB_SBSDY_PRM_TYP_CD", "SRC_INVC_SUB_SBSDY_PRM_TYP_CD")
    .withColumnRenamed("CRT_TS", "SRC_CRT_TS")
    .withColumnRenamed("INVC_SUB_SBSDY_BILL_DISP_CD", "SRC_INVC_SUB_SBSDY_BILL_DISP_CD")
    .withColumnRenamed("SRC_SYS_CD_SK", "SRC_SRC_SYS_CD_SK")
    .withColumnRenamed("BILL_INVC_ID_1", "TRGT_BILL_INVC_ID")
    .withColumnRenamed("SUB_UNIQ_KEY_1", "TRGT_SUB_UNIQ_KEY")
    .withColumnRenamed("CLS_PLN_ID_1", "TRGT_CLS_PLN_ID")
    .withColumnRenamed("PROD_ID_1", "TRGT_PROD_ID")
    .withColumnRenamed("PROD_BILL_CMPNT_ID_1", "TRGT_PROD_BILL_CMPNT_ID")
    .withColumnRenamed("COV_DUE_DT_SK_1", "TRGT_COV_DUE_DT_SK")
    .withColumnRenamed("COV_STRT_DT_SK_1", "TRGT_COV_STRT_DT_SK")
    .withColumnRenamed("INVC_SUB_SBSDY_PRM_TYP_CD_1", "TRGT_INVC_SUB_SBSDY_PRM_TYP_CD")
    .withColumnRenamed("CRT_TS_1", "TRGT_CRT_TS")
    .withColumnRenamed("INVC_SUB_SBSDY_BILL_DISP_CD_1", "TRGT_INVC_SUB_SBSDY_BILL_DISP_CD")
    .withColumnRenamed("SRC_SYS_CD_SK_1", "TRGT_SRC_SYS_CD_SK")
)

df_Research = df_SrcTrgtComp.filter(
    isnull(col("SRC_BILL_INVC_ID"))
    | isnull(col("SRC_CLS_PLN_ID"))
    | isnull(col("SRC_COV_DUE_DT_SK"))
    | isnull(col("SRC_COV_STRT_DT_SK"))
    | isnull(col("SRC_CRT_TS"))
    | isnull(col("SRC_INVC_SUB_SBSDY_BILL_DISP_CD"))
    | isnull(col("SRC_INVC_SUB_SBSDY_PRM_TYP_CD"))
    | isnull(col("SRC_PROD_BILL_CMPNT_ID"))
    | isnull(col("SRC_PROD_ID"))
    | isnull(col("SRC_SRC_SYS_CD_SK"))
    | isnull(col("SRC_SUB_UNIQ_KEY"))
    | isnull(col("TRGT_CLS_PLN_ID"))
    | isnull(col("TRGT_BILL_INVC_ID"))
    | isnull(col("TRGT_COV_DUE_DT_SK"))
    | isnull(col("TRGT_COV_STRT_DT_SK"))
    | isnull(col("TRGT_CRT_TS"))
    | isnull(col("TRGT_INVC_SUB_SBSDY_BILL_DISP_CD"))
    | isnull(col("TRGT_INVC_SUB_SBSDY_PRM_TYP_CD"))
    | isnull(col("TRGT_PROD_BILL_CMPNT_ID"))
    | isnull(col("TRGT_PROD_ID"))
    | isnull(col("TRGT_SRC_SYS_CD_SK"))
    | isnull(col("TRGT_SUB_UNIQ_KEY"))
)

df_ResearchSelected = df_Research.select(
    col("TRGT_BILL_INVC_ID"),
    col("TRGT_SUB_UNIQ_KEY"),
    col("TRGT_CLS_PLN_ID"),
    col("TRGT_PROD_ID"),
    col("TRGT_PROD_BILL_CMPNT_ID"),
    rpad(col("TRGT_COV_DUE_DT_SK"), 10, " ").alias("TRGT_COV_DUE_DT_SK"),
    rpad(col("TRGT_COV_STRT_DT_SK"), 10, " ").alias("TRGT_COV_STRT_DT_SK"),
    col("TRGT_INVC_SUB_SBSDY_PRM_TYP_CD"),
    col("TRGT_CRT_TS"),
    col("TRGT_INVC_SUB_SBSDY_BILL_DISP_CD"),
    col("TRGT_SRC_SYS_CD_SK"),
    col("SRC_BILL_INVC_ID"),
    col("SRC_SUB_UNIQ_KEY"),
    col("SRC_CLS_PLN_ID"),
    col("SRC_PROD_ID"),
    col("SRC_PROD_BILL_CMPNT_ID"),
    rpad(col("SRC_COV_DUE_DT_SK"), 10, " ").alias("SRC_COV_DUE_DT_SK"),
    rpad(col("SRC_COV_STRT_DT_SK"), 10, " ").alias("SRC_COV_STRT_DT_SK"),
    col("SRC_INVC_SUB_SBSDY_PRM_TYP_CD"),
    col("SRC_CRT_TS"),
    col("SRC_INVC_SUB_SBSDY_BILL_DISP_CD"),
    col("SRC_SRC_SYS_CD_SK")
)

w = Window.orderBy(lit(1))
df_temp = df_SrcTrgtComp.withColumn("__rownum", row_number().over(w))

if ToleranceCd == 'OUT':
    df_notify_temp = df_temp.filter(col("__rownum") == 1)
else:
    df_notify_temp = spark.createDataFrame([], df_temp.schema)

df_NotifyFinal = df_notify_temp.select(
    rpad(
        lit("ROW COUNT BALANCING FACETS - IDS INVC SUB SUBSIDY OUT OF TOLERANCE"),
        70,
        " "
    ).alias("NOTIFICATION")
)

research_file_path = f"{adls_path}/balancing/research/FctsIdsInvcSubSubsidyResearch.dat.{RunID}"
write_files(
    df_ResearchSelected,
    research_file_path,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

error_notify_file_path = f"{adls_path}/balancing/notify/IncomeBalancingNotification.dat"
write_files(
    df_NotifyFinal,
    error_notify_file_path,
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)