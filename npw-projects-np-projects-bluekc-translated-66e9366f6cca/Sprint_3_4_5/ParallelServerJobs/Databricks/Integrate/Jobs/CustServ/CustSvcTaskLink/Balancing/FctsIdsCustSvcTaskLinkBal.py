# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:          FctsIdsCustSvcTaskLinkBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                        Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                  ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               06/22/2007          3264                              Originally Programmed                                   devlIDS30
# MAGIC 
# MAGIC Parikshith Chada               8/24/2007         3264                              Modified the balancing process,                       devlIDS30                   Steph Goddard             09/14/2007
# MAGIC                                                                                                           by changing snapshot file to snapshot table       IntegrateDev2
# MAGIC  
# MAGIC Jaideep Mankala               2017-03-13         5321                            Modified SQL aliases in columns in stage            IntegrateDev2            Jag Yelavarthi             2017-03-13
# MAGIC 							SrcTrgtComp

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
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
RunID = get_widget_value('RunID','')
ToleranceCd = get_widget_value('ToleranceCd','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

sql_query = f"""
SELECT 
CUST_SVC_TASK_LINK.SRC_SYS_CD_SK AS TRGT_SRC_SYS_CD_SK,
CUST_SVC_TASK_LINK.CUST_SVC_ID AS TRGT_CUST_SVC_ID,
CUST_SVC_TASK_LINK.TASK_SEQ_NO AS TRGT_TASK_SEQ_NO,
CUST_SVC_TASK_LINK.CUST_SVC_TASK_LINK_TYP_CD_SK  AS TRGT_CUST_SVC_TASK_LINK_TYP_CD_SK,
CUST_SVC_TASK_LINK.LINK_RCRD_ID AS TRGT_LINK_RCRD_ID,
B_CUST_SVC_TASK_LINK.SRC_SYS_CD_SK AS SRC_SRC_SYS_CD_SK,
B_CUST_SVC_TASK_LINK.CUST_SVC_ID AS SRC_CUST_SVC_ID,
B_CUST_SVC_TASK_LINK.TASK_SEQ_NO AS SRC_TASK_SEQ_NO,
B_CUST_SVC_TASK_LINK.CUST_SVC_TASK_LINK_TYP_CD_SK AS SRC_CUST_SVC_TASK_LINK_TYP_CD_SK,
B_CUST_SVC_TASK_LINK.LINK_RCRD_ID AS SRC_LINK_RCRD_ID
FROM 
{IDSOwner}.CD_MPPNG MPPNG, {IDSOwner}.CUST_SVC_TASK_LINK CUST_SVC_TASK_LINK
FULL OUTER JOIN {IDSOwner}.B_CUST_SVC_TASK_LINK B_CUST_SVC_TASK_LINK
ON CUST_SVC_TASK_LINK.SRC_SYS_CD_SK = B_CUST_SVC_TASK_LINK.SRC_SYS_CD_SK
AND CUST_SVC_TASK_LINK.CUST_SVC_ID = B_CUST_SVC_TASK_LINK.CUST_SVC_ID
AND CUST_SVC_TASK_LINK.TASK_SEQ_NO = B_CUST_SVC_TASK_LINK.TASK_SEQ_NO
AND CUST_SVC_TASK_LINK.CUST_SVC_TASK_LINK_TYP_CD_SK = B_CUST_SVC_TASK_LINK.CUST_SVC_TASK_LINK_TYP_CD_SK
AND CUST_SVC_TASK_LINK.LINK_RCRD_ID = B_CUST_SVC_TASK_LINK.LINK_RCRD_ID
WHERE 
CUST_SVC_TASK_LINK.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
AND CUST_SVC_TASK_LINK.SRC_SYS_CD_SK = MPPNG.CD_MPPNG_SK 
AND MPPNG.TRGT_CD = 'FACETS'
"""

df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", sql_query)
    .load()
)

df_transformlogic_in = df_SrcTrgtComp

df_Research = df_transformlogic_in.filter(
    F.col("SRC_SRC_SYS_CD_SK").isNull() |
    F.col("SRC_CUST_SVC_ID").isNull() |
    F.col("SRC_TASK_SEQ_NO").isNull() |
    F.col("SRC_CUST_SVC_TASK_LINK_TYP_CD_SK").isNull() |
    F.col("SRC_LINK_RCRD_ID").isNull() |
    F.col("TRGT_CUST_SVC_ID").isNull() |
    F.col("TRGT_SRC_SYS_CD_SK").isNull() |
    F.col("TRGT_TASK_SEQ_NO").isNull() |
    F.col("TRGT_CUST_SVC_TASK_LINK_TYP_CD_SK").isNull() |
    F.col("TRGT_LINK_RCRD_ID").isNull()
).select(
    "TRGT_SRC_SYS_CD_SK",
    "TRGT_CUST_SVC_ID",
    "TRGT_TASK_SEQ_NO",
    "TRGT_CUST_SVC_TASK_LINK_TYP_CD_SK",
    "TRGT_LINK_RCRD_ID",
    "SRC_SRC_SYS_CD_SK",
    "SRC_CUST_SVC_ID",
    "SRC_TASK_SEQ_NO",
    "SRC_CUST_SVC_TASK_LINK_TYP_CD_SK",
    "SRC_LINK_RCRD_ID"
)

notify_schema = StructType([StructField("NOTIFICATION", StringType(), True)])
df_notify_in = df_transformlogic_in.withColumn("rownum", F.row_number().over(Window.orderBy(F.lit(1))))

if ToleranceCd == 'OUT':
    df_temp = df_notify_in.filter(F.col("rownum") == 1).select(
        F.lit("ROW COUNT BALANCING FACETS - IDS CUST SVC TASK LINK OUT OF TOLERANCE").alias("NOTIFICATION")
    )
    df_notify = df_temp.withColumn("NOTIFICATION", F.rpad(F.col("NOTIFICATION"), 70, " "))
else:
    df_notify = spark.createDataFrame([], schema=notify_schema)

write_files(
    df_Research,
    f"{adls_path}/balancing/research/FctsIdsCustSvcTaskLinkResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

write_files(
    df_notify,
    f"{adls_path}/balancing/notify/CustSvcBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)