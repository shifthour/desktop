# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 01/29/08 14:29:13 Batch  14639_52158 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 01/09/08 13:32:13 Batch  14619_48738 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/24/07 09:35:50 Batch  14573_34558 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_2 11/01/07 15:16:03 Batch  14550_54982 PROMOTE bckcetl ids20 dsadm rc for brent
# MAGIC ^1_2 11/01/07 15:05:36 Batch  14550_54348 INIT bckcett testIDS30 dsadm rc for brent 
# MAGIC ^1_2 10/31/07 10:30:33 Batch  14549_37838 PROMOTE bckcett testIDS30 u08717 Brent
# MAGIC ^1_2 10/31/07 10:24:16 Batch  14549_37464 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_1 10/24/07 15:30:15 Batch  14542_55818 INIT bckcett devlIDS30 u10157 sa - DRG project - moving to ids_current devlopment for coding changes
# MAGIC ^1_1 10/10/07 07:31:23 Batch  14528_27086 INIT bckcett devlIDS30 u08717 Brent
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:          IdsWebdmClmnSumPaymtSumBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target tables are compared on the specified column to see if there are any discrepancies
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Bhoomi Dasari               08/31/2007          3264                              Originally Programmed                           devlIDS30                     Steph Goddard             09/28/2007

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
from pyspark.sql import functions as F
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


ClmMartOwner = get_widget_value('ClmMartOwner','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','100')

jdbc_url, jdbc_props = get_db_config(clmmart_secret_name)

extract_query = f"""SELECT 
Paymtsum.SRC_SYS_CD, 
Paymtsum.PAYMT_REF_ID,
Paymtsum.PAYMT_SUM_LOB_CD,
Paymtsum.NET_AMT
FROM {ClmMartOwner}.CLM_DM_PAYMT_SUM Paymtsum FULL OUTER JOIN {ClmMartOwner}.B_CLM_DM_PAYMT_SUM BPaymtsum
ON Paymtsum.SRC_SYS_CD = BPaymtsum.SRC_SYS_CD
AND Paymtsum.PAYMT_REF_ID = BPaymtsum.PAYMT_REF_ID
AND Paymtsum.PAYMT_SUM_LOB_CD = BPaymtsum.PAYMT_SUM_LOB_CD
WHERE Paymtsum.LAST_UPDT_RUN_CYC_NO = {ExtrRunCycle}
AND Paymtsum.NET_AMT <> BPaymtsum.PAYMT_SUM_NET_AMT
"""

df_SrcTrgtRowComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_research = df_SrcTrgtRowComp.select(
    "SRC_SYS_CD",
    "PAYMT_REF_ID",
    "PAYMT_SUM_LOB_CD",
    "NET_AMT"
)

window_notify = Window.orderBy(F.lit(1))
df_notify = (
    df_SrcTrgtRowComp
    .withColumn("rownum", F.row_number().over(window_notify))
    .filter("rownum = 1")
    .withColumn("NOTIFICATION", F.lit("COLUMN SUM BALANCING IDS - DATAMART CLM DM PAYMT SUM OUT OF TOLERANCE"))
    .withColumn("NOTIFICATION", F.rpad("NOTIFICATION", 70, " "))
    .select("NOTIFICATION")
)

write_files(
    df_research,
    f"{adls_path}/balancing/research/IdsWebdmPaymtSumColumnSumResearch.dat.#RunID#",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)

write_files(
    df_notify,
    f"{adls_path}/balancing/notify/PaymtSumBalancingNotification.dat",
    ",",
    "append",
    False,
    False,
    "\"",
    None
)