# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:          IdsEdwRcvdIncmFBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target tables are compared on the specified column to see if there are any discrepancies
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               07/09/2007          3264                              Originally Programmed                          devlEDW10               Steph Goddard             10/26/2007     
# MAGIC 
# MAGIC Srikanth Mettpalli               10/05/2013         5114                              Originally Programmed                         EnterpriseWrhsDevl   Peter Marshall                12/12/2013    
# MAGIC                                                                                                             (Server to Parallel Conversion)

# MAGIC Pull all the Matching Records from RCVD_INCM_F and B_RCVD_INCM_F Table.
# MAGIC Job Name: IdsEdwRowToRowRcvdIncmFBal
# MAGIC Write all the Matching Records from RCVD_INCM_F and B_RCVD_INCM_F Table.
# MAGIC Copy stage used to remove duplicates and create one record for the Notification file.
# MAGIC File checked later for rows and email to on-call
# MAGIC Detail file for on-call to research errors
# MAGIC Rows with Failed Comparison on Specified columns
# MAGIC Pull all the Missing Records from RCVD_INCM_F and B_RCVD_INCM_F Table.
# MAGIC IDS - EDW Received Income Facts Row To Row Comparisons
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../../../Utility_Enterprise
# COMMAND ----------


EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
RunID = get_widget_value('RunID','')

jdbc_url, jdbc_props = get_db_config(edw_secret_name)

query_db2_RCVD_INCM_F_Missing_in = f"""SELECT
RcvdIncmF.SRC_SYS_CD,
RcvdIncmF.BILL_ENTY_UNIQ_KEY,
RcvdIncmF.DUE_YR_MO_SK,
RcvdIncmF.BILL_INCM_RCPT_BILL_CNTR_ID,
RcvdIncmF.RCPT_YR_MO_SK,
RcvdIncmF.BILL_INCM_RCPT_CRT_DTM,
RcvdIncmF.FNCL_LOB_CD
FROM {EDWOwner}.RCVD_INCM_F RcvdIncmF
FULL OUTER JOIN {EDWOwner}.B_RCVD_INCM_F BRcvdIncm
ON RcvdIncmF.SRC_SYS_CD = BRcvdIncm.SRC_SYS_CD
AND RcvdIncmF.BILL_ENTY_UNIQ_KEY = BRcvdIncm.BILL_ENTY_UNIQ_KEY
AND RcvdIncmF.DUE_YR_MO_SK = BRcvdIncm.DUE_YR_MO_SK
AND RcvdIncmF.BILL_INCM_RCPT_BILL_CNTR_ID = BRcvdIncm.BILL_INCM_RCPT_BILL_CNTR_ID
AND RcvdIncmF.RCPT_YR_MO_SK = BRcvdIncm.RCPT_YR_MO_SK
AND RcvdIncmF.BILL_INCM_RCPT_CRT_DTM = BRcvdIncm.BILL_INCM_RCPT_CRT_DTM
WHERE RcvdIncmF.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
AND RcvdIncmF.FNCL_LOB_CD <> BRcvdIncm.FNCL_LOB_CD
"""

df_db2_RCVD_INCM_F_Missing_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_db2_RCVD_INCM_F_Missing_in)
    .load()
)

df_xfrm_BusinessLogic_in = df_db2_RCVD_INCM_F_Missing_in

df_xfrm_BusinessLogic_Error = (
    df_xfrm_BusinessLogic_in
    .withColumn("DUE_YR_MO_SK", rpad("DUE_YR_MO_SK", 6, " "))
    .withColumn("RCPT_YR_MO_SK", rpad("RCPT_YR_MO_SK", 6, " "))
    .select(
        "SRC_SYS_CD",
        "BILL_ENTY_UNIQ_KEY",
        "DUE_YR_MO_SK",
        "BILL_INCM_RCPT_BILL_CNTR_ID",
        "RCPT_YR_MO_SK",
        "BILL_INCM_RCPT_CRT_DTM",
        "FNCL_LOB_CD"
    )
)

df_NotifyDDup_in = (
    df_xfrm_BusinessLogic_in
    .withColumn("__rownum", F.row_number().over(Window.orderBy(F.lit(1))))
    .filter(F.col("__rownum") == 1)
    .select(F.lit("ROW TO ROW BALANCING IDS - EDW MBR D OUT OF TOLERANCE").alias("NOTIFICATION"))
)

df_NotifyDDup_in = df_NotifyDDup_in.withColumn("NOTIFICATION", rpad("NOTIFICATION", 100, " "))
df_NotifyDDup_in = df_NotifyDDup_in.select("NOTIFICATION")

write_files(
    df_xfrm_BusinessLogic_Error,
    f"{adls_path}/balancing/research/IdsEdwRcvdIncmFRowToRowResearch.dat.{RunID}",
    ",",
    "overwrite",
    False,
    False,
    "^",
    None
)

df_cpy_DDup = dedup_sort(df_NotifyDDup_in, ["NOTIFICATION"], [])

df_seq_RCVD_INCM_F_Notify_csv_out = (
    df_cpy_DDup
    .withColumn("NOTIFICATION", rpad("NOTIFICATION", 100, " "))
    .select("NOTIFICATION")
)

write_files(
    df_seq_RCVD_INCM_F_Notify_csv_out,
    f"{adls_path}/balancing/notify/IncomeBalancingNotification.dat",
    ",",
    "append",
    False,
    False,
    "^",
    None
)

query_db2_RCVD_INCM_F_Matching_in = f"""SELECT
RcvdIncmF.FNCL_LOB_CD
FROM {EDWOwner}.RCVD_INCM_F RcvdIncmF
INNER JOIN {EDWOwner}.B_RCVD_INCM_F BRcvdIncm
ON RcvdIncmF.SRC_SYS_CD = BRcvdIncm.SRC_SYS_CD
AND RcvdIncmF.BILL_ENTY_UNIQ_KEY = BRcvdIncm.BILL_ENTY_UNIQ_KEY
AND RcvdIncmF.DUE_YR_MO_SK = BRcvdIncm.DUE_YR_MO_SK
AND RcvdIncmF.BILL_INCM_RCPT_BILL_CNTR_ID = BRcvdIncm.BILL_INCM_RCPT_BILL_CNTR_ID
AND RcvdIncmF.RCPT_YR_MO_SK = BRcvdIncm.RCPT_YR_MO_SK
AND RcvdIncmF.BILL_INCM_RCPT_CRT_DTM = BRcvdIncm.BILL_INCM_RCPT_CRT_DTM
WHERE RcvdIncmF.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
AND RcvdIncmF.FNCL_LOB_CD = BRcvdIncm.FNCL_LOB_CD
"""

df_db2_RCVD_INCM_F_Matching_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_db2_RCVD_INCM_F_Matching_in)
    .load()
)

df_seq_RCVD_INCM_F_Matching_csv_out = df_db2_RCVD_INCM_F_Matching_in.select("FNCL_LOB_CD")

write_files(
    df_seq_RCVD_INCM_F_Matching_csv_out,
    f"{adls_path}/balancing/sync/RcvdIncmFBalancingTotalMatch.dat",
    ",",
    "overwrite",
    False,
    False,
    "^",
    None
)