# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:          IdsEdwRowToRowDscrtnIncmFBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target tables are compared on the specified column to see if there are any discrepancies
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               07/27/2007          3264                              Originally Programmed                           devlEDW10              Steph Goddard            10/26/2007      
# MAGIC 
# MAGIC Srikanth Mettpalli               12/18/2013         5114                              Originally Programmed                         EnterpriseWrhsDevl    Peter Marshall              12/26/2013   
# MAGIC                                                                                                             (Server to Parallel Conversion)

# MAGIC Pull all the Matching Records from DSCRTN_INCM_F and B_DSCRTN_INCM_F  Table.
# MAGIC Job Name: IdsEdwRowToRowDscrtnIncmFBal
# MAGIC Write all the Matching Records from DSCRTN_INCM_F and B_DSCRTN_INCM_F Table.
# MAGIC Copy stage used to remove duplicates and create one record for the Notification file.
# MAGIC File checked later for rows and email to on-call
# MAGIC Detail file for on-call to research errors
# MAGIC Rows with Failed Comparison on Specified columns
# MAGIC Pull all the Missing Records from DSCRTN_INCM_F and B_DSCRTN_INCM_F Table.
# MAGIC IDS - EDW Discretionary Income Facts Row To Row Comparisons
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lit, rpad, row_number
# COMMAND ----------
# MAGIC %run ../../../../../../../Utility_Enterprise
# COMMAND ----------

jdbc_url, jdbc_props = None, None

ExtrRunCycle = get_widget_value('ExtrRunCycle','')
RunID = get_widget_value('RunID','')
EDWOwner = get_widget_value('$EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')

jdbc_url, jdbc_props = get_db_config(edw_secret_name)
df_db2_DSCRTN_INCM_F_Missing_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
SELECT
DSCRTN_INCM_F.SRC_SYS_CD,
DSCRTN_INCM_F.BILL_INVC_ID,
DSCRTN_INCM_F.INVC_DSCRTN_SEQ_NO,
DSCRTN_INCM_F.INVC_DSCRTN_YR_MO_SK,
DSCRTN_INCM_F.GRP_ID,
DSCRTN_INCM_F.SUBGRP_ID,
DSCRTN_INCM_F.CLS_ID,
DSCRTN_INCM_F.CLS_PLN_ID,
DSCRTN_INCM_F.PROD_ID,
DSCRTN_INCM_F.INVC_TYP_CD
FROM {EDWOwner}.DSCRTN_INCM_F DSCRTN_INCM_F
FULL OUTER JOIN {EDWOwner}.B_DSCRTN_INCM_F B_DSCRTN_INCM_F
  ON DSCRTN_INCM_F.SRC_SYS_CD = B_DSCRTN_INCM_F.SRC_SYS_CD
  AND DSCRTN_INCM_F.BILL_INVC_ID = B_DSCRTN_INCM_F.BILL_INVC_ID
  AND DSCRTN_INCM_F.INVC_DSCRTN_SEQ_NO = B_DSCRTN_INCM_F.INVC_DSCRTN_SEQ_NO
  AND DSCRTN_INCM_F.INVC_DSCRTN_YR_MO_SK = B_DSCRTN_INCM_F.INVC_DSCRTN_YR_MO_SK
  AND DSCRTN_INCM_F.GRP_ID = B_DSCRTN_INCM_F.GRP_ID
  AND DSCRTN_INCM_F.SUBGRP_ID = B_DSCRTN_INCM_F.SUBGRP_ID
  AND DSCRTN_INCM_F.CLS_ID = B_DSCRTN_INCM_F.CLS_ID
  AND DSCRTN_INCM_F.CLS_PLN_ID = B_DSCRTN_INCM_F.CLS_PLN_ID
  AND DSCRTN_INCM_F.PROD_ID = B_DSCRTN_INCM_F.PROD_ID
WHERE
DSCRTN_INCM_F.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
AND DSCRTN_INCM_F.INVC_TYP_CD <> B_DSCRTN_INCM_F.INVC_TYP_CD
"""
    )
    .load()
)

df_error = df_db2_DSCRTN_INCM_F_Missing_in.select(
    col("SRC_SYS_CD"),
    col("BILL_INVC_ID"),
    col("INVC_DSCRTN_SEQ_NO"),
    rpad(col("INVC_DSCRTN_YR_MO_SK"), 6, " ").alias("INVC_DSCRTN_YR_MO_SK"),
    col("GRP_ID"),
    col("SUBGRP_ID"),
    col("CLS_ID"),
    col("CLS_PLN_ID"),
    col("PROD_ID"),
    col("INVC_TYP_CD")
)

df_notify = df_db2_DSCRTN_INCM_F_Missing_in.limit(1).select(
    rpad(lit("ROW TO ROW BALANCING IDS - EDW DSCRTN INCM F OUT OF TOLERANCE"), 100, " ").alias("NOTIFICATION")
)

write_files(
    df_error,
    f"{adls_path}/balancing/research/IdsEdwDscrtnIncmFRowToRowResearch.dat.{RunID}",
    ",",
    "overwrite",
    False,
    False,
    "^",
    None
)

df_notify_dedup = dedup_sort(
    df_notify,
    ["NOTIFICATION"],
    [("NOTIFICATION","A")]
)

df_notify_dedup = df_notify_dedup.select(
    rpad(col("NOTIFICATION"), 100, " ").alias("NOTIFICATION")
)

write_files(
    df_notify_dedup,
    f"{adls_path}/balancing/notify/IncomeBalancingNotification.dat",
    ",",
    "append",
    False,
    False,
    "^",
    None
)

jdbc_url, jdbc_props = get_db_config(edw_secret_name)
df_db2_DSCRTN_INCM_F_Matching_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
SELECT
DSCRTN_INCM_F.INVC_TYP_CD
FROM {EDWOwner}.DSCRTN_INCM_F DSCRTN_INCM_F
INNER JOIN {EDWOwner}.B_DSCRTN_INCM_F B_DSCRTN_INCM_F
  ON DSCRTN_INCM_F.SRC_SYS_CD = B_DSCRTN_INCM_F.SRC_SYS_CD
  AND DSCRTN_INCM_F.BILL_INVC_ID = B_DSCRTN_INCM_F.BILL_INVC_ID
  AND DSCRTN_INCM_F.INVC_DSCRTN_SEQ_NO = B_DSCRTN_INCM_F.INVC_DSCRTN_SEQ_NO
  AND DSCRTN_INCM_F.INVC_DSCRTN_YR_MO_SK = B_DSCRTN_INCM_F.INVC_DSCRTN_YR_MO_SK
  AND DSCRTN_INCM_F.GRP_ID = B_DSCRTN_INCM_F.GRP_ID
  AND DSCRTN_INCM_F.SUBGRP_ID = B_DSCRTN_INCM_F.SUBGRP_ID
  AND DSCRTN_INCM_F.CLS_ID = B_DSCRTN_INCM_F.CLS_ID
  AND DSCRTN_INCM_F.CLS_PLN_ID = B_DSCRTN_INCM_F.CLS_PLN_ID
  AND DSCRTN_INCM_F.PROD_ID = B_DSCRTN_INCM_F.PROD_ID
WHERE
DSCRTN_INCM_F.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
AND DSCRTN_INCM_F.INVC_TYP_CD = B_DSCRTN_INCM_F.INVC_TYP_CD
"""
    )
    .load()
)

write_files(
    df_db2_DSCRTN_INCM_F_Matching_in,
    f"{adls_path}/balancing/sync/DscrtnIncmFBalancingTotalMatch.dat",
    ",",
    "overwrite",
    False,
    False,
    "^",
    None
)