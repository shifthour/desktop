# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:          IdsEdwMbrDBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target tables are compared on the specified column to see if there are any discrepancies
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               07/18/2007          3264                              Originally Programmed                           devlEDW10               Steph Goddard            10/08/2007       
# MAGIC 
# MAGIC Srikanth Mettpalli               07/24/2013         5114                              Originally Programmed                         EnterpriseWrhsDevl       
# MAGIC                                                                                                             (Server to Parallel Conversion)

# MAGIC Pull all the Matching Records from MBR_D and B_MBR_D Table.
# MAGIC Job Name: IdsEdwBMbrDBal
# MAGIC IDS - EDW Membership Dim Row To Row Comparisons
# MAGIC Write all the Matching Records from NBR_D and B_MBR_D Table.
# MAGIC Copy stage used to remove duplicates and create one record for the Notification file.
# MAGIC File checked later for rows and email to on-call
# MAGIC Detail file for on-call to research errors
# MAGIC Rows with Failed Comparison on Specified columns
# MAGIC Pull all the Missing Records from MBR_D and B_MBR_D Table.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../../../Utility_Enterprise
# COMMAND ----------


ExtrRunCycle = get_widget_value('ExtrRunCycle','')
EDWOwner = get_widget_value('$EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
RunID = get_widget_value('RunID','')

jdbc_url_db2_MBR_D_Missing_in, jdbc_props_db2_MBR_D_Missing_in = get_db_config(edw_secret_name)
extract_query_db2_MBR_D_Missing_in = f"""
SELECT
MBR_D.SRC_SYS_CD,
MBR_D.MBR_UNIQ_KEY,
MBR_D.CLS_ID,
MBR_D.GRP_ID,
MBR_D.SUBGRP_ID,
MBR_D.SUB_UNIQ_KEY

FROM {EDWOwner}.MBR_D MBR_D
FULL OUTER JOIN {EDWOwner}.B_MBR_D B_MBR_D
ON MBR_D.SRC_SYS_CD = B_MBR_D.SRC_SYS_CD
AND MBR_D.MBR_UNIQ_KEY = B_MBR_D.MBR_UNIQ_KEY

WHERE
MBR_D.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
AND
(
  MBR_D.CLS_ID <> B_MBR_D.CLS_ID
  OR MBR_D.GRP_ID <> B_MBR_D.GRP_ID
  OR MBR_D.SUBGRP_ID <> B_MBR_D.SUBGRP_ID
  OR MBR_D.SUB_UNIQ_KEY <> B_MBR_D.SUB_UNIQ_KEY
)
"""
df_db2_MBR_D_Missing_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_MBR_D_Missing_in)
    .options(**jdbc_props_db2_MBR_D_Missing_in)
    .option("query", extract_query_db2_MBR_D_Missing_in)
    .load()
)

df_lnk_IdsEdwBMbrDBal_Error_InABC = df_db2_MBR_D_Missing_in.select(
    F.col("SRC_SYS_CD"),
    F.col("MBR_UNIQ_KEY"),
    F.col("CLS_ID"),
    F.col("GRP_ID"),
    F.col("SUBGRP_ID"),
    F.col("SUB_UNIQ_KEY")
)

df_lnk_NotifyDDup_in = (
    df_db2_MBR_D_Missing_in.limit(1)
    .select(
        F.lit("ROW TO ROW BALANCING IDS - EDW MBR D OUT OF TOLERANCE").alias("NOTIFICATION")
    )
)

df_seq_MBR_D_Error_csv_out = df_lnk_IdsEdwBMbrDBal_Error_InABC.select(
    "SRC_SYS_CD",
    "MBR_UNIQ_KEY",
    "CLS_ID",
    "GRP_ID",
    "SUBGRP_ID",
    "SUB_UNIQ_KEY"
)
write_files(
    df_seq_MBR_D_Error_csv_out,
    f"{adls_path}/balancing/research/IdsEdwMbrDRowToRowResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

df_cpy_DDup = dedup_sort(
    df_lnk_NotifyDDup_in,
    ["NOTIFICATION"],
    [("NOTIFICATION", "A")]
)

df_seq_MBR_D_Notify_csv_out = df_cpy_DDup.withColumn(
    "NOTIFICATION", F.rpad(F.col("NOTIFICATION"), 70, " ")
).select("NOTIFICATION")
write_files(
    df_seq_MBR_D_Notify_csv_out,
    f"{adls_path}/balancing/notify/MembershipBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

jdbc_url_db2_MBR_D_Matching_in, jdbc_props_db2_MBR_D_Matching_in = get_db_config(edw_secret_name)
extract_query_db2_MBR_D_Matching_in = f"""
SELECT
MBR_D.CLS_ID,
MBR_D.GRP_ID,
MBR_D.SUBGRP_ID,
MBR_D.SUB_UNIQ_KEY

FROM {EDWOwner}.MBR_D MBR_D
INNER JOIN {EDWOwner}.B_MBR_D B_MBR_D
ON MBR_D.SRC_SYS_CD = B_MBR_D.SRC_SYS_CD
AND MBR_D.MBR_UNIQ_KEY = B_MBR_D.MBR_UNIQ_KEY

WHERE
MBR_D.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
AND MBR_D.CLS_ID = B_MBR_D.CLS_ID
AND MBR_D.GRP_ID = B_MBR_D.GRP_ID
AND MBR_D.SUBGRP_ID = B_MBR_D.SUBGRP_ID
AND MBR_D.SUB_UNIQ_KEY = B_MBR_D.SUB_UNIQ_KEY
"""
df_db2_MBR_D_Matching_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_MBR_D_Matching_in)
    .options(**jdbc_props_db2_MBR_D_Matching_in)
    .option("query", extract_query_db2_MBR_D_Matching_in)
    .load()
)

df_seq_MBR_D_Matching_csv_out = df_db2_MBR_D_Matching_in.select(
    "CLS_ID",
    "GRP_ID",
    "SUBGRP_ID",
    "SUB_UNIQ_KEY"
)
write_files(
    df_seq_MBR_D_Matching_csv_out,
    f"{adls_path}/balancing/sync/MbrDBalancingTotalMatch.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)