# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:          IdsEdwProdDBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target tables are compared on the specified column to see if there are any discrepancies
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               07/19/2007          3264                              Originally Programmed                           devlEDW10              Steph Goddard             10/08/2007      
# MAGIC 
# MAGIC Aditya Raju                       06/13/2013          5114                              Originally Programmed                         EnterpriseWrhsDevl     Peter Marshall               8/29/2013  
# MAGIC                                                                                                             (Server to Parallel Conversion)

# MAGIC Pull all the Matching Records from PROV_D and B_PROV_D Table.
# MAGIC Pull all the Missing Records from PROV_D and B_PROV_D Table.
# MAGIC Job Name: IdsEdwBProvDBal
# MAGIC IDS - EDW Product Dim Row To Row Comparisons
# MAGIC Rows with Failed Comparison on Specified columns
# MAGIC Write all the Matching Records from PROV_D and B_PROV_D Table.
# MAGIC Detail file for on-call to research errors
# MAGIC File checked later for rows and email to on-call
# MAGIC Copy stage used to remove duplicates and create one record for the Notification file.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../../../Utility_Enterprise
# COMMAND ----------


EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
RunID = get_widget_value('RunID','')

jdbc_url, jdbc_props = get_db_config(edw_secret_name)

# ----------------------------------------------------------------------------
# Stage: db2_Prov_D_Matching_in
# ----------------------------------------------------------------------------
extract_query_1 = f"""SELECT
ProvD.CMN_PRCT_ID,
ProvD.PROV_ENTY_NM,
ProvD.PROV_REL_GRP_PROV_ID,
ProvD.PROV_REL_IPA_PROV_ID
FROM {EDWOwner}.PROV_D ProvD INNER JOIN {EDWOwner}.B_PROV_D BProv
ON ProvD.SRC_SYS_CD = BProv.SRC_SYS_CD 
AND ProvD.PROV_ID = BProv.PROV_ID
WHERE ProvD.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
AND ProvD.CMN_PRCT_ID = BProv.CMN_PRCT_ID
AND ProvD.PROV_ENTY_NM = BProv.PROV_ENTY_NM
AND ProvD.PROV_REL_GRP_PROV_ID = BProv.PROV_REL_GRP_PROV_ID
AND ProvD.PROV_REL_IPA_PROV_ID = BProv.PROV_REL_IPA_PROV_ID
"""

df_db2_Prov_D_Matching_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_1)
    .load()
)

# ----------------------------------------------------------------------------
# Stage: seq_PROV_D_Matching_csv_out
# ----------------------------------------------------------------------------
df_seq_PROV_D_Matching_csv_out = df_db2_Prov_D_Matching_in.select(
    "CMN_PRCT_ID",
    "PROV_ENTY_NM",
    "PROV_REL_GRP_PROV_ID",
    "PROV_REL_IPA_PROV_ID"
)

file_path_1 = f"{adls_path}/balancing/sync/ProvDBalancingTotalMatch.dat"
write_files(
    df_seq_PROV_D_Matching_csv_out,
    file_path_1,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=" "
)

# ----------------------------------------------------------------------------
# Stage: db2_Prov_D_Missing_in
# ----------------------------------------------------------------------------
extract_query_2 = f"""SELECT
ProvD.SRC_SYS_CD,
ProvD.PROV_ID,
ProvD.CMN_PRCT_ID,
ProvD.PROV_ENTY_NM,
ProvD.PROV_REL_GRP_PROV_ID,
ProvD.PROV_REL_IPA_PROV_ID
FROM {EDWOwner}.PROV_D ProvD FULL OUTER JOIN {EDWOwner}.B_PROV_D BProv
ON ProvD.SRC_SYS_CD = BProv.SRC_SYS_CD
AND ProvD.PROV_ID = BProv.PROV_ID
WHERE ProvD.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
AND (
   ProvD.CMN_PRCT_ID <> BProv.CMN_PRCT_ID
   OR ProvD.PROV_ENTY_NM <> BProv.PROV_ENTY_NM
   OR ProvD.PROV_REL_GRP_PROV_ID <> BProv.PROV_REL_GRP_PROV_ID
   OR ProvD.PROV_REL_IPA_PROV_ID <> BProv.PROV_REL_IPA_PROV_ID
)
"""

df_db2_Prov_D_Missing_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_2)
    .load()
)

# ----------------------------------------------------------------------------
# Stage: xfrm_BusinessLogic
# ----------------------------------------------------------------------------
df_xfrm_BusinessLogic_out_1 = df_db2_Prov_D_Missing_in.select(
    "SRC_SYS_CD",
    "PROV_ID",
    "CMN_PRCT_ID",
    "PROV_ENTY_NM",
    "PROV_REL_GRP_PROV_ID",
    "PROV_REL_IPA_PROV_ID"
)

df_tmp = df_db2_Prov_D_Missing_in.limit(1)
df_xfrm_BusinessLogic_out_2 = df_tmp.select(
    F.lit("ROW TO ROW BALANCING IDS - EDW PROV D OUT OF TOLERANCE").alias("NOTIFICATION")
)
df_xfrm_BusinessLogic_out_2 = df_xfrm_BusinessLogic_out_2.withColumn(
    "NOTIFICATION", F.rpad("NOTIFICATION", 70, " ")
)

# ----------------------------------------------------------------------------
# Stage: seq_PROV_D_Error_csv_out
# ----------------------------------------------------------------------------
df_seq_PROV_D_Error_csv_out = df_xfrm_BusinessLogic_out_1.select(
    "SRC_SYS_CD",
    "PROV_ID",
    "CMN_PRCT_ID",
    "PROV_ENTY_NM",
    "PROV_REL_GRP_PROV_ID",
    "PROV_REL_IPA_PROV_ID"
)
file_path_2 = f"{adls_path}/balancing/research/IdsEdwProvDRowToRowResearch.dat.{RunID}"
write_files(
    df_seq_PROV_D_Error_csv_out,
    file_path_2,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=" "
)

# ----------------------------------------------------------------------------
# Stage: cpy_DDup
# ----------------------------------------------------------------------------
df_cpy_DDup = dedup_sort(
    df_xfrm_BusinessLogic_out_2,
    ["NOTIFICATION"],
    [("NOTIFICATION", "A")]
)

# ----------------------------------------------------------------------------
# Stage: seq_PROV_D_Notify_csv_out
# ----------------------------------------------------------------------------
df_seq_PROV_D_Notify_csv_out = df_cpy_DDup.select(
    F.rpad("NOTIFICATION", 70, " ").alias("NOTIFICATION")
)
file_path_3 = f"{adls_path}/balancing/notify/ProviderBalancingNotification.dat"
write_files(
    df_seq_PROV_D_Notify_csv_out,
    file_path_3,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)