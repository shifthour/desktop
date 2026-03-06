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
# MAGIC Srikanth Mettpalli               06/13/2013         5114                              Originally Programmed                         EnterpriseWrhsDevl      Jag Yelavarthi              2013-08-11
# MAGIC                                                                                                             (Server to Parallel Conversion)

# MAGIC Pull all the Matching Records from PROD_D and B_PROD_D Table.
# MAGIC Pull all the Missing Records from PROD_D and B_PROD_D Table.
# MAGIC Job Name: IdsEdwBProdDBal
# MAGIC IDS - EDW Product Dim Row To Row Comparisons
# MAGIC Rows with Failed Comparison on Specified columns
# MAGIC Write all the Matching Records from PROD_D and B_PROD_D Table.
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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../../../../Utility_Enterprise
# COMMAND ----------

ExtrRunCycle = get_widget_value('ExtrRunCycle','')
EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
RunID = get_widget_value('RunID','')

jdbc_url_db2_Prod_D_Matching_in, jdbc_props_db2_Prod_D_Matching_in = get_db_config(edw_secret_name)
extract_query_db2_Prod_D_Matching_in = f"""SELECT
PROD_D.EXPRNC_CAT_CD,
PROD_D.FNCL_LOB_CD
FROM {EDWOwner}.PROD_D PROD_D
INNER JOIN {EDWOwner}.B_PROD_D B_PROD_D
ON PROD_D.SRC_SYS_CD = B_PROD_D.SRC_SYS_CD
AND PROD_D.PROD_ID = B_PROD_D.PROD_ID
WHERE
PROD_D.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
AND PROD_D.EXPRNC_CAT_CD = B_PROD_D.EXPRNC_CAT_CD
AND PROD_D.FNCL_LOB_CD = B_PROD_D.FNCL_LOB_CD"""
df_db2_Prod_D_Matching_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_Prod_D_Matching_in)
    .options(**jdbc_props_db2_Prod_D_Matching_in)
    .option("query", extract_query_db2_Prod_D_Matching_in)
    .load()
)
df_final_seq_PROD_D_Matching_csv_out = df_db2_Prod_D_Matching_in.select(
    "EXPRNC_CAT_CD",
    "FNCL_LOB_CD"
)
write_files(
    df_final_seq_PROD_D_Matching_csv_out,
    f"{adls_path}/balancing/sync/ProdDBalancingTotalMatch.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

jdbc_url_db2_Prod_D_Missing_in, jdbc_props_db2_Prod_D_Missing_in = get_db_config(edw_secret_name)
extract_query_db2_Prod_D_Missing_in = f"""SELECT
PROD_D.SRC_SYS_CD,
PROD_D.PROD_ID,
PROD_D.EXPRNC_CAT_CD,
PROD_D.FNCL_LOB_CD
FROM {EDWOwner}.PROD_D PROD_D
FULL OUTER JOIN {EDWOwner}.B_PROD_D B_PROD_D
ON PROD_D.SRC_SYS_CD = B_PROD_D.SRC_SYS_CD
AND PROD_D.PROD_ID = B_PROD_D.PROD_ID
WHERE
PROD_D.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
AND
(PROD_D.EXPRNC_CAT_CD <> B_PROD_D.EXPRNC_CAT_CD
OR PROD_D.FNCL_LOB_CD <> B_PROD_D.FNCL_LOB_CD)"""
df_db2_Prod_D_Missing_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_Prod_D_Missing_in)
    .options(**jdbc_props_db2_Prod_D_Missing_in)
    .option("query", extract_query_db2_Prod_D_Missing_in)
    .load()
)

df_xfrm_BusinessLogic_out1 = df_db2_Prod_D_Missing_in.select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("PROD_ID").alias("PROD_ID"),
    col("EXPRNC_CAT_CD").alias("EXPRNC_CAT_CD"),
    col("FNCL_LOB_CD").alias("FNCL_LOB_CD")
)

df_xfrm_BusinessLogic_out2 = df_db2_Prod_D_Missing_in.limit(1).select(
    rpad(lit("ROW TO ROW BALANCING IDS - EDW PROD D OUT OF TOLERANCE"), 70, " ").alias("NOTIFICATION")
)

df_final_seq_PROD_D_Error_csv_out = df_xfrm_BusinessLogic_out1.select(
    "SRC_SYS_CD",
    "PROD_ID",
    "EXPRNC_CAT_CD",
    "FNCL_LOB_CD"
)
write_files(
    df_final_seq_PROD_D_Error_csv_out,
    f"{adls_path}/balancing/research/IdsEdwProdDRowToRowResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

df_cpy_DDup = dedup_sort(
    df_xfrm_BusinessLogic_out2,
    partition_cols=["NOTIFICATION"],
    sort_cols=[("NOTIFICATION", "A")]
)

df_final_seq_PROD_D_Notify_csv_out = df_cpy_DDup.select(
    rpad(col("NOTIFICATION"), 70, " ").alias("NOTIFICATION")
)
write_files(
    df_final_seq_PROD_D_Notify_csv_out,
    f"{adls_path}/balancing/notify/ProductsBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)