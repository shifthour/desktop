# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:          IdsEdwFeeDscntDBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target tables are compared on the specified column to see if there are any discrepancies
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               07/09/2007          3264                              Originally Programmed                          devlEDW10               Steph Goddard             10/26/2007    
# MAGIC 
# MAGIC Srikanth Mettpalli               12/18/2013         5114                              Originally Programmed                         EnterpriseWrhsDevl   Peter Marshall               12/26/2013    
# MAGIC                                                                                                             (Server to Parallel Conversion)

# MAGIC Pull all the Matching Records from FEE_DSCNT_D and B_FEE_DSCNT_D  Table.
# MAGIC Job Name: IdsEdwFeeDscntDBal
# MAGIC Write all the Matching Records from FEE_DSCNT_D and B_FEE_DSCNT_D Table.
# MAGIC Copy stage used to remove duplicates and create one record for the Notification file.
# MAGIC File checked later for rows and email to on-call
# MAGIC Detail file for on-call to research errors
# MAGIC Rows with Failed Comparison on Specified columns
# MAGIC Pull all the Missing Records from FEE_DSCNT_D and B_FEE_DSCNT_D Table.
# MAGIC IDS - EDW Fee Discount Dim Row To Row Comparisons
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
EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
RunID = get_widget_value('RunID','')

jdbc_url, jdbc_props = get_db_config(edw_secret_name)
df_db2_FEE_DSCNT_D_Missing_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
SELECT
    FeeDscntD.SRC_SYS_CD,
    FeeDscntD.FEE_DSCNT_ID,
    FeeDscntD.FNCL_LOB_SK
FROM {EDWOwner}.FEE_DSCNT_D FeeDscntD
FULL OUTER JOIN {EDWOwner}.B_FEE_DSCNT_D BFeeDscnt
    ON FeeDscntD.SRC_SYS_CD = BFeeDscnt.SRC_SYS_CD
    AND FeeDscntD.FEE_DSCNT_ID = BFeeDscnt.FEE_DSCNT_ID
WHERE
    FeeDscntD.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
    AND FeeDscntD.FNCL_LOB_SK <> BFeeDscnt.FNCL_LOB_SK
"""
    )
    .load()
)

df_xfrm_BusinessLogic_1 = df_db2_FEE_DSCNT_D_Missing_in.select(
    "SRC_SYS_CD",
    "FEE_DSCNT_ID",
    "FNCL_LOB_SK"
)

write_files(
    df_xfrm_BusinessLogic_1,
    f"{adls_path}/balancing/research/IdsEdwFeeDscntDRowToRowResearch.dat.{RunID}",
    ",",
    "overwrite",
    False,
    False,
    "^",
    None
)

df_xfrm_BusinessLogic_2 = df_db2_FEE_DSCNT_D_Missing_in.limit(1).select(
    F.lit("ROW TO ROW BALANCING IDS - EDW FEE DSCNT D OUT OF TOLERANCE").alias("NOTIFICATION")
)

df_cpy_DDup = dedup_sort(
    df_xfrm_BusinessLogic_2,
    ["NOTIFICATION"],
    [("NOTIFICATION", "A")]
)

df_cpy_DDup_padded = df_cpy_DDup.select(
    F.rpad(F.col("NOTIFICATION"), 100, " ").alias("NOTIFICATION")
)

write_files(
    df_cpy_DDup_padded,
    f"{adls_path}/balancing/notify/IncomeBalancingNotification.dat",
    ",",
    "append",
    False,
    False,
    "^",
    None
)

df_db2_FEE_DSCNT_D_Matching_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
SELECT
    FeeDscntD.FNCL_LOB_SK
FROM {EDWOwner}.FEE_DSCNT_D FeeDscntD
INNER JOIN {EDWOwner}.B_FEE_DSCNT_D BFeeDscnt
    ON FeeDscntD.SRC_SYS_CD = BFeeDscnt.SRC_SYS_CD
    AND FeeDscntD.FEE_DSCNT_ID = BFeeDscnt.FEE_DSCNT_ID
WHERE
    FeeDscntD.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
    AND FeeDscntD.FNCL_LOB_SK = BFeeDscnt.FNCL_LOB_SK
"""
    )
    .load()
)

write_files(
    df_db2_FEE_DSCNT_D_Matching_in,
    f"{adls_path}/balancing/sync/FeeDscntDBalancingTotalMatch.dat",
    ",",
    "overwrite",
    False,
    False,
    "^",
    None
)