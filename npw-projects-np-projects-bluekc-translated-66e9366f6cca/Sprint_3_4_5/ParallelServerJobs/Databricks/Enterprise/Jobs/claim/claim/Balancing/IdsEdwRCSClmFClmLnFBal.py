# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:          IdsEdwRCSClmFClmLnFBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target tables are compared on the specified column to see if there are any discrepancies
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                                  Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                            ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               09/25/2007          3264                              Originally Programmed                                                 devlEDW10              Steph Goddard            10/22/2007                
# MAGIC 
# MAGIC Shanmugam A.                  03/08/2017          5321                       Updated Aliase for all Sum function used columns          EnterpriseDev2            Jag Yelavarthi              2017-03-08
# MAGIC                                                                                                            for link ClmLnSum in SrcTrgtRowComp stage

# MAGIC Rows with Failed Comparison on Specified columns
# MAGIC Detail file for on-call to research errors
# MAGIC File checked later for rows and email to on-call
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

edw_secret_name = get_widget_value('edw_secret_name','')
EDWOwner = get_widget_value('EDWOwner','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
RunID = get_widget_value('RunID','')

jdbc_url, jdbc_props = get_db_config(edw_secret_name)

extract_query_ClmFExtr = f"""
SELECT
 CLM_F.CLM_SK AS CLM_SK,
 CLM_F.CLM_LN_TOT_ALW_AMT AS CLM_LN_TOT_ALW_AMT,
 CLM_F.CLM_LN_TOT_CHRG_AMT AS CLM_LN_TOT_CHRG_AMT,
 CLM_F.CLM_LN_TOT_COINS_AMT AS CLM_LN_TOT_COINS_AMT,
 CLM_F.CLM_LN_TOT_CNSD_CHRG_AMT AS CLM_LN_TOT_CNSD_CHRG_AMT,
 CLM_F.CLM_LN_TOT_COPAY_AMT AS CLM_LN_TOT_COPAY_AMT,
 CLM_F.CLM_LN_TOT_DEDCT_AMT AS CLM_LN_TOT_DEDCT_AMT,
 CLM_F.CLM_LN_TOT_DSALW_AMT AS CLM_LN_TOT_DSALW_AMT,
 CLM_F.CLM_LN_TOT_PAYBL_AMT AS CLM_LN_TOT_PAYBL_AMT,
 CLM_F.CLM_LN_TOT_PAYBL_TO_PROV_AMT AS CLM_LN_TOT_PAYBL_TO_PROV_AMT,
 CLM_F.CLM_LN_TOT_PAYBL_TO_SUB_AMT AS CLM_LN_TOT_PAYBL_TO_SUB_AMT
FROM {EDWOwner}.CLM_F CLM_F
WHERE CLM_F.LAST_UPDT_RUN_CYC_EXCTN_SK = {ExtrRunCycle}
"""

df_SrcTrgtRowComp_ClmFExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_ClmFExtr)
    .load()
)

extract_query_ClmLnSum = f"""
SELECT
 CLM_LN_F.CLM_SK AS CLM_SK,
 SUM(CLM_LN_F.CLM_LN_ALW_AMT) AS CLM_LN_ALW_AMT,
 SUM(CLM_LN_F.CLM_LN_CHRG_AMT) AS CLM_LN_CHRG_AMT,
 SUM(CLM_LN_F.CLM_LN_COINS_AMT) AS CLM_LN_COINS_AMT,
 SUM(CLM_LN_F.CLM_LN_CNSD_CHRG_AMT) AS CLM_LN_CNSD_CHRG_AMT,
 SUM(CLM_LN_F.CLM_LN_COPAY_AMT) AS CLM_LN_COPAY_AMT,
 SUM(CLM_LN_F.CLM_LN_DEDCT_AMT) AS CLM_LN_DEDCT_AMT,
 SUM(CLM_LN_F.CLM_LN_DSALW_AMT) AS CLM_LN_DSALW_AMT,
 SUM(CLM_LN_F.CLM_LN_PAYBL_AMT) AS CLM_LN_PAYBL_AMT,
 SUM(CLM_LN_F.CLM_LN_PAYBL_TO_PROV_AMT) AS CLM_LN_PAYBL_TO_PROV_AMT,
 SUM(CLM_LN_F.CLM_LN_PAYBL_TO_SUB_AMT) AS CLM_LN_PAYBL_TO_SUB_AMT
FROM {EDWOwner}.CLM_LN_F CLM_LN_F
WHERE CLM_LN_F.LAST_UPDT_RUN_CYC_EXCTN_SK = {ExtrRunCycle}
GROUP BY CLM_LN_F.CLM_SK
"""

df_SrcTrgtRowComp_ClmLnSum = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_ClmLnSum)
    .load()
)

df_hf_rel_clm_sum_clm_ln_lkup = dedup_sort(df_SrcTrgtRowComp_ClmLnSum, ["CLM_SK"], [])

df_TransformLogic_join = df_SrcTrgtRowComp_ClmFExtr.alias("ClmFExtr").join(
    df_hf_rel_clm_sum_clm_ln_lkup.alias("ClmLnSumLkup"),
    F.col("ClmFExtr.CLM_SK") == F.col("ClmLnSumLkup.CLM_SK"),
    "left"
)

transform_condition = (
    F.isnull(F.col("ClmLnSumLkup.CLM_SK"))
    | (F.col("ClmLnSumLkup.CLM_LN_ALW_AMT") != F.col("ClmFExtr.CLM_LN_TOT_ALW_AMT"))
    | (F.col("ClmLnSumLkup.CLM_LN_CHRG_AMT") != F.col("ClmFExtr.CLM_LN_TOT_CHRG_AMT"))
    | (F.col("ClmLnSumLkup.CLM_LN_COINS_AMT") != F.col("ClmFExtr.CLM_LN_TOT_COINS_AMT"))
    | (F.col("ClmLnSumLkup.CLM_LN_CNSD_CHRG_AMT") != F.col("ClmFExtr.CLM_LN_TOT_CNSD_CHRG_AMT"))
    | (F.col("ClmLnSumLkup.CLM_LN_COPAY_AMT") != F.col("ClmFExtr.CLM_LN_TOT_COPAY_AMT"))
    | (F.col("ClmLnSumLkup.CLM_LN_DEDCT_AMT") != F.col("ClmFExtr.CLM_LN_TOT_DEDCT_AMT"))
    | (F.col("ClmLnSumLkup.CLM_LN_DSALW_AMT") != F.col("ClmFExtr.CLM_LN_TOT_DSALW_AMT"))
    | (F.col("ClmLnSumLkup.CLM_LN_PAYBL_AMT") != F.col("ClmFExtr.CLM_LN_TOT_PAYBL_AMT"))
    | (F.col("ClmLnSumLkup.CLM_LN_PAYBL_TO_PROV_AMT") != F.col("ClmFExtr.CLM_LN_TOT_PAYBL_TO_PROV_AMT"))
    | (F.col("ClmLnSumLkup.CLM_LN_PAYBL_TO_SUB_AMT") != F.col("ClmFExtr.CLM_LN_TOT_PAYBL_TO_SUB_AMT"))
)

df_TransformLogic_Missing = (
    df_TransformLogic_join
    .filter(transform_condition)
    .select(
        F.col("ClmFExtr.CLM_SK").alias("CLM_SK"),
        F.col("ClmFExtr.CLM_LN_TOT_ALW_AMT").alias("CLM_LN_TOT_ALW_AMT"),
        F.col("ClmFExtr.CLM_LN_TOT_CHRG_AMT").alias("CLM_LN_TOT_CHRG_AMT"),
        F.col("ClmFExtr.CLM_LN_TOT_COINS_AMT").alias("CLM_LN_TOT_COINS_AMT"),
        F.col("ClmFExtr.CLM_LN_TOT_CNSD_CHRG_AMT").alias("CLM_LN_TOT_CNSD_CHRG_AMT"),
        F.col("ClmFExtr.CLM_LN_TOT_COPAY_AMT").alias("CLM_LN_TOT_COPAY_AMT"),
        F.col("ClmFExtr.CLM_LN_TOT_DEDCT_AMT").alias("CLM_LN_TOT_DEDCT_AMT"),
        F.col("ClmFExtr.CLM_LN_TOT_DSALW_AMT").alias("CLM_LN_TOT_DSALW_AMT"),
        F.col("ClmFExtr.CLM_LN_TOT_PAYBL_AMT").alias("CLM_LN_TOT_PAYBL_AMT"),
        F.col("ClmFExtr.CLM_LN_TOT_PAYBL_TO_PROV_AMT").alias("CLM_LN_TOT_PAYBL_TO_PROV_AMT"),
        F.col("ClmFExtr.CLM_LN_TOT_PAYBL_TO_SUB_AMT").alias("CLM_LN_TOT_PAYBL_TO_SUB_AMT")
    )
)

df_Transformer_11_with_rn = df_TransformLogic_Missing.withColumn(
    "rownum", F.row_number().over(Window.orderBy(F.monotonically_increasing_id()))
)

df_Transformer_11_Output = df_Transformer_11_with_rn.select(
    "CLM_SK",
    "CLM_LN_TOT_ALW_AMT",
    "CLM_LN_TOT_CHRG_AMT",
    "CLM_LN_TOT_COINS_AMT",
    "CLM_LN_TOT_CNSD_CHRG_AMT",
    "CLM_LN_TOT_COPAY_AMT",
    "CLM_LN_TOT_DEDCT_AMT",
    "CLM_LN_TOT_DSALW_AMT",
    "CLM_LN_TOT_PAYBL_AMT",
    "CLM_LN_TOT_PAYBL_TO_PROV_AMT",
    "CLM_LN_TOT_PAYBL_TO_SUB_AMT",
    "rownum"
)

df_Transformer_11_Notify = (
    df_Transformer_11_Output
    .filter(F.col("rownum") == 1)
    .selectExpr(
        "'RELATIONSHIP COLUMN SUM BALANCING IDS - EDW CLM F AND CLM LN F OUT OF TOLERANCE FOR AMOUNT FIELDS' AS NOTIFICATION"
    )
)

df_Transformer_11_Notify = df_Transformer_11_Notify.withColumn(
    "NOTIFICATION",
    F.rpad(F.col("NOTIFICATION"), 70, " ")
)

write_files(
    df_Transformer_11_Notify.select("NOTIFICATION"),
    f"{adls_path}/balancing/notify/ClaimsBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_Transformer_11_FinalOutput = df_Transformer_11_Output.select(
    "CLM_SK",
    "CLM_LN_TOT_ALW_AMT",
    "CLM_LN_TOT_CHRG_AMT",
    "CLM_LN_TOT_COINS_AMT",
    "CLM_LN_TOT_CNSD_CHRG_AMT",
    "CLM_LN_TOT_COPAY_AMT",
    "CLM_LN_TOT_DEDCT_AMT",
    "CLM_LN_TOT_DSALW_AMT",
    "CLM_LN_TOT_PAYBL_AMT",
    "CLM_LN_TOT_PAYBL_TO_PROV_AMT",
    "CLM_LN_TOT_PAYBL_TO_SUB_AMT"
)

write_files(
    df_Transformer_11_FinalOutput,
    f"{adls_path}/balancing/research/IdsEdwClmFClmLnFRelationshipColumnSumResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)