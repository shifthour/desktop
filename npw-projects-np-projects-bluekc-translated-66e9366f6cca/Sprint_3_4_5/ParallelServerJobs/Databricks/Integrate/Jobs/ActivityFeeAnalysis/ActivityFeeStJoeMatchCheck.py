# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2020  Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC ZENA JOBNAME:  EDW_ACTIVITY_FEE_ANALYSIS_EXTRACT_REPORT_000
# MAGIC 
# MAGIC CONTROL JOB:  EdwActivityFeeStJosephRptCntl
# MAGIC 
# MAGIC JOB NAME:  ActivityFeeStJosephMatchCheck
# MAGIC 
# MAGIC Description:  Job reads St. Joseph School District Activity Fee Invoice from Optum and EDW database to compare differences and produce a report for the BlueKC Finance Team.  The report shows the active members under a given product/group on the Invoice and in the EDW database and displays the active member count (quantity status) of Match, No Match, or Quantity Mismatch.
# MAGIC 
# MAGIC Modifications:
# MAGIC 
# MAGIC Developer\(9)Date\(9)\(9)User Story       Change Description\(9)\(9)\(9)\(9)Development Project\(9)\(9)Code Reviewer            Date Reviewed
# MAGIC ==================================================================================================================================================================
# MAGIC Bill Schroeder\(9)  2021-09-13\(9)US-416640     Initial Programming.\(9)\(9)\(9)\(9)            IntegrateDev2\(9)\(9)\(9)
# MAGIC Bill Schroeder            2022-01-17            US-254834     Replaced lookup to table with lookup to file. Added Sort stage.   IntegrateDev2                            Raja Gummadi                 02/17/2022

# MAGIC Read Activity Fee Invoice from Optum
# MAGIC Find Group Id, Product Id, and Active Member counts in EDW from file created on 20th of the month (Optum Invoice cutoff date) that match the Invoice received on the last day of the month.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../Utility_Integrate
# COMMAND ----------


InFile = get_widget_value("InFile", "")
RunID = get_widget_value("RunID", "")

schema_ActivityFeeEdwStJoeExtr_2 = StructType([
    StructField("GRP_ID", StringType(), False),
    StructField("GRP_NM", StringType(), False),
    StructField("PROD_ID", StringType(), False),
    StructField("CNT", IntegerType(), False)
])

df_ActivityFeeEdwStJoeExtr_2 = (
    spark.read
    .option("delimiter", ",")
    .option("quote", "\"")
    .option("header", "false")
    .schema(schema_ActivityFeeEdwStJoeExtr_2)
    .csv(f"{adls_path_raw}/landing/ActivityFeeEdwStJoeExtr.txt")
)

df_Copy_54_Lnk_To_Copy2 = df_ActivityFeeEdwStJoeExtr_2.select(
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("GRP_NM").alias("GRP_NM"),
    F.col("PROD_ID").alias("PROD_ID"),
    F.col("CNT").alias("CNT")
)

df_Copy_54_Lnk_To_Copy = df_ActivityFeeEdwStJoeExtr_2.select(
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("GRP_NM").alias("GRP_NM"),
    F.col("PROD_ID").alias("PROD_ID"),
    F.col("CNT").alias("CNT")
)

schema_ActivityFee_StJoe_Invoice = StructType([
    StructField("INVOICE_NUMBER", StringType(), False),
    StructField("INVOICE_DATE", StringType(), False),
    StructField("BILLING_ENTITY_ID", StringType(), False),
    StructField("BILLING_ENTITY_NAME", StringType(), False),
    StructField("ACTIVITY_LEVEL", StringType(), False),
    StructField("CARRIER_ID", StringType(), False),
    StructField("CARRIER_NAME", StringType(), False),
    StructField("ACCOUNT_ID", StringType(), False),
    StructField("ACCOUNT_NAME", StringType(), False),
    StructField("GROUP_ID", StringType(), False),
    StructField("GROUP_NAME", StringType(), False),
    StructField("SERVICE_DATE_PERIOD", StringType(), False),
    StructField("ACTIVITY_CODE", StringType(), False),
    StructField("SUB_CODE", StringType(), False),
    StructField("ACTIVITY_DESCRIPTION", StringType(), False),
    StructField("QUANTITY", StringType(), False),
    StructField("UNIT_COST", StringType(), False),
    StructField("TOTAL_FEES", StringType(), False),
    StructField("ADDITIONAL_INFORMATION", StringType(), False)
])

df_ActivityFee_StJoe_Invoice = (
    spark.read
    .option("delimiter", ",")
    .option("quote", "\"")
    .option("header", "true")
    .schema(schema_ActivityFee_StJoe_Invoice)
    .csv(InFile)
)

df_Xfm1_pre = df_ActivityFee_StJoe_Invoice.filter(
    (F.col("GROUP_NAME").isNotNull()) &
    (F.col("GROUP_NAME") != "") &
    (trim(F.col("ACTIVITY_CODE")) == "PMPM")
)

df_Xfm1 = df_Xfm1_pre.select(
    strip_field(trim(Ereplace(F.col("INVOICE_NUMBER"), F.lit("\""), F.lit(" ")))).alias("INVOICE_NUM"),
    strip_field(trim(Ereplace(F.col("ACCOUNT_ID"), F.lit("\""), F.lit(" ")))).alias("ACCOUNT_ID"),
    strip_field(trim(Ereplace(F.col("GROUP_ID"), F.lit("\""), F.lit(" ")))).alias("GROUP_ID"),
    strip_field(trim(Ereplace(F.col("QUANTITY"), F.lit("\""), F.lit(" ")))).alias("QUANTITY")
)

df_Rm_Dup_GrpName = dedup_sort(
    df_Xfm1,
    ["GROUP_ID", "ACCOUNT_ID"],
    [("GROUP_ID", "A"), ("ACCOUNT_ID", "A")]
)

df_Rm_Dup_GrpName_LnkLkp = df_Rm_Dup_GrpName.select(
    F.col("INVOICE_NUM").alias("INVOICE_NUM"),
    F.col("ACCOUNT_ID").alias("ACCOUNT_ID"),
    F.col("GROUP_ID").alias("GROUP_ID"),
    F.col("QUANTITY").alias("QTY")
)

df_Lkp_Match_primary = df_Rm_Dup_GrpName_LnkLkp.alias("LnkLkp")
df_Lkp_Match_lookup = df_Copy_54_Lnk_To_Copy.alias("Lnk_To_Copy")
df_Lkp_Match_joined = df_Lkp_Match_primary.join(
    df_Lkp_Match_lookup,
    (F.col("LnkLkp.ACCOUNT_ID") == F.col("Lnk_To_Copy.GRP_ID")) &
    (F.col("LnkLkp.GROUP_ID") == F.col("Lnk_To_Copy.PROD_ID")) &
    (F.col("LnkLkp.QTY") == F.col("Lnk_To_Copy.CNT")),
    "left"
)

df_Lkp_Match_matched = df_Lkp_Match_joined.filter(F.col("Lnk_To_Copy.GRP_NM").isNotNull())
df_Lkp_Match_unmatched = df_Lkp_Match_joined.filter(F.col("Lnk_To_Copy.GRP_NM").isNull())

df_Lkp_Match_Lnk_MatchXmf = df_Lkp_Match_matched.select(
    F.col("LnkLkp.INVOICE_NUM").alias("INVOICE_NUM"),
    F.col("LnkLkp.ACCOUNT_ID").alias("ACCOUNT_ID"),
    F.col("LnkLkp.GROUP_ID").alias("GROUP_ID"),
    F.col("Lnk_To_Copy.GRP_NM").alias("GROUP_NAME"),
    F.col("LnkLkp.QTY").alias("SRC_QTY"),
    F.col("Lnk_To_Copy.CNT").alias("TBL_QTY")
)

df_Lkp_Match_Lnk_Rej = df_Lkp_Match_unmatched.select(
    F.col("LnkLkp.INVOICE_NUM").alias("INVOICE_NUM"),
    F.col("LnkLkp.ACCOUNT_ID").alias("ACCOUNT_ID"),
    F.col("LnkLkp.GROUP_ID").alias("GROUP_ID"),
    F.col("LnkLkp.QTY").alias("QTY")
)

df_Xfm2 = df_Lkp_Match_Lnk_MatchXmf.select(
    F.col("INVOICE_NUM").alias("INVOICE_NUM"),
    F.col("ACCOUNT_ID").alias("ACCOUNT_ID"),
    F.col("GROUP_ID").alias("GROUP_ID"),
    F.col("GROUP_NAME").alias("GROUP_NAME"),
    F.lit("Match").alias("Status"),
    F.col("SRC_QTY").alias("SRC_QTY"),
    F.col("TBL_QTY").alias("TBL_QTY")
)

df_Lkp_NoMatch_primary = df_Lkp_Match_Lnk_Rej.alias("Lnk_Rej")
df_Lkp_NoMatch_lookup = df_Copy_54_Lnk_To_Copy2.alias("Lnk_To_Copy2")
df_Lkp_NoMatch_joined = df_Lkp_NoMatch_primary.join(
    df_Lkp_NoMatch_lookup,
    (F.col("Lnk_Rej.ACCOUNT_ID") == F.col("Lnk_To_Copy2.GRP_ID")) &
    (F.col("Lnk_Rej.GROUP_ID") == F.col("Lnk_To_Copy2.PROD_ID")),
    "left"
)

df_Lkp_NoMatch_matched = df_Lkp_NoMatch_joined.filter(F.col("Lnk_To_Copy2.GRP_NM").isNotNull())
df_Lkp_NoMatch_unmatched = df_Lkp_NoMatch_joined.filter(F.col("Lnk_To_Copy2.GRP_NM").isNull())

df_Lkp_NoMatch_Lnk_QtyMismatch = df_Lkp_NoMatch_matched.select(
    F.col("Lnk_Rej.INVOICE_NUM").alias("INVOICE_NUM"),
    F.col("Lnk_Rej.ACCOUNT_ID").alias("ACCOUNT_ID"),
    F.col("Lnk_Rej.GROUP_ID").alias("GROUP_ID"),
    F.col("Lnk_To_Copy2.GRP_NM").alias("GROUP_NAME"),
    F.col("Lnk_Rej.QTY").alias("SRC_QTY"),
    F.col("Lnk_To_Copy2.CNT").alias("TBL_QTY")
)

df_Lkp_NoMatch_Lnk_NoMatch = df_Lkp_NoMatch_unmatched.select(
    F.col("Lnk_Rej.INVOICE_NUM").alias("INVOICE_NUM"),
    F.col("Lnk_Rej.ACCOUNT_ID").alias("ACCOUNT_ID"),
    F.col("Lnk_Rej.GROUP_ID").alias("GROUP_ID"),
    F.col("Lnk_Rej.QTY").alias("QTY")
)

df_Xfm3 = df_Lkp_NoMatch_Lnk_QtyMismatch.select(
    F.col("INVOICE_NUM").alias("INVOICE_NUM"),
    F.col("ACCOUNT_ID").alias("ACCOUNT_ID"),
    F.col("GROUP_ID").alias("GROUP_ID"),
    F.col("GROUP_NAME").alias("GROUP_NAME"),
    F.lit("QTYMisMatch").alias("Status"),
    F.col("SRC_QTY").alias("SRC_QTY"),
    F.col("TBL_QTY").alias("TBL_QTY")
)

df_Xfm4 = df_Lkp_NoMatch_Lnk_NoMatch.select(
    F.col("INVOICE_NUM").alias("INVOICE_NUM"),
    F.col("ACCOUNT_ID").alias("ACCOUNT_ID"),
    F.col("GROUP_ID").alias("GROUP_ID"),
    F.lit(" ").alias("GROUP_NAME"),
    F.lit("No Match").alias("Status"),
    F.col("QTY").alias("SRC_QTY"),
    F.lit("").alias("TBL_QTY")
)

df_Funnel = (
    df_Xfm3.unionByName(df_Xfm4)
    .unionByName(df_Xfm2)
)

df_Sort = df_Funnel.orderBy(
    F.col("Status"),
    F.col("ACCOUNT_ID"),
    F.col("GROUP_ID")
)

df_CombinedFile = df_Sort.select(
    F.col("INVOICE_NUM").alias("INVOICE_NUM"),
    F.col("ACCOUNT_ID").alias("ACCOUNT_ID"),
    F.col("GROUP_ID").alias("GROUP_ID"),
    F.col("GROUP_NAME").alias("GROUP_NAME"),
    F.col("Status").alias("STATUS"),
    F.col("SRC_QTY").alias("INVOICE_QTY"),
    F.col("TBL_QTY").alias("TABLE_QTY")
)

df_Seq_ActivityFeeStJoeStatus = (
    df_CombinedFile
    .withColumn("INVOICE_NUM", rpad(F.col("INVOICE_NUM"), <...>, " "))
    .withColumn("ACCOUNT_ID", rpad(F.col("ACCOUNT_ID"), <...>, " "))
    .withColumn("GROUP_ID", rpad(F.col("GROUP_ID"), <...>, " "))
    .withColumn("GROUP_NAME", rpad(F.col("GROUP_NAME"), <...>, " "))
    .withColumn("STATUS", rpad(F.col("STATUS"), <...>, " "))
    .withColumn("INVOICE_QTY", rpad(F.col("INVOICE_QTY"), <...>, " "))
    .withColumn("TABLE_QTY", rpad(F.col("TABLE_QTY"), <...>, " "))
)

write_files(
    df_Seq_ActivityFeeStJoeStatus,
    f"{adls_path_raw}/landing/ActivityFeeStJoeMatchCheck_{RunID}.csv",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=True,
    quote="\"",
    nullValue=None
)