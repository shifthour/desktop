# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2020  Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC ZENA JOBNAME:  EDW_ACTIVITY_FEE_ANALYSIS_REPORT_MONTHLY_000
# MAGIC 
# MAGIC CONTROL JOB:  EdwActivityFeeRptCntl
# MAGIC 
# MAGIC JOB NAME:  EdwMedDActivityFeeExtr
# MAGIC 
# MAGIC Description:  Job reads Med-D Activity Fee Invoice from Optum and reads EDW database to compare differences and produce a report for the BlueKC Finance Team.  The report shows the active members under a given product/group on the Invoice and in the EDW database and displays the active member count (quantity status) of Match, No Match, or Quantity Mismatch.
# MAGIC 
# MAGIC Modifications:
# MAGIC 
# MAGIC Developer                  Date            User Story       Change Description                                                                             Development Project    Code Reviewer            Date Reviewed
# MAGIC -------------------------------   -----------------   ---------------------  ---------------------------------------------------------------------------------------------------------   ----------------------------------   ---------------------------------   -------------------------
# MAGIC Velmani Kondappan  2020-10-01  US-254715     Initial Programming.                                                                              IntegrateDev2   
# MAGIC Bill Schroeder            2022-01-17  US-486052     Replaced lookup to table with lookup to file. Added Sort stage.          IntegrateDev2               Raja Gummadi            02/17/2022

# MAGIC Find Group Id, Product Id, and Active Member counts in EDW from file created on 20th of the month (Optum Invoice cutoff date) that match the Invoice received on the last day of the month.
# MAGIC Read Activity Fee Invoice from Optum
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../Utility_Integrate
# COMMAND ----------


# Parameters
InFile = get_widget_value('InFile','')
RunID = get_widget_value('RunID','')

# Read "ActivityFeeEdwMedDExtr_2" (landing/ActivityFeeEdwMedDExtr.txt)
schema_ActivityFeeEdwMedDExtr_2 = StructType([
    StructField("GRP_ID", StringType(), False),
    StructField("GRP_NM", StringType(), False),
    StructField("PROD_ID", StringType(), False),
    StructField("CNT", IntegerType(), False)
])
df_ActivityFeeEdwMedDExtr_2 = (
    spark.read
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "\"")
    .schema(schema_ActivityFeeEdwMedDExtr_2)
    .csv(f"{adls_path_raw}/landing/ActivityFeeEdwMedDExtr.txt")
)

# Copy_50 stage: producing two outputs

# Lnk_To_Copy2 -> Lkp_NoMatch
df_copy_50_output1 = df_ActivityFeeEdwMedDExtr_2.select(
    col("GRP_ID").alias("GRP_ID"),
    col("GRP_NM").alias("GRP_NM"),
    col("PROD_ID").alias("PROD_ID"),
    col("CNT").alias("CNT")
)

# Lnk_To_Copy -> Lkp_Match
df_copy_50_output2 = df_ActivityFeeEdwMedDExtr_2.select(
    col("GRP_ID").alias("GRP_ID"),
    col("GRP_NM").alias("GRP_NM"),
    col("PROD_ID").alias("PROD_ID"),
    col("CNT").alias("CNT")
)

# Read "ActivityFee_MedD_Invoice" (landing/#InFile#)
schema_ActivityFee_MedD_Invoice = StructType([
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
df_ActivityFee_MedD_Invoice = (
    spark.read
    .option("header", "true")
    .option("delimiter", ",")
    .option("quote", "\"")
    .schema(schema_ActivityFee_MedD_Invoice)
    .csv(f"{adls_path_raw}/landing/{InFile}")
)

# Xfm1: Transformer with constraint
df_xfm1_filtered = df_ActivityFee_MedD_Invoice.filter(
    (col("GROUP_NAME").isNotNull()) &
    (trim(col("GROUP_NAME")) != "") &
    (trim(col("ACTIVITY_CODE")) == "PMPM")
)
df_xfm1_out = (
    df_xfm1_filtered
    .withColumn("INVOICE_NUM", StripWhiteSpace(trim(Ereplace(col("INVOICE_NUMBER"), '"', ' '))))
    .withColumn("ACCOUNT_ID", StripWhiteSpace(trim(Ereplace(col("ACCOUNT_ID"), '"', ' '))))
    .withColumn("GROUP_ID", StripWhiteSpace(trim(Ereplace(col("GROUP_ID"), '"', ' '))))
    .withColumn("QTY", StripWhiteSpace(trim(Ereplace(col("QUANTITY"), '"', ' '))))
    .select("INVOICE_NUM", "ACCOUNT_ID", "GROUP_ID", "QTY")
)

# Rm_Dup_GrpName: RemDup (retain last) on (GROUP_ID, ACCOUNT_ID)
df_rm_dup_grpname = dedup_sort(
    df_xfm1_out,
    ["GROUP_ID", "ACCOUNT_ID"],
    [("GROUP_ID", "A"), ("ACCOUNT_ID", "A")]
)

# Lkp_Match (PxLookup)
# Primary link: df_rm_dup_grpname
# Lookup link: df_copy_50_output2 (left join on ACCOUNT_ID=GRP_ID, GROUP_ID=PROD_ID, QTY=CNT)
df_lkp_match_temp = df_rm_dup_grpname.alias("Lnk_RmDup").join(
    df_copy_50_output2.alias("Lnk_To_Copy"),
    (col("Lnk_RmDup.ACCOUNT_ID") == col("Lnk_To_Copy.GRP_ID")) &
    (col("Lnk_RmDup.GROUP_ID") == col("Lnk_To_Copy.PROD_ID")) &
    (col("Lnk_RmDup.QTY") == col("Lnk_To_Copy.CNT")),
    "left"
)

df_lkp_match_main = df_lkp_match_temp.filter(col("Lnk_To_Copy.GRP_NM").isNotNull()).select(
    col("Lnk_RmDup.INVOICE_NUM").alias("INVOICE_NUM"),
    col("Lnk_RmDup.ACCOUNT_ID").alias("ACCOUNT_ID"),
    col("Lnk_RmDup.GROUP_ID").alias("GROUP_ID"),
    col("Lnk_To_Copy.GRP_NM").alias("GROUP_NAME"),
    col("Lnk_RmDup.QTY").alias("SRC_QTY"),
    col("Lnk_To_Copy.CNT").alias("TBL_QTY")
)

df_lkp_reject = df_lkp_match_temp.filter(col("Lnk_To_Copy.GRP_NM").isNull()).select(
    col("Lnk_RmDup.INVOICE_NUM").alias("INVOICE_NUM"),
    col("Lnk_RmDup.ACCOUNT_ID").alias("ACCOUNT_ID"),
    col("Lnk_RmDup.GROUP_ID").alias("GROUP_ID"),
    col("Lnk_RmDup.QTY").alias("QTY")
)

# Xfm2
df_xfm2_out = df_lkp_match_main.select(
    col("INVOICE_NUM").alias("INVOICE_NUM"),
    col("ACCOUNT_ID").alias("ACCOUNT_ID"),
    col("GROUP_ID").alias("GROUP_ID"),
    col("GROUP_NAME").alias("GROUP_NAME"),
    lit("Match").alias("Status"),
    col("SRC_QTY").alias("SRC_QTY"),
    col("TBL_QTY").alias("TBL_QTY")
)

# Lkp_NoMatch (PxLookup)
# Primary link: df_lkp_reject
# Lookup link: df_copy_50_output1
df_lkp_no_match_temp = df_lkp_reject.alias("Lnk_Rej").join(
    df_copy_50_output1.alias("Lnk_To_Copy2"),
    (col("Lnk_Rej.ACCOUNT_ID") == col("Lnk_To_Copy2.GRP_ID")) &
    (col("Lnk_Rej.GROUP_ID") == col("Lnk_To_Copy2.PROD_ID")),
    "left"
)

df_lkp_no_match_qty_mismatch = df_lkp_no_match_temp.filter(col("Lnk_To_Copy2.GRP_NM").isNotNull()).select(
    col("Lnk_Rej.INVOICE_NUM").alias("INVOICE_NUM"),
    col("Lnk_Rej.ACCOUNT_ID").alias("ACCOUNT_ID"),
    col("Lnk_Rej.GROUP_ID").alias("GROUP_ID"),
    col("Lnk_To_Copy2.GRP_NM").alias("GROUP_NAME"),
    col("Lnk_Rej.QTY").alias("SRC_QTY"),
    col("Lnk_To_Copy2.CNT").alias("TBL_QTY")
)

df_lkp_no_match_nomatch = df_lkp_no_match_temp.filter(col("Lnk_To_Copy2.GRP_NM").isNull()).select(
    col("Lnk_Rej.INVOICE_NUM").alias("INVOICE_NUM"),
    col("Lnk_Rej.GROUP_ID").alias("GROUP_ID"),
    col("Lnk_Rej.QTY").alias("QTY"),
    col("Lnk_Rej.ACCOUNT_ID").alias("ACCOUNT_ID")
)

# Xfm3
df_xfm3_out = df_lkp_no_match_qty_mismatch.select(
    col("INVOICE_NUM").alias("INVOICE_NUM"),
    col("ACCOUNT_ID").alias("ACCOUNT_ID"),
    col("GROUP_ID").alias("GROUP_ID"),
    col("GROUP_NAME").alias("GROUP_NAME"),
    lit("QTYMisMatch").alias("Status"),
    col("SRC_QTY").alias("SRC_QTY"),
    col("TBL_QTY").alias("TBL_QTY")
)

# Xfm4
df_xfm4_out = df_lkp_no_match_nomatch.select(
    col("INVOICE_NUM").alias("INVOICE_NUM"),
    col("ACCOUNT_ID").alias("ACCOUNT_ID"),
    col("GROUP_ID").alias("GROUP_ID"),
    lit(" ").alias("GROUP_NAME"),
    lit("No Match").alias("Status"),
    col("QTY").alias("SRC_QTY"),
    lit("").alias("TBL_QTY")
)

# Funnel
df_funnel = (
    df_xfm3_out.unionByName(df_xfm4_out, allowMissingColumns=True)
    .unionByName(df_xfm2_out, allowMissingColumns=True)
)

# Sort_48
df_sort_48 = df_funnel.orderBy(["Status", "ACCOUNT_ID", "GROUP_ID"], ascending=[True, True, True])

df_sort_48_out = df_sort_48.select(
    col("INVOICE_NUM").alias("INVOICE_NUM"),
    col("ACCOUNT_ID").alias("ACCOUNT_ID"),
    col("GROUP_ID").alias("GROUP_ID"),
    col("GROUP_NAME").alias("GROUP_NAME"),
    col("Status").alias("STATUS"),
    col("SRC_QTY").alias("INVOICE_QTY"),
    col("TBL_QTY").alias("TABLE_QTY")
)

# Apply rpad for final varchar columns
df_final = (
    df_sort_48_out
    .withColumn("INVOICE_NUM", rpad(col("INVOICE_NUM"), <...>, " "))
    .withColumn("ACCOUNT_ID", rpad(col("ACCOUNT_ID"), <...>, " "))
    .withColumn("GROUP_ID", rpad(col("GROUP_ID"), <...>, " "))
    .withColumn("GROUP_NAME", rpad(col("GROUP_NAME"), <...>, " "))
    .withColumn("STATUS", rpad(col("STATUS"), <...>, " "))
    .withColumn("INVOICE_QTY", rpad(col("INVOICE_QTY"), <...>, " "))
    .withColumn("TABLE_QTY", col("TABLE_QTY"))
    .select("INVOICE_NUM", "ACCOUNT_ID", "GROUP_ID", "GROUP_NAME", "STATUS", "INVOICE_QTY", "TABLE_QTY")
)

# Seq_ActivityFeeMedDStatus write
file_path_Seq_ActivityFeeMedDStatus = f"{adls_path_raw}/landing/ActivityFeeMedDMatchCheck_{RunID}.csv"
write_files(
    df_final,
    file_path_Seq_ActivityFeeMedDStatus,
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=True,
    quote="\"",
    nullValue=None
)