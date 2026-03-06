# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2020  Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC ZENA JOBNAME:  EDW_ACTIVITY_FEE_ANALYSIS_EXTRACT_REPORT_000
# MAGIC 
# MAGIC CONTROL JOB:  EdwActivityFeeRptCntl
# MAGIC 
# MAGIC JOB NAME:  ActivityFeeCommMatchCheck
# MAGIC 
# MAGIC Description:  Job reads Commercial Activity Fee Invoice from Optum and EDW database to compare differences and produce a report for the BlueKC Finance Team.  The report shows the active members under a given product/group on the Invoice and in the EDW database and displays the active member count (quantity status) of Match, No Match, or Quantity Mismatch.
# MAGIC 
# MAGIC Modifications:
# MAGIC 
# MAGIC Developer\(9)Date\(9)\(9)User Story       Change Description\(9)\(9)\(9)\(9)Development Project\(9)\(9)Code Reviewer            Date Reviewed
# MAGIC ==================================================================================================================================================================
# MAGIC Velmani Kondappan  2020-10-01  US-338638     Initial Programming.\(9)                                                                    IntegrateDev2                                  Ken Bradmon\(9)06/15/2021
# MAGIC Bill Schroeder            2022-01-17  US-486052     Replaced lookup to table with lookup to file. Added Sort stage.     IntegrateDev2                                  Raja Gummadi            02/17/2022

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
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../Utility_Integrate
# COMMAND ----------


InFile = get_widget_value("InFile", "")
RunID = get_widget_value("RunID", "")

# Schema for ActivityFeeEdwCommExtr (landing/ActivityFeeEdwCommExtr.txt)
schema_ActivityFeeEdwCommExtr = StructType([
    StructField("GRP_ID", StringType(), False),
    StructField("GRP_NM", StringType(), False),
    StructField("PROD_ID", StringType(), False),
    StructField("CNT", IntegerType(), False)
])
df_ActivityFeeEdwCommExtr = (
    spark.read.format("csv")
    .option("header", False)
    .option("sep", ",")
    .option("quote", '"')
    .schema(schema_ActivityFeeEdwCommExtr)
    .load(f"{adls_path_raw}/landing/ActivityFeeEdwCommExtr.txt")
)

# Copy_58 stage: produce two outputs Lnk_To_Copy (df_Lnk_To_Copy) and Lnk_To_Copy2 (df_Lnk_To_Copy2)
df_Lnk_To_Copy = df_ActivityFeeEdwCommExtr.select(
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("GRP_NM").alias("GRP_NM"),
    F.col("PROD_ID").alias("PROD_ID"),
    F.col("CNT").alias("CNT")
)

df_Lnk_To_Copy2 = df_ActivityFeeEdwCommExtr.select(
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("GRP_NM").alias("GRP_NM"),
    F.col("PROD_ID").alias("PROD_ID"),
    F.col("CNT").alias("CNT")
)

# Schema for ActivityFee_Comm_Invoice (read from parameter InFile, header = true)
schema_ActivityFee_Comm_Invoice = StructType([
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
df_ActivityFee_Comm_Invoice = (
    spark.read.format("csv")
    .option("header", True)
    .option("sep", ",")
    .option("quote", '"')
    .schema(schema_ActivityFee_Comm_Invoice)
    .load(InFile)
)

# Xfm1 stage: Filter + Column derivations
df_Xfm1_input = df_ActivityFee_Comm_Invoice.filter(
    (F.col("GROUP_NAME").isNotNull()) &
    (F.col("GROUP_NAME") != "") &
    (trim(F.col("ACTIVITY_CODE")) == "PMPM")
)

df_Xfm1_output = df_Xfm1_input.select(
    strip_field(trim(Ereplace(F.col("INVOICE_NUMBER"), '"', ' '))).alias("INVOICE_NUM"),
    strip_field(trim(Ereplace(F.col("ACCOUNT_ID"), '"', ' '))).alias("ACCOUNT_ID"),
    strip_field(trim(Ereplace(F.col("GROUP_ID"), '"', ' '))).alias("GROUP_ID"),
    strip_field(trim(Ereplace(F.col("QUANTITY"), '"', ' '))).alias("QUANTITY")
)

# Rm_Dup_GrpName (PxRemDup) retaining last record by keys => GROUP_ID, ACCOUNT_ID
df_RmDup_temp = dedup_sort(
    df_Xfm1_output,
    ["GROUP_ID", "ACCOUNT_ID"],
    [("GROUP_ID", "A"), ("ACCOUNT_ID", "A")]
)
df_RmDup_GrpName = df_RmDup_temp.select(
    F.col("INVOICE_NUM").alias("INVOICE_NUM"),
    F.col("ACCOUNT_ID").alias("ACCOUNT_ID"),
    F.col("GROUP_ID").alias("GROUP_ID"),
    F.col("QUANTITY").alias("QTY")
)

# Lkp_Match (PxLookup) - Primary link: df_RmDup_GrpName, Lookup link: df_Lnk_To_Copy, left join
df_Lkp_Match_joined = df_RmDup_GrpName.alias("LnkLkp").join(
    df_Lnk_To_Copy.alias("Lnk_To_Copy"),
    [
        F.col("LnkLkp.ACCOUNT_ID") == F.col("Lnk_To_Copy.GRP_ID"),
        F.col("LnkLkp.GROUP_ID") == F.col("Lnk_To_Copy.PROD_ID"),
        F.col("LnkLkp.QTY") == F.col("Lnk_To_Copy.CNT")
    ],
    how="left"
)

df_Lkp_MatchXmf = df_Lkp_Match_joined.select(
    F.col("LnkLkp.INVOICE_NUM").alias("INVOICE_NUM"),
    F.col("LnkLkp.ACCOUNT_ID").alias("ACCOUNT_ID"),
    F.col("LnkLkp.GROUP_ID").alias("GROUP_ID"),
    F.col("Lnk_To_Copy.GRP_NM").alias("GROUP_NAME"),
    F.col("LnkLkp.QTY").alias("SRC_QTY"),
    F.col("Lnk_To_Copy.CNT").alias("TBL_QTY")
).filter(F.col("Lnk_To_Copy.GRP_NM").isNotNull())

df_LkpMatch_Rej = df_Lkp_Match_joined.select(
    F.col("LnkLkp.INVOICE_NUM").alias("INVOICE_NUM"),
    F.col("LnkLkp.ACCOUNT_ID").alias("ACCOUNT_ID"),
    F.col("LnkLkp.GROUP_ID").alias("GROUP_ID"),
    F.col("LnkLkp.QTY").alias("QTY")
).filter(F.col("Lnk_To_Copy.GRP_NM").isNull())

# Xfm2 stage
df_Xfm2_output = df_Lkp_MatchXmf.select(
    F.col("INVOICE_NUM").alias("INVOICE_NUM"),
    F.col("ACCOUNT_ID").alias("ACCOUNT_ID"),
    F.col("GROUP_ID").alias("GROUP_ID"),
    F.col("GROUP_NAME").alias("GROUP_NAME"),
    F.lit("Match").alias("Status"),
    F.col("SRC_QTY").alias("SRC_QTY"),
    F.col("TBL_QTY").alias("TBL_QTY")
)

# Lkp_NoMatch (PxLookup) - Primary link: df_LkpMatch_Rej, Lookup link: df_Lnk_To_Copy2, left join
df_Lkp_NoMatch_joined = df_LkpMatch_Rej.alias("Lnk_Rej").join(
    df_Lnk_To_Copy2.alias("Lnk_To_Copy2"),
    [
        F.col("Lnk_Rej.ACCOUNT_ID") == F.col("Lnk_To_Copy2.GRP_ID"),
        F.col("Lnk_Rej.GROUP_ID") == F.col("Lnk_To_Copy2.PROD_ID")
    ],
    how="left"
)

df_Lkp_NoMatch_QtyMismatch = df_Lkp_NoMatch_joined.select(
    F.col("Lnk_Rej.INVOICE_NUM").alias("INVOICE_NUM"),
    F.col("Lnk_Rej.ACCOUNT_ID").alias("ACCOUNT_ID"),
    F.col("Lnk_Rej.GROUP_ID").alias("GROUP_ID"),
    F.col("Lnk_To_Copy2.GRP_NM").alias("GROUP_NAME"),
    F.col("Lnk_Rej.QTY").alias("SRC_QTY"),
    F.col("Lnk_To_Copy2.CNT").alias("TBL_QTY")
).filter(F.col("Lnk_To_Copy2.GRP_NM").isNotNull())

df_Lkp_NoMatch_NoMatch = df_Lkp_NoMatch_joined.select(
    F.col("Lnk_Rej.INVOICE_NUM").alias("INVOICE_NUM"),
    F.col("Lnk_Rej.ACCOUNT_ID").alias("ACCOUNT_ID"),
    F.col("Lnk_Rej.GROUP_ID").alias("GROUP_ID"),
    F.col("Lnk_Rej.QTY").alias("QTY")
).filter(F.col("Lnk_To_Copy2.GRP_NM").isNull())

# Xfm3 stage
df_Xfm3_output = df_Lkp_NoMatch_QtyMismatch.select(
    F.col("INVOICE_NUM").alias("INVOICE_NUM"),
    F.col("ACCOUNT_ID").alias("ACCOUNT_ID"),
    F.col("GROUP_ID").alias("GROUP_ID"),
    F.col("GROUP_NAME").alias("GROUP_NAME"),
    F.lit("QTYMisMatch").alias("Status"),
    F.col("SRC_QTY").alias("SRC_QTY"),
    F.col("TBL_QTY").alias("TBL_QTY")
)

# Xfm4 stage
df_Xfm4_output = df_Lkp_NoMatch_NoMatch.select(
    F.col("INVOICE_NUM").alias("INVOICE_NUM"),
    F.col("ACCOUNT_ID").alias("ACCOUNT_ID"),
    F.col("GROUP_ID").alias("GROUP_ID"),
    F.lit(" ").alias("GROUP_NAME"),
    F.lit("No Match").alias("Status"),
    F.col("QTY").alias("SRC_QTY"),
    F.lit("").alias("TBL_QTY")
)

# Funnel stage => union all
df_Funnel_part1 = df_Xfm3_output.select(
    "INVOICE_NUM",
    "ACCOUNT_ID",
    "GROUP_ID",
    "GROUP_NAME",
    "Status",
    "SRC_QTY",
    "TBL_QTY"
)
df_Funnel_part2 = df_Xfm4_output.select(
    "INVOICE_NUM",
    "ACCOUNT_ID",
    "GROUP_ID",
    "GROUP_NAME",
    "Status",
    "SRC_QTY",
    "TBL_QTY"
)
df_Funnel_part3 = df_Xfm2_output.select(
    "INVOICE_NUM",
    "ACCOUNT_ID",
    "GROUP_ID",
    "GROUP_NAME",
    "Status",
    "SRC_QTY",
    "TBL_QTY"
)

df_Funnel = df_Funnel_part1.unionByName(df_Funnel_part2, allowMissingColumns=True).unionByName(df_Funnel_part3, allowMissingColumns=True)

# Sort stage => order by Status, ACCOUNT_ID, GROUP_ID
df_Sort_output = df_Funnel.orderBy(["Status", "ACCOUNT_ID", "GROUP_ID"], ascending=True)

# Seq_ActivityFeeCommStatus => final select and write
df_final = df_Sort_output.select(
    F.col("INVOICE_NUM").alias("INVOICE_NUM"),
    F.col("ACCOUNT_ID").alias("ACCOUNT_ID"),
    F.col("GROUP_ID").alias("GROUP_ID"),
    F.col("GROUP_NAME").alias("GROUP_NAME"),
    F.col("Status").alias("STATUS"),
    F.col("SRC_QTY").alias("INVOICE_QTY"),
    F.col("TBL_QTY").alias("TABLE_QTY")
)

df_final_rpad = df_final.select(
    F.rpad(F.col("INVOICE_NUM"), <...>, " ").alias("INVOICE_NUM"),
    F.rpad(F.col("ACCOUNT_ID"), <...>, " ").alias("ACCOUNT_ID"),
    F.rpad(F.col("GROUP_ID"), <...>, " ").alias("GROUP_ID"),
    F.rpad(F.col("GROUP_NAME"), <...>, " ").alias("GROUP_NAME"),
    F.rpad(F.col("STATUS"), <...>, " ").alias("STATUS"),
    F.rpad(F.col("INVOICE_QTY"), <...>, " ").alias("INVOICE_QTY"),
    F.rpad(F.col("TABLE_QTY"), <...>, " ").alias("TABLE_QTY")
)

write_files(
    df_final_rpad,
    f"{adls_path_raw}/landing/ActivityFeeCommMatchCheck_{RunID}.csv",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote='"',
    nullValue=None
)