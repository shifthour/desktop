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
# MAGIC JOB NAME:  ActivityFeeACAMatchCheck
# MAGIC 
# MAGIC Description:  Job reads ACA Activity Fee Invoice from Optum and reads EDW database to compare differences and produce a report for the BlueKC Finance Team.  The report shows the active members under a given product/group on the Invoice and in the EDW database and displays the active member count (quantity status) of Match, No Match, or Quantity Mismatch.
# MAGIC 
# MAGIC Modifications:
# MAGIC 
# MAGIC Developer                  Date            User Story       Change Description                                                                             Development Project    Code Reviewer            Date Reviewed
# MAGIC -------------------------------   -----------------   ---------------------  ---------------------------------------------------------------------------------------------------------   ----------------------------------   ---------------------------------   -------------------------
# MAGIC Velmani Kondappan  2020-10-01  US-254834     Initial Programming.                                                                              IntegrateDev2      
# MAGIC Bill Schroeder            2022-01-17  US-486052     Replaced lookup to table with lookup to file. Added Sort stage.          IntegrateDev2                Raja Gummadi             02/17/2022

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
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
# COMMAND ----------
# MAGIC %run ../../../Utility_Integrate
# COMMAND ----------


InFile = get_widget_value('InFile','')
RunID = get_widget_value('RunID','')

schema_ActivityFeeEdwACAExtr_2 = T.StructType([
    T.StructField("GRP_ID", T.StringType(), False),
    T.StructField("GRP_NM", T.StringType(), False),
    T.StructField("PROD_ID", T.StringType(), False),
    T.StructField("CNT", T.IntegerType(), False)
])

df_ActivityFeeEdwACAExtr_2 = (
    spark.read.format("csv")
    .option("header", False)
    .option("inferSchema", False)
    .option("delimiter", ",")
    .option("quote", "\"")
    .schema(schema_ActivityFeeEdwACAExtr_2)
    .load(f"{adls_path_raw}/landing/ActivityFeeEdwACAExtr.txt")
)

df_Copy_47_out_Lnk_To_Copy2 = df_ActivityFeeEdwACAExtr_2.select(
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("GRP_NM").alias("GRP_NM"),
    F.col("PROD_ID").alias("PROD_ID"),
    F.col("CNT").alias("CNT")
)

df_Copy_47_out_Lnk_To_Copy = df_ActivityFeeEdwACAExtr_2.select(
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("GRP_NM").alias("GRP_NM"),
    F.col("PROD_ID").alias("PROD_ID"),
    F.col("CNT").alias("CNT")
)

schema_ActivityFee_ACA_Invoice = T.StructType([
    T.StructField("Invoice_Number", T.StringType(), False),
    T.StructField("Invoice_Date", T.StringType(), False),
    T.StructField("Billing_Entity_ID", T.StringType(), False),
    T.StructField("Billing_Entity_Name", T.StringType(), False),
    T.StructField("Activity_Level", T.StringType(), False),
    T.StructField("Carrier_ID", T.StringType(), False),
    T.StructField("Carrier_Name", T.StringType(), False),
    T.StructField("Account_ID", T.StringType(), False),
    T.StructField("Account_Name", T.StringType(), False),
    T.StructField("Group_ID", T.StringType(), False),
    T.StructField("Group_Name", T.StringType(), False),
    T.StructField("Service_Date_Period", T.StringType(), False),
    T.StructField("Activity_Code", T.StringType(), False),
    T.StructField("Sub_Code", T.StringType(), False),
    T.StructField("Activity_Description", T.StringType(), False),
    T.StructField("Quantity", T.StringType(), False),
    T.StructField("Unit_Cost", T.StringType(), False),
    T.StructField("Total_Fees", T.StringType(), False),
    T.StructField("Additional_Information", T.StringType(), False)
])

df_ActivityFee_ACA_Invoice = (
    spark.read.format("csv")
    .option("header", True)
    .option("inferSchema", False)
    .option("delimiter", ",")
    .option("quote", "\"")
    .schema(schema_ActivityFee_ACA_Invoice)
    .load(f"{adls_path_raw}/landing/{InFile}")
)

df_Xfm1_temp = (
    df_ActivityFee_ACA_Invoice.withColumn(
        "temp_Constraint",
        (F.col("Group_ID").isNotNull())
        & (F.col("Group_ID") != "")
        & (trim(F.col("Activity_Code")) == "PMPM")
    )
    .withColumn(
        "INVOICE_NUM",
        StripWhiteSpace(
            trim(
                Ereplace(F.col("Invoice_Number"), F.lit("\""), F.lit(" "))
            )
        )
    )
    .withColumn(
        "ACCOUNT_ID",
        StripWhiteSpace(
            trim(
                Ereplace(F.col("Account_ID"), F.lit("\""), F.lit(" "))
            )
        )
    )
    .withColumn(
        "GROUP_ID",
        StripWhiteSpace(
            trim(
                Ereplace(F.col("Group_ID"), F.lit("\""), F.lit(" "))
            )
        )
    )
    .withColumn(
        "QTY",
        StripWhiteSpace(
            trim(
                Ereplace(F.col("Quantity"), F.lit("\""), F.lit(" "))
            )
        )
    )
)

df_Xfm1 = df_Xfm1_temp.filter(F.col("temp_Constraint")).select(
    "INVOICE_NUM",
    "ACCOUNT_ID",
    "GROUP_ID",
    "QTY"
)

df_Rm_Dup_GrpName = dedup_sort(
    df_Xfm1,
    ["GROUP_ID", "ACCOUNT_ID"],
    [("GROUP_ID", "A"), ("ACCOUNT_ID", "A")]
)

df_Lkp_Match_Joined = df_Rm_Dup_GrpName.alias("LnkLkp").join(
    df_Copy_47_out_Lnk_To_Copy.alias("Lnk_To_Copy"),
    (
        (F.col("LnkLkp.ACCOUNT_ID") == F.col("Lnk_To_Copy.GRP_ID"))
        & (F.col("LnkLkp.GROUP_ID") == F.col("Lnk_To_Copy.PROD_ID"))
        & (F.col("LnkLkp.QTY") == F.col("Lnk_To_Copy.CNT"))
    ),
    "left"
)

df_Lkp_Match_out_Lnk_MatchXmf = (
    df_Lkp_Match_Joined.filter(F.col("Lnk_To_Copy.GRP_ID").isNotNull())
    .select(
        F.col("LnkLkp.INVOICE_NUM").alias("INVOICE_NUM"),
        F.col("LnkLkp.ACCOUNT_ID").alias("ACCOUNT_ID"),
        F.col("LnkLkp.GROUP_ID").alias("GROUP_ID"),
        F.col("Lnk_To_Copy.GRP_NM").alias("GROUP_NAME"),
        F.col("LnkLkp.QTY").alias("SRC_QTY"),
        F.col("Lnk_To_Copy.CNT").alias("TBL_QTY")
    )
)

df_Lkp_Match_Rej = (
    df_Lkp_Match_Joined.filter(F.col("Lnk_To_Copy.GRP_ID").isNull())
    .select(
        F.col("LnkLkp.INVOICE_NUM").alias("INVOICE_NUM"),
        F.col("LnkLkp.ACCOUNT_ID").alias("ACCOUNT_ID"),
        F.col("LnkLkp.GROUP_ID").alias("GROUP_ID"),
        F.col("LnkLkp.QTY").alias("QTY")
    )
)

df_Xfm2_temp = df_Lkp_Match_out_Lnk_MatchXmf.withColumn("Status", F.lit("Match"))
df_Xfm2 = df_Xfm2_temp.select(
    "INVOICE_NUM",
    "ACCOUNT_ID",
    "GROUP_ID",
    "GROUP_NAME",
    "Status",
    "SRC_QTY",
    "TBL_QTY"
)

df_Lkp_NoMatch_join = df_Lkp_Match_Rej.alias("Lnk_Rej").join(
    df_Copy_47_out_Lnk_To_Copy2.alias("Lnk_To_Copy2"),
    (
        (F.col("Lnk_Rej.ACCOUNT_ID") == F.col("Lnk_To_Copy2.GRP_ID"))
        & (F.col("Lnk_Rej.GROUP_ID") == F.col("Lnk_To_Copy2.PROD_ID"))
    ),
    "left"
)

df_Lkp_NoMatch_matched = (
    df_Lkp_NoMatch_join.filter(F.col("Lnk_To_Copy2.GRP_ID").isNotNull())
    .select(
        F.col("Lnk_Rej.INVOICE_NUM").alias("INVOICE_NUM"),
        F.col("Lnk_Rej.ACCOUNT_ID").alias("ACCOUNT_ID"),
        F.col("Lnk_Rej.GROUP_ID").alias("GROUP_ID"),
        F.col("Lnk_To_Copy2.GRP_NM").alias("GROUP_NAME"),
        F.col("Lnk_Rej.QTY").alias("SRC_QTY"),
        F.col("Lnk_To_Copy2.CNT").alias("TBL_QTY")
    )
)

df_Lkp_NoMatch_unmatched = (
    df_Lkp_NoMatch_join.filter(F.col("Lnk_To_Copy2.GRP_ID").isNull())
    .select(
        F.col("Lnk_Rej.INVOICE_NUM").alias("INVOICE_NUM"),
        F.col("Lnk_Rej.ACCOUNT_ID").alias("ACCOUNT_ID"),
        F.col("Lnk_Rej.GROUP_ID").alias("GROUP_ID"),
        F.col("Lnk_Rej.QTY").alias("QTY")
    )
)

df_Xfm3_temp = df_Lkp_NoMatch_matched.withColumn("Status", F.lit("QTYMisMatch"))
df_Xfm3 = df_Xfm3_temp.select(
    "INVOICE_NUM",
    "ACCOUNT_ID",
    "GROUP_ID",
    "GROUP_NAME",
    "Status",
    F.col("SRC_QTY"),
    F.col("TBL_QTY")
)

df_Xfm4_temp = (
    df_Lkp_NoMatch_unmatched
    .withColumn("GROUP_NAME", F.lit(" "))
    .withColumn("Status", F.lit("No Match"))
    .withColumn("TBL_QTY", F.lit(""))
)
df_Xfm4 = df_Xfm4_temp.select(
    "INVOICE_NUM",
    "ACCOUNT_ID",
    "GROUP_ID",
    "GROUP_NAME",
    "Status",
    F.col("QTY").alias("SRC_QTY"),
    "TBL_QTY"
)

df_Funnel = (
    df_Xfm3.select(
        "INVOICE_NUM",
        "ACCOUNT_ID",
        "GROUP_ID",
        "GROUP_NAME",
        "Status",
        "SRC_QTY",
        "TBL_QTY"
    )
    .union(
        df_Xfm4.select(
            "INVOICE_NUM",
            "ACCOUNT_ID",
            "GROUP_ID",
            "GROUP_NAME",
            "Status",
            "SRC_QTY",
            "TBL_QTY"
        )
    )
    .union(
        df_Xfm2.select(
            "INVOICE_NUM",
            "ACCOUNT_ID",
            "GROUP_ID",
            "GROUP_NAME",
            "Status",
            "SRC_QTY",
            "TBL_QTY"
        )
    )
)

df_Sort = df_Funnel.orderBy(["Status", "ACCOUNT_ID", "GROUP_ID"], ascending=[True, True, True])

df_Final = (
    df_Sort
    .withColumnRenamed("SRC_QTY", "INVOICE_QTY")
    .withColumnRenamed("TBL_QTY", "TABLE_QTY")
)

df_Final_padded = (
    df_Final
    .withColumn("INVOICE_NUM", F.rpad(F.col("INVOICE_NUM"), <...>, " "))
    .withColumn("ACCOUNT_ID", F.rpad(F.col("ACCOUNT_ID"), <...>, " "))
    .withColumn("GROUP_ID", F.rpad(F.col("GROUP_ID"), <...>, " "))
    .withColumn("GROUP_NAME", F.rpad(F.col("GROUP_NAME"), <...>, " "))
    .withColumn("STATUS", F.rpad(F.col("STATUS"), <...>, " "))
    .withColumn("INVOICE_QTY", F.rpad(F.col("INVOICE_QTY"), <...>, " "))
    .withColumn("TABLE_QTY", F.rpad(F.col("TABLE_QTY"), <...>, " "))
)

write_files(
    df_Final_padded.select(
        "INVOICE_NUM",
        "ACCOUNT_ID",
        "GROUP_ID",
        "GROUP_NAME",
        "STATUS",
        "INVOICE_QTY",
        "TABLE_QTY"
    ),
    f"{adls_path_raw}/landing/ActivityFeeACAMatchCheck_{RunID}.csv",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)