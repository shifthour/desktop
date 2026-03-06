# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Job: I820CMSEnrPaymtDtlValExtr
# MAGIC 
# MAGIC This job Extracts data from I820  file and balance the amounts in file
# MAGIC 
# MAGIC CALLED BY: CMSI820BCBSFinanceTablesLoadSeq
# MAGIC 
# MAGIC Developer                                            Date                          Project or Ticket                         Description                                    Environment                   Code Reviewer               Review Date
# MAGIC ------------------------                           -----------------------          ---------------------------------------            ---------------------------------------------------    -----------------------------          -----------------------------          ------------------------------
# MAGIC Raj Kommineni                               2021-12-15              US390105                                        Original programming                     IntegrateDev2             Jeyaprasanna                 2021-12-18

# MAGIC Sum UF and APTC amounts
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


RunID = get_widget_value('RunID','')

# I820HeaderFile (PxSequentialFile)
header_schema = StructType([
    StructField("TRANSACTION_SET_CONTROL_NUMBER", StringType(), True),
    StructField("RUN_DATE", StringType(), True),
    StructField("PAYEE_ID", StringType(), True),
    StructField("PAYMENT_METHOD_CODE", StringType(), True),
    StructField("POLICY_BASED_TRANSITION_MONTH", StringType(), True),
    StructField("TOTAL_PAYMENT", StringType(), True),
    StructField("PAYEE_APTC_TOTAL", StringType(), True),
    StructField("PAYEE_CSR_TOTAL", StringType(), True),
    StructField("PAYEE_UF_TOTAL", StringType(), True)
])

df_I820HeaderFile = (
    spark.read.format("csv")
    .option("header", False)
    .option("delimiter", "\\s+")
    .schema(header_schema)
    .load(f"{adls_path_raw}/landing/I820HeaderFile_{RunID}.dat")
)

# transfheadkey (CTransformerStage)
df_transfheadkey = (
    df_I820HeaderFile
    .withColumn("KEY", F.lit("Y"))
    .select(
        "TRANSACTION_SET_CONTROL_NUMBER",
        "RUN_DATE",
        "PAYEE_ID",
        "PAYMENT_METHOD_CODE",
        "POLICY_BASED_TRANSITION_MONTH",
        "TOTAL_PAYMENT",
        "PAYEE_APTC_TOTAL",
        "PAYEE_CSR_TOTAL",
        "PAYEE_UF_TOTAL",
        "KEY"
    )
)

# I820DetailFile (PxSequentialFile)
detail_schema = StructType([
    StructField("ISSUER_ID", StringType(), True),
    StructField("ISSUER_APTC_TOTAL", StringType(), True),
    StructField("ISSUER_CSR_TOTAL", StringType(), True),
    StructField("ISSUER_UF_TOTAL", StringType(), True),
    StructField("LAST_NAME", StringType(), True),
    StructField("FIRST_NAME", StringType(), True),
    StructField("MIDDLE_NAME", StringType(), True),
    StructField("NAME_PREFIX", StringType(), True),
    StructField("NAME_SUFFIX", StringType(), True),
    StructField("EXCHANGE_ASSIGNED_SUBSCRIBER_ID", StringType(), True),
    StructField("EXCHANGE_ASSIGNED_QHP_ID", StringType(), True),
    StructField("EXCHANGE_ASSIGNED_POLICY_ID", StringType(), True),
    StructField("ISSUER_ASSIGNED_POLICY_ID", StringType(), True),
    StructField("ISSUER_ASSIGNED_SUBSCRIBER_ID", StringType(), True),
    StructField("POLICY_TOTAL_PREMIUM_AMOUNT", StringType(), True),
    StructField("EXCHANGE_PAYMENT_TYPE", StringType(), True),
    StructField("PAYMENT_AMOUNT", StringType(), True),
    StructField("EXCHANGE_RELATED_REPORT_TYPE", StringType(), True),
    StructField("EXCHANGE_REPORT_DOCUMENT_CONTROL_NUMBER", StringType(), True),
    StructField("COVERAGE_PERIOD_START_DATE", StringType(), True),
    StructField("COVERAGE_PERIOD_END_DATE", StringType(), True)
])

df_I820DetailFile = (
    spark.read.format("csv")
    .option("header", False)
    .option("delimiter", "|")
    .schema(detail_schema)
    .load(f"{adls_path_raw}/landing/I820DetailFile_{RunID}.dat")
)

# transfkey (CTransformerStage)
df_transfkey = (
    df_I820DetailFile
    .withColumn("KEY", F.lit("Y"))
    .select(
        "ISSUER_ID",
        "ISSUER_APTC_TOTAL",
        "ISSUER_CSR_TOTAL",
        "ISSUER_UF_TOTAL",
        "LAST_NAME",
        "FIRST_NAME",
        "MIDDLE_NAME",
        "NAME_PREFIX",
        "NAME_SUFFIX",
        "EXCHANGE_ASSIGNED_SUBSCRIBER_ID",
        "EXCHANGE_ASSIGNED_QHP_ID",
        "EXCHANGE_ASSIGNED_POLICY_ID",
        "ISSUER_ASSIGNED_POLICY_ID",
        "ISSUER_ASSIGNED_SUBSCRIBER_ID",
        "POLICY_TOTAL_PREMIUM_AMOUNT",
        "EXCHANGE_PAYMENT_TYPE",
        "PAYMENT_AMOUNT",
        "EXCHANGE_RELATED_REPORT_TYPE",
        "EXCHANGE_REPORT_DOCUMENT_CONTROL_NUMBER",
        "COVERAGE_PERIOD_START_DATE",
        "COVERAGE_PERIOD_END_DATE",
        "KEY"
    )
)

# lkpheadet (PxLookup)
df_lkpheadet = (
    df_transfkey.alias("detlkpextr")
    .join(
        df_transfheadkey.alias("headerlkpextr"),
        F.col("detlkpextr.KEY") == F.col("headerlkpextr.KEY"),
        how="left"
    )
    .select(
        F.col("detlkpextr.ISSUER_ID").alias("ISSUER_ID"),
        F.col("detlkpextr.ISSUER_APTC_TOTAL").alias("ISSUER_APTC_TOTAL"),
        F.col("detlkpextr.ISSUER_CSR_TOTAL").alias("ISSUER_CSR_TOTAL"),
        F.col("detlkpextr.ISSUER_UF_TOTAL").alias("ISSUER_UF_TOTAL"),
        F.col("detlkpextr.LAST_NAME").alias("LAST_NAME"),
        F.col("detlkpextr.FIRST_NAME").alias("FIRST_NAME"),
        F.col("detlkpextr.MIDDLE_NAME").alias("MIDDLE_NAME"),
        F.col("detlkpextr.NAME_PREFIX").alias("NAME_PREFIX"),
        F.col("detlkpextr.NAME_SUFFIX").alias("NAME_SUFFIX"),
        F.col("detlkpextr.EXCHANGE_ASSIGNED_SUBSCRIBER_ID").alias("EXCHANGE_ASSIGNED_SUBSCRIBER_ID"),
        F.col("detlkpextr.EXCHANGE_ASSIGNED_QHP_ID").alias("EXCHANGE_ASSIGNED_QHP_ID"),
        F.col("detlkpextr.EXCHANGE_ASSIGNED_POLICY_ID").alias("EXCHANGE_ASSIGNED_POLICY_ID"),
        F.col("detlkpextr.ISSUER_ASSIGNED_POLICY_ID").alias("ISSUER_ASSIGNED_POLICY_ID"),
        F.col("detlkpextr.ISSUER_ASSIGNED_SUBSCRIBER_ID").alias("ISSUER_ASSIGNED_SUBSCRIBER_ID"),
        F.col("detlkpextr.POLICY_TOTAL_PREMIUM_AMOUNT").alias("POLICY_TOTAL_PREMIUM_AMOUNT"),
        F.col("detlkpextr.EXCHANGE_PAYMENT_TYPE").alias("EXCHANGE_PAYMENT_TYPE"),
        F.col("detlkpextr.PAYMENT_AMOUNT").alias("PAYMENT_AMOUNT"),
        F.col("detlkpextr.EXCHANGE_RELATED_REPORT_TYPE").alias("EXCHANGE_RELATED_REPORT_TYPE"),
        F.col("detlkpextr.EXCHANGE_REPORT_DOCUMENT_CONTROL_NUMBER").alias("EXCHANGE_REPORT_DOCUMENT_CONTROL_NUMBER"),
        F.col("detlkpextr.COVERAGE_PERIOD_START_DATE").alias("COVERAGE_PERIOD_START_DATE"),
        F.col("detlkpextr.COVERAGE_PERIOD_END_DATE").alias("COVERAGE_PERIOD_END_DATE"),
        F.col("headerlkpextr.TRANSACTION_SET_CONTROL_NUMBER").alias("TRANSACTION_SET_CONTROL_NUMBER"),
        F.col("headerlkpextr.RUN_DATE").alias("RUN_DATE"),
        F.col("headerlkpextr.TOTAL_PAYMENT").alias("TOTAL_PAYMENT"),
        F.col("headerlkpextr.PAYEE_APTC_TOTAL").alias("PAYEE_APTC_TOTAL"),
        F.col("headerlkpextr.PAYEE_UF_TOTAL").alias("PAYEE_UF_TOTAL")
    )
)

# transfclmstlmtext (CTransformerStage) => 3 outputs
df_lkpamtextr = (
    df_lkpheadet
    .withColumn("DUMMY_COL", F.lit("Y"))
    .withColumn("TOTAL_PAYMENT", StringToDecimal(F.col("TOTAL_PAYMENT")))
    .withColumn("PAYEE_APTC_TOTAL", StringToDecimal(F.col("PAYEE_APTC_TOTAL")))
    .withColumn("PAYEE_UF_TOTAL", StringToDecimal(F.col("PAYEE_UF_TOTAL")))
    .select("DUMMY_COL", "TOTAL_PAYMENT", "PAYEE_APTC_TOTAL", "PAYEE_UF_TOTAL")
)

df_totpayaggextr = (
    df_lkpheadet
    .withColumn("KEY", F.lit("Y"))
    .withColumn("PAYMENT_AMOUNT", StringToDecimal(F.col("PAYMENT_AMOUNT")))
    .select("KEY", "PAYMENT_AMOUNT")
)

df_ufaptcextr = (
    df_lkpheadet
    .withColumn("KEY", F.lit("Y"))
    .withColumn("ISSUER_UF_TOTAL", StringToDecimal(F.col("ISSUER_UF_TOTAL")))
    .withColumn("ISSUER_APTC_TOTAL", StringToDecimal(F.col("ISSUER_APTC_TOTAL")))
    .select("KEY", "ISSUER_UF_TOTAL", "ISSUER_APTC_TOTAL")
)

# aggtotpay (PxAggregator)
df_aggtotpay = (
    df_totpayaggextr
    .groupBy("KEY")
    .agg(F.sum("PAYMENT_AMOUNT").alias("SUM_PAYMENT_AMOUNT"))
    .select("KEY", "SUM_PAYMENT_AMOUNT")
)

# rm_dups_UF_APTC (PxRemDup)
df_rm_dups_UF_APTC = dedup_sort(
    df_ufaptcextr,
    partition_cols=["KEY","ISSUER_APTC_TOTAL","ISSUER_UF_TOTAL"],
    sort_cols=[]
).select("KEY","ISSUER_UF_TOTAL","ISSUER_APTC_TOTAL")

# aggufaptc (PxAggregator)
df_aggufaptc = (
    df_rm_dups_UF_APTC
    .groupBy("KEY")
    .agg(
        F.sum("ISSUER_UF_TOTAL").alias("SUM_ISSUER_UF_TOTAL"),
        F.sum("ISSUER_APTC_TOTAL").alias("SUM_ISSUER_APTC_TOTAL")
    )
    .select("KEY","SUM_ISSUER_UF_TOTAL","SUM_ISSUER_APTC_TOTAL")
)

# lkpsumamt (PxLookup)
df_lkpsumamt_temp = (
    df_lkpamtextr.alias("lkpamtextr")
    .join(
        df_aggtotpay.alias("totpaylkpextr"),
        F.col("lkpamtextr.DUMMY_COL") == F.col("totpaylkpextr.KEY"),
        how="left"
    )
)

df_lkpsumamt = (
    df_lkpsumamt_temp
    .join(
        df_aggufaptc.alias("ufaptclkpextr"),
        F.col("lkpamtextr.DUMMY_COL") == F.col("ufaptclkpextr.KEY"),
        how="left"
    )
    .select(
        F.col("ufaptclkpextr.SUM_ISSUER_UF_TOTAL").alias("SUM_ISSUER_UF_TOTAL"),
        F.col("ufaptclkpextr.SUM_ISSUER_APTC_TOTAL").alias("SUM_ISSUER_APTC_TOTAL"),
        F.col("totpaylkpextr.SUM_PAYMENT_AMOUNT").alias("SUM_PAYMENT_AMOUNT"),
        F.col("lkpamtextr.TOTAL_PAYMENT").alias("TOTAL_PAYMENT"),
        F.col("lkpamtextr.PAYEE_APTC_TOTAL").alias("PAYEE_APTC_TOTAL"),
        F.col("lkpamtextr.PAYEE_UF_TOTAL").alias("PAYEE_UF_TOTAL")
    )
)

# rm_dups_sum (PxRemDup)
df_rm_dups_sum = dedup_sort(
    df_lkpsumamt,
    partition_cols=[
        "PAYEE_APTC_TOTAL",
        "PAYEE_UF_TOTAL",
        "TOTAL_PAYMENT",
        "SUM_ISSUER_APTC_TOTAL",
        "SUM_ISSUER_UF_TOTAL",
        "SUM_PAYMENT_AMOUNT"
    ],
    sort_cols=[]
).select(
    "SUM_ISSUER_UF_TOTAL",
    "SUM_ISSUER_APTC_TOTAL",
    "SUM_PAYMENT_AMOUNT",
    "TOTAL_PAYMENT",
    "PAYEE_APTC_TOTAL",
    "PAYEE_UF_TOTAL"
)

# transfval (CTransformerStage)
df_transfval_stagevars = (
    df_rm_dups_sum
    .withColumn(
        "APTCVal",
        F.when(F.col("SUM_ISSUER_APTC_TOTAL") == F.col("PAYEE_APTC_TOTAL"), F.lit("Y")).otherwise(F.lit("N"))
    )
    .withColumn(
        "UFVal",
        F.when(F.col("SUM_ISSUER_UF_TOTAL") == F.col("PAYEE_UF_TOTAL"), F.lit("Y")).otherwise(F.lit("N"))
    )
    .withColumn(
        "TOTALVal",
        F.when(F.col("SUM_PAYMENT_AMOUNT") == F.col("TOTAL_PAYMENT"), F.lit("Y")).otherwise(F.lit("N"))
    )
)

df_finalval = (
    df_transfval_stagevars
    .filter((F.col("APTCVal") == "N") | (F.col("UFVal") == "N") | (F.col("TOTALVal") == "N"))
    .withColumn("REASON", F.lit("SUM OF I820 Detail Amounts  is not Equal to I820 Header Amounts"))
    .select(
        "SUM_ISSUER_UF_TOTAL",
        "SUM_ISSUER_APTC_TOTAL",
        "SUM_PAYMENT_AMOUNT",
        "TOTAL_PAYMENT",
        "PAYEE_APTC_TOTAL",
        "PAYEE_UF_TOTAL",
        "REASON"
    )
)

# I820_Val_Final (PxSequentialFile)
write_files(
    df_finalval,
    f"{adls_path_raw}/landing/I820FileValExtr_{RunID}.csv",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)