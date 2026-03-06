# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Job: I820CMSEnrPaymtAmtPolAmtValExtr
# MAGIC 
# MAGIC This job validates the sum of PAYMT_AMT in CMS_ENR_PAYMT_AMT table with TOTAL_POLICY_AMOUNT from Gorman Policy  file
# MAGIC 
# MAGIC CALLED BY: GORMANI820BCBSFinanceTablesLoadSeq
# MAGIC 
# MAGIC Developer                                            Date                          Project or Ticket                         Description                                    Environment                   Code Reviewer               Review Date
# MAGIC ------------------------                           -----------------------          ---------------------------------------            ---------------------------------------------------    -----------------------------          -----------------------------          ------------------------------
# MAGIC Raj Kommineni                               2021-12-15              US390105                                        Original programming                     IntegrateDev2            Jeyaprasanna                      2021-12-18	
# MAGIC Prabhu ES                                     2022-04-27               S2S                                       MSSQL ODBC conn params added         IntegrateDev5 		Harsha Ravuri	06-03-2022
# MAGIC Vikas Abbu                                     2022-05-20               S2S                                       Fixed Datatype issues                              IntegrateDev5  		Harsha Ravuri	06-03-2022

# MAGIC This job validates sum of CMS_ENR_PAYMT_AMT.PAYMT_AMT to 820_FFM_POLICY.TOTAL_POLICY_AMOUNT
# MAGIC Sum of Paymt_amt
# MAGIC Inner Join on Dummy to get total payment amount from Policy
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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


RunID = get_widget_value('RunID','')
BCBSFINOwner = get_widget_value('BCBSFINOwner','')
bcbsfin_secret_name = get_widget_value('bcbsfin_secret_name','')
PrvMtDate = get_widget_value('PrvMtDate','')
GormPolicyFile = get_widget_value('GormPolicyFile','')

schema_Valencia_820FfmPolicy = StructType([
    StructField("HIX_820_CMS_ID", IntegerType(), True),
    StructField("RUN_DATE", StringType(), True),
    StructField("TRADING_PARTNER_ID", StringType(), True),
    StructField("EFT_EFFECTIVE_DATE", StringType(), True),
    StructField("EFT_TRACE_NUMBER", StringType(), True),
    StructField("EXCHANGE_TOTAL_EFT_PAYMENT_AMOUNT", DecimalType(38,10), True),
    StructField("SUBSCRIBER_FIRST_NAME", StringType(), True),
    StructField("SUBSCRIBER_LAST_NAME", StringType(), True),
    StructField("EXCHANGE_ASSIGNED_SUBSCRIBER_ID", StringType(), True),
    StructField("EXCHANGE_ASSIGNED_QHP_ID", StringType(), True),
    StructField("EXCHANGE_ASSIGNED_POLICY_ID", StringType(), True),
    StructField("SUBSCRIBER_ID", StringType(), True),
    StructField("PAYMENT_TYPE", StringType(), True),
    StructField("PAYMENT_AMOUNT", DecimalType(38,10), True),
    StructField("REPORT_TYPE", StringType(), True),
    StructField("REPORT_DCN", StringType(), True),
    StructField("PAYMENT_START_DATE", StringType(), True),
    StructField("PAYMENT_END_DATE", StringType(), True),
    StructField("FILE_TYPE", StringType(), True),
    StructField("LOAD_FILE_NAME", StringType(), True),
    StructField("CREATED_BY", StringType(), True),
    StructField("CREATED_ON", StringType(), True),
    StructField("ACTIVE_IND", StringType(), True),
    StructField("TOTAL_POLICY_AMOUNT", DecimalType(38,10), True)
])

df_Valencia_820FfmPolicy = (
    spark.read
    .options(delimiter=",", header=True)
    .schema(schema_Valencia_820FfmPolicy)
    .csv(f"{adls_path_raw}/landing/{GormPolicyFile}")
)

df_Trns_Dummy = (
    df_Valencia_820FfmPolicy
    .select(
        F.col("TOTAL_POLICY_AMOUNT"),
        F.lit("Y").alias("DUMMY_COL")
    )
)

jdbc_url_bcbsfin, jdbc_props_bcbsfin = get_db_config(bcbsfin_secret_name)
extract_query_bcbsfin = (
    f"SELECT A.CMS_ENR_PAYMT_DTL_CK, A.PAYMT_TYP_CD, 'Y' AS DUMMY_COL, A.PAYMT_AMT "
    f"FROM {BCBSFINOwner}.CMS_ENR_PAYMT_AMT A, {BCBSFINOwner}.CMS_ENR_PAYMT_DTL B "
    f"WHERE A.CMS_ENR_PAYMT_DTL_CK = B.CMS_ENR_PAYMT_DTL_CK AND B.AS_OF_DT = '{PrvMtDate}'"
)

df_CMS_ENR_PAYMT_AMT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_bcbsfin)
    .options(**jdbc_props_bcbsfin)
    .option("query", extract_query_bcbsfin)
    .load()
)

df_Trns_PaymtAmt = (
    df_CMS_ENR_PAYMT_AMT
    .withColumn(
        "SvPaymtTypeCd",
        F.when(
            (F.col("PAYMT_TYP_CD") == "UF") | 
            (F.col("PAYMT_TYP_CD") == "UFADJ") | 
            (F.col("PAYMT_TYP_CD") == "UFMADJ"),
            F.lit(1)
        ).otherwise(F.lit(0))
    )
    .withColumn(
        "PAYMT_AMT",
        F.when(
            F.col("SvPaymtTypeCd") == 1,
            -1 * F.col("PAYMT_AMT")
        ).otherwise(F.col("PAYMT_AMT"))
    )
    .select(
        "CMS_ENR_PAYMT_DTL_CK",
        "PAYMT_TYP_CD",
        "PAYMT_AMT",
        "DUMMY_COL"
    )
)

df_Aggr_PaymtAmt = (
    df_Trns_PaymtAmt
    .groupBy("DUMMY_COL")
    .agg(
        F.sum("PAYMT_AMT").alias("SUM_PAYMT_AMT")
    )
    .select("SUM_PAYMT_AMT", "DUMMY_COL")
)

df_Jn_Dummy = (
    df_Trns_Dummy.alias("LnkInto_Jn_Dummy")
    .join(
        df_Aggr_PaymtAmt.alias("Lnk_Jn_Dummy_Out"),
        F.col("LnkInto_Jn_Dummy.DUMMY_COL") == F.col("Lnk_Jn_Dummy_Out.DUMMY_COL"),
        "inner"
    )
    .select(
        F.col("LnkInto_Jn_Dummy.TOTAL_POLICY_AMOUNT").alias("TOTAL_POLICY_AMOUNT"),
        F.col("Lnk_Jn_Dummy_Out.SUM_PAYMT_AMT").alias("SUM_PAYMT_AMT")
    )
)

df_Valencia_PaymtAmtRej = (
    df_Jn_Dummy
    .withColumn(
        "svAmtCheck",
        F.when(
            F.col("SUM_PAYMT_AMT") == F.col("TOTAL_POLICY_AMOUNT"),
            F.lit(1)
        ).otherwise(F.lit(0))
    )
    .filter(F.col("svAmtCheck") == 0)
    .withColumn(
        "REASON",
        F.lit("SUM OF CMS_ENR_PAYMT_AMT.PAYMT_AMT is not Equal to 820_FFM_POLICY.TOTAL_POLICY_AMOUNT")
    )
    .select("TOTAL_POLICY_AMOUNT","SUM_PAYMT_AMT","REASON")
)

df_Valencia_PaymtAmtRej_final = (
    df_Valencia_PaymtAmtRej
    .withColumn("REASON", rpad(F.col("REASON"), <...>, " "))
    .select("TOTAL_POLICY_AMOUNT","SUM_PAYMT_AMT","REASON")
)

write_files(
    df_Valencia_PaymtAmtRej_final,
    f"{adls_path_raw}/landing/CMS_820PayPolamtRejAftr_{RunID}.csv",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)