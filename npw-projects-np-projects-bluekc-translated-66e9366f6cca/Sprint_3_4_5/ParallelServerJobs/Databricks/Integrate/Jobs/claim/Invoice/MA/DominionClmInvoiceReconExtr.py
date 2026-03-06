# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Process to compare Dominion Invoice Detail to Claim Encounters and create an Email to Business
# MAGIC 
# MAGIC CALLED BY : DominionClmInvoiceReconCntl
# MAGIC 
# MAGIC PROCESSING : DominionClmInvoiceReconExtr
# MAGIC 
# MAGIC MODIFICATIONS :
# MAGIC                                                                                                                                                                                                                DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Kishore Goud            2024-08-29          US628200                  process to compare Invoice Detail to Claim Encounters and 
# MAGIC                                                                                                             create an Alert and Email to Business if out of balance.                IntegrateDev2           Jeyaprasanna              2024-09-04
# MAGIC 
# MAGIC Rahul David            2024-10-14           US634004                  Removed A09 from CLM.CLM_STTUS_CD in CLM_F and                       IntegrateDev2           Jeyaprasanna              2024-11-15
# MAGIC                                                                                                EDW_CLM_F stage

# MAGIC This job reads Dominion Monthly Invoice and EDW CLM_F to create reports for Business
# MAGIC Claim Count and Totals by LOB sent to Business
# MAGIC Reads the data from CLM_F and group by LOB
# MAGIC EdwDominionClmInvoiceReconExtr
# MAGIC 
# MAGIC Version -v1.0
# MAGIC Intiatial creation 
# MAGIC Developed by -kishore 
# MAGIC Date:2024-03-27
# MAGIC Monthly Invoice file from Dominion
# MAGIC Filter Claims based on DISPOSITION_CODE and CLAIM_LINE_STATUS
# MAGIC Totals by CLM_ID
# MAGIC Claim Mismatch Report sent to Business
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
PaidMonthYr = get_widget_value('PaidMonthYr','')
InvFileNm = get_widget_value('InvFileNm','')

# --------------------------------------------------------------------------------
# Stage: CLM_F (DB2ConnectorPX)
# --------------------------------------------------------------------------------
jdbc_url, jdbc_props = get_db_config(edw_secret_name)
extract_query = (
    f"SELECT 'DOMINION' AS SOURCE_SYS, "
    f"CLM_PD_YR_MO_SK, "
    f"CLM.GRP_ID, "
    f"CLM.FNCL_LOB_CD, "
    f"FNC.LOB_NM, "
    f"COUNT(CLM.FNCL_LOB_CD) AS LOB_CNT, "
    f"SUM(CLM_ACTL_PD_AMT) AS TOT_CLM_ACTL_PD_AMT, "
    f"SUM(CLM_LN_TOT_PAYBL_AMT) AS CLM_LN_TOT_PAYBL_AMT "
    f"FROM {EDWOwner}.CLM_F CLM "
    f"JOIN {EDWOwner}.FNCL_LOB_D FNC ON CLM.FNCL_LOB_SK = FNC.FNCL_LOB_SK "
    f"WHERE CLM_PD_YR_MO_SK = '{PaidMonthYr}' "
    f"AND CLM.SRC_SYS_CD = 'DOMINION' "
    f"AND CLM.CLM_STTUS_CD IN ('A02') "
    f"GROUP BY ROLLUP((CLM.GRP_ID, CLM.FNCL_LOB_CD, FNC.LOB_NM, CLM_PD_YR_MO_SK)) "
    f"ORDER BY CLM.GRP_ID, CLM.FNCL_LOB_CD, FNC.LOB_NM, CLM_PD_YR_MO_SK"
)
df_CLM_F = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: Txn_Mnthly_counts (CTransformerStage)
# --------------------------------------------------------------------------------
df_Txn_Mnthly_counts = df_CLM_F.select(
    F.col("SOURCE_SYS"),
    F.col("CLM_PD_YR_MO_SK"),
    F.col("GRP_ID"),
    F.col("FNCL_LOB_CD"),
    F.col("LOB_NM"),
    F.col("LOB_CNT"),
    DecimalToString(F.col("TOT_CLM_ACTL_PD_AMT"), "suppress_zero").alias("TOT_CLM_ACTL_PD_AMT"),
    DecimalToString(F.col("CLM_LN_TOT_PAYBL_AMT"), "suppress_zero").alias("CLM_LN_TOT_PAYBL_AMT")
)

# --------------------------------------------------------------------------------
# Stage: EDW_CLM_LOB_Report (PxSequentialFile)
# --------------------------------------------------------------------------------
df_EDW_CLM_LOB_Report = df_Txn_Mnthly_counts.select(
    "SOURCE_SYS",
    F.rpad(F.trim(F.col("CLM_PD_YR_MO_SK")), 6, " ").alias("CLM_PD_YR_MO_SK"),
    "GRP_ID",
    "FNCL_LOB_CD",
    "LOB_NM",
    "LOB_CNT",
    "TOT_CLM_ACTL_PD_AMT",
    "CLM_LN_TOT_PAYBL_AMT"
)
write_files(
    df_EDW_CLM_LOB_Report,
    f"{adls_path_publish}/external/DOMINION_{PaidMonthYr}_EDW_CLM_LOB_TOTALS.csv",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)

# --------------------------------------------------------------------------------
# Stage: EDW_CLM_F (DB2ConnectorPX)
# --------------------------------------------------------------------------------
jdbc_url, jdbc_props = get_db_config(edw_secret_name)
extract_query = (
    f"SELECT CLM_ID, CLM_PD_DT_SK, CLM_ACTL_PD_AMT "
    f"FROM {EDWOwner}.CLM_F CLM "
    f"WHERE CLM.SRC_SYS_CD = 'DOMINION' "
    f"AND CLM.CLM_STTUS_CD IN ('A02') "
    f"AND CLM_PD_YR_MO_SK = '{PaidMonthYr}'"
)
df_EDW_CLM_F = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: Dominion_Invoice (PxSequentialFile)
# --------------------------------------------------------------------------------
schema_Dominion_Invoice = StructType([
    StructField("RECORD_IDENTIFIER", StringType(), True),
    StructField("member_id", StringType(), True),
    StructField("group_id", StringType(), True),
    StructField("plan_id", StringType(), True),
    StructField("fc_id", StringType(), True),
    StructField("pv_id", StringType(), True),
    StructField("CLAIM_NUMBER", StringType(), True),
    StructField("DATE_OF_SERVICE_START", StringType(), True),
    StructField("DATE_OF_SERVICE_END", StringType(), True),
    StructField("RECEIPT_DATE", StringType(), True),
    StructField("DISPOSITION_CODE", StringType(), True),
    StructField("CHECK_NUMBER", StringType(), True),
    StructField("SUBSCRIBER_FIRST_NAME", StringType(), True),
    StructField("SUBSCRIBER_LAST_NAME", StringType(), True),
    StructField("SUBSCRIBER_ZIP_CODE", StringType(), True),
    StructField("PATIENT_BIRTH_DATE", StringType(), True),
    StructField("PATIENT_FIRST_NAME", StringType(), True),
    StructField("PATIENT_LAST_NAME", StringType(), True),
    StructField("PATIENT_RELATIONSHIP", StringType(), True),
    StructField("NETWORK_INDICATOR", StringType(), True),
    StructField("PATIENT_PAID_AMOUNT", StringType(), True),
    StructField("TOTAL_ALLOWED", StringType(), True),
    StructField("TOTAL_CHARGE_AMOUNT", StringType(), True),
    StructField("LINE_ITEM_COINSURANCE", StringType(), True),
    StructField("TOTAL_COINSURANCE", StringType(), True),
    StructField("TOTAL_COVERED_CHARGES", StringType(), True),
    StructField("TOTAL_DEDUCTIBLE", StringType(), True),
    StructField("TOTAL_NON_COVERED_CHARGES", StringType(), True),
    StructField("TOTAL_NUMBER_CHARGES", StringType(), True),
    StructField("TOTAL_PAID_AMOUNT", StringType(), True),
    StructField("OTHER_CARRIER_AMOUNT", StringType(), True),
    StructField("PAID_REJECT_DATE", StringType(), True),
    StructField("PRIMARY_PROCEDURE_CODE", StringType(), True),
    StructField("PROVIDER_NUMBER", StringType(), True),
    StructField("SERVICING_TAXONOMY_CD", StringType(), True),
    StructField("PROVIDER_ZIP_CODE", StringType(), True),
    StructField("LINE_CHARGE_AMOUNT", StringType(), True),
    StructField("LINE_NONCOVERED_AMOUNT", StringType(), True),
    StructField("ALLOWED_AMOUNT", StringType(), True),
    StructField("NUMBER_OF_SERVICES", StringType(), True),
    StructField("CHARGED_UNITS", StringType(), True),
    StructField("COVERED_UNITS", StringType(), True),
    StructField("REJECT_REASON_CODE", StringType(), True),
    StructField("SERVICE_LINE_NUMBER", StringType(), True),
    StructField("PLAN_PAID_AMOUNT_BY_LINE_ITEM", StringType(), True),
    StructField("TOOTH_CODE", StringType(), True),
    StructField("TOOTH_SURFACE", StringType(), True),
    StructField("QUADRANT", StringType(), True),
    StructField("PATIENT_ADDRESS", StringType(), True),
    StructField("PATIENT_CITY", StringType(), True),
    StructField("PATIENT_STATE", StringType(), True),
    StructField("PATIENT_ZIP", StringType(), True),
    StructField("PROVIDER_INIT", StringType(), True),
    StructField("PROVIDER_LAST_NAME", StringType(), True),
    StructField("PROVIDER_ADDRESS", StringType(), True),
    StructField("PROVIDER_CITY", StringType(), True),
    StructField("PROVIDER_STATE", StringType(), True),
    StructField("POSITIVE_NEGATIVE_INDICATOR", StringType(), True),
    StructField("PATIENT_MIDDLE_INITIAL", StringType(), True),
    StructField("GENDER_CODE", StringType(), True),
    StructField("MEMBER_SSN", StringType(), True),
    StructField("PROCEDURE_CODE_DESCRIPTION", StringType(), True),
    StructField("SUB_GROUP_ID", StringType(), True),
    StructField("PROVIDER_PRACTICE_NAME", StringType(), True),
    StructField("BILLING_VENDOR", StringType(), True),
    StructField("Facility_NPI", StringType(), True),
    StructField("Provider_Practitioner_NPI", StringType(), True),
    StructField("Facility_Tax_ID", StringType(), True),
    StructField("Provider_Practitioner_Tax_ID", StringType(), True),
    StructField("Facility_Tax_SSN", StringType(), True),
    StructField("Provider_Practitioner_SSN", StringType(), True),
    StructField("Provider_Practitioner_First_Name", StringType(), True),
    StructField("LINE_MAX_ALLOWED", StringType(), True),
    StructField("TOTAL_PATIENT_PAID_AMOUNT", StringType(), True),
    StructField("LINE_PROVIDER_RESPONSIBILITY", StringType(), True),
    StructField("LINE_PERCENT_COVERED", StringType(), True),
    StructField("DOCUMENT_NUMBER", StringType(), True),
    StructField("LINE_OTHER_CARRIER_AMOUNT", StringType(), True),
    StructField("LINE_DEDUCTIBLE", StringType(), True),
    StructField("REJECT_REASON_CODE_2", StringType(), True),
    StructField("REJECT_REASON_CODE_3", StringType(), True),
    StructField("REJECT_REASON_CODE_4", StringType(), True),
    StructField("ALT_BEN", StringType(), True),
    StructField("ALTBENALLOWED", StringType(), True),
    StructField("ALTBENFLAG", StringType(), True),
    StructField("CLAIM_LINE_STATUS", StringType(), True),
    StructField("SERV_LINE_ID", StringType(), True),
    StructField("PROVIDER_MIDDLE_INITIAL", StringType(), True),
    StructField("SUBSCRIBER_MIDDLE_INITIAL", StringType(), True),
    StructField("SUBSCRIBER_ADDRESS_LINE_1", StringType(), True),
    StructField("SUBSCRIBER_ADDRESS_LINE_2", StringType(), True),
    StructField("SUBSCRIBER_ADDRESS_CITY_NAME", StringType(), True),
    StructField("SUBSCRIBER_ADDRESS_STATE_CODE", StringType(), True),
    StructField("PATIENT_ADDRESS_LINE_2", StringType(), True),
    StructField("PROVIDER_ADDRESS_LINE_2", StringType(), True),
    StructField("LINE_PATIENT_COPAY", StringType(), True),
    StructField("DIAGNOSIS_CODE1", StringType(), True),
    StructField("DIAGNOSIS_CODE2", StringType(), True),
    StructField("LINK_CLAIM_NUMBER", StringType(), True),
    StructField("PRODUCTION_DATE", StringType(), True),
    StructField("MEDIA_TP_CD", StringType(), True),
    StructField("d_cat_sd", StringType(), True),
    StructField("d_subcat_sd", StringType(), True),
    StructField("DISPOSITION_INDICATOR", StringType(), True),
    StructField("PAYEE_CD", StringType(), True),
    StructField("REVERSE_LINE_ID", StringType(), True),
    StructField("PERSON_ENTERPRISE_ID", StringType(), True),
    StructField("OTHER_INSURANCE_SOURCE", StringType(), True),
    StructField("BILLING_PROVIDER_TAX_ID", StringType(), True),
    StructField("SURFACE_B", StringType(), True),
    StructField("SURFACE_D", StringType(), True),
    StructField("SURFACE_F", StringType(), True),
    StructField("SURFACE_I", StringType(), True),
    StructField("SURFACE_L", StringType(), True),
    StructField("SURFACE_M", StringType(), True),
    StructField("SURFACE_O", StringType(), True)
])

df_Dominion_Invoice = (
    spark.read.format("csv")
    .option("header", True)
    .option("sep", "|")
    .option("quote", None)
    .option("nullValue", None)
    .schema(schema_Dominion_Invoice)
    .load(f"{adls_path_raw}/landing/{InvFileNm}")
)

# --------------------------------------------------------------------------------
# Stage: Txn (CTransformerStage)
# --------------------------------------------------------------------------------
df_Txn = df_Dominion_Invoice.filter(
    (trim(F.col("DISPOSITION_CODE")) == '1') &
    (trim(F.col("CLAIM_LINE_STATUS")) == 'Processed')
).select(
    F.col("CLAIM_NUMBER").alias("CLAIM_NUMBER"),
    F.col("DOCUMENT_NUMBER").alias("DOCUMENT_NUMBER"),
    F.col("PAID_REJECT_DATE").alias("PAID_REJECT_DATE"),
    (StringToDecimal(F.col("PLAN_PAID_AMOUNT_BY_LINE_ITEM")) / 100).alias("PLAN_PAID_AMOUNT_BY_LINE_ITEM")
)

# --------------------------------------------------------------------------------
# Stage: Agg_total_Clm_amt (PxAggregator)
# --------------------------------------------------------------------------------
df_Agg_total_Clm_amt = df_Txn.groupBy(
    "CLAIM_NUMBER",
    "DOCUMENT_NUMBER",
    "PAID_REJECT_DATE"
).agg(
    F.sum("PLAN_PAID_AMOUNT_BY_LINE_ITEM").alias("CLM_PAID_TOT")
)

# --------------------------------------------------------------------------------
# Stage: Format_Data (CTransformerStage)
# --------------------------------------------------------------------------------
df_Format_Data = df_Agg_total_Clm_amt.select(
    F.concat(trim(F.col("CLAIM_NUMBER")), F.lit("00")).alias("CLM_ID"),
    F.col("DOCUMENT_NUMBER").alias("DOCUMENT_NUMBER"),
    FORMAT.DATE.EE(F.col("PAID_REJECT_DATE"), 'DATE', 'MM/DD/CCYY', 'CCYY-MM-DD').alias("CLM_PD_DT_INVOICE"),
    F.col("CLM_PAID_TOT").alias("CLM_ACTL_PD_AMT_INVOICE")
)

# --------------------------------------------------------------------------------
# Stage: Lkp_Edw (PxLookup)
# --------------------------------------------------------------------------------
df_Lkp_Edw = df_Format_Data.alias("Lnk_Invoice").join(
    df_EDW_CLM_F.alias("Lnk_Clmf"),
    df_Format_Data["CLM_ID"] == df_EDW_CLM_F["CLM_ID"],
    how="left"
).select(
    F.col("Lnk_Invoice.CLM_ID").alias("CLM_ID"),
    F.col("Lnk_Invoice.DOCUMENT_NUMBER").alias("DOCUMENT_NUMBER"),
    F.col("Lnk_Invoice.CLM_PD_DT_INVOICE").alias("CLM_PD_DT_INVOICE"),
    F.col("Lnk_Invoice.CLM_ACTL_PD_AMT_INVOICE").alias("CLM_ACTL_PD_AMT_INVOICE"),
    F.col("Lnk_Clmf.CLM_PD_DT_SK").alias("CLM_PD_DT_EDW"),
    F.col("Lnk_Clmf.CLM_ACTL_PD_AMT").alias("CLM_ACTL_PD_AMT_EDW")
)

# --------------------------------------------------------------------------------
# Stage: Business_Rules (CTransformerStage)
# --------------------------------------------------------------------------------
df_Business_Rules = (
    df_Lkp_Edw
    .withColumn(
        "svMisMatch",
        F.when(trim(F.col("CLM_PD_DT_EDW")) == '', 'Y')
        .otherwise(
            F.when(F.col("CLM_ACTL_PD_AMT_EDW") != F.col("CLM_ACTL_PD_AMT_INVOICE"), 'Y')
            .otherwise(
                F.when(F.col("CLM_PD_DT_EDW") != F.col("CLM_PD_DT_INVOICE"), 'Y')
                .otherwise('N')
            )
        )
    )
    .withColumn(
        "svMisMatchReason",
        F.when(trim(F.col("CLM_PD_DT_EDW")) == '', 'Claim missing in EDW')
        .otherwise(
            F.when(
                (F.col("CLM_ACTL_PD_AMT_EDW") != F.col("CLM_ACTL_PD_AMT_INVOICE")) &
                (F.col("CLM_PD_DT_EDW") != F.col("CLM_PD_DT_INVOICE")),
                'Claim Paid Date and Claim Paid Total Amount Mismatch'
            )
            .otherwise(
                F.when(
                    F.col("CLM_ACTL_PD_AMT_EDW") == F.col("CLM_ACTL_PD_AMT_INVOICE"),
                    'Claim Paid Date Mismatch'
                )
                .otherwise('Claim Paid Total Amount Mismatch')
            )
        )
    )
)

df_Mismatch_Report = df_Business_Rules.filter(
    F.col("svMisMatch") == 'Y'
).select(
    F.col("CLM_ID"),
    F.col("DOCUMENT_NUMBER"),
    F.rpad(trim(F.col("CLM_PD_DT_INVOICE")), 10, " ").alias("CLM_PD_DT_INVOICE"),
    F.col("CLM_ACTL_PD_AMT_INVOICE").alias("CLM_ACTL_PD_INVOICE"),
    F.rpad(trim(F.col("CLM_PD_DT_EDW")), 10, " ").alias("CLM_PD_DT_EDW"),
    F.col("CLM_ACTL_PD_AMT_EDW"),
    F.col("svMisMatchReason").alias("MISMATCH_REASON")
)

# --------------------------------------------------------------------------------
# Stage: Mismatch_Report (PxSequentialFile)
# --------------------------------------------------------------------------------
write_files(
    df_Mismatch_Report,
    f"{adls_path_publish}/external/DOMINION_{PaidMonthYr}_MISMATCH_REPORT.csv",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)