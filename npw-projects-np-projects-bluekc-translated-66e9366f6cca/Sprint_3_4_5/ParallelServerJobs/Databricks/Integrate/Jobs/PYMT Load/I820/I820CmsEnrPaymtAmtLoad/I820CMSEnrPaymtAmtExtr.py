# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Job: I820CMSEnrPaymtAmtExtr
# MAGIC 
# MAGIC This job Extracts data from I820  file
# MAGIC 
# MAGIC CALLED BY: CMSI820BCBSFinanceTablesLoadSeq
# MAGIC 
# MAGIC Developer                                            Date                          Project or Ticket                         Description                                    Environment                   Code Reviewer               Review Date
# MAGIC ------------------------                           -----------------------          ---------------------------------------            ---------------------------------------------------    -----------------------------          -----------------------------          ------------------------------
# MAGIC Raj Kommineni                               2021-12-15              US390105                                        Original programming                     IntegrateDev2           Jeyaprasanna                 2021-12-18   
# MAGIC Vikas Abbu                                    2022-04-18               S2S                                         Updated Facets to SQL server              IntegrateDev5

# MAGIC Left Join on Natural Key. CMS_ENR_PAYMT_DTL_CK will be available for every row from the input
# MAGIC Sort on CK to generate seq no
# MAGIC This job generates the intermediate file I820_CMS_ENR_PAYMT_VALENCIA.dat
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
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Parameter retrieval
RunID = get_widget_value('RunID','')
BCBSFINOwner = get_widget_value('BCBSFINOwner','')
bcbs_secret_name = get_widget_value('bcbs_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
PrvMtDate = get_widget_value('PrvMtDate','')

# 1) Stage: CMS_ENR_PAYMT_DTL (ODBCConnectorPX)
jdbc_url_bcbs, jdbc_props_bcbs = get_db_config(bcbs_secret_name)
df_CMS_ENR_PAYMT_DTL = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_bcbs)
    .options(**jdbc_props_bcbs)
    .option(
        "query",
        f"SELECT DTL.CMS_ENR_PAYMT_DTL_CK, DTL.EXCH_MBR_ID, DTL.QHP_ID, DTL.PAYMT_SUBMT_DT, DTL.PAYMT_COV_YRMO, DTL.ST_CD, DTL.SRC_SYS_CD, DTL.ENR_TRANS_TYP_CD "
        f"FROM {BCBSFINOwner}.CMS_ENR_PAYMT_DTL DTL "
        f"WHERE AS_OF_DT = '{PrvMtDate}'"
    )
    .load()
)

# 2) Stage: DB2_CD_MPPNG (DB2ConnectorPX)
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
df_DB2_CD_MPPNG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"SELECT DISTINCT 'Y' as DUMMY_COL, SRC_CD "
        f"FROM {IDSOwner}.CD_MPPNG "
        f"WHERE SRC_CD = 'CMS' "
        f"AND SRC_SYS_CD = 'IDS' "
        f"AND SRC_CLCTN_CD = 'IDS' "
        f"AND SRC_DOMAIN_NM = 'SOURCE SYSTEM' "
        f"AND TRGT_CLCTN_CD = 'IDS' "
        f"AND TRGT_DOMAIN_NM = 'SOURCE SYSTEM'"
    )
    .load()
)

# 3) Stage: I820HeaderFile (PxSequentialFile)
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
df_I820HeaderFile = spark.read.schema(header_schema) \
    .option("header", "false") \
    .option("sep", " ") \
    .csv(f"{adls_path_raw}/landing/I820HeaderFile_{RunID}.dat")

# 4) Stage: transfheadkey (CTransformerStage)
df_transfheadkey = df_I820HeaderFile.select(
    F.col("TRANSACTION_SET_CONTROL_NUMBER").alias("TRANSACTION_SET_CONTROL_NUMBER"),
    F.col("RUN_DATE").alias("RUN_DATE"),
    F.col("PAYEE_ID").alias("PAYEE_ID"),
    F.col("PAYMENT_METHOD_CODE").alias("PAYMENT_METHOD_CODE"),
    F.col("POLICY_BASED_TRANSITION_MONTH").alias("POLICY_BASED_TRANSITION_MONTH"),
    F.col("TOTAL_PAYMENT").alias("TOTAL_PAYMENT"),
    F.col("PAYEE_APTC_TOTAL").alias("PAYEE_APTC_TOTAL"),
    F.col("PAYEE_CSR_TOTAL").alias("PAYEE_CSR_TOTAL"),
    F.col("PAYEE_UF_TOTAL").alias("PAYEE_UF_TOTAL"),
    F.lit("Y").alias("KEY")
)

# 5) Stage: I820DetailFile (PxSequentialFile)
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
df_I820DetailFile = spark.read.schema(detail_schema) \
    .option("header", "false") \
    .option("sep", "|") \
    .csv(f"{adls_path_raw}/landing/I820DetailFile_{RunID}.dat")

# 6) Stage: transfkey (CTransformerStage)
df_transfkey = df_I820DetailFile.select(
    F.col("ISSUER_ID").alias("ISSUER_ID"),
    F.col("ISSUER_APTC_TOTAL").alias("ISSUER_APTC_TOTAL"),
    F.col("ISSUER_CSR_TOTAL").alias("ISSUER_CSR_TOTAL"),
    F.col("ISSUER_UF_TOTAL").alias("ISSUER_UF_TOTAL"),
    F.col("LAST_NAME").alias("LAST_NAME"),
    F.col("FIRST_NAME").alias("FIRST_NAME"),
    F.col("MIDDLE_NAME").alias("MIDDLE_NAME"),
    F.col("NAME_PREFIX").alias("NAME_PREFIX"),
    F.col("NAME_SUFFIX").alias("NAME_SUFFIX"),
    F.col("EXCHANGE_ASSIGNED_SUBSCRIBER_ID").alias("EXCHANGE_ASSIGNED_SUBSCRIBER_ID"),
    F.col("EXCHANGE_ASSIGNED_QHP_ID").alias("EXCHANGE_ASSIGNED_QHP_ID"),
    F.col("EXCHANGE_ASSIGNED_POLICY_ID").alias("EXCHANGE_ASSIGNED_POLICY_ID"),
    F.col("ISSUER_ASSIGNED_POLICY_ID").alias("ISSUER_ASSIGNED_POLICY_ID"),
    F.col("ISSUER_ASSIGNED_SUBSCRIBER_ID").alias("ISSUER_ASSIGNED_SUBSCRIBER_ID"),
    F.col("POLICY_TOTAL_PREMIUM_AMOUNT").alias("POLICY_TOTAL_PREMIUM_AMOUNT"),
    F.col("EXCHANGE_PAYMENT_TYPE").alias("EXCHANGE_PAYMENT_TYPE"),
    F.col("PAYMENT_AMOUNT").alias("PAYMENT_AMOUNT"),
    F.col("EXCHANGE_RELATED_REPORT_TYPE").alias("EXCHANGE_RELATED_REPORT_TYPE"),
    F.col("EXCHANGE_REPORT_DOCUMENT_CONTROL_NUMBER").alias("EXCHANGE_REPORT_DOCUMENT_CONTROL_NUMBER"),
    F.col("COVERAGE_PERIOD_START_DATE").alias("COVERAGE_PERIOD_START_DATE"),
    F.col("COVERAGE_PERIOD_END_DATE").alias("COVERAGE_PERIOD_END_DATE"),
    F.lit("Y").alias("KEY")
)

# 7) Stage: lkpheadet (PxLookup)
df_lkpheadet = (
    df_transfkey.alias("detlkpextr")
    .join(
        df_transfheadkey.alias("headerlkpextr"),
        F.col("detlkpextr.KEY") == F.col("headerlkpextr.KEY"),
        "left"
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
        F.col("headerlkpextr.RUN_DATE").alias("RUN_DATE")
    )
)

# 8) Stage: Trns_CmsEnrPaymtAmt (CTransformerStage)
#    Define stage variables using user-defined functions (assumed available)
df_enriched = (
    df_lkpheadet
    .withColumn("stgDate", DateOffsetByComponents(F.col("RUN_DATE"), F.lit(0), F.lit(-1), F.lit(0)))
    .withColumn("stgYr", YearFromDate(F.col("stgDate")))
    .withColumn(
        "stgMn",
        F.when(
            MonthFromDate(F.col("stgDate")) >= 10,
            MonthFromDate(F.col("stgDate"))
        ).otherwise(
            F.concat(F.lit("0"), MonthFromDate(F.col("stgDate")))
        )
    )
    .withColumn(
        "stgPrevMonthDt",
        F.concat(F.col("stgYr"), F.lit("-"), F.col("stgMn"), F.lit("-15"))
    )
)

df_Trns_CmsEnrPaymtAmt = (
    df_enriched
    .filter(trim("EXCHANGE_ASSIGNED_SUBSCRIBER_ID") != '')
    .select(
        F.expr("CASE WHEN IsNull(EXCHANGE_PAYMENT_TYPE) THEN ' ' ELSE EXCHANGE_PAYMENT_TYPE END").alias("PAYMT_TYP_CD"),
        F.expr("CASE WHEN IsNull(PAYMENT_AMOUNT) THEN (CASE WHEN IsNotNull(PAYMENT_AMOUNT) THEN PAYMENT_AMOUNT ELSE 0 END) ELSE StringToDecimal(PAYMENT_AMOUNT) END").alias("PAYMT_AMT"),
        F.expr("CASE WHEN IsNull(EXCHANGE_ASSIGNED_SUBSCRIBER_ID) THEN ' ' ELSE EXCHANGE_ASSIGNED_SUBSCRIBER_ID END").alias("EXCH_MBR_ID"),
        F.expr("CASE WHEN IsNull(EXCHANGE_ASSIGNED_QHP_ID) THEN ' ' ELSE EXCHANGE_ASSIGNED_QHP_ID END").alias("QHP_ID"),
        F.col("stgPrevMonthDt").alias("PAYMT_SUBMT_DT"),
        F.expr("concat(substring(trim(COVERAGE_PERIOD_START_DATE), 1, 4), substring(trim(COVERAGE_PERIOD_START_DATE), 5, 2))").alias("PAYMT_COV_YRMO"),
        F.expr("CASE WHEN substring(trim(EXCHANGE_ASSIGNED_QHP_ID), 1, 5) = '34762' THEN 'MO' ELSE 'KS' END").alias("ST_CD"),
        F.lit("CONFIRM").alias("ENR_TRANS_TYP_CD"),
        F.lit("Y").alias("DUMMY_COL"),
        F.col("RUN_DATE").alias("RUN_DATE")  # Keep for continuity if needed downstream
    )
)

# 9) Stage: LkpCdMppng (PxLookup)
df_LkpCdMppng = (
    df_Trns_CmsEnrPaymtAmt.alias("Lnk_LkpCdMppng_Out")
    .join(
        df_DB2_CD_MPPNG.alias("LkFrom_CD_MPPNG"),
        F.col("Lnk_LkpCdMppng_Out.DUMMY_COL") == F.col("LkFrom_CD_MPPNG.DUMMY_COL"),
        "left"
    )
    .select(
        F.col("Lnk_LkpCdMppng_Out.PAYMT_TYP_CD").alias("PAYMT_TYP_CD"),
        F.col("Lnk_LkpCdMppng_Out.PAYMT_AMT").alias("PAYMT_AMT"),
        F.col("Lnk_LkpCdMppng_Out.EXCH_MBR_ID").alias("EXCH_MBR_ID"),
        F.col("Lnk_LkpCdMppng_Out.QHP_ID").alias("QHP_ID"),
        F.col("Lnk_LkpCdMppng_Out.PAYMT_SUBMT_DT").alias("PAYMT_SUBMT_DT"),
        F.col("Lnk_LkpCdMppng_Out.PAYMT_COV_YRMO").alias("PAYMT_COV_YRMO"),
        F.col("Lnk_LkpCdMppng_Out.ST_CD").alias("ST_CD"),
        F.col("LkFrom_CD_MPPNG.SRC_CD").alias("SRC_SYS_CD"),
        F.col("Lnk_LkpCdMppng_Out.ENR_TRANS_TYP_CD").alias("ENR_TRANS_TYP_CD")
    )
)

# 10) Stage: lkpCK (PxLookup)
#     Left join with df_CMS_ENR_PAYMT_DTL on multiple columns
df_lkpCK = (
    df_LkpCdMppng.alias("Lnk_Jn_CmsEnrPaymtDtl_Out")
    .join(
        df_CMS_ENR_PAYMT_DTL.alias("CMS_ENR_PAYMT_DTL_EXTR"),
        (
            (F.col("Lnk_Jn_CmsEnrPaymtDtl_Out.EXCH_MBR_ID") == F.col("CMS_ENR_PAYMT_DTL_EXTR.EXCH_MBR_ID"))
            & (F.col("Lnk_Jn_CmsEnrPaymtDtl_Out.QHP_ID") == F.col("CMS_ENR_PAYMT_DTL_EXTR.QHP_ID"))
            & (F.col("Lnk_Jn_CmsEnrPaymtDtl_Out.PAYMT_SUBMT_DT") == F.col("CMS_ENR_PAYMT_DTL_EXTR.PAYMT_SUBMT_DT"))
            & (F.col("Lnk_Jn_CmsEnrPaymtDtl_Out.PAYMT_COV_YRMO") == F.col("CMS_ENR_PAYMT_DTL_EXTR.PAYMT_COV_YRMO"))
            & (F.col("Lnk_Jn_CmsEnrPaymtDtl_Out.ST_CD") == F.col("CMS_ENR_PAYMT_DTL_EXTR.ST_CD"))
            & (F.col("Lnk_Jn_CmsEnrPaymtDtl_Out.SRC_SYS_CD") == F.col("CMS_ENR_PAYMT_DTL_EXTR.SRC_SYS_CD"))
            & (F.col("Lnk_Jn_CmsEnrPaymtDtl_Out.ENR_TRANS_TYP_CD") == F.col("CMS_ENR_PAYMT_DTL_EXTR.ENR_TRANS_TYP_CD"))
        ),
        "left"
    )
    .select(
        F.col("Lnk_Jn_CmsEnrPaymtDtl_Out.PAYMT_TYP_CD").alias("PAYMT_TYP_CD"),
        F.col("Lnk_Jn_CmsEnrPaymtDtl_Out.PAYMT_AMT").alias("PAYMT_AMT"),
        F.col("CMS_ENR_PAYMT_DTL_EXTR.CMS_ENR_PAYMT_DTL_CK").alias("CMS_ENR_PAYMT_DTL_CK")
    )
)

# 11) Stage: SortDtlPaymtCk (PxSort) - sort by CMS_ENR_PAYMT_DTL_CK (stable)
df_SortDtlPaymtCk = df_lkpCK.orderBy(F.col("CMS_ENR_PAYMT_DTL_CK"))

# 12) Stage: Trns_SeqNo (CTransformerStage)
#     We emulate svSEQNO logic via row_number over partitionBy(CMS_ENR_PAYMT_DTL_CK)
window_ck = Window.partitionBy("CMS_ENR_PAYMT_DTL_CK").orderBy(F.col("CMS_ENR_PAYMT_DTL_CK"))

df_Trns_SeqNo = (
    df_SortDtlPaymtCk
    .withColumn("svCsrChk",
        F.when(
            (F.col("PAYMT_TYP_CD") == "CSR") | (F.col("PAYMT_TYP_CD") == "CSRADJ"), 'Y'
        ).otherwise('N')
    )
    .withColumn("SvPaymtTypeCd",
        F.when(
            (F.col("PAYMT_TYP_CD") == "UF") | (F.col("PAYMT_TYP_CD") == "UFADJ") | (F.col("PAYMT_TYP_CD") == "UFMADJ"), 1
        ).otherwise(0)
    )
    .withColumn("CMS_ENR_PAYMT_AMT_SEQ_NO", F.row_number().over(window_ck))
)

df_Trns_SeqNo_filtered = df_Trns_SeqNo.filter(F.col("svCsrChk") == 'N').select(
    F.col("CMS_ENR_PAYMT_DTL_CK"),
    F.col("PAYMT_TYP_CD"),
    F.col("CMS_ENR_PAYMT_AMT_SEQ_NO"),
    F.expr("CASE WHEN SvPaymtTypeCd = 1 THEN -1 * PAYMT_AMT ELSE PAYMT_AMT END").alias("PAYMT_AMT")
)

# 13) Stage: SF_CMSEnrPaymtAmtExtr (PxSequentialFile) - write to DAT
#     Maintain the column order and types. None are declared as char with length in final stage, so no rpad needed.
final_cols = [
    "CMS_ENR_PAYMT_DTL_CK",
    "PAYMT_TYP_CD",
    "CMS_ENR_PAYMT_AMT_SEQ_NO",
    "PAYMT_AMT"
]
df_final = df_Trns_SeqNo_filtered.select(final_cols)

write_files(
    df_final,
    f"{adls_path}/load/I820_CMS_ENR_PAYMT_AMT_VALENCIA.DAT",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote=None,
    nullValue=None
)