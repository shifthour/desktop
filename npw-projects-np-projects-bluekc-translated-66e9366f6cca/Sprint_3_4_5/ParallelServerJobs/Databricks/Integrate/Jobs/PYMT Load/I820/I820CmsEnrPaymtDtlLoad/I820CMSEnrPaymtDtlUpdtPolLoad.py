# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Job: I820CMSEnrPaymtDtlUpdtPolLoad
# MAGIC 
# MAGIC This job Updates EFT_TRACE_ID in CMS_ENR_PAYMT_AMT table from GORMAN Policy  file
# MAGIC 
# MAGIC CALLED BY: GORMANI820BCBSFinanceTablesLoadSeq
# MAGIC 
# MAGIC Developer                                            Date                          Project or Ticket                         Description                                    Environment                   Code Reviewer               Review Date
# MAGIC ------------------------                           -----------------------          ---------------------------------------            ---------------------------------------------------    -----------------------------          -----------------------------          ------------------------------
# MAGIC Raj Kommineni                               2021-12-15              US390105                                        Original programming                     IntegrateDev2            Jeyaprasanna                2021-12-18 
# MAGIC 
# MAGIC Raj Kommineni                               2022-01-18              US481795                               Updated transformation rule for              IntegrateDev2            Jeyaprasanna                 2022-01-31
# MAGIC                                                                                                                                        EXCHANGE_ASSIGNED_SUBSCRIBER_ID 
# MAGIC                                                                                                                                        in Xfm_Business_Logic1       
# MAGIC Prabhu ES                                     2022-04-07               S2S                                         MSSQL ODBC conn params added       IntegrateDev5 		Harsha Ravuri	06-02-2022

# MAGIC Lookup to map the keys from policy file and table
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
from pyspark.sql.functions import col, lit, when, length, substring, concat, to_timestamp, to_date
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


bcbsfin_secret_name = get_widget_value('bcbsfin_secret_name','')
PrvMtDate = get_widget_value('PrvMtDate','')
RunTS = get_widget_value('RunTS','')
GormPolicyFile = get_widget_value('GormPolicyFile','')

jdbc_url, jdbc_props = get_db_config(bcbsfin_secret_name)

# -- Stage: CMS_ENR_PAYMT_DTL (ODBCConnectorPX)
extract_query = f"SELECT CMS_ENR_PAYMT_DTL_CK, EXCH_MBR_ID, QHP_ID, PAYMT_SUBMT_DT, PAYMT_COV_YRMO, ST_CD, SRC_SYS_CD, TOT_PRM_AMT, COV_STRT_DT, COV_END_DT, ENR_TRANS_TYP_CD, FCTS_SUB_ID, FCTS_PROD_ID, MBR_RELSHP_TYP_CD, MBR_PRM_AMT, PAYMT_RCVD_DT, AS_OF_DT, BILL_ENTY_UNIQ_KEY, CLS_PLN_ID, GRP_LVL_BILL_ENTY_UNIQ_KEY, EFT_TRACE_ID, ENR_PAYMT_UNIQ_KEY, SUB_FIRST_NM, SUB_LAST_NM, EXCH_ASG_POL_ID, EXCH_ASG_SUB_ID, EXCH_RPT_DOC_CTL_NO, EXCH_RPT_NM, CRT_DT, LAST_UPDT_DT FROM #$BCBSFINOwner#.CMS_ENR_PAYMT_DTL WHERE AS_OF_DT = '{PrvMtDate}'"
df_CMS_ENR_PAYMT_DTL = (
    spark.read.format("jdbc")
        .option("url", jdbc_url)
        .options(**jdbc_props)
        .option("query", extract_query)
        .load()
)

# -- Stage: FfmPolicy (PxSequentialFile)
schema_FfmPolicy = StructType([
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

df_FfmPolicy = (
    spark.read.format("csv")
    .option("header", True)
    .option("delimiter", ",")
    .schema(schema_FfmPolicy)
    .load(f"{adls_path_raw}/landing/{GormPolicyFile}")
)

# -- Stage: srt_Lnk_Src_in (PxSort)
df_srt_Lnk_Src_in = df_FfmPolicy.orderBy(
    col("EXCHANGE_ASSIGNED_SUBSCRIBER_ID"),
    col("EXCHANGE_ASSIGNED_QHP_ID"),
    col("PAYMENT_START_DATE"),
    col("RUN_DATE")
)

df_Lnk_Rmv_AltKeys = df_srt_Lnk_Src_in.select(
    col("HIX_820_CMS_ID").alias("HIX_820_CMS_ID"),
    col("RUN_DATE").alias("RUN_DATE"),
    col("TRADING_PARTNER_ID").alias("TRADING_PARTNER_ID"),
    col("EFT_EFFECTIVE_DATE").alias("EFT_EFFECTIVE_DATE"),
    col("EFT_TRACE_NUMBER").alias("EFT_TRACE_NUMBER"),
    col("EXCHANGE_TOTAL_EFT_PAYMENT_AMOUNT").alias("EXCHANGE_TOTAL_EFT_PAYMENT_AMOUNT"),
    col("SUBSCRIBER_FIRST_NAME").alias("SUBSCRIBER_FIRST_NAME"),
    col("SUBSCRIBER_LAST_NAME").alias("SUBSCRIBER_LAST_NAME"),
    col("EXCHANGE_ASSIGNED_SUBSCRIBER_ID").alias("EXCHANGE_ASSIGNED_SUBSCRIBER_ID"),
    col("EXCHANGE_ASSIGNED_QHP_ID").alias("EXCHANGE_ASSIGNED_QHP_ID"),
    col("EXCHANGE_ASSIGNED_POLICY_ID").alias("EXCHANGE_ASSIGNED_POLICY_ID"),
    col("SUBSCRIBER_ID").alias("SUBSCRIBER_ID"),
    col("PAYMENT_TYPE").alias("PAYMENT_TYPE"),
    col("PAYMENT_AMOUNT").alias("PAYMENT_AMOUNT"),
    col("REPORT_TYPE").alias("REPORT_TYPE"),
    col("REPORT_DCN").alias("REPORT_DCN"),
    col("PAYMENT_START_DATE").alias("PAYMENT_START_DATE"),
    col("PAYMENT_END_DATE").alias("PAYMENT_END_DATE"),
    col("FILE_TYPE").alias("FILE_TYPE"),
    col("LOAD_FILE_NAME").alias("LOAD_FILE_NAME"),
    col("CREATED_BY").alias("CREATED_BY"),
    col("CREATED_ON").alias("CREATED_ON"),
    col("ACTIVE_IND").alias("ACTIVE_IND"),
    col("TOTAL_POLICY_AMOUNT").alias("TOTAL_POLICY_AMOUNT")
)

# -- Stage: Xfm_Business_Logic1 (CTransformerStage)
df_SubscriberId_Null = df_Lnk_Rmv_AltKeys.select(
    col("HIX_820_CMS_ID").alias("HIX_820_CMS_ID"),
    col("RUN_DATE").alias("RUN_DATE"),
    col("TRADING_PARTNER_ID").alias("TRADING_PARTNER_ID"),
    concat(
        substring(col("EFT_EFFECTIVE_DATE"), 7, 4),
        lit("-"),
        substring(col("EFT_EFFECTIVE_DATE"), 1, 2),
        lit("-"),
        substring(col("EFT_EFFECTIVE_DATE"), 4, 2)
    ).alias("EFT_EFFECTIVE_DATE"),
    col("EFT_TRACE_NUMBER").alias("EFT_TRACE_NUMBER"),
    col("EXCHANGE_TOTAL_EFT_PAYMENT_AMOUNT").alias("EXCHANGE_TOTAL_EFT_PAYMENT_AMOUNT"),
    col("SUBSCRIBER_FIRST_NAME").alias("SUBSCRIBER_FIRST_NAME"),
    col("SUBSCRIBER_LAST_NAME").alias("SUBSCRIBER_LAST_NAME"),
    substring(
        concat(lit("0000000000"), trim(col("EXCHANGE_ASSIGNED_SUBSCRIBER_ID"))),
        length(concat(lit("0000000000"), trim(col("EXCHANGE_ASSIGNED_SUBSCRIBER_ID")))) - 10 + 1,
        10
    ).alias("EXCHANGE_ASSIGNED_SUBSCRIBER_ID"),
    trim(col("EXCHANGE_ASSIGNED_QHP_ID")).alias("EXCHANGE_ASSIGNED_QHP_ID"),
    col("EXCHANGE_ASSIGNED_POLICY_ID").alias("EXCHANGE_ASSIGNED_POLICY_ID"),
    substring(col("EXCHANGE_ASSIGNED_QHP_ID"), 6, 2).alias("SBAD_STATE"),
    col("PAYMENT_TYPE").alias("PAYMENT_TYPE"),
    col("PAYMENT_AMOUNT").alias("PAYMENT_AMOUNT"),
    col("REPORT_TYPE").alias("REPORT_TYPE"),
    col("REPORT_DCN").alias("REPORT_DCN"),
    when(col("PAYMENT_START_DATE").isNull(), lit("1753-01-01 00:00:00"))
    .otherwise(
        to_timestamp(
            concat(
                substring(col("PAYMENT_START_DATE"), 7, 4),
                lit("-"),
                substring(col("PAYMENT_START_DATE"), 1, 2),
                lit("-"),
                substring(col("PAYMENT_START_DATE"), 4, 2),
                lit(" 00:00:00")
            ),
            "yyyy-MM-dd HH:mm:ss"
        )
    ).alias("PAYMENT_START_DATE"),
    to_timestamp(
        concat(
            substring(col("PAYMENT_END_DATE"), 7, 4),
            lit("-"),
            substring(col("PAYMENT_END_DATE"), 1, 2),
            lit("-"),
            substring(col("PAYMENT_END_DATE"), 4, 2),
            lit(" 00:00:00")
        ),
        "yyyy-MM-dd HH:mm:ss"
    ).alias("PAYMENT_END_DATE"),
    col("FILE_TYPE").alias("FILE_TYPE"),
    col("LOAD_FILE_NAME").alias("LOAD_FILE_NAME"),
    col("CREATED_BY").alias("CREATED_BY"),
    col("CREATED_ON").alias("CREATED_ON"),
    col("ACTIVE_IND").alias("ACTIVE_IND"),
    col("TOTAL_POLICY_AMOUNT").alias("TOTAL_POLICY_AMOUNT")
)

# -- Stage: lkppoldtl (PxLookup) with cross-join (inner, no conditions)
df_matchextr = df_SubscriberId_Null.alias("SubscriberId_Null").join(
    df_CMS_ENR_PAYMT_DTL.alias("cmsdtlextr"),
    lit(True),
    "inner"
).select(
    col("cmsdtlextr.CMS_ENR_PAYMT_DTL_CK").alias("CMS_ENR_PAYMT_DTL_CK"),
    col("cmsdtlextr.EXCH_MBR_ID").alias("EXCH_MBR_ID"),
    col("cmsdtlextr.QHP_ID").alias("QHP_ID"),
    col("cmsdtlextr.PAYMT_SUBMT_DT").alias("PAYMT_SUBMT_DT"),
    col("cmsdtlextr.PAYMT_COV_YRMO").alias("PAYMT_COV_YRMO"),
    col("cmsdtlextr.ST_CD").alias("ST_CD"),
    col("cmsdtlextr.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("cmsdtlextr.TOT_PRM_AMT").alias("TOT_PRM_AMT"),
    col("cmsdtlextr.COV_STRT_DT").alias("COV_STRT_DT"),
    col("cmsdtlextr.COV_END_DT").alias("COV_END_DT"),
    col("cmsdtlextr.ENR_TRANS_TYP_CD").alias("ENR_TRANS_TYP_CD"),
    col("cmsdtlextr.FCTS_SUB_ID").alias("FCTS_SUB_ID"),
    col("cmsdtlextr.FCTS_PROD_ID").alias("FCTS_PROD_ID"),
    col("cmsdtlextr.MBR_RELSHP_TYP_CD").alias("MBR_RELSHP_TYP_CD"),
    col("cmsdtlextr.MBR_PRM_AMT").alias("MBR_PRM_AMT"),
    col("cmsdtlextr.PAYMT_RCVD_DT").alias("PAYMT_RCVD_DT"),
    col("cmsdtlextr.AS_OF_DT").alias("AS_OF_DT"),
    col("cmsdtlextr.BILL_ENTY_UNIQ_KEY").alias("BILL_ENTY_UNIQ_KEY"),
    col("cmsdtlextr.CLS_PLN_ID").alias("CLS_PLN_ID"),
    col("cmsdtlextr.GRP_LVL_BILL_ENTY_UNIQ_KEY").alias("GRP_LVL_BILL_ENTY_UNIQ_KEY"),
    col("SubscriberId_Null.EFT_TRACE_NUMBER").alias("EFT_TRACE_ID"),
    col("cmsdtlextr.ENR_PAYMT_UNIQ_KEY").alias("ENR_PAYMT_UNIQ_KEY"),
    col("cmsdtlextr.SUB_FIRST_NM").alias("SUB_FIRST_NM"),
    col("cmsdtlextr.SUB_LAST_NM").alias("SUB_LAST_NM"),
    col("cmsdtlextr.EXCH_ASG_POL_ID").alias("EXCH_ASG_POL_ID"),
    col("cmsdtlextr.EXCH_ASG_SUB_ID").alias("EXCH_ASG_SUB_ID"),
    col("cmsdtlextr.EXCH_RPT_DOC_CTL_NO").alias("EXCH_RPT_DOC_CTL_NO"),
    col("cmsdtlextr.EXCH_RPT_NM").alias("EXCH_RPT_NM"),
    col("cmsdtlextr.CRT_DT").alias("CRT_DT"),
    col("cmsdtlextr.LAST_UPDT_DT").alias("LAST_UPDT_DT"),
    col("SubscriberId_Null.EFT_EFFECTIVE_DATE").alias("EFT_EFFECTIVE_DATE")
)

# -- Stage: transf (CTransformerStage)
df_dtlupdate = df_matchextr.select(
    col("CMS_ENR_PAYMT_DTL_CK").alias("CMS_ENR_PAYMT_DTL_CK"),
    col("EXCH_MBR_ID").alias("EXCH_MBR_ID"),
    col("QHP_ID").alias("QHP_ID"),
    col("PAYMT_SUBMT_DT").alias("PAYMT_SUBMT_DT"),
    col("PAYMT_COV_YRMO").alias("PAYMT_COV_YRMO"),
    col("ST_CD").alias("ST_CD"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("TOT_PRM_AMT").alias("TOT_PRM_AMT"),
    col("COV_STRT_DT").alias("COV_STRT_DT"),
    col("COV_END_DT").alias("COV_END_DT"),
    col("ENR_TRANS_TYP_CD").alias("ENR_TRANS_TYP_CD"),
    col("FCTS_SUB_ID").alias("FCTS_SUB_ID"),
    col("FCTS_PROD_ID").alias("FCTS_PROD_ID"),
    col("MBR_RELSHP_TYP_CD").alias("MBR_RELSHP_TYP_CD"),
    col("MBR_PRM_AMT").alias("MBR_PRM_AMT"),
    to_date(col("EFT_EFFECTIVE_DATE"), "yyyy-MM-dd").alias("PAYMT_RCVD_DT"),
    col("AS_OF_DT").alias("AS_OF_DT"),
    col("BILL_ENTY_UNIQ_KEY").alias("BILL_ENTY_UNIQ_KEY"),
    col("CLS_PLN_ID").alias("CLS_PLN_ID"),
    col("GRP_LVL_BILL_ENTY_UNIQ_KEY").alias("GRP_LVL_BILL_ENTY_UNIQ_KEY"),
    when(
        length(trim(when(col("EFT_TRACE_ID").isNotNull(), col("EFT_TRACE_ID")).otherwise(lit("")))) == 0,
        "9999999"
    ).otherwise(col("EFT_TRACE_ID")).alias("EFT_TRACE_ID"),
    col("ENR_PAYMT_UNIQ_KEY").alias("ENR_PAYMT_UNIQ_KEY"),
    col("SUB_FIRST_NM").alias("SUB_FIRST_NM"),
    col("SUB_LAST_NM").alias("SUB_LAST_NM"),
    col("EXCH_ASG_POL_ID").alias("EXCH_ASG_POL_ID"),
    col("EXCH_ASG_SUB_ID").alias("EXCH_ASG_SUB_ID"),
    col("EXCH_RPT_DOC_CTL_NO").alias("EXCH_RPT_DOC_CTL_NO"),
    col("EXCH_RPT_NM").alias("EXCH_RPT_NM"),
    col("CRT_DT").alias("CRT_DT"),
    lit(RunTS).alias("LAST_UPDT_DT")
)

# -- Stage: CMS_ENR_PAYMT_DTL_UPDT (ODBCConnectorPX) with Merge (Upsert)
table_target = "#$BCBSFINOwner#.CMS_ENR_PAYMT_DTL"
temp_table = "STAGING.I820CMSEnrPaymtDtlUpdtPolLoad_CMS_ENR_PAYMT_DTL_UPDT_temp"

execute_dml(f"DROP TABLE IF EXISTS {temp_table}", jdbc_url, jdbc_props)

(
    df_dtlupdate.write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", temp_table)
    .mode("overwrite")
    .save()
)

merge_sql = f"""
MERGE INTO {table_target} AS target
USING {temp_table} AS source
ON (target.CMS_ENR_PAYMT_DTL_CK = source.CMS_ENR_PAYMT_DTL_CK)
WHEN MATCHED THEN
  UPDATE SET
    target.EXCH_MBR_ID = source.EXCH_MBR_ID,
    target.QHP_ID = source.QHP_ID,
    target.PAYMT_SUBMT_DT = source.PAYMT_SUBMT_DT,
    target.PAYMT_COV_YRMO = source.PAYMT_COV_YRMO,
    target.ST_CD = source.ST_CD,
    target.SRC_SYS_CD = source.SRC_SYS_CD,
    target.TOT_PRM_AMT = source.TOT_PRM_AMT,
    target.COV_STRT_DT = source.COV_STRT_DT,
    target.COV_END_DT = source.COV_END_DT,
    target.ENR_TRANS_TYP_CD = source.ENR_TRANS_TYP_CD,
    target.FCTS_SUB_ID = source.FCTS_SUB_ID,
    target.FCTS_PROD_ID = source.FCTS_PROD_ID,
    target.MBR_RELSHP_TYP_CD = source.MBR_RELSHP_TYP_CD,
    target.MBR_PRM_AMT = source.MBR_PRM_AMT,
    target.PAYMT_RCVD_DT = source.PAYMT_RCVD_DT,
    target.AS_OF_DT = source.AS_OF_DT,
    target.BILL_ENTY_UNIQ_KEY = source.BILL_ENTY_UNIQ_KEY,
    target.CLS_PLN_ID = source.CLS_PLN_ID,
    target.GRP_LVL_BILL_ENTY_UNIQ_KEY = source.GRP_LVL_BILL_ENTY_UNIQ_KEY,
    target.EFT_TRACE_ID = source.EFT_TRACE_ID,
    target.ENR_PAYMT_UNIQ_KEY = source.ENR_PAYMT_UNIQ_KEY,
    target.SUB_FIRST_NM = source.SUB_FIRST_NM,
    target.SUB_LAST_NM = source.SUB_LAST_NM,
    target.EXCH_ASG_POL_ID = source.EXCH_ASG_POL_ID,
    target.EXCH_ASG_SUB_ID = source.EXCH_ASG_SUB_ID,
    target.EXCH_RPT_DOC_CTL_NO = source.EXCH_RPT_DOC_CTL_NO,
    target.EXCH_RPT_NM = source.EXCH_RPT_NM,
    target.CRT_DT = source.CRT_DT,
    target.LAST_UPDT_DT = source.LAST_UPDT_DT
WHEN NOT MATCHED THEN
  INSERT (
    CMS_ENR_PAYMT_DTL_CK,
    EXCH_MBR_ID,
    QHP_ID,
    PAYMT_SUBMT_DT,
    PAYMT_COV_YRMO,
    ST_CD,
    SRC_SYS_CD,
    TOT_PRM_AMT,
    COV_STRT_DT,
    COV_END_DT,
    ENR_TRANS_TYP_CD,
    FCTS_SUB_ID,
    FCTS_PROD_ID,
    MBR_RELSHP_TYP_CD,
    MBR_PRM_AMT,
    PAYMT_RCVD_DT,
    AS_OF_DT,
    BILL_ENTY_UNIQ_KEY,
    CLS_PLN_ID,
    GRP_LVL_BILL_ENTY_UNIQ_KEY,
    EFT_TRACE_ID,
    ENR_PAYMT_UNIQ_KEY,
    SUB_FIRST_NM,
    SUB_LAST_NM,
    EXCH_ASG_POL_ID,
    EXCH_ASG_SUB_ID,
    EXCH_RPT_DOC_CTL_NO,
    EXCH_RPT_NM,
    CRT_DT,
    LAST_UPDT_DT
  )
  VALUES (
    source.CMS_ENR_PAYMT_DTL_CK,
    source.EXCH_MBR_ID,
    source.QHP_ID,
    source.PAYMT_SUBMT_DT,
    source.PAYMT_COV_YRMO,
    source.ST_CD,
    source.SRC_SYS_CD,
    source.TOT_PRM_AMT,
    source.COV_STRT_DT,
    source.COV_END_DT,
    source.ENR_TRANS_TYP_CD,
    source.FCTS_SUB_ID,
    source.FCTS_PROD_ID,
    source.MBR_RELSHP_TYP_CD,
    source.MBR_PRM_AMT,
    source.PAYMT_RCVD_DT,
    source.AS_OF_DT,
    source.BILL_ENTY_UNIQ_KEY,
    source.CLS_PLN_ID,
    source.GRP_LVL_BILL_ENTY_UNIQ_KEY,
    source.EFT_TRACE_ID,
    source.ENR_PAYMT_UNIQ_KEY,
    source.SUB_FIRST_NM,
    source.SUB_LAST_NM,
    source.EXCH_ASG_POL_ID,
    source.EXCH_ASG_SUB_ID,
    source.EXCH_RPT_DOC_CTL_NO,
    source.EXCH_RPT_NM,
    source.CRT_DT,
    source.LAST_UPDT_DT
  );
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)