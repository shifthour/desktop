# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Brent Leland                   04/05/2005                                                 Changed SQL lookups to hash files for FNCL_LOB and CD_MPPNG.
# MAGIC 
# MAGIC 
# MAGIC Sudheer Champati         09/11/2017        5599                            Added the Sybase (GL_Cap_BCP_table) to pull the  IntegrateDev2             Jag Yelavarthi            2017-09-12
# MAGIC                                                                                                       workday columns.
# MAGIC 
# MAGIC Tim Sieg                        10/11/2017        5599                            Correcting IDS parameter names, missing $               IntergrateDev2              Jag Yelavarthi          2017-11-30

# MAGIC Extract FNCL_LOB_CD for journal lines
# MAGIC Extract correct DR CR code for journal lines
# MAGIC Extract required JE fields from DTL table
# MAGIC Extract journal line detail from IDS
# MAGIC Create required header file for loading to peoplesoft GL
# MAGIC Create required detail file for loading to peoplesoft GL
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Retrieve job parameters
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
bcbsfindb_secret_name = get_widget_value('bcbsfindb_secret_name','')
Acctg_Dt = get_widget_value('Acctg_Dt','')
Trans_LOB = get_widget_value('Trans_LOB','')
TmpOutFile = get_widget_value('TmpOutFile','')
StrtDt = get_widget_value('StrtDt','')
EndDt = get_widget_value('EndDt','')

# JRNL_ENTRY_TRANS (DB2Connector to IDS)
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_jrnl = (
    f"SELECT "
    f"ACCTG_DT_SK as ACCTG_DT_SK,"
    f"JRNL_ENTRY_TRANS_AMT as JRNL_ENTRY_TRANS_AMT,"
    f"TRANS_LN_NO as TRANS_LN_NO,"
    f"ACCT_NO as ACCT_NO,"
    f"AFFILIATE_NO as AFFILIATE_NO,"
    f"BUS_UNIT_GL_NO as BUS_UNIT_GL_NO,"
    f"BUS_UNIT_NO as BUS_UNIT_NO,"
    f"CC_ID as CC_ID,"
    f"JRNL_LN_DESC as JRNL_LN_DESC,"
    f"OPR_UNIT_NO as OPR_UNIT_NO,"
    f"SUB_ACCT_NO as SUB_ACCT_NO,"
    f"FNCL_LOB_SK as FNCL_LOB_SK,"
    f"JRNL_ENTRY_TRANS_DR_CR_CD_SK as JRNL_ENTRY_TRANS_DR_CR_CD_SK,"
    f"DIST_GL_IN as DIST_GL_IN,"
    f"APP_JRNL_ID as APP_JRNL_ID,"
    f"JRNL_LN_REF_NO as JRNL_LN_REF_NO,"
    f"SRC_TRANS_CK "
    f"FROM {IDSOwner}.JRNL_ENTRY_TRANS "
    f"WHERE DIST_GL_IN = 'Y' "
    f"AND ACCTG_DT_SK = '{Acctg_Dt}' "
    f"AND TRANS_LN_NO = {Trans_LOB}"
)
df_JRNL_ENTRY_TRANS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_jrnl)
    .load()
)

# hf_etrnl_cd_mppng (CHashedFileStage) - Scenario C: read from parquet
df_hf_etrnl_cd_mppng = spark.read.parquet(f"{adls_path}/hf_etrnl_cd_mppng.parquet")

# FNCL_LOB (DB2Connector to IDS)
extract_query_fncl = (
    f"SELECT "
    f"FNCL_LOB_SK, "
    f"FNCL_LOB_CD as FNCL_LOB_CD "
    f"FROM {IDSOwner}.FNCL_LOB"
)
df_FNCL_LOB = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_fncl)
    .load()
)

# hf_ps_mvp_fncl_lob (CHashedFileStage) - Scenario A: deduplicate based on key FNCL_LOB_SK
df_hf_ps_mvp_fncl_lob = df_FNCL_LOB.dropDuplicates(["FNCL_LOB_SK"])

# GL_INTER_PLAN_BILL_DTL (CODBCStage) reading from BCBSFINDB
jdbc_url_bcbs, jdbc_props_bcbs = get_db_config(bcbsfindb_secret_name)
extract_query_bcbs = (
    f"SELECT "
    f"GL_BCP.GL_INTER_PLAN_BILL_DTL_CK as GL_DTL_CK, "
    f"GL_BCP.ACCOUNT, "
    f"GL_BCP.OPERATING_UNIT, "
    f"GL_BCP.AFFILIATE, "
    f"GL_BCP.DEPTID, "
    f"GL_BCP.CHARTFIELD1, "
    f"GL_BCP.JRNL_LN_REF "
    f"FROM {bcbsfindb_secret_name}..GL_INTER_PLAN_BILL_DTL GL_BCP "
    f"WHERE "
    f"GL_BCP.MAP_EFF_DT = '{StrtDt}' "
    f"AND GL_BCP.SNAP_ACT_DT = '{EndDt}'"
)
df_GL_INTER_PLAN_BILL_DTL = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_bcbs)
    .options(**jdbc_props_bcbs)
    .option("query", extract_query_bcbs)
    .load()
)

# hf_bcpData (CHashedFileStage) - Scenario A: deduplicate based on key GL_DTL_CK
df_hf_bcpData = df_GL_INTER_PLAN_BILL_DTL.dropDuplicates(["GL_DTL_CK"])

# TrnsJrnlMapFields (CTransformerStage) - multiple left joins
df_TrnsJrnlMapFields = (
    df_JRNL_ENTRY_TRANS.alias("lnkJrnlEntryIn")
    .join(
        df_hf_ps_mvp_fncl_lob.alias("lnkFNCL_LOBIn"),
        F.col("lnkJrnlEntryIn.FNCL_LOB_SK") == F.col("lnkFNCL_LOBIn.FNCL_LOB_SK"),
        "left",
    )
    .join(
        df_hf_etrnl_cd_mppng.alias("lnkCD_MAPPINGIn"),
        F.col("lnkJrnlEntryIn.JRNL_ENTRY_TRANS_DR_CR_CD_SK") == F.col("lnkCD_MAPPINGIn.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_hf_bcpData.alias("bcpData"),
        F.col("lnkJrnlEntryIn.SRC_TRANS_CK") == F.col("bcpData.GL_DTL_CK"),
        "left",
    )
)

df_lnkJrnlDetail = df_TrnsJrnlMapFields.select(
    F.col("lnkJrnlEntryIn.BUS_UNIT_NO").alias("BUSINESS_UNIT"),
    F.col("lnkJrnlEntryIn.TRANS_LN_NO").alias("TRANSACTION_LINE"),
    F.date_format(
        F.to_date(F.col("lnkJrnlEntryIn.ACCTG_DT_SK").cast(StringType()), "yyyyMMdd"),
        "MM/dd/yyyy",
    ).alias("ACCOUNTING_DT"),
    F.col("lnkJrnlEntryIn.BUS_UNIT_GL_NO").alias("BUSINESS_UNIT_GL"),
    F.col("bcpData.ACCOUNT").alias("ACCOUNT"),
    F.col("bcpData.DEPTID").alias("DEPTID"),
    F.col("bcpData.OPERATING_UNIT").alias("OPERATION_UNIT"),
    F.col("lnkFNCL_LOBIn.FNCL_LOB_CD").alias("PRODUCT"),
    F.col("bcpData.AFFILIATE").alias("AFFILIATE"),
    F.col("bcpData.CHARTFIELD1").alias("CHARTFIELD1"),
    F.when(F.col("lnkCD_MAPPINGIn.TRGT_CD") == F.lit("CR"), -F.col("lnkJrnlEntryIn.JRNL_ENTRY_TRANS_AMT"))
     .otherwise(F.col("lnkJrnlEntryIn.JRNL_ENTRY_TRANS_AMT"))
     .alias("MONETARY_AMOUNT"),
    F.col("lnkJrnlEntryIn.JRNL_LN_DESC").alias("LINE_DESC"),
    F.col("lnkJrnlEntryIn.APP_JRNL_ID").alias("APP_JRNL_ID"),
    F.col("bcpData.JRNL_LN_REF").alias("JRNL_LN_REF_NO"),
)

# AggJrnlSum (AGGREGATOR) - group by and sum
df_AggJrnlSum = (
    df_lnkJrnlDetail.groupBy(
        "BUSINESS_UNIT",
        "TRANSACTION_LINE",
        "ACCOUNTING_DT",
        "BUSINESS_UNIT_GL",
        "ACCOUNT",
        "DEPTID",
        "OPERATION_UNIT",
        "PRODUCT",
        "AFFILIATE",
        "CHARTFIELD1",
        "APP_JRNL_ID",
        "JRNL_LN_REF_NO"
    )
    .agg(
        F.sum("MONETARY_AMOUNT").alias("MONETARY_AMOUNT"),
        F.first("LINE_DESC").alias("LINE_DESC")
    )
)

# TrnsGetHdrFields (CTransformerStage) - two output links
# 1) lnkJrnlSumFieldsIntoCount
df_lnkJrnlSumFieldsIntoCount = df_AggJrnlSum.select(
    F.col("TRANSACTION_LINE").alias("TRANSACTION_LINE"),
    F.when(F.col("MONETARY_AMOUNT") > 0.0, F.col("MONETARY_AMOUNT")).otherwise(F.lit(0.0)).alias("CREDIT_AMOUNT"),
    F.when(F.col("MONETARY_AMOUNT") < 0.0, F.col("MONETARY_AMOUNT")).otherwise(F.lit(0.0)).alias("DEBIT_AMT")
)

# 2) lnkJrnlSumFields
df_lnkJrnlSumFields = df_AggJrnlSum.select(
    F.rpad(F.col("BUSINESS_UNIT"), 5, " ").alias("BUSINESS_UNIT"),
    F.col("TRANSACTION_LINE").alias("TRANSACTION_LINE"),
    F.rpad(F.col("ACCOUNTING_DT"), 10, " ").alias("ACCOUNTING_DT"),
    F.rpad(F.col("BUSINESS_UNIT_GL"), 5, " ").alias("BUSINESS_UNIT_GL"),
    trim(F.col("ACCOUNT")).alias("ACCOUNT"),
    trim(F.col("DEPTID")).alias("DEPTID"),
    trim(F.col("OPERATION_UNIT")).alias("OPERATION_UNIT"),
    trim(F.col("PRODUCT")).alias("PRODUCT"),
    trim(F.col("AFFILIATE")).alias("AFFILIATE"),
    trim(F.col("CHARTFIELD1")).alias("CHARTFIELD1"),
    F.col("MONETARY_AMOUNT").alias("MONETARY_AMOUNT"),
    F.rpad(F.col("LINE_DESC"), 30, " ").alias("LINE_DESC"),
    F.rpad(F.col("APP_JRNL_ID"), 10, " ").alias("APP_JRNL_ID"),
    F.rpad(F.col("JRNL_LN_REF_NO"), 10, " ").alias("JRNL_LN_REF_NO"),
)

# trnsJrnlSplit (CTransformerStage)
df_lnkJrnlOut = df_lnkJrnlSumFields.select(
    F.rpad(
        F.when(
            (F.col("BUSINESS_UNIT").isin("0", "NA")), F.lit(" ")
        ).otherwise(F.col("BUSINESS_UNIT")),
        5, " "
    ).alias("BUSINESS_UNIT"),
    F.col("TRANSACTION_LINE").alias("TRANSACTION_LINE"),
    F.rpad(F.col("ACCOUNTING_DT"), 10, " ").alias("ACCOUNTING_DT"),
    F.rpad(
        F.when(
            (F.col("BUSINESS_UNIT_GL").isin("0", "NA")), F.lit(" ")
        ).otherwise(F.col("BUSINESS_UNIT_GL")),
        5, " "
    ).alias("BUSINESS_UNIT_GL"),
    F.rpad(
        F.when(
            (F.col("ACCOUNT").isin("0", "NA")), F.lit(" ")
        ).otherwise(F.col("ACCOUNT")),
        10, " "
    ).alias("ACCOUNT"),
    F.rpad(
        F.when(
            (F.col("DEPTID").isin("0", "NA")), F.lit(" ")
        ).otherwise(F.col("DEPTID")),
        10, " "
    ).alias("DEPTID"),
    F.rpad(
        F.when(
            (F.col("OPERATION_UNIT").isin("0", "NA")), F.lit(" ")
        ).otherwise(F.col("OPERATION_UNIT")),
        8, " "
    ).alias("OPERATION_UNIT"),
    F.rpad(
        F.when(
            (F.col("PRODUCT").isin("0", "NA")), F.lit(" ")
        ).otherwise(F.col("PRODUCT")),
        6, " "
    ).alias("PRODUCT"),
    F.rpad(
        F.when(
            (F.col("AFFILIATE").isin("0", "NA")), F.lit(" ")
        ).otherwise(F.col("AFFILIATE")),
        5, " "
    ).alias("AFFILIATE"),
    F.rpad(
        F.when(
            (F.col("CHARTFIELD1").isin("0", "NA")), F.lit(" ")
        ).otherwise(F.col("CHARTFIELD1")),
        10, " "
    ).alias("CHARTFIELD1"),
    F.col("MONETARY_AMOUNT").alias("MONETARY_AMOUNT"),
    F.rpad(F.col("LINE_DESC"), 30, " ").alias("LINE_DESC"),
    F.rpad(F.col("APP_JRNL_ID"), 10, " ").alias("APP_JRNL_ID"),
    F.rpad(F.col("JRNL_LN_REF_NO"), 10, " ").alias("JRNL_LN_REF_NO"),
)

# Dtl_PeopleSoftExtr (CSeqFileStage) - write to external/DTL_#TmpOutFile#
df_final_jrnl = df_lnkJrnlOut.select(
    F.rpad(F.col("BUSINESS_UNIT"), 5, " ").alias("BUSINESS_UNIT"),
    "TRANSACTION_LINE",
    F.rpad(F.col("ACCOUNTING_DT"), 10, " ").alias("ACCOUNTING_DT"),
    F.rpad(F.col("BUSINESS_UNIT_GL"), 5, " ").alias("BUSINESS_UNIT_GL"),
    F.rpad(F.col("ACCOUNT"), 10, " ").alias("ACCOUNT"),
    F.rpad(F.col("DEPTID"), 10, " ").alias("DEPTID"),
    F.rpad(F.col("OPERATION_UNIT"), 8, " ").alias("OPERATION_UNIT"),
    F.rpad(F.col("PRODUCT"), 6, " ").alias("PRODUCT"),
    F.rpad(F.col("AFFILIATE"), 5, " ").alias("AFFILIATE"),
    F.rpad(F.col("CHARTFIELD1"), 10, " ").alias("CHARTFIELD1"),
    "MONETARY_AMOUNT",
    F.rpad(F.col("LINE_DESC"), 30, " ").alias("LINE_DESC"),
    F.rpad(F.col("APP_JRNL_ID"), 10, " ").alias("APP_JRNL_ID"),
    F.rpad(F.col("JRNL_LN_REF_NO"), 10, " ").alias("JRNL_LN_REF_NO"),
)
write_files(
    df_final_jrnl,
    f"{adls_path_publish}/external/DTL_{TmpOutFile}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# AggHeaderCount (AGGREGATOR)
df_AggHeaderCount = (
    df_lnkJrnlSumFieldsIntoCount.groupBy("TRANSACTION_LINE")
    .agg(
        F.sum("CREDIT_AMOUNT").alias("CREDIT_SUM"),
        F.sum("DEBIT_AMT").alias("DEBIT_SUM"),
        F.count("TRANSACTION_LINE").alias("ROW_COUNT")
    )
)

# trnsHeaderCounts (CTransformerStage) - define LOBType from Trans_LOB
if Trans_LOB.isdigit():
    numeric_trans_lob = int(Trans_LOB)
else:
    numeric_trans_lob = -999999

if numeric_trans_lob == 231:
    LOBType = "CAP"
elif numeric_trans_lob == 240:
    LOBType = "DRG"
elif numeric_trans_lob == 230:
    LOBType = "CLM"
elif numeric_trans_lob == 210:
    LOBType = "INC"
elif numeric_trans_lob == 270:
    LOBType = "COM"
else:
    LOBType = "UNK"

df_trnsHeaderCounts = df_AggHeaderCount.select(
    F.concat(F.lit(LOBType), F.lit("\n"), F.lit("QQQ HEADER"), F.lit("\n")).alias("HEADER"),
    F.col("CREDIT_SUM").alias("CREDIT"),
    F.col("DEBIT_SUM").alias("DEBIT"),
    F.col("ROW_COUNT").alias("ROWS"),
    F.concat(F.lit("\n"), F.lit(LOBType), F.lit("\n"), F.lit("QQQ DETAIL")).alias("TRAILER")
)

# Hdr_PeopleSoftExtr (CSeqFileStage) - write to external/HDR_#TmpOutFile#
df_final_header = df_trnsHeaderCounts.select(
    F.rpad(F.col("HEADER"), 15, " ").alias("HEADER"),
    "CREDIT",
    "DEBIT",
    "ROWS",
    F.rpad(F.col("TRAILER"), 15, " ").alias("TRAILER"),
)
write_files(
    df_final_header,
    f"{adls_path_publish}/external/HDR_{TmpOutFile}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)