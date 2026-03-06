# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Brent Leland                   04/05/2005                                                 Changed SQL lookups to hash files for FNCL_LOB and CD_MPPNG.
# MAGIC 
# MAGIC 
# MAGIC Sudheer Champati         09/11/2017        5599                            Added the Sybase (GL_Cap_BCP_table) to pull the  IntegrateDev2               Jag Yelavarthi          2017-09-12
# MAGIC                                                                                                       workday columns.
# MAGIC 
# MAGIC Tim Sieg                        10/11/2017        5599                             Correcting IDS parameter names, missing $             IntegrateDev2                Jag Yelavarthi           2017-11-30
# MAGIC Prabhu ES                     2022-03-14         S2S                              MSSQL ODBC conn params added                                            IntegrateDev5

# MAGIC Extract required JE fields from DTL table
# MAGIC Extract FNCL_LOB_CD for journal lines
# MAGIC Extract correct DR CR code for journal lines
# MAGIC Create required header file for loading to peoplesoft GL
# MAGIC Extract journal line detail from IDS
# MAGIC Create required detail file for loading to peoplesoft GL
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('$IDSOwner','')
BCBSFINOwner = get_widget_value('$BCBSFINOwner','')
Acctg_Dt = get_widget_value('Acctg_Dt','')
Trans_LOB = get_widget_value('Trans_LOB','')
TmpOutFile = get_widget_value('TmpOutFile','')
CurrYrMo = get_widget_value('CurrYrMo','')
ids_secret_name = get_widget_value('ids_secret_name','')
bcbsfin_secret_name = get_widget_value('bcbsfin_secret_name','')

# DB2Connector Stage: JRNL_ENTRY_TRANS (lnkJrnlEntryIn)
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"""
SELECT 
  ACCTG_DT_SK as ACCTG_DT_SK,
  JRNL_ENTRY_TRANS_AMT as JRNL_ENTRY_TRANS_AMT,
  TRANS_LN_NO as TRANS_LN_NO,
  ACCT_NO as ACCT_NO,
  AFFILIATE_NO as AFFILIATE_NO,
  BUS_UNIT_GL_NO as BUS_UNIT_GL_NO,
  BUS_UNIT_NO as BUS_UNIT_NO,
  CC_ID as CC_ID,
  JRNL_LN_DESC as JRNL_LN_DESC,
  OPR_UNIT_NO as OPR_UNIT_NO,
  SUB_ACCT_NO as SUB_ACCT_NO,
  FNCL_LOB_SK as FNCL_LOB_SK,
  JRNL_ENTRY_TRANS_DR_CR_CD_SK as JRNL_ENTRY_TRANS_DR_CR_CD_SK,
  DIST_GL_IN as DIST_GL_IN,
  APP_JRNL_ID as APP_JRNL_ID,
  JRNL_LN_REF_NO as JRNL_LN_REF_NO,
  SRC_TRANS_CK
FROM {IDSOwner}.JRNL_ENTRY_TRANS
WHERE DIST_GL_IN = 'Y'
  AND ACCTG_DT_SK = '{Acctg_Dt}'
  AND TRANS_LN_NO = {Trans_LOB}
"""
df_JRNL_ENTRY_TRANS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# CHashedFileStage: hf_etrnl_cd_mppng (scenario C - reading from parquet)
df_hf_etrnl_cd_mppng = spark.read.parquet(f"{adls_path}/hf_etrnl_cd_mppng.parquet")

# DB2Connector Stage: FNCL_LOB
extract_query = f"""
SELECT 
  FNCL_LOB_SK,
  FNCL_LOB_CD as FNCL_LOB_CD
FROM {IDSOwner}.FNCL_LOB
"""
df_FNCL_LOB = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# CHashedFileStage: hf_ps_820_fncl_lob (scenario A – deduplicate from fncl_lob)
df_hf_ps_820_fncl_lob = df_FNCL_LOB.dropDuplicates(["FNCL_LOB_SK"])

# ODBCConnector Stage: GL_ON_EXCH_FED_PYMT_DTL
jdbc_url, jdbc_props = get_db_config(bcbsfin_secret_name)
extract_query = f"""
SELECT 
 GL_BCP.GL_ON_EXCH_FED_PYMT_DTL_CK as GL_DTL_CK, 
 GL_BCP.ACCOUNT, 
 GL_BCP.OPERATING_UNIT, 
 GL_BCP.AFFILIATE, 
 GL_BCP.DEPTID, 
 GL_BCP.CHARTFIELD1,
 GL_BCP.JRNL_LN_REF
FROM {BCBSFINOwner}.GL_ON_EXCH_FED_PYMT_DTL GL_BCP
WHERE SUBSTRING(CONVERT(Char(8),(GL_BCP.PAYMT_SUBMT_DT),112),1,6) = '{CurrYrMo}'
"""
df_GL_ON_EXCH_FED_PYMT_DTL = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# CHashedFileStage: hf_bcpData (scenario A – deduplicate from GL_ON_EXCH_FED_PYMT_DTL)
df_hf_bcpData = df_GL_ON_EXCH_FED_PYMT_DTL.dropDuplicates(["GL_DTL_CK"])

# CTransformerStage: TrnsJrnlMapFields
df_JrnlEntryTrans_Joined = (
    df_JRNL_ENTRY_TRANS.alias("lnkJrnlEntryIn")
    .join(
        df_hf_ps_820_fncl_lob.alias("lnkFNCL_LOBIn"),
        F.col("lnkJrnlEntryIn.FNCL_LOB_SK") == F.col("lnkFNCL_LOBIn.FNCL_LOB_SK"),
        "left"
    )
    .join(
        df_hf_etrnl_cd_mppng.alias("lnkCD_MAPPINGIn"),
        F.col("lnkJrnlEntryIn.JRNL_ENTRY_TRANS_DR_CR_CD_SK") == F.col("lnkCD_MAPPINGIn.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_hf_bcpData.alias("bcpData"),
        F.col("lnkJrnlEntryIn.SRC_TRANS_CK") == F.col("bcpData.GL_DTL_CK"),
        "left"
    )
)

# Output link "lnkJrnlDetail" columns
df_lnkJrnlDetail = df_JrnlEntryTrans_Joined.select(
    F.col("lnkJrnlEntryIn.BUS_UNIT_NO").alias("BUSINESS_UNIT"),
    F.col("lnkJrnlEntryIn.TRANS_LN_NO").alias("TRANSACTION_LINE"),
    Oconv(Iconv(F.col("lnkJrnlEntryIn.ACCTG_DT_SK"), "D-YMD[4,2,2]"), "D/MDY[2,2,4]").alias("ACCOUNTING_DT"),
    F.col("lnkJrnlEntryIn.BUS_UNIT_GL_NO").alias("BUSINESS_UNIT_GL"),
    F.col("bcpData.ACCOUNT").alias("ACCOUNT"),
    F.col("bcpData.DEPTID").alias("DEPTID"),
    F.col("bcpData.OPERATING_UNIT").alias("OPERATION_UNIT"),
    F.col("lnkFNCL_LOBIn.FNCL_LOB_CD").alias("PRODUCT"),
    F.col("bcpData.AFFILIATE").alias("AFFILIATE"),
    F.col("bcpData.CHARTFIELD1").alias("CHARTFIELD1"),
    F.when(F.col("lnkCD_MAPPINGIn.TRGT_CD") == F.lit("CR"),
           F.lit(0) - F.col("lnkJrnlEntryIn.JRNL_ENTRY_TRANS_AMT")
          ).otherwise(F.col("lnkJrnlEntryIn.JRNL_ENTRY_TRANS_AMT")).alias("MONETARY_AMOUNT"),
    F.col("lnkJrnlEntryIn.JRNL_LN_DESC").alias("LINE_DESC"),
    F.col("lnkJrnlEntryIn.APP_JRNL_ID").alias("APP_JRNL_ID"),
    F.col("bcpData.JRNL_LN_REF").alias("JRNL_LN_REF_NO")
)

# AGGREGATOR: AggJrnlSum
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
        "LINE_DESC",
        "APP_JRNL_ID",
        "JRNL_LN_REF_NO"
    ).agg(F.sum("MONETARY_AMOUNT").alias("MONETARY_AMOUNT"))
)

# CTransformerStage: TrnsGetHdrFields
#   Output link lnkJrnlSumFieldsIntoCount
df_lnkJrnlSumFieldsIntoCount = df_AggJrnlSum.select(
    F.col("TRANSACTION_LINE").alias("TRANSACTION_LINE"),
    F.when(F.col("MONETARY_AMOUNT") > 0.0, F.col("MONETARY_AMOUNT")).otherwise(F.lit(0.0)).alias("CREDIT_AMOUNT"),
    F.when(F.col("MONETARY_AMOUNT") < 0.0, F.col("MONETARY_AMOUNT")).otherwise(F.lit(0.0)).alias("DEBIT_AMT")
)

#   Output link lnkJrnlSumFields
df_lnkJrnlSumFields = df_AggJrnlSum.select(
    F.col("BUSINESS_UNIT"),
    F.col("TRANSACTION_LINE"),
    F.col("ACCOUNTING_DT"),
    F.col("BUSINESS_UNIT_GL"),
    F.expr("TRIM(ACCOUNT)").alias("ACCOUNT"),
    F.expr("TRIM(DEPTID)").alias("DEPTID"),
    F.expr("TRIM(OPERATION_UNIT)").alias("OPERATION_UNIT"),
    F.expr("TRIM(PRODUCT)").alias("PRODUCT"),
    F.expr("TRIM(AFFILIATE)").alias("AFFILIATE"),
    F.expr("TRIM(CHARTFIELD1)").alias("CHARTFIELD1"),
    F.col("MONETARY_AMOUNT"),
    F.col("LINE_DESC"),
    F.col("APP_JRNL_ID"),
    F.col("JRNL_LN_REF_NO")
)

# CTransformerStage: trnsJrnlSplit
#   Output link lnkJrnlOut
df_lnkJrnlOut = df_lnkJrnlSumFields.select(
    F.col("BUSINESS_UNIT"),
    F.col("TRANSACTION_LINE"),
    F.col("ACCOUNTING_DT"),
    F.col("BUSINESS_UNIT_GL"),
    F.when((F.col("ACCOUNT") == "0") | (F.col("ACCOUNT") == "NA"), F.lit(" ")).otherwise(F.col("ACCOUNT")).alias("ACCOUNT"),
    F.when((F.col("DEPTID") == "0") | (F.col("DEPTID") == "NA"), F.lit(" ")).otherwise(F.col("DEPTID")).alias("DEPTID"),
    F.when((F.col("OPERATION_UNIT") == "0") | (F.col("OPERATION_UNIT") == "NA"), F.lit(" ")).otherwise(F.col("OPERATION_UNIT")).alias("OPERATION_UNIT"),
    F.when((F.col("PRODUCT") == "0") | (F.col("PRODUCT") == "NA"), F.lit(" ")).otherwise(F.col("PRODUCT")).alias("PRODUCT"),
    F.when((F.col("AFFILIATE") == "0") | (F.col("AFFILIATE") == "NA"), F.lit(" ")).otherwise(F.col("AFFILIATE")).alias("AFFILIATE"),
    F.when((F.col("CHARTFIELD1") == "0") | (F.col("CHARTFIELD1") == "NA"), F.lit(" ")).otherwise(F.col("CHARTFIELD1")).alias("CHARTFIELD1"),
    F.col("MONETARY_AMOUNT"),
    F.col("LINE_DESC"),
    F.col("APP_JRNL_ID"),
    F.col("JRNL_LN_REF_NO")
)

# CSeqFileStage: Dtl_PeopleSoftExtr
df_final_Dtl_PeopleSoftExtr = df_lnkJrnlOut.select(
    F.rpad(F.col("BUSINESS_UNIT"), 5, " ").alias("BUSINESS_UNIT"),
    F.col("TRANSACTION_LINE"),
    F.rpad(F.col("ACCOUNTING_DT"), 10, " ").alias("ACCOUNTING_DT"),
    F.rpad(F.col("BUSINESS_UNIT_GL"), 5, " ").alias("BUSINESS_UNIT_GL"),
    F.rpad(F.col("ACCOUNT"), 10, " ").alias("ACCOUNT"),
    F.rpad(F.col("DEPTID"), 10, " ").alias("DEPTID"),
    F.rpad(F.col("OPERATION_UNIT"), 8, " ").alias("OPERATION_UNIT"),
    F.rpad(F.col("PRODUCT"), 6, " ").alias("PRODUCT"),
    F.rpad(F.col("AFFILIATE"), 5, " ").alias("AFFILIATE"),
    F.rpad(F.col("CHARTFIELD1"), 10, " ").alias("CHARTFIELD1"),
    F.col("MONETARY_AMOUNT"),
    F.rpad(F.col("LINE_DESC"), 30, " ").alias("LINE_DESC"),
    F.rpad(F.col("APP_JRNL_ID"), 10, " ").alias("APP_JRNL_ID"),
    F.rpad(F.col("JRNL_LN_REF_NO"), 10, " ").alias("JRNL_LN_REF_NO")
)

write_files(
    df_final_Dtl_PeopleSoftExtr,
    f"{adls_path_publish}/external/DTL_{TmpOutFile}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# AGGREGATOR: AggHeaderCount
df_AggHeaderCount = (
    df_lnkJrnlSumFieldsIntoCount.groupBy("TRANSACTION_LINE")
    .agg(
        F.sum("CREDIT_AMOUNT").alias("CREDIT_SUM"),
        F.sum("DEBIT_AMT").alias("DEBIT_SUM"),
        F.count("TRANSACTION_LINE").alias("ROW_COUNT")
    )
)

# CTransformerStage: trnsHeaderCounts
df_trnsHeaderCountsPrep = df_AggHeaderCount.withColumn(
    "LOBType",
    F.when(F.lit(Trans_LOB) == "231", "CAP")
     .when(F.lit(Trans_LOB) == "240", "DRG")
     .when(F.lit(Trans_LOB) == "230", "CLM")
     .when(F.lit(Trans_LOB) == "210", "INC")
     .when(F.lit(Trans_LOB) == "270", "COM")
     .otherwise("UNK")
)

df_lnkHeader = df_trnsHeaderCountsPrep.select(
    (F.col("LOBType") + F.lit("\n") + F.lit("QQQ HEADER") + F.lit("\n")).alias("HEADER"),
    F.col("CREDIT_SUM").alias("CREDIT"),
    F.col("DEBIT_SUM").alias("DEBIT"),
    F.col("ROW_COUNT").alias("ROWS"),
    (F.lit("\n") + F.col("LOBType") + F.lit("\n") + F.lit("QQQ DETAIL")).alias("TRAILER")
)

# CSeqFileStage: Hdr_PeopleSoftExtr
df_final_Hdr_PeopleSoftExtr = df_lnkHeader.select(
    F.rpad(F.col("HEADER"), 15, " ").alias("HEADER"),
    F.col("CREDIT"),
    F.col("DEBIT"),
    F.col("ROWS"),
    F.rpad(F.col("TRAILER"), 15, " ").alias("TRAILER")
)

write_files(
    df_final_Hdr_PeopleSoftExtr,
    f"{adls_path_publish}/external/HDR_{TmpOutFile}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)