# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC DESCRIPTION:    Pulls data from FACETS CMC_BLAC_BILL_ACCT to a landing file for the IDS
# MAGIC       
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC   Tao Luo - 10/20/05 - Orginal Development
# MAGIC 
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              6/4/2007          3264                              Added Balancing process to the overall                 devlIDS30                Steph Goddard             09/18/2007
# MAGIC                                                                                                       job that takes a snapshot of the source data  
# MAGIC Sharon Andrew               1/16/2008        TTR                              Added new parameters PreviousMnthFirstDay and  PreviousMnthLastDay            devlIDS30 
# MAGIC                                                                                                        Changed main extraction criteria to ensure that all changes are from the previous monthly only and not to the process date.     
# MAGIC                                                                                                       Changed main extraction outputs by breaking down each finanaical record type to its own ODBC call.    
# MAGIC                                                                                                       Changed balancing extract to reflect new date rules   devlIDS                 Steph Goddard            01/23/2008               
# MAGIC 
# MAGIC Bhoomi Dasari                 09/18/2008       3567                             Added new primay key contianer and SrcSysCdsk  devlIDS                    Steph Goddard            10/03/2008               
# MAGIC                                                                                                        and SrcSysCd                                                                                                      
# MAGIC Prabhu ES                       02/24/2022                                           S2S Remediation/MSSQL conn param added          IntegrateDev5              Kalyan Neelam            2022-06-13

# MAGIC Assign primary surrogate key, apply FormatDates command here to BILL_DUE_DT_SK and CRT_DT_SK to solve the duplicate BILL_INCM_RCPT_SK problem that arises if we remove the time earlier.
# MAGIC Balancing snapshot of source table
# MAGIC Hash file hf_bill_incm_rcpt_allcol cleared
# MAGIC Called by Commission Monthly Controller in the Commission subject area.    
# MAGIC 
# MAGIC Extract is based on dates,  
# MAGIC 
# MAGIC Runs at the beginning of the month looking for due or created receipts LAST month.
# MAGIC Writing Sequential File to ../key
# MAGIC Apply business logic, set blanks and nulls to values
# MAGIC Strip Carriage Return, Line Feed, and  Tab
# MAGIC 
# MAGIC Use STRIP.FIELD function on each character column coming in
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


CurrRunCycle = get_widget_value('CurrRunCycle','132')
RunID = get_widget_value('RunID','20080922')
ExtractFromMonthBeginDate = get_widget_value('ExtractFromMonthBeginDate','2008-08-01')
ExtractThruMonthBeginDate = get_widget_value('ExtractThruMonthBeginDate','2008-09-22')
CurrentDate = get_widget_value('CurrentDate','2008-09-22')
TmpOutFile = get_widget_value('TmpOutFile','IdsBillIncmRcptExtr.dat.pkey')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','105838')
SrcSysCd = get_widget_value('SrcSysCd','FACETS')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

# MAGIC %run ../../../../shared_containers/PrimaryKey/ComsnIncmRcpntPK
# COMMAND ----------

jdbc_url, jdbc_props = get_db_config(facets_secret_name)

# Facets_Source (ODBCConnector)
extract_query_facets_source = (
    f"SELECT \nCMC_BLAC_BILL_ACCT.BLEI_CK,\nCMC_BLAC_BILL_ACCT.BLBL_DUE_DT,\nUpper (CMC_BLAC_BILL_ACCT.BLCN_ID) as BLCN_ID,\n"
    f"CMC_BLAC_BILL_ACCT.BLAC_CREATE_DTM,\nCMC_BLAC_BILL_ACCT.BLAC_ACCT_CAT,\nCMC_BLAC_BILL_ACCT.BLAC_TYPE,\n"
    f"CMC_BLAC_BILL_ACCT.BLAC_DEBIT_AMT,\nCMC_BLAC_BILL_ACCT.BLAC_CREDIT_AMT\n\n"
    f"FROM {FacetsOwner}.CMC_BLAC_BILL_ACCT CMC_BLAC_BILL_ACCT  \n"
    f"WHERE   \n              CMC_BLAC_BILL_ACCT.ACGL_TYPE = 'T' \n  AND   CMC_BLAC_BILL_ACCT.ACGL_ACTIVITY IN ('P', 'D', 'F') \n  AND   \n  (\n       (  CMC_BLAC_BILL_ACCT.BLAC_TYPE = 'R' \n           AND \n           (\n            CMC_BLAC_BILL_ACCT.BLAC_SOURCE = 'R'  \n            OR\n            CMC_BLAC_BILL_ACCT.BLAC_SOURCE = 'B'\n           )\n\n     AND\n           (\n                  (\n                   CMC_BLAC_BILL_ACCT.BLBL_DUE_DT >= '{ExtractFromMonthBeginDate}'   \n                   AND  \n                   CMC_BLAC_BILL_ACCT.BLBL_DUE_DT <'{ExtractThruMonthBeginDate}'\n                   )\n                  OR     \n                   (\n                   CMC_BLAC_BILL_ACCT.BLAC_CREATE_DTM  >= '{ExtractFromMonthBeginDate}'   \n                   AND  \n                   CMC_BLAC_BILL_ACCT.BLAC_CREATE_DTM  <'{ExtractThruMonthBeginDate}'\n                   )\n           )\n   )\n   OR \n       (CMC_BLAC_BILL_ACCT.BLAC_TYPE = 'B'              \n         AND\n         CMC_BLAC_BILL_ACCT.BLAC_SOURCE = 'B'\n        AND\n         CMC_BLAC_BILL_ACCT.BLAC_CREATE_DTM  >= '{ExtractFromMonthBeginDate}'   \n         AND  \n         CMC_BLAC_BILL_ACCT.BLAC_CREATE_DTM  <'{ExtractThruMonthBeginDate}'\n      )\n  )"
)
df_Facets_Source = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_facets_source)
    .load()
)

# Transform (CTransformerStage)
df_transform = (
    df_Facets_Source
    .withColumn(
        "svFnclLobSk",
        GetFkeyFnclLob("PSI", F.lit(100), strip_field(F.col("BLAC_ACCT_CAT")), "X")
    )
    .withColumn("SRC_SYS_CD_SK", F.lit(SrcSysCdSk))
    .withColumn("BILL_ENTY_UNIQ_KEY", F.col("BLEI_CK"))
    .withColumn("BILL_DUE_DT_SK", F.date_format(F.col("BLBL_DUE_DT"), "yyyy-MM-dd"))
    .withColumn("BILL_CNTR_ID", strip_field(F.col("BLCN_ID")))
    .withColumn("CRT_DTM", F.date_format(F.col("BLAC_CREATE_DTM"), "yyyy-MM-dd HH:mm:ss"))
    .withColumn(
        "FNCL_LOB_SK",
        F.when(F.col("svFnclLobSk").isNull(), F.lit(0)).otherwise(F.col("svFnclLobSk"))
    )
    .withColumn(
        "ERN_INCM_AMT",
        F.when(strip_field(F.col("BLAC_TYPE")) == F.lit("B"),
               F.col("BLAC_DEBIT_AMT") - F.col("BLAC_CREDIT_AMT"))
        .otherwise(F.lit(0))
    )
)
df_transform = df_transform.select(
    "SRC_SYS_CD_SK",
    "BILL_ENTY_UNIQ_KEY",
    "BILL_DUE_DT_SK",
    "BILL_CNTR_ID",
    "CRT_DTM",
    "FNCL_LOB_SK",
    "ERN_INCM_AMT"
)

# For char columns, apply rpad if needed
# (BILL_DUE_DT_SK is char(10))
df_transform = df_transform.withColumn(
    "BILL_DUE_DT_SK", F.rpad(F.col("BILL_DUE_DT_SK"), 10, " ")
)

# Snapshot_File (CSeqFileStage) - Write
write_files(
    df_transform,
    f"{adls_path}/load/B_BILL_INCM_RCPT.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# CMC_BLAC_BILL_ACCT (ODBCConnector) - multiple queries
extract_query_Receipts_Applied_Bill_Amt_due = (
    f"SELECT \nRCPTS.BLEI_CK,\nRCPTS.BLBL_DUE_DT,\nUpper (RCPTS.BLCN_ID) as BLCN_ID,\n"
    f"RCPTS.BLAC_CREATE_DTM,\nRCPTS.CSPI_ID,\nRCPTS.PMFA_ID,\nRCPTS.BLAC_ACCT_CAT,\nRCPTS.PDPD_ID,\n"
    f"RCPTS.ACGL_ACTIVITY,\nRCPTS.ACGL_TYPE,\nRCPTS.BLAC_SOURCE,\nRCPTS.BLAC_TYPE,\nRCPTS.LOBD_ID,\n"
    f"RCPTS.BLAC_YR1_IND,\nRCPTS.BLAC_POSTING_DT,\nRCPTS.BLAC_DEBIT_AMT,\nRCPTS.BLAC_CREDIT_AMT,\n"
    f"RCPTS.ACAD_GEN_LDGR_NO,\nRCPTS.PDBL_ID \n"
    f"FROM {FacetsOwner}.CMC_BLAC_BILL_ACCT RCPTS\n"
    f"WHERE   \n(              RCPTS.ACGL_TYPE = 'T' \n  AND    RCPTS.ACGL_ACTIVITY IN ('P', 'D', 'F') \n  AND   RCPTS.BLAC_TYPE = 'R' \n"
    f"  AND   RCPTS.BLAC_SOURCE = 'R'  \n  AND   RCPTS.BLBL_DUE_DT >= '{ExtractFromMonthBeginDate}'   \n"
    f" AND    RCPTS.BLBL_DUE_DT <  '{ExtractThruMonthBeginDate}'\n)"
)
df_Receipts_Applied_Bill_Amt_due = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_Receipts_Applied_Bill_Amt_due)
    .load()
)

extract_query_Receipts_Applied_Bill_Amt_created = (
    f"SELECT \nRCPTS.BLEI_CK,\nRCPTS.BLBL_DUE_DT,\nUpper (RCPTS.BLCN_ID) as BLCN_ID,\n"
    f"RCPTS.BLAC_CREATE_DTM,\nRCPTS.CSPI_ID,\nRCPTS.PMFA_ID,\nRCPTS.BLAC_ACCT_CAT,\nRCPTS.PDPD_ID,\n"
    f"RCPTS.ACGL_ACTIVITY,\nRCPTS.ACGL_TYPE,\nRCPTS.BLAC_SOURCE,\nRCPTS.BLAC_TYPE,\nRCPTS.LOBD_ID,\n"
    f"RCPTS.BLAC_YR1_IND,\nRCPTS.BLAC_POSTING_DT,\nRCPTS.BLAC_DEBIT_AMT,\nRCPTS.BLAC_CREDIT_AMT,\n"
    f"RCPTS.ACAD_GEN_LDGR_NO,\nRCPTS.PDBL_ID \n"
    f"FROM {FacetsOwner}.CMC_BLAC_BILL_ACCT RCPTS\n"
    f"WHERE   \n(              RCPTS.ACGL_TYPE = 'T' \n  AND    RCPTS.ACGL_ACTIVITY IN ('P', 'D', 'F') \n"
    f"  AND    RCPTS.BLAC_TYPE = 'R' \n  AND   RCPTS.BLAC_SOURCE = 'R'  \n\n"
    f"  AND   RCPTS.BLAC_CREATE_DTM  >= '{ExtractFromMonthBeginDate}' \n"
    f"  AND     RCPTS.BLAC_CREATE_DTM  <'{ExtractThruMonthBeginDate}'\n)"
)
df_Receipts_Applied_Bill_Amt_created = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_Receipts_Applied_Bill_Amt_created)
    .load()
)

extract_query_Receipt_Suspense_Amt_due = (
    f"SELECT \nRCPTS.BLEI_CK,\nRCPTS.BLBL_DUE_DT,\nUpper (RCPTS.BLCN_ID)  as BLCN_ID,\n"
    f"RCPTS.BLAC_CREATE_DTM,\nRCPTS.CSPI_ID,\nRCPTS.PMFA_ID,\nRCPTS.BLAC_ACCT_CAT,\nRCPTS.PDPD_ID,\n"
    f"RCPTS.ACGL_ACTIVITY,\nRCPTS.ACGL_TYPE,\nRCPTS.BLAC_SOURCE,\nRCPTS.BLAC_TYPE,\nRCPTS.LOBD_ID,\n"
    f"RCPTS.BLAC_YR1_IND,\nRCPTS.BLAC_POSTING_DT,\nRCPTS.BLAC_DEBIT_AMT,\nRCPTS.BLAC_CREDIT_AMT,\n"
    f"RCPTS.ACAD_GEN_LDGR_NO,\nRCPTS.PDBL_ID \n"
    f"FROM {FacetsOwner}.CMC_BLAC_BILL_ACCT RCPTS\n"
    f"WHERE   \n(             RCPTS.ACGL_TYPE = 'T' \n  AND   RCPTS.ACGL_ACTIVITY IN ('P', 'D', 'F') \n"
    f"  AND   RCPTS.BLAC_TYPE = 'R' \n  AND   RCPTS.BLAC_SOURCE = 'B'  \n"
    f"  AND   RCPTS.BLBL_DUE_DT >= '{ExtractFromMonthBeginDate}'   \n"
    f" AND    RCPTS.BLBL_DUE_DT <  '{ExtractThruMonthBeginDate}'\n\n)"
)
df_Receipt_Suspense_Amt_due = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_Receipt_Suspense_Amt_due)
    .load()
)

extract_query_Receipt_Suspense_Amt_created = (
    f"SELECT \nRCPTS.BLEI_CK,\nRCPTS.BLBL_DUE_DT,\nUpper (RCPTS.BLCN_ID) as BLCN_ID,\n"
    f"RCPTS.BLAC_CREATE_DTM,\nRCPTS.CSPI_ID,\nRCPTS.PMFA_ID,\nRCPTS.BLAC_ACCT_CAT,\nRCPTS.PDPD_ID,\n"
    f"RCPTS.ACGL_ACTIVITY,\nRCPTS.ACGL_TYPE,\nRCPTS.BLAC_SOURCE,\nRCPTS.BLAC_TYPE,\nRCPTS.LOBD_ID,\n"
    f"RCPTS.BLAC_YR1_IND,\nRCPTS.BLAC_POSTING_DT,\nRCPTS.BLAC_DEBIT_AMT,\nRCPTS.BLAC_CREDIT_AMT,\n"
    f"RCPTS.ACAD_GEN_LDGR_NO,\nRCPTS.PDBL_ID \n"
    f"FROM {FacetsOwner}.CMC_BLAC_BILL_ACCT RCPTS\n"
    f"WHERE   \n(             RCPTS.ACGL_TYPE = 'T' \n  AND   RCPTS.ACGL_ACTIVITY IN ('P', 'D', 'F') \n"
    f"  AND   RCPTS.BLAC_TYPE = 'R' \n  AND   RCPTS.BLAC_SOURCE = 'B'  \n\n"
    f"  AND   RCPTS.BLAC_CREATE_DTM  >= '{ExtractFromMonthBeginDate}' \n"
    f"  AND     RCPTS.BLAC_CREATE_DTM  <'{ExtractThruMonthBeginDate}'\n)"
)
df_Receipt_Suspense_Amt_created = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_Receipt_Suspense_Amt_created)
    .load()
)

extract_query_Am_Billed_before_Receipts_created = (
    f"SELECT \nRCPTS.BLEI_CK,\nRCPTS.BLBL_DUE_DT,\nUpper (RCPTS.BLCN_ID) as BLCN_ID,\n"
    f"RCPTS.BLAC_CREATE_DTM,\nRCPTS.CSPI_ID,\nRCPTS.PMFA_ID,\nRCPTS.BLAC_ACCT_CAT,\nRCPTS.PDPD_ID,\n"
    f"RCPTS.ACGL_ACTIVITY,\nRCPTS.ACGL_TYPE,\nRCPTS.BLAC_SOURCE,\nRCPTS.BLAC_TYPE,\nRCPTS.LOBD_ID,\n"
    f"RCPTS.BLAC_YR1_IND,\nRCPTS.BLAC_POSTING_DT,\nRCPTS.BLAC_DEBIT_AMT,\nRCPTS.BLAC_CREDIT_AMT,\n"
    f"RCPTS.ACAD_GEN_LDGR_NO,\nRCPTS.PDBL_ID \n"
    f"FROM {FacetsOwner}.CMC_BLAC_BILL_ACCT RCPTS\n"
    f"WHERE   \n(              RCPTS.ACGL_TYPE = 'T' \n  AND   RCPTS.ACGL_ACTIVITY IN ('P', 'D', 'F') \n"
    f"  AND   RCPTS.BLAC_TYPE = 'B' \n  AND   RCPTS.BLAC_SOURCE = 'B'  \n"
    f"  AND   RCPTS.BLAC_CREATE_DTM  >= '{ExtractFromMonthBeginDate}' \n"
    f"  AND   RCPTS.BLAC_CREATE_DTM  <'{ExtractThruMonthBeginDate}'\n)"
)
df_Am_Billed_before_Receipts_created = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_Am_Billed_before_Receipts_created)
    .load()
)

# Link_Collector_13 (CCollector) - union of all inputs
df_link_collector_13 = (
    df_Receipts_Applied_Bill_Amt_due
    .unionByName(df_Receipts_Applied_Bill_Amt_created)
    .unionByName(df_Receipt_Suspense_Amt_due)
    .unionByName(df_Receipt_Suspense_Amt_created)
    .unionByName(df_Am_Billed_before_Receipts_created)
)

# hf_bill_incm_rcpt_all (CHashedFileStage) - Scenario A intermediate hash file
# Key columns: BLEI_CK, BLBL_DUE_DT, BLCN_ID, BLAC_CREATE_DTM
df_hf_bill_incm_rcpt_all = dedup_sort(
    df_link_collector_13,
    ["BLEI_CK", "BLBL_DUE_DT", "BLCN_ID", "BLAC_CREATE_DTM"],
    []
)

# StripField (CTransformerStage)
df_stripfield = (
    df_hf_bill_incm_rcpt_all
    .withColumn("BLEI_CK", F.col("BLEI_CK"))
    .withColumn("BLBL_DUE_DT", F.col("BLBL_DUE_DT"))
    .withColumn("BLCN_ID", strip_field(F.col("BLCN_ID")))
    .withColumn("BLAC_CREATE_DTM", F.col("BLAC_CREATE_DTM"))
    .withColumn("CSPI_ID", strip_field(F.col("CSPI_ID")))
    .withColumn("PMFA_ID", strip_field(F.col("PMFA_ID")))
    .withColumn("BLAC_ACCT_CAT", strip_field(F.col("BLAC_ACCT_CAT")))
    .withColumn("PDPD_ID", strip_field(F.col("PDPD_ID")))
    .withColumn("ACGL_ACTIVITY", strip_field(F.col("ACGL_ACTIVITY")))
    .withColumn("ACGL_TYPE", strip_field(F.col("ACGL_TYPE")))
    .withColumn("BLAC_SOURCE", strip_field(F.col("BLAC_SOURCE")))
    .withColumn("BLAC_TYPE", strip_field(F.col("BLAC_TYPE")))
    .withColumn("LOBD_ID", strip_field(F.col("LOBD_ID")))
    .withColumn("BLAC_YR1_IND", strip_field(F.col("BLAC_YR1_IND")))
    .withColumn("BLAC_POSTING_DT", F.col("BLAC_POSTING_DT"))
    .withColumn("BLAC_DEBIT_AMT", F.col("BLAC_DEBIT_AMT"))
    .withColumn("BLAC_CREDIT_AMT", F.col("BLAC_CREDIT_AMT"))
    .withColumn("ACAD_GEN_LDGR_NO", strip_field(F.col("ACAD_GEN_LDGR_NO")))
    .withColumn("PDBL_ID", strip_field(F.col("PDBL_ID")))
)
df_stripfield = df_stripfield.select(
    "BLEI_CK",
    "BLBL_DUE_DT",
    "BLCN_ID",
    "BLAC_CREATE_DTM",
    "CSPI_ID",
    "PMFA_ID",
    "BLAC_ACCT_CAT",
    "PDPD_ID",
    "ACGL_ACTIVITY",
    "ACGL_TYPE",
    "BLAC_SOURCE",
    "BLAC_TYPE",
    "LOBD_ID",
    "BLAC_YR1_IND",
    "BLAC_POSTING_DT",
    "BLAC_DEBIT_AMT",
    "BLAC_CREDIT_AMT",
    "ACAD_GEN_LDGR_NO",
    "PDBL_ID"
)

# BusinessRules (CTransformerStage)
df_businessrules_input = df_stripfield.withColumn("RowPassThru", F.lit("Y"))

# Output link "AllCol"
df_allcol = (
    df_businessrules_input
    .withColumn("SRC_SYS_CD_SK", F.lit(SrcSysCdSk))
    .withColumn("BLEI_CK", F.col("BLEI_CK"))
    .withColumn("BLBL_DUE_DT", F.date_format(F.col("BLBL_DUE_DT"), "yyyy-MM-dd"))
    .withColumn("BLCN_ID", F.col("BLCN_ID"))
    .withColumn("BLAC_CREATE_DTM", F.date_format(F.col("BLAC_CREATE_DTM"), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
    .withColumn("INSRT_UPDT_CD", F.lit("I"))
    .withColumn("DISCARD_IN", F.lit("N"))
    .withColumn("PASS_THRU_IN", F.col("RowPassThru"))
    .withColumn("FIRST_RECYC_DT", F.lit(CurrentDate))
    .withColumn("ERR_CT", F.lit(0))
    .withColumn("RECYCLE_CT", F.lit(0))
    .withColumn("SRC_SYS_CD", F.lit("FACETS"))
    .withColumn(
        "PRI_KEY_STRING",
        F.concat_ws(";", F.lit("FACETS"), F.col("BLEI_CK"), F.col("BLBL_DUE_DT"), F.col("BLCN_ID"), F.col("BLAC_CREATE_DTM"))
    )
    .withColumn("CSPI_ID", F.col("CSPI_ID"))
    .withColumn(
        "PMFA_ID",
        F.when(F.length(F.col("PMFA_ID")) < 1, F.lit("NA")).otherwise(F.col("PMFA_ID"))
    )
    .withColumn("BLAC_ACCT_CAT", F.col("BLAC_ACCT_CAT"))
    .withColumn("PDPD_ID", F.col("PDPD_ID"))
    .withColumn("ACGL_ACTIVITY", F.col("ACGL_ACTIVITY"))
    .withColumn("ACGL_TYPE", F.col("ACGL_TYPE"))
    .withColumn("BLAC_SOURCE", F.col("BLAC_SOURCE"))
    .withColumn("BLAC_TYPE", F.col("BLAC_TYPE"))
    .withColumn("LOBD_ID", F.col("LOBD_ID"))
    .withColumn(
        "BLAC_YR1_IND",
        F.when((F.col("BLAC_YR1_IND") == "") | (F.col("BLAC_YR1_IND").isNull()), F.lit("X"))
        .otherwise(F.col("BLAC_YR1_IND"))
    )
    .withColumn(
        "CRT_DT_SK",
        F.date_format(F.col("BLAC_CREATE_DTM"), "yyyy-MM-dd")
    )
    .withColumn(
        "BLAC_POSTING_DT",
        F.date_format(F.col("BLAC_POSTING_DT"), "yyyy-MM-dd")
    )
    .withColumn(
        "ERN_INCM_AMT",
        F.when(F.col("BLAC_TYPE") == F.lit("B"), F.col("BLAC_DEBIT_AMT") - F.col("BLAC_CREDIT_AMT"))
        .otherwise(F.lit(0))
    )
    .withColumn(
        "RVNU_RCVD_AMT",
        F.when(F.col("BLAC_TYPE") == F.lit("B"), F.lit(0))
        .otherwise(-1 * (F.col("BLAC_DEBIT_AMT") - F.col("BLAC_CREDIT_AMT")))
    )
    .withColumn(
        "ACAD_GEN_LDGR_NO",
        F.when((F.col("ACAD_GEN_LDGR_NO").isNull()) | (F.length(F.trim(F.col("ACAD_GEN_LDGR_NO"))) == 0),
               F.lit("NA"))
        .otherwise(F.col("ACAD_GEN_LDGR_NO"))
    )
    .withColumn(
        "PDBL_ID",
        F.when((F.col("PDBL_ID").isNull()) | (F.length(F.trim(F.col("PDBL_ID"))) == 0),
               F.lit("NA"))
        .otherwise(F.col("PDBL_ID"))
    )
)
df_allcol = df_allcol.select(
    "SRC_SYS_CD_SK",
    "BLEI_CK",
    "BLBL_DUE_DT",
    "BLCN_ID",
    "BLAC_CREATE_DTM",
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "CSPI_ID",
    "PMFA_ID",
    "BLAC_ACCT_CAT",
    "PDPD_ID",
    "ACGL_ACTIVITY",
    "ACGL_TYPE",
    "BLAC_SOURCE",
    "BLAC_TYPE",
    "LOBD_ID",
    "BLAC_YR1_IND",
    "CRT_DT_SK",
    "BLAC_POSTING_DT",
    "ERN_INCM_AMT",
    "RVNU_RCVD_AMT",
    "ACAD_GEN_LDGR_NO",
    "PDBL_ID"
)

# Output link "Transform"
df_transform_out = (
    df_businessrules_input
    .withColumn("SRC_SYS_CD_SK", F.lit(SrcSysCdSk))
    .withColumn("BILL_ENTY_UNIQ_KEY", F.col("BLEI_CK"))
    .withColumn("BILL_DUE_DT_SK", F.date_format(F.col("BLBL_DUE_DT"), "yyyy-MM-dd"))
    .withColumn("BILL_CNTR_ID", F.col("BLCN_ID"))
    .withColumn(
        "CRT_DTM",
        F.concat(
            F.substring(F.col("BLAC_CREATE_DTM"),1,10), F.lit(" "),
            F.substring(F.col("BLAC_CREATE_DTM"),12,2), F.lit(":"),
            F.substring(F.col("BLAC_CREATE_DTM"),15,2), F.lit(":"),
            F.substring(F.col("BLAC_CREATE_DTM"),18,2), F.lit("."),
            F.substring(F.col("BLAC_CREATE_DTM"),21,6)
        )
    )
)
df_transform_out = df_transform_out.select(
    "SRC_SYS_CD_SK",
    "BILL_ENTY_UNIQ_KEY",
    "BILL_DUE_DT_SK",
    "BILL_CNTR_ID",
    "CRT_DTM"
)

# ComsnIncmRcpntPK (CContainerStage)
params = {
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd,
    "RunID": RunID,
    "CurrentDate": CurrentDate,
    "IDSOwner": IDSOwner
}
df_ComsnIncmRcpntPK_output = ComsnIncmRcpntPK(df_allcol, df_transform_out, params)

# IdsBillIncmRcptExtr (CSeqFileStage) - final write
# Apply rpad for char columns in the final output
df_final = df_ComsnIncmRcpntPK_output

# Identify which columns are char with known lengths from the output pin:
# JOB_EXCTN_RCRD_ERR_SK (no char), INSRT_UPDT_CD (char(10)), DISCARD_IN (char(1)), PASS_THRU_IN (char(1)),
# FIRST_RECYC_DT (no char), ERR_CT (no char), RECYCLE_CT (no char), SRC_SYS_CD (no char),
# PRI_KEY_STRING (no char), BILL_INCM_RCPT_SK (no length given, skip), BILL_ENTY_UNIQ_KEY (no length given),
# BILL_DUE_DT_SK (char(10)), BILL_CNTR_ID (no length given), CRT_DTM (no length given),
# CRT_RUN_CYC_EXCTN_SK (no definition?), LAST_UPDT_RUN_CYC_EXCTN_SK (no definition?),
# CSPI_ID (char(8)), PMFA_ID (char(2)), BLAC_ACCT_CAT (char(4)), PDPD_ID (char(8)),
# ACGL_ACTIVITY (char(1)), ACGL_TYPE (char(1)), BLAC_SOURCE (char(1)), BLAC_TYPE (char(1)),
# LOBD_ID (char(4)), FIRST_YR_IN (char(1)), CRT_DT_SK (char(10)), POSTED_DT_SK (char(10)),
# ERN_INCM_AMT (no char), RVNU_RCVD_AMT (no char), GL_NO (???), PROD_BILL_CMPNT_ID (char(10))

# We will do rpad where lengths are indicated
df_final = df_final.withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " "))
df_final = df_final.withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " "))
df_final = df_final.withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " "))
df_final = df_final.withColumn("BILL_DUE_DT_SK", F.rpad(F.col("BILL_DUE_DT_SK"), 10, " "))
df_final = df_final.withColumn("CSPI_ID", F.rpad(F.col("CSPI_ID"), 8, " "))
df_final = df_final.withColumn("PMFA_ID", F.rpad(F.col("PMFA_ID"), 2, " "))
df_final = df_final.withColumn("BLAC_ACCT_CAT", F.rpad(F.col("BLAC_ACCT_CAT"), 4, " "))
df_final = df_final.withColumn("PDPD_ID", F.rpad(F.col("PDPD_ID"), 8, " "))
df_final = df_final.withColumn("ACGL_ACTIVITY", F.rpad(F.col("ACGL_ACTIVITY"), 1, " "))
df_final = df_final.withColumn("ACGL_TYPE", F.rpad(F.col("ACGL_TYPE"), 1, " "))
df_final = df_final.withColumn("BLAC_SOURCE", F.rpad(F.col("BLAC_SOURCE"), 1, " "))
df_final = df_final.withColumn("BLAC_TYPE", F.rpad(F.col("BLAC_TYPE"), 1, " "))
df_final = df_final.withColumn("LOBD_ID", F.rpad(F.col("LOBD_ID"), 4, " "))
df_final = df_final.withColumn("FIRST_YR_IN", F.rpad(F.col("FIRST_YR_IN"), 1, " "))
df_final = df_final.withColumn("CRT_DT_SK", F.rpad(F.col("CRT_DT_SK"), 10, " "))
df_final = df_final.withColumn("POSTED_DT_SK", F.rpad(F.col("POSTED_DT_SK"), 10, " "))
df_final = df_final.withColumn("PROD_BILL_CMPNT_ID", F.rpad(F.col("PROD_BILL_CMPNT_ID"), 10, " "))

write_files(
    df_final,
    f"{adls_path}/key/{TmpOutFile}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)