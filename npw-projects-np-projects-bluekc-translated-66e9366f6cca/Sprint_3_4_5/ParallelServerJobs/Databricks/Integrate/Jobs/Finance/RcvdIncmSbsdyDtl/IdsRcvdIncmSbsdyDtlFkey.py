# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Called By:FacetsBCBSRcvdIncmSbsdyDtlLoadSeq (FacetsBCBSRcvdIncmSbsdyDtlCntl)
# MAGIC  
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                                   Project/                                                                                                                        Code                  Date
# MAGIC Developer            Date              Altiris #     Change Description                                                                                     Reviewer            Reviewed
# MAGIC -----------------------    ------------------   -------------   -----------------------------------------------------------------------------------------------------------------   -------------------------  -------------------
# MAGIC Santosh Bokka  04/09/2014    5128        Originally Programmed                                                                                 Kalyan Neelam   2014-04-28
# MAGIC Santosh Bokka  06/17/2014    5128      Added new fields  PROD_ID,RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD  Kalyan Neelam   2014-06-19
# MAGIC Abhiram Dasarathy	10/26/2015  5128	Changed the business logic based on the change in the table structure        Bhoomi Dasari      10/28/2015  
# MAGIC Abhiram Dasarathy	11/18/2015  5128	Added the BILL_INCM_RCPT stage lookup for RCVD_INCM_LOB_SK      Kalyan Neelam   2015-11-19
# MAGIC Abhiram Dasarathy	12/14/2015  5128	Changed the business logic for change in the table structure - added new  Kalyan Neelam   2015-12-14
# MAGIC 				new field CMS_ENR_PAYMT_UNIQ_KEY
# MAGIC Abhiram Dasarathy	02/11/2016  5128   Changed the table structure for the IDS table                                              Kalyan Neelam    2016-02-11
# MAGIC 				Received Income Subsidy Detail Table : Natural Key Change
# MAGIC 				CMS Enrollment Payment Unique Key
# MAGIC  				CMS Enrollment Payment Amount Sequence Number
# MAGIC  				Received Income Subsidy Detail Payment Type Code
# MAGIC  				Received Income Subsidy Account Activity Code
# MAGIC  				Source System Code SK
# MAGIC 
# MAGIC Deepika C       2025-04-09  US 644701   1.Modified the input file FctsBCBSRcvdIncmSbsdyDtlExtr stage              IntegrateDev2          Jeyaprasanna      2025-04-28
# MAGIC                                                                     to be in sync with the primary key job output file
# MAGIC                                                                  2.Created new stage variable svSrcSysCdSk and used it in 
# MAGIC                                                                      RCVD_INCM_SBSDY_LOB_CD_SK column logic

# MAGIC Set all foreign surrogate keys
# MAGIC Merge source data with default rows
# MAGIC Writing Sequential File to /load
# MAGIC Read common record format file from extract job.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DecimalType
)
from pyspark.sql import functions as F
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
Logging = get_widget_value('Logging','')
InFile = get_widget_value('InFile','')
RunDate = get_widget_value('RunDate','')
SourceSK = get_widget_value('SourceSK','')

schemaFctsBCBSRcvdIncmSbsdyDtlExtr = StructType([
    StructField("RCVD_INCM_SBSDY_DTL_SK", IntegerType(), True),
    StructField("CMS_ENR_PAYMT_UNIQ_KEY", IntegerType(), True),
    StructField("CMS_ENR_PAYMT_AMT_SEQ_NO", IntegerType(), True),
    StructField("RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD", StringType(), True),
    StructField("RCVD_INCM_SBSDY_ACCT_ACTVTY_CD", StringType(), True),
    StructField("SRC_SYS_CD_SK", IntegerType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("BILL_ENTY_SK", IntegerType(), True),
    StructField("CLS_PLN_SK", IntegerType(), True),
    StructField("FEE_DSCNT_SK", IntegerType(), True),
    StructField("FNCL_LOB_SK", IntegerType(), True),
    StructField("PROD_SK", IntegerType(), True),
    StructField("QHP_SK", IntegerType(), True),
    StructField("RCVD_INCM_SBSDY_ACCT_ACTVTY_CD_SK", IntegerType(), True),
    StructField("RCVD_INCM_SBSDY_ACCT_TYP_CD_SK", IntegerType(), True),
    StructField("RCVD_INCM_SBSDY_ACTV_SRC_CD_SK", IntegerType(), True),
    StructField("RCVD_INCM_SBSDY_ACTV_TYP_CD_SK", IntegerType(), True),
    StructField("RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD_SK", IntegerType(), True),
    StructField("RCVD_INCM_SBSDY_LOB_CD_SK", IntegerType(), True),
    StructField("FIRST_YR_IN", StringType(), True),
    StructField("BILL_DUE_DT_SK", StringType(), True),
    StructField("CRT_DT_SK", StringType(), True),
    StructField("POSTED_DT_SK", StringType(), True),
    StructField("ERN_INCM_AMT", DecimalType(38,10), True),
    StructField("RVNU_RCVD_AMT", DecimalType(38,10), True),
    StructField("BILL_ENTY_UNIQ_KEY", IntegerType(), True),
    StructField("BILL_GRP_BILL_ENTY_UNIQ_KEY", IntegerType(), True),
    StructField("EXCH_MBR_ID", StringType(), True),
    StructField("EXCH_SUB_ID", StringType(), True),
    StructField("EXCH_POL_ID", StringType(), True),
    StructField("GL_NO", StringType(), True),
    StructField("PROD_ID", StringType(), True),
    StructField("PROD_BILL_CMPNT_ID", StringType(), True),
    StructField("QHP_ID", StringType(), True)
])

file_path_FctsBCBSRcvdIncmSbsdyDtlExtr = f"{adls_path}/key/{InFile}"
df_FctsBCBSRcvdIncmSbsdyDtlExtr = (
    spark.read.format("csv")
    .schema(schemaFctsBCBSRcvdIncmSbsdyDtlExtr)
    .option("header", "true")
    .option("quote", "\u0000")
    .load(file_path_FctsBCBSRcvdIncmSbsdyDtlExtr)
)

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query_BILL_INCM_RCPT = f"SELECT BILL_ENTY_UNIQ_KEY,BILL_DUE_DT_SK,BILL_INCM_RCPT_LOB_CD_SK FROM {IDSOwner}.BILL_INCM_RCPT"
df_BILL_INCM_RCPT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_BILL_INCM_RCPT)
    .load()
)

df_lobcdsk = dedup_sort(
    df_BILL_INCM_RCPT,
    ["BILL_ENTY_UNIQ_KEY", "BILL_DUE_DT_SK"],
    [("BILL_ENTY_UNIQ_KEY", "A"), ("BILL_DUE_DT_SK", "A")]
)

df_joined = (
    df_FctsBCBSRcvdIncmSbsdyDtlExtr.alias("Key")
    .join(
        df_lobcdsk.alias("LobCdSk_Lkup"),
        (
            (F.col("Key.BILL_ENTY_UNIQ_KEY") == F.col("LobCdSk_Lkup.BILL_ENTY_UNIQ_KEY"))
            & (F.col("Key.BILL_DUE_DT_SK") == F.col("LobCdSk_Lkup.BILL_DUE_DT_SK"))
        ),
        "left"
    )
)

df_enriched_pre = (
    df_joined
    .withColumn("PassThru", F.lit("Y"))
    .withColumn("svBillEntySK", GetFkeyBillEnty(F.lit("FACETS"), F.col("Key.RCVD_INCM_SBSDY_DTL_SK"), F.col("Key.BILL_ENTY_UNIQ_KEY"), F.lit(Logging)))
    .withColumn("svFeeDscntSk", GetFkeyFeeDscnt(F.lit("FACETS"), F.col("Key.RCVD_INCM_SBSDY_DTL_SK"), F.col("Key.FEE_DSCNT_SK"), F.lit("X")))
    .withColumn("svLobCdSK", GetFkeyCodes(F.lit("BCBSFINANCE"), F.col("Key.RCVD_INCM_SBSDY_DTL_SK"), F.lit("NA"), F.col("Key.RCVD_INCM_SBSDY_LOB_CD_SK"), F.lit("X")))
    .withColumn("svFnclLobSk", GetFkeyFnclLob(F.lit("FACETS"), F.col("Key.RCVD_INCM_SBSDY_DTL_SK"), F.col("Key.FNCL_LOB_SK"), F.lit("X")))
    .withColumn("svAcctActvCdSK", GetFkeyClctnDomainCodes(F.lit("BCBSFINANCE"), F.col("Key.RCVD_INCM_SBSDY_DTL_SK"), F.lit("INCOME SUBSIDY ACCOUNT ACTIVITY"), F.lit("BCBS FINANCE"), F.lit("INCOME SUBSIDY ACCOUNT ACTIVITY"), F.lit("IDS"), F.col("Key.RCVD_INCM_SBSDY_ACCT_ACTVTY_CD"), F.lit(Logging)))
    .withColumn("svAcctTypCdSK", GetFkeyClctnDomainCodes(F.lit("BCBSFINANCE"), F.col("Key.RCVD_INCM_SBSDY_DTL_SK"), F.lit("INCOME SUBSIDY ACCOUNT ACTIVITY"), F.lit("BCBS FINANCE"), F.lit("INCOME SUBSIDY ACCOUNT ACTIVITY"), F.lit("IDS"), F.col("Key.RCVD_INCM_SBSDY_ACCT_ACTVTY_CD"), F.lit(Logging)))
    .withColumn("svProdSk", GetFkeyProd(F.lit("FACETS"), F.col("Key.RCVD_INCM_SBSDY_DTL_SK"), F.col("Key.PROD_ID"), F.lit(Logging)))
    .withColumn("svActvSrcCdSK", GetFkeyClctnDomainCodes(F.lit("BCBSFINANCE"), F.col("Key.RCVD_INCM_SBSDY_DTL_SK"), F.lit("INCOME SUBSIDY ACCOUNT ACTIVITY"), F.lit("BCBS FINANCE"), F.lit("INCOME SUBSIDY ACCOUNT ACTIVITY"), F.lit("IDS"), F.col("Key.RCVD_INCM_SBSDY_ACCT_ACTVTY_CD"), F.lit(Logging)))
    .withColumn("svActvTypCdSK", GetFkeyClctnDomainCodes(F.lit("BCBSFINANCE"), F.col("Key.RCVD_INCM_SBSDY_DTL_SK"), F.lit("INCOME SUBSIDY ACCOUNT ACTIVITY"), F.lit("BCBS FINANCE"), F.lit("INCOME SUBSIDY ACCOUNT ACTIVITY"), F.lit("IDS"), F.col("Key.RCVD_INCM_SBSDY_ACCT_ACTVTY_CD"), F.lit(Logging)))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("Key.RCVD_INCM_SBSDY_DTL_SK")))
    .withColumn("svSbsdyTypCdSk", GetFkeyClctnDomainCodes(F.lit("CMS"), F.col("Key.RCVD_INCM_SBSDY_DTL_SK"), F.lit("ENROLLMENT PAYMENT TYPE"), F.lit("CMS"), F.lit("ENROLLMENT PAYMENT TYPE"), F.lit("IDS"), F.col("Key.RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD"), F.lit(Logging)))
    .withColumn("svSrcSysCdSk", GetFkeyCodes(F.lit("IDS"), F.lit(1), F.lit("SOURCE SYSTEM"), F.lit("BCBSKC"), F.lit("X")))
)

windowSpec = Window.orderBy(F.lit(1))
df_enriched = df_enriched_pre.withColumn("row_id", F.row_number().over(windowSpec))

df_recycle = (
    df_enriched
    .filter(F.col("ErrCount") > 0)
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.rpad(F.lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
        F.rpad(F.lit("N"), 1, " ").alias("DISCARD_IN"),
        F.rpad(F.col("PassThru"), 1, " ").alias("PASS_THRU_IN"),
        F.lit(RunDate).alias("FIRST_RECYC_DT"),
        F.col("ErrCount").alias("ERR_CT"),
        F.lit(1).alias("RECYCLE_CT"),
        F.concat(
            F.col("CMS_ENR_PAYMT_UNIQ_KEY"),
            F.lit(";"),
            F.col("CMS_ENR_PAYMT_AMT_SEQ_NO"),
            F.lit(";"),
            F.col("RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD"),
            F.lit(";"),
            F.col("RCVD_INCM_SBSDY_ACCT_ACTVTY_CD"),
            F.lit(";"),
            F.col("SRC_SYS_CD_SK")
        ).alias("PRI_KEY_STRING"),
        F.col("RCVD_INCM_SBSDY_DTL_SK").alias("RCVD_INCM_SBSDY_DTL_SK"),
        F.col("CMS_ENR_PAYMT_UNIQ_KEY").alias("CMS_ENR_PAYMT_UNIQ_KEY"),
        F.col("CMS_ENR_PAYMT_AMT_SEQ_NO").alias("CMS_ENR_PAYMT_AMT_SEQ_NO"),
        F.col("RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD").alias("RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD"),
        F.col("RCVD_INCM_SBSDY_ACCT_ACTVTY_CD").alias("RCVD_INCM_SBSDY_ACCT_ACTVTY_CD"),
        F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("BILL_ENTY_UNIQ_KEY").alias("BILL_ENTY_UNIQ_KEY"),
        F.col("CLS_PLN_SK").alias("CLS_PLN_SK"),
        F.col("FEE_DSCNT_SK").alias("FEE_DSCNT_SK"),
        F.col("FNCL_LOB_SK").alias("FNCL_LOB_SK"),
        F.col("QHP_SK").alias("QHP_SK"),
        F.col("RCVD_INCM_SBSDY_LOB_CD_SK").alias("RCVD_INCM_SBSDY_LOB_CD_SK"),
        F.rpad(F.col("FIRST_YR_IN"), 1, " ").alias("FIRST_YR_IN"),
        F.rpad(F.col("BILL_DUE_DT_SK"), 10, " ").alias("BILL_DUE_DT_SK"),
        F.rpad(F.col("CRT_DT_SK"), 10, " ").alias("CRT_DT_SK"),
        F.rpad(F.col("POSTED_DT_SK"), 10, " ").alias("POSTED_DT_SK"),
        F.col("ERN_INCM_AMT").alias("ERN_INCM_AMT"),
        F.col("RVNU_RCVD_AMT").alias("RVNU_RCVD_AMT"),
        F.col("BILL_GRP_BILL_ENTY_UNIQ_KEY").alias("BILL_GRP_BILL_ENTY_UNIQ_KEY"),
        F.col("EXCH_MBR_ID").alias("EXCH_MBR_ID"),
        F.col("EXCH_POL_ID").alias("EXCH_ASG_POL_ID"),
        F.col("EXCH_SUB_ID").alias("EXCH_ASG_SUB_ID"),
        F.col("GL_NO").alias("GL_NO"),
        F.rpad(F.col("PROD_ID"), 8, " ").alias("PROD_ID"),
        F.col("PROD_BILL_CMPNT_ID").alias("PROD_BILL_CMPNT_ID"),
        F.col("QHP_ID").alias("QHP_ID")
    )
)

write_files(
    df_recycle,
    f"{adls_path}/hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_parqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_fkey = (
    df_enriched
    .filter((F.col("ErrCount") == 0) | (F.col("PassThru") == "Y"))
    .select(
        F.col("RCVD_INCM_SBSDY_DTL_SK").alias("RCVD_INCM_SBSDY_DTL_SK"),
        F.col("CMS_ENR_PAYMT_UNIQ_KEY").alias("CMS_ENR_PAYMT_UNIQ_KEY"),
        F.col("CMS_ENR_PAYMT_AMT_SEQ_NO").alias("CMS_ENR_PAYMT_AMT_SEQ_NO"),
        F.col("RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD").alias("RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD"),
        F.col("RCVD_INCM_SBSDY_ACCT_ACTVTY_CD").alias("RCVD_INCM_SBSDY_ACCT_ACTVTY_CD"),
        F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("svBillEntySK").alias("BILL_ENTY_SK"),
        F.col("Key.CLS_PLN_SK").alias("CLS_PLN_SK"),
        F.lit(1).alias("FEE_DSCNT_SK"),
        F.lit(1).alias("FNCL_LOB_SK"),
        F.col("svProdSk").alias("PROD_SK"),
        F.col("Key.QHP_SK").alias("QHP_SK"),
        F.col("svAcctActvCdSK").alias("RCVD_INCM_SBSDY_ACCT_ACTVTY_CD_SK"),
        F.col("svAcctTypCdSK").alias("RCVD_INCM_SBSDY_ACCT_TYP_CD_SK"),
        F.col("svActvSrcCdSK").alias("RCVD_INCM_SBSDY_ACTV_SRC_CD_SK"),
        F.col("svActvTypCdSK").alias("RCVD_INCM_SBSDY_ACTV_TYP_CD_SK"),
        F.col("svSbsdyTypCdSk").alias("RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD_SK"),
        F.expr("CASE WHEN Key.SRC_SYS_CD_SK = svSrcSysCdSk THEN 1 WHEN LobCdSk_Lkup.BILL_INCM_RCPT_LOB_CD_SK IS NULL OR LENGTH(trim(LobCdSk_Lkup.BILL_INCM_RCPT_LOB_CD_SK))=0 THEN 0 ELSE LobCdSk_Lkup.BILL_INCM_RCPT_LOB_CD_SK END").alias("RCVD_INCM_SBSDY_LOB_CD_SK"),
        F.rpad(F.col("Key.FIRST_YR_IN"), 1, " ").alias("FIRST_YR_IN"),
        F.rpad(F.col("Key.BILL_DUE_DT_SK"), 10, " ").alias("BILL_DUE_DT_SK"),
        F.rpad(F.col("Key.CRT_DT_SK"), 10, " ").alias("CRT_DT_SK"),
        F.rpad(F.col("Key.POSTED_DT_SK"), 10, " ").alias("POSTED_DT_SK"),
        F.col("Key.ERN_INCM_AMT").alias("ERN_INCM_AMT"),
        F.col("Key.RVNU_RCVD_AMT").alias("RVNU_RCVD_AMT"),
        F.col("Key.BILL_ENTY_UNIQ_KEY").alias("BILL_ENTY_UNIQ_KEY"),
        F.expr("CASE WHEN Key.BILL_GRP_BILL_ENTY_UNIQ_KEY IS NULL OR LENGTH(trim(Key.BILL_GRP_BILL_ENTY_UNIQ_KEY))=0 THEN 0 ELSE Key.BILL_GRP_BILL_ENTY_UNIQ_KEY END").alias("BILL_GRP_BILL_ENTY_UNIQ_KEY"),
        F.col("Key.EXCH_MBR_ID").alias("EXCH_MBR_ID"),
        F.col("Key.EXCH_SUB_ID").alias("EXCH_SUB_ID"),
        F.col("Key.EXCH_POL_ID").alias("EXCH_POL_ID"),
        F.col("Key.GL_NO").alias("GL_NO"),
        F.rpad(F.col("Key.PROD_ID"), 8, " ").alias("PROD_ID"),
        F.col("Key.PROD_BILL_CMPNT_ID").alias("PROD_BILL_CMPNT_ID"),
        F.col("Key.QHP_ID").alias("QHP_ID")
    )
)

df_defaultNA = (
    df_enriched
    .filter(F.col("row_id") == 1)
    .select(
        F.lit(1).alias("RCVD_INCM_SBSDY_DTL_SK"),
        F.lit(1).alias("CMS_ENR_PAYMT_UNIQ_KEY"),
        F.lit(1).alias("CMS_ENR_PAYMT_AMT_SEQ_NO"),
        F.lit("NA").alias("RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD"),
        F.lit("NA").alias("RCVD_INCM_SBSDY_ACCT_ACTVTY_CD"),
        F.lit(1).alias("SRC_SYS_CD_SK"),
        F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("BILL_ENTY_SK"),
        F.lit(1).alias("CLS_PLN_SK"),
        F.lit(1).alias("FEE_DSCNT_SK"),
        F.lit(1).alias("FNCL_LOB_SK"),
        F.lit(1).alias("PROD_SK"),
        F.lit(1).alias("QHP_SK"),
        F.lit(1).alias("RCVD_INCM_SBSDY_ACCT_ACTVTY_CD_SK"),
        F.lit(1).alias("RCVD_INCM_SBSDY_ACCT_TYP_CD_SK"),
        F.lit(1).alias("RCVD_INCM_SBSDY_ACTV_SRC_CD_SK"),
        F.lit(1).alias("RCVD_INCM_SBSDY_ACTV_TYP_CD_SK"),
        F.lit(1).alias("RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD_SK"),
        F.lit(1).alias("RCVD_INCM_SBSDY_LOB_CD_SK"),
        F.rpad(F.lit("N"), 1, " ").alias("FIRST_YR_IN"),
        F.rpad(F.lit("1753-01-01"), 10, " ").alias("BILL_DUE_DT_SK"),
        F.rpad(F.lit("1753-01-01"), 10, " ").alias("CRT_DT_SK"),
        F.rpad(F.lit("1753-01-01"), 10, " ").alias("POSTED_DT_SK"),
        F.lit(1).alias("ERN_INCM_AMT"),
        F.lit(1).alias("RVNU_RCVD_AMT"),
        F.lit(1).alias("BILL_ENTY_UNIQ_KEY"),
        F.lit(1).alias("BILL_GRP_BILL_ENTY_UNIQ_KEY"),
        F.lit("NA").alias("EXCH_MBR_ID"),
        F.lit("NA").alias("EXCH_SUB_ID"),
        F.lit("NA").alias("EXCH_POL_ID"),
        F.lit("NA").alias("GL_NO"),
        F.rpad(F.lit("NA"), 8, " ").alias("PROD_ID"),
        F.lit("NA").alias("PROD_BILL_CMPNT_ID"),
        F.lit("NA").alias("QHP_ID")
    )
)

df_defaultUNK = (
    df_enriched
    .filter(F.col("row_id") == 1)
    .select(
        F.lit(0).alias("RCVD_INCM_SBSDY_DTL_SK"),
        F.lit(0).alias("CMS_ENR_PAYMT_UNIQ_KEY"),
        F.lit(0).alias("CMS_ENR_PAYMT_AMT_SEQ_NO"),
        F.lit("UNK").alias("RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD"),
        F.lit("UNK").alias("RCVD_INCM_SBSDY_ACCT_ACTVTY_CD"),
        F.lit(0).alias("SRC_SYS_CD_SK"),
        F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("BILL_ENTY_SK"),
        F.lit(0).alias("CLS_PLN_SK"),
        F.lit(0).alias("FEE_DSCNT_SK"),
        F.lit(0).alias("FNCL_LOB_SK"),
        F.lit(0).alias("PROD_SK"),
        F.lit(0).alias("QHP_SK"),
        F.lit(0).alias("RCVD_INCM_SBSDY_ACCT_ACTVTY_CD_SK"),
        F.lit(0).alias("RCVD_INCM_SBSDY_ACCT_TYP_CD_SK"),
        F.lit(0).alias("RCVD_INCM_SBSDY_ACTV_SRC_CD_SK"),
        F.lit(0).alias("RCVD_INCM_SBSDY_ACTV_TYP_CD_SK"),
        F.lit(0).alias("RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD_SK"),
        F.lit(0).alias("RCVD_INCM_SBSDY_LOB_CD_SK"),
        F.rpad(F.lit("N"), 1, " ").alias("FIRST_YR_IN"),
        F.rpad(F.lit("1753-01-01"), 10, " ").alias("BILL_DUE_DT_SK"),
        F.rpad(F.lit("1753-01-01"), 10, " ").alias("CRT_DT_SK"),
        F.rpad(F.lit("1753-01-01"), 10, " ").alias("POSTED_DT_SK"),
        F.lit(0).alias("ERN_INCM_AMT"),
        F.lit(0).alias("RVNU_RCVD_AMT"),
        F.lit(0).alias("BILL_ENTY_UNIQ_KEY"),
        F.lit(0).alias("BILL_GRP_BILL_ENTY_UNIQ_KEY"),
        F.lit("UNK").alias("EXCH_MBR_ID"),
        F.lit("UNK").alias("EXCH_SUB_ID"),
        F.lit("UNK").alias("EXCH_POL_ID"),
        F.lit("UNK").alias("GL_NO"),
        F.rpad(F.lit("UNK"), 8, " ").alias("PROD_ID"),
        F.lit("UNK").alias("PROD_BILL_CMPNT_ID"),
        F.lit("UNK").alias("QHP_ID")
    )
)

df_collector = df_fkey.unionByName(df_defaultNA).unionByName(df_defaultUNK)

final_columns = [
    "RCVD_INCM_SBSDY_DTL_SK",
    "CMS_ENR_PAYMT_UNIQ_KEY",
    "CMS_ENR_PAYMT_AMT_SEQ_NO",
    "RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD",
    "RCVD_INCM_SBSDY_ACCT_ACTVTY_CD",
    "SRC_SYS_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "BILL_ENTY_SK",
    "CLS_PLN_SK",
    "FEE_DSCNT_SK",
    "FNCL_LOB_SK",
    "PROD_SK",
    "QHP_SK",
    "RCVD_INCM_SBSDY_ACCT_ACTVTY_CD_SK",
    "RCVD_INCM_SBSDY_ACCT_TYP_CD_SK",
    "RCVD_INCM_SBSDY_ACTV_SRC_CD_SK",
    "RCVD_INCM_SBSDY_ACTV_TYP_CD_SK",
    "RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD_SK",
    "RCVD_INCM_SBSDY_LOB_CD_SK",
    "FIRST_YR_IN",
    "BILL_DUE_DT_SK",
    "CRT_DT_SK",
    "POSTED_DT_SK",
    "ERN_INCM_AMT",
    "RVNU_RCVD_AMT",
    "BILL_ENTY_UNIQ_KEY",
    "BILL_GRP_BILL_ENTY_UNIQ_KEY",
    "EXCH_MBR_ID",
    "EXCH_SUB_ID",
    "EXCH_POL_ID",
    "GL_NO",
    "PROD_ID",
    "PROD_BILL_CMPNT_ID",
    "QHP_ID"
]

df_final = df_collector.select(final_columns)

write_files(
    df_final,
    f"{adls_path}/load/RCVD_INCM_SBSDY_DTL.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)