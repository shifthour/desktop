# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_2 03/04/09 10:12:49 Batch  15039_36773 PROMOTE bckcetl ids20 dsadm bls for sa
# MAGIC ^1_2 03/04/09 10:01:12 Batch  15039_36074 INIT bckcett testIDS dsadm BLS FOR SA
# MAGIC ^1_1 01/13/09 09:00:06 Batch  14989_32412 PROMOTE bckcett testIDS u03651 steph for Sharon
# MAGIC ^1_1 01/13/09 08:41:23 Batch  14989_31287 INIT bckcett devlIDS u03651 steffy
# MAGIC ^1_1 01/28/08 09:39:22 Batch  14638_34767 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 12/27/07 09:46:56 Batch  14606_35219 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/24/07 09:35:50 Batch  14573_34558 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 08/27/07 08:55:46 Batch  14484_32151 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_3 03/27/07 13:27:29 Batch  14331_48452 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 03/07/07 16:30:43 Batch  14311_59451 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_2 01/26/06 07:54:36 Batch  13906_28483 INIT bckcetl ids20 dsadm Gina
# MAGIC ^1_1 01/24/06 14:49:28 Batch  13904_53375 PROMOTE bckcetl ids20 dsadm Gina Parr
# MAGIC ^1_1 01/24/06 14:47:17 Batch  13904_53242 INIT bckcett testIDS30 dsadm Gina Parr
# MAGIC ^1_2 01/24/06 14:36:07 Batch  13904_52574 INIT bckcett testIDS30 dsadm Gina Parr
# MAGIC ^1_1 01/24/06 14:21:30 Batch  13904_51695 INIT bckcett testIDS30 dsadm Gina Parr
# MAGIC ^1_2 12/23/05 11:39:15 Batch  13872_41979 PROMOTE bckcett testIDS30 u10913 Move Income Commission to test
# MAGIC ^1_2 12/23/05 11:12:01 Batch  13872_40337 INIT bckcett devlIDS30 u10913 Ollie Move Income/Commisions to test
# MAGIC ^1_1 12/23/05 10:39:46 Batch  13872_38400 INIT bckcett devlIDS30 u10913 Ollie Moving Income / Commission to Test
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsBillIncmRcptFkey
# MAGIC 
# MAGIC DESCRIPTION:     Takes the sorted file and applies the primary key.   Preps the file for the foreign key lookup process that follows.
# MAGIC       
# MAGIC 
# MAGIC INPUTS:  Primary Key output file
# MAGIC 	
# MAGIC   
# MAGIC HASH FILES:     hf_recycle
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  GetFkeyCodes
# MAGIC                             BILL_ENTY_UNIQ_KEY is used to retrieve the foreign key for BILL_ENTY_SK instead of BLEI_CK, because BILL_ENTY_UNIQ_KEY is directly mapped from BLEI_CK.
# MAGIC                             
# MAGIC PROCESSING:  Natural keys are converted to surrogate keys
# MAGIC 
# MAGIC OUTPUTS:   CLS table load file
# MAGIC                      hf_recycle - If recycling was used for this job records would be written to this hash file that had errors.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                    Tao Luo - 10/20/2005 - Originally Programmed
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                 Change Description                                            Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------      ---------------------------------------------------------                      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Bhoomi Dasari                  2008-09-18        3657                               Added SrcSysCdSk parame/3567                          devlIDS                       Steph Goddard            10/03/2008

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
    DecimalType,
    TimestampType
)
from pyspark.sql import functions as F
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


TmpOutFile = get_widget_value("TmpOutFile", "IdsBillIncmRcptExtr.dat.fkey")
InFile = get_widget_value("InFile", "IdsBillIncmRcptExtr.dat.pkey")
Logging = get_widget_value("Logging", "X")
SrcSysCdSk = get_widget_value("SrcSysCdSk", "")

schema_IdsBillIncmRcpt = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38,10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("BILL_INCM_RCPT_SK", IntegerType(), nullable=False),
    StructField("BILL_ENTY_UNIQ_KEY", IntegerType(), nullable=False),
    StructField("BILL_DUE_DT_SK", StringType(), nullable=False),
    StructField("BILL_CNTR_ID", StringType(), nullable=False),
    StructField("CRT_DTM", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("CSPI_ID", StringType(), nullable=False),
    StructField("PMFA_ID", StringType(), nullable=False),
    StructField("BLAC_ACCT_CAT", StringType(), nullable=False),
    StructField("PDPD_ID", StringType(), nullable=False),
    StructField("ACGL_ACTIVITY", StringType(), nullable=False),
    StructField("ACGL_TYPE", StringType(), nullable=False),
    StructField("BLAC_SOURCE", StringType(), nullable=False),
    StructField("BLAC_TYPE", StringType(), nullable=False),
    StructField("LOBD_ID", StringType(), nullable=False),
    StructField("FIRST_YR_IN", StringType(), nullable=False),
    StructField("CRT_DT_SK", StringType(), nullable=False),
    StructField("POSTED_DT_SK", StringType(), nullable=False),
    StructField("ERN_INCM_AMT", DecimalType(38,10), nullable=False),
    StructField("RVNU_RCVD_AMT", DecimalType(38,10), nullable=False),
    StructField("GL_NO", StringType(), nullable=False),
    StructField("PROD_BILL_CMPNT_ID", StringType(), nullable=False),
])

df_key = (
    spark.read
    .option("sep", ",")
    .option("quote", "\"")
    .schema(schema_IdsBillIncmRcpt)
    .csv(f"{adls_path}/key/{InFile}", header=False)
)

df_enriched = (
    df_key
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("svBillEnty", GetFkeyBillEnty(F.col("SRC_SYS_CD"), F.col("BILL_INCM_RCPT_SK"), F.col("BILL_ENTY_UNIQ_KEY"), F.lit(Logging)))
    .withColumn("svClsPln", GetFkeyClsPln(F.col("SRC_SYS_CD"), F.col("BILL_INCM_RCPT_SK"), F.col("CSPI_ID"), F.lit(Logging)))
    .withColumn("svFeeDscnt", GetFkeyFeeDscnt(F.col("SRC_SYS_CD"), F.col("BILL_INCM_RCPT_SK"), F.col("PMFA_ID"), F.lit(Logging)))
    .withColumn("svFnclLob", GetFkeyFnclLob(F.lit("PSI"), F.col("BILL_INCM_RCPT_SK"), F.col("BLAC_ACCT_CAT"), F.lit(Logging)))
    .withColumn("svProd", GetFkeyProd(F.col("SRC_SYS_CD"), F.col("BILL_INCM_RCPT_SK"), F.col("PDPD_ID"), F.lit(Logging)))
    .withColumn("svBillIncmRcptAcctActvCd", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("BILL_INCM_RCPT_SK"), F.lit("BILLING RECEIPT ACCOUNT ACTIVITY"), F.col("ACGL_ACTIVITY"), F.lit(Logging)))
    .withColumn("svBillIncmRcptAcctTypCd", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("BILL_INCM_RCPT_SK"), F.lit("BILLING RECEIPT ACCOUNT TYPE"), F.col("ACGL_TYPE"), F.lit(Logging)))
    .withColumn("svBillIncmRcptActvSrcCd", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("BILL_INCM_RCPT_SK"), F.lit("BILLING RECEIPT ACTIVITY SOURCE"), F.col("BLAC_SOURCE"), F.lit(Logging)))
    .withColumn("svBillIncmRcptActvTypCd", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("BILL_INCM_RCPT_SK"), F.lit("BILLING RECEIPT ACTIVITY TYPE"), F.col("BLAC_TYPE"), F.lit(Logging)))
    .withColumn("svBillIncmRcptLobCd", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("BILL_INCM_RCPT_SK"), F.lit("CLAIM LINE LOB"), F.col("LOBD_ID"), F.lit(Logging)))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("BILL_INCM_RCPT_SK")))
)

df_fkey = (
    df_enriched
    .filter((F.col("ErrCount") == 0) | (F.col("PassThru") == 'Y'))
    .select(
        F.col("BILL_INCM_RCPT_SK").alias("BILL_INCM_RCPT_SK"),
        F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        F.col("BILL_ENTY_UNIQ_KEY").alias("BILL_ENTY_UNIQ_KEY"),
        F.col("BILL_DUE_DT_SK").alias("BILL_DUE_DT_SK"),
        F.col("BILL_CNTR_ID").alias("BILL_CNTR_ID"),
        F.col("CRT_DTM").alias("CRT_DTM"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.when(F.col("svBillEnty").isNull(), F.lit(0)).otherwise(F.col("svBillEnty")).alias("BILL_ENTY_SK"),
        F.when(F.col("svClsPln").isNull(), F.lit(0)).otherwise(F.col("svClsPln")).alias("CLS_PLN_SK"),
        F.when(F.col("svFeeDscnt").isNull(), F.lit(0)).otherwise(F.col("svFeeDscnt")).alias("FEE_DSCNT_SK"),
        F.when(F.col("svFnclLob").isNull(), F.lit(0)).otherwise(F.col("svFnclLob")).alias("FNCL_LOB_SK"),
        F.when(F.col("svProd").isNull(), F.lit(0)).otherwise(F.col("svProd")).alias("PROD_SK"),
        F.when(F.col("svBillIncmRcptAcctActvCd").isNull(), F.lit(0)).otherwise(F.col("svBillIncmRcptAcctActvCd")).alias("BILL_INCM_RCPT_ACCT_ACTV_CD_SK"),
        F.when(F.col("svBillIncmRcptAcctTypCd").isNull(), F.lit(0)).otherwise(F.col("svBillIncmRcptAcctTypCd")).alias("BILL_INCM_RCPT_ACCT_TYP_CD_SK"),
        F.when(F.col("svBillIncmRcptActvSrcCd").isNull(), F.lit(0)).otherwise(F.col("svBillIncmRcptActvSrcCd")).alias("BILL_INCM_RCPT_ACTV_SRC_CD_SK"),
        F.when(F.col("svBillIncmRcptActvTypCd").isNull(), F.lit(0)).otherwise(F.col("svBillIncmRcptActvTypCd")).alias("BILL_INCM_RCPT_ACTV_TYP_CD_SK"),
        F.when(F.col("svBillIncmRcptLobCd").isNull(), F.lit(0)).otherwise(F.col("svBillIncmRcptLobCd")).alias("BILL_INCM_RCPT_LOB_CD_SK"),
        F.col("FIRST_YR_IN").alias("FIRST_YR_IN"),
        F.col("CRT_DT_SK").alias("CRT_DT_SK"),
        F.col("POSTED_DT_SK").alias("POSTED_DT_SK"),
        F.col("ERN_INCM_AMT").alias("ERN_INCM_AMT"),
        F.col("RVNU_RCVD_AMT").alias("RVNU_RCVD_AMT"),
        F.col("GL_NO").alias("GL_NO"),
        F.col("PROD_BILL_CMPNT_ID").alias("PROD_BILL_CMPNT_ID")
    )
)

df_recycle = (
    df_enriched
    .filter(F.col("ErrCount") > 0)
    .select(
        GetRecycleKey(F.col("BILL_INCM_RCPT_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("DISCARD_IN").alias("DISCARD_IN"),
        F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("ERR_CT").alias("ERR_CT"),
        F.col("RECYCLE_CT").alias("RECYCLE_CT"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("BILL_INCM_RCPT_SK").alias("BILL_INCM_RCPT_SK"),
        F.col("BILL_ENTY_UNIQ_KEY").alias("BILL_ENTY_UNIQ_KEY"),
        F.col("BILL_DUE_DT_SK").alias("BILL_DUE_DT_SK"),
        F.col("BILL_CNTR_ID").alias("BILL_CNTR_ID"),
        F.col("CRT_DTM").alias("CRT_DTM"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("CSPI_ID").alias("CSPI_ID"),
        F.col("PMFA_ID").alias("PMFA_ID"),
        F.col("BLAC_ACCT_CAT").alias("BLAC_ACCT_CAT"),
        F.col("PDPD_ID").alias("PDPD_ID"),
        F.col("ACGL_ACTIVITY").alias("ACGL_ACTIVITY"),
        F.col("ACGL_TYPE").alias("ACGL_TYPE"),
        F.col("BLAC_SOURCE").alias("BLAC_SOURCE"),
        F.col("BLAC_TYPE").alias("BLAC_TYPE"),
        F.col("LOBD_ID").alias("LOBD_ID"),
        F.col("FIRST_YR_IN").alias("FIRST_YR_IN"),
        F.col("CRT_DT_SK").alias("CRT_DT_SK"),
        F.col("POSTED_DT_SK").alias("POSTED_DT_SK"),
        F.col("ERN_INCM_AMT").alias("ERN_INCM_AMT"),
        F.col("RVNU_RCVD_AMT").alias("RVNU_RCVD_AMT"),
        F.col("GL_NO").alias("GL_NO"),
        F.col("PROD_BILL_CMPNT_ID").alias("PROD_BILL_CMPNT_ID")
    )
)

w = Window.orderBy(F.lit(1))
df_enriched_rn = df_enriched.withColumn("rownum", F.row_number().over(w))

df_defaultUNK_interim = (
    df_enriched_rn
    .filter(F.col("rownum") == 1)
    .select(
        F.lit(0).alias("BILL_INCM_RCPT_SK"),
        F.lit(0).alias("SRC_SYS_CD_SK"),
        F.lit(0).alias("BILL_ENTY_UNIQ_KEY"),
        F.lit("UNK").alias("BILL_DUE_DT_SK"),
        F.lit("UNK").alias("BILL_CNTR_ID"),
        F.lit("2000-01-01-00.00.00.000000").alias("CRT_DTM"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("BILL_ENTY_SK"),
        F.lit(0).alias("CLS_PLN_SK"),
        F.lit(0).alias("FEE_DSCNT_SK"),
        F.lit(0).alias("FNCL_LOB_SK"),
        F.lit(0).alias("PROD_SK"),
        F.lit(0).alias("BILL_INCM_RCPT_ACCT_ACTV_CD_SK"),
        F.lit(0).alias("BILL_INCM_RCPT_ACCT_TYP_CD_SK"),
        F.lit(0).alias("BILL_INCM_RCPT_ACTV_SRC_CD_SK"),
        F.lit(0).alias("BILL_INCM_RCPT_ACTV_TYP_CD_SK"),
        F.lit(0).alias("BILL_INCM_RCPT_LOB_CD_SK"),
        F.lit("U").alias("FIRST_YR_IN"),
        F.lit("UNK").alias("CRT_DT_SK"),
        F.lit("UNK").alias("POSTED_DT_SK"),
        F.lit(0).alias("ERN_INCM_AMT"),
        F.lit(0).alias("RVNU_RCVD_AMT"),
        F.lit("UNK").alias("GL_NO"),
        F.lit("UNK").alias("PROD_BILL_CMPNT_ID")
    )
)

df_defaultNA_interim = (
    df_enriched_rn
    .filter(F.col("rownum") == 1)
    .select(
        F.lit(1).alias("BILL_INCM_RCPT_SK"),
        F.lit(1).alias("SRC_SYS_CD_SK"),
        F.lit(1).alias("BILL_ENTY_UNIQ_KEY"),
        F.lit("NA").alias("BILL_DUE_DT_SK"),
        F.lit("NA").alias("BILL_CNTR_ID"),
        F.lit("2000-01-01-00.00.00.000000").alias("CRT_DTM"),
        F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("BILL_ENTY_SK"),
        F.lit(1).alias("CLS_PLN_SK"),
        F.lit(1).alias("FEE_DSCNT_SK"),
        F.lit(1).alias("FNCL_LOB_SK"),
        F.lit(1).alias("PROD_SK"),
        F.lit(1).alias("BILL_INCM_RCPT_ACCT_ACTV_CD_SK"),
        F.lit(1).alias("BILL_INCM_RCPT_ACCT_TYP_CD_SK"),
        F.lit(1).alias("BILL_INCM_RCPT_ACTV_SRC_CD_SK"),
        F.lit(1).alias("BILL_INCM_RCPT_ACTV_TYP_CD_SK"),
        F.lit(1).alias("BILL_INCM_RCPT_LOB_CD_SK"),
        F.lit("X").alias("FIRST_YR_IN"),
        F.lit("NA").alias("CRT_DT_SK"),
        F.lit("NA").alias("POSTED_DT_SK"),
        F.lit(0).alias("ERN_INCM_AMT"),
        F.lit(0).alias("RVNU_RCVD_AMT"),
        F.lit("NA").alias("GL_NO"),
        F.lit("NA").alias("PROD_BILL_CMPNT_ID")
    )
)

# Union all three inputs to Collector
df_collector = df_fkey.unionByName(df_defaultUNK_interim).unionByName(df_defaultNA_interim)

# Now apply rpad for columns that are declared char(...) in the final set going to "LoadFile"
df_collector_final = (
    df_collector
    .withColumn("BILL_DUE_DT_SK", F.rpad(F.col("BILL_DUE_DT_SK"), 10, " "))
    .withColumn("CRT_DTM", F.rpad(F.col("CRT_DTM"), 23, " "))
    .withColumn("FIRST_YR_IN", F.rpad(F.col("FIRST_YR_IN"), 1, " "))
    .withColumn("CRT_DT_SK", F.rpad(F.col("CRT_DT_SK"), 10, " "))
    .withColumn("POSTED_DT_SK", F.rpad(F.col("POSTED_DT_SK"), 10, " "))
    .withColumn("PROD_BILL_CMPNT_ID", F.rpad(F.col("PROD_BILL_CMPNT_ID"), 10, " "))
    .select(
        "BILL_INCM_RCPT_SK",
        "SRC_SYS_CD_SK",
        "BILL_ENTY_UNIQ_KEY",
        "BILL_DUE_DT_SK",
        "BILL_CNTR_ID",
        "CRT_DTM",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "BILL_ENTY_SK",
        "CLS_PLN_SK",
        "FEE_DSCNT_SK",
        "FNCL_LOB_SK",
        "PROD_SK",
        "BILL_INCM_RCPT_ACCT_ACTV_CD_SK",
        "BILL_INCM_RCPT_ACCT_TYP_CD_SK",
        "BILL_INCM_RCPT_ACTV_SRC_CD_SK",
        "BILL_INCM_RCPT_ACTV_TYP_CD_SK",
        "BILL_INCM_RCPT_LOB_CD_SK",
        "FIRST_YR_IN",
        "CRT_DT_SK",
        "POSTED_DT_SK",
        "ERN_INCM_AMT",
        "RVNU_RCVD_AMT",
        "GL_NO",
        "PROD_BILL_CMPNT_ID"
    )
)

# Write final collector output to sequential file
write_files(
    df_collector_final,
    f"{adls_path}/load/{TmpOutFile}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Apply rpad for hashed-file columns that are char(...). Then write as parquet.
df_recycle_final = (
    df_recycle
    .withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " "))
    .withColumn("BILL_DUE_DT_SK", F.rpad(F.col("BILL_DUE_DT_SK"), 10, " "))
    .withColumn("CRT_DTM", F.rpad(F.col("CRT_DTM"), 23, " "))
    .withColumn("CSPI_ID", F.rpad(F.col("CSPI_ID"), 8, " "))
    .withColumn("PMFA_ID", F.rpad(F.col("PMFA_ID"), 2, " "))
    .withColumn("BLAC_ACCT_CAT", F.rpad(F.col("BLAC_ACCT_CAT"), 4, " "))
    .withColumn("PDPD_ID", F.rpad(F.col("PDPD_ID"), 8, " "))
    .withColumn("ACGL_ACTIVITY", F.rpad(F.col("ACGL_ACTIVITY"), 1, " "))
    .withColumn("ACGL_TYPE", F.rpad(F.col("ACGL_TYPE"), 1, " "))
    .withColumn("BLAC_SOURCE", F.rpad(F.col("BLAC_SOURCE"), 1, " "))
    .withColumn("BLAC_TYPE", F.rpad(F.col("BLAC_TYPE"), 1, " "))
    .withColumn("LOBD_ID", F.rpad(F.col("LOBD_ID"), 4, " "))
    .withColumn("FIRST_YR_IN", F.rpad(F.col("FIRST_YR_IN"), 1, " "))
    .withColumn("CRT_DT_SK", F.rpad(F.col("CRT_DT_SK"), 10, " "))
    .withColumn("POSTED_DT_SK", F.rpad(F.col("POSTED_DT_SK"), 10, " "))
    .select(
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "PASS_THRU_IN",
        "FIRST_RECYC_DT",
        "ERR_CT",
        "RECYCLE_CT",
        "SRC_SYS_CD",
        "PRI_KEY_STRING",
        "BILL_INCM_RCPT_SK",
        "BILL_ENTY_UNIQ_KEY",
        "BILL_DUE_DT_SK",
        "BILL_CNTR_ID",
        "CRT_DTM",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CSPI_ID",
        "PMFA_ID",
        "BLAC_ACCT_CAT",
        "PDPD_ID",
        "ACGL_ACTIVITY",
        "ACGL_TYPE",
        "BLAC_SOURCE",
        "BLAC_TYPE",
        "LOBD_ID",
        "FIRST_YR_IN",
        "CRT_DT_SK",
        "POSTED_DT_SK",
        "ERN_INCM_AMT",
        "RVNU_RCVD_AMT",
        "GL_NO",
        "PROD_BILL_CMPNT_ID"
    )
)

write_files(
    df_recycle_final,
    f"{adls_path}/hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_parquet=True,
    header=True,
    quote="\"",
    nullValue=None
)