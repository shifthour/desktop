# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 11/12/08 10:10:08 Batch  14927_36617 PROMOTE bckcetl ids20 dsadm rc for brent 
# MAGIC ^1_1 11/12/08 10:04:14 Batch  14927_36261 INIT bckcett testIDS dsadm rc for brent 
# MAGIC ^1_1 11/06/08 08:57:33 Batch  14921_32258 PROMOTE bckcett testIDS u03651 steph for Brent
# MAGIC ^1_1 11/06/08 08:52:23 Batch  14921_31946 INIT bckcett devlIDS u03651 steffy
# MAGIC ^1_1 04/04/07 10:04:56 Batch  14339_36302 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 01/26/06 07:54:36 Batch  13906_28483 INIT bckcetl ids20 dsadm Gina
# MAGIC ^1_2 07/29/05 14:39:17 Batch  13725_52764 INIT bckcetl ids20 dsadm Brent
# MAGIC ^1_1 07/29/05 13:02:36 Batch  13725_46962 INIT bckcetl ids20 dsadm Brent
# MAGIC ^1_1 07/20/05 07:35:13 Batch  13716_27318 PROMOTE bckcetl ids20 dsadm Gina Parr
# MAGIC ^1_1 07/20/05 07:29:40 Batch  13716_26986 INIT bckcett testIDS30 dsadm Gina Parr
# MAGIC ^1_6 06/22/05 09:10:14 Batch  13688_33020 PROMOTE bckcett VERSION u06640 Ralph
# MAGIC ^1_6 06/22/05 09:08:48 Batch  13688_32937 INIT bckcett devlIDS30 u06640 Ralph
# MAGIC ^1_5 06/20/05 13:59:07 Batch  13686_50351 INIT bckcett devlIDS30 u06640 Ralph
# MAGIC ^1_4 06/17/05 10:29:55 Batch  13683_37806 INIT bckcett devlIDS30 u06640 Ralph
# MAGIC ^1_3 06/17/05 10:28:29 Batch  13683_37715 INIT bckcett devlIDS30 u06640 Ralph
# MAGIC ^1_2 06/15/05 08:54:19 Batch  13681_32063 INIT bckcett devlIDS30 u06640 Ralph
# MAGIC ^1_1 06/09/05 13:18:55 Batch  13675_47941 INIT bckcett devlIDS30 u06640 Ralph
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsCapFundFkey
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
# MAGIC                             GetFkeyErrorCnt
# MAGIC 
# MAGIC PROCESSING:  Natural keys are converted to surrogate keys
# MAGIC 
# MAGIC OUTPUTS:   CAP_FUND table load file
# MAGIC                      hf_recycle - If recycling was used for this job records would be written to this hash file that had errors.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC             Ralph Tucker  -  4/22/2005  -  Originally programmed
# MAGIC 
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parik                               2008-09-05       3567(Primary Key)         Added Source System Code SK as parameter      devlIDS                         Steph Goddard             09/10/2008
# MAGIC Ralph Tucker                 2011-05-26       TTR-1058                     Changed Run cycle on UNK & NA rows                IntegrateCurDevl 
# MAGIC 
# MAGIC sHARON ANDREW        2021-02-01       MA LHO Lumeris Capitation   changed tthe source system code value           IntegrateDev2         Kalyan Neelam             2021-02-25
# MAGIC                                                                                                               in the fkey loookups  to be flexible and allow 
# MAGIC                                                                                                                   for either Facets, BCBSA, or Lumeris

# MAGIC Set all foreign surragote keys
# MAGIC Merge source data with default rows
# MAGIC Writing Sequential File to /load
# MAGIC Read common record format file from Primary Key job.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
from pyspark.sql.functions import col, lit, when, upper, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------

TmpOutFile = get_widget_value('TmpOutFile','CAP_FUND.dat')
InFile = get_widget_value('InFile','FctsCapFundExtr.CapFund.dat.20080905')
Logging = get_widget_value('Logging','Y')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
CurrRunCyc = get_widget_value('CurrRunCyc','100')

schemaCapFundCrf = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38,10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("CAP_FUND_SK", IntegerType(), False),
    StructField("CAP_FUND_ID", StringType(), False),
    StructField("CRT_RUN_CYC_EXTCN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXTCN_SK", IntegerType(), False),
    StructField("CAP_FUND_ACCTG_CAT_CD", StringType(), False),
    StructField("CAP_FUND_PAYMT_METH_CD", StringType(), False),
    StructField("CAP_FUND_PRORT_RULE_CD", StringType(), False),
    StructField("CAP_FUND_RATE_CD", StringType(), False),
    StructField("GRP_CAP_MOD_APLD_IN", StringType(), False),
    StructField("MAX_AMT", DecimalType(38,10), False),
    StructField("MIN_AMT", DecimalType(38,10), False),
    StructField("FUND_DESC", StringType(), False)
])

df_CapFundCrf = (
    spark.read
    .option("sep", ",")
    .option("quote", "\"")
    .schema(schemaCapFundCrf)
    .csv(f"{adls_path}/key/{InFile}", header=False)
)

df_purgeTrn = (
    df_CapFundCrf
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn(
        "AcctgCatCd",
        GetFkeyCodes(
            col("SRC_SYS_CD"),
            col("CAP_FUND_SK"),
            lit("CAPITATION FUND ACCOUNTING CATEGORY"),
            col("CAP_FUND_ACCTG_CAT_CD"),
            Logging
        )
    )
    .withColumn(
        "PaymtMethCd",
        GetFkeyCodes(
            col("SRC_SYS_CD"),
            col("CAP_FUND_SK"),
            lit("CAPITATION FUND PAYMENT METHOD"),
            col("CAP_FUND_PAYMT_METH_CD"),
            Logging
        )
    )
    .withColumn(
        "PrortRuleCd",
        GetFkeyCodes(
            col("SRC_SYS_CD"),
            col("CAP_FUND_SK"),
            lit("CAPITATION FUND PRORATE RULE"),
            col("CAP_FUND_PRORT_RULE_CD"),
            Logging
        )
    )
    .withColumn(
        "FundRateCd",
        GetFkeyCodes(
            col("SRC_SYS_CD"),
            col("CAP_FUND_SK"),
            lit("CAPITATION FUND RATE"),
            col("CAP_FUND_RATE_CD"),
            Logging
        )
    )
    .withColumn(
        "ErrCount",
        GetFkeyErrorCnt(col("CAP_FUND_SK"))
    )
)

df_ClsFkeyOut = (
    df_purgeTrn
    .filter((col("ErrCount") == lit(0)) | (col("PassThru") == lit("Y")))
    .select(
        col("CAP_FUND_SK").alias("CAP_FUND_SK"),
        lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        col("CAP_FUND_ID").alias("CAP_FUND_ID"),
        col("CRT_RUN_CYC_EXTCN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXTCN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("AcctgCatCd").alias("CAP_FUND_ACCTG_CAT_CD_SK"),
        col("PaymtMethCd").alias("CAP_FUND_PAYMT_METH_CD_SK"),
        col("PrortRuleCd").alias("CAP_FUND_PRORT_RULE_CD_SK"),
        col("FundRateCd").alias("CAP_FUND_RATE_CD_SK"),
        when(
            col("GRP_CAP_MOD_APLD_IN").isNull() | (trim(col("GRP_CAP_MOD_APLD_IN")) == ''),
            lit("U")
        ).otherwise(
            upper(trim(col("GRP_CAP_MOD_APLD_IN")))
        ).alias("GRP_CAP_MOD_APLD_IN"),
        col("MAX_AMT").alias("MAX_AMT"),
        col("MIN_AMT").alias("MIN_AMT"),
        col("FUND_DESC").alias("FUND_DESC")
    )
)

df_lnkRecycle = (
    df_purgeTrn
    .filter(col("ErrCount") > lit(0))
    .select(
        GetRecycleKey(col("CAP_FUND_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        col("DISCARD_IN").alias("DISCARD_IN"),
        col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        col("ErrCount").alias("ERR_CT"),
        (col("RECYCLE_CT") + lit(1)).alias("RECYCLE_CT"),
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        col("CAP_FUND_SK").alias("CAP_FUND_SK"),
        col("CAP_FUND_ID").alias("CAP_FUND_ID"),
        col("CRT_RUN_CYC_EXTCN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXTCN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("CAP_FUND_ACCTG_CAT_CD").alias("CAP_FUND_ACCTG_CAT_CD"),
        col("CAP_FUND_PAYMT_METH_CD").alias("CAP_FUND_PAYMT_METH_CD"),
        col("CAP_FUND_PRORT_RULE_CD").alias("CAP_FUND_PRORT_RULE_CD"),
        col("CAP_FUND_RATE_CD").alias("CAP_FUND_RATE_CD"),
        col("GRP_CAP_MOD_APLD_IN").alias("GRP_CAP_MOD_APLD_IN"),
        col("MAX_AMT").alias("MAX_AMT"),
        col("MIN_AMT").alias("MIN_AMT"),
        col("FUND_DESC").alias("FUND_DESC")
    )
)

write_files(
    df_lnkRecycle,
    "hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_DefaultUNK = (
    df_purgeTrn
    .limit(1)
    .select(
        lit(0).alias("CAP_FUND_SK"),
        lit(0).alias("SRC_SYS_CD_SK"),
        lit("UNK").alias("CAP_FUND_ID"),
        col("CRT_RUN_CYC_EXTCN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXTCN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(0).alias("CAP_FUND_ACCTG_CAT_CD_SK"),
        lit(0).alias("CAP_FUND_PAYMT_METH_CD_SK"),
        lit(0).alias("CAP_FUND_PRORT_RULE_CD_SK"),
        lit(0).alias("CAP_FUND_RATE_CD_SK"),
        lit("U").alias("GRP_CAP_MOD_APLD_IN"),
        lit(0).alias("MAX_AMT"),
        lit(0).alias("MIN_AMT"),
        lit("UNK").alias("FUND_DESC")
    )
)

df_DefaultNA = (
    df_purgeTrn
    .limit(1)
    .select(
        lit(1).alias("CAP_FUND_SK"),
        lit(1).alias("SRC_SYS_CD_SK"),
        lit("NA").alias("CAP_FUND_ID"),
        col("CRT_RUN_CYC_EXTCN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXTCN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(1).alias("CAP_FUND_ACCTG_CAT_CD_SK"),
        lit(1).alias("CAP_FUND_PAYMT_METH_CD_SK"),
        lit(1).alias("CAP_FUND_PRORT_RULE_CD_SK"),
        lit(1).alias("CAP_FUND_RATE_CD_SK"),
        lit("X").alias("GRP_CAP_MOD_APLD_IN"),
        lit(0).alias("MAX_AMT"),
        lit(0).alias("MIN_AMT"),
        lit("NA").alias("FUND_DESC")
    )
)

columns_Collector = [
    "CAP_FUND_SK",
    "SRC_SYS_CD_SK",
    "CAP_FUND_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CAP_FUND_ACCTG_CAT_CD_SK",
    "CAP_FUND_PAYMT_METH_CD_SK",
    "CAP_FUND_PRORT_RULE_CD_SK",
    "CAP_FUND_RATE_CD_SK",
    "GRP_CAP_MOD_APLD_IN",
    "MAX_AMT",
    "MIN_AMT",
    "FUND_DESC"
]

df_Collector = (
    df_ClsFkeyOut.select(columns_Collector)
    .unionByName(df_DefaultUNK.select(columns_Collector))
    .unionByName(df_DefaultNA.select(columns_Collector))
)

df_Collector_final = (
    df_Collector
    .withColumn("GRP_CAP_MOD_APLD_IN", rpad(col("GRP_CAP_MOD_APLD_IN"), 1, " "))
    .withColumn("FUND_DESC", rpad(col("FUND_DESC"), 70, " "))
)

write_files(
    df_Collector_final.select(columns_Collector),
    f"{adls_path}/load/{TmpOutFile}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)