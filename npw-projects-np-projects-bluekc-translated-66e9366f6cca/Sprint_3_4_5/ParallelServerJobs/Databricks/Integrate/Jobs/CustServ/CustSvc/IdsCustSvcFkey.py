# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 10/21/08 10:40:52 Batch  14905_38463 PROMOTE bckcetl ids20 dsadm rc for brent  
# MAGIC ^1_1 10/21/08 10:17:42 Batch  14905_37069 INIT bckcett testIDS dsadm rc for brent 
# MAGIC ^1_1 10/20/08 12:51:44 Batch  14904_46312 PROMOTE bckcett testIDS u08717 Brent
# MAGIC ^1_1 10/20/08 10:48:46 Batch  14904_38931 INIT bckcett devlIDS u08717 Brent
# MAGIC ^1_1 08/01/08 10:50:09 Batch  14824_39011 INIT bckcett devlIDS u03651 steffy
# MAGIC ^1_3 07/23/08 15:42:49 Batch  14815_56575 PROMOTE bckcett devlIDS u08717 Brent
# MAGIC ^1_3 07/23/08 15:08:26 Batch  14815_54510 INIT bckcett devlIDScur u08717 Brent
# MAGIC ^1_2 05/12/08 08:17:54 Batch  14743_29878 INIT bckcett devlIDScur u08717 Brent
# MAGIC ^1_2 05/21/07 13:43:57 Batch  14386_49443 PROMOTE bckcetl ids20 dsadm bls for rt
# MAGIC ^1_2 05/21/07 13:37:43 Batch  14386_49068 INIT bckcett testIDS30 dsadm bls for rt
# MAGIC ^1_1 05/21/07 10:17:11 Batch  14386_37038 INIT bckcett testIDS30 dsadm bls for rt
# MAGIC ^1_3 05/08/07 14:17:42 Batch  14373_51469 PROMOTE bckcett testIDS30 u03651 steph for ralph
# MAGIC ^1_3 05/08/07 14:04:19 Batch  14373_50664 INIT bckcett devlIDS30 u03651 steffy
# MAGIC ^1_2 04/06/07 14:05:51 Batch  14341_50754 INIT bckcett devlIDS30 dsadm dsadm
# MAGIC ^1_1 04/05/07 13:31:14 Batch  14340_48677 INIT bckcett devlIDS30 dsadm dsadm
# MAGIC ^1_2 04/04/07 08:38:14 Batch  14339_31097 INIT bckcett devlIDS30 u03651 steffy
# MAGIC ^1_1 03/30/07 10:52:52 Batch  14334_39177 INIT bckcett devlIDS30 u03651 steffy
# MAGIC 
# MAGIC ***************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006, 2007, 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:   Takes the primary key file and applies the foreign keys.   
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer             Date                         Change Description                                                        Project #                   Development Project          Code Reviewer          Date Reviewed  
# MAGIC ------------------           ----------------------------     -----------------------------------------------------------------------                ----------------                 ------------------------------------       ----------------------------      ----------------
# MAGIC Naren Garapaty    11/2006                    Initial program                                                                                                  devlIDS30                          Steph Goddard          02/12/2007
# MAGIC Brent Leland         02/29/2008              Added error recycle                                                       3567 Primary Key       devlIDScur                         Steph Goddard          05/02/2008
# MAGIC                                                               Added source system to load file

# MAGIC Set all foreign surrogate keys
# MAGIC Merge source data with default rows
# MAGIC Writing Sequential File to /load
# MAGIC Read common record format file from extract job.
# MAGIC This hash file is written to by all customer service foriegn key jobs.
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
    StringType,
    IntegerType,
    DecimalType,
    TimestampType
)
from pyspark.sql.functions import col, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


Logging = get_widget_value('Logging','X')
InFile = get_widget_value('InFile','')
RunID = get_widget_value('RunID','2007010101')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
SrcSysCd = get_widget_value('SrcSysCd','')

schema_IdsCustSvcExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38, 10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("CUST_SVC_SK", IntegerType(), nullable=False),
    StructField("CUST_SVC_ID", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("CUST_SVC_CNTCT_RELSHP_CD", StringType(), nullable=False),
    StructField("CUST_SVC_EXCL_CD", StringType(), nullable=False),
    StructField("CUST_SVC_METH_CD", StringType(), nullable=False),
    StructField("CUST_SVC_SATSFCTN_LVL_CD", StringType(), nullable=False),
    StructField("DISCLMR_IN", StringType(), nullable=False),
    StructField("CNTCT_INFO_TX", StringType(), nullable=True),
    StructField("CNTCT_RQST_DESC", StringType(), nullable=True),
    StructField("CSTK_SEQ_NO", IntegerType(), nullable=False)
])

df_IdsCustSvcExtr = (
    spark.read.format("csv")
    .option("sep", ",")
    .option("quote", "\"")
    .schema(schema_IdsCustSvcExtr)
    .load(f"{adls_path}/key/{InFile}")
)

df_foreignKey_stagevars = (
    df_IdsCustSvcExtr
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn(
        "svCntctRelshpCdSk",
        GetFkeyCodes(
            col("SRC_SYS_CD"),
            col("CUST_SVC_SK"),
            lit("CUSTOMER SERVICE CONTACT RELATIONSHIP"),
            col("CUST_SVC_CNTCT_RELSHP_CD"),
            lit(Logging)
        )
    )
    .withColumn(
        "svExclCdSk",
        GetFkeyCodes(
            col("SRC_SYS_CD"),
            col("CUST_SVC_SK"),
            lit("CUSTOMER SERVICE EXCLUSION"),
            col("CUST_SVC_EXCL_CD"),
            lit(Logging)
        )
    )
    .withColumn(
        "svMethCdSk",
        GetFkeyCodes(
            col("SRC_SYS_CD"),
            col("CUST_SVC_SK"),
            lit("CUSTOMER SERVICE METHOD"),
            col("CUST_SVC_METH_CD"),
            lit(Logging)
        )
    )
    .withColumn(
        "svSatsfctnLvlCdSk",
        GetFkeyCodes(
            col("SRC_SYS_CD"),
            col("CUST_SVC_SK"),
            lit("CUSTOMER SERVICE SATISFACTION LEVEL"),
            col("CUST_SVC_SATSFCTN_LVL_CD"),
            lit(Logging)
        )
    )
    .withColumn(
        "ErrCount",
        GetFkeyErrorCnt(col("CUST_SVC_SK"))
    )
)

df_Fkey_pre = df_foreignKey_stagevars.filter(
    (col("ErrCount") == lit(0)) | (col("PassThru") == lit('Y'))
)
df_Fkey = df_Fkey_pre.select(
    col("CUST_SVC_SK").alias("CUST_SVC_SK"),
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("CUST_SVC_ID").alias("CUST_SVC_ID"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("svCntctRelshpCdSk").alias("CUST_SVC_CNTCT_RELSHP_CD_SK"),
    col("svExclCdSk").alias("CUST_SVC_EXCL_CD_SK"),
    col("svMethCdSk").alias("CUST_SVC_METH_CD_SK"),
    col("svSatsfctnLvlCdSk").alias("CUST_SVC_SATSFCTN_LVL_CD_SK"),
    col("DISCLMR_IN").alias("DISCLMR_IN"),
    col("CNTCT_INFO_TX").alias("CNTCT_INFO_TX"),
    col("CNTCT_RQST_DESC").alias("CNTCT_RQST_DESC")
)

df_recycle_pre = df_foreignKey_stagevars.filter(col("ErrCount") > lit(0))
df_recycle_pre = (
    df_recycle_pre
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", GetRecycleKey(col("CUST_SVC_SK")))
    .withColumn("ERR_CT", col("ErrCount"))
    .withColumn("RECYCLE_CT", col("RECYCLE_CT") + lit(1))
    .withColumn("CUST_SVC_CNTCT_RELSHP_CD_SK", col("CUST_SVC_CNTCT_RELSHP_CD"))
    .withColumn("CUST_SVC_EXCL_CD_SK", col("CUST_SVC_EXCL_CD"))
    .withColumn("CUST_SVC_METH_CD_SK", col("CUST_SVC_METH_CD"))
    .withColumn("CUST_SVC_SATSFCTN_LVL_CD_SK", col("CUST_SVC_SATSFCTN_LVL_CD"))
)
df_recycle = df_recycle_pre.select(
    col("JOB_EXCTN_RCRD_ERR_SK"),
    col("INSRT_UPDT_CD"),
    col("DISCARD_IN"),
    col("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    col("SRC_SYS_CD"),
    col("PRI_KEY_STRING"),
    col("CUST_SVC_SK"),
    col("CUST_SVC_ID"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("CUST_SVC_CNTCT_RELSHP_CD_SK"),
    col("CUST_SVC_EXCL_CD_SK"),
    col("CUST_SVC_METH_CD_SK"),
    col("CUST_SVC_SATSFCTN_LVL_CD_SK"),
    col("DISCLMR_IN"),
    col("CNTCT_INFO_TX"),
    col("CNTCT_RQST_DESC")
)

df_recycle = (
    df_recycle
    .withColumn("INSRT_UPDT_CD", rpad("INSRT_UPDT_CD", 10, " "))
    .withColumn("DISCARD_IN", rpad("DISCARD_IN", 1, " "))
    .withColumn("PASS_THRU_IN", rpad("PASS_THRU_IN", 1, " "))
    .withColumn("DISCLMR_IN", rpad("DISCLMR_IN", 1, " "))
)
write_files(
    df_recycle,
    "hf_recycle.parquet",
    ',',
    'overwrite',
    True,
    True,
    '"',
    None
)

df_recycleKeys = df_foreignKey_stagevars.filter(col("ErrCount") > lit(0)).select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("CUST_SVC_ID").alias("CUST_SVC_ID"),
    col("CSTK_SEQ_NO").alias("CSTK_SEQ_NO")
)
write_files(
    df_recycleKeys,
    "hf_custsvc_recycle_keys.parquet",
    ',',
    'overwrite',
    True,
    True,
    '"',
    None
)

count_foreignKey_stagevars = df_foreignKey_stagevars.limit(1).count()

if count_foreignKey_stagevars > 0:
    df_defaultUNK = spark.createDataFrame(
        [
            (
                0, 0, "UNK", 0, 0, 0, 0, 0, 0,
                "U", "UNK", "UNK"
            )
        ],
        [
            "CUST_SVC_SK",
            "SRC_SYS_CD_SK",
            "CUST_SVC_ID",
            "CRT_RUN_CYC_EXCTN_SK",
            "LAST_UPDT_RUN_CYC_EXCTN_SK",
            "CUST_SVC_CNTCT_RELSHP_CD_SK",
            "CUST_SVC_EXCL_CD_SK",
            "CUST_SVC_METH_CD_SK",
            "CUST_SVC_SATSFCTN_LVL_CD_SK",
            "DISCLMR_IN",
            "CNTCT_INFO_TX",
            "CNTCT_RQST_DESC"
        ]
    )
else:
    df_defaultUNK = spark.createDataFrame([], df_Fkey.schema)

if count_foreignKey_stagevars > 0:
    df_defaultNA = spark.createDataFrame(
        [
            (
                1, 1, "NA", 1, 1, 1, 1, 1, 1,
                "X", "NA", "NA"
            )
        ],
        [
            "CUST_SVC_SK",
            "SRC_SYS_CD_SK",
            "CUST_SVC_ID",
            "CRT_RUN_CYC_EXCTN_SK",
            "LAST_UPDT_RUN_CYC_EXCTN_SK",
            "CUST_SVC_CNTCT_RELSHP_CD_SK",
            "CUST_SVC_EXCL_CD_SK",
            "CUST_SVC_METH_CD_SK",
            "CUST_SVC_SATSFCTN_LVL_CD_SK",
            "DISCLMR_IN",
            "CNTCT_INFO_TX",
            "CNTCT_RQST_DESC"
        ]
    )
else:
    df_defaultNA = spark.createDataFrame([], df_Fkey.schema)

df_collector = df_Fkey.unionByName(df_defaultUNK).unionByName(df_defaultNA)

df_collector_final = df_collector.select(
    "CUST_SVC_SK",
    "SRC_SYS_CD_SK",
    "CUST_SVC_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CUST_SVC_CNTCT_RELSHP_CD_SK",
    "CUST_SVC_EXCL_CD_SK",
    "CUST_SVC_METH_CD_SK",
    "CUST_SVC_SATSFCTN_LVL_CD_SK",
    "DISCLMR_IN",
    "CNTCT_INFO_TX",
    "CNTCT_RQST_DESC"
).withColumn("DISCLMR_IN", rpad("DISCLMR_IN", 1, " "))

write_files(
    df_collector_final,
    f"{adls_path}/load/CUST_SVC.{SrcSysCd}.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)