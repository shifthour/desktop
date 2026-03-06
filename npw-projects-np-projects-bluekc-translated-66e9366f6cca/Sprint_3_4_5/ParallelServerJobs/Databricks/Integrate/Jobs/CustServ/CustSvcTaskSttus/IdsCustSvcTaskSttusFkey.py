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
# MAGIC COPYRIGHT 2007, 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer             Date                         Change Description                                                        Project #                   Development Project          Code Reviewer          Date Reviewed  
# MAGIC ------------------           ----------------------------     -----------------------------------------------------------------------                ----------------                 ------------------------------------       ----------------------------      ----------------
# MAGIC Parikshith Chada  01/31/2007              Originally Programmed                                                   3028                           devlIDS30                          Steph Goddard            02/21/2007
# MAGIC Brent Leland         02/29/2008              Added error recycle                                                       3567 Primary Key       devlIDScur                         Steph Goddard            05/06/2008
# MAGIC                                                               Added source system to load file name
# MAGIC Kalyan Neelam     2010-01-29               Added new field RTE_TO_GRP_ID                               TTR - 604                IntegrateWrhsDevl              Steph Goddard            01/30/2010

# MAGIC Read common record format file from extract job.
# MAGIC Set all foreign surragote keys
# MAGIC Merge source data with default rows
# MAGIC Writing Sequential File to /load
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
    IntegerType,
    StringType,
    TimestampType,
    DecimalType
)
from pyspark.sql.functions import col, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


Logging = get_widget_value("Logging","Y")
InFile = get_widget_value("InFile","IdsCustSvcTaskSttusExtr.CSTaskSttus.dat")
RunID = get_widget_value("RunID","20061108856")
SrcSysCdSk = get_widget_value("SrcSysCdSk","")
SrcSysCd = get_widget_value("SrcSysCd","")

schema_IdsCustSvcTaskSttusFkey = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(10,0), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("CUST_SVC_TASK_STTUS_SK", IntegerType(), nullable=False),
    StructField("CUST_SVC_ID", StringType(), nullable=False),
    StructField("TASK_SEQ_NO", IntegerType(), nullable=False),
    StructField("STTUS_SEQ_NO", IntegerType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("CRT_BY_USER", StringType(), nullable=False),
    StructField("CUST_SVC_TASK_SK", IntegerType(), nullable=False),
    StructField("RTE_TO_USER", StringType(), nullable=False),
    StructField("CUST_SVC_TASK_STTUS_CD", StringType(), nullable=False),
    StructField("CUST_SVC_TASK_STTUS_RSN_CD", StringType(), nullable=False),
    StructField("STTUS_DTM", TimestampType(), nullable=False),
    StructField("RTE_TO_GRP_ID", StringType(), nullable=False)
])

df_IdsCustSvcTaskSttusFkey = (
    spark.read
    .option("delimiter", ",")
    .option("quote", "\"")
    .schema(schema_IdsCustSvcTaskSttusFkey)
    .csv(f"{adls_path}/key/{InFile}")
)

df_intermediate = (
    df_IdsCustSvcTaskSttusFkey
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn("svCrtByUserSk", GetFkeyAppUsr(col("SRC_SYS_CD"), col("CUST_SVC_TASK_STTUS_SK"), col("CRT_BY_USER"), Logging))
    .withColumn("svRteToUserSk", GetFkeyAppUsr(col("SRC_SYS_CD"), col("CUST_SVC_TASK_STTUS_SK"), col("RTE_TO_USER"), Logging))
    .withColumn("svCustSvcTaskSttusCdSk", GetFkeyCodes(col("SRC_SYS_CD"), col("CUST_SVC_TASK_STTUS_SK"), lit("CUSTOMER SERVICE TASK STATUS"), col("CUST_SVC_TASK_STTUS_CD"), Logging))
    .withColumn("svCustSvcTaskSttusRsnCdSk", GetFkeyCodes(col("SRC_SYS_CD"), col("CUST_SVC_TASK_STTUS_SK"), lit("CUSTOMER SERVICE TASK ROUTE REASON"), col("CUST_SVC_TASK_STTUS_RSN_CD"), Logging))
    .withColumn("svCustSvcTaskSk", GetFkeyCustSvcTask(col("SRC_SYS_CD"), col("CUST_SVC_TASK_STTUS_SK"), col("CUST_SVC_ID"), col("TASK_SEQ_NO"), Logging))
    .withColumn("ErrCount", GetFkeyErrorCnt(col("CUST_SVC_TASK_STTUS_SK")))
)

df_CustSvcTaskSttus = (
    df_intermediate
    .filter((col("ErrCount") == 0) | (col("PassThru") == "Y"))
    .select(
        col("CUST_SVC_TASK_STTUS_SK").alias("CUST_SVC_TASK_STTUS_SK"),
        lit(SrcSysCdSk).cast(IntegerType()).alias("SRC_SYS_CD_SK"),
        rpad(col("CUST_SVC_ID"), 50, " ").alias("CUST_SVC_ID"),
        col("TASK_SEQ_NO").alias("TASK_SEQ_NO"),
        col("STTUS_SEQ_NO").alias("STTUS_SEQ_NO"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("svCrtByUserSk").cast(IntegerType()).alias("CRT_BY_USER_SK"),
        col("svCustSvcTaskSk").alias("CUST_SVC_TASK_SK"),
        col("svRteToUserSk").cast(IntegerType()).alias("RTE_TO_USER_SK"),
        col("svCustSvcTaskSttusCdSk").cast(IntegerType()).alias("CUST_SVC_TASK_STTUS_CD_SK"),
        col("svCustSvcTaskSttusRsnCdSk").cast(IntegerType()).alias("CUST_SVC_TASK_STTUS_RSN_CD_SK"),
        col("STTUS_DTM").alias("STTUS_DTM"),
        rpad(col("RTE_TO_GRP_ID"), 50, " ").alias("RTE_TO_GRP_ID")
    )
)

df_lnkRecycle_pre = df_intermediate.filter(col("ErrCount") > 0)
df_lnkRecycle = (
    df_lnkRecycle_pre
    .select(
        GetRecycleKey(col("CUST_SVC_TASK_STTUS_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        rpad(col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
        rpad(col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
        rpad(col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
        col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        col("ErrCount").alias("ERR_CT"),
        (col("RECYCLE_CT") + lit(1)).alias("RECYCLE_CT"),
        rpad(col("SRC_SYS_CD"), 50, " ").alias("SRC_SYS_CD"),
        rpad(col("PRI_KEY_STRING"), 50, " ").alias("PRI_KEY_STRING"),
        col("CUST_SVC_TASK_STTUS_SK").alias("CUST_SVC_TASK_STTUS_SK"),
        rpad(col("CUST_SVC_ID"), 50, " ").alias("CUST_SVC_ID"),
        col("TASK_SEQ_NO").alias("TASK_SEQ_NO"),
        col("STTUS_SEQ_NO").alias("STTUS_SEQ_NO"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        rpad(col("CRT_BY_USER"), 50, " ").alias("CRT_BY_USER_SK"),
        col("CUST_SVC_TASK_SK").alias("CUST_SVC_TASK_SK"),
        rpad(col("RTE_TO_USER"), 50, " ").alias("RTE_TO_USER_SK"),
        rpad(col("CUST_SVC_TASK_STTUS_CD"), 50, " ").alias("CUST_SVC_TASK_STTUS_CD_SK"),
        rpad(col("CUST_SVC_TASK_STTUS_RSN_CD"), 50, " ").alias("CUST_SVC_TASK_STTUS_RSN_CD_SK"),
        col("STTUS_DTM").alias("STTUS_DTM"),
        rpad(col("RTE_TO_GRP_ID"), 50, " ").alias("RTE_TO_GRP_ID")
    )
)

df_Recycle_Keys = (
    df_lnkRecycle_pre
    .select(
        rpad(col("SRC_SYS_CD"), 50, " ").alias("SRC_SYS_CD"),
        rpad(col("CUST_SVC_ID"), 50, " ").alias("CUST_SVC_ID"),
        col("TASK_SEQ_NO").alias("TASK_SEQ_NO")
    )
)

df_DefaultUNK = (
    df_intermediate
    .limit(1)
    .select(
        lit(1).cast(IntegerType()).alias("CUST_SVC_TASK_STTUS_SK"),
        lit(1).cast(IntegerType()).alias("SRC_SYS_CD_SK"),
        rpad(lit("UNK"), 50, " ").alias("CUST_SVC_ID"),
        lit(1).cast(IntegerType()).alias("TASK_SEQ_NO"),
        lit(1).cast(IntegerType()).alias("STTUS_SEQ_NO"),
        lit(1).cast(IntegerType()).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(1).cast(IntegerType()).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(1).cast(IntegerType()).alias("CRT_BY_USER_SK"),
        lit(1).cast(IntegerType()).alias("CUST_SVC_TASK_SK"),
        lit(1).cast(IntegerType()).alias("RTE_TO_USER_SK"),
        lit(1).cast(IntegerType()).alias("CUST_SVC_TASK_STTUS_CD_SK"),
        lit(1).cast(IntegerType()).alias("CUST_SVC_TASK_STTUS_RSN_CD_SK"),
        lit("1753-01-01 00:00:00").cast(TimestampType()).alias("STTUS_DTM"),
        rpad(lit("1"), 50, " ").alias("RTE_TO_GRP_ID")
    )
)

df_DefaultNA = (
    df_intermediate
    .limit(1)
    .select(
        lit(0).cast(IntegerType()).alias("CUST_SVC_TASK_STTUS_SK"),
        lit(0).cast(IntegerType()).alias("SRC_SYS_CD_SK"),
        rpad(lit("NA"), 50, " ").alias("CUST_SVC_ID"),
        lit(0).cast(IntegerType()).alias("TASK_SEQ_NO"),
        lit(0).cast(IntegerType()).alias("STTUS_SEQ_NO"),
        lit(0).cast(IntegerType()).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(0).cast(IntegerType()).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(0).cast(IntegerType()).alias("CRT_BY_USER_SK"),
        lit(0).cast(IntegerType()).alias("CUST_SVC_TASK_SK"),
        lit(0).cast(IntegerType()).alias("RTE_TO_USER_SK"),
        lit(0).cast(IntegerType()).alias("CUST_SVC_TASK_STTUS_CD_SK"),
        lit(0).cast(IntegerType()).alias("CUST_SVC_TASK_STTUS_RSN_CD_SK"),
        lit("1753-01-01 00:00:00").cast(TimestampType()).alias("STTUS_DTM"),
        rpad(lit("0"), 50, " ").alias("RTE_TO_GRP_ID")
    )
)

df_Collector = (
    df_CustSvcTaskSttus.select(
        col("CUST_SVC_TASK_STTUS_SK").cast(IntegerType()),
        col("SRC_SYS_CD_SK").cast(IntegerType()),
        col("CUST_SVC_ID").cast(StringType()),
        col("TASK_SEQ_NO").cast(IntegerType()),
        col("STTUS_SEQ_NO").cast(IntegerType()),
        col("CRT_RUN_CYC_EXCTN_SK").cast(IntegerType()),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").cast(IntegerType()),
        col("CRT_BY_USER_SK").cast(IntegerType()),
        col("CUST_SVC_TASK_SK").cast(IntegerType()),
        col("RTE_TO_USER_SK").cast(IntegerType()),
        col("CUST_SVC_TASK_STTUS_CD_SK").cast(IntegerType()),
        col("CUST_SVC_TASK_STTUS_RSN_CD_SK").cast(IntegerType()),
        col("STTUS_DTM").cast(TimestampType()),
        col("RTE_TO_GRP_ID").cast(StringType())
    )
    .union(
        df_DefaultUNK.select(
            col("CUST_SVC_TASK_STTUS_SK").cast(IntegerType()),
            col("SRC_SYS_CD_SK").cast(IntegerType()),
            col("CUST_SVC_ID").cast(StringType()),
            col("TASK_SEQ_NO").cast(IntegerType()),
            col("STTUS_SEQ_NO").cast(IntegerType()),
            col("CRT_RUN_CYC_EXCTN_SK").cast(IntegerType()),
            col("LAST_UPDT_RUN_CYC_EXCTN_SK").cast(IntegerType()),
            col("CRT_BY_USER_SK").cast(IntegerType()),
            col("CUST_SVC_TASK_SK").cast(IntegerType()),
            col("RTE_TO_USER_SK").cast(IntegerType()),
            col("CUST_SVC_TASK_STTUS_CD_SK").cast(IntegerType()),
            col("CUST_SVC_TASK_STTUS_RSN_CD_SK").cast(IntegerType()),
            col("STTUS_DTM").cast(TimestampType()),
            col("RTE_TO_GRP_ID").cast(StringType())
        )
    )
    .union(
        df_DefaultNA.select(
            col("CUST_SVC_TASK_STTUS_SK").cast(IntegerType()),
            col("SRC_SYS_CD_SK").cast(IntegerType()),
            col("CUST_SVC_ID").cast(StringType()),
            col("TASK_SEQ_NO").cast(IntegerType()),
            col("STTUS_SEQ_NO").cast(IntegerType()),
            col("CRT_RUN_CYC_EXCTN_SK").cast(IntegerType()),
            col("LAST_UPDT_RUN_CYC_EXCTN_SK").cast(IntegerType()),
            col("CRT_BY_USER_SK").cast(IntegerType()),
            col("CUST_SVC_TASK_SK").cast(IntegerType()),
            col("RTE_TO_USER_SK").cast(IntegerType()),
            col("CUST_SVC_TASK_STTUS_CD_SK").cast(IntegerType()),
            col("CUST_SVC_TASK_STTUS_RSN_CD_SK").cast(IntegerType()),
            col("STTUS_DTM").cast(TimestampType()),
            col("RTE_TO_GRP_ID").cast(StringType())
        )
    )
)

write_files(
    df_Collector.select(
        "CUST_SVC_TASK_STTUS_SK",
        "SRC_SYS_CD_SK",
        "CUST_SVC_ID",
        "TASK_SEQ_NO",
        "STTUS_SEQ_NO",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CRT_BY_USER_SK",
        "CUST_SVC_TASK_SK",
        "RTE_TO_USER_SK",
        "CUST_SVC_TASK_STTUS_CD_SK",
        "CUST_SVC_TASK_STTUS_RSN_CD_SK",
        "STTUS_DTM",
        "RTE_TO_GRP_ID"
    ),
    f"{adls_path}/load/CUST_SVC_TASK_STTUS.{SrcSysCd}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

write_files(
    df_lnkRecycle.select(
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "PASS_THRU_IN",
        "FIRST_RECYC_DT",
        "ERR_CT",
        "RECYCLE_CT",
        "SRC_SYS_CD",
        "PRI_KEY_STRING",
        "CUST_SVC_TASK_STTUS_SK",
        "CUST_SVC_ID",
        "TASK_SEQ_NO",
        "STTUS_SEQ_NO",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CRT_BY_USER_SK",
        "CUST_SVC_TASK_SK",
        "RTE_TO_USER_SK",
        "CUST_SVC_TASK_STTUS_CD_SK",
        "CUST_SVC_TASK_STTUS_RSN_CD_SK",
        "STTUS_DTM",
        "RTE_TO_GRP_ID"
    ),
    "hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

write_files(
    df_Recycle_Keys.select(
        "SRC_SYS_CD",
        "CUST_SVC_ID",
        "TASK_SEQ_NO"
    ),
    "hf_custsvc_recycle_keys.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)