# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 10/21/08 10:40:52 Batch  14905_38463 PROMOTE bckcetl ids20 dsadm rc for brent  
# MAGIC ^1_1 10/21/08 10:17:42 Batch  14905_37069 INIT bckcett testIDS dsadm rc for brent 
# MAGIC ^1_1 10/20/08 12:51:44 Batch  14904_46312 PROMOTE bckcett testIDS u08717 Brent
# MAGIC ^1_1 10/20/08 10:48:46 Batch  14904_38931 INIT bckcett devlIDS u08717 Brent
# MAGIC ^1_1 08/01/08 11:02:29 Batch  14824_39756 PROMOTE bckcett testIDS u03651 steph for Brent
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
# MAGIC 
# MAGIC ***************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006, 2007, 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  Takes the sorted file and applies the primary key.   Preps the file for the foreign key lookup process that follows.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer             Date                         Change Description                                                        Project #                   Development Project          Code Reviewer          Date Reviewed  
# MAGIC ------------------           ----------------------------     -----------------------------------------------------------------------                ----------------                 ------------------------------------       ----------------------------      ----------------
# MAGIC Naren Garapaty    11/2006                    Initial program                                                                                                  devlIDS30                          Steph Goddard          02/12/2007
# MAGIC Brent Leland         02/29/2008              Added error recycle                                                       3567 Primary Key       devlIDScur                         Steph Goddard          05/06/2008
# MAGIC                                                               Added source system to load file name

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
import pyspark.sql.types as T
from pyspark.sql.functions import col, lit, row_number, rpad
from pyspark.sql import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


Logging = get_widget_value("Logging","'X'")
InFile = get_widget_value("InFile","")
RunID = get_widget_value("RunID","2007010167")
SycSysCdSk = get_widget_value("SycSysCdSk","")
SrcSysCd = get_widget_value("SrcSysCd","")

schema_IdsCustSvcTaskCstmDtlExtr = T.StructType([
    T.StructField("JOB_EXCTN_RCRD_ERR_SK", T.IntegerType(), nullable=False),
    T.StructField("INSRT_UPDT_CD", T.StringType(), nullable=False),
    T.StructField("DISCARD_IN", T.StringType(), nullable=False),
    T.StructField("PASS_THRU_IN", T.StringType(), nullable=False),
    T.StructField("FIRST_RECYC_DT", T.TimestampType(), nullable=False),
    T.StructField("ERR_CT", T.IntegerType(), nullable=False),
    T.StructField("RECYCLE_CT", T.DecimalType(38,10), nullable=False),
    T.StructField("SRC_SYS_CD", T.StringType(), nullable=False),
    T.StructField("PRI_KEY_STRING", T.StringType(), nullable=False),
    T.StructField("CUST_SVC_TASK_CSTM_DTL_SK", T.IntegerType(), nullable=False),
    T.StructField("CUST_SVC_ID", T.StringType(), nullable=False),
    T.StructField("TASK_SEQ_NO", T.IntegerType(), nullable=False),
    T.StructField("CUST_SVC_TASK_CSTM_DTL_CD", T.StringType(), nullable=False),
    T.StructField("CSTM_DTL_UNIQ_ID", T.TimestampType(), nullable=False),
    T.StructField("CSTM_DTL_SEQ_NO", T.IntegerType(), nullable=False),
    T.StructField("CRT_RUN_CYC_EXCTN_SK", T.IntegerType(), nullable=False),
    T.StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", T.IntegerType(), nullable=False),
    T.StructField("CRT_BY_USER", T.IntegerType(), nullable=False),
    T.StructField("CUST_SVC_TASK", T.IntegerType(), nullable=False),
    T.StructField("LAST_UPDT_USER", T.IntegerType(), nullable=False),
    T.StructField("CRT_DTM", T.TimestampType(), nullable=False),
    T.StructField("CSTM_DTL_DT_1", T.StringType(), nullable=False),
    T.StructField("CSTM_DTL_DT_2", T.StringType(), nullable=False),
    T.StructField("LAST_UPDT_DTM", T.TimestampType(), nullable=False),
    T.StructField("CSTM_DTL_MNY_1", T.DecimalType(38,10), nullable=False),
    T.StructField("CSTM_DTL_NO_1", T.IntegerType(), nullable=False),
    T.StructField("CSTM_DTL_DESC", T.StringType(), nullable=True),
    T.StructField("CSTM_DTL_TX_1", T.StringType(), nullable=True),
    T.StructField("CSTM_DTL_TX_2", T.StringType(), nullable=True),
    T.StructField("CSTM_DTL_TX_3", T.StringType(), nullable=True)
])

df_IdsCustSvcTaskCstmDtlExtr = (
    spark.read.format("csv")
    .option("sep", ",")
    .option("quote", "\"")
    .schema(schema_IdsCustSvcTaskCstmDtlExtr)
    .load(f"{adls_path}/key/{InFile}")
)

df_foreignKey = (
    df_IdsCustSvcTaskCstmDtlExtr
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn("svCustSvcTaskLtrSk", GetFkeyCodes(col("SRC_SYS_CD"), col("CUST_SVC_TASK_CSTM_DTL_SK"), lit("ATTACHMENT TYPE"), col("CUST_SVC_TASK_CSTM_DTL_CD"), lit(Logging)))
    .withColumn("svCrtByUserSk", GetFkeyAppUsr(col("SRC_SYS_CD"), col("CUST_SVC_TASK_CSTM_DTL_SK"), col("CRT_BY_USER"), lit(Logging)))
    .withColumn("svCustSvcTaskSk", GetFkeyCustSvcTask(col("SRC_SYS_CD"), col("CUST_SVC_TASK_CSTM_DTL_SK"), col("CUST_SVC_ID"), col("TASK_SEQ_NO"), lit(Logging)))
    .withColumn("svLastUpdUserSk", GetFkeyAppUsr(col("SRC_SYS_CD"), col("CUST_SVC_TASK_CSTM_DTL_SK"), col("LAST_UPDT_USER"), lit(Logging)))
    .withColumn("svCstmdtlDt1", GetFkeyDate(lit("IDS"), col("CUST_SVC_TASK_CSTM_DTL_SK"), col("CSTM_DTL_DT_1"), lit(Logging)))
    .withColumn("svCstmdtlDt2", GetFkeyDate(lit("IDS"), col("CUST_SVC_TASK_CSTM_DTL_SK"), col("CSTM_DTL_DT_2"), lit(Logging)))
    .withColumn("ErrCount", GetFkeyErrorCnt(col("CUST_SVC_TASK_CSTM_DTL_SK")))
)

df_CustSrvTaskCstmDtl = (
    df_foreignKey
    .filter((col("ErrCount") == lit(0)) | (col("PassThru") == lit("Y")))
    .select(
        col("CUST_SVC_TASK_CSTM_DTL_SK").alias("CUST_SVC_TASK_CSTM_DTL_SK"),
        lit(SycSysCdSk).alias("SRC_SYS_CD_SK"),
        col("CUST_SVC_ID").alias("CUST_SVC_ID"),
        col("TASK_SEQ_NO").alias("TASK_SEQ_NO"),
        col("svCustSvcTaskLtrSk").alias("CUST_SVC_TASK_CSTM_DTL_CD_SK"),
        col("CSTM_DTL_UNIQ_ID").alias("CSTM_DTL_UNIQ_ID"),
        col("CSTM_DTL_SEQ_NO").alias("CSTM_DTL_SEQ_NO"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("svCrtByUserSk").alias("CRT_BY_USER_SK"),
        col("svCustSvcTaskSk").alias("CUST_SVC_TASK_SK"),
        col("svLastUpdUserSk").alias("LAST_UPDT_USER_SK"),
        col("CRT_DTM").alias("CRT_DTM"),
        col("svCstmdtlDt1").alias("CSTM_DTL_DT_1_SK"),
        col("svCstmdtlDt2").alias("CSTM_DTL_DT_2_SK"),
        col("LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
        col("CSTM_DTL_MNY_1").alias("CSTM_DTL_MNY_1"),
        col("CSTM_DTL_NO_1").alias("CSTM_DTL_NO_1"),
        col("CSTM_DTL_DESC").alias("CSTM_DTL_DESC"),
        col("CSTM_DTL_TX_1").alias("CSTM_DTL_TX_1"),
        col("CSTM_DTL_TX_2").alias("CSTM_DTL_TX_2"),
        col("CSTM_DTL_TX_3").alias("CSTM_DTL_TX_3")
    )
)

df_lnkRecycle = (
    df_foreignKey
    .filter(col("ErrCount") > lit(0))
    .select(
        GetRecycleKey(col("CUST_SVC_TASK_CSTM_DTL_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        col("DISCARD_IN").alias("DISCARD_IN"),
        col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        col("ErrCount").alias("ERR_CT"),
        (col("RECYCLE_CT") + lit(1)).alias("RECYCLE_CT"),
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        col("CUST_SVC_TASK_CSTM_DTL_SK").alias("CUST_SVC_TASK_CSTM_DTL_SK"),
        col("CUST_SVC_TASK_CSTM_DTL_SK").alias("CUST_SVC_ID"),
        col("TASK_SEQ_NO").alias("TASK_SEQ_NO"),
        col("CUST_SVC_TASK_CSTM_DTL_CD").alias("CUST_SVC_TASK_CSTM_DTL_CD"),
        col("CSTM_DTL_UNIQ_ID").alias("CSTM_DTL_UNIQ_ID"),
        col("CSTM_DTL_SEQ_NO").alias("CSTM_DTL_SEQ_NO"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("CRT_BY_USER").alias("CRT_BY_USER_SK"),
        col("CUST_SVC_TASK").alias("CUST_SVC_TASK_SK"),
        col("LAST_UPDT_USER").alias("LAST_UPDT_USER_SK"),
        col("CRT_DTM").alias("CRT_DTM"),
        col("CSTM_DTL_DT_1").alias("CSTM_DTL_DT_1_SK"),
        col("CSTM_DTL_DT_2").alias("CSTM_DTL_DT_2_SK"),
        col("LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
        col("CSTM_DTL_MNY_1").alias("CSTM_DTL_MNY_1"),
        col("CSTM_DTL_NO_1").alias("CSTM_DTL_NO_1"),
        col("CSTM_DTL_DESC").alias("CSTM_DTL_DESC"),
        col("CSTM_DTL_TX_1").alias("CSTM_DTL_TX_1"),
        col("CSTM_DTL_TX_2").alias("CSTM_DTL_TX_2"),
        col("CSTM_DTL_TX_3").alias("CSTM_DTL_TX_3")
    )
)

df_Recycle_Keys = (
    df_foreignKey
    .filter(col("ErrCount") > lit(0))
    .select(
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("CUST_SVC_ID").alias("CUST_SVC_ID"),
        col("TASK_SEQ_NO").alias("TASK_SEQ_NO")
    )
)

w = Window.orderBy(lit(1))
df_with_rn = df_foreignKey.withColumn("rownum", row_number().over(w))

df_DefaultUNK = (
    df_with_rn
    .filter(col("rownum") == lit(1))
    .select(
        lit(0).alias("CUST_SVC_TASK_CSTM_DTL_SK"),
        lit(0).alias("SRC_SYS_CD_SK"),
        lit("UNK").alias("CUST_SVC_ID"),
        lit(0).alias("TASK_SEQ_NO"),
        lit(0).alias("CUST_SVC_TASK_CSTM_DTL_CD_SK"),
        lit("1753-01-01").cast(T.TimestampType()).alias("CSTM_DTL_UNIQ_ID"),
        lit(0).alias("CSTM_DTL_SEQ_NO"),
        lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(0).alias("CRT_BY_USER_SK"),
        lit(0).alias("CUST_SVC_TASK_SK"),
        lit(0).alias("LAST_UPDT_USER_SK"),
        lit("1753-01-01").cast(T.TimestampType()).alias("CRT_DTM"),
        lit("UNK").alias("CSTM_DTL_DT_1_SK"),
        lit("UNK").alias("CSTM_DTL_DT_2_SK"),
        lit("1753-01-01").cast(T.TimestampType()).alias("LAST_UPDT_DTM"),
        lit(0).alias("CSTM_DTL_MNY_1"),
        lit(0).alias("CSTM_DTL_NO_1"),
        lit("UNK").alias("CSTM_DTL_DESC"),
        lit("UNK").alias("CSTM_DTL_TX_1"),
        lit("UNK").alias("CSTM_DTL_TX_2"),
        lit("UNK").alias("CSTM_DTL_TX_3")
    )
)

df_DefaultNA = (
    df_with_rn
    .filter(col("rownum") == lit(1))
    .select(
        lit(1).alias("CUST_SVC_TASK_CSTM_DTL_SK"),
        lit(1).alias("SRC_SYS_CD_SK"),
        lit("NA").alias("CUST_SVC_ID"),
        lit(1).alias("TASK_SEQ_NO"),
        lit(1).alias("CUST_SVC_TASK_CSTM_DTL_CD_SK"),
        lit("1753-01-01").cast(T.TimestampType()).alias("CSTM_DTL_UNIQ_ID"),
        lit(1).alias("CSTM_DTL_SEQ_NO"),
        lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(1).alias("CRT_BY_USER_SK"),
        lit(1).alias("CUST_SVC_TASK_SK"),
        lit(1).alias("LAST_UPDT_USER_SK"),
        lit("1753-01-01").cast(T.TimestampType()).alias("CRT_DTM"),
        lit("NA").alias("CSTM_DTL_DT_1_SK"),
        lit("NA").alias("CSTM_DTL_DT_2_SK"),
        lit("1753-01-01").cast(T.TimestampType()).alias("LAST_UPDT_DTM"),
        lit(1).alias("CSTM_DTL_MNY_1"),
        lit(1).alias("CSTM_DTL_NO_1"),
        lit("NA").alias("CSTM_DTL_DESC"),
        lit("NA").alias("CSTM_DTL_TX_1"),
        lit("NA").alias("CSTM_DTL_TX_2"),
        lit("NA").alias("CSTM_DTL_TX_3")
    )
)

df_Collector = df_CustSrvTaskCstmDtl.unionByName(df_DefaultUNK).unionByName(df_DefaultNA)

df_lnkRecycle_for_write = df_lnkRecycle.select(
    rpad(col("JOB_EXCTN_RCRD_ERR_SK").cast(T.StringType()), 100, " ").alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    rpad(col("ERR_CT").cast(T.StringType()), 100, " ").alias("ERR_CT"),
    rpad(col("RECYCLE_CT").cast(T.StringType()), 100, " ").alias("RECYCLE_CT"),
    rpad(col("SRC_SYS_CD"), 100, " ").alias("SRC_SYS_CD"),
    rpad(col("PRI_KEY_STRING"), 100, " ").alias("PRI_KEY_STRING"),
    rpad(col("CUST_SVC_TASK_CSTM_DTL_SK").cast(T.StringType()), 100, " ").alias("CUST_SVC_TASK_CSTM_DTL_SK"),
    rpad(col("CUST_SVC_ID").cast(T.StringType()), 100, " ").alias("CUST_SVC_ID"),
    rpad(col("TASK_SEQ_NO").cast(T.StringType()), 100, " ").alias("TASK_SEQ_NO"),
    rpad(col("CUST_SVC_TASK_CSTM_DTL_CD"), 100, " ").alias("CUST_SVC_TASK_CSTM_DTL_CD"),
    col("CSTM_DTL_UNIQ_ID").alias("CSTM_DTL_UNIQ_ID"),
    rpad(col("CSTM_DTL_SEQ_NO").cast(T.StringType()), 100, " ").alias("CSTM_DTL_SEQ_NO"),
    rpad(col("CRT_RUN_CYC_EXCTN_SK").cast(T.StringType()), 100, " ").alias("CRT_RUN_CYC_EXCTN_SK"),
    rpad(col("LAST_UPDT_RUN_CYC_EXCTN_SK").cast(T.StringType()), 100, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(col("CRT_BY_USER_SK").cast(T.StringType()), 100, " ").alias("CRT_BY_USER_SK"),
    rpad(col("CUST_SVC_TASK_SK").cast(T.StringType()), 100, " ").alias("CUST_SVC_TASK_SK"),
    rpad(col("LAST_UPDT_USER_SK").cast(T.StringType()), 100, " ").alias("LAST_UPDT_USER_SK"),
    col("CRT_DTM").alias("CRT_DTM"),
    rpad(col("CSTM_DTL_DT_1_SK"), 10, " ").alias("CSTM_DTL_DT_1_SK"),
    rpad(col("CSTM_DTL_DT_2_SK"), 10, " ").alias("CSTM_DTL_DT_2_SK"),
    col("LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
    rpad(col("CSTM_DTL_MNY_1").cast(T.StringType()), 100, " ").alias("CSTM_DTL_MNY_1"),
    rpad(col("CSTM_DTL_NO_1").cast(T.StringType()), 100, " ").alias("CSTM_DTL_NO_1"),
    rpad(col("CSTM_DTL_DESC"), 100, " ").alias("CSTM_DTL_DESC"),
    rpad(col("CSTM_DTL_TX_1"), 100, " ").alias("CSTM_DTL_TX_1"),
    rpad(col("CSTM_DTL_TX_2"), 100, " ").alias("CSTM_DTL_TX_2"),
    rpad(col("CSTM_DTL_TX_3"), 100, " ").alias("CSTM_DTL_TX_3")
)

write_files(
    df_lnkRecycle_for_write,
    "hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_Recycle_Keys_for_write = df_Recycle_Keys.select(
    rpad(col("SRC_SYS_CD"), 100, " ").alias("SRC_SYS_CD"),
    rpad(col("CUST_SVC_ID"), 100, " ").alias("CUST_SVC_ID"),
    rpad(col("TASK_SEQ_NO").cast(T.StringType()), 100, " ").alias("TASK_SEQ_NO")
)

write_files(
    df_Recycle_Keys_for_write,
    "hf_custsvc_recycle_keys.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_Collector_for_write = df_Collector.select(
    rpad(col("CUST_SVC_TASK_CSTM_DTL_SK").cast(T.StringType()), 100, " ").alias("CUST_SVC_TASK_CSTM_DTL_SK"),
    rpad(col("SRC_SYS_CD_SK").cast(T.StringType()), 100, " ").alias("SRC_SYS_CD_SK"),
    rpad(col("CUST_SVC_ID"), 100, " ").alias("CUST_SVC_ID"),
    rpad(col("TASK_SEQ_NO").cast(T.StringType()), 100, " ").alias("TASK_SEQ_NO"),
    rpad(col("CUST_SVC_TASK_CSTM_DTL_CD_SK").cast(T.StringType()), 100, " ").alias("CUST_SVC_TASK_CSTM_DTL_CD_SK"),
    col("CSTM_DTL_UNIQ_ID").alias("CSTM_DTL_UNIQ_ID"),
    rpad(col("CSTM_DTL_SEQ_NO").cast(T.StringType()), 100, " ").alias("CSTM_DTL_SEQ_NO"),
    rpad(col("CRT_RUN_CYC_EXCTN_SK").cast(T.StringType()), 100, " ").alias("CRT_RUN_CYC_EXCTN_SK"),
    rpad(col("LAST_UPDT_RUN_CYC_EXCTN_SK").cast(T.StringType()), 100, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(col("CRT_BY_USER_SK").cast(T.StringType()), 100, " ").alias("CRT_BY_USER_SK"),
    rpad(col("CUST_SVC_TASK_SK").cast(T.StringType()), 100, " ").alias("CUST_SVC_TASK_SK"),
    rpad(col("LAST_UPDT_USER_SK").cast(T.StringType()), 100, " ").alias("LAST_UPDT_USER_SK"),
    col("CRT_DTM").alias("CRT_DTM"),
    rpad(col("CSTM_DTL_DT_1_SK"), 10, " ").alias("CSTM_DTL_DT_1_SK"),
    rpad(col("CSTM_DTL_DT_2_SK"), 10, " ").alias("CSTM_DTL_DT_2_SK"),
    col("LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
    rpad(col("CSTM_DTL_MNY_1").cast(T.StringType()), 100, " ").alias("CSTM_DTL_MNY_1"),
    rpad(col("CSTM_DTL_NO_1").cast(T.StringType()), 100, " ").alias("CSTM_DTL_NO_1"),
    rpad(col("CSTM_DTL_DESC"), 100, " ").alias("CSTM_DTL_DESC"),
    rpad(col("CSTM_DTL_TX_1"), 100, " ").alias("CSTM_DTL_TX_1"),
    rpad(col("CSTM_DTL_TX_2"), 100, " ").alias("CSTM_DTL_TX_2"),
    rpad(col("CSTM_DTL_TX_3"), 100, " ").alias("CSTM_DTL_TX_3")
)

write_files(
    df_Collector_for_write,
    f"{adls_path}/load/CUST_SVC_TASK_CSTM_DTL.{SrcSysCd}.dat",
    ",",
    "overwrite",
    False,
    True,
    "\"",
    None
)