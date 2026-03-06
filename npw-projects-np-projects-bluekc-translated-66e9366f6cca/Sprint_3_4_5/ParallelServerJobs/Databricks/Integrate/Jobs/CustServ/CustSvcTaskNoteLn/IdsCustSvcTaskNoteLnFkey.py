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
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007, 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:
# MAGIC 
# MAGIC PROCESSING:
# MAGIC 
# MAGIC              
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer             Date                         Change Description                                                        Project #                   Development Project          Code Reviewer          Date Reviewed  
# MAGIC ------------------           ----------------------------     -----------------------------------------------------------------------                ----------------                 ------------------------------------       ----------------------------      ----------------
# MAGIC Parikshith Chada  02/8/2007                Originally Programmed                                                    3028                          devlIDS30                          Steph Goddrad             02/21/2007
# MAGIC Brent Leland         02/29/2008              Added error recycle                                                       3567 Primary Key       devlIDScur                          Steph Goddard            05/06/2008
# MAGIC                                                                Added source system to load file name

# MAGIC Read common record format file from extract job.
# MAGIC Set all foreign surrogate keys
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
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DecimalType
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


Logging = get_widget_value('Logging','Y')
InFile = get_widget_value('InFile','IdsCustSvcTaskNoteLnExtr.CSTaskNoteLn.dat')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
SrcSysCd = get_widget_value('SrcSysCd','')

schema_IdsCustSvcTaskNoteLnFkey = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38,10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("CUST_SVC_TASK_NOTE_LN_SK", IntegerType(), False),
    StructField("CUST_SVC_ID", StringType(), False),
    StructField("TASK_SEQ_NO", IntegerType(), False),
    StructField("CUST_SVC_TASK_NOTE_LOC_CD_SK", IntegerType(), False),
    StructField("NOTE_SEQ_NO", IntegerType(), False),
    StructField("LAST_UPDT_DTM", TimestampType(), False),
    StructField("LN_SEQ_NO", IntegerType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("CUST_SVC_TASK_NOTE_SK", IntegerType(), False),
    StructField("LN_TX", StringType(), True)
])

df_Key = (
    spark.read
    .option("delimiter", ",")
    .option("quote", "\"")
    .schema(schema_IdsCustSvcTaskNoteLnFkey)
    .csv(f"{adls_path}/key/{InFile}", header=False)
)

df_Tx = (
    df_Key
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("svCustSvcTaskNoteSrcCdSk", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("CUST_SVC_TASK_NOTE_LN_SK"), F.lit("CUSTOMER SERVICE TASK NOTE SOURCE"), F.col("CUST_SVC_TASK_NOTE_LOC_CD_SK"), Logging))
    .withColumn("svCustSvcTaskNoteSk", GetFkeyCustSvcTaskNote(F.col("SRC_SYS_CD"), F.col("CUST_SVC_TASK_NOTE_LN_SK"), F.col("CUST_SVC_ID"), F.col("TASK_SEQ_NO"), F.col("CUST_SVC_TASK_NOTE_LOC_CD_SK"), F.col("NOTE_SEQ_NO"), F.col("LAST_UPDT_DTM"), Logging))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("CUST_SVC_TASK_NOTE_LN_SK")))
)

df_CustSvcTaskNoteLn = (
    df_Tx
    .filter((F.col("ErrCount") == 0) | (F.col("PassThru") == 'Y'))
    .select(
        F.col("CUST_SVC_TASK_NOTE_LN_SK").alias("CUST_SVC_TASK_NOTE_LN_SK"),
        F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        F.col("CUST_SVC_ID").alias("CUST_SVC_ID"),
        F.col("TASK_SEQ_NO").alias("TASK_SEQ_NO"),
        F.col("svCustSvcTaskNoteSrcCdSk").alias("CUST_SVC_TASK_NOTE_LOC_CD_SK"),
        F.col("NOTE_SEQ_NO").alias("NOTE_SEQ_NO"),
        F.col("LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
        F.col("LN_SEQ_NO").alias("LN_SEQ_NO"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("svCustSvcTaskNoteSk").alias("CUST_SVC_TASK_NOTE_SK"),
        F.col("LN_TX").alias("LN_TX")
    )
)

df_lnkRecycle = (
    df_Tx
    .filter(F.col("ErrCount") > 0)
    .select(
        GetRecycleKey(F.col("CUST_SVC_TASK_NOTE_LN_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("DISCARD_IN").alias("DISCARD_IN"),
        F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        (F.col("ERR_CT") + F.lit(1)).alias("ERR_CT"),
        F.col("RECYCLE_CT").alias("RECYCLE_CT"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("CUST_SVC_TASK_NOTE_LN_SK").alias("CUST_SVC_TASK_NOTE_LN_SK"),
        F.col("CUST_SVC_ID").alias("CUST_SVC_ID"),
        F.col("TASK_SEQ_NO").alias("TASK_SEQ_NO"),
        F.col("CUST_SVC_TASK_NOTE_LOC_CD_SK").alias("CUST_SVC_TASK_NOTE_LOC_CD_SK"),
        F.col("NOTE_SEQ_NO").alias("NOTE_SEQ_NO"),
        F.col("LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
        F.col("LN_SEQ_NO").alias("LN_SEQ_NO"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("CUST_SVC_TASK_NOTE_SK").alias("CUST_SVC_TASK_NOTE_SK"),
        F.col("LN_TX").alias("LN_TX")
    )
)

w = Window.orderBy(F.lit(1))
df_Tx_rn = df_Tx.withColumn("rownum", F.row_number().over(w))

df_DefaultUNK = (
    df_Tx_rn
    .filter(F.col("rownum") == 1)
    .select(
        F.lit(0).alias("CUST_SVC_TASK_NOTE_LN_SK"),
        F.lit(0).alias("SRC_SYS_CD_SK"),
        F.lit("UNK").alias("CUST_SVC_ID"),
        F.lit(0).alias("TASK_SEQ_NO"),
        F.lit(0).alias("CUST_SVC_TASK_NOTE_LOC_CD_SK"),
        F.lit(0).alias("NOTE_SEQ_NO"),
        F.lit("1753-01-01 00:00:00").alias("LAST_UPDT_DTM"),
        F.lit(0).alias("LN_SEQ_NO"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("CUST_SVC_TASK_NOTE_SK"),
        F.lit("UNK").alias("LN_TX")
    )
)

df_DefaultNA = (
    df_Tx_rn
    .filter(F.col("rownum") == 1)
    .select(
        F.lit(1).alias("CUST_SVC_TASK_NOTE_LN_SK"),
        F.lit(1).alias("SRC_SYS_CD_SK"),
        F.lit("NA").alias("CUST_SVC_ID"),
        F.lit(1).alias("TASK_SEQ_NO"),
        F.lit(1).alias("CUST_SVC_TASK_NOTE_LOC_CD_SK"),
        F.lit(1).alias("NOTE_SEQ_NO"),
        F.lit("1753-01-01 00:00:00").alias("LAST_UPDT_DTM"),
        F.lit(1).alias("LN_SEQ_NO"),
        F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("CUST_SVC_TASK_NOTE_SK"),
        F.lit("NA").alias("LN_TX")
    )
)

df_Recycle_Keys = (
    df_Tx
    .filter(F.col("ErrCount") > 0)
    .select(
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("CUST_SVC_ID").alias("CUST_SVC_ID"),
        F.col("TASK_SEQ_NO").alias("TASK_SEQ_NO")
    )
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
        "CUST_SVC_TASK_NOTE_LN_SK",
        "CUST_SVC_ID",
        "TASK_SEQ_NO",
        "CUST_SVC_TASK_NOTE_LOC_CD_SK",
        "NOTE_SEQ_NO",
        "LAST_UPDT_DTM",
        "LN_SEQ_NO",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CUST_SVC_TASK_NOTE_SK",
        "LN_TX"
    ),
    f"{adls_path}/hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

write_files(
    df_Recycle_Keys.select(
        "SRC_SYS_CD",
        "CUST_SVC_ID",
        "TASK_SEQ_NO"
    ),
    f"{adls_path}/hf_custsvc_recycle_keys.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_Collector = (
    df_CustSvcTaskNoteLn.select(
        "CUST_SVC_TASK_NOTE_LN_SK",
        "SRC_SYS_CD_SK",
        "CUST_SVC_ID",
        "TASK_SEQ_NO",
        "CUST_SVC_TASK_NOTE_LOC_CD_SK",
        "NOTE_SEQ_NO",
        "LAST_UPDT_DTM",
        "LN_SEQ_NO",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CUST_SVC_TASK_NOTE_SK",
        "LN_TX"
    )
    .unionByName(
        df_DefaultUNK.select(
            "CUST_SVC_TASK_NOTE_LN_SK",
            "SRC_SYS_CD_SK",
            "CUST_SVC_ID",
            "TASK_SEQ_NO",
            "CUST_SVC_TASK_NOTE_LOC_CD_SK",
            "NOTE_SEQ_NO",
            "LAST_UPDT_DTM",
            "LN_SEQ_NO",
            "CRT_RUN_CYC_EXCTN_SK",
            "LAST_UPDT_RUN_CYC_EXCTN_SK",
            "CUST_SVC_TASK_NOTE_SK",
            "LN_TX"
        )
    )
    .unionByName(
        df_DefaultNA.select(
            "CUST_SVC_TASK_NOTE_LN_SK",
            "SRC_SYS_CD_SK",
            "CUST_SVC_ID",
            "TASK_SEQ_NO",
            "CUST_SVC_TASK_NOTE_LOC_CD_SK",
            "NOTE_SEQ_NO",
            "LAST_UPDT_DTM",
            "LN_SEQ_NO",
            "CRT_RUN_CYC_EXCTN_SK",
            "LAST_UPDT_RUN_CYC_EXCTN_SK",
            "CUST_SVC_TASK_NOTE_SK",
            "LN_TX"
        )
    )
)

df_Final = df_Collector.select(
    F.col("CUST_SVC_TASK_NOTE_LN_SK"),
    F.col("SRC_SYS_CD_SK"),
    F.rpad("CUST_SVC_ID", <...>, " ").alias("CUST_SVC_ID"),
    F.col("TASK_SEQ_NO"),
    F.col("CUST_SVC_TASK_NOTE_LOC_CD_SK"),
    F.col("NOTE_SEQ_NO"),
    F.col("LAST_UPDT_DTM"),
    F.col("LN_SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CUST_SVC_TASK_NOTE_SK"),
    F.rpad("LN_TX", <...>, " ").alias("LN_TX")
)

write_files(
    df_Final,
    f"{adls_path}/load/CUST_SVC_TASK_NOTE_LN.{SrcSysCd}.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)