# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_2 03/10/09 09:25:21 Batch  15045_33943 INIT bckcetl ids20 dcg01 sa Bringing ALL Claim code down to devlIDS
# MAGIC ^1_4 02/10/09 11:16:45 Batch  15017_40611 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_3 02/02/09 09:52:49 Batch  15009_35578 PROMOTE bckcetl ids20 dsadm bls for brent
# MAGIC ^1_3 01/26/09 13:03:15 Batch  15002_46999 INIT bckcett testIDS dsadm bls for bl
# MAGIC ^1_1 12/16/08 10:00:18 Batch  14961_36031 PROMOTE bckcett testIDS u03651 steph for Brent - claim primary keying
# MAGIC ^1_1 12/16/08 09:15:37 Batch  14961_33343 INIT bckcett devlIDS u03651 steffy
# MAGIC ^1_1 01/29/08 08:55:59 Batch  14639_32164 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 12/28/07 10:13:19 Batch  14607_36806 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/26/07 09:37:10 Batch  14575_34636 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_2 08/30/07 07:19:35 Batch  14487_26382 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 08/27/07 10:06:05 Batch  14484_36371 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_3 03/26/07 12:34:37 Batch  14330_45286 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 03/07/07 16:30:43 Batch  14311_59451 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_2 03/07/07 15:24:05 Batch  14311_55447 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_1 12/18/06 15:24:00 Batch  14232_55459 PROMOTE bckcetl ids20 dsadm Keith for Ralph install 12/18/2006
# MAGIC ^1_1 12/18/06 14:39:27 Batch  14232_52797 INIT bckcett testIDS30 dsadm Keith for Ralph CDHP Install 12182006
# MAGIC ^1_1 11/17/06 13:46:03 Batch  14201_49568 PROMOTE bckcett testIDS30 u06640 Ralph
# MAGIC ^1_1 11/17/06 11:12:19 Batch  14201_40344 INIT bckcett devlIDS30 u06640 Ralph
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2004 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsClmExtrnlRefDataFkey
# MAGIC 
# MAGIC DESCRIPTION:     Takes the sorted file and applies the foreign keys.  Output is final table format for CLM_EXTRNL_REF_DATA
# MAGIC       
# MAGIC 
# MAGIC INPUTS:   Common record format file from Primary key assignment job
# MAGIC 	
# MAGIC   
# MAGIC HASH FILES:     hf_recycle - used to keep records with an error in surrogate key assignment
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                               GetFkeyCodes
# MAGIC                               GetFkeyErrorCnt
# MAGIC                            
# MAGIC 
# MAGIC PROCESSING:  assign foreign (surrogate) keys to record
# MAGIC 
# MAGIC OUTPUTS:  file ready to load to CLM_EXTRNL_REF_DATA table in IDS
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC             Parikshith Chada         10/25/2006         -  Originally Programmed
# MAGIC             
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                                                Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                                                         --------------------------------       -------------------------------   ----------------------------       
# MAGIC Bhoomi Dasari         2008-08-12      3567(Primary Key)    Added new parameter SrcSysCdSk and passing it directly from source                  devlIDS                          Steph Goddard          08/15/2008
# MAGIC Kalyan Neelam        2010-02-19      4278                        Changed length of the fields EXTR_REF_ID and 
# MAGIC                                                                                         PCA_EXTRNL_ID to 255 (before it was 20)                                                            IntegrateCurDevl            Steph Goddard          03/08/2010

# MAGIC Read common record format file from extract job.
# MAGIC Set all foreign surragote keys
# MAGIC Writing Sequential File to /load
# MAGIC Merge source data with default rows
# MAGIC This hash file is written to by all claim foriegn key jobs.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DecimalType
from pyspark.sql.functions import col, lit, row_number, rpad
from pyspark.sql import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


InFile = get_widget_value('InFile','IdsClmOvrPayPkey.ClmOvrPay.dat')
Source = get_widget_value('Source','FACETS')
Logging = get_widget_value('Logging','Y')
CurrRunCycle = get_widget_value('CurrRunCycle','100')
RunID = get_widget_value('RunID','20080805')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')

schema_ClmExtrnlRefData = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38,10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("CLM_SK", IntegerType(), False),
    StructField("CLM_ID", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN", IntegerType(), False),
    StructField("EXTRNL_REF_ID", StringType(), False),
    StructField("PCA_EXTRNL_ID", StringType(), False),
    StructField("PCA_SRC_NM", StringType(), False),
    StructField("TRDNG_PRTNR_NM", StringType(), False)
])

df_ClmExtrnlRefData = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "\"")
    .schema(schema_ClmExtrnlRefData)
    .load(f"{adls_path}/key/{InFile}")
)

df_ForeignKey = (
    df_ClmExtrnlRefData
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn("ErrCount", GetFkeyErrorCnt(col("CLM_SK")))
)

df_Fkey = (
    df_ForeignKey
    .filter((col("ErrCount") == lit(0)) | (col("PassThru") == lit("Y")))
    .select(
        col("CLM_SK").alias("CLM_SK"),
        lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        col("CLM_ID").alias("CLM_ID"),
        col("CRT_RUN_CYC_EXCTN").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("EXTRNL_REF_ID").alias("EXTRNL_REF_ID"),
        col("PCA_EXTRNL_ID").alias("PCA_EXTRNL_ID"),
        col("PCA_SRC_NM").alias("PCA_SRC_NM"),
        col("TRDNG_PRTNR_NM").alias("TRDNG_PRTNR_NM")
    )
)

df_Recycle = (
    df_ForeignKey
    .filter(col("ErrCount") > lit(0))
    .select(
        col("INSRT_UPDT_CD"),
        col("DISCARD_IN"),
        col("PASS_THRU_IN"),
        col("FIRST_RECYC_DT"),
        (col("ERR_CT") + lit(1)).alias("ERR_CT"),
        col("RECYCLE_CT"),
        col("SRC_SYS_CD"),
        col("PRI_KEY_STRING"),
        col("CLM_SK"),
        col("CLM_ID"),
        col("CRT_RUN_CYC_EXCTN"),
        col("LAST_UPDT_RUN_CYC_EXCTN"),
        col("EXTRNL_REF_ID"),
        col("PCA_EXTRNL_ID"),
        col("PCA_SRC_NM"),
        col("TRDNG_PRTNR_NM")
    )
)

windowSpec = Window.orderBy(lit(1))
df_ForeignKeyIndexed = df_ForeignKey.withColumn("_row_num_", row_number().over(windowSpec))

df_DefaultUNK = (
    df_ForeignKeyIndexed
    .filter(col("_row_num_") == 1)
    .select(
        lit(0).alias("CLM_SK"),
        lit(0).alias("SRC_SYS_CD_SK"),
        lit("UNK").alias("CLM_ID"),
        lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit("UNK").alias("EXTRNL_REF_ID"),
        lit("UNK").alias("PCA_EXTRNL_ID"),
        lit("UNK").alias("PCA_SRC_NM"),
        lit("UNK").alias("TRDNG_PRTNR_NM")
    )
)

df_DefaultNA = (
    df_ForeignKeyIndexed
    .filter(col("_row_num_") == 1)
    .select(
        lit(1).alias("CLM_SK"),
        lit(1).alias("SRC_SYS_CD_SK"),
        lit("NA").alias("CLM_ID"),
        lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit("NA").alias("EXTRNL_REF_ID"),
        lit("NA").alias("PCA_EXTRNL_ID"),
        lit("NA").alias("PCA_SRC_NM"),
        lit("NA").alias("TRDNG_PRTNR_NM")
    )
)

df_RecycleClms = (
    df_ForeignKey
    .filter(col("ErrCount") > lit(0))
    .select(
        col("SRC_SYS_CD"),
        col("CLM_ID")
    )
)

df_recycle_write = df_Recycle.select(
    rpad(col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    col("ERR_CT").alias("ERR_CT"),
    col("RECYCLE_CT").alias("RECYCLE_CT"),
    rpad(col("SRC_SYS_CD"), 100, " ").alias("SRC_SYS_CD"),
    rpad(col("PRI_KEY_STRING"), 100, " ").alias("PRI_KEY_STRING"),
    col("CLM_SK").alias("CLM_SK"),
    rpad(col("CLM_ID"), 100, " ").alias("CLM_ID"),
    col("CRT_RUN_CYC_EXCTN").alias("CRT_RUN_CYC_EXCTN"),
    col("LAST_UPDT_RUN_CYC_EXCTN").alias("LAST_UPDT_RUN_CYC_EXCTN"),
    rpad(col("EXTRNL_REF_ID"), 100, " ").alias("EXTRNL_REF_ID"),
    rpad(col("PCA_EXTRNL_ID"), 100, " ").alias("PCA_EXTRNL_ID"),
    rpad(col("PCA_SRC_NM"), 100, " ").alias("PCA_SRC_NM"),
    rpad(col("TRDNG_PRTNR_NM"), 100, " ").alias("TRDNG_PRTNR_NM")
)

write_files(
    df_recycle_write,
    "hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_recycle_clms_write = df_RecycleClms.select(
    rpad(col("SRC_SYS_CD"), 100, " ").alias("SRC_SYS_CD"),
    rpad(col("CLM_ID"), 100, " ").alias("CLM_ID")
)

write_files(
    df_recycle_clms_write,
    "hf_claim_recycle_keys.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_Collector = df_DefaultNA.unionByName(df_DefaultUNK).unionByName(df_Fkey)

df_loadfile = df_Collector.select(
    col("CLM_SK").alias("CLM_SK"),
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    rpad(col("CLM_ID"), 100, " ").alias("CLM_ID"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(col("EXTRNL_REF_ID"), 100, " ").alias("EXTRNL_REF_ID"),
    rpad(col("PCA_EXTRNL_ID"), 100, " ").alias("PCA_EXTRNL_ID"),
    rpad(col("PCA_SRC_NM"), 100, " ").alias("PCA_SRC_NM"),
    rpad(col("TRDNG_PRTNR_NM"), 100, " ").alias("TRDNG_PRTNR_NM")
)

write_files(
    df_loadfile,
    f"{adls_path}/load/CLM_EXTRNL_REF_DATA.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)