# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 01/28/08 08:28:47 Batch  14638_30532 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 12/26/07 11:16:35 Batch  14605_40601 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/21/07 14:41:36 Batch  14570_52900 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 08/21/07 12:23:43 Batch  14478_44630 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_3 03/21/07 15:26:34 Batch  14325_55600 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 03/07/07 16:30:43 Batch  14311_59451 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_3 06/05/06 15:07:57 Batch  14036_54483 PROMOTE bckcetl ids20 dsadm Gina
# MAGIC ^1_3 06/05/06 14:57:55 Batch  14036_53885 INIT bckcett testIDS30 dsadm J. Mahaffey for B. Leland
# MAGIC ^1_1 05/23/06 11:53:17 Batch  14023_42832 PROMOTE bckcett testIDS30 u05779 bj
# MAGIC ^1_1 05/23/06 11:51:34 Batch  14023_42698 INIT bckcett devlIDS30 u05779 bj
# MAGIC ^1_10 12/13/05 13:55:16 Batch  13862_50122 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_9 10/05/05 10:21:04 Batch  13793_37270 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_8 09/13/05 13:35:31 Batch  13771_48935 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_7 09/12/05 16:01:51 Batch  13770_57714 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_6 09/12/05 15:45:16 Batch  13770_56719 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_5 09/12/05 15:38:28 Batch  13770_56313 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_4 09/09/05 16:34:40 Batch  13767_59682 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_3 09/09/05 16:10:39 Batch  13767_58243 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_2 08/15/05 09:23:22 Batch  13742_33806 INIT bckcett devlIDS30 u05779 bj
# MAGIC ^1_1 08/12/05 14:08:33 Batch  13739_50918 INIT bckcett devlIDS30 dsadm Gina Parr
# MAGIC ^1_1 08/05/05 14:03:16 Batch  13732_50601 INIT bckcett devlIDS30 u05779 bj
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsTccFKey
# MAGIC 
# MAGIC DESCRIPTION:    Assigns foreign keys to TCC records
# MAGIC       
# MAGIC 
# MAGIC INPUTS:	#$FilePath#/key/FdbNdcTccExtr.dat
# MAGIC HASH FILES:  hf_recycle
# MAGIC 
# MAGIC TRANSFORMS:  none
# MAGIC 
# MAGIC                            
# MAGIC PROCESSING:    Output file is created with a temp. name.  File renamed in job control.
# MAGIC   
# MAGIC 
# MAGIC OUTPUTS:   TCC.dat
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC               Hugh Sisson - 06/2005   - Original program  
# MAGIC               BJ Luce         04/2006      use environment variables, hard code input and output

# MAGIC Read common record format file from extract job.
# MAGIC Set all foreign surragote keys
# MAGIC Merge source data with default rows
# MAGIC Writing Sequential File to /load
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


Logging = get_widget_value("Logging","Y")

schema_TccExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38,10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("TCC_SK", IntegerType(), False),
    StructField("TCC", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("TCC_DESC", StringType(), False)
])

df_TccExtr = (
    spark.read.format("csv")
    .option("header", "false")
    .option("quote", "\"")
    .schema(schema_TccExtr)
    .load(f"{adls_path}/key/FdbNdcTccExtr.dat")
)

df_ForeignKey_in = (
    df_TccExtr
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("ERR_CT")))
    .withColumn("rownum", F.row_number().over(Window.orderBy(F.lit(1))))
)

df_ForeignKey_Fkey = (
    df_ForeignKey_in
    .filter((F.col("ErrCount") == 0) | (F.col("PassThru") == "Y"))
    .select(
        F.col("TCC_SK"),
        F.col("TCC"),
        F.col("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("TCC_DESC")
    )
)

df_ForeignKey_Recycle = (
    df_ForeignKey_in
    .filter(F.col("ErrCount") > 0)
    .select(
        GetRecycleKey(F.col("TCC_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD"),
        F.col("DISCARD_IN"),
        F.col("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT"),
        F.col("ErrCount").alias("ERR_CT"),
        F.col("RECYCLE_CT"),
        F.col("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING"),
        F.col("TCC_SK"),
        F.col("TCC"),
        F.col("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("TCC_DESC")
    )
)

df_ForeignKey_Recycle_write = df_ForeignKey_Recycle.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("TCC_SK"),
    F.rpad(F.col("TCC"), 3, " ").alias("TCC"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("TCC_DESC")
)

write_files(
    df_ForeignKey_Recycle_write,
    f"{adls_path}/hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_ForeignKey_DefaultNA = (
    df_ForeignKey_in
    .filter(F.col("rownum") == 1)
    .select(
        F.lit(1).alias("TCC_SK"),
        F.lit("1").alias("TCC"),
        F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit("NA").alias("TCC_DESC")
    )
)

df_ForeignKey_DefaultUNK = (
    df_ForeignKey_in
    .filter(F.col("rownum") == 1)
    .select(
        F.lit(0).alias("TCC_SK"),
        F.lit("0").alias("TCC"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit("UNK").alias("TCC_DESC")
    )
)

df_Collector = (
    df_ForeignKey_Fkey
    .unionByName(df_ForeignKey_DefaultNA)
    .unionByName(df_ForeignKey_DefaultUNK)
)

df_DrugTcc_write = df_Collector.select(
    F.col("TCC_SK"),
    F.rpad(F.col("TCC"), 3, " ").alias("TCC"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("TCC_DESC")
)

write_files(
    df_DrugTcc_write,
    f"{adls_path}/load/TCC.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)