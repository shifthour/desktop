# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_2 02/20/09 11:05:46 Batch  15027_39950 PROMOTE bckcetl ids20 dsadm bls for sa
# MAGIC ^1_2 02/20/09 10:48:45 Batch  15027_38927 INIT bckcett testIDS dsadm bls for sa
# MAGIC ^1_2 02/19/09 15:48:36 Batch  15026_56922 PROMOTE bckcett testIDS u03651 steph for Sharon
# MAGIC ^1_2 02/19/09 15:43:11 Batch  15026_56617 INIT bckcett devlIDS u03651 steffy
# MAGIC ^1_1 01/13/09 08:41:23 Batch  14989_31287 INIT bckcett devlIDS u03651 steffy
# MAGIC ^1_1 01/30/08 05:00:10 Batch  14640_18019 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 08/27/07 08:55:46 Batch  14484_32151 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_2 07/02/07 10:26:00 Batch  14428_37563 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_1 07/02/07 10:19:17 Batch  14428_37158 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_1 03/28/07 06:42:38 Batch  14332_24162 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 02/11/07 12:33:13 Batch  14287_45195 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_1 11/03/06 13:03:29 Batch  14187_47011 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_3 05/22/06 11:41:30 Batch  14022_42099 PROMOTE bckcetl ids20 dsadm Jim Hart
# MAGIC ^1_3 05/22/06 10:32:59 Batch  14022_37985 INIT bckcett testIDS30 dsadm Jim Hart
# MAGIC ^1_6 05/12/06 15:17:06 Batch  14012_55029 PROMOTE bckcett testIDS30 u10157 sa
# MAGIC ^1_6 05/12/06 15:03:19 Batch  14012_54207 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_5 05/11/06 16:20:33 Batch  14011_58841 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_4 05/11/06 12:36:55 Batch  14011_45419 PROMOTE bckcett devlIDS30 u10157 sa
# MAGIC ^1_4 05/11/06 12:31:29 Batch  14011_45092 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_1 05/02/06 13:04:24 Batch  14002_47066 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_2 01/26/06 07:54:36 Batch  13906_28483 INIT bckcetl ids20 dsadm Gina
# MAGIC ^1_1 01/24/06 14:49:28 Batch  13904_53375 PROMOTE bckcetl ids20 dsadm Gina Parr
# MAGIC ^1_1 01/24/06 14:47:17 Batch  13904_53242 INIT bckcett testIDS30 dsadm Gina Parr
# MAGIC ^1_2 01/24/06 14:36:07 Batch  13904_52574 INIT bckcett testIDS30 dsadm Gina Parr
# MAGIC ^1_1 01/24/06 14:21:30 Batch  13904_51695 INIT bckcett testIDS30 dsadm Gina Parr
# MAGIC ^1_3 12/23/05 11:39:15 Batch  13872_41979 PROMOTE bckcett testIDS30 u10913 Move Income Commission to test
# MAGIC ^1_3 12/23/05 11:12:01 Batch  13872_40337 INIT bckcett devlIDS30 u10913 Ollie Move Income/Commisions to test
# MAGIC ^1_2 12/23/05 10:39:46 Batch  13872_38400 INIT bckcett devlIDS30 u10913 Ollie Moving Income / Commission to Test
# MAGIC ^1_1 12/19/05 09:30:02 Batch  13868_34210 INIT bckcett devlIDS30 u11141 Hugh Sisson
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsComsnArgmtFkey
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
# MAGIC                             GetFkeyDate
# MAGIC                             GetFkeyErrorCnt
# MAGIC 
# MAGIC PROCESSING:  Natural keys are converted to surrogate keys
# MAGIC 
# MAGIC OUTPUTS:   COMSN_ARGMT.dat table load file
# MAGIC                      hf_recycle - If recycling was used for this job records would be written to this hash file that had errors.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC             Hugh Sisson  -  10/26/2005  -  Originally program
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer               Date                         Change Description                                                                           Project #          Development Project          Code Reviewer          Date Reviewed  
# MAGIC ------------------             ----------------------------     -----------------------------------------------------------------------                                   ----------------         ------------------------------------       ----------------------------      ----------------
# MAGIC Bhoomi Dasari     2008-09-09                   Added SrcSysCdSk parameter                                                          3567                 devlIDS

# MAGIC Set all foreign surragote keys
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
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DecimalType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/Logging
# COMMAND ----------

InFile = get_widget_value('InFile','IdsComsnArgmtExtr.tmp')
FinalOutFile = get_widget_value('FinalOutFile','COMSN_ARGMT.dat')
Logging = get_widget_value('Logging','$PROJDEF')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')

schema_IdsComsnArgmtExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38,10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("COMSN_ARGMT_SK", IntegerType(), nullable=False),
    StructField("COMSN_ARGMT_ID", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXTCN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXTCN_SK", IntegerType(), nullable=False),
    StructField("BEG_DT", StringType(), nullable=False),
    StructField("ARGMT_DESC", StringType(), nullable=False)
])

df_IdsComsnArgmtExtr = (
    spark.read.format("csv")
    .schema(schema_IdsComsnArgmtExtr)
    .option("header", "false")
    .option("quote", "\"")
    .load(f"{adls_path}/key/{InFile}")
)

df_foreignKey = (
    df_IdsComsnArgmtExtr
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("svBegDtSk", GetFkeyDate(F.lit("IDS"), F.col("COMSN_ARGMT_SK"), F.col("BEG_DT"), F.lit(Logging)))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("COMSN_ARGMT_SK")))
)

windowSpec = Window.orderBy(F.lit(1))
df_foreignKey = df_foreignKey.withColumn("rownum", F.row_number().over(windowSpec))

# Fkey Output
df_fkey = df_foreignKey.filter((F.col("ErrCount") == 0) | (F.col("PassThru") == 'Y'))
df_fkey = df_fkey.select(
    F.col("COMSN_ARGMT_SK"),
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("COMSN_ARGMT_ID"),
    F.col("CRT_RUN_CYC_EXTCN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXTCN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("svBegDtSk").alias("BEG_DT_SK"),
    F.col("ARGMT_DESC")
)

# Recycle Output
df_recycle = df_foreignKey.filter(F.col("ErrCount") > 0)
df_recycle = df_recycle.select(
    GetRecycleKey(F.col("COMSN_ARGMT_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD"),
    F.col("DISCARD_IN"),
    F.col("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ErrCount").alias("ERR_CT"),
    (F.col("RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("COMSN_ARGMT_SK"),
    F.col("COMSN_ARGMT_ID"),
    F.col("CRT_RUN_CYC_EXTCN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXTCN_SK"),
    F.col("BEG_DT"),
    F.col("ARGMT_DESC")
)

# Write Hashed File (Scenario C: write as parquet)
write_files(
    df_recycle,
    "hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# DefaultUNK Output
df_defUNK = df_foreignKey.filter(F.col("rownum") == 1).select(
    F.lit(0).alias("COMSN_ARGMT_SK"),
    F.lit(0).alias("SRC_SYS_CD_SK"),
    F.lit("UNK").alias("COMSN_ARGMT_ID"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit("UNK").alias("BEG_DT_SK"),
    F.lit("UNK").alias("ARGMT_DESC")
)

# DefaultNA Output
df_defNA = df_foreignKey.filter(F.col("rownum") == 1).select(
    F.lit(1).alias("COMSN_ARGMT_SK"),
    F.lit(1).alias("SRC_SYS_CD_SK"),
    F.lit("NA").alias("COMSN_ARGMT_ID"),
    F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit("NA").alias("BEG_DT_SK"),
    F.lit("NA").alias("ARGMT_DESC")
)

# Collector - union all inputs
df_collector = (
    df_fkey.unionByName(df_defUNK)
           .unionByName(df_defNA)
)

# Final select with rpad for char columns
df_final = df_collector.select(
    F.col("COMSN_ARGMT_SK"),
    F.col("SRC_SYS_CD_SK"),
    F.rpad(F.col("COMSN_ARGMT_ID"), 12, " ").alias("COMSN_ARGMT_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.rpad(F.col("BEG_DT_SK"), 10, " ").alias("BEG_DT_SK"),
    F.rpad(F.col("ARGMT_DESC"), 70, " ").alias("ARGMT_DESC")
)

write_files(
    df_final,
    f"{adls_path}/load/{FinalOutFile}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)