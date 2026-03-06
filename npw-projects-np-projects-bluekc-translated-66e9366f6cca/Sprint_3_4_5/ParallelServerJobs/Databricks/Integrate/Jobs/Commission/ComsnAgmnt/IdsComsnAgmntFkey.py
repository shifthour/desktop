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
# MAGIC JOB NAME:     IdsComsnAgmntFkey
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
# MAGIC                             GetFkeyAgnt
# MAGIC                             GetFkeyComsnArngmt
# MAGIC                             GetFkeyComsnSchd
# MAGIC                             GetFkeyDate
# MAGIC                             GetFkeyErrorCnt
# MAGIC 
# MAGIC PROCESSING:  Natural keys are converted to surrogate keys
# MAGIC 
# MAGIC OUTPUTS:   CLS table load file
# MAGIC                      hf_recycle - If recycling was used for this job records would be written to this hash file that had errors.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC             Hugh Sisson - 11/2/05  -  Original program
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer               Date                         Change Description                                                                           Project #          Development Project          Code Reviewer          Date Reviewed  
# MAGIC ------------------             ----------------------------     -----------------------------------------------------------------------                                   ----------------         ------------------------------------       ----------------------------      ----------------
# MAGIC Bhoomi Dasari     2008-09-09                   Added SrcSysCdSk parameter                                                          3567                 devlIDS                              Steph Goddard          09/22/2008

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
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, TimestampType
from pyspark.sql.functions import col, lit, row_number, rpad
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


InFile = get_widget_value('InFile','')
OutFile = get_widget_value('OutFile','')
Logging = get_widget_value('Logging','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')

schema_IdsComsnAgmntExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(10, 0), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("COMSN_AGMNT_SK", IntegerType(), False),
    StructField("COMSN_ARGMT_ID", StringType(), False),
    StructField("EFF_DT", StringType(), False),
    StructField("AGNT_ID", StringType(), False),
    StructField("CRT_RUN_CYC_EXTCN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXTCN_SK", IntegerType(), False),
    StructField("AGNT", StringType(), False),
    StructField("COMSN_ARGMT", StringType(), False),
    StructField("COMSN_SCHD", StringType(), False),
    StructField("COMSN_AGMNT_PT_OF_SCHD_CD", StringType(), False),
    StructField("COMSN_AGMNT_TERM_RSN_CD", StringType(), False),
    StructField("TERM_DT", StringType(), False),
    StructField("SCHD_FCTR", IntegerType(), False)
])

df_IdsComsnAgmntExtr = (
    spark.read.format("csv")
    .option("header", "false")
    .option("sep", ",")
    .option("quote", "\"")
    .schema(schema_IdsComsnAgmntExtr)
    .load(f"{adls_path}/key/{InFile}")
)

df_ForeignKey = (
    df_IdsComsnAgmntExtr
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn("svAgntSk", GetFkeyAgnt(col("SRC_SYS_CD"), col("COMSN_AGMNT_SK"), col("AGNT"), Logging))
    .withColumn("svEffDtSk", GetFkeyDate(lit("IDS"), col("COMSN_AGMNT_SK"), col("EFF_DT"), Logging))
    .withColumn("svTermDtSk", GetFkeyDate(lit("IDS"), col("COMSN_AGMNT_SK"), col("TERM_DT"), Logging))
    .withColumn("svComsnArgmtSk", GetFkeyComsnArgmt(col("SRC_SYS_CD"), col("COMSN_AGMNT_SK"), col("COMSN_ARGMT_ID"), Logging))
    .withColumn("svComsnSchdSk", GetFkeyComsnSchd(col("SRC_SYS_CD"), col("COMSN_AGMNT_SK"), col("COMSN_SCHD"), Logging))
    .withColumn("svComsnAgmntPtOfSchdCd", GetFkeyCodes(col("SRC_SYS_CD"), col("COMSN_AGMNT_SK"), lit("COMMISSION AGREEMENT POINT OF SCHEDULE"), col("COMSN_AGMNT_PT_OF_SCHD_CD"), Logging))
    .withColumn("svComsnAgmntTermRsn", GetFkeyCodes(col("SRC_SYS_CD"), col("COMSN_AGMNT_SK"), lit("COMMISSION AGREEMENT TERMINATION REASON"), col("COMSN_AGMNT_TERM_RSN_CD"), Logging))
    .withColumn("ErrCount", GetFkeyErrorCnt(col("COMSN_AGMNT_SK")))
)

df_ForeignKey_Fkey = (
    df_ForeignKey
    .filter((col("ErrCount") == lit(0)) | (col("PassThru") == lit("Y")))
    .withColumn("SRC_SYS_CD_SK", lit(SrcSysCdSk))
    .select(
        col("COMSN_AGMNT_SK").alias("COMSN_AGMNT_SK"),
        col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        col("COMSN_ARGMT_ID").alias("COMSN_ARGMT_ID"),
        col("svEffDtSk").alias("EFF_DT_SK"),
        col("AGNT_ID").alias("AGNT_ID"),
        col("CRT_RUN_CYC_EXTCN_SK").alias("CRT_RUN_CYC_EXTCN_SK"),
        col("LAST_UPDT_RUN_CYC_EXTCN_SK").alias("LAST_UPDT_RUN_CYC_EXTCN_SK"),
        col("svAgntSk").alias("AGNT_SK"),
        col("svComsnArgmtSk").alias("COMSN_ARGMT_SK"),
        col("svComsnSchdSk").alias("COMSN_SCHD_SK"),
        col("svComsnAgmntPtOfSchdCd").alias("COMSN_AGMNT_PT_OF_SCHD_CD_SK"),
        col("svComsnAgmntTermRsn").alias("COMSN_AGMNT_TERM_RSN_CD_SK"),
        col("svTermDtSk").alias("TERM_DT_SK"),
        col("SCHD_FCTR").alias("SCHD_FCTR")
    )
)

df_ForeignKey_Recycle = df_ForeignKey.filter(col("ErrCount") > lit(0))
df_ForeignKey_Recycle = (
    df_ForeignKey_Recycle
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", GetRecycleKey(col("COMSN_AGMNT_SK")))
    .withColumn("RECYCLE_CT", col("RECYCLE_CT") + lit(1))
    .select(
        col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
        rpad(col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
        rpad(col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
        rpad(col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
        col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        col("ErrCount").alias("ERR_CT"),
        col("RECYCLE_CT").alias("RECYCLE_CT"),
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        col("COMSN_AGMNT_SK").alias("COMSN_AGMNT_SK"),
        rpad(col("COMSN_ARGMT_ID"), 12, " ").alias("COMSN_ARGMT_ID"),
        rpad(col("EFF_DT"), 10, " ").alias("EFF_DT"),
        rpad(col("AGNT_ID"), 10, " ").alias("AGNT_ID"),
        col("CRT_RUN_CYC_EXTCN_SK").alias("CRT_RUN_CYC_EXTCN_SK"),
        col("LAST_UPDT_RUN_CYC_EXTCN_SK").alias("LAST_UPDT_RUN_CYC_EXTCN_SK"),
        rpad(col("AGNT"), 12, " ").alias("AGNT"),
        rpad(col("COMSN_ARGMT"), 12, " ").alias("COMSN_ARGMT"),
        rpad(col("COMSN_SCHD"), 4, " ").alias("COMSN_SCHD"),
        rpad(col("COMSN_AGMNT_PT_OF_SCHD_CD"), 1, " ").alias("COMSN_AGMNT_PT_OF_SCHD_CD"),
        rpad(col("COMSN_AGMNT_TERM_RSN_CD"), 4, " ").alias("COMSN_AGMNT_TERM_RSN_CD"),
        rpad(col("TERM_DT"), 10, " ").alias("TERM_DT"),
        col("SCHD_FCTR").alias("SCHD_FCTR")
    )
)

write_files(
    df_ForeignKey_Recycle,
    f"{adls_path}/hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_ForeignKey_withRowNum = df_ForeignKey.withColumn("_rownum", row_number().over(Window.orderBy(lit(1))))

df_ForeignKey_DefaultUNK = (
    df_ForeignKey_withRowNum
    .filter(col("_rownum") == 1)
    .select(
        lit(0).alias("COMSN_AGMNT_SK"),
        lit(0).alias("SRC_SYS_CD_SK"),
        rpad(lit("UNK"), 12, " ").alias("COMSN_ARGMT_ID"),
        rpad(lit("UNK"), 10, " ").alias("EFF_DT_SK"),
        rpad(lit("UNK"), 10, " ").alias("AGNT_ID"),
        lit(0).alias("CRT_RUN_CYC_EXTCN_SK"),
        lit(0).alias("LAST_UPDT_RUN_CYC_EXTCN_SK"),
        lit(0).alias("AGNT_SK"),
        lit(0).alias("COMSN_ARGMT_SK"),
        lit(0).alias("COMSN_SCHD_SK"),
        lit(0).alias("COMSN_AGMNT_PT_OF_SCHD_CD_SK"),
        lit(0).alias("COMSN_AGMNT_TERM_RSN_CD_SK"),
        rpad(lit("UNK"), 10, " ").alias("TERM_DT_SK"),
        lit(0).alias("SCHD_FCTR")
    )
)

df_ForeignKey_DefaultNA = (
    df_ForeignKey_withRowNum
    .filter(col("_rownum") == 1)
    .select(
        lit(1).alias("COMSN_AGMNT_SK"),
        lit(1).alias("SRC_SYS_CD_SK"),
        rpad(lit("NA"), 12, " ").alias("COMSN_ARGMT_ID"),
        rpad(lit("NA"), 10, " ").alias("EFF_DT_SK"),
        rpad(lit("NA"), 10, " ").alias("AGNT_ID"),
        lit(1).alias("CRT_RUN_CYC_EXTCN_SK"),
        lit(1).alias("LAST_UPDT_RUN_CYC_EXTCN_SK"),
        lit(1).alias("AGNT_SK"),
        lit(1).alias("COMSN_ARGMT_SK"),
        lit(1).alias("COMSN_SCHD_SK"),
        lit(1).alias("COMSN_AGMNT_PT_OF_SCHD_CD_SK"),
        lit(1).alias("COMSN_AGMNT_TERM_RSN_CD_SK"),
        rpad(lit("NA"), 10, " ").alias("TERM_DT_SK"),
        lit(1).alias("SCHD_FCTR")
    )
)

df_CollectorInput = (
    df_ForeignKey_Fkey
    .unionByName(df_ForeignKey_DefaultUNK)
    .unionByName(df_ForeignKey_DefaultNA)
)

df_Collector = df_CollectorInput.select(
    col("COMSN_AGMNT_SK").alias("COMSN_AGMNT_SK"),
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    rpad(col("COMSN_ARGMT_ID"), 12, " ").alias("COMSN_ARGMT_ID"),
    rpad(col("EFF_DT_SK"), 10, " ").alias("EFF_DT_SK"),
    rpad(col("AGNT_ID"), 10, " ").alias("AGNT_ID"),
    col("CRT_RUN_CYC_EXTCN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXTCN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("AGNT_SK").alias("AGNT_SK"),
    col("COMSN_ARGMT_SK").alias("COMSN_ARGMT_SK"),
    col("COMSN_SCHD_SK").alias("COMSN_SCHD_SK"),
    col("COMSN_AGMNT_PT_OF_SCHD_CD_SK").alias("COMSN_AGMNT_PT_OF_SCHD_CD_SK"),
    col("COMSN_AGMNT_TERM_RSN_CD_SK").alias("COMSN_AGMNT_TERM_RSN_CD_SK"),
    rpad(col("TERM_DT_SK"), 10, " ").alias("TERM_DT_SK"),
    col("SCHD_FCTR").alias("SCHD_FCTR")
)

write_files(
    df_Collector,
    f"{adls_path}/load/{OutFile}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)