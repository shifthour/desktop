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
# MAGIC ^1_1 12/14/05 12:18:44 Batch  13863_44334 INIT bckcett devlIDS30 u11141 Hugh Sisson
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:     IdsAgntRelshpFkey
# MAGIC 
# MAGIC DESCRIPTION:     Takes the sorted file and applies the primary key.   Preps the file for the foreign key lookup process that follows.
# MAGIC      
# MAGIC 
# MAGIC INPUTS:  Primary Key output file
# MAGIC 
# MAGIC  
# MAGIC HASH FILES:     hf_recycle
# MAGIC 
# MAGIC TRANSFORMS:  GetFkeyCodes
# MAGIC                             GetFkeyDates
# MAGIC                             GetFkeyAgnt
# MAGIC                             GetFkeyErrorCnt
# MAGIC 
# MAGIC PROCESSING:  Natural keys are converted to surrogate keys
# MAGIC 
# MAGIC OUTPUTS:   AGNT_RELSHP.dat
# MAGIC                      hf_recycle - If recycling was used for this job records would be written to this hash file that had errors.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC             Hugh Sisson  -  10/11/2005  -  Originally programmed
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer               Date                         Change Description                                                                           Project #          Development Project          Code Reviewer          Date Reviewed  
# MAGIC ------------------             ----------------------------     -----------------------------------------------------------------------                                   ----------------         ------------------------------------       ----------------------------      ----------------
# MAGIC Bhoomi Dasari     2008-09-05                   Added SrcSysCdSk parameter                                                          3567                 devlIDS                              Steph Goddard          09/22/2008

# MAGIC Set all foreign surragote keys
# MAGIC Merge source data with default rows
# MAGIC Writing Sequential File to /load
# MAGIC Read common record format file from extract job.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    TimestampType,
    DecimalType
)
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


OutFile = get_widget_value('OutFile','AGNT_RELSHP.dat')
Logging = get_widget_value('Logging','N')
InFile = get_widget_value('InFile','FctsAgntRelshpExtr.tmp')
SrcSysCdSk = get_widget_value('SrcSysCdSk','105838')

schema_IdsAgntRelshpExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(10, 0), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("AGNT_RELSHP_SK", IntegerType(), nullable=False),
    StructField("AGNT_ID", StringType(), nullable=False),
    StructField("REL_AGNT_ID", StringType(), nullable=False),
    StructField("EFF_DT", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("AGNT", IntegerType(), nullable=False),
    StructField("REL_AGNT", IntegerType(), nullable=False),
    StructField("AGNT_RELSHP_TERM_RSN_CD", StringType(), nullable=False),
    StructField("TERM_DT", StringType(), nullable=False)
])

dfKey = (
    spark.read
    .option("header", "false")
    .option("quote", "\"")
    .option("delimiter", ",")
    .schema(schema_IdsAgntRelshpExtr)
    .csv(f"{adls_path}/key/{InFile}")
)

dfForeignKey = (
    dfKey
    .withColumn("svEffDtSk", GetFkeyDate(F.lit("IDS"), F.col("AGNT_RELSHP_SK"), F.col("EFF_DT"), F.lit(Logging)))
    .withColumn("svAgntSk", GetFkeyAgnt(F.col("SRC_SYS_CD"), F.col("AGNT_RELSHP_SK"), F.col("AGNT_ID"), F.lit(Logging)))
    .withColumn("svRelAgntSk", GetFkeyAgnt(F.col("SRC_SYS_CD"), F.col("AGNT_RELSHP_SK"), F.col("REL_AGNT"), F.lit(Logging)))
    .withColumn(
        "svAgntRelshpTermRsnCdSk",
        GetFkeyCodes(
            F.col("SRC_SYS_CD"),
            F.col("AGNT_RELSHP_SK"),
            F.lit("AGENT RELATIONSHIP TERMINATION REASON"),
            F.col("AGNT_RELSHP_TERM_RSN_CD"),
            F.lit(Logging)
        )
    )
    .withColumn("svTermDtSk", GetFkeyDate(F.lit("IDS"), F.col("AGNT_RELSHP_SK"), F.col("TERM_DT"), F.lit(Logging)))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("AGNT_RELSHP_SK")))
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
)

dfFkey = (
    dfForeignKey
    .filter((F.col("ErrCount") == 0) | (F.col("PassThru") == 'Y'))
    .select(
        F.col("AGNT_RELSHP_SK").alias("AGNT_RELSHP_SK"),
        F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        F.col("AGNT_ID").alias("AGNT_ID"),
        F.col("REL_AGNT_ID").alias("REL_AGNT_ID"),
        F.col("svEffDtSk").alias("EFF_DT_SK"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("svAgntSk").alias("AGNT_SK"),
        F.col("svRelAgntSk").alias("REL_AGNT_SK"),
        F.col("svAgntRelshpTermRsnCdSk").alias("AGNT_RELSHP_TERM_RSN_CD_SK"),
        F.col("svTermDtSk").alias("TERM_DT_SK")
    )
)

dfRecycle = (
    dfForeignKey
    .filter(F.col("ErrCount") > 0)
    .select(
        GetRecycleKey(F.col("AGNT_RELSHP_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("DISCARD_IN").alias("DISCARD_IN"),
        F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("ErrCount").alias("ERR_CT"),
        (F.col("RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("AGNT_RELSHP_SK").alias("AGNT_RELSHP_SK"),
        F.col("AGNT_ID").alias("AGNT_ID"),
        F.col("REL_AGNT_ID").alias("REL_AGNT_ID"),
        F.col("EFF_DT").alias("EFF_DT"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("AGNT").alias("AGNT"),
        F.col("REL_AGNT").alias("REL_AGNT"),
        F.col("AGNT_RELSHP_TERM_RSN_CD").alias("AGNT_RELSHP_TERM_RSN_CD"),
        F.col("TERM_DT").alias("TERM_DT")
    )
)

write_files(
    dfRecycle,
    "hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

w = Window.orderBy(F.lit(1))
dfForeignKeyWithRowNum = dfForeignKey.withColumn("_row_num_", F.row_number().over(w))

dfDefaultUNK_source = dfForeignKeyWithRowNum.filter(F.col("_row_num_") == 1)
dfDefaultUNK = dfDefaultUNK_source.select(
    F.lit(0).alias("AGNT_RELSHP_SK"),
    F.lit(0).alias("SRC_SYS_CD_SK"),
    F.lit("UNK").alias("AGNT_ID"),
    F.lit("UNK").alias("REL_AGNT_ID"),
    F.lit("UNK").alias("EFF_DT_SK"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("AGNT_SK"),
    F.lit(0).alias("REL_AGNT_SK"),
    F.lit(0).alias("AGNT_RELSHP_TERM_RSN_CD_SK"),
    F.lit("UNK").alias("TERM_DT_SK")
)

dfDefaultNA_source = dfForeignKeyWithRowNum.filter(F.col("_row_num_") == 1)
dfDefaultNA = dfDefaultNA_source.select(
    F.lit(1).alias("AGNT_RELSHP_SK"),
    F.lit(1).alias("SRC_SYS_CD_SK"),
    F.lit("NA").alias("AGNT_ID"),
    F.lit("NA").alias("REL_AGNT_ID"),
    F.lit("NA").alias("EFF_DT_SK"),
    F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("AGNT_SK"),
    F.lit(1).alias("REL_AGNT_SK"),
    F.lit(1).alias("AGNT_RELSHP_TERM_RSN_CD_SK"),
    F.lit("NA").alias("TERM_DT_SK")
)

dfCollector = dfFkey.unionByName(dfDefaultUNK).unionByName(dfDefaultNA)

dfCollector = (
    dfCollector
    .withColumn("EFF_DT_SK", F.rpad("EFF_DT_SK", 10, " "))
    .withColumn("TERM_DT_SK", F.rpad("TERM_DT_SK", 10, " "))
    .select(
        "AGNT_RELSHP_SK",
        "SRC_SYS_CD_SK",
        "AGNT_ID",
        "REL_AGNT_ID",
        "EFF_DT_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "AGNT_SK",
        "REL_AGNT_SK",
        "AGNT_RELSHP_TERM_RSN_CD_SK",
        "TERM_DT_SK"
    )
)

write_files(
    dfCollector,
    f"{adls_path}/load/{OutFile}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)