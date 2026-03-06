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
# MAGIC ^1_2 12/23/05 11:39:15 Batch  13872_41979 PROMOTE bckcett testIDS30 u10913 Move Income Commission to test
# MAGIC ^1_2 12/23/05 11:12:01 Batch  13872_40337 INIT bckcett devlIDS30 u10913 Ollie Move Income/Commisions to test
# MAGIC ^1_1 12/23/05 10:39:46 Batch  13872_38400 INIT bckcett devlIDS30 u10913 Ollie Moving Income / Commission to Test
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsComsnBillComsnFkey
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
# MAGIC                             
# MAGIC PROCESSING:  Natural keys are converted to surrogate keys
# MAGIC 
# MAGIC OUTPUTS:   BILL_COMSN Load file
# MAGIC                      hf_recycle - If recycling was used for this job records would be written to this hash file that had errors.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC             Oliver Nielsen  -  10/20/2005  -  Originally programmed
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer               Date                         Change Description                                                                           Project #          Development Project          Code Reviewer          Date Reviewed  
# MAGIC ------------------             ----------------------------     -----------------------------------------------------------------------                                   ----------------         ------------------------------------       ----------------------------      ----------------
# MAGIC Bhoomi Dasari     2008-09-09                   Added SrcSysCdSk parameter                                                          3567                 devlIDS                              Steph Goddard          09/22/2008

# MAGIC Set all foreign surrogate keys
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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
from pyspark.sql.functions import col, lit, rpad, trim
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


TmpOutFile = get_widget_value("TmpOutFile","")
InFile = get_widget_value("InFile","")
Logging = get_widget_value("Logging","")
SrcSysCdSk = get_widget_value("SrcSysCdSk","")

schema_IdsComsnBillComsn = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38,10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("BILL_COMSN_SK", IntegerType(), False),
    StructField("SRC_SYS_CD_SK", IntegerType(), False),
    StructField("BILL_ENTY_UNIQ_KEY", IntegerType(), False),
    StructField("CLS_PLN_ID", StringType(), False),
    StructField("FEE_DSCNT_ID", StringType(), False),
    StructField("SEQ_NO", IntegerType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("BILL_ENTY_SK", IntegerType(), False),
    StructField("CLS_PLN_SK", IntegerType(), False),
    StructField("COMSN_ARGMT", StringType(), False),
    StructField("FEE_DSCNT_SK", IntegerType(), False),
    StructField("EFF_DT_SK", StringType(), False),
    StructField("TERM_DT_SK", StringType(), False),
    StructField("PRM_PCT", DecimalType(38,10), False)
])

df_idscomsnbillcomsn = (
    spark.read
    .option("header", False)
    .option("sep", ",")
    .option("quote", "\"")
    .schema(schema_IdsComsnBillComsn)
    .csv(f"{adls_path}/key/{InFile}")
)

df_enriched = (
    df_idscomsnbillcomsn
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn("svBillEnty", GetFkeyBillEnty(col("SRC_SYS_CD"), col("BILL_COMSN_SK"), trim(col("BILL_ENTY_UNIQ_KEY")), lit(Logging)))
    .withColumn("svClsPln", GetFkeyClsPln(col("SRC_SYS_CD"), col("BILL_COMSN_SK"), col("CLS_PLN_ID"), lit(Logging)))
    .withColumn("svComsnArgmnt", GetFkeyComsnArgmt(col("SRC_SYS_CD"), col("BILL_COMSN_SK"), trim(col("COMSN_ARGMT")), lit(Logging)))
    .withColumn("svFeeDscnt", GetFkeyFeeDscnt(col("SRC_SYS_CD"), col("BILL_COMSN_SK"), col("FEE_DSCNT_ID"), lit(Logging)))
    .withColumn("ErrCount", GetFkeyErrorCnt(col("BILL_COMSN_SK")))
)

df_fkey = (
    df_enriched
    .filter((col("ErrCount") == lit(0)) | (col("PassThru") == lit("Y")))
    .select(
        col("BILL_COMSN_SK").alias("BILL_COMSN_SK"),
        lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        col("BILL_ENTY_UNIQ_KEY").alias("BILL_ENTY_UNIQ_KEY"),
        col("CLS_PLN_ID").alias("CLS_PLN_ID"),
        col("FEE_DSCNT_ID").alias("FEE_DSCNT_ID"),
        col("SEQ_NO").alias("SEQ_NO"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("svBillEnty").alias("BILL_ENTY_SK"),
        col("svClsPln").alias("CLS_PLN_SK"),
        col("svComsnArgmnt").alias("COMSN_ARGMT_SK"),
        col("svFeeDscnt").alias("FEE_DSCNT_SK"),
        col("EFF_DT_SK").alias("EFF_DT_SK"),
        col("TERM_DT_SK").alias("TERM_DT_SK"),
        col("PRM_PCT").alias("PRM_PCT")
    )
)

df_recycle = (
    df_enriched
    .filter(col("ErrCount") > lit(0))
    .select(
        col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
        col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        col("DISCARD_IN").alias("DISCARD_IN"),
        col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        col("ErrCount").alias("ERR_CT"),
        col("RECYCLE_CT").alias("RECYCLE_CT"),
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        col("BILL_COMSN_SK").alias("BILL_COMSN_SK"),
        col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        col("BILL_ENTY_UNIQ_KEY").alias("BILL_ENTY_UNIQ_KEY"),
        col("CLS_PLN_ID").alias("CLS_PLN_ID"),
        col("FEE_DSCNT_ID").alias("FEE_DSCNT_ID"),
        col("SEQ_NO").alias("SEQ_NO"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("BILL_ENTY_SK").alias("BILL_ENTY_SK"),
        col("CLS_PLN_SK").alias("CLS_PLN_SK"),
        col("COMSN_ARGMT").alias("COMSN_ARGMT"),
        col("FEE_DSCNT_SK").alias("FEE_DSCNT_SK"),
        col("EFF_DT_SK").alias("EFF_DT_SK"),
        col("TERM_DT_SK").alias("TERM_DT_SK"),
        col("PRM_PCT").alias("PRM_PCT")
    )
)

df_defaultUNK = spark.createDataFrame(
    [(0, 0, 0, 'UNK', 'UNK', 0, 0, 0, 0, 0, 0, 0, '0', '0', 0)],
    [
        "BILL_COMSN_SK","SRC_SYS_CD_SK","BILL_ENTY_UNIQ_KEY","CLS_PLN_ID","FEE_DSCNT_ID","SEQ_NO",
        "CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK","BILL_ENTY_SK","CLS_PLN_SK",
        "COMSN_ARGMT_SK","FEE_DSCNT_SK","EFF_DT_SK","TERM_DT_SK","PRM_PCT"
    ]
)

df_defaultNA = spark.createDataFrame(
    [(1, 1, 1, 'NA', 'NA', 1, 1, 1, 1, 1, 1, 1, '1', '1', 0)],
    [
        "BILL_COMSN_SK","SRC_SYS_CD_SK","BILL_ENTY_UNIQ_KEY","CLS_PLN_ID","FEE_DSCNT_ID","SEQ_NO",
        "CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK","BILL_ENTY_SK","CLS_PLN_SK",
        "COMSN_ARGMT_SK","FEE_DSCNT_SK","EFF_DT_SK","TERM_DT_SK","PRM_PCT"
    ]
)

df_collector = df_fkey.unionByName(df_defaultUNK).unionByName(df_defaultNA)

df_collector_final = df_collector.select(
    "BILL_COMSN_SK","SRC_SYS_CD_SK","BILL_ENTY_UNIQ_KEY","CLS_PLN_ID","FEE_DSCNT_ID",
    "SEQ_NO","CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK","BILL_ENTY_SK","CLS_PLN_SK",
    "COMSN_ARGMT_SK","FEE_DSCNT_SK","EFF_DT_SK","TERM_DT_SK","PRM_PCT"
)

df_collector_final = (
    df_collector_final
    .withColumn("CLS_PLN_ID", rpad(col("CLS_PLN_ID"), 20, " "))
    .withColumn("FEE_DSCNT_ID", rpad(col("FEE_DSCNT_ID"), lit(<...>), " "))
    .withColumn("EFF_DT_SK", rpad(col("EFF_DT_SK"), 10, " "))
    .withColumn("TERM_DT_SK", rpad(col("TERM_DT_SK"), 10, " "))
)

df_recycle_final = df_recycle.select(
    "JOB_EXCTN_RCRD_ERR_SK","INSRT_UPDT_CD","DISCARD_IN","PASS_THRU_IN","FIRST_RECYC_DT","ERR_CT",
    "RECYCLE_CT","SRC_SYS_CD","PRI_KEY_STRING","BILL_COMSN_SK","SRC_SYS_CD_SK","BILL_ENTY_UNIQ_KEY",
    "CLS_PLN_ID","FEE_DSCNT_ID","SEQ_NO","CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK",
    "BILL_ENTY_SK","CLS_PLN_SK","COMSN_ARGMT","FEE_DSCNT_SK","EFF_DT_SK","TERM_DT_SK","PRM_PCT"
)

df_recycle_final = (
    df_recycle_final
    .withColumn("INSRT_UPDT_CD", rpad(col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", rpad(col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", rpad(col("PASS_THRU_IN"), 1, " "))
    .withColumn("SRC_SYS_CD", rpad(col("SRC_SYS_CD"), lit(<...>), " "))
    .withColumn("CLS_PLN_ID", rpad(col("CLS_PLN_ID"), 20, " "))
    .withColumn("COMSN_ARGMT", rpad(col("COMSN_ARGMT"), 12, " "))
    .withColumn("EFF_DT_SK", rpad(col("EFF_DT_SK"), 10, " "))
    .withColumn("TERM_DT_SK", rpad(col("TERM_DT_SK"), 10, " "))
    .withColumn("FEE_DSCNT_ID", rpad(col("FEE_DSCNT_ID"), lit(<...>), " "))
    .withColumn("PRI_KEY_STRING", rpad(col("PRI_KEY_STRING"), lit(<...>), " "))
)

write_files(
    df_recycle_final,
    f"{adls_path}/hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

write_files(
    df_collector_final,
    f"{adls_path}/load/{TmpOutFile}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)