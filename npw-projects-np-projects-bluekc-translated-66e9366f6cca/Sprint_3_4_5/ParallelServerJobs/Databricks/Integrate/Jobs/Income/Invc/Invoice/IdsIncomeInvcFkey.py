# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_3 01/14/09 10:18:26 Batch  14990_37109 PROMOTE bckcetl ids20 dsadm bls for sg
# MAGIC ^1_3 01/14/09 10:10:44 Batch  14990_36646 INIT bckcett testIDS dsadm BLS FOR SG
# MAGIC ^1_1 12/16/08 22:11:05 Batch  14961_79874 PROMOTE bckcett testIDS u03651 steph - income primary key conversion
# MAGIC ^1_1 12/16/08 22:03:32 Batch  14961_79415 INIT bckcett devlIDS u03651 steffy
# MAGIC ^1_2 11/12/07 10:00:11 Batch  14561_36015 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_2 11/02/07 13:07:35 Batch  14551_47275 PROMOTE bckcetl ids20 dsadm bls for on
# MAGIC ^1_2 11/02/07 12:52:45 Batch  14551_46368 INIT bckcett testIDS30 dsadm bls for on
# MAGIC ^1_1 10/31/07 13:17:35 Batch  14549_47857 PROMOTE bckcett testIDS30 u03651 steffy
# MAGIC ^1_1 10/31/07 13:09:37 Batch  14549_47387 INIT bckcett devlIDS30 u03651 steffy
# MAGIC ^1_2 06/07/07 15:08:56 Batch  14403_54540 PROMOTE bckcett devlIDS30 u10157 sa
# MAGIC ^1_2 06/07/07 15:06:33 Batch  14403_54395 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_5 05/23/07 13:53:34 Batch  14388_50029 PROMOTE bckcetl ids20 dsadm bls for sa
# MAGIC ^1_5 05/23/07 13:45:45 Batch  14388_49559 INIT bckcett testIDS30 dsadm bls for sa
# MAGIC ^1_1 05/22/07 17:32:19 Batch  14387_63143 PROMOTE bckcett testIDS30 u10157 sa
# MAGIC ^1_1 05/22/07 17:31:27 Batch  14387_63088 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_2 03/05/07 16:45:23 Batch  14309_60325 PROMOTE bckcett devlIDS30 u10157 sa
# MAGIC ^1_2 03/05/07 16:34:28 Batch  14309_59670 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_1 02/11/07 12:33:13 Batch  14287_45195 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_4 12/18/06 12:24:28 Batch  14232_44709 INIT bckcetl ids20 dsadm Income Backup for 12/18/2006 install
# MAGIC ^1_1 11/03/06 13:03:29 Batch  14187_47011 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_3 05/22/06 11:41:30 Batch  14022_42099 PROMOTE bckcetl ids20 dsadm Jim Hart
# MAGIC ^1_3 05/22/06 11:21:03 Batch  14022_40870 INIT bckcett testIDS30 dsadm Jim Hart
# MAGIC ^1_2 05/22/06 11:13:00 Batch  14022_40388 INIT bckcett testIDS30 dsadm Jim Hart
# MAGIC ^1_1 05/22/06 10:32:59 Batch  14022_37985 INIT bckcett testIDS30 dsadm Jim Hart
# MAGIC ^1_5 05/12/06 15:17:06 Batch  14012_55029 PROMOTE bckcett testIDS30 u10157 sa
# MAGIC ^1_5 05/12/06 15:03:19 Batch  14012_54207 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_4 05/11/06 16:20:33 Batch  14011_58841 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_3 05/10/06 16:34:22 Batch  14010_59666 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_2 05/10/06 16:00:21 Batch  14010_57626 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_1 05/02/06 14:21:23 Batch  14002_51686 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_1 05/01/06 11:14:49 Batch  14001_45485 PROMOTE bckcett devlIDS30 u10157 sa
# MAGIC ^1_1 05/01/06 11:11:02 Batch  14001_40263 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_2 01/26/06 07:54:36 Batch  13906_28483 INIT bckcetl ids20 dsadm Gina
# MAGIC ^1_1 01/24/06 14:49:28 Batch  13904_53375 PROMOTE bckcetl ids20 dsadm Gina Parr
# MAGIC ^1_1 01/24/06 14:47:17 Batch  13904_53242 INIT bckcett testIDS30 dsadm Gina Parr
# MAGIC ^1_2 01/24/06 14:36:07 Batch  13904_52574 INIT bckcett testIDS30 dsadm Gina Parr
# MAGIC ^1_1 01/24/06 14:21:30 Batch  13904_51695 INIT bckcett testIDS30 dsadm Gina Parr
# MAGIC ^1_2 12/23/05 11:39:15 Batch  13872_41979 PROMOTE bckcett testIDS30 u10913 Move Income Commission to test
# MAGIC ^1_2 12/23/05 11:12:01 Batch  13872_40337 INIT bckcett devlIDS30 u10913 Ollie Move Income/Commisions to test
# MAGIC ^1_1 12/23/05 10:39:46 Batch  13872_38400 INIT bckcett devlIDS30 u10913 Ollie Moving Income / Commission to Test
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsInvcFkey
# MAGIC 
# MAGIC DESCRIPTION:     Takes the sorted file and applies the foreign keys.   
# MAGIC       
# MAGIC 
# MAGIC INPUTS:   File created by IdsInvcExtr
# MAGIC 	
# MAGIC   
# MAGIC HASH FILES:     hf_recycle
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  Fkey lookups
# MAGIC                            
# MAGIC 
# MAGIC PROCESSING:   Finds foreign keys for surrogate key fields
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC             Ralph Tucker   10/13/2005  -   Originally Programmed
# MAGIC        
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer               Date                         Change Description                                                                           Project #          Development Project          Code Reviewer          Date Reviewed  
# MAGIC ------------------             ----------------------------     -----------------------------------------------------------------------                                   ----------------         ------------------------------------       ----------------------------      ----------------
# MAGIC Bhoomi Dasari     2008-09-26                   Added SrcSysCdSk parameter                                                          3567                 devlIDS                             Steph Goddard           10/03/2008
# MAGIC 
# MAGIC 
# MAGIC Goutham Kalidindi  2021-03-24                       Changed Datatype length for field                                            258186                IntegrateDev1                  Reddy Sanam             04/01/2021
# MAGIC                                                                         BLIV_ID
# MAGIC                                                                           char(12) to Varchar(15)

# MAGIC Read common record format file.
# MAGIC Writing Sequential File to /load
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, TimestampType
from pyspark.sql.functions import col, lit, rpad, when
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


InFile = get_widget_value('InFile','INVC.Tmp')
Logging = get_widget_value('Logging','Y')
OutFile = get_widget_value('OutFile','INVC.dat')
SrcSysCdSk = get_widget_value('SrcSysCdSk','105838')

schema_IdsInvc = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38,10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("INVC_SK", IntegerType(), nullable=False),
    StructField("BILL_INVC_ID", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("BILL_ENTY", IntegerType(), nullable=False),
    StructField("INVC_TYPE_CD", StringType(), nullable=False),
    StructField("BILL_DUE_DT", StringType(), nullable=False),
    StructField("BILL_END_DT", StringType(), nullable=False),
    StructField("CRT_DT", StringType(), nullable=False),
    StructField("CUR_RCRD_IN", StringType(), nullable=False)
])

df_IdsInvc = (
    spark.read.format("csv")
    .schema(schema_IdsInvc)
    .option("quote", "\"")
    .option("header", "false")
    .option("delimiter", ",")
    .load(f"{adls_path}/key/{InFile}")
)

df_enriched = (
    df_IdsInvc
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn("BillEnty", GetFkeyBillEnty(col("SRC_SYS_CD"), col("INVC_SK"), col("BILL_ENTY"), Logging))
    .withColumn("InvcTypCd", GetFkeyCodes(col("SRC_SYS_CD"), col("INVC_SK"), lit("INVOICE TYPE"), col("INVC_TYPE_CD"), Logging))
    .withColumn("BillDueDt", GetFkeyDate(lit("IDS"), col("INVC_SK"), col("BILL_DUE_DT"), Logging))
    .withColumn("BillEndDt", GetFkeyDate(lit("IDS"), col("INVC_SK"), col("BILL_END_DT"), Logging))
    .withColumn("CrtDt", GetFkeyDate(lit("IDS"), col("INVC_SK"), col("CRT_DT"), Logging))
    .withColumn("ErrCount", GetFkeyErrorCnt(col("INVC_SK")))
)

df_fkey = (
    df_enriched
    .filter((col("ErrCount") == 0) | (col("PassThru") == "Y"))
    .select(
        col("INVC_SK").alias("INVC_SK"),
        lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        col("BILL_INVC_ID").alias("BILL_INVC_ID"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("BillEnty").alias("BILL_ENTY_SK"),
        col("InvcTypCd").alias("INVC_TYP_CD_SK"),
        col("CUR_RCRD_IN").alias("CUR_RCRD_IN"),
        col("BillDueDt").alias("BILL_DUE_DT_SK"),
        col("BillEndDt").alias("BILL_END_DT_SK"),
        col("CrtDt").alias("CRT_DT_SK")
    )
)

df_defaultUNK_base = df_enriched.limit(1)
df_defaultUNK = df_defaultUNK_base.select(
    lit(0).alias("INVC_SK"),
    lit(0).alias("SRC_SYS_CD_SK"),
    lit("UNK").alias("BILL_INVC_ID"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("BILL_ENTY_SK"),
    lit(0).alias("INVC_TYP_CD_SK"),
    lit("U").alias("CUR_RCRD_IN"),
    lit("UNK").alias("BILL_DUE_DT_SK"),
    lit("UNK").alias("BILL_END_DT_SK"),
    lit("UNK").alias("CRT_DT_SK")
)

df_defaultNA_base = df_enriched.limit(1)
df_defaultNA = df_defaultNA_base.select(
    lit(1).alias("INVC_SK"),
    lit(1).alias("SRC_SYS_CD_SK"),
    lit("NA").alias("BILL_INVC_ID"),
    lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(1).alias("BILL_ENTY_SK"),
    lit(1).alias("INVC_TYP_CD_SK"),
    lit("X").alias("CUR_RCRD_IN"),
    lit("NA").alias("BILL_DUE_DT_SK"),
    lit("NA").alias("BILL_END_DT_SK"),
    lit("NA").alias("CRT_DT_SK")
)

df_recycle = df_enriched.filter(col("ErrCount") > 0)

df_recycle_out = (
    df_recycle
    .select(
        GetRecycleKey(col("INVC_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        col("DISCARD_IN").alias("DISCARD_IN"),
        col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        col("ErrCount").alias("ERR_CT"),
        col("RECYCLE_CT").alias("RECYCLE_CT"),
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        col("INVC_SK").alias("INVC_SK"),
        col("BILL_INVC_ID").alias("BILL_INVC_ID"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("BILL_ENTY").alias("BILL_ENTY"),
        col("INVC_TYPE_CD").alias("INVC_TYPE_CD"),
        col("CUR_RCRD_IN").alias("CUR_RCRD_IN"),
        col("BILL_DUE_DT").alias("BILL_DUE_DT"),
        col("BILL_END_DT").alias("BILL_END_DT"),
        col("CRT_DT").alias("CRT_DT")
    )
    .withColumn("INSRT_UPDT_CD", rpad(col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", rpad(col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", rpad(col("PASS_THRU_IN"), 1, " "))
    .withColumn("BILL_INVC_ID", rpad(col("BILL_INVC_ID"), 12, " "))
    .withColumn("INVC_TYPE_CD", rpad(col("INVC_TYPE_CD"), 1, " "))
    .withColumn("CUR_RCRD_IN", rpad(col("CUR_RCRD_IN"), 1, " "))
    .withColumn("BILL_DUE_DT", rpad(col("BILL_DUE_DT"), 10, " "))
    .withColumn("BILL_END_DT", rpad(col("BILL_END_DT"), 10, " "))
    .withColumn("CRT_DT", rpad(col("CRT_DT"), 10, " "))
    .select(
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "PASS_THRU_IN",
        "FIRST_RECYC_DT",
        "ERR_CT",
        "RECYCLE_CT",
        "SRC_SYS_CD",
        "PRI_KEY_STRING",
        "INVC_SK",
        "BILL_INVC_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "BILL_ENTY",
        "INVC_TYPE_CD",
        "CUR_RCRD_IN",
        "BILL_DUE_DT",
        "BILL_END_DT",
        "CRT_DT"
    )
)

write_files(
    df_recycle_out,
    "hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    False,
    "\"",
    None
)

df_collector = df_fkey.unionByName(df_defaultUNK).unionByName(df_defaultNA)

df_out = (
    df_collector
    .withColumn("CUR_RCRD_IN", rpad(col("CUR_RCRD_IN"), 1, " "))
    .withColumn("BILL_DUE_DT_SK", rpad(col("BILL_DUE_DT_SK"), 10, " "))
    .withColumn("BILL_END_DT_SK", rpad(col("BILL_END_DT_SK"), 10, " "))
    .withColumn("CRT_DT_SK", rpad(col("CRT_DT_SK"), 10, " "))
    .select(
        "INVC_SK",
        "SRC_SYS_CD_SK",
        "BILL_INVC_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "BILL_ENTY_SK",
        "INVC_TYP_CD_SK",
        "CUR_RCRD_IN",
        "BILL_DUE_DT_SK",
        "BILL_END_DT_SK",
        "CRT_DT_SK"
    )
)

write_files(
    df_out,
    f"{adls_path}/load/{OutFile}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)