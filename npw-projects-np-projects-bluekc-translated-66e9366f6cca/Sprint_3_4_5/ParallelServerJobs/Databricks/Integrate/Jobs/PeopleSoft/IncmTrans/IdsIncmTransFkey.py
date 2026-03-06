# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 02/06/09 14:24:00 Batch  15013_51863 PROMOTE bckcetl ids20 dsadm rc for steph 
# MAGIC ^1_1 02/06/09 14:02:28 Batch  15013_50566 INIT bckcett testIDS dsadm rc for steph 
# MAGIC ^1_2 01/23/09 15:14:34 Batch  14999_54898 PROMOTE bckcett testIDS u03651 steph - primary key
# MAGIC ^1_2 01/23/09 14:36:26 Batch  14999_52588 INIT bckcett devlIDS u03651 steffy
# MAGIC ^1_1 01/08/09 06:44:33 Batch  14984_24277 INIT bckcett devlIDS u03651 steffy
# MAGIC ^1_1 03/07/07 16:30:43 Batch  14311_59451 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 01/26/06 07:54:36 Batch  13906_28483 INIT bckcetl ids20 dsadm Gina
# MAGIC ^1_3 07/29/05 15:21:56 Batch  13725_55324 INIT bckcetl ids20 dsadm Brent
# MAGIC ^1_4 07/28/05 10:08:43 Batch  13724_36528 PROMOTE bckcetl ids20 dsadm Gina Parr
# MAGIC ^1_4 07/28/05 10:02:54 Batch  13724_36180 INIT bckcett testIDS30 dsadm Gina Parr
# MAGIC ^1_3 07/28/05 08:20:43 Batch  13724_30048 PROMOTE bckcett VERSION u03651 steffy
# MAGIC ^1_3 07/28/05 08:19:06 Batch  13724_29948 INIT bckcett devlIDS30 u03651 steffy
# MAGIC ^1_2 07/28/05 08:15:12 Batch  13724_29715 INIT bckcett devlIDS30 u03651 steffy
# MAGIC ^1_1 07/25/05 08:29:54 Batch  13721_30598 INIT bckcett devlIDS30 u03651 steffy
# MAGIC ^1_2 04/04/05 15:27:53 Batch  13609_55681 PROMOTE bckcetl VERSION dsadm J. Mahaffey
# MAGIC ^1_2 04/04/05 15:23:18 Batch  13609_55402 INIT bckcett migrate dsadm J. Mahaffey
# MAGIC ^1_1 04/04/05 15:14:19 Batch  13609_54872 INIT bckcett migrate dsadm J. Mahaffey
# MAGIC ^1_1 04/04/05 14:34:35 Batch  13609_52481 PROMOTE bckcett VERSION u03651 steffy
# MAGIC ^1_1 04/04/05 14:29:58 Batch  13609_52201 INIT bckcett IDS51 u03651 steffy
# MAGIC ^1_8 03/03/05 09:58:45 Batch  13577_35930 PROMOTE bckccdt VERSION u03651 steffy
# MAGIC ^1_8 03/03/05 09:50:13 Batch  13577_35416 INIT bckccdt devIDS21 u03651 steffy
# MAGIC ^1_3 01/04/05 09:23:37 Batch  13519_33819 INIT bckccdt devIDS21 u03651 steffy
# MAGIC ^1_2 11/10/04 13:05:22 Batch  13464_47126 INIT bckccdt devIDS21 u03651 steffy
# MAGIC ^1_1 11/08/04 10:29:24 Batch  13462_37767 INIT bckccdt devIDS21 u03651 steffy
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2004 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsIncmTransFkey
# MAGIC 
# MAGIC DESCRIPTION:     Takes the sorted file and applies the foreign keys; essentially converting the common record format file to a table load file.   
# MAGIC                             If any of the foreign key lookups fail, then it is written to the recycle hash file.
# MAGIC      
# MAGIC                            Not date specific.   
# MAGIC 
# MAGIC                          No databases are used.  Just files.
# MAGIC                                   
# MAGIC   
# MAGIC INPUTS:             #FilePath#  IdsIncmTransPkey.IncmTrans.RUNID
# MAGIC 	
# MAGIC   
# MAGIC HASH FILES:     hf_recycle - written to only.
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                              GetFkeyClm ()
# MAGIC                              GetFkeyDate()
# MAGIC                             GetFkeyCodes()
# MAGIC                             GetFkeyErrorCnt(  )
# MAGIC 
# MAGIC PROCESSING:
# MAGIC 
# MAGIC                   All records from the input file are processed; no records are filtered out.  
# MAGIC                   Output file is created with a temp. name.  After-Job Subroutine renames file after successful run.
# MAGIC                   
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                               #FilePath#/ load / #TmpOutFile#
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC             Steph Goddard     10/11/2004     -   Originally Programmed
# MAGIC             Steph Goddard     07/13/2005         Changes for sequencer
# MAGIC 
# MAGIC            
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parik                                 2008-08-19     3567(Primary Key)          Added Source System Code SK as parameter       devlIDS                       Steph Goddard            08/21/2008

# MAGIC Read common record format file.
# MAGIC Writing Sequential File to /load
# MAGIC Assign foreign keys and recycle keys not found.
# MAGIC Create default rows for UNK and NA
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DateType, DecimalType
from pyspark.sql import Window
from pyspark.sql.functions import col, lit, row_number, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


InFile = get_widget_value('InFile','')
TmpOutFile = get_widget_value('TmpOutFile','')
Logging = get_widget_value('Logging','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')

schema_IdsIncmTransExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38,10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("INCM_TRANS_SK", IntegerType(), False),
    StructField("INCM_TRANS_CK", DecimalType(38,10), False),
    StructField("ACCTG_DT_SK", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("GRP", IntegerType(), False),
    StructField("PROD", IntegerType(), False),
    StructField("SUBGRP", StringType(), False),
    StructField("BILL_ENTY_CK", IntegerType(), False),
    StructField("FNCL_LOB", StringType(), False),
    StructField("ORIG_FNCL_LOB", StringType(), False),
    StructField("RVSED_FNCL_LOB", StringType(), False),
    StructField("INCM_TRANS_LOB_CD", StringType(), False),
    StructField("FIRST_YR_IN", StringType(), False),
    StructField("BILL_DUE_DT", DateType(), False),
    StructField("CRT_DT", DateType(), False),
    StructField("POSTING_DT", DateType(), False),
    StructField("BILL_CMPNT_ID", StringType(), False)
])

df_IdsIncmTransExtr = (
    spark.read.format("csv")
    .option("header", "false")
    .option("quote", "\"")
    .schema(schema_IdsIncmTransExtr)
    .load(f"{adls_path}/key/{InFile}")
)

df_enriched = (
    df_IdsIncmTransExtr
    .withColumn("SrcSysCdSk", lit(SrcSysCdSk))
    .withColumn("ErrCount", GetFkeyErrorCnt(col("INCM_TRANS_SK")))
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn("SubGrpSk", GetFkeySubgrp(col("SRC_SYS_CD"), col("INCM_TRANS_SK"), col("GRP"), col("SUBGRP"), lit(Logging)))
    .withColumn("AcctDtSk", GetFkeyDate(lit("IDS"), col("INCM_TRANS_SK"), col("ACCTG_DT_SK"), lit(Logging)))
    .withColumn("BillDueDtSk", GetFkeyDate(lit("IDS"), col("INCM_TRANS_SK"), col("BILL_DUE_DT"), lit(Logging)))
    .withColumn("CrtDtSk", GetFkeyDate(lit("IDS"), col("INCM_TRANS_SK"), col("CRT_DT"), lit(Logging)))
    .withColumn("PostingDtSk", GetFkeyDate(lit("IDS"), col("INCM_TRANS_SK"), col("POSTING_DT"), lit(Logging)))
    .withColumn("svGrpSk", GetFkeyGrp(trim(col("SRC_SYS_CD")), col("INCM_TRANS_SK"), trim(col("GRP")), lit(Logging)))
    .withColumn("svProdSk", GetFkeyProd(trim(col("SRC_SYS_CD")), col("INCM_TRANS_SK"), col("PROD"), lit(Logging)))
    .withColumn("OrigFnclLOB", GetFkeyFnclLob(lit("PSI"), col("INCM_TRANS_SK"), col("ORIG_FNCL_LOB"), lit(Logging)))
    .withColumn("RvsdFnclLOB", GetFkeyFnclLob(lit("PSI"), col("INCM_TRANS_SK"), col("RVSED_FNCL_LOB"), lit(Logging)))
    .withColumn("svFnclLOB", GetFkeyFnclLob(lit("PSI"), col("INCM_TRANS_SK"), trim(col("FNCL_LOB")), lit(Logging)))
    .withColumn("IncmTrnsLOB", GetFkeyCodes(col("SRC_SYS_CD"), col("INCM_TRANS_SK"), trim(lit("CLAIM LINE LOB")), trim(col("INCM_TRANS_LOB_CD")), lit(Logging)))
)

w = Window.orderBy(lit(1))
df_enriched = df_enriched.withColumn("rownum", row_number().over(w))

df_Fkey = df_enriched.filter((col("ErrCount") == 0) | (col("PassThru") == 'Y')).select(
    col("INCM_TRANS_SK").alias("INCM_TRANS_SK"),
    col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
    col("INCM_TRANS_CK").alias("INCM_TRANS_CK"),
    col("AcctDtSk").alias("ACCTG_DT_SK"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("svFnclLOB").alias("FNCL_LOB_SK"),
    col("svGrpSk").alias("GRP_SK"),
    col("OrigFnclLOB").alias("ORIG_FNCL_LOB_SK"),
    col("svProdSk").alias("PROD_SK"),
    col("RvsdFnclLOB").alias("RVSED_FNCL_LOB_SK"),
    col("SubGrpSk").alias("SUBGRP_SK"),
    col("IncmTrnsLOB").alias("INCM_TRANS_LOB_CD_SK"),
    col("FIRST_YR_IN").alias("FIRST_YR_IN"),
    col("BillDueDtSk").alias("BILL_DUE_DT_SK"),
    col("CrtDtSk").alias("CRT_DT_SK"),
    col("PostingDtSk").alias("POSTING_DT_SK"),
    col("BILL_CMPNT_ID").alias("BILL_CMPNT_ID"),
    col("BILL_ENTY_CK").alias("BILL_ENTY_CK")
)

df_Recycle = df_enriched.filter(col("ErrCount") > 0).select(
    GetRecycleKey(col("INCM_TRANS_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
    col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    col("DISCARD_IN").alias("DISCARD_IN"),
    col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    col("ErrCount").alias("ERR_CT"),
    (col("RECYCLE_CT") + lit(1)).alias("RECYCLE_CT"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    col("INCM_TRANS_SK").alias("INCM_TRANS_SK"),
    col("INCM_TRANS_CK").alias("INCM_TRANS_CK"),
    col("ACCTG_DT_SK").alias("ACCTG_DT_SK"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("GRP").alias("GRP"),
    col("PROD").alias("PROD"),
    col("SUBGRP").alias("SUBGRP"),
    col("BILL_ENTY_CK").alias("BILL_ENTY_CK"),
    col("FNCL_LOB").alias("FNCL_LOB"),
    col("ORIG_FNCL_LOB").alias("ORIG_FNCL_LOB"),
    col("RVSED_FNCL_LOB").alias("RVSED_FNCL_LOB"),
    col("INCM_TRANS_LOB_CD").alias("INCM_TRANS_LOB_CD"),
    col("FIRST_YR_IN").alias("FIRST_YR_IN"),
    col("BILL_DUE_DT").alias("BILL_DUE_DT"),
    col("CRT_DT").alias("CRT_DT"),
    col("POSTING_DT").alias("POSTING_DT"),
    col("BILL_CMPNT_ID").alias("BILL_CMPNT_ID")
)

write_files(
    df_Recycle,
    "hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_DefaultUNK = df_enriched.filter(col("rownum") == 1).select(
    lit(0).alias("INCM_TRANS_SK"),
    lit(0).alias("SRC_SYS_CD_SK"),
    lit(0).alias("INCM_TRANS_CK"),
    lit("NA").alias("ACCTG_DT_SK"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("FNCL_LOB_SK"),
    lit(0).alias("GRP_SK"),
    lit(0).alias("ORIG_FNCL_LOB_SK"),
    lit(0).alias("PROD_SK"),
    lit(0).alias("RVSED_FNCL_LOB_SK"),
    lit(0).alias("SUBGRP_SK"),
    lit(0).alias("INCM_TRANS_LOB_CD_SK"),
    lit("U").alias("FIRST_YR_IN"),
    lit("NA").alias("BILL_DUE_DT_SK"),
    lit("NA").alias("CRT_DT_SK"),
    lit("NA").alias("POSTING_DT_SK"),
    lit("UNK").alias("BILL_CMPNT_ID"),
    lit(0).alias("BILL_ENTY_CK")
)

df_DefaultNA = df_enriched.filter(col("rownum") == 1).select(
    lit(1).alias("INCM_TRANS_SK"),
    lit(1).alias("SRC_SYS_CD_SK"),
    lit(1).alias("INCM_TRANS_CK"),
    lit("NA").alias("ACCTG_DT_SK"),
    lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(1).alias("FNCL_LOB_SK"),
    lit(1).alias("GRP_SK"),
    lit(1).alias("ORIG_FNCL_LOB_SK"),
    lit(1).alias("PROD_SK"),
    lit(1).alias("RVSED_FNCL_LOB_SK"),
    lit(1).alias("SUBGRP_SK"),
    lit(1).alias("INCM_TRANS_LOB_CD_SK"),
    lit("X").alias("FIRST_YR_IN"),
    lit("NA").alias("BILL_DUE_DT_SK"),
    lit("NA").alias("CRT_DT_SK"),
    lit("NA").alias("POSTING_DT_SK"),
    lit("NA").alias("BILL_CMPNT_ID"),
    lit(1).alias("BILL_ENTY_CK")
)

df_collector = (
    df_Fkey
    .unionByName(df_DefaultUNK)
    .unionByName(df_DefaultNA)
    .select(
        "INCM_TRANS_SK",
        "SRC_SYS_CD_SK",
        "INCM_TRANS_CK",
        "ACCTG_DT_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "FNCL_LOB_SK",
        "GRP_SK",
        "ORIG_FNCL_LOB_SK",
        "PROD_SK",
        "RVSED_FNCL_LOB_SK",
        "SUBGRP_SK",
        "INCM_TRANS_LOB_CD_SK",
        "FIRST_YR_IN",
        "BILL_DUE_DT_SK",
        "CRT_DT_SK",
        "POSTING_DT_SK",
        "BILL_CMPNT_ID",
        "BILL_ENTY_CK"
    )
)

df_collector_final = (
    df_collector
    .withColumn("ACCTG_DT_SK", rpad(col("ACCTG_DT_SK"), 10, " "))
    .withColumn("FIRST_YR_IN", rpad(col("FIRST_YR_IN"), 1, " "))
    .withColumn("BILL_DUE_DT_SK", rpad(col("BILL_DUE_DT_SK"), 10, " "))
    .withColumn("CRT_DT_SK", rpad(col("CRT_DT_SK"), 10, " "))
    .withColumn("POSTING_DT_SK", rpad(col("POSTING_DT_SK"), 10, " "))
    .withColumn("BILL_CMPNT_ID", rpad(col("BILL_CMPNT_ID"), <...>, " "))
)

write_files(
    df_collector_final,
    f"{adls_path}/load/{TmpOutFile}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)