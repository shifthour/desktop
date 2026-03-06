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
# MAGIC JOB NAME:     IdsDrugTransFkey
# MAGIC 
# MAGIC DESCRIPTION:     Takes the sorted file and applies the foreign keys; essentially converting the common record format file to a table load file.   
# MAGIC                             If any of the foreign key lookups fail, then it is written to the recycle hash file.
# MAGIC      
# MAGIC                            Not date specific.   Just drives off what is on the Claim header file.
# MAGIC 
# MAGIC                          No databases are used.  Just files.
# MAGIC                                   
# MAGIC   
# MAGIC INPUTS:             #FilePath#  NpsClmExtrnlMbrshPkey.NPSExtrnMbr.RUNID
# MAGIC 	
# MAGIC   
# MAGIC HASH FILES:     hf_recycle - written to only.
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                             GetFkeyDate()
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
# MAGIC             Steph Goddard  10/14/2004     -   Originally Programmed
# MAGIC             Steph Goddard  07/13/2005         Sequencer changes
# MAGIC           
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parik                                2008-08-18     3567(Primary Key)          Added Source System Code SK as parameter            devlIDS                   Steph Goddard             08/21/2008

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
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------

# All user-defined helper functions (GetFkeyGrp, GetFkeyProd, ..., write_files, etc.) are assumed to be available in the environment.

InFile = get_widget_value("InFile", "PSDrugTransExtr.DrugTrans.dat.20080821")
TmpOutFile = get_widget_value("TmpOutFile", "DRUG_TRANS.dat")
Logging = get_widget_value("Logging", "N")
SrcSysCdSkParam = get_widget_value("SrcSysCdSk","")

schema_IdsDrugTransExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38,10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("DRUG_TRANS_SK", IntegerType(), nullable=False),
    StructField("DRUG_TRANS_CK", DecimalType(38,10), nullable=False),
    StructField("ACCTG_DT_SK", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("FNCL_LOB", StringType(), nullable=False),
    StructField("GRP", IntegerType(), nullable=False),
    StructField("PROD", IntegerType(), nullable=False),
    StructField("DRUG_TRANS_LOB_CD", StringType(), nullable=False),
    StructField("BILL_CMPNT_ID", StringType(), nullable=False)
])

df_IdsDrugTransExtr = (
    spark.read.format("csv")
    .schema(schema_IdsDrugTransExtr)
    .option("sep", ",")
    .option("quote", "\"")
    .option("header", "false")
    .load(f"{adls_path}/key/{InFile}")
)

df_with_vars = (
    df_IdsDrugTransExtr
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("svGrpSk", GetFkeyGrp(F.col("SRC_SYS_CD"), F.col("DRUG_TRANS_SK"), F.col("GRP"), F.lit(Logging)))
    .withColumn("svProdSk", GetFkeyProd(F.col("SRC_SYS_CD"), F.col("DRUG_TRANS_SK"), F.col("PROD"), F.lit(Logging)))
    .withColumn("AcctgDtSk", GetFkeyDate(F.lit("IDS"), F.col("DRUG_TRANS_SK"), F.col("ACCTG_DT_SK"), F.lit(Logging)))
    .withColumn("DrugTransLOBSk", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("DRUG_TRANS_SK"), F.lit("CLAIM LINE LOB"), F.col("DRUG_TRANS_LOB_CD"), F.lit(Logging)))
    .withColumn("svFnclLOB", GetFkeyFnclLob(F.lit("PSI"), F.col("DRUG_TRANS_SK"), trim(F.col("FNCL_LOB")), F.lit(Logging)))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("DRUG_TRANS_SK")))
)

df_Fkey = (
    df_with_vars
    .filter((F.col("ErrCount") == 0) | (F.col("PassThru") == 'Y'))
    .select(
        F.col("DRUG_TRANS_SK").alias("DRUG_TRANS_SK"),
        F.lit(SrcSysCdSkParam).alias("SRC_SYS_CD_SK"),
        F.col("DRUG_TRANS_CK").alias("DRUG_TRANS_CK"),
        F.rpad(F.col("AcctgDtSk"), 10, " ").alias("ACCTG_DT_SK"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("svFnclLOB").alias("FNCL_LOB_SK"),
        F.col("svGrpSk").alias("GRP_SK"),
        F.col("svProdSk").alias("PROD_SK"),
        F.col("DrugTransLOBSk").alias("DRUG_TRANS_LOB_CD_SK"),
        F.rpad(F.col("BILL_CMPNT_ID"), 255, " ").alias("BILL_CMPNT_ID")
    )
)

df_Recycle = (
    df_with_vars
    .filter(F.col("ErrCount") > 0)
    .select(
        GetRecycleKey(F.col("DRUG_TRANS_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
        F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
        F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("ErrCount").alias("ERR_CT"),
        (F.col("RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
        F.rpad(F.col("SRC_SYS_CD"), 255, " ").alias("SRC_SYS_CD"),
        F.rpad(F.col("PRI_KEY_STRING"), 255, " ").alias("PRI_KEY_STRING"),
        F.col("DRUG_TRANS_SK").alias("DRUG_TRANS_SK"),
        F.col("DRUG_TRANS_CK").alias("DRUG_TRANS_CK"),
        F.rpad(F.col("ACCTG_DT_SK"), 10, " ").alias("ACCTG_DT_SK"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.rpad(F.col("FNCL_LOB"), 10, " ").alias("FNCL_LOB"),
        F.col("GRP").alias("GRP"),
        F.col("PROD").alias("PROD"),
        F.rpad(F.col("DRUG_TRANS_LOB_CD"), 10, " ").alias("DRUG_TRANS_LOB_CD"),
        F.rpad(F.col("BILL_CMPNT_ID"), 255, " ").alias("BILL_CMPNT_ID")
    )
)

write_files(
    df_Recycle,
    "hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

win = Window.orderBy(F.lit(1))
df_rnum = df_with_vars.withColumn("rownum", F.row_number().over(win))

df_DefaultUNK = (
    df_rnum
    .filter(F.col("rownum") == 1)
    .select(
        F.lit(0).alias("DRUG_TRANS_SK"),
        F.lit(0).alias("SRC_SYS_CD_SK"),
        F.lit(0).alias("DRUG_TRANS_CK"),
        F.rpad(F.lit("NA"), 10, " ").alias("ACCTG_DT_SK"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("FNCL_LOB_SK"),
        F.lit(0).alias("GRP_SK"),
        F.lit(0).alias("PROD_SK"),
        F.lit(0).alias("DRUG_TRANS_LOB_CD_SK"),
        F.rpad(F.lit("UNK"), 255, " ").alias("BILL_CMPNT_ID")
    )
)

df_DefaultNA = (
    df_rnum
    .filter(F.col("rownum") == 1)
    .select(
        F.lit(1).alias("DRUG_TRANS_SK"),
        F.lit(1).alias("SRC_SYS_CD_SK"),
        F.lit(1).alias("DRUG_TRANS_CK"),
        F.rpad(F.lit("NA"), 10, " ").alias("ACCTG_DT_SK"),
        F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("FNCL_LOB_SK"),
        F.lit(1).alias("GRP_SK"),
        F.lit(1).alias("PROD_SK"),
        F.lit(1).alias("DRUG_TRANS_LOB_CD_SK"),
        F.rpad(F.lit("NA"), 255, " ").alias("BILL_CMPNT_ID")
    )
)

df_Collector = df_Fkey.unionByName(df_DefaultUNK).unionByName(df_DefaultNA)

write_files(
    df_Collector,
    f"{adls_path}/load/{TmpOutFile}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)