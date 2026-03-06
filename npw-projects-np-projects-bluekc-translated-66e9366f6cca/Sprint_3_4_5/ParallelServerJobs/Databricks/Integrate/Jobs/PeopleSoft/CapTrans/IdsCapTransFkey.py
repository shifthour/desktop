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
# MAGIC ^1_5 12/15/05 14:41:28 Batch  13864_52894 PROMOTE bckcetl ids20 dsadm Gina Parr
# MAGIC ^1_5 12/15/05 14:39:42 Batch  13864_52786 INIT bckcett testIDS30 dsadm Gina Parr
# MAGIC ^1_4 12/15/05 13:18:29 Batch  13864_47936 PROMOTE bckcett testIDS30 u03651 steffy
# MAGIC ^1_4 12/15/05 13:17:03 Batch  13864_47826 INIT bckcett devlIDS30 u03651 steffy
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
# MAGIC JOB NAME:     IdsCapTransFkey
# MAGIC DESCRIPTION:     Takes the sorted file and applies the foreign keys; essentially converting the common record format file to a table load file.   
# MAGIC                             If any of the foreign key lookups fail, then it is written to the recycle hash file.
# MAGIC      
# MAGIC                            Not date specific.  
# MAGIC                          No databases are used.  Just files.
# MAGIC                                   
# MAGIC   
# MAGIC INPUTS:           
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
# MAGIC                            
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC             Steph Goddard    10/12/2004   Initial program
# MAGIC             Steph Goddard    07/19/2005   Changes for sequencer
# MAGIC             Steph Goddard    08/04/2005   change source for CAP_TRANS_PAYMT_METH_CD to "PSI"
# MAGIC 
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parik                                 2008-08-15    3567 (Primary Key)            Added Source System Code SK as parameter    devlIDS                         Steph Goddard           08/21/2008

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
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DecimalType
from pyspark.sql.functions import col, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/GetFkeyGrp
# COMMAND ----------
# MAGIC %run ../../../../shared_containers/PrimaryKey/GetFkeyProd
# COMMAND ----------
# MAGIC %run ../../../../shared_containers/PrimaryKey/GetFkeyDate
# COMMAND ----------
# MAGIC %run ../../../../shared_containers/PrimaryKey/GetFkeyCodes
# COMMAND ----------
# MAGIC %run ../../../../shared_containers/PrimaryKey/GetFkeyFnclLob
# COMMAND ----------
# MAGIC %run ../../../../shared_containers/PrimaryKey/GetFkeyErrorCnt
# COMMAND ----------
# MAGIC %run ../../../../shared_containers/PrimaryKey/GetRecycleKey
# COMMAND ----------

InFile = get_widget_value('InFile','PSCapTransExtr.CapTrans.dat.20080820')
TmpOutFile = get_widget_value('TmpOutFile','CAP_TRANS.dat')
Logging = get_widget_value('Logging','N')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')

schema_IdsCapTransExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38, 10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("CAP_TRANS_SK", IntegerType(), False),
    StructField("CAP_TRANS_CK", DecimalType(38, 10), False),
    StructField("ACCTG_DT_SK", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("GRP", StringType(), False),
    StructField("PROD", StringType(), False),
    StructField("FNCL_LOB", StringType(), False),
    StructField("CAP_TRANS_LOB_CD", StringType(), False),
    StructField("CAP_TRANS_PAYMT_METH_CD", StringType(), False),
    StructField("PD_FROM_DT", StringType(), False),
    StructField("PD_THRU_DT", StringType(), False),
    StructField("CALC_FUND_AMT", DecimalType(38,10), False),
    StructField("FUND_RATE_AMT", DecimalType(38,10), False)
])

df_IdsCapTransExtr = (
    spark.read
    .option("sep", ",")
    .option("quote", "\"")
    .option("header", "false")
    .schema(schema_IdsCapTransExtr)
    .csv(f"{adls_path}/key/{InFile}")
)

df_enriched = (
    df_IdsCapTransExtr
    .withColumn("Logging", lit(Logging))
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn("svGrpSk", GetFkeyGrp(col("SRC_SYS_CD"), col("CAP_TRANS_SK"), trim(col("GRP")), lit("Y")))
    .withColumn("svProdSk", GetFkeyProd(col("SRC_SYS_CD"), col("CAP_TRANS_SK"), col("PROD"), lit("Y")))
    .withColumn("AcctDtSk", GetFkeyDate("IDS", col("CAP_TRANS_SK"), col("ACCTG_DT_SK"), col("Logging")))
    .withColumn("PdFromDtSk", GetFkeyDate("IDS", col("CAP_TRANS_SK"), col("PD_FROM_DT"), col("Logging")))
    .withColumn("PdToDtSk", GetFkeyDate("IDS", col("CAP_TRANS_SK"), col("PD_THRU_DT"), col("Logging")))
    .withColumn("CapTransLobCdSk", GetFkeyCodes(col("SRC_SYS_CD"), col("CAP_TRANS_SK"), lit("CLAIM LINE LOB"), col("CAP_TRANS_LOB_CD"), col("Logging")))
    .withColumn("CapTransPayMethSk", GetFkeyCodes(lit("PSI"), col("CAP_TRANS_SK"), lit("CAPITATION TRANSACTION PAYMENT METHOD"), col("CAP_TRANS_PAYMT_METH_CD"), col("Logging")))
    .withColumn("svFnclLOB", GetFkeyFnclLob(lit("PSI"), col("CAP_TRANS_SK"), trim(col("FNCL_LOB")), lit("Y")))
    .withColumn("ErrCount", GetFkeyErrorCnt(col("CAP_TRANS_SK")))
)

df_fkey = (
    df_enriched
    .filter((col("ErrCount") == 0) | (col("PassThru") == 'Y'))
    .select(
        col("CAP_TRANS_SK").alias("CAP_TRANS_SK"),
        lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        col("CAP_TRANS_CK").alias("CAP_TRANS_CK"),
        col("AcctDtSk").alias("ACCTG_DT_SK"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("svFnclLOB").alias("FNCL_LOB_SK"),
        col("svGrpSk").alias("GRP_SK"),
        col("svProdSk").alias("PROD_SK"),
        col("CapTransLobCdSk").alias("CAP_TRANS_LOB_CD_SK"),
        col("CapTransPayMethSk").alias("CAP_TRANS_PAYMT_METH_CD_SK"),
        col("PdFromDtSk").alias("PD_FROM_DT_SK"),
        col("PdToDtSk").alias("PD_THRU_DT_SK"),
        col("CALC_FUND_AMT").alias("CALC_FUND_AMT"),
        col("FUND_RATE_AMT").alias("FUND_RATE_AMT")
    )
)

df_recycle = (
    df_enriched
    .filter(col("ErrCount") > 0)
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", GetRecycleKey(col("CAP_TRANS_SK")))
    .withColumn("ERR_CT", col("ErrCount"))
    .withColumn("RECYCLE_CT", col("RECYCLE_CT") + lit(1))
    .select(
        col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
        col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        col("DISCARD_IN").alias("DISCARD_IN"),
        col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        col("ERR_CT").alias("ERR_CT"),
        col("RECYCLE_CT").alias("RECYCLE_CT"),
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        col("CAP_TRANS_SK").alias("CAP_TRANS_SK"),
        col("CAP_TRANS_CK").alias("CAP_TRANS_CK"),
        col("ACCTG_DT_SK").alias("ACCTG_DT_SK"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("GRP").alias("GRP"),
        col("PROD").alias("PROD"),
        col("FNCL_LOB").alias("FNCL_LOB"),
        col("CAP_TRANS_LOB_CD").alias("CAP_TRANS_LOB_CD"),
        col("CAP_TRANS_PAYMT_METH_CD").alias("CAP_TRANS_PAYMT_METH_CD"),
        col("PD_FROM_DT").alias("PD_FROM_DT"),
        col("PD_THRU_DT").alias("PD_THRU_DT"),
        col("CALC_FUND_AMT").alias("CALC_FUND_AMT"),
        col("FUND_RATE_AMT").alias("FUND_RATE_AMT")
    )
)

write_files(
    df_recycle,
    f"{adls_path}/hf_recycle_cap_trans.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

schema_collector = StructType([
    StructField("CAP_TRANS_SK", IntegerType(), True),
    StructField("SRC_SYS_CD_SK", IntegerType(), True),
    StructField("CAP_TRANS_CK", DecimalType(38, 10), True),
    StructField("ACCTG_DT_SK", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("FNCL_LOB_SK", IntegerType(), True),
    StructField("GRP_SK", IntegerType(), True),
    StructField("PROD_SK", IntegerType(), True),
    StructField("CAP_TRANS_LOB_CD_SK", IntegerType(), True),
    StructField("CAP_TRANS_PAYMT_METH_CD_SK", IntegerType(), True),
    StructField("PD_FROM_DT_SK", StringType(), True),
    StructField("PD_THRU_DT_SK", StringType(), True),
    StructField("CALC_FUND_AMT", DecimalType(38, 10), True),
    StructField("FUND_RATE_AMT", DecimalType(38, 10), True)
])

df_DefaultUNK = spark.createDataFrame(
    [
        (
            0,                     # CAP_TRANS_SK
            0,                     # SRC_SYS_CD_SK
            0.00,                  # CAP_TRANS_CK
            "NA",                  # ACCTG_DT_SK
            0,                     # CRT_RUN_CYC_EXCTN_SK
            0,                     # LAST_UPDT_RUN_CYC_EXCTN_SK
            0,                     # FNCL_LOB_SK
            0,                     # GRP_SK
            0,                     # PROD_SK
            0,                     # CAP_TRANS_LOB_CD_SK
            0,                     # CAP_TRANS_PAYMT_METH_CD_SK
            "NA",                  # PD_FROM_DT_SK
            "NA",                  # PD_THRU_DT_SK
            0.00,                  # CALC_FUND_AMT
            0.00                   # FUND_RATE_AMT
        )
    ],
    schema_collector
)

df_DefaultNA = spark.createDataFrame(
    [
        (
            1,                     # CAP_TRANS_SK
            1,                     # SRC_SYS_CD_SK
            1.00,                  # CAP_TRANS_CK
            "NA",                  # ACCTG_DT_SK
            1,                     # CRT_RUN_CYC_EXCTN_SK
            1,                     # LAST_UPDT_RUN_CYC_EXCTN_SK
            1,                     # FNCL_LOB_SK
            1,                     # GRP_SK
            1,                     # PROD_SK
            1,                     # CAP_TRANS_LOB_CD_SK
            1,                     # CAP_TRANS_PAYMT_METH_CD_SK
            "NA",                  # PD_FROM_DT_SK
            "NA",                  # PD_THRU_DT_SK
            0.00,                  # CALC_FUND_AMT
            0.00                   # FUND_RATE_AMT
        )
    ],
    schema_collector
)

df_collector = df_fkey.unionByName(df_DefaultUNK).unionByName(df_DefaultNA)

df_collector = df_collector.select(
    "CAP_TRANS_SK",
    "SRC_SYS_CD_SK",
    "CAP_TRANS_CK",
    "ACCTG_DT_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "FNCL_LOB_SK",
    "GRP_SK",
    "PROD_SK",
    "CAP_TRANS_LOB_CD_SK",
    "CAP_TRANS_PAYMT_METH_CD_SK",
    "PD_FROM_DT_SK",
    "PD_THRU_DT_SK",
    "CALC_FUND_AMT",
    "FUND_RATE_AMT"
)

df_collector = df_collector.withColumn("ACCTG_DT_SK", rpad(col("ACCTG_DT_SK"), 10, " ")) \
                           .withColumn("PD_FROM_DT_SK", rpad(col("PD_FROM_DT_SK"), 10, " ")) \
                           .withColumn("PD_THRU_DT_SK", rpad(col("PD_THRU_DT_SK"), 10, " "))

write_files(
    df_collector,
    f"{adls_path}/load/{TmpOutFile}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)