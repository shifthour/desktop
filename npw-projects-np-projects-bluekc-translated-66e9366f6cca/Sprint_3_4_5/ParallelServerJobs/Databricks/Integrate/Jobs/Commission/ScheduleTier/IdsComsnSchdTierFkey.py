# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_3 02/20/09 11:05:46 Batch  15027_39950 PROMOTE bckcetl ids20 dsadm bls for sa
# MAGIC ^1_3 02/20/09 10:48:45 Batch  15027_38927 INIT bckcett testIDS dsadm bls for sa
# MAGIC ^1_2 02/19/09 15:48:36 Batch  15026_56922 PROMOTE bckcett testIDS u03651 steph for Sharon
# MAGIC ^1_2 02/19/09 15:43:11 Batch  15026_56617 INIT bckcett devlIDS u03651 steffy
# MAGIC ^1_1 01/13/09 08:41:23 Batch  14989_31287 INIT bckcett devlIDS u03651 steffy
# MAGIC ^1_2 02/19/08 12:47:08 Batch  14660_46032 PROMOTE bckcetl ids20 dsadm bls for on
# MAGIC ^1_2 02/19/08 12:44:40 Batch  14660_45896 INIT bckcett testIDS dsadm bls for on
# MAGIC ^1_1 02/15/08 12:24:50 Batch  14656_44697 PROMOTE bckcett testIDS u03651 steph for Ollie
# MAGIC ^1_1 02/15/08 12:22:56 Batch  14656_44584 INIT bckcett devlIDS u03651 steffy
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
# MAGIC JOB NAME:     IdsComsnSchdTierFkey
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
# MAGIC OUTPUTS:   CLS table load file
# MAGIC                      hf_recycle - If recycling was used for this job records would be written to this hash file that had errors.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC             Suzanne Saylor  -  10/24/2005  -  Originally programmed
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                 Change Description                                            Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------      ---------------------------------------------------------                      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Oliver Nielsen                   02/14/2008    Production Support          Fix SrcSysCdSK Stage Variable call                               devlIDS                 Steph Goddard             02/15/2008
# MAGIC                                                                                                         Was passing in wrong argument
# MAGIC           
# MAGIC Bhoomi Dasari                 2008-09-09     Added SrcSysCdSk parameter/3567                                                                       devlIDS                 Steph Goddard             09/22/2008

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
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DecimalType
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lit, row_number, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Parameters
InFile = get_widget_value('InFile','IdsComsnSchdTierExtr.dat.pkey')
Logging = get_widget_value('Logging','X')
OutFile = get_widget_value('OutFile','COMSN_SCHD_TIER.dat')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')

# Schema for IdsComsnSchdTier (CSeqFileStage)
schema_IdsComsnSchdTier = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38,10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("COMSN_SCHD_TIER_SK", IntegerType(), False),
    StructField("COMSN_SCHD_ID", StringType(), False),
    StructField("DURATN_EFF_DT_SK", StringType(), False),
    StructField("DURATN_STRT_PERD_NO", IntegerType(), False),
    StructField("PRM_FROM_THRS_HLD_AMT", DecimalType(38,10), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("COMSN_SCHD_SK", IntegerType(), False),
    StructField("COMSN_SCHD_TIER_CALC_METH_CD_SK", StringType(), False),
    StructField("PRM_THRU_THRESOLD_AMT", DecimalType(38,10), False),
    StructField("TIER_AMT", DecimalType(38,10), False),
    StructField("TIER_PCT", IntegerType(), False),
    StructField("TIER_UNIQ_KEY", IntegerType(), False)
])

df_IdsComsnSchdTier = (
    spark.read.format("csv")
    .option("header", "false")
    .option("quote", "\"")
    .schema(schema_IdsComsnSchdTier)
    .load(f"{adls_path}/key/{InFile}")
)

# ForeignKey Transformer logic
df_fk = (
    df_IdsComsnSchdTier
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn("svComsnSchdSk", GetFkeyComsnSchd(col("SRC_SYS_CD"), col("COMSN_SCHD_TIER_SK"), col("COMSN_SCHD_ID"), lit(Logging)))
    .withColumn("svComsnSchdTierCalcMethCdSk", GetFkeyCodes(col("SRC_SYS_CD"), col("COMSN_SCHD_TIER_SK"), lit("COMMISSION SCHEDULE CALCULATION METHOD"), col("COMSN_SCHD_TIER_CALC_METH_CD_SK"), lit(Logging)))
    .withColumn("ErrCount", GetFkeyErrorCnt(col("COMSN_SCHD_TIER_SK")))
)

# Fkey link
df_fkey = (
    df_fk
    .filter((col("ErrCount") == 0) | (col("PassThru") == 'Y'))
    .select(
        col("COMSN_SCHD_TIER_SK").alias("COMSN_SCHD_TIER_SK"),
        lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        col("COMSN_SCHD_ID").alias("COMSN_SCHD_ID"),
        col("DURATN_EFF_DT_SK").alias("DURATN_EFF_DT_SK"),
        col("DURATN_STRT_PERD_NO").alias("DURATN_STRT_PERD_NO"),
        col("PRM_FROM_THRS_HLD_AMT").alias("PRM_FROM_THRSHLD_AMT"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("svComsnSchdSk").alias("COMSN_SCHD_SK"),
        col("svComsnSchdTierCalcMethCdSk").alias("COMSN_SCHD_TIER_CALC_METH_CD_SK"),
        col("PRM_THRU_THRESOLD_AMT").alias("PRM_THRU_THRESOLD_AMT"),
        col("TIER_AMT").alias("TIER_AMT"),
        col("TIER_PCT").alias("TIER_PCT"),
        col("TIER_UNIQ_KEY").alias("TIER_UNIQ_KEY")
    )
)

# Recycle link
df_recycle = (
    df_fk
    .filter(col("ErrCount") > 0)
    .select(
        GetRecycleKey(col("COMSN_SCHD_TIER_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        col("DISCARD_IN").alias("DISCARD_IN"),
        col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        col("ERR_CT").alias("ERR_CT"),
        (col("RECYCLE_CT") + lit(1)).alias("RECYCLE_CT"),
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        col("COMSN_SCHD_TIER_SK").alias("COMSN_SCHD_TIER_SK"),
        col("COMSN_SCHD_ID").alias("COMSN_SCHD_ID"),
        col("DURATN_EFF_DT_SK").alias("DURATN_EFF_DT_SK"),
        col("DURATN_STRT_PERD_NO").alias("DURATN_STRT_PERD_NO"),
        col("PRM_FROM_THRS_HLD_AMT").alias("PRM_FROM_THRSHLD_AMT"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("COMSN_SCHD_SK").alias("COMSN_SCHD_SK"),
        col("COMSN_SCHD_TIER_CALC_METH_CD_SK").alias("COMSN_SCHD_TIER_CALC_METH_CD_SK"),
        col("PRM_THRU_THRESOLD_AMT").alias("PRM_THRU_THRESOLD_AMT"),
        col("TIER_AMT").alias("TIER_AMT"),
        col("TIER_PCT").alias("TIER_PCT"),
        col("TIER_UNIQ_KEY").alias("TIER_UNIQ_KEY")
    )
)

# Write the recycle output to Parquet (Scenario C for CHashedFileStage)
write_files(
    df_recycle,
    f"{adls_path}/hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# Prepare for DefaultUNK and DefaultNA (both constraints @INROWNUM=1)
df_fk2 = df_fk.withColumn("_rownum", row_number().over(Window.orderBy(lit(1))))

df_defaultUNK = (
    df_fk2
    .filter(col("_rownum") == 1)
    .select(
        lit(0).cast(IntegerType()).alias("COMSN_SCHD_TIER_SK"),
        lit(0).cast(IntegerType()).alias("SRC_SYS_CD_SK"),
        lit("UNK").alias("COMSN_SCHD_ID"),
        lit("UNK").alias("DURATN_EFF_DT_SK"),
        lit(0).cast(IntegerType()).alias("DURATN_STRT_PERD_NO"),
        lit(0.0).cast(DecimalType(38,10)).alias("PRM_FROM_THRSHLD_AMT"),
        lit(0).cast(IntegerType()).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(0).cast(IntegerType()).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(0).cast(IntegerType()).alias("COMSN_SCHD_SK"),
        lit("0").alias("COMSN_SCHD_TIER_CALC_METH_CD_SK"),
        lit(0.0).cast(DecimalType(38,10)).alias("PRM_THRU_THRESOLD_AMT"),
        lit(0.0).cast(DecimalType(38,10)).alias("TIER_AMT"),
        lit(0).cast(IntegerType()).alias("TIER_PCT"),
        lit(0).cast(IntegerType()).alias("TIER_UNIQ_KEY")
    )
)

df_defaultNA = (
    df_fk2
    .filter(col("_rownum") == 1)
    .select(
        lit(1).cast(IntegerType()).alias("COMSN_SCHD_TIER_SK"),
        lit(1).cast(IntegerType()).alias("SRC_SYS_CD_SK"),
        lit("NA").alias("COMSN_SCHD_ID"),
        lit("NA").alias("DURATN_EFF_DT_SK"),
        lit(1).cast(IntegerType()).alias("DURATN_STRT_PERD_NO"),
        lit(0.0).cast(DecimalType(38,10)).alias("PRM_FROM_THRSHLD_AMT"),
        lit(1).cast(IntegerType()).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(1).cast(IntegerType()).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(1).cast(IntegerType()).alias("COMSN_SCHD_SK"),
        lit("1").alias("COMSN_SCHD_TIER_CALC_METH_CD_SK"),
        lit(0.0).cast(DecimalType(38,10)).alias("PRM_THRU_THRESOLD_AMT"),
        lit(0.0).cast(DecimalType(38,10)).alias("TIER_AMT"),
        lit(0).cast(IntegerType()).alias("TIER_PCT"),
        lit(1).cast(IntegerType()).alias("TIER_UNIQ_KEY")
    )
)

# Collector logic: union of Fkey, DefaultUNK, DefaultNA
df_collector = df_fkey.union(df_defaultUNK).union(df_defaultNA)

# Final select with rpad for char/varchar columns
df_final = (
    df_collector
    .withColumn("COMSN_SCHD_ID", rpad(col("COMSN_SCHD_ID"), 4, " "))
    .withColumn("DURATN_EFF_DT_SK", rpad(col("DURATN_EFF_DT_SK"), 10, " "))
    .withColumn("COMSN_SCHD_TIER_CALC_METH_CD_SK", rpad(col("COMSN_SCHD_TIER_CALC_METH_CD_SK"), 1, " "))
    .select(
        "COMSN_SCHD_TIER_SK",
        "SRC_SYS_CD_SK",
        "COMSN_SCHD_ID",
        "DURATN_EFF_DT_SK",
        "DURATN_STRT_PERD_NO",
        "PRM_FROM_THRSHLD_AMT",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "COMSN_SCHD_SK",
        "COMSN_SCHD_TIER_CALC_METH_CD_SK",
        "PRM_THRU_THRESOLD_AMT",
        "TIER_AMT",
        "TIER_PCT",
        "TIER_UNIQ_KEY"
    )
)

# Write final output (CSeqFileStage)
write_files(
    df_final,
    f"{adls_path}/load/{OutFile}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)