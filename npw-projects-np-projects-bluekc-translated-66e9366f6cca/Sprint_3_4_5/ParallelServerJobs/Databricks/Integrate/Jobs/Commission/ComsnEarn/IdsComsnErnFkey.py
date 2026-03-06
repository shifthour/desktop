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
# MAGIC JOB NAME:     IdsComsnErnFkey
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
# MAGIC             Suzanne Saylor  -  10/20/2005  -  Originally programmed
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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


InFile = get_widget_value('InFile','IdsComsnErnExtr.dat.pkey')
Logging = get_widget_value('Logging','X')
OutFile = get_widget_value('OutFile','COMSN_ERN.dat')
SrcSysCdSk = get_widget_value('SrcSysCdSk','105838')

schema_IdsCmsnErn = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), True),
    StructField("INSRT_UPDT_CD", StringType(), True),
    StructField("DISCARD_IN", StringType(), True),
    StructField("PASS_THRU_IN", StringType(), True),
    StructField("FIRST_RECYC_DT", TimestampType(), True),
    StructField("ERR_CT", IntegerType(), True),
    StructField("RECYCLE_CT", DecimalType(38,10), True),
    StructField("PRI_KEY_STRING", StringType(), True),
    StructField("COMSN_ERN_SK", IntegerType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("COMSN_BILL_REL_UNIQ_KEY", IntegerType(), True),
    StructField("LOBD_ID", StringType(), True),
    StructField("COMSN_SCHD_TIER_UNIQ_KEY", IntegerType(), True),
    StructField("SEQ_NO", IntegerType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("FNCL_COMSN_RPTNG_RCS_IN", StringType(), True),
    StructField("PD_DT_SK", StringType(), True),
    StructField("ADV_ERN_AMT", DecimalType(38,10), True),
    StructField("ERN_COMSN_AMT", DecimalType(38,10), True),
    StructField("FORGO_AMT", DecimalType(38,10), True),
    StructField("NET_COMSN_ERN_AMT", DecimalType(38,10), True),
    StructField("OVRD_AMT", DecimalType(38,10), True),
    StructField("PD_COMSN_AMT", DecimalType(38,10), True),
    StructField("RECON_HOLD_AMT", DecimalType(38,10), True),
    StructField("SRC_INCM_AMT", DecimalType(38,10), True),
    StructField("COCE_ID_PAYEE", StringType(), True)
])

df_IdsCmsnErn = (
    spark.read
    .schema(schema_IdsCmsnErn)
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "\"")
    .csv(f"{adls_path}/key/{InFile}")
)

df_ForeignKeyStageVars = (
    df_IdsCmsnErn
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn(
        "svComsnCalcLobCdSk",
        GetFkeyCodes(
            F.col("SRC_SYS_CD"),
            F.col("COMSN_ERN_SK"),
            F.lit("CLAIM LINE LOB"),
            F.col("LOBD_ID"),
            F.lit(Logging)
        )
    )
    .withColumn(
        "svComsnCalcSK",
        GetFkeyComsnCalc(
            F.col("SRC_SYS_CD"),
            F.col("COMSN_ERN_SK"),
            F.col("COMSN_BILL_REL_UNIQ_KEY"),
            F.col("LOBD_ID"),
            F.col("COMSN_SCHD_TIER_UNIQ_KEY"),
            F.lit(Logging)
        )
    )
    .withColumn(
        "svPayeAgntSk",
        GetFkeyAgnt(
            F.col("SRC_SYS_CD"),
            F.col("COMSN_ERN_SK"),
            F.col("COCE_ID_PAYEE"),
            F.lit(Logging)
        )
    )
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("COMSN_ERN_SK")))
)

df_Fkey = (
    df_ForeignKeyStageVars
    .filter((F.col("ErrCount") == 0) | (F.col("PassThru") == "Y"))
    .select(
        F.col("COMSN_ERN_SK").alias("COMSN_ERN_SK"),
        F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        F.col("COMSN_BILL_REL_UNIQ_KEY").alias("COMSN_BILL_REL_UNIQ_KEY"),
        F.col("svComsnCalcLobCdSk").alias("COMSN_CALC_LOB_CD_SK"),
        F.col("COMSN_SCHD_TIER_UNIQ_KEY").alias("COMSN_SCHD_TIER_UNIQ_KEY"),
        F.col("SEQ_NO").alias("SEQ_NO"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("svComsnCalcSK").alias("COMSN_CALC_SK"),
        F.col("svPayeAgntSk").alias("PAYE_AGNT_SK"),
        F.col("FNCL_COMSN_RPTNG_RCS_IN").alias("FNCL_COMSN_RPTNG_RCS_IN"),
        F.col("PD_DT_SK").alias("PD_DT_SK"),
        F.col("ADV_ERN_AMT").alias("ADV_ERN_AMT"),
        F.col("ERN_COMSN_AMT").alias("ERN_COMSN_AMT"),
        F.col("FORGO_AMT").alias("FORGO_AMT"),
        F.col("NET_COMSN_ERN_AMT").alias("NET_COMSN_ERN_AMT"),
        F.col("OVRD_AMT").alias("OVRD_AMT"),
        F.col("PD_COMSN_AMT").alias("PD_COMSN_AMT"),
        F.col("RECON_HOLD_AMT").alias("RECON_HOLD_AMT"),
        F.col("SRC_INCM_AMT").alias("SRC_INCM_AMT")
    )
)

df_Recycle = (
    df_ForeignKeyStageVars
    .filter(F.col("ErrCount") > 0)
    .select(
        GetRecycleKey(F.col("COMSN_ERN_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("DISCARD_IN").alias("DISCARD_IN"),
        F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("ErrCount").alias("ERR_CT"),
        (F.col("RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("COMSN_ERN_SK").alias("COMSN_ERN_SK"),
        F.col("COMSN_BILL_REL_UNIQ_KEY").alias("COMSN_BILL_REL_UNIQ_KEY"),
        F.col("LOBD_ID").alias("LOBD_ID"),
        F.col("COMSN_SCHD_TIER_UNIQ_KEY").alias("COMSN_SCHD_TIER_UNIQ_KEY"),
        F.col("SEQ_NO").alias("SEQ_NO"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("FNCL_COMSN_RPTNG_RCS_IN").alias("FNCL_COMSN_RPTNG_RCS_IN"),
        F.col("PD_DT_SK").alias("PD_DT_SK"),
        F.col("ADV_ERN_AMT").alias("ADV_ERN_AMT"),
        F.col("ERN_COMSN_AMT").alias("ERN_COMSN_AMT"),
        F.col("FORGO_AMT").alias("FORGO_AMT"),
        F.col("NET_COMSN_ERN_AMT").alias("NET_COMSN_ERN_AMT"),
        F.col("OVRD_AMT").alias("OVRD_AMT"),
        F.col("PD_COMSN_AMT").alias("PD_COMSN_AMT"),
        F.col("RECON_HOLD_AMT").alias("RECON_HOLD_AMT"),
        F.col("SRC_INCM_AMT").alias("SRC_INCM_AMT"),
        F.col("COCE_ID_PAYEE").alias("COCE_ID_PAYEE")
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

df_DefaultUNK = (
    df_ForeignKeyStageVars
    .limit(1)
    .select(
        F.lit(0).alias("COMSN_ERN_SK"),
        F.lit(0).alias("SRC_SYS_CD_SK"),
        F.lit(0).alias("COMSN_BILL_REL_UNIQ_KEY"),
        F.lit("0").alias("COMSN_CALC_LOB_CD_SK"),
        F.lit(0).alias("COMSN_SCHD_TIER_UNIQ_KEY"),
        F.lit(0).alias("SEQ_NO"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("COMSN_CALC_SK"),
        F.lit(0).alias("PAYE_AGNT_SK"),
        F.lit("0").alias("FNCL_COMSN_RPTNG_RCS_IN"),
        F.lit("UNK").alias("PD_DT_SK"),
        F.lit(0.00).alias("ADV_ERN_AMT"),
        F.lit(0.00).alias("ERN_COMSN_AMT"),
        F.lit(0.00).alias("FORGO_AMT"),
        F.lit(0.00).alias("NET_COMSN_ERN_AMT"),
        F.lit(0.00).alias("OVRD_AMT"),
        F.lit(0.00).alias("PD_COMSN_AMT"),
        F.lit(0.00).alias("RECON_HOLD_AMT"),
        F.lit(0.00).alias("SRC_INCM_AMT")
    )
)

df_DefaultNA = (
    df_ForeignKeyStageVars
    .limit(1)
    .select(
        F.lit(1).alias("COMSN_ERN_SK"),
        F.lit(1).alias("SRC_SYS_CD_SK"),
        F.lit(1).alias("COMSN_BILL_REL_UNIQ_KEY"),
        F.lit("1").alias("COMSN_CALC_LOB_CD_SK"),
        F.lit(1).alias("COMSN_SCHD_TIER_UNIQ_KEY"),
        F.lit(1).alias("SEQ_NO"),
        F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("COMSN_CALC_SK"),
        F.lit(1).alias("PAYE_AGNT_SK"),
        F.lit("1").alias("FNCL_COMSN_RPTNG_RCS_IN"),
        F.lit("NA").alias("PD_DT_SK"),
        F.lit(0.00).alias("ADV_ERN_AMT"),
        F.lit(0.00).alias("ERN_COMSN_AMT"),
        F.lit(0.00).alias("FORGO_AMT"),
        F.lit(0.00).alias("NET_COMSN_ERN_AMT"),
        F.lit(0.00).alias("OVRD_AMT"),
        F.lit(0.00).alias("PD_COMSN_AMT"),
        F.lit(0.00).alias("RECON_HOLD_AMT"),
        F.lit(0.00).alias("SRC_INCM_AMT")
    )
)

commonColsForCollector = [
    "COMSN_ERN_SK",
    "SRC_SYS_CD_SK",
    "COMSN_BILL_REL_UNIQ_KEY",
    "COMSN_CALC_LOB_CD_SK",
    "COMSN_SCHD_TIER_UNIQ_KEY",
    "SEQ_NO",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "COMSN_CALC_SK",
    "PAYE_AGNT_SK",
    "FNCL_COMSN_RPTNG_RCS_IN",
    "PD_DT_SK",
    "ADV_ERN_AMT",
    "ERN_COMSN_AMT",
    "FORGO_AMT",
    "NET_COMSN_ERN_AMT",
    "OVRD_AMT",
    "PD_COMSN_AMT",
    "RECON_HOLD_AMT",
    "SRC_INCM_AMT"
]

df_CollectorFkey = df_Fkey.select(commonColsForCollector)
df_CollectorDefaultUNK = df_DefaultUNK.select(commonColsForCollector)
df_CollectorDefaultNA = df_DefaultNA.select(commonColsForCollector)

df_Collector = df_CollectorFkey.union(df_CollectorDefaultUNK).union(df_CollectorDefaultNA)

df_COMSN_ERN = df_Collector.select(
    F.col("COMSN_ERN_SK").alias("COMSN_ERN_SK"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("COMSN_BILL_REL_UNIQ_KEY").alias("COMSN_BILL_REL_UNIQ_KEY"),
    F.rpad(F.col("COMSN_CALC_LOB_CD_SK"), 4, " ").alias("COMSN_CALC_LOB_CD_SK"),
    F.col("COMSN_SCHD_TIER_UNIQ_KEY").alias("COMSN_SCHD_TIER_UNIQ_KEY"),
    F.col("SEQ_NO").alias("SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("COMSN_CALC_SK").alias("COMSN_CALC_SK"),
    F.col("PAYE_AGNT_SK").alias("PAYE_AGNT_SK"),
    F.rpad(F.col("FNCL_COMSN_RPTNG_RCS_IN"), 1, " ").alias("FNCL_COMSN_RPTNG_PRCS_IN"),
    F.rpad(F.col("PD_DT_SK"), 10, " ").alias("PD_DT_SK"),
    F.col("ADV_ERN_AMT").alias("ADV_ERN_AMT"),
    F.col("ERN_COMSN_AMT").alias("ERN_COMSN_AMT"),
    F.col("FORGO_AMT").alias("FORGO_AMT"),
    F.col("NET_COMSN_ERN_AMT").alias("NET_COMSN_ERN_AMT"),
    F.col("OVRD_AMT").alias("OVRD_AMT"),
    F.col("PD_COMSN_AMT").alias("PD_COMSN_AMT"),
    F.col("RECON_HOLD_AMT").alias("RECON_HOLD_AMT"),
    F.col("SRC_INCM_AMT").alias("SRC_INCM_AMT")
)

write_files(
    df_COMSN_ERN,
    f"{adls_path}/load/{OutFile}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)