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
# MAGIC ^1_4 12/27/05 15:23:50 Batch  13876_55437 PROMOTE bckcett testIDS30 u10913 Ollie Move to test
# MAGIC ^1_4 12/27/05 15:20:19 Batch  13876_55225 INIT bckcett devlIDS30 u10913 Ollie Move To Test
# MAGIC ^1_3 12/27/05 15:18:02 Batch  13876_55090 INIT bckcett devlIDS30 u10913 Ollie Move to Test
# MAGIC ^1_2 12/23/05 11:12:01 Batch  13872_40337 INIT bckcett devlIDS30 u10913 Ollie Move Income/Commisions to test
# MAGIC ^1_1 12/23/05 10:39:46 Batch  13872_38400 INIT bckcett devlIDS30 u10913 Ollie Moving Income / Commission to Test
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsComsnMnlAdjFkey
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
# MAGIC                             AGNT_ID is used to retrieve the foreign key for AGNT_SK instead of COCE_ID, because AGNT_ID is directly mapped from COCE_ID.
# MAGIC                             
# MAGIC PROCESSING:  Natural keys are converted to surrogate keys
# MAGIC 
# MAGIC OUTPUTS:   CLS table load file
# MAGIC                      hf_recycle - If recycling was used for this job records would be written to this hash file that had errors.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC             Oliver Nielsen  -  10/20/2005  -  Originally programmed
# MAGIC             Tao Luo - 10/20/2005 - Modified to IdsComsnMnlAdjFkey
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
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


TmpOutFile = get_widget_value('TmpOutFile','IdsComsnMnlAdjExtr.dat.fkey')
InFile = get_widget_value('InFile','IdsComsnMnlAdjExtr.dat.pkey')
Logging = get_widget_value('Logging','X')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')

schema_IdsComsnMnlAdjExtr = T.StructType([
    T.StructField("JOB_EXCTN_RCRD_ERR_SK", T.IntegerType(), nullable=False),
    T.StructField("INSRT_UPDT_CD", T.StringType(), nullable=False),
    T.StructField("DISCARD_IN", T.StringType(), nullable=False),
    T.StructField("PASS_THRU_IN", T.StringType(), nullable=False),
    T.StructField("FIRST_RECYC_DT", T.TimestampType(), nullable=False),
    T.StructField("ERR_CT", T.IntegerType(), nullable=False),
    T.StructField("RECYCLE_CT", T.DecimalType(38,10), nullable=False),
    T.StructField("SRC_SYS_CD", T.StringType(), nullable=False),
    T.StructField("PRI_KEY_STRING", T.StringType(), nullable=False),
    T.StructField("COMSN_MNL_ADJ_SK", T.IntegerType(), nullable=False),
    T.StructField("AGNT_ID", T.StringType(), nullable=False),
    T.StructField("SEQ_NO", T.IntegerType(), nullable=False),
    T.StructField("CRT_RUN_CYC_EXCTN_SK", T.IntegerType(), nullable=False),
    T.StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", T.IntegerType(), nullable=False),
    T.StructField("COAR_ID", T.StringType(), nullable=False),
    T.StructField("COCE_ID_PAYEE", T.StringType(), nullable=False),
    T.StructField("LOBD_ID", T.StringType(), nullable=False),
    T.StructField("COCA_PYMT_TYPE", T.StringType(), nullable=False),
    T.StructField("COCA_MCTR_RSN", T.StringType(), nullable=False),
    T.StructField("COCA_STS", T.StringType(), nullable=False),
    T.StructField("COAG_EFF_DT", T.StringType(), nullable=False),
    T.StructField("COCA_PAID_DT", T.StringType(), nullable=False),
    T.StructField("ADJ_AMT", T.DecimalType(38,10), nullable=False),
    T.StructField("ADJ_DESC", T.StringType(), nullable=True)
])

df_IdsComsnMnlAdjExtr = (
    spark.read.format("csv")
    .option("header", "false")
    .option("quote", "\"")
    .schema(schema_IdsComsnMnlAdjExtr)
    .load(f"{adls_path}/key/" + InFile)
)

df_enriched = (
    df_IdsComsnMnlAdjExtr
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("svAgnt", GetFkeyAgnt(F.col("SRC_SYS_CD"), F.col("COMSN_MNL_ADJ_SK"), F.col("AGNT_ID"), F.lit(Logging)))
    .withColumn("svComsnArgmt", GetFkeyComsnArgmt(F.col("SRC_SYS_CD"), F.col("COMSN_MNL_ADJ_SK"), F.col("COAR_ID"), F.lit(Logging)))
    .withColumn("svPdAgnt", GetFkeyAgnt(F.col("SRC_SYS_CD"), F.col("COMSN_MNL_ADJ_SK"), F.col("COCE_ID_PAYEE"), F.lit(Logging)))
    .withColumn("svComsnMnlAdjLobCd", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("COMSN_MNL_ADJ_SK"), F.lit("CLAIM LINE LOB"), F.col("LOBD_ID"), F.lit(Logging)))
    .withColumn("svComsnMnlAdjPaymtTypCd", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("COMSN_MNL_ADJ_SK"), F.lit("COMMISSION MANUAL ADJUSTMENT PAYMENT TYPE"), F.col("COCA_PYMT_TYPE"), F.lit(Logging)))
    .withColumn("svComsnMnlAdjRsnCd", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("COMSN_MNL_ADJ_SK"), F.lit("COMMISSION MANUAL ADJUSTMENT REASON"), F.col("COCA_MCTR_RSN"), F.lit(Logging)))
    .withColumn("svComsnMnlAdjSttusCd", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("COMSN_MNL_ADJ_SK"), F.lit("COMMISSION MANUAL ADJUSTMENT STATUS"), F.col("COCA_STS"), F.lit(Logging)))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("COMSN_MNL_ADJ_SK")))
)

df_fkey = df_enriched.filter(
    (F.col("ErrCount") == 0) | (F.col("PassThru") == 'Y')
).select(
    F.col("COMSN_MNL_ADJ_SK").alias("COMSN_MNL_ADJ_SK"),
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("AGNT_ID").alias("AGNT_ID"),
    F.col("SEQ_NO").alias("SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(F.col("svAgnt").isNull(), F.lit(0)).otherwise(F.col("svAgnt")).alias("AGNT_SK"),
    F.when(F.col("svComsnArgmt").isNull(), F.lit(0)).otherwise(F.col("svComsnArgmt")).alias("COMSN_ARGMT_SK"),
    F.when(F.col("svPdAgnt").isNull(), F.lit(0)).otherwise(F.col("svPdAgnt")).alias("PD_AGNT_SK"),
    F.when(F.col("svComsnMnlAdjLobCd").isNull(), F.lit(0)).otherwise(F.col("svComsnMnlAdjLobCd")).alias("COMSN_MNL_ADJ_LOB_CD_SK"),
    F.when(F.col("svComsnMnlAdjPaymtTypCd").isNull(), F.lit(0)).otherwise(F.col("svComsnMnlAdjPaymtTypCd")).alias("COMSN_MNL_ADJ_PAYMT_TYP_CD_SK"),
    F.when(F.col("svComsnMnlAdjRsnCd").isNull(), F.lit(0)).otherwise(F.col("svComsnMnlAdjRsnCd")).alias("COMSN_MNL_ADJ_RSN_CD_SK"),
    F.when(F.col("svComsnMnlAdjSttusCd").isNull(), F.lit(0)).otherwise(F.col("svComsnMnlAdjSttusCd")).alias("COMSN_MNL_ADJ_STTUS_CD_SK"),
    F.col("COAG_EFF_DT").alias("COMSN_AGMNT_EFF_DT_SK"),
    F.col("COCA_PAID_DT").alias("PD_DT_SK"),
    F.col("ADJ_AMT").alias("ADJ_AMT"),
    F.col("ADJ_DESC").alias("ADJ_DESC")
)

df_recycle = df_enriched.filter(
    F.col("ErrCount") > 0
).select(
    GetRecycleKey(F.col("COMSN_MNL_ADJ_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("COMSN_MNL_ADJ_SK").alias("COMSN_MNL_ADJ_SK"),
    F.col("AGNT_ID").alias("AGNT_ID"),
    F.col("SEQ_NO").alias("SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("COAR_ID").alias("COAR_ID"),
    F.col("COCE_ID_PAYEE").alias("COCE_ID_PAYEE"),
    F.col("LOBD_ID").alias("LOBD_ID"),
    F.col("COCA_PYMT_TYPE").alias("COCA_PYMT_TYPE"),
    F.col("COCA_MCTR_RSN").alias("COCA_MCTR_RSN"),
    F.col("COCA_STS").alias("COCA_STS"),
    F.col("COAG_EFF_DT").alias("COAG_EFF_DT"),
    F.col("COCA_PAID_DT").alias("COCA_PAID_DT"),
    F.col("ADJ_AMT").alias("ADJ_AMT"),
    F.col("ADJ_DESC").alias("ADJ_DESC")
)

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

df_defaultUNK = spark.createDataFrame(
    [
        (
            0, 0, 'UNK', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 'UNK', 'UNK', 0, 'UNK'
        )
    ],
    [
        "COMSN_MNL_ADJ_SK",
        "SRC_SYS_CD_SK",
        "AGNT_ID",
        "SEQ_NO",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "AGNT_SK",
        "COMSN_ARGMT_SK",
        "PD_AGNT_SK",
        "COMSN_MNL_ADJ_LOB_CD_SK",
        "COMSN_MNL_ADJ_PAYMT_TYP_CD_SK",
        "COMSN_MNL_ADJ_RSN_CD_SK",
        "COMSN_MNL_ADJ_STTUS_CD_SK",
        "COMSN_AGMNT_EFF_DT_SK",
        "PD_DT_SK",
        "ADJ_AMT",
        "ADJ_DESC"
    ]
)

df_defaultNA = spark.createDataFrame(
    [
        (
            1, 1, 'NA', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 'NA', 'NA', 0, 'NA'
        )
    ],
    [
        "COMSN_MNL_ADJ_SK",
        "SRC_SYS_CD_SK",
        "AGNT_ID",
        "SEQ_NO",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "AGNT_SK",
        "COMSN_ARGMT_SK",
        "PD_AGNT_SK",
        "COMSN_MNL_ADJ_LOB_CD_SK",
        "COMSN_MNL_ADJ_PAYMT_TYP_CD_SK",
        "COMSN_MNL_ADJ_RSN_CD_SK",
        "COMSN_MNL_ADJ_STTUS_CD_SK",
        "COMSN_AGMNT_EFF_DT_SK",
        "PD_DT_SK",
        "ADJ_AMT",
        "ADJ_DESC"
    ]
)

df_collector = df_fkey.unionByName(df_defaultUNK).unionByName(df_defaultNA)

df_collector_final = (
    df_collector
    .withColumn("COMSN_AGMNT_EFF_DT_SK", rpad("COMSN_AGMNT_EFF_DT_SK", 10, " "))
    .withColumn("PD_DT_SK", rpad("PD_DT_SK", 10, " "))
    .select(
        "COMSN_MNL_ADJ_SK",
        "SRC_SYS_CD_SK",
        "AGNT_ID",
        "SEQ_NO",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "AGNT_SK",
        "COMSN_ARGMT_SK",
        "PD_AGNT_SK",
        "COMSN_MNL_ADJ_LOB_CD_SK",
        "COMSN_MNL_ADJ_PAYMT_TYP_CD_SK",
        "COMSN_MNL_ADJ_RSN_CD_SK",
        "COMSN_MNL_ADJ_STTUS_CD_SK",
        "COMSN_AGMNT_EFF_DT_SK",
        "PD_DT_SK",
        "ADJ_AMT",
        "ADJ_DESC"
    )
)

write_files(
    df_collector_final,
    f"{adls_path}/load/" + TmpOutFile,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)