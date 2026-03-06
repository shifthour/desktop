# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_3 05/12/09 13:30:39 Batch  15108_48642 PROMOTE bckcetl ids20 dsadm bls for rt
# MAGIC ^1_3 05/12/09 13:07:49 Batch  15108_47273 INIT bckcett:31540 testIDS dsadm BLS FOR RT
# MAGIC ^1_1 04/28/09 15:30:57 Batch  15094_55861 PROMOTE bckcett testIDS u03651 steph for Ralph
# MAGIC ^1_1 04/28/09 15:26:41 Batch  15094_55604 INIT bckcett devlIDS u03651 steffy
# MAGIC ^1_2 07/31/08 07:38:12 Batch  14823_27497 INIT bckcett devlIDS u08717 Brent
# MAGIC ^1_1 01/30/08 05:00:10 Batch  14640_18019 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 08/27/07 08:55:46 Batch  14484_32151 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 03/27/07 13:53:11 Batch  14331_49995 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_2 07/13/06 09:01:21 Batch  14074_32499 PROMOTE bckcetl ids20 dsadm J. Mahaffey for O. Nielsen
# MAGIC ^1_2 07/13/06 08:56:37 Batch  14074_32207 INIT bckcett testIDS30 dsadm J. Mahaffey for O. Nielsen
# MAGIC ^1_2 06/28/06 09:07:27 Batch  14059_32851 INIT bckcett testIDS30 u50871 By Tao Luo
# MAGIC ^1_1 06/28/06 09:02:32 Batch  14059_32558 INIT bckcett testIDS30 u50871 By Tao Luo
# MAGIC ^1_1 04/11/06 15:49:38 Batch  13981_56990 INIT bckcett testIDS30 dsadm J. Mahaffey for Hugh Sisson
# MAGIC ^1_1 03/22/06 11:59:36 Batch  13961_43182 PROMOTE bckcett testIDS30 u11141 Hugh Sisson
# MAGIC ^1_1 03/22/06 11:56:45 Batch  13961_43028 INIT bckcett devlIDS30 u11141 Hugh Sisson
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsMedMgtNoteFkey
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
# MAGIC OUTPUTS:   table load file
# MAGIC                      hf_recycle - If recycling was used for this job records would be written to this hash file that had errors.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC             Suzanne Saylor  -  02/14/2006  -  Originally programmed
# MAGIC 
# MAGIC Developer                          Date                    Project/Altiris #                     Change Description                                                       Development Project               Code Reviewer                      Date Reviewed
# MAGIC ----------------------------------      -------------------      -----------------------------------           ---------------------------------------------------------                                 ----------------------------------              ---------------------------------               -------------------------
# MAGIC Ralph Tucker                  03/27/2009      3808 - BICC                           Initial development                                                            devlIDS

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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DecimalType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


Logging = get_widget_value("Logging","X")
OutFile = get_widget_value("OutFile","MED_MGT_NOTE.dat")
RunID = get_widget_value("RunID","")
SrcSysCdSk = get_widget_value("SrcSysCdSk","105838")

schema_IdsMedMgtNoteExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38,10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("MED_MGT_NOTE_SK", IntegerType(), False),
    StructField("MED_MGT_NOTE_DTM", TimestampType(), False),
    StructField("MED_MGT_NOTE_INPT_DTM", TimestampType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("INPT_USER_SK", IntegerType(), False),
    StructField("UPDT_USER_SK", IntegerType(), False),
    StructField("MED_MGT_NOTE_CAT_CD_SK", IntegerType(), False),
    StructField("MED_MGT_NOTE_SUBJ_CD_SK", IntegerType(), False),
    StructField("UPDT_DTM", TimestampType(), False),
    StructField("CNTCT_NM", StringType(), False),
    StructField("CNTCT_PHN_NO", StringType(), True),
    StructField("CNTCT_PHN_NO_EXT", StringType(), True),
    StructField("CNTCT_FAX_NO", StringType(), True),
    StructField("CNTCT_FAX_NO_EXT", StringType(), True),
    StructField("SUM_DESC", StringType(), True)
])

df_IdsMedMgtNoteExtr = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "\"")
    .schema(schema_IdsMedMgtNoteExtr)
    .load(f"{adls_path}/key/FctsMedMgtNoteExtr.MedMgtNote.dat.{RunID}")
)

df_ForeignKey_temp = (
    df_IdsMedMgtNoteExtr
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("svMedMgtNoteCatCdSk", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("MED_MGT_NOTE_SK"), F.lit("MEDICAL MANAGEMENT NOTE CATEGORY"), F.col("MED_MGT_NOTE_CAT_CD_SK"), F.lit(Logging)))
    .withColumn("svMedMgtNoteSubjCdSk", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("MED_MGT_NOTE_SK"), F.lit("MEDICAL MANAGEMENT NOTE SUBJECT"), F.col("MED_MGT_NOTE_SUBJ_CD_SK"), F.lit(Logging)))
    .withColumn("svInptUserSk", GetFkeyAppUsr(F.col("SRC_SYS_CD"), F.col("MED_MGT_NOTE_SK"), F.col("INPT_USER_SK"), F.lit(Logging)))
    .withColumn("svUpdtUserSk", GetFkeyAppUsr(F.col("SRC_SYS_CD"), F.col("MED_MGT_NOTE_SK"), F.col("UPDT_USER_SK"), F.lit(Logging)))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("MED_MGT_NOTE_SK")))
)

df_ForeignKeyFkey = (
    df_ForeignKey_temp
    .filter((F.col("ErrCount") == 0) | (F.col("PassThru") == "Y"))
    .select(
        F.col("MED_MGT_NOTE_SK").alias("MED_MGT_NOTE_SK"),
        F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        F.col("MED_MGT_NOTE_DTM").alias("MED_MGT_NOTE_DTM"),
        F.col("MED_MGT_NOTE_INPT_DTM").alias("MED_MGT_NOTE_INPT_DTM"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("svInptUserSk").alias("INPT_USER_SK"),
        F.col("svUpdtUserSk").alias("UPDT_USER_SK"),
        F.col("svMedMgtNoteCatCdSk").alias("MED_MGT_NOTE_CAT_CD_SK"),
        F.col("svMedMgtNoteSubjCdSk").alias("MED_MGT_NOTE_SUBJ_CD_SK"),
        F.col("UPDT_DTM").alias("UPDT_DTM"),
        F.col("CNTCT_NM").alias("CNTCT_NM"),
        F.col("CNTCT_PHN_NO").alias("CNTCT_PHN_NO"),
        F.col("CNTCT_PHN_NO_EXT").alias("CNTCT_PHN_NO_EXT"),
        F.col("CNTCT_FAX_NO").alias("CNTCT_FAX_NO"),
        F.col("CNTCT_FAX_NO_EXT").alias("CNTCT_FAX_NO_EXT"),
        F.col("SUM_DESC").alias("SUM_DESC")
    )
)

df_ForeignKeyRecycle_intermediate = (
    df_ForeignKey_temp
    .filter(F.col("ErrCount") > 0)
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", GetRecycleKey(F.col("MED_MGT_NOTE_SK")))
    .withColumn("RECYCLE_CT", F.col("RECYCLE_CT") + F.lit(1))
)

df_ForeignKeyRecycle = df_ForeignKeyRecycle_intermediate.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ErrCount").alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("MED_MGT_NOTE_SK").alias("MED_MGT_NOTE_SK"),
    F.col("MED_MGT_NOTE_DTM").alias("MED_MGT_NOTE_DTM"),
    F.col("MED_MGT_NOTE_INPT_DTM").alias("MED_MGT_NOTE_INPT_DTM"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("INPT_USER_SK").alias("INPT_USER_SK"),
    F.col("UPDT_USER_SK").alias("UPDT_USER_SK"),
    F.col("MED_MGT_NOTE_CAT_CD_SK").alias("MED_MGT_NOTE_CAT_CD_SK"),
    F.col("MED_MGT_NOTE_SUBJ_CD_SK").alias("MED_MGT_NOTE_SUBJ_CD_SK"),
    F.col("UPDT_DTM").alias("UPDT_DTM"),
    F.col("CNTCT_NM").alias("CNTCT_NM"),
    F.col("CNTCT_PHN_NO").alias("CNTCT_PHN_NO"),
    F.col("CNTCT_PHN_NO_EXT").alias("CNTCT_PHN_NO_EXT"),
    F.col("CNTCT_FAX_NO").alias("CNTCT_FAX_NO"),
    F.col("CNTCT_FAX_NO_EXT").alias("CNTCT_FAX_NO_EXT"),
    F.col("SUM_DESC").alias("SUM_DESC")
)

write_files(
    df_ForeignKeyRecycle,
    f"{adls_path}/hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_DefaultUNK = spark.createDataFrame(
    [
        (
            0,
            0,
            "1753-01-01 00:00:00.000000",
            "1753-01-01 00:00:00.000000",
            0,
            0,
            0,
            0,
            0,
            0,
            "1753-01-01 00:00:00.000000",
            "UNK",
            "UNK",
            "UNK",
            "UNK",
            "UNK",
            "UNK"
        )
    ],
    [
        "MED_MGT_NOTE_SK",
        "SRC_SYS_CD_SK",
        "MED_MGT_NOTE_DTM",
        "MED_MGT_NOTE_INPT_DTM",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "INPT_USER_SK",
        "UPDT_USER_SK",
        "MED_MGT_NOTE_CAT_CD_SK",
        "MED_MGT_NOTE_SUBJ_CD_SK",
        "UPDT_DTM",
        "CNTCT_NM",
        "CNTCT_PHN_NO",
        "CNTCT_PHN_NO_EXT",
        "CNTCT_FAX_NO",
        "CNTCT_FAX_NO_EXT",
        "SUM_DESC"
    ]
)

df_DefaultNA = spark.createDataFrame(
    [
        (
            1,
            1,
            "1753-01-01 00:00:00.000000",
            "1753-01-01 00:00:00.000000",
            1,
            1,
            1,
            1,
            1,
            1,
            "1753-01-01 00:00:00.000000",
            "NA",
            "NA",
            "NA",
            "NA",
            "NA",
            "NA"
        )
    ],
    [
        "MED_MGT_NOTE_SK",
        "SRC_SYS_CD_SK",
        "MED_MGT_NOTE_DTM",
        "MED_MGT_NOTE_INPT_DTM",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "INPT_USER_SK",
        "UPDT_USER_SK",
        "MED_MGT_NOTE_CAT_CD_SK",
        "MED_MGT_NOTE_SUBJ_CD_SK",
        "UPDT_DTM",
        "CNTCT_NM",
        "CNTCT_PHN_NO",
        "CNTCT_PHN_NO_EXT",
        "CNTCT_FAX_NO",
        "CNTCT_FAX_NO_EXT",
        "SUM_DESC"
    ]
)

df_Collector = df_ForeignKeyFkey.unionByName(df_DefaultUNK).unionByName(df_DefaultNA)

df_final = df_Collector.select(
    F.col("MED_MGT_NOTE_SK"),
    F.col("SRC_SYS_CD_SK"),
    F.col("MED_MGT_NOTE_DTM"),
    F.col("MED_MGT_NOTE_INPT_DTM"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("INPT_USER_SK"),
    F.col("UPDT_USER_SK"),
    F.col("MED_MGT_NOTE_CAT_CD_SK"),
    F.col("MED_MGT_NOTE_SUBJ_CD_SK"),
    F.col("UPDT_DTM"),
    F.col("CNTCT_NM"),
    F.col("CNTCT_PHN_NO"),
    F.rpad(F.col("CNTCT_PHN_NO_EXT"), 5, " ").alias("CNTCT_PHN_NO_EXT"),
    F.col("CNTCT_FAX_NO"),
    F.rpad(F.col("CNTCT_FAX_NO_EXT"), 5, " ").alias("CNTCT_FAX_NO_EXT"),
    F.col("SUM_DESC")
)

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