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
# MAGIC 
# MAGIC JOB NAME:     IdsAgntIndvFkey
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
# MAGIC                             GetFkeyErrorCnt
# MAGIC 
# MAGIC PROCESSING:  Natural keys are converted to surrogate keys
# MAGIC 
# MAGIC OUTPUTS:   AGNT_INDV.dat file
# MAGIC                      hf_recycle - If recycling was used for this job records would be written to this hash file that had errors.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC             Hugh Sisson  -  10/11/2005  -  Originally programmed
# MAGIC 
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
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, TimestampType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


Logging = get_widget_value("Logging","N")
InFile = get_widget_value("InFile","FctsAgntIndvExtr.tmp")
OutFile = get_widget_value("OutFile","AGNT_INDIV.dat")
SrcSysCdSk = get_widget_value("SrcSysCdSk","105838")

schema_IdsAgntIndvExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38,10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("AGNT_INDV_SK", IntegerType(), nullable=False),
    StructField("AGNT_INDV_ID", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("AGNT_INDV_ENTY_TYP_CD", IntegerType(), nullable=False),
    StructField("SSN", StringType(), nullable=False),
    StructField("FIRST_NM", StringType(), nullable=True),
    StructField("MIDINIT", StringType(), nullable=True),
    StructField("LAST_NM", StringType(), nullable=True),
    StructField("INDV_TTL", StringType(), nullable=True)
])

df_IdsAgntIndvExtr = (
    spark.read
    .option("header", False)
    .option("quote", "\"")
    .option("delimiter", ",")
    .schema(schema_IdsAgntIndvExtr)
    .csv(f"{adls_path}/key/{InFile}")
)

df_IdsAgntIndvExtr = df_IdsAgntIndvExtr.withColumn("Logging", F.lit(Logging))

df_ForeignKey_in = (
    df_IdsAgntIndvExtr
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn(
        "svAgntIndvEntityCodeSK",
        GetFkeyCodes(
            F.lit("FACETS"),
            F.col("AGNT_INDV_SK"),
            F.lit("AGENT INDIVIDUAL ENTITY TYPE"),
            F.col("AGNT_INDV_ENTY_TYP_CD"),
            F.col("Logging")
        )
    )
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("AGNT_INDV_SK")))
)

df_ForeignKey_fkey = (
    df_ForeignKey_in
    .filter((F.col("ErrCount") == 0) | (F.col("PassThru") == "Y"))
    .select(
        F.col("AGNT_INDV_SK").alias("AGNT_INDV_SK"),
        F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        F.col("AGNT_INDV_ID").alias("AGNT_INDV_ID"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("svAgntIndvEntityCodeSK").alias("AGNT_INDV_ENTY_TYP_CD_SK"),
        F.col("SSN").alias("SSN"),
        F.col("FIRST_NM").alias("FIRST_NM"),
        F.col("MIDINIT").alias("MIDINIT"),
        F.col("LAST_NM").alias("LAST_NM"),
        F.col("INDV_TTL").alias("INDV_TTL")
    )
)

df_ForeignKey_recycle = (
    df_ForeignKey_in
    .filter(F.col("ErrCount") > 0)
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", GetRecycleKey(F.col("AGNT_INDV_SK")))
    .withColumn("RECYCLE_CT_plus_one", F.col("RECYCLE_CT") + F.lit(1))
    .select(
        F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("DISCARD_IN").alias("DISCARD_IN"),
        F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("ErrCount").alias("ERR_CT"),
        F.col("RECYCLE_CT_plus_one").alias("RECYCLE_CT"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("AGNT_INDV_SK").alias("AGNT_INDV_SK"),
        F.col("AGNT_INDV_ID").alias("AGNT_INDV_ID"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("AGNT_INDV_ENTY_TYP_CD").alias("AGNT_INDV_ENTY_TYP_CD"),
        F.col("SSN").alias("SSN"),
        F.col("FIRST_NM").alias("FIRST_NM"),
        F.col("MIDINIT").alias("MIDINIT"),
        F.col("LAST_NM").alias("LAST_NM"),
        F.col("INDV_TTL").alias("INDV_TTL")
    )
)

df_ForeignKey_recycle = df_ForeignKey_recycle.select(
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    "JOB_EXCTN_RCRD_ERR_SK",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "AGNT_INDV_SK",
    "AGNT_INDV_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "AGNT_INDV_ENTY_TYP_CD",
    F.rpad(F.col("SSN"), 20, " ").alias("SSN"),
    "FIRST_NM",
    F.rpad(F.col("MIDINIT"), 1, " ").alias("MIDINIT"),
    "LAST_NM",
    "INDV_TTL"
)

write_files(
    df_ForeignKey_recycle,
    "hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_ForeignKey_defaultUNK = spark.createDataFrame(
    [
        (
            0,
            0,
            "UNK",
            0,
            0,
            0,
            "UNK",
            "UNK",
            "U",
            "UNK",
            "UNK"
        )
    ],
    [
        "AGNT_INDV_SK",
        "SRC_SYS_CD_SK",
        "AGNT_INDV_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "AGNT_INDV_ENTY_TYP_CD_SK",
        "SSN",
        "FIRST_NM",
        "MIDINIT",
        "LAST_NM",
        "INDV_TTL"
    ]
)

df_ForeignKey_defaultNA = spark.createDataFrame(
    [
        (
            1,
            1,
            "NA",
            1,
            1,
            1,
            "NA",
            "NA",
            "X",
            "NA",
            "NA"
        )
    ],
    [
        "AGNT_INDV_SK",
        "SRC_SYS_CD_SK",
        "AGNT_INDV_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "AGNT_INDV_ENTY_TYP_CD_SK",
        "SSN",
        "FIRST_NM",
        "MIDINIT",
        "LAST_NM",
        "INDV_TTL"
    ]
)

df_Collector_out = df_ForeignKey_fkey.unionByName(df_ForeignKey_defaultUNK).unionByName(df_ForeignKey_defaultNA)

df_Collector_out = df_Collector_out.select(
    F.col("AGNT_INDV_SK"),
    F.col("SRC_SYS_CD_SK"),
    F.col("AGNT_INDV_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("AGNT_INDV_ENTY_TYP_CD_SK"),
    F.rpad(F.col("SSN"), 20, " ").alias("SSN"),
    "FIRST_NM",
    F.rpad(F.col("MIDINIT"), 1, " ").alias("MIDINIT"),
    "LAST_NM",
    "INDV_TTL"
)

write_files(
    df_Collector_out,
    f"{adls_path}/load/{OutFile}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)