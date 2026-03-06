# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_4 02/10/09 14:53:08 Batch  15017_53592 PROMOTE bckcetl ids20 dsadm bls for rt
# MAGIC ^1_4 02/10/09 14:44:16 Batch  15017_53058 INIT bckcett testIDSnew dsadm bls for rt
# MAGIC ^1_2 01/16/09 09:14:22 Batch  14992_33385 PROMOTE bckcett testIDSnew u06640 Ralph
# MAGIC ^1_2 01/15/09 15:57:37 Batch  14991_57464 PROMOTE bckcett testIDSnew u06640 Ralph
# MAGIC ^1_2 01/15/09 15:48:27 Batch  14991_56913 INIT bckcett devlIDS u06640 Ralph
# MAGIC ^1_1 01/15/09 10:49:15 Batch  14991_38962 INIT bckcett devlIDS u06640 Ralph
# MAGIC ^1_3 01/14/09 13:20:52 Batch  14990_48060 INIT bckcett devlIDS u03651 steffy
# MAGIC ^1_2 12/11/08 11:15:25 Batch  14956_40534 INIT bckcett devlIDS u03651 steffy
# MAGIC ^1_1 01/28/08 13:39:53 Batch  14638_49197 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 01/10/08 10:01:22 Batch  14620_36086 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 12/26/07 15:33:58 Batch  14605_56043 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/23/07 15:42:37 Batch  14572_56566 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_2 10/30/07 15:21:36 Batch  14548_55318 INIT bckcetl ids20 dcg01 sa - for Member Accum Hit List - wasn't using the driver table
# MAGIC ^1_1 10/29/07 14:51:28 Batch  14547_53489 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_1 09/25/07 16:07:19 Batch  14513_58043 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_2 09/18/07 09:05:03 Batch  14506_32892 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 08/23/07 12:38:45 Batch  14480_45530 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_7 03/28/07 15:08:03 Batch  14332_54485 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_3 03/23/07 14:23:20 Batch  14327_51805 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 03/07/07 16:30:43 Batch  14311_59451 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 02/02/07 11:39:23 Batch  14278_41978 PROMOTE bckcetl ids20 dsadm Keith for Hugh
# MAGIC ^1_1 02/02/07 11:20:49 Batch  14278_40857 INIT bckcett testIDS30 dsadm Keith for Ralph
# MAGIC ^1_5 01/29/07 14:33:50 Batch  14274_52442 PROMOTE bckcett testIDS30 u11141 Hugh Sisson
# MAGIC ^1_5 01/29/07 14:16:26 Batch  14274_51398 INIT bckcett devlIDS30 u11141 Hugh Sisson
# MAGIC ^1_4 01/29/07 14:09:25 Batch  14274_50971 INIT bckcett devlIDS30 u11141 Hugh Sisson
# MAGIC ^1_3 01/25/07 15:43:59 Batch  14270_56676 INIT bckcett devlIDS30 u11141 Hugh Sisson
# MAGIC ^1_2 11/17/06 17:15:46 Batch  14201_62175 INIT bckcett devlIDS30 u11141 Hugh Sisson - GRGR_ID fix
# MAGIC ^1_1 11/17/06 11:12:19 Batch  14201_40344 INIT bckcett devlIDS30 u06640 Ralph
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:
# MAGIC                                IdsSubPcaFkey
# MAGIC 
# MAGIC DESCRIPTION:
# MAGIC                                Foreign Key Building for load file
# MAGIC 
# MAGIC INPUTS:
# MAGIC                                Temporary output file from FctsSubPcaExtr
# MAGIC   
# MAGIC HASH FILES:
# MAGIC                                hf_recycle
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                            
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                                Lookups for surrogate key assignement.
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                                SUB_PCA.dat
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                                2006-10-11   Hugh Sisson      Originally Programmed
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                       Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                  ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parik                                2008-08-28      3567(Primary Key)         Added Source System Code SK as parameter    devlIDS                         Steph Goddard             09/02/2008
# MAGIC Ralph Tucker                  2009-01-09       3567(Primary Key)        Moved updates from production to devl              devlIDS

# MAGIC Read common record format file created in primary key job.
# MAGIC Writing Sequential File to /load
# MAGIC Create default rows for UNK and NA
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType, TimestampType
from pyspark.sql.functions import col, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


InFile = get_widget_value('InFile','SubPcaCrf.tmp')
Logging = get_widget_value('Logging','N')
RunCycle = get_widget_value('RunCycle','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')

schema_subpcacrf = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38, 10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("SUB_PCA_SK", IntegerType(), False),
    StructField("SUB_UNIQ_KEY", IntegerType(), False),
    StructField("SUB_PCA_ACCUM_PFX_ID", StringType(), False),
    StructField("PLN_YR_BEG_DT", StringType(), False),
    StructField("EFF_DT", DateType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("GRP", StringType(), False),
    StructField("SUB", IntegerType(), False),
    StructField("SUB_PCA_CAROVR_CALC_RULE_CD", StringType(), False),
    StructField("TERM_DT", DateType(), False),
    StructField("ALLOC_AMT", DecimalType(38, 10), False),
    StructField("BAL_AMT", DecimalType(38, 10), False),
    StructField("CAROVR_AMT", DecimalType(38, 10), False),
    StructField("MAX_CAROVR_AMT", DecimalType(38, 10), False),
    StructField("PD_AMT", DecimalType(38, 10), False)
])

df_subpcacrf = (
    spark.read.format("csv")
    .option("sep", ",")
    .option("quote", '"')
    .option("header", "false")
    .schema(schema_subpcacrf)
    .load(f"{adls_path}/key/{InFile}")
)

df_trns = (
    df_subpcacrf
    .withColumn("GrpSk", GetFkeyGrp(col("SRC_SYS_CD"), col("SUB_PCA_SK"), col("GRP"), lit(Logging)))
    .withColumn("SubSk", GetFkeySub(col("SRC_SYS_CD"), col("SUB_PCA_SK"), col("SUB"), lit(Logging)))
    .withColumn("PlnYrBegDtSk", GetFkeyDate(lit("IDS"), col("SUB_PCA_SK"), col("PLN_YR_BEG_DT"), lit(Logging)))
    .withColumn("EffDtSk", GetFkeyDate(lit("IDS"), col("SUB_PCA_SK"), col("EFF_DT"), lit(Logging)))
    .withColumn("TermDtSk", GetFkeyDate(lit("IDS"), col("SUB_PCA_SK"), col("TERM_DT"), lit(Logging)))
    .withColumn("CarOvrCalcRuleCdSk", GetFkeyCodes(col("SRC_SYS_CD"), col("SUB_PCA_SK"), lit("PERSONAL CARE ACCOUNT CARRYOVER RULE"), col("SUB_PCA_CAROVR_CALC_RULE_CD"), lit(Logging)))
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn("ErrCount", GetFkeyErrorCnt(col("SUB_PCA_SK")))
    .withColumn("SrcSysCdSk", lit(SrcSysCdSk))
)

df_subpcafkeyout = (
    df_trns
    .filter((col("ErrCount") == lit(0)) | (col("PassThru") == lit("Y")))
    .select(
        col("SUB_PCA_SK").alias("SUB_PCA_SK"),
        col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
        col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
        col("SUB_PCA_ACCUM_PFX_ID").alias("SUB_PCA_ACCUM_PFX_ID"),
        col("PlnYrBegDtSk").alias("PLN_YR_BEG_DT_SK"),
        col("EffDtSk").alias("EFF_DT"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("GrpSk").alias("GRP_SK"),
        col("SubSk").alias("SUB_SK"),
        col("CarOvrCalcRuleCdSk").alias("SUB_PCA_CAROVR_CALC_RULE_CD_SK"),
        col("TermDtSk").alias("TERM_DT"),
        col("ALLOC_AMT").alias("ALLOC_AMT"),
        col("BAL_AMT").alias("BAL_AMT"),
        col("CAROVR_AMT").alias("CAROVR_AMT"),
        col("MAX_CAROVR_AMT").alias("MAX_CAROVR_AMT"),
        col("PD_AMT").alias("PD_AMT")
    )
)

df_defaultunk = df_trns.limit(1).select(
    lit(0).alias("SUB_PCA_SK"),
    lit(0).alias("SRC_SYS_CD_SK"),
    lit(0).alias("SUB_UNIQ_KEY"),
    lit("UNK").alias("SUB_PCA_ACCUM_PFX_ID"),
    lit("UNK").alias("PLN_YR_BEG_DT_SK"),
    lit("UNK").alias("EFF_DT"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("GRP_SK"),
    lit(0).alias("SUB_SK"),
    lit(0).alias("SUB_PCA_CAROVR_CALC_RULE_CD_SK"),
    lit("UNK").alias("TERM_DT"),
    lit(0).alias("ALLOC_AMT"),
    lit(0).alias("BAL_AMT"),
    lit(0).alias("CAROVR_AMT"),
    lit(0).alias("MAX_CAROVR_AMT"),
    lit(0).alias("PD_AMT")
)

df_defaultna = df_trns.limit(1).select(
    lit(1).alias("SUB_PCA_SK"),
    lit(1).alias("SRC_SYS_CD_SK"),
    lit(1).alias("SUB_UNIQ_KEY"),
    lit("NA").alias("SUB_PCA_ACCUM_PFX_ID"),
    lit("NA").alias("PLN_YR_BEG_DT_SK"),
    lit("NA").alias("EFF_DT"),
    lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(1).alias("GRP_SK"),
    lit(1).alias("SUB_SK"),
    lit(1).alias("SUB_PCA_CAROVR_CALC_RULE_CD_SK"),
    lit("NA").alias("TERM_DT"),
    lit(0).alias("ALLOC_AMT"),
    lit(0).alias("BAL_AMT"),
    lit(0).alias("CAROVR_AMT"),
    lit(0).alias("MAX_CAROVR_AMT"),
    lit(0).alias("PD_AMT")
)

df_recycle = (
    df_trns
    .filter(col("ErrCount") > lit(0))
    .select(
        GetRecycleKey(col("SUB_PCA_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        col("DISCARD_IN").alias("DISCARD_IN"),
        col("PassThru").alias("PASS_THRU_IN"),
        col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        col("ErrCount").alias("ERR_CT"),
        (col("RECYCLE_CT") + lit(1)).alias("RECYCLE_CT"),
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        col("SUB_PCA_SK").alias("SUB_PCA_SK"),
        col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
        col("SUB_PCA_ACCUM_PFX_ID").alias("SUB_PCA_ACCUM_PFX_ID"),
        col("PLN_YR_BEG_DT").alias("PLN_YR_BEG_DT"),
        col("EFF_DT").alias("EFF_DT"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("GRP").alias("GRP"),
        col("SUB").alias("SUB"),
        col("SUB_PCA_CAROVR_CALC_RULE_CD").alias("SUB_PCA_CAROVR_CALC_RULE_CD"),
        col("TERM_DT").alias("TERM_DT"),
        col("ALLOC_AMT").alias("ALLOC_AMT"),
        col("BAL_AMT").alias("BAL_AMT"),
        col("CAROVR_AMT").alias("CAROVR_AMT"),
        col("MAX_CAROVR_AMT").alias("MAX_CAROVR_AMT"),
        col("PD_AMT").alias("PD_AMT")
    )
)

df_recycle_final = (
    df_recycle
    .withColumn("INSRT_UPDT_CD", rpad(col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", rpad(col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", rpad(col("PASS_THRU_IN"), 1, " "))
    .withColumn("PLN_YR_BEG_DT", rpad(col("PLN_YR_BEG_DT"), 10, " "))
    .withColumn("SUB_PCA_CAROVR_CALC_RULE_CD", rpad(col("SUB_PCA_CAROVR_CALC_RULE_CD"), 1, " "))
    .withColumn("SRC_SYS_CD", rpad(col("SRC_SYS_CD"), <...>, " "))
    .withColumn("PRI_KEY_STRING", rpad(col("PRI_KEY_STRING"), <...>, " "))
    .withColumn("SUB_PCA_ACCUM_PFX_ID", rpad(col("SUB_PCA_ACCUM_PFX_ID"), <...>, " "))
)

write_files(
    df_recycle_final,
    "hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_subpcafkeyout_collector = df_subpcafkeyout.select(
    col("SUB_PCA_SK"),
    col("SRC_SYS_CD_SK"),
    col("SUB_UNIQ_KEY"),
    col("SUB_PCA_ACCUM_PFX_ID"),
    col("PLN_YR_BEG_DT_SK"),
    col("EFF_DT").alias("EFF_DT_SK"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("GRP_SK"),
    col("SUB_SK"),
    col("SUB_PCA_CAROVR_CALC_RULE_CD_SK"),
    col("TERM_DT").alias("TERM_DT_SK"),
    col("ALLOC_AMT"),
    col("BAL_AMT"),
    col("CAROVR_AMT"),
    col("MAX_CAROVR_AMT"),
    col("PD_AMT")
)

df_defaultunk_collector = df_defaultunk.select(
    col("SUB_PCA_SK"),
    col("SRC_SYS_CD_SK"),
    col("SUB_UNIQ_KEY"),
    col("SUB_PCA_ACCUM_PFX_ID"),
    col("PLN_YR_BEG_DT_SK"),
    col("EFF_DT").alias("EFF_DT_SK"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("GRP_SK"),
    col("SUB_SK"),
    col("SUB_PCA_CAROVR_CALC_RULE_CD_SK"),
    col("TERM_DT").alias("TERM_DT_SK"),
    col("ALLOC_AMT"),
    col("BAL_AMT"),
    col("CAROVR_AMT"),
    col("MAX_CAROVR_AMT"),
    col("PD_AMT")
)

df_defaultna_collector = df_defaultna.select(
    col("SUB_PCA_SK"),
    col("SRC_SYS_CD_SK"),
    col("SUB_UNIQ_KEY"),
    col("SUB_PCA_ACCUM_PFX_ID"),
    col("PLN_YR_BEG_DT_SK"),
    col("EFF_DT").alias("EFF_DT_SK"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("GRP_SK"),
    col("SUB_SK"),
    col("SUB_PCA_CAROVR_CALC_RULE_CD_SK"),
    col("TERM_DT").alias("TERM_DT_SK"),
    col("ALLOC_AMT"),
    col("BAL_AMT"),
    col("CAROVR_AMT"),
    col("MAX_CAROVR_AMT"),
    col("PD_AMT")
)

df_merge_load = (
    df_subpcafkeyout_collector
    .unionByName(df_defaultunk_collector)
    .unionByName(df_defaultna_collector)
)

df_merge_load_final = (
    df_merge_load
    .withColumn("PLN_YR_BEG_DT_SK", rpad(col("PLN_YR_BEG_DT_SK"), 10, " "))
    .withColumn("EFF_DT_SK", rpad(col("EFF_DT_SK"), 10, " "))
    .withColumn("TERM_DT_SK", rpad(col("TERM_DT_SK"), 10, " "))
    .withColumn("SUB_PCA_ACCUM_PFX_ID", rpad(col("SUB_PCA_ACCUM_PFX_ID"), <...>, " "))
)

write_files(
    df_merge_load_final,
    f"{adls_path}/load/SUB_PCA.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)