# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC ***************************************************************************************************************************************************************
# MAGIC Â© Copyright 2007, 2008 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by:
# MAGIC                     WellLifeClnclHealthScreenExtrSeq
# MAGIC 
# MAGIC Control Job Rerun Information: 
# MAGIC                     Previous Run Successful:    Restart, no other steps necessary
# MAGIC                     Previous Run Aborted:         Restart, no other steps necessary
# MAGIC 
# MAGIC Modifications:                         
# MAGIC                                                    Project/                                                                                                                                  Dev                                   Code                        Date
# MAGIC Developer             Date              Altiris #        Change Description                                                                                           project                             Reviewer                  Reviewed
# MAGIC ---------------------------  -------------------   -----------------  ---------------------------------------------------------------------------------------------------                ------------------------------------        ---------------------------     -------------------   
# MAGIC Bhoomi Dasari      2007-12-11    3036           Original Programming.                                                                                                                              Steph Goddard        12/14/2007
# MAGIC Hugh Sisson         2008-10-20    TTR-388    Changed definition of three columns and changed the way the                                                               Steph Goddard        10/28/2008
# MAGIC                                                                       health screen flat file is read from a single string of characters to 
# MAGIC                                                                       comma-delimited values
# MAGIC Kalyan Neelam     2010-04-14    4428           Added 8 new fields at the end. BODY_FAT_PCT, FSTNG_IN,                     IntegrateWrhsDevl            Steph Goddard          04/19/2010
# MAGIC                                                                       HA1C_NO, PRGNCY_IN, RFRL_TO_DM_IN, RFRL_TO_PHYS_IN, 
# MAGIC                                                                       RESCRN_IN, TSH_NO.
# MAGIC Bhoomi Dasari      2011-10-17     4673           Changed WAIST_CRCMFR_NO to Decimal(7,2) from Integer(10)                IntegrateWrhsDevl n          SAndrew                      2011-10-24
# MAGIC 
# MAGIC Bhoomi Dasari    2012-08-24     4830            Added 3 new fields                                                                                        IntegrareWrhsDevl            SAndrew                      2012-09-21     
# MAGIC                                                                       BONE_DENSITY_NO,
# MAGIC                                                                       HLTH_SCRN_SRC_SUBTYP_CD_SK,
# MAGIC                                                                       NCTN_TST_RSLT_CD_SK    
# MAGIC   
# MAGIC Kalyan Neelam     2012-12-03   4830            Updated Target Domain Name for HLTH_SCRN_SRC_SUBTYP_CD_SK  IntegrateNewDevl               Bhoomi Dasari              12/4/2012
# MAGIC                                                                       in stage variable svSrcSubTypCdSk
# MAGIC Kalyan Neelam    2013-06-11  AHY3.0        Added new column HLTH_SCRN_VNDR_CD_SK                                         IntegrateNewDevl             Bhoomi Dasari              6/14/2013
# MAGIC                                                Prod Supp
# MAGIC Dan Long           2013-09-25   TFS-3338    Change BMI_NO field to Decimal in IdsHlthScrnExtr, ForeignKey,                   IntegrateNewDevl              Kalyan Neelam          2013-10-01
# MAGIC                                                                     hf_recycle, Collector, and HLTH_SCRN

# MAGIC Set all foreign surrogate keys
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
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DecimalType
from pyspark.sql.functions import lit, col, row_number, rpad
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


Logging = get_widget_value("Logging","")
InFile = get_widget_value("InFile","")

schema_idsHlthScrnExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38,10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("HLTH_SCRN_SK", IntegerType(), nullable=False),
    StructField("MBR_UNIQ_KEY", IntegerType(), nullable=False),
    StructField("SCRN_DT_SK", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("GRP_SK", IntegerType(), nullable=False),
    StructField("MBR_SK", IntegerType(), nullable=False),
    StructField("HLTH_SCRN_MBR_GNDR_CD", StringType(), nullable=False),
    StructField("CUR_SMOKER_RISK_IN", StringType(), nullable=False),
    StructField("DBTC_RISK_IN", StringType(), nullable=False),
    StructField("FRMR_SMOKER_RISK_IN", StringType(), nullable=False),
    StructField("HEART_DSS_RISK_IN", StringType(), nullable=False),
    StructField("HI_CHLSTRL_RISK_IN", StringType(), nullable=False),
    StructField("HI_BP_RISK_IN", StringType(), nullable=False),
    StructField("EXRCS_LACK_RISK_IN", StringType(), nullable=False),
    StructField("OVERWT_RISK_IN", StringType(), nullable=False),
    StructField("STRESS_RISK_IN", StringType(), nullable=False),
    StructField("STROKE_RISK_IN", StringType(), nullable=False),
    StructField("BMI_NO", DecimalType(38,10), nullable=False),
    StructField("CHLSTRL_RATIO_NO", DecimalType(38,10), nullable=False),
    StructField("DIASTOLIC_BP_NO", IntegerType(), nullable=False),
    StructField("GLUCOSE_NO", IntegerType(), nullable=False),
    StructField("HDL_NO", IntegerType(), nullable=False),
    StructField("HT_INCH_NO", DecimalType(38,10), nullable=False),
    StructField("LDL_NO", IntegerType(), nullable=False),
    StructField("MBR_AGE_NO", IntegerType(), nullable=False),
    StructField("PSA_NO", DecimalType(38,10), nullable=True),
    StructField("SYSTOLIC_BP_NO", IntegerType(), nullable=False),
    StructField("TOT_CHLSTRL_NO", IntegerType(), nullable=False),
    StructField("TGL_NO", IntegerType(), nullable=False),
    StructField("WAIST_CRCMFR_NO", DecimalType(38,10), nullable=False),
    StructField("WT_NO", IntegerType(), nullable=False),
    StructField("BODY_FAT_PCT", DecimalType(38,10), nullable=True),
    StructField("FSTNG_IN", StringType(), nullable=False),
    StructField("HA1C_NO", DecimalType(38,10), nullable=True),
    StructField("PRGNCY_IN", StringType(), nullable=False),
    StructField("RFRL_TO_DM_IN", StringType(), nullable=False),
    StructField("RFRL_TO_PHYS_IN", StringType(), nullable=False),
    StructField("RESCRN_IN", StringType(), nullable=False),
    StructField("TSH_NO", DecimalType(38,10), nullable=True),
    StructField("BONE_DENSITY_NO", IntegerType(), nullable=True),
    StructField("HLTH_SCRN_SRC_SUBTYP", StringType(), nullable=True),
    StructField("NCTN_TST_RSLT_CD", StringType(), nullable=True),
    StructField("HLTH_SCRN_VNDR_CD", StringType(), nullable=False)
])

df_idsHlthScrnExtr = (
    spark.read.format("csv")
    .option("sep", ",")
    .option("quote", "\"")
    .option("header", "false")
    .schema(schema_idsHlthScrnExtr)
    .load(f"{adls_path}/key/{InFile}")
)

df_enriched = (
    df_idsHlthScrnExtr
    .withColumn("SrcSysCdSk", GetFkeyCodes(lit('IDS'), col("HLTH_SCRN_SK"), lit("SOURCE SYSTEM"), col("SRC_SYS_CD"), lit(Logging)))
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn("svHlthScrnMbrGndrSk", GetFkeyCodes(lit('WELLLIFE'), col("HLTH_SCRN_SK"), lit('GENDER'), col("HLTH_SCRN_MBR_GNDR_CD"), lit(Logging)))
    .withColumn("svSrcSubTypCdSk", GetFkeyCodes(col("SRC_SYS_CD"), col("HLTH_SCRN_SK"), lit('HEALTH SCREEN SOURCE SUBTYPE'), col("HLTH_SCRN_SRC_SUBTYP"), lit(Logging)))
    .withColumn("svNctnTstRsltCdSk", GetFkeyCodes(col("SRC_SYS_CD"), col("HLTH_SCRN_SK"), lit('NICOTINE TEST RESULT'), col("NCTN_TST_RSLT_CD"), lit(Logging)))
    .withColumn("svScrnDtSk", GetFkeyDate(lit('IDS'), col("HLTH_SCRN_SK"), col("SCRN_DT_SK"), lit(Logging)))
    .withColumn("svHlthScrnVndrCdSk", GetFkeyCodes(lit('BCBSKCCOMMON'), col("HLTH_SCRN_SK"), lit('HEALTH SCREEN VENDOR'), col("HLTH_SCRN_VNDR_CD"), lit(Logging)))
    .withColumn("ErrCount", GetFkeyErrorCnt(col("HLTH_SCRN_SK")))
)

df_fkey = (
    df_enriched
    .filter((col("ErrCount") == 0) | (col("PassThru") == 'Y'))
    .select(
        col("HLTH_SCRN_SK"),
        col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
        col("MBR_UNIQ_KEY"),
        col("svScrnDtSk").alias("SCRN_DT_SK"),
        col("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("GRP_SK"),
        col("MBR_SK"),
        col("svHlthScrnMbrGndrSk").alias("HLTH_SCRN_MBR_GNDR_CD_SK"),
        col("CUR_SMOKER_RISK_IN"),
        col("DBTC_RISK_IN"),
        col("FRMR_SMOKER_RISK_IN"),
        col("HEART_DSS_RISK_IN"),
        col("HI_CHLSTRL_RISK_IN"),
        col("HI_BP_RISK_IN"),
        col("EXRCS_LACK_RISK_IN"),
        col("OVERWT_RISK_IN"),
        col("STRESS_RISK_IN"),
        col("STROKE_RISK_IN"),
        col("BMI_NO"),
        col("CHLSTRL_RATIO_NO"),
        col("DIASTOLIC_BP_NO"),
        col("GLUCOSE_NO"),
        col("HDL_NO"),
        col("HT_INCH_NO"),
        col("LDL_NO"),
        col("MBR_AGE_NO"),
        col("PSA_NO"),
        col("SYSTOLIC_BP_NO"),
        col("TOT_CHLSTRL_NO"),
        col("TGL_NO"),
        col("WAIST_CRCMFR_NO"),
        col("WT_NO"),
        col("BODY_FAT_PCT"),
        col("FSTNG_IN"),
        col("HA1C_NO"),
        col("PRGNCY_IN"),
        col("RFRL_TO_DM_IN"),
        col("RFRL_TO_PHYS_IN"),
        col("RESCRN_IN"),
        col("TSH_NO"),
        col("BONE_DENSITY_NO"),
        col("svSrcSubTypCdSk").alias("HLTH_SCRN_SRC_SUBTYP_CD_SK"),
        col("svNctnTstRsltCdSk").alias("NCTN_TST_RSLT_CD_SK"),
        col("svHlthScrnVndrCdSk").alias("HLTH_SCRN_VNDR_CD_SK")
    )
)

df_recycle = (
    df_enriched
    .filter(col("ErrCount") > 0)
    .select(
        GetRecycleKey(col("HLTH_SCRN_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        col("INSRT_UPDT_CD"),
        col("DISCARD_IN"),
        col("PASS_THRU_IN"),
        col("FIRST_RECYC_DT"),
        col("ErrCount").alias("ERR_CT"),
        (col("RECYCLE_CT") + lit(1)).alias("RECYCLE_CT"),
        col("SRC_SYS_CD"),
        col("PRI_KEY_STRING"),
        col("HLTH_SCRN_SK"),
        col("MBR_UNIQ_KEY"),
        col("SCRN_DT_SK"),
        col("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("GRP_SK"),
        col("MBR_SK"),
        col("HLTH_SCRN_MBR_GNDR_CD").alias("HLTH_SCRN_MBR_GNDR_CD_SK"),
        col("CUR_SMOKER_RISK_IN"),
        col("DBTC_RISK_IN"),
        col("FRMR_SMOKER_RISK_IN"),
        col("HEART_DSS_RISK_IN"),
        col("HI_CHLSTRL_RISK_IN"),
        col("HI_BP_RISK_IN"),
        col("EXRCS_LACK_RISK_IN"),
        col("OVERWT_RISK_IN"),
        col("STRESS_RISK_IN"),
        col("STROKE_RISK_IN"),
        col("BMI_NO"),
        col("CHLSTRL_RATIO_NO"),
        col("DIASTOLIC_BP_NO"),
        col("GLUCOSE_NO"),
        col("HDL_NO"),
        col("HT_INCH_NO"),
        col("LDL_NO"),
        col("MBR_AGE_NO"),
        col("PSA_NO"),
        col("SYSTOLIC_BP_NO"),
        col("TOT_CHLSTRL_NO"),
        col("TGL_NO"),
        col("WAIST_CRCMFR_NO"),
        col("WT_NO"),
        col("BONE_DENSITY_NO"),
        col("HLTH_SCRN_SRC_SUBTYP"),
        col("NCTN_TST_RSLT_CD"),
        col("HLTH_SCRN_VNDR_CD")
    )
)

w = Window.orderBy(lit(1))

df_single_row = df_enriched.withColumn("rowNum", row_number().over(w))

df_defaultUNK = df_single_row.filter(col("rowNum") == 1).select(
    lit("0").alias("HLTH_SCRN_SK"),
    lit("0").alias("SRC_SYS_CD_SK"),
    lit("0").alias("MBR_UNIQ_KEY"),
    lit("UNK").alias("SCRN_DT_SK"),
    lit("0").alias("CRT_RUN_CYC_EXCTN_SK"),
    lit("0").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit("0").alias("GRP_SK"),
    lit("0").alias("MBR_SK"),
    lit("0").alias("HLTH_SCRN_MBR_GNDR_CD_SK"),
    lit("X").alias("CUR_SMOKER_RISK_IN"),
    lit("X").alias("DBTC_RISK_IN"),
    lit("X").alias("FRMR_SMOKER_RISK_IN"),
    lit("X").alias("HEART_DSS_RISK_IN"),
    lit("X").alias("HI_CHLSTRL_RISK_IN"),
    lit("X").alias("HI_BP_RISK_IN"),
    lit("X").alias("EXRCS_LACK_RISK_IN"),
    lit("X").alias("OVERWT_RISK_IN"),
    lit("X").alias("STRESS_RISK_IN"),
    lit("X").alias("STROKE_RISK_IN"),
    lit("0").alias("BMI_NO"),
    lit("0").alias("CHLSTRL_RATIO_NO"),
    lit("0").alias("DIASTOLIC_BP_NO"),
    lit("0").alias("GLUCOSE_NO"),
    lit("0").alias("HDL_NO"),
    lit("0").alias("HT_INCH_NO"),
    lit("0").alias("LDL_NO"),
    lit("0").alias("MBR_AGE_NO"),
    lit("0").alias("PSA_NO"),
    lit("0").alias("SYSTOLIC_BP_NO"),
    lit("0").alias("TOT_CHLSTRL_NO"),
    lit("0").alias("TGL_NO"),
    lit("0").alias("WAIST_CRCMFR_NO"),
    lit("0").alias("WT_NO"),
    lit("0").alias("BODY_FAT_PCT"),
    lit("X").alias("FSTNG_IN"),
    lit("0").alias("HA1C_NO"),
    lit("X").alias("PRGNCY_IN"),
    lit("X").alias("RFRL_TO_DM_IN"),
    lit("X").alias("RFRL_TO_PHYS_IN"),
    lit("X").alias("RESCRN_IN"),
    lit("0").alias("TSH_NO"),
    lit("0").alias("BONE_DENSITY_NO"),
    lit("0").alias("HLTH_SCRN_SRC_SUBTYP_CD_SK"),
    lit("0").alias("NCTN_TST_RSLT_CD_SK"),
    lit("0").alias("HLTH_SCRN_VNDR_CD_SK")
)

df_defaultNA = df_single_row.filter(col("rowNum") == 1).select(
    lit("1").alias("HLTH_SCRN_SK"),
    lit("1").alias("SRC_SYS_CD_SK"),
    lit("1").alias("MBR_UNIQ_KEY"),
    lit("NA").alias("SCRN_DT_SK"),
    lit("1").alias("CRT_RUN_CYC_EXCTN_SK"),
    lit("1").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit("1").alias("GRP_SK"),
    lit("1").alias("MBR_SK"),
    lit("1").alias("HLTH_SCRN_MBR_GNDR_CD_SK"),
    lit("X").alias("CUR_SMOKER_RISK_IN"),
    lit("X").alias("DBTC_RISK_IN"),
    lit("X").alias("FRMR_SMOKER_RISK_IN"),
    lit("X").alias("HEART_DSS_RISK_IN"),
    lit("X").alias("HI_CHLSTRL_RISK_IN"),
    lit("X").alias("HI_BP_RISK_IN"),
    lit("X").alias("EXRCS_LACK_RISK_IN"),
    lit("X").alias("OVERWT_RISK_IN"),
    lit("X").alias("STRESS_RISK_IN"),
    lit("X").alias("STROKE_RISK_IN"),
    lit("1").alias("BMI_NO"),
    lit("1").alias("CHLSTRL_RATIO_NO"),
    lit("1").alias("DIASTOLIC_BP_NO"),
    lit("1").alias("GLUCOSE_NO"),
    lit("1").alias("HDL_NO"),
    lit("1").alias("HT_INCH_NO"),
    lit("1").alias("LDL_NO"),
    lit("1").alias("MBR_AGE_NO"),
    lit("1").alias("PSA_NO"),
    lit("1").alias("SYSTOLIC_BP_NO"),
    lit("1").alias("TOT_CHLSTRL_NO"),
    lit("1").alias("TGL_NO"),
    lit("1").alias("WAIST_CRCMFR_NO"),
    lit("1").alias("WT_NO"),
    lit("1").alias("BODY_FAT_PCT"),
    lit("X").alias("FSTNG_IN"),
    lit("1").alias("HA1C_NO"),
    lit("X").alias("PRGNCY_IN"),
    lit("X").alias("RFRL_TO_DM_IN"),
    lit("X").alias("RFRL_TO_PHYS_IN"),
    lit("X").alias("RESCRN_IN"),
    lit("1").alias("TSH_NO"),
    lit("1").alias("BONE_DENSITY_NO"),
    lit("1").alias("HLTH_SCRN_SRC_SUBTYP_CD_SK"),
    lit("1").alias("NCTN_TST_RSLT_CD_SK"),
    lit("1").alias("HLTH_SCRN_VNDR_CD_SK")
)

df_collector = df_fkey.unionByName(df_defaultUNK).unionByName(df_defaultNA)

df_recycle_rpad = df_recycle.select(
    rpad(col("JOB_EXCTN_RCRD_ERR_SK").cast(StringType()), 0, " ").alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    rpad(col("SRC_SYS_CD"), 0, " ").alias("SRC_SYS_CD"),
    rpad(col("PRI_KEY_STRING"), 0, " ").alias("PRI_KEY_STRING"),
    col("HLTH_SCRN_SK"),
    col("MBR_UNIQ_KEY"),
    rpad(col("SCRN_DT_SK"), 10, " ").alias("SCRN_DT_SK"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("GRP_SK"),
    col("MBR_SK"),
    rpad(col("HLTH_SCRN_MBR_GNDR_CD_SK"), 20, " ").alias("HLTH_SCRN_MBR_GNDR_CD_SK"),
    rpad(col("CUR_SMOKER_RISK_IN"), 1, " ").alias("CUR_SMOKER_RISK_IN"),
    rpad(col("DBTC_RISK_IN"), 1, " ").alias("DBTC_RISK_IN"),
    rpad(col("FRMR_SMOKER_RISK_IN"), 1, " ").alias("FRMR_SMOKER_RISK_IN"),
    rpad(col("HEART_DSS_RISK_IN"), 1, " ").alias("HEART_DSS_RISK_IN"),
    rpad(col("HI_CHLSTRL_RISK_IN"), 1, " ").alias("HI_CHLSTRL_RISK_IN"),
    rpad(col("HI_BP_RISK_IN"), 1, " ").alias("HI_BP_RISK_IN"),
    rpad(col("EXRCS_LACK_RISK_IN"), 1, " ").alias("EXRCS_LACK_RISK_IN"),
    rpad(col("OVERWT_RISK_IN"), 1, " ").alias("OVERWT_RISK_IN"),
    rpad(col("STRESS_RISK_IN"), 1, " ").alias("STRESS_RISK_IN"),
    rpad(col("STROKE_RISK_IN"), 1, " ").alias("STROKE_RISK_IN"),
    col("BMI_NO"),
    col("CHLSTRL_RATIO_NO"),
    col("DIASTOLIC_BP_NO"),
    col("GLUCOSE_NO"),
    col("HDL_NO"),
    col("HT_INCH_NO"),
    col("LDL_NO"),
    col("MBR_AGE_NO"),
    col("PSA_NO"),
    col("SYSTOLIC_BP_NO"),
    col("TOT_CHLSTRL_NO"),
    col("TGL_NO"),
    col("WAIST_CRCMFR_NO"),
    col("WT_NO"),
    col("BONE_DENSITY_NO"),
    rpad(col("HLTH_SCRN_SRC_SUBTYP"), 10, " ").alias("HLTH_SCRN_SRC_SUBTYP"),
    rpad(col("NCTN_TST_RSLT_CD"), 10, " ").alias("NCTN_TST_RSLT_CD"),
    rpad(col("HLTH_SCRN_VNDR_CD"), 10, " ").alias("HLTH_SCRN_VNDR_CD")
)

write_files(
    df_recycle_rpad,
    "hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_loadFile = df_collector.select(
    rpad(col("HLTH_SCRN_SK").cast(StringType()), 0, " ").alias("HLTH_SCRN_SK"),
    rpad(col("SRC_SYS_CD_SK").cast(StringType()), 0, " ").alias("SRC_SYS_CD_SK"),
    rpad(col("MBR_UNIQ_KEY").cast(StringType()), 0, " ").alias("MBR_UNIQ_KEY"),
    rpad(col("SCRN_DT_SK"), 10, " ").alias("SCRN_DT_SK"),
    rpad(col("CRT_RUN_CYC_EXCTN_SK").cast(StringType()), 0, " ").alias("CRT_RUN_CYC_EXCTN_SK"),
    rpad(col("LAST_UPDT_RUN_CYC_EXCTN_SK").cast(StringType()), 0, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(col("GRP_SK").cast(StringType()), 0, " ").alias("GRP_SK"),
    rpad(col("MBR_SK").cast(StringType()), 0, " ").alias("MBR_SK"),
    rpad(col("HLTH_SCRN_MBR_GNDR_CD_SK").cast(StringType()), 0, " ").alias("HLTH_SCRN_MBR_GNDR_CD_SK"),
    rpad(col("CUR_SMOKER_RISK_IN"), 1, " ").alias("CUR_SMOKER_RISK_IN"),
    rpad(col("DBTC_RISK_IN"), 1, " ").alias("DBTC_RISK_IN"),
    rpad(col("FRMR_SMOKER_RISK_IN"), 1, " ").alias("FRMR_SMOKER_RISK_IN"),
    rpad(col("HEART_DSS_RISK_IN"), 1, " ").alias("HEART_DSS_RISK_IN"),
    rpad(col("HI_CHLSTRL_RISK_IN"), 1, " ").alias("HI_CHLSTRL_RISK_IN"),
    rpad(col("HI_BP_RISK_IN"), 1, " ").alias("HI_BP_RISK_IN"),
    rpad(col("EXRCS_LACK_RISK_IN"), 1, " ").alias("EXRCS_LACK_RISK_IN"),
    rpad(col("OVERWT_RISK_IN"), 1, " ").alias("OVERWT_RISK_IN"),
    rpad(col("STRESS_RISK_IN"), 1, " ").alias("STRESS_RISK_IN"),
    rpad(col("STROKE_RISK_IN"), 1, " ").alias("STROKE_RISK_IN"),
    col("BMI_NO"),
    col("CHLSTRL_RATIO_NO"),
    col("DIASTOLIC_BP_NO"),
    col("GLUCOSE_NO"),
    col("HDL_NO"),
    col("HT_INCH_NO"),
    col("LDL_NO"),
    col("MBR_AGE_NO"),
    col("PSA_NO"),
    col("SYSTOLIC_BP_NO"),
    col("TOT_CHLSTRL_NO"),
    col("TGL_NO"),
    col("WAIST_CRCMFR_NO"),
    col("WT_NO"),
    col("BODY_FAT_PCT"),
    rpad(col("FSTNG_IN"), 1, " ").alias("FSTNG_IN"),
    col("HA1C_NO"),
    rpad(col("PRGNCY_IN"), 1, " ").alias("PRGNCY_IN"),
    rpad(col("RFRL_TO_DM_IN"), 1, " ").alias("RFRL_TO_DM_IN"),
    rpad(col("RFRL_TO_PHYS_IN"), 1, " ").alias("RFRL_TO_PHYS_IN"),
    rpad(col("RESCRN_IN"), 1, " ").alias("RESCRN_IN"),
    col("TSH_NO"),
    col("BONE_DENSITY_NO"),
    col("HLTH_SCRN_SRC_SUBTYP_CD_SK"),
    col("NCTN_TST_RSLT_CD_SK"),
    col("HLTH_SCRN_VNDR_CD_SK")
)

write_files(
    df_loadFile,
    f"{adls_path}/load/HLTH_SCRN.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)