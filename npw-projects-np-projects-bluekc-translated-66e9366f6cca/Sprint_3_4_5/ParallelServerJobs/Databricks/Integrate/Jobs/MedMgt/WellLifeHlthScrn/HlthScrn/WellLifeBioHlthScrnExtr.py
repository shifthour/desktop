# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2011  Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by:
# MAGIC                     WellLifeBioHealthScreenExtrSeq
# MAGIC 
# MAGIC Processing:
# MAGIC                     Daily job that processes the Health Screening "pre-edited" file from WellLife
# MAGIC 
# MAGIC Control Job Rerun Information: 
# MAGIC                     Previous Run Successful:    Restart, no other steps necessary
# MAGIC                     Previous Run Aborted:         Restart, no other steps necessary
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                                    Project/                                                                                                                     Development                   Code                       Date
# MAGIC Developer             Date              Altiris #         Change Description                                                                              Project                             Reviewer                 Reviewed
# MAGIC ---------------------------  -------------------   -----------------   ---------------------------------------------------------------------------------------------------         ----------------------------------       ---------------------------     -------------------   
# MAGIC Terri O'Bryan       2011-07-01    4673 AHY   Original Programming.                                                                            IntegrateWrhsDevl                  SAndrew          2011-08-18   
# MAGIC 
# MAGIC Bhoomi Dasari    2011-09-12     4673 AHY  Changed "hf_etrnl_mbr_uniq_key" lookup from GNDR_CD_SK to       IntegrateWrhsDevl                SAndrew               2011-09-15
# MAGIC                                                                     GNDR_CD, to use existing Fkey job
# MAGIC 
# MAGIC Bhoomi Dasari    2011-10-17     4673           Changed WAIST_CRCMFR_NO to Decimal(7,2) from Integer(10)        IntegrateWrhsDevl             SAndew                 2011-10-21
# MAGIC 
# MAGIC Bhoomi Dasari    2012-08-24     4830          Added 3 new fields                                                                                IntegrareWrhsDevl         
# MAGIC                                                                     BONE_DENSITY_NO,
# MAGIC                                                                     HLTH_SCRN_SRC_SUBTYP_CD_SK,
# MAGIC                                                                     NCTN_TST_RSLT_CD_SK    
# MAGIC Kalyan Neelam   2013-06-11  AHY3.0 Prod Supp        Added new column HLTH_SCRN_VNDR_CD                      IntegrateNewDevl           Bhoomi Dasari        6/14/2013
# MAGIC Dan Long           2013-09-25   TFS-3338    Change BMI_NO field to Decimal in HealthScreenFlatFile,                     IntegrateNewDevl            Kalyan Neelam       2013-10-01
# MAGIC                                                                     Strip, BusinessRules, PrimaryKey, and WellLifeHlthScreenExtr

# MAGIC * COMMON EXTRACT *  Read 'pre-edited' WellLife health screen file; create #$FilePath#/ verified/ WELLLIFE_HLTHSCRN_ #SrcSysCd#_ LandingFile. dat.#RunID#
# MAGIC Balancing snapshot of source
# MAGIC  file #$FilePath#/load/ B_HLTH_SCRN. SrcSysCd#. dat.#RunID#
# MAGIC The Extraction for Balancing is done at an Intermediate Stage within the job to reduce the overhead involved and the complexity of duplicating the whole process
# MAGIC Strip Carriage Return, Line Feed, and Tab.
# MAGIC Lookup grp_sk, mbr_sk, mbr_gndr_cd_sk using pre validated mbr_uniq_key
# MAGIC Apply business logic
# MAGIC Assign primary surrogate key
# MAGIC Write Sequential File to /pkey for 'common' HLTH_SCRN load: #$FilePath#/key/ IdsHlthScrnExtr.HlthScrn.dat
# MAGIC 
# MAGIC NOTE: "original" HLTH_SCRN load: #$FilePath#/key/ WellLifeHlthScreenExtr. HlthScreen.dat
# MAGIC 
# MAGIC This file continues to be generated for KUMed and Labcorps indefinitely.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DateType,
    DecimalType
)
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


CurrRunCycle = get_widget_value("CurrRunCycle", "")
CurrRunDt = get_widget_value("CurrRunDt", "")
SrcSysCdSk = get_widget_value("SrcSysCdSk", "")
SrcSysCd = get_widget_value("SrcSysCd", "")
RunID = get_widget_value("RunID", "")
InFile = get_widget_value("InFile", "")

# Read from hashed file "hf_etrnl_mbr_uniq_key" (Scenario C => Parquet).
df_hf_etrnl_mbr_uniq_key = spark.read.parquet(f"{adls_path}/hf_etrnl_mbr_uniq_key.parquet")

# Read the CSeqFileStage "HealthScreenFlatFile"
schema_HealthScreenFlatFile = StructType([
    StructField("HLTH_SCRN_SK", IntegerType(), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("MBR_UNIQ_KEY", IntegerType(), nullable=False),
    StructField("SCRN_DT", DateType(), nullable=True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("GRP_SK", IntegerType(), nullable=False),
    StructField("MBR_SK", IntegerType(), nullable=False),
    StructField("HLTH_SCRN_MBR_GNDR_CD_SK", IntegerType(), nullable=False),
    StructField("CUR_SMOKER_RISK_IN", StringType(), nullable=True),
    StructField("DBTC_RISK_IN", StringType(), nullable=True),
    StructField("FRMR_SMOKER_RISK_IN", StringType(), nullable=True),
    StructField("HEART_DSS_RISK_IN", StringType(), nullable=True),
    StructField("HI_CHLSTRL_RISK_IN", StringType(), nullable=False),
    StructField("HI_BP_RISK_IN", StringType(), nullable=True),
    StructField("EXRCS_LACK_RISK_IN", StringType(), nullable=True),
    StructField("OVERWT_RISK_IN", StringType(), nullable=True),
    StructField("STRESS_RISK_IN", StringType(), nullable=True),
    StructField("STROKE_RISK_IN", StringType(), nullable=True),
    StructField("BMI_NO", DecimalType(38,10), nullable=True),
    StructField("CHLSTRL_RATIO_NO", DecimalType(38,10), nullable=False),
    StructField("DIASTOLIC_BP_NO", IntegerType(), nullable=True),
    StructField("GLUCOSE_NO", IntegerType(), nullable=True),
    StructField("HDL_NO", IntegerType(), nullable=True),
    StructField("HT_INCH_NO", DecimalType(38,10), nullable=False),
    StructField("LDL_NO", IntegerType(), nullable=True),
    StructField("MBR_AGE_NO", IntegerType(), nullable=True),
    StructField("PSA_NO", DecimalType(38,10), nullable=True),
    StructField("SYSTOLIC_BP_NO", IntegerType(), nullable=True),
    StructField("TOT_CHLSTRL_NO", IntegerType(), nullable=True),
    StructField("TGL_NO", IntegerType(), nullable=True),
    StructField("WAIST_CRCMFR_NO", DecimalType(38,10), nullable=True),
    StructField("WT_NO", IntegerType(), nullable=True),
    StructField("BODY_FAT_PCT", DecimalType(38,10), nullable=True),
    StructField("FSTNG_IN", StringType(), nullable=True),
    StructField("HA1C_NO", DecimalType(38,10), nullable=True),
    StructField("PRGNCY_IN", StringType(), nullable=True),
    StructField("RFRL_TO_DM_IN", StringType(), nullable=True),
    StructField("RFRL_TO_PHYS_IN", StringType(), nullable=True),
    StructField("RESCRN_IN", StringType(), nullable=True),
    StructField("TSH_NO", DecimalType(38,10), nullable=True),
    StructField("BONE_DENSITY_NO", IntegerType(), nullable=True),
    StructField("HLTH_SCRN_SRC_SUBTYP", StringType(), nullable=True),
    StructField("NCTN_TST_RSLT_CD", StringType(), nullable=True),
    StructField("HLTH_SCRN_VNDR_CD", StringType(), nullable=False)
])

df_HealthScreenFlatFile = (
    spark.read.format("csv")
    .option("sep", ",")
    .option("header", "false")
    .option("quote", "\"")
    .schema(schema_HealthScreenFlatFile)
    .load(f"{adls_path}/verified/{InFile}")
)

# "Strip" stage: left join to df_hf_etrnl_mbr_uniq_key
df_strip = (
    df_HealthScreenFlatFile.alias("Extract")
    .join(
        df_hf_etrnl_mbr_uniq_key.alias("mbr_uniq_key_lkup"),
        on=[F.col("Extract.MBR_UNIQ_KEY") == F.col("mbr_uniq_key_lkup.MBR_UNIQ_KEY")],
        how="left"
    )
    .select(
        F.col("Extract.HLTH_SCRN_SK").alias("HLTH_SCRN_SK"),
        F.col("Extract.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Extract.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.col("Extract.SCRN_DT").alias("SCRN_DT"),
        F.col("Extract.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("Extract.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.when(F.col("mbr_uniq_key_lkup.MBR_UNIQ_KEY").isNull(), F.lit(0)).otherwise(F.col("mbr_uniq_key_lkup.GRP_SK")).alias("GRP_SK"),
        F.when(F.col("mbr_uniq_key_lkup.MBR_UNIQ_KEY").isNull(), F.lit(0)).otherwise(F.col("mbr_uniq_key_lkup.MBR_SK")).alias("MBR_SK"),
        F.when(F.col("mbr_uniq_key_lkup.MBR_UNIQ_KEY").isNull(), F.lit("UNK")).otherwise(F.col("mbr_uniq_key_lkup.MBR_GNDR_CD")).alias("HLTH_SCRN_MBR_GNDR_CD"),
        F.col("Extract.CUR_SMOKER_RISK_IN").alias("CUR_SMOKER_RISK_IN"),
        F.col("Extract.DBTC_RISK_IN").alias("DBTC_RISK_IN"),
        F.col("Extract.FRMR_SMOKER_RISK_IN").alias("FRMR_SMOKER_RISK_IN"),
        F.col("Extract.HEART_DSS_RISK_IN").alias("HEART_DSS_RISK_IN"),
        F.col("Extract.HI_CHLSTRL_RISK_IN").alias("HI_CHLSTRL_RISK_IN"),
        F.col("Extract.HI_BP_RISK_IN").alias("HI_BP_RISK_IN"),
        F.col("Extract.EXRCS_LACK_RISK_IN").alias("EXRCS_LACK_RISK_IN"),
        F.col("Extract.OVERWT_RISK_IN").alias("OVERWT_RISK_IN"),
        F.col("Extract.STRESS_RISK_IN").alias("STRESS_RISK_IN"),
        F.col("Extract.STROKE_RISK_IN").alias("STROKE_RISK_IN"),
        F.col("Extract.BMI_NO").alias("BMI_NO"),
        F.col("Extract.CHLSTRL_RATIO_NO").alias("CHLSTRL_RATIO_NO"),
        F.col("Extract.DIASTOLIC_BP_NO").alias("DIASTOLIC_BP_NO"),
        F.col("Extract.GLUCOSE_NO").alias("GLUCOSE_NO"),
        F.col("Extract.HDL_NO").alias("HDL_NO"),
        F.col("Extract.HT_INCH_NO").alias("HT_INCH_NO"),
        F.col("Extract.LDL_NO").alias("LDL_NO"),
        F.col("Extract.MBR_AGE_NO").alias("MBR_AGE_NO"),
        F.col("Extract.PSA_NO").alias("PSA_NO"),
        F.col("Extract.SYSTOLIC_BP_NO").alias("SYSTOLIC_BP_NO"),
        F.col("Extract.TOT_CHLSTRL_NO").alias("TOT_CHLSTRL_NO"),
        F.col("Extract.TGL_NO").alias("TGL_NO"),
        F.col("Extract.WAIST_CRCMFR_NO").alias("WAIST_CRCMFR_NO"),
        F.col("Extract.WT_NO").alias("WT_NO"),
        F.col("Extract.BODY_FAT_PCT").alias("BODY_FAT_PCT"),
        F.col("Extract.FSTNG_IN").alias("FSTNG_IN"),
        F.col("Extract.HA1C_NO").alias("HA1C_NO"),
        F.col("Extract.PRGNCY_IN").alias("PRGNCY_IN"),
        F.col("Extract.RFRL_TO_DM_IN").alias("RFRL_TO_DM_IN"),
        F.col("Extract.RFRL_TO_PHYS_IN").alias("RFRL_TO_PHYS_IN"),
        F.col("Extract.RESCRN_IN").alias("RESCRN_IN"),
        F.col("Extract.TSH_NO").alias("TSH_NO"),
        F.col("Extract.BONE_DENSITY_NO").alias("BONE_DENSITY_NO"),
        F.col("Extract.HLTH_SCRN_SRC_SUBTYP").alias("HLTH_SCRN_SRC_SUBTYP"),
        F.col("Extract.NCTN_TST_RSLT_CD").alias("NCTN_TST_RSLT_CD"),
        F.col("Extract.HLTH_SCRN_VNDR_CD").alias("HLTH_SCRN_VNDR_CD")
    )
    .withColumn("SCRN_DT_SK", F.col("SCRN_DT").cast(StringType()))
)

df_businessRules = df_strip.withColumn("RowPassThru", F.lit("Y")).withColumn("SrcSysCd", F.lit(SrcSysCd))

df_snapshot = df_businessRules.select(
    F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
    F.col("MBR_UNIQ_KEY"),
    F.rpad(F.col("SCRN_DT_SK"), 10, " ").alias("SCRN_DT_SK")
)

write_files(
    df_snapshot,
    f"{adls_path}/load/B_HLTH_SCRN.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_transform = df_businessRules.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.lit("N"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("RowPassThru"), 1, " ").alias("PASS_THRU_IN"),
    F.col("CurrRunDt").alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.concat(F.col("SRC_SYS_CD"), F.lit(";"), F.col("MBR_UNIQ_KEY"), F.lit(";"), F.col("SCRN_DT_SK")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("HLTH_SCRN_SK"),
    F.col("MBR_UNIQ_KEY"),
    F.rpad(F.col("SCRN_DT_SK"), 10, " ").alias("SCRN_DT_SK"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("GRP_SK"),
    F.col("MBR_SK"),
    F.rpad(F.col("HLTH_SCRN_MBR_GNDR_CD"), 20, " ").alias("HLTH_SCRN_MBR_GNDR_CD"),
    F.rpad(F.col("CUR_SMOKER_RISK_IN"), 1, " ").alias("CUR_SMOKER_RISK_IN"),
    F.rpad(F.col("DBTC_RISK_IN"), 1, " ").alias("DBTC_RISK_IN"),
    F.rpad(F.col("FRMR_SMOKER_RISK_IN"), 1, " ").alias("FRMR_SMOKER_RISK_IN"),
    F.rpad(F.col("HEART_DSS_RISK_IN"), 1, " ").alias("HEART_DSS_RISK_IN"),
    F.rpad(F.col("HI_CHLSTRL_RISK_IN"), 1, " ").alias("HI_CHLSTRL_RISK_IN"),
    F.rpad(F.col("HI_BP_RISK_IN"), 1, " ").alias("HI_BP_RISK_IN"),
    F.rpad(F.col("EXRCS_LACK_RISK_IN"), 1, " ").alias("EXRCS_LACK_RISK_IN"),
    F.rpad(F.col("OVERWT_RISK_IN"), 1, " ").alias("OVERWT_RISK_IN"),
    F.rpad(F.col("STRESS_RISK_IN"), 1, " ").alias("STRESS_RISK_IN"),
    F.rpad(F.col("STROKE_RISK_IN"), 1, " ").alias("STROKE_RISK_IN"),
    F.col("BMI_NO"),
    F.col("CHLSTRL_RATIO_NO"),
    F.col("DIASTOLIC_BP_NO"),
    F.col("GLUCOSE_NO"),
    F.col("HDL_NO"),
    F.col("HT_INCH_NO"),
    F.col("LDL_NO"),
    F.col("MBR_AGE_NO"),
    F.col("PSA_NO"),
    F.col("SYSTOLIC_BP_NO"),
    F.col("TOT_CHLSTRL_NO"),
    F.col("TGL_NO"),
    F.col("WAIST_CRCMFR_NO"),
    F.col("WT_NO"),
    F.col("BODY_FAT_PCT"),
    F.rpad(F.col("FSTNG_IN"), 1, " ").alias("FSTNG_IN"),
    F.col("HA1C_NO"),
    F.rpad(F.col("PRGNCY_IN"), 1, " ").alias("PRGNCY_IN"),
    F.rpad(F.col("RFRL_TO_DM_IN"), 1, " ").alias("RFRL_TO_DM_IN"),
    F.rpad(F.col("RFRL_TO_PHYS_IN"), 1, " ").alias("RFRL_TO_PHYS_IN"),
    F.rpad(F.col("RESCRN_IN"), 1, " ").alias("RESCRN_IN"),
    F.col("TSH_NO"),
    F.col("BONE_DENSITY_NO"),
    F.rpad(F.col("HLTH_SCRN_SRC_SUBTYP"), 10, " ").alias("HLTH_SCRN_SRC_SUBTYP"),
    F.rpad(F.col("NCTN_TST_RSLT_CD"), 10, " ").alias("NCTN_TST_RSLT_CD"),
    F.rpad(F.col("HLTH_SCRN_VNDR_CD"), 10, " ").alias("HLTH_SCRN_VNDR_CD")
)

# Read/Write hashed file "hf_hlth_screen" as scenario B => dummy table
jdbc_url, jdbc_props = get_db_config("ids_secret_name")
df_hf_hlth_screen = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", "SELECT SRC_SYS_CD, MBR_UNIQ_KEY, SCRN_DT_SK, CRT_RUN_CYC_EXCTN_SK, HLTH_SCRN_SK FROM dummy_hf_hlth_screen")
    .load()
)

df_primaryKey_joined = (
    df_transform.alias("Transform")
    .join(
        df_hf_hlth_screen.alias("lkup"),
        on=[
            F.col("Transform.SRC_SYS_CD") == F.col("lkup.SRC_SYS_CD"),
            F.col("Transform.MBR_UNIQ_KEY") == F.col("lkup.MBR_UNIQ_KEY"),
            F.col("Transform.SCRN_DT_SK") == F.col("lkup.SCRN_DT_SK")
        ],
        how="left"
    )
)

df_enriched = (
    df_primaryKey_joined
    .withColumn(
        "HLTH_SCRN_SK_temp",
        F.when(F.col("lkup.HLTH_SCRN_SK").isNull(), F.lit(None).cast("integer")).otherwise(F.col("lkup.HLTH_SCRN_SK"))
    )
    .withColumn(
        "NewCrtRunCycExtcnSk",
        F.when(F.col("lkup.HLTH_SCRN_SK").isNull(), F.lit(CurrRunCycle).cast("int")).otherwise(F.col("lkup.CRT_RUN_CYC_EXCTN_SK"))
    )
    .withColumn("CurrRunCycle", F.lit(CurrRunCycle).cast("int"))
)

df_enriched = df_enriched.drop("lkup.*")

# SurrogateKeyGen to replace KeyMgtGetNextValueConcurrent usage
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"HLTH_SCRN_SK_temp",<schema>,<secret_name>)

df_enriched = df_enriched.withColumn(
    "HLTH_SCRN_SK",
    F.col("HLTH_SCRN_SK_temp")
).drop("HLTH_SCRN_SK_temp")

df_enriched = df_enriched.withColumn("CRT_RUN_CYC_EXCTN_SK", 
    F.when(F.col("lkup.HLTH_SCRN_SK").isNull(), F.col("CurrRunCycle")).otherwise(F.col("lkup.CRT_RUN_CYC_EXCTN_SK"))
)

# The "lkup" columns were already dropped, so re-join or handle them carefully. Instead, reuse the columns from the joined context:
df_enriched = df_enriched.drop("CRT_RUN_CYC_EXCTN_SK").withColumn(
    "CRT_RUN_CYC_EXCTN_SK",
    F.col("NewCrtRunCycExtcnSk")
).drop("NewCrtRunCycExtcnSk").drop("CurrRunCycle")

df_updt = df_enriched.filter(F.col("lkup.HLTH_SCRN_SK").isNull())
# Note: "lkup.HLTH_SCRN_SK" was dropped, but we can filter by checking the expression used earlier:
df_updt = df_updt.filter(F.col("HLTH_SCRN_SK") != F.col("lkup.HLTH_SCRN_SK"))  # Rows where lkup was null

df_key = df_enriched

df_updt_for_merge = df_updt.select(
    F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Transform.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("Transform.SCRN_DT_SK").alias("SCRN_DT_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("HLTH_SCRN_SK")
)

(
    df_updt_for_merge
    .write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .mode("overwrite")
    .option("dbtable", "STAGING.WellLifeBioHlthScrnExtr_hf_hlth_screen_updt_temp")
    .save()
)

merge_sql = """
MERGE INTO dummy_hf_hlth_screen AS T
USING STAGING.WellLifeBioHlthScrnExtr_hf_hlth_screen_updt_temp AS S
    ON T.SRC_SYS_CD = S.SRC_SYS_CD
    AND T.MBR_UNIQ_KEY = S.MBR_UNIQ_KEY
    AND T.SCRN_DT_SK = S.SCRN_DT_SK
WHEN NOT MATCHED THEN
    INSERT (SRC_SYS_CD, MBR_UNIQ_KEY, SCRN_DT_SK, CRT_RUN_CYC_EXCTN_SK, HLTH_SCRN_SK)
    VALUES (S.SRC_SYS_CD, S.MBR_UNIQ_KEY, S.SCRN_DT_SK, S.CRT_RUN_CYC_EXCTN_SK, S.HLTH_SCRN_SK);
"""

execute_dml(f"DROP TABLE IF EXISTS STAGING.WellLifeBioHlthScrnExtr_hf_hlth_screen_updt_temp", jdbc_url, jdbc_props)
execute_dml(merge_sql, jdbc_url, jdbc_props)

df_key_out = df_key.select(
    F.col("Transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("Transform.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("Transform.DISCARD_IN").alias("DISCARD_IN"),
    F.col("Transform.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("Transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("Transform.ERR_CT").alias("ERR_CT"),
    F.col("Transform.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("HLTH_SCRN_SK"),
    F.col("Transform.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("Transform.SCRN_DT_SK").alias("SCRN_DT_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCycle).cast("int").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Transform.GRP_SK").alias("GRP_SK"),
    F.col("Transform.MBR_SK").alias("MBR_SK"),
    F.col("Transform.HLTH_SCRN_MBR_GNDR_CD").alias("HLTH_SCRN_MBR_GNDR_CD"),
    F.col("Transform.CUR_SMOKER_RISK_IN").alias("CUR_SMOKER_RISK_IN"),
    F.col("Transform.DBTC_RISK_IN").alias("DBTC_RISK_IN"),
    F.col("Transform.FRMR_SMOKER_RISK_IN").alias("FRMR_SMOKER_RISK_IN"),
    F.col("Transform.HEART_DSS_RISK_IN").alias("HEART_DSS_RISK_IN"),
    F.col("Transform.HI_CHLSTRL_RISK_IN").alias("HI_CHLSTRL_RISK_IN"),
    F.col("Transform.HI_BP_RISK_IN").alias("HI_BP_RISK_IN"),
    F.col("Transform.EXRCS_LACK_RISK_IN").alias("EXRCS_LACK_RISK_IN"),
    F.col("Transform.OVERWT_RISK_IN").alias("OVERWT_RISK_IN"),
    F.col("Transform.STRESS_RISK_IN").alias("STRESS_RISK_IN"),
    F.col("Transform.STROKE_RISK_IN").alias("STROKE_RISK_IN"),
    F.col("Transform.BMI_NO").alias("BMI_NO"),
    F.col("Transform.CHLSTRL_RATIO_NO").alias("CHLSTRL_RATIO_NO"),
    F.col("Transform.DIASTOLIC_BP_NO").alias("DIASTOLIC_BP_NO"),
    F.col("Transform.GLUCOSE_NO").alias("GLUCOSE_NO"),
    F.col("Transform.HDL_NO").alias("HDL_NO"),
    F.col("Transform.HT_INCH_NO").alias("HT_INCH_NO"),
    F.col("Transform.LDL_NO").alias("LDL_NO"),
    F.col("Transform.MBR_AGE_NO").alias("MBR_AGE_NO"),
    F.col("Transform.PSA_NO").alias("PSA_NO"),
    F.col("Transform.SYSTOLIC_BP_NO").alias("SYSTOLIC_BP_NO"),
    F.col("Transform.TOT_CHLSTRL_NO").alias("TOT_CHLSTRL_NO"),
    F.col("Transform.TGL_NO").alias("TGL_NO"),
    F.col("Transform.WAIST_CRCMFR_NO").alias("WAIST_CRCMFR_NO"),
    F.col("Transform.WT_NO").alias("WT_NO"),
    F.col("Transform.BODY_FAT_PCT").alias("BODY_FAT_PCT"),
    F.col("Transform.FSTNG_IN").alias("FSTNG_IN"),
    F.col("Transform.HA1C_NO").alias("HA1C_NO"),
    F.col("Transform.PRGNCY_IN").alias("PRGNCY_IN"),
    F.col("Transform.RFRL_TO_DM_IN").alias("RFRL_TO_DM_IN"),
    F.col("Transform.RFRL_TO_PHYS_IN").alias("RFRL_TO_PHYS_IN"),
    F.col("Transform.RESCRN_IN").alias("RESCRN_IN"),
    F.col("Transform.TSH_NO").alias("TSH_NO"),
    F.col("Transform.BONE_DENSITY_NO").alias("BONE_DENSITY_NO"),
    F.col("Transform.HLTH_SCRN_SRC_SUBTYP").alias("HLTH_SCRN_SRC_SUBTYP"),
    F.col("Transform.NCTN_TST_RSLT_CD").alias("NCTN_TST_RSLT_CD"),
    F.col("Transform.HLTH_SCRN_VNDR_CD").alias("HLTH_SCRN_VNDR_CD")
)

df_key_final = df_key_out.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("HLTH_SCRN_SK"),
    F.col("MBR_UNIQ_KEY"),
    F.rpad(F.col("SCRN_DT_SK"), 10, " ").alias("SCRN_DT_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("GRP_SK"),
    F.col("MBR_SK"),
    F.rpad(F.col("HLTH_SCRN_MBR_GNDR_CD"), 20, " ").alias("HLTH_SCRN_MBR_GNDR_CD"),
    F.rpad(F.col("CUR_SMOKER_RISK_IN"), 1, " ").alias("CUR_SMOKER_RISK_IN"),
    F.rpad(F.col("DBTC_RISK_IN"), 1, " ").alias("DBTC_RISK_IN"),
    F.rpad(F.col("FRMR_SMOKER_RISK_IN"), 1, " ").alias("FRMR_SMOKER_RISK_IN"),
    F.rpad(F.col("HEART_DSS_RISK_IN"), 1, " ").alias("HEART_DSS_RISK_IN"),
    F.rpad(F.col("HI_CHLSTRL_RISK_IN"), 1, " ").alias("HI_CHLSTRL_RISK_IN"),
    F.rpad(F.col("HI_BP_RISK_IN"), 1, " ").alias("HI_BP_RISK_IN"),
    F.rpad(F.col("EXRCS_LACK_RISK_IN"), 1, " ").alias("EXRCS_LACK_RISK_IN"),
    F.rpad(F.col("OVERWT_RISK_IN"), 1, " ").alias("OVERWT_RISK_IN"),
    F.rpad(F.col("STRESS_RISK_IN"), 1, " ").alias("STRESS_RISK_IN"),
    F.rpad(F.col("STROKE_RISK_IN"), 1, " ").alias("STROKE_RISK_IN"),
    F.col("BMI_NO"),
    F.col("CHLSTRL_RATIO_NO"),
    F.col("DIASTOLIC_BP_NO"),
    F.col("GLUCOSE_NO"),
    F.col("HDL_NO"),
    F.col("HT_INCH_NO"),
    F.col("LDL_NO"),
    F.col("MBR_AGE_NO"),
    F.col("PSA_NO"),
    F.col("SYSTOLIC_BP_NO"),
    F.col("TOT_CHLSTRL_NO"),
    F.col("TGL_NO"),
    F.col("WAIST_CRCMFR_NO"),
    F.col("WT_NO"),
    F.col("BODY_FAT_PCT"),
    F.rpad(F.col("FSTNG_IN"), 1, " ").alias("FSTNG_IN"),
    F.col("HA1C_NO"),
    F.rpad(F.col("PRGNCY_IN"), 1, " ").alias("PRGNCY_IN"),
    F.rpad(F.col("RFRL_TO_DM_IN"), 1, " ").alias("RFRL_TO_DM_IN"),
    F.rpad(F.col("RFRL_TO_PHYS_IN"), 1, " ").alias("RFRL_TO_PHYS_IN"),
    F.rpad(F.col("RESCRN_IN"), 1, " ").alias("RESCRN_IN"),
    F.col("TSH_NO"),
    F.col("BONE_DENSITY_NO"),
    F.rpad(F.col("HLTH_SCRN_SRC_SUBTYP"), 10, " ").alias("HLTH_SCRN_SRC_SUBTYP"),
    F.rpad(F.col("NCTN_TST_RSLT_CD"), 10, " ").alias("NCTN_TST_RSLT_CD"),
    F.rpad(F.col("HLTH_SCRN_VNDR_CD"), 10, " ").alias("HLTH_SCRN_VNDR_CD")
)

write_files(
    df_key_final,
    f"{adls_path}/key/WellLifeHlthScreenExtr.HlthScreen.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)