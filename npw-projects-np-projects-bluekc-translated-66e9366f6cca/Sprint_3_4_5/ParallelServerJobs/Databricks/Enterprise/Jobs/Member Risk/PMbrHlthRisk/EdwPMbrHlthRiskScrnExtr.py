# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2010   Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by:
# MAGIC                    
# MAGIC 
# MAGIC Processing:
# MAGIC                     This is a new DataStage job that loads the Health Screen data into the Claim DataMart table MBRSH_DM_MBR_HLTH_RISK.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                                Project/                                                                                                                                           Code                  Date
# MAGIC Developer         Date              Altiris #           Change Description                                                                                                  Reviewer            Reviewed
# MAGIC ----------------------   -------------------   -------------------   ------------------------------------------------------------------------------------------------------------------------------   ----------------------     -------------------   
# MAGIC Sharon Andrew 2010-01-15   4250              Initial Programming                                                                                                   Steph Goddard   01/20/2010
# MAGIC Kalyan Neelam 2014-10-15     5063                Added new logic for source HLTHFTNS                                                              Bhoomi Dasari     10/17/2014

# MAGIC hlth_screen
# MAGIC Loads the Health Scrn data into the  EDW P_MBR_HLTH_RISK
# MAGIC SELECT
# MAGIC              RISK.MBR_UNIQ_KEY,
# MAGIC              RISK.MBR_SRVY_RSPN_DT_SK,
# MAGIC              RISK.BP_CT,
# MAGIC              RISK.BMI_CT,
# MAGIC              RISK.PHYSCL_ACTVTY_CT,
# MAGIC              RISK.SMOKE_CT,
# MAGIC              RISK.STRESS_CT,
# MAGIC              ANLYS.BRST_XRAY_TX,
# MAGIC              ANLYS.COLON_SCRN_TX,
# MAGIC              ANLYS.EAT_FIBER_SERVING_TX,
# MAGIC              ANLYS.FATTY_FOOD_SERVING_TX,
# MAGIC              ANLYS.HYSTRCTMY_TX,
# MAGIC              ANLYS.LAST_PAP_SMEAR_TX 
# MAGIC FROM 
# MAGIC #$EDWOwner#.MBR_SRVY_RSPN_RISK_FCTR_F RISK,
# MAGIC #$EDWOwner#.MBR_SRVY_RSPN_ANLYS_F ANLYS
# MAGIC 
# MAGIC WHERE
# MAGIC RISK.SRC_SYS_CD = ANLYS.SRC_SYS_CD AND
# MAGIC RISK.MBR_UNIQ_KEY = ANLYS.MBR_UNIQ_KEY AND
# MAGIC RISK.MBR_SRVY_RSPN_DT_SK = ANLYS.MBR_SRVY_RSPN_DT_SK AND
# MAGIC RISK.MOST_RECENT_SRVY_IN = 'Y' AND
# MAGIC RISK.MBR_SRVY_RSPN_DT_SK BETWEEN '#CurrDateMinus3Mons#' AND '#CurrDate#'
# MAGIC SELECT 
# MAGIC               SCRN.MBR_UNIQ_KEY,
# MAGIC               SCRN.HLTH_SCRN_DT_SK,
# MAGIC               SCRN.HLTH_SCRN_HI_BP_RISK_IN,
# MAGIC               SCRN.HLTH_SCRN_EXRCS_LACK_RISK_IN,
# MAGIC               SCRN.HLTH_SCRN_OVERWT_RISK_IN,
# MAGIC               SCRN.HLTH_SCRN_STRESS_RISK_IN,
# MAGIC               SCRN.HLTH_SCRN_STROKE_RISK_IN,
# MAGIC               SCRN.HLTH_SCRN_CUR_SMOKER_RISK_IN 
# MAGIC FROM 
# MAGIC #$EDWOwner#.HLTH_SCRN_F SCRN
# MAGIC WHERE SCRN.HLTH_SCRN_DT_SK = (SELECT MAX(SCRN1.HLTH_SCRN_DT_SK) 
# MAGIC                                                                   FROM #$EDWOwner#.HLTH_SCRN_F SCRN1
# MAGIC                                                                   WHERE SCRN.MBR_UNIQ_KEY=SCRN1.MBR_UNIQ_KEY)
# MAGIC AND SCRN.LAST_UPDT_RUN_CYC_EXCTN_DT_SK > '#LastRunDate#'
# MAGIC risk_fctr
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
from pyspark.sql.functions import col, lit, when, length, substring, rpad
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


# Retrieve parameters
LastRunDate = get_widget_value('LastRunDate','2009-09-04')
CurrRunCycle = get_widget_value('CurrRunCycle','103')
CurrDateMinus3Mons = get_widget_value('CurrDateMinus3Mons','2009-06-11')
CurrDate = get_widget_value('CurrDate','2009-09-11')
EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')

# Read from EDW - risk_fctr
jdbc_url, jdbc_props = get_db_config(edw_secret_name)
extract_query_risk_fctr = (
    "SELECT "
    "RISK.SRC_SYS_CD, "
    "RISK.MBR_UNIQ_KEY, "
    "RISK.MBR_SRVY_RSPN_DT_SK, "
    "RISK.BP_CT, "
    "RISK.BMI_CT, "
    "RISK.PHYSCL_ACTVTY_CT, "
    "RISK.SMOKE_CT, "
    "RISK.STRESS_CT, "
    "ANLYS.BRST_XRAY_TX, "
    "ANLYS.COLON_SCRN_TX, "
    "ANLYS.EAT_FIBER_SERVING_TX, "
    "ANLYS.FATTY_FOOD_SERVING_TX, "
    "ANLYS.HYSTRCTMY_TX, "
    "ANLYS.LAST_PAP_SMEAR_TX, "
    "RISK.FRUIT_VGTBL_CT, "
    "RISK.SMKLSS_CT "
    "FROM " + EDWOwner + ".MBR_SRVY_RSPN_RISK_FCTR_F RISK, "
           + EDWOwner + ".MBR_SRVY_RSPN_ANLYS_F ANLYS "
    "WHERE RISK.SRC_SYS_CD = ANLYS.SRC_SYS_CD "
    "AND RISK.MBR_UNIQ_KEY = ANLYS.MBR_UNIQ_KEY "
    "AND RISK.MBR_SRVY_RSPN_DT_SK = ANLYS.MBR_SRVY_RSPN_DT_SK "
    "AND RISK.MOST_RECENT_SRVY_IN = 'Y' "
    "AND RISK.MBR_SRVY_RSPN_DT_SK BETWEEN '" + CurrDateMinus3Mons + "' AND '" + CurrDate + "'"
)
df_risk_fctr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_risk_fctr)
    .load()
)

# Read from EDW - hlth_scrn
extract_query_hlth_scrn = (
    "SELECT "
    "SCRN.SRC_SYS_CD, "
    "SCRN.MBR_UNIQ_KEY, "
    "SCRN.HLTH_SCRN_DT_SK, "
    "SCRN.HLTH_SCRN_HI_BP_RISK_IN, "
    "SCRN.HLTH_SCRN_EXRCS_LACK_RISK_IN, "
    "SCRN.HLTH_SCRN_OVERWT_RISK_IN, "
    "SCRN.HLTH_SCRN_STRESS_RISK_IN, "
    "SCRN.HLTH_SCRN_STROKE_RISK_IN, "
    "SCRN.HLTH_SCRN_CUR_SMOKER_RISK_IN "
    "FROM " + EDWOwner + ".HLTH_SCRN_F SCRN "
    "WHERE SCRN.HLTH_SCRN_DT_SK = ("
    "SELECT MAX(SCRN1.HLTH_SCRN_DT_SK) "
    "FROM " + EDWOwner + ".HLTH_SCRN_F SCRN1 "
    "WHERE SCRN.MBR_UNIQ_KEY = SCRN1.MBR_UNIQ_KEY"
    ") "
    "AND SCRN.LAST_UPDT_RUN_CYC_EXCTN_DT_SK > '" + LastRunDate + "' "
    "AND SCRN.MBR_UNIQ_KEY <> 0 "
    "AND SCRN.MBR_UNIQ_KEY <> 1"
)
df_hlth_scrn = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_hlth_scrn)
    .load()
)

# Scenario A: Intermediate hashed file -> deduplicate on key columns
df_risk_fctr_dedup = df_risk_fctr.dropDuplicates(["SRC_SYS_CD","MBR_UNIQ_KEY","MBR_SRVY_RSPN_DT_SK"])

# Transformer - BusinessLogic (left join)
df_temp = df_hlth_scrn.alias("hlth_scrn").join(
    df_risk_fctr_dedup.alias("risk_lkup"),
    (col("hlth_scrn.SRC_SYS_CD") == col("risk_lkup.SRC_SYS_CD"))
    & (col("hlth_scrn.MBR_UNIQ_KEY") == col("risk_lkup.MBR_UNIQ_KEY")),
    how="left"
)

df_temp2 = (
    df_temp
    .withColumn("svMbrSrvyRspnDtSk",
        when(col("risk_lkup.MBR_SRVY_RSPN_DT_SK").isNull(), lit("1753-01-01"))
        .otherwise(col("risk_lkup.MBR_SRVY_RSPN_DT_SK"))
    )
    .withColumn("svBpCt",
        when(col("risk_lkup.BP_CT").isNull(), lit(0))
        .otherwise(col("risk_lkup.BP_CT"))
    )
    .withColumn("svBmiCt",
        when(col("risk_lkup.BMI_CT").isNull(), lit(0))
        .otherwise(col("risk_lkup.BMI_CT"))
    )
    .withColumn("svPhysclActvtyCt",
        when(col("risk_lkup.PHYSCL_ACTVTY_CT").isNull(), lit(0))
        .otherwise(col("risk_lkup.PHYSCL_ACTVTY_CT"))
    )
    .withColumn("svSmokeCt",
        when(col("risk_lkup.SMOKE_CT").isNull(), lit(0))
        .otherwise(col("risk_lkup.SMOKE_CT"))
    )
    .withColumn("svStressCt",
        when(col("risk_lkup.STRESS_CT").isNull(), lit(0))
        .otherwise(col("risk_lkup.STRESS_CT"))
    )
    .withColumn("svColonScrnTx",
        when(col("risk_lkup.COLON_SCRN_TX").isNull(), lit(None))
        .otherwise(col("risk_lkup.COLON_SCRN_TX"))
    )
    .withColumn("svEatFiberServingTx",
        when(col("risk_lkup.EAT_FIBER_SERVING_TX").isNull(), lit(None))
        .otherwise(col("risk_lkup.EAT_FIBER_SERVING_TX"))
    )
    .withColumn("svFattyFoodServingTx",
        when(col("risk_lkup.FATTY_FOOD_SERVING_TX").isNull(), lit(None))
        .otherwise(col("risk_lkup.FATTY_FOOD_SERVING_TX"))
    )
    .withColumn("svHystrctmyTx",
        when(col("risk_lkup.HYSTRCTMY_TX").isNull(), lit(None))
        .otherwise(col("risk_lkup.HYSTRCTMY_TX"))
    )
    .withColumn("svLastPapSmearTx",
        when(col("risk_lkup.LAST_PAP_SMEAR_TX").isNull(), lit(None))
        .otherwise(col("risk_lkup.LAST_PAP_SMEAR_TX"))
    )
    .withColumn("svFruitVgtblCt",
        when(col("risk_lkup.FRUIT_VGTBL_CT").isNull(), lit(0))
        .otherwise(col("risk_lkup.FRUIT_VGTBL_CT"))
    )
    .withColumn("svSmklssCt",
        when(col("risk_lkup.SMKLSS_CT").isNull(), lit(0))
        .otherwise(col("risk_lkup.SMKLSS_CT"))
    )
)

df_enriched = df_temp2.select(
    # 1) SRC_SYS_CD
    lit("FACETS").alias("SRC_SYS_CD"),

    # 2) MBR_UNIQ_KEY
    col("hlth_scrn.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),

    # 3) MBR_SRVY_RSPN_DT
    when(trim(col("hlth_scrn.HLTH_SCRN_DT_SK")) == "UNK", lit("1800-01-01"))
    .when(trim(col("hlth_scrn.HLTH_SCRN_DT_SK")) == "NA", lit("1800-01-01"))
    .when(length(trim(col("hlth_scrn.HLTH_SCRN_DT_SK"))) == 0, lit("1800-01-01"))
    .otherwise(col("hlth_scrn.HLTH_SCRN_DT_SK").substr(1,10))
    .alias("MBR_SRVY_RSPN_DT"),

    # 4) BRST_XRAY_IN
    when(col("risk_lkup.BRST_XRAY_TX").isNull(), lit("N"))
    .when(col("risk_lkup.BRST_XRAY_TX") != "Less_than_1_year", lit("Y"))
    .otherwise(lit("N"))
    .alias("BRST_XRAY_IN"),

    # 5) CERV_SCRN_IN
    when(
        (col("svLastPapSmearTx") != "1_2_years_ago")
        & (col("svLastPapSmearTx") != "Less_than_1_year")
        & ((col("svHystrctmyTx") == "No") | (col("svHystrctmyTx") == "no")),
        lit("Y")
    ).otherwise(lit("N")).alias("CERV_SCRN_IN"),

    # 6) COLON_SCRN_IN
    when(
        (col("svColonScrnTx") == "Never") | (col("svColonScrnTx") == "Dont_know"),
        lit("Y")
    ).otherwise(lit("N")).alias("COLON_SCRN_IN"),

    # 7) EXRCS_IN
    when(
        col("hlth_scrn.SRC_SYS_CD") == "HLTHFTNS",
        when(
            (col("svPhysclActvtyCt") == 1) | (col("hlth_scrn.HLTH_SCRN_EXRCS_LACK_RISK_IN") == "Y"),
            lit("Y")
        ).otherwise(lit("N"))
    ).otherwise(
        when(
            (col("svBmiCt") == 1) | (col("hlth_scrn.HLTH_SCRN_EXRCS_LACK_RISK_IN") == "Y"),
            lit("Y")
        ).otherwise(lit("N"))
    ).alias("EXRCS_IN"),

    # 8) HYPRTN_IN
    when(
        (col("svBpCt") == 1) | (col("hlth_scrn.HLTH_SCRN_HI_BP_RISK_IN") == "Y"),
        lit("Y")
    ).otherwise(lit("N")).alias("HYPRTN_IN"),

    # 9) NTRTN_IN
    when(
        col("hlth_scrn.SRC_SYS_CD") == "HLTHFTNS",
        when(col("svFruitVgtblCt") == 1, lit("Y")).otherwise(lit("N"))
    ).otherwise(
        when(
            (col("svEatFiberServingTx") == "Rarely_never")
            | (col("svEatFiberServingTx") == "1_2_servings_a_day")
            | (col("svFattyFoodServingTx") == "5_6_servings_a_day")
            | (col("svFattyFoodServingTx") == "3_4_servings_a_day"),
            lit("Y")
        ).otherwise(lit("N"))
    ).alias("NTRTN_IN"),

    # 10) STRS_IN
    when(
        (col("svStressCt") == 1) | (col("hlth_scrn.HLTH_SCRN_STRESS_RISK_IN") == "Y"),
        lit("Y")
    ).otherwise(lit("N")).alias("STRS_IN"),

    # 11) TOBAC_IN
    when(
        col("hlth_scrn.SRC_SYS_CD") == "HLTHFTNS",
        when(
            (col("svSmokeCt") == 1)
            | (col("svSmklssCt") == 1)
            | (col("hlth_scrn.HLTH_SCRN_CUR_SMOKER_RISK_IN") == "Y"),
            lit("Y")
        ).otherwise(lit("N"))
    ).otherwise(
        when(
            (col("svSmokeCt") == 1) | (col("hlth_scrn.HLTH_SCRN_CUR_SMOKER_RISK_IN") == "Y"),
            lit("Y")
        ).otherwise(lit("N"))
    ).alias("TOBAC_IN"),

    # 12) WT_IN
    when(
        (col("hlth_scrn.HLTH_SCRN_OVERWT_RISK_IN") == "Y")
        | (col("svBmiCt") == 1)
        | (col("hlth_scrn.HLTH_SCRN_EXRCS_LACK_RISK_IN") == "Y")
        | (col("svPhysclActvtyCt") == 1),
        lit("Y")
    ).otherwise(lit("N")).alias("WT_IN"),

    # 13) LAST_UPDT_RUN_CYC_NO
    lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_NO"),

    # 14) LAST_UPDT_DT
    lit(CurrDate).alias("LAST_UPDT_DT")
)

# Apply rpad for char/varchar columns that have lengths
df_final = (
    df_enriched
    .withColumn("MBR_SRVY_RSPN_DT", rpad(col("MBR_SRVY_RSPN_DT"), 10, " "))
    .withColumn("BRST_XRAY_IN", rpad(col("BRST_XRAY_IN"), 1, " "))
    .withColumn("CERV_SCRN_IN", rpad(col("CERV_SCRN_IN"), 1, " "))
    .withColumn("COLON_SCRN_IN", rpad(col("COLON_SCRN_IN"), 1, " "))
    .withColumn("EXRCS_IN", rpad(col("EXRCS_IN"), 1, " "))
    .withColumn("HYPRTN_IN", rpad(col("HYPRTN_IN"), 1, " "))
    .withColumn("NTRTN_IN", rpad(col("NTRTN_IN"), 1, " "))
    .withColumn("STRS_IN", rpad(col("STRS_IN"), 1, " "))
    .withColumn("TOBAC_IN", rpad(col("TOBAC_IN"), 1, " "))
    .withColumn("WT_IN", rpad(col("WT_IN"), 1, " "))
    .withColumn("LAST_UPDT_DT", rpad(col("LAST_UPDT_DT"), 10, " "))
)

# Write final file (CSeqFileStage)
write_files(
    df_final,
    f"{adls_path}load/P_MBR_HLTH_RISK.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)