# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_2 04/17/06 09:28:31 Batch  13987_34153 PROMOTE bckcetl edw10 dsadm J. Mahaffey for Celeste Schmidt
# MAGIC ^1_2 04/17/06 09:26:04 Batch  13987_33978 INIT bckcett migrate dsadm J. Mahaffey for Celeste Schmidt
# MAGIC ^1_5 04/03/06 10:38:10 Batch  13973_38297 PROMOTE bckcett testEDW10 u06731 cls
# MAGIC ^1_5 04/03/06 10:28:58 Batch  13973_37743 INIT bckcett testEDW10 u06731 cls
# MAGIC ^1_4 03/30/06 12:49:17 Batch  13969_46189 PROMOTE bckcett testEDW10 u10157 sa
# MAGIC ^1_4 03/30/06 12:47:12 Batch  13969_46039 INIT bckcett devlEDW10 u10157 sa
# MAGIC ^1_3 03/29/06 11:47:23 Batch  13968_42450 INIT bckcett devlEDW10 u06731 CS
# MAGIC ^1_2 03/17/06 16:35:13 Batch  13956_59714 INIT bckcett devlEDW10 u10157 sa
# MAGIC ^1_1 03/17/06 16:19:32 Batch  13956_58774 INIT bckcett devlEDW10 u10157 sa
# MAGIC ^1_1 03/17/06 14:43:42 Batch  13956_53025 INIT bckcett devlEDW10 u10157 sa
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     EDWMedDMotorRtdListExtr 
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC       
# MAGIC 
# MAGIC INPUTS:    EDW Member Eligibility, Member Enroll,
# MAGIC 	
# MAGIC 
# MAGIC   
# MAGIC HASH FILES:
# MAGIC 
# MAGIC             hf_med_d_retiree_ford;
# MAGIC             hf_med_d_retiree_gm
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                   trans_ford_output, trans_ford_subs_trm_date, trans_gm_output, trans_gm_subs_trm_date - take the EDW data and place the fields into the order that EDW is expecting
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                   EDW - derive Subsidy data from EDW and place it in the format to go back into EDW with Subsidy information
# MAGIC 
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                   motors_output - the file that will be taken into the EDW external process
# MAGIC 
# MAGIC MODIFICATIONS:


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, when, substring, lit, rpad, expr, date_format, year, concat
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


# Parameter retrieval
IDSOwner = get_widget_value('IDSOwner','')
EDWOwner = get_widget_value('EDWOwner','')
IDSFilePath = get_widget_value('IDSFilePath','')
ids_secret_name = get_widget_value('ids_secret_name','')
edw_secret_name = get_widget_value('edw_secret_name','')

# EDW_FORD (DB2Connector)
jdbc_url, jdbc_props = get_db_config(edw_secret_name)
extract_query_edw_ford = """
SELECT distinct 
MBRD.MBR_ID,
MBRD.MBR_SFX_NO,
MBRD.MBR_SSN,
MBRD.MBR_MCARE_NO,
MBRD.MBR_FIRST_NM,
MBRD.MBR_MIDINIT,
MBRD.MBR_LAST_NM,
MBRD.MBR_BRTH_DT_SK,
MBRD.MBR_GNDR_CD,
MBRD.MBR_RELSHP_CD,
' ' AS SUBSIDY_EFF_DT_1,
' ' AS SUBSIDY_TERM_DT_1,
' ' AS SUBSIDY_EFF_DT_2,
' ' AS SUBSIDY_TERM_DT_2,
' ' AS SUBSIDY_EFF_DT_3,
' ' AS SUBSIDY_TERM_DT_3,
' ' AS SUBSIDY_EFF_DT_4,
' ' AS SUBSIDY_TERM_DT_4,
' ' AS SUBSIDY_EFF_DT_5,
' ' AS SUBSIDY_TERM_DT_5,
' ' AS SUBSIDY_EFF_DT_6,
' ' AS SUBSIDY_TERM_DT_6,
CAST(dbo.GetDateCST() AS DATE) AS DATE_SUBMITTED,
' ' AS PLAN_YR_START_DT,
' ' AS PLAN_YR_END_DT,
MBRD.GRP_SK,
MBRD.MBR_SK,
MCOB.MBR_COB_EFF_DT_SK,
MCOB.MBR_COB_TERM_DT_SK,
CSPI.CLS_PLN_DTL_EFF_DT_SK,
CSPI.CLS_PLN_DTL_TERM_DT_SK,
MCOB.MBR_COB_PAYMT_PRTY_CD
FROM {EDWOwner}.MBR_D MBRD,
     {EDWOwner}.MBR_ENR_D MEND,
     {EDWOwner}.MBR_COB_D MCOB,
     {EDWOwner}.CLS_PLN_DTL_I CSPI
WHERE 
 MBRD.GRP_ID = '10037000'
 AND (MBRD.CLS_ID LIKE 'R0%' OR MBRD.CLS_ID LIKE 'S0%')
 AND MBRD.MBR_SK = MEND.MBR_SK
 AND MBRD.MBR_SK = MCOB.MBR_SK
 AND MBRD.GRP_SK = CSPI.GRP_SK
 AND MBRD.CLS_SK = CSPI.CLS_SK
 AND MEND.PROD_ID = CSPI.PROD_ID
 AND MEND.MBR_ENR_CLS_PLN_PROD_CAT_CD = 'MED'
 AND CSPI.CLS_PLN_DTL_TERM_DT_SK = '9999-12-31'
 AND MEND.MBR_ENR_EFF_DT_SK <= CAST(dbo.GetDateCST() AS DATE)
 AND MEND.MBR_ENR_TERM_DT_SK >= CAST(dbo.GetDateCST() AS DATE)
 AND MEND.MBR_ENR_TERM_DT_SK = '2199-12-31'
 AND MEND.MBR_ENR_ELIG_IN = 'Y'
 AND MEND.MBR_ENR_TERM_DT_SK >= MCOB.MBR_COB_EFF_DT_SK
 AND MEND.MBR_ENR_EFF_DT_SK <= MCOB.MBR_COB_TERM_DT_SK
 AND MCOB.ACTV_COB_IN = 'Y'
 AND MCOB.MBR_COB_TYP_CD = 'MCARE'
 AND MCOB.MBR_COB_PAYMT_PRTY_CD = 'PRI'
order by
 MBRD.MBR_ID,
 MBRD.MBR_SFX_NO,
 MBRD.MBR_SSN

{IDSOwner}.CLS CLS, {IDSOwner}.GRP GRP
""".format(EDWOwner=EDWOwner, IDSOwner=IDSOwner)

df_EDW_FORD = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_edw_ford)
    .load()
)

# trans_ford_output (CTransformerStage)
df_trans_ford_output = df_EDW_FORD.select(
    trim(substring(col("MBR_ID"), 1, 9)).alias("MBR_ID"),
    trim(substring(col("MBR_SFX_NO"), 1, 2)).alias("MBR_SFX_NO"),
    trim(col("MBR_SSN")).alias("MBR_SSN"),
    trim(col("MBR_MCARE_NO")).alias("MBR_MCARE_NO"),
    trim(col("MBR_FIRST_NM")).alias("MBR_FIRST_NM"),
    trim(col("MBR_MIDINIT")).alias("MBR_MIDINIT"),
    trim(col("MBR_LAST_NM")).alias("MBR_LAST_NM"),
    trim(col("MBR_BRTH_DT_SK")).alias("MBR_BRTH_DT_SK"),
    trim(col("MBR_GNDR_CD")).alias("MBR_GNDR_CD"),
    trim(col("MBR_RELSHP_CD")).alias("MBR_RELSHP_CD"),
    trim(col("SUBSIDY_EFF_DT_1")).alias("SUBSIDY_EFF_DT_1"),
    trim(col("SUBSIDY_TERM_DT_1")).alias("SUBSIDY_TERM_DT_1"),
    lit("          ").alias("SUBSIDY_EFF_DT_2"),
    lit("          ").alias("SUBSIDY_TERM_DT_2"),
    lit("          ").alias("SUBSIDY_EFF_DT_3"),
    lit("          ").alias("SUBSIDY_TERM_DT_3"),
    lit("          ").alias("SUBSIDY_EFF_DT_4"),
    lit("          ").alias("SUBSIDY_TERM_DT_4"),
    lit("          ").alias("SUBSIDY_EFF_DT_5"),
    lit("          ").alias("SUBSIDY_TERM_DT_5"),
    lit("          ").alias("SUBSIDY_EFF_DT_6"),
    lit("          ").alias("SUBSIDY_TERM_DT_6"),
    current_date().alias("DATE_SUBMITTED"),
    concat(year(current_timestamp()), lit('-03-01')).alias("PLAN_YR_START_DT"),
    concat((year(current_timestamp()) + 1).cast("string"), lit('-02-28')).alias("PLAN_YR_END_DT"),
    trim(col("GRP_SK")).alias("GRP_SK"),
    trim(col("MBR_SK")).alias("MBR_SK"),
    trim(col("MBR_COB_EFF_DT_SK")).alias("MBR_COB_EFF_DT_SK"),
    trim(col("MBR_COB_TERM_DT_SK")).alias("MBR_COB_TERM_DT_SK"),
    trim(col("CLS_PLN_DTL_EFF_DT_SK")).alias("CLS_PLN_DTL_EFF_DT_SK"),
    trim(col("CLS_PLN_DTL_TERM_DT_SK")).alias("CLS_PLN_DTL_TERM_DT_SK"),
    trim(col("MBR_COB_PAYMT_PRTY_CD")).alias("MBR_COB_PAYMT_PRTY_CD"),
)

# trans_ford_subs_trm_date (CTransformerStage)
df_trans_ford_subs_trm_date = df_trans_ford_output.select(
    col("MBR_ID"),
    col("MBR_SFX_NO"),
    col("MBR_SSN"),
    col("MBR_MCARE_NO"),
    col("MBR_FIRST_NM"),
    col("MBR_MIDINIT"),
    col("MBR_LAST_NM"),
    col("MBR_BRTH_DT_SK"),
    col("MBR_GNDR_CD"),
    col("MBR_RELSHP_CD"),
    when(
        (col("PLAN_YR_START_DT") >= col("MBR_COB_EFF_DT_SK")) & (col("PLAN_YR_START_DT") >= col("CLS_PLN_DTL_EFF_DT_SK")),
        col("PLAN_YR_START_DT")
    ).when(
        (col("MBR_COB_EFF_DT_SK") >= col("CLS_PLN_DTL_EFF_DT_SK")) & (col("MBR_COB_EFF_DT_SK") >= col("PLAN_YR_START_DT")),
        col("MBR_COB_EFF_DT_SK")
    ).when(
        (col("CLS_PLN_DTL_EFF_DT_SK") >= col("PLAN_YR_START_DT")) & (col("CLS_PLN_DTL_EFF_DT_SK") >= col("MBR_COB_EFF_DT_SK")),
        col("CLS_PLN_DTL_EFF_DT_SK")
    ).otherwise(col("PLAN_YR_START_DT")).alias("SUBSIDY_EFF_DT_1"),
    when(
        (col("PLAN_YR_END_DT") <= col("MBR_COB_TERM_DT_SK")) & (col("PLAN_YR_END_DT") <= col("CLS_PLN_DTL_TERM_DT_SK")),
        col("PLAN_YR_END_DT")
    ).when(
        (col("MBR_COB_TERM_DT_SK") <= col("CLS_PLN_DTL_TERM_DT_SK")) & (col("MBR_COB_TERM_DT_SK") <= col("PLAN_YR_END_DT")),
        col("MBR_COB_TERM_DT_SK")
    ).when(
        (col("CLS_PLN_DTL_TERM_DT_SK") <= col("PLAN_YR_END_DT")) & (col("CLS_PLN_DTL_TERM_DT_SK") <= col("MBR_COB_TERM_DT_SK")),
        col("CLS_PLN_DTL_TERM_DT_SK")
    ).otherwise(col("PLAN_YR_END_DT")).alias("SUBSIDY_TERM_DT_1"),
    col("SUBSIDY_EFF_DT_2"),
    col("SUBSIDY_TERM_DT_2"),
    col("SUBSIDY_EFF_DT_3"),
    col("SUBSIDY_TERM_DT_3"),
    col("SUBSIDY_EFF_DT_4"),
    col("SUBSIDY_TERM_DT_4"),
    col("SUBSIDY_EFF_DT_5"),
    col("SUBSIDY_TERM_DT_5"),
    col("SUBSIDY_EFF_DT_6"),
    col("SUBSIDY_TERM_DT_6"),
    col("DATE_SUBMITTED"),
    col("PLAN_YR_START_DT"),
    col("PLAN_YR_END_DT"),
    col("GRP_SK"),
    col("MBR_SK")
)

# hf_medd_ford (CHashedFileStage) -> Scenario A dedup
df_hf_medd_ford = df_trans_ford_subs_trm_date.dropDuplicates(["MBR_ID", "MBR_SFX_NO"])
df_lc_medd_motors_ford = df_hf_medd_ford

# EDW_GM (DB2Connector)
extract_query_edw_gm = """
SELECT 
MBRD.MBR_ID,
MBRD.MBR_SFX_NO,
MBRD.MBR_SSN,
MBRD.MBR_MCARE_NO,
MBRD.MBR_FIRST_NM,
MBRD.MBR_MIDINIT,
MBRD.MBR_LAST_NM,
MBRD.MBR_BRTH_DT_SK,
MBRD.MBR_GNDR_CD,
MBRD.MBR_RELSHP_CD,
' ' AS SUBSIDY_EFF_DT_1,
' ' AS SUBSIDY_TERM_DT_1,
' ' AS SUBSIDY_EFF_DT_2,
' ' AS SUBSIDY_TERM_DT_2,
' ' AS SUBSIDY_EFF_DT_3,
' ' AS SUBSIDY_TERM_DT_3,
' ' AS SUBSIDY_EFF_DT_4,
' ' AS SUBSIDY_TERM_DT_4,
' ' AS SUBSIDY_EFF_DT_5,
' ' AS SUBSIDY_TERM_DT_5,
' ' AS SUBSIDY_EFF_DT_6,
' ' AS SUBSIDY_TERM_DT_6,
CAST(dbo.GetDateCST() AS DATE) AS DATE_SUBMITTED,
' ' AS PLAN_YR_START_DT,
' ' AS PLAN_YR_END_DT,
MBRD.GRP_SK,
MBRD.MBR_SK,
MCOB.MBR_COB_EFF_DT_SK,
MCOB.MBR_COB_TERM_DT_SK,
CSPI.CLS_PLN_DTL_EFF_DT_SK,
CSPI.CLS_PLN_DTL_TERM_DT_SK,
MCOB.MBR_COB_PAYMT_PRTY_CD
FROM {EDWOwner}.MBR_D MBRD,
     {EDWOwner}.MBR_ENR_D MEND,
     {EDWOwner}.MBR_COB_D MCOB,
     {EDWOwner}.CLS_PLN_DTL_I CSPI
WHERE 
 MBRD.GRP_ID = '10033000'
 AND (MBRD.CLS_ID LIKE 'R0%' OR MBRD.CLS_ID LIKE 'S0%')
 AND MBRD.MBR_SK = MEND.MBR_SK
 AND MBRD.MBR_SK = MCOB.MBR_SK
 AND MBRD.GRP_SK = CSPI.GRP_SK
 AND MBRD.CLS_SK = CSPI.CLS_SK
 AND MEND.PROD_ID = CSPI.PROD_ID
 AND MEND.MBR_ENR_CLS_PLN_PROD_CAT_CD = 'MED'
 AND CSPI.CLS_PLN_DTL_TERM_DT_SK = '9999-12-31'
 AND MEND.MBR_ENR_EFF_DT_SK <= CAST(dbo.GetDateCST() AS DATE)
 AND MEND.MBR_ENR_TERM_DT_SK >= CAST(dbo.GetDateCST() AS DATE)
 AND MEND.MBR_ENR_TERM_DT_SK = '2199-12-31'
 AND MEND.MBR_ENR_ELIG_IN = 'Y'
 AND MEND.MBR_ENR_TERM_DT_SK >= MCOB.MBR_COB_EFF_DT_SK
 AND MEND.MBR_ENR_EFF_DT_SK <= MCOB.MBR_COB_TERM_DT_SK
 AND MCOB.ACTV_COB_IN = 'Y'
 AND MCOB.MBR_COB_TYP_CD = 'MCARE'
 AND MCOB.MBR_COB_PAYMT_PRTY_CD = 'PRI'
order by
 MBRD.MBR_ID,
 MBRD.MBR_SFX_NO,
 MBRD.MBR_SSN

{IDSOwner}.CLS CLS, {IDSOwner}.GRP GRP
""".format(EDWOwner=EDWOwner, IDSOwner=IDSOwner)

df_EDW_GM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_edw_gm)
    .load()
)

# trans_gm_output (CTransformerStage)
df_trans_gm_output = df_EDW_GM.select(
    trim(substring(col("MBR_ID"), 1, 9)).alias("MBR_ID"),
    trim(substring(col("MBR_SFX_NO"), 1, 2)).alias("MBR_SFX_NO"),
    trim(col("MBR_SSN")).alias("MBR_SSN"),
    trim(col("MBR_MCARE_NO")).alias("MBR_MCARE_NO"),
    trim(col("MBR_FIRST_NM")).alias("MBR_FIRST_NM"),
    trim(col("MBR_MIDINIT")).alias("MBR_MIDINIT"),
    trim(col("MBR_LAST_NM")).alias("MBR_LAST_NM"),
    trim(col("MBR_BRTH_DT_SK")).alias("MBR_BRTH_DT_SK"),
    trim(col("MBR_GNDR_CD")).alias("MBR_GNDR_CD"),
    trim(col("MBR_RELSHP_CD")).alias("MBR_RELSHP_CD"),
    trim(col("SUBSIDY_EFF_DT_1")).alias("SUBSIDY_EFF_DT_1"),
    trim(col("SUBSIDY_TERM_DT_1")).alias("SUBSIDY_TERM_DT_1"),
    lit("          ").alias("SUBSIDY_EFF_DT_2"),
    lit("          ").alias("SUBSIDY_TERM_DT_2"),
    lit("          ").alias("SUBSIDY_EFF_DT_3"),
    lit("          ").alias("SUBSIDY_TERM_DT_3"),
    lit("          ").alias("SUBSIDY_EFF_DT_4"),
    lit("          ").alias("SUBSIDY_TERM_DT_4"),
    lit("          ").alias("SUBSIDY_EFF_DT_5"),
    lit("          ").alias("SUBSIDY_TERM_DT_5"),
    lit("          ").alias("SUBSIDY_EFF_DT_6"),
    lit("          ").alias("SUBSIDY_TERM_DT_6"),
    current_date().alias("DATE_SUBMITTED"),
    concat(year(current_timestamp()), lit('-01-01')).alias("PLAN_YR_START_DT"),
    concat(year(current_timestamp()), lit('-12-31')).alias("PLAN_YR_END_DT"),
    trim(col("GRP_SK")).alias("GRP_SK"),
    trim(col("MBR_SK")).alias("MBR_SK"),
    trim(col("MBR_COB_EFF_DT_SK")).alias("MBR_COB_EFF_DT_SK"),
    trim(col("MBR_COB_TERM_DT_SK")).alias("MBR_COB_TERM_DT_SK"),
    trim(col("CLS_PLN_DTL_EFF_DT_SK")).alias("CLS_PLN_DTL_EFF_DT_SK"),
    trim(col("CLS_PLN_DTL_TERM_DT_SK")).alias("CLS_PLN_DTL_TERM_DT_SK"),
    trim(col("MBR_COB_PAYMT_PRTY_CD")).alias("MBR_COB_PAYMT_PRTY_CD"),
)

# trans_gm_subs_trm_date (CTransformerStage)
df_trans_gm_subs_trm_date = df_trans_gm_output.select(
    col("MBR_ID"),
    col("MBR_SFX_NO"),
    col("MBR_SSN"),
    col("MBR_MCARE_NO"),
    col("MBR_FIRST_NM"),
    col("MBR_MIDINIT"),
    col("MBR_LAST_NM"),
    col("MBR_BRTH_DT_SK"),
    col("MBR_GNDR_CD"),
    col("MBR_RELSHP_CD"),
    when(
        (col("PLAN_YR_START_DT") >= col("MBR_COB_EFF_DT_SK")) & (col("PLAN_YR_START_DT") >= col("CLS_PLN_DTL_EFF_DT_SK")),
        col("PLAN_YR_START_DT")
    ).when(
        (col("MBR_COB_EFF_DT_SK") >= col("CLS_PLN_DTL_EFF_DT_SK")) & (col("MBR_COB_EFF_DT_SK") >= col("PLAN_YR_START_DT")),
        col("MBR_COB_EFF_DT_SK")
    ).when(
        (col("CLS_PLN_DTL_EFF_DT_SK") >= col("PLAN_YR_START_DT")) & (col("CLS_PLN_DTL_EFF_DT_SK") >= col("MBR_COB_EFF_DT_SK")),
        col("CLS_PLN_DTL_EFF_DT_SK")
    ).otherwise(col("PLAN_YR_START_DT")).alias("SUBSIDY_EFF_DT_1"),
    when(
        (col("PLAN_YR_END_DT") <= col("MBR_COB_TERM_DT_SK")) & (col("PLAN_YR_END_DT") <= col("CLS_PLN_DTL_TERM_DT_SK")),
        col("PLAN_YR_END_DT")
    ).when(
        (col("MBR_COB_TERM_DT_SK") <= col("CLS_PLN_DTL_TERM_DT_SK")) & (col("MBR_COB_TERM_DT_SK") <= col("PLAN_YR_END_DT")),
        col("MBR_COB_TERM_DT_SK")
    ).when(
        (col("CLS_PLN_DTL_TERM_DT_SK") <= col("PLAN_YR_END_DT")) & (col("CLS_PLN_DTL_TERM_DT_SK") <= col("MBR_COB_TERM_DT_SK")),
        col("CLS_PLN_DTL_TERM_DT_SK")
    ).otherwise(col("PLAN_YR_END_DT")).alias("SUBSIDY_TERM_DT_1"),
    col("SUBSIDY_EFF_DT_2"),
    col("SUBSIDY_TERM_DT_2"),
    col("SUBSIDY_EFF_DT_3"),
    col("SUBSIDY_TERM_DT_3"),
    col("SUBSIDY_EFF_DT_4"),
    col("SUBSIDY_TERM_DT_4"),
    col("SUBSIDY_EFF_DT_5"),
    col("SUBSIDY_TERM_DT_5"),
    col("SUBSIDY_EFF_DT_6"),
    col("SUBSIDY_TERM_DT_6"),
    col("DATE_SUBMITTED"),
    col("PLAN_YR_START_DT"),
    col("PLAN_YR_END_DT"),
    col("GRP_SK"),
    col("MBR_SK")
)

# hf_medd_gm (CHashedFileStage) -> Scenario A dedup
df_hf_medd_gm = df_trans_gm_subs_trm_date.dropDuplicates(["MBR_ID", "MBR_SFX_NO"])
df_lc_medd_motors_gm = df_hf_medd_gm

# lc_medd_motors (CCollector) -> union of Ford & GM
df_lc_medd_motors = df_lc_medd_motors_ford.unionByName(df_lc_medd_motors_gm)

# motors_output (CSeqFileStage)
# Final select to maintain column order and apply rpad for char columns
df_final = df_lc_medd_motors.select(
    rpad(col("MBR_ID"), 9, " ").alias("MBR_ID"),
    rpad(col("MBR_SFX_NO"), 2, " ").alias("MBR_SFX_NO"),
    rpad(col("MBR_SSN"), 9, " ").alias("MBR_SSN"),
    rpad(col("MBR_MCARE_NO"), 12, " ").alias("MBR_MCARE_NO"),
    rpad(col("MBR_FIRST_NM"), 30, " ").alias("MBR_FIRST_NM"),
    rpad(col("MBR_MIDINIT"), 1, " ").alias("MBR_MIDINIT"),
    rpad(col("MBR_LAST_NM"), 40, " ").alias("MBR_LAST_NM"),
    rpad(col("MBR_BRTH_DT_SK"), 10, " ").alias("MBR_BRTH_DT_SK"),
    rpad(col("MBR_GNDR_CD"), 20, " ").alias("MBR_GNDR_CD"),
    rpad(col("MBR_RELSHP_CD"), 20, " ").alias("MBR_RELSHP_CD"),
    rpad(col("SUBSIDY_EFF_DT_1"), 10, " ").alias("SUBSIDY_EFF_DT_1"),
    rpad(col("SUBSIDY_TERM_DT_1"), 10, " ").alias("SUBSIDY_TERM_DT_1"),
    rpad(col("SUBSIDY_EFF_DT_2"), 10, " ").alias("SUBSIDY_EFF_DT_2"),
    rpad(col("SUBSIDY_TERM_DT_2"), 10, " ").alias("SUBSIDY_TERM_DT_2"),
    rpad(col("SUBSIDY_EFF_DT_3"), 10, " ").alias("SUBSIDY_EFF_DT_3"),
    rpad(col("SUBSIDY_TERM_DT_3"), 10, " ").alias("SUBSIDY_TERM_DT_3"),
    rpad(col("SUBSIDY_EFF_DT_4"), 10, " ").alias("SUBSIDY_EFF_DT_4"),
    rpad(col("SUBSIDY_TERM_DT_4"), 10, " ").alias("SUBSIDY_TERM_DT_4"),
    rpad(col("SUBSIDY_EFF_DT_5"), 10, " ").alias("SUBSIDY_EFF_DT_5"),
    rpad(col("SUBSIDY_TERM_DT_5"), 10, " ").alias("SUBSIDY_TERM_DT_5"),
    rpad(col("SUBSIDY_EFF_DT_6"), 10, " ").alias("SUBSIDY_EFF_DT_6"),
    rpad(col("SUBSIDY_TERM_DT_6"), 10, " ").alias("SUBSIDY_TERM_DT_6"),
    rpad(col("DATE_SUBMITTED"), 10, " ").alias("DATE_SUBMITTED"),
    rpad(col("PLAN_YR_START_DT"), 10, " ").alias("PLAN_YR_START_DT"),
    rpad(col("PLAN_YR_END_DT"), 10, " ").alias("PLAN_YR_END_DT"),
    col("GRP_SK").alias("GRP_SK"),
    col("MBR_SK").alias("MBR_SK")
)

write_files(
    df_final,
    f"{adls_path_publish}/external/Mbr_MedicareD_Retiree.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)