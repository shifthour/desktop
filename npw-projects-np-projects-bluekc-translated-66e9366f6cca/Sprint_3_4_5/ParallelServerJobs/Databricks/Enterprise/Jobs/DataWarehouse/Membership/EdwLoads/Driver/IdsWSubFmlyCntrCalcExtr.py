# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC DESCRIPTION:  
# MAGIC 
# MAGIC ** This job creates a load file for IDS W_SUB_FMLY_CNTR_CAL table. 
# MAGIC ** Creates a record by the for every subscriber loaded to the IDS W_MBR_DRVR table.   
# MAGIC ** The record will contain a date for which MED or DNTL eligiblity is checked all members (including the sub).   
# MAGIC ** Table W_SUB_FMLY_CNTR_CALC is used by the IdsSubDExtr job. 
# MAGIC ** The order for which the dates are picked is set:  first current, then future and then past.
# MAGIC *  Find all members per contract with MED OR DNTL future eligibility, determine the minium effective date and load that to the table.
# MAGIC ** Find all members per contract with past MED OR DNTL  eligibility, determine the maxium termination date and load that to the table.  If the contract has any member with future elig, overide the date with the past maxium term date.
# MAGIC **  Find any member with current MED OR DNTL eligibility per contract, and load the #CurrentDate# to the table.  Override any future or past date with the #CurrentDate#.
# MAGIC ** If a record is loaded with a date of 1753-01-01 that means there never existed a member with eligibility - ever.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                                                             -------------------------------       -------------------------------   ----------------------------       
# MAGIC Sharon Andrew        09/05/2006                                   Initial Creation.
# MAGIC SAndrew                  01/15/2008      IAD Qrt Release    Added where criteria to the future, past and current eligibility selections to target        devlEDW                       Steph Goddard          01/16/2008
# MAGIC                                                                                               only Medical and Dental enrollment.                                                                             
# MAGIC 
# MAGIC Srikanth Mettpalli      06/17/2013        5114                              Original Programming                                                                                       EnterpriseWrhsDevl         Jag Yelavarthi            2013-09-15
# MAGIC                                                                                                    (Server to Parallel Conversion)                
# MAGIC 
# MAGIC 
# MAGIC Srikanth Mettpalli      11/25/2013        5114                              Production Fix - Daptiv Defect # 551 under project #5114                             EnterpriseWrhsDevl         Jag Yelavarthi            2013-11-26
# MAGIC 
# MAGIC Jag Yelavarthi          2015-10-21         TFS#10773         Added MBR_ENR_CLS_PLN_PROD_CAT_CD column into the work table              EnterpriseDev1                Kalyan Neelam           2015-10-26
# MAGIC                                                                                        for separating the CNTR_CALC values for Medical and Dental

# MAGIC ** This job creates a load file for IDS W_SUB_FMLY_CNTR_CAL table. 
# MAGIC ** Creates a record by the for every subscriber loaded to the IDS W_MBR_DRVR table.   
# MAGIC ** The record will contain a date for which MED or DNTL eligiblity is checked all members (including the sub).   
# MAGIC ** Table W_SUB_FMLY_CNTR_CALC is used by the IdsSubDExtr job. 
# MAGIC ** The order for which the dates are picked is set:  first current, then future and then past.
# MAGIC *  Find all members per contract with MED OR DNTL future eligibility, determine the minium effective date and load that to the table.
# MAGIC ** Find all members per contract with past MED OR DNTL  eligibility, determine the maxium termination date and load that to the table.  If the contract has any member with future elig, overide the date with the past maxium term date.
# MAGIC **  Find any member with current MED OR DNTL eligibility per contract, and load the #CurrentDate# to the table.  Override any future or past date with the #CurrentDate#.
# MAGIC ** If a record is loaded with a date of 1753-01-01 that means there never existed a member with eligibility - ever.
# MAGIC Job Name: IdsWSubFmlyCntrCalcExtr
# MAGIC Load File for IDS W_SUB_FMLY_CNTR_CALC
# MAGIC Business Logic to Apply Future, Past and Current Dates
# MAGIC Pull all the Subscriber Records from IDS
# MAGIC Left Join on SUB_KEY for Future, Past and Current Dates
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, when, rpad
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
CurrentDate = get_widget_value("CurrentDate","")

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query_db2_SUB_Dental_FutureDt = f"""
SELECT
MBR.SUB_SK,
MIN(ENR.EFF_DT_SK) AS SUB_FMLY_CNTR_DT_DNTL_F,
MAP1.SRC_CD as MBR_ENR_CLS_PLN_PROD_CAT_CD
FROM {IDSOwner}.MBR MBR,
     {IDSOwner}.SUB SUB,
     {IDSOwner}.MBR_ENR ENR,
     {IDSOwner}.CD_MPPNG MAP1
WHERE SUB.SUB_SK = MBR.SUB_SK
  AND MBR.MBR_SK = ENR.MBR_SK
  AND ENR.ELIG_IN = 'Y'
  AND ENR.EFF_DT_SK >= '{CurrentDate}'
  AND ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = MAP1.CD_MPPNG_SK
  AND MAP1.SRC_CD='D'
  AND ENR.MBR_SK in (
      SELECT ENR2.MBR_SK
      FROM {IDSOwner}.MBR_ENR ENR2,
           {IDSOwner}.CD_MPPNG MAP2
      WHERE ENR2.MBR_SK = MBR.MBR_SK
        AND ENR2.ELIG_IN = 'Y'
        AND ENR2.EFF_DT_SK >= '{CurrentDate}'
        AND ENR2.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = MAP2.CD_MPPNG_SK
        AND MAP2.SRC_CD= 'D'
  )
GROUP BY
MBR.SUB_SK,
MAP1.SRC_CD
"""
df_db2_SUB_Dental_FutureDt = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_SUB_Dental_FutureDt)
    .load()
)

extract_query_db2_SUB_Dental_PastDt = f"""
SELECT
MBR.SUB_SK,
MAP1.SRC_CD as MBR_ENR_CLS_PLN_PROD_CAT_CD,
MAX(ENR.TERM_DT_SK) AS SUB_FMLY_CNTR_DT_DNTL_P
FROM {IDSOwner}.MBR MBR,
     {IDSOwner}.SUB SUB,
     {IDSOwner}.MBR_ENR ENR,
     {IDSOwner}.CD_MPPNG MAP1
WHERE SUB.SUB_SK = MBR.SUB_SK
  AND MBR.MBR_SK = ENR.MBR_SK
  AND ENR.ELIG_IN = 'Y'
  AND ENR.EFF_DT_SK <= '{CurrentDate}'
  AND ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = MAP1.CD_MPPNG_SK
  AND MAP1.SRC_CD='D'
  AND ENR.MBR_SK in (
      SELECT ENR2.MBR_SK
      FROM {IDSOwner}.MBR_ENR ENR2,
           {IDSOwner}.CD_MPPNG MAP2
      WHERE ENR2.MBR_SK = MBR.MBR_SK
        AND ENR2.ELIG_IN = 'Y'
        AND ENR2.TERM_DT_SK <= '{CurrentDate}'
        AND ENR2.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = MAP2.CD_MPPNG_SK
        AND MAP2.SRC_CD='D'
  )
GROUP BY
MBR.SUB_SK,
MAP1.SRC_CD
"""
df_db2_SUB_Dental_PastDt = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_SUB_Dental_PastDt)
    .load()
)

extract_query_db2_SUB_Dental_CurrentDt = f"""
SELECT DISTINCT
MBR.SUB_SK,
MAP.SRC_CD as MBR_ENR_CLS_PLN_PROD_CAT_CD,
'{CurrentDate}' as SUB_FMLY_CNTR_DT_DNTL_C
FROM {IDSOwner}.MBR MBR,
     {IDSOwner}.SUB SUB,
     {IDSOwner}.MBR_ENR ENR,
     {IDSOwner}.CD_MPPNG MAP
WHERE MBR.SUB_SK = SUB.SUB_SK
  AND MBR.MBR_SK = ENR.MBR_SK
  AND ENR.ELIG_IN = 'Y'
  AND ENR.EFF_DT_SK <= '{CurrentDate}'
  AND ENR.TERM_DT_SK >= '{CurrentDate}'
  AND ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = MAP.CD_MPPNG_SK
  AND MAP.SRC_CD='D'
"""
df_db2_SUB_Dental_CurrentDt = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_SUB_Dental_CurrentDt)
    .load()
)

extract_query_db2_SUB_Medical_CurrentDt = f"""
SELECT DISTINCT
MBR.SUB_SK,
MAP.SRC_CD as MBR_ENR_CLS_PLN_PROD_CAT_CD,
'{CurrentDate}' as SUB_FMLY_CNTR_DT_MED_C
FROM {IDSOwner}.MBR MBR,
     {IDSOwner}.SUB SUB,
     {IDSOwner}.MBR_ENR ENR,
     {IDSOwner}.CD_MPPNG MAP
WHERE MBR.SUB_SK = SUB.SUB_SK
  AND MBR.MBR_SK = ENR.MBR_SK
  AND ENR.ELIG_IN = 'Y'
  AND ENR.EFF_DT_SK <= '{CurrentDate}'
  AND ENR.TERM_DT_SK >= '{CurrentDate}'
  AND ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = MAP.CD_MPPNG_SK
  AND MAP.SRC_CD='M'
"""
df_db2_SUB_Medical_CurrentDt = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_SUB_Medical_CurrentDt)
    .load()
)

extract_query_db2_SUB_Medical_PastDt = f"""
SELECT
MBR.SUB_SK,
MAP1.SRC_CD as MBR_ENR_CLS_PLN_PROD_CAT_CD,
MAX(ENR.TERM_DT_SK) AS SUB_FMLY_CNTR_DT_MED_P
FROM {IDSOwner}.MBR MBR,
     {IDSOwner}.SUB SUB,
     {IDSOwner}.MBR_ENR ENR,
     {IDSOwner}.CD_MPPNG MAP1
WHERE SUB.SUB_SK = MBR.SUB_SK
  AND MBR.MBR_SK = ENR.MBR_SK
  AND ENR.ELIG_IN = 'Y'
  AND ENR.EFF_DT_SK <= '{CurrentDate}'
  AND ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = MAP1.CD_MPPNG_SK
  AND MAP1.SRC_CD='M'
  AND ENR.MBR_SK in (
      SELECT ENR2.MBR_SK
      FROM {IDSOwner}.MBR_ENR ENR2,
           {IDSOwner}.CD_MPPNG MAP2
      WHERE ENR2.MBR_SK = MBR.MBR_SK
        AND ENR2.ELIG_IN = 'Y'
        AND ENR2.TERM_DT_SK <= '{CurrentDate}'
        AND ENR2.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = MAP2.CD_MPPNG_SK
        AND MAP2.SRC_CD='M'
  )
GROUP BY
MBR.SUB_SK,
MAP1.SRC_CD
"""
df_db2_SUB_Medical_PastDt = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_SUB_Medical_PastDt)
    .load()
)

extract_query_db2_SUB_Medical_FutureDt = f"""
SELECT
MBR.SUB_SK,
MAP1.SRC_CD as MBR_ENR_CLS_PLN_PROD_CAT_CD,
MIN(ENR.EFF_DT_SK) AS SUB_FMLY_CNTR_DT_MED_F
FROM {IDSOwner}.MBR MBR,
     {IDSOwner}.SUB SUB,
     {IDSOwner}.MBR_ENR ENR,
     {IDSOwner}.CD_MPPNG MAP1
WHERE SUB.SUB_SK = MBR.SUB_SK
  AND MBR.MBR_SK = ENR.MBR_SK
  AND ENR.ELIG_IN = 'Y'
  AND ENR.EFF_DT_SK >= '{CurrentDate}'
  AND ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = MAP1.CD_MPPNG_SK
  AND MAP1.SRC_CD='M'
  AND ENR.MBR_SK in (
      SELECT ENR2.MBR_SK
      FROM {IDSOwner}.MBR_ENR ENR2,
           {IDSOwner}.CD_MPPNG MAP2
      WHERE ENR2.MBR_SK = MBR.MBR_SK
        AND ENR2.ELIG_IN = 'Y'
        AND ENR2.EFF_DT_SK >= '{CurrentDate}'
        AND ENR2.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = MAP2.CD_MPPNG_SK
        AND MAP2.SRC_CD='M'
  )
GROUP BY
MBR.SUB_SK,
MAP1.SRC_CD
"""
df_db2_SUB_Medical_FutureDt = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_SUB_Medical_FutureDt)
    .load()
)

extract_query_db2_SUB_in = f"""
SELECT
MBR.SUB_SK,
SUB.SUB_UNIQ_KEY,
'1753-01-01' as SUB_FMLY_CNTR_DT,
'M' as MBR_ENR_CLS_PLN_PROD_CAT_CD
FROM {IDSOwner}.MBR MBR,
     {IDSOwner}.SUB SUB,
     {IDSOwner}.CD_MPPNG C
WHERE SUB.SUB_SK = MBR.SUB_SK
  AND MBR.MBR_RELSHP_CD_SK = C.CD_MPPNG_SK
  AND C.TRGT_CD = 'SUB'
GROUP BY
MBR.SUB_SK,
SUB.SUB_UNIQ_KEY

UNION

SELECT
SUB.SUB_SK,
SUB.SUB_UNIQ_KEY,
'1753-01-01' AS SUB_FMLY_CNTR_DT,
'M' as MBR_ENR_CLS_PLN_PROD_CAT_CD
FROM {IDSOwner}.SUB SUB

UNION

SELECT
MBR.SUB_SK,
SUB.SUB_UNIQ_KEY,
'1753-01-01' as SUB_FMLY_CNTR_DT,
'D' as MBR_ENR_CLS_PLN_PROD_CAT_CD
FROM {IDSOwner}.MBR MBR,
     {IDSOwner}.SUB SUB,
     {IDSOwner}.CD_MPPNG C
WHERE SUB.SUB_SK = MBR.SUB_SK
  AND MBR.MBR_RELSHP_CD_SK = C.CD_MPPNG_SK
  AND C.TRGT_CD = 'SUB'
GROUP BY
MBR.SUB_SK,
SUB.SUB_UNIQ_KEY

UNION

SELECT
SUB.SUB_SK,
SUB.SUB_UNIQ_KEY,
'1753-01-01' AS SUB_FMLY_CNTR_DT,
'D' as MBR_ENR_CLS_PLN_PROD_CAT_CD
FROM {IDSOwner}.SUB SUB
"""
df_db2_SUB_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_SUB_in)
    .load()
)

df_jn_SUB_Dates = (
    df_db2_SUB_in.alias("lnk_IdsWSubFmlyCntrCalcExtr_InABC")
    .join(
        df_db2_SUB_Medical_FutureDt.alias("lnk_SUB_Med_min_effective_in"),
        on=["SUB_SK", "MBR_ENR_CLS_PLN_PROD_CAT_CD"],
        how="left"
    )
    .join(
        df_db2_SUB_Medical_PastDt.alias("lnk_SUB_Med_max_term_in"),
        on=["SUB_SK", "MBR_ENR_CLS_PLN_PROD_CAT_CD"],
        how="left"
    )
    .join(
        df_db2_SUB_Medical_CurrentDt.alias("lnk_SUB_Med_current_date_in"),
        on=["SUB_SK", "MBR_ENR_CLS_PLN_PROD_CAT_CD"],
        how="left"
    )
    .join(
        df_db2_SUB_Dental_FutureDt.alias("lnk_SUB_Dntl_min_effective_in"),
        on=["SUB_SK", "MBR_ENR_CLS_PLN_PROD_CAT_CD"],
        how="left"
    )
    .join(
        df_db2_SUB_Dental_PastDt.alias("lnk_SUB_Dntl_max_term_in"),
        on=["SUB_SK", "MBR_ENR_CLS_PLN_PROD_CAT_CD"],
        how="left"
    )
    .join(
        df_db2_SUB_Dental_CurrentDt.alias("lnk_SUB_Dntl_current_date_in"),
        on=["SUB_SK", "MBR_ENR_CLS_PLN_PROD_CAT_CD"],
        how="left"
    )
    .select(
        col("lnk_IdsWSubFmlyCntrCalcExtr_InABC.SUB_SK").alias("SUB_SK"),
        col("lnk_IdsWSubFmlyCntrCalcExtr_InABC.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
        col("lnk_IdsWSubFmlyCntrCalcExtr_InABC.SUB_FMLY_CNTR_DT").alias("SUB_FMLY_CNTR_DT"),
        col("lnk_IdsWSubFmlyCntrCalcExtr_InABC.MBR_ENR_CLS_PLN_PROD_CAT_CD").alias("MBR_ENR_CLS_PLN_PROD_CAT_CD"),
        col("lnk_SUB_Med_min_effective_in.SUB_FMLY_CNTR_DT_MED_F").alias("SUB_FMLY_CNTR_DT_MED_F"),
        col("lnk_SUB_Med_max_term_in.SUB_FMLY_CNTR_DT_MED_P").alias("SUB_FMLY_CNTR_DT_MED_P"),
        col("lnk_SUB_Med_current_date_in.SUB_FMLY_CNTR_DT_MED_C").alias("SUB_FMLY_CNTR_DT_MED_C"),
        col("lnk_SUB_Dntl_min_effective_in.SUB_FMLY_CNTR_DT_DNTL_F").alias("SUB_FMLY_CNTR_DT_DNTL_F"),
        col("lnk_SUB_Dntl_max_term_in.SUB_FMLY_CNTR_DT_DNTL_P").alias("SUB_FMLY_CNTR_DT_DNTL_P"),
        col("lnk_SUB_Dntl_current_date_in.SUB_FMLY_CNTR_DT_DNTL_C").alias("SUB_FMLY_CNTR_DT_DNTL_C")
    )
)

df_xfrm_SUB_BusinessLogic_1 = df_jn_SUB_Dates.withColumn(
    "svSubFmlyCntrDtSkMed",
    when(col("SUB_FMLY_CNTR_DT_MED_C").isNull(),
         when(col("SUB_FMLY_CNTR_DT_MED_P").isNull(),
              when(col("SUB_FMLY_CNTR_DT_MED_F").isNull(), col("SUB_FMLY_CNTR_DT"))
              .otherwise(col("SUB_FMLY_CNTR_DT_MED_F"))
         )
         .otherwise(col("SUB_FMLY_CNTR_DT_MED_P"))
    ).otherwise(col("SUB_FMLY_CNTR_DT_MED_C"))
).withColumn(
    "svSubFmlyCntrDtSkDntl",
    when(col("SUB_FMLY_CNTR_DT_DNTL_C").isNull(),
         when(col("SUB_FMLY_CNTR_DT_DNTL_P").isNull(),
              when(col("SUB_FMLY_CNTR_DT_DNTL_F").isNull(), col("SUB_FMLY_CNTR_DT"))
              .otherwise(col("SUB_FMLY_CNTR_DT_DNTL_F"))
         )
         .otherwise(col("SUB_FMLY_CNTR_DT_DNTL_P"))
    ).otherwise(col("SUB_FMLY_CNTR_DT_DNTL_C"))
)

df_xfrm_SUB_BusinessLogic_2 = df_xfrm_SUB_BusinessLogic_1.withColumn(
    "SUB_FMLY_CNTR_DT_SK",
    when(col("MBR_ENR_CLS_PLN_PROD_CAT_CD") == "M", col("svSubFmlyCntrDtSkMed"))
    .otherwise(col("svSubFmlyCntrDtSkDntl"))
)

df_enriched = df_xfrm_SUB_BusinessLogic_2.select(
    col("SUB_SK"),
    col("SUB_UNIQ_KEY"),
    col("SUB_FMLY_CNTR_DT_SK"),
    col("MBR_ENR_CLS_PLN_PROD_CAT_CD")
)

df_enriched = df_enriched.withColumn(
    "SUB_FMLY_CNTR_DT_SK",
    rpad(col("SUB_FMLY_CNTR_DT_SK"), 10, " ")
)

write_files(
    df_enriched,
    f"{adls_path}/load/W_SUB_FMLY_CNTR_CALC.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)