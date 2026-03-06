# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 01/30/08 05:00:10 Batch  14640_18019 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 10/31/07 14:11:20 Batch  14549_51083 PROMOTE bckcetl ids20 dsadm bls for on
# MAGIC ^1_1 10/31/07 12:55:15 Batch  14549_46521 PROMOTE bckcetl ids20 dsadm bls for on
# MAGIC ^1_1 10/31/07 12:32:28 Batch  14549_45152 INIT bckcett testIDS30 dsadm bls for on
# MAGIC ^1_1 10/26/07 08:07:18 Batch  14544_29241 PROMOTE bckcett testIDS30 u03651 steffy
# MAGIC ^1_1 10/26/07 07:46:17 Batch  14544_27982 INIT bckcett devlIDS30 u03651 steffy
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:     Both FctsCommissionDailyBalCntl and FctsCommissionMonthlyBalCntl
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   
# MAGIC                 The Job loads the SUBJECT_AREA_BAL_SUM table after the balancing control job is done for a given Subject Area              
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               6/5/2007          3264                              Originally Programmed                           devlIDS30                     Steph Goddard            09/17/2007


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

UWSOwner = get_widget_value('UWSOwner','')
uws_secret_name = get_widget_value('uws_secret_name','')
Target = get_widget_value('Target','')
Subject = get_widget_value('Subject','')
CurrDate = get_widget_value('CurrDate','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
UWSAcct = get_widget_value('UWSAcct','')

jdbc_url, jdbc_props = get_db_config(uws_secret_name)

# TBL_BAL_SUM -> TblBalExtr (first query)
query_TblBalExtr = f"""
SELECT 
  TBL_BAL_SUM.TRGT_SYS_CD, 
  TBL_BAL_SUM.SUBJ_AREA_NM, 
  TBL_BAL_SUM.BAL_DT, 
  TBL_BAL_SUM.ROW_CT_TLRNC_CD
FROM {UWSOwner}.TBL_BAL_SUM TBL_BAL_SUM
WHERE TBL_BAL_SUM.TRGT_SYS_CD = '{Target}'
  AND TBL_BAL_SUM.SUBJ_AREA_NM = '{Subject}'
ORDER BY TBL_BAL_SUM.BAL_DT ASC
"""
df_TblBalExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_TblBalExtr)
    .load()
)

df_TblBalExtr = df_TblBalExtr.alias("TblBalExtr")

# TBL_BAL_SUM -> ComsnCt (second query)
query_ComsnCt = f"""
SELECT 
  TBL_BAL_SUM.TRGT_SYS_CD, 
  TBL_BAL_SUM.SUBJ_AREA_NM,
  COUNT(*) AS ROW_COUNT
FROM {UWSOwner}.TBL_BAL_SUM TBL_BAL_SUM
WHERE TBL_BAL_SUM.TRGT_SYS_CD = '{Target}'
  AND TBL_BAL_SUM.SUBJ_AREA_NM = '{Subject}'
GROUP BY 
  TBL_BAL_SUM.TRGT_SYS_CD,
  TBL_BAL_SUM.SUBJ_AREA_NM
"""
df_ComsnCt = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_ComsnCt)
    .load()
)

df_ComsnCt = df_ComsnCt.alias("ComsnCt")

# hf_comsn_tbl_bal_lkup (CHashedFileStage) - Scenario A
# We drop duplicates on key columns: TRGT_SYS_CD, SUBJ_AREA_NM
df_ComsnCtLoad = df_ComsnCt.dropDuplicates(["TRGT_SYS_CD","SUBJ_AREA_NM"]).alias("ComsnCtLoad")

# Transform stage: Primary (TblBalExtr) joined left with ComsnCtLoad
df_join = df_TblBalExtr.join(
    df_ComsnCtLoad,
    [
        F.col("TblBalExtr.TRGT_SYS_CD") == F.col("ComsnCtLoad.TRGT_SYS_CD"),
        F.col("TblBalExtr.SUBJ_AREA_NM") == F.col("ComsnCtLoad.SUBJ_AREA_NM")
    ],
    "left"
)

# --------------------------------------------------------------------------
# Replicate the row-based transformer variables as best as possible in a window.
# Because the original logic references prior values of the same stage variables,
# we use a simple window to track row numbering and approximate the logic.
# --------------------------------------------------------------------------

w = Window.orderBy(F.col("TblBalExtr.BAL_DT").asc())

# Approximate row counting logic
df_with_counts = (
    df_join
    .withColumn("row_index", F.row_number().over(w))
    # We interpret: svCurrCount = svPrevCount + 1 => effectively row_index + 1, but DataStage reassigns them in a loop.
    # We'll store a simple approach for the stage variables:
    # svPrevCount = the row_index - 1
    # svCurrCount = row_index
    .withColumn("svPrevCount", F.col("row_index") - F.lit(1))
    .withColumn("svCurrCount", F.col("row_index"))
)

# Replicate the tricky tolerance logic in a non-iterative manner:
# Because "svTempTlrncCd" references itself, we cannot do perfect iteration.
# We create a partial approximation:
#   If the first 10 chars of BAL_DT != CurrDate => 'NEXT ROW'
#   else if ROW_CT_TLRNC_CD='BAL' => 'BAL'
#   else if ROW_CT_TLRNC_CD='IN' => 'IN'
#   else => 'OUT'
df_transformed = (
    df_with_counts
    .withColumn(
        "svTempTlrncCd",
        F.when(
            F.substring(F.col("TblBalExtr.BAL_DT"), 1, 10) != CurrDate,
            F.lit("NEXT ROW")
        ).when(
            F.col("TblBalExtr.ROW_CT_TLRNC_CD") == F.lit("BAL"),
            F.lit("BAL")
        ).when(
            F.col("TblBalExtr.ROW_CT_TLRNC_CD") == F.lit("IN"),
            F.lit("IN")
        ).otherwise(F.lit("OUT"))
    )
    # Similarly approximate for svTempRslvIn
    # If BAL_DT[1,10] = CurrDate => keep old value, else if current TLRNC_CD in('BAL','IN') => 'Y' else 'N'
    .withColumn(
        "svTempRslvIn",
        F.when(
            F.substring(F.col("TblBalExtr.BAL_DT"), 1, 10) == CurrDate,
            F.lit("Y")
        ).when(
            F.col("TblBalExtr.ROW_CT_TLRNC_CD").isin("BAL", "IN"),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    # Similarly approximate for svTempRslvIncd
    .withColumn(
        "svTempRslvIncd",
        F.when(
            F.substring(F.col("TblBalExtr.BAL_DT"), 1, 10) == CurrDate,
            F.lit("NA")
        ).when(
            F.col("TblBalExtr.ROW_CT_TLRNC_CD") == F.lit("BAL"),
            F.lit("BAL")
        ).when(
            F.col("TblBalExtr.ROW_CT_TLRNC_CD") == F.lit("IN"),
            F.lit("IN")
        ).otherwise(F.lit("OUT"))
    )
)

# Constraint: "svPrevCount = ComsnCtLoad.ROW_COUNT"
df_filtered = df_transformed.filter(
    F.col("svPrevCount") == F.col("ComsnCtLoad.ROW_COUNT")
)

# Produce final columns for SubjAreaBalLoad
# TRGT_SYS_CD -> "Target"
# SUBJ_AREA_NM -> "Subject"
# LAST_BAL_DT -> CurrDate
# TLRNC_CD -> "svTempTlrncCd"
# PREV_ISSUE_CRCTD_IN -> "svTempRslvIn" (char(1), so rpad to length 1)
# TRGT_RUN_CYC_NO -> "ExtrRunCycle"
# CRT_DTM -> CurrDate
# LAST_UPDT_DTM -> CurrDate
# USER_ID -> UWSAcct

df_SubjAreaBalLoad = (
    df_filtered
    .withColumn("TRGT_SYS_CD", F.lit(Target))
    .withColumn("SUBJ_AREA_NM", F.lit(Subject))
    .withColumn("LAST_BAL_DT", F.lit(CurrDate))
    .withColumn("TLRNC_CD", F.col("svTempTlrncCd"))
    .withColumn(
        "PREV_ISSUE_CRCTD_IN",
        F.rpad(F.col("svTempRslvIn"), 1, " ")
    )
    .withColumn("TRGT_RUN_CYC_NO", F.lit(ExtrRunCycle))
    .withColumn("CRT_DTM", F.lit(CurrDate))
    .withColumn("LAST_UPDT_DTM", F.lit(CurrDate))
    .withColumn("USER_ID", F.lit(UWSAcct))
)

df_SubjAreaBalLoad = df_SubjAreaBalLoad.select(
    "TRGT_SYS_CD",
    "SUBJ_AREA_NM",
    "LAST_BAL_DT",
    "TLRNC_CD",
    "PREV_ISSUE_CRCTD_IN",
    "TRGT_RUN_CYC_NO",
    "CRT_DTM",
    "LAST_UPDT_DTM",
    "USER_ID"
)

# Now we replicate the CODBCStage for SUBJ_AREA_BAL_SUM with upsert logic.
# Primary key: (TRGT_SYS_CD, SUBJ_AREA_NM)

# 1) Write df_SubjAreaBalLoad into a physical STAGING table.
temp_table_name = "STAGING.UwsComsnSubjAreaBalSumLoad_SUBJ_AREA_BAL_SUM_temp"

execute_dml(f"DROP TABLE IF EXISTS {temp_table_name}", jdbc_url, jdbc_props)

write_files(
    df_SubjAreaBalLoad,
    temp_table_name,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)

# 2) Build MERGE statement
#    Insert columns: (TRGT_SYS_CD, SUBJ_AREA_NM, LAST_BAL_DT, TLRNC_CD, PREV_ISSUE_CRCTD_IN, TRGT_RUN_CYC_NO, CRT_DTM, LAST_UPDT_DTM, USER_ID)
#    Update columns: (LAST_BAL_DT, TLRNC_CD, PREV_ISSUE_CRCTD_IN, TRGT_RUN_CYC_NO, CRT_DTM, LAST_UPDT_DTM, USER_ID)
merge_sql = f"""
MERGE INTO {UWSOwner}.SUBJ_AREA_BAL_SUM AS T
USING {temp_table_name} AS S
ON T.TRGT_SYS_CD = S.TRGT_SYS_CD
  AND T.SUBJ_AREA_NM = S.SUBJ_AREA_NM
WHEN MATCHED THEN
  UPDATE SET
    T.LAST_BAL_DT = S.LAST_BAL_DT,
    T.TLRNC_CD = S.TLRNC_CD,
    T.PREV_ISSUE_CRCTD_IN = S.PREV_ISSUE_CRCTD_IN,
    T.TRGT_RUN_CYC_NO = S.TRGT_RUN_CYC_NO,
    T.CRT_DTM = S.CRT_DTM,
    T.LAST_UPDT_DTM = S.LAST_UPDT_DTM,
    T.USER_ID = S.USER_ID
WHEN NOT MATCHED THEN
  INSERT (
    TRGT_SYS_CD, SUBJ_AREA_NM, LAST_BAL_DT, TLRNC_CD, PREV_ISSUE_CRCTD_IN,
    TRGT_RUN_CYC_NO, CRT_DTM, LAST_UPDT_DTM, USER_ID
  )
  VALUES (
    S.TRGT_SYS_CD, S.SUBJ_AREA_NM, S.LAST_BAL_DT, S.TLRNC_CD, S.PREV_ISSUE_CRCTD_IN,
    S.TRGT_RUN_CYC_NO, S.CRT_DTM, S.LAST_UPDT_DTM, S.USER_ID
  );
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)