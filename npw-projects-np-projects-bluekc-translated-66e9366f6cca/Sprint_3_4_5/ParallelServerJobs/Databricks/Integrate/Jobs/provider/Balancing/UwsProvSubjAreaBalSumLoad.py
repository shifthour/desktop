# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 04/01/09 11:15:41 Batch  15067_40549 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 01/28/08 13:39:53 Batch  14638_49197 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 12/26/07 13:00:58 Batch  14605_46863 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/23/07 09:25:41 Batch  14572_33950 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_3 10/09/07 10:10:10 Batch  14527_36614 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 10/01/07 10:17:35 Batch  14519_37064 PROMOTE bckcetl ids20 dsadm bls for bl
# MAGIC ^1_1 10/01/07 09:47:40 Batch  14519_35267 INIT bckcett testIDS30 dsadm bls for bl
# MAGIC ^1_2 09/26/07 17:31:58 Batch  14514_63125 PROMOTE bckcett testIDS30 u08717 Brent
# MAGIC ^1_2 09/26/07 17:07:37 Batch  14514_61663 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_1 09/25/07 15:06:34 Batch  14513_54400 INIT bckcett devlIDS30 u08717 Brent
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:     Both FctsProvBalCntl and DeaProvBalCntl
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   
# MAGIC                 The Job loads the SUBJECT_AREA_BAL_SUM table after the balancing control job is done for a given Subject Area              
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               05/24/2007          3264                              Originally Programmed                           devlIDS30                Steph Goddard             9/6/07


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Retrieve all required parameter values
UWSOwner = get_widget_value("UWSOwner","")
uws_secret_name = get_widget_value("uws_secret_name","")
Target = get_widget_value("Target","")
Subject = get_widget_value("Subject","")
CurrDate = get_widget_value("CurrDate","")
ExtrRunCycle = get_widget_value("ExtrRunCycle","")
# Not listed in parameters, but used as expression: $UWSAcct
UWSAcct = get_widget_value("UWSAcct","")

# Configure JDBC
jdbc_url, jdbc_props = get_db_config(uws_secret_name)

# ---------------------------------------------------------------------
# Stage: TBL_BAL_SUM (CODBCStage)
#   Output Pin 1: "TblBalExtr"
#   Output Pin 2: "ProvCtExtr"
# ---------------------------------------------------------------------

# TblBalExtr
extract_query_1 = f"SELECT TBL_BAL_SUM.TRGT_SYS_CD, TBL_BAL_SUM.SUBJ_AREA_NM, TBL_BAL_SUM.BAL_DT, TBL_BAL_SUM.ROW_CT_TLRNC_CD FROM {UWSOwner}.TBL_BAL_SUM TBL_BAL_SUM WHERE TBL_BAL_SUM.TRGT_SYS_CD = '{Target}' AND TBL_BAL_SUM.SUBJ_AREA_NM = '{Subject}' ORDER BY TBL_BAL_SUM.BAL_DT ASC"
df_TblBalExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_1)
    .load()
)

# ProvCtExtr
extract_query_2 = f"""
SELECT 
TBL_BAL_SUM.TRGT_SYS_CD,
TBL_BAL_SUM.SUBJ_AREA_NM,
COUNT(*) AS ROW_COUNT
FROM {UWSOwner}.TBL_BAL_SUM TBL_BAL_SUM
WHERE TBL_BAL_SUM.TRGT_SYS_CD = '{Target}' AND TBL_BAL_SUM.SUBJ_AREA_NM = '{Subject}'
GROUP BY
TBL_BAL_SUM.TRGT_SYS_CD,
TBL_BAL_SUM.SUBJ_AREA_NM
"""
df_ProvCtExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_2)
    .load()
)

# ---------------------------------------------------------------------
# Stage: hf_prov_tbl_bal_lkup (CHashedFileStage)
#   Scenario A (intermediate hashed file):
#   Deduplicate on key columns TRGT_SYS_CD, SUBJ_AREA_NM
# ---------------------------------------------------------------------

df_ProvCtExtr_dedup = df_ProvCtExtr.dropDuplicates(["TRGT_SYS_CD", "SUBJ_AREA_NM"])
df_ProvCtLoad = df_ProvCtExtr_dedup  # This replaces the hashed file output

# ---------------------------------------------------------------------
# Stage: Transform (CTransformerStage)
#   Primary Input: TblBalExtr
#   Lookup Input: ProvCtLoad (left join)
#   Constraint on output: svPrevCount = ProvCtLoad.ROW_COUNT
#   Complex row-by-row stage variable logic
# ---------------------------------------------------------------------

df_transform_joined = df_TblBalExtr.alias("TblBalExtr").join(
    df_ProvCtLoad.alias("ProvCtLoad"),
    (F.col("TblBalExtr.TRGT_SYS_CD") == F.col("ProvCtLoad.TRGT_SYS_CD"))
    & (F.col("TblBalExtr.SUBJ_AREA_NM") == F.col("ProvCtLoad.SUBJ_AREA_NM")),
    how="left"
).orderBy("TblBalExtr.BAL_DT")

# We must replicate the row-by-row stage-variable logic exactly.
# Collect all rows (note: no function definitions allowed), process in driver.
rows = df_transform_joined.collect()

final_schema = StructType([
    StructField("TRGT_SYS_CD", StringType(), True),
    StructField("SUBJ_AREA_NM", StringType(), True),
    StructField("LAST_BAL_DT", StringType(), True),
    StructField("TLRNC_CD", StringType(), True),
    StructField("PREV_ISSUE_CRCTD_IN", StringType(), True),
    StructField("TRGT_RUN_CYC_NO", StringType(), True),
    StructField("CRT_DTM", StringType(), True),
    StructField("LAST_UPDT_DTM", StringType(), True),
    StructField("USER_ID", StringType(), True)
])

new_rows = []
current_key = None

# Stage variable initial states
svPrevCount = 0
svTempTlrncCd = ""
svTempRslvIn = "Y"
svTempRslvIncd = ""

for row in rows:
    # Identify the group by TRGT_SYS_CD, SUBJ_AREA_NM
    row_trgt = row["TblBalExtr.TRGT_SYS_CD"]
    row_subj = row["TblBalExtr.SUBJ_AREA_NM"]
    row_key = (row_trgt, row_subj)

    # If new group, reset stage variables
    if row_key != current_key:
        current_key = row_key
        svPrevCount = 0
        svTempTlrncCd = ""
        svTempRslvIn = "Y"
        svTempRslvIncd = ""

    # Current row's tolerance code from input
    svCurrTlrncCd = row["TblBalExtr.ROW_CT_TLRNC_CD"]
    # Increase count
    svCurrCount = svPrevCount + 1

    # Evaluate svTempTlrncCd
    bal_dt_str = ""
    if row["TblBalExtr.BAL_DT"] is not None:
        bal_dt_str = str(row["TblBalExtr.BAL_DT"])[:10]  # mimic [1, 10] in DataStage (approx)

    if bal_dt_str != CurrDate:
        newSvTempTlrncCd = "NEXT ROW"
    else:
        if svCurrTlrncCd == "BAL" and svTempTlrncCd != "OUT" and svTempTlrncCd != "IN":
            newSvTempTlrncCd = "BAL"
        elif svCurrTlrncCd == "BAL" and svTempTlrncCd == "IN":
            newSvTempTlrncCd = "IN"
        elif svCurrTlrncCd == "IN" and svTempTlrncCd != "OUT":
            newSvTempTlrncCd = "IN"
        else:
            newSvTempTlrncCd = "OUT"

    # Evaluate svTempRslvIn
    if bal_dt_str == CurrDate:
        newSvTempRslvIn = svTempRslvIn
    else:
        if svCurrTlrncCd == "BAL" and svTempRslvIncd != "OUT":
            newSvTempRslvIn = "Y"
        elif svCurrTlrncCd == "IN" and svTempRslvIncd != "OUT":
            newSvTempRslvIn = "Y"
        else:
            newSvTempRslvIn = "N"

    # Evaluate svTempRslvIncd
    if bal_dt_str == CurrDate:
        newSvTempRslvIncd = "NA"
    else:
        if svCurrTlrncCd == "BAL" and svTempRslvIncd not in ["OUT","IN"]:
            newSvTempRslvIncd = "BAL"
        elif svCurrTlrncCd == "BAL" and svTempRslvIncd == "IN":
            newSvTempRslvIncd = "IN"
        elif svCurrTlrncCd == "IN" and svTempRslvIncd != "OUT":
            newSvTempRslvIncd = "IN"
        else:
            newSvTempRslvIncd = "OUT"

    # Update stage variables
    svTempTlrncCd = newSvTempTlrncCd
    svTempRslvIn = newSvTempRslvIn
    svTempRslvIncd = newSvTempRslvIncd

    svPrevCount = svCurrCount

    # Constraint: output only if svPrevCount = ProvCtLoad.ROW_COUNT
    # (i.e., we only pass the row where the final svPrevCount matches ROW_COUNT)
    row_count_val = row["ProvCtLoad.ROW_COUNT"]
    if row_count_val is not None and svPrevCount == row_count_val:
        # Build final output columns
        TRGT_SYS_CD = Target
        SUBJ_AREA_NM = Subject
        LAST_BAL_DT = CurrDate
        TLRNC_CD = svTempTlrncCd
        PREV_ISSUE_CRCTD_IN = svTempRslvIn
        TRGT_RUN_CYC_NO = ExtrRunCycle
        CRT_DTM = CurrDate
        LAST_UPDT_DTM = CurrDate
        USER_ID = UWSAcct

        new_rows.append((
            TRGT_SYS_CD,
            SUBJ_AREA_NM,
            LAST_BAL_DT,
            TLRNC_CD,
            PREV_ISSUE_CRCTD_IN,
            TRGT_RUN_CYC_NO,
            CRT_DTM,
            LAST_UPDT_DTM,
            USER_ID
        ))

df_enriched = spark.createDataFrame(new_rows, final_schema)

# Ensure column order and apply rpad if needed (PREV_ISSUE_CRCTD_IN is char(1))
df_enriched = df_enriched.select(
    F.col("TRGT_SYS_CD"),
    F.col("SUBJ_AREA_NM"),
    F.col("LAST_BAL_DT"),
    F.col("TLRNC_CD"),
    F.rpad(F.col("PREV_ISSUE_CRCTD_IN"), 1, " ").alias("PREV_ISSUE_CRCTD_IN"),
    F.col("TRGT_RUN_CYC_NO"),
    F.col("CRT_DTM"),
    F.col("LAST_UPDT_DTM"),
    F.col("USER_ID")
)

# ---------------------------------------------------------------------
# Stage: SUBJ_AREA_BAL_SUM (CODBCStage)
#   Perform an upsert (merge) into #$UWSOwner#.SUBJ_AREA_BAL_SUM
# ---------------------------------------------------------------------

# 1) Drop temp table if exists
execute_dml(
    f"DROP TABLE IF EXISTS STAGING.UwsProvSubjAreaBalSumLoad_SUBJ_AREA_BAL_SUM_temp",
    jdbc_url,
    jdbc_props
)

# 2) Write df_enriched to a physical staging table
(
    df_enriched
    .write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", "STAGING.UwsProvSubjAreaBalSumLoad_SUBJ_AREA_BAL_SUM_temp")
    .mode("overwrite")
    .save()
)

# 3) Merge into target table
merge_sql = f"""
MERGE {UWSOwner}.SUBJ_AREA_BAL_SUM AS T
USING STAGING.UwsProvSubjAreaBalSumLoad_SUBJ_AREA_BAL_SUM_temp AS S
ON (
    T.TRGT_SYS_CD = S.TRGT_SYS_CD
    AND T.SUBJ_AREA_NM = S.SUBJ_AREA_NM
)
WHEN MATCHED THEN UPDATE SET
    T.LAST_BAL_DT = S.LAST_BAL_DT,
    T.TLRNC_CD = S.TLRNC_CD,
    T.PREV_ISSUE_CRCTD_IN = S.PREV_ISSUE_CRCTD_IN,
    T.TRGT_RUN_CYC_NO = S.TRGT_RUN_CYC_NO,
    T.CRT_DTM = S.CRT_DTM,
    T.LAST_UPDT_DTM = S.LAST_UPDT_DTM,
    T.USER_ID = S.USER_ID
WHEN NOT MATCHED THEN INSERT
(
    TRGT_SYS_CD,
    SUBJ_AREA_NM,
    LAST_BAL_DT,
    TLRNC_CD,
    PREV_ISSUE_CRCTD_IN,
    TRGT_RUN_CYC_NO,
    CRT_DTM,
    LAST_UPDT_DTM,
    USER_ID
)
VALUES
(
    S.TRGT_SYS_CD,
    S.SUBJ_AREA_NM,
    S.LAST_BAL_DT,
    S.TLRNC_CD,
    S.PREV_ISSUE_CRCTD_IN,
    S.TRGT_RUN_CYC_NO,
    S.CRT_DTM,
    S.LAST_UPDT_DTM,
    S.USER_ID
);
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)