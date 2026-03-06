# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 11/28/07 11:03:55 Batch  14577_39840 PROMOTE bckcetl ids20 dsadm bls forhs
# MAGIC ^1_1 11/28/07 10:51:03 Batch  14577_39066 INIT bckcett testIDS30 dsadm bls for hs
# MAGIC ^1_1 11/27/07 14:16:37 Batch  14576_51408 PROMOTE bckcett testIDS30 u03651 steph for Hugh
# MAGIC ^1_1 11/27/07 14:06:57 Batch  14576_50821 INIT bckcett devlIDS30 u03651 steffy
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:   FctsAplBalCntl
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   
# MAGIC                 The Job loads the SUBJECT_AREA_BAL_SUM table after the balancing control job is done for a given Subject Area              
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Bhoomi Dasari                 10/10/2007          3028                              Originally Programmed                           devlIDS30                   Steph Goddard            10/18/2007


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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


UWSOwner = get_widget_value("UWSOwner","")
uws_secret_name = get_widget_value("uws_secret_name","")
Target = get_widget_value("Target","")
Subject = get_widget_value("Subject","")
CurrDate = get_widget_value("CurrDate","")
ExtrRunCycle = get_widget_value("ExtrRunCycle","")
UWSAcct = get_widget_value("UWSAcct","")

jdbc_url, jdbc_props = get_db_config(uws_secret_name)

df_TblBalExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
SELECT
  TBL_BAL_SUM.TRGT_SYS_CD,
  TBL_BAL_SUM.SUBJ_AREA_NM,
  TBL_BAL_SUM.BAL_DT,
  TBL_BAL_SUM.ROW_CT_TLRNC_CD
FROM {UWSOwner}.TBL_BAL_SUM TBL_BAL_SUM
WHERE
  TBL_BAL_SUM.TRGT_SYS_CD = '{Target}'
  AND TBL_BAL_SUM.SUBJ_AREA_NM = '{Subject}'
ORDER BY
  TBL_BAL_SUM.BAL_DT ASC
"""
    )
    .load()
)

df_hf_mbr_tbl_bal_lkup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
SELECT
  TBL_BAL_SUM.TRGT_SYS_CD,
  TBL_BAL_SUM.SUBJ_AREA_NM,
  COUNT(*) AS ROW_COUNT
FROM {UWSOwner}.TBL_BAL_SUM TBL_BAL_SUM
WHERE
  TBL_BAL_SUM.TRGT_SYS_CD = '{Target}'
  AND TBL_BAL_SUM.SUBJ_AREA_NM = '{Subject}'
GROUP BY
  TBL_BAL_SUM.TRGT_SYS_CD,
  TBL_BAL_SUM.SUBJ_AREA_NM
"""
    )
    .load()
)

df_hf_mbr_tbl_bal_lkup = df_hf_mbr_tbl_bal_lkup.dropDuplicates(["TRGT_SYS_CD","SUBJ_AREA_NM"])

df_join = df_TblBalExtr.alias("TblBalExtr").join(
    df_hf_mbr_tbl_bal_lkup.alias("DSLink20"),
    (
        (F.col("TblBalExtr.TRGT_SYS_CD") == F.col("DSLink20.TRGT_SYS_CD"))
        & (F.col("TblBalExtr.SUBJ_AREA_NM") == F.col("DSLink20.SUBJ_AREA_NM"))
    ),
    "left"
)

df_rows = df_join.orderBy("TblBalExtr.BAL_DT").collect()

svTempTlrncCd_old = ""
svTempRslvIn_old = "Y"
svTempRslvIncd_old = ""
svPrevCount_old = 0

newRows = []
for row in df_rows:
    svCurrTlrncCd = row["ROW_CT_TLRNC_CD"]
    new_svCurrCount = svPrevCount_old + 1
    bal_dt_10 = str(row["BAL_DT"])[0:10] if row["BAL_DT"] is not None else ""

    if bal_dt_10 != CurrDate:
        new_svTempTlrncCd = "NEXT ROW"
    else:
        if svCurrTlrncCd == "BAL" and svTempTlrncCd_old not in ("OUT","IN"):
            new_svTempTlrncCd = "BAL"
        elif svCurrTlrncCd == "BAL" and svTempTlrncCd_old == "IN":
            new_svTempTlrncCd = "IN"
        elif svCurrTlrncCd == "IN" and svTempTlrncCd_old != "OUT":
            new_svTempTlrncCd = "IN"
        else:
            new_svTempTlrncCd = "OUT"

    if bal_dt_10 == CurrDate:
        new_svTempRslvIn = svTempRslvIn_old
    else:
        if svCurrTlrncCd == "BAL" and svTempRslvIncd_old != "OUT":
            new_svTempRslvIn = "Y"
        elif svCurrTlrncCd == "IN" and svTempRslvIncd_old != "OUT":
            new_svTempRslvIn = "Y"
        else:
            new_svTempRslvIn = "N"

    if bal_dt_10 == CurrDate:
        new_svTempRslvIncd = "NA"
    else:
        if svCurrTlrncCd == "BAL" and svTempRslvIncd_old not in ("OUT","IN"):
            new_svTempRslvIncd = "BAL"
        elif svCurrTlrncCd == "BAL" and svTempRslvIncd_old == "IN":
            new_svTempRslvIncd = "IN"
        elif svCurrTlrncCd == "IN" and svTempRslvIncd_old != "OUT":
            new_svTempRslvIncd = "IN"
        else:
            new_svTempRslvIncd = "OUT"

    new_svPrevCount = new_svCurrCount

    if row["ROW_COUNT"] is not None and new_svPrevCount == row["ROW_COUNT"]:
        final_TRGT_SYS_CD = Target
        final_SUBJ_AREA_NM = Subject
        final_LAST_BAL_DT = CurrDate
        final_TLRNC_CD = new_svTempTlrncCd
        final_PREV_ISSUE_CRCTD_IN = new_svTempRslvIn
        final_TRGT_RUN_CYC_NO = ExtrRunCycle
        final_CRT_DTM = CurrDate
        final_LAST_UPDT_DTM = CurrDate
        final_USER_ID = UWSAcct
        newRows.append((
            final_TRGT_SYS_CD,
            final_SUBJ_AREA_NM,
            final_LAST_BAL_DT,
            final_TLRNC_CD,
            final_PREV_ISSUE_CRCTD_IN,
            final_TRGT_RUN_CYC_NO,
            final_CRT_DTM,
            final_LAST_UPDT_DTM,
            final_USER_ID
        ))

    svTempTlrncCd_old = new_svTempTlrncCd
    svTempRslvIn_old = new_svTempRslvIn
    svTempRslvIncd_old = new_svTempRslvIncd
    svPrevCount_old = new_svPrevCount

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

df_final = spark.createDataFrame(newRows, schema=final_schema)

execute_dml(
    f"DROP TABLE IF EXISTS STAGING.UwsAplSubjAreaBalSumLoad_SUBJ_AREA_BAL_SUM_temp",
    jdbc_url,
    jdbc_props
)

df_final.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.UwsAplSubjAreaBalSumLoad_SUBJ_AREA_BAL_SUM_temp") \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO {UWSOwner}.SUBJ_AREA_BAL_SUM AS T
USING STAGING.UwsAplSubjAreaBalSumLoad_SUBJ_AREA_BAL_SUM_temp AS S
ON
  T.TRGT_SYS_CD = S.TRGT_SYS_CD
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
  INSERT
    (TRGT_SYS_CD, SUBJ_AREA_NM, LAST_BAL_DT, TLRNC_CD, PREV_ISSUE_CRCTD_IN, TRGT_RUN_CYC_NO, CRT_DTM, LAST_UPDT_DTM, USER_ID)
  VALUES
    (S.TRGT_SYS_CD, S.SUBJ_AREA_NM, S.LAST_BAL_DT, S.TLRNC_CD, S.PREV_ISSUE_CRCTD_IN, S.TRGT_RUN_CYC_NO, S.CRT_DTM, S.LAST_UPDT_DTM, S.USER_ID)
;
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)