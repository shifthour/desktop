# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 10/24/07 09:59:24 Batch  14542_35967 PROMOTE bckcetl ids20 dsadm bls for on
# MAGIC ^1_1 10/24/07 09:54:15 Batch  14542_35658 INIT bckcett testIDS30 dsadm bls for on
# MAGIC ^1_2 10/17/07 13:59:14 Batch  14535_50357 PROMOTE bckcett testIDS30 u03651 steffy
# MAGIC ^1_2 10/17/07 13:54:06 Batch  14535_50077 INIT bckcett devlIDS30 u03651 steffy
# MAGIC ^1_1 10/16/07 09:44:13 Batch  14534_35065 INIT bckcett devlIDS30 u03651 steffy
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:   FctsCapBalCntl
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   
# MAGIC                 The Job loads the SUBJECT_AREA_BAL_SUM table after the balancing control job is done for a given Subject Area              
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               04/20/2007          3264                              Originally Programmed                           devlIDS30                  Steph Goddard           09/19/2007


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import Window
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

extract_query_tblBalExtr = f"SELECT TBL_BAL_SUM.TRGT_SYS_CD, TBL_BAL_SUM.SUBJ_AREA_NM, TBL_BAL_SUM.BAL_DT, TBL_BAL_SUM.ROW_CT_TLRNC_CD FROM {UWSOwner}.TBL_BAL_SUM TBL_BAL_SUM WHERE TBL_BAL_SUM.TRGT_SYS_CD = '{Target}' AND TBL_BAL_SUM.SUBJ_AREA_NM = '{Subject}' ORDER BY TBL_BAL_SUM.BAL_DT ASC"
df_TblBalExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_tblBalExtr)
    .load()
)

extract_query_capCt = f"SELECT TBL_BAL_SUM.TRGT_SYS_CD, TBL_BAL_SUM.SUBJ_AREA_NM, COUNT(*) AS ROW_COUNT FROM {UWSOwner}.TBL_BAL_SUM TBL_BAL_SUM WHERE TBL_BAL_SUM.TRGT_SYS_CD = '{Target}' AND TBL_BAL_SUM.SUBJ_AREA_NM = '{Subject}' GROUP BY TBL_BAL_SUM.TRGT_SYS_CD, TBL_BAL_SUM.SUBJ_AREA_NM"
df_CapCt = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_capCt)
    .load()
)

df_CapCt = df_CapCt.dropDuplicates(["TRGT_SYS_CD","SUBJ_AREA_NM"])

df_join = (
    df_TblBalExtr.alias("TblBalExtr")
    .join(
        df_CapCt.alias("CapCtLoad"),
        (F.col("TblBalExtr.TRGT_SYS_CD") == F.col("CapCtLoad.TRGT_SYS_CD"))
        & (F.col("TblBalExtr.SUBJ_AREA_NM") == F.col("CapCtLoad.SUBJ_AREA_NM")),
        "left"
    )
    .select(
        F.col("TblBalExtr.TRGT_SYS_CD"),
        F.col("TblBalExtr.SUBJ_AREA_NM"),
        F.col("TblBalExtr.BAL_DT"),
        F.col("TblBalExtr.ROW_CT_TLRNC_CD"),
        F.col("CapCtLoad.ROW_COUNT")
    )
)

df_temp = df_join.orderBy("BAL_DT").collect()

final_data = []
svPrevCount = 0
svTempTlrncCd = ""
svTempRslvIn = "Y"
svTempRslvIncd = ""

for row in df_temp:
    svCurrTlrncCd = row["ROW_CT_TLRNC_CD"] if row["ROW_CT_TLRNC_CD"] else ""
    svCurrCount = svPrevCount + 1
    dt_str = str(row["BAL_DT"])[0:10] if row["BAL_DT"] else ""
    if dt_str != CurrDate:
        new_svTempTlrncCd = "NEXT ROW"
    else:
        if svCurrTlrncCd == "BAL" and svTempTlrncCd not in ["OUT","IN"]:
            new_svTempTlrncCd = "BAL"
        elif svCurrTlrncCd == "BAL" and svTempTlrncCd == "IN":
            new_svTempTlrncCd = "IN"
        elif svCurrTlrncCd == "IN" and svTempTlrncCd != "OUT":
            new_svTempTlrncCd = "IN"
        else:
            new_svTempTlrncCd = "OUT"
    if dt_str == CurrDate:
        new_svTempRslvIn = svTempRslvIn
    else:
        if svCurrTlrncCd == "BAL" and svTempRslvIncd != "OUT":
            new_svTempRslvIn = "Y"
        elif svCurrTlrncCd == "IN" and svTempRslvIncd != "OUT":
            new_svTempRslvIn = "Y"
        else:
            new_svTempRslvIn = "N"
    if dt_str == CurrDate:
        new_svTempRslvIncd = "NA"
    else:
        if svCurrTlrncCd == "BAL" and svTempRslvIncd not in ["OUT","IN"]:
            new_svTempRslvIncd = "BAL"
        elif svCurrTlrncCd == "BAL" and svTempRslvIncd == "IN":
            new_svTempRslvIncd = "IN"
        elif svCurrTlrncCd == "IN" and svTempRslvIncd != "OUT":
            new_svTempRslvIncd = "IN"
        else:
            new_svTempRslvIncd = "OUT"
    svTempTlrncCd = new_svTempTlrncCd
    svTempRslvIn = new_svTempRslvIn
    svTempRslvIncd = new_svTempRslvIncd
    svPrevCount = svCurrCount
    if row["ROW_COUNT"] == svPrevCount:
        final_data.append((
            Target,
            Subject,
            CurrDate,
            svTempTlrncCd,
            svTempRslvIn,
            ExtrRunCycle,
            CurrDate,
            CurrDate,
            UWSAcct
        ))

final_schema = [
    "TRGT_SYS_CD",
    "SUBJ_AREA_NM",
    "LAST_BAL_DT",
    "TLRNC_CD",
    "PREV_ISSUE_CRCTD_IN",
    "TRGT_RUN_CYC_NO",
    "CRT_DTM",
    "LAST_UPDT_DTM",
    "USER_ID"
]
final_df = spark.createDataFrame(final_data, schema=final_schema)

final_df = final_df.select(
    F.rpad(F.col("TRGT_SYS_CD"), 10, " ").alias("TRGT_SYS_CD"),
    F.rpad(F.col("SUBJ_AREA_NM"), 10, " ").alias("SUBJ_AREA_NM"),
    F.col("LAST_BAL_DT"),
    F.rpad(F.col("TLRNC_CD"), 10, " ").alias("TLRNC_CD"),
    F.rpad(F.col("PREV_ISSUE_CRCTD_IN"), 1, " ").alias("PREV_ISSUE_CRCTD_IN"),
    F.col("TRGT_RUN_CYC_NO"),
    F.col("CRT_DTM"),
    F.col("LAST_UPDT_DTM"),
    F.rpad(F.col("USER_ID"), 10, " ").alias("USER_ID")
)

execute_dml(f"DROP TABLE IF EXISTS STAGING.UwsCapSubjAreaBalSumLoad_SUBJ_AREA_BAL_SUM_temp", jdbc_url, jdbc_props)

(
    final_df.write.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", "STAGING.UwsCapSubjAreaBalSumLoad_SUBJ_AREA_BAL_SUM_temp")
    .mode("append")
    .save()
)

merge_sql = f"""
MERGE INTO {UWSOwner}.SUBJ_AREA_BAL_SUM AS T
USING STAGING.UwsCapSubjAreaBalSumLoad_SUBJ_AREA_BAL_SUM_temp AS S
ON (T.TRGT_SYS_CD = S.TRGT_SYS_CD AND T.SUBJ_AREA_NM = S.SUBJ_AREA_NM)
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
  INSERT (TRGT_SYS_CD, SUBJ_AREA_NM, LAST_BAL_DT, TLRNC_CD, PREV_ISSUE_CRCTD_IN, TRGT_RUN_CYC_NO, CRT_DTM, LAST_UPDT_DTM, USER_ID)
  VALUES (S.TRGT_SYS_CD, S.SUBJ_AREA_NM, S.LAST_BAL_DT, S.TLRNC_CD, S.PREV_ISSUE_CRCTD_IN, S.TRGT_RUN_CYC_NO, S.CRT_DTM, S.LAST_UPDT_DTM, S.USER_ID);
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)