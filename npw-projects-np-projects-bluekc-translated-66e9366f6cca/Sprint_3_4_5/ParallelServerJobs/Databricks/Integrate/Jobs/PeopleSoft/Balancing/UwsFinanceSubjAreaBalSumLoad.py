# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_2 11/07/07 14:46:06 Batch  14556_53195 PROMOTE bckcetl ids20 dsadm bls for rt
# MAGIC ^1_2 11/07/07 14:33:10 Batch  14556_52396 INIT bckcett testIDScur dsadm bls for rt
# MAGIC ^1_3 11/06/07 10:51:39 Batch  14555_39105 PROMOTE bckcett testIDScur u06640 Ralph
# MAGIC ^1_3 11/06/07 10:33:20 Batch  14555_38005 PROMOTE bckcett testIDScur u06640 Ralph
# MAGIC ^1_3 11/06/07 10:32:05 Batch  14555_37929 INIT bckcett devlIDS30 u06640 Ralph
# MAGIC ^1_2 11/06/07 09:47:12 Batch  14555_35238 INIT bckcett devlIDS30 u06640 Ralph
# MAGIC ^1_1 10/08/07 16:58:04 Batch  14526_61088 INIT bckcett devlIDS30 u08717 Brent
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:   FctsPaymtSumBalCntl, PSIJrnlEntryBalCntl and PSIFnclLobBalCntl
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   
# MAGIC                 The Job loads the SUBJECT_AREA_BAL_SUM table after the balancing control job is done for a given Subject Area              
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               05/10/2007          3264                              Originally Programmed                           devlIDS30                 Steph Goddard            09/15/2007


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


UWSOwner = get_widget_value('UWSOwner','')
uws_secret_name = get_widget_value('uws_secret_name','')
Target = get_widget_value('Target','')
Subject = get_widget_value('Subject','')
CurrDate = get_widget_value('CurrDate','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
UWSAcct = get_widget_value('UWSAcct','')

jdbc_url, jdbc_props = get_db_config(uws_secret_name)

extract_query_1 = (
    f"SELECT TBL_BAL_SUM.TRGT_SYS_CD, "
    f"TBL_BAL_SUM.SUBJ_AREA_NM, "
    f"TBL_BAL_SUM.BAL_DT, "
    f"TBL_BAL_SUM.ROW_CT_TLRNC_CD "
    f"FROM {UWSOwner}.TBL_BAL_SUM TBL_BAL_SUM "
    f"WHERE TBL_BAL_SUM.TRGT_SYS_CD = '{Target}' "
    f"AND TBL_BAL_SUM.SUBJ_AREA_NM = '{Subject}' "
    f"ORDER BY TBL_BAL_SUM.BAL_DT ASC"
)

df_TblBalExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_1)
    .load()
)

extract_query_2 = (
    f"SELECT TBL_BAL_SUM.TRGT_SYS_CD, "
    f"TBL_BAL_SUM.SUBJ_AREA_NM, "
    f"COUNT(*) AS ROW_COUNT "
    f"FROM {UWSOwner}.TBL_BAL_SUM TBL_BAL_SUM "
    f"WHERE TBL_BAL_SUM.TRGT_SYS_CD = '{Target}' "
    f"AND TBL_BAL_SUM.SUBJ_AREA_NM = '{Subject}' "
    f"GROUP BY TBL_BAL_SUM.TRGT_SYS_CD, TBL_BAL_SUM.SUBJ_AREA_NM"
)

df_FncCt = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_2)
    .load()
)

df_FncCtLoad = dedup_sort(df_FncCt, ["TRGT_SYS_CD","SUBJ_AREA_NM"], [])

df_TblBalExtr_sorted = df_TblBalExtr.orderBy(F.col("BAL_DT").asc()).coalesce(1)

df_joined = df_TblBalExtr_sorted.alias("TblBalExtr").join(
    df_FncCtLoad.alias("FncCtLoad"),
    on=["TRGT_SYS_CD","SUBJ_AREA_NM"],
    how="left"
)

rdd_joined = df_joined.rdd.mapPartitions(
    lambda iter: (
        (
            Target,
            Subject,
            CurrDate,
            ExtrRunCycle,
            row["TblBalExtr.TRGT_SYS_CD"],
            row["TblBalExtr.SUBJ_AREA_NM"],
            row["TblBalExtr.BAL_DT"],
            row["TblBalExtr.ROW_CT_TLRNC_CD"],
            row["FncCtLoad.ROW_COUNT"] if "FncCtLoad.ROW_COUNT" in row and row["FncCtLoad.ROW_COUNT"] is not None else None
        )
        for row in iter
    )
)

def process_partition(part):
    svPrevCount = 0
    svTempTlrncCd = ""
    svTempRslvIn = "Y"
    svTempRslvIncd = ""
    for (pTarget, pSubject, pCurrDate, pExtrRunCycle, trgtSysCd, subjAreaNm, balDt, rowCtTlrnc, rowCount) in part:
        svCurrTlrncCd = rowCtTlrnc
        svCurrCount = svPrevCount + 1
        oldTlrnc = svTempTlrncCd
        if balDt and balDt[:10] != pCurrDate:
            newTlrnc = "NEXT ROW"
        else:
            if svCurrTlrncCd == "BAL" and oldTlrnc not in ("OUT","IN"):
                newTlrnc = "BAL"
            elif svCurrTlrncCd == "BAL" and oldTlrnc == "IN":
                newTlrnc = "IN"
            elif svCurrTlrncCd == "IN" and oldTlrnc != "OUT":
                newTlrnc = "IN"
            else:
                newTlrnc = "OUT"
        newRslvIn = svTempRslvIn
        oldRslvIncd = svTempRslvIncd
        if balDt and balDt[:10] == pCurrDate:
            newRslvIn = svTempRslvIn
        else:
            if svCurrTlrncCd == "BAL" and oldRslvIncd != "OUT":
                newRslvIn = "Y"
            elif svCurrTlrncCd == "IN" and oldRslvIncd != "OUT":
                newRslvIn = "Y"
            else:
                newRslvIn = "N"
        if balDt and balDt[:10] == pCurrDate:
            newRslvIncd = "NA"
        else:
            if svCurrTlrncCd == "BAL" and oldRslvIncd not in ("OUT","IN"):
                newRslvIncd = "BAL"
            elif svCurrTlrncCd == "BAL" and oldRslvIncd == "IN":
                newRslvIncd = "IN"
            elif svCurrTlrncCd == "IN" and oldRslvIncd != "OUT":
                newRslvIncd = "IN"
            else:
                newRslvIncd = "OUT"
        svPrevCount = svCurrCount
        svTempTlrncCd = newTlrnc
        svTempRslvIn = newRslvIn
        svTempRslvIncd = newRslvIncd
        if rowCount is not None and svPrevCount == rowCount:
            yield (
                F.lit(pTarget),
                F.lit(pSubject),
                F.lit(pCurrDate),
                F.lit(newTlrnc),
                F.lit(newRslvIn),
                F.lit(pExtrRunCycle),
                F.lit(pCurrDate),
                F.lit(pCurrDate),
                F.lit(UWSAcct)
            )

rdd_processed = rdd_joined.mapPartitions(process_partition)

df_enriched = rdd_processed.toDF([
    "TRGT_SYS_CD",
    "SUBJ_AREA_NM",
    "LAST_BAL_DT",
    "TLRNC_CD",
    "PREV_ISSUE_CRCTD_IN",
    "TRGT_RUN_CYC_NO",
    "CRT_DTM",
    "LAST_UPDT_DTM",
    "USER_ID"
])

df_enriched = df_enriched.select(
    F.rpad("TRGT_SYS_CD", <...>, " ").alias("TRGT_SYS_CD"),
    F.rpad("SUBJ_AREA_NM", <...>, " ").alias("SUBJ_AREA_NM"),
    "LAST_BAL_DT",
    F.rpad("TLRNC_CD", <...>, " ").alias("TLRNC_CD"),
    F.rpad("PREV_ISSUE_CRCTD_IN", 1, " ").alias("PREV_ISSUE_CRCTD_IN"),
    "TRGT_RUN_CYC_NO",
    "CRT_DTM",
    "LAST_UPDT_DTM",
    F.rpad("USER_ID", <...>, " ").alias("USER_ID")
)

drop_temp_sql = "DROP TABLE IF EXISTS STAGING.UwsFinanceSubjAreaBalSumLoad_SUBJ_AREA_BAL_SUM_temp"
execute_dml(drop_temp_sql, jdbc_url, jdbc_props)

df_enriched.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.UwsFinanceSubjAreaBalSumLoad_SUBJ_AREA_BAL_SUM_temp") \
    .mode("overwrite") \
    .save()

merge_sql = (
    f"MERGE {UWSOwner}.SUBJ_AREA_BAL_SUM AS T "
    f"USING STAGING.UwsFinanceSubjAreaBalSumLoad_SUBJ_AREA_BAL_SUM_temp AS S "
    f"ON T.TRGT_SYS_CD = S.TRGT_SYS_CD "
    f"AND T.SUBJ_AREA_NM = S.SUBJ_AREA_NM "
    f"WHEN MATCHED THEN UPDATE SET "
    f"T.LAST_BAL_DT = S.LAST_BAL_DT, "
    f"T.TLRNC_CD = S.TLRNC_CD, "
    f"T.PREV_ISSUE_CRCTD_IN = S.PREV_ISSUE_CRCTD_IN, "
    f"T.TRGT_RUN_CYC_NO = S.TRGT_RUN_CYC_NO, "
    f"T.CRT_DTM = S.CRT_DTM, "
    f"T.LAST_UPDT_DTM = S.LAST_UPDT_DTM, "
    f"T.USER_ID = S.USER_ID "
    f"WHEN NOT MATCHED THEN INSERT "
    f"(TRGT_SYS_CD, SUBJ_AREA_NM, LAST_BAL_DT, TLRNC_CD, PREV_ISSUE_CRCTD_IN, TRGT_RUN_CYC_NO, CRT_DTM, LAST_UPDT_DTM, USER_ID) "
    f"VALUES (S.TRGT_SYS_CD, S.SUBJ_AREA_NM, S.LAST_BAL_DT, S.TLRNC_CD, S.PREV_ISSUE_CRCTD_IN, S.TRGT_RUN_CYC_NO, S.CRT_DTM, S.LAST_UPDT_DTM, S.USER_ID) "
    f"WHEN NOT MATCHED BY SOURCE THEN DELETE;"
)
execute_dml(merge_sql, jdbc_url, jdbc_props)