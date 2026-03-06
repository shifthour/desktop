# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 01/29/08 14:29:13 Batch  14639_52158 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 01/09/08 13:32:13 Batch  14619_48738 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/24/07 09:35:50 Batch  14573_34558 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_2 11/01/07 15:16:03 Batch  14550_54982 PROMOTE bckcetl ids20 dsadm rc for brent
# MAGIC ^1_2 11/01/07 15:05:36 Batch  14550_54348 INIT bckcett testIDS30 dsadm rc for brent 
# MAGIC ^1_2 10/31/07 10:30:33 Batch  14549_37838 PROMOTE bckcett testIDS30 u08717 Brent
# MAGIC ^1_2 10/31/07 10:24:16 Batch  14549_37464 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_1 10/24/07 15:30:15 Batch  14542_55818 INIT bckcett devlIDS30 u10157 sa - DRG project - moving to ids_current devlopment for coding changes
# MAGIC ^1_1 10/10/07 07:31:23 Batch  14528_27086 INIT bckcett devlIDS30 u08717 Brent
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:   IdsClmBalCntl
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   
# MAGIC                 The Job loads the SUBJECT_AREA_BAL_SUM table after the balancing control job is done for a given Subject Area              
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Bhoomi Dasari                 09/11/2007          3264                              Originally Programmed                           devlIDS30                    Steph Goddard            09/28/2007


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


UWSOwner = get_widget_value('UWSOwner','')
uws_secret_name = get_widget_value('uws_secret_name','')
Target = get_widget_value('Target','')
Subject = get_widget_value('Subject','')
CurrDate = get_widget_value('CurrDate','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
UWSAcct = get_widget_value('UWSAcct','')

jdbc_url, jdbc_props = get_db_config(uws_secret_name)

extract_query_TblBalExtr = (
    f"SELECT TBL_BAL_SUM.TRGT_SYS_CD, "
    f"TBL_BAL_SUM.SUBJ_AREA_NM, "
    f"TBL_BAL_SUM.BAL_DT, "
    f"TBL_BAL_SUM.CLMN_SUM_TLRNC_CD, "
    f"TBL_BAL_SUM.ROW_TO_ROW_TLRNC_CD "
    f"FROM {UWSOwner}.TBL_BAL_SUM TBL_BAL_SUM "
    f"WHERE TBL_BAL_SUM.TRGT_SYS_CD = '{Target}' "
    f"AND TBL_BAL_SUM.SUBJ_AREA_NM = '{Subject}' "
    f"ORDER BY TBL_BAL_SUM.BAL_DT ASC"
)

df_TblBalExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_TblBalExtr)
    .load()
)

extract_query_DSLink18 = (
    f"SELECT TBL_BAL_SUM.TRGT_SYS_CD, "
    f"TBL_BAL_SUM.SUBJ_AREA_NM, "
    f"COUNT(*) AS ROW_COUNT "
    f"FROM {UWSOwner}.TBL_BAL_SUM TBL_BAL_SUM "
    f"WHERE TBL_BAL_SUM.TRGT_SYS_CD = '{Target}' "
    f"AND TBL_BAL_SUM.SUBJ_AREA_NM = '{Subject}' "
    f"GROUP BY TBL_BAL_SUM.TRGT_SYS_CD, TBL_BAL_SUM.SUBJ_AREA_NM"
)

df_DSLink18 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_DSLink18)
    .load()
)

df_DSLink20 = dedup_sort(
    df_DSLink18,
    partition_cols=["TRGT_SYS_CD", "SUBJ_AREA_NM"],
    sort_cols=[]
)

df_Joined = df_TblBalExtr.alias("TblBalExtr").join(
    df_DSLink20.alias("DSLink20"),
    (
        (F.col("TblBalExtr.TRGT_SYS_CD") == F.col("DSLink20.TRGT_SYS_CD"))
        & (F.col("TblBalExtr.SUBJ_AREA_NM") == F.col("DSLink20.SUBJ_AREA_NM"))
    ),
    "left"
)

df_Ordered = df_Joined.select(
    F.col("TblBalExtr.TRGT_SYS_CD").alias("TblBalExtr_TRGT_SYS_CD"),
    F.col("TblBalExtr.SUBJ_AREA_NM").alias("TblBalExtr_SUBJ_AREA_NM"),
    F.col("TblBalExtr.BAL_DT").alias("TblBalExtr_BAL_DT"),
    F.col("TblBalExtr.CLMN_SUM_TLRNC_CD").alias("TblBalExtr_CLMN_SUM_TLRNC_CD"),
    F.col("TblBalExtr.ROW_TO_ROW_TLRNC_CD").alias("TblBalExtr_ROW_TO_ROW_TLRNC_CD"),
    F.col("DSLink20.ROW_COUNT").alias("DSLink20_ROW_COUNT")
).orderBy("TblBalExtr_BAL_DT").coalesce(1)

schema_final = T.StructType([
    T.StructField("TRGT_SYS_CD", T.StringType(), True),
    T.StructField("SUBJ_AREA_NM", T.StringType(), True),
    T.StructField("LAST_BAL_DT", T.StringType(), True),
    T.StructField("TLRNC_CD", T.StringType(), True),
    T.StructField("PREV_ISSUE_CRCTD_IN", T.StringType(), True),
    T.StructField("TRGT_RUN_CYC_NO", T.StringType(), True),
    T.StructField("CRT_DTM", T.StringType(), True),
    T.StructField("LAST_UPDT_DTM", T.StringType(), True),
    T.StructField("USER_ID", T.StringType(), True)
])

rdd_final = df_Ordered.rdd.mapPartitions(
    lambda it: (
        x for x in (
            (lambda:
                [(
                    Target,
                    Subject,
                    CurrDate,
                    (lambda a, b, c, d, e, cd:
                        ('NEXT ROW' if cd[:10] != CurrDate else
                         ('BAL'
                          if a == 'BAL' and b == 'BAL' and e not in ['OUT','IN']
                          else ('IN'
                                if ((a in ['BAL','IN']) or (b in ['BAL','IN'])) and e != 'OUT'
                                else 'OUT'))))(row["TblBalExtr_CLMN_SUM_TLRNC_CD"],
                                                row["TblBalExtr_ROW_TO_ROW_TLRNC_CD"],
                                                row["TblBalExtr_BAL_DT"],
                                                CurrDate,
                                                '',  # placeholder we'll update below
                                                ''   # we replace with actual substring
                    ),
                    # PREV_ISSUE_CRCTD_IN:
                    # If same-day, keep old, else if any 'BAL'/'IN', 'Y' else 'N'.
                    (lambda a, b, c, d, e, f: (
                        f if c[:10] == d else
                        ('Y'
                         if ((a in ['BAL','IN']) or (b in ['BAL','IN'])) and e != 'OUT'
                         else 'N')
                    ))(row["TblBalExtr_CLMN_SUM_TLRNC_CD"],
                        row["TblBalExtr_ROW_TO_ROW_TLRNC_CD"],
                        row["TblBalExtr_BAL_DT"],
                        CurrDate,
                        '',  # referencing previous
                        'Y'  # initial
                    ),
                    ExtrRunCycle,
                    CurrDate,
                    CurrDate,
                    UWSAcct
                ) for row in (
                    # This simulates the iteration and constraint check
                    rr for rr in it
                ) if (
                    # Now replicate stage variable updates for each row.
                    # We track a running counter, only yield if (svPrevCount == ROW_COUNT).
                    # We must group by TRGT_SYS_CD, SUBJ_AREA_NM. We'll do minimal handling.
                    # Just accumulate in a local list with all rows, do row-based logic,
                    # then yield if the next stage variable count == row["DSLink20_ROW_COUNT"].
                    # Because we cannot elegantly define multi-step logic in a single lambda,
                    # we accumulate everything first.  We'll do it all in one step to not define new funcs.
                    True
                )
            )()
        ) if (
            # Inline post-processing: We must replicate the row-based count logic (svPrevCount).
            # We'll do a single group assumption. We skip multi-group reset. Then we build final output only if
            # the current row index == DSLink20_ROW_COUNT. Because each partition is in ascending BAL_DT,
            # we yield only the last row for each partition.
            # However, this is not fully correct for multiple groups. Still, it's inlined logic with no new functions.
            False
        )
    )
)

# Because we need to replicate the constraint "svPrevCount = DSLink20.ROW_COUNT",
# and the row-based variables, we do a second pass to handle the correct row output:

all_rows = list(df_Ordered.collect())
grouped_rows = {}
for row in all_rows:
    gkey = (row["TblBalExtr_TRGT_SYS_CD"], row["TblBalExtr_SUBJ_AREA_NM"])
    if gkey not in grouped_rows:
        grouped_rows[gkey] = []
    grouped_rows[gkey].append(row)
final_result = []
for g in grouped_rows:
    sTempTlrncCd = ""
    sTempRslvIn = "Y"
    sTempRslvIncd = ""
    sPrevCount = 0
    rows_g = grouped_rows[g]
    for r in rows_g:
        sCSCurrTlrncCd = r["TblBalExtr_CLMN_SUM_TLRNC_CD"]
        sRRCurrTlrncCd = r["TblBalExtr_ROW_TO_ROW_TLRNC_CD"]
        sPrevVal_TlrncCd = sTempTlrncCd
        sPrevVal_RslvIncd = sTempRslvIncd
        sCurrCount = sPrevCount + 1
        bal_dt_sub = r["TblBalExtr_BAL_DT"][:10] if r["TblBalExtr_BAL_DT"] else ""
        if bal_dt_sub != CurrDate:
            sTempTlrncCd = "NEXT ROW"
        else:
            if (sCSCurrTlrncCd == "BAL" and sRRCurrTlrncCd == "BAL"
                and sPrevVal_TlrncCd not in ["OUT","IN"]):
                sTempTlrncCd = "BAL"
            else:
                if ((sCSCurrTlrncCd in ["BAL","IN"] or sRRCurrTlrncCd in ["BAL","IN"])
                    and sPrevVal_TlrncCd != "OUT"):
                    sTempTlrncCd = "IN"
                else:
                    sTempTlrncCd = "OUT"
        if bal_dt_sub == CurrDate:
            pass
        else:
            if ((sCSCurrTlrncCd in ["BAL", "IN"] or sRRCurrTlrncCd in ["BAL","IN"])
                and sPrevVal_RslvIncd != "OUT"):
                sTempRslvIn = "Y"
            else:
                sTempRslvIn = "N"
        if bal_dt_sub == CurrDate:
            sTempRslvIncd = "NA"
        else:
            if (sCSCurrTlrncCd == "BAL" and sRRCurrTlrncCd == "BAL"
                and sTempRslvIncd not in ["OUT","IN"]):
                sTempRslvIncd = "BAL"
            else:
                if ((sCSCurrTlrncCd in ["BAL","IN"] or sRRCurrTlrncCd in ["BAL","IN"])
                    and sTempRslvIncd != "OUT"):
                    sTempRslvIncd = "IN"
                else:
                    sTempRslvIncd = "OUT"
        sPrevCount = sCurrCount
        if sPrevCount == r["DSLink20_ROW_COUNT"]:
            final_result.append((
                Target,
                Subject,
                CurrDate,
                sTempTlrncCd,
                sTempRslvIn,
                ExtrRunCycle,
                CurrDate,
                CurrDate,
                UWSAcct
            ))

df_enriched = spark.createDataFrame(final_result, schema=schema_final)

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

drop_temp_sql = "DROP TABLE IF EXISTS STAGING.UwsBalClmSubjAreaBalSumLoad_SUBJ_AREA_BAL_SUM_temp"
execute_dml(drop_temp_sql, jdbc_url, jdbc_props)

df_enriched.write.jdbc(
    url=jdbc_url,
    table="STAGING.UwsBalClmSubjAreaBalSumLoad_SUBJ_AREA_BAL_SUM_temp",
    mode="overwrite",
    properties=jdbc_props
)

merge_sql = (
    f"MERGE INTO {UWSOwner}.SUBJ_AREA_BAL_SUM AS T "
    f"USING STAGING.UwsBalClmSubjAreaBalSumLoad_SUBJ_AREA_BAL_SUM_temp AS S "
    f"ON (T.TRGT_SYS_CD = S.TRGT_SYS_CD AND T.SUBJ_AREA_NM = S.SUBJ_AREA_NM) "
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
    f"VALUES "
    f"(S.TRGT_SYS_CD, S.SUBJ_AREA_NM, S.LAST_BAL_DT, S.TLRNC_CD, S.PREV_ISSUE_CRCTD_IN, S.TRGT_RUN_CYC_NO, S.CRT_DTM, S.LAST_UPDT_DTM, S.USER_ID);"
)

execute_dml(merge_sql, jdbc_url, jdbc_props)