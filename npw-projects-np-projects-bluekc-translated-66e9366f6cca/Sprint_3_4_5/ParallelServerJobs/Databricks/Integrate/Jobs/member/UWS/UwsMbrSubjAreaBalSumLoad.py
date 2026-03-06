# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 01/28/08 13:39:53 Batch  14638_49197 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 01/10/08 10:01:22 Batch  14620_36086 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 12/26/07 15:33:58 Batch  14605_56043 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/23/07 15:42:37 Batch  14572_56566 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 09/25/07 16:07:19 Batch  14513_58043 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 09/20/07 14:41:37 Batch  14508_52916 PROMOTE bckcetl ids20 dsadm bls for bl
# MAGIC ^1_1 09/20/07 13:58:24 Batch  14508_50316 INIT bckcett testIDS30 dsadm bls for bl
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:   FctsMbrshpBalCntl
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   
# MAGIC                 The Job loads the SUBJECT_AREA_BAL_SUM table after the balancing control job is done for a given Subject Area              
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               04/10/2007          3264                              Originally Programmed                           devlIDS30


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
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

extract_query_TblBalExtr = (
    f"SELECT TBL_BAL_SUM.TRGT_SYS_CD, "
    f"       TBL_BAL_SUM.SUBJ_AREA_NM, "
    f"       TBL_BAL_SUM.BAL_DT, "
    f"       TBL_BAL_SUM.ROW_CT_TLRNC_CD "
    f"  FROM {UWSOwner}.TBL_BAL_SUM TBL_BAL_SUM "
    f" WHERE TBL_BAL_SUM.TRGT_SYS_CD = '{Target}' "
    f"   AND TBL_BAL_SUM.SUBJ_AREA_NM = '{Subject}' "
    f" ORDER BY TBL_BAL_SUM.BAL_DT ASC"
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
    f"       TBL_BAL_SUM.SUBJ_AREA_NM, "
    f"       COUNT(*) AS ROW_COUNT "
    f"  FROM {UWSOwner}.TBL_BAL_SUM TBL_BAL_SUM "
    f" WHERE TBL_BAL_SUM.TRGT_SYS_CD = '{Target}' "
    f"   AND TBL_BAL_SUM.SUBJ_AREA_NM = '{Subject}' "
    f" GROUP BY TBL_BAL_SUM.TRGT_SYS_CD, TBL_BAL_SUM.SUBJ_AREA_NM"
)

df_DSLink18 = (
    spark.read.format("jdbc")
        .option("url", jdbc_url)
        .options(**jdbc_props)
        .option("query", extract_query_DSLink18)
        .load()
)

# Scenario A for hashed file: remove physical hashed file, then drop duplicates on key columns.
df_DSLink20 = df_DSLink18.dropDuplicates(["TRGT_SYS_CD", "SUBJ_AREA_NM"])

# Transformer: left join df_TblBalExtr(alias TblBalExtr) with df_DSLink20(alias DSLink20)
df_joined = (
    df_TblBalExtr.alias("TblBalExtr")
    .join(
        df_DSLink20.alias("DSLink20"),
        (
            (F.col("TblBalExtr.TRGT_SYS_CD") == F.col("DSLink20.TRGT_SYS_CD"))
            & (F.col("TblBalExtr.SUBJ_AREA_NM") == F.col("DSLink20.SUBJ_AREA_NM"))
        ),
        how="left"
    )
)

# Sort by BAL_DT ascending to simulate the row-by-row processing order.
df_joined = df_joined.orderBy("TblBalExtr.BAL_DT")

# Collect all rows to emulate DataStage row-by-row Stage Variable processing.
rows = df_joined.collect()

final_rows = []

# We will maintain per-(TRGT_SYS_CD,SUBJ_AREA_NM) state to replicate DataStage stage variables.
# Each row in DataStage updates these variables in sequence, then only emits if the constraint is true.
state_map = {}

for r in rows:
    # Extract source columns
    source_trgt_sys_cd = r["TblBalExtr.TRGT_SYS_CD"]
    source_subj_area_nm = r["TblBalExtr.SUBJ_AREA_NM"]
    source_bal_dt = r["TblBalExtr.BAL_DT"]
    source_row_ct_tlrnc_cd = r["TblBalExtr.ROW_CT_TLRNC_CD"]
    lookup_row_count = r["DSLink20.ROW_COUNT"] if r["DSLink20.ROW_COUNT"] is not None else None

    key = (source_trgt_sys_cd, source_subj_area_nm)
    if key not in state_map:
        # Initialize stage variable states for this group
        state_map[key] = {
            "svTempTlrncCd": "",
            "svTempRslvIn": "\"Y\"",   # Stage Variable 4 initial value
            "svTempRslvIncd": "",
            "svPrevCount": 0
        }

    curr_sv = state_map[key]

    #
    # Stage Variable 1: svCurrTlrncCd = TblBalExtr.ROW_CT_TLRNC_CD
    #
    svCurrTlrncCd = source_row_ct_tlrnc_cd if source_row_ct_tlrnc_cd is not None else ""

    #
    # Stage Variable 2: svCurrCount = svPrevCount + 1
    #
    svCurrCount = curr_sv["svPrevCount"] + 1

    #
    # Stage Variable 3: svTempTlrncCd
    # Expression:
    #   If TblBalExtr.BAL_DT[1,10] <> CurrDate then 'NEXT ROW'
    #   Else 
    #     If svCurrTlrncCd = 'BAL' And svTempTlrncCd <> 'OUT' And svTempTlrncCd <> 'IN' then 'BAL'
    #     else if svCurrTlrncCd = 'BAL' And svTempTlrncCd = 'IN' then 'IN'
    #     else if svCurrTlrncCd = 'IN' And svTempTlrncCd <> 'OUT' then 'IN'
    #     else 'OUT'
    #
    old_svTempTlrncCd = curr_sv["svTempTlrncCd"]
    if source_bal_dt is not None:
        bal_dt_sub = source_bal_dt[:10]
    else:
        bal_dt_sub = ""

    if bal_dt_sub != CurrDate:
        new_svTempTlrncCd = "NEXT ROW"
    else:
        if svCurrTlrncCd == "BAL" and old_svTempTlrncCd not in ("OUT", "IN"):
            new_svTempTlrncCd = "BAL"
        elif svCurrTlrncCd == "BAL" and old_svTempTlrncCd == "IN":
            new_svTempTlrncCd = "IN"
        elif svCurrTlrncCd == "IN" and old_svTempTlrncCd != "OUT":
            new_svTempTlrncCd = "IN"
        else:
            new_svTempTlrncCd = "OUT"

    #
    # Stage Variable 4: svTempRslvIn
    # Expression:
    #   If TblBalExtr.BAL_DT[1,10] = CurrDate then svTempRslvIn
    #   else 
    #     If svCurrTlrncCd = 'BAL' And svTempRslvIncd <> 'OUT' then 'Y'
    #     else if svCurrTlrncCd = 'IN' And svTempRslvIncd <> 'OUT' then 'Y'
    #     else 'N'
    #
    old_svTempRslvIn = curr_sv["svTempRslvIn"]
    old_svTempRslvIncd = curr_sv["svTempRslvIncd"]
    if bal_dt_sub == CurrDate:
        new_svTempRslvIn = old_svTempRslvIn
    else:
        if svCurrTlrncCd == "BAL" and old_svTempRslvIncd != "OUT":
            new_svTempRslvIn = "Y"
        elif svCurrTlrncCd == "IN" and old_svTempRslvIncd != "OUT":
            new_svTempRslvIn = "Y"
        else:
            new_svTempRslvIn = "N"

    #
    # Stage Variable 5: svTempRslvIncd
    # Expression:
    #   If TblBalExtr.BAL_DT[1,10] = CurrDate then 'NA'
    #   else 
    #     If svCurrTlrncCd = 'BAL' And svTempRslvIncd <> 'OUT' And svTempRslvIncd <> 'IN' then 'BAL'
    #     else if svCurrTlrncCd = 'BAL' And svTempRslvIncd = 'IN' then 'IN'
    #     else if svCurrTlrncCd = 'IN' And svTempRslvIncd <> 'OUT' then 'IN'
    #     else 'OUT'
    #
    old_svTempRslvIncd = curr_sv["svTempRslvIncd"]
    if bal_dt_sub == CurrDate:
        new_svTempRslvIncd = "NA"
    else:
        if svCurrTlrncCd == "BAL" and old_svTempRslvIncd not in ("OUT", "IN"):
            new_svTempRslvIncd = "BAL"
        elif svCurrTlrncCd == "BAL" and old_svTempRslvIncd == "IN":
            new_svTempRslvIncd = "IN"
        elif svCurrTlrncCd == "IN" and old_svTempRslvIncd != "OUT":
            new_svTempRslvIncd = "IN"
        else:
            new_svTempRslvIncd = "OUT"

    #
    # Stage Variable 6: svPrevCount = svCurrCount
    #
    new_svPrevCount = svCurrCount

    # Update the state
    state_map[key]["svTempTlrncCd"] = new_svTempTlrncCd
    state_map[key]["svTempRslvIn"] = new_svTempRslvIn
    state_map[key]["svTempRslvIncd"] = new_svTempRslvIncd
    state_map[key]["svPrevCount"] = new_svPrevCount

    # Constraint: output row if svPrevCount = DSLink20.ROW_COUNT
    # i.e. if new_svPrevCount == lookup_row_count
    if lookup_row_count is not None and new_svPrevCount == lookup_row_count:
        # Produce the output columns from the Transformer
        out_row = {
            "TRGT_SYS_CD": Target,
            "SUBJ_AREA_NM": Subject,
            "LAST_BAL_DT": CurrDate,
            "TLRNC_CD": new_svTempTlrncCd,
            "PREV_ISSUE_CRCTD_IN": new_svTempRslvIn,
            "TRGT_RUN_CYC_NO": ExtrRunCycle,
            "CRT_DTM": CurrDate,
            "LAST_UPDT_DTM": CurrDate,
            "USER_ID": UWSAcct
        }
        final_rows.append(out_row)

# Create final dataframe
# The final schema must match exactly the output columns in the same order.
schema_final = StructType([
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

df_enriched = spark.createDataFrame(final_rows, schema=schema_final)

# For all char/varchar columns, apply rpad with an assumed length if known. 
# "PREV_ISSUE_CRCTD_IN" is char(1).
# The other text columns have no explicit lengths, so use a large rpad (e.g. 255) as we have no exact metadata.
df_enriched = df_enriched \
    .withColumn("TRGT_SYS_CD", F.rpad("TRGT_SYS_CD", 255, " ")) \
    .withColumn("SUBJ_AREA_NM", F.rpad("SUBJ_AREA_NM", 255, " ")) \
    .withColumn("TLRNC_CD", F.rpad("TLRNC_CD", 255, " ")) \
    .withColumn("PREV_ISSUE_CRCTD_IN", F.rpad("PREV_ISSUE_CRCTD_IN", 1, " ")) \
    .withColumn("USER_ID", F.rpad("USER_ID", 255, " "))

# Now write to a staging table, then do a MERGE upsert into the final table #$UWSOwner#.SUBJ_AREA_BAL_SUM
temp_table_name = f"STAGING.UwsMbrSubjAreaBalSumLoad_SUBJ_AREA_BAL_SUM_temp"
spark.sql(f"DROP TABLE IF EXISTS {temp_table_name}")

(
    df_enriched.write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", temp_table_name)
    .mode("overwrite")
    .save()
)

merge_sql = (
    f"MERGE INTO {UWSOwner}.SUBJ_AREA_BAL_SUM AS T "
    f"USING {temp_table_name} AS S "
    f"ON T.TRGT_SYS_CD = S.TRGT_SYS_CD AND T.SUBJ_AREA_NM = S.SUBJ_AREA_NM "
    f"WHEN MATCHED THEN UPDATE SET "
    f"  T.LAST_BAL_DT = S.LAST_BAL_DT, "
    f"  T.TLRNC_CD = S.TLRNC_CD, "
    f"  T.PREV_ISSUE_CRCTD_IN = S.PREV_ISSUE_CRCTD_IN, "
    f"  T.TRGT_RUN_CYC_NO = S.TRGT_RUN_CYC_NO, "
    f"  T.CRT_DTM = S.CRT_DTM, "
    f"  T.LAST_UPDT_DTM = S.LAST_UPDT_DTM, "
    f"  T.USER_ID = S.USER_ID "
    f"WHEN NOT MATCHED THEN INSERT "
    f"  (TRGT_SYS_CD, SUBJ_AREA_NM, LAST_BAL_DT, TLRNC_CD, PREV_ISSUE_CRCTD_IN, TRGT_RUN_CYC_NO, CRT_DTM, LAST_UPDT_DTM, USER_ID) "
    f"  VALUES "
    f"  (S.TRGT_SYS_CD, S.SUBJ_AREA_NM, S.LAST_BAL_DT, S.TLRNC_CD, S.PREV_ISSUE_CRCTD_IN, S.TRGT_RUN_CYC_NO, S.CRT_DTM, S.LAST_UPDT_DTM, S.USER_ID);"
)

execute_dml(merge_sql, jdbc_url, jdbc_props)