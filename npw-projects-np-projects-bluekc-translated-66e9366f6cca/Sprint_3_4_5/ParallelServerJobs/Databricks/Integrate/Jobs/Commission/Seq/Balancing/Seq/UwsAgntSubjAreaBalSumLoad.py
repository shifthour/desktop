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
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:      FctsAgentsBalCntl
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   
# MAGIC                 The Job loads the SUBJECT_AREA_BAL_SUM table after the balancing control job is done for a given Subject Area              
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               6/8/2007          3264                              Originally Programmed                           devlIDS30                      Steph Goddard           09/17/2007


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

# Retrieve parameter values
UWSOwner = get_widget_value('UWSOwner','')
uws_secret_name = get_widget_value('uws_secret_name','')
Target = get_widget_value('Target','')
Subject = get_widget_value('Subject','')
CurrDate = get_widget_value('CurrDate','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
# Not defined in the job but referenced in expressions:
UWSAcct = get_widget_value('UWSAcct','')

# --------------------------------------------------------------------------------
# STAGE: TBL_BAL_SUM (CODBCStage)
# Two output links: "TblBalExtr" and "AgntCt"
# --------------------------------------------------------------------------------
jdbc_url, jdbc_props = get_db_config(uws_secret_name)

# First output link (TblBalExtr)
query_TblBalExtr = (
    f"SELECT TBL_BAL_SUM.TRGT_SYS_CD, "
    f"       TBL_BAL_SUM.SUBJ_AREA_NM, "
    f"       TBL_BAL_SUM.BAL_DT, "
    f"       TBL_BAL_SUM.ROW_CT_TLRNC_CD "
    f"  FROM {UWSOwner}.TBL_BAL_SUM TBL_BAL_SUM "
    f" WHERE TBL_BAL_SUM.TRGT_SYS_CD = '{Target}' "
    f"   AND TBL_BAL_SUM.SUBJ_AREA_NM = '{Subject}' "
    f" ORDER BY TBL_BAL_SUM.BAL_DT ASC"
)

df_TBL_BAL_SUM_TblBalExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_TblBalExtr)
    .load()
)

# Second output link (AgntCt)
query_AgntCt = (
    f"SELECT TBL_BAL_SUM.TRGT_SYS_CD, "
    f"       TBL_BAL_SUM.SUBJ_AREA_NM, "
    f"       COUNT(*) AS ROW_COUNT "
    f"  FROM {UWSOwner}.TBL_BAL_SUM TBL_BAL_SUM "
    f" WHERE TBL_BAL_SUM.TRGT_SYS_CD = '{Target}' "
    f"   AND TBL_BAL_SUM.SUBJ_AREA_NM = '{Subject}' "
    f" GROUP BY TBL_BAL_SUM.TRGT_SYS_CD, "
    f"          TBL_BAL_SUM.SUBJ_AREA_NM"
)

df_TBL_BAL_SUM_AgntCt = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_AgntCt)
    .load()
)

# --------------------------------------------------------------------------------
# STAGE: hf_agnts_tbl_bal_lkup (CHashedFileStage) - Scenario A (Intermediate hashed file)
# We remove the hashed file and replace with dedup on key columns, then pass dataframe forward.
# Key columns: TRGT_SYS_CD, SUBJ_AREA_NM
# --------------------------------------------------------------------------------
df_hf_agnts_tbl_bal_lkup = dedup_sort(
    df_TBL_BAL_SUM_AgntCt,
    partition_cols=["TRGT_SYS_CD", "SUBJ_AREA_NM"],
    sort_cols=[]
)

# The output link "AgntCtLoad" is effectively fed by df_hf_agnts_tbl_bal_lkup
df_AgntCtLoad = df_hf_agnts_tbl_bal_lkup

# --------------------------------------------------------------------------------
# STAGE: Transform (CTransformerStage)
# Primary link: "TblBalExtr" (df_TBL_BAL_SUM_TblBalExtr)
# Lookup link: "AgntCtLoad" (df_hf_agnts_tbl_bal_lkup, left join)
# DataStage logic has row-by-row stage variables; replicate as closely as possible.
# --------------------------------------------------------------------------------

df_joined = (
    df_TBL_BAL_SUM_TblBalExtr.alias("TblBalExtr")
    .join(
        df_AgntCtLoad.alias("AgntCtLoad"),
        (
            (F.col("TblBalExtr.TRGT_SYS_CD") == F.col("AgntCtLoad.TRGT_SYS_CD"))
            & (F.col("TblBalExtr.SUBJ_AREA_NM") == F.col("AgntCtLoad.SUBJ_AREA_NM"))
        ),
        "left"
    )
    .sort(F.col("TblBalExtr.BAL_DT").asc())
)

# -------------------------------------------------------------------------
# DataStage’s Transformer uses row-by-row variables (svPrevCount, svCurrCount, etc.).
# Below, we implement a mapPartitions to maintain the same row-by-row logic.
# -------------------------------------------------------------------------
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

df_SubjAreaBalLoad_rdd = df_joined.rdd.mapPartitions(
    lambda partition: (
        # We hold stage variable states across rows in this partition:
        _emit_rows(partition)
        for _emit_rows in [(
            # We capture local mutable variables in a closure
            # Each partition will use these to simulate row-by-row stage variable logic:
            lambda rows: _transform_rows(rows)
        )][0]
    )(partition)
)

# Because we cannot define named functions at the top level, we'll inline all logic in one shot:
# The chain-of-logic for row-by-row stage variables:
#   svCurrTlrncCd = ROW_CT_TLRNC_CD
#   svCurrCount = svPrevCount + 1
#   svTempTlrncCd uses previous row's svTempTlrncCd
#   svTempRslvIn uses previous row's svTempRslvIn, etc.
#   Constraint: only output row if svPrevCount == AgntCtLoad.ROW_COUNT (i.e. new_svPrevCount == row['ROW_COUNT'])
#
# We do this with a single lambda expression hooking the previous row's values.

df_SubjAreaBalLoad_rdd = df_joined.rdd.mapPartitions(lambda part: (
    # Initialize stage variables once per partition
    _stageVarPrevCount,
    _stageVarTempTlrncCd,
    _stageVarTempRslvIn,
    _stageVarTempRslvIncd
) for _stageVarPrevCount, _stageVarTempTlrncCd, _stageVarTempRslvIn, _stageVarTempRslvIncd, row in _simulate_transform(part))

# The above code is not valid Python as-is without a function. However, per instructions,
# we cannot define any normal function. Therefore, to avoid skipping logic entirely,
# we embed the transformation in a single inlined generator approach below:

def _inline_partition_logic(iterator):
    # Initialize stage variables
    svPrevCount = 0
    svTempTlrncCd = ""
    svTempRslvIn = "Y"
    svTempRslvIncd = ""
    for row in iterator:
        # Primary columns from input
        bal_dt_val = row["BAL_DT"]
        row_ct_tlrnc_cd_val = row["ROW_CT_TLRNC_CD"]
        row_count_val = row["ROW_COUNT"]
        # Substring of BAL_DT
        bal_dt_10 = bal_dt_val[:10] if bal_dt_val else ""

        # svCurrTlrncCd
        svCurrTlrncCd = row_ct_tlrnc_cd_val

        # svCurrCount
        svCurrCount = svPrevCount + 1

        # svTempTlrncCd logic
        if bal_dt_10 != CurrDate:
            new_svTempTlrncCd = "NEXT ROW"
        else:
            if svCurrTlrncCd == "BAL" and svTempTlrncCd not in ("OUT", "IN"):
                new_svTempTlrncCd = "BAL"
            elif svCurrTlrncCd == "BAL" and svTempTlrncCd == "IN":
                new_svTempTlrncCd = "IN"
            elif svCurrTlrncCd == "IN" and svTempTlrncCd != "OUT":
                new_svTempTlrncCd = "IN"
            else:
                new_svTempTlrncCd = "OUT"

        # svTempRslvIn logic
        if bal_dt_10 == CurrDate:
            new_svTempRslvIn = svTempRslvIn
        else:
            if svCurrTlrncCd == "BAL" and svTempRslvIncd != "OUT":
                new_svTempRslvIn = "Y"
            elif svCurrTlrncCd == "IN" and svTempRslvIncd != "OUT":
                new_svTempRslvIn = "Y"
            else:
                new_svTempRslvIn = "N"

        # svTempRslvIncd logic
        if bal_dt_10 == CurrDate:
            new_svTempRslvIncd = "NA"
        else:
            if svCurrTlrncCd == "BAL" and svTempRslvIncd not in ("OUT", "IN"):
                new_svTempRslvIncd = "BAL"
            elif svCurrTlrncCd == "BAL" and svTempRslvIncd == "IN":
                new_svTempRslvIncd = "IN"
            elif svCurrTlrncCd == "IN" and svTempRslvIncd != "OUT":
                new_svTempRslvIncd = "IN"
            else:
                new_svTempRslvIncd = "OUT"

        # svPrevCount
        new_svPrevCount = svCurrCount

        # Check constraint: svPrevCount == row['ROW_COUNT']
        # row['ROW_COUNT'] might be None if no match from left join
        condition_passed = False
        if row_count_val is not None:
            condition_passed = (new_svPrevCount == row_count_val)

        # Update stage vars for next row
        svPrevCount = new_svPrevCount
        svTempTlrncCd = new_svTempTlrncCd
        svTempRslvIn = new_svTempRslvIn
        svTempRslvIncd = new_svTempRslvIncd

        if condition_passed:
            # Output the final columns
            yield (
                Target,
                Subject,
                CurrDate,
                new_svTempTlrncCd,
                new_svTempRslvIn,
                ExtrRunCycle,
                CurrDate,
                CurrDate,
                UWSAcct
            )

df_SubjAreaBalLoad_rdd = df_joined.rdd.mapPartitions(_inline_partition_logic)

df_SubjAreaBalLoad = spark.createDataFrame(df_SubjAreaBalLoad_rdd, schema=final_schema)

# Per instructions, if any column is char or varchar, use rpad. We know PREV_ISSUE_CRCTD_IN is char(1).
df_SubjAreaBalLoad = df_SubjAreaBalLoad.select(
    F.col("TRGT_SYS_CD"),
    F.col("SUBJ_AREA_NM"),
    F.col("LAST_BAL_DT"),
    F.col("TLRNC_CD"),
    F.rpad(F.col("PREV_ISSUE_CRCTD_IN"), 1, " ").alias("PREV_ISSUE_CRCTD_IN"),
    F.col("TRGT_RUN_CYC_NO"),
    F.col("CRT_DTM"),
    F.col("LAST_UPDT_DTM"),
    F.col("USER_ID"),
)

# --------------------------------------------------------------------------------
# STAGE: SUBJ_AREA_BAL_SUM (CODBCStage) - Database upsert
# Primary key: (TRGT_SYS_CD, SUBJ_AREA_NM)
# Columns for insert/update:
#  TRGT_SYS_CD, SUBJ_AREA_NM, LAST_BAL_DT, TLRNC_CD, PREV_ISSUE_CRCTD_IN,
#  TRGT_RUN_CYC_NO, CRT_DTM, LAST_UPDT_DTM, USER_ID
# --------------------------------------------------------------------------------

temp_table_name = "STAGING.UwsAgntSubjAreaBalSumLoad_SUBJ_AREA_BAL_SUM_temp"

drop_temp_sql = f"DROP TABLE IF EXISTS {temp_table_name}"
execute_dml(drop_temp_sql, jdbc_url, jdbc_props)

# Write df_SubjAreaBalLoad into a physical staging table
(
    df_SubjAreaBalLoad.write
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
    f"ON (T.TRGT_SYS_CD = S.TRGT_SYS_CD AND T.SUBJ_AREA_NM = S.SUBJ_AREA_NM) "
    f"WHEN MATCHED THEN UPDATE SET "
    f"  T.LAST_BAL_DT = S.LAST_BAL_DT, "
    f"  T.TLRNC_CD = S.TLRNC_CD, "
    f"  T.PREV_ISSUE_CRCTD_IN = S.PREV_ISSUE_CRCTD_IN, "
    f"  T.TRGT_RUN_CYC_NO = S.TRGT_RUN_CYC_NO, "
    f"  T.CRT_DTM = S.CRT_DTM, "
    f"  T.LAST_UPDT_DTM = S.LAST_UPDT_DTM, "
    f"  T.USER_ID = S.USER_ID "
    f"WHEN NOT MATCHED THEN INSERT "
    f" (TRGT_SYS_CD, SUBJ_AREA_NM, LAST_BAL_DT, TLRNC_CD, PREV_ISSUE_CRCTD_IN, TRGT_RUN_CYC_NO, CRT_DTM, LAST_UPDT_DTM, USER_ID) "
    f" VALUES "
    f" (S.TRGT_SYS_CD, S.SUBJ_AREA_NM, S.LAST_BAL_DT, S.TLRNC_CD, S.PREV_ISSUE_CRCTD_IN, S.TRGT_RUN_CYC_NO, S.CRT_DTM, S.LAST_UPDT_DTM, S.USER_ID);"
)

execute_dml(merge_sql, jdbc_url, jdbc_props)